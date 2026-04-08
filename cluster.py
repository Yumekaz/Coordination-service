"""
Cluster replication primitives for leader/follower deployments.

This is not consensus. It is honest multi-node groundwork:
- leader exposes committed operations
- follower catches up from leader history
- cluster status reports role, lag, and peer reachability
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional
from urllib import error, parse, request

from coordinator import Coordinator
from models import Operation
from errors import ConflictError
from logger import get_logger

logger = get_logger("cluster")

REPLICATION_TOKEN_HEADER = "X-Coord-Cluster-Token"


class ClusterManager:
    """Leader/follower replication manager backed by committed operation history."""

    def __init__(
        self,
        coordinator: Coordinator,
        *,
        node_id: str = "node-1",
        role: str = "standalone",
        leader_url: Optional[str] = None,
        peer_urls: Optional[List[str]] = None,
        replication_token: str = "",
        poll_interval_seconds: float = 1.0,
        batch_size: int = 128,
        require_write_quorum: bool = False,
        push_commit_replication: bool = True,
        advertise_url: Optional[str] = None,
        heartbeat_interval_seconds: float = 1.0,
        election_timeout_seconds: float = 3.0,
        prepare_timeout_seconds: float = 5.0,
        request_json: Optional[Callable[[str, str, Optional[Dict[str, Any]]], Dict[str, Any]]] = None,
    ):
        self._coordinator = coordinator
        self._node_id = node_id
        self._role = role.lower().strip() or "standalone"
        self._leader_url = leader_url.rstrip("/") if leader_url else None
        self._peer_urls = [peer.rstrip("/") for peer in (peer_urls or []) if peer]
        self._replication_token = replication_token
        self._poll_interval_seconds = max(0.2, float(poll_interval_seconds))
        self._batch_size = max(1, int(batch_size))
        self._require_write_quorum = require_write_quorum
        self._push_commit_replication = push_commit_replication
        self._advertise_url = advertise_url.rstrip("/") if advertise_url else None
        self._heartbeat_interval_seconds = max(0.2, float(heartbeat_interval_seconds))
        self._election_timeout_seconds = max(
            self._heartbeat_interval_seconds * 2.0,
            float(election_timeout_seconds),
        )
        self._prepare_timeout_seconds = max(0.5, float(prepare_timeout_seconds))
        self._request_json = request_json or self._default_request_json

        self._lock = threading.RLock()
        self._prepare_barrier = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_sync_at: Optional[float] = None
        self._last_error: Optional[str] = None
        self._leader_id: Optional[str] = None
        self._leader_commit_index = coordinator.get_stats().get("last_sequence", 0)
        self._recent_apply_events: Deque[Dict[str, Any]] = deque(maxlen=20)
        self._current_term = 0
        self._voted_for: Optional[str] = None
        self._last_leader_contact_at: Optional[float] = time.time() if self._role == "leader" else None
        self._quorum_commit_index = 0
        self._prepared_sequence: Optional[int] = None
        self._prepared_count: int = 0
        self._prepared_term: Optional[int] = None
        self._prepared_leader_id: Optional[str] = None
        self._prepared_expires_at: Optional[float] = None
        self._peer_states: Dict[str, Dict[str, Any]] = {
            peer: {
                "peer_url": peer,
                "peer_id": None,
                "role": "unknown",
                "reachable": False,
                "healthy": False,
                "last_applied": 0,
                "replication_lag": None,
                "last_ack_at": None,
                "last_error": None,
            }
            for peer in self._peer_urls
        }
        self._commit_callback_registered = False

        persisted_state = self._coordinator.load_cluster_state(self._node_id) or {}
        self._current_term = int(persisted_state.get("current_term", self._current_term))
        self._voted_for = persisted_state.get("voted_for") or self._voted_for
        self._leader_id = persisted_state.get("leader_id") or self._leader_id
        if not self._leader_url:
            self._leader_url = persisted_state.get("leader_url") or self._leader_url

        if self._role == "leader":
            self._current_term = max(1, self._current_term + 1)
            self._voted_for = self._node_id
            self._leader_id = self._node_id
            self._leader_url = self._advertise_url or self._leader_url

        self._apply_replica_mode()
        with self._lock:
            self._persist_cluster_state_locked()

    @classmethod
    def from_env(cls, coordinator: Coordinator) -> "ClusterManager":
        """Create cluster manager config from environment variables."""
        node_id = os.environ.get("COORD_NODE_ID", "node-1")
        role = os.environ.get("COORD_CLUSTER_ROLE", "standalone")
        leader_url = os.environ.get("COORD_LEADER_URL")
        peers_raw = os.environ.get("COORD_PEER_URLS", "")
        peer_urls = [item.strip() for item in peers_raw.split(",") if item.strip()]
        token = os.environ.get("COORD_REPLICATION_TOKEN", "")
        poll_interval = float(os.environ.get("COORD_REPLICATION_POLL_INTERVAL", "1.0"))
        batch_size = int(os.environ.get("COORD_REPLICATION_BATCH_SIZE", "128"))
        require_write_quorum = os.environ.get("COORD_REQUIRE_WRITE_QUORUM", "").lower() in {"1", "true", "yes", "on"}
        push_commit_replication = os.environ.get("COORD_PUSH_REPLICATION", "1").lower() in {"1", "true", "yes", "on"}
        advertise_url = os.environ.get("COORD_ADVERTISE_URL")
        heartbeat_interval = float(os.environ.get("COORD_HEARTBEAT_INTERVAL", "1.0"))
        election_timeout = float(os.environ.get("COORD_ELECTION_TIMEOUT", "3.0"))
        prepare_timeout = float(os.environ.get("COORD_PREPARE_TIMEOUT", "5.0"))
        return cls(
            coordinator,
            node_id=node_id,
            role=role,
            leader_url=leader_url,
            peer_urls=peer_urls,
            replication_token=token,
            poll_interval_seconds=poll_interval,
            batch_size=batch_size,
            require_write_quorum=require_write_quorum,
            push_commit_replication=push_commit_replication,
            advertise_url=advertise_url,
            heartbeat_interval_seconds=heartbeat_interval,
            election_timeout_seconds=election_timeout,
            prepare_timeout_seconds=prepare_timeout,
        )

    def start(self) -> None:
        """Start background catch-up/peer-monitoring if needed."""
        with self._lock:
            if self._running:
                return
            if self._role == "standalone" and not self._peer_urls and not self._leader_url:
                return
            self._running = True
            self._sync_commit_callback_locked()
            self._thread = threading.Thread(
                target=self._loop,
                daemon=True,
                name="cluster-replication",
            )
            self._thread.start()
            logger.info("Cluster manager started: role=%s node=%s", self._role, self._node_id)

    def stop(self) -> None:
        """Stop background replication work."""
        with self._lock:
            self._running = False
            thread = self._thread
            self._thread = None
            if self._commit_callback_registered:
                self._coordinator.remove_commit_callback(self._on_leader_commit)
                self._commit_callback_registered = False
            self._coordinator.set_write_guard(None)
            self._coordinator.set_prepare_guard(None)
        if thread is not None:
            thread.join(timeout=2.0)

    def export_operations(self, since_sequence: int = 0, limit: Optional[int] = None) -> Dict[str, Any]:
        """Expose committed operations for follower catch-up."""
        limit = self._batch_size if limit is None else max(1, int(limit))
        operations = self._coordinator.get_operations(since_sequence=since_sequence, limit=limit)
        commit_index = self._coordinator.get_stats().get("last_sequence", 0)
        return {
            "node_id": self._node_id,
            "role": self._role,
            "term": self._current_term,
            "since_sequence": since_sequence,
            "commit_index": commit_index,
            "operations": [operation.to_dict() for operation in operations],
            "count": len(operations),
        }

    def get_internal_state(self) -> Dict[str, Any]:
        """Return local replication state for leader peer monitoring."""
        stats = self._coordinator.get_stats()
        read_only = self._coordinator.get_read_only_status()
        return {
            "node_id": self._node_id,
            "role": self._role,
            "term": self._current_term,
            "commit_index": stats.get("last_sequence", 0),
            "last_applied": stats.get("last_sequence", 0),
            "read_only": read_only["read_only"],
            "last_sync_at": self._last_sync_at,
            "last_error": self._last_error,
            "leader_id": self._leader_id,
            "leader_url": self._leader_url,
        }

    def apply_replication_batch(
        self,
        source_node_id: Optional[str],
        source_term: Optional[int],
        operations_payload: List[Dict[str, Any]],
        prepared_write: bool = False,
    ) -> Dict[str, Any]:
        """Apply a pushed replication batch on a follower."""
        operations = [Operation.from_dict(item) for item in operations_payload]
        started_at = time.time()
        with self._lock:
            self._clear_expired_prepare_locked()
            if prepared_write and operations:
                reason = self._validate_prepare_reservation_locked(
                    leader_id=source_node_id,
                    term=source_term,
                    operations=operations,
                )
                if reason is not None:
                    raise ConflictError(
                        "Replication apply rejected: follower has no matching prepare reservation",
                        error="replication_prepare_mismatch",
                        reason=reason,
                    )
        applied = self._coordinator.apply_replicated_operations(operations)
        applied_at = time.time()
        with self._lock:
            self._clear_expired_prepare_locked()
            if source_node_id:
                self._leader_id = source_node_id
                self._role = "follower"
            if source_term is not None and source_term > self._current_term:
                self._current_term = int(source_term)
                self._voted_for = source_node_id or self._voted_for
            if operations:
                self._leader_commit_index = max(self._leader_commit_index, operations[-1].sequence_number)
                self._advance_prepare_reservation_locked(
                    leader_id=source_node_id,
                    term=source_term,
                    operations=operations,
                )
            self._last_sync_at = applied_at
            self._last_error = None
            self._last_leader_contact_at = applied_at
            self._persist_cluster_state_locked()
            if applied and operations:
                self._recent_apply_events.appendleft(
                    {
                        "sequence_number": operations[-1].sequence_number,
                        "peer_id": source_node_id or self._leader_url or "leader",
                        "status": "applied",
                        "applied_at": applied_at,
                        "latency_ms": int((applied_at - started_at) * 1000),
                        "applied_count": applied,
                    }
                )
        return {
            "status": "ok",
            "applied_count": applied,
            "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
            "node_id": self._node_id,
        }

    def receive_prepare(
        self,
        leader_id: str,
        term: int,
        leader_url: Optional[str],
        start_sequence: int,
        count: int = 1,
    ) -> Dict[str, Any]:
        """Reserve the exact next committed sequence range for a leader."""
        if count < 1:
            raise ValueError("prepare count must be at least 1")

        with self._lock:
            self._clear_expired_prepare_locked()
            local_last_applied = self._coordinator.get_stats().get("last_sequence", 0)
            expected_next = local_last_applied + 1

            if term < self._current_term:
                return {
                    "accepted": False,
                    "term": self._current_term,
                    "node_id": self._node_id,
                    "last_applied": local_last_applied,
                    "expected_next": expected_next,
                    "reason": "stale_term",
                }

            if term == self._current_term and self._voted_for not in {None, leader_id}:
                return {
                    "accepted": False,
                    "term": self._current_term,
                    "node_id": self._node_id,
                    "last_applied": local_last_applied,
                    "expected_next": expected_next,
                    "reason": "voted_for_other_leader",
                }

            if start_sequence != expected_next:
                return {
                    "accepted": False,
                    "term": self._current_term,
                    "node_id": self._node_id,
                    "last_applied": local_last_applied,
                    "expected_next": expected_next,
                    "reason": "sequence_gap",
                }

            if self._prepared_sequence is not None:
                same_reservation = (
                    self._prepared_sequence == start_sequence
                    and self._prepared_count == count
                    and self._prepared_term == term
                    and self._prepared_leader_id == leader_id
                )
                if not same_reservation:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_applied": local_last_applied,
                        "expected_next": expected_next,
                        "reason": "prepare_conflict",
                    }

            if term > self._current_term:
                self._current_term = term
                self._voted_for = leader_id
                if self._role != "standalone":
                    self._role = "follower"
                    self._apply_replica_mode()
            elif self._voted_for is None:
                self._voted_for = leader_id

            self._leader_id = leader_id
            self._leader_url = leader_url.rstrip("/") if leader_url else self._leader_url
            self._last_leader_contact_at = time.time()
            self._prepared_sequence = start_sequence
            self._prepared_count = count
            self._prepared_term = term
            self._prepared_leader_id = leader_id
            self._prepared_expires_at = time.time() + self._prepare_timeout_seconds
            self._persist_cluster_state_locked()

            return {
                "accepted": True,
                "term": self._current_term,
                "node_id": self._node_id,
                "last_applied": local_last_applied,
                "prepared_sequence": start_sequence,
                "prepared_count": count,
            }

    def receive_cancel_prepare(
        self,
        leader_id: str,
        term: int,
        start_sequence: int,
        count: int = 1,
    ) -> Dict[str, Any]:
        """Best-effort release for a follower-side prepare reservation."""
        with self._lock:
            cancelled = (
                self._prepared_sequence == start_sequence
                and self._prepared_count == count
                and self._prepared_term == term
                and self._prepared_leader_id == leader_id
            )
            if cancelled:
                self._clear_prepare_reservation_locked()
            return {
                "cancelled": cancelled,
                "term": self._current_term,
                "node_id": self._node_id,
                "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
            }

    def prepare_write_quorum(
        self,
        operations: List[Operation],
    ) -> Optional[Callable[[], None]]:
        """Require a majority of peers to reserve the exact next sequence range."""
        if self._role != "leader" or not self._require_write_quorum or not self._peer_urls:
            return None
        if not operations:
            return None

        ordered = sorted(operations, key=lambda operation: operation.sequence_number)
        start_sequence = ordered[0].sequence_number
        for index, operation in enumerate(ordered):
            expected_sequence = start_sequence + index
            if operation.sequence_number != expected_sequence:
                raise ConflictError(
                    "Prepared write batch must be contiguous",
                    error="write_quorum_prepare_invalid",
                )

        barrier = self._prepare_barrier
        barrier.acquire()
        released = False
        prepared_peers: List[str] = []

        def release_prepare() -> None:
            nonlocal released
            if not released:
                released = True
                barrier.release()

        try:
            with self._lock:
                if self._role != "leader":
                    raise ConflictError(
                        "Write quorum prepare failed: this node is no longer the leader",
                        error="write_quorum_stepped_down",
                    )
                current_term = self._current_term

            self.assert_write_quorum_available(current_sequence_override=start_sequence - 1)

            reserved_votes = 1
            for peer in self._peer_urls:
                started_at = time.time()
                try:
                    payload = self._request_json(
                        f"{peer}/internal/replication/prepare",
                        "POST",
                        {
                            "leader_id": self._node_id,
                            "leader_url": self._advertise_url,
                            "term": current_term,
                            "start_sequence": start_sequence,
                            "count": len(ordered),
                        },
                    )
                    peer_term = int(payload.get("term", current_term))
                    if peer_term > current_term:
                        self._become_follower(peer_term, payload.get("node_id"), peer)
                        raise ConflictError(
                            "Write quorum prepare failed: remote node advertises a higher term leader",
                            error="write_quorum_stepped_down",
                        )

                    reserved = bool(payload.get("accepted"))
                    last_applied = int(payload.get("last_applied", 0))
                    self._peer_states[peer] = {
                        "peer_url": peer,
                        "peer_id": payload.get("node_id"),
                        "role": "follower",
                        "reachable": True,
                        "healthy": reserved,
                        "last_applied": last_applied,
                        "replication_lag": max(0, self._coordinator.get_stats().get("last_sequence", 0) - last_applied),
                        "last_ack_at": time.time(),
                        "last_error": None if reserved else payload.get("reason", "prepare_rejected"),
                    }
                    if reserved:
                        prepared_peers.append(peer)
                        reserved_votes += 1
                    self._recent_apply_events.appendleft(
                        {
                            "sequence_number": ordered[-1].sequence_number,
                            "peer_id": payload.get("node_id") or peer,
                            "status": "prepared" if reserved else "prepare_rejected",
                            "applied_at": time.time(),
                            "latency_ms": int((time.time() - started_at) * 1000),
                            "applied_count": 0,
                        }
                    )
                except ConflictError:
                    raise
                except Exception as exc:
                    self._peer_states[peer] = {
                        "peer_url": peer,
                        "peer_id": None,
                        "role": "unknown",
                        "reachable": False,
                        "healthy": False,
                        "last_applied": 0,
                        "replication_lag": None,
                        "last_ack_at": None,
                        "last_error": str(exc),
                    }

            if reserved_votes < self._quorum_size():
                self._cancel_prepare_reservations(
                    peers=prepared_peers,
                    term=current_term,
                    start_sequence=start_sequence,
                    count=len(ordered),
                )
                raise ConflictError(
                    "Write quorum prepare failed: majority could not reserve the next committed sequence",
                    error="write_quorum_prepare_failed",
                    start_sequence=start_sequence,
                    prepared_votes=reserved_votes,
                    quorum_size=self._quorum_size(),
                )
            return release_prepare
        except Exception:
            release_prepare()
            raise

    def receive_heartbeat(
        self,
        leader_id: str,
        term: int,
        leader_url: Optional[str],
        commit_index: int,
    ) -> Dict[str, Any]:
        """Accept or reject a leader heartbeat."""
        apply_mode = False
        with self._prepare_barrier:
            with self._lock:
                if term < self._current_term:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                    }

                if term == self._current_term and self._voted_for not in {None, leader_id}:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                    }

                if term > self._current_term:
                    self._current_term = term
                    self._voted_for = leader_id
                elif self._voted_for is None:
                    self._voted_for = leader_id

                self._leader_id = leader_id
                self._leader_url = leader_url.rstrip("/") if leader_url else self._leader_url
                self._leader_commit_index = max(self._leader_commit_index, commit_index)
                self._last_leader_contact_at = time.time()
                self._clear_prepare_reservation_locked()
                if self._role != "standalone":
                    self._role = "follower"
                    apply_mode = True
                    self._last_leader_contact_at = time.time()
                self._persist_cluster_state_locked()

                response = {
                    "accepted": True,
                    "term": self._current_term,
                    "node_id": self._node_id,
                    "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                }

        if apply_mode:
            self._apply_replica_mode()
            with self._lock:
                self._last_leader_contact_at = time.time()
        return response

    def request_vote(
        self,
        candidate_id: str,
        term: int,
        candidate_last_applied: int,
    ) -> Dict[str, Any]:
        """Evaluate a vote request from a candidate."""
        apply_mode = False
        with self._prepare_barrier:
            with self._lock:
                if term < self._current_term:
                    return {
                        "vote_granted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                    }

                if term > self._current_term:
                    self._current_term = term
                    self._voted_for = None
                    self._leader_id = None
                    self._leader_url = None
                    self._clear_prepare_reservation_locked()
                    if self._role != "standalone":
                        self._role = "follower"
                        apply_mode = True

                local_last_applied = self._coordinator.get_stats().get("last_sequence", 0)
                vote_granted = (
                    (self._voted_for is None or self._voted_for == candidate_id)
                    and candidate_last_applied >= local_last_applied
                )
                if vote_granted:
                    self._voted_for = candidate_id
                self._persist_cluster_state_locked()

                response = {
                    "vote_granted": vote_granted,
                    "term": self._current_term,
                    "node_id": self._node_id,
                }

        if apply_mode:
            self._apply_replica_mode()
        return response

    def trigger_election(self) -> Dict[str, Any]:
        """Start one leader-election attempt and return the outcome."""
        return self._start_election_once(auto=False)

    def build_status(self) -> Dict[str, Any]:
        """Return user-facing cluster status."""
        stats = self._coordinator.get_stats()
        current_sequence = stats.get("last_sequence", 0)
        read_only = self._coordinator.get_read_only_status()
        with self._lock:
            peers = [dict(state) for state in self._peer_states.values()]
            healthy_peers = sum(1 for peer in peers if peer.get("healthy"))
            lag_values = [
                peer["replication_lag"]
                for peer in peers
                if isinstance(peer.get("replication_lag"), int)
            ]
            max_lag = max(lag_values) if lag_values else 0
            quorum_commit_index = self._compute_quorum_commit_index_locked(current_sequence)
            return {
                "node_id": self._node_id,
                "role": self._role,
                "leader_id": self._leader_id,
                "leader_url": self._leader_url,
                "current_term": self._current_term,
                "voted_for": self._voted_for,
                "commit_index": current_sequence if self._role != "follower" else self._leader_commit_index,
                "last_applied": current_sequence,
                "quorum_size": self._quorum_size(),
                "read_only": read_only["read_only"],
                "read_only_reason": read_only["reason"],
                "replication_enabled": self._role != "standalone",
                "peer_count": len(peers),
                "healthy_peer_count": healthy_peers,
                "max_replication_lag": max_lag,
                "quorum_commit_index": quorum_commit_index,
                "quorum_commit_lag": (
                    max(0, current_sequence - quorum_commit_index)
                    if quorum_commit_index is not None
                    else None
                ),
                "write_quorum_ready": (1 + healthy_peers) >= self._quorum_size(),
                "require_write_quorum": self._require_write_quorum,
                "push_commit_replication": self._push_commit_replication,
                "last_leader_contact_at": self._last_leader_contact_at,
                "leader_contact_stale": self._leader_contact_stale_locked(),
                "last_sync_at": self._last_sync_at,
                "last_error": self._last_error,
                "peers": peers,
                "recent_apply": list(self._recent_apply_events),
            }

    def catch_up_once(self) -> int:
        """Catch a follower up to the leader's committed history once."""
        if self._role != "follower" or not self._leader_url:
            return 0

        current_sequence = self._coordinator.get_stats().get("last_sequence", 0)
        started_at = time.time()
        payload = self._request_json(
            f"{self._leader_url}/internal/replication/operations"
            f"?{parse.urlencode({'since_sequence': current_sequence, 'limit': self._batch_size})}",
            "GET",
            None,
        )
        operations = [Operation.from_dict(item) for item in payload.get("operations", [])]
        applied = self._coordinator.apply_replicated_operations(operations)
        applied_at = time.time()

        with self._lock:
            self._leader_id = payload.get("node_id") or self._leader_id
            payload_term = payload.get("term")
            if payload_term is not None:
                self._current_term = max(self._current_term, int(payload_term))
            self._leader_commit_index = int(payload.get("commit_index", self._leader_commit_index))
            self._last_sync_at = applied_at
            self._last_error = None
            self._last_leader_contact_at = applied_at
            self._persist_cluster_state_locked()
            if applied:
                last_sequence = operations[-1].sequence_number
                self._recent_apply_events.appendleft(
                    {
                        "sequence_number": last_sequence,
                        "peer_id": self._leader_id or self._leader_url,
                        "status": "applied",
                        "applied_at": applied_at,
                        "latency_ms": int((applied_at - started_at) * 1000),
                        "applied_count": applied,
                    }
                )
        return applied

    def _loop(self) -> None:
        """Background loop for follower catch-up and leader peer monitoring."""
        while True:
            with self._lock:
                if not self._running:
                    return
            try:
                if self._role == "follower":
                    if self._leader_url:
                        self.catch_up_once()
                    should_auto_elect = False
                    with self._lock:
                        should_auto_elect = self._role == "follower" and self._leader_contact_stale_locked()
                    if should_auto_elect:
                        self._start_election_once(auto=True)
                elif self._role == "candidate":
                    self._start_election_once(auto=False)
                elif self._role == "leader":
                    self._send_heartbeats_once()
                    self._poll_peers_once()
            except Exception as exc:
                with self._lock:
                    self._last_error = str(exc)
                logger.error("Cluster loop error: %s", exc)
            time.sleep(self._poll_interval_seconds)

    def _poll_peers_once(self) -> None:
        """Refresh leader-side visibility into follower health and lag."""
        current_sequence = self._coordinator.get_stats().get("last_sequence", 0)
        for peer in self._peer_urls:
            try:
                payload = self._request_json(f"{peer}/internal/replication/state", "GET", None)
                last_applied = int(payload.get("last_applied", 0))
                self._peer_states[peer] = {
                    "peer_url": peer,
                    "peer_id": payload.get("node_id"),
                    "role": payload.get("role", "unknown"),
                    "reachable": True,
                    "healthy": not payload.get("last_error"),
                    "last_applied": last_applied,
                    "replication_lag": max(0, current_sequence - last_applied),
                    "last_ack_at": time.time(),
                    "last_error": payload.get("last_error"),
                }
            except Exception as exc:
                self._peer_states[peer] = {
                    "peer_url": peer,
                    "peer_id": None,
                    "role": "unknown",
                    "reachable": False,
                    "healthy": False,
                    "last_applied": 0,
                    "replication_lag": None,
                    "last_ack_at": None,
                    "last_error": str(exc),
                }

    def _send_heartbeats_once(self) -> None:
        """Send heartbeats from the leader to all peers."""
        if self._role != "leader":
            return
        current_sequence = self._coordinator.get_stats().get("last_sequence", 0)
        for peer in self._peer_urls:
            try:
                payload = self._request_json(
                    f"{peer}/internal/cluster/heartbeat",
                    "POST",
                    {
                        "leader_id": self._node_id,
                        "leader_url": self._advertise_url,
                        "term": self._current_term,
                        "commit_index": current_sequence,
                    },
                )
                peer_term = int(payload.get("term", self._current_term))
                if peer_term > self._current_term:
                    self._become_follower(peer_term, payload.get("node_id"), peer)
                    return
            except Exception as exc:
                self._peer_states[peer] = {
                    "peer_url": peer,
                    "peer_id": None,
                    "role": "unknown",
                    "reachable": False,
                    "healthy": False,
                    "last_applied": 0,
                    "replication_lag": None,
                    "last_ack_at": None,
                    "last_error": str(exc),
                }

    def assert_write_quorum_available(self, current_sequence_override: Optional[int] = None) -> None:
        """Reject writes when the leader cannot reach a majority of peers."""
        if self._role != "leader" or not self._require_write_quorum:
            return
        self._poll_peers_once()
        current_sequence = (
            self._coordinator.get_stats().get("last_sequence", 0)
            if current_sequence_override is None
            else int(current_sequence_override)
        )
        with self._lock:
            healthy_peers = sum(1 for peer in self._peer_states.values() if peer.get("healthy"))
            if (1 + healthy_peers) < self._quorum_size():
                raise ConflictError(
                    "Write quorum unavailable: not enough healthy peers acknowledged replication health",
                    error="write_quorum_unavailable",
                )
            quorum_commit_index = self._compute_quorum_commit_index_locked(current_sequence)
            if quorum_commit_index is not None and quorum_commit_index < current_sequence:
                raise ConflictError(
                    "Write quorum unavailable: prior committed operations have not reached a majority yet",
                    error="write_quorum_behind",
                )

    def _start_election_once(self, auto: bool = False) -> Dict[str, Any]:
        """Run one election round and become leader on majority votes."""
        if self._role == "standalone":
            return {"status": "standalone", "term": self._current_term, "votes": 1}

        with self._lock:
            if auto:
                if self._role != "follower":
                    return {"status": self._role, "term": self._current_term, "votes": 0}
                if not self._leader_contact_stale_locked():
                    return {"status": "follower", "term": self._current_term, "votes": 0}
            self._role = "candidate"
            self._current_term += 1
            term = self._current_term
            self._voted_for = self._node_id
            self._leader_id = None
            self._leader_url = None
            self._last_error = None
            self._clear_prepare_reservation_locked()
            self._apply_replica_mode()
            self._persist_cluster_state_locked()
            candidate_last_applied = self._coordinator.get_stats().get("last_sequence", 0)

        votes = 1
        for peer in self._peer_urls:
            try:
                payload = self._request_json(
                    f"{peer}/internal/cluster/request-vote",
                    "POST",
                    {
                        "candidate_id": self._node_id,
                        "term": term,
                        "candidate_last_applied": candidate_last_applied,
                    },
                )
                peer_term = int(payload.get("term", term))
                if peer_term > term:
                    self._become_follower(peer_term, payload.get("node_id"), peer)
                    return {"status": "stepped_down", "term": peer_term, "votes": votes}
                if payload.get("vote_granted"):
                    votes += 1
            except Exception:
                continue

        if votes >= self._quorum_size():
            with self._lock:
                if self._current_term == term:
                    self._role = "leader"
                    self._leader_id = self._node_id
                    self._leader_url = self._advertise_url
                    self._last_leader_contact_at = time.time()
                    self._clear_prepare_reservation_locked()
                    self._apply_replica_mode()
                    self._sync_commit_callback_locked()
                    self._persist_cluster_state_locked()
            self._send_heartbeats_once()
            self._poll_peers_once()
            return {"status": "leader", "term": term, "votes": votes}

        with self._lock:
            self._last_error = f"Election failed in term {term}: {votes}/{self._quorum_size()} votes"
        return {"status": "candidate", "term": term, "votes": votes}

    def _on_leader_commit(self, operation: Operation) -> None:
        """Push newly committed operations to followers for lower replication lag."""
        if self._role != "leader" or not self._push_commit_replication or not self._peer_urls:
            return
        for peer in self._peer_urls:
            started_at = time.time()
            try:
                payload = self._request_json(
                    f"{peer}/internal/replication/apply",
                    "POST",
                    {
                        "source_node_id": self._node_id,
                        "source_term": self._current_term,
                        "prepared_write": self._require_write_quorum,
                        "operations": [operation.to_dict()],
                    },
                )
                last_applied = int(payload.get("last_applied", operation.sequence_number))
                applied_at = time.time()
                self._peer_states[peer] = {
                    "peer_url": peer,
                    "peer_id": payload.get("node_id"),
                    "role": "follower",
                    "reachable": True,
                    "healthy": True,
                    "last_applied": last_applied,
                    "replication_lag": max(0, self._coordinator.get_stats().get("last_sequence", 0) - last_applied),
                    "last_ack_at": applied_at,
                    "last_error": None,
                }
                self._recent_apply_events.appendleft(
                    {
                        "sequence_number": operation.sequence_number,
                        "peer_id": payload.get("node_id") or peer,
                        "status": "acked",
                        "applied_at": applied_at,
                        "latency_ms": int((applied_at - started_at) * 1000),
                        "applied_count": int(payload.get("applied_count", 1)),
                    }
                )
            except Exception as exc:
                self._peer_states[peer] = {
                    "peer_url": peer,
                    "peer_id": None,
                    "role": "unknown",
                    "reachable": False,
                    "healthy": False,
                    "last_applied": 0,
                    "replication_lag": None,
                    "last_ack_at": None,
                    "last_error": str(exc),
                }

    def _apply_replica_mode(self) -> None:
        """Align coordinator runtime behavior with cluster role."""
        if self._role in {"follower", "candidate"}:
            self._coordinator.set_read_only(
                True,
                "This node is not the active leader. Write to the leader instead.",
            )
            self._coordinator.disable_local_maintenance()
            self._coordinator.set_write_guard(None)
            self._coordinator.set_prepare_guard(None)
        else:
            self._coordinator.set_read_only(False)
            self._coordinator.set_write_guard(self.assert_write_quorum_available if self._require_write_quorum else None)
            self._coordinator.set_prepare_guard(self.prepare_write_quorum if self._require_write_quorum else None)
            self._coordinator.enable_local_maintenance()
        self._sync_commit_callback_locked()

    def _sync_commit_callback_locked(self) -> None:
        """Ensure leader-only commit callbacks track current role."""
        should_register = self._role == "leader" and self._push_commit_replication
        if should_register and not self._commit_callback_registered:
            self._coordinator.add_commit_callback(self._on_leader_commit)
            self._commit_callback_registered = True
        elif not should_register and self._commit_callback_registered:
            self._coordinator.remove_commit_callback(self._on_leader_commit)
            self._commit_callback_registered = False

    def _leader_contact_stale_locked(self) -> bool:
        """Return whether the follower has lost contact with its leader."""
        if self._role == "leader" or self._role == "standalone":
            return False
        if self._last_leader_contact_at is None:
            return True
        return (time.time() - self._last_leader_contact_at) > self._election_timeout_seconds

    def _clear_expired_prepare_locked(self) -> None:
        """Drop stale follower-side prepare reservations."""
        if self._prepared_expires_at is None:
            return
        if time.time() >= self._prepared_expires_at:
            self._clear_prepare_reservation_locked()

    def _clear_prepare_reservation_locked(self) -> None:
        """Clear any follower-side prepare reservation."""
        self._prepared_sequence = None
        self._prepared_count = 0
        self._prepared_term = None
        self._prepared_leader_id = None
        self._prepared_expires_at = None

    def _validate_prepare_reservation_locked(
        self,
        leader_id: Optional[str],
        term: Optional[int],
        operations: List[Operation],
    ) -> Optional[str]:
        """Return a rejection reason when a pushed batch does not match the reservation."""
        if self._prepared_sequence is None:
            return "prepare_missing"
        if leader_id != self._prepared_leader_id:
            self._clear_prepare_reservation_locked()
            return "leader_mismatch"
        if term is not None and self._prepared_term is not None and term != self._prepared_term:
            self._clear_prepare_reservation_locked()
            return "term_mismatch"

        next_sequence = self._prepared_sequence
        remaining = self._prepared_count
        for operation in sorted(operations, key=lambda item: item.sequence_number):
            if operation.sequence_number != next_sequence:
                self._clear_prepare_reservation_locked()
                return "sequence_mismatch"
            next_sequence += 1
            remaining -= 1
            if remaining < 0:
                self._clear_prepare_reservation_locked()
                return "prepared_count_exceeded"
        return None

    def _advance_prepare_reservation_locked(
        self,
        leader_id: Optional[str],
        term: Optional[int],
        operations: List[Operation],
    ) -> None:
        """Consume or invalidate a reserved sequence range as commits arrive."""
        self._clear_expired_prepare_locked()
        if (
            self._prepared_sequence is None
            or not operations
            or leader_id != self._prepared_leader_id
            or (term is not None and self._prepared_term is not None and term != self._prepared_term)
        ):
            return

        next_sequence = self._prepared_sequence
        remaining = self._prepared_count
        for operation in sorted(operations, key=lambda item: item.sequence_number):
            if operation.sequence_number != next_sequence:
                self._clear_prepare_reservation_locked()
                return
            next_sequence += 1
            remaining -= 1
            if remaining <= 0:
                self._clear_prepare_reservation_locked()
                return

        self._prepared_sequence = next_sequence
        self._prepared_count = remaining
        self._prepared_expires_at = time.time() + self._prepare_timeout_seconds

    def _cancel_prepare_reservations(
        self,
        *,
        peers: List[str],
        term: int,
        start_sequence: int,
        count: int,
    ) -> None:
        """Best-effort release of follower reservations after a failed prepare quorum."""
        for peer in peers:
            try:
                self._request_json(
                    f"{peer}/internal/replication/cancel-prepare",
                    "POST",
                    {
                        "leader_id": self._node_id,
                        "term": term,
                        "start_sequence": start_sequence,
                        "count": count,
                    },
                )
            except Exception:
                continue

    def _compute_quorum_commit_index_locked(self, current_sequence: int) -> Optional[int]:
        """Compute the highest sequence known to be present on a majority."""
        if self._role == "standalone":
            self._quorum_commit_index = current_sequence
            return current_sequence
        if self._role != "leader":
            return None

        acked_sequences: List[int] = [current_sequence]
        for peer in self._peer_urls:
            peer_state = self._peer_states.get(peer, {})
            last_applied = peer_state.get("last_applied")
            acked_sequences.append(int(last_applied) if isinstance(last_applied, int) else 0)

        acked_sequences.sort(reverse=True)
        quorum_index = acked_sequences[self._quorum_size() - 1] if len(acked_sequences) >= self._quorum_size() else 0
        self._quorum_commit_index = quorum_index
        return quorum_index

    def _persist_cluster_state_locked(self) -> None:
        """Persist durable election metadata for this node."""
        self._coordinator.save_cluster_state(
            node_id=self._node_id,
            current_term=self._current_term,
            voted_for=self._voted_for,
            leader_id=self._leader_id,
            leader_url=self._leader_url,
        )

    def _become_follower(
        self,
        term: int,
        leader_id: Optional[str],
        leader_url: Optional[str],
    ) -> None:
        """Transition into follower mode under a newer term or remote leader."""
        with self._prepare_barrier:
            with self._lock:
                if term > self._current_term:
                    self._current_term = term
                    self._voted_for = None
                self._role = "follower"
                self._leader_id = leader_id
                self._leader_url = leader_url.rstrip("/") if isinstance(leader_url, str) and leader_url else self._leader_url
                self._last_leader_contact_at = time.time()
                self._clear_prepare_reservation_locked()
                self._last_leader_contact_at = time.time()
                self._persist_cluster_state_locked()
        self._apply_replica_mode()

    def _quorum_size(self) -> int:
        """Return the majority quorum for the configured node count."""
        node_count = 1 + len(self._peer_urls)
        return (node_count // 2) + 1

    def _default_request_json(
        self,
        url: str,
        method: str,
        payload: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Perform a small JSON HTTP request with optional replication auth."""
        headers = {"Accept": "application/json"}
        data = None
        if self._replication_token:
            headers[REPLICATION_TOKEN_HEADER] = self._replication_token
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(url, method=method.upper(), data=data, headers=headers)
        try:
            with request.urlopen(req, timeout=3.0) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else {}
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{exc.code} {exc.reason}: {body}") from exc
        except error.URLError as exc:
            raise RuntimeError(str(exc.reason)) from exc
