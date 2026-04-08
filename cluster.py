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
        self._request_json = request_json or self._default_request_json

        self._lock = threading.RLock()
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

        if self._role == "leader":
            self._current_term = 1
            self._leader_id = self._node_id
            self._leader_url = self._advertise_url

        self._apply_replica_mode()

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
        operations_payload: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Apply a pushed replication batch on a follower."""
        operations = [Operation.from_dict(item) for item in operations_payload]
        started_at = time.time()
        applied = self._coordinator.apply_replicated_operations(operations)
        applied_at = time.time()
        if source_node_id:
            self._leader_id = source_node_id
            self._role = "follower"
        if operations:
            self._leader_commit_index = max(self._leader_commit_index, operations[-1].sequence_number)
        self._last_sync_at = applied_at
        self._last_error = None
        self._last_leader_contact_at = applied_at
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

    def receive_heartbeat(
        self,
        leader_id: str,
        term: int,
        leader_url: Optional[str],
        commit_index: int,
    ) -> Dict[str, Any]:
        """Accept or reject a leader heartbeat."""
        with self._lock:
            if term < self._current_term:
                return {
                    "accepted": False,
                    "term": self._current_term,
                    "node_id": self._node_id,
                    "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                }

            if term > self._current_term:
                self._current_term = term
                self._voted_for = None

            self._leader_id = leader_id
            self._leader_url = leader_url.rstrip("/") if leader_url else self._leader_url
            self._leader_commit_index = max(self._leader_commit_index, commit_index)
            self._last_leader_contact_at = time.time()
            if self._role != "standalone":
                self._role = "follower"
                self._apply_replica_mode()
                self._last_leader_contact_at = time.time()

            return {
                "accepted": True,
                "term": self._current_term,
                "node_id": self._node_id,
                "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
            }

    def request_vote(
        self,
        candidate_id: str,
        term: int,
        candidate_last_applied: int,
    ) -> Dict[str, Any]:
        """Evaluate a vote request from a candidate."""
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
                if self._role != "standalone":
                    self._role = "follower"
                    self._apply_replica_mode()

            local_last_applied = self._coordinator.get_stats().get("last_sequence", 0)
            vote_granted = (
                (self._voted_for is None or self._voted_for == candidate_id)
                and candidate_last_applied >= local_last_applied
            )
            if vote_granted:
                self._voted_for = candidate_id

            return {
                "vote_granted": vote_granted,
                "term": self._current_term,
                "node_id": self._node_id,
            }

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

    def assert_write_quorum_available(self) -> None:
        """Reject writes when the leader cannot reach a majority of peers."""
        if self._role != "leader" or not self._require_write_quorum:
            return
        self._poll_peers_once()
        healthy_peers = sum(1 for peer in self._peer_states.values() if peer.get("healthy"))
        if (1 + healthy_peers) < self._quorum_size():
            raise ConflictError(
                "Write quorum unavailable: not enough healthy peers acknowledged replication health",
                error="write_quorum_unavailable",
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
            self._apply_replica_mode()
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
                    self._apply_replica_mode()
                    self._sync_commit_callback_locked()
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
        else:
            self._coordinator.set_read_only(False)
            self._coordinator.set_write_guard(self.assert_write_quorum_available if self._require_write_quorum else None)
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

    def _become_follower(
        self,
        term: int,
        leader_id: Optional[str],
        leader_url: Optional[str],
    ) -> None:
        """Transition into follower mode under a newer term or remote leader."""
        with self._lock:
            if term > self._current_term:
                self._current_term = term
                self._voted_for = None
            self._role = "follower"
            self._leader_id = leader_id
            self._leader_url = leader_url.rstrip("/") if isinstance(leader_url, str) and leader_url else self._leader_url
            self._last_leader_contact_at = time.time()
            self._apply_replica_mode()
            self._last_leader_contact_at = time.time()

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
