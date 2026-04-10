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
from models import Operation, OperationType, WatchFireRecord
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
        self._leader_lease_until: Optional[float] = None
        self._config_version = 1
        self._previous_config_version: Optional[int] = None
        self._previous_peer_urls: List[str] = []
        self._pending_config_version: Optional[int] = None
        self._pending_peer_urls: List[str] = []
        self._reconfig_in_progress = False
        self._decommissioned = False
        self._reconfig_timeout_seconds = max(8.0, self._prepare_timeout_seconds * 2.0)
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
                "last_log_index": 0,
                "last_log_term": 0,
                "match_index": 0,
                "match_term": 0,
                "replication_lag": None,
                "last_ack_at": None,
                "last_error": None,
            }
            for peer in self._peer_urls
        }
        self._commit_callback_registered = False

        persisted_state = self._coordinator.load_cluster_state(self._node_id) or {}
        loaded_persisted_state = bool(persisted_state)
        self._current_term = int(persisted_state.get("current_term", self._current_term))
        self._voted_for = persisted_state.get("voted_for") or self._voted_for
        self._leader_id = persisted_state.get("leader_id") or self._leader_id
        self._config_version = int(persisted_state.get("config_version", self._config_version))
        previous_config_version = persisted_state.get("previous_config_version")
        self._previous_config_version = (
            None if previous_config_version is None else int(previous_config_version)
        )
        self._previous_peer_urls = self._normalize_peer_urls(
            persisted_state.get("previous_peer_urls", [])
        )
        self._pending_config_version = persisted_state.get("pending_config_version")
        self._pending_peer_urls = self._normalize_peer_urls(persisted_state.get("pending_peer_urls", []))
        self._reconfig_in_progress = bool(persisted_state.get("reconfig_in_progress", False))
        self._decommissioned = bool(persisted_state.get("decommissioned", False))
        self._leader_commit_index = int(
            persisted_state.get("commit_index", self._leader_commit_index)
        )
        self._quorum_commit_index = int(
            persisted_state.get("commit_index", self._quorum_commit_index)
        )
        if not self._leader_url:
            self._leader_url = persisted_state.get("leader_url") or self._leader_url
        persisted_peer_urls = persisted_state.get("peer_urls")
        if isinstance(persisted_peer_urls, list):
            self._peer_urls = self._normalize_peer_urls(persisted_peer_urls)
            self._peer_states = self._rebuild_peer_states(self._peer_urls, self._peer_states)

        if self._decommissioned:
            self._role = "decommissioned"
            self._leader_id = None
            self._leader_url = None
            self._leader_lease_until = None
            self._last_leader_contact_at = None
            self._voted_for = None

        should_bootstrap_leader = self._role == "leader" and not (
            loaded_persisted_state and self._peer_urls
        )

        if self._role == "leader" and not should_bootstrap_leader:
            self._role = "follower"
            self._leader_id = None
            self._leader_url = None
            self._leader_lease_until = None
            self._last_leader_contact_at = None

        if should_bootstrap_leader:
            self._current_term = max(1, self._current_term + 1)
            self._voted_for = self._node_id
            self._leader_id = self._node_id
            self._leader_url = self._advertise_url or self._leader_url
            self._leader_lease_until = time.time() + self._election_timeout_seconds

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
            if self._role == "decommissioned":
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
        batch = self._coordinator.export_replication_batch(
            since_sequence=since_sequence,
            limit=limit,
        )
        return {
            "node_id": self._node_id,
            "role": self._role,
            "term": self._current_term,
            "since_sequence": since_sequence,
            "commit_index": batch["commit_index"],
            "operations": batch["operations"],
            "watch_fires": batch["watch_fires"],
            "active_watch_commit_index": batch["active_watch_commit_index"],
            "active_watches": batch["active_watches"],
            "count": len(batch["operations"]),
        }

    def export_snapshot(self) -> Dict[str, Any]:
        """Expose a committed full-state snapshot for follower bootstrap/rejoin."""
        commit_index = self._coordinator.get_stats().get("last_sequence", 0)
        snapshot = self._coordinator.export_replica_snapshot()
        return {
            "node_id": self._node_id,
            "role": self._role,
            "term": self._current_term,
            "commit_index": commit_index,
            **snapshot,
        }

    def export_active_watches(self) -> Dict[str, Any]:
        """Expose the current active-watch mirror for follower parity sync."""
        return {
            "node_id": self._node_id,
            "role": self._role,
            "term": self._current_term,
            "commit_index": self._coordinator.get_stats().get("last_sequence", 0),
            "watches": self._coordinator.export_active_watches(),
        }

    def get_internal_state(self) -> Dict[str, Any]:
        """Return local replication state for leader peer monitoring."""
        stats = self._coordinator.get_stats()
        read_only = self._coordinator.get_read_only_status()
        last_log_index, last_log_term = self._coordinator.get_last_replication_position()
        return {
            "node_id": self._node_id,
            "role": self._role,
            "term": self._current_term,
            "config_version": self._config_version,
            "pending_config_version": self._pending_config_version,
            "pending_peer_urls": list(self._pending_peer_urls),
            "reconfig_in_progress": self._reconfig_in_progress,
            "commit_index": self._leader_commit_index if self._role == "follower" else stats.get("last_sequence", 0),
            "last_applied": stats.get("last_sequence", 0),
            "last_log_index": last_log_index,
            "last_log_term": last_log_term,
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
        config_version: int = 1,
        prepared_write: bool = False,
        watch_fires_payload: Optional[List[Dict[str, Any]]] = None,
        active_watch_commit_index: Optional[int] = None,
        active_watches_payload: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Apply a pushed replication batch on a follower."""
        operations = [Operation.from_dict(item) for item in operations_payload]
        watch_fires = [
            WatchFireRecord.from_dict(item)
            for item in (watch_fires_payload or [])
        ]
        started_at = time.time()
        with self._lock:
            self._clear_expired_prepare_locked()
            membership_reason = self._replication_membership_rejection_reason_locked(config_version)
            if membership_reason is not None:
                raise ConflictError(
                    "Replication apply rejected: cluster membership fence blocked the sender",
                    error=membership_reason,
                    config_version=self._config_version,
                )
            if source_term is not None and int(source_term) < self._current_term:
                raise ConflictError(
                    "Replication apply rejected: stale leader term",
                    error="replication_stale_term",
                )
            if (
                source_term is not None
                and int(source_term) == self._current_term
                and self._leader_id is not None
                and source_node_id not in {None, self._leader_id}
            ):
                raise ConflictError(
                    "Replication apply rejected: sender is not the active leader",
                    error="replication_stale_leader",
                )
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
        if watch_fires:
            self._coordinator.record_replicated_watch_fires(watch_fires)
        current_applied = self._coordinator.get_stats().get("last_sequence", 0)
        if (
            active_watch_commit_index is not None
            and int(active_watch_commit_index) == current_applied
        ):
            self._coordinator.restore_active_watches(active_watches_payload or [])
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
            "last_applied": current_applied,
            "node_id": self._node_id,
        }

    def receive_append(
        self,
        leader_id: str,
        term: int,
        leader_url: Optional[str],
        prev_log_index: int,
        prev_log_term: int,
        operations_payload: List[Dict[str, Any]],
        config_version: int = 1,
    ) -> Dict[str, Any]:
        """Durably append a leader batch without applying it yet."""
        operations = [Operation.from_dict(item) for item in operations_payload]
        if not operations:
            last_log_index, last_log_term = self._coordinator.get_last_replication_position()
            return {
                "accepted": True,
                "term": self._current_term,
                "node_id": self._node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term,
                "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
            }

        ordered = sorted(operations, key=lambda item: item.sequence_number)
        expected_start = int(prev_log_index) + 1
        apply_mode = False
        truncated = 0
        with self._prepare_barrier:
            with self._lock:
                local_last_log_index, local_last_log_term = self._coordinator.get_last_replication_position()
                local_last_applied = self._coordinator.get_stats().get("last_sequence", 0)
                membership_reason = self._replication_membership_rejection_reason_locked(config_version)
                if membership_reason is not None:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_log_index": local_last_log_index,
                        "last_log_term": local_last_log_term,
                        "last_applied": local_last_applied,
                        "reason": membership_reason,
                        "config_version": self._config_version,
                    }
                if term < self._current_term:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_log_index": local_last_log_index,
                        "last_log_term": local_last_log_term,
                        "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                        "reason": "stale_term",
                    }
                if term == self._current_term and self._voted_for not in {None, leader_id}:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_log_index": local_last_log_index,
                        "last_log_term": local_last_log_term,
                        "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                        "reason": "voted_for_other_leader",
                    }
                if ordered[0].sequence_number != expected_start:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_log_index": local_last_log_index,
                        "last_log_term": local_last_log_term,
                        "last_applied": local_last_applied,
                        "expected_next_log": expected_start,
                        "reason": "append_sequence_mismatch",
                    }
                if int(prev_log_index) < local_last_applied:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_log_index": local_last_log_index,
                        "last_log_term": local_last_log_term,
                        "last_applied": local_last_applied,
                        "reason": "prev_log_before_committed",
                    }
                if int(prev_log_index) > local_last_log_index:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_log_index": local_last_log_index,
                        "last_log_term": local_last_log_term,
                        "last_applied": local_last_applied,
                        "expected_prev_log_index": local_last_log_index,
                        "reason": "missing_prev_log",
                    }
                if int(prev_log_index) > 0:
                    previous_entries = self._coordinator.load_replication_operations_between(
                        int(prev_log_index),
                        int(prev_log_index),
                    )
                    if not previous_entries:
                        return {
                            "accepted": False,
                            "term": self._current_term,
                            "node_id": self._node_id,
                            "last_log_index": local_last_log_index,
                            "last_log_term": local_last_log_term,
                            "last_applied": local_last_applied,
                            "reason": "missing_prev_log",
                        }
                    local_prev_term = int(previous_entries[0].term or 0)
                    if local_prev_term != int(prev_log_term):
                        return {
                            "accepted": False,
                            "term": self._current_term,
                            "node_id": self._node_id,
                            "last_log_index": local_last_log_index,
                            "last_log_term": local_last_log_term,
                            "last_applied": local_last_applied,
                            "local_prev_term": local_prev_term,
                            "reason": "prev_log_term_mismatch",
                        }

                if term > self._current_term:
                    self._current_term = term
                    self._voted_for = leader_id
                    self._clear_prepare_reservation_locked()
                    self._leader_lease_until = None
                    if self._role != "standalone":
                        self._role = "follower"
                        apply_mode = True
                elif self._voted_for is None:
                    self._voted_for = leader_id

                overlap_end = min(local_last_log_index, ordered[-1].sequence_number)
                if overlap_end >= expected_start:
                    overlapping = self._coordinator.load_replication_operations_between(
                        expected_start,
                        overlap_end,
                    )
                    for index, incoming in enumerate(
                        ordered[: max(0, overlap_end - expected_start + 1)]
                    ):
                        existing = overlapping[index]
                        if existing != incoming or int(existing.term or 0) != int(incoming.term or term):
                            conflict_sequence = incoming.sequence_number
                            if conflict_sequence <= local_last_applied:
                                return {
                                    "accepted": False,
                                    "term": self._current_term,
                                    "node_id": self._node_id,
                                    "last_log_index": local_last_log_index,
                                    "last_log_term": local_last_log_term,
                                    "last_applied": local_last_applied,
                                    "conflict_sequence": conflict_sequence,
                                    "reason": "conflict_with_applied",
                                }
                            truncated += self._coordinator.truncate_replication_entries_after(
                                conflict_sequence - 1
                            )
                            local_last_log_index = conflict_sequence - 1
                            break

                if local_last_log_index > ordered[-1].sequence_number:
                    if ordered[-1].sequence_number < local_last_applied:
                        return {
                            "accepted": False,
                            "term": self._current_term,
                            "node_id": self._node_id,
                            "last_log_index": local_last_log_index,
                            "last_log_term": local_last_log_term,
                            "last_applied": local_last_applied,
                            "reason": "tail_conflict_with_applied",
                        }
                    truncated += self._coordinator.truncate_replication_entries_after(
                        ordered[-1].sequence_number
                    )

                self._coordinator.append_replication_entries(term, ordered)
                self._leader_id = leader_id
                self._leader_url = leader_url.rstrip("/") if leader_url else self._leader_url
                self._last_leader_contact_at = time.time()
                self._persist_cluster_state_locked()

                last_log_index, last_log_term = self._coordinator.get_last_replication_position()
                response = {
                    "accepted": True,
                    "term": self._current_term,
                    "node_id": self._node_id,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                    "last_applied": local_last_applied,
                    "truncated": truncated,
                }

        if apply_mode:
            self._apply_replica_mode()
        return response

    def receive_commit(
        self,
        leader_id: str,
        term: int,
        leader_url: Optional[str],
        commit_index: int,
        prev_log_index: int = 0,
        prev_log_term: int = 0,
        commit_term: Optional[int] = None,
        watch_fires_payload: Optional[List[Dict[str, Any]]] = None,
        active_watch_commit_index: Optional[int] = None,
        active_watches_payload: Optional[List[Dict[str, Any]]] = None,
        config_version: int = 1,
    ) -> Dict[str, Any]:
        """Advance a follower commit index and apply durable log entries."""
        apply_mode = False
        with self._prepare_barrier:
            with self._lock:
                membership_reason = self._replication_membership_rejection_reason_locked(config_version)
                if membership_reason is not None:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                        "reason": membership_reason,
                        "config_version": self._config_version,
                    }
                if term < self._current_term:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                        "reason": "stale_term",
                    }
                if term == self._current_term and self._voted_for not in {None, leader_id}:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                        "reason": "voted_for_other_leader",
                    }

                if term > self._current_term:
                    self._current_term = term
                    self._voted_for = leader_id
                    self._clear_prepare_reservation_locked()
                    self._leader_lease_until = None
                    if self._role != "standalone":
                        self._role = "follower"
                        apply_mode = True
                elif self._voted_for is None:
                    self._voted_for = leader_id

                self._leader_id = leader_id
                self._leader_url = leader_url.rstrip("/") if leader_url else self._leader_url
                self._leader_commit_index = max(self._leader_commit_index, int(commit_index))
                self._last_leader_contact_at = time.time()
                self._persist_cluster_state_locked()

        target_commit = int(commit_index)
        current_applied = self._coordinator.get_stats().get("last_sequence", 0)
        last_log_index, _ = self._coordinator.get_last_replication_position()
        if target_commit > last_log_index:
            return {
                "accepted": False,
                "term": self._current_term,
                "node_id": self._node_id,
                "last_applied": current_applied,
                "last_log_index": last_log_index,
                "reason": "commit_ahead_of_log",
            }

        if target_commit > 0 and commit_term is not None:
            if int(prev_log_index) > 0:
                previous_entries = self._coordinator.load_replication_operations_between(
                    int(prev_log_index),
                    int(prev_log_index),
                )
                if not previous_entries or int(previous_entries[0].term or 0) != int(prev_log_term):
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_applied": current_applied,
                        "last_log_index": last_log_index,
                        "reason": "commit_log_mismatch",
                    }
            commit_entries = self._coordinator.load_replication_operations_between(
                target_commit,
                target_commit,
            )
            if not commit_entries or int(commit_entries[0].term or 0) != int(commit_term):
                return {
                    "accepted": False,
                    "term": self._current_term,
                    "node_id": self._node_id,
                    "last_applied": current_applied,
                    "last_log_index": last_log_index,
                    "reason": "commit_log_mismatch",
                }

        watch_fires = [
            WatchFireRecord.from_dict(item)
            for item in (watch_fires_payload or [])
        ]
        if watch_fires and any(
            record.cause_sequence_number < (current_applied + 1)
            or record.cause_sequence_number > target_commit
            for record in watch_fires
        ):
            return {
                "accepted": False,
                "term": self._current_term,
                "node_id": self._node_id,
                "last_applied": current_applied,
                "last_log_index": last_log_index,
                "reason": "watch_fire_range_mismatch",
            }

        applied_count = 0
        if target_commit > current_applied:
            operations = self._coordinator.load_replication_operations_between(
                current_applied + 1,
                target_commit,
            )
            expected_count = target_commit - current_applied
            if len(operations) != expected_count:
                return {
                    "accepted": False,
                    "term": self._current_term,
                    "node_id": self._node_id,
                    "last_applied": current_applied,
                    "last_log_index": last_log_index,
                    "reason": "commit_gap",
                }
            applied_count = self._coordinator.apply_replicated_operations(operations)

        if watch_fires:
            self._coordinator.record_replicated_watch_fires(watch_fires)

        applied_at = time.time()
        current_applied = self._coordinator.get_stats().get("last_sequence", 0)
        if (
            active_watch_commit_index is not None
            and int(active_watch_commit_index) == current_applied
        ):
            self._coordinator.restore_active_watches(active_watches_payload or [])
        with self._lock:
            self._leader_commit_index = max(self._leader_commit_index, current_applied)
            self._coordinator.advance_cluster_cursors(
                node_id=self._node_id,
                commit_index=self._leader_commit_index,
                last_applied=current_applied,
            )
            self._last_sync_at = applied_at
            self._last_error = None
            self._last_leader_contact_at = applied_at
            self._persist_cluster_state_locked()
            if applied_count:
                self._recent_apply_events.appendleft(
                    {
                        "sequence_number": current_applied,
                        "peer_id": leader_id,
                        "status": "committed",
                        "applied_at": applied_at,
                        "latency_ms": 0,
                        "applied_count": applied_count,
                    }
                )

        if apply_mode:
            self._apply_replica_mode()
        return {
            "accepted": True,
            "term": self._current_term,
            "node_id": self._node_id,
            "last_applied": current_applied,
            "last_log_index": last_log_index,
            "applied_count": applied_count,
            }

    def receive_truncate(
        self,
        leader_id: str,
        term: int,
        truncate_after: int,
        config_version: int = 1,
    ) -> Dict[str, Any]:
        """Best-effort rollback of an uncommitted replicated-log tail."""
        with self._prepare_barrier:
            with self._lock:
                membership_reason = self._replication_membership_rejection_reason_locked(config_version)
                if membership_reason is not None:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "reason": membership_reason,
                        "config_version": self._config_version,
                    }
                if term < self._current_term:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "reason": "stale_term",
                    }
                if term == self._current_term and self._voted_for not in {None, leader_id}:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "reason": "voted_for_other_leader",
                    }
                if term > self._current_term:
                    self._current_term = term
                    self._voted_for = leader_id

                current_applied = self._coordinator.get_stats().get("last_sequence", 0)
                if int(truncate_after) < current_applied:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "reason": "truncate_before_applied",
                    }

                removed = self._coordinator.truncate_replication_entries_after(int(truncate_after))
                self._leader_id = leader_id
                self._last_leader_contact_at = time.time()
                self._persist_cluster_state_locked()
                return {
                    "accepted": True,
                    "term": self._current_term,
                    "node_id": self._node_id,
                    "removed": removed,
                    "last_applied": current_applied,
                }

    def receive_prepare(
        self,
        leader_id: str,
        term: int,
        leader_url: Optional[str],
        start_sequence: int,
        count: int = 1,
        config_version: int = 1,
    ) -> Dict[str, Any]:
        """Reserve the exact next committed sequence range for a leader."""
        if count < 1:
            raise ValueError("prepare count must be at least 1")

        with self._lock:
            self._clear_expired_prepare_locked()
            membership_reason = self._replication_membership_rejection_reason_locked(config_version)
            if membership_reason is not None:
                return {
                    "accepted": False,
                    "term": self._current_term,
                    "node_id": self._node_id,
                    "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                    "expected_next": self._coordinator.get_stats().get("last_sequence", 0) + 1,
                    "reason": membership_reason,
                    "config_version": self._config_version,
                }
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
        config_version: int = 1,
    ) -> Dict[str, Any]:
        """Best-effort release for a follower-side prepare reservation."""
        with self._lock:
            membership_reason = self._replication_membership_rejection_reason_locked(config_version)
            if membership_reason is not None:
                return {
                    "cancelled": False,
                    "term": self._current_term,
                    "node_id": self._node_id,
                    "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                    "reason": membership_reason,
                    "config_version": self._config_version,
                }
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
    ) -> Optional[Callable[[bool], None]]:
        """Append a write batch to a majority before local state-machine apply."""
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
        appended_peers: List[str] = []
        append_succeeded = False
        current_term = 0

        def finish_prepare(committed: bool) -> None:
            nonlocal released
            if not released:
                try:
                    if committed and append_succeeded:
                        commit_index = ordered[-1].sequence_number
                        watch_fire_records = self._coordinator.load_watch_fires_for_operations(
                            [operation.sequence_number for operation in ordered]
                        )
                        self._leader_commit_index = max(self._leader_commit_index, commit_index)
                        self._coordinator.advance_cluster_cursors(
                            node_id=self._node_id,
                            commit_index=commit_index,
                            last_applied=commit_index,
                        )
                        self._broadcast_commit(
                            peers=list(appended_peers),
                            term=current_term,
                            commit_index=commit_index,
                            prev_log_index=start_sequence - 1,
                            prev_log_term=batch_prev_log_term,
                            commit_term=int(ordered[-1].term or current_term),
                            watch_fire_records=watch_fire_records,
                        )
                    else:
                        truncate_after = start_sequence - 1
                        self._coordinator.truncate_replication_entries_after(truncate_after)
                        self._broadcast_truncate(
                            peers=appended_peers,
                            term=current_term,
                            truncate_after=truncate_after,
                        )
                finally:
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
            batch_prev_log_index, batch_prev_log_term = self._coordinator.get_last_replication_position()
            if batch_prev_log_index != start_sequence - 1:
                raise ConflictError(
                    "Write quorum append failed: local replicated log is not aligned with the next sequence",
                    error="write_quorum_log_misaligned",
                    last_log_index=batch_prev_log_index,
                    start_sequence=start_sequence,
                )

            self._coordinator.append_replication_entries(current_term, ordered)

            reserved_votes = 1
            for peer in self._peer_urls:
                started_at = time.time()
                try:
                    payload = self._request_json(
                        f"{peer}/internal/replication/append",
                        "POST",
                        {
                            "leader_id": self._node_id,
                            "leader_url": self._advertise_url,
                            "term": current_term,
                            "config_version": self._config_version,
                            "prev_log_index": batch_prev_log_index,
                            "prev_log_term": batch_prev_log_term,
                            "operations": [operation.to_dict() for operation in ordered],
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
                    peer_last_log_index = int(payload.get("last_log_index", 0))
                    peer_last_log_term = int(payload.get("last_log_term", 0))
                    self._peer_states[peer] = {
                        "peer_url": peer,
                        "peer_id": payload.get("node_id"),
                        "role": "follower",
                        "reachable": True,
                        "healthy": reserved,
                        "last_applied": last_applied,
                        "last_log_index": peer_last_log_index,
                        "last_log_term": peer_last_log_term,
                        "match_index": peer_last_log_index if reserved else 0,
                        "match_term": peer_last_log_term if reserved else 0,
                        "replication_lag": max(0, self._coordinator.get_stats().get("last_sequence", 0) - last_applied),
                        "last_ack_at": time.time(),
                        "last_error": None if reserved else payload.get("reason", "append_rejected"),
                    }
                    if reserved:
                        appended_peers.append(peer)
                        reserved_votes += 1
                    self._recent_apply_events.appendleft(
                        {
                            "sequence_number": ordered[-1].sequence_number,
                            "peer_id": payload.get("node_id") or peer,
                            "status": "appended" if reserved else "append_rejected",
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
                raise ConflictError(
                    "Write quorum append failed: majority could not durably append the next sequence range",
                    error="write_quorum_append_failed",
                    start_sequence=start_sequence,
                    prepared_votes=reserved_votes,
                    quorum_size=self._quorum_size(),
                )
            append_succeeded = True
            return finish_prepare
        except Exception:
            finish_prepare(False)
            raise

    def receive_heartbeat(
        self,
        leader_id: str,
        term: int,
        leader_url: Optional[str],
        commit_index: int,
        config_version: int = 1,
    ) -> Dict[str, Any]:
        """Accept or reject a leader heartbeat."""
        apply_mode = False
        with self._prepare_barrier:
            with self._lock:
                if self._decommissioned or self._role == "decommissioned":
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                        "reason": "decommissioned_node",
                    }
                if term < self._current_term:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                    }

                if not self._config_version_accepts(int(config_version)):
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "last_applied": self._coordinator.get_stats().get("last_sequence", 0),
                        "reason": "stale_config_version",
                        "config_version": self._config_version,
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
                self._leader_lease_until = None
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
                    "config_version": self._config_version,
                }

        if apply_mode:
            self._apply_replica_mode()
            with self._lock:
                self._last_leader_contact_at = time.time()
        self._sync_active_watches_from_leader()
        with self._lock:
            self._last_leader_contact_at = time.time()
        return response

    def request_vote(
        self,
        candidate_id: str,
        term: int,
        candidate_last_applied: int,
        candidate_last_log_index: int = 0,
        candidate_last_log_term: int = 0,
        candidate_config_version: int = 1,
    ) -> Dict[str, Any]:
        """Evaluate a vote request from a candidate."""
        apply_mode = False
        with self._prepare_barrier:
            with self._lock:
                if self._decommissioned or self._role == "decommissioned":
                    return {
                        "vote_granted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "reason": "decommissioned_node",
                    }
                if term < self._current_term:
                    return {
                        "vote_granted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                    }

                if not self._config_version_accepts(int(candidate_config_version)):
                    return {
                        "vote_granted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "reason": "stale_config_version",
                        "config_version": self._config_version,
                    }

                if self._reconfig_in_progress:
                    self._persist_cluster_state_locked()
                    return {
                        "vote_granted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "reason": "cluster_reconfig_in_progress",
                        "config_version": self._config_version,
                    }

                if term > self._current_term:
                    self._current_term = term
                    self._voted_for = None
                    self._leader_id = None
                    self._leader_url = None
                    self._leader_lease_until = None
                    self._clear_prepare_reservation_locked()
                    if self._role != "standalone":
                        self._role = "follower"
                        apply_mode = True

                local_last_applied = self._coordinator.get_stats().get("last_sequence", 0)
                local_last_log_index, local_last_log_term = self._coordinator.get_last_replication_position()
                candidate_log_is_fresh = (
                    int(candidate_last_log_term),
                    int(candidate_last_log_index or candidate_last_applied),
                ) >= (
                    int(local_last_log_term),
                    int(local_last_log_index or local_last_applied),
                )
                vote_granted = (
                    (self._voted_for is None or self._voted_for == candidate_id)
                    and candidate_log_is_fresh
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

    def reconfigure_cluster(self, expected_version: int, peer_urls: List[str]) -> Dict[str, Any]:
        """Stage and activate a conservative single-peer add/remove cluster reconfiguration."""
        desired_peer_urls = self._normalize_peer_urls(peer_urls)
        if not self._advertise_url:
            raise ConflictError(
                "Cluster reconfiguration requires this leader to advertise its own URL",
                error="cluster_reconfig_missing_advertise_url",
            )

        staged_peers: List[str] = []
        activated_peers: List[str] = []
        activation_started = False
        decommissioned_peer: Optional[str] = None
        with self._prepare_barrier:
            self._poll_peers_once()
            current_applied = self._coordinator.get_stats().get("last_sequence", 0)
            local_last_log_index, local_last_log_term = self._coordinator.get_last_replication_position()

            with self._lock:
                if self._role != "leader":
                    raise ConflictError(
                        "Cluster reconfiguration is only allowed on the active leader",
                        error="cluster_reconfig_not_leader",
                    )
                if self._reconfig_in_progress:
                    raise ConflictError(
                        "Cluster reconfiguration is already in progress",
                        error="cluster_reconfig_in_progress",
                    )
                if int(expected_version) != self._config_version:
                    raise ConflictError(
                        "Cluster configuration version mismatch",
                        error="cluster_reconfig_version_mismatch",
                        expected_version=expected_version,
                        current_version=self._config_version,
                    )
                if self._prepared_sequence is not None:
                    raise ConflictError(
                        "Cluster reconfiguration requires a quiet write path",
                        error="cluster_reconfig_prepare_active",
                    )
                if self._leader_lease_expired_locked():
                    raise ConflictError(
                        "Cluster reconfiguration rejected because leader quorum lease is expired",
                        error="leader_lease_expired",
                    )

                current_peers = list(self._peer_urls)
                additions = [peer for peer in desired_peer_urls if peer not in current_peers]
                removals = [peer for peer in current_peers if peer not in desired_peer_urls]
                add_mode = (
                    len(additions) == 1
                    and not removals
                    and len(desired_peer_urls) == (len(current_peers) + 1)
                )
                remove_mode = (
                    len(removals) == 1
                    and not additions
                    and len(current_peers) >= 2
                    and len(desired_peer_urls) == (len(current_peers) - 1)
                )
                if len(removals) == 1 and not additions and len(current_peers) < 2:
                    raise ConflictError(
                        "This reconfiguration path does not support collapsing to a single node",
                        error="cluster_reconfig_remove_requires_survivor_quorum",
                    )
                if not add_mode and not remove_mode:
                    raise ConflictError(
                        "This reconfiguration path only supports adding or removing one follower at a time",
                        error="cluster_reconfig_conservative_only",
                    )

                for peer in current_peers:
                    peer_state = self._peer_states.get(peer, {})
                    if not peer_state.get("healthy"):
                        raise ConflictError(
                            "Cluster reconfiguration requires every current peer to be healthy",
                            error="cluster_reconfig_peer_unhealthy",
                            peer_url=peer,
                        )
                    if int(peer_state.get("last_applied", 0) or 0) < current_applied:
                        raise ConflictError(
                            "Cluster reconfiguration requires every current peer to be fully applied",
                            error="cluster_reconfig_peer_behind",
                            peer_url=peer,
                        )
                    if int(peer_state.get("last_log_index", 0) or 0) != local_last_log_index:
                        raise ConflictError(
                            "Cluster reconfiguration requires every current peer to have the full durable log",
                            error="cluster_reconfig_peer_log_behind",
                            peer_url=peer,
                        )

                target_peer_url = additions[0] if add_mode else removals[0]
                next_version = self._config_version + 1
                self._pending_config_version = next_version
                self._pending_peer_urls = list(desired_peer_urls)
                self._reconfig_in_progress = True
                tracked_peers = desired_peer_urls if add_mode else current_peers
                self._peer_states = self._rebuild_peer_states(tracked_peers, self._peer_states)
                self._persist_cluster_state_locked()

            cluster_urls = [self._advertise_url] + list(desired_peer_urls)
            stage_targets = desired_peer_urls if add_mode else current_peers

            try:
                for peer in stage_targets:
                    self._request_json(
                        f"{peer}/internal/cluster/configure",
                        "POST",
                        {
                            "leader_id": self._node_id,
                            "leader_url": self._advertise_url,
                            "term": self._current_term,
                            "config_version": next_version,
                            "cluster_urls": cluster_urls,
                            "node_url": peer,
                            "phase": "stage",
                        },
                    )
                    staged_peers.append(peer)

                if add_mode:
                    self._wait_for_peer_alignment(
                        peer_url=target_peer_url,
                        expected_applied=current_applied,
                        expected_log_index=local_last_log_index,
                        expected_log_term=local_last_log_term,
                    )

                activation_started = True
                for peer in desired_peer_urls:
                    self._request_json(
                        f"{peer}/internal/cluster/configure",
                        "POST",
                        {
                            "leader_id": self._node_id,
                            "leader_url": self._advertise_url,
                            "term": self._current_term,
                            "config_version": next_version,
                            "cluster_urls": cluster_urls,
                            "node_url": peer,
                            "phase": "activate",
                        },
                    )
                    activated_peers.append(peer)

                if remove_mode:
                    self._request_json(
                        f"{target_peer_url}/internal/cluster/configure",
                        "POST",
                        {
                            "leader_id": self._node_id,
                            "leader_url": self._advertise_url,
                            "term": self._current_term,
                            "config_version": next_version,
                            "cluster_urls": cluster_urls,
                            "node_url": target_peer_url,
                            "phase": "decommission",
                        },
                    )
                    decommissioned_peer = target_peer_url

                with self._lock:
                    self._previous_config_version = self._config_version
                    self._previous_peer_urls = list(self._peer_urls)
                    self._peer_urls = list(desired_peer_urls)
                    self._peer_states = self._rebuild_peer_states(self._peer_urls, self._peer_states)
                    self._config_version = next_version
                    self._pending_config_version = None
                    self._pending_peer_urls = []
                    self._reconfig_in_progress = False
                    self._persist_cluster_state_locked()
            except Exception as exc:
                if not activation_started:
                    if staged_peers:
                        for peer in staged_peers:
                            try:
                                self._request_json(
                                    f"{peer}/internal/cluster/configure",
                                    "POST",
                                    {
                                        "leader_id": self._node_id,
                                        "leader_url": self._advertise_url,
                                        "term": self._current_term,
                                        "config_version": next_version,
                                        "cluster_urls": cluster_urls,
                                        "node_url": peer,
                                        "phase": "abort",
                                    },
                                )
                            except Exception:
                                continue
                    with self._lock:
                        self._pending_config_version = None
                        self._pending_peer_urls = []
                        self._reconfig_in_progress = False
                        self._peer_states = self._rebuild_peer_states(self._peer_urls, self._peer_states)
                        self._last_error = str(exc)
                        self._persist_cluster_state_locked()
                    if isinstance(exc, ConflictError):
                        raise
                    raise ConflictError(
                        "Cluster reconfiguration failed during staging",
                        error="cluster_reconfig_failed",
                        detail=str(exc),
                    ) from exc

                rollback_failures: List[str] = []
                staged_only_peers = [
                    peer for peer in staged_peers
                    if peer not in activated_peers and peer != decommissioned_peer
                ]
                for peer in staged_only_peers:
                    try:
                        self._request_json(
                            f"{peer}/internal/cluster/configure",
                            "POST",
                            {
                                "leader_id": self._node_id,
                                "leader_url": self._advertise_url,
                                "term": self._current_term,
                                "config_version": next_version,
                                "cluster_urls": cluster_urls,
                                "node_url": peer,
                                "phase": "abort",
                            },
                        )
                    except Exception:
                        rollback_failures.append(peer)
                if decommissioned_peer is not None:
                    try:
                        self._request_json(
                            f"{decommissioned_peer}/internal/cluster/configure",
                            "POST",
                            {
                                "leader_id": self._node_id,
                                "leader_url": self._advertise_url,
                                "term": self._current_term,
                                "config_version": next_version,
                                "cluster_urls": cluster_urls,
                                "node_url": decommissioned_peer,
                                "phase": "rollback",
                            },
                        )
                    except Exception:
                        rollback_failures.append(decommissioned_peer)
                for peer in reversed(activated_peers):
                    try:
                        self._request_json(
                            f"{peer}/internal/cluster/configure",
                            "POST",
                            {
                                "leader_id": self._node_id,
                                "leader_url": self._advertise_url,
                                "term": self._current_term,
                                "config_version": next_version,
                                "cluster_urls": cluster_urls,
                                "node_url": peer,
                                "phase": "rollback",
                            },
                        )
                    except Exception:
                        rollback_failures.append(peer)

                if not rollback_failures:
                    with self._lock:
                        self._pending_config_version = None
                        self._pending_peer_urls = []
                        self._reconfig_in_progress = False
                        self._peer_states = self._rebuild_peer_states(self._peer_urls, self._peer_states)
                        self._last_error = f"cluster reconfiguration activation failed and rolled back: {exc}"
                        self._persist_cluster_state_locked()
                    raise ConflictError(
                        "Cluster reconfiguration activation failed and was rolled back",
                        error="cluster_reconfig_activation_rolled_back",
                        detail=str(exc),
                    ) from exc

                with self._lock:
                    self._last_error = (
                        f"cluster reconfiguration activation failed: {exc}; "
                        f"rollback failed for {', '.join(rollback_failures)}"
                    )
                    self._persist_cluster_state_locked()
                self._step_down_to_candidate("Cluster reconfiguration activation rollback failed")
                raise ConflictError(
                    "Cluster reconfiguration activation failed and rollback was incomplete",
                    error="cluster_reconfig_activation_failed",
                    detail=str(exc),
                    rollback_failures=rollback_failures,
                ) from exc

        return self.build_status()

    def receive_configure(
        self,
        *,
        leader_id: str,
        leader_url: Optional[str],
        term: int,
        config_version: int,
        cluster_urls: List[str],
        node_url: str,
        phase: str,
    ) -> Dict[str, Any]:
        """Stage, activate, decommission, or roll back one conservative cluster configuration change."""
        normalized_node_url = node_url.rstrip("/")
        normalized_cluster_urls = self._normalize_peer_urls(cluster_urls)
        desired_peer_urls = [
            url
            for url in normalized_cluster_urls
            if url != normalized_node_url
        ]
        phase_value = (phase or "").strip().lower()
        apply_mode = False
        refresh_mode = False

        with self._prepare_barrier:
            with self._lock:
                if term < self._current_term:
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "reason": "stale_term",
                    }

                if phase_value not in {"stage", "activate", "abort", "rollback", "decommission"}:
                    raise ValueError(f"Unsupported configure phase: {phase}")

                if term > self._current_term:
                    self._current_term = term
                    self._voted_for = leader_id
                    self._leader_lease_until = None
                    self._clear_prepare_reservation_locked()
                    if self._role not in {"standalone", "decommissioned"}:
                        self._role = "follower"
                        apply_mode = True
                elif (
                    phase_value != "stage"
                    and not self._config_version_accepts(int(config_version))
                ):
                    return {
                        "accepted": False,
                        "term": self._current_term,
                        "node_id": self._node_id,
                        "reason": "stale_config_version",
                        "config_version": self._config_version,
                    }

                self._leader_id = leader_id
                self._leader_url = leader_url.rstrip("/") if leader_url else self._leader_url
                self._last_leader_contact_at = time.time()

                if phase_value == "stage":
                    self._decommissioned = False
                    self._previous_config_version = self._config_version
                    self._previous_peer_urls = list(self._peer_urls)
                    self._pending_config_version = int(config_version)
                    self._pending_peer_urls = list(desired_peer_urls)
                    self._reconfig_in_progress = True
                    if self._role != "leader" or leader_id != self._node_id:
                        self._role = "follower"
                        apply_mode = True
                        refresh_mode = True
                    self._persist_cluster_state_locked()
                elif phase_value == "activate":
                    if (
                        self._pending_config_version != int(config_version)
                        or self._pending_peer_urls != list(desired_peer_urls)
                    ):
                        return {
                            "accepted": False,
                            "term": self._current_term,
                            "node_id": self._node_id,
                            "reason": "config_not_staged",
                            "config_version": self._config_version,
                        }
                    if self._previous_config_version is None:
                        self._previous_config_version = self._config_version
                        self._previous_peer_urls = list(self._peer_urls)
                    self._decommissioned = False
                    self._peer_urls = list(desired_peer_urls)
                    self._peer_states = self._rebuild_peer_states(self._peer_urls, self._peer_states)
                    self._config_version = int(config_version)
                    self._pending_config_version = None
                    self._pending_peer_urls = []
                    self._reconfig_in_progress = False
                    if self._role != "leader" or leader_id != self._node_id:
                        self._role = "follower"
                        apply_mode = True
                        refresh_mode = True
                    self._persist_cluster_state_locked()
                elif phase_value == "decommission":
                    if (
                        self._pending_config_version != int(config_version)
                        or self._pending_peer_urls != list(desired_peer_urls)
                    ):
                        return {
                            "accepted": False,
                            "term": self._current_term,
                            "node_id": self._node_id,
                            "reason": "config_not_staged",
                            "config_version": self._config_version,
                        }
                    if self._previous_config_version is None:
                        self._previous_config_version = self._config_version
                        self._previous_peer_urls = list(self._peer_urls)
                    self._config_version = int(config_version)
                    self._peer_urls = []
                    self._peer_states = self._rebuild_peer_states(self._peer_urls, self._peer_states)
                    self._pending_config_version = None
                    self._pending_peer_urls = []
                    self._reconfig_in_progress = False
                    self._decommissioned = True
                    self._voted_for = None
                    self._leader_id = None
                    self._leader_url = None
                    self._leader_lease_until = None
                    self._last_leader_contact_at = None
                    self._role = "decommissioned"
                    apply_mode = True
                    self._persist_cluster_state_locked()
                else:
                    staged_matches = self._pending_config_version == int(config_version)
                    active_matches = (
                        phase_value == "rollback"
                        and self._config_version == int(config_version)
                        and self._previous_config_version is not None
                    )
                    if not staged_matches and not active_matches:
                        return {
                            "accepted": False,
                            "term": self._current_term,
                            "node_id": self._node_id,
                            "reason": "config_not_staged",
                            "config_version": self._config_version,
                        }

                    if active_matches:
                        self._config_version = int(self._previous_config_version or self._config_version)
                        self._peer_urls = list(self._previous_peer_urls)
                        self._peer_states = self._rebuild_peer_states(self._peer_urls, self._peer_states)

                    self._pending_config_version = None
                    self._pending_peer_urls = []
                    self._reconfig_in_progress = False
                    self._decommissioned = False

                    if self._role != "leader" or leader_id != self._node_id:
                        if not self._peer_urls:
                            self._role = "standalone"
                            self._leader_id = None
                            self._leader_url = None
                        else:
                            self._role = "follower"
                            apply_mode = True
                        refresh_mode = True

                    self._previous_config_version = None
                    self._previous_peer_urls = []
                    self._persist_cluster_state_locked()

        if apply_mode:
            self._apply_replica_mode()
        if refresh_mode:
            if self._role != "standalone" or self._peer_urls or self._leader_url:
                self.start()
        return {
            "accepted": True,
            "term": self._current_term,
            "node_id": self._node_id,
            "config_version": self._config_version,
            "pending_config_version": self._pending_config_version,
            "reconfig_in_progress": self._reconfig_in_progress,
        }

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
            leader_lease_expired = self._leader_lease_expired_locked()
            config_peer_sets = self._config_peer_sets_locked()
            write_quorum_ready = not self._reconfig_in_progress and all(
                self._healthy_node_count_for_peer_urls_locked(peer_urls)
                >= self._quorum_size_for_peer_urls_locked(peer_urls)
                for peer_urls in config_peer_sets
            )
            if self._role == "leader" and self._peer_urls:
                write_quorum_ready = write_quorum_ready and not leader_lease_expired
            return {
                "node_id": self._node_id,
                "role": self._role,
                "leader_id": self._leader_id,
                "leader_url": self._leader_url,
                "current_term": self._current_term,
                "config_version": self._config_version,
                "pending_config_version": self._pending_config_version,
                "pending_peer_urls": list(self._pending_peer_urls),
                "reconfig_in_progress": self._reconfig_in_progress,
                "decommissioned": self._decommissioned,
                "voted_for": self._voted_for,
                "commit_index": current_sequence if self._role != "follower" else self._leader_commit_index,
                "last_applied": current_sequence,
                "quorum_size": max(
                    self._quorum_size_for_peer_urls_locked(peer_urls)
                    for peer_urls in config_peer_sets
                ),
                "read_only": read_only["read_only"],
                "read_only_reason": read_only["reason"],
                "replication_enabled": self._role not in {"standalone", "decommissioned"},
                "peer_count": len(peers),
                "healthy_peer_count": healthy_peers,
                "max_replication_lag": max_lag,
                "quorum_commit_index": quorum_commit_index,
                "quorum_commit_lag": (
                    max(0, current_sequence - quorum_commit_index)
                    if quorum_commit_index is not None
                    else None
                ),
                "write_quorum_ready": write_quorum_ready,
                "require_write_quorum": self._require_write_quorum,
                "push_commit_replication": self._push_commit_replication,
                "last_leader_contact_at": self._last_leader_contact_at,
                "leader_lease_until": self._leader_lease_until,
                "leader_lease_expired": leader_lease_expired,
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
        request_since = max(0, current_sequence - 1) if current_sequence > 0 else 0
        request_limit = self._batch_size + (1 if current_sequence > 0 else 0)
        started_at = time.time()
        payload = self._request_json(
            f"{self._leader_url}/internal/replication/operations"
            f"?{parse.urlencode({'since_sequence': request_since, 'limit': request_limit})}",
            "GET",
            None,
        )
        operations = [Operation.from_dict(item) for item in payload.get("operations", [])]
        watch_fires = [
            WatchFireRecord.from_dict(item)
            for item in payload.get("watch_fires", [])
        ]
        active_watch_commit_index = payload.get("active_watch_commit_index")
        active_watches_payload = payload.get("active_watches", [])
        leader_commit_index = int(payload.get("commit_index", 0))
        if current_sequence > leader_commit_index:
            return self._resync_from_leader_snapshot(reason="local_ahead_of_leader")

        operations_to_apply = list(operations)
        if current_sequence > 0:
            local_operation = self._coordinator.get_operation(current_sequence)
            overlap_operation = next(
                (operation for operation in operations if operation.sequence_number == current_sequence),
                None,
            )
            if local_operation is None or overlap_operation is None or local_operation != overlap_operation:
                return self._resync_from_leader_snapshot(reason="history_mismatch")
            operations_to_apply = [
                operation
                for operation in operations
                if operation.sequence_number > current_sequence
            ]

        local_last_log_index, _ = self._coordinator.get_last_replication_position()
        log_operations = [
            operation
            for operation in operations
            if operation.sequence_number > local_last_log_index
        ]
        if log_operations:
            leader_term_hint = payload.get("term")
            self._coordinator.append_replication_entries(
                int(leader_term_hint or 0),
                log_operations,
            )

        try:
            applied = self._coordinator.apply_replicated_operations(operations_to_apply)
        except (KeyError, ValueError):
            return self._resync_from_leader_snapshot(reason="apply_conflict")
        if watch_fires:
            self._coordinator.record_replicated_watch_fires(watch_fires)
        target_sequence = (
            operations_to_apply[-1].sequence_number
            if operations_to_apply
            else current_sequence
        )
        if (
            active_watch_commit_index is not None
            and int(active_watch_commit_index) == int(target_sequence)
        ):
            self._coordinator.restore_active_watches(active_watches_payload or [])
        applied_at = time.time()

        with self._lock:
            self._leader_id = payload.get("node_id") or self._leader_id
            payload_term = payload.get("term")
            if payload_term is not None:
                self._current_term = max(self._current_term, int(payload_term))
            self._leader_commit_index = leader_commit_index or self._leader_commit_index
            self._last_sync_at = applied_at
            self._last_error = None
            self._last_leader_contact_at = applied_at
            self._persist_cluster_state_locked()
            if applied and operations_to_apply:
                last_sequence = operations_to_apply[-1].sequence_number
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

    def _resync_from_leader_snapshot(self, reason: str) -> int:
        """Clear a divergent follower and rebuild it from a leader snapshot."""
        if self._role != "follower" or not self._leader_url:
            return 0

        started_at = time.time()
        payload = self._request_json(
            f"{self._leader_url}/internal/replication/snapshot",
            "GET",
            None,
        )
        summary = self._coordinator.restore_replica_snapshot(payload)
        leader_id = payload.get("node_id")
        payload_term = payload.get("term")
        leader_term = int(payload_term) if payload_term is not None else None
        leader_commit_index = int(payload.get("commit_index", summary["last_sequence"]))
        total_applied = summary["operations"]

        applied_at = time.time()
        with self._lock:
            self._leader_id = leader_id or self._leader_id
            if leader_term is not None:
                self._current_term = max(self._current_term, leader_term)
            self._leader_commit_index = leader_commit_index
            self._last_sync_at = applied_at
            self._last_error = None
            self._last_leader_contact_at = applied_at
            self._persist_cluster_state_locked()
            self._recent_apply_events.appendleft(
                {
                    "sequence_number": summary["last_sequence"],
                    "peer_id": self._leader_id or self._leader_url,
                    "status": "snapshotted",
                    "applied_at": applied_at,
                    "latency_ms": int((applied_at - started_at) * 1000),
                    "applied_count": total_applied,
                    "reason": reason,
                }
            )
        return total_applied

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
        with self._lock:
            poll_targets = self._reconfig_target_peer_urls_locked()
        for peer in poll_targets:
            try:
                payload = self._request_json(f"{peer}/internal/replication/state", "GET", None)
                previous = self._peer_states.get(peer, {})
                last_applied = int(payload.get("last_applied", 0))
                last_log_index = int(payload.get("last_log_index", last_applied))
                last_log_term = int(payload.get("last_log_term", previous.get("last_log_term", 0) or 0))
                self._peer_states[peer] = {
                    "peer_url": peer,
                    "peer_id": payload.get("node_id"),
                    "role": payload.get("role", "unknown"),
                    "reachable": True,
                    "healthy": not payload.get("last_error"),
                    "last_applied": last_applied,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                    "match_index": last_log_index,
                    "match_term": last_log_term,
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
                    "last_log_index": 0,
                    "last_log_term": 0,
                    "match_index": 0,
                    "match_term": 0,
                    "replication_lag": None,
                    "last_ack_at": None,
                    "last_error": str(exc),
                }

    def _send_heartbeats_once(self) -> None:
        """Send heartbeats from the leader to all peers."""
        if self._role != "leader":
            return
        current_sequence = self._coordinator.get_stats().get("last_sequence", 0)
        accepted_votes = 1
        with self._lock:
            heartbeat_targets = self._reconfig_target_peer_urls_locked()
        for peer in heartbeat_targets:
            try:
                payload = self._request_json(
                    f"{peer}/internal/cluster/heartbeat",
                    "POST",
                    {
                        "leader_id": self._node_id,
                        "leader_url": self._advertise_url,
                        "term": self._current_term,
                        "commit_index": current_sequence,
                        "config_version": self._config_version,
                    },
                )
                peer_term = int(payload.get("term", self._current_term))
                if peer_term > self._current_term:
                    self._become_follower(peer_term, payload.get("node_id"), peer)
                    return
                accepted = bool(payload.get("accepted"))
                last_applied = int(payload.get("last_applied", 0))
                previous = self._peer_states.get(peer, {})
                last_log_index = int(payload.get("last_log_index", previous.get("last_log_index", last_applied) or last_applied))
                last_log_term = int(payload.get("last_log_term", previous.get("last_log_term", 0) or 0))
                if accepted:
                    accepted_votes += 1
                self._peer_states[peer] = {
                    "peer_url": peer,
                    "peer_id": payload.get("node_id"),
                    "role": "follower" if accepted else "unknown",
                    "reachable": True,
                    "healthy": accepted,
                    "last_applied": last_applied,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                    "match_index": last_log_index if accepted else int(previous.get("match_index", 0) or 0),
                    "match_term": last_log_term if accepted else int(previous.get("match_term", 0) or 0),
                    "replication_lag": max(0, current_sequence - last_applied),
                    "last_ack_at": time.time() if accepted else None,
                    "last_error": None if accepted else "heartbeat_rejected",
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
        should_step_down = False
        with self._lock:
            quorum_sets = self._config_peer_sets_locked()
            has_joint_majority = all(
                self._healthy_node_count_for_peer_urls_locked(peer_urls)
                >= self._quorum_size_for_peer_urls_locked(peer_urls)
                for peer_urls in quorum_sets
            )
            if has_joint_majority:
                self._leader_lease_until = time.time() + self._election_timeout_seconds
            elif self._leader_lease_expired_locked():
                should_step_down = True
        if should_step_down:
            self._step_down_to_candidate("Leader lease expired: majority heartbeat quorum lost")

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
        should_step_down = False
        with self._lock:
            if self._reconfig_in_progress:
                raise ConflictError(
                    "Write quorum unavailable: cluster reconfiguration is in progress",
                    error="cluster_reconfig_in_progress",
                )
            if self._leader_lease_expired_locked():
                should_step_down = True
            healthy_peers = sum(1 for peer in self._peer_states.values() if peer.get("healthy"))
            if not should_step_down and (1 + healthy_peers) < self._quorum_size():
                raise ConflictError(
                    "Write quorum unavailable: not enough healthy peers acknowledged replication health",
                    error="write_quorum_unavailable",
                )
            quorum_commit_index = self._compute_quorum_commit_index_locked(current_sequence)
            if (
                not should_step_down
                and quorum_commit_index is not None
                and quorum_commit_index < current_sequence
            ):
                raise ConflictError(
                    "Write quorum unavailable: prior committed operations have not reached a majority yet",
                    error="write_quorum_behind",
                )
        if should_step_down:
            self._step_down_to_candidate("Leader lease expired: majority heartbeat quorum lost")
            raise ConflictError(
                "Write quorum unavailable: leader lease expired",
                error="leader_lease_expired",
            )

    def assert_cluster_write_allowed(self) -> None:
        """Reject leader writes that violate cluster-level safety fences."""
        with self._lock:
            if self._role == "leader" and self._reconfig_in_progress:
                raise ConflictError(
                    "Write quorum unavailable: cluster reconfiguration is in progress",
                    error="cluster_reconfig_in_progress",
                )
        self.assert_write_quorum_available()

    def _start_election_once(self, auto: bool = False) -> Dict[str, Any]:
        """Run one election round and become leader on majority votes."""
        if self._role == "standalone":
            return {"status": "standalone", "term": self._current_term, "votes": 1}
        if self._role == "decommissioned":
            return {"status": "decommissioned", "term": self._current_term, "votes": 0}

        with self._lock:
            if self._reconfig_in_progress:
                return {"status": "reconfiguring", "term": self._current_term, "votes": 0}
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
            self._leader_lease_until = None
            self._last_error = None
            self._clear_prepare_reservation_locked()
            self._apply_replica_mode()
            self._persist_cluster_state_locked()
            candidate_last_applied = self._coordinator.get_stats().get("last_sequence", 0)
            candidate_last_log_index, candidate_last_log_term = self._coordinator.get_last_replication_position()

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
                        "candidate_last_log_index": candidate_last_log_index,
                        "candidate_last_log_term": candidate_last_log_term,
                        "candidate_config_version": self._config_version,
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
                    self._leader_lease_until = time.time() + self._election_timeout_seconds
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
        watch_fire_records = self._coordinator.load_watch_fires_for_operations(
            [operation.sequence_number]
        )
        active_watch_sync = self._build_active_watch_sync_payload(
            operations=[operation],
            watch_fire_records=watch_fire_records,
            commit_index=operation.sequence_number,
        )
        for peer in self._peer_urls:
            started_at = time.time()
            try:
                payload = self._request_json(
                    f"{peer}/internal/replication/apply",
                    "POST",
                    {
                        "source_node_id": self._node_id,
                        "source_term": self._current_term,
                        "config_version": self._config_version,
                        "prepared_write": self._require_write_quorum,
                        "operations": [operation.to_dict()],
                        "watch_fires": [record.to_dict() for record in watch_fire_records],
                        "active_watch_commit_index": active_watch_sync["active_watch_commit_index"],
                        "active_watches": active_watch_sync["active_watches"],
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
        if self._role in {"follower", "candidate", "decommissioned"}:
            reason = "This node is not the active leader. Write to the leader instead."
            if self._role == "decommissioned":
                reason = "This node has been decommissioned and cannot accept cluster writes."
            self._coordinator.set_read_only(
                True,
                reason,
            )
            self._coordinator.disable_local_maintenance()
            self._coordinator.set_write_guard(None)
            self._coordinator.set_prepare_guard(None)
        else:
            self._coordinator.set_read_only(False)
            self._coordinator.set_write_guard(
                self.assert_cluster_write_allowed if self._role == "leader" else None
            )
            self._coordinator.set_prepare_guard(
                self.prepare_write_quorum if self._role == "leader" and self._require_write_quorum else None
            )
            self._coordinator.enable_local_maintenance()
        self._sync_commit_callback_locked()

    def _sync_commit_callback_locked(self) -> None:
        """Ensure leader-only commit callbacks track current role."""
        should_register = (
            self._role == "leader"
            and self._push_commit_replication
            and not self._require_write_quorum
        )
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

    def _leader_lease_expired_locked(self) -> bool:
        """Return whether the current leader has lost its majority heartbeat lease."""
        if self._role != "leader" or not self._peer_urls:
            return False
        if self._leader_lease_until is None:
            return True
        return time.time() >= self._leader_lease_until

    def _step_down_to_candidate(self, reason: str) -> None:
        """Drop leadership when quorum freshness is lost so writes stop immediately."""
        with self._prepare_barrier:
            with self._lock:
                if self._role != "leader":
                    return
                self._role = "candidate"
                self._leader_id = None
                self._leader_url = None
                self._leader_lease_until = None
                self._last_error = reason
                self._clear_prepare_reservation_locked()
                self._persist_cluster_state_locked()
        self._apply_replica_mode()

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
                        "config_version": self._config_version,
                        "start_sequence": start_sequence,
                        "count": count,
                    },
                )
            except Exception:
                continue

    def _broadcast_commit(
        self,
        *,
        peers: List[str],
        term: int,
        commit_index: int,
        prev_log_index: int,
        prev_log_term: int,
        commit_term: int,
        watch_fire_records: Optional[List[WatchFireRecord]] = None,
    ) -> None:
        """Tell followers to apply all appended entries through one commit index."""
        commit_operations = self._coordinator.load_replication_operations_between(
            max(1, int(prev_log_index) + 1),
            commit_index,
        )
        active_watch_sync = self._build_active_watch_sync_payload(
            operations=commit_operations,
            watch_fire_records=watch_fire_records or [],
            commit_index=commit_index,
        )
        for peer in peers:
            started_at = time.time()
            try:
                payload = self._request_json(
                    f"{peer}/internal/replication/commit",
                    "POST",
                    {
                        "leader_id": self._node_id,
                        "leader_url": self._advertise_url,
                        "term": term,
                        "config_version": self._config_version,
                        "commit_index": commit_index,
                        "prev_log_index": prev_log_index,
                        "prev_log_term": prev_log_term,
                        "commit_term": commit_term,
                        "watch_fires": [
                            record.to_dict()
                            for record in (watch_fire_records or [])
                        ],
                        "active_watch_commit_index": active_watch_sync["active_watch_commit_index"],
                        "active_watches": active_watch_sync["active_watches"],
                    },
                )
                last_applied = int(payload.get("last_applied", 0))
                self._peer_states[peer] = {
                    "peer_url": peer,
                    "peer_id": payload.get("node_id"),
                    "role": "follower",
                    "reachable": True,
                    "healthy": bool(payload.get("accepted", True)),
                    "last_applied": last_applied,
                    "last_log_index": int(payload.get("last_log_index", commit_index)),
                    "last_log_term": int(payload.get("last_log_term", commit_term)),
                    "match_index": int(payload.get("last_log_index", commit_index)),
                    "match_term": int(payload.get("last_log_term", commit_term)),
                    "replication_lag": max(0, commit_index - last_applied),
                    "last_ack_at": time.time(),
                    "last_error": None if payload.get("accepted", True) else payload.get("reason", "commit_rejected"),
                }
                self._recent_apply_events.appendleft(
                    {
                        "sequence_number": commit_index,
                        "peer_id": payload.get("node_id") or peer,
                        "status": "committed" if payload.get("accepted", True) else "commit_rejected",
                        "applied_at": time.time(),
                        "latency_ms": int((time.time() - started_at) * 1000),
                        "applied_count": int(payload.get("applied_count", 0)),
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
                    "last_log_index": 0,
                    "last_log_term": 0,
                    "match_index": 0,
                    "match_term": 0,
                    "replication_lag": None,
                    "last_ack_at": None,
                    "last_error": str(exc),
                }

    def _build_active_watch_sync_payload(
        self,
        *,
        operations: List[Operation],
        watch_fire_records: List[WatchFireRecord],
        commit_index: int,
    ) -> Dict[str, Any]:
        """Project the leader's post-commit active-watch set for one replicated boundary."""
        fired_watch_ids = {
            record.watch_id
            for record in watch_fire_records
        }
        closed_session_ids = {
            operation.session_id
            for operation in operations
            if operation.operation_type == OperationType.SESSION_CLOSE
            and operation.session_id
        }
        return {
            "active_watch_commit_index": int(commit_index),
            "active_watches": self._coordinator.export_active_watches(
                exclude_watch_ids=fired_watch_ids,
                exclude_session_ids=closed_session_ids,
            ),
        }

    def _reconfig_target_peer_urls_locked(self) -> List[str]:
        """Return the peer URLs that matter for the current config transition."""
        targets = list(self._peer_urls)
        if self._reconfig_in_progress and self._pending_peer_urls:
            targets = self._normalize_peer_urls(targets + list(self._pending_peer_urls))
        return targets

    def _config_peer_sets_locked(self) -> List[List[str]]:
        """Return the active config peer sets that currently need quorum coverage."""
        peer_sets = [list(self._peer_urls)]
        if self._reconfig_in_progress and self._pending_peer_urls:
            peer_sets.append(list(self._pending_peer_urls))
        return [self._normalize_peer_urls(peer_urls) for peer_urls in peer_sets]

    def _quorum_size_for_peer_urls_locked(self, peer_urls: List[str]) -> int:
        """Return the majority size for one specific peer configuration."""
        node_count = 1 + len(self._normalize_peer_urls(peer_urls))
        return (node_count // 2) + 1

    def _healthy_node_count_for_peer_urls_locked(self, peer_urls: List[str]) -> int:
        """Count leader+self plus healthy peers for one specific config."""
        healthy_nodes = 1
        for peer in self._normalize_peer_urls(peer_urls):
            if self._peer_states.get(peer, {}).get("healthy"):
                healthy_nodes += 1
        return healthy_nodes

    def _config_quorum_commit_index_for_peer_urls_locked(
        self,
        *,
        current_sequence: int,
        leader_last_log_index: int,
        peer_urls: List[str],
    ) -> int:
        """Compute the commit index for one specific config's majority."""
        acked_sequences: List[int] = [leader_last_log_index or current_sequence]
        for peer in self._normalize_peer_urls(peer_urls):
            peer_state = self._peer_states.get(peer, {})
            match_index = peer_state.get("match_index")
            acked_sequences.append(int(match_index) if isinstance(match_index, int) else 0)

        acked_sequences.sort(reverse=True)
        quorum_size = self._quorum_size_for_peer_urls_locked(peer_urls)
        return acked_sequences[quorum_size - 1] if len(acked_sequences) >= quorum_size else 0

    def _broadcast_truncate(
        self,
        *,
        peers: List[str],
        term: int,
        truncate_after: int,
    ) -> None:
        """Best-effort rollback for uncommitted replicated-log entries."""
        for peer in peers:
            try:
                self._request_json(
                    f"{peer}/internal/replication/truncate",
                    "POST",
                    {
                        "leader_id": self._node_id,
                        "term": term,
                        "config_version": self._config_version,
                        "truncate_after": truncate_after,
                    },
                )
            except Exception:
                continue

    def _sync_active_watches_from_leader(self) -> bool:
        """Best-effort fallback mirror for follower active-watch parity."""
        if self._role != "follower" or not self._leader_url:
            return False
        try:
            payload = self._request_json(
                f"{self._leader_url}/internal/replication/watches",
                "GET",
                None,
            )
            self._coordinator.restore_active_watches(payload.get("watches", []))
            with self._lock:
                if self._last_error == "active_watch_sync_failed":
                    self._last_error = None
                    self._persist_cluster_state_locked()
            return True
        except Exception:
            with self._lock:
                self._last_error = "active_watch_sync_failed"
                self._persist_cluster_state_locked()
            return False

    def _compute_quorum_commit_index_locked(self, current_sequence: int) -> Optional[int]:
        """Compute the highest sequence known to be present on a majority."""
        if self._role == "standalone":
            self._quorum_commit_index = current_sequence
            return current_sequence
        if self._role != "leader":
            return None

        leader_last_log_index, _ = self._coordinator.get_last_replication_position()
        quorum_indexes = [
            self._config_quorum_commit_index_for_peer_urls_locked(
                current_sequence=current_sequence,
                leader_last_log_index=leader_last_log_index,
                peer_urls=peer_urls,
            )
            for peer_urls in self._config_peer_sets_locked()
        ]
        quorum_index = min(quorum_indexes) if quorum_indexes else 0
        self._quorum_commit_index = quorum_index
        return quorum_index

    def _persist_cluster_state_locked(self) -> None:
        """Persist durable election metadata for this node."""
        current_applied = self._coordinator.get_stats().get("last_sequence", 0)
        commit_index = current_applied if self._role == "leader" else max(self._leader_commit_index, current_applied)
        self._coordinator.save_cluster_state(
            node_id=self._node_id,
            current_term=self._current_term,
            voted_for=self._voted_for,
            leader_id=self._leader_id,
            leader_url=self._leader_url,
            config_version=self._config_version,
            peer_urls=self._peer_urls,
            previous_config_version=self._previous_config_version,
            previous_peer_urls=self._previous_peer_urls,
            pending_config_version=self._pending_config_version,
            pending_peer_urls=self._pending_peer_urls,
            reconfig_in_progress=self._reconfig_in_progress,
            decommissioned=self._decommissioned,
            commit_index=commit_index,
            last_applied=current_applied,
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
                self._leader_lease_until = None
                self._last_leader_contact_at = time.time()
                self._clear_prepare_reservation_locked()
                self._last_leader_contact_at = time.time()
                self._persist_cluster_state_locked()
        self._apply_replica_mode()

    def _quorum_size(self) -> int:
        """Return the majority quorum for the configured node count."""
        node_count = 1 + len(self._peer_urls)
        return (node_count // 2) + 1

    def _normalize_peer_urls(self, peer_urls: Optional[List[str]]) -> List[str]:
        """Normalize peer URLs while preserving order and removing duplicates."""
        normalized: List[str] = []
        seen: set[str] = set()
        for peer in (peer_urls or []):
            candidate = str(peer or "").strip().rstrip("/")
            if not candidate or candidate in seen:
                continue
            normalized.append(candidate)
            seen.add(candidate)
        return normalized

    def _rebuild_peer_states(
        self,
        peer_urls: List[str],
        previous: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Rebuild the peer-state map while preserving known peers when possible."""
        previous = previous or {}
        rebuilt: Dict[str, Dict[str, Any]] = {}
        for peer in self._normalize_peer_urls(peer_urls):
            state = dict(previous.get(peer, {}))
            state.setdefault("peer_url", peer)
            state.setdefault("peer_id", None)
            state.setdefault("role", "unknown")
            state.setdefault("reachable", False)
            state.setdefault("healthy", False)
            state.setdefault("last_applied", 0)
            state.setdefault("last_log_index", 0)
            state.setdefault("last_log_term", 0)
            state.setdefault("match_index", 0)
            state.setdefault("match_term", 0)
            state.setdefault("replication_lag", None)
            state.setdefault("last_ack_at", None)
            state.setdefault("last_error", None)
            rebuilt[peer] = state
        return rebuilt

    def _config_version_accepts(self, incoming_version: int) -> bool:
        """Return whether one cluster message version is valid for the local state."""
        allowed_versions = {int(self._config_version)}
        if self._pending_config_version is not None:
            allowed_versions.add(int(self._pending_config_version))
        return int(incoming_version) in allowed_versions

    def _replication_membership_rejection_reason_locked(self, config_version: int) -> Optional[str]:
        """Return a membership-fence rejection reason for replication-plane RPCs."""
        if self._decommissioned or self._role == "decommissioned":
            return "decommissioned_node"
        if not self._config_version_accepts(int(config_version)):
            return "stale_config_version"
        return None

    def _wait_for_peer_alignment(
        self,
        *,
        peer_url: str,
        expected_applied: int,
        expected_log_index: int,
        expected_log_term: int,
    ) -> None:
        """Wait briefly for one staged peer to catch up before activation."""
        deadline = time.time() + self._reconfig_timeout_seconds
        while time.time() < deadline:
            payload = self._request_json(f"{peer_url}/internal/replication/state", "GET", None)
            if (
                int(payload.get("last_applied", 0)) >= int(expected_applied)
                and int(payload.get("last_log_index", 0)) >= int(expected_log_index)
                and int(payload.get("last_log_term", 0)) == int(expected_log_term)
            ):
                return
            time.sleep(0.2)
        raise ConflictError(
            "New peer did not catch up before reconfiguration activation",
            error="cluster_reconfig_peer_catchup_timeout",
            peer_url=peer_url,
        )

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
