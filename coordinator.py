"""
Coordinator - Main Orchestrator for the Coordination Service.

The Coordinator is the single entry point for all operations.
It serializes requests through a single operation pipeline and
ensures all components work together correctly.

Responsibilities:
- Serialize all metadata operations for linearizability
- Coordinate session lifecycle with ephemeral node cleanup
- Trigger watches on state transitions
- Ensure persistence before acknowledgment
- Orchestrate crash recovery on startup
"""

import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from datetime import datetime

from models import (
    Node,
    Session,
    Watch,
    Event,
    Operation,
    NodeType,
    EventType,
    OperationType,
    WatchFireRecord,
    DELETE_CAUSE_DELETE,
    DELETE_CAUSE_LEASE_EXPIRED,
    DELETE_CAUSE_LEASE_RELEASE,
    DELETE_CAUSE_SESSION_CLOSED,
    DELETE_CAUSE_SESSION_EXPIRED,
    encode_delete_operation_payload,
    decode_delete_operation_payload,
)
from metadata_tree import MetadataTree
from session_manager import SessionManager
from watch_manager import WatchManager
from operation_log import OperationLog
from persistence import Persistence
from recovery import RecoveryManager
from logger import get_logger
from config import DATABASE_PATH
from errors import ConflictError, ForbiddenError

logger = get_logger("coordinator")


class Coordinator:
    """
    Main orchestrator for the Coordination Service.
    
    All operations flow through the Coordinator to ensure:
    - Linearizability (single operation pipeline)
    - Session correctness (ephemeral cleanup)
    - Watch correctness (exactly-once firing)
    - Crash safety (persist before ack)
    
    Thread-safe for concurrent client access.
    """
    
    def __init__(self, db_path: str = DATABASE_PATH):
        """Initialize all components and perform recovery."""
        self._lock = threading.RLock()
        self._started = False
        self._start_time = datetime.now().timestamp()
        self._last_recovery_stats: Dict[str, Any] = {}
        self._session_inventory_version = 0
        self._session_inventory_condition = threading.Condition(self._lock)
        self._lease_wait_condition = threading.Condition(self._lock)
        self._lease_waiters: Dict[str, List[str]] = {}
        self._lease_reaper_running = False
        self._lease_reaper_thread: Optional[threading.Thread] = None
        self._read_only = False
        self._read_only_reason = ""
        self._write_guard: Optional[Callable[[], None]] = None
        
        # Initialize components
        self._persistence = Persistence(db_path)
        self._metadata_tree = MetadataTree()
        self._session_manager = SessionManager()
        self._watch_manager = WatchManager()
        self._operation_log = OperationLog()
        
        # Recovery manager
        self._recovery_manager = RecoveryManager(
            persistence=self._persistence,
            metadata_tree=self._metadata_tree,
            session_manager=self._session_manager,
            watch_manager=self._watch_manager,
            operation_log=self._operation_log,
        )
        
        # Wire up session expiry callback
        self._session_manager.add_expiry_callback(self._on_session_expired)
        
        logger.info("Coordinator initialized")
    
    def start(self) -> dict:
        """
        Start the coordination service.
        
        Performs crash recovery and starts background threads.
        
        Returns:
            Recovery statistics
        """
        with self._lock:
            if self._started:
                return {"status": "already_started"}
            
            # Perform crash recovery
            recovery_stats = self._recovery_manager.recover()
            self._last_recovery_stats = dict(recovery_stats)
            
            # Start session timeout checker
            self._session_manager.start()
            self._lease_reaper_running = True
            self._lease_reaper_thread = threading.Thread(
                target=self._lease_reaper_loop,
                daemon=True,
                name="lease-expiry-checker",
            )
            self._lease_reaper_thread.start()
            
            self._started = True
            self._start_time = datetime.now().timestamp()
            
            logger.info("Coordinator started")
            
            return recovery_stats
    
    def stop(self) -> None:
        """Stop the coordination service."""
        with self._lock:
            if not self._started:
                return
            
            self._lease_reaper_running = False
            self._session_manager.stop()
            if self._lease_reaper_thread:
                self._lease_reaper_thread.join(timeout=2.0)
                self._lease_reaper_thread = None
            self._persistence.close()
            self._started = False
            
            logger.info("Coordinator stopped")

    def set_read_only(self, read_only: bool, reason: str = "") -> None:
        """Toggle read-only mode for follower replicas."""
        with self._lock:
            self._read_only = read_only
            self._read_only_reason = reason or ""

    def set_write_guard(self, guard: Optional[Callable[[], None]]) -> None:
        """Install an optional guard that can reject client writes."""
        with self._lock:
            self._write_guard = guard

    def get_read_only_status(self) -> Dict[str, Any]:
        """Return current read-only status metadata."""
        with self._lock:
            return {
                "read_only": self._read_only,
                "reason": self._read_only_reason,
            }

    def save_cluster_state(
        self,
        *,
        node_id: str,
        current_term: int,
        voted_for: Optional[str] = None,
        leader_id: Optional[str] = None,
        leader_url: Optional[str] = None,
    ) -> None:
        """Persist cluster-election metadata for the local node."""
        self._persistence.save_cluster_state(
            node_id=node_id,
            current_term=current_term,
            voted_for=voted_for,
            leader_id=leader_id,
            leader_url=leader_url,
        )

    def load_cluster_state(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Load persisted cluster-election metadata for the local node."""
        return self._persistence.load_cluster_state(node_id)

    def add_commit_callback(self, callback: Callable[[Operation], None]) -> None:
        """Register a callback for committed operations."""
        self._operation_log.add_commit_callback(callback)

    def remove_commit_callback(self, callback: Callable[[Operation], None]) -> None:
        """Remove a previously registered commit callback."""
        self._operation_log.remove_commit_callback(callback)

    def disable_local_maintenance(self) -> None:
        """Stop local expiry/TTL threads so follower replicas mirror the leader only."""
        with self._lock:
            self._session_manager.stop()
            self._lease_reaper_running = False
            if self._lease_reaper_thread:
                self._lease_reaper_thread.join(timeout=2.0)
                self._lease_reaper_thread = None

    def enable_local_maintenance(self) -> None:
        """Ensure local expiry/TTL threads are active on writable nodes."""
        with self._lock:
            self._session_manager.start()
            if self._lease_reaper_thread is None:
                self._lease_reaper_running = True
                self._lease_reaper_thread = threading.Thread(
                    target=self._lease_reaper_loop,
                    daemon=True,
                    name="lease-expiry-checker",
                )
                self._lease_reaper_thread.start()

    def _assert_writable(self) -> None:
        """Reject client writes when this coordinator is acting as a follower."""
        if self._read_only:
            raise ForbiddenError(
                self._read_only_reason or "This node is read-only",
                error="read_only_replica",
            )
        if self._write_guard is not None:
            self._write_guard()
    
    # ========== Session Operations ==========
    
    def open_session(self, timeout_seconds: int = 30) -> Session:
        """
        Open a new client session.
        
        Args:
            timeout_seconds: Session timeout (5-300 seconds)
            
        Returns:
            The created Session
        """
        with self._lock:
            self._assert_writable()
            session = self._session_manager.open_session(timeout_seconds)
            operation = self._operation_log.append(
                operation_type=OperationType.SESSION_OPEN,
                session_id=session.session_id,
            )
            operation.data = self._encode_session_operation_payload(session)
            try:
                self._persistence.atomic_save_session(session, operation)
            except Exception:
                self._session_manager.remove_session(session.session_id)
                self._operation_log.discard_last_operation(operation.sequence_number)
                raise
            self._operation_log.commit(operation)
            self._mark_session_inventory_changed()
            
            logger.info(f"Session opened: {session.session_id}")
            
            return session
    
    def heartbeat(self, session_id: str) -> Session:
        """
        Process a session heartbeat.
        
        Args:
            session_id: The session ID
            
        Returns:
            The updated Session
            
        Raises:
            KeyError: If session doesn't exist
            ValueError: If session is dead
        """
        with self._lock:
            self._assert_writable()
            existing = self._session_manager.get_session(session_id)
            if existing is None:
                raise KeyError(f"Session does not exist: {session_id}")
            snapshot = self._clone_session(existing)
            session = self._session_manager.heartbeat(session_id)
            
            operation = self._operation_log.append(
                operation_type=OperationType.SESSION_HEARTBEAT,
                session_id=session_id,
            )
            operation.data = self._encode_session_operation_payload(session)
            try:
                self._persistence.atomic_save_session(session, operation)
            except Exception:
                self._session_manager.replace_session(snapshot)
                self._operation_log.discard_last_operation(operation.sequence_number)
                raise
            self._operation_log.commit(operation)
            self._mark_session_inventory_changed()
            
            return session
    
    def close_session(self, session_id: str) -> Optional[Session]:
        """
        Close a session explicitly.
        
        This triggers ephemeral node cleanup.
        
        Returns:
            The closed Session, or None if not found
        """
        with self._lock:
            self._assert_writable()
            return self._session_manager.close_session(session_id)

    def get_sessions(self, alive_only: bool = False) -> List[Dict[str, Any]]:
        """Return session inventory data for the API and visualizer."""
        with self._lock:
            return self._build_session_views_locked(alive_only=alive_only)

    def get_session_stream_snapshot(self, alive_only: bool = False) -> Tuple[int, List[Dict[str, Any]]]:
        """Return the current session inventory version plus a snapshot."""
        with self._lock:
            return self._session_inventory_version, self._build_session_views_locked(alive_only=alive_only)

    def wait_for_sessions(
        self,
        version: int,
        timeout_seconds: float = 30.0,
        alive_only: bool = False,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        """Wait until the session inventory changes or the timeout elapses."""
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")

        deadline = time.monotonic() + timeout_seconds
        with self._session_inventory_condition:
            while self._session_inventory_version <= version:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return version, []
                self._session_inventory_condition.wait(timeout=remaining)

            return self._session_inventory_version, self._build_session_views_locked(alive_only=alive_only)

    def get_session_detail(
        self,
        session_id: str,
        operation_limit: int = 20,
    ) -> Optional[Dict[str, Any]]:
        """Return a drill-down view for one session."""
        with self._lock:
            session = self._session_manager.get_session(session_id)
            if session is None:
                return None

            summary = self._build_session_views_locked(alive_only=False)
            session_summary = next(
                (item for item in summary if item["session_id"] == session_id),
                None,
            )
            if session_summary is None:
                return None

            watches = [
                watch.to_dict()
                for watch in self._watch_manager.get_watches_for_session(session_id)
            ]
            owned_nodes = []
            for path in sorted(session.ephemeral_nodes):
                node = self._metadata_tree.get(path)
                if node is None:
                    continue
                raw_data = node.data.decode("utf-8") if isinstance(node.data, bytes) else str(node.data)
                owned_nodes.append({
                    "path": node.path,
                    "node_type": node.node_type.value,
                    "version": node.version,
                    "session_id": node.session_id,
                    "data_preview": raw_data if len(raw_data) <= 120 else raw_data[:117] + "...",
                    "created_at": node.created_at,
                    "modified_at": node.modified_at,
                })
            recent_operations = self._get_recent_operations_locked(
                limit=operation_limit,
                session_id=session_id,
            )
            recent_watch_fires = [
                record.to_dict()
                for record in self._persistence.load_watch_fires_for_session(
                    session_id,
                    limit=operation_limit,
                )
            ]

            return {
                **session_summary,
                "owned_nodes": owned_nodes,
                "watches": watches,
                "recent_watch_fires": recent_watch_fires,
                "recent_operations": recent_operations,
            }

    def get_path_detail(
        self,
        path: str,
        operation_limit: int = 20,
        watch_limit: int = 20,
    ) -> Optional[Dict[str, Any]]:
        """Return a current or historical drill-down for an exact path."""
        normalized_path = self._normalize_path(path)
        with self._lock:
            node = self._metadata_tree.get(normalized_path)
            history = self._reconstruct_path_history_locked(normalized_path)
            recent_operations = self._get_operations_for_exact_path_locked(
                normalized_path,
                limit=operation_limit,
            )
            if node is None and not recent_operations:
                return None

            current_node = None
            current_lease = None
            holder_session = None
            if node is not None:
                current_node = self._build_path_node_view(
                    path=node.path,
                    data=node.data,
                    version=node.version,
                    node_type=node.node_type,
                    session_id=node.session_id,
                    created_at=node.created_at,
                    modified_at=node.modified_at,
                    lease_token=self._get_lease_token(node.path) if node.node_type == NodeType.EPHEMERAL else None,
                )
                if node.node_type == NodeType.EPHEMERAL:
                    current_lease = self._lease_info_from_node(node)
                    holder_session = self._lookup_session_view_locked(node.session_id)

            last_known_node = None
            if current_node is None and history["last_known_state"] is not None:
                last_known_state = history["last_known_state"]
                last_known_node = self._build_path_node_view(
                    path=last_known_state["path"],
                    data=last_known_state["data"],
                    version=last_known_state["version"],
                    node_type=last_known_state["node_type"],
                    session_id=last_known_state["session_id"],
                    created_at=last_known_state["created_at"],
                    modified_at=last_known_state["modified_at"],
                    lease_token=last_known_state["lease_token"],
                )

            owner_session_id = None
            if current_node is not None:
                owner_session_id = current_node.get("session_id")
            elif last_known_node is not None:
                owner_session_id = last_known_node.get("session_id")

            owner_session = self._lookup_session_view_locked(owner_session_id)
            active_watches = [
                watch.to_dict()
                for watch in self._get_relevant_watches_for_path_locked(normalized_path)
                if not watch.is_fired
            ]
            waiters = self._get_lease_waiters_locked(normalized_path)

            last_delete = history["last_delete"]
            session_close_operation = self._find_session_close_operation_locked(
                owner_session_id,
                before_sequence=last_delete.sequence_number if last_delete else None,
            )
            close_reason = self._decode_session_close_reason(session_close_operation)
            if session_close_operation is not None and all(
                operation.sequence_number != session_close_operation.sequence_number
                for operation in recent_operations
            ):
                recent_operations = sorted(
                    [*recent_operations, session_close_operation],
                    key=lambda operation: operation.sequence_number,
                    reverse=True,
                )[:operation_limit]

            disappearance = None
            fired_watches = []
            if last_delete is not None:
                fired_watches = [
                    record.to_dict()
                    for record in self._persistence.load_watch_fires_for_path(
                        normalized_path,
                        limit=watch_limit,
                        cause_sequence_number=last_delete.sequence_number,
                    )
                ]
                delete_cause = self._decode_delete_cause(last_delete)

                if delete_cause == DELETE_CAUSE_LEASE_EXPIRED:
                    cause_kind = "lease_expired"
                elif delete_cause == DELETE_CAUSE_LEASE_RELEASE:
                    cause_kind = "lease_released"
                elif close_reason == "explicit":
                    cause_kind = "session_closed_cleanup"
                elif close_reason == "timeout":
                    cause_kind = "session_timeout_cleanup"
                elif (
                    owner_session_id
                    and last_delete.session_id == owner_session_id
                    and last_known_node is not None
                    and last_known_node.get("node_type") == NodeType.EPHEMERAL.value
                ):
                    cause_kind = "owner_delete"
                else:
                    cause_kind = "delete"

                disappearance = {
                    "state": "deleted",
                    "cause_kind": cause_kind,
                    "reason": close_reason,
                    "cause_session_id": session_close_operation.session_id if session_close_operation else last_delete.session_id,
                    "delete_operation": last_delete,
                    "cause_operation": session_close_operation,
                }

            return {
                "path": normalized_path,
                "current_node": current_node,
                "last_known_node": last_known_node,
                "owner_session": owner_session,
                "current_lease": current_lease,
                "holder_session": holder_session,
                "waiters": waiters,
                "waiter_count": len(waiters),
                "holder_history": history["holder_history"],
                "active_watches": active_watches,
                "fired_watches": fired_watches,
                "disappearance": disappearance,
                "recent_operations": recent_operations,
            }

    def get_operation_incident(
        self,
        sequence_number: int,
        path_limit: int = 12,
        watch_limit: int = 40,
    ) -> Optional[Dict[str, Any]]:
        """Return an operation-centric incident report for one committed operation."""
        with self._lock:
            operation = self._operation_log.get_operation(sequence_number)
            if operation is None:
                return None

            cause_operation: Optional[Operation] = None
            cleanup_operations: List[Operation] = []
            watch_fire_records: List[WatchFireRecord] = []
            incident_kind = operation.operation_type.value.lower()
            summary = f"{operation.operation_type.value.replace('_', ' ').title()} committed"
            subtitle = "Committed operation incident"
            notes: List[str] = []
            path_changes: List[Tuple[str, str, int]] = []

            if operation.operation_type == OperationType.SESSION_CLOSE:
                cleanup_operations = self._find_cleanup_delete_operations_locked(
                    operation.session_id,
                    after_sequence=operation.sequence_number,
                )
                cleanup_paths = self._collect_paths_for_operations(cleanup_operations)
                for cleanup_operation in cleanup_operations:
                    watch_fire_records.extend(
                        self._persistence.load_watch_fires_for_operation(
                            cleanup_operation.sequence_number,
                            limit=watch_limit,
                        )
                    )
                    path_changes.extend(
                        (path, "deleted", cleanup_operation.sequence_number)
                        for path in self._decode_delete_paths(cleanup_operation)
                    )

                close_reason = self._decode_session_close_reason(operation)
                incident_kind = (
                    "session_timeout_cleanup"
                    if close_reason == "timeout"
                    else "session_cleanup"
                    if cleanup_paths
                    else "session_close"
                )
                summary = (
                    "Session timed out"
                    if close_reason == "timeout"
                    else "Session closed"
                )
                subtitle = (
                    f"{len(cleanup_paths)} path(s) cleaned up and {len(watch_fire_records)} watch firing(s) recorded."
                    if cleanup_paths
                    else "No ephemeral cleanup was required after the session ended."
                )
                if close_reason:
                    notes.append(f"Close reason recorded as {close_reason}.")
            else:
                if operation.operation_type == OperationType.DELETE:
                    deleted_paths = self._decode_delete_paths(operation)
                    delete_cause = self._decode_delete_cause(operation)
                    path_changes.extend(
                        (path, "deleted", operation.sequence_number)
                        for path in deleted_paths
                    )
                    cause_operation = self._find_session_close_operation_locked(
                        operation.session_id,
                        before_sequence=operation.sequence_number,
                    )
                    close_reason = self._decode_session_close_reason(cause_operation)
                    if cause_operation is not None:
                        incident_kind = (
                            "session_timeout_cleanup"
                            if close_reason == "timeout"
                            else "session_cleanup"
                        )
                        summary = (
                            "Timeout cleanup deleted path(s)"
                            if close_reason == "timeout"
                            else "Session close cleanup deleted path(s)"
                        )
                        if close_reason:
                            notes.append(f"Cleanup reason recorded as {close_reason}.")
                    elif delete_cause == DELETE_CAUSE_LEASE_EXPIRED:
                        incident_kind = "lease_expiration"
                        summary = "Lease TTL expired"
                    elif delete_cause == DELETE_CAUSE_LEASE_RELEASE:
                        incident_kind = "lease_release"
                        summary = "Lease released"
                    else:
                        incident_kind = "recursive_delete" if len(deleted_paths) > 1 else "delete"
                        summary = "Recursive delete committed" if len(deleted_paths) > 1 else "Delete committed"
                elif operation.path:
                    change_kind = "created" if operation.operation_type == OperationType.CREATE else "updated"
                    path_changes.append((operation.path, change_kind, operation.sequence_number))
                    if operation.operation_type == OperationType.CREATE and operation.node_type == NodeType.EPHEMERAL:
                        incident_kind = "lease_acquire"
                        summary = "Ephemeral lease acquired"
                        subtitle = f"Lease path {operation.path} created for session-backed ownership."
                    elif operation.operation_type == OperationType.CREATE:
                        incident_kind = "create"
                        summary = "Node created"
                        subtitle = f"Persistent path {operation.path} entered the namespace."
                    elif operation.operation_type == OperationType.SET:
                        incident_kind = "mutation"
                        summary = "Node updated"
                        subtitle = f"Committed write to {operation.path}."
                else:
                    if operation.operation_type == OperationType.SESSION_OPEN:
                        incident_kind = "session_open"
                        summary = "Session opened"
                        subtitle = "A new live session joined the coordinator."
                    elif operation.operation_type == OperationType.SESSION_HEARTBEAT:
                        incident_kind = "session_heartbeat"
                        summary = "Session heartbeat recorded"
                        subtitle = "Heartbeat advanced the session TTL without mutating paths."

                watch_fire_records = self._persistence.load_watch_fires_for_operation(
                    operation.sequence_number,
                    limit=watch_limit,
                )
                if not subtitle or subtitle == "Committed operation incident":
                    affected_count = len(path_changes)
                    subtitle = (
                        f"{affected_count} path(s) affected and {len(watch_fire_records)} watch firing(s) recorded."
                        if affected_count or watch_fire_records
                        else "No dependent paths or watch firings were recorded for this operation."
                    )

            deduped_path_changes: List[Tuple[str, str, int]] = []
            seen_paths: Set[str] = set()
            for path, change_kind, change_sequence in path_changes:
                if path in seen_paths:
                    continue
                deduped_path_changes.append((path, change_kind, change_sequence))
                seen_paths.add(path)

            visible_path_changes = deduped_path_changes[:path_limit] if path_limit > 0 else deduped_path_changes
            affected_paths = [
                self._build_incident_path_entry_locked(
                    path=path,
                    operation_sequence=change_sequence,
                    change_kind=change_kind,
                )
                for path, change_kind, change_sequence in visible_path_changes
            ]

            unique_watcher_session_ids: List[str] = []
            seen_watcher_session_ids: Set[str] = set()
            for record in watch_fire_records:
                if record.watch_session_id in seen_watcher_session_ids:
                    continue
                unique_watcher_session_ids.append(record.watch_session_id)
                seen_watcher_session_ids.add(record.watch_session_id)

            watcher_sessions = [
                session_view
                for session_view in (
                    self._lookup_session_view_locked(session_id)
                    for session_id in unique_watcher_session_ids
                )
                if session_view is not None
            ]

            session_roles: Dict[str, Set[str]] = {}

            def add_session_role(session_id: Optional[str], role: str) -> None:
                if not session_id:
                    return
                session_roles.setdefault(session_id, set()).add(role)

            add_session_role(operation.session_id, "actor")
            add_session_role(cause_operation.session_id if cause_operation else None, "cause")
            for cleanup_operation in cleanup_operations:
                add_session_role(cleanup_operation.session_id, "cleanup")
            for path_view in affected_paths:
                add_session_role(path_view.get("session_id"), "owner")
            for watcher_session in watcher_sessions:
                add_session_role(watcher_session.get("session_id"), "watcher")

            impacted_sessions = self._build_incident_session_views_locked(session_roles)
            source_session = next(
                (
                    session_view
                    for session_view in impacted_sessions
                    if session_view["session_id"] == operation.session_id
                ),
                self._lookup_session_view_locked(operation.session_id),
            )

            causal_chain: List[Operation] = []
            if cause_operation is not None:
                causal_chain.append(cause_operation)
            causal_chain.append(operation)
            if operation.operation_type == OperationType.SESSION_CLOSE:
                causal_chain.extend(cleanup_operations)
            primary_path = ""
            if operation.operation_type == OperationType.DELETE:
                deleted_paths = self._decode_delete_paths(operation)
                primary_path = deleted_paths[0] if deleted_paths else ""
            elif deduped_path_changes:
                primary_path = deduped_path_changes[0][0]
            elif operation.path:
                primary_path = operation.path

            return {
                "sequence_number": operation.sequence_number,
                "operation": operation,
                "incident_kind": incident_kind,
                "summary": summary,
                "subtitle": subtitle,
                "primary_path": primary_path,
                "source_session": source_session,
                "cause_operation": cause_operation,
                "cleanup_operations": cleanup_operations,
                "related_operations": cleanup_operations,
                "causal_chain": causal_chain,
                "affected_path_count": len(deduped_path_changes),
                "path_overflow_count": max(0, len(deduped_path_changes) - len(affected_paths)),
                "affected_paths": affected_paths,
                "watch_fire_count": len(watch_fire_records),
                "watch_overflow_count": 0,
                "watch_fires": [record.to_dict() for record in watch_fire_records],
                "watcher_sessions": watcher_sessions,
                "impacted_sessions": impacted_sessions,
                "blast_radius": {
                    "affected_paths": len(deduped_path_changes),
                    "watches_fired": len(watch_fire_records),
                    "related_operations": len(cleanup_operations) + (1 if cause_operation is not None else 0),
                    "sessions_touched": len(impacted_sessions),
                },
                "stats": {
                    "affected_paths": len(deduped_path_changes),
                    "displayed_paths": len(affected_paths),
                    "watch_fires": len(watch_fire_records),
                    "impacted_sessions": len(impacted_sessions),
                    "cascade_operations": len(cleanup_operations),
                },
                "notes": notes,
            }
    
    def _on_session_expired(self, session: Session, reason: str = "timeout") -> None:
        """
        Handle session expiration.
        
        Called by SessionManager when a session times out.
        Deletes all ephemeral nodes and triggers watches.
        """
        with self._lock:
            logger.info(f"Handling session expiry: {session.session_id}")

            deleted_paths = self._metadata_tree.plan_delete_session_nodes(session.session_id)
            persisted_session = self._clone_session(session)
            persisted_session.ephemeral_nodes.difference_update(deleted_paths)
            session_close_operation = self._operation_log.append(
                operation_type=OperationType.SESSION_CLOSE,
                session_id=session.session_id,
            )
            session_close_operation.data = self._encode_session_operation_payload(
                persisted_session,
                reason=reason,
            )

            if deleted_paths:
                delete_operation = self._operation_log.append(
                    operation_type=OperationType.DELETE,
                    path=",".join(deleted_paths),
                    session_id=session.session_id,
                )
                delete_operation.data = encode_delete_operation_payload(
                    deleted_paths,
                    cause=DELETE_CAUSE_SESSION_EXPIRED if reason == "timeout" else DELETE_CAUSE_SESSION_CLOSED,
                )
                watch_fires = self._collect_watch_fire_records_locked(
                    deleted_paths,
                    event_type=EventType.DELETE,
                    sequence_number=delete_operation.sequence_number,
                    timestamp=delete_operation.timestamp,
                )
                try:
                    self._persistence.atomic_delete_node(
                        deleted_paths,
                        delete_operation,
                        sessions=[persisted_session],
                        watch_fires=watch_fires,
                        extra_operations=[session_close_operation],
                    )
                except Exception:
                    self._operation_log.discard_last_operation(delete_operation.sequence_number)
                    self._operation_log.discard_last_operation(session_close_operation.sequence_number)
                    raise

                self._metadata_tree.apply_delete_paths(deleted_paths)
                session.ephemeral_nodes.clear()
                self._operation_log.commit(session_close_operation)
                self._operation_log.commit(delete_operation)

                for path in deleted_paths:
                    self._watch_manager.trigger(
                        path=path,
                        event_type=EventType.DELETE,
                        sequence_number=delete_operation.sequence_number,
                    )
            else:
                try:
                    self._persistence.atomic_save_session(
                        persisted_session,
                        operation=session_close_operation,
                    )
                except Exception:
                    self._operation_log.discard_last_operation(session_close_operation.sequence_number)
                    raise
                self._operation_log.commit(session_close_operation)

            self._watch_manager.clear_session_watches(session.session_id)
            self._remove_waiter_from_all_lease_queues_locked(session.session_id)
            self._notify_lease_waiters_locked()

            if not deleted_paths:
                session.ephemeral_nodes.clear()
                session.ephemeral_nodes.update(persisted_session.ephemeral_nodes)
            self._mark_session_inventory_changed()
            
            logger.info(f"Session cleanup complete: deleted {len(deleted_paths)} ephemeral nodes")
    
    # ========== Metadata Operations ==========
    
    def create(
        self,
        path: str,
        data: bytes,
        persistent: bool = True,
        session_id: Optional[str] = None,
    ) -> Node:
        """
        Create a new node in the metadata tree.
        
        Args:
            path: The path where to create the node
            data: The data to store
            persistent: If True, create persistent node; if False, create ephemeral
            session_id: Required for ephemeral nodes
            
        Returns:
            The created Node
            
        Raises:
            ValueError: If path invalid or parent doesn't exist
            KeyError: If node already exists
        """
        with self._lock:
            node, _ = self._create_node(
                path=path,
                data=data,
                persistent=persistent,
                session_id=session_id,
            )
            return node

    def _create_node(
        self,
        path: str,
        data: bytes,
        persistent: bool = True,
        session_id: Optional[str] = None,
    ) -> Tuple[Node, Operation]:
        """Internal create helper that returns the committed operation."""
        self._assert_writable()
        # Determine node type
        node_type = NodeType.PERSISTENT if persistent else NodeType.EPHEMERAL
        
        # Validate ephemeral requirements
        if node_type == NodeType.EPHEMERAL:
            if session_id is None:
                raise ValueError("Ephemeral nodes require a session_id")
            if not self._session_manager.is_alive(session_id):
                raise ValueError(f"Session is not alive: {session_id}")
        
        # Convert data to bytes if needed
        if isinstance(data, str):
            data = data.encode("utf-8")
        
        node = self._metadata_tree.prepare_create(
            path=path,
            data=data,
            node_type=node_type,
            session_id=session_id,
        )
        
        persisted_session = None
        if node_type == NodeType.EPHEMERAL:
            live_session = self._session_manager.get_session(session_id)
            if live_session is None:
                raise KeyError(f"Session does not exist: {session_id}")
            persisted_session = self._clone_session(live_session)
            persisted_session.ephemeral_nodes.add(node.path)
        
        # Log operation
        operation = self._operation_log.append(
            operation_type=OperationType.CREATE,
            path=node.path,
            data=data,
            session_id=session_id,
            node_type=node_type,
        )
        watch_fires = self._collect_watch_fire_records_locked(
            [node.path],
            event_type=EventType.CREATE,
            sequence_number=operation.sequence_number,
            timestamp=operation.timestamp,
        )
        
        try:
            self._persistence.atomic_create_node(
                node,
                operation,
                persisted_session,
                watch_fires=watch_fires,
            )
        except Exception:
            self._operation_log.discard_last_operation(operation.sequence_number)
            raise

        committed_node = self._metadata_tree.commit_create(node)
        if node_type == NodeType.EPHEMERAL:
            self._session_manager.add_ephemeral_node(session_id, committed_node.path)
            self._mark_session_inventory_changed()
        self._operation_log.commit(operation)
        
        # Trigger watches
        self._watch_manager.trigger(
            path=committed_node.path,
            event_type=EventType.CREATE,
            data=data,
            sequence_number=operation.sequence_number,
        )
        
        logger.info(f"Created node: {committed_node.path} (type={node_type.value})")
        
        return committed_node, operation

    def _normalize_path(self, path: str) -> str:
        """Normalize a path into canonical form."""
        if not path:
            return "/"
        if not path.startswith("/"):
            path = "/" + path
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
        while "//" in path:
            path = path.replace("//", "/")
        return path

    def _clone_session(self, session: Session) -> Session:
        """Create a detached copy of a session for staged persistence."""
        return Session.from_dict(session.to_dict())

    def _mark_session_inventory_changed(self) -> None:
        """Advance the session inventory version and wake stream listeners."""
        with self._session_inventory_condition:
            self._session_inventory_version += 1
            self._session_inventory_condition.notify_all()

    def _notify_lease_waiters_locked(self) -> None:
        """Wake blocked lease contenders after a lease state change."""
        with self._lease_wait_condition:
            self._lease_wait_condition.notify_all()

    def _prune_lease_waiters_locked(self, path: str) -> List[str]:
        """Drop dead or duplicate waiters from a lease queue and return the queue."""
        normalized_path = self._normalize_path(path)
        queue = self._lease_waiters.get(normalized_path, [])
        cleaned: List[str] = []
        seen: Set[str] = set()
        for session_id in queue:
            if session_id in seen:
                continue
            if not self._session_manager.is_alive(session_id):
                continue
            cleaned.append(session_id)
            seen.add(session_id)

        if cleaned:
            self._lease_waiters[normalized_path] = cleaned
        else:
            self._lease_waiters.pop(normalized_path, None)
        return list(cleaned)

    def _enqueue_lease_waiter_locked(self, path: str, session_id: str) -> List[str]:
        """Append a waiter to a lease queue if it is not already queued."""
        normalized_path = self._normalize_path(path)
        queue = self._prune_lease_waiters_locked(normalized_path)
        if session_id not in queue:
            queue.append(session_id)
            self._lease_waiters[normalized_path] = queue
        return list(queue)

    def _dequeue_lease_waiter_locked(self, path: str, session_id: str) -> None:
        """Remove one session from a lease queue."""
        normalized_path = self._normalize_path(path)
        queue = [queued for queued in self._lease_waiters.get(normalized_path, []) if queued != session_id]
        if queue:
            self._lease_waiters[normalized_path] = queue
        else:
            self._lease_waiters.pop(normalized_path, None)

    def _remove_waiter_from_all_lease_queues_locked(self, session_id: str) -> None:
        """Remove a session from every lease queue when it dies or disconnects."""
        for path in list(self._lease_waiters.keys()):
            self._dequeue_lease_waiter_locked(path, session_id)

    def _get_lease_waiters_locked(self, path: str) -> List[str]:
        """Return the live FIFO waiter queue for a lease path."""
        return self._prune_lease_waiters_locked(path)

    def _lease_reaper_loop(self) -> None:
        """Background loop that expires leases with an independent TTL."""
        while self._lease_reaper_running:
            try:
                self._expire_due_leases()
            except Exception as exc:
                logger.error(f"Lease expiry error: {exc}")
            time.sleep(0.5)

    def _expire_due_leases(self) -> None:
        """Delete expired lease nodes while preserving normal durability semantics."""
        with self._lock:
            now = datetime.now().timestamp()
            due_paths: List[str] = []
            for node in self._metadata_tree.get_all_nodes():
                if node.path == "/" or node.node_type != NodeType.EPHEMERAL:
                    continue
                _, _, _, lease_expires_at = self._decode_lease_record_data(node.data, node.session_id)
                if lease_expires_at is not None and lease_expires_at <= now:
                    due_paths.append(node.path)

        for due_path in due_paths:
            try:
                self.delete(due_path, cause=DELETE_CAUSE_LEASE_EXPIRED)
                logger.info(f"Lease expired independently of session TTL: {due_path}")
            except KeyError:
                continue
            except Exception as exc:
                logger.error(f"Failed to expire lease {due_path}: {exc}")

    def _build_session_views_locked(self, alive_only: bool = False) -> List[Dict[str, Any]]:
        """Build detached session inventory views while holding the coordinator lock."""
        current_time = datetime.now().timestamp()
        sessions = (
            self._session_manager.get_alive_sessions()
            if alive_only
            else self._session_manager.get_all_sessions()
        )

        session_views: List[Dict[str, Any]] = []
        for session in sessions:
            expires_at = session.last_heartbeat + session.timeout_seconds
            remaining_seconds = max(0.0, expires_at - current_time) if session.is_alive else 0.0
            ephemeral_nodes = sorted(session.ephemeral_nodes)
            watch_count = len(self._watch_manager.get_watches_for_session(session.session_id))

            session_views.append({
                "session_id": session.session_id,
                "created_at": session.created_at,
                "last_heartbeat": session.last_heartbeat,
                "timeout_seconds": session.timeout_seconds,
                "expires_at": expires_at,
                "remaining_seconds": remaining_seconds,
                "is_alive": session.is_alive,
                "ephemeral_nodes": ephemeral_nodes,
                "ephemeral_node_count": len(ephemeral_nodes),
                "watch_count": watch_count,
            })

        session_views.sort(
            key=lambda item: (
                not item["is_alive"],
                -item["last_heartbeat"],
                item["session_id"],
            )
        )
        return session_views

    def _get_recent_operations_locked(
        self,
        limit: int = 20,
        path: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> List[Operation]:
        """Return recent committed operations in reverse chronological order."""
        if limit <= 0:
            return []

        operations = list(reversed(self._operation_log.get_all_operations()))
        if path is not None:
            operations = [operation for operation in operations if operation.path == path]
        if session_id is not None:
            operations = [operation for operation in operations if operation.session_id == session_id]
        return operations[:limit]

    def _lookup_session_view_locked(self, session_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Look up a detached session summary by ID."""
        if not session_id:
            return None

        for item in self._build_session_views_locked(alive_only=False):
            if item["session_id"] == session_id:
                return item
        return None

    def _encode_session_operation_payload(
        self,
        session: Session,
        reason: Optional[str] = None,
    ) -> bytes:
        """Encode a committed session snapshot for replication and postmortems."""
        payload: Dict[str, Any] = {"session": session.to_dict()}
        if reason:
            payload["reason"] = reason
        return json.dumps(payload, sort_keys=True).encode("utf-8")

    def _decode_session_operation_payload(
        self,
        operation: Optional[Operation],
    ) -> Tuple[Optional[Session], Optional[str]]:
        """Decode an optional session snapshot and close reason from an operation."""
        if operation is None or not operation.data:
            return None, None

        try:
            payload = json.loads(operation.data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None, None

        if not isinstance(payload, dict):
            return None, None

        session_payload = payload.get("session")
        session = None
        if isinstance(session_payload, dict):
            try:
                session = Session.from_dict(session_payload)
            except Exception:
                session = None

        reason = payload.get("reason")
        return session, str(reason) if reason else None

    def _decode_session_close_reason(self, operation: Optional[Operation]) -> Optional[str]:
        """Decode the close/timeout reason from a session-close operation."""
        _, reason = self._decode_session_operation_payload(operation)
        return reason

    def _find_session_cleanup_delete_operation_locked(
        self,
        session_id: Optional[str],
        after_sequence: int,
    ) -> Optional[Operation]:
        """Return the immediate cleanup delete paired with a session-close op."""
        if not session_id:
            return None

        candidate = self._operation_log.get_operation(after_sequence + 1)
        if (
            candidate is not None
            and candidate.operation_type == OperationType.DELETE
            and candidate.session_id == session_id
        ):
            return candidate
        return None

    def _collect_watch_fire_records_locked(
        self,
        paths: List[str],
        event_type: EventType,
        sequence_number: int,
        timestamp: float,
    ) -> List[Any]:
        """Plan persisted watch-fire records in the same order trigger() will use."""
        planned_records = []
        ordinal = 1
        for observed_path in paths:
            for record in self._watch_manager.plan_triggers(
                observed_path,
                event_type=event_type,
                sequence_number=sequence_number,
                timestamp=timestamp,
            ):
                record.ordinal = ordinal
                planned_records.append(record)
                ordinal += 1
        return planned_records

    def _build_session_updates_for_deleted_paths(
        self,
        deleted_paths: List[str],
    ) -> Dict[str, Tuple[Session, Session]]:
        """Build staged session updates for any ephemeral nodes being deleted."""
        session_updates: Dict[str, Tuple[Session, Session]] = {}
        for deleted_path in deleted_paths:
            deleted_node = self._metadata_tree.get(deleted_path)
            if deleted_node is None or not deleted_node.session_id:
                continue

            live_session = self._session_manager.get_session(deleted_node.session_id)
            if live_session is None:
                continue

            if deleted_node.session_id not in session_updates:
                session_updates[deleted_node.session_id] = (
                    live_session,
                    self._clone_session(live_session),
                )

            session_updates[deleted_node.session_id][1].ephemeral_nodes.discard(deleted_path)

        return session_updates

    def _ensure_parent_paths(self, path: str) -> List[str]:
        """Create missing persistent parents for a target path."""
        normalized = self._normalize_path(path)
        if normalized == "/":
            return []
        
        created_paths = []
        current = ""
        for segment in normalized.strip("/").split("/")[:-1]:
            current = f"{current}/{segment}" if current else f"/{segment}"
            if not self._metadata_tree.exists(current):
                self._create_node(
                    path=current,
                    data=b"",
                    persistent=True,
                    session_id=None,
                )
                created_paths.append(current)
        
        return created_paths

    def _encode_lease_payload(
        self,
        holder: str,
        metadata: Optional[Dict[str, Any]] = None,
        lease_ttl_seconds: Optional[float] = None,
    ) -> bytes:
        """Serialize lease metadata into the underlying node payload."""
        try:
            lease_expires_at = None
            if lease_ttl_seconds is not None:
                lease_expires_at = datetime.now().timestamp() + lease_ttl_seconds
            return json.dumps(
                {
                    "holder": holder,
                    "metadata": metadata or {},
                    "lease_ttl_seconds": lease_ttl_seconds,
                    "lease_expires_at": lease_expires_at,
                },
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("Lease metadata must be JSON-serializable") from exc

    def _decode_lease_record_data(
        self,
        data: bytes,
        fallback_holder: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any], Optional[float], Optional[float]]:
        """Decode holder, metadata, and optional independent lease TTL fields."""
        if not data:
            return fallback_holder or "", {}, None, None

        raw = data.decode("utf-8") if isinstance(data, bytes) else str(data)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return raw, {}, None, None

        if not isinstance(payload, dict):
            return raw, {}, None, None

        holder = payload.get("holder") or payload.get("data") or fallback_holder or ""
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}

        lease_ttl_seconds = payload.get("lease_ttl_seconds")
        if lease_ttl_seconds is not None:
            lease_ttl_seconds = float(lease_ttl_seconds)

        lease_expires_at = payload.get("lease_expires_at")
        if lease_expires_at is not None:
            lease_expires_at = float(lease_expires_at)

        return holder, metadata, lease_ttl_seconds, lease_expires_at

    def _decode_lease_payload_data(
        self,
        data: bytes,
        fallback_holder: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """Decode holder metadata from a raw lease payload."""
        holder, metadata, _, _ = self._decode_lease_record_data(data, fallback_holder)
        return holder, metadata

    def _decode_lease_payload(self, node: Node) -> Tuple[str, Dict[str, Any]]:
        """Decode holder metadata from a lease node."""
        return self._decode_lease_payload_data(node.data, node.session_id)

    def _get_lease_token(self, path: str) -> Optional[int]:
        """Return the create sequence for the currently held lease."""
        for operation in reversed(self._operation_log.get_all_operations()):
            if (
                operation.operation_type == OperationType.CREATE
                and operation.path == path
                and operation.node_type == NodeType.EPHEMERAL
            ):
                return operation.sequence_number
        return None

    def _decode_delete_paths(self, operation: Operation) -> List[str]:
        """Decode all paths affected by a delete operation."""
        if operation.operation_type != OperationType.DELETE:
            return []
        if not operation.data:
            return [operation.path] if operation.path else []

        try:
            payload = decode_delete_operation_payload(operation.data)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return [operation.path] if operation.path else []

        return [str(path).strip() for path in payload.get("paths", []) if str(path).strip()] or ([operation.path] if operation.path else [])

    def _decode_delete_cause(self, operation: Optional[Operation]) -> str:
        """Decode a delete cause token from an operation payload."""
        if operation is None or operation.operation_type != OperationType.DELETE or not operation.data:
            return DELETE_CAUSE_DELETE
        try:
            payload = decode_delete_operation_payload(operation.data)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return DELETE_CAUSE_DELETE
        return str(payload.get("cause") or DELETE_CAUSE_DELETE)

    def _get_operations_for_exact_path_locked(
        self,
        path: str,
        limit: Optional[int] = 20,
    ) -> List[Operation]:
        """Return the most recent operations that touched an exact path."""
        matched: List[Operation] = []
        for operation in reversed(self._operation_log.get_all_operations()):
            include = operation.path == path
            if not include and operation.operation_type == OperationType.DELETE and operation.data:
                deleted_paths = self._decode_delete_paths(operation)
                if path in deleted_paths:
                    include = True

            if include:
                matched.append(operation)
            if limit is not None and len(matched) >= limit:
                break

        return matched

    def _find_cleanup_delete_operations_locked(
        self,
        session_id: Optional[str],
        after_sequence: int,
    ) -> List[Operation]:
        """Return committed delete operations emitted after a session close."""
        if not session_id:
            return []

        matches: List[Operation] = []
        for operation in self._operation_log.get_all_operations():
            if operation.sequence_number <= after_sequence:
                continue
            if operation.session_id != session_id:
                continue
            if operation.operation_type != OperationType.DELETE:
                continue
            matches.append(operation)
        return matches

    def _collect_paths_for_operations(self, operations: List[Operation]) -> List[str]:
        """Flatten and de-duplicate affected paths for a set of operations."""
        paths: List[str] = []
        for operation in operations:
            if operation.operation_type == OperationType.DELETE:
                paths.extend(self._decode_delete_paths(operation))
            elif operation.path:
                paths.append(operation.path)
        return self._dedupe_paths(paths)

    def _dedupe_paths(self, paths: List[str]) -> List[str]:
        """Preserve input order while removing duplicate paths."""
        ordered: List[str] = []
        seen: Set[str] = set()
        for path in paths:
            if not path or path in seen:
                continue
            ordered.append(path)
            seen.add(path)
        return ordered

    def _build_incident_path_entry_locked(
        self,
        path: str,
        operation_sequence: int,
        change_kind: str,
    ) -> Dict[str, Any]:
        """Build a rich before/after path summary for an operation incident."""
        detail = self.get_path_detail(path, operation_limit=8, watch_limit=8)
        before_history = self._reconstruct_path_history_locked(path, through_sequence=operation_sequence - 1)
        after_history = self._reconstruct_path_history_locked(path, through_sequence=operation_sequence)
        current_history = self._reconstruct_path_history_locked(path)

        before_view = self._build_path_node_view_from_state_locked(before_history.get("current_state"))
        after_view = self._build_path_node_view_from_state_locked(after_history.get("current_state"))
        current_view = self._build_path_node_view_from_state_locked(current_history.get("current_state"))
        last_known_view = self._build_path_node_view_from_state_locked(current_history.get("last_known_state"))
        node = current_view or after_view or before_view or last_known_view or {}
        disappearance = detail.get("disappearance") if detail else None

        if current_view is not None and after_view is None:
            state = "recreated"
        elif current_view is not None:
            state = "live"
        elif disappearance is not None:
            state = "deleted"
        else:
            state = "historical"

        summary = {
            "created": "Path created in this operation.",
            "updated": "Path data changed in this operation.",
            "deleted": "Path deleted in this operation.",
        }.get(change_kind, "Path touched in this operation.")

        if before_view and after_view and before_view.get("version") is not None and after_view.get("version") is not None:
            summary = f"{summary.rstrip('.')} Version {before_view['version']} -> {after_view['version']}."

        note = ""
        if change_kind == "deleted" and disappearance is not None and disappearance.get("cause_kind"):
            note = f"Current history records this as {disappearance['cause_kind']}."
        elif state == "recreated":
            note = "This path was later recreated after the inspected operation."

        return {
            "path": path,
            "change_kind": change_kind,
            "summary": summary,
            "state": state,
            "session_id": node.get("session_id"),
            "node_type": node.get("node_type"),
            "holder": node.get("holder"),
            "data_preview": node.get("data_preview", ""),
            "modified_at": node.get("modified_at"),
            "version_before": before_view.get("version") if before_view else None,
            "version_after": after_view.get("version") if after_view else None,
            "before": before_view,
            "after": after_view,
            "note": note,
            "cause_kind": disappearance.get("cause_kind") if disappearance else None,
            "deleted_in_operation": change_kind == "deleted",
            "delete_sequence_number": operation_sequence if change_kind == "deleted" else None,
        }

    def _find_session_close_operation_locked(
        self,
        session_id: Optional[str],
        before_sequence: Optional[int] = None,
    ) -> Optional[Operation]:
        """Find the latest committed session-close operation for a session."""
        if not session_id:
            return None

        for operation in reversed(self._operation_log.get_all_operations()):
            if operation.operation_type != OperationType.SESSION_CLOSE:
                continue
            if operation.session_id != session_id:
                continue
            if before_sequence is not None and operation.sequence_number > before_sequence:
                continue
            return operation
        return None

    def _get_relevant_watches_for_path_locked(self, path: str) -> List[Watch]:
        """Return direct watches plus parent CHILDREN watches relevant to a path."""
        relevant: List[Watch] = []
        seen: Set[str] = set()

        for watch in self._watch_manager.get_watches_for_path(path):
            if watch.watch_id not in seen:
                relevant.append(watch)
                seen.add(watch.watch_id)

        parent_path = self._normalize_path(path).rsplit("/", 1)[0] if path != "/" else ""
        if not parent_path:
            parent_path = "/"
        if parent_path and parent_path != path:
            for watch in self._watch_manager.get_watches_for_path(parent_path):
                if EventType.CHILDREN not in watch.event_types:
                    continue
                if watch.watch_id in seen:
                    continue
                relevant.append(watch)
                seen.add(watch.watch_id)

        return relevant

    def _build_path_node_view(
        self,
        *,
        path: str,
        data: bytes,
        version: int,
        node_type: Optional[NodeType],
        session_id: Optional[str],
        created_at: float,
        modified_at: float,
        lease_token: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Build a serializable current or historical view of a path."""
        raw_data = data.decode("utf-8") if isinstance(data, bytes) else str(data)
        preview = raw_data if len(raw_data) <= 120 else raw_data[:117] + "..."
        holder = None
        metadata: Dict[str, Any] = {}
        if node_type == NodeType.EPHEMERAL:
            holder, metadata, lease_ttl_seconds, lease_expires_at = self._decode_lease_record_data(data, session_id)
            if lease_ttl_seconds is not None:
                metadata = {**metadata, "lease_ttl_seconds": lease_ttl_seconds}
            if lease_expires_at is not None:
                metadata = {**metadata, "lease_expires_at": lease_expires_at}

        return {
            "path": path,
            "node_type": node_type.value if node_type else None,
            "version": version,
            "session_id": session_id,
            "data_preview": preview,
            "created_at": created_at,
            "modified_at": modified_at,
            "holder": holder,
            "metadata": metadata,
            "lease_token": lease_token,
        }

    def _build_path_node_view_from_state_locked(
        self,
        state: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Convert an internal reconstructed state dictionary into an API view."""
        if state is None:
            return None

        return self._build_path_node_view(
            path=state["path"],
            data=state["data"],
            version=state["version"],
            node_type=state["node_type"],
            session_id=state["session_id"],
            created_at=state["created_at"],
            modified_at=state["modified_at"],
            lease_token=state.get("lease_token"),
        )

    def _reconstruct_path_history_locked(
        self,
        path: str,
        through_sequence: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Reconstruct a path lifecycle from committed operations up to a sequence."""
        chronological_ops = list(reversed(self._get_operations_for_exact_path_locked(path, limit=None)))
        if through_sequence is not None:
            chronological_ops = [
                operation
                for operation in chronological_ops
                if operation.sequence_number <= through_sequence
            ]
        state: Optional[Dict[str, Any]] = None
        last_known: Optional[Dict[str, Any]] = None
        last_delete: Optional[Operation] = None
        holder_history: List[Dict[str, Any]] = []

        for operation in chronological_ops:
            if operation.operation_type == OperationType.CREATE and operation.path == path:
                state = {
                    "path": path,
                    "data": operation.data,
                    "version": 1,
                    "node_type": operation.node_type,
                    "session_id": operation.session_id,
                    "created_at": operation.timestamp,
                    "modified_at": operation.timestamp,
                    "lease_token": operation.sequence_number if operation.node_type == NodeType.EPHEMERAL else None,
                }
                if operation.node_type == NodeType.EPHEMERAL:
                    holder, metadata = self._decode_lease_payload_data(operation.data, operation.session_id)
                    holder_history.append({
                        "sequence_number": operation.sequence_number,
                        "timestamp": operation.timestamp,
                        "session_id": operation.session_id,
                        "holder": holder,
                        "metadata": metadata,
                    })
                continue

            if operation.operation_type == OperationType.SET and operation.path == path and state is not None:
                state["data"] = operation.data
                state["version"] += 1
                state["modified_at"] = operation.timestamp
                continue

            if operation.operation_type == OperationType.DELETE and path in self._decode_delete_paths(operation):
                if state is not None:
                    last_known = dict(state)
                last_delete = operation
                state = None

        return {
            "current_state": state,
            "last_known_state": last_known,
            "last_delete": last_delete,
            "holder_history": holder_history,
            "chronological_operations": chronological_ops,
        }

    def _build_incident_session_views_locked(
        self,
        session_roles: Dict[str, Set[str]],
    ) -> List[Dict[str, Any]]:
        """Project involved sessions into a stable incident view."""
        priority = {
            "actor": 0,
            "cause": 1,
            "cleanup": 2,
            "owner": 3,
            "watcher": 4,
        }
        session_views: List[Dict[str, Any]] = []
        for session_id, roles in session_roles.items():
            summary = self._lookup_session_view_locked(session_id)
            ordered_roles = sorted(roles, key=lambda role: (priority.get(role, 99), role))
            if summary is None:
                session_views.append({
                    "session_id": session_id,
                    "created_at": 0.0,
                    "last_heartbeat": 0.0,
                    "timeout_seconds": 0,
                    "expires_at": 0.0,
                    "remaining_seconds": 0.0,
                    "is_alive": False,
                    "ephemeral_nodes": [],
                    "ephemeral_node_count": 0,
                    "watch_count": 0,
                    "roles": ordered_roles,
                })
            else:
                session_views.append({
                    **summary,
                    "roles": ordered_roles,
                })

        session_views.sort(
            key=lambda session: (
                min(priority.get(role, 99) for role in session["roles"]) if session["roles"] else 99,
                session["session_id"],
            )
        )
        return session_views

    def _lease_info_from_node(
        self,
        node: Node,
        lease_token: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Project an ephemeral node into a first-class lease view."""
        if node.node_type != NodeType.EPHEMERAL:
            raise ValueError(f"Node at path is not a lease: {node.path}")
        
        holder, metadata, lease_ttl_seconds, lease_expires_at = self._decode_lease_record_data(node.data, node.session_id)
        session = self._session_manager.get_session(node.session_id) if node.session_id else None
        expires_at = None
        if session is not None:
            expires_at = session.last_heartbeat + session.timeout_seconds
        if lease_expires_at is not None:
            expires_at = lease_expires_at if expires_at is None else min(expires_at, lease_expires_at)
        
        return {
            "path": node.path,
            "session_id": node.session_id,
            "holder": holder,
            "metadata": metadata,
            "version": node.version,
            "acquired_at": node.created_at,
            "modified_at": node.modified_at,
            "expires_at": expires_at,
            "lease_ttl_seconds": lease_ttl_seconds,
            "lease_token": lease_token if lease_token is not None else self._get_lease_token(node.path),
        }

    def get_lease(self, path: str) -> Optional[Dict[str, Any]]:
        """Get the current holder information for a lease path."""
        with self._lock:
            node = self._metadata_tree.get(path)
            if node is None:
                return None
            return self._lease_info_from_node(node)

    def get_lease_detail(
        self,
        path: str,
        operation_limit: int = 20,
    ) -> Optional[Dict[str, Any]]:
        """Return a drill-down view for a lease path."""
        normalized_path = self._normalize_path(path)
        with self._lock:
            node = self._metadata_tree.get(normalized_path)
            current_lease = None
            holder_session = None
            if node is not None and node.node_type == NodeType.EPHEMERAL:
                current_lease = self._lease_info_from_node(node)
                if node.session_id:
                    holder_session = next(
                        (
                            item for item in self._build_session_views_locked(alive_only=False)
                            if item["session_id"] == node.session_id
                        ),
                        None,
                    )
            waiters = self._get_lease_waiters_locked(normalized_path)
            recent_operations = self._get_operations_for_exact_path_locked(
                normalized_path,
                limit=operation_limit,
            )
            if current_lease is None and not recent_operations:
                return None

            holder_history = []
            for operation in recent_operations:
                if (
                    operation.operation_type == OperationType.CREATE
                    and operation.path == normalized_path
                    and operation.node_type == NodeType.EPHEMERAL
                ):
                    holder, metadata = self._decode_lease_payload_data(operation.data, operation.session_id)
                    holder_history.append({
                        "sequence_number": operation.sequence_number,
                        "timestamp": operation.timestamp,
                        "session_id": operation.session_id,
                        "holder": holder,
                        "metadata": metadata,
                    })

            return {
                "path": normalized_path,
                "current_lease": current_lease,
                "holder_session": holder_session,
                "waiters": waiters,
                "waiter_count": len(waiters),
                "holder_history": holder_history,
                "recent_operations": recent_operations,
            }

    def acquire_lease(
        self,
        path: str,
        session_id: str,
        holder: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        lease_ttl_seconds: Optional[float] = None,
        wait_timeout_seconds: float = 0.0,
        create_parents: bool = True,
    ) -> Dict[str, Any]:
        """
        Acquire an exclusive lease backed by an ephemeral node.
        
        The returned lease_token is a monotonic fencing token derived
        from the committed create operation sequence number.
        """
        if wait_timeout_seconds < 0:
            raise ValueError("wait_timeout_seconds must be non-negative")
        if lease_ttl_seconds is not None and lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        
        normalized_path = self._normalize_path(path)
        holder = holder or session_id
        deadline = time.monotonic() + wait_timeout_seconds
        
        while True:
            with self._lock:
                if not self._session_manager.is_alive(session_id):
                    self._dequeue_lease_waiter_locked(normalized_path, session_id)
                    raise ValueError(f"Session is not alive: {session_id}")

                if create_parents:
                    self._ensure_parent_paths(normalized_path)
                
                existing = self._metadata_tree.get(normalized_path)
                waiters = self._get_lease_waiters_locked(normalized_path)
                if existing is None and waiters and waiters[0] != session_id:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self._dequeue_lease_waiter_locked(normalized_path, session_id)
                        raise ConflictError(
                            f"Lease already held: {normalized_path}",
                            error="lease_conflict",
                            path=normalized_path,
                        )
                    self._lease_wait_condition.wait(timeout=remaining)
                    continue

                if existing is None:
                    self._dequeue_lease_waiter_locked(normalized_path, session_id)
                    node, operation = self._create_node(
                        path=normalized_path,
                        data=self._encode_lease_payload(
                            holder,
                            metadata,
                            lease_ttl_seconds=lease_ttl_seconds,
                        ),
                        persistent=False,
                        session_id=session_id,
                    )
                    self._notify_lease_waiters_locked()
                    logger.info(f"Lease acquired: {normalized_path} (session={session_id})")
                    return self._lease_info_from_node(
                        node,
                        lease_token=operation.sequence_number,
                    )
                
                if existing.node_type != NodeType.EPHEMERAL:
                    raise ConflictError(
                        f"Cannot acquire lease on non-ephemeral node: {normalized_path}",
                        error="lease_conflict",
                        path=normalized_path,
                    )
                
                if existing.session_id == session_id:
                    self._dequeue_lease_waiter_locked(normalized_path, session_id)
                    return self._lease_info_from_node(existing)
                
                if wait_timeout_seconds <= 0:
                    raise ConflictError(
                        f"Lease already held: {normalized_path}",
                        error="lease_conflict",
                        path=normalized_path,
                    )

                waiters = self._enqueue_lease_waiter_locked(normalized_path, session_id)
                remaining = deadline - time.monotonic()
                if waiters and waiters[0] != session_id:
                    if remaining <= 0:
                        self._dequeue_lease_waiter_locked(normalized_path, session_id)
                        raise ConflictError(
                            f"Lease already held: {normalized_path}",
                            error="lease_conflict",
                            path=normalized_path,
                        )
                    self._lease_wait_condition.wait(timeout=remaining)
                    continue

            if remaining <= 0:
                with self._lock:
                    self._dequeue_lease_waiter_locked(normalized_path, session_id)
                raise ConflictError(
                    f"Lease already held: {normalized_path}",
                    error="lease_conflict",
                    path=normalized_path,
                )

            with self._lock:
                self._lease_wait_condition.wait(timeout=remaining)

    def release_lease(self, path: str, session_id: str) -> bool:
        """Release a lease, but only if the caller owns it."""
        with self._lock:
            normalized_path = self._normalize_path(path)
            node = self._metadata_tree.get(normalized_path)
            if node is None:
                raise KeyError(f"Lease does not exist: {normalized_path}")
            if node.node_type != NodeType.EPHEMERAL:
                raise ValueError(f"Node at path is not a lease: {normalized_path}")
            if node.session_id != session_id:
                raise ForbiddenError(
                    f"Lease is owned by another session: {normalized_path}",
                    error="lease_forbidden",
                    path=normalized_path,
                    owner_session_id=node.session_id,
                )
            self.delete(normalized_path, cause=DELETE_CAUSE_LEASE_RELEASE)
            logger.info(f"Lease released: {normalized_path} (session={session_id})")
            return True

    def renew_lease(
        self,
        path: str,
        session_id: str,
        lease_ttl_seconds: float,
    ) -> Dict[str, Any]:
        """Extend or reset the independent TTL for a currently owned lease."""
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")

        with self._lock:
            normalized_path = self._normalize_path(path)
            if not self._session_manager.is_alive(session_id):
                raise ValueError(f"Session is not alive: {session_id}")

            node = self._metadata_tree.get(normalized_path)
            if node is None:
                raise KeyError(f"Lease does not exist: {normalized_path}")
            if node.node_type != NodeType.EPHEMERAL:
                raise ValueError(f"Node at path is not a lease: {normalized_path}")
            if node.session_id != session_id:
                raise ForbiddenError(
                    f"Lease is owned by another session: {normalized_path}",
                    error="lease_forbidden",
                    path=normalized_path,
                    owner_session_id=node.session_id,
                )

            holder, metadata, _, _ = self._decode_lease_record_data(node.data, node.session_id)
            updated_node = self.set(
                normalized_path,
                self._encode_lease_payload(
                    holder,
                    metadata,
                    lease_ttl_seconds=lease_ttl_seconds,
                ),
            )

            logger.info(
                f"Lease renewed: {normalized_path} (session={session_id}, ttl={lease_ttl_seconds}s)"
            )
            return self._lease_info_from_node(updated_node)

    def apply_replicated_operations(self, operations: List[Operation]) -> int:
        """Apply an ordered batch of committed operations from a leader."""
        applied = 0
        for operation in operations:
            if self.apply_replicated_operation(operation):
                applied += 1
        return applied

    def apply_replicated_operation(self, operation: Operation) -> bool:
        """
        Apply one externally committed operation without allocating a new sequence.

        Returns True when applied, False when already present locally.
        """
        with self._lock:
            if self._operation_log.get_operation(operation.sequence_number) is not None:
                return False

            next_sequence = self._operation_log.current_sequence + 1
            if operation.sequence_number != next_sequence:
                raise ValueError(
                    f"Replicated operation sequence gap: expected {next_sequence}, "
                    f"got {operation.sequence_number}"
                )

            if operation.operation_type == OperationType.SESSION_OPEN:
                session, _ = self._decode_session_operation_payload(operation)
                if session is None:
                    raise ValueError("SESSION_OPEN replication requires session payload")
                self._persistence.atomic_save_session(session, operation)
                self._session_manager.replace_session(session)
                self._operation_log.append_external_committed(operation)
                self._mark_session_inventory_changed()
                return True

            if operation.operation_type == OperationType.SESSION_HEARTBEAT:
                session, _ = self._decode_session_operation_payload(operation)
                if session is None:
                    raise ValueError("SESSION_HEARTBEAT replication requires session payload")
                self._persistence.atomic_save_session(session, operation)
                self._session_manager.replace_session(session)
                self._operation_log.append_external_committed(operation)
                self._mark_session_inventory_changed()
                return True

            if operation.operation_type == OperationType.SESSION_CLOSE:
                session, _ = self._decode_session_operation_payload(operation)
                if session is None:
                    raise ValueError("SESSION_CLOSE replication requires session payload")
                self._persistence.atomic_save_session(session, operation)
                self._session_manager.replace_session(session)
                self._operation_log.append_external_committed(operation)
                self._mark_session_inventory_changed()
                return True

            if operation.operation_type == OperationType.CREATE:
                node_type = operation.node_type or NodeType.PERSISTENT
                node = Node(
                    path=operation.path,
                    data=operation.data,
                    version=1,
                    node_type=node_type,
                    session_id=operation.session_id,
                    created_at=operation.timestamp,
                    modified_at=operation.timestamp,
                )
                persisted_session = None
                if node_type == NodeType.EPHEMERAL and operation.session_id:
                    live_session = self._session_manager.get_session(operation.session_id)
                    if live_session is None:
                        raise KeyError(
                            f"Replica cannot apply ephemeral create without session: "
                            f"{operation.session_id}"
                        )
                    persisted_session = self._clone_session(live_session)
                    persisted_session.ephemeral_nodes.add(node.path)

                self._persistence.atomic_create_node(
                    node,
                    operation,
                    persisted_session,
                    watch_fires=None,
                )
                committed_node = self._metadata_tree.commit_create(node)
                if node_type == NodeType.EPHEMERAL and operation.session_id:
                    self._session_manager.add_ephemeral_node(operation.session_id, committed_node.path)
                    self._mark_session_inventory_changed()
                self._operation_log.append_external_committed(operation)
                self._watch_manager.trigger(
                    path=committed_node.path,
                    event_type=EventType.CREATE,
                    data=committed_node.data,
                    sequence_number=operation.sequence_number,
                )
                return True

            if operation.operation_type == OperationType.SET:
                current = self._metadata_tree.get(operation.path)
                if current is None:
                    raise KeyError(f"Replica cannot apply SET for missing node: {operation.path}")
                updated = Node(
                    path=current.path,
                    data=operation.data,
                    version=current.version + 1,
                    node_type=current.node_type,
                    session_id=current.session_id,
                    created_at=current.created_at,
                    modified_at=operation.timestamp,
                )
                self._persistence.atomic_update_node(updated, operation, watch_fires=None)
                self._metadata_tree.commit_set(updated)
                self._operation_log.append_external_committed(operation)
                self._watch_manager.trigger(
                    path=updated.path,
                    event_type=EventType.UPDATE,
                    data=updated.data,
                    sequence_number=operation.sequence_number,
                )
                return True

            if operation.operation_type == OperationType.DELETE:
                deleted_paths = self._decode_delete_paths(operation)
                session_updates = self._build_session_updates_for_deleted_paths(deleted_paths)
                self._persistence.atomic_delete_node(
                    deleted_paths,
                    operation,
                    sessions=[snapshot for _, snapshot in session_updates.values()],
                    watch_fires=None,
                )
                self._metadata_tree.apply_delete_paths(deleted_paths)
                for live_session, snapshot in session_updates.values():
                    live_session.ephemeral_nodes.clear()
                    live_session.ephemeral_nodes.update(snapshot.ephemeral_nodes)
                if session_updates:
                    self._mark_session_inventory_changed()
                self._operation_log.append_external_committed(operation)
                for deleted_path in deleted_paths:
                    self._watch_manager.trigger(
                        path=deleted_path,
                        event_type=EventType.DELETE,
                        sequence_number=operation.sequence_number,
                    )
                self._notify_lease_waiters_locked()
                return True

            raise ValueError(f"Unsupported replicated operation: {operation.operation_type.value}")
    
    def get(self, path: str) -> Optional[Node]:
        """
        Get a node by path.
        
        Args:
            path: The path to retrieve
            
        Returns:
            The Node if it exists, None otherwise
        """
        with self._lock:
            return self._metadata_tree.get(path)
    
    def set(
        self,
        path: str,
        data: bytes,
        expected_version: Optional[int] = None,
    ) -> Node:
        """
        Update a node's data.

        Args:
            path: The path to update
            data: The new data
            expected_version: Optional compare-and-swap guard
            
        Returns:
            The updated Node
            
        Raises:
            KeyError: If node doesn't exist
        """
        with self._lock:
            self._assert_writable()
            # Convert data to bytes if needed
            if isinstance(data, str):
                data = data.encode("utf-8")
            
            node = self._metadata_tree.prepare_set(
                path,
                data,
                expected_version=expected_version,
            )
            
            # Log operation
            operation = self._operation_log.append(
                operation_type=OperationType.SET,
                path=path,
                data=data,
            )
            watch_fires = self._collect_watch_fire_records_locked(
                [path],
                event_type=EventType.UPDATE,
                sequence_number=operation.sequence_number,
                timestamp=operation.timestamp,
            )
            
            try:
                self._persistence.atomic_update_node(
                    node,
                    operation,
                    watch_fires=watch_fires,
                )
            except Exception:
                self._operation_log.discard_last_operation(operation.sequence_number)
                raise

            committed_node = self._metadata_tree.commit_set(node)
            self._operation_log.commit(operation)
            
            # Trigger watches
            self._watch_manager.trigger(
                path=path,
                event_type=EventType.UPDATE,
                data=data,
                sequence_number=operation.sequence_number,
            )
            
            logger.info(f"Updated node: {path} (version={committed_node.version})")
            
            return committed_node
    
    def delete(self, path: str, cause: str = DELETE_CAUSE_DELETE) -> List[str]:
        """
        Delete a node and all its children.
        
        Args:
            path: The path to delete
            
        Returns:
            List of deleted paths
            
        Raises:
            KeyError: If node doesn't exist
            ValueError: If trying to delete root
        """
        with self._lock:
            self._assert_writable()
            # Get node info before deletion
            node = self._metadata_tree.get(path)
            if node is None:
                raise KeyError(f"Node does not exist: {path}")

            deleted_paths = self._metadata_tree.plan_delete(path, recursive=True)
            session_updates = self._build_session_updates_for_deleted_paths(deleted_paths)
            
            # Log operation
            operation = self._operation_log.append(
                operation_type=OperationType.DELETE,
                path=path,
                session_id=node.session_id,
            )
            operation.data = encode_delete_operation_payload(deleted_paths, cause=cause)
            watch_fires = self._collect_watch_fire_records_locked(
                deleted_paths,
                event_type=EventType.DELETE,
                sequence_number=operation.sequence_number,
                timestamp=operation.timestamp,
            )
            
            try:
                self._persistence.atomic_delete_node(
                    deleted_paths,
                    operation,
                    sessions=[snapshot for _, snapshot in session_updates.values()],
                    watch_fires=watch_fires,
                )
            except Exception:
                self._operation_log.discard_last_operation(operation.sequence_number)
                raise

            self._metadata_tree.apply_delete_paths(deleted_paths)
            for _, (live_session, snapshot) in session_updates.items():
                live_session.ephemeral_nodes.clear()
                live_session.ephemeral_nodes.update(snapshot.ephemeral_nodes)
            if session_updates:
                self._mark_session_inventory_changed()
            self._operation_log.commit(operation)
            
            # Trigger watches for each deleted node
            for deleted_path in deleted_paths:
                self._watch_manager.trigger(
                    path=deleted_path,
                    event_type=EventType.DELETE,
                    sequence_number=operation.sequence_number,
                )
            self._notify_lease_waiters_locked()
            
            logger.info(f"Deleted node: {path} (and {len(deleted_paths)-1} children)")
            
            return deleted_paths
    
    def exists(self, path: str) -> bool:
        """Check if a node exists."""
        with self._lock:
            return self._metadata_tree.exists(path)
    
    def list_children(self, path: str) -> List[str]:
        """
        List the direct children of a node.
        
        Args:
            path: The parent path
            
        Returns:
            List of child node names
            
        Raises:
            KeyError: If parent doesn't exist
        """
        with self._lock:
            return self._metadata_tree.list_children(path)

    def get_operations(
        self,
        since_sequence: int = 0,
        limit: int = 100,
        operation_types: Optional[Set[OperationType]] = None,
        path_prefix: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> List[Operation]:
        """Return committed operations with optional filtering."""
        with self._lock:
            operations = self._operation_log.get_operations_since(since_sequence)

        operations = self._filter_operations(
            operations,
            operation_types=operation_types,
            path_prefix=path_prefix,
            session_id=session_id,
        )

        if limit <= 0:
            return []
        return operations[:limit]

    def get_operation(self, sequence_number: int) -> Optional[Operation]:
        """Return a committed operation by sequence number."""
        with self._lock:
            return self._operation_log.get_operation(sequence_number)

    def _filter_operations(
        self,
        operations: List[Operation],
        operation_types: Optional[Set[OperationType]] = None,
        path_prefix: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> List[Operation]:
        """Apply read-side operation filters."""
        filtered = operations
        if operation_types:
            filtered = [op for op in filtered if op.operation_type in operation_types]
        if path_prefix:
            filtered = [op for op in filtered if op.path.startswith(path_prefix)]
        if session_id:
            filtered = [op for op in filtered if op.session_id == session_id]
        return filtered

    def wait_for_operations(
        self,
        since_sequence: int = 0,
        timeout_seconds: float = 30.0,
        limit: int = 100,
        operation_types: Optional[Set[OperationType]] = None,
        path_prefix: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> List[Operation]:
        """Wait for committed operations that match the provided filters."""
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")

        deadline = time.monotonic() + timeout_seconds
        current_since = since_sequence

        while True:
            with self._lock:
                raw_operations = self._operation_log.get_operations_since(current_since)
            operations = self._filter_operations(
                raw_operations,
                operation_types=operation_types,
                path_prefix=path_prefix,
                session_id=session_id,
            )
            if operations:
                return operations[:limit]

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return []

            raw_operations = self._operation_log.wait_for_operations_since(
                current_since,
                remaining,
            )
            if not raw_operations:
                return []
            operations = self._filter_operations(
                raw_operations,
                operation_types=operation_types,
                path_prefix=path_prefix,
                session_id=session_id,
            )
            if operations:
                return operations[:limit]
            current_since = raw_operations[-1].sequence_number
    
    # ========== Watch Operations ==========
    
    def register_watch(
        self,
        path: str,
        session_id: str,
        event_types: Optional[Set[EventType]] = None,
    ) -> Watch:
        """
        Register a watch on a path.
        
        Args:
            path: The path to watch
            session_id: The session registering the watch
            event_types: Types of events to watch for (default: all)
            
        Returns:
            The created Watch
            
        Raises:
            ValueError: If session is not alive
        """
        with self._lock:
            if not self._session_manager.is_alive(session_id):
                raise ValueError(f"Session is not alive: {session_id}")
            
            watch = self._watch_manager.register(
                path=path,
                session_id=session_id,
                event_types=event_types,
            )
            self._mark_session_inventory_changed()
            
            logger.info(f"Registered watch: {watch.watch_id} on {path}")
            
            return watch
    
    def wait_watch(
        self,
        watch_id: str,
        timeout_seconds: float = 30.0,
    ) -> Optional[Event]:
        """
        Wait for a watch to fire.
        
        Args:
            watch_id: The watch ID to wait on
            timeout_seconds: Maximum time to wait
            
        Returns:
            The Event if watch fired, None on timeout
        """
        # Don't hold the lock while waiting
        return self._watch_manager.wait(watch_id, timeout_seconds)
    
    def unregister_watch(self, watch_id: str) -> bool:
        """
        Unregister a watch.
        
        Returns:
            True if watch was found and removed
        """
        with self._lock:
            result = self._watch_manager.unregister(watch_id)
            if result:
                self._mark_session_inventory_changed()
                logger.info(f"Unregistered watch: {watch_id}")
            return result
    
    # ========== Health & Stats ==========
    
    def get_health(self) -> dict:
        """Get the health status of the service."""
        with self._lock:
            uptime = datetime.now().timestamp() - self._start_time
            
            return {
                "status": "healthy" if self._started else "not_started",
                "nodes_count": self._metadata_tree.get_node_count(),
                "sessions_count": self._session_manager.get_alive_session_count(),
                "watches_count": self._watch_manager.get_watch_count(),
                "uptime_seconds": int(uptime),
                "last_sequence": self._operation_log.current_sequence,
            }
    
    def get_stats(self) -> dict:
        """Get detailed statistics."""
        with self._lock:
            return {
                "nodes": self._metadata_tree.get_node_count(),
                "sessions_total": self._session_manager.get_session_count(),
                "sessions_alive": self._session_manager.get_alive_session_count(),
                "watches": self._watch_manager.get_watch_count(),
                "operations": len(self._operation_log),
                "last_sequence": self._operation_log.current_sequence,
                "persistence": self._persistence.get_stats(),
            }
    
    def verify_consistency(self) -> Tuple[bool, List[str]]:
        """Verify system consistency."""
        return self._recovery_manager.verify_consistency()

    def get_last_recovery_stats(self) -> Dict[str, Any]:
        """Return the most recent startup recovery report."""
        with self._lock:
            return dict(self._last_recovery_stats)
