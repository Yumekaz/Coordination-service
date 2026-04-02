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
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime

from models import Node, Session, Watch, Event, Operation, NodeType, EventType, OperationType
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
            
            self._started = True
            self._start_time = datetime.now().timestamp()
            
            logger.info("Coordinator started")
            
            return recovery_stats
    
    def stop(self) -> None:
        """Stop the coordination service."""
        with self._lock:
            if not self._started:
                return
            
            self._session_manager.stop()
            self._persistence.close()
            self._started = False
            
            logger.info("Coordinator stopped")
    
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
            session = self._session_manager.open_session(timeout_seconds)
            operation = self._operation_log.append(
                operation_type=OperationType.SESSION_OPEN,
                session_id=session.session_id,
            )
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
            existing = self._session_manager.get_session(session_id)
            if existing is None:
                raise KeyError(f"Session does not exist: {session_id}")
            snapshot = self._clone_session(existing)
            session = self._session_manager.heartbeat(session_id)
            
            operation = self._operation_log.append(
                operation_type=OperationType.SESSION_HEARTBEAT,
                session_id=session_id,
            )
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
    
    def _on_session_expired(self, session: Session) -> None:
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

            if deleted_paths:
                operation = self._operation_log.append(
                    operation_type=OperationType.DELETE,
                    path=",".join(deleted_paths),
                    session_id=session.session_id,
                )
                try:
                    self._persistence.atomic_delete_node(
                        deleted_paths,
                        operation,
                        sessions=[persisted_session],
                    )
                except Exception:
                    self._operation_log.discard_last_operation(operation.sequence_number)
                    raise

                self._metadata_tree.apply_delete_paths(deleted_paths)
                session.ephemeral_nodes.clear()
                self._operation_log.commit(operation)

                for path in deleted_paths:
                    self._watch_manager.trigger(
                        path=path,
                        event_type=EventType.DELETE,
                        sequence_number=operation.sequence_number,
                    )
            else:
                self._persistence.save_session(persisted_session)

            self._watch_manager.clear_session_watches(session.session_id)

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
        
        try:
            self._persistence.atomic_create_node(node, operation, persisted_session)
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
    ) -> bytes:
        """Serialize lease metadata into the underlying node payload."""
        try:
            return json.dumps(
                {
                    "holder": holder,
                    "metadata": metadata or {},
                },
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("Lease metadata must be JSON-serializable") from exc

    def _decode_lease_payload(self, node: Node) -> Tuple[str, Dict[str, Any]]:
        """Decode holder metadata from a lease node."""
        if not node.data:
            return node.session_id or "", {}
        
        raw = node.data.decode("utf-8") if isinstance(node.data, bytes) else str(node.data)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return raw, {}
        
        if not isinstance(payload, dict):
            return raw, {}
        
        holder = payload.get("holder") or payload.get("data") or node.session_id or ""
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        
        return holder, metadata

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

    def _lease_info_from_node(
        self,
        node: Node,
        lease_token: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Project an ephemeral node into a first-class lease view."""
        if node.node_type != NodeType.EPHEMERAL:
            raise ValueError(f"Node at path is not a lease: {node.path}")
        
        holder, metadata = self._decode_lease_payload(node)
        session = self._session_manager.get_session(node.session_id) if node.session_id else None
        expires_at = None
        if session is not None:
            expires_at = session.last_heartbeat + session.timeout_seconds
        
        return {
            "path": node.path,
            "session_id": node.session_id,
            "holder": holder,
            "metadata": metadata,
            "version": node.version,
            "acquired_at": node.created_at,
            "modified_at": node.modified_at,
            "expires_at": expires_at,
            "lease_token": lease_token if lease_token is not None else self._get_lease_token(node.path),
        }

    def get_lease(self, path: str) -> Optional[Dict[str, Any]]:
        """Get the current holder information for a lease path."""
        with self._lock:
            node = self._metadata_tree.get(path)
            if node is None:
                return None
            return self._lease_info_from_node(node)

    def acquire_lease(
        self,
        path: str,
        session_id: str,
        holder: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
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
        
        normalized_path = self._normalize_path(path)
        holder = holder or session_id
        deadline = time.monotonic() + wait_timeout_seconds
        
        while True:
            with self._lock:
                if not self._session_manager.is_alive(session_id):
                    raise ValueError(f"Session is not alive: {session_id}")

                if create_parents:
                    self._ensure_parent_paths(normalized_path)
                
                existing = self._metadata_tree.get(normalized_path)
                if existing is None:
                    node, operation = self._create_node(
                        path=normalized_path,
                        data=self._encode_lease_payload(holder, metadata),
                        persistent=False,
                        session_id=session_id,
                    )
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
                    return self._lease_info_from_node(existing)
                
                if wait_timeout_seconds <= 0:
                    raise ConflictError(
                        f"Lease already held: {normalized_path}",
                        error="lease_conflict",
                        path=normalized_path,
                    )
                
                watch = self._watch_manager.register(
                    path=normalized_path,
                    session_id=session_id,
                    event_types={EventType.DELETE},
                )
                
                # Re-check after watch registration so we do not miss a release.
                if not self._metadata_tree.exists(normalized_path):
                    self._watch_manager.unregister(watch.watch_id)
                    continue
                
                watch_id = watch.watch_id
                remaining = deadline - time.monotonic()
            
            if remaining <= 0:
                self.unregister_watch(watch_id)
                raise ConflictError(
                    f"Lease already held: {normalized_path}",
                    error="lease_conflict",
                    path=normalized_path,
                )
            
            event = self.wait_watch(watch_id, remaining)
            self.unregister_watch(watch_id)
            if event is None:
                raise ConflictError(
                    f"Lease already held: {normalized_path}",
                    error="lease_conflict",
                    path=normalized_path,
                )

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
            self.delete(normalized_path)
            logger.info(f"Lease released: {normalized_path} (session={session_id})")
            return True
    
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
            
            try:
                self._persistence.atomic_update_node(node, operation)
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
    
    def delete(self, path: str) -> List[str]:
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
            
            try:
                self._persistence.atomic_delete_node(
                    deleted_paths,
                    operation,
                    sessions=[snapshot for _, snapshot in session_updates.values()],
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
