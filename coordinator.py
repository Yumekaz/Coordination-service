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

import threading
from typing import List, Optional, Set, Tuple
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
            # Create session
            session = self._session_manager.open_session(timeout_seconds)
            
            # Log operation
            self._operation_log.append(
                operation_type=OperationType.SESSION_OPEN,
                session_id=session.session_id,
            )
            
            # Persist
            self._persistence.save_session(session)
            
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
            session = self._session_manager.heartbeat(session_id)
            
            # Log operation
            self._operation_log.append(
                operation_type=OperationType.SESSION_HEARTBEAT,
                session_id=session_id,
            )
            
            # Persist updated heartbeat time
            self._persistence.save_session(session)
            
            return session
    
    def close_session(self, session_id: str) -> Optional[Session]:
        """
        Close a session explicitly.
        
        This triggers ephemeral node cleanup.
        
        Returns:
            The closed Session, or None if not found
        """
        with self._lock:
            session = self._session_manager.close_session(session_id)
            
            if session:
                # Log operation
                self._operation_log.append(
                    operation_type=OperationType.SESSION_CLOSE,
                    session_id=session_id,
                )
                
                # Persist
                self._persistence.save_session(session)
            
            return session
    
    def _on_session_expired(self, session: Session) -> None:
        """
        Handle session expiration.
        
        Called by SessionManager when a session times out.
        Deletes all ephemeral nodes and triggers watches.
        """
        with self._lock:
            logger.info(f"Handling session expiry: {session.session_id}")
            
            # Delete ephemeral nodes
            deleted_paths = self._metadata_tree.delete_session_nodes(session.session_id)
            
            # Persist deletions
            if deleted_paths:
                operation = self._operation_log.append(
                    operation_type=OperationType.DELETE,
                    path=",".join(deleted_paths),
                    session_id=session.session_id,
                )
                self._persistence.delete_nodes(deleted_paths)
                
                # Trigger watches for each deleted node
                for path in deleted_paths:
                    self._watch_manager.trigger(
                        path=path,
                        event_type=EventType.DELETE,
                        sequence_number=operation.sequence_number,
                    )
            
            # Clear session's watches
            self._watch_manager.clear_session_watches(session.session_id)
            
            # Persist session state
            self._persistence.save_session(session)
            
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
            
            # Create node in tree
            node = self._metadata_tree.create(
                path=path,
                data=data,
                node_type=node_type,
                session_id=session_id,
            )
            
            # Track ephemeral node in session
            session = None
            if node_type == NodeType.EPHEMERAL:
                self._session_manager.add_ephemeral_node(session_id, path)
                session = self._session_manager.get_session(session_id)
            
            # Log operation
            operation = self._operation_log.append(
                operation_type=OperationType.CREATE,
                path=path,
                data=data,
                session_id=session_id,
                node_type=node_type,
            )
            
            # Persist atomically
            self._persistence.atomic_create_node(node, operation, session)
            
            # Trigger watches
            self._watch_manager.trigger(
                path=path,
                event_type=EventType.CREATE,
                data=data,
                sequence_number=operation.sequence_number,
            )
            
            logger.info(f"Created node: {path} (type={node_type.value})")
            
            return node
    
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
    
    def set(self, path: str, data: bytes) -> Node:
        """
        Update a node's data.
        
        Args:
            path: The path to update
            data: The new data
            
        Returns:
            The updated Node
            
        Raises:
            KeyError: If node doesn't exist
        """
        with self._lock:
            # Convert data to bytes if needed
            if isinstance(data, str):
                data = data.encode("utf-8")
            
            # Update node
            node = self._metadata_tree.set(path, data)
            
            # Log operation
            operation = self._operation_log.append(
                operation_type=OperationType.SET,
                path=path,
                data=data,
            )
            
            # Persist atomically
            self._persistence.atomic_update_node(node, operation)
            
            # Trigger watches
            self._watch_manager.trigger(
                path=path,
                event_type=EventType.UPDATE,
                data=data,
                sequence_number=operation.sequence_number,
            )
            
            logger.info(f"Updated node: {path} (version={node.version})")
            
            return node
    
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
            
            # Delete from tree
            deleted_paths = self._metadata_tree.delete(path, recursive=True)
            
            # Update session tracking for ephemeral nodes
            session = None
            if node.session_id:
                self._session_manager.remove_ephemeral_node(node.session_id, path)
                session = self._session_manager.get_session(node.session_id)
            
            # Log operation
            operation = self._operation_log.append(
                operation_type=OperationType.DELETE,
                path=path,
                session_id=node.session_id,
            )
            
            # Persist atomically
            self._persistence.atomic_delete_node(deleted_paths, operation, session)
            
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
