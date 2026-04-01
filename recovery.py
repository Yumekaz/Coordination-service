"""
Crash Recovery for the Coordination Service.

Implements the recovery algorithm from Spec Section 12:
1. Load last valid snapshot (if any)
2. Replay WAL entries in order
3. Rebuild metadata tree from operations
4. Rebuild session state (all marked dead)
5. Remove ephemeral nodes belonging to expired sessions
6. Clear all watches (clients lost connection)
7. Resume serving requests

Properties (Section 12):
- Deterministic (same crash, same recovery)
- Idempotent (replay multiple times safe)
- Safe under repeated crashes (no data loss)

Invariants (Section 13):
- Tree Consistency: No orphaned nodes, no partial creates
- Session Invariant: No ephemeral node without live session
- Watch Invariant: Each watch fires at most once
- Durability Invariant: Acknowledged operations survive crashes
"""

import json
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from models import Node, Session, Operation, NodeType, EventType, OperationType
from metadata_tree import MetadataTree
from session_manager import SessionManager
from watch_manager import WatchManager
from operation_log import OperationLog
from persistence import Persistence
from logger import get_logger

logger = get_logger("recovery")


class RecoveryManager:
    """
    Manages crash recovery for the coordination service.
    
    Recovery guarantees (Section 12):
    - Deterministic: Same state after every recovery
    - Idempotent: Safe to run multiple times
    - Complete: All committed operations survive
    - Consistent: Tree structure is valid after recovery
    """
    
    def __init__(
        self,
        persistence: Persistence,
        metadata_tree: MetadataTree,
        session_manager: SessionManager,
        watch_manager: WatchManager,
        operation_log: OperationLog,
    ):
        """Initialize the recovery manager."""
        self._persistence = persistence
        self._metadata_tree = metadata_tree
        self._session_manager = session_manager
        self._watch_manager = watch_manager
        self._operation_log = operation_log
        
        logger.info("RecoveryManager initialized")
    
    def recover(self) -> dict:
        """
        Perform crash recovery per Spec Section 12.
        
        Recovery Algorithm:
        1. Load last valid snapshot (if any) 
        2. Replay WAL entries in order
        3. Rebuild metadata tree from operations
        4. Rebuild session state (all marked dead)
        5. Remove ephemeral nodes belonging to expired sessions
        6. Clear all watches (clients lost connection)
        7. Resume serving requests
        
        Returns:
            Recovery statistics
        """
        logger.info("Starting crash recovery (Section 12 algorithm)...")
        start_time = datetime.now().timestamp()
        
        stats = {
            "snapshot_loaded": False,
            "wal_entries_read": 0,
            "operations_replayed": 0,
            "nodes_loaded": 0,
            "sessions_loaded": 0,
            "sessions_expired": 0,
            "ephemeral_nodes_deleted": 0,
            "recovery_time_ms": 0,
        }
        
        # Step 1: Load last valid snapshot (if any)
        # Snapshots are stored in SQLite as the baseline state
        snapshot_data = self._load_snapshot()
        if snapshot_data:
            stats["snapshot_loaded"] = True
            logger.info("Loaded snapshot from SQLite baseline")
        
        # Step 2: Read WAL entries 
        # Per Section 10, WAL contains: [timestamp][operation_type][path][data][session_id][sequence_number]
        wal_operations = self._persistence.read_wal()
        stats["wal_entries_read"] = len(wal_operations)
        logger.info(f"Read {len(wal_operations)} entries from WAL file")
        
        # Step 3: Replay WAL entries to rebuild state
        # This is the key step - replay operations in sequence order
        if wal_operations:
            replayed = self._replay_wal_operations(wal_operations)
            stats["operations_replayed"] = replayed
            logger.info(f"Replayed {replayed} operations from WAL")
        
        # Load current state after replay
        nodes = self._persistence.load_all_nodes()
        stats["nodes_loaded"] = len(nodes)
        logger.info(f"Loaded {len(nodes)} nodes from database")
        
        # Step 4: Load and mark all sessions as dead
        # After a crash, all clients have lost connection
        sessions = self._persistence.load_all_sessions()
        stats["sessions_loaded"] = len(sessions)
        
        for session in sessions:
            session.is_alive = False
        self._persistence.mark_all_sessions_dead()
        stats["sessions_expired"] = len(sessions)
        logger.info(f"Marked {len(sessions)} sessions as dead")
        
        # Step 5: Remove ephemeral nodes belonging to expired sessions
        # Ephemeral nodes cannot outlive their sessions (Session Invariant)
        ephemeral_paths = []
        for node in nodes:
            if node.node_type == NodeType.EPHEMERAL:
                ephemeral_paths.append(node.path)
        
        if ephemeral_paths:
            self._persistence.delete_nodes(ephemeral_paths)
            stats["ephemeral_nodes_deleted"] = len(ephemeral_paths)
            logger.info(f"Deleted {len(ephemeral_paths)} ephemeral nodes")
        
        # Rebuild metadata tree (without ephemeral nodes)
        persistent_nodes = [n for n in nodes if n.node_type == NodeType.PERSISTENT]
        self._metadata_tree.restore_nodes(persistent_nodes)
        logger.info(f"Rebuilt metadata tree with {len(persistent_nodes)} persistent nodes")
        
        # Restore sessions (all marked dead)
        self._session_manager.restore_sessions(sessions)
        logger.info("Restored session manager state")
        
        # Step 6: Clear all watches
        # Clients have disconnected, watches are invalid
        self._watch_manager.clear()
        logger.info("Cleared all watches")
        
        # Step 7: Restore the in-memory operation log from the committed log.
        operations = self._persistence.load_all_operations()
        self._operation_log.restore_operations(operations)
        logger.info(f"Restored {len(operations)} operations into in-memory log")

        # Truncate WAL after successful recovery (checkpoint)
        self._persistence.truncate_wal()
        logger.info("WAL truncated after successful recovery")
        
        # Calculate recovery time
        end_time = datetime.now().timestamp()
        stats["recovery_time_ms"] = int((end_time - start_time) * 1000)
        
        logger.info(f"Recovery complete in {stats['recovery_time_ms']}ms")
        logger.info(f"Recovery stats: {stats}")
        
        return stats
    
    def _load_snapshot(self) -> Optional[dict]:
        """
        Load the last valid snapshot.
        
        Per Section 12, snapshots are the baseline state.
        We use SQLite as our snapshot mechanism - the database
        state represents the last checkpoint.
        
        Returns:
            Snapshot data or None if no snapshot exists
        """
        # Check if database has any data
        try:
            nodes = self._persistence.load_all_nodes()
            sessions = self._persistence.load_all_sessions()
            
            if nodes or sessions:
                return {
                    "nodes": len(nodes),
                    "sessions": len(sessions),
                    "timestamp": datetime.now().timestamp(),
                }
        except Exception as e:
            logger.warning(f"Error loading snapshot: {e}")
        
        return None
    
    def _replay_wal_operations(self, operations: List[Operation]) -> int:
        """
        Replay WAL operations to rebuild state.
        
        Per Section 12, operations are replayed in order to
        reconstruct the state at the time of crash.
        
        This is IDEMPOTENT - replaying the same operations
        multiple times produces the same result.
        
        Args:
            operations: List of operations from WAL
            
        Returns:
            Number of operations successfully replayed
        """
        # Sort by sequence number to ensure correct order
        sorted_ops = sorted(operations, key=lambda op: op.sequence_number)
        
        replayed = 0
        
        for op in sorted_ops:
            # Check if operation is already in database
            # (idempotent - skip if already applied)
            existing = self._persistence.get_operation(op.sequence_number)
            if existing:
                logger.debug(f"Skipping already applied operation: seq={op.sequence_number}")
                continue

            if op.operation_type == OperationType.CREATE:
                self._replay_create(op)
            elif op.operation_type == OperationType.SET:
                self._replay_set(op)
            elif op.operation_type == OperationType.DELETE:
                self._replay_delete(op)
            else:
                raise ValueError(f"Unsupported operation type during recovery: {op.operation_type}")

            # Log the operation to database
            self._persistence.save_operation(op)

            replayed += 1
            logger.debug(f"Replayed operation: seq={op.sequence_number}, type={op.operation_type.value}")
        
        return replayed
    
    def _replay_create(self, op: Operation) -> None:
        """Replay a CREATE operation."""
        # Check if node already exists (idempotent)
        existing = self._persistence.load_node(op.path)
        if existing:
            logger.debug(f"Node already exists during replay: {op.path}")
            return
        
        # Determine node type
        node_type = op.node_type if op.node_type else NodeType.PERSISTENT
        
        # Create node in database
        node = Node(
            path=op.path,
            data=op.data,
            version=1,
            node_type=node_type,
            session_id=op.session_id,
            created_at=op.timestamp,
            modified_at=op.timestamp,
        )
        self._persistence.save_node(node)
    
    def _replay_set(self, op: Operation) -> None:
        """Replay a SET operation."""
        # Load existing node
        node = self._persistence.load_node(op.path)
        if not node:
            raise KeyError(f"Node not found during replay set: {op.path}")
        
        # Update node
        node.data = op.data
        node.version += 1
        node.modified_at = op.timestamp
        self._persistence.save_node(node)
    
    def _replay_delete(self, op: Operation) -> None:
        """Replay a DELETE operation."""
        paths = self._extract_delete_paths(op)
        for path in paths:
            deleted = self._persistence.delete_node(path)
            if not deleted:
                logger.debug(f"Node already deleted during replay: {path}")

    def _extract_delete_paths(self, op: Operation) -> List[str]:
        """Decode all paths affected by a delete operation."""
        if op.data:
            try:
                payload = json.loads(op.data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                raise ValueError(f"Invalid delete payload for seq={op.sequence_number}") from e

            if not isinstance(payload, list):
                raise ValueError(f"Delete payload must be a list for seq={op.sequence_number}")

            paths = [str(path).strip() for path in payload if str(path).strip()]
            if paths:
                return paths

        if op.path:
            return [path.strip() for path in op.path.split(",") if path.strip()]

        raise ValueError(f"Delete operation missing target paths for seq={op.sequence_number}")
    
    def verify_consistency(self) -> Tuple[bool, List[str]]:
        """
        Verify the consistency of the recovered state.
        
        Checks invariants from Section 13:
        - Tree Consistency: No orphaned nodes, no partial creates
        - Session Invariant: No ephemeral node without live session
        
        Returns:
            (is_consistent, list_of_issues)
        """
        issues = []
        
        # Check tree structure
        nodes = self._metadata_tree.get_all_nodes()
        node_paths = {n.path for n in nodes}
        
        for node in nodes:
            if node.path == "/":
                continue
            
            # Check parent exists (Tree Consistency Invariant)
            parent_path = self._get_parent_path(node.path)
            if parent_path not in node_paths:
                issues.append(f"Orphaned node: {node.path} (parent {parent_path} missing)")
            
            # Check ephemeral nodes have live sessions (Session Invariant)
            if node.node_type == NodeType.EPHEMERAL:
                if node.session_id is None:
                    issues.append(f"Ephemeral node without session: {node.path}")
                else:
                    session = self._session_manager.get_session(node.session_id)
                    if session is None:
                        issues.append(f"Ephemeral node with missing session: {node.path}")
                    elif not session.is_alive:
                        issues.append(f"Ephemeral node with dead session: {node.path}")
        
        is_consistent = len(issues) == 0
        
        if is_consistent:
            logger.info("Consistency check passed (Section 13 invariants verified)")
        else:
            logger.error(f"Consistency check failed: {issues}")
        
        return is_consistent, issues
    
    def _get_parent_path(self, path: str) -> str:
        """Get the parent path of a given path."""
        if path == "/" or not path:
            return ""
        parts = path.rsplit("/", 1)
        return parts[0] if parts[0] else "/"
    
    def repair(self) -> dict:
        """
        Attempt to repair inconsistencies.
        
        This is a best-effort repair that:
        - Deletes orphaned nodes
        - Deletes ephemeral nodes with dead sessions
        
        Returns:
            Repair statistics
        """
        logger.warning("Starting repair procedure...")
        
        stats = {
            "orphans_deleted": 0,
            "dead_ephemerals_deleted": 0,
        }
        
        nodes = self._metadata_tree.get_all_nodes()
        node_paths = {n.path for n in nodes}
        
        paths_to_delete = []
        
        for node in nodes:
            if node.path == "/":
                continue
            
            # Check for orphans
            parent_path = self._get_parent_path(node.path)
            if parent_path not in node_paths:
                paths_to_delete.append(node.path)
                stats["orphans_deleted"] += 1
                continue
            
            # Check ephemeral nodes
            if node.node_type == NodeType.EPHEMERAL:
                if node.session_id is None:
                    paths_to_delete.append(node.path)
                    stats["dead_ephemerals_deleted"] += 1
                else:
                    session = self._session_manager.get_session(node.session_id)
                    if session is None or not session.is_alive:
                        paths_to_delete.append(node.path)
                        stats["dead_ephemerals_deleted"] += 1
        
        # Delete problematic nodes
        for path in paths_to_delete:
            try:
                self._metadata_tree.delete(path, recursive=True)
                self._persistence.delete_node(path)
            except Exception as e:
                logger.error(f"Failed to delete {path}: {e}")
        
        logger.warning(f"Repair complete: {stats}")
        
        return stats
    
    def create_snapshot(self) -> str:
        """
        Create a snapshot of the current state.
        
        Per Section 12, snapshots provide a baseline for recovery.
        After creating a snapshot, the WAL can be truncated.
        
        Returns:
            Snapshot identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        snapshot_id = f"snapshot_{timestamp}"
        
        # Force a checkpoint - ensure all data is written to database
        self._persistence.checkpoint()
        
        # Truncate WAL - snapshot is the new baseline
        self._persistence.truncate_wal()
        
        logger.info(f"Snapshot created: {snapshot_id}")
        
        return snapshot_id
