"""
Operation Log for Linearizability.

Provides global sequence numbering for all operations to ensure
total ordering and linearizable semantics.

Every operation is assigned a unique, monotonically increasing sequence number.
This guarantees that all clients see operations in the same order.
"""

import threading
import time
from typing import List, Optional, Callable
from dataclasses import dataclass
from datetime import datetime

from models import Operation, OperationType, NodeType
from logger import get_logger

logger = get_logger("operation_log")


class OperationLog:
    """
    Manages the global operation sequence for linearizability.
    
    Guarantees:
    - Every operation gets a unique sequence number
    - Sequence numbers are strictly monotonically increasing
    - Operations are serialized through a single pipeline
    - Thread-safe for concurrent access
    """
    
    def __init__(self):
        """Initialize the operation log."""
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._sequence_number = 0
        self._operations: List[Operation] = []
        self._commit_callbacks: List[Callable[[Operation], None]] = []
        logger.info("OperationLog initialized")
    
    @property
    def current_sequence(self) -> int:
        """Get the current sequence number (thread-safe)."""
        with self._lock:
            return self._sequence_number
    
    def next_sequence(self) -> int:
        """
        Atomically increment and return the next sequence number.
        
        This is the core of linearizability - each operation gets a
        unique position in the total order.
        """
        with self._lock:
            self._sequence_number += 1
            return self._sequence_number
    
    def append(
        self,
        operation_type: OperationType,
        path: str = "",
        data: bytes = b"",
        session_id: Optional[str] = None,
        node_type: Optional[NodeType] = None,
    ) -> Operation:
        """
        Append a new operation to the log.
        
        Args:
            operation_type: Type of operation
            path: Path for the operation
            data: Data payload
            session_id: Associated session ID
            node_type: Node type for CREATE operations
            
        Returns:
            The created Operation with assigned sequence number
        """
        with self._lock:
            self._sequence_number += 1
            operation = Operation(
                sequence_number=self._sequence_number,
                operation_type=operation_type,
                path=path,
                data=data,
                session_id=session_id,
                timestamp=datetime.now().timestamp(),
                node_type=node_type,
            )
            self._operations.append(operation)
            logger.debug(f"Appended operation: seq={operation.sequence_number}, type={operation_type.value}, path={path}")
            
            return operation

    def discard_last_operation(self, sequence_number: int) -> bool:
        """
        Remove the most recent in-memory operation if it was not committed.

        This is used by the coordinator to roll back provisional log entries
        when persistence fails before an operation becomes durable.
        """
        with self._lock:
            if self._operations and self._operations[-1].sequence_number == sequence_number:
                self._operations.pop()
                if self._sequence_number == sequence_number:
                    self._sequence_number -= 1
                logger.debug(f"Discarded provisional operation: seq={sequence_number}")
                return True

            if self._sequence_number == sequence_number:
                self._sequence_number -= 1
                logger.debug(f"Released provisional sequence without committed op: seq={sequence_number}")
                return True

            return False
    
    def get_operations_since(self, sequence_number: int) -> List[Operation]:
        """
        Get all operations after a given sequence number.
        
        Useful for clients that need to catch up after a disconnect.
        """
        with self._lock:
            return [op for op in self._operations if op.sequence_number > sequence_number]
    
    def get_operation(self, sequence_number: int) -> Optional[Operation]:
        """Get a specific operation by sequence number."""
        with self._lock:
            for op in self._operations:
                if op.sequence_number == sequence_number:
                    return op
            return None
    
    def get_all_operations(self) -> List[Operation]:
        """Get all operations in order."""
        with self._lock:
            return list(self._operations)

    def commit(self, operation: Operation) -> None:
        """
        Mark an operation as committed and notify listeners.

        Operations are appended provisionally before durable persistence.
        This method is the point where downstream observers are told that
        an operation is now safe to observe as committed.
        """
        with self._condition:
            callbacks = list(self._commit_callbacks)
            self._condition.notify_all()

        for callback in callbacks:
            try:
                callback(operation)
            except Exception as e:
                logger.error(f"Commit callback error: {e}")

    def wait_for_operations_since(
        self,
        sequence_number: int,
        timeout_seconds: float,
    ) -> List[Operation]:
        """Wait until at least one committed operation exists after a sequence."""
        deadline = time.monotonic() + timeout_seconds
        with self._condition:
            while True:
                operations = [
                    op for op in self._operations
                    if op.sequence_number > sequence_number
                ]
                if operations:
                    return list(operations)

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return []

                self._condition.wait(timeout=remaining)
    
    def add_commit_callback(self, callback: Callable[[Operation], None]) -> None:
        """
        Add a callback to be notified when operations are committed.
        
        Used by the watch manager to detect state changes.
        """
        with self._lock:
            self._commit_callbacks.append(callback)
    
    def remove_commit_callback(self, callback: Callable[[Operation], None]) -> None:
        """Remove a commit callback."""
        with self._lock:
            if callback in self._commit_callbacks:
                self._commit_callbacks.remove(callback)
    
    def clear(self) -> None:
        """Clear all operations (used for testing and recovery)."""
        with self._lock:
            self._operations.clear()
            logger.info("OperationLog cleared")
    
    def restore_sequence(self, sequence_number: int) -> None:
        """
        Restore the sequence number during recovery.
        
        Called during crash recovery to continue from the last
        committed sequence number.
        """
        with self._lock:
            self._sequence_number = sequence_number
            logger.info(f"Sequence number restored to {sequence_number}")
    
    def restore_operations(self, operations: List[Operation]) -> None:
        """
        Restore operations during recovery.
        
        Called during crash recovery to rebuild the in-memory log.
        """
        with self._lock:
            self._operations = list(operations)
            if operations:
                self._sequence_number = max(op.sequence_number for op in operations)
            logger.info(f"Restored {len(operations)} operations, sequence at {self._sequence_number}")
    
    def __len__(self) -> int:
        """Return the number of operations in the log."""
        with self._lock:
            return len(self._operations)
