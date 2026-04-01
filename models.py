"""
Data models for the Coordination Service.
Defines Node, Session, Watch, Event, and Operation data structures.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Set, Any
from datetime import datetime
import uuid


class NodeType(Enum):
    """Type of node in the metadata tree."""
    PERSISTENT = "PERSISTENT"
    EPHEMERAL = "EPHEMERAL"


class EventType(Enum):
    """Types of events that can trigger watches."""
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CHILDREN = "CHILDREN"


class OperationType(Enum):
    """Types of operations in the operation log."""
    CREATE = "CREATE"
    SET = "SET"
    DELETE = "DELETE"
    SESSION_OPEN = "SESSION_OPEN"
    SESSION_CLOSE = "SESSION_CLOSE"
    SESSION_HEARTBEAT = "SESSION_HEARTBEAT"


@dataclass
class Node:
    """
    A node in the hierarchical metadata tree.
    
    Nodes can be either persistent (survive client disconnects) or
    ephemeral (deleted when owning session expires).
    
    Invariant: version is monotonically increasing on each update.
    """
    path: str
    data: bytes
    version: int
    node_type: NodeType
    session_id: Optional[str] = None
    created_at: float = field(default_factory=lambda: datetime.now().timestamp())
    modified_at: float = field(default_factory=lambda: datetime.now().timestamp())
    
    def __post_init__(self):
        """Validate node invariants."""
        if self.node_type == NodeType.EPHEMERAL and self.session_id is None:
            raise ValueError("Ephemeral nodes must have a session_id")
        if not self.path.startswith("/"):
            raise ValueError("Path must start with /")
    
    def to_dict(self) -> dict:
        """Convert node to dictionary for API responses."""
        return {
            "path": self.path,
            "data": self.data.decode("utf-8") if isinstance(self.data, bytes) else self.data,
            "version": self.version,
            "node_type": self.node_type.value,
            "session_id": self.session_id,
            "created_at": self.created_at,
            "modified_at": self.modified_at,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Node":
        """Create a Node from a dictionary."""
        node_data = data.get("data", b"")
        if isinstance(node_data, str):
            node_data = node_data.encode("utf-8")
        return cls(
            path=data["path"],
            data=node_data,
            version=data["version"],
            node_type=NodeType(data["node_type"]),
            session_id=data.get("session_id"),
            created_at=data.get("created_at", datetime.now().timestamp()),
            modified_at=data.get("modified_at", datetime.now().timestamp()),
        )


@dataclass
class Session:
    """
    A client session with the coordination service.
    
    Sessions track:
    - Heartbeat status for liveness detection
    - Ephemeral nodes owned by this session
    - Watches registered by this session
    
    Invariant: When a session dies, all its ephemeral nodes are deleted atomically.
    """
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: float = field(default_factory=lambda: datetime.now().timestamp())
    last_heartbeat: float = field(default_factory=lambda: datetime.now().timestamp())
    timeout_seconds: int = 30
    ephemeral_nodes: Set[str] = field(default_factory=set)
    is_alive: bool = True
    
    def is_expired(self, current_time: Optional[float] = None) -> bool:
        """Check if the session has expired based on heartbeat timeout."""
        if not self.is_alive:
            return True
        if current_time is None:
            current_time = datetime.now().timestamp()
        return (current_time - self.last_heartbeat) > self.timeout_seconds
    
    def heartbeat(self) -> None:
        """Update the last heartbeat timestamp."""
        self.last_heartbeat = datetime.now().timestamp()
    
    def to_dict(self) -> dict:
        """Convert session to dictionary for API responses."""
        return {
            "session_id": self.session_id,
            "created_at": self.created_at,
            "last_heartbeat": self.last_heartbeat,
            "timeout_seconds": self.timeout_seconds,
            "ephemeral_nodes": list(self.ephemeral_nodes),
            "is_alive": self.is_alive,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Session":
        """Create a Session from a dictionary."""
        session = cls(
            session_id=data["session_id"],
            created_at=data.get("created_at", datetime.now().timestamp()),
            last_heartbeat=data.get("last_heartbeat", datetime.now().timestamp()),
            timeout_seconds=data.get("timeout_seconds", 30),
            is_alive=data.get("is_alive", True),
        )
        session.ephemeral_nodes = set(data.get("ephemeral_nodes", []))
        return session


@dataclass
class Watch:
    """
    A one-shot notification registered on a node or path.
    
    Watch semantics:
    - Fires exactly once
    - Corresponds to a specific state transition
    - Delivered in deterministic order (by sequence number)
    - Must be re-registered after firing (one-shot)
    
    Invariant: is_fired transitions from False to True exactly once.
    """
    watch_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    path: str = ""
    session_id: str = ""
    event_types: Set[EventType] = field(default_factory=lambda: {EventType.CREATE, EventType.UPDATE, EventType.DELETE, EventType.CHILDREN})
    is_fired: bool = False
    created_at: float = field(default_factory=lambda: datetime.now().timestamp())
    
    def should_fire(self, event_type: EventType) -> bool:
        """Check if this watch should fire for the given event type."""
        return not self.is_fired and event_type in self.event_types
    
    def fire(self) -> None:
        """Mark this watch as fired (one-shot semantics)."""
        if self.is_fired:
            raise RuntimeError("Watch already fired (exactly-once violation)")
        self.is_fired = True
    
    def to_dict(self) -> dict:
        """Convert watch to dictionary for API responses."""
        return {
            "watch_id": self.watch_id,
            "path": self.path,
            "session_id": self.session_id,
            "event_types": [et.value for et in self.event_types],
            "is_fired": self.is_fired,
            "created_at": self.created_at,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Watch":
        """Create a Watch from a dictionary."""
        return cls(
            watch_id=data["watch_id"],
            path=data["path"],
            session_id=data["session_id"],
            event_types={EventType(et) for et in data.get("event_types", ["CREATE", "UPDATE", "DELETE", "CHILDREN"])},
            is_fired=data.get("is_fired", False),
            created_at=data.get("created_at", datetime.now().timestamp()),
        )


@dataclass
class Event:
    """
    A notification event delivered to a client when a watch fires.
    
    Events are ordered by sequence_number for deterministic delivery.
    """
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    watch_id: str = ""
    path: str = ""
    event_type: EventType = EventType.UPDATE
    data: bytes = b""
    sequence_number: int = 0
    timestamp: float = field(default_factory=lambda: datetime.now().timestamp())
    
    def to_dict(self) -> dict:
        """Convert event to dictionary for API responses."""
        return {
            "event_id": self.event_id,
            "watch_id": self.watch_id,
            "path": self.path,
            "event_type": self.event_type.value,
            "data": self.data.decode("utf-8") if isinstance(self.data, bytes) else self.data,
            "sequence": self.sequence_number,
            "timestamp": self.timestamp,
        }


@dataclass
class Operation:
    """
    An operation in the operation log for linearizability.
    
    Every metadata mutation is logged with a unique sequence number
    to ensure total ordering of operations.
    """
    sequence_number: int
    operation_type: OperationType
    path: str = ""
    data: bytes = b""
    session_id: Optional[str] = None
    timestamp: float = field(default_factory=lambda: datetime.now().timestamp())
    node_type: Optional[NodeType] = None
    
    def to_dict(self) -> dict:
        """Convert operation to dictionary for persistence."""
        return {
            "sequence_number": self.sequence_number,
            "operation_type": self.operation_type.value,
            "path": self.path,
            "data": self.data.decode("utf-8") if isinstance(self.data, bytes) else self.data,
            "session_id": self.session_id,
            "timestamp": self.timestamp,
            "node_type": self.node_type.value if self.node_type else None,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Operation":
        """Create an Operation from a dictionary."""
        op_data = data.get("data", b"")
        if isinstance(op_data, str):
            op_data = op_data.encode("utf-8")
        return cls(
            sequence_number=data["sequence_number"],
            operation_type=OperationType(data["operation_type"]),
            path=data.get("path", ""),
            data=op_data,
            session_id=data.get("session_id"),
            timestamp=data.get("timestamp", datetime.now().timestamp()),
            node_type=NodeType(data["node_type"]) if data.get("node_type") else None,
        )
