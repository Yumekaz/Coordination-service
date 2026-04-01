"""
In-Memory Metadata Tree.

Implements a hierarchical namespace for storing metadata nodes.
Provides thread-safe access with RLock for concurrent clients.

Invariants:
- No orphaned nodes (parent must exist before child)
- No partial creates (atomic operations)
- Version monotonically increases on each write
- Paths are normalized and validated
"""

import threading
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime

from models import Node, NodeType
from errors import VersionConflictError
from logger import get_logger

logger = get_logger("metadata_tree")


class MetadataTree:
    """
    Thread-safe hierarchical metadata tree.
    
    The tree stores nodes addressed by paths (e.g., /config/db/host).
    All operations are atomic and serialized through an RLock.
    
    Guarantees:
    - Tree consistency (no orphaned nodes)
    - Atomic operations (all-or-nothing)
    - Version monotonicity
    - Thread safety
    """
    
    def __init__(self):
        """Initialize the metadata tree with a root node."""
        self._lock = threading.RLock()
        self._nodes: Dict[str, Node] = {}
        
        # Create the root node
        self._nodes["/"] = Node(
            path="/",
            data=b"",
            version=0,
            node_type=NodeType.PERSISTENT,
        )
        logger.info("MetadataTree initialized with root node")

    def _clone_node(self, node: Node) -> Node:
        """Create a detached copy of a node."""
        return Node.from_dict(node.to_dict())
    
    def _normalize_path(self, path: str) -> str:
        """
        Normalize a path to canonical form.
        
        - Must start with /
        - No trailing slash (except root)
        - No double slashes
        """
        if not path:
            return "/"
        
        # Ensure starts with /
        if not path.startswith("/"):
            path = "/" + path
        
        # Remove trailing slash (except for root)
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
        
        # Remove double slashes
        while "//" in path:
            path = path.replace("//", "/")
        
        return path
    
    def _get_parent_path(self, path: str) -> str:
        """Get the parent path of a given path."""
        if path == "/":
            return ""
        normalized = self._normalize_path(path)
        parts = normalized.rsplit("/", 1)
        parent = parts[0] if parts[0] else "/"
        return parent
    
    def _get_node_name(self, path: str) -> str:
        """Get the node name from a path."""
        normalized = self._normalize_path(path)
        if normalized == "/":
            return ""
        return normalized.rsplit("/", 1)[1]
    
    def _validate_path(self, path: str) -> None:
        """Validate that a path is well-formed."""
        if not path:
            raise ValueError("Path cannot be empty")
        if not path.startswith("/"):
            raise ValueError("Path must start with /")
        if path != "/" and path.endswith("/"):
            raise ValueError("Path cannot end with / (except root)")
        
        # Check for invalid characters
        invalid_chars = ["\0", "\n", "\r", "\t"]
        for char in invalid_chars:
            if char in path:
                raise ValueError(f"Path contains invalid character: {repr(char)}")
    
    def create(
        self,
        path: str,
        data: bytes,
        node_type: NodeType,
        session_id: Optional[str] = None,
    ) -> Node:
        """
        Create a new node in the tree.
        
        Args:
            path: The path where the node should be created
            data: The data to store in the node
            node_type: PERSISTENT or EPHEMERAL
            session_id: Required for ephemeral nodes
            
        Returns:
            The created Node
            
        Raises:
            ValueError: If path is invalid or parent doesn't exist
            KeyError: If node already exists
        """
        node = self.prepare_create(path, data, node_type, session_id=session_id)
        return self.commit_create(node)

    def prepare_create(
        self,
        path: str,
        data: bytes,
        node_type: NodeType,
        session_id: Optional[str] = None,
    ) -> Node:
        """Validate a create and return the node that would be inserted."""
        with self._lock:
            path = self._normalize_path(path)
            self._validate_path(path)
            
            # Check if node already exists
            if path in self._nodes:
                raise KeyError(f"Node already exists: {path}")
            
            # Check if parent exists
            parent_path = self._get_parent_path(path)
            if parent_path and parent_path not in self._nodes:
                raise ValueError(f"Parent node does not exist: {parent_path}")
            if parent_path and self._nodes[parent_path].node_type == NodeType.EPHEMERAL:
                raise ValueError(f"Cannot create child under ephemeral node: {parent_path}")
            
            # Validate ephemeral node requirements
            if node_type == NodeType.EPHEMERAL and session_id is None:
                raise ValueError("Ephemeral nodes require a session_id")
            
            # Create the node
            now = datetime.now().timestamp()
            node = Node(
                path=path,
                data=data if isinstance(data, bytes) else data.encode("utf-8"),
                version=1,
                node_type=node_type,
                session_id=session_id,
                created_at=now,
                modified_at=now,
            )
            return self._clone_node(node)

    def commit_create(self, node: Node) -> Node:
        """Insert a previously prepared node into the live tree."""
        with self._lock:
            path = self._normalize_path(node.path)
            if path in self._nodes:
                raise KeyError(f"Node already exists: {path}")

            parent_path = self._get_parent_path(path)
            if parent_path and parent_path not in self._nodes:
                raise ValueError(f"Parent node does not exist: {parent_path}")
            if parent_path and self._nodes[parent_path].node_type == NodeType.EPHEMERAL:
                raise ValueError(f"Cannot create child under ephemeral node: {parent_path}")

            committed = self._clone_node(node)
            self._nodes[path] = committed
            logger.debug(f"Created node: {path} (type={committed.node_type.value})")
            return committed
    
    def get(self, path: str) -> Optional[Node]:
        """
        Get a node by path.
        
        Args:
            path: The path of the node to retrieve
            
        Returns:
            The Node if it exists, None otherwise
        """
        with self._lock:
            path = self._normalize_path(path)
            return self._nodes.get(path)
    
    def set(
        self,
        path: str,
        data: bytes,
        expected_version: Optional[int] = None,
    ) -> Node:
        """
        Update a node's data.

        Args:
            path: The path of the node to update
            data: The new data
            expected_version: Optional compare-and-swap guard
            
        Returns:
            The updated Node
            
        Raises:
            KeyError: If node doesn't exist
        """
        node = self.prepare_set(path, data, expected_version=expected_version)
        return self.commit_set(node)

    def prepare_set(
        self,
        path: str,
        data: bytes,
        expected_version: Optional[int] = None,
    ) -> Node:
        """Validate an update and return the new node state without applying it."""
        with self._lock:
            path = self._normalize_path(path)
            
            if path not in self._nodes:
                raise KeyError(f"Node does not exist: {path}")
            
            if path == "/":
                raise ValueError("Cannot modify root node")

            current = self._nodes[path]
            if expected_version is not None and current.version != expected_version:
                raise VersionConflictError(path, expected_version, current.version)

            updated = self._clone_node(current)
            updated.data = data if isinstance(data, bytes) else data.encode("utf-8")
            updated.version += 1
            updated.modified_at = datetime.now().timestamp()
            return updated

    def commit_set(self, node: Node) -> Node:
        """Apply a previously prepared node update to the live tree."""
        with self._lock:
            path = self._normalize_path(node.path)
            if path not in self._nodes:
                raise KeyError(f"Node does not exist: {path}")
            if path == "/":
                raise ValueError("Cannot modify root node")

            committed = self._clone_node(node)
            self._nodes[path] = committed
            logger.debug(f"Updated node: {path} (version={committed.version})")
            return committed
    
    def delete(self, path: str, recursive: bool = True) -> List[str]:
        """
        Delete a node and optionally its children.
        
        Args:
            path: The path of the node to delete
            recursive: If True, delete all children recursively
            
        Returns:
            List of deleted paths (for watch notifications)
            
        Raises:
            KeyError: If node doesn't exist
            ValueError: If trying to delete root or non-empty node without recursive
        """
        deleted_paths = self.plan_delete(path, recursive=recursive)
        return self.apply_delete_paths(deleted_paths)

    def plan_delete(self, path: str, recursive: bool = True) -> List[str]:
        """Plan a delete without applying it to the live tree."""
        with self._lock:
            path = self._normalize_path(path)

            if path == "/":
                raise ValueError("Cannot delete root node")

            if path not in self._nodes:
                raise KeyError(f"Node does not exist: {path}")

            children = self._get_all_children(path)

            if children and not recursive:
                raise ValueError(f"Node has children: {path}")

            return sorted(children + [path], reverse=True)

    def apply_delete_paths(self, paths: List[str]) -> List[str]:
        """Delete a known set of paths from the live tree."""
        with self._lock:
            deleted_paths = []
            for path in paths:
                if path in self._nodes:
                    del self._nodes[path]
                    deleted_paths.append(path)
                    logger.debug(f"Deleted node: {path}")
            return deleted_paths
    
    def exists(self, path: str) -> bool:
        """Check if a node exists."""
        with self._lock:
            path = self._normalize_path(path)
            return path in self._nodes
    
    def list_children(self, path: str) -> List[str]:
        """
        List the direct children of a node.
        
        Args:
            path: The parent path
            
        Returns:
            List of child node names (not full paths)
            
        Raises:
            KeyError: If parent doesn't exist
        """
        with self._lock:
            path = self._normalize_path(path)
            
            if path not in self._nodes:
                raise KeyError(f"Node does not exist: {path}")
            
            children = []
            prefix = path if path == "/" else path + "/"
            
            for node_path in self._nodes:
                if node_path == path:
                    continue
                if node_path.startswith(prefix):
                    # Check if it's a direct child
                    remaining = node_path[len(prefix):]
                    if "/" not in remaining:
                        children.append(remaining)
            
            return sorted(children)
    
    def _get_all_children(self, path: str) -> List[str]:
        """Get all descendant paths of a node."""
        path = self._normalize_path(path)
        prefix = path if path == "/" else path + "/"
        
        children = []
        for node_path in self._nodes:
            if node_path != path and node_path.startswith(prefix):
                children.append(node_path)
        
        return children
    
    def get_nodes_by_session(self, session_id: str) -> List[Node]:
        """Get all nodes owned by a session (ephemeral nodes)."""
        with self._lock:
            return [
                node for node in self._nodes.values()
                if node.session_id == session_id
            ]
    
    def delete_session_nodes(self, session_id: str) -> List[str]:
        """
        Delete all ephemeral nodes belonging to a session.
        
        Called when a session expires or disconnects.
        Returns the list of deleted paths for watch notifications.
        """
        deleted_paths = self.plan_delete_session_nodes(session_id)
        return self.apply_delete_paths(deleted_paths)

    def plan_delete_session_nodes(self, session_id: str) -> List[str]:
        """Plan all ephemeral node deletions for an expired session."""
        with self._lock:
            paths_to_delete = [
                node.path for node in self._nodes.values()
                if node.session_id == session_id and node.node_type == NodeType.EPHEMERAL
            ]

            deleted_paths = []
            seen_paths = set()
            paths_to_delete.sort(key=lambda p: p.count("/"), reverse=True)

            for path in paths_to_delete:
                if path in self._nodes:
                    for deleted_path in self.plan_delete(path, recursive=True):
                        if deleted_path not in seen_paths:
                            seen_paths.add(deleted_path)
                            deleted_paths.append(deleted_path)

            return deleted_paths
    
    def get_all_nodes(self) -> List[Node]:
        """Get all nodes in the tree."""
        with self._lock:
            return list(self._nodes.values())
    
    def get_node_count(self) -> int:
        """Get the total number of nodes."""
        with self._lock:
            return len(self._nodes)
    
    def clear(self) -> None:
        """Clear all nodes except root (used for testing and recovery)."""
        with self._lock:
            self._nodes.clear()
            self._nodes["/"] = Node(
                path="/",
                data=b"",
                version=0,
                node_type=NodeType.PERSISTENT,
            )
            logger.info("MetadataTree cleared (root preserved)")
    
    def restore_nodes(self, nodes: List[Node]) -> None:
        """
        Restore nodes during recovery.
        
        Clears existing nodes and restores from the provided list.
        """
        with self._lock:
            self._nodes.clear()
            
            # Always ensure root exists
            self._nodes["/"] = Node(
                path="/",
                data=b"",
                version=0,
                node_type=NodeType.PERSISTENT,
            )
            
            for node in nodes:
                self._nodes[node.path] = node
            
            logger.info(f"Restored {len(nodes)} nodes")
    
    def __len__(self) -> int:
        """Return the number of nodes."""
        with self._lock:
            return len(self._nodes)
