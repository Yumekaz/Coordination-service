"""
Tests for the Metadata Tree.

Tests hierarchical namespace operations:
- Create, get, set, delete operations
- Hierarchical relationships
- Version monotonicity
- Thread safety
"""

import pytest
import threading
from models import Node, NodeType
from metadata_tree import MetadataTree


class TestMetadataTreeBasic:
    """Basic metadata tree operations."""
    
    def test_root_exists(self, metadata_tree: MetadataTree):
        """Root node should exist by default."""
        root = metadata_tree.get("/")
        assert root is not None
        assert root.path == "/"
        assert root.node_type == NodeType.PERSISTENT
    
    def test_create_node(self, metadata_tree: MetadataTree):
        """Should create a node successfully."""
        node = metadata_tree.create("/config", b"data", NodeType.PERSISTENT)
        
        assert node.path == "/config"
        assert node.data == b"data"
        assert node.version == 1
        assert node.node_type == NodeType.PERSISTENT
    
    def test_create_nested_node(self, metadata_tree: MetadataTree):
        """Should create nested nodes with proper hierarchy."""
        metadata_tree.create("/config", b"", NodeType.PERSISTENT)
        metadata_tree.create("/config/db", b"", NodeType.PERSISTENT)
        node = metadata_tree.create("/config/db/host", b"localhost", NodeType.PERSISTENT)
        
        assert node.path == "/config/db/host"
        assert node.data == b"localhost"
    
    def test_create_without_parent_fails(self, metadata_tree: MetadataTree):
        """Should fail to create node without parent."""
        with pytest.raises(ValueError, match="Parent node does not exist"):
            metadata_tree.create("/config/db/host", b"data", NodeType.PERSISTENT)
    
    def test_create_duplicate_fails(self, metadata_tree: MetadataTree):
        """Should fail to create duplicate node."""
        metadata_tree.create("/config", b"data", NodeType.PERSISTENT)
        
        with pytest.raises(KeyError, match="already exists"):
            metadata_tree.create("/config", b"other", NodeType.PERSISTENT)
    
    def test_get_existing_node(self, metadata_tree: MetadataTree):
        """Should get an existing node."""
        metadata_tree.create("/config", b"data", NodeType.PERSISTENT)
        
        node = metadata_tree.get("/config")
        assert node is not None
        assert node.data == b"data"
    
    def test_get_nonexistent_returns_none(self, metadata_tree: MetadataTree):
        """Should return None for nonexistent node."""
        node = metadata_tree.get("/nonexistent")
        assert node is None
    
    def test_set_updates_data(self, metadata_tree: MetadataTree):
        """Should update node data."""
        metadata_tree.create("/config", b"old", NodeType.PERSISTENT)
        
        node = metadata_tree.set("/config", b"new")
        assert node.data == b"new"
    
    def test_set_increments_version(self, metadata_tree: MetadataTree):
        """Version should increment on each update."""
        metadata_tree.create("/config", b"v1", NodeType.PERSISTENT)
        
        node = metadata_tree.set("/config", b"v2")
        assert node.version == 2
        
        node = metadata_tree.set("/config", b"v3")
        assert node.version == 3
    
    def test_set_nonexistent_fails(self, metadata_tree: MetadataTree):
        """Should fail to set nonexistent node."""
        with pytest.raises(KeyError):
            metadata_tree.set("/nonexistent", b"data")

    def test_set_with_expected_version_succeeds(self, metadata_tree: MetadataTree):
        """Version-guarded writes should succeed when the version matches."""
        metadata_tree.create("/config", b"v1", NodeType.PERSISTENT)

        node = metadata_tree.set("/config", b"v2", expected_version=1)

        assert node.data == b"v2"
        assert node.version == 2

    def test_set_with_expected_version_mismatch_fails(self, metadata_tree: MetadataTree):
        """Version-guarded writes should reject stale writes."""
        metadata_tree.create("/config", b"v1", NodeType.PERSISTENT)

        with pytest.raises(ValueError, match="version"):
            metadata_tree.set("/config", b"stale", expected_version=99)


class TestMetadataTreeDelete:
    """Delete operation tests."""
    
    def test_delete_leaf_node(self, metadata_tree: MetadataTree):
        """Should delete a leaf node."""
        metadata_tree.create("/config", b"data", NodeType.PERSISTENT)
        
        deleted = metadata_tree.delete("/config")
        assert "/config" in deleted
        assert metadata_tree.get("/config") is None
    
    def test_delete_with_children_recursive(self, metadata_tree: MetadataTree):
        """Should delete node and all children recursively."""
        metadata_tree.create("/config", b"", NodeType.PERSISTENT)
        metadata_tree.create("/config/db", b"", NodeType.PERSISTENT)
        metadata_tree.create("/config/db/host", b"", NodeType.PERSISTENT)
        
        deleted = metadata_tree.delete("/config", recursive=True)
        
        assert len(deleted) == 3
        assert metadata_tree.get("/config") is None
        assert metadata_tree.get("/config/db") is None
        assert metadata_tree.get("/config/db/host") is None
    
    def test_delete_root_fails(self, metadata_tree: MetadataTree):
        """Should not allow deleting root."""
        with pytest.raises(ValueError, match="Cannot delete root"):
            metadata_tree.delete("/")
    
    def test_delete_nonexistent_fails(self, metadata_tree: MetadataTree):
        """Should fail to delete nonexistent node."""
        with pytest.raises(KeyError):
            metadata_tree.delete("/nonexistent")


class TestMetadataTreeChildren:
    """Children listing tests."""
    
    def test_list_children_empty(self, metadata_tree: MetadataTree):
        """Should return empty list for leaf node."""
        metadata_tree.create("/config", b"", NodeType.PERSISTENT)
        
        children = metadata_tree.list_children("/config")
        assert children == []
    
    def test_list_children(self, metadata_tree: MetadataTree):
        """Should list direct children only."""
        metadata_tree.create("/config", b"", NodeType.PERSISTENT)
        metadata_tree.create("/config/db", b"", NodeType.PERSISTENT)
        metadata_tree.create("/config/cache", b"", NodeType.PERSISTENT)
        metadata_tree.create("/config/db/host", b"", NodeType.PERSISTENT)
        
        children = metadata_tree.list_children("/config")
        assert sorted(children) == ["cache", "db"]
    
    def test_list_children_of_root(self, metadata_tree: MetadataTree):
        """Should list children of root."""
        metadata_tree.create("/config", b"", NodeType.PERSISTENT)
        metadata_tree.create("/services", b"", NodeType.PERSISTENT)
        
        children = metadata_tree.list_children("/")
        assert sorted(children) == ["config", "services"]
    
    def test_list_children_nonexistent_fails(self, metadata_tree: MetadataTree):
        """Should fail for nonexistent parent."""
        with pytest.raises(KeyError):
            metadata_tree.list_children("/nonexistent")


class TestMetadataTreeEphemeral:
    """Ephemeral node tests."""
    
    def test_create_ephemeral_with_session(self, metadata_tree: MetadataTree):
        """Should create ephemeral node with session."""
        node = metadata_tree.create(
            "/lock",
            b"data",
            NodeType.EPHEMERAL,
            session_id="session-1"
        )
        
        assert node.node_type == NodeType.EPHEMERAL
        assert node.session_id == "session-1"
    
    def test_create_ephemeral_without_session_fails(self, metadata_tree: MetadataTree):
        """Should fail to create ephemeral without session."""
        with pytest.raises(ValueError, match="session_id"):
            metadata_tree.create("/lock", b"data", NodeType.EPHEMERAL)
    
    def test_get_nodes_by_session(self, metadata_tree: MetadataTree):
        """Should get all nodes for a session."""
        metadata_tree.create("/lock1", b"", NodeType.EPHEMERAL, session_id="s1")
        metadata_tree.create("/lock2", b"", NodeType.EPHEMERAL, session_id="s1")
        metadata_tree.create("/lock3", b"", NodeType.EPHEMERAL, session_id="s2")
        
        nodes = metadata_tree.get_nodes_by_session("s1")
        paths = [n.path for n in nodes]
        
        assert sorted(paths) == ["/lock1", "/lock2"]
    
    def test_delete_session_nodes(self, metadata_tree: MetadataTree):
        """Should delete all nodes for a session."""
        metadata_tree.create("/lock1", b"", NodeType.EPHEMERAL, session_id="s1")
        metadata_tree.create("/lock2", b"", NodeType.EPHEMERAL, session_id="s1")
        metadata_tree.create("/other", b"", NodeType.PERSISTENT)
        
        deleted = metadata_tree.delete_session_nodes("s1")
        
        assert sorted(deleted) == ["/lock1", "/lock2"]
        assert metadata_tree.get("/lock1") is None
        assert metadata_tree.get("/lock2") is None
        assert metadata_tree.get("/other") is not None


class TestMetadataTreeThreadSafety:
    """Thread safety tests."""
    
    def test_concurrent_creates(self, metadata_tree: MetadataTree):
        """Should handle concurrent creates safely."""
        errors = []
        
        def create_node(i):
            try:
                metadata_tree.create(f"/node{i}", f"data{i}".encode(), NodeType.PERSISTENT)
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=create_node, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert metadata_tree.get_node_count() == 11  # 10 + root
    
    def test_concurrent_reads_writes(self, metadata_tree: MetadataTree):
        """Should handle concurrent reads and writes."""
        metadata_tree.create("/counter", b"0", NodeType.PERSISTENT)
        
        read_results = []
        
        def increment():
            for _ in range(10):
                node = metadata_tree.get("/counter")
                value = int(node.data.decode())
                metadata_tree.set("/counter", str(value + 1).encode())
        
        def read():
            for _ in range(10):
                node = metadata_tree.get("/counter")
                read_results.append(int(node.data.decode()))
        
        threads = []
        for _ in range(5):
            threads.append(threading.Thread(target=increment))
            threads.append(threading.Thread(target=read))
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Final value should be 50 (5 threads * 10 increments)
        final = metadata_tree.get("/counter")
        assert int(final.data.decode()) == 50


class TestMetadataTreeVersionMonotonicity:
    """Version monotonicity tests."""
    
    def test_version_always_increases(self, metadata_tree: MetadataTree):
        """Version should always increase."""
        metadata_tree.create("/node", b"v1", NodeType.PERSISTENT)
        
        versions = [1]
        for i in range(2, 101):
            node = metadata_tree.set("/node", f"v{i}".encode())
            versions.append(node.version)
        
        # Check monotonicity
        for i in range(1, len(versions)):
            assert versions[i] > versions[i-1]
    
    def test_version_persists_through_reads(self, metadata_tree: MetadataTree):
        """Version should remain stable between writes."""
        metadata_tree.create("/node", b"data", NodeType.PERSISTENT)
        node = metadata_tree.set("/node", b"updated")
        expected_version = node.version
        
        # Multiple reads shouldn't change version
        for _ in range(10):
            node = metadata_tree.get("/node")
            assert node.version == expected_version
