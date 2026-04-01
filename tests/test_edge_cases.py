"""
Tests for Edge Cases and Error Handling.

Covers uncovered code paths to achieve higher coverage:
- Error handling in models
- Edge cases in metadata tree
- Operation log callbacks
- Persistence edge cases
- Recovery repair functionality
"""

import pytest
import tempfile
import os
import time
import threading

from models import Node, Session, Watch, Event, Operation, NodeType, EventType, OperationType
from metadata_tree import MetadataTree
from operation_log import OperationLog
from persistence import Persistence
from recovery import RecoveryManager
from session_manager import SessionManager
from watch_manager import WatchManager
from coordinator import Coordinator


class TestModelEdgeCases:
    """Test edge cases in data models."""
    
    def test_node_repr(self):
        """Test Node string representation."""
        node = Node(
            path="/test",
            data=b"data",
            version=1,
            node_type=NodeType.PERSISTENT,
            created_at=time.time(),
            modified_at=time.time()
        )
        repr_str = repr(node)
        assert "/test" in repr_str
        assert "PERSISTENT" in repr_str
    
    def test_node_to_dict(self):
        """Test Node serialization to dict."""
        node = Node(
            path="/test",
            data=b"data",
            version=1,
            node_type=NodeType.PERSISTENT,
            created_at=1234567890.0,
            modified_at=1234567890.0
        )
        d = node.to_dict()
        assert d["path"] == "/test"
        assert d["version"] == 1
        assert d["node_type"] == "PERSISTENT"
    
    def test_node_from_dict(self):
        """Test Node deserialization from dict."""
        d = {
            "path": "/test",
            "data": "ZGF0YQ==",  # base64 of "data"
            "version": 1,
            "node_type": "EPHEMERAL",
            "session_id": "session-123",
            "created_at": 1234567890.0,
            "modified_at": 1234567890.0
        }
        node = Node.from_dict(d)
        assert node.path == "/test"
        assert node.node_type == NodeType.EPHEMERAL
        assert node.session_id == "session-123"
    
    def test_session_repr(self):
        """Test Session string representation."""
        session = Session(
            session_id="test-session",
            created_at=time.time(),
            last_heartbeat=time.time(),
            timeout_seconds=30
        )
        repr_str = repr(session)
        assert "test-session" in repr_str
    
    def test_session_to_dict(self):
        """Test Session serialization."""
        session = Session(
            session_id="test-session",
            created_at=1234567890.0,
            last_heartbeat=1234567890.0,
            timeout_seconds=30
        )
        session.ephemeral_nodes.add("/node1")
        d = session.to_dict()
        assert d["session_id"] == "test-session"
        assert "/node1" in d["ephemeral_nodes"]
    
    def test_session_from_dict(self):
        """Test Session deserialization."""
        d = {
            "session_id": "test-session",
            "created_at": 1234567890.0,
            "last_heartbeat": 1234567890.0,
            "timeout_seconds": 30,
            "ephemeral_nodes": ["/node1", "/node2"],
            "is_alive": True
        }
        session = Session.from_dict(d)
        assert session.session_id == "test-session"
        assert len(session.ephemeral_nodes) == 2
    
    def test_watch_repr(self):
        """Test Watch string representation."""
        watch = Watch(
            watch_id="watch-123",
            path="/test",
            session_id="session-123",
            created_at=time.time()
        )
        repr_str = repr(watch)
        assert "watch-123" in repr_str
    
    def test_event_repr(self):
        """Test Event string representation."""
        event = Event(
            event_id="event-123",
            watch_id="watch-123",
            path="/test",
            event_type=EventType.CREATE,
            sequence_number=1,
            timestamp=time.time()
        )
        repr_str = repr(event)
        assert "event-123" in repr_str
    
    def test_operation_repr(self):
        """Test Operation string representation."""
        op = Operation(
            sequence_number=1,
            operation_type=OperationType.CREATE,
            path="/test",
            timestamp=time.time()
        )
        repr_str = repr(op)
        assert "CREATE" in repr_str


class TestMetadataTreeEdgeCases:
    """Test edge cases in metadata tree."""
    
    def test_normalize_path_edge_cases(self):
        """Test path normalization edge cases."""
        tree = MetadataTree()
        
        # These should be handled gracefully
        assert tree._normalize_path("/") == "/"
        assert tree._normalize_path("//test") == "/test"
        assert tree._normalize_path("/test/") == "/test"
        assert tree._normalize_path("/test//child") == "/test/child"
    
    def test_validate_path_invalid(self):
        """Test path validation with invalid paths."""
        tree = MetadataTree()
        
        with pytest.raises(ValueError):
            tree._validate_path("")
        
        with pytest.raises(ValueError):
            tree._validate_path("no-leading-slash")
    
    def test_get_all_nodes(self):
        """Test getting all nodes."""
        tree = MetadataTree()
        tree.create("/test1", b"data1", NodeType.PERSISTENT)
        tree.create("/test2", b"data2", NodeType.PERSISTENT)
        
        nodes = tree.get_all_nodes()
        paths = [n.path for n in nodes]
        
        assert "/" in paths
        assert "/test1" in paths
        assert "/test2" in paths
    
    def test_get_node_count(self):
        """Test node counting."""
        tree = MetadataTree()
        initial_count = tree.get_node_count()
        
        tree.create("/test1", b"data1", NodeType.PERSISTENT)
        tree.create("/test2", b"data2", NodeType.PERSISTENT)
        
        assert tree.get_node_count() == initial_count + 2


class TestOperationLogEdgeCases:
    """Test edge cases in operation log."""
    
    def test_operation_log_callbacks(self):
        """Test commit callbacks."""
        log = OperationLog()
        
        callback_called = []
        
        def on_commit(op):
            callback_called.append(op)
        
        log.add_commit_callback(on_commit)
        
        op = log.append(
            operation_type=OperationType.CREATE,
            path="/test"
        )
        
        assert len(callback_called) == 1
        assert callback_called[0].path == "/test"
    
    def test_operation_log_get_operations(self):
        """Test retrieving operations."""
        log = OperationLog()
        
        log.append(OperationType.CREATE, "/test1")
        log.append(OperationType.CREATE, "/test2")
        log.append(OperationType.CREATE, "/test3")
        
        ops = log.get_operations_since(sequence_number=1)
        assert len(ops) >= 2
    
    def test_operation_log_len(self):
        """Test operation log length."""
        log = OperationLog()
        initial_len = len(log)
        
        log.append(OperationType.CREATE, "/test1")
        log.append(OperationType.CREATE, "/test2")
        
        assert len(log) == initial_len + 2


class TestPersistenceEdgeCases:
    """Test edge cases in persistence layer."""
    
    def test_get_stats(self):
        """Test getting persistence stats."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            persistence = Persistence(db_path)
            stats = persistence.get_stats()
            
            assert "node_count" in stats
            assert "session_count" in stats
            assert "operation_count" in stats
            
            persistence.close()
        finally:
            os.unlink(db_path)
    
    def test_delete_nonexistent_node(self):
        """Test deleting a node that doesn't exist."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            persistence = Persistence(db_path)
            
            # Should not raise, just do nothing
            persistence.delete_node("/nonexistent")
            
            persistence.close()
        finally:
            os.unlink(db_path)
    
    def test_get_nonexistent_session(self):
        """Test getting a session that doesn't exist."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            persistence = Persistence(db_path)
            
            session = persistence.load_session("nonexistent")
            assert session is None
            
            persistence.close()
        finally:
            os.unlink(db_path)


class TestRecoveryEdgeCases:
    """Test edge cases in recovery."""
    
    def test_verify_consistency_clean(self):
        """Test consistency verification on clean state."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/test", b"data", persistent=True, session_id=session.session_id)
            
            is_consistent, issues = coord.verify_consistency()
            assert is_consistent is True
            assert len(issues) == 0
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_recovery_with_no_data(self):
        """Test recovery with empty database."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            # First coordinator creates empty db
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            coord1.stop()
            
            # Second coordinator recovers from empty
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Should work fine
            session = coord2.open_session()
            coord2.create("/test", b"data", persistent=True, session_id=session.session_id)
            
            coord2.stop()
        finally:
            os.unlink(db_path)


class TestSessionManagerEdgeCases:
    """Test edge cases in session manager."""
    
    def test_get_alive_sessions(self):
        """Test getting list of alive sessions."""
        manager = SessionManager()
        
        s1 = manager.open_session(timeout_seconds=30)
        s2 = manager.open_session(timeout_seconds=30)
        
        alive = manager.get_alive_sessions()
        assert len(alive) == 2
        
        manager.expire_session(s1.session_id)
        
        alive = manager.get_alive_sessions()
        assert len(alive) == 1
    
    def test_get_session_count(self):
        """Test session counting."""
        manager = SessionManager()
        
        assert manager.get_session_count() == 0
        
        manager.open_session()
        manager.open_session()
        
        assert manager.get_session_count() == 2
    
    def test_get_alive_session_count(self):
        """Test alive session counting."""
        manager = SessionManager()
        
        s1 = manager.open_session()
        s2 = manager.open_session()
        
        assert manager.get_alive_session_count() == 2
        
        manager.expire_session(s1.session_id)
        
        assert manager.get_alive_session_count() == 1


class TestWatchManagerEdgeCases:
    """Test edge cases in watch manager."""
    
    def test_get_watch_count(self):
        """Test watch counting."""
        manager = WatchManager()
        
        assert manager.get_watch_count() == 0
        
        manager.register("/test", "session-1")
        manager.register("/test", "session-2")
        
        assert manager.get_watch_count() == 2
    
    def test_trigger_no_watches(self):
        """Test triggering when no watches exist."""
        manager = WatchManager()
        
        # Should not raise
        events = manager.trigger("/test", EventType.CREATE, b"data", 1)
        assert len(events) == 0
    
    def test_clear(self):
        """Test clearing all watches."""
        manager = WatchManager()
        
        manager.register("/test1", "session-1")
        manager.register("/test2", "session-2")
        
        assert manager.get_watch_count() == 2
        
        manager.clear()
        
        assert manager.get_watch_count() == 0


class TestCoordinatorEdgeCases:
    """Test edge cases in coordinator."""
    
    def test_get_health(self):
        """Test health endpoint."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            health = coord.get_health()
            
            assert health["status"] == "healthy"
            assert "nodes_count" in health
            assert "sessions_count" in health
            assert "uptime_seconds" in health
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_get_stats(self):
        """Test stats endpoint."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            stats = coord.get_stats()
            
            assert "nodes" in stats
            assert "sessions_total" in stats
            assert "sessions_alive" in stats
            assert "watches" in stats
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_create_with_string_data(self):
        """Test creating node with string data (auto-converted to bytes)."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            
            # Pass string instead of bytes
            node = coord.create("/test", "string data", persistent=True, session_id=session.session_id)
            
            assert node.data == b"string data"
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_set_with_string_data(self):
        """Test setting node with string data."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/test", b"initial", persistent=True, session_id=session.session_id)
            
            # Pass string instead of bytes
            node = coord.set("/test", "updated string")
            
            assert node.data == b"updated string"
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_delete_nonexistent_node(self):
        """Test deleting a node that doesn't exist."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            with pytest.raises(KeyError):
                coord.delete("/nonexistent")
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_list_children_nonexistent(self):
        """Test listing children of nonexistent node."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            with pytest.raises(KeyError):
                coord.list_children("/nonexistent")
            
            coord.stop()
        finally:
            os.unlink(db_path)
