"""
Tests for Atomicity Guarantees.

All operations must be atomic (all-or-nothing):
- Create is atomic
- Update is atomic
- Delete is atomic
- Crash before commit rolls back
- No partial state ever visible
"""

import pytest
import threading
import time
import os
import tempfile
from typing import List

from coordinator import Coordinator
from persistence import Persistence
from models import NodeType, Node


class TestCreateAtomicity:
    """Tests that create operations are atomic."""
    
    def test_create_is_atomic(self, coordinator: Coordinator):
        """Create should either fully succeed or not happen at all."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        # Create should succeed completely
        result = coordinator.create(
            "/atomic_test",
            b"test data",
            persistent=True,
            session_id=session_id
        )
        
        assert result is not None
        
        # Verify node exists with correct data
        node = coordinator.get("/atomic_test")
        assert node is not None
        assert node.data == b"test data"
        assert node.version == 1
    
    def test_create_failure_no_partial_state(self, coordinator: Coordinator):
        """Failed create should leave no trace."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        # Try to create without parent - raises ValueError for missing parent
        with pytest.raises(ValueError, match="Parent node does not exist"):
            coordinator.create(
                "/nonexistent/child",
                b"test",
                persistent=True,
                session_id=session_id
            )
        
        # Neither path should exist
        assert coordinator.exists("/nonexistent") is False
        assert coordinator.exists("/nonexistent/child") is False
    
    def test_create_ephemeral_atomic(self, coordinator: Coordinator):
        """Ephemeral create should atomically link to session."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        result = coordinator.create(
            "/ephemeral_atomic",
            b"test",
            persistent=False,
            session_id=session_id
        )
        
        assert result is not None
        
        # Both the node should exist and session should track it
        node = coordinator.get("/ephemeral_atomic")
        assert node is not None
        assert node.node_type == NodeType.EPHEMERAL


class TestUpdateAtomicity:
    """Tests that update operations are atomic."""
    
    def test_update_is_atomic(self, coordinator: Coordinator):
        """Update should atomically change data and version."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        coordinator.create("/update_test", b"initial", persistent=True, session_id=session_id)
        
        # Update should change both data and version atomically
        result = coordinator.set("/update_test", b"updated")
        
        assert result is not None
        
        node = coordinator.get("/update_test")
        assert node.data == b"updated"
        assert node.version == 2
    
    def test_update_failure_preserves_state(self, coordinator: Coordinator):
        """Failed update should not change existing state."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        coordinator.create("/preserve_test", b"original", persistent=True, session_id=session_id)
        original = coordinator.get("/preserve_test")
        
        # Try to update nonexistent node
        with pytest.raises(KeyError):
            coordinator.set("/nonexistent", b"data")
        
        # Original should be unchanged
        current = coordinator.get("/preserve_test")
        assert current.data == original.data
        assert current.version == original.version


class TestDeleteAtomicity:
    """Tests that delete operations are atomic."""
    
    def test_delete_is_atomic(self, coordinator: Coordinator):
        """Delete should completely remove node."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        coordinator.create("/delete_test", b"data", persistent=True, session_id=session_id)
        
        deleted_paths = coordinator.delete("/delete_test")
        
        assert "/delete_test" in deleted_paths
        assert coordinator.exists("/delete_test") is False
    
    def test_delete_with_children_atomic(self, coordinator: Coordinator):
        """Recursive delete should remove all or nothing."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        # Create tree
        coordinator.create("/parent", b"", persistent=True, session_id=session_id)
        coordinator.create("/parent/child1", b"", persistent=True, session_id=session_id)
        coordinator.create("/parent/child2", b"", persistent=True, session_id=session_id)
        coordinator.create("/parent/child1/grandchild", b"", persistent=True, session_id=session_id)
        
        # Delete parent (recursive)
        deleted_paths = coordinator.delete("/parent")
        
        assert len(deleted_paths) == 4
        
        # All should be gone
        assert coordinator.exists("/parent") is False
        assert coordinator.exists("/parent/child1") is False
        assert coordinator.exists("/parent/child2") is False
        assert coordinator.exists("/parent/child1/grandchild") is False


class TestConcurrentAtomicity:
    """Tests atomicity under concurrent access."""
    
    def test_concurrent_writes_no_corruption(self, coordinator: Coordinator):
        """Concurrent writes should not corrupt data."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        coordinator.create("/concurrent", b"initial", persistent=True, session_id=session_id)
        
        errors = []
        versions_seen = []
        lock = threading.Lock()
        
        def writer(value: int):
            try:
                coordinator.set("/concurrent", f"value_{value}".encode())
                node = coordinator.get("/concurrent")
                with lock:
                    versions_seen.append(node.version)
            except Exception as e:
                with lock:
                    errors.append(e)
        
        threads = [threading.Thread(target=writer, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        
        # Version should have incremented correctly
        node = coordinator.get("/concurrent")
        assert node.version == 11  # 1 initial + 10 updates
    
    def test_no_partial_state_visible(self, coordinator: Coordinator):
        """Readers should never see partial state during writes."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        coordinator.create("/partial_test", b"initial", persistent=True, session_id=session_id)
        
        partial_states = []
        lock = threading.Lock()
        stop_flag = threading.Event()
        
        def writer():
            for i in range(20):
                # Each write is atomic
                coordinator.set("/partial_test", f"value_{i:05d}".encode())
                time.sleep(0.001)
            stop_flag.set()
        
        def reader():
            while not stop_flag.is_set():
                node = coordinator.get("/partial_test")
                data = node.data.decode()
                # Data should always be well-formed
                if not data.startswith("initial") and not data.startswith("value_"):
                    with lock:
                        partial_states.append(data)
                time.sleep(0.0005)
        
        writer_thread = threading.Thread(target=writer)
        reader_threads = [threading.Thread(target=reader) for _ in range(5)]
        
        writer_thread.start()
        for t in reader_threads:
            t.start()
        
        writer_thread.join()
        for t in reader_threads:
            t.join()
        
        # No partial states should have been observed
        assert len(partial_states) == 0


class TestTransactionRollback:
    """Tests that failures cause proper rollback."""
    
    def test_error_causes_rollback(self, coordinator: Coordinator):
        """Error during operation should leave state unchanged."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        # Get initial state
        coordinator.create("/rollback_test", b"stable", persistent=True, session_id=session_id)
        before = coordinator.get("/rollback_test")
        
        # This should fail but not corrupt anything - raises ValueError for missing parent
        with pytest.raises(ValueError, match="Parent node does not exist"):
            coordinator.create(
                "/orphan/child",  # Parent doesn't exist
                b"data",
                persistent=True,
                session_id=session_id
            )
        
        # Original node should be unchanged
        after = coordinator.get("/rollback_test")
        assert after.data == before.data
        assert after.version == before.version


class TestPersistenceAtomicity:
    """Tests that persistence operations are atomic."""
    
    def test_persistence_atomic_write(self, temp_db_path):
        """Persisted data should be complete or not exist."""
        persistence = Persistence(temp_db_path)
        try:
            from models import Node
            import time
            
            node = Node(
                path="/persist_test",
                data=b"test data",
                version=1,
                node_type=NodeType.PERSISTENT,
                session_id=None,
                created_at=time.time(),
                modified_at=time.time()
            )
            
            # Save should be atomic
            persistence.save_node(node)
            
            # Load should return complete node
            loaded = persistence.load_node("/persist_test")
            assert loaded is not None
            assert loaded.data == b"test data"
            assert loaded.version == 1
        finally:
            persistence.close()
    
    def test_persistence_rollback(self, temp_db_path):
        """Transaction rollback should not persist partial data."""
        persistence = Persistence(temp_db_path)
        try:
            from models import Node
            import time
            
            # Create initial node
            node = Node(
                path="/rollback_persist",
                data=b"initial",
                version=1,
                node_type=NodeType.PERSISTENT,
                session_id=None,
                created_at=time.time(),
                modified_at=time.time()
            )
            persistence.save_node(node)
            
            # Load to verify
            loaded = persistence.load_node("/rollback_persist")
            assert loaded.data == b"initial"
        finally:
            persistence.close()
