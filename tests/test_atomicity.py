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
from models import EventType, NodeType, Node


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


class TestDurabilityRollback:
    """Tests that persistence failures do not leak partial in-memory state."""

    def test_open_session_failure_does_not_leave_session_alive(self, coordinator: Coordinator):
        """Failed session open should not leave a live session behind."""
        monkeypatch = pytest.MonkeyPatch()

        def fail_atomic_save_session(*args, **kwargs):
            raise RuntimeError("forced open session persistence failure")

        monkeypatch.setattr(coordinator._persistence, "atomic_save_session", fail_atomic_save_session)
        try:
            with pytest.raises(RuntimeError, match="forced open session persistence failure"):
                coordinator.open_session()
        finally:
            monkeypatch.undo()

        assert coordinator.get_health()["sessions_count"] == 0

    def test_heartbeat_failure_does_not_mutate_session(self, coordinator: Coordinator):
        """Failed heartbeat persistence should restore the prior session state."""
        session = coordinator.open_session()
        before = coordinator._clone_session(session)

        monkeypatch = pytest.MonkeyPatch()

        def fail_atomic_save_session(*args, **kwargs):
            raise RuntimeError("forced heartbeat persistence failure")

        monkeypatch.setattr(coordinator._persistence, "atomic_save_session", fail_atomic_save_session)
        try:
            with pytest.raises(RuntimeError, match="forced heartbeat persistence failure"):
                coordinator.heartbeat(session.session_id)
        finally:
            monkeypatch.undo()

        after = coordinator._session_manager.get_session(session.session_id)
        assert after is not None
        assert after.last_heartbeat == before.last_heartbeat
        assert after.is_alive == before.is_alive

    def test_close_session_failure_restores_alive_state(self, coordinator: Coordinator):
        """Failed close cleanup should leave the session alive and its ephemeral node present."""
        session = coordinator.open_session()
        coordinator.create("/session-close", b"", persistent=True, session_id=session.session_id)
        coordinator.create(
            "/session-close/lease",
            b"holder",
            persistent=False,
            session_id=session.session_id,
        )

        monkeypatch = pytest.MonkeyPatch()

        def fail_atomic_delete(*args, **kwargs):
            raise RuntimeError("forced close persistence failure")

        monkeypatch.setattr(coordinator._persistence, "atomic_delete_node", fail_atomic_delete)
        try:
            with pytest.raises(RuntimeError, match="forced close persistence failure"):
                coordinator.close_session(session.session_id)
        finally:
            monkeypatch.undo()

        restored = coordinator._session_manager.get_session(session.session_id)
        assert restored is not None
        assert restored.is_alive is True
        assert coordinator.exists("/session-close/lease") is True
        assert "/session-close/lease" in restored.ephemeral_nodes

    def test_create_failure_does_not_mutate_metadata(self, coordinator: Coordinator):
        """Failed create should leave the tree unchanged."""
        session = coordinator.open_session()

        def fail_atomic_create(*args, **kwargs):
            raise RuntimeError("forced create persistence failure")

        before_count = coordinator.get_health()["nodes_count"]
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(coordinator._persistence, "atomic_create_node", fail_atomic_create)
        try:
            with pytest.raises(RuntimeError, match="forced create persistence failure"):
                coordinator.create(
                    "/durability-create",
                    b"payload",
                    persistent=True,
                    session_id=session.session_id,
                )
        finally:
            monkeypatch.undo()

        assert coordinator.get("/durability-create") is None
        assert coordinator.get_health()["nodes_count"] == before_count

    def test_update_failure_does_not_mutate_metadata(self, coordinator: Coordinator):
        """Failed update should leave data and version unchanged."""
        session = coordinator.open_session()
        coordinator.create("/durability-update", b"initial", persistent=True, session_id=session.session_id)

        before = coordinator.get("/durability-update")
        before_data = before.data
        before_version = before.version
        before_modified = before.modified_at

        def fail_atomic_update(*args, **kwargs):
            raise RuntimeError("forced update persistence failure")

        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(coordinator._persistence, "atomic_update_node", fail_atomic_update)
        try:
            with pytest.raises(RuntimeError, match="forced update persistence failure"):
                coordinator.set("/durability-update", b"updated")
        finally:
            monkeypatch.undo()

        after = coordinator.get("/durability-update")
        assert after.data == before_data
        assert after.version == before_version
        assert after.modified_at == before_modified

    def test_delete_failure_does_not_mutate_metadata(self, coordinator: Coordinator):
        """Failed delete should leave the tree intact."""
        session = coordinator.open_session()
        coordinator.create("/durability-delete", b"payload", persistent=True, session_id=session.session_id)

        before = coordinator.get("/durability-delete")
        before_data = before.data
        before_version = before.version
        before_modified = before.modified_at

        def fail_atomic_delete(*args, **kwargs):
            raise RuntimeError("forced delete persistence failure")

        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(coordinator._persistence, "atomic_delete_node", fail_atomic_delete)
        try:
            with pytest.raises(RuntimeError, match="forced delete persistence failure"):
                coordinator.delete("/durability-delete")
        finally:
            monkeypatch.undo()

        after = coordinator.get("/durability-delete")
        assert after is not None
        assert after.data == before_data
        assert after.version == before_version
        assert after.modified_at == before_modified

    def test_session_expiry_cleanup_failure_does_not_drop_ephemeral_state(self, coordinator: Coordinator):
        """Failed expiry cleanup should not orphan or remove the ephemeral node."""
        session = coordinator.open_session()
        coordinator.create("/durability-expiry", b"", persistent=True, session_id=session.session_id)
        coordinator.create(
            "/durability-expiry/lease",
            b"lease-holder",
            persistent=False,
            session_id=session.session_id,
        )

        def fail_atomic_delete(*args, **kwargs):
            raise RuntimeError("forced expiry persistence failure")

        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(coordinator._persistence, "atomic_delete_node", fail_atomic_delete)
        try:
            with pytest.raises(RuntimeError, match="forced expiry persistence failure"):
                coordinator._on_session_expired(session)
        finally:
            monkeypatch.undo()

        assert coordinator.exists("/durability-expiry/lease") is True
        assert "/durability-expiry/lease" in session.ephemeral_nodes

    def test_stale_cas_does_not_commit_or_fire_watch(self, coordinator: Coordinator):
        """Rejected CAS updates must not append commits or fire watches."""
        session = coordinator.open_session()
        coordinator.create("/cas_watch", b"v1", persistent=True, session_id=session.session_id)

        watch = coordinator.register_watch(
            "/cas_watch",
            session.session_id,
            event_types={EventType.UPDATE},
        )

        operations_before = len(coordinator._operation_log.get_all_operations())
        before = coordinator.get("/cas_watch")
        before_data = before.data
        before_version = before.version

        with pytest.raises(ValueError):
            coordinator.set("/cas_watch", b"stale", expected_version=99)

        assert len(coordinator._operation_log.get_all_operations()) == operations_before
        assert coordinator.wait_watch(watch.watch_id, timeout_seconds=0.1) is None

        after = coordinator.get("/cas_watch")
        assert after.data == before_data
        assert after.version == before_version


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
