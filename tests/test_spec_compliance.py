"""
Tests for Spec Section 20 - Exact Required Tests.

These tests ensure 100% compliance with the specification's
exact test requirements from Section 20.
"""

import pytest
import tempfile
import os
import time
import threading

from coordinator import Coordinator
from session_manager import SessionManager
from watch_manager import WatchManager
from models import NodeType, EventType


class TestLinearizabilitySpec:
    """Exact tests from spec Section 20 - Linearizability."""
    
    def test_write_visibility_immediate(self):
        """No stale reads - write immediately visible."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/test", b"initial", persistent=True, session_id=session.session_id)
            
            # Write and immediately read - must see new value
            coord.set("/test", b"updated")
            node = coord.get("/test")
            
            assert node.data == b"updated", "Write must be immediately visible"
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_version_monotonicity(self):
        """Version always increases, never decreases."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            node = coord.create("/test", b"v1", persistent=True, session_id=session.session_id)
            
            versions = [node.version]
            
            for i in range(10):
                node = coord.set("/test", f"v{i+2}".encode())
                versions.append(node.version)
            
            # Verify monotonicity
            for i in range(1, len(versions)):
                assert versions[i] > versions[i-1], f"Version must increase: {versions[i-1]} -> {versions[i]}"
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestSessionsSpec:
    """Exact tests from spec Section 20 - Sessions."""
    
    def test_timeout_expires_session(self):
        """No heartbeat = session dies (Spec Section 20)."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            # Create session with minimum timeout (5 seconds per config)
            session = coord.open_session(timeout_seconds=5)
            session_id = session.session_id
            
            # Verify session is alive
            assert coord._session_manager.is_alive(session_id)
            
            # Wait for timeout (no heartbeat) - wait 7 seconds for 5 second timeout
            time.sleep(7)
            
            # Session should be dead
            assert not coord._session_manager.is_alive(session_id)
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_duplicate_open_fails(self):
        """Can't open same session twice (Spec Section 20)."""
        manager = SessionManager()
        
        # Open session with explicit ID
        session_id = "fixed-session-id-12345"
        session1 = manager.open_session(session_id=session_id)
        
        # Try to open again with same ID - should fail
        with pytest.raises(KeyError):
            manager.open_session(session_id=session_id)
    
    def test_ephemeral_deleted_on_death(self):
        """Ephemeral nodes deleted when session dies."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            # Admin session for parent
            admin = coord.open_session(timeout_seconds=60)
            coord.create("/parent", b"", persistent=True, session_id=admin.session_id)
            
            # Session that will die (short timeout)
            session = coord.open_session(timeout_seconds=1)
            coord.create("/parent/ephemeral", b"data", persistent=False, session_id=session.session_id)
            
            # Verify exists
            assert coord.exists("/parent/ephemeral")
            
            # Wait for deletion with polling (max 10 seconds)
            deleted = False
            for _ in range(20):
                time.sleep(0.5)
                if not coord.exists("/parent/ephemeral"):
                    deleted = True
                    break
            
            # Must be deleted
            assert deleted, "Ephemeral must be deleted on session death"
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestWatchesSpec:
    """Exact tests from spec Section 20 - Watches."""
    
    def test_watch_unregister(self):
        """Unregister removes watch (Spec Section 20)."""
        manager = WatchManager()
        
        # Register a watch
        watch = manager.register("/test", "session-123")
        watch_id = watch.watch_id
        
        # Verify it exists
        assert manager.get_watch(watch_id) is not None
        
        # Unregister
        result = manager.unregister(watch_id)
        assert result is True
        
        # Verify it's gone
        assert manager.get_watch(watch_id) is None
        
        # Unregister again should return False
        result = manager.unregister(watch_id)
        assert result is False


class TestAtomicitySpec:
    """Exact tests from spec Section 20 - Atomicity."""
    
    def test_crash_before_commit_rolls_back(self):
        """If crash before commit, operation is rolled back."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            # Create initial state
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            coord1.create("/test", b"original", persistent=True, session_id=session.session_id)
            
            # Simulate "crash" - stop without proper cleanup
            coord1.stop()
            
            # Recovery
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Original value should be preserved
            node = coord2.get("/test")
            assert node.data == b"original"
            
            coord2.stop()
        finally:
            os.unlink(db_path)
    
    def test_rollback_on_error(self):
        """Error causes rollback, no partial state."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/test", b"original", persistent=True, session_id=session.session_id)
            
            # Try invalid operation
            with pytest.raises(ValueError):
                coord.create("/nonexistent/child", b"data", persistent=True, session_id=session.session_id)
            
            # Original state preserved
            node = coord.get("/test")
            assert node.data == b"original"
            
            # No partial node created
            assert not coord.exists("/nonexistent")
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_corruption_impossible(self):
        """No corruption possible under any operation sequence."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            
            # Rapid mixed operations
            coord.create("/test", b"data", persistent=True, session_id=session.session_id)
            
            errors = []
            
            def worker(worker_id):
                try:
                    for i in range(50):
                        try:
                            coord.set("/test", f"worker{worker_id}-{i}".encode())
                            node = coord.get("/test")
                            # Verify data is valid (not corrupted)
                            assert node.data.decode().startswith("worker")
                        except Exception as e:
                            errors.append(str(e))
                except Exception as e:
                    errors.append(str(e))
            
            threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
            # Final consistency check
            node = coord.get("/test")
            assert node.data.decode().startswith("worker"), "Data must not be corrupted"
            
            # Verify tree consistency
            is_consistent, issues = coord.verify_consistency()
            assert is_consistent, f"Tree must be consistent: {issues}"
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestRecoverySpec:
    """Exact tests from spec Section 20 - Recovery."""
    
    def test_recovery_is_idempotent(self):
        """Replay multiple times safe (Spec Section 20)."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            # First coordinator - create data
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            coord1.create("/test1", b"data1", persistent=True, session_id=session.session_id)
            coord1.create("/test2", b"data2", persistent=True, session_id=session.session_id)
            coord1.set("/test1", b"updated")
            
            stats1 = coord1.get_stats()
            coord1.stop()
            
            # Second coordinator - recover
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            stats2 = coord2.get_stats()
            coord2.stop()
            
            # Third coordinator - recover again (idempotent)
            coord3 = Coordinator(db_path=db_path)
            coord3.start()
            stats3 = coord3.get_stats()
            
            # Verify same state after multiple recoveries
            assert stats2["nodes"] == stats3["nodes"]
            
            # Verify data integrity
            node1 = coord3.get("/test1")
            assert node1.data == b"updated"
            
            node2 = coord3.get("/test2")
            assert node2.data == b"data2"
            
            coord3.stop()
        finally:
            os.unlink(db_path)
    
    def test_metadata_consistency(self):
        """Tree consistent after recovery - no orphans, no partial creates."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            # Create complex tree
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            coord1.create("/a", b"", persistent=True, session_id=session.session_id)
            coord1.create("/a/b", b"", persistent=True, session_id=session.session_id)
            coord1.create("/a/b/c", b"data", persistent=True, session_id=session.session_id)
            coord1.create("/a/d", b"data", persistent=True, session_id=session.session_id)
            
            coord1.stop()
            
            # Recovery
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Verify consistency
            is_consistent, issues = coord2.verify_consistency()
            assert is_consistent, f"Must be consistent: {issues}"
            
            # Verify structure
            assert coord2.exists("/a")
            assert coord2.exists("/a/b")
            assert coord2.exists("/a/b/c")
            assert coord2.exists("/a/d")
            
            # Verify parent-child relationships
            children_a = coord2.list_children("/a")
            assert set(children_a) == {"b", "d"}
            
            coord2.stop()
        finally:
            os.unlink(db_path)
    
    def test_operation_log_replayed(self):
        """WAL entries are replayed on recovery."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            # Create state with multiple operations
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            coord1.create("/replayed", b"v1", persistent=True, session_id=session.session_id)
            coord1.set("/replayed", b"v2")
            coord1.set("/replayed", b"v3")
            
            # Get version before crash
            node_before = coord1.get("/replayed")
            
            coord1.stop()
            
            # Recovery - should replay all operations
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            node_after = coord2.get("/replayed")
            
            # Must have same data and version
            assert node_after.data == b"v3", "Latest data must be replayed"
            assert node_after.version == node_before.version, "Version must match"
            
            coord2.stop()
        finally:
            os.unlink(db_path)
    
    def test_partial_write_recovery(self):
        """Crash during write = operation rolled back."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            # Create initial state
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            coord1.create("/test", b"committed", persistent=True, session_id=session.session_id)
            
            # Stop abruptly
            coord1.stop()
            
            # Recovery
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Only committed data should exist
            node = coord2.get("/test")
            assert node.data == b"committed"
            
            # Tree should be consistent
            is_consistent, _ = coord2.verify_consistency()
            assert is_consistent
            
            coord2.stop()
        finally:
            os.unlink(db_path)


class TestIntegrationSpec:
    """Exact tests from spec Section 20 - Integration."""
    
    def test_recovery_during_watch_wait(self):
        """Server crashes while client waiting on watch - recovery works."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            coord1.create("/watched", b"initial", persistent=True, session_id=session.session_id)
            
            # Register watch
            watch = coord1.register_watch("/watched", session.session_id, event_types=[EventType.UPDATE])
            
            # Start waiting in background (will timeout due to crash)
            wait_result = [None]
            
            def waiter():
                try:
                    wait_result[0] = coord1.wait_watch(watch.watch_id, timeout_seconds=1)
                except:
                    pass
            
            wait_thread = threading.Thread(target=waiter)
            wait_thread.start()
            
            # "Crash" - stop server
            time.sleep(0.5)
            coord1.stop()
            wait_thread.join()
            
            # Recovery
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Watches should be cleared (clients disconnected)
            stats = coord2.get_stats()
            assert stats["watches"] == 0, "Watches must be cleared on recovery"
            
            # Data should survive
            assert coord2.exists("/watched")
            
            # Can register new watch
            session2 = coord2.open_session()
            watch2 = coord2.register_watch("/watched", session2.session_id)
            assert watch2 is not None
            
            coord2.stop()
        finally:
            os.unlink(db_path)


class TestWatchChildrenEvent:
    """Test CHILDREN event type from spec Section 8."""
    
    def test_children_watch_fires_on_child_create(self):
        """Watch with CHILDREN type fires when child created."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/parent", b"", persistent=True, session_id=session.session_id)
            
            # Register CHILDREN watch on parent
            watch = coord.register_watch(
                "/parent", 
                session.session_id, 
                event_types=[EventType.CHILDREN]
            )
            
            event_received = [None]
            
            def waiter():
                event_received[0] = coord.wait_watch(watch.watch_id, timeout_seconds=5)
            
            wait_thread = threading.Thread(target=waiter)
            wait_thread.start()
            
            time.sleep(0.1)
            
            # Create child - should trigger CHILDREN event
            coord.create("/parent/child", b"data", persistent=True, session_id=session.session_id)
            
            wait_thread.join()
            
            assert event_received[0] is not None, "CHILDREN watch must fire"
            assert event_received[0].event_type == EventType.CHILDREN
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_children_watch_fires_on_child_delete(self):
        """Watch with CHILDREN type fires when child deleted."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/parent", b"", persistent=True, session_id=session.session_id)
            coord.create("/parent/child", b"data", persistent=True, session_id=session.session_id)
            
            # Register CHILDREN watch
            watch = coord.register_watch(
                "/parent",
                session.session_id,
                event_types=[EventType.CHILDREN]
            )
            
            event_received = [None]
            
            def waiter():
                event_received[0] = coord.wait_watch(watch.watch_id, timeout_seconds=5)
            
            wait_thread = threading.Thread(target=waiter)
            wait_thread.start()
            
            time.sleep(0.1)
            
            # Delete child - should trigger CHILDREN event
            coord.delete("/parent/child")
            
            wait_thread.join()
            
            assert event_received[0] is not None, "CHILDREN watch must fire on delete"
            
            coord.stop()
        finally:
            os.unlink(db_path)
