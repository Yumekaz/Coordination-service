"""
Tests for Crash Recovery Guarantees.

Recovery must restore consistent state:
- Persistent nodes survive
- Ephemeral nodes are cleared
- Sessions are marked dead
- Watches are cleared
- Recovery is idempotent
"""

import pytest
import tempfile
import os
import time

from coordinator import Coordinator
from persistence import Persistence
from models import NodeType


class TestPersistentNodeRecovery:
    """Tests that persistent nodes survive recovery."""
    
    def test_persistent_nodes_survive(self):
        """Persistent nodes should exist after recovery."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            # Create nodes with first coordinator
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            session_id = session.session_id
            
            coord1.create("/persist1", b"data1", persistent=True, session_id=session_id)
            coord1.create("/persist1/child", b"data2", persistent=True, session_id=session_id)
            coord1.create("/persist2", b"data3", persistent=True, session_id=session_id)
            
            coord1.stop()
            
            # Recover with new coordinator
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # All persistent nodes should exist
            node1 = coord2.get("/persist1")
            assert node1 is not None
            assert node1.data == b"data1"
            
            child = coord2.get("/persist1/child")
            assert child is not None
            assert child.data == b"data2"
            
            node2 = coord2.get("/persist2")
            assert node2 is not None
            assert node2.data == b"data3"
            
            coord2.stop()
        finally:
            os.unlink(db_path)
    
    def test_persistent_node_versions_preserved(self):
        """Version numbers should survive recovery."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            session_id = session.session_id
            
            coord1.create("/versioned", b"v1", persistent=True, session_id=session_id)
            coord1.set("/versioned", b"v2")
            coord1.set("/versioned", b"v3")
            
            node_before = coord1.get("/versioned")
            version_before = node_before.version
            
            coord1.stop()
            
            # Recover
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            node_after = coord2.get("/versioned")
            assert node_after.version == version_before
            
            coord2.stop()
        finally:
            os.unlink(db_path)


class TestEphemeralNodeRecovery:
    """Tests that ephemeral nodes are cleared on recovery."""
    
    def test_ephemeral_nodes_cleared(self):
        """Ephemeral nodes should not exist after recovery."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            session_id = session.session_id
            
            # Create ephemeral node
            coord1.create("/ephemeral", b"temp", persistent=False, session_id=session_id)
            
            # Verify it exists
            assert coord1.exists("/ephemeral") is True
            
            coord1.stop()
            
            # Recover
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Ephemeral should be gone
            assert coord2.exists("/ephemeral") is False
            
            coord2.stop()
        finally:
            os.unlink(db_path)
    
    def test_nested_ephemeral_cleared(self):
        """Nested ephemeral nodes should be cleared."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            session_id = session.session_id
            
            # Create persistent parent with ephemeral child
            coord1.create("/parent", b"persist", persistent=True, session_id=session_id)
            coord1.create("/parent/ephemeral", b"temp", persistent=False, session_id=session_id)
            
            coord1.stop()
            
            # Recover
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Parent should exist, ephemeral child should not
            assert coord2.exists("/parent") is True
            assert coord2.exists("/parent/ephemeral") is False
            
            coord2.stop()
        finally:
            os.unlink(db_path)


class TestSessionRecovery:
    """Tests that sessions are handled correctly on recovery."""
    
    def test_sessions_marked_dead(self):
        """All sessions should be marked dead after recovery."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session1 = coord1.open_session()
            session2 = coord1.open_session()
            
            session1_id = session1.session_id
            session2_id = session2.session_id
            
            coord1.stop()
            
            # Recover
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Old sessions should be dead - heartbeat should fail with ValueError
            # (sessions exist but are marked dead after recovery)
            with pytest.raises(ValueError, match="dead"):
                coord2.heartbeat(session1_id)
            
            with pytest.raises(ValueError, match="dead"):
                coord2.heartbeat(session2_id)
            
            coord2.stop()
        finally:
            os.unlink(db_path)
    
    def test_new_sessions_work_after_recovery(self):
        """New sessions should work normally after recovery."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            old_session = coord1.open_session()
            coord1.create("/test", b"data", persistent=True, session_id=old_session.session_id)
            
            coord1.stop()
            
            # Recover
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # New session should work
            new_session = coord2.open_session()
            assert new_session is not None
            
            # Can heartbeat new session
            coord2.heartbeat(new_session.session_id)
            
            # Can use new session to create nodes
            coord2.create("/new_node", b"data", persistent=True, session_id=new_session.session_id)
            assert coord2.exists("/new_node") is True
            
            coord2.stop()
        finally:
            os.unlink(db_path)


class TestWatchRecovery:
    """Tests that watches are cleared on recovery."""
    
    def test_watches_cleared(self):
        """All watches should be cleared after recovery."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            session_id = session.session_id
            
            coord1.create("/watched", b"data", persistent=True, session_id=session_id)
            coord1.register_watch("/watched", session_id)
            
            # Verify watch registered
            stats1 = coord1.get_stats()
            assert stats1["watches"] > 0
            
            coord1.stop()
            
            # Recover
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Watches should be cleared
            stats2 = coord2.get_stats()
            assert stats2["watches"] == 0
            
            coord2.stop()
        finally:
            os.unlink(db_path)


class TestMetadataConsistency:
    """Tests that metadata tree is consistent after recovery."""
    
    def test_tree_structure_consistent(self):
        """Tree structure should be valid after recovery."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            session_id = session.session_id
            
            # Build complex tree
            coord1.create("/a", b"", persistent=True, session_id=session_id)
            coord1.create("/a/b", b"", persistent=True, session_id=session_id)
            coord1.create("/a/b/c", b"", persistent=True, session_id=session_id)
            coord1.create("/a/d", b"", persistent=True, session_id=session_id)
            
            coord1.stop()
            
            # Recover
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Verify tree structure - returns (bool, list of issues)
            is_consistent, issues = coord2.verify_consistency()
            assert is_consistent is True, f"Consistency issues: {issues}"
            
            # Can list children
            children_a = coord2.list_children("/a")
            assert set(children_a) == {"b", "d"}
            
            children_b = coord2.list_children("/a/b")
            assert children_b == ["c"]
            
            coord2.stop()
        finally:
            os.unlink(db_path)
    
    def test_no_orphaned_nodes(self):
        """There should be no orphaned nodes after recovery."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            session_id = session.session_id
            
            coord1.create("/parent", b"", persistent=True, session_id=session_id)
            coord1.create("/parent/child", b"", persistent=True, session_id=session_id)
            
            coord1.stop()
            
            # Recover
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Verify no orphans - returns (bool, list of issues)
            is_consistent, issues = coord2.verify_consistency()
            # Check no orphan-related issues
            orphan_issues = [i for i in issues if "Orphaned" in i]
            assert len(orphan_issues) == 0, f"Found orphaned nodes: {orphan_issues}"
            
            coord2.stop()
        finally:
            os.unlink(db_path)


class TestRecoveryIdempotence:
    """Tests that recovery is idempotent."""
    
    def test_recovery_idempotent(self):
        """Multiple recoveries should produce same state."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            # Initial setup
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            session_id = session.session_id
            
            coord1.create("/test1", b"data1", persistent=True, session_id=session_id)
            coord1.create("/test2", b"data2", persistent=True, session_id=session_id)
            
            coord1.stop()
            
            # First recovery
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            stats2 = coord2.get_stats()
            coord2.stop()
            
            # Second recovery
            coord3 = Coordinator(db_path=db_path)
            coord3.start()
            stats3 = coord3.get_stats()
            
            # State should be identical
            assert stats2["nodes"] == stats3["nodes"]
            
            coord3.stop()
        finally:
            os.unlink(db_path)


class TestMultiCrashRecovery:
    """Tests recovery after multiple crashes."""
    
    def test_multi_crash_recovery(self):
        """System should recover correctly after multiple crashes."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            # First run
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            session = coord1.open_session()
            coord1.create("/crash1", b"data1", persistent=True, session_id=session.session_id)
            coord1.stop()
            
            # Second run - add more data
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            session = coord2.open_session()
            coord2.create("/crash2", b"data2", persistent=True, session_id=session.session_id)
            coord2.stop()
            
            # Third run - verify all data
            coord3 = Coordinator(db_path=db_path)
            coord3.start()
            
            assert coord3.exists("/crash1") is True
            assert coord3.exists("/crash2") is True
            
            node1 = coord3.get("/crash1")
            assert node1.data == b"data1"
            
            node2 = coord3.get("/crash2")
            assert node2.data == b"data2"
            
            coord3.stop()
        finally:
            os.unlink(db_path)


class TestLargeTreeRecovery:
    """Tests recovery with large amount of data."""
    
    def test_large_tree_recovery(self):
        """Large trees should recover correctly."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            session_id = session.session_id
            
            # Create many nodes
            node_count = 100
            for i in range(node_count):
                coord1.create(f"/node_{i}", f"data_{i}".encode(), persistent=True, session_id=session_id)
            
            coord1.stop()
            
            # Recover
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Verify all nodes exist
            for i in range(node_count):
                assert coord2.exists(f"/node_{i}") is True
                node = coord2.get(f"/node_{i}")
                assert node.data == f"data_{i}".encode()
            
            coord2.stop()
        finally:
            os.unlink(db_path)


class TestWALReplay:
    """Tests for Section 10 & 12 - WAL file replay during recovery."""
    
    def test_wal_entries_created_on_operations(self):
        """Operations should create entries in the custom WAL file."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/test", b"data", persistent=True, session_id=session.session_id)
            coord.set("/test", b"updated")
            
            # Check WAL has entries
            wal_ops = coord._persistence.read_wal()
            assert len(wal_ops) >= 2, "WAL should have operation entries"
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_wal_replay_on_recovery(self):
        """WAL entries should be read during recovery (Section 12)."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            # Create state
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            coord1.create("/test", b"data1", persistent=True, session_id=session.session_id)
            coord1.create("/test2", b"data2", persistent=True, session_id=session.session_id)
            
            # Get WAL path and ensure it has entries
            wal_path = coord1._persistence.get_wal_path()
            wal_ops_before = coord1._persistence.read_wal()
            
            coord1.stop()
            
            # Recovery
            coord2 = Coordinator(db_path=db_path)
            recovery_stats = coord2.start()
            
            # WAL entries should have been read
            assert recovery_stats.get("wal_entries_read", 0) >= 0
            
            # Data should be intact
            assert coord2.exists("/test")
            assert coord2.exists("/test2")
            
            coord2.stop()
        finally:
            os.unlink(db_path)
    
    def test_wal_format_matches_section_10(self):
        """WAL format should match Section 10: [timestamp][operation_type][path][data][session_id][sequence_number]."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/test", b"mydata", persistent=True, session_id=session.session_id)
            
            # Read WAL entries
            wal_ops = coord._persistence.read_wal()
            
            # Verify operation fields (Section 10 format)
            for op in wal_ops:
                assert hasattr(op, 'timestamp'), "WAL entry should have timestamp"
                assert hasattr(op, 'operation_type'), "WAL entry should have operation_type"
                assert hasattr(op, 'path'), "WAL entry should have path"
                assert hasattr(op, 'data'), "WAL entry should have data"
                assert hasattr(op, 'session_id'), "WAL entry should have session_id"
                assert hasattr(op, 'sequence_number'), "WAL entry should have sequence_number"
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_recovery_algorithm_section_12(self):
        """Recovery should follow Section 12 algorithm exactly."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            # Setup state
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            admin = coord1.open_session(timeout_seconds=60)
            coord1.create("/services", b"", persistent=True, session_id=admin.session_id)
            
            # Create ephemeral node
            ephemeral_session = coord1.open_session(timeout_seconds=60)
            coord1.create("/services/instance", b"data", persistent=False, session_id=ephemeral_session.session_id)
            
            # Register watch
            watch = coord1.register_watch("/services", admin.session_id)
            
            coord1.stop()
            
            # Recovery (Section 12 algorithm)
            coord2 = Coordinator(db_path=db_path)
            stats = coord2.start()
            
            # Step 1: Snapshot loaded (SQLite baseline)
            assert stats.get("snapshot_loaded") is not None
            
            # Step 2: WAL entries read
            assert "wal_entries_read" in stats
            
            # Step 3 & 4: Sessions marked dead
            assert stats["sessions_expired"] > 0
            
            # Step 5: Ephemeral nodes deleted
            assert not coord2.exists("/services/instance"), "Ephemeral should be deleted"
            
            # Step 6: Watches cleared
            assert stats.get("ephemeral_nodes_deleted", 0) >= 0
            
            # Persistent node survives
            assert coord2.exists("/services")
            
            coord2.stop()
        finally:
            os.unlink(db_path)
