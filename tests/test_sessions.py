"""
Tests for Session Management.

Tests session lifecycle:
- Session creation and configuration
- Heartbeat processing
- Timeout detection
- Ephemeral node cleanup
- Atomic cleanup operations
"""

import pytest
import time
import threading
from session_manager import SessionManager
from models import Session


class TestSessionBasic:
    """Basic session operations."""
    
    def test_open_session(self, session_manager: SessionManager):
        """Should create a new session."""
        session = session_manager.open_session(timeout_seconds=30)
        
        assert session.session_id is not None
        assert session.is_alive is True
        assert session.timeout_seconds == 30
    
    def test_open_session_with_explicit_id(self, session_manager: SessionManager):
        """Should create session with explicit ID."""
        session = session_manager.open_session(
            timeout_seconds=30,
            session_id="test-session-123"
        )
        
        assert session.session_id == "test-session-123"
    
    def test_duplicate_session_fails(self, session_manager: SessionManager):
        """Should not allow duplicate session IDs."""
        session_manager.open_session(session_id="dup-session")
        
        with pytest.raises(KeyError, match="already exists"):
            session_manager.open_session(session_id="dup-session")
    
    def test_get_session(self, session_manager: SessionManager):
        """Should retrieve an existing session."""
        created = session_manager.open_session()
        
        retrieved = session_manager.get_session(created.session_id)
        
        assert retrieved is not None
        assert retrieved.session_id == created.session_id
    
    def test_get_nonexistent_session(self, session_manager: SessionManager):
        """Should return None for nonexistent session."""
        result = session_manager.get_session("nonexistent")
        assert result is None
    
    def test_is_alive(self, session_manager: SessionManager):
        """Should check if session is alive."""
        session = session_manager.open_session()
        
        assert session_manager.is_alive(session.session_id) is True
        assert session_manager.is_alive("nonexistent") is False


class TestSessionHeartbeat:
    """Heartbeat processing tests."""
    
    def test_heartbeat_keeps_alive(self, session_manager: SessionManager):
        """Heartbeat should reset timeout."""
        session = session_manager.open_session(timeout_seconds=30)
        initial_heartbeat = session.last_heartbeat
        
        time.sleep(0.1)
        session_manager.heartbeat(session.session_id)
        
        assert session.last_heartbeat > initial_heartbeat
    
    def test_heartbeat_nonexistent_fails(self, session_manager: SessionManager):
        """Should fail to heartbeat nonexistent session."""
        with pytest.raises(KeyError):
            session_manager.heartbeat("nonexistent")
    
    def test_dead_session_heartbeat_fails(self, session_manager: SessionManager):
        """Should fail to heartbeat dead session."""
        session = session_manager.open_session()
        session_manager.expire_session(session.session_id)
        
        with pytest.raises(ValueError, match="dead"):
            session_manager.heartbeat(session.session_id)


class TestSessionTimeout:
    """Timeout detection tests."""
    
    def test_session_expires_on_timeout(self, session_manager: SessionManager):
        """Session should expire after timeout."""
        session_manager.start()
        
        # Use minimum timeout (5 seconds per config)
        session = session_manager.open_session(timeout_seconds=5)
        assert session.is_alive is True
        
        # Wait for timeout (need to wait longer than timeout + check interval)
        time.sleep(6.5)
        
        assert session.is_alive is False
    
    def test_heartbeat_prevents_timeout(self, session_manager: SessionManager):
        """Heartbeat should prevent timeout."""
        session_manager.start()
        
        session = session_manager.open_session(timeout_seconds=2)
        
        # Keep alive with heartbeats
        for _ in range(4):
            time.sleep(0.5)
            session_manager.heartbeat(session.session_id)
        
        assert session.is_alive is True
    
    def test_is_expired(self, session_manager: SessionManager):
        """Should correctly detect expired sessions."""
        session = session_manager.open_session(timeout_seconds=5)
        
        assert session.is_expired() is False
        
        time.sleep(5.5)
        
        assert session.is_expired() is True


class TestSessionEphemeralTracking:
    """Ephemeral node tracking tests."""
    
    def test_add_ephemeral_node(self, session_manager: SessionManager):
        """Should track ephemeral nodes."""
        session = session_manager.open_session()
        
        session_manager.add_ephemeral_node(session.session_id, "/lock1")
        session_manager.add_ephemeral_node(session.session_id, "/lock2")
        
        nodes = session_manager.get_ephemeral_nodes(session.session_id)
        assert nodes == {"/lock1", "/lock2"}
    
    def test_remove_ephemeral_node(self, session_manager: SessionManager):
        """Should remove ephemeral node tracking."""
        session = session_manager.open_session()
        
        session_manager.add_ephemeral_node(session.session_id, "/lock1")
        session_manager.remove_ephemeral_node(session.session_id, "/lock1")
        
        nodes = session_manager.get_ephemeral_nodes(session.session_id)
        assert nodes == set()
    
    def test_add_to_nonexistent_session_fails(self, session_manager: SessionManager):
        """Should fail to track for nonexistent session."""
        with pytest.raises(KeyError):
            session_manager.add_ephemeral_node("nonexistent", "/lock")


class TestSessionExpiry:
    """Session expiry and cleanup tests."""
    
    def test_expire_session(self, session_manager: SessionManager):
        """Should expire a session."""
        session = session_manager.open_session()
        
        result = session_manager.expire_session(session.session_id)
        
        assert result is not None
        assert result.is_alive is False
    
    def test_expire_nonexistent_returns_none(self, session_manager: SessionManager):
        """Should return None for nonexistent session."""
        result = session_manager.expire_session("nonexistent")
        assert result is None
    
    def test_expire_already_expired(self, session_manager: SessionManager):
        """Should handle already expired session."""
        session = session_manager.open_session()
        session_manager.expire_session(session.session_id)
        
        result = session_manager.expire_session(session.session_id)
        assert result.is_alive is False
    
    def test_expiry_callback(self, session_manager: SessionManager):
        """Should call expiry callbacks."""
        callback_sessions = []
        
        def on_expire(session):
            callback_sessions.append(session)
        
        session_manager.add_expiry_callback(on_expire)
        session = session_manager.open_session()
        
        session_manager.expire_session(session.session_id)
        
        assert len(callback_sessions) == 1
        assert callback_sessions[0].session_id == session.session_id


class TestSessionConcurrent:
    """Concurrent session tests."""
    
    def test_concurrent_sessions(self, session_manager: SessionManager):
        """Should handle multiple concurrent sessions."""
        sessions = []
        
        def create_session():
            s = session_manager.open_session()
            sessions.append(s)
        
        threads = [threading.Thread(target=create_session) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(sessions) == 10
        session_ids = [s.session_id for s in sessions]
        assert len(set(session_ids)) == 10  # All unique
    
    def test_concurrent_heartbeats(self, session_manager: SessionManager):
        """Should handle concurrent heartbeats safely."""
        session = session_manager.open_session()
        heartbeat_count = [0]
        lock = threading.Lock()
        
        def heartbeat():
            for _ in range(10):
                session_manager.heartbeat(session.session_id)
                with lock:
                    heartbeat_count[0] += 1
        
        threads = [threading.Thread(target=heartbeat) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert heartbeat_count[0] == 50
        assert session.is_alive is True


class TestSessionCleanup:
    """Cleanup atomicity tests."""
    
    def test_cleanup_is_atomic(self, session_manager: SessionManager):
        """Session state change should be atomic."""
        session = session_manager.open_session()
        session_manager.add_ephemeral_node(session.session_id, "/lock1")
        session_manager.add_ephemeral_node(session.session_id, "/lock2")
        
        # Expire session
        result = session_manager.expire_session(session.session_id)
        
        # Session should be dead
        assert result.is_alive is False
        
        # Ephemeral nodes should still be tracked (cleanup happens in coordinator)
        nodes = result.ephemeral_nodes
        assert len(nodes) == 2
    
    def test_mark_all_dead(self, session_manager: SessionManager):
        """Should mark all sessions as dead."""
        for i in range(5):
            session_manager.open_session()
        
        session_manager.mark_all_dead()
        
        alive = session_manager.get_alive_sessions()
        assert len(alive) == 0
