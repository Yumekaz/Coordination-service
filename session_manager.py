"""
Session Manager for the Coordination Service.

Manages session lifecycle including:
- Session creation with configurable timeout
- Heartbeat processing
- Timeout detection (background thread)
- Ephemeral node cleanup on session death

Invariants:
- Ephemeral nodes cannot outlive their sessions
- Session expiration is deterministic
- Cleanup is atomic (all-or-nothing)
"""

import threading
import time
from typing import Dict, List, Optional, Callable, Set
from datetime import datetime
import uuid

from models import Session
from logger import get_logger
from config import (
    DEFAULT_SESSION_TIMEOUT,
    SESSION_CHECK_INTERVAL,
    MIN_SESSION_TIMEOUT,
    MAX_SESSION_TIMEOUT,
)

logger = get_logger("session_manager")


class SessionManager:
    """
    Thread-safe session lifecycle manager.
    
    Guarantees:
    - Session IDs are unique
    - Timeout detection is deterministic
    - Session cleanup is atomic
    - Thread-safe for concurrent access
    """
    
    def __init__(self):
        """Initialize the session manager."""
        self._lock = threading.RLock()
        self._sessions: Dict[str, Session] = {}
        self._expiry_callbacks: List[Callable[[Session, str], None]] = []
        
        # Background thread for timeout detection
        self._running = False
        self._timeout_thread: Optional[threading.Thread] = None
        
        logger.info("SessionManager initialized")
    
    def start(self) -> None:
        """Start the background timeout detection thread."""
        if self._running:
            return
        
        self._running = True
        self._timeout_thread = threading.Thread(
            target=self._timeout_loop,
            daemon=True,
            name="session-timeout-checker"
        )
        self._timeout_thread.start()
        logger.info("Session timeout checker started")
    
    def stop(self) -> None:
        """Stop the background timeout detection thread."""
        self._running = False
        if self._timeout_thread:
            self._timeout_thread.join(timeout=2.0)
            self._timeout_thread = None
        logger.info("Session timeout checker stopped")
    
    def _timeout_loop(self) -> None:
        """Background loop to detect expired sessions."""
        while self._running:
            try:
                self._check_timeouts()
            except Exception as e:
                logger.error(f"Error in timeout check: {e}")
            time.sleep(SESSION_CHECK_INTERVAL)
    
    def _check_timeouts(self) -> None:
        """Check all sessions for timeout and expire dead ones."""
        current_time = datetime.now().timestamp()
        expired_sessions = []
        
        with self._lock:
            for session in list(self._sessions.values()):
                if session.is_alive and session.is_expired(current_time):
                    expired_sessions.append(session)
        
        # Expire sessions outside the lock to prevent deadlock
        for session in expired_sessions:
            self.expire_session(session.session_id)

    def _clone_session(self, session: Session) -> Session:
        """Create a detached copy of a session."""
        return Session.from_dict(session.to_dict())
    
    def open_session(
        self,
        timeout_seconds: int = DEFAULT_SESSION_TIMEOUT,
        session_id: Optional[str] = None,
    ) -> Session:
        """
        Open a new session.
        
        Args:
            timeout_seconds: Session timeout (5-300 seconds)
            session_id: Optional explicit session ID (for recovery)
            
        Returns:
            The created Session
            
        Raises:
            ValueError: If timeout is out of range
            KeyError: If session_id already exists
        """
        # Validate timeout
        if timeout_seconds < MIN_SESSION_TIMEOUT:
            timeout_seconds = MIN_SESSION_TIMEOUT
        if timeout_seconds > MAX_SESSION_TIMEOUT:
            timeout_seconds = MAX_SESSION_TIMEOUT
        
        with self._lock:
            # Generate or validate session ID
            if session_id is None:
                session_id = str(uuid.uuid4())
            
            if session_id in self._sessions:
                raise KeyError(f"Session already exists: {session_id}")
            
            # Create session
            session = Session(
                session_id=session_id,
                timeout_seconds=timeout_seconds,
            )
            
            self._sessions[session_id] = session
            logger.info(f"Session opened: {session_id} (timeout={timeout_seconds}s)")
            
            return session

    def remove_session(self, session_id: str) -> Optional[Session]:
        """Remove a session from the manager."""
        with self._lock:
            return self._sessions.pop(session_id, None)

    def replace_session(self, session: Session) -> Session:
        """Replace the stored state for a session."""
        with self._lock:
            self._sessions[session.session_id] = self._clone_session(session)
            return self._sessions[session.session_id]
    
    def heartbeat(self, session_id: str) -> Session:
        """
        Process a heartbeat for a session.
        
        Args:
            session_id: The session ID to heartbeat
            
        Returns:
            The updated Session
            
        Raises:
            KeyError: If session doesn't exist
            ValueError: If session is dead
        """
        with self._lock:
            if session_id not in self._sessions:
                raise KeyError(f"Session does not exist: {session_id}")
            
            session = self._sessions[session_id]
            
            if not session.is_alive:
                raise ValueError(f"Session is dead: {session_id}")
            
            session.heartbeat()
            logger.debug(f"Heartbeat received: {session_id}")
            
            return session
    
    def get_session(self, session_id: str) -> Optional[Session]:
        """Get a session by ID."""
        with self._lock:
            return self._sessions.get(session_id)
    
    def is_alive(self, session_id: str) -> bool:
        """Check if a session is alive."""
        with self._lock:
            session = self._sessions.get(session_id)
            return session is not None and session.is_alive
    
    def expire_session(self, session_id: str) -> Optional[Session]:
        """
        Expire a session and trigger cleanup callbacks.
        
        This marks the session as dead and notifies callbacks
        to clean up ephemeral nodes.
        
        Args:
            session_id: The session ID to expire
            
        Returns:
            The expired Session, or None if not found
        """
        with self._lock:
            if session_id not in self._sessions:
                return None
            
            session = self._sessions[session_id]
            
            if not session.is_alive:
                return session  # Already expired
            snapshot = self._clone_session(session)
            session.is_alive = False
            logger.info(f"Session expired: {session_id}")
        
        # Notify expiry callbacks outside the lock
        for callback in self._expiry_callbacks:
            try:
                callback(session, "timeout")
            except Exception as e:
                with self._lock:
                    self._sessions[session_id] = snapshot
                logger.error(f"Expiry callback error: {e}")
                raise
        
        return session
    
    def close_session(self, session_id: str) -> Optional[Session]:
        """
        Close a session explicitly (client disconnect).
        
        This is equivalent to expire_session but with a different log message.
        """
        with self._lock:
            if session_id not in self._sessions:
                return None
            
            session = self._sessions[session_id]
            
            if not session.is_alive:
                return session
            snapshot = self._clone_session(session)
            session.is_alive = False
            logger.info(f"Session closed: {session_id}")
        
        # Notify expiry callbacks
        for callback in self._expiry_callbacks:
            try:
                callback(session, "explicit")
            except Exception as e:
                with self._lock:
                    self._sessions[session_id] = snapshot
                logger.error(f"Expiry callback error: {e}")
                raise
        
        return session
    
    def add_ephemeral_node(self, session_id: str, path: str) -> None:
        """
        Track an ephemeral node for a session.
        
        Called when an ephemeral node is created.
        """
        with self._lock:
            if session_id not in self._sessions:
                raise KeyError(f"Session does not exist: {session_id}")
            
            session = self._sessions[session_id]
            session.ephemeral_nodes.add(path)
            logger.debug(f"Added ephemeral node tracking: {path} -> {session_id}")
    
    def remove_ephemeral_node(self, session_id: str, path: str) -> None:
        """
        Remove tracking for an ephemeral node.
        
        Called when an ephemeral node is deleted.
        """
        with self._lock:
            if session_id in self._sessions:
                self._sessions[session_id].ephemeral_nodes.discard(path)
                logger.debug(f"Removed ephemeral node tracking: {path}")
    
    def get_ephemeral_nodes(self, session_id: str) -> Set[str]:
        """Get all ephemeral node paths for a session."""
        with self._lock:
            if session_id not in self._sessions:
                return set()
            return set(self._sessions[session_id].ephemeral_nodes)
    
    def add_expiry_callback(self, callback: Callable[[Session, str], None]) -> None:
        """
        Add a callback to be notified when sessions expire.
        
        Used by the coordinator to clean up ephemeral nodes.
        """
        with self._lock:
            self._expiry_callbacks.append(callback)
    
    def remove_expiry_callback(self, callback: Callable[[Session, str], None]) -> None:
        """Remove an expiry callback."""
        with self._lock:
            if callback in self._expiry_callbacks:
                self._expiry_callbacks.remove(callback)
    
    def get_all_sessions(self) -> List[Session]:
        """Get all sessions."""
        with self._lock:
            return list(self._sessions.values())
    
    def get_alive_sessions(self) -> List[Session]:
        """Get all alive sessions."""
        with self._lock:
            return [s for s in self._sessions.values() if s.is_alive]
    
    def get_session_count(self) -> int:
        """Get the total number of sessions."""
        with self._lock:
            return len(self._sessions)
    
    def get_alive_session_count(self) -> int:
        """Get the number of alive sessions."""
        with self._lock:
            return sum(1 for s in self._sessions.values() if s.is_alive)
    
    def clear(self) -> None:
        """Clear all sessions (used for testing and recovery)."""
        with self._lock:
            self._sessions.clear()
            logger.info("SessionManager cleared")
    
    def mark_all_dead(self) -> None:
        """
        Mark all sessions as dead.
        
        Called during crash recovery - all sessions are considered
        expired after a crash because clients have lost connection.
        """
        with self._lock:
            for session in self._sessions.values():
                session.is_alive = False
            logger.info(f"Marked {len(self._sessions)} sessions as dead")
    
    def restore_sessions(self, sessions: List[Session]) -> None:
        """
        Restore sessions during recovery.
        
        Note: All restored sessions are marked as dead because
        clients have lost connection during the crash.
        """
        with self._lock:
            self._sessions.clear()
            for session in sessions:
                session.is_alive = False  # All dead after crash
                self._sessions[session.session_id] = session
            logger.info(f"Restored {len(sessions)} sessions (all marked dead)")
