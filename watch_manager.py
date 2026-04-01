"""
Watch Manager for the Coordination Service.

Implements one-shot notifications for metadata changes:
- Watch registration on paths
- Event triggering on state transitions
- Exactly-once delivery semantics
- Ordered delivery by sequence number

Invariants:
- Each watch fires exactly once (not 0, not 2)
- Watches are delivered after the triggering operation commits
- Watches are ordered by global sequence number
- Must re-register after firing (one-shot)
"""

import threading
import queue
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
import uuid

from models import Watch, Event, EventType
from logger import get_logger
from config import MAX_WATCHES_PER_SESSION, WATCH_WAIT_TIMEOUT

logger = get_logger("watch_manager")


class WatchManager:
    """
    Thread-safe watch registration and triggering.
    
    Guarantees:
    - Exactly-once firing (critical invariant)
    - Ordered delivery by sequence number
    - One-shot semantics
    - Thread-safe for concurrent access
    """
    
    def __init__(self):
        """Initialize the watch manager."""
        self._lock = threading.RLock()
        
        # Watches indexed by path for efficient triggering
        self._watches_by_path: Dict[str, List[Watch]] = {}
        
        # Watches indexed by ID for efficient lookup
        self._watches_by_id: Dict[str, Watch] = {}
        
        # Watches indexed by session for cleanup
        self._watches_by_session: Dict[str, Set[str]] = {}
        
        # Events pending delivery, indexed by watch_id
        self._pending_events: Dict[str, Event] = {}
        
        # Condition variables for blocking wait
        self._wait_conditions: Dict[str, threading.Condition] = {}
        
        # Global sequence counter for event ordering
        self._sequence_counter = 0
        
        logger.info("WatchManager initialized")
    
    def register(
        self,
        path: str,
        session_id: str,
        event_types: Optional[Set[EventType]] = None,
        watch_id: Optional[str] = None,
    ) -> Watch:
        """
        Register a watch on a path.
        
        Args:
            path: The path to watch
            session_id: The session registering the watch
            event_types: Types of events to watch for (default: all)
            watch_id: Optional explicit watch ID
            
        Returns:
            The created Watch
            
        Raises:
            ValueError: If too many watches for session
        """
        with self._lock:
            # Check watch limit per session
            session_watches = self._watches_by_session.get(session_id, set())
            if len(session_watches) >= MAX_WATCHES_PER_SESSION:
                raise ValueError(f"Too many watches for session: {session_id}")
            
            # Create watch
            if watch_id is None:
                watch_id = str(uuid.uuid4())
            
            if event_types is None:
                event_types = {EventType.CREATE, EventType.UPDATE, EventType.DELETE, EventType.CHILDREN}
            
            watch = Watch(
                watch_id=watch_id,
                path=path,
                session_id=session_id,
                event_types=event_types,
            )
            
            # Index by path
            if path not in self._watches_by_path:
                self._watches_by_path[path] = []
            self._watches_by_path[path].append(watch)
            
            # Index by ID
            self._watches_by_id[watch_id] = watch
            
            # Index by session
            if session_id not in self._watches_by_session:
                self._watches_by_session[session_id] = set()
            self._watches_by_session[session_id].add(watch_id)
            
            # Create condition for blocking wait
            self._wait_conditions[watch_id] = threading.Condition(self._lock)
            
            logger.debug(f"Registered watch: {watch_id} on {path}")
            
            return watch
    
    def unregister(self, watch_id: str) -> bool:
        """
        Unregister a watch.
        
        Args:
            watch_id: The watch ID to unregister
            
        Returns:
            True if watch was found and removed
        """
        with self._lock:
            if watch_id not in self._watches_by_id:
                return False
            
            watch = self._watches_by_id[watch_id]
            
            # Remove from path index
            if watch.path in self._watches_by_path:
                self._watches_by_path[watch.path] = [
                    w for w in self._watches_by_path[watch.path]
                    if w.watch_id != watch_id
                ]
                if not self._watches_by_path[watch.path]:
                    del self._watches_by_path[watch.path]
            
            # Remove from session index
            if watch.session_id in self._watches_by_session:
                self._watches_by_session[watch.session_id].discard(watch_id)
            
            # Remove from ID index
            del self._watches_by_id[watch_id]
            
            # Clean up condition
            if watch_id in self._wait_conditions:
                del self._wait_conditions[watch_id]
            
            # Clean up pending event
            if watch_id in self._pending_events:
                del self._pending_events[watch_id]
            
            logger.debug(f"Unregistered watch: {watch_id}")
            
            return True
    
    def trigger(
        self,
        path: str,
        event_type: EventType,
        data: bytes = b"",
        sequence_number: Optional[int] = None,
    ) -> List[Event]:
        """
        Trigger watches on a path for a given event type.
        
        This is the core of the watch mechanism. It finds all matching
        watches, fires them exactly once, and creates events.
        
        Args:
            path: The path where the event occurred
            event_type: The type of event
            data: The new data (if applicable)
            sequence_number: Optional explicit sequence number
            
        Returns:
            List of Events that were created
        """
        events = []
        
        with self._lock:
            # Get sequence number
            if sequence_number is None:
                self._sequence_counter += 1
                sequence_number = self._sequence_counter
            else:
                # Update counter if external sequence is higher
                if sequence_number > self._sequence_counter:
                    self._sequence_counter = sequence_number
            
            # Find watches on this path
            watches_to_fire = []
            for watch in self._watches_by_path.get(path, []):
                watches_to_fire.append((watch, event_type))
            
            # Also check parent path for CHILDREN events
            if event_type in (EventType.CREATE, EventType.DELETE):
                parent_path = self._get_parent_path(path)
                if parent_path:
                    parent_watches = self._watches_by_path.get(parent_path, [])
                    for watch in parent_watches:
                        if EventType.CHILDREN in watch.event_types:
                            # Fire with CHILDREN event type, not the original
                            watches_to_fire.append((watch, EventType.CHILDREN))
            
            # Fire matching watches
            for watch, fire_event_type in watches_to_fire:
                if watch.should_fire(fire_event_type):
                    try:
                        # Mark as fired (exactly-once)
                        watch.fire()
                        
                        # Create event with the appropriate event type
                        event = Event(
                            watch_id=watch.watch_id,
                            path=path,
                            event_type=fire_event_type,
                            data=data,
                            sequence_number=sequence_number,
                        )
                        events.append(event)
                        
                        # Store for retrieval
                        self._pending_events[watch.watch_id] = event
                        
                        # Notify waiting clients
                        if watch.watch_id in self._wait_conditions:
                            self._wait_conditions[watch.watch_id].notify_all()
                        
                        logger.debug(f"Watch fired: {watch.watch_id} for {fire_event_type.value} on {path}")
                        
                    except RuntimeError as e:
                        # Watch already fired (should never happen)
                        logger.error(f"Watch already fired (invariant violation): {watch.watch_id}")
        
        return events
    
    def _get_parent_path(self, path: str) -> str:
        """Get the parent path of a given path."""
        if path == "/" or not path:
            return ""
        parts = path.rsplit("/", 1)
        return parts[0] if parts[0] else "/"
    
    def wait(
        self,
        watch_id: str,
        timeout_seconds: float = WATCH_WAIT_TIMEOUT,
    ) -> Optional[Event]:
        """
        Block until a watch fires or timeout.
        
        Args:
            watch_id: The watch ID to wait on
            timeout_seconds: Maximum time to wait
            
        Returns:
            The Event if watch fired, None if timeout or not found
        """
        with self._lock:
            if watch_id not in self._watches_by_id:
                # Check if already fired
                return self._pending_events.get(watch_id)
            
            watch = self._watches_by_id[watch_id]
            
            # Already fired?
            if watch.is_fired:
                return self._pending_events.get(watch_id)
            
            # Wait for fire or timeout
            condition = self._wait_conditions.get(watch_id)
            if condition:
                # Wait returns False on timeout
                fired = condition.wait(timeout=timeout_seconds)
                
                if watch.is_fired:
                    return self._pending_events.get(watch_id)
        
        return None
    
    def get_event(self, watch_id: str) -> Optional[Event]:
        """Get the pending event for a watch (non-blocking)."""
        with self._lock:
            return self._pending_events.get(watch_id)
    
    def get_watch(self, watch_id: str) -> Optional[Watch]:
        """Get a watch by ID."""
        with self._lock:
            return self._watches_by_id.get(watch_id)
    
    def is_fired(self, watch_id: str) -> bool:
        """Check if a watch has fired."""
        with self._lock:
            watch = self._watches_by_id.get(watch_id)
            return watch is not None and watch.is_fired
    
    def clear_session_watches(self, session_id: str) -> List[str]:
        """
        Clear all watches for a session.
        
        Called when a session expires or disconnects.
        
        Returns:
            List of cleared watch IDs
        """
        with self._lock:
            watch_ids = list(self._watches_by_session.get(session_id, set()))
            
            for watch_id in watch_ids:
                self.unregister(watch_id)
            
            if session_id in self._watches_by_session:
                del self._watches_by_session[session_id]
            
            logger.debug(f"Cleared {len(watch_ids)} watches for session: {session_id}")
            
            return watch_ids
    
    def get_watches_for_path(self, path: str) -> List[Watch]:
        """Get all watches registered on a path."""
        with self._lock:
            return list(self._watches_by_path.get(path, []))
    
    def get_watches_for_session(self, session_id: str) -> List[Watch]:
        """Get all watches for a session."""
        with self._lock:
            watch_ids = self._watches_by_session.get(session_id, set())
            return [
                self._watches_by_id[wid]
                for wid in watch_ids
                if wid in self._watches_by_id
            ]
    
    def get_all_watches(self) -> List[Watch]:
        """Get all registered watches."""
        with self._lock:
            return list(self._watches_by_id.values())
    
    def get_watch_count(self) -> int:
        """Get the total number of watches."""
        with self._lock:
            return len(self._watches_by_id)
    
    def clear(self) -> None:
        """Clear all watches (used for testing and recovery)."""
        with self._lock:
            self._watches_by_path.clear()
            self._watches_by_id.clear()
            self._watches_by_session.clear()
            self._pending_events.clear()
            self._wait_conditions.clear()
            logger.info("WatchManager cleared")
    
    def restore_sequence(self, sequence_number: int) -> None:
        """Restore the sequence counter during recovery."""
        with self._lock:
            self._sequence_counter = sequence_number
            logger.info(f"Watch sequence restored to {sequence_number}")
