"""
Tests for Watch Management.

Tests one-shot notification mechanism:
- Watch registration
- Event triggering
- Exactly-once semantics (critical)
- Ordering guarantees
- One-shot behavior
"""

import pytest
import time
import threading
from watch_manager import WatchManager
from models import Watch, Event, EventType


class TestWatchRegistration:
    """Watch registration tests."""
    
    def test_register_watch(self, watch_manager: WatchManager):
        """Should register a watch successfully."""
        watch = watch_manager.register(
            path="/config",
            session_id="session-1",
        )
        
        assert watch.watch_id is not None
        assert watch.path == "/config"
        assert watch.session_id == "session-1"
        assert watch.is_fired is False
    
    def test_register_with_explicit_id(self, watch_manager: WatchManager):
        """Should register with explicit watch ID."""
        watch = watch_manager.register(
            path="/config",
            session_id="session-1",
            watch_id="custom-watch-id",
        )
        
        assert watch.watch_id == "custom-watch-id"
    
    def test_register_with_event_types(self, watch_manager: WatchManager):
        """Should register with specific event types."""
        watch = watch_manager.register(
            path="/config",
            session_id="session-1",
            event_types={EventType.CREATE, EventType.UPDATE},
        )
        
        assert EventType.CREATE in watch.event_types
        assert EventType.UPDATE in watch.event_types
        assert EventType.DELETE not in watch.event_types
    
    def test_get_watch(self, watch_manager: WatchManager):
        """Should retrieve a watch by ID."""
        created = watch_manager.register(path="/config", session_id="s1")
        
        retrieved = watch_manager.get_watch(created.watch_id)
        
        assert retrieved is not None
        assert retrieved.watch_id == created.watch_id


class TestWatchTriggering:
    """Watch triggering tests."""
    
    def test_watch_fires_on_create(self, watch_manager: WatchManager):
        """Watch should fire on node creation."""
        watch = watch_manager.register(path="/config", session_id="s1")
        
        events = watch_manager.trigger(
            path="/config",
            event_type=EventType.CREATE,
            data=b"data",
        )
        
        assert len(events) == 1
        assert events[0].event_type == EventType.CREATE
        assert events[0].path == "/config"
        assert watch.is_fired is True
    
    def test_watch_fires_on_update(self, watch_manager: WatchManager):
        """Watch should fire on node update."""
        watch = watch_manager.register(path="/config", session_id="s1")
        
        events = watch_manager.trigger(
            path="/config",
            event_type=EventType.UPDATE,
            data=b"updated",
        )
        
        assert len(events) == 1
        assert events[0].event_type == EventType.UPDATE
    
    def test_watch_fires_on_delete(self, watch_manager: WatchManager):
        """Watch should fire on node deletion."""
        watch = watch_manager.register(path="/config", session_id="s1")
        
        events = watch_manager.trigger(
            path="/config",
            event_type=EventType.DELETE,
        )
        
        assert len(events) == 1
        assert events[0].event_type == EventType.DELETE


class TestWatchExactlyOnce:
    """Exactly-once semantics tests (CRITICAL)."""
    
    def test_watch_fires_exactly_once(self, watch_manager: WatchManager):
        """Watch should fire exactly once - not 0, not 2."""
        watch = watch_manager.register(path="/config", session_id="s1")
        
        # First trigger
        events1 = watch_manager.trigger(path="/config", event_type=EventType.UPDATE)
        assert len(events1) == 1
        
        # Second trigger - should NOT fire again
        events2 = watch_manager.trigger(path="/config", event_type=EventType.UPDATE)
        assert len(events2) == 0
        
        # Third trigger - still no fire
        events3 = watch_manager.trigger(path="/config", event_type=EventType.DELETE)
        assert len(events3) == 0
    
    def test_fire_twice_raises_error(self, watch_manager: WatchManager):
        """Manually firing twice should raise error."""
        watch = watch_manager.register(path="/config", session_id="s1")
        
        watch.fire()
        
        with pytest.raises(RuntimeError, match="already fired"):
            watch.fire()
    
    def test_multiple_watches_same_node_all_fire(self, watch_manager: WatchManager):
        """Multiple watches on same node should all fire exactly once."""
        watches = []
        for i in range(3):
            w = watch_manager.register(path="/config", session_id=f"s{i}")
            watches.append(w)
        
        events = watch_manager.trigger(path="/config", event_type=EventType.UPDATE)
        
        assert len(events) == 3
        for watch in watches:
            assert watch.is_fired is True
    
    def test_watches_fire_independently(self, watch_manager: WatchManager):
        """Each watch fires independently."""
        watch1 = watch_manager.register(path="/node1", session_id="s1")
        watch2 = watch_manager.register(path="/node2", session_id="s1")
        
        # Trigger only node1
        events = watch_manager.trigger(path="/node1", event_type=EventType.UPDATE)
        
        assert len(events) == 1
        assert watch1.is_fired is True
        assert watch2.is_fired is False  # Not triggered


class TestWatchOrdering:
    """Watch ordering tests."""
    
    def test_watch_ordering_preserved(self, watch_manager: WatchManager):
        """Events should be ordered by sequence number."""
        watch = watch_manager.register(path="/config", session_id="s1")
        
        events = watch_manager.trigger(
            path="/config",
            event_type=EventType.UPDATE,
            sequence_number=42,
        )
        
        assert len(events) == 1
        assert events[0].sequence_number == 42
    
    def test_sequence_numbers_monotonic(self, watch_manager: WatchManager):
        """Sequence numbers should be monotonically increasing."""
        sequences = []
        
        for i in range(10):
            watch = watch_manager.register(path=f"/node{i}", session_id="s1")
            events = watch_manager.trigger(path=f"/node{i}", event_type=EventType.CREATE)
            sequences.append(events[0].sequence_number)
        
        # Check monotonicity
        for i in range(1, len(sequences)):
            assert sequences[i] > sequences[i-1]
    
    def test_watch_ordering_under_load(self, watch_manager: WatchManager):
        """Ordering should be preserved under load."""
        all_events = []
        lock = threading.Lock()
        
        def register_and_trigger(i):
            watch = watch_manager.register(path=f"/node{i}", session_id="s1")
            events = watch_manager.trigger(path=f"/node{i}", event_type=EventType.CREATE)
            with lock:
                all_events.extend(events)
        
        threads = [threading.Thread(target=register_and_trigger, args=(i,)) for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # All events should have unique sequence numbers
        sequences = [e.sequence_number for e in all_events]
        assert len(sequences) == len(set(sequences))


class TestWatchOneShot:
    """One-shot semantics tests."""
    
    def test_watch_one_shot_semantics(self, watch_manager: WatchManager):
        """Watch must be re-registered after firing."""
        watch1 = watch_manager.register(path="/config", session_id="s1")
        
        # Fire the watch
        watch_manager.trigger(path="/config", event_type=EventType.UPDATE)
        assert watch1.is_fired is True
        
        # Re-register a new watch
        watch2 = watch_manager.register(path="/config", session_id="s1")
        
        # New watch should fire
        events = watch_manager.trigger(path="/config", event_type=EventType.UPDATE)
        assert len(events) == 1
        assert events[0].watch_id == watch2.watch_id


class TestWatchWait:
    """Watch wait/blocking tests."""
    
    def test_wait_returns_event(self, watch_manager: WatchManager):
        """Wait should return event when fired."""
        watch = watch_manager.register(path="/config", session_id="s1")
        
        def trigger_after_delay():
            time.sleep(0.1)
            watch_manager.trigger(path="/config", event_type=EventType.UPDATE, data=b"data")
        
        threading.Thread(target=trigger_after_delay).start()
        
        event = watch_manager.wait(watch.watch_id, timeout_seconds=5.0)
        
        assert event is not None
        assert event.event_type == EventType.UPDATE
    
    def test_wait_timeout(self, watch_manager: WatchManager):
        """Wait should timeout if watch doesn't fire."""
        watch = watch_manager.register(path="/config", session_id="s1")
        
        event = watch_manager.wait(watch.watch_id, timeout_seconds=0.1)
        
        assert event is None
    
    def test_wait_already_fired(self, watch_manager: WatchManager):
        """Wait should return immediately if already fired."""
        watch = watch_manager.register(path="/config", session_id="s1")
        watch_manager.trigger(path="/config", event_type=EventType.UPDATE)
        
        event = watch_manager.wait(watch.watch_id, timeout_seconds=0.1)
        
        assert event is not None


class TestWatchUnregister:
    """Watch unregistration tests."""
    
    def test_unregister_removes_watch(self, watch_manager: WatchManager):
        """Unregister should remove the watch."""
        watch = watch_manager.register(path="/config", session_id="s1")
        
        result = watch_manager.unregister(watch.watch_id)
        
        assert result is True
        assert watch_manager.get_watch(watch.watch_id) is None
    
    def test_unregister_nonexistent(self, watch_manager: WatchManager):
        """Unregister nonexistent should return False."""
        result = watch_manager.unregister("nonexistent")
        assert result is False
    
    def test_unregistered_watch_doesnt_fire(self, watch_manager: WatchManager):
        """Unregistered watch should not fire."""
        watch = watch_manager.register(path="/config", session_id="s1")
        watch_manager.unregister(watch.watch_id)
        
        events = watch_manager.trigger(path="/config", event_type=EventType.UPDATE)
        
        assert len(events) == 0


class TestWatchSession:
    """Session-related watch tests."""
    
    def test_clear_session_watches(self, watch_manager: WatchManager):
        """Should clear all watches for a session."""
        for i in range(5):
            watch_manager.register(path=f"/node{i}", session_id="s1")
        for i in range(3):
            watch_manager.register(path=f"/other{i}", session_id="s2")
        
        cleared = watch_manager.clear_session_watches("s1")
        
        assert len(cleared) == 5
        assert watch_manager.get_watch_count() == 3
    
    def test_get_watches_for_session(self, watch_manager: WatchManager):
        """Should get all watches for a session."""
        for i in range(3):
            watch_manager.register(path=f"/node{i}", session_id="s1")
        watch_manager.register(path="/other", session_id="s2")
        
        watches = watch_manager.get_watches_for_session("s1")
        
        assert len(watches) == 3


class TestWatchConcurrent:
    """Concurrent watch tests."""
    
    def test_concurrent_watch_registration(self, watch_manager: WatchManager):
        """Should handle concurrent registrations safely."""
        watches = []
        lock = threading.Lock()
        
        def register():
            for i in range(10):
                w = watch_manager.register(path=f"/node{threading.current_thread().name}_{i}", session_id="s1")
                with lock:
                    watches.append(w)
        
        threads = [threading.Thread(target=register) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(watches) == 50
        assert watch_manager.get_watch_count() == 50
    
    def test_many_concurrent_watches(self, watch_manager: WatchManager):
        """Should handle 100+ concurrent watches."""
        watches = []
        events = []
        lock = threading.Lock()
        
        # Register 100 watches
        for i in range(100):
            w = watch_manager.register(path=f"/node{i}", session_id="s1")
            watches.append(w)
        
        # Trigger all concurrently
        def trigger(i):
            evts = watch_manager.trigger(path=f"/node{i}", event_type=EventType.UPDATE)
            with lock:
                events.extend(evts)
        
        threads = [threading.Thread(target=trigger, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(events) == 100
        for watch in watches:
            assert watch.is_fired is True


class TestWatchNonExistent:
    """Edge case tests."""
    
    def test_watch_non_existent_path(self, watch_manager: WatchManager):
        """Should allow watching non-existent path."""
        watch = watch_manager.register(path="/does/not/exist", session_id="s1")
        
        assert watch is not None
        
        # When path is created, watch should fire
        events = watch_manager.trigger(path="/does/not/exist", event_type=EventType.CREATE)
        assert len(events) == 1
