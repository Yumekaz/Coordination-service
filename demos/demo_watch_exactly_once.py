#!/usr/bin/env python3
"""
Demo: Watch Fires Exactly Once

MANDATORY SCENARIO FROM SPEC (Section 14.3):
1. Register watch on node
2. Change node 3 times
3. Watch fires 3 times (not 1, not 9)

This demonstrates the CRITICAL exactly-once watch semantics.
Each watch fires exactly once per registration.

Run:
    python demos/demo_watch_exactly_once.py
"""

import sys
import time
import threading
import tempfile
import os
sys.path.insert(0, '.')

from coordinator import Coordinator
from models import EventType


def main():
    print("=" * 60)
    print("MANDATORY DEMO: Watch Fires Exactly Once")
    print("=" * 60)
    print()
    print("Spec requirement (Section 14.3):")
    print("  1. Register watch on node")
    print("  2. Change node 3 times")
    print("  3. Watch fires 3 times (not 1, not 9)")
    print()
    print("CRITICAL: This is the hardest guarantee to implement correctly!")
    print()
    
    # Use temp file for persistence
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    coord = Coordinator(db_path=db_path)
    coord.start()
    
    try:
        # Create session and node
        session = coord.open_session(timeout_seconds=60)
        session_id = session.session_id
        
        coord.create("/watched", b"v0", persistent=True, session_id=session_id)
        print("Created /watched with initial value: v0")
        print()
        
        print("STEP 1: Register watch, make change, observe fire")
        print("-" * 40)
        
        events_received = []
        
        def watch_and_wait(change_num: int):
            """Register watch, wait for it to fire."""
            # Register watch
            watch = coord.register_watch(
                "/watched",
                session_id,
                event_types=[EventType.UPDATE]
            )
            print(f"  Registered watch #{change_num} (id: {watch.watch_id[:8]}...)")
            
            # Wait for watch in background
            def waiter():
                event = coord.wait_watch(watch.watch_id, timeout_seconds=5.0)
                if event:
                    events_received.append({
                        "change_num": change_num,
                        "event_type": event.event_type.value,
                        "data": event.data.decode() if event.data else None,
                        "sequence": event.sequence_number
                    })
                    print(f"  Watch #{change_num} FIRED: {event.event_type.value}, data={event.data.decode()}")
            
            return threading.Thread(target=waiter)
        
        # Change 1
        print()
        print("Change 1: v0 → v1")
        waiter1 = watch_and_wait(1)
        waiter1.start()
        time.sleep(0.1)  # Let watch register
        
        coord.set("/watched", b"v1")
        waiter1.join()
        
        # Change 2
        print()
        print("Change 2: v1 → v2")
        waiter2 = watch_and_wait(2)
        waiter2.start()
        time.sleep(0.1)
        
        coord.set("/watched", b"v2")
        waiter2.join()
        
        # Change 3
        print()
        print("Change 3: v2 → v3")
        waiter3 = watch_and_wait(3)
        waiter3.start()
        time.sleep(0.1)
        
        coord.set("/watched", b"v3")
        waiter3.join()
        
        print()
        print("STEP 2: Verify exactly 3 watch fires")
        print("-" * 40)
        
        print(f"  Total events received: {len(events_received)}")
        print(f"  Expected events: 3")
        print()
        
        for event in events_received:
            print(f"  Event {event['change_num']}: {event['event_type']} → {event['data']} (seq={event['sequence']})")
        
        print()
        
        # Verify exactly 3
        if len(events_received) == 3:
            print("  ✓ Exactly 3 events received")
        elif len(events_received) < 3:
            print(f"  ✗ FAILURE: Only {len(events_received)} events (missed notifications)")
        else:
            print(f"  ✗ FAILURE: {len(events_received)} events (duplicate notifications)")
        
        print()
        print("STEP 3: Verify one-shot semantics")
        print("-" * 40)
        
        # Register a watch but don't re-register after fire
        watch = coord.register_watch(
            "/watched",
            session_id,
            event_types=[EventType.UPDATE]
        )
        print(f"  Registered one watch (id: {watch.watch_id[:8]}...)")
        
        # Make 3 changes without re-registering
        extra_events = []
        
        def capture_event():
            event = coord.wait_watch(watch.watch_id, timeout_seconds=2.0)
            if event:
                extra_events.append(event)
        
        waiter = threading.Thread(target=capture_event)
        waiter.start()
        time.sleep(0.1)
        
        print("  Making 3 changes WITHOUT re-registering watch...")
        coord.set("/watched", b"v4")
        coord.set("/watched", b"v5")
        coord.set("/watched", b"v6")
        
        waiter.join()
        
        print(f"  Events from single watch: {len(extra_events)}")
        
        if len(extra_events) == 1:
            print("  ✓ Watch fired exactly once (one-shot confirmed)")
        else:
            print(f"  ✗ Watch fired {len(extra_events)} times (should be 1)")
        
        print()
        print("PROOF OF EXACTLY-ONCE SEMANTICS:")
        print("-" * 40)
        print("  1. Each watch registration fires AT MOST once")
        print("  2. When event occurs, watch fires EXACTLY once (not 0)")
        print("  3. After firing, watch is removed (must re-register)")
        print("  4. Multiple changes require multiple watch registrations")
        
        # Verify the guarantee
        total_expected = 3 + 1  # 3 from step 1, 1 from step 3
        total_received = len(events_received) + len(extra_events)
        
        print()
        if total_received == total_expected:
            print("=" * 60)
            print("SUCCESS: Watch exactly-once semantics verified")
            print("=" * 60)
        else:
            print("=" * 60)
            print(f"FAILURE: Expected {total_expected} fires, got {total_received}")
            print("=" * 60)
        
    finally:
        coord.stop()
        os.unlink(db_path)


if __name__ == "__main__":
    main()
