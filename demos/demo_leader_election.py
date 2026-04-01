#!/usr/bin/env python3
"""
Demo 1: Leader Election

This demo shows how to use the coordination service to implement
leader election among multiple processes.

Pattern:
1. All candidates create ephemeral sequential nodes under /election
2. The node with the smallest sequence number becomes leader
3. If leader dies (session expires), next in line becomes leader
4. Watches notify candidates when leadership changes

Run:
    python demos/demo_leader_election.py
"""

import sys
import time
import threading
import random
sys.path.insert(0, '.')

from coordinator import Coordinator


def simulate_candidate(coord: Coordinator, name: str, results: dict):
    """Simulate a candidate participating in leader election."""
    
    # Open a session for this candidate
    session = coord.open_session(timeout_seconds=5)
    session_id = session.session_id
    print(f"[{name}] Joined election (session: {session_id[:8]}...)")
    
    # Create an ephemeral sequential node
    # In a real system, we'd use sequential nodes; here we use naming
    election_path = f"/election/{name}"
    try:
        coord.create(
            election_path,
            name.encode(),
            persistent=False,  # Ephemeral - disappears when session dies
            session_id=session_id
        )
    except Exception as e:
        print(f"[{name}] Failed to join: {e}")
        return
    
    # Check who's the leader (first child alphabetically)
    def check_leadership():
        children = coord.list_children("/election")
        if children:
            children.sort()
            leader = children[0]
            is_leader = leader == name
            return is_leader, leader
        return False, None
    
    is_leader, current_leader = check_leadership()
    if is_leader:
        print(f"[{name}] *** I AM THE LEADER ***")
        results["leader"] = name
    else:
        print(f"[{name}] Leader is: {current_leader}")
    
    # Simulate some work
    work_time = random.uniform(0.5, 2.0)
    for _ in range(int(work_time * 10)):
        time.sleep(0.1)
        # Heartbeat to stay alive
        try:
            coord.heartbeat(session_id)
        except:
            break
    
    # Leader "crashes" (stops heartbeating)
    if is_leader:
        print(f"[{name}] Leader stepping down...")
        coord.close_session(session_id)
        time.sleep(0.5)
        
        # Check new leader
        is_leader, new_leader = check_leadership()
        if new_leader:
            print(f"[{name}] New leader: {new_leader}")
            results["leader"] = new_leader


def main():
    print("=" * 60)
    print("DEMO 1: Leader Election")
    print("=" * 60)
    print()
    
    # Create coordinator with temp file (in-memory doesn't work across threads)
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    coord = Coordinator(db_path=db_path)
    coord.start()
    
    try:
        # Create election parent node
        session = coord.open_session()
        coord.create("/election", b"", persistent=True, session_id=session.session_id)
        print("Created /election node")
        print()
        
        # Results tracking
        results = {"leader": None}
        
        # Start multiple candidates
        candidates = ["alice", "bob", "charlie"]
        threads = []
        
        print("Starting candidates...")
        for name in candidates:
            t = threading.Thread(
                target=simulate_candidate,
                args=(coord, name, results)
            )
            threads.append(t)
            t.start()
            time.sleep(0.2)  # Stagger starts
        
        # Wait for all candidates
        for t in threads:
            t.join()
        
        print()
        print(f"Final leader: {results.get('leader', 'None')}")
        
    finally:
        coord.stop()
        import os
        os.unlink(db_path)
    
    print()
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
