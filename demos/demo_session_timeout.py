#!/usr/bin/env python3
"""
Demo: Session Timeout → Ephemeral Nodes Deleted

MANDATORY SCENARIO FROM SPEC (Section 14.1):
1. Create session
2. Create ephemeral node
3. Wait for timeout
4. Node auto-deleted

This demonstrates that ephemeral nodes are automatically cleaned up
when their owning session expires.

Run:
    python demos/demo_session_timeout.py
"""

import sys
import time
import tempfile
import os
sys.path.insert(0, '.')

from coordinator import Coordinator


def main():
    print("=" * 60)
    print("MANDATORY DEMO: Session Timeout → Ephemeral Nodes Deleted")
    print("=" * 60)
    print()
    print("Spec requirement (Section 14.1):")
    print("  1. Create session")
    print("  2. Create ephemeral node")
    print("  3. Wait for timeout")
    print("  4. Node auto-deleted")
    print()
    
    # Use temp file for persistence
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    coord = Coordinator(db_path=db_path)
    coord.start()
    
    try:
        # Create admin session for persistent nodes
        admin_session = coord.open_session(timeout_seconds=60)
        coord.create("/locks", b"", persistent=True, session_id=admin_session.session_id)
        
        print("STEP 1: Create session with 3 second timeout")
        print("-" * 40)
        session = coord.open_session(timeout_seconds=3)
        session_id = session.session_id
        print(f"  Session created: {session_id[:16]}...")
        print(f"  Timeout: 3 seconds")
        print()
        
        print("STEP 2: Create ephemeral node")
        print("-" * 40)
        coord.create(
            "/locks/my-lock",
            b'{"holder": "client-1"}',
            persistent=False,  # EPHEMERAL
            session_id=session_id
        )
        print(f"  Created: /locks/my-lock (EPHEMERAL)")
        print(f"  Owner session: {session_id[:16]}...")
        
        # Verify node exists
        node = coord.get("/locks/my-lock")
        print(f"  Verified: Node exists with data: {node.data.decode()}")
        print()
        
        print("STEP 3: Wait for session timeout (no heartbeats)")
        print("-" * 40)
        print("  NOT sending heartbeats...")
        print("  Session will timeout after 3 seconds...")
        print()
        
        # Wait and show countdown
        for i in range(5, 0, -1):
            exists = coord.exists("/locks/my-lock")
            status = "EXISTS" if exists else "DELETED"
            print(f"  T+{5-i}s: /locks/my-lock {status}")
            time.sleep(1)
        
        # Extra wait for cleanup
        time.sleep(1)
        
        print()
        print("STEP 4: Verify node auto-deleted")
        print("-" * 40)
        
        exists = coord.exists("/locks/my-lock")
        
        if not exists:
            print("  ✓ /locks/my-lock no longer exists")
            print("  ✓ Ephemeral node was automatically deleted")
            print("  ✓ Session timeout triggered cleanup")
            print()
            print("=" * 60)
            print("SUCCESS: Session timeout correctly deleted ephemeral node")
            print("=" * 60)
        else:
            print("  ✗ ERROR: Node still exists!")
            print("  ✗ Ephemeral cleanup failed")
            
    finally:
        coord.stop()
        os.unlink(db_path)


if __name__ == "__main__":
    main()
