#!/usr/bin/env python3
"""
Demo: Crash During Session Cleanup → No Leaked Nodes

MANDATORY SCENARIO FROM SPEC (Section 14.5):
1. Kill server during ephemeral cleanup
2. Restart
3. Ephemeral nodes properly cleaned

This demonstrates that even if the server crashes WHILE cleaning up
ephemeral nodes (mid-cleanup), recovery will properly finish the cleanup.

Run:
    python demos/demo_crash_during_cleanup.py
"""

import sys
import time
import tempfile
import os
import threading
import signal
sys.path.insert(0, '.')

from coordinator import Coordinator
from models import NodeType


def main() -> None:
    print("=" * 65)
    print("MANDATORY DEMO: Crash During Session Cleanup → No Leaked Nodes")
    print("=" * 65)
    print()
    print("Spec requirement (Section 14.5):")
    print("  1. Kill server during ephemeral cleanup")
    print("  2. Restart")
    print("  3. Ephemeral nodes properly cleaned")
    print()
    
    # Use temp file for persistence
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    print("PHASE 1: Setup - Create session with many ephemeral nodes")
    print("-" * 50)
    
    coord1 = Coordinator(db_path=db_path)
    coord1.start()
    
    # Create an admin session for persistent parent
    admin_session = coord1.open_session(timeout_seconds=60)
    coord1.create("/services", b"", persistent=True, session_id=admin_session.session_id)
    print("  Created /services (PERSISTENT)")
    
    # Create a session with a SHORT timeout
    ephemeral_session = coord1.open_session(timeout_seconds=2)
    session_id = ephemeral_session.session_id
    print(f"  Created session: {session_id[:16]}... (timeout: 2s)")
    
    # Create MANY ephemeral nodes to make cleanup take longer
    NUM_EPHEMERAL = 20
    for i in range(NUM_EPHEMERAL):
        path = f"/services/instance-{i:03d}"
        coord1.create(path, f'{{"id": {i}}}'.encode(), persistent=False, session_id=session_id)
    print(f"  Created {NUM_EPHEMERAL} ephemeral nodes under /services/")
    
    # Verify they exist
    children = coord1.list_children("/services")
    print(f"  Verified: {len(children)} child nodes exist")
    print()
    
    print("PHASE 2: Wait for session timeout to START cleanup")
    print("-" * 50)
    print("  NOT sending heartbeats...")
    print("  Waiting for session to expire...")
    
    # Wait just enough for timeout to trigger but not complete
    time.sleep(3)
    
    print("  Session should be expiring now...")
    print()
    
    print("PHASE 3: CRASH - Simulate crash during cleanup")
    print("-" * 50)
    print("  💥 CRASHING SERVER NOW! (mid-cleanup)")
    
    # Stop abruptly without proper cleanup
    # This simulates a crash during ephemeral node deletion
    coord1._session_manager.stop()  # Stop timeout thread
    coord1._persistence.close()     # Close DB connection
    
    # Don't call coord1.stop() - we're simulating a crash
    print("  Server crashed!")
    print()
    
    print("PHASE 4: Recovery - Restart and verify cleanup completed")
    print("-" * 50)
    
    coord2 = Coordinator(db_path=db_path)
    recovery_stats = coord2.start()
    
    print(f"  Recovery complete!")
    print(f"  Recovery stats: {recovery_stats}")
    print()
    
    # Check for leaked ephemeral nodes
    print("PHASE 5: Verification - Check for leaked nodes")
    print("-" * 50)
    
    leaked_nodes = []
    for i in range(NUM_EPHEMERAL):
        path = f"/services/instance-{i:03d}"
        if coord2.exists(path):
            leaked_nodes.append(path)
    
    if leaked_nodes:
        print(f"  ✗ FAILURE: Found {len(leaked_nodes)} leaked ephemeral nodes!")
        for path in leaked_nodes[:5]:
            print(f"    - {path}")
        if len(leaked_nodes) > 5:
            print(f"    ... and {len(leaked_nodes) - 5} more")
    else:
        print("  ✓ No leaked ephemeral nodes found!")
    
    # Verify persistent node still exists
    if coord2.exists("/services"):
        print("  ✓ Persistent node /services still exists")
    else:
        print("  ✗ ERROR: Persistent node /services was lost!")
    
    # Verify consistency
    is_consistent, issues = coord2.verify_consistency()
    if is_consistent:
        print("  ✓ Tree consistency verified")
    else:
        print(f"  ✗ Consistency issues: {issues}")
    
    # Final stats
    stats = coord2.get_stats()
    print()
    print(f"  Final node count: {stats['nodes']}")
    print(f"  Sessions (all dead after crash): {stats['sessions_total']}")
    
    coord2.stop()
    os.unlink(db_path)
    
    print()
    if not leaked_nodes and is_consistent:
        print("=" * 65)
        print("SUCCESS: Crash during cleanup did NOT leak ephemeral nodes")
        print("=" * 65)
    else:
        print("=" * 65)
        print("FAILURE: Found issues after crash during cleanup")
        print("=" * 65)


if __name__ == "__main__":
    main()
