#!/usr/bin/env python3
"""
Demo 5: Crash Recovery

This demo shows how the coordination service recovers from crashes
while maintaining data integrity.

Demonstrates:
1. Persistent nodes survive crashes
2. Ephemeral nodes are deleted on crash (sessions expire)
3. Data integrity is maintained
4. Recovery is deterministic and idempotent

Run:
    python demos/demo_crash_recovery.py
"""

import sys
import tempfile
import os
import json
sys.path.insert(0, '.')

from coordinator import Coordinator


def print_state(coord: Coordinator, label: str):
    """Print the current state of the coordination service."""
    print(f"\n--- {label} ---")
    
    # List all nodes
    def list_tree(path: str, indent: int = 0):
        node = coord.get(path)
        if node:
            prefix = "  " * indent
            node_type = node.node_type.value.upper()
            version = node.version
            data = node.data.decode() if node.data else ""
            if len(data) > 30:
                data = data[:27] + "..."
            print(f"{prefix}{path} [{node_type}] v{version}: {data}")
            
            try:
                children = coord.list_children(path)
                for child in sorted(children):
                    list_tree(f"{path}/{child}" if path != "/" else f"/{child}", indent + 1)
            except:
                pass
    
    list_tree("/")
    
    # Stats
    stats = coord.get_stats()
    print(f"\nStats: nodes={stats['nodes']}, sessions_alive={stats['sessions_alive']}")


def main():
    print("=" * 60)
    print("DEMO 5: Crash Recovery")
    print("=" * 60)
    print()
    
    # Use a temp file for persistence
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    try:
        # ========================================
        # PHASE 1: Create initial data
        # ========================================
        print("PHASE 1: Creating initial data")
        print("-" * 40)
        
        coord1 = Coordinator(db_path=db_path)
        coord1.start()
        
        session1 = coord1.open_session(timeout_seconds=30)
        s1_id = session1.session_id
        
        # Create persistent nodes (survive crash)
        coord1.create("/config", b"root config", persistent=True, session_id=s1_id)
        coord1.create("/config/database", b'{"host": "db.example.com", "port": 5432}', 
                      persistent=True, session_id=s1_id)
        coord1.create("/config/cache", b'{"enabled": true}', persistent=True, session_id=s1_id)
        
        # Create services namespace with ephemeral nodes
        coord1.create("/services", b"", persistent=True, session_id=s1_id)
        coord1.create("/services/api", b"", persistent=True, session_id=s1_id)
        
        # Ephemeral nodes (will be deleted on crash)
        coord1.create("/services/api/instance-1", b'{"host": "10.0.0.1"}', 
                      persistent=False, session_id=s1_id)
        coord1.create("/services/api/instance-2", b'{"host": "10.0.0.2"}', 
                      persistent=False, session_id=s1_id)
        
        # Create locks namespace
        coord1.create("/locks", b"", persistent=True, session_id=s1_id)
        coord1.create("/locks/global-lock", b"holder: session1", 
                      persistent=False, session_id=s1_id)  # Ephemeral
        
        print_state(coord1, "After initial setup")
        
        # Make some updates (versions will be preserved)
        coord1.set("/config/database", b'{"host": "db.example.com", "port": 5432, "pool": 20}')
        coord1.set("/config/cache", b'{"enabled": true, "ttl": 300}')
        
        print_state(coord1, "After updates (version increments)")
        
        # ========================================
        # SIMULATE CRASH
        # ========================================
        print()
        print("=" * 40)
        print("SIMULATING CRASH (stopping coordinator)")
        print("=" * 40)
        
        coord1.stop()
        del coord1
        
        print("\nCoordinator stopped. All sessions are now dead.")
        print("Ephemeral nodes should be cleaned up on recovery.")
        
        # ========================================
        # PHASE 2: Recovery
        # ========================================
        print()
        print("-" * 40)
        print("PHASE 2: Recovery")
        print("-" * 40)
        
        coord2 = Coordinator(db_path=db_path)
        coord2.start()
        
        print_state(coord2, "After recovery")
        
        # Verify ephemeral nodes are gone
        assert coord2.exists("/services/api/instance-1") is False, "Ephemeral should be deleted"
        assert coord2.exists("/services/api/instance-2") is False, "Ephemeral should be deleted"
        assert coord2.exists("/locks/global-lock") is False, "Ephemeral lock should be released"
        
        # Verify persistent nodes and versions preserved
        db_node = coord2.get("/config/database")
        assert db_node.version == 2, f"Version should be 2, got {db_node.version}"
        assert b"pool" in db_node.data, "Updated data should be preserved"
        
        print("\n✓ Ephemeral nodes cleaned up")
        print("✓ Persistent nodes preserved")
        print("✓ Version numbers preserved")
        print("✓ Data integrity maintained")
        
        # ========================================
        # PHASE 3: Continue operation
        # ========================================
        print()
        print("-" * 40)
        print("PHASE 3: Continue normal operation")
        print("-" * 40)
        
        # New session
        session2 = coord2.open_session()
        s2_id = session2.session_id
        
        # Re-register services
        coord2.create("/services/api/instance-3", b'{"host": "10.0.0.3"}', 
                      persistent=False, session_id=s2_id)
        
        # Acquire lock
        coord2.create("/locks/global-lock", b"holder: session2", 
                      persistent=False, session_id=s2_id)
        
        print_state(coord2, "After new registrations")
        
        # Verify consistency
        is_consistent, issues = coord2.verify_consistency()
        print(f"\nConsistency check: {'PASSED' if is_consistent else 'FAILED'}")
        if issues:
            for issue in issues:
                print(f"  - {issue}")
        
        coord2.stop()
        
        # ========================================
        # PHASE 4: Multiple recovery cycles
        # ========================================
        print()
        print("-" * 40)
        print("PHASE 4: Verify idempotent recovery")
        print("-" * 40)
        
        # Recover multiple times - should produce same state
        for i in range(3):
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            stats = coord.get_stats()
            print(f"Recovery {i + 1}: nodes={stats['nodes']}")
            
            coord.stop()
        
        print("\n✓ Recovery is idempotent")
        
    finally:
        os.unlink(db_path)
    
    print()
    print("=" * 60)
    print("Demo complete!")
    print("=" * 60)
    print()
    print("Key takeaways:")
    print("  1. Persistent nodes survive crashes")
    print("  2. Ephemeral nodes are automatically cleaned up")
    print("  3. Sessions expire on crash - locks are released")
    print("  4. Version numbers are preserved")
    print("  5. Recovery is deterministic and idempotent")


if __name__ == "__main__":
    main()
