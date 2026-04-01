#!/usr/bin/env python3
"""
Demo 3: Distributed Locking

This demo shows how to implement distributed locks using
the coordination service.

Pattern:
1. Create an ephemeral node to acquire the lock
2. If node already exists, watch and wait for it to be deleted
3. Lock is automatically released if holder crashes (ephemeral node deleted)
4. Only one process can hold the lock at a time

Run:
    python demos/demo_distributed_lock.py
"""

import sys
import time
import threading
import random
sys.path.insert(0, '.')

from coordinator import Coordinator
from models import EventType


class DistributedLock:
    """A distributed lock implementation using the coordination service."""
    
    def __init__(self, coord: Coordinator, lock_path: str, session_id: str):
        self.coord = coord
        self.lock_path = lock_path
        self.session_id = session_id
        self._held = False
    
    def acquire(self, timeout: float = 10.0) -> bool:
        """
        Acquire the lock.
        
        Returns True if lock acquired, False on timeout.
        """
        start = time.time()
        
        while time.time() - start < timeout:
            try:
                # Try to create the lock node (ephemeral)
                self.coord.create(
                    self.lock_path,
                    self.session_id.encode(),
                    persistent=False,
                    session_id=self.session_id
                )
                self._held = True
                return True
                
            except KeyError:
                # Lock already exists - wait for it
                # Register a watch to be notified when deleted
                watch = self.coord.register_watch(
                    self.lock_path,
                    self.session_id,
                    event_types=[EventType.DELETE]
                )
                
                # Check if lock still exists (might have been deleted)
                if not self.coord.exists(self.lock_path):
                    self.coord.unregister_watch(watch.watch_id)
                    continue
                
                # Wait for watch to fire (with short timeout to allow retry)
                event = self.coord.wait_watch(watch.watch_id, timeout_seconds=1.0)
                
                if event:
                    # Lock was deleted, try to acquire
                    continue
                else:
                    # Timeout, check if we should continue waiting
                    continue
        
        return False
    
    def release(self):
        """Release the lock."""
        if self._held:
            try:
                self.coord.delete(self.lock_path)
                self._held = False
            except:
                pass  # Already deleted (session expired)
    
    def __enter__(self):
        if not self.acquire():
            raise TimeoutError("Could not acquire lock")
        return self
    
    def __exit__(self, *args):
        self.release()


def worker(coord: Coordinator, worker_id: str, lock_path: str, 
           shared_resource: list, results: list):
    """Worker that needs exclusive access to a shared resource."""
    
    session = coord.open_session(timeout_seconds=10)
    session_id = session.session_id
    
    print(f"[Worker-{worker_id}] Started, trying to acquire lock...")
    
    lock = DistributedLock(coord, lock_path, session_id)
    
    try:
        if lock.acquire(timeout=15.0):
            print(f"[Worker-{worker_id}] *** LOCK ACQUIRED ***")
            
            # Critical section - exclusive access to shared resource
            shared_resource.append(f"worker-{worker_id}")
            
            # Verify exclusive access (no concurrent modifications)
            before = len(shared_resource)
            time.sleep(random.uniform(0.3, 0.8))  # Simulate work
            after = len(shared_resource)
            
            if after != before:
                results.append(("VIOLATION", worker_id))
                print(f"[Worker-{worker_id}] ERROR: Concurrent modification detected!")
            else:
                results.append(("OK", worker_id))
                print(f"[Worker-{worker_id}] Work completed successfully")
            
            lock.release()
            print(f"[Worker-{worker_id}] Lock released")
        else:
            print(f"[Worker-{worker_id}] Failed to acquire lock (timeout)")
            results.append(("TIMEOUT", worker_id))
    
    except Exception as e:
        print(f"[Worker-{worker_id}] Error: {e}")
        results.append(("ERROR", worker_id))
    
    finally:
        try:
            coord.close_session(session_id)
        except:
            pass


def main():
    print("=" * 60)
    print("DEMO 3: Distributed Locking")
    print("=" * 60)
    print()
    
    # Use temp file instead of memory for thread safety
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    coord = Coordinator(db_path=db_path)
    coord.start()
    
    try:
        # Setup lock namespace
        session = coord.open_session()
        coord.create("/locks", b"", persistent=True, session_id=session.session_id)
        print("Created lock namespace: /locks")
        print()
        
        lock_path = "/locks/shared-resource"
        shared_resource = []
        results = []
        
        # Start multiple workers competing for the same lock
        print("Starting 5 workers competing for the same lock...")
        print()
        
        threads = []
        for i in range(5):
            t = threading.Thread(
                target=worker,
                args=(coord, str(i + 1), lock_path, shared_resource, results)
            )
            threads.append(t)
            t.start()
            time.sleep(0.1)  # Small stagger
        
        # Wait for all workers
        for t in threads:
            t.join()
        
        print()
        print("=" * 40)
        print("Results:")
        print(f"  Successful acquisitions: {sum(1 for r in results if r[0] == 'OK')}")
        print(f"  Timeouts: {sum(1 for r in results if r[0] == 'TIMEOUT')}")
        print(f"  Violations: {sum(1 for r in results if r[0] == 'VIOLATION')}")
        print(f"  Errors: {sum(1 for r in results if r[0] == 'ERROR')}")
        print()
        print(f"  Total operations recorded: {len(shared_resource)}")
        
        if not any(r[0] == 'VIOLATION' for r in results):
            print()
            print("  ✓ No concurrent access violations - lock worked correctly!")
        
    finally:
        coord.stop()
        import os
        os.unlink(db_path)
    
    print()
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
