"""
Tests for Concurrency and Stress Testing.

These tests verify thread safety and correctness under load:
- Many concurrent operations
- Race conditions
- Stress testing
- Performance under load
"""

import pytest
import time
import threading
import tempfile
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from coordinator import Coordinator
from models import EventType


class TestConcurrentOperations:
    """Concurrent operation stress tests."""
    
    def test_concurrent_creates_no_duplicates(self):
        """Concurrent creates don't create duplicates."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/concurrent", b"", persistent=True, session_id=session.session_id)
            
            errors = []
            success = []
            lock = threading.Lock()
            
            def try_create(i):
                try:
                    s = coord.open_session()
                    coord.create(
                        f"/concurrent/node-{i % 10}",  # Only 10 unique paths
                        f"data-{i}".encode(),
                        persistent=True,
                        session_id=s.session_id
                    )
                    with lock:
                        success.append(i)
                except KeyError:
                    # Already exists - expected for duplicates
                    pass
                except Exception as e:
                    with lock:
                        errors.append((i, str(e)))
            
            # 50 threads trying to create 10 unique nodes
            with ThreadPoolExecutor(max_workers=50) as executor:
                futures = [executor.submit(try_create, i) for i in range(50)]
                for f in as_completed(futures):
                    pass
            
            # Exactly 10 nodes should exist
            children = coord.list_children("/concurrent")
            assert len(children) == 10
            assert len(errors) == 0
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_concurrent_updates_version_consistency(self):
        """Concurrent updates maintain version consistency."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/version", b"", persistent=True, session_id=session.session_id)
            coord.create("/version/data", b"0", persistent=True, session_id=session.session_id)
            
            update_count = [0]
            lock = threading.Lock()
            
            def update():
                for _ in range(10):
                    coord.set("/version/data", f"update-{time.time()}".encode())
                    with lock:
                        update_count[0] += 1
            
            # 10 threads each doing 10 updates = 100 total updates
            threads = [threading.Thread(target=update) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
            # Version should be 1 (initial) + 100 updates = 101
            node = coord.get("/version/data")
            assert node.version == 101
            assert update_count[0] == 100
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_concurrent_reads_writes_no_corruption(self):
        """Concurrent reads and writes never see corruption."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/rw", b"", persistent=True, session_id=session.session_id)
            coord.create("/rw/data", b"initial-value", persistent=True, session_id=session.session_id)
            
            corruptions = []
            stop_flag = threading.Event()
            lock = threading.Lock()
            
            def writer():
                i = 0
                while not stop_flag.is_set():
                    # Always write well-formed data
                    coord.set("/rw/data", f"value-{i:010d}".encode())
                    i += 1
                    time.sleep(0.001)
            
            def reader():
                while not stop_flag.is_set():
                    node = coord.get("/rw/data")
                    data = node.data.decode()
                    # Check for corruption
                    if not data.startswith("initial-value") and not data.startswith("value-"):
                        with lock:
                            corruptions.append(data)
                    time.sleep(0.0005)
            
            # Start readers and writers
            writer_thread = threading.Thread(target=writer)
            reader_threads = [threading.Thread(target=reader) for _ in range(5)]
            
            writer_thread.start()
            for t in reader_threads:
                t.start()
            
            time.sleep(1)  # Run for 1 second
            stop_flag.set()
            
            writer_thread.join()
            for t in reader_threads:
                t.join()
            
            assert len(corruptions) == 0, f"Corruptions detected: {corruptions}"
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestSessionConcurrency:
    """Concurrent session operations."""
    
    def test_many_concurrent_sessions(self):
        """Many sessions operate correctly."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            admin = coord.open_session()
            coord.create("/sessions", b"", persistent=True, session_id=admin.session_id)
            
            sessions = []
            lock = threading.Lock()
            
            def create_session(i):
                s = coord.open_session(timeout_seconds=30)
                coord.create(
                    f"/sessions/s-{i}",
                    b"data",
                    persistent=False,
                    session_id=s.session_id
                )
                with lock:
                    sessions.append(s)
            
            # Create 50 concurrent sessions
            with ThreadPoolExecutor(max_workers=50) as executor:
                futures = [executor.submit(create_session, i) for i in range(50)]
                for f in as_completed(futures):
                    pass
            
            assert len(sessions) == 50
            
            # All ephemeral nodes exist
            children = coord.list_children("/sessions")
            assert len(children) == 50
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_concurrent_heartbeats(self):
        """Concurrent heartbeats don't cause issues."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session(timeout_seconds=30)
            heartbeat_count = [0]
            lock = threading.Lock()
            
            def heartbeat():
                for _ in range(20):
                    coord.heartbeat(session.session_id)
                    with lock:
                        heartbeat_count[0] += 1
                    time.sleep(0.01)
            
            # 10 threads sending heartbeats
            threads = [threading.Thread(target=heartbeat) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
            assert heartbeat_count[0] == 200
            assert coord._session_manager.is_alive(session.session_id)
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestWatchConcurrency:
    """Concurrent watch operations."""
    
    def test_concurrent_watch_registration(self):
        """Many watches registered concurrently."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/watched", b"", persistent=True, session_id=session.session_id)
            coord.create("/watched/data", b"initial", persistent=True, session_id=session.session_id)
            
            watches = []
            lock = threading.Lock()
            
            def register():
                for _ in range(10):
                    watch = coord.register_watch(
                        "/watched/data",
                        session.session_id,
                        event_types=[EventType.UPDATE]
                    )
                    with lock:
                        watches.append(watch)
            
            # 10 threads each registering 10 watches = 100 total
            threads = [threading.Thread(target=register) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
            assert len(watches) == 100
            
            # All unique
            watch_ids = [w.watch_id for w in watches]
            assert len(set(watch_ids)) == 100
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_watch_fire_under_load(self):
        """Watches fire correctly under load."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/load", b"", persistent=True, session_id=session.session_id)
            
            # Create many nodes with watches
            watches = []
            for i in range(20):
                coord.create(f"/load/node-{i}", b"initial", persistent=True, 
                           session_id=session.session_id)
                watch = coord.register_watch(
                    f"/load/node-{i}",
                    session.session_id,
                    event_types=[EventType.UPDATE]
                )
                watches.append((f"/load/node-{i}", watch))
            
            # Update all nodes concurrently
            def update_node(path):
                coord.set(path, b"updated")
            
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(update_node, path) for path, _ in watches]
                for f in as_completed(futures):
                    pass
            
            # All watches should fire
            fired = 0
            for path, watch in watches:
                event = coord.wait_watch(watch.watch_id, timeout_seconds=1.0)
                if event:
                    fired += 1
            
            assert fired == 20
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestStressScenarios:
    """Stress testing scenarios."""
    
    def test_rapid_create_delete_cycles(self):
        """Rapid create/delete cycles don't cause issues."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/stress", b"", persistent=True, session_id=session.session_id)
            
            cycles = [0]
            errors = []
            lock = threading.Lock()
            
            def cycle(thread_id):
                for i in range(20):
                    path = f"/stress/node-{thread_id}-{i}"
                    try:
                        coord.create(path, b"data", persistent=True, session_id=session.session_id)
                        coord.delete(path)
                        with lock:
                            cycles[0] += 1
                    except Exception as e:
                        with lock:
                            errors.append((thread_id, i, str(e)))
            
            # 5 threads each doing 20 create/delete cycles
            threads = [threading.Thread(target=cycle, args=(i,)) for i in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
            assert cycles[0] == 100
            assert len(errors) == 0
            
            # No nodes left
            children = coord.list_children("/stress")
            assert len(children) == 0
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_mixed_operations_stress(self):
        """Mixed operations under stress."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            admin = coord.open_session()
            coord.create("/mixed", b"", persistent=True, session_id=admin.session_id)
            coord.create("/mixed/shared", b"0", persistent=True, session_id=admin.session_id)
            
            operations = [0]
            errors = []
            lock = threading.Lock()
            stop_flag = threading.Event()
            
            def worker(worker_id):
                session = coord.open_session(timeout_seconds=30)
                
                while not stop_flag.is_set():
                    op = operations[0] % 4
                    try:
                        if op == 0:  # Read
                            coord.get("/mixed/shared")
                        elif op == 1:  # Write
                            coord.set("/mixed/shared", f"v-{operations[0]}".encode())
                        elif op == 2:  # Check exists
                            coord.exists("/mixed/shared")
                        elif op == 3:  # List children
                            coord.list_children("/mixed")
                        
                        with lock:
                            operations[0] += 1
                    except Exception as e:
                        with lock:
                            errors.append((worker_id, str(e)))
                    
                    time.sleep(0.001)
                
                coord.close_session(session.session_id)
            
            # Start workers
            threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
            for t in threads:
                t.start()
            
            time.sleep(2)  # Run for 2 seconds
            stop_flag.set()
            
            for t in threads:
                t.join()
            
            assert len(errors) == 0, f"Errors: {errors}"
            assert operations[0] > 100  # Should have done many operations
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestRecoveryConcurrency:
    """Recovery under concurrent operations."""
    
    def test_recovery_preserves_concurrent_writes(self):
        """All committed writes survive recovery."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            coord1.create("/recovery", b"", persistent=True, session_id=session.session_id)
            
            # Many concurrent writes
            def write(i):
                coord1.create(
                    f"/recovery/node-{i}",
                    f"data-{i}".encode(),
                    persistent=True,
                    session_id=session.session_id
                )
            
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(write, i) for i in range(100)]
                for f in as_completed(futures):
                    pass
            
            coord1.stop()
            
            # Recovery
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # All nodes survived
            children = coord2.list_children("/recovery")
            assert len(children) == 100
            
            coord2.stop()
        finally:
            os.unlink(db_path)
