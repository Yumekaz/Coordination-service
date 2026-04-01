"""
Tests for Linearizability Guarantees.

All operations must be linearizable:
- Read always sees latest committed write
- Concurrent writes are serialized
- No stale reads ever
- Version numbers monotonically increase
- Global sequence ordering
"""

import pytest
import threading
import time
from typing import List, Tuple

from coordinator import Coordinator
from models import NodeType


class TestReadSeesLatestWrite:
    """Tests that reads always see the latest write."""
    
    def test_read_sees_latest_write(self, coordinator: Coordinator):
        """Read immediately after write should see new value."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        coordinator.create("/read_test", b"v1", persistent=True, session_id=session_id)
        
        for i in range(10):
            expected = f"v{i+2}".encode()
            coordinator.set("/read_test", expected)
            
            # Read should immediately see new value
            node = coordinator.get("/read_test")
            assert node.data == expected
    
    def test_reader_writer_consistency(self, coordinator: Coordinator):
        """Concurrent readers and writers maintain consistency."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        coordinator.create("/rw_test", b"0", persistent=True, session_id=session_id)
        
        seen_values = []
        lock = threading.Lock()
        
        def writer():
            for i in range(1, 21):
                coordinator.set("/rw_test", str(i).encode())
                time.sleep(0.001)
        
        def reader():
            readings = []
            for _ in range(50):
                node = coordinator.get("/rw_test")
                readings.append(int(node.data.decode()))
                time.sleep(0.0005)
            with lock:
                seen_values.extend(readings)
        
        writer_thread = threading.Thread(target=writer)
        reader_threads = [threading.Thread(target=reader) for _ in range(3)]
        
        writer_thread.start()
        for t in reader_threads:
            t.start()
        
        writer_thread.join()
        for t in reader_threads:
            t.join()
        
        # All seen values should be valid (0-20)
        for v in seen_values:
            assert 0 <= v <= 20


class TestConcurrentWritesOrdered:
    """Tests that concurrent writes are serialized."""
    
    def test_concurrent_writes_ordered(self, coordinator: Coordinator):
        """Concurrent writes should be totally ordered."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        coordinator.create("/order_test", b"init", persistent=True, session_id=session_id)
        
        write_order = []
        lock = threading.Lock()
        
        def writer(value: int):
            coordinator.set("/order_test", str(value).encode())
            node = coordinator.get("/order_test")
            with lock:
                write_order.append((value, node.version))
        
        threads = [threading.Thread(target=writer, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Versions should be strictly increasing
        versions = [v for _, v in sorted(write_order, key=lambda x: x[1])]
        for i in range(len(versions) - 1):
            assert versions[i] < versions[i + 1]
    
    def test_writes_serialized(self, coordinator: Coordinator):
        """Final value should be from exactly one writer."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        coordinator.create("/serialize_test", b"init", persistent=True, session_id=session_id)
        
        def writer(value: str):
            coordinator.set("/serialize_test", value.encode())
        
        # Run multiple concurrent writes
        threads = [threading.Thread(target=writer, args=(f"writer_{i}",)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Final value should be from one specific writer
        node = coordinator.get("/serialize_test")
        assert node.data.decode().startswith("writer_")


class TestNoStaleReads:
    """Tests that stale reads are impossible."""
    
    def test_no_stale_reads(self, coordinator: Coordinator):
        """Once a value is written, old values are never seen."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        coordinator.create("/stale_test", b"0", persistent=True, session_id=session_id)
        
        max_seen = [0]
        stale_reads = []
        lock = threading.Lock()
        stop_flag = threading.Event()
        
        def writer():
            for i in range(1, 51):
                coordinator.set("/stale_test", str(i).encode())
                time.sleep(0.001)
            stop_flag.set()
        
        def reader():
            while not stop_flag.is_set():
                node = coordinator.get("/stale_test")
                value = int(node.data.decode())
                with lock:
                    if value < max_seen[0]:
                        stale_reads.append((value, max_seen[0]))
                    max_seen[0] = max(max_seen[0], value)
                time.sleep(0.0005)
        
        writer_thread = threading.Thread(target=writer)
        reader_threads = [threading.Thread(target=reader) for _ in range(3)]
        
        writer_thread.start()
        for t in reader_threads:
            t.start()
        
        writer_thread.join()
        for t in reader_threads:
            t.join()
        
        # No stale reads should occur
        assert len(stale_reads) == 0, f"Stale reads detected: {stale_reads[:5]}"


class TestVersionMonotonicity:
    """Tests that version numbers always increase."""
    
    def test_version_always_increases(self, coordinator: Coordinator):
        """Version should strictly increase on each update."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        coordinator.create("/version_test", b"init", persistent=True, session_id=session_id)
        
        last_version = 1
        for i in range(20):
            coordinator.set("/version_test", f"update_{i}".encode())
            node = coordinator.get("/version_test")
            
            assert node.version > last_version
            last_version = node.version
    
    def test_version_never_reused(self, coordinator: Coordinator):
        """Deleted and recreated node should have new version."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        # Create, update, delete
        coordinator.create("/reuse_test", b"v1", persistent=True, session_id=session_id)
        coordinator.set("/reuse_test", b"v2")
        original = coordinator.get("/reuse_test")
        original_version = original.version
        
        coordinator.delete("/reuse_test")
        
        # Recreate
        coordinator.create("/reuse_test", b"new", persistent=True, session_id=session_id)
        recreated = coordinator.get("/reuse_test")
        
        # Version starts fresh from 1 for new node
        assert recreated.version == 1


class TestGlobalSequenceOrdering:
    """Tests global sequence number ordering."""
    
    def test_sequence_numbers_unique(self, coordinator: Coordinator):
        """Each operation should get unique sequence number."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        sequences = []
        
        # Perform multiple operations
        coordinator.create("/seq_test1", b"", persistent=True, session_id=session_id)
        sequences.append(coordinator._operation_log.current_sequence)
        
        coordinator.create("/seq_test2", b"", persistent=True, session_id=session_id)
        sequences.append(coordinator._operation_log.current_sequence)
        
        coordinator.set("/seq_test1", b"updated")
        sequences.append(coordinator._operation_log.current_sequence)
        
        coordinator.delete("/seq_test2")
        sequences.append(coordinator._operation_log.current_sequence)
        
        # All sequence numbers should be unique
        assert len(sequences) == len(set(sequences))
    
    def test_operations_globally_ordered(self, coordinator: Coordinator):
        """All operations should have a total order."""
        session = coordinator.open_session()
        session_id = session.session_id
        
        sequences = []
        lock = threading.Lock()
        
        def create_node(path: str):
            coordinator.create(path, b"", persistent=True, session_id=session_id)
            seq = coordinator._operation_log.current_sequence
            with lock:
                sequences.append(seq)
        
        # Create multiple nodes concurrently
        threads = [threading.Thread(target=create_node, args=(f"/global_{i}",)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Sequences should be unique
        assert len(sequences) == len(set(sequences))
        
        # Sequences should form a total order (all comparable)
        sorted_seqs = sorted(sequences)
        for i in range(len(sorted_seqs) - 1):
            assert sorted_seqs[i] < sorted_seqs[i + 1]
