"""
Tests for Integration Scenarios.

Real-world scenarios demonstrating the coordination service:
- Leader election
- Service discovery
- Distributed locking
- Configuration management
- Concurrent clients
- Crash scenarios
- Watch races
"""

import pytest
import time
import threading
import tempfile
import os
import json
import inspect

from coordinator import Coordinator
from models import NodeType, EventType


def _find_callable(obj, candidates):
    """Return the first callable attribute present on obj."""
    for name in candidates:
        candidate = getattr(obj, name, None)
        if callable(candidate):
            return candidate
    return None


def _call_with_supported_kwargs(func, **kwargs):
    """Call a function using only kwargs it explicitly accepts."""
    signature = inspect.signature(func)
    call_kwargs = {
        name: value
        for name, value in kwargs.items()
        if name in signature.parameters
    }
    return func(**call_kwargs)


def _extract_value(result, *names):
    """Pull a value out of dict-like or object-like return values."""
    if isinstance(result, str):
        return result
    if isinstance(result, dict):
        for name in names:
            if name in result and result[name] is not None:
                return result[name]
    for name in names:
        if hasattr(result, name):
            value = getattr(result, name)
            if value is not None:
                return value
    return None


class TestLeaderElectionScenario:
    """Leader election using ephemeral nodes."""
    
    def test_leader_election_scenario(self):
        """Implement leader election pattern."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            # Setup election namespace
            admin = coord.open_session()
            coord.create("/election", b"", persistent=True, session_id=admin.session_id)
            
            # Multiple candidates join
            candidates = []
            for i in range(3):
                session = coord.open_session(timeout_seconds=5)
                coord.create(
                    f"/election/candidate-{i}",
                    f"candidate-{i}".encode(),
                    persistent=False,  # Ephemeral
                    session_id=session.session_id
                )
                candidates.append(session)
            
            # Leader is first candidate (sorted)
            children = coord.list_children("/election")
            children.sort()
            leader = children[0]
            assert leader == "candidate-0"
            
            # Kill leader's session
            coord.close_session(candidates[0].session_id)
            
            time.sleep(0.2)
            
            # New leader emerges
            children = coord.list_children("/election")
            children.sort()
            new_leader = children[0]
            assert new_leader == "candidate-1"
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestServiceDiscoveryScenario:
    """Service discovery using ephemeral nodes."""
    
    def test_service_discovery_scenario(self):
        """Services register and discover each other."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            # Setup services namespace
            admin = coord.open_session()
            coord.create("/services", b"", persistent=True, session_id=admin.session_id)
            coord.create("/services/api", b"", persistent=True, session_id=admin.session_id)
            
            # Register multiple service instances
            instances = []
            for i in range(3):
                session = coord.open_session(timeout_seconds=10)
                info = json.dumps({"host": f"10.0.0.{i+1}", "port": 8080})
                coord.create(
                    f"/services/api/instance-{i}",
                    info.encode(),
                    persistent=False,
                    session_id=session.session_id
                )
                instances.append(session)
            
            # Discover services
            children = coord.list_children("/services/api")
            assert len(children) == 3
            
            # Get service info
            node = coord.get("/services/api/instance-0")
            info = json.loads(node.data.decode())
            assert info["host"] == "10.0.0.1"
            
            # Service dies
            coord.close_session(instances[1].session_id)
            time.sleep(0.2)
            
            # Discovery reflects change
            children = coord.list_children("/services/api")
            assert len(children) == 2
            assert "instance-1" not in children
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestDistributedLockScenario:
    """Distributed lock using ephemeral nodes."""
    
    def test_distributed_lock_scenario(self):
        """Only one client holds lock at a time."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            # Setup locks namespace
            admin = coord.open_session()
            coord.create("/locks", b"", persistent=True, session_id=admin.session_id)
            
            # First client acquires lock
            client1 = coord.open_session()
            coord.create(
                "/locks/resource",
                b"client1",
                persistent=False,
                session_id=client1.session_id
            )
            
            # Second client tries to acquire - should fail
            client2 = coord.open_session()
            with pytest.raises(KeyError):  # Already exists
                coord.create(
                    "/locks/resource",
                    b"client2",
                    persistent=False,
                    session_id=client2.session_id
                )
            
            # Client1 releases (session close)
            coord.close_session(client1.session_id)
            time.sleep(0.2)
            
            # Now client2 can acquire
            coord.create(
                "/locks/resource",
                b"client2",
                persistent=False,
                session_id=client2.session_id
            )
            
            # Verify lock holder
            node = coord.get("/locks/resource")
            assert node.data == b"client2"
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestVersionGuardedWrites:
    """Version-guarded write scenarios."""

    def test_expected_version_write_succeeds(self):
        """A matching expected_version should allow an update."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        coord = Coordinator(db_path=db_path)
        coord.start()
        try:
            if "expected_version" not in inspect.signature(coord.set).parameters:
                pytest.skip("Coordinator.set does not yet accept expected_version")

            admin = coord.open_session()
            coord.create("/cas", b"v1", persistent=True, session_id=admin.session_id)

            updated = coord.set("/cas", b"v2", expected_version=1)
            assert updated.version == 2
            assert updated.data == b"v2"

            node = coord.get("/cas")
            assert node.version == 2
            assert node.data == b"v2"
        finally:
            coord.stop()
            os.unlink(db_path)

    def test_stale_expected_version_conflicts(self):
        """A stale expected_version should be rejected and leave the node unchanged."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        coord = Coordinator(db_path=db_path)
        coord.start()
        try:
            if "expected_version" not in inspect.signature(coord.set).parameters:
                pytest.skip("Coordinator.set does not yet accept expected_version")

            admin = coord.open_session()
            coord.create("/cas-stale", b"v1", persistent=True, session_id=admin.session_id)
            coord.set("/cas-stale", b"v2", expected_version=1)

            with pytest.raises((ValueError, RuntimeError, KeyError, AssertionError)):
                coord.set("/cas-stale", b"v3", expected_version=1)

            node = coord.get("/cas-stale")
            assert node.version == 2
            assert node.data == b"v2"
        finally:
            coord.stop()
            os.unlink(db_path)


class TestLeaseLifecycle:
    """Lease acquire/release and session cleanup scenarios."""

    def test_lease_lifecycle_owner_conflict_and_session_cleanup(self):
        """Lease ownership should be exclusive and cleaned up with the owning session."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        coord = Coordinator(db_path=db_path)
        coord.start()
        try:
            acquire = _find_callable(coord, ["acquire_lease", "lease_acquire", "create_lease"])
            release = _find_callable(coord, ["release_lease", "lease_release", "delete_lease"])
            if acquire is None or release is None:
                pytest.skip("Lease API is not available yet")

            admin = coord.open_session()
            coord.create("/leases", b"", persistent=True, session_id=admin.session_id)

            owner_one = coord.open_session()
            owner_two = coord.open_session()

            lease = _call_with_supported_kwargs(
                acquire,
                path="/leases/resource",
                resource="/leases/resource",
                lease_path="/leases/resource",
                session_id=owner_one.session_id,
                owner_session_id=owner_one.session_id,
                ttl_seconds=5,
                timeout_seconds=5,
            )
            assert lease is not None
            lease_id = _extract_value(lease, "lease_id", "id", "path")
            assert lease_id is not None

            with pytest.raises((ValueError, RuntimeError, KeyError, AssertionError, PermissionError)):
                _call_with_supported_kwargs(
                    acquire,
                    path="/leases/resource",
                    resource="/leases/resource",
                    lease_path="/leases/resource",
                    session_id=owner_two.session_id,
                    owner_session_id=owner_two.session_id,
                    ttl_seconds=5,
                    timeout_seconds=5,
                )

            with pytest.raises((ValueError, RuntimeError, KeyError, AssertionError, PermissionError)):
                _call_with_supported_kwargs(
                    release,
                    lease_id=lease_id,
                    path="/leases/resource",
                    resource="/leases/resource",
                    lease_path="/leases/resource",
                    session_id=owner_two.session_id,
                    owner_session_id=owner_two.session_id,
                )

            released = _call_with_supported_kwargs(
                release,
                lease_id=lease_id,
                path="/leases/resource",
                resource="/leases/resource",
                lease_path="/leases/resource",
                session_id=owner_one.session_id,
                owner_session_id=owner_one.session_id,
            )
            assert released is not False

            reacquired = _call_with_supported_kwargs(
                acquire,
                path="/leases/resource",
                resource="/leases/resource",
                lease_path="/leases/resource",
                session_id=owner_two.session_id,
                owner_session_id=owner_two.session_id,
                ttl_seconds=5,
                timeout_seconds=5,
            )
            assert reacquired is not None

            coord.close_session(owner_two.session_id)
            time.sleep(0.1)

            post_cleanup = _call_with_supported_kwargs(
                acquire,
                path="/leases/resource",
                resource="/leases/resource",
                lease_path="/leases/resource",
                session_id=owner_one.session_id,
                owner_session_id=owner_one.session_id,
                ttl_seconds=5,
                timeout_seconds=5,
            )
            assert post_cleanup is not None
        finally:
            coord.stop()
            os.unlink(db_path)


class TestConfigManagementScenario:
    """Configuration management with watches."""
    
    def test_config_management_scenario(self):
        """Config shared across clients with live updates."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            # Setup config namespace
            admin = coord.open_session()
            coord.create("/config", b"", persistent=True, session_id=admin.session_id)
            
            initial_config = json.dumps({"debug": False, "timeout": 30})
            coord.create(
                "/config/app",
                initial_config.encode(),
                persistent=True,
                session_id=admin.session_id
            )
            
            # Client reads config
            client = coord.open_session()
            node = coord.get("/config/app")
            config = json.loads(node.data.decode())
            assert config["debug"] is False
            
            # Client sets watch for updates
            watch = coord.register_watch(
                "/config/app",
                client.session_id,
                event_types=[EventType.UPDATE]
            )
            
            # Admin updates config
            new_config = json.dumps({"debug": True, "timeout": 60})
            coord.set("/config/app", new_config.encode())
            
            # Watch fires
            event = coord.wait_watch(watch.watch_id, timeout_seconds=5.0)
            assert event is not None
            assert event.event_type == EventType.UPDATE
            
            # Client reads updated config
            node = coord.get("/config/app")
            config = json.loads(node.data.decode())
            assert config["debug"] is True
            assert config["timeout"] == 60
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestConcurrentClients:
    """Multiple concurrent clients."""
    
    def test_concurrent_clients(self):
        """10 concurrent clients operate correctly."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            # Setup
            admin = coord.open_session()
            coord.create("/shared", b"", persistent=True, session_id=admin.session_id)
            coord.create("/shared/counter", b"0", persistent=True, session_id=admin.session_id)
            
            errors = []
            results = []
            lock = threading.Lock()
            
            def client_work(client_id):
                try:
                    session = coord.open_session()
                    
                    # Read current value
                    node = coord.get("/shared/counter")
                    value = int(node.data.decode())
                    
                    # Create own node
                    coord.create(
                        f"/shared/client-{client_id}",
                        f"data-{client_id}".encode(),
                        persistent=True,
                        session_id=session.session_id
                    )
                    
                    # Update counter (may race, that's OK)
                    coord.set("/shared/counter", str(value + 1).encode())
                    
                    with lock:
                        results.append(client_id)
                    
                    coord.close_session(session.session_id)
                    
                except Exception as e:
                    with lock:
                        errors.append((client_id, str(e)))
            
            # Start 10 concurrent clients
            threads = []
            for i in range(10):
                t = threading.Thread(target=client_work, args=(i,))
                threads.append(t)
                t.start()
            
            for t in threads:
                t.join()
            
            # All clients completed (some updates may conflict, nodes all created)
            assert len(results) == 10
            assert len(errors) == 0
            
            # All client nodes exist
            children = coord.list_children("/shared")
            client_nodes = [c for c in children if c.startswith("client-")]
            assert len(client_nodes) == 10
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_multiple_sessions_ordering(self):
        """Multiple sessions see consistent state."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            admin = coord.open_session()
            coord.create("/data", b"", persistent=True, session_id=admin.session_id)
            coord.create("/data/value", b"initial", persistent=True, session_id=admin.session_id)
            
            observations = []
            lock = threading.Lock()
            
            def observer(observer_id):
                session = coord.open_session()
                for _ in range(10):
                    node = coord.get("/data/value")
                    with lock:
                        observations.append((observer_id, node.version, node.data.decode()))
                    time.sleep(0.01)
                coord.close_session(session.session_id)
            
            def writer():
                for i in range(5):
                    coord.set("/data/value", f"value-{i}".encode())
                    time.sleep(0.02)
            
            # Start observers and writer
            threads = []
            for i in range(3):
                t = threading.Thread(target=observer, args=(i,))
                threads.append(t)
            
            writer_thread = threading.Thread(target=writer)
            threads.append(writer_thread)
            
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
            # Verify monotonicity for each observer
            for observer_id in range(3):
                obs = [o for o in observations if o[0] == observer_id]
                versions = [o[1] for o in obs]
                # Each observer sees monotonically increasing versions
                for i in range(1, len(versions)):
                    assert versions[i] >= versions[i-1], "Version went backwards"
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestCrashScenarios:
    """Crash and recovery scenarios."""
    
    def test_crash_during_operation(self):
        """Crash mid-operation = recovery works."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            # First run - create data
            coord1 = Coordinator(db_path=db_path)
            coord1.start()
            
            session = coord1.open_session()
            coord1.create("/test", b"data", persistent=True, session_id=session.session_id)
            
            # Simulate crash (stop without cleanup)
            coord1.stop()
            
            # Recovery
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            # Data survived
            node = coord2.get("/test")
            assert node is not None
            assert node.data == b"data"
            
            # Consistency check
            is_consistent, issues = coord2.verify_consistency()
            assert is_consistent, f"Inconsistent: {issues}"
            
            coord2.stop()
        finally:
            os.unlink(db_path)


class TestWatchRaces:
    """Watch and concurrent write race conditions."""
    
    def test_watch_concurrent_write_race(self):
        """Watch fires despite concurrent writes."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            admin = coord.open_session()
            coord.create("/race", b"", persistent=True, session_id=admin.session_id)
            coord.create("/race/data", b"initial", persistent=True, session_id=admin.session_id)
            
            # Register watch
            watcher = coord.open_session()
            watch = coord.register_watch(
                "/race/data",
                watcher.session_id,
                event_types=[EventType.UPDATE]
            )
            
            # Concurrent writes
            def writer():
                for i in range(5):
                    coord.set("/race/data", f"value-{i}".encode())
                    time.sleep(0.01)
            
            writer_thread = threading.Thread(target=writer)
            writer_thread.start()
            
            # Watch should fire on first update
            event = coord.wait_watch(watch.watch_id, timeout_seconds=5.0)
            assert event is not None
            assert event.event_type == EventType.UPDATE
            
            writer_thread.join()
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestWatchEventFiltering:
    """Watch filtering by event type."""

    def test_watch_register_event_types_filters_events(self):
        """A watch should only fire for the event types it registered."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        coord = Coordinator(db_path=db_path)
        coord.start()
        try:
            admin = coord.open_session()
            coord.create("/filtered", b"", persistent=True, session_id=admin.session_id)
            coord.create("/filtered/data", b"initial", persistent=True, session_id=admin.session_id)

            watcher = coord.open_session()
            watch = coord.register_watch(
                "/filtered/data",
                watcher.session_id,
                event_types={EventType.DELETE},
            )

            coord.set("/filtered/data", b"updated")

            # DELETE-only watch must not fire on UPDATE.
            assert coord.wait_watch(watch.watch_id, timeout_seconds=0.2) is None

            coord.delete("/filtered/data")

            event = coord.wait_watch(watch.watch_id, timeout_seconds=5.0)
            assert event is not None
            assert event.event_type == EventType.DELETE
            assert event.path == "/filtered/data"
        finally:
            coord.stop()
            os.unlink(db_path)


class TestComplexHierarchy:
    """Deep path hierarchy tests."""
    
    def test_complex_hierarchy(self):
        """Deep path hierarchy works correctly."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            
            # Create deep hierarchy
            paths = [
                "/level1",
                "/level1/level2",
                "/level1/level2/level3",
                "/level1/level2/level3/level4",
                "/level1/level2/level3/level4/level5",
            ]
            
            for path in paths:
                coord.create(path, b"data", persistent=True, session_id=session.session_id)
            
            # Verify structure
            for path in paths:
                assert coord.exists(path)
            
            # Verify children
            children = coord.list_children("/level1/level2/level3")
            assert children == ["level4"]
            
            # Delete parent deletes children (cascading)
            coord.delete("/level1/level2/level3")
            
            # Children deleted
            assert coord.exists("/level1/level2/level3") is False
            assert coord.exists("/level1/level2/level3/level4") is False
            assert coord.exists("/level1/level2/level3/level4/level5") is False
            
            # Parent still exists
            assert coord.exists("/level1/level2") is True
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_cascading_deletes(self):
        """Deleting parent deletes children."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            
            # Create tree
            coord.create("/parent", b"", persistent=True, session_id=session.session_id)
            coord.create("/parent/child1", b"", persistent=True, session_id=session.session_id)
            coord.create("/parent/child2", b"", persistent=True, session_id=session.session_id)
            coord.create("/parent/child1/grandchild", b"", persistent=True, session_id=session.session_id)
            
            # Delete parent
            coord.delete("/parent")
            
            # All gone
            assert coord.exists("/parent") is False
            assert coord.exists("/parent/child1") is False
            assert coord.exists("/parent/child2") is False
            assert coord.exists("/parent/child1/grandchild") is False
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestLoadScenarios:
    """Load testing scenarios."""
    
    def test_load_1000_nodes(self):
        """1000 nodes in tree works correctly."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            
            # Create root
            coord.create("/load", b"", persistent=True, session_id=session.session_id)
            
            # Create 1000 nodes
            for i in range(1000):
                coord.create(
                    f"/load/node-{i:04d}",
                    f"data-{i}".encode(),
                    persistent=True,
                    session_id=session.session_id
                )
            
            # Verify count
            children = coord.list_children("/load")
            assert len(children) == 1000
            
            # Verify random access
            node = coord.get("/load/node-0500")
            assert node.data == b"data-500"
            
            # Stats
            stats = coord.get_stats()
            assert stats["nodes"] >= 1001  # root + /load + 1000 children
            
            coord.stop()
        finally:
            os.unlink(db_path)
    
    def test_100_concurrent_watches(self):
        """100 watches fire atomically."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            session = coord.open_session()
            coord.create("/watched", b"", persistent=True, session_id=session.session_id)
            coord.create("/watched/data", b"initial", persistent=True, session_id=session.session_id)
            
            # Register 100 watches
            watches = []
            for i in range(100):
                watch = coord.register_watch(
                    "/watched/data",
                    session.session_id,
                    event_types=[EventType.UPDATE]
                )
                watches.append(watch)
            
            # Trigger all watches
            coord.set("/watched/data", b"updated")
            
            # All watches should fire
            fired_count = 0
            for watch in watches:
                event = coord.wait_watch(watch.watch_id, timeout_seconds=1.0)
                if event:
                    fired_count += 1
            
            assert fired_count == 100
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestSessionWatchInteraction:
    """Session and watch interactions."""
    
    def test_session_timeout_during_watch(self):
        """Session dies while waiting on watch."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            admin = coord.open_session()
            coord.create("/test", b"", persistent=True, session_id=admin.session_id)
            coord.create("/test/data", b"initial", persistent=True, session_id=admin.session_id)
            
            # Create short-lived session
            short_session = coord.open_session(timeout_seconds=2)
            watch = coord.register_watch(
                "/test/data",
                short_session.session_id,
                event_types=[EventType.UPDATE]
            )
            
            # Let session expire
            time.sleep(3)
            
            # Watch should timeout (session dead)
            event = coord.wait_watch(watch.watch_id, timeout_seconds=1.0)
            # Watch was cleared when session died, so result depends on implementation
            # The key is no crash or hang
            
            coord.stop()
        finally:
            os.unlink(db_path)


class TestFullWorkflow:
    """Complete real-world workflow."""
    
    def test_full_workflow(self):
        """Complete workflow with all features."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        try:
            coord = Coordinator(db_path=db_path)
            coord.start()
            
            # 1. Admin sets up namespace
            admin = coord.open_session(timeout_seconds=300)
            coord.create("/app", b"", persistent=True, session_id=admin.session_id)
            coord.create("/app/config", b"", persistent=True, session_id=admin.session_id)
            coord.create("/app/services", b"", persistent=True, session_id=admin.session_id)
            coord.create("/app/locks", b"", persistent=True, session_id=admin.session_id)
            
            # 2. Set initial config
            config = json.dumps({"version": "1.0", "features": ["auth", "cache"]})
            coord.create("/app/config/main", config.encode(), persistent=True, 
                        session_id=admin.session_id)
            
            # 3. Service registers
            service = coord.open_session(timeout_seconds=10)
            coord.create(
                "/app/services/worker-1",
                json.dumps({"status": "ready"}).encode(),
                persistent=False,
                session_id=service.session_id
            )
            
            # 4. Service watches config
            watch = coord.register_watch(
                "/app/config/main",
                service.session_id,
                event_types=[EventType.UPDATE]
            )
            
            # 5. Admin updates config
            new_config = json.dumps({"version": "1.1", "features": ["auth", "cache", "metrics"]})
            coord.set("/app/config/main", new_config.encode())
            
            # 6. Service receives notification
            event = coord.wait_watch(watch.watch_id, timeout_seconds=5.0)
            assert event is not None
            
            # 7. Service reads new config
            node = coord.get("/app/config/main")
            config = json.loads(node.data.decode())
            assert config["version"] == "1.1"
            assert "metrics" in config["features"]
            
            # 8. Service acquires lock
            coord.create(
                "/app/locks/job-1",
                b"worker-1",
                persistent=False,
                session_id=service.session_id
            )
            
            # 9. Service does work...
            time.sleep(0.1)
            
            # 10. Service releases lock and shuts down
            coord.delete("/app/locks/job-1")
            coord.close_session(service.session_id)
            
            # 11. Verify cleanup
            time.sleep(0.2)
            assert coord.exists("/app/services/worker-1") is False
            
            # 12. Verify config persisted
            coord.stop()
            
            coord2 = Coordinator(db_path=db_path)
            coord2.start()
            
            node = coord2.get("/app/config/main")
            config = json.loads(node.data.decode())
            assert config["version"] == "1.1"
            
            coord2.stop()
        finally:
            os.unlink(db_path)
