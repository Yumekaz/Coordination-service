"""
Tests for HTTP API Endpoints.

Tests all HTTP endpoints for correct behavior:
- Session endpoints
- Node endpoints
- Watch endpoints
- Health endpoint
"""

import pytest
import tempfile
import os
import sys
import inspect
import json
import threading
import time
from fastapi.testclient import TestClient

# Setup test database path before importing main
_test_db = tempfile.mktemp(suffix=".db")
os.environ["COORD_DB_PATH"] = _test_db

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from coordinator import Coordinator
import main


def _read_sse_event(response) -> tuple[str, dict]:
    """Read one SSE event from a streaming response."""
    event_name = "message"
    data_lines = []

    for raw_line in response.iter_lines():
        line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
        if line == "":
            if data_lines:
                return event_name, json.loads("\n".join(data_lines))
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event_name = line.split(":", 1)[1].strip()
        elif line.startswith("data:"):
            data_lines.append(line.split(":", 1)[1].lstrip())

    raise AssertionError("No SSE event received")


def _lease_routes_present() -> bool:
    """Detect whether lease endpoints have landed yet."""
    routes = {getattr(route, "path", "") for route in main.app.routes}
    return {
        "/api/lease/acquire",
        "/api/lease/get",
        "/api/lease/release",
    }.issubset(routes)


def _model_has_field(model_cls, field_name: str) -> bool:
    """Compatibility helper for Pydantic v1/v2 field inspection."""
    model_fields = getattr(model_cls, "model_fields", None)
    if model_fields is None:
        model_fields = getattr(model_cls, "__fields__", {})
    return field_name in model_fields


def _model_has_field(model, field_name: str) -> bool:
    """Detect whether a pydantic model exposes a field."""
    fields = getattr(model, "model_fields", None)
    if fields is not None:
        return field_name in fields

    fields = getattr(model, "__fields__", None)
    if fields is not None:
        return field_name in fields

    return False


@pytest.fixture
def client():
    """Create test client with fresh coordinator."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    # Create fresh coordinator
    coord = Coordinator(db_path=db_path)
    coord.start()
    
    # Inject into main module BEFORE creating TestClient
    # This prevents the lifespan from creating its own coordinator
    old_coordinator = main.coordinator
    main.coordinator = coord
    
    try:
        with TestClient(main.app) as test_client:
            yield test_client
    finally:
        # Cleanup
        main.coordinator = old_coordinator
        coord.stop()
        
        try:
            os.unlink(db_path)
        except:
            pass


class TestSessionEndpoints:
    """Tests for session management endpoints."""
    
    def test_open_session(self, client):
        """POST /api/session/open should create session."""
        response = client.post("/api/session/open", json={"timeout_seconds": 30})
        
        assert response.status_code == 200
        data = response.json()
        assert "session_id" in data
        assert data["status"] == "created"
    
    def test_open_session_default_timeout(self, client):
        """Should use default timeout if not specified."""
        response = client.post("/api/session/open", json={})
        
        assert response.status_code == 200
        data = response.json()
        assert "session_id" in data
    
    def test_heartbeat(self, client):
        """POST /api/session/heartbeat should refresh session."""
        # Create session
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Heartbeat
        response = client.post("/api/session/heartbeat", json={"session_id": session_id})
        
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
    
    def test_heartbeat_invalid_session(self, client):
        """Heartbeat for invalid session should return 404."""
        response = client.post("/api/session/heartbeat", json={"session_id": "invalid"})
        
        assert response.status_code == 404
    
    def test_close_session(self, client):
        """POST /api/session/close should terminate session."""
        # Create session
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Close
        response = client.post("/api/session/close", json={"session_id": session_id})
        
        assert response.status_code == 200
        assert response.json()["status"] == "closed"
        
        # Heartbeat should now fail
        response = client.post("/api/session/heartbeat", json={"session_id": session_id})
        assert response.status_code in [400, 404]


class TestNodeEndpoints:
    """Tests for node operation endpoints."""
    
    def test_create_node(self, client):
        """POST /api/node/create should create node."""
        # Create session first
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Create node
        response = client.post("/api/node/create", json={
            "path": "/test",
            "data": "hello",
            "persistent": True,
            "session_id": session_id
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["path"] == "/test"
        assert data["status"] == "created"
        assert data["version"] == 1
    
    def test_create_ephemeral_node(self, client):
        """Should create ephemeral node."""
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        response = client.post("/api/node/create", json={
            "path": "/ephemeral",
            "data": "temp",
            "persistent": False,
            "session_id": session_id
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["path"] == "/ephemeral"
    
    def test_create_hierarchical_nodes(self, client):
        """Should create hierarchical nodes with parents first."""
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Create parent
        response = client.post("/api/node/create", json={
            "path": "/parent",
            "data": "parent_data",
            "persistent": True,
            "session_id": session_id
        })
        assert response.status_code == 200
        
        # Create child
        response = client.post("/api/node/create", json={
            "path": "/parent/child",
            "data": "child_data",
            "persistent": True,
            "session_id": session_id
        })
        assert response.status_code == 200
    
    def test_create_without_parent_fails(self, client):
        """Creating node without parent should fail."""
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        response = client.post("/api/node/create", json={
            "path": "/nonexistent/child",
            "data": "data",
            "persistent": True,
            "session_id": session_id
        })
        
        assert response.status_code in [400, 404]
    
    def test_get_node(self, client):
        """GET /api/node/get should return node data."""
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Create node
        client.post("/api/node/create", json={
            "path": "/gettest",
            "data": "mydata",
            "persistent": True,
            "session_id": session_id
        })
        
        # Get node
        response = client.get("/api/node/get", params={"path": "/gettest"})
        
        assert response.status_code == 200
        data = response.json()
        assert data["path"] == "/gettest"
        assert data["data"] == "mydata"
        assert data["version"] >= 1
    
    def test_get_nonexistent_node(self, client):
        """Getting nonexistent node should return 404."""
        response = client.get("/api/node/get", params={"path": "/nonexistent"})
        
        assert response.status_code == 404
    
    def test_set_node(self, client):
        """POST /api/node/set should update node data."""
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Create node
        client.post("/api/node/create", json={
            "path": "/settest",
            "data": "original",
            "persistent": True,
            "session_id": session_id
        })
        
        # Update node
        response = client.post("/api/node/set", json={
            "path": "/settest",
            "data": "updated"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["version"] == 2  # Version incremented
        
        # Verify update
        response = client.get("/api/node/get", params={"path": "/settest"})
        assert response.json()["data"] == "updated"
    
    def test_delete_node(self, client):
        """DELETE /api/node/delete should remove node."""
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Create node
        client.post("/api/node/create", json={
            "path": "/deletetest",
            "data": "tobedeleted",
            "persistent": True,
            "session_id": session_id
        })
        
        # Delete node
        response = client.delete("/api/node/delete", params={"path": "/deletetest"})
        
        assert response.status_code == 200
        
        # Verify deletion
        response = client.get("/api/node/get", params={"path": "/deletetest"})
        assert response.status_code == 404
    
    def test_exists_node(self, client):
        """GET /api/node/exists should check node existence."""
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Create node
        client.post("/api/node/create", json={
            "path": "/existstest",
            "data": "data",
            "persistent": True,
            "session_id": session_id
        })
        
        # Check exists
        response = client.get("/api/node/exists", params={"path": "/existstest"})
        assert response.status_code == 200
        assert response.json()["exists"] == True
        
        # Check nonexistent
        response = client.get("/api/node/exists", params={"path": "/nonexistent"})
        assert response.status_code == 200
        assert response.json()["exists"] == False
    
    def test_list_children(self, client):
        """GET /api/node/list_children should return child nodes."""
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Create parent and children
        client.post("/api/node/create", json={
            "path": "/parent",
            "data": "parent",
            "persistent": True,
            "session_id": session_id
        })
        
        client.post("/api/node/create", json={
            "path": "/parent/child1",
            "data": "c1",
            "persistent": True,
            "session_id": session_id
        })
        
        client.post("/api/node/create", json={
            "path": "/parent/child2",
            "data": "c2",
            "persistent": True,
            "session_id": session_id
        })
        
        # List children
        response = client.get("/api/node/list_children", params={"path": "/parent"})
        
        assert response.status_code == 200
        data = response.json()
        assert set(data["children"]) == {"child1", "child2"}


class TestWatchEndpoints:
    """Tests for watch operation endpoints."""
    
    def test_register_watch(self, client):
        """POST /api/watch/register should register watch."""
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Create node
        client.post("/api/node/create", json={
            "path": "/watchnode",
            "data": "data",
            "persistent": True,
            "session_id": session_id
        })
        
        # Register watch
        response = client.post("/api/watch/register", json={
            "path": "/watchnode",
            "session_id": session_id,
            "event_types": ["UPDATE", "DELETE"]
        })
        
        assert response.status_code == 200
        data = response.json()
        assert "watch_id" in data
        assert data["status"] == "registered"
    
    def test_unregister_watch(self, client):
        """DELETE /api/watch/unregister should remove watch."""
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Create node
        client.post("/api/node/create", json={
            "path": "/watchnode2",
            "data": "data",
            "persistent": True,
            "session_id": session_id
        })
        
        # Register watch
        response = client.post("/api/watch/register", json={
            "path": "/watchnode2",
            "session_id": session_id,
            "event_types": ["UPDATE"]
        })
        watch_id = response.json()["watch_id"]
        
        # Unregister watch
        response = client.delete("/api/watch/unregister", params={"watch_id": watch_id})
        
        assert response.status_code == 200

    def test_register_watch_event_types_filter_updates_through_http(self, client):
        """HTTP watch registration should honor event type filters."""
        if not _model_has_field(main.RegisterWatchRequest, "event_types"):
            pytest.skip("RegisterWatchRequest does not yet expose event_types")

        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]

        client.post("/api/node/create", json={
            "path": "/filtered-watch",
            "data": "payload",
            "persistent": True,
            "session_id": session_id
        })

        update_watch = client.post("/api/watch/register", json={
            "path": "/filtered-watch",
            "session_id": session_id,
            "event_types": ["UPDATE"]
        })
        delete_watch = client.post("/api/watch/register", json={
            "path": "/filtered-watch",
            "session_id": session_id,
            "event_types": ["DELETE"]
        })

        assert update_watch.status_code == 200
        assert delete_watch.status_code == 200

        response = client.delete("/api/node/delete", params={"path": "/filtered-watch"})
        assert response.status_code == 200

        update_wait = client.get("/api/watch/wait", params={
            "watch_id": update_watch.json()["watch_id"],
            "timeout_seconds": 1,
        })
        delete_wait = client.get("/api/watch/wait", params={
            "watch_id": delete_watch.json()["watch_id"],
            "timeout_seconds": 1,
        })

        assert update_wait.status_code == 200
        assert update_wait.json()["status"] == "timeout"
        assert delete_wait.status_code == 200
        assert delete_wait.json()["event_type"] == "DELETE"


class TestLeaseEndpoints:
    """Tests for lease APIs, enabled only if the routes exist."""

    def test_lease_acquire_get_release_round_trip(self, client):
        """Lease APIs should create, fetch, and release a lease."""
        if not _lease_routes_present():
            pytest.skip("Lease endpoints are not implemented yet")

        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]

        acquire = client.post("/api/lease/acquire", json={
            "path": "/leases/resource",
            "session_id": session_id,
            "data": "holder-a"
        })
        assert acquire.status_code == 200
        lease_path = acquire.json()["path"]

        current = client.get("/api/lease/get", params={"path": lease_path})
        assert current.status_code == 200
        assert current.json()["path"] == lease_path
        assert current.json()["session_id"] == session_id

        release = client.post("/api/lease/release", json={
            "path": lease_path,
            "session_id": session_id
        })
        assert release.status_code == 200

        after_release = client.get("/api/lease/get", params={"path": lease_path})
        assert after_release.status_code in [404, 410]

    def test_lease_conflict_rejected(self, client):
        """Competing lease claims should be rejected."""
        if not _lease_routes_present():
            pytest.skip("Lease endpoints are not implemented yet")

        session_one = client.post("/api/session/open", json={}).json()["session_id"]
        session_two = client.post("/api/session/open", json={}).json()["session_id"]

        first = client.post("/api/lease/acquire", json={
            "path": "/leases/conflict",
            "session_id": session_one,
            "data": "first"
        })
        assert first.status_code == 200

        second = client.post("/api/lease/acquire", json={
            "path": "/leases/conflict",
            "session_id": session_two,
            "data": "second"
        })
        assert second.status_code == 409

    def test_lease_cleared_on_session_close(self, client):
        """A lease should disappear when the owning session closes."""
        if not _lease_routes_present():
            pytest.skip("Lease endpoints are not implemented yet")

        session_id = client.post("/api/session/open", json={}).json()["session_id"]

        acquired = client.post("/api/lease/acquire", json={
            "path": "/leases/session-cleanup",
            "session_id": session_id,
            "data": "cleanup"
        })
        assert acquired.status_code == 200
        lease_path = acquired.json()["path"]

        closed = client.post("/api/session/close", json={"session_id": session_id})
        assert closed.status_code == 200

        after_close = client.get("/api/lease/get", params={"path": lease_path})
        assert after_close.status_code in [404, 410]


class TestHealthEndpoint:
    """Tests for health and stats endpoints."""
    
    def test_health(self, client):
        """GET /api/health should return health status."""
        response = client.get("/api/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
    
    def test_stats(self, client):
        """GET /api/stats should return statistics."""
        response = client.get("/api/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert "nodes" in data
        assert "sessions_total" in data
        assert "watches" in data
    
    def test_verify(self, client):
        """GET /api/verify should verify consistency."""
        response = client.get("/api/verify")
        
        assert response.status_code == 200
        data = response.json()
        assert "consistent" in data


class TestOperationTimelineEndpoints:
    """Tests for committed operation timeline APIs."""

    def test_list_operations_supports_filters(self, client):
        """The operations endpoint should filter by type and path."""
        session_id = client.post("/api/session/open", json={}).json()["session_id"]

        client.post("/api/node/create", json={
            "path": "/ops-filter",
            "data": "v1",
            "persistent": True,
            "session_id": session_id,
        })
        client.post("/api/node/set", json={
            "path": "/ops-filter",
            "data": "v2",
        })

        response = client.get("/api/operations", params=[
            ("path_prefix", "/ops-filter"),
            ("operation_types", "CREATE"),
        ])

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["operations"][0]["operation_type"] == "CREATE"
        assert data["operations"][0]["path"] == "/ops-filter"
        assert data["operations"][0]["summary"].startswith("CREATE")
        assert data["next_since"] == data["last_sequence"]

    def test_tail_operations_waits_for_next_commit(self, client):
        """The tail endpoint should block until a matching commit appears."""
        session_id = client.post("/api/session/open", json={}).json()["session_id"]
        baseline = client.get("/api/stats").json()["last_sequence"]

        def writer():
            time.sleep(0.2)
            main.coordinator.create(
                "/ops-tail",
                b"payload",
                persistent=True,
                session_id=session_id,
            )

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            response = client.get("/api/operations/tail", params={
                "since_sequence": baseline,
                "timeout_seconds": 2,
                "path_prefix": "/ops-tail",
            })
        finally:
            thread.join()

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["count"] >= 1
        assert data["operations"][0]["path"] == "/ops-tail"
        assert data["operations"][0]["summary"].startswith("CREATE")

    def test_tail_operations_times_out_cleanly(self, client):
        """The tail endpoint should report timeouts without inventing data."""
        baseline = client.get("/api/stats").json()["last_sequence"]

        response = client.get("/api/operations/tail", params={
            "since_sequence": baseline,
            "timeout_seconds": 0.1,
            "path_prefix": "/ops-never",
        })

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "timeout"
        assert data["count"] == 0
        assert data["operations"] == []

    def test_operation_detail_and_recovery_endpoints(self, client):
        """Detail and recovery endpoints should expose committed state."""
        session_id = client.post("/api/session/open", json={}).json()["session_id"]
        client.post("/api/node/create", json={
            "path": "/ops-detail",
            "data": "payload",
            "persistent": True,
            "session_id": session_id,
        })

        operations = client.get("/api/operations", params={"path_prefix": "/ops-detail"}).json()
        sequence_number = operations["operations"][0]["sequence_number"]

        detail = client.get(f"/api/operations/{sequence_number}")
        assert detail.status_code == 200
        assert detail.json()["path"] == "/ops-detail"
        assert detail.json()["data_preview"] == "payload"

        recovery = client.get("/api/recovery/last")
        assert recovery.status_code == 200
        assert "wal_entries_read" in recovery.json()


class TestSessionInventoryEndpoints:
    """Tests for server-sourced session inventory."""

    def test_list_sessions_exposes_live_state_counts(self, client):
        """Session inventory should include TTL, watch count, and ephemeral nodes."""
        session_id = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]

        watch = client.post("/api/watch/register", json={
            "path": "/inventory-watch",
            "session_id": session_id,
        })
        assert watch.status_code == 200

        node = client.post("/api/node/create", json={
            "path": "/inventory-ephemeral",
            "data": "payload",
            "persistent": False,
            "session_id": session_id,
        })
        assert node.status_code == 200

        response = client.get("/api/sessions", params={"alive_only": True})
        assert response.status_code == 200

        data = response.json()
        entry = next(item for item in data["sessions"] if item["session_id"] == session_id)

        assert data["count"] >= 1
        assert entry["is_alive"] is True
        assert entry["watch_count"] == 1
        assert entry["ephemeral_node_count"] == 1
        assert "/inventory-ephemeral" in entry["ephemeral_nodes"]
        assert entry["remaining_seconds"] > 0
        assert entry["expires_at"] >= entry["last_heartbeat"]

    def test_list_sessions_can_include_closed_sessions(self, client):
        """Closed sessions should remain visible in the full inventory but not alive_only."""
        session_id = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]

        close_response = client.post("/api/session/close", json={"session_id": session_id})
        assert close_response.status_code == 200

        alive_response = client.get("/api/sessions", params={"alive_only": True})
        assert alive_response.status_code == 200
        assert session_id not in {item["session_id"] for item in alive_response.json()["sessions"]}

        all_response = client.get("/api/sessions")
        assert all_response.status_code == 200
        entry = next(item for item in all_response.json()["sessions"] if item["session_id"] == session_id)

        assert entry["is_alive"] is False
        assert entry["remaining_seconds"] == 0
        assert entry["watch_count"] == 0


class TestInspectorEndpoints:
    """Tests for session and lease drill-down endpoints."""

    def test_session_detail_includes_owned_nodes_watches_and_recent_ops(self, client):
        """Session detail should expose the owned nodes, watches, and recent operations."""
        session_id = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        client.post("/api/watch/register", json={
            "path": "/inspect-session",
            "session_id": session_id,
        })
        client.post("/api/node/create", json={
            "path": "/inspect-session",
            "data": "payload",
            "persistent": False,
            "session_id": session_id,
        })

        response = client.get("/api/session/detail", params={"session_id": session_id})
        assert response.status_code == 200

        data = response.json()
        assert data["session_id"] == session_id
        assert data["watch_count"] == 1
        assert any(node["path"] == "/inspect-session" for node in data["owned_nodes"])
        assert any(watch["path"] == "/inspect-session" for watch in data["watches"])
        assert any(op["operation_type"] == "SESSION_OPEN" for op in data["recent_operations"])
        assert any(op["path"] == "/inspect-session" for op in data["recent_operations"])

    def test_session_detail_includes_recent_watch_fires(self, client):
        """Session detail should expose watch firings observed by that session."""
        owner_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        watcher_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]

        client.post("/api/node/create", json={
            "path": "/inspect-fired-watch",
            "data": "v1",
            "persistent": True,
            "session_id": owner_session,
        })
        client.post("/api/watch/register", json={
            "path": "/inspect-fired-watch",
            "session_id": watcher_session,
            "event_types": ["UPDATE"],
        })
        client.post("/api/node/set", json={
            "path": "/inspect-fired-watch",
            "data": "v2",
        })

        response = client.get("/api/session/detail", params={"session_id": watcher_session})
        assert response.status_code == 200

        data = response.json()
        assert any(record["observed_path"] == "/inspect-fired-watch" for record in data["recent_watch_fires"])
        assert any(record["watch_session_id"] == watcher_session for record in data["recent_watch_fires"])
        assert any(record["event_type"] == "UPDATE" for record in data["recent_watch_fires"])

    def test_path_detail_exposes_current_state_for_persistent_node(self, client):
        """Path detail should describe current state for ordinary persistent nodes too."""
        session_id = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        client.post("/api/node/create", json={
            "path": "/inspect-path",
            "data": "payload",
            "persistent": True,
            "session_id": session_id,
        })

        response = client.get("/api/path/detail", params={"path": "/inspect-path"})
        assert response.status_code == 200

        data = response.json()
        assert data["path"] == "/inspect-path"
        assert data["current_node"]["path"] == "/inspect-path"
        assert data["current_node"]["node_type"] == "PERSISTENT"
        assert data["disappearance"] is None
        assert any(op["path"] == "/inspect-path" for op in data["recent_operations"])

    def test_path_detail_includes_disappearance_story_and_fired_watches(self, client):
        """Deleted lease-like paths should keep the causal chain and triggered watches."""
        owner_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        watcher_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]

        acquired = client.post("/api/lease/acquire", json={
            "path": "/inspect-postmortem",
            "session_id": owner_session,
            "holder": "alpha",
        })
        assert acquired.status_code == 200

        watch = client.post("/api/watch/register", json={
            "path": "/inspect-postmortem",
            "session_id": watcher_session,
            "event_types": ["DELETE"],
        })
        assert watch.status_code == 200

        released = client.post("/api/lease/release", json={
            "path": "/inspect-postmortem",
            "session_id": owner_session,
        })
        assert released.status_code == 200

        response = client.get("/api/path/detail", params={"path": "/inspect-postmortem"})
        assert response.status_code == 200

        data = response.json()
        assert data["current_node"] is None
        assert data["last_known_node"]["path"] == "/inspect-postmortem"
        assert data["disappearance"]["state"] == "deleted"
        assert data["disappearance"]["cause_kind"] == "owner_delete"
        assert data["disappearance"]["delete_operation"]["operation_type"] == "DELETE"
        assert any(record["observed_path"] == "/inspect-postmortem" for record in data["fired_watches"])
        assert any(record["watch_session_id"] == watcher_session for record in data["fired_watches"])

    def test_lease_detail_includes_waiters_and_history(self, client):
        """Lease detail should expose current holder, live waiters, and holder history."""
        holder_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        waiter_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]

        lease_response = client.post("/api/lease/acquire", json={
            "path": "/locks/demo",
            "session_id": holder_session,
            "holder": "alpha",
            "metadata": {"role": "leader"},
        })
        assert lease_response.status_code == 200

        result = {}

        def waiter():
            try:
                main.coordinator.acquire_lease(
                    "/locks/demo",
                    waiter_session,
                    holder="beta",
                    metadata={"role": "candidate"},
                    wait_timeout_seconds=0.5,
                )
                result["status"] = "acquired"
            except Exception as exc:
                result["status"] = type(exc).__name__

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.15)
        try:
            response = client.get("/api/lease/detail", params={"path": "/locks/demo"})
        finally:
            thread.join()

        assert response.status_code == 200
        data = response.json()
        assert data["path"] == "/locks/demo"
        assert data["current_lease"]["holder"] == "alpha"
        assert data["current_lease"]["session_id"] == holder_session
        assert data["holder_session"]["session_id"] == holder_session
        assert data["waiter_count"] >= 1
        assert waiter_session in data["waiters"]
        assert any(entry["holder"] == "alpha" for entry in data["holder_history"])
        assert any(op["path"] == "/locks/demo" for op in data["recent_operations"])
        assert result["status"] in {"ConflictError", "acquired"}

    def test_lease_detail_preserves_history_after_release(self, client):
        """Lease detail should stay inspectable after the active lease is gone."""
        session_id = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]

        acquired = client.post("/api/lease/acquire", json={
            "path": "/locks/history",
            "session_id": session_id,
            "holder": "historian",
            "metadata": {"role": "primary"},
        })
        assert acquired.status_code == 200

        released = client.post("/api/lease/release", json={
            "path": "/locks/history",
            "session_id": session_id,
        })
        assert released.status_code == 200

        response = client.get("/api/lease/detail", params={"path": "/locks/history"})
        assert response.status_code == 200

        data = response.json()
        assert data["path"] == "/locks/history"
        assert data["current_lease"] is None
        assert data["holder_session"] is None
        assert any(entry["holder"] == "historian" for entry in data["holder_history"])
        assert any(op["operation_type"] == "CREATE" for op in data["recent_operations"])
        assert any(op["operation_type"] == "DELETE" for op in data["recent_operations"])

    def test_path_detail_shows_live_persistent_state(self, client):
        """Path detail should inspect ordinary live nodes, not just leases."""
        session_id = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        parent = client.post("/api/node/create", json={
            "path": "/inspect",
            "data": "",
            "persistent": True,
            "session_id": session_id,
        })
        assert parent.status_code == 200
        created = client.post("/api/node/create", json={
            "path": "/inspect/live-node",
            "data": "payload",
            "persistent": True,
            "session_id": session_id,
        })
        assert created.status_code == 200

        watch = client.post("/api/watch/register", json={
            "path": "/inspect/live-node",
            "session_id": session_id,
            "event_types": ["UPDATE"],
        })
        assert watch.status_code == 200

        response = client.get("/api/path/detail", params={"path": "/inspect/live-node"})
        assert response.status_code == 200

        data = response.json()
        assert data["path"] == "/inspect/live-node"
        assert data["current_node"]["node_type"] == "PERSISTENT"
        assert data["current_node"]["version"] == 1
        assert data["disappearance"] is None
        assert any(watch["path"] == "/inspect/live-node" for watch in data["active_watches"])
        assert any(op["operation_type"] == "CREATE" for op in data["recent_operations"])

    def test_path_detail_explains_session_cleanup_and_fired_watches(self, client):
        """Path detail should explain cleanup cause and which watches fired."""
        owner_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        direct_watch_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        parent_watch_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]

        root = client.post("/api/node/create", json={
            "path": "/trace",
            "data": "",
            "persistent": True,
            "session_id": owner_session,
        })
        assert root.status_code == 200

        acquired = client.post("/api/lease/acquire", json={
            "path": "/trace/lease",
            "session_id": owner_session,
            "holder": "alpha",
            "metadata": {"role": "leader"},
        })
        assert acquired.status_code == 200

        direct_watch = client.post("/api/watch/register", json={
            "path": "/trace/lease",
            "session_id": direct_watch_session,
            "event_types": ["DELETE"],
        })
        assert direct_watch.status_code == 200

        parent_watch = client.post("/api/watch/register", json={
            "path": "/trace",
            "session_id": parent_watch_session,
            "event_types": ["CHILDREN"],
        })
        assert parent_watch.status_code == 200

        closed = client.post("/api/session/close", json={"session_id": owner_session})
        assert closed.status_code == 200

        response = client.get("/api/path/detail", params={"path": "/trace/lease"})
        assert response.status_code == 200

        data = response.json()
        assert data["current_node"] is None
        assert data["last_known_node"]["node_type"] == "EPHEMERAL"
        assert data["last_known_node"]["holder"] == "alpha"
        assert data["owner_session"]["session_id"] == owner_session
        assert data["disappearance"]["cause_kind"] == "session_closed_cleanup"
        assert data["disappearance"]["reason"] == "explicit"
        assert data["disappearance"]["delete_operation"]["operation_type"] == "DELETE"
        assert data["disappearance"]["cause_operation"]["operation_type"] == "SESSION_CLOSE"
        assert data["disappearance"]["cause_session_id"] == owner_session
        assert data["fired_watches"][0]["ordinal"] < data["fired_watches"][1]["ordinal"]
        assert {entry["watch_session_id"] for entry in data["fired_watches"]} == {direct_watch_session, parent_watch_session}
        assert {entry["event_type"] for entry in data["fired_watches"]} == {"DELETE", "CHILDREN"}
        assert any(op["operation_type"] == "SESSION_CLOSE" for op in data["recent_operations"])

    def test_operation_detail_alias_exposes_session_cleanup_blast_radius(self, client):
        """The visualizer alias should expose a full cleanup incident for session close."""
        owner_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        direct_watch_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        parent_watch_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]

        client.post("/api/node/create", json={
            "path": "/incident",
            "data": "",
            "persistent": True,
            "session_id": owner_session,
        })
        client.post("/api/lease/acquire", json={
            "path": "/incident/lease",
            "session_id": owner_session,
            "holder": "alpha",
            "metadata": {"role": "leader"},
        })
        client.post("/api/watch/register", json={
            "path": "/incident/lease",
            "session_id": direct_watch_session,
            "event_types": ["DELETE"],
        })
        client.post("/api/watch/register", json={
            "path": "/incident",
            "session_id": parent_watch_session,
            "event_types": ["CHILDREN"],
        })

        closed = client.post("/api/session/close", json={"session_id": owner_session})
        assert closed.status_code == 200

        operations = client.get("/api/operations", params={"since_sequence": 0, "limit": 50}).json()["operations"]
        session_close = next(
            op for op in operations
            if op["operation_type"] == "SESSION_CLOSE" and op["session_id"] == owner_session
        )

        response = client.get("/api/operation/detail", params={"sequence_number": session_close["sequence_number"]})
        assert response.status_code == 200

        data = response.json()
        assert data["sequence_number"] == session_close["sequence_number"]
        assert data["source_session"]["session_id"] == owner_session
        assert data["affected_path_count"] == 1
        assert data["primary_path"] == "/incident/lease"
        assert data["blast_radius"]["watches_fired"] == 2
        assert data["related_operations"][0]["operation_type"] == "DELETE"
        assert [op["operation_type"] for op in data["causal_chain"]] == ["SESSION_CLOSE", "DELETE"]
        assert data["affected_paths"][0]["path"] == "/incident/lease"
        assert data["affected_paths"][0]["change_kind"] == "deleted"
        assert data["affected_paths"][0]["before"]["node_type"] == "EPHEMERAL"
        assert data["affected_paths"][0]["after"] is None
        assert set(record["event_type"] for record in data["watch_fires"]) == {"DELETE", "CHILDREN"}
        assert any("reason" in note.lower() for note in data["notes"])

    def test_operation_incident_for_update_includes_before_and_after_snapshots(self, client):
        """Operation incident reports should show a real before/after delta for a write."""
        session_id = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        client.post("/api/node/create", json={
            "path": "/op-set",
            "data": "v1",
            "persistent": True,
            "session_id": session_id,
        })
        updated = client.post("/api/node/set", json={
            "path": "/op-set",
            "data": "v2",
        })
        assert updated.status_code == 200

        operations = client.get("/api/operations", params={"since_sequence": 0, "limit": 20}).json()["operations"]
        set_operation = next(
            op for op in operations
            if op["operation_type"] == "SET" and op["path"] == "/op-set"
        )

        response = client.get(f"/api/operations/{set_operation['sequence_number']}/incident")
        assert response.status_code == 200

        data = response.json()
        assert data["incident_kind"] == "mutation"
        assert data["primary_path"] == "/op-set"
        assert data["affected_path_count"] == 1
        assert data["affected_paths"][0]["path"] == "/op-set"
        assert data["affected_paths"][0]["version_before"] == 1
        assert data["affected_paths"][0]["version_after"] == 2
        assert data["affected_paths"][0]["before"]["data_preview"] == "v1"
        assert data["affected_paths"][0]["after"]["data_preview"] == "v2"
        assert data["blast_radius"]["affected_paths"] == 1
        assert data["watch_fires"] == []

    def test_operation_incident_reports_recursive_delete_blast_radius(self, client):
        """Operation incident detail should summarize recursive delete impact across paths and watches."""
        owner_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        direct_watch_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        parent_watch_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]

        client.post("/api/node/create", json={
            "path": "/incident",
            "data": "",
            "persistent": True,
            "session_id": owner_session,
        })
        client.post("/api/node/create", json={
            "path": "/incident/a",
            "data": "one",
            "persistent": True,
            "session_id": owner_session,
        })
        client.post("/api/node/create", json={
            "path": "/incident/b",
            "data": "two",
            "persistent": True,
            "session_id": owner_session,
        })

        client.post("/api/watch/register", json={
            "path": "/incident/b",
            "session_id": direct_watch_session,
            "event_types": ["DELETE"],
        })
        client.post("/api/watch/register", json={
            "path": "/incident",
            "session_id": parent_watch_session,
            "event_types": ["CHILDREN"],
        })

        deleted = client.delete("/api/node/delete", params={"path": "/incident"})
        assert deleted.status_code == 200
        assert set(deleted.json()["deleted_paths"]) == {"/incident", "/incident/a", "/incident/b"}

        operations = client.get("/api/operations", params={"path_prefix": "/incident", "limit": 20})
        assert operations.status_code == 200
        delete_operation = next(
            op for op in reversed(operations.json()["operations"])
            if op["operation_type"] == "DELETE" and op["path"] == "/incident"
        )

        response = client.get(f"/api/operations/{delete_operation['sequence_number']}/incident")
        assert response.status_code == 200

        data = response.json()
        assert data["incident_kind"] == "recursive_delete"
        assert data["operation"]["operation_type"] == "DELETE"
        assert data["blast_radius"]["affected_paths"] == 3
        assert data["affected_path_count"] == 3
        assert {item["path"] for item in data["affected_paths"]} == {"/incident", "/incident/a", "/incident/b"}
        assert {item["watch_session_id"] for item in data["watch_firings"]} == {direct_watch_session, parent_watch_session}
        assert any(session["session_id"] == owner_session and "actor" in session["roles"] for session in data["impacted_sessions"])
        assert data["causal_chain"][0]["sequence_number"] == delete_operation["sequence_number"]

    def test_operation_incident_reports_session_cleanup_cascade(self, client):
        """Session-close incidents should expose the cleanup delete and impacted watchers."""
        owner_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        direct_watch_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        parent_watch_session = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]

        root = client.post("/api/node/create", json={
            "path": "/incident-trace",
            "data": "",
            "persistent": True,
            "session_id": owner_session,
        })
        assert root.status_code == 200

        lease = client.post("/api/lease/acquire", json={
            "path": "/incident-trace/lease",
            "session_id": owner_session,
            "holder": "alpha",
        })
        assert lease.status_code == 200

        client.post("/api/watch/register", json={
            "path": "/incident-trace/lease",
            "session_id": direct_watch_session,
            "event_types": ["DELETE"],
        })
        client.post("/api/watch/register", json={
            "path": "/incident-trace",
            "session_id": parent_watch_session,
            "event_types": ["CHILDREN"],
        })

        closed = client.post("/api/session/close", json={"session_id": owner_session})
        assert closed.status_code == 200

        operations = client.get("/api/operations", params={"session_id": owner_session, "limit": 20})
        assert operations.status_code == 200
        session_close = next(
            op for op in operations.json()["operations"]
            if op["operation_type"] == "SESSION_CLOSE"
        )

        response = client.get("/api/operation/detail", params={"sequence_number": session_close["sequence_number"]})
        assert response.status_code == 200

        data = response.json()
        assert data["incident_kind"] == "session_cleanup"
        assert data["operation"]["operation_type"] == "SESSION_CLOSE"
        assert data["source_session"]["session_id"] == owner_session
        assert data["blast_radius"]["affected_paths"] == 1
        assert len(data["related_operations"]) == 1
        assert data["related_operations"][0]["operation_type"] == "DELETE"
        assert any(item["path"] == "/incident-trace/lease" for item in data["affected_paths"])
        assert {item["watch_session_id"] for item in data["watch_firings"]} == {direct_watch_session, parent_watch_session}
        assert any(session["session_id"] == owner_session and "actor" in session["roles"] for session in data["impacted_sessions"])
        assert [op["operation_type"] for op in data["causal_chain"]] == ["SESSION_CLOSE", "DELETE"]


class TestStreamingEndpoints:
    """Tests for SSE streaming APIs."""

    def test_operation_stream_emits_committed_event(self, client):
        """Operation SSE should emit committed operations as events."""
        session_id = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]
        client.post("/api/node/create", json={
            "path": "/stream-op",
            "data": "payload",
            "persistent": True,
            "session_id": session_id,
        })

        with client.stream("GET", "/api/stream/operations", params={"path_prefix": "/stream-op", "snapshot_only": True}) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            event_name, payload = _read_sse_event(response)

        assert event_name == "operation"
        assert payload["path"] == "/stream-op"
        assert payload["operation_type"] == "CREATE"
        assert payload["summary"].startswith("CREATE")

    def test_session_stream_emits_inventory_snapshot(self, client):
        """Session SSE should emit the current inventory snapshot immediately."""
        session_id = client.post("/api/session/open", json={"timeout_seconds": 30}).json()["session_id"]

        with client.stream("GET", "/api/stream/sessions", params={"alive_only": True, "snapshot_only": True}) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            event_name, payload = _read_sse_event(response)

        assert event_name == "sessions"
        assert payload["count"] >= 1
        assert session_id in {item["session_id"] for item in payload["sessions"]}


class TestEphemeralNodeCleanup:
    """Tests for ephemeral node cleanup when session closes."""
    
    def test_ephemeral_deleted_on_session_close(self, client):
        """Ephemeral nodes should be deleted when session closes."""
        # Create session
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Create ephemeral node
        response = client.post("/api/node/create", json={
            "path": "/ephemeral_cleanup",
            "data": "temp",
            "persistent": False,
            "session_id": session_id
        })
        assert response.status_code == 200
        
        # Verify exists
        response = client.get("/api/node/exists", params={"path": "/ephemeral_cleanup"})
        assert response.json()["exists"] == True
        
        # Close session
        response = client.post("/api/session/close", json={"session_id": session_id})
        assert response.status_code == 200
        
        # Verify ephemeral node deleted
        response = client.get("/api/node/exists", params={"path": "/ephemeral_cleanup"})
        assert response.json()["exists"] == False


class TestAPIErrorHandling:
    """Tests for API error handling."""
    
    def test_heartbeat_dead_session(self, client):
        """Heartbeat on dead session should return 400."""
        # Open and close a session
        open_resp = client.post("/api/session/open", json={"timeout_seconds": 30})
        session_id = open_resp.json()["session_id"]
        
        client.post("/api/session/close", json={"session_id": session_id})
        
        # Heartbeat should fail
        resp = client.post("/api/session/heartbeat", json={"session_id": session_id})
        assert resp.status_code == 400
    
    def test_create_without_parent(self, client):
        """Creating without parent should return 400."""
        open_resp = client.post("/api/session/open", json={"timeout_seconds": 30})
        session_id = open_resp.json()["session_id"]
        
        resp = client.post("/api/node/create", json={
            "path": "/nonexistent/child",
            "data": "test",
            "persistent": True,
            "session_id": session_id
        })
        assert resp.status_code == 400
    
    def test_set_nonexistent_node(self, client):
        """Setting nonexistent node should return 404."""
        resp = client.post("/api/node/set", json={
            "path": "/nonexistent",
            "data": "test"
        })
        assert resp.status_code == 404
    
    def test_delete_nonexistent_node(self, client):
        """Deleting nonexistent node should return 404."""
        resp = client.delete("/api/node/delete", params={"path": "/nonexistent"})
        assert resp.status_code == 404
    
    def test_list_children_nonexistent(self, client):
        """Listing children of nonexistent should return 404."""
        resp = client.get("/api/node/list_children", params={"path": "/nonexistent"})
        assert resp.status_code == 404
    
    def test_watch_wait_timeout(self, client):
        """Watch wait should timeout gracefully."""
        open_resp = client.post("/api/session/open", json={"timeout_seconds": 30})
        session_id = open_resp.json()["session_id"]
        
        # Create a node first
        client.post("/api/node/create", json={
            "path": "/timeout_test",
            "data": "test",
            "persistent": True,
            "session_id": session_id
        })
        
        # Register a watch
        watch_resp = client.post("/api/watch/register", json={
            "path": "/timeout_test",
            "session_id": session_id
        })
        watch_id = watch_resp.json()["watch_id"]
        
        # Wait with short timeout (don't trigger the watch)
        resp = client.get("/api/watch/wait", params={
            "watch_id": watch_id,
            "timeout_seconds": 1
        })
        # Should return 408 timeout or empty response
        assert resp.status_code in [200, 408]
    
    def test_unregister_nonexistent_watch(self, client):
        """Unregistering nonexistent watch should handle gracefully."""
        resp = client.delete("/api/watch/unregister", params={"watch_id": "nonexistent"})
        # Should return 404 or 200 depending on implementation
        assert resp.status_code in [200, 404]


class TestVersioningAPI:
    """Tests for version semantics via API."""
    
    def test_version_increments(self, client):
        """Version should increment on each update."""
        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]
        
        # Create node - version 1
        response = client.post("/api/node/create", json={
            "path": "/versioned",
            "data": "v1",
            "persistent": True,
            "session_id": session_id
        })
        assert response.json()["version"] == 1
        
        # Update - version 2
        response = client.post("/api/node/set", json={
            "path": "/versioned",
            "data": "v2"
        })
        assert response.json()["version"] == 2
        
        # Update - version 3
        response = client.post("/api/node/set", json={
            "path": "/versioned",
            "data": "v3"
        })
        assert response.json()["version"] == 3

    def test_set_with_expected_version_succeeds(self, client):
        """A matching expected_version should allow the write."""
        if not _model_has_field(main.SetNodeRequest, "expected_version"):
            pytest.skip("SetNodeRequest does not yet expose expected_version")

        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]

        client.post("/api/node/create", json={
            "path": "/cas",
            "data": "v1",
            "persistent": True,
            "session_id": session_id,
        })

        response = client.post("/api/node/set", json={
            "path": "/cas",
            "data": "v2",
            "expected_version": 1,
        })

        assert response.status_code == 200
        data = response.json()
        assert data["version"] == 2

        node_resp = client.get("/api/node/get", params={"path": "/cas"})
        assert node_resp.status_code == 200
        assert node_resp.json()["data"] == "v2"
        assert node_resp.json()["version"] == 2

    def test_set_with_stale_expected_version_conflicts(self, client):
        """A stale expected_version should reject the write."""
        if not _model_has_field(main.SetNodeRequest, "expected_version"):
            pytest.skip("SetNodeRequest does not yet expose expected_version")

        response = client.post("/api/session/open", json={})
        session_id = response.json()["session_id"]

        client.post("/api/node/create", json={
            "path": "/cas-stale",
            "data": "v1",
            "persistent": True,
            "session_id": session_id,
        })

        first_write = client.post("/api/node/set", json={
            "path": "/cas-stale",
            "data": "v2",
            "expected_version": 1,
        })
        assert first_write.status_code == 200

        stale_write = client.post("/api/node/set", json={
            "path": "/cas-stale",
            "data": "v3",
            "expected_version": 1,
        })

        assert stale_write.status_code in [400, 409, 412]
        detail = stale_write.json().get("detail", "")
        assert any(keyword in detail.lower() for keyword in ["version", "conflict", "expected"])

        node_resp = client.get("/api/node/get", params={"path": "/cas-stale"})
        assert node_resp.status_code == 200
        assert node_resp.json()["data"] == "v2"
        assert node_resp.json()["version"] == 2
