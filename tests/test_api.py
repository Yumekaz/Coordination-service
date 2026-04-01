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
from fastapi.testclient import TestClient

# Setup test database path before importing main
_test_db = tempfile.mktemp(suffix=".db")
os.environ["COORD_DB_PATH"] = _test_db

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from coordinator import Coordinator
import main


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
