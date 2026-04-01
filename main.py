"""
HTTP API for the Coordination Service.

FastAPI application exposing all coordination operations:
- Session management (open, heartbeat, close)
- Metadata operations (create, get, set, delete, exists, list_children)
- Watch operations (register, wait, unregister)
- Health and statistics

All operations are serialized through the Coordinator for linearizability.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager
import uvicorn
import os

from coordinator import Coordinator
from models import EventType, NodeType
from logger import get_logger
from config import HOST, PORT, DEFAULT_SESSION_TIMEOUT, WATCH_WAIT_TIMEOUT
from errors import ConflictError, ForbiddenError

logger = get_logger("api")

# Global coordinator instance
coordinator: Optional[Coordinator] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager - handles startup and shutdown."""
    global coordinator
    
    # Startup - only create if not already set (allows testing with custom coordinator)
    if coordinator is None:
        logger.info("Starting Coordination Service...")
        coordinator = Coordinator()
        recovery_stats = coordinator.start()
        logger.info(f"Recovery complete: {recovery_stats}")
    else:
        logger.info("Using pre-configured coordinator")
    
    yield
    
    # Shutdown
    logger.info("Stopping Coordination Service...")
    if coordinator:
        coordinator.stop()
    logger.info("Coordination Service stopped")


app = FastAPI(
    title="Coordination Service",
    description="ZooKeeper-class metadata and coordination service",
    version="1.0.0",
    lifespan=lifespan,
)

# Enable CORS for the visualizer
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ========== Request/Response Models ==========

class OpenSessionRequest(BaseModel):
    timeout_seconds: int = Field(default=DEFAULT_SESSION_TIMEOUT, ge=5, le=300)


class OpenSessionResponse(BaseModel):
    session_id: str
    status: str = "created"


class HeartbeatRequest(BaseModel):
    session_id: str


class HeartbeatResponse(BaseModel):
    status: str = "ok"


class CreateNodeRequest(BaseModel):
    path: str
    data: str = ""
    persistent: bool = True
    session_id: Optional[str] = None


class CreateNodeResponse(BaseModel):
    path: str
    version: int
    status: str = "created"


class GetNodeResponse(BaseModel):
    path: str
    data: str
    version: int
    node_type: str
    created_at: float
    modified_at: float


class SetNodeRequest(BaseModel):
    path: str
    data: str
    expected_version: Optional[int] = Field(default=None, ge=0)


class SetNodeResponse(BaseModel):
    path: str
    version: int
    status: str = "updated"


class DeleteNodeResponse(BaseModel):
    status: str = "deleted"
    deleted_paths: List[str] = []


class ExistsResponse(BaseModel):
    exists: bool


class ListChildrenResponse(BaseModel):
    children: List[str]


class RegisterWatchRequest(BaseModel):
    path: str
    session_id: str
    event_types: Optional[List[EventType]] = None


class AcquireLeaseRequest(BaseModel):
    path: str
    session_id: str
    holder: Optional[str] = None
    data: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    wait_timeout_seconds: float = Field(default=0.0, ge=0, le=300)
    create_parents: bool = True


class LeaseResponse(BaseModel):
    path: str
    session_id: str
    holder: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    version: int
    acquired_at: float
    modified_at: float
    expires_at: Optional[float] = None
    lease_token: Optional[int] = None


class ReleaseLeaseRequest(BaseModel):
    path: str
    session_id: str


class RegisterWatchResponse(BaseModel):
    watch_id: str
    status: str = "registered"


class WatchEventResponse(BaseModel):
    event_type: str
    path: str
    data: str
    sequence: int


class HealthResponse(BaseModel):
    status: str
    nodes_count: int
    sessions_count: int
    uptime_seconds: int


class ErrorResponse(BaseModel):
    error: str
    detail: str


# ========== Exception Handlers ==========

@app.exception_handler(KeyError)
async def key_error_handler(request, exc) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={"error": "not_found", "detail": str(exc)},
    )


@app.exception_handler(ValueError)
async def value_error_handler(request, exc) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content={"error": "bad_request", "detail": str(exc)},
    )


@app.exception_handler(ConflictError)
async def conflict_error_handler(request, exc: ConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content=exc.to_response(),
    )


@app.exception_handler(ForbiddenError)
async def forbidden_error_handler(request, exc: ForbiddenError) -> JSONResponse:
    return JSONResponse(
        status_code=403,
        content=exc.to_response(),
    )


# ========== Session Endpoints ==========

@app.post("/api/session/open", response_model=OpenSessionResponse)
async def open_session(request: OpenSessionRequest) -> OpenSessionResponse:
    """Open a new client session."""
    try:
        session = coordinator.open_session(request.timeout_seconds)
        return OpenSessionResponse(session_id=session.session_id)
    except Exception as e:
        logger.error(f"Error opening session: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/session/heartbeat", response_model=HeartbeatResponse)
async def heartbeat(request: HeartbeatRequest) -> HeartbeatResponse:
    """Send a heartbeat for a session."""
    try:
        coordinator.heartbeat(request.session_id)
        return HeartbeatResponse()
    except KeyError as e:
        raise HTTPException(status_code=404, detail=f"Session not found: {request.session_id}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/session/close")
async def close_session(request: HeartbeatRequest) -> dict:
    """Close a session explicitly."""
    session = coordinator.close_session(request.session_id)
    if session is None:
        raise HTTPException(status_code=404, detail=f"Session not found: {request.session_id}")
    return {"status": "closed", "session_id": request.session_id}


# ========== Metadata Endpoints ==========

@app.post("/api/node/create", response_model=CreateNodeResponse)
async def create_node(request: CreateNodeRequest) -> CreateNodeResponse:
    """Create a new node in the metadata tree."""
    try:
        node = coordinator.create(
            path=request.path,
            data=request.data.encode("utf-8"),
            persistent=request.persistent,
            session_id=request.session_id,
        )
        return CreateNodeResponse(path=node.path, version=node.version)
    except KeyError as e:
        raise HTTPException(status_code=409, detail=f"Node already exists: {request.path}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/node/get", response_model=GetNodeResponse)
async def get_node(path: str = Query(..., description="Node path")) -> GetNodeResponse:
    """Get a node's data and metadata."""
    node = coordinator.get(path)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Node not found: {path}")
    
    return GetNodeResponse(
        path=node.path,
        data=node.data.decode("utf-8") if isinstance(node.data, bytes) else node.data,
        version=node.version,
        node_type=node.node_type.value,
        created_at=node.created_at,
        modified_at=node.modified_at,
    )


@app.post("/api/node/set", response_model=SetNodeResponse)
async def set_node(request: SetNodeRequest) -> SetNodeResponse:
    """Update a node's data."""
    try:
        node = coordinator.set(
            request.path,
            request.data.encode("utf-8"),
            expected_version=request.expected_version,
        )
        return SetNodeResponse(path=node.path, version=node.version)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=f"Node not found: {request.path}")


@app.delete("/api/node/delete", response_model=DeleteNodeResponse)
async def delete_node(path: str = Query(..., description="Node path")) -> DeleteNodeResponse:
    """Delete a node and all its children."""
    try:
        deleted_paths = coordinator.delete(path)
        return DeleteNodeResponse(deleted_paths=deleted_paths)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=f"Node not found: {path}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/node/exists", response_model=ExistsResponse)
async def node_exists(path: str = Query(..., description="Node path")) -> ExistsResponse:
    """Check if a node exists."""
    exists = coordinator.exists(path)
    return ExistsResponse(exists=exists)


@app.get("/api/node/list_children", response_model=ListChildrenResponse)
async def list_children(path: str = Query(..., description="Parent path")) -> ListChildrenResponse:
    """List the direct children of a node."""
    try:
        children = coordinator.list_children(path)
        return ListChildrenResponse(children=children)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=f"Node not found: {path}")


# ========== Watch Endpoints ==========

@app.post("/api/watch/register", response_model=RegisterWatchResponse)
async def register_watch(request: RegisterWatchRequest) -> RegisterWatchResponse:
    """Register a watch on a path."""
    try:
        watch = coordinator.register_watch(
            path=request.path,
            session_id=request.session_id,
            event_types=set(request.event_types) if request.event_types else None,
        )
        return RegisterWatchResponse(watch_id=watch.watch_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/watch/wait")
async def wait_watch(
    watch_id: str = Query(..., description="Watch ID"),
    timeout_seconds: float = Query(default=WATCH_WAIT_TIMEOUT, ge=1, le=300),
) -> dict:
    """Wait for a watch to fire (blocking)."""
    event = coordinator.wait_watch(watch_id, timeout_seconds)
    
    if event is None:
        return {"status": "timeout", "watch_id": watch_id}
    
    response = WatchEventResponse(
        event_type=event.event_type.value,
        path=event.path,
        data=event.data.decode("utf-8") if isinstance(event.data, bytes) else event.data,
        sequence=event.sequence_number,
    )
    if hasattr(response, "model_dump"):
        return response.model_dump()
    return response.dict()


@app.delete("/api/watch/unregister")
async def unregister_watch(watch_id: str = Query(..., description="Watch ID")) -> dict:
    """Unregister a watch."""
    result = coordinator.unregister_watch(watch_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Watch not found: {watch_id}")
    return {"status": "unregistered", "watch_id": watch_id}


# ========== Lease Endpoints ==========

@app.post("/api/lease/acquire", response_model=LeaseResponse)
async def acquire_lease(request: AcquireLeaseRequest) -> LeaseResponse:
    """Acquire an exclusive lease backed by an ephemeral node."""
    lease = coordinator.acquire_lease(
        path=request.path,
        session_id=request.session_id,
        holder=request.holder or request.data,
        metadata=request.metadata,
        wait_timeout_seconds=request.wait_timeout_seconds,
        create_parents=request.create_parents,
    )
    return LeaseResponse(**lease)


@app.get("/api/lease/get", response_model=LeaseResponse)
async def get_lease(path: str = Query(..., description="Lease path")) -> LeaseResponse:
    """Get the current state of a lease."""
    lease = coordinator.get_lease(path)
    if lease is None:
        raise HTTPException(status_code=404, detail=f"Lease not found: {path}")
    return LeaseResponse(**lease)


@app.post("/api/lease/release")
async def release_lease(request: ReleaseLeaseRequest) -> dict:
    """Release a lease if the caller currently owns it."""
    coordinator.release_lease(request.path, request.session_id)
    return {"status": "released", "path": request.path}


# ========== Health Endpoints ==========

@app.get("/api/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Get the health status of the service."""
    health_data = coordinator.get_health()
    return HealthResponse(**health_data)


@app.get("/api/stats")
async def stats() -> dict:
    """Get detailed statistics."""
    return coordinator.get_stats()


@app.get("/api/verify")
async def verify() -> dict:
    """Verify system consistency."""
    is_consistent, issues = coordinator.verify_consistency()
    return {
        "consistent": is_consistent,
        "issues": issues,
    }


# ========== Visualizer ==========

from fastapi.responses import FileResponse

@app.get("/")
async def visualizer():
    """Serve the visualizer HTML."""
    visualizer_path = os.path.join(os.path.dirname(__file__), "visualizer.html")
    return FileResponse(visualizer_path, media_type="text/html")


# ========== Main Entry Point ==========

def main() -> None:
    """Run the server."""
    uvicorn.run(
        "main:app",
        host=HOST,
        port=PORT,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
