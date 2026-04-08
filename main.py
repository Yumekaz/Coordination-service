"""
HTTP API for the Coordination Service.

FastAPI application exposing all coordination operations:
- Session management (open, heartbeat, close)
- Metadata operations (create, get, set, delete, exists, list_children)
- Watch operations (register, wait, unregister)
- Health and statistics

All operations are serialized through the Coordinator for linearizability.
"""

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager
import asyncio
import uvicorn
import os
import json

from cluster import ClusterManager, REPLICATION_TOKEN_HEADER
from coordinator import Coordinator
from models import EventType, NodeType, Operation, OperationType
from logger import get_logger
from config import HOST, PORT, DEFAULT_SESSION_TIMEOUT, WATCH_WAIT_TIMEOUT
from errors import ConflictError, ForbiddenError

logger = get_logger("api")

# Global coordinator instance
coordinator: Optional[Coordinator] = None
cluster_manager: Optional[ClusterManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager - handles startup and shutdown."""
    global coordinator, cluster_manager
    
    # Startup - only create if not already set (allows testing with custom coordinator)
    if coordinator is None:
        logger.info("Starting Coordination Service...")
        coordinator = Coordinator()
        recovery_stats = coordinator.start()
        logger.info(f"Recovery complete: {recovery_stats}")
    else:
        logger.info("Using pre-configured coordinator")

    if cluster_manager is None and coordinator is not None:
        cluster_manager = ClusterManager.from_env(coordinator)
        cluster_manager.start()
    
    yield
    
    # Shutdown
    logger.info("Stopping Coordination Service...")
    if cluster_manager:
        cluster_manager.stop()
        cluster_manager = None
    if coordinator:
        coordinator.stop()
    logger.info("Coordination Service stopped")


app = FastAPI(
    title="Coordination Service",
    description="Crash-aware coordination engine with leases, watches, and committed operation history",
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


class SessionSummaryResponse(BaseModel):
    session_id: str
    created_at: float
    last_heartbeat: float
    timeout_seconds: int
    expires_at: float
    remaining_seconds: float
    is_alive: bool
    ephemeral_nodes: List[str] = Field(default_factory=list)
    ephemeral_node_count: int
    watch_count: int


class SessionsResponse(BaseModel):
    sessions: List[SessionSummaryResponse]
    count: int
    status: str = "ok"


class WatchSummaryResponse(BaseModel):
    watch_id: str
    path: str
    session_id: str
    event_types: List[str]
    is_fired: bool
    created_at: float


class OwnedNodeResponse(BaseModel):
    path: str
    node_type: str
    version: int
    session_id: Optional[str] = None
    data_preview: str
    created_at: float
    modified_at: float


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
    lease_ttl_seconds: Optional[float] = Field(default=None, gt=0, le=3600)
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
    lease_ttl_seconds: Optional[float] = None
    lease_token: Optional[int] = None


class ReleaseLeaseRequest(BaseModel):
    path: str
    session_id: str


class RenewLeaseRequest(BaseModel):
    path: str
    session_id: str
    lease_ttl_seconds: float = Field(..., gt=0, le=3600)


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


class OperationResponse(BaseModel):
    sequence_number: int
    operation_type: str
    path: str
    data: str
    data_size: int
    data_preview: str
    delete_paths: List[str] = Field(default_factory=list)
    summary: str
    session_id: Optional[str] = None
    timestamp: float
    node_type: Optional[str] = None


class OperationsResponse(BaseModel):
    operations: List[OperationResponse]
    count: int
    last_sequence: int
    next_since: int
    status: str = "ok"


class WatchFireResponse(BaseModel):
    cause_sequence_number: int
    ordinal: int
    watch_id: str
    watch_session_id: str
    watch_path: str
    observed_path: str
    event_type: str
    timestamp: float


class SessionDetailResponse(SessionSummaryResponse):
    owned_nodes: List[OwnedNodeResponse] = Field(default_factory=list)
    watches: List[WatchSummaryResponse] = Field(default_factory=list)
    recent_watch_fires: List[WatchFireResponse] = Field(default_factory=list)
    recent_operations: List[OperationResponse] = Field(default_factory=list)


class LeaseHistoryEntryResponse(BaseModel):
    sequence_number: int
    timestamp: float
    session_id: Optional[str] = None
    holder: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class LeaseDetailResponse(BaseModel):
    path: str
    current_lease: Optional[LeaseResponse] = None
    holder_session: Optional[SessionSummaryResponse] = None
    waiters: List[str] = Field(default_factory=list)
    waiter_count: int
    holder_history: List[LeaseHistoryEntryResponse] = Field(default_factory=list)
    recent_operations: List[OperationResponse] = Field(default_factory=list)


class ClusterPeerResponse(BaseModel):
    peer_url: str
    peer_id: Optional[str] = None
    role: str
    reachable: bool
    healthy: bool
    last_applied: int = 0
    replication_lag: Optional[int] = None
    last_ack_at: Optional[float] = None
    last_error: Optional[str] = None


class ClusterApplyEventResponse(BaseModel):
    sequence_number: int
    peer_id: str
    status: str
    applied_at: float
    latency_ms: int
    applied_count: int = 1


class ClusterStatusResponse(BaseModel):
    node_id: str
    role: str
    leader_id: Optional[str] = None
    leader_url: Optional[str] = None
    current_term: int
    voted_for: Optional[str] = None
    commit_index: int
    last_applied: int
    quorum_size: int
    read_only: bool
    read_only_reason: str = ""
    replication_enabled: bool
    peer_count: int
    healthy_peer_count: int
    max_replication_lag: int
    quorum_commit_index: Optional[int] = None
    quorum_commit_lag: Optional[int] = None
    write_quorum_ready: bool
    require_write_quorum: bool
    push_commit_replication: bool
    last_leader_contact_at: Optional[float] = None
    leader_contact_stale: bool
    last_sync_at: Optional[float] = None
    last_error: Optional[str] = None
    peers: List[ClusterPeerResponse] = Field(default_factory=list)
    recent_apply: List[ClusterApplyEventResponse] = Field(default_factory=list)


class InternalReplicationApplyRequest(BaseModel):
    source_node_id: Optional[str] = None
    source_term: Optional[int] = Field(default=None, ge=0)
    prepared_write: bool = False
    operations: List[Dict[str, Any]] = Field(default_factory=list)


class InternalReplicationPrepareRequest(BaseModel):
    leader_id: str
    leader_url: Optional[str] = None
    term: int = Field(..., ge=0)
    start_sequence: int = Field(..., ge=1)
    count: int = Field(default=1, ge=1)


class InternalReplicationCancelPrepareRequest(BaseModel):
    leader_id: str
    term: int = Field(..., ge=0)
    start_sequence: int = Field(..., ge=1)
    count: int = Field(default=1, ge=1)


class InternalHeartbeatRequest(BaseModel):
    leader_id: str
    leader_url: Optional[str] = None
    term: int = Field(..., ge=0)
    commit_index: int = Field(default=0, ge=0)


class InternalVoteRequest(BaseModel):
    candidate_id: str
    term: int = Field(..., ge=0)
    candidate_last_applied: int = Field(default=0, ge=0)


class PathNodeResponse(BaseModel):
    path: str
    node_type: Optional[str] = None
    version: int
    session_id: Optional[str] = None
    data_preview: str = ""
    created_at: float
    modified_at: float
    holder: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    lease_token: Optional[int] = None


class PathDisappearanceResponse(BaseModel):
    state: str
    cause_kind: str
    reason: Optional[str] = None
    cause_session_id: Optional[str] = None
    delete_operation: OperationResponse
    cause_operation: Optional[OperationResponse] = None


class PathDetailResponse(BaseModel):
    path: str
    current_node: Optional[PathNodeResponse] = None
    last_known_node: Optional[PathNodeResponse] = None
    owner_session: Optional[SessionSummaryResponse] = None
    current_lease: Optional[LeaseResponse] = None
    holder_session: Optional[SessionSummaryResponse] = None
    waiters: List[str] = Field(default_factory=list)
    waiter_count: int
    holder_history: List[LeaseHistoryEntryResponse] = Field(default_factory=list)
    active_watches: List[WatchSummaryResponse] = Field(default_factory=list)
    fired_watches: List[WatchFireResponse] = Field(default_factory=list)
    disappearance: Optional[PathDisappearanceResponse] = None
    recent_operations: List[OperationResponse] = Field(default_factory=list)


class IncidentPathResponse(BaseModel):
    path: str
    change_kind: str = "change"
    summary: str = ""
    state: str
    session_id: Optional[str] = None
    node_type: Optional[str] = None
    holder: Optional[str] = None
    data_preview: str = ""
    modified_at: Optional[float] = None
    version_before: Optional[int] = None
    version_after: Optional[int] = None
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    note: str = ""
    cause_kind: Optional[str] = None
    deleted_in_operation: bool = False
    delete_sequence_number: Optional[int] = None


class IncidentSessionResponse(SessionSummaryResponse):
    roles: List[str] = Field(default_factory=list)


class IncidentStatsResponse(BaseModel):
    affected_paths: int
    displayed_paths: int
    watch_fires: int
    impacted_sessions: int
    cascade_operations: int


class IncidentBlastRadiusResponse(BaseModel):
    affected_paths: int
    watches_fired: int
    related_operations: int
    sessions_touched: int


class OperationIncidentResponse(BaseModel):
    sequence_number: int
    incident_kind: str
    summary: str
    subtitle: str
    operation: OperationResponse
    source_session: Optional[IncidentSessionResponse] = None
    primary_path: str = ""
    blast_radius: IncidentBlastRadiusResponse
    cause_operation: Optional[OperationResponse] = None
    cascade_operations: List[OperationResponse] = Field(default_factory=list)
    related_operations: List[OperationResponse] = Field(default_factory=list)
    causal_chain: List[OperationResponse] = Field(default_factory=list)
    affected_path_count: int
    affected_paths: List[IncidentPathResponse] = Field(default_factory=list)
    impacted_sessions: List[IncidentSessionResponse] = Field(default_factory=list)
    watch_fires: List[WatchFireResponse] = Field(default_factory=list)
    watch_firings: List[WatchFireResponse] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)
    stats: IncidentStatsResponse


class ErrorResponse(BaseModel):
    error: str
    detail: str


def _decode_delete_paths_payload(operation: Operation) -> List[str]:
    """Decode delete-path payloads from current or future shapes."""
    if operation.operation_type != OperationType.DELETE or not operation.data:
        return []

    try:
        decoded = json.loads(operation.data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return []

    if isinstance(decoded, list):
        return [str(path) for path in decoded]
    if isinstance(decoded, dict):
        paths = decoded.get("paths")
        if isinstance(paths, list):
            return [str(path) for path in paths]
    return []


def _serialize_operation(operation: Operation) -> OperationResponse:
    """Convert an internal operation into an API response model."""
    payload = operation.to_dict()
    delete_paths = _decode_delete_paths_payload(operation)

    data_preview = payload["data"]
    if len(data_preview) > 120:
        data_preview = data_preview[:117] + "..."

    summary = f"{payload['operation_type']} {payload['path'] or '(session)'}"
    if operation.operation_type == OperationType.DELETE and delete_paths:
        summary = f"DELETE {len(delete_paths)} path(s)"
    elif operation.operation_type in (OperationType.CREATE, OperationType.SET) and payload["path"]:
        summary = f"{payload['operation_type']} {payload['path']}"
    elif operation.session_id:
        summary = f"{payload['operation_type']} {operation.session_id[:8]}"

    return OperationResponse(
        sequence_number=payload["sequence_number"],
        operation_type=payload["operation_type"],
        path=payload["path"],
        data=payload["data"],
        data_size=len(operation.data or b""),
        data_preview=data_preview,
        delete_paths=delete_paths,
        summary=summary,
        session_id=payload["session_id"],
        timestamp=payload["timestamp"],
        node_type=payload["node_type"],
    )


def _model_dump(model: Any) -> Dict[str, Any]:
    """Compat helper for Pydantic v1/v2 models."""
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _require_cluster_manager() -> ClusterManager:
    """Return the initialized cluster manager."""
    if cluster_manager is None:
        raise HTTPException(status_code=503, detail="Cluster manager is not initialized")
    return cluster_manager


def _require_replication_auth(request: Request) -> None:
    """Protect internal replication endpoints when a token is configured."""
    manager = _require_cluster_manager()
    token = getattr(manager, "_replication_token", "")
    if not token:
        return
    if request.headers.get(REPLICATION_TOKEN_HEADER) != token:
        raise HTTPException(status_code=403, detail="Invalid replication token")


def _sse_event(
    data: Dict[str, Any],
    event: str,
    event_id: Optional[int] = None,
    retry_ms: Optional[int] = None,
) -> str:
    """Serialize one Server-Sent Event payload."""
    lines: List[str] = []
    if retry_ms is not None:
        lines.append(f"retry: {retry_ms}")
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    payload = json.dumps(data)
    for line in payload.splitlines() or [""]:
        lines.append(f"data: {line}")
    return "\n".join(lines) + "\n\n"


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


@app.get("/api/sessions", response_model=SessionsResponse)
async def list_sessions(
    alive_only: bool = Query(default=False, description="Only return alive sessions"),
) -> SessionsResponse:
    """List current session inventory for clients and the visualizer."""
    sessions = coordinator.get_sessions(alive_only=alive_only)
    return SessionsResponse(
        sessions=[SessionSummaryResponse(**session) for session in sessions],
        count=len(sessions),
    )


@app.get("/api/session/detail", response_model=SessionDetailResponse)
async def session_detail(
    session_id: str = Query(..., description="Session ID"),
    operation_limit: int = Query(default=20, ge=1, le=100),
) -> SessionDetailResponse:
    """Return a drill-down view of one session."""
    detail = coordinator.get_session_detail(session_id=session_id, operation_limit=operation_limit)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")

    return SessionDetailResponse(
        **{key: value for key, value in detail.items() if key not in {"owned_nodes", "watches", "recent_watch_fires", "recent_operations"}},
        owned_nodes=[OwnedNodeResponse(**node) for node in detail.get("owned_nodes", [])],
        watches=[
            WatchSummaryResponse(
                watch_id=watch["watch_id"],
                path=watch["path"],
                session_id=watch["session_id"],
                event_types=[event_type.value if hasattr(event_type, "value") else str(event_type) for event_type in watch["event_types"]],
                is_fired=watch["is_fired"],
                created_at=watch["created_at"],
            )
            for watch in detail["watches"]
        ],
        recent_watch_fires=[WatchFireResponse(**record) for record in detail.get("recent_watch_fires", [])],
        recent_operations=[_serialize_operation(operation) for operation in detail["recent_operations"]],
    )


@app.get("/api/path/detail", response_model=PathDetailResponse)
async def path_detail(
    path: str = Query(..., description="Exact node path"),
    operation_limit: int = Query(default=20, ge=1, le=100),
    watch_limit: int = Query(default=20, ge=1, le=100),
) -> PathDetailResponse:
    """Return a current or historical trace for an exact path."""
    detail = coordinator.get_path_detail(
        path=path,
        operation_limit=operation_limit,
        watch_limit=watch_limit,
    )
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Path not found in current state or history: {path}")

    disappearance = detail.get("disappearance")
    return PathDetailResponse(
        path=detail["path"],
        current_node=PathNodeResponse(**detail["current_node"]) if detail.get("current_node") else None,
        last_known_node=PathNodeResponse(**detail["last_known_node"]) if detail.get("last_known_node") else None,
        owner_session=SessionSummaryResponse(**detail["owner_session"]) if detail.get("owner_session") else None,
        current_lease=LeaseResponse(**detail["current_lease"]) if detail.get("current_lease") else None,
        holder_session=SessionSummaryResponse(**detail["holder_session"]) if detail.get("holder_session") else None,
        waiters=detail.get("waiters", []),
        waiter_count=detail.get("waiter_count", 0),
        holder_history=[LeaseHistoryEntryResponse(**entry) for entry in detail.get("holder_history", [])],
        active_watches=[
            WatchSummaryResponse(
                watch_id=watch["watch_id"],
                path=watch["path"],
                session_id=watch["session_id"],
                event_types=[event_type.value if hasattr(event_type, "value") else str(event_type) for event_type in watch["event_types"]],
                is_fired=watch["is_fired"],
                created_at=watch["created_at"],
            )
            for watch in detail.get("active_watches", [])
        ],
        fired_watches=[WatchFireResponse(**record) for record in detail.get("fired_watches", [])],
        disappearance=PathDisappearanceResponse(
            state=disappearance["state"],
            cause_kind=disappearance["cause_kind"],
            reason=disappearance.get("reason"),
            cause_session_id=disappearance.get("cause_session_id"),
            delete_operation=_serialize_operation(disappearance["delete_operation"]),
            cause_operation=_serialize_operation(disappearance["cause_operation"]) if disappearance.get("cause_operation") else None,
        ) if disappearance else None,
        recent_operations=[_serialize_operation(operation) for operation in detail["recent_operations"]],
    )


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
        lease_ttl_seconds=request.lease_ttl_seconds,
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


@app.get("/api/lease/detail", response_model=LeaseDetailResponse)
async def lease_detail(
    path: str = Query(..., description="Lease path"),
    operation_limit: int = Query(default=20, ge=1, le=100),
) -> LeaseDetailResponse:
    """Return a drill-down view of a lease path."""
    detail = coordinator.get_lease_detail(path=path, operation_limit=operation_limit)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Lease not found: {path}")

    return LeaseDetailResponse(
        path=detail["path"],
        current_lease=LeaseResponse(**detail["current_lease"]) if detail.get("current_lease") else None,
        holder_session=SessionSummaryResponse(**detail["holder_session"]) if detail.get("holder_session") else None,
        waiters=detail["waiters"],
        waiter_count=detail["waiter_count"],
        holder_history=[LeaseHistoryEntryResponse(**entry) for entry in detail.get("holder_history", [])],
        recent_operations=[_serialize_operation(operation) for operation in detail["recent_operations"]],
    )


@app.post("/api/lease/release")
async def release_lease(request: ReleaseLeaseRequest) -> dict:
    """Release a lease if the caller currently owns it."""
    coordinator.release_lease(request.path, request.session_id)
    return {"status": "released", "path": request.path}


@app.post("/api/lease/renew", response_model=LeaseResponse)
async def renew_lease(request: RenewLeaseRequest) -> LeaseResponse:
    """Renew a lease by resetting its independent TTL."""
    lease = coordinator.renew_lease(
        path=request.path,
        session_id=request.session_id,
        lease_ttl_seconds=request.lease_ttl_seconds,
    )
    return LeaseResponse(**lease)


@app.get("/api/cluster/status", response_model=ClusterStatusResponse)
async def cluster_status() -> ClusterStatusResponse:
    """Return current cluster role, lag, and peer health."""
    status = _require_cluster_manager().build_status()
    return ClusterStatusResponse(
        **{
            **status,
            "peers": [ClusterPeerResponse(**peer) for peer in status.get("peers", [])],
            "recent_apply": [
                ClusterApplyEventResponse(**event)
                for event in status.get("recent_apply", [])
            ],
        }
    )


@app.get("/internal/replication/operations")
async def internal_replication_operations(
    request: Request,
    since_sequence: int = Query(default=0, ge=0),
    limit: int = Query(default=128, ge=1, le=1000),
) -> dict:
    """Expose committed operations for follower catch-up."""
    _require_replication_auth(request)
    return _require_cluster_manager().export_operations(
        since_sequence=since_sequence,
        limit=limit,
    )


@app.get("/internal/replication/state")
async def internal_replication_state(request: Request) -> dict:
    """Expose local replication progress for leader peer monitoring."""
    _require_replication_auth(request)
    return _require_cluster_manager().get_internal_state()


@app.post("/internal/replication/apply")
async def internal_replication_apply(
    request: Request,
    payload: InternalReplicationApplyRequest,
) -> dict:
    """Apply a pushed replication batch on a follower."""
    _require_replication_auth(request)
    return _require_cluster_manager().apply_replication_batch(
        source_node_id=payload.source_node_id,
        source_term=payload.source_term,
        prepared_write=payload.prepared_write,
        operations_payload=payload.operations,
    )


@app.post("/internal/replication/prepare")
async def internal_replication_prepare(
    request: Request,
    payload: InternalReplicationPrepareRequest,
) -> dict:
    """Reserve the exact next operation sequence on a follower."""
    _require_replication_auth(request)
    return _require_cluster_manager().receive_prepare(
        leader_id=payload.leader_id,
        leader_url=payload.leader_url,
        term=payload.term,
        start_sequence=payload.start_sequence,
        count=payload.count,
    )


@app.post("/internal/replication/cancel-prepare")
async def internal_replication_cancel_prepare(
    request: Request,
    payload: InternalReplicationCancelPrepareRequest,
) -> dict:
    """Release a follower-side prepare reservation after a failed quorum attempt."""
    _require_replication_auth(request)
    return _require_cluster_manager().receive_cancel_prepare(
        leader_id=payload.leader_id,
        term=payload.term,
        start_sequence=payload.start_sequence,
        count=payload.count,
    )


@app.post("/internal/cluster/heartbeat")
async def internal_cluster_heartbeat(
    request: Request,
    payload: InternalHeartbeatRequest,
) -> dict:
    """Accept a leader heartbeat for election/failover groundwork."""
    _require_replication_auth(request)
    return _require_cluster_manager().receive_heartbeat(
        leader_id=payload.leader_id,
        term=payload.term,
        leader_url=payload.leader_url,
        commit_index=payload.commit_index,
    )


@app.post("/internal/cluster/request-vote")
async def internal_cluster_request_vote(
    request: Request,
    payload: InternalVoteRequest,
) -> dict:
    """Evaluate a vote request from a candidate."""
    _require_replication_auth(request)
    return _require_cluster_manager().request_vote(
        candidate_id=payload.candidate_id,
        term=payload.term,
        candidate_last_applied=payload.candidate_last_applied,
    )


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


@app.get("/api/operations", response_model=OperationsResponse)
async def list_operations(
    since_sequence: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    operation_types: Optional[List[OperationType]] = Query(default=None),
    path_prefix: Optional[str] = Query(default=None),
    session_id: Optional[str] = Query(default=None),
) -> OperationsResponse:
    """List committed operations in sequence order."""
    operations = coordinator.get_operations(
        since_sequence=since_sequence,
        limit=limit,
        operation_types=set(operation_types) if operation_types else None,
        path_prefix=path_prefix,
        session_id=session_id,
    )
    serialized = [_serialize_operation(op) for op in operations]
    last_sequence = serialized[-1].sequence_number if serialized else since_sequence
    return OperationsResponse(
        operations=serialized,
        count=len(serialized),
        last_sequence=last_sequence,
        next_since=last_sequence,
    )


@app.get("/api/operations/tail", response_model=OperationsResponse)
async def tail_operations(
    since_sequence: int = Query(default=0, ge=0),
    timeout_seconds: float = Query(default=WATCH_WAIT_TIMEOUT, ge=0, le=300),
    limit: int = Query(default=100, ge=1, le=1000),
    operation_types: Optional[List[OperationType]] = Query(default=None),
    path_prefix: Optional[str] = Query(default=None),
    session_id: Optional[str] = Query(default=None),
) -> OperationsResponse:
    """Wait for the next committed operations after a sequence number."""
    operations = coordinator.wait_for_operations(
        since_sequence=since_sequence,
        timeout_seconds=timeout_seconds,
        limit=limit,
        operation_types=set(operation_types) if operation_types else None,
        path_prefix=path_prefix,
        session_id=session_id,
    )
    serialized = [_serialize_operation(op) for op in operations]
    last_sequence = serialized[-1].sequence_number if serialized else since_sequence
    status = "ok" if serialized else "timeout"
    return OperationsResponse(
        operations=serialized,
        count=len(serialized),
        last_sequence=last_sequence,
        next_since=last_sequence,
        status=status,
    )


@app.get("/api/operations/{sequence_number}", response_model=OperationResponse)
async def get_operation(sequence_number: int) -> OperationResponse:
    """Fetch one committed operation by sequence number."""
    operation = coordinator.get_operation(sequence_number)
    if operation is None:
        raise HTTPException(status_code=404, detail=f"Operation not found: {sequence_number}")
    return _serialize_operation(operation)


@app.get("/api/operations/{sequence_number}/incident", response_model=OperationIncidentResponse)
async def get_operation_incident(
    sequence_number: int,
    path_limit: int = Query(default=12, ge=1, le=100),
    watch_limit: int = Query(default=50, ge=1, le=200),
) -> OperationIncidentResponse:
    """Return an operation-centric incident report with blast radius."""
    incident = coordinator.get_operation_incident(
        sequence_number=sequence_number,
        path_limit=path_limit,
        watch_limit=watch_limit,
    )
    if incident is None:
        raise HTTPException(status_code=404, detail=f"Operation not found: {sequence_number}")

    raw_source_session = incident.get("source_session")
    impacted_sessions_by_id: Dict[str, Dict[str, Any]] = {}
    if raw_source_session and raw_source_session.get("session_id"):
        impacted_sessions_by_id[raw_source_session["session_id"]] = {
            **raw_source_session,
            "roles": ["actor", *raw_source_session.get("roles", [])],
        }
    elif incident["operation"].session_id:
        impacted_sessions_by_id[incident["operation"].session_id] = {
            "session_id": incident["operation"].session_id,
            "is_alive": False,
            "timeout_seconds": 0,
            "remaining_seconds": 0,
            "ephemeral_node_count": 0,
            "watch_count": 0,
            "created_at": 0,
            "last_heartbeat": 0,
            "roles": ["actor"],
        }

    for session in incident.get("impacted_sessions", []):
        session_id = session.get("session_id")
        if not session_id:
            continue
        roles = list(session.get("roles", []))
        if session_id in impacted_sessions_by_id:
            merged_roles = [*impacted_sessions_by_id[session_id].get("roles", []), *roles]
            impacted_sessions_by_id[session_id] = {
                **impacted_sessions_by_id[session_id],
                **session,
                "roles": list(dict.fromkeys(merged_roles)),
            }
            continue
        impacted_sessions_by_id[session_id] = {
            **session,
            "roles": roles or ["watcher"],
        }

    impacted_sessions = [
        IncidentSessionResponse(**session)
        for session in impacted_sessions_by_id.values()
    ]
    source_session = next(
        (session for session in impacted_sessions if "actor" in session.roles),
        None,
    )
    cause_operation = incident.get("cause_operation")
    cascade_operations = [_serialize_operation(operation) for operation in incident.get("cascade_operations", [])]
    related_operations = [
        _serialize_operation(operation)
        for operation in incident.get("related_operations", incident.get("cascade_operations", []))
    ]
    causal_chain = [
        _serialize_operation(operation)
        for operation in incident.get("causal_chain", [cause_operation, incident["operation"], *incident.get("cascade_operations", [])])
        if operation is not None
    ]
    primary_path = ""
    if incident.get("primary_path"):
        primary_path = incident["primary_path"]
    elif incident.get("affected_paths"):
        primary_path = incident["affected_paths"][0]["path"]
    elif incident["operation"].path:
        primary_path = incident["operation"].path

    operation_response = _serialize_operation(incident["operation"])
    incident_kind = incident["incident_kind"]
    summary = incident.get("summary") or {
        "session_cleanup": f"Session cleanup incident for {primary_path or operation_response.session_id or '(session)'}",
        "session_timeout_cleanup": f"Session timeout cleanup for {primary_path or operation_response.session_id or '(session)'}",
        "recursive_delete": f"Recursive delete across {incident.get('affected_path_count', 0)} path(s)",
        "delete": operation_response.summary,
        "create": operation_response.summary,
        "update": operation_response.summary,
        "session_close": operation_response.summary,
        "session_timeout": operation_response.summary,
    }.get(incident_kind, operation_response.summary)
    subtitle = incident.get("subtitle") or "Committed operation incident"
    blast_radius = incident.get("blast_radius") or {}
    related_operation_count = len(related_operations) or (len(cascade_operations) + (1 if cause_operation is not None else 0))
    notes = [note for note in incident.get("notes", []) if note]
    if incident.get("affected_path_count", 0) > len(incident.get("affected_paths", [])):
        notes.append(f"Showing first {path_limit} affected path(s).")

    return OperationIncidentResponse(
        sequence_number=incident["sequence_number"],
        incident_kind=incident_kind,
        summary=summary,
        subtitle=subtitle,
        operation=operation_response,
        source_session=source_session,
        primary_path=primary_path,
        blast_radius=IncidentBlastRadiusResponse(
            affected_paths=blast_radius.get("affected_paths", incident.get("affected_path_count", 0)),
            watches_fired=blast_radius.get("watches_fired", len(incident.get("watch_fires", []))),
            related_operations=blast_radius.get("related_operations", related_operation_count),
            sessions_touched=blast_radius.get("sessions_touched", len(impacted_sessions)),
        ),
        cause_operation=_serialize_operation(cause_operation) if cause_operation else None,
        cascade_operations=cascade_operations,
        related_operations=related_operations,
        causal_chain=causal_chain,
        affected_path_count=incident.get("affected_path_count", 0),
        affected_paths=[IncidentPathResponse(**path) for path in incident.get("affected_paths", [])],
        impacted_sessions=impacted_sessions,
        watch_fires=[WatchFireResponse(**record) for record in incident.get("watch_fires", [])],
        watch_firings=[WatchFireResponse(**record) for record in incident.get("watch_fires", [])],
        notes=notes,
        stats=IncidentStatsResponse(**incident.get("stats", {})),
    )


@app.get("/api/operation/detail", response_model=OperationIncidentResponse)
async def get_operation_incident_legacy(
    sequence_number: int = Query(..., ge=1),
    path_limit: int = Query(default=12, ge=1, le=100),
    watch_limit: int = Query(default=50, ge=1, le=200),
) -> OperationIncidentResponse:
    """Legacy alias for operation incident detail used by the visualizer."""
    return await get_operation_incident(
        sequence_number=sequence_number,
        path_limit=path_limit,
        watch_limit=watch_limit,
    )


@app.get("/api/recovery/last")
async def last_recovery() -> dict:
    """Return the most recent startup recovery report."""
    return coordinator.get_last_recovery_stats()


@app.get("/api/stream/operations")
async def stream_operations(
    request: Request,
    since_sequence: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    operation_types: Optional[List[OperationType]] = Query(default=None),
    path_prefix: Optional[str] = Query(default=None),
    session_id: Optional[str] = Query(default=None),
    snapshot_only: bool = Query(default=False),
) -> StreamingResponse:
    """Stream committed operations as Server-Sent Events."""
    requested_types = set(operation_types) if operation_types else None
    last_event_id = request.headers.get("last-event-id", "").strip()
    current_since = since_sequence
    if last_event_id.isdigit():
        current_since = max(current_since, int(last_event_id))

    async def event_stream():
        nonlocal current_since
        backlog = coordinator.get_operations(
            since_sequence=current_since,
            limit=limit,
            operation_types=requested_types,
            path_prefix=path_prefix,
            session_id=session_id,
        )
        for operation in backlog:
            payload = _model_dump(_serialize_operation(operation))
            current_since = operation.sequence_number
            yield _sse_event(payload, event="operation", event_id=operation.sequence_number, retry_ms=2000)

        if snapshot_only:
            return

        while True:
            if await request.is_disconnected():
                break

            operations = await asyncio.to_thread(
                coordinator.wait_for_operations,
                current_since,
                15.0,
                limit,
                requested_types,
                path_prefix,
                session_id,
            )
            if not operations:
                yield ": keep-alive\n\n"
                continue

            for operation in operations:
                payload = _model_dump(_serialize_operation(operation))
                current_since = operation.sequence_number
                yield _sse_event(payload, event="operation", event_id=operation.sequence_number)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/stream/sessions")
async def stream_sessions(
    request: Request,
    alive_only: bool = Query(default=True),
    snapshot_only: bool = Query(default=False),
) -> StreamingResponse:
    """Stream session inventory snapshots as Server-Sent Events."""

    async def event_stream():
        current_version, sessions = coordinator.get_session_stream_snapshot(alive_only=alive_only)
        payload = _model_dump(
            SessionsResponse(
                sessions=[SessionSummaryResponse(**session) for session in sessions],
                count=len(sessions),
            )
        )
        yield _sse_event(payload, event="sessions", event_id=current_version, retry_ms=2000)

        if snapshot_only:
            return

        while True:
            if await request.is_disconnected():
                break

            next_version, sessions = await asyncio.to_thread(
                coordinator.wait_for_sessions,
                current_version,
                15.0,
                alive_only,
            )
            if next_version == current_version:
                yield ": keep-alive\n\n"
                continue

            current_version = next_version
            payload = _model_dump(
                SessionsResponse(
                    sessions=[SessionSummaryResponse(**session) for session in sessions],
                    count=len(sessions),
                )
            )
            yield _sse_event(payload, event="sessions", event_id=current_version)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


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
