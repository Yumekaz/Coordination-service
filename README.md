# Coordination Service

A single-node coordination engine for hierarchical metadata, session-backed leases, one-shot watches, committed operation history, and crash recovery.

`245 tests passing` | `Python + FastAPI + SQLite`

## What It Does

- Persistent and ephemeral nodes in a hierarchical namespace
- Session lifecycle with deterministic expiry cleanup
- Version-guarded writes through `expected_version`
- Exclusive leases with monotonic fencing tokens
- One-shot watches with `event_types` filtering
- Committed operation timeline with per-operation lookup
- Startup recovery report plus WAL-backed replay
- Rollback-safe metadata and session persistence paths

## Current Product Surface

### Metadata
- `POST /api/node/create`
- `GET /api/node/get`
- `POST /api/node/set` with optional `expected_version`
- `DELETE /api/node/delete`
- `GET /api/node/exists`
- `GET /api/node/list_children`

### Sessions
- `POST /api/session/open`
- `POST /api/session/heartbeat`
- `POST /api/session/close`
- `GET /api/sessions`
- `GET /api/stream/sessions`

### Watches
- `POST /api/watch/register`
- `GET /api/watch/wait`
- `DELETE /api/watch/unregister`

### Leases
- `POST /api/lease/acquire`
- `GET /api/lease/get`
- `POST /api/lease/release`

### Operations And Recovery
- `GET /api/operations`
- `GET /api/operations/tail`
- `GET /api/operations/{sequence_number}`
- `GET /api/stream/operations`
- `GET /api/recovery/last`

### Visualizer
- `GET /`
- Live node tree
- Committed operations timeline
- Startup recovery summary
- Live session inventory sourced from the backend
- SSE-driven updates for sessions and operations

## Architecture

```text
HTTP API
  -> Coordinator
      -> MetadataTree
      -> SessionManager
      -> WatchManager
      -> OperationLog
      -> Persistence
      -> RecoveryManager
```

The `Coordinator` is the serialization point. Writes are staged, persisted, then applied to in-memory state so persistence failure does not leak partial state into the live tree or session manager.

`OperationLog` now represents committed history, not just provisional intent. The visualizer and history endpoints read from that committed stream, and the control plane now consumes SSE streams instead of polling every second. `RecoveryManager` replays WAL-backed operations on startup, expires old sessions, removes dead ephemerals, clears watches, restores committed history, and only truncates the WAL after successful recovery.

Leases are implemented on top of ephemeral ownership. `lease_token` is derived from committed create order, so downstream consumers can use it as a fencing token.

## Key Behavior

- `expected_version` on `/api/node/set` provides optimistic concurrency control.
- Stale CAS writes return `409 Conflict`.
- Lease acquire and release are owner-checked.
- Watch registration supports `event_types`, so watchers do not fire on unrelated changes.
- Metadata writes and session mutations roll back cleanly if persistence fails.
- Session expiry cleanup is rollback-safe.
- `/api/operations` returns committed operations in sequence order.
- `/api/operations/tail` blocks until a matching committed operation arrives or times out.
- `/api/stream/operations` streams committed operations as SSE.
- `/api/stream/sessions` streams live session inventory as SSE.
- `/api/recovery/last` exposes the last startup recovery report for the current process.

## Setup

```bash
python -m venv .venv
.venv\Scripts\pip.exe install -r requirements.txt
```

If the local environment already exists:

```bash
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Run

```bash
.venv\Scripts\python.exe main.py
```

The API listens on the host and port defined in `config.py`.

## Test

```bash
.venv\Scripts\python.exe -m pytest -q
```

Latest verified local run: `245 passed in 213.36s`.

## Demos

The `demos/` folder still covers the core scenarios:

- session timeout and ephemeral cleanup
- concurrent linearizable clients
- exactly-once watch firing
- crash recovery
- distributed lock behavior
- service discovery
- leader election
- configuration management

## Testing And Quality

- Recovery coverage includes WAL-only replay cases for `SET`, recursive delete, and ephemeral create behavior.
- Atomicity coverage includes metadata writes, session lifecycle failures, and rollback behavior.
- API coverage includes CAS, lease behavior, watch filtering, operation timeline, recovery reporting, session inventory, and SSE stream snapshots.
- Integration coverage includes concurrent behavior and recovery scenarios.

## Honest Limits

- This is still a single-node service.
- Lease TTL is still session timeout, not an independent per-lease TTL.
- Fair queueing for competing lease waiters is not guaranteed.
- Recovery is stronger than before, but it is still SQLite plus a custom WAL, not distributed consensus.
- This is not a drop-in ZooKeeper replacement.

## Roadmap

If we keep pushing this as a product, the next high-value steps are:

1. Session detail and ownership drill-down, not just top-level inventory.
2. Streaming watch delivery over SSE or WebSocket.
3. Richer lease inspection with holder history and contention visibility.
4. More crash-injection tooling around persistence and recovery boundaries.
5. More end-to-end examples that show why this is a coordination engine, not a generic key-value store.

## Project Layout

- `coordinator.py` - orchestration and commit contract
- `metadata_tree.py` - in-memory namespace and delete planning
- `session_manager.py` - session lifecycle and expiry
- `watch_manager.py` - watch registration and firing
- `operation_log.py` - committed sequence history
- `persistence.py` - SQLite durability and WAL handling
- `recovery.py` - crash recovery and restart repair
- `main.py` - FastAPI surface and visualizer entry point
- `tests/` - unit, API, integration, atomicity, and recovery coverage
