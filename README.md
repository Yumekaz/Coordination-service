# Coordination Service

A coordination engine for hierarchical metadata, session-backed leases, one-shot watches, committed operation history, crash recovery, and leader/follower replication with a durable replicated log, quorum-commit flow, leadership handoff, and stronger failover fencing.

`306 tests passing` | `Python + FastAPI + SQLite`

## Fast First Impression

In under 30 seconds: this is not a toy key-value store. It is a coordination engine with a live control plane that shows committed history, causal incidents, session and lease state, follower lag, recovery reports, and cluster health in one place, so failure behavior is visible instead of hidden.

What makes it memorable is the combination of hard backend behavior and inspectability. You can see why a path disappeared, which session owned it, what the watch and recovery story was, and how the cluster responded.

## Try These Demo Flows

- Open `/`, click `Run Demo`, then watch the committed timeline and inspector populate with real state.
- Create a session with `+ Session`, create a node with `+ Node`, then click the session card or a timeline event to inspect the causal chain.
- Open a path incident in the timeline and use the inspector to see blast radius, watch firings, and cleanup details.
- Use the [`demos/README.md`](demos/README.md) guide for the best run order across crash recovery, locks, leader election, and configuration management.

## Operational Playbooks

- Leadership handoff: term-aware heartbeats, log-fresh vote checks, and majority fencing keep the active leader honest while followers stay read-only until they catch up.
- Safe old-leader removal: reconfiguration is staged, committed, and fenced so a removed or stale leader stays out of the write path on restart.
- Cluster chaos coverage: the test suite covers partitions, rejoin, leader crash timing, divergent tails, snapshot rebuilds, quorum rollback, and reconfiguration failure paths.

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
- `POST /api/lease/renew`
- `POST /api/lease/release`

### Operations And Recovery
- `GET /api/operations`
- `GET /api/operations/tail`
- `GET /api/operations/{sequence_number}`
- `GET /api/operations/{sequence_number}/incident`
- `GET /api/operation/detail`
- `GET /api/stream/operations`
- `GET /api/recovery/last`

### Cluster
- `GET /api/cluster/status`
- `POST /api/cluster/transfer-leadership`
- `GET /internal/replication/operations`
- `GET /internal/replication/snapshot`
- `GET /internal/replication/state`
- `GET /internal/replication/watches`
- `POST /internal/replication/append`
- `POST /internal/replication/commit`
- `POST /internal/replication/truncate`
- `POST /internal/replication/prepare`
- `POST /internal/replication/cancel-prepare`
- `POST /internal/replication/apply`
- `POST /internal/cluster/heartbeat`
- `POST /internal/cluster/request-vote`
- `POST /internal/cluster/transfer-leadership`

### Visualizer
- `GET /`
- Live node tree
- Committed operations timeline
- Operation incident inspector with causal chain, blast radius, and affected paths
- Startup recovery summary
- Live session inventory sourced from the backend
- SSE-driven updates for sessions and operations

## Architecture

```text
HTTP API
  -> Coordinator
      -> ClusterManager
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
- Lease TTL can be independent from session timeout and can be renewed by the owner.
- Competing lease waiters are queued in FIFO order instead of racing on delete watches.
- Watch registration supports `event_types`, so watchers do not fire on unrelated changes.
- Metadata writes and session mutations roll back cleanly if persistence fails.
- Session expiry cleanup is rollback-safe.
- Follower replicas catch up from the leader's committed operation history and can accept pushed replication batches from the leader.
- Followers detect divergent local history and can rebuild atomically from a leader snapshot instead of staying silently wrong.
- Snapshot-based follower rebuild preserves persisted watch-fire history for operation incidents after rejoin.
- Leaders can durably append a write batch to a majority before local apply when write quorum is enabled.
- Followers keep a durable replicated log and apply entries only after commit advancement.
- Leaders can roll back uncommitted replicated-log tails when local persistence fails after quorum append.
- Leaders can optionally reject writes when a quorum of peers is not healthy.
- Leaders also reject new writes when prior committed operations have not reached a majority yet.
- Leaders use a majority heartbeat lease and step down when that lease expires.
- Followers track leader term/contact health and can trigger election attempts when the leader goes stale.
- Vote requests compare candidate log freshness instead of only comparing last applied sequence.
- Follower replicas are read-only and report replication lag through `/api/cluster/status`.
- Follower replicas sync active watches from the leader and preserve watch-fire history and path/session postmortem detail after snapshot rebuild and append/commit replay.
- `/api/operations` returns committed operations in sequence order.
- `/api/operations/tail` blocks until a matching committed operation arrives or times out.
- `/api/stream/operations` streams committed operations as SSE.
- `/api/stream/sessions` streams live session inventory as SSE.
- Timeline events can be inspected as first-class incidents with affected-path summaries, watch firings, and cleanup cascades.
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

Latest verified local run: `306 passed in 584.87s (0:09:44)`.

## Demos

The `demos/` folder covers the core scenarios:

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
- API coverage includes CAS, lease behavior, watch filtering, operation timeline, incident reporting, recovery reporting, session inventory, replica snapshot rebuilds, and SSE stream snapshots.
- Integration coverage includes concurrent behavior and recovery scenarios.
- Cluster coverage includes durable append/commit/truncate flow, quorum rollback on local failure, leadership handoff, safe old-leader removal fencing, majority-partition failover, stale-leader rejection after failover, follower restart replay from the replicated log, replicated watch parity, log-freshness vote checks, leader-lease step-down, and follower divergence snapshot rebuilds.

## Honest Limits

- This is stronger than simple leader/follower replication now: it has a durable replicated log and quorum-commit flow when write quorum is enabled.
- It is still not full Raft or ZooKeeper-class distributed consensus. There is no general log-matching/overwrite protocol, no formal joint consensus membership changes, and no claim of split-brain-proof production consensus.
- Leader election and failover are materially harder now because votes consider log freshness and leaders fence themselves with a heartbeat lease, but this is still not a complete consensus proof.
- Recovery is stronger than before, but it is still SQLite plus a custom WAL underneath.
- This is not a drop-in ZooKeeper replacement.

## Roadmap

If we keep pushing this as a product, the next high-value steps are:

1. Streaming watch delivery over SSE or WebSocket.
2. More crash-injection tooling around persistence and recovery boundaries.
3. Timeline filtering and exportable postmortem snapshots.
4. More end-to-end examples that show why this is a coordination engine, not a generic key-value store.
5. Deeper chaos testing and broader follower parity across every observability surface if this graduates further as a distributed system.

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
