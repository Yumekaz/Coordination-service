# Demo Guide

Use these scripts as a story, not a pile of examples. The best order is the one that starts with coordination primitives, moves into correctness, then ends with failure handling.

## Recommended Order

1. `demo_leader_election.py`
2. `demo_distributed_lock.py`
3. `demo_watch_exactly_once.py`
4. `demo_session_timeout.py`
5. `demo_service_discovery.py`
6. `demo_config_management.py`
7. `demo_concurrent_linearizable.py`
8. `demo_crash_recovery.py`
9. `demo_crash_during_cleanup.py`

## What Each Demo Proves

| Script | What it shows | What it proves |
| --- | --- | --- |
| `demo_leader_election.py` | Candidates compete for leadership through ephemeral nodes. | The service can coordinate election-style ownership and fail over when the leader goes away. |
| `demo_distributed_lock.py` | Multiple workers contend for one lock path. | Only one holder can own the lock at a time, and watches let others wait without busy looping. |
| `demo_watch_exactly_once.py` | A watched node changes three times. | Watches fire once per registration, not zero times and not duplicated. |
| `demo_session_timeout.py` | An ephemeral node is tied to a session with no heartbeats. | Session expiry cleans up owned ephemeral state automatically. |
| `demo_service_discovery.py` | Service instances register and disappear as sessions live or die. | The service can act as dynamic registry + discovery for live instances. |
| `demo_config_management.py` | Clients watch config nodes for updates. | Persistent nodes support centralized config with live fan-out and restart-safe state. |
| `demo_concurrent_linearizable.py` | Five clients write the same counter concurrently. | Concurrent writes are serialized into one consistent order with no lost updates. |
| `demo_crash_recovery.py` | The service is stopped and restarted around live data. | Persistent state survives restart, ephemeral state does not, and recovery is deterministic. |
| `demo_crash_during_cleanup.py` | A crash happens in the middle of ephemeral cleanup. | Recovery finishes the cleanup and avoids leaked nodes after a bad shutdown. |

## Best Portfolio Path

If you only run three, run these:

1. `demo_watch_exactly_once.py`
2. `demo_crash_recovery.py`
3. `demo_crash_during_cleanup.py`

That sequence makes the strongest case fast: correctness of notification semantics, then durability, then recovery under failure.

If you want a more product-like walkthrough, use:

1. `demo_leader_election.py`
2. `demo_distributed_lock.py`
3. `demo_service_discovery.py`
4. `demo_config_management.py`

That path sells the system as a coordination engine people can actually build on.

## Notes

- Run each script from the repo root, for example `python demos/demo_crash_recovery.py`.
- The demos are intentionally small and direct. Their job is to prove behavior, not to hide the system behind UI polish.
- For interviews, do not describe this as “just demos.” Describe them as proof points for coordination, failure handling, and recovery.
