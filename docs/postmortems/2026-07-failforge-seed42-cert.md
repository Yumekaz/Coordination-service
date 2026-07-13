# FailForge Coordination seed 42 — certification note

**Date**: 2026-07-13  
**Scope**: FailForge + Coordination-service (portable launch path)  
**Outcome**: **PASSED** (0 ERROR checker violations)

## What was run

```bash
cd FAILFORGE
go build -o bin/failforge ./cmd/failforge
# sibling ../Coordination-service (or COORD_ROOT)
./bin/failforge run failforge_coordination.yml --seed 42
./bin/failforge report runs/coordination-42
```

- **Config**: `failforge_coordination.yml` (portable `python3 "${COORD_ROOT:-../Coordination-service}/main.py"`)
- **Run ID**: `run-1783935202433599189`
- **Checkers**: `lock_exclusivity`, `no_two_leaders`
- **Fault profile**: seeded_random, max 3 (restart_node, kill_node, partition)
- **Result**: process completed successfully; **0 ERROR** severity violations (2 info-level fault_injector events only)

## Limits (honest)

- High operation-fail rate under kill/restart is expected; checkers evaluate successful ops and exclusivity/leadership invariants.
- Proxy **does** intercept HTTP peer traffic for Coordination (unlike MiniDB TCP path).
- This cert does not claim multi-hour soak or multi-datacenter behavior.

## Related

- MiniDB seed 42 residual closed (5/5 zero ERROR):  
  [Mini-Redis-Cassandra postmortem](https://github.com/Yumekaz/Mini-Redis-Cassandra/blob/main/docs/postmortems/2026-07-failforge-seed42-raw.md)
- Stack map: [Cairn docs/STACK.md](https://github.com/Yumekaz/Cairn/blob/main/docs/STACK.md)
