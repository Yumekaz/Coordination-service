# Coordination-service clock-jump hardening, 2026-10-23

Scope: only this repository's `cluster.py`, cluster timing tests and documentation. Base revision: `eef961b` (`fix: preserve newer peer acknowledgements across delayed heartbeats`). The initial worktree was clean. No commits, pushes, host clock changes, service management, reboots, other repository edits or campaign-fixture changes were performed.

## Causal evidence

The original implementation used `time.time()` for the local leader lease, leader contact/election age, prepared-write reservation expiry, handoff confirmation and reconfiguration alignment loops. A wall rollback could extend authority/reservations or delay an election/timeout; a forward step could expire them early. Delayed poll/heartbeat protection also compared wall ACK times with request start times, allowing an older result to replace a newer commit ACK after rollback. Wall-derived replication latency could become negative or include an entire clock jump.

`tests/test_cluster_clock_jumps.py` supplies independent fake civil and monotonic clocks only to the `cluster` module. It steps civil time by +/-86,400 seconds while advancing elapsed time explicitly; fake sleeps bound loops without touching the real host clock or clocks used by coordinator maintenance threads.

The first 25 jump cases against unchanged production code yielded **18 failed, 7 passed in 53.72s**, exit 1 (`clock-jumps-before.log`, `clock-jumps-before.xml`). Failures included both directions of leader-lease/election timing, both timeout loops and rollback corrupting a real commit ACK via an older success/failure response. The 7 passing cases were retained, not removed. Coverage was subsequently expanded to 44 jump/lifecycle cases.

Production changes separate private monotonic runtime fields from wall status fields; refresh/clear paths update the corresponding clock fields together. All five successful peer-ACK producers stamp a private monotonic timestamp, which is filtered from public status. Persistence continues to save only authority/configuration/cursor metadata. Restart coverage changes the simulated monotonic epoch while retaining term, vote and pending-configuration fences.

Existing tests keep every authority/health/log-position assertion. Only private clock injection targets changed to the runtime monotonic fields. The existing handoff/removal/restart/joint-majority and delayed-heartbeat regressions remain enabled.

## Commands and results

All commands ran from `/home/yumekaz/Desktop/Coordination-service`, using Python 3.14.4 / pytest 8.4.2. Shell pipelines use `set -o pipefail`; reports and logs are retained here, never under `/tmp`. Existing test-owned transient database fixtures are unchanged.

1. Original-code focused baseline:

   ```bash
   python3 -m pytest -q -p no:cacheprovider tests/test_election_timing.py tests/test_api.py --junitxml=validation-runs/hardening-20261023/baseline-focused.xml 2>&1 | tee validation-runs/hardening-20261023/baseline-focused.log
   ```

   Interrupted, exit 130, after 11 timing cases completed and the first HTTP fixture stalled inside the restricted sandbox. No baseline JUnit file was produced; this is not a passing baseline-suite claim.

2. Original-code jump reproduction (first 25 cases):

   ```bash
   python3 -m pytest -q -p no:cacheprovider tests/test_cluster_clock_jumps.py --junitxml=validation-runs/hardening-20261023/clock-jumps-before.xml 2>&1 | tee validation-runs/hardening-20261023/clock-jumps-before.log
   ```

   **18 failed, 7 passed**, exit 1.

3. First post-fix timing run (25 jump cases + 11 existing timing cases):

   ```bash
   python3 -m pytest -q -p no:cacheprovider tests/test_cluster_clock_jumps.py tests/test_election_timing.py --junitxml=validation-runs/hardening-20261023/clock-jumps-after.xml 2>&1 | tee validation-runs/hardening-20261023/clock-jumps-after.log
   ```

   **36 passed in 80.74s**, exit 0.

4. Sandbox HTTP diagnosis:

   ```bash
   timeout --signal=INT --kill-after=5s 25s python3 -m pytest -vv -p no:cacheprovider -o faulthandler_timeout=10 tests/test_api.py -k test_open_session 2>&1 | tee validation-runs/hardening-20261023/sandbox-http-diagnostic.log
   timeout --signal=INT --kill-after=5s 60s python3 -m pytest -q -p no:cacheprovider tests/test_api.py::TestSessionEndpoints::test_open_session --junitxml=validation-runs/hardening-20261023/http-prerequisite.xml 2>&1 | tee validation-runs/hardening-20261023/http-prerequisite.log
   ```

   First command (sandbox) timed out/was terminated, exit 137; stack capture shows the TestClient fixture waiting on the asyncio portal. Second command (approved host execution) **1 passed in 2.09s**, exit 0, with a pre-existing Starlette/httpx deprecation warning. An intervening focused sandbox attempt (`focused.log`, the same selection as command 5 without the `-host` filenames) also stalled at that fixture and was interrupted, exit 130. Tests were not skipped or weakened to bypass this restriction.

5. Focused cluster regression run (approved host execution; collected before the last 19 added cases):

   ```bash
   timeout --signal=INT --kill-after=15s 1200s python3 -m pytest -q -p no:cacheprovider -o faulthandler_timeout=90 tests/test_api.py::TestClusterEndpoints tests/test_election_timing.py tests/test_cluster_clock_jumps.py --junitxml=validation-runs/hardening-20261023/focused-host.xml 2>&1 | tee validation-runs/hardening-20261023/focused-host.log
   ```

   **82 passed in 299.41s**, exit 0; one pre-existing Starlette/httpx deprecation warning. Includes all 46 cluster endpoint cases plus 11 existing timing and the first 25 new cases.

6. Final expanded timing run (restricted sandbox):

   ```bash
   timeout --signal=INT --kill-after=10s 300s python3 -m pytest -q -p no:cacheprovider -W error::pytest.PytestUnhandledThreadExceptionWarning tests/test_cluster_clock_jumps.py tests/test_election_timing.py --junitxml=validation-runs/hardening-20261023/timing-final.xml 2>&1 | tee validation-runs/hardening-20261023/timing-final.log
   ```

   **55 passed in 134.37s**, exit 0, no warnings. All 44 new jump/lifecycle cases and 11 existing timing cases executed; thread exceptions were errors.

7. Complete suite, no exclusions (approved host execution):

   ```bash
   timeout --signal=INT --kill-after=15s 1200s python3 -m pytest -q -p no:cacheprovider -W error::pytest.PytestUnhandledThreadExceptionWarning -o faulthandler_timeout=90 --junitxml=validation-runs/hardening-20261023/full.xml 2>&1 | tee validation-runs/hardening-20261023/full.log
   ```

   Result pending at record creation; final result will be recorded below.

An initial expanded timing run (same command as 6 without `-W`, retained as `timing-initial.log`/`.xml`) passed 55 cases in 134.06s but reported two fake-clock worker-thread warnings. The configure-contact case had auto-started a background replication loop. Its test now stubs only the instance's automatic `start()` so this isolated fake-clock case remains synchronous; real threaded reconfiguration remains covered by the unmodified API tests. A dedicated startup timing case remains enabled. Command 6 and the restarted complete run promote thread-exception warnings to errors. The first complete run was intentionally interrupted (exit 130; `full-initial.log`/`.xml`) to run this corrected harness from a clean collection; it is not a complete-suite result.

8. Read-only source/evidence audit:

   ```bash
   python3 validation-runs/hardening-20261023/audit_results.py focused-host.xml
   git diff --check
   ```

   Passed: AST comparison against `eef961b` finds all **587 API assertions and 27 existing timing assertions unchanged**. No wall-clock calls/status fields remain in cluster comparisons. The focused JUnit report contains 82 cases, zero failures/errors/skips. Output: `static-focused-audit.log`. The same audit checks the final timing/full reports, exact complete-suite count, all new cases and mandatory handoff/removal/heartbeat/fencing regressions.

## Limits

Main's final complete revalidation (`main-full.log` and `main-full.xml`) passed
**367 tests in 646.75s**, exit 0, with zero failures/errors/skips. Unhandled test
thread exceptions were promoted to errors. The one warning is the pre-existing
Starlette/httpx deprecation, not a hidden background exception. The final CI
workflow now requires all 367 cases, exactly 44 clock-jump cases, the retained
heartbeat regressions, zero skips and the same thread-exception policy on
Python 3.12 and 3.14. Actual pushed-head results remain separately required.

This remains the existing custom term/vote, durable-append quorum and conservative membership model, not a consensus implementation or proof. No term/vote/log-freshness, joint-majority, removal, restart or read-only fences were relaxed. No distributed clock synchronization or cross-host monotonic comparison is introduced. Session/client-lease TTLs outside cluster bookkeeping are unchanged. Monotonic clock-rate drift, host suspension, arbitrary pauses and RPC scheduling/in-flight-request overshoot are not solved by this change. Existing bootstrap and quorum-mode policy are unchanged. No multi-host campaign, physical clock adjustment or reboot was attempted.
