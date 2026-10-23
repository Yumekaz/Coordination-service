"""Read-only checks of retained JUnit evidence and unchanged old assertions."""

import ast
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]
RUN = Path(__file__).resolve().parent


def assertions(source):
    return [ast.dump(node) for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Assert)]


for relative in ("tests/test_api.py", "tests/test_election_timing.py"):
    original = subprocess.check_output(
        ["git", "show", f"eef961b:{relative}"], cwd=ROOT, text=True,
    )
    current = (ROOT / relative).read_text()
    assert assertions(original) == assertions(current), f"existing assertions changed: {relative}"
    print(f"UNCHANGED assertions: {relative} ({len(assertions(current))})")

tree = ast.parse((ROOT / "cluster.py").read_text())
wall_fields = {"_last_leader_contact_at", "_leader_lease_until"}
for comparison in (node for node in ast.walk(tree) if isinstance(node, ast.Compare)):
    for node in ast.walk(comparison):
        assert not (isinstance(node, ast.Attribute) and node.attr in wall_fields), (
            f"wall status field controls comparison at line {comparison.lineno}"
        )
        assert not (
            isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name) and node.func.value.id == "time"
            and node.func.attr == "time"
        ), f"wall clock controls comparison at line {comparison.lineno}"
print("PASS: no wall-clock calls/status fields in cluster comparisons")

for filename in sys.argv[1:]:
    report = ET.parse(RUN / filename).getroot()
    cases = report.findall(".//testcase")
    failures = report.findall(".//failure")
    errors = report.findall(".//error")
    skipped = report.findall(".//skipped")
    print(f"{filename}: cases={len(cases)}, failures={len(failures)}, errors={len(errors)}, skipped={len(skipped)}")
    assert cases and not failures and not errors and not skipped, f"incomplete/failing report: {filename}"
    names = {case.attrib["name"] for case in cases}
    if filename.endswith("full.xml"):
        assert len(cases) == 367, f"expected all 323 original + 44 new cases; got {len(cases)}"
        required = {
            "test_transfer_leadership_allows_safe_old_leader_removal",
            "test_majority_partition_elects_new_leader_and_old_leader_stays_fenced_after_heal",
            "test_remove_follower_decommissions_removed_peer_and_shrinks_cluster",
            "test_removed_peer_restart_stays_decommissioned",
            "test_reconfig_joint_quorum_lease_requires_pending_majority",
            "test_reconfigure_rollback_failure_persists_restart_fence_and_stale_config_rejection",
            "test_old_heartbeat_cannot_replace_newer_commit_ack[False]",
            "test_old_heartbeat_cannot_replace_newer_commit_ack[True]",
            "test_fresh_heartbeat_reports_lower_peer_state_instead_of_hiding_it",
        }
        assert required <= names, f"missing authority regressions: {required - names}"
        new_cases = [case for case in cases if "test_cluster_clock_jumps" in case.attrib.get("classname", "")]
        assert len(new_cases) == 44, f"clock-jump coverage incomplete: {len(new_cases)}"
        print("PASS: all 44 new cases and required handoff/removal/heartbeat/fencing regressions executed")
