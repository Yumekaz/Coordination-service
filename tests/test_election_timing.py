"""Regression coverage for campaign startup and distinct membership votes."""
import time
import threading
import pytest

from cluster import ClusterManager


def test_initial_membership_excludes_self_and_duplicate_peers(coordinator):
    manager = ClusterManager(coordinator, node_id="a", role="follower",
                             advertise_url="http://a/",
                             peer_urls=["http://a", "http://b", "http://b/", "http://c"])
    assert manager._peer_urls == ["http://b", "http://c"]
    assert manager.build_status()["quorum_size"] == 2


def test_a_node_cannot_elect_itself_by_counting_its_url_as_a_peer(coordinator):
    requested = []

    def unavailable_peer(url, method, payload):
        requested.append(url)
        raise ConnectionError("isolated remote peers")

    manager = ClusterManager(coordinator, node_id="a", role="follower",
                             advertise_url="http://a", peer_urls=["http://a", "http://b"],
                             request_json=unavailable_peer)
    result = manager.trigger_election()
    assert result["votes"] == 1
    assert result["status"] == "candidate"
    assert all(not url.startswith("http://a/") for url in requested)


def test_automatic_retry_deadlines_differ_after_a_split_vote(coordinator, monkeypatch):
    deadlines = []
    for node_id in ("a", "b", "c"):
        manager = ClusterManager(coordinator, node_id=node_id, role="follower",
                                 election_timeout_seconds=1.0,
                                 heartbeat_interval_seconds=0.2)
        manager._role = "candidate"
        manager._current_term = 1
        manager._last_leader_contact_monotonic = 100.0
        due_at = None
        for hundredths in range(101, 201):
            now = 100.0 + hundredths / 100.0
            monkeypatch.setattr(time, "monotonic", lambda now=now: now)
            if manager._automatic_election_due_locked():
                due_at = now
                break
        deadlines.append(due_at)
    assert all(deadline is not None for deadline in deadlines)
    assert len(set(deadlines)) > 1


def test_cluster_persistence_does_not_wait_for_coordinator_write_lock(coordinator):
    manager = ClusterManager(coordinator, node_id="a", role="follower")
    held, release, completed = threading.Event(), threading.Event(), threading.Event()

    def hold_write_lock():
        with coordinator._lock:
            held.set()
            release.wait(3.0)

    def persist_cluster_state():
        with manager._lock:
            manager._persist_cluster_state_locked()
        completed.set()

    writer = threading.Thread(target=hold_write_lock)
    persister = threading.Thread(target=persist_cluster_state)
    writer.start()
    try:
        assert held.wait(1.0)
        persister.start()
        assert completed.wait(1.0), "cluster lock waited for the write lock, allowing lock inversion"
    finally:
        release.set()
        writer.join(3.0)
        if persister.ident is not None:
            persister.join(3.0)


def test_old_peer_poll_cannot_replace_newer_commit_ack(coordinator):
    manager = None

    def delayed_poll(url, method, payload):
        # Reproduce the response from a read that started before a commit.
        manager._peer_states["http://b"] = {
            "healthy": True, "last_applied": 7, "last_ack_at": time.time() + 0.001,
            "_last_ack_monotonic": time.monotonic() + 0.001,
        }
        return {"node_id": "b", "last_applied": 6}

    manager = ClusterManager(coordinator, node_id="a", role="leader",
                             peer_urls=["http://b"], request_json=delayed_poll)
    manager._poll_peers_once()
    assert manager._peer_states["http://b"]["last_applied"] == 7


@pytest.mark.parametrize("response_fails", [False, True])
def test_old_heartbeat_cannot_replace_newer_commit_ack(coordinator, response_fails):
    manager = None

    def delayed_heartbeat(url, method, payload):
        # A commit or authoritative poll completes while this earlier
        # heartbeat response is still in flight. Its stale result must not
        # make an already-applied peer appear behind during reconfiguration.
        manager._peer_states["http://b"] = {
            "healthy": True, "last_applied": 7, "last_log_index": 7,
            "last_ack_at": time.time() + 0.001,
            "_last_ack_monotonic": time.monotonic() + 0.001,
        }
        if response_fails:
            raise ConnectionError("old heartbeat failed after newer acknowledgement")
        return {"accepted": True, "term": 0, "node_id": "b", "last_applied": 6,
                "last_log_index": 6}

    manager = ClusterManager(coordinator, node_id="a", role="leader",
                             peer_urls=["http://b"], request_json=delayed_heartbeat)
    manager._send_heartbeats_once()
    assert manager._peer_states["http://b"]["last_applied"] == 7
    assert manager._peer_states["http://b"]["last_log_index"] == 7
    assert manager._peer_states["http://b"]["healthy"] is True


def test_fresh_heartbeat_reports_lower_peer_state_instead_of_hiding_it(coordinator):
    manager = ClusterManager(
        coordinator, node_id="a", role="leader", peer_urls=["http://b"],
        request_json=lambda *args: {"accepted": True, "term": 0, "node_id": "b",
                                   "last_applied": 0, "last_log_index": 0},
    )
    manager._peer_states["http://b"] = {
        "healthy": True, "last_applied": 7, "last_log_index": 7,
        "last_ack_at": time.time() - 1,
        "_last_ack_monotonic": time.monotonic() - 1,
    }
    manager._send_heartbeats_once()
    assert manager._peer_states["http://b"]["last_applied"] == 0
    assert manager._peer_states["http://b"]["last_log_index"] == 0


def test_reserved_operation_does_not_advance_committed_cursor(coordinator):
    from models import OperationType
    before = coordinator.get_committed_sequence()
    operation = coordinator.reserve_internal_operation(operation_type=OperationType.SESSION_OPEN)
    assert operation.sequence_number == before + 1
    assert coordinator.get_committed_sequence() == before
    assert coordinator.rollback_reserved_internal_operation(operation.sequence_number)
    session = coordinator.open_session(30)
    assert session.session_id
    assert coordinator.get_committed_sequence() == before + 1


def test_inflight_old_leader_heartbeat_cannot_claim_new_term(coordinator):
    sent_terms = []
    manager = None
    def peer_response(url, method, payload):
        sent_terms.append(payload["term"])
        if len(sent_terms) == 1:
            # A concurrent request_vote demotes the sender while the first
            # heartbeat is in flight. Remaining messages retain term 1.
            manager._current_term = 2
            manager._role = "follower"
            manager._voted_for = "b"
        return {"accepted": True, "term": 1, "node_id": "peer", "last_applied": 0}
    manager = ClusterManager(coordinator, node_id="a", role="leader",
                             peer_urls=["http://b", "http://c"], request_json=peer_response)
    manager._current_term = 1
    manager._send_heartbeats_once()
    assert sent_terms == [1, 1]
    assert manager._role == "follower"
    assert manager._current_term == 2


def test_catchup_from_demoted_leader_cannot_cast_a_phantom_vote(coordinator):
    manager = ClusterManager(coordinator, node_id="c", role="follower",
                             leader_url="http://a",
                             request_json=lambda *args: {"node_id":"a", "role":"follower", "term":2, "commit_index":0, "operations":[]})
    manager._current_term = 1
    manager._voted_for = "a"
    assert manager.catch_up_once() == 0
    assert manager._current_term == 1
    response=manager.request_vote(candidate_id="b",term=2,candidate_last_applied=0)
    assert response["vote_granted"]
    assert manager._voted_for == "b"
