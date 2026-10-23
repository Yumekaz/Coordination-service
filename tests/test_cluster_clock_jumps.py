"""Process-local cluster timing must ignore discontinuous civil time."""

from types import SimpleNamespace

import pytest

import cluster
from cluster import ClusterManager
from errors import ConflictError
from models import Operation, OperationType


class Clock:
    """Independent wall/elapsed clocks; never patch clocks used by other threads."""

    def __init__(self):
        self.wall = 1_700_000_000.0
        self.elapsed = 100.0
        self.sleep_jump = 0.0
        self.sleeps = 0

    def time(self):
        return self.wall

    def monotonic(self):
        return self.elapsed

    def advance(self, seconds):
        self.wall += seconds
        self.elapsed += seconds

    def sleep(self, seconds):
        self.sleeps += 1
        assert self.sleeps < 100, "wall-clock rollback made the wait unbounded"
        self.advance(seconds)
        self.wall += self.sleep_jump
        self.sleep_jump = 0.0


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(cluster, "time", clock)
    return clock


def unavailable(*args):
    raise ConnectionError("isolated peers")


@pytest.mark.parametrize("jump", [-86400.0, 86400.0])
def test_leader_write_fence_uses_elapsed_lease(coordinator, clock, jump):
    manager = ClusterManager(
        coordinator, role="leader", peer_urls=["http://b"],
        require_write_quorum=True, request_json=unavailable,
    )
    try:
        wall_deadline = manager.build_status()["leader_lease_until"]
        clock.advance(1.0)
        clock.wall += jump
        status = manager.build_status()
        assert status["leader_lease_until"] == wall_deadline
        assert not status["leader_lease_expired"], "wall jump expired a live lease"
        clock.advance(2.0)
        assert manager.build_status()["leader_lease_expired"], "wall rollback extended authority"
        with pytest.raises(ConflictError) as exc:
            coordinator.open_session(30)
        assert exc.value.error == "leader_lease_expired"
        assert manager.build_status()["role"] == "candidate"
        assert coordinator.get_read_only_status()["read_only"]
    finally:
        manager.stop()


@pytest.mark.parametrize("jump", [-86400.0, 86400.0])
def test_heartbeat_renews_elapsed_lease_then_partition_demotes(coordinator, clock, jump):
    manager = ClusterManager(
        coordinator, role="leader", peer_urls=["http://b"],
        request_json=lambda *args: {"accepted": True, "term": 1, "last_applied": 0},
    )
    clock.advance(2.0)
    clock.wall += jump
    manager._send_heartbeats_once()
    assert manager.build_status()["leader_lease_until"] == clock.time() + 3.0
    clock.advance(2.0)
    assert not manager.build_status()["leader_lease_expired"]
    manager._request_json = unavailable
    clock.wall -= jump * 2
    clock.advance(1.0)
    manager._send_heartbeats_once()
    assert manager.build_status()["role"] == "candidate"
    assert coordinator.get_read_only_status()["read_only"]


@pytest.mark.parametrize("jump", [-86400.0, 86400.0])
@pytest.mark.parametrize("contact", ["heartbeat", "vote", "catchup", "apply", "append", "commit", "truncate", "configure"])
def test_election_contact_and_retry_ignore_wall_jumps(coordinator, clock, monkeypatch, jump, contact):
    manager = ClusterManager(
        coordinator, node_id="b", role="follower", leader_url="http://a",
        request_json=lambda *args: {"node_id": "a", "role": "leader", "term": 1,
                                   "commit_index": 0, "operations": [], "watches": []},
    )
    clock.advance(10.0)
    if contact == "heartbeat":
        assert manager.receive_heartbeat("a", 1, "http://a", 0)["accepted"]
    elif contact == "vote":
        assert manager.request_vote("a", 1, 0)["vote_granted"]
    elif contact == "catchup":
        assert manager.catch_up_once() == 0
    elif contact == "apply":
        assert manager.apply_replication_batch("a", 1, [])["status"] == "ok"
    elif contact == "append":
        assert manager.receive_append("a", 1, "http://a", 0, 0, [
            Operation(1, OperationType.CREATE, path="/one", term=1).to_dict(),
        ])["accepted"]
    elif contact == "commit":
        assert manager.receive_commit("a", 1, "http://a", 0)["accepted"]
    elif contact == "truncate":
        assert manager.receive_truncate("a", 1, 0)["accepted"]
    else:
        # Configure normally auto-starts catch-up. Keep this clock-stepping
        # test synchronous; real threaded reconfiguration is covered by the
        # existing API integration tests, and start timing is tested below.
        monkeypatch.setattr(manager, "start", lambda: None)
        assert manager.receive_configure(
            leader_id="a", leader_url="http://a", term=1, config_version=2,
            cluster_urls=["http://a", "http://b"], node_url="http://b", phase="stage",
        )["accepted"]
    assert manager.build_status()["last_leader_contact_at"] == clock.time()
    clock.advance(1.0)
    clock.wall += jump
    assert not manager._leader_contact_stale_locked()
    assert not manager._automatic_election_due_locked()
    clock.advance(6.0)
    assert manager._leader_contact_stale_locked()
    assert manager._automatic_election_due_locked()


@pytest.mark.parametrize("jump", [-86400.0, 86400.0])
def test_unconsumed_prepare_expires_without_wall_clock_progress(coordinator, clock, jump):
    manager = ClusterManager(coordinator, role="follower", prepare_timeout_seconds=5.0)
    assert manager.receive_prepare("a", 1, None, 1)["accepted"]
    clock.wall += jump
    clock.advance(5.0)
    manager._clear_expired_prepare_locked()
    assert manager._prepared_sequence is None


@pytest.mark.parametrize("jump", [-86400.0, 86400.0])
def test_prepare_reservation_expiry_and_refresh_use_elapsed_time(coordinator, clock, jump):
    manager = ClusterManager(coordinator, role="follower", prepare_timeout_seconds=5.0)
    assert manager.receive_prepare("a", 1, None, 1, count=3)["accepted"]
    clock.advance(1.0)
    clock.wall += jump
    manager._clear_expired_prepare_locked()
    assert manager._prepared_sequence == 1, "wall jump discarded a live reservation"
    operation = Operation(1, OperationType.CREATE, path="/one", term=1)
    manager._advance_prepare_reservation_locked("a", 1, [operation])
    clock.advance(4.0)
    manager._clear_expired_prepare_locked()
    assert manager._prepared_sequence == 2
    clock.wall -= jump * 2
    clock.advance(1.0)
    manager._clear_expired_prepare_locked()
    assert manager._prepared_sequence is None, "wall rollback retained an expired reservation"
    with pytest.raises(ConflictError) as exc:
        manager.apply_replication_batch(
            "a", 1, [Operation(2, OperationType.CREATE, path="/two", term=1).to_dict()],
            prepared_write=True,
        )
    assert exc.value.error == "replication_prepare_mismatch"


@pytest.mark.parametrize("jump", [-86400.0, 86400.0])
def test_peer_alignment_timeout_is_elapsed_and_bounded(coordinator, clock, jump):
    manager = ClusterManager(coordinator, role="follower", request_json=lambda *args: {})
    clock.sleep_jump = jump
    began = clock.monotonic()
    with pytest.raises(ConflictError) as exc:
        manager._wait_for_peer_alignment(
            peer_url="http://b", expected_applied=1, expected_log_index=1, expected_log_term=1,
        )
    assert exc.value.error == "cluster_reconfig_peer_catchup_timeout"
    assert manager._reconfig_timeout_seconds <= clock.monotonic() - began < manager._reconfig_timeout_seconds + 0.3


@pytest.mark.parametrize("jump", [-86400.0, 86400.0])
def test_handoff_wait_timeout_is_elapsed_and_bounded(coordinator, clock, monkeypatch, jump):
    manager = ClusterManager(coordinator, role="follower")
    # Lose leadership immediately after a successful election: exercise the
    # handoff confirmation wait without bypassing its authority checks.
    monkeypatch.setattr(manager, "_start_election_once", lambda **kw: {"status": "leader"})
    clock.sleep_jump = jump
    began = clock.monotonic()
    result = manager.receive_leadership_transfer(
        leader_id="a", leader_url="http://a", term=1, config_version=1,
        target_last_applied=0, target_last_log_index=0, target_last_log_term=0,
        timeout_seconds=0.5,
    )
    assert not result["accepted"]
    assert result["reason"] == "transfer_timeout"
    assert 0.5 <= clock.monotonic() - began < 0.6


@pytest.mark.parametrize("jump", [-86400.0, 86400.0])
@pytest.mark.parametrize("observation", ["poll", "heartbeat"])
@pytest.mark.parametrize("response_fails", [False, True])
def test_delayed_observation_preserves_real_commit_ack_across_jump(
    coordinator, clock, jump, observation, response_fails,
):
    manager = None

    def request(url, method, payload):
        if url.endswith("/apply"):
            return {"last_applied": 7, "applied_count": 1}
        # A real commit ACK completes while the earlier observation is in
        # flight. Its wall timestamp can be either behind or ahead of it.
        clock.advance(0.25)
        clock.wall += jump
        manager._on_leader_commit(Operation(7, OperationType.CREATE, path="/seven"))
        if response_fails:
            raise ConnectionError("delayed failure")
        return {"accepted": True, "term": 1, "node_id": "b", "last_applied": 6}

    manager = ClusterManager(coordinator, role="leader", peer_urls=["http://b"], request_json=request)
    if observation == "poll":
        manager._poll_peers_once()
    else:
        manager._send_heartbeats_once()
    peer = manager.build_status()["peers"][0]
    assert peer["healthy"] is True
    assert peer["last_applied"] == 7
    assert peer["last_ack_at"] == clock.time()
    assert not any("monotonic" in key for key in peer)
    assert manager.build_status()["recent_apply"][0]["latency_ms"] == 0


@pytest.mark.parametrize("jump", [-86400.0, 86400.0])
def test_replication_latency_is_elapsed_but_apply_timestamp_is_wall(coordinator, clock, jump):
    def respond(*args):
        clock.advance(0.125)
        clock.wall += jump
        return {"last_applied": 1}

    manager = ClusterManager(coordinator, role="leader", peer_urls=["http://b"], request_json=respond)
    manager._on_leader_commit(Operation(1, OperationType.CREATE, path="/one"))
    event = manager.build_status()["recent_apply"][0]
    assert event["latency_ms"] == 125
    assert event["applied_at"] == clock.time()


@pytest.mark.parametrize("jump", [-86400.0, 86400.0])
def test_jump_cannot_replace_pending_majority_or_newer_term(coordinator, clock, jump):
    def respond(url, method, payload):
        return {"accepted": url.startswith("http://b/"), "term": 1, "last_applied": 0}

    manager = ClusterManager(
        coordinator, node_id="a", role="leader", peer_urls=["http://b"], request_json=respond,
    )
    with manager._lock:
        manager._pending_config_version = 2
        manager._pending_peer_urls = ["http://b", "http://c", "http://d"]
        manager._reconfig_in_progress = True
        manager._peer_states = manager._rebuild_peer_states(manager._pending_peer_urls)
    clock.wall += jump
    clock.advance(3.0)
    manager._send_heartbeats_once()
    assert manager.build_status()["role"] == "candidate"
    assert manager.build_status()["reconfig_in_progress"]
    assert coordinator.get_read_only_status()["read_only"]
    # Timing cannot clear the durable joint-configuration authority fence.
    assert manager.request_vote("c", 2, 0, candidate_config_version=2)["reason"] == "cluster_reconfig_in_progress"


@pytest.mark.parametrize("jump", [-86400.0, 86400.0])
def test_inflight_heartbeat_cannot_reacquire_lease_after_higher_term(coordinator, clock, jump):
    sent_terms = []
    manager = None

    def respond(url, method, payload):
        sent_terms.append(payload["term"])
        if len(sent_terms) == 1:
            clock.advance(0.25)
            clock.wall += jump
            assert manager.request_vote("b", 2, 0)["vote_granted"]
        return {"accepted": True, "term": 1, "last_applied": 0}

    manager = ClusterManager(
        coordinator, node_id="a", role="leader", peer_urls=["http://b", "http://c"], request_json=respond,
    )
    manager._send_heartbeats_once()
    assert sent_terms == [1, 1]
    status = manager.build_status()
    assert status["current_term"] == 2
    assert status["role"] == "follower"
    assert status["leader_lease_until"] is None
    assert manager._leader_lease_deadline_monotonic is None
    assert coordinator.get_read_only_status()["read_only"]


def test_start_resets_election_elapsed_clock_even_at_zero(coordinator, clock, monkeypatch):
    class IdleThread:
        def __init__(self, **kwargs):
            pass

        def start(self):
            pass

        def join(self, **kwargs):
            pass

    clock.elapsed = 0.0
    manager = ClusterManager(coordinator, role="follower")
    assert not manager._leader_contact_stale_locked()
    assert not manager._automatic_election_due_locked()
    clock.advance(30.0)
    clock.wall -= 86400.0
    monkeypatch.setattr(cluster, "threading", SimpleNamespace(Thread=IdleThread))
    manager.start()
    try:
        assert not manager._leader_contact_stale_locked()
        assert not manager._automatic_election_due_locked()
        clock.advance(6.1)
        assert manager._automatic_election_due_locked()
    finally:
        manager.stop()


def test_restart_keeps_durable_fences_but_not_process_clock_authority(coordinator, clock):
    manager = ClusterManager(
        coordinator, node_id="a", role="leader", peer_urls=["http://b"],
        advertise_url="http://a", request_json=unavailable,
    )
    manager._current_term = 7
    manager._voted_for = "a"
    manager._pending_config_version = 2
    manager._pending_peer_urls = ["http://b", "http://c"]
    manager._reconfig_in_progress = True
    with manager._lock:
        manager._persist_cluster_state_locked()
    saved = coordinator.load_cluster_state("a")
    assert saved["current_term"] == 7
    assert not any(any(word in key for word in ("monotonic", "deadline", "lease_until", "contact")) for key in saved)
    # Simulate a new boot with an unrelated monotonic epoch and civil time.
    clock.elapsed = 2.0
    clock.wall -= 86400.0
    restarted = ClusterManager(coordinator, node_id="a", role="leader", request_json=unavailable)
    status = restarted.build_status()
    assert status["role"] == "follower"
    assert status["leader_lease_until"] is None
    assert status["current_term"] == 7
    assert status["voted_for"] == "a"
    assert status["reconfig_in_progress"]
    assert restarted.trigger_election()["status"] == "reconfiguring"
    assert restarted.request_vote("b", 8, 0)["reason"] == "cluster_reconfig_in_progress"
