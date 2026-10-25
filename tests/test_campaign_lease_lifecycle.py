"""Seed-43's token-88 -> TTL cleanup -> token-125 lifecycle, without sleeps."""

from datetime import datetime

import pytest

import coordinator as coordinator_module
from coordinator import Coordinator
from errors import ConflictError, ForbiddenError


def test_reacquire_is_not_renewal_and_expired_owner_cannot_release_successor(
    tmp_path, monkeypatch
):
    # Drive the reaper explicitly. No background timing, network fixture, or
    # election is needed to isolate the semantics observed in the campaign.
    coord = Coordinator(str(tmp_path / "campaign-lease.db"))
    now = [datetime.now().timestamp()]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromtimestamp(now[0], tz)

    monkeypatch.setattr(coordinator_module, "datetime", Clock)
    try:
        first_session = coord.open_session(timeout_seconds=60).session_id
        next_session = coord.open_session(timeout_seconds=60).session_id
        first = coord.acquire_lease(
            "/locks/lock-b", first_session, holder="client-1", lease_ttl_seconds=10
        )

        now[0] += 8.6
        repeated = coord.acquire_lease(
            "/locks/lock-b", first_session, holder="client-1", lease_ttl_seconds=10
        )
        assert repeated["lease_token"] == first["lease_token"]
        assert repeated["acquired_at"] == first["acquired_at"]
        assert repeated["expires_at"] == first["expires_at"]
        with pytest.raises(ConflictError):
            coord.acquire_lease("/locks/lock-b", next_session, lease_ttl_seconds=10)

        # Expiry cleanup still needs the normal write guard/quorum path. A
        # failed cleanup must not remove the old grant or admit a successor.
        now[0] = first["expires_at"] + 0.1

        def unavailable_quorum():
            raise ValueError("Write quorum unavailable")

        coord.set_write_guard(unavailable_quorum)
        coord._expire_due_leases()
        assert coord.get_lease("/locks/lock-b")["lease_token"] == first["lease_token"]
        with pytest.raises(ConflictError):
            coord.acquire_lease("/locks/lock-b", next_session, lease_ttl_seconds=10)

        coord.set_write_guard(None)
        coord._expire_due_leases()
        assert coord.get_lease("/locks/lock-b") is None
        assert coord._session_manager.is_alive(first_session)
        successor = coord.acquire_lease(
            "/locks/lock-b", next_session, holder="client-3", lease_ttl_seconds=10
        )
        assert successor["lease_token"] > first["lease_token"]
        with pytest.raises(ForbiddenError):
            coord.release_lease("/locks/lock-b", first_session)
        assert coord.get_lease("/locks/lock-b")["lease_token"] == successor["lease_token"]
    finally:
        coord._persistence.close()
