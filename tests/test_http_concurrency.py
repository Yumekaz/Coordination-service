"""Blocking coordination work must not stall the ASGI event loop."""
import asyncio
import threading

import httpx
import main


def test_health_responds_while_watch_wait_is_blocked(monkeypatch):
    entered, release = threading.Event(), threading.Event()

    class FakeCoordinator:
        def wait_watch(self, watch_id, timeout):
            entered.set()
            release.wait(5.0)
            return None

        def get_health(self):
            return {"status": "healthy", "nodes_count": 0, "sessions_count": 0,
                    "uptime_seconds": 1}

    monkeypatch.setattr(main, "coordinator", FakeCoordinator())

    async def exercise():
        transport = httpx.ASGITransport(app=main.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            waiting = asyncio.create_task(client.get("/api/watch/wait?watch_id=test&timeout_seconds=1"))
            try:
                assert await asyncio.to_thread(entered.wait, 1.0)
                assert not waiting.done(), "blocking route occupied the event loop"
                health = await asyncio.wait_for(client.get("/api/health"), 1.0)
                assert health.status_code == 200
                assert not waiting.done(), "health only returned after the blocking watch ended"
            finally:
                release.set()
                await waiting

    asyncio.run(exercise())
