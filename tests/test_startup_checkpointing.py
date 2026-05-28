"""Tests for startup checkpointer bootstrap behavior."""

import pytest

from fusion_council_service.config import Settings


class _FakeAsyncSaver:
    setup_calls = 0

    async def setup(self):
        type(self).setup_calls += 1


class _FakeCM:
    def __init__(self, saver):
        self._saver = saver
        self.closed = False

    async def __aenter__(self):
        return self._saver

    async def __aexit__(self, exc_type, exc, tb):
        self.closed = True


@pytest.mark.asyncio
async def test_run_startup_noop_when_checkpoint_disabled():
    from fusion_council_service.startup import get_checkpoint_saver, run_shutdown, run_startup

    settings = Settings(
        DATABASE_URL="postgresql://u:p@h/db",
        SERVICE_API_KEYS="k1",
        SERVICE_ADMIN_API_KEYS="a1",
        LANGGRAPH_CHECKPOINT_ENABLED=False,
    )
    await run_startup(settings)
    assert get_checkpoint_saver() is None
    await run_shutdown()


@pytest.mark.asyncio
async def test_run_startup_initializes_async_postgres_saver(monkeypatch):
    from fusion_council_service.startup import get_checkpoint_saver, run_shutdown, run_startup

    fake_saver = _FakeAsyncSaver()
    fake_cm = _FakeCM(fake_saver)

    class _Factory:
        @staticmethod
        def from_conn_string(_db_url):
            return fake_cm

    monkeypatch.setattr("fusion_council_service.startup.AsyncPostgresSaver", _Factory)

    settings = Settings(
        DATABASE_URL="postgresql://u:p@h/db",
        SERVICE_API_KEYS="k1",
        SERVICE_ADMIN_API_KEYS="a1",
        LANGGRAPH_CHECKPOINT_ENABLED=True,
        LANGGRAPH_CHECKPOINT_DB_URL="postgresql://u:p@h/db",
    )
    await run_startup(settings)
    assert get_checkpoint_saver() is fake_saver
    assert _FakeAsyncSaver.setup_calls >= 1

    await run_shutdown()
    assert fake_cm.closed is True

