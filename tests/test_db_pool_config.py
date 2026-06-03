import pytest

from fusion_council_service import db as db_module


@pytest.fixture(autouse=True)
def _reset_db_globals(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:***@localhost:5432/testdb")
    monkeypatch.setenv("POSTGRES_PASSWORD", "supersecret")
    monkeypatch.delenv("DATABASE_PATH", raising=False)
    monkeypatch.setattr(db_module, "_engine", None)
    monkeypatch.setattr(db_module, "_SessionFactory", None)
    monkeypatch.setattr(db_module, "_is_postgresql", False)


def test_postgres_pool_is_capped_for_three_pod_deployment(monkeypatch):
    captured = {}

    def fake_create_engine(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(db_module, "create_engine", fake_create_engine)
    monkeypatch.setattr(db_module, "sessionmaker", lambda **kwargs: object())

    db_module.get_engine()

    assert captured["url"] == "postgresql://test:supersecret@localhost:5432/testdb"
    assert captured["kwargs"]["pool_size"] == 3
    assert captured["kwargs"]["max_overflow"] == 5
    assert captured["kwargs"]["pool_timeout"] == 30
