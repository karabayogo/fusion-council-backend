from __future__ import annotations

import fusion_council_service.db as db_module


class _FakePgSession:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, stmt, params=None):  # noqa: ANN001
        sql = str(stmt)
        self.statements.append(sql)
        if "AUTOINCREMENT" in sql.upper():
            raise AssertionError("PostgreSQL DDL must not include AUTOINCREMENT")
        return _ScalarOneResult()

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _ScalarOneResult:
    def fetchone(self):
        return (1,)

    def fetchall(self):
        return []


def test_initialize_schema_strips_sqlite_autoincrement_in_postgres_mode(monkeypatch):
    fake_db = _FakePgSession()
    monkeypatch.setattr(db_module, "_is_postgresql", True)
    monkeypatch.setattr(db_module, "apply_schema_migrations", lambda _db: None)

    db_module.initialize_schema(fake_db)

    assert fake_db.commits >= 1
    assert any("run_shadow_diff" in stmt for stmt in fake_db.statements)
    assert all("AUTOINCREMENT" not in stmt.upper() for stmt in fake_db.statements)
