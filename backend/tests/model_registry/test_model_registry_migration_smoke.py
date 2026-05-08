from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

import scripts.model_registry_migration_smoke as smoke


def test_static_migration_smoke_passes_without_db_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_connect(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("default static smoke must not connect to DB")

    monkeypatch.setattr(smoke, "_connect", fail_connect)

    report = smoke.run_static_smoke()

    assert report.status == "passed"
    assert report.mode == "static_dry_run"
    assert report.db_target is None
    assert set(report.checks["migration"]["tables"]) == set(smoke.EXPECTED_TABLES)
    assert report.checks["rollback"]["guarded"] is True


def test_cli_default_json_is_static_dry_run(capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_connect(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("default CLI smoke must not connect to DB")

    monkeypatch.setattr(smoke, "_connect", fail_connect)

    assert smoke.main(["--json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "passed"
    assert payload["mode"] == "static_dry_run"
    assert payload["db_target"] is None


def test_migration_sql_contains_expected_phase5_objects_and_comments() -> None:
    sql = smoke.MIGRATION_PATH.read_text(encoding="utf-8")

    migration = smoke._validate_migration_sql(sql)

    assert migration["schema"] == "model_registry"
    assert set(migration["views"]) == set(smoke.EXPECTED_VIEWS)
    assert "DROP SCHEMA" not in sql.upper()
    for table in smoke.EXPECTED_TABLES:
        for column in smoke._table_columns(sql, table):
            assert f"COMMENT ON COLUMN model_registry.{table}.{column}" in sql


def test_guarded_rollback_file_requires_session_confirmation() -> None:
    sql = smoke.ROLLBACK_PATH.read_text(encoding="utf-8")

    rollback = smoke._validate_rollback_sql(sql)

    assert rollback == {"guarded": True, "destructive": True}
    assert "DROP SCHEMA IF EXISTS model_registry CASCADE" in sql
    assert "current_setting('aistock.model_registry_rollback_confirm'" in sql
    assert "IS DISTINCT FROM 'DROP_MODEL_REGISTRY_PHASE5_DEV_ONLY'" in sql
    assert "RAISE EXCEPTION" in sql


def test_db_transaction_check_requires_confirmation_and_env_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(smoke.ENV_DB_CHECK_ENABLED, raising=False)
    args = argparse.Namespace(
        db_transaction_check=True,
        apply=False,
        confirm_db_check="",
        confirm_apply="",
        allow_production_like_rollback_check=False,
    )
    target = smoke.DbTarget(host="127.0.0.1", port=5432, dbname="aistock_dev", user="postgres")

    with pytest.raises(smoke.MigrationSmokeError, match="confirm-db-check"):
        smoke._require_db_execution_safety(args, target)

    args.confirm_db_check = smoke.CONFIRM_DB_CHECK
    with pytest.raises(smoke.MigrationSmokeError, match=smoke.ENV_DB_CHECK_ENABLED):
        smoke._require_db_execution_safety(args, target)


def test_db_transaction_check_refuses_production_like_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(smoke.ENV_DB_CHECK_ENABLED, "true")
    monkeypatch.delenv(smoke.ENV_ALLOW_SUSPICIOUS_ROLLBACK, raising=False)
    args = argparse.Namespace(
        db_transaction_check=True,
        apply=False,
        confirm_db_check=smoke.CONFIRM_DB_CHECK,
        confirm_apply="",
        allow_production_like_rollback_check=False,
    )
    target = smoke.DbTarget(host="127.0.0.1", port=5432, dbname="aistock", user="postgres")

    with pytest.raises(smoke.MigrationSmokeError, match="production-like target"):
        smoke._require_db_execution_safety(args, target)


def test_db_transaction_check_allows_explicit_dev_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(smoke.ENV_DB_CHECK_ENABLED, "true")
    args = argparse.Namespace(
        db_transaction_check=True,
        apply=False,
        confirm_db_check=smoke.CONFIRM_DB_CHECK,
        confirm_apply="",
        allow_production_like_rollback_check=False,
    )
    target = smoke.DbTarget(host="localhost", port=5432, dbname="aistock_dev", user="postgres")

    smoke._require_db_execution_safety(args, target)


def test_apply_requires_dev_only_env_guard_and_refuses_plain_aistock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(smoke.ENV_APPLY_ENABLED, raising=False)
    args = argparse.Namespace(
        db_transaction_check=False,
        apply=True,
        confirm_db_check="",
        confirm_apply=smoke.CONFIRM_APPLY,
        allow_production_like_rollback_check=False,
    )
    dev_target = smoke.DbTarget(host="localhost", port=5432, dbname="aistock_dev", user="postgres")

    with pytest.raises(smoke.MigrationSmokeError, match=smoke.ENV_APPLY_ENABLED):
        smoke._require_db_execution_safety(args, dev_target)

    monkeypatch.setenv(smoke.ENV_APPLY_ENABLED, "true")
    prod_like = smoke.DbTarget(host="127.0.0.1", port=5432, dbname="aistock", user="postgres")
    with pytest.raises(smoke.MigrationSmokeError, match="Refusing --apply"):
        smoke._require_db_execution_safety(args, prod_like)

    prod_marker_with_dev = smoke.DbTarget(host="127.0.0.1", port=5432, dbname="aistock_prod_dev", user="postgres")
    with pytest.raises(smoke.MigrationSmokeError, match="dbname_contains_production_marker"):
        smoke._require_db_execution_safety(args, prod_marker_with_dev)


def test_db_transaction_check_rolls_back_and_never_commits(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.last_sql = ""

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc_info: object) -> None:
            return None

        def execute(self, sql: str) -> None:
            self.last_sql = sql

        def fetchone(self) -> tuple[object, ...]:
            if "to_regclass" in self.last_sql:
                return ("public.aistock_model_catalog",)
            if "COUNT(*)" in self.last_sql:
                return (99,)
            raise AssertionError(f"unexpected fetchone SQL: {self.last_sql}")

        def fetchall(self) -> list[tuple[str]]:
            if "information_schema.tables" in self.last_sql:
                return [(name,) for name in smoke.EXPECTED_TABLES]
            if "information_schema.views" in self.last_sql:
                return [(name,) for name in smoke.EXPECTED_VIEWS]
            raise AssertionError(f"unexpected fetchall SQL: {self.last_sql}")

    class FakeConnection:
        def __init__(self) -> None:
            self.autocommit = True
            self.commit_count = 0
            self.rollback_count = 0
            self.close_count = 0

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            self.commit_count += 1

        def rollback(self) -> None:
            self.rollback_count += 1

        def close(self) -> None:
            self.close_count += 1

    fake_conn = FakeConnection()
    monkeypatch.setattr(smoke, "_connect", lambda _target: fake_conn)

    report = smoke.run_db_execution(
        target=smoke.DbTarget(host="localhost", port=5432, dbname="aistock_dev", user="postgres"),
        apply=False,
    )

    assert report.mode == "db_transaction_check_rolled_back"
    assert fake_conn.commit_count == 0
    assert fake_conn.rollback_count == 1
    assert fake_conn.close_count == 1


def test_static_smoke_fails_when_rollback_file_is_missing(tmp_path: Path) -> None:
    missing = tmp_path / "missing_rollback.sql"

    with pytest.raises(smoke.MigrationSmokeError, match="Missing rollback SQL"):
        smoke.run_static_smoke(migration_path=smoke.MIGRATION_PATH, rollback_path=missing)
