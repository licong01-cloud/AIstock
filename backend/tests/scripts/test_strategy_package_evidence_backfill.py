from __future__ import annotations

import json
import re

import pytest

import scripts.strategy_package_evidence_backfill as backfill


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def execute(self, sql: str, params: object | None = None) -> None:
        self.connection.executed_sql.append(sql)
        self.connection.executed_params.append(params)
        first_keyword = sql.lstrip().split(None, 1)[0].upper()
        if self.connection.select_only and first_keyword != "SELECT":
            raise AssertionError(f"dry-run executed non-SELECT SQL: {sql}")
        if self.connection.select_only and re.search(r"\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE)\b", sql, re.I):
            raise AssertionError(f"dry-run executed write/DDL SQL: {sql}")

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self.connection.rows)


class FakeConnection:
    def __init__(self, *, select_only: bool = True) -> None:
        self.select_only = select_only
        self.rows = [("pkg_a", 3, 0, 0, 0), ("pkg_b", 0, 3, 0, 1)]
        self.executed_sql: list[str] = []
        self.executed_params: list[object | None] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.close_count = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.close_count += 1


def test_dry_run_preview_uses_dev_5433_select_only(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = FakeConnection(select_only=True)
    seen_targets: list[backfill.DbTarget] = []

    def fake_connect(target: backfill.DbTarget) -> FakeConnection:
        seen_targets.append(target)
        return fake_conn

    monkeypatch.setattr(backfill, "_connect", fake_connect)

    report = backfill.run_dry_run_preview(target=backfill.default_dev_target(), limit=10)

    assert seen_targets == [backfill.default_dev_target()]
    assert report["status"] == "passed"
    assert report["dry_run"] is True
    assert report["target_db"] == "dev"
    assert report["db_writes"] is False
    assert report["ddl"] is False
    assert report["packages"] == [
        {"id": "pkg_a", "evidence_planned": 3, "evidence_existing": 0, "asset_planned": 0, "asset_existing": 0},
        {"id": "pkg_b", "evidence_planned": 0, "evidence_existing": 3, "asset_planned": 0, "asset_existing": 1},
    ]
    assert all(sql.lstrip().upper().startswith("SELECT") for sql in fake_conn.executed_sql)
    assert fake_conn.commit_count == 0
    assert fake_conn.rollback_count == 1
    assert fake_conn.close_count == 1


def test_cli_dry_run_exit_code_and_json_schema(capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = FakeConnection(select_only=True)
    monkeypatch.setattr(backfill, "_connect", lambda _target: fake_conn)

    assert backfill.main(["--json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["schema_version"] == backfill.SCHEMA_VERSION
    assert payload["status"] == "passed"
    assert payload["db_writes"] is False
    assert payload["ddl"] is False
    assert payload["dry_run"] is True
    assert payload["target_db"] == "dev"
    assert set(payload["packages"][0]) == {"id", "evidence_planned", "evidence_existing", "asset_planned", "asset_existing"}


def test_cli_output_writes_json_without_stdout(tmp_path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = FakeConnection(select_only=True)
    output = tmp_path / "evidence.json"
    monkeypatch.setattr(backfill, "_connect", lambda _target: fake_conn)

    assert backfill.main(["--output", str(output)]) == 0

    assert capsys.readouterr().out == ""
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["status"] == "passed"
    assert payload["db_writes"] is False


def test_dry_run_rejects_non_dev_or_non_5433_before_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    connect_called = False

    def fail_connect(_target: backfill.DbTarget) -> object:
        nonlocal connect_called
        connect_called = True
        raise AssertionError("guard failure must happen before connect")

    monkeypatch.setattr(backfill, "_connect", fail_connect)

    target = backfill.DbTarget(target_db="prod", host="127.0.0.1", port=5432, dbname="aistock", user="postgres")
    with pytest.raises(backfill.StrategyPackageEvidenceBackfillError, match="target_db=dev"):
        backfill.run_dry_run_preview(target=target, limit=10)
    assert connect_called is False

    target = backfill.DbTarget(target_db="dev", host="127.0.0.1", port=5432, dbname="aistock_dev", user="postgres")
    with pytest.raises(backfill.StrategyPackageEvidenceBackfillError, match="port 5433"):
        backfill.run_dry_run_preview(target=target, limit=10)
    assert connect_called is False


def test_dry_run_rejects_non_aistock_dev_dbname_before_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    connect_called = False

    def fail_connect(_target: backfill.DbTarget) -> object:
        nonlocal connect_called
        connect_called = True
        raise AssertionError("guard failure must happen before connect")

    monkeypatch.setattr(backfill, "_connect", fail_connect)

    target = backfill.DbTarget(target_db="dev", host="127.0.0.1", port=5433, dbname="aistock", user="postgres")
    with pytest.raises(backfill.StrategyPackageEvidenceBackfillError, match="aistock_dev"):
        backfill.run_dry_run_preview(target=target, limit=10)
    assert connect_called is False


def test_apply_guard_blocks_before_connect_and_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    connect_called = False

    def fail_connect(_target: backfill.DbTarget) -> object:
        nonlocal connect_called
        connect_called = True
        raise AssertionError("unguarded apply must not connect")

    monkeypatch.setattr(backfill, "_connect", fail_connect)
    monkeypatch.delenv(backfill.ENV_APPLY_ENABLED, raising=False)

    assert backfill.main(["--apply", "--json"]) == 2
    assert connect_called is False
    assert backfill.main(["--apply", "--confirm-apply", backfill.CONFIRM_APPLY, "--json"]) == 2
    assert connect_called is False


def test_apply_guard_rejects_non_aistock_dev_dbname_before_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    connect_called = False

    def fail_connect(_target: backfill.DbTarget) -> object:
        nonlocal connect_called
        connect_called = True
        raise AssertionError("unguarded apply must not connect")

    monkeypatch.setattr(backfill, "_connect", fail_connect)
    monkeypatch.setenv(backfill.ENV_APPLY_ENABLED, "true")

    assert (
        backfill.main(
            [
                "--apply",
                "--confirm-apply",
                backfill.CONFIRM_APPLY,
                "--db-port",
                "5433",
                "--db-name",
                "aistock",
                "--json",
            ]
        )
        == 2
    )
    assert connect_called is False


def test_apply_path_is_gated_and_mockable(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = FakeConnection(select_only=False)
    monkeypatch.setenv(backfill.ENV_APPLY_ENABLED, "true")
    monkeypatch.setattr(backfill, "_connect", lambda _target: fake_conn)

    assert backfill.main(["--apply", "--confirm-apply", backfill.CONFIRM_APPLY, "--json"]) == 0

    assert fake_conn.commit_count == 1
    assert fake_conn.close_count == 1
    assert any(sql.lstrip().upper().startswith("INSERT") for sql in fake_conn.executed_sql)
    assert fake_conn.executed_params[-1] == (100,)


def test_apply_path_respects_limit_in_preview_and_insert(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = FakeConnection(select_only=False)
    monkeypatch.setenv(backfill.ENV_APPLY_ENABLED, "true")
    monkeypatch.setattr(backfill, "_connect", lambda _target: fake_conn)

    assert backfill.main(["--apply", "--confirm-apply", backfill.CONFIRM_APPLY, "--limit", "2", "--json"]) == 0

    assert fake_conn.executed_params[0] == (2,)
    assert fake_conn.executed_params[-1] == (2,)
