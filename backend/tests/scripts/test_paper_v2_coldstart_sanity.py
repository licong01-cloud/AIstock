from __future__ import annotations

import argparse
import importlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest


sanity = importlib.import_module("scripts.paper_v2_coldstart_sanity")


TOKEN = sanity.CONFIRM_PROD
ENV_FLAG = sanity.ENV_PROD_ENABLED
MUTEX_ENV = sanity.ENV_MUTEX_HELD


class FakeCursor:
    def __init__(self, conn: "FakeConnection") -> None:
        self.conn = conn
        self.rowcount = 0

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def execute(self, sql: str, params: object | None = None) -> None:
        self.conn.executed.append((sql, params))
        self.conn.last_sql = sql
        self.conn.last_params = params
        sql_upper = sql.upper()
        if self.conn.fail_cleanup_table and sql_upper.startswith("DELETE") and self.conn.fail_cleanup_table.upper() in sql_upper:
            raise RuntimeError(f"cleanup failed for {self.conn.fail_cleanup_table}")
        if self.conn.fail_ping and "SELECT 1" in sql_upper:
            raise RuntimeError("ping failed")
        if sql_upper.startswith("DELETE"):
            self.rowcount = 1
        else:
            self.rowcount = 0

    def fetchone(self) -> Any:
        sql_upper = self.conn.last_sql.upper()
        params = self.conn.last_params
        if "SELECT 1" in sql_upper:
            return (1,)
        if "TO_REGCLASS" in sql_upper:
            table = params[0] if isinstance(params, tuple) else None
            return (None if table in self.conn.missing_tables else table,)
        if "FROM PAPER_V2.FILLS" in sql_upper:
            return self.conn.fill_row
        if "FROM QE_ARCHIVE.OUTBOX_EVENT" in sql_upper:
            return self.conn.outbox_rows[0] if self.conn.outbox_rows else None
        if "FROM STRATEGY_PKG.PACKAGE_VALIDATION_RUN" in sql_upper:
            return self.conn.evidence_row
        if "FROM STRATEGY_PKG.PACKAGE_ASSET" in sql_upper:
            return self.conn.ledger_row
        return None

    def fetchall(self) -> list[Any]:
        sql_upper = self.conn.last_sql.upper()
        if "INFORMATION_SCHEMA.COLUMNS" in sql_upper:
            return [
                ("created_at",),
                ("updated_at",),
                ("intended_price",),
                ("fill_market_context",),
            ]
        if "FROM STRATEGY_PKG.PACKAGE P" in sql_upper:
            return self.conn.package_rows
        if "FROM QE_ARCHIVE.OUTBOX_EVENT" in sql_upper:
            return self.conn.outbox_rows
        return []


class FakeConnection:
    def __init__(
        self,
        *,
        missing_tables: set[str] | None = None,
        package_rows: list[Any] | None = None,
        fill_row: Any | None = None,
        outbox_rows: list[Any] | None = None,
        evidence_row: Any = "__default__",
        ledger_row: Any = "__default__",
        fail_ping: bool = False,
        fail_cleanup_table: str | None = None,
    ) -> None:
        self.missing_tables = missing_tables or set()
        self.package_rows = package_rows if package_rows is not None else [_package_row()]
        self.fill_row = fill_row if fill_row is not None else _fill_row()
        self.outbox_rows = outbox_rows if outbox_rows is not None else [_outbox_row()]
        self.evidence_row = _evidence_row() if evidence_row == "__default__" else evidence_row
        self.ledger_row = _ledger_row() if ledger_row == "__default__" else ledger_row
        self.fail_ping = fail_ping
        self.fail_cleanup_table = fail_cleanup_table
        self.executed: list[tuple[str, object | None]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.last_sql = ""
        self.last_params: object | None = None
        self.readonly_modes: list[bool] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def set_session(self, *, readonly: bool, autocommit: bool = False) -> None:
        self.readonly_modes.append(readonly)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def _dt(second: int = 0) -> datetime:
    return datetime(2026, 5, 11, 8, 30, second)


def _package_row(
    package_id: str = "pkg_1",
    status: str = "PAPER_ENABLED",
    validation: bool = True,
    variant: bool = True,
    stability: bool = True,
    ledger: bool = True,
) -> tuple[Any, ...]:
    return (package_id, status, validation, variant, stability, ledger)


def _fill_row(
    *,
    run_id: str = "sanity-test",
    intended_price: str = "10.00",
    context: dict[str, Any] | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> tuple[Any, ...]:
    return (
        "fill_1",
        run_id,
        "order_1",
        "000001.SZ",
        "BUY",
        100,
        "10.00",
        intended_price,
        context if context is not None else {"stock_id": "000001.SZ"},
        created_at or _dt(1),
        updated_at or _dt(2),
        _dt(1),
    )


def _outbox_row(routing: str = "telemetry", status: str = "pending") -> tuple[Any, ...]:
    return (
        "evt_1",
        "paper.daemon.fill_received",
        "sanity-test",
        status,
        {"routing_class": routing, "run_id": "sanity-test"},
        _dt(3),
        _dt(3),
    )


def _evidence_row() -> tuple[Any, ...]:
    return ("vr_sanity", _dt(4))


def _ledger_row() -> tuple[Any, ...]:
    return (101, _dt(5))


def _args(**overrides: Any) -> argparse.Namespace:
    data = {
        "mode": "prod",
        "confirm_prod": TOKEN,
        "operator_confirmation": f"{TOKEN} target=prod package_id=pkg_1",
        "api_base": "http://127.0.0.1:8001/api/v1",
        "health_path": "/health",
        "sentinel_endpoint": "/paper-v2/coldstart-sanity/sentinel-order",
        "daemon_process_name": "paper_v2",
        "package_id": ["pkg_1"],
        "sentinel_package_id": None,
        "run_id": "sanity-test",
        "timeout_seconds": 30,
        "poll_seconds": 0.001,
        "http_timeout": 1.0,
        "require_ledger_audit": True,
        "target_db": "prod",
        "db_host": "prod-db.invalid",
        "db_port": 5432,
        "db_name": "aistock",
        "db_user": "aistock_operator",
        "db_password": "",
        "db_password_env": "AISTOCK_PROD_DB_PASSWORD",
        "json": True,
        "output": None,
    }
    data.update(overrides)
    return argparse.Namespace(**data)


def _enable_guards(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_FLAG, "true")
    monkeypatch.setenv(MUTEX_ENV, "true")
    monkeypatch.setattr(sanity, "_now_local", lambda: _dt(), raising=False)


def _patch_prod_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sanity, "_http_get_json", lambda *a, **k: {"status": "ok"}, raising=False)
    monkeypatch.setattr(sanity, "_http_post_json", lambda *a, **k: {"ok": True, "status": "accepted"}, raising=False)
    monkeypatch.setattr(sanity, "_find_daemon_process", lambda name: {"pid": 123, "name": name}, raising=False)


def _forbid_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError("guard failure must happen before DB connect")

    monkeypatch.setattr(sanity, "_connect", forbidden, raising=False)


def test_default_dry_run_opens_no_db_or_http(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    monkeypatch.setattr(sanity, "_http_get_json", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no HTTP")), raising=False)

    assert sanity.main(["--json", "--run-id", "sanity-test"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["db_connection_opened"] is False
    assert payload["db_writes_executed"] is False
    assert payload["production_services_touched"] is False
    assert payload["sentinel_order"]["symbol"] == "000001.SZ"


def test_sentinel_package_id_defaults_to_first_package_id(capsys: pytest.CaptureFixture[str]) -> None:
    assert sanity.main(["--json", "--run-id", "sanity-test", "--package-id", "pkg_first", "--package-id", "pkg_second"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["sentinel_order"]["package_id"] == "pkg_first"


def test_sentinel_package_id_can_be_explicit(capsys: pytest.CaptureFixture[str]) -> None:
    assert sanity.main([
        "--json",
        "--run-id",
        "sanity-test",
        "--package-id",
        "pkg_first",
        "--sentinel-package-id",
        "pkg_explicit",
    ]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["sentinel_order"]["package_id"] == "pkg_explicit"


@pytest.mark.parametrize(
    ("argv", "env_flag", "mutex", "needle"),
    [
        (["--mode", "prod", "--operator-confirmation", f"{TOKEN} target=prod package_id=pkg_1"], True, True, "confirm-prod"),
        (["--mode", "prod", "--confirm-prod", TOKEN, "--operator-confirmation", f"{TOKEN} target=prod package_id=pkg_1"], False, True, ENV_FLAG),
        (["--mode", "prod", "--confirm-prod", TOKEN, "--operator-confirmation", f"{TOKEN} target=prod package_id=pkg_1"], True, False, "mutex"),
        (["--mode", "prod", "--confirm-prod", TOKEN], True, True, "operator confirmation"),
    ],
)
def test_prod_guard_rejects_before_connect(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
    env_flag: bool,
    mutex: bool,
    needle: str,
) -> None:
    monkeypatch.setattr(sanity, "_now_local", lambda: _dt(), raising=False)
    if env_flag:
        monkeypatch.setenv(ENV_FLAG, "true")
    if mutex:
        monkeypatch.setenv(MUTEX_ENV, "true")
    _forbid_connect(monkeypatch)

    assert sanity.main(["--json", *argv]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert needle.lower() in payload["error"].lower()
    assert payload["db_connection_opened"] is False


def test_prod_rejects_trading_hours_before_connect(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv(ENV_FLAG, "true")
    monkeypatch.setenv(MUTEX_ENV, "true")
    monkeypatch.setattr(sanity, "_now_local", lambda: datetime(2026, 5, 11, 9, 31), raising=False)
    _forbid_connect(monkeypatch)

    assert sanity.main(["--json", "--mode", "prod", "--confirm-prod", TOKEN, "--operator-confirmation", f"{TOKEN} target=prod package_id=pkg_1"]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert "trading hours" in payload["error"]


@pytest.mark.parametrize(
    ("overrides", "needle"),
    [
        ({"target_db": "dev"}, "target-db prod"),
        ({"db_port": 5433}, "port 5432"),
        ({"db_name": "aistock_dev"}, "dev/test"),
    ],
)
def test_prod_rejects_wrong_db_target_before_connect(monkeypatch: pytest.MonkeyPatch, overrides: dict[str, Any], needle: str) -> None:
    _enable_guards(monkeypatch)
    _forbid_connect(monkeypatch)
    args = _args(**overrides)

    with pytest.raises(sanity.ColdStartSanityError, match=needle):
        sanity._require_prod_guards(args, sanity._target_from_args(args), sanity.SentinelOrder("sanity-test", "pkg_1"), now=_dt())


def test_prod_guard_requires_sentinel_package_id_in_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_guards(monkeypatch)
    _forbid_connect(monkeypatch)
    args = _args(operator_confirmation=f"{TOKEN} target=prod", sentinel_package_id="pkg_1")

    with pytest.raises(sanity.ColdStartSanityError, match="sentinel package id"):
        sanity._require_prod_guards(args, sanity._target_from_args(args), sanity.SentinelOrder("sanity-test", "pkg_1"), now=_dt())


def test_preflight_backend_down_stops_before_db(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_guards(monkeypatch)
    monkeypatch.setattr(sanity, "_http_get_json", lambda *a, **k: {"status": "down"}, raising=False)
    monkeypatch.setattr(sanity, "_find_daemon_process", lambda name: {"pid": 123}, raising=False)
    _forbid_connect(monkeypatch)

    report = sanity.run_prod(_args(), sanity._target_from_args(_args()), sanity.SentinelOrder("sanity-test", "pkg_1"))
    assert report["verdict"] == "NO-GO"
    assert "backend_health" in report["failed_checks"]
    assert report["sentinel_order_requested"] is False


def test_preflight_daemon_down_stops_before_db(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_guards(monkeypatch)
    monkeypatch.setattr(sanity, "_http_get_json", lambda *a, **k: {"status": "ok"}, raising=False)
    monkeypatch.setattr(sanity, "_find_daemon_process", lambda name: None, raising=False)
    _forbid_connect(monkeypatch)

    report = sanity.run_prod(_args(), sanity._target_from_args(_args()), sanity.SentinelOrder("sanity-test", "pkg_1"))
    assert "paper_v2_daemon_process" in report["failed_checks"]
    assert report["sentinel_order_requested"] is False


def test_preflight_db_unreachable_is_no_go(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_guards(monkeypatch)
    _patch_prod_runtime(monkeypatch)

    def fail_connect(*args: object, **kwargs: object) -> object:
        raise sanity.ColdStartSanityError("db unreachable")

    monkeypatch.setattr(sanity, "_connect", fail_connect, raising=False)
    assert sanity.main(["--json", "--mode", "prod", "--confirm-prod", TOKEN, "--operator-confirmation", f"{TOKEN} target=prod package_id=pkg_1", "--package-id", "pkg_1"]) == 2


def test_missing_table_blocks_phase_two() -> None:
    conn = FakeConnection(missing_tables={"paper_v2.fills"})
    checks = sanity._preflight_db_checks(conn, ["pkg_1"])
    assert "required_tables" in [item["check"] for item in checks if item["status"] == "FAIL"]


def test_missing_capture_columns_blocks_before_sentinel_trigger() -> None:
    class MissingColumnCursor(FakeCursor):
        def fetchall(self) -> list[Any]:
            if "INFORMATION_SCHEMA.COLUMNS" in self.conn.last_sql.upper():
                return [("created_at",), ("updated_at",)]
            return super().fetchall()

    class MissingColumnConnection(FakeConnection):
        def cursor(self) -> MissingColumnCursor:
            return MissingColumnCursor(self)

    checks = sanity._preflight_db_checks(MissingColumnConnection(), ["pkg_1"])
    gate = next(item for item in checks if item["check"] == "required_capture_columns")

    assert gate["status"] == "FAIL"
    assert gate["data"]["missing_columns"] == ["fill_market_context", "intended_price"]


def test_prod_missing_capture_columns_stops_before_sentinel_trigger(monkeypatch: pytest.MonkeyPatch) -> None:
    class MissingColumnCursor(FakeCursor):
        def fetchall(self) -> list[Any]:
            if "INFORMATION_SCHEMA.COLUMNS" in self.conn.last_sql.upper():
                return [("created_at",), ("updated_at",)]
            return super().fetchall()

    class MissingColumnConnection(FakeConnection):
        def cursor(self) -> MissingColumnCursor:
            return MissingColumnCursor(self)

    _enable_guards(monkeypatch)
    _patch_prod_runtime(monkeypatch)
    monkeypatch.setattr(sanity, "_connect", lambda *a, **k: MissingColumnConnection(), raising=False)
    monkeypatch.setattr(
        sanity,
        "_http_post_json",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("sentinel trigger must not run")),
        raising=False,
    )

    report = sanity.run_prod(_args(), sanity._target_from_args(_args()), sanity.SentinelOrder("sanity-test", "pkg_1"))

    assert report["verdict"] == "NO-GO"
    assert "required_capture_columns" in report["failed_checks"]
    assert report["sentinel_order_requested"] is False
    assert any("add_paper_v2_capture_fields" in item for item in report["remedial_action"])


def test_missing_evidence_blocks_phase_two() -> None:
    conn = FakeConnection(package_rows=[_package_row(validation=False)])
    checks = sanity._preflight_db_checks(conn, ["pkg_1"])
    gate = next(item for item in checks if item["check"] == "governance_evidence_and_enable_paper")
    assert gate["status"] == "FAIL"
    assert "validation_evidence" in gate["data"]["package_failures"][0]


def test_gate_disabled_blocks_phase_two() -> None:
    conn = FakeConnection(package_rows=[_package_row(status="BACKTEST_APPROVED")])
    checks = sanity._preflight_db_checks(conn, ["pkg_1"])
    gate = next(item for item in checks if item["check"] == "governance_evidence_and_enable_paper")
    assert gate["status"] == "FAIL"
    assert "paper_enabled" in gate["data"]["package_failures"][0]


def test_sentinel_payload_is_exact() -> None:
    payload = sanity.SentinelOrder(run_id="sanity-test", package_id="pkg_1").payload()
    assert payload["symbol"] == "000001.SZ"
    assert payload["side"] == "BUY"
    assert payload["quantity"] == 100
    assert payload["qty"] == 100
    assert payload["package_id"] == "pkg_1"
    assert payload["intended_price"] == "10.00"
    assert payload["broker_backend"] == "local_sim"


def test_poll_fill_passes_complete_row() -> None:
    check, fill = sanity._poll_fill(FakeConnection(), sanity.SentinelOrder("sanity-test", "pkg_1"), timeout_seconds=1, poll_seconds=0.001)
    assert check["status"] == "PASS"
    assert fill is not None
    assert fill["fill_market_context"]["stock_id"] == "000001.SZ"


def test_poll_fill_rejects_missing_market_context() -> None:
    conn = FakeConnection(fill_row=_fill_row(context={}))
    check, _ = sanity._poll_fill(conn, sanity.SentinelOrder("sanity-test", "pkg_1"), timeout_seconds=1, poll_seconds=0.001)
    assert check["status"] == "FAIL"
    assert "fill_market_context" in check["data"]["failed_fields"]


def test_poll_fill_rejects_missing_timestamps() -> None:
    conn = FakeConnection(fill_row=_fill_row(created_at=None, updated_at=None))
    check, _ = sanity._poll_fill(conn, sanity.SentinelOrder("sanity-test", "pkg_1"), timeout_seconds=1, poll_seconds=0.001)
    # Helper defaults fill in timestamps, so use explicit malformed tuple here.
    if check["status"] == "PASS":
        malformed = list(_fill_row())
        malformed[9] = None
        malformed[10] = None
        check, _ = sanity._poll_fill(FakeConnection(fill_row=tuple(malformed)), sanity.SentinelOrder("sanity-test", "pkg_1"), timeout_seconds=1, poll_seconds=0.001)
    assert check["status"] == "FAIL"
    assert "created_at" in check["data"]["failed_fields"]
    assert "updated_at" in check["data"]["failed_fields"]


def test_outbox_requires_telemetry_routing() -> None:
    check, event = sanity._check_outbox(FakeConnection(outbox_rows=[_outbox_row("archive")]), sanity.SentinelOrder("sanity-test", "pkg_1"))
    assert check["status"] == "FAIL"
    assert event is None


def test_outbox_allows_pending_or_sent() -> None:
    for status in ("pending", "sent"):
        check, event = sanity._check_outbox(FakeConnection(outbox_rows=[_outbox_row("telemetry", status)]), sanity.SentinelOrder("sanity-test", "pkg_1"))
        assert check["status"] == "PASS"
        assert event is not None


def test_audit_chain_requires_evidence() -> None:
    conn = FakeConnection(evidence_row=None)
    check = sanity._check_audit_chain(conn, sanity.SentinelOrder("sanity-test", "pkg_1"), {"created_at": _dt(1)}, require_ledger=True)
    assert check["status"] == "FAIL"
    assert "missing_governance_evidence" in check["data"]["failed_fields"]


def test_audit_chain_rejects_bad_timestamp_order() -> None:
    conn = FakeConnection(evidence_row=("vr", _dt(1)), ledger_row=(1, _dt(0)))
    check = sanity._check_audit_chain(conn, sanity.SentinelOrder("sanity-test", "pkg_1"), {"created_at": _dt(2)}, require_ledger=True)
    assert check["status"] == "FAIL"
    assert "fill_after_evidence" in check["data"]["failed_fields"]
    assert "evidence_after_ledger" in check["data"]["failed_fields"]


def test_cleanup_commits_each_table() -> None:
    conn = FakeConnection()
    check = sanity._cleanup_sentinel(conn, sanity.SentinelOrder("sanity-test", "pkg_1"))
    assert check["status"] == "PASS"
    assert conn.commits == len(sanity.CLEANUP_TABLES)
    assert conn.rollbacks == 0


def test_cleanup_rolls_back_on_failure() -> None:
    conn = FakeConnection(fail_cleanup_table="paper_v2.fills")
    check = sanity._cleanup_sentinel(conn, sanity.SentinelOrder("sanity-test", "pkg_1"))
    assert check["status"] == "FAIL"
    assert conn.rollbacks == 1


def test_full_mocked_prod_path_go(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_guards(monkeypatch)
    _patch_prod_runtime(monkeypatch)
    read_conn = FakeConnection()
    cleanup_conn = FakeConnection()
    connections = [read_conn, cleanup_conn]

    def fake_connect(target: object, *, readonly: bool = True) -> FakeConnection:
        conn = connections.pop(0)
        conn.set_session(readonly=readonly)
        return conn

    monkeypatch.setattr(sanity, "_connect", fake_connect, raising=False)
    report = sanity.run_prod(_args(), sanity._target_from_args(_args()), sanity.SentinelOrder("sanity-test", "pkg_1"))
    assert report["verdict"] == "GO"
    assert report["real_trading_ready"] is True
    assert read_conn.readonly_modes == [True]
    assert cleanup_conn.readonly_modes == [False]
    assert report["db_writes_executed"] is True


def test_failure_verdict_has_schema_failed_checks_and_remediation(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_guards(monkeypatch)
    _patch_prod_runtime(monkeypatch)
    monkeypatch.setattr(sanity, "_connect", lambda *a, **k: FakeConnection(package_rows=[_package_row(variant=False)]), raising=False)
    report = sanity.run_prod(_args(), sanity._target_from_args(_args()), sanity.SentinelOrder("sanity-test", "pkg_1"))
    assert report["schema_version"] == sanity.SCHEMA_VERSION
    assert report["verdict"] == "NO-GO"
    assert report["failed_checks"]
    assert report["remedial_action"]


def test_prod_main_success_exit_zero(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_guards(monkeypatch)
    _patch_prod_runtime(monkeypatch)
    monkeypatch.setattr(sanity, "_connect", lambda *a, **k: FakeConnection(), raising=False)
    rc = sanity.main(["--json", "--mode", "prod", "--confirm-prod", TOKEN, "--operator-confirmation", f"{TOKEN} target=prod package_id=pkg_1", "--package-id", "pkg_1", "--run-id", "sanity-test"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["verdict"] == "GO"


def test_dry_run_output_file_written(tmp_path: Path) -> None:
    output = tmp_path / "dry.json"
    rc = sanity.main(["--output", str(output), "--run-id", "sanity-test"])
    assert rc == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["mode"] == "dry-run"
    assert payload["verdict"] == "NO-GO"


def test_source_does_not_import_dev_backfill_apply_scripts() -> None:
    source = Path("scripts/paper_v2_coldstart_sanity.py").read_text(encoding="utf-8")
    assert "strategy_package_evidence_backfill.py --apply" not in source
    assert "protected_asset_ledger_backfill.py --apply" not in source
    assert "scripts.strategy_package_evidence_backfill" not in source
    assert "scripts.protected_asset_ledger_backfill" not in source
