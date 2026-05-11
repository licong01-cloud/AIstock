from __future__ import annotations

import importlib
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

executor = importlib.import_module("scripts.protected_asset_ledger_backfill_prod_executor")


CONFIRM_TOKEN = "APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_PROD"
ENV_FLAG = "AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_PROD_APPLY_ENABLED"
MUTEX_ENV = "AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_MUTEX_HELD"
PLAN_SCHEMA_VERSION = "aistock_protected_asset_ledger_backfill_plan_v1"


class FakeCursor:
    def __init__(self, conn: "FakeConnection") -> None:
        self.conn = conn

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def execute(self, sql: str, params: object | None = None) -> None:
        self.conn.executed.append((sql, params))
        self.conn.last_sql = sql
        self.conn.last_params = params
        if self.conn.fail_on_execute_number and len(self.conn.executed) == self.conn.fail_on_execute_number:
            raise RuntimeError("simulated protected asset ledger write failure")

    def fetchone(self) -> dict[str, Any] | None:
        sql = self.conn.last_sql.upper()
        params = self.conn.last_params
        package_id = "pkg_1"
        if isinstance(params, tuple) and params and isinstance(params[0], str) and params[0].startswith("pkg_"):
            package_id = params[0]
        suffix = package_id.rsplit("_", 1)[-1]
        if "FROM STRATEGY_PKG.PACKAGE" in sql:
            return {
                "package_id": package_id,
                "manifest_sha256": f"manifest_{suffix}",
                "package_status": "BACKTEST_APPROVED",
            }
        if self.conn.existing_rows:
            return self.conn.existing_rows.pop(0)
        return None

    def fetchall(self) -> list[dict[str, Any]]:
        return []


class FakeConnection:
    def __init__(self, *, fail_on_execute_number: int | None = None, existing_rows: list[dict[str, Any]] | None = None) -> None:
        self.executed: list[tuple[str, object | None]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.fail_on_execute_number = fail_on_execute_number
        self.existing_rows = list(existing_rows or [])
        self.last_sql = ""
        self.last_params: object | None = None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def _token() -> str:
    return getattr(executor, "CONFIRM_APPLY", CONFIRM_TOKEN)


def _env_flag() -> str:
    return getattr(executor, "ENV_APPLY_ENABLED", ENV_FLAG)


def _mutex_env() -> str:
    return getattr(executor, "ENV_MUTEX_HELD", MUTEX_ENV)


def _plan_schema() -> str:
    return getattr(executor, "PLAN_SCHEMA_VERSION", PLAN_SCHEMA_VERSION)


def _bundle(package_count: int = 4) -> dict[str, Any]:
    return {
        "schema_version": _plan_schema(),
        "packages": [
            {
                "package_id": f"pkg_{idx}",
                "manifest_sha256": f"manifest_{idx}",
                "package_status": "BACKTEST_APPROVED",
                "required_gates": {
                    "manifest_identity": True,
                    "protected_asset_ledger": True,
                    "original_fixed_weight_retest": True,
                    "runtime_variant_candidate": True,
                },
                "blockers": [],
                "rows": [
                    {
                        "table": "strategy_pkg.package_asset",
                        "natural_key": {
                            "package_id": f"pkg_{idx}",
                            "asset_type": "protected_asset_ledger_evidence",
                            "asset_ref": "governance/protected_asset_ledger_backfill",
                        },
                        "action": "insert_or_update_protected_asset_ledger_evidence",
                        "columns": {
                            "package_id": f"pkg_{idx}",
                            "asset_type": "protected_asset_ledger_evidence",
                            "asset_ref": "governance/protected_asset_ledger_backfill",
                            "asset_sha256": f"ledger_asset_hash_{idx}",
                            "metadata": {
                                "source": "protected_asset_ledger_backfill_prod_executor_test",
                                "manifest_sha256": f"manifest_{idx}",
                                "evidence_kind": "protected_asset_ledger",
                            },
                            "asset_role": "governance_evidence",
                            "protected_asset": True,
                            "source_uri": "strategy_pkg.package",
                        },
                    }
                ],
            }
            for idx in range(1, package_count + 1)
        ],
    }


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _dr_snapshot(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "dr_snapshot.json",
        {"status": "verified", "snapshot_id": "snap-ledger-r6", "checksum": "sha256:ledger", "completed_at": "2026-05-11T00:00:00Z"},
    )


def _plan_preview(tmp_path: Path, payload: dict[str, Any] | None = None) -> Path:
    bundle = payload or _bundle()
    return _write_json(
        tmp_path / "plan_preview.json",
        {
            "schema_version": bundle["schema_version"],
            "status": "passed",
            "mode": "offline_dry_run",
            "dry_run": True,
            "db_writes": False,
            "ddl": False,
            "package_count": len(bundle["packages"]),
            "package_ids": [package["package_id"] for package in bundle["packages"]],
            "blocked_packages": {},
            "db_connection_opened": False,
            "db_writes_executed": False,
            "packages": bundle["packages"],
        },
    )


def _plan_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _operator_confirmation(tmp_path: Path, plan_preview_path: Path | None = None) -> Path:
    plan_path = plan_preview_path or (tmp_path / "plan_preview.json")
    plan_payload = _plan_payload(plan_path)
    package_ids = " ".join(plan_payload["package_ids"])
    confirmation = (
        f"{_token()} target=aistock_prod plan={_sha256_file(plan_path)} "
        f"dr=snap-ledger-r6 packages={package_ids}"
    )
    return _write_json(
        tmp_path / "operator_confirmation.json",
        {
            "status": "approved",
            "operator": "pytest-db-operator",
            "confirmed_at": "2026-05-11T00:00:00Z",
            "confirmation": confirmation,
        },
    )


def _base_args(tmp_path: Path, bundle: dict[str, Any] | None = None) -> list[str]:
    payload = bundle or _bundle()
    bundle_path = _write_json(tmp_path / "bundle.json", payload)
    plan_preview_path = _plan_preview(tmp_path, payload)
    return [
        "--evidence-bundle",
        str(bundle_path),
        "--dr-snapshot",
        str(_dr_snapshot(tmp_path)),
        "--plan-preview",
        str(plan_preview_path),
        "--operator-confirmation",
        str(_operator_confirmation(tmp_path, plan_preview_path)),
        "--confirm-apply",
        _token(),
        "--target-db",
        "prod",
        "--db-host",
        "prod-db.invalid",
        "--db-port",
        "5432",
        "--db-name",
        "aistock_prod",
        "--db-user",
        "db_operator",
        "--json",
    ]


def _enable_prod_guards(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_env_flag(), "true")
    monkeypatch.setenv(_mutex_env(), "true")


def _forbid_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_connect(*args: object, **kwargs: object) -> object:
        raise AssertionError("guard failure must happen before DB connect")

    monkeypatch.setattr(executor, "_connect", forbidden_connect, raising=False)


def _error_type() -> type[Exception]:
    return getattr(executor, "ProtectedAssetLedgerBackfillProdExecutorError", RuntimeError)


def test_default_cli_is_dry_run_and_does_not_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    bundle_path = _write_json(tmp_path / "bundle.json", _bundle())

    assert executor.main(["--evidence-bundle", str(bundle_path), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["db_connection_opened"] is False
    assert payload["db_writes_executed"] is False
    assert payload["production_services_touched"] is False


def test_apply_requires_explicit_apply_flag_even_with_other_guards(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)

    assert executor.main(_base_args(tmp_path)) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["db_writes_executed"] is False


def test_apply_requires_exact_confirmation_token_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index("--confirm-apply") + 1] = "wrong-token"

    assert executor.main(args) != 0

    payload = json.loads(capsys.readouterr().out)
    assert "confirm-apply" in payload["error"]
    assert payload["db_connection_opened"] is False
    assert payload["db_writes_executed"] is False


def test_apply_requires_enabled_environment_flag_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    monkeypatch.delenv(_env_flag(), raising=False)
    monkeypatch.setenv(_mutex_env(), "true")

    assert executor.main(["--apply", *_base_args(tmp_path)]) != 0

    payload = json.loads(capsys.readouterr().out)
    assert _env_flag() in payload["error"]
    assert payload["db_connection_opened"] is False
    assert payload["db_writes_executed"] is False


def test_apply_requires_mutex_guard_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    monkeypatch.setenv(_env_flag(), "true")
    monkeypatch.delenv(_mutex_env(), raising=False)

    assert executor.main(["--apply", *_base_args(tmp_path)]) != 0

    payload = json.loads(capsys.readouterr().out)
    assert "mutex" in payload["error"].lower()
    assert payload["db_connection_opened"] is False
    assert payload["db_writes_executed"] is False


@pytest.mark.parametrize(
    ("flag", "value", "expected"),
    [
        ("--target-db", "dev", "prod"),
        ("--db-port", "5433", "5432"),
        ("--db-name", "aistock_dev", "dev"),
    ],
)
def test_apply_requires_prod_target_triple_check_before_connect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    flag: str,
    value: str,
    expected: str,
) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index(flag) + 1] = value

    assert executor.main(args) != 0

    payload = json.loads(capsys.readouterr().out)
    assert expected in payload["error"].lower()
    assert payload["db_connection_opened"] is False
    assert payload["db_writes_executed"] is False


@pytest.mark.parametrize(
    ("flag", "expected"),
    [
        ("--dr-snapshot", "dr snapshot"),
        ("--plan-preview", "plan preview"),
        ("--operator-confirmation", "operator confirmation"),
    ],
)
def test_apply_refuses_missing_pre_apply_evidence_files_before_connect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    flag: str,
    expected: str,
) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    args = ["--apply", *_base_args(tmp_path)]
    index = args.index(flag)
    del args[index : index + 2]

    assert executor.main(args) != 0

    payload = json.loads(capsys.readouterr().out)
    assert expected in payload["error"].lower()
    assert payload["db_connection_opened"] is False
    assert payload["db_writes_executed"] is False


def test_apply_refuses_unverified_dr_snapshot_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    bad_snapshot = _write_json(tmp_path / "bad_snapshot.json", {"status": "pending", "snapshot_id": "snap-ledger-r6"})
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index("--dr-snapshot") + 1] = str(bad_snapshot)

    assert executor.main(args) != 0

    payload = json.loads(capsys.readouterr().out)
    assert "dr snapshot" in payload["error"].lower()
    assert payload["db_connection_opened"] is False
    assert payload["db_writes_executed"] is False


def test_apply_refuses_blocked_plan_preview_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    blocked_plan = _write_json(
        tmp_path / "blocked_plan.json",
        {
            "schema_version": _plan_schema(),
            "status": "blocked",
            "mode": "offline_dry_run",
            "dry_run": True,
            "db_writes": False,
            "ddl": False,
            "package_count": 4,
            "blocked_packages": {"pkg_1": ["missing protected asset ledger"]},
        },
    )
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index("--plan-preview") + 1] = str(blocked_plan)

    assert executor.main(args) != 0

    payload = json.loads(capsys.readouterr().out)
    assert "plan preview" in payload["error"].lower()
    assert payload["db_connection_opened"] is False
    assert payload["db_writes_executed"] is False


def test_apply_refuses_plan_preview_with_db_connection_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    plan = _plan_payload(_plan_preview(tmp_path, _bundle()))
    plan["db_connection_opened"] = True
    unsafe_plan = _write_json(tmp_path / "unsafe_plan.json", plan)
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index("--plan-preview") + 1] = str(unsafe_plan)

    assert executor.main(args) != 0

    payload = json.loads(capsys.readouterr().out)
    assert "db connection" in payload["error"].lower()
    assert payload["db_connection_opened"] is False


def test_apply_refuses_unapproved_operator_confirmation_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    bad_confirmation = _write_json(tmp_path / "bad_confirmation.json", {"status": "draft", "operator": "pytest"})
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index("--operator-confirmation") + 1] = str(bad_confirmation)

    assert executor.main(args) != 0

    payload = json.loads(capsys.readouterr().out)
    assert "operator confirmation" in payload["error"].lower()
    assert payload["db_connection_opened"] is False
    assert payload["db_writes_executed"] is False


def test_apply_requires_operator_confirmation_scope_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    bad_confirmation = _write_json(
        tmp_path / "bad_confirmation_scope.json",
        {"status": "approved", "operator": "pytest", "confirmation": "approved without scoped ledger target proof"},
    )
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index("--operator-confirmation") + 1] = str(bad_confirmation)

    assert executor.main(args) != 0

    payload = json.loads(capsys.readouterr().out)
    assert "operator confirmation" in payload["error"].lower()
    assert payload["db_connection_opened"] is False
    assert payload["db_writes_executed"] is False


def test_apply_uses_one_transaction_per_package(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)
    fake_conn = FakeConnection()
    monkeypatch.setattr(executor, "_connect", lambda target: fake_conn, raising=False)

    assert executor.main(["--apply", *_base_args(tmp_path)]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "applied"
    assert payload["db_connection_opened"] is True
    assert payload["db_writes_executed"] is True
    assert fake_conn.commits == 4
    assert fake_conn.rollbacks == 0
    assert fake_conn.closed is True


def test_apply_rolls_back_failed_package_transaction(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)
    fake_conn = FakeConnection(fail_on_execute_number=3)
    monkeypatch.setattr(executor, "_connect", lambda target: fake_conn, raising=False)

    assert executor.main(["--apply", *_base_args(tmp_path)]) != 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
    assert payload["db_writes_executed"] is False
    assert fake_conn.rollbacks >= 1
    assert fake_conn.closed is True


def test_apply_commits_prior_package_before_later_package_rollback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)
    fake_conn = FakeConnection(fail_on_execute_number=6)
    monkeypatch.setattr(executor, "_connect", lambda target: fake_conn, raising=False)

    assert executor.main(["--apply", *_base_args(tmp_path)]) != 0

    json.loads(capsys.readouterr().out)
    assert fake_conn.commits >= 1
    assert fake_conn.rollbacks >= 1


def test_apply_output_contains_ledger_audit_rows_with_json_safe_details(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)
    fake_conn = FakeConnection()
    monkeypatch.setattr(executor, "_connect", lambda target: fake_conn, raising=False)

    assert executor.main(["--apply", *_base_args(tmp_path)]) == 0

    payload = json.loads(capsys.readouterr().out)
    audit_rows = payload["audit_rows"]
    assert len(audit_rows) == 4
    assert {row["package_id"] for row in audit_rows} == {"pkg_1", "pkg_2", "pkg_3", "pkg_4"}
    assert all("ledger" in row["action"] for row in audit_rows)
    assert all(row["dry_run"] is False for row in audit_rows)
    assert all(row["tables"] == ["strategy_pkg.package_asset"] for row in audit_rows)
    assert json.loads(json.dumps(audit_rows)) == audit_rows


def test_dry_run_preview_outputs_ledger_audit_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    bundle_path = _write_json(tmp_path / "bundle.json", _bundle())

    assert executor.main(["--evidence-bundle", str(bundle_path), "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "passed"
    assert payload["dry_run"] is True
    assert len(payload["audit_rows"]) == 4
    assert all("ledger" in row["action"] for row in payload["audit_rows"])


def test_sql_package_validation_accepts_reviewed_ledger_insert_update_sql() -> None:
    sql = """
    BEGIN;
    SET LOCAL lock_timeout = '3s';
    INSERT INTO strategy_pkg.package_asset (package_id, asset_type, asset_ref)
    VALUES ('pkg_1', 'protected_asset_ledger_evidence', 'governance/protected_asset_ledger_backfill');
    UPDATE strategy_pkg.package_asset SET protected_asset = TRUE WHERE package_id = 'pkg_1';
    COMMIT;
    """

    report = executor.validate_sql_package(sql, expected_package_ids={"pkg_1"})

    assert report["status"] == "passed"
    assert report["destructive_sql"] is False
    assert report["unexpected_package_ids"] == []


@pytest.mark.parametrize(
    "bad_sql",
    [
        "DROP TABLE strategy_pkg.package_asset;",
        "ALTER TABLE strategy_pkg.package_asset ADD COLUMN x int;",
        "TRUNCATE strategy_pkg.package_asset;",
    ],
)
def test_sql_package_validation_rejects_destructive_or_ddl_sql(bad_sql: str) -> None:
    with pytest.raises(_error_type()):
        executor.validate_sql_package(bad_sql, expected_package_ids={"pkg_1"})


def test_sql_package_validation_rejects_unexpected_package_id() -> None:
    sql = "INSERT INTO strategy_pkg.package_asset (package_id, asset_type) VALUES ('pkg_not_approved', 'protected_asset_ledger_evidence');"

    with pytest.raises(_error_type(), match="pkg_not_approved"):
        executor.validate_sql_package(sql, expected_package_ids={"pkg_1"})


def test_sql_package_validation_rejects_unexpected_table() -> None:
    sql = "INSERT INTO paper_v2.cash_ledger (package_id) VALUES ('pkg_1');"

    with pytest.raises(_error_type(), match="table|paper_v2|package_asset"):
        executor.validate_sql_package(sql, expected_package_ids={"pkg_1"})


def test_bundle_validation_rejects_manifest_mismatch_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    args = ["--apply", *_base_args(tmp_path, _bundle())]
    bundle_path = Path(args[args.index("--evidence-bundle") + 1])
    bundle_payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    bundle_payload["packages"][0]["manifest_sha256"] = "wrong_manifest"
    bundle_path.write_text(json.dumps(bundle_payload), encoding="utf-8")

    assert executor.main(args) != 0

    payload = json.loads(capsys.readouterr().out)
    assert "manifest" in payload["error"].lower()
    assert payload["db_connection_opened"] is False


def test_bundle_validation_rejects_unapproved_package_status_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    payload = _bundle()
    payload["packages"][0]["package_status"] = "DRAFT"

    assert executor.main(["--apply", *_base_args(tmp_path, payload)]) != 0

    body = capsys.readouterr().out
    assert "package_status" in body
    assert "db_connection_opened" in body


def test_bundle_validation_rejects_non_ledger_table_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    payload = _bundle()
    payload["packages"][0]["rows"][0]["table"] = "strategy_pkg.package_validation_run"

    assert executor.main(["--apply", *_base_args(tmp_path, payload)]) != 0

    body = capsys.readouterr().out
    assert "table" in body.lower()
    assert "db_connection_opened" in body


def test_bundle_validation_rejects_non_ledger_asset_type_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    payload = _bundle()
    payload["packages"][0]["rows"][0]["columns"]["asset_type"] = "validation_report"

    assert executor.main(["--apply", *_base_args(tmp_path, payload)]) != 0

    body = capsys.readouterr().out
    assert "asset_type" in body.lower() or "ledger" in body.lower()
    assert "db_connection_opened" in body


def test_plan_preview_package_set_must_match_bundle_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _forbid_connect(monkeypatch)
    _enable_prod_guards(monkeypatch)
    bundle = _bundle()
    args = ["--apply", *_base_args(tmp_path, bundle)]
    plan_payload = _plan_payload(Path(args[args.index("--plan-preview") + 1]))
    plan_payload["packages"][0]["package_id"] = "pkg_unexpected"
    mismatched_plan = _write_json(tmp_path / "mismatched_plan.json", plan_payload)
    args[args.index("--plan-preview") + 1] = str(mismatched_plan)
    args[args.index("--operator-confirmation") + 1] = str(_operator_confirmation(tmp_path, mismatched_plan))

    assert executor.main(args) != 0

    payload = json.loads(capsys.readouterr().out)
    assert "package" in payload["error"].lower()
    assert payload["db_connection_opened"] is False


def test_prod_executor_does_not_import_or_delegate_to_dev_locked_apply_script() -> None:
    source = Path(executor.__file__).read_text(encoding="utf-8")

    assert "scripts.protected_asset_ledger_backfill" not in source
    assert "protected_asset_ledger_backfill.py --apply" not in source
    assert "APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_DEV_ONLY" not in source
