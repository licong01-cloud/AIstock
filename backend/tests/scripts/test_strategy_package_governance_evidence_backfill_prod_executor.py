from __future__ import annotations

import importlib
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

executor = importlib.import_module("scripts.strategy_package_governance_evidence_backfill_prod_executor")


CONFIRM_TOKEN = "APPLY_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD"
ENV_FLAG = "AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_PROD_APPLY_ENABLED"
MUTEX_ENV = "AISTOCK_QE_GOVERNANCE_EVIDENCE_BACKFILL_MUTEX_HELD"


class FakeCursor:
    def __init__(self, conn: "FakeConnection") -> None:
        self.conn = conn

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def execute(self, sql: str, params: object | None = None) -> None:
        self.conn.executed.append((sql, params))
        self.conn.last_params = params
        if self.conn.fail_on_execute_number and len(self.conn.executed) == self.conn.fail_on_execute_number:
            raise RuntimeError("simulated package write failure")

    def fetchone(self) -> dict[str, Any]:
        package_id = "pkg_1"
        params = self.conn.last_params
        if isinstance(params, tuple) and params and isinstance(params[0], str) and params[0].startswith("pkg_"):
            package_id = params[0]
        suffix = package_id.rsplit("_", 1)[-1]
        return {"package_id": package_id, "manifest_sha256": f"manifest_{suffix}", "package_status": "BACKTEST_APPROVED"}

    def fetchall(self) -> list[dict[str, Any]]:
        return []


class FakeConnection:
    def __init__(self, *, fail_on_execute_number: int | None = None) -> None:
        self.executed: list[tuple[str, object | None]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.fail_on_execute_number = fail_on_execute_number
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


def _bundle(package_count: int = 4) -> dict[str, Any]:
    return {
        "schema_version": getattr(executor, "PLAN_SCHEMA_VERSION", "aistock_qe_governance_evidence_backfill_plan_v1"),
        "packages": [
            {
                "package_id": f"pkg_{idx}",
                "manifest_sha256": f"manifest_{idx}",
                "package_status": "BACKTEST_APPROVED",
                "required_gates": {
                    "manifest_identity": True,
                    "protected_assets": True,
                    "original_fixed_weight_retest": True,
                    "seed_sample_count_present": True,
                    "regime_sample_count_present": True,
                    "runtime_variant_candidate": True,
                },
                "blockers": [],
                "rows": [
                    {
                        "table": "strategy_pkg.package_validation_run",
                        "natural_key": {"validation_run_id": f"vr_{idx}"},
                        "action": "insert_append_only_validation_evidence",
                        "columns": {
                            "validation_run_id": f"vr_{idx}",
                            "package_id": f"pkg_{idx}",
                            "manifest_sha256": f"manifest_{idx}",
                            "validation_type": "original_fixed_weight",
                            "retrain_mode": "no_retrain",
                            "status": "PASSED",
                            "metrics_json": {"annual_return": 0.12},
                            "artifact_manifest_json": {"uri": f"artifact_{idx}"},
                            "evidence_json": {"source": "pytest"},
                            "created_by": "pytest",
                            "completed_at": "2026-05-11T00:00:00Z",
                        },
                    },
                    {
                        "table": "strategy_pkg.package_asset",
                        "natural_key": {
                            "package_id": f"pkg_{idx}",
                            "asset_type": "validation_report",
                            "asset_ref": f"governance/pkg_{idx}/report.json",
                        },
                        "action": "insert_or_update_protected_asset_metadata",
                        "columns": {
                            "package_id": f"pkg_{idx}",
                            "asset_type": "validation_report",
                            "asset_ref": f"governance/pkg_{idx}/report.json",
                            "asset_sha256": f"asset_hash_{idx}",
                            "metadata": {"source": "pytest"},
                            "asset_role": "governed_asset",
                            "protected_asset": True,
                        },
                    },
                    {
                        "table": "strategy_pkg.package_runtime_variant",
                        "natural_key": {"variant_id": f"rv_{idx}"},
                        "action": "insert_or_update_runtime_candidate_evidence",
                        "columns": {
                            "variant_id": f"rv_{idx}",
                            "package_id": f"pkg_{idx}",
                            "manifest_sha256": f"manifest_{idx}",
                            "locked_core_hash": "locked_core_hash",
                            "variant_name": "pytest variant",
                            "variant_kind": "risk_policy",
                            "variant_config": {"risk": "pytest"},
                            "variant_hash": f"variant_hash_{idx}",
                            "validation_status": "VALIDATION_PASSED",
                            "paper_candidate": True,
                            "validation_evidence": {"passed": True},
                            "created_by": "pytest",
                        },
                    },
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
        {"status": "verified", "snapshot_id": "snap-r6", "checksum": "sha256:abc", "completed_at": "2026-05-11T00:00:00Z"},
    )


def _plan_preview(tmp_path: Path, payload: dict[str, Any] | None = None) -> Path:
    bundle = payload or _bundle()
    return _write_json(
        tmp_path / "plan_preview.json",
        {
            "schema_version": bundle["schema_version"],
            "status": "passed",
            "mode": "dry_run_plan",
            "package_count": len(bundle["packages"]),
            "package_ids": [package["package_id"] for package in bundle["packages"]],
            "blocked_packages": {},
            "db_connection_opened": False,
            "db_writes_executed": False,
            "packages": bundle["packages"],
        },
    )


def _plan_preview_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _plan_preview_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _operator_confirmation(tmp_path: Path, plan_preview_path: Path | None = None) -> Path:
    plan_path = plan_preview_path or (tmp_path / "plan_preview.json")
    plan_payload = _plan_preview_payload(plan_path)
    package_ids = " ".join(plan_payload["package_ids"])
    confirmation = (
        f"{_token()} target=aistock_prod plan={_plan_preview_sha256(plan_path)} "
        f"dr=snap-r6 packages={package_ids}"
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


def test_default_cli_is_dry_run_and_does_not_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def forbidden_connect(*args: object, **kwargs: object) -> object:
        raise AssertionError("default dry-run must not connect to DB")

    monkeypatch.setattr(executor, "_connect", forbidden_connect, raising=False)
    bundle_path = _write_json(tmp_path / "bundle.json", _bundle())

    assert executor.main(["--evidence-bundle", str(bundle_path), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["db_writes_executed"] is False
    assert payload["target_db"] != "prod" or payload["mode"] in {"dry_run", "dry_run_plan", "static_preview"}


def test_apply_requires_explicit_apply_flag_even_with_other_guards(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)

    exit_code = executor.main(_base_args(tmp_path))

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["db_writes_executed"] is False


def test_apply_requires_mutex_guard(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv(_env_flag(), "true")
    monkeypatch.delenv(_mutex_env(), raising=False)

    exit_code = executor.main(["--apply", *_base_args(tmp_path)])

    assert exit_code != 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
    assert "mutex" in payload["error"].lower()
    assert payload["db_writes_executed"] is False


def test_apply_requires_exact_confirmation_token(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index("--confirm-apply") + 1] = "wrong-token"

    exit_code = executor.main(args)

    assert exit_code != 0
    payload = json.loads(capsys.readouterr().out)
    assert "confirm-apply" in payload["error"]
    assert payload["db_writes_executed"] is False


def test_apply_requires_enabled_environment_flag(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.delenv(_env_flag(), raising=False)
    monkeypatch.setenv(_mutex_env(), "true")

    exit_code = executor.main(["--apply", *_base_args(tmp_path)])

    assert exit_code != 0
    payload = json.loads(capsys.readouterr().out)
    assert _env_flag() in payload["error"]
    assert payload["db_writes_executed"] is False


def test_apply_requires_prod_target_triple_check(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index("--target-db") + 1] = "dev"

    exit_code = executor.main(args)

    assert exit_code != 0
    payload = json.loads(capsys.readouterr().out)
    assert "prod" in payload["error"].lower()
    assert payload["db_writes_executed"] is False


@pytest.mark.parametrize(
    ("flag", "expected"),
    [
        ("--dr-snapshot", "dr snapshot"),
        ("--plan-preview", "plan preview"),
        ("--operator-confirmation", "operator confirmation"),
    ],
)
def test_apply_refuses_missing_pre_apply_evidence_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    flag: str,
    expected: str,
) -> None:
    _enable_prod_guards(monkeypatch)
    args = ["--apply", *_base_args(tmp_path)]
    index = args.index(flag)
    del args[index : index + 2]

    exit_code = executor.main(args)

    assert exit_code != 0
    payload = json.loads(capsys.readouterr().out)
    assert expected in payload["error"].lower()
    assert payload["db_writes_executed"] is False


def test_apply_refuses_unverified_dr_snapshot(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)
    bad_snapshot = _write_json(tmp_path / "bad_snapshot.json", {"status": "pending", "snapshot_id": "snap-r6"})
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index("--dr-snapshot") + 1] = str(bad_snapshot)

    exit_code = executor.main(args)

    assert exit_code != 0
    payload = json.loads(capsys.readouterr().out)
    assert "dr snapshot" in payload["error"].lower()
    assert payload["db_writes_executed"] is False


def test_apply_refuses_blocked_plan_preview(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)
    blocked_plan = _write_json(
        tmp_path / "blocked_plan.json",
        {"status": "blocked", "mode": "dry_run_plan", "package_count": 4, "blocked_packages": {"pkg_1": ["missing"]}},
    )
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index("--plan-preview") + 1] = str(blocked_plan)

    exit_code = executor.main(args)

    assert exit_code != 0
    payload = json.loads(capsys.readouterr().out)
    assert "plan preview" in payload["error"].lower()
    assert payload["db_writes_executed"] is False


def test_apply_refuses_unapproved_operator_confirmation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)
    bad_confirmation = _write_json(tmp_path / "bad_confirmation.json", {"status": "draft", "operator": "pytest"})
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index("--operator-confirmation") + 1] = str(bad_confirmation)

    exit_code = executor.main(args)

    assert exit_code != 0
    payload = json.loads(capsys.readouterr().out)
    assert "operator confirmation" in payload["error"].lower()
    assert payload["db_writes_executed"] is False


def test_apply_requires_operator_confirmation_scope_before_connect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def forbidden_connect(*args: object, **kwargs: object) -> object:
        raise AssertionError("bad operator confirmation scope must fail before DB connect")

    monkeypatch.setattr(executor, "_connect", forbidden_connect, raising=False)
    _enable_prod_guards(monkeypatch)
    bad_confirmation = _write_json(
        tmp_path / "bad_confirmation_scope.json",
        {"status": "approved", "operator": "pytest", "confirmation": "approved without scoped target proof"},
    )
    args = ["--apply", *_base_args(tmp_path)]
    args[args.index("--operator-confirmation") + 1] = str(bad_confirmation)

    exit_code = executor.main(args)

    assert exit_code != 0
    payload = json.loads(capsys.readouterr().out)
    assert "operator confirmation" in payload["error"].lower()
    assert payload["db_connection_opened"] is False
    assert payload["db_writes_executed"] is False


def test_apply_uses_one_transaction_per_package(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)
    fake_conn = FakeConnection()
    monkeypatch.setattr(executor, "_connect", lambda target: fake_conn, raising=False)

    exit_code = executor.main(["--apply", *_base_args(tmp_path)])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "applied"
    assert payload["db_writes_executed"] is True
    assert fake_conn.commits == 4
    assert fake_conn.rollbacks == 0
    assert fake_conn.closed is True


def test_apply_rolls_back_only_failed_package_transaction(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)
    fake_conn = FakeConnection(fail_on_execute_number=2)
    monkeypatch.setattr(executor, "_connect", lambda target: fake_conn, raising=False)

    exit_code = executor.main(["--apply", *_base_args(tmp_path)])

    assert exit_code != 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
    assert payload["db_writes_executed"] is False
    assert fake_conn.rollbacks >= 1
    assert fake_conn.closed is True


def test_apply_output_contains_audit_rows_with_json_safe_details(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _enable_prod_guards(monkeypatch)
    fake_conn = FakeConnection()
    monkeypatch.setattr(executor, "_connect", lambda target: fake_conn, raising=False)

    assert executor.main(["--apply", *_base_args(tmp_path)]) == 0

    payload = json.loads(capsys.readouterr().out)
    audit_rows = payload["audit_rows"]
    assert len(audit_rows) == 4
    assert {row["package_id"] for row in audit_rows} == {"pkg_1", "pkg_2", "pkg_3", "pkg_4"}
    assert all(row["action"] == "governance_evidence_backfill_apply" for row in audit_rows)
    assert all(row["dry_run"] is False for row in audit_rows)
    assert json.loads(json.dumps(audit_rows)) == audit_rows


def test_sql_package_validation_accepts_reviewed_insert_update_sql() -> None:
    sql = """
    BEGIN;
    SET LOCAL lock_timeout = '3s';
    INSERT INTO strategy_pkg.package_validation_run (validation_run_id, package_id) VALUES ('vr_1', 'pkg_1')
    ON CONFLICT (validation_run_id) DO NOTHING;
    UPDATE strategy_pkg.package_asset SET protected_asset = TRUE WHERE package_id = 'pkg_1';
    COMMIT;
    """

    report = executor.validate_sql_package(sql, expected_package_ids={"pkg_1"})

    assert report["status"] == "passed"
    assert report["destructive_sql"] is False
    assert report["unexpected_package_ids"] == []


@pytest.mark.parametrize("bad_sql", ["DROP TABLE strategy_pkg.package;", "ALTER TABLE strategy_pkg.package ADD COLUMN x int;", "TRUNCATE strategy_pkg.package_asset;"])
def test_sql_package_validation_rejects_destructive_or_ddl_sql(bad_sql: str) -> None:
    with pytest.raises(getattr(executor, "GovernanceEvidenceBackfillProdExecutorError", RuntimeError)):
        executor.validate_sql_package(bad_sql, expected_package_ids={"pkg_1"})


def test_sql_package_validation_rejects_unexpected_package_id() -> None:
    sql = "INSERT INTO strategy_pkg.package_validation_run (validation_run_id, package_id) VALUES ('vr_x', 'pkg_not_approved');"

    with pytest.raises(getattr(executor, "GovernanceEvidenceBackfillProdExecutorError", RuntimeError), match="pkg_not_approved"):
        executor.validate_sql_package(sql, expected_package_ids={"pkg_1"})


def test_bundle_validation_rejects_manifest_mismatch_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def forbidden_connect(*args: object, **kwargs: object) -> object:
        raise AssertionError("invalid bundle must fail before DB connect")

    monkeypatch.setattr(executor, "_connect", forbidden_connect, raising=False)
    _enable_prod_guards(monkeypatch)
    payload = _bundle()
    payload["packages"][0]["rows"][0]["columns"]["manifest_sha256"] = "wrong_manifest"

    exit_code = executor.main(["--apply", *_base_args(tmp_path, payload)])

    assert exit_code != 0
    body = capsys.readouterr().out
    assert "manifest" in body.lower()


def test_bundle_validation_rejects_unapproved_package_status_before_connect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def forbidden_connect(*args: object, **kwargs: object) -> object:
        raise AssertionError("invalid package status must fail before DB connect")

    monkeypatch.setattr(executor, "_connect", forbidden_connect, raising=False)
    _enable_prod_guards(monkeypatch)
    payload = _bundle()
    payload["packages"][0]["package_status"] = "DRAFT"

    exit_code = executor.main(["--apply", *_base_args(tmp_path, payload)])

    assert exit_code != 0
    assert "package_status" in capsys.readouterr().out


def test_prod_executor_does_not_import_or_delegate_to_dev_locked_apply_scripts() -> None:
    source = Path(executor.__file__).read_text(encoding="utf-8")

    assert "scripts.strategy_package_evidence_backfill" not in source
    assert "scripts.protected_asset_ledger_backfill" not in source
    assert "strategy_package_evidence_backfill.py --apply" not in source
    assert "protected_asset_ledger_backfill.py --apply" not in source
