from __future__ import annotations

import argparse
import importlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest


wrapper = importlib.import_module("scripts.r6_prod_cutover_e2e_wrapper")

TOKEN = wrapper.CONFIRM_PROD
ENV_FLAG = wrapper.ENV_PROD_ENABLED
MUTEX_ENV = wrapper.ENV_MUTEX_HELD
NON_CUTOVER = wrapper.CONFIRM_NON_CUTOVER_HOURS
FINAL_INTENT = wrapper.FINAL_INTENT


def _dt(hour: int = 8, minute: int = 50) -> datetime:
    return datetime(2026, 5, 11, hour, minute)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _tmp_inputs(tmp_path: Path) -> dict[str, str]:
    release = tmp_path / "release"
    prod = tmp_path / "prod"
    evidence = tmp_path / "evidence"
    release.mkdir()
    prod.mkdir()
    evidence.mkdir()
    bundle = _write_json(evidence / "bundle.json", {"status": "passed"})
    plan = _write_json(evidence / "plan.json", {"status": "passed"})
    ledger = _write_json(evidence / "ledger.json", {"status": "passed"})
    dr = _write_json(evidence / "dr.json", {"status": "verified", "snapshot_ref": "dr-1"})
    ready1 = _write_text(evidence / "ready1.md", "R6 component READY\nGO=YES\n")
    ready2 = _write_text(evidence / "ready2.md", "paper-v2 VERIFY READY\nFULL R6 GO\n")
    return {
        "release_worktree": str(release),
        "prod_repo": str(prod),
        "secure_evidence_dir": str(evidence),
        "evidence_bundle": str(bundle),
        "evidence_plan": str(plan),
        "ledger_plan": str(ledger),
        "dr_snapshot": str(dr),
        "ready1": str(ready1),
        "ready2": str(ready2),
        "dr_snapshot_ref": "dr-1",
    }


def _operator_confirmation(inputs: dict[str, str]) -> str:
    values = [
        TOKEN,
        inputs["release_worktree"],
        inputs["prod_repo"],
        inputs["secure_evidence_dir"],
        inputs["evidence_bundle"],
        inputs["evidence_plan"],
        inputs["ledger_plan"],
        inputs["dr_snapshot"],
        inputs["ready1"],
        inputs["ready2"],
        inputs["dr_snapshot_ref"],
        FINAL_INTENT,
    ]
    return " ".join(values)


def _base_args(tmp_path: Path, **overrides: Any) -> argparse.Namespace:
    inputs = _tmp_inputs(tmp_path)
    data = {
        "mode": "prod",
        "confirm_prod": TOKEN,
        "non_cutover_hours_ok": NON_CUTOVER,
        "operator_confirmation": _operator_confirmation(inputs),
        "release_worktree": inputs["release_worktree"],
        "prod_repo": inputs["prod_repo"],
        "secure_evidence_dir": inputs["secure_evidence_dir"],
        "ready_doc": [inputs["ready1"], inputs["ready2"]],
        "min_ready_docs": 2,
        "evidence_bundle": inputs["evidence_bundle"],
        "evidence_plan": inputs["evidence_plan"],
        "ledger_plan": inputs["ledger_plan"],
        "dr_snapshot": inputs["dr_snapshot"],
        "dr_snapshot_ref": inputs["dr_snapshot_ref"],
        "backend_restart_command": json.dumps(["python", "-c", "print('backend ok')"]),
        "daemon_restart_command": json.dumps(["python", "-c", "print('daemon ok')"]),
        "api_base": "<PROD_API_BASE>",
        "health_path": "/health",
        "sentinel_endpoint": "/paper-v2/coldstart-sanity/sentinel-order",
        "daemon_process_name": "paper_v2",
        "package_id": ["pkg_1", "pkg_2", "pkg_3", "pkg_4"],
        "target_db": "prod",
        "db_host": "prod-db.invalid",
        "db_port": 5432,
        "db_name": "aistock",
        "db_user": "aistock_operator",
        "db_password_env": "AISTOCK_PROD_DB_PASSWORD",
        "command_timeout_seconds": 30,
        "json": True,
        "output": None,
    }
    data.update(overrides)
    return argparse.Namespace(**data)


def _enable_guards(monkeypatch: pytest.MonkeyPatch, hour: int = 8, minute: int = 50) -> None:
    monkeypatch.setenv(ENV_FLAG, "true")
    monkeypatch.setenv(MUTEX_ENV, "true")
    monkeypatch.setattr(wrapper, "_now_local", lambda: _dt(hour, minute), raising=False)


def test_default_dry_run_executes_no_commands(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    def forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError("dry-run must not execute subprocesses")

    monkeypatch.setattr(wrapper, "_run_command", forbidden, raising=False)
    rc = wrapper.main(["--json", "--release-worktree", "<R6_WORKTREE>"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["dry_run"] is True
    assert payload["prod_db_touched"] is False
    assert payload["production_services_touched"] is False
    assert len(payload["steps"]) >= 11
    assert {step["status"] for step in payload["steps"]} == {"SKIPPED"}


def test_dry_run_includes_seven_cutover_phases(tmp_path: Path) -> None:
    args = _base_args(tmp_path, mode="dry-run")
    report = wrapper.run_dry_run(args, wrapper._target_from_args(args))
    steps = [step["step"] for step in report["steps"]]
    assert steps[0:2] == ["preflight_static_plan", "preflight_static_migration_smoke"]
    assert len([step for step in steps if step.startswith("migration_")]) == 6
    assert "strategy_package_evidence_apply" in steps
    assert "protected_asset_ledger_apply" in steps
    assert "backend_restart" in steps
    assert "paper_v2_daemon_enable_restart" in steps
    assert "paper_v2_coldstart_sanity_prod" in steps


@pytest.mark.parametrize(
    ("overrides", "env_flag", "mutex", "needle"),
    [
        ({"confirm_prod": "WRONG"}, True, True, "confirm-prod"),
        ({}, False, True, ENV_FLAG),
        ({}, True, False, "mutex"),
        ({"operator_confirmation": ""}, True, True, "operator confirmation"),
        ({"target_db": "dev"}, True, True, "target-db prod"),
        ({"db_port": 5433}, True, True, "port 5432"),
        ({"db_name": "aistock_dev"}, True, True, "dev/test"),
    ],
)
def test_prod_guards_reject_before_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, Any],
    env_flag: bool,
    mutex: bool,
    needle: str,
) -> None:
    monkeypatch.setattr(wrapper, "_now_local", lambda: _dt(), raising=False)
    if env_flag:
        monkeypatch.setenv(ENV_FLAG, "true")
    if mutex:
        monkeypatch.setenv(MUTEX_ENV, "true")
    args = _base_args(tmp_path, **overrides)

    def forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError("guard failure must happen before subprocess")

    monkeypatch.setattr(wrapper, "_run_command", forbidden, raising=False)
    report = wrapper.run_prod(args, wrapper._target_from_args(args))
    assert report["verdict"] == "NO-GO"
    assert needle.lower() in report["steps"][-1]["message"].lower()
    assert report["production_services_touched"] is False


def test_prod_outside_cutover_window_requires_ack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_guards(monkeypatch, hour=10, minute=0)
    args = _base_args(tmp_path, non_cutover_hours_ok="")
    report = wrapper.run_prod(args, wrapper._target_from_args(args))
    assert report["verdict"] == "NO-GO"
    assert "non-cutover-hours-ok" in report["steps"][-1]["message"]


def test_prod_inside_cutover_window_does_not_require_ack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_guards(monkeypatch, hour=8, minute=50)
    args = _base_args(tmp_path, non_cutover_hours_ok="")

    def fake_execute(command: wrapper.PlannedCommand, **kwargs: Any) -> dict[str, Any]:
        return wrapper._step_payload(step=command.step, status="FAIL", message="stop after guards", command=command)

    monkeypatch.setattr(wrapper, "_execute_command_step", fake_execute, raising=False)
    report = wrapper.run_prod(args, wrapper._target_from_args(args))
    assert report["steps"][0]["step"] == "prod_guards"
    assert report["steps"][0]["status"] == "PASS"


def test_operator_confirmation_must_include_all_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_guards(monkeypatch)
    args = _base_args(tmp_path, operator_confirmation=f"{TOKEN} {FINAL_INTENT}")
    report = wrapper.run_prod(args, wrapper._target_from_args(args))
    assert report["verdict"] == "NO-GO"
    assert "prerequisite paths" in report["steps"][-1]["message"]


def test_ready_doc_rejects_caveat(tmp_path: Path) -> None:
    doc = _write_text(tmp_path / "ready.md", "READY-WITH-CAVEATS\n")
    with pytest.raises(wrapper.R6CutoverE2EError, match="not clean READY"):
        wrapper._validate_ready_doc(str(doc))


def test_ready_doc_accepts_clean_ready(tmp_path: Path) -> None:
    doc = _write_text(tmp_path / "ready.md", "component READY\nGO=YES\n")
    report = wrapper._validate_ready_doc(str(doc))
    assert report["status"] == "READY"
    assert len(report["sha256"]) == 64


def test_dr_snapshot_must_be_verified(tmp_path: Path) -> None:
    dr = _write_json(tmp_path / "dr.json", {"status": "failed", "snapshot_ref": "dr-1"})
    with pytest.raises(wrapper.R6CutoverE2EError, match="DR snapshot"):
        wrapper._validate_dr_snapshot(str(dr), "dr-1")


def test_command_json_requires_array() -> None:
    with pytest.raises(wrapper.R6CutoverE2EError, match="JSON array"):
        wrapper._command_from_json("not-json", label="backend", required=True)
    with pytest.raises(wrapper.R6CutoverE2EError, match="non-empty"):
        wrapper._command_from_json("[]", label="backend", required=True)


def test_planned_commands_use_single_transaction_per_migration(tmp_path: Path) -> None:
    args = _base_args(tmp_path)
    migrations = [cmd for cmd in wrapper._build_planned_commands(args) if cmd.step.startswith("migration_")]
    assert len(migrations) == 6
    assert all("--single-transaction" in cmd.command for cmd in migrations)
    assert all(cmd.ddl and cmd.db_write for cmd in migrations)
    assert [cmd.command[-1].replace("\\", "/").split("/")[-1] for cmd in migrations] == [Path(item).name for item in wrapper.MIGRATION_FILES]


def test_planned_commands_call_approved_executors(tmp_path: Path) -> None:
    args = _base_args(tmp_path)
    commands = {cmd.step: cmd for cmd in wrapper._build_planned_commands(args)}
    assert wrapper.STRATEGY_EXECUTOR_CONFIRM in commands["strategy_package_evidence_apply"].command
    assert wrapper.LEDGER_EXECUTOR_CONFIRM in commands["protected_asset_ledger_apply"].command
    assert "scripts/strategy_package_evidence_backfill.py" not in " ".join(commands["strategy_package_evidence_apply"].command)
    assert "scripts/protected_asset_ledger_backfill.py" not in " ".join(commands["protected_asset_ledger_apply"].command)


def test_coldstart_command_uses_task6_script_and_packages(tmp_path: Path) -> None:
    args = _base_args(tmp_path)
    command = [cmd for cmd in wrapper._build_planned_commands(args) if cmd.step == "paper_v2_coldstart_sanity_prod"][0]
    joined = " ".join(command.command)
    assert "scripts/paper_v2_coldstart_sanity.py" in joined
    assert wrapper.COLDSTART_CONFIRM in command.command
    assert command.command.count("--package-id") == 4
    assert command.service_touch is True


def test_redacted_command_masks_secret_values() -> None:
    assert wrapper._redacted_command(["tool", "--db-password", "secret", "--other", "ok"])[2] == "<REDACTED>"


def test_execute_command_step_writes_stdout_to_artifact(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    command = wrapper.PlannedCommand("echo", ["python", "-c", "print('ok')"], output_path=str(out))
    step = wrapper._execute_command_step(command, timeout=10)
    assert step["status"] == "PASS"
    assert out.read_text(encoding="utf-8").strip() == "ok"


def test_execute_command_step_failure_is_fail(tmp_path: Path) -> None:
    command = wrapper.PlannedCommand("bad", ["python", "-c", "import sys; sys.exit(7)"])
    step = wrapper._execute_command_step(command, timeout=10)
    assert step["status"] == "FAIL"
    assert step["returncode"] == 7


def test_run_prod_stops_after_first_failed_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_guards(monkeypatch)
    args = _base_args(tmp_path)
    calls: list[str] = []

    def fake_execute(command: wrapper.PlannedCommand, **kwargs: Any) -> dict[str, Any]:
        calls.append(command.step)
        status = "FAIL" if command.step == "migration_01" else "PASS"
        return wrapper._step_payload(step=command.step, status=status, message=status, command=command)

    monkeypatch.setattr(wrapper, "_execute_command_step", fake_execute, raising=False)
    report = wrapper.run_prod(args, wrapper._target_from_args(args))
    assert report["verdict"] == "NO-GO"
    assert calls == ["preflight_static_plan", "preflight_static_migration_smoke", "migration_01"]
    assert "migration_01" in report["failed_steps"]


def test_final_evidence_requires_coldstart_go(tmp_path: Path) -> None:
    args = _base_args(tmp_path)
    _write_json(Path(args.secure_evidence_dir) / "r6_strategy_package_evidence_backfill_apply.json", {"status": "applied"})
    _write_json(Path(args.secure_evidence_dir) / "r6_protected_asset_ledger_backfill_apply.json", {"status": "applied"})
    _write_json(Path(args.secure_evidence_dir) / "paper_v2_coldstart_sanity_prod.json", {"verdict": "NO-GO", "real_trading_ready": False})
    with pytest.raises(wrapper.R6CutoverE2EError, match="coldstart"):
        wrapper._final_evidence_checks(args)


def test_final_evidence_passes_with_go_artifacts(tmp_path: Path) -> None:
    args = _base_args(tmp_path)
    _write_json(Path(args.secure_evidence_dir) / "r6_strategy_package_evidence_backfill_apply.json", {"status": "applied"})
    _write_json(Path(args.secure_evidence_dir) / "r6_protected_asset_ledger_backfill_apply.json", {"status": "applied"})
    _write_json(Path(args.secure_evidence_dir) / "paper_v2_coldstart_sanity_prod.json", {"verdict": "GO", "real_trading_ready": True})
    result = wrapper._final_evidence_checks(args)
    assert result["coldstart_sanity"]["verdict"] == "GO"


def test_full_mocked_prod_path_go(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_guards(monkeypatch)
    args = _base_args(tmp_path)

    def fake_execute(command: wrapper.PlannedCommand, **kwargs: Any) -> dict[str, Any]:
        if command.output_path:
            payload = {"status": "passed"}
            if command.step == "strategy_package_evidence_apply":
                payload = {"status": "applied"}
            elif command.step == "protected_asset_ledger_apply":
                payload = {"status": "applied"}
            elif command.step == "paper_v2_coldstart_sanity_prod":
                payload = {"verdict": "GO", "real_trading_ready": True}
            Path(command.output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(command.output_path).write_text(json.dumps(payload), encoding="utf-8")
        return wrapper._step_payload(step=command.step, status="PASS", message="PASS", command=command)

    monkeypatch.setattr(wrapper, "_execute_command_step", fake_execute, raising=False)
    report = wrapper.run_prod(args, wrapper._target_from_args(args))
    assert report["verdict"] == "GO"
    assert report["real_trading_ready"] is True
    assert report["db_writes_executed"] is True
    assert report["ddl_executed"] is True
    assert report["prod_backend_port_touched"] is True


def test_main_prod_guard_failure_exit_two(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    args = ["--json", "--mode", "prod", "--release-worktree", str(tmp_path)]
    rc = wrapper.main(args)
    payload = json.loads(capsys.readouterr().out)
    assert rc == 2
    assert payload["verdict"] == "NO-GO"


def test_output_file_written_for_dry_run(tmp_path: Path) -> None:
    output = tmp_path / "wrapper.json"
    rc = wrapper.main(["--output", str(output), "--secure-evidence-dir", str(tmp_path / "evidence")])
    assert rc == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["mode"] == "dry-run"
    assert payload["verdict"] == "NO-GO"


def test_source_does_not_run_shell_or_service_commands_directly() -> None:
    source = Path("scripts/r6_prod_cutover_e2e_wrapper.py").read_text(encoding="utf-8")
    assert "shell=True" not in source
    assert "os.system" not in source
    assert "Restart-Service" not in source
    assert "systemctl restart" not in source


def test_source_has_no_main_merge_execution() -> None:
    source = Path("scripts/r6_prod_cutover_e2e_wrapper.py").read_text(encoding="utf-8")
    assert "git merge --" not in source
    assert "git", "merge" not in source
    assert "git reset --hard" not in source
    assert "main_merge_executed" in source


def test_wrapper_default_flags_no_prod_touch(tmp_path: Path) -> None:
    args = _base_args(tmp_path, mode="dry-run")
    report = wrapper.run_dry_run(args, wrapper._target_from_args(args))
    assert report["prod_db_touched"] is False
    assert report["prod_backend_port_touched"] is False
    assert report["main_merge_executed"] is False
    assert report["db_connection_opened_by_wrapper"] is False
