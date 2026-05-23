from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from importlib.util import module_from_spec, spec_from_file_location

from backend.services.validation.execution_runner import (
    JOB_SCHEMA_VERSION,
    RunnerResult,
    ValidationExecutionRunner,
    ValidationRunnerError,
)
from backend.services.validation.history_store import COVERAGE_SCHEMA, EVIDENCE_SCHEMA, RUN_SCHEMA, ValidationHistoryStore
from backend.services.validation.plan_catalog import ValidationCatalogError, ValidationPlanCatalog


_ROUTER_PATH = Path(__file__).resolve().parents[1] / "routers" / "validation.py"
_ROUTER_SPEC = spec_from_file_location("backend.routers.validation", _ROUTER_PATH)
assert _ROUTER_SPEC is not None and _ROUTER_SPEC.loader is not None
validation = module_from_spec(_ROUTER_SPEC)
_ROUTER_SPEC.loader.exec_module(validation)


def _write_catalog(path: Path, *, runner_enabled: bool = True, writes_business_state: bool = False) -> None:
    path.write_text(
        "schema_version: aistock_validation_plans_v1\n"
        "plans:\n"
        "  - plan_key: l0\n"
        "    title: L0 static guardrail gate\n"
        "    module: development_guardrails\n"
        "    level: L0\n"
        "    command_key: nox_l0\n"
        "    nox_session: l0\n"
        "    enabled: true\n"
        "    requires_backend: false\n"
        "    requires_frontend: false\n"
        "    allowed_backend_ports: []\n"
        "    allowed_frontend_ports: []\n"
        "    writes_database: false\n"
        "    writes_artifacts: true\n"
        f"    writes_business_state: {'true' if writes_business_state else 'false'}\n"
        f"    requires_confirmation: {'true' if writes_business_state else 'false'}\n"
        f"    runner_enabled: {'true' if runner_enabled else 'false'}\n"
        "    max_duration_seconds: 60\n",
        encoding="utf-8",
    )


def _write_backend_catalog(path: Path) -> None:
    path.write_text(
        "schema_version: aistock_validation_plans_v1\n"
        "plans:\n"
        "  - plan_key: qe_read_l3\n"
        "    title: QE read-only local L3 validation\n"
        "    module: qe\n"
        "    level: L3\n"
        "    command_key: nox_qe_read_l3\n"
        "    nox_session: qe_read_l3\n"
        "    enabled: true\n"
        "    requires_backend: true\n"
        "    requires_frontend: true\n"
        "    allowed_backend_ports: [8011]\n"
        "    allowed_frontend_ports: [3011]\n"
        "    writes_database: false\n"
        "    writes_artifacts: true\n"
        "    writes_business_state: false\n"
        "    runner_enabled: true\n"
        "    max_duration_seconds: 60\n",
        encoding="utf-8",
    )


def _write_data_sync_catalog(path: Path) -> None:
    path.write_text(
        "schema_version: aistock_validation_plans_v1\n"
        "plans:\n"
        "  - plan_key: data_sync_autonomy_backend\n"
        "    title: Data sync autonomy backend regression\n"
        "    module: local_data_management\n"
        "    level: L2\n"
        "    command_key: nox_data_sync_autonomy_backend\n"
        "    nox_session: data_sync_autonomy_backend\n"
        "    enabled: true\n"
        "    requires_backend: false\n"
        "    requires_frontend: false\n"
        "    allowed_backend_ports: []\n"
        "    allowed_frontend_ports: []\n"
        "    writes_database: false\n"
        "    writes_artifacts: true\n"
        "    writes_business_state: false\n"
        "    runner_enabled: true\n"
        "    max_duration_seconds: 300\n",
        encoding="utf-8",
    )


def test_plan_catalog_rejects_runner_enabled_business_state(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True, writes_business_state=True)

    with pytest.raises(ValidationCatalogError, match="controlled runner"):
        ValidationPlanCatalog(catalog_path).list_plans()


def test_runner_executes_allowlisted_plan_and_writes_evidence(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True)
    calls: list[tuple[list[str], dict[str, str], int]] = []

    def fake_executor(command, env, cwd, timeout_seconds):
        calls.append((command, env, timeout_seconds))
        coverage_path = cwd / "tmp" / "validation" / "coverage" / "l0_snapshot.json"
        coverage_path.parent.mkdir(parents=True, exist_ok=True)
        coverage_path.write_text(
            json.dumps(
                {
                    "schema_version": COVERAGE_SCHEMA,
                    "module": "development_guardrails",
                    "level": "L0",
                    "title": "fake coverage",
                    "status": "passed",
                    "generated_at": "2026-05-04T12:00:00+00:00",
                    "totals": {"line_percent": 88.0, "branch_percent": 66.0},
                    "quality_gates": [],
                    "failed_gates": [],
                }
            )
            + "\n",
            encoding="utf-8",
        )
        guardrail_md = cwd / "tmp" / "validation" / "guardrails" / "l0_paths.md"
        guardrail_md.parent.mkdir(parents=True, exist_ok=True)
        guardrail_md.write_text("# Guardrail details\n", encoding="utf-8")
        return RunnerResult(return_code=0, output="fake nox ok\n")

    history_root = tmp_path / "history"
    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        history_root=history_root,
        repo_root=tmp_path,
        executor=fake_executor,
        run_inline=True,
    )

    job = runner.start_job(plan_key="l0", requested_by="pytest", timeout_seconds=30)

    assert job["schema_version"] == JOB_SCHEMA_VERSION
    assert job["status"] == "passed"
    assert job["return_code"] == 0
    assert job["production_8001_touched"] is False
    assert job["arbitrary_shell_allowed"] is False
    assert calls[0][0][-2:] == ["-s", "l0"]
    assert calls[0][1]["VALIDATION_RUNNER_PLAN_KEY"] == "l0"
    assert calls[0][2] == 30
    log_path = Path(job["log_path"])
    if not log_path.is_absolute():
        log_path = Path.cwd() / log_path
    assert "fake nox ok" in log_path.read_text(encoding="utf-8")
    evidence_path = Path(job["evidence_path"])
    if not evidence_path.is_absolute():
        evidence_path = Path.cwd() / evidence_path
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["schema_version"] == "aistock_validation_runner_evidence_v1"
    assert evidence["status"] == "passed"
    assert evidence["archive"]["status"] == "archived"
    archive = job["archive"]
    assert archive["run_id"]
    assert archive["run_record_path"].endswith("-validation.md")
    assert archive["coverage_snapshot_path"].endswith("-coverage-snapshot.json")
    assert any(path.endswith("-guardrail-md.txt") for path in archive["artifact_paths"])
    metadata_path = Path(archive["metadata_path"])
    run_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert run_metadata["schema_version"] == RUN_SCHEMA
    assert run_metadata["status"] == "passed"
    assert run_metadata["runner_job_id"] == job["job_id"]
    standard_evidence = json.loads(Path(archive["evidence_manifest_path"]).read_text(encoding="utf-8"))
    assert standard_evidence["schema_version"] == EVIDENCE_SCHEMA
    assert standard_evidence["missing_count"] == 0
    history_runs = ValidationHistoryStore(history_root=history_root, repo_root=tmp_path).list_runs(module="development_guardrails")
    assert history_runs["total"] == 1
    assert runner.list_jobs(page=1, page_size=20)["total"] == 1
    assert runner.list_jobs(page=1, page_size=20, plan_key="l0")["total"] == 1
    assert "fake nox ok" in runner.get_job_log(job["job_id"], tail_lines=10)["content"]
    assert runner.get_job_evidence(job["job_id"])["standard_evidence"]["schema_version"] == EVIDENCE_SCHEMA
    evidence_path.unlink()
    archived_evidence = runner.get_job_evidence(job["job_id"])
    assert archived_evidence["runner_evidence"]["schema_version"] == "aistock_validation_runner_evidence_v1"
    assert archived_evidence["runner_evidence_path"] == archive["runner_evidence_archive_path"]
    assert runner.get_job(job["job_id"])["status"] == "passed"
    assert runner.get_job("../not_a_job") is None


def test_runner_archives_failed_job(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True)

    def fake_executor(_command, _env, _cwd, _timeout_seconds):
        return RunnerResult(return_code=7, output="fake failure\n")

    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        history_root=tmp_path / "history",
        repo_root=tmp_path,
        executor=fake_executor,
        run_inline=True,
    )

    job = runner.start_job(plan_key="l0", requested_by="pytest", timeout_seconds=30)

    assert job["status"] == "failed"
    assert job["archive"]["status"] == "archived"
    metadata = json.loads(Path(job["archive"]["metadata_path"]).read_text(encoding="utf-8"))
    assert metadata["status"] == "failed"
    assert metadata["business_assertion"]["can_user_complete_operation"] is False


def test_runner_marks_executor_exception_failed_and_archives(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True)

    def failing_executor(_command, _env, _cwd, _timeout_seconds):
        raise RuntimeError("boom")

    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        history_root=tmp_path / "history",
        repo_root=tmp_path,
        executor=failing_executor,
        run_inline=True,
    )

    job = runner.start_job(plan_key="l0", requested_by="pytest", timeout_seconds=30)

    assert job["status"] == "failed"
    assert "RuntimeError: boom" in job["error"]
    assert job["archive"]["status"] == "archived"
    assert "validation runner executor error: RuntimeError: boom" in runner.get_job_log(job["job_id"])["content"]
    metadata = json.loads(Path(job["archive"]["metadata_path"]).read_text(encoding="utf-8"))
    assert metadata["status"] == "failed"


def test_runner_rejects_non_runner_plan_and_forbidden_port(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=False)
    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        history_root=tmp_path / "history",
        repo_root=tmp_path,
        run_inline=True,
    )

    with pytest.raises(ValueError, match="not enabled for controlled runner"):
        runner.start_job(plan_key="l0")

    backend_catalog = tmp_path / "backend_plans.yaml"
    _write_backend_catalog(backend_catalog)
    backend_runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(backend_catalog),
        execution_root=tmp_path / "backend_jobs",
        history_root=tmp_path / "backend_history",
        repo_root=tmp_path,
        run_inline=True,
    )
    with pytest.raises(ValueError, match="forbidden production backend port"):
        backend_runner.start_job(plan_key="qe_read_l3", backend_port=8001, frontend_port=3011)


def test_runner_executes_data_sync_autonomy_allowlisted_nox_session(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_data_sync_catalog(catalog_path)
    calls: list[list[str]] = []

    def fake_executor(command, _env, _cwd, _timeout_seconds):
        calls.append(command)
        return RunnerResult(return_code=0, output="data sync nox ok\n")

    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        history_root=tmp_path / "history",
        repo_root=tmp_path,
        executor=fake_executor,
        run_inline=True,
    )

    job = runner.start_job(
        plan_key="data_sync_autonomy_backend",
        requested_by="pytest",
        timeout_seconds=300,
    )

    assert job["status"] == "passed"
    assert job["command_key"] == "nox_data_sync_autonomy_backend"
    assert calls[0][-2:] == ["-s", "data_sync_autonomy_backend"]
    assert job["production_8001_touched"] is False


def test_validation_execution_api_starts_and_lists_job(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True)

    def fake_executor(_command, _env, _cwd, _timeout_seconds):
        return RunnerResult(return_code=0, output="api runner ok\n")

    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        history_root=tmp_path / "history",
        repo_root=tmp_path,
        executor=fake_executor,
        run_inline=True,
    )
    app = FastAPI()
    app.include_router(validation.router, prefix="/api/v1")
    app.dependency_overrides[validation.get_plan_catalog] = lambda: ValidationPlanCatalog(catalog_path)
    app.dependency_overrides[validation.get_execution_runner] = lambda: runner
    client = TestClient(app)

    created = client.post(
        "/api/v1/validation/executions",
        json={"plan_key": "l0", "requested_by": "pytest", "timeout_seconds": 30},
    )

    assert created.status_code == 200
    job = created.json()["data"]
    assert job["status"] == "passed"
    assert job["nox_session"] == "l0"
    listed = client.get("/api/v1/validation/executions").json()["data"]
    assert listed["total"] == 1
    detail = client.get(f"/api/v1/validation/executions/{job['job_id']}").json()["data"]
    assert detail["job_id"] == job["job_id"]
    log = client.get(f"/api/v1/validation/executions/{job['job_id']}/log").json()["data"]
    assert "api runner ok" in log["content"]
    execution_evidence = client.get(f"/api/v1/validation/executions/{job['job_id']}/evidence").json()["data"]
    assert execution_evidence["standard_evidence"]["schema_version"] == EVIDENCE_SCHEMA
    assert client.get("/api/v1/validation/executions/not-a-job/log").status_code == 404

    rejected = client.post("/api/v1/validation/executions", json={"plan_key": "missing"})
    assert rejected.status_code == 400


def _init_git_repo(path: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=str(path), check=True, shell=False)
    subprocess.run(["git", "config", "user.email", "test@test"], cwd=str(path), check=True, shell=False)
    subprocess.run(["git", "config", "user.name", "test"], cwd=str(path), check=True, shell=False)
    (path / "dummy.txt").write_text("hello", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=str(path), check=True, shell=False)
    subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=str(path), check=True, shell=False)
    subprocess.run(["git", "checkout", "-q", "-b", "feature/test-branch"], cwd=str(path), check=True, shell=False)


def test_workspace_path_accepted_for_allowlisted_worktree(tmp_path: Path) -> None:
    worktree = tmp_path / "worktrees" / "bug-xxx"
    worktree.mkdir(parents=True)
    _init_git_repo(worktree)

    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True)
    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        history_root=tmp_path / "history",
        repo_root=tmp_path,
        run_inline=True,
        executor=lambda c, e, cwd, t: RunnerResult(return_code=0, output="ok"),
    )
    job = runner.start_job(plan_key="l0", workspace_path=str(worktree))

    assert job["status"] == "passed"
    assert job["workspace_is_root"] is False
    assert job["workspace_branch"] == "feature/test-branch"
    assert job["workspace_commit"] is not None
    assert str(worktree.resolve()) in job["workspace_path"]


def test_workspace_path_rejects_path_outside_allowlist(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True)
    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        history_root=tmp_path / "history",
        repo_root=tmp_path,
        run_inline=True,
    )
    outside_dir = Path(os.environ.get("TEMP", "C:/Windows/Temp")) / "aistock_test_reject"
    outside_dir.mkdir(parents=True, exist_ok=True)
    try:
        with pytest.raises(ValidationRunnerError, match="workspace_path is not in the allowlist"):
            runner.start_job(plan_key="l0", workspace_path=str(outside_dir))
    finally:
        shutil.rmtree(outside_dir, ignore_errors=True)


def test_workspace_path_rejects_non_directory(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True)
    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        history_root=tmp_path / "history",
        repo_root=tmp_path,
        run_inline=True,
    )
    nonexistent = tmp_path / "nonexistent_dir"
    with pytest.raises(ValidationRunnerError, match="workspace_path is not a directory"):
        runner.start_job(plan_key="l0", workspace_path=str(nonexistent))


def test_workspace_path_none_defaults_to_repo_root(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True)
    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        history_root=tmp_path / "history",
        repo_root=tmp_path,
        run_inline=True,
        executor=lambda c, e, cwd, t: RunnerResult(return_code=0, output="ok"),
    )
    job = runner.start_job(plan_key="l0")

    assert job["status"] == "passed"
    assert job["workspace_is_root"] is True
    assert tmp_path.resolve() == Path(job["workspace_path"]).resolve()


def test_expected_branch_mismatch_rejected(tmp_path: Path) -> None:
    worktree = tmp_path / "worktrees" / "bug-xxx"
    worktree.mkdir(parents=True)
    _init_git_repo(worktree)

    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True)
    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        history_root=tmp_path / "history",
        repo_root=tmp_path,
        run_inline=True,
    )
    with pytest.raises(ValidationRunnerError, match="expected_branch.*does not match"):
        runner.start_job(plan_key="l0", workspace_path=str(worktree), expected_branch="wrong-branch")
