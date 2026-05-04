from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from importlib.util import module_from_spec, spec_from_file_location

from backend.services.validation.execution_runner import (
    JOB_SCHEMA_VERSION,
    RunnerResult,
    ValidationExecutionRunner,
)
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


def test_plan_catalog_rejects_runner_enabled_business_state(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True, writes_business_state=True)

    with pytest.raises(ValidationCatalogError, match="controlled runner"):
        ValidationPlanCatalog(catalog_path).list_plans()


def test_runner_executes_allowlisted_plan_and_writes_evidence(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True)
    calls: list[tuple[list[str], dict[str, str], int]] = []

    def fake_executor(command, env, _cwd, timeout_seconds):
        calls.append((command, env, timeout_seconds))
        return RunnerResult(return_code=0, output="fake nox ok\n")

    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        repo_root=Path.cwd(),
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
    assert runner.list_jobs(page=1, page_size=20)["total"] == 1
    assert runner.get_job(job["job_id"])["status"] == "passed"
    assert runner.get_job("../not_a_job") is None


def test_runner_rejects_non_runner_plan_and_forbidden_port(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=False)
    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        repo_root=Path.cwd(),
        run_inline=True,
    )

    with pytest.raises(ValueError, match="not enabled for controlled runner"):
        runner.start_job(plan_key="l0")

    backend_catalog = tmp_path / "backend_plans.yaml"
    _write_backend_catalog(backend_catalog)
    backend_runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(backend_catalog),
        execution_root=tmp_path / "backend_jobs",
        repo_root=Path.cwd(),
        run_inline=True,
    )
    with pytest.raises(ValueError, match="forbidden production backend port"):
        backend_runner.start_job(plan_key="qe_read_l3", backend_port=8001, frontend_port=3011)


def test_validation_execution_api_starts_and_lists_job(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, runner_enabled=True)

    def fake_executor(_command, _env, _cwd, _timeout_seconds):
        return RunnerResult(return_code=0, output="api runner ok\n")

    runner = ValidationExecutionRunner(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_root=tmp_path / "jobs",
        repo_root=Path.cwd(),
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

    rejected = client.post("/api/v1/validation/executions", json={"plan_key": "missing"})
    assert rejected.status_code == 400
