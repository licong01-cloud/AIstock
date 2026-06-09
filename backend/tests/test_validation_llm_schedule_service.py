from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.validation.execution_runner import RunnerResult, ValidationExecutionRunner
from backend.services.validation.llm_schedule_service import SCHEDULE_SCHEMA_VERSION, ValidationLlmScheduleService
from backend.services.validation.plan_catalog import ValidationPlanCatalog

_ROUTER_PATH = Path(__file__).resolve().parents[1] / "routers" / "validation.py"
_ROUTER_SPEC = spec_from_file_location("backend.routers.validation", _ROUTER_PATH)
assert _ROUTER_SPEC is not None and _ROUTER_SPEC.loader is not None
validation = module_from_spec(_ROUTER_SPEC)
_ROUTER_SPEC.loader.exec_module(validation)


def _write_l0_catalog(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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
        "    writes_business_state: false\n"
        "    runner_enabled: true\n"
        "    max_duration_seconds: 60\n",
        encoding="utf-8",
    )


def _runner(tmp_path: Path, catalog: ValidationPlanCatalog, calls: list[tuple[list[str], Path]]) -> ValidationExecutionRunner:
    def fake_executor(command, _env, cwd, _timeout_seconds):
        calls.append((command, cwd))
        return RunnerResult(return_code=0, output="schedule runner ok\n")

    return ValidationExecutionRunner(
        plan_catalog=catalog,
        execution_root=tmp_path / "jobs",
        history_root=tmp_path / "history",
        repo_root=tmp_path,
        executor=fake_executor,
        run_inline=True,
    )


def test_llm_schedule_dry_run_is_plan_key_only_and_does_not_execute(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_l0_catalog(catalog_path)
    calls: list[tuple[list[str], Path]] = []
    service = ValidationLlmScheduleService(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_runner=_runner(tmp_path, ValidationPlanCatalog(catalog_path), calls),
    )

    decision = service.schedule(provider="deterministic", codegraph_freshness="fresh")

    assert decision["schema_version"] == SCHEDULE_SCHEMA_VERSION
    assert decision["workflow_gate"] == "ready"
    assert decision["queue"] == [
        {
            "plan_key": "l0",
            "priority": "baseline",
            "reason": "fixed nightly baseline",
            "budget_seconds": 60,
            "allowed": True,
            "deferred_reason": None,
            "module": "development_guardrails",
            "level": "L0",
            "runner_enabled": True,
        }
    ]
    assert decision["execute"] is False
    assert decision["run_count"] == 0
    assert decision["runs"] == []
    assert decision["shell_commands_allowed"] is False
    assert decision["production_actions_allowed"] is False
    assert decision["llm_invocation_evidence"]["invoked"] is False
    assert calls == []


def test_llm_schedule_execute_runs_only_allowed_runner_plan_and_links_evidence(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_l0_catalog(catalog_path)
    catalog = ValidationPlanCatalog(catalog_path)
    calls: list[tuple[list[str], Path]] = []
    service = ValidationLlmScheduleService(
        plan_catalog=catalog,
        execution_runner=_runner(tmp_path, catalog, calls),
    )

    decision = service.schedule(
        provider="deterministic",
        codegraph_freshness="fresh",
        execute=True,
        requested_by="pytest",
        failure_event_ref="fe://unit",
        bug_id="BUG-999",
        github_issue_number=999,
    )

    assert decision["workflow_gate"] == "ready"
    assert decision["gate"]["allowed_to_schedule_validation"] is True
    assert decision["run_count"] == 1
    assert decision["runs"][0]["status"] == "passed"
    assert decision["runs"][0]["plan_key"] == "l0"
    assert calls[0][0][-2:] == ["-s", "l0"]
    assert calls[0][1].resolve() == tmp_path.resolve()
    assert decision["run_evidence_links"] == [
        {
            "job_id": decision["runs"][0]["job_id"],
            "plan_key": "l0",
            "failure_event_ref": "fe://unit",
            "bug_id": "BUG-999",
            "github_issue_number": 999,
            "github_issue_url": None,
            "evidence_path": decision["runs"][0]["evidence_path"],
            "run_record_path": decision["runs"][0]["archive"]["run_record_path"],
        }
    ]


def test_llm_schedule_warning_codegraph_still_allows_runner_execution(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_l0_catalog(catalog_path)
    catalog = ValidationPlanCatalog(catalog_path)
    service = ValidationLlmScheduleService(
        plan_catalog=catalog,
        execution_runner=_runner(tmp_path, catalog, []),
    )

    decision = service.schedule(
        provider="deterministic",
        codegraph_freshness="missing",
        execute=True,
    )

    assert decision["workflow_gate"] == "warning"
    assert decision["run_count"] == 1
    assert decision["test_plan_advice_gate"]["workflow_gate"] == "ready"


def test_llm_schedule_execute_rejects_blocked_workspace_path(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_l0_catalog(catalog_path)
    service = ValidationLlmScheduleService(
        plan_catalog=ValidationPlanCatalog(catalog_path),
        execution_runner=_runner(tmp_path, ValidationPlanCatalog(catalog_path), []),
    )

    missing = tmp_path / "missing-worktree"
    try:
        service.schedule(
            provider="deterministic",
            codegraph_freshness="fresh",
            execute=True,
            workspace_path=str(missing),
        )
    except ValueError as exc:
        assert "LLM schedule gate is not ready" in str(exc)
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("blocked workspace_path should reject execute=True")


def test_llm_schedule_api_returns_compact_decision_and_http_400_for_blocked_execute(tmp_path: Path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_l0_catalog(catalog_path)
    catalog = ValidationPlanCatalog(catalog_path)
    runner = _runner(tmp_path, catalog, [])
    app = FastAPI()
    app.include_router(validation.router, prefix="/api/v1")
    app.dependency_overrides[validation.get_plan_catalog] = lambda: catalog
    app.dependency_overrides[validation.get_execution_runner] = lambda: runner
    client = TestClient(app)

    response = client.post(
        "/api/v1/validation/llm/schedule",
        json={"provider": "deterministic", "codegraph_freshness": "fresh", "execute": False},
    )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["schema_version"] == SCHEDULE_SCHEMA_VERSION
    assert payload["queue"][0]["plan_key"] == "l0"
    assert payload["run_count"] == 0

    rejected = client.post(
        "/api/v1/validation/llm/schedule",
        json={
            "provider": "deterministic",
            "codegraph_freshness": "fresh",
            "execute": True,
            "workspace_path": str(tmp_path / "not-registered"),
        },
    )

    assert rejected.status_code == 400
    assert "LLM schedule gate is not ready" in rejected.json()["detail"]
