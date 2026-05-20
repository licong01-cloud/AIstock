from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import validation
from backend.services.validation.active_discovery import ActiveDiscoveryService


class _FakeHistoryStore:
    def list_runs(self, **kwargs):
        return {
            "items": [
                {
                    "run_id": "run_20260520_01",
                    "title": "Nightly discovery fixture",
                    "status": "passed",
                    "started_at": "2026-05-20T01:00:00+00:00",
                    "finished_at": "2026-05-20T01:15:00+00:00",
                },
                {"run_id": "run_20260519_01", "title": "Previous nightly fixture", "status": "failed"},
            ],
            "total": 2,
            "page": 1,
            "page_size": kwargs.get("page_size", 20),
            "has_more": False,
        }


class _FakeFindingStore:
    def list_bugs(self, **kwargs):
        return {
            "items": [
                {
                    "bug_id": "BUG-900",
                    "title": "QE fixed-pool drift",
                    "module": "qe",
                    "severity": "P1",
                    "status": "open",
                    "reproduce_command": "python -m pytest backend/tests/test_qe_fixed_pool.py",
                    "github_issue_number": 900,
                    "github_issue_url": "https://github.example/issues/900",
                },
                {
                    "bug_id": "BUG-901",
                    "title": "Selection UI missing trace",
                    "module": "selection",
                    "severity": "P2",
                    "status": "open",
                    "reproduce_command": "npx playwright test selection.spec.ts",
                },
            ],
            "total": 2,
            "page": 1,
            "page_size": kwargs.get("page_size", 20),
            "has_more": False,
        }

    def list_findings(self, **kwargs):
        return {
            "items": [
                {
                    "finding_id": "guardrail_ui_001",
                    "source_type": "guardrail",
                    "title": "Validation page without UI target coverage",
                    "module": "validation",
                    "severity": "P1",
                    "file_path": "frontend/src/app/validation/nightly-reports/page.tsx",
                    "first_seen_at": "2026-05-20T00:00:00+00:00",
                    "last_seen_at": "2026-05-20T00:00:00+00:00",
                }
            ],
            "total": 1,
            "page": 1,
            "page_size": kwargs.get("page_size", 20),
            "has_more": False,
        }


class _FakeExecutionRunner:
    def list_jobs(self, **kwargs):
        return {
            "items": [
                {
                    "job_id": "job_900",
                    "plan_key": "validation_center_backend",
                    "module": "validation",
                    "status": "passed",
                    "updated_at": "2026-05-20T02:00:00+00:00",
                }
            ],
            "total": 1,
            "page": 1,
            "page_size": kwargs.get("page_size", 20),
            "has_more": False,
        }


class _FakeModuleQuality:
    def module_quality_summary(self, **_kwargs):
        modules = []
        for module_id, display_name, coverage_status in [
            ("validation", "Validation Center", "passed"),
            ("qe", "QuantEvolver", "missing"),
            ("strategy_package", "Strategy Package", "passed"),
            ("selection", "Selection Center", "missing"),
            ("paper_v2", "Paper v2", "passed"),
        ]:
            modules.append(
                {
                    "module_id": module_id,
                    "display_name": display_name,
                    "coverage": {"status": coverage_status, "line_percent": 82.5 if coverage_status == "passed" else None},
                    "quality": {"bug_count": 1 if module_id in {"qe", "selection"} else 0, "finding_count": 1 if module_id == "validation" else 0},
                    "workspace": {"changed_file_count": 0},
                    "test_plans": {"required": ["validation_center_backend"]},
                }
            )
        return {"summary": {"module_count": len(modules)}, "modules": modules}


class _FakeUiTargetCatalog:
    def list_targets(self, **kwargs):
        return {
            "items": [
                {"route_id": "validation.nightly", "coverage_status": "missing"},
                {"route_id": "validation.candidates", "coverage_status": "partial"},
            ],
            "total": 2,
            "page": kwargs.get("page", 1),
            "page_size": kwargs.get("page_size", 20),
            "has_more": False,
        }

    def summary(self):
        return {"target_count": 2, "targets_requiring_action": 1}


class _FakePipelineCenter:
    def github_issues_summary(self):
        return {"bug_count": 2, "linked_count": 1, "missing_link_count": 1, "by_sync_state": {"linked": 1, "missing": 1}}

    def pipeline_tests_summary(self):
        return {"test_count": 3, "missing_evidence_count": 1}


def _write_repo_fixture(root: Path) -> None:
    (root / "backend" / "routers").mkdir(parents=True)
    (root / "backend" / "routers" / "validation.py").write_text(
        '@router.get("/validation/discovery")\ndef route():\n    return {}\n',
        encoding="utf-8",
    )
    (root / "frontend").mkdir(parents=True)
    (root / "frontend" / "playwright.config.ts").write_text("export default {};\n", encoding="utf-8")
    (root / "tests" / "aistock_validation" / "catalog").mkdir(parents=True)
    (root / "tests" / "aistock_validation" / "catalog" / "ui_targets.yaml").write_text("targets: []\n", encoding="utf-8")
    (root / "tests" / "aistock_validation" / "discovery_rules" / "semgrep").mkdir(parents=True)
    (root / "tests" / "aistock_validation" / "discovery_rules" / "api_fuzz_targets.yaml").write_text("targets: []\n", encoding="utf-8")
    (root / "tests" / "aistock_validation" / "llm_eval").mkdir(parents=True)
    (root / "tests" / "aistock_validation" / "bugs").mkdir(parents=True)
    (root / "tests" / "aistock_validation" / "bugs" / "bug.json").write_text(
        json.dumps({"github_issue_url": "https://github.example/issues/900"}, ensure_ascii=False),
        encoding="utf-8",
    )


def _service(tmp_path: Path) -> ActiveDiscoveryService:
    _write_repo_fixture(tmp_path)
    return ActiveDiscoveryService(
        repo_root=tmp_path,
        history_store=_FakeHistoryStore(),
        finding_store=_FakeFindingStore(),
        execution_runner=_FakeExecutionRunner(),
        module_quality_service=_FakeModuleQuality(),
        ui_target_catalog=_FakeUiTargetCatalog(),
        pipeline_center=_FakePipelineCenter(),
    )


def test_nightly_report_surfaces_modules_candidates_tasks_and_llm(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DEEPSEEK_V4_PRO_API_KEY", "secret-token-not-exposed")
    service = _service(tmp_path)

    summary = service.summary()
    report = service.get_nightly_report("current")
    llm_report = service.get_nightly_llm_report(report["report_id"])

    assert summary["candidate_by_severity"]["P1"] == 2
    assert {item["module_id"] for item in report["modules"]}.issuperset({"validation", "qe", "strategy_package", "selection", "paper_v2"})
    assert report["execution_tree"][0]["node_id"] == "nightly_baseline"
    assert report["llm_summary"]["provider_summary"]["deepseek"] == "configured"
    assert llm_report["profiles"][0]["secret_visible"] is False
    assert "secret-token-not-exposed" not in json.dumps(llm_report, ensure_ascii=False)


def test_candidate_review_and_promotion_preserve_github_sync_gate(tmp_path: Path) -> None:
    service = _service(tmp_path)
    candidate = service.get_candidate("ic_bug-900")
    assert candidate is not None
    assert candidate["github_issue_url"] == "https://github.example/issues/900"

    review = service.review_candidate("ic_bug-900", {"action": "accepted", "reviewer": "pytest", "evidence_checklist": ["log", "reproduce"]})
    promoted = service.promote_candidate("ic_bug-900", {"confirm_promote": "ic_bug-900", "reviewer": "pytest"})

    assert review["action"] == "accepted"
    assert promoted["promotion_status"] == "linked_existing_github_issue"
    assert promoted["github_issue_url"] == "https://github.example/issues/900"

    with pytest.raises(ValueError, match="confirm_promote"):
        service.promote_candidate("ic_bug-900", {"confirm_promote": "wrong", "reviewer": "pytest"})
    with pytest.raises(ValueError, match="requires reviewer"):
        service.promote_candidate("ic_guardrail_ui_001", {"confirm_promote": "ic_guardrail_ui_001"})


def test_task_l4_confirmation_agent_lifecycle_and_evidence_manifest(tmp_path: Path) -> None:
    service = _service(tmp_path)

    with pytest.raises(ValueError, match="confirm_schedule=L4"):
        service.schedule_task({"task_id": "task_l4", "module": "qe", "risk_level": "L4"})

    task = service.schedule_task({"task_id": "task_l4", "module": "qe", "risk_level": "L4", "confirm_schedule": "L4"})
    with pytest.raises(ValueError, match="confirm_run=task_l4"):
        service.run_task("task_l4", {"dry_run": False})

    claimed = service.claim_agent_task("task_l4", {"agent_runtime": "codex", "workspace": "F:/Dev/AIstock_worktrees/task_l4"})
    context = service.get_agent_context_pack("task_l4")
    evidence = service.attach_agent_evidence(
        "task_l4",
        {
            "logs": [{"kind": "pytest", "text": "passed"}],
            "api_responses": [{"path": "/api/v1/validation/discovery/tasks", "status": 200}],
            "mcp_responses": [{"tool": "mcp_github_issue_sync_bug", "status": "dry_run"}],
            "screenshots": [{"path": "artifacts/trace.png"}],
            "reproduce_command": "python -m pytest backend/tests/test_validation_active_discovery.py",
        },
    )
    completed = service.complete_agent_task("task_l4", {"status": "completed", "summary": "done"})

    assert claimed["status"] == "claimed"
    assert context["sensitive_payload_policy"].startswith("tokens")
    assert evidence["manifest_id"] == "evid_task_l4"
    assert evidence["reproduce_command"].startswith("python -m pytest")
    assert completed["status"] == "completed"


def test_tool_adapters_and_llm_eval_are_dry_run_safe(tmp_path: Path) -> None:
    service = _service(tmp_path)

    adapters = service.list_tool_adapters()["items"]
    assert {item["adapter_id"] for item in adapters}.issuperset(
        {
            "semgrep_business_rule_adapter",
            "schemathesis_api_fuzz_adapter",
            "playwright_trace_probe_adapter",
            "contract_alignment_adapter",
            "llm_eval_adapter",
        }
    )
    result = service.run_tool_adapter("playwright_trace_probe_adapter", {"dry_run": True})
    eval_result = service.run_llm_eval({"dry_run": True, "profiles": ["a", "b"]})

    assert result["dry_run"] is True
    assert result["result"]["trace_required"] is True
    assert eval_result["profiles_compared"] == ["a", "b"]


def test_router_exposes_active_discovery_endpoints(tmp_path: Path) -> None:
    service = _service(tmp_path)
    app = FastAPI()
    app.include_router(validation.router, prefix="/api/v1")
    app.dependency_overrides[validation.get_active_discovery_service] = lambda: service
    client = TestClient(app)

    summary = client.get("/api/v1/validation/discovery/summary")
    candidates = client.get("/api/v1/validation/discovery/candidates?severity=P1")
    bad_schedule = client.post("/api/v1/validation/discovery/tasks", json={"task_id": "bad", "risk_level": "L5"})

    assert summary.status_code == 200
    assert summary.json()["data"]["candidate_count"] >= 3
    assert candidates.status_code == 200
    assert candidates.json()["data"]["total"] == 2
    assert bad_schedule.status_code == 400
