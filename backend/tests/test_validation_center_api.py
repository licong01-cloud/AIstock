from __future__ import annotations

import json
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.validation.finding_store import (
    BUG_SCHEMA,
    GUARDRAIL_SCHEMA,
    LEGACY_SCHEMA,
    ValidationFindingStore,
)
from backend.services.validation.history_store import (
    COVERAGE_SCHEMA,
    EVIDENCE_SCHEMA,
    RUN_SCHEMA,
    ValidationHistoryStore,
)
from backend.services.validation.plan_catalog import ValidationCatalogError, ValidationPlanCatalog


_ROUTER_PATH = Path(__file__).resolve().parents[1] / "routers" / "validation.py"
_ROUTER_SPEC = spec_from_file_location("backend.routers.validation", _ROUTER_PATH)
assert _ROUTER_SPEC is not None and _ROUTER_SPEC.loader is not None
validation = module_from_spec(_ROUTER_SPEC)
sys.modules[_ROUTER_SPEC.name] = validation
_ROUTER_SPEC.loader.exec_module(validation)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_catalog(path: Path, *, backend_ports: list[int] | None = None, command_key: str = "nox_l0") -> None:
    payload = {
        "schema_version": "aistock_validation_plans_v1",
        "plans": [
            {
                "plan_key": "l0",
                "title": "L0",
                "module": "validation_center",
                "level": "L0",
                "command_key": command_key,
                "nox_session": "l0",
                "enabled": True,
                "requires_backend": bool(backend_ports),
                "requires_frontend": False,
                "allowed_backend_ports": backend_ports or [],
                "allowed_frontend_ports": [],
                "writes_database": False,
                "writes_artifacts": False,
                "writes_business_state": False,
            }
        ],
    }
    _write_json(path, payload)


def _write_history(history_root: Path) -> dict[str, Path]:
    module_dir = history_root / "validation_center"
    module_dir.mkdir(parents=True)

    run_md = module_dir / "20260504_120000_l2_validation-api.md"
    run_md.write_text("# Validation API Run\n\n- Final status: PASS\n", encoding="utf-8")
    _write_json(
        run_md.with_suffix(".json"),
        {
            "schema_version": RUN_SCHEMA,
            "module": "validation_center",
            "module_slug": "validation_center",
            "level": "L2",
            "title": "Validation API Run",
            "status": "passed",
            "git_commit": "abc1234",
            "operator": "pytest",
            "started_at": "2026-05-04T12:00:00",
            "finished_at": "2026-05-04T12:01:00",
            "coverage": {"schema_version": COVERAGE_SCHEMA, "status": "not_collected"},
            "quality_gates": [],
            "pass_scope": {
                "level": "L2",
                "real_backend": False,
                "real_database": False,
                "real_node_api": False,
                "real_frontend_click": False,
                "writes_business_state": False,
                "positive_business_success": False,
                "negative_failfast_only": False,
                "mock_api_used": False,
                "production_8001_touched": False,
            },
            "business_assertion": {
                "can_user_complete_operation": False,
                "operation_name": "read validation history",
                "evidence": {"api": "TestClient", "ui": "", "db": "", "logs": ""},
                "unresolved_blockers": ["read-only backend API test only"],
            },
        },
    )
    _write_json(
        module_dir / "20260504_120000_l2_validation-api-snapshot.json",
        {
            "schema_version": COVERAGE_SCHEMA,
            "generated_at": "2026-05-04T12:02:00",
            "module": "validation_center",
            "level": "L2",
            "title": "Validation API coverage",
            "run_id": None,
            "git_commit": "abc1234",
            "status": "passed",
            "totals": {
                "lines_valid": 10,
                "lines_covered": 9,
                "line_percent": 90.0,
                "branches_valid": 4,
                "branches_covered": 3,
                "branch_percent": 75.0,
            },
            "diff": {"enabled": False, "line_percent": None, "files": []},
            "quality_gates": [{"metric": "line", "status": "passed", "actual": 90.0, "threshold": 80.0}],
            "failed_gates": [],
            "files": [],
        },
    )
    _write_json(
        module_dir / "20260504_120000_l2_validation-api.evidence.json",
        {
            "schema_version": EVIDENCE_SCHEMA,
            "generated_at": "2026-05-04T12:03:00",
            "module": "validation_center",
            "level": "L2",
            "title": "Validation API evidence",
            "run_id": None,
            "git_commit": "abc1234",
            "evidence": [{"kind": "pytest", "path": "backend/tests/test_validation_center_api.py", "exists": True}],
            "missing": [],
            "missing_count": 0,
        },
    )

    markdown_only = module_dir / "20260504_121000_l1_markdown-only.md"
    markdown_only.write_text("# Markdown Only\n\nNo JSON sidecar.\n", encoding="utf-8")

    malformed = module_dir / "20260504_122000_l1_malformed.md"
    malformed.write_text("# Malformed Metadata\n", encoding="utf-8")
    malformed.with_suffix(".json").write_text("{bad json", encoding="utf-8")
    return {"run_md": run_md, "markdown_only": markdown_only, "malformed": malformed}


def _write_quality_inputs(tmp_path: Path) -> dict[str, Path]:
    guardrail_root = tmp_path / "guardrails"
    legacy_root = tmp_path / "legacy_inventory"
    bug_root = tmp_path / "bugs"
    _write_json(
        guardrail_root / "changed_scan.json",
        {
            "schema_version": GUARDRAIL_SCHEMA,
            "generated_at": "2026-05-04T13:00:00",
            "mode": "changed_only",
            "files_scanned": 2,
            "summary": {"total_findings": 1},
            "findings": [
                {
                    "rule_id": "NO-SILENT-FALLBACK",
                    "title": "No silent fallback",
                    "severity": "P1",
                    "category": "reliability",
                    "file": "backend/services/demo.py",
                    "line": 42,
                    "message": "Exception handler may hide a business failure.",
                    "remediation": "Fail fast and surface the error.",
                    "baseline_policy": "block_new_only",
                    "fingerprint": "guardrail_fp_001",
                }
            ],
        },
    )
    _write_json(
        legacy_root / "inventory.json",
        {
            "schema_version": LEGACY_SCHEMA,
            "generated_at": "2026-05-04T13:05:00",
            "mode": "paths",
            "files_scanned": 1,
            "summary": {"total_items": 1},
            "items": [
                {
                    "path": "scripts/old_debug_probe.py",
                    "category": "script_lifecycle_review",
                    "lifecycle_status": "delete_candidate",
                    "risk": "medium",
                    "confidence": "medium",
                    "recommended_action": "move_to_debug_tools_or_remove_after_review",
                    "signals": ["script_lifecycle_review"],
                    "references_found": 0,
                    "reference_examples": [],
                }
            ],
        },
    )
    _write_json(
        bug_root / "bug_demo.json",
        {
            "schema_version": BUG_SCHEMA,
            "bug_id": "bug_demo_001",
            "title": "Demo validation failure",
            "description": "A mocked validation failure for registry contract tests.",
            "module": "validation_center",
            "severity": "P2",
            "risk_area": "validation",
            "status": "detected",
            "trigger_condition": {"plan_key": "validation_center_backend"},
            "reproduce_command": "python -m nox -s validation_center_backend",
            "failing_run_id": "run_failed_demo",
            "evidence_uris": ["tests/aistock_validation/history/validation_center/demo.md"],
            "fingerprint": "bug_fp_001",
            "assigned_agent": "codex",
            "allowed_write_scope": ["backend/services/validation"],
            "suspected_modules": ["backend/services/validation"],
            "required_verification": ["python -m nox -s validation_center_backend"],
            "closure_requirements": ["verification_run_id required"],
            "created_at": "2026-05-04T13:10:00",
        },
    )
    return {"guardrail_root": guardrail_root, "legacy_root": legacy_root, "bug_root": bug_root}


@pytest.fixture()
def client(tmp_path) -> TestClient:
    history_root = tmp_path / "history"
    catalog_path = tmp_path / "test_plans.yaml"
    _write_history(history_root)
    _write_catalog(catalog_path)
    quality_roots = _write_quality_inputs(tmp_path)

    app = FastAPI()
    app.include_router(validation.router, prefix="/api/v1")
    app.dependency_overrides[validation.get_history_store] = lambda: ValidationHistoryStore(
        history_root=history_root,
        repo_root=tmp_path,
    )
    app.dependency_overrides[validation.get_plan_catalog] = lambda: ValidationPlanCatalog(catalog_path)
    app.dependency_overrides[validation.get_finding_store] = lambda: ValidationFindingStore(
        repo_root=tmp_path,
        guardrail_root=quality_roots["guardrail_root"],
        legacy_root=quality_roots["legacy_root"],
        bug_root=quality_roots["bug_root"],
    )
    return TestClient(app)


def test_plan_catalog_rejects_unknown_command_key(tmp_path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, command_key="shell_anything")

    with pytest.raises(ValidationCatalogError, match="non-allowlisted"):
        ValidationPlanCatalog(catalog_path).list_plans()


def test_plan_catalog_rejects_production_backend_port(tmp_path) -> None:
    catalog_path = tmp_path / "plans.yaml"
    _write_catalog(catalog_path, backend_ports=[8001])

    with pytest.raises(ValidationCatalogError, match="forbidden production"):
        ValidationPlanCatalog(catalog_path).list_plans()


def test_validation_health_and_plans_are_read_only(client: TestClient) -> None:
    health = client.get("/api/v1/validation/health").json()["data"]
    assert health["mode"] == "read_only"
    assert health["production_8001_touched"] is False
    assert health["plan_catalog"]["plan_count"] == 1
    assert health["quality"]["finding_count"] == 2
    assert health["quality"]["bug_count"] == 1

    plans = client.get("/api/v1/validation/plans").json()["data"]["plans"]
    assert plans[0]["plan_key"] == "l0"
    assert plans[0]["nox_session"] == "l0"


def test_runs_list_and_detail_preserve_success_scope(client: TestClient) -> None:
    response = client.get("/api/v1/validation/runs", params={"page_size": 20})
    assert response.status_code == 200
    runs = response.json()["data"]
    assert runs["total"] == 3
    valid_run = next(item for item in runs["items"] if item["title"] == "Validation API Run")
    assert valid_run["status"] == "passed"
    assert valid_run["success_scope_recorded"] is True
    assert valid_run["pass_scope"]["positive_business_success"] is False
    assert valid_run["coverage_snapshot_id"]
    assert valid_run["evidence_manifest_id"]

    detail = client.get(f"/api/v1/validation/runs/{valid_run['run_id']}").json()["data"]
    assert detail["metadata"]["schema_version"] == RUN_SCHEMA
    assert "Validation API Run" in detail["markdown_text"]
    assert detail["coverage_snapshot"]["totals"]["line_percent"] == 90.0
    assert detail["evidence_manifest"]["missing_count"] == 0


def test_runs_mark_metadata_missing_and_parse_error(client: TestClient) -> None:
    runs = client.get("/api/v1/validation/runs", params={"page_size": 20}).json()["data"]["items"]
    markdown_only = next(item for item in runs if item["title"] == "Markdown Only")
    malformed = next(item for item in runs if item["title"] == "Malformed Metadata")

    assert markdown_only["metadata_missing"] is True
    assert markdown_only["coverage_missing"] is True
    assert markdown_only["success_scope_recorded"] is False
    assert malformed["metadata_parse_error"].startswith("invalid JSON")

    filtered = client.get(
        "/api/v1/validation/runs",
        params={"include_markdown_only": False, "page_size": 20},
    ).json()["data"]
    assert filtered["total"] == 2


def test_coverage_and_evidence_detail_endpoints(client: TestClient) -> None:
    coverage_items = client.get("/api/v1/validation/coverage").json()["data"]["items"]
    assert coverage_items[0]["totals"]["branch_percent"] == 75.0
    coverage_id = coverage_items[0]["snapshot_id"]
    coverage_detail = client.get(f"/api/v1/validation/coverage/{coverage_id}").json()["data"]
    assert coverage_detail["snapshot"]["schema_version"] == COVERAGE_SCHEMA
    assert coverage_detail["summary"]["failed_gates"] == []

    evidence_items = client.get("/api/v1/validation/evidence").json()["data"]["items"]
    assert evidence_items[0]["missing_count"] == 0
    evidence_id = evidence_items[0]["manifest_id"]
    evidence_detail = client.get(f"/api/v1/validation/evidence/{evidence_id}").json()["data"]
    assert evidence_detail["manifest"]["schema_version"] == EVIDENCE_SCHEMA
    assert evidence_detail["summary"]["evidence_count"] == 1


def test_unknown_ids_return_404(client: TestClient) -> None:
    assert client.get("/api/v1/validation/runs/not-found").status_code == 404
    assert client.get("/api/v1/validation/coverage/not-found").status_code == 404
    assert client.get("/api/v1/validation/evidence/not-found").status_code == 404


def test_summary_reports_module_counts(client: TestClient) -> None:
    summary = client.get("/api/v1/validation/summary").json()["data"]
    assert summary["run_count"] == 3
    assert summary["coverage_snapshot_count"] == 1
    assert summary["evidence_manifest_count"] == 1
    assert summary["plan_count"] == 1
    assert summary["quality"]["finding_count"] == 2
    assert summary["quality"]["bug_count"] == 1
    assert summary["modules"][0]["module"] == "validation_center"


def test_quality_findings_and_bug_agent_context(client: TestClient) -> None:
    findings = client.get("/api/v1/validation/findings", params={"page_size": 20}).json()["data"]
    assert findings["total"] == 2
    guardrail = next(item for item in findings["items"] if item["source_type"] == "guardrail")
    assert guardrail["severity"] == "P1"
    assert guardrail["status"] == "detected"
    assert guardrail["allowed_write_scope"] == ["backend/services/demo.py"]

    finding_detail = client.get(f"/api/v1/validation/findings/{guardrail['finding_id']}").json()["data"]
    assert finding_detail["agent_context"]["context_type"] == "quality_finding"
    assert "aistock_guardrail_scan.py" in finding_detail["agent_context"]["reproduce_command"]

    finding_summary = client.get("/api/v1/validation/findings/summary").json()["data"]
    assert finding_summary["by_source_type"]["guardrail"] == 1
    assert finding_summary["by_source_type"]["legacy_inventory"] == 1

    bugs = client.get("/api/v1/validation/bugs").json()["data"]
    assert bugs["total"] == 1
    assert bugs["items"][0]["bug_id"] == "bug_demo_001"

    bug_detail = client.get("/api/v1/validation/bugs/bug_demo_001").json()["data"]
    assert bug_detail["agent_context"]["context_type"] == "bug"
    assert bug_detail["agent_context"]["allowed_write_scope"] == ["backend/services/validation"]

    agent_context = client.get("/api/v1/validation/bugs/bug_demo_001/agent-context").json()["data"]
    assert agent_context["reproduce_command"] == "python -m nox -s validation_center_backend"
    assert agent_context["closure_requirements"] == ["verification_run_id required"]
