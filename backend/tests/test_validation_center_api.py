from __future__ import annotations

import json
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

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


@pytest.fixture()
def client(tmp_path) -> TestClient:
    history_root = tmp_path / "history"
    catalog_path = tmp_path / "test_plans.yaml"
    _write_history(history_root)
    _write_catalog(catalog_path)

    app = FastAPI()
    app.include_router(validation.router, prefix="/api/v1")
    app.dependency_overrides[validation.get_history_store] = lambda: ValidationHistoryStore(
        history_root=history_root,
        repo_root=tmp_path,
    )
    app.dependency_overrides[validation.get_plan_catalog] = lambda: ValidationPlanCatalog(catalog_path)
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
    assert summary["modules"][0]["module"] == "validation_center"
