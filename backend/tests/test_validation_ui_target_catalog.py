from __future__ import annotations

import json
import re
import sys
import textwrap
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.validation.history_store import RUN_SCHEMA, ValidationHistoryStore
from backend.services.validation.module_registry import ModuleRegistry
from backend.services.validation.plan_catalog import ValidationPlanCatalog
from backend.services.validation.ui_target_catalog import (
    ValidationUiTargetCatalog,
    ValidationUiTargetCatalogError,
)


_ROUTER_PATH = Path(__file__).resolve().parents[1] / "routers" / "validation.py"
_ROUTER_SPEC = spec_from_file_location("backend.routers.validation", _ROUTER_PATH)
assert _ROUTER_SPEC is not None and _ROUTER_SPEC.loader is not None
validation = module_from_spec(_ROUTER_SPEC)
sys.modules[_ROUTER_SPEC.name] = validation
_ROUTER_SPEC.loader.exec_module(validation)


def _write_yaml(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip(), encoding="utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_registry(path: Path) -> None:
    _write_yaml(
        path,
        """
        schema_version: aistock_module_registry_v1
        modules:
          - module_id: validation
            display_name: Validation
            module_type: cross_cutting
            risk_level: medium
          - module_id: validation.center
            display_name: Validation Center
            parent_module: validation
            module_type: product_feature
            risk_level: medium
            ui_routes: [/validation-center]
          - module_id: validation.runner
            display_name: Validation Runner
            parent_module: validation
            module_type: technical_layer
            risk_level: high
          - module_id: qe
            display_name: QE
            module_type: product_feature
            risk_level: high
          - module_id: qe.archive
            display_name: QE Archive
            parent_module: qe
            module_type: data_pipeline
            risk_level: high
        """,
    )


def _write_plans(path: Path) -> None:
    _write_yaml(
        path,
        """
        schema_version: aistock_validation_plans_v1
        plans:
          - plan_key: l0
            title: L0
            module: validation_center
            level: L0
            command_key: nox_l0
            nox_session: l0
            enabled: true
            requires_backend: false
            requires_frontend: false
            allowed_backend_ports: []
            allowed_frontend_ports: []
            writes_database: false
            writes_artifacts: true
            writes_business_state: false
          - plan_key: validation_center_backend
            title: Validation backend
            module: validation_center
            level: L2
            command_key: nox_validation_center_backend
            nox_session: validation_center_backend
            enabled: true
            requires_backend: false
            requires_frontend: false
            allowed_backend_ports: []
            allowed_frontend_ports: []
            writes_database: false
            writes_artifacts: true
            writes_business_state: false
          - plan_key: qe_archive_l3
            title: QE archive L3
            module: qe_archive
            level: L3
            command_key: nox_qe_archive_l3
            nox_session: qe_archive_l3
            enabled: true
            requires_backend: true
            requires_frontend: true
            allowed_backend_ports: [8011]
            allowed_frontend_ports: [3011]
            writes_database: false
            writes_artifacts: true
            writes_business_state: false
        """,
    )


def _write_targets(path: Path) -> None:
    _write_yaml(
        path,
        """
        schema_version: aistock_validation_ui_targets_v1
        targets:
          - route_id: validation.center
            href: /validation-center
            label: Validation Center
            nav_group: Validation Pipeline
            primary_module: validation.center
            impact_modules: [validation.runner]
            risk_level: medium
            required_test_plans: [l0, validation_center_backend]
            recommended_test_plans: []
            business_operations: [Open Validation Center, Review quality state]
            coverage_status: partial
          - route_id: qe.archive
            href: /qe-archive
            label: QE Archive
            nav_group: QuantEvolver
            primary_module: qe.archive
            impact_modules: [qe]
            risk_level: high
            required_test_plans: [l0, qe_archive_l3]
            recommended_test_plans: []
            business_operations: [Open QE archive, Review archived experiments]
            coverage_status: planned
        """,
    )


def _write_history(history_root: Path) -> None:
    run_md = history_root / "validation_center" / "20260507_l2_validation-center-api.md"
    run_md.parent.mkdir(parents=True, exist_ok=True)
    run_md.write_text("# Validation Center API\n", encoding="utf-8")
    _write_json(
        run_md.with_suffix(".json"),
        {
            "schema_version": RUN_SCHEMA,
            "module": "validation_center",
            "module_slug": "validation_center",
            "level": "L2",
            "title": "Validation Center API",
            "status": "passed",
            "started_at": "2026-05-07T10:00:00",
        },
    )


class _FakeModuleQualityService:
    def module_quality_summary(self, *, commit_limit: int = 50) -> dict[str, Any]:
        return {
            "modules": [
                {
                    "module_id": "validation.center",
                    "coverage": {"status": "passed", "line_percent": 82.0, "branch_percent": 61.0},
                    "priority": {"score": 18, "level": "medium", "reason_codes": ["recent_commits"]},
                },
                {
                    "module_id": "qe.archive",
                    "coverage": {"status": "missing", "line_percent": None, "branch_percent": None},
                    "priority": {"score": 45, "level": "critical", "reason_codes": ["changed_without_passing_coverage"]},
                },
            ]
        }


def _catalog(tmp_path: Path) -> ValidationUiTargetCatalog:
    registry_path = tmp_path / "module_registry.yaml"
    plans_path = tmp_path / "test_plans.yaml"
    targets_path = tmp_path / "ui_targets.yaml"
    history_root = tmp_path / "history"
    _write_registry(registry_path)
    _write_plans(plans_path)
    _write_targets(targets_path)
    _write_history(history_root)
    return ValidationUiTargetCatalog(
        targets_path,
        module_registry=ModuleRegistry(registry_path),
        plan_catalog=ValidationPlanCatalog(plans_path),
        history_store=ValidationHistoryStore(history_root=history_root, repo_root=tmp_path),
        module_quality_service=_FakeModuleQualityService(),
    )


def test_default_ui_target_catalog_matches_frontend_nav_groups() -> None:
    loaded = ValidationUiTargetCatalog().load()
    target_hrefs = {item["href"] for item in loaded["targets"]}
    nav_groups_ts = Path("frontend/src/lib/navigation/nav-groups.ts").read_text(encoding="utf-8")
    nav_hrefs = set(re.findall(r'href: "([^"]+)"', nav_groups_ts))

    assert len(target_hrefs) >= 40
    assert nav_hrefs
    assert nav_hrefs - target_hrefs == set()
    assert target_hrefs - nav_hrefs == set()


def test_ui_target_catalog_loads_and_enriches_quality(tmp_path: Path) -> None:
    catalog = _catalog(tmp_path)

    loaded = catalog.load()
    assert loaded["missing"] is False
    assert [item["route_id"] for item in loaded["targets"]] == ["validation.center", "qe.archive"]

    page = catalog.list_targets(page_size=20)
    validation_target = next(item for item in page["items"] if item["route_id"] == "validation.center")
    assert validation_target["module_quality"]["coverage"]["line_percent"] == 82.0
    assert validation_target["latest_run"]["status"] == "passed"
    assert "route_coverage_not_fully_proven" in validation_target["warnings"]

    qe_target = next(item for item in page["items"] if item["route_id"] == "qe.archive")
    assert "module_coverage_missing_or_failed" in qe_target["warnings"]
    assert "module_priority_requires_validation" in qe_target["warnings"]


def test_ui_target_catalog_rejects_duplicates_and_unknown_references(tmp_path: Path) -> None:
    registry_path = tmp_path / "module_registry.yaml"
    plans_path = tmp_path / "test_plans.yaml"
    targets_path = tmp_path / "ui_targets.yaml"
    _write_registry(registry_path)
    _write_plans(plans_path)
    _write_targets(targets_path)

    text = targets_path.read_text(encoding="utf-8")
    targets_path.write_text(text + text.split("targets:", 1)[1], encoding="utf-8")
    with pytest.raises(ValidationUiTargetCatalogError, match="Duplicate route_id"):
        ValidationUiTargetCatalog(
            targets_path,
            module_registry=ModuleRegistry(registry_path),
            plan_catalog=ValidationPlanCatalog(plans_path),
        ).load()

    _write_targets(targets_path)
    targets_path.write_text(
        targets_path.read_text(encoding="utf-8").replace("primary_module: qe.archive", "primary_module: missing.module"),
        encoding="utf-8",
    )
    with pytest.raises(ValidationUiTargetCatalogError, match="unknown primary_module"):
        ValidationUiTargetCatalog(
            targets_path,
            module_registry=ModuleRegistry(registry_path),
            plan_catalog=ValidationPlanCatalog(plans_path),
        ).load()


def test_ui_target_catalog_rejects_missing_business_operations(tmp_path: Path) -> None:
    registry_path = tmp_path / "module_registry.yaml"
    plans_path = tmp_path / "test_plans.yaml"
    targets_path = tmp_path / "ui_targets.yaml"
    _write_registry(registry_path)
    _write_plans(plans_path)
    _write_targets(targets_path)
    targets_path.write_text(
        targets_path.read_text(encoding="utf-8").replace("business_operations: [Open QE archive, Review archived experiments]", "business_operations: []"),
        encoding="utf-8",
    )

    with pytest.raises(ValidationUiTargetCatalogError, match="business_operations"):
        ValidationUiTargetCatalog(
            targets_path,
            module_registry=ModuleRegistry(registry_path),
            plan_catalog=ValidationPlanCatalog(plans_path),
        ).load()


def test_ui_target_api_list_summary_and_detail(tmp_path: Path) -> None:
    app = FastAPI()
    app.include_router(validation.router, prefix="/api/v1")
    catalog = _catalog(tmp_path)
    app.dependency_overrides[validation.get_ui_target_catalog] = lambda: catalog
    client = TestClient(app)

    response = client.get("/api/v1/validation/ui-targets", params={"page_size": 20})
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["total"] == 2
    assert data["items"][0]["route_id"] == "validation.center"

    filtered = client.get("/api/v1/validation/ui-targets", params={"module": "qe.archive"}).json()["data"]
    assert filtered["total"] == 1
    assert filtered["items"][0]["href"] == "/qe-archive"

    summary = client.get("/api/v1/validation/ui-targets/summary").json()["data"]
    assert summary["target_count"] == 2
    assert summary["by_coverage_status"]["partial"] == 1
    assert summary["by_coverage_status"]["planned"] == 1
    assert summary["production_8001_touched"] is False

    detail = client.get("/api/v1/validation/ui-targets/validation.center").json()["data"]
    assert detail["target"]["href"] == "/validation-center"
    assert detail["target"]["latest_run"]["status"] == "passed"

    assert client.get("/api/v1/validation/ui-targets/not_found").status_code == 404
