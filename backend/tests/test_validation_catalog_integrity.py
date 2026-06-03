from __future__ import annotations

import json
import textwrap
from pathlib import Path

import yaml

from backend.services.validation.catalog_integrity import (
    CATALOG_INTEGRITY_SCHEMA,
    run_catalog_integrity,
)
from scripts.aistock_validation_catalog_integrity import main as catalog_integrity_main


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip(), encoding="utf-8")


def _write_pass_repo(repo_root: Path) -> None:
    _write(
        repo_root / "tests" / "aistock_validation" / "catalog" / "test_plans.yaml",
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
            runner_enabled: true
            max_duration_seconds: 300
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
            runner_enabled: true
            max_duration_seconds: 300
            resource_policy:
              resource_mode: readonly
              business_state_write: none
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
            runner_enabled: false
            max_duration_seconds: 900
          - plan_key: qe_mcp_backend
            title: QE MCP backend
            module: qe
            level: L2
            command_key: nox_qe_mcp_backend
            nox_session: qe_mcp_backend
            enabled: true
            requires_backend: false
            requires_frontend: false
            allowed_backend_ports: []
            allowed_frontend_ports: []
            writes_database: false
            writes_artifacts: false
            writes_business_state: false
            runner_enabled: true
            max_duration_seconds: 300
        """,
    )
    _write(
        repo_root / "tests" / "aistock_validation" / "catalog" / "module_registry.yaml",
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
            test_plans:
              required_on_change: [l0, validation_center_backend]
          - module_id: validation.runner
            display_name: Validation Runner
            parent_module: validation
            module_type: technical_layer
            risk_level: high
          - module_id: qe
            display_name: QE
            module_type: product_feature
            risk_level: high
            test_plans:
              recommended: [qe_mcp_backend]
          - module_id: qe.archive
            display_name: QE Archive
            parent_module: qe
            module_type: data_pipeline
            risk_level: high
            test_plans:
              required_on_change: [qe_archive_l3]
        """,
    )
    _write(
        repo_root / "tests" / "aistock_validation" / "catalog" / "ui_targets.yaml",
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
    _write(
        repo_root / "tests" / "aistock_validation" / "catalog" / "file_ownership.yaml",
        """
        schema_version: aistock_file_ownership_v1
        rules:
          - rule_id: validation_catalog
            priority: 100
            include: [backend/services/validation/**]
            primary_module: validation.center
            impact_modules: [validation.runner]
            layer: backend_service
            risk_level: medium
        """,
    )
    _write(
        repo_root / "tests" / "aistock_validation" / "catalog" / "resource_policies.yaml",
        """
        schema_version: aistock_validation_resource_policies_v1
        status: design_draft
        policies:
          readonly:
            description: Read-only validation against fixtures or dev endpoints.
            resource_mode: readonly
            business_state_write: none
            cleanup_required: false
        """,
    )
    _write(
        repo_root / "noxfile.py",
        """
        import nox

        @nox.session(venv_backend="none")
        def l0(session):
            pass

        @nox.session(venv_backend="none")
        def validation_center_backend(session):
            pass

        @nox.session(venv_backend="none")
        def qe_mcp_backend(session):
            pass

        @nox.session(venv_backend="none")
        def qe_archive_l3(session):
            pass
        """,
    )
    _write(
        repo_root / "frontend" / "src" / "lib" / "navigation" / "nav-groups.ts",
        """
        export const NAV_GROUPS = [
          {
            title: "Automation",
            items: [
              { href: "/validation-center", label: "Validation Center" },
              { href: "/qe-archive", label: "QE Archive" },
            ],
          },
        ];
        """,
    )
    _write(
        repo_root / ".github" / "workflows" / "test.yml",
        """
        name: CI
        jobs:
          static-gate:
            runs-on: windows-latest
            steps:
              - run: python -m nox -s l0
              - run: python -m nox -s validation_center_backend
        """,
    )
    _write(
        repo_root / ".github" / "workflows" / "nightly.yml",
        """
        name: Nightly
        jobs:
          nightly-l3:
            runs-on: windows-latest
            strategy:
              matrix:
                session: [qe_archive_l3]
            steps:
              - run: python -m nox -s ${{ matrix.session }}
        """,
    )


def test_catalog_integrity_passes_on_aligned_catalogs(tmp_path: Path) -> None:
    _write_pass_repo(tmp_path)

    report = run_catalog_integrity(repo_root=tmp_path)

    assert report["schema_version"] == CATALOG_INTEGRITY_SCHEMA
    assert report["state"] == "passed"
    assert report["summary"]["error_count"] == 0
    assert report["summary"]["warning_count"] == 0
    assert report["summary"]["plans"] == 4
    assert report["summary"]["runner_enabled_plans"] == 3
    assert report["findings"] == []


def test_nightly_codegraph_freshness_is_not_skipped_by_weekly_ua_guard() -> None:
    workflow_path = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "nightly.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8")) or {}
    steps = workflow["jobs"]["code-intelligence-weekly"]["steps"]
    run_blocks = [str(step.get("run") or "") for step in steps if isinstance(step, dict)]

    freshness_runs = [block for block in run_blocks if "code_intelligence_adapter.py freshness" in block]
    ua_runs = [block for block in run_blocks if "code_intelligence_adapter.py ua-summary-all" in block]

    assert freshness_runs
    assert "date -u +%u" not in freshness_runs[0]
    assert "exit 0" not in freshness_runs[0]
    assert "|| true" in freshness_runs[0]
    assert ua_runs
    assert "date -u +%u" in ua_runs[0]
    assert "exit 0" in ua_runs[0]


def test_catalog_integrity_reports_command_session_module_and_resource_issues(tmp_path: Path) -> None:
    _write_pass_repo(tmp_path)
    _write(
        tmp_path / "tests" / "aistock_validation" / "catalog" / "test_plans.yaml",
        """
        schema_version: aistock_validation_plans_v1
        plans:
          - plan_key: broken_plan
            title: Broken plan
            module: missing_module
            level: L4
            command_key: shell_anything
            enabled: true
            requires_backend: false
            requires_frontend: false
            writes_database: true
            writes_artifacts: true
            writes_business_state: true
            runner_enabled: true
            allowed_backend_ports: [8001]
            allowed_frontend_ports: [3000]
            max_duration_seconds: 1800
          - plan_key: resource_ref_plan
            title: Resource reference plan
            module: validation_center
            level: L2
            command_key: nox_validation_center_backend
            nox_session: validation_center_backend
            enabled: true
            requires_backend: false
            requires_frontend: false
            writes_database: false
            writes_artifacts: true
            writes_business_state: false
            runner_enabled: false
            resource_policy: unknown_policy
        """,
    )

    report = run_catalog_integrity(repo_root=tmp_path)
    finding_ids = [item["finding_id"] for item in report["findings"]]

    assert report["state"] == "failed"
    assert "CATALOG-003" in finding_ids
    assert "CATALOG-004" in finding_ids
    assert "CATALOG-005" in finding_ids
    assert "CATALOG-012" in finding_ids
    assert "RESOURCE-001" in finding_ids
    assert "RESOURCE-005" in finding_ids
    assert "RESOURCE-REF-001" in finding_ids
    assert "CATALOG-006" in finding_ids


def test_catalog_integrity_cli_writes_json_report(tmp_path: Path, capsys) -> None:
    _write_pass_repo(tmp_path)
    output_json = tmp_path / "tmp" / "validation" / "catalog" / "report.json"

    exit_code = catalog_integrity_main(
        [
            "--repo-root",
            str(tmp_path),
            "--output-json",
            str(output_json),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["schema_version"] == CATALOG_INTEGRITY_SCHEMA
    assert output_json.exists()
    assert json.loads(output_json.read_text(encoding="utf-8"))["state"] == "passed"
