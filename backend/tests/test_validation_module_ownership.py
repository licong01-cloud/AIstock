from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.services.validation.file_ownership import FileOwnershipCatalog, FileOwnershipError, write_scan_outputs
from backend.services.validation.module_registry import ModuleRegistry, ModuleRegistryError
from scripts.aistock_module_ownership_scan import main as ownership_scan_main


def _write_registry(path: Path) -> None:
    path.write_text(
        """
schema_version: aistock_module_registry_v1
modules:
  - module_id: validation
    display_name: Validation
    module_type: cross_cutting
    risk_level: medium
  - module_id: validation.module_quality
    display_name: Module quality
    parent_module: validation
    module_type: cross_cutting
    risk_level: high
  - module_id: docs
    display_name: Docs
    module_type: docs
    risk_level: low
  - module_id: docs.architecture
    display_name: Architecture docs
    parent_module: docs
    module_type: docs
    risk_level: low
  - module_id: tests
    display_name: Tests
    module_type: tests
    risk_level: medium
""".lstrip(),
        encoding="utf-8",
    )


def _write_ownership(path: Path) -> None:
    path.write_text(
        """
schema_version: aistock_file_ownership_v1
rules:
  - rule_id: module_quality_backend
    priority: 100
    include: [backend/services/validation/**]
    primary_module: validation.module_quality
    impact_modules: [validation]
    layer: backend_service
    risk_level: high
  - rule_id: docs_architecture
    priority: 10
    include: [docs/architecture/**]
    primary_module: docs.architecture
    layer: docs
    risk_level: low
""".lstrip(),
        encoding="utf-8",
    )


def test_default_module_registry_and_file_ownership_catalog_load() -> None:
    registry = ModuleRegistry()
    loaded = registry.load()
    assert loaded["missing"] is False
    assert registry.get_module("validation.module_quality") is not None

    catalog = FileOwnershipCatalog(module_registry=registry)
    match = catalog.match_path("scripts/aistock_module_ownership_scan.py")
    assert match.ownership_status == "mapped"
    assert match.primary_module == "validation.guardrails"
    assert "validation.module_quality" in match.impact_modules


def test_registry_rejects_duplicate_module_id(tmp_path: Path) -> None:
    registry_path = tmp_path / "module_registry.yaml"
    registry_path.write_text(
        """
schema_version: aistock_module_registry_v1
modules:
  - module_id: validation
    display_name: Validation
    module_type: cross_cutting
    risk_level: medium
  - module_id: validation
    display_name: Duplicate
    module_type: cross_cutting
    risk_level: medium
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(ModuleRegistryError, match="Duplicate module_id"):
        ModuleRegistry(registry_path).load()


def test_file_ownership_matches_paths_and_reports_unmapped(tmp_path: Path) -> None:
    registry_path = tmp_path / "module_registry.yaml"
    ownership_path = tmp_path / "file_ownership.yaml"
    _write_registry(registry_path)
    _write_ownership(ownership_path)

    catalog = FileOwnershipCatalog(ownership_path, module_registry=ModuleRegistry(registry_path))
    mapped = catalog.match_path("backend/services/validation/module_registry.py")
    assert mapped.ownership_status == "mapped"
    assert mapped.primary_module == "validation.module_quality"
    assert mapped.risk_level == "high"

    unmapped = catalog.match_path("random_root_file.py")
    assert unmapped.ownership_status == "unmapped"
    assert unmapped.reason_codes == ("no_matching_file_ownership_rule",)


def test_file_ownership_detects_ambiguous_same_priority(tmp_path: Path) -> None:
    registry_path = tmp_path / "module_registry.yaml"
    ownership_path = tmp_path / "file_ownership.yaml"
    _write_registry(registry_path)
    ownership_path.write_text(
        """
schema_version: aistock_file_ownership_v1
rules:
  - rule_id: one
    priority: 10
    include: [shared/**]
    primary_module: validation.module_quality
    layer: backend_service
    risk_level: high
  - rule_id: two
    priority: 10
    include: [shared/**]
    primary_module: docs.architecture
    layer: docs
    risk_level: low
""".lstrip(),
        encoding="utf-8",
    )

    catalog = FileOwnershipCatalog(ownership_path, module_registry=ModuleRegistry(registry_path))
    match = catalog.match_path("shared/demo.py")
    assert match.ownership_status == "ambiguous"
    assert set(match.matched_rule_ids) == {"one", "two"}


def test_scan_outputs_and_cli_fail_on_unmapped(tmp_path: Path) -> None:
    registry_path = tmp_path / "module_registry.yaml"
    ownership_path = tmp_path / "file_ownership.yaml"
    output_json = tmp_path / "scan.json"
    summary_md = tmp_path / "scan.md"
    _write_registry(registry_path)
    _write_ownership(ownership_path)

    exit_code = ownership_scan_main(
        [
            "--module-registry",
            str(registry_path),
            "--file-ownership",
            str(ownership_path),
            "--output-json",
            str(output_json),
            "--summary-md",
            str(summary_md),
            "--fail-on-unmapped",
            "backend/services/validation/file_ownership.py",
            "unknown.py",
        ]
    )
    assert exit_code == 1
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["totals"]["files"] == 2
    assert payload["totals"]["mapped_files"] == 1
    assert payload["totals"]["unmapped_files"] == 1
    assert "unknown.py" in summary_md.read_text(encoding="utf-8")


def test_write_scan_outputs_accepts_empty_problem_set(tmp_path: Path) -> None:
    output_json = tmp_path / "scan.json"
    summary_md = tmp_path / "scan.md"
    payload = {
        "schema_version": "aistock_module_ownership_scan_v1",
        "generated_at": "2026-05-05T00:00:00+00:00",
        "source": "paths",
        "totals": {"files": 1, "mapped_files": 1, "unmapped_files": 0, "ambiguous_files": 0},
        "items": [{"path": "docs/architecture/demo.md", "ownership_status": "mapped"}],
    }

    write_scan_outputs(payload, output_json=output_json, summary_md=summary_md)
    assert json.loads(output_json.read_text(encoding="utf-8"))["totals"]["mapped_files"] == 1
    assert "No unmapped or ambiguous files" in summary_md.read_text(encoding="utf-8")
