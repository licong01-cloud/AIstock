from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
import yaml

from backend.services.validation.file_ownership import FileOwnershipCatalog, write_scan_outputs
from backend.services.validation.module_registry import ModuleRegistry, ModuleRegistryError
from scripts.aistock_module_ownership_scan import main as ownership_scan_main


def _write_registry(path: Path) -> None:
    modules = []
    for module_id, name, kind, risk, parent in [
        ("validation", "Validation", "cross_cutting", "medium", None),
        ("validation.module_quality", "Module quality", "cross_cutting", "high", "validation"),
        ("docs", "Docs", "docs", "low", None),
        ("docs.architecture", "Architecture docs", "docs", "low", "docs"),
        ("tests", "Tests", "tests", "medium", None),
    ]:
        entry = dict(module_id=module_id, display_name=name, module_type=kind, risk_level=risk)
        if parent:
            entry["parent_module"] = parent
        modules.append(entry)
    path.write_text(yaml.safe_dump({"schema_version": "aistock_module_registry_v1", "modules": modules}), encoding="utf-8")


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


def _git(repo_root: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )


def test_default_module_registry_and_file_ownership_catalog_load() -> None:
    registry = ModuleRegistry()
    loaded = registry.load()
    assert loaded["missing"] is False
    assert registry.get_module("validation.module_quality") is not None
    descriptions_zh = [item.get("description_zh") or "" for item in loaded["modules"]]
    assert all(descriptions_zh)
    assert not any("?" in text or "\ufffd" in text for text in descriptions_zh)
    assert registry.get_module("qe") is not None
    assert registry.get_module("qe").description_zh.startswith("覆盖 QuantEvolver")

    catalog = FileOwnershipCatalog(module_registry=registry)
    match = catalog.match_path("scripts/aistock_module_ownership_scan.py")
    assert match.ownership_status == "mapped"
    assert match.primary_module == "validation.guardrails"
    assert "validation.module_quality" in match.impact_modules

    for path, expected_module in [
        ("frontend/src/app/hmm-evolution/page.tsx", "hmm.evolution"),
        ("frontend/src/app/page.tsx", "frontend_common"),
        ("backend/routers/health.py", "platform.api"),
        ("backend/tests/platform_api/test_health_contract.py", "platform.api"),
        ("backend/services/core_index_membership.py", "qlib_data"),
        ("backend/tests/scripts/test_prepare_core_index_membership_pit.py", "qlib_data"),
        ("backend/tests/quantevolver/test_stock_pool_sync.py", "qe.core"),
        ("CLAUDE.md", "docs.standards"),
        ("docs/process/research_assistant_blueprint_execution_runbook_20260531.md", "docs.standards"),
        ("docs/process/cross_tool_review_protocol_20260510.md", "docs"),
        ("docs/discussion/cross_tool_channel_protocol_20260510.md", "docs"),
        ("backend/services/validation/plan_catalog.py", "validation.runner"),
    ]:
        matched = catalog.match_path(path)
        assert matched.ownership_status == "mapped", path
        assert matched.primary_module == expected_module, path
    platform_module = registry.get_module("platform.api")
    assert platform_module is not None
    assert "platform_api_backend" in platform_module.test_plans_required

    assert registry.get_module("qmt").test_plans_required == ("l0", "qmt_client_contract")
    assert registry.get_module("qlib_data").test_plans_required == ("l0", "qlib_data_backend")
    assert registry.get_module("watchlist").test_plans_required == ("l0", "watchlist_backend")
    assert registry.get_module("validation.runner").test_plans_required == ("l0", "validation_catalog_integrity")

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


def test_changed_only_detects_untracked_unmapped_file(tmp_path: Path) -> None:
    registry_path = tmp_path / "module_registry.yaml"
    ownership_path = tmp_path / "file_ownership.yaml"
    _write_registry(registry_path)
    _write_ownership(ownership_path)
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "pytest@example.invalid")
    _git(tmp_path, "config", "user.name", "pytest")
    tracked_doc = tmp_path / "docs" / "architecture" / "tracked.md"
    tracked_doc.parent.mkdir(parents=True)
    tracked_doc.write_text("tracked\n", encoding="utf-8")
    _git(tmp_path, "add", "docs/architecture/tracked.md", "module_registry.yaml", "file_ownership.yaml")
    _git(tmp_path, "commit", "-m", "seed")
    tracked_doc.write_text("changed\n", encoding="utf-8")
    (tmp_path / "root_debug_probe.py").write_text("print('bad')\n", encoding="utf-8")

    catalog = FileOwnershipCatalog(ownership_path, module_registry=ModuleRegistry(registry_path))
    payload = catalog.scan_changed_files(repo_root=tmp_path)

    assert payload["source"] == "changed_files"
    assert payload["totals"]["files"] == 2
    assert payload["totals"]["mapped_files"] == 1
    assert payload["totals"]["unmapped_files"] == 1
    unmapped = next(item for item in payload["items"] if item["ownership_status"] == "unmapped")
    assert unmapped["path"] == "root_debug_probe.py"


def test_staged_only_and_cli_block_unmapped_staged_file(tmp_path: Path) -> None:
    registry_path = tmp_path / "module_registry.yaml"
    ownership_path = tmp_path / "file_ownership.yaml"
    output_json = tmp_path / "staged.json"
    _write_registry(registry_path)
    _write_ownership(ownership_path)
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "pytest@example.invalid")
    _git(tmp_path, "config", "user.name", "pytest")
    mapped = tmp_path / "backend" / "services" / "validation" / "mapped.py"
    unmapped = tmp_path / "root_tmp.py"
    mapped.parent.mkdir(parents=True)
    mapped.write_text("VALUE = 1\n", encoding="utf-8")
    unmapped.write_text("VALUE = 2\n", encoding="utf-8")
    _git(tmp_path, "add", "backend/services/validation/mapped.py", "root_tmp.py")

    catalog = FileOwnershipCatalog(ownership_path, module_registry=ModuleRegistry(registry_path))
    payload = catalog.scan_staged_files(repo_root=tmp_path)
    assert payload["source"] == "staged_files"
    assert payload["totals"]["mapped_files"] == 1
    assert payload["totals"]["unmapped_files"] == 1

    exit_code = ownership_scan_main(
        [
            "--module-registry",
            str(registry_path),
            "--file-ownership",
            str(ownership_path),
            "--repo-root",
            str(tmp_path),
            "--staged-only",
            "--fail-on-unmapped",
            "--output-json",
            str(output_json),
        ]
    )
    assert exit_code == 1
    assert json.loads(output_json.read_text(encoding="utf-8"))["totals"]["unmapped_files"] == 1


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
