from __future__ import annotations

import json
import os
import subprocess
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.validation.file_ownership import FileOwnershipCatalog
from backend.services.validation.finding_store import BUG_SCHEMA, GUARDRAIL_SCHEMA, ValidationFindingStore
from backend.services.validation.git_activity_provider import GitCommitActivityProvider
from backend.services.validation.git_status_provider import GitWorkspaceStatusProvider
from backend.services.validation.history_store import COVERAGE_SCHEMA, ValidationHistoryStore
from backend.services.validation.module_quality import ModuleQualityService
from backend.services.validation.module_registry import ModuleRegistry


_ROUTER_PATH = Path(__file__).resolve().parents[1] / "routers" / "validation.py"
_ROUTER_SPEC = spec_from_file_location("backend.routers.validation", _ROUTER_PATH)
assert _ROUTER_SPEC is not None and _ROUTER_SPEC.loader is not None
validation = module_from_spec(_ROUTER_SPEC)
sys.modules[_ROUTER_SPEC.name] = validation
_ROUTER_SPEC.loader.exec_module(validation)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _git(repo_root: Path, *args: str, at: str | None = None) -> None:
    env = os.environ.copy()
    if at:
        env["GIT_AUTHOR_DATE"] = at
        env["GIT_COMMITTER_DATE"] = at
    subprocess.run(
        ["git", *args],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        check=True,
    )


def _write_registry(path: Path) -> None:
    path.write_text(
        """
schema_version: aistock_module_registry_v1
modules:
  - module_id: docs
    display_name: Docs
    module_type: docs
    risk_level: low
  - module_id: docs.architecture
    display_name: Architecture docs
    parent_module: docs
    module_type: docs
    risk_level: low
    test_plans:
      required_on_change: [l0]
      recommended: [validation_center_ui]
  - module_id: validation
    display_name: Validation
    module_type: cross_cutting
    risk_level: medium
  - module_id: validation.center
    display_name: Validation Center
    parent_module: validation
    module_type: cross_cutting
    risk_level: high
    test_plans:
      required_on_change: [l0, validation_center_backend]
      recommended: [validation_center_ui]
""".lstrip(),
        encoding="utf-8",
    )


def _write_ownership(path: Path) -> None:
    path.write_text(
        """
schema_version: aistock_file_ownership_v1
rules:
  - rule_id: docs_architecture
    priority: 10
    include: [docs/architecture/**]
    primary_module: docs.architecture
    layer: docs
    risk_level: low
  - rule_id: validation_router
    priority: 20
    include: [backend/routers/validation.py]
    primary_module: validation.center
    impact_modules: [validation]
    layer: backend_api
    risk_level: high
""".lstrip(),
        encoding="utf-8",
    )


def _seed_repo(tmp_path: Path) -> tuple[ModuleRegistry, FileOwnershipCatalog, GitWorkspaceStatusProvider, GitCommitActivityProvider]:
    registry_path = tmp_path / "module_registry.yaml"
    ownership_path = tmp_path / "file_ownership.yaml"
    _write_registry(registry_path)
    _write_ownership(ownership_path)
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "pytest@example.invalid")
    _git(tmp_path, "config", "user.name", "pytest")

    docs_dir = tmp_path / "docs" / "architecture"
    router_dir = tmp_path / "backend" / "routers"
    docs_dir.mkdir(parents=True)
    router_dir.mkdir(parents=True)
    (docs_dir / "design.md").write_text("seed docs\n", encoding="utf-8")
    (router_dir / "validation.py").write_text("VALUE = 1\n", encoding="utf-8")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-m", "seed validation repo", at="2026-05-01T10:00:00+08:00")

    (docs_dir / "design.md").write_text("docs change\n", encoding="utf-8")
    _git(tmp_path, "add", "docs/architecture/design.md")
    _git(tmp_path, "commit", "-m", "docs module change", at="2026-05-02T11:00:00+08:00")

    (router_dir / "validation.py").write_text("VALUE = 2\n", encoding="utf-8")
    (tmp_path / "root_tmp.py").write_text("print('unmapped')\n", encoding="utf-8")
    _git(tmp_path, "add", "backend/routers/validation.py", "root_tmp.py")
    _git(tmp_path, "commit", "-m", "touch validation and unmapped", at="2026-05-03T12:00:00+08:00")

    (router_dir / "validation.py").write_text("VALUE = 3\n", encoding="utf-8")

    registry = ModuleRegistry(registry_path)
    ownership = FileOwnershipCatalog(ownership_path, module_registry=registry)
    status_provider = GitWorkspaceStatusProvider(repo_root=tmp_path, file_ownership_catalog=ownership)
    activity_provider = GitCommitActivityProvider(
        repo_root=tmp_path,
        module_registry=registry,
        file_ownership_catalog=ownership,
        git_status_provider=status_provider,
    )
    return registry, ownership, status_provider, activity_provider


def _write_validation_evidence(tmp_path: Path) -> tuple[ValidationHistoryStore, ValidationFindingStore]:
    history_root = tmp_path / "history"
    quality_root = tmp_path / "guardrails"
    bug_root = tmp_path / "bugs"
    _write_json(
        history_root / "docs_architecture" / "20260503_l2_docs-coverage-snapshot.json",
        {
            "schema_version": COVERAGE_SCHEMA,
            "generated_at": "2026-05-03T13:00:00+08:00",
            "module": "docs.architecture",
            "level": "L2",
            "title": "Docs architecture coverage",
            "status": "passed",
            "totals": {"line_percent": 88.0, "branch_percent": 70.0},
            "diff": {},
            "quality_gates": [],
            "failed_gates": [],
        },
    )
    _write_json(
        quality_root / "validation_guardrail.json",
        {
            "schema_version": GUARDRAIL_SCHEMA,
            "generated_at": "2026-05-03T14:00:00+08:00",
            "findings": [
                {
                    "rule_id": "NO-SILENT-FALLBACK",
                    "title": "No silent fallback",
                    "severity": "P1",
                    "file": "backend/routers/validation.py",
                    "line": 10,
                    "message": "Route hides an error.",
                    "fingerprint": "validation_router_guardrail",
                }
            ],
        },
    )
    _write_json(
        bug_root / "validation_bug.json",
        {
            "schema_version": BUG_SCHEMA,
            "bug_id": "bug_validation_001",
            "title": "Validation UI stale test gap",
            "module": "validation_center",
            "severity": "P2",
            "status": "detected",
            "created_at": "2026-05-03T15:00:00+08:00",
        },
    )
    return (
        ValidationHistoryStore(history_root=history_root, repo_root=tmp_path),
        ValidationFindingStore(
            repo_root=tmp_path,
            guardrail_root=quality_root,
            legacy_root=tmp_path / "legacy_inventory",
            bug_root=bug_root,
        ),
    )


def test_commit_activity_maps_recent_commits_to_modules(tmp_path: Path) -> None:
    _, _, _, activity_provider = _seed_repo(tmp_path)

    payload = activity_provider.commit_activity(limit=3)

    assert payload["schema_version"] == "aistock_git_commit_activity_v1"
    assert payload["summary"]["commit_count"] == 3
    assert payload["summary"]["latest_commit"]["subject"] == "touch validation and unmapped"
    assert payload["by_day"][0]["period"] == "2026-05-03"
    latest = payload["commits"][0]
    assert latest["ownership_summary"]["unmapped"] == 1
    assert "validation.center" in latest["module_ids"]
    by_module = {item["module_id"]: item for item in payload["by_module"]}
    assert by_module["validation.center"]["commit_count"] == 2
    assert by_module["docs.architecture"]["commit_count"] == 2


def test_module_quality_summary_combines_workspace_commits_coverage_and_findings(tmp_path: Path) -> None:
    registry, ownership, status_provider, activity_provider = _seed_repo(tmp_path)
    history_store, finding_store = _write_validation_evidence(tmp_path)
    service = ModuleQualityService(
        repo_root=tmp_path,
        module_registry=registry,
        file_ownership_catalog=ownership,
        git_status_provider=status_provider,
        git_activity_provider=activity_provider,
        history_store=history_store,
        finding_store=finding_store,
    )

    payload = service.module_quality_summary(commit_limit=3)

    assert payload["schema_version"] == "aistock_validation_module_quality_v1"
    assert payload["summary"]["modules_with_workspace_changes"] == 1
    modules = {item["module_id"]: item for item in payload["modules"]}
    validation_module = modules["validation.center"]
    assert validation_module["workspace"]["changed_file_count"] == 1
    assert validation_module["commits"]["commit_count"] == 2
    assert validation_module["quality"]["finding_count"] == 1
    assert validation_module["quality"]["bug_count"] == 1
    assert validation_module["priority"]["level"] in {"high", "critical"}
    docs_module = modules["docs.architecture"]
    assert docs_module["coverage"]["line_percent"] == 88.0
    assert docs_module["commits"]["commit_count"] == 2


def test_validation_git_activity_and_module_quality_api_use_injected_services(tmp_path: Path) -> None:
    registry, ownership, status_provider, activity_provider = _seed_repo(tmp_path)
    history_store, finding_store = _write_validation_evidence(tmp_path)
    quality_service = ModuleQualityService(
        repo_root=tmp_path,
        module_registry=registry,
        file_ownership_catalog=ownership,
        git_status_provider=status_provider,
        git_activity_provider=activity_provider,
        history_store=history_store,
        finding_store=finding_store,
    )
    app = FastAPI()
    app.include_router(validation.router, prefix="/api/v1")
    app.dependency_overrides[validation.get_git_activity_provider] = lambda: activity_provider
    app.dependency_overrides[validation.get_module_quality_service] = lambda: quality_service
    client = TestClient(app)

    activity = client.get("/api/v1/validation/git/commit-activity", params={"limit": 3}).json()["data"]
    quality = client.get("/api/v1/validation/modules/quality-summary", params={"commit_limit": 3}).json()["data"]

    assert activity["commits"][0]["subject"] == "touch validation and unmapped"
    assert quality["summary"]["modules_needing_validation"] >= 1
    assert quality["production_8001_touched"] is False
