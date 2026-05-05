from __future__ import annotations

import subprocess
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.validation.file_ownership import FileOwnershipCatalog
from backend.services.validation.git_status_provider import GitWorkspaceStatusProvider
from backend.services.validation.module_registry import ModuleRegistry


_ROUTER_PATH = Path(__file__).resolve().parents[1] / "routers" / "validation.py"
_ROUTER_SPEC = spec_from_file_location("backend.routers.validation", _ROUTER_PATH)
assert _ROUTER_SPEC is not None and _ROUTER_SPEC.loader is not None
validation = module_from_spec(_ROUTER_SPEC)
sys.modules[_ROUTER_SPEC.name] = validation
_ROUTER_SPEC.loader.exec_module(validation)


def _git(repo_root: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
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
""".lstrip(),
        encoding="utf-8",
    )


def _seed_repo(tmp_path: Path) -> GitWorkspaceStatusProvider:
    registry_path = tmp_path / "module_registry.yaml"
    ownership_path = tmp_path / "file_ownership.yaml"
    _write_registry(registry_path)
    _write_ownership(ownership_path)
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "pytest@example.invalid")
    _git(tmp_path, "config", "user.name", "pytest")
    docs_dir = tmp_path / "docs" / "architecture"
    docs_dir.mkdir(parents=True)
    for name in ["modified.md", "staged.md", "delete.md", "staged_delete.md", "rename_old.md"]:
        (docs_dir / name).write_text(f"{name}\n", encoding="utf-8")
    _git(
        tmp_path,
        "add",
        "docs/architecture",
        "module_registry.yaml",
        "file_ownership.yaml",
    )
    _git(tmp_path, "commit", "-m", "seed")
    (docs_dir / "modified.md").write_text("changed unstaged\n", encoding="utf-8")
    (docs_dir / "staged.md").write_text("changed staged\n", encoding="utf-8")
    _git(tmp_path, "add", "docs/architecture/staged.md")
    (docs_dir / "delete.md").unlink()
    _git(tmp_path, "rm", "docs/architecture/staged_delete.md")
    _git(tmp_path, "mv", "docs/architecture/rename_old.md", "docs/architecture/rename_new.md")
    (docs_dir / "new_staged.md").write_text("new staged\n", encoding="utf-8")
    _git(tmp_path, "add", "docs/architecture/new_staged.md")
    (tmp_path / "root_tmp.py").write_text("print('unmapped')\n", encoding="utf-8")
    catalog = FileOwnershipCatalog(ownership_path, module_registry=ModuleRegistry(registry_path))
    return GitWorkspaceStatusProvider(repo_root=tmp_path, file_ownership_catalog=catalog)


def test_workspace_status_maps_git_dirty_files_to_modules(tmp_path: Path) -> None:
    provider = _seed_repo(tmp_path)

    payload = provider.workspace_status()

    assert payload["schema_version"] == "aistock_git_workspace_status_v1"
    assert payload["dirty"] is True
    assert payload["summary"]["changed_files"] == 7
    assert payload["summary"]["staged_files"] == 4
    assert payload["summary"]["unstaged_files"] == 2
    assert payload["summary"]["untracked_files"] == 1
    assert payload["summary"]["deleted_files"] == 2
    assert payload["summary"]["renamed_files"] == 1
    assert payload["summary"]["unmapped_files"] == 1
    by_path = {item["path"]: item for item in payload["files"]}
    assert by_path["docs/architecture/modified.md"]["status"] == "unstaged_modified"
    assert by_path["docs/architecture/staged.md"]["status"] == "staged_modified"
    assert by_path["docs/architecture/delete.md"]["status"] == "unstaged_deleted"
    assert by_path["docs/architecture/staged_delete.md"]["status"] == "staged_deleted"
    assert by_path["docs/architecture/new_staged.md"]["status"] == "staged_added"
    rename = by_path["docs/architecture/rename_new.md"]
    assert rename["status"] == "renamed"
    assert rename["old_path"] == "docs/architecture/rename_old.md"
    assert by_path["root_tmp.py"]["ownership_status"] == "unmapped"
    assert by_path["root_tmp.py"]["recommended_action"] == "add_file_ownership_mapping_before_commit"


def test_branch_status_is_read_only_and_reports_head(tmp_path: Path) -> None:
    provider = _seed_repo(tmp_path)

    payload = provider.branch_status()

    assert payload["schema_version"] == "aistock_git_branch_status_v1"
    assert payload["branch"]
    assert payload["short_head_commit"]
    assert payload["ahead_count"] == 0
    assert payload["behind_count"] == 0
    assert payload["arbitrary_shell_allowed"] is False
    assert payload["production_8001_touched"] is False


def test_validation_git_status_api_uses_injected_provider(tmp_path: Path) -> None:
    provider = _seed_repo(tmp_path)
    app = FastAPI()
    app.include_router(validation.router, prefix="/api/v1")
    app.dependency_overrides[validation.get_git_status_provider] = lambda: provider
    client = TestClient(app)

    workspace = client.get("/api/v1/validation/git/workspace-status").json()["data"]
    branch = client.get("/api/v1/validation/git/branch-status").json()["data"]

    assert workspace["summary"]["changed_files"] == 7
    assert workspace["summary"]["unmapped_files"] == 1
    assert workspace["production_8001_touched"] is False
    assert branch["short_head_commit"] == workspace["short_head_commit"]
