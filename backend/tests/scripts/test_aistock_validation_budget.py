from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scripts import aistock_validation_budget as audit


@dataclass(frozen=True)
class _Ownership:
    primary_module: str | None
    ownership_status: str = "mapped"
    layer: str | None = None


class _Catalog:
    def __init__(self, owners: dict[str, str | tuple[str | None, str] | None]) -> None:
        self.owners = owners

    def match_path(self, path: str) -> _Ownership:
        owner = self.owners.get(path)
        if isinstance(owner, tuple):
            module_id, layer = owner
            return _Ownership(module_id, "mapped" if module_id else "unmapped", layer)
        return _Ownership(owner, "mapped" if owner else "unmapped")


def _write(root: Path, path: str, text: str) -> None:
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def test_effective_sloc_ignores_blank_line_comments_and_block_comments() -> None:
    source = """
// line comment
/* block
comment */
const first = 1;

const second = 2; // inline comment still belongs to code
"""

    assert audit._effective_sloc(source, ".ts") == 2


def test_build_audit_counts_tracked_code_once_by_primary_owner(tmp_path: Path) -> None:
    paths = [
        "backend/services/example.py",
        "backend/tests/test_example.py",
        "backend/tests/helpers/example_fixture.py",
        "backend/tests/test_only.py",
        "support/contract_fixture.py",
        "docs/ignored.py",
        "qlib_src_backup/vendor.py",
    ]
    _write(tmp_path, paths[0], "value = 1\nvalue = 2\nvalue = 3\n")
    _write(tmp_path, paths[1], "def test_one():\n    assert True\n")
    _write(tmp_path, paths[2], "fixture = object()\n")
    _write(tmp_path, paths[3], "assert True\n")
    _write(tmp_path, paths[4], "contract_fixture = True\n")
    _write(tmp_path, paths[5], "document_helper = True\n")
    _write(tmp_path, paths[6], "vendor = True\n")
    catalog = _Catalog(
        {
            paths[0]: "example",
            paths[1]: "example",
            paths[2]: "example",
            paths[3]: "tests.backend",
            paths[4]: ("example", "backend_test_helper"),
            paths[5]: "docs",
        }
    )

    report = audit.build_audit(
        repo_root=tmp_path,
        catalog=catalog,  # type: ignore[arg-type]
        tracked_paths=paths,
        max_ratio=0.30,
        top_files=1,
    )
    rows = {row["module_id"]: row for row in report["modules"]}

    assert rows["example"]["production_sloc"] == 3
    assert rows["example"]["test_sloc"] == 4
    assert rows["example"]["test_to_production_percent"] == 133.33
    assert rows["example"]["over_budget_sloc"] == 4
    assert rows["example"]["largest_test_files"] == [
        {"path": "backend/tests/test_example.py", "sloc": 2}
    ]
    assert rows["tests.backend"]["test_only_bucket"] is True
    assert rows["docs"]["production_sloc"] == 1
    assert report["totals"]["test_only_bucket_count"] == 1
    assert all(row["module_id"] != "__unmapped__" for row in report["modules"])


def test_test_path_detection_covers_helpers_and_language_conventions() -> None:
    assert audit._is_test_path("backend/tests/helpers/factory.py")
    assert audit._is_test_path("tdx-api-main/pkg/router_test.go")
    assert audit._is_test_path("frontend/src/example.spec.ts")
    assert audit._is_test_path("frontend/src/example.test.tsx")
    assert not audit._is_test_path("backend/services/testing_policy.py")


def test_max_ratio_validation_is_fail_closed(tmp_path: Path) -> None:
    try:
        audit.build_audit(repo_root=tmp_path, catalog=_Catalog({}), tracked_paths=[], max_ratio=0)  # type: ignore[arg-type]
    except ValueError as exc:
        assert "max_ratio" in str(exc)
    else:
        raise AssertionError("invalid max_ratio must fail closed")


def test_cli_rejects_explicit_forbidden_test_only_owner(tmp_path: Path, monkeypatch, capsys) -> None:
    report = {
        "totals": {
            "production_sloc": 0,
            "test_sloc": 1,
            "test_to_production_percent": None,
            "module_count": 1,
            "over_budget_module_count": 1,
            "test_only_bucket_count": 1,
        },
        "modules": [
            {
                "module_id": "tests.backend",
                "test_only_bucket": False,
                "test_files": 1,
                "over_budget": True,
                "over_budget_sloc": 1,
            }
        ],
    }
    monkeypatch.setattr(audit, "build_audit", lambda **_: report)
    class _Loader:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def load(self) -> None:
            pass

    monkeypatch.setattr(audit, "ModuleRegistry", _Loader)
    monkeypatch.setattr(audit, "FileOwnershipCatalog", _Loader)

    result = audit.main(
        ["--repo-root", str(tmp_path), "--json", "--fail-test-only-owner", "tests.backend"]
    )

    assert result == 1
    assert "forbidden test-only owners: tests.backend" in capsys.readouterr().err


def test_repository_has_no_executable_tests_in_generic_backend_bucket() -> None:
    root = audit.REPO_ROOT_FOR_IMPORT
    registry = audit.ModuleRegistry(root / "tests/aistock_validation/catalog/module_registry.yaml")
    catalog = audit.FileOwnershipCatalog(
        root / "tests/aistock_validation/catalog/file_ownership.yaml",
        module_registry=registry,
    )
    registry.load()
    catalog.load()

    offenders = [
        path
        for path in audit._git_tracked_paths(root)
        if audit._is_executable_source(path)
        and audit._is_test_path(path)
        and catalog.match_path(path).primary_module == "tests.backend"
    ]

    assert offenders == []


def test_workflow_automation_has_an_honest_production_denominator() -> None:
    root = audit.REPO_ROOT_FOR_IMPORT
    registry = audit.ModuleRegistry(root / "tests/aistock_validation/catalog/module_registry.yaml")
    catalog = audit.FileOwnershipCatalog(
        root / "tests/aistock_validation/catalog/file_ownership.yaml",
        module_registry=registry,
    )
    registry.load()
    catalog.load()

    assert catalog.match_path("scripts/aistock_issue_workflow.py").primary_module == (
        "validation.workflow_automation"
    )
    assert catalog.match_path("scripts/aistock_guardrail_scan.py").primary_module == (
        "validation.guardrails"
    )
    report = audit.build_audit(
        repo_root=root,
        catalog=catalog,
        tracked_paths=audit._git_tracked_paths(root),
        max_ratio=0.30,
        top_files=1,
    )
    workflow = next(
        row for row in report["modules"] if row["module_id"] == "validation.workflow_automation"
    )

    assert workflow["production_sloc"] > 0
    assert workflow["test_only_bucket"] is False
