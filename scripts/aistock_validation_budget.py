from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT_FOR_IMPORT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT_FOR_IMPORT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_FOR_IMPORT))

from backend.services.validation.file_ownership import FileOwnershipCatalog, FileOwnershipError  # noqa: E402
from backend.services.validation.module_registry import ModuleRegistry, ModuleRegistryError  # noqa: E402


SCHEMA_VERSION = "aistock_validation_budget_v1"
CODE_SUFFIXES = {".bat", ".go", ".js", ".jsx", ".ps1", ".py", ".sh", ".sql", ".ts", ".tsx"}
EXCLUDED_PREFIXES = (
    "frontend/.next/",
    "frontend/node_modules/",
    "node_modules/",
    "qlib_src_backup/",
    "third_party/",
    "vendor/",
)


def _git_tracked_paths(repo_root: Path) -> list[str]:
    completed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=30,
    )
    if completed.returncode != 0:
        error = completed.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"git ls-files failed: {error}")
    return [item.decode("utf-8", errors="strict").replace("\\", "/") for item in completed.stdout.split(b"\0") if item]


def _is_test_path(path: str) -> bool:
    normalized = path.replace("\\", "/")
    parts = normalized.split("/")
    name = parts[-1].lower()
    return (
        "tests" in {part.lower() for part in parts[:-1]}
        or name.startswith("test_")
        or name.endswith("_test.go")
        or ".spec." in name
        or ".test." in name
    )


def _is_executable_source(path: str) -> bool:
    normalized = path.replace("\\", "/")
    suffix = Path(normalized).suffix.lower()
    executable_workflow = normalized.startswith(".github/workflows/") and suffix in {".yml", ".yaml"}
    return (suffix in CODE_SUFFIXES or executable_workflow) and not any(
        normalized.startswith(prefix) for prefix in EXCLUDED_PREFIXES
    )


def _effective_sloc(text: str, suffix: str) -> int:
    suffix = suffix.lower()
    line_comment_prefixes = ("#",) if suffix in {".py", ".ps1", ".sh"} else ("//",)
    if suffix == ".sql":
        line_comment_prefixes = ("--",)
    count = 0
    in_block_comment = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if suffix in {".go", ".js", ".jsx", ".ts", ".tsx", ".sql"}:
            if in_block_comment:
                if "*/" in line:
                    line = line.split("*/", 1)[1].strip()
                    in_block_comment = False
                    if not line:
                        continue
                else:
                    continue
            if line.startswith("/*"):
                if "*/" in line[2:]:
                    line = line.split("*/", 1)[1].strip()
                    if not line:
                        continue
                else:
                    in_block_comment = True
                    continue
        if any(line.startswith(prefix) for prefix in line_comment_prefixes):
            continue
        count += 1
    return count


def _read_sloc(repo_root: Path, relative_path: str) -> int:
    path = (repo_root / relative_path).resolve()
    try:
        path.relative_to(repo_root.resolve())
    except ValueError as exc:
        raise RuntimeError(f"tracked path escapes repository root: {relative_path}") from exc
    if not path.is_file() or path.is_symlink():
        return 0
    return _effective_sloc(path.read_text(encoding="utf-8", errors="replace"), path.suffix)


def build_audit(
    *,
    repo_root: Path,
    catalog: FileOwnershipCatalog,
    tracked_paths: Iterable[str] | None = None,
    max_ratio: float = 0.30,
    top_files: int = 20,
) -> dict[str, Any]:
    if not 0 < max_ratio <= 1:
        raise ValueError("max_ratio must be within (0, 1]")
    paths = list(tracked_paths) if tracked_paths is not None else _git_tracked_paths(repo_root)
    modules: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "production_sloc": 0,
            "test_sloc": 0,
            "production_files": 0,
            "test_files": 0,
            "largest_test_files": [],
        }
    )
    totals = {"production_sloc": 0, "test_sloc": 0, "production_files": 0, "test_files": 0}
    skipped_non_code = 0
    for relative_path in sorted(dict.fromkeys(path.replace("\\", "/") for path in paths)):
        if not _is_executable_source(relative_path):
            skipped_non_code += 1
            continue
        sloc = _read_sloc(repo_root, relative_path)
        ownership = catalog.match_path(relative_path)
        module_id = ownership.primary_module or f"__{ownership.ownership_status}__"
        ownership_layer = str(ownership.layer or "").lower()
        is_test = _is_test_path(relative_path) or "test" in ownership_layer
        kind = "test" if is_test else "production"
        modules[module_id][f"{kind}_sloc"] += sloc
        modules[module_id][f"{kind}_files"] += 1
        totals[f"{kind}_sloc"] += sloc
        totals[f"{kind}_files"] += 1
        if is_test and sloc:
            modules[module_id]["largest_test_files"].append({"path": relative_path, "sloc": sloc})

    rows: list[dict[str, Any]] = []
    for module_id, values in modules.items():
        production_sloc = int(values["production_sloc"])
        test_sloc = int(values["test_sloc"])
        allowed_test_sloc = int(production_sloc * max_ratio)
        ratio = None if production_sloc == 0 else test_sloc / production_sloc
        row = {
            "module_id": module_id,
            "production_sloc": production_sloc,
            "test_sloc": test_sloc,
            "production_files": int(values["production_files"]),
            "test_files": int(values["test_files"]),
            "test_to_production_ratio": ratio,
            "test_to_production_percent": None if ratio is None else round(ratio * 100, 2),
            "allowed_test_sloc": allowed_test_sloc,
            "over_budget_sloc": max(0, test_sloc - allowed_test_sloc),
            "test_only_bucket": production_sloc == 0 and test_sloc > 0,
            "largest_test_files": sorted(
                values["largest_test_files"],
                key=lambda item: (-int(item["sloc"]), str(item["path"])),
            )[:top_files],
        }
        row["over_budget"] = bool(row["test_only_bucket"] or (ratio is not None and ratio > max_ratio))
        rows.append(row)

    rows.sort(key=lambda item: (-int(item["over_budget_sloc"]), str(item["module_id"])))
    global_ratio = None if totals["production_sloc"] == 0 else totals["test_sloc"] / totals["production_sloc"]
    return {
        "schema_version": SCHEMA_VERSION,
        "policy": {
            "max_ratio": max_ratio,
            "tracked_files_only": True,
            "test_helpers_and_fixtures_count_as_test": True,
            "excluded_prefixes": list(EXCLUDED_PREFIXES),
            "code_suffixes": sorted(CODE_SUFFIXES),
        },
        "totals": {
            **totals,
            "test_to_production_ratio": global_ratio,
            "test_to_production_percent": None if global_ratio is None else round(global_ratio * 100, 2),
            "module_count": len(rows),
            "over_budget_module_count": sum(1 for row in rows if row["over_budget"]),
            "test_only_bucket_count": sum(1 for row in rows if row["test_only_bucket"]),
            "skipped_non_code_files": skipped_non_code,
        },
        "modules": rows,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit tracked AIstock test debt by primary module ownership.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT_FOR_IMPORT)
    parser.add_argument("--module-registry", type=Path, default=None)
    parser.add_argument("--file-ownership", type=Path, default=None)
    parser.add_argument("--max-ratio", type=float, default=0.30)
    parser.add_argument("--top-files", type=int, default=20)
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--json", action="store_true", help="Print the complete JSON report instead of a compact summary.")
    parser.add_argument("--fail-over-budget", action="store_true")
    parser.add_argument(
        "--fail-test-only-owner",
        action="append",
        default=[],
        help="Fail when the named synthetic owner still contains any executable test files.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    repo_root = args.repo_root.resolve()
    registry_path = args.module_registry or repo_root / "tests/aistock_validation/catalog/module_registry.yaml"
    ownership_path = args.file_ownership or repo_root / "tests/aistock_validation/catalog/file_ownership.yaml"
    try:
        registry = ModuleRegistry(registry_path)
        catalog = FileOwnershipCatalog(ownership_path, module_registry=registry)
        registry.load()
        catalog.load()
        report = build_audit(
            repo_root=repo_root,
            catalog=catalog,
            max_ratio=args.max_ratio,
            top_files=max(0, args.top_files),
        )
    except (FileOwnershipError, ModuleRegistryError, OSError, RuntimeError, ValueError) as exc:
        print(f"test debt audit failed: {exc}", file=sys.stderr)
        return 2
    if args.output_json:
        _write_json(args.output_json, report)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        totals = report["totals"]
        print(
            "test_debt_audit "
            f"production_sloc={totals['production_sloc']} test_sloc={totals['test_sloc']} "
            f"ratio={totals['test_to_production_percent']}% modules={totals['module_count']} "
            f"over_budget={totals['over_budget_module_count']} test_only={totals['test_only_bucket_count']}"
        )
        for row in report["modules"][:10]:
            if not row["over_budget"]:
                continue
            ratio = "test-only" if row["test_only_bucket"] else f"{row['test_to_production_percent']}%"
            print(f"  {row['module_id']}: ratio={ratio} excess_sloc={row['over_budget_sloc']}")
    forbidden_test_only = {
        str(owner)
        for owner in args.fail_test_only_owner
        if any(
            row["module_id"] == str(owner) and int(row.get("test_files") or 0) > 0
            for row in report["modules"]
        )
    }
    if forbidden_test_only:
        print(
            "forbidden test-only owners: " + ", ".join(sorted(forbidden_test_only)),
            file=sys.stderr,
        )
        return 1
    return 1 if args.fail_over_budget and report["totals"]["over_budget_module_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
