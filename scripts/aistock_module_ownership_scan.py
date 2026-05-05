from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT_FOR_IMPORT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT_FOR_IMPORT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_FOR_IMPORT))

from backend.services.validation.file_ownership import (
    FileOwnershipCatalog,
    FileOwnershipError,
    write_scan_outputs,
)
from backend.services.validation.module_registry import ModuleRegistry, ModuleRegistryError, REPO_ROOT


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scan AIstock files against the module registry and file ownership catalog."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Optional repo-relative paths to scan. If omitted, tracked repository files are scanned unless a mode flag is used.",
    )
    parser.add_argument(
        "--module-registry",
        type=Path,
        default=None,
        help="Override module_registry.yaml path.",
    )
    parser.add_argument(
        "--file-ownership",
        type=Path,
        default=None,
        help="Override file_ownership.yaml path.",
    )
    parser.add_argument(
        "--include-untracked",
        action="store_true",
        help="Include git untracked files when scanning the repository.",
    )
    parser.add_argument(
        "--changed-only",
        action="store_true",
        help="Scan git changed files plus untracked files, excluding deletions.",
    )
    parser.add_argument(
        "--staged-only",
        action="store_true",
        help="Scan git staged files, excluding deletions.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=REPO_ROOT,
        help="Repository root used for git-based scan modes.",
    )
    parser.add_argument(
        "--fail-on-unmapped",
        action="store_true",
        help="Exit non-zero if any scanned file is unmapped.",
    )
    parser.add_argument(
        "--fail-on-ambiguous",
        action="store_true",
        help="Exit non-zero if any scanned file has ambiguous ownership.",
    )
    parser.add_argument("--output-json", type=Path, default=None, help="Write machine-readable scan JSON.")
    parser.add_argument("--summary-md", type=Path, default=None, help="Write human-readable scan summary.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.changed_only and args.staged_only:
        parser.error("--changed-only and --staged-only are mutually exclusive.")
    if args.paths and (args.changed_only or args.staged_only):
        parser.error("Explicit paths cannot be combined with --changed-only or --staged-only.")
    registry = ModuleRegistry(args.module_registry) if args.module_registry else ModuleRegistry()
    catalog = FileOwnershipCatalog(args.file_ownership, module_registry=registry)
    try:
        # Load both catalogs first so schema errors fail before any scan output is written.
        registry.load()
        catalog.load()
        if args.changed_only:
            payload = catalog.scan_changed_files(repo_root=args.repo_root)
        elif args.staged_only:
            payload = catalog.scan_staged_files(repo_root=args.repo_root)
        elif args.paths:
            payload = catalog.scan_paths(args.paths)
        else:
            payload = catalog.scan_repository(repo_root=args.repo_root, include_untracked=args.include_untracked)
    except (FileOwnershipError, ModuleRegistryError) as exc:
        print(f"module ownership scan failed: {exc}", file=sys.stderr)
        return 2

    write_scan_outputs(payload, output_json=args.output_json, summary_md=args.summary_md)
    totals = payload["totals"]
    print(
        "Module ownership scan completed: "
        f"files={totals['files']}, mapped={totals['mapped_files']}, "
        f"unmapped={totals['unmapped_files']}, ambiguous={totals['ambiguous_files']}"
    )
    if args.fail_on_unmapped and totals["unmapped_files"]:
        return 1
    if args.fail_on_ambiguous and totals["ambiguous_files"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
