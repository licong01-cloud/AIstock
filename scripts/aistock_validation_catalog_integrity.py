from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT_FOR_IMPORT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT_FOR_IMPORT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_FOR_IMPORT))

from backend.services.validation.catalog_integrity import (
    DEFAULT_OUTPUT_PATH,
    CATALOG_INTEGRITY_SCHEMA,
    run_catalog_integrity,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run AIstock validation catalog integrity checks.")
    parser.add_argument("--repo-root", default=None, help="Repository root to inspect.")
    parser.add_argument(
        "--output-json",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Where to write the structured integrity report JSON.",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Return a non-zero exit code when the report contains warnings.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repo_root = Path(args.repo_root) if args.repo_root else None
    output_path = Path(args.output_json) if args.output_json else None
    report = run_catalog_integrity(repo_root=repo_root, output_path=output_path)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report.get("schema_version") != CATALOG_INTEGRITY_SCHEMA:
        return 1
    if report.get("state") != "passed":
        return 1
    if args.fail_on_warning and (report.get("summary") or {}).get("warning_count", 0):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
