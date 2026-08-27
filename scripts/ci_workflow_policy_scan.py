"""Fail-closed policy scan for CI workflows.

The repository's CI lanes run on pre-provisioned Windows environments.  This
small stdlib-only scanner intentionally checks workflow source rather than
running any project command: dependency installation and disposable database
services must be rejected before a workflow can be merged.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Iterable


DEFAULT_WORKFLOW_ROOT = Path(".github/workflows")
_INSTALL_RE = re.compile(
    r"\b(?:python\s+-m\s+)?pip(?:\d+(?:\.\d+)?)?\s+install\b"
    r"|\bnpm\s+(?:ci|install)\b"
    r"|\b(?:conda|mamba)\s+install\b"
    r"|\bgo\s+mod\s+download\b",
    re.IGNORECASE,
)
_DB_CREATE_RE = re.compile(
    r"\b(?:docker\s+(?:run|compose\s+(?:up|run))|docker-compose\s+up)\b[^\n]*"
    r"\b(?:postgres|timescale)\b",
    re.IGNORECASE,
)
_SETUP_ACTION_RE = re.compile(r"^\s*(?:-\s*)?uses:\s*actions/setup-(?:python|node|go|miniconda)@", re.IGNORECASE)
_SERVICES_RE = re.compile(r"^\s*(?:-\s*)?services\s*:", re.IGNORECASE)
_DB_IMAGE_RE = re.compile(r"^\s*image:\s*[^#\n]*(?:postgres|timescale)", re.IGNORECASE)


def scan_workflow_text(text: str, path: str = "workflow.yml") -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.rstrip()
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        reason: str | None = None
        if _SERVICES_RE.search(line):
            reason = "CI workflow services are prohibited; use the existing DEV database lane"
        elif _DB_IMAGE_RE.search(line):
            reason = "disposable postgres/timescale image is prohibited in CI"
        elif _SETUP_ACTION_RE.search(line):
            reason = "setup-* actions install mutable toolchains; use a prebuilt runner"
        elif _INSTALL_RE.search(line):
            reason = "dependency installation is prohibited in CI"
        elif _DB_CREATE_RE.search(line):
            reason = "creating a postgres/timescale container is prohibited in CI"
        if reason:
            findings.append({"path": path, "line": str(line_number), "reason": reason, "text": line.strip()})
    return findings


def scan_workflows(paths: Iterable[Path]) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for path in paths:
        findings.extend(scan_workflow_text(path.read_text(encoding="utf-8"), path.as_posix()))
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow-root", type=Path, default=DEFAULT_WORKFLOW_ROOT)
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args(argv)
    paths = sorted(path for path in args.workflow_root.glob("*.yml"))
    findings = scan_workflows(paths)
    payload = {
        "schema_version": "aistock_ci_workflow_policy_receipt_v1",
        "workflow_count": len(paths),
        "status": "pass" if not findings else "blocked",
        "findings": findings,
    }
    encoded = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if not findings else 1


if __name__ == "__main__":
    raise SystemExit(main())
