from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


TEXT_SUFFIXES = {
    ".py",
    ".ts",
    ".tsx",
    ".js",
    ".jsx",
    ".yaml",
    ".yml",
    ".json",
    ".sql",
    ".ps1",
    ".sh",
}
SKIP_PARTS = {
    ".git",
    ".next",
    ".codex",
    ".codex_tmp",
    ".semgrep",
    "node_modules",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".hypothesis",
    "mlruns",
    "rdagent_assets",
}


@dataclass(frozen=True)
class Rule:
    code: str
    severity: str
    pattern: re.Pattern[str]
    message: str


RULES = [
    Rule(
        "HARD_CODED_WSL_UNC",
        "HIGH",
        re.compile(r"\\\\wsl(?:\$|\.localhost)", re.IGNORECASE),
        "Direct Windows-side WSL UNC runtime access is forbidden.",
    ),
    Rule(
        "HARD_CODED_LOCAL_PATH",
        "MEDIUM",
        re.compile(r"(?:[A-Za-z]:\\Users\\|/mnt/[a-z]/Dev/AIstock|F:\\Dev\\AIstock)", re.IGNORECASE),
        "Hardcoded workstation path must be replaced by config/catalog/runtime input.",
    ),
    Rule(
        "POSSIBLE_SECRET",
        "HIGH",
        re.compile(r"(?i)(password|passwd|secret|api[_-]?key|token)\s*[:=]\s*['\"][^'\"]{8,}"),
        "Possible hardcoded secret.",
    ),
    Rule(
        "SILENT_EMPTY_SUCCESS",
        "HIGH",
        re.compile(r"except\s+Exception[^\n]*:\s*(?:\n\s*){0,3}return\s+(?:\[\]|\{\}|None|True)", re.MULTILINE),
        "Exception handler may silently return fake success/default.",
    ),
    Rule(
        "FORBIDDEN_TRADING_FALLBACK",
        "HIGH",
        re.compile(r"(?i)(fallback|降级).{0,100}(daily|日频|twap|default|默认)"),
        "Review possible forbidden trading fallback.",
    ),
    Rule(
        "RAW_JSON_UI",
        "MEDIUM",
        re.compile(r"JSON\.stringify|<pre>|JsonPanel", re.IGNORECASE),
        "Review UI for raw JSON exposure to ordinary operators.",
    ),
]


def _git_changed_files() -> list[Path]:
    output = subprocess.check_output(
        ["git", "diff", "--name-only", "HEAD"],
        text=True,
        stderr=subprocess.DEVNULL,
    )
    return [Path(line.strip()) for line in output.splitlines() if line.strip()]


def _iter_files(paths: Iterable[Path]) -> Iterable[Path]:
    for path in paths:
        if not path.exists():
            continue
        if any(part in SKIP_PARTS for part in path.parts):
            continue
        if path.is_file():
            if path.suffix.lower() in TEXT_SUFFIXES:
                yield path
            continue
        for child in path.rglob("*"):
            if any(part in SKIP_PARTS for part in child.parts):
                continue
            if child.is_file() and child.suffix.lower() in TEXT_SUFFIXES:
                yield child


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="*", help="Files or directories to scan.")
    parser.add_argument("--changed-only", action="store_true", help="Scan git changed files only.")
    parser.add_argument("--fail-on", choices=["HIGH", "MEDIUM", "LOW", "NONE"], default="HIGH")
    args = parser.parse_args()

    paths = _git_changed_files() if args.changed_only else [Path(item) for item in args.paths]
    if not paths:
        print("Guardrail scan skipped: no matching paths.")
        return 0

    severity_rank = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "NONE": 99}
    fail_rank = severity_rank[args.fail_on]
    findings: list[tuple[Rule, Path, int]] = []
    for file_path in _iter_files(paths):
        text = file_path.read_text(encoding="utf-8", errors="ignore")
        for rule in RULES:
            for match in rule.pattern.finditer(text):
                line = text.count("\n", 0, match.start()) + 1
                findings.append((rule, file_path, line))

    for rule, file_path, line in findings:
        print(f"{rule.severity} {rule.code} {file_path}:{line} - {rule.message}")

    should_fail = any(severity_rank[item[0].severity] >= fail_rank for item in findings)
    if should_fail:
        print(f"Guardrail scan failed with {len(findings)} finding(s).")
        return 1
    print(f"Guardrail scan completed with {len(findings)} finding(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
