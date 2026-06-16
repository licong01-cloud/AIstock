from __future__ import annotations

import argparse
import json
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


def _finding_dict(rule: Rule, file_path: Path, line: int) -> dict[str, object]:
    return {
        "severity": rule.severity,
        "code": rule.code,
        "file": file_path.as_posix(),
        "line": line,
        "message": rule.message,
    }


def _write_json(path: str, payload: dict[str, object]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _format_finding(rule: Rule, file_path: Path, line: int) -> str:
    return f"{rule.severity} {rule.code} {file_path}:{line} - {rule.message}"


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="*", help="Files or directories to scan.")
    parser.add_argument("--changed-only", action="store_true", help="Scan git changed files only.")
    parser.add_argument("--fail-on", choices=["HIGH", "MEDIUM", "LOW", "NONE"], default="HIGH")
    parser.add_argument("--output-json", help="Write full machine-readable findings to a JSON artifact.")
    parser.add_argument(
        "--verbose-findings",
        action="store_true",
        help="Print all findings even when the gate passes. Default success output is compact.",
    )
    parser.add_argument(
        "--max-stdout-findings",
        type=int,
        default=80,
        help="Maximum findings to print to stdout on failure or with --verbose-findings.",
    )
    args = parser.parse_args(argv)

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

    should_fail = any(severity_rank[item[0].severity] >= fail_rank for item in findings)
    if args.output_json:
        _write_json(
            args.output_json,
            {
                "schema_version": "aistock_verify_skill_guardrail_scan_v1",
                "paths": [path.as_posix() for path in paths],
                "finding_count": len(findings),
                "blocking_count": sum(1 for item in findings if severity_rank[item[0].severity] >= fail_rank),
                "fail_on": args.fail_on,
                "status": "failed" if should_fail else "passed",
                "findings": [_finding_dict(rule, file_path, line) for rule, file_path, line in findings],
            },
        )
    if should_fail or args.verbose_findings:
        visible = findings[: args.max_stdout_findings]
        for rule, file_path, line in visible:
            print(_format_finding(rule, file_path, line))
        if len(findings) > len(visible):
            print(f"... omitted {len(findings) - len(visible)} finding(s); see artifact output for full details.")

    if should_fail:
        print(f"Guardrail scan failed with {len(findings)} finding(s).")
        return 1
    suffix = f" details={args.output_json}" if args.output_json else ""
    print(f"Guardrail scan completed with {len(findings)} finding(s).{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
