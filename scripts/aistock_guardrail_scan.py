from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml


DEFAULT_CATALOG = Path("docs/standards/aistock_development_standard_v1.5_20260523.yaml")
SEVERITY_RANK = {"P3": 1, "P2": 2, "P1": 3, "P0": 4, "NONE": 99}


@dataclass(frozen=True)
class CompiledRule:
    rule_id: str
    title: str
    severity: str
    category: str
    checker_type: str
    patterns: tuple[Any, ...]
    checker_options: dict[str, Any]
    include_globs: tuple[str, ...]
    exclude_globs: tuple[str, ...]
    remediation: str
    baseline_policy: str


@dataclass(frozen=True)
class Finding:
    rule_id: str
    title: str
    severity: str
    category: str
    file: str
    line: int
    message: str
    remediation: str
    baseline_policy: str
    fingerprint: str
    baseline_status: str = "unclassified"

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "title": self.title,
            "severity": self.severity,
            "category": self.category,
            "file": self.file,
            "line": self.line,
            "message": self.message,
            "remediation": self.remediation,
            "baseline_policy": self.baseline_policy,
            "fingerprint": self.fingerprint,
            "baseline_status": self.baseline_status,
        }


def load_catalog(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Guardrail catalog must be a mapping: {path}")
    if data.get("schema_version") != "aistock_development_guardrails_v1":
        raise ValueError(f"Unsupported guardrail catalog schema: {data.get('schema_version')}")
    return data


def _as_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(str(item) for item in value)


def compile_rules(catalog: dict[str, Any]) -> list[CompiledRule]:
    import re

    compiled: list[CompiledRule] = []
    for raw_rule in catalog.get("rules", []):
        if not raw_rule.get("enabled", True):
            continue
        checker = raw_rule.get("checker") or {}
        checker_type = str(checker.get("type") or "")
        if checker_type not in {"regex", "path_regex", "regex_and_python_loop_contains"}:
            continue
        checker_options: dict[str, Any] = {}
        if checker_type == "regex_and_python_loop_contains":
            checker_options["loop_patterns"] = tuple(
                re.compile(pattern, re.MULTILINE) for pattern in _as_tuple(checker.get("loop_patterns"))
            )
            checker_options["max_following_lines"] = int(checker.get("max_following_lines") or 8)
        applies_to = raw_rule.get("applies_to") or {}
        compiled.append(
            CompiledRule(
                rule_id=str(raw_rule["rule_id"]),
                title=str(raw_rule.get("title") or raw_rule["rule_id"]),
                severity=str(raw_rule.get("severity") or "P3"),
                category=str(raw_rule.get("category") or "general"),
                checker_type=checker_type,
                patterns=tuple(re.compile(pattern, re.MULTILINE) for pattern in _as_tuple(checker.get("patterns"))),
                checker_options=checker_options,
                include_globs=_as_tuple(applies_to.get("include_globs") or ["**/*"]),
                exclude_globs=_as_tuple(applies_to.get("exclude_globs")),
                remediation=str(raw_rule.get("remediation") or "Review and fix according to AIstock development standards."),
                baseline_policy=str(raw_rule.get("baseline_policy") or "block_new_only"),
            )
        )
    return compiled


def _path_key(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _matches(path_key: str, patterns: Iterable[str]) -> bool:
    for pattern in patterns:
        if fnmatch.fnmatchcase(path_key, pattern):
            return True
        # Treat ** as zero-or-more path segments for repository glob ergonomics.
        if "/**/" in pattern and fnmatch.fnmatchcase(path_key, pattern.replace("/**/", "/")):
            return True
    return False


def _is_text_file(path: Path, suffixes: set[str]) -> bool:
    return path.is_file() and path.suffix.lower() in suffixes


def _skip_path(path: Path, skip_parts: set[str]) -> bool:
    normalized = path.as_posix()
    parts = set(path.parts)
    return any(part in parts or part in normalized for part in skip_parts)


def _indent_width(line: str) -> int:
    return len(line) - len(line.lstrip(" \t"))


def _git_output(args: list[str], root: Path) -> str:
    return subprocess.check_output(
        args,
        cwd=root,
        text=True,
        encoding="utf-8",
        errors="replace",
        stderr=subprocess.DEVNULL,
    )


def git_tracked_files(root: Path, roots: Iterable[str]) -> list[Path]:
    output = _git_output(["git", "ls-files", *roots], root)
    return [root / line.strip() for line in output.splitlines() if line.strip()]


def git_changed_files(root: Path) -> list[Path]:
    changed = _git_output(["git", "diff", "--name-only", "HEAD"], root)
    untracked = _git_output(["git", "ls-files", "--others", "--exclude-standard"], root)
    names = [line.strip() for line in (changed + "\n" + untracked).splitlines() if line.strip()]
    return [root / name for name in dict.fromkeys(names)]


def git_staged_files(root: Path) -> list[Path]:
    output = _git_output(["git", "diff", "--cached", "--name-only", "--diff-filter=ACMRT"], root)
    return [root / line.strip() for line in output.splitlines() if line.strip()]


def iter_files(paths: Iterable[Path], root: Path, suffixes: set[str], skip_parts: set[str]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if not path.exists() or _skip_path(path, skip_parts):
            continue
        if _is_text_file(path, suffixes):
            files.append(path)
            continue
        if path.is_dir():
            for child in path.rglob("*"):
                if not _skip_path(child, skip_parts) and _is_text_file(child, suffixes):
                    files.append(child)
    return sorted(set(files), key=lambda item: _path_key(item, root))


def scan_files(files: Iterable[Path], rules: Iterable[CompiledRule], root: Path) -> list[Finding]:
    findings: list[Finding] = []
    for file_path in files:
        path_key = _path_key(file_path, root)
        for rule in rules:
            if not _matches(path_key, rule.include_globs) or _matches(path_key, rule.exclude_globs):
                continue
            if rule.checker_type == "path_regex":
                for pattern in rule.patterns:
                    if pattern.search(path_key):
                        fingerprint = hashlib.sha256(f"{rule.rule_id}:{path_key}:1".encode("utf-8")).hexdigest()[:16]
                        findings.append(
                            Finding(
                                rule_id=rule.rule_id,
                                title=rule.title,
                                severity=rule.severity,
                                category=rule.category,
                                file=path_key,
                                line=1,
                                message=rule.title,
                                remediation=rule.remediation,
                                baseline_policy=rule.baseline_policy,
                                fingerprint=fingerprint,
                            )
                        )
                continue
            text = file_path.read_text(encoding="utf-8", errors="ignore")
            if rule.checker_type in {"regex", "regex_and_python_loop_contains"}:
                for pattern in rule.patterns:
                    for match in pattern.finditer(text):
                        line = text.count("\n", 0, match.start()) + 1
                        fingerprint = hashlib.sha256(f"{rule.rule_id}:{path_key}:{line}".encode("utf-8")).hexdigest()[:16]
                        findings.append(
                            Finding(
                                rule_id=rule.rule_id,
                                title=rule.title,
                                severity=rule.severity,
                                category=rule.category,
                                file=path_key,
                                line=line,
                                message=rule.title,
                                remediation=rule.remediation,
                                baseline_policy=rule.baseline_policy,
                                fingerprint=fingerprint,
                            )
                        )
            if rule.checker_type == "regex_and_python_loop_contains":
                max_following_lines = int(rule.checker_options.get("max_following_lines") or 8)
                loop_patterns = rule.checker_options.get("loop_patterns") or ()
                lines = text.splitlines()
                for index, line_text in enumerate(lines):
                    stripped = line_text.lstrip(" \t")
                    if not stripped.startswith("for "):
                        continue
                    loop_indent = _indent_width(line_text)
                    end = min(len(lines), index + max_following_lines + 2)
                    for inner_index in range(index + 1, end):
                        inner_line = lines[inner_index]
                        if not inner_line.strip():
                            continue
                        if _indent_width(inner_line) <= loop_indent:
                            break
                        if any(pattern.search(inner_line) for pattern in loop_patterns):
                            line = inner_index + 1
                            fingerprint = hashlib.sha256(
                                f"{rule.rule_id}:{path_key}:{line}".encode("utf-8")
                            ).hexdigest()[:16]
                            findings.append(
                                Finding(
                                    rule_id=rule.rule_id,
                                    title=rule.title,
                                    severity=rule.severity,
                                    category=rule.category,
                                    file=path_key,
                                    line=line,
                                    message=rule.title,
                                    remediation=rule.remediation,
                                    baseline_policy=rule.baseline_policy,
                                    fingerprint=fingerprint,
                                )
                            )
                            break
    return findings


def _changed_line_numbers(root: Path, paths: Iterable[Path], *, staged: bool) -> dict[str, set[int]]:
    args = ["git", "diff", "--unified=0"]
    if staged:
        args.append("--cached")
    args.append("--")
    args.extend(_path_key(path, root) for path in paths)
    output = _git_output(args, root)
    changed: dict[str, set[int]] = {}
    current_file: str | None = None
    new_line = 0
    for raw_line in output.splitlines():
        if raw_line.startswith("+++ b/"):
            current_file = raw_line[6:]
            changed.setdefault(current_file, set())
            continue
        if raw_line.startswith("@@"):
            match = re.search(r"\+(\d+)(?:,(\d+))?", raw_line)
            if match:
                span = int(match.group(2) or "1")
                # Pure insert hunks use +N,0; the first added line is N+1.
                new_line = int(match.group(1)) + (1 if span == 0 else 0)
            continue
        if current_file is None:
            continue
        if raw_line.startswith("+") and not raw_line.startswith("+++"):
            changed.setdefault(current_file, set()).add(new_line)
            new_line += 1
        elif raw_line.startswith("-") and not raw_line.startswith("---"):
            continue
        else:
            new_line += 1
    return changed


def filter_findings_to_changed_lines(
    findings: list[Finding],
    changed_lines_by_file: dict[str, set[int]],
) -> list[Finding]:
    if not changed_lines_by_file:
        return findings
    filtered: list[Finding] = []
    for finding in findings:
        changed_lines = changed_lines_by_file.get(finding.file)
        if changed_lines is None:
            continue
        if not changed_lines or finding.line in changed_lines:
            filtered.append(finding)
    return filtered


def load_baseline_fingerprints(path: Path | None) -> set[str]:
    if path is None or not path.exists():
        return set()
    payload = json.loads(path.read_text(encoding="utf-8"))
    findings = payload.get("findings") if isinstance(payload, dict) else None
    if not isinstance(findings, list):
        raise ValueError(f"Baseline JSON must contain a findings list: {path}")
    fingerprints: set[str] = set()
    for finding in findings:
        if isinstance(finding, dict) and finding.get("fingerprint"):
            fingerprints.add(str(finding["fingerprint"]))
    return fingerprints


def apply_baseline_status(findings: list[Finding], baseline_fingerprints: set[str]) -> list[Finding]:
    if not baseline_fingerprints:
        return [
            Finding(
                rule_id=finding.rule_id,
                title=finding.title,
                severity=finding.severity,
                category=finding.category,
                file=finding.file,
                line=finding.line,
                message=finding.message,
                remediation=finding.remediation,
                baseline_policy=finding.baseline_policy,
                fingerprint=finding.fingerprint,
                baseline_status="new",
            )
            for finding in findings
        ]
    classified: list[Finding] = []
    for finding in findings:
        classified.append(
            Finding(
                rule_id=finding.rule_id,
                title=finding.title,
                severity=finding.severity,
                category=finding.category,
                file=finding.file,
                line=finding.line,
                message=finding.message,
                remediation=finding.remediation,
                baseline_policy=finding.baseline_policy,
                fingerprint=finding.fingerprint,
                baseline_status="baseline" if finding.fingerprint in baseline_fingerprints else "new",
            )
        )
    return classified


def _finding_scope(file_path: str) -> str:
    path_key = file_path.replace("\\", "/").lstrip("/")
    if path_key.startswith("./"):
        path_key = path_key[2:]
    lower_path = path_key.lower()

    if "/catalog/" in lower_path or lower_path.startswith("catalog/"):
        return "config_or_metadata"
    if lower_path.startswith(("backend/tests/", "frontend/tests/", "tests/")):
        return "test_or_validation"
    if lower_path == "noxfile.py" or lower_path.startswith(("scripts/", ".github/workflows/")):
        return "runtime_or_pipeline"
    if lower_path.startswith("frontend/src/"):
        return "frontend_runtime"
    if lower_path.startswith("backend/"):
        return "runtime_or_pipeline"
    if lower_path.startswith("docs/") or lower_path.endswith(".md"):
        return "docs_or_historical"
    if lower_path.startswith(".github/") or lower_path.endswith((".json", ".yaml", ".yml", ".toml")):
        return "config_or_metadata"
    return "other"


def _scope_summary(findings: list[Finding]) -> dict[str, Any]:
    by_scope: dict[str, int] = {}
    by_scope_and_severity: dict[str, dict[str, int]] = {}
    runtime_rules: dict[str, int] = {}
    for finding in findings:
        scope = _finding_scope(finding.file)
        by_scope[scope] = by_scope.get(scope, 0) + 1
        severity_counts = by_scope_and_severity.setdefault(scope, {})
        severity_counts[finding.severity] = severity_counts.get(finding.severity, 0) + 1
        if scope == "runtime_or_pipeline":
            runtime_rules[finding.rule_id] = runtime_rules.get(finding.rule_id, 0) + 1
    return {
        "by_scope": dict(sorted(by_scope.items())),
        "by_scope_and_severity": {
            scope: dict(sorted(counts.items(), key=lambda item: item[0]))
            for scope, counts in sorted(by_scope_and_severity.items())
        },
        "top_runtime_or_pipeline_rules": [
            {"rule_id": rule_id, "count": count}
            for rule_id, count in sorted(runtime_rules.items(), key=lambda item: (-item[1], item[0]))[:10]
        ],
    }


def summarize(findings: list[Finding]) -> dict[str, Any]:
    by_severity: dict[str, int] = {}
    by_rule: dict[str, int] = {}
    by_category: dict[str, int] = {}
    by_baseline_status: dict[str, int] = {}
    for finding in findings:
        by_severity[finding.severity] = by_severity.get(finding.severity, 0) + 1
        by_rule[finding.rule_id] = by_rule.get(finding.rule_id, 0) + 1
        by_category[finding.category] = by_category.get(finding.category, 0) + 1
        by_baseline_status[finding.baseline_status] = by_baseline_status.get(finding.baseline_status, 0) + 1
    scope_summary = _scope_summary(findings)
    return {
        "total_findings": len(findings),
        "by_severity": dict(sorted(by_severity.items(), key=lambda item: item[0])),
        "by_rule": dict(sorted(by_rule.items())),
        "by_category": dict(sorted(by_category.items())),
        "by_baseline_status": dict(sorted(by_baseline_status.items())),
        "by_scope": scope_summary["by_scope"],
        "by_scope_and_severity": scope_summary["by_scope_and_severity"],
        "top_runtime_or_pipeline_rules": scope_summary["top_runtime_or_pipeline_rules"],
    }


def blocking_findings(findings: list[Finding], fail_on_severity: str, *, fail_new_only: bool) -> list[Finding]:
    if fail_on_severity == "NONE":
        return []
    fail_rank = SEVERITY_RANK[fail_on_severity]
    return [
        finding
        for finding in findings
        if SEVERITY_RANK.get(finding.severity, 0) >= fail_rank
        and (not fail_new_only or finding.baseline_status != "baseline")
    ]


def write_json(
    path: Path,
    findings: list[Finding],
    files_scanned: int,
    mode: str,
    *,
    baseline_json: str | None = None,
    fail_on_severity: str = "NONE",
    fail_new_only: bool = False,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    blocked = blocking_findings(findings, fail_on_severity, fail_new_only=fail_new_only)
    payload = {
        "schema_version": "aistock_guardrail_scan_result_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "files_scanned": files_scanned,
        "baseline": {
            "baseline_json": baseline_json,
            "fail_new_only": fail_new_only,
        },
        "gate": {
            "fail_on_severity": fail_on_severity,
            "blocking_count": len(blocked),
            "status": "failed" if blocked else "passed",
        },
        "summary": summarize(findings),
        "findings": [finding.to_dict() for finding in findings],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_summary_md(path: Path, findings: list[Finding], files_scanned: int, mode: str, max_findings: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = summarize(findings)
    lines = [
        "# AIstock Guardrail Baseline Scan",
        "",
        f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
        f"- Mode: `{mode}`",
        f"- Files scanned: {files_scanned}",
        f"- Total findings: {summary['total_findings']}",
        "",
        "## Summary By Baseline Status",
        "",
        "| Status | Count |",
        "|---|---:|",
    ]
    for status, count in summary["by_baseline_status"].items():
        lines.append(f"| `{status}` | {count} |")
    lines.extend(
        [
            "",
            "## Summary By Severity",
            "",
            "| Severity | Count |",
            "|---|---:|",
        ]
    )
    for severity in ("P0", "P1", "P2", "P3"):
        lines.append(f"| {severity} | {summary['by_severity'].get(severity, 0)} |")
    lines.extend(
        [
            "",
            "## Summary By Scope",
            "",
            "| Scope | Count | P0 | P1 | P2 | P3 |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for scope, count in summary["by_scope"].items():
        severity_counts = summary["by_scope_and_severity"].get(scope, {})
        lines.append(
            f"| `{scope}` | {count} | "
            f"{severity_counts.get('P0', 0)} | {severity_counts.get('P1', 0)} | "
            f"{severity_counts.get('P2', 0)} | {severity_counts.get('P3', 0)} |"
        )
    if summary["top_runtime_or_pipeline_rules"]:
        lines.extend(["", "## Top Runtime Or Pipeline Rules", "", "| Rule | Count |", "|---|---:|"])
        for item in summary["top_runtime_or_pipeline_rules"]:
            lines.append(f"| `{item['rule_id']}` | {item['count']} |")
    lines.extend(["", "## Summary By Rule", "", "| Rule | Count |", "|---|---:|"])
    for rule_id, count in summary["by_rule"].items():
        lines.append(f"| `{rule_id}` | {count} |")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "This is a read-only baseline report. It does not mean all historical findings must be fixed immediately.",
            "New or changed P0/P1 findings should be blocked after the changed-files gate is enabled.",
            "Historical findings should be triaged by module and burned down with regression tests.",
            "",
            f"## First {min(max_findings, len(findings))} Findings",
            "",
            "| Severity | Status | Rule | File | Line | Remediation |",
            "|---|---|---|---|---:|---|",
        ]
    )
    for finding in findings[:max_findings]:
        remediation = finding.remediation.replace("|", "/")
        lines.append(
            f"| {finding.severity} | `{finding.baseline_status}` | `{finding.rule_id}` | `{finding.file}` | {finding.line} | {remediation} |"
        )
    if len(findings) > max_findings:
        lines.append("")
        lines.append(f"Report truncated to {max_findings} findings. See JSON output for full machine-readable details.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _format_finding_line(finding: Finding) -> str:
    return (
        f"{finding.severity} {finding.baseline_status} "
        f"{finding.rule_id} {finding.file}:{finding.line} - {finding.title}"
    )


def _artifact_refs(*, output_json: str | None, summary_md: str | None) -> str:
    refs = [item for item in (output_json, summary_md) if item]
    return f", details={','.join(refs)}" if refs else ""


def print_stdout_summary(
    *,
    findings: list[Finding],
    blocked: list[Finding],
    files_scanned: int,
    mode: str,
    output_json: str | None,
    summary_md: str | None,
    verbose_findings: bool,
    max_stdout_findings: int,
) -> None:
    """Keep passing validation output compact while preserving failure details."""
    if verbose_findings:
        visible = findings[:max_stdout_findings]
        for finding in visible:
            print(_format_finding_line(finding))
        if len(findings) > len(visible):
            print(f"... omitted {len(findings) - len(visible)} finding(s); see artifact output for full details.")
    elif blocked:
        visible = blocked[:max_stdout_findings]
        for finding in visible:
            print(_format_finding_line(finding))
        if len(blocked) > len(visible):
            print(f"... omitted {len(blocked) - len(visible)} blocking finding(s); see artifact output for full details.")

    print(
        "Guardrail scan completed: "
        f"mode={mode}, files={files_scanned}, findings={len(findings)}, blocking={len(blocked)}"
        f"{_artifact_refs(output_json=output_json, summary_md=summary_md)}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan AIstock development guardrails.")
    parser.add_argument("paths", nargs="*", help="Files or directories to scan. Defaults to catalog roots.")
    parser.add_argument("--catalog", default=str(DEFAULT_CATALOG), help="Path to the machine-readable development standard YAML.")
    parser.add_argument("--baseline", action="store_true", help="Scan tracked files under catalog roots.")
    parser.add_argument("--changed-only", action="store_true", help="Scan changed and untracked files only.")
    parser.add_argument("--staged-only", action="store_true", help="Scan staged files only; useful before committing in a dirty workspace.")
    parser.add_argument("--baseline-json", help="Existing guardrail JSON whose fingerprints are treated as historical baseline.")
    parser.add_argument("--fail-new-only", action="store_true", help="Do not fail on findings whose fingerprint exists in --baseline-json.")
    parser.add_argument("--output-json", help="Write machine-readable result JSON.")
    parser.add_argument("--summary-md", help="Write human-readable summary Markdown.")
    parser.add_argument("--max-findings-md", type=int, default=200, help="Maximum findings to include in Markdown.")
    parser.add_argument(
        "--verbose-findings",
        action="store_true",
        help="Print findings to stdout even when the gate passes. Default success output is compact.",
    )
    parser.add_argument(
        "--max-stdout-findings",
        type=int,
        default=80,
        help="Maximum findings to print to stdout on failure or with --verbose-findings.",
    )
    parser.add_argument("--fail-on-severity", choices=["P0", "P1", "P2", "P3", "NONE"], default="P0")
    args = parser.parse_args()

    root = Path.cwd()
    catalog = load_catalog(root / args.catalog)
    rules = compile_rules(catalog)
    scan_config = catalog.get("scan") or {}
    suffixes = set(str(item).lower() for item in scan_config.get("text_suffixes", []))
    skip_parts = set(str(item) for item in scan_config.get("skip_parts", []))
    roots = [str(item) for item in scan_config.get("default_roots", [])]

    selected_modes = sum(bool(value) for value in (args.changed_only, args.staged_only, args.baseline, bool(args.paths)))
    if selected_modes > 1:
        raise SystemExit("Choose only one scan mode: paths, --baseline, --changed-only, or --staged-only.")

    if args.changed_only:
        mode = "changed_only"
        candidate_paths = git_changed_files(root)
    elif args.staged_only:
        mode = "staged_only"
        candidate_paths = git_staged_files(root)
    elif args.baseline:
        mode = "baseline_tracked"
        candidate_paths = git_tracked_files(root, roots)
    elif args.paths:
        mode = "paths"
        candidate_paths = [root / item for item in args.paths]
    else:
        mode = "default_roots"
        candidate_paths = [root / item for item in roots]

    files = iter_files(candidate_paths, root=root, suffixes=suffixes, skip_parts=skip_parts)
    findings = scan_files(files, rules=rules, root=root)
    if args.changed_only or args.staged_only:
        findings = filter_findings_to_changed_lines(
            findings,
            _changed_line_numbers(root, files, staged=args.staged_only),
        )
    baseline_path = root / args.baseline_json if args.baseline_json else None
    baseline_fingerprints = load_baseline_fingerprints(baseline_path)
    findings = apply_baseline_status(findings, baseline_fingerprints)
    blocked = blocking_findings(findings, args.fail_on_severity, fail_new_only=args.fail_new_only)

    if args.output_json:
        write_json(
            root / args.output_json,
            findings=findings,
            files_scanned=len(files),
            mode=mode,
            baseline_json=args.baseline_json,
            fail_on_severity=args.fail_on_severity,
            fail_new_only=args.fail_new_only,
        )
    if args.summary_md:
        write_summary_md(root / args.summary_md, findings=findings, files_scanned=len(files), mode=mode, max_findings=args.max_findings_md)
    print_stdout_summary(
        findings=findings,
        blocked=blocked,
        files_scanned=len(files),
        mode=mode,
        output_json=args.output_json,
        summary_md=args.summary_md,
        verbose_findings=args.verbose_findings,
        max_stdout_findings=args.max_stdout_findings,
    )

    return 1 if blocked else 0


if __name__ == "__main__":
    raise SystemExit(main())
