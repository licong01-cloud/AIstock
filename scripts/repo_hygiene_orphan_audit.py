from __future__ import annotations

import argparse
import csv
import fnmatch
import json
import os
import re
import subprocess
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "aistock_repo_hygiene_orphan_audit_v1"
DEFAULT_OUTPUT_DIR = Path("tmp") / "validation" / "code-intelligence" / "repo-hygiene"
CODE_EXTENSIONS = {".py", ".ts", ".tsx", ".js", ".jsx"}
DOC_EXTENSIONS = {".md", ".mdx", ".rst"}
DATA_EXTENSIONS = {".csv", ".json", ".jsonl", ".log", ".txt", ".pkl", ".pickle", ".dump", ".sqlite", ".db"}
AUDIT_EXTENSIONS = (
    CODE_EXTENSIONS | DOC_EXTENSIONS | DATA_EXTENSIONS | {".yaml", ".yml", ".toml", ".sql", ".ps1", ".sh", ".bat"}
)
DEFAULT_SCAN_MODE = "tracked"
DEFAULT_MAX_SECONDS = int(os.environ.get("AISTOCK_REPO_HYGIENE_MAX_SECONDS", "300"))
DEFAULT_GIT_LOG_MAX_COMMITS = int(os.environ.get("AISTOCK_REPO_HYGIENE_GIT_LOG_MAX_COMMITS", "2000"))
DEFAULT_MAX_TEXT_BYTES = int(os.environ.get("AISTOCK_REPO_HYGIENE_MAX_TEXT_BYTES", "600000"))

PROTECTED_PREFIXES = (
    ".github/",
    ".codex/",
    ".claude/",
    "backend/migrations/",
    "backend/alembic/",
    "tests/aistock_validation/bugs/",
    "tests/aistock_validation/catalog/",
    "docs/standards/",
    "configs/",
    "prompt_packs/validation_llm/",
)
PROTECTED_FILES = {
    "AGENTS.md",
    "AGENTS.override.md",
    "README.md",
    "pyproject.toml",
    "ruff.toml",
    "noxfile.py",
    ".pre-commit-config.yaml",
    ".semgrep.yml",
    ".mcp.json",
}
EXCLUDED_PREFIXES = (
    ".git/",
    ".idea/",
    ".rtk/",
    ".tox/",
    ".venv/",
    ".vscode/",
    ".pytest_cache/",
    ".ruff_cache/",
    ".mypy_cache/",
    ".codegraph/",
    ".understand-anything/intermediate/",
    ".understand-anything/tmp/",
    "frontend/.next",
    "frontend/node_modules/",
    "build/",
    "dist/",
    "env/",
    "htmlcov/",
    "node_modules/",
    "tmp/validation/",
    "tmp/issue_workflow/",
    "venv/",
    "__pycache__/",
)
REFERENCE_SKIP_PREFIXES = (
    "docs/archive/",
    "tests/aistock_validation/bugs/",
    "tests/aistock_validation/history/",
)
DOC_ALLOWED_PREFIXES = (
    "docs/analysis/",
    "docs/architecture/",
    "docs/design/",
    "docs/handoff/",
    "docs/operations/",
    "docs/standards/",
    "docs/archive/",
    "docs/contracts/",
)
DOC_ROOT_ALLOWED = {"README.md", "AGENTS.md", "AGENTS.override.md"}
DEBUG_PATTERNS = (
    "*debug*",
    "*diagnostic*",
    "*tmp*",
    "*temp*",
    "*scratch*",
    "*dump*",
    "*checkpoint*",
    "*manual*",
    "*smoke*",
    "*oneoff*",
    "*adhoc*",
)
TEMP_PATTERNS = ("*tmp*", "*temp*", "*scratch*", "*dump*", "*checkpoint*")
BACKFILL_PATTERNS = ("*backfill*", "*migration*", "*migrate*")
ENTRYPOINT_HINTS = (
    'if __name__ == "__main__"',
    "if __name__ == '__main__'",
    "argparse.ArgumentParser",
    "@router.",
    "FastMCP(",
    "@mcp.tool",
    "nox.session",
    "pytest.fixture",
    "click.command",
    "typer.Typer",
)
REFERENCE_EXTENSIONS = CODE_EXTENSIONS | DOC_EXTENSIONS | {".yaml", ".yml", ".toml", ".json", ".sql", ".ps1", ".sh"}
TEXT_TOKEN_RE = re.compile(r"[A-Za-z0-9_./\\-]{4,}")


class AuditBudgetExceeded(RuntimeError):
    """Raised when the audit exceeds its configured wall-clock budget."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def norm(value: str | Path) -> str:
    text = str(value).replace("\\", "/")
    return text[2:] if text.startswith("./") else text


def repo_rel(path: Path, root: Path) -> str:
    try:
        return norm(path.relative_to(root))
    except ValueError:
        return norm(path)


def git_lines(root: Path, *args: str, timeout: int = 30) -> list[str]:
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), *args],
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return []
    if proc.returncode != 0:
        return []
    return [line.strip().replace("\\", "/") for line in proc.stdout.splitlines() if line.strip()]


def git_value(root: Path, *args: str) -> str | None:
    lines = git_lines(root, *args)
    return lines[0] if lines else None


def tracked_files(root: Path, *, include_untracked: bool = False) -> list[str]:
    args = ["ls-files"]
    if include_untracked:
        args.extend(["--cached", "--others", "--exclude-standard"])
    return [item for item in git_lines(root, *args) if not is_excluded(item)]


def discovered_files_and_empty_dirs(root: Path, *, include_untracked: bool = False) -> tuple[list[str], list[str]]:
    git_files = tracked_files(root, include_untracked=include_untracked)
    if not include_untracked:
        return sorted(set(git_files)), []

    files: set[str] = set()
    empty_dirs: set[str] = set()
    root = root.resolve()
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = Path(dirpath).relative_to(root)
        filtered_dirnames: list[str] = []
        for dirname in dirnames:
            rel = norm(rel_dir / dirname)
            if is_excluded(rel):
                continue
            filtered_dirnames.append(dirname)
        dirnames[:] = filtered_dirnames
        filtered_filenames: list[str] = []
        for filename in filenames:
            rel = norm(rel_dir / filename)
            if is_excluded(rel):
                continue
            filtered_filenames.append(filename)
            if not git_files:
                files.add(rel)
        if rel_dir != Path(".") and not filtered_dirnames and not filtered_filenames:
            rel = norm(rel_dir)
            if not is_excluded(rel):
                empty_dirs.add(rel)
    if git_files:
        files.update(git_files)
    return sorted(files), sorted(empty_dirs)


def is_excluded(path: str) -> bool:
    p = norm(path)
    return (
        p == ".git"
        or any(p == prefix.rstrip("/") or p.startswith(prefix) for prefix in EXCLUDED_PREFIXES)
        or "/__pycache__/" in p
    )


def safe_text(path: Path, max_chars: int = 200_000) -> str:
    try:
        return path.read_text(encoding="utf-8-sig", errors="replace")[:max_chars]
    except OSError:
        return ""


def file_type(path: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix in CODE_EXTENSIONS:
        return "code"
    if suffix in DOC_EXTENSIONS:
        return "doc"
    if suffix in {".yaml", ".yml", ".toml"}:
        return "config"
    if suffix == ".sql":
        return "sql"
    if suffix in DATA_EXTENSIONS:
        return "data_or_artifact"
    return "other"


def protected_reason(path: str) -> str | None:
    p = norm(path)
    if p in PROTECTED_FILES:
        return "protected project entry/config file"
    for prefix in PROTECTED_PREFIXES:
        if p.startswith(prefix):
            return f"protected prefix {prefix}"
    return None


def basename_stem(path: str) -> str:
    return Path(path).stem


def reference_tokens(path: str) -> list[str]:
    p = norm(path)
    name = Path(p).name
    stem = Path(p).stem
    tokens = [p, name]
    if stem and len(stem) >= 4:
        tokens.append(stem)
    if p.endswith(".py"):
        mod = p[:-3].replace("/", ".")
        tokens.extend([mod, mod.rsplit(".", 1)[-1]])
    return list(dict.fromkeys(tokens))


def build_reference_index(root: Path, files: list[str]) -> dict[str, set[str]]:
    refs: dict[str, set[str]] = {}
    token_to_targets: dict[str, set[str]] = {}
    for target in files:
        for token in reference_tokens(target):
            if len(token) < 4:
                continue
            token_to_targets.setdefault(token, set()).add(target)
    searchable = [
        p
        for p in files
        if Path(p).suffix.lower() in REFERENCE_EXTENSIONS
        and not any(norm(p).startswith(prefix) for prefix in REFERENCE_SKIP_PREFIXES)
        and (size := file_size(root, p)) is not None
        and size <= DEFAULT_MAX_TEXT_BYTES
    ]
    for rel in searchable:
        text = safe_text(root / rel, max_chars=350_000)
        if not text:
            continue
        found_targets: set[str] = set()
        for token in set(TEXT_TOKEN_RE.findall(text)):
            for target in token_to_targets.get(token.replace("\\", "/"), set()):
                if target != rel:
                    found_targets.add(target)
        for target in found_targets:
            refs.setdefault(target, set()).add(rel)
    return refs


def build_reference_index_slow(root: Path, files: list[str]) -> dict[str, set[str]]:
    refs: dict[str, set[str]] = {}
    searchable = [p for p in files if Path(p).suffix.lower() in REFERENCE_EXTENSIONS]
    for rel in searchable:
        text = safe_text(root / rel, max_chars=350_000)
        if not text:
            continue
        for target in files:
            if target == rel:
                continue
            for token in reference_tokens(target):
                if token and token in text:
                    refs.setdefault(target, set()).add(rel)
                    break
    return refs


def entrypoint_detected(root: Path, path: str) -> bool:
    if Path(path).suffix.lower() not in CODE_EXTENSIONS | {".yaml", ".yml", ".toml", ".sql"}:
        return False
    text = safe_text(root / path, max_chars=120_000)
    return any(hint in text for hint in ENTRYPOINT_HINTS)


def category_for(
    path: str, *, ref_count: int, entrypoint: bool, protected: str | None
) -> tuple[str, str, str, str, bool]:
    p = norm(path)
    lower = p.lower()
    suffix = Path(p).suffix.lower()
    name = Path(p).name.lower()
    if protected:
        return "protected", "P0", "keep", protected, False
    if suffix in DOC_EXTENSIONS and p.startswith("docs/") and not p.startswith(DOC_ALLOWED_PREFIXES):
        return "misplaced_doc", "P2", "relocate", "doc path is outside approved docs taxonomy", False
    if suffix in DOC_EXTENSIONS and not p.startswith("docs/") and p not in DOC_ROOT_ALLOWED:
        return "misplaced_doc", "P2", "relocate", "doc file is outside docs/ or approved root docs", False
    if suffix in DATA_EXTENSIONS:
        if ref_count == 0 and not entrypoint:
            if (
                p.startswith("docs/analysis/")
                or p.startswith("docs/archive/")
                or p.startswith("tests/aistock_validation/history/")
            ):
                return (
                    "legacy_artifact",
                    "P3",
                    "archive",
                    "historical evidence artifact without detected references",
                    True,
                )
            if any(fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(lower, pattern) for pattern in TEMP_PATTERNS):
                if not p.startswith("tests/") and not p.startswith("docs/") and not p.startswith("configs/"):
                    return (
                        "generated_or_temp_artifact",
                        "P4",
                        "delete_candidate",
                        "temporary artifact outside evidence directories",
                        True,
                    )
            if not p.startswith("docs/") and not p.startswith("tests/") and not p.startswith("configs/"):
                return (
                    "generated_or_temp_artifact",
                    "P4",
                    "delete_candidate",
                    "unreferenced data artifact outside protected directories",
                    True,
                )
    if any(fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(lower, pattern) for pattern in BACKFILL_PATTERNS):
        if ref_count == 0 and not entrypoint:
            return (
                "legacy_backfill_or_migration_like",
                "P1",
                "review",
                "backfill/migration-like file may be one-time but can retain audit value",
                True,
            )
        return (
            "backfill_or_migration_entry",
            "P0",
            "keep",
            "backfill/migration-like file has references or entrypoint hints",
            False,
        )
    if any(fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(lower, pattern) for pattern in DEBUG_PATTERNS):
        if ref_count == 0 and not entrypoint:
            return (
                "legacy_debug_or_smoke",
                "P1",
                "review",
                "debug/smoke/manual-looking file lacks references and entrypoint hints",
                True,
            )
        return (
            "debug_or_smoke_referenced",
            "P1",
            "review",
            "debug/smoke/manual-looking file should be reviewed before cleanup",
            True,
        )
    if suffix in CODE_EXTENSIONS and ref_count == 0 and not entrypoint:
        if p.startswith("scripts/"):
            return (
                "manual_script_or_orphan_candidate",
                "P1",
                "review",
                "script has no detected references; may be manual CLI entrypoint",
                True,
            )
        return "orphan_code_candidate", "P1", "review", "code file has no detected references or entrypoint hints", True
    if "legacy" in lower or "retired" in lower or "old" in lower:
        return "legacy_named_asset", "P3", "archive", "legacy/retired naming indicates possible archive candidate", True
    return "referenced_or_low_signal", "P0", "keep", "referenced, protected, or low-confidence cleanup signal", False


def git_last_commit(root: Path, path: str) -> str | None:
    value = git_value(root, "log", "-1", "--format=%h %cs", "--", path)
    return value


def git_last_commit_map(root: Path, *, max_commits: int = DEFAULT_GIT_LOG_MAX_COMMITS) -> dict[str, str]:
    try:
        proc = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "log",
                f"--max-count={max_commits}",
                "--name-only",
                "--pretty=format:COMMIT %h %cs",
            ],
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        return {}
    if proc.returncode != 0:
        return {}
    last_commit: dict[str, str] = {}
    current: str | None = None
    for raw in proc.stdout.splitlines():
        line = raw.strip().replace("\\", "/")
        if not line:
            continue
        if line.startswith("COMMIT "):
            current = line.removeprefix("COMMIT ").strip()
            continue
        if current and line not in last_commit:
            last_commit[line] = current
    return last_commit


def file_mtime(root: Path, path: str) -> str | None:
    try:
        return datetime.fromtimestamp((root / path).stat().st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except OSError:
        return None


def file_size(root: Path, path: str) -> int | None:
    try:
        return (root / path).stat().st_size
    except OSError:
        return None


def row_for(
    root: Path, path: str, refs: dict[str, set[str]], last_commit: dict[str, str] | None = None
) -> dict[str, Any]:
    ref_sources = sorted(refs.get(path, set()))
    protected = protected_reason(path)
    entrypoint = entrypoint_detected(root, path)
    category, risk, action, reason, approval = category_for(
        path,
        ref_count=len(ref_sources),
        entrypoint=entrypoint,
        protected=protected,
    )
    return {
        "path": path,
        "file_type": file_type(path),
        "last_modified": file_mtime(root, path),
        "git_last_commit": (last_commit or {}).get(path),
        "reference_count": len(ref_sources),
        "reference_examples": ";".join(ref_sources[:5]),
        "entrypoint_detected": entrypoint,
        "category": category,
        "risk_level": risk,
        "suggested_action": action,
        "reason": reason,
        "needs_human_approval": approval,
    }


def row_for_empty_dir(root: Path, path: str, last_commit: dict[str, str] | None = None) -> dict[str, Any]:
    protected = protected_reason(path)
    if protected:
        category, risk, action, reason, approval = "protected", "P0", "keep", protected, False
    else:
        category, risk, action, reason, approval = (
            "empty_directory",
            "P4",
            "delete_candidate",
            "directory contains no files or subdirectories",
            True,
        )
    return {
        "path": path,
        "file_type": "empty_dir",
        "last_modified": file_mtime(root, path),
        "git_last_commit": (last_commit or {}).get(path),
        "reference_count": 0,
        "reference_examples": "",
        "entrypoint_detected": False,
        "category": category,
        "risk_level": risk,
        "suggested_action": action,
        "reason": reason,
        "needs_human_approval": approval,
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_risk = Counter(str(row["risk_level"]) for row in rows)
    by_action = Counter(str(row["suggested_action"]) for row in rows)
    by_category = Counter(str(row["category"]) for row in rows)
    candidates = [row for row in rows if row["needs_human_approval"] or row["suggested_action"] != "keep"]
    return {
        "total_scanned": len(rows),
        "candidate_count": len(candidates),
        "protected_count": by_risk.get("P0", 0),
        "review_count": by_risk.get("P1", 0),
        "relocate_count": by_risk.get("P2", 0),
        "archive_count": by_risk.get("P3", 0),
        "delete_candidate_count": by_risk.get("P4", 0),
        "by_risk": dict(sorted(by_risk.items())),
        "by_action": dict(sorted(by_action.items())),
        "by_category": dict(sorted(by_category.items())),
    }


def check_budget(started_at: float, max_seconds: int | None, phase: str) -> None:
    if max_seconds is not None and max_seconds > 0 and time.monotonic() - started_at > max_seconds:
        raise AuditBudgetExceeded(f"repo hygiene audit exceeded {max_seconds}s during {phase}")


def build_audit(
    root: Path = ROOT,
    *,
    run_id: str | None = None,
    max_rows: int | None = None,
    scan_mode: str = DEFAULT_SCAN_MODE,
    max_seconds: int | None = DEFAULT_MAX_SECONDS,
) -> dict[str, Any]:
    started_at = time.monotonic()
    include_untracked = scan_mode == "tracked-and-untracked"
    discovered_files, empty_dirs = discovered_files_and_empty_dirs(root, include_untracked=include_untracked)
    check_budget(started_at, max_seconds, "file discovery")
    files = [p for p in discovered_files if Path(p).suffix.lower() in AUDIT_EXTENSIONS]
    if max_rows is not None:
        files = files[:max_rows]
    refs = build_reference_index(root, files)
    check_budget(started_at, max_seconds, "reference indexing")
    last_commit = git_last_commit_map(root)
    check_budget(started_at, max_seconds, "git commit indexing")
    rows = [row_for(root, path, refs, last_commit) for path in files]
    rows.extend(row_for_empty_dir(root, path, last_commit) for path in empty_dirs)
    check_budget(started_at, max_seconds, "row classification")
    rows.sort(key=lambda row: (str(row["risk_level"]), str(row["category"]), str(row["path"])))
    summary = summarize(rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "run_id": run_id,
        "workflow_gate": "ready",
        "audit_key": "repo_hygiene_orphan_audit",
        "warning_only": True,
        "candidate_only": True,
        "scan_mode": scan_mode,
        "elapsed_seconds": round(time.monotonic() - started_at, 3),
        "source_modifications_allowed": False,
        "cleanup_requires_human_pr": True,
        "side_effects": {
            "readonly_inputs": True,
            "writes_source": False,
            "writes_database": False,
            "writes_business_state": False,
            "deletes_files": False,
            "moves_files": False,
            "writes_github_issue": False,
        },
        "risk_policy": {
            "P0": "keep/protected; never auto-clean",
            "P1": "review; human-confirmed cleanup or archive PR required",
            "P2": "relocate candidate; human-reviewed docs/metadata PR required",
            "P3": "archive candidate; preserve evidence trail",
            "P4": "delete candidate; human-reviewed PR only",
        },
        "summary": summary,
        "rows": rows,
        "production_gates": {
            "production_ddl_gate": "noop",
            "production_frontend_dependency_gate": "noop",
            "production_backend_dependency_gate": "noop",
        },
    }


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "path",
        "file_type",
        "last_modified",
        "git_last_commit",
        "reference_count",
        "entrypoint_detected",
        "category",
        "risk_level",
        "suggested_action",
        "reason",
        "needs_human_approval",
        "reference_examples",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fields})


def render_markdown(payload: dict[str, Any], *, max_rows: int = 80) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    candidates = [row for row in rows if row.get("needs_human_approval") or row.get("suggested_action") != "keep"]
    lines = [
        "# Repo Hygiene Orphan Audit",
        "",
        f"- workflow_gate: `{payload.get('workflow_gate') or 'unknown'}`",
        f"- audit_key: `{payload.get('audit_key')}`",
        f"- warning_only: `{bool(payload.get('warning_only'))}`",
        f"- candidate_only: `{bool(payload.get('candidate_only'))}`",
        f"- cleanup_requires_human_pr: `{bool(payload.get('cleanup_requires_human_pr'))}`",
        f"- scanned: `{summary.get('total_scanned', 0)}`",
        f"- candidates: `{summary.get('candidate_count', 0)}`",
        f"- P1_review: `{summary.get('review_count', 0)}`",
        f"- P2_relocate: `{summary.get('relocate_count', 0)}`",
        f"- P3_archive: `{summary.get('archive_count', 0)}`",
        f"- P4_delete_candidate: `{summary.get('delete_candidate_count', 0)}`",
        "",
        "## Safety Policy",
        "",
        "- Nightly only generates artifacts; it does not delete, move, or modify source files.",
        "- Protected assets include migrations, BUG JSON, standards, CI/nox/workflows, production config, MCP/tools/routers, and dynamic entrypoints.",
        "- Cleanup must be split into human-reviewed PRs by category: docs relocate, debug/backfill cleanup, unused-code review, or archive.",
        "",
        "## Top Candidates",
        "",
        "| risk | action | category | refs | entry | path | reason |",
        "| --- | --- | --- | ---: | --- | --- | --- |",
    ]
    for row in candidates[:max_rows]:
        reason = str(row.get("reason") or "").replace("|", "\\|")[:160]
        path = str(row.get("path") or "").replace("|", "\\|")
        lines.append(
            f"| {row.get('risk_level')} | {row.get('suggested_action')} | {row.get('category')} | {row.get('reference_count')} | {row.get('entrypoint_detected')} | `{path}` | {reason} |"
        )
    if len(candidates) > max_rows:
        lines.append(f"\n_Only first {max_rows} candidates are shown; see CSV/JSON for full details._")
    lines.extend(
        [
            "",
            "## Production Gates",
            "",
            "- production_ddl_gate: `noop`",
            "- production_frontend_dependency_gate: `noop`",
            "- production_backend_dependency_gate: `noop`",
        ]
    )
    return "\n".join(lines).rstrip("\n") + "\n"


def write_outputs(payload: dict[str, Any], output: Path, markdown_output: Path, csv_output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.write_text(render_markdown(payload), encoding="utf-8")
    write_csv(payload.get("rows") or [], csv_output)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a read-only AIstock repo hygiene orphan audit artifact.")
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--run-id")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--markdown-output", default=None)
    parser.add_argument("--csv-output", default=None)
    parser.add_argument("--max-rows", type=int, default=None, help="Test helper to limit scanned files.")
    parser.add_argument(
        "--scan-mode",
        choices=["tracked", "tracked-and-untracked"],
        default=DEFAULT_SCAN_MODE,
        help="Default tracks only git-managed files to keep Nightly bounded; opt in to untracked scans for manual audits.",
    )
    parser.add_argument(
        "--max-seconds",
        type=int,
        default=DEFAULT_MAX_SECONDS,
        help="Wall-clock budget before failing with compact diagnostics. Use 0 to disable.",
    )
    parser.add_argument("--json", action="store_true", help="Print compact JSON summary to stdout.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.root).resolve()
    output_dir = Path(args.output_dir) if args.output_dir else root / DEFAULT_OUTPUT_DIR
    output = Path(args.output) if args.output else output_dir / "repo-hygiene-orphan-audit.json"
    markdown_output = (
        Path(args.markdown_output) if args.markdown_output else output_dir / "repo-hygiene-orphan-audit.md"
    )
    csv_output = Path(args.csv_output) if args.csv_output else output_dir / "repo-hygiene-orphan-audit.csv"
    try:
        payload = build_audit(
            root,
            run_id=args.run_id,
            max_rows=args.max_rows,
            scan_mode=args.scan_mode,
            max_seconds=args.max_seconds,
        )
    except AuditBudgetExceeded as exc:
        compact_error = {
            "schema_version": SCHEMA_VERSION,
            "workflow_gate": "blocked",
            "audit_key": "repo_hygiene_orphan_audit",
            "error": str(exc),
            "production_gates": {
                "production_ddl_gate": "noop",
                "production_frontend_dependency_gate": "noop",
                "production_backend_dependency_gate": "noop",
            },
        }
        if args.json:
            print(json.dumps(compact_error, ensure_ascii=False, sort_keys=True))
        else:
            print(f"FAIL repo-hygiene-orphan-audit {exc}")
        return 2
    write_outputs(payload, output, markdown_output, csv_output)
    compact = {
        "schema_version": payload["schema_version"],
        "workflow_gate": payload["workflow_gate"],
        "audit_key": payload["audit_key"],
        "scan_mode": payload["scan_mode"],
        "elapsed_seconds": payload["elapsed_seconds"],
        "candidate_count": payload["summary"]["candidate_count"],
        "delete_candidate_count": payload["summary"]["delete_candidate_count"],
        "markdown_output": repo_rel(markdown_output.resolve(), root),
        "csv_output": repo_rel(csv_output.resolve(), root),
        "production_gates": payload["production_gates"],
    }
    if args.json:
        print(json.dumps(compact, ensure_ascii=False, sort_keys=True))
    else:
        print(
            "PASS repo-hygiene-orphan-audit "
            f"candidates={compact['candidate_count']} delete_candidates={compact['delete_candidate_count']} "
            f"markdown={compact['markdown_output']} csv={compact['csv_output']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
