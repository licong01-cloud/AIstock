from __future__ import annotations

import argparse
import contextlib
import fnmatch
import hashlib
import io
import json
import os
import platform
import re
import signal
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml

try:
    from scripts.aistock_bug_id_allocator import (
        FINGERPRINT_INDEX_VERSION,
        BugIdLockError,
        GlobalBugIdLock,
        bootstrap_fingerprint_index,
        compact_terminal_reservation,
        compact_terminal_reservations as compact_terminal_reservations,
        find_matching_reservation,
        read_allocator_state,
        read_reservations,
        remove_fingerprint_index,
        write_fingerprint_index,
        write_allocator_state,
    )
except ModuleNotFoundError:  # Direct execution: python scripts/aistock_issue_workflow.py
    from aistock_bug_id_allocator import (
        FINGERPRINT_INDEX_VERSION,
        BugIdLockError,
        GlobalBugIdLock,
        bootstrap_fingerprint_index,
        compact_terminal_reservation,
        compact_terminal_reservations as compact_terminal_reservations,
        find_matching_reservation,
        read_allocator_state,
        read_reservations,
        remove_fingerprint_index,
        write_fingerprint_index,
        write_allocator_state,
    )

REPO_ROOT = Path(__file__).resolve().parents[1]
BUGS_ROOT = REPO_ROOT / "tests" / "aistock_validation" / "bugs"
WORKFLOW_ROOT = Path("tmp") / "issue_workflow"
ALLOWED_FIX_STATUSES = {"open", "in_progress"}
GITHUB_REPO = "licong01-cloud/AIstock"
ACTIVE_WORKFLOW_STATES = {
    "discovered",
    "context_ready",
    "fix_in_progress",
    "fix_applied",
    "validation_planned",
    "validation_running",
    "validation_passed",
    "pushed",
    "pr_opened",
    "ci_running",
    "ci_green",
}
TERMINAL_WORKFLOW_STATES = {"merged", "close_synced", "cleanup_done", "complete"}
CLEANUP_BATCH_MANIFEST_SCHEMA = "aistock_cleanup_after_merge_batch_manifest_v1"
CLEANUP_BATCH_RESULT_SCHEMA = "aistock_cleanup_after_merge_batch_v1"
CLEANUP_BATCH_MAX_TARGETS = 200
CLEANUP_BATCH_TARGET_KEYS = {
    "branch",
    "bug_id",
    "worktree",
    "pr_url",
    "source_receipt_path",
}
NON_BLOCKING_CHECK_CONCLUSIONS = {"SUCCESS", "NEUTRAL", "SKIPPED"}
MERGE_QUALITY_CHECK_CONTEXTS = (
    "CI verdict",
    "CodeQL verdict",
)
ARTIFACT_PATH_PATTERNS = (
    ".codex_tmp",
    ".coverage",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "__pycache__",
    "catboost_info",
    ".next",
    "node_modules",
)
BUG_ID_RE = re.compile(r"\bBUG-(\d{3,})\b", re.IGNORECASE)
BUG_ID_FILENAME_RE = re.compile(r"(?:^|[^A-Za-z0-9])BUG-(\d{3,})(?!\d)", re.IGNORECASE)
OUTPUT_FORMAT_TOKENS = {"json", "yaml", "yml", "text", "txt", "stdout", "stderr", "console"}
OUTPUT_FORMAT_CHOICES = ("compact", "summary", "full-json")
PR_BODY_CODEGRAPH_TEST_LIMIT = 10
ACTIONABLE_CI_CLASSIFICATIONS = {"real_regression_candidate", "test_fixture_gap_or_real_regression"}
SUPERSEDED_CI_CLASSIFICATIONS = {
    "superseded_by_later_main_success",
    "superseded_by_later_branch_success",
}
NIGHTLY_BUG_CANDIDATE_ISSUE_PAYLOAD_SCHEMA = "aistock_bug_candidate_github_issue_payload_v1"
NIGHTLY_BUG_CANDIDATE_READY_THRESHOLD = 0.80
SAFE_OUTPUT_DIRS = (WORKFLOW_ROOT, Path("tmp") / "validation")
COMMITTABLE_BUG_REGISTRY_PATHS = (
    "tests/aistock_validation/bugs",
)
FAST_PATH_TIER_ORDER = {"T0": 0, "T1": 1, "T2": 2, "T3": 3}
VALIDATION_RECEIPT_SCHEMA = "aistock_validation_receipt_v1"
SOURCE_MERGE_RECEIPT_SCHEMA = "aistock_source_merge_receipt_v1"
RUNTIME_CONTRACT_SCHEMA = "aistock_bug_runtime_contract_v1"
RUNTIME_INFERENCE_PLANNED_SCOPE = "planned_scope"
RUNTIME_INFERENCE_ACTUAL_CHANGED_FILES = "actual_changed_files"
FILE_SCOPE_SOURCE_PLANNED_INTAKE = "planned_intake"
FILE_SCOPE_SOURCE_GIT_FINISH = "git_finish"
RUNTIME_VERIFY_RECEIPT_SCHEMA = "aistock_post_restart_verify_receipt_v1"
RUNTIME_VERIFY_RECEIPT_SUMMARY_SCHEMA = "aistock_post_restart_verify_receipt_summary_v1"
RUNTIME_TARGET_CATALOG = REPO_ROOT / "docs" / "standards" / "aistock_runtime_targets_v1.yaml"
RUNTIME_IMPACTS = {"none", "frontend", "client", "database", "backend", "worker_scheduler", "unknown"}
_DATASET_RELEASE_WORKER_HEARTBEAT_MODE = "dataset_release_worker_heartbeat"
_DATASET_RELEASE_WORKER_HEARTBEAT_REFS = {
    "health_ref": "worker_heartbeat.health",
    "identity_ref": "worker_heartbeat.identity",
    "business_smoke_ref": "worker_heartbeat.business",
    "database_readback_ref": "not_required",
}
VALIDATION_PASS_RE = re.compile(r"\b(?:pass|passed|success|successful|ok)\b|\b\d+\s+passed\b", re.IGNORECASE)
VALIDATION_FAIL_RE = re.compile(r"\b(?:fail|failed|failure|error|blocked)\b", re.IGNORECASE)
VALIDATION_RECEIPT_COMMIT_RE = re.compile(
    r"validation-receipt:\s+id=[0-9a-f]{16}\s+commit=([0-9a-f]{7,40})\b",
    re.IGNORECASE,
)
WORKTREE_TRANSIENT_CACHE_DIRS = {
    ".codex_tmp",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "catboost_info",
    "node_modules",
}
WORKTREE_TRANSIENT_PREFIXES = ("tmp/", "var/research_assistant/")
WORKTREE_TRANSIENT_EXACT_FILES = {".coverage", "debug.log"}
WORKTREE_QE_LIVE_LOG_ROOT = "rdagent_assets/qe_live_logs"
WORKTREE_QE_LIVE_LOG_SCHEMA = "qe_live_log_record_v1"
WORKTREE_QE_LIVE_LOG_MAX_FILE_BYTES = 16 * 1024 * 1024
WORKTREE_QE_LIVE_LOG_PATHS = frozenset(
    f"{WORKTREE_QE_LIVE_LOG_ROOT}/qe-live-{index}.jsonl" for index in range(5)
)
WORKTREE_BACKEND_LOG_ROOT = "backend/logs"
WORKTREE_BACKEND_LOG_LIMITS = {
    "backend/logs/aistock.log": 10 * 1024 * 1024,
    "backend/logs/errors.log": 5 * 1024 * 1024,
}
WORKTREE_BACKEND_LOG_LINE_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} "
    r"(?:DEBUG|INFO|WARNING|ERROR|CRITICAL) \[[^\]\r\n]+\] .+$"
)
RTK_COMMAND_PREFIX = r"(?:rtk(?:\.exe)?\s+)?"
VALIDATION_COMMAND_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("nox", re.compile(rf"^{RTK_COMMAND_PREFIX}(?:python(?:\.exe)?\s+-m\s+)?nox\s+-s\s+(?P<plan>[A-Za-z0-9_-]+)\b", re.IGNORECASE)),
    (
        "pytest",
        re.compile(
            rf"^{RTK_COMMAND_PREFIX}(?:python(?:\.exe)?\s+-m\s+)?pytest\b[^\r\n]*(?:backend[/\\]tests|frontend[/\\]tests|tests)[/\\]\S+",
            re.IGNORECASE,
        ),
    ),
    ("ruff", re.compile(rf"^{RTK_COMMAND_PREFIX}(?:python(?:\.exe)?\s+-m\s+)?ruff\s+check\b", re.IGNORECASE)),
    ("diff_check", re.compile(rf"^{RTK_COMMAND_PREFIX}git\s+diff\s+--check\b", re.IGNORECASE)),
    ("compile", re.compile(rf"^{RTK_COMMAND_PREFIX}python(?:\.exe)?\s+-m\s+(?:compileall|py_compile)\b", re.IGNORECASE)),
    (
        "workflow_smoke",
        re.compile(
            rf"^{RTK_COMMAND_PREFIX}python(?:\.exe)?\s+scripts/aistock_issue_workflow\.py\s+(?:batch-)?workflow-smoke\b",
            re.IGNORECASE,
        ),
    ),
    ("feature_validation", re.compile(rf"^{RTK_COMMAND_PREFIX}python(?:\.exe)?\s+scripts/aistock_feature_workflow\.py\s+validate\b", re.IGNORECASE)),
    ("frontend", re.compile(rf"^{RTK_COMMAND_PREFIX}(?:npm|npx)\s+(?:run|exec|test)\b", re.IGNORECASE)),
    ("go", re.compile(rf"^{RTK_COMMAND_PREFIX}go\s+test\b", re.IGNORECASE)),
)
FAST_PATH_REGISTRY_PREFIXES = ("tests/aistock_validation/bugs/",)
FAST_PATH_CATALOG_PREFIXES = ("tests/aistock_validation/catalog/",)
FAST_PATH_WORKFLOW_FILES = {
    "scripts/aistock_issue_workflow.py",
    "scripts/issue_flow.py",
    "scripts/ci_failure_issue_summary.py",
    "scripts/code_intelligence_adapter.py",
}
FAST_PATH_DEPENDENCY_FILES = {
    "frontend/package.json",
    "frontend/package-lock.json",
    "frontend/pnpm-lock.yaml",
    "package.json",
    "package-lock.json",
    "pnpm-lock.yaml",
    "requirements.txt",
    "requirements-dev.txt",
    "requirements.lock.txt",
    "pyproject.toml",
}

UI_ROUTE_HINTS = {
    "advisory": {
        "routes": ["/paper-v2/advisory"],
        "scope": [
            "frontend/src/app/paper-v2/advisory/page.tsx",
            "frontend/src/lib/api/advisory.ts",
            "frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts",
            "backend/routers/advisory.py",
            "backend/services/advisory_program.py",
            "backend/tests/watchlist/test_advisory_api.py",
            "backend/tests/watchlist/test_advisory_program.py",
        ],
        "verification": [
            "frontend_type_lint",
            "watchlist_backend",
        ],
    },
    "paper_v2": {
        "routes": ["/paper-v2"],
        "scope": [
            "frontend/src/app/paper-v2",
            "frontend/src/lib/api",
            "frontend/tests/paper-v2",
            "backend/routers/paper_trading_v2.py",
            "backend/services/paper_trading_v2",
        ],
        "verification": ["frontend_type_lint", "paper_v2_ui", "paper_v2_backend"],
    },
}
UI_KEYWORDS = ("ui", "页面", "前端", "显示", "按钮", "弹窗", "表格", "分页", "排序", "json", "route", "page")

sys.path.insert(0, str(REPO_ROOT))
from scripts import issue_flow as flow  # noqa: E402
from scripts import ci_failure_issue_summary as ci_failure_summary  # noqa: E402
from scripts import code_intelligence_adapter as code_intelligence  # noqa: E402


class WorkflowError(ValueError):
    """Raised when the high-level AIstock issue workflow cannot proceed safely."""


class CleanupBlockedError(WorkflowError):
    """Raised with the completed cleanup preflight so recovery need not rebuild it."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        blocking = [str(item) for item in payload.get("blocking") or [] if str(item).strip()]
        super().__init__("; ".join(blocking) or "cleanup preflight blocked")


class GitHubOutcomeUnknownError(WorkflowError):
    """Raised when a GitHub write may have succeeded but cannot be confirmed."""


class WorkflowPayloadError(WorkflowError):
    """Raised with a compact machine-readable recovery payload."""

    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__(str(payload.get("message") or payload.get("reason") or "workflow payload error"))
        self.payload = payload


def _estimate_tokens(text: str) -> int:
    if not text:
        return 0
    # Conservative mixed Chinese/English estimate for workflow budgeting.
    return max(1, (len(text) + 3) // 4)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _today_compact() -> str:
    return datetime.now().strftime("%Y%m%d")


def _slug(value: str, max_len: int = 72) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return (slug or "issue")[:max_len].strip("-") or "issue"


def _short_hash(*values: Any, length: int = 8) -> str:
    payload = json.dumps(values, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise WorkflowError(f"JSON root must be an object: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    )
    temporary_path = Path(handle.name)
    try:
        with handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass


def _size_and_token_estimate(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "bytes": 0, "estimated_tokens": 0}
    text = path.read_text(encoding="utf-8", errors="replace") if path.is_file() else ""
    return {
        "path": str(path),
        "exists": True,
        "bytes": path.stat().st_size,
        "estimated_tokens": _estimate_tokens(text),
    }


LOCAL_PREMERGE_PLAN_KEYS = {
    "l0",
    "guardrail_changed_files",
    "validation_catalog_integrity",
    "validation_module_registry_l0",
}
BROAD_VALIDATION_PLAN_SUFFIXES = ("_backend", "_ui", "_l2", "_l3")
BROAD_VALIDATION_PLAN_KEYS = {
    "data_quality_deep",
    "paper_v2_l3",
    "dr_validate",
}
LOCAL_VALIDATION_TOKENS = (
    "ruff",
    "compile",
    "py_compile",
    "diff --check",
    "git diff --check",
    "grep_guard",
    "section",
    "scope",
    "lint",
)


def _known_plan_keys() -> set[str]:
    return set(flow._plans_by_key())


def _is_broad_validation_plan(value: str) -> bool:
    plan = value.strip()
    if not plan or plan in LOCAL_PREMERGE_PLAN_KEYS:
        return False
    if plan in BROAD_VALIDATION_PLAN_KEYS:
        return True
    if plan.startswith("ra_phase"):
        return True
    return plan.endswith(BROAD_VALIDATION_PLAN_SUFFIXES)


def _is_local_validation_item(value: str) -> bool:
    item = value.strip()
    if not item:
        return False
    normalized = item.replace("\\", "/")
    lower = normalized.lower()
    if item in LOCAL_PREMERGE_PLAN_KEYS:
        return True
    if lower.endswith((".py", ".ts", ".tsx", ".js")) and (
        normalized.startswith("backend/tests/")
        or normalized.startswith("frontend/tests/")
        or normalized.startswith("tests/")
    ):
        return True
    return any(token in lower for token in LOCAL_VALIDATION_TOKENS)


def _split_validation_budget_items(items: Iterable[Any]) -> dict[str, list[str]]:
    known_plans = _known_plan_keys()
    local: list[str] = []
    deferred: list[str] = []
    for raw in items:
        item = str(raw or "").strip()
        if not item:
            continue
        if _is_broad_validation_plan(item):
            deferred.append(item)
        elif item in known_plans and item not in LOCAL_PREMERGE_PLAN_KEYS and not _is_local_validation_item(item):
            deferred.append(item)
        else:
            local.append(item)
    return {
        "local": flow._unique_strings(local),
        "deferred": flow._unique_strings(deferred),
    }


def _apply_validation_budget(
    *,
    record: dict[str, Any],
    validation: dict[str, Any],
    record_required: list[str] | None = None,
) -> dict[str, Any]:
    """Keep pre-merge BUG validation narrow and move broad plans to nightly/VC."""

    selected_required_items = flow._unique_strings(validation.get("required_plans") or [])
    selected_direct = [item for item in selected_required_items if item != "l0"]
    selected_local = selected_direct or selected_required_items
    selected_recommended = flow._unique_strings(validation.get("recommended_plans") or [])
    record_split = _split_validation_budget_items(record_required or record.get("required_verification") or [])
    local_required = flow._unique_strings([*record_split["local"], *selected_local]) or ["l0"]
    if any(item != "l0" for item in local_required):
        local_required = [item for item in local_required if item != "l0"]
    deferred = flow._unique_strings(item for item in record_split["deferred"] if item not in selected_local)
    budgeted = dict(validation)
    budgeted["required_plans"] = local_required
    budgeted["recommended_plans"] = flow._unique_strings([*selected_recommended, *deferred])
    budgeted["deferred_nightly_plans"] = deferred
    budgeted["validation_budget_gate"] = {
        "schema_version": "aistock_validation_budget_gate_v1",
        "premerge_required": local_required,
        "deferred_nightly_plans": deferred,
        "policy": "broad module/UI/API/business-flow plans are nightly/VC by default; run pre-merge only on explicit request or production-gate need",
    }
    return budgeted


def _deferred_modules_from_plans(module: str, plans: list[str]) -> list[str]:
    if not plans:
        return []
    modules = [module] if module else []
    modules.extend(str(item).replace("_backend", "").replace("_ui", "").replace("_l2", "").replace("_l3", "") for item in plans)
    return [item for item in flow._unique_strings(modules) if item]


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _sha256_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sha256_tree(path: Path) -> str | None:
    if not path.exists():
        return None
    if path.is_file():
        return _sha256_file(path)
    digest = hashlib.sha256()
    for child in sorted(item for item in path.rglob("*") if item.is_file()):
        if ".git" in child.parts:
            continue
        rel = child.relative_to(path).as_posix()
        digest.update(rel.encode("utf-8"))
        digest.update(b"\0")
        digest.update((_sha256_file(child) or "").encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


WORKFLOW_RULE_DIGEST_REFS = (
    "AGENTS.md",
    ".codex/skills/aistock-task-router/SKILL.md",
    ".codex/skills/fix-aistock-issue/SKILL.md",
    ".codex/skills/aistock-merge-aftercare/SKILL.md",
    ".codex/skills/aistock-readonly-triage/SKILL.md",
    ".codex/skills/aistock-docs-handoff/SKILL.md",
    ".codex/skills/aistock-validation-delegation/SKILL.md",
    ".codex/skills/verify-aistock-feature/SKILL.md",
    ".claude/commands/aistock-task-router.md",
    ".claude/commands/fix-aistock-issue.md",
    ".claude/commands/aistock-merge-aftercare.md",
    ".claude/commands/aistock-readonly-triage.md",
    ".claude/commands/aistock-docs-handoff.md",
    ".claude/commands/aistock-validation-delegation.md",
    ".claude/commands/aistock-feature-workflow.md",
    "docs/codex_project_memory.md",
    "docs/standards/README.md",
    "docs/standards/aistock_development_standard_v1.5_20260523.md",
    "docs/standards/aistock_development_standard_v1.5_20260523.yaml",
)


def _workflow_rule_digests(root: Path | None = None) -> list[dict[str, Any]]:
    base = root or REPO_ROOT
    refs: list[dict[str, Any]] = []
    for rel in WORKFLOW_RULE_DIGEST_REFS:
        path = base / rel
        stat = path.stat() if path.exists() and path.is_file() else None
        digest = _sha256_file(path)
        refs.append(
            {
                "path": rel,
                "exists": bool(stat),
                "sha256_12": digest[:12] if digest else None,
                "bytes": stat.st_size if stat else 0,
                "mtime_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat().replace("+00:00", "Z")
                if stat
                else None,
            }
        )
    return refs


def _workflow_context_resume_digest(state: dict[str, Any], *, root: Path | None = None) -> dict[str, Any]:
    allowed = flow._as_list(state.get("allowed_write_scope"))
    required = flow._as_list(state.get("required_verification"))
    verification_budget = state.get("verification_budget") if isinstance(state.get("verification_budget"), dict) else None
    return {
        "schema_version": "aistock_workflow_context_resume_digest_v1",
        "rule_digests": _workflow_rule_digests(root),
        "reuse_policy": [
            "After context compaction, read this resume digest and task-card.md first.",
            "Do not re-read skills, project memory, standards README, quickstart, or RTK unless a listed hash changed, state is missing, or the user explicitly asks.",
            "Do not re-print the same source range; use precise rg and record a scoped miss reason before broad scans.",
        ],
        "allowed_write_scope_count": len(allowed),
        "required_verification_count": len(required),
        "verification_budget": verification_budget,
        "nightly_deferred_verification": verification_budget.get("deferred_nightly_verification") if verification_budget else None,
        "exploration_command_budget": {
            "soft_limit": 40,
            "action": "pause_and_summarize_before_more_search_or_repeated_file_reads",
        },
        "validation_loop_budget": {
            "failure_resume_first": ["pytest <path>::<test_name> -q", "pytest --lf -q", "pytest --ff -x -q"],
            "max_final_related_matrix_runs": 1,
            "delegate_when": [
                "local validation or exploration exceeds 30 minutes",
                "exploration commands exceed the soft limit",
                "broad module/cross-module/UI/API/business-flow coverage is needed",
                "a suite already passed and only non-behavioral edits followed",
            ],
            "rule": "do not rerun broad suites after each edit; rerun failed nodeids first and delegate deep validation",
        },
        "success_artifact_policy": {
            "stdout": "compact_by_default",
            "json": "diagnostic_only_on_failure_or_AISTOCK_WORKFLOW_ARTIFACTS=1",
            "required_runtime_state": ["state.json", "events.jsonl", "task-card.md", "context-pack.md", "pr-body.md"],
        },
    }


def _task_card_exists_payload(bug_id: str, root: Path) -> dict[str, Any]:
    json_path = _task_card_json_path(bug_id, root)
    md_path = _task_card_md_path(bug_id, root)
    return {
        "json": _size_and_token_estimate(json_path),
        "md": _size_and_token_estimate(md_path),
        "available": json_path.exists() and md_path.exists(),
        "expected_json": _repo_rel(json_path, root),
        "expected_md": _repo_rel(md_path, root),
    }


def _fallback_context_metrics(bug_id: str, root: Path) -> dict[str, Any]:
    workflow_dir = root / WORKFLOW_ROOT / bug_id
    metrics = {
        "context_pack_md": _size_and_token_estimate(workflow_dir / "context-pack.md"),
        "context_pack_json": _size_and_token_estimate(workflow_dir / "context-pack.json"),
        "fix_ready_json": _size_and_token_estimate(workflow_dir / "fix-ready.json"),
        "task_card_md": _size_and_token_estimate(_task_card_md_path(bug_id, root)),
        "task_card_json": _size_and_token_estimate(_task_card_json_path(bug_id, root)),
    }
    return {key: value for key, value in metrics.items() if value.get("exists")}


def _build_validation_receipts(
    evidence: Iterable[str],
    *,
    root: Path,
    changed_files: Iterable[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    commit = _git(["rev-parse", "HEAD"], cwd=root, check=False).strip() or "unknown"
    environment_identity = {
        "os": os.name,
        "platform": platform.system().lower(),
        "python": platform.python_version(),
        "executable": Path(sys.executable).name,
    }
    changed_files_digest = hashlib.sha256(
        "\n".join(sorted(str(item).replace("\\", "/") for item in changed_files or [])).encode("utf-8")
    ).hexdigest()
    receipts: list[dict[str, Any]] = []
    errors: list[str] = []
    for raw_item in evidence:
        item = str(raw_item or "").strip()
        if not item:
            continue
        if "->" not in item:
            errors.append(f"validation evidence must use '<command> -> <passed result>': {item}")
            continue
        command, result = (part.strip() for part in item.rsplit("->", 1))
        if not command or not result or VALIDATION_FAIL_RE.search(result) or not VALIDATION_PASS_RE.search(result):
            errors.append(f"validation result is not an explicit pass: {item}")
            continue
        evidence_kind = ""
        plan = ""
        for kind, pattern in VALIDATION_COMMAND_PATTERNS:
            match = pattern.search(command)
            if not match:
                continue
            evidence_kind = kind
            plan = str(match.groupdict().get("plan") or "")
            break
        if not evidence_kind:
            errors.append(f"validation command is not allowlisted: {command}")
            continue
        identity_inputs = {
            "commit": commit,
            "changed_files_digest": changed_files_digest,
            "command": command,
            "result": result,
            "evidence_kind": evidence_kind,
            "plan": plan or None,
            "environment": environment_identity,
        }
        normalized = json.dumps(identity_inputs, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        receipts.append(
            {
                "schema_version": VALIDATION_RECEIPT_SCHEMA,
                "receipt_id": hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16],
                "reuse_key": hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
                "commit": commit,
                "changed_files_digest": changed_files_digest,
                "environment_identity": environment_identity,
                "command": command,
                "result": result,
                "status": "passed",
                "evidence_kind": evidence_kind,
                "plan": plan or None,
                "recorded_at": _utc_now(),
            }
        )
    return receipts, errors


def _validation_receipt_plan_coverage(
    *,
    validation: dict[str, Any],
    receipts: list[dict[str, Any]],
) -> dict[str, Any]:
    required_plans = flow._unique_strings(validation.get("required_plans") or [])
    receipt_ids_by_plan: dict[str, list[str]] = {}
    for receipt in receipts:
        plan = str(receipt.get("plan") or "").strip()
        if not plan:
            continue
        receipt_ids_by_plan.setdefault(plan, []).append(str(receipt.get("receipt_id") or ""))
    observed_plans = list(receipt_ids_by_plan)
    missing_required_plans = [plan for plan in required_plans if plan not in receipt_ids_by_plan]
    duplicate_plan_receipts = {
        plan: receipt_ids
        for plan, receipt_ids in receipt_ids_by_plan.items()
        if len(receipt_ids) > 1
    }
    return {
        "schema_version": "aistock_validation_receipt_plan_coverage_v1",
        "required_plans": required_plans,
        "observed_plans": observed_plans,
        "missing_required_plans": missing_required_plans,
        "unexpected_plans": [plan for plan in observed_plans if plan not in required_plans],
        "duplicate_plan_receipts": duplicate_plan_receipts,
        "complete": not missing_required_plans,
    }


def _validation_receipt_plan_errors(coverage: dict[str, Any]) -> list[str]:
    missing = [str(plan) for plan in coverage.get("missing_required_plans") or []]
    duplicates = {
        str(plan): [str(receipt_id) for receipt_id in receipt_ids]
        for plan, receipt_ids in (coverage.get("duplicate_plan_receipts") or {}).items()
    }
    errors: list[str] = []
    if missing:
        errors.append(f"missing required validation plan receipts: {missing}")
    if duplicates:
        errors.append(f"duplicate validation plan receipts are not allowed: {duplicates}")
    return errors


def _render_validation_receipt(receipt: dict[str, Any]) -> str:
    command = str(receipt.get("command") or "").replace("`", "'")
    result = str(receipt.get("result") or "").replace("`", "'")
    return (
        "validation-receipt: "
        f"id={receipt.get('receipt_id')} "
        f"commit={receipt.get('commit')} "
        f"kind={receipt.get('evidence_kind')} "
        f"plan={receipt.get('plan') or 'direct'} "
        f"status={receipt.get('status')} "
        f"command=`{command}` result=`{result}`"
    )


def _validation_receipt_summary(evidence: list[str], deferred_plans: list[str] | None = None) -> dict[str, Any]:
    local_items: list[str] = []
    broad_items: list[str] = []
    for item in evidence:
        text = str(item or "").strip()
        if not text:
            continue
        lower = text.lower()
        plan_hits = [plan for plan in _known_plan_keys() if plan.lower() in lower]
        if any(_is_broad_validation_plan(plan) for plan in plan_hits) or any(
            token in lower for token in ("_l2", "_l3", "paper_v2_backend", "backend tests (")
        ):
            broad_items.append(text)
        else:
            local_items.append(text)
    return {
        "schema_version": "aistock_validation_receipt_summary_v1",
        "evidence_count": len([item for item in evidence if str(item or "").strip()]),
        "local_gate_evidence_count": len(local_items),
        "broad_premerge_evidence_count": len(broad_items),
        "broad_premerge_detected": bool(broad_items),
        "deferred_nightly_plans": flow._unique_strings(deferred_plans or []),
        "recommendation": "keep broad validation in nightly/VC unless explicitly required"
        if broad_items
        else "pre-merge evidence appears local/targeted",
    }


def _resolve_output_path(output: str | None) -> Path | None:
    if not output or output == "-":
        return None
    raw = output.strip()
    if not raw:
        return None
    if raw.lower() in OUTPUT_FORMAT_TOKENS:
        raise WorkflowError("--output expects a JSON file path; omit it or use --output - for stdout")

    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path

    roots = [REPO_ROOT.resolve()]
    canonical = _canonical_root().resolve()
    if canonical not in roots:
        roots.append(canonical)
    safe_roots = [root / safe_dir for root in roots for safe_dir in SAFE_OUTPUT_DIRS]
    in_safe_dir = any(_is_inside(path, safe_root) for safe_root in safe_roots)
    parent = path.parent.resolve()
    if not path.suffix and parent in roots:
        raise WorkflowError(
            "--output refuses a root-level bare file; use --output - or write under tmp/issue_workflow/"
        )
    if not path.suffix and not in_safe_dir:
        raise WorkflowError("--output path must include a file suffix or be under tmp/issue_workflow/ or tmp/validation/")
    return path


def _pick(payload: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: payload.get(key) for key in keys if key in payload}


def _compact_count(value: Any) -> int | None:
    if isinstance(value, (list, tuple, set, dict)):
        return len(value)
    return None


def _compact_check_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    summary: dict[str, Any] = {}
    for key in ("failed", "pending", "non_blocking", "passed"):
        items = value.get(key)
        count = _compact_count(items)
        if count is not None:
            summary[f"{key}_count"] = count
            if key in {"failed", "pending"} and items:
                summary[key] = list(items)[:5]
    return summary


def _compact_stale_pr_check(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return value if value is not None else None
    compact = _pick(value, "status")
    open_prs = value.get("open_prs")
    merged_prs = value.get("merged_prs")
    if isinstance(open_prs, list):
        compact["open_pr_count"] = len(open_prs)
    if isinstance(merged_prs, list):
        compact["merged_pr_count"] = len(merged_prs)
    return compact


def _compact_phase_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return _pick(
        value,
        "event_count",
        "known_duration_seconds",
        "inferred_elapsed_seconds",
        "code_repair_seconds",
        "total_estimated_tokens",
        "context_estimated_tokens",
        "artifact_estimated_tokens",
        "top_phase",
        "token_usage_status",
        "queue_seconds",
        "active_fix_seconds",
        "local_validation_seconds",
        "pr_ci_seconds",
        "merge_aftercare_seconds",
        "rtk_telemetry",
    )


def _compact_merge(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(value, "already_merged", "merge_commit", "recovered_from_local_merge_error")
    verified = value.get("verified")
    if isinstance(verified, dict):
        merge_commit = _merge_commit_from_pr_check(verified)
        if merge_commit:
            compact["merge_commit"] = merge_commit
        pr = verified.get("pr")
        if isinstance(pr, dict):
            compact["merged_at"] = pr.get("mergedAt")
            compact["pr_state"] = pr.get("state")
    if "check_summary" in value:
        compact["check_summary"] = _compact_check_summary(value["check_summary"])
    return compact


def _compact_pr_automation(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(value, "branch", "dry_run", "pr_url")
    if "pre_pr_gate" in value:
        compact["pre_pr_gate"] = _pick(value["pre_pr_gate"], "workflow_gate", "blocking", "warnings", "next_actions")
    actions = value.get("actions")
    if isinstance(actions, list):
        compact["actions_count"] = len(actions)
    if isinstance(value.get("ci_watch"), dict):
        compact["ci_watch"] = _pick(value["ci_watch"], "workflow_gate", "check_summary", "attempts", "next_actions")
    if value.get("next_commands"):
        compact["next_commands"] = value.get("next_commands")
    return compact


def _compact_timing_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return _pick(
        value,
        "event_count",
        "known_duration_seconds",
        "inferred_elapsed_seconds",
        "queue_seconds",
        "active_fix_seconds",
        "local_validation_seconds",
        "pr_ci_seconds",
        "merge_aftercare_seconds",
        "code_repair_seconds",
        "started_at",
        "ended_at",
    )


def _compact_start(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(
        value,
        "bug_id",
        "module",
        "status",
        "workflow_gate",
        "source_bug_json",
        "context_pack_md",
        "fix_ready_path",
        "task_card_json",
        "task_card_md",
        "state_path",
        "events_path",
        "github_issue_url",
    )
    worktree_plan = value.get("worktree_plan")
    if isinstance(worktree_plan, dict):
        compact["worktree_plan"] = _pick(worktree_plan, "branch", "worktree", "created", "dry_run")
    if "required_verification" in value:
        compact["required_verification"] = value.get("required_verification")
    if "production_gates" in value:
        compact["production_gates"] = value.get("production_gates")
    return compact


def _compact_finish(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(
        value,
        "bug_id",
        "workflow_gate",
        "closure_ready",
        "draft_ready",
        "changed_files",
        "required_verification",
        "recommended_verification",
        "production_gates",
        "pr_body_path",
        "state_path",
        "events_path",
        "artifact_policy",
        "backend_restart",
        "post_restart_effective_gate",
        "runtime_identity_match",
        "next_user_action",
        "error",
    )
    if "validation_evidence" in value:
        compact["validation_evidence_count"] = len(value.get("validation_evidence") or [])
    if "validation_receipts" in value:
        compact["validation_receipts"] = [
            _pick(
                receipt,
                "schema_version",
                "receipt_id",
                "commit",
                "command",
                "result",
                "status",
                "evidence_kind",
                "plan",
            )
            for receipt in value.get("validation_receipts") or []
            if isinstance(receipt, dict)
        ]
    if value.get("validation_evidence_errors"):
        compact["validation_evidence_errors"] = value.get("validation_evidence_errors")
    if "scope_check" in value:
        compact["scope_check"] = _pick(value["scope_check"], "status", "violations", "status_source")
    if "code_intelligence" in value:
        compact["code_intelligence"] = _pick(
            value["code_intelligence"],
            "status",
            "context_ref",
            "affected_tests_ref",
            "affected_tests_count",
            "understand_anything_summary_ref",
            "fallback_used",
        )
    if "pre_pr_gate" in value:
        compact["pre_pr_gate"] = _pick(value["pre_pr_gate"], "workflow_gate", "blocking")
    return compact


def _compact_postmortem(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(
        value,
        "bug_id",
        "workflow_root",
        "duplicate_active_count",
        "production_gates",
        "postmortem_md_path",
        "postmortem_json_path",
        "waiting_for_user_restart_minutes",
        "backend_restart_owner",
        "tool_telemetry_policy",
    )
    if isinstance(value.get("state"), dict):
        compact["state"] = _pick(value["state"], "state", "branch", "worktree", "pr_url", "commit", "next_actions")
    timing_summary = _compact_timing_summary(value.get("timing_summary"))
    if timing_summary:
        compact["timing_summary"] = timing_summary
    flow_summary = _compact_phase_summary(value.get("flow_overhead_estimate"))
    if flow_summary:
        compact["flow_overhead_estimate"] = flow_summary
    if isinstance(value.get("phase_cost_table"), list) and value["phase_cost_table"]:
        compact["top_phase"] = value["phase_cost_table"][0]
    if isinstance(value.get("h6_summary"), dict):
        compact["h6_summary"] = _compact_phase_summary(value["h6_summary"])
    if isinstance(value.get("h7_code_intelligence"), dict):
        compact["h7_code_intelligence"] = _pick(
            value["h7_code_intelligence"],
            "status",
            "codegraph_status",
            "codegraph_freshness",
            "codegraph_freshness_ref",
            "fallback_used",
            "readiness_next_command",
            "fallback_reason",
        )
    active = value.get("active_workflows")
    if isinstance(active, list):
        compact["active_workflow_count"] = len(active)
    stale = value.get("stale_pr_check")
    stale_compact = _compact_stale_pr_check(stale)
    if stale_compact is not None:
        compact["stale_pr_check"] = stale_compact
    return compact


def _compact_finalizer(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(value, "workflow_gate", "blocking", "bug_ids", "batch_mode", "source_merge_commit", "next_actions")
    if "close_sync" in value:
        compact["close_sync"] = _pick(value["close_sync"], "workflow_gate", "registry_root", "updated_bug_json", "merge_commit")
    if "close_sync_commit" in value:
        compact["close_sync_commit"] = _pick(value["close_sync_commit"], "workflow_gate", "branch", "pr_url", "commit", "next_command")
    if "close_sync_pr_merge" in value:
        compact["close_sync_pr_merge"] = _pick(value["close_sync_pr_merge"], "workflow_gate", "merge_commit", "blocking")
    if "cleanup" in value and isinstance(value["cleanup"], dict):
        compact["cleanup"] = _pick(value["cleanup"], "workflow_gate", "branch", "worktree", "sync_root", "blocking", "warnings")
    if "close_sync_cleanup" in value and isinstance(value["close_sync_cleanup"], dict):
        compact["close_sync_cleanup"] = _pick(
            value["close_sync_cleanup"],
            "workflow_gate",
            "branch",
            "worktree",
            "sync_root",
            "blocking",
            "warnings",
        )
    if "postmortem" in value:
        postmortem = _compact_postmortem(value["postmortem"])
        if postmortem:
            compact["postmortem"] = _pick(
                postmortem,
                "bug_id",
                "timing_summary",
                "flow_overhead_estimate",
                "top_phase",
                "h6_summary",
                "stale_pr_check",
                "production_gates",
                "postmortem_md_path",
                "postmortem_json_path",
            )
    return compact


def _compact_triage_ci_issue(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(
        value,
        "detected_run_id",
        "classification_recommendation",
        "needs_bug_json",
        "failure_event_path",
        "context_pack_md_path",
        "triage_action",
    )
    issue = value.get("github_issue")
    if isinstance(issue, dict):
        compact["github_issue"] = _pick(issue, "number", "url", "title", "state")
    summary = value.get("summary")
    if isinstance(summary, dict):
        compact["diagnostic_status"] = summary.get("diagnostic_status")
        compact["workflow"] = summary.get("workflow")
        compact["run_url"] = summary.get("run_url")
        failed_jobs = summary.get("failed_jobs")
        if isinstance(failed_jobs, list):
            compact["failed_job_count"] = len(failed_jobs)
        modules = summary.get("suspected_modules")
        if modules:
            compact["suspected_modules"] = modules[:5] if isinstance(modules, list) else modules
        files = summary.get("suspected_files")
        if files:
            compact["suspected_files_count"] = len(files) if isinstance(files, list) else None
        if summary.get("reproduce_command"):
            compact["reproduce_command"] = summary.get("reproduce_command")
    linked = value.get("linked_bug")
    if isinstance(linked, dict) and linked:
        compact["linked_bug"] = _pick(linked, "bug_id", "path")
    suggested = value.get("suggested_bug")
    if isinstance(suggested, dict) and value.get("needs_bug_json"):
        compact["suggested_bug"] = _pick(suggested, "module", "severity", "title", "risk_area")
        required = suggested.get("required_verification")
        if isinstance(required, list):
            compact["suggested_bug"]["required_verification_count"] = len(required)
        scope = suggested.get("allowed_write_scope")
        if isinstance(scope, list):
            compact["suggested_bug"]["allowed_write_scope_count"] = len(scope)
    infra = value.get("infra_action")
    if isinstance(infra, dict):
        compact["infra_action"] = _pick(infra, "workflow_gate", "reason", "production_gates")
        actions = infra.get("next_actions")
        if isinstance(actions, list):
            compact["infra_action"]["next_actions"] = actions[:4]
    superseded = value.get("superseded_action")
    if isinstance(superseded, dict):
        compact["superseded_action"] = _pick(superseded, "workflow_gate", "reason", "next_command", "production_gates")
        run = superseded.get("superseding_run")
        if isinstance(run, dict):
            compact["superseded_action"]["superseding_run"] = _pick(run, "run_id", "run_url", "head_sha", "created_at")
    return compact


def _compact_promote_ci_issue(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(value, "dry_run")
    triage = _compact_triage_ci_issue(value.get("triage"))
    if triage:
        compact["triage"] = triage
    if isinstance(value.get("infra_action"), dict):
        compact["infra_action"] = _pick(value["infra_action"], "workflow_gate", "reason")
    if isinstance(value.get("superseded_action"), dict):
        compact["superseded_action"] = _pick(value["superseded_action"], "workflow_gate", "reason", "next_command")
    if "triage_action" in value:
        compact["triage_action"] = value.get("triage_action")
    if isinstance(value.get("submit_bug"), dict):
        submit_bug = value["submit_bug"]
        compact["submit_bug"] = _pick(submit_bug, "workflow_gate", "bug_id", "state_path", "events_path", "next_command")
        if isinstance(submit_bug.get("fix_chain"), dict):
            compact["submit_bug"]["fix_chain"] = _pick(
                submit_bug["fix_chain"],
                "continue_to_fix_in_same_workflow",
                "registry_pr_only",
                "next_command",
            )
    return compact


def _compact_promote_nightly_candidate(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(
        value,
        "dry_run",
        "candidate_id",
        "candidate_confidence",
        "candidate_module",
        "candidate_severity",
        "github_issue_number",
    )
    if isinstance(value.get("dedupe"), dict):
        compact["dedupe"] = _pick(value["dedupe"], "fingerprint", "issue_already_exists")
    if isinstance(value.get("quality_gate"), dict):
        compact["quality_gate"] = _pick(value["quality_gate"], "workflow_gate", "issue_payload_ready", "auto_submit_allowed", "reasons")
    if isinstance(value.get("submit_bug"), dict):
        submit_bug = value["submit_bug"]
        compact["submit_bug"] = _pick(submit_bug, "workflow_gate", "bug_id", "state_path", "events_path", "next_command")
        if isinstance(submit_bug.get("github"), dict):
            compact["submit_bug"]["github"] = _pick(submit_bug["github"], "created", "number", "url")
    return compact


def _compact_ci_issue_janitor(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    compact = _pick(
        value,
        "workflow_gate",
        "dry_run",
        "scanned_count",
        "superseded_count",
        "infra_count",
        "closed_count",
        "skipped_count",
        "failed_count",
        "production_gates",
        "next_command",
    )
    if value.get("closed_issues"):
        compact["closed_issues"] = value.get("closed_issues")
    if value.get("failed_issues"):
        compact["failed_issues"] = value.get("failed_issues")
    return compact


def _compact_code_intelligence_client_verification(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    codegraph = value.get("codegraph") if isinstance(value.get("codegraph"), dict) else {}
    freshness = value.get("freshness") if isinstance(value.get("freshness"), dict) else {}
    ua = value.get("understand_anything") if isinstance(value.get("understand_anything"), dict) else {}
    clients = value.get("clients") if isinstance(value.get("clients"), dict) else {}
    artifacts = value.get("artifacts") if isinstance(value.get("artifacts"), dict) else {}
    ready_clients = sum(1 for item in clients.values() if isinstance(item, dict) and item.get("status") == "ready")
    return {
        "codegraph_status": codegraph.get("status"),
        "effective_freshness": freshness.get("effective_freshness"),
        "latest_artifact_ref": freshness.get("latest_artifact_ref"),
        "understand_anything_status": ua.get("status"),
        "understand_anything_freshness": ua.get("freshness"),
        "clients_ready": f"{ready_clients}/{len(clients)}",
        "context_ref": artifacts.get("context_ref"),
        "affected_tests_ref": artifacts.get("affected_tests_ref"),
        "ua_summary_ref": artifacts.get("ua_summary_ref"),
        "artifact_path": value.get("artifact_path"),
        "next_actions": (value.get("efficiency") or {}).get("next_actions")
        if isinstance(value.get("efficiency"), dict)
        else [],
    }


def _compact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    schema = str(payload.get("schema_version") or "")
    if not schema:
        return payload
    compact = _pick(payload, "schema_version", "workflow_gate", "bug_id", "mode", "next_command")
    if payload.get("blocking"):
        compact["blocking"] = payload.get("blocking")
    if payload.get("warnings"):
        compact["warnings_count"] = len(payload.get("warnings") or [])
    for key in (
        "backend_restart",
        "post_restart_effective_gate",
        "runtime_identity_match",
        "next_user_action",
        "target_id",
        "operator_runbook_ref",
        "process_control_performed",
        "receipt_path",
    ):
        if key in payload:
            compact[key] = payload.get(key)
    if "ui_intake_hints" in payload and isinstance(payload.get("ui_intake_hints"), dict):
        compact["ui_intake_hints"] = _pick(
            payload["ui_intake_hints"],
            "ui_issue",
            "ui_route",
            "scope_source",
            "reproduce_required",
            "visual_acceptance_required",
        )
        compact["ui_intake_hints"]["scope_count"] = len(payload["ui_intake_hints"].get("ui_component_scope") or [])
        compact["ui_intake_hints"]["recommended_verification_count"] = len(payload["ui_intake_hints"].get("recommended_verification") or [])
    if "workflow_efficiency_recommendations" in payload and isinstance(payload.get("workflow_efficiency_recommendations"), dict):
        compact["workflow_efficiency_recommendations"] = _pick(
            payload["workflow_efficiency_recommendations"],
            "batch_candidate",
            "docs_only_merge_with_related_code",
            "compact_success_output",
            "full_json_on_failure_only",
        )

    if schema.endswith("_run_v1"):
        compact.update(_pick(payload, "mode"))
        if "start" in payload:
            compact["start"] = _compact_start(payload["start"])
        if "finish" in payload:
            compact["finish"] = _compact_finish(payload["finish"])
        if "pr_automation" in payload:
            compact["pr_automation"] = _compact_pr_automation(payload["pr_automation"])
        if "merge" in payload:
            compact["merge"] = _compact_merge(payload["merge"])
        if "finalizer" in payload:
            compact["finalizer"] = _compact_finalizer(payload["finalizer"])
        if "close_sync" in payload and "finalizer" not in payload:
            compact["close_sync"] = _pick(payload["close_sync"], "workflow_gate", "registry_root", "updated_bug_json", "merge_commit")
        if "close_sync_commit" in payload and "finalizer" not in payload:
            compact["close_sync_commit"] = _pick(payload["close_sync_commit"], "workflow_gate", "branch", "pr_url", "commit")
        if "cleanup" in payload and isinstance(payload["cleanup"], dict):
            compact["cleanup"] = _pick(payload["cleanup"], "workflow_gate", "branch", "worktree", "sync_root", "blocking", "warnings")
        if "active_decision" in payload:
            compact["active_decision"] = _pick(payload["active_decision"], "decision", "workflow_gate", "next_command", "blocking", "warnings")
    elif schema.endswith("_doctor_v1"):
        compact.update(_pick(payload, "restart_recommended", "install_client_next_command"))
        if "client_manifest" in payload:
            compact["client_manifest"] = _pick(
                payload["client_manifest"],
                "codex_skill_status",
                "codex_feature_skill_status",
                "codex_router_skill_status",
                "codex_docs_handoff_skill_status",
                "codex_merge_aftercare_skill_status",
                "codex_readonly_triage_skill_status",
                "codex_validation_delegation_skill_status",
                "claude_command_status",
                "claude_feature_command_status",
                "claude_router_command_status",
                "claude_docs_handoff_command_status",
                "claude_merge_aftercare_command_status",
                "claude_readonly_triage_command_status",
                "claude_validation_delegation_command_status",
                "restart_recommended",
            )
        if "h7_code_intelligence" in payload:
            compact["h7_code_intelligence"] = _pick(
                payload["h7_code_intelligence"],
                "workflow_gate",
                "codegraph_status",
                "codegraph_freshness",
                "codegraph_freshness_ref",
                "understand_anything_status",
                "understand_anything_graph_exists",
                "understand_anything_next_command",
                "fallback_used",
                "readiness_next_command",
                "fallback_reason",
            )
        if "bug_id_allocation" in payload:
            compact["bug_id_allocation"] = _pick(
                payload["bug_id_allocation"],
                "next_number",
                "allocator_max_number",
                "observed_max_number",
                "github_max_number",
                "github_scanned",
            )
        if "validation_center_runtime_safety" in payload:
            compact["validation_center_runtime_safety"] = _pick(
                payload["validation_center_runtime_safety"],
                "workflow_gate",
                "safe_app_module",
                "unsafe_app_module",
                "safe_command",
                "allowed_backend_ports",
                "production_ports_forbidden",
            )
        if "worktree_hygiene" in payload:
            compact["worktree_hygiene"] = _pick(
                payload["worktree_hygiene"],
                "workflow_gate",
                "canonical_branch",
            )
            compact["worktree_hygiene"]["noncanonical_main_worktree_count"] = len(
                payload["worktree_hygiene"].get("noncanonical_main_worktrees") or []
            )
        if "cleanup_janitor" in payload:
            compact["cleanup_janitor"] = _pick(
                payload["cleanup_janitor"],
                "workflow_gate",
                "safe_merged_local_branch_count",
                "stale_backup_or_temp_branch_count",
                "checked_out_merged_branch_count",
                "safe_merged_local_branch_samples",
                "stale_backup_or_temp_branch_samples",
                "checked_out_merged_branch_samples",
                "next_command",
            )
    elif schema.endswith("_start_v1"):
        compact.update(_compact_start(payload) or {})
    elif schema.endswith("_finish_v1") or schema.endswith("_finish_batch_v1"):
        compact.update(_compact_finish(payload) or {})
        if "batch_id" in payload:
            compact["batch_id"] = payload.get("batch_id")
            compact["bug_ids"] = payload.get("bug_ids")
    elif schema.endswith("_resume_v1"):
        compact.update(
            _pick(
                payload,
                "workflow_root",
                "worktree",
                "planned_worktree",
                "branch",
                "planned_branch",
                "task_card_json",
                "task_card_md",
                "state_path",
                "events_path",
                "context_resume_digest",
                "stop_conditions",
            )
        )
    elif schema == "aistock_nightly_intake_smoke_v1":
        compact.update(
            _pick(
                payload,
                "dry_run",
                "github_writes",
                "candidate_history_path",
                "nightly_failed_stages",
                "unexpected_dirty_paths",
                "production_gates",
            )
        )
        if isinstance(payload.get("artifacts"), dict):
            compact["artifacts"] = _pick(payload["artifacts"], "summary", "context", "github_issue_payload")
    elif schema == "aistock_batch_workflow_smoke_v1":
        compact.update(
            _pick(
                payload,
                "dry_run",
                "github_writes",
                "batch_id",
                "bug_ids",
                "unexpected_dirty_paths",
                "production_gates",
            )
        )
        if isinstance(payload.get("artifacts"), dict):
            compact["artifacts"] = _pick(payload["artifacts"], "batch_state", "finish_plan", "pr_body")
        if isinstance(payload.get("finish"), dict):
            compact["finish"] = _pick(payload["finish"], "workflow_gate", "closure_ready", "validation_evidence_count")
    elif schema.endswith("_smoke_v1"):
        compact.update(
            _pick(
                payload,
                "dry_run",
                "unexpected_dirty_paths",
                "production_gates",
            )
        )
        compact["changed_files_count"] = len(payload.get("changed_files") or [])
        if isinstance(payload.get("fast_path"), dict):
            compact["fast_path"] = _pick(payload["fast_path"], "task_tier", "module", "workflow_gate")
        if "postmortem_preview" in payload:
            preview = _compact_postmortem(payload["postmortem_preview"])
            if preview:
                compact["postmortem_preview"] = _pick(preview, "bug_id", "timing_summary", "stale_pr_check")
    elif schema.endswith("_postmortem_v1"):
        compact.update(_compact_postmortem(payload) or {})
    elif (
        schema.endswith("_close_sync_v1")
        or schema.endswith("_close_sync_batch_v1")
        or schema.endswith("_close_sync_aggregate_v1")
    ):
        compact.update(
            _pick(
                payload,
                "bug_ids",
                "source_bug_json",
                "registry_root",
                "current_status",
                "github_issue_url",
                "merged_pr",
                "merge_commit",
                "production_gates",
                "dry_run",
                "updated_bug_json",
                "updated_bug_jsons",
                "timing_summary",
            )
        )
        compact["validation_evidence_count"] = len(payload.get("validation_evidence") or [])
        if "github_issue_sync" in payload:
            compact["github_issue_sync"] = _pick(payload["github_issue_sync"], "status", "channel", "fallback_used")
    elif schema == CLEANUP_BATCH_RESULT_SCHEMA:
        compact.update(
            _pick(
                payload,
                "manifest_sha256",
                "target_count",
                "completed_count",
                "success_count",
                "failed_count",
                "sync_root",
                "dry_run",
                "duration_seconds",
            )
        )
        compact["blocking_count"] = len(payload.get("blocking") or [])
        compact["blocking_samples"] = list(payload.get("blocking") or [])[:5]
    elif schema.endswith("_cleanup_v1"):
        compact.update(
            _pick(
                payload,
                "branch",
                "worktree",
                "canonical_root",
                "sync_root",
                "merged_into_origin_main",
                "worktree_clean",
                "dry_run",
                "duration_seconds",
                "blocking",
                "warnings",
            )
        )
        if "actions" in payload:
            compact["actions_count"] = len(payload.get("actions") or [])
        if "applied" in payload:
            compact["applied_count"] = len(payload.get("applied") or [])
        if "complete_state" in payload:
            compact["complete_state"] = _pick(payload["complete_state"], "state", "updated_at")
    elif schema.endswith("_merge_finalizer_v1"):
        compact.update(_compact_finalizer(payload) or {})
    elif schema.endswith("_triage_ci_issue_v1"):
        compact.update(_compact_triage_ci_issue(payload) or {})
    elif schema.endswith("_ci_issue_janitor_v1"):
        compact.update(_compact_ci_issue_janitor(payload) or {})
    elif schema.endswith("_promote_ci_issue_v1"):
        compact.update(_compact_promote_ci_issue(payload) or {})
    elif schema.endswith("_promote_nightly_candidate_v1"):
        compact.update(_compact_promote_nightly_candidate(payload) or {})
    elif schema == "aistock_code_intelligence_client_verification_v1":
        compact.update(_compact_code_intelligence_client_verification(payload))
    elif schema == "aistock_workflow_client_verification_v1":
        compact.update(
            _pick(
                payload,
                "selected_lane",
                "selected_lane_keys",
                "blocking",
                "warnings",
                "checkout_advisories",
                "remediation",
                "restart_recommended",
            )
        )
    elif schema == "aistock_issue_workflow_client_install_v2":
        compact.update(
            _pick(
                payload,
                "selected_lane",
                "selected_lane_keys",
                "installed_count",
                "skipped_current_count",
                "blocking",
            )
        )
    elif schema.endswith("_watch_ci_v1") or schema.endswith("_check_watch_v1"):
        compact.update(_pick(payload, "pr_url", "state", "check_summary", "next_actions"))
    elif schema.endswith("_missing_bug_record_v1"):
        compact.update(
            _pick(
                payload,
                "reason",
                "github_issue",
                "inferred_module",
                "inferred_severity",
                "blocking",
                "warnings",
                "next_actions",
                "next_command",
            )
        )
    else:
        for key in (
            "module",
            "status",
            "task_tier",
            "required_verification",
            "recommended_verification",
            "production_gates",
            "pr_url",
            "branch",
            "worktree",
            "state_path",
            "events_path",
        ):
            if key in payload:
                compact[key] = payload[key]
        if "code_intelligence_hint" in payload and isinstance(payload["code_intelligence_hint"], dict):
            compact["code_intelligence_hint"] = _pick(
                payload["code_intelligence_hint"],
                "workflow_gate",
                "latest_freshness",
                "artifact_path",
                "consume_command",
                "readiness_next_command",
                "blocking_for_issue_workflow",
            )

    compact["full_payload"] = "use --output-format full-json or --output <tmp/issue_workflow/...json> for details"
    return compact


def _short_status_word(gate: str | None) -> str:
    value = (gate or "").lower()
    if value in {"passed", "ready", "ready_for_pr", "ready_for_apply", "checks_passed", "planned", "promoted", "complete", "merged", "merged_close_synced"}:
        return "PASS"
    if "blocked" in value or value in {"failed", "checks_failed", "validation_evidence_missing"}:
        return "BLOCKED"
    if "warning" in value or "pending" in value:
        return "WAIT"
    return "INFO"


def _format_summary_lines(payload: dict[str, Any], compact: dict[str, Any]) -> list[str]:
    schema = str(payload.get("schema_version") or compact.get("schema_version") or "")
    gate = str(compact.get("workflow_gate") or payload.get("workflow_gate") or "unknown")
    bug_id = str(compact.get("bug_id") or payload.get("bug_id") or "").strip()
    prefix = f"{_short_status_word(gate)} {bug_id}".strip()
    if schema == "aistock_workflow_client_verification_v1":
        return [
            (
                f"{prefix} client-sync workflow_gate={gate} lane={compact.get('selected_lane') or 'all'} "
                f"blocking={len(compact.get('blocking') or [])} warnings={len(compact.get('warnings') or [])} "
                f"restart_recommended={str(bool(compact.get('restart_recommended'))).lower()} "
                f"action={(compact.get('remediation') or {}).get('action') or 'unknown'}"
            )
        ]
    if schema == "aistock_issue_workflow_client_install_v2":
        return [
            (
                f"{prefix} client-install workflow_gate={gate} lane={compact.get('selected_lane') or 'all'} "
                f"installed={compact.get('installed_count', 0)} skipped_current={compact.get('skipped_current_count', 0)}"
            )
        ]
    if schema == "aistock_issue_workflow_watch_ci_v1":
        checks = compact.get("check_summary") if isinstance(compact.get("check_summary"), dict) else payload.get("check_summary") or {}
        return [
            (
                f"{prefix} workflow_gate={gate} pr={compact.get('pr_url') or 'unknown'} "
                f"passed={checks.get('passed_count', 0)} pending={checks.get('pending_count', 0)} "
                f"failed={checks.get('failed_count', 0)} next={';'.join(payload.get('next_actions') or compact.get('next_actions') or []) or 'none'}"
            )
        ]
    if schema == "aistock_nightly_intake_smoke_v1":
        return [
            (
                f"{prefix} nightly-intake-smoke workflow_gate={gate} github_writes={str(compact.get('github_writes')).lower()} "
                f"unexpected_dirty_paths={len(compact.get('unexpected_dirty_paths') or [])} "
                f"next={compact.get('next_command') or 'none'}"
            )
        ]
    if schema.endswith("_triage_ci_issue_v1"):
        return [
            (
                f"{prefix} triage-ci-issue workflow_gate={gate} classification={compact.get('classification_recommendation') or 'unknown'} "
                f"needs_bug_json={str(bool(compact.get('needs_bug_json'))).lower()} next={compact.get('next_command') or compact.get('triage_action') or 'none'}"
            )
        ]
    if schema.endswith("_promote_ci_issue_v1"):
        triage = compact.get("triage") if isinstance(compact.get("triage"), dict) else {}
        return [
            (
                f"{prefix} promote-ci-issue workflow_gate={gate} classification={triage.get('classification_recommendation') or 'unknown'} "
                f"next={compact.get('next_command') or compact.get('triage_action') or 'none'}"
            )
        ]
    if schema.endswith("_promote_nightly_candidate_v1"):
        submit = compact.get("submit_bug") if isinstance(compact.get("submit_bug"), dict) else {}
        return [
            (
                f"{prefix} promote-nightly-candidate workflow_gate={gate} candidate={compact.get('candidate_id') or 'unknown'} "
                f"bug={submit.get('bug_id') or 'not_created'} issue={compact.get('github_issue_number') or 'not_created'} "
                f"next={compact.get('next_command') or submit.get('next_command') or 'none'}"
            )
        ]
    if schema.endswith("_start_v1"):
        worktree_plan = compact.get("worktree_plan") if isinstance(compact.get("worktree_plan"), dict) else {}
        return [
            (
                f"{prefix} workflow_gate={gate} module={compact.get('module') or 'unknown'} "
                f"worktree_created={str(bool(worktree_plan.get('created'))).lower()} "
                f"context={compact.get('context_pack_md') or 'not_generated'}"
            )
        ]
    if schema.endswith("_finish_v1") or schema.endswith("_finish_batch_v1"):
        return [
            (
                f"{prefix} workflow_gate={gate} closure_ready={str(bool(compact.get('closure_ready'))).lower()} "
                f"validation_evidence={compact.get('validation_evidence_count', 0)} pr_body={compact.get('pr_body_path') or 'not_generated'}"
            )
        ]
    if schema.endswith("_doctor_v1"):
        return [
            (
                f"{prefix} doctor workflow_gate={gate} restart_recommended={str(bool(compact.get('restart_recommended'))).lower()} "
                f"warnings={compact.get('warnings_count', 0)} next={compact.get('next_command') or 'none'}"
            )
        ]
    if schema.endswith("_smoke_v1") or schema == "aistock_batch_workflow_smoke_v1":
        return [
            (
                f"{prefix} workflow_gate={gate} dry_run={str(bool(compact.get('dry_run'))).lower()} "
                f"unexpected_dirty_paths={len(compact.get('unexpected_dirty_paths') or [])} next={compact.get('next_command') or 'none'}"
            )
        ]
    if schema.endswith("_run_v1"):
        return [
            (
                f"{prefix} workflow_gate={gate} mode={compact.get('mode') or payload.get('mode') or 'unknown'} "
                f"next={compact.get('next_command') or 'none'}"
            )
        ]
    if schema == "aistock_code_intelligence_client_verification_v1":
        next_actions = ";".join(compact.get("next_actions") or []) or "none"
        return [
            (
                f"{prefix} verify-clients workflow_gate={gate} "
                f"codegraph={compact.get('codegraph_status') or 'unknown'} "
                f"effective={compact.get('effective_freshness') or 'unknown'} "
                f"ua={compact.get('understand_anything_status') or 'unknown'} "
                f"clients_ready={compact.get('clients_ready') or 'unknown'} "
                f"next={next_actions}"
            )
        ]
    return [
        (
            f"{prefix} workflow_gate={gate} "
            f"next={compact.get('next_command') or 'none'}"
        )
    ]


def _emit(payload: dict[str, Any], output: str | None = None, output_format: str = "compact") -> None:
    output_path = _resolve_output_path(output)
    if output_path:
        _write_json(output_path, payload)
    if output_format == "full-json":
        sys.stdout.write(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
        return
    compact = _compact_payload(payload)
    if output_format == "summary":
        sys.stdout.write("\n".join(_format_summary_lines(payload, compact)) + "\n")
        return
    sys.stdout.write(json.dumps(compact, ensure_ascii=True, indent=2, sort_keys=True) + "\n")


def _emit_args(payload: dict[str, Any], args: argparse.Namespace) -> None:
    _emit(payload, getattr(args, "output", None), getattr(args, "output_format", "compact"))


def _repo_rel(path: Path, root: Path | None = None) -> str:
    root = root or REPO_ROOT
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except Exception:
        return str(path).replace("\\", "/")


def _load_runtime_target_catalog(root: Path | None = None) -> dict[str, Any]:
    root = root or REPO_ROOT
    path = root / "docs" / "standards" / "aistock_runtime_targets_v1.yaml"
    if not path.exists():
        raise WorkflowError(f"runtime target catalog is missing: {_repo_rel(path, root)}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise WorkflowError("runtime target catalog must be a mapping")
    if payload.get("schema_version") != "aistock_runtime_target_catalog_v1":
        raise WorkflowError("runtime target catalog schema_version must be aistock_runtime_target_catalog_v1")
    targets = payload.get("targets")
    if not isinstance(targets, dict) or not targets:
        raise WorkflowError("runtime target catalog targets must be a non-empty mapping")
    def validate_worker_local_probe(owner: str, local_probe: Any) -> None:
        if not isinstance(local_probe, dict):
            raise WorkflowError(f"{owner} local_probe must be a mapping")
        profile_path = str(local_probe.get("profile_path") or "").strip()
        profile_candidate = Path(profile_path)
        if (
            not profile_path
            or profile_candidate.is_absolute()
            or "\\" in profile_path
            or ".." in profile_candidate.parts
            or not profile_path.startswith("configs/datasets/")
            or not (root / profile_candidate).is_file()
        ):
            raise WorkflowError(
                f"{owner} local_probe profile_path must be an existing repository-relative configs/datasets file"
            )
        max_files = local_probe.get("max_files", 200)
        if type(max_files) is not int or not 0 < max_files <= 200:
            raise WorkflowError(f"{owner} local_probe max_files must be in 1..200")

    seen_ports: dict[int, str] = {}
    for target_id, target in targets.items():
        if not isinstance(target, dict):
            raise WorkflowError(f"runtime target {target_id} must be a mapping")
        for field in ("runtime_kind", "source_globs", "operator_runbook_ref", "expected_identity_ref", "probes"):
            if not target.get(field):
                raise WorkflowError(f"runtime target {target_id} is missing {field}")
        probe_mode = str(target.get("probe_mode") or "http").strip()
        if probe_mode == "http":
            if not target.get("probe_origins"):
                raise WorkflowError(f"runtime target {target_id} is missing probe_origins")
        elif probe_mode == _DATASET_RELEASE_WORKER_HEARTBEAT_MODE:
            if target.get("runtime_kind") != "worker_scheduler":
                raise WorkflowError(
                    f"runtime target {target_id} dataset-release heartbeat mode requires worker_scheduler"
                )
            validate_worker_local_probe(f"runtime target {target_id}", target.get("local_probe"))
        else:
            raise WorkflowError(f"runtime target {target_id} has unsupported probe_mode: {probe_mode}")
        probe_routes = target.get("probe_routes", [])
        if not isinstance(probe_routes, list):
            raise WorkflowError(f"runtime target {target_id} probe_routes must be a list")
        route_ids: set[str] = set()
        for route in probe_routes:
            if not isinstance(route, dict):
                raise WorkflowError(f"runtime target {target_id} probe route must be a mapping")
            route_id = str(route.get("route_id") or "").strip()
            route_globs = route.get("source_globs")
            if not route_id or route_id in route_ids:
                raise WorkflowError(f"runtime target {target_id} probe route_id is missing or duplicated")
            route_ids.add(route_id)
            if not isinstance(route_globs, list) or not route_globs or any(
                not isinstance(item, str) or not item.strip() for item in route_globs
            ):
                raise WorkflowError(f"runtime target {target_id} probe route {route_id} source_globs are invalid")
            if not set(route_globs).issubset(set(flow._as_list(target.get("source_globs")))):
                raise WorkflowError(
                    f"runtime target {target_id} probe route {route_id} contains sources outside its target"
                )
            if route.get("probe_mode") != _DATASET_RELEASE_WORKER_HEARTBEAT_MODE:
                raise WorkflowError(f"runtime target {target_id} probe route {route_id} mode is unsupported")
            if route.get("probes") != _DATASET_RELEASE_WORKER_HEARTBEAT_REFS:
                raise WorkflowError(
                    f"runtime target {target_id} probe route {route_id} does not use canonical heartbeat probes"
                )
            validate_worker_local_probe(
                f"runtime target {target_id} probe route {route_id}",
                route.get("local_probe"),
            )
        port = target.get("production_port")
        if port is not None:
            port = int(port)
            if port in seen_ports:
                raise WorkflowError(f"runtime target production_port conflict: {port} ({seen_ports[port]}, {target_id})")
            seen_ports[port] = str(target_id)
    raw_non_runtime_paths = payload.get("non_runtime_source_paths", [])
    if not isinstance(raw_non_runtime_paths, list):
        raise WorkflowError("runtime target catalog non_runtime_source_paths must be a list")
    non_runtime_paths: list[str] = []
    non_runtime_path_keys: set[str] = set()
    root_resolved = root.resolve()
    for raw_path in raw_non_runtime_paths:
        if not isinstance(raw_path, str):
            raise WorkflowError("runtime target catalog non_runtime_source_paths entries must be strings")
        path_value = raw_path.strip()
        if (
            not path_value
            or "\\" in path_value
            or path_value.startswith(("/", "./"))
            or re.match(r"^[A-Za-z]:(?:/|$)", path_value)
            or any(part in {"", ".", ".."} for part in path_value.split("/"))
        ):
            raise WorkflowError(
                f"runtime target catalog non_runtime_source_paths contains an invalid relative path: {raw_path}"
            )
        if any(character in path_value for character in "*?["):
            raise WorkflowError(
                "runtime target catalog non_runtime_source_paths requires exact paths without wildcards: "
                f"{path_value}"
            )
        if path_value in non_runtime_paths:
            raise WorkflowError(
                f"runtime target catalog non_runtime_source_paths contains a duplicate: {path_value}"
            )
        overlapping_targets = sorted(
            str(target_id)
            for target_id, target in targets.items()
            if any(
                _runtime_glob_matches(path_value, str(pattern))
                for pattern in flow._as_list(target.get("source_globs"))
            )
        )
        if overlapping_targets:
            raise WorkflowError(
                "runtime target catalog non-runtime path overlaps runtime targets: "
                f"{path_value} -> {overlapping_targets}"
            )
        if not path_value.startswith("scripts/") or Path(path_value).suffix.casefold() not in {".py", ".ps1"}:
            raise WorkflowError(
                "runtime target catalog non_runtime_source_paths only accepts Python or PowerShell operator scripts under scripts/: "
                f"{path_value}"
            )
        candidate = root.joinpath(*path_value.split("/"))
        try:
            resolved_candidate = candidate.resolve(strict=True)
            resolved_relative = resolved_candidate.relative_to(root_resolved).as_posix()
        except (FileNotFoundError, OSError, ValueError):
            raise WorkflowError(
                "runtime target catalog non_runtime_source_paths entry must be an existing repository file: "
                f"{path_value}"
            ) from None
        if not candidate.is_file():
            raise WorkflowError(
                "runtime target catalog non_runtime_source_paths entry must be a regular file: "
                f"{path_value}"
            )
        if candidate.is_symlink() or resolved_relative.casefold() != path_value.casefold():
            raise WorkflowError(
                "runtime target catalog non_runtime_source_paths entry must not use a symbolic-link alias: "
                f"{path_value}"
            )
        cursor = root
        canonical_parts: list[str] = []
        for part in path_value.split("/"):
            matches = [entry for entry in cursor.iterdir() if entry.name.casefold() == part.casefold()]
            if len(matches) != 1:
                raise WorkflowError(
                    "runtime target catalog non_runtime_source_paths entry has ambiguous or missing path casing: "
                    f"{path_value}"
                )
            cursor = matches[0]
            canonical_parts.append(cursor.name)
        canonical_path = "/".join(canonical_parts)
        if canonical_path != path_value:
            raise WorkflowError(
                "runtime target catalog non_runtime_source_paths entry must use canonical repository path casing: "
                f"{path_value} -> {canonical_path}"
            )
        normalized_key = os.path.normcase(path_value).casefold()
        if normalized_key in non_runtime_path_keys:
            raise WorkflowError(
                f"runtime target catalog non_runtime_source_paths contains a duplicate: {path_value}"
            )
        non_runtime_path_keys.add(normalized_key)
        non_runtime_paths.append(path_value)
    payload["non_runtime_source_paths"] = non_runtime_paths
    return payload


def _runtime_glob_matches(path: str, pattern: str) -> bool:
    candidates = {pattern}
    current = pattern
    while "**/" in current:
        current = current.replace("**/", "", 1)
        candidates.add(current)
    return any(fnmatch.fnmatchcase(path, candidate) for candidate in candidates)


def _classify_runtime_impact(changed_files: Iterable[str], *, root: Path | None = None) -> dict[str, Any]:
    root = root or REPO_ROOT
    normalized = sorted(
        {
            str(item).replace("\\", "/").removeprefix("./")
            for item in changed_files
            if str(item).strip()
        }
    )
    impacts: set[str] = set()
    runtime_files: list[str] = []
    target_ids: set[str] = set()
    catalog: dict[str, Any] = {}
    with contextlib.suppress(WorkflowError):
        catalog = _load_runtime_target_catalog(root)
    catalog_targets = catalog.get("targets") or {}
    catalog_non_runtime_files = set(flow._as_list(catalog.get("non_runtime_source_paths")))
    known_non_runtime_prefixes = (
        ".github/",
        "backend/tests/",
        "docs/",
        "frontend/tests/",
        "scripts/ci/",
        "scripts/ci_",
        "tests/",
    )
    known_non_runtime_files = {
        "backend/services/advisory_model_first/selection_liability_gate_pipeline.py",
        "backend/services/advisory_model_first/p0g_anchored_liability_local_reranker_bundle.py",
        "backend/services/advisory_model_first/p0g_anchored_liability_local_reranker_contracts.py",
        "backend/services/advisory_model_first/p0g_anchored_liability_local_reranker_pipeline.py",
        "backend/services/advisory_model_first/p0g_anchored_liability_local_reranker_training.py",
        "backend/services/advisory_model_first/qe_alpha_generator_contracts.py",
        "backend/services/advisory_model_first/qe_alpha_generator_pipeline.py",
        "backend/services/advisory_model_first/turnover_constrained_utility_training.py",
        "scripts/advisory_p0l_build_training_request.py",
        "scripts/wsl/advisory_p0l_train.py",
        "backend/services/advisory_phase0b/audit_service.py",
        "backend/services/advisory_phase0b/snapshot_reader.py",
        "backend/services/hmm_risk/b3_d1_inactive_dimension.py",
        "backend/services/hmm_risk/b3_mixed_dimension.py",
        "backend/services/hmm_risk/b3_training.py",
        "backend/services/hmm_risk/market_relative_jump_spike.py",
        "backend/services/hmm_risk/market_relative_ridge_candidate.py",
        "backend/services/hmm_risk/market_relative_ridge_holdout.py",
        "backend/services/hmm_risk/rotation_l1_input_bundle.py",
        "backend/services/hmm_risk/state_model_set.py",
        "backend/services/dataset_release/direct_monthly.py",
        "backend/services/hmm_risk/stock_fact_repository.py",
        "backend/services/announcements/title_classifier.py",
        "backend/services/event_signal/st_announcement_adapter.py",
        "scripts/advisory_short_rebound_batch_b.py",
        "scripts/aistock_bug_id_allocator.py",
        "scripts/bug_registry_metadata_check.py",
        "scripts/aistock_issue_workflow.py",
        "scripts/issue_flow.py",
        "scripts/aistock_guardrail_scan.py",
        "scripts/ci_failure_issue_summary.py",
        "scripts/ci/prepare_self_hosted_workspace.py",
        "scripts/export_qe_qlib_candidate.py",
        "scripts/export_suspend_d_candidate.py",
        "scripts/build_stock_universe_pit_spans.py",
        "scripts/classify_announcement_titles_v0.py",
        "scripts/sync_eastmoney_anns_metadata.py",
        "scripts/dataset_release_control_store.py",
        "scripts/update_backtest_dataset_monthly.py",
        "scripts/qlib_multi_dataset_smoke_backtest.py",
        "scripts/qlib_authoritative_smoke_backtest.py",
        "scripts/llm_provider_adapter.py",
        "scripts/nightly_adaptive_scheduler.py",
        "scripts/nightly_session_runner.py",
        "scripts/ci_change_classifier.py",
        "scripts/hmm_risk/prepare_state_model_set.py",
        "scripts/hmm_risk/run_market_relative_jump_spike.py",
        "scripts/hmm_risk/run_market_relative_ridge_candidate.py",
        "scripts/hmm_risk/run_market_relative_ridge_holdout.py",
        "scripts/hmm_risk/build_rotation_l1_input_bundle.py",
        "noxfile.py",
    }
    known_client_files = {
        "scripts/aistock_mcp_server.py",
    }
    for path in normalized:
        lower = path.lower()
        if path in known_client_files or lower.startswith((".codex/", ".claude/")):
            impacts.add("client")
            continue
        if (
            path in known_non_runtime_files
            or path in catalog_non_runtime_files
            or lower.startswith(known_non_runtime_prefixes)
        ):
            impacts.add("none")
            continue
        matched_targets: list[tuple[str, str]] = []
        for catalog_target_id, catalog_target in catalog_targets.items():
            if not isinstance(catalog_target, dict):
                continue
            if any(
                _runtime_glob_matches(path, str(pattern))
                for pattern in flow._as_list(catalog_target.get("source_globs"))
            ):
                matched_targets.append((str(catalog_target_id), str(catalog_target.get("runtime_kind") or "unknown")))
        worker_matches = [item for item in matched_targets if item[1] == "worker_scheduler"]
        if worker_matches:
            matched_targets = worker_matches
        if matched_targets:
            runtime_files.append(path)
            for catalog_target_id, runtime_kind in matched_targets:
                target_ids.add(catalog_target_id)
                impacts.add(runtime_kind if runtime_kind in RUNTIME_IMPACTS else "unknown")
        elif lower.startswith("tdx-api-main/") and lower.endswith(".go"):
            impacts.add("backend")
            runtime_files.append(path)
            target_ids.add("tdx-go-backend")
        elif lower.startswith("backend/") and lower.endswith(".py"):
            if "scheduler" in lower or "worker" in lower:
                impacts.add("worker_scheduler")
                target_ids.add("worker-scheduler")
            else:
                impacts.add("backend")
                target_ids.add("backend-main")
            runtime_files.append(path)
        elif lower.startswith(("frontend/src/", "frontend/app/", "frontend/pages/", "frontend/components/", "frontend/lib/")):
            impacts.add("frontend")
        elif lower.startswith(("migrations/", "backend/migrations/")) or lower.endswith(".sql"):
            impacts.add("database")
        elif lower.endswith((".md", ".json", ".yaml", ".yml")):
            impacts.add("none")
        else:
            impacts.add("unknown")
    effective = "none"
    for candidate in ("unknown", "worker_scheduler", "backend", "database", "frontend", "client"):
        if candidate in impacts:
            effective = candidate
            break
    return {
        "runtime_impact": effective,
        "observed_impacts": sorted(impacts),
        "runtime_files": runtime_files,
        "target_ids": sorted(target_ids),
    }


def _record_has_file_scope_changed_files(record: dict[str, Any]) -> bool:
    contract = record.get("file_scope_contract")
    changed = None
    if isinstance(contract, dict):
        changed = contract.get("actual_changed_files") or contract.get("changed_files")
    return isinstance(changed, list) and any(str(item).strip() for item in changed)


def resolve_record_runtime_changed_files(record: dict[str, Any]) -> list[str]:
    """Return the authoritative changed-file list for record-based runtime inference.

    Prefers ``file_scope_contract.actual_changed_files`` after finish, then the
    legacy ``changed_files`` field. Only records without either valid non-empty
    list fall back to ``allowed_write_scope``; the fallback keeps fail-closed
    semantics and never downgrades ``unknown`` to ``none``.
    Normalization matches ``_classify_runtime_impact``: unify ``/``, strip
    ``./``, drop empties, dedupe, deterministic sort.
    """
    source: Iterable[Any]
    if _record_has_file_scope_changed_files(record):
        contract = record.get("file_scope_contract") or {}
        source = contract.get("actual_changed_files") or contract.get("changed_files") or []
    else:
        source = flow._as_list(record.get("allowed_write_scope"))
    return sorted(
        {
            str(item).replace("\\", "/").removeprefix("./")
            for item in source
            if str(item).strip()
        }
    )


def _runtime_contract_is_provisional(record: dict[str, Any], explicit: dict[str, Any]) -> bool:
    basis = str(explicit.get("inference_basis") or "").strip()
    if basis:
        return basis == RUNTIME_INFERENCE_PLANNED_SCOPE
    file_scope = record.get("file_scope_contract")
    if not isinstance(file_scope, dict):
        return False
    source = str(file_scope.get("changed_files_source") or "").strip()
    if source:
        return source == FILE_SCOPE_SOURCE_PLANNED_INTAKE
    return file_scope.get("schema_version") == "aistock_submit_bug_file_scope_v1"


def _can_reconcile_provisional_runtime_contract(
    *,
    explicit_impact: str,
    inferred_impact: str,
    planned_target_ids: list[str],
    actual_target_ids: list[str],
) -> bool:
    if inferred_impact == "unknown":
        return False
    if not set(actual_target_ids).issubset(set(planned_target_ids)):
        return False
    if inferred_impact == "none":
        return True
    if inferred_impact in {"backend", "worker_scheduler"}:
        return bool(actual_target_ids)
    return inferred_impact == explicit_impact


def _actual_file_scope_contract(record: dict[str, Any], changed_files: list[str]) -> dict[str, Any]:
    existing = record.get("file_scope_contract")
    contract = dict(existing) if isinstance(existing, dict) else {
        "schema_version": "aistock_submit_bug_file_scope_v1",
    }
    planned = flow._unique_strings(
        flow._as_list(contract.get("planned_files"))
        or flow._as_list(contract.get("scope_files"))
        or flow._as_list(contract.get("changed_files"))
    )
    actual = _normalize_changed_files(changed_files)
    contract.update(
        {
            "planned_files": planned,
            "changed_files": actual,
            "actual_changed_files": actual,
            "changed_files_source": FILE_SCOPE_SOURCE_GIT_FINISH,
        }
    )
    return contract


def _resolve_runtime_ref(value: Any, explicit: dict[str, Any]) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    marker = "bug_record.runtime_contract."
    if text.startswith(marker):
        replacement = explicit.get(text[len(marker):])
        return str(replacement).strip() if replacement else None
    return text


def _validate_operator_runbook_ref(value: str | None, *, root: Path) -> str | None:
    text = str(value or "").strip().replace("\\", "/")
    if not text:
        return "operator runbook ref is incomplete"
    candidate = Path(text)
    if candidate.is_absolute() or ".." in candidate.parts:
        return f"operator runbook ref must be a repository-relative docs/operations file: {text}"
    if not text.startswith("docs/operations/"):
        return f"operator runbook ref must be under docs/operations: {text}"
    resolved = (root / candidate).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError:
        return f"operator runbook ref escapes repository root: {text}"
    if not resolved.is_file():
        return f"operator runbook ref does not exist: {text}"
    return None


def _runtime_contract_digest(contract: dict[str, Any]) -> str:
    target = contract.get("target") if isinstance(contract.get("target"), dict) else {}
    payload = {
        "schema_version": contract.get("schema_version"),
        "runtime_impact": contract.get("runtime_impact"),
        "target_id": contract.get("target_id"),
        "target_ids": contract.get("target_ids") or [],
        "catalog_ref": contract.get("catalog_ref"),
        "operator_runbook_ref": contract.get("operator_runbook_ref"),
        "expected_identity_ref": contract.get("expected_identity_ref"),
        "expected_terminal_outcome": contract.get("expected_terminal_outcome"),
        "probe_route_id": target.get("probe_route_id"),
        "probe_mode": target.get("probe_mode") or "http",
        "probe_origins": target.get("probe_origins") or [],
        "local_probe": target.get("local_probe") or None,
        "probes": target.get("probes") or {},
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _select_runtime_probe_route(
    target: dict[str, Any],
    *,
    runtime_files: Iterable[str],
) -> tuple[dict[str, Any], str | None]:
    selected = {key: value for key, value in target.items() if key != "probe_routes"}
    routes = target.get("probe_routes") if isinstance(target.get("probe_routes"), list) else []
    files = flow._unique_strings(runtime_files)
    fully_matched: list[dict[str, Any]] = []
    partially_matched: list[str] = []
    for route in routes:
        if not isinstance(route, dict):
            continue
        patterns = flow._as_list(route.get("source_globs"))
        matches = [any(_runtime_glob_matches(path, str(pattern)) for pattern in patterns) for path in files]
        if matches and all(matches):
            fully_matched.append(route)
        elif any(matches):
            partially_matched.append(str(route.get("route_id") or "unknown"))
    if len(fully_matched) > 1:
        return selected, "runtime files match multiple probe routes"
    if not fully_matched:
        if partially_matched:
            return (
                selected,
                "runtime files mix routed and non-routed Worker sources: "
                + json.dumps(partially_matched, ensure_ascii=False),
            )
        return selected, None
    route = fully_matched[0]
    selected.update(
        {
            "probe_route_id": str(route.get("route_id")),
            "probe_mode": route.get("probe_mode"),
            "local_probe": route.get("local_probe"),
            "probes": route.get("probes"),
        }
    )
    selected.pop("probe_origins", None)
    return selected, None


def _runtime_catalog_sha256(*, root: Path) -> str:
    return hashlib.sha256((root / "docs" / "standards" / "aistock_runtime_targets_v1.yaml").read_bytes()).hexdigest()


def build_runtime_contract(
    *,
    record: dict[str, Any],
    changed_files: Iterable[str],
    root: Path | None = None,
    fresh_process_evidence: Iterable[str] | None = None,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    inferred = _classify_runtime_impact(changed_files, root=root)
    explicit = record.get("runtime_contract") if isinstance(record.get("runtime_contract"), dict) else {}
    blocking: list[str] = []
    explicit_impact = str(explicit.get("runtime_impact") or "").strip()
    inferred_impact = str(inferred["runtime_impact"])
    if explicit and explicit.get("schema_version") != RUNTIME_CONTRACT_SCHEMA:
        blocking.append(f"runtime_contract schema_version must be {RUNTIME_CONTRACT_SCHEMA}")
    if explicit_impact and explicit_impact not in RUNTIME_IMPACTS:
        blocking.append(f"runtime_contract runtime_impact is invalid: {explicit_impact}")
        explicit_impact = "unknown"
    explicit_target_ids = flow._unique_strings(
        flow._as_list(explicit.get("target_ids")) + flow._as_list(explicit.get("target_id"))
    )
    inferred_target_ids = flow._unique_strings(inferred["target_ids"])
    provisional = _runtime_contract_is_provisional(record, explicit)
    reconciled = provisional and _can_reconcile_provisional_runtime_contract(
        explicit_impact=explicit_impact,
        inferred_impact=inferred_impact,
        planned_target_ids=explicit_target_ids,
        actual_target_ids=inferred_target_ids,
    )
    reconciliation_status = None
    if reconciled:
        runtime_impact = inferred_impact
        target_ids = inferred_target_ids
        reconciliation_status = (
            "exact" if set(explicit_target_ids) == set(inferred_target_ids) and explicit_impact == inferred_impact else "narrowed"
        )
    else:
        runtime_impact = explicit_impact or inferred_impact
        if inferred_impact != "none" and explicit_impact == "none":
            blocking.append(
                f"runtime_contract cannot downgrade inferred runtime impact: explicit=none inferred={inferred_impact}"
            )
            runtime_impact = inferred_impact
        elif inferred_impact != "none" and explicit_impact and explicit_impact != inferred_impact:
            blocking.append(
                "runtime_contract impact conflicts with changed-file inference: "
                f"explicit={explicit_impact} inferred={inferred_impact}"
            )
            runtime_impact = "unknown"
        target_ids = flow._unique_strings(explicit_target_ids + inferred_target_ids)
        if inferred_target_ids and explicit_target_ids and set(explicit_target_ids) != set(inferred_target_ids):
            blocking.append(
                "runtime_contract target set conflicts with changed-file inference: "
                f"explicit={explicit_target_ids} inferred={inferred_target_ids}"
            )
    if runtime_impact not in RUNTIME_IMPACTS:
        runtime_impact = "unknown"
    target_id = target_ids[0] if len(target_ids) == 1 else None
    inferred_backend_restart = inferred_impact in {"backend", "worker_scheduler"}
    backend_restart_required = inferred_backend_restart or runtime_impact in {"backend", "worker_scheduler"}
    if backend_restart_required and len(target_ids) > 1:
        blocking.append(
            "multiple runtime targets require separate BUGs and post-restart receipts: "
            + ", ".join(target_ids)
        )
    target: dict[str, Any] | None = None
    catalog_ref = _repo_rel(root / "docs" / "standards" / "aistock_runtime_targets_v1.yaml", root)
    if backend_restart_required:
        try:
            catalog = _load_runtime_target_catalog(root)
            raw_target = (catalog.get("targets") or {}).get(target_id)
            if not isinstance(raw_target, dict):
                blocking.append(f"runtime target is missing or ambiguous: {target_id or target_ids or 'none'}")
            else:
                raw_target, probe_route_error = _select_runtime_probe_route(
                    raw_target,
                    runtime_files=inferred.get("runtime_files") or [],
                )
                if probe_route_error:
                    blocking.append(f"runtime target {target_id} {probe_route_error}")
                probes = raw_target.get("probes") if isinstance(raw_target.get("probes"), dict) else {}
                target = {
                    **raw_target,
                    "target_id": target_id,
                    "operator_runbook_ref": _resolve_runtime_ref(raw_target.get("operator_runbook_ref"), explicit),
                    "probes": {key: _resolve_runtime_ref(value, explicit) for key, value in probes.items()},
                }
                if not target.get("operator_runbook_ref"):
                    blocking.append(f"runtime target {target_id} operator runbook ref is incomplete")
                else:
                    runbook_error = _validate_operator_runbook_ref(target.get("operator_runbook_ref"), root=root)
                    if runbook_error:
                        blocking.append(f"runtime target {target_id} {runbook_error}")
                probe_mode = str(target.get("probe_mode") or "http")
                if probe_mode == _DATASET_RELEASE_WORKER_HEARTBEAT_MODE:
                    if target["probes"] != _DATASET_RELEASE_WORKER_HEARTBEAT_REFS:
                        blocking.append(
                            f"runtime target {target_id} dataset-release heartbeat probes must match "
                            "the canonical local probe set"
                        )
                else:
                    for field in ("health_ref", "identity_ref", "business_smoke_ref"):
                        if not target["probes"].get(field):
                            blocking.append(f"runtime target {target_id} probe is incomplete: {field}")
                        else:
                            probe_error = _validate_runtime_probe_ref(
                                field,
                                target["probes"].get(field),
                                allowed_origins=flow._as_list(target.get("probe_origins")),
                            )
                            if probe_error:
                                blocking.append(f"runtime target {target_id} {probe_error}")
                    database_ref = target["probes"].get("database_readback_ref")
                    if database_ref:
                        probe_error = _validate_runtime_probe_ref(
                            "database_readback_ref",
                            database_ref,
                            allowed_origins=flow._as_list(target.get("probe_origins")),
                        )
                        if probe_error:
                            blocking.append(f"runtime target {target_id} {probe_error}")
        except WorkflowError as exc:
            blocking.append(str(exc))
        if not explicit:
            blocking.append("legacy or runtime BUG requires an explicit runtime_contract schema upgrade")
    elif runtime_impact == "unknown":
        blocking.append("runtime_impact is unknown and cannot be treated as none")
    expectation, expectation_errors = _normalize_expected_terminal_outcome(
        explicit.get("expected_terminal_outcome")
    )
    blocking.extend(expectation_errors)
    if expectation is not None:
        smoke_ref = ((target or {}).get("probes") or {}).get("business_smoke_ref")
        smoke_path = urllib.parse.urlsplit(str(smoke_ref or "")).path or "/"
        smoke_contract = _business_smoke_semantic_contract(smoke_path) if smoke_ref else None
        resolved_contract_id = smoke_contract[0] if smoke_contract else None
        if not smoke_ref:
            blocking.append("expected_terminal_outcome requires a resolved business_smoke_ref probe")
        elif resolved_contract_id != expectation.get("contract_id"):
            blocking.append(
                "expected_terminal_outcome contract does not match the business_smoke_ref probe contract: "
                f"declared={expectation.get('contract_id')} resolved={resolved_contract_id or 'none'}"
            )
        elif resolved_contract_id == "run_terminal_evidence":
            run_id_match = re.search(r"/runs/([^/]+)/terminal-evidence$", smoke_path)
            probe_run_id = urllib.parse.unquote(run_id_match.group(1)) if run_id_match else ""
            declared_run_id = str(expectation.get("expected_run_id") or "")
            if probe_run_id != declared_run_id:
                blocking.append(
                    "expected_terminal_outcome expected_run_id does not match the business_smoke_ref "
                    f"subject run: declared={declared_run_id or 'missing'} probe={probe_run_id or 'none'}"
                )
    persistence_basis = (
        str(explicit.get("persistence_basis") or "git_tracked_source")
        if backend_restart_required
        else "not_required"
    )
    valid_persistence_basis = {"git_tracked_source", "controlled_migration", "controlled_config", "not_required"}
    if persistence_basis not in valid_persistence_basis:
        blocking.append(f"runtime_contract persistence_basis is invalid: {persistence_basis}")
    observed_fresh_process_evidence = flow._unique_strings(
        flow._as_list(explicit.get("fresh_process_evidence")) + list(fresh_process_evidence or [])
    )
    if not backend_restart_required:
        observed_fresh_process_evidence = []
    if backend_restart_required and persistence_basis in {"", "unknown", "not_required"}:
        blocking.append("persistent fix basis is missing")
    if backend_restart_required and not observed_fresh_process_evidence:
        blocking.append("fresh-process load evidence is missing")
    post_restart_gate = (
        str(explicit.get("post_restart_effective_gate") or "pending_user_restart")
        if backend_restart_required
        else "not_required"
    )
    return {
        "schema_version": RUNTIME_CONTRACT_SCHEMA,
        "runtime_impact": runtime_impact,
        "runtime_contract_source": (
            RUNTIME_INFERENCE_ACTUAL_CHANGED_FILES
            if reconciled
            else ("explicit" if explicit else "inferred")
        ),
        "provisional_reconciliation": {
            "applied": reconciled,
            "status": reconciliation_status,
            "planned_target_ids": explicit_target_ids,
            "actual_target_ids": inferred_target_ids,
        },
        "backend_restart_required": backend_restart_required,
        "backend_restart_owner": "user",
        "target_id": target_id,
        "target_ids": target_ids,
        "catalog_ref": catalog_ref,
        "operator_runbook_ref": (target or {}).get("operator_runbook_ref"),
        "expected_identity_ref": (target or {}).get("expected_identity_ref"),
        "expected_terminal_outcome": expectation if backend_restart_required else None,
        "persistence_basis": persistence_basis,
        "fresh_process_evidence": observed_fresh_process_evidence,
        "post_restart_effective_gate": post_restart_gate,
        "runtime_identity_match": (
            str(explicit.get("runtime_identity_match") or "pending")
            if backend_restart_required
            else "not_required"
        ),
        "activation_states": {
            "backend_restart": "pending_user_action" if backend_restart_required else "not_required",
            "frontend_activation": "required" if runtime_impact == "frontend" else "not_required",
            "client_reload": "required" if runtime_impact == "client" else "not_required",
            "database_migration": "required" if runtime_impact == "database" else "not_required",
        },
        "target": target,
        "blocking": flow._unique_strings(blocking),
        "pre_pr_ready": not blocking,
    }


def build_restart_plan(*, bug_id: str | None, issue_json: str | None) -> dict[str, Any]:
    record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
    canonical_bug_id = str(record.get("bug_id") or bug_id or source_path.stem).upper()
    contract = build_runtime_contract(
        record=record,
        changed_files=resolve_record_runtime_changed_files(record),
        fresh_process_evidence=flow._as_list((record.get("runtime_contract") or {}).get("fresh_process_evidence"))
        if isinstance(record.get("runtime_contract"), dict)
        else [],
    )
    blocking = list(contract.get("blocking") or [])
    return {
        "schema_version": "aistock_backend_restart_plan_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "workflow_gate": "operator_action_required" if contract.get("backend_restart_required") and not blocking else (
            "not_required" if not contract.get("backend_restart_required") and not blocking else "blocked"
        ),
        "backend_restart_owner": "user",
        "process_control_performed": False,
        "target_id": contract.get("target_id"),
        "catalog_ref": contract.get("catalog_ref"),
        "operator_runbook_ref": contract.get("operator_runbook_ref"),
        "expected_identity_ref": contract.get("expected_identity_ref"),
        "post_restart_smoke_ref": ((contract.get("target") or {}).get("probes") or {}).get("business_smoke_ref"),
        "blocking": blocking,
        "next_user_action": "restart the catalog target, then run post-restart-verify" if not blocking and contract.get("backend_restart_required") else None,
        "runtime_contract": contract,
    }


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        return None


def _open_read_only_url(request: urllib.request.Request, *, timeout_seconds: float) -> Any:
    return urllib.request.build_opener(_NoRedirectHandler()).open(request, timeout=timeout_seconds)


def _normalized_http_origin(url: str) -> str | None:
    try:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
            return None
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError:
        return None
    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    return f"{parsed.scheme}://{host}:{port}"


def _validate_runtime_probe_ref(
    name: str,
    value: Any,
    *,
    allowed_origins: Iterable[str],
) -> str | None:
    text = str(value or "").strip()
    if name == "database_readback_ref" and text.lower() == "not_required":
        return None
    if re.search(r"\s|[{}]", text):
        return f"probe {name} must be an executable absolute endpoint without whitespace or placeholders: {text or 'missing'}"
    origin = _normalized_http_origin(text)
    if origin is None:
        return f"probe {name} must be an http(s) read-only endpoint without credentials: {text or 'missing'}"
    allowed = {_normalized_http_origin(item) for item in allowed_origins}
    allowed.discard(None)
    if origin not in allowed:
        return f"probe {name} origin is not catalog-allowed: {origin}"
    return None


def _json_payload_kind(payload: Any) -> str:
    if isinstance(payload, dict):
        return "object"
    if isinstance(payload, list):
        return "array"
    return "scalar"


def _payload_schema_evidence(body: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return {"json": False, "kind": "none"}
    return {"json": True, "kind": _json_payload_kind(payload)}


_READ_ONLY_HTTP_PROBE_MAX_BYTES = 8 * 1024 * 1024


def _read_only_http_probe(
    name: str,
    url: str,
    *,
    allowed_origins: Iterable[str],
    timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    origin = _normalized_http_origin(url)
    allowed = {_normalized_http_origin(item) for item in allowed_origins}
    allowed.discard(None)
    if origin is None:
        reason = "probe ref must be an http(s) read-only endpoint without credentials"
        return {
            "name": name,
            "url": url,
            "status": "blocked",
            "error": reason,
            "transport": {"status_code": None, "ok": False, "error": reason},
            "payload_schema": {"json": False, "kind": "none"},
        }
    if origin not in allowed:
        reason = f"probe origin is not catalog-allowed: {origin}"
        return {
            "name": name,
            "url": url,
            "status": "blocked",
            "error": reason,
            "transport": {"status_code": None, "ok": False, "error": reason},
            "payload_schema": {"json": False, "kind": "none"},
        }
    request = urllib.request.Request(url, method="GET", headers={"Accept": "application/json,text/plain,*/*"})
    try:
        with _open_read_only_url(request, timeout_seconds=timeout_seconds) as response:
            status_code = int(getattr(response, "status", 200))
            body = response.read(_READ_ONLY_HTTP_PROBE_MAX_BYTES + 1)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return {
            "name": name,
            "url": url,
            "status": "failed",
            "error": str(exc),
            "transport": {"status_code": None, "ok": False, "error": str(exc)},
            "payload_schema": {"json": False, "kind": "none"},
        }
    transport_ok = 200 <= status_code < 400
    if len(body) > _READ_ONLY_HTTP_PROBE_MAX_BYTES:
        reason = (
            "probe response exceeds maximum allowed size: "
            f">{_READ_ONLY_HTTP_PROBE_MAX_BYTES} bytes"
        )
        return {
            "name": name,
            "url": url,
            "status": "failed",
            "status_code": status_code,
            "error": reason,
            "transport": {
                "status_code": status_code,
                "ok": transport_ok,
                "error": None if transport_ok else f"unexpected HTTP status code: {status_code}",
            },
            "payload_schema": {"json": False, "kind": "none"},
            "response_bytes": len(body),
            "response_limit_bytes": _READ_ONLY_HTTP_PROBE_MAX_BYTES,
        }
    return {
        "name": name,
        "url": url,
        "status": "passed" if transport_ok else "failed",
        "status_code": status_code,
        "transport": {
            "status_code": status_code,
            "ok": transport_ok,
            "error": None if transport_ok else f"unexpected HTTP status code: {status_code}",
        },
        "payload_schema": _payload_schema_evidence(body),
        "response_sha256": hashlib.sha256(body).hexdigest(),
        "response_bytes": len(body),
        "_response_body": body.decode("utf-8", errors="replace"),
    }


def _load_dataset_release_worker_heartbeat_snapshot(target: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    local_probe = target.get("local_probe") if isinstance(target.get("local_probe"), dict) else {}
    profile_ref = str(local_probe.get("profile_path") or "").strip()
    profile_path = (REPO_ROOT / profile_ref).resolve()
    try:
        profile_path.relative_to(REPO_ROOT.resolve())
    except ValueError as exc:
        raise WorkflowError("dataset-release Worker heartbeat profile escapes repository root") from exc

    from backend.services.dataset_release.control_store import ControlStore
    from backend.services.dataset_release.profile import load_dataset_profile
    from backend.services.dataset_release.worker_identity import WorkerHeartbeatStore

    profile = load_dataset_profile(profile_path)
    store = ControlStore(Path(str(profile.control_root)), read_only=True)
    heartbeats = WorkerHeartbeatStore(store)
    health = heartbeats.read_latest(
        profile=profile.profile,
        config_digest=profile.config_digest,
        ttl_seconds=profile.worker_heartbeat_ttl_seconds,
        max_files=int(local_probe.get("max_files", 200)),
    ).as_dict()
    instance_id = str(health.get("instance_id") or "")
    payload = dict(heartbeats.read(instance_id)) if instance_id else {}
    identity = payload.get("identity") if isinstance(payload.get("identity"), dict) else {}
    if (
        instance_id
        and (
            str(identity.get("instance_id") or "") != instance_id
            or str(identity.get("capability_digest") or "") != str(health.get("capability_digest") or "")
            or str(payload.get("last_poll_at") or "") != str(health.get("last_poll_at") or "")
        )
    ):
        raise WorkflowError("dataset-release Worker heartbeat changed during verified read")
    return health, payload


def _read_dataset_release_worker_heartbeat_probes(
    target: dict[str, Any],
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    if timeout_seconds <= 0:
        raise WorkflowError("dataset-release Worker heartbeat probe timeout must be positive")
    try:
        health, payload = _load_dataset_release_worker_heartbeat_snapshot(target)
        raw = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except Exception as exc:
        reason = f"dataset-release Worker heartbeat probe failed: {type(exc).__name__}: {exc}"
        return [
            {
                "name": name,
                "url": f"worker-heartbeat://worker-scheduler/{name}",
                "status": "failed",
                "error": reason,
                "transport": {"status_code": None, "ok": False, "error": reason, "kind": "local_file"},
                "payload_schema": {"json": False, "kind": "none"},
            }
            for name in ("health_ref", "identity_ref", "business_smoke_ref")
        ]

    identity = payload.get("identity") if isinstance(payload.get("identity"), dict) else {}
    code_sha = str(identity.get("code_sha") or "").strip().lower()
    worker_status = str(payload.get("status") or "").strip().upper()
    claim_kind = payload.get("claim_kind")
    claim_id = payload.get("claim_id")
    claim_consistent = (claim_kind is None) == (claim_id is None) and (
        claim_kind is None or (bool(str(claim_kind).strip()) and bool(str(claim_id).strip()))
    )
    health_ok = health.get("state") == "healthy"
    identity_ok = bool(_FULL_GIT_COMMIT_RE.fullmatch(code_sha))
    business_ok = bool(
        health_ok
        and identity_ok
        and payload.get("stop_requested") is False
        and worker_status not in {"", "STARTED", "STOP_REQUESTED", "STOPPED"}
        and claim_consistent
    )
    response_sha = hashlib.sha256(raw).hexdigest()
    common = {
        "url": "worker-heartbeat://worker-scheduler",
        "status_code": None,
        "transport": {"status_code": None, "ok": True, "error": None, "kind": "local_file"},
        "payload_schema": {"json": True, "kind": "object"},
        "response_sha256": response_sha,
        "response_bytes": len(raw),
    }
    health_reason = None if health_ok else f"worker health is {health.get('state')}: {health.get('reason')}"
    identity_reason = None if identity_ok else "worker heartbeat code_sha is not one full Git commit"
    business_reason = None
    if not business_ok:
        business_reason = (
            "worker heartbeat is not business-ready: "
            f"health={health.get('state')} status={worker_status or 'missing'} "
            f"stop_requested={payload.get('stop_requested')} claim_consistent={claim_consistent}"
        )
    semantic = {
        "schema_version": BUSINESS_SMOKE_SEMANTIC_SCHEMA,
        "contract_id": "dataset_release_worker_heartbeat",
        "verdict": "passed" if business_ok else "failed",
        "reason": business_reason,
        "facts": {
            "state": health.get("state"),
            "worker_status": worker_status or None,
            "stop_requested": payload.get("stop_requested"),
            "claim_present": claim_id is not None,
        },
        "expectation": None,
        "expectation_digest": None,
        "response_sha256": response_sha,
    }
    results: list[dict[str, Any]] = []
    for name, passed, reason in (
        ("health_ref", health_ok, health_reason),
        ("identity_ref", identity_ok, identity_reason),
        ("business_smoke_ref", business_ok, business_reason),
    ):
        result = {"name": name, **common, "status": "passed" if passed else "failed"}
        result["url"] = f"worker-heartbeat://worker-scheduler/{name}"
        if name == "identity_ref":
            result["_response_body"] = json.dumps({"commit": code_sha}, sort_keys=True)
        if reason:
            result["error"] = reason
        if name == "business_smoke_ref":
            result["semantic"] = semantic
        results.append(result)
    return results


# ---------------------------------------------------------------------------
# Target-owned business-smoke semantic contracts (BUG-1084).
#
# HTTP 2xx only proves transport success. A business-smoke probe may only
# contribute to a verified gate when the response payload satisfies the
# semantic success contract owned by the probed endpoint family. Endpoint
# path families map to contract kinds below; an endpoint without a
# registered contract, an unparsable payload, missing key fields, type
# errors, or conflicting fields all fail closed -- there is intentionally
# no HTTP-only fallback.
# ---------------------------------------------------------------------------

BUSINESS_SMOKE_SEMANTIC_SCHEMA = "aistock_business_smoke_semantic_verdict_v1"
SCHEDULER_VERIFICATION_STATUS_SCHEMA = "simulation_scheduler_verification_status_v1"
SCHEDULER_VERIFICATION_SCOPE_SCHEMA = "simulation_scheduler_verification_scope_v1"
_SCHEDULER_VERIFICATION_RUN_ID_RE = re.compile(r"^simrun_[0-9a-f]{16}$")
EXPECTED_TERMINAL_OUTCOME_SCHEMA = "aistock_expected_terminal_outcome_v1"
SIMULATION_RUN_TERMINAL_EVIDENCE_PAYLOAD_SCHEMA = "simulation_run_terminal_evidence_v1"
_EXPECTATION_CAPABLE_CONTRACT_IDS = frozenset({"run_terminal_evidence"})
_EXPECTED_TERMINAL_OUTCOME_FIELDS = (
    "schema_version",
    "contract_id",
    "expected_status",
    "expected_previous_status",
    "expected_reason_code",
    "expected_evidence_schema",
    "expected_run_id",
)
_EXPECTED_REASON_CODE_RE = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")
_EXPECTED_EVIDENCE_SCHEMA_RE = re.compile(r"^[a-z][a-z0-9_]{2,127}$")
_EXPECTED_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]{2,127}$")

_RUN_STATUS_SUCCESS = frozenset({
    "APPLIED",
    "CLOSED",
    "COMPLETED",
    "DONE",
    "FILLED",
    "PASSED",
    "READY",
    "SUCCESS",
    "SUCCEEDED",
})
_RUN_STATUS_FAILURE = frozenset({
    "ABORTED",
    "BLOCKED",
    "CANCELED",
    "CANCELLED",
    "DEAD",
    "ERROR",
    "FAILED",
    "FAILED_RETRYABLE",
    "FAILED_TERMINAL",
    "REJECTED",
    "TIMED_OUT",
    "TIMEOUT",
})
_HEALTH_FAILURE_STATUSES = _RUN_STATUS_FAILURE | {"DEGRADED", "DOWN", "STALLED", "UNHEALTHY"}
_HEALTH_SUCCESS_STATUSES = frozenset({"healthy", "ok", "passed", "ready", "success", "succeeded", "up"})
_CONTAINER_KEYS = ("run", "result", "batch", "task", "plan", "operation", "data", "scheduler", "summary")
_STATUS_FIELD_NAMES = ("status", "state", "verdict")

_COLLECTION_LIST_KEYS = (
    "items",
    "runs",
    "tasks",
    "nodes",
    "schedules",
    "entries",
    "results",
    "rows",
    "records",
    "batches",
    "experiments",
    "strategies",
    "programs",
    "data",
)
_SCHEDULER_BLOCKING_LIST_KEYS = frozenset({
    "blockers",
    "blocking_reasons",
    "errors",
    "failure_reasons",
    "last_result_errors",
})
_SCHEDULER_CURRENT_TRADE_DATE_BLOCKERS_KEY = "current_trade_date_blockers"
_SCHEDULER_BLOCKING_VALUE_KEYS = frozenset({"blocking_result", "last_blocking_result"})
_SCHEDULER_REASON_KEYS = frozenset({
    "blocked_reason",
    "blocking_reason",
    "failure_reason",
    "recovery_failure_reason",
})


def _collect_status_fields(payload: Any) -> dict[str, str]:
    """Collect status/state/verdict fields from a payload and known containers."""
    found: dict[str, str] = {}
    queue: list[tuple[str, Any]] = [("", payload)]
    depth = 0
    while queue and depth <= 4:
        depth += 1
        label, container = queue.pop(0)
        if not isinstance(container, dict):
            continue
        for field in _STATUS_FIELD_NAMES:
            value = container.get(field)
            if isinstance(value, str) and value.strip():
                name = f"{label}.{field}" if label else field
                found.setdefault(name, value.strip())
        for key in _CONTAINER_KEYS:
            nested = container.get(key)
            if isinstance(nested, dict):
                nested_label = f"{label}.{key}" if label else key
                queue.append((nested_label, nested))
    return found


def _normalize_expected_terminal_outcome(raw: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """Validate and normalize a declared expected terminal outcome.

    The declaration is the only channel through which a failure-class terminal
    status may satisfy a run-class business-smoke semantic contract. It is
    fail-closed: any schema violation yields blocking errors and no
    expectation. Normalization emits the fixed field set in canonical order so
    digests are stable.
    """
    if raw is None:
        return None, []
    if not isinstance(raw, dict):
        return None, ["expected_terminal_outcome must be an object"]
    errors: list[str] = []
    unknown_fields = sorted(set(raw) - set(_EXPECTED_TERMINAL_OUTCOME_FIELDS))
    if unknown_fields:
        errors.append(f"expected_terminal_outcome has unknown fields: {unknown_fields}")
    if raw.get("schema_version") != EXPECTED_TERMINAL_OUTCOME_SCHEMA:
        errors.append(f"expected_terminal_outcome schema_version must be {EXPECTED_TERMINAL_OUTCOME_SCHEMA}")
    contract_id = str(raw.get("contract_id") or "").strip()
    if contract_id not in _EXPECTATION_CAPABLE_CONTRACT_IDS:
        errors.append(
            "expected_terminal_outcome contract_id is not expectation-capable: "
            f"{contract_id or 'missing'}"
        )
    expected_status = str(raw.get("expected_status") or "").strip().upper()
    if expected_status not in _RUN_STATUS_FAILURE:
        errors.append(
            "expected_terminal_outcome expected_status must be a declared failure-class "
            f"terminal status: {expected_status or 'missing'}"
        )
    expected_previous_status = str(raw.get("expected_previous_status") or "").strip().upper()
    if expected_previous_status not in (_RUN_STATUS_FAILURE | _RUN_STATUS_SUCCESS):
        errors.append(
            "expected_terminal_outcome expected_previous_status must be a known terminal "
            f"status: {expected_previous_status or 'missing'}"
        )
    expected_reason_code = str(raw.get("expected_reason_code") or "").strip()
    if not _EXPECTED_REASON_CODE_RE.fullmatch(expected_reason_code):
        errors.append(
            "expected_terminal_outcome expected_reason_code must be an upper snake-case "
            f"reason code: {expected_reason_code or 'missing'}"
        )
    expected_evidence_schema = str(raw.get("expected_evidence_schema") or "").strip()
    if not _EXPECTED_EVIDENCE_SCHEMA_RE.fullmatch(expected_evidence_schema):
        errors.append(
            "expected_terminal_outcome expected_evidence_schema must be a lower snake-case "
            f"schema id: {expected_evidence_schema or 'missing'}"
        )
    expected_run_id = str(raw.get("expected_run_id") or "").strip()
    if not _EXPECTED_RUN_ID_RE.fullmatch(expected_run_id):
        errors.append(
            "expected_terminal_outcome expected_run_id must be a non-empty run id "
            f"(alphanumeric, underscore or dash): {expected_run_id or 'missing'}"
        )
    if errors:
        return None, errors
    normalized = {
        "schema_version": EXPECTED_TERMINAL_OUTCOME_SCHEMA,
        "contract_id": contract_id,
        "expected_status": expected_status,
        "expected_previous_status": expected_previous_status,
        "expected_reason_code": expected_reason_code,
        "expected_evidence_schema": expected_evidence_schema,
        "expected_run_id": expected_run_id,
    }
    return normalized, []


def _expectation_outcome_digest(expectation: dict[str, Any] | None) -> str | None:
    if expectation is None:
        return None
    encoded = json.dumps(expectation, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_run_terminal_success(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Run-class payloads must prove terminal target closure."""
    if not isinstance(payload, dict):
        return "failed", "run payload must be a JSON object", {}
    if payload.get("ok") is False:
        return "failed", "run payload reports ok=false", {}
    statuses = _collect_status_fields(payload)
    if not statuses:
        return "failed", "run payload is missing a status/state field to prove target closure", {}
    normalized = {name: value.upper() for name, value in statuses.items()}
    failures = {name: value for name, value in normalized.items() if value in _RUN_STATUS_FAILURE}
    if failures:
        detail = ", ".join(f"{name}={value}" for name, value in sorted(failures.items()))
        return "failed", f"run payload reports failure status: {detail}", {"statuses": normalized}
    open_states = {name: value for name, value in normalized.items() if value not in _RUN_STATUS_SUCCESS}
    if open_states:
        detail = ", ".join(f"{name}={value}" for name, value in sorted(open_states.items()))
        return (
            "failed",
            f"run payload has not reached terminal target closure: {detail}",
            {"statuses": normalized},
        )
    return "passed", None, {"statuses": normalized}


def _validate_run_terminal_evidence(
    payload: Any,
    *,
    expectation: dict[str, Any] | None = None,
) -> tuple[str, str | None, dict[str, Any]]:
    """Bounded terminal-evidence payloads prove an exact expected terminal outcome.

    Without a declared expectation this contract is exactly as strict as
    ``run_terminal_success``: every status/state/verdict field in the payload
    and its known containers is scanned, and failure-class or non-terminal
    values fail closed. With a declared expectation the verdict passes only
    when the subject run id, the run status, the evidence carrier schema, the
    terminal reason code and the previous status all match the declaration
    exactly; every mismatch fails closed with the specific drift.
    """
    if not isinstance(payload, dict):
        return "failed", "run terminal-evidence payload must be a JSON object", {}
    if payload.get("ok") is False:
        return "failed", "run terminal-evidence payload reports ok=false", {}
    if payload.get("schema_version") != SIMULATION_RUN_TERMINAL_EVIDENCE_PAYLOAD_SCHEMA:
        return (
            "failed",
            "run terminal-evidence payload schema mismatch: "
            f"expected {SIMULATION_RUN_TERMINAL_EVIDENCE_PAYLOAD_SCHEMA}, "
            f"observed {payload.get('schema_version')!r}",
            {},
        )
    run = payload.get("run")
    if not isinstance(run, dict):
        return "failed", "run terminal-evidence payload is missing the run object", {}
    raw_status = run.get("status")
    if not isinstance(raw_status, str) or not raw_status.strip():
        return "failed", "run terminal-evidence payload is missing run.status", {}
    status = raw_status.strip().upper()
    facts: dict[str, Any] = {"statuses": {"run.status": status}}
    if expectation is None:
        statuses = _collect_status_fields(payload)
        statuses.setdefault("run.status", status)
        normalized = {name: value.upper() for name, value in statuses.items()}
        facts = {"statuses": normalized}
        failures = {name: value for name, value in normalized.items() if value in _RUN_STATUS_FAILURE}
        if failures:
            detail = ", ".join(f"{name}={value}" for name, value in sorted(failures.items()))
            return "failed", f"run payload reports failure status: {detail}", facts
        open_states = {name: value for name, value in normalized.items() if value not in _RUN_STATUS_SUCCESS}
        if open_states:
            detail = ", ".join(f"{name}={value}" for name, value in sorted(open_states.items()))
            return (
                "failed",
                f"run payload has not reached terminal target closure: {detail}",
                facts,
            )
        return "passed", None, facts
    expected_status = str(expectation.get("expected_status") or "").strip().upper()
    if expected_status not in _RUN_STATUS_FAILURE:
        return (
            "failed",
            "declared expected terminal status is not failure-class: "
            f"{expected_status or 'missing'}",
            facts,
        )
    expected_run_id = str(expectation.get("expected_run_id") or "").strip()
    observed_run_id = str(run.get("run_id") or "").strip()
    facts["run_id"] = observed_run_id
    if not expected_run_id or observed_run_id != expected_run_id:
        return (
            "failed",
            "run id does not match the declared expected terminal outcome subject: "
            f"observed={observed_run_id or 'missing'} expected={expected_run_id or 'missing'}",
            facts,
        )
    if status != expected_status:
        return (
            "failed",
            f"run status does not match the declared expected terminal status: "
            f"run.status={status} expected={expected_status}",
            facts,
        )
    carriers = run.get("terminal_evidence")
    if not isinstance(carriers, list):
        return (
            "failed",
            "run terminal-evidence payload is missing the terminal_evidence carrier list",
            facts,
        )
    expected_schema = str(expectation.get("expected_evidence_schema") or "").strip()
    carrier = next(
        (
            item
            for item in carriers
            if isinstance(item, dict) and str(item.get("schema_version") or "").strip() == expected_schema
        ),
        None,
    )
    if carrier is None:
        return (
            "failed",
            f"expected terminal evidence carrier is absent: schema={expected_schema}",
            facts,
        )
    facts["matched_evidence"] = {
        "schema_version": expected_schema,
        "reason_code": carrier.get("reason_code"),
        "previous_status": carrier.get("previous_status"),
        "terminal_status": carrier.get("terminal_status"),
    }
    expected_reason = str(expectation.get("expected_reason_code") or "").strip()
    if str(carrier.get("reason_code") or "").strip() != expected_reason:
        return (
            "failed",
            f"terminal evidence reason code does not match the declaration: "
            f"observed={carrier.get('reason_code')!r} expected={expected_reason}",
            facts,
        )
    expected_previous = str(expectation.get("expected_previous_status") or "").strip().upper()
    observed_previous = str(carrier.get("previous_status") or "").strip().upper()
    if observed_previous != expected_previous:
        return (
            "failed",
            f"terminal evidence previous status does not match the declaration: "
            f"observed={observed_previous or 'missing'} expected={expected_previous}",
            facts,
        )
    observed_terminal = str(carrier.get("terminal_status") or "").strip().upper()
    if observed_terminal != expected_status:
        return (
            "failed",
            f"terminal evidence terminal status does not match the declaration: "
            f"observed={observed_terminal or 'missing'} expected={expected_status}",
            facts,
        )
    return "passed", None, facts


def _structured_current_trade_date_blockers_are_clear(value: Any) -> bool:
    """Accept only the scheduler's explicit, internally consistent CLEAR shape."""
    if not isinstance(value, dict):
        return False
    return bool(
        str(value.get("status") or "").strip().upper() == "CLEAR"
        and type(value.get("blocker_count")) is int
        and value["blocker_count"] == 0
        and type(value.get("observed_blocker_count")) is int
        and value["observed_blocker_count"] == 0
        and value.get("blockers") == []
        and value.get("execution_gate") is False
        and value.get("truncated") is False
    )


def _scheduler_failure_markers(payload: Any) -> list[str]:
    """Scan a scheduler/health-class payload for blocking or failure markers."""
    markers: list[str] = []
    queue: list[tuple[str, Any]] = [("", payload)]
    visited = 0
    while queue and visited < 400:
        prefix, node = queue.pop(0)
        if isinstance(node, dict):
            visited += 1
            for key, value in node.items():
                name = f"{prefix}.{key}" if prefix else str(key)
                lowered = str(key).lower()
                if lowered == _SCHEDULER_CURRENT_TRADE_DATE_BLOCKERS_KEY:
                    if isinstance(value, list):
                        if value:
                            markers.append(name)
                    elif not _structured_current_trade_date_blockers_are_clear(value):
                        markers.append(name)
                elif lowered in _SCHEDULER_BLOCKING_LIST_KEYS and value:
                    markers.append(name)
                elif lowered in _SCHEDULER_BLOCKING_VALUE_KEYS and isinstance(value, dict) and value:
                    markers.append(name)
                elif lowered in _SCHEDULER_REASON_KEYS and isinstance(value, str) and value.strip():
                    markers.append(name)
                elif lowered in _STATUS_FIELD_NAMES and isinstance(value, str) and value.strip().upper() in _HEALTH_FAILURE_STATUSES:
                    markers.append(f"{name}={value.strip()}")
                if isinstance(value, (dict, list)) and len(str(prefix).split(".")) < 8:
                    queue.append((name, value))
        elif isinstance(node, list):
            visited += 1
            for index, value in enumerate(node):
                if isinstance(value, (dict, list)):
                    queue.append((f"{prefix}[{index}]", value))
    return sorted(set(markers))


def _validate_scheduler_status(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Scheduler/health-class payloads must prove ok=true without blockers."""
    if not isinstance(payload, dict):
        return "failed", "scheduler payload must be a JSON object", {}
    if payload.get("ok") is not True:
        if "ok" not in payload:
            return "failed", "scheduler payload is missing the required ok=true envelope", {}
        return "failed", "scheduler payload reports ok=false", {}
    markers = _scheduler_failure_markers(payload)
    if markers:
        return (
            "failed",
            "scheduler payload exposes blocking/failure markers: " + ", ".join(markers[:8]),
            {"markers": markers},
        )
    return "passed", None, {}


def _validate_scheduler_verification_status(
    payload: Any,
    *,
    url: str,
) -> tuple[str, str | None, dict[str, Any]]:
    """Bind a scoped scheduler verdict to the exact broker/run subject in the probe URL."""

    verdict, reason, facts = _validate_scheduler_status(payload)
    if verdict != "passed":
        return verdict, reason, facts
    scheduler = payload.get("scheduler") if isinstance(payload, dict) else None
    if not isinstance(scheduler, dict):
        return "failed", "scheduler verification payload is missing scheduler object", {}
    if scheduler.get("schema_version") != SCHEDULER_VERIFICATION_STATUS_SCHEMA:
        return "failed", "scheduler verification payload schema_version is invalid", {}
    if scheduler.get("read_only") is not True:
        return "failed", "scheduler verification payload is not read-only", {}
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query, keep_blank_values=True)
    unknown_query = sorted(set(query) - {"broker_backend", "run_id"})
    if unknown_query:
        return "failed", f"scheduler verification probe has unknown subject query fields: {unknown_query}", {}
    if not query or any(len(values) != 1 for values in query.values()):
        return "failed", "scheduler verification probe must declare one value per broker/run subject field", {}
    broker_backend = query.get("broker_backend", [None])[0]
    run_id = query.get("run_id", [None])[0]
    if broker_backend not in {None, "local_sim", "minqmt_sim"}:
        return "failed", "scheduler verification probe broker_backend is invalid", {}
    if run_id is not None and _SCHEDULER_VERIFICATION_RUN_ID_RE.fullmatch(run_id) is None:
        return "failed", "scheduler verification probe run_id is invalid", {}
    if broker_backend is None and run_id is None:
        return "failed", "scheduler verification probe subject is missing", {}
    scope = scheduler.get("verification_scope")
    blockers = scheduler.get("current_trade_date_blockers")
    blocker_scope = blockers.get("verification_scope") if isinstance(blockers, dict) else None
    if not isinstance(scope, dict) or scope.get("schema_version") != SCHEDULER_VERIFICATION_SCOPE_SCHEMA:
        return "failed", "scheduler verification response scope schema is invalid", {}
    if scope.get("active") is not True:
        return "failed", "scheduler verification response scope is not active", {}
    observed_scope = {
        "broker_backend": scope.get("broker_backend"),
        "run_id": scope.get("run_id"),
    }
    if broker_backend is None and observed_scope["broker_backend"] not in {"local_sim", "minqmt_sim"}:
        return "failed", "scheduler verification response resolved broker_backend is invalid", {}
    expected_scope = {
        "broker_backend": broker_backend or observed_scope["broker_backend"],
        "run_id": run_id,
    }
    if observed_scope != expected_scope:
        return (
            "failed",
            "scheduler verification response scope does not match probe subject: "
            f"observed={observed_scope} expected={expected_scope}",
            {"verification_scope": observed_scope},
        )
    if blocker_scope != scope:
        return "failed", "scheduler verification blocker scope does not match response scope", {}
    return "passed", None, {"verification_scope": observed_scope}


def _validate_health_ok(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Health endpoints must report ok=true or an explicitly healthy status."""
    if not isinstance(payload, dict):
        return "failed", "health payload must be a JSON object", {}
    if payload.get("ok") is False:
        return "failed", "health payload reports ok=false", {}
    status = payload.get("status") or payload.get("state")
    if isinstance(status, str) and status.strip().upper() in _HEALTH_FAILURE_STATUSES:
        return "failed", f"health payload reports unhealthy status: {status.strip()}", {}
    errors = payload.get("errors")
    if errors:
        return "failed", "health payload reports errors", {}
    if payload.get("ok") is True:
        return "passed", None, {}
    if isinstance(status, str) and status.strip().lower() in _HEALTH_SUCCESS_STATUSES:
        return "passed", None, {"status": status.strip()}
    return "failed", "health payload is missing ok=true or an explicitly healthy status", {}


def _validate_collection_payload(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Collection endpoints must return a list or a non-error list envelope."""
    if isinstance(payload, list):
        return "passed", None, {"kind": "array", "size": len(payload)}
    if not isinstance(payload, dict):
        return "failed", "collection payload must be a JSON array or object", {}
    if payload.get("ok") is False:
        return "failed", "collection payload reports ok=false", {}
    errors = payload.get("errors")
    if errors:
        return "failed", "collection payload reports errors", {}
    for field in _STATUS_FIELD_NAMES:
        value = payload.get(field)
        if isinstance(value, str) and value.strip().upper() in _RUN_STATUS_FAILURE:
            return "failed", f"collection payload reports failure {field}={value.strip()}", {}
    for key in _COLLECTION_LIST_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return "passed", None, {"items_key": key, "size": len(value)}
    if payload.get("ok") is True:
        return "passed", None, {"ok": True}
    for field in _STATUS_FIELD_NAMES:
        value = payload.get(field)
        if isinstance(value, str) and value.strip().lower() in _HEALTH_SUCCESS_STATUSES:
            return "passed", None, {field: value.strip()}
    return "failed", "collection payload is missing an items array or an explicit ok/success marker", {}


def _validate_object_liveness(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Object endpoints must return a non-error object or array payload."""
    if isinstance(payload, list):
        return "passed", None, {"kind": "array", "size": len(payload)}
    if not isinstance(payload, dict):
        return "failed", "object payload must be a JSON object or array", {}
    if payload.get("ok") is False:
        return "failed", "object payload reports ok=false", {}
    errors = payload.get("errors")
    if errors:
        return "failed", "object payload reports errors", {}
    for field in _STATUS_FIELD_NAMES:
        value = payload.get(field)
        if isinstance(value, str) and value.strip().upper() in _RUN_STATUS_FAILURE:
            return "failed", f"object payload reports failure {field}={value.strip()}", {}
    return "passed", None, {"kind": "object"}


def _validate_openapi_document(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """The OpenAPI document smoke must prove the app serves its route schema."""
    if not isinstance(payload, dict):
        return "failed", "openapi payload must be a JSON object", {}
    version = payload.get("openapi")
    if not isinstance(version, str) or not version.strip():
        return "failed", "openapi payload is missing the openapi version field", {}
    paths = payload.get("paths")
    if not isinstance(paths, dict) or not paths:
        return "failed", "openapi payload is missing a non-empty paths mapping", {}
    return "passed", None, {"openapi": version.strip(), "paths": len(paths)}


def _validate_correlation_status(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Correlation status reports idle/computing plus explicit refresh errors."""
    if not isinstance(payload, dict):
        return "failed", "correlation status payload must be a JSON object", {}
    status = payload.get("status")
    if not isinstance(status, str) or not status.strip():
        return "failed", "correlation status payload is missing the status field", {}
    normalized = status.strip()
    if normalized.upper() in _HEALTH_FAILURE_STATUSES:
        return "failed", f"correlation status payload reports failure status: {normalized}", {}
    refresh_errors = payload.get("refresh_errors")
    if refresh_errors:
        count = len(refresh_errors) if isinstance(refresh_errors, list) else 1
        return "failed", "correlation status payload reports refresh errors", {"refresh_errors": count}
    if normalized.lower() in {"idle", "computing"}:
        return "passed", None, {"status": normalized}
    return "failed", f"correlation status payload reports unknown status: {normalized}", {}


def _validate_factor_lifecycle_detail(
    payload: Any,
    *,
    url: str,
) -> tuple[str, str | None, dict[str, Any]]:
    """Bind factor-library detail readback to an explicit lifecycle expectation."""

    if not isinstance(payload, dict):
        return "failed", "factor detail payload must be a JSON object", {}
    if payload.get("ok") is not True or payload.get("domain") != "factor_library":
        return "failed", "factor detail payload must report ok=true and domain=factor_library", {}
    factor = payload.get("factor")
    if not isinstance(factor, dict):
        return "failed", "factor detail payload is missing factor", {}

    parsed = urllib.parse.urlsplit(url)
    path_match = re.fullmatch(r"/api/v1/factor-library/factors/([^/]+)", parsed.path)
    if path_match is None:
        return "failed", "factor detail probe path is invalid", {}
    requested_name = urllib.parse.unquote(path_match.group(1)).strip()
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

    def required_query_value(name: str) -> tuple[str | None, str | None]:
        values = query.get(name) or []
        if len(values) != 1 or not str(values[0]).strip():
            return None, f"factor detail probe requires exactly one non-empty {name} query value"
        return str(values[0]).strip(), None

    requested_source, query_error = required_query_value("source")
    if query_error:
        return "failed", query_error, {}
    expected_available_text, query_error = required_query_value("expected_is_available")
    if query_error:
        return "failed", query_error, {}
    expected_available_normalized = str(expected_available_text).lower()
    if expected_available_normalized not in {"true", "false"}:
        return "failed", "factor detail expected_is_available must be true or false", {}
    expected_available = expected_available_normalized == "true"

    factor_id = factor.get("id")
    factor_name = factor.get("factor_name")
    source = factor.get("source")
    available = factor.get("is_available")
    if type(factor_id) is not int or factor_id <= 0:
        return "failed", "factor detail id must be a positive integer", {}
    if not isinstance(factor_name, str) or factor_name.strip() != requested_name:
        return "failed", "factor detail factor_name does not match the requested factor", {}
    if not isinstance(source, str) or source.strip() != requested_source:
        return "failed", "factor detail source does not match the requested source", {}
    if type(available) is not bool:
        return "failed", "factor detail is_available must be boolean", {}
    if available is not expected_available:
        return (
            "failed",
            f"factor detail availability mismatch: expected={expected_available} observed={available}",
            {},
        )

    facts: dict[str, Any] = {
        "factor_id": factor_id,
        "factor_name": factor_name.strip(),
        "source": source.strip(),
        "is_available": available,
    }
    expected_reason_code_values = query.get("expected_disable_reason_code") or []
    expected_batch_id_values = query.get("expected_disable_batch_id") or []
    if expected_available:
        if expected_reason_code_values or expected_batch_id_values:
            return "failed", "available factor detail must not declare disable expectations", facts
        return "passed", None, facts

    expected_reason_code, query_error = required_query_value("expected_disable_reason_code")
    if query_error:
        return "failed", query_error, facts
    expected_batch_id, query_error = required_query_value("expected_disable_batch_id")
    if query_error:
        return "failed", query_error, facts
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{2,127}", str(expected_reason_code)):
        return "failed", "factor detail expected_disable_reason_code is invalid", facts
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.\-]{2,127}", str(expected_batch_id)):
        return "failed", "factor detail expected_disable_batch_id is invalid", facts

    disable_reason = factor.get("disable_reason")
    disable_batch_id = factor.get("disable_batch_id")
    disable_at = factor.get("disable_at")
    rehab_candidate = factor.get("rehab_candidate")
    if not isinstance(disable_reason, str) or not disable_reason.strip():
        return "failed", "disabled factor detail is missing disable_reason", facts
    reason_prefix = f"{expected_reason_code}:"
    if not disable_reason.strip().startswith(reason_prefix):
        return "failed", "factor detail disable reason code does not match the declared expectation", facts
    observed_reason_code = expected_reason_code
    if not isinstance(disable_batch_id, str) or disable_batch_id.strip() != expected_batch_id:
        return "failed", "factor detail disable_batch_id does not match the declared expectation", facts
    if not isinstance(disable_at, str) or not disable_at.strip():
        return "failed", "disabled factor detail is missing disable_at", facts
    try:
        parsed_disable_at = datetime.fromisoformat(disable_at.strip().replace("Z", "+00:00"))
    except ValueError:
        return "failed", "factor detail disable_at must be a timezone-aware timestamp", facts
    if parsed_disable_at.tzinfo is None or parsed_disable_at.utcoffset() is None:
        return "failed", "factor detail disable_at must be a timezone-aware timestamp", facts
    if rehab_candidate is not False:
        return "failed", "quarantined factor detail must report rehab_candidate=false", facts

    facts.update(
        {
            "disable_reason_code": observed_reason_code,
            "disable_batch_id": disable_batch_id.strip(),
            "disable_at": parsed_disable_at.isoformat(),
            "rehab_candidate": rehab_candidate,
        }
    )
    return "passed", None, facts


_LOCALSIM_CUTOVER_REQUIRED_RELATIONS = frozenset({
    "paper_v2.simulation_account_v1",
    "paper_v2.legacy_localsim_account_lineage_v1",
    "paper_v2.localsim_replay_job_v1",
    "paper_v2.localsim_runtime_profile_v1",
    "paper_v2.localsim_runtime_profile_version_v1",
    "paper_v2.simulation_ledger_scope_v1",
    "strategy_pkg.strategy_runtime_release",
    "paper_v2.simulation_release_binding",
    "paper_v2.simulation_daily_run",
    "paper_v2.run",
    "paper_v2.intraday_snapshots",
})


def _validate_localsim_cutover_readiness(payload: Any) -> tuple[str, str | None, dict[str, Any]]:
    """Require the complete LocalSIM cutover authority to report a safe boundary."""
    if not isinstance(payload, dict):
        return "failed", "LocalSIM cutover-readiness payload must be a JSON object", {}
    if payload.get("ok") is not True:
        return "failed", "LocalSIM cutover-readiness payload must report ok=true", {}
    readiness = payload.get("readiness")
    if not isinstance(readiness, dict):
        return "failed", "LocalSIM cutover-readiness payload is missing readiness", {}
    if readiness.get("schema_version") != "localsim_cutover_readiness_v1":
        return "failed", "LocalSIM cutover-readiness schema_version is invalid", {}
    checked_at = readiness.get("checked_at")
    if not isinstance(checked_at, str) or not checked_at.strip():
        return "failed", "LocalSIM cutover-readiness checked_at must be a timezone-aware timestamp", {}
    try:
        parsed_checked_at = datetime.fromisoformat(checked_at.strip().replace("Z", "+00:00"))
    except ValueError:
        return "failed", "LocalSIM cutover-readiness checked_at must be a timezone-aware timestamp", {}
    if parsed_checked_at.tzinfo is None:
        return "failed", "LocalSIM cutover-readiness checked_at must be a timezone-aware timestamp", {}

    ready = readiness.get("ready")
    blockers = readiness.get("blockers")
    if type(ready) is not bool:
        return "failed", "LocalSIM cutover-readiness ready must be boolean", {}
    if not isinstance(blockers, list) or any(not isinstance(item, str) or not item.strip() for item in blockers):
        return "failed", "LocalSIM cutover-readiness blockers must be a list of non-empty strings", {}
    if ready != (not blockers):
        return "failed", "LocalSIM cutover-readiness ready and blockers are inconsistent", {}

    relation_presence = readiness.get("relation_presence")
    if (
        not isinstance(relation_presence, dict)
        or not relation_presence
        or any(not isinstance(name, str) or type(present) is not bool for name, present in relation_presence.items())
    ):
        return "failed", "LocalSIM cutover-readiness relation_presence is invalid", {}
    relation_names = set(relation_presence)
    if relation_names != _LOCALSIM_CUTOVER_REQUIRED_RELATIONS:
        missing = sorted(_LOCALSIM_CUTOVER_REQUIRED_RELATIONS - relation_names)
        unexpected = sorted(relation_names - _LOCALSIM_CUTOVER_REQUIRED_RELATIONS)
        return (
            "failed",
            f"LocalSIM cutover-readiness relation_presence keys are invalid: missing={missing}, unexpected={unexpected}",
            {},
        )
    missing_relations = sorted(name for name, present in relation_presence.items() if not present)

    count_fields = (
        "runtime_fk_count",
        "orphan_ledger_scope_count",
        "invalid_ledger_scope_count",
        "legacy_active_session_count",
        "legacy_auto_run_count",
        "legacy_sentinel_count",
        "in_flight_economic_run_count",
    )
    counts: dict[str, int] = {}
    for field in count_fields:
        value = readiness.get(field)
        if type(value) is not int or value < 0:
            return "failed", f"LocalSIM cutover-readiness {field} must be a non-negative integer", {}
        counts[field] = value

    retained_accounts = readiness.get("retained_legacy_account_ids")
    missing_lineages = readiness.get("missing_lineage_account_ids")
    for field, value in (
        ("retained_legacy_account_ids", retained_accounts),
        ("missing_lineage_account_ids", missing_lineages),
    ):
        if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
            return "failed", f"LocalSIM cutover-readiness {field} must be a list of non-empty strings", {}

    facts = {
        "ready": ready,
        "checked_at": parsed_checked_at.isoformat(),
        "blockers": list(blockers),
        "relation_count": len(relation_presence),
        "missing_relations": missing_relations,
        **counts,
        "retained_legacy_account_count": len(retained_accounts),
        "missing_lineage_account_count": len(missing_lineages),
    }
    invariant_failures: list[str] = []
    if missing_relations:
        invariant_failures.append(f"missing_relations={missing_relations}")
    if counts["runtime_fk_count"] != 2:
        invariant_failures.append(f"runtime_fk_count={counts['runtime_fk_count']}")
    for field in count_fields[1:]:
        if counts[field]:
            invariant_failures.append(f"{field}={counts[field]}")
    if missing_lineages:
        invariant_failures.append(f"missing_lineage_account_count={len(missing_lineages)}")
    if blockers:
        invariant_failures.append(f"blockers={blockers}")
    if invariant_failures:
        return "failed", "LocalSIM cutover is not ready: " + "; ".join(invariant_failures), facts
    return "passed", None, facts


_BUSINESS_SMOKE_SEMANTIC_CONTRACTS: tuple[tuple[re.Pattern[str], str, Any], ...] = (
    (re.compile(r"^/api/v1/health$"), "health_ok", _validate_health_ok),
    (re.compile(r"^/api/v1/qe-archive/health$"), "health_ok", _validate_health_ok),
    (re.compile(r"^/api/v1/simulation-runtime/scheduler/status$"), "scheduler_status", _validate_scheduler_status),
    (
        re.compile(r"^/api/v1/simulation-runtime/scheduler/verification-status$"),
        "scheduler_verification_status",
        _validate_scheduler_verification_status,
    ),
    (re.compile(r"^/api/v1/simulation-runtime/platform-diagnostics$"), "scheduler_status", _validate_scheduler_status),
    (
        re.compile(r"^/api/v1/simulation-runtime/localsim/cutover-readiness$"),
        "localsim_cutover_readiness",
        _validate_localsim_cutover_readiness,
    ),
    (re.compile(r"^/api/v1/advisory/forward/status$"), "scheduler_status", _validate_scheduler_status),
    (re.compile(r"^/api/v1/quantevolver/evolution/correlations/status$"), "correlation_status", _validate_correlation_status),
    (
        re.compile(r"^/api/v1/factor-library/factors/[^/]+$"),
        "factor_lifecycle_detail",
        _validate_factor_lifecycle_detail,
    ),
    (re.compile(r"^/api/v1/simulation-runtime/runs/[^/]+$"), "run_terminal_success", _validate_run_terminal_success),
    (re.compile(r"^/api/v1/simulation-runtime/runs/[^/]+/terminal-evidence$"), "run_terminal_evidence", _validate_run_terminal_evidence),
    (re.compile(r"^/api/v1/simulation-runtime/execution-plans/[^/]+$"), "run_terminal_success", _validate_run_terminal_success),
    (re.compile(r"^/api/v1/advisory/historical-range-batches/[^/]+$"), "run_terminal_success", _validate_run_terminal_success),
    (re.compile(r"^/api/v1/advisory/historical-range-operations/[^/]+$"), "run_terminal_success", _validate_run_terminal_success),
    (re.compile(r"^/api/v1/advisory/historical-range-runs/[^/]+$"), "run_terminal_success", _validate_run_terminal_success),
    (re.compile(r"^/api/v1/quantevolver/evolution/tasks/[^/]+$"), "run_terminal_success", _validate_run_terminal_success),
    (re.compile(r"^/api/v1/multi-alpha/combine-backtest/runs/[^/]+$"), "run_terminal_success", _validate_run_terminal_success),
    (re.compile(r"^/api/v1/simulation-runtime/runs$"), "collection", _validate_collection_payload),
    (re.compile(r"^/api/v1/advisory/historical-range-batches$"), "collection", _validate_collection_payload),
    (re.compile(r"^/api/v1/quantevolver/evolution/tasks$"), "collection", _validate_collection_payload),
    (re.compile(r"^/api/v1/quantevolver/experiments$"), "collection", _validate_collection_payload),
    (re.compile(r"^/api/v1/quantevolver/strategies$"), "collection", _validate_collection_payload),
    (re.compile(r"^/api/v1/multi-alpha/combine/tasks$"), "collection", _validate_collection_payload),
    (re.compile(r"^/api/v1/multi-alpha/combine-backtest/runs$"), "collection", _validate_collection_payload),
    (re.compile(r"^/api/ingestion/schedule$"), "collection", _validate_collection_payload),
    (re.compile(r"^/api/data-stats$"), "collection", _validate_collection_payload),
    (re.compile(r"^/api/v1/local-data/schedules$"), "collection", _validate_collection_payload),
    (re.compile(r"^/api/v1/dispatch/nodes$"), "collection", _validate_collection_payload),
    (re.compile(r"^/api/v1/quantevolver/evolution/tasks/[^/]+/.+$"), "object_liveness", _validate_object_liveness),
    (re.compile(r"^/api/v1/advisory/historical-range-options$"), "object_liveness", _validate_object_liveness),
    (re.compile(r"^/api/v1/advisory/programs/[^/]+/model-shadow$"), "object_liveness", _validate_object_liveness),
    (re.compile(r"^/api/v1/local-data/targets/[^/]+$"), "object_liveness", _validate_object_liveness),
    (re.compile(r"^/api/v1/qlib/config$"), "object_liveness", _validate_object_liveness),
    (re.compile(r"^/api/v1/runtime-contracts/[^/]+$"), "object_liveness", _validate_object_liveness),
    (re.compile(r"^/api/v1/validation/plans/[^/]+$"), "object_liveness", _validate_object_liveness),
    (re.compile(r"^/openapi\.json$"), "openapi_document", _validate_openapi_document),
)


def _business_smoke_semantic_contract(path: str) -> tuple[str, Any] | None:
    for pattern, contract_id, validator in _BUSINESS_SMOKE_SEMANTIC_CONTRACTS:
        if pattern.fullmatch(path):
            return contract_id, validator
    return None


def _evaluate_business_smoke_semantics(
    url: str,
    body: str,
    *,
    response_sha256: str,
    expectation: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Evaluate the target-owned semantic contract for a business-smoke payload.

    Returns (payload_schema, semantic) evidence. Fail-closed: unparsable
    bodies, unregistered endpoints, expectation/contract mismatches, and
    contract violations all yield a failed verdict; nothing falls back to
    HTTP-only success. The declared expectation (when any) is embedded in the
    semantic verdict with its digest so receipts stay bound to the exact
    declaration that produced them.
    """
    expectation_digest = _expectation_outcome_digest(expectation)
    path = urllib.parse.urlsplit(url).path or "/"
    contract = _business_smoke_semantic_contract(path)
    contract_id = contract[0] if contract else None
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, ValueError):
        schema = {"json": False, "kind": "none"}
        semantic = {
            "schema_version": BUSINESS_SMOKE_SEMANTIC_SCHEMA,
            "contract_id": contract_id,
            "verdict": "failed",
            "reason": "business-smoke payload is not parseable JSON",
            "facts": {},
            "response_sha256": response_sha256,
            "expectation": expectation,
            "expectation_digest": expectation_digest,
        }
        return schema, semantic
    schema = {"json": True, "kind": _json_payload_kind(payload)}
    if contract is None:
        semantic = {
            "schema_version": BUSINESS_SMOKE_SEMANTIC_SCHEMA,
            "contract_id": None,
            "verdict": "failed",
            "reason": f"no target-owned business-smoke semantic contract is registered for endpoint path: {path}",
            "facts": {},
            "response_sha256": response_sha256,
            "expectation": expectation,
            "expectation_digest": expectation_digest,
        }
        return schema, semantic
    contract_id, validator = contract
    if expectation is not None:
        if contract_id not in _EXPECTATION_CAPABLE_CONTRACT_IDS:
            semantic = {
                "schema_version": BUSINESS_SMOKE_SEMANTIC_SCHEMA,
                "contract_id": contract_id,
                "verdict": "failed",
                "reason": (
                    "a declared expected terminal outcome is not supported by the "
                    f"semantic contract resolved for this probe: {contract_id}"
                ),
                "facts": {},
                "response_sha256": response_sha256,
                "expectation": expectation,
                "expectation_digest": expectation_digest,
            }
            return schema, semantic
        if str(expectation.get("contract_id") or "").strip() != contract_id:
            semantic = {
                "schema_version": BUSINESS_SMOKE_SEMANTIC_SCHEMA,
                "contract_id": contract_id,
                "verdict": "failed",
                "reason": (
                    "declared expected terminal outcome targets a different semantic "
                    f"contract: declared={expectation.get('contract_id')!r} resolved={contract_id!r}"
                ),
                "facts": {},
                "response_sha256": response_sha256,
                "expectation": expectation,
                "expectation_digest": expectation_digest,
            }
            return schema, semantic
        verdict, reason, facts = validator(payload, expectation=expectation)
    elif contract_id in {"scheduler_verification_status", "factor_lifecycle_detail"}:
        verdict, reason, facts = validator(payload, url=url)
    else:
        verdict, reason, facts = validator(payload)
    semantic = {
        "schema_version": BUSINESS_SMOKE_SEMANTIC_SCHEMA,
        "contract_id": contract_id,
        "verdict": verdict,
        "reason": reason,
        "facts": facts,
        "response_sha256": response_sha256,
        "expectation": expectation,
        "expectation_digest": expectation_digest,
    }
    return schema, semantic


def _identity_values(body: str) -> set[str]:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return {body.strip()} if body.strip() else set()
    values: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if str(key).lower() in {"commit", "commit_sha", "git_sha", "identity", "merge_commit", "sha", "version"}:
                    if isinstance(item, (str, int)):
                        values.add(str(item).strip())
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(payload)
    return {item for item in values if item}


_FULL_GIT_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{40}$")
_RUNTIME_IDENTITY_PROOF_SCHEMA = "aistock_runtime_identity_proof_v1"


def _runtime_identity_proof_digest(proof: dict[str, Any]) -> str:
    encoded = json.dumps(proof, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _git_commit_is_ancestor(ancestor: str, descendant: str, *, root: Path) -> bool:
    return bool(
        _run_command(
            ["git", "merge-base", "--is-ancestor", ancestor, descendant],
            cwd=root,
            timeout=30,
        ).get("ok")
    )


def _origin_main_commit(*, root: Path) -> str | None:
    result = _run_command(
        ["git", "rev-parse", "--verify", "origin/main^{commit}"],
        cwd=root,
        timeout=30,
    )
    value = str(result.get("stdout") or "").strip()
    return value.lower() if result.get("ok") and _FULL_GIT_COMMIT_RE.fullmatch(value) else None


def _build_runtime_identity_proof(
    *,
    expected_identity: str,
    response_body: str,
    root: Path,
) -> tuple[dict[str, Any], str | None]:
    expected = expected_identity.strip()
    values = _identity_values(response_body)
    if expected in values:
        return (
            {
                "schema_version": _RUNTIME_IDENTITY_PROOF_SCHEMA,
                "mode": "exact",
                "expected_identity": expected,
                "observed_identity": expected,
                "origin_main_identity": None,
                "expected_is_ancestor": True,
                "observed_in_origin_main": True,
            },
            None,
        )

    candidates = sorted({value.lower() for value in values if _FULL_GIT_COMMIT_RE.fullmatch(value)})
    observed = candidates[0] if len(candidates) == 1 else None
    proof: dict[str, Any] = {
        "schema_version": _RUNTIME_IDENTITY_PROOF_SCHEMA,
        "mode": "origin_main_descendant",
        "expected_identity": expected,
        "observed_identity": observed,
        "origin_main_identity": None,
        "expected_is_ancestor": False,
        "observed_in_origin_main": False,
    }
    if not _FULL_GIT_COMMIT_RE.fullmatch(expected):
        return proof, "expected identity must be a full 40-hex Git commit for descendant proof"
    if not candidates:
        return proof, "runtime identity response does not contain the expected identity or one full Git commit"
    if len(candidates) != 1:
        return proof, f"runtime identity response is ambiguous: observed {len(candidates)} full Git commits"
    origin_main = _origin_main_commit(root=root)
    proof["origin_main_identity"] = origin_main
    if origin_main is None:
        return proof, "origin/main commit identity is unavailable for descendant proof"
    expected_is_ancestor = _git_commit_is_ancestor(expected.lower(), observed, root=root)
    proof["expected_is_ancestor"] = expected_is_ancestor
    if not expected_is_ancestor:
        return proof, "runtime identity is not a deployed origin/main descendant of the expected merge commit"
    observed_in_origin_main = _git_commit_is_ancestor(observed, origin_main, root=root)
    proof["observed_in_origin_main"] = observed_in_origin_main
    if not observed_in_origin_main:
        return proof, "runtime identity descendant is not contained in the verified origin/main lineage"
    return proof, None


def _runtime_identity_proof_errors(
    receipt: dict[str, Any],
    *,
    expected_identity: str,
    root: Path,
) -> list[str]:
    proof = receipt.get("runtime_identity_proof")
    if not isinstance(proof, dict):
        return ["post-restart receipt runtime identity proof is missing"]
    errors: list[str] = []
    if proof.get("schema_version") != _RUNTIME_IDENTITY_PROOF_SCHEMA:
        errors.append("post-restart receipt runtime identity proof schema mismatch")
    if receipt.get("runtime_identity_proof_digest") != _runtime_identity_proof_digest(proof):
        errors.append("post-restart receipt runtime identity proof digest mismatch")
    expected = expected_identity.strip()
    observed = str(proof.get("observed_identity") or "").strip()
    if str(proof.get("expected_identity") or "").strip() != expected:
        errors.append("post-restart receipt runtime identity proof expected identity mismatch")
    mode = proof.get("mode")
    if mode == "exact":
        if observed != expected or proof.get("expected_is_ancestor") is not True or proof.get("observed_in_origin_main") is not True:
            errors.append("post-restart receipt exact runtime identity proof is inconsistent")
        if proof.get("origin_main_identity") is not None:
            errors.append("post-restart receipt exact runtime identity proof must not claim an origin/main snapshot")
        return errors
    if mode != "origin_main_descendant":
        errors.append("post-restart receipt runtime identity proof mode is invalid")
        return errors
    origin_main = str(proof.get("origin_main_identity") or "").strip().lower()
    if not all(_FULL_GIT_COMMIT_RE.fullmatch(value) for value in (expected, observed, origin_main)):
        errors.append("post-restart receipt descendant identity proof requires full Git commits")
        return errors
    if proof.get("expected_is_ancestor") is not True or not _git_commit_is_ancestor(expected.lower(), observed.lower(), root=root):
        errors.append("post-restart receipt expected merge is not an ancestor of the observed runtime identity")
    if proof.get("observed_in_origin_main") is not True or not _git_commit_is_ancestor(observed.lower(), origin_main, root=root):
        errors.append("post-restart receipt observed runtime identity is not in the recorded origin/main lineage")
    current_origin_main = _origin_main_commit(root=root)
    if current_origin_main is None or not _git_commit_is_ancestor(origin_main, current_origin_main, root=root):
        errors.append("post-restart receipt recorded origin/main lineage is not contained in current origin/main")
    return errors


def _probe_evidence_digest(results: list[dict[str, Any]]) -> str:
    payload = []
    for item in results:
        transport = item.get("transport") if isinstance(item.get("transport"), dict) else {}
        semantic = item.get("semantic") if isinstance(item.get("semantic"), dict) else {}
        payload.append(
            {
                "name": item.get("name"),
                "url": item.get("url"),
                "status": item.get("status"),
                "status_code": item.get("status_code"),
                "response_sha256": item.get("response_sha256"),
                "response_bytes": item.get("response_bytes"),
                "transport_ok": transport.get("ok"),
                "semantic_contract_id": semantic.get("contract_id"),
                "semantic_verdict": semantic.get("verdict"),
                "semantic_reason": semantic.get("reason"),
                "semantic_expectation_digest": semantic.get("expectation_digest"),
            }
        )
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _post_restart_receipt_summary(receipt_path: Path, receipt: dict[str, Any]) -> dict[str, Any]:
    semantic = receipt.get("business_smoke_semantic") if isinstance(receipt.get("business_smoke_semantic"), dict) else {}
    return {
        "schema_version": RUNTIME_VERIFY_RECEIPT_SUMMARY_SCHEMA,
        "receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
        "bug_id": receipt.get("bug_id"),
        "target_id": receipt.get("target_id"),
        "generated_at": receipt.get("generated_at"),
        "expected_identity": receipt.get("expected_identity"),
        "observed_identity": receipt.get("observed_identity"),
        "runtime_identity_proof_digest": receipt.get("runtime_identity_proof_digest"),
        "contract_digest": receipt.get("contract_digest"),
        "expected_terminal_outcome_digest": receipt.get("expected_terminal_outcome_digest"),
        "catalog_sha256": receipt.get("catalog_sha256"),
        "probe_evidence_digest": receipt.get("probe_evidence_digest"),
        "post_restart_effective_gate": receipt.get("post_restart_effective_gate"),
        "mode": receipt.get("mode"),
        "business_smoke_semantic": {
            "contract_id": semantic.get("contract_id"),
            "verdict": semantic.get("verdict"),
            "reason": semantic.get("reason"),
        }
        if semantic
        else None,
        "response_content_persisted": False,
    }


def build_post_restart_verify(
    *,
    bug_id: str | None,
    issue_json: str | None,
    target_id: str,
    expected_identity: str | None,
    timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
    canonical_bug_id = str(record.get("bug_id") or bug_id or source_path.stem).upper()
    contract = build_runtime_contract(
        record=record,
        changed_files=resolve_record_runtime_changed_files(record),
        fresh_process_evidence=flow._as_list((record.get("runtime_contract") or {}).get("fresh_process_evidence"))
        if isinstance(record.get("runtime_contract"), dict)
        else [],
    )
    blocking = list(contract.get("blocking") or [])
    if contract.get("target_id") != target_id:
        blocking.append(f"target mismatch: contract={contract.get('target_id')} requested={target_id}")
    expected = str(expected_identity or record.get("fix_commit") or record.get("merge_commit") or "").strip()
    if not expected:
        blocking.append("expected merged runtime identity is required")
    probes = ((contract.get("target") or {}).get("probes") or {}) if isinstance(contract.get("target"), dict) else {}
    target = contract.get("target") if isinstance(contract.get("target"), dict) else {}
    allowed_origins = flow._as_list(target.get("probe_origins"))
    probe_mode = str(target.get("probe_mode") or "http")
    results: list[dict[str, Any]] = []
    if not blocking:
        if probe_mode == _DATASET_RELEASE_WORKER_HEARTBEAT_MODE:
            results = _read_dataset_release_worker_heartbeat_probes(target, timeout_seconds)
        else:
            for name in ("health_ref", "identity_ref", "business_smoke_ref"):
                results.append(
                    _read_only_http_probe(
                        name,
                        str(probes[name]),
                        allowed_origins=allowed_origins,
                        timeout_seconds=timeout_seconds,
                    )
                )
            database_ref = probes.get("database_readback_ref")
            if database_ref and str(database_ref).lower() != "not_required":
                results.append(
                    _read_only_http_probe(
                        "database_readback_ref",
                        str(database_ref),
                        allowed_origins=allowed_origins,
                        timeout_seconds=timeout_seconds,
                    )
                )
    smoke_result = next((item for item in results if item.get("name") == "business_smoke_ref"), None)
    expectation = contract.get("expected_terminal_outcome") if isinstance(contract.get("expected_terminal_outcome"), dict) else None
    business_smoke_semantic: dict[str, Any] | None = None
    if isinstance(smoke_result, dict) and smoke_result.get("status") == "passed":
        semantic = smoke_result.get("semantic") if isinstance(smoke_result.get("semantic"), dict) else None
        if semantic is None:
            schema_evidence, semantic = _evaluate_business_smoke_semantics(
                str(smoke_result.get("url") or ""),
                str(smoke_result.get("_response_body") or ""),
                response_sha256=str(smoke_result.get("response_sha256") or ""),
                expectation=expectation,
            )
            smoke_result["payload_schema"] = schema_evidence
            smoke_result["semantic"] = semantic
        business_smoke_semantic = semantic
        if semantic.get("verdict") != "passed":
            smoke_result["status"] = "failed"
            smoke_result["error"] = str(semantic.get("reason") or "business-smoke semantic contract failed")
    identity_result = next((item for item in results if item.get("name") == "identity_ref"), {})
    identity_proof, identity_error = _build_runtime_identity_proof(
        expected_identity=expected,
        response_body=str(identity_result.get("_response_body") or ""),
        root=REPO_ROOT,
    ) if results and expected else (
        {
            "schema_version": _RUNTIME_IDENTITY_PROOF_SCHEMA,
            "mode": "unverified",
            "expected_identity": expected or None,
            "observed_identity": None,
            "origin_main_identity": None,
            "expected_is_ancestor": False,
            "observed_in_origin_main": False,
        },
        "runtime identity proof was not executed",
    )
    identity_match = identity_error is None
    if results and identity_error:
        blocking.append(identity_error)
    failed_probes = [item.get("name") for item in results if item.get("status") != "passed"]
    if failed_probes:
        blocking.append(f"post-restart read-only probes failed: {failed_probes}")
    passed = not blocking and bool(results)
    sanitized_results = [{key: value for key, value in item.items() if key != "_response_body"} for item in results]
    required_probe_names = ["health_ref", "identity_ref", "business_smoke_ref"]
    if probes.get("database_readback_ref") and str(probes.get("database_readback_ref")).lower() != "not_required":
        required_probe_names.append("database_readback_ref")
    receipt_path = REPO_ROOT / WORKFLOW_ROOT / canonical_bug_id / "post-restart-verify.json"
    payload = {
        "schema_version": RUNTIME_VERIFY_RECEIPT_SCHEMA,
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "target_id": target_id,
        "mode": "read_only",
        "process_control_performed": False,
        "tracked_files_written": False,
        "expected_identity": expected or None,
        "observed_identity": identity_proof.get("observed_identity"),
        "runtime_identity_proof": identity_proof,
        "runtime_identity_proof_digest": _runtime_identity_proof_digest(identity_proof),
        "contract_digest": _runtime_contract_digest(contract),
        "expected_terminal_outcome": expectation,
        "expected_terminal_outcome_digest": _expectation_outcome_digest(expectation),
        "catalog_sha256": _runtime_catalog_sha256(root=REPO_ROOT),
        "required_probe_names": required_probe_names,
        "runtime_identity_match": identity_match,
        "post_restart_effective_gate": "passed" if passed else "failed",
        "workflow_gate": "verified" if passed else "blocked",
        "business_smoke_semantic": business_smoke_semantic,
        "probes": sanitized_results,
        "probe_evidence_digest": _probe_evidence_digest(sanitized_results),
        "blocking": flow._unique_strings(blocking),
        "receipt_path": _repo_rel(receipt_path),
    }
    _write_json(receipt_path, payload)
    _write_state(
        canonical_bug_id,
        state="runtime_verified" if passed else "fixed_source_pending_user_restart",
        post_restart_effective_gate=payload["post_restart_effective_gate"],
        runtime_identity_match=identity_match,
        post_restart_receipt=_repo_rel(receipt_path),
        next_actions=["run_close_sync_with_post_restart_receipt"] if passed else ["user_restart_then_rerun_post_restart_verify"],
        stop_reason=None if passed else "; ".join(payload["blocking"]),
    )
    return payload


def _git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
    cwd = cwd or REPO_ROOT
    proc = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
        env=_subprocess_env(["git", *args]),
    )
    if check and proc.returncode != 0:
        raise WorkflowError(proc.stderr.strip() or proc.stdout.strip() or f"git {' '.join(args)} failed")
    return proc.stdout.strip()


def _git_fetch_with_transport_retry(
    args: list[str],
    *,
    cwd: Path | None = None,
    attempts: int = 2,
    timeout: int = 60,
) -> str:
    """Retry only transport-class failures for an idempotent git fetch."""

    total = max(1, int(attempts))
    for index in range(total):
        result = _run_command(["git", "fetch", *args], cwd=cwd or REPO_ROOT, timeout=timeout)
        if result.get("ok"):
            return str(result.get("stdout") or "").strip()
        message = str(result.get("stderr") or result.get("stdout") or "git fetch failed")
        if not _looks_like_github_transport_failure(message) or index + 1 >= total:
            raise WorkflowError(message)
        time.sleep(0.5 * (index + 1))
    raise AssertionError("unreachable git fetch retry state")


def _assert_no_user_backend_process_control(args: list[str]) -> None:
    if not args:
        return
    executable = Path(str(args[0])).name.lower()
    lowered = [str(item).strip().lower().replace("\\", "/") for item in args]
    command = " ".join(lowered)
    blocked = False
    if executable in {"taskkill", "taskkill.exe"}:
        blocked = True
    elif executable in {"sc", "sc.exe", "net", "net.exe"} and any(
        action in lowered[1:3] for action in {"start", "stop", "restart"}
    ):
        blocked = True
    elif executable in {"systemctl", "docker"} and any(
        action in lowered[1:3] for action in {"start", "stop", "restart", "kill"}
    ):
        blocked = True
    elif executable in {"python", "python.exe", "py", "py.exe"} and (
        any(item.endswith("scripts/_restart_backend.py") for item in lowered[1:])
        or any(item.endswith("backend/main.py") for item in lowered[1:])
        or ("-m" in lowered and "uvicorn" in lowered and "backend.main:app" in command)
    ):
        blocked = True
    elif executable in {"uvicorn", "uvicorn.exe"} and "backend.main:app" in command:
        blocked = True
    elif executable in {"powershell", "powershell.exe", "pwsh", "pwsh.exe", "cmd", "cmd.exe"}:
        shell_process_control_tokens = (
            "restart" + "-service",
            "stop" + "-process",
            "start" + "-process",
            "task" + "kill",
            "scripts/_restart_" + "backend.py",
            "uvicorn " + "backend.main:app",
            "python " + "backend/main.py",
            "python.exe " + "backend/main.py",
        )
        blocked = any(token in command for token in shell_process_control_tokens)
    if blocked:
        raise WorkflowError(
            "workflow-owned user backend process control is forbidden; emit the catalog runbook and wait for the user"
        )


def _owned_process_creation_options() -> dict[str, Any]:
    if os.name == "nt":
        return {
            "creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW,
        }
    return {"start_new_session": True}


def _terminate_owned_process_tree(proc: subprocess.Popen[str], *, timeout: float = 10.0) -> dict[str, Any]:
    """Terminate only the process tree created by this workflow command."""

    if proc.poll() is not None:
        return {"attempted": False, "method": "already_exited", "returncode": proc.returncode}
    if os.name == "nt":
        try:
            killed = subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
                timeout=timeout,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            if proc.poll() is None:
                with contextlib.suppress(OSError):
                    proc.kill()
            return {
                "attempted": True,
                "method": "windows_taskkill_tree",
                "returncode": killed.returncode,
                "stderr": killed.stderr.strip(),
            }
        except (OSError, subprocess.TimeoutExpired) as exc:
            with contextlib.suppress(OSError):
                proc.kill()
            return {
                "attempted": True,
                "method": "windows_taskkill_tree_fallback_kill",
                "returncode": None,
                "stderr": str(exc),
            }
    try:
        os.killpg(proc.pid, signal.SIGKILL)
        return {"attempted": True, "method": "posix_process_group_kill", "returncode": 0}
    except (OSError, ProcessLookupError) as exc:
        with contextlib.suppress(OSError):
            proc.kill()
        return {
            "attempted": True,
            "method": "posix_process_group_fallback_kill",
            "returncode": None,
            "stderr": str(exc),
        }


def _run_command(args: list[str], cwd: Path | None = None, timeout: int = 30) -> dict[str, Any]:
    cwd = cwd or REPO_ROOT
    _assert_no_user_backend_process_control(args)
    proc: subprocess.Popen[str] | None = None
    try:
        proc = subprocess.Popen(  # lifecycle timeout is enforced by communicate(timeout=timeout) below
            args,
            cwd=str(cwd),
            text=True,
            encoding="utf-8",
            errors="replace",
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_subprocess_env(args),
            **_owned_process_creation_options(),
        )
        stdout, stderr = proc.communicate(timeout=timeout)
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout": stdout.strip(),
            "stderr": stderr.strip(),
        }
    except subprocess.TimeoutExpired as exc:
        termination = _terminate_owned_process_tree(proc) if proc is not None else {"attempted": False, "method": "not_started"}
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        if proc is not None:
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                final_stdout, final_stderr = proc.communicate(timeout=5)
                stdout = final_stdout or stdout
                stderr = final_stderr or stderr
        message = f"command timed out after {timeout} seconds"
        if str(stderr).strip():
            message = f"{message}: {str(stderr).strip()}"
        return {
            "ok": False,
            "returncode": None,
            "stdout": str(stdout).strip(),
            "stderr": message,
            "timed_out": True,
            "timeout_seconds": timeout,
            "termination": termination,
        }
    except Exception as exc:
        termination = None
        if proc is not None and proc.poll() is None:
            termination = _terminate_owned_process_tree(proc)
        return {
            "ok": False,
            "returncode": None,
            "stdout": "",
            "stderr": str(exc),
            "termination": termination,
        }


def _run_read_command_with_retry(
    args: list[str],
    *,
    cwd: Path | None = None,
    timeout: int = 30,
    attempts: int = 3,
) -> dict[str, Any]:
    """Retry a bounded idempotent remote/read command with short backoff."""

    total = max(1, int(attempts))
    last: dict[str, Any] = {"ok": False, "returncode": None, "stdout": "", "stderr": "not run"}
    for index in range(total):
        last = _run_command(args, cwd=cwd, timeout=timeout)
        if last.get("ok"):
            return {**last, "attempts": index + 1}
        if index + 1 < total:
            time.sleep(0.5 * (index + 1))
    return {**last, "attempts": total}


def _subprocess_env(args: list[str]) -> dict[str, str] | None:
    """Return a safe environment for workflow child processes."""
    if not args or Path(str(args[0])).name.lower() != "git":
        return None
    env = os.environ.copy()
    shell = env.get("SHELL", "")
    if os.name == "nt" and shell:
        shell_name = Path(shell).name.lower()
        if shell_name in {"powershell.exe", "pwsh.exe", "cmd.exe"}:
            env.pop("SHELL", None)
    return env


def _parse_git_porcelain_path(line: str) -> str:
    raw = line.rstrip()
    if not raw.strip():
        return ""
    if len(raw) >= 3 and raw[2] == " ":
        path = raw[3:]
    elif len(raw) >= 2 and raw[1] == " ":
        # _run_command strips stdout; recover paths from lines like "M path".
        path = raw[2:]
    else:
        path = raw[3:] if len(raw) > 3 else raw
    if " -> " in path:
        path = path.split(" -> ", 1)[1]
    return path.strip().strip('"')


def _workflow_dir(bug_id: str, root: Path | None = None) -> Path:
    return (root or REPO_ROOT) / WORKFLOW_ROOT / bug_id


def _state_path(bug_id: str, root: Path | None = None) -> Path:
    return _workflow_dir(bug_id, root) / "state.json"


def _events_path(bug_id: str, root: Path | None = None) -> Path:
    return _workflow_dir(bug_id, root) / "events.jsonl"


def _remove_synthetic_smoke_workflow_dir(path: Path, bug_id: str) -> None:
    canonical_bug_id = bug_id.strip().upper()
    is_synthetic = canonical_bug_id == "BUG-000" or re.fullmatch(r"BUG-9\d{15,17}", canonical_bug_id)
    expected = (REPO_ROOT / WORKFLOW_ROOT / canonical_bug_id).resolve()
    if not is_synthetic or path.resolve() != expected:
        raise WorkflowError("refusing to remove non-synthetic workflow-smoke state")
    if not path.exists():
        return
    if not path.is_dir() or path.is_symlink():
        raise WorkflowError(f"refusing to remove unsafe workflow-smoke path: {path}")
    shutil.rmtree(path)


def _synthetic_smoke_bug_id() -> str:
    return f"BUG-9{os.getpid() % 100000:05d}{time.time_ns() % 10_000_000_000:010d}"


def _cleanup_synthetic_smoke_artifacts(paths: list[Path], bug_id: str) -> list[str]:
    warnings: list[str] = []
    canonical_bug_id = bug_id.strip().upper()
    for path in paths:
        try:
            if path.is_dir():
                _remove_synthetic_smoke_workflow_dir(path, canonical_bug_id)
            elif path.exists():
                expected_parent = (REPO_ROOT / WORKFLOW_ROOT / "smoke").resolve()
                expected_name = f"synthetic-{canonical_bug_id}.json"
                if path.resolve().parent != expected_parent or path.name != expected_name:
                    raise WorkflowError("refusing to remove non-synthetic workflow-smoke issue file")
                path.unlink()
        except FileNotFoundError:
            continue
        except OSError as exc:
            warnings.append(f"synthetic workflow-smoke cleanup skipped for {_repo_rel(path)}: {exc}")
    return warnings


def _task_card_json_path(bug_id: str, root: Path | None = None) -> Path:
    return _workflow_dir(bug_id, root) / "task-card.json"


def _task_card_md_path(bug_id: str, root: Path | None = None) -> Path:
    return _workflow_dir(bug_id, root) / "task-card.md"


def _active_index_path(root: Path | None = None) -> Path:
    return (root or REPO_ROOT) / WORKFLOW_ROOT / "index" / "active_bugs.json"


def _active_index_lock_path(index_path: Path) -> Path:
    override = os.environ.get("AISTOCK_ACTIVE_INDEX_LOCK_ROOT")
    lock_root = Path(override) if override else _default_worktree_root() / ".locks"
    try:
        identity = os.path.normcase(str(index_path.resolve()))
    except OSError:
        identity = os.path.normcase(str(index_path.absolute()))
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
    return lock_root / f"active-bugs-index-{digest}.lock"


def _load_state(bug_id: str, root: Path | None = None) -> dict[str, Any] | None:
    path = _state_path(bug_id, root)
    if not path.exists():
        return None
    return _load_json(path)


def _append_event(
    bug_id: str,
    *,
    event: str,
    state: str,
    actor: str = "aistock_issue_workflow.py",
    command: str | None = None,
    cwd: Path | None = None,
    result: str = "ok",
    evidence: dict[str, Any] | None = None,
    root: Path | None = None,
    duration_seconds: float | None = None,
) -> dict[str, Any]:
    command_text = str(command or "").strip()
    rtk_marker = os.environ.get("AISTOCK_RTK_USED", "").strip().lower()
    if command_text.lower().startswith(("rtk ", "rtk.exe ")):
        rtk_used: bool | str = True
    elif rtk_marker in {"1", "true", "yes", "on"}:
        rtk_used = True
    elif rtk_marker in {"0", "false", "no", "off"}:
        rtk_used = False
    else:
        rtk_used = "not_recorded"
    event_payload = {
        "timestamp": _utc_now(),
        "client": os.environ.get("AISTOCK_WORKFLOW_CLIENT") or os.environ.get("CODEX_WORKFLOW_CLIENT") or "unknown",
        "actor": actor,
        "event": event,
        "state": state,
        "command": command,
        "cwd": str((cwd or root or REPO_ROOT).resolve()),
        "duration_seconds": round(duration_seconds, 3) if duration_seconds is not None else None,
        "result": result,
        "evidence": evidence or {},
        "tooling": {
            "rtk_used": rtk_used,
            "rtk_version": os.environ.get("AISTOCK_RTK_VERSION") or "not_recorded",
            "rtk_fallback": os.environ.get("AISTOCK_RTK_FALLBACK") or "not_recorded",
        },
    }
    path = _events_path(bug_id, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event_payload, ensure_ascii=False, sort_keys=True) + "\n")
    return event_payload


def _parse_utc_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _event_phase(event: dict[str, Any]) -> str:
    raw = str(event.get("event") or event.get("state") or "unknown")
    if raw.startswith("state:"):
        raw = raw.split(":", 1)[1]
    if raw.startswith("command:"):
        return raw.split(":", 1)[1] or "command"
    if raw in {"active_worktree_decision"}:
        return "active_worktree_guard"
    return raw or "unknown"


def _read_events(bug_id: str, root: Path | None = None) -> list[dict[str, Any]]:
    path = _events_path(bug_id, root)
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            events.append(payload)
    return events


def _prior_postmortem_paths(bug_id: str, roots: list[Path]) -> list[Path]:
    candidates: list[Path] = []
    known_roots = roots + [REPO_ROOT, _canonical_root()]
    for root in known_roots:
        candidates.extend(
            [
                root / WORKFLOW_ROOT / bug_id / "postmortem.json",
                root / WORKFLOW_ROOT / bug_id / "postmortem-pre-cleanup.json",
            ]
        )
    worktree_root = _default_worktree_root()
    if worktree_root.exists():
        candidates.extend(worktree_root.glob(f"*/{WORKFLOW_ROOT.as_posix()}/{bug_id}/postmortem*.json"))
    return _unique_existing_paths(candidates)


def _load_prior_postmortem(bug_id: str, roots: list[Path]) -> dict[str, Any] | None:
    paths = _prior_postmortem_paths(bug_id, roots)
    if not paths:
        return None
    def sort_key(path: Path) -> tuple[float, float, str]:
        try:
            payload = _load_json(path)
            timing = payload.get("timing_summary") if isinstance(payload, dict) else {}
            event_count = float((timing or {}).get("event_count") or 0)
            known_duration = float((timing or {}).get("known_duration_seconds") or 0)
            return (event_count, known_duration, str(path))
        except OSError:
            return (0.0, 0.0, str(path))
        except WorkflowError:
            return (0.0, 0.0, str(path))

    path = sorted(paths, key=sort_key)[-1]
    try:
        payload = _load_json(path)
    except WorkflowError:
        return None
    payload = dict(payload)
    payload.setdefault("schema_version", "aistock_issue_workflow_postmortem_v1")
    payload["workflow_gate"] = payload.get("workflow_gate") or "artifact_fallback"
    payload["artifact_fallback"] = {
        "reason": "workflow_state_missing_or_cleaned",
        "path": str(path),
    }
    return payload


def _workflow_timing_summary(bug_id: str, root: Path | None = None) -> dict[str, Any]:
    events = _read_events(bug_id, root)
    events = sorted(events, key=lambda item: str(item.get("timestamp") or ""))
    phases: dict[str, dict[str, Any]] = {}
    previous_ts: datetime | None = None
    previous_phase: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    known_duration = 0.0
    inferred_duration = 0.0

    for event in events:
        ts = _parse_utc_timestamp(str(event.get("timestamp") or ""))
        if ts:
            started_at = started_at or event.get("timestamp")
            ended_at = event.get("timestamp")
        phase = _event_phase(event)
        bucket = phases.setdefault(
            phase,
            {
                "event_count": 0,
                "known_duration_seconds": 0.0,
                "inferred_since_previous_seconds": 0.0,
                "inferred_until_next_seconds": 0.0,
                "first_at": event.get("timestamp"),
                "last_at": event.get("timestamp"),
            },
        )
        bucket["event_count"] += 1
        bucket["last_at"] = event.get("timestamp")
        duration = event.get("duration_seconds")
        if isinstance(duration, (int, float)):
            bucket["known_duration_seconds"] = round(float(bucket["known_duration_seconds"]) + float(duration), 3)
            known_duration += float(duration)
        if ts and previous_ts and previous_phase:
            delta = max(0.0, (ts - previous_ts).total_seconds())
            previous_bucket = phases[previous_phase]
            previous_bucket["inferred_until_next_seconds"] = round(
                float(previous_bucket["inferred_until_next_seconds"]) + delta,
                3,
            )
            inferred_duration += delta
        if ts:
            previous_ts = ts
            previous_phase = phase

    rtk_used_count = sum(
        1 for event in events if (event.get("tooling") or {}).get("rtk_used") is True
    )
    rtk_fallback_count = sum(
        1
        for event in events
        if str((event.get("tooling") or {}).get("rtk_fallback") or "not_recorded") != "not_recorded"
    )
    rtk_not_recorded_count = sum(
        1 for event in events if (event.get("tooling") or {}).get("rtk_used") == "not_recorded"
    )

    queue_seconds = _phase_seconds(phases, "discovered")
    context_seconds = _phase_seconds(phases, "context_ready")
    active_fix_seconds = _phase_seconds(phases, "fix_in_progress") + _phase_seconds(phases, "fix_applied") + context_seconds
    local_validation_seconds = _phase_seconds(phases, "validation_planned") + _phase_seconds(phases, "validation_running") + _phase_seconds(phases, "validation_passed")
    pr_ci_seconds = _phase_seconds(phases, "pr_opened") + _phase_seconds(phases, "ci_running") + _phase_seconds(phases, "ci_green") + _phase_seconds(phases, "gh_pr_create")
    merge_aftercare_seconds = sum(_phase_seconds(phases, phase) for phase in ("merged", "close_synced", "cleanup_done", "complete", "close_sync_apply", "close_sync_persisted"))

    return {
        "schema_version": "aistock_issue_workflow_timing_summary_v1",
        "bug_id": bug_id,
        "event_count": len(events),
        "started_at": started_at,
        "ended_at": ended_at,
        "known_duration_seconds": round(known_duration, 3),
        "inferred_elapsed_seconds": round(inferred_duration, 3),
        "phases": phases,
        "queue_seconds": round(queue_seconds, 3) if queue_seconds else None,
        "active_fix_seconds": round(active_fix_seconds, 3) if active_fix_seconds else None,
        "local_validation_seconds": round(local_validation_seconds, 3) if local_validation_seconds else None,
        "pr_ci_seconds": round(pr_ci_seconds, 3) if pr_ci_seconds else None,
        "merge_aftercare_seconds": round(merge_aftercare_seconds, 3) if merge_aftercare_seconds else None,
        "code_repair_seconds": round(active_fix_seconds, 3) if active_fix_seconds else None,
        "rtk_telemetry": {
            "used_event_count": rtk_used_count,
            "fallback_event_count": rtk_fallback_count,
            "not_recorded_event_count": rtk_not_recorded_count,
            "status": "recorded" if rtk_used_count or rtk_fallback_count else "not_recorded",
        },
        "notes": [
            "known_duration_seconds comes from command-level telemetry when available",
            "inferred_elapsed_seconds is wall-clock distance between recorded events and may include human/CI wait time",
            "code_repair_seconds is an upper bound between automatic repair-start and finish-plan boundaries and may include local validation run before finish",
        ],
    }




def _augment_timing_with_issue_record(timing: dict[str, Any], state: dict[str, Any], root: Path) -> dict[str, Any]:
    source_path = _state_issue_json_path(root, state)
    if not source_path or not source_path.exists():
        return timing
    try:
        record = _load_json(source_path)
    except WorkflowError:
        return timing
    created_at = _parse_utc_timestamp(str(record.get("created_at") or record.get("first_seen_at") or ""))
    started_at = _parse_utc_timestamp(str(timing.get("started_at") or ""))
    if created_at and started_at and started_at > created_at:
        timing = dict(timing)
        queue_seconds = round((started_at - created_at).total_seconds(), 3)
        existing = float(timing.get("queue_seconds") or 0)
        timing["queue_seconds"] = max(existing, queue_seconds)
        timing["issue_created_at"] = record.get("created_at") or record.get("first_seen_at")
        timing["active_work_started_at"] = timing.get("started_at")
        timing.setdefault("notes", []).append(
            "queue_seconds uses BUG created_at to first workflow event when available, so queue time is not hidden."
        )
    return timing


def _phase_seconds(phases: dict[str, Any], phase: str) -> float:
    item = phases.get(phase)
    if not isinstance(item, dict):
        return 0.0
    return max(
        float(item.get("known_duration_seconds") or 0),
        float(item.get("inferred_until_next_seconds") or item.get("inferred_since_previous_seconds") or 0),
    )


def _phase_cost_table(timing: dict[str, Any]) -> list[dict[str, Any]]:
    phases = timing.get("phases") if isinstance(timing, dict) else {}
    if not isinstance(phases, dict):
        return []
    rows: list[dict[str, Any]] = []
    for phase, item in sorted(phases.items()):
        if not isinstance(item, dict):
            continue
        known = round(float(item.get("known_duration_seconds") or 0), 3)
        inferred = round(
            float(item.get("inferred_until_next_seconds") or item.get("inferred_since_previous_seconds") or 0),
            3,
        )
        rows.append(
            {
                "phase": str(phase),
                "event_count": int(item.get("event_count") or 0),
                "known_seconds": known,
                "inferred_seconds": inferred,
                "dominant_seconds": max(known, inferred),
            }
        )
    return rows


def _h6_summary(timing: dict[str, Any], context_metrics: dict[str, Any], artifact_metrics: dict[str, Any]) -> dict[str, Any]:
    phase_rows = _phase_cost_table(timing)
    top_phase = max(phase_rows, key=lambda item: item["dominant_seconds"], default=None)
    context_token_values = [
        int(item.get("estimated_tokens") or 0)
        for item in context_metrics.values()
        if isinstance(item, dict) and item.get("exists") is not False and "estimated_tokens" in item
    ]
    artifact_token_values = [
        int(item.get("estimated_tokens") or 0)
        for item in artifact_metrics.values()
        if isinstance(item, dict) and item.get("exists") is not False and "estimated_tokens" in item
    ]
    context_tokens: int | None = sum(context_token_values) if context_token_values else None
    artifact_tokens: int | None = sum(artifact_token_values) if artifact_token_values else None
    total_tokens: int | None = (
        (context_tokens or 0) + (artifact_tokens or 0)
        if context_tokens is not None or artifact_tokens is not None
        else None
    )
    return {
        "schema_version": "aistock_issue_workflow_h6_summary_v1",
        "event_count": timing.get("event_count"),
        "known_duration_seconds": timing.get("known_duration_seconds"),
        "inferred_elapsed_seconds": timing.get("inferred_elapsed_seconds"),
        "top_phase": top_phase,
        "context_estimated_tokens": context_tokens,
        "artifact_estimated_tokens": artifact_tokens,
        "total_estimated_tokens": total_tokens,
        "token_usage_status": "estimated" if total_tokens is not None else "unknown",
        "queue_seconds": timing.get("queue_seconds"),
        "active_fix_seconds": timing.get("active_fix_seconds"),
        "local_validation_seconds": timing.get("local_validation_seconds"),
        "pr_ci_seconds": timing.get("pr_ci_seconds"),
        "merge_aftercare_seconds": timing.get("merge_aftercare_seconds"),
        "code_repair_seconds": timing.get("code_repair_seconds"),
        "rtk_telemetry": timing.get("rtk_telemetry") or {"status": "not_recorded"},
        "code_repair_note": "active_fix_seconds is a phase-bound upper bound, not exact editor-only time; validation executed before finish may be included",
    }


def _code_intelligence_readiness(code_intel: dict[str, Any] | None) -> dict[str, Any]:
    code_intel = code_intel or {}
    codegraph = code_intel.get("codegraph") if isinstance(code_intel.get("codegraph"), dict) else {}
    freshness = code_intel.get("codegraph_freshness") if isinstance(code_intel.get("codegraph_freshness"), dict) else {}
    latest_freshness = freshness.get("latest") if isinstance(freshness.get("latest"), dict) else {}
    ua = code_intel.get("understand_anything") if isinstance(code_intel.get("understand_anything"), dict) else {}
    bootstrap = code_intel.get("bootstrap_commands") if isinstance(code_intel.get("bootstrap_commands"), dict) else {}
    context = code_intel.get("context") if isinstance(code_intel.get("context"), dict) else {}
    fallback = context.get("fallback") if isinstance(context.get("fallback"), dict) else {}
    fallback_used = bool(
        code_intel.get("fallback_used")
        or fallback.get("used")
        or str(code_intel.get("status") or "").lower() == "fallback"
        or str(codegraph.get("status") or "").lower() in {"unavailable", "missing_index", "stale"}
    )
    codegraph_status = str(codegraph.get("status") or code_intel.get("status") or "unknown")
    fallback_reason = fallback.get("reason") or code_intel.get("fallback_reason")
    if not fallback_reason and fallback_used:
        fallback_reason = "codegraph_" + codegraph_status
    next_command = None
    if fallback_used:
        next_command = codegraph.get("bootstrap_command") or bootstrap.get("codegraph") or "codegraph init -i"
    ua_status = str(ua.get("status") or "unknown")
    ua_next_command = None
    if ua_status == "not_configured":
        ua_next_command = ua.get("configure_command") or bootstrap.get("understand_anything_configure")
    elif not ua.get("graph_exists"):
        ua_next_command = ua.get("generate_graph_command") or bootstrap.get("understand_anything_generate_graph")
    return {
        "schema_version": "aistock_issue_workflow_h7_code_intelligence_readiness_v1",
        "workflow_gate": "warning" if fallback_used else "ready",
        "status": code_intel.get("status") or codegraph_status,
        "codegraph_status": codegraph_status,
        "codegraph_freshness": latest_freshness.get("freshness"),
        "codegraph_freshness_ref": latest_freshness.get("artifact_path"),
        "understand_anything_status": ua_status,
        "understand_anything_graph_exists": ua.get("graph_exists"),
        "understand_anything_next_command": ua_next_command,
        "fallback_used": fallback_used,
        "fallback_reason": fallback_reason,
        "readiness_next_command": next_command,
        "blocking_for_issue_workflow": False,
    }


def _code_intelligence_efficiency_summary(code_intel: dict[str, Any] | None) -> dict[str, Any]:
    """Record graph-first usage without expanding graph artifacts in successful output."""

    code_intel = code_intel or {}
    affected_tests = code_intel.get("affected_tests") if isinstance(code_intel.get("affected_tests"), dict) else {}
    context = code_intel.get("context") if isinstance(code_intel.get("context"), dict) else {}
    fallback = context.get("fallback") if isinstance(context.get("fallback"), dict) else {}
    ua = code_intel.get("understand_anything") if isinstance(code_intel.get("understand_anything"), dict) else {}
    context_ref = code_intel.get("context_ref")
    affected_tests_ref = code_intel.get("affected_tests_ref")
    ua_summary_ref = code_intel.get("understand_anything_summary_ref")
    graph_refs = [item for item in (context_ref, affected_tests_ref, ua_summary_ref) if item]
    fallback_used = bool(
        code_intel.get("fallback_used")
        or fallback.get("used")
        or str(code_intel.get("status") or "").lower() == "fallback"
    )
    fallback_reason = fallback.get("reason") or code_intel.get("fallback_reason")
    if not fallback_reason and fallback_used:
        fallback_reason = "graph_context_fallback"
    broad_scan_avoided = bool(graph_refs) and not fallback_used
    return {
        "schema_version": "aistock_issue_workflow_code_intelligence_efficiency_v1",
        "graph_first_context_used": broad_scan_avoided,
        "codegraph_context_ref": context_ref,
        "affected_tests_ref": affected_tests_ref,
        "understand_anything_summary_ref": ua_summary_ref,
        "affected_tests_count": code_intel.get("affected_tests_count") or len(affected_tests.get("suggested_tests") or []),
        "affected_quality": code_intel.get("affected_quality"),
        "fallback_used": fallback_used,
        "fallback_reason": fallback_reason,
        "broad_scan_avoided": broad_scan_avoided,
        "estimated_broad_scan_tokens_avoided": 8000 if broad_scan_avoided else None,
        "understand_anything_status": ua.get("status"),
        "full_graph_payload_included": False,
    }


def _code_intelligence_hint(root: Path | None = None) -> dict[str, Any]:
    freshness = code_intelligence.latest_codegraph_freshness(root or REPO_ROOT)
    latest = freshness.get("latest") if isinstance(freshness.get("latest"), dict) else {}
    freshness_value = latest.get("freshness") if latest else None
    return {
        "schema_version": "aistock_issue_workflow_code_intelligence_hint_v1",
        "workflow_gate": freshness.get("workflow_gate") or "warning",
        "blocking_for_issue_workflow": False,
        "latest_freshness": freshness_value,
        "artifact_path": latest.get("artifact_path") if latest else None,
        "summary_ref": latest.get("summary_ref") if latest else None,
        "consume_command": "python scripts/code_intelligence_adapter.py latest-freshness",
        "readiness_next_command": None
        if freshness_value == "fresh"
        else "python scripts/code_intelligence_adapter.py freshness --skip-external",
    }


def _active_index_entry(bug_id: str, state_payload: dict[str, Any], *, root: Path) -> dict[str, Any]:
    active_worktree = state_payload.get("worktree") or state_payload.get("cwd") or str(root)
    return {
        "bug_id": bug_id,
        "active_state": state_payload.get("state"),
        "branch": state_payload.get("branch"),
        "planned_branch": state_payload.get("planned_branch"),
        "worktree": active_worktree,
        "planned_worktree": state_payload.get("planned_worktree"),
        "pr_url": state_payload.get("pr_url"),
        "last_event_at": state_payload.get("updated_at"),
        "next_command": _next_command_for_state(bug_id, state_payload),
    }


def _rebuild_active_index_from_states(root: Path) -> dict[str, Any]:
    workflow_root = root / WORKFLOW_ROOT
    index: dict[str, Any] = {}
    invalid_states: list[str] = []
    for state_path in sorted(workflow_root.glob("BUG-*/state.json")):
        try:
            state_payload = _load_json(state_path)
        except (OSError, json.JSONDecodeError, WorkflowError):
            invalid_states.append(_repo_rel(state_path))
            continue
        bug_id = str(state_payload.get("bug_id") or state_path.parent.name).strip().upper()
        if not BUG_ID_RE.fullmatch(bug_id):
            invalid_states.append(_repo_rel(state_path))
            continue
        if _state_is_active(state_payload):
            index[bug_id] = _active_index_entry(bug_id, state_payload, root=root)
    if invalid_states:
        raise WorkflowError(
            "active BUG index recovery found invalid authoritative state file(s): "
            + ", ".join(invalid_states[:10])
        )
    return index


def _update_active_index(bug_id: str, state_payload: dict[str, Any], *, root: Path | None = None) -> dict[str, Any]:
    root = root or REPO_ROOT
    path = _active_index_path(root)
    lock = GlobalBugIdLock(
        _active_index_lock_path(path),
        timeout=30.0,
        process_is_alive=_process_id_is_alive,
    )
    try:
        with lock:
            if not path.exists():
                existing: dict[str, Any] = {
                    "schema_version": "aistock_issue_workflow_active_index_v1",
                    "active_bugs": _rebuild_active_index_from_states(root),
                }
            else:
                try:
                    existing = _load_json(path)
                except (OSError, json.JSONDecodeError, WorkflowError):
                    existing = {
                        "schema_version": "aistock_issue_workflow_active_index_v1",
                        "active_bugs": _rebuild_active_index_from_states(root),
                    }
            existing_index = existing.get("active_bugs")
            if existing_index is None:
                existing_index = existing
            if not isinstance(existing_index, dict):
                raise WorkflowError(f"active BUG index has invalid entries: {path}")
            index = dict(existing_index)
            key = bug_id.strip().upper()
            if not _state_is_active(state_payload):
                index.pop(key, None)
            else:
                index[key] = _active_index_entry(key, state_payload, root=root)
            payload = {
                "schema_version": "aistock_issue_workflow_active_index_v1",
                "updated_at": _utc_now(),
                "active_bugs": index,
            }
            _write_json(path, payload)
            return payload
    except BugIdLockError as exc:
        raise WorkflowError(f"active BUG index lock failed: {path}: {exc}") from exc


def _write_state(
    bug_id: str,
    *,
    state: str,
    root: Path | None = None,
    next_actions: list[str] | None = None,
    stop_reason: str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    previous = _load_state(bug_id, root) or {}
    payload = {
        **previous,
        **fields,
        "schema_version": "aistock_issue_workflow_state_v1",
        "bug_id": bug_id,
        "state": state,
        "updated_at": _utc_now(),
        "cwd": str(root.resolve()),
        "next_actions": next_actions or fields.get("next_actions") or [],
    }
    for key, value in fields.items():
        if value is None:
            payload.pop(key, None)
    if stop_reason:
        payload["stop_reason"] = stop_reason
    elif state not in {"blocked"}:
        payload.pop("stop_reason", None)
    _write_json(_state_path(bug_id, root), payload)
    _append_event(
        bug_id,
        event=f"state:{state}",
        state=state,
        root=root,
        evidence={key: payload.get(key) for key in ("branch", "worktree", "commit", "pr_url") if payload.get(key)},
    )
    _update_active_index(bug_id, payload, root=root)
    return payload


def _default_worktree_root() -> Path:
    override = os.environ.get("AISTOCK_WORKTREE_ROOT")
    if override:
        return Path(override)
    if REPO_ROOT.parent.name == "AIstock_worktrees":
        return REPO_ROOT.parent
    if REPO_ROOT.name.lower() == "aistock":
        return REPO_ROOT.parent / "AIstock_worktrees"
    return REPO_ROOT.parent / "AIstock_worktrees"


def _bug_files(bugs_root: Path | None = None) -> list[Path]:
    bugs_root = bugs_root or BUGS_ROOT
    return sorted(path for path in bugs_root.glob("*.json") if not path.name.startswith("."))


def _bugs_root(root: Path | None = None) -> Path:
    return (root / "tests" / "aistock_validation" / "bugs") if root else BUGS_ROOT


def _allocator_path(root: Path | None = None) -> Path:
    return _bugs_root(root) / ".bug_id_allocator.json"


def _bug_id_number(value: str | None) -> int | None:
    match = BUG_ID_RE.search(value or "")
    return int(match.group(1)) if match else None


def _bug_id_number_from_filename(value: str | None) -> int | None:
    match = BUG_ID_FILENAME_RE.search(value or "")
    return int(match.group(1)) if match else None


def _unique_existing_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    unique: list[Path] = []
    for path in paths:
        try:
            key = str(path.resolve()).lower()
        except OSError:
            key = str(path.absolute()).lower()
        if key in seen:
            continue
        seen.add(key)
        if path.exists():
            unique.append(path)
    return unique


def _path_identity(path: Path) -> str:
    try:
        return str(path.resolve()).lower()
    except OSError:
        return str(path.absolute()).lower()


def _same_path(left: Path, right: Path) -> bool:
    return _path_identity(left) == _path_identity(right)


def _strict_bug_id_scan_roots(root: Path | None = None) -> set[str]:
    repo_root = root or REPO_ROOT
    return {
        _path_identity(path)
        for path in (
            _bugs_root(repo_root),
            _bugs_root(REPO_ROOT),
            _bugs_root(_canonical_root()),
        )
    }


def _bug_id_scan_roots(root: Path | None = None) -> list[Path]:
    repo_root = root or REPO_ROOT
    candidates = [
        _bugs_root(repo_root),
        _bugs_root(REPO_ROOT),
        _bugs_root(_canonical_root()),
    ]
    # Active allocations are represented by the global reservation ledger.
    # Scanning every worktree re-reads hundreds of copies of the same BUG
    # registry and makes allocation cost grow as worktrees * BUG records.
    # Keep legacy/canonical recovery bounded to the current, invocation and
    # canonical registry roots; reservations cover in-flight task worktrees.
    return _unique_existing_paths(candidates)


def _bug_id_reservation_root() -> Path:
    override = os.environ.get("AISTOCK_BUG_ID_RESERVATION_ROOT")
    return Path(override) if override else _default_worktree_root() / ".locks" / "bug-id-reservations"


def _bug_id_state_path() -> Path:
    override = os.environ.get("AISTOCK_BUG_ID_STATE_PATH")
    return Path(override) if override else _default_worktree_root() / ".locks" / "bug-id-state.json"


def _scan_bug_id_reservations() -> list[dict[str, Any]]:
    reservation_root = _bug_id_reservation_root()
    if not reservation_root.exists():
        return []
    sources: list[dict[str, Any]] = []
    for path in sorted(reservation_root.glob("BUG-*.json")):
        number = _bug_id_number(path.name)
        if not number:
            continue
        sources.append(
            {
                "bug_id": f"BUG-{number:03d}",
                "number": number,
                "kind": "reservation",
                "source": str(path),
            }
        )
    return sources


def _scan_bug_registry_ids(
    root: Path | None = None,
    *,
    tolerate_unrelated_allocator_errors: bool = False,
    warnings: list[str] | None = None,
) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    strict_roots = _strict_bug_id_scan_roots(root)
    for bugs_root in _bug_id_scan_roots(root):
        allocator = bugs_root / ".bug_id_allocator.json"
        if allocator.exists():
            try:
                payload = _load_json(allocator)
                number = int(payload.get("last_allocated") or 0)
            except (OSError, json.JSONDecodeError, WorkflowError, TypeError, ValueError) as exc:
                if tolerate_unrelated_allocator_errors and _path_identity(bugs_root) not in strict_roots:
                    if warnings is not None:
                        warnings.append(f"skipped unrelated invalid BUG id allocator: {allocator} ({type(exc).__name__})")
                    number = 0
                else:
                    raise WorkflowError(f"invalid bug id allocator: {allocator}") from exc
            if number > 0:
                sources.append(
                    {
                        "bug_id": f"BUG-{number:03d}",
                        "number": number,
                        "kind": "allocator",
                        "source": str(allocator),
                    }
                )
        for path in sorted(p for p in bugs_root.glob("*.json") if not p.name.startswith(".")):
            # Canonical BUG filenames already carry BUG-NNN. Avoid opening and
            # parsing every historical JSON; only legacy/non-standard names
            # need a content fallback.
            number = _bug_id_number_from_filename(path.name)
            if number is None:
                try:
                    payload = _load_json(path)
                    number = _bug_id_number(str(payload.get("bug_id") or ""))
                except (OSError, json.JSONDecodeError, WorkflowError):
                    number = None
            if number:
                sources.append(
                    {
                        "bug_id": f"BUG-{number:03d}",
                        "number": number,
                        "kind": "bug_json",
                        "source": str(path),
                    }
                )
    return sources


def _fast_bug_id_sources(
    root: Path | None = None,
    *,
    tolerate_unrelated_allocator_errors: bool = True,
    warnings: list[str] | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """Use one host-wide counter plus reservation filenames on the normal path.

    A missing state file is a one-time migration case.  That bootstrap is
    bounded to the invocation/current/canonical registries and never visits
    every worktree or performs network I/O.
    """

    state_path = _bug_id_state_path()
    try:
        state = read_allocator_state(state_path)
    except BugIdLockError as exc:
        raise WorkflowError(str(exc)) from exc
    sources: list[dict[str, Any]] = []
    bootstrapped = state is None
    fingerprint_index_bootstrap_required = (
        state is None
        or int(state.get("fingerprint_index_version") or 0) != FINGERPRINT_INDEX_VERSION
    )
    if state is None:
        sources.extend(
            _scan_bug_registry_ids(
                root,
                tolerate_unrelated_allocator_errors=tolerate_unrelated_allocator_errors,
                warnings=warnings,
            )
        )
        # Reservation filenames participate only in the one-time high-water
        # bootstrap. They are not opened on the steady-state path.
        sources.extend(_scan_bug_id_reservations())
    else:
        number = int(state.get("last_allocated") or 0)
        if number > 0:
            sources.append(
                {
                    "bug_id": f"BUG-{number:03d}",
                    "number": number,
                    "kind": "allocator_state",
                    "source": str(state_path),
                }
            )
    return sources, fingerprint_index_bootstrap_required or bootstrapped


def _scan_github_bug_ids(*, limit: int = 1000, timeout: int = 30) -> tuple[list[dict[str, Any]], list[str]]:
    result = _run_command(
        [
            "gh",
            "issue",
            "list",
            "--repo",
            GITHUB_REPO,
            "--state",
            "all",
            "--limit",
            str(limit),
            "--json",
            "number,title,url,state,labels",
        ],
        timeout=timeout,
    )
    if not result.get("ok"):
        message = result.get("stderr") or result.get("stdout") or "gh issue list failed"
        return [], [f"github BUG id scan unavailable: {message}"]
    try:
        issues = json.loads(str(result.get("stdout") or "[]"))
    except json.JSONDecodeError as exc:
        return [], [f"github BUG id scan returned invalid JSON: {exc}"]
    sources: list[dict[str, Any]] = []
    for issue in issues if isinstance(issues, list) else []:
        if not isinstance(issue, dict):
            continue
        title = str(issue.get("title") or "")
        number = _bug_id_number(title)
        if not number:
            continue
        sources.append(
            {
                "bug_id": f"BUG-{number:03d}",
                "number": number,
                "kind": "github_issue",
                "source": issue.get("url") or f"github_issue:{issue.get('number')}",
                "github_issue_number": issue.get("number"),
                "github_state": issue.get("state"),
                "title": title,
                "labels": issue.get("labels") or [],
            }
        )
    if len(issues) >= limit:
        sources.append(
            {
                "bug_id": "BUG-000",
                "number": 0,
                "kind": "github_scan_limit_warning",
                "source": f"gh issue list reached limit {limit}; increase limit if BUG ids exceed this window",
            }
        )
    return sources, []


def _github_bug_issue_for_id(bug_id: str, *, limit: int = 20, timeout: int = 30) -> tuple[dict[str, Any] | None, list[str]]:
    normalized = bug_id.strip().upper()
    result = _run_command(
        [
            "gh",
            "issue",
            "list",
            "--repo",
            GITHUB_REPO,
            "--state",
            "all",
            "--search",
            f"{normalized} in:title",
            "--limit",
            str(limit),
            "--json",
            "number,title,url,state,labels",
        ],
        timeout=timeout,
    )
    if not result.get("ok"):
        message = result.get("stderr") or result.get("stdout") or "gh issue lookup failed"
        return None, [f"GitHub lookup for {normalized} unavailable: {message}"]
    try:
        issues = json.loads(str(result.get("stdout") or "[]"))
    except json.JSONDecodeError as exc:
        return None, [f"GitHub lookup for {normalized} returned invalid JSON: {exc}"]
    matches = [
        issue
        for issue in issues if isinstance(issue, dict) and _bug_id_number(str(issue.get("title") or "")) == _bug_id_number(normalized)
    ]
    if not matches:
        return None, []
    matches.sort(key=lambda item: int(item.get("number") or 0), reverse=True)
    issue = matches[0]
    return {
        "bug_id": normalized,
        "number": _bug_id_number(normalized),
        "kind": "github_issue",
        "source": issue.get("url") or f"github_issue:{issue.get('number')}",
        "github_issue_number": issue.get("number"),
        "github_state": issue.get("state"),
        "title": issue.get("title"),
        "labels": issue.get("labels") or [],
    }, []


def _github_bug_issue_by_number(issue_number: int | str, *, timeout: int = 30) -> tuple[dict[str, Any] | None, list[str]]:
    result = _run_command(
        [
            "gh",
            "api",
            f"repos/{GITHUB_REPO}/issues/{issue_number}",
        ],
        timeout=timeout,
    )
    if not result.get("ok"):
        message = result.get("stderr") or result.get("stdout") or "gh issue view failed"
        return None, [f"linked GitHub Issue lookup unavailable: {message}"]
    try:
        issue = json.loads(str(result.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        return None, [f"linked GitHub Issue lookup returned invalid JSON: {exc}"]
    if not isinstance(issue, dict) or not issue.get("number"):
        return None, [f"linked GitHub Issue {issue_number} was not found"]
    title = str(issue.get("title") or "")
    bug_number = _bug_id_number(title)
    return {
        "bug_id": f"BUG-{bug_number:03d}" if bug_number else None,
        "number": bug_number,
        "kind": "github_issue",
        "source": issue.get("html_url") or _github_issue_url(issue.get("number")),
        "github_issue_number": issue.get("number"),
        "github_state": issue.get("state"),
        "title": title,
        "labels": issue.get("labels") or [],
    }, []


def _looks_like_github_transport_failure(message: str) -> bool:
    normalized = message.casefold()
    return bool(re.search(r"\beof\b", normalized)) or any(
        token in normalized
        for token in (
            "timeout",
            "timed out",
            "tls handshake",
            "schannel",
            "ssl/tls",
            "gnutls",
            "curl 35",
            "curl 56",
            "unexpected eof",
            "connection reset",
            "connection aborted",
            "connection refused",
            "network is unreachable",
            "remote end hung up",
            "http2",
            "stream error",
            "temporary failure",
        )
    )


def _create_github_issue_with_recovery(
    *,
    bug_id: str,
    title: str,
    body_path: Path,
    labels: list[str],
    cwd: Path,
) -> dict[str, Any]:
    result = _run_command(
        [
            "gh",
            "issue",
            "create",
            "--repo",
            GITHUB_REPO,
            "--title",
            title,
            "--body-file",
            str(body_path),
            "--label",
            _csv_arg(labels),
        ],
        cwd=cwd,
        timeout=120,
    )
    issue_url = str(result.get("stdout") or "").splitlines()[-1].strip() if result.get("ok") else ""
    issue_number = _github_issue_number_from_url(issue_url) if issue_url else None
    if result.get("ok") and issue_url and issue_number:
        return {
            "created": True,
            "url": issue_url,
            "number": issue_number,
            "recovered_after_transport_error": False,
            "warnings": [],
        }

    message = str(result.get("stderr") or result.get("stdout") or "gh issue create failed")
    uncertain_remote_result = bool(result.get("ok")) or _looks_like_github_transport_failure(message)
    if uncertain_remote_result:
        recovered, warnings = _github_bug_issue_for_id(bug_id)
        if recovered is not None and str(recovered.get("title") or "").strip() == title.strip():
            recovered_number = recovered.get("github_issue_number")
            recovered_url = str(recovered.get("source") or _github_issue_url(recovered_number))
            return {
                "created": True,
                "url": recovered_url,
                "number": recovered_number,
                "recovered_after_transport_error": True,
                "warnings": warnings,
            }
        recovery_detail = "; ".join(warnings) if warnings else f"no exact {bug_id} GitHub Issue found yet"
        raise GitHubOutcomeUnknownError(
            f"{message}; GitHub create outcome is unknown: {recovery_detail}; "
            "reservation preserved and automatic recreate blocked"
        )
    raise WorkflowError(message)


def _build_bug_id_allocation_report(
    sources: list[dict[str, Any]],
    warnings: list[str],
    *,
    github_scanned: bool,
) -> dict[str, Any]:
    sources = list(sources)
    warnings = flow._unique_strings(warnings)
    max_number = max((int(source.get("number") or 0) for source in sources), default=0)
    max_by_kind: dict[str, int] = {}
    for source in sources:
        kind = str(source.get("kind") or "unknown")
        number = int(source.get("number") or 0)
        if number > max_by_kind.get(kind, 0):
            max_by_kind[kind] = number
    allocator_max = max(max_by_kind.get("allocator", 0), max_by_kind.get("allocator_state", 0))
    observed_max = max(
        max_by_kind.get("bug_json", 0),
        max_by_kind.get("reservation", 0),
        max_by_kind.get("github_issue", 0),
    )
    if allocator_max and observed_max > allocator_max:
        warnings.append(
            "BUG id allocator is behind observed BUG ids: "
            f"allocator=BUG-{allocator_max:03d}, observed=BUG-{observed_max:03d}; "
            f"next allocation will use BUG-{max_number + 1:03d}"
        )
    return {
        "schema_version": "aistock_bug_id_allocation_report_v1",
        "max_number": max_number,
        "next_number": max_number + 1,
        "sources": sources,
        "warnings": warnings,
        "github_scanned": github_scanned,
        "max_by_kind": max_by_kind,
        "allocator_max_number": allocator_max,
        "allocator_state_max_number": max_by_kind.get("allocator_state", 0),
        "observed_max_number": observed_max,
        "github_max_number": max_by_kind.get("github_issue", 0),
    }


def _bug_id_allocation_report(
    root: Path | None = None,
    *,
    include_github: bool = False,
    github_required: bool = False,
    tolerate_unrelated_allocator_errors: bool = True,
) -> dict[str, Any]:
    warnings: list[str] = []
    sources, bootstrapped = _fast_bug_id_sources(
        root,
        tolerate_unrelated_allocator_errors=tolerate_unrelated_allocator_errors,
        warnings=warnings,
    )
    github_lookup_mode = "not_requested"
    if include_github:
        local_report = _build_bug_id_allocation_report(sources, warnings, github_scanned=False)
        candidate_bug_id = f"BUG-{int(local_report['next_number']):03d}"
        github_issue, github_warnings = _github_bug_issue_for_id(candidate_bug_id)
        if github_issue is not None:
            sources.append(github_issue)
        warnings.extend(github_warnings)
        if github_required and github_warnings:
            raise WorkflowError("; ".join(github_warnings))
        github_lookup_mode = "exact_candidate"
    report = _build_bug_id_allocation_report(sources, warnings, github_scanned=False)
    report["github_lookup_mode"] = github_lookup_mode
    report["allocator_state_bootstrap_required"] = bootstrapped
    return report


def _bug_id_allocation_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "aistock_bug_id_allocation_summary_v1",
        "max_number": report.get("max_number"),
        "next_number": report.get("next_number"),
        "allocator_max_number": report.get("allocator_max_number"),
        "allocator_state_max_number": report.get("allocator_state_max_number"),
        "allocator_state_bootstrap_required": report.get("allocator_state_bootstrap_required"),
        "observed_max_number": report.get("observed_max_number"),
        "github_max_number": report.get("github_max_number"),
        "github_scanned": report.get("github_scanned"),
        "github_lookup_mode": report.get("github_lookup_mode"),
        "warnings": report.get("warnings", []),
    }


def _duplicate_bug_id_sources(
    report: dict[str, Any],
    bug_id: str,
    *,
    allowed_github_issue_number: int | str | None = None,
    allowed_reservation_source: str | None = None,
) -> list[dict[str, Any]]:
    number = _bug_id_number(bug_id)
    allowed_issue = str(allowed_github_issue_number) if allowed_github_issue_number is not None else None
    duplicates: list[dict[str, Any]] = []
    for source in report.get("sources", []):
        if int(source.get("number") or 0) != number:
            continue
        kind = source.get("kind")
        # The allocator state is an observation of the reservation counter,
        # not a durable BUG record.  Treat both historical ``allocator`` and
        # current ``allocator_state`` source kinds as non-duplicates so an
        # interrupted registration can safely resume its reserved BUG id.
        if kind in {"allocator", "allocator_state"}:
            continue
        if kind == "github_issue" and allowed_issue and str(source.get("github_issue_number")) == allowed_issue:
            continue
        if kind == "reservation" and allowed_reservation_source and _same_path(Path(str(source.get("source") or "")), Path(allowed_reservation_source)):
            continue
        duplicates.append(source)
    return duplicates


def _normalized_github_bug_subject(title: str | None) -> str:
    raw = re.sub(r"^\s*BUG-\d{3,}\s+(?:P[0-3]\s*:\s*)?", "", str(title or ""), flags=re.IGNORECASE)
    return " ".join(raw.casefold().split())


def _matching_open_github_issue(sources: list[dict[str, Any]], title: str | None) -> dict[str, Any] | None:
    expected = _normalized_github_bug_subject(title)
    if not expected:
        return None
    matches = [
        source
        for source in sources
        if source.get("kind") == "github_issue"
        and str(source.get("github_state") or "").upper() == "OPEN"
        and _normalized_github_bug_subject(str(source.get("title") or "")) == expected
    ]
    if not matches:
        return None
    matches.sort(key=lambda item: int(item.get("github_issue_number") or 0), reverse=True)
    return matches[0]


def _update_bug_id_reservation(path: Path | None, **updates: Any) -> None:
    if path is None or not path.exists():
        return
    payload = _load_json(path)
    payload.update({key: value for key, value in updates.items() if value is not None})
    payload["updated_at"] = _utc_now()
    _write_json(path, payload)
    write_fingerprint_index(
        _bug_id_reservation_root(),
        {**payload, "reservation_path": str(path)},
    )


def _reservation_can_resume(
    payload: dict[str, Any],
    *,
    github_issue_number: int | str | None,
    reservation_title: str | None,
    reservation_fingerprint: str | None,
) -> bool:
    if str(payload.get("status") or "") not in {
        "reserved",
        "github_preflight_unknown",
        "github_create_outcome_unknown",
        "github_issue_confirmed",
        "github_issue_confirmed_local_incomplete",
    }:
        return False
    existing_issue = payload.get("github_issue_number")
    if existing_issue is not None and github_issue_number is not None and str(existing_issue) != str(github_issue_number):
        return False
    existing_title = str(payload.get("title") or "")
    if existing_title and reservation_title and _normalized_github_bug_subject(existing_title) != _normalized_github_bug_subject(reservation_title):
        return False
    existing_fingerprint = str(payload.get("fingerprint") or "")
    if existing_fingerprint and reservation_fingerprint and existing_fingerprint != reservation_fingerprint:
        return False
    return bool(github_issue_number or existing_issue)


def _matching_automatic_reservation(
    *,
    reservation_title: str | None,
    reservation_fingerprint: str | None,
) -> tuple[Path, dict[str, Any]] | None:
    if not reservation_fingerprint:
        return None
    root = _bug_id_reservation_root()
    payload = find_matching_reservation(root, reservation_fingerprint)
    if payload is None:
        return None
    resumable_statuses = {
        "github_preflight_unknown",
        "github_create_outcome_unknown",
        "github_issue_confirmed",
        "github_issue_confirmed_local_incomplete",
    }
    if str(payload.get("status") or "") not in resumable_statuses:
        return None
    if _normalized_github_bug_subject(str(payload.get("title") or "")) != _normalized_github_bug_subject(reservation_title):
        return None
    return Path(str(payload["reservation_path"])), payload


def _fingerprint_bootstrap_records(root: Path | None) -> list[dict[str, Any]]:
    """Read bounded current/canonical records for a one-time index migration."""

    records: list[dict[str, Any]] = []
    for bugs_root in _bug_id_scan_roots(root):
        for path in bugs_root.glob("*.json"):
            try:
                payload = _load_json(path)
            except (OSError, json.JSONDecodeError, WorkflowError):
                continue
            if not str(payload.get("fingerprint") or "").strip():
                continue
            records.append({**payload, "registry_path": str(path)})
    records.extend(read_reservations(_bug_id_reservation_root()))
    return records


def _reserve_bug_id(
    root: Path | None,
    *,
    bug_id: str | None,
    include_github: bool,
    github_required: bool,
    allowed_github_issue_number: int | str | None,
    reservation_title: str | None = None,
    reservation_fingerprint: str | None = None,
    _candidate_attempt: int = 0,
) -> tuple[str, int, dict[str, Any], Path]:
    if _candidate_attempt >= 20:
        raise WorkflowError("unable to find an available BUG id after 20 exact GitHub candidate checks")
    canonical_bug_id = (bug_id or "").strip().upper()
    number = _bug_id_number(canonical_bug_id) if canonical_bug_id else None
    if canonical_bug_id and (not number or not re.fullmatch(r"BUG-\d{3,}", canonical_bug_id)):
        raise WorkflowError("--bug-id must match BUG-NNN when provided")
    direct_linked_issue = bool(canonical_bug_id and allowed_github_issue_number)
    github_warnings: list[str] = []
    linked_issue: dict[str, Any] | None = None

    # Explicit linkage can be verified before the critical section because it
    # does not depend on the next local candidate. Automatic allocation first
    # reserves a local candidate, then performs one exact GitHub lookup.
    if direct_linked_issue:
        linked_issue, github_warnings = _github_bug_issue_by_number(allowed_github_issue_number)
        if (github_warnings or linked_issue is None) and github_required:
            raise WorkflowError("; ".join(github_warnings or ["linked GitHub Issue lookup failed"]))
        if linked_issue is not None and str(linked_issue.get("bug_id") or "").upper() != canonical_bug_id:
            raise WorkflowError(
                f"linked GitHub Issue {allowed_github_issue_number} title does not match {canonical_bug_id}"
            )

    resumed_status: str | None = None
    allocation_lock = _GlobalBugIdAllocatorLock()
    with allocation_lock:
        local_warnings: list[str] = []
        sources, allocator_bootstrap_required = _fast_bug_id_sources(
            root,
            tolerate_unrelated_allocator_errors=True,
            warnings=local_warnings,
        )
        if linked_issue is not None:
            sources.append(linked_issue)
        report = _build_bug_id_allocation_report(
            sources,
            [*local_warnings, *github_warnings],
            github_scanned=False,
        )
        report["github_lookup_mode"] = "linked_issue_number" if direct_linked_issue else (
            "exact_candidate" if include_github else "not_requested"
        )
        if allocator_bootstrap_required:
            bootstrap_records = _fingerprint_bootstrap_records(root)
            report["fingerprint_index_bootstrap_count"] = bootstrap_fingerprint_index(
                _bug_id_reservation_root(),
                bootstrap_records,
            )
            durable_bug_ids = {
                str(item.get("bug_id") or "").upper()
                for item in bootstrap_records
                if item.get("registry_path") and item.get("bug_id")
            }
            report["terminal_reservation_compaction_count"] = len(
                compact_terminal_reservations(
                    _bug_id_reservation_root(),
                    durable_bug_ids,
                )
            )
            try:
                write_allocator_state(
                    _bug_id_state_path(),
                    last_allocated=int(report.get("max_number") or 0),
                    updated_at=_utc_now(),
                    updated_by="aistock_issue_workflow.py/bootstrap",
                    fingerprint_index_version=FINGERPRINT_INDEX_VERSION,
                )
            except BugIdLockError as exc:
                raise WorkflowError(str(exc)) from exc
        try:
            matching_reservation = find_matching_reservation(
                _bug_id_reservation_root(),
                reservation_fingerprint,
            )
        except BugIdLockError as exc:
            raise WorkflowError(str(exc)) from exc
        resumed_automatic = None if bug_id else _matching_automatic_reservation(
            reservation_title=reservation_title,
            reservation_fingerprint=reservation_fingerprint,
        )
        if not bug_id and matching_reservation is not None and resumed_automatic is None:
            existing_title = _normalized_github_bug_subject(str(matching_reservation.get("title") or ""))
            expected_title = _normalized_github_bug_subject(reservation_title)
            if not expected_title or existing_title == expected_title:
                existing_id = matching_reservation.get("bug_id") or Path(
                    str(matching_reservation.get("reservation_path") or "")
                ).stem
                existing_status = matching_reservation.get("status") or "unknown"
                raise WorkflowError(
                    f"matching BUG registration already exists: {existing_id} status={existing_status}; "
                    "resume the existing intake instead of allocating another id"
                )
        if resumed_automatic is not None:
            reservation_path, existing_reservation = resumed_automatic
            canonical_bug_id = str(existing_reservation.get("bug_id") or reservation_path.stem).upper()
            number = _bug_id_number(canonical_bug_id)
            allowed_github_issue_number = existing_reservation.get("github_issue_number")
            resumed_status = str(existing_reservation.get("status") or "")
        else:
            existing_reservation = None
            if not bug_id:
                number = int(report["next_number"])
                canonical_bug_id = f"BUG-{number:03d}"
        assert number is not None
        reservation_root = _bug_id_reservation_root()
        reservation_path = reservation_root / f"{canonical_bug_id}.json"
        if not bug_id and existing_reservation is None:
            while reservation_path.exists():
                number += 1
                canonical_bug_id = f"BUG-{number:03d}"
                reservation_path = reservation_root / f"{canonical_bug_id}.json"
            report["next_number"] = number
        if existing_reservation is None and reservation_path.exists():
            existing_reservation = _load_json(reservation_path)
        reusable_reservation = bool(
            existing_reservation
            and _reservation_can_resume(
                existing_reservation,
                github_issue_number=allowed_github_issue_number,
                reservation_title=reservation_title,
                reservation_fingerprint=reservation_fingerprint,
            )
        )
        automatic_resume = resumed_automatic is not None
        if bug_id:
            duplicates = _duplicate_bug_id_sources(
                report,
                canonical_bug_id,
                allowed_github_issue_number=allowed_github_issue_number,
                allowed_reservation_source=str(reservation_path) if reusable_reservation else None,
            )
            if duplicates:
                detail = "; ".join(f"{item.get('kind')}:{item.get('source')}" for item in duplicates[:5])
                raise WorkflowError(f"{canonical_bug_id} already exists in global BUG id scan: {detail}")
        if existing_reservation is not None and not (reusable_reservation or automatic_resume):
            raise WorkflowError(f"{canonical_bug_id} is already reserved: {reservation_path}")
        if existing_reservation is None:
            reservation_payload = {
                "schema_version": "aistock_bug_id_reservation_v1",
                "bug_id": canonical_bug_id,
                "reserved_at": _utc_now(),
                "reserved_by": "aistock_issue_workflow.py",
                "root": str((root or REPO_ROOT).resolve()),
                "status": "reserved",
                "title": reservation_title,
                "fingerprint": reservation_fingerprint,
            }
            _write_json(reservation_path, reservation_payload)
            write_fingerprint_index(
                reservation_root,
                {**reservation_payload, "reservation_path": str(reservation_path)},
            )
        elif not automatic_resume:
            _update_bug_id_reservation(
                reservation_path,
                status="reserved",
                github_issue_number=allowed_github_issue_number,
                title=reservation_title,
                fingerprint=reservation_fingerprint,
            )
        try:
            write_allocator_state(
                _bug_id_state_path(),
                last_allocated=max(int(report.get("max_number") or 0), int(number)),
                updated_at=_utc_now(),
                updated_by="aistock_issue_workflow.py",
                fingerprint_index_version=FINGERPRINT_INDEX_VERSION,
            )
        except BugIdLockError as exc:
            raise WorkflowError(str(exc)) from exc
        report["allocator_state_bootstrap_required"] = allocator_bootstrap_required
        report["allocator_state_path"] = str(_bug_id_state_path())

    telemetry = getattr(allocation_lock, "telemetry", None)
    report["allocator_lock"] = telemetry() if callable(telemetry) else {"wait_ms": None, "hold_ms": None}

    if direct_linked_issue or not include_github:
        return canonical_bug_id, number, report, reservation_path

    github_issue, lookup_warnings = _github_bug_issue_for_id(canonical_bug_id)
    report["warnings"] = flow._unique_strings([*(report.get("warnings") or []), *lookup_warnings])
    if lookup_warnings:
        unknown_status = (
            "github_create_outcome_unknown"
            if resumed_status in {
                "github_create_outcome_unknown",
                "github_issue_confirmed",
                "github_issue_confirmed_local_incomplete",
            }
            else "github_preflight_unknown"
        )
        _update_bug_id_reservation(
            reservation_path,
            status=unknown_status,
            last_error="; ".join(lookup_warnings),
        )
        if github_required:
            raise GitHubOutcomeUnknownError(
                f"GitHub exact lookup for {canonical_bug_id} is unavailable; reservation preserved: "
                + "; ".join(lookup_warnings)
            )
        return canonical_bug_id, number, report, reservation_path

    if github_issue is not None:
        report.setdefault("sources", []).append(github_issue)
        same_subject = _normalized_github_bug_subject(str(github_issue.get("title") or "")) == _normalized_github_bug_subject(reservation_title)
        is_open = str(github_issue.get("github_state") or "").upper() == "OPEN"
        if same_subject and is_open:
            report["recovered_existing_github_issue"] = github_issue
            _update_bug_id_reservation(
                reservation_path,
                status="github_issue_confirmed",
                github_issue_number=github_issue.get("github_issue_number"),
                github_issue_url=github_issue.get("source"),
            )
            return canonical_bug_id, number, report, reservation_path
        _update_bug_id_reservation(
            reservation_path,
            status="remote_collision",
            github_issue_number=github_issue.get("github_issue_number"),
            github_issue_url=github_issue.get("source"),
            remote_title=github_issue.get("title"),
        )
        remove_fingerprint_index(
            _bug_id_reservation_root(),
            reservation_fingerprint,
            bug_id=canonical_bug_id,
        )
        if bug_id:
            raise WorkflowError(f"{canonical_bug_id} already exists on GitHub with a different title or state")
        return _reserve_bug_id(
            root,
            bug_id=None,
            include_github=include_github,
            github_required=github_required,
            allowed_github_issue_number=None,
            reservation_title=reservation_title,
            reservation_fingerprint=reservation_fingerprint,
            _candidate_attempt=_candidate_attempt + 1,
        )

    if resumed_status in {
        "github_create_outcome_unknown",
        "github_issue_confirmed",
        "github_issue_confirmed_local_incomplete",
    }:
        _update_bug_id_reservation(
            reservation_path,
            status="github_create_outcome_unknown",
            last_error=f"exact GitHub lookup still has no indexed {canonical_bug_id}; automatic recreate remains blocked",
        )
        raise GitHubOutcomeUnknownError(
            f"GitHub create outcome for {canonical_bug_id} is still unknown; reservation preserved and automatic recreate blocked"
        )
    _update_bug_id_reservation(
        reservation_path,
        status="reserved",
        github_preflight_checked_at=_utc_now(),
    )
    return canonical_bug_id, number, report, reservation_path


def _release_bug_id_reservation(path: Path | None) -> None:
    if not path:
        return
    payload: dict[str, Any] = {}
    try:
        payload = _load_json(path)
    except (FileNotFoundError, OSError, json.JSONDecodeError, WorkflowError):
        pass
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    remove_fingerprint_index(
        _bug_id_reservation_root(),
        payload.get("fingerprint"),
        bug_id=str(payload.get("bug_id") or path.stem),
    )


def _process_id_is_alive(pid: int) -> bool | None:
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True
    if os.name == "nt":
        try:
            import ctypes

            process_query_limited_information = 0x1000
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
            if handle:
                kernel32.CloseHandle(handle)
                return True
            # Access denied still proves that the PID exists.
            return True if ctypes.get_last_error() == 5 else False
        except (AttributeError, OSError, ValueError):
            return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return None
    return True


class _GlobalBugIdAllocatorLock:
    def __init__(self, *, timeout: float = 30.0) -> None:
        self.timeout = timeout
        self.path = Path(os.environ.get("AISTOCK_BUG_ID_LOCK_PATH") or (_default_worktree_root() / ".locks" / "bug-id-allocator.lock"))
        self._delegate: GlobalBugIdLock | None = None

    def __enter__(self) -> "_GlobalBugIdAllocatorLock":
        self._delegate = GlobalBugIdLock(
            self.path,
            timeout=self.timeout,
            process_is_alive=_process_id_is_alive,
        )
        try:
            self._delegate.__enter__()
        except BugIdLockError as exc:
            raise WorkflowError(str(exc)) from exc
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._delegate is None:
            return
        try:
            self._delegate.__exit__(exc_type, exc, tb)
        except BugIdLockError as lock_exc:
            if exc_type is None:
                raise WorkflowError(str(lock_exc)) from lock_exc

    def telemetry(self) -> dict[str, float | None]:
        return self._delegate.telemetry() if self._delegate is not None else {"wait_ms": None, "hold_ms": None}


def _next_bug_id(
    root: Path | None = None,
    *,
    include_github: bool = False,
    github_required: bool = False,
) -> tuple[str, int]:
    report = _bug_id_allocation_report(root, include_github=include_github, github_required=github_required)
    next_number = int(report["next_number"])
    return f"BUG-{next_number:03d}", next_number


def _write_allocator(next_number: int, root: Path | None = None) -> None:
    allocator = _allocator_path(root)
    current = 0
    if allocator.exists():
        try:
            current = int(_load_json(allocator).get("last_allocated") or 0)
        except (OSError, json.JSONDecodeError, WorkflowError, TypeError, ValueError) as exc:
            raise WorkflowError(f"invalid bug id allocator: {allocator}") from exc
    if current > next_number:
        next_number = current
    allocator = _allocator_path(root)
    _write_json(
        _allocator_path(root),
        {
            "schema_version": "aistock_bug_id_allocator_v1",
            "last_allocated": next_number,
            "updated_at": _utc_now(),
            "updated_by": "aistock_issue_workflow.py",
        },
    )


def _bug_json_path(record: dict[str, Any], root: Path | None = None) -> Path:
    bug_id = str(record["bug_id"])
    return _bugs_root(root) / f"{_today_compact()}_{bug_id}-{_slug(str(record.get('title') or bug_id))}.json"


def _add_record_allowed_scope(record: dict[str, Any], *paths: str | Path) -> None:
    scope = [str(item).replace("\\", "/").strip("/") for item in flow._as_list(record.get("allowed_write_scope"))]
    for raw_path in paths:
        path = str(raw_path).replace("\\", "/").strip("/")
        if path and path not in scope:
            scope.append(path)
    record["allowed_write_scope"] = scope


def _registry_target_root() -> Path:
    override = os.environ.get("AISTOCK_ISSUE_REGISTRY_ROOT")
    return Path(override) if override else REPO_ROOT


def _registry_worktree_names(*, title: str, module: str, severity: str) -> tuple[str, Path]:
    title_slug = _slug(title, max_len=34)
    module_slug = _slug(module, max_len=22)
    suffix = _short_hash(title, module, severity, _utc_now(), length=6)
    name = f"registry-{module_slug}-{title_slug}-{_today_compact()}-{suffix}"
    branch = f"bug/{name}"
    return branch, _default_worktree_root() / name


def _close_sync_worktree_names(*, bug_id: str) -> tuple[str, Path]:
    name = f"{bug_id.strip().upper()}-close-sync-{_today_compact()}"
    branch = f"chore/{name}"
    return branch, _default_worktree_root() / name


def _close_sync_batch_worktree_names(*, bug_ids: list[str]) -> tuple[str, Path]:
    normalized = [item.strip().upper() for item in bug_ids if item.strip()]
    label = "-".join(normalized[:3])
    if len(normalized) > 3:
        label = f"{label}-plus-{len(normalized) - 3}"
    name = f"{label}-close-sync-batch-{_today_compact()}".strip("-")
    branch = f"chore/{name}"
    return branch, _default_worktree_root() / name


def _close_sync_aggregate_worktree_names(*, bug_ids: list[str]) -> tuple[str, Path]:
    normalized = [item.strip().upper() for item in bug_ids if item.strip()]
    label = _delimited_text(normalized[:3], "-")
    if len(normalized) > 3:
        label = f"{label}-plus-{len(normalized) - 3}"
    name = f"{label}-close-sync-aggregate-{_today_compact()}".strip("-")
    branch = f"chore/{name}"
    return branch, _default_worktree_root() / name


def _maybe_create_registry_worktree(
    *,
    title: str,
    module: str,
    severity: str,
    create: bool,
    dry_run: bool,
) -> dict[str, Any]:
    branch, worktree = _registry_worktree_names(title=title, module=module, severity=severity)
    plan = {
        "create_worktree": create,
        "dry_run": dry_run,
        "branch": branch,
        "worktree": str(worktree),
        "base": "origin/main",
    }
    if not create or dry_run:
        return plan
    if worktree.exists():
        raise WorkflowError(f"target registry worktree already exists: {worktree}")
    git_root = _canonical_root() if _canonical_root().exists() else REPO_ROOT
    _git(["fetch", "origin", "main"], cwd=git_root)
    _git_worktree_add_new_branch(worktree=worktree, branch=branch, base="origin/main", cwd=git_root)
    plan["created"] = True
    plan["git_root"] = str(git_root)
    return plan


def _maybe_create_fix_chain_worktree(
    *,
    record: dict[str, Any],
    bug_id: str,
    create: bool,
    dry_run: bool,
    task_slug: str | None = None,
) -> dict[str, Any]:
    branch, worktree = _target_names(record, bug_id, task_slug)
    plan = {
        "create_worktree": create,
        "dry_run": dry_run,
        "branch": branch,
        "worktree": str(worktree),
        "base": "origin/main",
        "registration_strategy": "fix_pr_persists_bug_registration",
    }
    if not create or dry_run:
        return plan
    if worktree.exists():
        raise WorkflowError(f"target fix-chain worktree already exists: {worktree}")
    _git(["fetch", "origin", "main"])
    _git_worktree_add_new_branch(worktree=worktree, branch=branch)
    plan["created"] = True
    return plan


def _git_worktree_add_new_branch(
    *,
    worktree: Path,
    branch: str,
    base: str = "origin/main",
    cwd: Path | None = None,
) -> None:
    # Keep options before the path; some Git versions otherwise infer `main` from origin/main in linked worktrees.
    _git(["worktree", "add", "-b", branch, str(worktree), base], cwd=cwd)


def _refresh_reused_close_sync_worktree(
    *,
    worktree: Path,
    branch: str,
    label: str,
) -> tuple[dict[str, Any], str]:
    git = _git_snapshot(worktree)
    if not git.get("ok"):
        raise WorkflowError(f"target {label} worktree is not a git checkout: {worktree}")
    if git.get("dirty"):
        raise WorkflowError(f"target {label} worktree is dirty: {worktree}")
    if git.get("branch") != branch:
        raise WorkflowError(
            f"target {label} worktree branch mismatch: expected={branch} actual={git.get('branch')}"
        )
    head = str(git.get("head") or "")
    origin_main = str(git.get("origin_main") or "")
    if not head or not origin_main or head == origin_main:
        return git, "current"
    behind = _run_command(["git", "merge-base", "--is-ancestor", "HEAD", "origin/main"], cwd=worktree)
    if behind.get("ok"):
        _git(["merge", "--ff-only", "origin/main"], cwd=worktree)
        refreshed = _git_snapshot(worktree)
        if refreshed.get("head") != refreshed.get("origin_main"):
            raise WorkflowError(f"target {label} worktree is stale after fast-forward: {worktree}")
        return refreshed, "fast_forwarded"
    ahead = _run_command(["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"], cwd=worktree)
    if ahead.get("ok"):
        return git, "ahead_with_task_commits"
    raise WorkflowError(f"target {label} worktree diverged from origin/main: {worktree}")


def _maybe_create_close_sync_worktree(*, bug_id: str, create: bool, dry_run: bool) -> dict[str, Any]:
    branch, worktree = _close_sync_worktree_names(bug_id=bug_id)
    plan = {
        "create_worktree": create,
        "dry_run": dry_run,
        "branch": branch,
        "worktree": str(worktree),
        "base": "origin/main",
    }
    if not create or dry_run:
        return plan
    _git_fetch_with_transport_retry(["origin", "main"])
    if worktree.exists():
        git, relation = _refresh_reused_close_sync_worktree(
            worktree=worktree,
            branch=branch,
            label="close-sync",
        )
        if relation == "fast_forwarded":
            plan["fast_forwarded"] = True
        elif relation == "ahead_with_task_commits":
            plan["ahead_with_task_commits"] = True
        plan["reused"] = True
        plan["git"] = git
        return plan
    if _git_ref_exists(branch):
        _git(["worktree", "add", str(worktree), branch])
        _git_state, relation = _refresh_reused_close_sync_worktree(
            worktree=worktree,
            branch=branch,
            label="close-sync",
        )
        plan[relation] = True
        plan["reused_branch"] = True
    else:
        _git_worktree_add_new_branch(worktree=worktree, branch=branch)
    plan["created"] = True
    return plan


def _maybe_create_close_sync_group_worktree(
    *,
    bug_ids: list[str],
    create: bool,
    dry_run: bool,
    mode: str,
) -> dict[str, Any]:
    if mode == "batch":
        branch, worktree = _close_sync_batch_worktree_names(bug_ids=bug_ids)
    elif mode == "aggregate":
        branch, worktree = _close_sync_aggregate_worktree_names(bug_ids=bug_ids)
    else:
        raise WorkflowError(f"unsupported close-sync group worktree mode: {mode}")
    label = f"close-sync {mode}"
    plan = {
        "create_worktree": create,
        "dry_run": dry_run,
        "branch": branch,
        "worktree": str(worktree),
        "base": "origin/main",
        "bug_ids": bug_ids,
        "mode": mode,
    }
    if not create or dry_run:
        return plan
    _git_fetch_with_transport_retry(["origin", "main"])
    if worktree.exists():
        git, relation = _refresh_reused_close_sync_worktree(
            worktree=worktree,
            branch=branch,
            label=label,
        )
        if relation == "fast_forwarded":
            plan["fast_forwarded"] = True
        elif relation == "ahead_with_task_commits":
            plan["ahead_with_task_commits"] = True
        plan["reused"] = True
        plan["git"] = git
        return plan
    if _git_ref_exists(branch):
        _git(["worktree", "add", str(worktree), branch])
        _git_state, relation = _refresh_reused_close_sync_worktree(
            worktree=worktree,
            branch=branch,
            label=label,
        )
        plan[relation] = True
        plan["reused_branch"] = True
    else:
        _git_worktree_add_new_branch(worktree=worktree, branch=branch)
    plan["created"] = True
    return plan


def _maybe_create_close_sync_batch_worktree(
    *,
    bug_ids: list[str],
    create: bool,
    dry_run: bool,
) -> dict[str, Any]:
    return _maybe_create_close_sync_group_worktree(
        bug_ids=bug_ids,
        create=create,
        dry_run=dry_run,
        mode="batch",
    )


def _maybe_create_close_sync_aggregate_worktree(
    *,
    bug_ids: list[str],
    create: bool,
    dry_run: bool,
) -> dict[str, Any]:
    return _maybe_create_close_sync_group_worktree(
        bug_ids=bug_ids,
        create=create,
        dry_run=dry_run,
        mode="aggregate",
    )


def _validate_registry_apply_target(target_root: Path) -> dict[str, Any]:
    target = target_root.resolve()
    canonical = _canonical_root().resolve()
    git = _git_snapshot(target_root)
    blocking: list[str] = []
    warnings: list[str] = []

    if not git.get("ok"):
        blocking.append(str(git.get("error") or f"not a git checkout: {target_root}"))
    if target == canonical:
        blocking.append(
            "refusing to write BUG registry files in canonical root; use a task/registry worktree and branch"
        )
    if git.get("branch") == "main":
        blocking.append("refusing to write BUG registry files on main; use a task/registry branch")
    if git.get("dirty"):
        blocking.append(f"registry target is dirty ({git.get('dirty_count')} file(s)); start from a clean task worktree")
    if (
        target_root.name.lower() == "aistock"
        and not _is_inside(target_root, _default_worktree_root())
        and target != canonical
    ):
        warnings.append("registry target does not look like an isolated AIstock worktree")

    return {
        "target_root": str(target),
        "canonical_root": str(canonical),
        "git": git,
        "blocking": blocking,
        "warnings": warnings,
    }


def _validate_close_sync_apply_target(target_root: Path) -> dict[str, Any]:
    guard = _validate_registry_apply_target(target_root)
    blocking = list(guard.get("blocking") or [])
    if blocking:
        guard["blocking"] = [
            item.replace("write BUG registry files", "close-sync BUG registry files")
            for item in blocking
        ]
        guard["next_command_hint"] = (
            "Run close-sync from a clean registry/task worktree, or rerun with "
            "--create-registry-worktree so the wrapper creates an isolated branch."
        )
    return guard


def _dirty_files(root: Path) -> list[str]:
    status = _run_command(["git", "status", "--porcelain=v1"], cwd=root)
    if not status.get("ok"):
        return []
    files: list[str] = []
    for line in str(status.get("stdout") or "").splitlines():
        if not line.strip():
            continue
        path = _parse_git_porcelain_path(line)
        if path:
            files.append(path)
    return files


def _git_ref_has_path(root: Path, ref: str, rel: str) -> bool:
    normalized = rel.replace("\\", "/").strip("/")
    if not normalized:
        return False
    return _run_command(["git", "cat-file", "-e", f"{ref}:{normalized}"], cwd=root).get("ok", False)


def _origin_equivalent_dirty_files(root: Path, files: list[str]) -> list[str]:
    equivalent: list[str] = []
    for rel in files:
        if not rel:
            continue
        if not _git_ref_has_path(root, "origin/main", rel):
            continue
        if _run_command(["git", "diff", "--quiet", "origin/main", "--", rel], cwd=root).get("ok"):
            equivalent.append(rel)
    return equivalent


def _root_sync_safe_with_dirty(root_git: dict[str, Any]) -> bool:
    head = str(root_git.get("head") or "")
    origin_main = str(root_git.get("origin_main") or "")
    return bool(head and origin_main and head == origin_main)


def _github_issue_number_from_url(url: str) -> int | None:
    match = re.search(r"/issues/(\d+)(?:$|[?#])", url.strip())
    return int(match.group(1)) if match else None


def _is_inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _cwd_is_inside(path: str | Path | None) -> bool:
    if not path:
        return False
    try:
        target = Path(path).resolve()
        cwd = Path.cwd().resolve()
    except OSError:
        return False
    return cwd == target or _is_inside(cwd, target)


def _relocate_cwd_before_cleanup(*worktrees: str | Path | None, root: Path | None = None) -> dict[str, Any] | None:
    current_root = root or _canonical_root()
    for worktree in worktrees:
        if _cwd_is_inside(worktree):
            relocation = {
                "from": str(worktree),
                "to": str(current_root),
                "reason": "cleanup_target_contains_current_cwd",
                "relocated": False,
            }
            if not current_root.exists():
                relocation["error"] = f"canonical root missing: {current_root}"
                return relocation
            try:
                os.chdir(current_root)
                relocation["relocated"] = True
            except OSError as exc:
                relocation["error"] = str(exc)
            return relocation
    return None


def _deferred_cleanup_from_safe_cwd_plan(
    *,
    branch: str,
    bug_id: str,
    worktree: str | None,
    pr_url: str | None,
    sync_root: bool,
    source_receipt_path: str | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    canonical_root = root or _canonical_root()
    command = (
        f'cd /d "{canonical_root}" && python scripts/aistock_issue_workflow.py cleanup-after-merge '
        f'--branch "{branch}" --bug-id {bug_id} '
    )
    if worktree:
        command += f'--worktree "{worktree}" '
    if pr_url:
        command += f'--pr-url "{pr_url}" '
    if source_receipt_path:
        command += f'--source-receipt-path "{source_receipt_path}" '
    if sync_root:
        command += "--sync-root "
    command += "--apply"
    return {
        "schema_version": "aistock_issue_workflow_deferred_cleanup_v1",
        "workflow_gate": "ready_for_cleanup",
        "branch": branch,
        "bug_id": bug_id,
        "worktree": worktree,
        "pr_url": pr_url,
        "sync_root": sync_root,
        "reason": "source_worktree_contains_invoking_cwd",
        "next_command": command,
        "warnings": [
            "Source worktree cleanup is deferred because the invoking shell may still hold the target cwd on Windows."
        ],
    }


def _canonical_root() -> Path:
    override = os.environ.get("AISTOCK_CANONICAL_ROOT") or os.environ.get("AISTOCK_ROOT")
    if override:
        return Path(override)
    default = Path("F:/Dev/AIstock")
    return default if default.exists() else REPO_ROOT


def _git_snapshot(root: Path) -> dict[str, Any]:
    if not (root / ".git").exists() and not (root / ".git").is_file():
        return {"ok": False, "error": f"not a git checkout: {root}"}
    status = _run_command(["git", "status", "--porcelain=v1", "-b"], cwd=root)
    branch = _run_command(["git", "branch", "--show-current"], cwd=root)
    head = _run_command(["git", "rev-parse", "--short", "HEAD"], cwd=root)
    upstream = _run_command(["git", "rev-parse", "--short", "@{u}"], cwd=root)
    origin_main = _run_command(["git", "rev-parse", "--short", "origin/main"], cwd=root)
    porcelain = status.get("stdout", "")
    dirty_lines = [line for line in porcelain.splitlines()[1:] if line.strip()]
    return {
        "ok": bool(status.get("ok")),
        "branch": branch.get("stdout"),
        "head": head.get("stdout"),
        "upstream": upstream.get("stdout"),
        "origin_main": origin_main.get("stdout"),
        "status": porcelain,
        "dirty": bool(dirty_lines),
        "dirty_count": len(dirty_lines),
    }


def _parse_worktree_list() -> list[dict[str, str]]:
    result = _run_command(["git", "worktree", "list", "--porcelain"], cwd=REPO_ROOT, timeout=30)
    if not result.get("ok"):
        return []
    items: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in str(result.get("stdout") or "").splitlines():
        if not line.strip():
            if current:
                items.append(current)
                current = {}
            continue
        key, _, value = line.partition(" ")
        current[key] = value
    if current:
        items.append(current)
    return items


def _worktree_hygiene_report(canonical_root: Path | None = None) -> dict[str, Any]:
    """Detect stale worktrees that can poison root sync or issue workflow state."""
    canonical_root = canonical_root or _canonical_root()
    blocking: list[str] = []
    warnings: list[str] = []
    items: list[dict[str, Any]] = []

    canonical_git = _git_snapshot(canonical_root) if canonical_root.exists() else {"ok": False}
    if canonical_git.get("ok") and canonical_git.get("branch") != "main":
        blocking.append(
            "canonical root is not on local main; restore F:\\Dev\\AIstock to main...origin/main before workflow work"
        )

    for item in _parse_worktree_list():
        raw_worktree = item.get("worktree")
        if not raw_worktree:
            continue
        worktree_path = Path(raw_worktree)
        branch_ref = item.get("branch") or ""
        branch = branch_ref.removeprefix("refs/heads/")
        is_canonical = _same_path(worktree_path, canonical_root)
        if branch != "main" or is_canonical:
            continue
        snapshot = _git_snapshot(worktree_path)
        dirty_count = int(snapshot.get("dirty_count") or 0)
        status_text = str(snapshot.get("status") or "")
        staged_count = sum(
            1
            for line in status_text.splitlines()[1:]
            if len(line) >= 2 and line[0] not in {" ", "?"}
        )
        finding = {
            "worktree": str(worktree_path),
            "branch": branch,
            "head": item.get("HEAD") or snapshot.get("head"),
            "dirty_count": dirty_count,
            "staged_count": staged_count,
            "workflow_gate": "blocked",
        }
        items.append(finding)
        blocking.append(
            f"non-canonical worktree {worktree_path} is bound to local main; "
            "task worktrees must use task branches and must not hold refs/heads/main"
        )
        if staged_count >= 100 or dirty_count >= 100:
            blocking.append(
                f"non-canonical main worktree {worktree_path} has {dirty_count} dirty path(s) "
                f"and {staged_count} staged path(s); treat as stale-index pseudo changes until audited"
            )

    return {
        "schema_version": "aistock_worktree_hygiene_v1",
        "workflow_gate": "blocked" if blocking else ("warning" if warnings else "ready"),
        "blocking": blocking,
        "warnings": warnings,
        "noncanonical_main_worktrees": items,
        "canonical_branch": canonical_git.get("branch"),
    }


def _cleanup_janitor_report(canonical_root: Path | None = None, *, sample_limit: int = 5) -> dict[str, Any]:
    """Return a compact, read-only branch/worktree cleanup debt summary.

    The janitor intentionally reports counts and tiny samples only; deep cleanup
    decisions still use cleanup-after-merge/read-only triage to avoid noisy
    doctor output and token-heavy branch dumps.
    """
    root = canonical_root or _canonical_root()
    local_branches = set(
        item.strip()
        for item in _git(["for-each-ref", "--format=%(refname:short)", "refs/heads"], cwd=root, check=False).splitlines()
        if item.strip() and item.strip() != "main"
    )
    merged_refs = set(
        item.strip()
        for item in _git(["branch", "--format=%(refname:short)", "--merged", "origin/main"], cwd=root, check=False).splitlines()
        if item.strip() and item.strip() != "main"
    )
    checked_out: set[str] = set()
    for item in _parse_worktree_list():
        branch_ref = item.get("branch") or ""
        branch = branch_ref.removeprefix("refs/heads/")
        if branch:
            checked_out.add(branch)
    stale_backup_or_temp = sorted(
        branch
        for branch in local_branches
        if branch not in checked_out
        and (
            branch.startswith("backup/")
            or branch.startswith("temp/")
            or branch.startswith("tmp/")
            or "temp-check" in branch
        )
    )
    safe_merged_local = sorted(branch for branch in merged_refs if branch in local_branches and branch not in checked_out)
    checked_out_merged = sorted(branch for branch in merged_refs if branch in checked_out)
    return {
        "schema_version": "aistock_cleanup_janitor_v1",
        "workflow_gate": "warning" if (safe_merged_local or stale_backup_or_temp or checked_out_merged) else "ready",
        "sample_limit": sample_limit,
        "safe_merged_local_branch_count": len(safe_merged_local),
        "safe_merged_local_branch_samples": safe_merged_local[:sample_limit],
        "stale_backup_or_temp_branch_count": len(stale_backup_or_temp),
        "stale_backup_or_temp_branch_samples": stale_backup_or_temp[:sample_limit],
        "checked_out_merged_branch_count": len(checked_out_merged),
        "checked_out_merged_branch_samples": checked_out_merged[:sample_limit],
        "next_command": (
            "python scripts/aistock_issue_workflow.py cleanup-after-merge --branch <branch> --pr-url <merged-pr-url> "
            "--worktree <task-worktree> --sync-root --apply"
        ),
        "policy": "compact_counts_only; do not print full branch/worktree lists in doctor output",
    }


def _branch_for_path(path: Path) -> str | None:
    target = str(path.resolve()).replace("\\", "/").lower()
    for item in _parse_worktree_list():
        worktree = item.get("worktree")
        if worktree and str(Path(worktree).resolve()).replace("\\", "/").lower() == target:
            branch_ref = item.get("branch") or ""
            return branch_ref.removeprefix("refs/heads/") or None
    return None


def _state_issue_json_path(root: Path, state: dict[str, Any]) -> Path | None:
    raw = str(state.get("source_bug_json") or state.get("target_bug_json") or "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = root / path
    return path


def _registry_commit_next_command(worktree: str | Path, bug_id: str, *, continue_fix: bool = False, issue_json: str | None = None) -> str:
    parts = [
        f"cd /d {worktree}",
        "git status --short",
        f"git add {' '.join(COMMITTABLE_BUG_REGISTRY_PATHS)}",
        f'git commit -m "chore(issue): register {bug_id}"',
    ]
    if continue_fix:
        issue_arg = f' --issue-json "{issue_json}"' if issue_json else " --issue-json <BUG_JSON>"
        parts.append(f"python scripts/aistock_issue_workflow.py run --bug-id {bug_id}{issue_arg} --mode plan --create-worktree")
    return " && ".join(parts)


def _commit_bug_registration_in_fix_worktree(root: Path, bug_id: str) -> dict[str, Any]:
    dirty = [
        path for path in _dirty_files(root)
        if path.replace("\\", "/").startswith("tests/aistock_validation/bugs/")
    ]
    if not dirty:
        return {"workflow_gate": "no_changes", "root": str(root), "branch": _branch_for_path(root)}
    unexpected = sorted(
        path for path in _dirty_files(root)
        if not path.replace("\\", "/").startswith("tests/aistock_validation/bugs/")
    )
    if unexpected:
        raise WorkflowError(
            "fix-chain registration worktree has unexpected dirty files outside BUG registry: "
            + ", ".join(unexpected[:10])
        )
    started = time.monotonic()
    actions: list[dict[str, Any]] = []
    add = _run_command(["git", "add", *COMMITTABLE_BUG_REGISTRY_PATHS], cwd=root, timeout=60)
    actions.append({"command": f"git add {' '.join(COMMITTABLE_BUG_REGISTRY_PATHS)}", "result": add})
    if not add.get("ok"):
        raise WorkflowError(add.get("stderr") or add.get("stdout") or "fix-chain registration git add failed")
    commit = _run_command(["git", "commit", "-m", f"chore(issue): register {bug_id}"], cwd=root, timeout=120)
    actions.append({"command": f"git commit -m chore(issue): register {bug_id}", "result": commit})
    if not commit.get("ok") and "nothing to commit" not in f"{commit.get('stdout')}\n{commit.get('stderr')}".lower():
        raise WorkflowError(commit.get("stderr") or commit.get("stdout") or "fix-chain registration git commit failed")
    commit_sha = _run_command(["git", "rev-parse", "--short=12", "HEAD"], cwd=root, timeout=30)
    return {
        "workflow_gate": "committed",
        "root": str(root),
        "branch": _branch_for_path(root),
        "changed_files": dirty,
        "actions": actions,
        "commit": str(commit_sha.get("stdout") or "").strip() if commit_sha.get("ok") else None,
        "duration_seconds": round(time.monotonic() - started, 3),
    }


def _issue_json_path_for_worktree(source_path: Path, target_root: Path) -> Path:
    if _is_inside(source_path, target_root):
        return source_path
    normalized_parts = [part.replace("\\", "/") for part in source_path.parts]
    for index in range(len(normalized_parts) - 2):
        if normalized_parts[index : index + 3] == ["tests", "aistock_validation", "bugs"]:
            return target_root / Path(*source_path.parts[index:])
    try:
        relative = source_path.resolve().relative_to(REPO_ROOT.resolve())
    except ValueError:
        try:
            relative = source_path.resolve().relative_to(_canonical_root().resolve())
        except ValueError:
            relative = Path("tests") / "aistock_validation" / "bugs" / source_path.name
    return target_root / relative


def _ensure_issue_json_in_worktree(source_path: Path, target_root: Path, *, record: dict[str, Any] | None = None) -> Path:
    target = _issue_json_path_for_worktree(source_path, target_root)
    if target.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    if source_path.exists():
        target.write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")
    elif record is not None:
        _write_json(target, record)
    else:
        raise WorkflowError(f"BUG JSON source does not exist: {source_path}")
    return target


def _workflow_role_for_state(root: Path, state: dict[str, Any], git: dict[str, Any] | None = None) -> str:
    explicit = str(state.get("workflow_role") or "").strip().lower()
    if explicit:
        return explicit
    if state.get("registry_pr_only"):
        return "registry_intake"
    branch = str(state.get("branch") or (git or {}).get("branch") or _branch_for_path(root) or "")
    worktree_name = Path(str(state.get("worktree") or root)).name
    if branch.startswith("bug/registry-") or worktree_name.startswith("registry-"):
        return "registry_intake"
    if branch.startswith("chore/") and "close-sync" in branch:
        return "close_sync"
    if "close-sync" in worktree_name:
        return "close_sync"
    return "fix"


def _workflow_state_sort_key(root: Path, state: dict[str, Any]) -> tuple[int, str, str]:
    state_rank = {
        "discovered": 10,
        "context_ready": 20,
        "fix_in_progress": 30,
        "fix_applied": 40,
        "validation_planned": 50,
        "validation_running": 60,
        "validation_passed": 70,
        "pushed": 80,
        "pr_opened": 90,
        "ci_running": 100,
        "ci_green": 110,
        "merged": 120,
        "close_synced": 130,
        "cleanup_done": 140,
        "complete": 150,
    }
    role = _workflow_role_for_state(root, state)
    if role.startswith("registry"):
        role_bonus = -100
    elif role == "close_sync":
        role_bonus = 0
    else:
        role_bonus = 100
    pr_bonus = 25 if state.get("pr_url") else 0
    worktree_bonus = 10 if state.get("worktree") else 0
    score = role_bonus + pr_bonus + worktree_bonus + state_rank.get(str(state.get("state") or ""), 0)
    return (score, str(state.get("updated_at") or ""), str(root))


def _state_is_active(state: dict[str, Any]) -> bool:
    value = str(state.get("state") or "")
    if not value:
        return False
    if state.get("planned_worktree") and not state.get("worktree"):
        return False
    if value in TERMINAL_WORKFLOW_STATES:
        return False
    return value in ACTIVE_WORKFLOW_STATES or value not in {"fixed", "verified", "wontfix"}


def _active_workflows_for_bug(bug_id: str) -> list[dict[str, Any]]:
    canonical_bug_id = bug_id.strip().upper()
    active: list[dict[str, Any]] = []
    for root in _state_roots_for_bug(canonical_bug_id):
        state = _load_state(canonical_bug_id, root)
        if not state or not _state_is_active(state):
            continue
        worktree = Path(str(state.get("worktree") or root))
        if not worktree.is_absolute():
            worktree = root / worktree
        if state.get("worktree") and not worktree.exists():
            continue
        git = _git_snapshot(worktree) if worktree.exists() else {"ok": False, "error": f"workflow worktree missing: {worktree}"}
        role = _workflow_role_for_state(root, state, git)
        issue_json = _state_issue_json_path(root, state)
        active.append(
            {
                "bug_id": canonical_bug_id,
                "root": str(root),
                "worktree": str(worktree),
                "branch": state.get("branch") or git.get("branch") or _branch_for_path(worktree),
                "workflow_role": role,
                "issue_json": str(issue_json) if issue_json and issue_json.exists() else None,
                "state": state.get("state"),
                "state_path": _repo_rel(_state_path(canonical_bug_id, root), root),
                "dirty": bool(git.get("dirty")),
                "dirty_count": git.get("dirty_count"),
                "git": git,
                "next_command": _next_command_for_state(canonical_bug_id, state),
            }
        )
    return active


def _active_worktree_decision(
    *,
    bug_id: str,
    create_worktree: bool,
    force_new_worktree: bool,
    force_reason: str | None,
) -> dict[str, Any]:
    active = _active_workflows_for_bug(bug_id)
    decision: dict[str, Any] = {
        "bug_id": bug_id,
        "create_worktree_requested": create_worktree,
        "force_new_worktree": force_new_worktree,
        "force_reason": force_reason,
        "active_workflows": active,
        "decision": "create_or_continue",
        "workflow_gate": "ready",
        "blocking": [],
        "warnings": [],
        "next_command": None,
        "rescue_checklist": [],
    }
    if not create_worktree or not active:
        return decision
    registry_active = [item for item in active if str(item.get("workflow_role") or "").startswith("registry")]
    fix_active = [item for item in active if item not in registry_active]
    if registry_active and not fix_active:
        dirty_registry = [item for item in registry_active if item.get("dirty")]
        first_registry = sorted(
            registry_active,
            key=lambda item: (
                1 if item.get("issue_json") else 0,
                str(item.get("state") or ""),
                str(item.get("root") or ""),
            ),
            reverse=True,
        )[0]
        if dirty_registry:
            dirty_first = dirty_registry[0]
            decision["decision"] = "blocked_dirty_registry_intake"
            decision["workflow_gate"] = "blocked"
            decision["blocking"].append(
                f"registry intake worktree is dirty and must be committed before creating a separate fix worktree: {dirty_first.get('worktree')}"
            )
            decision["next_command"] = _registry_commit_next_command(dirty_first.get("worktree") or REPO_ROOT, bug_id)
            decision["rescue_checklist"] = [
                "commit_or_pr_the_registry_intake_branch",
                "do_not_edit_code_in_the_registry_intake_worktree",
                "rerun_with_create_worktree_to_build_the_fix_worktree",
            ]
            return decision
        registry_issue_json = first_registry.get("issue_json")
        if not registry_issue_json:
            decision["decision"] = "blocked_registry_issue_json_missing"
            decision["workflow_gate"] = "blocked"
            decision["blocking"].append("registry intake workflow does not record a readable source_bug_json")
            decision["next_command"] = f"python scripts/aistock_issue_workflow.py run --bug-id {bug_id} --issue-json <BUG_JSON> --mode plan --create-worktree"
            return decision
        decision["decision"] = "create_fix_from_registry_intake"
        decision["workflow_gate"] = "ready"
        decision["registry_intake_workflows"] = registry_active
        decision["registry_issue_json"] = registry_issue_json
        decision["warnings"].append(
            "only registry intake workflow exists; creating a separate fix worktree from latest origin/main with the registry BUG JSON"
        )
        decision["next_command"] = (
            f"python scripts/aistock_issue_workflow.py run --bug-id {bug_id} --issue-json \"{registry_issue_json}\" "
            "--mode plan --create-worktree"
        )
        return decision
    active_for_decision = fix_active or active
    dirty = [item for item in active_for_decision if item.get("dirty")]
    if force_new_worktree:
        if not str(force_reason or "").strip():
            decision["decision"] = "blocked_dirty_active" if dirty else "blocked_requires_force_reason"
            decision["workflow_gate"] = "blocked"
            decision["blocking"].append("--force-new-worktree requires --reason so the exception is auditable")
            return decision
        decision["decision"] = "force_new_worktree"
        decision["workflow_gate"] = "warning"
        decision["warnings"].append("active workflow exists, but --force-new-worktree was supplied with a reason")
        return decision
    first = active_for_decision[0]
    if dirty:
        dirty_first = dirty[0]
        decision["decision"] = "blocked_dirty_active"
        decision["workflow_gate"] = "blocked"
        decision["blocking"].append(f"active workflow worktree is dirty: {dirty_first.get('worktree')}")
        decision["next_command"] = f"python scripts/aistock_issue_workflow.py resume --bug-id {bug_id} --worktree \"{dirty_first.get('worktree')}\""
        decision["rescue_checklist"] = [
            "switch_to_active_worktree",
            "inspect_git_status_without_reset_or_clean",
            "commit_or_stash_task_files_if_they_belong_to_this_issue",
            "rerun_resume_and_continue_existing_workflow",
            "use_force_new_worktree_only_with_an_audited_reason",
        ]
        return decision
    decision["decision"] = "resume_existing"
    decision["workflow_gate"] = "resume"
    decision["next_command"] = f"python scripts/aistock_issue_workflow.py resume --bug-id {bug_id} --worktree \"{first.get('worktree')}\""
    return decision


def _github_issue_url(number: int | str) -> str:
    return f"https://github.com/{GITHUB_REPO}/issues/{number}"


def _extract_run_id_from_issue_body(body: str) -> str | None:
    marker = re.search(r"aistock-issue-on-test-fail:(\d+)", body or "")
    if marker:
        return marker.group(1)
    run_url = re.search(r"github\.com/[^/]+/[^/]+/actions/runs/(\d+)", body or "")
    return run_url.group(1) if run_url else None


def _issue_body_failure_text(body: str) -> str:
    """Keep CI classification focused on failure evidence, not generic checklists."""
    text = str(body or "")
    return re.split(r"\n##\s+(Agent Handoff|Suggested Triage|BUG JSON Linkage|Production Gates)\b", text, maxsplit=1)[0]


def _extract_regression_locator_from_issue_body(body: str, summary: dict[str, Any]) -> dict[str, Any] | None:
    text = str(body or "")
    status_match = re.search(r"last_green_status:\s*`?([A-Za-z0-9_-]+)`?", text)
    range_match = re.search(r"commit_range:\s*`?([0-9a-fA-F.]+)`?", text)
    previous_match = re.search(r"previous_success_run:\s*(https://github\.com/[^\s`]+/actions/runs/(\d+))", text)
    if not (status_match or range_match or previous_match):
        return None
    previous_run = None
    if previous_match:
        previous_run = {"run_url": previous_match.group(1), "run_id": previous_match.group(2)}
    return {
        "schema_version": "aistock_ci_last_green_locator_v1",
        "status": status_match.group(1) if status_match else "found",
        "commit_range": range_match.group(1) if range_match else None,
        "previous_success_run": previous_run,
        "current_run": {
            "workflow": summary.get("workflow"),
            "run_id": str(summary.get("run_id") or ""),
            "run_url": summary.get("run_url"),
            "branch": summary.get("branch"),
            "commit": summary.get("commit"),
        },
        "blocking_for_issue_workflow": False,
        "source": "github_issue_body",
        "warnings": [],
    }


def _merge_issue_body_regression_locator(summary: dict[str, Any], body: str) -> dict[str, Any]:
    locator = _extract_regression_locator_from_issue_body(body, summary)
    if not locator:
        return summary
    current = summary.get("last_green_locator") if isinstance(summary.get("last_green_locator"), dict) else {}
    if current and current.get("status") not in {None, "", "not_found", "unknown", "not_requested"}:
        return summary
    enriched = dict(summary)
    enriched["last_green_locator"] = locator
    notes = list(enriched.get("triage_notes") or [])
    notes.append("last_green_locator recovered from GitHub Issue body")
    enriched["triage_notes"] = notes
    return enriched


def _find_superseding_main_success(summary: dict[str, Any]) -> dict[str, Any] | None:
    workflow = str(summary.get("workflow") or "").strip()
    branch = str(summary.get("branch") or "").strip()
    run_id = str(summary.get("run_id") or "").strip()
    if not workflow or not branch or not run_id.isdigit():
        return None
    result = _run_command(
        [
            "gh",
            "run",
            "list",
            "--repo",
            GITHUB_REPO,
            "--branch",
            branch,
            "--workflow",
            workflow,
            "--limit",
            "20",
            "--json",
            "databaseId,headSha,conclusion,status,url,createdAt,displayTitle",
        ],
        cwd=REPO_ROOT,
        timeout=30,
    )
    if not result.get("ok"):
        return None
    try:
        runs = json.loads(str(result.get("stdout") or "[]"))
    except json.JSONDecodeError:
        return None
    if not isinstance(runs, list):
        return None
    current_id = int(run_id)
    for item in runs:
        if not isinstance(item, dict):
            continue
        candidate_id = str(item.get("databaseId") or "")
        if not candidate_id.isdigit() or int(candidate_id) <= current_id:
            continue
        if str(item.get("status") or "").lower() != "completed":
            continue
        if str(item.get("conclusion") or "").lower() != "success":
            continue
        return {
            "run_id": candidate_id,
            "run_url": item.get("url"),
            "head_sha": item.get("headSha"),
            "created_at": item.get("createdAt"),
            "display_title": item.get("displayTitle"),
            "branch": branch,
            "supersede_scope": "main" if branch == "main" else "same_branch",
        }
    return None


def _superseded_success_phrase(superseding_run: dict[str, Any], workflow: str | None) -> str:
    workflow_name = workflow or "CI"
    branch = str(superseding_run.get("branch") or "").strip()
    if superseding_run.get("supersede_scope") == "same_branch" and branch:
        return f"later successful {workflow_name} run on the same branch {branch}"
    return f"later successful main {workflow_name} run"


def _load_github_issue(issue_number: int | str) -> dict[str, Any]:
    result = _execute_checked(
        [
            "gh",
            "issue",
            "view",
            str(issue_number),
            "--repo",
            GITHUB_REPO,
            "--json",
            "number,title,state,body,url,labels,createdAt,updatedAt",
        ],
        cwd=REPO_ROOT,
        timeout=60,
    )
    try:
        payload = json.loads(str(result.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"cannot parse GitHub issue #{issue_number}: {exc}") from exc
    if not payload.get("number"):
        raise WorkflowError(f"GitHub issue not found or unreadable: {issue_number}")
    return payload


def _rest_pr_search_for_bug(bug_id: str, *, state: str) -> list[dict[str, Any]]:
    """Read BUG-specific PR state through REST without scanning repository history."""
    if state not in {"open", "merged"}:
        raise WorkflowError(f"unsupported PR search state: {state}")
    qualifier = "is:open" if state == "open" else "is:merged"
    query = f"repo:{GITHUB_REPO} is:pr {qualifier} {bug_id} in:title,body"
    search = _run_transport_read_with_retry(
        ["gh", "api", "--method", "GET", "search/issues", "-f", f"q={query}", "-f", "per_page=20"],
        cwd=REPO_ROOT,
        timeout=60,
        attempts=2,
    )
    if not search.get("ok"):
        detail = search.get("stderr") or search.get("stdout") or "unknown REST search error"
        raise WorkflowError(f"cannot search {state} PRs through REST: {detail}")
    try:
        payload = json.loads(str(search.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"cannot parse {state} PR REST search: {exc}") from exc
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise WorkflowError(f"{state} PR REST search returned an invalid item list")
    rows: list[dict[str, Any]] = []
    for item in items:
        number = int(item.get("number") or 0) if isinstance(item, dict) else 0
        if not number:
            continue
        detail = _run_transport_read_with_retry(
            ["gh", "api", f"repos/{GITHUB_REPO}/pulls/{number}"],
            cwd=REPO_ROOT,
            timeout=60,
            attempts=2,
        )
        if not detail.get("ok"):
            message = detail.get("stderr") or detail.get("stdout") or "unknown REST PR detail error"
            raise WorkflowError(f"cannot read PR #{number} through REST: {message}")
        try:
            pr = json.loads(str(detail.get("stdout") or "{}"))
        except json.JSONDecodeError as exc:
            raise WorkflowError(f"cannot parse PR #{number} REST readback: {exc}") from exc
        if not isinstance(pr, dict):
            raise WorkflowError(f"PR #{number} REST readback is not an object")
        if state == "merged" and not pr.get("merged_at"):
            continue
        head = pr.get("head") if isinstance(pr.get("head"), dict) else {}
        rows.append(
            {
                "number": pr.get("number"),
                "title": pr.get("title"),
                "url": pr.get("html_url"),
                "headRefName": head.get("ref"),
                "headRefOid": head.get("sha"),
                "mergedAt": pr.get("merged_at"),
                "body": pr.get("body"),
                "source": "github_rest_search",
            }
        )
    return rows


def _stale_pr_check_for_bug(bug_id: str) -> dict[str, Any]:
    if not (REPO_ROOT / ".git").exists():
        return {"status": "skipped_no_git_checkout", "open_prs": [], "merged_prs": []}
    result_open = _run_command(
        [
            "gh",
            "pr",
            "list",
            "--repo",
            GITHUB_REPO,
            "--state",
            "open",
            "--search",
            f"{bug_id} in:title,body",
            "--json",
            "number,title,url,headRefName,body",
            "--limit",
            "20",
        ],
        cwd=REPO_ROOT,
        timeout=20,
    )
    result_merged = _run_command(
        [
            "gh",
            "pr",
            "list",
            "--repo",
            GITHUB_REPO,
            "--state",
            "merged",
            "--search",
            f"{bug_id} in:title,body",
            "--json",
            "number,title,url,headRefName,mergedAt,body",
            "--limit",
            "20",
        ],
        cwd=REPO_ROOT,
        timeout=20,
    )
    def parse(result: dict[str, Any]) -> list[dict[str, Any]] | None:
        if not result.get("ok"):
            return None
        try:
            data = json.loads(str(result.get("stdout") or "[]"))
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, list) else None

    open_prs = parse(result_open)
    merged_prs = parse(result_merged)
    fallback_states: list[str] = []
    try:
        if open_prs is None:
            open_prs = _rest_pr_search_for_bug(bug_id, state="open")
            fallback_states.append("open")
        if merged_prs is None:
            merged_prs = _rest_pr_search_for_bug(bug_id, state="merged")
            fallback_states.append("merged")
    except WorkflowError as exc:
        return {
            "status": "unavailable",
            "open_prs": open_prs or [],
            "merged_prs": merged_prs or [],
            "error": str(exc),
            "fallback_states": fallback_states,
        }
    cleanup_needed = bool(open_prs and merged_prs)
    return {
        "status": "cleanup_recommended" if cleanup_needed else "checked",
        "open_prs": open_prs,
        "merged_prs": merged_prs,
        "fallback_states": fallback_states,
        "cleanup_plan": [
            "inspect open registry-only PRs for this BUG",
            "close stale registry-only PR if a merged fix PR already resolved the BUG",
        ] if cleanup_needed else [],
    }


def _find_bug_by_github_issue(issue_number: int | str) -> tuple[dict[str, Any], Path] | None:
    target = int(issue_number)
    for path in _bug_files():
        record = _load_json(path)
        if int(record.get("github_issue_number") or 0) == target:
            return record, path
    return None


def _shell_quote(value: str) -> str:
    text = str(value)
    return '"' + text.replace('"', '\\"') + '"'


def _github_issue_label_names(issue: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for item in issue.get("labels") or []:
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
        else:
            name = str(item or "").strip()
        if name:
            names.append(name)
    return flow._unique_strings(names)


def _infer_bug_module_from_github_issue(issue: dict[str, Any]) -> str | None:
    for label in _github_issue_label_names(issue):
        lower = label.lower()
        if lower.startswith("module:"):
            value = lower.split(":", 1)[1].strip()
            return value or None
        if lower == "paper-v2":
            return "paper_v2"
        if lower == "qe":
            return "qe"
    return None


def _infer_bug_severity_from_github_issue(issue: dict[str, Any]) -> str | None:
    text = " ".join([str(issue.get("title") or ""), *_github_issue_label_names(issue)])
    match = re.search(r"\bP([0-3])\b", text, flags=re.IGNORECASE)
    return f"P{match.group(1)}" if match else None


def _adopt_bug_command(*, bug_id: str, title: str, module: str | None, severity: str | None, issue_number: Any, issue_url: str) -> str:
    module_arg = module or "<module>"
    severity_arg = severity or "P1"
    return (
        "python scripts/aistock_issue_workflow.py submit-bug "
        f"--bug-id {bug_id} --title {_shell_quote(title)} "
        f"--module {module_arg} --severity {severity_arg} "
        f"--github-issue-number {issue_number} --github-issue-url {issue_url} "
        "--create-fix-worktree --apply"
    )


def _missing_bug_record_recovery_payload(bug_id: str) -> dict[str, Any]:
    normalized = bug_id.strip().upper()
    github_issue, warnings = _github_bug_issue_for_id(normalized)
    if not github_issue:
        return {
            "schema_version": "aistock_issue_workflow_missing_bug_record_v1",
            "generated_at": _utc_now(),
            "bug_id": normalized,
            "workflow_gate": "blocked",
            "reason": "local_bug_json_missing",
            "blocking": [f"BUG record not found: {normalized}"],
            "warnings": warnings,
            "next_command": _adopt_bug_command(
                bug_id=normalized,
                title=f"{normalized} <title>",
                module=None,
                severity=None,
                issue_number="<issue_number>",
                issue_url="<issue_url>",
            ),
        }

    issue_number = github_issue.get("github_issue_number")
    issue_url = str(github_issue.get("source") or _github_issue_url(issue_number))
    issue_title = str(github_issue.get("title") or normalized)
    module = _infer_bug_module_from_github_issue(github_issue)
    severity = _infer_bug_severity_from_github_issue(github_issue)
    return {
        "schema_version": "aistock_issue_workflow_missing_bug_record_v1",
        "generated_at": _utc_now(),
        "bug_id": normalized,
        "workflow_gate": "missing_bug_record",
        "reason": "github_issue_exists_without_local_bug_json",
        "github_issue": {
            "number": issue_number,
            "url": issue_url,
            "state": github_issue.get("github_state"),
            "title": issue_title,
            "labels": _github_issue_label_names(github_issue),
        },
        "inferred_module": module,
        "inferred_severity": severity,
        "blocking": [f"{normalized} exists on GitHub but local BUG JSON is missing from this checkout"],
        "warnings": warnings,
        "next_actions": [
            "adopt_or_reconstruct_bug_json_in_isolated_worktree",
            "avoid_creating_duplicate_bug_id",
        ],
        "next_command": _adopt_bug_command(
            bug_id=normalized,
            title=issue_title,
            module=module,
            severity=severity,
            issue_number=issue_number,
            issue_url=issue_url,
        ),
    }


def find_bug_record(bug_id: str | None = None, issue_json: str | None = None) -> tuple[dict[str, Any], Path]:
    if issue_json:
        path = Path(issue_json)
        record = _load_json(path)
        return record, path
    if not bug_id:
        raise WorkflowError("Either --bug-id or --issue-json is required")
    normalized = bug_id.strip().upper()
    target_number = _bug_id_number(normalized)
    matches: list[tuple[dict[str, Any], Path]] = []
    candidates: list[Path] = []
    for path in _bug_files():
        filename_number = _bug_id_number_from_filename(path.name)
        if filename_number == target_number or filename_number is None:
            candidates.append(path)
    for path in candidates:
        record = _load_json(path)
        if str(record.get("bug_id") or "").upper() == normalized:
            matches.append((record, path))
    if not matches:
        raise WorkflowPayloadError(_missing_bug_record_recovery_payload(normalized))
    if len(matches) > 1:
        raise WorkflowError(f"Multiple BUG records found for {normalized}: {[str(path) for _, path in matches]}")
    return matches[0]


def _find_bug_record_from_active_registry(bug_id: str) -> tuple[dict[str, Any], Path] | None:
    for item in sorted(
        _active_workflows_for_bug(bug_id),
        key=lambda entry: (
            1 if str(entry.get("workflow_role") or "").startswith("registry") else 0,
            1 if entry.get("issue_json") else 0,
            str(entry.get("root") or ""),
        ),
        reverse=True,
    ):
        if not str(item.get("workflow_role") or "").startswith("registry"):
            continue
        issue_json = item.get("issue_json")
        if not issue_json:
            continue
        path = Path(str(issue_json))
        if path.exists():
            return _load_json(path), path
    return None



def _normalize_module_label(module: str | None) -> str:
    module_label = re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(module or "unknown").strip().lower()).strip("_")
    return module_label or "unknown"


def _small_text_blob(parts: Iterable[str | None]) -> str:
    text = ""
    for part in parts:
        if part is None:
            continue
        text += f"\n{part}"
    return text


def _csv_arg(items: Iterable[str]) -> str:
    result = ""
    for item in items:
        if not item:
            continue
        result = item if not result else f"{result},{item}"
    return result


def _delimited_text(items: Iterable[Any], separator: str) -> str:
    result = ""
    for item in items:
        value = str(item)
        result = value if not result else f"{result}{separator}{value}"
    return result


def _matches_ui_keyword(haystack: str, token: str) -> bool:
    normalized = token.lower()
    if normalized.isascii():
        return re.search(rf"(?<![A-Za-z0-9_-]){re.escape(normalized)}(?![A-Za-z0-9_-])", haystack) is not None
    return normalized in haystack


def _text_indicates_cleanup_fast(title: str | None, description: str | None = None, actual: str | None = None, expected: str | None = None) -> bool:
    haystack = _small_text_blob([str(title or ""), str(description or ""), str(actual or ""), str(expected or "")]).lower()
    cleanup_terms = (
        "cleanup",
        "scratch",
        "untracked",
        "root pollution",
        "temporary file",
        "临时",
        "污染",
        "清理",
        "归档",
    )
    docs_terms = ("docs", "documentation", "handoff", "analysis doc", "文档")
    return any(term in haystack for term in cleanup_terms) and any(term in haystack for term in docs_terms)


def _text_indicates_workflow_policy(title: str | None, description: str | None = None, module: str | None = None) -> bool:
    haystack = _small_text_blob([str(title or ""), str(description or ""), str(module or "")]).lower()
    workflow_terms = ("workflow", "流水线", "流程", "validation", "ci", "nightly", "bug处理")
    policy_terms = ("budget", "token", "over-validates", "验证预算", "过度验证", "规范", "policy")
    return any(term in haystack for term in workflow_terms) and any(term in haystack for term in policy_terms)


def _is_ui_issue(title: str | None, module: str | None, changed_files: list[str], description: str | None = None) -> bool:
    normalized_paths = [path.replace("\\", "/") for path in changed_files]
    has_frontend_scope = any(path.startswith("frontend/") or "/frontend/" in path for path in normalized_paths)
    has_ui_catalog_scope = any(
        path.startswith("frontend/tests/")
        or path.startswith("tests/aistock_validation/catalog/ui_targets")
        for path in normalized_paths
    )
    if has_frontend_scope or has_ui_catalog_scope:
        return True
    # UI words in workflow scripts or command text (for example statusCheckRollup)
    # must not create false visual-acceptance routes.
    if changed_files and not any(path.startswith(("frontend/", "tests/e2e/", "playwright")) for path in normalized_paths):
        return False
    if not changed_files and _text_indicates_cleanup_fast(title, description):
        return False
    if not changed_files and _text_indicates_workflow_policy(title, description, module):
        return False
    haystack = _small_text_blob([str(title or ""), str(module or ""), str(description or "")]).lower()
    keywords = UI_KEYWORDS
    if not changed_files:
        # Text-only BUG reports often mention BUG JSON or paths such as
        # "historical/design"; neither is enough to infer a visual UI issue.
        keywords = tuple(token for token in UI_KEYWORDS if token.lower() != "json")
    return any(_matches_ui_keyword(haystack, token) for token in keywords)


def _is_cleanup_fast_candidate(record: dict[str, Any]) -> bool:
    return _text_indicates_cleanup_fast(
        str(record.get("title") or ""),
        str(record.get("description") or ""),
        str(record.get("actual") or ""),
        str(record.get("expected") or ""),
    )


def _verification_budget_for_record(
    record: dict[str, Any],
    ui_hints: dict[str, Any] | None = None,
    validation_budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    module = _normalize_module_label(record.get("module"))
    severity = str(record.get("severity") or "").upper().split()[0] if str(record.get("severity") or "").strip() else ""
    required = [str(item) for item in flow._as_list(record.get("required_verification"))]
    has_production_gate = any(
        str(record.get(key) or "noop") not in {"", "noop"}
        for key in ("production_ddl_gate", "production_backend_dependency_gate", "production_frontend_dependency_gate")
    )
    scope_files = flow._unique_strings(
        [
            *flow._as_list(record.get("allowed_write_scope")),
            *flow._as_list((record.get("file_scope_contract") or {}).get("scope_files")),
        ]
    )
    normalized_scope = [str(path).replace("\\", "/").lower() for path in scope_files]
    schema_scope = (
        module in {"database", "db", "platform.database", "validation.database"}
        or any(
            flow._requires_production_ddl(path)
            or path.startswith(("backend/db/", "scripts/db/"))
            for path in normalized_scope
        )
    )
    high_risk_modules = {"paper_v2", "strategy_package", "selection_center", "research_assistant", "validation_center"}
    runtime_markers = ("runtime", "order", "cash", "position", "miniqmt", "broker", "ddl", "migration")
    text = _small_text_blob(
        [str(record.get("title") or ""), str(record.get("description") or ""), str(record.get("actual") or ""), str(record.get("expected") or "")]
    ).lower()
    if has_production_gate or (
        schema_scope and any(marker in text for marker in ("ddl", "migration", "production db"))
    ):
        budget = "deep"
        target_pct = "45-60%"
    elif severity in {"P0", "P1"} or module in high_risk_modules or any(marker in text for marker in runtime_markers):
        budget = "standard"
        target_pct = "30-45%"
    elif ui_hints:
        budget = "light_ui"
        target_pct = "30-40%"
    else:
        budget = "light"
        target_pct = "25-35%"
    split = _split_validation_budget_items(required)
    if validation_budget is not None:
        local_plans = flow._unique_strings(validation_budget.get("required_plans") or [])
        deferred_plans = flow._unique_strings(validation_budget.get("deferred_nightly_plans") or [])
    else:
        local_plans = flow._unique_strings(split["local"] or ["l0"])
        deferred_plans = flow._unique_strings(split["deferred"])
    deferred_modules = _deferred_modules_from_plans(module, deferred_plans)
    return {
        "schema_version": "aistock_verification_budget_v1",
        "budget": budget,
        "target_cost_percent_of_legacy": target_pct,
        "premerge_gate": [
            "changed-file lint/compile",
            "direct fix-point targeted test or API/contract smoke",
            "git diff --check",
            "production gates",
        ],
        "premerge_required_plans": local_plans,
        "delegated_validation": {
            "skill": "aistock-validation-delegation",
            "use_when": "broad UI/API/business-flow, LLM design-drift, or cross-module validation exceeds the local gate",
            "receipt_default": "compact",
        },
        "local_loop_policy": {
            "failure_resume_first": ["pytest <path>::<test_name> -q", "pytest --lf -q", "pytest --ff -x -q"],
            "max_final_related_matrix_runs": 1,
            "no_repeat_rule": "do not rerun broad or full related suites after every edit; rerun failed nodeids first",
            "delegate_when": [
                "local validation or exploration exceeds 30 minutes",
                "exploration commands exceed the task-card soft limit",
                "validation needs broad module, UI/API, business-flow, or cross-module coverage",
                "the relevant small matrix already passed and only non-behavioral edits followed",
            ],
        },
        "deferred_nightly_verification": {
            "required": bool(deferred_plans),
            "modules": [item for item in deferred_modules if item],
            "plans": deferred_plans,
            "scope": "deduplicate all merged BUG/PR changes for the day and run deep UI/API/business-flow validation once in nightly or delegated VC/CI runs",
        },
    }


def _ui_intake_hints(
    *,
    title: str,
    module: str,
    description: str | None,
    changed_files: list[str],
    reproduce_command: str | None,
) -> dict[str, Any] | None:
    if not _is_ui_issue(title, module, changed_files, description):
        return None
    normalized_module = _normalize_module_label(module)
    hint = UI_ROUTE_HINTS.get(normalized_module) or next(
        (value for key, value in UI_ROUTE_HINTS.items() if key in normalized_module),
        None,
    )
    route = None
    search_text = _small_text_blob([title, description or "", *changed_files])
    route_match = re.search(r"(/[a-zA-Z0-9_.~:/?#\[\]@!$&'()*+,;=-]+)", search_text)
    if route_match:
        route = route_match.group(1).rstrip(".,;。；")
    routes = list((hint or {}).get("routes") or [])
    if route and route not in routes:
        routes.insert(0, route)
    scope = flow._unique_strings(list((hint or {}).get("scope") or []) + changed_files)
    verification = flow._unique_strings(list((hint or {}).get("verification") or []) + ["l0"])
    reproduce_missing = not reproduce_command or reproduce_command.strip().lower() in {"n/a", "na", "none"}
    return {
        "schema_version": "aistock_ui_issue_intake_hints_v1",
        "ui_issue": True,
        "scope_source": "inferred_from_module_route_and_changed_files",
        "ui_route": routes[0] if routes else None,
        "ui_routes": routes,
        "ui_component_scope": scope,
        "recommended_verification": verification,
        "reproduce_required": reproduce_missing,
        "reproduce_template": [
            "Open the affected AIstock page/route.",
            "Perform the visible user action that currently fails or looks wrong.",
            "Verify the expected visible state, table, dialog, or form behavior.",
        ],
        "visual_acceptance_required": True,
        "labels": ["bug", "ui", f"module:{normalized_module}", "type:bug"],
    }


def _issue_labels_for_bug(*, module: str, severity: str, ui_hints: dict[str, Any] | None = None) -> list[str]:
    normalized_module = _normalize_module_label(module)
    module_label = f"module:{normalized_module}"
    if normalized_module in {"advisory", "paper_v2_advisory"}:
        module_label = "module:paper_v2"
    labels = ["aistock:bug", "bug", severity.upper(), f"severity:{severity.lower()}", module_label, "status:open"]
    if ui_hints and module_label == "module:paper_v2":
        labels.append("paper-v2")
    return flow._unique_strings(labels)


def _workflow_efficiency_recommendations(record: dict[str, Any], ui_hints: dict[str, Any] | None = None) -> dict[str, Any]:
    module = _normalize_module_label(record.get("module"))
    required = record.get("required_verification") or []
    cleanup_fast_candidate = _is_cleanup_fast_candidate(record)
    recs = [
        "Use compact success output; request full JSON only for failures or diagnostics.",
        "Run targeted validation first, then final gates once the patch is stable.",
    ]
    batch_candidate = module in {"validation", "validation.guardrails", "validation_center"} or str(record.get("risk_area") or "") in {"ci_failure_intake", "workflow"}
    if batch_candidate:
        recs.append("Batch compatible workflow/CI/docs changes into one PR with per-issue evidence.")
    if ui_hints:
        recs.append("Use inferred UI route/scope to avoid broad repo scans; validate with frontend tsc and focused E2E when available.")
    if cleanup_fast_candidate:
        recs.append(
            "Use cleanup-fast for docs/scratch relocation: keep changes mechanical and run git diff --check unless executable code is intentionally retained."
        )
    if any(str(item).startswith("validation_center_backend") for item in required):
        recs.append("Keep validation_center_backend only when the changed files actually affect Validation Center.")
    recs.append("Default BUG validation to the smallest safe pre-merge gate; defer broad UI/API/business-flow suites to nightly.")
    return {
        "schema_version": "aistock_workflow_efficiency_recommendations_v1",
        "batch_candidate": batch_candidate,
        "cleanup_fast_candidate": cleanup_fast_candidate,
        "docs_only_merge_with_related_code": True,
        "compact_success_output": True,
        "full_json_on_failure_only": True,
        "verification_budget": _verification_budget_for_record(record, ui_hints),
        "recommendations": recs,
    }

def _render_github_issue_body(record: dict[str, Any], candidate: dict[str, Any]) -> str:
    evidence = record.get("evidence_uris") or []
    scope = record.get("allowed_write_scope") or []
    verification = record.get("required_verification") or []
    ui_hints = record.get("ui_intake_hints") if isinstance(record.get("ui_intake_hints"), dict) else None
    lines = [
        f"<!-- aistock-bug:{record.get('bug_id')} -->",
        f"<!-- aistock-candidate:{candidate.get('candidate_id')} -->",
        "",
        "## Summary",
        str(record.get("description") or record.get("title") or ""),
        "",
        "## Expected",
        str(record.get("expected") or "Expected behavior should be restored."),
        "",
        "## Actual",
        str(record.get("actual") or record.get("description") or ""),
        "",
        "## Reproduce",
        f"`{record.get('reproduce_command') or 'n/a'}`",
        "",
    ]
    if ui_hints:
        lines.extend([
            "## UI Intake Hints",
            f"- route: `{ui_hints.get('ui_route') or 'unknown'}`",
            f"- reproduce_required: `{ui_hints.get('reproduce_required')}`",
            "- visual_acceptance_required: `true`",
            "- inferred_scope:",
            *[f"  - `{item}`" for item in ui_hints.get('ui_component_scope') or []],
            "- recommended_verification:",
            *[f"  - `{item}`" for item in ui_hints.get('recommended_verification') or []],
            "",
        ])
    lines.extend([
        "## Scope",
        *[f"- `{item}`" for item in scope or ["triage required"]],
        "",
        "## Required Verification",
        *[f"- `{item}`" for item in verification or ["l0"]],
        "",
        "## Evidence",
        *[f"- {item}" for item in evidence or ["n/a"]],
        "",
    ])
    for section in record.get("github_issue_extra_sections") or []:
        text = str(section or "").strip()
        if text:
            lines.extend([text, ""])
    lines.extend([
        "## Next Step",
        "",
        f"`python scripts/aistock_issue_workflow.py run --bug-id {record.get('bug_id')} --mode plan --create-worktree`",
        "",
        "## Workflow Gates",
        "- production_ddl_gate: `noop`",
        "- production_frontend_dependency_gate: `noop`",
        "- production_backend_dependency_gate: `noop`",
    ])
    return "\n".join(lines)


def build_submit_bug_plan(
    *,
    title: str,
    module: str,
    severity: str,
    description: str | None,
    expected: str | None,
    actual: str | None,
    reproduce_command: str | None,
    evidence_refs: list[str],
    changed_files: list[str],
    plan_key: str | None,
    nox_session: str | None,
    candidate_type: str,
    bug_id: str | None,
    github_issue_number: str | None,
    github_issue_url: str | None,
    create_github: bool,
    apply: bool,
    allow_current_worktree: bool = False,
    create_registry_worktree: bool = False,
    create_fix_worktree: bool = False,
    registry_pr_only: bool = False,
    dry_run: bool = False,
    github_issue_extra_sections: list[str] | None = None,
    extra_github_labels: list[str] | None = None,
    added_files: list[str] | None = None,
) -> dict[str, Any]:
    file_input_preflight = _validate_submit_bug_file_inputs(
        changed_files=changed_files,
        added_files=list(added_files or []),
        module=module,
        root=_submit_bug_file_root(),
    )
    normalized_changed_files = file_input_preflight["changed_files"]
    normalized_added_files = file_input_preflight["added_files"]
    scope_files = file_input_preflight["scope_files"]
    scope_file_contract = {
        "schema_version": "aistock_submit_bug_file_scope_v1",
        "changed_files": normalized_changed_files,
        "planned_files": scope_files,
        "changed_files_source": FILE_SCOPE_SOURCE_PLANNED_INTAKE,
        "added_files": normalized_added_files,
        "scope_files": scope_files,
        "ownership": file_input_preflight["ownership"],
    }
    effective_apply = apply and not dry_run
    if create_fix_worktree and create_registry_worktree:
        raise WorkflowError("--create-fix-worktree and --create-registry-worktree are mutually exclusive")
    if create_fix_worktree and registry_pr_only:
        raise WorkflowError("--create-fix-worktree cannot be combined with --registry-pr-only")
    if effective_apply and not create_github and not (github_issue_number and github_issue_url):
        raise WorkflowError("--apply requires --create-github or existing --github-issue-number and --github-issue-url")
    registry_worktree_plan = _maybe_create_registry_worktree(
        title=title,
        module=module,
        severity=severity,
        create=create_registry_worktree,
        dry_run=dry_run or not apply,
    )
    registry_root = (
        Path(registry_worktree_plan["worktree"])
        if create_registry_worktree
        else _registry_target_root()
    )
    github_create_root = _canonical_root() if create_fix_worktree else registry_root
    allocation_root = registry_root if registry_root.exists() else _registry_target_root()
    registry_guard = None if (effective_apply and create_fix_worktree) else (
        _validate_registry_apply_target(registry_root) if effective_apply else None
    )
    if registry_guard and registry_guard["blocking"] and not allow_current_worktree:
        raise WorkflowError("; ".join(registry_guard["blocking"]))
    reservation_path: Path | None = None
    include_github_scan = create_github or bool(github_issue_number and github_issue_url)
    canonical_bug_id: str
    allocated_number: int
    allocation_report: dict[str, Any]
    event_args = argparse.Namespace(
        source="manual",
        source_json=None,
        title=title,
        module=module,
        severity_guess=severity,
        actual=actual or description or title,
        plan_key=plan_key,
        nox_session=nox_session,
        reproduce_command=reproduce_command,
        evidence_ref=evidence_refs,
        changed_file=scope_files,
    )
    event = flow.build_failure_event(event_args)
    candidate = flow.candidate_from_event(
        event,
        title=title,
        candidate_type=candidate_type,
        expected=expected,
        actual=actual or description,
    )
    remote_issue_confirmed = False
    try:
        if effective_apply:
            canonical_bug_id, allocated_number, allocation_report, reservation_path = _reserve_bug_id(
                allocation_root,
                bug_id=bug_id,
                include_github=include_github_scan,
                github_required=create_github,
                allowed_github_issue_number=github_issue_number,
                reservation_title=title,
                reservation_fingerprint=str(candidate.get("fingerprint") or ""),
            )
        else:
            allocation_report = _bug_id_allocation_report(
                allocation_root,
                include_github=include_github_scan,
                github_required=False,
            )
            canonical_bug_id = (bug_id or "").strip().upper() or f"BUG-{int(allocation_report['next_number']):03d}"
            number = _bug_id_number(canonical_bug_id)
            if not number or not re.fullmatch(r"BUG-\d{3,}", canonical_bug_id):
                raise WorkflowError("--bug-id must match BUG-NNN when provided")
            duplicates = _duplicate_bug_id_sources(
                allocation_report,
                canonical_bug_id,
                allowed_github_issue_number=github_issue_number,
            )
            if duplicates:
                detail = "; ".join(f"{item.get('kind')}:{item.get('source')}" for item in duplicates[:5])
                raise WorkflowError(f"{canonical_bug_id} already exists in global BUG id scan: {detail}")
            allocated_number = number

        recovered_existing_issue = allocation_report.get("recovered_existing_github_issue")
        effective_github_issue_number = github_issue_number
        effective_github_issue_url = github_issue_url
        if isinstance(recovered_existing_issue, dict):
            effective_github_issue_number = recovered_existing_issue.get("github_issue_number")
            effective_github_issue_url = str(
                recovered_existing_issue.get("source") or _github_issue_url(effective_github_issue_number)
            )
            remote_issue_confirmed = True
        record = flow.promote_candidate_to_bug(
            candidate,
            bug_id=canonical_bug_id,
            github_issue_number=effective_github_issue_number,
            github_issue_url=effective_github_issue_url,
        )
        now = _utc_now()
        record.setdefault("created_at", now)
        record.setdefault("first_seen_at", now)
        record.setdefault("last_seen_at", now)
        record.setdefault("assigned_agent", None)
        record.setdefault("fix_branch", None)
        record.setdefault("fix_commit", None)
        record.setdefault("verification_run_id", None)
        record.setdefault("github_issue_number", None)
        record.setdefault("github_issue_url", None)
        record.setdefault("fixed_at", None)
        record.setdefault("closed_at", None)
        record.setdefault("production_ddl_gate", "noop")
        record.setdefault("production_frontend_dependency_gate", "noop")
        record.setdefault("production_backend_dependency_gate", "noop")
        record["file_scope_contract"] = scope_file_contract
        runtime_inference = _classify_runtime_impact(scope_files)
        record["runtime_contract"] = {
            "schema_version": RUNTIME_CONTRACT_SCHEMA,
            "inference_basis": RUNTIME_INFERENCE_PLANNED_SCOPE,
            "provisional": True,
            "runtime_impact": runtime_inference["runtime_impact"],
            "backend_restart_owner": "user",
            "target_id": runtime_inference["target_ids"][0] if len(runtime_inference["target_ids"]) == 1 else None,
            "target_ids": runtime_inference["target_ids"],
            "persistence_basis": (
                "git_tracked_source"
                if runtime_inference["runtime_impact"] in {"backend", "worker_scheduler"}
                else "not_required"
            ),
            "fresh_process_evidence": [],
            "post_restart_effective_gate": (
                "pending_user_restart"
                if runtime_inference["runtime_impact"] in {"backend", "worker_scheduler"}
                else "not_required"
            ),
        }
        if github_issue_extra_sections:
            record["github_issue_extra_sections"] = [str(section) for section in github_issue_extra_sections if str(section or "").strip()]
        ui_hints = _ui_intake_hints(
            title=title,
            module=module,
            description=description,
            changed_files=scope_files,
            reproduce_command=reproduce_command,
        )
        if ui_hints:
            record["ui_intake_hints"] = ui_hints
            _add_record_allowed_scope(record, *(ui_hints.get("ui_component_scope") or []))
            record["required_verification"] = flow._unique_strings(
                flow._as_list(record.get("required_verification")) + list(ui_hints.get("recommended_verification") or [])
            )
        record["workflow_efficiency_recommendations"] = _workflow_efficiency_recommendations(record, ui_hints)
        record["verification_budget"] = record["workflow_efficiency_recommendations"]["verification_budget"]

        output_dir = registry_root / WORKFLOW_ROOT / canonical_bug_id
        candidate_path = output_dir / "candidate.json"
        github_body_path = output_dir / "github-issue-body.md"
        bug_path = _bug_json_path(record, registry_root)
        _add_record_allowed_scope(record, _repo_rel(bug_path, registry_root))
        if not create_fix_worktree:
            _add_record_allowed_scope(record, _repo_rel(_allocator_path(registry_root), registry_root))
        record["repository_allocator_persistence"] = (
            "omitted_from_fix_pr_global_reservation_is_authoritative"
            if create_fix_worktree
            else "registry_intake_updates_legacy_observation"
        )
        github_result: dict[str, Any] | None = (
            {
                "created": False,
                "recovered_existing": True,
                "url": effective_github_issue_url,
                "number": effective_github_issue_number,
            }
            if isinstance(recovered_existing_issue, dict)
            else None
        )
        github_labels = flow._unique_strings(
            _issue_labels_for_bug(module=module, severity=severity, ui_hints=ui_hints)
            + list(extra_github_labels or [])
        )

        if effective_apply and bug_path.exists():
            raise WorkflowError(f"BUG JSON already exists: {bug_path}")

        if create_github and not record.get("github_issue_url") and effective_apply:
            github_body_for_create = github_body_path
            if create_fix_worktree:
                github_body_for_create = Path(tempfile.gettempdir()) / "aistock_issue_workflow" / f"{canonical_bug_id}-github-issue-body.md"
            _write_text(github_body_for_create, _render_github_issue_body(record, candidate))
            github_title = f"{canonical_bug_id} {severity}: {title}"
            github_result = _create_github_issue_with_recovery(
                bug_id=canonical_bug_id,
                title=github_title,
                body_path=github_body_for_create,
                labels=github_labels,
                cwd=github_create_root,
            )
            issue_url = str(github_result.get("url") or "")
            issue_number = github_result.get("number")
            record["github_issue_url"] = issue_url
            record["github_issue_number"] = issue_number
            remote_issue_confirmed = True
            _update_bug_id_reservation(
                reservation_path,
                status="github_issue_confirmed",
                github_issue_number=issue_number,
                github_issue_url=issue_url,
                github_issue_title=github_title,
                recovered_after_transport_error=github_result.get("recovered_after_transport_error"),
            )
        elif create_github and not record.get("github_issue_url"):
            github_result = {"created": False, "planned": True, "body_path": _repo_rel(github_body_path, registry_root)}

        if effective_apply and record.get("github_issue_number") and record.get("github_issue_url"):
            remote_issue_confirmed = True
            _update_bug_id_reservation(
                reservation_path,
                status="github_issue_confirmed",
                github_issue_number=record.get("github_issue_number"),
                github_issue_url=record.get("github_issue_url"),
                title=title,
                fingerprint=str(candidate.get("fingerprint") or ""),
            )

        has_linkage = bool(record.get("github_issue_number") and record.get("github_issue_url"))
        if effective_apply and not has_linkage:
            raise WorkflowError("--apply requires --create-github or existing --github-issue-number and --github-issue-url")
        fix_worktree_plan = _maybe_create_fix_chain_worktree(
            record=record,
            bug_id=canonical_bug_id,
            create=create_fix_worktree,
            dry_run=dry_run or not apply,
        )
        fix_chain_root = Path(fix_worktree_plan["worktree"]) if create_fix_worktree else None
        write_root = fix_chain_root if fix_chain_root is not None and effective_apply else registry_root
        write_output_dir = write_root / WORKFLOW_ROOT / canonical_bug_id
        write_candidate_path = write_output_dir / "candidate.json"
        write_github_body_path = write_output_dir / "github-issue-body.md"
        write_bug_path = _issue_json_path_for_worktree(bug_path, write_root) if write_root != registry_root else bug_path
        fix_worktree_guard = _validate_registry_apply_target(write_root) if (effective_apply and create_fix_worktree) else None
        if fix_worktree_guard and fix_worktree_guard["blocking"] and not allow_current_worktree:
            raise WorkflowError("; ".join(fix_worktree_guard["blocking"]))
        if write_root != registry_root:
            _add_record_allowed_scope(record, _repo_rel(write_bug_path, write_root))

        payload = {
        "schema_version": "aistock_issue_workflow_submit_bug_v1",
        "generated_at": now,
        "bug_id": canonical_bug_id,
        "candidate_id": candidate.get("candidate_id"),
        "dry_run": not effective_apply,
        "workflow_gate": "submitted" if effective_apply else ("ready_for_apply" if has_linkage or create_github else "needs_github_sync"),
        "registry_root": str(registry_root),
        "registry_guard": registry_guard or (
            {
                "target_root": str(registry_root),
                "canonical_root": str(_canonical_root().resolve()),
                "blocking": [],
                "warnings": [
                    "BUG registry will be persisted in the fix worktree/branch"
                    if create_fix_worktree
                    else "registry worktree will be created on apply"
                ],
                "planned": True,
            }
            if (create_fix_worktree or (create_registry_worktree and not registry_root.exists()))
            else _validate_registry_apply_target(registry_root)
        ),
        "registry_worktree_plan": registry_worktree_plan,
        "fix_worktree_plan": fix_worktree_plan,
        "fix_worktree_guard": fix_worktree_guard,
        "registration_strategy": "fix_pr_persists_bug_registration" if create_fix_worktree else "registry_intake_then_fix_worktree",
        "candidate_path": _repo_rel(candidate_path, registry_root),
        "github_issue_body_path": _repo_rel(github_body_path, registry_root),
        "bug_json_path": _repo_rel(bug_path, registry_root),
        "github": github_result or {
            "created": False,
            "number": record.get("github_issue_number"),
            "url": record.get("github_issue_url"),
        },
        "record": record,
        "file_input_preflight": file_input_preflight,
        "file_scope_contract": scope_file_contract,
        "ui_intake_hints": ui_hints,
        "workflow_efficiency_recommendations": record.get("workflow_efficiency_recommendations"),
        "github_issue_labels": github_labels,
        "registry_pr_only": registry_pr_only,
        "stale_pr_check": (
            _stale_pr_check_for_bug(canonical_bug_id)
            if effective_apply and bug_id
            else {
                "status": "skipped_fresh_allocation" if effective_apply else "not_applicable_before_apply",
                "reason": "a newly allocated BUG id cannot have a stale PR",
            }
        ),
        "bug_id_allocation": {
            "allocator_root": str(allocation_root),
            "global_max_number": allocation_report.get("max_number"),
            "github_scanned": allocation_report.get("github_scanned"),
            "github_lookup_mode": allocation_report.get("github_lookup_mode"),
            "warnings": allocation_report.get("warnings", []),
            "reservation_path": str(reservation_path) if reservation_path else None,
        },
        "next_agent_steps": [
            "switch_to_fix_worktree_and_continue_fix" if create_fix_worktree else "switch_to_registry_worktree",
            "commit_registry_only_pr_without_fix" if registry_pr_only else (
                "continue_fix_in_same_branch" if create_fix_worktree else "commit_registry_files_then_continue_fix_worktree"
            ),
            "do_not_write_bug_json_in_canonical_root",
        ] if effective_apply else [
            "create_or_switch_to_clean_registry_worktree",
            "rerun_submit_bug_with_github_linkage",
        ],
        "next_command": (
            f"cd /d {fix_chain_root} && python scripts/aistock_issue_workflow.py run --bug-id {canonical_bug_id} "
            f"--issue-json \"{_repo_rel(write_bug_path, write_root)}\" --mode plan"
            if effective_apply and create_fix_worktree and fix_chain_root
            else _registry_commit_next_command(
                registry_root,
                canonical_bug_id,
                continue_fix=not registry_pr_only,
                issue_json=_repo_rel(bug_path, registry_root),
            )
        )
        if effective_apply
        else (
            f"python scripts/aistock_issue_workflow.py submit-bug --title \"{title}\" --module {module} "
            f"--severity {severity} --create-github --create-registry-worktree --apply"
        ),
        }

        if effective_apply:
            _write_json(write_candidate_path, {"event": event, "candidate": candidate})
            _write_text(write_github_body_path, _render_github_issue_body(record, candidate))
            _write_json(write_bug_path, record)
            if not create_fix_worktree:
                _write_allocator(max(allocated_number, int(allocation_report.get("max_number") or 0)), write_root)
            fix_registration_commit = (
                _commit_bug_registration_in_fix_worktree(write_root, canonical_bug_id)
                if create_fix_worktree and write_root != registry_root
                else None
            )
            _write_state(
                canonical_bug_id,
                state="discovered",
                root=write_root,
                branch=_branch_for_path(write_root),
                workflow_role="fix" if create_fix_worktree else "registry_intake",
                registry_pr_only=registry_pr_only,
                source_bug_json=_repo_rel(write_bug_path, write_root),
                target_bug_json=_repo_rel(write_bug_path, write_root),
                candidate_path=_repo_rel(write_candidate_path, write_root),
                github_issue_number=record.get("github_issue_number"),
                github_issue_url=record.get("github_issue_url"),
                worktree=str(write_root) if create_fix_worktree else None,
                fix_chain_registration_commit=fix_registration_commit,
                next_actions=(
                    ["run_issue_workflow_plan_in_current_fix_worktree", "read_context_pack", "fix", "validate", "create_pr"]
                    if create_fix_worktree
                    else ["run_issue_workflow_plan", "create_worktree", "read_context_pack"]
                ),
            )
            _update_bug_id_reservation(
                reservation_path,
                status="registered",
                bug_json=_repo_rel(write_bug_path, write_root),
                registration_root=str(write_root.resolve()),
            )
            payload["state_path"] = _repo_rel(_state_path(canonical_bug_id, write_root), write_root)
            payload["events_path"] = _repo_rel(_events_path(canonical_bug_id, write_root), write_root)
            payload["candidate_path"] = _repo_rel(write_candidate_path, write_root)
            payload["github_issue_body_path"] = _repo_rel(write_github_body_path, write_root)
            payload["bug_json_path"] = _repo_rel(write_bug_path, write_root)
            if fix_registration_commit:
                payload["fix_registration_commit"] = fix_registration_commit
            payload["fix_chain"] = {
                "registry_pr_required": registry_pr_only,
                "continue_to_fix_in_same_workflow": not registry_pr_only,
                "run_next_command": payload["next_command"],
                "next_command": (
                    None
                    if registry_pr_only
                    else (
                        f"cd /d {fix_chain_root} && python scripts/aistock_issue_workflow.py run --bug-id {canonical_bug_id} "
                        f"--issue-json \"{_repo_rel(write_bug_path, write_root)}\" --mode plan"
                        if create_fix_worktree and fix_chain_root
                        else (
                            f"python scripts/aistock_issue_workflow.py run --bug-id {canonical_bug_id} "
                            f"--issue-json \"{_repo_rel(bug_path, registry_root)}\" --mode plan --create-worktree"
                        )
                    )
                ),
                "default_path": (
                    "registry_pr_only"
                    if registry_pr_only
                    else ("single_fix_branch_registration_and_fix" if create_fix_worktree else "commit_registry_then_create_fix_worktree")
                ),
                "stop_reason": "registry_pr_only_requested" if registry_pr_only else None,
                "note": (
                    "User explicitly requested registry-only tracking; stop after the registry PR."
                    if registry_pr_only
                    else (
                        "BUG registration has been committed in the fix branch; continue coding and include that BUG JSON in the fix PR."
                        if create_fix_worktree
                        else "BUG registration seeds a registry-intake state. Commit the BUG JSON, then create a separate fix worktree from latest origin/main using the registry BUG JSON until it lands on main."
                    )
                ),
            }
        return payload
    except Exception as exc:
        if remote_issue_confirmed:
            _update_bug_id_reservation(
                reservation_path,
                status="github_issue_confirmed_local_incomplete",
                last_error_type=type(exc).__name__,
                last_error=str(exc)[:1000],
            )
        elif isinstance(exc, GitHubOutcomeUnknownError) and reservation_path is not None:
            _update_bug_id_reservation(
                reservation_path,
                status="github_create_outcome_unknown",
                last_error_type=type(exc).__name__,
                last_error=str(exc)[:1000],
            )
        else:
            _release_bug_id_reservation(reservation_path)
        raise


def _require_github_linkage(record: dict[str, Any], *, allow_missing: bool = False) -> list[str]:
    missing = [key for key in ("github_issue_number", "github_issue_url") if not record.get(key)]
    if missing and not allow_missing:
        bug_id = record.get("bug_id") or "<unknown>"
        raise WorkflowError(f"{bug_id} missing GitHub linkage: {', '.join(missing)}")
    return missing


def _require_fixable_status(record: dict[str, Any], *, allow_closed: bool = False) -> str:
    status = str(record.get("status") or "").strip()
    if not allow_closed and status not in ALLOWED_FIX_STATUSES:
        bug_id = record.get("bug_id") or "<unknown>"
        raise WorkflowError(f"{bug_id} status is {status!r}; only {sorted(ALLOWED_FIX_STATUSES)} are fixable by default")
    return status


def _target_names(record: dict[str, Any], bug_id: str, task_slug: str | None = None) -> tuple[str, Path]:
    title_slug = _slug(task_slug or str(record.get("title") or bug_id), max_len=48)
    today = _today_compact()
    branch = f"bug/{bug_id}-{title_slug}-{today}"
    worktree = _default_worktree_root() / f"{bug_id}-{title_slug}-{today}"
    return branch, worktree


def _batch_target_names(batch_id: str, module: str, task_slug: str | None = None) -> tuple[str, Path]:
    batch_slug = _slug(task_slug or f"{batch_id}-{module}", max_len=56)
    today = _today_compact()
    branch = f"bug/{batch_slug}-{today}"
    worktree = _default_worktree_root() / f"{batch_slug}-{today}"
    return branch, worktree


def _maybe_create_worktree(
    *,
    record: dict[str, Any],
    bug_id: str,
    source_bug_json: Path,
    create: bool,
    dry_run: bool,
    task_slug: str | None,
) -> dict[str, Any]:
    branch, worktree = _target_names(record, bug_id, task_slug)
    plan = {
        "create_worktree": create,
        "dry_run": dry_run,
        "branch": branch,
        "worktree": str(worktree),
        "base": "origin/main",
    }
    if not create or dry_run:
        return plan
    if worktree.exists():
        raise WorkflowError(f"target worktree already exists: {worktree}")
    _git(["fetch", "origin", "main"])
    _git_worktree_add_new_branch(worktree=worktree, branch=branch)
    copied_bug_json = _ensure_issue_json_in_worktree(source_bug_json, worktree, record=record)
    plan["seeded_issue_json"] = _repo_rel(copied_bug_json, worktree)
    plan["created"] = True
    return plan


def _actual_and_planned_worktree(worktree_plan: dict[str, Any]) -> tuple[str | None, str | None]:
    worktree = str(worktree_plan.get("worktree") or "").strip() or None
    if worktree_plan.get("created"):
        return worktree, None
    return None, worktree


def _maybe_create_named_worktree(
    *,
    branch: str,
    worktree: Path,
    create: bool,
    dry_run: bool,
) -> dict[str, Any]:
    plan = {
        "create_worktree": create,
        "dry_run": dry_run,
        "branch": branch,
        "worktree": str(worktree),
        "base": "origin/main",
    }
    if not create or dry_run:
        return plan
    if worktree.exists():
        raise WorkflowError(f"target worktree already exists: {worktree}")
    _git(["fetch", "origin", "main"])
    _git_worktree_add_new_branch(worktree=worktree, branch=branch)
    plan["created"] = True
    return plan


def _bug_path_for_target(original_path: Path, target_root: Path) -> Path:
    try:
        relative = original_path.resolve().relative_to(REPO_ROOT.resolve())
        return target_root / relative
    except Exception:
        return original_path


def _issue_query(record: dict[str, Any], changed_files: list[str] | None = None) -> str:
    parts = [
        str(record.get("bug_id") or record.get("candidate_id") or "AIstock issue"),
        str(record.get("title") or ""),
        str(record.get("module") or record.get("module_guess") or ""),
        str(record.get("description") or record.get("actual") or ""),
    ]
    if changed_files:
        parts.extend(changed_files[:8])
    return " ".join(part for part in parts if part).strip() or "AIstock issue"


def _code_intelligence_scope(record: dict[str, Any], changed_files: list[str] | None = None) -> list[str]:
    """Use explicit changed files, then allowed scope, so pre-fix task cards stay scoped."""
    return flow._unique_strings(
        [
            str(path).replace("\\", "/").lstrip("./")
            for path in (changed_files or flow._as_list(record.get("allowed_write_scope")) or flow._as_list(record.get("suggested_scope")))
            if str(path).strip()
        ]
    )


def _build_code_intelligence_summary(
    *,
    item_id: str,
    record: dict[str, Any],
    changed_files: list[str] | None,
    root: Path,
) -> dict[str, Any]:
    scoped_files = _code_intelligence_scope(record, changed_files)
    return code_intelligence.build_summary(
        item_id=item_id,
        query=_issue_query(record, scoped_files),
        changed_files=scoped_files,
        module=str(record.get("module") or "").strip() or None,
        root=root,
        skip_external=False,
    )


def _compact_code_intelligence_for_task_card(code_intelligence_summary: dict[str, Any]) -> dict[str, Any]:
    ua = code_intelligence_summary.get("understand_anything")
    ua_summary = code_intelligence_summary.get("understand_anything_summary")
    context_ref = code_intelligence_summary.get("context_ref")
    affected_tests_ref = code_intelligence_summary.get("affected_tests_ref")
    ua_summary_ref = code_intelligence_summary.get("understand_anything_summary_ref")
    graph_first_required_refs = [context_ref, affected_tests_ref, ua_summary_ref]
    return {
        "provider": code_intelligence_summary.get("provider"),
        "status": code_intelligence_summary.get("status"),
        "context_ref": context_ref,
        "manifest_ref": code_intelligence_summary.get("manifest_ref"),
        "affected_tests_ref": affected_tests_ref,
        "affected_tests_count": code_intelligence_summary.get("affected_tests_count"),
        "affected_quality": code_intelligence_summary.get("affected_quality"),
        "latest_freshness": code_intelligence_summary.get("latest_freshness"),
        "latest_freshness_ref": code_intelligence_summary.get("latest_freshness_ref"),
        "consume_command": code_intelligence_summary.get("consume_command"),
        "verify_command": code_intelligence_summary.get("verify_command"),
        "stale_metadata_warning": bool(code_intelligence_summary.get("stale_metadata_warning")),
        "context_quality": (
            ((code_intelligence_summary.get("context") or {}).get("context_quality") or {}).get("quality")
            if isinstance(code_intelligence_summary.get("context"), dict)
            else None
        ),
        "noisy_context_warning": bool(
            ((code_intelligence_summary.get("context") or {}).get("context_quality") or {}).get("noisy_context_warning")
        )
        if isinstance(code_intelligence_summary.get("context"), dict)
        else False,
        "fallback_used": bool(code_intelligence_summary.get("fallback_used")),
        "fallback_reason": code_intelligence_summary.get("fallback_reason"),
        "understand_anything_status": (ua or {}).get("status") if isinstance(ua, dict) else None,
        "understand_anything_summary_ref": ua_summary_ref,
        "understand_anything_nodes_used": (ua_summary or {}).get("nodes_used") if isinstance(ua_summary, dict) else None,
        "understand_anything_graph_exists": (ua_summary or {}).get("graph_exists") if isinstance(ua_summary, dict) else None,
        "understand_anything_freshness": (ua_summary or {}).get("freshness") if isinstance(ua_summary, dict) else None,
        "understand_anything_graph_commit": (ua_summary or {}).get("graph_commit") if isinstance(ua_summary, dict) else None,
        "understand_anything_current_commit": (ua_summary or {}).get("current_git_commit") if isinstance(ua_summary, dict) else None,
        "understand_anything_generate_graph_command": (ua or {}).get("generate_graph_command") if isinstance(ua, dict) else None,
        "graph_first_required": True,
        "graph_first_refs_ready": all(bool(item) for item in graph_first_required_refs),
        "broad_scan_requires_scoped_miss_reason": True,
        "blocking_for_issue_workflow": False,
    }


def build_task_card(
    *,
    bug_id: str,
    record: dict[str, Any],
    root: Path,
    branch: str | None,
    planned_branch: str | None,
    worktree: str | None,
    planned_worktree: str | None,
    context_pack_json_path: Path,
    context_pack_md_path: Path,
    fix_ready_path: Path,
    state_path: Path,
    events_path: Path,
    fix_ready: dict[str, Any],
    context_pack: dict[str, Any],
    code_intelligence_summary: dict[str, Any],
    fast_path: dict[str, Any],
) -> dict[str, Any]:
    validation = fix_ready.get("validation_selection") if isinstance(fix_ready.get("validation_selection"), dict) else {}
    code_intel = _compact_code_intelligence_for_task_card(code_intelligence_summary)
    verification_budget = record.get("verification_budget") if isinstance(record.get("verification_budget"), dict) else _verification_budget_for_record(record)
    runtime_contract = build_runtime_contract(
        record=record,
        changed_files=resolve_record_runtime_changed_files(record),
        root=root,
    )
    return {
        "schema_version": "aistock_agent_task_card_v1",
        "generated_at": _utc_now(),
        "task_card_id": f"TC-{bug_id}-{flow._stable_hash(record.get('github_issue_number'), record.get('title'), record.get('module'))}",
        "agent_neutral": True,
        "supported_clients": ["Codex", "Claude Code", "Cursor", "CLI"],
        "bug_id": bug_id,
        "github_issue": {
            "number": record.get("github_issue_number"),
            "url": record.get("github_issue_url"),
        },
        "module": record.get("module"),
        "severity": record.get("severity"),
        "risk_level": fix_ready.get("risk_level"),
        "branch": branch,
        "planned_branch": planned_branch,
        "worktree": worktree,
        "planned_worktree": planned_worktree,
        "artifact_refs": {
            "context_pack_json": _repo_rel(context_pack_json_path, root),
            "context_pack_md": _repo_rel(context_pack_md_path, root),
            "fix_ready_json": _repo_rel(fix_ready_path, root),
            "state_json": _repo_rel(state_path, root),
            "events_jsonl": _repo_rel(events_path, root),
        },
        "machine_json_policy": {
            "default_context": ["task-card.md", "compact stdout", "context-pack.md only when needed"],
            "debug_only": ["state.json", "events.jsonl", "finish-plan.json", "fix-ready.json"],
            "rule": "Do not read machine JSON artifacts during ordinary fixes unless a command failed or state recovery is required.",
        },
        "problem": {
            "title": record.get("title"),
            "statement": context_pack.get("problem_statement"),
            "reproduce_command": context_pack.get("reproduce_command"),
        },
        "allowed_write_scope": fix_ready.get("allowed_write_scope") or [],
        "required_verification": fix_ready.get("required_verification") or validation.get("required_plans") or [],
        "recommended_verification": fix_ready.get("recommended_verification") or validation.get("recommended_plans") or [],
        "production_gates": validation.get("production_gates") or _production_gates_payload(),
        "runtime_contract": _pick(
            runtime_contract,
            "schema_version",
            "runtime_impact",
            "backend_restart_required",
            "backend_restart_owner",
            "target_id",
            "catalog_ref",
            "operator_runbook_ref",
            "post_restart_effective_gate",
            "runtime_identity_match",
            "activation_states",
            "blocking",
        ),
        "verification_budget": verification_budget,
        "workflow_efficiency_recommendations": record.get("workflow_efficiency_recommendations"),
        "context_resume_digest": _workflow_context_resume_digest(
            {
                "allowed_write_scope": fix_ready.get("allowed_write_scope") or [],
                "required_verification": fix_ready.get("required_verification") or validation.get("required_plans") or [],
                "verification_budget": verification_budget,
            },
            root=root,
        ),
        "code_intelligence": code_intel,
        "fast_path": _pick(fast_path or {}, "task_tier", "module", "workflow_gate", "required_validation", "recommended_validation"),
        "stop_conditions": [
            "missing GitHub linkage",
            "scope expansion required outside allowed_write_scope",
            "required validation cannot run",
            "local validation/exploration exceeds budget and should switch to validation delegation",
            "production runtime or DB action requested without explicit approval",
        ],
        "next_client_steps": [
            "switch_to_worktree_if_created",
            "read task-card.md first, then context-pack.md only when needed",
            "use exactly one task-specific skill/command; do not load other scenario skills or full standards unless task-card/user requires it",
            "after context compaction, use Context Resume Digest hashes instead of re-reading standards/quickstart/RTK",
            "read Code Intelligence refs before rg; record a scoped miss reason before broad scans",
            "stop and summarize before more search if exploration commands exceed the soft budget",
            "after a test failure, rerun the failed nodeid or pytest --lf before any broader suite",
            "run the final related small matrix at most once; delegate broad/deep validation to VC/CI/nightly",
            "prefer RTK for supported high-output interactive commands; record capability fallback and never make RTK a gate",
            "never start, stop, or restart a user backend without explicit authorization for the current target",
            "for backend/worker/scheduler fixes, attach persistent-source and fresh-process evidence before PR readiness",
            "edit only files under allowed_write_scope or stop for scope expansion",
            "run finish --plan-only before reporting the issue fixed",
        ],
        "token_budget": {
            "context_pack_target_tokens": (context_pack.get("token_budget") or {}).get("target_tokens"),
            "task_card_target_tokens": 2000,
            "large_graph_payload_inlined": False,
        },
        "blocking_for_issue_workflow": False,
    }


def render_task_card_markdown(task_card: dict[str, Any]) -> str:
    artifacts = task_card.get("artifact_refs") if isinstance(task_card.get("artifact_refs"), dict) else {}
    github_issue = task_card.get("github_issue") if isinstance(task_card.get("github_issue"), dict) else {}
    code_intel = task_card.get("code_intelligence") if isinstance(task_card.get("code_intelligence"), dict) else {}
    resume_digest = task_card.get("context_resume_digest") if isinstance(task_card.get("context_resume_digest"), dict) else {}
    budget = task_card.get("verification_budget") if isinstance(task_card.get("verification_budget"), dict) else {}
    deferred = budget.get("deferred_nightly_verification") if isinstance(budget.get("deferred_nightly_verification"), dict) else {}
    delegated = budget.get("delegated_validation") if isinstance(budget.get("delegated_validation"), dict) else {}
    local_loop = budget.get("local_loop_policy") if isinstance(budget.get("local_loop_policy"), dict) else {}
    resume_validation = (
        resume_digest.get("validation_loop_budget") if isinstance(resume_digest.get("validation_loop_budget"), dict) else {}
    )
    runtime_contract = task_card.get("runtime_contract") if isinstance(task_card.get("runtime_contract"), dict) else {}
    lines = [
        f"# AIstock Agent Task Card {task_card.get('bug_id')}",
        "",
        f"- task_card_id: `{task_card.get('task_card_id')}`",
        f"- clients: `{', '.join(task_card.get('supported_clients') or [])}`",
        f"- module: `{task_card.get('module') or 'unknown'}`",
        f"- risk_level: `{task_card.get('risk_level') or 'unknown'}`",
        f"- github_issue: {github_issue.get('url') or 'missing'}",
        f"- branch: `{task_card.get('branch') or task_card.get('planned_branch') or 'not_created'}`",
        f"- worktree: `{task_card.get('worktree') or task_card.get('planned_worktree') or 'not_created'}`",
        "",
        "## Start Here",
        *[f"- {item}" for item in task_card.get("next_client_steps") or []],
        "",
        "## Problem",
        f"- title: {((task_card.get('problem') or {}).get('title') if isinstance(task_card.get('problem'), dict) else None) or 'n/a'}",
        f"- reproduce: `{((task_card.get('problem') or {}).get('reproduce_command') if isinstance(task_card.get('problem'), dict) else None) or 'n/a'}`",
        "",
        "## Allowed Write Scope",
        *[f"- `{item}`" for item in task_card.get("allowed_write_scope") or ["triage_only_until_scope_is_set"]],
        "",
        "## Required Verification",
        *[f"- `{item}`" for item in task_card.get("required_verification") or ["l0"]],
        "",
        "## Runtime Contract",
        f"- runtime_impact: `{runtime_contract.get('runtime_impact') or 'unknown'}`",
        f"- backend_restart_required: `{str(bool(runtime_contract.get('backend_restart_required'))).lower()}`",
        f"- backend_restart_owner: `{runtime_contract.get('backend_restart_owner') or 'user'}`",
        f"- target_id: `{runtime_contract.get('target_id') or 'none'}`",
        f"- catalog_ref: `{runtime_contract.get('catalog_ref') or 'none'}`",
        f"- operator_runbook_ref: `{runtime_contract.get('operator_runbook_ref') or 'none'}`",
        f"- post_restart_effective_gate: `{runtime_contract.get('post_restart_effective_gate') or 'unknown'}`",
        *[f"- blocking: {item}" for item in runtime_contract.get("blocking") or []],
        "",
        "## Verification Budget",
        f"- budget: `{budget.get('budget') or 'not_recorded'}`",
        f"- target_cost_percent_of_legacy: `{budget.get('target_cost_percent_of_legacy') or 'not_recorded'}`",
        f"- deferred_nightly_required: `{str(bool(deferred.get('required'))).lower()}`",
        f"- deferred_nightly_modules: `{', '.join(deferred.get('modules') or []) or 'none'}`",
        f"- deferred_nightly_plans: `{', '.join(deferred.get('plans') or []) or 'none'}`",
        f"- delegated_validation_skill: `{delegated.get('skill') or 'none'}`",
        f"- delegated_receipt_default: `{delegated.get('receipt_default') or 'none'}`",
        "",
        "## Local Validation Loop Policy",
        f"- failure_resume_first: `{', '.join(local_loop.get('failure_resume_first') or []) or 'pytest --lf -q'}`",
        f"- max_final_related_matrix_runs: `{local_loop.get('max_final_related_matrix_runs') or 1}`",
        f"- no_repeat_rule: `{local_loop.get('no_repeat_rule') or 'rerun failed nodeids before broader suites'}`",
        f"- delegate_when: `{', '.join(local_loop.get('delegate_when') or []) or 'broad validation needed'}`",
        "",
        "## Context Resume Digest",
        f"- rule_digest_count: `{len(resume_digest.get('rule_digests') or [])}`",
        f"- exploration_soft_limit: `{((resume_digest.get('exploration_command_budget') or {}).get('soft_limit')) or 40}`",
        f"- validation_resume_first: `{', '.join(resume_validation.get('failure_resume_first') or []) or 'pytest --lf -q'}`",
        f"- json_artifact_policy: `{((resume_digest.get('success_artifact_policy') or {}).get('json')) or 'diagnostic_only'}`",
        *[
            f"- rule: `{item.get('path')}` sha256_12=`{item.get('sha256_12') or 'missing'}`"
            for item in resume_digest.get("rule_digests") or []
        ],
        "",
        "## Artifacts",
        *[f"- {key}: `{value}`" for key, value in artifacts.items() if value],
        "- machine JSON policy: debug/resume only; do not read `state.json`, `events.jsonl`, `finish-plan.json`, or `fix-ready.json` during ordinary fixes.",
        "",
        "## Code Intelligence",
        f"- status: `{code_intel.get('status') or 'unknown'}`",
        f"- context_ref: `{code_intel.get('context_ref') or 'not_generated'}`",
        f"- affected_tests_ref: `{code_intel.get('affected_tests_ref') or 'not_generated'}`",
        f"- affected_tests_count: `{code_intel.get('affected_tests_count', 0)}`",
        f"- latest_freshness: `{code_intel.get('latest_freshness') or 'not_available'}`",
        f"- latest_freshness_ref: `{code_intel.get('latest_freshness_ref') or 'not_available'}`",
        f"- consume_command: `{code_intel.get('consume_command') or 'python scripts/code_intelligence_adapter.py latest-freshness --refresh-if-stale'}`",
        f"- verify_command: `{code_intel.get('verify_command') or 'python scripts/code_intelligence_adapter.py verify-clients'}`",
        f"- context_quality: `{code_intel.get('context_quality') or 'unknown'}`",
        f"- stale_metadata_warning: `{str(bool(code_intel.get('stale_metadata_warning'))).lower()}`",
        f"- noisy_context_warning: `{str(bool(code_intel.get('noisy_context_warning'))).lower()}`",
        f"- understand_anything_summary_ref: `{code_intel.get('understand_anything_summary_ref') or 'not_generated'}`",
        f"- understand_anything_status: `{code_intel.get('understand_anything_status') or 'unknown'}`",
        f"- understand_anything_freshness: `{code_intel.get('understand_anything_freshness') or 'unknown'}`",
        f"- understand_anything_graph_commit: `{code_intel.get('understand_anything_graph_commit') or 'unknown'}`",
        f"- understand_anything_current_commit: `{code_intel.get('understand_anything_current_commit') or 'unknown'}`",
        f"- understand_anything_graph_exists: `{str(bool(code_intel.get('understand_anything_graph_exists'))).lower()}`",
        f"- understand_anything_nodes_used: `{code_intel.get('understand_anything_nodes_used', 0)}`",
        f"- graph_first_required: `{str(bool(code_intel.get('graph_first_required'))).lower()}`",
        f"- graph_first_refs_ready: `{str(bool(code_intel.get('graph_first_refs_ready'))).lower()}`",
        f"- broad_scan_requires_scoped_miss_reason: `{str(bool(code_intel.get('broad_scan_requires_scoped_miss_reason'))).lower()}`",
        f"- fallback_used: `{str(bool(code_intel.get('fallback_used'))).lower()}`",
        f"- blocking_for_issue_workflow: `{str(bool(code_intel.get('blocking_for_issue_workflow'))).lower()}`",
        f"- ua_generate_graph_command: `{code_intel.get('understand_anything_generate_graph_command') or 'not_required'}`",
        "",
        "## Production Gates",
        *[f"- {key}: `{value}`" for key, value in sorted((task_card.get("production_gates") or {}).items())],
        "",
        "Large graph payloads are intentionally not inlined. Use artifact refs only when needed.",
    ]
    return "\n".join(lines)


def _batch_code_intelligence_query(records: list[dict[str, Any]], changed_files: list[str] | None = None) -> str:
    parts = ["AIstock batch issue"]
    parts.extend(
        " ".join(
            str(value)
            for value in (
                record.get("bug_id"),
                record.get("title"),
                record.get("module"),
                record.get("description") or record.get("actual"),
            )
            if value
        )
        for record in records
    )
    if changed_files:
        parts.extend(changed_files[:12])
    return " ".join(part for part in parts if part).strip() or "AIstock batch issue"


def _record_scope(record: dict[str, Any]) -> list[str]:
    return flow._unique_strings(
        flow._as_list(record.get("allowed_write_scope")) or flow._as_list(record.get("suggested_scope"))
    )


def _record_required_verification(record: dict[str, Any]) -> list[str]:
    return flow._unique_strings(
        flow._as_list(record.get("required_verification")) or flow._as_list(record.get("suggested_validation"))
    )


def _path_matches_scope(path: str, scope: list[str]) -> bool:
    normalized = path.replace("\\", "/").lstrip("./")
    for item in scope:
        allowed = str(item).replace("\\", "/").lstrip("./").rstrip("/")
        if normalized and (
            normalized == allowed
            or normalized.startswith(allowed + "/")
            or flow._pattern_matches(allowed, normalized)
            or flow._pattern_matches(allowed.rstrip("/") + "/**", normalized)
        ):
            return True
    return False


def _batch_scope_check(changed_files: list[str], allowed_scope: list[str]) -> dict[str, Any]:
    changed = _normalize_changed_files(changed_files)
    if not changed:
        return {
            "schema_version": "aistock_batch_scope_check_v1",
            "status": "skipped_no_changed_files",
            "allowed_scope": allowed_scope,
            "changed_files": [],
            "violations": [],
        }
    violations = [path for path in changed if not _path_matches_scope(path, allowed_scope)]
    return {
        "schema_version": "aistock_batch_scope_check_v1",
        "status": "passed" if not violations else "failed",
        "allowed_scope": allowed_scope,
        "changed_files": changed,
        "violations": violations,
    }


def _batch_validation_selector(
    records: list[dict[str, Any]],
    *,
    module: str,
    changed_files: list[str] | None = None,
) -> dict[str, Any]:
    per_issue: dict[str, dict[str, Any]] = {}
    blocking: list[str] = []
    warnings: list[str] = []
    shared_scope = flow._unique_strings(path for record in records for path in _record_scope(record))
    selector_files = _normalize_changed_files(changed_files) or shared_scope
    selected = flow.select_validation(selector_files, module=module)
    required_plans = flow._unique_strings(selected.get("required_plans") or [])
    required_set = set(required_plans)

    for record in records:
        bug_id = str(record.get("bug_id") or record.get("candidate_id") or record.get("title"))
        scope = _record_scope(record)
        required = _record_required_verification(record)
        missing_required = [plan for plan in required if plan and plan not in required_set]
        if not scope:
            blocking.append(f"{bug_id} has no allowed_write_scope/suggested_scope for safe batching")
        if missing_required:
            blocking.append(f"{bug_id} required verification not covered by shared selector: {missing_required}")
        per_issue[bug_id] = {
            "allowed_write_scope": scope,
            "required_verification": required,
            "missing_required_plans": missing_required,
            "covered_by_shared_validation": not missing_required,
        }

    gates = selected.get("production_gates") or {}
    special_gates = {key: value for key, value in gates.items() if value != "noop"}
    if special_gates:
        blocking.append(f"batch includes production/dependency gates and must be split or handled explicitly: {special_gates}")
    if len(shared_scope) > 20:
        warnings.append("batch shared scope is broad; consider splitting before PR if review becomes unclear")

    return {
        "schema_version": "aistock_batch_validation_selector_v1",
        "module": module,
        "shared_files": shared_scope,
        "selector_files": selector_files,
        "selected_validation": selected,
        "required_plans": required_plans,
        "recommended_plans": flow._unique_strings(selected.get("recommended_plans") or []),
        "production_gates": gates,
        "per_issue": per_issue,
        "blocking": blocking,
        "warnings": warnings,
        "workflow_gate": "compatible" if not blocking else "blocked",
    }


def _build_batch_code_intelligence_summary(
    *,
    batch_id: str,
    records: list[dict[str, Any]],
    changed_files: list[str] | None,
    root: Path,
) -> dict[str, Any]:
    scoped_files = _normalize_changed_files(changed_files)
    if not scoped_files:
        scoped_files = flow._unique_strings(path for record in records for path in _record_scope(record))
    return code_intelligence.build_summary(
        item_id=batch_id,
        query=_batch_code_intelligence_query(records, scoped_files),
        changed_files=scoped_files,
        module=str(records[0].get("module") or "").strip() if records else None,
        root=root,
        skip_external=False,
    )


def _normalize_changed_files(changed_files: list[str] | None) -> list[str]:
    normalized: list[str] = []
    for path in changed_files or []:
        value = str(path).strip().replace("\\", "/")
        if not value:
            continue
        while value.startswith("./"):
            value = value[2:]
        normalized.append(value)
    return flow._unique_strings(normalized)


def _submit_bug_file_root() -> Path:
    return REPO_ROOT


def _normalize_submit_bug_input_path(value: str, *, option: str, root: Path) -> str:
    raw = str(value or "").strip()
    normalized = raw.replace("\\", "/")
    if (
        not normalized
        or normalized.startswith("/")
        or re.match(r"^[A-Za-z]:(?:/|$)", normalized)
        or any(part == ".." for part in normalized.split("/"))
    ):
        raise WorkflowError(f"{option} must be a safe repository-relative path: {raw!r}")
    parts = [part for part in normalized.split("/") if part not in {"", "."}]
    if not parts:
        raise WorkflowError(f"{option} must be a safe repository-relative path: {raw!r}")
    reserved_windows_names = {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{index}" for index in range(1, 10)),
        *(f"LPT{index}" for index in range(1, 10)),
    }
    if any(
        re.search(r'[\x00:*?"<>|]', part)
        or part.endswith((" ", "."))
        or part.split(".", 1)[0].upper() in reserved_windows_names
        for part in parts
    ):
        raise WorkflowError(f"{option} must be a safe repository-relative path: {raw!r}")
    normalized = "/".join(parts)
    try:
        target = (root / Path(*parts)).resolve()
    except OSError as exc:
        raise WorkflowError(f"{option} must be a safe repository-relative path: {raw!r}") from exc
    try:
        target.relative_to(root.resolve())
    except ValueError as exc:
        raise WorkflowError(f"{option} must be a safe repository-relative path: {raw!r}") from exc
    return normalized


def _normalize_submit_bug_input_group(values: list[str], *, option: str, root: Path) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        path = _normalize_submit_bug_input_path(value, option=option, root=root)
        identity = path.casefold()
        if identity in seen:
            continue
        seen.add(identity)
        normalized.append(path)
    return normalized


def _validate_submit_bug_file_inputs(
    *,
    changed_files: list[str],
    added_files: list[str],
    module: str,
    root: Path,
) -> dict[str, Any]:
    normalized_changed = _normalize_submit_bug_input_group(
        changed_files,
        option="--changed-file",
        root=root,
    )
    normalized_added = _normalize_submit_bug_input_group(
        added_files,
        option="--added-file",
        root=root,
    )
    changed_identities = {path.casefold() for path in normalized_changed}
    category_duplicates = [path for path in normalized_added if path.casefold() in changed_identities]
    if category_duplicates:
        raise WorkflowError(
            "file cannot be declared by both --changed-file and --added-file: "
            + ", ".join(category_duplicates)
        )

    for path in normalized_changed:
        target = root / Path(path)
        if not target.exists():
            raise WorkflowError(f"--changed-file does not exist: {path}")
        if not target.is_file():
            raise WorkflowError(f"--changed-file must identify a file, not a directory: {path}")
    for path in normalized_added:
        target = root / Path(path)
        if target.exists():
            raise WorkflowError(f"--added-file already exists; use --changed-file instead: {path}")

    scope_files = normalized_changed + normalized_added
    ownership = flow.match_changed_files(scope_files)
    unmatched = list(ownership.get("unmatched_files") or [])
    if unmatched:
        raise WorkflowError(
            "file ownership catalog has no match for submit-bug scope: " + ", ".join(unmatched)
        )
    return {
        "schema_version": "aistock_submit_bug_file_preflight_v1",
        "module": module,
        "changed_files": normalized_changed,
        "added_files": normalized_added,
        "scope_files": scope_files,
        "ownership": ownership,
        "workflow_gate": "passed",
    }


def _path_in_prefixes(path: str, prefixes: tuple[str, ...]) -> bool:
    normalized = path.replace("\\", "/").lstrip("./")
    return any(normalized == prefix.rstrip("/") or normalized.startswith(prefix) for prefix in prefixes)


def _tier_ge(left: str, right: str) -> bool:
    return FAST_PATH_TIER_ORDER[left] >= FAST_PATH_TIER_ORDER[right]


def _bump_fast_path_tier(current: str, candidate: str) -> str:
    return candidate if _tier_ge(candidate, current) else current


def _plans_to_commands(plan_keys: list[str]) -> list[str]:
    plans = flow._plans_by_key()
    commands: list[str] = []
    for key in plan_keys:
        plan = plans.get(key) or {}
        session = str(plan.get("nox_session") or "").strip()
        if session:
            commands.append(f"python -m nox -s {session}")
        elif key:
            commands.append(f"python -m nox -s {key}")
    return flow._unique_strings(commands)


def _file_category(path: str) -> str:
    normalized = path.replace("\\", "/").lstrip("./")
    if normalized in FAST_PATH_WORKFLOW_FILES:
        return "workflow"
    if _path_in_prefixes(normalized, FAST_PATH_REGISTRY_PREFIXES):
        return "bug_registry"
    if _path_in_prefixes(normalized, FAST_PATH_CATALOG_PREFIXES):
        return "validation_catalog"
    if normalized.startswith("docs/"):
        return "docs"
    if normalized.startswith(".codex/") or normalized.startswith(".claude/"):
        return "client_wrapper"
    if normalized in FAST_PATH_DEPENDENCY_FILES or flow._requires_production_ddl(normalized):
        return "production_gate_sensitive"
    if normalized.startswith("frontend/"):
        return "frontend"
    if normalized.startswith("backend/"):
        return "backend"
    if normalized.startswith("tests/"):
        return "tests"
    if normalized.startswith("scripts/"):
        return "scripts"
    return "other"


def _infer_fast_path_tier(
    *,
    record: dict[str, Any] | None,
    changed_files: list[str],
    validation: dict[str, Any],
    ownership: dict[str, Any],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    tier = "T0"
    categories = {_file_category(path) for path in changed_files}
    docs_lite_change = bool(changed_files) and all(flow._is_docs_lite_path(path) for path in changed_files)
    docs_fast_tier = validation.get("docs_fast_tier")
    docs_controlled_required = bool(validation.get("docs_controlled_required"))
    metadata_only = bool(categories) and categories <= {"docs", "client_wrapper", "bug_registry"} and not record
    severity_parts = str((record or {}).get("severity") or "").upper().split()
    severity = severity_parts[0] if severity_parts else ""
    module = str((record or {}).get("module") or "").strip()

    if record:
        tier = _bump_fast_path_tier(tier, "T1")
        reasons.append("linked BUG/GitHub issue requires standard issue traceability")
    if severity in {"P0", "P1"}:
        tier = _bump_fast_path_tier(tier, "T1")
        reasons.append(f"{severity} issue keeps at least T1 validation and evidence")
    if not changed_files:
        reasons.append("no changed files supplied; plan uses issue metadata and l0 fallback")
    if docs_lite_change:
        reasons.append(f"{docs_fast_tier or 'docs-fast-update'} scope uses git diff check plus version/change note only")
    elif metadata_only:
        reasons.append("docs/client/registry-only scope can stay T0")
    if docs_controlled_required:
        tier = _bump_fast_path_tier(tier, "T1")
        reasons.append("controlled docs/client instructions keep normal workflow guardrails")
    if categories & {"workflow", "validation_catalog", "backend", "frontend", "scripts", "tests"}:
        tier = _bump_fast_path_tier(tier, "T1")
        reasons.append("code, workflow, test, or validation catalog files require T1")
    if categories & {"production_gate_sensitive"}:
        tier = _bump_fast_path_tier(tier, "T2")
        reasons.append("dependency, migration, or DDL-sensitive files require expanded gate checks")
    matched_rules = ownership.get("matched_rules") or []
    primary_modules = flow._unique_strings([item.get("primary_module") for item in matched_rules if item.get("primary_module")])
    if not metadata_only and len(primary_modules) > 1:
        tier = _bump_fast_path_tier(tier, "T2")
        reasons.append("multiple primary modules require shared validation planning")
    risk_levels = {str(item).lower() for item in ownership.get("risk_levels") or []}
    if "critical" in risk_levels:
        tier = _bump_fast_path_tier(tier, "T2")
        reasons.append("critical ownership risk requires T2")
    if module in {"paper_v2", "strategy_package", "research_assistant"} and changed_files:
        tier = _bump_fast_path_tier(tier, "T2")
        reasons.append(f"{module} is high-risk product scope; avoid T0 shortcut")
    if any(path.startswith("docs/architecture/") for path in changed_files) and not docs_lite_change:
        tier = _bump_fast_path_tier(tier, "T3")
        reasons.append("strict architecture/design documents require T3 design review context")
    return tier, flow._unique_strings(reasons)


def build_fast_path_plan(
    *,
    bug_id: str | None,
    issue_json: str | None,
    changed_files: list[str] | None,
    module: str | None = None,
    allow_missing_linkage: bool = False,
    allow_closed: bool = False,
) -> dict[str, Any]:
    changed = _normalize_changed_files(changed_files)
    record: dict[str, Any] | None = None
    source_path: Path | None = None
    canonical_bug_id: str | None = bug_id.strip().upper() if bug_id else None
    missing_linkage: list[str] = []
    status: str | None = None
    if bug_id or issue_json:
        record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
        canonical_bug_id = str(record.get("bug_id") or canonical_bug_id or source_path.stem).upper()
        missing_linkage = _require_github_linkage(record, allow_missing=allow_missing_linkage)
        status = _require_fixable_status(record, allow_closed=allow_closed)
        module = str(record.get("module") or module or "").strip() or None

    validation = flow.select_validation(changed, module=module)
    if record:
        validation = _apply_validation_budget(record=record, validation=validation)
    ownership = validation.get("ownership") or flow.match_changed_files(changed)
    tier, reasons = _infer_fast_path_tier(
        record=record,
        changed_files=changed,
        validation=validation,
        ownership=ownership,
    )
    required = [str(item) for item in validation.get("required_plans") or []]
    recommended = [str(item) for item in validation.get("recommended_plans") or []]
    context_strategy = {
        "primary_sources": [
            "current BUG/GitHub issue body" if record else "explicit user request",
            "tmp/issue_workflow/<BUG>/context-pack.md when a BUG is linked",
            "changed-file ownership and test plan catalogs",
            "CodeGraph/Understand Anything refs only when available and relevant",
        ],
        "avoid_by_default": [
            "archived standards",
            "old design notes",
            "feature/module/architecture design documents unless the BUG cites them, the user asks, or the tier is T3",
            "full module restart plans",
            "full logs unless triage requires the failing excerpt",
        ],
        "max_initial_files": 4 if tier in {"T0", "T1"} else 8,
    }
    if tier == "T0":
        context_strategy["goal"] = "metadata-only or docs/registry fast path; do not load module history"
    elif tier == "T1":
        context_strategy["goal"] = "single issue Context Pack plus targeted code snippets; do not read design docs by default"
    elif tier == "T2":
        context_strategy["goal"] = "shared same-module or multi-impact context with selected validation; keep design docs opt-in"
    else:
        context_strategy["goal"] = "design/architecture review with broader acceptance evidence"

    stop_conditions = [
        "doctor reports workflow_gate=blocked",
        "required validation cannot run",
        "production runtime, production DB, or DDL action is needed without explicit user approval",
    ]
    if record:
        stop_conditions.extend(
            [
                "BUG JSON lacks github_issue_number or github_issue_url",
                "fix requires files outside allowed_write_scope",
            ]
        )

    if canonical_bug_id:
        next_command = f"python scripts/aistock_issue_workflow.py run --bug-id {canonical_bug_id} --mode plan --create-worktree"
    else:
        next_command = "python scripts/aistock_issue_workflow.py doctor"

    return {
        "schema_version": "aistock_issue_workflow_fast_path_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "planned",
        "bug_id": canonical_bug_id,
        "module": module,
        "status": status,
        "source_bug_json": _repo_rel(source_path) if source_path else None,
        "missing_github_linkage": missing_linkage,
        "changed_files": changed,
        "file_categories": {path: _file_category(path) for path in changed},
        "task_tier": tier,
        "tier_reasons": reasons,
        "context_strategy": context_strategy,
        "validation": validation,
        "code_intelligence_hint": _code_intelligence_hint(REPO_ROOT),
        "required_validation": required,
        "recommended_validation": recommended,
        "required_commands": _plans_to_commands(required),
        "recommended_commands": _plans_to_commands(recommended),
        "production_gates": validation.get("production_gates") or {},
        "estimated_workflow_steps": _estimated_fast_path_steps(tier, has_bug=bool(record)),
        "stop_conditions": stop_conditions,
        "next_command": next_command,
    }


def _estimated_fast_path_steps(tier: str, *, has_bug: bool) -> list[str]:
    if tier == "T0":
        return [
            "conditional doctor only for client/bootstrap/stale-state diagnostics",
            "targeted diff or metadata edit",
            "changed-file lint or l0 only when code/catalog changed",
            "commit and PR evidence",
        ]
    if tier == "T1":
        return [
            "conditional doctor only for client/bootstrap/stale-state diagnostics",
            "run plan/create worktree and read compact Context Pack",
            "targeted fix within allowed_write_scope",
            "finish plan-only, selected validation, PR automation",
        ]
    if tier == "T2":
        return [
            "conditional doctor only for client/bootstrap/stale-state diagnostics",
            "run-p0/start-batch when issues share module and validation",
            "shared context/code-intelligence refs",
            "selected module validation plus per-issue evidence",
        ]
    return [
        "conditional doctor only for client/bootstrap/stale-state diagnostics",
        "design/architecture doc and acceptance matrix",
        "implementation with broader scope review",
        "full required validation and production gates",
    ]


def build_workflow_smoke_plan(
    *,
    bug_id: str | None = None,
    issue_json: str | None = None,
    changed_files: list[str] | None = None,
    module: str | None = None,
) -> dict[str, Any]:
    synthetic_record = False
    cleanup_paths: list[Path] = []
    if not bug_id and not issue_json:
        synthetic_record = True
        smoke_bug_id = _synthetic_smoke_bug_id()
        smoke_dir = REPO_ROOT / WORKFLOW_ROOT / "smoke"
        smoke_workflow_dir = REPO_ROOT / WORKFLOW_ROOT / smoke_bug_id
        if smoke_workflow_dir.exists():
            _remove_synthetic_smoke_workflow_dir(smoke_workflow_dir, smoke_bug_id)
        cleanup_paths.append(smoke_workflow_dir)
        issue_path = smoke_dir / f"synthetic-{smoke_bug_id}.json"
        record = {
            "bug_id": smoke_bug_id,
            "title": "Workflow smoke synthetic issue",
            "module": module or "validation.guardrails",
            "severity": "P2",
            "status": "open",
            "description": "Synthetic issue used to dry-run the workflow state machine.",
            "reproduce_command": "python scripts/aistock_issue_workflow.py workflow-smoke",
            "allowed_write_scope": ["scripts/aistock_issue_workflow.py", "backend/tests/scripts/test_aistock_issue_workflow.py"],
            "required_verification": ["l0"],
            "github_issue_number": 999999,
            "github_issue_url": "https://github.com/licong01-cloud/AIstock/issues/999999",
        }
        _write_json(issue_path, record)
        cleanup_paths.append(issue_path)
        issue_json = str(issue_path)
        bug_id = smoke_bug_id

    changed = _normalize_changed_files(changed_files) or ["scripts/aistock_issue_workflow.py"]
    blocking: list[str] = []
    warnings: list[str] = []
    dirty_before = _git_status_paths(REPO_ROOT)
    doctor_payload: dict[str, Any] | None = None
    fast_path: dict[str, Any] | None = None
    start: dict[str, Any] | None = None
    finish: dict[str, Any] | None = None
    postmortem_preview: dict[str, Any] | None = None
    try:
        doctor_payload = {
            "workflow_gate": "skipped",
            "reason": "workflow-smoke validates the lightweight state-machine contract; run doctor explicitly for diagnostics",
        }
        fast_path = build_fast_path_plan(
            bug_id=bug_id,
            issue_json=issue_json,
            changed_files=changed,
            module=module,
            allow_missing_linkage=True,
            allow_closed=True,
        )
        shared_code_intelligence = {
            "schema_version": "aistock_code_intelligence_summary_v1",
            "provider": "workflow_smoke_contract",
            "status": "skipped_external_in_smoke",
            "context_ref": "not_required_workflow_smoke",
            "manifest_ref": "not_required_workflow_smoke",
            "affected_tests_ref": "not_required_workflow_smoke",
            "affected_tests_count": 0,
            "affected_quality": "not_required_workflow_smoke",
            "affected_tests": {"suggested_tests": []},
            "fallback_used": False,
            "understand_anything": {"status": "not_required_workflow_smoke", "graph_exists": True},
            "understand_anything_summary_ref": "not_required_workflow_smoke",
        }
        start = build_start_plan(
            bug_id=bug_id,
            issue_json=issue_json,
            changed_files=changed,
            create_worktree=True,
            dry_run=True,
            task_slug="workflow-smoke",
            allow_missing_linkage=True,
            allow_closed=True,
            code_intelligence_summary_override=shared_code_intelligence,
        )
        finish = build_finish_plan(
            bug_id=bug_id,
            issue_json=issue_json,
            changed_files=changed,
            base="origin/main",
            head="HEAD",
            validation_evidence=[],
            plan_only=True,
            allow_missing_evidence=False,
            code_intelligence_summary_override=shared_code_intelligence,
        )
        postmortem_preview = {
            "schema_version": "aistock_issue_workflow_smoke_postmortem_preview_v1",
            "bug_id": start["bug_id"],
            "timing_summary": _workflow_timing_summary(start["bug_id"], root=REPO_ROOT),
            "state_path": start.get("state_path"),
            "finish_plan_path": (finish or {}).get("artifact_metrics", {}).get("finish_plan", {}).get("path"),
            "stale_pr_check": "skipped_in_smoke_to_avoid_external_github_reads",
        }
    except Exception as exc:
        blocking.append(str(exc))
    if synthetic_record:
        warnings.extend(_cleanup_synthetic_smoke_artifacts(list(reversed(cleanup_paths)), bug_id or "BUG-000"))
    dirty_after = _git_status_paths(REPO_ROOT)
    before_paths = {row["path"] for row in dirty_before}
    after_paths = {row["path"] for row in dirty_after}
    new_paths = sorted(after_paths - before_paths)
    allowed_prefixes = (
        f"{WORKFLOW_ROOT.as_posix()}/{(bug_id or 'BUG-000').upper()}",
        f"{WORKFLOW_ROOT.as_posix()}/smoke",
    )
    unexpected = [
        path
        for path in new_paths
        if not any(path == prefix or path.startswith(prefix + "/") for prefix in allowed_prefixes)
    ]
    if unexpected:
        blocking.append(f"workflow smoke created unexpected git-status paths: {unexpected}")
    if synthetic_record:
        warnings.append("used isolated synthetic BUG-000-compatible record under ignored tmp/issue_workflow")
    return {
        "schema_version": "aistock_issue_workflow_smoke_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "passed" if not blocking else "blocked",
        "blocking": blocking,
        "warnings": warnings,
        "dry_run": True,
        "synthetic_record": synthetic_record,
        "changed_files": changed,
        "dirty_paths_before": dirty_before,
        "dirty_paths_after": dirty_after,
        "new_dirty_paths": new_paths,
        "unexpected_dirty_paths": unexpected,
        "client_manifest": (doctor_payload or {}).get("client_manifest"),
        "restart_recommended": (doctor_payload or {}).get("restart_recommended"),
        "h7_code_intelligence": (doctor_payload or {}).get("h7_code_intelligence"),
        "fast_path": fast_path,
        "start": start,
        "finish": finish,
        "postmortem_preview": postmortem_preview,
        "cleanup_paths": [_repo_rel(path) for path in cleanup_paths],
        "production_gates": (fast_path or {}).get("production_gates") or _production_gates_payload(),
        "next_command": f"python scripts/aistock_issue_workflow.py fast-path --bug-id {bug_id} --changed-file <path>"
        if bug_id
        else "python scripts/aistock_issue_workflow.py doctor",
    }


def _smoke_nightly_status_payload() -> dict[str, Any]:
    return {
        "statuses": {
            "runnerPreflight": "success",
            "drSnapshot": "success",
            "drValidate": "success",
            "nightlyL3": "failure",
            "paperV2Live": "skipped",
            "codeIntelligence": "success",
        },
        "run_id": "999999999",
        "run_url": "https://github.com/licong01-cloud/AIstock/actions/runs/999999999",
    }


def _smoke_batch_record(
    bug_id: str,
    issue_number: int,
    *,
    title: str,
) -> dict[str, Any]:
    return {
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": bug_id,
        "title": title,
        "module": "validation.guardrails",
        "severity": "P1",
        "status": "open",
        "description": "Synthetic same-module batch workflow smoke record.",
        "actual": "Batch handoff needs a compact no-root-pollution smoke gate.",
        "expected": "Batch start/finish can produce per-issue context and closure evidence under ignored workflow artifacts.",
        "reproduce_command": "python scripts/aistock_issue_workflow.py batch-workflow-smoke",
        "allowed_write_scope": ["scripts/aistock_issue_workflow.py"],
        "required_verification": ["l0"],
        "closure_requirements": [
            "Generate per-issue Context Packs.",
            "Generate a batch finish plan and PR body.",
            "Keep synthetic artifacts ignored.",
        ],
        "non_goals": ["Do not touch production runtime services."],
        "github_issue_number": issue_number,
        "github_issue_url": f"https://github.example/issues/{issue_number}",
        "production_ddl_gate": "noop",
        "production_frontend_dependency_gate": "noop",
        "production_backend_dependency_gate": "noop",
    }


def build_batch_workflow_smoke_plan() -> dict[str, Any]:
    """Dry-run same-module batch start/finish without GitHub or tracked writes."""
    bug_ids = ["BUG-000", "BUG-001"]
    smoke_root = REPO_ROOT / WORKFLOW_ROOT / "batch-smoke"
    issue_dir = smoke_root / "synthetic-bugs"
    records = [
        _smoke_batch_record(bug_ids[0], 9000, title="Synthetic batch smoke one"),
        _smoke_batch_record(bug_ids[1], 9001, title="Synthetic batch smoke two"),
    ]
    issue_paths = [issue_dir / f"{record['bug_id']}.json" for record in records]
    blocking: list[str] = []
    warnings: list[str] = []
    dirty_before = _git_status_paths(REPO_ROOT)
    start_payload: dict[str, Any] | None = None
    finish_payload: dict[str, Any] | None = None

    try:
        for path, record in zip(issue_paths, records, strict=True):
            _write_json(path, record)
        start_payload = build_start_batch_plan(
            bug_ids=bug_ids,
            create_worktree=False,
            dry_run=False,
            task_slug="batch-smoke",
            allow_missing_linkage=False,
            allow_closed=False,
            record_pairs=list(zip(records, issue_paths, strict=True)),
        )
        finish_payload = build_finish_batch_plan(
            batch_id=str(start_payload["batch_id"]),
            bug_ids=[],
            changed_files=["scripts/aistock_issue_workflow.py"],
            base="origin/main",
            head="HEAD",
            validation_evidence=[
                "python scripts/aistock_issue_workflow.py batch-workflow-smoke -> passed",
                "python -m nox -s l0 -> passed",
                "python -m nox -s guardrail_changed_files -> passed",
            ],
            issue_commit=["BUG-000=synthetic-shared-pr", "BUG-001=synthetic-shared-pr"],
            plan_only=False,
            allow_missing_evidence=False,
            record_pairs=list(zip(records, issue_paths, strict=True)),
        )
    except Exception as exc:
        blocking.append(str(exc))

    batch_id = str((start_payload or {}).get("batch_id") or "BATCH-smoke")
    output_dir = REPO_ROOT / WORKFLOW_ROOT / batch_id
    artifacts = {
        "batch_plan": output_dir / "batch-plan.json",
        "batch_state": output_dir / "batch-state.json",
        "finish_plan": output_dir / "finish-plan.json",
        "pr_body": output_dir / "pr-body.md",
    }
    for label, path in artifacts.items():
        if not path.exists():
            blocking.append(f"missing batch smoke artifact: {label}={_repo_rel(path)}")

    batch_state = _load_json(artifacts["batch_state"]) if artifacts["batch_state"].exists() else {}
    finish_plan = _load_json(artifacts["finish_plan"]) if artifacts["finish_plan"].exists() else {}
    pr_body = artifacts["pr_body"].read_text(encoding="utf-8", errors="replace") if artifacts["pr_body"].exists() else ""
    context_dir = Path(str(batch_state.get("context_dir") or ""))
    fix_ready_dir = Path(str(batch_state.get("fix_ready_dir") or ""))
    for bug_id in bug_ids:
        if context_dir and not (REPO_ROOT / context_dir / f"{bug_id}.md").exists():
            blocking.append(f"missing per-issue context markdown for {bug_id}")
        if fix_ready_dir and not (REPO_ROOT / fix_ready_dir / f"{bug_id}.json").exists():
            blocking.append(f"missing per-issue fix-ready JSON for {bug_id}")
        if bug_id not in str(finish_plan.get("per_issue_closure_map") or ""):
            blocking.append(f"finish plan missing per-issue closure map for {bug_id}")
        expected_issue = 9000 if bug_id == "BUG-000" else 9001
        if f"Closes #{expected_issue}" not in pr_body:
            blocking.append(f"batch PR body missing closing keyword for {bug_id}")

    if (finish_payload or {}).get("workflow_gate") != "ready_for_pr":
        blocking.append("batch finish smoke did not reach ready_for_pr")
    if (finish_payload or {}).get("scope_check", {}).get("status") != "passed":
        blocking.append("batch finish scope check did not pass")

    dirty_after = _git_status_paths(REPO_ROOT)
    before_paths = {row["path"] for row in dirty_before}
    new_paths = sorted({row["path"] for row in dirty_after} - before_paths)
    allowed_prefixes = (
        f"{WORKFLOW_ROOT.as_posix()}/batch-smoke",
        f"{WORKFLOW_ROOT.as_posix()}/{batch_id}",
    )
    unexpected = [
        path
        for path in new_paths
        if not any(path == prefix or path.startswith(prefix + "/") for prefix in allowed_prefixes)
    ]
    if unexpected:
        blocking.append(f"batch workflow smoke created unexpected git-status paths: {unexpected}")
    if not dirty_before and not dirty_after:
        warnings.append("git status stayed clean; tmp/issue_workflow batch smoke artifacts are ignored as expected")

    return {
        "schema_version": "aistock_batch_workflow_smoke_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "passed" if not blocking else "blocked",
        "blocking": blocking,
        "warnings": warnings,
        "dry_run": True,
        "github_writes": False,
        "bug_ids": bug_ids,
        "batch_id": batch_id,
        "artifacts": {key: _repo_rel(path) for key, path in artifacts.items()},
        "start": start_payload,
        "finish": finish_payload,
        "dirty_paths_before": dirty_before,
        "dirty_paths_after": dirty_after,
        "new_dirty_paths": new_paths,
        "unexpected_dirty_paths": unexpected,
        "production_gates": _production_gates_payload(),
        "next_command": "python scripts/aistock_issue_workflow.py start-batch --bug-id BUG-XXX --bug-id BUG-YYY --create-worktree",
    }


def _path_under_repo_tmp_validation(path: Path) -> bool:
    try:
        rel = path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return False
    return rel.startswith("tmp/validation/")


def build_nightly_intake_smoke_plan() -> dict[str, Any]:
    """Dry-run Nightly failure intake artifacts without GitHub or tracked writes."""
    smoke_root = REPO_ROOT / "tmp" / "validation" / "nightly_failure_issue" / "smoke"
    status_path = smoke_root / "status.json"
    summary_path = smoke_root / "summary.json"
    markdown_path = smoke_root / "body.md"
    context_path = smoke_root / "context-pack.json"
    context_md_path = smoke_root / "context-pack.md"
    issue_payload_path = smoke_root / "github-issue-payload.json"
    artifacts = {
        "status": status_path,
        "summary": summary_path,
        "markdown": markdown_path,
        "context": context_path,
        "context_markdown": context_md_path,
        "github_issue_payload": issue_payload_path,
    }
    blocking: list[str] = []
    warnings: list[str] = []
    dirty_before = _git_status_paths(REPO_ROOT)
    stdout_payload: dict[str, Any] = {}
    try:
        _write_json(status_path, _smoke_nightly_status_payload())
        stdout_buffer = io.StringIO()
        with contextlib.redirect_stdout(stdout_buffer):
            exit_code = ci_failure_summary.main(
                [
                    "--nightly-status-json",
                    str(status_path),
                    "--run-id",
                    "999999999",
                    "--run-url",
                    "https://github.com/licong01-cloud/AIstock/actions/runs/999999999",
                    "--source-name",
                    "AIstock Nightly",
                    "--output",
                    str(summary_path),
                    "--markdown-output",
                    str(markdown_path),
                    "--context-output",
                    str(context_path),
                    "--context-markdown-output",
                    str(context_md_path),
                    "--github-issue-payload-output",
                    str(issue_payload_path),
                    "--stdout-format",
                    "compact",
                ]
            )
        if exit_code != 0:
            blocking.append(f"Nightly intake summary command exited with {exit_code}")
        stdout_payload = json.loads(stdout_buffer.getvalue() or "{}")
    except Exception as exc:
        blocking.append(str(exc))

    for label, path in artifacts.items():
        if not path.exists():
            blocking.append(f"missing Nightly intake artifact: {label}={_repo_rel(path)}")
        if not _path_under_repo_tmp_validation(path):
            blocking.append(f"Nightly intake artifact is outside tmp/validation: {label}={path}")

    candidate_history = stdout_payload.get("artifacts", {}).get("candidate_history") if isinstance(stdout_payload, dict) else None
    candidate_history_path = Path(candidate_history) if candidate_history else None
    if not candidate_history_path:
        blocking.append("Nightly intake smoke did not persist compact candidate history")
    elif "tests/aistock_validation/history" in candidate_history_path.as_posix():
        blocking.append(f"candidate history used tracked history path: {candidate_history_path}")
    elif not _path_under_repo_tmp_validation(candidate_history_path):
        blocking.append(f"candidate history is outside tmp/validation: {candidate_history_path}")

    context_pack = _load_json(context_path) if context_path.exists() else {}
    issue_payload = _load_json(issue_payload_path) if issue_payload_path.exists() else {}
    handoff = context_pack.get("agent_handoff") if isinstance(context_pack.get("agent_handoff"), dict) else {}
    entrypoints = handoff.get("workflow_entrypoints") if isinstance(handoff.get("workflow_entrypoints"), dict) else {}
    body = str(issue_payload.get("body") or "")
    closed_loop_checks = {
        "agent_handoff_section": "## Agent Handoff" in body,
        "triage_entrypoint": "triage-ci-issue" in str(entrypoints.get("triage") or ""),
        "promotion_requires_registry_worktree": "--create-registry-worktree --apply" in str(entrypoints.get("promote") or ""),
        "needs_bug_json_recorded": handoff.get("needs_bug_json") is True,
        "candidate_history_tmp_only": bool(candidate_history_path and _path_under_repo_tmp_validation(candidate_history_path)),
        "synthetic_marker_recorded": issue_payload.get("synthetic") is True
        and issue_payload.get("failure_kind") == "synthetic_smoke"
        and "aistock-failure-kind:synthetic_smoke" in body,
    }
    for label, ok in closed_loop_checks.items():
        if not ok:
            blocking.append(f"Nightly intake closed-loop check failed: {label}")
    if "triage-ci-issue --issue <issue-number>" not in body:
        blocking.append("GitHub Issue payload is missing triage-ci-issue handoff")
    if "promote-ci-issue --issue <issue-number> --create-registry-worktree --apply" not in body:
        blocking.append("GitHub Issue payload is missing promote-ci-issue registry worktree handoff")
    if "triage-ci-issue" not in str(entrypoints.get("triage") or ""):
        blocking.append("Context Pack is missing triage workflow entrypoint")
    if "promote-ci-issue" not in str(entrypoints.get("promote") or ""):
        blocking.append("Context Pack is missing promote workflow entrypoint")

    dirty_after = _git_status_paths(REPO_ROOT)
    before_paths = {row["path"] for row in dirty_before}
    new_paths = sorted({row["path"] for row in dirty_after} - before_paths)
    allowed_prefix = "tmp/validation/nightly_failure_issue/smoke"
    unexpected = [
        path for path in new_paths if not (path == allowed_prefix or path.startswith(allowed_prefix + "/"))
    ]
    if unexpected:
        blocking.append(f"Nightly intake smoke created unexpected git-status paths: {unexpected}")
    if not dirty_before and not dirty_after:
        warnings.append("git status stayed clean; tmp/validation artifacts are ignored as expected")

    return {
        "schema_version": "aistock_nightly_intake_smoke_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "passed" if not blocking else "blocked",
        "blocking": blocking,
        "warnings": warnings,
        "dry_run": True,
        "github_writes": False,
        "failure_kind": issue_payload.get("failure_kind"),
        "synthetic": issue_payload.get("synthetic") is True,
        "production_gates": _production_gates_payload(),
        "artifacts": {key: _repo_rel(path) for key, path in artifacts.items()},
        "candidate_history_path": _repo_rel(candidate_history_path) if candidate_history_path else None,
        "issue_title": issue_payload.get("title"),
        "nightly_failed_stages": stdout_payload.get("nightly_failed_stages") if isinstance(stdout_payload, dict) else [],
        "closed_loop_checks": closed_loop_checks,
        "handoff_entrypoints": entrypoints,
        "dirty_paths_before": dirty_before,
        "dirty_paths_after": dirty_after,
        "new_dirty_paths": new_paths,
        "unexpected_dirty_paths": unexpected,
        "next_command": "python scripts/aistock_issue_workflow.py triage-ci-issue --issue <created-nightly-issue-number>",
    }


def build_start_plan(
    *,
    bug_id: str | None,
    issue_json: str | None,
    changed_files: list[str],
    create_worktree: bool,
    dry_run: bool,
    task_slug: str | None,
    allow_missing_linkage: bool,
    allow_closed: bool,
    active_decision: dict[str, Any] | None = None,
    code_intelligence_summary_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not issue_json and active_decision and active_decision.get("registry_issue_json"):
        issue_json = str(active_decision["registry_issue_json"])
    record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
    canonical_bug_id = str(record.get("bug_id") or bug_id or source_path.stem).upper()
    missing_linkage = _require_github_linkage(record, allow_missing=allow_missing_linkage)
    status = _require_fixable_status(record, allow_closed=allow_closed)
    worktree_plan = _maybe_create_worktree(
        record=record,
        bug_id=canonical_bug_id,
        source_bug_json=source_path,
        create=create_worktree,
        dry_run=dry_run,
        task_slug=task_slug,
    )
    actual_worktree, planned_worktree = _actual_and_planned_worktree(worktree_plan)
    actual_branch = worktree_plan.get("branch") if actual_worktree else None
    planned_branch = worktree_plan.get("branch") if planned_worktree else None
    target_root = Path(actual_worktree) if actual_worktree else REPO_ROOT
    target_bug_path = _issue_json_path_for_worktree(source_path, target_root)
    output_dir = target_root / WORKFLOW_ROOT / canonical_bug_id
    fix_ready = flow.build_fix_ready(record, changed_files)
    context_pack = flow.build_context_pack(record, changed_files)
    budgeted_validation = _apply_validation_budget(
        record=record,
        validation=fix_ready.get("validation_selection") if isinstance(fix_ready.get("validation_selection"), dict) else {},
    )
    fix_ready["required_verification"] = budgeted_validation["required_plans"]
    fix_ready["recommended_verification"] = budgeted_validation["recommended_plans"]
    fix_ready["validation_selection"] = budgeted_validation
    context_pack["required_verification"] = budgeted_validation["required_plans"]
    context_pack["recommended_verification"] = budgeted_validation["recommended_plans"]
    context_pack["deferred_nightly_plans"] = budgeted_validation.get("deferred_nightly_plans") or []
    code_intelligence_summary = code_intelligence_summary_override or _build_code_intelligence_summary(
        item_id=canonical_bug_id,
        record=record,
        changed_files=changed_files,
        root=target_root,
    )
    fast_path = build_fast_path_plan(
        bug_id=canonical_bug_id,
        issue_json=str(source_path),
        changed_files=changed_files,
        module=record.get("module"),
        allow_missing_linkage=True,
        allow_closed=True,
    )
    context_pack["code_intelligence"] = {
        "provider": code_intelligence_summary.get("provider"),
        "status": code_intelligence_summary.get("status"),
        "context_ref": code_intelligence_summary.get("context_ref"),
        "manifest_ref": code_intelligence_summary.get("manifest_ref"),
        "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
        "fallback_used": code_intelligence_summary.get("fallback_used"),
        "affected_tests_count": code_intelligence_summary.get("affected_tests_count"),
        "affected_quality": code_intelligence_summary.get("affected_quality"),
        "understand_anything_summary_ref": code_intelligence_summary.get("understand_anything_summary_ref"),
        "understand_anything_summary": code_intelligence_summary.get("understand_anything_summary"),
        "understand_anything": code_intelligence_summary.get("understand_anything"),
    }
    fix_ready["code_intelligence"] = context_pack["code_intelligence"]
    fix_ready_path = output_dir / "fix-ready.json"
    context_json_path = output_dir / "context-pack.json"
    context_md_path = output_dir / "context-pack.md"
    task_card_json_path = _task_card_json_path(canonical_bug_id, target_root)
    task_card_md_path = _task_card_md_path(canonical_bug_id, target_root)
    task_record = dict(record)
    task_record["required_verification"] = budgeted_validation["required_plans"]
    task_record["verification_budget"] = _verification_budget_for_record(
        task_record,
        record.get("ui_intake_hints") if isinstance(record.get("ui_intake_hints"), dict) else None,
        budgeted_validation,
    )
    task_record["workflow_efficiency_recommendations"] = {
        **(record.get("workflow_efficiency_recommendations") if isinstance(record.get("workflow_efficiency_recommendations"), dict) else {}),
        "verification_budget": task_record["verification_budget"],
    }
    task_card = build_task_card(
        bug_id=canonical_bug_id,
        record=task_record,
        root=target_root,
        branch=actual_branch,
        planned_branch=planned_branch,
        worktree=actual_worktree,
        planned_worktree=planned_worktree,
        context_pack_json_path=context_json_path,
        context_pack_md_path=context_md_path,
        fix_ready_path=fix_ready_path,
        state_path=_state_path(canonical_bug_id, target_root),
        events_path=_events_path(canonical_bug_id, target_root),
        fix_ready=fix_ready,
        context_pack=context_pack,
        code_intelligence_summary=code_intelligence_summary,
        fast_path=fast_path,
    )
    if not dry_run:
        _write_json(fix_ready_path, fix_ready)
        _write_json(context_json_path, context_pack)
        _write_text(context_md_path, flow.render_context_pack_markdown(context_pack))
        _write_json(task_card_json_path, task_card)
        _write_text(task_card_md_path, render_task_card_markdown(task_card))
        context_metrics = {
            "context_pack_md": _size_and_token_estimate(context_md_path),
            "context_pack_json": _size_and_token_estimate(context_json_path),
            "fix_ready_json": _size_and_token_estimate(fix_ready_path),
            "task_card_md": _size_and_token_estimate(task_card_md_path),
            "task_card_json": _size_and_token_estimate(task_card_json_path),
        }
        _write_state(
            canonical_bug_id,
            state="context_ready",
            root=target_root,
            branch=actual_branch,
            planned_branch=planned_branch,
            workflow_role="fix",
            worktree=actual_worktree,
            planned_worktree=planned_worktree,
            base=worktree_plan.get("base"),
            source_bug_json=_repo_rel(source_path),
            target_bug_json=_repo_rel(target_bug_path, target_root),
            context_pack_md=_repo_rel(context_md_path, target_root),
            context_pack_json=_repo_rel(context_json_path, target_root),
            fix_ready_path=_repo_rel(fix_ready_path, target_root),
            task_card_json=_repo_rel(task_card_json_path, target_root),
            task_card_md=_repo_rel(task_card_md_path, target_root),
            github_issue_number=record.get("github_issue_number"),
            github_issue_url=record.get("github_issue_url"),
            production_gates=fix_ready.get("validation_selection", {}).get("production_gates", {}),
            code_intelligence=context_pack.get("code_intelligence"),
            fast_path=fast_path,
            active_decision=active_decision,
            context_metrics=context_metrics,
            required_verification=fix_ready.get("required_verification") or [],
            recommended_verification=fix_ready.get("recommended_verification") or [],
            deferred_nightly_plans=budgeted_validation.get("deferred_nightly_plans") or [],
            verification_budget=task_record.get("verification_budget"),
            task_card_availability=_task_card_exists_payload(canonical_bug_id, target_root),
            next_actions=[
                "switch_to_worktree_if_created",
                "read_task_card_md_then_context_pack_only_if_needed",
                "fix_only_within_allowed_write_scope_or_stop_for_scope_expansion",
                "run_finish_plan_before_reporting_done",
            ],
        )
        _append_event(
            canonical_bug_id,
            event="fix_in_progress",
            state="fix_in_progress",
            root=target_root,
            evidence={"automatic_phase_boundary": "context_ready_to_active_repair"},
        )
    else:
        context_metrics = {
            "context_pack_md": {"path": str(context_md_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
            "context_pack_json": {"path": str(context_json_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
            "fix_ready_json": {"path": str(fix_ready_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
            "task_card_md": {"path": str(task_card_md_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
            "task_card_json": {"path": str(task_card_json_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
        }
    return {
        "schema_version": "aistock_issue_workflow_start_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "status": status,
        "module": record.get("module"),
        "source_bug_json": _repo_rel(source_path),
        "target_bug_json": _repo_rel(target_bug_path, target_root),
        "github_issue_number": record.get("github_issue_number"),
        "github_issue_url": record.get("github_issue_url"),
        "missing_github_linkage": missing_linkage,
        "worktree_plan": worktree_plan,
        "fix_ready_path": _repo_rel(fix_ready_path, target_root),
        "context_pack_json": _repo_rel(context_json_path, target_root),
        "context_pack_md": _repo_rel(context_md_path, target_root),
        "task_card_json": _repo_rel(task_card_json_path, target_root),
        "task_card_md": _repo_rel(task_card_md_path, target_root),
        "state_path": _repo_rel(_state_path(canonical_bug_id, target_root), target_root),
        "events_path": _repo_rel(_events_path(canonical_bug_id, target_root), target_root),
        "allowed_write_scope": fix_ready.get("allowed_write_scope") or [],
        "required_verification": fix_ready.get("required_verification") or [],
        "recommended_verification": fix_ready.get("recommended_verification") or [],
        "deferred_nightly_plans": budgeted_validation.get("deferred_nightly_plans") or [],
        "production_gates": fix_ready.get("validation_selection", {}).get("production_gates", {}),
        "code_intelligence": context_pack.get("code_intelligence"),
        "fast_path": fast_path,
        "active_decision": active_decision,
        "context_metrics": context_metrics,
        "workflow_efficiency_recommendations": task_record.get("workflow_efficiency_recommendations") or _workflow_efficiency_recommendations(record, record.get("ui_intake_hints") if isinstance(record.get("ui_intake_hints"), dict) else None),
        "ui_intake_hints": record.get("ui_intake_hints"),
        "next_agent_steps": [
            "switch_to_worktree_if_created",
            "read_task_card_md_then_context_pack_only_if_needed",
            "fix_only_within_allowed_write_scope_or_stop_for_scope_expansion",
            "use_compact_success_output_and_full_json_only_on_failure",
            "run_finish_plan_before_reporting_done",
        ],
    }


def _finish_changed_files(base: str, head: str, *, root: Path = REPO_ROOT) -> list[str]:
    """Combine committed branch changes with the current staged, dirty, and untracked task paths."""

    return _normalize_changed_files(
        [
            *flow.changed_files_from_git(base, head),
            *_dirty_files(root),
        ]
    )


def build_finish_plan(
    *,
    bug_id: str | None,
    issue_json: str | None,
    changed_files: list[str] | None,
    base: str,
    head: str,
    validation_evidence: list[str],
    plan_only: bool,
    allow_missing_evidence: bool,
    fresh_process_evidence: list[str] | None = None,
    code_intelligence_summary_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
    canonical_bug_id = str(record.get("bug_id") or bug_id or source_path.stem).upper()
    current_state = _load_state(canonical_bug_id, REPO_ROOT) or {}
    if str(current_state.get("state") or "") in {"context_ready", "fix_in_progress"}:
        _append_event(
            canonical_bug_id,
            event="fix_applied",
            state="fix_applied",
            root=REPO_ROOT,
            evidence={"automatic_phase_boundary": "finish_plan_started"},
        )
    changed = _normalize_changed_files(changed_files) if changed_files is not None else _finish_changed_files(base, head)
    validation = _apply_validation_budget(
        record=record,
        validation=flow.select_validation(changed, module=record.get("module")),
    )
    pr_quality = flow.build_pr_quality(base=base, head=head, issue_record=record, changed_files=changed)
    code_intelligence_summary = code_intelligence_summary_override or _build_code_intelligence_summary(
        item_id=canonical_bug_id,
        record=record,
        changed_files=changed,
        root=REPO_ROOT,
    )
    h7_code_intelligence = _code_intelligence_readiness(code_intelligence_summary)
    codegraph_tests = code_intelligence_summary.get("affected_tests", {}).get("suggested_tests") or []
    if codegraph_tests:
        validation["codegraph_suggested_tests"] = codegraph_tests
    raw_evidence = [item for item in validation_evidence if item.strip()]
    validation_receipts, validation_evidence_errors = _build_validation_receipts(
        raw_evidence,
        root=REPO_ROOT,
        changed_files=changed,
    )
    validation_receipt_plan_coverage = _validation_receipt_plan_coverage(
        validation=validation,
        receipts=validation_receipts,
    )
    if validation_receipts:
        validation_evidence_errors.extend(_validation_receipt_plan_errors(validation_receipt_plan_coverage))
    evidence = [_render_validation_receipt(receipt) for receipt in validation_receipts]
    runtime_contract = build_runtime_contract(
        record=record,
        changed_files=changed,
        root=REPO_ROOT,
        fresh_process_evidence=fresh_process_evidence or [],
    )
    runtime_errors = list(runtime_contract.get("blocking") or [])
    validation_evidence_errors.extend(runtime_errors)
    reconciliation = runtime_contract.get("provisional_reconciliation") or {}
    should_persist_runtime = not runtime_errors and (
        (runtime_contract.get("backend_restart_required") and fresh_process_evidence)
        or (bool(reconciliation.get("applied")) and not plan_only)
    )
    if should_persist_runtime:
        persisted_runtime = dict(record.get("runtime_contract") or {})
        persisted_runtime.update(
            {
                "schema_version": RUNTIME_CONTRACT_SCHEMA,
                "runtime_impact": runtime_contract.get("runtime_impact"),
                "backend_restart_owner": "user",
                "target_id": runtime_contract.get("target_id"),
                "target_ids": runtime_contract.get("target_ids") or [],
                "persistence_basis": runtime_contract.get("persistence_basis"),
                "fresh_process_evidence": runtime_contract.get("fresh_process_evidence") or [],
                "post_restart_effective_gate": runtime_contract.get("post_restart_effective_gate"),
                "runtime_identity_match": runtime_contract.get("runtime_identity_match"),
            }
        )
        persisted_file_scope = record.get("file_scope_contract")
        if reconciliation.get("applied"):
            persisted_runtime.update(
                {
                    "inference_basis": RUNTIME_INFERENCE_ACTUAL_CHANGED_FILES,
                    "provisional": False,
                    "planned_target_ids": reconciliation.get("planned_target_ids") or [],
                }
            )
            persisted_file_scope = _actual_file_scope_contract(record, changed)
        if (
            persisted_runtime != record.get("runtime_contract")
            or persisted_file_scope != record.get("file_scope_contract")
        ):
            record = {**record, "runtime_contract": persisted_runtime}
            if reconciliation.get("applied"):
                record["file_scope_contract"] = persisted_file_scope
            _write_json(source_path, record)
    closure_ready = bool(evidence) and not validation_evidence_errors
    draft_ready = (
        (plan_only or (allow_missing_evidence and not evidence))
        and not validation_evidence_errors
    )
    output_dir = REPO_ROOT / WORKFLOW_ROOT / canonical_bug_id
    pr_body_path = output_dir / "pr-body.md"
    pr_body = render_pr_body(
        canonical_bug_id,
        record,
        changed,
        validation,
        pr_quality,
        evidence,
        closure_ready,
        runtime_contract,
    )
    finish_plan_path = output_dir / "finish-plan.json"
    persist_finish_plan = _workflow_artifacts_enabled() or not closure_ready
    if persist_finish_plan:
        _write_json(finish_plan_path, {
            "bug_id": canonical_bug_id,
            "changed_files": changed,
            "selected_validation": _compact_validation_for_finish(validation),
            "pr_quality": _compact_pr_quality_for_finish(pr_quality),
            "validation_evidence": evidence,
            "validation_receipts": validation_receipts,
            "validation_evidence_errors": validation_evidence_errors,
            "validation_receipt_plan_coverage": validation_receipt_plan_coverage,
            "runtime_contract": runtime_contract,
            "closure_ready": closure_ready,
            "draft_ready": draft_ready,
            "code_intelligence": _compact_code_intelligence_for_finish(code_intelligence_summary, codegraph_tests),
            "h7_code_intelligence": h7_code_intelligence,
            "artifact_policy": "compact_finish_plan_no_full_selected_validation_pr_quality_or_code_intelligence_payload",
        })
    elif finish_plan_path.exists():
        with contextlib.suppress(OSError):
            finish_plan_path.unlink()
    _write_text(pr_body_path, pr_body)
    next_state = "validation_passed" if closure_ready else ("validation_planned" if draft_ready else "blocked")
    _write_state(
        canonical_bug_id,
        state=next_state,
        changed_files=changed,
        validation_evidence=evidence,
        pr_body_path=_repo_rel(pr_body_path),
        allowed_write_scope=flow._as_list(record.get("allowed_write_scope")),
        required_verification=validation.get("required_plans") or [],
        verification_budget=record.get("verification_budget"),
        context_resume_digest=_workflow_context_resume_digest(
            {
                "allowed_write_scope": flow._as_list(record.get("allowed_write_scope")),
                "required_verification": validation.get("required_plans") or [],
                "verification_budget": record.get("verification_budget"),
            }
        ),
        production_gates=validation.get("production_gates") or {},
        runtime_contract=_pick(
            runtime_contract,
            "runtime_impact",
            "backend_restart_required",
            "backend_restart_owner",
            "target_id",
            "catalog_ref",
            "operator_runbook_ref",
            "persistence_basis",
            "fresh_process_evidence",
            "post_restart_effective_gate",
            "runtime_identity_match",
            "blocking",
        ),
        code_intelligence={
            "status": code_intelligence_summary.get("status"),
            "context_ref": code_intelligence_summary.get("context_ref"),
            "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
            "fallback_used": code_intelligence_summary.get("fallback_used"),
            "affected_tests_count": code_intelligence_summary.get("affected_tests_count"),
            "understand_anything_summary_ref": code_intelligence_summary.get("understand_anything_summary_ref"),
            "readiness_next_command": h7_code_intelligence.get("readiness_next_command"),
            "fallback_reason": h7_code_intelligence.get("fallback_reason"),
        },
        stop_reason=None
        if closure_ready
        else ("; ".join(validation_evidence_errors) if validation_evidence_errors else "validation_evidence_missing"),
        next_actions=[
            "commit_only_task_files",
            "push_task_branch",
            "create_pr_from_pr_body",
            "watch_ci_before_merge",
        ] if closure_ready else ["run_required_validation", "rerun_finish_with_validation_evidence"],
    )
    payload = {
        "schema_version": "aistock_issue_workflow_finish_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "source_bug_json": _repo_rel(source_path),
        "changed_files": changed,
        "fast_path": build_fast_path_plan(
            bug_id=canonical_bug_id,
            issue_json=str(source_path),
            changed_files=changed,
            module=record.get("module"),
            allow_missing_linkage=True,
            allow_closed=True,
        ),
        "required_verification": validation.get("required_plans") or [],
        "recommended_verification": validation.get("recommended_plans") or [],
        "deferred_nightly_plans": validation.get("deferred_nightly_plans") or [],
        "production_gates": validation.get("production_gates") or {},
        "runtime_contract": runtime_contract,
        "backend_restart": {
            "required": runtime_contract.get("backend_restart_required"),
            "owner": runtime_contract.get("backend_restart_owner"),
            "target_id": runtime_contract.get("target_id"),
            "operator_runbook_ref": runtime_contract.get("operator_runbook_ref"),
        },
        "post_restart_effective_gate": runtime_contract.get("post_restart_effective_gate"),
        "runtime_identity_match": runtime_contract.get("runtime_identity_match"),
        "next_user_action": (
            "after merge, restart the catalog target and run post-restart-verify"
            if runtime_contract.get("backend_restart_required") and not runtime_errors
            else None
        ),
        "scope_check": pr_quality.get("scope_check"),
        "code_intelligence": {
            "status": code_intelligence_summary.get("status"),
            "context_ref": code_intelligence_summary.get("context_ref"),
            "manifest_ref": code_intelligence_summary.get("manifest_ref"),
            "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
            "fallback_used": code_intelligence_summary.get("fallback_used"),
            "affected_tests_count": code_intelligence_summary.get("affected_tests_count"),
            "affected_quality": code_intelligence_summary.get("affected_quality"),
            "understand_anything_summary_ref": code_intelligence_summary.get("understand_anything_summary_ref"),
        },
        "codegraph_suggested_tests": codegraph_tests,
        "h7_code_intelligence": h7_code_intelligence,
        "validation_evidence": evidence,
        "validation_receipts": validation_receipts,
        "validation_evidence_errors": validation_evidence_errors,
        "validation_receipt_plan_coverage": validation_receipt_plan_coverage,
        "validation_receipt_summary": _validation_receipt_summary(
            evidence,
            validation.get("deferred_nightly_plans") or [],
        ),
        "closure_ready": closure_ready,
        "draft_ready": draft_ready,
        "workflow_gate": "ready_for_pr"
        if closure_ready
        else (
            "blocked"
            if validation_evidence_errors
            else (
                "plan_ready"
                if plan_only
                else (
                    "validation_evidence_missing_allowed"
                    if allow_missing_evidence
                    else "validation_evidence_missing"
                )
            )
        ),
        "pr_body_path": _repo_rel(pr_body_path),
        "state_path": _repo_rel(_state_path(canonical_bug_id)),
        "events_path": _repo_rel(_events_path(canonical_bug_id)),
        "artifact_metrics": {
            "pr_body": _size_and_token_estimate(pr_body_path),
            "finish_plan": _size_and_token_estimate(finish_plan_path),
        },
        "artifact_policy": (
            "compact_success_no_finish_plan_json"
            if not persist_finish_plan
            else ("draft_finish_plan_persisted" if draft_ready else "diagnostic_json_persisted")
        ),
    }
    payload["pre_pr_gate"] = _pre_pr_gate(finish=payload, validation_evidence=evidence, root=REPO_ROOT, run_lint=False)
    if not closure_ready:
        if validation_evidence_errors:
            payload["error"] = "; ".join(validation_evidence_errors)
        elif plan_only:
            payload["error"] = "plan-only draft generated; complete required validation evidence before PR readiness"
        elif allow_missing_evidence:
            payload["error"] = "missing-evidence draft generated; complete required validation evidence before PR readiness"
        else:
            payload["error"] = "validation evidence is required"
    return payload


def _codegraph_test_lines(tests: Iterable[Any], *, limit: int = PR_BODY_CODEGRAPH_TEST_LIMIT) -> list[str]:
    normalized = [str(item).strip() for item in tests or [] if str(item).strip()]
    if not normalized:
        return ["- CodeGraph suggested tests: `none`"]
    visible = normalized[:limit]
    lines = [f"- CodeGraph suggested test: `{path}`" for path in visible]
    omitted = len(normalized) - len(visible)
    if omitted > 0:
        lines.append(
            f"- CodeGraph suggested tests omitted: `{omitted}` more; see `affected-tests.json` / task card artifacts."
        )
    return lines


def _list_preview(items: Iterable[Any], *, limit: int = PR_BODY_CODEGRAPH_TEST_LIMIT) -> dict[str, Any]:
    normalized = [str(item).strip() for item in items or [] if str(item).strip()]
    visible = normalized[:limit]
    return {
        "count": len(normalized),
        "preview": visible,
        "omitted_count": max(0, len(normalized) - len(visible)),
    }


def _compact_code_intelligence_for_finish(
    code_intelligence_summary: dict[str, Any],
    tests: list[Any],
) -> dict[str, Any]:
    return {
        "status": code_intelligence_summary.get("status"),
        "context_ref": code_intelligence_summary.get("context_ref"),
        "manifest_ref": code_intelligence_summary.get("manifest_ref"),
        "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
        "fallback_used": code_intelligence_summary.get("fallback_used"),
        "fallback_reason": code_intelligence_summary.get("fallback_reason"),
        "affected_tests_count": code_intelligence_summary.get("affected_tests_count"),
        "affected_quality": code_intelligence_summary.get("affected_quality"),
        "understand_anything_summary_ref": code_intelligence_summary.get("understand_anything_summary_ref"),
        "suggested_tests_count": len(tests),
        "suggested_tests_preview": [str(item) for item in tests[:PR_BODY_CODEGRAPH_TEST_LIMIT]],
        "full_payload_inlined": False,
    }


def _compact_validation_for_finish(validation: dict[str, Any]) -> dict[str, Any]:
    codegraph_tests = validation.get("codegraph_suggested_tests") or []
    ownership = validation.get("ownership") if isinstance(validation.get("ownership"), dict) else {}
    return {
        "schema_version": validation.get("schema_version"),
        "required_plans": validation.get("required_plans") or [],
        "recommended_plans": validation.get("recommended_plans") or [],
        "deferred_nightly_plans": validation.get("deferred_nightly_plans") or [],
        "production_gates": validation.get("production_gates") or {},
        "primary_modules": validation.get("primary_modules") or [],
        "impacted_modules": validation.get("impacted_modules") or [],
        "ownership": {
            "matched_rule_count": len(ownership.get("matched_rules") or []),
            "unmatched_file_count": len(ownership.get("unmatched_files") or []),
            "risk_levels": ownership.get("risk_levels") or [],
            "suggested_scope": _list_preview(ownership.get("suggested_scope") or []),
        },
        "codegraph_suggested_tests": _list_preview(codegraph_tests),
        "full_payload_inlined": False,
        "token_policy": "skip maps, ownership rule bodies, and full CodeGraph test lists stay in source artifacts only.",
    }


def _compact_pr_quality_for_finish(pr_quality: dict[str, Any]) -> dict[str, Any]:
    scope = pr_quality.get("scope_check") if isinstance(pr_quality.get("scope_check"), dict) else {}
    return {
        "scope_check": _pick(scope, "status", "violations", "status_source"),
        "selected_validation_inlined": False,
        "llm_summary_inlined": False,
        "full_payload_inlined": False,
    }


def render_pr_body(
    bug_id: str,
    record: dict[str, Any],
    changed_files: list[str],
    validation: dict[str, Any],
    pr_quality: dict[str, Any],
    evidence: list[str],
    closure_ready: bool,
    runtime_contract: dict[str, Any] | None = None,
) -> str:
    gates = validation.get("production_gates") or {}
    code_intel = validation.get("h7_code_intelligence") or {}
    runtime_contract = runtime_contract or {}
    lines = [
        f"## {bug_id} issue workflow summary",
        "",
        f"- GitHub Issue: {record.get('github_issue_url') or 'missing'}",
        f"- Module: `{record.get('module') or 'unknown'}`",
        f"- Scope check: `{(pr_quality.get('scope_check') or {}).get('status')}`",
        f"- Closure ready: `{str(closure_ready).lower()}`",
        "",
        "## Changed files",
        *[f"- `{path}`" for path in changed_files or ["none"]],
        "",
        "## Required validation",
        *[f"- `{plan}`" for plan in validation.get("required_plans") or ["l0"]],
        "",
        "## Deferred nightly / VC validation",
        *[f"- `{plan}`" for plan in validation.get("deferred_nightly_plans") or ["none"]],
        "",
        "## Code intelligence",
        *_codegraph_test_lines(validation.get("codegraph_suggested_tests") or []),
        f"- H7 readiness: `{code_intel.get('workflow_gate') or 'unknown'}`",
        f"- fallback_used: `{str(bool(code_intel.get('fallback_used'))).lower()}`",
        f"- readiness_next_command: `{code_intel.get('readiness_next_command') or 'not_required'}`",
        "",
        "## Evidence",
        *[f"- {item}" for item in evidence or ["missing - run required validation before requesting merge"]],
        "",
        "## Production gates",
        f"- production_ddl_gate: `{gates.get('ddl', 'noop')}`",
        f"- production_frontend_dependency_gate: `{gates.get('frontend_dependency', 'noop')}`",
        f"- production_backend_dependency_gate: `{gates.get('backend_dependency', 'noop')}`",
        "",
        "## Runtime contract",
        f"- runtime_impact: `{runtime_contract.get('runtime_impact') or 'unknown'}`",
        f"- backend_restart_required: `{str(bool(runtime_contract.get('backend_restart_required'))).lower()}`",
        f"- backend_restart_owner: `{runtime_contract.get('backend_restart_owner') or 'user'}`",
        f"- target_ids: `{', '.join(runtime_contract.get('target_ids') or []) or 'none'}`",
        f"- persistence_basis: `{runtime_contract.get('persistence_basis') or 'unknown'}`",
        f"- post_restart_effective_gate: `{runtime_contract.get('post_restart_effective_gate') or 'unknown'}`",
        *[f"- fresh_process_evidence: {item}" for item in runtime_contract.get("fresh_process_evidence") or ["not_required"]],
        "",
        (
            f"Refs #{record.get('github_issue_number')}"
            if (runtime_contract or {}).get("backend_restart_required")
            else f"Closes #{record.get('github_issue_number')}"
        )
        if record.get("github_issue_number")
        else "",
    ]
    return "\n".join(line for line in lines if line is not None)


def build_triage_p0(*, include_fixed: bool = False) -> dict[str, Any]:
    accepted_statuses = set(ALLOWED_FIX_STATUSES)
    if include_fixed:
        accepted_statuses.update({"fixed", "verified"})
    items: list[dict[str, Any]] = []
    groups: dict[tuple[str, tuple[str, ...]], list[str]] = {}
    for path in _bug_files():
        record = _load_json(path)
        severity = str(record.get("severity") or "").upper().split()[0]
        status = str(record.get("status") or "")
        if severity != "P0" or status not in accepted_statuses:
            continue
        bug_id = str(record.get("bug_id") or path.stem)
        required = tuple(flow._unique_strings(flow._as_list(record.get("required_verification"))))
        key = (str(record.get("module") or "unknown"), required)
        groups.setdefault(key, []).append(bug_id)
        missing = [field for field in ("github_issue_number", "github_issue_url") if not record.get(field)]
        items.append(
            {
                "bug_id": bug_id,
                "title": record.get("title"),
                "status": status,
                "module": record.get("module"),
                "github_issue_number": record.get("github_issue_number"),
                "github_issue_url": record.get("github_issue_url"),
                "missing_github_linkage": missing,
                "required_verification": list(required),
                "allowed_write_scope": flow._as_list(record.get("allowed_write_scope")),
                "source_bug_json": _repo_rel(path),
            }
        )
    group_payload = []
    for (module, required), bug_ids in sorted(groups.items(), key=lambda item: (item[0][0], item[0][1], item[1])):
        group_payload.append(
            {
                "module": module,
                "required_verification": list(required),
                "bug_ids": bug_ids,
                "can_batch": len(bug_ids) > 1,
                "suggested_branch": f"bug/p0-{_slug(module)}-batch-{_today_compact()}" if len(bug_ids) > 1 else f"bug/{bug_ids[0]}-{_slug(module)}-{_today_compact()}",
            }
        )
    return {
        "schema_version": "aistock_issue_workflow_triage_p0_v1",
        "generated_at": _utc_now(),
        "count": len(items),
        "items": sorted(items, key=lambda item: (str(item.get("module")), str(item.get("bug_id")))),
        "groups": group_payload,
    }


def _github_issue_p0_items(*, include_fixed: bool = False) -> tuple[list[dict[str, Any]], list[str]]:
    github_sources, warnings = _scan_github_bug_ids()
    accepted_states = {"OPEN"}
    if include_fixed:
        accepted_states.update({"CLOSED", "MERGED"})
    items: list[dict[str, Any]] = []
    for issue in github_sources:
        if issue.get("kind") != "github_issue":
            continue
        severity = _infer_bug_severity_from_github_issue(issue)
        if severity != "P0":
            continue
        state = str(issue.get("github_state") or "").upper()
        if state not in accepted_states:
            continue
        module = _infer_bug_module_from_github_issue(issue)
        bug_id = str(issue.get("bug_id") or "")
        issue_number = issue.get("github_issue_number")
        issue_url = str(issue.get("source") or _github_issue_url(issue_number))
        items.append(
            {
                "bug_id": bug_id,
                "title": issue.get("title"),
                "status": "open" if state == "OPEN" else "fixed",
                "module": module,
                "github_issue_number": issue_number,
                "github_issue_url": issue_url,
                "missing_github_linkage": [],
                "missing_local_bug_json": True,
                "required_verification": [],
                "allowed_write_scope": [],
                "source_bug_json": None,
                "source_channel": "github",
                "next_command": _adopt_bug_command(
                    bug_id=bug_id,
                    title=str(issue.get("title") or bug_id),
                    module=module,
                    severity=severity,
                    issue_number=issue_number,
                    issue_url=issue_url,
                ),
            }
        )
    return items, warnings


def _dedupe_run_p0_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for item in items:
        bug_id = str(item.get("bug_id") or "").strip().upper()
        if not bug_id:
            continue
        existing = deduped.get(bug_id)
        if existing and existing.get("source_channel") != "github":
            continue
        deduped[bug_id] = item
    return sorted(deduped.values(), key=lambda item: (str(item.get("module")), str(item.get("bug_id"))))


def build_run_p0_plan(
    *,
    module: str | None = None,
    include_fixed: bool = False,
    source: str = "local",
    mode: str = "plan",
) -> dict[str, Any]:
    source = (source or "local").strip().lower()
    mode = (mode or "plan").strip().lower()
    if source not in {"local", "github", "both", "nightly"}:
        raise WorkflowError("--source must be one of: local, github, both, nightly")
    if mode != "plan":
        raise WorkflowError("run-p0 currently supports --mode plan only")
    warnings: list[str] = []
    triage = build_triage_p0(include_fixed=include_fixed)
    local_items = [
        {
            **item,
            "missing_local_bug_json": False,
            "source_channel": "local",
            "next_command": f"python scripts/aistock_issue_workflow.py run --bug-id {item['bug_id']} --mode plan --create-worktree",
        }
        for item in triage["items"]
    ]
    if source == "local":
        items = local_items
    elif source == "github":
        items, github_warnings = _github_issue_p0_items(include_fixed=include_fixed)
        warnings.extend(github_warnings)
    elif source == "both":
        github_items, github_warnings = _github_issue_p0_items(include_fixed=include_fixed)
        warnings.extend(github_warnings)
        items = _dedupe_run_p0_items([*local_items, *github_items])
    else:
        items = []
        warnings.append("nightly source has no separate registry scan yet; use promoted BUG/GitHub records with --source both")
    groups = triage["groups"] if source in {"local", "both"} else []
    if module:
        items = [item for item in items if str(item.get("module") or "") == module]
        groups = [group for group in groups if str(group.get("module") or "") == module]
    recommended_item = items[0] if items else None
    recommended = str(recommended_item.get("bug_id")) if recommended_item else None
    return {
        "schema_version": "aistock_issue_workflow_run_p0_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "planned" if recommended else "no_matching_p0",
        "module": module,
        "source": source,
        "mode": mode,
        "count": len(items),
        "items": items,
        "groups": groups,
        "warnings": warnings,
        "recommended_first_issue": recommended,
        "next_command": str(recommended_item.get("next_command")) if recommended_item else None,
    }


def _records_for_bug_ids(
    bug_ids: list[str],
    *,
    allow_missing_linkage: bool = False,
    allow_closed: bool = False,
) -> list[tuple[dict[str, Any], Path]]:
    normalized = [bug_id.strip().upper() for bug_id in bug_ids if bug_id.strip()]
    if len(normalized) != len(set(normalized)):
        raise WorkflowError(f"duplicate BUG ids in batch: {bug_ids}")
    if len(normalized) < 2:
        raise WorkflowError("batch workflow requires at least two BUG ids")
    records: list[tuple[dict[str, Any], Path]] = []
    for bug_id in normalized:
        record, path = find_bug_record(bug_id=bug_id)
        _require_github_linkage(record, allow_missing=allow_missing_linkage)
        _require_fixable_status(record, allow_closed=allow_closed)
        records.append((record, path))
    return records


def _batch_signature(records: list[dict[str, Any]]) -> dict[str, Any]:
    modules = sorted({str(record.get("module") or "unknown") for record in records})
    if len(modules) != 1:
        raise WorkflowError(f"batch issues must share one module; got {modules}")
    risks = sorted({flow._risk_from_severity(str(record.get("severity") or "P2")) for record in records})
    if len(risks) != 1:
        raise WorkflowError(f"batch issues must share one risk tier; got {risks}")
    verification_signatures = {
        tuple(flow._unique_strings(flow._as_list(record.get("required_verification"))))
        for record in records
    }
    if len(verification_signatures) != 1:
        raise WorkflowError("batch issues must share the same required_verification signature")
    selector = _batch_validation_selector(records, module=modules[0])
    if selector["workflow_gate"] != "compatible":
        raise WorkflowError("; ".join(selector["blocking"]))
    bug_ids = [str(record.get("bug_id")) for record in records]
    batch_id = f"BATCH-{_slug(modules[0], max_len=32)}-{_today_compact()}-{_short_hash(*bug_ids)}"
    return {
        "batch_id": batch_id,
        "module": modules[0],
        "risk_tier": risks[0],
        "bug_ids": bug_ids,
        "required_verification": list(next(iter(verification_signatures))),
        "batch_selector": selector,
    }


_CLOSE_SYNC_PRODUCTION_GATE_KEYS = (
    "production_ddl_gate",
    "production_frontend_dependency_gate",
    "production_backend_dependency_gate",
)


def _close_sync_production_gate_signature(value: dict[str, Any] | None) -> tuple[str, ...]:
    value = value if isinstance(value, dict) else {}
    return tuple(str(value.get(key) or "noop") for key in _CLOSE_SYNC_PRODUCTION_GATE_KEYS)


def _close_sync_batch_compatibility(
    records: list[dict[str, Any]],
    runtime_contracts: dict[str, dict[str, Any]],
    *,
    requested_production_gates: dict[str, Any] | None,
    pr_url: str | None,
) -> dict[str, Any]:
    """Return the fail-closed compatibility contract for close-sync-batch.

    close-sync-batch writes one PR/merge identity into every BUG record. It is
    therefore only safe for records that could have shared one source batch
    PR and one validation/activation policy. Runtime restart receipts are
    checked separately and remain single-issue.
    """
    modules = sorted({str(record.get("module") or "unknown") for record in records})
    risk_tiers = sorted({flow._risk_from_severity(str(record.get("severity") or "P2")) for record in records})
    verification_signatures = sorted(
        {
            tuple(flow._unique_strings(flow._as_list(record.get("required_verification"))))
            for record in records
        }
    )
    runtime_impacts = sorted(
        {
            str(runtime_contracts.get(str(record.get("bug_id")), {}).get("runtime_impact") or "unknown")
            for record in records
        }
    )
    activation_signatures = sorted(
        {
            json.dumps(
                (runtime_contracts.get(str(record.get("bug_id")), {}).get("activation_states") or {}),
                ensure_ascii=False,
                sort_keys=True,
            )
            for record in records
        }
    )
    stored_gate_signatures = sorted(
        {
            _close_sync_production_gate_signature(record)
            for record in records
        }
    )
    requested_gate_signature = _close_sync_production_gate_signature(requested_production_gates)
    source_prs = sorted({str(record.get("pr_url") or "").strip() for record in records if str(record.get("pr_url") or "").strip()})
    blocking: list[str] = []
    if len(modules) != 1:
        blocking.append(f"close-sync-batch issues must share one module; got {modules}")
    if len(risk_tiers) != 1:
        blocking.append(f"close-sync-batch issues must share one risk tier; got {risk_tiers}")
    if len(verification_signatures) != 1:
        blocking.append("close-sync-batch issues must share the same required_verification signature")
    if len(runtime_impacts) != 1:
        blocking.append(f"close-sync-batch issues must share one runtime_impact; got {runtime_impacts}")
    if len(activation_signatures) != 1:
        blocking.append("close-sync-batch issues must share the same activation policy")
    if len(stored_gate_signatures) != 1:
        blocking.append("close-sync-batch issues must share the same production gate state")
    if source_prs and len(source_prs) != 1:
        blocking.append(
            "close-sync-batch issues reference different source PRs; use one shared source batch PR or split the batch"
        )
    if pr_url and source_prs and source_prs[0] != pr_url:
        blocking.append(
            "close-sync-batch --pr-url does not match the existing source PR recorded by every BUG"
        )
    compatibility_fields = {
        "module": modules[0] if len(modules) == 1 else modules,
        "risk_tier": risk_tiers[0] if len(risk_tiers) == 1 else risk_tiers,
        "required_verification": list(verification_signatures[0]) if len(verification_signatures) == 1 else verification_signatures,
        "runtime_impact": runtime_impacts[0] if len(runtime_impacts) == 1 else runtime_impacts,
        "activation_policy": activation_signatures[0] if len(activation_signatures) == 1 else activation_signatures,
        "stored_production_gates": stored_gate_signatures,
        "requested_production_gates": requested_gate_signature,
        "source_prs": source_prs,
        "backend_restart_required": sorted(
            str(record.get("bug_id"))
            for record in records
            if (runtime_contracts.get(str(record.get("bug_id")), {}).get("backend_restart_required"))
        ),
    }
    compatibility_fields["compatibility_key"] = f"close-sync:{_short_hash(compatibility_fields, length=12)}"
    return {
        "schema_version": "aistock_close_sync_batch_compatibility_v1",
        **compatibility_fields,
        "blocking": blocking,
        "workflow_gate": "compatible" if not blocking else "blocked",
    }


def build_start_batch_plan(
    *,
    bug_ids: list[str],
    create_worktree: bool,
    dry_run: bool,
    task_slug: str | None,
    allow_missing_linkage: bool,
    allow_closed: bool,
    record_pairs: list[tuple[dict[str, Any], Path]] | None = None,
) -> dict[str, Any]:
    if record_pairs is None:
        record_pairs = _records_for_bug_ids(
            bug_ids,
            allow_missing_linkage=allow_missing_linkage,
            allow_closed=allow_closed,
        )
    records = [record for record, _ in record_pairs]
    signature = _batch_signature(records)
    batch_plan = flow.build_batch_plan(records)
    batch_plan["batch_id"] = signature["batch_id"]
    batch_plan["batch_selector"] = signature["batch_selector"]
    batch_plan["shared_files"] = signature["batch_selector"]["shared_files"]
    batch_plan["shared_validation"] = signature["batch_selector"]["required_plans"]
    branch, worktree = _batch_target_names(signature["batch_id"], signature["module"], task_slug)
    worktree_plan = _maybe_create_named_worktree(
        branch=branch,
        worktree=worktree,
        create=create_worktree,
        dry_run=dry_run,
    )
    target_root = Path(worktree_plan["worktree"]) if create_worktree and not dry_run else REPO_ROOT
    output_dir = target_root / WORKFLOW_ROOT / signature["batch_id"]
    context_dir = output_dir / "context-packs"
    fix_ready_dir = output_dir / "fix-ready"
    code_intelligence_summary = _build_batch_code_intelligence_summary(
        batch_id=signature["batch_id"],
        records=records,
        changed_files=batch_plan.get("shared_files") or [],
        root=target_root,
    )
    batch_code_intelligence = {
        "provider": code_intelligence_summary.get("provider"),
        "status": code_intelligence_summary.get("status"),
        "context_ref": code_intelligence_summary.get("context_ref"),
        "manifest_ref": code_intelligence_summary.get("manifest_ref"),
        "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
        "fallback_used": code_intelligence_summary.get("fallback_used"),
        "affected_tests_count": code_intelligence_summary.get("affected_tests_count"),
        "affected_quality": code_intelligence_summary.get("affected_quality"),
        "understand_anything_summary_ref": code_intelligence_summary.get("understand_anything_summary_ref"),
        "understand_anything_summary": code_intelligence_summary.get("understand_anything_summary"),
        "understand_anything": code_intelligence_summary.get("understand_anything"),
    }
    batch_plan["code_intelligence"] = batch_code_intelligence
    context_metrics: dict[str, Any] = {}
    if not dry_run:
        for record, source_path in record_pairs:
            bug_id = str(record.get("bug_id"))
            context_pack = flow.build_context_pack(record, [])
            fix_ready = flow.build_fix_ready(record, [])
            context_pack["code_intelligence"] = batch_code_intelligence
            fix_ready["code_intelligence"] = batch_code_intelligence
            _write_json(context_dir / f"{bug_id}.json", context_pack)
            _write_text(context_dir / f"{bug_id}.md", flow.render_context_pack_markdown(context_pack))
            _write_json(fix_ready_dir / f"{bug_id}.json", fix_ready)
            context_metrics[bug_id] = {
                "context_md": _size_and_token_estimate(context_dir / f"{bug_id}.md"),
                "context_json": _size_and_token_estimate(context_dir / f"{bug_id}.json"),
                "fix_ready_json": _size_and_token_estimate(fix_ready_dir / f"{bug_id}.json"),
            }
            target_bug_path = _issue_json_path_for_worktree(source_path, target_root)
            batch_plan.setdefault("source_bug_json", {})[bug_id] = _repo_rel(source_path)
            batch_plan.setdefault("target_bug_json", {})[bug_id] = _repo_rel(target_bug_path, target_root)
        _write_json(output_dir / "batch-plan.json", batch_plan)
        _write_json(output_dir / "batch-state.json", {
            **signature,
            "state": "context_ready",
            "branch": worktree_plan.get("branch"),
            "worktree": worktree_plan.get("worktree"),
            "batch_plan_path": _repo_rel(output_dir / "batch-plan.json", target_root),
            "context_dir": _repo_rel(context_dir, target_root),
            "fix_ready_dir": _repo_rel(fix_ready_dir, target_root),
            "context_metrics": context_metrics,
            "batch_selector": signature["batch_selector"],
            "updated_at": _utc_now(),
        })
        _write_state(
            signature["batch_id"],
            state="context_ready",
            root=target_root,
            branch=worktree_plan.get("branch"),
            worktree=worktree_plan.get("worktree"),
            base=worktree_plan.get("base"),
            batch_id=signature["batch_id"],
            bug_ids=signature["bug_ids"],
            module=signature["module"],
            risk_tier=signature["risk_tier"],
            batch_plan_path=_repo_rel(output_dir / "batch-plan.json", target_root),
            batch_state_path=_repo_rel(output_dir / "batch-state.json", target_root),
            context_dir=_repo_rel(context_dir, target_root),
            fix_ready_dir=_repo_rel(fix_ready_dir, target_root),
            code_intelligence=batch_code_intelligence,
            batch_selector=signature["batch_selector"],
            context_metrics=context_metrics,
            next_actions=[
                "switch_to_batch_worktree_if_created",
                "read_each_context_pack",
                "fix_only_within_shared_scope_or_stop_for_scope_expansion",
                "run_finish_batch_before_reporting_done",
            ],
        )
    return {
        "schema_version": "aistock_issue_workflow_start_batch_v1",
        "generated_at": _utc_now(),
        **signature,
        "worktree_plan": worktree_plan,
        "batch_plan": batch_plan,
        "batch_plan_path": _repo_rel(output_dir / "batch-plan.json", target_root),
        "batch_state_path": _repo_rel(output_dir / "batch-state.json", target_root),
        "context_dir": _repo_rel(context_dir, target_root),
        "fix_ready_dir": _repo_rel(fix_ready_dir, target_root),
        "state_path": _repo_rel(_state_path(signature["batch_id"], target_root), target_root),
        "events_path": _repo_rel(_events_path(signature["batch_id"], target_root), target_root),
        "code_intelligence": batch_code_intelligence,
        "batch_selector": signature["batch_selector"],
        "context_metrics": context_metrics,
        "workflow_gate": "ready_for_batch_fix",
        "next_command": f"python scripts/aistock_issue_workflow.py finish-batch --batch-id {signature['batch_id']} --plan-only",
    }


def _load_batch_state(batch_id: str) -> dict[str, Any] | None:
    path = REPO_ROOT / WORKFLOW_ROOT / batch_id / "batch-state.json"
    if path.exists():
        return _load_json(path)
    state = _load_state(batch_id)
    return state


def _parse_issue_commit_map(entries: list[str] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for entry in entries or []:
        if "=" not in entry:
            raise WorkflowError(f"--issue-commit must use BUG-XXX=<commit>: {entry}")
        issue, commit = entry.split("=", 1)
        issue = issue.strip().upper()
        commit = commit.strip()
        if not issue or not commit:
            raise WorkflowError(f"--issue-commit must use BUG-XXX=<commit>: {entry}")
        result[issue] = commit
    return result


def render_batch_pr_body(
    batch_id: str,
    records: list[dict[str, Any]],
    changed_files: list[str],
    validation: dict[str, Any],
    evidence: list[str],
    commit_map: dict[str, str],
    code_intelligence_summary: dict[str, Any],
    closure_ready: bool,
    runtime_contracts: dict[str, dict[str, Any]] | None = None,
) -> str:
    gates = validation.get("production_gates") or {}
    codegraph_tests = code_intelligence_summary.get("affected_tests", {}).get("suggested_tests") or []
    lines = [
        f"## {batch_id} batch issue workflow summary",
        "",
        f"- Module: `{records[0].get('module') if records else 'unknown'}`",
        f"- Closure ready: `{str(closure_ready).lower()}`",
        "",
        "## Issues",
    ]
    for record in records:
        bug_id = str(record.get("bug_id"))
        issue = record.get("github_issue_number")
        url = record.get("github_issue_url") or "missing"
        commit = commit_map.get(bug_id, "shared PR")
        lines.append(f"- `{bug_id}` / #{issue}: {url} / commit: `{commit}`")
    lines.extend([
        "",
        "## Changed files",
        *[f"- `{path}`" for path in changed_files or ["none"]],
        "",
        "## Required validation",
        *[f"- `{plan}`" for plan in validation.get("required_plans") or ["l0"]],
        "",
        "## Code intelligence",
        f"- context_ref: `{code_intelligence_summary.get('context_ref') or 'not_generated'}`",
        f"- affected_tests_ref: `{code_intelligence_summary.get('affected_tests_ref') or 'not_generated'}`",
        f"- fallback_used: `{str(bool(code_intelligence_summary.get('fallback_used'))).lower()}`",
        *_codegraph_test_lines(codegraph_tests),
        "",
        "## Evidence",
        *[f"- {item}" for item in evidence or ["missing - run required validation before requesting merge"]],
        "",
        "## Production gates",
        f"- production_ddl_gate: `{gates.get('ddl', 'noop')}`",
        f"- production_frontend_dependency_gate: `{gates.get('frontend_dependency', 'noop')}`",
        f"- production_backend_dependency_gate: `{gates.get('backend_dependency', 'noop')}`",
        "",
        "## Per-issue closure map",
    ])
    for record in records:
        bug_id = str(record.get("bug_id"))
        requirements = flow._unique_strings(flow._as_list(record.get("closure_requirements"))) or ["Fix issue-specific behavior."]
        lines.append(f"- `{bug_id}`")
        lines.extend(f"  - {item}" for item in requirements)
    runtime_contracts = runtime_contracts or {}
    closing = [
        (
            f"Refs #{record.get('github_issue_number')}"
            if (runtime_contracts.get(str(record.get("bug_id"))) or {}).get("backend_restart_required")
            else f"Closes #{record.get('github_issue_number')}"
        )
        for record in records
        if record.get("github_issue_number")
    ]
    if closing:
        lines.extend(["", *closing])
    return "\n".join(lines)


def build_finish_batch_plan(
    *,
    batch_id: str | None,
    bug_ids: list[str],
    changed_files: list[str] | None,
    base: str,
    head: str,
    validation_evidence: list[str],
    issue_commit: list[str] | None,
    plan_only: bool,
    allow_missing_evidence: bool,
    record_pairs: list[tuple[dict[str, Any], Path]] | None = None,
) -> dict[str, Any]:
    if not batch_id and not bug_ids and record_pairs is None:
        raise WorkflowError("finish-batch requires --batch-id or at least two --bug-id values")
    if batch_id:
        state = _load_batch_state(batch_id)
        if state and not bug_ids:
            bug_ids = [str(item) for item in state.get("bug_ids") or []]
    if record_pairs is None:
        record_pairs = _records_for_bug_ids(bug_ids, allow_missing_linkage=False, allow_closed=True)
    records = [record for record, _ in record_pairs]
    signature = _batch_signature(records)
    if batch_id and batch_id != signature["batch_id"]:
        signature["batch_id"] = batch_id
    canonical_batch_id = signature["batch_id"]
    changed = changed_files if changed_files is not None else flow.changed_files_from_git(base, head)
    validation = flow.select_validation(changed, module=signature["module"])
    batch_selector = _batch_validation_selector(records, module=signature["module"], changed_files=changed)
    scope_check = _batch_scope_check(changed, batch_selector["shared_files"])
    selector_blocking = list(batch_selector.get("blocking") or [])
    if scope_check["status"] == "failed":
        selector_blocking.append(f"batch changed files exceed shared scope: {scope_check['violations']}")
    code_intelligence_summary = _build_batch_code_intelligence_summary(
        batch_id=canonical_batch_id,
        records=records,
        changed_files=changed,
        root=REPO_ROOT,
    )
    codegraph_tests = code_intelligence_summary.get("affected_tests", {}).get("suggested_tests") or []
    if codegraph_tests:
        validation["codegraph_suggested_tests"] = codegraph_tests
    raw_evidence = [item for item in validation_evidence if item.strip()]
    validation_receipts, validation_evidence_errors = _build_validation_receipts(raw_evidence, root=REPO_ROOT)
    validation_receipt_plan_coverage = _validation_receipt_plan_coverage(
        validation=validation,
        receipts=validation_receipts,
    )
    if validation_receipts:
        validation_evidence_errors.extend(_validation_receipt_plan_errors(validation_receipt_plan_coverage))
    evidence = [_render_validation_receipt(receipt) for receipt in validation_receipts]
    runtime_contracts = {
        str(record.get("bug_id")): build_runtime_contract(
            record=record,
            changed_files=changed,
            root=REPO_ROOT,
            fresh_process_evidence=flow._as_list((record.get("runtime_contract") or {}).get("fresh_process_evidence"))
            if isinstance(record.get("runtime_contract"), dict)
            else [],
        )
        for record in records
    }
    runtime_batch_bug_ids = sorted(
        bug_id
        for bug_id, contract in runtime_contracts.items()
        if contract.get("backend_restart_required")
    )
    if runtime_batch_bug_ids:
        selector_blocking.append(
            "runtime BUGs require the single-issue finish and post-restart receipt workflow: "
            + ", ".join(runtime_batch_bug_ids)
        )
    closure_ready = (
        bool(evidence)
        and not selector_blocking
        and not validation_evidence_errors
    )
    draft_ready = (
        (plan_only or (allow_missing_evidence and not evidence))
        and not selector_blocking
        and not validation_evidence_errors
    )
    commit_map = _parse_issue_commit_map(issue_commit)
    output_dir = REPO_ROOT / WORKFLOW_ROOT / canonical_batch_id
    pr_body_path = output_dir / "pr-body.md"
    finish_plan = {
        "schema_version": "aistock_issue_workflow_finish_batch_v1",
        "generated_at": _utc_now(),
        **signature,
        "batch_id": canonical_batch_id,
        "changed_files": changed,
        "selected_validation": validation,
        "batch_selector": batch_selector,
        "scope_check": scope_check,
        "validation_evidence": evidence,
        "validation_receipts": validation_receipts,
        "validation_evidence_errors": validation_evidence_errors,
        "validation_receipt_plan_coverage": validation_receipt_plan_coverage,
        "per_issue_commit_map": commit_map,
        "per_issue_closure_map": {
            str(record.get("bug_id")): flow._unique_strings(flow._as_list(record.get("closure_requirements")))
            for record in records
        },
        "code_intelligence": code_intelligence_summary,
        "closure_ready": closure_ready,
        "draft_ready": draft_ready,
        "production_gates": validation.get("production_gates") or {},
        "runtime_contracts": runtime_contracts,
        "blocking": selector_blocking,
    }
    _write_json(output_dir / "finish-plan.json", finish_plan)
    if evidence:
        _write_json(output_dir / "validation-evidence.json", {"batch_id": canonical_batch_id, "items": evidence})
    _write_text(
        pr_body_path,
        render_batch_pr_body(
            canonical_batch_id,
            records,
            changed,
            validation,
            evidence,
            commit_map,
            code_intelligence_summary,
            closure_ready,
            runtime_contracts,
        ),
    )
    next_state = "validation_passed" if closure_ready else ("validation_planned" if draft_ready else "blocked")
    _write_state(
        canonical_batch_id,
        state=next_state,
        batch_id=canonical_batch_id,
        bug_ids=signature["bug_ids"],
        changed_files=changed,
        validation_evidence=evidence,
        pr_body_path=_repo_rel(pr_body_path),
        production_gates=validation.get("production_gates") or {},
        code_intelligence={
            "status": code_intelligence_summary.get("status"),
            "context_ref": code_intelligence_summary.get("context_ref"),
            "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
            "fallback_used": code_intelligence_summary.get("fallback_used"),
        },
        stop_reason=None
        if closure_ready
        else (
            "; ".join(selector_blocking + validation_evidence_errors)
            if selector_blocking or validation_evidence_errors
            else "validation_evidence_missing"
        ),
        next_actions=[
            "commit_only_batch_files",
            "push_task_branch",
            "create_pr_from_batch_pr_body",
            "watch_ci_before_merge",
        ] if closure_ready else ["run_required_validation", "rerun_finish_batch_with_validation_evidence"],
    )
    payload = {
        **finish_plan,
        "workflow_gate": "ready_for_pr"
        if closure_ready
        else (
            "blocked"
            if selector_blocking or validation_evidence_errors
            else (
                "plan_ready"
                if plan_only
                else (
                    "validation_evidence_missing_allowed"
                    if allow_missing_evidence
                    else "validation_evidence_missing"
                )
            )
        ),
        "required_verification": validation.get("required_plans") or [],
        "recommended_verification": validation.get("recommended_plans") or [],
        "code_intelligence": {
            "status": code_intelligence_summary.get("status"),
            "context_ref": code_intelligence_summary.get("context_ref"),
            "manifest_ref": code_intelligence_summary.get("manifest_ref"),
            "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
            "fallback_used": code_intelligence_summary.get("fallback_used"),
        },
        "codegraph_suggested_tests": codegraph_tests,
        "pr_body_path": _repo_rel(pr_body_path),
        "state_path": _repo_rel(_state_path(canonical_batch_id)),
        "events_path": _repo_rel(_events_path(canonical_batch_id)),
    }
    if not closure_ready:
        blocking_reasons = selector_blocking + validation_evidence_errors
        if blocking_reasons:
            payload["error"] = "; ".join(blocking_reasons)
        elif plan_only:
            payload["error"] = "plan-only draft generated; complete required validation evidence before PR readiness"
        elif allow_missing_evidence:
            payload["error"] = "missing-evidence draft generated; complete required validation evidence before PR readiness"
        else:
            payload["error"] = "validation evidence is required"
    return payload


def _path_check(path: Path, label: str, *, blocking: bool = True) -> dict[str, Any]:
    return {
        "label": label,
        "path": str(path),
        "exists": path.exists(),
        "blocking": blocking,
    }


def _codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex")


def _claude_home() -> Path:
    return Path(os.environ.get("CLAUDE_HOME") or Path.home() / ".claude")


def _mcp_config_snapshot() -> dict[str, Any]:
    candidates = [
        _codex_home() / "config.toml",
        _codex_home() / "config.json",
        REPO_ROOT / ".mcp.json",
    ]
    files = []
    stale_paths: list[str] = []
    for path in candidates:
        item = {"path": str(path), "exists": path.exists(), "mentions_aistock_root": False, "mentions_worktree": False}
        if path.exists():
            text = path.read_text(encoding="utf-8", errors="replace")
            normalized = text.replace("\\", "/").lower()
            item["mentions_aistock_root"] = "f:/dev/aistock" in normalized
            item["mentions_worktree"] = "aistock_worktrees" in normalized
            if item["mentions_worktree"]:
                stale_paths.append(str(path))
        files.append(item)
    return {"files": files, "stale_worktree_config_files": stale_paths}


CLIENT_CODEX_SKILLS: tuple[tuple[str, str], ...] = (
    ("issue", "fix-aistock-issue"),
    ("feature", "verify-aistock-feature"),
    ("router", "aistock-task-router"),
    ("docs_handoff", "aistock-docs-handoff"),
    ("merge_aftercare", "aistock-merge-aftercare"),
    ("readonly_triage", "aistock-readonly-triage"),
    ("validation_delegation", "aistock-validation-delegation"),
)

CLIENT_CLAUDE_COMMANDS: tuple[tuple[str, str], ...] = (
    ("issue", "fix-aistock-issue.md"),
    ("feature", "aistock-feature-workflow.md"),
    ("router", "aistock-task-router.md"),
    ("docs_handoff", "aistock-docs-handoff.md"),
    ("merge_aftercare", "aistock-merge-aftercare.md"),
    ("readonly_triage", "aistock-readonly-triage.md"),
    ("validation_delegation", "aistock-validation-delegation.md"),
)

CLIENT_CLAUDE_STARTUP_ROUTER_KEY = "startup_router"
CLIENT_CLAUDE_STARTUP_ROUTER_SOURCE = Path(".claude/commands/aistock-global-router.md")
CLIENT_CLAUDE_STARTUP_ROUTER_TARGET = "CLAUDE.md"
CLIENT_CLAUDE_STARTUP_ROUTER_BEGIN = "<!-- BEGIN AISTOCK MANAGED ROUTER -->"
CLIENT_CLAUDE_STARTUP_ROUTER_END = "<!-- END AISTOCK MANAGED ROUTER -->"

CLIENT_LANE_CHOICES: tuple[str, ...] = tuple(key for key, _name in CLIENT_CODEX_SKILLS)


def _selected_client_lane_keys(selected_lane: str | None) -> set[str]:
    if selected_lane is None:
        return set(CLIENT_LANE_CHOICES)
    normalized = selected_lane.strip().lower()
    if normalized not in CLIENT_LANE_CHOICES:
        raise WorkflowError(
            f"unsupported client lane {selected_lane!r}; expected one of {', '.join(CLIENT_LANE_CHOICES)}"
        )
    return {"router", normalized}


def _client_lanes_for_changed_files(changed_files: Iterable[str]) -> list[str]:
    normalized = {
        str(path).replace("\\", "/").removeprefix("./").strip("/")
        for path in changed_files
        if str(path).strip()
    }
    lanes: set[str] = set()
    for key, skill_name in CLIENT_CODEX_SKILLS:
        prefix = f".codex/skills/{skill_name}/"
        if any(path == prefix.rstrip("/") or path.startswith(prefix) for path in normalized):
            lanes.add(key)
    for key, command_name in CLIENT_CLAUDE_COMMANDS:
        path = f".claude/commands/{command_name}"
        if path in normalized:
            lanes.add(key)
    if CLIENT_CLAUDE_STARTUP_ROUTER_SOURCE.as_posix() in normalized:
        lanes.add("router")
    return sorted(lanes)


def _stale_client_lanes(manifest: dict[str, Any]) -> list[str]:
    stale_statuses = {"stale", "stale_global", "missing_global"}
    lanes: set[str] = set()
    for entries_key in ("codex_entries", "claude_entries"):
        entries = manifest.get(entries_key) if isinstance(manifest.get(entries_key), dict) else {}
        for lane, entry in entries.items():
            status = str((entry or {}).get("status") or "")
            if status in stale_statuses or status.startswith("stale"):
                lanes.add("router" if lane == CLIENT_CLAUDE_STARTUP_ROUTER_KEY else str(lane))
    return sorted(lanes)


def _merge_commit_changed_files(merge_commit: str, *, root: Path) -> dict[str, Any]:
    parent_result = _run_command(
        ["git", "rev-list", "--parents", "-n", "1", merge_commit],
        cwd=root,
        timeout=30,
    )
    if not parent_result.get("ok"):
        return {"ok": False, "files": [], "error": parent_result.get("stderr") or "merge commit unavailable"}
    parts = str(parent_result.get("stdout") or "").split()
    if len(parts) < 2:
        return {"ok": False, "files": [], "error": f"merge commit has no parent: {merge_commit}"}
    diff_result = _run_command(
        ["git", "diff", "--name-only", parts[1], merge_commit],
        cwd=root,
        timeout=30,
    )
    return {
        "ok": bool(diff_result.get("ok")),
        "base": parts[1],
        "merge_commit": merge_commit,
        "files": sorted(set(str(diff_result.get("stdout") or "").splitlines())),
        "error": None if diff_result.get("ok") else diff_result.get("stderr") or "merge diff unavailable",
    }


def _publish_changed_clients_after_merge(
    *,
    merge_commit: str,
    sync_root: bool,
    apply: bool,
) -> dict[str, Any]:
    root = _canonical_root()
    payload: dict[str, Any] = {
        "schema_version": "aistock_merge_aftercare_client_publish_v1",
        "merge_commit": merge_commit,
        "canonical_root": str(root),
        "sync_root": sync_root,
        "dry_run": not apply,
        "changed_files": [],
        "selected_lanes": [],
        "installs": [],
        "verifications": [],
        "blocking": [],
    }
    if not sync_root:
        payload.update({"workflow_gate": "deferred", "reason": "canonical_root_sync_not_requested"})
        return payload
    if not apply:
        payload.update({"workflow_gate": "ready_for_apply", "reason": "inspect_after_source_merge"})
        return payload

    fetch = _cleanup_preflight_fetch_origin(root, apply=True)
    payload["fetch"] = fetch
    if fetch.get("status") != "fetched":
        result = fetch.get("result") if isinstance(fetch.get("result"), dict) else {}
        payload["blocking"].append(result.get("stderr") or result.get("stdout") or "failed to fetch origin")
    root_git = _git_snapshot(root)
    payload["root_before"] = root_git
    if root_git.get("branch") != "main":
        payload["blocking"].append(f"canonical root is not on main: {root_git.get('branch')}")
    if root_git.get("dirty") and root_git.get("head") != root_git.get("origin_main"):
        payload["blocking"].append("canonical root is dirty and behind origin/main")
    if payload["blocking"]:
        payload["workflow_gate"] = "blocked"
        return payload
    if root_git.get("head") != root_git.get("origin_main"):
        sync_result = _run_command(["git", "merge", "--ff-only", "origin/main"], cwd=root, timeout=120)
        payload["root_sync"] = sync_result
        if not sync_result.get("ok"):
            payload["blocking"].append(sync_result.get("stderr") or "canonical root fast-forward failed")
            payload["workflow_gate"] = "blocked"
            return payload

    root_after = _git_snapshot(root)
    payload["root_after"] = root_after
    contains_merge = _run_command(
        ["git", "merge-base", "--is-ancestor", merge_commit, "HEAD"],
        cwd=root,
        timeout=30,
    )
    payload["merge_commit_containment"] = {
        "ok": bool(contains_merge.get("ok")),
        "merge_commit": merge_commit,
        "canonical_head": root_after.get("head"),
    }
    if not contains_merge.get("ok"):
        payload["blocking"].append(
            "canonical main does not contain the verified source merge commit after fast-forward"
        )
        payload["workflow_gate"] = "blocked"
        return payload

    changed = _merge_commit_changed_files(merge_commit, root=root)
    payload["merge_diff"] = changed
    payload["changed_files"] = changed.get("files") or []
    if not changed.get("ok"):
        payload["blocking"].append(str(changed.get("error") or "merge changed-file inspection failed"))
        payload["workflow_gate"] = "blocked"
        return payload
    changed_lanes = _client_lanes_for_changed_files(payload["changed_files"])
    payload["changed_lanes"] = changed_lanes
    if not changed_lanes:
        payload.update({"workflow_gate": "not_required", "reason": "merge_did_not_change_workflow_clients"})
        return payload
    with _ClientInstallLock():
        preinstall_manifest = _client_manifest()
    stale_lanes = _stale_client_lanes(preinstall_manifest)
    lanes = sorted(set(changed_lanes) | set(stale_lanes))
    payload["stale_lanes_before"] = stale_lanes
    payload["selected_lanes"] = lanes

    for lane in lanes:
        install = build_client_install_plan(apply=True, selected_lane=lane)
        payload["installs"].append({"lane": lane, **install})
        if install.get("workflow_gate") != "installed":
            payload["blocking"].extend(install.get("blocking") or [f"client install failed for lane {lane}"])
            continue
        with _ClientInstallLock():
            manifest = _client_manifest()
        verification = _client_lane_verification(
            manifest,
            selected_lane=lane,
            verify_codex=True,
            verify_claude=True,
        )
        payload["verifications"].append({"lane": lane, **verification})
        payload["blocking"].extend(verification.get("blocking") or [])
    payload["workflow_gate"] = "blocked" if payload["blocking"] else "installed_and_verified"
    payload["restart_recommended"] = False
    return payload


def _client_source_authority() -> dict[str, Any]:
    canonical_root = _canonical_root()
    canonical_git = _git_snapshot(canonical_root)
    head_result = _run_command(["git", "rev-parse", "--verify", "HEAD^{commit}"], cwd=canonical_root, timeout=15)
    head = str(head_result.get("stdout") or "").strip().lower()
    origin_main = _origin_main_commit(root=canonical_root)
    authority_status = _run_command(
        [
            "git",
            "status",
            "--porcelain=v1",
            "--",
            ".codex/skills",
            ".claude/commands",
            str(CLIENT_CLAUDE_STARTUP_ROUTER_SOURCE),
            "scripts/aistock_issue_workflow.py",
        ],
        cwd=canonical_root,
        timeout=15,
    )
    authority_paths_dirty = bool(str(authority_status.get("stdout") or "").strip())
    if (
        canonical_git.get("ok")
        and canonical_git.get("branch") == "main"
        and authority_status.get("ok")
        and not authority_paths_dirty
        and head
        and origin_main
        and head == origin_main
    ):
        return {
            "ready": True,
            "source": "canonical_main",
            "root": str(canonical_root),
            "commit": head,
            "origin_main_commit": origin_main,
            "authority_paths_clean": True,
            "blocking_reason": None,
            "blocking_reasons": [],
        }

    reasons: list[str] = []
    if not canonical_git.get("ok"):
        reasons.append("canonical root is not a readable Git checkout")
    if canonical_git.get("branch") != "main":
        reasons.append(f"canonical root branch is {canonical_git.get('branch') or 'unknown'}, expected main")
    if not authority_status.get("ok"):
        reasons.append("canonical client-authority path status is unavailable")
    elif authority_paths_dirty:
        reasons.append("canonical client-authority paths are dirty")
    if not origin_main:
        reasons.append("origin/main identity is unavailable")
    if head and origin_main and head != origin_main:
        reasons.append("canonical main is not aligned with origin/main")
    return {
        "ready": False,
        "source": "canonical_main",
        "root": str(canonical_root),
        "commit": head or None,
        "origin_main_commit": origin_main,
        "authority_paths_clean": bool(authority_status.get("ok")) and not authority_paths_dirty,
        "blocking_reason": reasons[0] if reasons else "canonical client authority is unavailable",
        "blocking_reasons": reasons,
    }


def _client_checkout_root() -> Path:
    result = _run_command(["git", "rev-parse", "--show-toplevel"], cwd=Path.cwd(), timeout=15)
    value = str(result.get("stdout") or "").strip()
    return Path(value) if result.get("ok") and value else REPO_ROOT


def _client_checkout_relation(authority: dict[str, Any]) -> str:
    checkout_root = _client_checkout_root()
    authority_commit = str(authority.get("commit") or "").strip()
    head_result = _run_command(["git", "rev-parse", "--verify", "HEAD^{commit}"], cwd=checkout_root, timeout=15)
    checkout_commit = str(head_result.get("stdout") or "").strip().lower()
    if not authority_commit or not checkout_commit:
        return "authority_checkout" if _same_path(Path(authority["root"]), checkout_root) else "unknown"
    if checkout_commit == authority_commit:
        return "matches_authority"
    if _git_commit_is_ancestor(checkout_commit, authority_commit, root=checkout_root):
        return "behind_authority"
    if _git_commit_is_ancestor(authority_commit, checkout_commit, root=checkout_root):
        return "ahead_of_authority"
    return "divergent_from_authority"


class _ClientInstallLock:
    def __init__(self, *, timeout: float = 30.0) -> None:
        self.timeout = timeout
        self.path = Path(
            os.environ.get("AISTOCK_CLIENT_INSTALL_LOCK_PATH")
            or (_default_worktree_root() / ".locks" / "client-install.lock")
        )
        self._fd: int | None = None

    def __enter__(self) -> "_ClientInstallLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self._fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(self._fd, f"{os.getpid()}\n{_utc_now()}\n".encode("ascii"))
                return self
            except FileExistsError as exc:
                if time.monotonic() >= deadline:
                    raise WorkflowError(f"timed out waiting for client install lock: {self.path}") from exc
                time.sleep(0.1)

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        if self._fd is not None:
            os.close(self._fd)
        with contextlib.suppress(FileNotFoundError):
            self.path.unlink()


def _staged_replace_tree(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{target.name}.stage-", dir=target.parent))
    backup = target.parent / f".{target.name}.backup-{os.getpid()}-{time.time_ns()}"
    try:
        shutil.copytree(source, stage, dirs_exist_ok=True)
        if target.exists():
            os.replace(target, backup)
        os.replace(stage, target)
    except Exception:
        if backup.exists() and not target.exists():
            os.replace(backup, target)
        raise
    finally:
        if stage.exists():
            shutil.rmtree(stage)
        if backup.exists():
            shutil.rmtree(backup)


def _staged_replace_file(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, stage_name = tempfile.mkstemp(prefix=f".{target.name}.stage-", dir=target.parent)
    os.close(fd)
    stage = Path(stage_name)
    try:
        shutil.copy2(source, stage)
        os.replace(stage, target)
    finally:
        with contextlib.suppress(FileNotFoundError):
            stage.unlink()


def _claude_startup_router_block(
    path: Path,
    *,
    require_only_block: bool = False,
) -> tuple[str | None, str | None]:
    if not path.exists():
        return None, "missing"
    if not path.is_file():
        return None, "not_a_file"
    text = path.read_text(encoding="utf-8")
    begin_count = text.count(CLIENT_CLAUDE_STARTUP_ROUTER_BEGIN)
    end_count = text.count(CLIENT_CLAUDE_STARTUP_ROUTER_END)
    if begin_count == 0 and end_count == 0:
        return None, "missing"
    if begin_count != 1 or end_count != 1:
        return None, "malformed_markers"
    begin = text.index(CLIENT_CLAUDE_STARTUP_ROUTER_BEGIN)
    end = text.find(CLIENT_CLAUDE_STARTUP_ROUTER_END, begin)
    if end < 0:
        return None, "malformed_marker_order"
    end += len(CLIENT_CLAUDE_STARTUP_ROUTER_END)
    if require_only_block and (text[:begin].strip() or text[end:].strip()):
        return None, "authority_contains_unmanaged_text"
    return text[begin:end].strip(), None


def _merge_claude_startup_router_block(current: str, managed_block: str) -> str:
    begin_count = current.count(CLIENT_CLAUDE_STARTUP_ROUTER_BEGIN)
    end_count = current.count(CLIENT_CLAUDE_STARTUP_ROUTER_END)
    if begin_count == 0 and end_count == 0:
        prefix = current.rstrip()
        return f"{prefix}\n\n{managed_block}\n" if prefix else f"{managed_block}\n"
    if begin_count != 1 or end_count != 1:
        raise WorkflowError("Claude startup memory has malformed AIstock managed-router markers")
    begin = current.index(CLIENT_CLAUDE_STARTUP_ROUTER_BEGIN)
    end = current.find(CLIENT_CLAUDE_STARTUP_ROUTER_END, begin)
    if end < 0:
        raise WorkflowError("Claude startup memory has reversed AIstock managed-router markers")
    end += len(CLIENT_CLAUDE_STARTUP_ROUTER_END)
    return f"{current[:begin]}{managed_block}{current[end:]}"


def _staged_replace_text(target: Path, text: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, stage_name = tempfile.mkstemp(prefix=f".{target.name}.stage-", dir=target.parent)
    stage = Path(stage_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(stage, target)
    finally:
        with contextlib.suppress(OSError):
            os.close(fd)
        with contextlib.suppress(FileNotFoundError):
            stage.unlink()


def _client_manifest(codex_home: Path | None = None, claude_home: Path | None = None) -> dict[str, Any]:
    codex_home = codex_home or _codex_home()
    claude_home = claude_home or _claude_home()
    checkout_root = _client_checkout_root()
    cli = REPO_ROOT / "scripts" / "aistock_issue_workflow.py"
    checkout_cli = checkout_root / "scripts" / "aistock_issue_workflow.py"
    repo_head = _run_command(["git", "rev-parse", "HEAD"], cwd=checkout_root, timeout=15)
    cli_sha = _sha256_file(cli)
    authority = _client_source_authority()
    authority_root = Path(authority["root"])
    authority_cli = authority_root / "scripts" / "aistock_issue_workflow.py"
    checkout_relation = _client_checkout_relation(authority)

    def _tree_status(authority_sha: str | None, global_sha: str | None) -> str:
        if not authority.get("ready"):
            return "authority_unavailable"
        if authority_sha and global_sha:
            return "current" if authority_sha == global_sha else "stale"
        if authority_sha and not global_sha:
            return "missing_global"
        return "missing_authority"

    def _file_status(authority_sha: str | None, global_sha: str | None) -> str:
        if not authority.get("ready"):
            return "authority_unavailable"
        if authority_sha and global_sha:
            return "current" if authority_sha == global_sha else "stale_global"
        if authority_sha and not global_sha:
            return "missing_global"
        return "missing_authority"

    def _combined_status(statuses: Iterable[str], *, stale_status: str) -> str:
        values = list(statuses)
        if any(value == "authority_unavailable" for value in values):
            return "authority_unavailable"
        if any(value == "missing_authority" for value in values):
            return "missing_authority"
        if any(value.startswith("stale") for value in values):
            return stale_status
        if any(value == "missing_global" for value in values):
            return "missing_global"
        return "current"

    codex_entries: dict[str, dict[str, Any]] = {}
    for key, skill_name in CLIENT_CODEX_SKILLS:
        repo_path = checkout_root / ".codex" / "skills" / skill_name
        authority_path = authority_root / ".codex" / "skills" / skill_name
        global_path = codex_home / "skills" / skill_name
        repo_sha = _sha256_tree(repo_path)
        authority_sha = _sha256_tree(authority_path) if authority.get("ready") else None
        global_sha = _sha256_tree(global_path)
        codex_entries[key] = {
            "name": skill_name,
            "repo_path": str(repo_path),
            "authority_path": str(authority_path),
            "global_path": str(global_path),
            "repo_sha256": repo_sha,
            "authority_sha256": authority_sha,
            "global_sha256": global_sha,
            "checkout_status": "matches_authority" if repo_sha == authority_sha else "differs_from_authority",
            "status": _tree_status(authority_sha, global_sha),
        }

    claude_entries: dict[str, dict[str, Any]] = {}
    for key, command_name in CLIENT_CLAUDE_COMMANDS:
        repo_path = checkout_root / ".claude" / "commands" / command_name
        authority_path = authority_root / ".claude" / "commands" / command_name
        global_path = claude_home / "commands" / command_name
        repo_sha = _sha256_file(repo_path)
        authority_sha = _sha256_file(authority_path) if authority.get("ready") else None
        global_sha = _sha256_file(global_path)
        claude_entries[key] = {
            "name": command_name,
            "repo_path": str(repo_path),
            "authority_path": str(authority_path),
            "global_path": str(global_path),
            "repo_sha256": repo_sha,
            "authority_sha256": authority_sha,
            "global_sha256": global_sha,
            "checkout_status": "matches_authority" if repo_sha == authority_sha else "differs_from_authority",
            "status": _file_status(authority_sha, global_sha),
        }

    startup_repo_path = checkout_root / CLIENT_CLAUDE_STARTUP_ROUTER_SOURCE
    startup_authority_path = authority_root / CLIENT_CLAUDE_STARTUP_ROUTER_SOURCE
    startup_global_path = claude_home / CLIENT_CLAUDE_STARTUP_ROUTER_TARGET
    startup_repo_block, startup_repo_error = _claude_startup_router_block(
        startup_repo_path,
        require_only_block=True,
    )
    if authority.get("ready"):
        startup_authority_block, startup_authority_error = _claude_startup_router_block(
            startup_authority_path,
            require_only_block=True,
        )
    else:
        startup_authority_block, startup_authority_error = None, "authority_unavailable"
    startup_global_block, startup_global_error = _claude_startup_router_block(startup_global_path)
    startup_repo_sha = _sha256_text(startup_repo_block) if startup_repo_block is not None else None
    startup_authority_sha = (
        _sha256_text(startup_authority_block) if startup_authority_block is not None else None
    )
    startup_global_sha = _sha256_text(startup_global_block) if startup_global_block is not None else None
    if not authority.get("ready"):
        startup_status = "authority_unavailable"
    elif startup_authority_error:
        startup_status = "missing_authority"
    elif startup_global_error == "missing":
        startup_status = "missing_global"
    elif startup_global_error:
        startup_status = "stale_global_malformed"
    else:
        startup_status = _file_status(startup_authority_sha, startup_global_sha)
    claude_entries[CLIENT_CLAUDE_STARTUP_ROUTER_KEY] = {
        "name": f"{CLIENT_CLAUDE_STARTUP_ROUTER_TARGET}#aistock-managed-router",
        "repo_path": str(startup_repo_path),
        "authority_path": str(startup_authority_path),
        "global_path": str(startup_global_path),
        "repo_sha256": startup_repo_sha,
        "authority_sha256": startup_authority_sha,
        "global_sha256": startup_global_sha,
        "repo_error": startup_repo_error,
        "authority_error": startup_authority_error,
        "global_error": startup_global_error,
        "checkout_status": (
            "matches_authority"
            if startup_repo_sha is not None and startup_repo_sha == startup_authority_sha
            else "differs_from_authority"
        ),
        "status": startup_status,
    }

    codex_status = _combined_status(
        (entry["status"] for entry in codex_entries.values()),
        stale_status="stale",
    )
    claude_status = _combined_status(
        (entry["status"] for entry in claude_entries.values()),
        stale_status="stale_global",
    )

    paths: dict[str, str] = {"workflow_cli": str(cli), "checkout_workflow_cli": str(checkout_cli)}
    for key, entry in codex_entries.items():
        paths[f"repo_codex_{key}_skill"] = entry["repo_path"]
        paths[f"authority_codex_{key}_skill"] = entry["authority_path"]
        paths[f"global_codex_{key}_skill"] = entry["global_path"]
    for key, entry in claude_entries.items():
        paths[f"claude_{key}_command"] = entry["repo_path"]
        paths[f"authority_claude_{key}_command"] = entry["authority_path"]
        paths[f"global_claude_{key}_command"] = entry["global_path"]

    payload: dict[str, Any] = {
        "schema_version": "aistock_issue_workflow_client_manifest_v2",
        "repo_commit": repo_head.get("stdout") if repo_head.get("ok") else None,
        "checkout_root": str(checkout_root),
        "checkout_commit_relation": checkout_relation,
        "source_authority": authority,
        "codex_home": str(codex_home),
        "claude_home": str(claude_home),
        "workflow_cli_sha256": cli_sha,
        "authority_workflow_cli_sha256": _sha256_file(authority_cli) if authority.get("ready") else None,
        "codex_skill_status": codex_status,
        "claude_command_status": claude_status,
        "codex_entries": codex_entries,
        "claude_entries": claude_entries,
        "paths": paths,
        "restart_recommended": False,
        "install_client_next_command": subprocess.list2cmdline(
            [
                "python",
                str(authority_cli),
                "install-client",
                "--apply",
                "--codex-home",
                str(codex_home),
                "--claude-home",
                str(claude_home),
            ]
        ),
    }

    # Backward-compatible flat fields used by compact output and older tests.
    if "issue" in codex_entries:
        payload["codex_skill_sha256"] = codex_entries["issue"]["authority_sha256"]
        payload["checkout_codex_skill_sha256"] = codex_entries["issue"]["repo_sha256"]
        payload["global_codex_skill_sha256"] = codex_entries["issue"]["global_sha256"]
        payload["codex_issue_skill_status"] = codex_entries["issue"]["status"]
    if "feature" in codex_entries:
        payload["codex_feature_skill_sha256"] = codex_entries["feature"]["authority_sha256"]
        payload["checkout_codex_feature_skill_sha256"] = codex_entries["feature"]["repo_sha256"]
        payload["global_codex_feature_skill_sha256"] = codex_entries["feature"]["global_sha256"]
        payload["codex_feature_skill_status"] = codex_entries["feature"]["status"]
    if "issue" in claude_entries:
        payload["claude_command_sha256"] = claude_entries["issue"]["authority_sha256"]
        payload["checkout_claude_command_sha256"] = claude_entries["issue"]["repo_sha256"]
        payload["global_claude_command_sha256"] = claude_entries["issue"]["global_sha256"]
        payload["claude_issue_command_status"] = claude_entries["issue"]["status"]
    if "feature" in claude_entries:
        payload["claude_feature_command_sha256"] = claude_entries["feature"]["authority_sha256"]
        payload["checkout_claude_feature_command_sha256"] = claude_entries["feature"]["repo_sha256"]
        payload["global_claude_feature_command_sha256"] = claude_entries["feature"]["global_sha256"]
        payload["claude_feature_command_status"] = claude_entries["feature"]["status"]
    for key in ("router", "docs_handoff", "merge_aftercare", "readonly_triage", "validation_delegation"):
        if key in codex_entries:
            payload[f"codex_{key}_skill_status"] = codex_entries[key]["status"]
        if key in claude_entries:
            payload[f"claude_{key}_command_status"] = claude_entries[key]["status"]
    return payload


def _client_lane_verification(
    manifest: dict[str, Any],
    *,
    selected_lane: str | None,
    verify_codex: bool,
    verify_claude: bool,
) -> dict[str, Any]:
    selected_keys = _selected_client_lane_keys(selected_lane)
    blocking: list[str] = []
    warnings: list[str] = []
    checkout_advisories: list[str] = []
    checked: list[dict[str, str]] = []
    for client, enabled, entries_key in (
        ("codex", verify_codex, "codex_entries"),
        ("claude", verify_claude, "claude_entries"),
    ):
        if not enabled:
            continue
        entries = manifest.get(entries_key) or {}
        for key, entry in entries.items():
            status = str((entry or {}).get("status") or "missing")
            checkout_status = str((entry or {}).get("checkout_status") or "unknown")
            relevant = key in selected_keys or (
                client == "claude"
                and key == CLIENT_CLAUDE_STARTUP_ROUTER_KEY
                and "router" in selected_keys
            )
            checked.append(
                {
                    "client": client,
                    "lane": key,
                    "status": status,
                    "checkout_status": checkout_status,
                    "relevance": "selected" if relevant else "unrelated",
                }
            )
            if relevant and checkout_status == "differs_from_authority":
                checkout_advisories.append(
                    f"{client} lane {key} checkout differs from merged client authority; "
                    "do not install from this task worktree"
                )
            if status == "current":
                continue
            message = f"{client} lane {key} is {status}"
            if relevant:
                blocking.append(message)
            else:
                warnings.append(f"unrelated {message}")
    authority = manifest.get("source_authority") if isinstance(manifest.get("source_authority"), dict) else {}
    authority_cli = Path(str(authority.get("root") or _canonical_root())) / "scripts" / "aistock_issue_workflow.py"
    install_args = ["python", str(authority_cli), "install-client", "--apply"]
    verify_args = ["python", str(authority_cli), "verify-clients", "--workflow-only"]
    if selected_lane:
        install_args.extend(["--selected-lane", selected_lane])
        verify_args.extend(["--selected-lane", selected_lane])
    if verify_codex:
        install_args.extend(["--codex-home", str(manifest.get("codex_home") or _codex_home())])
        verify_args.extend(["--codex-home", str(manifest.get("codex_home") or _codex_home())])
    else:
        install_args.append("--skip-codex")
        verify_args.append("--skip-codex")
    if verify_claude:
        install_args.extend(["--claude-home", str(manifest.get("claude_home") or _claude_home())])
        verify_args.extend(["--claude-home", str(manifest.get("claude_home") or _claude_home())])
    else:
        install_args.append("--skip-claude")
        verify_args.append("--skip-claude")
    if blocking:
        authority_blocked = any("authority_unavailable" in item or "missing_authority" in item for item in blocking)
        action = "sync_canonical_main_then_single_owner_install" if authority_blocked else "request_single_owner_sync"
    else:
        action = "continue_without_install"
    remediation = {
        "action": action,
        "single_owner_required": bool(blocking),
        "must_not_install_from_task_worktree": True,
        "owner_command": subprocess.list2cmdline(install_args) if blocking else None,
        "window_verify_command": subprocess.list2cmdline(verify_args),
        "authority_root": str(authority.get("root") or _canonical_root()),
        "authority_commit": authority.get("commit"),
    }
    return {
        "selected_lane": selected_lane,
        "selected_lane_keys": sorted(selected_keys),
        "checked": checked,
        "blocking": blocking,
        "warnings": warnings,
        "checkout_advisories": checkout_advisories,
        "remediation": remediation,
        "ready": not blocking,
    }

def _validation_center_runtime_safety(root: Path | None = None) -> dict[str, Any]:
    root = root or REPO_ROOT
    app_path = root / "backend" / "validation_app.py"
    return {
        "schema_version": "aistock_validation_center_runtime_safety_v1",
        "workflow_gate": "ready" if app_path.exists() else "warning",
        "safe_app_module": "backend.validation_app:app",
        "unsafe_app_module": "backend.main:app",
        "safe_command": "python -m uvicorn backend.validation_app:app --host 127.0.0.1 --port 8012",
        "allowed_backend_ports": [8011, 8012],
        "production_ports_forbidden": [8001, 3000],
        "warning": (
            "Use backend.validation_app:app for Validation Center-only restarts; "
            "do not start backend.main:app on VC dev ports because it can load business schedulers/QMT."
        ),
        "app_path": _repo_rel(app_path, root),
        "app_exists": app_path.exists(),
    }


def build_doctor_report(*, skip_external: bool = False) -> dict[str, Any]:
    canonical_root = _canonical_root()
    codex_skill = _codex_home() / "skills" / "fix-aistock-issue" / "SKILL.md"
    checks = [
        _path_check(REPO_ROOT, "repo_root"),
        _path_check(REPO_ROOT / "scripts" / "aistock_issue_workflow.py", "high_level_cli"),
        _path_check(REPO_ROOT / "scripts" / "issue_flow.py", "lower_level_cli"),
        _path_check(REPO_ROOT / ".codex" / "skills" / "fix-aistock-issue" / "SKILL.md", "repo_codex_skill"),
        _path_check(codex_skill, "global_codex_skill", blocking=False),
        _path_check(REPO_ROOT / ".claude" / "commands" / "fix-aistock-issue.md", "claude_code_command", blocking=False),
        _path_check(REPO_ROOT / "docs" / "standards" / "aistock_development_standard_v1.5_20260523.md", "active_standard"),
        _path_check(REPO_ROOT / "docs" / "architecture" / "aistock_issue_workflow_opensource_cicd_design_v2_20260525.md", "v2_design"),
        _path_check(canonical_root, "canonical_root", blocking=False),
    ]
    blocking = [
        f"missing {item['label']}: {item['path']}"
        for item in checks
        if item["blocking"] and not item["exists"]
    ]
    warnings = [
        f"missing optional {item['label']}: {item['path']}"
        for item in checks
        if not item["blocking"] and not item["exists"]
    ]

    repo_git = _git_snapshot(REPO_ROOT)
    canonical_git = _git_snapshot(canonical_root) if canonical_root.exists() else {"ok": False, "error": "canonical root missing"}
    if not repo_git.get("ok"):
        blocking.append(str(repo_git.get("error") or "repo git status failed"))
    if canonical_git.get("dirty"):
        warnings.append(f"canonical root has {canonical_git.get('dirty_count')} dirty file(s); root sync must stop")
    if canonical_git.get("branch") == "main" and canonical_git.get("head") != canonical_git.get("origin_main"):
        warnings.append("canonical root main is not equal to origin/main")
    worktree_hygiene = _worktree_hygiene_report(canonical_root)
    for item in worktree_hygiene.get("blocking") or []:
        blocking.append(f"worktree hygiene: {item}")
    for item in worktree_hygiene.get("warnings") or []:
        warnings.append(f"worktree hygiene: {item}")
    cleanup_janitor = _cleanup_janitor_report(canonical_root)
    if cleanup_janitor.get("workflow_gate") == "warning":
        warnings.append(
            "cleanup janitor found branch/worktree cleanup debt; run read-only triage or cleanup-after-merge for listed samples"
        )

    github: dict[str, Any] = {
        "env_repository": os.environ.get("GITHUB_REPOSITORY"),
        "auth": {"ok": None, "skipped": skip_external},
        "repo": {"ok": None, "skipped": skip_external},
    }
    if not skip_external:
        github["auth"] = _run_command(["gh", "auth", "status"], cwd=REPO_ROOT, timeout=20)
        github["repo"] = _run_command(["gh", "repo", "view", "licong01-cloud/AIstock", "--json", "nameWithOwner"], cwd=REPO_ROOT, timeout=20)
        if not github["auth"].get("ok"):
            warnings.append("GitHub CLI auth check failed; MCP may still work, but gh fallback is not ready")
        if not github["repo"].get("ok"):
            warnings.append("GitHub repo check failed for licong01-cloud/AIstock")

    bug_id_allocation = _bug_id_allocation_summary(
        _bug_id_allocation_report(
            REPO_ROOT,
            include_github=not skip_external,
            github_required=False,
            tolerate_unrelated_allocator_errors=True,
        )
    )
    for warning in bug_id_allocation.get("warnings") or []:
        warnings.append(f"bug id allocation: {warning}")

    mcp = _mcp_config_snapshot()
    if mcp["stale_worktree_config_files"]:
        warnings.append("MCP/Codex config mentions AIstock_worktrees; verify it is not a stale server target")

    client_manifest = _client_manifest()
    if client_manifest["codex_skill_status"] in {"authority_unavailable", "missing_authority"}:
        blocking.append("merged Codex workflow authority is unavailable or incomplete")
    elif client_manifest["codex_skill_status"] in {"stale", "missing_global"}:
        warnings.append(
            "global Codex workflow skill set is missing or stale; verify the router and selected lane for this window "
            "before any target-scoped install"
        )
    if client_manifest["claude_command_status"] in {"authority_unavailable", "missing_authority"}:
        blocking.append("merged Claude Code workflow authority is unavailable or incomplete")
    elif client_manifest["claude_command_status"] in {"missing_global", "stale_global"}:
        warnings.append(
            "global Claude Code workflow command set is missing or stale; verify the router and selected lane for this "
            "window before any target-scoped install"
        )

    code_intel = code_intelligence.build_doctor_report(REPO_ROOT, skip_external=skip_external)
    for warning in code_intel.get("warnings") or []:
        warnings.append(f"code intelligence: {warning}")
    for item in code_intel.get("blocking") or []:
        blocking.append(f"code intelligence: {item}")
    h7_code_intelligence = _code_intelligence_readiness(code_intel)
    vc_runtime_safety = _validation_center_runtime_safety(REPO_ROOT)
    if vc_runtime_safety["workflow_gate"] != "ready":
        warnings.append("Validation Center-only runtime app is missing; do not use backend.main:app as a substitute")

    gate = "blocked" if blocking else ("warning" if warnings else "ready")
    next_command = f"python {REPO_ROOT / 'scripts' / 'aistock_issue_workflow.py'} run --bug-id BUG-XXX --mode plan --create-worktree"
    return {
        "schema_version": "aistock_issue_workflow_doctor_v1",
        "generated_at": _utc_now(),
        "workflow_gate": gate,
        "blocking": blocking,
        "warnings": warnings,
        "checks": checks,
        "repo_git": repo_git,
        "canonical_root": str(canonical_root),
        "canonical_git": canonical_git,
        "worktree_hygiene": worktree_hygiene,
        "cleanup_janitor": cleanup_janitor,
        "github": github,
        "bug_id_allocation": bug_id_allocation,
        "mcp": mcp,
        "code_intelligence": code_intel,
        "h7_code_intelligence": h7_code_intelligence,
        "validation_center_runtime_safety": vc_runtime_safety,
        "client_manifest": client_manifest,
        "restart_recommended": client_manifest.get("restart_recommended"),
        "install_client_next_command": client_manifest.get("install_client_next_command"),
        "next_command": next_command,
    }


def build_client_install_plan(
    *,
    apply: bool = False,
    codex_home: str | None = None,
    claude_home: str | None = None,
    install_codex: bool = True,
    install_claude: bool = True,
    selected_lane: str | None = None,
) -> dict[str, Any]:
    if not install_codex and not install_claude:
        raise WorkflowError("install-client requires at least one target client")
    target_home = Path(codex_home) if codex_home else _codex_home()
    target_claude_home = Path(claude_home) if claude_home else _claude_home()
    selected_keys = _selected_client_lane_keys(selected_lane)
    source_authority = _client_source_authority()
    authority_root = Path(source_authority["root"])
    source_codex_skills = [
        (key, name, authority_root / ".codex" / "skills" / name, target_home / "skills" / name)
        for key, name in CLIENT_CODEX_SKILLS
        if key in selected_keys
    ] if install_codex else []
    source_claude_commands = [
        (key, name, authority_root / ".claude" / "commands" / name, target_claude_home / "commands" / name)
        for key, name in CLIENT_CLAUDE_COMMANDS
        if key in selected_keys
    ] if install_claude else []
    startup_router_selected = install_claude and "router" in selected_keys
    startup_router_source = authority_root / CLIENT_CLAUDE_STARTUP_ROUTER_SOURCE
    startup_router_target = target_claude_home / CLIENT_CLAUDE_STARTUP_ROUTER_TARGET
    startup_router_block: str | None = None
    startup_router_error: str | None = None
    startup_router_target_block: str | None = None
    startup_router_target_error: str | None = None
    if startup_router_selected:
        startup_router_block, startup_router_error = _claude_startup_router_block(
            startup_router_source,
            require_only_block=True,
        )
        startup_router_target_block, startup_router_target_error = _claude_startup_router_block(
            startup_router_target
        )
    blocking: list[str] = []
    if not source_authority.get("ready"):
        blocking.append(
            "merged client authority is unavailable: "
            f"{source_authority.get('blocking_reason') or 'sync canonical main with origin/main'}"
        )
    for _key, name, source, _target in source_codex_skills:
        if not source.exists():
            blocking.append(f"missing repo Codex skill {name}: {source}")
    for _key, name, source, _target in source_claude_commands:
        if not source.exists():
            blocking.append(f"missing repo Claude Code command {name}: {source}")
    if startup_router_selected and startup_router_error:
        blocking.append(
            "invalid repo Claude startup router authority "
            f"{startup_router_source}: {startup_router_error}"
        )
    if startup_router_selected and startup_router_target_error not in {None, "missing"}:
        blocking.append(
            "Claude startup memory has invalid AIstock managed-router block "
            f"{startup_router_target}: {startup_router_target_error}"
        )

    actions: list[dict[str, Any]] = []
    for key, name, source, target in source_codex_skills:
        source_sha = _sha256_tree(source)
        target_sha = _sha256_tree(target)
        actions.append(
            {
                "action": f"sync_global_codex_{key}_skill",
                "name": name,
                "source": str(source),
                "target": str(target),
                "safe": source.exists() and not blocking,
                "source_sha256": source_sha,
                "target_sha256": target_sha,
                "sync_required": source_sha != target_sha,
            }
        )
    for key, name, source, target in source_claude_commands:
        source_sha = _sha256_file(source)
        target_sha = _sha256_file(target)
        actions.append(
            {
                "action": f"sync_claude_code_{key}_command",
                "name": name,
                "source": str(source),
                "target": str(target),
                "safe": source.exists() and not blocking,
                "source_sha256": source_sha,
                "target_sha256": target_sha,
                "sync_required": source_sha != target_sha,
            }
        )
    if startup_router_selected:
        source_sha = _sha256_text(startup_router_block) if startup_router_block is not None else None
        target_sha = (
            _sha256_text(startup_router_target_block)
            if startup_router_target_block is not None
            else None
        )
        actions.append(
            {
                "action": "sync_claude_code_startup_router",
                "name": f"{CLIENT_CLAUDE_STARTUP_ROUTER_TARGET}#aistock-managed-router",
                "source": str(startup_router_source),
                "target": str(startup_router_target),
                "safe": startup_router_block is not None and not blocking,
                "source_sha256": source_sha,
                "target_sha256": target_sha,
                "sync_required": source_sha != target_sha,
            }
        )

    payload = {
        "schema_version": "aistock_issue_workflow_client_install_v2",
        "generated_at": _utc_now(),
        "dry_run": not apply,
        "workflow_gate": "ready_for_install" if not blocking else "blocked",
        "blocking": blocking,
        "actions": actions,
        "codex_home": str(target_home),
        "claude_home": str(target_claude_home),
        "install_codex": install_codex,
        "install_claude": install_claude,
        "selected_lane": selected_lane,
        "selected_lane_keys": sorted(selected_keys),
        "source_authority": source_authority,
        "single_owner_required": any(bool(item.get("sync_required")) for item in actions),
        "task_worktree_is_install_source": False,
        "client_manifest_before": _client_manifest(target_home, target_claude_home),
    }
    if apply:
        if blocking:
            raise WorkflowError("; ".join(blocking))
        installed: list[dict[str, str]] = []
        skipped_current: list[dict[str, str]] = []
        with _ClientInstallLock():
            locked_authority = _client_source_authority()
            if (
                not locked_authority.get("ready")
                or not _same_path(Path(locked_authority["root"]), authority_root)
                or locked_authority.get("commit") != source_authority.get("commit")
            ):
                raise WorkflowError(
                    "merged client authority changed before install; rerun from synchronized canonical main"
                )
            if startup_router_selected:
                locked_startup_block, locked_startup_error = _claude_startup_router_block(
                    startup_router_source,
                    require_only_block=True,
                )
                locked_target_block, locked_target_error = _claude_startup_router_block(
                    startup_router_target
                )
                if locked_startup_error or locked_startup_block is None:
                    raise WorkflowError(
                        "Claude startup router authority changed before install: "
                        f"{locked_startup_error or 'missing'}"
                    )
                if locked_target_error not in {None, "missing"}:
                    raise WorkflowError(
                        "Claude startup memory changed to malformed managed-router markers before install"
                    )
                if _sha256_text(locked_startup_block) == (
                    _sha256_text(locked_target_block) if locked_target_block is not None else None
                ):
                    skipped_current.append(
                        {
                            "client": "claude",
                            "lane": CLIENT_CLAUDE_STARTUP_ROUTER_KEY,
                            "target": str(startup_router_target),
                        }
                    )
                else:
                    current_text = (
                        startup_router_target.read_text(encoding="utf-8")
                        if startup_router_target.exists()
                        else ""
                    )
                    merged_text = _merge_claude_startup_router_block(
                        current_text,
                        locked_startup_block,
                    )
                    _staged_replace_text(startup_router_target, merged_text)
                    installed.append(
                        {
                            "client": "claude",
                            "lane": CLIENT_CLAUDE_STARTUP_ROUTER_KEY,
                            "target": str(startup_router_target),
                        }
                    )
            for key, _name, source, target in source_codex_skills:
                if _sha256_tree(source) == _sha256_tree(target):
                    skipped_current.append({"client": "codex", "lane": key, "target": str(target)})
                    continue
                _staged_replace_tree(source, target)
                installed.append({"client": "codex", "lane": key, "target": str(target)})
            for key, _name, source, target in source_claude_commands:
                if _sha256_file(source) == _sha256_file(target):
                    skipped_current.append({"client": "claude", "lane": key, "target": str(target)})
                    continue
                _staged_replace_file(source, target)
                installed.append({"client": "claude", "lane": key, "target": str(target)})
            payload["client_manifest_after"] = _client_manifest(target_home, target_claude_home)
        payload["workflow_gate"] = "installed"
        payload["dry_run"] = False
        payload["installed"] = installed
        payload["skipped_current"] = skipped_current
        payload["installed_count"] = len(installed)
        payload["skipped_current_count"] = len(skipped_current)
    manifest_path = REPO_ROOT / WORKFLOW_ROOT / "client-manifest.json"
    _write_json(
        manifest_path,
        payload.get("client_manifest_after")
        or payload.get("client_manifest_before")
        or _client_manifest(target_home, target_claude_home),
    )
    payload["client_manifest_path"] = _repo_rel(manifest_path)
    return payload

def _state_roots_for_bug(bug_id: str) -> list[Path]:
    roots: list[Path] = [REPO_ROOT]
    worktree_root = _default_worktree_root()
    if worktree_root.exists():
        for state in worktree_root.glob(f"*/{WORKFLOW_ROOT.as_posix()}/{bug_id}/state.json"):
            roots.append(state.parents[3])
    unique: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root.resolve())
        if key not in seen:
            seen.add(key)
            unique.append(root)
    return unique


def build_resume_plan(*, bug_id: str, worktree: str | None = None, events_limit: int = 8) -> dict[str, Any]:
    canonical_bug_id = bug_id.strip().upper()
    roots = [Path(worktree)] if worktree else _state_roots_for_bug(canonical_bug_id)
    candidates = [(root, _load_state(canonical_bug_id, root)) for root in roots]
    candidates = [(root, state) for root, state in candidates if state]
    if not candidates:
        raise WorkflowError(f"No workflow state found for {canonical_bug_id}; run start or run --mode plan first")
    root, state = sorted(candidates, key=lambda item: _workflow_state_sort_key(item[0], item[1]))[-1]
    events_path = _events_path(canonical_bug_id, root)
    events: list[dict[str, Any]] = []
    if events_path.exists():
        lines = events_path.read_text(encoding="utf-8").splitlines()[-events_limit:]
        events = [json.loads(line) for line in lines if line.strip()]
    git = _git_snapshot(root) if root.exists() else {"ok": False, "error": f"workflow root missing: {root}"}
    dirty_stop = bool(git.get("dirty") and state.get("state") == "validation_passed")
    state_worktree = str(state.get("worktree") or "").strip()
    missing_state_worktree = bool(state_worktree and not Path(state_worktree).exists())
    actual_worktree = None if missing_state_worktree else state.get("worktree")
    planned_worktree = state.get("planned_worktree") or (state_worktree if missing_state_worktree else None)
    planned_only = bool(planned_worktree and not actual_worktree)
    stop_conditions = ["commit task files before PR automation"] if dirty_stop else []
    if planned_only:
        stop_conditions.append("planned worktree has not been created; rerun plan with --create-worktree")
    task_card_availability = state.get("task_card_availability") or _task_card_exists_payload(canonical_bug_id, root)
    if not task_card_availability.get("available") and state.get("state") not in TERMINAL_WORKFLOW_STATES:
        stop_conditions.append("task-card.md/json missing; rerun plan to regenerate compact resume context")
    resume_state = {
        **state,
        "allowed_write_scope": state.get("allowed_write_scope") or state.get("scope") or [],
        "required_verification": state.get("required_verification") or state.get("validation_evidence") or [],
    }
    return {
        "schema_version": "aistock_issue_workflow_resume_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "workflow_root": str(root),
        "workflow_git": git,
        "worktree": actual_worktree,
        "planned_worktree": planned_worktree,
        "branch": state.get("branch") or git.get("branch"),
        "planned_branch": state.get("planned_branch"),
        "task_card_json": state.get("task_card_json") or _repo_rel(_task_card_json_path(canonical_bug_id, root), root),
        "task_card_md": state.get("task_card_md") or _repo_rel(_task_card_md_path(canonical_bug_id, root), root),
        "task_card_availability": task_card_availability,
        "state_path": _repo_rel(_state_path(canonical_bug_id, root), root),
        "events_path": _repo_rel(events_path, root),
        "context_resume_digest": _workflow_context_resume_digest(resume_state, root=root),
        "state": state,
        "recent_events": events,
        "stop_conditions": stop_conditions,
        "next_command": _next_command_for_state(canonical_bug_id, state),
    }


def _workflow_artifacts_enabled() -> bool:
    value = os.environ.get("AISTOCK_WORKFLOW_ARTIFACTS", "").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    return os.environ.get("AISTOCK_VALIDATION_ARTIFACTS", "").strip().lower() in {"1", "true", "yes", "on"}


def build_postmortem_plan(
    *,
    bug_id: str,
    worktree: str | None = None,
    output_markdown: bool = True,
    persist_artifacts: bool | None = None,
) -> dict[str, Any]:
    canonical_bug_id = bug_id.strip().upper()
    roots = [Path(worktree)] if worktree else _state_roots_for_bug(canonical_bug_id)
    candidates = [(root, _load_state(canonical_bug_id, root)) for root in roots]
    candidates = [(root, state) for root, state in candidates if state]
    if not candidates:
        prior = _load_prior_postmortem(canonical_bug_id, roots)
        if prior:
            return prior
        raise WorkflowError(f"No workflow state found for {canonical_bug_id}; run start or run --mode plan first")
    root, state = sorted(candidates, key=lambda item: _workflow_state_sort_key(item[0], item[1]))[-1]
    events = _read_events(canonical_bug_id, root)
    timing = _augment_timing_with_issue_record(_workflow_timing_summary(canonical_bug_id, root), state, root)
    embedded_pre_cleanup = state.get("pre_cleanup_postmortem") if isinstance(state, dict) else None
    embedded_pre_cleanup_fallback: dict[str, Any] | None = None
    if isinstance(embedded_pre_cleanup, dict):
        embedded_timing = embedded_pre_cleanup.get("timing_summary")
        if isinstance(embedded_timing, dict) and (embedded_timing.get("event_count") or 0) > (timing.get("event_count") or 0):
            timing = dict(embedded_timing)
            timing.setdefault("notes", []).append(
                "timing_summary uses pre-cleanup phase evidence embedded in cleanup state to avoid losing source fix timing."
            )
            embedded_pre_cleanup_fallback = {
                "reason": "pre_cleanup_postmortem_embedded_in_cleanup_state",
                "current_event_count": (_workflow_timing_summary(canonical_bug_id, root).get("event_count") or 0),
                "prior_event_count": embedded_timing.get("event_count") or 0,
            }
    if state.get("state") == "complete" and (timing.get("event_count") or 0) <= 1:
        prior = _load_prior_postmortem(canonical_bug_id, roots)
        prior_timing = prior.get("timing_summary") if isinstance(prior, dict) else {}
        if prior and (prior_timing.get("event_count") or 0) > (timing.get("event_count") or 0):
            prior["workflow_gate"] = "artifact_fallback"
            prior["artifact_fallback"] = {
                "reason": "prior_postmortem_has_more_phase_evidence_than_cleanup_state",
                "current_event_count": timing.get("event_count") or 0,
                "prior_event_count": prior_timing.get("event_count") or 0,
            }
            return prior
    active = _active_workflows_for_bug(canonical_bug_id)
    duplicate_active_count = max(0, len(active) - 1)
    stale_pr_check = state.get("stale_pr_check") or {
        "status": "skipped_postmortem_remote_read",
        "reason": "postmortem reuses durable workflow state and does not repeat GitHub PR inventory queries",
    }
    context_metrics = state.get("context_metrics") or {}
    if not context_metrics:
        context_metrics = _fallback_context_metrics(canonical_bug_id, root)
    artifact_metrics = {}
    finish_plan = root / WORKFLOW_ROOT / canonical_bug_id / "finish-plan.json"
    pr_body = root / WORKFLOW_ROOT / canonical_bug_id / "pr-body.md"
    if finish_plan.exists() or pr_body.exists():
        artifact_metrics = {
            "finish_plan": _size_and_token_estimate(finish_plan),
            "pr_body": _size_and_token_estimate(pr_body),
        }
    phase_cost_table = _phase_cost_table(timing)
    h6_summary = _h6_summary(timing, context_metrics, artifact_metrics)
    h7_code_intelligence = _code_intelligence_readiness(state.get("code_intelligence") or {})
    code_intelligence_efficiency = _code_intelligence_efficiency_summary(state.get("code_intelligence") or {})
    waiting_for_user_restart_minutes: float | None = None
    wait_started: datetime | None = None
    for event in events:
        event_name = str(event.get("event") or "")
        event_time = _parse_utc_timestamp(str(event.get("timestamp") or ""))
        if event_name in {"source_fixed_runtime_pending", "state:fixed_source_pending_user_restart"} and event_time:
            wait_started = event_time
        elif event_name in {"state:runtime_verified", "post_restart_verify"} and wait_started and event_time:
            waiting_for_user_restart_minutes = round(max(0.0, (event_time - wait_started).total_seconds()) / 60.0, 3)
            break
    tool_telemetry = state.get("tool_telemetry") if isinstance(state.get("tool_telemetry"), dict) else None
    payload = {
        "schema_version": "aistock_issue_workflow_postmortem_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "workflow_root": str(root),
        "state": {
            "state": state.get("state"),
            "branch": state.get("branch"),
            "worktree": state.get("worktree") or str(root),
            "pr_url": state.get("pr_url"),
            "commit": state.get("commit"),
            "next_actions": state.get("next_actions") or [],
        },
        "timing_summary": timing,
        "phase_cost_table": phase_cost_table,
        "h6_summary": h6_summary,
        "context_metrics": context_metrics,
        "artifact_metrics": artifact_metrics,
        "task_card_availability": state.get("task_card_availability") or _task_card_exists_payload(canonical_bug_id, root),
        "validation_receipt_summary": _validation_receipt_summary(
            flow._as_list(state.get("validation_evidence")),
            flow._as_list(state.get("deferred_nightly_plans")),
        ),
        "h7_code_intelligence": h7_code_intelligence,
        "code_intelligence_efficiency": code_intelligence_efficiency,
        "active_workflows": active,
        "duplicate_active_count": duplicate_active_count,
        "stale_pr_check": stale_pr_check,
        "flow_overhead_estimate": {
            "known_duration_seconds": timing.get("known_duration_seconds"),
            "inferred_elapsed_seconds": timing.get("inferred_elapsed_seconds"),
            "event_count": timing.get("event_count"),
            "context_estimated_tokens": h6_summary.get("context_estimated_tokens"),
            "artifact_estimated_tokens": h6_summary.get("artifact_estimated_tokens"),
            "code_intelligence_broad_scan_avoided": code_intelligence_efficiency.get("broad_scan_avoided"),
            "estimated_broad_scan_tokens_avoided": code_intelligence_efficiency.get("estimated_broad_scan_tokens_avoided"),
            "top_phase": h6_summary.get("top_phase"),
        },
        "production_gates": state.get("production_gates") or {},
        "waiting_for_user_restart_minutes": waiting_for_user_restart_minutes,
        "backend_restart_owner": "user",
        "tool_telemetry": tool_telemetry,
        "tool_telemetry_policy": "optional_no_probe" if tool_telemetry is None else "caller_supplied",
        "recent_events": events[-20:],
    }
    if embedded_pre_cleanup_fallback:
        payload["workflow_gate"] = "artifact_fallback"
        payload["artifact_fallback"] = embedded_pre_cleanup_fallback
    if persist_artifacts is None:
        persist_artifacts = _workflow_artifacts_enabled() or str(state.get("state") or "") == "blocked"
    payload["artifact_policy"] = "persisted" if persist_artifacts else "compact_success_no_artifact"
    output_dir = root / WORKFLOW_ROOT / canonical_bug_id
    if output_markdown and persist_artifacts:
        lines = [
            f"# {canonical_bug_id} Workflow Postmortem",
            "",
            f"- State: `{payload['state']['state']}`",
            f"- Branch: `{payload['state']['branch'] or 'unknown'}`",
            f"- PR: {payload['state']['pr_url'] or 'n/a'}",
            f"- Events: `{timing.get('event_count')}`",
            f"- Known command duration: `{timing.get('known_duration_seconds')}s`",
            f"- Inferred elapsed duration: `{timing.get('inferred_elapsed_seconds')}s`",
            f"- Duplicate active workflows: `{duplicate_active_count}`",
            "",
            "## Phase Timing",
            "",
            "| Phase | Events | Known seconds | Inferred seconds |",
            "| --- | ---: | ---: | ---: |",
        ]
        for item in phase_cost_table:
            lines.append(
                f"| `{item.get('phase')}` | {item.get('event_count')} | {item.get('known_seconds')} | {item.get('inferred_seconds')} |"
            )
        lines.extend(
            [
                "",
                "## H6 Cost Summary",
                "",
                f"- Top phase: `{(h6_summary.get('top_phase') or {}).get('phase') or 'none'}`",
                f"- Token usage status: `{h6_summary.get('token_usage_status') or 'unknown'}`",
                f"- Context estimated tokens: `{h6_summary.get('context_estimated_tokens') if h6_summary.get('context_estimated_tokens') is not None else 'unknown'}`",
                f"- Artifact estimated tokens: `{h6_summary.get('artifact_estimated_tokens') if h6_summary.get('artifact_estimated_tokens') is not None else 'unknown'}`",
                f"- Queue seconds: `{h6_summary.get('queue_seconds') or 'not_recorded'}`",
                f"- Active fix seconds: `{h6_summary.get('active_fix_seconds') or 'not_recorded'}`",
                f"- Local validation seconds: `{h6_summary.get('local_validation_seconds') or 'not_recorded'}`",
                f"- PR/CI seconds: `{h6_summary.get('pr_ci_seconds') or 'not_recorded'}`",
                f"- Merge aftercare seconds: `{h6_summary.get('merge_aftercare_seconds') or 'not_recorded'}`",
                f"- Code repair seconds: `{h6_summary.get('code_repair_seconds') or 'not_recorded'}`",
                f"- Task card available: `{str(bool((payload.get('task_card_availability') or {}).get('available'))).lower()}`",
                f"- Broad pre-merge validation detected: `{str(bool((payload.get('validation_receipt_summary') or {}).get('broad_premerge_detected'))).lower()}`",
                "",
                "## H7 Code Intelligence",
                "",
                f"- status: `{h7_code_intelligence.get('status') or 'unknown'}`",
                f"- codegraph_status: `{h7_code_intelligence.get('codegraph_status') or 'unknown'}`",
                f"- codegraph_freshness: `{h7_code_intelligence.get('codegraph_freshness') or 'not_available'}`",
                f"- codegraph_freshness_ref: `{h7_code_intelligence.get('codegraph_freshness_ref') or 'not_available'}`",
                f"- understand_anything_status: `{h7_code_intelligence.get('understand_anything_status') or 'unknown'}`",
                f"- understand_anything_graph_exists: `{str(bool(h7_code_intelligence.get('understand_anything_graph_exists'))).lower()}`",
                f"- understand_anything_next_command: `{h7_code_intelligence.get('understand_anything_next_command') or 'not_required'}`",
                f"- fallback_used: `{str(bool(h7_code_intelligence.get('fallback_used'))).lower()}`",
                f"- readiness_next_command: `{h7_code_intelligence.get('readiness_next_command') or 'not_required'}`",
                f"- broad_scan_avoided: `{str(bool(code_intelligence_efficiency.get('broad_scan_avoided'))).lower()}`",
                f"- estimated_broad_scan_tokens_avoided: `{code_intelligence_efficiency.get('estimated_broad_scan_tokens_avoided') if code_intelligence_efficiency.get('estimated_broad_scan_tokens_avoided') is not None else 'unknown'}`",
                "",
                "## Production Gates",
                "",
                *[f"- {key}: `{value}`" for key, value in sorted((payload.get("production_gates") or {}).items())],
            ]
        )
        _write_text(output_dir / "postmortem.md", "\n".join(lines))
        payload["postmortem_md_path"] = _repo_rel(output_dir / "postmortem.md", root)
    if persist_artifacts:
        payload["postmortem_json_path"] = _repo_rel(output_dir / "postmortem.json", root)
        _write_json(output_dir / "postmortem.json", payload)
    return payload


def _classify_ci_issue(summary: dict[str, Any], issue: dict[str, Any]) -> str:
    title = str(issue.get("title") or "").lower()
    evidence_body = _issue_body_failure_text(str(issue.get("body") or "")).lower()
    title_body = f"{title}\n{evidence_body}"
    errors = "\n".join(
        str(item)
        for job in summary.get("failed_jobs") or []
        for item in [job.get("error_signature"), *(job.get("key_log_excerpt") or [])]
        if item
    ).lower()
    infra_signatures = [
        "self-hosted",
        "runner-preflight",
        "runner unavailable",
        "runner availability",
        "no online github actions runner",
        "unable to query github runner health",
        "aistock_runner_health_token",
    ]
    if any(token in title_body or token in errors for token in infra_signatures):
        return "infra_blocker"
    if any(token in title_body for token in ["flaky", "timeout", "network"]):
        return "infra_flaky"
    if any(token in errors for token in ["relation ", "does not exist", "fixture", "test fixture"]):
        return "test_fixture_gap_or_real_regression"
    if summary.get("diagnostic_status") == "complete":
        return "real_regression_candidate"
    return "needs_log_triage"


def build_triage_ci_issue_plan(
    *,
    issue_number: int | str,
    run_id: str | None = None,
    summary_json: str | None = None,
    skip_github_summary: bool = False,
) -> dict[str, Any]:
    issue = _load_github_issue(issue_number)
    linked = _find_bug_by_github_issue(issue["number"])
    body = str(issue.get("body") or "")
    detected_run_id = run_id or _extract_run_id_from_issue_body(body)
    summary: dict[str, Any]
    extraction_errors: list[str] = []
    if summary_json:
        summary = _load_json(Path(summary_json))
    elif detected_run_id and not skip_github_summary:
        summary = ci_failure_summary.summarize_actions_run(
            repo=GITHUB_REPO,
            run_id=detected_run_id,
            run_url=None,
            severity="P1",
        )
    else:
        extraction_errors.append("No Actions run id was found in the GitHub Issue body.")
        summary = ci_failure_summary.finalize_summary(
            {
                "schema_version": "aistock_ci_failure_summary_v1",
                "generated_at": _utc_now(),
                "severity": "P1",
                "workflow": "unknown",
                "run_id": detected_run_id or "",
                "run_url": None,
                "branch": None,
                "commit": None,
                "failed_jobs": [],
                "extraction_errors": extraction_errors,
            }
        )
    summary = _merge_issue_body_regression_locator(summary, body)
    classification = _classify_ci_issue(summary, issue)
    superseding_run = _find_superseding_main_success(summary)
    superseded_action = None
    if superseding_run and linked is None:
        same_branch = superseding_run.get("supersede_scope") == "same_branch"
        classification = "superseded_by_later_branch_success" if same_branch else "superseded_by_later_main_success"
        success_phrase = _superseded_success_phrase(superseding_run, str(summary.get("workflow") or "CI"))
        close_command = (
            f"gh issue close {issue.get('number')} --repo {GITHUB_REPO} --comment "
            f"\"Superseded by {success_phrase}: {superseding_run.get('run_url')}. "
            "No BUG JSON promotion is required.\""
        )
        superseded_action = {
            "workflow_gate": "superseded_by_latest_branch_success" if same_branch else "superseded_by_latest_main_success",
            "reason": (
                "A later successful run of the same workflow on the same branch supersedes this CI failure."
                if same_branch
                else "A later successful default-branch run of the same workflow supersedes this CI failure."
            ),
            "superseding_run": superseding_run,
            "next_command": close_command,
            "production_gates": {
                "production_ddl_gate": "noop",
                "production_frontend_dependency_gate": "noop",
                "production_backend_dependency_gate": "noop",
            },
        }
    module = (summary.get("suspected_modules") or ["validation"])[0]
    first_job = (summary.get("failed_jobs") or [{}])[0]
    failed_test = ((first_job.get("failed_tests") or [None])[0] or "").split("::")[-1]
    suggested_title = (
        f"{module} CI failure requires triage: {failed_test or first_job.get('error_signature') or issue.get('title')}"
    )
    output_dir = REPO_ROOT / WORKFLOW_ROOT / f"ci-issue-{issue.get('number')}"
    github_issue_url = issue.get("url") or _github_issue_url(issue.get("number"))
    failure_event = ci_failure_summary.build_failure_event(
        summary,
        github_issue_number=issue.get("number"),
        github_issue_url=github_issue_url,
    )
    context_pack = ci_failure_summary.build_context_pack(
        summary,
        github_issue_number=issue.get("number"),
        github_issue_url=github_issue_url,
    )
    actionable = classification in ACTIONABLE_CI_CLASSIFICATIONS
    triage_action: str | None = None
    if not actionable and classification not in {"infra_flaky", "infra_blocker", *SUPERSEDED_CI_CLASSIFICATIONS}:
        triage_action = "triage_incomplete_collect_failure_diagnostics_before_bug_promotion"
        failure_event["candidate_status"] = "triage_incomplete"
        context_pack["failure_event"] = failure_event
        handoff = context_pack.get("agent_handoff") if isinstance(context_pack.get("agent_handoff"), dict) else {}
        handoff["needs_bug_json"] = False
        handoff["handoff_mode"] = "triage_only"
        handoff["workflow_entrypoints"] = {
            "triage": f"python scripts/aistock_issue_workflow.py triage-ci-issue --issue {issue.get('number')}",
            "promote": "blocked_until_diagnostic_status_complete",
        }
        handoff["next_commands"] = [f"python scripts/aistock_issue_workflow.py triage-ci-issue --issue {issue.get('number')}"]
        handoff["stop_conditions"] = [
            "Do not promote BUG JSON until CI/Nightly diagnostics identify a concrete code or test failure.",
            "Do not edit source files from this triage-only issue.",
        ]
        context_pack["agent_handoff"] = handoff
    if superseded_action:
        failure_event["candidate_status"] = classification
        failure_event["superseded_action"] = superseded_action
        context_pack["failure_event"] = failure_event
        context_pack["superseded_action"] = superseded_action
        verification_scope = "same branch" if classification == "superseded_by_later_branch_success" else "default-branch"
        context_pack["required_verification"] = [
            f"Verify the superseding {verification_scope} run for the same workflow is successful.",
            "Close the auto-filed GitHub Issue with the superseding run URL; do not promote BUG JSON.",
        ]
        handoff = context_pack.get("agent_handoff") if isinstance(context_pack.get("agent_handoff"), dict) else {}
        handoff["workflow_entrypoints"] = {
            "triage": f"python scripts/aistock_issue_workflow.py triage-ci-issue --issue {issue.get('number')}",
            "close_superseded_issue": superseded_action["next_command"],
        }
        handoff["next_commands"] = [superseded_action["next_command"]]
        handoff["required_verification"] = context_pack["required_verification"]
        handoff["stop_conditions"] = ["Do not promote BUG JSON for this superseded CI failure."]
        context_pack["agent_handoff"] = handoff
    failure_event_path = output_dir / "failure-event.json"
    context_pack_json_path = output_dir / "context-pack.json"
    context_pack_md_path = output_dir / "context-pack.md"
    payload = {
        "schema_version": "aistock_issue_workflow_triage_ci_issue_v1",
        "generated_at": _utc_now(),
        "github_issue": {
            "number": issue.get("number"),
            "url": issue.get("url"),
            "title": issue.get("title"),
            "state": issue.get("state"),
        },
        "detected_run_id": detected_run_id,
        "summary": summary,
        "failure_event": failure_event,
        "failure_event_path": _repo_rel(failure_event_path),
        "context_pack": context_pack,
        "context_pack_json_path": _repo_rel(context_pack_json_path),
        "context_pack_md_path": _repo_rel(context_pack_md_path),
        "classification_recommendation": classification,
        "linked_bug": {"bug_id": linked[0].get("bug_id"), "path": _repo_rel(linked[1])} if linked else None,
        "needs_bug_json": linked is None and actionable,
        "triage_action": triage_action,
        "suggested_bug": {
            "module": module,
            "severity": summary.get("severity") or "P1",
            "title": suggested_title[:180],
            "risk_area": "ci_failure_intake",
            "allowed_write_scope": context_pack.get("allowed_write_scope") or [],
            "required_verification": context_pack.get("required_verification") or [],
        },
        "infra_action": (
            {
                "workflow_gate": "infra_action_required",
                "reason": "CI/Nightly failure is classified as infrastructure, not a code regression.",
                "next_actions": [
                    "restore or register the self-hosted Windows GitHub Actions runner",
                    "verify runner labels include: self-hosted, windows",
                    "configure AISTOCK_RUNNER_HEALTH_TOKEN if runner API access is denied",
                    "rerun the failed workflow after infrastructure is healthy",
                ],
                "production_gates": {
                    "production_ddl_gate": "noop",
                    "production_frontend_dependency_gate": "noop",
                    "production_backend_dependency_gate": "noop",
                },
            }
            if classification in {"infra_flaky", "infra_blocker"}
            else None
        ),
        "superseded_action": superseded_action,
        "next_command": (
            "infra_action_required_no_code_bug"
            if classification in {"infra_flaky", "infra_blocker"} and linked is None
            else (
                superseded_action["next_command"]
                if classification in SUPERSEDED_CI_CLASSIFICATIONS and linked is None and superseded_action
                else (
                    f"python scripts/aistock_issue_workflow.py promote-ci-issue --issue {issue.get('number')} "
                    "--create-registry-worktree --apply"
                    if linked is None and actionable
                    else triage_action
                    if linked is None
                    else f"python scripts/aistock_issue_workflow.py run --bug-id {linked[0].get('bug_id')} --mode plan --create-worktree"
                )
            )
        ),
    }
    _write_json(failure_event_path, failure_event)
    _write_json(context_pack_json_path, context_pack)
    _write_text(context_pack_md_path, ci_failure_summary.render_context_pack_markdown(context_pack))
    _write_json(output_dir / "triage-ci-issue.json", payload)
    return payload


def _list_open_auto_filed_ci_issues(*, limit: int = 50) -> list[dict[str, Any]]:
    result = _execute_checked(
        [
            "gh",
            "issue",
            "list",
            "--repo",
            GITHUB_REPO,
            "--state",
            "open",
            "--label",
            "auto-filed",
            "--limit",
            str(limit),
            "--json",
            "number,title,state,url,labels",
        ],
        cwd=REPO_ROOT,
        timeout=60,
    )
    try:
        issues = json.loads(str(result.get("stdout") or "[]"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"cannot parse auto-filed CI issue list: {exc}") from exc
    if not isinstance(issues, list):
        raise WorkflowError("auto-filed CI issue list returned non-list JSON")
    filtered: list[dict[str, Any]] = []
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        labels = {
            str(item.get("name") or "")
            for item in issue.get("labels") or []
            if isinstance(item, dict)
        }
        if "ci" in labels:
            filtered.append(issue)
    return filtered


def _close_superseded_ci_issue(issue_number: int | str, superseding_run: dict[str, Any], workflow: str | None) -> dict[str, Any]:
    success_phrase = _superseded_success_phrase(superseding_run, workflow)
    comment = (
        f"Superseded by {success_phrase}: {superseding_run.get('run_url')}. "
        "No BUG JSON promotion is required. production_ddl_gate=noop; "
        "production_frontend_dependency_gate=noop; production_backend_dependency_gate=noop."
    )
    result = _execute_checked(
        [
            "gh",
            "issue",
            "close",
            str(issue_number),
            "--repo",
            GITHUB_REPO,
            "--comment",
            comment,
        ],
        cwd=REPO_ROOT,
        timeout=60,
    )
    label_result = _sync_closed_auto_filed_issue_labels(issue_number)
    result["label_sync"] = _pick(label_result, "ok", "returncode", "skipped")
    return result


def _close_infra_ci_issue(issue_number: int | str, infra_action: dict[str, Any], workflow: str | None) -> dict[str, Any]:
    next_actions = infra_action.get("next_actions") if isinstance(infra_action.get("next_actions"), list) else []
    next_action_text = "; ".join(str(item) for item in next_actions[:4])
    comment = (
        f"Closed as infrastructure-only {workflow or 'CI'} failure. "
        f"{infra_action.get('reason') or 'No code BUG JSON promotion is required.'} "
        f"Next actions: {next_action_text or 'restore infrastructure and rerun CI/Nightly'}. "
        "production_ddl_gate=noop; production_frontend_dependency_gate=noop; "
        "production_backend_dependency_gate=noop."
    )
    result = _execute_checked(
        [
            "gh",
            "issue",
            "close",
            str(issue_number),
            "--repo",
            GITHUB_REPO,
            "--comment",
            comment,
        ],
        cwd=REPO_ROOT,
        timeout=60,
    )
    label_result = _sync_closed_auto_filed_issue_labels(issue_number)
    result["label_sync"] = _pick(label_result, "ok", "returncode", "skipped")
    return result


def _sync_closed_issue_status_labels(
    issue_number: int | str,
    *,
    required_label: str | None = None,
    add_fixed: bool = True,
    require_closed: bool = False,
) -> dict[str, Any]:
    """Keep a closed issue from retaining active workflow labels."""
    view_args = ["gh", "issue", "view", str(issue_number), "--repo", GITHUB_REPO, "--json", "state,labels"]
    last_result: dict[str, Any] = {"ok": False, "skipped": False, "reason": "not run"}
    for attempt in range(1, 3):
        view = _run_transport_read_with_retry(view_args, cwd=REPO_ROOT, timeout=60, attempts=2)
        if not view.get("ok"):
            return {**view, "ok": False, "skipped": False, "attempts": attempt}
        try:
            payload = json.loads(str(view.get("stdout") or "{}"))
        except json.JSONDecodeError as exc:
            return {"ok": False, "skipped": False, "reason": f"invalid issue label readback: {exc}", "attempts": attempt}
        labels = {
            str(item.get("name") or "")
            for item in payload.get("labels") or []
            if isinstance(item, dict)
        }
        if str(payload.get("state") or "").upper() != "CLOSED":
            if require_closed:
                return {"ok": False, "skipped": False, "attempts": attempt, "reason": "GitHub Issue is not CLOSED"}
            return {"ok": True, "skipped": True, "attempts": attempt, "verified": True}
        if required_label and required_label not in labels:
            return {"ok": True, "skipped": True, "attempts": attempt, "verified": True}
        remove_labels = [label for label in ("status:open", "status:in_progress") if label in labels]
        add_labels = ["status:fixed"] if add_fixed and "status:fixed" not in labels else []
        if not remove_labels and not add_labels:
            return {"ok": True, "skipped": attempt == 1, "attempts": attempt, "verified": True}
        args = ["gh", "issue", "edit", str(issue_number), "--repo", GITHUB_REPO]
        for label in remove_labels:
            args.extend(["--remove-label", label])
        for label in add_labels:
            args.extend(["--add-label", label])
        last_result = _run_command(args, cwd=REPO_ROOT, timeout=60)
        message = str(last_result.get("stderr") or last_result.get("stdout") or "")
        if not last_result.get("ok") and not _looks_like_github_transport_failure(message):
            return {**last_result, "ok": False, "skipped": False, "attempts": attempt}
        if attempt < 2:
            time.sleep(0.5)
    final_view = _run_transport_read_with_retry(view_args, cwd=REPO_ROOT, timeout=60, attempts=2)
    if final_view.get("ok"):
        try:
            final_payload = json.loads(str(final_view.get("stdout") or "{}"))
        except json.JSONDecodeError:
            final_payload = {}
        final_labels = {
            str(item.get("name") or "")
            for item in final_payload.get("labels") or []
            if isinstance(item, dict)
        }
        aligned = (
            str(final_payload.get("state") or "").upper() == "CLOSED"
            and not final_labels.intersection({"status:open", "status:in_progress"})
            and (not add_fixed or "status:fixed" in final_labels)
        )
        if aligned:
            return {**last_result, "ok": True, "skipped": False, "attempts": 2, "verified": True}
    return {**last_result, "ok": False, "skipped": False, "attempts": 2, "reason": "issue status labels remain unverified"}


def _sync_closed_auto_filed_issue_labels(issue_number: int | str) -> dict[str, Any]:
    """Keep closed auto-filed issues from retaining the active status label."""
    return _sync_closed_issue_status_labels(issue_number, required_label="auto-filed", add_fixed=False)


def build_ci_issue_janitor_plan(
    *,
    issue_numbers: list[int | str] | None = None,
    apply: bool = False,
    limit: int = 50,
    skip_github_summary: bool = False,
    close_infra: bool = True,
) -> dict[str, Any]:
    if issue_numbers:
        issues = [{"number": str(item)} for item in issue_numbers]
        source = "explicit_issues"
    else:
        issues = _list_open_auto_filed_ci_issues(limit=limit)
        source = "open_auto_filed_ci_issues"
    evaluated: list[dict[str, Any]] = []
    closed: list[int | str] = []
    failed: list[dict[str, Any]] = []
    superseded_count = 0
    infra_count = 0
    for item in issues:
        issue_number = item.get("number")
        if issue_number is None:
            continue
        issue_value = int(issue_number) if str(issue_number).isdigit() else issue_number
        entry: dict[str, Any] = {"issue": issue_value, "action": "skip"}
        try:
            triage = build_triage_ci_issue_plan(
                issue_number=issue_number,
                skip_github_summary=skip_github_summary,
            )
        except WorkflowError as exc:
            entry.update({"action": "failed", "reason": str(exc)})
            failed.append(entry)
            evaluated.append(entry)
            continue
        entry["classification"] = triage.get("classification_recommendation")
        entry["linked_bug"] = triage.get("linked_bug")
        entry["github_issue"] = triage.get("github_issue")
        if (
            triage.get("classification_recommendation") in SUPERSEDED_CI_CLASSIFICATIONS
            and not triage.get("linked_bug")
            and isinstance(triage.get("superseded_action"), dict)
        ):
            superseded_count += 1
            entry["action"] = "close_superseded"
            entry["superseding_run"] = triage["superseded_action"].get("superseding_run")
            if apply:
                try:
                    result = _close_superseded_ci_issue(
                        issue_value,
                        triage["superseded_action"].get("superseding_run") or {},
                        (triage.get("summary") or {}).get("workflow"),
                    )
                    entry["close_result"] = _pick(result, "ok", "returncode")
                    closed.append(issue_value)
                except WorkflowError as exc:
                    entry.update({"action": "failed", "reason": str(exc)})
                    failed.append(entry)
        elif (
            triage.get("classification_recommendation") in {"infra_flaky", "infra_blocker"}
            and not triage.get("linked_bug")
            and triage.get("needs_bug_json") is False
            and isinstance(triage.get("infra_action"), dict)
        ):
            if not close_infra:
                entry["reason"] = "infra_closure_disabled"
                evaluated.append(entry)
                continue
            infra_count += 1
            entry["action"] = "close_infra"
            entry["infra_action"] = _pick(
                triage["infra_action"],
                "workflow_gate",
                "reason",
                "production_gates",
            )
            actions = triage["infra_action"].get("next_actions")
            if isinstance(actions, list):
                entry["infra_action"]["next_actions"] = actions[:4]
            if apply:
                try:
                    result = _close_infra_ci_issue(
                        issue_value,
                        triage["infra_action"],
                        (triage.get("summary") or {}).get("workflow"),
                    )
                    entry["close_result"] = _pick(result, "ok", "returncode")
                    closed.append(issue_value)
                except WorkflowError as exc:
                    entry.update({"action": "failed", "reason": str(exc)})
                    failed.append(entry)
        else:
            entry["reason"] = "not_superseded_or_infra_or_requires_bug_workflow"
        evaluated.append(entry)
    actionable_count = superseded_count + infra_count
    workflow_gate = "failed" if failed else ("closed" if apply and closed else ("ready_for_apply" if actionable_count else "no_actionable_ci_issues"))
    payload = {
        "schema_version": "aistock_issue_workflow_ci_issue_janitor_v1",
        "generated_at": _utc_now(),
        "workflow_gate": workflow_gate,
        "dry_run": not apply,
        "source": source,
        "limit": limit,
        "close_infra": close_infra,
        "scanned_count": len(evaluated),
        "superseded_count": superseded_count,
        "infra_count": infra_count,
        "closed_count": len(closed),
        "skipped_count": len([item for item in evaluated if item.get("action") == "skip"]),
        "failed_count": len(failed),
        "closed_issues": closed,
        "failed_issues": [{"issue": item.get("issue"), "reason": item.get("reason")} for item in failed],
        "issues": evaluated,
        "production_gates": {
            "production_ddl_gate": "noop",
            "production_frontend_dependency_gate": "noop",
            "production_backend_dependency_gate": "noop",
        },
    }
    if not apply and actionable_count:
        issue_args = " ".join(f"--issue {item.get('issue')}" for item in evaluated if item.get("action") in {"close_superseded", "close_infra"})
        limit_arg = "" if issue_args else f" --limit {limit}"
        infra_arg = "" if close_infra else " --superseded-only"
        payload["next_command"] = (
            f"python scripts/aistock_issue_workflow.py ci-issue-janitor {issue_args}{infra_arg} --apply"
            if issue_args
            else f"python scripts/aistock_issue_workflow.py ci-issue-janitor{limit_arg}{infra_arg} --apply"
        )
    output_dir = REPO_ROOT / WORKFLOW_ROOT / "ci-issue-janitor"
    _write_json(output_dir / "ci-issue-janitor.json", payload)
    return payload


def _github_actions_registry_pr_capability() -> dict[str, Any]:
    if str(os.environ.get("GITHUB_ACTIONS") or "").strip().lower() != "true":
        return {
            "allowed": True,
            "source": "not_github_actions",
            "reason": "local operator promotion uses the authenticated user capability",
        }
    result = _run_command(
        ["gh", "api", f"repos/{GITHUB_REPO}/actions/permissions/workflow"],
        timeout=30,
    )
    if not result.get("ok"):
        return {
            "allowed": False,
            "source": "github_actions_workflow_permissions",
            "reason": result.get("stderr") or result.get("stdout") or "workflow permission query failed",
        }
    try:
        payload = json.loads(str(result.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        return {
            "allowed": False,
            "source": "github_actions_workflow_permissions",
            "reason": f"workflow permission query returned invalid JSON: {exc}",
        }
    allowed = payload.get("can_approve_pull_request_reviews") is True
    return {
        "allowed": allowed,
        "source": "github_actions_workflow_permissions",
        "reason": "registry PR creation is enabled" if allowed else "repository Actions cannot create or approve pull requests",
        "default_workflow_permissions": payload.get("default_workflow_permissions"),
        "can_approve_pull_request_reviews": payload.get("can_approve_pull_request_reviews"),
    }


def build_promote_ci_issue_plan(
    *,
    issue_number: int | str,
    apply: bool,
    bug_id: str | None = None,
    summary_json: str | None = None,
    skip_github_summary: bool = False,
    create_registry_worktree: bool = False,
) -> dict[str, Any]:
    triage = build_triage_ci_issue_plan(
        issue_number=issue_number,
        summary_json=summary_json,
        skip_github_summary=skip_github_summary,
    )
    if triage.get("linked_bug"):
        return {
            "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
            "generated_at": _utc_now(),
            "workflow_gate": "already_linked",
            "dry_run": not apply,
            "triage": triage,
            "next_command": f"python scripts/aistock_issue_workflow.py run --bug-id {triage['linked_bug']['bug_id']} --mode plan --create-worktree",
        }
    if triage.get("classification_recommendation") in {"infra_flaky", "infra_blocker"}:
        return {
            "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
            "generated_at": _utc_now(),
            "workflow_gate": "blocked_infra_issue_not_code_bug",
            "dry_run": not apply,
            "triage": triage,
            "infra_action": triage.get("infra_action"),
            "next_command": "resolve_infrastructure_then_rerun_triage_ci_issue",
        }
    if triage.get("classification_recommendation") in SUPERSEDED_CI_CLASSIFICATIONS:
        action = triage.get("superseded_action") if isinstance(triage.get("superseded_action"), dict) else {}
        return {
            "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
            "generated_at": _utc_now(),
            "workflow_gate": "blocked_superseded_issue_not_code_bug",
            "dry_run": not apply,
            "triage": triage,
            "superseded_action": action,
            "next_command": action.get("next_command") or "close_ci_issue_as_superseded_after_review",
        }
    if triage.get("classification_recommendation") not in ACTIONABLE_CI_CLASSIFICATIONS:
        return {
            "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
            "generated_at": _utc_now(),
            "workflow_gate": "blocked_triage_incomplete_not_code_bug",
            "dry_run": not apply,
            "triage": triage,
            "triage_action": triage.get("triage_action")
            or "triage_incomplete_collect_failure_diagnostics_before_bug_promotion",
            "next_command": triage.get("next_command")
            or "rerun_triage_ci_issue_after_failure_diagnostics_are_available",
        }
    if apply and not create_registry_worktree:
        return {
            "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
            "generated_at": _utc_now(),
            "workflow_gate": "registry_worktree_required",
            "dry_run": False,
            "triage": triage,
            "blocking": [
                "promote-ci-issue --apply must use --create-registry-worktree so CI/Nightly intake cannot write BUG JSON from canonical root or main"
            ],
            "next_command": (
                f"python scripts/aistock_issue_workflow.py promote-ci-issue --issue {issue_number} "
                "--create-registry-worktree --apply"
            ),
        }
    if apply and create_registry_worktree:
        registry_pr_capability = _github_actions_registry_pr_capability()
        if not registry_pr_capability.get("allowed"):
            return {
                "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
                "generated_at": _utc_now(),
                "workflow_gate": "deferred_registry_pr_capability",
                "dry_run": False,
                "triage": triage,
                "registry_pr_capability": registry_pr_capability,
                "warnings": [
                    "Nightly failure remains an actionable GitHub Issue; BUG allocation is deferred until a registry PR can be persisted"
                ],
                "next_command": (
                    f"python scripts/aistock_issue_workflow.py promote-ci-issue --issue {issue_number} "
                    "--create-registry-worktree --apply"
                ),
            }
    summary = triage["summary"]
    suggested = triage["suggested_bug"]
    first_job = (summary.get("failed_jobs") or [{}])[0]
    failed_tests = first_job.get("failed_tests") or []
    error_signature = first_job.get("error_signature")
    details = ci_failure_summary.render_issue_markdown(summary, github_issue_number=issue_number)
    changed_files = list(suggested.get("allowed_write_scope") or [])
    evidence_refs = flow._unique_strings(
        [
            str(summary.get("run_url") or ""),
            _github_issue_url(issue_number),
            str(triage.get("failure_event_path") or ""),
            str(triage.get("context_pack_md_path") or ""),
        ]
    )
    plan = build_submit_bug_plan(
        title=suggested["title"],
        module=suggested["module"],
        severity=suggested["severity"],
        description=f"Auto-filed CI issue #{issue_number} requires actionable triage and repair.\n\n{details}",
        expected="CI/Nightly failure issues include enough diagnostic detail to enter the BUG JSON workflow without manual log rediscovery.",
        actual=f"Failure summary: {error_signature or (failed_tests[0] if failed_tests else 'diagnostic extraction incomplete')}",
        reproduce_command=str(summary.get("reproduce_command") or "Inspect linked CI run log."),
        evidence_refs=evidence_refs,
        changed_files=changed_files,
        plan_key="ci_failure_issue_intake",
        nox_session=first_job.get("nox_session"),
        candidate_type="regression",
        bug_id=bug_id,
        github_issue_number=str(issue_number),
        github_issue_url=_github_issue_url(issue_number),
        create_github=False,
        apply=apply,
        create_registry_worktree=create_registry_worktree,
        registry_pr_only=False,
    )
    return {
        "schema_version": "aistock_issue_workflow_promote_ci_issue_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "promoted" if apply else "ready_for_apply",
        "dry_run": not apply,
        "triage": triage,
        "submit_bug": plan,
        "next_command": plan.get("next_command"),
    }


def _nightly_payload_path(path_text: str) -> Path:
    path = Path(path_text)
    return path if path.is_absolute() else REPO_ROOT / path


def _load_nightly_candidate_issue_payload(path_text: str) -> dict[str, Any]:
    path = _nightly_payload_path(path_text)
    if not path.exists():
        raise WorkflowError(f"Nightly candidate issue payload not found: {path}")
    payload = _load_json(path)
    if payload.get("schema_version") != NIGHTLY_BUG_CANDIDATE_ISSUE_PAYLOAD_SCHEMA:
        raise WorkflowError(
            "Nightly candidate issue payload schema mismatch: "
            f"{payload.get('schema_version')!r}"
        )
    candidate = payload.get("candidate")
    if not isinstance(candidate, dict):
        raise WorkflowError("Nightly candidate issue payload is missing candidate object")
    payload["_source_path"] = str(path)
    return payload


def _load_nightly_candidate_issue_payloads(
    *,
    issue_payload: list[str] | None = None,
    queue_manifest: str | None = None,
) -> list[dict[str, Any]]:
    payload_paths = list(issue_payload or [])
    if queue_manifest:
        manifest_path = _nightly_payload_path(queue_manifest)
        manifest = _load_json(manifest_path)
        for ref in manifest.get("issue_payload_refs") or []:
            if not str(ref or "").strip():
                continue
            ref_path = Path(str(ref))
            payload_paths.append(str(ref_path if ref_path.is_absolute() else REPO_ROOT / ref_path))
    if not payload_paths:
        raise WorkflowError("--issue-payload or --queue-manifest is required")
    seen: set[str] = set()
    payloads: list[dict[str, Any]] = []
    for path_text in payload_paths:
        normalized = str(_nightly_payload_path(path_text).resolve())
        if normalized in seen:
            continue
        seen.add(normalized)
        payloads.append(_load_nightly_candidate_issue_payload(path_text))
    return payloads


def _as_str_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item or "").strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item or "").strip()]
    text = str(value or "").strip()
    return [text] if text else []


def _nightly_candidate_quality_blocking(
    issue_payload: dict[str, Any],
    *,
    apply: bool,
    opt_in_auto_file: bool,
    create_registry_worktree: bool,
    create_fix_worktree: bool,
) -> list[str]:
    candidate = issue_payload.get("candidate") if isinstance(issue_payload.get("candidate"), dict) else {}
    quality = candidate.get("quality_gate") if isinstance(candidate.get("quality_gate"), dict) else {}
    blocking: list[str] = []
    if apply and not (create_registry_worktree or create_fix_worktree):
        blocking.append(
            "promote-nightly-candidate --apply must use --create-registry-worktree or --create-fix-worktree to avoid canonical root BUG JSON writes"
        )
    if issue_payload.get("mode") not in {"draft_only", "ready_for_auto_file"}:
        blocking.append(f"unsupported issue payload mode: {issue_payload.get('mode')!r}")
    if quality.get("issue_payload_ready") is not True:
        blocking.append("candidate quality_gate.issue_payload_ready is not true")
    try:
        confidence = float(candidate.get("confidence"))
    except (TypeError, ValueError):
        confidence = -1.0
    if confidence < NIGHTLY_BUG_CANDIDATE_READY_THRESHOLD:
        blocking.append(f"candidate confidence {confidence} is below {NIGHTLY_BUG_CANDIDATE_READY_THRESHOLD}")
    if quality.get("auto_submit_allowed") not in {False, None} and quality.get("auto_submit_allowed") is not True:
        blocking.append("candidate quality_gate.auto_submit_allowed is invalid")
    source_anomaly = candidate.get("source_anomaly") if isinstance(candidate.get("source_anomaly"), dict) else {}
    if source_anomaly.get("synthetic") is True:
        blocking.append("synthetic nightly candidates cannot be promoted")
    gates = candidate.get("production_gates") if isinstance(candidate.get("production_gates"), dict) else {}
    if any(value != "noop" for value in gates.values()):
        blocking.append("nightly candidate requires non-noop production gates")
    for field in ("title", "module", "severity", "expected", "actual"):
        if not str(candidate.get(field) or "").strip():
            blocking.append(f"candidate missing {field}")
    if not candidate.get("reproduce"):
        blocking.append("candidate missing reproduce")
    if not candidate.get("evidence_refs"):
        blocking.append("candidate missing evidence_refs")
    if not candidate.get("allowed_write_scope"):
        blocking.append("candidate missing allowed_write_scope")
    return blocking


def _github_search_issue_by_marker(marker: str) -> dict[str, Any] | None:
    if not marker.strip():
        return None
    result = _run_command(
        [
            "gh",
            "issue",
            "list",
            "--repo",
            GITHUB_REPO,
            "--state",
            "all",
            "--search",
            marker,
            "--limit",
            "5",
            "--json",
            "number,url,state,title",
        ],
        timeout=60,
    )
    if not result.get("ok"):
        return None
    try:
        issues = json.loads(str(result.get("stdout") or "[]"))
    except json.JSONDecodeError:
        return None
    for issue in issues if isinstance(issues, list) else []:
        if isinstance(issue, dict) and issue.get("number"):
            return issue
    return None


def _nightly_extra_issue_sections(candidate: dict[str, Any], issue_payload: dict[str, Any]) -> list[str]:
    lines: list[str] = [
        "## Suggested Validation",
        "",
        *[f"- `{cmd}`" for cmd in candidate.get("suggested_validation") or []],
        "",
        "## CodeGraph / Understand Anything Refs",
        "",
        *[f"- `{ref}`" for ref in (candidate.get("codegraph_refs") or []) + (candidate.get("ua_refs") or [])],
        "",
        "## Dedupe Fingerprint",
        "",
        f"`{candidate.get('dedupe_fingerprint') or candidate.get('fingerprint')}`",
        str((issue_payload.get("dedupe") or {}).get("marker") or "").strip(),
    ]
    return ["\n".join(line for line in lines if line is not None)]


def _first_reproduce_command(candidate: dict[str, Any]) -> str:
    reproduce = candidate.get("reproduce")
    if isinstance(reproduce, list) and reproduce:
        return str(reproduce[0])
    return str(reproduce or "Review nightly discovery candidate payload.")


def _first_nox_session(candidate: dict[str, Any]) -> str | None:
    for command in candidate.get("suggested_validation") or []:
        match = re.search(r"\bnox\s+-s\s+([A-Za-z0-9_.-]+)", str(command))
        if match:
            return match.group(1)
    return None


def build_promote_nightly_candidate_plan(
    *,
    issue_payload: list[str] | None = None,
    queue_manifest: str | None = None,
    apply: bool,
    opt_in_auto_file: bool = False,
    bug_id: str | None = None,
    create_registry_worktree: bool = False,
    create_fix_worktree: bool = False,
    skip_dedupe_search: bool = False,
) -> dict[str, Any]:
    payloads = _load_nightly_candidate_issue_payloads(
        issue_payload=issue_payload,
        queue_manifest=queue_manifest,
    )
    if len(payloads) != 1:
        evaluated = []
        for payload in payloads:
            candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}
            evaluated.append(
                {
                    "candidate_id": candidate.get("candidate_id") or payload.get("candidate_id"),
                    "payload": _repo_rel(Path(str(payload.get("_source_path")))) if payload.get("_source_path") else None,
                    "quality_gate": (candidate.get("quality_gate") or {}).get("workflow_gate")
                    if isinstance(candidate.get("quality_gate"), dict)
                    else None,
                }
            )
        return {
            "schema_version": "aistock_issue_workflow_promote_nightly_candidate_v1",
            "generated_at": _utc_now(),
            "workflow_gate": "candidate_selection_required",
            "dry_run": not apply,
            "candidate_count": len(payloads),
            "evaluated_candidates": evaluated,
            "blocking": ["exactly one Nightly candidate issue payload must be selected for promotion"],
            "next_command": "python scripts/aistock_issue_workflow.py promote-nightly-candidate --issue-payload <payload-json> --create-registry-worktree --apply",
            "production_gates": _production_gates_payload(),
        }

    issue_payload_obj = payloads[0]
    candidate = issue_payload_obj["candidate"]
    candidate_id = str(candidate.get("candidate_id") or issue_payload_obj.get("candidate_id") or "unknown")
    quality = candidate.get("quality_gate") if isinstance(candidate.get("quality_gate"), dict) else {}
    dedupe = issue_payload_obj.get("dedupe") if isinstance(issue_payload_obj.get("dedupe"), dict) else {}
    marker = str(dedupe.get("marker") or "")
    existing_issue = None if skip_dedupe_search else _github_search_issue_by_marker(marker)
    blocking = _nightly_candidate_quality_blocking(
        issue_payload_obj,
        apply=apply,
        opt_in_auto_file=opt_in_auto_file,
        create_registry_worktree=create_registry_worktree,
        create_fix_worktree=create_fix_worktree,
    )
    if existing_issue:
        blocking.append(f"dedupe marker already exists in GitHub Issue #{existing_issue.get('number')}")
    if blocking:
        return {
            "schema_version": "aistock_issue_workflow_promote_nightly_candidate_v1",
            "generated_at": _utc_now(),
            "workflow_gate": "blocked",
            "dry_run": not apply,
            "candidate_id": candidate_id,
            "candidate_confidence": candidate.get("confidence"),
            "candidate_module": candidate.get("module"),
            "candidate_severity": candidate.get("severity"),
            "promotion_mode": "llm_enhanced_opt_in" if opt_in_auto_file else "deterministic_quality_gate",
            "llm_enhancement_opt_in": opt_in_auto_file,
            "quality_gate": quality,
            "dedupe": {
                "fingerprint": candidate.get("dedupe_fingerprint") or candidate.get("fingerprint"),
                "marker": marker,
                "issue_already_exists": bool(existing_issue),
                "existing_issue": existing_issue,
            },
            "blocking": blocking,
            "next_command": (
                f"python scripts/aistock_issue_workflow.py promote-nightly-candidate --issue-payload \"{issue_payload_obj.get('_source_path')}\" "
                "--create-registry-worktree --apply"
            ),
            "production_gates": _production_gates_payload(),
        }

    evidence_refs = flow._unique_strings(
        [
            *_as_str_list(candidate.get("evidence_refs")),
            *_as_str_list(candidate.get("codegraph_refs")),
            *_as_str_list(candidate.get("ua_refs")),
            str(issue_payload_obj.get("_source_path") or ""),
        ]
    )
    labels = flow._unique_strings(
        [str(item) for item in issue_payload_obj.get("labels") or [] if str(item or "").strip()]
        + ["nightly-discovery", "needs-triage"]
    )
    plan = build_submit_bug_plan(
        title=str(candidate.get("title") or issue_payload_obj.get("title") or "Nightly discovery candidate"),
        module=str(candidate.get("module") or "validation.runner"),
        severity=str(candidate.get("severity") or "P2"),
        description=str(candidate.get("summary") or issue_payload_obj.get("body") or candidate.get("title") or ""),
        expected=str(candidate.get("expected") or "Nightly discovery should not report this anomaly in a healthy workspace."),
        actual=str(candidate.get("actual") or candidate.get("summary") or "Nightly discovery reported an anomaly."),
        reproduce_command=_first_reproduce_command(candidate),
        evidence_refs=evidence_refs,
        changed_files=_as_str_list(candidate.get("allowed_write_scope")),
        plan_key=str(candidate.get("source_plan_key") or "nightly_bug_candidate_queue"),
        nox_session=_first_nox_session(candidate),
        candidate_type="regression",
        bug_id=bug_id,
        github_issue_number=None,
        github_issue_url=None,
        create_github=True,
        apply=apply,
        create_registry_worktree=create_registry_worktree,
        create_fix_worktree=create_fix_worktree,
        registry_pr_only=False,
        dry_run=False,
        github_issue_extra_sections=_nightly_extra_issue_sections(candidate, issue_payload_obj),
        extra_github_labels=labels,
    )
    if apply and create_registry_worktree and not create_fix_worktree and plan.get("bug_id"):
        registry_root = Path(str(plan.get("registry_root") or REPO_ROOT))
        registry_commit = _commit_bug_registration_in_fix_worktree(registry_root, str(plan["bug_id"]))
        absolute_issue_json = registry_root / str(plan.get("bug_json_path") or "")
        next_command = (
            f"python scripts/aistock_issue_workflow.py run --bug-id {plan['bug_id']} "
            f"--issue-json \"{absolute_issue_json}\" --mode plan --create-worktree"
        )
        plan["nightly_registry_commit"] = registry_commit
        plan["next_command"] = next_command
        if isinstance(plan.get("fix_chain"), dict):
            plan["fix_chain"]["next_command"] = next_command
            plan["fix_chain"]["run_next_command"] = next_command
    source_path = str(issue_payload_obj.get("_source_path") or "")
    apply_next_command = (
        f"python scripts/aistock_issue_workflow.py promote-nightly-candidate --issue-payload \"{source_path}\" "
        "--create-registry-worktree --apply"
    )
    return {
        "schema_version": "aistock_issue_workflow_promote_nightly_candidate_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "promoted" if apply else "ready_for_apply",
        "dry_run": not apply,
        "candidate_id": candidate_id,
        "candidate_confidence": candidate.get("confidence"),
        "candidate_module": candidate.get("module"),
        "candidate_severity": candidate.get("severity"),
        "promotion_mode": "llm_enhanced_opt_in" if opt_in_auto_file else "deterministic_quality_gate",
        "llm_enhancement_opt_in": opt_in_auto_file,
        "quality_gate": quality,
        "source_payload": _repo_rel(Path(source_path)) if source_path else None,
        "dedupe": {
            "fingerprint": candidate.get("dedupe_fingerprint") or candidate.get("fingerprint"),
            "marker": marker,
            "issue_already_exists": False,
        },
        "github_issue_number": (plan.get("github") or {}).get("number"),
        "github_issue_url": (plan.get("github") or {}).get("url"),
        "submit_bug": plan,
        "next_command": plan.get("next_command") if apply else apply_next_command,
        "production_gates": _production_gates_payload(),
    }


def _next_command_for_state(bug_id: str, state: dict[str, Any]) -> str:
    current = str(state.get("state") or "")
    worktree = str(state.get("worktree") or state.get("cwd") or "").strip()
    state_worktree = str(state.get("worktree") or "").strip()
    has_missing_state_worktree = bool(state_worktree and not Path(state_worktree).exists())
    prefix = f"cd /d {worktree} && " if worktree and not has_missing_state_worktree else ""
    if (state.get("planned_worktree") and not state.get("worktree")) or has_missing_state_worktree:
        return f"python scripts/aistock_issue_workflow.py run --bug-id {bug_id} --mode plan --create-worktree"
    if current in {"context_ready", "fix_in_progress"}:
        return f"{prefix}python scripts/aistock_issue_workflow.py finish --bug-id {bug_id} --plan-only"
    if current in {"validation_planned", "blocked"}:
        return f"{prefix}python scripts/aistock_issue_workflow.py finish --bug-id {bug_id} --validation-evidence \"<command> -> passed\""
    if current == "validation_passed":
        return (
            f"{prefix}git status --short && git add <task files> && git commit -m \"fix: resolve {bug_id}\" && "
            f"python scripts/aistock_issue_workflow.py run --bug-id {bug_id} --mode pr "
            "--validation-evidence \"<command> -> passed\" --push --create-pr"
        )
    if current in {"pr_opened", "ci_running"}:
        pr_url = str(state.get("pr_url") or "<PR_URL>")
        return f"{prefix}python scripts/aistock_issue_workflow.py watch-ci --bug-id {bug_id} --pr-url {pr_url}"
    return f"{prefix}python scripts/aistock_issue_workflow.py run --bug-id {bug_id} --mode plan"


def _execute_checked(args: list[str], *, cwd: Path | None = None, timeout: int = 120) -> dict[str, Any]:
    result = _run_command(args, cwd=cwd, timeout=timeout)
    if not result.get("ok"):
        raise WorkflowError(result.get("stderr") or result.get("stdout") or f"command failed: {' '.join(args)}")
    return result


def _is_reparse_or_symlink(path: Path) -> bool:
    try:
        if path.is_symlink():
            return True
        is_junction = getattr(path, "is_junction", None)
        if callable(is_junction) and is_junction():
            return True
        attrs = getattr(path.stat(follow_symlinks=False), "st_file_attributes", 0)
        return bool(attrs & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))
    except OSError:
        return False


def _normalize_worktree_artifact_path(value: str) -> str:
    normalized = str(value or "").replace("\\", "/").strip()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _worktree_transient_root(
    relative_path: str,
    *,
    worktree_path: Path,
    canonical_root: Path,
) -> tuple[str | None, str]:
    rel = _normalize_worktree_artifact_path(relative_path)
    rel_path = Path(rel)
    if not rel or rel_path.is_absolute() or ".." in rel_path.parts:
        return None, "invalid_path"
    parts = rel.split("/")
    if rel.startswith(WORKTREE_TRANSIENT_PREFIXES):
        if rel.startswith("var/research_assistant/"):
            return "var/research_assistant", "task_local_runtime_artifact"
        return "/".join(parts[:2]), "task_temporary_artifact"
    for index, part in enumerate(parts):
        if part in WORKTREE_TRANSIENT_CACHE_DIRS or part == ".next" or part.startswith(".next-"):
            return "/".join(parts[: index + 1]), "reproducible_cache"
    if rel in WORKTREE_TRANSIENT_EXACT_FILES or rel.startswith(".coverage.") or rel.endswith((".pyc", ".pyo")):
        return rel, "reproducible_cache_file"
    if rel == "proxy_config.json":
        candidate = worktree_path / rel
        canonical = canonical_root / rel
        if candidate.is_file() and canonical.is_file() and hashlib.sha256(candidate.read_bytes()).digest() == hashlib.sha256(canonical.read_bytes()).digest():
            return rel, "canonical_equivalent_local_config"
        return None, "non_equivalent_local_config"
    return None, "unknown_ignored_artifact"


def _validated_qe_live_log_transient_paths(
    ignored_paths: Iterable[str],
    *,
    worktree_path: Path,
) -> tuple[set[str], str]:
    prefix = WORKTREE_QE_LIVE_LOG_ROOT + "/"
    observed = {
        _normalize_worktree_artifact_path(item)
        for item in ignored_paths
        if _normalize_worktree_artifact_path(item).startswith(prefix)
    }
    if not observed:
        return set(), "qe_live_log_ring_not_present"
    if observed != WORKTREE_QE_LIVE_LOG_PATHS:
        return set(), "qe_live_log_ring_inventory_mismatch"

    root = worktree_path / WORKTREE_QE_LIVE_LOG_ROOT
    if (
        not root.is_dir()
        or _is_reparse_or_symlink(root.parent)
        or _is_reparse_or_symlink(root)
    ):
        return set(), "qe_live_log_ring_unsafe_directory"

    for relative_path in sorted(WORKTREE_QE_LIVE_LOG_PATHS):
        candidate = worktree_path / relative_path
        try:
            if _is_reparse_or_symlink(candidate) or not candidate.is_file():
                return set(), "qe_live_log_ring_unsafe_file"
            if candidate.stat().st_size > WORKTREE_QE_LIVE_LOG_MAX_FILE_BYTES:
                return set(), "qe_live_log_ring_file_too_large"
            with candidate.open("r", encoding="utf-8", errors="strict") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    if not isinstance(record, dict) or record.get("schema_version") != WORKTREE_QE_LIVE_LOG_SCHEMA:
                        return set(), "qe_live_log_ring_schema_mismatch"
        except (OSError, UnicodeError, json.JSONDecodeError):
            return set(), "qe_live_log_ring_invalid_jsonl"
    return set(WORKTREE_QE_LIVE_LOG_PATHS), "bounded_non_authoritative_qe_live_log_ring"


def _validated_backend_lifespan_log_transient_paths(
    ignored_paths: Iterable[str],
    *,
    worktree_path: Path,
) -> tuple[set[str], str]:
    prefix = WORKTREE_BACKEND_LOG_ROOT + "/"
    observed = {
        _normalize_worktree_artifact_path(item)
        for item in ignored_paths
        if _normalize_worktree_artifact_path(item).startswith(prefix)
    }
    if not observed:
        return set(), "backend_lifespan_logs_not_present"
    allowed = set(WORKTREE_BACKEND_LOG_LIMITS)
    if not observed.issubset(allowed):
        return set(), "backend_lifespan_log_inventory_mismatch"

    root = worktree_path / WORKTREE_BACKEND_LOG_ROOT
    if not root.is_dir() or _is_reparse_or_symlink(root.parent) or _is_reparse_or_symlink(root):
        return set(), "backend_lifespan_log_unsafe_directory"
    for relative_path in sorted(observed):
        candidate = worktree_path / relative_path
        try:
            if _is_reparse_or_symlink(candidate) or not candidate.is_file():
                return set(), "backend_lifespan_log_unsafe_file"
            if candidate.stat().st_size > WORKTREE_BACKEND_LOG_LIMITS[relative_path]:
                return set(), "backend_lifespan_log_file_too_large"
            with candidate.open("r", encoding="utf-8", errors="strict") as handle:
                for line in handle:
                    text = line.rstrip("\r\n")
                    if not text or not WORKTREE_BACKEND_LOG_LINE_RE.fullmatch(text):
                        return set(), "backend_lifespan_log_format_mismatch"
        except (OSError, UnicodeError):
            return set(), "backend_lifespan_log_unreadable"
    return observed, "bounded_test_created_backend_lifespan_log"


def _minimal_relative_roots(roots: Iterable[str]) -> list[str]:
    ordered = sorted({_normalize_worktree_artifact_path(item) for item in roots if item}, key=lambda item: (item.count("/"), item))
    minimal: list[str] = []
    for item in ordered:
        if any(item == parent or item.startswith(parent + "/") for parent in minimal):
            continue
        minimal.append(item)
    return minimal


def _cleanup_protected_receipt_paths(bug_id: str | None) -> set[str]:
    if not bug_id:
        return set()
    try:
        record, _source_path = find_bug_record(bug_id=bug_id, issue_json=None)
    except Exception:
        return set()
    runtime = record.get("runtime_contract") if isinstance(record.get("runtime_contract"), dict) else {}
    receipt_ref = _normalize_worktree_artifact_path(str(runtime.get("post_restart_receipt_ref") or ""))
    workflow_marker = WORKFLOW_ROOT.as_posix().rstrip("/") + "/"
    marker_index = receipt_ref.find(workflow_marker)
    if marker_index > 0:
        receipt_ref = receipt_ref[marker_index:]
    summary = runtime.get("post_restart_receipt_summary")
    required_summary_fields = (
        "receipt_sha256",
        "expected_identity",
        "observed_identity",
        "runtime_identity_proof_digest",
        "contract_digest",
        "catalog_sha256",
        "probe_evidence_digest",
    )
    summary_durable = (
        isinstance(summary, dict)
        and summary.get("schema_version") == RUNTIME_VERIFY_RECEIPT_SUMMARY_SCHEMA
        and all(bool(str(summary.get(field) or "").strip()) for field in required_summary_fields)
        and summary.get("post_restart_effective_gate") == "passed"
        and summary.get("response_content_persisted") is False
    )
    return {receipt_ref} if receipt_ref and not summary_durable else set()


def _cleanup_evidence_finalization_from_record(
    record: dict[str, Any],
    *,
    source: str,
    expected_bug_id: str | None,
) -> dict[str, Any]:
    observed_bug_id = str(record.get("bug_id") or "").strip().upper()
    canonical_expected = str(expected_bug_id or "").strip().upper()
    if canonical_expected and observed_bug_id != canonical_expected:
        return {
            "schema_version": "aistock_cleanup_evidence_finalization_v1",
            "status": "bug_record_identity_mismatch",
            "durable_receipt_present": False,
            "expected_bug_id": canonical_expected,
            "observed_bug_id": observed_bug_id or None,
            "bug_json": source,
        }
    evidence = [
        *flow._as_list(record.get("validation_receipts")),
        *flow._as_list(record.get("validation_evidence")),
    ]
    structured_receipt_present = any(flow._has_validation_receipt(item) for item in evidence)
    legacy_closure_present = bool(
        str(record.get("status") or "") in {"fixed", "verified"}
        and str(record.get("fix_commit") or "").strip()
        and str(record.get("pr_url") or "").strip()
        and evidence
    )
    durable_receipt_present = structured_receipt_present or legacy_closure_present
    return {
        "schema_version": "aistock_cleanup_evidence_finalization_v1",
        "status": (
            "finalized_structured_receipt"
            if structured_receipt_present
            else ("finalized_legacy_closed_bug" if legacy_closure_present else "missing_durable_receipt")
        ),
        "durable_receipt_present": durable_receipt_present,
        "structured_receipt_present": structured_receipt_present,
        "legacy_closure_present": legacy_closure_present,
        "bug_json": source,
        "evidence_item_count": len(evidence),
    }


def _cleanup_evidence_finalization(bug_id: str | None) -> dict[str, Any]:
    if not bug_id:
        return {
            "schema_version": "aistock_cleanup_evidence_finalization_v1",
            "status": "not_required_without_bug_record",
            "durable_receipt_present": True,
        }
    try:
        record, source_path = find_bug_record(bug_id=bug_id, issue_json=None)
    except Exception as exc:
        return {
            "schema_version": "aistock_cleanup_evidence_finalization_v1",
            "status": "bug_record_unavailable",
            "durable_receipt_present": False,
            "error": str(exc),
        }
    return _cleanup_evidence_finalization_from_record(
        record,
        source=_repo_rel(source_path),
        expected_bug_id=bug_id,
    )


def _worktree_active_process_profile(worktree_path: Path) -> dict[str, Any]:
    resolved = str(worktree_path.resolve())
    profile: dict[str, Any] = {
        "schema_version": "aistock_worktree_process_reference_v1",
        "target": resolved,
        "scan_status": "complete",
        "reference_count": 0,
        "references": [],
    }
    if os.name == "nt":
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if not powershell:
            profile.update({"scan_status": "unavailable", "error": "PowerShell unavailable"})
            return profile
        env = os.environ.copy()
        env["AISTOCK_CLEANUP_TARGET"] = resolved
        env["AISTOCK_CLEANUP_CALLER_PID"] = str(os.getpid())
        script = (
            "$target=$env:AISTOCK_CLEANUP_TARGET;"
            "$forward=$target.Replace('\\','/');"
            "$all=@(Get-CimInstance Win32_Process);"
            "$exclude=New-Object 'System.Collections.Generic.HashSet[int]';"
            "$null=$exclude.Add([int]$PID);$cursor=[int]$env:AISTOCK_CLEANUP_CALLER_PID;"
            "while($cursor -gt 0 -and $exclude.Add($cursor)){"
            "$row=$all|Where-Object{[int]$_.ProcessId -eq $cursor}|Select-Object -First 1;"
            "if($null -eq $row){break};$cursor=[int]$row.ParentProcessId};"
            "$hits=@($all|Where-Object{-not $exclude.Contains([int]$_.ProcessId)}|"
            "Where-Object{([string]$_.CommandLine).IndexOf($target,[StringComparison]::OrdinalIgnoreCase)-ge 0 -or "
            "([string]$_.CommandLine).IndexOf($forward,[StringComparison]::OrdinalIgnoreCase)-ge 0 -or "
            "([string]$_.ExecutablePath).IndexOf($target,[StringComparison]::OrdinalIgnoreCase)-ge 0}|"
            "Select-Object ProcessId,Name);$hits|ConvertTo-Json -Compress"
        )
        try:
            proc = subprocess.run(
                [powershell, "-NoProfile", "-Command", script],
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                check=False,
                timeout=30,
                env=env,
            )
            if proc.returncode != 0:
                profile.update({"scan_status": "failed", "error": (proc.stderr or proc.stdout).strip()})
                return profile
            raw = proc.stdout.strip()
            parsed = json.loads(raw) if raw else []
            references = parsed if isinstance(parsed, list) else [parsed]
            profile["references"] = references[:20]
            profile["reference_count"] = len(references)
            return profile
        except Exception as exc:
            profile.update({"scan_status": "failed", "error": str(exc)})
            return profile

    proc_root = Path("/proc")
    if not proc_root.exists():
        profile.update({"scan_status": "unsupported"})
        return profile
    references: list[dict[str, Any]] = []
    for item in proc_root.iterdir():
        if not item.name.isdigit() or int(item.name) in {os.getpid(), os.getppid()}:
            continue
        try:
            cwd = (item / "cwd").resolve()
            executable = (item / "exe").resolve()
            command_line = (item / "cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", errors="replace")
            target = worktree_path.resolve()
            matched = (
                cwd == target
                or target in cwd.parents
                or executable == target
                or target in executable.parents
                or str(target) in command_line
            )
            if matched:
                references.append({"process_id": int(item.name), "name": (item / "comm").read_text(encoding="utf-8").strip()})
        except (OSError, PermissionError):
            continue
    profile["references"] = references[:20]
    profile["reference_count"] = len(references)
    return profile


def _worktree_ignored_artifact_profile(
    worktree_path: Path,
    *,
    canonical_root: Path,
    protected_paths: set[str] | None = None,
) -> dict[str, Any]:
    protected = {_normalize_worktree_artifact_path(item) for item in (protected_paths or set()) if item}
    result = _run_command(
        ["git", "ls-files", "--others", "--ignored", "--exclude-standard", "-z"],
        cwd=worktree_path,
        timeout=120,
    )
    profile: dict[str, Any] = {
        "schema_version": "aistock_worktree_ignored_artifact_profile_v1",
        "scan_status": "complete" if result.get("ok") else "failed",
        "ignored_count": 0,
        "transient_count": 0,
        "protected_count": 0,
        "unknown_count": 0,
        "transient_roots": [],
        "transient_samples": [],
        "protected_samples": [],
        "unknown_samples": [],
        "manifest_sha256": None,
    }
    if not result.get("ok"):
        profile["error"] = result.get("stderr") or result.get("stdout") or "ignored artifact scan failed"
        return profile
    ignored = sorted({_normalize_worktree_artifact_path(item) for item in str(result.get("stdout") or "").split("\0") if item})
    qe_live_log_paths, qe_live_log_reason = _validated_qe_live_log_transient_paths(
        ignored,
        worktree_path=worktree_path,
    )
    qe_live_log_prefix = WORKTREE_QE_LIVE_LOG_ROOT + "/"
    backend_log_paths, backend_log_reason = _validated_backend_lifespan_log_transient_paths(
        ignored,
        worktree_path=worktree_path,
    )
    backend_log_prefix = WORKTREE_BACKEND_LOG_ROOT + "/"
    roots: list[str] = []
    transient_entries: list[tuple[str, str]] = []
    canonical_lines: list[str] = []
    for rel in ignored:
        if rel in protected:
            category, reason, root = "protected", "durable_receipt_not_finalized", None
            profile["protected_count"] += 1
            if len(profile["protected_samples"]) < 20:
                profile["protected_samples"].append(rel)
        else:
            if rel.startswith(qe_live_log_prefix):
                root = WORKTREE_QE_LIVE_LOG_ROOT if rel in qe_live_log_paths else None
                reason = qe_live_log_reason
            elif rel.startswith(backend_log_prefix):
                root = rel if rel in backend_log_paths else None
                reason = backend_log_reason
            else:
                root, reason = _worktree_transient_root(rel, worktree_path=worktree_path, canonical_root=canonical_root)
            if root:
                category = "transient"
                roots.append(root)
                transient_entries.append((rel, root))
                profile["transient_count"] += 1
                if len(profile["transient_samples"]) < 20:
                    profile["transient_samples"].append(rel)
            else:
                category = "unknown"
                profile["unknown_count"] += 1
                if len(profile["unknown_samples"]) < 20:
                    profile["unknown_samples"].append({"path": rel, "reason": reason})
        canonical_lines.append(f"{rel}\t{category}\t{reason}\t{root or ''}")
    minimal_roots = _minimal_relative_roots(roots)
    tracked_conflicts: list[str] = []
    for root in minimal_roots:
        tracked = _run_command(["git", "ls-files", "-z", "--", root], cwd=worktree_path, timeout=60)
        if not tracked.get("ok"):
            tracked_conflicts.append(root)
        elif str(tracked.get("stdout") or "").strip("\0"):
            tracked_conflicts.append(root)
    if tracked_conflicts:
        profile["unknown_count"] += len(tracked_conflicts)
        profile["unknown_samples"].extend(
            {"path": item, "reason": "transient_root_contains_tracked_files"}
            for item in tracked_conflicts[: max(0, 20 - len(profile["unknown_samples"]))]
        )
        minimal_roots = [item for item in minimal_roots if item not in set(tracked_conflicts)]
    profile["ignored_count"] = len(ignored)
    profile["transient_roots"] = minimal_roots
    profile["transient_root_count"] = len(minimal_roots)
    retained_transient_paths = sorted(
        rel
        for rel, _classified_root in transient_entries
        if any(rel == root or rel.startswith(root.rstrip("/") + "/") for root in minimal_roots)
    )
    profile["transient_manifest_sha256"] = hashlib.sha256(
        "\n".join(retained_transient_paths).encode("utf-8")
    ).hexdigest()
    profile["manifest_sha256"] = hashlib.sha256("\n".join(canonical_lines).encode("utf-8")).hexdigest()
    return profile


def _validated_transient_root_target(worktree_path: Path, relative_root: str) -> Path:
    normalized = _normalize_worktree_artifact_path(relative_root)
    rel = Path(normalized)
    if not normalized or rel.is_absolute() or ".." in rel.parts:
        raise WorkflowError(f"invalid transient cleanup root: {relative_root}")
    lexical_root = Path(os.path.abspath(worktree_path))
    target = Path(os.path.abspath(lexical_root / rel))
    try:
        target.relative_to(lexical_root)
    except ValueError as exc:
        raise WorkflowError(f"transient cleanup escaped worktree: {target}") from exc
    cursor = lexical_root
    for part in rel.parts[:-1]:
        cursor /= part
        if _is_reparse_or_symlink(cursor):
            raise WorkflowError(f"transient cleanup root crosses reparse point: {cursor}")
    return target


def _remove_exact_transient_root(worktree_path: Path, relative_root: str, *, target: Path | None = None) -> None:
    target = target or _validated_transient_root_target(worktree_path, relative_root)
    if not target.exists() and not _is_reparse_or_symlink(target):
        return
    if target.is_symlink():
        target.unlink()
    elif _is_reparse_or_symlink(target):
        if target.is_dir():
            target.rmdir()
        else:
            target.unlink()
    elif target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()


def _purge_worktree_transient_artifacts(
    worktree_path: Path,
    *,
    canonical_root: Path,
    expected_profile: dict[str, Any],
    protected_paths: set[str] | None = None,
) -> dict[str, Any]:
    del canonical_root, protected_paths  # The complete preflight profile is the authority for this purge.
    if expected_profile.get("scan_status") != "complete":
        raise WorkflowError(str(expected_profile.get("error") or "ignored artifact preflight was incomplete"))
    if expected_profile.get("protected_count") or expected_profile.get("unknown_count"):
        raise WorkflowError("ignored artifacts include protected or unknown files")
    transient_roots = [str(item) for item in expected_profile.get("transient_roots") or []]
    live = _run_command(
        ["git", "ls-files", "--others", "--ignored", "--exclude-standard", "-z", "--", *transient_roots],
        cwd=worktree_path,
        timeout=120,
    )
    if not live.get("ok"):
        raise WorkflowError(str(live.get("stderr") or live.get("stdout") or "targeted transient rescan failed"))
    live_paths = sorted(
        {_normalize_worktree_artifact_path(item) for item in str(live.get("stdout") or "").split("\0") if item}
    )
    live_digest = hashlib.sha256("\n".join(live_paths).encode("utf-8")).hexdigest()
    if live_digest != expected_profile.get("transient_manifest_sha256"):
        raise WorkflowError("ignored artifact manifest changed after cleanup preflight")
    tracked = _run_command(
        ["git", "ls-files", "-z", "--", *transient_roots],
        cwd=worktree_path,
        timeout=60,
    )
    if not tracked.get("ok") or str(tracked.get("stdout") or "").strip("\0"):
        raise WorkflowError("transient cleanup roots gained tracked files after cleanup preflight")
    validated_roots = [
        (str(relative_root), _validated_transient_root_target(worktree_path, str(relative_root)))
        for relative_root in transient_roots
    ]
    removed_roots: list[str] = []
    for relative_root, target in validated_roots:
        _remove_exact_transient_root(worktree_path, relative_root, target=target)
        removed_roots.append(relative_root)
    after = _run_command(
        ["git", "ls-files", "--others", "--ignored", "--exclude-standard", "-z", "--", *transient_roots],
        cwd=worktree_path,
        timeout=120,
    )
    after_paths = [item for item in str(after.get("stdout") or "").split("\0") if item]
    if not after.get("ok") or after_paths:
        raise WorkflowError("transient artifact purge did not leave the targeted roots empty")
    return {
        "ok": True,
        "schema_version": "aistock_worktree_transient_purge_v1",
        "manifest_sha256": expected_profile.get("manifest_sha256"),
        "transient_manifest_sha256": live_digest,
        "removed_root_count": len(removed_roots),
        "removed_roots": removed_roots[:50],
        "removed_roots_truncated": len(removed_roots) > 50,
        "ignored_count_before": len(live_paths),
        "ignored_count_after": 0,
        "scan_scope": "preflight_full_manifest_then_targeted_root_readback",
    }


def _cleanup_post_removal_verification(
    *,
    root: Path,
    worktree_path: Path | None,
    branch: str,
) -> dict[str, Any]:
    local_refs = set(
        _git(["for-each-ref", "--format=%(refname:short)", "refs/heads"], cwd=root, check=False).splitlines()
    )
    remote_result = _run_read_command_with_retry(
        ["git", "ls-remote", "--heads", "origin", branch],
        cwd=root,
        timeout=60,
    )
    remote_output = str(remote_result.get("stdout") or "") if remote_result.get("ok") else ""
    remote_check_ok = bool(remote_result.get("ok"))
    local_absent = branch not in local_refs
    remote_absent = remote_check_ok and not remote_output.strip()
    path_absent = worktree_path is None or not worktree_path.exists()
    registration_absent = worktree_path is None or not _path_is_registered_worktree(worktree_path, cwd=root)
    all_clear = bool(path_absent and registration_absent and local_absent and remote_absent)
    return {
        "schema_version": "aistock_worktree_cleanup_verification_v1",
        "path_absent": path_absent,
        "registration_absent": registration_absent,
        "local_branch_absent": local_absent,
        "remote_branch_absent": remote_absent,
        "remote_check_ok": remote_check_ok,
        "remote_check_attempts": remote_result.get("attempts"),
        "all_clear": all_clear,
    }


def _remote_branch_sha(remote_ref: str, branch: str) -> str | None:
    expected_name = f"refs/heads/{branch}"
    for line in str(remote_ref or "").splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[1] == expected_name and _FULL_GIT_COMMIT_RE.fullmatch(fields[0].lower()):
            return fields[0].lower()
    return None


def _delete_remote_branch_with_lease(
    *,
    root: Path,
    branch: str,
    expected_remote_ref: str,
) -> dict[str, Any]:
    expected_sha = _remote_branch_sha(expected_remote_ref, branch)
    if not expected_sha:
        raise WorkflowError(f"remote branch preflight identity is invalid: {branch}")
    observed = _run_transport_read_with_retry(
        ["git", "ls-remote", "--heads", "origin", branch],
        cwd=root,
        timeout=60,
        attempts=2,
    )
    if not observed.get("ok"):
        raise WorkflowError(
            observed.get("stderr")
            or observed.get("stdout")
            or f"cannot verify remote branch before cleanup: {branch}"
        )
    observed_remote_ref = str(observed.get("stdout") or "")
    observed_sha = _remote_branch_sha(observed_remote_ref, branch)
    if not observed_remote_ref.strip():
        return {
            "ok": True,
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "expected_sha": expected_sha,
            "already_absent": True,
            "readback_attempts": observed.get("attempts"),
        }
    if observed_sha != expected_sha:
        raise WorkflowError(
            f"remote branch changed after cleanup preflight: {branch} "
            f"expected={expected_sha} observed={observed_sha or 'absent'}"
        )
    result = _execute_checked(
        [
            "git",
            "push",
            "origin",
            "--delete",
            f"--force-with-lease=refs/heads/{branch}:{expected_sha}",
            branch,
        ],
        cwd=root,
        timeout=180,
    )
    return {**result, "expected_sha": expected_sha, "already_absent": False}


def _orphan_worktree_dir_profile(path: Path) -> dict[str, Any]:
    sample_limit = 20
    profile: dict[str, Any] = {
        "path": str(path),
        "inside_worktree_root": _is_inside(path, _default_worktree_root()) if path.exists() else False,
        "regular_entries": [],
        "reparse_entries": [],
        "empty_dirs": [],
        "regular_entry_count": 0,
        "reparse_entry_count": 0,
        "empty_dir_count": 0,
        "sample_limit": sample_limit,
        "top_regular_dirs": {},
        "missing": not path.exists(),
    }
    if not path.exists():
        profile["safe_reparse_or_empty_only"] = True
        return profile

    def add_sample(key: str, rel: str) -> None:
        count_key = {
            "regular_entries": "regular_entry_count",
            "reparse_entries": "reparse_entry_count",
            "empty_dirs": "empty_dir_count",
        }[key]
        profile[count_key] += 1
        if len(profile[key]) < sample_limit:
            profile[key].append(rel)
        if key == "regular_entries":
            top = rel.split("/", 1)[0] if "/" in rel else rel
            top_dirs = profile["top_regular_dirs"]
            top_dirs[top] = int(top_dirs.get(top, 0)) + 1

    def scan_dir(directory: Path) -> None:
        try:
            children = sorted(directory.iterdir(), key=lambda item: item.as_posix())
        except OSError:
            add_sample("regular_entries", _repo_rel(directory, path))
            return
        if not children and directory != path:
            add_sample("empty_dirs", _repo_rel(directory, path))
            return
        for child in children:
            rel = _repo_rel(child, path)
            if _is_reparse_or_symlink(child):
                add_sample("reparse_entries", rel)
            elif child.is_dir():
                scan_dir(child)
            else:
                add_sample("regular_entries", rel)

    scan_dir(path)
    profile["regular_entries_truncated"] = profile["regular_entry_count"] > len(profile["regular_entries"])
    profile["reparse_entries_truncated"] = profile["reparse_entry_count"] > len(profile["reparse_entries"])
    profile["empty_dirs_truncated"] = profile["empty_dir_count"] > len(profile["empty_dirs"])
    profile["top_regular_dirs"] = dict(
        sorted(
            profile["top_regular_dirs"].items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )[:10]
    )
    profile["safe_reparse_or_empty_only"] = bool(profile["inside_worktree_root"]) and not profile["regular_entry_count"]
    return profile


def _orphan_worktree_refusal_message(profile: dict[str, Any]) -> str:
    return (
        "refusing orphan worktree cleanup with regular files: "
        f"count={profile.get('regular_entry_count', 0)} "
        f"samples={profile.get('regular_entries') or []} "
        f"top_dirs={profile.get('top_regular_dirs') or {}} "
        "full file list intentionally omitted; close open processes or delete the orphan directory manually"
    )


def _remove_reparse_or_empty_tree(path: Path) -> dict[str, Any]:
    profile = _orphan_worktree_dir_profile(path)
    if not profile.get("safe_reparse_or_empty_only"):
        raise WorkflowError(_orphan_worktree_refusal_message(profile))
    removed: list[str] = []
    if not path.exists():
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": "", "profile": profile, "removed": removed}

    def remove_children(directory: Path) -> None:
        for child in sorted(directory.iterdir(), key=lambda item: item.as_posix(), reverse=True):
            if _is_reparse_or_symlink(child):
                if child.is_dir():
                    child.rmdir()
                else:
                    child.unlink()
                removed.append(_repo_rel(child, path))
            elif child.is_dir():
                remove_children(child)
                try:
                    child.rmdir()
                    removed.append(_repo_rel(child, path))
                except OSError:
                    pass

    remove_children(path)
    try:
        path.rmdir()
        removed.append(".")
    except PermissionError as exc:
        return {
            "ok": True,
            "returncode": 0,
            "stdout": "",
            "stderr": str(exc),
            "profile": profile,
            "removed": removed,
            "deferred": True,
            "deferred_reason": "empty_directory_locked_by_windows_handle",
        }
    return {
        "ok": True,
        "returncode": 0,
        "stdout": json.dumps({"removed": removed}, ensure_ascii=True),
        "stderr": "",
        "profile": profile,
        "removed": removed,
    }
def _looks_like_reparse_cleanup_failure(result: dict[str, Any]) -> bool:
    text = f"{result.get('stdout') or ''}\n{result.get('stderr') or ''}".lower()
    return any(token in text for token in ("invalid argument", "junction", "reparse", "node_modules"))


def _remove_worktree_with_reparse_fallback(*, root: Path, worktree_path: Path) -> dict[str, Any]:
    result = _run_command(["git", "worktree", "remove", str(worktree_path)], cwd=root, timeout=120)
    payload: dict[str, Any] = {
        "ok": bool(result.get("ok")),
        "primary": result,
        "fallback_used": False,
    }
    if result.get("ok") or not worktree_path.exists():
        return payload
    if not _looks_like_reparse_cleanup_failure(result):
        raise WorkflowError(result.get("stderr") or result.get("stdout") or f"git worktree remove failed: {worktree_path}")
    fallback = _remove_reparse_or_empty_tree(worktree_path)
    prune = _run_command(["git", "worktree", "prune"], cwd=root, timeout=60)
    payload.update(
        {
            "ok": not worktree_path.exists(),
            "fallback_used": True,
            "fallback_reason": "git_worktree_remove_left_reparse_or_empty_tree",
            "fallback": fallback,
            "prune": prune,
        }
    )
    if not payload["ok"]:
        raise WorkflowError(f"worktree reparse fallback did not remove {worktree_path}")
    return payload


def _check_name(item: dict[str, Any]) -> str:
    return str(item.get("name") or item.get("workflowName") or item.get("__typename") or "unknown")


def _classify_pr_checks(checks: list[dict[str, Any]]) -> dict[str, list[str]]:
    failed: list[str] = []
    pending: list[str] = []
    non_blocking: list[str] = []
    passed: list[str] = []
    for item in checks:
        name = _check_name(item)
        status = str(item.get("status") or "").upper()
        conclusion = str(item.get("conclusion") or "").upper()
        if status != "COMPLETED":
            pending.append(name)
        elif conclusion in NON_BLOCKING_CHECK_CONCLUSIONS:
            passed.append(name)
            if conclusion in {"NEUTRAL", "SKIPPED"}:
                non_blocking.append(name)
        elif conclusion:
            failed.append(name)
    return {
        "failed": failed,
        "pending": pending,
        "non_blocking": non_blocking,
        "passed": passed,
    }


def _required_pr_check_summary(result: dict[str, Any]) -> dict[str, list[str]]:
    """Classify the repository-owned merge-quality contract."""
    raw = str(result.get("stdout") or "").strip()
    if not raw:
        if result.get("ok"):
            checks: list[dict[str, Any]] = []
        else:
            raise WorkflowError(
                str(result.get("stderr") or "required PR check query failed without a result").strip()
            )
    else:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise WorkflowError(f"required PR check query returned invalid JSON: {exc}") from exc
        if not isinstance(parsed, list):
            raise WorkflowError("required PR check query returned a non-list payload")
        checks = [item for item in parsed if isinstance(item, dict)]

    failed: list[str] = []
    pending: list[str] = []
    non_blocking: list[str] = []
    passed: list[str] = []
    for item in checks:
        name = _check_name(item)
        bucket = str(item.get("bucket") or "").lower()
        if bucket == "pass":
            passed.append(name)
        elif bucket == "skipping":
            passed.append(name)
            non_blocking.append(name)
        elif bucket == "pending":
            pending.append(name)
        else:
            # fail, cancel, and unknown buckets must all fail closed.
            failed.append(name)
    return {
        "failed": failed,
        "pending": pending,
        "non_blocking": non_blocking,
        "passed": passed,
    }


def _merge_quality_contexts_for_head_ref(head_ref: str | None) -> tuple[str, ...]:
    branch = str(head_ref or "").strip()
    if re.match(r"^chore/BUG-\d+-close-sync(?:-|$)", branch):
        return ("CI verdict",)
    return MERGE_QUALITY_CHECK_CONTEXTS


def _normalize_merge_quality_check_result(
    result: dict[str, Any],
    *,
    required_contexts: tuple[str, ...] = MERGE_QUALITY_CHECK_CONTEXTS,
) -> dict[str, Any] | None:
    """Select the stable merge contract and synthesize missing checks as pending.

    ``gh pr checks`` returns every check and exits non-zero when any check fails.
    The merge contract must neither inherit unrelated advisory failures nor rely
    on a potentially stale branch-protection subset, so this function evaluates
    only the repository-owned stable contexts.
    """

    raw = str(result.get("stdout") or "").strip()
    if not raw:
        if not result.get("ok"):
            return None
        parsed: Any = []
    else:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
    if not isinstance(parsed, list):
        return None
    checks = [item for item in parsed if isinstance(item, dict)]
    rows: list[dict[str, Any]] = []
    for context in required_contexts:
        matches = [item for item in checks if _check_name(item) == context]
        if matches:
            rows.append(matches[-1])
        else:
            rows.append(
                {
                    "name": context,
                    "state": "pending",
                    "bucket": "pending",
                    "workflow": "aistock-merge-quality-contract",
                }
            )
    normalized = dict(result)
    normalized.update(
        {
            "ok": True,
            "returncode": 0,
            "stdout": json.dumps(rows),
            "source": "github_cli_merge_quality_contract",
        }
    )
    return normalized


def _execute_workflow_command(
    bug_id: str,
    args: list[str],
    *,
    state: str,
    cwd: Path | None = None,
    timeout: int = 120,
    event: str | None = None,
    root: Path | None = None,
    allow_failure: bool = False,
) -> dict[str, Any]:
    started = time.monotonic()
    result = _run_command(args, cwd=cwd, timeout=timeout)
    duration = time.monotonic() - started
    _append_event(
        bug_id,
        event=event or f"command:{args[0]}",
        state=state,
        command=" ".join(args),
        cwd=cwd,
        root=root,
        duration_seconds=duration,
        result="ok" if result.get("ok") else "failed",
        evidence={
            "returncode": result.get("returncode"),
            "stdout_excerpt": str(result.get("stdout") or "")[:1000],
            "stderr_excerpt": str(result.get("stderr") or "")[:1000],
        },
    )
    if not result.get("ok") and not allow_failure:
        raise WorkflowError(result.get("stderr") or result.get("stdout") or f"command failed: {' '.join(args)}")
    return result


def _path_is_artifact(path: str) -> bool:
    normalized = path.replace("\\", "/").strip("/")
    parts = [part for part in normalized.split("/") if part]
    return any(pattern in parts or normalized == pattern or normalized.startswith(pattern + "/") for pattern in ARTIFACT_PATH_PATTERNS)


def _git_status_paths(root: Path) -> list[dict[str, str]]:
    result = _run_command(["git", "status", "--porcelain=v1", "-uall"], cwd=root, timeout=30)
    if not result.get("ok"):
        return []
    rows: list[dict[str, str]] = []
    for line in str(result.get("stdout") or "").splitlines():
        if len(line) < 4:
            continue
        status = line[:2]
        raw_path = _parse_git_porcelain_path(line)
        rows.append({"status": status, "path": raw_path})
    return rows


def _run_changed_file_lint(changed_files: list[str], *, root: Path) -> dict[str, Any]:
    python_files = [path for path in changed_files if path.endswith(".py") and (root / path).exists()]
    if not python_files:
        return {"status": "not_required", "python_files": [], "commands": []}
    candidates = [
        ["ruff", "check", *python_files],
        [sys.executable, "-m", "ruff", "check", *python_files],
    ]
    commands: list[dict[str, Any]] = []
    for command in candidates:
        result = _run_command(command, cwd=root, timeout=120)
        commands.append({"command": " ".join(command), "result": result})
        if result.get("ok"):
            return {"status": "passed", "python_files": python_files, "commands": commands}
        combined = f"{result.get('stdout')}\n{result.get('stderr')}".lower()
        if "no module named ruff" in combined or "not recognized" in combined or "no such file" in combined:
            continue
        return {"status": "failed", "python_files": python_files, "commands": commands}
    return {"status": "unavailable", "python_files": python_files, "commands": commands}


def _pre_pr_gate(
    *,
    finish: dict[str, Any],
    validation_evidence: list[str],
    root: Path,
    run_lint: bool = True,
    require_clean: bool = False,
) -> dict[str, Any]:
    changed_files = [str(item) for item in finish.get("changed_files") or []]
    scope_check = finish.get("scope_check") or {}
    fast_path = finish.get("fast_path") if isinstance(finish.get("fast_path"), dict) else {}
    ownership = fast_path.get("ownership") if isinstance(fast_path.get("ownership"), dict) else {}
    status_rows = _git_status_paths(root)
    artifact_rows = [row for row in status_rows if _path_is_artifact(row["path"])]
    task_dirty_rows = [row for row in status_rows if not _path_is_artifact(row["path"])]
    blocking: list[str] = []
    warnings: list[str] = []
    next_actions: list[str] = []
    if require_clean and task_dirty_rows:
        blocking.append(f"commit_required before push/PR: {[row['path'] for row in task_dirty_rows]}")
        next_actions.append('git add <task files> && git commit -m "fix: resolve issue"')
    elif task_dirty_rows:
        warnings.append(f"uncommitted task file(s) present: {[row['path'] for row in task_dirty_rows]}")
    if not validation_evidence:
        blocking.append("validation evidence is required before PR creation")
    if finish.get("closure_ready") is False:
        blocking.append("finish plan is not closure-ready")
    if scope_check.get("status") not in {None, "passed"}:
        blocking.append(f"scope check failed: {scope_check.get('violations') or scope_check.get('status')}")
    if ownership.get("unmapped_count"):
        blocking.append(f"ownership check failed: unmapped={ownership.get('unmapped') or ownership.get('unmapped_count')}")
    if ownership.get("ambiguous_count"):
        blocking.append(f"ownership check failed: ambiguous={ownership.get('ambiguous') or ownership.get('ambiguous_count')}")
    if artifact_rows:
        blocking.append(f"temporary/cache artifacts are present in git status: {[row['path'] for row in artifact_rows]}")
    lint = _run_changed_file_lint(changed_files, root=root) if run_lint else {"status": "skipped", "python_files": []}
    if lint.get("status") == "failed":
        blocking.append("changed-file Ruff lint failed")
    elif lint.get("status") == "unavailable" and lint.get("python_files"):
        warnings.append("Ruff is unavailable; run the required nox/pytest validation before PR")
    return {
        "schema_version": "aistock_issue_workflow_pre_pr_gate_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "passed" if not blocking else "blocked",
        "blocking": blocking,
        "warnings": warnings,
        "changed_files": changed_files,
        "scope_check": scope_check,
        "ownership_check": ownership,
        "artifact_guard": {
            "status": "passed" if not artifact_rows else "failed",
            "artifact_paths": artifact_rows,
            "patterns": list(ARTIFACT_PATH_PATTERNS),
        },
        "dirty_task_files": task_dirty_rows,
        "next_actions": next_actions,
        "lint": lint,
        "validation_evidence_present": bool(validation_evidence),
    }


def _current_branch(root: Path | None = None) -> str:
    branch = _git(["branch", "--show-current"], cwd=root or REPO_ROOT)
    if not branch:
        raise WorkflowError("current branch is empty; cannot create issue PR")
    if branch == "main":
        raise WorkflowError("refusing to create an issue PR from main")
    return branch


def _pr_worktree_guard(root: Path | None = None) -> dict[str, Any]:
    work_root = root or REPO_ROOT
    canonical = _canonical_root().resolve()
    git = _git_snapshot(work_root)
    blocking: list[str] = []
    warnings: list[str] = []
    target = work_root.resolve()
    if not git.get("ok"):
        blocking.append(str(git.get("error") or f"not a git checkout: {work_root}"))
    if target == canonical:
        blocking.append("refusing PR automation from canonical root; continue in the issue worktree")
    if git.get("branch") == "main":
        blocking.append("refusing PR automation from main; continue on the task branch")
    if git.get("dirty"):
        warnings.append(f"task worktree has {git.get('dirty_count')} uncommitted file(s); commit only task files before push/PR")
    return {
        "root": str(target),
        "canonical_root": str(canonical),
        "git": git,
        "blocking": blocking,
        "warnings": warnings,
    }


def _checks_summary_payload(checks_view: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "failed_count": len(checks_view.get("failed") or []),
        "pending_count": len(checks_view.get("pending") or []),
        "passed_count": len(checks_view.get("passed") or []),
        "non_blocking_count": len(checks_view.get("non_blocking") or []),
        "failed": list(checks_view.get("failed") or [])[:5],
        "pending": list(checks_view.get("pending") or [])[:5],
        "non_blocking": list(checks_view.get("non_blocking") or [])[:5],
    }


def _view_pr_checks(pr_url: str) -> dict[str, Any]:
    source = "github_graphql"
    view = _run_transport_read_with_retry(
        ["gh", "pr", "view", pr_url, "--json", "statusCheckRollup"],
        cwd=REPO_ROOT,
        timeout=60,
        attempts=2,
    )
    if not view.get("ok"):
        message = str(view.get("stderr") or view.get("stdout") or "PR checks unavailable")
        if not _looks_like_github_transport_failure(message):
            return {"workflow_gate": "checks_unavailable", "check_summary": {"failed_count": 0, "pending_count": 0, "passed_count": 0, "non_blocking_count": 0}, "error": message}
        try:
            readback = _github_pull_rest_readback(pr_url)
            check_runs = _run_transport_read_with_retry(
                ["gh", "api", f"repos/{GITHUB_REPO}/commits/{readback['head_sha']}/check-runs?per_page=100"],
                cwd=REPO_ROOT,
                timeout=30,
                attempts=2,
            )
            payload = _parse_rest_object(check_runs, context="PR check-runs readback")
            checks = [item for item in payload.get("check_runs") or [] if isinstance(item, dict)]
            if int(payload.get("total_count") or len(checks)) > len(checks):
                raise WorkflowError("PR check-runs readback exceeds the supported single page")
            source = "github_rest"
        except WorkflowError as exc:
            return {"workflow_gate": "checks_unavailable", "check_summary": {"failed_count": 0, "pending_count": 0, "passed_count": 0, "non_blocking_count": 0}, "error": str(exc)}
    else:
        try:
            checks = json.loads(str(view.get("stdout") or "{}")).get("statusCheckRollup") or []
        except json.JSONDecodeError as exc:
            return {"workflow_gate": "checks_unavailable", "check_summary": {"failed_count": 0, "pending_count": 0, "passed_count": 0, "non_blocking_count": 0}, "error": str(exc)}
    classified = _classify_pr_checks(checks)
    if not checks:
        gate = "checks_pending"
    elif classified["failed"]:
        gate = "checks_failed"
    elif classified["pending"]:
        gate = "checks_pending"
    else:
        gate = "checks_passed"
    return {"workflow_gate": gate, "check_summary": _checks_summary_payload(classified), "classified": classified, "raw_count": len(checks), "source": source}


def _watch_pr_checks_compact(bug_id: str, pr_url: str, *, attempts: int = 3, delay_seconds: int = 10) -> dict[str, Any]:
    attempts_payload: list[dict[str, Any]] = []
    latest = {"workflow_gate": "checks_pending", "check_summary": {"failed_count": 0, "pending_count": 0, "passed_count": 0, "non_blocking_count": 0}}
    for index in range(1, attempts + 1):
        latest = _view_pr_checks(pr_url)
        attempts_payload.append({"attempt": index, "workflow_gate": latest.get("workflow_gate"), "check_summary": latest.get("check_summary")})
        if latest.get("workflow_gate") in {"checks_passed", "checks_failed"}:
            break
        if index < attempts:
            time.sleep(delay_seconds)
    gate = str(latest.get("workflow_gate") or "checks_pending")
    next_actions = ["merge_only_if_user_authorized"] if gate == "checks_passed" else ["rerun_checks_after_github_reports_jobs"]
    if gate == "checks_failed":
        next_actions = ["inspect_failed_ci_jobs", "fix_on_same_task_branch"]
    payload = {
        "schema_version": "aistock_issue_workflow_check_watch_v1",
        "workflow_gate": gate,
        "pr_url": pr_url,
        "attempts": attempts_payload,
        "check_summary": latest.get("check_summary"),
        "next_actions": next_actions,
    }
    _append_event(
        bug_id,
        event="command:gh_pr_checks_compact",
        state="ci_running" if gate == "checks_pending" else ("ci_green" if gate == "checks_passed" else "blocked"),
        command=f"gh pr view {pr_url} --json statusCheckRollup",
        cwd=REPO_ROOT,
        result=gate,
        evidence={"check_summary": payload["check_summary"]},
    )
    return payload


def build_watch_ci_plan(
    *,
    bug_id: str,
    pr_url: str | None = None,
    attempts: int = 1,
    delay_seconds: int = 0,
) -> dict[str, Any]:
    canonical_bug_id = bug_id.strip().upper()
    state = _load_state(canonical_bug_id) or {}
    resolved_pr_url = pr_url or str(state.get("pr_url") or "").strip()
    if not resolved_pr_url:
        raise WorkflowError("--pr-url is required when workflow state does not include a PR URL")
    attempts = max(1, attempts)
    delay_seconds = max(0, delay_seconds)
    ci_watch = _watch_pr_checks_compact(
        canonical_bug_id,
        resolved_pr_url,
        attempts=attempts,
        delay_seconds=delay_seconds,
    )
    gate = str(ci_watch.get("workflow_gate") or "checks_pending")
    check_ok = gate == "checks_passed"
    _write_state(
        canonical_bug_id,
        state="ci_green" if check_ok else ("blocked" if gate == "checks_failed" else "ci_running"),
        pr_url=resolved_pr_url,
        checks=ci_watch.get("check_summary"),
        next_actions=ci_watch.get("next_actions") or [],
        stop_reason=None if check_ok else gate,
    )
    return {
        "schema_version": "aistock_issue_workflow_watch_ci_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "pr_url": resolved_pr_url,
        "workflow_gate": gate,
        "check_summary": ci_watch.get("check_summary"),
        "next_actions": ci_watch.get("next_actions") or [],
        "state": "ci_green" if check_ok else ("blocked" if gate == "checks_failed" else "ci_running"),
    }


def _maybe_create_pr(
    *,
    bug_id: str,
    finish: dict[str, Any],
    push: bool,
    create_pr: bool,
    watch_ci: bool,
    pr_title: str | None,
) -> dict[str, Any]:
    actions: list[dict[str, Any]] = []
    pr_url: str | None = None
    guard = _pr_worktree_guard()
    if guard["blocking"]:
        raise WorkflowError("; ".join(guard["blocking"]))
    branch = _current_branch()
    pre_pr_gate = _pre_pr_gate(
        finish=finish,
        validation_evidence=finish.get("validation_evidence") or [],
        root=REPO_ROOT,
        require_clean=bool(push or create_pr),
    )
    if not (push or create_pr or watch_ci):
        return {
            "branch": branch,
            "dry_run": True,
            "worktree_guard": guard,
            "pre_pr_gate": pre_pr_gate,
            "next_commands": [
                f"git push -u origin {branch}",
                f"gh pr create --base main --head {branch} --title \"{pr_title or bug_id + ' issue workflow fix'}\" --body-file {finish.get('pr_body_path')}",
            ],
        }
    if not finish.get("validation_evidence"):
        raise WorkflowError("validation evidence is required before push/create-pr automation")
    if pre_pr_gate["workflow_gate"] != "passed":
        raise WorkflowError("; ".join(pre_pr_gate["blocking"]))
    if push:
        actions.append(
            {
                "command": f"git push -u origin {branch}",
                "result": _execute_workflow_command(
                    bug_id,
                    ["git", "push", "-u", "origin", branch],
                    state="pushed",
                    cwd=REPO_ROOT,
                    timeout=180,
                    event="command:git_push",
                ),
            }
        )
        _write_state(bug_id, state="pushed", branch=branch, next_actions=["create_pr_from_pr_body"])
    if create_pr:
        title = pr_title or f"{bug_id} issue workflow fix"
        body_path = REPO_ROOT / str(finish.get("pr_body_path"))
        head_result = _run_command(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, timeout=30)
        expected_head = str(head_result.get("stdout") or "").strip()
        if not head_result.get("ok") or not re.fullmatch(r"[0-9a-fA-F]{40}", expected_head):
            raise WorkflowError(head_result.get("stderr") or "cannot resolve source PR head SHA")
        result = _create_pr_with_transport_fallback(
            branch=branch,
            base="main",
            title=title,
            body_path=body_path,
            expected_head=expected_head,
            root=REPO_ROOT,
        )
        if not result.get("ok"):
            raise WorkflowError(result.get("stderr") or result.get("stdout") or "PR create failed")
        _append_event(
            bug_id,
            event="command:gh_pr_create",
            state="pr_opened",
            command="gh pr create",
            cwd=REPO_ROOT,
            result="ok",
            evidence={
                "source": result.get("source"),
                "recovered_from_transport_error": result.get("recovered_from_transport_error", False),
                "head_sha": expected_head,
            },
        )
        pr_url = str(result.get("stdout") or "").splitlines()[-1].strip()
        actions.append({"command": "gh pr create", "result": result})
        _write_state(bug_id, state="pr_opened", branch=branch, pr_url=pr_url, next_actions=["watch_ci_before_merge"])
    ci_watch: dict[str, Any] | None = None
    if watch_ci:
        if not pr_url:
            raise WorkflowError("--watch-ci requires --create-pr in this Phase 1 wrapper")
        ci_watch = _watch_pr_checks_compact(bug_id, pr_url)
        actions.append({"command": "gh pr view --json statusCheckRollup", "result": {"ok": True, "summary": ci_watch.get("check_summary")}})
        check_ok = ci_watch.get("workflow_gate") == "checks_passed"
        _write_state(
            bug_id,
            state="ci_green" if check_ok else "ci_running",
            branch=branch,
            pr_url=pr_url,
            checks=ci_watch.get("check_summary"),
            next_actions=ci_watch.get("next_actions") or [],
            stop_reason=None if check_ok else str(ci_watch.get("workflow_gate") or "ci_not_green"),
        )
    result_payload = {"branch": branch, "dry_run": False, "pr_url": pr_url, "actions": actions, "worktree_guard": guard, "pre_pr_gate": pre_pr_gate}
    if ci_watch is not None:
        result_payload["ci_watch"] = ci_watch
    return result_payload


def _verify_pr_merged(pr_url: str, *, skip_github_check: bool = False) -> dict[str, Any]:
    if skip_github_check:
        return _verify_pr_merged_with_rest(
            pr_url,
            reason="explicit_rest_only",
            graphql_attempts=0,
        )
    result = _run_read_command_with_retry(
        ["gh", "pr", "view", pr_url, "--json", "state,mergedAt,mergeCommit,url,headRefName,headRefOid"],
        cwd=REPO_ROOT,
        timeout=30,
        attempts=1,
    )
    if not result.get("ok"):
        message = str(result.get("stderr") or result.get("stdout") or f"cannot inspect PR: {pr_url}")
        if _looks_like_github_transport_failure(message):
            return _verify_pr_merged_with_rest(
                pr_url,
                reason="graphql_transport_failure",
                graphql_attempts=int(result.get("attempts") or 0),
                graphql_error=message,
            )
        raise WorkflowError(message)
    try:
        payload = json.loads(str(result.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"cannot parse gh pr view output for {pr_url}: {exc}") from exc
    merged = payload.get("state") == "MERGED" or bool(payload.get("mergedAt"))
    if not merged:
        raise WorkflowError(f"PR is not merged: {pr_url}")
    return {"checked": True, "merged": True, "pr": payload}


def _merged_pr_validation_receipt_profile(pr_url: str) -> dict[str, Any]:
    """Confirm a merged PR carries a durable validation receipt without retaining its body."""

    profile: dict[str, Any] = {
        "schema_version": "aistock_merged_pr_validation_receipt_v1",
        "pr_url": pr_url,
        "checked": False,
        "merged": False,
        "durable_receipt_present": False,
        "status": "check_failed",
    }
    result = _run_read_command_with_retry(
        ["gh", "pr", "view", pr_url, "--json", "state,mergedAt,mergeCommit,url,headRefOid,body"],
        cwd=REPO_ROOT,
        timeout=30,
    )
    if not result.get("ok"):
        message = str(result.get("stderr") or result.get("stdout") or "cannot inspect merged PR receipt")
        if not _looks_like_github_transport_failure(message):
            profile["error"] = message
            return profile
        try:
            readback = _github_pull_rest_readback(pr_url)
        except WorkflowError as exc:
            profile["error"] = str(exc)
            return profile
        payload = {
            "state": "MERGED" if readback.get("merged") else readback.get("state"),
            "mergedAt": readback.get("merged_at"),
            "url": readback.get("url"),
            "headRefOid": readback.get("head_sha"),
            "mergeCommit": {"oid": readback.get("merge_sha")},
            "body": readback.get("body"),
        }
        profile["read_fallback"] = "github_rest"
    else:
        try:
            payload = json.loads(str(result.get("stdout") or "{}"))
        except json.JSONDecodeError as exc:
            profile["error"] = f"cannot parse merged PR receipt check: {exc}"
            return profile
    merged = payload.get("state") == "MERGED" or bool(payload.get("mergedAt"))
    head_oid = str(payload.get("headRefOid") or "").strip().lower()
    receipt_commits = sorted(
        {item.lower() for item in VALIDATION_RECEIPT_COMMIT_RE.findall(str(payload.get("body") or ""))}
    )
    matching_commit = next(
        (item for item in receipt_commits if _FULL_GIT_COMMIT_RE.fullmatch(head_oid) and head_oid.startswith(item)),
        None,
    )
    structured_receipt_present = flow._has_validation_receipt(payload.get("body"))
    receipt_present = bool(merged and structured_receipt_present and matching_commit)
    profile.update(
        {
            "pr_url": str(payload.get("url") or pr_url),
            "checked": True,
            "merged": merged,
            "durable_receipt_present": receipt_present,
            "head_oid": head_oid or None,
            "merge_commit": _merge_commit_from_pr_check({"pr": payload}),
            "merged_at": payload.get("mergedAt"),
            "receipt_commit": matching_commit,
            "status": (
                "finalized_merged_pr_receipt"
                if receipt_present
                else (
                    "receipt_commit_mismatch"
                    if merged and structured_receipt_present
                    else ("missing_structured_receipt" if merged else "pr_not_merged")
                )
            ),
        }
    )
    return profile


def _build_source_merge_receipt(
    *,
    bug_id: str,
    source_pr_url: str,
    source_pr_check: dict[str, Any] | None,
    merge_commit: str,
    validation_evidence: list[str],
    runtime_contract: dict[str, Any] | None,
    production_gates: dict[str, Any] | None,
) -> dict[str, Any]:
    """Create a compact receipt that authorizes source-worktree cleanup.

    This receipt deliberately does not claim runtime verification.  It binds
    only the merged source identity, validation summary, runtime contract and
    production-gate state, so a pending user restart remains pending while
    source cleanup can proceed independently.
    """
    source_pr = (source_pr_check or {}).get("pr") if isinstance(source_pr_check, dict) else {}
    if not isinstance(source_pr, dict):
        source_pr = {}
    source_head = str(
        source_pr.get("headRefOid")
        or source_pr.get("head_sha")
        or (source_pr_check or {}).get("head_sha")
        or ""
    ).strip()
    evidence = flow._unique_strings([item for item in validation_evidence if str(item).strip()])
    evidence_digest = hashlib.sha256(
        json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    contract = runtime_contract if isinstance(runtime_contract, dict) else {}
    runtime_pending = bool(contract.get("backend_restart_required"))
    gates = {
        key: str((production_gates or {}).get(key) or "noop")
        for key in _CLOSE_SYNC_PRODUCTION_GATE_KEYS
    }
    receipt_identity = {
        "bug_id": bug_id.upper(),
        "source_pr_url": source_pr_url,
        "source_head_oid": source_head,
        "source_merge_commit": merge_commit,
        "validation_evidence_digest": evidence_digest,
        "runtime_contract_digest": _runtime_contract_digest(contract) if contract else None,
        "production_gates": gates,
        "runtime_verification": "pending_user_restart" if runtime_pending else "not_required",
    }
    receipt_id = hashlib.sha256(
        json.dumps(receipt_identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    return {
        "schema_version": SOURCE_MERGE_RECEIPT_SCHEMA,
        "receipt_id": receipt_id,
        "bug_id": bug_id.upper(),
        "source_pr_url": source_pr_url,
        "source_head_oid": source_head or None,
        "source_merge_commit": merge_commit,
        "validation_evidence": evidence,
        "validation_evidence_digest": evidence_digest,
        "runtime_contract_digest": receipt_identity["runtime_contract_digest"],
        "runtime_verification": receipt_identity["runtime_verification"],
        "runtime_identity_match": "pending" if runtime_pending else "not_required",
        "production_gates": gates,
        "recorded_at": _utc_now(),
    }


def _source_merge_receipt_profile(
    receipt: Any,
    *,
    bug_id: str | None,
    source_pr_url: str | None,
    merge_commit: str | None,
) -> dict[str, Any]:
    """Validate a source receipt without treating runtime pending as failure."""
    blocking: list[str] = []
    if not isinstance(receipt, dict) or receipt.get("schema_version") != SOURCE_MERGE_RECEIPT_SCHEMA:
        blocking.append("source merge receipt schema is missing or invalid")
        return {"schema_version": "aistock_source_merge_receipt_profile_v1", "status": "invalid", "blocking": blocking}
    if not re.fullmatch(r"[0-9a-f]{16}", str(receipt.get("receipt_id") or "").lower()):
        blocking.append("source merge receipt id is missing or invalid")
    if not str(receipt.get("source_pr_url") or "").strip():
        blocking.append("source merge receipt source PR is missing")
    if not str(receipt.get("source_head_oid") or "").strip():
        blocking.append("source merge receipt source head is missing")
    if bug_id and str(receipt.get("bug_id") or "").upper() != str(bug_id).upper():
        blocking.append("source merge receipt BUG id mismatch")
    if source_pr_url and str(receipt.get("source_pr_url") or "") != source_pr_url:
        blocking.append("source merge receipt source PR mismatch")
    if merge_commit and str(receipt.get("source_merge_commit") or "") != merge_commit:
        blocking.append("source merge receipt merge commit mismatch")
    evidence = flow._as_list(receipt.get("validation_evidence"))
    if not evidence:
        blocking.append("source merge receipt validation evidence is empty")
    evidence_digest = hashlib.sha256(
        json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if evidence_digest != str(receipt.get("validation_evidence_digest") or ""):
        blocking.append("source merge receipt validation evidence digest mismatch")
    if not str(receipt.get("source_merge_commit") or "").strip():
        blocking.append("source merge receipt merge commit is missing")
    if receipt.get("runtime_verification") not in {"not_required", "pending_user_restart"}:
        blocking.append("source merge receipt runtime verification state is invalid")
    return {
        "schema_version": "aistock_source_merge_receipt_profile_v1",
        "status": "valid" if not blocking else "invalid",
        "durable_receipt_present": not blocking,
        "receipt_id": receipt.get("receipt_id"),
        "runtime_verification": receipt.get("runtime_verification"),
        "blocking": blocking,
    }


def _merge_commit_from_pr_check(pr_check: dict[str, Any] | None) -> str | None:
    pr = (pr_check or {}).get("pr") or {}
    merge_commit = pr.get("mergeCommit") if isinstance(pr, dict) else None
    if isinstance(merge_commit, dict):
        return str(merge_commit.get("oid") or "") or None
    return str(merge_commit or "") or None


def _merged_commit_changed_files(merge_commit: str) -> list[str]:
    """Return the source-PR delta from the verified merge commit's first parent."""
    normalized = str(merge_commit or "").strip().lower()
    if not _FULL_GIT_COMMIT_RE.fullmatch(normalized):
        raise WorkflowError("close-sync merge commit is not a full Git identity")
    result = _run_command(
        ["git", "diff", "--name-only", f"{normalized}^1", normalized, "--"],
        cwd=REPO_ROOT,
        timeout=30,
    )
    if not result.get("ok"):
        raise WorkflowError(result.get("stderr") or result.get("stdout") or "close-sync cannot resolve merged PR changed files")
    changed_files = flow._unique_strings(
        line.strip().replace("\\", "/")
        for line in str(result.get("stdout") or "").splitlines()
        if line.strip()
    )
    if not changed_files:
        raise WorkflowError("close-sync merged PR changed-file evidence is empty")
    return changed_files


def _closed_at_from_pr_check(pr_check: dict[str, Any] | None) -> str:
    """Use the authoritative source PR merge time, or the explicit skip-check time."""

    pr = (pr_check or {}).get("pr") or {}
    if isinstance(pr, dict):
        merged_at = str(pr.get("mergedAt") or "").strip()
        if merged_at:
            return merged_at
    return _utc_now()


def _pr_head_oid_from_pr_check(pr_check: dict[str, Any] | None) -> str | None:
    pr = (pr_check or {}).get("pr") or {}
    if not isinstance(pr, dict):
        return None
    head_ref_oid = str(pr.get("headRefOid") or "").strip()
    return head_ref_oid or None


def _git_ref_exists(ref: str, *, cwd: Path | None = None) -> bool:
    if not ref:
        return False
    return bool(_run_command(["git", "rev-parse", "--verify", "--quiet", ref], cwd=cwd or REPO_ROOT).get("ok"))


def _registered_worktree_paths(*, cwd: Path | None = None) -> set[Path]:
    result = _run_command(["git", "worktree", "list", "--porcelain"], cwd=cwd or REPO_ROOT, timeout=30)
    if not result.get("ok"):
        return set()
    paths: set[Path] = set()
    for line in str(result.get("stdout") or "").splitlines():
        if not line.startswith("worktree "):
            continue
        raw_path = line.removeprefix("worktree ").strip()
        if not raw_path:
            continue
        try:
            paths.add(Path(raw_path).resolve())
        except OSError:
            paths.add(Path(raw_path))
    return paths


def _path_is_registered_worktree(path: Path, *, cwd: Path | None = None) -> bool:
    try:
        target = path.resolve()
    except OSError:
        target = path
    return target in _registered_worktree_paths(cwd=cwd)


def _registered_worktree_for_branch(branch: str, *, cwd: Path | None = None) -> Path | None:
    if not branch:
        return None
    result = _run_command(["git", "worktree", "list", "--porcelain"], cwd=cwd or REPO_ROOT, timeout=30)
    if not result.get("ok"):
        return None
    current: Path | None = None
    expected_ref = f"refs/heads/{branch}"
    for line in str(result.get("stdout") or "").splitlines():
        if line.startswith("worktree "):
            raw_path = line.removeprefix("worktree ").strip()
            current = Path(raw_path) if raw_path else None
            continue
        if line.startswith("branch ") and current:
            ref = line.removeprefix("branch ").strip()
            if ref in {branch, expected_ref}:
                return current
    return None


def _git_refs_tree_equivalent(left: str, right: str, *, cwd: Path | None = None) -> bool:
    if not left or not right:
        return False
    root = cwd or REPO_ROOT
    if not _git_ref_exists(left, cwd=root) or not _git_ref_exists(right, cwd=root):
        return False
    return bool(_run_command(["git", "diff", "--quiet", left, right], cwd=root).get("ok"))


def _git_changed_files(base: str, head: str, *, cwd: Path | None = None) -> list[str]:
    if not base or not head:
        return []
    result = _run_command(["git", "diff", "--name-only", base, head], cwd=cwd or REPO_ROOT)
    if not result.get("ok"):
        return []
    return [line.strip() for line in str(result.get("stdout") or "").splitlines() if line.strip()]


def _git_merge_base(left: str, right: str, *, cwd: Path | None = None) -> str | None:
    result = _run_command(["git", "merge-base", left, right], cwd=cwd or REPO_ROOT)
    if not result.get("ok"):
        return None
    value = str(result.get("stdout") or "").strip()
    return value or None


def _git_paths_equivalent(left: str, right: str, paths: list[str], *, cwd: Path | None = None) -> bool:
    if not left or not right or not paths:
        return False
    root = cwd or REPO_ROOT
    if not _git_ref_exists(left, cwd=root) or not _git_ref_exists(right, cwd=root):
        return False
    return bool(_run_command(["git", "diff", "--quiet", left, right, "--", *paths], cwd=root).get("ok"))


def _git_squash_head_equivalent_to_ref(
    head_oid: str,
    target_ref: str,
    *,
    target_label: str | None = None,
    cwd: Path | None = None,
) -> dict[str, Any]:
    root = cwd or REPO_ROOT
    base_ref = target_label or target_ref
    payload: dict[str, Any] = {
        "head_ref": head_oid,
        "base_ref": base_ref,
        "target_ref": target_ref,
        "base": None,
        "changed_files": [],
        "verified": False,
        "reason": None,
    }
    if not _git_ref_exists(head_oid, cwd=root) or not _git_ref_exists(target_ref, cwd=root):
        payload["reason"] = "missing_ref"
        return payload
    if _git_refs_tree_equivalent(head_oid, target_ref, cwd=root):
        payload["verified"] = True
        payload["reason"] = "full_tree_equivalent"
        return payload
    base = _git_merge_base(head_oid, target_ref, cwd=root)
    payload["base"] = base
    if not base:
        payload["reason"] = "missing_merge_base"
        return payload
    changed_files = _git_changed_files(base, head_oid, cwd=root)
    payload["changed_files"] = changed_files
    if not changed_files:
        payload["reason"] = "empty_pr_diff"
        return payload
    payload["verified"] = _git_paths_equivalent(head_oid, target_ref, changed_files, cwd=root)
    payload["reason"] = "changed_paths_equivalent" if payload["verified"] else "changed_paths_differ"
    return payload


def _git_squash_head_equivalent_to_origin(head_oid: str, *, cwd: Path | None = None) -> dict[str, Any]:
    return _git_squash_head_equivalent_to_ref(head_oid, "origin/main", cwd=cwd)


def _cleanup_verified_pr_check_matches_target(
    value: dict[str, Any] | None,
    *,
    pr_url: str,
    branch: str,
) -> bool:
    pr = value.get("pr") if isinstance(value, dict) else None
    if not isinstance(pr, dict) or not value.get("checked") or not value.get("merged"):
        return False
    requested_url = pr_url.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    observed_url = str(pr.get("url") or "").split("?", 1)[0].split("#", 1)[0].rstrip("/")
    head_oid = str(pr.get("headRefOid") or "").strip().lower()
    merge_commit = str((pr.get("mergeCommit") or {}).get("oid") or "").strip().lower()
    return bool(
        requested_url
        and observed_url == requested_url
        and str(pr.get("headRefName") or "").strip() == branch
        and _FULL_GIT_COMMIT_RE.fullmatch(head_oid)
        and _FULL_GIT_COMMIT_RE.fullmatch(merge_commit)
    )


def _cleanup_merge_verification(
    branch: str,
    pr_url: str | None,
    merged: bool,
    *,
    cwd: Path | None = None,
    verified_pr_check: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = cwd or REPO_ROOT
    payload: dict[str, Any] = {
        "method": "git_merged_branch" if merged else None,
        "verified": bool(merged),
        "squash_merge_verified": False,
        "tree_equivalent_to_origin_main": bool(merged),
        "tree_equivalence_ref": "branch" if merged else None,
        "pr_check": None,
        "path_equivalence": None,
        "merge_commit_path_equivalence": None,
        "origin_path_equivalence": None,
    }
    if merged or not pr_url:
        return payload

    pr_check = (
        verified_pr_check
        if _cleanup_verified_pr_check_matches_target(verified_pr_check, pr_url=pr_url, branch=branch)
        else _verify_pr_merged(pr_url)
    )
    payload["pr_check"] = pr_check
    if not pr_check.get("merged"):
        return payload

    head_oid = _pr_head_oid_from_pr_check(pr_check)
    merge_commit = _merge_commit_from_pr_check(pr_check)
    if head_oid and merge_commit and _git_commit_is_ancestor(head_oid, merge_commit, root=root):
        payload.update(
            {
                "method": "merged_pr_head_is_ancestor_of_merge_commit",
                "verified": True,
                "squash_merge_verified": False,
                "tree_equivalent_to_origin_main": False,
                "tree_equivalence_ref": head_oid,
                "tree_equivalence_target": merge_commit,
            }
        )
        return payload

    pr = pr_check.get("pr") if isinstance(pr_check, dict) else None
    pr_head_name = str((pr or {}).get("headRefName") or "") if isinstance(pr, dict) else ""
    local_head = _git(
        ["rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}"],
        cwd=root,
        check=False,
    ).strip().lower()
    normalized_head = str(head_oid or "").strip().lower()
    normalized_merge_commit = str(merge_commit or "").strip().lower()
    merge_commit_in_origin_main = bool(
        _FULL_GIT_COMMIT_RE.fullmatch(normalized_merge_commit)
        and _git_commit_is_ancestor(normalized_merge_commit, "origin/main", root=root)
    )
    exact_pr_head_identity = bool(
        pr_head_name == branch
        and _FULL_GIT_COMMIT_RE.fullmatch(normalized_head)
        and local_head == normalized_head
        and merge_commit_in_origin_main
    )
    payload["pr_head_identity"] = {
        "verified": exact_pr_head_identity,
        "pr_head_name": pr_head_name or None,
        "expected_branch": branch,
        "pr_head_oid": normalized_head or None,
        "local_branch_oid": local_head or None,
        "merge_commit": normalized_merge_commit or None,
        "merge_commit_in_origin_main": merge_commit_in_origin_main,
    }
    if exact_pr_head_identity:
        payload.update(
            {
                "method": "merged_pr_exact_head_identity_in_origin_main",
                "verified": True,
                "squash_merge_verified": True,
                "tree_equivalent_to_origin_main": False,
                "tree_equivalence_ref": normalized_head,
                "tree_equivalence_target": normalized_merge_commit,
            }
        )
        return payload

    if head_oid and merge_commit:
        merge_commit_equivalence = _git_squash_head_equivalent_to_ref(
            head_oid,
            merge_commit,
            target_label="source_pr_merge_commit",
            cwd=root,
        )
    else:
        merge_commit_equivalence = {"verified": False, "reason": "missing_merge_commit" if head_oid else "missing_head_oid"}
    payload["merge_commit_path_equivalence"] = merge_commit_equivalence
    if head_oid and merge_commit_equivalence.get("verified"):
        payload["path_equivalence"] = merge_commit_equivalence
        payload.update(
            {
                "method": f"squash_merge_head_oid_{merge_commit_equivalence.get('reason')}_to_merge_commit",
                "verified": True,
                "squash_merge_verified": True,
                "tree_equivalent_to_origin_main": False,
                "tree_equivalence_ref": head_oid,
                "tree_equivalence_target": merge_commit,
            }
        )
        return payload

    head_equivalence = _git_squash_head_equivalent_to_origin(head_oid, cwd=root) if head_oid else {"verified": False, "reason": "missing_head_oid"}
    payload["origin_path_equivalence"] = head_equivalence
    payload["path_equivalence"] = head_equivalence
    if head_oid and head_equivalence.get("verified"):
        payload.update(
            {
                "method": f"squash_merge_head_oid_{head_equivalence.get('reason')}",
                "verified": True,
                "squash_merge_verified": True,
                "tree_equivalent_to_origin_main": head_equivalence.get("reason") == "full_tree_equivalent",
                "tree_equivalence_ref": head_oid,
            }
        )
        return payload

    if _git_ref_exists(branch, cwd=root) and _git_refs_tree_equivalent(branch, "origin/main", cwd=root):
        payload.update(
            {
                "method": "squash_merge_branch_tree_equivalent",
                "verified": True,
                "squash_merge_verified": True,
                "tree_equivalent_to_origin_main": True,
                "tree_equivalence_ref": branch,
            }
        )
    return payload


def _cleanup_preflight_fetch_origin(root: Path, *, apply: bool) -> dict[str, Any]:
    if not apply:
        return {"status": "skipped", "reason": "dry_run"}
    result = _run_read_command_with_retry(
        ["git", "fetch", "origin", "--prune"],
        cwd=root,
        timeout=120,
    )
    return {
        "status": "fetched" if result.get("ok") else "failed",
        "command": "git fetch origin --prune",
        "result": result,
    }


def _cleanup_preflight_fetch_for_plan(
    root: Path,
    *,
    apply: bool,
    cached: dict[str, Any] | None,
) -> dict[str, Any]:
    reusable = bool(
        apply
        and cached
        and cached.get("status") == "fetched"
        and isinstance(cached.get("result"), dict)
        and cached["result"].get("ok")
    )
    if reusable and cached is not None:
        return {**cached, "reused": True}
    return _cleanup_preflight_fetch_origin(root, apply=apply)


def _canonical_bug_record_snapshot(bug_id: str, root: Path | None = None) -> dict[str, Any]:
    canonical_root = root or _canonical_root()
    payload: dict[str, Any] = {
        "bug_id": bug_id,
        "canonical_root": str(canonical_root),
        "persisted": False,
        "path": None,
        "status": None,
    }
    bugs_root = _bugs_root(canonical_root)
    if not bugs_root.exists():
        payload["reason"] = "bugs_root_missing"
        return payload
    for path in _bug_files(bugs_root):
        try:
            record = _load_json(path)
        except WorkflowError:
            continue
        if str(record.get("bug_id") or "").strip().upper() != bug_id:
            continue
        payload.update(
            {
                "persisted": True,
                "path": _repo_rel(path, canonical_root),
                "status": record.get("status"),
                "github_issue_number": record.get("github_issue_number"),
                "github_issue_url": record.get("github_issue_url"),
            }
        )
        return payload
    payload["reason"] = "bug_record_missing"
    return payload


def build_registry_intake_cleanup_plan(
    *,
    bug_id: str,
    apply: bool = False,
    canonical_root: str | None = None,
) -> dict[str, Any]:
    canonical_bug_id = bug_id.strip().upper()
    root = Path(canonical_root) if canonical_root else _canonical_root()
    persisted = _canonical_bug_record_snapshot(canonical_bug_id, root)
    current_cwd = Path.cwd().resolve()
    local_branches = set(_git(["for-each-ref", "--format=%(refname:short)", "refs/heads"], check=False).splitlines())
    merged_refs = set(_git(["branch", "--format=%(refname:short)", "--merged", "origin/main"], check=False).splitlines())
    candidates: list[dict[str, Any]] = []
    warnings: list[str] = []

    for item in _active_workflows_for_bug(canonical_bug_id):
        if not str(item.get("workflow_role") or "").startswith("registry"):
            continue
        worktree_path = Path(str(item.get("worktree") or item.get("root") or ""))
        branch = str(item.get("branch") or "").strip()
        exists = bool(str(worktree_path)) and worktree_path.exists()
        git = item.get("git") if isinstance(item.get("git"), dict) else (_git_snapshot(worktree_path) if exists else {})
        dirty = bool(git.get("dirty")) if git else True
        is_current_cwd = exists and worktree_path.resolve() == current_cwd
        remote_result = (
            _run_read_command_with_retry(
                ["git", "ls-remote", "--heads", "origin", branch],
                cwd=root,
                timeout=60,
                attempts=2,
            )
            if branch
            else {"ok": True, "stdout": "", "stderr": "", "attempts": 0}
        )
        remote_ref = str(remote_result.get("stdout") or "") if remote_result.get("ok") else ""
        remote_check_ok = bool(remote_result.get("ok"))
        safe = bool(persisted.get("persisted")) and exists and not dirty and not is_current_cwd and remote_check_ok
        reason = None
        if not persisted.get("persisted"):
            reason = "canonical_bug_record_missing"
        elif not exists:
            reason = "worktree_missing"
        elif dirty:
            reason = "worktree_dirty"
        elif is_current_cwd:
            reason = "refusing_current_cwd"
        elif not remote_check_ok:
            reason = "remote_branch_check_failed"
        actions = []
        if exists:
            actions.append({"action": "remove_worktree", "worktree": str(worktree_path), "safe": safe})
        if branch and branch in local_branches:
            actions.append(
                {
                    "action": "delete_local_branch",
                    "branch": branch,
                    "safe": safe,
                    "delete_flag": "-d" if branch in merged_refs else "-D",
                }
            )
        if branch and remote_ref:
            actions.append(
                {
                    "action": "delete_remote_branch",
                    "branch": branch,
                    "expected_remote_ref": remote_ref,
                    "safe": safe,
                }
            )
        if not safe and reason:
            warnings.append(f"registry intake cleanup skipped for {worktree_path}: {reason}")
        candidates.append(
            {
                "worktree": str(worktree_path) if str(worktree_path) else None,
                "branch": branch or None,
                "issue_json": item.get("issue_json"),
                "dirty": dirty,
                "remote_check": remote_result,
                "safe": safe,
                "skip_reason": None if safe else reason,
                "actions": actions,
            }
        )

    safe_candidates = [item for item in candidates if item.get("safe")]
    payload: dict[str, Any] = {
        "schema_version": "aistock_issue_workflow_registry_intake_cleanup_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "canonical_bug_record": persisted,
        "candidates": candidates,
        "warnings": warnings,
        "dry_run": not apply,
        "workflow_gate": "ready_for_cleanup" if safe_candidates else "skipped",
    }
    if not apply or not safe_candidates:
        return payload

    started = time.monotonic()
    applied: list[dict[str, Any]] = []
    for candidate in safe_candidates:
        for action in candidate.get("actions") or []:
            if not action.get("safe"):
                continue
            if action["action"] == "remove_worktree":
                worktree_path = Path(str(action["worktree"]))
                applied.append(
                    {
                        "command": f"git worktree remove {action['worktree']}",
                        "result": _remove_worktree_with_reparse_fallback(root=REPO_ROOT, worktree_path=worktree_path),
                    }
                )
            elif action["action"] == "delete_local_branch":
                flag = str(action.get("delete_flag") or "-d")
                applied.append(
                    {
                        "command": f"git branch {flag} {action['branch']}",
                        "result": _execute_checked(["git", "branch", flag, str(action["branch"])], cwd=REPO_ROOT, timeout=120),
                    }
                )
            elif action["action"] == "delete_remote_branch":
                applied.append(
                    {
                        "command": f"git push origin --delete --force-with-lease {action['branch']}",
                        "result": _delete_remote_branch_with_lease(
                            root=REPO_ROOT,
                            branch=str(action["branch"]),
                            expected_remote_ref=str(action.get("expected_remote_ref") or ""),
                        ),
                    }
                )
    payload["applied"] = applied
    payload["duration_seconds"] = round(time.monotonic() - started, 3)
    payload["workflow_gate"] = "cleanup_done"
    payload["dry_run"] = False
    return payload


def _close_sync_issue_comment_marker(
    record: dict[str, Any],
    evidence_payload: dict[str, Any],
) -> str:
    bug_id = str(record.get("bug_id") or "BUG-UNKNOWN").strip().upper()
    if not re.fullmatch(r"BUG-\d+", bug_id):
        bug_id = "BUG-UNKNOWN"
    identity = _short_hash(
        bug_id,
        str(evidence_payload.get("merged_pr") or ""),
        str(evidence_payload.get("merge_commit") or ""),
        length=16,
    )
    return f"aistock-close-sync:{bug_id}:{identity}"


def _github_issue_comment_marker_readback(
    issue_number: int | str,
    marker: str,
    *,
    root: Path,
) -> dict[str, Any]:
    jq = f'.[] | select((.body // "") | contains("{marker}")) | .id'
    result = _run_transport_read_with_retry(
        [
            "gh",
            "api",
            "--paginate",
            f"repos/{GITHUB_REPO}/issues/{issue_number}/comments?per_page=100",
            "--jq",
            jq,
        ],
        cwd=root,
        timeout=60,
        attempts=2,
    )
    if not result.get("ok"):
        return {
            "ok": False,
            "present": False,
            "attempts": result.get("attempts"),
            "error": result.get("stderr") or result.get("stdout") or "comment marker readback failed",
        }
    comment_ids = [line.strip() for line in str(result.get("stdout") or "").splitlines() if line.strip()]
    return {
        "ok": True,
        "present": bool(comment_ids),
        "attempts": result.get("attempts"),
        "matching_comment_count": len(comment_ids),
    }


def _sync_github_issue_after_close(
    record: dict[str, Any],
    evidence_payload: dict[str, Any],
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    issue_number = record.get("github_issue_number")
    if not issue_number:
        return {"status": "skipped_missing_issue_number"}
    comment_marker = _close_sync_issue_comment_marker(record, evidence_payload)
    lines = [
        f"AIstock workflow close-sync persisted to the current registry worktree for `{record.get('bug_id')}`.",
        f"<!-- {comment_marker} -->",
        "",
        f"- PR: {evidence_payload.get('merged_pr') or 'n/a'}",
        f"- Merge commit: `{evidence_payload.get('merge_commit') or 'unknown'}`",
        f"- BUG JSON status: `{record.get('status') or 'unknown'}`",
        "- Note: this PR persists registry close-sync metadata; final completion requires this PR to merge into `origin/main`.",
        "",
        "Validation evidence:",
        *[f"- {item}" for item in evidence_payload.get("validation_evidence") or ["n/a"]],
        "",
        "Production gates:",
        *[
            f"- {key}: `{value}`"
            for key, value in sorted((evidence_payload.get("production_gates") or {}).items())
        ],
    ]
    tmp_comment = root / WORKFLOW_ROOT / str(record.get("bug_id") or issue_number) / "github-close-comment.md"
    _write_text(tmp_comment, "\n".join(lines))
    comment_readback_before = _github_issue_comment_marker_readback(
        issue_number,
        comment_marker,
        root=root,
    )
    if comment_readback_before.get("present"):
        comment = {
            "ok": True,
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "skipped": True,
            "reason": "exact_close_sync_comment_already_present",
        }
        comment_readback_after = comment_readback_before
    else:
        comment = _run_command(
            ["gh", "issue", "comment", str(issue_number), "--repo", GITHUB_REPO, "--body-file", str(tmp_comment)],
            cwd=root,
            timeout=60,
        )
        comment_message = str(comment.get("stderr") or comment.get("stdout") or "")
        comment_readback_after = (
            _github_issue_comment_marker_readback(issue_number, comment_marker, root=root)
            if not comment.get("ok") and _looks_like_github_transport_failure(comment_message)
            else None
        )
    comment_verified = bool(
        comment.get("ok")
        or (comment_readback_after and comment_readback_after.get("present"))
    )
    close = _run_command(["gh", "issue", "close", str(issue_number), "--repo", GITHUB_REPO], cwd=root, timeout=60)
    label_sync = _sync_closed_issue_status_labels(issue_number, require_closed=True)
    close_verified = bool(close.get("ok") or (label_sync.get("ok") and label_sync.get("verified")))
    return {
        "status": "synced" if comment_verified and close_verified and label_sync.get("ok") else "warning",
        "comment": comment,
        "comment_marker": comment_marker,
        "comment_verified": comment_verified,
        "comment_readback_before": comment_readback_before,
        "comment_readback_after": comment_readback_after,
        "close": close,
        "close_verified": close_verified,
        "label_sync": _pick(
            label_sync,
            "ok",
            "returncode",
            "skipped",
            "verified",
            "attempts",
            "reason",
            "removed_labels",
            "added_labels",
        ),
        "comment_path": _repo_rel(tmp_comment, root),
    }


def _sync_github_issue_runtime_pending(
    record: dict[str, Any],
    evidence_payload: dict[str, Any],
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    issue_number = record.get("github_issue_number")
    if not issue_number:
        return {"status": "skipped_missing_issue_number"}
    comment_path = root / WORKFLOW_ROOT / str(record.get("bug_id") or issue_number) / "github-runtime-pending-comment.md"
    _write_text(
        comment_path,
        "\n".join(
            [
                f"Source fix merged for `{record.get('bug_id')}`; runtime verification remains pending user restart.",
                "",
                f"- Source PR: {evidence_payload.get('merged_pr') or 'n/a'}",
                f"- Merge commit: `{evidence_payload.get('merge_commit') or 'unknown'}`",
                "- BUG JSON status: `fixed_source_pending_user_restart`",
                "- Backend restart owner: `user`",
            ]
        )
        + "\n",
    )
    view = _run_command(
        ["gh", "issue", "view", str(issue_number), "--repo", GITHUB_REPO, "--json", "state,labels"],
        cwd=root,
        timeout=60,
    )
    try:
        issue_view = json.loads(str(view.get("stdout") or "{}")) if view.get("ok") else {}
    except json.JSONDecodeError:
        issue_view = {}
    reopen = (
        {"ok": True, "stdout": "already open", "stderr": ""}
        if issue_view.get("state") == "OPEN"
        else _run_command(["gh", "issue", "reopen", str(issue_number), "--repo", GITHUB_REPO], cwd=root, timeout=60)
    )
    comment = _run_command(
        ["gh", "issue", "comment", str(issue_number), "--repo", GITHUB_REPO, "--body-file", str(comment_path)],
        cwd=root,
        timeout=60,
    )
    current_labels = {
        str(item.get("name"))
        for item in issue_view.get("labels") or []
        if isinstance(item, dict) and item.get("name")
    }
    label_args = ["gh", "issue", "edit", str(issue_number), "--repo", GITHUB_REPO]
    for label in ("status:fixed", "status:verified"):
        if label in current_labels:
            label_args.extend(["--remove-label", label])
    if "status:in_progress" not in current_labels:
        label_args.extend(["--add-label", "status:in_progress"])
    labels = (
        {"ok": True, "stdout": "labels already aligned", "stderr": ""}
        if len(label_args) == 6
        else _run_command(label_args, cwd=root, timeout=60)
    )
    if not reopen.get("ok") or not comment.get("ok") or not labels.get("ok"):
        raise WorkflowError(
            reopen.get("stderr")
            or comment.get("stderr")
            or labels.get("stderr")
            or "failed to keep runtime-pending GitHub Issue open"
        )
    return {"status": "reopened_runtime_pending", "issue_number": issue_number}


def _production_gates_payload(args: argparse.Namespace | None = None) -> dict[str, str]:
    if args is None:
        return {
            "production_ddl_gate": "noop",
            "production_frontend_dependency_gate": "noop",
            "production_backend_dependency_gate": "noop",
        }
    return {
        "production_ddl_gate": args.production_ddl_gate,
        "production_frontend_dependency_gate": args.production_frontend_dependency_gate,
        "production_backend_dependency_gate": args.production_backend_dependency_gate,
    }


def _github_pr_number_from_url(pr_url: str) -> int | None:
    match = re.search(r"/pull/(\d+)(?:$|[/?#])", pr_url.strip())
    return int(match.group(1)) if match else None


def _parse_bug_pr_mappings(values: list[str] | None) -> dict[str, str]:
    mappings: dict[str, str] = {}
    for raw in values or []:
        bug_text, separator, pr_text = str(raw).partition("=")
        bug_id = bug_text.strip().upper()
        pr_url = pr_text.strip().rstrip("/")
        if not separator or not re.fullmatch(r"BUG-\d+", bug_id):
            raise WorkflowError(f"invalid --source-pr mapping {raw!r}; expected BUG-NNN=https://github.com/.../pull/NNN")
        pr_number = _github_pr_number_from_url(pr_url)
        expected_url = f"https://github.com/{GITHUB_REPO}/pull/{pr_number}" if pr_number is not None else ""
        if not expected_url or pr_url != expected_url:
            raise WorkflowError(f"invalid --source-pr URL for {bug_id}: {pr_url or 'missing'}")
        previous = mappings.get(bug_id)
        if previous and previous != pr_url:
            raise WorkflowError(f"conflicting --source-pr mappings for {bug_id}: {previous} vs {pr_url}")
        mappings[bug_id] = pr_url
    return mappings


def _github_pull_rest_readback(pr_url: str) -> dict[str, Any]:
    pr_number = _github_pr_number_from_url(pr_url)
    if pr_number is None:
        raise WorkflowError(f"cannot derive GitHub PR number from URL: {pr_url}")
    result = _run_transport_read_with_retry(
        ["gh", "api", f"repos/{GITHUB_REPO}/pulls/{pr_number}"],
        cwd=REPO_ROOT,
        timeout=30,
        attempts=2,
    )
    if not result.get("ok"):
        raise WorkflowError(
            result.get("stderr") or result.get("stdout") or f"cannot inspect PR {pr_number} through GitHub REST"
        )
    try:
        payload = json.loads(str(result.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"cannot parse GitHub REST PR readback for {pr_number}: {exc}") from exc
    if not isinstance(payload, dict):
        raise WorkflowError(f"GitHub REST PR readback for {pr_number} is not an object")
    head = payload.get("head") or {}
    base = payload.get("base") or {}
    return {
        "pr_number": pr_number,
        "state": str(payload.get("state") or "").upper(),
        "merged": bool(payload.get("merged")),
        "mergeable": payload.get("mergeable"),
        "mergeable_state": str(payload.get("mergeable_state") or "").upper(),
        "merged_at": payload.get("merged_at"),
        "merge_commit": str(payload.get("merge_commit_sha") or "").strip() or None,
        "head_sha": str(head.get("sha") or "").strip(),
        "head_ref": str(head.get("ref") or "").strip(),
        "base_ref": str(base.get("ref") or "").strip(),
        "url": str(payload.get("html_url") or pr_url),
        "body": str(payload.get("body") or ""),
    }


def _verify_pr_merged_with_rest(
    pr_url: str,
    *,
    reason: str,
    graphql_attempts: int,
    graphql_error: str | None = None,
) -> dict[str, Any]:
    pr_number = _github_pr_number_from_url(pr_url)
    expected_url = f"https://github.com/{GITHUB_REPO}/pull/{pr_number}" if pr_number is not None else ""
    requested_url = pr_url.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    if not expected_url or requested_url != expected_url:
        raise WorkflowError(
            "close-sync REST PR identity mismatch: "
            f"expected_url={expected_url or 'unresolved'} requested_url={requested_url or 'missing'}"
        )
    readback = _github_pull_rest_readback(pr_url)
    observed_url = str(readback.get("url") or "").rstrip("/")
    if expected_url and observed_url != expected_url:
        raise WorkflowError(
            "close-sync REST PR identity mismatch: "
            f"expected_url={expected_url} observed_url={observed_url or 'missing'}"
        )
    if not readback.get("merged"):
        raise WorkflowError(f"PR is not merged: {pr_url}")
    head_sha = str(readback.get("head_sha") or "").strip().lower()
    merge_commit = str(readback.get("merge_commit") or "").strip().lower()
    merged_at = str(readback.get("merged_at") or "").strip()
    if not _FULL_GIT_COMMIT_RE.fullmatch(head_sha):
        raise WorkflowError("close-sync REST PR readback is missing a full head SHA")
    if not _FULL_GIT_COMMIT_RE.fullmatch(merge_commit):
        raise WorkflowError("close-sync REST PR readback is missing a full merge commit SHA")
    if not merged_at:
        raise WorkflowError("close-sync REST PR readback is missing merged_at")
    verified = _verified_pr_from_rest_readback(readback)
    verified["rest_fallback"] = {
        "reason": reason,
        "graphql_attempts": graphql_attempts,
        "graphql_error": str(graphql_error or "")[:1000] or None,
        "pr_number": pr_number,
        "head_sha": head_sha,
        "merge_commit": merge_commit,
    }
    return verified


def _verified_pr_from_rest_readback(readback: dict[str, Any]) -> dict[str, Any]:
    return {
        "checked": True,
        "merged": True,
        "source": "github_rest",
        "pr": {
            "state": "MERGED",
            "mergedAt": readback.get("merged_at"),
            "mergeCommit": {"oid": readback.get("merge_commit")},
            "url": readback.get("url"),
            "headRefName": readback.get("head_ref"),
            "headRefOid": readback.get("head_sha"),
        },
    }


def _run_merge_read_with_retry(
    args: list[str],
    *,
    bug_id: str | None,
    event: str,
) -> dict[str, Any]:
    started = time.monotonic()
    result = _run_transport_read_with_retry(args, cwd=REPO_ROOT, timeout=60, attempts=2)
    if bug_id:
        _append_event(
            bug_id,
            event=event,
            state="ci_green",
            command=" ".join(args),
            cwd=REPO_ROOT,
            duration_seconds=time.monotonic() - started,
            result="ok" if result.get("ok") else "failed",
            evidence={
                "attempts": result.get("attempts"),
                "returncode": result.get("returncode"),
                "stdout_excerpt": str(result.get("stdout") or "")[:1000],
                "stderr_excerpt": str(result.get("stderr") or "")[:1000],
            },
        )
    return result


def _run_transport_read_with_retry(
    args: list[str],
    *,
    cwd: Path | None = None,
    timeout: int = 30,
    attempts: int = 2,
) -> dict[str, Any]:
    total = max(1, int(attempts))
    last: dict[str, Any] = {"ok": False, "returncode": None, "stdout": "", "stderr": "not run"}
    for index in range(total):
        last = _run_command(args, cwd=cwd, timeout=timeout)
        if last.get("ok"):
            return {**last, "attempts": index + 1}
        message = str(last.get("stderr") or last.get("stdout") or "")
        if not _looks_like_github_transport_failure(message) or index + 1 >= total:
            return {**last, "attempts": index + 1}
        time.sleep(0.5)
    return {**last, "attempts": total}


def _merge_pr_view_with_transport_fallback(
    pr_url: str,
    *,
    bug_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    command = [
        "gh",
        "pr",
        "view",
        pr_url,
        "--json",
        "state,mergeStateStatus,mergeable,statusCheckRollup,url,headRefOid,baseRefName",
    ]
    result = _run_merge_read_with_retry(
        command,
        bug_id=bug_id,
        event="command:gh_pr_view_before_merge",
    )
    if result.get("ok"):
        try:
            payload = json.loads(str(result.get("stdout") or "{}"))
        except json.JSONDecodeError as exc:
            raise WorkflowError(f"cannot parse pre-merge PR view: {exc}") from exc
        if not isinstance(payload, dict):
            raise WorkflowError("pre-merge PR view returned a non-object payload")
        fallback = None
    else:
        message = str(result.get("stderr") or result.get("stdout") or "pre-merge PR view failed")
        if not _looks_like_github_transport_failure(message):
            raise WorkflowError(message)
        readback = _github_pull_rest_readback(pr_url)
        payload = {
            "state": "MERGED" if readback["merged"] else readback["state"],
            "mergeStateStatus": readback["mergeable_state"],
            "mergeable": (
                "MERGEABLE" if readback["mergeable"] is True else (
                    "CONFLICTING" if readback["mergeable"] is False else "UNKNOWN"
                )
            ),
            "url": readback["url"],
            "headRefOid": readback["head_sha"],
            "baseRefName": readback["base_ref"],
            "statusCheckRollup": [],
        }
        fallback = {
            "stage": "pr_view",
            "source": "github_rest",
            "graphql_attempts": result.get("attempts"),
            "head_sha": readback["head_sha"],
        }
        if bug_id:
            _append_event(
                bug_id,
                event="merge_graphql_view_rest_fallback",
                state="ci_green",
                result="recovered",
                evidence={"pr_url": pr_url, **fallback},
            )

    state = str(payload.get("state") or "").upper()
    mergeable = str(payload.get("mergeable") or "").upper()
    merge_state = str(payload.get("mergeStateStatus") or "").upper()
    if state not in {"OPEN", "MERGED"}:
        raise WorkflowError(f"PR is neither open nor merged: state={state or 'missing'}")
    if state == "OPEN" and (mergeable == "CONFLICTING" or merge_state == "DIRTY"):
        raise WorkflowError("PR cannot be cleanly merged")
    return payload, fallback


def _parse_rest_object(result: dict[str, Any], *, context: str) -> dict[str, Any]:
    if not result.get("ok"):
        raise WorkflowError(str(result.get("stderr") or result.get("stdout") or f"{context} failed"))
    try:
        payload = json.loads(str(result.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"{context} returned invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise WorkflowError(f"{context} returned a non-object payload")
    return payload


def _rest_required_pr_check_result(
    pr_url: str,
    *,
    expected_head: str,
    base_ref: str,
) -> dict[str, Any]:
    readback = _github_pull_rest_readback(pr_url)
    normalized_head = expected_head.strip().lower()
    if not _FULL_GIT_COMMIT_RE.fullmatch(normalized_head):
        raise WorkflowError("REST required-check fallback requires the verified full PR head SHA")
    if readback["head_sha"].lower() != normalized_head:
        raise WorkflowError(
            "PR head changed before REST required-check fallback: "
            f"expected={normalized_head}, observed={readback['head_sha'] or 'missing'}"
        )
    observed_base = readback["base_ref"]
    if base_ref and observed_base != base_ref:
        raise WorkflowError(
            f"PR base changed before REST required-check fallback: expected={base_ref}, observed={observed_base or 'missing'}"
        )
    effective_base = observed_base or base_ref
    if not effective_base:
        raise WorkflowError("REST required-check fallback requires the PR base branch")

    encoded_base = urllib.parse.quote(effective_base, safe="")
    protection_result = _run_transport_read_with_retry(
        ["gh", "api", f"repos/{GITHUB_REPO}/branches/{encoded_base}/protection/required_status_checks"],
        cwd=REPO_ROOT,
        timeout=30,
        attempts=2,
    )
    protection_message = str(protection_result.get("stderr") or protection_result.get("stdout") or "")
    no_required_checks = any(
        marker in protection_message.casefold()
        for marker in ("branch not protected", "required status checks are not enabled")
    )
    if not protection_result.get("ok") and no_required_checks:
        requirements: list[tuple[str, int | None]] = []
    else:
        protection = _parse_rest_object(protection_result, context="required-status-check protection readback")
        requirements = []
        for item in protection.get("checks") or []:
            if not isinstance(item, dict) or not str(item.get("context") or "").strip():
                continue
            try:
                parsed_app_id = int(item.get("app_id")) if item.get("app_id") is not None else None
            except (TypeError, ValueError) as exc:
                raise WorkflowError("required-status-check protection returned an invalid app_id") from exc
            requirements.append((str(item["context"]), None if parsed_app_id == -1 else parsed_app_id))
        known_contexts = {context for context, _ in requirements}
        for context in protection.get("contexts") or []:
            name = str(context or "").strip()
            if name and name not in known_contexts:
                requirements.append((name, None))

    required_contexts = _merge_quality_contexts_for_head_ref(readback.get("head_ref"))
    known_contexts = {context for context, _ in requirements}
    for context in required_contexts:
        if context not in known_contexts:
            requirements.append((context, None))

    check_runs_result = _run_transport_read_with_retry(
        ["gh", "api", f"repos/{GITHUB_REPO}/commits/{normalized_head}/check-runs?per_page=100"],
        cwd=REPO_ROOT,
        timeout=30,
        attempts=2,
    )
    check_runs_payload = _parse_rest_object(check_runs_result, context="required check-runs readback")
    check_runs = [item for item in check_runs_payload.get("check_runs") or [] if isinstance(item, dict)]
    if int(check_runs_payload.get("total_count") or len(check_runs)) > len(check_runs):
        raise WorkflowError("required check-runs readback exceeds the supported single page")

    missing_status_contexts: set[str] = set()
    rows: list[dict[str, Any]] = []
    for context, app_id in requirements:
        matches = [
            item
            for item in check_runs
            if str(item.get("name") or "") == context
            and (
                app_id is None
                or int(((item.get("app") or {}).get("id") or -1)) == app_id
            )
        ]
        if not matches:
            if app_id is None:
                missing_status_contexts.add(context)
            else:
                rows.append(
                    {
                        "name": context,
                        "state": "pending",
                        "bucket": "pending",
                        "workflow": "github-rest",
                    }
                )
            continue
        latest = max(matches, key=lambda item: int(item.get("id") or 0))
        status = str(latest.get("status") or "").upper()
        conclusion = str(latest.get("conclusion") or "").upper()
        bucket = (
            "pending" if status != "COMPLETED" else (
                "pass" if conclusion in NON_BLOCKING_CHECK_CONCLUSIONS else "fail"
            )
        )
        rows.append({"name": context, "state": conclusion or status, "bucket": bucket, "workflow": "github-rest"})

    if missing_status_contexts:
        status_result = _run_transport_read_with_retry(
            ["gh", "api", f"repos/{GITHUB_REPO}/commits/{normalized_head}/status"],
            cwd=REPO_ROOT,
            timeout=30,
            attempts=2,
        )
        status_payload = _parse_rest_object(status_result, context="required commit-status readback")
        statuses = [item for item in status_payload.get("statuses") or [] if isinstance(item, dict)]
        for context in sorted(missing_status_contexts):
            match = next((item for item in statuses if str(item.get("context") or "") == context), None)
            state = str((match or {}).get("state") or "pending").lower()
            bucket = "pass" if state == "success" else ("pending" if state in {"pending", "expected"} else "fail")
            rows.append({"name": context, "state": state, "bucket": bucket, "workflow": "github-rest"})

    return {
        "ok": True,
        "returncode": 0,
        "stdout": json.dumps(rows),
        "stderr": "",
        "source": "github_rest",
        "head_sha": normalized_head,
    }


def _merge_required_check_result_with_transport_fallback(
    pr_url: str,
    *,
    payload: dict[str, Any],
    bug_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    command = ["gh", "pr", "checks", pr_url, "--json", "name,state,bucket,workflow"]
    result = _run_merge_read_with_retry(
        command,
        bug_id=bug_id,
        event="command:gh_pr_required_checks_before_merge",
    )
    required_contexts = _merge_quality_contexts_for_head_ref(payload.get("headRefName"))
    normalized = _normalize_merge_quality_check_result(result, required_contexts=required_contexts)
    if normalized is not None:
        return normalized, None
    message = str(result.get("stderr") or result.get("stdout") or "required PR check query failed")
    if not _looks_like_github_transport_failure(message):
        # ``gh pr checks`` may briefly return a non-zero status while GitHub
        # has not published any contexts yet.  Keep this
        # fail-closed, but represent it as a pending sentinel so the bounded
        # poller can observe the next report instead of requiring a manual
        # rerun.
        if any(
            marker in message.casefold()
            for marker in ("no required checks reported", "no checks reported", "no required status checks")
        ):
            try:
                rest_result = _rest_required_pr_check_result(
                    pr_url,
                    expected_head=str(payload.get("headRefOid") or ""),
                    base_ref=str(payload.get("baseRefName") or ""),
                )
            except WorkflowError:
                rest_result = None
            if rest_result is not None:
                return rest_result, {
                    "stage": "required_checks_not_yet_reported",
                    "source": "github_rest",
                    "head_sha": rest_result.get("head_sha"),
                }
            return {
                "ok": True,
                "returncode": 0,
                "stdout": json.dumps(
                    [
                        {
                            "name": "github-required-checks-reporting",
                            "state": "pending",
                            "bucket": "pending",
                            "workflow": "github",
                        }
                    ]
                ),
                "stderr": message,
                "source": "github_pending_sentinel",
            }, None
        return result, None
    rest_result = _rest_required_pr_check_result(
        pr_url,
        expected_head=str(payload.get("headRefOid") or ""),
        base_ref=str(payload.get("baseRefName") or ""),
    )
    fallback = {
        "stage": "required_checks",
        "source": "github_rest",
        "graphql_attempts": result.get("attempts"),
        "head_sha": rest_result.get("head_sha"),
    }
    if bug_id:
        _append_event(
            bug_id,
            event="merge_graphql_required_checks_rest_fallback",
            state="ci_green",
            result="recovered",
            evidence={"pr_url": pr_url, **fallback},
        )
    return rest_result, fallback


def _head_pinned_rest_merge_after_transport_failure(
    *,
    pr_url: str,
    expected_head: str,
    graphql_error: str,
    bug_id: str | None = None,
) -> dict[str, Any]:
    normalized_head = expected_head.strip().lower()
    if not _FULL_GIT_COMMIT_RE.fullmatch(normalized_head):
        raise WorkflowError("GraphQL merge transport fallback requires the verified full PR head SHA")

    before = _github_pull_rest_readback(pr_url)
    if before["head_sha"].lower() != normalized_head:
        raise WorkflowError(
            "PR head changed before GitHub REST merge fallback: "
            f"expected={normalized_head}, observed={before['head_sha'] or 'missing'}"
        )
    if before["merged"]:
        return {
            "used": True,
            "rest_merge_attempted": False,
            "remote_already_merged": True,
            "reason": "graphql_transport_failure",
            "graphql_error": graphql_error[:1000],
            "expected_head": normalized_head,
            "merge_commit": before.get("merge_commit"),
            "verified": _verified_pr_from_rest_readback(before),
        }
    if before["state"] != "OPEN":
        raise WorkflowError(
            f"GitHub REST merge fallback requires an open PR; observed state={before['state'] or 'missing'}"
        )

    endpoint = f"repos/{GITHUB_REPO}/pulls/{before['pr_number']}/merge"
    command = [
        "gh",
        "api",
        "--method",
        "PUT",
        endpoint,
        "-f",
        f"sha={normalized_head}",
        "-f",
        "merge_method=squash",
    ]
    if bug_id:
        merge_result = _execute_workflow_command(
            bug_id,
            command,
            state="merged",
            cwd=REPO_ROOT,
            timeout=60,
            event="command:gh_rest_head_pinned_merge_fallback",
            allow_failure=True,
        )
    else:
        merge_result = _run_command(command, cwd=REPO_ROOT, timeout=60)

    after = _github_pull_rest_readback(pr_url)
    if after["head_sha"].lower() != normalized_head:
        raise WorkflowError(
            "PR head changed during GitHub REST merge fallback: "
            f"expected={normalized_head}, observed={after['head_sha'] or 'missing'}"
        )
    if not after["merged"]:
        message = str(merge_result.get("stderr") or merge_result.get("stdout") or "GitHub REST merge failed")
        if _looks_like_github_transport_failure(message):
            raise GitHubOutcomeUnknownError(
                f"{message}; head-pinned GitHub REST merge outcome is unknown and readback is not merged"
            )
        raise WorkflowError(message)

    try:
        merge_payload = json.loads(str(merge_result.get("stdout") or "{}")) if merge_result.get("ok") else {}
    except json.JSONDecodeError:
        merge_payload = {}
    response_merge_commit = str((merge_payload or {}).get("sha") or "").strip()
    if response_merge_commit and after.get("merge_commit") != response_merge_commit:
        raise WorkflowError(
            "GitHub REST merge commit readback mismatch: "
            f"response={response_merge_commit}, observed={after.get('merge_commit') or 'missing'}"
        )

    return {
        "used": True,
        "rest_merge_attempted": True,
        "remote_already_merged": False,
        "reason": "graphql_transport_failure",
        "graphql_error": graphql_error[:1000],
        "expected_head": normalized_head,
        "merge_commit": after.get("merge_commit"),
        "merge_result": merge_result,
        "verified": _verified_pr_from_rest_readback(after),
    }


def _complete_pr_merge_attempt(
    *,
    pr_url: str,
    expected_head: str,
    check_summary: dict[str, Any],
    merge_result: dict[str, Any],
    bug_id: str | None = None,
) -> dict[str, Any]:
    if merge_result.get("ok"):
        try:
            verified = _verify_pr_merged(pr_url)
        except WorkflowError as exc:
            message = str(exc)
            if not _looks_like_github_transport_failure(message):
                raise
            readback = _github_pull_rest_readback(pr_url)
            if readback["head_sha"].lower() != expected_head.strip().lower():
                raise WorkflowError(
                    "PR head changed before GitHub REST merge verification: "
                    f"expected={expected_head or 'missing'}, observed={readback['head_sha'] or 'missing'}"
                ) from exc
            if not readback["merged"]:
                raise GitHubOutcomeUnknownError(
                    f"{message}; gh pr merge reported success but GitHub REST readback is not merged"
                ) from exc
            fallback = {
                "used": True,
                "rest_merge_attempted": False,
                "remote_already_merged": True,
                "reason": "graphql_verification_transport_failure",
                "graphql_error": message[:1000],
                "expected_head": expected_head.strip().lower(),
                "merge_commit": readback.get("merge_commit"),
            }
            if bug_id:
                _append_event(
                    bug_id,
                    event="merge_graphql_verification_rest_readback",
                    state="merged",
                    result="recovered",
                    evidence={
                        "pr_url": pr_url,
                        "expected_head": expected_head,
                        "merge_commit": readback.get("merge_commit"),
                    },
                )
            return {
                "already_merged": False,
                "check_summary": check_summary,
                "merge_result": merge_result,
                "verified": _verified_pr_from_rest_readback(readback),
                "recovered_from_transport_error": True,
                "rest_fallback": fallback,
            }
        return {
            "already_merged": False,
            "check_summary": check_summary,
            "merge_result": merge_result,
            "verified": verified,
        }

    message = str(merge_result.get("stderr") or merge_result.get("stdout") or "gh pr merge failed")
    if _looks_like_github_transport_failure(message):
        fallback = _head_pinned_rest_merge_after_transport_failure(
            pr_url=pr_url,
            expected_head=expected_head,
            graphql_error=message,
            bug_id=bug_id,
        )
        if bug_id:
            _append_event(
                bug_id,
                event="merge_graphql_transport_rest_fallback",
                state="merged",
                result="recovered",
                evidence={
                    "pr_url": pr_url,
                    "expected_head": expected_head,
                    "rest_merge_attempted": fallback["rest_merge_attempted"],
                    "merge_commit": fallback.get("merge_commit"),
                },
            )
        return {
            "already_merged": bool(fallback["remote_already_merged"]),
            "check_summary": check_summary,
            "merge_result": merge_result,
            "verified": fallback["verified"],
            "recovered_from_local_merge_error": True,
            "recovered_from_transport_error": True,
            "rest_fallback": fallback,
        }

    try:
        verified = _verify_pr_merged(pr_url)
    except WorkflowError as exc:
        raise WorkflowError(message) from exc
    return {
        "already_merged": True,
        "check_summary": check_summary,
        "merge_result": merge_result,
        "verified": verified,
        "recovered_from_local_merge_error": True,
    }


def _await_required_pr_checks(
    pr_url: str,
    *,
    payload: dict[str, Any],
    bug_id: str | None = None,
    attempts: int = 6,
    delay_seconds: int = 10,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, list[str]], list[dict[str, Any]]]:
    """Poll queued required checks briefly instead of forcing manual retries.

    This remains fail-closed: only a completely green result returns to the
    merge caller; a failed check or an exhausted bounded wait is reported as
    blocked.  The short poll is limited to the merge operation and never
    becomes a long ``gh pr checks --watch`` loop.
    """

    max_attempts = max(1, int(attempts))
    wait_seconds = max(0, int(delay_seconds))
    history: list[dict[str, Any]] = []
    latest_result: dict[str, Any] = {"ok": False, "stdout": "[]", "stderr": "not run"}
    latest_fallback: dict[str, Any] | None = None
    latest_summary: dict[str, list[str]] = {"failed": [], "pending": [], "non_blocking": [], "passed": []}
    for index in range(1, max_attempts + 1):
        latest_result, latest_fallback = _merge_required_check_result_with_transport_fallback(
            pr_url,
            payload=payload,
            bug_id=bug_id,
        )
        latest_summary = _required_pr_check_summary(latest_result)
        history.append(
            {
                "attempt": index,
                "failed": list(latest_summary["failed"]),
                "pending": list(latest_summary["pending"]),
                "passed": list(latest_summary["passed"]),
            }
        )
        if latest_summary["failed"] or not latest_summary["pending"]:
            return latest_result, latest_fallback, latest_summary, history
        if index < max_attempts and wait_seconds:
            time.sleep(wait_seconds)
    if bug_id:
        _append_event(
            bug_id,
            event="required_checks_bounded_wait_exhausted",
            state="blocked",
            result="pending",
            evidence={"pr_url": pr_url, "attempts": history},
        )
    return latest_result, latest_fallback, latest_summary, history


def _merge_pr_if_ready(pr_url: str) -> dict[str, Any]:
    payload, view_fallback = _merge_pr_view_with_transport_fallback(pr_url)
    if payload.get("state") == "MERGED":
        result = {"already_merged": True, "view": payload}
        if view_fallback:
            result["read_fallbacks"] = [view_fallback]
        return result
    required_result, checks_fallback, check_summary, check_history = _await_required_pr_checks(
        pr_url,
        payload=payload,
    )
    failed = check_summary["failed"]
    pending = check_summary["pending"]
    if failed or pending:
        raise WorkflowError(f"PR checks are not green; failed={failed}, pending={pending}")
    result = _run_command(["gh", "pr", "merge", pr_url, "--squash"], cwd=REPO_ROOT, timeout=180)
    completed = _complete_pr_merge_attempt(
        pr_url=pr_url,
        expected_head=str(payload.get("headRefOid") or ""),
        check_summary=check_summary,
        merge_result=result,
    )
    read_fallbacks = [item for item in (view_fallback, checks_fallback) if item]
    if read_fallbacks:
        completed["read_fallbacks"] = read_fallbacks
    completed["required_checks_poll"] = check_history
    return completed


def _merge_pr_if_ready_for_bug(
    bug_id: str,
    pr_url: str,
    *,
    required_check_attempts: int = 6,
    required_check_delay_seconds: int = 10,
) -> dict[str, Any]:
    payload, view_fallback = _merge_pr_view_with_transport_fallback(pr_url, bug_id=bug_id)
    if payload.get("state") == "MERGED":
        result = {"already_merged": True, "view": payload}
        if view_fallback:
            result["read_fallbacks"] = [view_fallback]
        return result
    required_result, checks_fallback, check_summary, check_history = _await_required_pr_checks(
        pr_url,
        payload=payload,
        bug_id=bug_id,
        attempts=required_check_attempts,
        delay_seconds=required_check_delay_seconds,
    )
    failed = check_summary["failed"]
    pending = check_summary["pending"]
    if failed or pending:
        raise WorkflowError(f"PR checks are not green; failed={failed}, pending={pending}")
    result = _execute_workflow_command(
        bug_id,
        ["gh", "pr", "merge", pr_url, "--squash"],
        state="merged",
        cwd=REPO_ROOT,
        timeout=180,
        event="command:gh_pr_merge",
        allow_failure=True,
    )
    completed = _complete_pr_merge_attempt(
        pr_url=pr_url,
        expected_head=str(payload.get("headRefOid") or ""),
        check_summary=check_summary,
        merge_result=result,
        bug_id=bug_id,
    )
    if completed.get("recovered_from_local_merge_error") and not completed.get("recovered_from_transport_error"):
        _append_event(
            bug_id,
            event="merge_remote_verified_after_local_error",
            state="merged",
            result="recovered",
            evidence={
                "pr_url": pr_url,
                "merge_error": result.get("stderr") or result.get("stdout"),
                "merge_commit": _merge_commit_from_pr_check(completed.get("verified")),
            },
        )
    read_fallbacks = [item for item in (view_fallback, checks_fallback) if item]
    if read_fallbacks:
        completed["read_fallbacks"] = read_fallbacks
    completed["required_checks_poll"] = check_history
    return completed


def _close_sync_changed_files(close_sync: dict[str, Any]) -> list[str]:
    root = Path(str(close_sync.get("registry_root") or REPO_ROOT))
    updated = str(close_sync.get("updated_bug_json") or close_sync.get("source_bug_json") or "")
    files: list[str] = []
    status = _run_command(["git", "status", "--porcelain=v1", "--", "tests/aistock_validation/bugs"], cwd=root)
    if status.get("ok"):
        for line in str(status.get("stdout") or "").splitlines():
            rel = _parse_git_porcelain_path(line)
            if rel and rel.replace("\\", "/").startswith("tests/aistock_validation/bugs/"):
                files.append(rel.replace("\\", "/"))
    elif updated:
        files.append(updated.replace("\\", "/"))
    return sorted(set(files))


def _rest_open_pr_for_branch(
    branch: str,
    *,
    root: Path,
    base: str = "main",
    expected_head: str | None = None,
) -> dict[str, Any] | None:
    owner = GITHUB_REPO.split("/", 1)[0]
    query = urllib.parse.urlencode(
        {"state": "open", "head": f"{owner}:{branch}", "base": base, "per_page": "2"}
    )
    result = _run_transport_read_with_retry(
        ["gh", "api", f"repos/{GITHUB_REPO}/pulls?{query}"],
        cwd=root,
        timeout=60,
        attempts=2,
    )
    if not result.get("ok"):
        detail = result.get("stderr") or result.get("stdout") or "unknown transport error"
        raise WorkflowError(f"cannot query open PRs through REST: {detail}")
    try:
        rows = json.loads(str(result.get("stdout") or "[]"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"cannot parse open PR REST readback: {exc}") from exc
    if not isinstance(rows, list) or not rows:
        return None
    if len(rows) > 1:
        raise WorkflowError(f"multiple open PRs found for {branch} -> {base}")
    row = rows[0] if isinstance(rows[0], dict) else {}
    head = row.get("head") if isinstance(row.get("head"), dict) else {}
    observed_head = str(head.get("sha") or "").strip()
    observed_branch = str(head.get("ref") or "").strip()
    observed_base = str(((row.get("base") or {}).get("ref") if isinstance(row.get("base"), dict) else "") or "")
    if observed_branch != branch or observed_base != base:
        raise WorkflowError("GitHub REST PR readback returned a different head/base")
    if expected_head and observed_head != expected_head:
        raise WorkflowError(
            f"PR head changed before create readback: expected {expected_head}, observed {observed_head or 'missing'}"
        )
    return {
        "number": row.get("number"),
        "url": row.get("html_url"),
        "headRefName": observed_branch,
        "headRefOid": observed_head,
        "title": row.get("title"),
        "source": "github_rest",
    }


def _rest_remote_branch_sha(branch: str, *, root: Path) -> str:
    encoded = urllib.parse.quote(branch, safe="")
    result = _run_transport_read_with_retry(
        ["gh", "api", f"repos/{GITHUB_REPO}/git/ref/heads/{encoded}"],
        cwd=root,
        timeout=60,
        attempts=2,
    )
    if not result.get("ok"):
        raise WorkflowError(result.get("stderr") or result.get("stdout") or "cannot read remote branch head")
    try:
        payload = json.loads(str(result.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"cannot parse remote branch readback: {exc}") from exc
    return str(((payload.get("object") or {}).get("sha") if isinstance(payload, dict) else "") or "").strip()


def _create_pr_with_transport_fallback(
    *,
    branch: str,
    base: str,
    title: str,
    body_path: Path,
    expected_head: str,
    root: Path,
) -> dict[str, Any]:
    primary = _run_command(
        [
            "gh",
            "pr",
            "create",
            "--repo",
            GITHUB_REPO,
            "--base",
            base,
            "--head",
            branch,
            "--title",
            title,
            "--body-file",
            str(body_path),
        ],
        cwd=root,
        timeout=120,
    )
    if primary.get("ok"):
        return {**primary, "recovered_from_transport_error": False, "source": "github_graphql"}

    message = f"{primary.get('stdout')}\n{primary.get('stderr')}"
    transport_failure = _looks_like_github_transport_failure(message)
    already_exists = "already exists" in message.casefold()
    if not transport_failure and not already_exists:
        return primary

    existing = _rest_open_pr_for_branch(
        branch,
        root=root,
        base=base,
        expected_head=expected_head,
    )
    if existing:
        return {
            "ok": True,
            "returncode": 0,
            "stdout": str(existing.get("url") or ""),
            "stderr": "",
            "recovered_from_transport_error": transport_failure,
            "source": "github_rest_existing_readback",
            "pr": existing,
        }
    if already_exists and not transport_failure:
        return primary

    remote_head = _rest_remote_branch_sha(branch, root=root)
    if remote_head != expected_head:
        raise WorkflowError(
            f"remote branch changed before REST PR create: expected {expected_head}, observed {remote_head or 'missing'}"
        )
    created = _run_command(
        [
            "gh",
            "api",
            "--method",
            "POST",
            f"repos/{GITHUB_REPO}/pulls",
            "-f",
            f"title={title}",
            "-f",
            f"head={branch}",
            "-f",
            f"base={base}",
            "-F",
            f"body=@{body_path}",
        ],
        cwd=root,
        timeout=120,
    )
    if not created.get("ok"):
        created_message = f"{created.get('stdout')}\n{created.get('stderr')}"
        if _looks_like_github_transport_failure(created_message):
            existing = _rest_open_pr_for_branch(
                branch,
                root=root,
                base=base,
                expected_head=expected_head,
            )
            if existing:
                return {
                    "ok": True,
                    "returncode": 0,
                    "stdout": str(existing.get("url") or ""),
                    "stderr": "",
                    "recovered_from_transport_error": True,
                    "source": "github_rest_post_readback",
                    "pr": existing,
                }
        return created
    try:
        payload = json.loads(str(created.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"cannot parse REST PR create response: {exc}") from exc
    head = payload.get("head") if isinstance(payload.get("head"), dict) else {}
    observed_head = str(head.get("sha") or "").strip()
    observed_base = str(((payload.get("base") or {}).get("ref") if isinstance(payload.get("base"), dict) else "") or "")
    if observed_head != expected_head or observed_base != base:
        raise WorkflowError("REST PR create readback did not preserve the verified head/base")
    return {
        "ok": True,
        "returncode": 0,
        "stdout": str(payload.get("html_url") or ""),
        "stderr": "",
        "recovered_from_transport_error": True,
        "source": "github_rest_create",
        "pr": payload,
    }


def _open_pr_for_branch(
    branch: str,
    *,
    root: Path,
    expected_head: str | None = None,
) -> dict[str, Any] | None:
    if not branch:
        return None
    existing = _run_command(
        [
            "gh",
            "pr",
            "list",
            "--repo",
            GITHUB_REPO,
            "--head",
            branch,
            "--state",
            "open",
            "--json",
            "number,url,headRefName,headRefOid,title",
            "--limit",
            "1",
        ],
        cwd=root,
        timeout=60,
    )
    if not existing.get("ok"):
        return _rest_open_pr_for_branch(branch, root=root, expected_head=expected_head)
    try:
        rows = json.loads(str(existing.get("stdout") or "[]"))
    except json.JSONDecodeError:
        return _rest_open_pr_for_branch(branch, root=root, expected_head=expected_head)
    if not isinstance(rows, list):
        return _rest_open_pr_for_branch(branch, root=root, expected_head=expected_head)
    row = rows[0] if isinstance(rows, list) and rows else None
    if not isinstance(row, dict):
        return None
    observed_head = str(row.get("headRefOid") or "").strip()
    if expected_head and observed_head != expected_head:
        return _rest_open_pr_for_branch(branch, root=root, expected_head=expected_head)
    return row


def _close_sync_expected_registry_files(close_sync: dict[str, Any]) -> list[str]:
    files = flow._unique_strings(
        [
            close_sync.get("updated_bug_json"),
            close_sync.get("source_bug_json"),
            *[
                item.get("source_bug_json")
                for item in close_sync.get("per_issue") or []
                if isinstance(item, dict)
            ],
        ]
    )
    return sorted(
        path.replace("\\", "/")
        for path in files
        if path.replace("\\", "/").startswith("tests/aistock_validation/bugs/")
    )


def _inspect_pending_close_sync_commit(
    *,
    root: Path,
    branch: str,
    expected_subject: str,
    expected_files: list[str],
) -> dict[str, Any] | None:
    """Validate the clean local close-sync HEAD left after a transport failure."""
    commands = {
        "branch": ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        "head": ["git", "rev-parse", "HEAD"],
        "merge_base": ["git", "merge-base", "HEAD", "origin/main"],
    }
    results = {name: _run_command(args, cwd=root, timeout=30) for name, args in commands.items()}
    for name, result in results.items():
        if not result.get("ok"):
            raise WorkflowError(
                result.get("stderr") or result.get("stdout") or f"cannot inspect close-sync {name}"
            )
    observed_branch = str(results["branch"].get("stdout") or "").strip()
    if observed_branch != branch:
        raise WorkflowError(
            f"close-sync worktree branch changed: expected {branch}, observed {observed_branch or 'missing'}"
        )
    head = str(results["head"].get("stdout") or "").strip()
    merge_base = str(results["merge_base"].get("stdout") or "").strip()
    if not re.fullmatch(r"[0-9a-fA-F]{40}", head) or not re.fullmatch(r"[0-9a-fA-F]{40}", merge_base):
        raise WorkflowError("cannot resolve close-sync HEAD or merge-base identity")
    count = _run_command(["git", "rev-list", "--count", f"{merge_base}..HEAD"], cwd=root, timeout=30)
    if not count.get("ok"):
        raise WorkflowError(count.get("stderr") or count.get("stdout") or "cannot count close-sync commits")
    try:
        ahead_count = int(str(count.get("stdout") or "").strip())
    except ValueError as exc:
        raise WorkflowError("cannot parse close-sync ahead commit count") from exc
    if ahead_count == 0:
        return None
    parents = _run_command(["git", "rev-list", "--parents", "-n", "1", "HEAD"], cwd=root, timeout=30)
    if not parents.get("ok") or len(str(parents.get("stdout") or "").split()) != 2:
        raise WorkflowError("close-sync recovery refuses a merge commit or missing parent identity")
    subject = _run_command(["git", "show", "-s", "--format=%s", "HEAD"], cwd=root, timeout=30)
    observed_subject = str(subject.get("stdout") or "").strip()
    if not subject.get("ok") or observed_subject != expected_subject:
        raise WorkflowError(
            f"close-sync recovery commit subject mismatch: expected {expected_subject!r}, observed {observed_subject!r}"
        )
    diff = _run_command(
        ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"],
        cwd=root,
        timeout=30,
    )
    if not diff.get("ok"):
        raise WorkflowError(diff.get("stderr") or diff.get("stdout") or "cannot inspect close-sync commit files")
    observed_files = sorted(
        path.strip().replace("\\", "/")
        for path in str(diff.get("stdout") or "").splitlines()
        if path.strip()
    )
    if not expected_files or observed_files != sorted(expected_files):
        raise WorkflowError(
            "close-sync recovery commit files differ from the current BUG registry identity: "
            f"expected={sorted(expected_files)}, observed={observed_files}"
        )
    return {
        "commit": head,
        "changed_files": observed_files,
        "merge_base": merge_base,
        "ahead_of_merge_base": ahead_count,
    }


def _remote_close_sync_branch_head(branch: str, *, root: Path) -> str | None:
    result = _run_transport_read_with_retry(
        ["git", "ls-remote", "--heads", "origin", branch],
        cwd=root,
        timeout=60,
        attempts=2,
    )
    if not result.get("ok"):
        raise WorkflowError(result.get("stderr") or result.get("stdout") or "cannot read close-sync remote branch")
    rows = [line.split() for line in str(result.get("stdout") or "").splitlines() if line.strip()]
    if not rows:
        return None
    if len(rows) != 1 or len(rows[0]) != 2 or rows[0][1] != f"refs/heads/{branch}":
        raise WorkflowError("close-sync remote branch readback returned an ambiguous ref")
    sha = rows[0][0]
    if not re.fullmatch(r"[0-9a-fA-F]{40}", sha):
        raise WorkflowError("close-sync remote branch readback returned an invalid SHA")
    return sha


def _is_single_commit_fast_forward(*, root: Path, ancestor: str, descendant: str) -> bool:
    relation = _run_command(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=root,
        timeout=30,
    )
    if not relation.get("ok"):
        return False
    count = _run_command(["git", "rev-list", "--count", f"{ancestor}..{descendant}"], cwd=root, timeout=30)
    return bool(count.get("ok") and str(count.get("stdout") or "").strip() == "1")


def _write_close_sync_pr_body(
    *,
    root: Path,
    close_sync: dict[str, Any],
    label: str,
    title_label: str,
    bug_ids: list[str],
    changed_files: list[str],
    validation_evidence: list[str],
) -> Path:
    body_path = root / WORKFLOW_ROOT / label / "close-sync-pr-body.md"
    status_rows: list[str] = []
    for changed_file in changed_files:
        candidate = root / changed_file
        if not candidate.is_file():
            continue
        with contextlib.suppress(OSError, json.JSONDecodeError, WorkflowError):
            changed_record = _load_json(candidate)
            status_rows.append(
                f"{changed_record.get('bug_id') or candidate.stem}={changed_record.get('status') or 'unknown'}"
            )
    status_summary = ", ".join(status_rows) or "unknown"
    per_issue_sources = [
        item
        for item in close_sync.get("per_issue") or []
        if isinstance(item, dict) and item.get("source_pr_url")
    ]
    source_summary = (
        "multiple independently verified source PRs"
        if close_sync.get("schema_version") == "aistock_issue_workflow_close_sync_aggregate_v1"
        else (close_sync.get("merged_pr") or "n/a")
    )
    merge_summary = (
        "per-BUG identities below"
        if close_sync.get("schema_version") == "aistock_issue_workflow_close_sync_aggregate_v1"
        else f"`{close_sync.get('merge_commit') or 'unknown'}`"
    )
    body_lines = [
        f"## {title_label} close-sync",
        "",
        f"- Source PR: {source_summary}",
        f"- Merge commit: {merge_summary}",
        f"- BUG IDs: `{', '.join(bug_ids)}`",
        f"- BUG JSON status: `{status_summary}`",
        "- Note: this PR persists registry close-sync metadata; final completion requires this PR to merge into `origin/main`.",
        "",
        "## Validation",
        *[f"- {item}" for item in validation_evidence or close_sync.get("validation_evidence") or ["n/a"]],
    ]
    if close_sync.get("schema_version") == "aistock_issue_workflow_close_sync_aggregate_v1":
        body_lines.extend(["", "## Per-BUG source identities"])
        body_lines.extend(
            f"- {item.get('bug_id')}: {item.get('source_pr_url')} @ `{item.get('source_merge_commit') or 'unknown'}`"
            for item in per_issue_sources
        )
    source_receipt = close_sync.get("source_merge_receipt")
    if isinstance(source_receipt, dict):
        body_lines.extend(
            [
                "",
                "## Source merge receipt",
                f"- schema: `{source_receipt.get('schema_version') or 'unknown'}`",
                f"- receipt_id: `{source_receipt.get('receipt_id') or 'unknown'}`",
                f"- source_merge_commit: `{source_receipt.get('source_merge_commit') or 'unknown'}`",
                f"- runtime_verification: `{source_receipt.get('runtime_verification') or 'unknown'}`",
            ]
        )
    body_lines.extend(
        [
            "",
            "## Production gates",
            *[f"- {key}: `{value}`" for key, value in sorted((close_sync.get("production_gates") or {}).items())],
        ]
    )
    _write_text(body_path, "\n".join(body_lines) + "\n")
    return body_path


def _maybe_commit_and_pr_close_sync(
    *,
    bug_id: str,
    close_sync: dict[str, Any],
    validation_evidence: list[str],
) -> dict[str, Any]:
    root = Path(str(close_sync.get("registry_root") or ""))
    branch = ((close_sync.get("registry_worktree_plan") or {}).get("branch") or "")
    if not root.exists() or not branch:
        return {"workflow_gate": "skipped", "reason": "missing_close_sync_worktree"}
    bug_ids = flow._unique_strings(flow._as_list(close_sync.get("bug_ids")) or [bug_id])
    label = str(close_sync.get("aggregate_id") or close_sync.get("batch_id") or bug_id)
    title_label = label if len(bug_ids) > 1 else bug_id
    changed_files = _close_sync_changed_files(close_sync)
    dirty = _dirty_files(root)
    unexpected_dirty = sorted(
        path for path in dirty if not path.replace("\\", "/").startswith("tests/aistock_validation/bugs/")
    )
    if unexpected_dirty:
        raise WorkflowError(
            "close-sync worktree has unexpected dirty files outside BUG registry: "
            + ", ".join(unexpected_dirty[:10])
        )
    commit_message = f"chore(issue): close-sync {title_label} after merge"
    if not changed_files:
        if dirty:
            raise WorkflowError("close-sync worktree is dirty but no BUG registry change could be resolved")
        pending = _inspect_pending_close_sync_commit(
            root=root,
            branch=branch,
            expected_subject=commit_message,
            expected_files=_close_sync_expected_registry_files(close_sync),
        )
        if not pending:
            return {"workflow_gate": "no_changes", "root": str(root), "branch": branch}
        expected_head = str(pending["commit"])
        remote_head = _remote_close_sync_branch_head(branch, root=root)
        actions: list[dict[str, Any]] = []
        push_required = not remote_head
        if remote_head and remote_head != expected_head:
            if not _is_single_commit_fast_forward(
                root=root,
                ancestor=remote_head,
                descendant=expected_head,
            ):
                raise WorkflowError(
                    "close-sync remote branch diverges from the verified local recovery commit: "
                    f"local={expected_head}, remote={remote_head}"
                )
            push_required = True
        elif not remote_head and int(pending.get("ahead_of_merge_base") or 0) != 1:
            raise WorkflowError(
                "close-sync recovery without a remote branch requires exactly one local commit; "
                f"observed {pending.get('ahead_of_merge_base') or 0}"
            )
        if push_required:
            push = _run_command(["git", "push", "-u", "origin", branch], cwd=root, timeout=180)
            actions.append({"command": f"git push -u origin {branch}", "result": push})
            if not push.get("ok"):
                raise WorkflowError(push.get("stderr") or push.get("stdout") or "close-sync recovery push failed")
            remote_head = _remote_close_sync_branch_head(branch, root=root)
            if remote_head != expected_head:
                raise WorkflowError(
                    "close-sync recovery push readback mismatch: "
                    f"expected={expected_head}, observed={remote_head or 'missing'}"
                )
        existing_pr = _open_pr_for_branch(branch, root=root, expected_head=expected_head)
        if existing_pr:
            return {
                "workflow_gate": "pr_opened",
                "reason": "recovered_existing_close_sync_commit_and_pr",
                "root": str(root),
                "branch": branch,
                "changed_files": pending["changed_files"],
                "actions": actions,
                "commit": expected_head,
                "pr_url": existing_pr.get("url"),
                "open_pr": existing_pr,
            }
        body_path = _write_close_sync_pr_body(
            root=root,
            close_sync=close_sync,
            label=label,
            title_label=title_label,
            bug_ids=bug_ids,
            changed_files=pending["changed_files"],
            validation_evidence=validation_evidence,
        )
        pr = _create_pr_with_transport_fallback(
            branch=branch,
            base="main",
            title=f"chore(issue): close-sync {title_label}",
            body_path=body_path,
            expected_head=expected_head,
            root=root,
        )
        actions.append({"command": "gh pr create close-sync recovery", "result": pr})
        pr_url = _pr_url_from_create_output(pr)
        if not pr.get("ok") or not pr_url:
            raise WorkflowError(pr.get("stderr") or pr.get("stdout") or "close-sync recovery PR create failed")
        return {
            "workflow_gate": "pr_opened",
            "reason": "recovered_unpushed_close_sync_commit",
            "root": str(root),
            "branch": branch,
            "changed_files": pending["changed_files"],
            "actions": actions,
            "commit": expected_head,
            "pr_url": pr_url,
        }
    existing_pr = _open_pr_for_branch(branch, root=root)
    if existing_pr and not dirty:
        return {
            "workflow_gate": "pr_opened",
            "reason": "existing_open_close_sync_pr_for_branch",
            "root": str(root),
            "branch": branch,
            "changed_files": changed_files,
            "pr_url": existing_pr.get("url"),
            "open_pr": existing_pr,
            "commit": None,
        }

    started = time.monotonic()
    actions: list[dict[str, Any]] = []
    add = _run_command(["git", "add", *changed_files], cwd=root, timeout=60)
    actions.append({"command": f"git add {' '.join(changed_files)}", "result": add})
    if not add.get("ok"):
        raise WorkflowError(add.get("stderr") or add.get("stdout") or "close-sync git add failed")
    commit = _run_command(["git", "commit", "-m", commit_message], cwd=root, timeout=120)
    actions.append({"command": f"git commit -m {commit_message}", "result": commit})
    if not commit.get("ok") and "nothing to commit" not in f"{commit.get('stdout')}\n{commit.get('stderr')}".lower():
        raise WorkflowError(commit.get("stderr") or commit.get("stdout") or "close-sync git commit failed")
    commit_sha = _run_command(["git", "rev-parse", "HEAD"], cwd=root, timeout=30)
    expected_head = str(commit_sha.get("stdout") or "").strip()
    if not commit_sha.get("ok") or not re.fullmatch(r"[0-9a-fA-F]{40}", expected_head):
        raise WorkflowError(commit_sha.get("stderr") or "cannot resolve close-sync commit SHA")

    push = _run_command(["git", "push", "-u", "origin", branch], cwd=root, timeout=180)
    actions.append({"command": f"git push -u origin {branch}", "result": push})
    if not push.get("ok"):
        raise WorkflowError(push.get("stderr") or push.get("stdout") or "close-sync git push failed")

    if existing_pr:
        result = {
            "workflow_gate": "pr_opened",
            "reason": "updated_existing_open_close_sync_pr",
            "root": str(root),
            "branch": branch,
            "changed_files": changed_files,
            "actions": actions,
            "commit": expected_head,
            "pr_url": existing_pr.get("url"),
            "open_pr": existing_pr,
            "duration_seconds": round(time.monotonic() - started, 3),
        }
        _append_event(
            bug_id,
            event="close_sync_existing_pr_updated",
            state="close_synced",
            root=root,
            duration_seconds=result["duration_seconds"],
            evidence={
                "branch": branch,
                "commit": expected_head,
                "pr_url": existing_pr.get("url"),
                "changed_files": changed_files,
            },
        )
        return result

    body_path = _write_close_sync_pr_body(
        root=root,
        close_sync=close_sync,
        label=label,
        title_label=title_label,
        bug_ids=bug_ids,
        changed_files=changed_files,
        validation_evidence=validation_evidence,
    )
    pr = _create_pr_with_transport_fallback(
        branch=branch,
        base="main",
        title=f"chore(issue): close-sync {title_label}",
        body_path=body_path,
        expected_head=expected_head,
        root=root,
    )
    actions.append({"command": "gh pr create close-sync", "result": pr})
    pr_url = str(pr.get("stdout") or "").splitlines()[-1].strip() if pr.get("ok") else None
    if not pr.get("ok"):
        raise WorkflowError(pr.get("stderr") or pr.get("stdout") or "close-sync PR create failed")
    result = {
        "workflow_gate": "pr_opened" if pr.get("ok") or pr_url else "committed",
        "root": str(root),
        "branch": branch,
        "changed_files": changed_files,
        "actions": actions,
        "commit": str(commit_sha.get("stdout") or "").strip() if commit_sha.get("ok") else None,
        "pr_url": pr_url,
        "duration_seconds": round(time.monotonic() - started, 3),
    }
    _append_event(
        bug_id,
        event="close_sync_persisted",
        state="close_synced",
        root=root,
        duration_seconds=result["duration_seconds"],
        evidence={"branch": branch, "commit": result["commit"], "pr_url": pr_url, "changed_files": changed_files},
    )
    return result


def _attach_source_merge_receipts_to_close_sync(
    close_sync: dict[str, Any],
    *,
    source_pr_check: dict[str, Any] | None,
    merge_commit: str,
    validation_evidence: list[str],
    production_gates: dict[str, Any] | None,
) -> dict[str, Any]:
    """Persist source receipts in the tracked close-sync BUG JSON(s)."""
    root = Path(str(close_sync.get("registry_root") or ""))
    if not root.exists():
        raise WorkflowError("close-sync registry root is unavailable for source receipt persistence")
    records: list[tuple[str, Path, dict[str, Any]]] = []
    if close_sync.get("updated_bug_json"):
        bug_id = str(close_sync.get("bug_id") or "").upper()
        target = root / str(close_sync["updated_bug_json"])
        records.append((bug_id, target, close_sync))
    else:
        for item in close_sync.get("per_issue") or []:
            if not isinstance(item, dict):
                continue
            bug_id = str(item.get("bug_id") or "").upper()
            source_bug_json = str(item.get("source_bug_json") or "")
            if bug_id and source_bug_json:
                records.append((bug_id, root / source_bug_json, item))
    receipts: dict[str, dict[str, Any]] = {}
    for bug_id, target, metadata in records:
        if not target.is_file():
            raise WorkflowError(f"close-sync BUG JSON is missing for source receipt: {target}")
        record = _load_json(target)
        runtime_contract = metadata.get("runtime_contract") if isinstance(metadata, dict) else None
        if not isinstance(runtime_contract, dict):
            runtime_contract = close_sync.get("runtime_contract") if isinstance(close_sync.get("runtime_contract"), dict) else {}
        receipt = _build_source_merge_receipt(
            bug_id=bug_id or str(record.get("bug_id") or ""),
            source_pr_url=str(close_sync.get("merged_pr") or ""),
            source_pr_check=source_pr_check,
            merge_commit=merge_commit,
            validation_evidence=validation_evidence or flow._as_list(close_sync.get("validation_evidence")),
            runtime_contract=runtime_contract,
            production_gates=production_gates or close_sync.get("production_gates"),
        )
        existing_receipt = record.get("source_merge_receipt")
        if isinstance(existing_receipt, dict):
            profile = _source_merge_receipt_profile(
                existing_receipt,
                bug_id=bug_id or str(record.get("bug_id") or ""),
                source_pr_url=str(close_sync.get("merged_pr") or ""),
                merge_commit=merge_commit,
            )
            if profile.get("status") != "valid":
                raise WorkflowError(
                    "existing source merge receipt is invalid: "
                    + "; ".join(flow._as_list(profile.get("blocking")))
                )
            immutable_keys = (
                "bug_id",
                "source_pr_url",
                "source_head_oid",
                "source_merge_commit",
                "runtime_contract_digest",
                "runtime_verification",
                "runtime_identity_match",
                "production_gates",
            )
            if any(existing_receipt.get(key) != receipt.get(key) for key in immutable_keys):
                raise WorkflowError("existing source merge receipt immutable identity differs from the retry input")
            receipt = existing_receipt
        if record.get("source_merge_receipt") != receipt:
            record["source_merge_receipt"] = receipt
            _write_json(target, record)
        receipts[receipt["bug_id"]] = receipt
    close_sync["source_merge_receipts"] = receipts
    if len(receipts) == 1:
        close_sync["source_merge_receipt"] = next(iter(receipts.values()))
    return close_sync


def _source_merge_receipt_path_from_close_sync(close_sync: dict[str, Any] | None, *, bug_id: str) -> str | None:
    if not isinstance(close_sync, dict):
        return None
    root = str(close_sync.get("registry_root") or "").strip()
    relative = str(close_sync.get("updated_bug_json") or "").strip()
    if root and relative:
        return str(Path(root) / relative)
    for item in close_sync.get("per_issue") or []:
        if isinstance(item, dict) and str(item.get("bug_id") or "").upper() == bug_id.upper():
            source = str(item.get("source_bug_json") or "").strip()
            if root and source:
                return str(Path(root) / source)
    return None


def _source_merge_receipt_from_close_sync(
    close_sync: dict[str, Any] | None,
    *,
    bug_id: str,
) -> dict[str, Any] | None:
    if not isinstance(close_sync, dict):
        return None
    direct = close_sync.get("source_merge_receipt")
    if isinstance(direct, dict):
        return direct
    receipts = close_sync.get("source_merge_receipts")
    if isinstance(receipts, dict) and isinstance(receipts.get(bug_id.upper()), dict):
        return receipts[bug_id.upper()]
    receipt_path = _source_merge_receipt_path_from_close_sync(close_sync, bug_id=bug_id)
    if not receipt_path:
        return None
    path = Path(receipt_path)
    if not path.is_file():
        return None
    with contextlib.suppress(OSError, UnicodeError, json.JSONDecodeError, WorkflowError):
        candidate = _load_json(path)
        receipt = candidate.get("source_merge_receipt") if isinstance(candidate, dict) else None
        if isinstance(receipt, dict):
            return receipt
    return None


def _persist_source_merge_receipt_for_close_sync(
    close_sync: dict[str, Any],
    *,
    source_pr_check: dict[str, Any] | None,
    merge_commit: str,
    validation_evidence: list[str],
    production_gates: dict[str, Any] | None,
) -> dict[str, Any]:
    """Persist the receipt only for real close-sync payloads.

    Lightweight test doubles and historical markers do not own a writable
    close-sync worktree; they remain untouched and therefore cannot authorize
    source cleanup without an actual durable receipt.
    """
    schema = str(close_sync.get("schema_version") or "")
    if not schema.startswith("aistock_issue_workflow_close_sync"):
        return close_sync
    registry_root = Path(str(close_sync.get("registry_root") or ""))
    if not registry_root.exists():
        close_sync.setdefault(
            "source_merge_receipt_persistence",
            {
                "status": "blocked",
                "reason": "close-sync registry root is unavailable for source receipt persistence",
            },
        )
        return close_sync
    return _attach_source_merge_receipts_to_close_sync(
        close_sync,
        source_pr_check=source_pr_check,
        merge_commit=merge_commit,
        validation_evidence=validation_evidence,
        production_gates=production_gates,
    )


def _pr_url_from_create_output(result: dict[str, Any]) -> str | None:
    text = f"{result.get('stdout') or ''}\n{result.get('stderr') or ''}"
    match = re.search(r"https://github\.com/[^\s]+/pull/\d+", text)
    return match.group(0) if match else None


def _pr_number_from_url(pr_url: str | None) -> int | None:
    match = re.search(r"/pull/(\d+)(?:\D*$|$)", str(pr_url or ""))
    return int(match.group(1)) if match else None


def _bug_record_matches_close_sync(
    record: dict[str, Any],
    *,
    source_pr_url: str,
    merge_commit: str | None,
) -> bool:
    status = str(record.get("status") or "").strip().lower()
    record_pr_url = str(record.get("pr_url") or "").strip()
    record_commit = str(record.get("fix_commit") or "").strip()
    expected_commit = str(merge_commit or "").strip()
    if status not in {"fixed", "verified", "closed"}:
        return False
    if record_pr_url != source_pr_url:
        return False
    return not expected_commit or record_commit == expected_commit


def _find_bug_record_in_root(root: Path, bug_id: str) -> tuple[dict[str, Any], Path] | None:
    bugs_root = _bugs_root(root)
    if not bugs_root.exists():
        return None
    normalized = bug_id.strip().upper()
    for path in _bug_files(bugs_root):
        try:
            record = _load_json(path)
        except WorkflowError:
            continue
        if str(record.get("bug_id") or "").strip().upper() == normalized:
            return record, path
    return None


def _fetch_origin_main_for_close_sync(root: Path) -> dict[str, Any]:
    if not (root / ".git").exists() and not (root / ".git").is_file():
        return {"status": "skipped_no_git_checkout"}
    try:
        stdout = _git_fetch_with_transport_retry(["origin", "main", "--quiet"], cwd=root)
        result = {"ok": True, "returncode": 0, "stdout": stdout, "stderr": ""}
    except WorkflowError as exc:
        result = {"ok": False, "returncode": 1, "stdout": "", "stderr": str(exc)}
    return {"status": "fetched" if result.get("ok") else "warning", "result": result}


def _find_bug_record_in_git_ref(
    bug_id: str,
    *,
    ref: str = "origin/main",
    cwd: Path | None = None,
) -> tuple[dict[str, Any], str] | None:
    root = cwd or (_canonical_root() if _canonical_root().exists() else REPO_ROOT)
    if not (root / ".git").exists() and not (root / ".git").is_file():
        return None
    grep = _run_command(
        ["git", "grep", "-l", bug_id.strip().upper(), ref, "--", "tests/aistock_validation/bugs"],
        cwd=root,
        timeout=30,
    )
    if not grep.get("ok"):
        return None
    for line in str(grep.get("stdout") or "").splitlines():
        _, _, rel_path = line.partition(":")
        rel_path = rel_path.strip()
        if not rel_path or not rel_path.endswith(".json") or Path(rel_path).name.startswith("."):
            continue
        show = _run_command(["git", "show", f"{ref}:{rel_path}"], cwd=root, timeout=30)
        if not show.get("ok"):
            continue
        try:
            record = json.loads(str(show.get("stdout") or "{}"))
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and str(record.get("bug_id") or "").strip().upper() == bug_id.strip().upper():
            return record, f"{ref}:{rel_path}"
    return None


def _close_sync_bug_snapshot(
    *,
    bug_id: str,
    source_pr_url: str,
    merge_commit: str | None,
    issue_json: str | None = None,
) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_candidate(record: dict[str, Any], path_label: str, source: str, root: Path | None = None) -> None:
        key = f"{source}:{path_label}"
        if key in seen:
            return
        seen.add(key)
        candidates.append({"record": record, "path": path_label, "source": source, "root": str(root) if root else None})

    if issue_json:
        path = Path(issue_json)
        if path.exists():
            try:
                add_candidate(_load_json(path), str(path), "issue_json", path.parent)
            except WorkflowError:
                pass

    try:
        record, path = find_bug_record(bug_id=bug_id)
        add_candidate(record, _repo_rel(path, REPO_ROOT), "current_worktree", REPO_ROOT)
    except WorkflowError:
        pass

    canonical = _canonical_root()
    if canonical.exists():
        found = _find_bug_record_in_root(canonical, bug_id)
        if found:
            record, path = found
            add_candidate(record, _repo_rel(path, canonical), "canonical_root", canonical)

    for candidate in candidates:
        if _bug_record_matches_close_sync(candidate["record"], source_pr_url=source_pr_url, merge_commit=merge_commit):
            return candidate

    ref_root = canonical if canonical.exists() else REPO_ROOT
    fetch = _fetch_origin_main_for_close_sync(ref_root)
    found_in_ref = _find_bug_record_in_git_ref(bug_id, ref="origin/main", cwd=ref_root)
    if found_in_ref:
        record, path_label = found_in_ref
        if _bug_record_matches_close_sync(record, source_pr_url=source_pr_url, merge_commit=merge_commit):
            return {
                "record": record,
                "path": path_label,
                "source": "origin_main_ref",
                "root": str(ref_root),
                "fetch": fetch,
            }
    return None


def _close_sync_is_complete(
    *,
    bug_id: str,
    source_pr_url: str,
    merge_commit: str | None,
    issue_json: str | None = None,
) -> dict[str, Any] | None:
    """Return a completed close-sync marker when BUG JSON already reflects this source PR."""
    snapshot = _close_sync_bug_snapshot(
        bug_id=bug_id,
        source_pr_url=source_pr_url,
        merge_commit=merge_commit,
        issue_json=issue_json,
    )
    if not snapshot:
        return None
    record = snapshot["record"]
    status = str(record.get("status") or "").strip().lower()
    record_commit = str(record.get("fix_commit") or "").strip()
    expected_commit = str(merge_commit or "").strip()

    marker = {
        "workflow_gate": "already_close_synced",
        "bug_id": bug_id,
        "registry_root": str(snapshot.get("root") or REPO_ROOT),
        "source_bug_json": snapshot["path"],
        "updated_bug_json": snapshot["path"],
        "merged_pr": source_pr_url,
        "merge_commit": record_commit or expected_commit or None,
        "current_status": status,
        "snapshot_source": snapshot["source"],
        "production_gates": {
            "production_backend_dependency_gate": record.get("production_backend_dependency_gate"),
            "production_ddl_gate": record.get("production_ddl_gate"),
            "production_frontend_dependency_gate": record.get("production_frontend_dependency_gate"),
        },
        "reason": "bug_json_already_fixed_for_source_pr",
    }
    if snapshot.get("fetch"):
        marker["origin_main_fetch"] = snapshot["fetch"]
    stale = _stale_pr_check_for_bug(bug_id)
    source_pr_number = _pr_number_from_url(source_pr_url)
    merged_close_sync_prs = []
    open_close_sync_prs = []
    for item in stale.get("merged_prs") or []:
        if _looks_like_close_sync_pr_for_source(
            item,
            source_pr_number=source_pr_number,
            source_pr_url=source_pr_url,
        ):
            merged_close_sync_prs.append(item)
    for item in stale.get("open_prs") or []:
        if _looks_like_close_sync_pr_for_source(
            item,
            source_pr_number=source_pr_number,
            source_pr_url=source_pr_url,
        ):
            open_close_sync_prs.append(item)
    marker["stale_pr_check"] = stale
    marker["merged_close_sync_prs"] = merged_close_sync_prs
    marker["open_close_sync_prs"] = open_close_sync_prs
    if merged_close_sync_prs:
        marker["close_sync_pr"] = merged_close_sync_prs[0]
    if open_close_sync_prs:
        marker["open_close_sync_pr"] = open_close_sync_prs[0]
    return marker


def _looks_like_close_sync_pr_for_source(
    item: dict[str, Any],
    *,
    source_pr_number: int | None,
    source_pr_url: str,
) -> bool:
    number = int(item.get("number") or 0)
    title = str(item.get("title") or "").lower()
    body = str(item.get("body") or "")
    if source_pr_number and number == source_pr_number:
        return False
    if "close-sync" not in title and "close sync" not in title:
        return False
    if source_pr_url and source_pr_url in body:
        return True
    if source_pr_number and re.search(rf"(?:#|/pull/){source_pr_number}\b", body):
        return True
    # Older PR list payloads and tests may not include body. The title and BUG search still identify the retry target.
    return not body.strip()


def _close_sync_pr_in_progress_marker(
    *,
    bug_id: str,
    source_pr_url: str,
    merge_commit: str | None,
) -> dict[str, Any] | None:
    """Return an existing close-sync PR marker even before BUG JSON reaches origin/main."""
    stale = _stale_pr_check_for_bug(bug_id)
    if stale.get("status") == "unavailable":
        raise WorkflowError(
            "cannot verify whether a close-sync PR already exists; refusing duplicate creation: "
            + str(stale.get("error") or "GitHub PR state unavailable")
        )
    source_pr_number = _pr_number_from_url(source_pr_url)
    open_close_sync_prs = [
        item
        for item in stale.get("open_prs") or []
        if _looks_like_close_sync_pr_for_source(
            item,
            source_pr_number=source_pr_number,
            source_pr_url=source_pr_url,
        )
    ]
    if not open_close_sync_prs:
        return None
    pr = open_close_sync_prs[0]
    return {
        "workflow_gate": "close_sync_pr_open",
        "bug_id": bug_id,
        "registry_root": str(REPO_ROOT),
        "source_bug_json": None,
        "updated_bug_json": None,
        "merged_pr": source_pr_url,
        "merge_commit": merge_commit,
        "current_status": "unknown",
        "snapshot_source": "open_close_sync_pr",
        "reason": "existing_open_close_sync_pr_for_source",
        "stale_pr_check": stale,
        "open_close_sync_prs": open_close_sync_prs,
        "open_close_sync_pr": pr,
    }


def _close_sync_commit_already_merged(close_sync: dict[str, Any]) -> dict[str, Any]:
    pr = close_sync.get("close_sync_pr") or {}
    pr_url = str(pr.get("url") or "").strip()
    return {
        "workflow_gate": "already_merged",
        "reason": "close_sync_already_persisted",
        "branch": pr.get("headRefName"),
        "pr_url": pr_url or None,
        "commit": close_sync.get("merge_commit"),
    }


def _close_sync_commit_existing_open_pr(close_sync: dict[str, Any]) -> dict[str, Any]:
    pr = close_sync.get("open_close_sync_pr") or {}
    pr_url = str(pr.get("url") or "").strip()
    return {
        "workflow_gate": "pr_opened",
        "reason": "existing_open_close_sync_pr",
        "branch": pr.get("headRefName"),
        "pr_url": pr_url or None,
        "commit": close_sync.get("merge_commit"),
    }


def _close_sync_commit_needs_persistence(close_sync: dict[str, Any]) -> dict[str, Any]:
    return {
        "workflow_gate": "blocked",
        "reason": "close_sync_bug_json_fixed_but_not_persisted_to_origin_main",
        "blocking": [
            "BUG JSON is already fixed in the current close-sync snapshot, but no merged or open close-sync PR was found."
        ],
        "commit": close_sync.get("merge_commit"),
    }


def _merge_close_sync_pr_if_ready(
    *,
    bug_id: str,
    close_sync_commit: dict[str, Any],
    auto_merge: bool,
) -> dict[str, Any]:
    pr_url = str(close_sync_commit.get("pr_url") or "").strip()
    if not pr_url:
        return {
            "workflow_gate": "skipped",
            "reason": "missing_close_sync_pr_url",
            "auto_merge": auto_merge,
        }
    if not auto_merge:
        return {
            "workflow_gate": "ready_for_merge",
            "auto_merge": False,
            "pr_url": pr_url,
            "next_command": f"gh pr merge {pr_url} --squash",
        }
    try:
        # Close-sync CI normally queues behind the source merge's default-branch
        # CodeQL run on the single Windows runner.  Keep this wait bounded, but
        # long enough to avoid a guaranteed second manual finalizer invocation.
        result = _merge_pr_if_ready_for_bug(
            bug_id,
            pr_url,
            required_check_attempts=16,
            required_check_delay_seconds=30,
        )
    except WorkflowError as exc:
        return {
            "workflow_gate": "blocked",
            "auto_merge": True,
            "pr_url": pr_url,
            "blocking": [str(exc)],
            "next_command": (
                f"python scripts/aistock_issue_workflow.py watch-ci --bug-id {bug_id} "
                f"--pr-url {pr_url} --attempts 16 --delay-seconds 30"
            ),
        }
    return {
        "workflow_gate": "merged",
        "auto_merge": True,
        "pr_url": pr_url,
        "merge": result,
        "merge_commit": _merge_commit_from_pr_check(result.get("verified")) if isinstance(result, dict) else None,
    }


def _path_differs_from_canonical_root(path: Path) -> bool:
    canonical = _canonical_root()
    try:
        return path.resolve() != canonical.resolve()
    except OSError:
        return path != canonical


def _close_sync_worktree_for_cleanup(
    *,
    branch: str,
    close_sync_commit: dict[str, Any],
) -> str | None:
    root_text = str(close_sync_commit.get("root") or "").strip()
    if root_text:
        root_path = Path(root_text)
        if _path_differs_from_canonical_root(root_path):
            return str(root_path)
    discovered = _registered_worktree_for_branch(branch, cwd=_canonical_root()) if branch else None
    if discovered and _path_differs_from_canonical_root(discovered):
        return str(discovered)
    return None


def _build_close_sync_cleanup_after_merge_plan(
    *,
    bug_id: str,
    close_sync_commit: dict[str, Any],
    close_sync_pr_merge: dict[str, Any],
    cleanup: bool,
    apply: bool,
    sync_root: bool = False,
    verified_pr_check: dict[str, Any] | None = None,
    preflight_fetch: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not cleanup or close_sync_pr_merge.get("workflow_gate") not in {"merged", "already_merged"}:
        return None
    branch = str(close_sync_commit.get("branch") or "").strip()
    worktree = _close_sync_worktree_for_cleanup(
        branch=branch,
        close_sync_commit=close_sync_commit,
    )
    if not branch or not worktree:
        return None
    return build_cleanup_after_merge_plan(
        branch=branch,
        bug_id=bug_id,
        worktree=worktree,
        pr_url=close_sync_commit.get("pr_url") or close_sync_pr_merge.get("pr_url"),
        apply=bool(apply and close_sync_pr_merge.get("auto_merge")),
        sync_root=sync_root,
        verified_pr_check=verified_pr_check,
        preflight_fetch=preflight_fetch,
    )


def _cleanup_root_sync_deferred_payload(
    plan: dict[str, Any] | None,
    *,
    phase: str,
) -> dict[str, Any] | None:
    if not plan or plan.get("workflow_gate") != "blocked":
        return None
    blocking = [str(item) for item in plan.get("blocking") or [] if str(item).strip()]
    if not blocking or any("canonical root is dirty and not synced to origin/main" not in item for item in blocking):
        return None
    root = str(plan.get("canonical_root") or _canonical_root())
    return {
        "schema_version": "aistock_merge_finalizer_root_sync_deferred_v1",
        "workflow_gate": "deferred",
        "phase": phase,
        "reason": "canonical_root_dirty_not_synced_to_origin_main",
        "blocking": blocking,
        "canonical_root": root,
        "root_dirty_files": plan.get("root_dirty_files") or [],
        "unrelated_root_dirty_files": plan.get("unrelated_root_dirty_files") or [],
        "origin_equivalent_dirty_files": plan.get("origin_equivalent_dirty_files") or [],
        "root_git": plan.get("root_git") or {},
        "next_actions": ["resolve_or_commit_unrelated_root_dirty_files", "fast_forward_canonical_root_main"],
        "next_commands": [
            f'git -C "{root}" fetch origin --prune',
            f'git -C "{root}" merge --ff-only origin/main',
        ],
    }


def _build_cleanup_after_merge_plan_with_root_sync_deferral(
    *,
    phase: str,
    branch: str,
    bug_id: str,
    worktree: str | None,
    pr_url: str | None,
    apply: bool,
    sync_root: bool,
    source_merge_receipt: dict[str, Any] | None = None,
    source_receipt_path: str | None = None,
    verified_pr_check: dict[str, Any] | None = None,
    preflight_fetch: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    cleanup_kwargs: dict[str, Any] = {
        "branch": branch,
        "bug_id": bug_id,
        "worktree": worktree,
        "pr_url": pr_url,
        "apply": apply,
        "sync_root": sync_root,
        "verified_pr_check": verified_pr_check,
        "preflight_fetch": preflight_fetch,
    }
    if source_merge_receipt is not None:
        cleanup_kwargs["source_merge_receipt"] = source_merge_receipt
    if source_receipt_path:
        cleanup_kwargs["source_receipt_path"] = source_receipt_path
    try:
        plan = build_cleanup_after_merge_plan(**cleanup_kwargs)
    except CleanupBlockedError as exc:
        if not (apply and sync_root and "canonical root is dirty and not synced to origin/main" in str(exc)):
            raise
        plan = exc.payload
    deferred = _cleanup_root_sync_deferred_payload(plan, phase=phase) if sync_root else None
    if not deferred:
        return plan, None
    retry_kwargs = dict(cleanup_kwargs)
    retry_kwargs["sync_root"] = False
    retry_kwargs["preflight_fetch"] = plan.get("pre_cleanup_fetch")
    retry_kwargs["verified_pr_check"] = (plan.get("merge_verification") or {}).get("pr_check") or verified_pr_check
    retry = build_cleanup_after_merge_plan(**retry_kwargs)
    retry.setdefault("warnings", []).append(
        "canonical root sync deferred; cleanup retried without --sync-root to avoid blocking safe aftercare"
    )
    deferred["retry_without_root_sync"] = {
        "workflow_gate": retry.get("workflow_gate"),
        "branch": retry.get("branch"),
        "worktree": retry.get("worktree"),
        "sync_root": retry.get("sync_root"),
        "blocking": retry.get("blocking") or [],
    }
    return retry, deferred


def _build_close_sync_cleanup_after_merge_plan_with_root_sync_deferral(
    *,
    bug_id: str,
    close_sync_commit: dict[str, Any],
    close_sync_pr_merge: dict[str, Any],
    cleanup: bool,
    apply: bool,
    sync_root: bool = False,
    verified_pr_check: dict[str, Any] | None = None,
    preflight_fetch: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    try:
        plan = _build_close_sync_cleanup_after_merge_plan(
            bug_id=bug_id,
            close_sync_commit=close_sync_commit,
            close_sync_pr_merge=close_sync_pr_merge,
            cleanup=cleanup,
            apply=apply,
            sync_root=sync_root,
            verified_pr_check=verified_pr_check,
            preflight_fetch=preflight_fetch,
        )
    except CleanupBlockedError as exc:
        if not (apply and sync_root and "canonical root is dirty and not synced to origin/main" in str(exc)):
            raise
        plan = exc.payload
    deferred = _cleanup_root_sync_deferred_payload(plan, phase="close_sync_cleanup") if sync_root else None
    if not deferred:
        return plan, None
    retry = _build_close_sync_cleanup_after_merge_plan(
        bug_id=bug_id,
        close_sync_commit=close_sync_commit,
        close_sync_pr_merge=close_sync_pr_merge,
        cleanup=cleanup,
        apply=apply,
        sync_root=False,
        verified_pr_check=(plan.get("merge_verification") or {}).get("pr_check") or verified_pr_check,
        preflight_fetch=plan.get("pre_cleanup_fetch"),
    )
    if retry:
        retry.setdefault("warnings", []).append(
            "canonical root sync deferred; close-sync cleanup retried without --sync-root"
        )
        deferred["retry_without_root_sync"] = {
            "workflow_gate": retry.get("workflow_gate"),
            "branch": retry.get("branch"),
            "worktree": retry.get("worktree"),
            "sync_root": retry.get("sync_root"),
            "blocking": retry.get("blocking") or [],
        }
    return retry, deferred


def build_merge_finalizer_plan(
    *,
    bug_id: str | list[str],
    source_pr_url: str,
    source_branch: str | None,
    source_worktree: str | None,
    validation_evidence: list[str],
    issue_json: str | None = None,
    allow_missing_linkage: bool = False,
    production_gates: dict[str, str] | None = None,
    sync_root: bool = True,
    merge_close_sync_pr: bool = False,
    cleanup: bool = False,
    apply: bool = False,
    source_pr_check: dict[str, Any] | None = None,
) -> dict[str, Any]:
    canonical_bug_ids = flow._unique_strings(
        [str(item).strip().upper() for item in flow._as_list(bug_id) if str(item).strip()]
    )
    if not canonical_bug_ids:
        raise WorkflowError("merge-finalizer requires at least one --bug-id")
    canonical_bug_id = canonical_bug_ids[0]
    batch_mode = len(canonical_bug_ids) > 1
    evidence = [item for item in validation_evidence if item.strip()]
    blocking: list[str] = []
    warnings: list[str] = []
    if apply and not evidence:
        blocking.append("validation evidence is required before merge finalizer apply")
    if cleanup and not source_branch:
        blocking.append("--cleanup requires --source-branch")
    if cleanup and not source_worktree:
        warnings.append("--source-worktree is missing; cleanup can delete branches but cannot remove the task worktree")
    source_cleanup_deferred = bool(cleanup and apply and source_worktree and _cwd_is_inside(source_worktree))
    cleanup_cwd_relocation = None
    root_sync_deferrals: list[dict[str, Any]] = []
    if cleanup and apply:
        cleanup_cwd_relocation = _relocate_cwd_before_cleanup(source_worktree)
        if cleanup_cwd_relocation and not cleanup_cwd_relocation.get("relocated"):
            blocking.append(cleanup_cwd_relocation.get("error") or "failed to relocate cwd before cleanup")

    payload: dict[str, Any] = {
        "schema_version": "aistock_issue_workflow_merge_finalizer_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "bug_ids": canonical_bug_ids,
        "batch_mode": batch_mode,
        "source_pr_url": source_pr_url,
        "source_branch": source_branch,
        "source_worktree": source_worktree,
        "merge_close_sync_pr": merge_close_sync_pr,
        "cleanup_requested": cleanup,
        "sync_root": sync_root,
        "dry_run": not apply,
        "blocking": blocking,
        "warnings": warnings,
        "production_gates": production_gates or _production_gates_payload(),
        "cleanup_cwd_relocation": cleanup_cwd_relocation,
        "source_cleanup_deferred": source_cleanup_deferred,
    }
    if blocking:
        payload["workflow_gate"] = "blocked"
        return payload
    if not apply:
        payload["workflow_gate"] = "ready_for_apply"
        command_parts = [
            "python scripts/aistock_issue_workflow.py merge-finalizer",
            *(f"--bug-id {item}" for item in canonical_bug_ids),
            f"--source-pr-url {_shell_quote(source_pr_url)}",
        ]
        if issue_json:
            command_parts.append(f"--issue-json {_shell_quote(issue_json)}")
        if source_branch:
            command_parts.append(f"--source-branch {_shell_quote(source_branch)}")
        if source_worktree:
            command_parts.append(f"--source-worktree {_shell_quote(source_worktree)}")
        for item in evidence or ["<command> -> passed"]:
            command_parts.append(f"--validation-evidence {_shell_quote(item)}")
        if allow_missing_linkage:
            command_parts.append("--allow-missing-linkage")
        if sync_root:
            command_parts.append("--sync-root")
        if merge_close_sync_pr:
            command_parts.append("--merge-close-sync-pr")
        if cleanup:
            command_parts.append("--cleanup")
        for key, flag in (
            ("production_ddl_gate", "--production-ddl-gate"),
            ("production_frontend_dependency_gate", "--production-frontend-dependency-gate"),
            ("production_backend_dependency_gate", "--production-backend-dependency-gate"),
        ):
            command_parts.append(f"{flag} {_shell_quote(str(payload['production_gates'].get(key) or 'noop'))}")
        command_parts.append("--apply")
        payload["next_command"] = " ".join(command_parts)
        return payload

    source_pr_check = source_pr_check or _verify_pr_merged(source_pr_url)
    merge_commit = _merge_commit_from_pr_check(source_pr_check)
    client_publish = _publish_changed_clients_after_merge(
        merge_commit=merge_commit,
        sync_root=sync_root,
        apply=True,
    )
    payload["client_publish"] = client_publish
    if client_publish.get("workflow_gate") == "blocked":
        payload["workflow_gate"] = "blocked"
        payload["blocking"] = flow._unique_strings(client_publish.get("blocking") or [])
        payload["next_actions"] = ["restore_canonical_client_authority_then_resume_merge_finalizer"]
        return payload
    if batch_mode:
        incomplete_bug_ids: list[str] = []
        complete_markers: list[dict[str, Any]] = []
        for item in canonical_bug_ids:
            marker = _close_sync_is_complete(
                bug_id=item,
                source_pr_url=source_pr_url,
                merge_commit=merge_commit,
                issue_json=issue_json if item == canonical_bug_id else None,
            )
            if not marker:
                marker = _close_sync_pr_in_progress_marker(
                    bug_id=item,
                    source_pr_url=source_pr_url,
                    merge_commit=merge_commit,
                )
            if marker and (marker.get("close_sync_pr") or marker.get("snapshot_source") == "origin_main_ref"):
                complete_markers.append(marker)
            elif marker and marker.get("open_close_sync_pr"):
                complete_markers.append(marker)
            else:
                incomplete_bug_ids.append(item)
        if not incomplete_bug_ids and complete_markers:
            close_sync = dict(complete_markers[0])
            close_sync["bug_ids"] = canonical_bug_ids
            if close_sync.get("open_close_sync_pr"):
                close_sync_commit = _close_sync_commit_existing_open_pr(close_sync)
                close_sync_pr_merge = _merge_close_sync_pr_if_ready(
                    bug_id=canonical_bug_id,
                    close_sync_commit=close_sync_commit,
                    auto_merge=merge_close_sync_pr,
                )
            else:
                close_sync_commit = _close_sync_commit_already_merged(close_sync)
                close_sync_pr_merge = {
                    "workflow_gate": "already_merged",
                    "auto_merge": merge_close_sync_pr,
                    "pr_url": close_sync_commit.get("pr_url"),
                    "reason": "batch_close_sync_pr_or_bug_json_already_persisted",
                }
        else:
            close_sync = build_close_sync_batch_plan(
                bug_ids=canonical_bug_ids,
                pr_url=source_pr_url,
                apply=True,
                allow_missing_linkage=allow_missing_linkage,
                validation_evidence=evidence,
                merge_commit=merge_commit,
                production_gates=production_gates or _production_gates_payload(),
                create_registry_worktree=True,
            )
            close_sync = _persist_source_merge_receipt_for_close_sync(
                close_sync,
                source_pr_check=source_pr_check,
                merge_commit=merge_commit,
                validation_evidence=evidence,
                production_gates=production_gates or _production_gates_payload(),
            )
            close_sync_commit = _maybe_commit_and_pr_close_sync(
                bug_id=canonical_bug_id,
                close_sync=close_sync,
                validation_evidence=evidence,
            )
            close_sync_pr_merge = _merge_close_sync_pr_if_ready(
                bug_id=canonical_bug_id,
                close_sync_commit=close_sync_commit,
                auto_merge=merge_close_sync_pr,
            )
    else:
        close_sync = _close_sync_is_complete(
            bug_id=canonical_bug_id,
            source_pr_url=source_pr_url,
            merge_commit=merge_commit,
            issue_json=issue_json,
        )
        if not close_sync:
            close_sync = _close_sync_pr_in_progress_marker(
                bug_id=canonical_bug_id,
                source_pr_url=source_pr_url,
                merge_commit=merge_commit,
            )
        if close_sync:
            if close_sync.get("close_sync_pr") or close_sync.get("snapshot_source") == "origin_main_ref":
                close_sync_commit = _close_sync_commit_already_merged(close_sync)
                close_sync_pr_merge = {
                    "workflow_gate": "already_merged",
                    "auto_merge": merge_close_sync_pr,
                    "pr_url": close_sync_commit.get("pr_url"),
                    "reason": "close_sync_pr_or_bug_json_already_persisted",
                }
            elif close_sync.get("open_close_sync_pr"):
                close_sync_commit = _close_sync_commit_existing_open_pr(close_sync)
                close_sync_pr_merge = _merge_close_sync_pr_if_ready(
                    bug_id=canonical_bug_id,
                    close_sync_commit=close_sync_commit,
                    auto_merge=merge_close_sync_pr,
                )
            else:
                close_sync_commit = _close_sync_commit_needs_persistence(close_sync)
                close_sync_pr_merge = {
                    "workflow_gate": "blocked",
                    "auto_merge": merge_close_sync_pr,
                    "blocking": close_sync_commit.get("blocking") or [],
                    "reason": close_sync_commit.get("reason"),
                }
        else:
            close_sync = build_close_sync_plan(
                bug_id=canonical_bug_id,
                issue_json=issue_json,
                pr_url=source_pr_url,
                apply=True,
                allow_missing_linkage=allow_missing_linkage,
                validation_evidence=evidence,
                merge_commit=merge_commit,
                production_gates=production_gates or _production_gates_payload(),
                create_registry_worktree=True,
            )
            close_sync = _persist_source_merge_receipt_for_close_sync(
                close_sync,
                source_pr_check=source_pr_check,
                merge_commit=merge_commit,
                validation_evidence=evidence,
                production_gates=production_gates or _production_gates_payload(),
            )
            if close_sync.get("workflow_gate") == "fixed_source_pending_user_restart":
                close_sync_commit = _maybe_commit_and_pr_close_sync(
                    bug_id=canonical_bug_id,
                    close_sync=close_sync,
                    validation_evidence=evidence,
                )
                close_sync_pr_merge = {
                    "workflow_gate": "ready_for_merge" if close_sync_commit.get("pr_url") else "skipped",
                    "auto_merge": False,
                    "pr_url": close_sync_commit.get("pr_url"),
                    "reason": "runtime verification receipt is pending; keep source receipt close-sync PR open",
                }
            else:
                close_sync_commit = _maybe_commit_and_pr_close_sync(
                    bug_id=canonical_bug_id,
                    close_sync=close_sync,
                    validation_evidence=evidence,
                )
                close_sync_pr_merge = _merge_close_sync_pr_if_ready(
                    bug_id=canonical_bug_id,
                    close_sync_commit=close_sync_commit,
                    auto_merge=merge_close_sync_pr,
                )

    if close_sync.get("close_sync_pr") or close_sync.get("snapshot_source") == "origin_main_ref":
        completed_record, _ = find_bug_record(bug_id=canonical_bug_id, issue_json=issue_json)
        completed_status = str(completed_record.get("status") or "")
        if completed_status != "fixed_source_pending_user_restart":
            issue_number = completed_record.get("github_issue_number")
            compensation = (
                _sync_closed_issue_status_labels(issue_number, require_closed=True)
                if issue_number
                else {"ok": False, "reason": "missing github_issue_number"}
            )
            close_sync["github_issue_compensation"] = _pick(
                compensation,
                "ok",
                "skipped",
                "attempts",
                "verified",
                "reason",
            )
            if not compensation.get("ok"):
                payload["workflow_gate"] = "blocked"
                payload["blocking"] = ["merged close-sync GitHub Issue state/labels are not aligned"]
                payload["close_sync"] = close_sync
                return payload

    source_merge_receipt = _source_merge_receipt_from_close_sync(
        close_sync,
        bug_id=canonical_bug_id,
    )
    source_receipt_path = (
        _source_merge_receipt_path_from_close_sync(close_sync, bug_id=canonical_bug_id)
        if source_merge_receipt
        else None
    )
    cleanup_plan = None
    if cleanup and source_branch:
        if source_cleanup_deferred:
            deferred_cleanup_kwargs: dict[str, Any] = {
                "branch": source_branch,
                "bug_id": canonical_bug_id,
                "worktree": source_worktree,
                "pr_url": source_pr_url,
                "sync_root": sync_root,
            }
            if source_receipt_path:
                deferred_cleanup_kwargs["source_receipt_path"] = source_receipt_path
            cleanup_plan = _deferred_cleanup_from_safe_cwd_plan(
                **deferred_cleanup_kwargs,
            )
        else:
            cleanup_kwargs: dict[str, Any] = {
                "phase": "source_cleanup",
                "branch": source_branch,
                "bug_id": canonical_bug_id,
                "worktree": source_worktree,
                "pr_url": source_pr_url,
                "apply": apply,
                "sync_root": sync_root,
                "verified_pr_check": source_pr_check,
            }
            if source_merge_receipt:
                cleanup_kwargs["source_merge_receipt"] = source_merge_receipt
            if source_receipt_path:
                cleanup_kwargs["source_receipt_path"] = source_receipt_path
            cleanup_plan, cleanup_root_sync_deferred = _build_cleanup_after_merge_plan_with_root_sync_deferral(
                **cleanup_kwargs,
            )
            if cleanup_root_sync_deferred:
                root_sync_deferrals.append(cleanup_root_sync_deferred)
    cleanup_fetch_candidate = (cleanup_plan or {}).get("pre_cleanup_fetch")
    shared_cleanup_fetch = (
        cleanup_fetch_candidate
        if isinstance(cleanup_fetch_candidate, dict) and cleanup_fetch_candidate.get("status") == "fetched"
        else None
    )
    close_sync_cleanup_sync_root = bool(
        sync_root
        and close_sync_pr_merge.get("workflow_gate") in {"merged", "already_merged"}
        and (
            cleanup_plan is None
            or cleanup_plan.get("workflow_gate") != "cleanup_done"
            or not cleanup_plan.get("sync_root")
        )
    )
    close_sync_merge_result = close_sync_pr_merge.get("merge") or {}
    close_sync_verified_pr_check = (
        close_sync_pr_merge.get("verified")
        or (close_sync_merge_result.get("verified") if isinstance(close_sync_merge_result, dict) else None)
    )
    close_sync_cleanup_plan, close_sync_root_sync_deferred = _build_close_sync_cleanup_after_merge_plan_with_root_sync_deferral(
        bug_id=canonical_bug_id,
        close_sync_commit=close_sync_commit,
        close_sync_pr_merge=close_sync_pr_merge,
        cleanup=cleanup,
        apply=apply,
        sync_root=close_sync_cleanup_sync_root,
        verified_pr_check=close_sync_verified_pr_check,
        preflight_fetch=shared_cleanup_fetch,
    )
    if close_sync_root_sync_deferred:
        root_sync_deferrals.append(close_sync_root_sync_deferred)
    try:
        postmortem = build_postmortem_plan(bug_id=canonical_bug_id)
    except WorkflowError as exc:
        postmortem = {
            "schema_version": "aistock_issue_workflow_postmortem_v1",
            "workflow_gate": "skipped",
            "reason": str(exc),
        }
    final_blocking = []
    if close_sync_commit.get("workflow_gate") == "blocked":
        final_blocking.extend(close_sync_commit.get("blocking") or [])
    if close_sync_pr_merge.get("workflow_gate") == "blocked":
        final_blocking.extend(close_sync_pr_merge.get("blocking") or [])
    if cleanup_plan and cleanup_plan.get("workflow_gate") == "blocked":
        final_blocking.extend(cleanup_plan.get("blocking") or [])
    if close_sync_cleanup_plan and close_sync_cleanup_plan.get("workflow_gate") == "blocked":
        final_blocking.extend(close_sync_cleanup_plan.get("blocking") or [])

    cleanup_complete = bool(cleanup_plan and cleanup_plan.get("workflow_gate") == "cleanup_done")
    close_sync_persisted = close_sync_pr_merge.get("workflow_gate") in {"merged", "already_merged"}
    runtime_pending = close_sync.get("workflow_gate") == "fixed_source_pending_user_restart"
    close_sync_cleanup_complete = (
        close_sync_cleanup_plan is None or close_sync_cleanup_plan.get("workflow_gate") == "cleanup_done"
    )

    payload.update(
        {
            "workflow_gate": "blocked" if final_blocking else (
                "fixed_source_pending_user_restart"
                if runtime_pending
                else (
                    "complete" if cleanup_complete and close_sync_persisted and close_sync_cleanup_complete else "close_sync_persisted"
                )
            ),
            "blocking": final_blocking,
            "source_pr_check": source_pr_check,
            "source_merge_commit": merge_commit,
            "source_merge_receipt": source_merge_receipt,
            "source_receipt_path": source_receipt_path,
            "close_sync": close_sync,
            "close_sync_commit": close_sync_commit,
            "close_sync_pr_merge": close_sync_pr_merge,
            "cleanup": cleanup_plan,
            "close_sync_cleanup": close_sync_cleanup_plan,
            "postmortem": postmortem,
            "next_actions": [],
            "next_commands": [],
        }
    )
    if root_sync_deferrals:
        root_sync_deferred = {
            "schema_version": "aistock_merge_finalizer_root_sync_deferred_v1",
            "workflow_gate": "deferred",
            "reason": "canonical_root_dirty_not_synced_to_origin_main",
            "phases": root_sync_deferrals,
            "canonical_root": root_sync_deferrals[0].get("canonical_root"),
            "root_dirty_files": root_sync_deferrals[0].get("root_dirty_files") or [],
            "unrelated_root_dirty_files": root_sync_deferrals[0].get("unrelated_root_dirty_files") or [],
            "next_actions": root_sync_deferrals[0].get("next_actions") or [],
            "next_commands": root_sync_deferrals[0].get("next_commands") or [],
        }
        payload["root_sync_deferred"] = root_sync_deferred
        payload["warnings"].append(
            "canonical root sync was deferred because unrelated dirty files made fast-forward unsafe"
        )
    if close_sync_pr_merge.get("workflow_gate") == "ready_for_merge":
        payload["next_actions"].append("merge_close_sync_pr_after_checks_are_green")
    if runtime_pending:
        payload["next_actions"].extend(
            ["user_restart_catalog_target", "run_post_restart_verify", "rerun_close_sync_with_post_restart_receipt"]
        )
    if cleanup_plan is None and source_branch:
        payload["next_actions"].append("run_cleanup_after_merge")
    if cleanup_plan and cleanup_plan.get("workflow_gate") == "ready_for_cleanup":
        payload["next_actions"].append("rerun_cleanup_after_merge_with_apply")
        if cleanup_plan.get("next_command"):
            payload["next_commands"].append(cleanup_plan["next_command"])
    if close_sync_cleanup_plan and close_sync_cleanup_plan.get("workflow_gate") == "ready_for_cleanup":
        payload["next_actions"].append("rerun_close_sync_cleanup_after_merge_with_apply")
        if close_sync_cleanup_plan.get("next_command"):
            payload["next_commands"].append(close_sync_cleanup_plan["next_command"])
    if root_sync_deferrals:
        payload["next_actions"].append("sync_root_after_unrelated_dirty_files_are_resolved")
        payload["next_commands"].extend(payload["root_sync_deferred"].get("next_commands") or [])
    durable_state = (
        "blocked"
        if payload["workflow_gate"] == "blocked"
        else (
            "complete"
            if payload["workflow_gate"] == "complete"
            else (
                "fixed_source_pending_user_restart"
                if runtime_pending
                else ("close_synced" if close_sync_persisted else "merged")
            )
        )
    )
    for state_bug_id in canonical_bug_ids:
        _write_state(
            state_bug_id,
            state=durable_state,
            pr_url=source_pr_url,
            commit=merge_commit,
            close_sync=close_sync,
            close_sync_commit=close_sync_commit,
            close_sync_pr_merge=close_sync_pr_merge,
            cleanup_plan=cleanup_plan,
            close_sync_cleanup_plan=close_sync_cleanup_plan,
            postmortem=postmortem if state_bug_id == canonical_bug_id else None,
            next_actions=payload["next_actions"],
        )
    return payload


def build_run_plan(
    *,
    bug_id: str,
    mode: str,
    issue_json: str | None,
    changed_files: list[str],
    create_worktree: bool,
    dry_run: bool,
    validation_evidence: list[str],
    task_slug: str | None,
    allow_missing_linkage: bool,
    allow_closed: bool,
    base: str,
    head: str,
    push: bool = False,
    create_pr: bool = False,
    watch_ci: bool = False,
    pr_title: str | None = None,
    force_new_worktree: bool = False,
    force_reason: str | None = None,
    pr_url: str | None = None,
    merge: bool = False,
    sync_root: bool = False,
    branch: str | None = None,
    worktree: str | None = None,
    production_gates: dict[str, str] | None = None,
) -> dict[str, Any]:
    canonical_bug_id = bug_id.strip().upper()
    if mode in {"plan", "fix"}:
        if not issue_json:
            active_registry_record = _find_bug_record_from_active_registry(canonical_bug_id)
            if active_registry_record:
                _, active_registry_issue_path = active_registry_record
                issue_json = str(active_registry_issue_path)
        active_decision = _active_worktree_decision(
            bug_id=canonical_bug_id,
            create_worktree=create_worktree,
            force_new_worktree=force_new_worktree,
            force_reason=force_reason,
        )
        if active_decision["workflow_gate"] in {"resume", "blocked"}:
            event_root = Path(str((active_decision.get("active_workflows") or [{}])[0].get("root") or REPO_ROOT))
            _append_event(
                canonical_bug_id,
                event="active_worktree_decision",
                state="blocked" if active_decision["workflow_gate"] == "blocked" else "context_ready",
                result=str(active_decision["decision"]),
                evidence=active_decision,
                root=event_root,
            )
            return {
                "schema_version": "aistock_issue_workflow_run_v1",
                "generated_at": _utc_now(),
                "bug_id": canonical_bug_id,
                "mode": mode,
                "workflow_gate": active_decision["workflow_gate"],
                "active_decision": active_decision,
                "blocking": active_decision["blocking"],
                "warnings": active_decision["warnings"],
                "next_command": active_decision["next_command"],
            }
        start = build_start_plan(
            bug_id=canonical_bug_id,
            issue_json=issue_json,
            changed_files=changed_files,
            create_worktree=create_worktree,
            dry_run=dry_run,
            task_slug=task_slug,
            allow_missing_linkage=allow_missing_linkage,
            allow_closed=allow_closed,
            active_decision=active_decision,
        )
        gate = "ready_for_fix" if mode == "fix" else "planned"
        return {
            "schema_version": "aistock_issue_workflow_run_v1",
            "generated_at": _utc_now(),
            "bug_id": start["bug_id"],
            "mode": mode,
            "workflow_gate": gate,
            "start": start,
            "next_command": f"python scripts/aistock_issue_workflow.py resume --bug-id {start['bug_id']}",
        }
    if mode == "pr":
        finish = build_finish_plan(
            bug_id=canonical_bug_id,
            issue_json=issue_json,
            changed_files=changed_files or None,
            base=base,
            head=head,
            validation_evidence=validation_evidence,
            plan_only=not validation_evidence,
            allow_missing_evidence=False,
        )
        state_name = "validation_passed" if finish.get("validation_evidence") else "validation_planned"
        _write_state(
            canonical_bug_id,
            state=state_name,
            pr_body_path=finish.get("pr_body_path"),
            production_gates=finish.get("production_gates"),
            next_actions=["create_pr_from_pr_body", "watch_ci_before_merge"] if finish.get("validation_evidence") else ["run_required_validation"],
        )
        pr_automation = _maybe_create_pr(
            bug_id=canonical_bug_id,
            finish=finish,
            push=push,
            create_pr=create_pr,
            watch_ci=watch_ci,
            pr_title=pr_title,
        )
        return {
            "schema_version": "aistock_issue_workflow_run_v1",
            "generated_at": _utc_now(),
            "bug_id": canonical_bug_id,
            "mode": mode,
            "workflow_gate": "ready_for_pr" if finish.get("validation_evidence") else "validation_required",
            "finish": finish,
            "pr_automation": pr_automation,
            "next_command": f"gh pr create --body-file {finish.get('pr_body_path')} --fill"
            if finish.get("validation_evidence")
            else f"python scripts/aistock_issue_workflow.py finish --bug-id {canonical_bug_id} --validation-evidence \"<command> -> passed\"",
        }
    if mode == "merge":
        if not pr_url:
            raise WorkflowError("run --mode merge requires --pr-url")
        if not merge:
            return {
                "schema_version": "aistock_issue_workflow_run_v1",
                "generated_at": _utc_now(),
                "bug_id": canonical_bug_id,
                "mode": mode,
                "workflow_gate": "merge_requires_explicit_flag",
                "next_command": f"python scripts/aistock_issue_workflow.py run --bug-id {canonical_bug_id} --mode merge --pr-url {pr_url} --merge",
            }
        merge_evidence = flow._unique_strings(
            [str(item).strip() for item in validation_evidence if str(item).strip()]
        )
        if not merge_evidence:
            durable_state = _load_state(canonical_bug_id, REPO_ROOT) or {}
            durable_errors = flow._as_list(durable_state.get("validation_evidence_errors"))
            if not durable_errors:
                merge_evidence = flow._unique_strings(
                    [
                        str(item).strip()
                        for item in flow._as_list(durable_state.get("validation_evidence"))
                        if str(item).strip()
                    ]
                )
        if not merge_evidence:
            raise WorkflowError(
                "run --mode merge requires validated evidence before source merge; "
                "rerun finish or pass --validation-evidence"
            )
        merge_result = _merge_pr_if_ready_for_bug(canonical_bug_id, pr_url)
        finalizer = build_merge_finalizer_plan(
            bug_id=canonical_bug_id,
            source_pr_url=pr_url,
            source_branch=branch,
            source_worktree=worktree,
            validation_evidence=merge_evidence,
            issue_json=issue_json,
            allow_missing_linkage=allow_missing_linkage,
            production_gates=production_gates or _production_gates_payload(),
            sync_root=sync_root,
            merge_close_sync_pr=False,
            cleanup=False,
            apply=True,
            source_pr_check=merge_result.get("verified") if isinstance(merge_result, dict) else None,
        )
        finalizer_gate = str(finalizer.get("workflow_gate") or "blocked")
        finalizer_blocked = finalizer_gate == "blocked"
        close_sync_merge_gate = str(
            (finalizer.get("close_sync_pr_merge") or {}).get("workflow_gate") or ""
        )
        close_sync_is_merged = close_sync_merge_gate in {"merged", "already_merged"}
        if finalizer_blocked:
            wrapper_gate = "merged_aftercare_blocked"
            wrapper_state = "merged"
        elif finalizer_gate == "fixed_source_pending_user_restart":
            wrapper_gate = "merged_runtime_verification_pending"
            wrapper_state = "fixed_source_pending_user_restart"
        elif close_sync_is_merged:
            wrapper_gate = "merged_close_synced"
            wrapper_state = "close_synced"
        else:
            wrapper_gate = "merged_close_sync_pr_opened"
            wrapper_state = "merged"
        next_actions = flow._unique_strings(flow._as_list(finalizer.get("next_actions")))
        _write_state(
            canonical_bug_id,
            state=wrapper_state,
            pr_url=pr_url,
            commit=finalizer.get("source_merge_commit"),
            merge=merge_result,
            finalizer=finalizer,
            close_sync=finalizer.get("close_sync"),
            close_sync_commit=finalizer.get("close_sync_commit"),
            cleanup_plan=finalizer.get("cleanup"),
            next_actions=next_actions,
        )
        return {
            "schema_version": "aistock_issue_workflow_run_v1",
            "generated_at": _utc_now(),
            "bug_id": canonical_bug_id,
            "mode": mode,
            "workflow_gate": wrapper_gate,
            "blocking": flow._as_list(finalizer.get("blocking")) if finalizer_blocked else [],
            "merge": merge_result,
            "finalizer": finalizer,
            "close_sync": finalizer.get("close_sync"),
            "close_sync_commit": finalizer.get("close_sync_commit"),
            "cleanup": finalizer.get("cleanup"),
            "next_actions": next_actions,
        }
    raise WorkflowError(f"Unsupported run mode for Phase 1: {mode}")


def build_close_sync_plan(
    *,
    bug_id: str | None,
    issue_json: str | None,
    pr_url: str | None,
    apply: bool,
    allow_missing_linkage: bool,
    validation_evidence: list[str] | None = None,
    merge_commit: str | None = None,
    production_gates: dict[str, str] | None = None,
    skip_github_check: bool = False,
    create_registry_worktree: bool = False,
    allow_current_worktree: bool = False,
    post_restart_receipt: str | None = None,
) -> dict[str, Any]:
    record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
    canonical_bug_id = str(record.get("bug_id") or bug_id or source_path.stem).upper()
    missing_linkage = _require_github_linkage(record, allow_missing=allow_missing_linkage)
    status = str(record.get("status") or "").strip()
    if status not in flow.VALID_BUG_STATUSES:
        raise WorkflowError(f"{canonical_bug_id} has invalid status for close/sync: {status!r}")
    evidence = [item for item in validation_evidence or [] if item.strip()]
    gates = production_gates or _production_gates_payload()
    runtime_changed_files = (
        _merged_commit_changed_files(merge_commit)
        if merge_commit
        else resolve_record_runtime_changed_files(record)
    )
    runtime_contract = build_runtime_contract(
        record=record,
        changed_files=runtime_changed_files,
        fresh_process_evidence=flow._as_list((record.get("runtime_contract") or {}).get("fresh_process_evidence"))
        if isinstance(record.get("runtime_contract"), dict)
        else [],
    )
    runtime_contract_errors = list(runtime_contract.get("blocking") or [])
    if apply and runtime_contract_errors:
        raise WorkflowError("runtime contract blocks close-sync: " + "; ".join(runtime_contract_errors))
    runtime_receipt: dict[str, Any] | None = None
    runtime_receipt_path: Path | None = None
    runtime_receipt_errors: list[str] = []
    if post_restart_receipt:
        receipt_path = Path(post_restart_receipt)
        if not receipt_path.is_absolute():
            receipt_path = REPO_ROOT / receipt_path
        runtime_receipt_path = receipt_path
        if not receipt_path.exists():
            runtime_receipt_errors.append(f"post-restart receipt not found: {receipt_path}")
        else:
            runtime_receipt = _load_json(receipt_path)
            if runtime_receipt.get("schema_version") != RUNTIME_VERIFY_RECEIPT_SCHEMA:
                runtime_receipt_errors.append("post-restart receipt schema mismatch")
            if str(runtime_receipt.get("bug_id") or "").upper() != canonical_bug_id:
                runtime_receipt_errors.append("post-restart receipt BUG id mismatch")
            if runtime_receipt.get("target_id") != runtime_contract.get("target_id"):
                runtime_receipt_errors.append("post-restart receipt target mismatch")
            if runtime_receipt.get("post_restart_effective_gate") != "passed":
                runtime_receipt_errors.append("post-restart receipt gate is not passed")
            if runtime_receipt.get("runtime_identity_match") is not True:
                runtime_receipt_errors.append("post-restart receipt runtime identity did not match")
            if runtime_receipt.get("mode") != "read_only":
                runtime_receipt_errors.append("post-restart receipt mode is not read_only")
            if runtime_receipt.get("tracked_files_written") is not False:
                runtime_receipt_errors.append("post-restart receipt must not include tracked writes")
            if not str(runtime_receipt.get("expected_identity") or "").strip():
                runtime_receipt_errors.append("post-restart receipt expected identity is missing")
            if merge_commit and str(runtime_receipt.get("expected_identity") or "").strip() != str(merge_commit).strip():
                runtime_receipt_errors.append("post-restart receipt expected identity does not match merge commit")
            receipt_expected_identity = str(runtime_receipt.get("expected_identity") or "").strip()
            if receipt_expected_identity:
                runtime_receipt_errors.extend(
                    _runtime_identity_proof_errors(
                        runtime_receipt,
                        expected_identity=receipt_expected_identity,
                        root=REPO_ROOT,
                    )
                )
            proof_observed_identity = (
                str((runtime_receipt.get("runtime_identity_proof") or {}).get("observed_identity") or "").strip()
                if isinstance(runtime_receipt.get("runtime_identity_proof"), dict)
                else ""
            )
            if str(runtime_receipt.get("observed_identity") or "").strip() != proof_observed_identity:
                runtime_receipt_errors.append("post-restart receipt observed identity does not match identity proof")
            if runtime_receipt.get("process_control_performed") is not False:
                runtime_receipt_errors.append("post-restart receipt must not include process control")
            if runtime_receipt.get("blocking"):
                runtime_receipt_errors.append("post-restart receipt contains blocking findings")
            if runtime_receipt.get("contract_digest") != _runtime_contract_digest(runtime_contract):
                runtime_receipt_errors.append("post-restart receipt runtime contract digest mismatch")
            if runtime_receipt.get("catalog_sha256") != _runtime_catalog_sha256(root=REPO_ROOT):
                runtime_receipt_errors.append("post-restart receipt runtime catalog digest mismatch")
            receipt_probes = runtime_receipt.get("probes") if isinstance(runtime_receipt.get("probes"), list) else []
            required_probe_names = ["health_ref", "identity_ref", "business_smoke_ref"]
            contract_database_ref = (((runtime_contract.get("target") or {}).get("probes") or {}).get("database_readback_ref"))
            if contract_database_ref and str(contract_database_ref).lower() != "not_required":
                required_probe_names.append("database_readback_ref")
            observed_probe_names = [str(item.get("name")) for item in receipt_probes if isinstance(item, dict)]
            if sorted(observed_probe_names) != sorted(required_probe_names):
                runtime_receipt_errors.append(
                    "post-restart receipt probe set mismatch: "
                    f"expected={required_probe_names} observed={observed_probe_names}"
                )
            for probe in receipt_probes:
                if not isinstance(probe, dict):
                    runtime_receipt_errors.append("post-restart receipt contains an invalid probe entry")
                    continue
                if probe.get("status") != "passed" or not str(probe.get("response_sha256") or "").strip():
                    runtime_receipt_errors.append(f"post-restart receipt probe is incomplete or failed: {probe.get('name')}")
                if "response_preview" in probe or "_response_body" in probe:
                    runtime_receipt_errors.append(f"post-restart receipt probe leaks response content: {probe.get('name')}")
            smoke_probe = next(
                (
                    probe
                    for probe in receipt_probes
                    if isinstance(probe, dict) and probe.get("name") == "business_smoke_ref"
                ),
                None,
            )
            if smoke_probe is not None:
                semantic = smoke_probe.get("semantic")
                if not isinstance(semantic, dict) or semantic.get("schema_version") != BUSINESS_SMOKE_SEMANTIC_SCHEMA:
                    runtime_receipt_errors.append(
                        "post-restart receipt business-smoke semantic verdict is missing or has an unknown schema"
                    )
                else:
                    if semantic.get("verdict") != "passed":
                        runtime_receipt_errors.append(
                            "post-restart receipt business-smoke semantic verdict is not passed: "
                            f"{semantic.get('reason')}"
                        )
                    if str(semantic.get("response_sha256") or "") != str(smoke_probe.get("response_sha256") or ""):
                        runtime_receipt_errors.append(
                            "post-restart receipt business-smoke semantic digest does not match probe evidence"
                        )
            current_expectation = runtime_contract.get("expected_terminal_outcome")
            receipt_expectation = runtime_receipt.get("expected_terminal_outcome")
            if current_expectation is None:
                if receipt_expectation is not None:
                    runtime_receipt_errors.append(
                        "post-restart receipt carries an expected terminal outcome but the current "
                        "runtime contract declares none"
                    )
                if runtime_receipt.get("expected_terminal_outcome_digest"):
                    runtime_receipt_errors.append(
                        "post-restart receipt carries an expected terminal outcome digest but the "
                        "current runtime contract declares none"
                    )
            else:
                if receipt_expectation != current_expectation:
                    runtime_receipt_errors.append(
                        "post-restart receipt expected terminal outcome does not match the current "
                        "runtime contract declaration"
                    )
                if str(runtime_receipt.get("expected_terminal_outcome_digest") or "") != str(
                    _expectation_outcome_digest(current_expectation) or ""
                ):
                    runtime_receipt_errors.append(
                        "post-restart receipt expected terminal outcome digest mismatch"
                    )
            smoke_semantic = smoke_probe.get("semantic") if isinstance(smoke_probe, dict) else None
            top_level_semantic = runtime_receipt.get("business_smoke_semantic")
            if top_level_semantic != smoke_semantic and (
                top_level_semantic is not None
                or runtime_receipt.get("post_restart_effective_gate") == "passed"
            ):
                runtime_receipt_errors.append(
                    "post-restart receipt top-level business-smoke semantic does not match probe evidence"
                )
            if isinstance(smoke_semantic, dict):
                semantic_expectation = smoke_semantic.get("expectation")
                if current_expectation is None:
                    if semantic_expectation is not None:
                        runtime_receipt_errors.append(
                            "post-restart receipt semantic verdict carries an expectation the current "
                            "runtime contract does not declare"
                        )
                    if smoke_semantic.get("expectation_digest"):
                        runtime_receipt_errors.append(
                            "post-restart receipt semantic verdict carries an expectation digest the "
                            "current runtime contract does not declare"
                        )
                else:
                    if semantic_expectation != current_expectation:
                        runtime_receipt_errors.append(
                            "post-restart receipt semantic verdict expectation does not match the current "
                            "runtime contract declaration"
                        )
                    if str(smoke_semantic.get("expectation_digest") or "") != str(
                        _expectation_outcome_digest(current_expectation) or ""
                    ):
                        runtime_receipt_errors.append(
                            "post-restart receipt semantic verdict expectation digest does not match the "
                            "current runtime contract declaration"
                        )
            if runtime_receipt.get("probe_evidence_digest") != _probe_evidence_digest(receipt_probes):
                runtime_receipt_errors.append("post-restart receipt probe evidence digest mismatch")
    runtime_gate_passed = bool(
        not runtime_contract_errors
        and (
            not runtime_contract.get("backend_restart_required")
            or (runtime_receipt and not runtime_receipt_errors)
        )
    )
    registry_worktree_plan = _maybe_create_close_sync_worktree(
        bug_id=canonical_bug_id,
        create=create_registry_worktree,
        dry_run=not apply,
    )
    close_sync_root = Path(registry_worktree_plan["worktree"]) if create_registry_worktree else REPO_ROOT
    if create_registry_worktree and apply:
        rel_source = source_path.resolve().relative_to(REPO_ROOT.resolve())
        target_source = close_sync_root / rel_source
        if not target_source.exists():
            raise WorkflowError(f"BUG JSON does not exist in close-sync worktree: {target_source}")
        record = _load_json(target_source)
        source_path = target_source
    apply_guard = _validate_close_sync_apply_target(close_sync_root) if apply else None
    if apply_guard and apply_guard["blocking"] and not allow_current_worktree:
        raise WorkflowError("; ".join(apply_guard["blocking"]))
    output_dir = close_sync_root / WORKFLOW_ROOT / canonical_bug_id
    workflow_gate = "ready_for_apply" if pr_url and evidence else ("missing_validation_evidence" if pr_url else "missing_pr_url")
    if runtime_contract_errors:
        workflow_gate = "blocked_runtime_contract"
    elif pr_url and evidence and runtime_contract.get("backend_restart_required") and not runtime_gate_passed:
        workflow_gate = "fixed_source_pending_user_restart"
    payload = {
        "schema_version": "aistock_issue_workflow_close_sync_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "source_bug_json": _repo_rel(source_path, close_sync_root),
        "registry_root": str(close_sync_root),
        "registry_worktree_plan": registry_worktree_plan,
        "apply_guard": apply_guard,
        "current_status": status,
        "github_issue_number": record.get("github_issue_number"),
        "github_issue_url": record.get("github_issue_url"),
        "missing_github_linkage": missing_linkage,
        "merged_pr": pr_url,
        "merge_commit": merge_commit,
        "runtime_changed_files": runtime_changed_files,
        "runtime_changed_files_source": "merge_commit" if merge_commit else (
            "file_scope_contract.changed_files"
            if _record_has_file_scope_changed_files(record)
            else "allowed_write_scope"
        ),
        "validation_evidence": evidence,
        "production_gates": gates,
        "backend_restart": {
            "required": runtime_contract.get("backend_restart_required"),
            "owner": "user",
            "target_id": runtime_contract.get("target_id"),
            "operator_runbook_ref": runtime_contract.get("operator_runbook_ref"),
        },
        "post_restart_effective_gate": "passed" if runtime_gate_passed else "pending_user_restart",
        "runtime_identity_match": (
            True
            if runtime_gate_passed and runtime_contract.get("backend_restart_required")
            else ("pending" if runtime_contract.get("backend_restart_required") else "not_required")
        ),
        "post_restart_receipt": post_restart_receipt,
        "post_restart_receipt_errors": runtime_receipt_errors,
        "runtime_contract_errors": runtime_contract_errors,
        "runtime_contract": runtime_contract,
        "dry_run": not apply,
        "workflow_gate": workflow_gate,
        "required_checks": [
            "closure_requirements_completed",
            "validation_evidence_attached",
            "BUG_JSON_and_GitHub_issue_status_aligned",
            "production_gates_reported",
            "post_restart_runtime_identity_and_smoke_verified_when_backend_restart_required",
        ],
        "next_agent_steps": [
            "verify_closure_requirements_item_by_item",
            "run_close_sync_apply_from_clean_registry_worktree",
            "sync_github_issue_status_with_gh_or_mcp",
            "record_final_production_gates",
        ],
    }
    _write_json(output_dir / "close-sync-plan.json", payload)
    if apply:
        if not pr_url:
            raise WorkflowError("close-sync --apply requires --pr-url")
        if not evidence:
            raise WorkflowError("close-sync --apply requires at least one --validation-evidence")
        started = time.monotonic()
        pr_check = _verify_pr_merged(pr_url, skip_github_check=skip_github_check)
        verified_merge_commit = _merge_commit_from_pr_check(pr_check)
        if merge_commit and verified_merge_commit and merge_commit != verified_merge_commit:
            raise WorkflowError("close-sync merge commit differs from the verified source PR")
        merge_commit = merge_commit or verified_merge_commit
        closed_at = _closed_at_from_pr_check(pr_check)
        if (
            runtime_contract.get("backend_restart_required")
            and runtime_receipt
            and str(runtime_receipt.get("expected_identity") or "").strip() != str(merge_commit or "").strip()
        ):
            runtime_receipt_errors.append("post-restart receipt expected identity does not match verified PR merge commit")
            runtime_gate_passed = False
            payload["post_restart_effective_gate"] = "pending_user_restart"
            payload["runtime_identity_match"] = False
            payload["post_restart_receipt_errors"] = flow._unique_strings(runtime_receipt_errors)
        updated = dict(record)
        runtime_pending = bool(runtime_contract.get("backend_restart_required") and not runtime_gate_passed)
        source_fixed_at = record.get("fixed_at") or closed_at or _utc_now()
        issue_closed_at = (
            None
            if runtime_pending
            else (_utc_now() if runtime_contract.get("backend_restart_required") else closed_at)
        )
        already_fixed_for_source = (
            str(record.get("status") or "").strip().lower() in {"fixed", "verified"}
            and str(record.get("pr_url") or "").strip() == str(pr_url or "").strip()
            and (
                not merge_commit
                or str(record.get("fix_commit") or "").strip() == str(merge_commit).strip()
            )
        )
        durable_validation_evidence = (
            flow._as_list(record.get("validation_evidence"))
            if already_fixed_for_source
            else flow._unique_strings([*flow._as_list(record.get("validation_evidence")), *evidence])
        )
        updated.update(
            {
                "status": "fixed_source_pending_user_restart" if runtime_pending else (
                    "verified" if runtime_contract.get("backend_restart_required") else "fixed"
                ),
                "closed_at": issue_closed_at,
                "fixed_at": source_fixed_at,
                "fix_commit": merge_commit,
                "pr_url": pr_url,
                "validation_evidence": durable_validation_evidence,
                **gates,
            }
        )
        updated_runtime = dict(updated.get("runtime_contract") or {})
        updated_runtime.update(
            {
                "backend_restart_owner": "user",
                "post_restart_effective_gate": "pending_user_restart" if runtime_pending else (
                    "passed" if runtime_contract.get("backend_restart_required") else "not_required"
                ),
                "runtime_identity_match": "pending" if runtime_pending else (
                    True if runtime_contract.get("backend_restart_required") else "not_required"
                ),
                "post_restart_receipt_ref": post_restart_receipt,
            }
        )
        if (
            runtime_contract.get("backend_restart_required")
            and not runtime_pending
            and runtime_receipt
            and runtime_receipt_path
        ):
            updated_runtime["post_restart_receipt_summary"] = _post_restart_receipt_summary(
                runtime_receipt_path,
                runtime_receipt,
            )
        updated["runtime_contract"] = updated_runtime
        _write_json(source_path, updated)
        evidence_payload = {
            **payload,
            "workflow_gate": "fixed_source_pending_user_restart" if runtime_pending else "close_synced",
            "dry_run": False,
            "pr_check": pr_check,
            "merge_commit": merge_commit,
            "updated_bug_json": _repo_rel(source_path, close_sync_root),
        }
        github_sync = (
            _sync_github_issue_runtime_pending(updated, evidence_payload, root=close_sync_root)
            if runtime_pending
            else _sync_github_issue_after_close(updated, evidence_payload, root=close_sync_root)
        )
        evidence_payload["github_issue_sync"] = github_sync
        _write_json(output_dir / "close-sync-evidence.json", evidence_payload)
        if not runtime_pending and github_sync.get("status") != "synced":
            raise WorkflowError(
                "close-sync GitHub Issue state/label synchronization is incomplete; "
                f"status={github_sync.get('status') or 'unknown'}"
            )
        timing = _workflow_timing_summary(canonical_bug_id, root=close_sync_root)
        _write_state(
            canonical_bug_id,
            state="fixed_source_pending_user_restart" if runtime_pending else "close_synced",
            root=close_sync_root,
            pr_url=pr_url,
            commit=merge_commit,
            validation_evidence=evidence,
            production_gates=gates,
            github_issue_sync=github_sync,
            timing_summary=timing,
            post_restart_effective_gate=updated_runtime["post_restart_effective_gate"],
            runtime_identity_match=updated_runtime["runtime_identity_match"],
            next_actions=(
                ["user_restart_catalog_target", "run_post_restart_verify", "rerun_close_sync_with_receipt", "source_cleanup_independent"]
                if runtime_pending
                else ["sync_local_main", "cleanup_after_merge"]
            ),
        )
        _append_event(
            canonical_bug_id,
            event="close_sync_apply" if not runtime_pending else "source_fixed_runtime_pending",
            state="close_synced" if not runtime_pending else "fixed_source_pending_user_restart",
            root=close_sync_root,
            duration_seconds=time.monotonic() - started,
            evidence={
                "pr_url": pr_url,
                "merge_commit": merge_commit,
                "github_issue_sync_status": github_sync.get("status"),
            },
        )
        evidence_payload["timing_summary"] = _workflow_timing_summary(canonical_bug_id, root=close_sync_root)
        return evidence_payload
    return payload


def build_close_sync_batch_plan(
    *,
    bug_ids: list[str],
    pr_url: str | None,
    apply: bool,
    allow_missing_linkage: bool,
    validation_evidence: list[str] | None = None,
    merge_commit: str | None = None,
    production_gates: dict[str, str] | None = None,
    skip_github_check: bool = False,
    create_registry_worktree: bool = False,
    allow_current_worktree: bool = False,
) -> dict[str, Any]:
    canonical_bug_ids = flow._unique_strings([item.strip().upper() for item in bug_ids if item.strip()])
    if not canonical_bug_ids:
        raise WorkflowError("close-sync-batch requires at least one --bug-id")
    evidence = [item for item in validation_evidence or [] if item.strip()]
    gates = production_gates or _production_gates_payload()
    records: list[dict[str, Any]] = []
    source_paths: list[Path] = []
    for item in canonical_bug_ids:
        record, source_path = find_bug_record(bug_id=item, issue_json=None)
        missing = _require_github_linkage(record, allow_missing=allow_missing_linkage)
        status = str(record.get("status") or "").strip()
        if status not in flow.VALID_BUG_STATUSES:
            raise WorkflowError(f"{item} has invalid status for close/sync: {status!r}")
        records.append({"bug_id": item, "record": record, "source_path": source_path, "missing_github_linkage": missing})
        source_paths.append(source_path)
    runtime_contracts = {
        row["bug_id"]: build_runtime_contract(
            record=row["record"],
            changed_files=resolve_record_runtime_changed_files(row["record"]),
            root=REPO_ROOT,
            fresh_process_evidence=flow._as_list((row["record"].get("runtime_contract") or {}).get("fresh_process_evidence"))
            if isinstance(row["record"].get("runtime_contract"), dict)
            else [],
        )
        for row in records
    }
    runtime_contract_errors = {
        bug_id: list(contract.get("blocking") or [])
        for bug_id, contract in runtime_contracts.items()
        if contract.get("blocking")
    }
    if runtime_contract_errors:
        detail = "; ".join(
            f"{bug_id}: {', '.join(errors)}"
            for bug_id, errors in sorted(runtime_contract_errors.items())
        )
        raise WorkflowError("runtime contracts block close-sync-batch: " + detail)
    runtime_batch_bug_ids = [
        bug_id for bug_id, contract in runtime_contracts.items() if contract.get("backend_restart_required")
    ]
    if runtime_batch_bug_ids:
        raise WorkflowError(
            "close-sync-batch cannot close runtime BUGs; use per-issue post-restart receipts: "
            + ", ".join(runtime_batch_bug_ids)
        )
    compatibility = _close_sync_batch_compatibility(
        [row["record"] for row in records],
        runtime_contracts,
        requested_production_gates=gates,
        pr_url=pr_url,
    )
    if compatibility["blocking"]:
        raise WorkflowError("close-sync-batch compatibility blocked: " + "; ".join(compatibility["blocking"]))
    registry_worktree_plan = _maybe_create_close_sync_batch_worktree(
        bug_ids=canonical_bug_ids,
        create=create_registry_worktree,
        dry_run=not apply,
    )
    close_sync_root = Path(registry_worktree_plan["worktree"]) if create_registry_worktree else REPO_ROOT
    target_pairs: list[tuple[str, dict[str, Any], Path, list[str]]] = []
    if create_registry_worktree and apply:
        for item, record, source_path in [(row["bug_id"], row["record"], row["source_path"]) for row in records]:
            rel_source = source_path.resolve().relative_to(REPO_ROOT.resolve())
            target_source = close_sync_root / rel_source
            if not target_source.exists():
                raise WorkflowError(f"BUG JSON does not exist in close-sync batch worktree: {target_source}")
            target_record = _load_json(target_source)
            target_pairs.append((item, target_record, target_source, []))
    else:
        target_pairs = [
            (row["bug_id"], row["record"], row["source_path"], row["missing_github_linkage"])
            for row in records
        ]
    apply_guard = _validate_close_sync_apply_target(close_sync_root) if apply else None
    if apply_guard and apply_guard["blocking"] and not allow_current_worktree:
        raise WorkflowError("; ".join(apply_guard["blocking"]))
    batch_id = "-".join(canonical_bug_ids)
    output_dir = close_sync_root / WORKFLOW_ROOT / batch_id
    workflow_gate = "ready_for_apply" if pr_url and evidence else ("missing_validation_evidence" if pr_url else "missing_pr_url")
    payload: dict[str, Any] = {
        "schema_version": "aistock_issue_workflow_close_sync_batch_v1",
        "generated_at": _utc_now(),
        "batch_id": batch_id,
        "bug_ids": canonical_bug_ids,
        "source_bug_jsons": [_repo_rel(path, close_sync_root) for path in source_paths],
        "registry_root": str(close_sync_root),
        "registry_worktree_plan": registry_worktree_plan,
        "apply_guard": apply_guard,
        "merged_pr": pr_url,
        "merge_commit": merge_commit,
        "validation_evidence": evidence,
        "production_gates": gates,
        "compatibility": compatibility,
        "dry_run": not apply,
        "workflow_gate": workflow_gate,
        "per_issue": [
            {
                "bug_id": item,
                "github_issue_number": record.get("github_issue_number"),
                "github_issue_url": record.get("github_issue_url"),
                "source_bug_json": _repo_rel(path, close_sync_root),
                "missing_github_linkage": missing,
                "source_pr_url": str(record.get("pr_url") or "") or None,
                "source_fix_commit": str(record.get("fix_commit") or "") or None,
                "runtime_contract": runtime_contracts.get(item),
                "compatibility_key": compatibility["compatibility_key"],
            }
            for item, record, path, missing in target_pairs
        ],
        "next_agent_steps": [
            "verify_shared_source_pr_covers_each_bug",
            "run_close_sync_batch_apply_from_clean_registry_worktree",
            "sync_each_github_issue_status",
            "persist_one_close_sync_pr_for_the_batch",
        ],
    }
    _write_json(output_dir / "close-sync-batch-plan.json", payload)
    if not apply:
        return payload
    if not pr_url:
        raise WorkflowError("close-sync-batch --apply requires --pr-url")
    if not evidence:
        raise WorkflowError("close-sync-batch --apply requires at least one --validation-evidence")
    started = time.monotonic()
    pr_check = _verify_pr_merged(pr_url, skip_github_check=skip_github_check)
    merge_commit = merge_commit or _merge_commit_from_pr_check(pr_check)
    closed_at = _closed_at_from_pr_check(pr_check)
    updated_paths: list[str] = []
    github_syncs: dict[str, Any] = {}
    for item, record, source_path, _missing in target_pairs:
        updated = dict(record)
        already_fixed_for_source = (
            str(record.get("status") or "").strip().lower() in {"fixed", "verified"}
            and str(record.get("pr_url") or "").strip() == str(pr_url or "").strip()
            and (
                not merge_commit
                or str(record.get("fix_commit") or "").strip() == str(merge_commit).strip()
            )
        )
        durable_validation_evidence = (
            flow._as_list(record.get("validation_evidence"))
            if already_fixed_for_source
            else flow._unique_strings([*flow._as_list(record.get("validation_evidence")), *evidence])
        )
        updated.update(
            {
                "status": "fixed",
                "closed_at": closed_at,
                "fixed_at": record.get("fixed_at") or closed_at or _utc_now(),
                "fix_commit": merge_commit,
                "pr_url": pr_url,
                "validation_evidence": durable_validation_evidence,
                **gates,
            }
        )
        _write_json(source_path, updated)
        updated_paths.append(_repo_rel(source_path, close_sync_root))
        issue_evidence = {
            **payload,
            "workflow_gate": "close_synced",
            "bug_id": item,
            "merge_commit": merge_commit,
            "updated_bug_json": _repo_rel(source_path, close_sync_root),
            "per_issue_compatibility_key": compatibility["compatibility_key"],
        }
        github_syncs[item] = _sync_github_issue_after_close(updated, issue_evidence, root=close_sync_root)
        _write_state(
            item,
            state="close_synced",
            root=close_sync_root,
            pr_url=pr_url,
            commit=merge_commit,
            validation_evidence=evidence,
            production_gates=gates,
            github_issue_sync=github_syncs[item],
            next_actions=["persist_batch_close_sync_pr", "sync_local_main", "cleanup_after_merge"],
        )
        _append_event(
            item,
            event="close_sync_batch_apply",
            state="close_synced",
            root=close_sync_root,
            duration_seconds=0.0,
            evidence={"batch_id": batch_id, "pr_url": pr_url, "merge_commit": merge_commit},
        )
    evidence_payload = {
        **payload,
        "workflow_gate": "close_synced",
        "dry_run": False,
        "pr_check": pr_check,
        "merge_commit": merge_commit,
        "updated_bug_jsons": updated_paths,
        "github_issue_sync": github_syncs,
        "duration_seconds": round(time.monotonic() - started, 3),
    }
    _write_json(output_dir / "close-sync-batch-evidence.json", evidence_payload)
    return evidence_payload


def build_close_sync_aggregate_plan(
    *,
    bug_ids: list[str],
    apply: bool,
    source_pr_urls: dict[str, str] | None = None,
    skip_github_check: bool = False,
    create_registry_worktree: bool = False,
    allow_current_worktree: bool = False,
) -> dict[str, Any]:
    """Close independently merged, non-runtime BUGs in one metadata-only PR.

    Unlike close-sync-batch, this mode never projects one source PR identity or
    one validation receipt onto every BUG. Source identities may come from the
    record or explicit per-BUG mappings. Missing BUG JSON evidence may be
    satisfied only by a commit-bound durable receipt in that BUG's merged
    source PR. Every source PR is verified before any BUG JSON is changed.
    """
    canonical_bug_ids = flow._unique_strings([item.strip().upper() for item in bug_ids if item.strip()])
    if len(canonical_bug_ids) < 2:
        raise WorkflowError("close-sync-aggregate requires at least two unique --bug-id values")
    requested_source_prs = {
        str(key).strip().upper(): str(value).strip().rstrip("/")
        for key, value in (source_pr_urls or {}).items()
    }
    unknown_mappings = sorted(set(requested_source_prs) - set(canonical_bug_ids))
    if unknown_mappings:
        raise WorkflowError(
            "close-sync-aggregate received --source-pr mappings for unselected BUGs: "
            + _delimited_text(unknown_mappings, ", ")
        )

    records: list[dict[str, Any]] = []
    for item in canonical_bug_ids:
        record, source_path = find_bug_record(bug_id=item, issue_json=None)
        missing = _require_github_linkage(record, allow_missing=False)
        status = str(record.get("status") or "").strip()
        if status not in flow.VALID_BUG_STATUSES:
            raise WorkflowError(f"{item} has invalid status for close/sync: {status!r}")
        recorded_pr_url = str(record.get("pr_url") or "").strip().rstrip("/")
        mapped_pr_url = requested_source_prs.get(item, "")
        if recorded_pr_url and mapped_pr_url and recorded_pr_url != mapped_pr_url:
            raise WorkflowError(
                f"close-sync-aggregate source PR mismatch for {item}: "
                f"recorded={recorded_pr_url} requested={mapped_pr_url}"
            )
        source_pr_url = mapped_pr_url or recorded_pr_url
        if not source_pr_url:
            raise WorkflowError(
                f"close-sync-aggregate requires a source PR for {item}; "
                f"pass --source-pr {item}=https://github.com/{GITHUB_REPO}/pull/NNN"
            )
        durable_evidence = flow._unique_strings(flow._as_list(record.get("validation_evidence")))
        records.append(
            {
                "bug_id": item,
                "record": record,
                "source_path": source_path,
                "missing_github_linkage": missing,
                "source_pr_url": source_pr_url,
                "validation_evidence": durable_evidence,
                "validation_receipt_profile": None,
                "source_record_fingerprint": _short_hash(record, length=16),
            }
        )

    selected_source_pr_urls = [row["source_pr_url"] for row in records]
    if len(set(selected_source_pr_urls)) != len(selected_source_pr_urls):
        raise WorkflowError(
            "close-sync-aggregate requires independent source PRs; shared source PR records belong in close-sync-batch"
        )

    runtime_contracts = {
        row["bug_id"]: build_runtime_contract(
            record=row["record"],
            changed_files=resolve_record_runtime_changed_files(row["record"]),
            root=REPO_ROOT,
            fresh_process_evidence=flow._as_list((row["record"].get("runtime_contract") or {}).get("fresh_process_evidence"))
            if isinstance(row["record"].get("runtime_contract"), dict)
            else [],
        )
        for row in records
    }
    runtime_contract_errors = {
        bug_id: list(contract.get("blocking") or [])
        for bug_id, contract in runtime_contracts.items()
        if contract.get("blocking")
    }
    if runtime_contract_errors:
        detail = _delimited_text(
            (
                f"{bug_id}: {_delimited_text(errors, ', ')}"
                for bug_id, errors in sorted(runtime_contract_errors.items())
            ),
            "; ",
        )
        raise WorkflowError("runtime contracts block close-sync-aggregate: " + detail)
    runtime_pending = sorted(
        bug_id for bug_id, contract in runtime_contracts.items() if contract.get("backend_restart_required")
    )
    if runtime_pending:
        raise WorkflowError(
            "close-sync-aggregate excludes runtime/restart-pending BUGs; complete them individually: "
            + _delimited_text(runtime_pending, ", ")
        )

    for row in records:
        if row["validation_evidence"]:
            continue
        receipt_profile = _merged_pr_validation_receipt_profile(row["source_pr_url"])
        if not receipt_profile.get("durable_receipt_present"):
            raise WorkflowError(
                f"close-sync-aggregate requires durable validation evidence for {row['bug_id']}; "
                f"source PR receipt status={receipt_profile.get('status') or 'unknown'} "
                f"error={receipt_profile.get('error') or 'none'}"
            )
        row["validation_receipt_profile"] = receipt_profile
        row["validation_evidence"] = [
            "source PR durable validation receipt: "
            f"pr={row['source_pr_url']} head={receipt_profile.get('head_oid')} "
            f"receipt_commit={receipt_profile.get('receipt_commit')} status=passed"
        ]

    apply_guard = (
        _validate_close_sync_apply_target(REPO_ROOT)
        if apply and not create_registry_worktree
        else None
    )
    if apply_guard and apply_guard["blocking"] and not allow_current_worktree:
        raise WorkflowError(_delimited_text(apply_guard["blocking"], "; "))

    verified_sources: dict[str, dict[str, Any]] = {}
    if apply:
        for row in records:
            receipt_profile = row.get("validation_receipt_profile") or {}
            if receipt_profile.get("durable_receipt_present") and receipt_profile.get("merge_commit"):
                pr_check = {
                    "checked": True,
                    "merged": True,
                    "source": "commit_bound_validation_receipt",
                    "pr": {
                        "mergeCommit": {"oid": receipt_profile["merge_commit"]},
                        "mergedAt": receipt_profile.get("merged_at"),
                        "url": row["source_pr_url"],
                    },
                }
            else:
                pr_check = _verify_pr_merged(row["source_pr_url"], skip_github_check=skip_github_check)
            source_merge_commit = _merge_commit_from_pr_check(pr_check)
            if not source_merge_commit:
                raise WorkflowError(f"cannot resolve merged commit for {row['bug_id']} source PR")
            verified_sources[row["bug_id"]] = {
                "pr_check": pr_check,
                "source_merge_commit": source_merge_commit,
                "closed_at": _closed_at_from_pr_check(pr_check),
            }

    registry_worktree_plan = _maybe_create_close_sync_aggregate_worktree(
        bug_ids=canonical_bug_ids,
        create=create_registry_worktree,
        dry_run=not apply,
    )
    close_sync_root = (
        Path(registry_worktree_plan["worktree"])
        if create_registry_worktree and apply
        else REPO_ROOT
    )
    target_rows: list[dict[str, Any]] = []
    if create_registry_worktree and apply:
        for row in records:
            rel_source = row["source_path"].resolve().relative_to(REPO_ROOT.resolve())
            target_source = close_sync_root / rel_source
            if not target_source.exists():
                raise WorkflowError(f"BUG JSON does not exist in close-sync aggregate worktree: {target_source}")
            target_record = _load_json(target_source)
            if (
                str(target_record.get("bug_id") or "").strip().upper() != row["bug_id"]
                or _short_hash(target_record, length=16) != row["source_record_fingerprint"]
            ):
                raise WorkflowError(
                    f"{row['bug_id']} changed while the aggregate worktree was created; rebuild the plan from current main"
                )
            target_rows.append(
                {
                    **row,
                    "record": target_record,
                    "source_path": target_source,
                }
            )
    else:
        target_rows = records

    selected_source_pr_urls = [row["source_pr_url"] for row in target_rows]

    if apply and create_registry_worktree:
        apply_guard = _validate_close_sync_apply_target(close_sync_root)
    if apply_guard and apply_guard["blocking"] and not allow_current_worktree:
        raise WorkflowError(_delimited_text(apply_guard["blocking"], "; "))

    aggregate_id = _delimited_text(canonical_bug_ids, "-")
    aggregate_key = f"close-sync-aggregate:{_short_hash([(row['bug_id'], row['source_pr_url']) for row in target_rows], length=12)}"
    output_dir = close_sync_root / WORKFLOW_ROOT / aggregate_id
    per_issue = [
        {
            "bug_id": row["bug_id"],
            "github_issue_number": row["record"].get("github_issue_number"),
            "github_issue_url": row["record"].get("github_issue_url"),
            "source_bug_json": _repo_rel(row["source_path"], close_sync_root),
            "missing_github_linkage": row["missing_github_linkage"],
            "source_pr_url": row["source_pr_url"],
            "source_merge_commit": str(row["record"].get("fix_commit") or "") or None,
            "validation_evidence": row["validation_evidence"],
            "validation_receipt_profile": row.get("validation_receipt_profile"),
            "runtime_contract": runtime_contracts[row["bug_id"]],
        }
        for row in target_rows
    ]
    payload: dict[str, Any] = {
        "schema_version": "aistock_issue_workflow_close_sync_aggregate_v1",
        "generated_at": _utc_now(),
        "aggregate_id": aggregate_id,
        "aggregate_key": aggregate_key,
        "bug_ids": canonical_bug_ids,
        "source_bug_jsons": [row["source_bug_json"] for row in per_issue],
        "source_prs": selected_source_pr_urls,
        "registry_root": str(close_sync_root),
        "registry_worktree_plan": registry_worktree_plan,
        "apply_guard": apply_guard,
        "merged_pr": None,
        "merge_commit": None,
        "validation_evidence": [],
        "production_gates": {},
        "per_issue": per_issue,
        "dry_run": not apply,
        "workflow_gate": "ready_for_apply",
        "next_agent_steps": [
            "verify_each_source_pr_and_receipt_independently",
            "run_close_sync_aggregate_apply_from_clean_registry_worktree",
            "sync_each_github_issue_status",
            "persist_one_metadata_only_close_sync_pr",
        ],
    }
    _write_json(output_dir / "close-sync-aggregate-plan.json", payload)
    if not apply:
        return payload

    started = time.monotonic()
    verified_rows = [
        {**row, **verified_sources[row["bug_id"]]}
        for row in target_rows
    ]

    updated_paths: list[str] = []
    github_syncs: dict[str, Any] = {}
    verified_per_issue: list[dict[str, Any]] = []
    for row in verified_rows:
        item = row["bug_id"]
        record = row["record"]
        closed_at = row["closed_at"]
        updated = dict(record)
        updated.update(
            {
                "status": "fixed",
                "closed_at": closed_at,
                "fixed_at": record.get("fixed_at") or closed_at or _utc_now(),
                "fix_commit": row["source_merge_commit"],
                "pr_url": row["source_pr_url"],
                "validation_evidence": row["validation_evidence"],
            }
        )
        _write_json(row["source_path"], updated)
        updated_path = _repo_rel(row["source_path"], close_sync_root)
        updated_paths.append(updated_path)
        per_issue_gates = {
            key: str(updated.get(key) or "noop") for key in _CLOSE_SYNC_PRODUCTION_GATE_KEYS
        }
        issue_evidence = {
            "schema_version": payload["schema_version"],
            "workflow_gate": "close_synced",
            "aggregate_id": aggregate_id,
            "aggregate_key": aggregate_key,
            "bug_id": item,
            "merged_pr": row["source_pr_url"],
            "merge_commit": row["source_merge_commit"],
            "source_pr_url": row["source_pr_url"],
            "source_merge_commit": row["source_merge_commit"],
            "validation_evidence": row["validation_evidence"],
            "production_gates": per_issue_gates,
            "updated_bug_json": updated_path,
        }
        github_syncs[item] = _sync_github_issue_after_close(updated, issue_evidence, root=close_sync_root)
        if github_syncs[item].get("status") != "synced":
            raise WorkflowError(
                f"close-sync-aggregate GitHub Issue synchronization is incomplete for {item}: "
                f"status={github_syncs[item].get('status') or 'unknown'}"
            )
        _write_state(
            item,
            state="close_synced",
            root=close_sync_root,
            pr_url=row["source_pr_url"],
            commit=row["source_merge_commit"],
            validation_evidence=row["validation_evidence"],
            production_gates=per_issue_gates,
            github_issue_sync=github_syncs[item],
            next_actions=["persist_aggregate_close_sync_pr", "sync_local_main", "cleanup_after_merge"],
        )
        _append_event(
            item,
            event="close_sync_aggregate_apply",
            state="close_synced",
            root=close_sync_root,
            duration_seconds=0.0,
            evidence={
                "aggregate_id": aggregate_id,
                "source_pr_url": row["source_pr_url"],
                "source_merge_commit": row["source_merge_commit"],
            },
        )
        verified_per_issue.append(
            {
                **next(value for value in per_issue if value["bug_id"] == item),
                "source_merge_commit": row["source_merge_commit"],
                "pr_check": row["pr_check"],
            }
        )

    evidence_payload = {
        **payload,
        "workflow_gate": "close_synced",
        "dry_run": False,
        "per_issue": verified_per_issue,
        "updated_bug_jsons": updated_paths,
        "github_issue_sync": github_syncs,
        "duration_seconds": round(time.monotonic() - started, 3),
    }
    _write_json(output_dir / "close-sync-aggregate-evidence.json", evidence_payload)
    return evidence_payload


def build_cleanup_after_merge_plan(
    *,
    branch: str,
    bug_id: str | None = None,
    worktree: str | None = None,
    pr_url: str | None = None,
    apply: bool = False,
    sync_root: bool = False,
    canonical_root: str | None = None,
    source_merge_receipt: dict[str, Any] | None = None,
    source_receipt_path: str | None = None,
    verified_pr_check: dict[str, Any] | None = None,
    preflight_fetch: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(canonical_root) if canonical_root else _canonical_root()
    branch_bug_match = BUG_ID_RE.search(branch)
    evidence_bug_id = bug_id or (branch_bug_match.group(0).upper() if branch_bug_match else None)
    pre_cleanup_fetch = _cleanup_preflight_fetch_for_plan(
        root,
        apply=apply,
        cached=preflight_fetch,
    )
    current_branch = _git(["branch", "--show-current"], cwd=root, check=False)
    local_branches = set(
        _git(["for-each-ref", "--format=%(refname:short)", "refs/heads"], cwd=root, check=False).splitlines()
    )
    remote_ref_result = _run_read_command_with_retry(
        ["git", "ls-remote", "--heads", "origin", branch],
        cwd=root,
        timeout=60,
        attempts=2 if apply else 1,
    )
    remote_ref = str(remote_ref_result.get("stdout") or "") if remote_ref_result.get("ok") else ""
    merged_refs = set(_git(["branch", "--format=%(refname:short)", "--merged", "origin/main"], cwd=root, check=False).splitlines())
    merged = branch in merged_refs
    merge_verification = _cleanup_merge_verification(
        branch,
        pr_url,
        merged,
        cwd=root,
        verified_pr_check=verified_pr_check,
    )
    squash_merge_verified = bool(merge_verification["squash_merge_verified"])
    pr_check = merge_verification["pr_check"]
    tree_equivalent = bool(merge_verification["tree_equivalent_to_origin_main"])
    merge_verified = bool(merge_verification["verified"])
    explicit_worktree_path = Path(worktree) if worktree else None
    registered_worktree_path = (
        None
        if explicit_worktree_path and explicit_worktree_path.exists()
        else _registered_worktree_for_branch(branch, cwd=root)
    )
    worktree_path = explicit_worktree_path or registered_worktree_path
    worktree_clean = True
    worktree_registered = False
    worktree_exists = bool(worktree_path and worktree_path.exists())
    worktree_empty = False
    worktree_is_current_cwd = False
    worktree_orphan_profile: dict[str, Any] | None = None
    worktree_ignored_artifacts: dict[str, Any] | None = None
    worktree_process_references: dict[str, Any] | None = None
    merged_pr_receipt_profile: dict[str, Any] | None = None
    protected_receipt_paths = _cleanup_protected_receipt_paths(evidence_bug_id)
    evidence_finalization = _cleanup_evidence_finalization(evidence_bug_id)
    loaded_source_receipt = source_merge_receipt
    explicit_record_finalization: dict[str, Any] | None = None
    if loaded_source_receipt is None and source_receipt_path:
        requested_receipt_path = Path(source_receipt_path)
        receipt_candidates: list[tuple[Path, bool]] = []
        relative_receipt_path: Path | None = None
        if requested_receipt_path.is_absolute():
            receipt_candidates.append((requested_receipt_path, True))
        elif ".." not in requested_receipt_path.parts:
            relative_receipt_path = requested_receipt_path
            if worktree_path:
                receipt_candidates.append((worktree_path / requested_receipt_path, True))
            receipt_candidates.append((root / requested_receipt_path, True))
        seen_receipt_paths: set[str] = set()
        for receipt_path, tracked_candidate in receipt_candidates:
            try:
                resolved_receipt_path = receipt_path.resolve()
            except OSError:
                continue
            receipt_key = os.path.normcase(str(resolved_receipt_path))
            if receipt_key in seen_receipt_paths or not resolved_receipt_path.is_file():
                continue
            seen_receipt_paths.add(receipt_key)
            with contextlib.suppress(OSError, UnicodeError, json.JSONDecodeError, WorkflowError):
                candidate = _load_json(resolved_receipt_path)
                trusted_roots = [base.resolve() for base in (root, worktree_path) if base]
                trusted_bug_record = False
                for trusted_root in trusted_roots:
                    if resolved_receipt_path == trusted_root or trusted_root not in resolved_receipt_path.parents:
                        continue
                    tracked_relative_path = resolved_receipt_path.relative_to(trusted_root).as_posix()
                    tracked_bug_path = _git(
                        ["ls-files", "--error-unmatch", "--", tracked_relative_path],
                        cwd=trusted_root,
                        check=False,
                    ).strip()
                    if tracked_candidate and tracked_bug_path == tracked_relative_path:
                        trusted_bug_record = True
                        break
                if trusted_bug_record and candidate.get("bug_id"):
                    candidate_finalization = _cleanup_evidence_finalization_from_record(
                        candidate,
                        source=_repo_rel(resolved_receipt_path, worktree_path or root),
                        expected_bug_id=evidence_bug_id,
                    )
                    if candidate_finalization.get("durable_receipt_present"):
                        explicit_record_finalization = {
                            **candidate_finalization,
                            "evidence_source": "explicit_tracked_bug_record",
                        }
                loaded_source_receipt = candidate.get("source_merge_receipt") or candidate
                if explicit_record_finalization or candidate.get("source_merge_receipt"):
                    break
        if relative_receipt_path and explicit_record_finalization is None:
            origin_record = _run_command(
                ["git", "show", f"origin/main:{relative_receipt_path.as_posix()}"],
                cwd=root,
                timeout=30,
            )
            if origin_record.get("ok"):
                with contextlib.suppress(json.JSONDecodeError, TypeError, WorkflowError):
                    candidate = json.loads(str(origin_record.get("stdout") or ""))
                    if isinstance(candidate, dict) and candidate.get("bug_id"):
                        candidate_finalization = _cleanup_evidence_finalization_from_record(
                            candidate,
                            source=f"origin/main:{relative_receipt_path.as_posix()}",
                            expected_bug_id=evidence_bug_id,
                        )
                        if candidate_finalization.get("durable_receipt_present"):
                            explicit_record_finalization = {
                                **candidate_finalization,
                                "evidence_source": "origin_main_bug_record",
                            }
                    if isinstance(candidate, dict) and candidate.get("source_merge_receipt"):
                        loaded_source_receipt = candidate["source_merge_receipt"]
                    elif loaded_source_receipt is None and isinstance(candidate, dict):
                        loaded_source_receipt = candidate
    if explicit_record_finalization:
        evidence_finalization = explicit_record_finalization
    if worktree_path and worktree_path.exists():
        try:
            current_cwd = Path.cwd().resolve()
            resolved_worktree = worktree_path.resolve()
            worktree_is_current_cwd = current_cwd == resolved_worktree or resolved_worktree in current_cwd.parents
        except OSError:
            worktree_is_current_cwd = False
        try:
            worktree_empty = not any(worktree_path.iterdir())
        except OSError:
            worktree_empty = False
        status_result = _run_command(["git", "status", "--porcelain=v1"], cwd=worktree_path)
        worktree_branch = _git(["branch", "--show-current"], cwd=worktree_path, check=False) if status_result.get("ok") else ""
        worktree_registered = _path_is_registered_worktree(worktree_path, cwd=root) or (
            bool(status_result.get("ok")) and worktree_branch == branch
        )
        worktree_clean = bool(status_result.get("ok")) and status_result.get("stdout") == ""
        if not worktree_registered:
            worktree_orphan_profile = _orphan_worktree_dir_profile(worktree_path)
        elif worktree_clean:
            worktree_ignored_artifacts = _worktree_ignored_artifact_profile(
                worktree_path,
                canonical_root=root,
                protected_paths=protected_receipt_paths,
            )
            worktree_process_references = (
                _worktree_active_process_profile(worktree_path)
                if apply
                else {
                    "schema_version": "aistock_worktree_process_reference_v1",
                    "target": str(worktree_path),
                    "scan_status": "deferred_until_apply",
                    "reference_count": 0,
                    "references": [],
                }
            )
    already_absent_profile = {
        "local_branch_absent": branch not in local_branches,
        "remote_branch_absent": bool(remote_ref_result.get("ok") and not remote_ref.strip()),
        "registered_worktree_absent": registered_worktree_path is None and not worktree_exists,
        "worktree_path_absent": not worktree_exists,
        "pr_identity_verified": bool(
            pr_url
            and _cleanup_verified_pr_check_matches_target(
                pr_check,
                pr_url=pr_url,
                branch=branch,
            )
        ),
        "merge_commit_in_origin_main": False,
    }
    absent_merge_commit = _merge_commit_from_pr_check(pr_check)
    already_absent_profile["merge_commit_in_origin_main"] = bool(
        absent_merge_commit
        and _git_commit_is_ancestor(absent_merge_commit, "origin/main", root=root)
    )
    already_absent_verified = bool(
        not merge_verified
        and all(already_absent_profile.values())
    )
    merge_verification["already_absent_profile"] = already_absent_profile
    if already_absent_verified:
        merge_verification.update(
            {
                "method": "merged_pr_source_four_state_already_absent",
                "verified": True,
                "squash_merge_verified": True,
                "tree_equivalent_to_origin_main": False,
                "tree_equivalence_ref": _pr_head_oid_from_pr_check(pr_check),
                "tree_equivalence_target": absent_merge_commit,
            }
        )
        merge_verified = True
        squash_merge_verified = True
        tree_equivalent = False
    if (
        worktree_ignored_artifacts
        and worktree_ignored_artifacts.get("transient_count")
        and not evidence_finalization.get("durable_receipt_present")
        and loaded_source_receipt is not None
    ):
        source_receipt_profile = _source_merge_receipt_profile(
            loaded_source_receipt,
            bug_id=evidence_bug_id,
            source_pr_url=pr_url,
            merge_commit=_merge_commit_from_pr_check(pr_check),
        )
        if source_receipt_profile.get("durable_receipt_present"):
            evidence_finalization = {
                **evidence_finalization,
                "status": "finalized_source_merge_receipt",
                "durable_receipt_present": True,
                "source_merge_receipt_present": True,
                "source_merge_receipt_id": source_receipt_profile.get("receipt_id"),
                "runtime_verification": source_receipt_profile.get("runtime_verification"),
            }
    if (
        worktree_ignored_artifacts
        and worktree_ignored_artifacts.get("transient_count")
        and not evidence_finalization.get("durable_receipt_present")
        and pr_url
    ):
        merged_pr_receipt_profile = _merged_pr_validation_receipt_profile(pr_url)
        if merged_pr_receipt_profile.get("durable_receipt_present"):
            evidence_finalization = {
                **evidence_finalization,
                "status": "finalized_merged_pr_receipt",
                "durable_receipt_present": True,
                "merged_pr_receipt_present": True,
                "pr_url": merged_pr_receipt_profile.get("pr_url") or pr_url,
            }
    root_git = _git_snapshot(root) if root.exists() else {"ok": False, "error": "canonical root missing"}
    root_dirty_files = _dirty_files(root) if root.exists() else []
    origin_equivalent_dirty_files = _origin_equivalent_dirty_files(root, root_dirty_files) if root_dirty_files else []
    unrelated_root_dirty_files = sorted(set(root_dirty_files) - set(origin_equivalent_dirty_files))
    root_sync_safe_with_dirty = _root_sync_safe_with_dirty(root_git)
    blocking: list[str] = []
    warnings: list[str] = []
    if pre_cleanup_fetch.get("status") == "failed":
        result = pre_cleanup_fetch.get("result") if isinstance(pre_cleanup_fetch.get("result"), dict) else {}
        detail = result.get("stderr") or result.get("stdout") or "unknown error"
        blocking.append(f"failed to refresh origin/main before cleanup: {detail}")
    if not remote_ref_result.get("ok"):
        detail = remote_ref_result.get("stderr") or remote_ref_result.get("stdout") or "unknown error"
        blocking.append(f"failed to inspect remote branch before cleanup: {detail}")
    if branch == current_branch and apply:
        blocking.append("refusing to cleanup the currently checked-out branch")
    if not merge_verified:
        blocking.append(f"branch is not merged into origin/main: {branch}")
    if worktree_path and worktree_exists and worktree_is_current_cwd and apply and not root.exists():
        blocking.append(f"refusing to remove the current working directory because canonical root is unavailable: {worktree_path}")
    if (
        worktree_path
        and worktree_exists
        and not worktree_registered
        and not worktree_empty
        and not (worktree_orphan_profile or {}).get("safe_reparse_or_empty_only")
    ):
        blocking.append(f"worktree path exists but is not a registered git worktree: {worktree_path}")
    if worktree_path and worktree_registered and not worktree_clean:
        blocking.append(f"worktree is dirty: {worktree_path}")
    if worktree_ignored_artifacts and worktree_ignored_artifacts.get("scan_status") != "complete":
        blocking.append(
            "ignored artifact scan failed: "
            f"{worktree_ignored_artifacts.get('error') or worktree_path}"
        )
    if worktree_ignored_artifacts and worktree_ignored_artifacts.get("protected_count"):
        blocking.append(
            "worktree contains a durable receipt that has not been finalized: "
            f"{worktree_ignored_artifacts.get('protected_samples') or []}"
        )
    if worktree_ignored_artifacts and worktree_ignored_artifacts.get("unknown_count"):
        blocking.append(
            "worktree contains unknown ignored artifacts: "
            f"{worktree_ignored_artifacts.get('unknown_samples') or []}"
        )
    if (
        worktree_ignored_artifacts
        and worktree_ignored_artifacts.get("transient_count")
        and not evidence_finalization.get("durable_receipt_present")
    ):
        blocking.append(
            "transient evidence cannot be purged before compact durable receipt finalization: "
            f"{evidence_finalization.get('status')}"
        )
    if worktree_process_references and worktree_process_references.get("scan_status") in {"failed", "unavailable", "unsupported"}:
        blocking.append(
            "active-process reference scan failed: "
            f"{worktree_process_references.get('error') or worktree_process_references.get('scan_status')}"
        )
    if worktree_process_references and worktree_process_references.get("reference_count"):
        blocking.append(
            "worktree is referenced by active processes: "
            f"{worktree_process_references.get('references') or []}"
        )
    if sync_root:
        if not root.exists():
            blocking.append(f"canonical root missing: {root}")
        elif root_git.get("branch") != "main":
            blocking.append(f"canonical root is not on main: {root_git.get('branch')}")
        elif root_git.get("dirty") and origin_equivalent_dirty_files and not unrelated_root_dirty_files:
            warnings.append("canonical root has only origin/main-equivalent dirty file(s); cleanup apply can restore them safely")
        elif root_git.get("dirty") and root_sync_safe_with_dirty:
            if unrelated_root_dirty_files:
                warnings.append(
                    "canonical root has unrelated dirty file(s); sync is already at origin/main and cleanup will ignore them"
                )
            if origin_equivalent_dirty_files:
                warnings.append("canonical root has origin/main-equivalent dirty file(s); cleanup apply can restore them safely")
        elif root_git.get("dirty"):
            blocking.append(f"canonical root is dirty and not synced to origin/main: {root}")
    actions = []
    if sync_root:
        actions.append({"action": "sync_root_main", "root": str(root), "safe": not any("canonical root" in item for item in blocking)})
    if worktree_path and worktree_exists and worktree_registered:
        if worktree_is_current_cwd:
            actions.append({"action": "relocate_current_cwd", "root": str(root), "safe": root.exists()})
        if worktree_ignored_artifacts and worktree_ignored_artifacts.get("transient_count"):
            actions.append(
                {
                    "action": "purge_transient_worktree_artifacts",
                    "worktree": str(worktree_path),
                    "manifest_sha256": worktree_ignored_artifacts.get("manifest_sha256"),
                    "ignored_count": worktree_ignored_artifacts.get("ignored_count"),
                    "root_count": worktree_ignored_artifacts.get("transient_root_count"),
                    "safe": not blocking,
                }
            )
        actions.append({"action": "remove_worktree", "worktree": str(worktree_path), "safe": merge_verified and worktree_clean})
    elif worktree_path and worktree_exists and (worktree_empty or (worktree_orphan_profile or {}).get("safe_reparse_or_empty_only")):
        if worktree_is_current_cwd:
            actions.append({"action": "relocate_current_cwd", "root": str(root), "safe": root.exists()})
        actions.append({
            "action": "remove_orphan_worktree_dir",
            "worktree": str(worktree_path),
            "safe": merge_verified and bool((worktree_orphan_profile or {"safe_reparse_or_empty_only": worktree_empty}).get("safe_reparse_or_empty_only")),
        })
    if branch in local_branches:
        actions.append({"action": "delete_local_branch", "branch": branch, "safe": merge_verified})
    if remote_ref:
        actions.append({"action": "delete_remote_branch", "branch": branch, "safe": merge_verified})
    payload = {
        "schema_version": "aistock_issue_workflow_cleanup_v1",
        "generated_at": _utc_now(),
        "branch": branch,
        "worktree": str(worktree_path) if worktree_path else None,
        "canonical_root": str(root),
        "sync_root": sync_root,
        "merged_into_origin_main": merged,
        "merge_verification": merge_verification,
        "squash_merge_verified": squash_merge_verified,
        "tree_equivalent_to_origin_main": tree_equivalent,
        "pr_check": pr_check,
        "worktree_clean": worktree_clean,
        "worktree_exists": worktree_exists,
        "worktree_registered": worktree_registered,
        "worktree_empty": worktree_empty,
        "worktree_orphan_profile": worktree_orphan_profile,
        "worktree_ignored_artifacts": worktree_ignored_artifacts,
        "worktree_process_references": worktree_process_references,
        "evidence_finalization": evidence_finalization,
        "merged_pr_receipt_profile": merged_pr_receipt_profile,
        "evidence_bug_id": evidence_bug_id,
        "worktree_is_current_cwd": worktree_is_current_cwd,
        "pre_cleanup_fetch": pre_cleanup_fetch,
        "remote_ref_check": remote_ref_result,
        "root_git": root_git,
        "root_dirty_files": root_dirty_files,
        "origin_equivalent_dirty_files": origin_equivalent_dirty_files,
        "unrelated_root_dirty_files": unrelated_root_dirty_files,
        "root_sync_safe_with_dirty": root_sync_safe_with_dirty,
        "blocking": blocking,
        "warnings": warnings,
        "actions": actions,
        "dry_run": not apply,
        "workflow_gate": "ready_for_cleanup" if not blocking else "blocked",
    }
    if bug_id:
        payload["registry_intake_cleanup"] = build_registry_intake_cleanup_plan(
            bug_id=bug_id,
            apply=False,
            canonical_root=str(root),
        )
    output_dir = REPO_ROOT / WORKFLOW_ROOT / "cleanup"
    _write_json(output_dir / f"{_slug(branch)}-cleanup-plan.json", payload)
    if apply:
        if blocking:
            raise CleanupBlockedError(payload)
        started = time.monotonic()
        applied: list[dict[str, Any]] = []
        if pre_cleanup_fetch.get("status") == "fetched":
            applied.append(
                {
                    "command": (
                        "reuse prior git fetch origin --prune receipt"
                        if pre_cleanup_fetch.get("reused")
                        else pre_cleanup_fetch.get("command") or "git fetch origin --prune"
                    ),
                    "phase": (
                        "pre_cleanup_verification_reused"
                        if pre_cleanup_fetch.get("reused")
                        else "pre_cleanup_verification"
                    ),
                    "result": pre_cleanup_fetch.get("result"),
                }
            )
        if sync_root:
            if pre_cleanup_fetch.get("status") != "fetched":
                applied.append({"command": "git fetch origin --prune", "result": _execute_checked(["git", "fetch", "origin", "--prune"], cwd=root, timeout=120)})
            if origin_equivalent_dirty_files and not unrelated_root_dirty_files:
                applied.append(
                    {
                        "command": "git restore origin/main-equivalent dirty file(s)",
                        "result": _execute_checked(
                            ["git", "restore", "--source=origin/main", "--", *origin_equivalent_dirty_files],
                            cwd=root,
                            timeout=120,
                        ),
                    }
                )
            if root_sync_safe_with_dirty:
                applied.append(
                    {
                        "command": "skip git merge --ff-only origin/main; canonical root already synced and dirty files are unrelated",
                        "result": {"ok": True, "stdout": "", "stderr": "", "returncode": 0},
                    }
                )
            else:
                applied.append({"command": "git merge --ff-only origin/main", "result": _execute_checked(["git", "merge", "--ff-only", "origin/main"], cwd=root, timeout=120)})
        if worktree_path and worktree_path.exists() and worktree_is_current_cwd:
            os.chdir(root)
            applied.append(
                {
                    "command": f"chdir {root} before removing current worktree",
                    "result": {"ok": True, "stdout": "", "stderr": "", "returncode": 0},
                }
            )
        if (
            worktree_path
            and worktree_path.exists()
            and worktree_registered
            and worktree_ignored_artifacts
            and worktree_ignored_artifacts.get("transient_count")
        ):
            applied.append(
                {
                    "command": f"purge finalized transient artifacts from {worktree_path}",
                    "result": _purge_worktree_transient_artifacts(
                        worktree_path,
                        canonical_root=root,
                        expected_profile=worktree_ignored_artifacts,
                        protected_paths=protected_receipt_paths,
                    ),
                }
            )
        if worktree_path and worktree_path.exists() and worktree_registered:
            applied.append({"command": f"git worktree remove {worktree_path}", "result": _remove_worktree_with_reparse_fallback(root=root, worktree_path=worktree_path)})
        elif worktree_path and worktree_path.exists() and (worktree_empty or (worktree_orphan_profile or {}).get("safe_reparse_or_empty_only")):
            removed = _remove_reparse_or_empty_tree(worktree_path)
            applied.append(
                {
                    "command": f"remove orphan worktree dir {worktree_path}",
                    "result": removed,
                }
            )
            if removed.get("deferred"):
                payload.setdefault("warnings", []).append(
                    f"deferred empty worktree directory cleanup: {worktree_path}"
                )
                payload["deferred_cleanup"] = {
                    "schema_version": "aistock_issue_workflow_deferred_cleanup_v1",
                    "worktree": str(worktree_path),
                    "reason": removed.get("deferred_reason"),
                    "safe_to_retry": True,
                    "profile": removed.get("profile"),
                }
        if branch in local_branches:
            delete_flag = "-d" if merged else "-D"
            applied.append({"command": f"git branch {delete_flag} {branch}", "result": _execute_checked(["git", "branch", delete_flag, branch], cwd=root, timeout=120)})
        if remote_ref:
            applied.append(
                {
                    "command": f"git push origin --delete --force-with-lease {branch}",
                    "result": _delete_remote_branch_with_lease(
                        root=root,
                        branch=branch,
                        expected_remote_ref=remote_ref,
                    ),
                }
            )
        cleanup_verification = _cleanup_post_removal_verification(
            root=root,
            worktree_path=worktree_path,
            branch=branch,
        )
        payload["cleanup_verification"] = cleanup_verification
        if not cleanup_verification.get("all_clear"):
            payload["applied"] = applied
            deferred_only = bool(
                payload.get("deferred_cleanup")
                and cleanup_verification.get("registration_absent")
                and cleanup_verification.get("local_branch_absent")
                and cleanup_verification.get("remote_branch_absent")
            )
            payload["workflow_gate"] = "cleanup_deferred" if deferred_only else "cleanup_incomplete"
            payload["dry_run"] = False
            _write_json(output_dir / f"{_slug(branch)}-cleanup-evidence.json", payload)
            if deferred_only:
                return payload
            raise WorkflowError(f"post-cleanup verification failed: {cleanup_verification}")
        if bug_id:
            registry_cleanup = build_registry_intake_cleanup_plan(
                bug_id=bug_id,
                apply=True,
                canonical_root=str(root),
            )
            payload["registry_intake_cleanup"] = registry_cleanup
            if registry_cleanup.get("warnings"):
                payload["warnings"].extend(registry_cleanup.get("warnings") or [])
            try:
                with _GlobalBugIdAllocatorLock(timeout=30.0):
                    removed_reservation = compact_terminal_reservation(
                        _bug_id_reservation_root(),
                        bug_id,
                        min_age_seconds=0,
                    )
                payload["reservation_cleanup"] = {
                    "status": "removed" if removed_reservation else "already_absent_or_non_terminal",
                    "path": removed_reservation,
                    "inventory_scanned": False,
                }
            except (BugIdLockError, WorkflowError) as exc:
                payload["reservation_cleanup"] = {"status": "deferred", "error": str(exc)}
                payload["warnings"].append(f"terminal BUG reservation cleanup deferred: {exc}")
        payload["applied"] = applied
        payload["workflow_gate"] = "cleanup_done"
        payload["dry_run"] = False
        payload["duration_seconds"] = round(time.monotonic() - started, 3)
        _write_json(output_dir / f"{_slug(branch)}-cleanup-evidence.json", payload)
    return payload


def _load_cleanup_batch_manifest(manifest_path: str) -> dict[str, Any]:
    path = Path(manifest_path)
    if not path.is_absolute():
        path = REPO_ROOT / path
    if _is_reparse_or_symlink(path):
        raise WorkflowError(f"cleanup batch manifest must not be a symlink or reparse point: {path}")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise WorkflowError(f"cleanup batch manifest is unavailable: {path}: {exc}") from exc
    if not resolved.is_file() or _is_reparse_or_symlink(resolved):
        raise WorkflowError(f"cleanup batch manifest must be a regular non-reparse file: {resolved}")
    payload = _load_json(resolved)
    if payload.get("schema_version") != CLEANUP_BATCH_MANIFEST_SCHEMA:
        raise WorkflowError(
            f"cleanup batch manifest schema must be {CLEANUP_BATCH_MANIFEST_SCHEMA}"
        )
    raw_targets = payload.get("targets")
    if not isinstance(raw_targets, list) or not raw_targets:
        raise WorkflowError("cleanup batch manifest requires a non-empty targets list")
    if len(raw_targets) > CLEANUP_BATCH_MAX_TARGETS:
        raise WorkflowError(
            f"cleanup batch manifest exceeds {CLEANUP_BATCH_MAX_TARGETS} targets"
        )
    targets: list[dict[str, Any]] = []
    branches: set[str] = set()
    worktrees: set[str] = set()
    for index, raw in enumerate(raw_targets):
        if not isinstance(raw, dict):
            raise WorkflowError(f"cleanup batch target {index} must be an object")
        unknown = sorted(set(raw) - CLEANUP_BATCH_TARGET_KEYS)
        if unknown:
            raise WorkflowError(
                f"cleanup batch target {index} has unsupported fields: {unknown}"
            )
        invalid_types = sorted(
            key
            for key, value in raw.items()
            if value is not None and not isinstance(value, str)
        )
        if invalid_types:
            raise WorkflowError(
                f"cleanup batch target {index} fields must be strings: {invalid_types}"
            )
        target = {
            key: str(raw.get(key) or "").strip() or None
            for key in CLEANUP_BATCH_TARGET_KEYS
        }
        branch = str(target.get("branch") or "")
        if not branch:
            raise WorkflowError(f"cleanup batch target {index} requires branch")
        branch_check = _run_command(
            ["git", "check-ref-format", "--branch", branch],
            cwd=REPO_ROOT,
            timeout=15,
        )
        if not branch_check.get("ok"):
            raise WorkflowError(f"cleanup batch target {index} has invalid branch: {branch}")
        if branch in branches:
            raise WorkflowError(f"cleanup batch manifest repeats branch: {branch}")
        branches.add(branch)
        worktree = str(target.get("worktree") or "")
        if worktree:
            worktree_key = os.path.normcase(str(Path(worktree).resolve()))
            if worktree_key in worktrees:
                raise WorkflowError(f"cleanup batch manifest repeats worktree: {worktree}")
            worktrees.add(worktree_key)
        bug_id = str(target.get("bug_id") or "")
        if bug_id and not re.fullmatch(r"BUG-\d+", bug_id.upper()):
            raise WorkflowError(f"cleanup batch target {index} has invalid bug_id: {bug_id}")
        if bug_id:
            target["bug_id"] = bug_id.upper()
        targets.append(target)
    return {
        "path": str(resolved),
        "sha256": hashlib.sha256(resolved.read_bytes()).hexdigest(),
        "targets": targets,
    }


def _finalize_cleanup_bug_completion(payload: dict[str, Any], bug_id: str | None) -> dict[str, Any]:
    if payload.get("workflow_gate") != "cleanup_done" or not bug_id:
        return payload
    canonical_bug_id = bug_id.strip().upper()
    try:
        pre_cleanup_postmortem = build_postmortem_plan(
            bug_id=canonical_bug_id,
            output_markdown=False,
        )
        if _workflow_artifacts_enabled():
            pre_cleanup_path = REPO_ROOT / WORKFLOW_ROOT / canonical_bug_id / "postmortem-pre-cleanup.json"
            _write_json(pre_cleanup_path, pre_cleanup_postmortem)
            payload["pre_cleanup_postmortem_path"] = _repo_rel(pre_cleanup_path)
        else:
            payload["pre_cleanup_postmortem"] = {
                "artifact_policy": "compact_success_no_artifact",
                "timing_summary": pre_cleanup_postmortem.get("timing_summary"),
                "h6_summary": pre_cleanup_postmortem.get("h6_summary"),
                "context_metrics": pre_cleanup_postmortem.get("context_metrics"),
                "artifact_metrics": pre_cleanup_postmortem.get("artifact_metrics"),
                "task_card_availability": pre_cleanup_postmortem.get("task_card_availability"),
                "validation_receipt_summary": pre_cleanup_postmortem.get("validation_receipt_summary"),
            }
    except WorkflowError as exc:
        payload.setdefault("warnings", []).append(f"pre-cleanup postmortem skipped: {exc}")
    cleanup_evidence = {
        key: payload.get(key)
        for key in (
            "schema_version",
            "branch",
            "worktree",
            "canonical_root",
            "sync_root",
            "workflow_gate",
            "actions",
            "applied",
            "duration_seconds",
        )
    }
    payload["complete_state"] = _write_state(
        canonical_bug_id,
        state="complete",
        root=REPO_ROOT,
        cleanup_evidence=cleanup_evidence,
        pre_cleanup_postmortem=payload.get("pre_cleanup_postmortem"),
        next_actions=[],
    )
    return payload


def _cleanup_batch_target_receipt(
    target: dict[str, Any],
    *,
    plan: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    value = plan or {}
    return {
        "branch": target.get("branch"),
        "bug_id": target.get("bug_id"),
        "worktree": target.get("worktree"),
        "pr_url": target.get("pr_url"),
        "workflow_gate": value.get("workflow_gate") or "failed",
        "cleanup_verification": value.get("cleanup_verification"),
        "blocking": value.get("blocking") or ([error] if error else []),
        "warnings": value.get("warnings") or [],
        "duration_seconds": value.get("duration_seconds"),
        "error": error,
    }


def _cleanup_batch_failure_plan(
    target: dict[str, Any],
    *,
    root: Path,
    error: str,
) -> dict[str, Any]:
    worktree = Path(str(target["worktree"])) if target.get("worktree") else None
    try:
        verification = _cleanup_post_removal_verification(
            root=root,
            worktree_path=worktree,
            branch=str(target["branch"]),
        )
    except (OSError, WorkflowError) as exc:
        verification = {
            "schema_version": "aistock_worktree_cleanup_verification_v1",
            "all_clear": False,
            "verification_error": str(exc),
        }
    return {
        "workflow_gate": "cleanup_incomplete",
        "cleanup_verification": verification,
        "blocking": [error],
        "warnings": [],
    }


def build_cleanup_after_merge_batch_plan(
    *,
    manifest_path: str,
    apply: bool = False,
    sync_root: bool = False,
    canonical_root: str | None = None,
) -> dict[str, Any]:
    root = Path(canonical_root) if canonical_root else _canonical_root()
    manifest = _load_cleanup_batch_manifest(manifest_path)
    targets = manifest["targets"]
    shared_fetch = _cleanup_preflight_fetch_origin(root, apply=apply)
    output_dir = REPO_ROOT / WORKFLOW_ROOT / "cleanup-batch"
    checkpoint_path = output_dir / f"{manifest['sha256'][:16]}-evidence.json"
    payload: dict[str, Any] = {
        "schema_version": CLEANUP_BATCH_RESULT_SCHEMA,
        "generated_at": _utc_now(),
        "manifest_path": manifest["path"],
        "manifest_sha256": manifest["sha256"],
        "target_count": len(targets),
        "canonical_root": str(root),
        "sync_root": sync_root,
        "dry_run": not apply,
        "shared_preflight_fetch": shared_fetch,
        "results": [],
        "blocking": [],
        "workflow_gate": "running",
    }
    _write_json(checkpoint_path, payload)
    if apply and shared_fetch.get("status") != "fetched":
        result = shared_fetch.get("result") if isinstance(shared_fetch.get("result"), dict) else {}
        payload["blocking"] = [
            str(result.get("stderr") or result.get("stdout") or "shared cleanup fetch failed")
        ]
        payload["workflow_gate"] = "blocked"
        _write_json(checkpoint_path, payload)
        return payload

    started = time.monotonic()
    for index, target in enumerate(targets, start=1):
        cleanup_started = False
        try:
            verified_pr_check = (
                _verify_pr_merged(str(target["pr_url"]))
                if target.get("pr_url")
                else None
            )
            cleanup_started = True
            plan = build_cleanup_after_merge_plan(
                branch=str(target["branch"]),
                bug_id=target.get("bug_id"),
                worktree=target.get("worktree"),
                pr_url=target.get("pr_url"),
                apply=apply,
                sync_root=sync_root,
                canonical_root=str(root),
                source_receipt_path=target.get("source_receipt_path"),
                verified_pr_check=verified_pr_check,
                preflight_fetch=shared_fetch,
            )
            if apply:
                plan = _finalize_cleanup_bug_completion(plan, target.get("bug_id"))
            receipt = _cleanup_batch_target_receipt(target, plan=plan)
        except CleanupBlockedError as exc:
            receipt = _cleanup_batch_target_receipt(target, plan=exc.payload, error=str(exc))
        except WorkflowError as exc:
            failure_plan = (
                _cleanup_batch_failure_plan(target, root=root, error=str(exc))
                if cleanup_started
                else None
            )
            receipt = _cleanup_batch_target_receipt(target, plan=failure_plan, error=str(exc))
        payload["results"].append(receipt)
        payload["completed_count"] = index
        payload["last_progress_at"] = _utc_now()
        _write_json(checkpoint_path, payload)

    success_gate = "cleanup_done" if apply else "ready_for_cleanup"
    success_count = sum(1 for item in payload["results"] if item.get("workflow_gate") == success_gate)
    failed_count = len(targets) - success_count
    payload.update(
        {
            "success_count": success_count,
            "failed_count": failed_count,
            "duration_seconds": round(time.monotonic() - started, 3),
            "workflow_gate": (
                success_gate
                if failed_count == 0
                else ("cleanup_partial" if apply and success_count else "blocked")
            ),
        }
    )
    if not apply and failed_count:
        payload["workflow_gate"] = "blocked"
    payload["blocking"] = [
        f"{item.get('branch')}: {item.get('blocking') or [item.get('error') or 'cleanup failed']}"
        for item in payload["results"]
        if item.get("workflow_gate") != success_gate
    ]
    _write_json(checkpoint_path, payload)
    return payload


def cmd_start(args: argparse.Namespace) -> int:
    payload = build_start_plan(
        bug_id=args.bug_id,
        issue_json=args.issue_json,
        changed_files=list(args.changed_file or []),
        create_worktree=args.create_worktree,
        dry_run=args.dry_run,
        task_slug=args.task_slug,
        allow_missing_linkage=args.allow_missing_linkage,
        allow_closed=args.allow_closed,
        active_decision=None,
    )
    _emit_args(payload, args)
    return 0


def cmd_finish(args: argparse.Namespace) -> int:
    payload = build_finish_plan(
        bug_id=args.bug_id,
        issue_json=args.issue_json,
        changed_files=list(args.changed_file) if args.changed_file else None,
        base=args.base,
        head=args.head,
        validation_evidence=list(args.validation_evidence or []),
        plan_only=args.plan_only,
        allow_missing_evidence=args.allow_missing_evidence,
        fresh_process_evidence=list(args.fresh_process_evidence or []),
    )
    _emit_args(payload, args)
    return 0 if payload.get("closure_ready") or payload.get("draft_ready") else 2


def cmd_restart_plan(args: argparse.Namespace) -> int:
    payload = build_restart_plan(bug_id=args.bug_id, issue_json=args.issue_json)
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"operator_action_required", "not_required"} else 2


def cmd_post_restart_verify(args: argparse.Namespace) -> int:
    payload = build_post_restart_verify(
        bug_id=args.bug_id,
        issue_json=args.issue_json,
        target_id=args.target,
        expected_identity=args.expected_identity,
        timeout_seconds=args.timeout_seconds,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") == "verified" else 2


def cmd_triage_p0(args: argparse.Namespace) -> int:
    payload = build_triage_p0(include_fixed=args.include_fixed)
    _emit_args(payload, args)
    return 0


def cmd_run_p0(args: argparse.Namespace) -> int:
    payload = build_run_p0_plan(
        module=args.module,
        include_fixed=args.include_fixed,
        source=args.source,
        mode=args.mode,
    )
    _emit_args(payload, args)
    return 0


def cmd_start_batch(args: argparse.Namespace) -> int:
    payload = build_start_batch_plan(
        bug_ids=list(args.bug_id or []),
        create_worktree=args.create_worktree,
        dry_run=args.dry_run,
        task_slug=args.task_slug,
        allow_missing_linkage=args.allow_missing_linkage,
        allow_closed=args.allow_closed,
    )
    _emit_args(payload, args)
    return 0


def cmd_finish_batch(args: argparse.Namespace) -> int:
    payload = build_finish_batch_plan(
        batch_id=args.batch_id,
        bug_ids=list(args.bug_id or []),
        changed_files=list(args.changed_file) if args.changed_file else None,
        base=args.base,
        head=args.head,
        validation_evidence=list(args.validation_evidence or []),
        issue_commit=list(args.issue_commit or []),
        plan_only=args.plan_only,
        allow_missing_evidence=args.allow_missing_evidence,
    )
    _emit_args(payload, args)
    return 0 if payload.get("closure_ready") or payload.get("draft_ready") else 2


def cmd_fast_path(args: argparse.Namespace) -> int:
    payload = build_fast_path_plan(
        bug_id=args.bug_id,
        issue_json=args.issue_json,
        changed_files=list(args.changed_file or []),
        module=args.module,
        allow_missing_linkage=args.allow_missing_linkage,
        allow_closed=args.allow_closed,
    )
    _emit_args(payload, args)
    return 0


def cmd_workflow_smoke(args: argparse.Namespace) -> int:
    payload = build_workflow_smoke_plan(
        bug_id=args.bug_id,
        issue_json=args.issue_json,
        changed_files=list(args.changed_file or []),
        module=args.module,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") == "passed" else 2


def cmd_nightly_intake_smoke(args: argparse.Namespace) -> int:
    payload = build_nightly_intake_smoke_plan()
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") == "passed" else 2


def cmd_batch_workflow_smoke(args: argparse.Namespace) -> int:
    payload = build_batch_workflow_smoke_plan()
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") == "passed" else 2


def cmd_doctor(args: argparse.Namespace) -> int:
    payload = build_doctor_report(skip_external=args.skip_external)
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") != "blocked" else 2


def cmd_submit_bug(args: argparse.Namespace) -> int:
    payload = build_submit_bug_plan(
        title=args.title,
        module=args.module,
        severity=args.severity,
        description=args.description,
        expected=args.expected,
        actual=args.actual,
        reproduce_command=args.reproduce_command,
        evidence_refs=list(args.evidence_ref or []),
        changed_files=list(args.changed_file or []),
        plan_key=args.plan_key,
        nox_session=args.nox_session,
        candidate_type=args.candidate_type,
        bug_id=args.bug_id,
        github_issue_number=args.github_issue_number,
        github_issue_url=args.github_issue_url,
        create_github=args.create_github,
        apply=args.apply,
        allow_current_worktree=args.allow_current_worktree,
        create_registry_worktree=args.create_registry_worktree,
        create_fix_worktree=args.create_fix_worktree,
        registry_pr_only=args.registry_pr_only,
        dry_run=args.dry_run,
        added_files=list(args.added_file or []),
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "submitted"} else 2


def cmd_install_client(args: argparse.Namespace) -> int:
    payload = build_client_install_plan(
        apply=args.apply,
        codex_home=args.codex_home,
        claude_home=args.claude_home,
        install_codex=not args.skip_codex,
        install_claude=not args.skip_claude,
        selected_lane=args.selected_lane,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_install", "installed"} else 2


def cmd_verify_clients(args: argparse.Namespace) -> int:
    if args.skip_codex and args.skip_claude:
        raise WorkflowError("verify-clients requires at least one target client")
    with _ClientInstallLock():
        manifest = _client_manifest(
            Path(args.codex_home) if args.codex_home else None,
            Path(args.claude_home) if args.claude_home else None,
        )
    lane_verification = _client_lane_verification(
        manifest,
        selected_lane=args.selected_lane,
        verify_codex=not args.skip_codex,
        verify_claude=not args.skip_claude,
    )
    workflow_clients_current = bool(lane_verification["ready"])
    if args.workflow_only:
        payload = {
            "schema_version": "aistock_workflow_client_verification_v1",
            "workflow_gate": "ready" if workflow_clients_current else "blocked",
            "client_manifest": manifest,
            "codex_home": args.codex_home or str(_codex_home()),
            "claude_home": args.claude_home or str(_claude_home()),
            "verify_codex": not args.skip_codex,
            "verify_claude": not args.skip_claude,
            "selected_lane": args.selected_lane,
            "selected_lane_keys": lane_verification["selected_lane_keys"],
            "blocking": lane_verification["blocking"],
            "warnings": lane_verification["warnings"],
            "checkout_advisories": lane_verification["checkout_advisories"],
            "remediation": lane_verification["remediation"],
            "checked": lane_verification["checked"],
            "restart_recommended": False,
        }
        _emit_args(payload, args)
        return 0 if workflow_clients_current else 2

    changed = list(args.changed_file or [])
    if args.changed_files_file:
        changed.extend(Path(args.changed_files_file).read_text(encoding="utf-8").splitlines())
    payload = code_intelligence.build_client_verification(
        item_id=args.item_id,
        query=args.query,
        changed_files=changed,
        module=args.module,
        root=Path(args.root) if args.root else REPO_ROOT,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        skip_external=args.skip_external,
    )
    payload["client_manifest"] = manifest
    if args.output_md:
        _write_text(Path(args.output_md), code_intelligence.render_client_verification_summary(payload))
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready", "warning"} else 2


def cmd_triage_ci_issue(args: argparse.Namespace) -> int:
    payload = build_triage_ci_issue_plan(
        issue_number=args.issue,
        run_id=args.run_id,
        summary_json=args.summary_json,
        skip_github_summary=args.skip_github_summary,
    )
    _emit_args(payload, args)
    return 0


def cmd_ci_issue_janitor(args: argparse.Namespace) -> int:
    payload = build_ci_issue_janitor_plan(
        issue_numbers=list(args.issue or []),
        apply=args.apply,
        limit=args.limit,
        skip_github_summary=args.skip_github_summary,
        close_infra=not args.superseded_only,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "closed", "no_actionable_ci_issues"} else 2


def cmd_promote_ci_issue(args: argparse.Namespace) -> int:
    payload = build_promote_ci_issue_plan(
        issue_number=args.issue,
        apply=args.apply,
        bug_id=args.bug_id,
        summary_json=args.summary_json,
        skip_github_summary=args.skip_github_summary,
        create_registry_worktree=args.create_registry_worktree,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {
        "ready_for_apply",
        "promoted",
        "already_linked",
        "deferred_registry_pr_capability",
    } else 2


def cmd_promote_nightly_candidate(args: argparse.Namespace) -> int:
    payload = build_promote_nightly_candidate_plan(
        issue_payload=list(args.issue_payload or []),
        queue_manifest=args.queue_manifest,
        apply=args.apply,
        opt_in_auto_file=args.opt_in_auto_file,
        bug_id=args.bug_id,
        create_registry_worktree=args.create_registry_worktree,
        create_fix_worktree=args.create_fix_worktree,
        skip_dedupe_search=args.skip_dedupe_search,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "promoted"} else 2


def cmd_run(args: argparse.Namespace) -> int:
    payload = build_run_plan(
        bug_id=args.bug_id,
        mode=args.mode,
        issue_json=args.issue_json,
        changed_files=list(args.changed_file or []),
        create_worktree=args.create_worktree,
        dry_run=args.dry_run,
        validation_evidence=list(args.validation_evidence or []),
        task_slug=args.task_slug,
        allow_missing_linkage=args.allow_missing_linkage,
        allow_closed=args.allow_closed,
        base=args.base,
        head=args.head,
        push=args.push,
        create_pr=args.create_pr,
        watch_ci=args.watch_ci,
        pr_title=args.pr_title,
        force_new_worktree=args.force_new_worktree,
        force_reason=args.reason,
        pr_url=args.pr_url,
        merge=args.merge,
        sync_root=args.sync_root,
        branch=args.branch,
        worktree=args.worktree,
        production_gates=_production_gates_payload(args),
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") not in {"validation_evidence_missing", "blocked"} else 2


def cmd_resume(args: argparse.Namespace) -> int:
    payload = build_resume_plan(bug_id=args.bug_id, worktree=args.worktree, events_limit=args.events_limit)
    _emit_args(payload, args)
    return 0


def cmd_postmortem(args: argparse.Namespace) -> int:
    payload = build_postmortem_plan(
        bug_id=args.bug_id,
        worktree=args.worktree,
        output_markdown=not args.no_markdown,
        persist_artifacts=args.persist_artifacts,
    )
    _emit_args(payload, args)
    return 0


def cmd_watch_ci(args: argparse.Namespace) -> int:
    payload = build_watch_ci_plan(
        bug_id=args.bug_id,
        pr_url=args.pr_url,
        attempts=args.attempts,
        delay_seconds=args.delay_seconds,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"checks_passed", "checks_pending"} else 2


def cmd_close_sync(args: argparse.Namespace) -> int:
    if args.create_pr and not args.apply:
        raise WorkflowError("close-sync --create-pr requires --apply")
    payload = build_close_sync_plan(
        bug_id=args.bug_id,
        issue_json=args.issue_json,
        pr_url=args.pr_url,
        apply=args.apply,
        allow_missing_linkage=args.allow_missing_linkage,
        validation_evidence=list(args.validation_evidence or []),
        merge_commit=args.merge_commit,
        production_gates=_production_gates_payload(args),
        skip_github_check=args.skip_github_check,
        create_registry_worktree=args.create_registry_worktree,
        allow_current_worktree=args.allow_current_worktree,
        post_restart_receipt=args.post_restart_receipt,
    )
    if args.create_pr and payload.get("workflow_gate") != "fixed_source_pending_user_restart":
        payload["close_sync_commit"] = _maybe_commit_and_pr_close_sync(
            bug_id=str(payload.get("bug_id") or args.bug_id or "").upper(),
            close_sync=payload,
            validation_evidence=list(args.validation_evidence or []),
        )
    elif args.create_pr:
        payload["close_sync_commit"] = {
            "workflow_gate": "deferred_runtime_verification",
            "reason": "skip the intermediate pending close-sync PR; create one final PR after post-restart verification",
        }
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "close_synced", "fixed_source_pending_user_restart"} else 2


def cmd_close_sync_batch(args: argparse.Namespace) -> int:
    payload = build_close_sync_batch_plan(
        bug_ids=list(args.bug_id or []),
        pr_url=args.pr_url,
        apply=args.apply,
        allow_missing_linkage=args.allow_missing_linkage,
        validation_evidence=list(args.validation_evidence or []),
        merge_commit=args.merge_commit,
        production_gates=_production_gates_payload(args),
        skip_github_check=args.skip_github_check,
        create_registry_worktree=args.create_registry_worktree,
        allow_current_worktree=args.allow_current_worktree,
    )
    if args.apply and args.create_pr:
        payload["close_sync_commit"] = _maybe_commit_and_pr_close_sync(
            bug_id=payload["bug_ids"][0],
            close_sync=payload,
            validation_evidence=list(args.validation_evidence or []),
        )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "close_synced"} else 2


def cmd_close_sync_aggregate(args: argparse.Namespace) -> int:
    if args.create_pr and not args.apply:
        raise WorkflowError("close-sync-aggregate --create-pr requires --apply")
    if args.create_pr and not args.create_registry_worktree:
        raise WorkflowError("close-sync-aggregate --create-pr requires --create-registry-worktree")
    if args.merge_close_sync_pr and not (args.apply and args.create_pr):
        raise WorkflowError("close-sync-aggregate --merge-close-sync-pr requires --apply --create-pr")
    if args.cleanup and not args.merge_close_sync_pr:
        raise WorkflowError("close-sync-aggregate --cleanup requires --merge-close-sync-pr")
    if args.sync_root and not args.cleanup:
        raise WorkflowError("close-sync-aggregate --sync-root requires --cleanup")
    payload = build_close_sync_aggregate_plan(
        bug_ids=list(args.bug_id or []),
        apply=args.apply,
        source_pr_urls=_parse_bug_pr_mappings(list(args.source_pr or [])),
        skip_github_check=args.skip_github_check,
        create_registry_worktree=args.create_registry_worktree,
        allow_current_worktree=args.allow_current_worktree,
    )
    if args.apply and args.create_pr:
        receipt_summary = [
            f"{item.get('bug_id')}: {len(item.get('validation_evidence') or [])} durable validation receipt(s)"
            for item in payload.get("per_issue") or []
            if isinstance(item, dict)
        ]
        payload["close_sync_commit"] = _maybe_commit_and_pr_close_sync(
            bug_id=payload["bug_ids"][0],
            close_sync=payload,
            validation_evidence=receipt_summary,
        )
        if args.merge_close_sync_pr:
            close_sync_pr_merge = _merge_close_sync_pr_if_ready(
                bug_id=payload["bug_ids"][0],
                close_sync_commit=payload["close_sync_commit"],
                auto_merge=True,
            )
            payload["close_sync_pr_merge"] = close_sync_pr_merge
            if close_sync_pr_merge.get("workflow_gate") == "blocked":
                payload["workflow_gate"] = "blocked"
                payload["blocking"] = close_sync_pr_merge.get("blocking") or []
            elif close_sync_pr_merge.get("workflow_gate") in {"merged", "already_merged"}:
                merge_result = close_sync_pr_merge.get("merge") or {}
                verified_pr_check = (
                    close_sync_pr_merge.get("verified")
                    or (merge_result.get("verified") if isinstance(merge_result, dict) else None)
                )
                cleanup_plan, root_sync_deferred = _build_close_sync_cleanup_after_merge_plan_with_root_sync_deferral(
                    bug_id=payload["bug_ids"][0],
                    close_sync_commit=payload["close_sync_commit"],
                    close_sync_pr_merge=close_sync_pr_merge,
                    cleanup=args.cleanup,
                    apply=True,
                    sync_root=args.sync_root,
                    verified_pr_check=verified_pr_check,
                )
                payload["close_sync_cleanup"] = cleanup_plan
                if root_sync_deferred:
                    payload["root_sync_deferred"] = root_sync_deferred
                if cleanup_plan and cleanup_plan.get("workflow_gate") == "blocked":
                    payload["workflow_gate"] = "blocked"
                    payload["blocking"] = cleanup_plan.get("blocking") or []
                elif args.cleanup and cleanup_plan and cleanup_plan.get("workflow_gate") == "cleanup_done":
                    payload["workflow_gate"] = "complete"
                else:
                    payload["workflow_gate"] = "close_sync_persisted"
            else:
                payload["workflow_gate"] = "blocked"
                payload["blocking"] = [
                    "aggregate close-sync PR merge was requested but no merged PR identity was returned: "
                    f"status={close_sync_pr_merge.get('workflow_gate') or 'unknown'}"
                ]
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {
        "ready_for_apply",
        "close_synced",
        "close_sync_persisted",
        "complete",
    } else 2


def cmd_cleanup_after_merge(args: argparse.Namespace) -> int:
    payload = build_cleanup_after_merge_plan(
        branch=args.branch,
        bug_id=args.bug_id,
        worktree=args.worktree,
        pr_url=args.pr_url,
        apply=args.apply,
        sync_root=args.sync_root,
        canonical_root=args.canonical_root,
        source_receipt_path=args.source_receipt_path,
    )
    payload = _finalize_cleanup_bug_completion(payload, args.bug_id)
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_cleanup", "cleanup_done"} else 2


def cmd_cleanup_after_merge_batch(args: argparse.Namespace) -> int:
    payload = build_cleanup_after_merge_batch_plan(
        manifest_path=args.manifest,
        apply=args.apply,
        sync_root=args.sync_root,
        canonical_root=args.canonical_root,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") in {"ready_for_cleanup", "cleanup_done"} else 2


def cmd_merge_finalizer(args: argparse.Namespace) -> int:
    payload = build_merge_finalizer_plan(
        bug_id=args.bug_id,
        source_pr_url=args.source_pr_url,
        source_branch=args.source_branch,
        source_worktree=args.source_worktree,
        validation_evidence=list(args.validation_evidence or []),
        issue_json=args.issue_json,
        allow_missing_linkage=args.allow_missing_linkage,
        production_gates=_production_gates_payload(args),
        sync_root=args.sync_root,
        merge_close_sync_pr=args.merge_close_sync_pr,
        cleanup=args.cleanup,
        apply=args.apply,
    )
    _emit_args(payload, args)
    return 0 if payload.get("workflow_gate") not in {"blocked"} else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AIstock high-level issue-fix workflow orchestrator.")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_output_options(command_parser: argparse.ArgumentParser) -> None:
        command_parser.add_argument("--output")
        command_parser.add_argument(
            "--output-format",
            choices=OUTPUT_FORMAT_CHOICES,
            default="compact",
            help="Stdout format. Default compact keeps success output short; --output still writes the full JSON artifact.",
        )
        command_parser.add_argument(
            "--stdout-format",
            choices=OUTPUT_FORMAT_CHOICES,
            dest="output_format",
            help=argparse.SUPPRESS,
        )
    doctor = sub.add_parser("doctor", help="Check repo, GitHub, MCP, and client-entry readiness.")
    doctor.add_argument("--skip-external", action="store_true", help="Skip gh/network-style checks for offline tests.")
    add_output_options(doctor)
    doctor.set_defaults(func=cmd_doctor)

    submit_bug = sub.add_parser("submit-bug", help="Create a normalized BUG candidate and optionally sync GitHub/BUG JSON.")
    submit_bug.add_argument("--title", required=True)
    submit_bug.add_argument("--module", required=True)
    submit_bug.add_argument("--severity", default="P2", choices=["P0", "P1", "P2", "P3"])
    submit_bug.add_argument("--description")
    submit_bug.add_argument("--expected")
    submit_bug.add_argument("--actual")
    submit_bug.add_argument("--reproduce-command")
    submit_bug.add_argument("--evidence-ref", action="append")
    submit_bug.add_argument(
        "--changed-file",
        action="append",
        help="Existing repository-relative file expected to change; typos and directories are rejected.",
    )
    submit_bug.add_argument(
        "--added-file",
        action="append",
        help="New repository-relative file planned by the fix; existing or unowned paths are rejected.",
    )
    submit_bug.add_argument("--plan-key")
    submit_bug.add_argument("--nox-session")
    submit_bug.add_argument("--candidate-type", default="bug", choices=["bug", "regression", "infra_failure", "flaky"])
    submit_bug.add_argument("--bug-id", help="Use an already reserved BUG-NNN id instead of the allocator.")
    submit_bug.add_argument("--github-issue-number")
    submit_bug.add_argument("--github-issue-url")
    submit_bug.add_argument("--create-github", action="store_true", help="Use gh to create the linked GitHub Issue when --apply is set.")
    submit_bug.add_argument("--apply", action="store_true", help="Write candidate/BUG JSON and update allocator after GitHub linkage exists.")
    submit_bug.add_argument("--create-registry-worktree", action="store_true", help="Create a clean registry worktree/branch from origin/main before writing BUG JSON.")
    submit_bug.add_argument("--create-fix-worktree", action="store_true", help="Fast-chain BUG registration into a new fix worktree/branch so the fix PR persists the BUG JSON.")
    submit_bug.add_argument("--registry-pr-only", action="store_true", help="Stop after a registry-only BUG PR; normal workflows continue directly to fix.")
    submit_bug.add_argument("--dry-run", action="store_true", help="Plan registry worktree creation without writing files or creating a worktree.")
    submit_bug.add_argument(
        "--allow-current-worktree",
        action="store_true",
        help="Override the registry guard for emergency/manual use. Normal agent workflows must not use this on canonical main.",
    )
    add_output_options(submit_bug)
    submit_bug.set_defaults(func=cmd_submit_bug)

    install_client = sub.add_parser("install-client", help="Install or dry-run developer-client entry wrappers.")
    install_client.add_argument("--apply", action="store_true")
    install_client.add_argument("--codex-home")
    install_client.add_argument("--claude-home")
    install_client.add_argument("--skip-codex", action="store_true")
    install_client.add_argument("--skip-claude", action="store_true")
    install_client.add_argument("--selected-lane", choices=CLIENT_LANE_CHOICES)
    add_output_options(install_client)
    install_client.set_defaults(func=cmd_install_client)

    verify_clients = sub.add_parser(
        "verify-clients",
        help="Verify CodeGraph, Understand Anything, and Codex/Claude workflow client readiness.",
    )
    verify_clients.add_argument("--item-id", default="VERIFY-CODE-INTELLIGENCE")
    verify_clients.add_argument("--query", default="AIstock code intelligence workflow verification")
    verify_clients.add_argument("--changed-file", action="append")
    verify_clients.add_argument("--changed-files-file")
    verify_clients.add_argument("--module", default="validation")
    verify_clients.add_argument("--root")
    verify_clients.add_argument("--codex-home")
    verify_clients.add_argument("--claude-home")
    verify_clients.add_argument("--workflow-only", action="store_true")
    verify_clients.add_argument("--skip-codex", action="store_true")
    verify_clients.add_argument("--skip-claude", action="store_true")
    verify_clients.add_argument("--selected-lane", choices=CLIENT_LANE_CHOICES)
    verify_clients.add_argument("--output-dir")
    verify_clients.add_argument("--skip-external", action="store_true")
    verify_clients.add_argument("--output-md")
    add_output_options(verify_clients)
    verify_clients.set_defaults(func=cmd_verify_clients)

    triage_ci = sub.add_parser("triage-ci-issue", help="Summarize and classify an auto-filed CI/Nightly GitHub Issue.")
    triage_ci.add_argument("--issue", required=True, help="GitHub Issue number to triage.")
    triage_ci.add_argument("--run-id", help="Override or provide the Actions run id.")
    triage_ci.add_argument("--summary-json", help="Use an existing CI failure summary JSON instead of querying Actions.")
    triage_ci.add_argument("--skip-github-summary", action="store_true", help="Do not query Actions logs; emit a partial triage summary.")
    add_output_options(triage_ci)
    triage_ci.set_defaults(func=cmd_triage_ci_issue)

    ci_janitor = sub.add_parser(
        "ci-issue-janitor",
        help="Dry-run or close auto-filed CI issues already superseded by a later successful main run or classified as infra-only.",
    )
    ci_janitor.add_argument("--issue", action="append", help="Limit janitor to a specific GitHub Issue number; repeatable.")
    ci_janitor.add_argument("--limit", type=int, default=50, help="Maximum open auto-filed CI issues to scan when --issue is omitted.")
    ci_janitor.add_argument("--skip-github-summary", action="store_true", help="Do not query Actions logs; useful for tests only.")
    ci_janitor.add_argument(
        "--superseded-only",
        action="store_true",
        help="Only close unlinked issues superseded by later successful runs; leave infra-only issues for manual ops review.",
    )
    ci_janitor.add_argument(
        "--apply",
        action="store_true",
        help="Close only unlinked issues classified as superseded_by_later_main_success or infra-only.",
    )
    add_output_options(ci_janitor)
    ci_janitor.set_defaults(func=cmd_ci_issue_janitor)

    promote_ci = sub.add_parser("promote-ci-issue", help="Promote a triaged CI GitHub Issue into the BUG JSON workflow.")
    promote_ci.add_argument("--issue", required=True, help="GitHub Issue number to promote.")
    promote_ci.add_argument("--bug-id", help="Use an already reserved BUG-NNN id.")
    promote_ci.add_argument("--summary-json", help="Use an existing CI failure summary JSON instead of querying Actions.")
    promote_ci.add_argument("--skip-github-summary", action="store_true", help="Do not query Actions logs; promote with partial diagnostics.")
    promote_ci.add_argument(
        "--create-registry-worktree",
        action="store_true",
        help="Create a clean registry worktree before writing BUG JSON; required for normal Nightly/CI promotion.",
    )
    promote_ci.add_argument("--apply", action="store_true")
    add_output_options(promote_ci)
    promote_ci.set_defaults(func=cmd_promote_ci_issue)

    promote_nightly = sub.add_parser(
        "promote-nightly-candidate",
        help="Safely promote one high-quality Nightly BugCandidate payload into GitHub Issue + linked BUG JSON.",
    )
    promote_nightly.add_argument("--issue-payload", action="append", help="Path to one aistock_bug_candidate_github_issue_payload_v1 JSON file.")
    promote_nightly.add_argument("--queue-manifest", help="Read issue payload refs from a Nightly BugCandidate queue manifest; exactly one ready payload is required.")
    promote_nightly.add_argument("--bug-id", help="Use an already reserved BUG-NNN id.")
    promote_nightly.add_argument(
        "--opt-in-auto-file",
        action="store_true",
        help="Explicitly allow LLM-assisted issue text enhancement; deterministic ready candidates can promote without this flag.",
    )
    promote_nightly.add_argument(
        "--create-registry-worktree",
        action="store_true",
        help="Create a clean registry worktree before writing BUG JSON; required for normal Nightly promotion.",
    )
    promote_nightly.add_argument(
        "--create-fix-worktree",
        action="store_true",
        help="Create and seed the eventual fix worktree after issue creation; mutually exclusive with registry worktree.",
    )
    promote_nightly.add_argument("--skip-dedupe-search", action="store_true", help="Skip GitHub marker search; intended for offline tests only.")
    promote_nightly.add_argument("--apply", action="store_true")
    add_output_options(promote_nightly)
    promote_nightly.set_defaults(func=cmd_promote_nightly_candidate)

    run = sub.add_parser("run", help="Run the Phase 1 issue workflow state machine for one BUG.")
    run.add_argument("--bug-id", required=True)
    run.add_argument("--issue-json")
    run.add_argument("--mode", choices=["plan", "fix", "pr", "merge"], default="plan")
    run.add_argument("--changed-file", action="append")
    run.add_argument("--create-worktree", action="store_true")
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--task-slug")
    run.add_argument("--allow-missing-linkage", action="store_true")
    run.add_argument("--allow-closed", action="store_true")
    run.add_argument("--base", default="origin/main")
    run.add_argument("--head", default="HEAD")
    run.add_argument("--validation-evidence", action="append")
    run.add_argument("--push", action="store_true", help="Push the current task branch after validation evidence exists.")
    run.add_argument("--create-pr", action="store_true", help="Create a GitHub PR from the generated PR body after validation evidence exists.")
    run.add_argument("--watch-ci", action="store_true", help="Watch GitHub PR checks after creating the PR.")
    run.add_argument("--pr-title")
    run.add_argument("--force-new-worktree", action="store_true", help="Override the single-active-workflow guard; requires --reason.")
    run.add_argument("--reason", help="Auditable reason for --force-new-worktree.")
    run.add_argument("--pr-url", help="Merged or merge-ready PR URL for --mode merge.")
    run.add_argument("--merge", action="store_true", help="Explicitly authorize --mode merge to merge the PR when checks are green.")
    run.add_argument("--branch", help="Task branch for post-merge cleanup planning.")
    run.add_argument("--worktree", help="Task worktree for post-merge cleanup planning.")
    run.add_argument("--sync-root", action="store_true", help="Plan canonical root fast-forward after merge.")
    run.add_argument("--production-ddl-gate", default="noop")
    run.add_argument("--production-frontend-dependency-gate", default="noop")
    run.add_argument("--production-backend-dependency-gate", default="noop")
    add_output_options(run)
    run.set_defaults(func=cmd_run)

    resume = sub.add_parser("resume", help="Resume a BUG workflow from state.json/events.jsonl.")
    resume.add_argument("--bug-id", required=True)
    resume.add_argument("--worktree")
    resume.add_argument("--events-limit", type=int, default=8)
    add_output_options(resume)
    resume.set_defaults(func=cmd_resume)

    postmortem = sub.add_parser("postmortem", help="Summarize workflow timing, context cost, active-worktree, and cleanup evidence.")
    postmortem.add_argument("--bug-id", required=True)
    postmortem.add_argument("--worktree")
    postmortem.add_argument("--no-markdown", action="store_true")
    postmortem.add_argument(
        "--persist-artifacts",
        action="store_true",
        help="Write postmortem JSON/Markdown artifacts. Defaults to compact stdout only on successful workflows.",
    )
    add_output_options(postmortem)
    postmortem.set_defaults(func=cmd_postmortem)

    watch_ci = sub.add_parser("watch-ci", help="Refresh compact GitHub PR check state for an existing BUG workflow.")
    watch_ci.add_argument("--bug-id", required=True)
    watch_ci.add_argument("--pr-url", help="PR URL; defaults to state.json pr_url when present.")
    watch_ci.add_argument("--attempts", type=int, default=1)
    watch_ci.add_argument("--delay-seconds", type=int, default=0)
    add_output_options(watch_ci)
    watch_ci.set_defaults(func=cmd_watch_ci)

    start = sub.add_parser("start", help="Prepare a BUG fix workflow and context pack.")
    start.add_argument("--bug-id")
    start.add_argument("--issue-json")
    start.add_argument("--changed-file", action="append")
    start.add_argument("--create-worktree", action="store_true")
    start.add_argument("--dry-run", action="store_true")
    start.add_argument("--task-slug")
    start.add_argument("--allow-missing-linkage", action="store_true")
    start.add_argument("--allow-closed", action="store_true")
    add_output_options(start)
    start.set_defaults(func=cmd_start)

    finish = sub.add_parser("finish", help="Select validation and generate a PR-ready finish plan.")
    finish.add_argument("--bug-id")
    finish.add_argument("--issue-json")
    finish.add_argument("--changed-file", action="append")
    finish.add_argument("--base", default="origin/main")
    finish.add_argument("--head", default="HEAD")
    finish.add_argument("--validation-evidence", action="append")
    finish.add_argument(
        "--fresh-process-evidence",
        action="append",
        help="Evidence that an isolated fresh process loaded the persistent runtime fix; does not authorize user-backend restart.",
    )
    finish.add_argument("--plan-only", action="store_true")
    finish.add_argument("--allow-missing-evidence", action="store_true")
    add_output_options(finish)
    finish.set_defaults(func=cmd_finish)

    restart_plan = sub.add_parser(
        "restart-plan",
        help="Expand runtime target and operator runbook references without performing process control.",
    )
    restart_plan.add_argument("--bug-id")
    restart_plan.add_argument("--issue-json")
    add_output_options(restart_plan)
    restart_plan.set_defaults(func=cmd_restart_plan)

    post_restart_verify = sub.add_parser(
        "post-restart-verify",
        help="Run read-only runtime/API/DB probes after the user restarts a backend target.",
    )
    post_restart_verify.add_argument("--bug-id")
    post_restart_verify.add_argument("--issue-json")
    post_restart_verify.add_argument("--target", required=True)
    post_restart_verify.add_argument("--expected-identity")
    post_restart_verify.add_argument("--timeout-seconds", type=float, default=15.0)
    add_output_options(post_restart_verify)
    post_restart_verify.set_defaults(func=cmd_post_restart_verify)

    triage = sub.add_parser("triage-p0", help="List and group open/in-progress P0 BUG records.")
    triage.add_argument("--include-fixed", action="store_true")
    add_output_options(triage)
    triage.set_defaults(func=cmd_triage_p0)

    run_p0 = sub.add_parser("run-p0", help="Plan current P0 handling and recommend the next issue command.")
    run_p0.add_argument("--module")
    run_p0.add_argument("--source", choices=["local", "github", "both", "nightly"], default="local")
    run_p0.add_argument("--mode", choices=["plan"], default="plan")
    run_p0.add_argument("--include-fixed", action="store_true")
    add_output_options(run_p0)
    run_p0.set_defaults(func=cmd_run_p0)

    start_batch = sub.add_parser("start-batch", help="Prepare a same-module batch BUG workflow and context packs.")
    start_batch.add_argument("--bug-id", action="append", required=True)
    start_batch.add_argument("--create-worktree", action="store_true")
    start_batch.add_argument("--dry-run", action="store_true")
    start_batch.add_argument("--task-slug")
    start_batch.add_argument("--allow-missing-linkage", action="store_true")
    start_batch.add_argument("--allow-closed", action="store_true")
    add_output_options(start_batch)
    start_batch.set_defaults(func=cmd_start_batch)

    finish_batch = sub.add_parser("finish-batch", help="Select validation and generate a PR-ready batch finish plan.")
    finish_batch.add_argument("--batch-id")
    finish_batch.add_argument("--bug-id", action="append")
    finish_batch.add_argument("--changed-file", action="append")
    finish_batch.add_argument("--base", default="origin/main")
    finish_batch.add_argument("--head", default="HEAD")
    finish_batch.add_argument("--validation-evidence", action="append")
    finish_batch.add_argument("--issue-commit", action="append", help="Per-issue commit map entry, e.g. BUG-123=<sha>.")
    finish_batch.add_argument("--plan-only", action="store_true")
    finish_batch.add_argument("--allow-missing-evidence", action="store_true")
    add_output_options(finish_batch)
    finish_batch.set_defaults(func=cmd_finish_batch)

    fast_path = sub.add_parser("fast-path", help="Plan the lightest safe T0/T1/T2/T3 issue workflow path.")
    fast_path.add_argument("--bug-id")
    fast_path.add_argument("--issue-json")
    fast_path.add_argument("--changed-file", action="append")
    fast_path.add_argument("--module")
    fast_path.add_argument("--allow-missing-linkage", action="store_true")
    fast_path.add_argument("--allow-closed", action="store_true")
    add_output_options(fast_path)
    fast_path.set_defaults(func=cmd_fast_path)

    workflow_smoke = sub.add_parser("workflow-smoke", help="Dry-run the issue workflow chain without GitHub/PR/DB writes.")
    workflow_smoke.add_argument("--bug-id")
    workflow_smoke.add_argument("--issue-json")
    workflow_smoke.add_argument("--changed-file", action="append")
    workflow_smoke.add_argument("--module")
    add_output_options(workflow_smoke)
    workflow_smoke.set_defaults(func=cmd_workflow_smoke)

    nightly_smoke = sub.add_parser(
        "nightly-intake-smoke",
        help="Dry-run Nightly failure issue context generation without GitHub, DB, runtime, or tracked root writes.",
    )
    add_output_options(nightly_smoke)
    nightly_smoke.set_defaults(func=cmd_nightly_intake_smoke)

    batch_smoke = sub.add_parser(
        "batch-workflow-smoke",
        help="Dry-run same-module batch start/finish without GitHub, DB, runtime, or tracked root writes.",
    )
    add_output_options(batch_smoke)
    batch_smoke.set_defaults(func=cmd_batch_workflow_smoke)

    close = sub.add_parser("close-sync", help="Prepare a dry-run close/sync plan after PR merge.")
    close.add_argument("--bug-id")
    close.add_argument("--issue-json")
    close.add_argument("--pr-url")
    close.add_argument("--apply", action="store_true")
    close.add_argument("--allow-missing-linkage", action="store_true")
    close.add_argument("--validation-evidence", action="append")
    close.add_argument(
        "--post-restart-receipt",
        help="Ignored workflow receipt produced by post-restart-verify; required to close a runtime BUG after user restart.",
    )
    close.add_argument("--merge-commit")
    close.add_argument("--production-ddl-gate", default="noop")
    close.add_argument("--production-frontend-dependency-gate", default="noop")
    close.add_argument("--production-backend-dependency-gate", default="noop")
    close.add_argument(
        "--skip-github-check",
        action="store_true",
        help="Use exact GitHub REST merged-PR verification instead of GraphQL; Issue synchronization is still required.",
    )
    close.add_argument("--create-registry-worktree", action="store_true")
    close.add_argument(
        "--create-pr",
        action="store_true",
        help="After --apply, commit the close-sync BUG metadata, push its registry branch, and create or reuse the follow-up PR.",
    )
    close.add_argument(
        "--allow-current-worktree",
        action="store_true",
        help="Override close-sync root/main guard for tests or audited recovery only.",
    )
    add_output_options(close)
    close.set_defaults(func=cmd_close_sync)

    close_batch = sub.add_parser("close-sync-batch", help="Close-sync multiple BUG JSON records in one registry worktree/PR.")
    close_batch.add_argument("--bug-id", action="append", required=True)
    close_batch.add_argument("--pr-url")
    close_batch.add_argument("--apply", action="store_true")
    close_batch.add_argument("--allow-missing-linkage", action="store_true")
    close_batch.add_argument("--validation-evidence", action="append")
    close_batch.add_argument("--merge-commit")
    close_batch.add_argument("--production-ddl-gate", default="noop")
    close_batch.add_argument("--production-frontend-dependency-gate", default="noop")
    close_batch.add_argument("--production-backend-dependency-gate", default="noop")
    close_batch.add_argument(
        "--skip-github-check",
        action="store_true",
        help="Use exact GitHub REST merged-PR verification instead of GraphQL; Issue synchronization is still required.",
    )
    close_batch.add_argument("--create-registry-worktree", action="store_true")
    close_batch.add_argument("--create-pr", action="store_true", help="Commit, push, and open one close-sync PR for the batch after --apply.")
    close_batch.add_argument(
        "--allow-current-worktree",
        action="store_true",
        help="Override close-sync root/main guard for tests or audited recovery only.",
    )
    add_output_options(close_batch)
    close_batch.set_defaults(func=cmd_close_sync_batch)

    close_aggregate = sub.add_parser(
        "close-sync-aggregate",
        help="Close-sync independently merged non-runtime BUGs in one metadata-only registry PR.",
    )
    close_aggregate.add_argument("--bug-id", action="append", required=True)
    close_aggregate.add_argument(
        "--source-pr",
        action="append",
        metavar="BUG-ID=PR-URL",
        help="Explicit source PR identity for a BUG whose merged registry record does not yet contain pr_url.",
    )
    close_aggregate.add_argument("--apply", action="store_true")
    close_aggregate.add_argument(
        "--skip-github-check",
        action="store_true",
        help="Use exact GitHub REST merged-PR verification instead of GraphQL for every source PR.",
    )
    close_aggregate.add_argument("--create-registry-worktree", action="store_true")
    close_aggregate.add_argument(
        "--create-pr",
        action="store_true",
        help="Commit, push, and open one metadata-only close-sync PR after every source identity is verified.",
    )
    close_aggregate.add_argument(
        "--merge-close-sync-pr",
        action="store_true",
        help="After --create-pr, wait for required checks and merge the aggregate metadata PR.",
    )
    close_aggregate.add_argument(
        "--cleanup",
        action="store_true",
        help="After the aggregate PR merges, remove only its task-owned worktree and branch.",
    )
    close_aggregate.add_argument(
        "--sync-root",
        action="store_true",
        help="Fast-forward canonical main during aggregate PR cleanup.",
    )
    close_aggregate.add_argument(
        "--allow-current-worktree",
        action="store_true",
        help="Override close-sync root/main guard for tests or audited recovery only.",
    )
    add_output_options(close_aggregate)
    close_aggregate.set_defaults(func=cmd_close_sync_aggregate)

    cleanup = sub.add_parser("cleanup-after-merge", help="Safely sync root and clean merged issue worktrees/branches.")
    cleanup.add_argument("--branch", required=True)
    cleanup.add_argument("--bug-id", help="Mark the BUG workflow complete after successful cleanup.")
    cleanup.add_argument("--worktree")
    cleanup.add_argument("--pr-url", help="Merged PR URL used to verify squash-merged branch cleanup.")
    cleanup.add_argument("--sync-root", action="store_true")
    cleanup.add_argument("--canonical-root")
    cleanup.add_argument(
        "--source-receipt-path",
        help="BUG JSON or compact source_merge_receipt_v1 path used to authorize source cleanup.",
    )
    cleanup.add_argument("--apply", action="store_true")
    add_output_options(cleanup)
    cleanup.set_defaults(func=cmd_cleanup_after_merge)

    cleanup_batch = sub.add_parser(
        "cleanup-after-merge-batch",
        help="Safely clean an explicit bounded manifest of merged worktrees with one shared fetch.",
    )
    cleanup_batch.add_argument("--manifest", required=True)
    cleanup_batch.add_argument("--sync-root", action="store_true")
    cleanup_batch.add_argument("--canonical-root")
    cleanup_batch.add_argument("--apply", action="store_true")
    add_output_options(cleanup_batch)
    cleanup_batch.set_defaults(func=cmd_cleanup_after_merge_batch)

    finalizer = sub.add_parser("merge-finalizer", help="Finalize a merged issue PR through close-sync, optional close-sync PR merge, cleanup, and postmortem.")
    finalizer.add_argument("--bug-id", action="append", required=True)
    finalizer.add_argument("--issue-json")
    finalizer.add_argument("--source-pr-url", required=True, help="Merged source/fix PR URL.")
    finalizer.add_argument("--source-branch", help="Source/fix PR branch for cleanup.")
    finalizer.add_argument("--source-worktree", help="Source/fix worktree for cleanup.")
    finalizer.add_argument("--validation-evidence", action="append")
    finalizer.add_argument("--allow-missing-linkage", action="store_true")
    finalizer.add_argument("--sync-root", action="store_true", help="Fast-forward the canonical root during cleanup.")
    finalizer.add_argument("--merge-close-sync-pr", action="store_true", help="Also merge the generated close-sync PR when checks are green.")
    finalizer.add_argument("--cleanup", action="store_true", help="Run cleanup-after-merge after close-sync is persisted.")
    finalizer.add_argument("--apply", action="store_true")
    finalizer.add_argument("--production-ddl-gate", default="noop")
    finalizer.add_argument("--production-frontend-dependency-gate", default="noop")
    finalizer.add_argument("--production-backend-dependency-gate", default="noop")
    add_output_options(finalizer)
    finalizer.set_defaults(func=cmd_merge_finalizer)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except WorkflowPayloadError as exc:
        _emit_args(exc.payload, args)
        return 2
    except WorkflowError as exc:
        print(f"aistock_issue_workflow error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

