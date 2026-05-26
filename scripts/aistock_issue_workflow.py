from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

sys.path.insert(0, str(REPO_ROOT))
from scripts import issue_flow as flow  # noqa: E402
from scripts import ci_failure_issue_summary as ci_failure_summary  # noqa: E402
from scripts import code_intelligence_adapter as code_intelligence  # noqa: E402


class WorkflowError(ValueError):
    """Raised when the high-level AIstock issue workflow cannot proceed safely."""


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
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def _emit(payload: dict[str, Any], output: str | None = None) -> None:
    if output:
        _write_json(Path(output), payload)
    sys.stdout.write(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")


def _repo_rel(path: Path, root: Path | None = None) -> str:
    root = root or REPO_ROOT
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except Exception:
        return str(path).replace("\\", "/")


def _git(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
    cwd = cwd or REPO_ROOT
    proc = subprocess.run(["git", *args], cwd=str(cwd), text=True, capture_output=True, check=False)
    if check and proc.returncode != 0:
        raise WorkflowError(proc.stderr.strip() or proc.stdout.strip() or f"git {' '.join(args)} failed")
    return proc.stdout.strip()


def _run_command(args: list[str], cwd: Path | None = None, timeout: int = 30) -> dict[str, Any]:
    cwd = cwd or REPO_ROOT
    try:
        proc = subprocess.run(
            args,
            cwd=str(cwd),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=timeout,
        )
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout": proc.stdout.strip(),
            "stderr": proc.stderr.strip(),
        }
    except Exception as exc:
        return {"ok": False, "returncode": None, "stdout": "", "stderr": str(exc)}


def _workflow_dir(bug_id: str, root: Path | None = None) -> Path:
    return (root or REPO_ROOT) / WORKFLOW_ROOT / bug_id


def _state_path(bug_id: str, root: Path | None = None) -> Path:
    return _workflow_dir(bug_id, root) / "state.json"


def _events_path(bug_id: str, root: Path | None = None) -> Path:
    return _workflow_dir(bug_id, root) / "events.jsonl"


def _active_index_path(root: Path | None = None) -> Path:
    return (root or REPO_ROOT) / WORKFLOW_ROOT / "index" / "active_bugs.json"


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


def _workflow_timing_summary(bug_id: str, root: Path | None = None) -> dict[str, Any]:
    events = _read_events(bug_id, root)
    events = sorted(events, key=lambda item: str(item.get("timestamp") or ""))
    phases: dict[str, dict[str, Any]] = {}
    previous_ts: datetime | None = None
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
        if ts and previous_ts:
            delta = max(0.0, (ts - previous_ts).total_seconds())
            bucket["inferred_since_previous_seconds"] = round(
                float(bucket["inferred_since_previous_seconds"]) + delta,
                3,
            )
            inferred_duration += delta
        if ts:
            previous_ts = ts

    return {
        "schema_version": "aistock_issue_workflow_timing_summary_v1",
        "bug_id": bug_id,
        "event_count": len(events),
        "started_at": started_at,
        "ended_at": ended_at,
        "known_duration_seconds": round(known_duration, 3),
        "inferred_elapsed_seconds": round(inferred_duration, 3),
        "phases": phases,
        "code_repair_seconds": None,
        "notes": [
            "known_duration_seconds comes from command-level telemetry when available",
            "inferred_elapsed_seconds is wall-clock distance between recorded events and may include human/CI wait time",
            "code_repair_seconds is intentionally not guessed unless the agent records explicit repair events",
        ],
    }


def _update_active_index(bug_id: str, state_payload: dict[str, Any], *, root: Path | None = None) -> dict[str, Any]:
    root = root or REPO_ROOT
    path = _active_index_path(root)
    existing: dict[str, Any] = {}
    if path.exists():
        try:
            existing = _load_json(path)
        except WorkflowError:
            existing = {}
    index = dict(existing.get("active_bugs") or existing)
    key = bug_id.strip().upper()
    if not _state_is_active(state_payload):
        index.pop(key, None)
    else:
        index[key] = {
            "bug_id": key,
            "active_state": state_payload.get("state"),
            "branch": state_payload.get("branch"),
            "worktree": state_payload.get("worktree") or state_payload.get("cwd") or str(root),
            "pr_url": state_payload.get("pr_url"),
            "last_event_at": state_payload.get("updated_at"),
            "next_command": _next_command_for_state(key, state_payload),
        }
    payload = {
        "schema_version": "aistock_issue_workflow_active_index_v1",
        "updated_at": _utc_now(),
        "active_bugs": index,
    }
    _write_json(path, payload)
    return payload


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
    if stop_reason:
        payload["stop_reason"] = stop_reason
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


def _next_bug_id(root: Path | None = None) -> tuple[str, int]:
    allocator = _allocator_path(root)
    last_allocated = 0
    if allocator.exists():
        payload = _load_json(allocator)
        try:
            last_allocated = int(payload.get("last_allocated") or 0)
        except (TypeError, ValueError) as exc:
            raise WorkflowError(f"invalid bug id allocator: {allocator}") from exc
    next_number = last_allocated + 1
    return f"BUG-{next_number:03d}", next_number


def _write_allocator(next_number: int, root: Path | None = None) -> None:
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
    _git(["fetch", "origin", "main"])
    _git(["worktree", "add", str(worktree), "-b", branch, "origin/main"])
    plan["created"] = True
    return plan


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


def _github_issue_number_from_url(url: str) -> int | None:
    match = re.search(r"/issues/(\d+)(?:$|[?#])", url.strip())
    return int(match.group(1)) if match else None


def _is_inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


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


def _branch_for_path(path: Path) -> str | None:
    target = str(path.resolve()).replace("\\", "/").lower()
    for item in _parse_worktree_list():
        worktree = item.get("worktree")
        if worktree and str(Path(worktree).resolve()).replace("\\", "/").lower() == target:
            branch_ref = item.get("branch") or ""
            return branch_ref.removeprefix("refs/heads/") or None
    return None


def _state_is_active(state: dict[str, Any]) -> bool:
    value = str(state.get("state") or "")
    if not value:
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
        git = _git_snapshot(worktree) if worktree.exists() else {"ok": False, "error": f"workflow worktree missing: {worktree}"}
        active.append(
            {
                "bug_id": canonical_bug_id,
                "root": str(root),
                "worktree": str(worktree),
                "branch": state.get("branch") or git.get("branch") or _branch_for_path(worktree),
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
    dirty = [item for item in active if item.get("dirty")]
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
    first = active[0]
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
            "number,title,url,headRefName",
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
            "number,title,url,headRefName,mergedAt",
            "--limit",
            "20",
        ],
        cwd=REPO_ROOT,
        timeout=20,
    )
    if not result_open.get("ok") and not result_merged.get("ok"):
        return {
            "status": "unavailable",
            "open_prs": [],
            "merged_prs": [],
            "error": result_open.get("stderr") or result_merged.get("stderr") or "gh pr list failed",
        }

    def parse(result: dict[str, Any]) -> list[dict[str, Any]]:
        if not result.get("ok"):
            return []
        try:
            data = json.loads(str(result.get("stdout") or "[]"))
        except json.JSONDecodeError:
            return []
        return data if isinstance(data, list) else []

    open_prs = parse(result_open)
    merged_prs = parse(result_merged)
    cleanup_needed = bool(open_prs and merged_prs)
    return {
        "status": "cleanup_recommended" if cleanup_needed else "checked",
        "open_prs": open_prs,
        "merged_prs": merged_prs,
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


def find_bug_record(bug_id: str | None = None, issue_json: str | None = None) -> tuple[dict[str, Any], Path]:
    if issue_json:
        path = Path(issue_json)
        record = _load_json(path)
        return record, path
    if not bug_id:
        raise WorkflowError("Either --bug-id or --issue-json is required")
    normalized = bug_id.strip().upper()
    matches: list[tuple[dict[str, Any], Path]] = []
    for path in _bug_files():
        record = _load_json(path)
        if str(record.get("bug_id") or "").upper() == normalized:
            matches.append((record, path))
    if not matches:
        raise WorkflowError(f"BUG record not found: {normalized}")
    if len(matches) > 1:
        raise WorkflowError(f"Multiple BUG records found for {normalized}: {[str(path) for _, path in matches]}")
    return matches[0]


def _render_github_issue_body(record: dict[str, Any], candidate: dict[str, Any]) -> str:
    evidence = record.get("evidence_uris") or []
    scope = record.get("allowed_write_scope") or []
    verification = record.get("required_verification") or []
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
        "## Scope",
        *[f"- `{item}`" for item in scope or ["triage required"]],
        "",
        "## Required Verification",
        *[f"- `{item}`" for item in verification or ["l0"]],
        "",
        "## Evidence",
        *[f"- {item}" for item in evidence or ["n/a"]],
        "",
        "## Workflow Gates",
        "- production_ddl_gate: `noop`",
        "- production_frontend_dependency_gate: `noop`",
        "- production_backend_dependency_gate: `noop`",
    ]
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
    registry_pr_only: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    effective_apply = apply and not dry_run
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
    allocation_root = registry_root if registry_root.exists() else _registry_target_root()
    registry_guard = _validate_registry_apply_target(registry_root) if effective_apply else None
    if registry_guard and registry_guard["blocking"] and not allow_current_worktree:
        raise WorkflowError("; ".join(registry_guard["blocking"]))
    canonical_bug_id = (bug_id or "").strip().upper() or None
    allocated_number: int | None = None
    if not canonical_bug_id:
        canonical_bug_id, allocated_number = _next_bug_id(allocation_root)
    else:
        match = re.fullmatch(r"BUG-(\d{3,})", canonical_bug_id)
        if not match:
            raise WorkflowError("--bug-id must match BUG-NNN when provided")
        allocated_number = int(match.group(1))

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
        changed_file=changed_files,
    )
    event = flow.build_failure_event(event_args)
    candidate = flow.candidate_from_event(
        event,
        title=title,
        candidate_type=candidate_type,
        expected=expected,
        actual=actual or description,
    )
    record = flow.promote_candidate_to_bug(
        candidate,
        bug_id=canonical_bug_id,
        github_issue_number=github_issue_number,
        github_issue_url=github_issue_url,
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

    output_dir = registry_root / WORKFLOW_ROOT / canonical_bug_id
    candidate_path = output_dir / "candidate.json"
    github_body_path = output_dir / "github-issue-body.md"
    bug_path = _bug_json_path(record, registry_root)
    github_result: dict[str, Any] | None = None

    if effective_apply and bug_path.exists():
        raise WorkflowError(f"BUG JSON already exists: {bug_path}")

    if create_github and not record.get("github_issue_url") and effective_apply:
        _write_text(github_body_path, _render_github_issue_body(record, candidate))
        github_title = f"{canonical_bug_id} {severity}: {title}"
        result = _execute_checked(
            [
                "gh",
                "issue",
                "create",
                "--repo",
                GITHUB_REPO,
                "--title",
                github_title,
                "--body-file",
                str(github_body_path),
            ],
            cwd=registry_root,
            timeout=120,
        )
        issue_url = str(result.get("stdout") or "").splitlines()[-1].strip()
        issue_number = _github_issue_number_from_url(issue_url)
        if not issue_url or not issue_number:
            raise WorkflowError(f"cannot parse created GitHub issue URL: {issue_url!r}")
        record["github_issue_url"] = issue_url
        record["github_issue_number"] = issue_number
        github_result = {"created": True, "url": issue_url, "number": issue_number}
    elif create_github and not record.get("github_issue_url"):
        github_result = {"created": False, "planned": True, "body_path": _repo_rel(github_body_path, registry_root)}

    has_linkage = bool(record.get("github_issue_number") and record.get("github_issue_url"))
    if effective_apply and not has_linkage:
        raise WorkflowError("--apply requires --create-github or existing --github-issue-number and --github-issue-url")

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
                "warnings": ["registry worktree will be created on apply"],
                "planned": True,
            }
            if create_registry_worktree and not registry_root.exists()
            else _validate_registry_apply_target(registry_root)
        ),
        "registry_worktree_plan": registry_worktree_plan,
        "candidate_path": _repo_rel(candidate_path, registry_root),
        "github_issue_body_path": _repo_rel(github_body_path, registry_root),
        "bug_json_path": _repo_rel(bug_path, registry_root),
        "github": github_result or {
            "created": False,
            "number": record.get("github_issue_number"),
            "url": record.get("github_issue_url"),
        },
        "record": record,
        "registry_pr_only": registry_pr_only,
        "stale_pr_check": _stale_pr_check_for_bug(canonical_bug_id) if effective_apply else {"status": "not_applicable_before_apply"},
        "next_agent_steps": [
            "switch_to_registry_worktree",
            "commit_registry_only_pr_without_fix" if registry_pr_only else "continue_fix_in_same_task_branch",
            "do_not_write_bug_json_in_canonical_root",
        ] if effective_apply else [
            "create_or_switch_to_clean_registry_worktree",
            "rerun_submit_bug_with_github_linkage",
        ],
        "next_command": (
            f"cd /d {registry_root} && git status --short && git add tests/aistock_validation/bugs tmp/issue_workflow "
            f"&& git commit -m \"chore(issue): register {canonical_bug_id}\""
            if registry_pr_only
            else (
                f"cd /d {registry_root} && python scripts/aistock_issue_workflow.py run "
                f"--bug-id {canonical_bug_id} --mode plan"
            )
        )
        if effective_apply
        else (
            f"python scripts/aistock_issue_workflow.py submit-bug --title \"{title}\" --module {module} "
            f"--severity {severity} --create-github --create-registry-worktree --apply"
        ),
    }

    if effective_apply:
        _write_json(candidate_path, {"event": event, "candidate": candidate})
        _write_text(github_body_path, _render_github_issue_body(record, candidate))
        _write_json(bug_path, record)
        if bug_id is None:
            _write_allocator(int(allocated_number or canonical_bug_id.split("-")[1]), registry_root)
        _write_state(
            canonical_bug_id,
            state="discovered",
            root=registry_root,
            source_bug_json=_repo_rel(bug_path, registry_root),
            candidate_path=_repo_rel(candidate_path, registry_root),
            github_issue_number=record.get("github_issue_number"),
            github_issue_url=record.get("github_issue_url"),
            next_actions=["run_issue_workflow_plan", "create_worktree", "read_context_pack"],
        )
        payload["state_path"] = _repo_rel(_state_path(canonical_bug_id, registry_root), registry_root)
        payload["events_path"] = _repo_rel(_events_path(canonical_bug_id, registry_root), registry_root)
        payload["fix_chain"] = {
            "registry_pr_required": registry_pr_only,
            "continue_to_fix_in_same_workflow": not registry_pr_only,
            "run_next_command": payload["next_command"],
            "note": (
                "User explicitly requested registry-only tracking; stop after the registry PR."
                if registry_pr_only
                else "BUG registration now seeds workflow state so the same branch/worktree can continue to fix unless the user explicitly asks for a registry-only PR."
            ),
        }
    return payload


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
    _git(["worktree", "add", str(worktree), "-b", branch, "origin/main"])
    plan["created"] = True
    return plan


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
    _git(["worktree", "add", str(worktree), "-b", branch, "origin/main"])
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


def _build_code_intelligence_summary(
    *,
    item_id: str,
    record: dict[str, Any],
    changed_files: list[str] | None,
    root: Path,
) -> dict[str, Any]:
    return code_intelligence.build_summary(
        item_id=item_id,
        query=_issue_query(record, changed_files),
        changed_files=changed_files or [],
        root=root,
        skip_external=False,
    )


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


def _build_batch_code_intelligence_summary(
    *,
    batch_id: str,
    records: list[dict[str, Any]],
    changed_files: list[str] | None,
    root: Path,
) -> dict[str, Any]:
    return code_intelligence.build_summary(
        item_id=batch_id,
        query=_batch_code_intelligence_query(records, changed_files),
        changed_files=changed_files or [],
        root=root,
        skip_external=False,
    )

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
) -> dict[str, Any]:
    record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
    canonical_bug_id = str(record.get("bug_id") or bug_id or source_path.stem).upper()
    missing_linkage = _require_github_linkage(record, allow_missing=allow_missing_linkage)
    status = _require_fixable_status(record, allow_closed=allow_closed)
    worktree_plan = _maybe_create_worktree(
        record=record,
        bug_id=canonical_bug_id,
        create=create_worktree,
        dry_run=dry_run,
        task_slug=task_slug,
    )
    target_root = Path(worktree_plan["worktree"]) if create_worktree and not dry_run else REPO_ROOT
    target_bug_path = _bug_path_for_target(source_path, target_root)
    output_dir = target_root / WORKFLOW_ROOT / canonical_bug_id
    fix_ready = flow.build_fix_ready(record, changed_files)
    context_pack = flow.build_context_pack(record, changed_files)
    code_intelligence_summary = _build_code_intelligence_summary(
        item_id=canonical_bug_id,
        record=record,
        changed_files=changed_files,
        root=target_root,
    )
    context_pack["code_intelligence"] = {
        "provider": code_intelligence_summary.get("provider"),
        "status": code_intelligence_summary.get("status"),
        "context_ref": code_intelligence_summary.get("context_ref"),
        "manifest_ref": code_intelligence_summary.get("manifest_ref"),
        "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
        "fallback_used": code_intelligence_summary.get("fallback_used"),
        "understand_anything": code_intelligence_summary.get("understand_anything"),
    }
    fix_ready["code_intelligence"] = context_pack["code_intelligence"]
    fix_ready_path = output_dir / "fix-ready.json"
    context_json_path = output_dir / "context-pack.json"
    context_md_path = output_dir / "context-pack.md"
    if not dry_run:
        _write_json(fix_ready_path, fix_ready)
        _write_json(context_json_path, context_pack)
        _write_text(context_md_path, flow.render_context_pack_markdown(context_pack))
        context_metrics = {
            "context_pack_md": _size_and_token_estimate(context_md_path),
            "context_pack_json": _size_and_token_estimate(context_json_path),
            "fix_ready_json": _size_and_token_estimate(fix_ready_path),
        }
        _write_state(
            canonical_bug_id,
            state="context_ready",
            root=target_root,
            branch=worktree_plan.get("branch"),
            worktree=worktree_plan.get("worktree"),
            base=worktree_plan.get("base"),
            source_bug_json=_repo_rel(source_path),
            target_bug_json=_repo_rel(target_bug_path, target_root),
            context_pack_md=_repo_rel(context_md_path, target_root),
            context_pack_json=_repo_rel(context_json_path, target_root),
            fix_ready_path=_repo_rel(fix_ready_path, target_root),
            github_issue_number=record.get("github_issue_number"),
            github_issue_url=record.get("github_issue_url"),
            production_gates=fix_ready.get("validation_selection", {}).get("production_gates", {}),
            code_intelligence=context_pack.get("code_intelligence"),
            active_decision=active_decision,
            context_metrics=context_metrics,
            next_actions=[
                "switch_to_worktree_if_created",
                "read_context_pack_md",
                "fix_only_within_allowed_write_scope_or_stop_for_scope_expansion",
                "run_finish_plan_before_reporting_done",
            ],
        )
    else:
        context_metrics = {
            "context_pack_md": {"path": str(context_md_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
            "context_pack_json": {"path": str(context_json_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
            "fix_ready_json": {"path": str(fix_ready_path), "exists": False, "bytes": 0, "estimated_tokens": 0},
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
        "state_path": _repo_rel(_state_path(canonical_bug_id, target_root), target_root),
        "events_path": _repo_rel(_events_path(canonical_bug_id, target_root), target_root),
        "allowed_write_scope": fix_ready.get("allowed_write_scope") or [],
        "required_verification": fix_ready.get("required_verification") or [],
        "recommended_verification": fix_ready.get("recommended_verification") or [],
        "production_gates": fix_ready.get("validation_selection", {}).get("production_gates", {}),
        "code_intelligence": context_pack.get("code_intelligence"),
        "active_decision": active_decision,
        "context_metrics": context_metrics,
        "next_agent_steps": [
            "switch_to_worktree_if_created",
            "read_context_pack_md",
            "fix_only_within_allowed_write_scope_or_stop_for_scope_expansion",
            "run_finish_plan_before_reporting_done",
        ],
    }


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
) -> dict[str, Any]:
    record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
    canonical_bug_id = str(record.get("bug_id") or bug_id or source_path.stem).upper()
    changed = changed_files if changed_files is not None else flow.changed_files_from_git(base, head)
    validation = flow.select_validation(changed, module=record.get("module"))
    pr_quality = flow.build_pr_quality(base=base, head=head, issue_record=record, changed_files=changed)
    code_intelligence_summary = _build_code_intelligence_summary(
        item_id=canonical_bug_id,
        record=record,
        changed_files=changed,
        root=REPO_ROOT,
    )
    codegraph_tests = code_intelligence_summary.get("affected_tests", {}).get("suggested_tests") or []
    if codegraph_tests:
        validation["codegraph_suggested_tests"] = codegraph_tests
    evidence = [item for item in validation_evidence if item.strip()]
    closure_ready = bool(evidence) or plan_only or allow_missing_evidence
    output_dir = REPO_ROOT / WORKFLOW_ROOT / canonical_bug_id
    pr_body_path = output_dir / "pr-body.md"
    pr_body = render_pr_body(canonical_bug_id, record, changed, validation, pr_quality, evidence, closure_ready)
    _write_json(output_dir / "finish-plan.json", {
        "bug_id": canonical_bug_id,
        "changed_files": changed,
        "selected_validation": validation,
        "pr_quality": pr_quality,
        "validation_evidence": evidence,
        "closure_ready": closure_ready,
        "code_intelligence": code_intelligence_summary,
    })
    _write_text(pr_body_path, pr_body)
    next_state = "validation_passed" if evidence else ("validation_planned" if plan_only else "blocked")
    _write_state(
        canonical_bug_id,
        state=next_state,
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
        stop_reason=None if closure_ready else "validation_evidence_missing",
        next_actions=[
            "commit_only_task_files",
            "push_task_branch",
            "create_pr_from_pr_body",
            "watch_ci_before_merge",
        ] if evidence else ["run_required_validation", "rerun_finish_with_validation_evidence"],
    )
    payload = {
        "schema_version": "aistock_issue_workflow_finish_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "source_bug_json": _repo_rel(source_path),
        "changed_files": changed,
        "required_verification": validation.get("required_plans") or [],
        "recommended_verification": validation.get("recommended_plans") or [],
        "production_gates": validation.get("production_gates") or {},
        "scope_check": pr_quality.get("scope_check"),
        "code_intelligence": {
            "status": code_intelligence_summary.get("status"),
            "context_ref": code_intelligence_summary.get("context_ref"),
            "manifest_ref": code_intelligence_summary.get("manifest_ref"),
            "affected_tests_ref": code_intelligence_summary.get("affected_tests_ref"),
            "fallback_used": code_intelligence_summary.get("fallback_used"),
        },
        "codegraph_suggested_tests": codegraph_tests,
        "validation_evidence": evidence,
        "closure_ready": closure_ready,
        "workflow_gate": "ready_for_pr" if closure_ready else "validation_evidence_missing",
        "pr_body_path": _repo_rel(pr_body_path),
        "state_path": _repo_rel(_state_path(canonical_bug_id)),
        "events_path": _repo_rel(_events_path(canonical_bug_id)),
        "artifact_metrics": {
            "pr_body": _size_and_token_estimate(pr_body_path),
            "finish_plan": _size_and_token_estimate(output_dir / "finish-plan.json"),
        },
    }
    payload["pre_pr_gate"] = _pre_pr_gate(finish=payload, validation_evidence=evidence, root=REPO_ROOT, run_lint=False)
    if not closure_ready:
        payload["error"] = "validation evidence is required unless --plan-only or --allow-missing-evidence is used"
    return payload


def render_pr_body(
    bug_id: str,
    record: dict[str, Any],
    changed_files: list[str],
    validation: dict[str, Any],
    pr_quality: dict[str, Any],
    evidence: list[str],
    closure_ready: bool,
) -> str:
    gates = validation.get("production_gates") or {}
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
        "## Code intelligence",
        *[f"- CodeGraph suggested test: `{path}`" for path in validation.get("codegraph_suggested_tests") or ["none"]],
        "",
        "## Evidence",
        *[f"- {item}" for item in evidence or ["missing - run required validation before requesting merge"]],
        "",
        "## Production gates",
        f"- production_ddl_gate: `{gates.get('ddl', 'noop')}`",
        f"- production_frontend_dependency_gate: `{gates.get('frontend_dependency', 'noop')}`",
        f"- production_backend_dependency_gate: `{gates.get('backend_dependency', 'noop')}`",
        "",
        f"Closes #{record.get('github_issue_number')}" if record.get("github_issue_number") else "",
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


def build_run_p0_plan(*, module: str | None = None, include_fixed: bool = False) -> dict[str, Any]:
    triage = build_triage_p0(include_fixed=include_fixed)
    items = triage["items"]
    groups = triage["groups"]
    if module:
        items = [item for item in items if str(item.get("module") or "") == module]
        groups = [group for group in groups if str(group.get("module") or "") == module]
    recommended = items[0]["bug_id"] if items else None
    return {
        "schema_version": "aistock_issue_workflow_run_p0_v1",
        "generated_at": _utc_now(),
        "module": module,
        "count": len(items),
        "items": items,
        "groups": groups,
        "recommended_first_issue": recommended,
        "next_command": f"python scripts/aistock_issue_workflow.py run --bug-id {recommended} --mode plan --create-worktree"
        if recommended
        else None,
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
    bug_ids = [str(record.get("bug_id")) for record in records]
    batch_id = f"BATCH-{_slug(modules[0], max_len=32)}-{_today_compact()}-{_short_hash(*bug_ids)}"
    return {
        "batch_id": batch_id,
        "module": modules[0],
        "risk_tier": risks[0],
        "bug_ids": bug_ids,
        "required_verification": list(next(iter(verification_signatures))),
    }


def build_start_batch_plan(
    *,
    bug_ids: list[str],
    create_worktree: bool,
    dry_run: bool,
    task_slug: str | None,
    allow_missing_linkage: bool,
    allow_closed: bool,
) -> dict[str, Any]:
    record_pairs = _records_for_bug_ids(
        bug_ids,
        allow_missing_linkage=allow_missing_linkage,
        allow_closed=allow_closed,
    )
    records = [record for record, _ in record_pairs]
    signature = _batch_signature(records)
    batch_plan = flow.build_batch_plan(records)
    batch_plan["batch_id"] = signature["batch_id"]
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
            target_bug_path = _bug_path_for_target(source_path, target_root)
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
        *[f"- CodeGraph suggested test: `{path}`" for path in codegraph_tests or ["none"]],
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
    closing = [f"Closes #{record.get('github_issue_number')}" for record in records if record.get("github_issue_number")]
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
) -> dict[str, Any]:
    if not batch_id and not bug_ids:
        raise WorkflowError("finish-batch requires --batch-id or at least two --bug-id values")
    if batch_id:
        state = _load_batch_state(batch_id)
        if state and not bug_ids:
            bug_ids = [str(item) for item in state.get("bug_ids") or []]
    record_pairs = _records_for_bug_ids(bug_ids, allow_missing_linkage=False, allow_closed=True)
    records = [record for record, _ in record_pairs]
    signature = _batch_signature(records)
    if batch_id and batch_id != signature["batch_id"]:
        signature["batch_id"] = batch_id
    canonical_batch_id = signature["batch_id"]
    changed = changed_files if changed_files is not None else flow.changed_files_from_git(base, head)
    validation = flow.select_validation(changed, module=signature["module"])
    code_intelligence_summary = _build_batch_code_intelligence_summary(
        batch_id=canonical_batch_id,
        records=records,
        changed_files=changed,
        root=REPO_ROOT,
    )
    codegraph_tests = code_intelligence_summary.get("affected_tests", {}).get("suggested_tests") or []
    if codegraph_tests:
        validation["codegraph_suggested_tests"] = codegraph_tests
    evidence = [item for item in validation_evidence if item.strip()]
    closure_ready = bool(evidence) or plan_only or allow_missing_evidence
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
        "validation_evidence": evidence,
        "per_issue_commit_map": commit_map,
        "per_issue_closure_map": {
            str(record.get("bug_id")): flow._unique_strings(flow._as_list(record.get("closure_requirements")))
            for record in records
        },
        "code_intelligence": code_intelligence_summary,
        "closure_ready": closure_ready,
        "production_gates": validation.get("production_gates") or {},
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
        ),
    )
    next_state = "validation_passed" if evidence else ("validation_planned" if plan_only else "blocked")
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
        stop_reason=None if closure_ready else "validation_evidence_missing",
        next_actions=[
            "commit_only_batch_files",
            "push_task_branch",
            "create_pr_from_batch_pr_body",
            "watch_ci_before_merge",
        ] if evidence else ["run_required_validation", "rerun_finish_batch_with_validation_evidence"],
    )
    payload = {
        **finish_plan,
        "workflow_gate": "ready_for_pr" if closure_ready else "validation_evidence_missing",
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
        payload["error"] = "validation evidence is required unless --plan-only or --allow-missing-evidence is used"
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


def _client_manifest(codex_home: Path | None = None) -> dict[str, Any]:
    codex_home = codex_home or _codex_home()
    repo_skill = REPO_ROOT / ".codex" / "skills" / "fix-aistock-issue"
    global_skill = codex_home / "skills" / "fix-aistock-issue"
    repo_claude = REPO_ROOT / ".claude" / "commands" / "fix-aistock-issue.md"
    cli = REPO_ROOT / "scripts" / "aistock_issue_workflow.py"
    repo_head = _run_command(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, timeout=15)
    repo_skill_sha = _sha256_tree(repo_skill)
    global_skill_sha = _sha256_tree(global_skill)
    claude_sha = _sha256_file(repo_claude)
    cli_sha = _sha256_file(cli)
    codex_status = "missing_global"
    if repo_skill_sha and global_skill_sha:
        codex_status = "current" if repo_skill_sha == global_skill_sha else "stale"
    elif repo_skill_sha and not global_skill_sha:
        codex_status = "missing_global"
    elif not repo_skill_sha:
        codex_status = "missing_repo_skill"
    return {
        "schema_version": "aistock_issue_workflow_client_manifest_v1",
        "repo_commit": repo_head.get("stdout") if repo_head.get("ok") else None,
        "workflow_cli_sha256": cli_sha,
        "codex_skill_sha256": repo_skill_sha,
        "global_codex_skill_sha256": global_skill_sha,
        "claude_command_sha256": claude_sha,
        "codex_skill_status": codex_status,
        "claude_command_status": "present" if claude_sha else "missing",
        "paths": {
            "repo_codex_skill": str(repo_skill),
            "global_codex_skill": str(global_skill),
            "claude_command": str(repo_claude),
            "workflow_cli": str(cli),
        },
        "restart_recommended": codex_status != "current",
        "install_client_next_command": f"python {REPO_ROOT / 'scripts' / 'aistock_issue_workflow.py'} install-client --apply",
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

    mcp = _mcp_config_snapshot()
    if mcp["stale_worktree_config_files"]:
        warnings.append("MCP/Codex config mentions AIstock_worktrees; verify it is not a stale server target")

    client_manifest = _client_manifest()
    if client_manifest["codex_skill_status"] in {"stale", "missing_global"}:
        warnings.append("global Codex issue skill is missing or stale; run install-client --apply and restart old client windows")
    elif client_manifest["codex_skill_status"] == "missing_repo_skill":
        blocking.append("repo Codex issue skill is missing")
    if client_manifest["claude_command_status"] == "missing":
        warnings.append("Claude Code issue command is missing; Claude can still call the repo CLI directly")

    code_intel = code_intelligence.build_doctor_report(REPO_ROOT, skip_external=skip_external)
    for warning in code_intel.get("warnings") or []:
        warnings.append(f"code intelligence: {warning}")
    for item in code_intel.get("blocking") or []:
        blocking.append(f"code intelligence: {item}")

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
        "github": github,
        "mcp": mcp,
        "code_intelligence": code_intel,
        "client_manifest": client_manifest,
        "restart_recommended": client_manifest.get("restart_recommended"),
        "install_client_next_command": client_manifest.get("install_client_next_command"),
        "next_command": next_command,
    }


def build_client_install_plan(*, apply: bool = False, codex_home: str | None = None) -> dict[str, Any]:
    source_skill = REPO_ROOT / ".codex" / "skills" / "fix-aistock-issue"
    source_claude = REPO_ROOT / ".claude" / "commands" / "fix-aistock-issue.md"
    target_home = Path(codex_home) if codex_home else _codex_home()
    target_skill = target_home / "skills" / "fix-aistock-issue"
    blocking: list[str] = []
    if not source_skill.exists():
        blocking.append(f"missing repo Codex skill: {source_skill}")
    if not source_claude.exists():
        blocking.append(f"missing repo Claude Code command: {source_claude}")
    actions = [
        {
            "action": "sync_global_codex_skill",
            "source": str(source_skill),
            "target": str(target_skill),
            "safe": not blocking,
        },
        {
            "action": "verify_claude_code_command",
            "source": str(source_claude),
            "target": "repo-local .claude/commands/fix-aistock-issue.md",
            "safe": source_claude.exists(),
        },
    ]
    payload = {
        "schema_version": "aistock_issue_workflow_client_install_v1",
        "generated_at": _utc_now(),
        "dry_run": not apply,
        "workflow_gate": "ready_for_install" if not blocking else "blocked",
        "blocking": blocking,
        "actions": actions,
        "codex_home": str(target_home),
        "client_manifest_before": _client_manifest(target_home),
    }
    if apply:
        if blocking:
            raise WorkflowError("; ".join(blocking))
        if target_skill.exists():
            shutil.rmtree(target_skill)
        target_skill.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source_skill, target_skill)
        payload["workflow_gate"] = "installed"
        payload["dry_run"] = False
        payload["installed"] = [{"target": str(target_skill)}]
        payload["client_manifest_after"] = _client_manifest(target_home)
    manifest_path = REPO_ROOT / WORKFLOW_ROOT / "client-manifest.json"
    _write_json(manifest_path, payload.get("client_manifest_after") or payload.get("client_manifest_before") or _client_manifest(target_home))
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
    root, state = sorted(candidates, key=lambda item: str(item[0]))[-1]
    events_path = _events_path(canonical_bug_id, root)
    events: list[dict[str, Any]] = []
    if events_path.exists():
        lines = events_path.read_text(encoding="utf-8").splitlines()[-events_limit:]
        events = [json.loads(line) for line in lines if line.strip()]
    git = _git_snapshot(root) if root.exists() else {"ok": False, "error": f"workflow root missing: {root}"}
    dirty_stop = bool(git.get("dirty") and state.get("state") == "validation_passed")
    return {
        "schema_version": "aistock_issue_workflow_resume_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "workflow_root": str(root),
        "workflow_git": git,
        "worktree": state.get("worktree") or str(root),
        "branch": state.get("branch") or git.get("branch"),
        "state_path": _repo_rel(_state_path(canonical_bug_id, root), root),
        "events_path": _repo_rel(events_path, root),
        "state": state,
        "recent_events": events,
        "stop_conditions": ["commit task files before PR automation"] if dirty_stop else [],
        "next_command": _next_command_for_state(canonical_bug_id, state),
    }


def build_postmortem_plan(*, bug_id: str, worktree: str | None = None, output_markdown: bool = True) -> dict[str, Any]:
    canonical_bug_id = bug_id.strip().upper()
    roots = [Path(worktree)] if worktree else _state_roots_for_bug(canonical_bug_id)
    candidates = [(root, _load_state(canonical_bug_id, root)) for root in roots]
    candidates = [(root, state) for root, state in candidates if state]
    if not candidates:
        raise WorkflowError(f"No workflow state found for {canonical_bug_id}; run start or run --mode plan first")
    root, state = sorted(candidates, key=lambda item: str(item[0]))[-1]
    events = _read_events(canonical_bug_id, root)
    timing = _workflow_timing_summary(canonical_bug_id, root)
    active = _active_workflows_for_bug(canonical_bug_id)
    duplicate_active_count = max(0, len(active) - 1)
    stale_pr_check = _stale_pr_check_for_bug(canonical_bug_id)
    context_metrics = state.get("context_metrics") or {}
    artifact_metrics = {}
    finish_plan = root / WORKFLOW_ROOT / canonical_bug_id / "finish-plan.json"
    pr_body = root / WORKFLOW_ROOT / canonical_bug_id / "pr-body.md"
    if finish_plan.exists() or pr_body.exists():
        artifact_metrics = {
            "finish_plan": _size_and_token_estimate(finish_plan),
            "pr_body": _size_and_token_estimate(pr_body),
        }
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
        "context_metrics": context_metrics,
        "artifact_metrics": artifact_metrics,
        "active_workflows": active,
        "duplicate_active_count": duplicate_active_count,
        "stale_pr_check": stale_pr_check,
        "flow_overhead_estimate": {
            "known_duration_seconds": timing.get("known_duration_seconds"),
            "inferred_elapsed_seconds": timing.get("inferred_elapsed_seconds"),
            "event_count": timing.get("event_count"),
            "context_estimated_tokens": sum(
                int(item.get("estimated_tokens") or 0)
                for item in context_metrics.values()
                if isinstance(item, dict)
            ),
        },
        "production_gates": state.get("production_gates") or {},
        "recent_events": events[-20:],
    }
    output_dir = root / WORKFLOW_ROOT / canonical_bug_id
    if output_markdown:
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
        for phase, item in sorted((timing.get("phases") or {}).items()):
            lines.append(
                f"| `{phase}` | {item.get('event_count')} | {item.get('known_duration_seconds')} | {item.get('inferred_since_previous_seconds')} |"
            )
        lines.extend(
            [
                "",
                "## Production Gates",
                "",
                *[f"- {key}: `{value}`" for key, value in sorted((payload.get("production_gates") or {}).items())],
            ]
        )
        _write_text(output_dir / "postmortem.md", "\n".join(lines))
        payload["postmortem_md_path"] = _repo_rel(output_dir / "postmortem.md", root)
    payload["postmortem_json_path"] = _repo_rel(output_dir / "postmortem.json", root)
    _write_json(output_dir / "postmortem.json", payload)
    return payload


def _classify_ci_issue(summary: dict[str, Any], issue: dict[str, Any]) -> str:
    title_body = f"{issue.get('title') or ''}\n{issue.get('body') or ''}".lower()
    errors = "\n".join(
        str(item)
        for job in summary.get("failed_jobs") or []
        for item in [job.get("error_signature"), *(job.get("key_log_excerpt") or [])]
        if item
    ).lower()
    if any(token in title_body for token in ["flaky", "timeout", "network", "runner"]):
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
    classification = _classify_ci_issue(summary, issue)
    module = (summary.get("suspected_modules") or ["validation"])[0]
    first_job = (summary.get("failed_jobs") or [{}])[0]
    failed_test = ((first_job.get("failed_tests") or [None])[0] or "").split("::")[-1]
    suggested_title = (
        f"{module} CI failure requires triage: {failed_test or first_job.get('error_signature') or issue.get('title')}"
    )
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
        "classification_recommendation": classification,
        "linked_bug": {"bug_id": linked[0].get("bug_id"), "path": _repo_rel(linked[1])} if linked else None,
        "needs_bug_json": linked is None and classification != "infra_flaky",
        "suggested_bug": {
            "module": module,
            "severity": summary.get("severity") or "P1",
            "title": suggested_title[:180],
            "risk_area": "ci_failure_intake",
            "allowed_write_scope": summary.get("suspected_files") or [],
            "required_verification": [
                "Reproduce the failed job or focused test when applicable.",
                "Run issue-specific validation selected by the promoted BUG JSON.",
                "Keep BUG JSON and GitHub Issue synchronized.",
            ],
        },
        "next_command": (
            f"python scripts/aistock_issue_workflow.py promote-ci-issue --issue {issue.get('number')} --apply"
            if linked is None
            else f"python scripts/aistock_issue_workflow.py run --bug-id {linked[0].get('bug_id')} --mode plan --create-worktree"
        ),
    }
    _write_json(REPO_ROOT / WORKFLOW_ROOT / f"ci-issue-{issue.get('number')}" / "triage-ci-issue.json", payload)
    return payload


def build_promote_ci_issue_plan(
    *,
    issue_number: int | str,
    apply: bool,
    bug_id: str | None = None,
    summary_json: str | None = None,
    skip_github_summary: bool = False,
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
    summary = triage["summary"]
    suggested = triage["suggested_bug"]
    first_job = (summary.get("failed_jobs") or [{}])[0]
    failed_tests = first_job.get("failed_tests") or []
    error_signature = first_job.get("error_signature")
    details = ci_failure_summary.render_issue_markdown(summary)
    changed_files = list(suggested.get("allowed_write_scope") or [])
    if not changed_files:
        changed_files = ["scripts/aistock_issue_workflow.py"]
    plan = build_submit_bug_plan(
        title=suggested["title"],
        module=suggested["module"],
        severity=suggested["severity"],
        description=f"Auto-filed CI issue #{issue_number} requires actionable triage and repair.\n\n{details}",
        expected="CI/Nightly failure issues include enough diagnostic detail to enter the BUG JSON workflow without manual log rediscovery.",
        actual=f"Failure summary: {error_signature or (failed_tests[0] if failed_tests else 'diagnostic extraction incomplete')}",
        reproduce_command=str(summary.get("reproduce_command") or "Inspect linked CI run log."),
        evidence_refs=[str(summary.get("run_url") or ""), _github_issue_url(issue_number)],
        changed_files=changed_files,
        plan_key="ci_failure_issue_intake",
        nox_session=first_job.get("nox_session"),
        candidate_type="regression",
        bug_id=bug_id,
        github_issue_number=str(issue_number),
        github_issue_url=_github_issue_url(issue_number),
        create_github=False,
        apply=apply,
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


def _next_command_for_state(bug_id: str, state: dict[str, Any]) -> str:
    current = str(state.get("state") or "")
    worktree = str(state.get("worktree") or state.get("cwd") or "").strip()
    prefix = f"cd /d {worktree} && " if worktree else ""
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
        return "watch CI and rerun finish if changes are needed"
    return f"{prefix}python scripts/aistock_issue_workflow.py run --bug-id {bug_id} --mode plan"


def _execute_checked(args: list[str], *, cwd: Path | None = None, timeout: int = 120) -> dict[str, Any]:
    result = _run_command(args, cwd=cwd, timeout=timeout)
    if not result.get("ok"):
        raise WorkflowError(result.get("stderr") or result.get("stdout") or f"command failed: {' '.join(args)}")
    return result


def _execute_workflow_command(
    bug_id: str,
    args: list[str],
    *,
    state: str,
    cwd: Path | None = None,
    timeout: int = 120,
    event: str | None = None,
    root: Path | None = None,
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
    if not result.get("ok"):
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
        raw_path = line[3:].strip()
        if " -> " in raw_path:
            raw_path = raw_path.split(" -> ", 1)[1].strip()
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
) -> dict[str, Any]:
    changed_files = [str(item) for item in finish.get("changed_files") or []]
    scope_check = finish.get("scope_check") or {}
    status_rows = _git_status_paths(root)
    artifact_rows = [row for row in status_rows if _path_is_artifact(row["path"])]
    blocking: list[str] = []
    warnings: list[str] = []
    if not validation_evidence:
        blocking.append("validation evidence is required before PR creation")
    if scope_check.get("status") not in {None, "passed"}:
        blocking.append(f"scope check failed: {scope_check.get('violations') or scope_check.get('status')}")
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
        "artifact_guard": {
            "status": "passed" if not artifact_rows else "failed",
            "artifact_paths": artifact_rows,
            "patterns": list(ARTIFACT_PATH_PATTERNS),
        },
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
    pre_pr_gate = _pre_pr_gate(finish=finish, validation_evidence=finish.get("validation_evidence") or [], root=REPO_ROOT)
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
        body_path = str(REPO_ROOT / str(finish.get("pr_body_path")))
        result = _execute_workflow_command(
            bug_id,
            ["gh", "pr", "create", "--base", "main", "--head", branch, "--title", title, "--body-file", body_path],
            state="pr_opened",
            cwd=REPO_ROOT,
            timeout=120,
            event="command:gh_pr_create",
        )
        pr_url = str(result.get("stdout") or "").splitlines()[-1].strip()
        actions.append({"command": "gh pr create", "result": result})
        _write_state(bug_id, state="pr_opened", branch=branch, pr_url=pr_url, next_actions=["watch_ci_before_merge"])
    if watch_ci:
        if not pr_url:
            raise WorkflowError("--watch-ci requires --create-pr in this Phase 1 wrapper")
        result = _execute_workflow_command(
            bug_id,
            ["gh", "pr", "checks", pr_url, "--watch", "--interval", "10"],
            state="ci_running",
            cwd=REPO_ROOT,
            timeout=900,
            event="command:gh_pr_checks_watch",
        )
        actions.append({"command": "gh pr checks --watch", "result": result})
        check_text = "\n".join(str(result.get(key) or "") for key in ("stdout", "stderr"))
        check_ok = bool(result.get("ok")) and not re.search(
            r"\b(fail|failed|failure|cancelled|timed out)\b",
            check_text,
            re.IGNORECASE,
        )
        _write_state(
            bug_id,
            state="ci_green" if check_ok else "ci_running",
            branch=branch,
            pr_url=pr_url,
            next_actions=["merge_only_if_user_authorized"] if check_ok else ["inspect_ci_failure", "fix_on_same_task_branch"],
            stop_reason=None if check_ok else "ci_not_green",
        )
    return {"branch": branch, "dry_run": False, "pr_url": pr_url, "actions": actions, "worktree_guard": guard, "pre_pr_gate": pre_pr_gate}


def _verify_pr_merged(pr_url: str, *, skip_github_check: bool = False) -> dict[str, Any]:
    if skip_github_check:
        return {"checked": False, "merged": True, "reason": "skip_github_check"}
    result = _run_command(
        ["gh", "pr", "view", pr_url, "--json", "state,mergedAt,mergeCommit,url"],
        cwd=REPO_ROOT,
        timeout=30,
    )
    if not result.get("ok"):
        raise WorkflowError(result.get("stderr") or result.get("stdout") or f"cannot inspect PR: {pr_url}")
    try:
        payload = json.loads(str(result.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        raise WorkflowError(f"cannot parse gh pr view output for {pr_url}: {exc}") from exc
    merged = payload.get("state") == "MERGED" or bool(payload.get("mergedAt"))
    if not merged:
        raise WorkflowError(f"PR is not merged: {pr_url}")
    return {"checked": True, "merged": True, "pr": payload}


def _sync_github_issue_after_close(record: dict[str, Any], evidence_payload: dict[str, Any]) -> dict[str, Any]:
    issue_number = record.get("github_issue_number")
    if not issue_number:
        return {"status": "skipped_missing_issue_number"}
    lines = [
        f"AIstock workflow close-sync completed for `{record.get('bug_id')}`.",
        "",
        f"- PR: {evidence_payload.get('merged_pr') or 'n/a'}",
        f"- Merge commit: `{evidence_payload.get('merge_commit') or 'unknown'}`",
        "- BUG JSON status: `fixed`",
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
    tmp_comment = REPO_ROOT / WORKFLOW_ROOT / str(record.get("bug_id") or issue_number) / "github-close-comment.md"
    _write_text(tmp_comment, "\n".join(lines))
    comment = _run_command(
        ["gh", "issue", "comment", str(issue_number), "--repo", GITHUB_REPO, "--body-file", str(tmp_comment)],
        cwd=REPO_ROOT,
        timeout=60,
    )
    close = _run_command(["gh", "issue", "close", str(issue_number), "--repo", GITHUB_REPO], cwd=REPO_ROOT, timeout=60)
    return {
        "status": "synced" if comment.get("ok") and close.get("ok") else "warning",
        "comment": comment,
        "close": close,
        "comment_path": _repo_rel(tmp_comment),
    }


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


def _merge_pr_if_ready(pr_url: str) -> dict[str, Any]:
    view = _execute_checked(
        ["gh", "pr", "view", pr_url, "--json", "state,mergeStateStatus,statusCheckRollup,url"],
        cwd=REPO_ROOT,
        timeout=60,
    )
    payload = json.loads(str(view.get("stdout") or "{}"))
    checks = payload.get("statusCheckRollup") or []
    failed = [
        item.get("name") or item.get("workflowName") or item.get("__typename")
        for item in checks
        if str(item.get("conclusion") or "").upper() not in {"", "SUCCESS", "NEUTRAL", "SKIPPED"}
    ]
    pending = [item.get("name") for item in checks if str(item.get("status") or "").upper() != "COMPLETED"]
    if payload.get("state") == "MERGED":
        return {"already_merged": True, "view": payload}
    if failed or pending:
        raise WorkflowError(f"PR checks are not green; failed={failed}, pending={pending}")
    result = _execute_checked(["gh", "pr", "merge", pr_url, "--squash", "--delete-branch"], cwd=REPO_ROOT, timeout=180)
    verified = _verify_pr_merged(pr_url)
    return {"already_merged": False, "merge_result": result, "verified": verified}


def _merge_pr_if_ready_for_bug(bug_id: str, pr_url: str) -> dict[str, Any]:
    view = _execute_workflow_command(
        bug_id,
        ["gh", "pr", "view", pr_url, "--json", "state,mergeStateStatus,statusCheckRollup,url"],
        state="ci_green",
        cwd=REPO_ROOT,
        timeout=60,
        event="command:gh_pr_view_before_merge",
    )
    payload = json.loads(str(view.get("stdout") or "{}"))
    checks = payload.get("statusCheckRollup") or []
    failed = [
        item.get("name") or item.get("workflowName") or item.get("__typename")
        for item in checks
        if str(item.get("conclusion") or "").upper() not in {"", "SUCCESS", "NEUTRAL", "SKIPPED"}
    ]
    pending = [item.get("name") for item in checks if str(item.get("status") or "").upper() != "COMPLETED"]
    if payload.get("state") == "MERGED":
        return {"already_merged": True, "view": payload}
    if failed or pending:
        raise WorkflowError(f"PR checks are not green; failed={failed}, pending={pending}")
    result = _execute_workflow_command(
        bug_id,
        ["gh", "pr", "merge", pr_url, "--squash", "--delete-branch"],
        state="merged",
        cwd=REPO_ROOT,
        timeout=180,
        event="command:gh_pr_merge",
    )
    verified = _verify_pr_merged(pr_url)
    return {"already_merged": False, "merge_result": result, "verified": verified}


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
        merge_result = _merge_pr_if_ready_for_bug(canonical_bug_id, pr_url)
        close_sync = build_close_sync_plan(
            bug_id=canonical_bug_id,
            issue_json=issue_json,
            pr_url=pr_url,
            apply=True,
            allow_missing_linkage=allow_missing_linkage,
            validation_evidence=validation_evidence,
            production_gates=production_gates or _production_gates_payload(),
        )
        cleanup = None
        if branch:
            cleanup = build_cleanup_after_merge_plan(
                branch=branch,
                worktree=worktree,
                pr_url=pr_url,
                apply=False,
                sync_root=sync_root,
            )
        _write_state(
            canonical_bug_id,
            state="merged",
            pr_url=pr_url,
            commit=close_sync.get("merge_commit"),
            close_sync=close_sync,
            cleanup_plan=cleanup,
            next_actions=["run_cleanup_after_merge_apply_when_ready"],
        )
        return {
            "schema_version": "aistock_issue_workflow_run_v1",
            "generated_at": _utc_now(),
            "bug_id": canonical_bug_id,
            "mode": mode,
            "workflow_gate": "merged_close_synced",
            "merge": merge_result,
            "close_sync": close_sync,
            "cleanup": cleanup,
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
) -> dict[str, Any]:
    record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
    canonical_bug_id = str(record.get("bug_id") or bug_id or source_path.stem).upper()
    missing_linkage = _require_github_linkage(record, allow_missing=allow_missing_linkage)
    status = str(record.get("status") or "").strip()
    if status not in flow.VALID_BUG_STATUSES:
        raise WorkflowError(f"{canonical_bug_id} has invalid status for close/sync: {status!r}")
    evidence = [item for item in validation_evidence or [] if item.strip()]
    gates = production_gates or _production_gates_payload()
    output_dir = REPO_ROOT / WORKFLOW_ROOT / canonical_bug_id
    workflow_gate = "ready_for_apply" if pr_url and evidence else ("missing_validation_evidence" if pr_url else "missing_pr_url")
    payload = {
        "schema_version": "aistock_issue_workflow_close_sync_v1",
        "generated_at": _utc_now(),
        "bug_id": canonical_bug_id,
        "source_bug_json": _repo_rel(source_path),
        "current_status": status,
        "github_issue_number": record.get("github_issue_number"),
        "github_issue_url": record.get("github_issue_url"),
        "missing_github_linkage": missing_linkage,
        "merged_pr": pr_url,
        "merge_commit": merge_commit,
        "validation_evidence": evidence,
        "production_gates": gates,
        "dry_run": not apply,
        "workflow_gate": workflow_gate,
        "required_checks": [
            "closure_requirements_completed",
            "validation_evidence_attached",
            "BUG_JSON_and_GitHub_issue_status_aligned",
            "production_gates_reported",
        ],
        "next_agent_steps": [
            "verify_closure_requirements_item_by_item",
            "run_close_sync_apply_after_pr_merge",
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
        merge_commit = merge_commit or (
            ((pr_check.get("pr") or {}).get("mergeCommit") or {}).get("oid")
            if isinstance((pr_check.get("pr") or {}).get("mergeCommit"), dict)
            else None
        )
        updated = dict(record)
        updated.update(
            {
                "status": "fixed",
                "fixed_at": _utc_now(),
                "fix_commit": merge_commit,
                "pr_url": pr_url,
                "validation_evidence": evidence,
                **gates,
            }
        )
        _write_json(source_path, updated)
        evidence_payload = {
            **payload,
            "workflow_gate": "close_synced",
            "dry_run": False,
            "pr_check": pr_check,
            "merge_commit": merge_commit,
            "updated_bug_json": _repo_rel(source_path),
        }
        github_sync = (
            {"status": "skipped_github_check_disabled"}
            if skip_github_check
            else _sync_github_issue_after_close(updated, evidence_payload)
        )
        evidence_payload["github_issue_sync"] = github_sync
        _write_json(output_dir / "close-sync-evidence.json", evidence_payload)
        timing = _workflow_timing_summary(canonical_bug_id)
        _write_state(
            canonical_bug_id,
            state="close_synced",
            pr_url=pr_url,
            commit=merge_commit,
            validation_evidence=evidence,
            production_gates=gates,
            github_issue_sync=github_sync,
            timing_summary=timing,
            next_actions=["sync_local_main", "cleanup_after_merge"],
        )
        _append_event(
            canonical_bug_id,
            event="close_sync_apply",
            state="close_synced",
            root=REPO_ROOT,
            duration_seconds=time.monotonic() - started,
            evidence={
                "pr_url": pr_url,
                "merge_commit": merge_commit,
                "github_issue_sync_status": github_sync.get("status"),
            },
        )
        evidence_payload["timing_summary"] = _workflow_timing_summary(canonical_bug_id)
        return evidence_payload
    return payload


def build_cleanup_after_merge_plan(
    *,
    branch: str,
    worktree: str | None = None,
    pr_url: str | None = None,
    apply: bool = False,
    sync_root: bool = False,
    canonical_root: str | None = None,
) -> dict[str, Any]:
    root = Path(canonical_root) if canonical_root else _canonical_root()
    current_branch = _git(["branch", "--show-current"], check=False)
    local_branches = set(_git(["for-each-ref", "--format=%(refname:short)", "refs/heads"], check=False).splitlines())
    remote_ref = _git(["ls-remote", "--heads", "origin", branch], check=False)
    merged_refs = set(_git(["branch", "--format=%(refname:short)", "--merged", "origin/main"], check=False).splitlines())
    merged = branch in merged_refs
    squash_merge_verified = False
    pr_check: dict[str, Any] | None = None
    tree_equivalent = False
    if not merged and pr_url:
        pr_check = _verify_pr_merged(pr_url)
        tree_equivalent = bool(_run_command(["git", "diff", "--quiet", branch, "origin/main"], cwd=REPO_ROOT).get("ok"))
        squash_merge_verified = bool(pr_check.get("merged")) and tree_equivalent
    worktree_path = Path(worktree) if worktree else None
    worktree_clean = True
    if worktree_path and worktree_path.exists():
        worktree_clean = _run_command(["git", "status", "--porcelain=v1"], cwd=worktree_path).get("stdout") == ""
    root_git = _git_snapshot(root) if root.exists() else {"ok": False, "error": "canonical root missing"}
    blocking: list[str] = []
    if branch == current_branch and apply:
        blocking.append("refusing to cleanup the currently checked-out branch")
    if not (merged or squash_merge_verified):
        blocking.append(f"branch is not merged into origin/main: {branch}")
    if worktree_path and worktree_path.exists() and not worktree_clean:
        blocking.append(f"worktree is dirty: {worktree_path}")
    if sync_root:
        if not root.exists():
            blocking.append(f"canonical root missing: {root}")
        elif root_git.get("dirty"):
            blocking.append(f"canonical root is dirty: {root}")
        elif root_git.get("branch") != "main":
            blocking.append(f"canonical root is not on main: {root_git.get('branch')}")
    actions = []
    if sync_root:
        actions.append({"action": "sync_root_main", "root": str(root), "safe": not any("canonical root" in item for item in blocking)})
    if worktree_path and worktree_path.exists():
        actions.append({"action": "remove_worktree", "worktree": str(worktree_path), "safe": (merged or squash_merge_verified) and worktree_clean})
    if branch in local_branches:
        actions.append({"action": "delete_local_branch", "branch": branch, "safe": merged or squash_merge_verified})
    if remote_ref:
        actions.append({"action": "delete_remote_branch", "branch": branch, "safe": merged or squash_merge_verified})
    payload = {
        "schema_version": "aistock_issue_workflow_cleanup_v1",
        "generated_at": _utc_now(),
        "branch": branch,
        "worktree": str(worktree_path) if worktree_path else None,
        "canonical_root": str(root),
        "sync_root": sync_root,
        "merged_into_origin_main": merged,
        "squash_merge_verified": squash_merge_verified,
        "tree_equivalent_to_origin_main": tree_equivalent,
        "pr_check": pr_check,
        "worktree_clean": worktree_clean,
        "root_git": root_git,
        "blocking": blocking,
        "actions": actions,
        "dry_run": not apply,
        "workflow_gate": "ready_for_cleanup" if not blocking else "blocked",
    }
    output_dir = REPO_ROOT / WORKFLOW_ROOT / "cleanup"
    _write_json(output_dir / f"{_slug(branch)}-cleanup-plan.json", payload)
    if apply:
        if blocking:
            raise WorkflowError("; ".join(blocking))
        started = time.monotonic()
        applied: list[dict[str, Any]] = []
        if sync_root:
            applied.append({"command": "git fetch origin --prune", "result": _execute_checked(["git", "fetch", "origin", "--prune"], cwd=root, timeout=120)})
            applied.append({"command": "git merge --ff-only origin/main", "result": _execute_checked(["git", "merge", "--ff-only", "origin/main"], cwd=root, timeout=120)})
        if worktree_path and worktree_path.exists():
            applied.append({"command": f"git worktree remove {worktree_path}", "result": _execute_checked(["git", "worktree", "remove", str(worktree_path)], cwd=REPO_ROOT, timeout=120)})
        if branch in local_branches:
            delete_flag = "-d" if merged else "-D"
            applied.append({"command": f"git branch {delete_flag} {branch}", "result": _execute_checked(["git", "branch", delete_flag, branch], cwd=REPO_ROOT, timeout=120)})
        if remote_ref:
            applied.append({"command": f"git push origin --delete {branch}", "result": _execute_checked(["git", "push", "origin", "--delete", branch], cwd=REPO_ROOT, timeout=180)})
        payload["applied"] = applied
        payload["workflow_gate"] = "cleanup_done"
        payload["dry_run"] = False
        payload["duration_seconds"] = round(time.monotonic() - started, 3)
        _write_json(output_dir / f"{_slug(branch)}-cleanup-evidence.json", payload)
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
    _emit(payload, args.output)
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
    )
    _emit(payload, args.output)
    return 0 if payload.get("closure_ready") else 2


def cmd_triage_p0(args: argparse.Namespace) -> int:
    payload = build_triage_p0(include_fixed=args.include_fixed)
    _emit(payload, args.output)
    return 0


def cmd_run_p0(args: argparse.Namespace) -> int:
    payload = build_run_p0_plan(module=args.module, include_fixed=args.include_fixed)
    _emit(payload, args.output)
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
    _emit(payload, args.output)
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
    _emit(payload, args.output)
    return 0 if payload.get("closure_ready") else 2


def cmd_doctor(args: argparse.Namespace) -> int:
    payload = build_doctor_report(skip_external=args.skip_external)
    _emit(payload, args.output)
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
        registry_pr_only=args.registry_pr_only,
        dry_run=args.dry_run,
    )
    _emit(payload, args.output)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "submitted"} else 2


def cmd_install_client(args: argparse.Namespace) -> int:
    payload = build_client_install_plan(apply=args.apply, codex_home=args.codex_home)
    _emit(payload, args.output)
    return 0 if payload.get("workflow_gate") in {"ready_for_install", "installed"} else 2


def cmd_triage_ci_issue(args: argparse.Namespace) -> int:
    payload = build_triage_ci_issue_plan(
        issue_number=args.issue,
        run_id=args.run_id,
        summary_json=args.summary_json,
        skip_github_summary=args.skip_github_summary,
    )
    _emit(payload, args.output)
    return 0


def cmd_promote_ci_issue(args: argparse.Namespace) -> int:
    payload = build_promote_ci_issue_plan(
        issue_number=args.issue,
        apply=args.apply,
        bug_id=args.bug_id,
        summary_json=args.summary_json,
        skip_github_summary=args.skip_github_summary,
    )
    _emit(payload, args.output)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "promoted", "already_linked"} else 2


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
    _emit(payload, args.output)
    return 0 if payload.get("workflow_gate") not in {"validation_evidence_missing", "blocked"} else 2


def cmd_resume(args: argparse.Namespace) -> int:
    payload = build_resume_plan(bug_id=args.bug_id, worktree=args.worktree, events_limit=args.events_limit)
    _emit(payload, args.output)
    return 0


def cmd_postmortem(args: argparse.Namespace) -> int:
    payload = build_postmortem_plan(
        bug_id=args.bug_id,
        worktree=args.worktree,
        output_markdown=not args.no_markdown,
    )
    _emit(payload, args.output)
    return 0


def cmd_close_sync(args: argparse.Namespace) -> int:
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
    )
    _emit(payload, args.output)
    return 0 if payload.get("workflow_gate") in {"ready_for_apply", "close_synced"} else 2


def cmd_cleanup_after_merge(args: argparse.Namespace) -> int:
    payload = build_cleanup_after_merge_plan(
        branch=args.branch,
        worktree=args.worktree,
        pr_url=args.pr_url,
        apply=args.apply,
        sync_root=args.sync_root,
        canonical_root=args.canonical_root,
    )
    if payload.get("workflow_gate") == "cleanup_done" and args.bug_id:
        bug_id = args.bug_id.strip().upper()
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
        state = _write_state(
            bug_id,
            state="complete",
            root=REPO_ROOT,
            cleanup_evidence=cleanup_evidence,
            next_actions=[],
        )
        payload["complete_state"] = state
    _emit(payload, args.output)
    return 0 if payload.get("workflow_gate") in {"ready_for_cleanup", "cleanup_done"} else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AIstock high-level issue-fix workflow orchestrator.")
    sub = parser.add_subparsers(dest="command", required=True)

    doctor = sub.add_parser("doctor", help="Check repo, GitHub, MCP, and client-entry readiness.")
    doctor.add_argument("--skip-external", action="store_true", help="Skip gh/network-style checks for offline tests.")
    doctor.add_argument("--output")
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
    submit_bug.add_argument("--changed-file", action="append")
    submit_bug.add_argument("--plan-key")
    submit_bug.add_argument("--nox-session")
    submit_bug.add_argument("--candidate-type", default="bug", choices=["bug", "regression", "infra_failure", "flaky"])
    submit_bug.add_argument("--bug-id", help="Use an already reserved BUG-NNN id instead of the allocator.")
    submit_bug.add_argument("--github-issue-number")
    submit_bug.add_argument("--github-issue-url")
    submit_bug.add_argument("--create-github", action="store_true", help="Use gh to create the linked GitHub Issue when --apply is set.")
    submit_bug.add_argument("--apply", action="store_true", help="Write candidate/BUG JSON and update allocator after GitHub linkage exists.")
    submit_bug.add_argument("--create-registry-worktree", action="store_true", help="Create a clean registry worktree/branch from origin/main before writing BUG JSON.")
    submit_bug.add_argument("--registry-pr-only", action="store_true", help="Stop after a registry-only BUG PR; normal workflows continue directly to fix.")
    submit_bug.add_argument("--dry-run", action="store_true", help="Plan registry worktree creation without writing files or creating a worktree.")
    submit_bug.add_argument(
        "--allow-current-worktree",
        action="store_true",
        help="Override the registry guard for emergency/manual use. Normal agent workflows must not use this on canonical main.",
    )
    submit_bug.add_argument("--output")
    submit_bug.set_defaults(func=cmd_submit_bug)

    install_client = sub.add_parser("install-client", help="Install or dry-run developer-client entry wrappers.")
    install_client.add_argument("--apply", action="store_true")
    install_client.add_argument("--codex-home")
    install_client.add_argument("--output")
    install_client.set_defaults(func=cmd_install_client)

    triage_ci = sub.add_parser("triage-ci-issue", help="Summarize and classify an auto-filed CI/Nightly GitHub Issue.")
    triage_ci.add_argument("--issue", required=True, help="GitHub Issue number to triage.")
    triage_ci.add_argument("--run-id", help="Override or provide the Actions run id.")
    triage_ci.add_argument("--summary-json", help="Use an existing CI failure summary JSON instead of querying Actions.")
    triage_ci.add_argument("--skip-github-summary", action="store_true", help="Do not query Actions logs; emit a partial triage summary.")
    triage_ci.add_argument("--output")
    triage_ci.set_defaults(func=cmd_triage_ci_issue)

    promote_ci = sub.add_parser("promote-ci-issue", help="Promote a triaged CI GitHub Issue into the BUG JSON workflow.")
    promote_ci.add_argument("--issue", required=True, help="GitHub Issue number to promote.")
    promote_ci.add_argument("--bug-id", help="Use an already reserved BUG-NNN id.")
    promote_ci.add_argument("--summary-json", help="Use an existing CI failure summary JSON instead of querying Actions.")
    promote_ci.add_argument("--skip-github-summary", action="store_true", help="Do not query Actions logs; promote with partial diagnostics.")
    promote_ci.add_argument("--apply", action="store_true")
    promote_ci.add_argument("--output")
    promote_ci.set_defaults(func=cmd_promote_ci_issue)

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
    run.add_argument("--output")
    run.set_defaults(func=cmd_run)

    resume = sub.add_parser("resume", help="Resume a BUG workflow from state.json/events.jsonl.")
    resume.add_argument("--bug-id", required=True)
    resume.add_argument("--worktree")
    resume.add_argument("--events-limit", type=int, default=8)
    resume.add_argument("--output")
    resume.set_defaults(func=cmd_resume)

    postmortem = sub.add_parser("postmortem", help="Summarize workflow timing, context cost, active-worktree, and cleanup evidence.")
    postmortem.add_argument("--bug-id", required=True)
    postmortem.add_argument("--worktree")
    postmortem.add_argument("--no-markdown", action="store_true")
    postmortem.add_argument("--output")
    postmortem.set_defaults(func=cmd_postmortem)

    start = sub.add_parser("start", help="Prepare a BUG fix workflow and context pack.")
    start.add_argument("--bug-id")
    start.add_argument("--issue-json")
    start.add_argument("--changed-file", action="append")
    start.add_argument("--create-worktree", action="store_true")
    start.add_argument("--dry-run", action="store_true")
    start.add_argument("--task-slug")
    start.add_argument("--allow-missing-linkage", action="store_true")
    start.add_argument("--allow-closed", action="store_true")
    start.add_argument("--output")
    start.set_defaults(func=cmd_start)

    finish = sub.add_parser("finish", help="Select validation and generate a PR-ready finish plan.")
    finish.add_argument("--bug-id")
    finish.add_argument("--issue-json")
    finish.add_argument("--changed-file", action="append")
    finish.add_argument("--base", default="origin/main")
    finish.add_argument("--head", default="HEAD")
    finish.add_argument("--validation-evidence", action="append")
    finish.add_argument("--plan-only", action="store_true")
    finish.add_argument("--allow-missing-evidence", action="store_true")
    finish.add_argument("--output")
    finish.set_defaults(func=cmd_finish)

    triage = sub.add_parser("triage-p0", help="List and group open/in-progress P0 BUG records.")
    triage.add_argument("--include-fixed", action="store_true")
    triage.add_argument("--output")
    triage.set_defaults(func=cmd_triage_p0)

    run_p0 = sub.add_parser("run-p0", help="Plan current P0 handling and recommend the next issue command.")
    run_p0.add_argument("--module")
    run_p0.add_argument("--include-fixed", action="store_true")
    run_p0.add_argument("--output")
    run_p0.set_defaults(func=cmd_run_p0)

    start_batch = sub.add_parser("start-batch", help="Prepare a same-module batch BUG workflow and context packs.")
    start_batch.add_argument("--bug-id", action="append", required=True)
    start_batch.add_argument("--create-worktree", action="store_true")
    start_batch.add_argument("--dry-run", action="store_true")
    start_batch.add_argument("--task-slug")
    start_batch.add_argument("--allow-missing-linkage", action="store_true")
    start_batch.add_argument("--allow-closed", action="store_true")
    start_batch.add_argument("--output")
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
    finish_batch.add_argument("--output")
    finish_batch.set_defaults(func=cmd_finish_batch)

    close = sub.add_parser("close-sync", help="Prepare a dry-run close/sync plan after PR merge.")
    close.add_argument("--bug-id")
    close.add_argument("--issue-json")
    close.add_argument("--pr-url")
    close.add_argument("--apply", action="store_true")
    close.add_argument("--allow-missing-linkage", action="store_true")
    close.add_argument("--validation-evidence", action="append")
    close.add_argument("--merge-commit")
    close.add_argument("--production-ddl-gate", default="noop")
    close.add_argument("--production-frontend-dependency-gate", default="noop")
    close.add_argument("--production-backend-dependency-gate", default="noop")
    close.add_argument("--skip-github-check", action="store_true")
    close.add_argument("--output")
    close.set_defaults(func=cmd_close_sync)

    cleanup = sub.add_parser("cleanup-after-merge", help="Safely sync root and clean merged issue worktrees/branches.")
    cleanup.add_argument("--branch", required=True)
    cleanup.add_argument("--bug-id", help="Mark the BUG workflow complete after successful cleanup.")
    cleanup.add_argument("--worktree")
    cleanup.add_argument("--pr-url", help="Merged PR URL used to verify squash-merged branch cleanup.")
    cleanup.add_argument("--sync-root", action="store_true")
    cleanup.add_argument("--canonical-root")
    cleanup.add_argument("--apply", action="store_true")
    cleanup.add_argument("--output")
    cleanup.set_defaults(func=cmd_cleanup_after_merge)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except WorkflowError as exc:
        print(f"aistock_issue_workflow error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())





