from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
BUGS_ROOT = REPO_ROOT / "tests" / "aistock_validation" / "bugs"
WORKFLOW_ROOT = Path("tmp") / "issue_workflow"
ALLOWED_FIX_STATUSES = {"open", "in_progress"}

sys.path.insert(0, str(REPO_ROOT))
from scripts import issue_flow as flow  # noqa: E402


class WorkflowError(ValueError):
    """Raised when the high-level AIstock issue workflow cannot proceed safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _today_compact() -> str:
    return datetime.now().strftime("%Y%m%d")


def _slug(value: str, max_len: int = 72) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return (slug or "issue")[:max_len].strip("-") or "issue"


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise WorkflowError(f"JSON root must be an object: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _emit(payload: dict[str, Any], output: str | None = None) -> None:
    if output:
        _write_json(Path(output), payload)
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


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


def _bug_path_for_target(original_path: Path, target_root: Path) -> Path:
    try:
        relative = original_path.resolve().relative_to(REPO_ROOT.resolve())
        return target_root / relative
    except Exception:
        return original_path


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
    fix_ready_path = output_dir / "fix-ready.json"
    context_json_path = output_dir / "context-pack.json"
    context_md_path = output_dir / "context-pack.md"
    if not dry_run:
        _write_json(fix_ready_path, fix_ready)
        _write_json(context_json_path, context_pack)
        _write_text(context_md_path, flow.render_context_pack_markdown(context_pack))
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
        "allowed_write_scope": fix_ready.get("allowed_write_scope") or [],
        "required_verification": fix_ready.get("required_verification") or [],
        "recommended_verification": fix_ready.get("recommended_verification") or [],
        "production_gates": fix_ready.get("validation_selection", {}).get("production_gates", {}),
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
    })
    _write_text(pr_body_path, pr_body)
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
        "validation_evidence": evidence,
        "closure_ready": closure_ready,
        "workflow_gate": "ready_for_pr" if closure_ready else "validation_evidence_missing",
        "pr_body_path": _repo_rel(pr_body_path),
    }
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


def build_close_sync_plan(
    *,
    bug_id: str | None,
    issue_json: str | None,
    pr_url: str | None,
    apply: bool,
    allow_missing_linkage: bool,
) -> dict[str, Any]:
    record, source_path = find_bug_record(bug_id=bug_id, issue_json=issue_json)
    canonical_bug_id = str(record.get("bug_id") or bug_id or source_path.stem).upper()
    missing_linkage = _require_github_linkage(record, allow_missing=allow_missing_linkage)
    status = str(record.get("status") or "").strip()
    if status not in flow.VALID_BUG_STATUSES:
        raise WorkflowError(f"{canonical_bug_id} has invalid status for close/sync: {status!r}")
    if apply:
        raise WorkflowError("close-sync --apply is intentionally not implemented in the high-level wrapper; use MCP sync tools after verification")
    output_dir = REPO_ROOT / WORKFLOW_ROOT / canonical_bug_id
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
        "dry_run": True,
        "workflow_gate": "ready_for_mcp_sync" if pr_url else "missing_pr_url",
        "required_checks": [
            "closure_requirements_completed",
            "validation_evidence_attached",
            "BUG_JSON_and_GitHub_issue_status_aligned",
            "production_gates_reported",
        ],
        "next_agent_steps": [
            "verify_closure_requirements_item_by_item",
            "sync_bug_json_and_github_issue_with_mcp_tools",
            "record_final_production_gates",
        ],
    }
    _write_json(output_dir / "close-sync-plan.json", payload)
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


def cmd_close_sync(args: argparse.Namespace) -> int:
    payload = build_close_sync_plan(
        bug_id=args.bug_id,
        issue_json=args.issue_json,
        pr_url=args.pr_url,
        apply=args.apply,
        allow_missing_linkage=args.allow_missing_linkage,
    )
    _emit(payload, args.output)
    return 0 if payload.get("workflow_gate") == "ready_for_mcp_sync" else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AIstock high-level issue-fix workflow orchestrator.")
    sub = parser.add_subparsers(dest="command", required=True)

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

    close = sub.add_parser("close-sync", help="Prepare a dry-run close/sync plan after PR merge.")
    close.add_argument("--bug-id")
    close.add_argument("--issue-json")
    close.add_argument("--pr-url")
    close.add_argument("--apply", action="store_true")
    close.add_argument("--allow-missing-linkage", action="store_true")
    close.add_argument("--output")
    close.set_defaults(func=cmd_close_sync)

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
