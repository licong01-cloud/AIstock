from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import bug_github_sync as sync


FAILURE_EVENT_SCHEMA = "aistock_validation_failure_event_v1"
BUG_SCHEMA = "aistock_validation_bug_v1"
VALID_SEVERITIES = {"P0", "P1", "P2", "P3"}


def _utcnow_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_safe(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def _slug(value: str, *, max_len: int = 80) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-").lower()
    return slug[:max_len] or "validation-failure"


def _normalized_text(value: Any) -> str:
    return str(value or "").strip()


def _normalized_key(value: Any) -> str:
    return _normalized_text(value).lower()


def fingerprint_for_failure(event: dict[str, Any]) -> str:
    parts = (
        _normalized_key(event.get("module")),
        _normalized_key(event.get("title")),
        _normalized_key(event.get("reproduce_command")),
    )
    return hashlib.sha256("::".join(parts).encode("utf-8")).hexdigest()[:16]


def load_failure_event(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise sync.BugGitHubSyncError("failure event JSON must be an object")
    return normalize_failure_event(payload)


def normalize_failure_event(payload: dict[str, Any]) -> dict[str, Any]:
    schema_version = _normalized_text(payload.get("schema_version"))
    if schema_version != FAILURE_EVENT_SCHEMA:
        raise sync.BugGitHubSyncError(
            f"failure event schema_version must be {FAILURE_EVENT_SCHEMA!r}; got {schema_version!r}"
        )

    required = ("module", "title", "reproduce_command")
    missing = [field for field in required if not _normalized_text(payload.get(field))]
    if missing:
        raise sync.BugGitHubSyncError(f"failure event missing required fields: {', '.join(missing)}")

    severity = _normalized_text(payload.get("severity") or "P1").upper()
    if severity not in VALID_SEVERITIES:
        raise sync.BugGitHubSyncError(f"severity must be one of {sorted(VALID_SEVERITIES)}; got {severity!r}")

    files = payload.get("files") or []
    if isinstance(files, str):
        files = [files]
    if not isinstance(files, list) or not all(isinstance(item, str) for item in files):
        raise sync.BugGitHubSyncError("failure event files must be a list[str]")

    normalized = dict(payload)
    normalized["schema_version"] = FAILURE_EVENT_SCHEMA
    normalized["severity"] = severity
    normalized["module"] = _normalized_text(payload.get("module"))
    normalized["title"] = _normalized_text(payload.get("title"))
    normalized["reproduce_command"] = _normalized_text(payload.get("reproduce_command"))
    normalized["source"] = _normalized_text(payload.get("source") or "validation_failure_event")
    normalized["files"] = [item.strip() for item in files if item.strip()]
    return normalized


def load_existing_bugs(bugs_dir: Path) -> list[dict[str, Any]]:
    if not bugs_dir.exists():
        return []
    return sync.load_bug_files(bugs_dir)


def _scan_existing_bug_numbers(bugs: list[dict[str, Any]]) -> set[int]:
    numbers: set[int] = set()
    for bug in bugs:
        match = re.search(r"\bBUG-(\d+)\b", str(bug.get("bug_id") or ""), flags=re.I)
        if match:
            numbers.add(int(match.group(1)))
    return numbers


def next_bug_id(existing_bugs: list[dict[str, Any]]) -> str:
    numbers = _scan_existing_bug_numbers(existing_bugs)
    return f"BUG-{(max(numbers) + 1) if numbers else 1:03d}"


def _source_path_for_bug(bugs_dir: Path, bug: dict[str, Any], *, now_iso: str) -> Path:
    date_prefix = now_iso[:10].replace("-", "")
    return bugs_dir / f"{date_prefix}_{bug['bug_id']}-{_slug(str(bug.get('title') or 'validation-failure'))}.json"


def _failure_dedupe_key(event: dict[str, Any]) -> str:
    return _normalized_text(event.get("dedupe_key"))


def find_existing_failure_bug(
    event: dict[str, Any],
    existing_bugs: list[dict[str, Any]],
) -> dict[str, Any] | None:
    fingerprint = fingerprint_for_failure(event)
    dedupe_key = _failure_dedupe_key(event)
    module = _normalized_key(event.get("module"))
    title = _normalized_key(event.get("title"))
    reproduce = _normalized_key(event.get("reproduce_command"))

    for bug in existing_bugs:
        if bug.get("fingerprint") == fingerprint:
            return bug
        if (
            _normalized_key(bug.get("module")) == module
            and _normalized_key(bug.get("title")) == title
            and _normalized_key(bug.get("reproduce_command")) == reproduce
        ):
            return bug
        validation_failure = bug.get("validation_failure")
        if isinstance(validation_failure, dict) and dedupe_key and validation_failure.get("dedupe_key") == dedupe_key:
            return bug
    return None


def _description_for_event(event: dict[str, Any]) -> str:
    lines = [
        f"Validation failure from {event.get('source')} event {event.get('event_id') or '<unknown>'}.",
        "",
        f"Expected: {_normalized_text(event.get('expected'))}" if _normalized_text(event.get("expected")) else "",
        f"Actual: {_normalized_text(event.get('actual'))}" if _normalized_text(event.get("actual")) else "",
    ]
    logs_excerpt = _normalized_text(event.get("logs_excerpt"))
    if logs_excerpt:
        lines.extend(["", "Logs excerpt:", logs_excerpt])
    description = "\n".join(line for line in lines if line != "").strip()
    return description or str(event["title"])


def _evidence_uris_for_event(event: dict[str, Any]) -> list[str]:
    evidence: list[str] = []
    event_id = _normalized_text(event.get("event_id"))
    if event_id:
        evidence.append(f"validation_failure_event:{event_id}")
    run_url = _normalized_text(event.get("run_url"))
    if run_url:
        evidence.append(run_url)
    commit = _normalized_text(event.get("commit"))
    if commit:
        evidence.append(f"commit:{commit}")
    for path in event.get("files") or []:
        evidence.append(f"file:{path}")
    return list(dict.fromkeys(evidence))


def _validation_failure_metadata(event: dict[str, Any], *, now_iso: str, previous: dict[str, Any] | None = None) -> dict[str, Any]:
    failure_count = 0
    first_seen_commit = _normalized_text(event.get("commit")) or None
    if previous:
        try:
            failure_count = int(previous.get("failure_count") or 0)
        except (TypeError, ValueError):
            failure_count = 0
        first_seen_commit = previous.get("first_seen_commit") or first_seen_commit

    return {
        "event_id": _normalized_text(event.get("event_id")) or None,
        "source": _normalized_text(event.get("source")) or None,
        "plan_key": _normalized_text(event.get("plan_key")) or None,
        "run_url": _normalized_text(event.get("run_url")) or None,
        "dedupe_key": _failure_dedupe_key(event) or fingerprint_for_failure(event),
        "first_seen_commit": first_seen_commit,
        "last_seen_commit": _normalized_text(event.get("commit")) or None,
        "last_seen_at": now_iso,
        "failure_count": failure_count + 1,
    }


def _github_unavailable_reason(repo: str | None) -> str:
    if not repo:
        return "github_repo_unavailable"
    if not (os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")):
        return "github_token_unavailable"
    return "github_live_write_disabled_by_validation_failure_event_to_bug"


def _github_sync_placeholder(
    *,
    now_iso: str,
    bugs_dir: Path,
    repo: str | None,
    reason: str,
) -> dict[str, Any]:
    command = f"python scripts/bug_github_sync.py --bugs-dir {bugs_dir.as_posix()} --json"
    if repo:
        command += f" --repo {repo}"
    return {
        "status": "pending",
        "reason": reason,
        "dry_run": True,
        "last_planned_at": now_iso,
        "tool": "scripts/bug_github_sync.py",
        "recommended_command": command,
    }


def build_bug_record(
    event: dict[str, Any],
    *,
    bug_id: str,
    bugs_dir: Path,
    now_iso: str,
    repo: str | None = None,
) -> dict[str, Any]:
    files = list(event.get("files") or [])
    reproduce_command = str(event["reproduce_command"])
    record: dict[str, Any] = {
        "schema_version": BUG_SCHEMA,
        "bug_id": bug_id,
        "title": str(event["title"]),
        "description": _description_for_event(event),
        "module": str(event["module"]),
        "severity": str(event["severity"]).upper(),
        "risk_area": "validation_failure",
        "status": "open",
        "trigger_condition": {
            "source": event.get("source"),
            "event_id": event.get("event_id"),
            "plan_key": event.get("plan_key"),
            "branch": event.get("branch"),
            "commit": event.get("commit"),
            "run_url": event.get("run_url"),
            "dedupe_key": event.get("dedupe_key"),
        },
        "reproduce_command": reproduce_command,
        "failing_run_id": event.get("event_id"),
        "evidence_uris": _evidence_uris_for_event(event),
        "fingerprint": fingerprint_for_failure(event),
        "assigned_agent": None,
        "fix_branch": None,
        "fix_commit": None,
        "verification_run_id": None,
        "created_at": now_iso,
        "first_seen_at": now_iso,
        "last_seen_at": now_iso,
        "fixed_at": None,
        "submitted_at": now_iso,
        "closed_at": None,
        "allowed_write_scope": files,
        "suspected_modules": files or [str(event["module"])],
        "required_verification": [reproduce_command],
        "closure_requirements": [
            "Re-run the reproduce_command and record the result before marking fixed.",
            "Run scripts/bug_github_sync.py dry-run before creating or updating a GitHub Issue mirror.",
            "Do not touch production 8001/3000 or production DB state without explicit user approval.",
        ],
        "non_goals": [
            "Do not create a live GitHub Issue from this script; use bug_github_sync.py with explicit approval.",
            "Do not modify unrelated dirty workspace files.",
        ],
        "validation_failure": _validation_failure_metadata(event, now_iso=now_iso),
        "github_sync": _github_sync_placeholder(
            now_iso=now_iso,
            bugs_dir=bugs_dir,
            repo=repo,
            reason=_github_unavailable_reason(repo),
        ),
        "events": [
            {
                "timestamp": now_iso,
                "actor": "validation_failure_event_to_bug.py",
                "action": "validation_failure_registered",
                "note": f"Registered from {event.get('source')} failure event {event.get('event_id') or '<unknown>'}.",
            }
        ],
    }
    if not files:
        record["workflow_gate"] = "triage_only_until_allowed_write_scope_is_set"
    return record


def _append_unique(existing: list[Any], additions: list[Any]) -> list[Any]:
    values = list(existing)
    for item in additions:
        if item not in values:
            values.append(item)
    return values


def build_updated_bug_record(existing: dict[str, Any], event: dict[str, Any], *, bugs_dir: Path, now_iso: str, repo: str | None = None) -> dict[str, Any]:
    record = copy.deepcopy({key: value for key, value in existing.items() if key != "_source_path"})
    previous_failure = record.get("validation_failure") if isinstance(record.get("validation_failure"), dict) else None
    record["fingerprint"] = record.get("fingerprint") or fingerprint_for_failure(event)
    record["last_seen_at"] = now_iso
    record["failing_run_id"] = event.get("event_id") or record.get("failing_run_id")
    record["evidence_uris"] = _append_unique(record.get("evidence_uris") or [], _evidence_uris_for_event(event))
    record["allowed_write_scope"] = _append_unique(record.get("allowed_write_scope") or [], list(event.get("files") or []))
    record["suspected_modules"] = _append_unique(record.get("suspected_modules") or [], list(event.get("files") or []) or [str(event["module"])])
    record["validation_failure"] = _validation_failure_metadata(event, now_iso=now_iso, previous=previous_failure)
    record["github_sync"] = _github_sync_placeholder(
        now_iso=now_iso,
        bugs_dir=bugs_dir,
        repo=repo,
        reason=_github_unavailable_reason(repo),
    )
    events = record.setdefault("events", [])
    if isinstance(events, list):
        events.append(
            {
                "timestamp": now_iso,
                "actor": "validation_failure_event_to_bug.py",
                "action": "validation_failure_seen_again",
                "note": f"Observed repeat failure event {event.get('event_id') or '<unknown>'}; failure_count={record['validation_failure']['failure_count']}.",
            }
        )
    return record


def _record_with_source(record: dict[str, Any], path: Path) -> dict[str, Any]:
    copy_record = copy.deepcopy(record)
    copy_record["_source_path"] = str(path)
    return copy_record


def _attach_github_dry_run(
    record: dict[str, Any],
    *,
    source_path: Path,
    issues_snapshot: Path | None,
) -> list[dict[str, Any]]:
    existing_issues = sync.load_issues_snapshot(issues_snapshot)
    plan = sync.plan_json_to_issues([_record_with_source(record, source_path)], existing_issues)
    record.setdefault("github_sync", {})
    if isinstance(record["github_sync"], dict):
        record["github_sync"]["dry_run_summary"] = sync.summarize_plan(plan)
        record["github_sync"]["planned_actions"] = [item.get("action") for item in plan]
    plan = sync.plan_json_to_issues([_record_with_source(record, source_path)], existing_issues)
    return plan


def plan_failure_event_to_bug(
    event: dict[str, Any],
    *,
    bugs_dir: Path = sync.BUGS_DIR,
    issues_snapshot: Path | None = None,
    repo: str | None = None,
    now_iso: str | None = None,
) -> dict[str, Any]:
    now = now_iso or _utcnow_iso()
    existing_bugs = load_existing_bugs(bugs_dir)
    existing = find_existing_failure_bug(event, existing_bugs)

    if existing is None:
        record = build_bug_record(event, bug_id=next_bug_id(existing_bugs), bugs_dir=bugs_dir, now_iso=now, repo=repo)
        path = _source_path_for_bug(bugs_dir, record, now_iso=now)
        action = "create_json"
    else:
        record = build_updated_bug_record(existing, event, bugs_dir=bugs_dir, now_iso=now, repo=repo)
        path = Path(str(existing.get("_source_path")))
        action = "update_json"

    github_plan = _attach_github_dry_run(record, source_path=path, issues_snapshot=issues_snapshot)
    plan_item = {
        "action": action,
        "bug_id": record["bug_id"],
        "path": str(path),
        "fingerprint": record["fingerprint"],
        "deduplicated": existing is not None,
        "desired": record,
        "github_sync_plan": github_plan,
    }
    return {
        "status": "planned",
        "dry_run": True,
        "schema_version": FAILURE_EVENT_SCHEMA,
        "event_id": event.get("event_id"),
        "bugs_dir": str(bugs_dir),
        "summary": sync.summarize_plan([plan_item]),
        "github_sync_summary": sync.summarize_plan(github_plan),
        "plan": [plan_item],
        "github_sync_plan": github_plan,
    }


def apply_failure_event_plan(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in plan:
        action = item["action"]
        path = Path(str(item["path"]))
        desired = {key: value for key, value in item["desired"].items() if key != "_source_path"}
        if action == "create_json":
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                raise sync.BugGitHubSyncError(f"refusing to overwrite existing bug JSON: {path}")
            path.write_text(_json_safe(desired) + "\n", encoding="utf-8")
            results.append({"action": "created_json", "bug_id": item["bug_id"], "path": str(path)})
            continue
        if action == "update_json":
            if not path.exists():
                raise sync.BugGitHubSyncError(f"cannot update missing bug JSON: {path}")
            path.write_text(_json_safe(desired) + "\n", encoding="utf-8")
            results.append({"action": "updated_json", "bug_id": item["bug_id"], "path": str(path)})
            continue
        raise sync.BugGitHubSyncError(f"unknown failure-event plan action: {action}")
    return results


def run(
    *,
    event_path: Path,
    bugs_dir: Path = sync.BUGS_DIR,
    issues_snapshot: Path | None = None,
    repo: str | None = None,
    apply: bool = False,
) -> dict[str, Any]:
    event = load_failure_event(event_path)
    result = plan_failure_event_to_bug(
        event,
        bugs_dir=bugs_dir,
        issues_snapshot=issues_snapshot,
        repo=repo,
    )
    if apply:
        result["results"] = apply_failure_event_plan(result["plan"])
        result["status"] = "applied"
        result["dry_run"] = False
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert an AIstock validation failure event JSON into a local bugs registry entry"
    )
    parser.add_argument(
        "--event-path",
        type=Path,
        default=Path(os.environ["AISTOCK_VALIDATION_FAILURE_EVENT_PATH"])
        if os.environ.get("AISTOCK_VALIDATION_FAILURE_EVENT_PATH")
        else None,
        help="Failure event JSON path; defaults to AISTOCK_VALIDATION_FAILURE_EVENT_PATH",
    )
    parser.add_argument("--bugs-dir", type=Path, default=sync.BUGS_DIR, help="Directory containing AIstock bug JSON files")
    parser.add_argument("--issues-snapshot", type=Path, help="Offline JSON snapshot of existing GitHub issues for idempotency planning")
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"), help="GitHub repo in owner/name form for dry-run metadata only")
    parser.add_argument("--apply", action="store_true", help="Write local bugs JSON changes; GitHub writes remain dry-run only")
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.event_path is None:
        print("ERROR: --event-path or AISTOCK_VALIDATION_FAILURE_EVENT_PATH is required", file=sys.stderr)
        return 2

    try:
        result = run(
            event_path=args.event_path,
            bugs_dir=args.bugs_dir,
            issues_snapshot=args.issues_snapshot,
            repo=args.repo,
            apply=args.apply,
        )
    except sync.BugGitHubSyncError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        summary = ", ".join(f"{key}={value}" for key, value in sorted(result["summary"].items())) or "no-op"
        github_summary = ", ".join(
            f"{key}={value}" for key, value in sorted(result["github_sync_summary"].items())
        ) or "no-op"
        mode = "dry-run" if result["dry_run"] else "apply"
        print(f"AIstock validation failure event to bug {mode}: {summary}; github_sync_dry_run={github_summary}")
        if result["dry_run"]:
            print("No bugs JSON writes performed. Re-run with --apply to write local registry changes.")
        print("No GitHub writes performed by this script; use scripts/bug_github_sync.py after review.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
