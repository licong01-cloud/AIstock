from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import bug_github_sync as sync

SUPPORTED_ISSUE_ACTIONS = {"opened", "edited", "closed", "reopened", "labeled", "unlabeled"}


def load_event_payload(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise sync.BugGitHubSyncError("GitHub event payload must be a JSON object")
    return payload


def _issues_from_payload(payload: dict[str, Any], *, event_name: str | None = None) -> tuple[list[dict[str, Any]], str]:
    if isinstance(payload.get("issues"), list):
        return [sync.normalize_issue(issue) for issue in payload["issues"]], "issues_snapshot"

    issue = payload.get("issue")
    if isinstance(issue, dict):
        action = str(payload.get("action") or "")
        if event_name in {None, "", "issues"} and action and action not in SUPPORTED_ISSUE_ACTIONS:
            return [], f"ignored_issues_action:{action}"
        return [sync.normalize_issue(issue)], f"issues.{action or 'unknown'}"

    if payload.get("number") and payload.get("title"):
        return [sync.normalize_issue(payload)], "single_issue"

    return [], "ignored_no_issue_payload"


def plan_from_event(
    payload: dict[str, Any],
    *,
    bugs_dir: Path = sync.BUGS_DIR,
    event_name: str | None = None,
    p0_p1_only: bool = False,
) -> dict[str, Any]:
    issues, source = _issues_from_payload(payload, event_name=event_name)
    if not issues:
        return {
            "status": "ignored",
            "dry_run": True,
            "event_name": event_name,
            "source": source,
            "bugs_dir": str(bugs_dir),
            "summary": {},
            "plan": [],
        }

    bugs = sync.load_bug_files(bugs_dir)
    plan = sync.plan_issues_to_json(issues, bugs, bugs_dir=bugs_dir, p0_p1_only=p0_p1_only)
    return {
        "status": "planned",
        "dry_run": True,
        "event_name": event_name,
        "source": source,
        "bugs_dir": str(bugs_dir),
        "p0_p1_only": p0_p1_only,
        "summary": sync.summarize_plan(plan),
        "plan": plan,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Action-friendly GitHub Issues event importer for AIstock bugs JSON"
    )
    parser.add_argument(
        "--event-path",
        type=Path,
        default=Path(os.environ["GITHUB_EVENT_PATH"]) if os.environ.get("GITHUB_EVENT_PATH") else None,
        help="GitHub event JSON path; defaults to GITHUB_EVENT_PATH",
    )
    parser.add_argument("--event-name", default=os.environ.get("GITHUB_EVENT_NAME"), help="GitHub event name")
    parser.add_argument("--bugs-dir", type=Path, default=sync.BUGS_DIR, help="Directory containing AIstock bug JSON files")
    parser.add_argument("--p0-p1-only", action="store_true", help="Only import/update P0/P1 GitHub Issues")
    parser.add_argument("--apply", action="store_true", help="Write planned bugs JSON changes; dry-run is the default")
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.event_path is None:
        print("ERROR: --event-path or GITHUB_EVENT_PATH is required", file=sys.stderr)
        return 2

    try:
        payload = load_event_payload(args.event_path)
        result = plan_from_event(
            payload,
            bugs_dir=args.bugs_dir,
            event_name=args.event_name,
            p0_p1_only=args.p0_p1_only,
        )
        if args.apply and result["plan"]:
            result["results"] = sync.apply_issues_to_json_plan(result["plan"])
            result["status"] = "applied"
        result["dry_run"] = not args.apply
    except sync.BugGitHubSyncError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        summary = ", ".join(f"{key}={value}" for key, value in sorted(result["summary"].items())) or "no-op"
        mode = "dry-run" if result["dry_run"] else "apply"
        print(f"AIstock GitHub issue webhook importer {mode}: {summary}")
        if result["dry_run"]:
            print("No bugs JSON writes performed. Re-run with --apply to write planned updates.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
