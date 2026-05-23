from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib import error, request

BUGS_DIR = Path("tests/aistock_validation/bugs")
BUG_LABEL = "aistock:bug"
HISTORICAL_IMPORT_LABEL = "import:historical"
BUG_ID_RE = re.compile(r"\bBUG(?:-[A-Z0-9]+)*-\d+\b", re.I)
SYNC_MARKER_RE = re.compile(r"<!--\s*aistock[-_\s]?bug[-_\s]?id\s*[:=]\s*([^>]+?)\s*-->", re.I | re.S)
TITLE_PREFIX_RE = re.compile(r"^\[([^\]]+)\]")
BODY_BUG_FIELD_RE = re.compile(
    r'(?im)^\s*(?:"?bug_id"?|bug\s*id|aistock\s*bug\s*id)\s*[:=]\s*"?([A-Za-z0-9_.:-]+)"?\s*,?\s*$'
)
P0_P1 = {"P0", "P1"}
CLOSED_BUG_STATUSES = {"fixed", "closed", "resolved", "verified"}
OPEN_BUG_STATUSES = {"open", "reopened", "triaged", "in_progress", "in-progress", "blocked"}
LOCAL_ENV_FILE = ".env.github-issues-local"


class BugGitHubSyncError(RuntimeError):
    pass


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    result: list[Path] = []
    for path in paths:
        try:
            resolved = path.resolve()
        except OSError:
            resolved = path
        key = str(resolved).lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(resolved)
    return result


def _git_toplevel(path: Path) -> Path | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "--show-toplevel"],
            capture_output=True,
            check=False,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    value = completed.stdout.strip()
    return Path(value).resolve() if value else None


def _candidate_repo_roots() -> list[Path]:
    roots: list[Path] = []
    explicit = os.environ.get("AISTOCK_REPO_ROOT")
    if explicit:
        roots.append(Path(explicit))
    roots.extend([Path.cwd(), Path(__file__).resolve().parents[1]])
    for root in list(roots):
        top = _git_toplevel(root)
        if top is not None:
            roots.append(top)
    return _dedupe_paths(roots)


def _load_local_github_env() -> None:
    """Load local GitHub sync defaults without overriding explicit process env."""
    if os.environ.get("AISTOCK_GITHUB_SKIP_ENV_FILE"):
        return

    env_paths: list[Path] = []
    explicit_file = os.environ.get("AISTOCK_GITHUB_ENV_FILE")
    if explicit_file:
        env_paths.append(Path(explicit_file))
    env_paths.extend(root / LOCAL_ENV_FILE for root in _candidate_repo_roots())

    for path in _dedupe_paths(env_paths):
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def _github_repo_from_remote_url(url: str) -> str | None:
    raw = url.strip()
    if not raw:
        return None
    patterns = (
        r"^git@github\.com:([^/]+)/(.+?)(?:\.git)?$",
        r"^ssh://git@github\.com/([^/]+)/(.+?)(?:\.git)?$",
        r"^https://github\.com/([^/]+)/(.+?)(?:\.git)?/?$",
        r"^http://github\.com/([^/]+)/(.+?)(?:\.git)?/?$",
    )
    for pattern in patterns:
        match = re.match(pattern, raw, flags=re.I)
        if match:
            owner, name = match.group(1), match.group(2)
            name = name[:-4] if name.endswith(".git") else name
            if owner and name and "/" not in name:
                return f"{owner}/{name}"
    return None


def _github_repo_from_git_remote(root: Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), "remote", "get-url", "origin"],
            capture_output=True,
            check=False,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return _github_repo_from_remote_url(completed.stdout.strip())


def _github_repo_default() -> str | None:
    _load_local_github_env()
    repo = os.environ.get("GITHUB_REPOSITORY")
    if repo:
        return repo
    for root in _candidate_repo_roots():
        repo = _github_repo_from_git_remote(root)
        if repo:
            os.environ.setdefault("GITHUB_REPOSITORY", repo)
            return repo
    return None


def _github_token_from_gh_cli() -> str | None:
    if os.environ.get("AISTOCK_GITHUB_DISABLE_GH_CLI_TOKEN"):
        return None
    try:
        completed = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True,
            check=False,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    token = completed.stdout.strip()
    return token if completed.returncode == 0 and token else None


def _github_token_default(*, remote_needed: bool) -> str | None:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token or not remote_needed:
        return token
    return _github_token_from_gh_cli()


@dataclass(frozen=True)
class SyncConfig:
    bugs_dir: Path = BUGS_DIR
    repo: str | None = None
    token: str | None = None
    apply: bool = False
    historical_import: bool = False
    p0_p1_only: bool = False
    issues_snapshot: Path | None = None
    direction: str = "json-to-issues"


def _json_safe(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2)


def _label_value(prefix: str, value: Any) -> str | None:
    if value in (None, ""):
        return None
    raw = str(value).strip().lower()
    slug = re.sub(r"[^a-z0-9_.-]+", "-", raw).strip("-")
    return f"{prefix}:{slug}" if slug else None


def normalize_bug_id(value: Any) -> str | None:
    match = BUG_ID_RE.search(str(value or "").strip())
    return match.group(0).upper() if match else None


def load_bug_files(bugs_dir: Path = BUGS_DIR) -> list[dict[str, Any]]:
    if not bugs_dir.exists():
        raise BugGitHubSyncError(f"bugs directory not found: {bugs_dir}")

    bugs: list[dict[str, Any]] = []
    for path in sorted(bugs_dir.glob("*.json")):
        try:
            with path.open("r", encoding="utf-8-sig") as handle:
                bug = json.load(handle)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            logging.warning("Skipping unreadable bug JSON file %s: %s", path, exc)
            continue
        if not isinstance(bug, dict) or not bug.get("bug_id"):
            logging.warning("Skipping bug JSON file missing bug_id: %s", path)
            continue
        bug["_source_path"] = str(path)
        bugs.append(bug)
    return bugs


def should_sync_bug(bug: dict[str, Any], *, p0_p1_only: bool) -> bool:
    if not p0_p1_only:
        return True
    return str(bug.get("severity", "")).upper() in P0_P1


def issue_title_for_bug(bug: dict[str, Any]) -> str:
    title = str(bug.get("title") or "Untitled AIstock bug").strip()
    bug_id = str(bug["bug_id"]).strip()
    return title if title.startswith(f"[{bug_id}]") else f"[{bug_id}] {title}"


def issue_labels_for_bug(bug: dict[str, Any], *, historical_import: bool = False) -> list[str]:
    labels = [BUG_LABEL]
    for prefix, key in (("severity", "severity"), ("module", "module"), ("status", "status"), ("risk", "risk_area")):
        label = _label_value(prefix, bug.get(key))
        if label:
            labels.append(label)
    severity = str(bug.get("severity") or "").upper()
    if severity in P0_P1:
        labels.append(severity)
    if historical_import:
        labels.append(HISTORICAL_IMPORT_LABEL)
    return sorted(dict.fromkeys(labels))


def _issue_body_validation_failure_metadata(bug: dict[str, Any]) -> dict[str, Any] | None:
    value = bug.get("validation_failure")
    if not isinstance(value, dict):
        return None
    stable_keys = (
        "event_id",
        "source",
        "plan_key",
        "run_url",
        "dedupe_key",
        "first_seen_commit",
        "last_seen_commit",
        "failure_count",
    )
    metadata = {key: value.get(key) for key in stable_keys if value.get(key) not in (None, "", [])}
    return metadata or None


def _issue_body_github_sync_metadata(bug: dict[str, Any]) -> dict[str, Any] | None:
    value = bug.get("github_sync")
    if not isinstance(value, dict):
        return None
    stable_keys = ("status", "reason", "dry_run", "tool", "recommended_command")
    metadata = {key: value.get(key) for key in stable_keys if value.get(key) not in (None, "", [])}
    return metadata or None


def issue_body_for_bug(bug: dict[str, Any]) -> str:
    compact_fields = {
        "bug_id": bug.get("bug_id"),
        "severity": bug.get("severity"),
        "module": bug.get("module"),
        "risk_area": bug.get("risk_area"),
        "status": bug.get("status"),
        "fingerprint": bug.get("fingerprint"),
        "reproduce_command": bug.get("reproduce_command"),
        "validation_failure": _issue_body_validation_failure_metadata(bug),
        "github_sync": _issue_body_github_sync_metadata(bug),
        "source_path": bug.get("_source_path"),
    }
    description = str(bug.get("description") or "").strip()
    evidence = bug.get("evidence_uris") or []
    required = bug.get("required_verification") or []
    closure = bug.get("closure_requirements") or []

    sections = [
        f"<!-- aistock-bug-id: {bug['bug_id']} -->",
        "<!-- managed-by: scripts/bug_github_sync.py -->",
        "",
        "## Summary",
        description or "No description provided in bugs JSON.",
        "",
        "## Source of Truth",
        "The versioned bugs JSON entry remains the source of truth; this GitHub Issue is a workflow/UI mirror.",
        "",
        "## Bug Metadata",
        "```json",
        _json_safe({key: value for key, value in compact_fields.items() if value not in (None, "", [])}),
        "```",
    ]

    if evidence:
        sections.extend(["", "## Evidence", *[f"- {item}" for item in evidence]])
    if required:
        sections.extend(["", "## Required Verification", *[f"- {item}" for item in required]])
    if closure:
        sections.extend(["", "## Closure Requirements", *[f"- {item}" for item in closure]])

    return "\n".join(sections).rstrip() + "\n"


def normalize_issue(raw: dict[str, Any]) -> dict[str, Any]:
    labels: list[str] = []
    for label in raw.get("labels", []) or []:
        if isinstance(label, dict):
            name = label.get("name")
        else:
            name = label
        if name:
            labels.append(str(name))
    return {
        "number": raw.get("number"),
        "title": raw.get("title") or "",
        "body": raw.get("body") or "",
        "state": raw.get("state") or "open",
        "labels": sorted(dict.fromkeys(labels)),
        "html_url": raw.get("html_url") or raw.get("url"),
    }


def issue_marker_bug_ids(issue: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    body = str(issue.get("body") or "")
    for match in SYNC_MARKER_RE.finditer(body):
        bug_id = normalize_bug_id(match.group(1))
        if bug_id:
            ids.append(bug_id)
    return ids


def issue_has_conflicting_markers(issue: dict[str, Any]) -> bool:
    return len(dict.fromkeys(issue_marker_bug_ids(issue))) > 1


def issue_bug_id(issue: dict[str, Any]) -> str | None:
    marker_ids = list(dict.fromkeys(issue_marker_bug_ids(issue)))
    if marker_ids:
        return marker_ids[0]
    title_match = TITLE_PREFIX_RE.search(str(issue.get("title") or ""))
    if title_match:
        bug_id = normalize_bug_id(title_match.group(1))
        if bug_id:
            return bug_id
    title_bug = normalize_bug_id(str(issue.get("title") or "")[:120])
    if title_bug:
        return title_bug
    for field_match in BODY_BUG_FIELD_RE.finditer(str(issue.get("body") or "")):
        bug_id = normalize_bug_id(field_match.group(1))
        if bug_id:
            return bug_id
    return None


def issue_status(issue: dict[str, Any]) -> str:
    status_label: str | None = None
    for label in issue.get("labels", []) or []:
        raw = str(label).lower()
        if raw.startswith("status:"):
            status_label = raw.split(":", 1)[1].strip()
            break

    state = str(issue.get("state") or "open").lower()
    if state == "closed":
        if status_label in CLOSED_BUG_STATUSES:
            return status_label
        return "closed"
    if status_label in OPEN_BUG_STATUSES:
        return status_label
    return "open"


def issue_severity(issue: dict[str, Any]) -> str:
    for label in issue.get("labels", []) or []:
        raw = str(label).lower()
        if raw.startswith("severity:"):
            severity = raw.split(":", 1)[1].upper()
            if severity in {"P0", "P1", "P2", "P3"}:
                return severity
        if raw.upper() in {"P0", "P1", "P2", "P3"}:
            return raw.upper()

    title = str(issue.get("title") or "")
    match = re.search(r"\[(P[0-3])\]", title, flags=re.I)
    if match:
        return match.group(1).upper()

    body = str(issue.get("body") or "")
    match = re.search(r"\bSeverity:\s*(P[0-3])\b", body, flags=re.I)
    return match.group(1).upper() if match else "P2"


def issue_module(issue: dict[str, Any]) -> str:
    for label in issue.get("labels", []) or []:
        raw = str(label).lower()
        if raw.startswith("module:"):
            module = raw.split(":", 1)[1].strip()
            if module:
                return module
    body = str(issue.get("body") or "")
    match = re.search(r"\bModule:\s*([A-Za-z0-9_.-]+)\b", body, flags=re.I)
    return match.group(1) if match else "github_issues"


def _slug(value: str, *, max_len: int = 80) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-").lower()
    return slug[:max_len] or "github-issue"


def bug_json_from_issue(issue: dict[str, Any], *, bug_id: str | None = None) -> dict[str, Any]:
    normalized = normalize_issue(issue)
    number = normalized.get("number")
    resolved_bug_id = bug_id or issue_bug_id(normalized) or f"BUG-GH-{number}"
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    title = str(normalized.get("title") or "").strip()
    title = TITLE_PREFIX_RE.sub("", title).strip() or f"GitHub issue #{number}"
    status = issue_status(normalized)
    return {
        "schema_version": "aistock_validation_bug_v1",
        "bug_id": resolved_bug_id,
        "title": title,
        "description": (normalized.get("body") or "").strip() or f"Imported from GitHub issue #{number}.",
        "module": issue_module(normalized),
        "severity": issue_severity(normalized),
        "risk_area": "github_issue_import",
        "status": status,
        "trigger_condition": {"source": "github_issues_sync"},
        "reproduce_command": None,
        "evidence_uris": [normalized.get("html_url") or f"github_issue:{number}"],
        "fingerprint": f"github_issue::{number or resolved_bug_id}",
        "assigned_agent": "github_issues_sync",
        "created_at": now,
        "first_seen_at": now,
        "last_seen_at": now,
        "closed_at": now if status in CLOSED_BUG_STATUSES else None,
        "github_issue_number": number,
        "github_issue_url": normalized.get("html_url"),
        "events": [
            {
                "timestamp": now,
                "actor": "bug_github_sync.py",
                "action": "imported_from_github_issue",
                "note": "GitHub Issues is a workflow layer; bugs JSON remains the versioned source of truth after import.",
            }
        ],
    }


def _source_path_for_import(bugs_dir: Path, bug: dict[str, Any]) -> Path:
    date_prefix = datetime.now(UTC).strftime("%Y%m%d")
    return bugs_dir / f"{date_prefix}_{bug['bug_id']}-{_slug(str(bug.get('title') or 'github-issue'))}.json"


def plan_issues_to_json(
    issues: list[dict[str, Any]],
    existing_bugs: list[dict[str, Any]],
    *,
    bugs_dir: Path = BUGS_DIR,
    p0_p1_only: bool = False,
) -> list[dict[str, Any]]:
    bugs_by_id = {str(bug["bug_id"]): bug for bug in existing_bugs}
    plan: list[dict[str, Any]] = []

    for raw_issue in issues:
        issue = normalize_issue(raw_issue)
        marker_ids = list(dict.fromkeys(issue_marker_bug_ids(issue)))
        if len(marker_ids) > 1:
            plan.append({
                "issue_number": issue.get("number"),
                "action": "skip",
                "reason": "conflicting_issue_markers",
                "bug_ids": marker_ids,
            })
            continue
        severity = issue_severity(issue)
        if p0_p1_only and severity not in P0_P1:
            plan.append({
                "issue_number": issue.get("number"),
                "action": "skip",
                "reason": "severity_filter_p0_p1_only",
                "severity": severity,
            })
            continue

        bug_id = marker_ids[0] if marker_ids else issue_bug_id(issue) or f"BUG-GH-{issue.get('number')}"
        existing = bugs_by_id.get(bug_id)
        desired = bug_json_from_issue(issue, bug_id=bug_id)
        if existing is None:
            plan.append({
                "bug_id": bug_id,
                "issue_number": issue.get("number"),
                "action": "create_json",
                "path": str(_source_path_for_import(bugs_dir, desired)),
                "desired": desired,
            })
            continue

        changes: dict[str, Any] = {}
        desired_status = desired["status"]
        if existing.get("status") != desired_status:
            changes["status"] = desired_status
        if desired.get("github_issue_number") and existing.get("github_issue_number") != desired["github_issue_number"]:
            changes["github_issue_number"] = desired["github_issue_number"]
        if desired.get("github_issue_url") and existing.get("github_issue_url") != desired["github_issue_url"]:
            changes["github_issue_url"] = desired["github_issue_url"]

        plan.append({
            "bug_id": bug_id,
            "issue_number": issue.get("number"),
            "action": "update_json" if changes else "noop",
            "path": existing.get("_source_path"),
            "desired": desired,
            "changes": changes,
        })
    return plan


def apply_issues_to_json_plan(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in plan:
        action = item["action"]
        if action in {"skip", "noop"}:
            results.append({"bug_id": item.get("bug_id"), "issue_number": item.get("issue_number"), "action": action})
            continue
        path = Path(str(item["path"]))
        if action == "create_json":
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                raise BugGitHubSyncError(f"refusing to overwrite existing bug JSON: {path}")
            path.write_text(_json_safe(item["desired"]) + "\n", encoding="utf-8")
            results.append({"bug_id": item["bug_id"], "issue_number": item.get("issue_number"), "action": "created_json", "path": str(path)})
            continue
        if action == "update_json":
            if not path.exists():
                raise BugGitHubSyncError(f"cannot update missing bug JSON: {path}")
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            previous_status = payload.get("status")
            payload.update(item["changes"])
            now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            if "status" in item["changes"]:
                status = str(payload.get("status") or "").lower()
                payload["closed_at"] = now if status in CLOSED_BUG_STATUSES else None
                events = payload.setdefault("events", [])
                if isinstance(events, list):
                    events.append({
                        "timestamp": now,
                        "actor": "bug_github_sync.py",
                        "action": "status_synced_from_github_issue",
                        "note": f"GitHub issue #{item.get('issue_number')} status changed {previous_status!r} -> {payload.get('status')!r}.",
                    })
            payload["last_seen_at"] = now
            path.write_text(_json_safe(payload) + "\n", encoding="utf-8")
            results.append({"bug_id": item["bug_id"], "issue_number": item.get("issue_number"), "action": "updated_json", "path": str(path)})
            continue
        raise BugGitHubSyncError(f"unknown issues-to-json action: {action}")
    return results


def load_issues_snapshot(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if isinstance(payload, dict):
        payload = payload.get("issues", [])
    if not isinstance(payload, list):
        raise BugGitHubSyncError("issues snapshot must be a JSON list or {'issues': [...]} object")
    return [normalize_issue(item) for item in payload]


def plan_json_to_issues(
    bugs: list[dict[str, Any]],
    existing_issues: list[dict[str, Any]] | None = None,
    *,
    historical_import: bool = False,
    p0_p1_only: bool = False,
) -> list[dict[str, Any]]:
    issues_by_bug_id: dict[str, dict[str, Any]] = {}
    for raw_issue in existing_issues or []:
        issue = normalize_issue(raw_issue)
        if issue_has_conflicting_markers(issue):
            continue
        bug_id = issue_bug_id(issue)
        if bug_id:
            issues_by_bug_id[bug_id] = issue
    plan: list[dict[str, Any]] = []

    for bug in bugs:
        if not should_sync_bug(bug, p0_p1_only=p0_p1_only):
            plan.append({
                "bug_id": bug["bug_id"],
                "action": "skip",
                "reason": "severity_filter_p0_p1_only",
                "severity": bug.get("severity"),
            })
            continue

        desired = {
            "title": issue_title_for_bug(bug),
            "body": issue_body_for_bug(bug),
            "labels": issue_labels_for_bug(bug, historical_import=historical_import),
            "state": "closed" if str(bug.get("status", "")).lower() in {"fixed", "closed", "resolved", "verified"} else "open",
        }
        existing = issues_by_bug_id.get(str(bug["bug_id"]))
        if existing is None:
            plan.append({"bug_id": bug["bug_id"], "action": "create", "desired": desired})
            continue

        changes: dict[str, Any] = {}
        for key in ("title", "body", "state"):
            if existing.get(key) != desired[key]:
                changes[key] = desired[key]
        if sorted(existing.get("labels", [])) != desired["labels"]:
            changes["labels"] = desired["labels"]

        plan.append({
            "bug_id": bug["bug_id"],
            "action": "update" if changes else "noop",
            "issue_number": existing.get("number"),
            "issue_url": existing.get("html_url"),
            "desired": desired,
            "changes": changes,
        })
    return plan


class GitHubClient:
    def __init__(self, *, repo: str, token: str) -> None:
        if not repo or "/" not in repo:
            raise BugGitHubSyncError("repo must be in owner/name form")
        if not token:
            raise BugGitHubSyncError("--apply requires a GitHub token via --token or GITHUB_TOKEN")
        self.repo = repo
        self.token = token
        self.api = f"https://api.github.com/repos/{repo}"

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        req = request.Request(
            f"{self.api}{path}",
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "aistock-bug-github-sync",
            },
        )
        try:
            with request.urlopen(req, timeout=30) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else None
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise BugGitHubSyncError(f"GitHub API {method} {path} failed: HTTP {exc.code}: {detail}") from exc

    def list_issues(self) -> list[dict[str, Any]]:
        page = 1
        issues: list[dict[str, Any]] = []
        while True:
            batch = self._request("GET", f"/issues?state=all&labels={BUG_LABEL}&per_page=100&page={page}")
            if not batch:
                break
            issues.extend(normalize_issue(item) for item in batch if "pull_request" not in item)
            if len(batch) < 100:
                break
            page += 1
        return issues

    def create_issue(self, desired: dict[str, Any]) -> dict[str, Any]:
        payload = {"title": desired["title"], "body": desired["body"], "labels": desired["labels"]}
        created = self._request("POST", "/issues", payload)
        if desired.get("state") == "closed":
            created = self._request("PATCH", f"/issues/{created['number']}", {"state": "closed"})
        return normalize_issue(created)

    def update_issue(self, number: int, changes: dict[str, Any]) -> dict[str, Any]:
        return normalize_issue(self._request("PATCH", f"/issues/{number}", changes))


def apply_plan(plan: list[dict[str, Any]], client: GitHubClient) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in plan:
        action = item["action"]
        if action in {"skip", "noop"}:
            results.append({"bug_id": item["bug_id"], "action": action})
        elif action == "create":
            issue = client.create_issue(item["desired"])
            results.append({"bug_id": item["bug_id"], "action": "created", "issue_number": issue.get("number"), "issue_url": issue.get("html_url")})
        elif action == "update":
            number = item.get("issue_number")
            if not isinstance(number, int):
                raise BugGitHubSyncError(f"cannot update issue without numeric issue_number for {item['bug_id']}")
            issue = client.update_issue(number, item["changes"])
            results.append({"bug_id": item["bug_id"], "action": "updated", "issue_number": issue.get("number"), "issue_url": issue.get("html_url")})
        else:
            raise BugGitHubSyncError(f"unknown plan action: {action}")
    return results


def summarize_plan(plan: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in plan:
        counts[item["action"]] = counts.get(item["action"], 0) + 1
    return counts


def run(config: SyncConfig) -> dict[str, Any]:
    directions = {"json-to-issues"} if config.direction == "json-to-issues" else {"issues-to-json"} if config.direction == "issues-to-json" else {"json-to-issues", "issues-to-json"}
    github_remote_needed = (config.apply and "json-to-issues" in directions) or (config.issues_snapshot is None and "issues-to-json" in directions)
    repo = config.repo or (_github_repo_default() if github_remote_needed else None)
    token = config.token if config.token is not None else _github_token_default(remote_needed=github_remote_needed)
    if config.apply and "json-to-issues" in directions and not token:
        raise BugGitHubSyncError("--apply requires --token, GITHUB_TOKEN/GH_TOKEN, or gh auth token; dry-run is the default offline mode")
    if github_remote_needed and not token:
        raise BugGitHubSyncError("live GitHub issue sync requires --token, GITHUB_TOKEN/GH_TOKEN, or gh auth token; use --issues-snapshot for offline planning")
    if github_remote_needed and not repo:
        raise BugGitHubSyncError("--apply requires --repo owner/name, GITHUB_REPOSITORY, .env.github-issues-local, or a GitHub origin remote")

    bugs = load_bug_files(config.bugs_dir)
    if github_remote_needed:
        client = GitHubClient(repo=str(repo), token=str(token))
        existing = client.list_issues()
    else:
        client = None
        existing = load_issues_snapshot(config.issues_snapshot)

    plan = plan_json_to_issues(
        bugs,
        existing,
        historical_import=config.historical_import,
        p0_p1_only=config.p0_p1_only,
    ) if "json-to-issues" in directions else []
    issues_to_json_plan = plan_issues_to_json(
        existing,
        bugs,
        bugs_dir=config.bugs_dir,
        p0_p1_only=config.p0_p1_only,
    ) if "issues-to-json" in directions else []
    result: dict[str, Any] = {
        "status": "planned" if not config.apply else "applied",
        "dry_run": not config.apply,
        "direction": config.direction,
        "bugs_dir": str(config.bugs_dir),
        "repo": repo,
        "historical_import": config.historical_import,
        "p0_p1_only": config.p0_p1_only,
        "summary": summarize_plan(plan),
        "json_to_issues_summary": summarize_plan(plan),
        "issues_to_json_summary": summarize_plan(issues_to_json_plan),
        "plan": plan,
        "issues_to_json_plan": issues_to_json_plan,
    }
    if config.apply and client is not None:
        result["results"] = apply_plan(plan, client)
    if config.apply and issues_to_json_plan:
        result["issues_to_json_results"] = apply_issues_to_json_plan(issues_to_json_plan)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline-first AIstock bugs JSON to GitHub Issues sync helper")
    parser.add_argument("--bugs-dir", type=Path, default=BUGS_DIR, help="Directory containing AIstock bug JSON files")
    parser.add_argument("--repo", default=_github_repo_default(), help="GitHub repo in owner/name form")
    parser.add_argument("--token", default=None, help="GitHub token; required only with --apply")
    parser.add_argument("--issues-snapshot", type=Path, help="Offline JSON snapshot of existing GitHub issues for idempotency planning")
    parser.add_argument("--historical-import", action="store_true", help="Add the import:historical label to planned issues")
    parser.add_argument("--p0-p1-only", action="store_true", help="Only plan P0/P1 bugs; use for auto-file/import flows")
    parser.add_argument("--all-severities", action="store_true", help="Explicitly disable the P0/P1 filter when composing reusable commands")
    parser.add_argument("--direction", choices=["json-to-issues", "issues-to-json", "both"], default="json-to-issues", help="Sync direction to plan/apply")
    parser.add_argument("--apply", action="store_true", help="Perform GitHub writes; without this flag the command is offline dry-run")
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    p0_p1_only = args.p0_p1_only and not args.all_severities
    directions = {"json-to-issues"} if args.direction == "json-to-issues" else {"issues-to-json"} if args.direction == "issues-to-json" else {"json-to-issues", "issues-to-json"}
    remote_needed = (args.apply and "json-to-issues" in directions) or (args.issues_snapshot is None and "issues-to-json" in directions)
    token = args.token if args.token is not None else _github_token_default(remote_needed=remote_needed)
    try:
        payload = run(
            SyncConfig(
                bugs_dir=args.bugs_dir,
                repo=args.repo,
                token=token,
                apply=args.apply,
                historical_import=args.historical_import,
                p0_p1_only=p0_p1_only,
                issues_snapshot=args.issues_snapshot,
                direction=args.direction,
            )
        )
    except BugGitHubSyncError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        summary = ", ".join(f"{key}={value}" for key, value in sorted(payload["summary"].items()))
        mode = "dry-run" if payload["dry_run"] else "apply"
        print(f"AIstock bug GitHub sync {mode}: {summary}")
        if payload["dry_run"]:
            print("No GitHub writes performed. Re-run with --apply plus --repo and --token to write issues.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
