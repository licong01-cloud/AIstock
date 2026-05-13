"""AIstock Validation Center MCP server (stdio transport).

Exposes a subset of the Validation Center read-only and controlled-runner
endpoints to MCP-aware clients (Claude Code, Codex App), plus a write-side
``report_bug`` tool that adds a new entry to ``tests/aistock_validation/bugs/``.

Design:
- Read/action tools call the Validation Center HTTP API at
  ``${AISTOCK_VALIDATION_BASE_URL}`` (default ``http://127.0.0.1:8001/api/v1/validation``).
- ``report_bug`` writes directly to the filesystem under
  ``tests/aistock_validation/bugs/`` because the backend is read-only for bug
  records. Bug ID assignment + fingerprint-based de-duplication are handled
  client-side.
- All tools raise on HTTP / FS error so the MCP client surfaces the failure
  to the agent instead of silently returning empty data.

Run:
    python scripts/aistock_mcp_server.py

Environment:
    AISTOCK_VALIDATION_BASE_URL  default http://127.0.0.1:8001/api/v1/validation
    AISTOCK_REPO_ROOT            default the parent of this script's directory
    AISTOCK_HTTP_TIMEOUT         default 30 seconds
    GH_TOKEN                     required only for live GitHub issue calls
    GITHUB_REPOSITORY            required only for live GitHub issue calls (owner/name)
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import importlib.util
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import httpx

try:  # FastMCP is the canonical stdio MCP server. Imported lazily-friendly.
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover - installation guard
    raise SystemExit(
        "mcp package is required: pip install mcp\n"
        f"Underlying error: {exc}"
    ) from exc


SCHEMA_VERSION = "aistock_validation_bug_v1"
DEFAULT_BASE_URL = "http://127.0.0.1:8001/api/v1/validation"
DEFAULT_TIMEOUT = 30.0
SEVERITY_VALUES = {"P0", "P1", "P2", "P3"}
STATUS_VALUES = {"open", "in_progress", "fixed", "verified", "wontfix"}
LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
GITHUB_API_BASE = "https://api.github.com"
GITHUB_BUG_LABEL = "aistock:bug"
GITHUB_STATE_VALUES = {"open", "closed", "all"}
GITHUB_SOURCE_VALUES = {"local", "github", "both"}
GITHUB_CLOSED_STATUSES = {"closed", "fixed", "resolved", "verified", "wontfix"}
GITHUB_MARKER_RE = re.compile(r"<!--\s*aistock-bug-id:\s*([^>\s]+)\s*-->", re.I)
TITLE_PREFIX_RE = re.compile(r"^\[([^\]]+)\]")
LOCAL_ENV_FILE = ".env.github-issues-local"


def _sanitize_identifier(value: Any, name: str) -> str:
    """Reject anything that could change the URL path semantics.

    Forbids ``/`` (path traversal / extra segments), ``%`` (encoded slash),
    ``?`` / ``#`` (query / fragment), whitespace, and any other punctuation.
    Allows the small alphabet used by canonical AIstock identifiers
    (BUG-NNN, paper_v2_backend, qe_20260415_173338_d1c5, exec-1, etc).
    """
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string; got {value!r}")
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(
            f"{name} contains illegal characters: {value!r}; "
            f"only [A-Za-z0-9_.-] allowed"
        )
    return value


def _assert_loopback_url(url: str) -> str:
    """Refuse to issue HTTP against any host outside the loopback set."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host not in LOOPBACK_HOSTS:
        raise ValueError(
            f"AISTOCK_VALIDATION_BASE_URL must be loopback "
            f"({sorted(LOOPBACK_HOSTS)}); got host={host!r} url={url!r}"
        )
    return url


def _load_local_github_env() -> None:
    """Load local GitHub issue defaults without overriding explicit process env."""
    if os.environ.get("AISTOCK_GITHUB_SKIP_ENV_FILE"):
        return

    roots: list[Path] = []
    for root in (Path.cwd(), Path(__file__).resolve().parents[1]):
        resolved = root.resolve()
        if resolved not in roots:
            roots.append(resolved)

    for root in roots:
        path = root / LOCAL_ENV_FILE
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


def _httpx_trust_env_for_github() -> bool:
    proxy_keys = ("HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "https_proxy", "http_proxy", "all_proxy")
    for key in proxy_keys:
        value = os.environ.get(key, "").strip().lower()
        if value.startswith(("socks://", "socks4://", "socks5://", "socks5h://")):
            return importlib.util.find_spec("socksio") is not None
    return True


_load_local_github_env()


def _resolve_repo_root() -> Path:
    explicit = os.environ.get("AISTOCK_REPO_ROOT")
    if explicit:
        return Path(explicit).resolve()
    return Path(__file__).resolve().parent.parent


REPO_ROOT = _resolve_repo_root()
BUG_ROOT = REPO_ROOT / "tests" / "aistock_validation" / "bugs"


def _utcnow_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _today_yyyymmdd() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d")


class ValidationCenterClient:
    """HTTP client for the Validation Center read-only + runner endpoints.

    The Validation Center wraps every successful response as
    ``{"data": <payload>}``. This client unwraps ``data`` and returns the
    inner payload directly so MCP tools see the same shape the agent
    actually wants.
    """

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        candidate = (base_url or os.environ.get("AISTOCK_VALIDATION_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        _assert_loopback_url(candidate)
        self.base_url = candidate
        self.timeout = float(timeout if timeout is not None else os.environ.get("AISTOCK_HTTP_TIMEOUT", DEFAULT_TIMEOUT))
        self._transport = transport

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout,
            transport=self._transport,
            trust_env=False,
        )

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        clean_params = None if params is None else {k: v for k, v in params.items() if v is not None}
        with self._client() as client:
            response = client.get(path, params=clean_params)
        if response.status_code >= 400:
            raise RuntimeError(
                f"Validation Center GET {path} failed with HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Validation Center GET {path} returned non-JSON body (HTTP {response.status_code})"
            ) from exc
        if not isinstance(payload, dict) or "data" not in payload:
            raise RuntimeError(
                f"Validation Center GET {path} returned unexpected envelope: keys={list(payload) if isinstance(payload, dict) else type(payload).__name__}"
            )
        return payload["data"]

    def post(self, path: str, json_body: dict[str, Any]) -> Any:
        with self._client() as client:
            response = client.post(path, json=json_body)
        if response.status_code >= 400:
            raise RuntimeError(
                f"Validation Center POST {path} failed with HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Validation Center POST {path} returned non-JSON body (HTTP {response.status_code})"
            ) from exc
        if not isinstance(payload, dict) or "data" not in payload:
            raise RuntimeError(
                f"Validation Center POST {path} returned unexpected envelope: keys={list(payload) if isinstance(payload, dict) else type(payload).__name__}"
            )
        return payload["data"]


def _slugify(value: str, *, max_length: int = 60) -> str:
    base = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    if not base:
        base = "issue"
    return base[:max_length].rstrip("-") or "issue"


def _scan_existing_bug_ids() -> set[int]:
    if not BUG_ROOT.exists():
        return set()
    ids: set[int] = set()
    pattern = re.compile(r"BUG-(\d{3,})")
    for path in BUG_ROOT.glob("*.json"):
        match = pattern.search(path.name)
        if match:
            ids.add(int(match.group(1)))
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        bug_id = str(payload.get("bug_id") or "")
        match = pattern.search(bug_id)
        if match:
            ids.add(int(match.group(1)))
    return ids


def _next_bug_id() -> str:
    existing = _scan_existing_bug_ids()
    next_int = (max(existing) + 1) if existing else 1
    return f"BUG-{next_int:03d}"


def _fingerprint(module: str, title: str, reproduce_command: str) -> str:
    payload = "::".join(part.strip().lower() for part in (module, title, reproduce_command))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _existing_bug_for_fingerprint(fingerprint: str) -> dict[str, Any] | None:
    if not BUG_ROOT.exists():
        return None
    for path in BUG_ROOT.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("fingerprint") == fingerprint:
            return {
                "path": str(path.relative_to(REPO_ROOT)).replace("\\", "/"),
                "bug_id": payload.get("bug_id"),
                "status": payload.get("status"),
                "title": payload.get("title"),
            }
    return None


def _build_bug_record(
    *,
    bug_id: str,
    title: str,
    severity: str,
    module: str,
    files: list[str],
    reproduce_command: str,
    expected: str,
    actual: str,
    fix_owner: str | None,
    related_drawer: str | None,
    comments: list[str] | None,
    fingerprint: str,
    now_iso: str,
) -> dict[str, Any]:
    description_lines = [
        f"Expected: {expected.strip()}" if expected else "",
        f"Actual: {actual.strip()}" if actual else "",
    ]
    description = "\n".join(line for line in description_lines if line) or title
    evidence: list[str] = []
    if related_drawer:
        normalized = related_drawer if ":" in related_drawer else f"drawer:cross-tool/codex-claude-coord/{related_drawer}"
        evidence.append(normalized)
    for path in files:
        evidence.append(f"file:{path}")
    events: list[dict[str, str]] = [
        {
            "timestamp": now_iso,
            "actor": fix_owner or "unknown",
            "action": "discovered",
            "note": f"Reported via aistock_mcp_server.report_bug. Comments: {len(comments) if comments else 0}",
        }
    ]
    for comment in comments or []:
        events.append(
            {
                "timestamp": now_iso,
                "actor": fix_owner or "unknown",
                "action": "comment",
                "note": comment,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "bug_id": bug_id,
        "title": title,
        "description": description,
        "module": module,
        "severity": severity.upper(),
        "risk_area": "unspecified",
        "status": "open",
        "trigger_condition": {
            "scenario": "reported_via_mcp",
            "files": list(files),
        },
        "reproduce_command": reproduce_command,
        "failing_run_id": None,
        "evidence_uris": evidence,
        "fingerprint": fingerprint,
        "assigned_agent": fix_owner,
        "fix_branch": None,
        "fix_commit": None,
        "verification_run_id": None,
        "created_at": now_iso,
        "first_seen_at": now_iso,
        "last_seen_at": now_iso,
        "fixed_at": None,
        "submitted_at": now_iso,
        "closed_at": None,
        "allowed_write_scope": list(files),
        "suspected_modules": list(files),
        "required_verification": [],
        "closure_requirements": [],
        "events": events,
    }


def _write_bug_record(record: dict[str, Any], slug: str) -> Path:
    BUG_ROOT.mkdir(parents=True, exist_ok=True)
    filename = f"{_today_yyyymmdd()}_{record['bug_id']}-{slug}.json"
    path = BUG_ROOT / filename
    path.write_text(json.dumps(record, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def _repo_relative_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT)).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def _load_bug_records() -> list[dict[str, Any]]:
    if not BUG_ROOT.exists():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(BUG_ROOT.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Failed to read bug JSON registry entry {path}: {exc}") from exc
        if not isinstance(payload, dict) or not payload.get("bug_id"):
            raise RuntimeError(f"Invalid bug JSON registry entry missing bug_id: {path}")
        payload = dict(payload)
        payload["_source_path"] = _repo_relative_path(path)
        records.append(payload)
    return records


def _label_slug(value: Any) -> str | None:
    if value in (None, ""):
        return None
    slug = re.sub(r"[^a-z0-9_.-]+", "-", str(value).strip().lower()).strip("-")
    return slug or None


def _dedupe_labels(labels: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for label in labels:
        clean = str(label).strip()
        if not clean:
            continue
        key = clean.lower()
        if key not in seen:
            seen.add(key)
            result.append(clean)
    return result


def _github_labels_for_bug_record(record: dict[str, Any], extra_labels: list[str] | None = None) -> list[str]:
    labels = [GITHUB_BUG_LABEL]
    for prefix, key in (("severity", "severity"), ("module", "module"), ("status", "status"), ("risk", "risk_area")):
        slug = _label_slug(record.get(key))
        if slug:
            labels.append(f"{prefix}:{slug}")
    labels.extend(extra_labels or [])
    return sorted(_dedupe_labels(labels), key=str.lower)


def _issue_title_for_bug_record(record: dict[str, Any]) -> str:
    title = str(record.get("title") or "Untitled AIstock bug").strip()
    bug_id = str(record.get("bug_id") or "").strip()
    return title if bug_id and title.startswith(f"[{bug_id}]") else f"[{bug_id}] {title}" if bug_id else title


def _issue_body_for_bug_record(record: dict[str, Any]) -> str:
    compact_fields = {
        "bug_id": record.get("bug_id"),
        "severity": record.get("severity"),
        "module": record.get("module"),
        "risk_area": record.get("risk_area"),
        "status": record.get("status"),
        "fingerprint": record.get("fingerprint"),
        "reproduce_command": record.get("reproduce_command"),
        "source_path": record.get("_source_path"),
    }
    sections = [
        f"<!-- aistock-bug-id: {record.get('bug_id')} -->",
        "<!-- managed-by: scripts/aistock_mcp_server.py -->",
        "",
        "## Summary",
        str(record.get("description") or record.get("title") or "").strip() or "No description provided.",
        "",
        "## Source of Truth",
        "The versioned bugs JSON entry remains the source of truth; this GitHub Issue is a workflow/UI mirror.",
        "",
        "## Bug Metadata",
        "```json",
        json.dumps({k: v for k, v in compact_fields.items() if v not in (None, "", [])}, ensure_ascii=False, indent=2, sort_keys=True),
        "```",
    ]
    return "\n".join(sections).rstrip() + "\n"


def _bug_state(record: dict[str, Any]) -> str:
    status = str(record.get("status") or "open").strip().lower()
    return "closed" if status in GITHUB_CLOSED_STATUSES else "open"


def _normalize_registry_issue(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": "bug_json",
        "registry_is_source_of_truth": True,
        "bug_id": record.get("bug_id"),
        "number": record.get("github_issue_number"),
        "title": record.get("title") or "",
        "body": record.get("description") or "",
        "state": _bug_state(record),
        "status": record.get("status") or "open",
        "severity": str(record.get("severity") or "").upper() or None,
        "module": record.get("module"),
        "labels": _github_labels_for_bug_record(record),
        "html_url": record.get("github_issue_url"),
        "source_path": record.get("_source_path"),
        "fingerprint": record.get("fingerprint"),
        "reproduce_command": record.get("reproduce_command"),
    }


def _normalize_source(source: str) -> str:
    value = str(source or "local").lower()
    if value not in GITHUB_SOURCE_VALUES:
        raise ValueError(f"source must be one of {sorted(GITHUB_SOURCE_VALUES)}; got {source!r}")
    return value


def _normalize_github_state(state: str) -> str:
    value = str(state or "open").lower()
    if value not in GITHUB_STATE_VALUES:
        raise ValueError(f"state must be one of {sorted(GITHUB_STATE_VALUES)}; got {state!r}")
    return value


def _normalize_label_filter(labels: list[str] | None) -> list[str]:
    if labels is None:
        return []
    if not isinstance(labels, list) or not all(isinstance(item, str) for item in labels):
        raise ValueError("labels must be a list[str]")
    return _dedupe_labels(labels)


def _filter_issue_items(
    items: list[dict[str, Any]],
    *,
    state: str,
    module: str | None,
    severity: str | None,
    status: str | None,
    labels: list[str],
) -> list[dict[str, Any]]:
    label_needles = {label.lower() for label in labels}
    severity_norm = severity.upper() if severity else None
    status_norm = status.lower() if status else None
    result: list[dict[str, Any]] = []
    for item in items:
        if state != "all" and str(item.get("state") or "").lower() != state:
            continue
        if module and str(item.get("module") or "") != module:
            continue
        if severity_norm and str(item.get("severity") or "").upper() != severity_norm:
            continue
        if status_norm and str(item.get("status") or "").lower() != status_norm:
            continue
        item_labels = {str(label).lower() for label in item.get("labels") or []}
        if label_needles and not label_needles.issubset(item_labels):
            continue
        result.append(item)
    return result


def _paginate(items: list[dict[str, Any]], *, page: int, page_size: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if page < 1:
        raise ValueError("page must be >= 1")
    if page_size < 1 or page_size > 200:
        raise ValueError("page_size must be between 1 and 200")
    start = (page - 1) * page_size
    end = start + page_size
    return items[start:end], {"page": page, "page_size": page_size, "total": len(items)}


def _extract_label_names(raw: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    for label in raw.get("labels", []) or []:
        name = label.get("name") if isinstance(label, dict) else label
        if name:
            labels.append(str(name))
    return sorted(_dedupe_labels(labels), key=str.lower)


def _github_issue_bug_id(issue: dict[str, Any]) -> str | None:
    marker = GITHUB_MARKER_RE.search(str(issue.get("body") or ""))
    if marker:
        return marker.group(1)
    title_match = TITLE_PREFIX_RE.search(str(issue.get("title") or ""))
    if title_match and title_match.group(1).upper().startswith("BUG"):
        return title_match.group(1)
    return None


def _issue_severity_from_labels(labels: list[str]) -> str | None:
    for label in labels:
        raw = label.lower()
        if raw.startswith("severity:"):
            value = raw.split(":", 1)[1].upper()
            return value if value in SEVERITY_VALUES else None
        if raw.upper() in SEVERITY_VALUES:
            return raw.upper()
    return None


def _issue_module_from_labels(labels: list[str]) -> str | None:
    for label in labels:
        raw = label.lower()
        if raw.startswith("module:"):
            value = raw.split(":", 1)[1].strip()
            return value or None
    return None


def _normalize_github_issue(raw: dict[str, Any]) -> dict[str, Any]:
    labels = _extract_label_names(raw)
    return {
        "source": "github",
        "registry_is_source_of_truth": False,
        "bug_id": _github_issue_bug_id(raw),
        "number": raw.get("number"),
        "title": raw.get("title") or "",
        "body": raw.get("body") or "",
        "state": raw.get("state") or "open",
        "status": raw.get("state") or "open",
        "severity": _issue_severity_from_labels(labels),
        "module": _issue_module_from_labels(labels),
        "labels": labels,
        "html_url": raw.get("html_url"),
    }


def _item_matches_query(item: dict[str, Any], query: str) -> bool:
    needle = query.lower()
    haystack = "\n".join(
        str(value or "")
        for value in (
            item.get("bug_id"),
            item.get("number"),
            item.get("title"),
            item.get("body"),
            item.get("module"),
            item.get("severity"),
            item.get("status"),
            item.get("source_path"),
            item.get("fingerprint"),
            item.get("reproduce_command"),
            " ".join(item.get("labels") or []),
        )
    ).lower()
    return needle in haystack


class GitHubIssueClient:
    """Small GitHub Issues client used only when explicit env is present."""

    def __init__(
        self,
        *,
        repo: str,
        token: str,
        timeout: float | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not repo or repo.count("/") != 1:
            raise ValueError("GITHUB_REPOSITORY must be in owner/name form")
        if not token:
            raise ValueError("GH_TOKEN is required for live GitHub issue calls")
        self.repo = repo
        self.token = token
        self.timeout = float(timeout if timeout is not None else os.environ.get("AISTOCK_HTTP_TIMEOUT", DEFAULT_TIMEOUT))
        self._transport = transport

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=f"{GITHUB_API_BASE}/repos/{self.repo}",
            timeout=self.timeout,
            transport=self._transport,
            trust_env=_httpx_trust_env_for_github(),
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "aistock-mcp-github-issues",
            },
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        with self._client() as client:
            response = client.request(method, path, params=params, json=json_body)
        if response.status_code >= 400:
            raise RuntimeError(
                f"GitHub Issues {method} {path} failed with HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )
        if not response.text:
            return None
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"GitHub Issues {method} {path} returned non-JSON body") from exc

    def list_issues(self, *, state: str = "open", labels: list[str] | None = None) -> list[dict[str, Any]]:
        issues: list[dict[str, Any]] = []
        page = 1
        while True:
            params: dict[str, Any] = {"state": state, "per_page": 100, "page": page}
            if labels:
                params["labels"] = ",".join(labels)
            batch = self._request("GET", "/issues", params=params)
            if not isinstance(batch, list):
                raise RuntimeError("GitHub Issues list returned unexpected payload")
            issue_batch = [item for item in batch if isinstance(item, dict) and "pull_request" not in item]
            issues.extend(_normalize_github_issue(item) for item in issue_batch)
            if len(batch) < 100:
                break
            page += 1
        return issues

    def create_issue(self, *, title: str, body: str, labels: list[str]) -> dict[str, Any]:
        created = self._request(
            "POST",
            "/issues",
            json_body={"title": title, "body": body, "labels": labels},
        )
        if not isinstance(created, dict):
            raise RuntimeError("GitHub Issues create returned unexpected payload")
        return _normalize_github_issue(created)


_github_client_factory: Callable[..., GitHubIssueClient] = GitHubIssueClient


def _github_issue_client_from_env() -> GitHubIssueClient:
    repo = os.environ.get("GITHUB_REPOSITORY")
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or _github_token_from_gh_cli()
    missing = [name for name, value in (("GH_TOKEN", token), ("GITHUB_REPOSITORY", repo)) if not value]
    if missing:
        raise ValueError(
            "live GitHub issue calls require env "
            f"{', '.join(missing)}; default MCP issue tools use the local bugs JSON registry"
        )
    return _github_client_factory(repo=str(repo), token=str(token))


def _collect_issue_items(
    *,
    source: str,
    state: str,
    labels: list[str],
) -> list[dict[str, Any]]:
    local_items = [_normalize_registry_issue(record) for record in _load_bug_records()] if source in {"local", "both"} else []
    github_items: list[dict[str, Any]] = []
    if source in {"github", "both"}:
        github_state = state if state != "all" else "all"
        github_items = _github_issue_client_from_env().list_issues(state=github_state, labels=labels or None)
    if source != "both":
        return local_items if source == "local" else github_items

    merged = list(local_items)
    local_by_bug_id = {str(item.get("bug_id")): item for item in merged if item.get("bug_id")}
    local_by_number = {int(item["number"]): item for item in merged if isinstance(item.get("number"), int)}
    for remote in github_items:
        match = None
        bug_id = remote.get("bug_id")
        number = remote.get("number")
        if bug_id:
            match = local_by_bug_id.get(str(bug_id))
        if match is None and isinstance(number, int):
            match = local_by_number.get(number)
        if match is None:
            merged.append(remote)
            continue
        match["github_issue"] = {
            "number": remote.get("number"),
            "state": remote.get("state"),
            "title": remote.get("title"),
            "labels": remote.get("labels"),
            "html_url": remote.get("html_url"),
        }
        match["number"] = match.get("number") or remote.get("number")
        match["html_url"] = match.get("html_url") or remote.get("html_url")
    return merged


# --- MCP server wiring ----------------------------------------------------

mcp = FastMCP("aistock-validation")
_default_client = ValidationCenterClient()


def _client() -> ValidationCenterClient:
    """Indirection so tests can swap a mock client per call."""
    return _default_client


@mcp.tool()
def health() -> dict[str, Any]:
    """Validation Center health probe (does not start any backend)."""
    return _client().get("/health")


@mcp.tool()
def list_plans() -> dict[str, Any]:
    """List validation test plans from the catalog."""
    return _client().get("/plans")


@mcp.tool()
def get_plan(plan_key: str) -> dict[str, Any]:
    """Get a single validation plan by ``plan_key``."""
    safe = _sanitize_identifier(plan_key, "plan_key")
    return _client().get(f"/plans/{safe}")


@mcp.tool()
def list_validation_runs(
    module: str | None = None,
    level: str | None = None,
    status: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """List validation history runs filtered by module / level / status."""
    return _client().get(
        "/runs",
        params={"module": module, "level": level, "status": status, "page": page, "page_size": page_size},
    )


@mcp.tool()
def get_validation_run(run_id: str) -> dict[str, Any]:
    """Get a single validation run record by ``run_id``."""
    safe = _sanitize_identifier(run_id, "run_id")
    return _client().get(f"/runs/{safe}")


@mcp.tool()
def list_findings(
    severity: str | None = None,
    source: str | None = None,
    module: str | None = None,
    status: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> dict[str, Any]:
    """List quality findings (guardrail + legacy_inventory) with filters."""
    return _client().get(
        "/findings",
        params={
            "severity": severity,
            "source_type": source,
            "module": module,
            "status": status,
            "page": page,
            "page_size": page_size,
        },
    )


@mcp.tool()
def list_bugs(
    status: str | None = None,
    module: str | None = None,
    severity: str | None = None,
    agent: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """List bug registry entries from ``tests/aistock_validation/bugs/`` via API."""
    return _client().get(
        "/bugs",
        params={
            "status": status,
            "module": module,
            "severity": severity,
            "agent": agent,
            "page": page,
            "page_size": page_size,
        },
    )


@mcp.tool()
def get_bug_agent_context(bug_id: str) -> dict[str, Any]:
    """Get the machine-readable repair context for ``bug_id``.

    The returned payload includes ``problem_statement``, ``reproduce_command``,
    ``allowed_write_scope``, ``suspected_modules``, ``required_verification``,
    and ``closure_requirements`` - everything an AI agent needs to start a
    bounded fix attempt without reading the raw JSON file.
    """
    safe = _sanitize_identifier(bug_id, "bug_id")
    return _client().get(f"/bugs/{safe}/agent-context")


@mcp.tool()
def get_module_quality_summary(module: str | None = None, commit_limit: int = 50) -> dict[str, Any]:
    """Get module quality + commit + coverage + guardrail priority summary.

    The Validation Center API returns the full summary; when ``module`` is
    provided this tool client-filters the ``modules`` array to that
    ``module_id`` for convenience. Pass ``module=None`` to get the full
    aggregate.
    """
    summary = _client().get("/modules/quality-summary", params={"commit_limit": commit_limit})
    if module is None:
        return summary
    if not isinstance(summary, dict):
        return summary
    modules = summary.get("modules") or []
    if not isinstance(modules, list):
        return summary
    filtered = [item for item in modules if isinstance(item, dict) and str(item.get("module_id") or "") == module]
    result = dict(summary)
    result["modules"] = filtered
    result["filter"] = {"module": module}
    return result


@mcp.tool()
def start_validation_execution(
    plan_key: str,
    requested_by: str = "mcp_agent",
    backend_port: int | None = None,
    frontend_port: int | None = None,
    timeout_seconds: int | None = None,
    confirm_text: str | None = None,
) -> dict[str, Any]:
    """Start an allowlisted controlled validation execution (nox session).

    Only ``plan_key`` values present in ``tests/aistock_validation/catalog/test_plans.yaml``
    with ``runner_enabled: true`` will be accepted; the backend rejects others
    with HTTP 400.
    """
    body: dict[str, Any] = {"plan_key": plan_key, "requested_by": requested_by}
    if backend_port is not None:
        body["backend_port"] = backend_port
    if frontend_port is not None:
        body["frontend_port"] = frontend_port
    if timeout_seconds is not None:
        body["timeout_seconds"] = timeout_seconds
    if confirm_text is not None:
        body["confirm_text"] = confirm_text
    return _client().post("/executions", json_body=body)


@mcp.tool()
def get_validation_execution_status(execution_id: str) -> dict[str, Any]:
    """Get the status / exit code / artifacts of a controlled validation execution."""
    safe = _sanitize_identifier(execution_id, "execution_id")
    return _client().get(f"/executions/{safe}")


@mcp.tool()
def get_validation_execution_log(execution_id: str, tail: int = 100) -> dict[str, Any]:
    """Get the tail of a controlled validation execution log."""
    if tail < 1 or tail > 2000:
        raise ValueError("tail must be between 1 and 2000")
    safe = _sanitize_identifier(execution_id, "execution_id")
    return _client().get(f"/executions/{safe}/log", params={"tail_lines": tail})


@mcp.tool()
def report_bug(
    title: str,
    severity: str,
    module: str,
    files: list[str],
    reproduce_command: str,
    expected: str,
    actual: str,
    fix_owner: str | None = None,
    related_drawer: str | None = None,
    comments: list[str] | None = None,
) -> dict[str, Any]:
    """Append a new bug record to ``tests/aistock_validation/bugs/``.

    Idempotent on (module + title + reproduce_command) fingerprint: if an
    existing bug has the same fingerprint, returns ``{"deduplicated": True,
    "existing": ...}`` without writing a new file.

    Returns ``{"deduplicated": False, "bug_id": ..., "path": ..., "fingerprint": ...}``
    on a successful write.
    """
    if not title or not module or not reproduce_command:
        raise ValueError("title, module, reproduce_command are required")
    severity_norm = severity.upper()
    if severity_norm not in SEVERITY_VALUES:
        raise ValueError(f"severity must be one of {sorted(SEVERITY_VALUES)}; got {severity!r}")
    if not isinstance(files, list) or not all(isinstance(item, str) for item in files):
        raise ValueError("files must be a list[str]")
    fingerprint = _fingerprint(module, title, reproduce_command)
    duplicate = _existing_bug_for_fingerprint(fingerprint)
    if duplicate is not None:
        return {
            "deduplicated": True,
            "existing": duplicate,
            "fingerprint": fingerprint,
        }
    bug_id = _next_bug_id()
    now_iso = _utcnow_iso()
    record = _build_bug_record(
        bug_id=bug_id,
        title=title,
        severity=severity_norm,
        module=module,
        files=files,
        reproduce_command=reproduce_command,
        expected=expected,
        actual=actual,
        fix_owner=fix_owner,
        related_drawer=related_drawer,
        comments=comments,
        fingerprint=fingerprint,
        now_iso=now_iso,
    )
    slug = _slugify(title)
    path = _write_bug_record(record, slug)
    return {
        "deduplicated": False,
        "bug_id": bug_id,
        "path": str(path.relative_to(REPO_ROOT)).replace("\\", "/"),
        "fingerprint": fingerprint,
    }


@mcp.tool()
def mcp_github_issue_list(
    state: str = "open",
    module: str | None = None,
    severity: str | None = None,
    status: str | None = None,
    labels: list[str] | None = None,
    source: str = "local",
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """List AIstock GitHub-issue mirrors, defaulting to the local bugs JSON registry.

    ``source`` can be ``local`` (default), ``github``, or ``both``. Live GitHub
    reads are opt-in and require ``GH_TOKEN`` + ``GITHUB_REPOSITORY``.
    """
    source_norm = _normalize_source(source)
    state_norm = _normalize_github_state(state)
    label_filter = _normalize_label_filter(labels)
    items = _collect_issue_items(source=source_norm, state=state_norm, labels=label_filter)
    filtered = _filter_issue_items(
        items,
        state=state_norm,
        module=module,
        severity=severity,
        status=status,
        labels=label_filter,
    )
    page_items, pagination = _paginate(filtered, page=page, page_size=page_size)
    return {
        "source": source_norm,
        "registry_source": _repo_relative_path(BUG_ROOT),
        "registry_is_source_of_truth": True,
        "filters": {
            "state": state_norm,
            "module": module,
            "severity": severity.upper() if severity else None,
            "status": status,
            "labels": label_filter,
        },
        **pagination,
        "items": page_items,
    }


@mcp.tool()
def mcp_github_issue_search(
    query: str,
    state: str = "all",
    module: str | None = None,
    severity: str | None = None,
    status: str | None = None,
    labels: list[str] | None = None,
    source: str = "local",
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Search AIstock issue mirrors across local JSON and optional live GitHub data."""
    if not isinstance(query, str) or not query.strip():
        raise ValueError("query must be a non-empty string")
    source_norm = _normalize_source(source)
    state_norm = _normalize_github_state(state)
    label_filter = _normalize_label_filter(labels)
    items = _collect_issue_items(source=source_norm, state=state_norm, labels=label_filter)
    filtered = [
        item
        for item in _filter_issue_items(
            items,
            state=state_norm,
            module=module,
            severity=severity,
            status=status,
            labels=label_filter,
        )
        if _item_matches_query(item, query.strip())
    ]
    page_items, pagination = _paginate(filtered, page=page, page_size=page_size)
    return {
        "source": source_norm,
        "registry_source": _repo_relative_path(BUG_ROOT),
        "registry_is_source_of_truth": True,
        "query": query.strip(),
        "filters": {
            "state": state_norm,
            "module": module,
            "severity": severity.upper() if severity else None,
            "status": status,
            "labels": label_filter,
        },
        **pagination,
        "items": page_items,
    }


@mcp.tool()
def mcp_github_issue_create(
    title: str,
    body: str = "",
    severity: str = "P2",
    module: str = "github_issues",
    labels: list[str] | None = None,
    reproduce_command: str | None = None,
    fix_owner: str | None = None,
    create_github: bool = False,
) -> dict[str, Any]:
    """Create a source-of-truth bugs JSON record and optionally mirror to GitHub.

    Live GitHub creation is disabled by default. Set ``create_github=True`` only
    when ``GH_TOKEN`` and ``GITHUB_REPOSITORY`` are present.
    """
    if not isinstance(title, str) or not title.strip():
        raise ValueError("title must be a non-empty string")
    if not isinstance(module, str) or not module.strip():
        raise ValueError("module must be a non-empty string")
    severity_norm = severity.upper()
    if severity_norm not in SEVERITY_VALUES:
        raise ValueError(f"severity must be one of {sorted(SEVERITY_VALUES)}; got {severity!r}")
    label_values = _normalize_label_filter(labels)
    description = body.strip() or title.strip()
    reproduce = reproduce_command or "n/a (created via mcp_github_issue_create)"
    fingerprint = _fingerprint(module.strip(), title.strip(), reproduce_command or description)
    duplicate = _existing_bug_for_fingerprint(fingerprint)
    if duplicate is not None:
        return {
            "deduplicated": True,
            "existing": duplicate,
            "fingerprint": fingerprint,
            "github": {"created": False, "reason": "duplicate_registry_record"},
            "registry_is_source_of_truth": True,
        }

    bug_id = _next_bug_id()
    now_iso = _utcnow_iso()
    record = _build_bug_record(
        bug_id=bug_id,
        title=title.strip(),
        severity=severity_norm,
        module=module.strip(),
        files=[],
        reproduce_command=reproduce,
        expected="Track and resolve this issue through the AIstock bugs JSON registry.",
        actual=description,
        fix_owner=fix_owner,
        related_drawer=None,
        comments=None,
        fingerprint=fingerprint,
        now_iso=now_iso,
    )
    record["description"] = description
    record["risk_area"] = "github_issue_tracking"
    record["trigger_condition"] = {"scenario": "created_via_mcp_github_issue_create"}
    record["allowed_write_scope"] = []
    record["suspected_modules"] = [module.strip()]
    record["custom_github_labels"] = label_values
    github_result: dict[str, Any] = {"created": False, "reason": "create_github_false"}

    if create_github:
        record["_source_path"] = _repo_relative_path(BUG_ROOT / f"{_today_yyyymmdd()}_{bug_id}-{_slugify(title)}.json")
        issue = _github_issue_client_from_env().create_issue(
            title=_issue_title_for_bug_record(record),
            body=_issue_body_for_bug_record(record),
            labels=_github_labels_for_bug_record(record, extra_labels=label_values),
        )
        record["github_issue_number"] = issue.get("number")
        record["github_issue_url"] = issue.get("html_url")
        if issue.get("html_url"):
            record["evidence_uris"].append(str(issue["html_url"]))
        github_result = {
            "created": True,
            "number": issue.get("number"),
            "html_url": issue.get("html_url"),
            "state": issue.get("state"),
        }
        record.pop("_source_path", None)

    slug = _slugify(title)
    path = _write_bug_record(record, slug)
    return {
        "deduplicated": False,
        "bug_id": bug_id,
        "path": str(path.relative_to(REPO_ROOT)).replace("\\", "/"),
        "fingerprint": fingerprint,
        "github": github_result,
        "registry_is_source_of_truth": True,
    }


def main() -> None:
    """Entry point: run the FastMCP stdio server."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
