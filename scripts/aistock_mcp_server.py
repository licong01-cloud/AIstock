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
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

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
        self.base_url = (base_url or os.environ.get("AISTOCK_VALIDATION_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.timeout = float(timeout if timeout is not None else os.environ.get("AISTOCK_HTTP_TIMEOUT", DEFAULT_TIMEOUT))
        self._transport = transport

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout,
            transport=self._transport,
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
    return _client().get(f"/plans/{plan_key}")


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
    return _client().get(f"/runs/{run_id}")


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
    return _client().get(f"/bugs/{bug_id}/agent-context")


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
    return _client().get(f"/executions/{execution_id}")


@mcp.tool()
def get_validation_execution_log(execution_id: str, tail: int = 100) -> dict[str, Any]:
    """Get the tail of a controlled validation execution log."""
    if tail < 1 or tail > 2000:
        raise ValueError("tail must be between 1 and 2000")
    return _client().get(f"/executions/{execution_id}/log", params={"tail_lines": tail})


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


def main() -> None:
    """Entry point: run the FastMCP stdio server."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
