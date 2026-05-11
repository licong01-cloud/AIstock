# AIstock MCP Server Protocol (2026-05-10)

`scripts/aistock_mcp_server.py` exposes 13 tools to MCP-aware clients
(Claude Code, Codex App) for read-only Validation Center introspection,
controlled nox execution, and bug-registry write-side reporting.

## 1. Transport

- stdio (default for Anthropic Claude Code and Codex App configurations)
- launched as `python scripts/aistock_mcp_server.py`
- one process per client; tear-down on client disconnect

## 2. Configuration

Environment variables consumed at startup:

| Variable | Default | Purpose |
|----------|---------|---------|
| `AISTOCK_VALIDATION_BASE_URL` | `http://127.0.0.1:8001/api/v1/validation` | Validation Center backend root |
| `AISTOCK_REPO_ROOT` | parent of `scripts/aistock_mcp_server.py` | repo root for write-side ops |
| `AISTOCK_HTTP_TIMEOUT` | `30` (seconds) | per-request httpx timeout |

Loopback only — the server makes outbound HTTP requests strictly to
`AISTOCK_VALIDATION_BASE_URL`. No external network.

## 3. Tool catalog

### 3.1 Read tools (8 + health)

| Tool | Backend route | Notes |
|------|---------------|-------|
| `health()` | `GET /health` | Validation Center mode + counts |
| `list_plans()` | `GET /plans` | full plan registry |
| `get_plan(plan_key)` | `GET /plans/{plan_key}` | single plan detail |
| `list_validation_runs(module, level, status, page, page_size)` | `GET /runs` | history runs |
| `get_validation_run(run_id)` | `GET /runs/{run_id}` | single run detail |
| `list_findings(severity, source, module, status, page, page_size)` | `GET /findings` | guardrail + legacy_inventory; `source` maps to `source_type` |
| `list_bugs(status, module, severity, agent, page, page_size)` | `GET /bugs` | bug registry |
| `get_bug_agent_context(bug_id)` | `GET /bugs/{bug_id}/agent-context` | **AI repair entry point** — see §4 |
| `get_module_quality_summary(module=None, commit_limit=50)` | `GET /modules/quality-summary` | client-side filters `modules[]` when `module` is set |

### 3.2 Action tools (3)

| Tool | Backend route | Notes |
|------|---------------|-------|
| `start_validation_execution(plan_key, requested_by="mcp_agent", backend_port, frontend_port, timeout_seconds, confirm_text)` | `POST /executions` | only allowlisted plans (`runner_enabled: true`) accepted |
| `get_validation_execution_status(execution_id)` | `GET /executions/{job_id}` | poll job state |
| `get_validation_execution_log(execution_id, tail=100)` | `GET /executions/{job_id}/log` | tail clamped to [1, 2000] |

### 3.3 Write tool (1)

| Tool | Effect | Notes |
|------|--------|-------|
| `report_bug(title, severity, module, files, reproduce_command, expected, actual, fix_owner=None, related_drawer=None, comments=None)` | append `tests/aistock_validation/bugs/<YYYYMMDD>_BUG-NNN-<slug>.json` | idempotent on `(module, title, reproduce_command)` fingerprint — dedup hits return `{"deduplicated": true, "existing": ...}` without writing |

## 4. AI repair loop (`bug_agent_context`)

`get_bug_agent_context(bug_id)` returns:

```json
{
  "schema_version": "aistock_validation_agent_context_v1",
  "context_type": "bug",
  "bug_id": "BUG-023",
  "problem_statement": "...",
  "finding_source": "validation_failure",
  "severity": "P0",
  "status": "in_progress",
  "reproduce_command": "pytest backend/tests/strategy_package/test_repository_service.py -k atomic -q",
  "evidence_uris": ["drawer:cross-tool/codex-claude-coord/dd17c102a3a16e087d453364", ...],
  "allowed_write_scope": ["backend/services/strategy_package/repository.py", ...],
  "suspected_modules": ["backend/services/strategy_package/", ...],
  "required_verification": ["pytest backend/tests/strategy_package/test_repository_service.py -q"],
  "closure_requirements": ["Atomic invariant confirmed", ...],
  "github_issue_url": null,
  "verification_run_id": null
}
```

A repair agent should:

1. Call `get_bug_agent_context(bug_id)` to retrieve the bounded scope
2. Constrain edits to paths in `allowed_write_scope`
3. Run every command in `required_verification` and confirm green
4. Satisfy every item in `closure_requirements`
5. Update the bug file directly (commit) — set `status` to `fixed` with `fix_commit`
6. Coordinate with a **different** agent (or human) to verify and promote `status` to `verified`

## 5. `report_bug` workflow

Step-by-step:

1. Caller calls `report_bug(...)` with title + severity + module + reproduce + files + expected/actual.
2. Server computes `fingerprint = sha256(module + title + reproduce_command)[:16]`.
3. Server scans existing `tests/aistock_validation/bugs/*.json` for the fingerprint.
4. **Hit:** returns `{"deduplicated": true, "existing": {bug_id, status, title, path}, "fingerprint": ...}` and writes nothing.
5. **Miss:** allocates next `BUG-NNN` (max existing ID + 1, zero-padded to 3 digits) and writes the new file with `status: open`, `created_at: now`.
6. Returns `{"deduplicated": false, "bug_id": ..., "path": ..., "fingerprint": ...}`.

The schema written is identical to the one consumed by
`backend/services/validation/finding_store.py::_normalize_bug` — the new file
appears in the next `list_bugs()` call automatically (no backend restart
needed; the store loads on every request).

## 6. Error propagation

All MCP tools fail loudly. There is no silent default:

- HTTP 4xx / 5xx → `RuntimeError` with status + truncated body
- non-JSON response → `RuntimeError`
- response missing `data` envelope → `RuntimeError`
- `report_bug` invalid params → `ValueError`
- filesystem errors → original `OSError` propagated

Clients should surface failures to the agent. The MCP framework auto-formats
exceptions as tool errors back to the caller.

## 7. Backend dependency

The 12 read/action tools require Validation Center backend running on the
configured base URL. Recommended dev wiring:

```bash
# Development backend on dev port 8011 (NOT production 8001)
cd F:/Dev/AIstock
conda activate AIstock
uvicorn backend.main:app --host 127.0.0.1 --port 8011

# Then point the MCP server at it
export AISTOCK_VALIDATION_BASE_URL=http://127.0.0.1:8011/api/v1/validation
python scripts/aistock_mcp_server.py
```

`report_bug` works without the backend — it writes directly to the
filesystem. Other tools will return clear `RuntimeError` messages when the
backend is unreachable.

## 8. Cross-tool review intake

Stage 1 protocol (manual): cross-tool drawer → human transcribes finding into
`tests/aistock_validation/bugs/*.json`.

Stage 3 protocol (this server): cross-tool drawer → reviewer calls
`report_bug` directly from their agent → registry updated immediately. The
fingerprint dedup ensures multiple drawers about the same issue don't
generate duplicate entries.

## 9. Agent identification

Tools that mutate state include an `assigned_agent` / `requested_by` field.
Recommended values:

- `claude_code` — Claude Code instances
- `codex_app` — Codex App instances
- `human` — manual filing
- `mcp_agent` (default for `start_validation_execution.requested_by`) when
  the caller hasn't passed an explicit value

Use these consistently so dashboards can attribute work correctly.

## 10. Future stages

- **Stage 4** (CI/CD): GitHub Actions calls the same Validation Center API
  endpoints directly (no MCP). The MCP server stays for interactive sessions.
- **Stage 5** (DR): not coupled to MCP server.
- **Stage 6** (full regression): `start_validation_execution(plan_key="l0")`
  becomes a callable smoke gate from any agent.

---

**Source of truth**:

- Tools: `scripts/aistock_mcp_server.py`
- Schema: `backend/services/validation/finding_store.py`
- API: `backend/routers/validation.py`
