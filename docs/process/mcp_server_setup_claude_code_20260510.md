# Claude Code MCP Setup — AIstock Validation (2026-05-10)

How to wire `scripts/aistock_mcp_server.py` into a local Claude Code session
so the agent can list/query bugs, runs, plans, and trigger nox sessions
without copy-pasting curl output.

## 1. Prerequisites

- AIstock checkout at `F:/Dev/AIstock` (or your local path)
- conda env `AIstock` with `mcp` Python SDK installed:
  ```bash
  conda activate AIstock
  pip install mcp httpx
  ```
- Validation Center backend running on a dev port (default 8011 or 8001 if
  the user has a single-port setup):
  ```bash
  conda activate AIstock
  uvicorn backend.main:app --host 127.0.0.1 --port 8011
  ```
  `report_bug` works without the backend; the other 12 tools need it.

## 2. Configure `.mcp.json` and opt-in via `settings.local.json`

Claude Code reads MCP server definitions from a project-scoped `.mcp.json`
at the repo root, then requires explicit opt-in via the
`enabledMcpjsonServers` array in `settings.local.json`. (The
`settings.local.json` schema does **not** accept `mcpServers` directly —
that key is rejected with a schema validation error.)

### 2.1 Create `F:/Dev/AIstock/.mcp.json`

```json
{
  "mcpServers": {
    "aistock-validation": {
      "command": "C:/Users/lc999/miniconda3/envs/AIstock/python.exe",
      "args": ["F:/Dev/AIstock/scripts/aistock_mcp_server.py"],
      "env": {
        "AISTOCK_VALIDATION_BASE_URL": "http://127.0.0.1:8011/api/v1/validation",
        "AISTOCK_REPO_ROOT": "F:/Dev/AIstock"
      }
    }
  }
}
```

### 2.2 Opt in via `F:/Dev/AIstock/.claude/settings.local.json`

Append (or merge) `enabledMcpjsonServers`:

```json
{
  "permissions": { ... },
  "enabledMcpjsonServers": ["aistock-validation"]
}
```

Notes:

- Use the **absolute path to the conda env's python.exe** so Claude Code
  doesn't pick up the system Python that lacks the `mcp` package.
- `AISTOCK_REPO_ROOT` is required when launching from a different cwd —
  otherwise the server resolves it from the script location.
- For dev-port 8011 setups this points away from production 8001; adjust if
  your single-port setup runs on 8001.
- `.mcp.json` is project-scoped. Either commit it to the repo (with
  host-specific paths replaced by env vars at runtime) or add it to
  `.gitignore` if the absolute paths are personal.

## 3. Reload Claude Code

```text
/mcp reload  (if the slash command exists in this Claude Code build)
```

Otherwise quit and reopen Claude Code so it re-reads
`settings.local.json`. After reload Claude Code should list 13 tools under
`aistock-validation`.

## 4. Smoke test

Ask the agent:

> List the most recent 5 bugs.

Claude Code should call:

```text
aistock-validation/list_bugs (page=1, page_size=5)
```

and return BUG-001 through BUG-005 (or whichever are most recent).

Then:

> Show me the agent context for BUG-023.

Claude Code should call:

```text
aistock-validation/get_bug_agent_context (bug_id="BUG-023")
```

and return the structured `reproduce_command` + `allowed_write_scope` etc.

## 5. Worked example: trigger a nox session

```text
> Run the rl_execution_smoke nox session and tail the log.
```

Claude Code should:

1. `start_validation_execution(plan_key="rl_execution_smoke")` → returns
   `{"job_id": "..."}` 
2. Poll `get_validation_execution_status(execution_id=...)` until status
   transitions to `succeeded` or `failed`
3. `get_validation_execution_log(execution_id=..., tail=200)` → returns the
   tail

## 6. Worked example: file a new bug

```text
> Report a bug: enable_paper returns 500 because the
> strategy_pkg.package_status_event sequence is behind max(event_id).
> Module dev_db_pipeline, severity P2, file
> scripts/dev_db/batch_a_import_real_data.py.
```

Claude Code should call `report_bug(...)`. If a bug with the same
`(module, title, reproduce_command)` fingerprint already exists, the
response is `{"deduplicated": true, "existing": {"bug_id": ..., ...}}`,
which the agent should report back rather than treat as success.

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `aistock-validation` not listed in `/mcp` | settings.local.json typo | re-read JSON syntax; reload |
| `ModuleNotFoundError: mcp` | wrong python | absolute path to conda env python.exe |
| `RuntimeError: HTTP 0` / connection refused | backend not running | start uvicorn on configured port |
| All read tools fail with HTTP 404 | wrong base URL | verify `/api/v1/validation/health` returns JSON in browser |
| `report_bug` says `deduplicated=true` unexpectedly | identical title+module+reproduce | adjust reproduce_command to differentiate, or update the existing bug instead of filing a new one |

## 8. Tear-down

Quit Claude Code or remove the `aistock-validation` entry from
`settings.local.json` and reload. The MCP server process exits when the
client disconnects.
