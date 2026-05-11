# Codex App MCP Setup — AIstock Validation (2026-05-10)

How to wire `scripts/aistock_mcp_server.py` into a local Codex App instance
so the agent can list/query bugs, runs, plans, and trigger nox sessions
through the same MCP server Claude Code uses.

## 1. Prerequisites

Same as Claude Code:

- AIstock checkout at `F:/Dev/AIstock` (Codex's worktree path differs but
  this server is invoked by absolute path so the worktree location does
  not matter)
- conda env `AIstock` with `mcp` SDK installed:
  ```bash
  conda activate AIstock
  pip install mcp httpx
  ```
- Validation Center backend on dev port (recommended 8011):
  ```bash
  uvicorn backend.main:app --host 127.0.0.1 --port 8011
  ```

## 2. Configure Codex agent

Codex App reads MCP server configurations from `.codex/agents/<agent>.json`
(per-agent) or a global `.codex/config.json`. Add the `aistock-validation`
entry under `mcpServers`:

```json
{
  "mcpServers": {
    "aistock-validation": {
      "command": "C:/Users/lc999/miniconda3/envs/AIstock/python.exe",
      "args": ["F:/Dev/AIstock/scripts/aistock_mcp_server.py"],
      "env": {
        "AISTOCK_VALIDATION_BASE_URL": "http://127.0.0.1:8011/api/v1/validation",
        "AISTOCK_REPO_ROOT": "F:/Dev/AIstock"
      },
      "transport": "stdio"
    }
  }
}
```

Place this in whichever agent config Codex is using for AIstock work
(typically the agent that handles backend / validation tasks). If Codex's
config schema differs in the build you're running, check Codex docs for the
exact key name (`mcp_servers` vs `mcpServers` vs `tools`).

## 3. Reload Codex

```text
codex reload   # or quit + relaunch the desktop / CLI session
```

After reload Codex's tool listing should include 13 tools prefixed with
`aistock-validation/`.

## 4. Same smoke tests as Claude Code

Codex can issue identical tool calls. The reference walk-through:

```text
> List bugs filed by codex_app
```

→ `list_bugs(agent="codex_app")` returns BUG-023 (and any others Codex
filed via report_bug after this server lands).

```text
> File a bug: <description>
```

→ `report_bug(...)` writes a new entry under
`tests/aistock_validation/bugs/`.

## 5. Cross-tool review workflow

Recommended adoption (proposed in cross-tool drawer 2026-05-10):

1. Codex finds an issue during review
2. Codex calls `aistock-validation/report_bug(title, severity, module,
   files, reproduce_command, expected, actual, fix_owner="claude_code"
   if Claude should fix or "codex_app" if Codex will, related_drawer=...)`
3. Server writes `tests/aistock_validation/bugs/<file>.json` (or returns
   `deduplicated=true` pointing at the existing BUG-NNN)
4. Codex includes `BUG-NNN` in its drawer reply so the strategy session can
   route the fix
5. Fix agent calls `get_bug_agent_context(bug_id)` to bootstrap repair

This replaces the manual Stage 1 / Stage 2 transcription path while
preserving drawer-based collaboration as the human-readable record.

## 6. Production safety

- `AISTOCK_VALIDATION_BASE_URL` must point to a **dev port** (8011) or your
  single-port dev setup. Never set it to a remote URL or production
  hostname.
- The MCP server makes only loopback HTTP requests — no external network.
- `start_validation_execution` is allowlist-gated by the backend
  (`runner_enabled: true` in `tests/aistock_validation/catalog/test_plans.yaml`),
  so Codex cannot trigger arbitrary commands.
- `report_bug` is the only state-changing tool not gated by the backend; it
  writes filesystem files only inside `tests/aistock_validation/bugs/`.

## 7. Troubleshooting

Same matrix as `mcp_server_setup_claude_code_20260510.md` §7. The most
common Codex-specific issue is config-key naming
(`mcpServers` vs `mcp_servers`); if Codex doesn't pick the entry up,
search Codex docs for the exact field.

## 8. Boundary confirmations Codex agents should record

When a Codex run uses MCP tools that touched the registry, include these in
the next status drawer:

```
mcp_tools_used: [list_bugs, report_bug, ...]
report_bug_results: [BUG-NNN, ...]
production_8001_touched: false
production_db_writes: false
```

This keeps the cross-tool ledger explicit even when no SQL ran.
