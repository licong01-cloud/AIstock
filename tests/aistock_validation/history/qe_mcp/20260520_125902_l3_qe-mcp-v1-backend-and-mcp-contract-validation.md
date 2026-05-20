# QE MCP L3 Validation - BUG-079

- Module: qe_mcp
- Level: L3
- Date: 2026-05-20T12:59:02
- Branch: bug/BUG-079-qe-archive-design-compliance
- Base commit before fix: 7affcae
- Operator: codex-app

## Scope

- Changed files: QE Archive MCP wrapper already had explicit selection tools; this run verifies it remains compatible with backend/manual-ingestion changes.
- Impacted flows: `qe_archive_backfill_selection_preview`, `qe_archive_backfill_selection_execute_confirmed`, `qe_archive_get_source_status`.
- Business goal: ensure MCP users can preview/write explicit experiment/task/loop selections without scan-limit ambiguity.
- Out of scope: production MCP server restart.
- Protected assets reviewed: no production service restart or DB write was performed by validation.

## Environment

- Backend port: validation port check 8011 only, no production restart.
- Frontend port: QE template UI validation port 3011.
- Browser/headless: Playwright Chromium headless for QE template regression.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| MCP wrapper import | QE MCP scripts start without import/runtime dependency errors | `python -m nox -s qe_mcp_backend` | Pass |
| Explicit ID preview | Archive MCP posts explicit ids to `/qe-archive/backfill` | `backend/tests/test_aistock_qe_mcp_servers.py` | Pass |
| Confirmed write guard | Archive selection execute requires `QE_ARCHIVE_WRITE` before HTTP | `backend/tests/test_aistock_qe_mcp_servers.py` | Pass |
| Source status | Archive MCP posts explicit ids to `/qe-archive/source-status` | `backend/tests/test_aistock_qe_mcp_servers.py` | Pass |
| L3 aggregate | QE MCP backend + archive backend + template UI gates pass | `python -m nox -s qe_mcp_l3` | Pass |

## Commands

```bash
python -m nox -s qe_mcp_backend
python -m nox -s qe_mcp_l3
```

## Evidence

- `qe_mcp_backend`: 29 passed.
- `qe_mcp_l3`: successful aggregate run; notified `qe_mcp_backend`, `qe_archive_backend`, and `qe_template_ui` all passed.
- Guardrail scan reported only medium raw JSON review findings in existing QE template paths; no high-severity blocker.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| None | N/A | N/A | N/A |

## Result

- Final status: Pass
- Remaining risks: active MCP client list in this current Codex session may not refresh tool schemas until runtime restart, but script-level and backend contracts pass.
- Need production backend restart: no
- Need production MCP/server restart: user-controlled later
