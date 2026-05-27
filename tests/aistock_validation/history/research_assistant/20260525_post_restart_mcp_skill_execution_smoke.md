# Research Assistant Post-Restart MCP/Skill Runtime Smoke - 2026-05-25

## Scope

- Module: Research Assistant
- Phase: post-restart runtime validation after user-owned backend restart
- Backend runtime: user restarted backend; Codex did not stop/start/restart backend
- Production DB target verified: `127.0.0.1:5432/aistock` (password loaded from `.env`, not printed)
- Production impact boundary: Research Assistant catalog/DDL validation only; no Paper v2, QE production run, materialize, or trading runtime action

## Runtime Status

| Check | Result |
|---|---|
| Backend `/api/v1/research-assistant/health` | HTTP 200 |
| Service status | `ok` |
| Phase | `mcp_skill_execution_closure` |
| Repository status | `ok` |
| Repository tables | `40/40` present |
| Catalog readiness | `ready=true`, `missing_catalogs=[]` |

## Production DDL Follow-Up

Initial post-restart catalog seed was blocked by a stale production DB check constraint on `assistant_prompt_nodes.category`.
The existing `ck_apn_category` did not include the newer `context` category, while the current seed imports context-budget prompt nodes.

Applied/verified fix:

- Updated committed DDL path to refresh `ck_apn_category` idempotently for existing DBs.
- Applied the constraint refresh to production DB.
- Verified `pg_get_constraintdef(ck_apn_category)` includes `context`.
- Verified catalog counts after seed:
  - approved capabilities: `10`
  - enabled prompt nodes: `15`
  - active prompt activations: `1`
  - active runtime config activations: `1`

`production_ddl_gate=applied_and_verified`

## Catalog and Capability Validation

| API | Result |
|---|---|
| `POST /catalogs/seed` | HTTP 200; catalog version `research_assistant_mcp_skill_execution_20260525` |
| `GET /catalogs/readiness` | HTTP 200; all 9 catalog checks ready |
| `POST /capabilities/sync` with `apply=false` | HTTP 200; `dry_run=true`, `source_count=10`, `applied_count=0`, all `unchanged` |
| `POST /capabilities/sync` with `apply=true` | HTTP 200; `dry_run=false`, `source_count=10`, `applied_count=0`, all `unchanged` |
| `GET /capabilities?status=approved&limit=20` | HTTP 200; `total=10` |

Approved capability keys:

- `rdagent.analyze_task`
- `factor.analyze_library`
- `memory.write_candidate`
- `issue.sync_github`
- `issue.create_candidate`
- `qe.analyze_result`
- `qe.run_experiment`
- `qe.materialize_template`
- `qe.validate_template`
- `qe.create_experiment_draft`

Runtime config returned by capability sync:

```json
{"max_tools_per_server":500,"timeout_seconds":30,"require_checksum":true}
```

## Action Proposal Dry-Run Smoke

Validated safe, non-production Action Proposal path using `qe.create_experiment_draft`.
No QE materialize or QE run was requested or performed.

| Step | Result |
|---|---|
| `POST /tasks` | HTTP 200; task `rat_e198e0bb8ef3499a85cb51217d59ef02` |
| `POST /actions/propose` | HTTP 200; action `actprop_92449e766c3c4ef5807ee0c5e3f2a0b8` |
| `POST /actions/{id}/confirm` | HTTP 200 with `CONFIRM_QE_DRAFT` |
| `POST /actions/{id}/preflight` | HTTP 200; status `preflight_passed` |
| `POST /actions/{id}/execute` with `dry_run=true` | HTTP 200; `status=dry_run`, `executed=false` |
| `GET /actions/{id}/events` | HTTP 200; task events `2`, MCP tool events `1`, trace events `1` |

Preflight checks:

- `schema`
- `fixed_seed`
- `draft_only`

Dry-run execution result:

```json
{"status":"dry_run","executed":false,"human_cards_count":1}
```

## Additional Runtime Finding Fixed in Code

The live API rejected `POST /tasks` when an `idempotency_key` was provided because the production repository lookup used `find_one("tasks", {"idempotency_key": ...})`, but `idempotency_key` was not allowed in the task repository search/filter metadata.

Fix:

- Added `idempotency_key` to the task repository filter/search allowlist.
- Verified with API smoke and existing test coverage.

## Validation Commands

```text
rtk python -m pytest backend/tests/research_assistant/test_schema_contract.py -q
# 2 passed in 0.64s

rtk python -m pytest backend/tests/research_assistant/test_execution_closure.py -q
# 8 passed in 0.81s

rtk python -m pytest backend/tests/research_assistant/test_service.py -q
# 23 passed in 1.36s

rtk python -m pytest backend/tests/research_assistant/test_api.py::test_research_assistant_api_phase1_smoke -q
# 1 passed in 13.74s

rtk python -m compileall backend/services/research_assistant/repository.py backend/db/init_research_assistant_schema_20260521.py
# passed

rtk git diff --check
# passed
```

## Acceptance Result

- Backend restart was performed by the user, not Codex.
- New Research Assistant route set and phase are active after restart.
- Production DB schema gate is applied and verified.
- Catalogs, prompt activation, runtime config activation, and capability registry are ready.
- Action Proposal confirm/preflight/dry-run flow works without real MCP execution side effects.
- No backend restart was performed by Codex after the code fixes; a later backend restart by the user is needed for the repository `idempotency_key` allowlist code fix to affect the running process.
