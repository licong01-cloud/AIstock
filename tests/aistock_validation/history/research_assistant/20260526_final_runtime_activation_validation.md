# Research Assistant Final Runtime Activation Validation - 2026-05-26

## Scope

- Module: Research Assistant
- Validation phase: final post-restart runtime activation validation after `aa6ebebd`
- Backend restart owner: user
- Codex boundary: validation only; Codex did not start, stop, or restart backend
- Backend runtime process observed: `python.exe` on port `8001`, PID `23068`, start time `2026-05-26 00:18:25` Asia/Shanghai
- Git state before validation: `HEAD=aa6ebebd`, `origin/main=aa6ebebd`, clean worktree
- Production DB target verified: `127.0.0.1:5432/aistock` (password loaded from `.env`, not printed)

## Runtime Health

| Check | Result |
|---|---|
| `GET /api/v1/research-assistant/health` | HTTP 200 |
| Service status | `ok` |
| Phase | `mcp_skill_execution_closure` |
| Repository status | `ok` |
| Repository tables | `40/40` present |
| Catalog readiness | `true` |

## Catalog Readiness

| Catalog | Present | Ready |
|---|---:|---|
| skills | 5 | true |
| mcp_servers | 4 | true |
| mcp_tools | 9 | true |
| capabilities | 10 | true |
| model_profiles | 1 | true |
| routing_policies | 1 | true |
| prompt_nodes | 15 | true |
| prompt_activations | 1 | true |
| runtime_config_activations | 1 | true |

`GET /api/v1/research-assistant/catalogs/readiness` returned `ready=true`, `status=ready`, `missing_catalogs=[]`.

## Capability Registry

`GET /api/v1/research-assistant/capabilities?status=approved&limit=20` returned `total=10`.

Approved capabilities:

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

Capability sync checks:

| API | Result |
|---|---|
| `POST /capabilities/sync` with `apply=false` | HTTP 200; `dry_run=true`; `source_count=10`; `applied_count=0`; all `unchanged` |
| `POST /capabilities/sync` with `apply=true` | HTTP 200; `dry_run=false`; `source_count=10`; `applied_count=0`; all `unchanged` |

Runtime config returned by capability sync:

```json
{"max_tools_per_server":500,"timeout_seconds":30,"require_checksum":true}
```

## Task Idempotency Runtime Fix Verification

This proves that the `repository.py` `idempotency_key` filter allowlist fix from `aa6ebebd` is loaded in the restarted backend runtime.

| Step | Result |
|---|---|
| First `POST /tasks` with idempotency key | HTTP 200; task `rat_5ba4f44104c54682b5a15c427c9a964d` |
| Second `POST /tasks` with same idempotency key | HTTP 200; task `rat_5ba4f44104c54682b5a15c427c9a964d` |
| Same task id | true |
| Runtime allowlist loaded | true |

Idempotency key used: `post-restart-final-idem-20260526-1779726410`.

## Action Proposal Dry-Run Smoke

Validated `qe.create_experiment_draft` with valid draft payload.
No QE materialize, QE run, production trading action, or Paper v2 action was performed.

| Step | Result |
|---|---|
| `POST /actions/propose` | HTTP 200; action `actprop_e85776ffdd2a4030bde6d7276d9e1490` |
| `POST /actions/{id}/confirm` | HTTP 200; confirmation text `CONFIRM_QE_DRAFT` |
| `POST /actions/{id}/preflight` | HTTP 200; proposal status `preflight_passed` |
| `POST /actions/{id}/execute` with `dry_run=true` | HTTP 200; `status=dry_run`; `executed=false` |
| `GET /actions/{id}/events` | HTTP 200; task events `2`, MCP tool events `1`, trace events `1` |

Preflight checks:

- `schema`
- `fixed_seed`
- `draft_only`

## Production DDL Gate

`production_ddl_gate=applied_and_verified`

Verified production DB state:

| Check | Result |
|---|---|
| `assistant_prompt_nodes.ck_apn_category` includes `context` | true |
| approved capabilities | 10 |
| enabled prompt nodes | 15 |
| active prompt activations | 1 |
| active runtime config activations | 1 |

## Acceptance Result

- Backend runtime has loaded the latest Research Assistant code from `aa6ebebd`.
- Catalogs and runtime config are active.
- Capability registry is approved and synchronized.
- Task idempotency runtime fix is active.
- Action Proposal confirm/preflight/dry-run closure works with no real execution side effects.
- This stage is ready to serve as the baseline for the next independent Research Assistant development branch.
