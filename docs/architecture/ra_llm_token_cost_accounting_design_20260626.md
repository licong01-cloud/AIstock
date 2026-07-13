# Research Assistant LLM Token And Cost Accounting Design

Version: 2026-06-26 v1
Tier: F1 standard single-module feature
Owner: research_assistant

## Background

Research Assistant already calls LiteLLM through `ResearchAssistantLlmClient.complete()` and stores trace rows in `assistant_trace_events`. The current trace path only keeps `cost_json={"usage": ...}` and drops LiteLLM usage objects that are not plain dictionaries. Operators therefore cannot answer basic questions such as how many tokens a chat turn used, whether token counts were provider-reported or estimated, and what LiteLLM thinks the USD cost was.

This feature adds an append-only Research Assistant LLM usage ledger and a read-only API/UI surface. It is observability only: it must not change routing, prompts, approval, grounding, model selection, or tool execution semantics.

## Scope

In scope:

- Add `assistant_llm_usage_events` as the authoritative append-only ledger for one row per LLM provider call.
- Add idempotent forward and rollback migration files plus bootstrap schema alignment.
- Normalize LiteLLM usage from dictionaries, Pydantic-style objects, and plain objects; estimate explicitly only when provider usage is missing.
- Calculate cost with LiteLLM when pricing is available and store explicit unavailable or failed reason codes otherwise.
- Record per-call usage events for the initial chat call and ReAct model turns, then write a trace `cost_json` summary cache.
- Add read-only APIs for usage events and aggregated summaries.
- Update the Research Assistant audit trace UI to show human-readable token and cost summaries instead of empty raw JSON.
- Add offline unit/API/schema/frontend-friendly tests without connecting to production DB or starting production services.

## Non-Goals

- No budget enforcement, token limit, provider throttling, or chat blocking based on cost.
- No prompt, full message, API key, or user private text persistence in the usage table.
- No change to Research Assistant prompt, routing, approval, grounding, forecasting discipline, or MCP execution behavior.
- No production DDL execution by Codex; production apply remains a separate controlled step.
- No daily rollup table in this phase; the first release aggregates from ledger rows.

## Design Acceptance Index

- F-001: Create idempotent `011_llm_usage_accounting` forward/rollback migrations and align `init_research_assistant_schema_20260521.py` with table, constraints, indexes, and comments.
- F-002: Add repository support for `assistant_llm_usage_events` with JSON field adaptation and in-memory test support.
- F-003: Normalize LiteLLM usage without silently converting non-dict usage to `{}`; record source/status/reason for provider-reported, estimated, unavailable, and failed states.
- F-004: Calculate LiteLLM cost when possible and record explicit `cost_status` / `cost_reason_code` when pricing or calculation is unavailable.
- F-005: Record one usage event per LLM call and keep `assistant_trace_events.cost_json` as a summary cache with event refs, not as the authority.
- F-006: Preserve ReAct multi-turn accounting: each model turn has its own usage payload and the final chat trace summary equals the per-turn aggregate.
- F-007: Add read-only APIs `GET /research-assistant/llm-usage/events` and `GET /research-assistant/llm-usage/summary` with filters by trace/task/conversation/model/date.
- F-008: Show token/cost summary in the audit Trace UI with explicit estimated/unavailable labels and raw data available only in details.
- F-009: Preserve safety and privacy: no prompt text capture, no silent errors, no production DB connection, no production DDL apply, no service start/restart.

## Implementation Plan

1. Add migration files under `backend/db/migrations/ra_upgrade/` and update the Research Assistant bootstrap schema.
2. Extend `backend/services/research_assistant/repository.py` `TABLES` with `llm_usage_events` and JSON columns.
3. Add usage/cost accounting helpers in `backend/services/research_assistant/service.py` close to the LiteLLM client wrapper so fake clients and ReAct turns can reuse the same payload shape.
4. Extend `LlmCallResult` and `ModelTurn` with an optional `usage_event` dictionary while keeping existing tests that only pass `usage` compatible.
5. Record usage events in `ResearchAssistantService.chat_turn()` after the trace row exists, then update `cost_json` summary with ledger refs.
6. Add service methods and router endpoints for event listing and summary aggregation.
7. Extend `frontend/src/lib/research-assistant/api.ts` and `frontend/src/app/research-assistant/audit/TraceSection.tsx` to show ledger-backed totals.
8. Add tests for normalization, no-silent reason codes, migration/schema contract, API list/summary, ReAct aggregation, and UI-readable labels where practical.

## Verification Plan

Required local validation before PR readiness:

- `rtk proxy python scripts/aistock_feature_workflow.py validate --design docs/architecture/ra_llm_token_cost_accounting_design_20260626.md --tier F1`
- Targeted pytest for new Research Assistant usage accounting tests.
- `rtk proxy python -m nox -s l0`
- `rtk proxy python -m nox -s research_assistant_backend`
- `rtk proxy python -m nox -s research_assistant_mcp_contract`
- `rtk proxy python -m nox -s ra_phase7_full_accept`
- `rtk proxy git diff --check`
- `rtk proxy ruff check` on changed Python files.
- Frontend checks if UI files change: `rtk proxy npm exec tsc --noEmit --incremental false`, `rtk proxy npm run lint`, and `rtk proxy npm run build` from `frontend/`.

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/db/migrations/ra_upgrade/011_llm_usage_accounting.sql`; rollback; `backend/db/init_research_assistant_schema_20260521.py` | schema contract and migration text tests | ready | none |
| F-002 | `backend/services/research_assistant/repository.py` | repository in-memory list/create tests | ready | none |
| F-003 | `backend/services/research_assistant/service.py` usage helpers | dict/object/missing usage unit tests | ready | none |
| F-004 | `backend/services/research_assistant/service.py` cost helpers | LiteLLM cost success and unavailable reason tests | ready | none |
| F-005 | `backend/services/research_assistant/service.py` chat trace recording | chat turn test asserts ledger row plus trace summary refs | ready | none |
| F-006 | `backend/services/research_assistant/service.py`; `backend/services/research_assistant/react_grounding.py` | ReAct multi-turn aggregate test | ready | none |
| F-007 | `backend/routers/research_assistant.py`; service list/summary methods | API tests for filters and totals | ready | none |
| F-008 | `frontend/src/lib/research-assistant/api.ts`; `frontend/src/app/research-assistant/audit/TraceSection.tsx` | frontend type/lint/build checks and readable-label assertions if present | ready | none |
| F-009 | design, migrations, service accounting guards | diff review and validation report with production gates | ready | none |

## Risks

- LiteLLM provider response shapes differ by provider. Mitigation: normalize `dict`, `model_dump()`, `dict()`, and attribute objects; store explicit failure reason when shape is unsupported.
- LiteLLM pricing may not include the operator's exact model. Mitigation: token usage is still recorded and cost is marked unavailable with `pricing_missing_or_unrecognized_model` rather than invented.
- Usage accounting writes could fail after the user answer is generated. Mitigation: chat should continue, but trace/task evidence must record `llm_usage_accounting_failed` with a concrete reason.
- The ledger may grow quickly. Mitigation: per-call rows are indexed by completed time, trace, task, conversation, and model; rollups can be added by a separate design if query load requires it.
- UI may confuse estimated cost with bills. Mitigation: labels must distinguish provider-reported token counts, LiteLLM-estimated tokens, and unavailable pricing.

## Production Gates

- `production_ddl_gate`: `pending` because this feature adds `assistant_llm_usage_events`; Codex must not apply production DDL.
- `production_frontend_dependency_gate`: `noop` because no new frontend dependency is planned.
- `production_backend_dependency_gate`: `noop` because LiteLLM is already an existing backend dependency.

Production apply runbook after merge, executed only by the user or established migration job:

```bash
psql "$AISTOCK_PROD_DATABASE_URL" --single-transaction -v ON_ERROR_STOP=1 -f backend/db/migrations/ra_upgrade/011_llm_usage_accounting.sql
```

Rollback if required, executed only by the user or established migration job:

```bash
psql "$AISTOCK_PROD_DATABASE_URL" --single-transaction -v ON_ERROR_STOP=1 -f backend/db/migrations/ra_upgrade/011_llm_usage_accounting.rollback.sql
```

No production DB connection, DDL apply, or service restart is part of this implementation step.
