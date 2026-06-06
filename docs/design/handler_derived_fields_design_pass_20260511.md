# QE Archive Handler-Derived Fields Design Pass

Version: v1.0
Date: 2026-05-29
Linked issue: BUG-011 / GitHub Issue #322
Module: `qe_archive`

## 1. Executive Decision

`PaperV2ArchiveHandler` owns several archive-only or handler-derived fields. These fields are not direct source-column copies. They are derived from `paper_v2.*` source rows, event payloads, or archive contract rules.

This design pass records those rules in one place so future Codex, Claude Code, and human changes do not need to rediscover old design notes or invent inconsistent field-by-field behavior.

Decisions:

- `paper_v2.*` source rows are read-only for archive handlers.
- Derived fields must be deterministic, replayable, and testable.
- Missing or weak source data must remain visible as `NULL`, `missing`, `low_coverage`, or explicit payload evidence.
- Source enum drift must fail fast instead of silently falling back.
- BIGINT source event/audit ids canonicalize to raw decimal TEXT strings, without table prefixes.
- `paper_v2_config_change_audit.change_type` is the source action enum. Subject/category queries must inspect JSON payloads unless a future approved migration adds `subject_type`.
- BUG-008 stays open: current `actual_bar_count` uses `paper_v2.intraday_snapshots` count as a coverage heuristic, not complete minute-bar truth.

## 2. Non Goals

- No production DDL.
- No production runtime restart.
- No change to existing `qe_archive` table names, primary keys, or unique keys.
- No attempt to close BUG-008 in this design pass.
- No implicit `subject_type` migration for config audit.

## 3. Common Rules

### 3.1 Deterministic

The same source row, event payload, and archive schema must produce the same archive value. Rules must not depend on current time, external APIs, LLM output, or non-deterministic ordering.

### 3.2 Fail Fast

If a source enum value is outside the known archive contract, the handler must raise and roll back the current event transaction. `NOOP` is allowed only for explicit lifecycle states such as source row not found or run not archived yet.

### 3.3 Missing Is Evidence

If source data is not strong enough to prove a value, the archive should store `NULL`, `missing`, `low_coverage`, or raw payload evidence. It must not fabricate a normal-looking value.

### 3.4 ID Canonicalization

BIGINT ids stored in TEXT archive natural-key columns use raw decimal strings:

```text
9223372036854775807 -> "9223372036854775807"
```

No table prefix, float conversion, scientific notation, or locale formatting is allowed.

Applies to:

- `paper_v2_session_event.event_id`
- `paper_v2_order_event.event_id`
- `paper_v2_config_change_audit.audit_id`
- `paper_v2_reset_audit.audit_id`

Current status:

- session event, config audit, and reset audit already cast explicitly.
- order event explicit cast and int64 boundary coverage remain tracked by BUG-009 / GitHub Issue #320.

## 4. Field Matrix

| Archive field | Source inputs | Rule | Missing or drift behavior | Validation |
| --- | --- | --- | --- | --- |
| `paper_v2_cash_ledger.entry_type` | `side`, `notional`, `fee`, `cash_delta` | `fee>0 and notional=0` -> `fee`; `SELL and cash_delta>0` -> `fill_credit`; `BUY and cash_delta<0` -> `fill_debit`; empty side with positive/negative cash delta -> `deposit`/`withdraw`; otherwise `adjustment` | Unknown but valid numeric combinations become `adjustment` with raw amounts preserved | `backend/tests/qe_archive/test_synthesize.py::TestCashLedgerEntryType` |
| `paper_v2_cash_ledger.amount` | `cash_delta`, `notional` | Prefer `cash_delta`, then `notional`, then `0` | No amount source keeps `0`; entry type and raw row still provide context | handler integration |
| `paper_v2_cash_ledger.balance_after` | source `balance_after` | Copy source value | `NULL` if source is missing | handler integration |
| `paper_v2_reset_audit.reset_type` | `rerun_policy`, `deleted_counts` | policy contains `full/all` -> `full_reset`; positions+cash/fills/orders -> `partial_reset`; positions only -> `position_only`; cash/fills/orders only -> `cash_only`; config policy or no deletes -> `config_only`; fallback `partial_reset` | No source row means no archive row; unknown combinations keep deterministic fallback | `backend/tests/qe_archive/test_synthesize.py::TestResetAuditResetType` |
| `paper_v2_reset_audit.reset_reason` | `rerun_policy` | Prefer source `rerun_policy`, else `synthesized_from_source` | auditable fallback string | handler integration |
| `paper_v2_reset_audit.snapshot_before_json` | `rerun_policy`, `start_date`, `end_date`, `deleted_counts`, `context`, `status` | Preserve reset-before context as JSON | Missing fields become JSON nulls | handler integration |
| `paper_v2_session_day.actual_bar_count` | `run_id`, `trade_date`, `paper_v2.intraday_snapshots` | Count source intraday snapshots for the same run/date | `NULL` if `trade_date` is missing; BUG-008 tracks stronger truth | `TestSessionDayDataQuality` plus handler integration |
| `paper_v2_session_day.data_quality` | `expected_bar_count`, `actual_bar_count` | actual `NULL` -> `missing`; no expected and actual>0 -> `ok`; actual>=expected -> `ok`; actual>=50% expected -> `partial`; otherwise `low_coverage` | Weak coverage remains visible | `backend/tests/qe_archive/test_synthesize.py::TestSessionDayDataQuality` |
| `dim_paper_v2_portfolio.broker_backend` | `frozen_manifest_json.broker_backend`, `data_source` | manifest value wins; `data_source` contains `MINIQMT` -> `miniqmtsim`; otherwise `localsim` | fallback is explicit and source `data_source` is preserved | handler integration |
| `paper_v2_run.broker_backend` | portfolio lookup | Re-derive from portfolio source so run table can satisfy NOT NULL | missing portfolio should NOOP or fail by lifecycle contract | handler integration |
| `paper_v2_position_snapshot.unrealized_pnl` | `quantity`, `avg_cost`, `market_value` | `market_value - quantity * avg_cost` | `NULL` if an input is missing or not numeric | handler integration |
| `paper_v2_error.error_class` | `error_code`, `message` | `BROKER*` -> `BrokerBackendError`; `PACKAGE*` or `STRATEGY*` -> `StrategyPackageError`; `VALIDATION*` or `VALIDATE*` -> `ValidationError`; else `GenericError` | Unknown codes are preserved and classified as generic | `backend/tests/qe_archive/test_synthesize.py::TestDeriveErrorClass` |
| `paper_v2_run_event.event_id` | `run_id`, `event_seq` | `{run_id}_{event_seq}` | missing `event_seq` violates source contract | handler integration |
| `paper_v2_session_event.event_id` | source `event_id` | raw decimal TEXT string | no prefix, no float conversion | handler integration |
| `paper_v2_order_event.event_id` | source `event_id` | raw decimal TEXT string | final explicit cast and int64 test in BUG-009 | BUG-009 |
| `paper_v2_config_change_audit.audit_id` | source `audit_id` | raw decimal TEXT string | payload lookup may cast numeric string for source query | `TestConfigChangedEvent` |
| `paper_v2_config_change_audit.change_type` | source `change_type` | preserve action enum: `CREATE`, `ACTIVATE`, `DEACTIVATE`, `MODIFY` | unknown action fails fast; subject queries use JSON payloads | BUG-010 plus `TestConfigChangedEvent` |

## 5. Why BUG-008 Remains Open

BUG-008 is not only about whether the handler can write `actual_bar_count`. It is about whether the value is strong enough to represent minute-bar truth.

Current implementation:

```text
actual_bar_count = COUNT(*) FROM paper_v2.intraday_snapshots
                   WHERE run_id = <run_id> AND trade_date = <trade_date>
```

This is useful as archive coverage evidence, but it has known limitations:

- Sparse snapshot capture can under-count true minute bars.
- Future snapshot granularity changes could make the count incompatible with actual bar truth.

Therefore BUG-011 can be fixed by documenting the rules and residual risk, while BUG-008 remains open until source schema or a verified minute-bar feed provides stronger truth.

## 6. Relationship To BUG-009 And BUG-010

### BUG-009

This design pass fixes the decision part: raw decimal TEXT string is the canonical id policy. BUG-009 still tracks implementation completion:

- explicit cast for `paper_v2_order_event.event_id`;
- int64 max boundary test;
- idempotent replay test.

### BUG-010

This design pass fixes the decision part: `change_type` is an action enum, not a subject enum. The handler already validates and writes action values. If future reporting needs `subject_type`, it must be a separate migration and review.

## 7. Acceptance Matrix

| ID | Requirement | Evidence |
| --- | --- | --- |
| HDF-001 | Known handler-derived fields have source inputs and rules | Section 4 |
| HDF-002 | Missing data is not hidden by normal-looking defaults | Sections 3.3 and 4 |
| HDF-003 | BUG-008 residual risk stays explicit | Section 5 |
| HDF-004 | BUG-009 id policy is explicit | Sections 3.4 and 6 |
| HDF-005 | BUG-010 action-vs-subject decision is explicit | Section 6 |
| HDF-006 | Pure helper rules map to tests | `backend/tests/qe_archive/test_synthesize.py` |
| HDF-007 | Handler write paths map to integration tests | `backend/tests/qe_archive/test_paper_v2_archive_handler.py` |

## 8. Production Gates

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`

This design pass changes documentation and BUG registry metadata only. It does not touch production runtime, production DB, or dependencies.
