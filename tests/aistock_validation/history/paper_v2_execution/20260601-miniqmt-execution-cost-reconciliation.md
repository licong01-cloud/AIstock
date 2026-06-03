# MiniQMT Execution Cost Reconciliation Validation Record

Date: 2026-06-01
Worktree: `F:\Dev\AIstock_worktrees\miniqmt-cost-reconciliation-20260601`
Branch: `feature/miniqmt-cost-reconciliation-20260601`

## Scope

Implemented a read-only Paper v2 MiniQMT execution quality and broker-cost reconciliation module for existing broker-authoritative orders and fills. The report is persisted in daily snapshot metadata and as a `MINIQMT_EXECUTION_QUALITY_REPORTED` run event, exposed through a read-only API, included in the live dashboard contract, and rendered on the existing Paper v2 live dashboard page.

This change does not submit orders, cancel orders, restart services, edit `.env`, write production DB data, apply DDL, or modify StrategyPackage/QE/HMM assets. It does not change scheduler/autostart behavior or MiniQMT order execution semantics.

## DESIGN-COMPLIANCE-001 Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| Preserve existing vn.py-style execution asset selection | `backend/execution_algos/vnpy_style/*`; `backend/services/paper_trading_v2/execution/minqmt_live_algo_adapter.py` | Existing `test_minqmt_vnpy_execution_adapter.py` remains green | complete | This slice did not change algorithm semantics |
| Add MiniQMT execution quality report as a reusable Paper v2 module | `backend/services/paper_trading_v2/execution/minqmt_execution_report.py` | `test_minqmt_execution_report_reconciles_broker_fee_and_slippage` | complete | Report is read-only and JSON-safe |
| Persist broker aggregate fee without falsely claiming broker tax breakdown | `minqmt_execution_report.py`; existing MiniQMT fill metadata in `day_runner.py` | `test_minqmt_execution_report_marks_sell_tax_as_estimated_not_broker_confirmed` | complete | `estimated_transfer_fee` remains `None` until broker/source confirms a field |
| Compute cost reconciliation and slippage from persisted orders/fills | `build_minqmt_execution_quality_report` | Unit tests cover broker fee delta, weighted slippage, precision counts | complete | Slippage uses order limit price as intended price when present |
| Surface failed/rejected/unfilled order diagnostic coverage | `minqmt_execution_report.py` | `test_minqmt_execution_report_requires_diagnostics_for_rejected_orders` | complete | Historical pre-fix rows can still be marked as diagnostic gaps |
| Persist report on MiniQMT day-run reconciliation | `PaperTradingDayRunner._persist_minqmt_authority_snapshot` | `test_minqmt_vnpy_twap_lite_can_persist_filled_child_trade` asserts run event and snapshot metadata | complete | Native reconcile with `fill_count_override` marks detail scope when only new fills are materialized |
| Query persisted reports without DDL or production writes | `list_minqmt_execution_quality_reports`; `PaperTradingV2Repository.list_daily_snapshots`; `PaperTradingV2Repository.list_run_events` | `test_minqmt_execution_quality_query_reads_snapshot_and_event_without_duplicate` | complete | De-duplicates snapshot/event records and prefers snapshot metadata as persisted daily source |
| Expose read-only execution-quality API | `GET /api/v1/paper-v2/portfolios/{portfolio_id}/execution-quality`; `frontend/src/lib/paper-v2/api.ts` | `test_minqmt_execution_quality_endpoint_returns_read_only_report`; TypeScript compile | complete | Supports optional `trade_date`, `run_id`, `limit`, `scan_limit` |
| Include execution quality in live dashboard contract | `backend/services/paper_trading_v2/live_dashboard.py`; `frontend/src/lib/paper-v2/types.ts` | `test_live_dashboard_aggregates_signal_minute_execution_and_snapshots`; `npx tsc --noEmit` | complete | Returns warning object when no report exists; no raw JSON primary view |
| Render operator-readable dashboard metrics | `frontend/src/app/paper-v2/portfolios/[portfolioId]/live-dashboard/page.tsx` | TypeScript compile; manual code review against UI-RAWJSON-001 | complete | Uses existing Paper v2 legacy components because this is an existing Paper v2 page |
| Preserve InMemory repository parity for metadata | `backend/services/paper_trading_v2/repository.py` | Query tests read snapshot metadata from `InMemoryPaperTradingV2Repository` | complete | No production DDL; PG path already stores snapshot `metadata` JSONB |
| Preserve production safety | No `.env`, runtime restart, DDL, production DB, protected asset changes | Git diff and validation commands | complete | Production gates are noop |

## Validation Evidence

- `python -m py_compile <changed backend Paper v2 files>` -> passed
- `python -m ruff check <changed backend Paper v2 files>` -> passed
- `python -m pytest -q backend/tests/paper_trading_v2/test_minqmt_execution_report.py backend/tests/paper_trading_v2/test_live_dashboard.py backend/tests/paper_trading_v2/test_minqmt_vnpy_execution_adapter.py -p no:cacheprovider` -> `12 passed`
- `npm ci --prefer-offline --no-audit --no-fund` in `frontend/` -> dependencies installed from existing lockfile; no package or lockfile diff
- `npx tsc --noEmit --pretty false` in `frontend/` -> passed
- `python -m nox -s miniqmt_sim_stub_l3` -> `44 passed`
- `python -m nox -s paper_v2_backend` -> `550 passed, 1 skipped, 2 xfailed`
- `python -m nox -s l0 validation_module_registry_l0` -> success; guardrail output contains existing/baseline or unrelated P2 findings, blocking=0
- `git diff --check` -> passed
- `python scripts/aistock_module_ownership_scan.py --fail-on-unmapped --fail-on-ambiguous <changed files>` -> `files=12, mapped=12, unmapped=0, ambiguous=0`

## Business Outcomes Verified

- MiniQMT broker-reported aggregate fees are visible alongside estimated AIstock fee-model fees.
- Sell-side stamp tax is marked as estimated; transfer fee stays unknown unless a true broker/source field is provided.
- Rejected or unfinished orders show diagnostic coverage and attention rows instead of a false success.
- Live dashboard exposes readable cost, slippage, fill-rate, diagnostic, and warning metrics instead of dumping raw report JSON.
- Report query is read-only and consumes existing daily snapshot metadata/run events only.

## Production Gates

- `production_ddl_gate`: `noop`
- `production_frontend_dependency_gate`: `noop`
- `production_backend_dependency_gate`: `noop`
- Production runtime touched: no
- Production DB touched: no
- `.env` touched: no
- Backend/frontend restart required before production activation: yes, after merge only; no restart was performed in this worktree
