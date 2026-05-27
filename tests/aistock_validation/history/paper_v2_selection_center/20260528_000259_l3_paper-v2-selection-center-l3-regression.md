# BUG-131 Paper v2 Selection Center / MiniQMT UI L3 validation record

- Module: `paper_v2_selection_center`
- Level: `L3` with targeted UI smoke
- Date: 2026-05-28 Asia/Shanghai
- Branch: `bug/BUG-131-paper-v2-miniqmt-20260527`
- Pre-commit base: `55255d1d chore(issue): close BUG-130 after merge (#251)`
- Issue: `BUG-131`, GitHub Issue `#252`
- Operator: Codex / lc999 workstation

## Scope

- Changed files:
  - `frontend/src/app/paper-v2/selection/page.tsx`
  - `frontend/src/app/paper-v2/miniqmt-sim/page.tsx`
  - `frontend/tests/paper-v2/paper-v2-real-flow.spec.ts`
  - `tests/aistock_validation/bugs/.bug_id_allocator.json`
  - `tests/aistock_validation/bugs/20260527_BUG-131-paper-v2-miniqmt.json`
  - `tests/aistock_validation/history/paper_v2_selection_center/20260528_000259_l3_paper-v2-selection-center-l3-regression.md`
- Impacted flows: Paper v2 Selection Center package selector/history list; MiniQMT SIM portfolio list, position table, trade table.
- Business goal: make the operator UI readable and batch-operable without changing trading authority, DB schema, broker behavior, or production runtime.
- Out of scope: backend trading logic, MiniQMT broker API contract, production service restart, production DB writes, `.env` changes.
- Protected assets reviewed: no StrategyPackage frozen manifests, QE artifacts, HMM snapshots, execution policies, ledger assets, or model files were modified.

## Design / Acceptance Compliance Matrix

| Acceptance item | Implementation refs | Evidence | Status |
|---|---|---|---|
| Strategy package selector and controls display vertically; selector at top | `frontend/src/app/paper-v2/selection/page.tsx` wraps top area as column and renders selector before controls | Targeted Playwright smoke compares selector checkbox y-position above `selection-mode` | PASS |
| Selection history supports selecting every record on the current page | `selection-history-select-page`, `setCurrentPageRunSelection(true)` | Targeted smoke selects all visible history checkboxes and verifies checked count equals page count | PASS |
| Selection history supports clearing current page selection | `selection-history-clear-page`, `setCurrentPageRunSelection(false)` | Targeted smoke clears and verifies checked count is 0 | PASS |
| MiniQMT positions explain missing cost/market value and source semantics | `miniqmt-sim/page.tsx` NoticePanel and cost/market value helpers | UI smoke verifies MiniQMT page loads; build/typecheck passed | PASS |
| MiniQMT position quantity 0 is explained without overclaiming same-day liquidation | position NoticePanel and quantity cell hint `可能已清仓` | Code review and build; text says quantity 0 may be current zero holding and requires trades/orders confirmation | PASS |
| Position stock code and name are split into separate columns | position table `股票代码` and `股票名称` columns | UI smoke checks sort headers for code/name; build passed | PASS |
| Position table supports tri-state sort on all displayed fields | `SortHeader`, `nextSortState`, position sort keys for code/name/quantity/can_sell/cost/current_price/market_value/profit/day_profit/profit_rate | Targeted smoke clicks code header asc -> desc -> clear; build passed | PASS |
| Trades table is hidden by default and expands on demand | `miniqmt-trades-toggle`, conditional `miniqmt-trades-table` | Targeted smoke verifies table count 0 before click and visible after click | PASS |
| Trades table has pagination controls | `miniqmt-trades-prev`, `miniqmt-trades-next`, `tradePage` | Targeted smoke verifies both controls visible after expand | PASS |
| Trades table includes stock name and sortable displayed fields | `股票名称` plus SortHeader for time/code/name/side/quantity/price/amount/strategy/order id | Targeted smoke verifies `miniqmt-trade-sort-name`; build passed | PASS |
| MiniQMT portfolio list explains local fields | `miniqmt-local-fields-help` NoticePanel | Targeted smoke verifies local fields help is visible | PASS |
| Existing real-flow Playwright regression covers new controls | `frontend/tests/paper-v2/paper-v2-real-flow.spec.ts` | Added checks for history select/clear and MiniQMT sort/collapse controls | PASS |
| Production gates remain no-op | no DB/schema/dependency/runtime files changed | `production_ddl_gate=noop`, `production_frontend_dependency_gate=noop`, `production_backend_dependency_gate=noop` | PASS |

## Commands And Results

```powershell
python scripts/aistock_issue_workflow.py doctor
# PASS/WARN: workflow_gate=warning only for missing optional CodeGraph/Understand Anything indexes; no blocking items; GitHub auth OK; client wrappers current.

python scripts/aistock_issue_workflow.py finish --bug-id BUG-131 --issue-json tests/aistock_validation/bugs/20260527_BUG-131-paper-v2-miniqmt.json --plan-only
# PASS: scope_check=passed, production gates noop; blocked only because validation evidence not yet supplied and .coverage artifact was present before cleanup.

git diff --check
# PASS

cd frontend
npm run build
# PASS: Next.js production build completed. Existing repository-wide react-hooks warnings remain in unrelated files.

python -m nox -s l0 paper_v2_backend validation_center_backend validation_module_registry_l0
# PASS: l0 success with non-blocking existing P2/baseline guardrail findings.
# PASS: paper_v2_backend: 511 passed, 1 skipped, 2 xfailed.
# PASS: validation_center_backend: 236 passed, coverage line=79.5 branch=61.41.
# PASS: validation_module_registry_l0: 8 passed; ownership scan files=12 mapped=12 unmapped=0 ambiguous=0.

python scripts/aistock_data_quality_smoke.py --scope paper_v2_selection_center --since-hours 12 --output tmp/bug131_data_quality_12h.json
# FAIL/RESIDUAL: schema/audit/calendar/dataset/strategy/selection/ledger checks passed, but paper_v2_run_traceability failed because sampled_succeeded_runs=0 in the moving 12h window.

python scripts/aistock_data_quality_smoke.py --scope paper_v2_selection_center --output tmp/bug131_data_quality_default.json
# FAIL/RESIDUAL: schema/audit/calendar/dataset/strategy/selection checks passed; paper_v2_run_traceability failed with missing_success_event=1 in historical local DB data; ledger consistency reported legacy WARN.

Copy tmp/bug131-ui-smoke.spec.ts to frontend/tests/paper-v2/bug131-ui-smoke.spec.ts, then:
cd frontend
$env:PAPER_V2_FRONTEND_PORT='3012'
$env:PAPER_V2_API_BASE='http://127.0.0.1:8012/api/v1'
npx playwright test tests/paper-v2/bug131-ui-smoke.spec.ts --config=playwright.paper-v2.config.ts
# PASS: 1 passed (13.6s). Temporary spec removed after run.
```

## Environment

- Backend validation port: `8012` dev backend only.
- Frontend validation port: `3012` Playwright webServer only.
- Production ports: `8001` and `3000` were not restarted or stopped.
- TDX/prod services: not modified.
- Database: read-only validation queries only; no DDL and no production write migration.
- `.env`: not edited; Paper v2 scheduler/auto-run/MiniQMT configuration preserved.

## Business Outcome

- Selection Center now has a top-first package selector and current-page bulk history selection controls.
- MiniQMT position view now exposes cost, current price, market value, code/name split, quantity-zero explanation, profit fields, and tri-state sorting.
- MiniQMT trade view now starts collapsed, expands on demand, has name column, sortable displayed fields, and pagination.
- MiniQMT portfolio list now explains that `initial_cash`/local fields are AIstock local schema metadata, not broker-authoritative cash.
- No broker authority changed: MiniQMT remains the authority for account, positions, costs, market value, orders, and trades.

## Failures / Residual Risks

| Finding | Evidence | Impact | Disposition |
|---|---|---|---|
| Full `paper_v2_l3` data-quality gate is not clean on this local DB snapshot | `aistock_data_quality_smoke.py` fails on `paper_v2_run_traceability`: 12h window has zero successful runs; default window has `missing_success_event=1` and legacy ledger WARN | Prevents claiming a fully clean L3 data-quality gate from this workstation state | Documented as pre-existing local DB/history residual; UI-specific acceptance is covered by build/backend regressions and targeted Playwright smoke. Do not close as fully verified until data-quality baseline is repaired or scoped validation run data is seeded. |
| Next build reports React hook warnings | `npm run build` warnings in config/rdagent/local-data/qmt/quantevolver etc. | No build failure; unrelated to changed files | Existing repo-wide warnings, not introduced by BUG-131. |
| Workflow `resume --bug-id BUG-131` discovers stale BUG-132 state from another active workflow | Resume output points to BUG-132 worktree/state while BUG-131 local `tmp/issue_workflow/BUG-131` is correct | Could confuse automated continuation commands | Continue with explicit `--issue-json tests/aistock_validation/bugs/20260527_BUG-131-paper-v2-miniqmt.json` from the BUG-131 branch/worktree. |

## Production Gates

- `production_ddl_gate=noop` — no DB DDL or runtime schema dependency changed.
- `production_frontend_dependency_gate=noop` — no frontend dependency manifest changed.
- `production_backend_dependency_gate=noop` — no backend dependency manifest changed.
- Production runtime touched: no.
- Production DB writes/DDL: no.
- Production restart required before review: no.

## Final Status

- Functional/UI acceptance for BUG-131: PASS on targeted UI smoke and build/backend regression.
- Full local L3 data-quality gate: RESIDUAL BLOCKED by pre-existing local Paper v2 history/data-quality state, not by the UI patch.
- Merge recommendation: PR can be opened for review with the residual explicitly disclosed; do not mark BUG-131 `verified` or merge to `main` until the user accepts the documented residual or the data-quality baseline is repaired and rerun.
