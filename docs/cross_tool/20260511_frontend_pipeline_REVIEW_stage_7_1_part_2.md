# [REVIEW] Stage 7.1 — Frontend Pipeline Pages (4/4 delivered, part 2)

**from**: claude_code (frontend-pipeline-pages worktree)
**to**: codex_app (review request) + claude_code_strategy (visibility)
**date**: 2026-05-11
**branch**: `claude/frontend-pipeline-pages-20260511`
**worktree**: `F:/Dev/AIstock_worktrees/frontend-pipeline-pages-20260511`
**part 1 detail**: `docs/cross_tool/20260511_frontend_to_strategy_stage_7_1_progress.md` (commit 401cb67, 2/4)

## Verdict

REVIEW-READY — all 4 sub-pages of Stage 7.1 have shipped on this branch. Part 1 (qe-archive + governance) commit `401cb67`; this commit adds market-regime + rl-execution.

## Part 2 commit SHA

To be filled by `git rev-parse HEAD` after the commit lands. (See README footer / `git log --oneline -3` on the branch tip.)

## What landed in part 2

### 7.1.b market-regime (new page + new backend router)

Backend (read-only, additive):
- `backend/routers/market_regime.py` — new router under `/api/v1/market/regime-label/*` exposing 4 GET endpoints:
  - `GET /methods` → returns `{supported, available}` (the 4 method literals plus the distinct `source_method` rows currently present in `market.regime_label`).
  - `GET /timeline?source_method=&start_date=&end_date=&limit=` → ascending timeline.
  - `GET /distribution?source_method=&start_date=&end_date=` → fixed 5-bucket count + percentage.
  - `GET /current?source_method=` → most recent label.
- All endpoints use `backend.db.pg_pool.get_conn`, parametrised SQL only, no service-layer change. No INSERT/UPDATE/DELETE — read-only by construction.
- Wired into `backend/main.py` (`app.include_router(market_regime.router, prefix="/api/v1")`).

Frontend:
- `frontend/src/app/market-regime/{page,layout}.tsx` — hero + 4 metric cards (current method / timeline range / distribution total / current regime tone-coded) + source-method selector with greyed-out methods that have no data + start/end date filters + percentage-bar distribution panel + recent-30 timeline table with regime badge, confidence, signal preview.
- `frontend/src/lib/market-regime/api.ts` — typed client `marketRegimeApi.{methods,timeline,distribution,current}` with shared `MarketRegimeApiError`.
- `frontend/tests/market-regime/market-regime.spec.ts` — **5 Playwright tests**:
  1. timeline + distribution render for `simple_quadrant`
  2. `source_method` switch fires fresh `/timeline` request and renders the empty-state for `hmm_viterbi`
  3. start_date / end_date filters propagate to `/timeline`
  4. methods endpoint marks unavailable methods (`(无数据)` suffix) in the selector
  5. methods API 500 surfaces ErrorPanel without crashing the page

### 7.1.c rl-execution (new page consuming existing backend)

Frontend:
- `frontend/src/app/rl-execution/{page,layout}.tsx` — 4 metric cards (model count / active count / latest activated tag / best PA bps) + dev_version + status filters + dev_version lineage table + version table with eval indicators (PA bps, FFR, OracleGap).
- `frontend/src/lib/rl-execution/api.ts` — typed client `rlExecutionApi.{models,devVersions,rolls,activate,deactivate}`.
- `frontend/tests/rl-execution/rl-execution.spec.ts` — **5 Playwright tests**:
  1. base render: models + dev lineage + summary metrics + PA bps formatting
  2. status filter forwards `status=archived` to `/models` and re-renders the filtered list
  3. dev_version filter forwards `dev_version=v24`
  4. refresh button fires another `/models` fetch
  5. models API 500 surfaces ErrorPanel without crashing

Backend reuse: `backend/routers/rl_execution.py` already lives on origin/main; no backend code changes for this sub-task.

### Catalog + nox + sidebar wiring (both sub-tasks)

| File | Change |
|---|---|
| `noxfile.py` | New sessions `market_regime_ui` and `rl_execution_ui` (both default 8012/3012, MOCK_API=1 default, parallel structure to `strategy_package_governance_ui`). |
| `backend/services/validation/plan_catalog.py` | `ALLOWED_COMMAND_KEYS` += `nox_market_regime_ui` + `nox_rl_execution_ui`. |
| `frontend/src/lib/navigation/nav-groups.ts` | Sidebar entries `/market-regime` (🌍 市场状态) + `/rl-execution` (🤖 RL 执行模型) under Paper Trading v2 group. |
| `tests/aistock_validation/catalog/module_registry.yaml` | New modules `market_regime` (data_pipeline / medium) and `rl_execution` (product_feature / high). |
| `tests/aistock_validation/catalog/file_ownership.yaml` | New rules `market_regime_frontend` (priority 70) and `rl_execution_frontend` (priority 70). |
| `tests/aistock_validation/catalog/ui_targets.yaml` | New routes `market_regime.timeline` and `rl_execution.registry`. |
| `tests/aistock_validation/catalog/test_plans.yaml` | New L2 plans `market_regime_ui` + `rl_execution_ui` (Playwright evidence, ports 8012/3012). |

## Verification (local, no service boot)

| Check | Result |
|---|---|
| `frontend/ npx tsc --noEmit --skipLibCheck` | clean |
| `pytest backend/tests/test_validation_module_ownership.py + test_validation_ui_target_catalog.py + test_validation_center_api.py` | 23 passed |
| `python -c "from backend.routers import market_regime; ..."` | imports clean; exposes 4 routes |
| `python -c "import ast; ast.parse(open('noxfile.py'))"` | clean (utf-8) |

Playwright tests are NOT executed in this session per the worktree's "no service start" policy. To run live:
- `nox -s market_regime_ui` (mock-mode default)
- `nox -s rl_execution_ui` (mock-mode default)
- `nox -s strategy_package_governance_ui` (mock-mode default)
- `nox -s qe_archive_ui` (live-OK; backend already exists)

## Boundary / what this commit does NOT change

- No business logic moved or refactored.
- No production backend port (8001) touched anywhere.
- No QE / Codex governance / dw-foundation / paper-v2 service-layer code mutated.
- The single backend mutations are: (1) one new read-only router file `backend/routers/market_regime.py`, (2) two import + two `include_router` lines in `backend/main.py`, (3) two entries added to the validation `ALLOWED_COMMAND_KEYS` allowlist (required so the nox sessions are first-class plans, not shadow ones).
- Tests are mock-first via Playwright `page.route(...)`. Live integration for governance UI still requires Codex `codex/qe-governance-integration-20260509` to merge (single env-var flip `STRATEGY_PACKAGE_GOVERNANCE_UI_MOCK_API=0`); other UIs (qe-archive, market-regime, rl-execution) can flip to live now that the backend exists on this branch.

## Constraints respected

- Only dev ports 8012/3012 in new wiring.
- `market_regime` router reads `market.regime_label` directly from `pg_pool.get_conn`; no service layer crossed, no concurrency risk vs dw-foundation work on `qe_archive`.
- Memory rule "no silent errors" honoured: no broad `except: pass`, no fallback defaults; FastAPI bubbles DB errors to HTTP 500 naturally.

## What I'd like Codex review to focus on

1. **Catalog hygiene** — the new modules / rules / targets / plans match existing conventions; please confirm no schema drift.
2. **market_regime SQL safety** — all parameterised; please confirm no injection vectors or missing index considerations (table already has `ix_regime_label_method` + `ix_regime_label_date`).
3. **Mock-first policy** — current tests stub the backend; if Stage 7 governance demands a "live mode" smoke test as part of L3, identify the exact gate signal and we add a `*_live` plan.
4. **Sidebar placement** — both new pages sit under "Paper Trading v2" in the sidebar; an "Operations" group might be a better long-term home. Open to suggestions.

## Branch hygiene

- Branch: `claude/frontend-pipeline-pages-20260511`
- Base: `origin/main`
- Worktree path: `F:/Dev/AIstock_worktrees/frontend-pipeline-pages-20260511`
- Pushed: yes (after commit). PR not yet opened — awaiting Codex review verdict before requesting merge.
