# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-05-29T22:49:09+08:00
- Git base: 539ded56 (origin/main at validation)
- Operator: Codex / lc999

## Scope

- Changed files:
  - scripts/aistock_data_quality_smoke.py
  - backend/tests/test_data_quality_smoke_env.py
  - tests/aistock_validation/bugs/.bug_id_allocator.json
  - tests/aistock_validation/bugs/20260529_BUG-167-paper-v2-miniqmt-data-quality-gate-treats-broker-authoritative-runs-as-l.json
- Impacted flows: Paper v2 data-quality smoke for broker-authoritative MiniQMT portfolios.
- Business goal: MiniQMT broker-authoritative Paper v2 runs must pass data-quality traceability when they have MiniQMT success events and broker-authority snapshots, without being judged by LocalSim-only cash ledger/NAV formulas.
- Out of scope: UI changes, production runtime restart, production DB writes, DDL.
- Protected assets reviewed: no StrategyPackage manifests, HMM snapshots, QE artifacts, models, or production assets modified.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No blocking high-risk finding introduced by this change | `python -m nox -s l0` | PASS |
| Targeted lint | Changed Python files lint clean | `python -m ruff check scripts/aistock_data_quality_smoke.py backend/tests/test_data_quality_smoke_env.py` | PASS |
| Targeted unit tests | MiniQMT success events and broker-authority SQL invariants are covered | `python -m pytest backend/tests/test_data_quality_smoke_env.py -q` -> 6 passed | PASS |
| MiniQMT scoped data-quality | MiniQMT portfolio no longer fails `missing_success_event`, `fills_without_cash_ledger`, or `invalid_daily_snapshots` | `python scripts/aistock_data_quality_smoke.py --scope paper_v2_selection_center --portfolio-id paper_1d9b1f03700f4810aef8351124c8ab6c --output tmp/paper_v2_data_quality_bug167_minqmt.json` | PASS |
| Paper v2 backend regression | Paper v2 / Selection / StrategyPackage backend tests pass | `python -m nox -s paper_v2_backend` -> 537 passed, 1 skipped, 2 xfailed | PASS |
| Paper v2 data-quality gate | Full non-strict data-quality gate remains successful while legacy unrelated history stays WARN only | `python -m nox -s paper_v2_data_quality` | PASS |
| Deep data-quality | Deep DB consistency checks remain healthy or skip cleanly where fixtures are absent | `python -m nox -s data_quality_deep` -> 10 passed, 21 skipped | PASS |
| L3 orchestration | Required non-UI L3 sessions pass; UI is not touched by BUG-167 and full UI E2E requires user-owned dev backend/frontend restart | `$env:PAPER_V2_L3_SKIP_UI='1'; python -m nox -s paper_v2_l3` -> 5 sessions passed | PASS with explicit UI runtime exception |
| UI service readiness | Dev backend 8012 availability checked before attempting UI E2E | `python scripts/aistock_validate.py services --backend-port 8012 --tdx-port 19080` -> backend 8012 connection refused, TDX 19080 ok | BLOCKED by user-owned dev service restart |

## Commands

```powershell
python -m ruff check scripts/aistock_data_quality_smoke.py backend/tests/test_data_quality_smoke_env.py
python -m pytest backend/tests/test_data_quality_smoke_env.py -q
python -m compileall scripts/aistock_data_quality_smoke.py backend/tests/test_data_quality_smoke_env.py
git diff --check
python scripts/aistock_data_quality_smoke.py --scope paper_v2_selection_center --portfolio-id paper_1d9b1f03700f4810aef8351124c8ab6c --output tmp/paper_v2_data_quality_bug167_minqmt.json
python -m nox -s l0
python -m nox -s paper_v2_backend
python -m nox -s paper_v2_data_quality
python -m nox -s data_quality_deep
$env:PAPER_V2_L3_SKIP_UI='1'; python -m nox -s paper_v2_l3
python scripts/aistock_validate.py services --backend-port 8012 --tdx-port 19080
```

## Evidence

- MiniQMT portfolio: `paper_1d9b1f03700f4810aef8351124c8ab6c`.
- MiniQMT scoped smoke result: `paper_v2_run_traceability` PASS with `sampled_succeeded_runs=3`; `paper_v2_ledger_consistency` PASS with `fills_without_cash_ledger=0`, `invalid_daily_snapshots=0`, `invalid_positions=0`.
- Full data-quality smoke result: PASS overall; legacy non-strict `order_fill_quantity_mismatches=3` remains WARN and is not introduced by BUG-167.
- Dev UI blocker: backend `8012` was not running; per user rule, Codex did not start/restart backend/frontend services.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| MiniQMT data-quality failed with `missing_success_event=3` | Smoke script only accepted LocalSim generic run events | Accepted `MINIQMT_RUN_RECONCILED` and `MINIQMT_NATIVE_RUN_RECONCILED` as Paper v2 success events | MiniQMT scoped smoke PASS |
| MiniQMT data-quality failed with `fills_without_cash_ledger=6` | Smoke script required LocalSim cash ledger for broker-authoritative fills | Excluded `minqmt_sim` / `minqmt_live` broker-authority backends from LocalSim cash ledger invariant | MiniQMT scoped smoke PASS |
| MiniQMT data-quality failed with `invalid_daily_snapshots=3` | Smoke script required LocalSim `nav = cash + market_value` formula for broker-authority snapshots | Kept non-negative snapshot checks for all backends, applied NAV formula only to non-broker-authority backends | MiniQMT scoped smoke PASS |

## Result

- Final status: PASS for BUG-167 backend/data-quality scope; full UI E2E not run because dev backend 8012 is not running and restarts are user-owned.
- Remaining risks: run full `python -m nox -s paper_v2_l3` without `PAPER_V2_L3_SKIP_UI` after user starts dev backend/frontend if UI evidence is required before merge.
- Need production backend restart: no.
- Need dev service restart: only for optional full UI E2E on dev ports.
