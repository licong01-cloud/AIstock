# L3 Validation - V25.1 Qlib Board-Lot Exchange Smoke

Date: 2026-05-06 (Asia/Shanghai)
Module: QE / Qlib minute execution / V25.1 small-cap execution
Risk level: L3 real-data smoke + order legality audit

## Scope

- Fixed V25.1 STAR-market child-order legality so 688/689 orders use raw-share board-lot rules: minimum 200 shares, then 1-share increments.
- Preserved legacy V25 behavior by keeping global `trade_unit=100` for V25 and using `trade_unit: null` plus a board-lot-aware exchange patch only for V25.1.
- Fixed Qlib adjusted-share handling: V25.1 now builds/legalizes schedules in raw shares via `$factor`, then converts emitted child orders back to Qlib adjusted amounts.
- Packaged `qe_board_lot_exchange.py` into QE workspaces and RD-Agent V25.1 templates so generated remote/local runs install the same exchange-layer patch.

## Business Oracles

- No TWAP/day/default-price fallback was introduced.
- V25.1 STAR child orders must survive Qlib exchange handling as non-100 raw-share amounts when legal (for example 203, 210, 609 shares).
- Qlib cash clipping must not turn a legal STAR BUY into an illegal sub-200 child fill.
- V25 comparison remains isolated: V25 uses `trade_unit=100`; V25.1 uses stock-aware board-lot exchange behavior.
- No protected model weights, StrategyPackage manifests, DB assets, HMM snapshots, or paper-trading ledgers were modified.

## Real-Data Qlib Smoke

Command:

```powershell
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
conda run -n AIstock --no-capture-output python scripts/compare_v25_vs_v25_1_1y.py `
  --minute-provider-uri F:/Dev/AIstock/qlib_bin/qlib_bin_st_pit_active_minute_candidate_20240102_20260430 `
  --day-provider-uri F:/Dev/AIstock/qlib_bin/qlib_bin_20260430_shsz_current_candidate `
  --start 2026-04-20 --end 2026-04-24 `
  --early-model F:/Dev/AIstock/rdagent_assets/model_cache/execution/V25_TWO_STAGE/v25_early_net_joint_fixed.pt `
  --late-model F:/Dev/AIstock/rdagent_assets/model_cache/execution/V25_TWO_STAGE/v25_late_net_joint_fixed.pt `
  --device cpu `
  --codes '000001.SZ,300750.SZ,600519.SH,688111.SH,688256.SH,688981.SH,689009.SH' `
  --topk 4 --drop 2 --account 1000000 --benchmark 000300.SH `
  --output .codex_tmp/v25_1_qlib_smoke_20260506_0030.json
```

Result:

- Exit code: 0
- Artifact: `.codex_tmp/v25_1_qlib_smoke_20260506_0030.json`
- Log: `.codex_tmp/v25_1_qlib_smoke_20260506_0030.log`
- V25 total_return: `-0.4245%`
- V25.1 total_return: `+0.3831%`
- V25.1 1min fill_rate_mean: `1.0000`
- V25.1 1min deal_amount_sum: `29279.7057`
- Config evidence: `v25_trade_unit=100`, `v25_1_trade_unit=null`, `v25_1_board_lot_exchange=true`

## STAR Order Legality Audit

Command:

```powershell
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
conda run -n AIstock --no-capture-output python .codex_tmp/v25_1_qlib_order_audit_board_exchange_star.py
```

Result:

- Exit code: 0
- Artifact: `.codex_tmp/v25_1_qlib_order_audit_20260506_0020_star.json`
- Log: `.codex_tmp/v25_1_qlib_order_audit_20260506_0020_star.log`
- Records: `60`
- STAR records: `26`
- STAR violations: `0`
- STAR exchange changed amount: `0`
- STAR nonzero deals: `26`
- STAR emitted non-100-share orders: `26`
- STAR dealt non-100-share orders: `26`
- STAR samples included legal raw-share fills such as `689009.SH` 203/406/609 shares and `688111.SH` 210/216 shares.

Note: the audit also records Qlib outer day-level target orders. One non-STAR `300750.SZ` day-level residual line is not a V25.1 STAR child-order violation and is outside the STAR exchange bug fixed here.

## Automated Regression

Commands and results:

```powershell
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
pytest backend/tests/test_tail_twap_v25_market_state.py backend/tests/unified_engine/test_qe_config_truth.py -k "v25_1 or trade_unit" -q -p no:cacheprovider
# 7 passed, 50 deselected

pytest backend/tests/test_tail_twap_v25_market_state.py backend/tests/trading_core/test_v25_1_small_cap_contract.py backend/tests/unified_engine/test_qe_config_truth.py -k "v25_1 or v25" -q -p no:cacheprovider
# 41 passed, 39 deselected

pytest backend/tests/test_tail_twap_v25_market_state.py backend/tests/trading_core/test_v25_1_small_cap_contract.py backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
# 80 passed

pytest backend/tests/unified_engine/test_qe_config_truth.py -k "v25_1 or v25" -q -p no:cacheprovider
# 6 passed, 39 deselected

python -m py_compile scripts/tail_twap_v25_strategy.py scripts/tail_twap_v25_1_strategy.py scripts/qe_board_lot_exchange.py scripts/qrun_limit_minute.py scripts/compare_v25_vs_v25_1_1y.py backend/services/quantevolver/config_composer.py
# passed
```

RD-Agent synced-copy compile:

```powershell
python - <<compile RD-Agent V25.1 template files>>
# compiled 32 RD-Agent V25.1 files
```

## Residual Risks

- This is a 5-trading-day, 7-symbol Qlib smoke, not a full production-scale backtest.
- Qlib order amounts are adjusted-share amounts; the audit converts to approximate raw shares through `$factor` and validates the raw-share board-lot rule.
- Existing local RD-Agent QE workspaces were synchronized for local consistency, but only staged Git paths are authoritative for GitHub review.
- No production backend/frontend ports were restarted.

## Guardrails

```powershell
conda run -n AIstock python -m nox -s guardrail_changed_files
# passed; blocking=0, P0/P1 new findings=0; residual P2 complexity findings are non-blocking.

git diff --cached --check
# passed

git -C F:/Dev/RD-Agent-main diff --cached --check
# passed
```

Additional focused rerun after final qrun packaging adjustment:

```powershell
pytest backend/tests/trading_core/test_execution_algo_capabilities.py backend/tests/test_tail_twap_v25_market_state.py backend/tests/unified_engine/test_qe_config_truth.py -k "v25_1 or v25 or trade_unit" -q -p no:cacheprovider
# 21 passed, 44 deselected
```
