# 2026-05-09 L2 Validation - Financial Distress Additional QE Policy Config Validation

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | Phase 14                                                            |
| validation level | L2 research + offline QE artifact validation                         |
| runtime impact   | no QE/Paper/Selection/QMT/runtime integration                        |
| production 8001  | not touched                                                         |
| generated report | reports/event_signal/financial_distress_policy_config_additional_qe_overlay/financial_distress_qe_multiloop_20240701_20260509_165605.json |
+------------------+---------------------------------------------------------------------+
```

## Offline Validation Command

```powershell
conda run -n AIstock python -m backend.services.event_signal.financial_distress_qe_overlay_research `
  --loop-spec-json .codex_tmp/event_signal/financial_distress_phase14_additional_loops.json `
  --date-from 2024-07-01 `
  --date-to 2026-04-27 `
  --active-trading-days 20 `
  --active-trading-days 60 `
  --simulator-mode score_down_context `
  --score-down-context-profile rank_decay_balanced `
  --include-size-bucket-rules `
  --include-refinement-rules `
  --rule-key indicator_large_decline_mv_10_30bn `
  --rule-key loss_to_market_cap_ge_50pct_mv_lt_10bn `
  --output-dir reports/event_signal/financial_distress_policy_config_additional_qe_overlay
```

## Offline Result

```text
+------------------+---------------------------------------------+
| metric           | value                                       |
+------------------+---------------------------------------------+
| loops            | 12                                          |
| validations      | 48                                          |
| main candidate   | indicator_large_decline_mv_10_30bn          |
| best row         | 60td, avg_return_delta +0.04%, pos 8/12     |
| rejected default | 20td as single default; avg_return_delta -0.01% |
| runtime action   | none                                        |
+------------------+---------------------------------------------+
```

## Regression Commands

```powershell
python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q
python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
rg -n "financial_distress_policy_config|indicator_large_decline_mv_10_30bn|rank_decay_balanced" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
git diff --check
```

## Regression Results

```text
+--------------------------------------+---------------------------------------------+
| check                                | result                                      |
+--------------------------------------+---------------------------------------------+
| targeted financial distress tests    | 31 passed                                   |
| full event_signal module tests       | 166 passed                                  |
| runtime isolation scan               | no matches in QE/Paper/Selection/QMT paths  |
| diff whitespace check                | passed; LF/CRLF warnings only               |
+--------------------------------------+---------------------------------------------+
```

## Issues Encountered

```text
+------------------------------------------+----------------------------------------------+----------------------------------------------+
| issue                                    | cause                                        | resolution                                   |
+------------------------------------------+----------------------------------------------+----------------------------------------------+
| missing artifact in one loop             | qe_20260429_015755_c4ba:Loop1 incomplete     | excluded Loop1 and used Loop5                |
| unknown rule_key                         | missing include flags for non-default rules   | reran with include-size/refinement flags      |
| DB auth failure                          | feature worktree lacks .env                   | loaded TDX_DB_* from root .env for process    |
| quoted DB port                           | raw .env values included surrounding quotes   | stripped quotes before invoking conda command |
+------------------------------------------+----------------------------------------------+----------------------------------------------+
```

## Residual Risks

```text
+----------------------------+---------------------------------------------------------------+
| risk                       | mitigation                                                     |
+----------------------------+---------------------------------------------------------------+
| artifact overlay approx    | true QE rerun is still required before runtime integration     |
| small effect size          | Phase 15 should test moderate parameter sweeps                 |
| no consumer audit yet      | future runtime integration must persist rerank application log |
+----------------------------+---------------------------------------------------------------+
```
