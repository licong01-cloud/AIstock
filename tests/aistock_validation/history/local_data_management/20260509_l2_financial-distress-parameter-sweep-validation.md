# 2026-05-09 L2 Validation - Financial Distress Parameter Sweep Research

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | Phase 15                                                            |
| validation level | L2 research + offline QE artifact validation                         |
| runtime impact   | no QE/Paper/Selection/QMT/runtime integration                        |
| production 8001  | not touched                                                         |
| generated report | docs/analysis/event_signal_financial_distress_parameter_sweep_result_20260509.md |
+------------------+---------------------------------------------------------------------+
```

## Commands

```powershell
conda run -n AIstock python -m backend.services.event_signal.financial_distress_qe_overlay_research `
  --loop-spec-json .codex_tmp/event_signal/financial_distress_phase15_combined_loops.json `
  --date-from 2024-07-01 `
  --date-to 2026-04-27 `
  --active-trading-days 20 `
  --active-trading-days 60 `
  --active-trading-days 120 `
  --simulator-mode score_down_context `
  --score-down-context-profile rank_decay_balanced `
  --score-down-context-profile rank_decay_severity `
  --include-refinement-rules `
  --rule-key indicator_large_decline_mv_10_30bn `
  --output-dir reports/event_signal/financial_distress_policy_parameter_sweep_context_qe_overlay

conda run -n AIstock python -m backend.services.event_signal.financial_distress_qe_overlay_research `
  --loop-spec-json .codex_tmp/event_signal/financial_distress_phase15_combined_loops.json `
  --date-from 2024-07-01 `
  --date-to 2026-04-27 `
  --active-trading-days 20 `
  --active-trading-days 60 `
  --active-trading-days 120 `
  --simulator-mode score_down `
  --score-down-rank-penalty-pct 0.10 `
  --score-down-rank-penalty-pct 0.15 `
  --score-down-rank-penalty-pct 0.20 `
  --include-refinement-rules `
  --rule-key indicator_large_decline_mv_10_30bn `
  --output-dir reports/event_signal/financial_distress_policy_parameter_sweep_fixed_qe_overlay

python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q
python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
rg -n "financial_distress_policy_config|indicator_large_decline_mv_10_30bn|rank_decay_balanced" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
git diff --check
```

## Results

```text
+--------------------------------------+---------------------------------------------+
| check                                | result                                      |
+--------------------------------------+---------------------------------------------+
| context sweep                        | 22 loops, 132 validations                   |
| fixed sweep                          | 22 loops, 198 validations                   |
| combined parameter sweep             | 330 validations                            |
| targeted financial distress tests    | 31 passed                                   |
| full event_signal module tests       | 166 passed                                  |
| runtime isolation scan               | no matches in QE/Paper/Selection/QMT paths  |
| diff whitespace check                | passed; LF/CRLF warnings only               |
+--------------------------------------+---------------------------------------------+
```

## Key Outcome

```text
+--------------------------------------+-------------------------+--------------------------------------------------------------+
| candidate                            | validation conclusion   | implication                                                  |
+--------------------------------------+-------------------------+--------------------------------------------------------------+
| 60td context-balanced                | preferred research shape| best average return and better tail than fixed baseline      |
| fixed 10% rank demotion              | baseline only           | explainable but weaker tail                                  |
| 20td variants                       | comparison only         | useful reference, not preferred default                      |
| 120td severity profile              | secondary diagnostic    | still viable, but not best overall                           |
+--------------------------------------+-------------------------+--------------------------------------------------------------+
```

## Issues Encountered

```text
+------------------------------------------+----------------------------------------------+----------------------------------------------+
| issue                                    | cause                                        | resolution                                   |
+------------------------------------------+----------------------------------------------+----------------------------------------------+
| missing artifact in one loop             | qe_20260429_015755_c4ba:Loop1 incomplete     | excluded Loop1 and used Loop5                |
| no DB password supplied                  | worktree has no .env                         | loaded TDX_DB_* from root .env for process    |
| quoted DB port                           | raw .env values included surrounding quotes   | stripped quotes before rerunning sweep        |
+------------------------------------------+----------------------------------------------+----------------------------------------------+
```

## Residual Risks

```text
+----------------------------+---------------------------------------------------------------+
| risk                       | mitigation                                                     |
+----------------------------+---------------------------------------------------------------+
| artifact overlay approx    | still needs true QE rerun before runtime promotion             |
| parameter sensitivity      | next phase should stay offline and sweep around 60td profile   |
| no consumer audit yet      | future runtime integration must persist candidate rerank trace |
+----------------------------+---------------------------------------------------------------+
```
