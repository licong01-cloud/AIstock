# Event Signal Financial Distress Research Progress

## Session Log

```text
+---------------------+--------------------------------------------------+-----------------+
| time                | action                                           | result          |
+---------------------+--------------------------------------------------+-----------------+
| 2026-05-08 morning  | added multiloop and size-bucket overlay research | committed       |
| 2026-05-08 noon     | added score-down rank20 research                 | committed       |
| 2026-05-08 afternoon| added severity and exposure diagnostics          | committed       |
| 2026-05-08 evening  | added rolling loss-history research              | committed bf67daa|
| 2026-05-08 evening  | added restart-safe research tracking docs        | committed b82f48b|
| 2026-05-08 evening  | added market-cap bucket coverage summary         | validation done |
+---------------------+--------------------------------------------------+-----------------+
```

## Latest Completed Commit Before Phase 8

```text
b82f48b docs(event): add financial distress research tracking
```

## Latest Validation Commands

```powershell
python -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py backend/tests/event_signal/test_financial_distress_qe_overlay_research.py
python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q
python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
rg -n "loss_history|loss_reports_ge_4|financial_distress_loss_history" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
rg -n "market_cap_bucket_summary|MARKET_CAP_BUCKET_ORDER|normalize_market_cap_bucket_counter" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
git diff --check
```

## Latest Validation Results

```text
+--------------------------------------+-----------------------------+
| check                                | result                      |
+--------------------------------------+-----------------------------+
| py_compile                           | pass                        |
| targeted financial-distress pytest   | 29 passed after phase 10    |
| event_signal pytest suite            | 159 passed                  |
| runtime isolation scan               | no runtime references added |
| WSL 10-loop offline overlay          | pass, 640 phase-10 rows     |
| refinement validation                | pass, 20/60/120/242td tested|
| git diff --check                     | pass, LF/CRLF warnings only |
+--------------------------------------+-----------------------------+
```

## Current Next Action

Phase 11: research sector-regime attribution and direct event-date returns for `indicator_large_decline_mv_10_30bn`. Industry concentration remains explanatory/rotation context only; do not implement industry neutralization.

## Commit Policy

- Commit curated tracking docs after this update.
- Continue pushing to the feature branch only.
- Do not merge to `main` until user explicitly requests integration.

## 2026-05-09 Phase 9 Session

```text
+---------------------+--------------------------------------------------+-------------------------------+
| time                | action                                           | result                        |
+---------------------+--------------------------------------------------+-------------------------------+
| 2026-05-09 morning  | resumed phase 9 in dedicated worktree           | branch clean; root main unused |
| 2026-05-09 morning  | checked structured event_signal availability    | mid/large samples exist        |
+---------------------+--------------------------------------------------+-------------------------------+
```

## 2026-05-09 Phase 9 Completion

```text
+---------------------+--------------------------------------------------+-------------------------------+
| time                | action                                           | result                        |
+---------------------+--------------------------------------------------+-------------------------------+
| 2026-05-09 morning  | implemented mid-large structured event rules     | tests passed                  |
| 2026-05-09 morning  | ran WSL 10-loop overlay validation               | 360 validations passed        |
| 2026-05-09 morning  | added curated report and validation record       | ready to commit               |
+---------------------+--------------------------------------------------+-------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_mid_large_qe_overlay_result_20260509.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260509_l2_financial-distress-mid-large-qe-overlay-validation.md`.

## 2026-05-09 Phase 10 Start

```text
+---------------------+--------------------------------------------------+-------------------------------+
| time                | action                                           | result                        |
+---------------------+--------------------------------------------------+-------------------------------+
| 2026-05-09 morning  | accepted no industry-neutral constraint          | phase-10 scope adjusted       |
| 2026-05-09 morning  | kept sector exposure as explanatory only         | no industry rejection planned |
+---------------------+--------------------------------------------------+-------------------------------+
```


## 2026-05-09 Phase 10 Completion

```text
+---------------------+--------------------------------------------------+-------------------------------+
| time                | action                                           | result                        |
+---------------------+--------------------------------------------------+-------------------------------+
| 2026-05-09 morning  | added refinement rule set and prior loss fields  | tests passed                  |
| 2026-05-09 morning  | ran WSL 10-loop refinement validation            | 640 validations passed        |
| 2026-05-09 morning  | documented no industry-neutralization decision   | sector as explanation only    |
+---------------------+--------------------------------------------------+-------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_refinement_qe_overlay_result_20260509.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260509_l2_financial-distress-refinement-qe-overlay-validation.md`.


## 2026-05-09 Phase 11 Completion

```text
+--------------------+---------------------------------------------+----------------------------------------------+
| time               | action                                      | result                                       |
+--------------------+---------------------------------------------+----------------------------------------------+
| 2026-05-09 morning | added direct event-return research script   | research-only; no DB writes                  |
| 2026-05-09 morning | added 000300.SH abnormal-return aggregation | raw and benchmark-adjusted returns available |
| 2026-05-09 morning | ran direct study for 4 rules                | 3713 events / 22278 return rows              |
| 2026-05-09 morning | documented Phase 11 conclusion              | contextual score-down, no hard ban           |
+--------------------+---------------------------------------------+----------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_direct_event_return_result_20260509.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260509_l2_financial-distress-direct-event-return-validation.md`.


## 2026-05-09 Phase 12 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-09 afternoon | added context score-down profiles     | rank/severity/decay/sector-relief supported     |
| 2026-05-09 afternoon | ran light/severity context validation | 480 validations passed                          |
| 2026-05-09 afternoon | ran balanced context validation       | 320 validations passed                          |
| 2026-05-09 afternoon | documented Phase 12 conclusion        | preferred non-hard context candidate identified |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_context_qe_overlay_result_20260509.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260509_l2_financial-distress-context-qe-overlay-validation.md`.


## 2026-05-09 Phase 13 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-09 afternoon | translated Phase 12 into policy config| draft profile/rule parameters documented        |
| 2026-05-09 afternoon | checked policy lifecycle schema fit   | no schema change required for the draft stage   |
| 2026-05-09 afternoon | kept runtime boundary unchanged       | no QE/Paper/Selection/QMT integration           |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_policy_config_proposal_20260509.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260509_l2_financial-distress-policy-config-proposal-validation.md`.


## 2026-05-09 Phase 14 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-09 afternoon | selected additional QE artifacts      | 12 loops across 3 experiments                   |
| 2026-05-09 afternoon | ran exact Phase-13 config validation  | 48 validations completed                        |
| 2026-05-09 afternoon | compared 20td and 60td windows        | 60td weakly better; 20td downgraded to test     |
| 2026-05-09 afternoon | preserved runtime boundary            | no QE/Paper/Selection/QMT integration           |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_additional_qe_policy_config_validation_20260509.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260509_l2_financial-distress-additional-qe-policy-config-validation.md`.

Phase 14 errors and resolutions:

```text
+------------------------------------------+----------------------------------------------+----------------------------------------------+
| error                                    | cause                                        | resolution                                   |
+------------------------------------------+----------------------------------------------+----------------------------------------------+
| missing artifact in Loop1                | qe_20260429_015755_c4ba:Loop1 lacks pkl files| replaced with Loop5 after artifact check     |
| unknown rule_key values                  | refinement and size-bucket flags omitted     | reran with include-size/refinement flags      |
| no DB password supplied                  | worktree has no .env                         | loaded TDX_DB_* from root .env for process    |
| quoted DB port                           | raw .env quotes were not stripped            | stripped surrounding quotes before rerun      |
+------------------------------------------+----------------------------------------------+----------------------------------------------+
```


## 2026-05-09 Phase 15 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-09 afternoon | combined Phase 12 and Phase 14 loops  | 22-loop validation set built                    |
| 2026-05-09 afternoon | ran context profile sweep             | 132 validations completed                       |
| 2026-05-09 afternoon | ran fixed-rank penalty sweep          | 198 validations completed                       |
| 2026-05-09 afternoon | compared all rows                     | 60td context-balanced is best overall           |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_parameter_sweep_result_20260509.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260509_l2_financial-distress-parameter-sweep-validation.md`.

Phase 15 selected result:

```text
+------------------------------------+-----------+----------------+-----------+-----------+-----------+
| rule                               | active_td | mode           | pos/loops | avg_ret_d | min_ret_d |
+------------------------------------+-----------+----------------+-----------+-----------+-----------+
| indicator_large_decline_mv_10_30bn | 60        | ctx_balanced   | 14/22     | 0.11%     | -0.32%    |
+------------------------------------+-----------+----------------+-----------+-----------+-----------+
```


## 2026-05-09 Phase 16 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-09 evening   | parsed Phase 15 selected profile      | robustness and by-experiment summaries built    |
| 2026-05-09 evening   | evaluated promotion gates             | runtime and DB promotion rejected for now        |
| 2026-05-09 evening   | documented true-QE-rerun direction    | next phase should design traceable rerun path    |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_robustness_gate_result_20260509.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260509_l2_financial-distress-robustness-gate-validation.md`.

Phase 16 selected result:

```text
+----------------+-----------+-----------+-----------+-----------+-----------+
| profile        | pos/loops | avg_ret_d | med_ret_d | min_ret_d | ex_max_avg|
+----------------+-----------+-----------+-----------+-----------+-----------+
| ctx_balanced60 | 14/22     | 0.1134%   | 0.0000%   | -0.3206%  | 0.0294%   |
+----------------+-----------+-----------+-----------+-----------+-----------+
```


## 2026-05-09 Phase 17 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-09 evening   | inspected QE pred-backtest path       | qrun_limit_minute.py can rerun external pred.pkl|
| 2026-05-09 evening   | added pred materializer prototype     | research-only adjusted pred.pkl + trace writer  |
| 2026-05-09 evening   | validated previous-date mapping       | T signal rewrites T-1 prediction rows           |
| 2026-05-09 evening   | documented true-rerun harness design  | next phase is copied-loop one-loop smoke        |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_true_qe_rerun_design_20260509.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260509_l2_financial-distress-true-qe-rerun-design-validation.md`.

Phase 17 validation:

```powershell
python -m py_compile backend/services/event_signal/financial_distress_pred_materializer.py backend/tests/event_signal/test_financial_distress_pred_materializer.py
python -m pytest backend/tests/event_signal/test_financial_distress_pred_materializer.py -q
python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py backend/tests/event_signal/test_financial_distress_pred_materializer.py -q
python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
rg -n "financial_distress_pred_materializer|true_qe_rerun|rank_decay_balanced|indicator_large_decline_mv_10_30bn" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
git diff --check
```

```text
+--------------------------------------+-----------------------------+
| check                                | result                      |
+--------------------------------------+-----------------------------+
| py_compile                           | pass                        |
| targeted materializer pytest         | 4 passed                    |
| focused financial distress pytest    | 35 passed                   |
| event_signal pytest suite            | 170 passed                  |
| runtime isolation scan               | no runtime references added |
| git diff --check                     | pass, LF/CRLF warnings only |
| runtime code changes                 | none outside event_signal   |
| production backend 8001              | not touched                 |
+--------------------------------------+-----------------------------+
```

Phase 17 errors and resolutions:

```text
+------------------------------------------+----------------------------------------------+----------------------------------------------+
| error                                    | cause                                        | resolution                                   |
+------------------------------------------+----------------------------------------------+----------------------------------------------+
| PowerShell rejected python heredoc syntax| bash-style python - <<'PY' is unsupported    | used PowerShell here-string piped to python  |
+------------------------------------------+----------------------------------------------+----------------------------------------------+
```

## 2026-05-09 Phase 18 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-09 evening   | patched copied workspace paths        | WSL Qlib/provider and V25 model paths usable    |
| 2026-05-09 evening   | fixed copied conf BOM                 | qlib_init parsed correctly                      |
| 2026-05-09 evening   | ran adjusted pred-backtest smoke      | SigAnaRecord + PortAnaRecord completed          |
| 2026-05-09 evening   | attempted full-universe true rerun    | MemoryError reproduced on copied workspaces     |
| 2026-05-09 evening   | reviewed narrowed-universe metrics    | technical smoke only; PnL evidence rejected     |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_true_qe_rerun_smoke_result_20260509.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260509_l2_financial-distress-true-qe-rerun-smoke-validation.md`.

Phase 18 validation:

```powershell
C:\Users\lc999\miniconda3\envs\AIstock\python.exe qrun_limit_minute.py conf.yaml --pred-backtest event_signal_pred_backtest\adjusted_pred.pkl
```

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| copied conf parse                    | pass: qlib_init key loaded after BOM cleanup                |
| adjusted pred-backtest               | pass: completed in narrowed copied workspace                 |
| full-universe copied rerun           | fail-current-machine: MemoryError                            |
| narrowed-universe PnL metrics        | rejected as non-comparable evidence                          |
| runtime promotion                    | rejected; technical smoke only                               |
| production backend 8001              | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

Phase 18 errors and resolutions:

```text
+------------------------------------------+----------------------------------------------+----------------------------------------------+
| error                                    | cause                                        | resolution                                   |
+------------------------------------------+----------------------------------------------+----------------------------------------------+
| MemoryError on full universe             | copied exchange creation too large           | recorded blocker; do not infer PnL from narrowed runs |
| V25 model path not found                 | copied config had Linux /home paths          | changed copied workspace to WSL UNC paths    |
| qlib_init parsed as missing              | conf.yaml had UTF-8 BOM after rewrite        | rewrote copied conf.yaml without BOM         |
+------------------------------------------+----------------------------------------------+----------------------------------------------+
```

## 2026-05-10 Phase 19 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-09 night     | ran WSL adjusted full-universe rerun  | PortAnaRecord completed, recorder 59eaf3f...   |
| 2026-05-10 early     | ran WSL baseline full-universe rerun  | PortAnaRecord completed, recorder 7b5782...    |
| 2026-05-10 early     | compared same-environment metrics     | weak positive effect, not deployment material  |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_true_qe_wsl_full_universe_result_20260510.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260510_l2_financial-distress-wsl-full-universe-validation.md`.

Phase 19 evidence:

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| WSL adjusted full-universe rerun      | pass: 442 backtest steps completed                           |
| WSL baseline full-universe rerun      | pass: 442 backtest steps completed                           |
| annualized excess return delta        | +0.0014692370                                                |
| information ratio delta               | +0.0063244896                                                |
| max drawdown delta                    | +0.0002757115                                                |
| runtime promotion                     | rejected; signal remains research-only                       |
| production backend 8001              | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## 2026-05-10 Phase 20 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-10 morning   | added cheap rerun shortlist script    | parsed latest ignored multiloop reports         |
| 2026-05-10 morning   | screened 343 stability rows           | no row passed WSL_TRUE_RERUN_NOW                |
| 2026-05-10 morning   | compared direct event sanity rows     | benchmark rule not a hard-risk proof            |
| 2026-05-10 morning   | documented next empirical step        | 22-loop cheap expansion before WSL rerun        |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_selective_true_qe_shortlist_20260510.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260510_l2_financial-distress-selective-true-qe-shortlist-validation.md`.

Phase 20 evidence:

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| multiloop JSON reports scanned        | 13 latest report files under reports/event_signal            |
| candidate stability rows              | 343 rows                                                     |
| direct WSL rerun candidates           | 0 rows passed strict gate                                    |
| top cheap-expansion candidate          | structured_financial_risk_mv_ge_10bn                         |
| runtime promotion                      | rejected; research-only                                      |
| production backend 8001               | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## 2026-05-10 Phase 21 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-10 morning   | expanded Phase 20 shortlist           | 22-loop cheap overlay set completed             |
| 2026-05-10 morning   | ran 10 shortlisted rule families      | 1320 validations / 60 stability rows            |
| 2026-05-10 morning   | reviewed tail and market-cap exposure | no candidate passed risk-first true-rerun gate  |
| 2026-05-10 morning   | documented next empirical step        | tail-control sweep before WSL true rerun        |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_phase21_22_loop_overlay_result_20260510.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260510_l2_financial-distress-phase21-22-loop-overlay-validation.md`.

Phase 21 evidence:

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| 22-loop overlay command               | completed                                                    |
| validations                           | 1320                                                         |
| best avg row                          | loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss, +0.178%          |
| best avg row worst loop               | -1.966%, too large for risk-first signal                     |
| structured ge10 row                   | +0.110% avg, worst -0.879%, no true-rerun promotion          |
| runtime promotion                     | rejected; research-only                                      |
| production backend 8001              | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## 2026-05-10 Phase 22 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-10 midday    | ran focused tail-control sweep        | 704 validations / 32 stability rows             |
| 2026-05-10 midday    | compared fixed_5 and ctx_light modes  | worst improved to -0.935% but still not safe    |
| 2026-05-10 midday    | rechecked clean benchmark             | loss/mv<10bn fixed_20 remains cleanest tail     |
| 2026-05-10 midday    | documented pivot direction            | benchmark smoke or cleaner signal-family screen |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_phase22_tail_control_result_20260510.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260510_l2_financial-distress-phase22-tail-control-validation.md`.

Phase 22 evidence:

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| tail-control overlay command          | completed                                                    |
| validations                           | 704                                                          |
| severity_balanced loss-history row     | avg +0.178%, worst -1.966%                                   |
| fixed_5 loss-history row               | avg +0.120%, worst -0.935%                                   |
| ctx_light loss-history row             | avg +0.111%, 17/22 positive, worst -0.935%                   |
| clean benchmark row                    | loss/mv<10bn fixed_20 avg +0.122%, worst -0.174%             |
| runtime promotion                      | rejected; research-only                                      |
| production backend 8001               | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## 2026-05-10 Phase 23 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-10 evening   | materialized loss/mv<10bn fixed_20    | 2.26m pred rows, 193 rank dates touched         |
| 2026-05-10 evening   | ran WSL full-universe true QE smoke   | PortAnaRecord completed, recorder 34ecffc...   |
| 2026-05-10 evening   | compared baseline vs adjusted metrics | very weak positive, not deployment material     |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_benchmark_true_qe_smoke_result_20260510.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260510_l2_financial-distress-benchmark-true-qe-smoke-validation.md`.

Phase 23 evidence:

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| WSL adjusted full-universe rerun      | pass: 442 backtest steps completed                           |
| annualized excess return delta        | +0.0003580542                                                |
| information ratio delta               | +0.0017516633                                                |
| max drawdown delta                    | +0.0000225773                                                |
| runtime promotion                     | rejected; benchmark remains research-only                    |
| production backend 8001              | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## 2026-05-11 Phase 24 Completion

```text
+----------------------+---------------------------------------+-------------------------------------------------+
| time                 | action                                | result                                          |
+----------------------+---------------------------------------+-------------------------------------------------+
| 2026-05-10 night     | added phase-24 structured rules       | expectation miss, deterioration, cashflow/leverage |
| 2026-05-11 early     | ran direct event return screen        | 12 rules / 36 rule-window rows                  |
| 2026-05-11 early     | ran 22-loop cheap overlay screen      | 1584 validations / 72 stability rows            |
| 2026-05-11 early     | documented phase-24 conclusion        | no TRUE_QE candidate; best row is watchlist      |
+----------------------+---------------------------------------+-------------------------------------------------+
```

Latest curated report: `docs/analysis/event_signal_financial_distress_phase24_signal_family_screen_result_20260510.md`.
Latest validation record: `tests/aistock_validation/history/local_data_management/20260511_l2_financial-distress-phase24-signal-family-screen-validation.md`.

Phase 24 evidence:

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| phase24 screen command                | pass                                                         |
| direct event report                   | 12 rules, 36 rule/window rows                                |
| 22-loop cheap overlay                 | 1584 validations, 72 stability rows                          |
| best cheap row                        | indicator_decline_ocf_negative_or_leverage_mv_ge_10bn        |
| best cheap row metrics                | 60td fixed_10, avg +0.131%, ex-best +0.047%, worst -0.183%   |
| true-QE promotion                     | rejected; no TRUE_QE_CANDIDATE                               |
| event_signal pytest suite             | 168 passed                                                   |
| runtime isolation                     | no Selection/Paper/QE/QMT references added                   |
| production backend 8001               | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## 2026-05-11 Phase 25 Start

```text
+------+--------------------------------------------------------------------------+
| item | note                                                                     |
+------+--------------------------------------------------------------------------+
| goal | refine Phase-24 OCF/leverage stress into size, component, compound rules |
| scope| research-only scripts/services/tests/docs; no runtime consumer changes   |
| gate | direct event + 22-loop cheap overlay before any WSL true QE spend        |
+------+--------------------------------------------------------------------------+
```

## 2026-05-11 Phase 25 Completed

```text
+-------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
| item                    | value                                                                                                                                      |
+-------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
| script                  | scripts/financial_distress_phase25_threshold_refinement_screen.py                                                                          |
| direct report           | reports\event_signal\financial_distress_phase25_threshold_refinement\direct\financial_distress_direct_event_20240701_20260511_011654.json  |
| overlay report          | reports\event_signal\financial_distress_phase25_threshold_refinement\overlay\financial_distress_qe_multiloop_20240701_20260511_014959.json |
| rules / validations     | 12 / 72 stability rows / 1584 validations                                                                                                  |
| best rule               | indicator_decline_ocf_negative_or_leverage_mv_10_30bn, score 55.5                                                                          |
| decision                | no TRUE_QE_CANDIDATE; continue with parameter-shape sweep                                                                                  |
| production backend 8001 | not touched                                                                                                                                |
+-------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
```

## 2026-05-11 Phase 26 Completed

```text
+-------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| item                    | value                                                                                                                                       |
+-------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| script                  | scripts/financial_distress_phase26_parameter_shape_sweep.py                                                                                 |
| direct report           | reports\event_signal\financial_distress_phase26_parameter_shape_sweep\direct\financial_distress_direct_event_20240701_20260511_134419.json  |
| overlay report          | reports\event_signal\financial_distress_phase26_parameter_shape_sweep\overlay\financial_distress_qe_multiloop_20240701_20260511_145003.json |
| rules / validations     | 12 / 180 stability rows / 3960 validations                                                                                                  |
| best rule               | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn, score 56.1                                                                                |
| decision                | no TRUE_QE_CANDIDATE; continue with tighter sweep                                                                                           |
| production backend 8001 | not touched                                                                                                                                 |
+-------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
```

## 2026-05-11 Phase 27 Completed

```text
+-------------------------+----------------------------------------------------------------------------------------------------------------------------------------+
| item                    | value                                                                                                                                  |
+-------------------------+----------------------------------------------------------------------------------------------------------------------------------------+
| script                  | scripts/financial_distress_phase27_q_ocf_fine_sweep.py                                                                                 |
| direct report           | reports\event_signal\financial_distress_phase27_q_ocf_fine_sweep\direct\financial_distress_direct_event_20240701_20260511_150037.json  |
| overlay report          | reports\event_signal\financial_distress_phase27_q_ocf_fine_sweep\overlay\financial_distress_qe_multiloop_20240701_20260511_151633.json |
| rules / validations     | 1 / 20 stability rows / 440 validations                                                                                                |
| best rule               | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn, score 68.4                                                                           |
| best shape              | 90td / score_down_rank_15pct_top50_previous                                                                                            |
| decision                | TRUE_QE_CANDIDATE; prepare WSL true QE smoke                                                                                           |
| production backend 8001 | not touched                                                                                                                            |
+-------------------------+----------------------------------------------------------------------------------------------------------------------------------------+
```
