# HMM full validation custom QE evolution - 2026-05-04

## Scope

- Source task: `qe_20260502_131502_9b54`, source loop: `Loop1`.
- Created task: `qe_20260504_014618_a9ec`.
- Task type: `custom_evo`.
- Goal: validate no-HMM plus all current HMM QE-selectable candidates while keeping every non-HMM setting identical to source Loop1.
- Required runtime: remote node `rdagent-node1`, execution mode `parallel_4`, node parallelism `{"rdagent-node1": 4}`.
- Production impact: existing FastAPI `8001` and frontend `3000` were used read-only for visibility checks; production backend was not restarted.

## Created Task

```text
Task ID                  qe_20260504_014618_a9ec
Status                   completed
Current / Max loops       10 / 10
Node                     rdagent-node1
Execution mode            parallel_4
Engine mode               unified
Create payload            .codex_tmp/hmm_full_validation_custom_evo_payload_20260504.json
Create response           .codex_tmp/hmm_full_validation_custom_evo_create_response_20260504.json
Final summary JSON        .codex_tmp/qe_20260504_014618_a9ec_final_validation_summary_20260504.json
```

## Loop Map And Results

```text
Loop  HMM  Label                                                     Snapshot                              AnnRet    MaxDD     Sharpe   DeltaAnn
----  ---  --------------------------------------------------------  ------------------------------------  --------  --------  -------  --------
1     N    NO_HMM__replica_of_qe_20260502_131502_9b54_Loop1         -                                     0.462117  -0.165808  1.99424  0.000000
2     Y    HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore      bbec3863-fb67-445f-938e-66f092d18696  0.475617  -0.155894  2.06453  0.013500
3     Y    HMM_TEST_hyb_old_primary_turnover_flow_core_c70__qe2026  78a4ecf7-4cca-4b67-af66-3d59573587eb  0.447539  -0.165975  1.94532 -0.014577
4     Y    HMM_TEST_old_covfix_primary_b020_p005__qe20260502        89753fae-0c3c-4c75-9282-c20d7d833ffa  0.458413  -0.161941  1.98360 -0.003704
5     Y    HMM_TEST_sf_turnover_fast_q20_b010_p005__qe20260502      28335a3c-64d8-4ce8-944e-25e48a68f77c  0.455963  -0.157120  1.98746 -0.006154
6     Y    HMM_TEST_old_covfix_boost_only_p105__qe20260504          377a8447-ee26-44a8-8ead-7338f525e0f2  0.465458  -0.164516  2.02808  0.003341
7     Y    HMM_TEST_old_covfix_penalty094_boost103__qe20260504      5a8ce90e-50bb-4fbd-8cd8-e3b95c9dffa0  0.466579  -0.157275  2.02213  0.004462
8     Y    HMM_TEST_old_covfix_penalty095_boost104__qe20260504      afa6acd9-f766-4394-970e-451d1a39bb06  0.453140  -0.157237  1.96493 -0.008976
9     Y    HMM_TEST_old_covfix_penalty095_boost106__qe20260504      8ddb5d29-8097-4aef-b110-f2f94f54ca4b  0.463593  -0.158945  1.99997  0.001477
10    Y    HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504   6ea64754-003d-48d8-ad9e-d0e7857716c8  0.480163  -0.157395  2.07694  0.018046
```

- IC is identical across loops within float noise: `0.07873336924198773` to `0.07873336924198775`.
- RankIC is identical across loops: `0.11314405611235591`.
- Best annualized return and Sharpe in this validation are both Loop10, `HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504`.
- Loop10 improves annualized return by `+0.018046` and Sharpe by about `+0.08270` versus no-HMM Loop1.

## Config Equivalence

```text
Non-HMM config violations          0
Factor count                       57
Source factor_list hash            bbdb7807988c45f0
Target factor_list hash            bbdb7807988c45f0
Source Loop1 conf.yaml SHA256      4dca31adb00982aca4455afb1903a7174cbe8dd9bd1ff3a1d9c4653334b45c81
Target Loop1 conf.yaml SHA256      4dca31adb00982aca4455afb1903a7174cbe8dd9bd1ff3a1d9c4653334b45c81
Source/target no-HMM conf equal    true
```

Validated settings inherited from source Loop1:

```text
model_id                  __seed_LGBModel_conservative_v1__
strategy_id               score_weighted_topk_v2
stock_pool                filtered_pool_20260502
label_horizon             10
strategy label_horizon    5
Alpha158                  disabled
execution_algo            V25_TWO_STAGE
execution device          cuda
early_model_path          /home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt
late_model_path           /home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt
unfilled_handler          TAIL_SUBSTITUTE
backup_depth              15
suspend_filter_strict     true
remote conf suspend flag  filter_suspended_on_signal: true
industry blacklist        enabled, 3 SW2 industries
```

## Completion And Data Integrity

```text
Local DB task status       completed
Local completed loops       10 / 10
Remote completed loops      10 / 10
QE experiments completed    10 / 10
metrics_json present        10 / 10
result_metrics present      10 / 10
enhanced metrics present    10 / 10
```

Each completed loop has these enhanced metric sections:

```text
absolute_returns, all_stocks, bottom_stocks, factor_analysis, ic_diagnostics,
prediction_diagnostics, return_curves, stock_trades, summary, top_stocks,
trade_diagnostics, training_diagnostics
```

## UI / API Visibility Checks

```text
Endpoint / check                                                        Result
---------------------------------------------------------------------  ------
GET /api/v1/quantevolver/evolution/tasks?limit=20                     200, contains task id
GET /api/v1/quantevolver/evolution/tasks/qe_20260504_014618_a9ec      200, contains task id and loop metrics
GET /api/v1/quantevolver/evolution/tasks/qe_20260504_014618_a9ec/custom-evo-config  200, contains task id and 10 configs
GET /api/v1/quantevolver/evolution/tasks/qe_20260504_014618_a9ec/trajectory         200, contains task id and loop data
GET http://127.0.0.1:3000/quantevolver/evolution                      200, frontend route loads
```

Saved API snapshots:

```text
.codex_tmp/qe_20260504_014618_a9ec_task_list_final_20260504.json
.codex_tmp/qe_20260504_014618_a9ec_task_detail_final_20260504.json
.codex_tmp/qe_20260504_014618_a9ec_custom_config_final_20260504.json
.codex_tmp/qe_20260504_014618_a9ec_trajectory_final_20260504.json
```

## Commands

```powershell
# Poll and reconcile DB/API state until terminal.
C:/Users/lc999/miniconda3/envs/AIstock/python.exe - <<'PY'
# Queried qe_evolution_tasks, qe_evolution_loops, qe_experiments and remote QE workspace statuses.
PY
```

```powershell
# Idempotent callback reconciliation used after remote completion was observed before local DB update.
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8001/api/v1/quantevolver/evolution/webhook/loop-completed' -ContentType 'application/json' -Body '{"task_id":"qe_20260504_014618_a9ec","loop_id":"qe_20260504_014618_a9ec_Loop5"}'
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8001/api/v1/quantevolver/evolution/webhook/loop-completed' -ContentType 'application/json' -Body '{"task_id":"qe_20260504_014618_a9ec","loop_id":"qe_20260504_014618_a9ec_Loop6"}'
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8001/api/v1/quantevolver/evolution/webhook/loop-completed' -ContentType 'application/json' -Body '{"task_id":"qe_20260504_014618_a9ec","loop_id":"qe_20260504_014618_a9ec_Loop7"}'
```

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/qe_evolution_diagnostic.py qe_20260504_014618_a9ec --json
```

```powershell
Invoke-WebRequest -Uri 'http://127.0.0.1:3000/quantevolver/evolution' -TimeoutSec 20 -UseBasicParsing
Invoke-WebRequest -Uri 'http://127.0.0.1:8001/api/v1/quantevolver/evolution/tasks?limit=20' -TimeoutSec 20 -UseBasicParsing
```

## Callback Note

- `rdagent-node1` uses callback base `http://192.168.50.14:8000`.
- A temporary local callback proxy was kept running on `0.0.0.0:8000` and forwarded to `http://127.0.0.1:8001` so remote loop-completed callbacks could update the existing production backend without restarting it.
- Proxy evidence: `.codex_tmp/callback_proxy_8000.py`, `.codex_tmp/callback_proxy_8000.log`.

## Business Outcome

- The requested dedicated QE custom evolution task was created, visible to QE UI/API, ran all 10 requested loops on `rdagent-node1`, and generated real backtest and enhanced metrics data for every loop.
- The experiment is a valid A/B comparison for HMM effect because the no-HMM target conf equals the source Loop1 generated conf byte-for-byte, and all target loops have identical non-HMM DB configs after stripping only HMM fields.
- The best validated HMM candidate from this run is Loop10, so the next sector-factor stacking test should use Loop10 as the baseline HMM version unless a different risk preference prioritizes lower drawdown over Sharpe/return.

## Residual Risk

- This is a single source-loop validation on one remote node and one historical window; it proves QE selectability and this backtest outcome, not cross-seed or cross-window robustness.
- Several old docs and unrelated workspace files are dirty before/around this task; they were not modified or staged as part of this validation.
- The callback proxy is temporary infrastructure. If remote callback base remains port `8000`, future remote QE tasks need either this proxy or a permanent callback URL correction.
