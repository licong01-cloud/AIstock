# HMM Stage-3 Retrained QE Task - L3

## Scope

- Registered four Stage-3 retrained HMM candidates as QE-selectable `sector_hmm` snapshots.
- Created a remote custom QE evolution task on `rdagent-node1`.
- Set `execution_mode=parallel_4` and `node_parallelism={"rdagent-node1": 4}`.
- Kept the comparison controls and non-HMM runtime settings cloned from the prior HMM validation baseline.

## Registered HMM Snapshots

| candidate | config_id | snapshot_id | mapping | script-level evidence |
| --- | --- | --- | --- | --- |
| `HMM_STAGE3_FBT_ROBUST_N2_TF_PEN_0p96__qe20260505` | `79e62bce-c41c-440b-b578-96762cc9c055` | `1d7ca1b9-fa65-4a97-9726-a6896f168121` | trend-fade `hmm_score`, direct clip, penalty-only `0.96-1.00` | `holdout_weighted_rank_ic=0.027554`; real retrain proof `sector_features_appended_before_GaussianHMM.fit` |
| `HMM_STAGE3_FBT_ROBUST_N2_TF_SYM_0p96_1p04__qe20260505` | `8e572f6d-d5d0-4eaf-8294-b2d97af0cad7` | `858b2ed9-4089-467f-98f0-54e6ce6b06b6` | trend-fade `hmm_score`, direct clip, symmetric `0.96-1.04` | same Stage-3 best model, conservative amplitude test |
| `HMM_STAGE3_FBT_ROBUST_N2_TF_AGG_0p95_1p08__qe20260505` | `a6007665-b542-4b41-8015-52a3ea8243cd` | `76267343-d182-4f12-974e-b8bfacfa56ee` | trend-fade `hmm_score`, direct clip, aggressive `0.95-1.08` | same Stage-3 best model, amplitude stress test |
| `HMM_STAGE3_TURNOVER_LIGHT_N3_UTIL_PEN_0p96__qe20260505` | `c0850f0b-ea2d-487d-9aff-ca1d338c6612` | `ef6e044a-095c-46a4-8c91-fcecf107764a` | `utility_raw_score`, validation zscore clip, penalty-only `0.96-1.00` | `holdout_weighted_rank_ic=0.025714`; real retrain proof `sector_features_appended_before_GaussianHMM.fit` |

All four artifacts contain:

- `models.json` copied from retrained diagnostic `model_diagnostics.json["models"]`, not from Loop10.
- `coefficients_preset_A_2024-07-01_2026-04-27.json` with 442 trade dates and 5,847 stock-sector mappings.
- 71 known missing sector-date rows from the source score panel filled neutrally at `1.0` and counted in metadata.
- `strict_no_leakage=true`, `precomputed_only=true`, and one QE default coefficient window.

## QE Task

- Task: `qe_20260505_123035_bf80`
- Name: `HMM_stage3_retrained_candidates_qe_20260502_131502_9b54_remote_p4_20260505_123035`
- Source baseline: `qe_20260502_131502_9b54`
- Submit API: `POST http://127.0.0.1:8001/api/v1/quantevolver/evolution/custom-tasks`
- Initial task status: `running`, `current_loop=0`, `max_loops=7`
- Initial submitted/running loops observed: Loop1-Loop4 running on `rdagent-node1`, matching parallelism 4.

## Loop Plan

| loop | label | HMM snapshot | purpose |
| ---: | --- | --- | --- |
| 1 | `NO_HMM__stage3_control_qe_20260502_131502_9b54_Loop1` | none | no-HMM control |
| 2 | `LOOP2_BASE__old_covfix_w3_raw__retained_control` | `bbec3863-fb67-445f-938e-66f092d18696` | earliest retained baseline |
| 3 | `LOOP10_BASE__penalty_only_f096__retained_best` | `6ea64754-003d-48d8-ad9e-d0e7857716c8` | current QE-best retained HMM |
| 4 | `STAGE3_FBT_ROBUST_N2_TF_PEN_0p96` | `1d7ca1b9-fa65-4a97-9726-a6896f168121` | Stage-3 best model, risk-only penalty |
| 5 | `STAGE3_FBT_ROBUST_N2_TF_SYM_0p96_1p04` | `858b2ed9-4089-467f-98f0-54e6ce6b06b6` | Stage-3 best model, conservative boost/penalty |
| 6 | `STAGE3_FBT_ROBUST_N2_TF_AGG_0p95_1p08` | `76267343-d182-4f12-974e-b8bfacfa56ee` | Stage-3 best model, stronger boost/penalty |
| 7 | `STAGE3_TURNOVER_LIGHT_N3_UTIL_PEN_0p96` | `ef6e044a-095c-46a4-8c91-fcecf107764a` | second-best Stage-3 model, risk-only penalty |

## Runtime Settings Held Constant

- Model: `__seed_LGBModel_conservative_v1__`
- Strategy: `score_weighted_topk_v2`
- Factor keys: 57
- Stock pool: `filtered_pool_20260502`
- Execution algorithm: `V25_TWO_STAGE`
- Tail handling: `TAIL_SUBSTITUTE`, backup depth 15
- Industry blacklist: enabled through strategy params, 3 blocked sector entries reconstructed for the source stock pool
- Suspend handling: `filter_suspended_on_signal=false`, `suspend_filter_strict=true`

## Evidence

- Registration script: `scripts/register_hmm_stage3_qe_candidates_20260505.py`
- Registry result: `.codex_tmp/hmm_registry_updates/hmm_stage3_qe_registry_result_20260505_122815.json`
- Registry stdout: `.codex_tmp/hmm_registry_updates/hmm_stage3_qe_register_stdout_20260505.txt`
- Custom task payload: `.codex_tmp/hmm_stage3_qe_20260505/custom_evo_payload_20260505_123035.json`
- Submit response: `.codex_tmp/hmm_stage3_qe_20260505/submit_response_20260505_123035.json`
- Initial task detail: `.codex_tmp/hmm_stage3_qe_20260505/task_detail_initial.json`
- Editable custom-evo config: `.codex_tmp/hmm_stage3_qe_20260505/custom_evo_config_initial.json`
- HMM selector after registration: `.codex_tmp/hmm_stage3_qe_20260505/hmm_selector_after_registration.json`
- Initial task log tail: `.codex_tmp/hmm_stage3_qe_20260505/task_log_tail_initial.json`

## Validation Commands

```powershell
python -m py_compile scripts/register_hmm_stage3_qe_candidates_20260505.py
python scripts/register_hmm_stage3_qe_candidates_20260505.py --dry-run
python scripts/register_hmm_stage3_qe_candidates_20260505.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- scripts/register_hmm_stage3_qe_candidates_20260505.py
```

## Results

- `py_compile`: passed.
- HMM selector API on production backend `8001`: returned 6 `sector_hmm` configs, including the four new Stage-3 candidates plus Loop10 and the earliest baseline.
- Snapshot artifact preflight: all four new snapshots expose one `preset_A` coefficient artifact covering `2024-07-01` to `2026-04-27`.
- QE task creation: passed with HTTP 200 and task id `qe_20260505_123035_bf80`.
- QE execution start: task is visible through both `8001` and `8011`; Loop1-Loop4 are running on `rdagent-node1`.
- Log evidence: task log tail contains `[START] loop=Loop1` and `Starting QLib backtest`, with `V25_TWO_STAGE`, `TAIL_SUBSTITUTE`, `filtered_pool_20260502`, and blacklist snapshot in the submitted config.

## Residual Risk

- The task is still running at this checkpoint; QE PnL/enhanced metrics are not available until remote loops finish.
- The new HMM snapshots are visible in the QE HMM selector by design; prune them later if the QE result proves them ineffective.
- The script created new protected HMM assets only; no existing HMM asset was overwritten or deleted.
