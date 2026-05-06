# HMM Utility Aggressive Mapping QE Task Validation

- Generated at: 2026-05-04T18:53:20
- Module: HMM / QuantEvolver custom_evo / remote QE
- Goal: register retrained HMM utility-score aggressive mappings as QE-selectable loop snapshots and start a remote parallel=4 QE validation task.

## Registered Hidden HMM Snapshots

| loop_use | variant | snapshot_id | mapping | source | stage3_weighted_rank_ic | coeff_change_rate | active_rate |
| --- | --- | --- | --- | --- | ---: | ---: | ---: |
| Loop4 | fpb_n3_csz_aggressive | `f81e9513-aa75-48d0-b08d-be7b7ec5212b` | utility_raw_score / cs_zscore_clip2 / aggressive_0p95_1p08 | flow_plus_breadth n=3 diag | 0.026153 | 0.755824 | 0.974543 |
| Loop5 | fpb_n3_valz_aggressive | `da0089a3-809a-426d-b597-97d10aa81e50` | utility_raw_score / val_zscore_clip2 / aggressive_0p95_1p08 | flow_plus_breadth n=3 diag | 0.026101 | 0.418466 | 0.981175 |
| Loop6 | fpb_n3_valz_conservative | `23872e31-d8b2-4f32-a4ed-7bef7a4efe56` | utility_raw_score / val_zscore_clip2 / conservative_0p98_1p03 | flow_plus_breadth n=3 diag | 0.026101 | 0.358837 | 0.941004 |
| Loop7 | flowcore_n2_valz_aggressive | `f69a6745-9860-4001-9e06-edaff570187e` | utility_raw_score / val_zscore_clip2 / aggressive_0p95_1p08 | flow_core n=2 diag | 0.025636 | 0.232105 | 0.976184 |
| Loop8 | volcompress_n4_valz_aggressive | `769e7882-409f-4bc1-a577-39b347476c81` | utility_raw_score / val_zscore_clip2 / aggressive_0p95_1p08 | vol_compress n=4 diag | 0.022310 | 0.384173 | 0.968239 |

## QE Task

- Active task: `qe_20260504_184036_3a3c`
- Created via: `POST http://127.0.0.1:8011/api/v1/quantevolver/evolution/custom-tasks`
- UI/API visibility checked via: `GET http://127.0.0.1:8001/api/v1/quantevolver/evolution/tasks/{task_id}`
- Remote node: `rdagent-node1`; execution_mode=`parallel_4`; node_parallelism=`{"rdagent-node1": 4}`
- Current status at last poll: `running`; current_loop=0/8

## Loop Plan

| loop | label | HMM snapshot | description |
| ---: | --- | --- | --- |
| 1 | NO_HMM__qe_20260502_131502_9b54_Loop1_replica | `` | No HMM baseline |
| 2 | LOOP2_BASE__old_covfix_w3_raw | `bbec3863-fb67-445f-938e-66f092d18696` | Loop2 old-covfix baseline |
| 3 | LOOP10_BASE__penalty_only_f096 | `6ea64754-003d-48d8-ad9e-d0e7857716c8` | Loop10 penalty-only retained best |
| 4 | UTIL_FPB_N3_CSZ_AGG_0p95_1p08__high_churn | `f81e9513-aa75-48d0-b08d-be7b7ec5212b` | Retrained HMM utility mapping candidate |
| 5 | UTIL_FPB_N3_VALZ_AGG_0p95_1p08 | `da0089a3-809a-426d-b597-97d10aa81e50` | Retrained HMM utility mapping candidate |
| 6 | UTIL_FPB_N3_VALZ_CONS_0p98_1p03__comparator | `23872e31-d8b2-4f32-a4ed-7bef7a4efe56` | Retrained HMM utility mapping candidate |
| 7 | UTIL_FLOWCORE_N2_VALZ_AGG_0p95_1p08 | `f69a6745-9860-4001-9e06-edaff570187e` | Retrained HMM utility mapping candidate |
| 8 | UTIL_VOLCOMP_N4_VALZ_AGG_0p95_1p08 | `769e7882-409f-4bc1-a577-39b347476c81` | Retrained HMM utility mapping candidate |

## Validation Commands / Evidence

- `python -m py_compile scripts/register_hmm_utility_mapping_qe_candidates_20260504.py` passed.
- `python scripts/register_hmm_utility_mapping_qe_candidates_20260504.py --dry-run` built artifacts, rolled DB back, and removed generated dirs.
- `python scripts/register_hmm_utility_mapping_qe_candidates_20260504.py` registered 5 hidden completed snapshots under `sector_hmm_experimental_utility_mapping_20260504`.
- Direct coefficient resolution via `ConfigComposer._resolve_hmm_coefficients_json` succeeded for all 5 new snapshots with 442 days and 131 sectors.
- Payload: `.codex_tmp/hmm_utility_aggressive_custom_evo_payload_dev8011_retry1_20260504.json`
- Submit response: `.codex_tmp/hmm_utility_aggressive_custom_evo_submit_response_dev8011_retry1_20260504.json`
- Latest task detail: `.codex_tmp/qe_20260504_184036_3a3c_detail_latest_20260504.json`
- Custom-evo editable config: `.codex_tmp/qe_20260504_184036_3a3c_custom_evo_config_20260504.json`

## Incident / Cleanup

- First submission to production port `8001` created `qe_20260504_183846_6db6`, but HMM loops failed because the running 8001 process still had an older loaded `HMMTrainingService` without `get_config`.
- That stale attempt was stopped to avoid resource confusion; Loop1 process was killed and DB status moved to paused/cancelled for the running loop.
- Stop response: `.codex_tmp/hmm_utility_aggressive_old_8001_attempt_stop_response_20260504.json`; stopped task detail: `qe_20260504_183846_6db6`

## Residual Risk

- As of this record, the active retry task is running and no loop has completed yet, so QE PnL/backtest-effect conclusions are not available.
- The utility-mapping snapshots are precomputed QE coefficient artifacts derived from retrained HMM score panels; runtime generation is intentionally disabled.
- Each candidate had 71 missing sector-date score-panel rows filled neutrally at coefficient 1.0; this is explicit in metadata and should be considered in interpretation.
- Production `8001` should be restarted or hot-reloaded later to pick up the HMM `get_config` fallback, but it was not restarted during this validation.
