# HMM Loop10 Conditional Sparse QE Start - 2026-05-06

## Scope

- Module: HMM / QuantEvolver custom_evo / remote QE.
- Goal: validate Loop10-anchored conditional-sparse HMM candidates selected by script-level TopK attribution.
- QE mode: backtest-only, remote node `rdagent-node1`, parallelism `4`.
- Source trained model: `qe_20260505_123035_bf80` Loop1; no duplicate stock-alpha training intended.

## Registered Hidden HMM Snapshots

| display_name | variant | snapshot_id |
| --- | --- | --- |
| `HMM_TEST_L10_TIGHTEN_FBT_P15_PEN_0p955__qe20260506` | `l10_tighten_fbt_p15_pen_0p955` | `5cdaa81c-76ce-41b4-bb7b-dbb3c279d587` |
| `HMM_TEST_L10_TIGHTEN_TL_P15_PEN_0p95__qe20260506` | `l10_tighten_tl_p15_pen_0p95` | `f105d289-6385-4cf6-8514-a3be1919c0f2` |
| `HMM_TEST_L10_TIGHTEN_FB_P15_PEN_0p95__qe20260506` | `l10_tighten_fb_p15_pen_0p95` | `1ace5ee3-b443-444c-a68e-88934d0b95f7` |

## QE Task

- Task id: `qe_20260506_004257_b34a`
- Task name: `HMM_l10_conditional_sparse_backtest_only_remote_p4_20260506_004256`
- Create response: `.codex_tmp/hmm_l10_conditional_sparse_qe_20260506/create_response.json`
- Payload: `.codex_tmp/hmm_l10_conditional_sparse_qe_20260506/payload.json`
- Initial task detail: `.codex_tmp/hmm_l10_conditional_sparse_qe_20260506/task_detail_initial.json`
- Initial remote backtest-only probe: `.codex_tmp/hmm_l10_conditional_sparse_qe_20260506/initial_remote_backtest_only_probe.json`

## Loop Plan

| loop | label | hmm_snapshot |
| ---: | --- | --- |
| 1 | `NO_HMM__bt_source_loop1_control` | `NO_HMM` |
| 2 | `LOOP10_BASE__penalty_only_f096__current_best` | `6ea64754-003d-48d8-ad9e-d0e7857716c8` |
| 3 | `LOOP2_BASE__old_covfix_w3_raw__drawdown_control` | `bbec3863-fb67-445f-938e-66f092d18696` |
| 4 | `STAGE3_SPARSE_TL_B15_PEN_0p995__near_best_prev` | `db001359-2ef4-4db3-8cab-c68bc1ea18b2` |
| 5 | `L10_TIGHTEN_FBT_P15_PEN_0p955` | `5cdaa81c-76ce-41b4-bb7b-dbb3c279d587` |
| 6 | `L10_TIGHTEN_TL_P15_PEN_0p95` | `f105d289-6385-4cf6-8514-a3be1919c0f2` |
| 7 | `L10_TIGHTEN_FB_P15_PEN_0p95` | `1ace5ee3-b443-444c-a68e-88934d0b95f7` |

## Monitor

- Scheduled task: `AIstock_HMM_L10_Conditional_QE_20260506`
- Interval: every 30 minutes.
- Wrapper: `.codex_tmp/hmm_l10_conditional_sparse_qe_20260506/run_monitor_once.ps1`
- Monitor script: `scripts/automation/hmm_l10_conditional_qe_monitor_20260506.py`
- State: `.codex_tmp/hmm_l10_conditional_sparse_qe_20260506/monitor_state.json`
- Log: `.codex_tmp/hmm_l10_conditional_sparse_qe_20260506/monitor.log`
- Behavior: analyze completed task; if no new candidate beats Loop10, run one narrower script-level refinement and launch one follow-up backtest-only QE round.

## Guardrails

- `py_compile` passed for registration and monitor scripts.
- `nox -s l0` passed for `scripts/register_hmm_loop10_conditional_sparse_qe_candidates_20260506.py` and `scripts/automation/hmm_l10_conditional_qe_monitor_20260506.py`; no P1/P0 blockers.
- Rechecked after handoff with `python -m py_compile scripts/hmm_loop10_conditional_sparse_screen_20260506.py scripts/register_hmm_loop10_conditional_sparse_qe_candidates_20260506.py scripts/automation/hmm_l10_conditional_qe_monitor_20260506.py`.
- Rechecked after handoff with `C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- scripts/hmm_loop10_conditional_sparse_screen_20260506.py scripts/register_hmm_loop10_conditional_sparse_qe_candidates_20260506.py scripts/automation/hmm_l10_conditional_qe_monitor_20260506.py`; result passed, no P0/P1 blockers, only expected P2 complexity notes.
- QE visible selector remains clean: `sector_hmm` has exactly two visible configs; new candidates are under hidden model type `sector_hmm_experimental_l10_conditional_sparse_20260506`.

## Current Status

- Initial monitor run recorded task status `running`, current loop `0/7`.
- Remote probe found Loop1-Loop4 created and running; command lines include `--backtest-only` and `Symlink mlruns` to source task Loop1.
- Windows scheduled task was manually smoke-tested at `2026-05-06 00:54:43 +08:00`; `Last Result = 0`.
- Scheduled task settings were hardened for overnight operation: `WakeToRun=True`, `DisallowStartIfOnBatteries=False`, `StopIfGoingOnBatteries=False`, `MultipleInstances=IgnoreNew`, `ExecutionTimeLimit=72h`.
