# HMM Regime Redefinition QE Progress Handoff - 2026-05-10

## Purpose

This handoff records the current HMM/QE evolution state so a new Codex window can continue without relying on chat memory. It covers the latest retrained sector-regime HMM direction, QE backtest results, registered hidden HMM snapshots, active experiments, and next actions.

## Repo And Branch

- Worktree: `F:\Dev\AIstock_worktrees\hmm-sector-regime-20260509`
- Branch: `codex/hmm-sector-regime-20260509`
- Latest pushed commit at handoff time: `2733cb7 feat(hmm): register bounded regime candidate for qe`
- Production/sync root: `F:\Dev\AIstock`
- Do not restart production backend `8001` unless explicitly requested.
- Do not touch unrelated root untracked file: `F:\Dev\AIstock\docs\architecture\data_warehouse_extension_design_20260510.md`.
- Temporary scratch outputs remain under `.codex_tmp/` and are not committed.

## Existing HMM Documentation

Earlier HMM evolution is already documented under `docs/analysis/`, including:

- `docs/analysis/hmm_training_current_status_20260503.md`
- `docs/analysis/hmm_offline_diagnostic_qe_20260502_131502_9b54.md`
- `docs/analysis/hmm_sector_factor_stacking_next_step_20260504.md`
- `docs/analysis/hmm_sector_factor_overlay_replacement_qe_20260502_131502_9b54.md`
- `docs/analysis/hmm_regime_bounded_screen_20260509.md`

This file is the continuation handoff for the 2026-05-10 retrained regime-HMM QE validation loop.

## Source Baseline

All latest QE loops intentionally preserve the original production-like baseline settings from:

- Base custom QE task: `qe_20260502_131502_9b54`
- Source model loop for backtest-only runs: `qe_20260502_131502_9b54/Loop1`
- Source model: `__seed_LGBModel_conservative_v1__`
- Strategy: `score_weighted_topk_v2`
- Stock pool: `filtered_pool_20260502`
- Label horizon: top-level `10`
- Execution algo: `V25_TWO_STAGE`
- Execution params:
  - `device=cuda`
  - `early_model_path=/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt`
  - `late_model_path=/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt`
- Tail handling: `TAIL_SUBSTITUTE`, backup depth `15`
- Suspend policy: `filter_suspended_on_signal=false`, `suspend_filter_strict=true`
- Industry blacklist: enabled via reconstructed current SW2 pool config snapshot
- Remote node: `rdagent-node1`
- QE execution mode: `parallel_4`

## Completed QE Round 1 - Regime Redefinition Comparison

Task: `qe_20260510_010004_8c2d`

Purpose: compare no-HMM, original production COVFIX, retained Loop10, and new retrained bounded regime-linear HMM using the same backtest-only source model.

Loops:

| Loop | Label | Snapshot | Result |
| --- | --- | --- | --- |
| 1 | `NO_HMM_L1_BACKTEST_ONLY` | none | Best result |
| 2 | `PROD_LOOP2_COVFIX_BACKTEST_ONLY` | `bbec3863-fb67-445f-938e-66f092d18696` | Underperformed no-HMM |
| 3 | `PROD_LOOP10_RETAINED_BACKTEST_ONLY` | `6ea64754-003d-48d8-ad9e-d0e7857716c8` | Underperformed no-HMM |
| 4 | `REGIME_BOUNDED_RETRAINED_20260510` | `bf4eda9d-d252-46f8-a063-fb3f95f49a1e` | Underperformed no-HMM |

Key metrics:

| Loop | Annualized Return | CAGR | Sharpe | MaxDD | Final NAV | Delta AnnRet vs no-HMM |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 no-HMM | `0.381778` | `0.730577` | `1.690212` | `-0.154973` | `2.616853` | `0.000000` |
| 2 COVFIX | `0.362050` | `0.692295` | `1.573572` | `-0.169608` | `2.516168` | `-0.019728` |
| 3 Loop10 | `0.372627` | `0.712238` | `1.631749` | `-0.176996` | `2.568409` | `-0.009151` |
| 4 Regime bounded | `0.368239` | `0.704819` | `1.611568` | `-0.169910` | `2.548921` | `-0.013539` |

Diagnosis:

- No-HMM is still the best in this controlled backtest-only comparison.
- All tested HMM overlays reduced annualized return and worsened or failed to improve drawdown.
- IC/RankIC are identical across loops because the source stock model is reused; HMM only changes score overlay/ranking/trades.
- The new regime-linear candidate did not beat retained Loop10 or no-HMM.
- Result summary saved locally at `.codex_tmp/qe_monitor/qe_20260510_010004_8c2d_loop_summary.json`.
- Diagnostic JSON saved locally at `.codex_tmp/qe_monitor/qe_20260510_010004_8c2d_diagnostic_rerun.json`.

Important fix during Round 1:

- Loop4 initially failed because snapshot `bf4eda9d-d252-46f8-a063-fb3f95f49a1e` had a `model_path` under the worktree, which production `8001` did not allow as an HMM artifact root.
- The HMM assets were copied to `F:\Dev\AIstock\backend\data\hmm_models\da8f18dc-53d3-4243-a04a-07c69be89f06\2026-05-10\`.
- DB `model_train_snapshots.model_path` was updated to `F:\Dev\AIstock\backend\data\hmm_models\da8f18dc-53d3-4243-a04a-07c69be89f06\2026-05-10\models.json`.
- Coefficient precompute validated with `days=442` and `stock_sector_map=5847`.

## Registered Hidden HMM Snapshots

The following hidden snapshots are registered in DB and backed by production-root artifacts under `F:\Dev\AIstock\backend\data\hmm_models`.

Round 1 registered candidate:

| Candidate | Snapshot ID | Role |
| --- | --- | --- |
| `REGHMM_REGIMELINEAR_BOTH_T20_B15_BOOST0p01_PEN0p005` | `bf4eda9d-d252-46f8-a063-fb3f95f49a1e` | Tested in `qe_20260510_010004_8c2d` Loop4; underperformed no-HMM |

Round 2 candidates registered after Round 1 result:

| Candidate | Snapshot ID | Asset Path |
| --- | --- | --- |
| `REGHMM_REGIMELINEAR_BOTH_T20_B15_BOOST0p005_PEN0p005` | `d2da20b1-f3c5-410b-aee9-9d71dff4e846` | `F:\Dev\AIstock\backend\data\hmm_models\444c14d8-87ef-43dd-8442-7e45c74d7e05\2026-05-10\models.json` |
| `REGHMM_REGIMETOPBOTLINEAR_BOTH_T20_B15_BOOST0p005_PEN0p005` | `41e5cea2-a8be-47ee-a3ca-831c9609be16` | `F:\Dev\AIstock\backend\data\hmm_models\ef7608e2-4b59-41af-947a-fcef0478e7c0\2026-05-10\models.json` |
| `REGHMM_REGIMETOPBOTLINEAR_BOTH_T20_B20_BOOST0p005_PEN0p03` | `8834983a-7a44-4073-8108-d509faa92a31` | `F:\Dev\AIstock\backend\data\hmm_models\8eea5ce8-9b1d-4d39-b041-9903b140fefd\2026-05-10\models.json` |

Validation status for all three Round 2 snapshots:

- `model_train_snapshots.status = completed`
- `ConfigComposer()._precompute_hmm_coefficients(...)` passed
- Daily coefficient coverage: `442` trading days
- Stock-sector map size: `5847`

## Current Running QE Round 2

Task: `qe_20260510_102726_4fd3`

Purpose: after Round 1 showed all tested HMM overlays underperformed no-HMM, test gentler and more risk-gated retrained regime candidates from the same offline redefinition pipeline.

Current state as of this handoff:

- Task status: `running`
- All 4 loops: `running`
- Node: `rdagent-node1`
- Mode: `parallel_4`
- Backtest mode: `backtest_only`
- Source model: `qe_20260502_131502_9b54/Loop1`
- Remote `run.log` confirmed real execution started; it is not merely locally marked as running.

Loops:

| Loop | Label | Snapshot |
| --- | --- | --- |
| 1 | `NO_HMM_CONTROL_R2` | none |
| 2 | `REGIME_LINEAR_GENTLE_B005_P005` | `d2da20b1-f3c5-410b-aee9-9d71dff4e846` |
| 3 | `REGIME_TOPBOT_GENTLE_B005_P005` | `41e5cea2-a8be-47ee-a3ca-831c9609be16` |
| 4 | `REGIME_TOPBOT_RISK_B005_P030` | `8834983a-7a44-4073-8108-d509faa92a31` |

Monitor:

- Background monitor process started with PID `88864` in this session.
- Latest status file: `.codex_tmp/qe_monitor/qe_20260510_102726_4fd3_latest_status.json`
- Monitor log: `.codex_tmp/qe_monitor/qe_20260510_102726_4fd3_monitor.jsonl`
- Auto diagnostic output after terminal status: `.codex_tmp/qe_monitor/qe_20260510_102726_4fd3_diagnostic.json`

If a new Codex window starts, do not rely on the PID still being alive. Just query the task from API:

```powershell
$tid='qe_20260510_102726_4fd3'
Invoke-RestMethod -Uri "http://127.0.0.1:8001/api/v1/quantevolver/evolution/tasks/$tid" -TimeoutSec 30
```

## Code And Commits

Committed and pushed:

- `2733cb7 feat(hmm): register bounded regime candidate for qe`
- Added script: `scripts/register_hmm_regime_bounded_qe_candidate_20260510.py`

This script registers the best bounded regime-HMM candidate as a hidden QE snapshot. It is not a training script; it packages precomputed offline coefficients into DB/runtime assets.

Not committed by design:

- `.codex_tmp/` offline outputs, monitor scripts, temporary QE payloads, and generated scratch CSV/JSON.
- Generated HMM model assets under `F:\Dev\AIstock\backend\data\hmm_models` are runtime/data assets, not source code commits.

## How To Continue In A New Window

1. Start from worktree `F:\Dev\AIstock_worktrees\hmm-sector-regime-20260509` on branch `codex/hmm-sector-regime-20260509`.
2. Read `docs/codex_project_memory.md` and this handoff file.
3. Check current task:

```powershell
$tid='qe_20260510_102726_4fd3'
$detail=Invoke-RestMethod -Uri "http://127.0.0.1:8001/api/v1/quantevolver/evolution/tasks/$tid" -TimeoutSec 30
$detail.data.status
$detail.data.loops | Select-Object loop_index,status,node_id,experiment_id,updated_at
```

4. If task is still running, optionally probe remote logs through `QEWorkspaceClient.for_node('rdagent-node1')` and continue monitoring.
5. If task is completed, extract metrics into a compact summary similar to `.codex_tmp/qe_monitor/qe_20260510_010004_8c2d_loop_summary.json`.
6. Compare every HMM loop against Loop1 no-HMM. Primary criteria:
   - annualized return
   - Sharpe / information ratio
   - max drawdown
   - final NAV / absolute final value
   - average cash ratio
   - turnover and trade count
7. If no HMM beats no-HMM again, do not keep doing Loop10-style coefficient tweaks. Move to a stricter HMM research direction:
   - HMM should act only as a risk gate, not boost top sectors.
   - Penalize only high-risk sectors or market regimes; avoid replacing strong stock candidates unless risk state is severe.
   - Consider training objective based on realized portfolio harm of HMM-induced entries/exits, not only sector forward-return rank.
   - Add a no-trade/neutral threshold so most days remain exactly no-HMM.
   - Evaluate changed-days and per-day entered/dropped attribution before launching expensive QE.

## Current Research Conclusion

As of Round 1, HMM is not yet adding value in actual QE. The current evidence says the HMM overlay tends to disturb the baseline stock model more than it improves sector timing. Round 2 is testing whether gentler/risk-gated retrained regime candidates can reduce this harm. If Round 2 also fails to beat no-HMM, the next useful direction is not another coefficient micro-tune, but a redesigned HMM training target that directly optimizes risk-gating and minimizes unnecessary TopK replacement.
