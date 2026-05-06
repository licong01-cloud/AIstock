# HMM Stage3 Sparse QE Final Analysis - qe_20260505_210355_155f

- Generated at: 2026-05-05T23:52:14.229761+08:00
- Validation level: L3 QE/HMM remote backtest validation
- Task status: `completed`; loops completed: 8/8
- Business goal: validate whether retrained Stage3 sparse HMM sector-rotation maps can beat the current Loop10 HMM baseline without retraining the base stock-alpha model.
- False-success risks checked: duplicate model training, wrong mlruns source, selector pollution, annual-return-only selection, and hidden cost/turnover regressions.

## Commands And Evidence

- QE diagnostic: `python scripts/qe_evolution_diagnostic.py qe_20260505_210355_155f --json --api-base http://127.0.0.1:8001/api/v1`
- Remote no-training proof: SSH log scan saved to `.codex_tmp/hmm_stage3_sparse_qe_20260505/final_remote_backtest_only_proof.json`.
- Attribution/turnover helper: `python scripts/diagnostics/hmm_qe_candidate_attribution.py qe_20260505_210355_155f --registry .codex_tmp/hmm_registry_updates/hmm_stage3_sparse_registry_result_20260505_210214.json --output-dir .codex_tmp/hmm_stage3_sparse_qe_20260505/attribution`.
- Loop summary CSV: `.codex_tmp/hmm_stage3_sparse_qe_20260505/final_analysis/loop_summary.csv`.
- API task snapshot: `.codex_tmp/hmm_stage3_sparse_qe_20260505/final_task_detail.json`.

## Loop Ranking

| Rank | Loop | Label | AnnRet | Sharpe | MaxDD | AvgTurnover | Delta Ann vs Loop10 | HMM Snapshot |
|---:|---:|---|---:|---:|---:|---:|---:|---|
| 1 | 2 | `LOOP10_BASE__penalty_only_f096__current_best` | 0.480163 | 2.076938 | -0.157395 | 0.082781 | 0.00000000 | `6ea64754-003d-48d8-ad9e-d0e7857716c8` |
| 2 | 7 | `STAGE3_SPARSE_TL_B15_PEN_0p995` | 0.479795 | 2.080019 | -0.157409 | 0.081970 | -0.00036763 | `db001359-2ef4-4db3-8cab-c68bc1ea18b2` |
| 3 | 6 | `STAGE3_SPARSE_FB_B05_PEN_0p995` | 0.477126 | 2.067620 | -0.157397 | 0.082468 | -0.00303678 | `c5fe7775-1b32-47a9-9d8b-e02610f89f4d` |
| 4 | 3 | `LOOP2_BASE__old_covfix_w3_raw__drawdown_control` | 0.475617 | 2.064530 | -0.155894 | 0.084421 | -0.00454612 | `bbec3863-fb67-445f-938e-66f092d18696` |
| 5 | 5 | `STAGE3_SPARSE_TL_B10_PEN_0p995` | 0.472091 | 2.054112 | -0.157412 | 0.081218 | -0.00807210 | `9869553f-632d-498c-8021-b1e15c2c1db8` |
| 6 | 4 | `STAGE3_SPARSE_TL_B05_PEN_0p995` | 0.469190 | 2.031342 | -0.157406 | 0.083167 | -0.01097248 | `7d7e78c0-1e2c-4796-a97d-dbe7371b08ef` |
| 7 | 8 | `STAGE3_SPARSE_FB_B20_PEN_0p995` | 0.464811 | 2.020081 | -0.160739 | 0.081764 | -0.01535192 | `19382026-9950-4764-bc7f-46cb1778b29e` |
| 8 | 1 | `NO_HMM__bt_source_loop1_control` | 0.462117 | 1.994239 | -0.165808 | 0.083869 | -0.01804614 | `NO_HMM` |

## Interpretation

- Loop2 / Loop10 baseline remains best by annualized return: `0.480163`, Sharpe `2.076938`, MaxDD `-0.157395`.
- Loop7 / `STAGE3_SPARSE_TL_B15_PEN_0p995` is the only near-miss: annualized return is lower by `0.000368` (~3.68 bps absolute annualized), while Sharpe is higher by `0.003081`; MaxDD is effectively tied but slightly worse.
- Loop6 is the second sparse candidate, but annualized return is lower than Loop10 by about `0.003037` (~30.37 bps), so it is not promotion-ready.
- Sparse Stage3 candidates are directionally useful because several beat no-HMM and approach Loop10; however none beats Loop10 on the primary objective in this QE run.
- IC/RankIC are identical across all loops because this experiment reuses the same trained base prediction model; HMM only changes the post-prediction sector/score overlay and final ranking/backtest path.

## Backtest-Only Proof

- All remote loops verified backtest-only: `True`.
- Each Loop1-Loop8 log contains `--backtest-only`, `Symlink mlruns`, and `Backtest-only mode: skipping model training, loading existing model`.
- Each loop `mlruns` symlink resolves to `/home/lc999/projects/RD-Agent-main/qe_workspace/qe_20260505_123035_bf80/Loop1/mlruns`.

## Selector Hygiene

- Visible `sector_hmm` configs: 2 -> ['HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504', 'HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore']
- Hidden experimental sparse configs: 5 -> ['HMM_TEST_STAGE3_SPARSE_TL_B05_PEN_0p995__qe20260505', 'HMM_TEST_STAGE3_SPARSE_TL_B10_PEN_0p995__qe20260505', 'HMM_TEST_STAGE3_SPARSE_FB_B05_PEN_0p995__qe20260505', 'HMM_TEST_STAGE3_SPARSE_TL_B15_PEN_0p995__qe20260505', 'HMM_TEST_STAGE3_SPARSE_FB_B20_PEN_0p995__qe20260505']
- Conclusion: production/QE visible selector stays clean; sparse candidates are hidden but directly usable by custom QE loops.

## Decision

- Keep Loop10 as the current best production/reference HMM version.
- Keep Loop7 / Stage3 sparse turnover-light B15 as a research candidate for robustness and cost-stress validation; do not promote yet.
- Do not promote Loop4/Loop5/Loop6/Loop8 from this batch; they are useful as evidence but not as selectable production candidates.
- Next R&D should stay Loop10-centered and focus on sparse/regime-conditional maps rather than broad continuous Stage3 coefficient replacement.

## Residual Risks

- This run validates one backtest window and one TopK/minute execution setup; robustness over alternate date windows and TopK settings is still unverified.
- Attribution TopK replay uses the diagnostic helper source artifacts and should be treated as secondary evidence; QE realized backtest metrics are the primary decision basis.
- Cost drag in the metric helper is zero for this run, so explicit transaction-cost stress tests are still needed before promotion.
