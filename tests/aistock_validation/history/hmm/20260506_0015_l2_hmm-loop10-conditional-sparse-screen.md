# HMM Loop10 Conditional Sparse Screen - 2026-05-06

## Scope

- Module: HMM / QE pre-validation.
- Level: L2 script-level diagnostic plus L0 guardrails.
- Goal: start the next evolution after Stage3 sparse QE by keeping Loop10 as the anchor and testing only sparse/conditional HMM sector adjustments before any long remote QE run.
- Production impact: no backend restart, no HMM registry/snapshot write, no QE submission.

## Changed Code

- Added `scripts/hmm_loop10_conditional_sparse_screen_20260506.py`.
- Candidate families: add sparse penalty, persistent sparse penalty, deepen existing Loop10 penalties when Stage3 confirms bad, relax Loop10 when Stage3 says good, and vote/confirm gates.
- Uses retrained Stage3 score panels as HMM inputs/signals; it does not retrain HMM in this step and does not modify protected model assets.

## Commands

```powershell
python -m py_compile scripts/hmm_loop10_conditional_sparse_screen_20260506.py
rg -n "except Exception|INSERT|UPDATE|DELETE|requests\.|8000|8001|quantevolver|model_train_|register|submit_qe|POST|http://|psycopg|sqlalchemy|create_engine" scripts/hmm_loop10_conditional_sparse_screen_20260506.py -S
python scripts/hmm_loop10_conditional_sparse_screen_20260506.py `
  --output-dir .codex_tmp/hmm_loop10_conditional_sparse_screen_20260506 `
  --task-id qe_20260505_210355_155f `
  --pcts 0.10 0.15 0.20 `
  --penalties 0.998 0.9975 0.995 `
  --tighten-penalties 0.955 0.95 `
  --persist-days 2 `
  --vote-thresholds 2
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- scripts/hmm_loop10_conditional_sparse_screen_20260506.py
```

## Results

- Tested candidates: 102.
- Strongest new family: `tighten_existing_loop10`, i.e. do not replace Loop10; only deepen existing Loop10 penalties when retrained Stage3 HMM also confirms the sector is bad.
- Negative/weak families: broad `confirm_loop10_penalty`, `vote_confirm_loop10_penalty`, and `relax_loop10_good` were weak or negative in this screen.
- Important calibration: `L10_ADD_TL_P15_PEN_0p995` corresponds to the previous near-best Loop7 sparse candidate; `L10_ADD_FB_P20_PEN_0p995` resembles the previous Loop8 family that failed QE despite good script metrics, so QE promotion should prioritize genuinely new tighten candidates rather than re-running known weak add-only variants.

## Best Candidates By Family

```text
                       family                         candidate  changed_days  avg_entered_per_day  net_mean_db_ret_5d  net_mean_db_ret_10d  net_mean_db_ret_20d  robust_screen_score
      tighten_existing_loop10     L10_TIGHTEN_FBT_P15_PEN_0p955            18             0.075314            0.032367             0.028898             0.039464             0.014839
           add_sparse_penalty          L10_ADD_FB_P20_PEN_0p995            30             0.129707            0.004715             0.019884             0.023098             0.012825
persistent_add_sparse_penalty L10_ADD_FB_P20_PERSIST2_PEN_0p995            24             0.100418            0.007837             0.020133             0.026658             0.011127
      vote_add_sparse_penalty       L10_ADD_VOTE2_P15_PEN_0p995            36             0.150628            0.000474             0.006456             0.004605             0.004524
            relax_loop10_good            L10_RELAX_GOOD_FBT_P15            70             0.372385           -0.009263             0.002132             0.002967            -0.001504
  vote_confirm_loop10_penalty        L10_CONFIRM_ONLY_VOTE2_P15           193             1.485356           -0.003223            -0.000672            -0.002825            -0.002813
       confirm_loop10_penalty          L10_CONFIRM_ONLY_FBT_P20           195             1.543933           -0.002468            -0.002317             0.001297            -0.003516
```

## Top Holdout Candidates

```text
                        candidate                        family source  pct  penalty  changed_days  avg_entered_per_day  net_mean_db_ret_5d  net_mean_db_ret_10d  net_mean_db_ret_20d  robust_screen_score
    L10_TIGHTEN_FBT_P15_PEN_0p955       tighten_existing_loop10    FBT 0.15    0.955            18             0.075314            0.032367             0.028898             0.039464             0.014839
      L10_TIGHTEN_TL_P15_PEN_0p95       tighten_existing_loop10     TL 0.15    0.950            20             0.083682            0.010254             0.031406             0.040526             0.014061
      L10_TIGHTEN_FB_P15_PEN_0p95       tighten_existing_loop10     FB 0.15    0.950            26             0.108787            0.006191             0.020688             0.048191             0.013353
         L10_ADD_FB_P20_PEN_0p995            add_sparse_penalty     FB 0.20    0.995            30             0.129707            0.004715             0.019884             0.023098             0.012825
      L10_TIGHTEN_FB_P20_PEN_0p95       tighten_existing_loop10     FB 0.20    0.950            39             0.167364            0.012740             0.008027             0.038246             0.012784
    L10_TIGHTEN_FBT_P10_PEN_0p955       tighten_existing_loop10    FBT 0.10    0.955             9             0.037657            0.050741             0.050196             0.059200             0.012294
      L10_TIGHTEN_TL_P20_PEN_0p95       tighten_existing_loop10     TL 0.20    0.950            32             0.133891            0.013657             0.013001             0.023677             0.012174
L10_ADD_FB_P20_PERSIST2_PEN_0p995 persistent_add_sparse_penalty     FB 0.20    0.995            24             0.100418            0.007837             0.020133             0.026658             0.011127
    L10_TIGHTEN_FBT_P20_PEN_0p955       tighten_existing_loop10    FBT 0.20    0.955            20             0.083682            0.023589             0.014581             0.033754             0.010109
L10_ADD_FB_P15_PERSIST2_PEN_0p995 persistent_add_sparse_penalty     FB 0.15    0.995            13             0.054393            0.004864             0.037914             0.030117             0.009689
         L10_ADD_TL_P15_PEN_0p995            add_sparse_penalty     TL 0.15    0.995            15             0.062762            0.007302             0.031744             0.010346             0.009171
L10_ADD_FB_P10_PERSIST2_PEN_0p995 persistent_add_sparse_penalty     FB 0.10    0.995             9             0.037657            0.021097             0.046145             0.022940             0.008869
L10_ADD_TL_P15_PERSIST2_PEN_0p995 persistent_add_sparse_penalty     TL 0.15    0.995            12             0.050209            0.004113             0.037655             0.022792             0.008603
         L10_ADD_FB_P15_PEN_0p995            add_sparse_penalty     FB 0.15    0.995            15             0.062762           -0.008920             0.033480             0.023315             0.008267
      L10_TIGHTEN_TL_P10_PEN_0p95       tighten_existing_loop10     TL 0.10    0.950            16             0.066946            0.021942             0.012750             0.044110             0.007814
```

## Family Summary

```text
                               candidates  best_score  best_10d  median_score
family
tighten_existing_loop10                18    0.014839  0.050196      0.007608
add_sparse_penalty                     27    0.012825  0.071656      0.001373
persistent_add_sparse_penalty          27    0.011127  0.071656      0.002334
vote_add_sparse_penalty                 9    0.004524  0.006627     -0.000442
relax_loop10_good                       9   -0.001504  0.002132     -0.004951
vote_confirm_loop10_penalty             3   -0.002813 -0.000672     -0.007213
confirm_loop10_penalty                  9   -0.003516 -0.002317     -0.009988
```

## Evidence

- Report: `.codex_tmp/hmm_loop10_conditional_sparse_screen_20260506/qe_20260505_210355_155f/conditional_sparse_screen_report.md`
- Ranked holdout table: `.codex_tmp/hmm_loop10_conditional_sparse_screen_20260506/qe_20260505_210355_155f/conditional_sparse_holdout_ranked.csv`
- Candidate coefficients: `.codex_tmp/hmm_loop10_conditional_sparse_screen_20260506/qe_20260505_210355_155f/candidate_coefficients/`
- Metadata: `.codex_tmp/hmm_loop10_conditional_sparse_screen_20260506/qe_20260505_210355_155f/conditional_sparse_candidate_metadata.csv`

## Recommendation

- Do not launch a broad QE batch from all 102 candidates.
- If moving to QE, register only 2-3 hidden candidates from `tighten_existing_loop10`, plus controls: Loop10, previous near-best TL_B15, No-HMM, and old Loop2 baseline.
- Candidate shortlist for QE: `L10_TIGHTEN_FBT_P15_PEN_0p955`, `L10_TIGHTEN_TL_P15_PEN_0p95`, and optionally `L10_TIGHTEN_FB_P15_PEN_0p95`.
- Use backtest-only mode against the already trained source model to avoid retraining the stock alpha model.

## Guardrails

- `py_compile`: passed.
- Mutation scan: no DB writes, HTTP calls, QE submission, or registry write strings in the new script; `rg` returned no matches.
- `nox -s l0`: passed. Five P2 complexity findings are non-blocking and expected for bounded diagnostic loops; no P1/P0 blocking findings.
