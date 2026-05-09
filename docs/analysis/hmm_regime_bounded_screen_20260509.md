# HMM Regime Bounded Screen - 2026-05-09

## Scope

- Goal: move away from Loop10 micro-tuning and test a redefined regime-HMM path with bounded sector coefficients.
- Source artifacts: `.codex_tmp/hmm_sector_rotation_redefine_20260509_oriented_full`
- Offline screen output: `.codex_tmp/hmm_regime_bounded_candidate_screen_20260509_run2`
- Runtime impact: none; no QE task submitted and no DB registry write was performed.

## Validation

- `python -m py_compile scripts/hmm_regime_bounded_candidate_screen_20260509.py`
- `python -m pytest backend\tests\test_hmm_forward_filter.py backend\tests\test_hmm_daily_coefficients.py -q`
- WSL smoke: `--shortlist 1 --skip-db-forward`
- WSL expanded screen: `--shortlist 32`

## Main Result

- The expanded screen found one clearly better balanced candidate: `REGHMM_REGIMELINEAR_BOTH_T20_B15_BOOST0p01_PEN0p005`.
- It is the only selected candidate that satisfied both a recent-holdout gate and a robust-full gate.
- Several top/bottom variants improved recent holdout but were weaker on the earlier train-pre-holdout window, so they should stay secondary.

## Key Candidates

```text
candidate                                              holdout_10d  full_10d   train_pre_10d  avg_entered/day  changed_days  note
-----------------------------------------------------  -----------  ---------  -------------  ---------------  ------------  ------------------------
REGHMM_REGIMELINEAR_BOTH_T20_B15_BOOST0p01_PEN0p005     0.013528     0.011337     0.002847          0.615385            82  best balanced candidate
REGHMM_REGIMETOPBOTLINEAR_BOTH_T20_B15_BOOST0p005_PEN0p005 0.008632   0.001840    -0.002639          0.378205            55  recent holdout only
REGHMM_REGIMETOPBOTLINEAR_BOTH_T20_B15_BOOST0p015_PEN0p005 0.004118   0.013378     0.011614          0.878205           100  robust full, weaker holdout
REGHMM_REGIMETOPBOTLINEAR_BOTH_T20_B20_BOOST0p005_PEN0p03  0.006801   0.002824    -0.003887          0.403846            59  holdout positive, weaker full
```

## Conclusion

- The redefined regime-HMM path is worth continuing, but the best candidate is not a pure top/bottom screen.
- The balanced `REGIMELINEAR` candidate is the first one that looks strong enough for a hidden QE registration step.
- The top/bottom family still has useful short-term signal, but its earlier-window stability is weaker and it should not be treated as the main line.

## Next Step

- If the user wants the next phase, register the balanced candidate first as a hidden QE-selectable snapshot, then decide whether to keep any top/bottom variants as secondary hidden candidates.
