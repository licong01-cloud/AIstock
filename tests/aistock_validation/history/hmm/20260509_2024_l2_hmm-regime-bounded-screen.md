# HMM Regime Bounded Screen Validation - 2026-05-09

## Scope

- Module: HMM model R&D / offline diagnostics.
- Goal: convert redefined regime-HMM scores into bounded sector coefficients and screen for QE-ready candidates without mutating QE or registry state.
- Runtime impact: no QE task submitted, no DB registry write, no production backend restart.
- Output dirs:
  - `.codex_tmp/hmm_regime_bounded_candidate_screen_20260509_smoke3`
  - `.codex_tmp/hmm_regime_bounded_candidate_screen_20260509_run2`

## Code Changes

- Added/updated `scripts/hmm_regime_bounded_candidate_screen_20260509.py`.
- Added `docs/analysis/hmm_regime_bounded_screen_20260509.md`.
- Updated `docs/analysis/hmm_training_current_status_20260503.md` with the latest bounded-screen status.

## Validation

```powershell
python -m py_compile scripts\hmm_regime_bounded_candidate_screen_20260509.py
python -m pytest backend\tests\test_hmm_forward_filter.py backend\tests\test_hmm_daily_coefficients.py -q
```

```bash
wsl bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/AIstock_worktrees/hmm-sector-regime-20260509 && PYTHONUNBUFFERED=1 python scripts/hmm_regime_bounded_candidate_screen_20260509.py --env-file /mnt/f/Dev/AIstock/.env --redefine-dir .codex_tmp/hmm_sector_rotation_redefine_20260509_oriented_full --output-dir .codex_tmp/hmm_regime_bounded_candidate_screen_20260509_smoke3 --hmm-diag-dir /mnt/f/Dev/AIstock_worktrees/hmm-evo-baseline-20260506/.codex_tmp/hmm_offline_diag/qe_20260506_220823_6489 --source ROT_REGIME_LINEAR_v1__INV --shortlist 1 --topk 50 --skip-db-forward"

wsl bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/AIstock_worktrees/hmm-sector-regime-20260509 && PYTHONUNBUFFERED=1 python scripts/hmm_regime_bounded_candidate_screen_20260509.py --env-file /mnt/f/Dev/AIstock/.env --redefine-dir .codex_tmp/hmm_sector_rotation_redefine_20260509_oriented_full --output-dir .codex_tmp/hmm_regime_bounded_candidate_screen_20260509_run2 --hmm-diag-dir /mnt/f/Dev/AIstock_worktrees/hmm-evo-baseline-20260506/.codex_tmp/hmm_offline_diag/qe_20260506_220823_6489 --shortlist 32 --topk 50"
```

## Results

- Smoke passed with `--shortlist 1 --skip-db-forward`.
- Expanded screen passed with `--shortlist 32`.
- Best balanced candidate: `REGHMM_REGIMELINEAR_BOTH_T20_B15_BOOST0p01_PEN0p005`.
- Other top/bottom variants improved recent holdout, but several were weaker on the earlier train-pre-holdout window and were not promoted as the primary line.

## Residual Risk

- This is still an offline screen only. No hidden QE snapshot has been registered yet.
- Sector coefficient candidates that look good on recent holdout can still be regime-sensitive; a hidden QE step is still required before treating them as production-relevant.
