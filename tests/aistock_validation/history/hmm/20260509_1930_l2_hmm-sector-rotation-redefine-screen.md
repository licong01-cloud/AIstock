# HMM Sector-Rotation Redefinition Offline Screen - 2026-05-09

## Scope

- Module: HMM model R&D / offline diagnostics.
- Goal: stop Loop10 coefficient micro-tuning and test redefined HMM training approaches for sector rotation.
- Runtime impact: no QE task submitted, no HMM registry write, no model asset committed, no production backend restart.
- Environment: WSL `rdagent-gpu`, local DB read-only via `.env`.

## Code Changes

- Added `scripts/hmm_sector_rotation_redefine_screen_20260509.py`.
- Added `backend/tests/test_hmm_forward_filter.py`.
- Fixed HMM forward-filter calls for current `hmmlearn`: `_hmmc.forward_log` receives probability `startprob_` and `transmat_`, while emission likelihood remains log-space.
- Touched HMM-only files: `backend/quant_models/hmm/sector_hmm.py`, `scripts/hmm_horizon_v2_train.py`, `scripts/hmm_sector_factor_retrain_diagnostic_20260504.py`.

## Candidate Families

- Per-sector HMM with future 5/10/20d sector relative-rank utility labels.
- Per-sector HMM with same-day industry cross-sectional rank features.
- Pooled/global HMM trained on all sector sequences.
- Market-conditioned pooled HMM with market trend/volatility/drawdown features.
- Sticky/top-bottom HMM approximation with higher self-transition floor.
- Daily market/dispersion regime HMM with state-specific train-only ridge maps from sector features to rotation utility.
- Inverted-score calibration variants for all new candidate outputs.

## Validation Commands

```powershell
python -m pytest backend\tests\test_hmm_forward_filter.py backend\tests\test_hmm_daily_coefficients.py -q
python -m py_compile scripts\hmm_sector_rotation_redefine_screen_20260509.py scripts\hmm_horizon_v2_train.py scripts\hmm_sector_factor_retrain_diagnostic_20260504.py backend\quant_models\hmm\sector_hmm.py
```

```bash
wsl bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/AIstock_worktrees/hmm-sector-regime-20260509 && python scripts/hmm_sector_rotation_redefine_screen_20260509.py --env-file /mnt/f/Dev/AIstock/.env --output-dir .codex_tmp/hmm_sector_rotation_redefine_20260509_oriented_full --n-iter 120 --baseline-coefficients LOOP2_COVFIX=/mnt/f/Dev/AIstock/backend/data/hmm_models/b99c907b-873a-4173-a4ee-5eab266f8c49/2026-04-27/coefficients_preset_A_2024-07-01_2026-04-27.json --baseline-coefficients LOOP10_PENALTY=/mnt/f/Dev/AIstock/backend/data/hmm_models/ce4952c1-4b0d-46a7-81f2-ae1d4a249555/2026-05-04/coefficients_preset_A_2024-07-01_2026-04-27.json"
```

## Results

- Data split:
  - Train: 2022-09-01 to 2025-05-30
  - Validation: 2025-06-02 to 2025-08-29
  - Test/QE-like holdout: 2025-09-01 to 2026-04-27
- Panel coverage: 124,581 sector-date rows, 131 L2 sectors, 951 dates.
- Baseline comparison assets:
  - `LOOP2_COVFIX`
  - `LOOP10_PENALTY`

| Rank | Candidate | Composite | Test RankIC 10d | Test Spread 10d | Val RankIC 10d | Val Spread 10d | Interpretation |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | `ROT_REGIME_TOPBOT_LINEAR_v1__INV` | 0.150323 | 0.011083 | 0.006233 | 0.038380 | 0.007756 | Best spread-style candidate, but RankIC below Loop2. |
| 2 | `ROT_REGIME_LINEAR_v1__INV` | 0.136882 | 0.030169 | 0.006427 | 0.039108 | 0.004928 | Best balanced new candidate; spread beats Loop2, RankIC does not. |
| 3 | `ROT_TOPBOTTOM_STICKY_v1__INV` | 0.125529 | 0.006047 | 0.002743 | 0.050515 | 0.009733 | Validation-positive but weak test spread. |
| 4 | `ROT_DRAWDOWN_RISK_v1__INV` | 0.124567 | 0.012263 | 0.006559 | 0.020168 | 0.004565 | Test spread strongest, RankIC weak. |
| 5 | `LOOP2_COVFIX` | 0.100247 | 0.063405 | 0.005535 | 0.084108 | -0.001174 | Still best RankIC baseline. |
| 6 | `LOOP10_PENALTY` | 0.095705 | 0.060239 | 0.004986 | 0.086076 | -0.000841 | Still close to Loop2 by RankIC. |

## Decision

- No redefined HMM candidate is ready to replace `LOOP2_COVFIX` as the primary HMM because none beats Loop2 on both 10d RankIC and 10d top-bottom spread.
- The best new direction is not per-sector or pooled emission HMM; it is daily market/dispersion regime HMM plus state-specific sector rotation maps, especially the inverted top-bottom and rank-utility variants.
- These two candidates are suitable for a second offline phase that converts continuous sector scores into bounded coefficients and stress-tests turnover/capacity before any remote QE run.
- The `hmmlearn.forward_log` fix is mandatory for future HMM inference/training scripts; prior assets should be treated as fixed historical artifacts until explicitly regenerated.

## Artifacts

- Full offline report: `.codex_tmp/hmm_sector_rotation_redefine_20260509_oriented_full/analysis_report.md`
- Metrics: `.codex_tmp/hmm_sector_rotation_redefine_20260509_oriented_full/candidate_metrics.csv`
- Decisions: `.codex_tmp/hmm_sector_rotation_redefine_20260509_oriented_full/candidate_decisions.json`
- Model summaries: `.codex_tmp/hmm_sector_rotation_redefine_20260509_oriented_full/model_meta.json`
