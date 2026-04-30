# HMM Strict Default Dynamic Training & Registration Report

Date: 2026-04-29

## Objective

Train and register dynamic HMM candidates that can be used by QE default-window backtests without future-information leakage.

## Strict Split

```text
+------------+------------+------------+----------------------------------------------+
| Segment    | Start      | End        | Note                                         |
+------------+------------+------------+----------------------------------------------+
| Train      | 2021-01-04 | 2024-02-29 | Model fit only                               |
| Validation | 2024-03-01 | 2024-05-30 | State utility calibration                    |
| Label End  | -          | 2024-06-28 | 20 trading days after validation end         |
| QE Test    | 2024-07-01 | 2026-03-10 | QE default test_end                          |
| Backtest   | 2024-07-01 | 2026-03-03 | QE derived backtest_end = test_end - 7 days  |
+------------+------------+------------+----------------------------------------------+
```

Embargo check passed: validation 20D forward label end `2024-06-28` is before QE test start `2024-07-01`.

## Registered Versions

```text
+------+-------------------------------------------------------------+--------------------------------------+--------------------------------------+
| ID   | Display Name                                                | Config ID                            | Snapshot ID                          |
+------+-------------------------------------------------------------+--------------------------------------+--------------------------------------+
| S1   | HMM_DYNAMIC_PUP_w20_50_conf_0p075_STRICT_DEFAULT__n3_diag    | 8ef81e6b-263d-4acd-93ff-4a20526b2d13 | c1c81aa0-aae2-4942-881c-4baafbd2f160 |
| S2   | HMM_DYNAMIC_PUP_w20_50_conf_0p10_STRICT_DEFAULT__n3_diag     | 5a3183b6-39bc-45dd-8b3d-d2027c476e62 | d11dc38e-84f0-4e5c-80e7-42cb5d978d40 |
+------+-------------------------------------------------------------+--------------------------------------+--------------------------------------+
```

## Artifacts

```text
+------+------------+------------+------+-------+--------+----------+----------------+
| ID   | Start      | End        | Days | Stock | Sector | Preset   | Strict         |
+------+------------+------------+------+-------+--------+----------+----------------+
| S1   | 2024-07-01 | 2026-03-03 | 404  | 5621  | 131    | preset_A | True           |
| S2   | 2024-07-01 | 2026-03-03 | 404  | 5621  | 131    | preset_A | True           |
+------+------------+------------+------+-------+--------+----------+----------------+
```

- S1 model: `backend/data/hmm_models/8ef81e6b-263d-4acd-93ff-4a20526b2d13/2026-04-29/models.json`
- S1 coefficients: `backend/data/hmm_models/8ef81e6b-263d-4acd-93ff-4a20526b2d13/2026-04-29/coefficients_preset_A_2024-07-01_2026-03-03.json`
- S2 model: `backend/data/hmm_models/5a3183b6-39bc-45dd-8b3d-d2027c476e62/2026-04-29/models.json`
- S2 coefficients: `backend/data/hmm_models/5a3183b6-39bc-45dd-8b3d-d2027c476e62/2026-04-29/coefficients_preset_A_2024-07-01_2026-03-03.json`

Stock-sector map policy: static as of QE test start `2024-07-01`, avoiding future sector membership leakage.

## Script-Level Qlib Validation

This is not a QE experiment; it is the same script-only Top50 / 5D qlib sanity check used to screen HMM direction before QE.

```text
+------+-------------------------------------------------------------+-----------+----------+-----------+----------------+
| ID   | Version                                                     | Total     | Sharpe   | MaxDD     | 20D Spread     |
+------+-------------------------------------------------------------+-----------+----------+-----------+----------------+
| BASE | NO_HMM_BASELINE                                             | -65.23%   | -0.951   | -65.81%   | -              |
| S1   | strict_default_pup_w20_50_clip_0p9800_1p0150_conf_0p075     | -59.02%   | -0.855   | -60.58%   | +0.59%         |
| S2   | strict_default_pup_w20_50_clip_0p9800_1p0150_conf_0p10      | -58.33%   | -0.837   | -59.91%   | +0.62%         |
+------+-------------------------------------------------------------+-----------+----------+-----------+----------------+
```

## Verification

```text
+------+--------------------------------------------------------------+----------+
| No   | Check                                                        | Result   |
+------+--------------------------------------------------------------+----------+
| 1    | DB sector_hmm row count                                      | 3        |
| 2    | Old leaky PIT1Y dynamic versions in DB                        | 0        |
| 3    | Strict S1/S2 snapshots completed                              | Passed   |
| 4    | Coefficient files cover QE default backtest window             | Passed   |
| 5    | ConfigComposer default split resolves both strict artifacts     | Passed   |
| 6    | DB strict config precomputed_only / no runtime fallback         | Passed   |
| 7    | py_compile relevant HMM/QE scripts                             | Passed   |
| 8    | Strict non-default HMM window fail-fast guard                   | Passed   |
| 9    | unified_engine pytest suite                                    | 74 passed|
+------+--------------------------------------------------------------+----------+
```

## Commands

```powershell
wsl bash /mnt/f/Dev/AIstock/.codex_tmp/run_hmm_dynamic_strict_default_train.sh
$env:TDX_DB_PASSWORD='lc78080808'; python scripts/register_strict_dynamic_hmm_candidates.py
python -m py_compile scripts/hmm_dynamic_strict_default_train.py scripts/register_strict_dynamic_hmm_candidates.py backend/services/quantevolver/config_composer.py backend/services/quantevolver/experiment_config.py backend/services/quantevolver/experiment_config_builders.py backend/routers/quantevolver.py backend/routers/quantevolver_evolution.py
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_multi_alpha_command_generation.py -q
```

## Residual Notes

- These two strict versions are suitable for QE default-window comparison from a split/embargo perspective.
- Runtime now rejects strict HMM requests whose `preset/test_start/backtest_end` do not match a registered strict coefficient window, and refuses real-time fallback when a strict precomputed coefficient file is missing.
- The DB `config_json` for S1/S2 is marked `precomputed_only=true`, `runtime_generation_supported=false`, `strict_runtime_guard=exact_registered_window_precomputed_file_only`.
- They still use a static stock-sector map as of `2024-07-01` because the current QE HMM coefficient schema consumes a static `stock_sector_map`; this is conservative and avoids future membership leakage, but it will not adjust stocks that only become mappable after the test start.
- A future rolling/PIT HMM runtime would be more realistic, but it requires a larger runtime schema change.
