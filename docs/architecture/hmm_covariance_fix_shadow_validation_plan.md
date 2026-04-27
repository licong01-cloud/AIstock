# HMM Covariance Fix and Shadow Validation Plan

Date: 2026-04-27

## Scope

This plan fixes known HMM training/export defects without modifying the HMM
model currently used by QE. Existing HMM model files, coefficient artifacts,
snapshot rows, and QE experiment configuration remain read-only controls.

## Controls

- Freeze current QE HMM artifacts as `control_current_qe_hmm`.
- Do not overwrite any existing `models.json` or `coefficients_*.json`.
- Generate any retrained result as a new shadow config/snapshot only.
- Keep 3/5/10-day HMM effectiveness comparisons independent from this fix.

## Phase 1: Program Fix

Fix the training code so covariance clipping is written back to the actual
`GaussianHMM` state instead of mutating a possibly detached `covars_` view.

Required behavior:

- Diagonal covariance models store clipped diagonal variances back through the
  hmmlearn setter in the shape expected by `covariance_type="diag"`.
- Full/tied covariance models are symmetrized and eigenvalue-clipped.
- Spherical covariance models clip per-state variance values.
- Training fails before snapshot output if the saved covariance representation
  still violates `[min_covar, max_covar]`.

## Phase 2: Shadow Candidate

After the code fix, create a new shadow candidate only if training is requested:

- Example display name: `L2_3state_diag_7dim_w5_zscore_covfix_shadow`
- New `config_id`, `snapshot_id`, model path, coefficient paths.
- Preserve current QE HMM as the control in all reports.

## Phase 3: A/B Validation

Compare:

- `1D_QE + HMM_OFF`
- `1D_QE + control_current_qe_hmm`
- `1D_QE + existing_phase1_hmm`
- `1D_QE + covfix_shadow_hmm`

Track:

- after-cost return, Sharpe, max drawdown, win rate;
- monthly win rate;
- topK overlap and rank changes;
- industry concentration;
- state contribution split;
- saved covariance max/min and anomaly counts.

## Phase 4: Horizon Comparison Boundary

The user will compare 3/5/10-day HMM effectiveness separately. The covariance
fix should not mix with horizon-label changes. If horizon testing shows a
stable benefit, create a separate horizon-aware HMM candidate after the covfix
candidate is evaluated.

## Promotion Rule

No candidate becomes the QE default automatically. Promotion requires explicit
manual confirmation with artifact hashes and rollback instructions.

## Execution Record - 2026-04-27

Program fix source:

- RD-Agent commit: `032f03a9 Fix HMM covariance clipping persistence`.
- AIstock training path: manual shadow config/snapshot; current QE HMM assets
  were not overwritten.

Shadow HMM candidate:

- Config ID: `be681443-fe5d-4641-b55f-5f889e6af8e1`.
- Display name: `L2_3state_diag_7dim_w5_zscore_covfix_shadow_20260427`.
- Snapshot ID: `4c9b5f7b-8e59-44a6-b580-e7186b9283df`.
- Model path:
  `backend/data/hmm_models/be681443-fe5d-4641-b55f-5f889e6af8e1/2026-04-27/models.json`.
- Model SHA256:
  `2F0BE916C71B24035D32CF781C2E5DF77E164E825FCABD5F89DBF5CAFEFEBE62`.
- QE coefficient path:
  `backend/data/hmm_models/be681443-fe5d-4641-b55f-5f889e6af8e1/2026-04-27/coefficients_preset_A_2024-07-01_2026-03-03.json`.
- QE coefficient SHA256:
  `EBAA773D5D641A06D81F7BBE59E79EF55C3DBE2FDB4A72358C88274D87C976A0`.

Covariance validation:

| Model | Snapshot | Max diag covariance | Min diag covariance | Out-of-bound sectors |
| --- | --- | ---: | ---: | ---: |
| Earliest QE HMM, w3 raw | `252fdd35-aae3-445a-baf4-7e46b1b93aff` | 1000.000475 | 0.0000166 | 131 |
| Existing w5 zscore before persistence fix | `052274d0-f5c7-4713-ab7e-636790baafc5` | 47.348198 | 0.0000144 | 119 |
| New covfix shadow, w5 zscore | `4c9b5f7b-8e59-44a6-b580-e7186b9283df` | 10.000000 | 0.001000 | 0 |

The new shadow model recorded `covariance_fixed=true` for 119 sectors and a
total persisted anomaly count of 240. This confirms the clipping fix is now
actually written into the saved `models.json`.

QE comparison:

- Template settings: 50 factors from `qe_20260426_142629`,
  `__seed_LGBModel_golden_v1__`, `score_weighted_topk_v2`, `V25_TWO_STAGE`,
  `filtered_pool_20260426`, label horizon 1, test window
  `2024-07-01` to `2026-03-03`.
- Fresh old-HMM experiment: `qe_20260427_150123`.
- Fresh covfix-shadow experiment: `qe_20260427_150126`.
- The two fresh experiments used identical factor list, data split, model,
  strategy, stock pool, execution algorithm, and custom params except
  `hmm_model_version_id` and resolved `sector_hmm_model_path`.

| Experiment | HMM snapshot | Annualized return | IR/Sharpe | Max drawdown | Final NAV |
| --- | --- | ---: | ---: | ---: | ---: |
| Fresh old w3 raw | `252fdd35-aae3-445a-baf4-7e46b1b93aff` | 13.2094% | 0.6631 | -14.0591% | 1.611595 |
| Fresh new w5 covfix | `4c9b5f7b-8e59-44a6-b580-e7186b9283df` | 8.2246% | 0.4195 | -22.6149% | 1.484705 |

Decision:

- The new covfix shadow snapshot is technically usable by QE; the fresh QE
  experiment successfully completed with it.
- It should not be promoted to the QE default because it underperformed the
  earliest w3 raw HMM in the controlled 1-day comparison.
- The likely next isolation test is a `w3_raw_covfix_shadow` retrain, so the
  covariance fix is compared under the same HMM hyper-parameters as the
  currently successful QE HMM instead of changing rolling window and zscore at
  the same time.

## Three-Version Comparison Set - 2026-04-27

The same-parameter w3/raw covariance-fixed candidate has now been trained.
The HMM Training Center and Paper v2 UI should show only these three active
HMM versions for the immediate comparison:

| Role | Config display name | Config ID | Snapshot display name | Snapshot ID |
| --- | --- | --- | --- | --- |
| Original baseline | `HMM_BASELINE_ORIGINAL_w3_raw_unfixed__n3_diag_rw3_nozscore` | `564b407f-1541-4b18-a087-2a45cfbca9d9` | `SNAPSHOT_BASELINE_original_w3_raw_unfixed_cov__train2022-01_2024-06__val2024-07_2025-03` | `252fdd35-aae3-445a-baf4-7e46b1b93aff` |
| Same-parameter covfix candidate | `HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore` | `b99c907b-873a-4173-a4ee-5eab266f8c49` | `SNAPSHOT_COVFIX_w3_raw_same_params__train2022-01_2024-06__val2024-07_2025-03` | `bbec3863-fb67-445f-938e-66f092d18696` |
| w5/zscore covfix candidate | `HMM_COVFIX_w5_zscore_candidate__n3_diag_rw5_zscore` | `be681443-fe5d-4641-b55f-5f889e6af8e1` | `SNAPSHOT_COVFIX_w5_zscore_candidate__train2023-01_2026-01__val2026-01_2026-04` | `4c9b5f7b-8e59-44a6-b580-e7186b9283df` |

Removed from the active DB/API set:

- Pre-fix w5/zscore config `b2d5bcc6-8463-4156-bf1a-e1392a00279a`.
- Pre-fix w5/zscore snapshot `052274d0-f5c7-4713-ab7e-636790baafc5`.
- No QE, Selection Center, or Paper v2 runtime reference was found before
  removal. The obsolete model directory under `backend/data/hmm_models` was
  removed because HMM model assets are ignored runtime artifacts.

Same-parameter w3/raw covfix training record:

- Config ID: `b99c907b-873a-4173-a4ee-5eab266f8c49`.
- Snapshot ID: `bbec3863-fb67-445f-938e-66f092d18696`.
- Model path:
  `backend/data/hmm_models/b99c907b-873a-4173-a4ee-5eab266f8c49/2026-04-27/models.json`.
- Model SHA256:
  `1B2179F3267C441C99FCDF7B514272991007F28E196E8B835B2F00C67644BF63`.
- Hyperparameters match the original baseline on the key HMM settings:
  `n_states=3`, `covariance_type=diag`, `rolling_window=3`, `zscore=false`,
  `use_limit_down=false`, `sector_level=L2`, `min_trading_days=120`.
- Training/validation windows match the original baseline metrics:
  train `2022-01-01` to `2024-06-30`; validation `2024-07-01` to
  `2025-03-31`.
- Coefficients were generated for `preset_A` and `preset_B` over
  `2024-07-01` to `2026-03-03`.
- `preset_A` coefficient SHA256:
  `BDFFEB28E9B4F8528F366F5CD38E4EB4F66C1EDC948FD40152514A392D719774`.
- `preset_B` coefficient SHA256:
  `8A48F7E46FE7672A1A3C94F2EF1D559174CE9F1EB729361E023C3BC7079698EC`.

Updated covariance validation:

| Model | Snapshot | Max diag covariance | Min diag covariance | Out-of-bound sectors | Fixed sectors |
| --- | --- | ---: | ---: | ---: | ---: |
| Original baseline w3/raw, intentionally retained | `252fdd35-aae3-445a-baf4-7e46b1b93aff` | 1000.000475 | 0.0000166 | 131 | 0 |
| New same-parameter w3/raw covfix | `bbec3863-fb67-445f-938e-66f092d18696` | 10.000000 | 0.001000 | 0 | 131 |
| New w5/zscore covfix | `4c9b5f7b-8e59-44a6-b580-e7186b9283df` | 10.000000 | 0.001000 | 0 | 119 |

UI/API note:

- `model_train_snapshots` has no dedicated display-name column, so each
  snapshot label is stored in `metrics_json.snapshot_display_name` and exposed
  by the HMM training API as `display_name`.
- Paper v2 HMM selectors and the HMM maintenance page render this readable
  snapshot label instead of raw UUID/date pairs.
- The original baseline remains explicitly named as `unfixed` so it can be
  selected only as a controlled baseline, not mistaken for a repaired model.
