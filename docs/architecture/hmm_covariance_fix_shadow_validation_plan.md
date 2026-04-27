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

- Example display name: `L2_3状态_diag_7维_w5_zscore_covfix_shadow`
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
