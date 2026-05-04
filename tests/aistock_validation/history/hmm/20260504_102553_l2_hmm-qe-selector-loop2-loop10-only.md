# HMM QE selector cleanup - 2026-05-04

## Scope

- User requested keeping only the QE-visible HMM configs used by Loop2 and Loop10 from `qe_20260504_014618_a9ec`.
- No HMM model files, snapshots, or coefficient artifacts were deleted.
- Non-kept configs were soft-hidden from QE by changing `model_train_configs.model_type`.

## Kept QE-visible configs

```text
Loop2   b99c907b-873a-4173-a4ee-5eab266f8c49  bbec3863-fb67-445f-938e-66f092d18696  HMM_COVFIX_w3_raw_same_params__n3_diag_rw3_nozscore
Loop10  ce4952c1-4b0d-46a7-81f2-ae1d4a249555  6ea64754-003d-48d8-ad9e-d0e7857716c8  HMM_TEST_old_covfix_penalty_only_f096_b000__qe20260504
```

## Soft-hidden configs

New model_type: `sector_hmm_disabled_superseded_by_loop2_loop10_20260504`.

```text
82a40d27-0e96-48a1-882a-4d182a58b931  HMM_TEST_old_covfix_boost_only_p105__qe20260504
22d53160-7195-4e69-86ec-76c19c615a69  HMM_TEST_old_covfix_penalty094_boost103__qe20260504
ea0db9d3-69bf-489e-aa55-c74b6340e68d  HMM_TEST_old_covfix_penalty095_boost104__qe20260504
518ddf2d-e4a0-4bf0-8572-7cea429e27d5  HMM_TEST_old_covfix_penalty095_boost106__qe20260504
14fd8dd6-896d-4a7d-b8be-ec6a7cf44c95  HMM_TEST_hyb_old_primary_turnover_flow_core_c70__qe20260502
90e2771e-3245-45c0-b8ad-471b10b24391  HMM_TEST_old_covfix_primary_b020_p005__qe20260502
94ba4a64-998d-4897-ace2-f0fe06133935  HMM_TEST_sf_turnover_fast_q20_b010_p005__qe20260502
```

## Backup evidence

```text
.codex_tmp/hmm_registry_updates/hmm_qe_visibility_before_20260504_102513.json
.codex_tmp/hmm_registry_updates/hmm_qe_visibility_after_20260504_102513.json
```

## Validation

```powershell
Invoke-RestMethod 'http://127.0.0.1:8001/api/v1/hmm-training/configs?model_type=sector_hmm'
```

Result: exactly 2 configs returned: Loop10 penalty-only and Loop2 old covfix baseline.

```text
counts by model_type
sector_hmm                                                     2
sector_hmm_disabled_ineffective_20260502                      2
sector_hmm_disabled_superseded_by_loop2_loop10_20260504        7
```

```powershell
Invoke-WebRequest 'http://127.0.0.1:8001/api/v1/health'
```

Result: HTTP 200.

## Sector-factor stacking status

- The exact experiment "Loop2 old-covfix full baseline plus sector-factor overlay" has not been run yet.
- Existing sector-factor evidence is weaker/different:
  - `qe_20260502_231229_0565` Loop4 used a hybrid based on weakened old-primary mapping (`b020/p005`) plus sector turnover/flow confirmation.
  - `qe_20260504_014618_a9ec` Loop3 reused that hybrid candidate.
  - `qe_20260504_014618_a9ec` Loop5 tested sector-factor-only ablation.
- None of those equals Loop2 full old-covfix coefficients plus sector-factor gate.

## Residual risk

- This is a DB registry visibility change. Historical tasks and snapshots remain available for traceability but no longer appear in the default QE HMM selector.
- UI may need a browser refresh to reload the selector list; backend restart is not required.
