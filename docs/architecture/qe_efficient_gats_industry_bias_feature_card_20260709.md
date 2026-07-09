# QE EfficientGATs Industry Bias Feature Card (F1)

## Background

`EfficientGATs` now fixes the full-pool attention memory issue and supports GPU-resident data, but its attention graph is still fully learned from features only. The model does not use the known Shenwan industry structure already present in AIstock sector data. This feature adds an optional same-industry attention bias so GATs can use a real graph prior without changing the default behavior.

This is F1 because it is a single-model capability change with config plumbing and targeted QE tests. It does not change qlib site-packages, database schema, production runtime, or live strategy activation.

## Scope

- Add `gats_adjacency_mode=off|industry_bias` to `EfficientGATs`, defaulting to `off`.
- In `industry_bias` mode, add learnable scalar `industry_bias_gamma` to pre-softmax attention logits for same-industry pairs.
- Use per-row industry ids as a side channel, not as a feature column.
- Support resident and streaming paths. Resident mode carries industry ids with the resident tensor metadata; streaming mode batches side-channel ids with the same `DailyBatchSampler` day groups.
- Keep qlib `fit(dataset, evals_result, save_path)` and `predict(dataset)` contracts.
- Keep existing GPU resident VRAM loud fallback behavior and BUG-609/612 optimizations.
- Pass the new GATs model hyperparameters through `config_composer`.

## Non-Goals

- Do not hard-mask cross-industry attention.
- Do not add multi-head attention or change model capacity beyond the scalar industry bias.
- Do not inject industry ids into the feature tensor.
- Do not query production DB from `efficient_gats.py`; industry ids must be supplied by the dataset/segment side-channel or provider.
- Do not modify qlib site-packages.
- Do not register or launch production QE experiments in this PR.

## Design Acceptance Index

- F-001: `gats_adjacency_mode=off` preserves current `EfficientGATs` numerical behavior.
- F-002: `industry_bias` adds `gamma * same_industry[i,j]` to attention logits, with `gamma` initialized to `0.0`.
- F-003: Bias is soft, not a mask: cross-industry attention remains nonzero.
- F-004: Industry ids are passed as a side channel in resident and streaming daily batches, never as feature columns.
- F-005: Missing industry ids do not crash and do not add bias for missing rows or columns; the model records a loud `reason_code`.
- F-006: Resident data path preserves existing GPU-resident preload, no per-batch `.to(device)`, float32 preload, and loud VRAM fallback behavior.
- F-007: `config_composer` passes `gats_adjacency_mode` and gamma init through GATs model kwargs, not strategy kwargs.
- F-008: `industry_bias` fit/predict remains non-degenerate with finite, non-constant predictions and computable RankIC.

## Implementation Plan

1. Extend `EfficientGATModel` with optional industry-bias attention and a `forward(..., industry_ids=None)` side-channel argument.
2. Extend `EfficientGATs` constructor with mode validation and gamma init, defaulting to `off`.
3. Add industry id extraction from segment/provider metadata and loud adjacency events for full or partial missing ids.
4. Carry industry ids through `_preload_segment_to_cpu`, `_move_segment_to_gpu`, `_iter_resident_batches`, custom streaming daily iterators, `train_epoch`, `test_epoch`, and `predict`.
5. Keep off-mode paths and plain qlib `GATs` paths unchanged.
6. Add config composer whitelist entries and focused tests in `test_qe_config_truth.py`.

## Verification Plan

- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_efficient_gats_industry_bias_feature_card_20260709.md --tier F1`
- `python -m py_compile aistock_models/aistock_models/efficient_gats.py backend/services/quantevolver/config_composer.py backend/tests/unified_engine/test_qe_config_truth.py`
- `python -m ruff check aistock_models/aistock_models/efficient_gats.py backend/services/quantevolver/config_composer.py backend/tests/unified_engine/test_qe_config_truth.py`
- Targeted pytest nodeids for EfficientGATs industry bias, resident fallback, and non-degenerate fit/predict.
- `git diff --check`

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `EfficientGATs` default `gats_adjacency_mode="off"` and off-path calls | `test_efficient_gats_adjacency_off_matches_default_fit_predict` | verified | - |
| F-002 | `EfficientGATModel.industry_bias_gamma`, `cal_attention(..., industry_ids=...)` | `test_efficient_gats_industry_bias_gamma_zero_matches_off_attention` | verified | - |
| F-003 | same attention implementation | `test_efficient_gats_industry_bias_increases_same_industry_attention_without_masking_cross_industry` | verified | - |
| F-004 | resident and streaming side-channel iterators | `test_efficient_gats_industry_ids_resident_and_streaming_side_channel` | verified | - |
| F-005 | `_extract_segment_industry_ids`, `_loud_adjacency_event`, same-industry valid mask | `test_efficient_gats_industry_missing_ids_are_loud_and_unbiased` | verified | - |
| F-006 | resident helpers and existing tests | existing GPU resident tests in `test_qe_config_truth.py` | verified | - |
| F-007 | `config_composer.py` `_GATS_HP_KEYS` | `test_gats_custom_params_route_to_model_kwargs_not_strategy_or_pt_model_kwargs` | verified | - |
| F-008 | `EfficientGATs` industry-bias fit/predict | `test_efficient_gats_industry_bias_fit_predict_non_degenerate_rank_ic` | verified | - |

## Risks

- Real QE datasets must provide point-in-time industry ids through the dataset/segment side channel or configured provider. If ids are absent, `industry_bias` will loudly record the missing-id reason and apply zero adjacency bias.
- A positive learned gamma can change model signal. The default `off` mode and `gamma=0` initial attention are explicitly covered by equivalence tests.
- Daily cross-section sizes remain variable, so this feature does not attempt CUDA Graph capture.

## Production Gates

- `production_ddl_gate`: noop; no schema or migration changes.
- `production_frontend_dependency_gate`: noop; no frontend dependency changes.
- `production_backend_dependency_gate`: noop; no backend dependency changes.
- Runtime activation gate: code merge does not register seeds, launch QE experiments, or restart production services.
