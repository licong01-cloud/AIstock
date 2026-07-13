# QE EfficientGATs Feature Card (F1)

## Background

R0 GATs PR #1919 让 QE 能直接运行 qlib 内置 `qlib.contrib.model.pytorch_gats_ts.GATs`，但全池每日截面约 5000 只股票时，qlib `GATModel.cal_attention` 会实体化 `[N, N, 2 * hidden]` 中间张量。实测 `qe_20260708_181142_b90d` 与 `qe_20260708_184202_1616` 在本机 16GB GPU + WDDM shared memory 下 CUDA OOM。

本任务按 FEATURE-WORKFLOW-001 判定为 F1：QE 模型能力增强，目标是提供项目自有 `EfficientGATs`，用加法拆解的注意力计算替代 qlib 的高峰值中间张量，同时保持 qlib GATs 的 fit/predict/DailyBatchSampler 行为。

## Scope

- 新增 `aistock_models.efficient_gats.EfficientGATs` 和 `EfficientGATModel`。
- 注意力计算不修改 site-packages qlib，不实体化 `[N, N, 2 * hidden]`。
- composer 的 GATs 分支尊重 seed `model_config.class` 和 `model_config.module_path`，使 `EfficientGATs` seed 能路由到项目自有模块。
- QE workspace/in-memory payload 会复制 `aistock_models/efficient_gats.py` 并设置 `PYTHONPATH`。
- 增加数值恒等、全池内存 smoke、非退化 fit/predict 与 composer payload 测试。

## Non-Goals

- 不注册生产 model-registry seed；合并后由 strategy session 使用 MCP 注册。
- 不运行真实全池 alpha 对比实验；Tier2 对比 LGBM-F12/TCN 与正交分析由 strategy session 接手。
- 不修改 vendored/site-packages qlib。
- 不改变现有 `GATs` qlib 路径；#1919 的 qlib GATs seed 仍可按原路径运行。
- 不写生产 DB，不应用 DDL，不重启运行时。

## Design Acceptance Index

- F-001: `EfficientGATModel.cal_attention` 数值恒等于 qlib 0.9.7 真实朴素实现的 `expand/transpose/cat/view/mm/leaky_relu/softmax` 输出。
- F-002: 注意力实现只保留 `[N, N]` 标量 score/softmax 矩阵，不实体化 `[N, N, 2 * hidden]`。
- F-003: 全池合成 N≈5000、hidden=64 的 smoke 不 OOM；GPU 环境断言峰值显存 `< 8GB`，无 GPU 环境断言 CPU fallback 形状与反向传播。
- F-004: `EfficientGATs` 继承 qlib `GATs` fit/predict 语义，最小训练/预测产出有限、非常数预测，RankIC 可计算。
- F-005: composer 的 GATs 分支尊重 seed `model_config.class/module_path`，可输出 `class: EfficientGATs` 和 `module_path: aistock_models.efficient_gats`。
- F-006: QE workspace/in-memory payload 包含 `aistock_models/efficient_gats.py` 和 `aistock_models/__init__.py`，并启用 workspace `PYTHONPATH`。
- F-007: 现有 qlib `GATs` 路径保持不变，不回归 #1919 contract。
- F-008: scope 仅限高效 GATs 类、composer 路由/打包、测试和本 Feature Card。

## Implementation Plan

1. 在 `aistock_models/aistock_models/efficient_gats.py` 增加高效注意力函数、`EfficientGATModel` 和 `EfficientGATs`。
2. 扩展 `config_composer.py`：
   - GATs 分支读取 `model_config.class/module_path`；
   - 对 `aistock_models.efficient_gats` 复制 workspace adapter 源文件；
   - 对自有模型模块启用 workspace `PYTHONPATH`。
3. 扩展 `test_qe_config_truth.py`：
   - composer route contract；
   - 数值恒等测试；
   - N≈5000 full-pool memory smoke；
   - EfficientGATs fit/predict 非退化；
   - in-memory payload 文件测试。

## Verification Plan

- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_efficient_gats_feature_card_20260708.md --tier F1`
- `python -m pytest backend/tests/unified_engine/test_qe_config_truth.py::test_efficient_gats_seed_respects_model_config_module_path backend/tests/unified_engine/test_qe_config_truth.py::test_efficient_gats_attention_is_numerically_identical_to_naive backend/tests/unified_engine/test_qe_config_truth.py::test_efficient_gats_full_pool_attention_smoke_no_n2_hidden_materialization backend/tests/unified_engine/test_qe_config_truth.py::test_efficient_gats_minimal_fit_predict_non_degenerate_rank_ic backend/tests/unified_engine/test_qe_config_truth.py::test_efficient_gats_in_memory_payload_includes_adapter_files -q -p no:cacheprovider`
- `python -m pytest backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider`
- `python -m py_compile aistock_models/aistock_models/efficient_gats.py backend/services/quantevolver/config_composer.py backend/tests/unified_engine/test_qe_config_truth.py`
- `git diff --check`

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `aistock_models/aistock_models/efficient_gats.py` `additive_gats_attention_logits` and `EfficientGATModel.cal_attention` | `test_efficient_gats_attention_is_numerically_identical_to_naive` | verified | - |
| F-002 | `EfficientGATModel.cal_attention` additive score construction | `test_efficient_gats_full_pool_attention_smoke_no_n2_hidden_materialization` monkeypatch shape guard | verified | - |
| F-003 | `EfficientGATModel` full-pool smoke | same test, GPU peak threshold or CPU fallback | verified | - |
| F-004 | `EfficientGATs` subclass of qlib `GATs` | `test_efficient_gats_minimal_fit_predict_non_degenerate_rank_ic` | verified | - |
| F-005 | `config_composer.py` GATs branch model_config routing | `test_efficient_gats_seed_respects_model_config_module_path` | verified | - |
| F-006 | `config_composer.py` workspace adapter source packaging | `test_efficient_gats_in_memory_payload_includes_adapter_files` | verified | - |
| F-007 | Existing qlib GATs branch defaults unchanged | existing #1919 tests in `test_qe_config_truth.py` | verified | - |
| F-008 | PR diff scope | `git diff --name-only` and `git diff --check` | verified | - |

## Notes

- qlib 0.9.7 的真实 `cal_attention` 由于 `x.expand(N, N, H)` 的维度对齐和 `transpose(0, 1)`，朴素矩阵元素顺序等价于 `a_l^T h_col + a_r^T h_row`。本实现以 site-packages 中真实朴素实现为数值恒等基准，保持 qlib 行为。
- CPU fallback 不声称 GPU 显存通过，只验证大截面 `[N, N]` 形状、反向传播和无 `[N, N, 2H]` Python-level materialization。GPU 环境会执行峰值显存阈值断言。

## Risks

- qlib 后续版本若修正 `cal_attention` 的 expand/transpose 方向，本实现需要按新的 site-packages 行为重新做数值恒等测试。
- CPU CI 不能证明真实 16GB GPU 峰值，只能验证无 `[N, N, 2H]` 中间张量和大截面反向传播；GPU 机器上会执行 `< 8GB` 阈值断言。
- `EfficientGATs` seed 由合并后的 model-registry MCP 注册；注册前只有 composer 与 workspace 打包能力，不代表生产 catalog 已可选。

## Production Gates

- `production_ddl_gate`: noop; no schema or migration changes.
- `production_frontend_dependency_gate`: noop; no frontend dependency changes.
- `production_backend_dependency_gate`: noop; no backend dependency changes.
- Runtime activation gate: code merge does not start QE runs or register production seeds.

## GPU Resident Data Extension (2026-07-09)

### Scope

- Add opt-in `EfficientGATs(gpu_resident=True)` mode.
- Keep qlib `fit(dataset, evals_result, save_path)` and `predict(dataset)` contracts.
- Keep the existing streaming DataLoader path and the plain qlib `GATs` path unchanged.
- Do not modify site-packages qlib.

### Design Acceptance Index

- F-009: GPU resident mode preloads train/valid/test feature+label tensors once, then forms daily batches by day index gather from the resident tensor.
- F-010: For each trading day, resident daily batch feature+label values equal the original qlib `DailyBatchSampler` streaming batch element by element.
- F-011: Resident `train_epoch` does not call `Tensor.to(device)` per daily batch; only the one-time preload path may move tensors.
- F-012: Before activating resident mode, VRAM is estimated from resident data, model parameters, working attention memory and margin; insufficient VRAM loudly falls back to streaming with `reason_code`, requested bytes and available bytes.
- F-013: Resident fit/predict remains non-degenerate: finite predictions, non-constant predictions and computable RankIC.
- F-014: Composer can pass `gpu_resident` and resident safety knobs as GATs model kwargs, not strategy kwargs.

### Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-009 | `aistock_models/aistock_models/efficient_gats.py` `_preload_segment_to_cpu`, `_move_segment_to_gpu`, `_iter_resident_batches` | `test_efficient_gats_gpu_resident_daily_batch_data_equivalent_to_streaming` | verified | - |
| F-010 | same resident batch helpers | `test_efficient_gats_gpu_resident_daily_batch_data_equivalent_to_streaming` uses `torch.allclose` against qlib DataLoader daily batches | verified | - |
| F-011 | `EfficientGATs.train_epoch` resident branch | `test_efficient_gats_gpu_resident_train_epoch_has_no_per_batch_to` monkeypatches `Tensor.to` | verified | - |
| F-012 | `_resident_estimate`, `_can_activate_gpu_resident`, `_loud_gpu_resident_fallback` | `test_efficient_gats_gpu_resident_vram_fallback_is_loud` | verified | - |
| F-013 | resident `fit` and `predict` branches | `test_efficient_gats_gpu_resident_fit_predict_non_degenerate_rank_ic` | verified | - |
| F-014 | `backend/services/quantevolver/config_composer.py` `_GATS_HP_KEYS` | `test_gats_custom_params_route_to_model_kwargs_not_strategy_or_pt_model_kwargs` | verified | - |
