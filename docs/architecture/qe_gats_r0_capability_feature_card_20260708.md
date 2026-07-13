# QE GATs R0 Capability Feature Card (F1)

## Background

`docs/analysis/p2_relational_model_hist_master_feasibility_20260708.md` 已确认关系型模型接入按 R0(GATs) -> R1(HIST-industry) 推进。R0 目标不是证明 alpha 优劣，而是让 QE 能把 qlib 内置 `qlib.contrib.model.pytorch_gats_ts.GATs` 当作一种模型类型端到端跑通，并产出真实预测和可计算指标。

本任务按 FEATURE-WORKFLOW-001 判定为 F1：QE config composer 单模块能力接入，包含 catalog seed、配置契约测试和最小 GATs fit/predict smoke。

## Scope

- `config_composer.py` 新增 GATs 模型类型分支。
- GATs 直接使用 qlib 内置 `GATs` 类和 `fit/predict`，不走 `GeneralPTNN`，不使用 `pt_model_kwargs`。
- GATs 使用 `TSDatasetH`，依赖 qlib `DailyBatchSampler` 在 `fit/predict` 内按每日截面成批。
- 新增 `__seed_GATs_default_v1__` catalog seed，供 QE custom_evo 选择。
- 增加 composer contract 测试和最小 qlib GATs fit/predict smoke，验证预测非全 NaN 且 RankIC 可计算。

## Non-Goals

- 不交付真实 alpha 对比结论；Tier2/多窗口/多 seed 实验由 strategy session 接手。
- 不接入 HIST、MASTER、stock2concept 或任何外部关系矩阵。
- 不修改 GeneralPTNN、BUG-605、BUG-606 或现有 PTNN 路由。
- 不修改生产 DB，不应用 DDL，不重启运行时。

## Design Acceptance Index

- F-001: composer 对 `model_type=GATS` 输出 `class: GATs` 和 `module_path: qlib.contrib.model.pytorch_gats_ts`。
- F-002: GATs 超参直接进入 model `kwargs` 顶层；`dropout/base_model/hidden_size/num_layers` 不进入 `pt_model_kwargs`，也不触发 `GeneralPTNN`。
- F-003: GATs composer 输出 `TSDatasetH` 和 `step_len`，保证 qlib GATs 可在 fit/predict 内使用 `DailyBatchSampler` 按日截面成批。
- F-004: catalog seed `__seed_GATs_default_v1__` 可被 QE 选择，配置声明为 GATs + TSDatasetH。
- F-005: 最小 GATs fit/predict smoke 真实实例化 qlib `GATs`，走 `fit` 和 `predict`，产出有限、非全 NaN 预测，并计算 RankIC。
- F-006: 如果 qlib GATs 批采样或数据接口不满足，测试必须 loud fail 或显式报告环境缺失，不允许退化为空预测或静默成功。
- F-007: 分钟执行配置保持与 LGBM 同构：日频 `<PRED>` 信号进入默认 `1min` NestedExecutor/V25 inner strategy，不新增 GATs 专用执行路径。
- F-008: 变更范围仅限 composer、seed SQL、测试和本 Feature Card。

## Implementation Plan

1. 在 `backend/services/quantevolver/config_composer.py` 增加 GATs 分支与 GATs 参数白名单。
2. 调整 task dataset 输出逻辑，让非 GeneralPTNN 的 GATs 也能输出 `TSDatasetH` 和 `step_len`。
3. 在 `backend/db/migrations/seed_multi_alpha_models.sql` 追加 `__seed_GATs_default_v1__`。
4. 在 `backend/tests/unified_engine/test_qe_config_truth.py` 增加 composer contract、V25 minute config contract 和 qlib GATs smoke。
5. 运行 feature workflow validate、目标 pytest、py_compile 和 diff hygiene。

## Verification Plan

- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_gats_r0_capability_feature_card_20260708.md --tier F1`
- `python -m pytest backend/tests/unified_engine/test_qe_config_truth.py::test_gats_seed_composes_direct_qlib_model_with_tsdataseth backend/tests/unified_engine/test_qe_config_truth.py::test_gats_custom_params_route_to_model_kwargs_not_strategy_or_pt_model_kwargs backend/tests/unified_engine/test_qe_config_truth.py::test_gats_v25_minute_execution_config_remains_daily_signal_nested_executor backend/tests/unified_engine/test_qe_config_truth.py::test_qlib_gats_minimal_fit_predict_non_degenerate_rank_ic -q -p no:cacheprovider`
- `python -m py_compile backend/services/quantevolver/config_composer.py backend/tests/unified_engine/test_qe_config_truth.py`
- `git diff --check`

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/quantevolver/config_composer.py` GATs branch | `test_gats_seed_composes_direct_qlib_model_with_tsdataseth` | verified | - |
| F-002 | `config_composer.py` GATs kwargs routing and `_GATS_HP_KEYS` | `test_gats_custom_params_route_to_model_kwargs_not_strategy_or_pt_model_kwargs` | verified | - |
| F-003 | `config_composer.py` task dataset output for GATs | `test_gats_seed_composes_direct_qlib_model_with_tsdataseth` | verified | - |
| F-004 | `backend/db/migrations/seed_multi_alpha_models.sql` seed row | SQL diff review plus composer seed-shaped fixture | verified | - |
| F-005 | qlib GATs smoke fixture in `test_qe_config_truth.py` | `test_qlib_gats_minimal_fit_predict_non_degenerate_rank_ic` | verified | - |
| F-006 | smoke test asserts finite non-empty predictions and finite RankIC | same qlib smoke test | verified | - |
| F-007 | default minute executor config unchanged for GATs | `test_gats_v25_minute_execution_config_remains_daily_signal_nested_executor` | verified | - |
| F-008 | PR diff scope | `git diff --name-only` and `git diff --check` | verified | - |

## Risks

- qlib 0.9.7 `GATs` accepts `batch_size` through `**kwargs` but internally uses full daily cross-section batches via `DailyBatchSampler`; seed retains `batch_size` for catalog/operator symmetry while the executable batching authority is qlib.
- The local smoke is intentionally tiny and proves capability/non-degenerate execution only. It is not evidence of alpha quality.

## Production Gates

- `production_ddl_gate`: noop; seed SQL is committed for later controlled catalog seeding, not applied in this task.
- `production_frontend_dependency_gate`: noop; no frontend dependency changes.
- `production_backend_dependency_gate`: noop; no backend dependency changes.
- Runtime activation gate: code merge does not start QE runs or restart services.
