# QE EfficientGATs SW2 Industry Provider F1 Design

## Background 背景

PR #1934 已为 `EfficientGATs` 增加 `gats_adjacency_mode=industry_bias` 的模型侧能力，但 QE 运行路径不能通过 `conf.yaml` 直接传入 callable `gats_industry_id_provider`。当前 Qlib `TSDatasetH` segment index 只有 `(datetime, instrument)`，segment 本身也没有 `get_industry_ids`，因此生产 QE 真跑 `industry_bias` 时会因行业 id 缺失而 loud 失败，或者在旧语义下退化为 no-op。

本功能把与现有行业因子同源的 `sector_data.h5` / sector 数据源接入 `EfficientGATs`，在 QE runner 的 Python 实例化路径中注入 point-in-time 行业 provider，使 `industry_bias` 真正获得逐行对齐的行业 id。

## Scope 范围

- 仅当 `gats_adjacency_mode=industry_bias` 时，在 qrun 路径向 model kwargs 或已加载 model 注入 `gats_industry_id_provider`。
- Provider 从 `sector_data.h5` / sector 源读取 SW2 行业展开数据，并返回与 segment `(datetime, instrument)` index 逐行对齐的行业 id。
- full、train-only、backtest-only、gpu_resident、streaming 共用同一个 model-level provider 契约。
- 行业源缺失、schema 不可用或覆盖率低于阈值时 fail-loud，带稳定 `reason_code` 和 source/candidate 信息。
- `off` 默认模式不加载行业源、不解析 sector 路径、不改变原训练和预测路径。

## Non-Goals 非目标

- 不修改 qlib site-packages，不改变 Qlib dataset/segment 对外契约。
- 不要求 `conf.yaml` 序列化 callable；provider 仍由 runner Python 侧注入。
- 不新增 DB DDL，不写生产 DB，不重启生产 backend/frontend/TDX runtime。
- 不重写 BUG-609/BUG-612 的 GPU resident 优化或 PR #1934 的 attention 数学实现。

## Design Acceptance Index 设计验收索引

| item | requirement |
| --- | --- |
| F-001 | QE runner 在请求 `industry_bias` 时注入 PIT 行业 provider，包含 full、train-only 和 backtest-only 路径。 |
| F-002 | Provider 支持 `provider(index)` / `provider(segment,index)` / `provider(segment,index,segment_name)`，返回与 `(datetime, instrument)` 逐行对齐的行业 id。 |
| F-003 | Provider 使用 source date `<= target datetime` 的 exact/as-of 查询，禁止未来行业调整泄露。 |
| F-004 | 行业源缺失、schema 无效或覆盖率低于阈值时必须 loud fail，包含稳定 `reason_code`、source/candidates、coverage。 |
| F-005 | `gats_adjacency_mode=off` 时不解析、不加载行业源，训练预测路径保持零改动。 |
| F-006 | resident 与 streaming 两条 EfficientGATs 路径都能消费同一 model-level provider，并保持 PR #1934 / BUG-609 / BUG-612 既有语义。 |

## Implementation Plan 实施方案

- 新增 `aistock_models.gats_industry_provider.SectorDataIndustryIdProvider`：
  - 只保存 source path 和覆盖率阈值，按需 lazy-load 数据；pickle 时清空 DataFrame cache，避免 `params.pkl` 嵌入完整行业数据。
  - 支持 HDF5/parquet/csv；优先使用显式行业列（如 `sw2_code`、`l2_code`、`industry_code`），否则从同源 `sw2_*` 展开列生成 same-industry signature id。
  - 对 target index 先 exact reindex，再按 instrument 执行 backward as-of，只允许使用 target 当日及之前数据。
  - 记录 `last_coverage` / `coverage_history`，覆盖率低于阈值时报 `qe_gats_industry_coverage_below_threshold`。
- 在 `scripts/qrun_limit.py` 和 `scripts/qrun_limit_minute.py` 的 `task_train` 调用前检测 `industry_bias` 并注入 provider；`off` 不导入 provider 模块。
- 在 minute backtest-only 路径加载 `params.pkl` 后、`SignalRecord.predict` 前向已加载 model 挂载同一 provider。
- 在 `config_composer` 复制 workspace model adapter 时同步复制 `gats_industry_provider.py`，保证 QE 工作目录可导入。
- 在 `EfficientGATs` 中把 `industry_bias` 下缺失行业 id 从“记录事件后继续”升级为 `efficient_gats_industry_ids_missing` fail-loud；未知行业行仍按 PR #1934 语义映射为 `-1` 并产生零 bias。

## Verification Plan 验证计划

- Provider 契约测试：PIT exact/as-of、缺失 source、覆盖率 0、off 不解析 source。
- 真跑路径测试：用 fixture `sector_data.h5` 注入 provider，执行 EfficientGATs fit/predict，验证 `coverage_history` 大于阈值且没有 `industry_ids_missing`。
- qrun 注入测试：确认只有 `industry_bias` 调 provider，`off` 不导入/不解析行业源，并且注入函数收到完整 config（保留 `qe_runtime`）。
- 回归测试：PR #1934 attention / resident / streaming、BUG-612 recorder isolation 和 qrun retry 相关测试保持通过。
- 静态验证：changed Python `py_compile`、`git diff --check`、`aistock_feature_workflow.py validate --tier F1`。

## Design Acceptance Matrix 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | `scripts/qrun_limit.py`, `scripts/qrun_limit_minute.py`, `aistock_models/aistock_models/gats_industry_provider.py` | `test_qrun_industry_provider_injection_only_for_industry_bias`, `test_efficient_gats_industry_bias_runner_injection_fit_predict` | pass | n/a |
| F-002 | `SectorDataIndustryIdProvider.__call__`, `SectorDataIndustryIdProvider.get_industry_ids`, `EfficientGATs._require_segment_industry_ids` | `test_gats_industry_provider_pit_asof_and_coverage`, `test_efficient_gats_industry_bias_runner_injection_fit_predict` | pass | n/a |
| F-003 | `SectorDataIndustryIdProvider._lookup_asof`, `_asof_rows_by_instrument` | `test_gats_industry_provider_pit_asof_and_coverage` | pass | n/a |
| F-004 | `validate_source_available`, `get_industry_ids` coverage gate, `EfficientGATs._require_segment_industry_ids` | `test_gats_industry_provider_source_missing_and_coverage_zero_are_loud`, `test_efficient_gats_industry_bias_missing_provider_fails_loud` | pass | n/a |
| F-005 | `_task_train_with_gats_industry_provider`, `inject_gats_industry_provider_if_needed` | `test_gats_industry_provider_off_mode_does_not_resolve_source`, `test_qrun_industry_provider_injection_only_for_industry_bias`, existing off-mode EfficientGATs regression | pass | n/a |
| F-006 | `aistock_models/aistock_models/efficient_gats.py`, `config_composer._workspace_aistock_model_sources` | Existing EfficientGATs PR #1934 / BUG-609 / BUG-612 targeted node set plus `test_efficient_gats_industry_bias_runner_injection_fit_predict` | pass | n/a |

## Risks 风险

- 当前生产 `sector_data.h5` 可能只包含展开到个股的 `sw2_*` 行业因子列，而不包含可读的原始 SW 行业代码。实现优先使用显式 code 列；无 code 列时用同日 `sw2_*` signature 生成稳定同业分组 id。该方式满足 GAT same-industry bias 所需的等价关系，但 id 不是人工可读 SW code。
- 如果未来 sector 源 schema 同时缺少显式行业列和 `sw2_*` signature 列，功能会 fail-loud，不会静默退回 off。
- Backtest-only 的 `params.pkl` 不保存行业 DataFrame cache；运行时仍要求工作目录或 `qe_runtime`/环境变量能定位同一 sector source。

## Production Gates 生产门

- `production_ddl_gate`: noop
- `production_frontend_dependency_gate`: noop
- `production_backend_dependency_gate`: noop
- Runtime/DB touch: 不重启生产服务；不写生产 DB；不应用 DDL。
