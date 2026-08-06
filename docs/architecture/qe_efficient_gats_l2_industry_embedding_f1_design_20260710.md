# EfficientGATs L2 行业 Embedding F1 设计

## Revision 修订记录（BUG-989，2026-08-06）

**数据面零数据库不变量（强制）：QE/多 Alpha 的训练、预测、回测和组合计算数据面不得访问数据库。数据库只用于控制面和结果面。所有计算输入来自具有版本、cutoff 和哈希的冻结 bin/H5/Parquet/sidecar 文件；缺失时 fail closed。**

本修订替代原文中"运行时从 `market.sw_index_member` 查询行业归属"的设计（原 F-001/F-002 的实现路径、Scope 第 1-3 条、Implementation Plan 第 1-2 条、Risks 第 1 条）。权威 PIT 来源仍是 `market.sw_index_member`，但它只允许在**冻结数据集构建期**被读取并导出为 `sector_data.h5` / `static_factors.parquet` 的显式 `l2_code_id` 列；QE 运行期（full / train-only / predict / backtest-only / 多 Alpha pred-backtest）只允许读取该冻结文件，逐交易日向后 as-of 对齐，禁止行业特征签名推断，禁止以当前快照替代历史 PIT。文件缺失、显式 `l2_code_id` 列缺失、日期/代码无法对齐或覆盖率低于阈值时一律 fail closed（稳定 reason_code），不得回退数据库、不得在线补齐、不得静默降级。运行时 provider 为 `SectorDataIndustryIdProvider`（`aistock_models.gats_industry_provider`），模块内不含 psycopg2、DB 连接配置或任何 DB 凭据环境变量；pickle 状态不嵌入冻结源数据，仅保留文件路径与阈值。

关联缺口（已解除，2026-08-06）：停牌输入不再依赖数据库——已构建版本化冻结候选数据集 `suspend_d_daily_candidate_20180801_20260630`（`suspend_d.parquet` + `manifest.json`，含逐交易日完整性回执，离线构建期只读导出自 `market.suspend_d`，`suspend_type='S'`，仅 sh/sz、剔除 BJ），作为冻结 bin 目录的同级 sidecar 同步至全部计算节点；新装配由 `qe_build_frozen_suspend_filter.py` 在计算节点按 `qe_frozen_build_spec.json` 的 suspend 钉（dataset_id/cutoff/universe key/SHA256）重建 `qe_suspend_filter.json`，钉/身份/日期覆盖/字段不符一律 fail closed，无数据库回退。原 `qe_suspend_filter_offline_dataset_gap` 阻断随之解除。

## Background 背景

EfficientGATs 已具备内存友好的 additive GAT attention，以及可选的 `industry_bias` 邻接 side-channel。上一版 QE 行业 provider 从 `sector_data.h5` / `sw2_*` 因子签名推导同业 id，能够表达 same-industry 关系，但不是权威离散行业码，也无法作为模型可学习的真实板块归属输入。

本功能把行业来源切到权威 PIT 数据 `market.sw_index_member` 的**冻结导出物**：数据集构建期从 `market.sw_index_member` 按 `in_date/out_date` 导出显式 `l2_code_id` 到 `sector_data.h5` / `static_factors.parquet`；QE 运行期 provider 只读该冻结文件，逐交易日向后 as-of 取真实 PIT L2 码，并在 EfficientGATs 中新增可选 Shenwan L2 embedding，使模型能够学习板块向量和板块轮动 alpha，同时保持默认 `off` 路径与现状数值等价。（原"运行期 as-of SQL 查询"设计已由 BUG-989 修订替代，见文首修订记录。）

## Scope 范围

- 升级 `aistock_models.gats_industry_provider`：运行期从冻结 `sector_data.h5` / `static_factors.parquet` 的显式 `l2_code_id` 列读取真实 PIT 申万 L2 离散码；模块不含 psycopg2 / DB 连接配置 / DB 凭据环境变量。
- 复用与 Selection Center as-of 语义等价的 PIT 规则：每个 `(instrument, trade_date)` 在冻结文件内按交易日向后 as-of 取最近一条不晚于当日的记录，未来记录不可见；该语义由数据集构建期的 `in_date <= trade_date AND (out_date IS NULL OR out_date >= trade_date)` 导出保证。
- Provider 仍由 qrun Python 路径注入，不在 `conf.yaml` 中序列化 callable；pickle 状态不嵌入冻结源数据，仅保留文件路径与阈值。
- 新增 `gats_industry_embedding=off|on`，默认 `off`；当 adjacency 也为 `off` 时必须与当前 EfficientGATs fit/predict 逐值等价。
- 当 `on` 时，将真实 L2 code 映射到 131 类词表，缺失行业映射到 unknown index `131`，并用 `nn.Embedding(132, emb_dim)` 生成行业向量。
- Embedding 固定接在 RNN last hidden 之后、attention 之前：`[hidden, industry_emb]` 拼接后经 projection 回到 `hidden_size`，再进入现有 attention / FC 路径。
- Resident 与 streaming 两条 EfficientGATs 路径复用同一个每日行业 side-channel；`industry_bias` 邻接也继续使用同一 provider 输出。
- `gats_industry_embedding` / `gats_industry_embedding_dim` 通过 ConfigComposer 的 `_GATS_HP_KEYS` 白名单透传到 model kwargs。

## Non-Goals 非目标

- 不修改 qlib site-packages，不改变 Qlib dataset/segment 对外契约。
- 不新增 DB DDL，不写生产 DB，不重启生产 backend/frontend/TDX，不改前后端依赖。
- 不重写 BUG-609 / BUG-612 的 GPU resident 激活、float/sync 优化，也不改变 PR #1934 / #1935 既有 loud 语义，只把同一 side-channel 扩展给 embedding 模式使用。
- 不再 fallback 到 `sw2_*` 因子签名作为行业身份；冻结文件缺失、显式 `l2_code_id` 列缺失或覆盖率不足必须 loud fail，禁止回退数据库。

## Design Acceptance Index 设计验收索引

| item | requirement |
| --- | --- |
| F-001 | EfficientGATs 行业 provider 从冻结 `sector_data.h5` / `static_factors.parquet` 的显式 `l2_code_id` 列读取真实 PIT L2 码（构建期导出自 `market.sw_index_member`），逐交易日 as-of 且无未来泄露。 |
| F-002 | Provider 保留覆盖率 loud failure、稳定 reason_code，并确保 pickle 不嵌冻结源数据；模块零 DB 依赖。 |
| F-003 | `gats_industry_embedding=off` 为默认值，且 adjacency off 时与当前 EfficientGATs 数值等价。 |
| F-004 | `gats_industry_embedding=on` 创建 `nn.Embedding(132, emb_dim)`，缺失行业走 index 131，embedding 拼接到 RNN hidden 后参与梯度。 |
| F-005 | ConfigComposer 只把 `gats_industry_embedding` / `gats_industry_embedding_dim` 传入 model kwargs，不泄漏到 strategy kwargs。 |
| F-006 | Resident 与 streaming fit/predict 路径共享 provider 供应的每日 L2 side-channel，并保持 BUG-609 / BUG-612 / PR #1934 / #1935 行为。 |

## Implementation Plan 实施方案

- 用文件专用 `SectorDataIndustryIdProvider` 替换旧 DB 查询 provider（原 `SwIndexMemberIndustryIdProvider` 设计由 BUG-989 废止）；只接受显式 `l2_code_id` 列，禁止行业特征签名推断。
- Provider 只读取冻结文件路径（`QE_GATS_INDUSTRY_SOURCE_PATH` / `qe_runtime` 配置 / 工作目录默认名），无任何 psycopg2 / DB 凭据环境变量；测试用真实 H5/Parquet fixture 验证 PIT 行为，并在 DB 连接被投毒（任何调用即抛错）时验证 provider 路径完整可用。
- qrun full、train-only、backtest-only 路径同时识别 `industry_bias` 和 `gats_industry_embedding=on`，只在需要行业 side-channel 时注入 provider。
- `EfficientGATModel` 增加可选 embedding 与 projection；`off` 模式不创建额外 module，不改变随机数消耗和默认路径。
- `EfficientGATs` 将 industry side-channel gate 从仅 `industry_bias` 扩展到 `industry_bias or embedding_on`；adjacency off + embedding off 仍委托原 Qlib 路径。
- 模型实例内维护 L2 code 到 id 的稳定映射，保留 index 131 给 unknown/missing；非缺失 L2 code 超过 131 个时 fail loud，避免静默截断。
- 补充 PIT 边界、覆盖率 loud、pickle cleanliness、config passthrough、off 等价、embedding 梯度参与、unknown index、resident/streaming side-channel parity 等测试。

## Verification Plan 验证方案

- Provider fixture 构造冻结 parquet/H5 行业迁移记录，验证迁移日前、迁移日、迁移后、未来记录不可见等 as-of 行为；另验证显式 `l2_code_id` 缺失、源文件缺失、覆盖率不足均 fail loud，且 DB 连接投毒（任何调用即抛错）下 provider 路径完整可用。
- Off 等价测试执行 fit -> predict，对比默认 EfficientGATs 与显式 `gats_industry_embedding=off` / adjacency off，要求 `allclose(1e-6)`。
- Embedding-on 测试执行同 seed 训练，断言 embedding weight 相比初始化被更新、预测与 off 不同、RankIC 可计算，并且缺行业股票使用 index 131 不崩。
- Regression 覆盖现有 PR #1934 attention / industry-bias 节点、BUG-609 resident predict activation 节点、BUG-612 resident float/sync 节点，以及 qrun 注入测试。
- 静态门禁：changed Python compile、目标 pytest、`git diff --check`、`aistock_guardrail_scan.py --changed-only --fail-on-severity P1`、`aistock_feature_workflow.py validate --tier F1`。

## Design Acceptance Matrix 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | `aistock_models/aistock_models/gats_industry_provider.py` | `backend/tests/unified_engine/test_qe_config_truth.py::test_gats_industry_provider_file_pit_asof_and_no_future_leakage`, `backend/tests/unified_engine/test_qe_config_truth.py::test_gats_industry_provider_normalises_qlib_and_ts_code_instruments` | pass | n/a |
| F-002 | `SectorDataIndustryIdProvider.get_industry_ids`, `SectorDataIndustryIdProvider.__getstate__` | `backend/tests/unified_engine/test_qe_config_truth.py::test_gats_industry_provider_missing_file_and_zero_coverage_fail_loud`, `backend/tests/unified_engine/test_qe_config_truth.py::test_gats_industry_provider_missing_explicit_l2_code_id_fails_loud`, `backend/tests/unified_engine/test_qe_config_truth.py::test_gats_industry_provider_pickle_does_not_embed_source_rows`, `backend/tests/unified_engine/test_qe_config_truth.py::test_gats_industry_provider_injects_for_embedding_on_without_db_connect` | pass | n/a |
| F-003 | `EfficientGATs.__init__`, `EfficientGATModel.__init__`, `_predict_streaming` | `backend/tests/unified_engine/test_qe_config_truth.py::test_efficient_gats_industry_embedding_off_matches_default_fit_predict` | pass | n/a |
| F-004 | `EfficientGATModel._with_industry_embedding`, `EfficientGATs._normalise_industry_ids` | `backend/tests/unified_engine/test_qe_config_truth.py::test_efficient_gats_industry_embedding_updates_weights_changes_predictions_and_unknown_works` | pass | n/a |
| F-005 | `backend/services/quantevolver/config_composer.py` | `backend/tests/unified_engine/test_qe_config_truth.py::test_gats_custom_params_route_to_model_kwargs_not_strategy_or_pt_model_kwargs` | pass | n/a |
| F-006 | `EfficientGATs._industry_side_channel_enabled`, `_preload_segment_to_cpu`, `_preload_streaming_segment_metadata`, `scripts/qrun_limit.py`, `scripts/qrun_limit_minute.py` | `backend/tests/unified_engine/test_qe_config_truth.py::test_efficient_gats_industry_embedding_resident_and_streaming_side_channel`, existing BUG-609/BUG-612 targeted nodes | pass | n/a |

## Risks 风险

- QE workspace 与 backend 解耦：provider 只依赖冻结文件路径，无 psycopg2 / 环境变量驱动连接；文件缺失或字段缺失会以稳定 reason_code loud fail。
- 若某次 run 的 universe/date 范围在冻结文件中覆盖不足，embedding 和 industry_bias 都会按覆盖率阈值失败，不会静默 fallback 到 off、因子签名或数据库。
- 131 类上限在模型 mapping 阶段强制执行；若源数据出现超过 131 个非空 L2 code，run 会以 cardinality exceeded fail loud，交由数据治理排查。

## Production Gates 生产门禁

- `production_ddl_gate`: noop
- `production_frontend_dependency_gate`: noop
- `production_backend_dependency_gate`: noop
- Runtime/DB touch: 不重启生产服务，不写 DB，不执行 DDL；QE 运行期数据面零数据库，行业 side-channel 只读冻结文件。
