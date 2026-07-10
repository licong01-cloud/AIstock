# EfficientGATs L2 行业 Embedding F1 设计

## Background 背景

EfficientGATs 已具备内存友好的 additive GAT attention，以及可选的 `industry_bias` 邻接 side-channel。上一版 QE 行业 provider 从 `sector_data.h5` / `sw2_*` 因子签名推导同业 id，能够表达 same-industry 关系，但不是权威离散行业码，也无法作为模型可学习的真实板块归属输入。

本功能把行业来源切到已确认的权威 PIT 数据源 `market.sw_index_member`：按个股、交易日、`in_date/out_date` as-of 查询真实 `l2_code`，并在 EfficientGATs 中新增可选 Shenwan L2 embedding，使模型能够学习板块向量和板块轮动 alpha，同时保持默认 `off` 路径与现状数值等价。

## Scope 范围

- 升级 `aistock_models.gats_industry_provider`：运行时从 `market.sw_index_member.l2_code` 查询真实 PIT 申万 L2 离散码。
- 复用 Selection Center 的 as-of SQL 语义：`ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY in_date DESC ...)`，`in_date <= trade_date`，且 `(out_date IS NULL OR out_date >= trade_date)`。
- Provider 仍由 qrun Python 路径注入，不在 `conf.yaml` 中序列化 callable；pickle 状态不得嵌入源表行数据或 DB 连接对象。
- 新增 `gats_industry_embedding=off|on`，默认 `off`；当 adjacency 也为 `off` 时必须与当前 EfficientGATs fit/predict 逐值等价。
- 当 `on` 时，将真实 L2 code 映射到 131 类词表，缺失行业映射到 unknown index `131`，并用 `nn.Embedding(132, emb_dim)` 生成行业向量。
- Embedding 固定接在 RNN last hidden 之后、attention 之前：`[hidden, industry_emb]` 拼接后经 projection 回到 `hidden_size`，再进入现有 attention / FC 路径。
- Resident 与 streaming 两条 EfficientGATs 路径复用同一个每日行业 side-channel；`industry_bias` 邻接也继续使用同一 provider 输出。
- `gats_industry_embedding` / `gats_industry_embedding_dim` 通过 ConfigComposer 的 `_GATS_HP_KEYS` 白名单透传到 model kwargs。

## Non-Goals 非目标

- 不修改 qlib site-packages，不改变 Qlib dataset/segment 对外契约。
- 不新增 DB DDL，不写生产 DB，不重启生产 backend/frontend/TDX，不改前后端依赖。
- 不重写 BUG-609 / BUG-612 的 GPU resident 激活、float/sync 优化，也不改变 PR #1934 / #1935 既有 loud 语义，只把同一 side-channel 扩展给 embedding 模式使用。
- 不再 fallback 到 `sw2_*` 因子签名作为行业身份；DB 查询失败或覆盖率不足必须 loud fail。

## Design Acceptance Index 设计验收索引

| item | requirement |
| --- | --- |
| F-001 | EfficientGATs 行业 provider 从 `market.sw_index_member` 读取真实 PIT `l2_code`，使用 as-of SQL 语义且无未来泄露。 |
| F-002 | Provider 保留覆盖率 loud failure、稳定 reason_code，并确保 pickle 不嵌源表行数据或 live DB 连接。 |
| F-003 | `gats_industry_embedding=off` 为默认值，且 adjacency off 时与当前 EfficientGATs 数值等价。 |
| F-004 | `gats_industry_embedding=on` 创建 `nn.Embedding(132, emb_dim)`，缺失行业走 index 131，embedding 拼接到 RNN hidden 后参与梯度。 |
| F-005 | ConfigComposer 只把 `gats_industry_embedding` / `gats_industry_embedding_dim` 传入 model kwargs，不泄漏到 strategy kwargs。 |
| F-006 | Resident 与 streaming fit/predict 路径共享 provider 供应的每日 L2 side-channel，并保持 BUG-609 / BUG-612 / PR #1934 / #1935 行为。 |

## Implementation Plan 实施方案

- 用 `SwIndexMemberIndustryIdProvider` 替换旧 `sector_data.h5` / `sw2_*` 签名 provider；保留 `SectorDataIndustryIdProvider` 作为兼容别名，但不再读取 sector 因子文件。
- Provider 内置轻量 psycopg2 连接工厂，适配 QE workspace 不一定能 import backend 的场景；测试通过 fake `conn_factory` 验证 SQL 和 PIT 行为，不触碰真实 DB。
- qrun full、train-only、backtest-only 路径同时识别 `industry_bias` 和 `gats_industry_embedding=on`，只在需要行业 side-channel 时注入 provider。
- `EfficientGATModel` 增加可选 embedding 与 projection；`off` 模式不创建额外 module，不改变随机数消耗和默认路径。
- `EfficientGATs` 将 industry side-channel gate 从仅 `industry_bias` 扩展到 `industry_bias or embedding_on`；adjacency off + embedding off 仍委托原 Qlib 路径。
- 模型实例内维护 L2 code 到 id 的稳定映射，保留 index 131 给 unknown/missing；非缺失 L2 code 超过 131 个时 fail loud，避免静默截断。
- 补充 PIT 边界、覆盖率 loud、pickle cleanliness、config passthrough、off 等价、embedding 梯度参与、unknown index、resident/streaming side-channel parity 等测试。

## Verification Plan 验证方案

- Provider fixture 构造 `sw_index_member` 行业迁移记录，验证迁移日前、迁移日、迁移后、未来记录不可见等 as-of 行为。
- Off 等价测试执行 fit -> predict，对比默认 EfficientGATs 与显式 `gats_industry_embedding=off` / adjacency off，要求 `allclose(1e-6)`。
- Embedding-on 测试执行同 seed 训练，断言 embedding weight 相比初始化被更新、预测与 off 不同、RankIC 可计算，并且缺行业股票使用 index 131 不崩。
- Regression 覆盖现有 PR #1934 attention / industry-bias 节点、BUG-609 resident predict activation 节点、BUG-612 resident float/sync 节点，以及 qrun 注入测试。
- 静态门禁：changed Python compile、目标 pytest、`git diff --check`、`aistock_guardrail_scan.py --changed-only --fail-on-severity P1`、`aistock_feature_workflow.py validate --tier F1`。

## Design Acceptance Matrix 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | `aistock_models/aistock_models/gats_industry_provider.py` | `test_gats_industry_provider_db_pit_asof_migration_and_no_future_leakage` | pass | n/a |
| F-002 | `SwIndexMemberIndustryIdProvider.get_industry_ids`, `SwIndexMemberIndustryIdProvider.__getstate__` | `test_gats_industry_provider_lookup_failure_and_coverage_zero_are_loud`, `test_gats_industry_provider_pickle_does_not_embed_source_rows_or_connection` | pass | n/a |
| F-003 | `EfficientGATs.__init__`, `EfficientGATModel.__init__`, `_predict_streaming` | `test_efficient_gats_industry_embedding_off_matches_default_fit_predict` | pass | n/a |
| F-004 | `EfficientGATModel._with_industry_embedding`, `EfficientGATs._normalise_industry_ids` | `test_efficient_gats_industry_embedding_updates_weights_changes_predictions_and_unknown_works` | pass | n/a |
| F-005 | `backend/services/quantevolver/config_composer.py` | `test_gats_custom_params_route_to_model_kwargs_not_strategy_or_pt_model_kwargs` | pass | n/a |
| F-006 | `EfficientGATs._industry_side_channel_enabled`, `_preload_segment_to_cpu`, `_preload_streaming_segment_metadata`, `scripts/qrun_limit.py`, `scripts/qrun_limit_minute.py` | `test_efficient_gats_industry_embedding_resident_and_streaming_side_channel`, existing BUG-609/BUG-612 targeted nodes | pass | n/a |

## Risks 风险

- QE workspace 可能无法 import backend 模块，因此 provider 使用环境变量驱动的轻量 psycopg2 连接；缺少驱动或连接失败会以 provider reason_code loud fail。
- 若某次 run 的 universe/date 范围在 `sw_index_member` 覆盖不足，embedding 和 industry_bias 都会按覆盖率阈值失败，不会静默 fallback 到 off 或因子签名。
- 131 类上限在模型 mapping 阶段强制执行；若源数据出现超过 131 个非空 L2 code，run 会以 cardinality exceeded fail loud，交由数据治理排查。

## Production Gates 生产门禁

- `production_ddl_gate`: noop
- `production_frontend_dependency_gate`: noop
- `production_backend_dependency_gate`: noop
- Runtime/DB touch: 不重启生产服务，不写 DB，不执行 DDL；仅当 QE run 显式请求行业 side-channel 时执行运行期只读查询。
