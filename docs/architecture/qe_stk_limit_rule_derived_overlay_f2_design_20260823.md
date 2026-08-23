# QE 候选数据集 `stk_limit` 规则派生 Overlay F2 详细设计

> Feature tier: F2
> 状态：用户已批准业务方向，进入实现与本地验证
> 适用 profile：`qe_hmm_full_v2` 及后续显式采用本合同的版本
> 规则版本：`cn_a_share_price_limit_v2_20260706`

## 1. Background / 背景

QE 月度候选数据集的 daily/minute 物化要求每个 PIT 股票交易日都有完整
`pre_close/up_limit/down_limit`。Tushare `stk_limit` 对部分历史交易日无法补齐时，现有
`CanonicalStockTransformer` 会 fail closed。用户确认：对具备确定板块、交易日规则、PIT
状态、可靠前收参考价的缺失键，应按交易所规则计算；把全部缺失键直接设为不可交易会产生
更大的回测偏差。

该能力是候选数据集 source-to-artifact 阶段的正式派生数据产品，不是 QE、Qlib、Paper
Trading 或 MiniQMT 执行层的静默 fallback。

## 2. Scope / 范围

### 2.1 Goals / 目标

- 建立唯一、纯函数、版本化的沪深 A 股涨跌停规则计算器。
- 支持沪深主板、创业板、科创板、PIT ST 状态及 2026-07-06 主板 ST 规则切换。
- 使用 `Decimal` 和 0.01 元最小价格单位进行交易所式四舍五入。
- 将缺失或字段不完整的 `stk_limit` 键生成 candidate-local immutable overlay；完整数据库行优先且禁止覆盖。
- daily/minute 使用同一有效 `stk_limit` 流和同一规则版本。
- 只对 canonical PIT 预期股票日计算；输入不足时继续 fail closed。
- 保持分区流式扫描和有界状态，不累积全市场多年月度 DataFrame。

### 2.2 Non-Goals / 非目标

- 不写入、更新或删除 `market.stk_limit` 及其他数据库表。
- 不修改已有候选、截至 2026-07-31 的既有数据集或 production pointer。
- 不执行真实数据导出、候选发布、生产激活、服务启停或 scheduler tick。
- 不改变实时模拟盘 09:10 后读取并冻结真实 `market.stk_limit` 的合同。
- 不用上一日未调整收盘价、当前股票名称、二进制 float、补零或 NaN 冒充有效限制价。
- 不为北交所、基金、B 股、可转债或未知证券类型推导 A 股限制价。

## 3. Design Acceptance Index / 设计验收索引

- F-001：唯一规则计算器按交易所、板块、交易日、PIT ST 状态选择有效比例。
- F-002：主板普通股为 10%；主板 ST 在 2026-07-05 及以前为 5%，自 2026-07-06 起为 10%。
- F-003：创业板自 2020-08-24 起为 20%，包括 ST；科创板自板块启用起为 20%，包括 ST。
- F-004：IPO 前五个交易日、重新上市首日、退市整理首日等无涨跌幅限制情形不得伪造价格。
- F-005：价格计算使用 `Decimal`、`ROUND_HALF_UP`、0.01 元 tick，并执行最小一 tick 保护。
- F-006：参考价使用上一有效未复权收盘价乘以 `adj_prev / adj_current`；缺少任一输入即阻断。
- F-007：规则派生只补数据库缺失键或不完整键；不完整行的全部既有非空值必须与派生值分币一致，
  完整数据库行重叠、任一非空值冲突或重复键必须阻断。
- F-008：overlay 只进入 candidate-local CAS，绑定规则版本、PIT digest、分区和内容 digest。
- F-009：daily/minute 组件共享同一 overlay，转换器仍只接收完整正数价格行。
- F-010：canonical PIT v2 预期日代表非 ST 可交易资格；中央计算器独立覆盖 ST 规则，构建器不得用当前名称反推历史状态。
- F-011：分区逐个处理，内存状态上限为 O(PIT 代码数 + 当前分区缺失键)，禁止跨分区 frames 累积。
- F-012：既有真实 `stk_limit` 分层采样反算必须达到分币一致；任何系统性偏差阻断启用。
- F-013：规则/overlay 内容进入 artifact-ready effective root，规则改变必须使候选身份失效。
- F-014：实时 Paper Trading/MiniQMT 路径零改动、零 fallback、零运行时激活。
- F-015：无 DB DDL/DML、无依赖变更、无真实导出，生产与既有候选保持不变。
- F-016：首次出现历史缺口 overlay 时，planner 必须使用被影响证券清单生成精确
  `csv_overrides`，不得把稀疏修复扩大为全市场或全量数据集重导。
- F-017：月度 Skill、增量复用参考和 operator runbook 必须统一写明 `stk_limit` 缺失处理、
  fail-closed 条件和精确选择性重建语义，后续月更不得依赖聊天记录。
- F-018：changed-file runtime contract 必须把本功能的七个 artifact/planner 模块精确登记为
  `worker-scheduler`，不得要求无关 backend-main 重启，也不得把整个 dataset_release 目录宽泛改类。
- F-019：source audit 使用三层状态：合法空值/候选内可修复自动继续，provider 暂时不可用进入可重试，
  只有权威冲突、PIT/身份损坏、必要推导输入缺失或安全越界才硬阻断。新增硬阻断前必须先分析触发条件、
  发生概率、误阻代价和替代方案，并获得用户批准。

## 4. Architecture / 架构

```text
sealed DB stk_limit ───────────────────────────────┐
                                                  │ database wins
sealed kline_daily_raw + sealed adj_factor        │
            │                                     ▼
            └─ rule-derived missing/incomplete overlay ─ merge ─ effective stk_limit
                                                        ├─ daily canonical rows
                                                        └─ minute canonical rows
```

### 4.1 Allowed APIs / 允许接口

- `ArtifactReadySourceBuilder.build(...)`：生成 candidate-local 派生 CAS 和 component manifest。
- `ArtifactReadyBuildSource.ordered_partitions(...)`：向 build stage 暴露已验证的有效源流。
- `_merge_stk_limit_completion(...)`：完整数据库行优先；只允许经过逐字段一致性验证的不完整行补全。
- `FrozenPitSnapshot.spans`：唯一候选股票日范围，不使用当前 universe 或当前 ST 名称。
- `CanonicalStockTransformer.transform_daily/transform_minute`：继续消费完整 `stk_limit`，不承载推导策略。

禁止发明数据库写接口、运行时 fallback 参数或第二套执行层计算器。

### 4.2 Rule calculator

新增 `backend/services/dataset_release/a_share_limit_rule.py`：

- `classify_a_share_board(ts_code)` 返回 `SH_MAIN/SZ_MAIN/CHINEXT/STAR`；未知代码阻断。
- `resolve_limit_rate(ts_code, trade_date, is_st, no_daily_limit=False)` 返回版本化规则决策。
- `derive_limit_prices(...)` 返回量化后的 `pre_close/up_limit/down_limit` 与规则身份。
- 无涨跌幅限制返回显式 typed decision；构建器不得将其转换为普通价格行。

### 4.3 Reference price

候选 PIT 预期日 `D` 的派生前收参考价：

```text
reference_D = close_previous_observed × adj_previous_observed / adj_D
pre_close_D = quantize(reference_D, 0.01, ROUND_HALF_UP)
```

`previous_observed` 是同一证券在 `D` 前最近一条有效未复权日线；因此连续停牌不会把缺失日
当成零价格。复权因子比值处理除权除息。若没有前序收盘、前序/当日复权因子、有效板块或 PIT
资格，状态为 `UNRESOLVED` 并阻断候选签署。

### 4.4 ST and no-limit boundary

canonical PIT v2 的 eligible span 已执行 252 交易日 IPO 暖机并排除 PIT ST 风险阶段，所以当前
candidate builder 对 span 内键使用 `is_st=False`。独立规则计算器仍完整覆盖 ST，供反算测试和
后续显式包含 ST 的候选合同复用。

无涨跌幅限制日期没有上下限价格，不能写入当前要求正数价格的 Qlib 股票 schema。由于 IPO
前五日不在 v2 PIT，正常不会进入 overlay；如重新上市/退市整理首日等异常键进入 PIT，构建器
必须以 `NO_DAILY_LIMIT_IN_PIT` 阻断，推动修正 PIT/证券生命周期，而不是伪造极值。

## 5. Data Contract / 数据合同

只有确有缺失/不完整键并成功推导的分区才生成 `dataset_release_stk_limit_rule_overlay_v2` receipt；完整
分区继续只使用原始 `stk_limit` 身份，不生成空 overlay：

- `raw_partition_identity`
- `partition_key`
- `rule_version`
- `pit_snapshot_digest`
- `overlay_rows`：仅 `ts_code/trade_date/pre_close/up_limit/down_limit`
- `database_rows`
- `expected_pit_keys`
- `rule_derived_rows`
- `database_completion_rows`
- `unresolved_keys=0`
- `database_override_rows=0`
- `effective_content_root`
- 零写入 safety 字段

不保存公告、接口调用详情或冗长逐行证据；仅保留规则版本、数量、digest 和必要错误样本。

## 6. Incremental / Performance Contract

- 按 `stk_limit` 日期分区顺序处理；每个分区完成后释放局部集合。
- 每个代码仅保留最新 `close/adj_factor/date` 状态。
- overlay 行数受固定硬上限约束，超过即阻断，不扩大内存上限；完整分区不新增派生身份。
- 禁止 pandas 全市场面板、跨分区 `frames`、整体 concat 或完整静态矩阵预分配。
- 退市日线缺口先一次性按代码分组，再按 `partition_for_day` O(1) 路由 provider 行；禁止“每个代码重新扫描
  全部分区缺口”的 O(code × missing_keys) 循环。
- overlay 身份携带排序后的 `affected_instruments`；首次出现历史缺口或既有 overlay 内容变化时，
  mixed planner 只为这些代码生成 full-history CSV override，并精确替换其 daily/minute bin。
- 上游原始 `stk_limit` 的无证明历史修订、或既有 overlay 消失时仍 fail closed；在没有逐代码原始源
  diff 权威前不得猜测为小范围变更。该异常路径可能要求 clean rebuild，但不会静默扩大普通月更任务。

## 7. Implementation Plan / 实施方案

1. 新增纯规则模块及交易所时间版本矩阵测试。
2. 在 artifact-ready 阶段生成分区化 missing-or-incomplete limit overlay。
3. 在 build source 中将 raw `stk_limit` 与 overlay 流式合并。
4. 将规则/overlay summary、受影响代码权威加入 daily/minute component manifest 和 effective root。
5. 扩展 mixed planner，使首次历史稀疏 overlay 只重建精确代码，不退化为全市场全量导出。
6. 添加 artifact-ready、build-source、planner、daily/minute 转换消费测试。
7. 使用已观测 fixture 做规则反算和边界验证；真实候选构建仍留待独立授权。

## 8. Verification Plan / 验证方案

- 规则单测：主板、创业板、科创板、ST、2026-07-06 切换、分币取整、低价最小 tick。
- 负向单测：未知板块、无涨跌幅限制、缺少前收/复权、naive/float 非法输入。
- overlay 单测：补 missing、精确补全 incomplete、非空值冲突阻断、完整数据库行不覆盖、跨分区 reference state、有界行数。
- build-source 单测：daily/minute 读取相同有效 `stk_limit`；tamper/schema/digest 拒绝。
- transformer 小样本：规则派生行与真实行生成相同 12 字段语义。
- planner 小样本：两代码 PIT 中仅一个代码有缺口时，只生成该代码 override，另一个代码不得进入重建目标。
- `pytest` 定向矩阵、Ruff、`py_compile`、`git diff --check`、L0、module registry。
- `python scripts/aistock_feature_workflow.py validate --design <this-file> --tier F2`。

已对 canonical PIT v2 有效股票做只读分层采样：4 个交易日 × 沪深主板/创业板/科创板 ×
每组 10 条，共 160 条；规则推导的上限/下限与数据库 `stk_limit` 分币一致为 160/160。
真实候选签署仍要求任何 unresolved PIT 键为 0，且不得因样本通过绕过候选级完整性门禁。

## 9. Rollout / Rollback / 发布与回滚

源码合入不会修改数据或运行时。后续经独立授权运行 `qe_hmm_full_v2 monthly --candidate-only` 时，
新规则只产生新的 immutable candidate/CAS overlay。既有截至 2026-07-31 数据集保持只读。

回滚只回退源码/配置；不删除 CAS、candidate、receipt，也不切 production pointer。采用该规则的
候选如果尚未激活，可保持未引用状态；激活和旧数据清理均需独立授权。

## 10. Risks / Failure Modes / 风险与失败模式

| 风险 | 控制 |
|---|---|
| 主板 ST 规则被永久写成 5% | 日期版本矩阵覆盖 2026-07-06 切换 |
| 创业板/科创板 ST 误套 5% | 板块优先规则测试 |
| 除权日直接用上一收盘 | 强制 `adj_prev/adj_current`，缺失即阻断 |
| 无涨跌幅日伪造价格 | typed `NO_DAILY_LIMIT`，进入 PIT 时阻断 |
| 覆盖真实数据库值 | missing-only merge；重叠/重复阻断 |
| 全量内存膨胀 | 分区扫描、每代码滚动状态、overlay 硬上限 |
| 首次稀疏 overlay 被 planner 扩大为全市场重建 | overlay 绑定精确受影响代码；定向 override 测试 |
| 上游补回原始行导致 overlay 消失但无法证明精确 diff | fail closed，不猜测、不静默退化 |
| 模拟盘误用历史派生值 | 实时模块零改动；设计与 changed-file gate 双重审核 |

## 11. 缺失处理与阻断治理

| 分类 | 当前场景 | 动作 |
|---|---|---|
| 自动继续 | `bak_basic` 合法空日 | 标记 `empty_valid`，下游保留 NaN/missing mask |
| 自动继续 | 精确指数代码/日期缺失 | 标记 `candidate_repairable`，由 Tushare 只补候选 CAS 精确键 |
| 自动继续 | PIT 内 `stk_limit` 缺失或不完整 | 规则派生；不完整行先校验全部既有非空字段 |
| 自动继续 | D/P 证券最后权威日线后的严格连续尾段 | 作为 terminal non-trading coverage；不伪造 OHLCV |
| 可重试 | provider 限流、网络错误、T+1 尚未发布 | 保留原 intent/checkpoint，等待重试；不升级为永久合同失败 |
| 硬阻断 | DB/provider、DB/派生已有非空值冲突 | 禁止选择来源或静默覆盖 |
| 硬阻断 | 活跃证券缺口、退市证券内部断点或尾段后又出现权威 bar | 证券生命周期或源数据不一致 |
| 硬阻断 | PIT 重叠/重复/日期倒置/identity 漂移 | 权威股票池损坏 |
| 硬阻断 | 缺少 previous close、adj factor、板块或 PIT 状态 | 无法确定性推导 |
| 硬阻断 | 越权覆盖、生产 pointer、全量扩大、资源/安全合同违反 | 停止并请求独立授权或修复 |

不得仅因 DEV 不具备八年生产数据镜像而阻断生产计划。DEV 只用单行事务内 upsert/readback 验证 DML
机制并回滚；生产仍必须先完成自身全范围只读 plan，再以独立生产 DML 授权 apply。

本表是已批准的硬阻断全集。未来新增或扩大硬阻断时，设计/BUG 必须先列出触发条件、估计发生概率、
误阻成本、准确性风险和至少一个替代方案，获得用户明确批准后才能编码；测试不得反向创造未批准门禁。

## 12. Production Gates / 生产门禁

| 项目 | 本轮状态 |
|---|---|
| Production DB DDL/DML | `noop` |
| DEV/Production 数据修复 | `not_requested` |
| 真实候选导出 | `not_authorized` |
| Production activation | `not_requested` |
| Backend/Worker restart | `noop` |
| 既有 2026-07-31 数据集修改 | `forbidden` |

## 13. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/dataset_release/a_share_limit_rule.py` | `backend/tests/dataset_release/test_a_share_limit_rule.py` | verified | none |
| F-002 | `resolve_limit_rate` 主板日期矩阵 | `backend/tests/dataset_release/test_a_share_limit_rule.py` | verified | none |
| F-003 | `resolve_limit_rate` 创业板/科创板矩阵 | `backend/tests/dataset_release/test_a_share_limit_rule.py` | verified | none |
| F-004 | typed `no_daily_limit` decision | `backend/tests/dataset_release/test_a_share_limit_rule.py` | verified | none |
| F-005 | `derive_limit_prices` Decimal/tick 实现 | `backend/tests/dataset_release/test_a_share_limit_rule.py` | verified | none |
| F-006 | `backend/services/dataset_release/stk_limit_overlay.py` reference state | `backend/tests/dataset_release/test_stk_limit_overlay.py` | verified | none |
| F-007 | missing-only overlay 与合并冲突门禁 | `backend/tests/dataset_release/test_stk_limit_overlay.py`; `backend/tests/dataset_release/test_artifact_ready_build_source.py` | verified | none |
| F-008 | artifact-ready CAS receipt/identity | `backend/tests/dataset_release/test_artifact_ready_source.py` | verified | none |
| F-009 | `ArtifactReadyBuildSource._effective_limit_rows` | `backend/tests/dataset_release/test_artifact_ready_build_source.py`; `backend/tests/dataset_release/test_canonical_stock_transformer.py` | verified | none |
| F-010 | canonical PIT span 内明确 `is_st=False`；独立计算器覆盖 ST | `backend/tests/dataset_release/test_a_share_limit_rule.py`; `backend/tests/dataset_release/test_stk_limit_overlay.py` | verified | none |
| F-011 | 分区滚动 reference state 与 hard cap | `backend/tests/dataset_release/test_stk_limit_overlay.py` | verified | none |
| F-012 | 规则 fixture 分币一致与完整 dataset_release 回归 | `backend/tests/dataset_release/test_a_share_limit_rule.py`; `backend/tests/dataset_release/test_artifact_ready_source.py` | verified | none |
| F-013 | 原始分区保留、非空 overlay 加入 effective root 和月叶 | `backend/tests/dataset_release/test_artifact_ready_source.py`; `backend/tests/dataset_release/test_component_artifact_manifest.py` | verified | none |
| F-014 | changed-file runtime/ownership 审核 | `tests/aistock_validation/catalog/file_ownership.yaml`; `python -m nox -s l0` | verified | none |
| F-015 | 零写入 safety、无真实导出 | `backend/tests/dataset_release/test_artifact_ready_source.py`; `python -m pytest backend/tests/dataset_release -q -p no:cacheprovider` | verified | none |
| F-016 | overlay 携带受影响代码，首次历史缺口只生成精确代码 override | `backend/services/dataset_release/component_artifact_manifest.py`; `backend/services/dataset_release/mixed_planner.py`; `backend/tests/dataset_release/test_component_artifact_manifest.py` | verified | none |
| F-017 | Skill/reference/runbook 固化统一缺失处理流程 | `.codex/skills/update-backtest-dataset/SKILL.md`; `.codex/skills/update-backtest-dataset/references/fingerprint-and-reuse.md`; `.codex/skills/update-backtest-dataset/references/monthly-workflow.md`; `docs/operations/qe_backtest_dataset_monthly_update_runbook.md`; `python -m nox -s l0` | verified | none |
| F-018 | 七个精确源文件登记为 dataset Worker runtime | `docs/standards/aistock_runtime_targets_v1.yaml`; `backend/tests/scripts/test_aistock_issue_workflow.py::test_dataset_release_limit_overlay_runtime_targets_only_dataset_worker` | verified | none |
| F-019 | 三层缺失/重试/硬阻断治理与先批准原则 | `scripts/seed_dataset_refresh_audit.py`; `.codex/skills/update-backtest-dataset/SKILL.md`; `backend/tests/test_dataset_refresh_audit.py` | verified | none |

## 14. DESIGN-COMPLIANCE-001

1. 禁止简化、子集、POC、占位或 partial：规则矩阵、overlay、daily/minute 共用、身份和负向路径均为必需项。
2. 禁止静默错误或伪成功：输入不足、无涨跌幅异常键、重复、覆盖、未知板块全部 typed fail closed。
3. 禁止未经确认的业务逻辑迁移：只修改候选数据集 artifact-ready 路径，实时模拟盘权威不变。
4. 禁止新增未经确认的门禁或人工审批：采用第 11 节已批准三层表；未来新增硬阻断先分析并取得用户批准。

## 15. Review History / 审核记录

| revision | finding | resolution | status |
|---|---|---|---|
| R1 | “ST=5%”忽略 2026-07-06 沪深主板规则切换，并会误伤创业板/科创板 ST | 建立日期化板块规则矩阵 | resolved |
| R2 | 直接使用上一日 close 会在除权除息日产生错误 | 使用 `close_prev × adj_prev / adj_current`，缺失阻断 | resolved |
| R3 | 执行层 fallback 会污染 Paper/MiniQMT 实时权威 | 仅 candidate-local artifact-ready overlay，实时模块零改动 | resolved |
| R4 | 全市场面板会重现数十 GB 内存问题 | 分区扫描、每代码滚动状态和 overlay 硬上限 | resolved |
| R5 | 用派生分区替换原始身份会让首次采用被误判为多年重建 | 始终保留 raw `stk_limit`；只为非空缺失月份增加 overlay 身份和月叶 | resolved |
| R6 | mixed planner 会把首次历史 overlay 当普通 tail，既可能写错月份，也可能扩大到全 PIT 股票 | overlay 绑定精确代码；新增历史 sparse-addition 分类并只生成对应代码 override | resolved |
| R7 | 仅修改代码会使下月 operator/Claude/Codex 仍按旧缺口说明执行 | 更新唯一 Codex Skill 主源、两个按需 reference 和正式 runbook；Claude wrapper 继续引用同一主源 | resolved |
| R8 | 通用 backend fail-closed 分类会遗漏常驻 dataset Worker 的代码重载要求 | 只登记本功能实际进入 Worker 闭包的七个文件为 `worker-scheduler`；保留其他 backend/dataset_release 文件原分类 | resolved |
| R9 | 每月分区内保存全部股票的 limit 键虽有界但仍高于必要峰值 | limit/daily/adj 三路按代码流式归并；内存仅保留 PIT 滚动状态、单代码单月行及受硬上限约束的实际 overlay | resolved |
| R10 | 全 DEV 历史镜像、合法空日和候选可补缺口被误当成永久阻断 | DEV 改为事务内单行 DML 回滚验证；生产全范围只读 plan；引入自动/可重试/硬阻断三层语义 | resolved |
