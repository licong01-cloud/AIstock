# Advisory 核心指数股票池选择 F2 详细设计

> 版本：v1.2
>
> 日期：2026-09-12
>
> 状态：IMPLEMENTED_LOCAL_VERIFIED_LIVE_UNIVERSE_DEV_READY
>
> 归属：Selection Center / Advisory
>
> 上位蓝图：`advisory_strategy_conditioned_model_blueprint_v1_20260710.md` v3.53

## 1. Background

QE 已提供统一的股票池选择语义，可按全市场、单个核心指数或多个核心指数并集运行实验。Advisory Program/Binding 此前没有显式股票池合同，旧流程只能消费 Selection 给出的候选全集，无法保证新进入荐股名单的股票属于用户选择的指数成分集合。

同时，QE 窗口已成为历史全量复验、多 seed、因子/模型组合 Alpha 审计和新组合搜索的唯一实验所有者。Advisory 不再建设平行实验线。本切片只补齐 QE 正式交付物进入荐股产品后的股票池约束能力，不提交 QE 任务，也不改变任何 Alpha 信号。

## 2. Scope

1. Advisory 创建 Program 和版本化 Binding 时接受 `universe_selection={mode,pool_ids}`。
2. 支持与 QE 同形的三种语义：`stock_universe`、`single_index`、`index_union`。
3. 支持 P0 核心指数：`csi300`、`csi500`、`csi1000`、`star50`、`star100`。
4. 单日复评和历史回放按复评日解析 PIT 指数成员；正式每日 forward 按 `selection_as_of_trade_date=D-1` 解析股票资格，`target_trade_date=D` 只表示推荐目标日。在 Selection 候选之后、Advisory 排名和动作决策之前过滤并重新编号；历史优先使用冻结 canonical PIT，超过其截止日时只读使用 Selection 已验证的实盘滚动 PIT。
5. API 返回可选股票池目录；页面可在创建任务和更新绑定时选择单指数或多指数并集。
6. 保存股票池准入 receipt，使每次复评可追溯到选择合同、日期、成员 revision、集合 hash 和过滤计数。
7. 正式 forward 发布、target-open 结算和 Advisory model/outcome/price-range shadow 都消费同一冻结后的候选子集与重排 rank。

## 3. Non-goals

- 不修改 QE、Selection Center、StrategyPackage、数据集、Paper、QMT 或其它模块源码。
- 不由 Advisory 启动训练、回测、multi-seed、LOO、因子组合或新模型实验。
- 不把指数股票池过滤宣称为新的 Alpha，也不改变策略包原始 score。
- 不宣称“全市场 StrategyPackage 产出后再过滤”等价于“该 StrategyPackage 在指数全集内重新推理”；严格同 QE 指数实验身份的运行必须消费 QE 后续正式发布的对应指数 StrategyPackage。
- 不在 Selection 候选不足时扩大候选深度、跨池补位或回退全市场。
- 不新增表、列、DDL/DML、后端进程控制或生产激活。
- 不在本切片实现动态资金仓位或指数权重配置。

## 4. Architecture

数据流固定为：

`QE正式StrategyPackage/预测 -> Selection候选 -> Advisory PIT股票池准入 -> 连续重排 -> Advisory排名/Entry/Exit -> 推荐列表`

股票池准入属于 Advisory 消费层，不反写上游。共享的 `core_index_membership` 服务作为只读指数成员权威；Advisory 自有 adapter 负责公开合同校验、P0 pool 限制、两类 PIT 身份选择和 receipt。历史日期先使用 QE 同源的冻结 canonical equity PIT；仅当共享 resolver 明确返回 `CANONICAL_EQUITY_PIT_UNAVAILABLE` 时，才改用 Selection 生产语义的 ready/clean 实盘滚动 PIT。其它成员错误不得触发切源。`stock_universe` 为透传路径，不额外读取指数成员。该层保证新增荐股不会越出绑定股票池，但不会重算已由 Selection/StrategyPackage 产生的全市场 score；当 QE 发布指数专属正式包时，package identity 与 Advisory binding 仍需分别核对。

一个 Program 的股票池是 Binding 语义。运行时 override 只能与 active binding 完全一致。股票池只约束新进入准入；Binding 切换和指数成分变更都不得直接删除既有 ACTIVE episode，既有 episode 继续由冻结的 Exit policy 产生可解释退出动作。

## 5. Contracts

### 5.1 输入合同

```json
{
  "mode": "stock_universe | single_index | index_union",
  "pool_ids": ["csi300", "csi500"]
}
```

- `stock_universe` 必须为空 pool 列表。
- `single_index` 必须且只能有一个 P0 pool。
- `index_union` 必须有一个或多个 P0 pool；去重并按权威顺序规范化。
- 不接受未知字段、未知 mode、字符串形式的 `pool_ids` 或非 P0 pool。
- 旧 Program/Binding 缺失该字段时按 `stock_universe` 读取。

### 5.2 API 合同

- `GET /api/v1/advisory/universe-options`：返回 mode、默认选择及 P0 pool 目录。
- `POST /api/v1/advisory/programs`：可携带 `universe_selection`。
- Program update 与 binding apply：可版本化更新 `universe_selection`。
- binding 响应顶层显式返回规范化后的 `universe_selection`，同时在既有 `runtime_config_json` 内持久化，无需 DDL。

### 5.3 数据和 PIT 合同

- 非全市场普通复评按复评日调用共享 resolver；正式 forward 若带有 `advisory_date_context.selection_as_of_trade_date`，则必须用该 D-1 日期解析成员与股票资格，且该日期必须严格早于 D。不得为生成 D 日推荐而要求或读取尚未形成的 D 日股票资格状态。
- 成员集合来自 `market.core_index_membership_pit` 与股票资格 PIT 的交集；价格、停牌和涨跌停等实时/日行情不能替代股票资格与历史指数身份。
- 默认股票资格身份为冻结 canonical `all_a_ex_st_bj_ipo_ge_1y_v1 / shsz_a_252td_st_delist_asof_v2`。仅在该权威对目标日返回 `CANONICAL_EQUITY_PIT_UNAVAILABLE` 时，Advisory 自有只读 adapter 可使用 Selection 的 `shsz_st_pit_active_v1 / st_pub_next_trade_restore_active_l_v1`。
- 实盘滚动 PIT 必须同时满足精确 key/rule、`ready`、`dirty=false`、起止日期覆盖目标日以及合法 SHA-256 revision；任一条件不满足即 fail closed。它用于实盘/近期运行，不冒充 QE frozen profile 或 sealed/确认性证据。
- 成分缺失或解析为空时 fail closed，返回 typed data unavailable；不得回退全市场。
- 入选候选保留原策略包 score，只将过滤后的 rank 连续重排；component receipt 记录原 rank 与所属 source pool。
- 已有 ACTIVE episode 不因指数成分变更或 Binding 股票池切换而被无价格、无退出证据地直接删除；股票池是新进入准入边界，既有 episode 继续由冻结 Exit policy 处理。

### 5.4 运行时 receipt

每次 review/replay day 保存：

- `universe_selection`
- `trade_date`
- `universe_as_of_trade_date`
- `membership_revision`
- `symbol_set_sha256`
- `pit_source`
- `pit_universe_key`
- `pit_rule_version`
- `pit_revision`
- `input_candidate_count`
- `output_candidate_count`
- `excluded_candidate_count`
- `admission_stage=AFTER_SELECTION_BEFORE_ADVISORY_RANKING`

零个候选是合法的 `NO_ELIGIBLE_RECOMMENDATION` 路径，不得补位。系统性成员数据不可用与主动零推荐必须分别表示。

## 6. Runtime and compatibility

单日运行和历史回放复用 `_filter_candidates_for_universe`，正式 forward 发布调用同一公共准入入口。发布列表冻结过滤后的候选和 rank；target-open 结算及 model/outcome/price-range shadow 从该列表恢复精确投影，不重新采用原 Selection 全集。全市场默认路径保持旧行为且不触发 membership resolver。创建、更新和 apply 均通过既有 binding version/CAS 生效；旧历史 binding 可读。

本功能合入后需要用户自行重启后端才能激活 API 和服务源码。重启、运行时读回和源码合入是独立状态；本 PR 不执行重启。

2026-09-12 DEV 只读数据读回：`market.daily_basic` 与 Selection 实盘滚动 PIT 均覆盖至 `2026-09-11`，后者状态为 ready/clean、source fingerprint 合法；共享核心指数成分表覆盖五个 P0 pool。真实 resolver 在 `2026-09-11` 得到沪深300 `299` 只、沪深300与中证500并集 `795` 只，receipt 明确记录 `LIVE_SELECTION_ROLLING_PIT` 及其 key/rule/revision。冻结 canonical PIT 仍只覆盖至 `2026-08-31`，历史日期继续优先使用它。直接请求尚未覆盖的 `2026-09-14` 仍会 typed fail closed；但正式周一 forward 保存 `trade_date=2026-09-14`、以 `universe_as_of_trade_date=2026-09-11` 解析准入，无需读取未来 PIT。后续每日能否荐股取决于 D-1 日频行情、滚动 PIT 和指数成员流水线是否在决策截止前到位。

该状态只证明“实盘日期股票池准入”已具备，不冒充“数据库盘中实时报价”已就绪。现有正式 forward 的 target-open 结算读取 `market.kline_daily_raw` 与 `market.suspend_d`；普通同日 Selection 的 `AUTO` 入场价/展示价仍可读取 `TDX_REALTIME`。正式日频 Advisory 的 D→T 推荐则由并列 F1 合同强制 `DAILY_DB_ONLY`，逐股读取不晚于 D 的最后数据库收盘价，正常停牌保留且不调用 TDX；这不是盘中实时荐股。2026-09-12 DEV 只读检查显示 `market.quote_snapshot` 表存在但为 `0` 行，因此当前不能声明盘中实时价格可完全从数据库消费。若产品以后要求 DB-only 盘中荐股，必须先由行情数据所有者持续写入带时间戳、来源和新鲜度合同的实时快照，再由 Advisory 以只读 adapter 消费；本切片不得在表为空时静默改用日线或猜测实时价格。

## 7. Risks

1. Selection 只返回较浅 TopK 时，指数过滤后可能不足目标数。处理：明确少推荐或零推荐，不静默扩池；候选深度扩展属于 QE/Selection 所有者的后续决策。
2. 当前持有 episode 与新股票池冲突。处理：新股票池仅约束后续进入，不直接删除旧 episode；旧 episode 继续按冻结 Exit policy、价格与排名证据退出。
3. 历史成分数据缺口被误作主动弃权，或实盘滚动 PIT 被误当冻结 QE 身份。处理：成员不可用 fail closed 并使用 typed reason；receipt 固化 PIT source/key/rule/revision，与空候选、QE frozen/sealed 证据分离。
4. QE 与 Advisory 合同漂移。处理：复用共享 mode/pool 权威并以定向 contract tests 固定语义，但不修改 QE parser。
5. 股票池过滤被误读为收益增强。处理：蓝图和 UI 仅声明准入约束；收益有效性仍由 QE 实验和 Advisory 消费侧验证分别负责。

## 8. Verification Plan

### 8.1 Backend

- 三种 mode、P0 pool 目录、规范化、未知字段/非法 pool/非法形状。
- 创建 binding 持久化；运行时 override 漂移拒绝；换池不删除 active episode。
- 单指数/并集按 PIT 集合过滤、保留 score、连续重排、receipt 完整；正式 forward 的 D 与 D-1 分离。
- 零匹配候选不回退；成员数据缺失 fail closed；只有 frozen canonical 截止错误可触发 live rolling PIT，指数成员错误不可触发。
- live rolling PIT 的 key/rule、ready/clean、日期覆盖和 revision 校验，以及 receipt 的 PIT 来源传播。
- `stock_universe` 透传且 resolver 零调用。
- replay、daily review 与正式 forward 使用相同 resolver 和 receipt；结算与模型子层只消费发布时冻结的候选投影。
- Advisory 相关回归套件通过。

### 8.2 API/UI

- options endpoint 返回 QE 兼容目录。
- 创建 Program 请求携带单指数或多指数并集。
- 策略绑定管理器把股票池选择写入 replay draft 和 apply binding。
- 页面明确展示 binding 的 mode 和 pool IDs。
- TypeScript、lint 和定向 Playwright 通过。

### 8.3 Boundary

- changed-file 清单只包含 Advisory Program、forward、model-first、API/UI、Advisory 测试及本蓝图/详细设计。
- 扫描确认没有新增 QE 任务提交、训练或回测入口。
- 不执行 DDL/DML、进程控制或后端重启。

## 9. Rollout and rollback

Rollout：源码审核通过后创建独立小 PR；合入后等待用户重启后端，再读取 options、创建测试 Program、执行 preview 并核对 receipt。数据库 schema 无迁移。

Rollback：回退本 PR 即可。既有 binding 中的 `runtime_config_json.universe_selection` 对旧代码属于无害扩展字段；历史记录不删除、不改写。若共享成员权威不可用，指数模式 fail closed，全市场 Program 不受额外查询影响。

## 10. Production Gates

1. F2 validator 与 DESIGN-COMPLIANCE-001 全项通过。
2. Backend/API/UI 定向测试和 Advisory 最小回归通过。
3. changed-file 边界无非 Advisory 模块代码。
4. 零 QE 实验提交、零 DDL/DML、零进程控制。
5. PR 合入后由用户执行后端重启；重启后业务读回单独验收。

## 11. Implementation Plan

1. 新增 Advisory universe adapter 与 receipt。
2. 接入 Program create/update/binding apply、daily review 和 replay。
3. 增加 API 目录和前端创建/绑定配置。
4. 补齐边界、回归、UI 与合同测试。
5. 执行多轮审核修复、F2 validator 和 DESIGN-COMPLIANCE-001。
6. 审核通过后提交 PR；不在本任务内重启后端。

## 12. Design Acceptance Index

| ID | Requirement |
|---|---|
| F-001 | QE 单一拥有模型/Alpha实验，Advisory只实现消费侧股票池能力 |
| F-002 | 与QE同形的三mode及P0 pool合同，Program/Binding持久化且不可运行时漂移 |
| F-003 | 普通复评按复评日、正式forward按D-1截止日执行PIT成分过滤；连续重排、空结果不补位、缺源fail closed |
| F-004 | 单日/回放/正式forward同核并保存目标日D与准入截止D-1的可追溯receipt；结算及模型子层消费冻结候选投影；全市场旧路径兼容 |
| F-005 | API/UI支持创建与更新单指数或多指数并集并展示当前绑定 |
| F-006 | 只改Advisory边界；无QE/Selection/StrategyPackage/数据集修改、无实验、无DDL和进程控制 |

## 13. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 蓝图页首、§16；本设计§§1～3 | `backend/tests/watchlist/test_advisory_universe_selection.py`；changed-file及QE提交入口边界扫描 | IMPLEMENTED_VERIFIED | approved_by_user: QE统一负责全部实验，Advisory只实现消费侧能力 |
| F-002 | `backend/services/advisory_universe.py`、`advisory_program.py`、`backend/routers/advisory.py` | `python -m pytest backend/tests/watchlist/test_advisory_universe_selection.py backend/tests/watchlist/test_advisory_api.py -q -p no:cacheprovider` | IMPLEMENTED_VERIFIED | approved_by_user: 无DDL，沿用`runtime_config_json` |
| F-003 | `CoreIndexAdvisoryUniverseResolver`、`AdvisoryLiveRollingPitRepository`、`_universe_as_of_trade_date`、`_filter_candidates_for_universe` | `python -m pytest backend/tests/watchlist/test_advisory_universe_selection.py -q -p no:cacheprovider`；2026-09-11 DEV真实resolver读回 | IMPLEMENTED_VERIFIED | approved_by_user: frozen canonical优先；正式forward只要求live PIT覆盖D-1，不读取D日未来资格；候选扩深和指数全集重推理不在本切片范围 |
| F-004 | `run_review`、`run_replay`、`advisory_forward/service.py`、`advisory_model_first/model_inference.py` | `python -m nox -s watchlist_backend advisory_modeling_backend` | IMPLEMENTED_VERIFIED | none |
| F-005 | `frontend/src/lib/api/advisory.ts`、`frontend/src/app/paper-v2/advisory/page.tsx` | `python -m nox -s frontend_type_lint`；`playwright test frontend/tests/paper-v2/paper-v2-advisory-ui.spec.ts` | IMPLEMENTED_VERIFIED | approved_by_user: 运行时激活需合入后用户重启 |
| F-006 | 本PR changed-file范围 | `backend/tests/watchlist/test_advisory_universe_selection.py`；F2 validator及DESIGN-COMPLIANCE-001 | IMPLEMENTED_VERIFIED | approved_by_user: Advisory只新增只读live PIT adapter；未修改QE、Selection、StrategyPackage或共享数据模块，未写数据库 |

## 14. DESIGN-COMPLIANCE-001

- [x] 上位蓝图、详细设计、实现和测试逐项映射，无遗漏或未声明偏差。
- [x] 真实 QE/Selection/Advisory 权责边界保持，未用 mock 或规则冒充业务完成。
- [x] PIT、数据缺失、空推荐和运行时绑定语义 fail closed，无未来数据或静默 fallback。
- [x] API/UI/单日/回放均覆盖，不只交付后端局部能力。
- [x] 回归、静态检查、F2 validator、changed-file 边界和运行安全完成。
- [x] 合入、后端重启、运行时读回分别报告；未越权执行重启或生产操作。
