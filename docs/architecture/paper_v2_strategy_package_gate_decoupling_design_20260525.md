# Paper v2 StrategyPackage 门禁解耦设计方案

> 日期：2026-05-25  
> 状态：设计基线，先合入 `main` 作为后续 issue 拆分与实现验收依据。  
> 范围：Selection Center、Paper Trading v2、MiniQMT 模拟盘相邻路径、HMM 平台运行、Paper v2 runtime profile、统一交易日状态、选股结果价格展示。  
> 明确排除：真实实盘下单验证。未来实盘审批可以复用本文的只读、模拟、回放、MiniQMT sim 验证证据，但不得把本文视为真实下单已验证。

## 1. 背景和问题定义

Paper v2 和 Selection Center 当前的主要不可用原因不是单一 bug，而是把多类不同性质的能力混成了 `StrategyPackage` 的准入门禁：

1. `StrategyPackage` 本应只冻结 QE 生成的 alpha core：模型、因子、权重、seed、训练配置、回测证据、manifest/hash。
2. HMM、行业黑名单、交易日状态、TDX 当前价格、停牌/涨跌停、broker、执行算法、MiniQMT 授权都是平台运行能力或运行前检查，不应成为策略包能否进入选股或模拟盘的静态门禁。
3. 当前 `enable-paper` 路径要求 `governance_eligibility.paper_ready=true`，而 `paper_ready` 又包含 original fixed-weight retest、seed/regime stability、protected asset ledger、runtime variant candidate 等实盘级或治理级证据，导致回测已批准的 QE 策略包仍无法进入模拟盘。
4. 选股、模拟盘、未来实盘应共享平台能力，但共享方式应是 runtime profile / preflight / run evidence，而不是改写或阻断 frozen StrategyPackage。

本文的目标是把“策略包资格”“平台运行配置”“本次运行检查”“未来实盘审批”拆开，使回测优秀的 QE 策略包可以尽快进入选股和模拟盘，并通过非实盘验证矩阵证明行为符合设计。

## 2. 文档发现和当前代码事实

本方案基于以下现有文件和接口事实：

| 来源 | 事实 |
| --- | --- |
| `docs/standards/aistock_development_standard_v1.5_20260523.md` | P0/P1、fail-fast、禁止交易/HMM 静默降级、DESIGN-COMPLIANCE-001、GitHub Issue 与本地 BUG JSON 同步是强制规则。 |
| `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md` | issue 修复必须保留 allowed write scope、closure requirements、required verification、production gates 和 GitHub/BUG 同步。 |
| `docs/architecture/paper_v2_qe_integration_overhaul_20260512.md` | 已记录 Paper v2 与实盘门禁定位错配：实盘级 governance gate 不应阻塞 Paper v2。 |
| `backend/services/strategy_package/service.py` | `transition_status(...PAPER_ENABLED...)` 调用 `_require_governance_paper_ready()`；`governance_eligibility()` 把 original retest、seed/regime stability、protected asset、runtime variant candidate 汇总为 `paper_ready`。 |
| `frontend/src/app/paper-v2/packages/page.tsx` | UI 仅允许 `selectedStatus === "PAPER_ENABLED"` 时创建模拟盘组合，尽管 Selection 已把 `BACKTEST_APPROVED` 视为可运行状态之一。 |
| `backend/services/strategy_package/validators.py` | `validate_manifest_identity_for_paper_trading()` 已允许 `BACKTEST_APPROVED`、`SELECTION_ENABLED`、`PAPER_ENABLED`，说明底层 manifest 身份校验已经可以支持较宽松的 Paper v2 准入。 |
| `backend/services/selection_center/service.py` | 选股包列表和运行准备已允许 `BACKTEST_APPROVED`、`SELECTION_ENABLED`、`PAPER_ENABLED`。 |
| `backend/services/selection_center/hmm_runtime.py` | HMM runtime 当前只能读取预计算 coefficient artifact，启用 HMM 时要求 `model_snapshot_id` 和 `signal_preset`，且缺失 trade date coefficient 时 fail-fast。 |
| `backend/services/strategy_package/selection_artifact.py` | live inference 当前仅支持 `DB_HISTORICAL`，因此 UI 不应把 `TDX_REALTIME` 当成选股因子计算数据源。 |
| `backend/services/paper_trading_v2/session.py` | `_is_trading_day_for_operation()` 仍存在 weekday fallback，必须按 BUG-116 要求移除。 |

## 3. 设计原则

### 3.1 StrategyPackage 只代表 QE alpha core

`StrategyPackage` 的硬门禁只保留以下内容：

1. manifest 可解析，`manifest_sha256` 与 payload 一致。
2. 生命周期至少达到 `BACKTEST_APPROVED`，且不是 `RETIRED`。
3. 模型、因子、权重等 alpha core 引用可定位；缺失时阻断本次选股/模拟盘创建，并返回明确 artifact 错误。
4. QE source、loop/run、seed、训练配置、回测指标必须可追溯。

以下内容不得作为 Selection 或 Paper v2 simulation 的 `StrategyPackage` 硬门禁：

1. HMM 是否已有每日 coefficient artifact。
2. 行业黑名单是否已经配置。
3. 当前日期是否交易日、数据是否已刷新到当天。
4. TDX 实时/最新价格是否可访问。
5. broker/backend 与行情源绑定状态。
6. 执行算法是否已经部署或显式启用。
7. MiniQMT 下单授权状态。
8. future live approval 状态。
9. seed stability 或 rolling retrain evidence 是否完整。

这些内容只能进入 runtime profile、platform preflight、run evidence、warning 或 live-only approval。

### 3.2 随机 seed 和 rolling training 规则

QE 生成的模型和策略包可能来自固定 seed、多 seed、随机 seed 或其他训练策略。只要满足以下条件，就可以进入选股和模拟盘：

1. seed policy 被记录；如果是随机 seed，实际 seed 值或可复现实验标识必须冻结在 QE evidence / manifest lineage 中。
2. 模型 artifact、因子 artifact、训练窗口、回测窗口、指标、manifest hash 可追溯。
3. 回测表现和风险指标达到用户或平台定义的进入模拟盘标准。

不支持 rolling retrain、缺少 rolling retrain evidence、`seed_stability=INSUFFICIENT_EVIDENCE`、`regime_stability=INSUFFICIENT_EVIDENCE` 不得阻止进入 Selection 或 Paper v2 simulation。它们应显示为：

1. 风险提示。
2. 可选追加验证建议。
3. future live approval 的审阅输入。

未来真实实盘审批可以要求更严格的稳定性证据，但该要求只作用于真实下单审批，不反向阻塞选股或模拟盘。

### 3.3 平台能力运行时化

平台能力通过以下层次表达：

| 层次 | 作用 | 是否阻断 StrategyPackage |
| --- | --- | --- |
| `StrategyPackage alpha core` | 冻结 QE 产物、模型因子和回测证据 | 是，仅限 manifest/status/artifact identity |
| `PaperRuntimeProfile` | HMM、行业黑名单、风险规则、报价策略、执行 profile、交易日服务等运行配置 | 否 |
| `Preflight` | 针对某次选股、回放、模拟盘运行检查当前数据、缓存、服务、broker、算法是否可用 | 否；只阻断本次运行 |
| `RunEvidence` | 记录本次运行实际用到的 profile hash、HMM cache key、交易日状态、价格来源、broker/source 等 | 否 |
| `LiveApproval` | 未来真实下单审批 | 否；只阻断真实下单路径 |

## 4. 目标架构

### 4.1 准入模型

引入四个概念边界：

1. `AlphaCoreEligibility`：策略包是否可用于选股和模拟盘的最小静态资格。
2. `PaperSimulationAdmission`：是否允许基于该策略包创建 Paper v2 模拟盘组合。
3. `RuntimePreflight`：某次运行是否具备需要的交易日、数据、报价、HMM cache、行业配置、broker/source、执行 profile。
4. `LiveStrictGovernance`：未来真实下单前的严格审批和人工授权。

`AlphaCoreEligibility` 的通过标准：

1. `package_status in {"BACKTEST_APPROVED", "SELECTION_ENABLED", "PAPER_ENABLED", "PAPER_RUNNING", "PAPER_PASSED"}`。
2. `package_status != "RETIRED"`。
3. manifest hash 校验通过。
4. alpha core 关键 artifact 引用可解析。

`PaperSimulationAdmission` 的通过标准：

1. `AlphaCoreEligibility` 通过。
2. 用户选择或系统生成一个可版本化的 `PaperRuntimeProfile`。
3. 创建组合时记录 runtime profile id/version/hash；不得改写 frozen StrategyPackage manifest。
4. 若运行期缺数据、缺 HMM cache、缺报价、broker/source 不匹配，失败对象是 preflight/run，不是 StrategyPackage。

### 4.2 现有门禁处置表

| 当前门禁/限制 | 当前影响 | 新位置 | 新行为 |
| --- | --- | --- | --- |
| `paper_ready=false` | 阻止 `enable-paper` 和 UI 创建模拟盘 | governance/read-only 或 live-strict | 不阻止 Paper v2 simulation；作为风险摘要展示。 |
| original fixed-weight retest missing | 阻止 `paper_ready` | optional validation evidence | Paper v2 仅提示；未来 live approval 可要求。 |
| seed/regime stability insufficient | 阻止 `paper_ready` | warning / optional validation | 不阻止 Selection/Paper；随机 seed 只要已记录且回测优秀即可进入模拟盘。 |
| protected asset ledger missing | 阻止 `paper_ready` | asset lineage health | Paper v2 不因 ledger 缺失阻断；关键 artifact 不可加载时才阻断本次运行。 |
| runtime variant paper candidate missing | 阻止 `paper_ready` | PaperRuntimeProfile | 不要求预先创建 package runtime variant；创建模拟盘时生成/绑定 runtime profile。 |
| HMM manual snapshot/coefficient | 要求手工选快照和 artifact | HMM platform resolver/cache | 选择模型/config/preset 后 compute-on-miss，同 key cache hit；无手工每日快照。 |
| 行业黑名单文本框 | 配置脆弱且可能误解为全局 | Paper runtime profile UI | QE-style 行业树选择，但写入 Paper profile；不写 QE global，不改 manifest。 |
| 交易日 weekday fallback | 可能误判交易日 | platform calendar service | 统一 API + 文件缓存；禁止 weekday fallback。 |
| `TDX_REALTIME` 选股数据源 | 明知会失败 | quote resolver/display price | 不再列为因子/selection artifact source；TDX 只用于报价和当前价。 |
| broker/source 绑定 | 被误解为策略包能力 | preflight/runtime binding | mismatch fail-fast 本次组合或运行；不影响策略包资格。 |
| 执行算法/执行策略 evidence | 阻止创建组合 | execution runtime profile/preflight | 平台默认或用户选定执行 profile；不可用时阻断本次运行，不阻断策略包资格。 |
| MiniQMT 授权 | 真实/模拟下单安全 | broker runtime preflight | 只阻断 MiniQMT 提交动作；不阻断选股和 LocalSim。 |
| live approval | 未来真实盘审批 | live-only gate | 只阻断真实下单，不阻断 Selection/Paper。 |

### 4.3 API 与 UI 调整方向

#### StrategyPackage API

保留现有 governance endpoint，但改变语义：

1. `/strategy-packages/{id}/governance-eligibility` 返回 `paper_ready` 可继续存在，但标记为 `live_strict_or_governance_readiness`，不得作为 Paper v2 simulation 硬门禁。
2. 新增或复用一个只读 admission summary，明确返回：
   - `alpha_core_eligible`
   - `paper_simulation_allowed`
   - `warnings`
   - `runtime_preflight_required`
   - `live_strict_blockers`
3. `enable-paper` 不再要求 `_require_governance_paper_ready()`；如果保留状态转换，它只记录用户意图或标记，不是创建模拟盘的必要步骤。

#### Paper v2 packages UI

1. `BACKTEST_APPROVED` 包应显示“可创建模拟盘”，但同时显示 governance warning。
2. “标记可用于模拟盘”不再是创建组合前置步骤。
3. “用此包创建模拟盘”按钮基于 `paper_simulation_allowed`，不是 `selectedStatus === "PAPER_ENABLED"`。
4. UI 必须区分：
   - 策略包 alpha core 状态。
   - runtime profile 完整性。
   - 本次 preflight 可运行性。
   - future live readiness。

#### Selection Center

1. 选股因子/score artifact source 只列出真实支持的 source，例如 `DB_HISTORICAL`。
2. `TDX_REALTIME`、`MINIQMT_REALTIME` 等已知无法作为因子计算 source 的选项不再展示。
3. TDX 通过 quote resolver 提供：
   - current-date selection 的 entry/watchlist price。
   - 历史 selection 的 display-only current price。
   - 当前最新价展示。
4. 历史日期选股，例如目标日 `2026-05-13`，因子和入池价必须使用 `2026-05-12` 或更早 PIT 可见数据；不得使用目标日未来数据。

#### HMM 平台 resolver/cache

1. runtime profile 选择 `hmm_model_id` 或 `hmm_config_id`、`signal_preset`，不得要求每日手工 `snapshot_id`。
2. resolver 使用 `(model/config, signal_preset, as_of_date, effective_trade_date, data_fingerprint)` 作为 cache key。
3. cache miss 自动计算；cache hit 直接复用。
4. 同 key 并发请求必须串行化或幂等，只产生一个有效 artifact。
5. 缺模型、缺输入数据、系数非法、行业映射缺失时 fail-fast；禁止中性系数 fallback。
6. backtest/research 仍可保留手工 snapshot，但不能成为 Selection/Paper/Live runtime 的日常前置操作。

#### 行业黑名单

1. QE global blacklist 只属于 QE 实验。
2. Paper v2 使用 package/runtime-profile scoped blacklist。
3. UI 复用 QE 风格行业树选择体验，但组件要拆成无 QE 副作用的 shared industry selector。
4. Paper v2 不调用 QE global blacklist API，不生成 QE filtered stock pool，不改 frozen manifest。
5. 每次 Selection/Paper run 持久化使用的 industry codes/names/profile hash。

#### 统一交易日状态

1. AIstock 后端提供唯一官方 trading-day status service/API。
2. source of truth 是 `market.trading_calendar`。
3. API 正常读取文件系统缓存，不在每次请求时查 DB。
4. cache 缺失、过期、跨月、calendar sync 成功后自动重建。
5. 如果 DB 不覆盖下一个完整月份，返回 warning，提醒更新交易日表。
6. 禁止所有 trading-sensitive path 使用 weekday fallback。
7. Paper v2 首页展示：
   - 今天是否交易日。
   - 最近已结束交易日。
   - 最近下一个交易日。
   - cache/coverage warning。

#### 选股结果价格和展示

1. 股票名称显示复用 Paper v2 已实现的名称解析逻辑。
2. 主表移除“Trace key 列表”式低价值展示；component_scores 只保留在详情/审计面板。
3. 新增或明确字段：
   - `stock_name`
   - `selection_entry_price`
   - `selection_entry_price_source`
   - `selection_entry_price_time`
   - `previous_close`
   - `volume`
   - `current_price`
   - `current_price_source`
   - `current_price_time`
4. 加入自选股票池时使用 `selection_entry_price`，不得使用 display-only `current_price`。

## 5. 后续 issue 组织方式

本设计先合入 `main`。新的 issue 工作流更新完成后，按本设计提交或更新 issue：

1. 创建一个 umbrella issue：`Paper v2 StrategyPackage Gate Decoupling`。
2. 将以下已有 issue 标为 umbrella 的子任务或纳入范围：
   - `BUG-103` / GitHub #172：manifest drift 阻断选股/模拟盘。
   - `BUG-112` / GitHub #182：HMM 手工快照改为平台 resolver/cache。
   - `BUG-114` / GitHub #184：Selection data source 语义拆分。
   - `BUG-115` / GitHub #185：Paper runtime profile 行业黑名单 UI。
3. 将以下 issue 保持独立但纳入同一项目看板或依赖：
   - `BUG-113` / GitHub #183：选股结果名称、价格、成交量展示。
   - `BUG-116` / GitHub #187：统一交易日状态服务。
4. 已 fixed 的 `BUG-085/#107`、`BUG-086/#108`、`BUG-092/#111`、`BUG-093/#112`、`BUG-096/#149` 作为回归保护，不重开。

## 6. 实施分期

### Phase A：准入解耦最小闭环

目标：`BACKTEST_APPROVED` QE 策略包可进入 Selection 和 Paper v2 simulation 创建。

任务：

1. 后端新增 `AlphaCoreEligibility` / `PaperSimulationAdmission` 服务或方法。
2. `enable-paper` 不再用 `paper_ready` 阻断 Paper v2 simulation；`paper_ready` 转为治理信息。
3. Paper v2 create portfolio 使用 `validate_manifest_identity_for_paper_trading()` 和 runtime profile preflight，而不是 `PAPER_ENABLED` 状态。
4. 前端创建模拟盘按钮改为基于 `paper_simulation_allowed`。
5. 对 `pkg_378...`、`pkg_2a9...`、`paper_d842...` 等历史问题包执行 manifest identity 和 admission 验证。

### Phase B：平台 runtime profile 与 preflight

目标：把 HMM、行业黑名单、交易日、报价、执行 profile、broker/source 都移动到 runtime profile/preflight。

任务：

1. HMM resolver/cache compute-on-miss。
2. Paper scoped industry selector 和 profile 版本化。
3. Selection source/quote source/execution source UI 语义拆分。
4. 统一交易日状态服务和文件缓存。
5. 执行 profile 与 broker/source mismatch 作为组合/运行 preflight，不再作为策略包门禁。

### Phase C：Selection/Paper 结果语义和可追溯性

目标：每次运行都能说明用了什么数据、什么 profile、什么价格、什么 cache。

任务：

1. Selection result 增加股票名称、PIT entry price、当前价、昨收、成交量。
2. Watchlist import 使用 entry price。
3. Run evidence 持久化 runtime profile hash、calendar cache id、HMM cache key、price source/time、broker/source。
4. component_scores 移到详情/审计面板。

### Phase D：非实盘 E2E 验证和回归保护

目标：不触发真实下单，但证明完整流程可用。

任务：

1. QE package -> Selection -> Paper portfolio -> LocalSim day run。
2. QE package -> historical replay。
3. HMM on/off 对比和 cache hit。
4. MiniQMT sim preflight / dry-run，不发真实订单。
5. UI E2E 覆盖 packages、selection、portfolio 创建、Paper 首页交易日状态。

## 7. 非实盘验证矩阵

所有实现 issue 必须把本矩阵拆成 `required_verification`。除明确标记为 future live 的内容外，不能以“实盘未开发”为理由跳过。

| 编号 | 验证层级 | 覆盖功能 | 标准 | 最低证据 |
| --- | --- | --- | --- | --- |
| V-01 | 静态设计合规 | StrategyPackage 与 runtime profile 边界 | grep/代码审查证明 HMM、行业黑名单、交易日、TDX quote、broker、MiniQMT 授权、live approval 不在 StrategyPackage paper admission 硬门禁中 | DESIGN-COMPLIANCE-001 表 |
| V-02 | 单元测试 | `BACKTEST_APPROVED` admission | `BACKTEST_APPROVED` 且 manifest hash 正确的包可通过 `paper_simulation_allowed`；`DRAFT`、`RETIRED`、manifest mismatch 被阻断 | backend unit tests |
| V-03 | 单元测试 | `paper_ready` 降级为信息 | original retest missing、seed/regime insufficient、protected asset ledger missing、runtime variant missing 不阻断 Paper simulation admission，但出现在 warnings/governance summary | backend unit tests |
| V-04 | 单元测试 | 随机 seed 规则 | `random_logged` 或等价 seed policy 包在 seed 已记录、回测通过时可进入 Selection/Paper；rolling retrain missing 仅 warning | backend unit tests |
| V-05 | 单元测试 | manifest drift | stored hash 与 manifest payload 不一致时仍 fail-fast，不能被 lenient admission 绕过 | backend unit tests |
| V-06 | API 集成 | selectable packages | `BACKTEST_APPROVED` 包在 `/selection-center/selectable-packages` 和 Paper admission API 中可见，返回可解释 warning | API test |
| V-07 | API 集成 | create portfolio | 不调用 `enable-paper` 也能用符合资格的 `BACKTEST_APPROVED` 包创建 Paper v2 portfolio；runtime profile hash 被记录 | API test |
| V-08 | 前端 E2E | packages page | `BACKTEST_APPROVED` 包显示“可创建模拟盘”，按钮可点击；governance warning 不阻塞 | Playwright |
| V-09 | HMM 单元 | compute-on-miss | 没有每日 coefficient 时第一次自动生成 cache artifact；不要求用户手工 snapshot | backend unit/integration |
| V-10 | HMM 单元 | cache hit | 同一 `(model/config, preset, as_of, effective_date, data_fingerprint)` 第二次运行复用 cache，不重复计算 | backend unit/integration |
| V-11 | HMM 并发 | idempotency | 同 key 并发只产生一个成功 artifact，其余等待或复用；失败保留明确错误状态 | backend concurrency test |
| V-12 | HMM 失败 | fail-fast | 缺模型、缺输入数据、sector mapping 缺失、非正/非有限 coefficient 全部失败；禁止 neutral coefficient fallback | backend tests + grep |
| V-13 | 行业 UI | QE-style selector | Paper v2 可用树状行业选择；不调用 QE global blacklist API，不生成 QE stock pool | Playwright + API mock/assert |
| V-14 | 行业后端 | runtime scoped blacklist | blacklist 以 package/runtime profile 版本保存；Selection 和 Paper 使用同一解析结果；不改 manifest | backend tests |
| V-15 | 数据源 UI | 移除必失败选项 | Selection 因子/artifact source 下拉不出现 `TDX_REALTIME`、`MINIQMT_REALTIME` | Playwright/static test |
| V-16 | 数据源后端 | source 语义拆分 | `DB_HISTORICAL` 用于 factor/artifact；TDX 只走 quote resolver；execution source 只在 Paper/Broker profile 中出现 | backend tests |
| V-17 | 价格语义 | 当前日选股 entry price | 当前日期选股从 TDX latest quote/close 取 `selection_entry_price`，记录 source/time；TDX 不可用时明确失败或标记不可入池 | backend/API test |
| V-18 | 价格语义 | 历史选股 PIT entry price | 目标日 `T` 的 entry/watchlist price 使用 `T-1` 或更早 PIT close，不使用 `T` 当天未来数据或当前价 | backend/API test |
| V-19 | 价格语义 | current price 展示 | `current_price` 从 TDX 最新 quote 获取，仅用于显示；watchlist import 不使用它 | backend + frontend E2E |
| V-20 | 股票名称 | Selection result name | 选股结果股票名称与 Paper v2 组合持仓显示逻辑一致 | backend + UI test |
| V-21 | 交易日服务 | cache 正常读 | 连续 API 调用读取文件缓存，不每次查询 DB；cache 记录 coverage、checksum、generated_at | backend tests |
| V-22 | 交易日服务 | 自动重建 | cache missing/stale/cross-month/calendar sync 后自动重建，无手工 cache 生成步骤 | backend tests |
| V-23 | 交易日服务 | coverage warning | DB 不覆盖下一个完整月份时返回 warning | backend tests |
| V-24 | 交易日服务 | 禁止 weekday fallback | trading-sensitive backend/frontend 不使用 weekday/browser weekday 作为 fallback；缺当前日期 row fail-fast | static grep + tests |
| V-25 | Paper 首页 | 交易日展示 | `/paper-v2` 显示今天是否交易日、最近已结束交易日、最近下一个交易日和 cache warning | Playwright |
| V-26 | broker/source | mismatch preflight | broker/source 不匹配只阻断组合创建或运行 preflight，不改变策略包资格 | backend tests |
| V-27 | execution profile | 平台能力化 | 缺执行 profile 或算法不可用阻断本次 Paper run/create，错误指向 runtime profile/preflight，不是 package not paper_ready | backend tests |
| V-28 | LocalSim E2E | 完整模拟盘 | 指定 QE 包完成 Selection -> Paper portfolio -> one-day LocalSim run；产生 run/events/errors/positions/snapshots | API/business test |
| V-29 | replay E2E | 历史回放 | 历史日期范围 replay 使用统一 runtime profile 和 trading-day service；重复 run 策略符合现有 rerun policy | backend integration |
| V-30 | MiniQMT sim dry-run | 非实盘 MiniQMT 相邻路径 | MiniQMT sim preflight/dry-run 读取持仓/现金或 mock ledger，不提交真实订单；授权缺失不影响 Selection/LocalSim | dry-run evidence |
| V-31 | 回测等价性 | score path | 同一 trade_date、同一包、同一 runtime profile 下，Paper/Selection score 与 QE artifact 或重算结果在 epsilon 内一致 | business oracle |
| V-32 | 审计可追溯 | run evidence | 每次运行记录 package id/hash、runtime profile hash、HMM cache key、calendar cache id、price source/time、broker/source | DB/API test |
| V-33 | 回归保护 | 已 fixed issue | BUG-085/086/092/093/096 的边界不回退：不改 manifest、runtime config 需版本/hash、shared engine 决策一致 | regression tests |
| V-34 | 生产边界 | 端口和依赖 | 验证记录必须声明 `8001/3000` 是否触碰，`production_ddl_gate`、`production_frontend_dependency_gate`、`production_backend_dependency_gate` 状态 | validation record |

## 8. 验证通过标准

### 8.1 单个 issue 的通过标准

每个根据本文创建或更新的 issue 必须满足：

1. `required_verification` 覆盖本设计对应矩阵编号。
2. `closure_requirements` 明确引用设计中的边界和非目标。
3. 实现完成后提交 DESIGN-COMPLIANCE-001 item-by-item 表。
4. 所有相关自动测试通过；无法自动化的项必须有人工验证记录、命令、截图或 API 输出摘要。
5. 若引入 DB migration，必须包含 table/column comment，且 production DDL gate 在合入后单独报告。
6. 不得以“后续 issue 再补”替代本 issue 已承诺的 closure requirements。

### 8.2 umbrella 项目的通过标准

umbrella 项目整体完成必须满足：

1. 一个 `BACKTEST_APPROVED` QE 策略包无需 `paper_ready` 即可完成 Selection 和 Paper v2 portfolio 创建。
2. 至少一个真实历史策略包完成 LocalSim one-day run 或 replay。
3. HMM 启用时不需要每日手工 snapshot，第一次自动计算，第二次 cache hit。
4. Selection UI 不再出现必失败 data source。
5. Paper v2 行业黑名单使用 QE-style 选择体验，但不写 QE global。
6. Paper 首页显示统一交易日状态，并且所有 weekday fallback 被移除。
7. 选股结果显示股票名称、entry price、current price、previous close、volume，watchlist import 使用 entry price。
8. seed/rolling training 不再作为 Selection/Paper simulation 硬门禁。
9. 所有非实盘验证矩阵中与已实现功能相关的行均为 passed；未实现功能必须在 umbrella issue 中保持 open，不得关闭。

### 8.3 禁止的交付方式

1. 只改 UI 按钮，但后端仍用 `paper_ready` 阻断。
2. 只绕过异常，不提供 runtime profile/preflight/run evidence。
3. 用 weekday fallback 兜底交易日。
4. HMM 缺 cache 时要求用户手工生成快照。
5. 用中性 HMM coefficient 伪装成功。
6. 把 TDX 实时价作为历史选股 entry price。
7. 把 Paper v2 行业黑名单写入 QE global 或 frozen manifest。
8. 把 MiniQMT 授权、broker、live approval 重新包装为 StrategyPackage 门禁。
9. 把 rolling retrain 缺失作为 Paper simulation 阻断项。

## 9. 实盘验证边界

本文明确不要求真实实盘下单验证。允许的非实盘验证包括：

1. LocalSim。
2. historical replay。
3. API dry-run。
4. UI E2E。
5. MiniQMT sim preflight / dry-run。
6. read-only MiniQMT account/position/cash snapshot。
7. mocked broker submit。

未来实盘 issue 必须另行设计：

1. live approval lifecycle。
2. 真实 broker 授权。
3. 真实订单 submit/cancel/fill reconciliation。
4. 风险人工审批。
5. 生产运行值班和回滚方案。

不得用未来实盘未完成来阻止本文的 Selection/Paper simulation 解耦。

## 10. 后续执行要求

1. 本文合入 `main` 后，所有相关 issue 必须引用本文路径。
2. 新 issue workflow 更新完成前，不新增本项目 issue；只保留本设计作为依据。
3. 实现时使用独立 worktree 和 task branch，不在生产 root 直接开发。
4. 每个实现 PR 必须报告：
   - 设计矩阵覆盖行。
   - 测试命令和结果。
   - GitHub Issue / BUG JSON 同步状态。
   - production gates。
   - 是否触碰 `8001/3000`。
5. 合入后不得自动重启生产 `8001/3000`，除非用户明确要求。
