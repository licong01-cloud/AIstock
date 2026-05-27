# Paper v2 / Selection Runtime Gate Cleanup Addendum (2026-05-26)

> 状态：项目级整改补充设计，当前只提交设计，不修改运行代码。
> 所属分支：`docs/paper-v2-gate-cleanup-design-20260526`。
> 适用范围：Selection Center、Paper Trading v2、AIstock LocalSim、MiniQMT SIM、Paper v2 UI、HMM runtime、PIT/交易日解析、错误展示和验证矩阵。
> 权威性：本补充设计覆盖 `paper_v2_gate_purge_project_design_20260525.md` 中未明确处理的 `runtime_profile_activation`、PIT 截止日/交易日校验、UI 残留 disabled gate 和错误分类问题。
> 非目标：不启用实盘、不修改生产 `8001`、不执行数据库 DDL、不走 Issue/BUG 流程。

## 1. 本次暴露的根因

2026-05-26 选股请求在策略包资产合格的情况下失败，错误为：

```text
STRATEGY_PACKAGE_VALIDATION_ERROR
runtime_config changes trading behavior without a versioned runtime profile activation
behavior_keys: display_top_n, runtime_profile, runtime_profile.*, st_pit_authoritative, top_k
path: strategy_package_selection.run_selection
```

该错误不是策略包资产问题，也不是用户选择错误，而是遗留的运行配置版本化门禁。现有实现把普通选股参数 `top_k`、`display_top_n`、`runtime_profile`、`st_pit_authoritative` 等当成“会改变交易行为的配置”，并强制要求用户或 UI 先激活 versioned runtime profile。这个逻辑在模拟盘和选股主路径上是错误的：

1. 用户只是运行 Selection/Paper，不是在申请未来实盘。
2. UI 本来就会发送 `top_k`、HMM、行业黑名单、停牌剔除等每日运行参数。
3. 系统完全可以自动生成 effective runtime config 与 hash 做审计，不需要把“版本化激活”变成用户前置门禁。
4. 当前检查发生在默认 binding 自动补齐之前，因此即使后续代码能生成 platform default binding，也已经被提前 400 阻断。
5. 错误使用 `STRATEGY_PACKAGE_VALIDATION_ERROR`，导致平台 runtime 配置问题被误报为策略包不可用。

结论：`runtime_profile_activation` 是本轮必须一次性清理的残留门禁。它只能保留在未来实盘 LiveApproval 或显式治理审计路径，不能出现在 Selection、AIstock 模拟盘、MiniQMT 模拟盘准入和每日运行主路径。

## 2. 设计原则：这是模拟盘和选股功能，不是门禁系统

目标业务路径只有一条：

```text
QE 生成策略包
  -> 策略包资产检查合格
  -> 选股
  -> AIstock LocalSim 模拟盘 / MiniQMT SIM 模拟盘
  -> 未来实盘另走 LiveApproval
```

必须坚持四层边界：

| 层级 | 允许硬阻断什么 | 不允许硬阻断什么 | 输出 |
|---|---|---|---|
| StrategyPackage Admission | 包记录不存在、manifest/hash 漂移、核心模型/因子/score artifact 不可加载、包已删除/隔离/退役、来源不是受认可 QE/回测 | HMM、TopK、行业黑名单、停牌剔除、交易日、PIT、TDX、DB、MiniQMT、broker、执行算法、runtime profile activation | `AssetEligibilityResult` |
| Runtime Resolution | 请求形状非法到无法解析，例如 `top_k` 非数字、trade_date 格式错误、启用 HMM 但没有选择模型配置 | 历史日期需要上一交易日 cutoff、当日价格需要 TDX/pre_close、HMM 当日系数未缓存、交易日历/行情源 runtime warning | `RuntimeResolutionPlan` + warning/弹窗 |
| Run / Session Execution | 本次 run/session 真实缺数据、broker 不可用、MiniQMT 拒单、分钟线/昨收/涨跌停/停牌数据缺失、HMM 自动计算失败 | 改变策略包资格、禁用策略包、要求手工生成 snapshot 或手工激活 runtime profile | 成功 evidence 或本次 run/session error |
| Future Live Approval | 未来实盘需要模拟盘证据、人工审批、MiniQMT live 明确授权 | QE 包直接进实盘、把 Paper 状态伪装成 live approval | `LiveApproval` |

任何 Selection/Paper/MiniQMT 主路径返回“策略包不可用”的唯一原因必须来自 StrategyPackage Admission。平台运行能力失败只能是本次 run/session 的失败或 warning。

## 3. 必须清理的残留门禁清单

### 3.1 Runtime profile activation 门禁

必须从 Selection/Paper 模拟盘主路径移除以下硬门禁：

| 现有位置 | 当前问题 | 目标处理 |
|---|---|---|
| `backend/services/simulation_runtime/selection.py` 的 `ensure_runtime_config_version_boundary(...)` | UI 正常发送 `top_k/display_top_n/runtime_profile/st_pit_authoritative` 就被阻断 | Selection 主路径删除该调用；改为自动生成 effective runtime config hash |
| `backend/services/selection_center/runtime_profile.py` 的 `BEHAVIOR_CHANGING_RUNTIME_CONFIG_KEYS` | 把普通平台运行参数定义成必须 versioned activation | 不再用于 Selection/Paper 模拟盘 gate；仅未来 live/governance strict path 可使用 |
| `runtime_profile.py` 中 `platform default runtime profile cannot bind caller-provided behavior-changing runtime_config` | 即使平台可补默认 binding，也会拒绝 caller-provided runtime 参数 | Selection/Paper 改用 generated/audit binding；不拒绝普通 runtime 参数 |
| `backend/services/paper_trading_v2/service.py` 的 active runtime profile activation 要求 | 创建或运行 Paper session 时强制事先激活 runtime profile | active profile 变成可选复现工具；没有 activation 时使用组合默认 + 本次 runtime 参数生成 effective config |
| `backend/services/paper_trading_v2/session.py` 的 `validate_runtime_profile_binding(...)` | session create 仍会验证 binding 门禁 | 改为校验 effective config 可解析；binding/hash 自动生成并写入 session evidence |
| `daily selection evidence` 要求 `runtime_profile_binding` | 缺 binding 会让 evidence 生成失败 | evidence 层自动写入 `source=generated_effective_runtime_config`、`config_sha256`、`resolved_at` |

禁止再出现的用户可见错误：

```text
runtime_config changes trading behavior without a versioned runtime profile activation
runtime_config changes trading behavior without an active runtime profile activation
platform default runtime profile cannot bind caller-provided behavior-changing runtime_config
runtime_config requires runtime_profile_binding for daily selection evidence
```

### 3.2 Package health / selection_health 门禁

`selection_health`、HMM 覆盖、ST PIT runtime profile、数据源 ready、broker ready 只能是诊断信息，不能禁用策略包 checkbox、运行按钮或 portfolio 创建按钮。

目标处理：

1. 后端保留 `selection_health` 作为 `runtime_diagnostics` 或 `warnings`，不得影响 selectable package 返回和运行入口。
2. 前端删除 `packageHealthRunnable()` / `selectedPackageBlocked` 等 disabled 逻辑。
3. 如果运行时真的失败，必须创建本次 run/session 失败记录，并返回可复制诊断，而不是提前让用户无法点击。

### 3.3 Paper 状态、paper_ready、paper_enabled、paper_candidate 门禁

继续执行 2026-05-25 主设计中的清理要求，并补充 L0 扫描门禁：

| 门禁 | 处理 |
|---|---|
| `PAPER_ENABLED/PAPER_RUNNING/PAPER_PASSED/PAPER_FAILED` | 不再作为策略包准入、可见性、按钮启用条件；仅允许 legacy/migration/display history |
| `SELECTION_ENABLED` | 不再作为选股准入；资产合格即可选股 |
| `paper_ready` | 仅未来 live/governance read-only；Paper/Selection/MiniQMT 不得依赖 |
| `paper_enabled execution policy` | 不再是模拟盘准入；执行策略是平台运行配置，可缺省或在本次 run/session 中校验 |
| `paper_candidate runtime variant` | 不再是模拟盘准入；runtime variant 只能作为可选配置模板 |

### 3.4 PIT 截止日 / 交易日校验门禁

PIT 截止日和交易日解析必须由平台自动完成，不能要求用户手工计算、手工生成 snapshot，也不能作为策略包门禁。

目标行为：

| 场景 | 自动处理 | UI 展示 | 是否阻断 |
|---|---|---|---|
| 当前日期选股，且当前日期是交易日 | 使用交易日服务确认日期；价格从 TDX 最新 quote 获取，若 `current_price <= 0` 且 `pre_close > 0`，用 `pre_close` 作为 `selection_entry_price`，source=`TDX latest close / pre_close` | 显示“使用当前 TDX 最新价/昨收作为入池参考价，当前价另列显示” | 不阻断；TDX 不可用时本次 run 失败为行情错误 |
| 历史交易日 `D` 选股 | 自动解析 `cutoff_date=previous_trading_day(D)`；因子和 PIT 数据只使用 `cutoff_date` 及以前数据；入池价使用 `cutoff_date` 收盘价 | 页面弹窗或 inline banner：“你选择 D，系统将按上一交易日 cutoff 计算，入池价为 cutoff 收盘价” | 不阻断；用户确认提示即可继续 |
| 用户选择非交易日 `D` | 自动解析最近已结束交易日 `T<=D` 和 `cutoff=previous_trading_day(T)`，或提示切换到 `T` | 弹窗说明非交易日映射规则，允许继续或改日期 | 不作为包门禁；交易日历无法覆盖才是平台运行错误 |
| 交易日历缓存缺失或数据库不覆盖 | 官方交易日服务返回 `TRADING_CALENDAR_UNAVAILABLE`，并提示更新本地交易日表/缓存 | 首页和运行页面显示明确警告 | 本次 run/session 失败，不改变策略包资格 |

UI 提示必须是解释性确认，不是死门禁。用户不需要输入任何 ID 或手工计算 cutoff。

## 4. 目标实现方案

### 4.1 后端：运行配置解析改为“自动解析 + 审计 hash”

新增或统一一个内部结构，供 Selection 和 Paper v2 共用：

```text
RuntimeResolutionPlan
  request_trade_date
  effective_trade_date
  cutoff_date
  calendar_source
  runtime_profile
  runtime_profile_source: ui | portfolio_default | model_default | generated
  runtime_config_sha256
  warnings[]
  user_notice[]
```

关键规则：

1. Selection/Paper 主路径不再要求 `runtime_profile_activation`、`runtime_profile_binding.profile_version_id` 或用户手工 profile version。
2. 每次运行都生成 `effective_runtime_config` 和 `runtime_config_sha256`，写入 selection evidence、paper session、run event、order target 或 diagnostic payload。
3. 如果存在用户主动激活的 runtime profile version，可以作为 source 输入；但缺失时必须走 platform generated config，不得失败。
4. `runtime_profile_config_sha256(...)` 继续用于审计和复现，但不再作为准入门禁。
5. `top_k/display_top_n/st_pit_authoritative/runtime_profile` 属于本次运行参数；非法值返回 `RUNTIME_CONFIG_INVALID`，合法值不需要 versioned activation。

### 4.2 Selection Center 主路径

目标执行顺序：

```text
validate request shape
  -> require StrategyPackageAssetEligibility only
  -> resolve selection date plan and PIT cutoff automatically
  -> normalize runtime config with platform defaults
  -> resolve HMM cache or schedule compute when enabled
  -> generate effective runtime config hash
  -> run/generate artifact/selection evidence
  -> persist run result or runtime failure
```

必须删除/降级：

1. `ensure_runtime_config_version_boundary` 在 Selection 主路径中的调用。
2. `validate_runtime_profile_binding` 对普通 Selection request 的硬阻断。
3. `runtime_profile_binding_for_evidence` 缺 binding 即失败的逻辑。
4. `SelectionPackageHealthService.require_runnable()` 或等价 runnable 判断对运行入口的影响。
5. `StrategyPackageValidationError` 对 runtime config、TDX、DB、HMM、PIT 等平台问题的误用。

保留：

1. package 不存在、manifest/hash/核心 artifact 不可用的资产硬检查。
2. `top_k` 格式和范围等请求合法性检查，但错误码必须是 `RUNTIME_CONFIG_INVALID` 或 `REQUEST_INVALID`，不是 package gate。
3. 运行中缺数据 fail-fast，并记录到 run。

### 4.3 Paper v2 / AIstock LocalSim 主路径

目标执行顺序：

```text
create portfolio from asset eligible package
  -> freeze package_id / manifest_hash / initial capital / start_date / broker role / fee / risk / execution strategy snapshot
  -> portfolio status READY
  -> session/run 时 resolve effective runtime config
  -> selection/target/order/fill/ledger/snapshot
  -> failure belongs to session/run, not package
```

必须删除/降级：

1. 创建 portfolio 不再检查 `paper_ready`、`paper_enabled`、`paper_candidate`、active runtime profile activation。
2. session create 不再要求 `freeze_runtime_profile` 触发 versioned binding gate；`freeze` 只表示把本次 effective runtime config 写入 session evidence。
3. 盘中启动/恢复不再被交易时段限制阻断；真实下单安全由交易日历、行情、停牌、涨跌停、执行策略和 broker runtime check 处理。
4. LocalSim 可以因为分钟线、昨收、涨跌停、停牌数据缺失使本次 session fail-fast，但不能禁用 portfolio 或策略包。

### 4.4 MiniQMT SIM 主路径

目标执行顺序：

```text
asset eligible package
  -> create MiniQMT SIM binding/portfolio
  -> runtime selection/target
  -> submit order intent to MiniQMT when broker available
  -> reconcile MiniQMT facts
```

必须保证：

1. MiniQMT 模拟盘不通过 TDX/DB/LocalSim 补撮合或伪造成交。
2. MiniQMT 连接、柜台状态、可卖数量、撤单/成交异常只影响 broker runtime 或本次订单状态，不影响策略包资格。
3. 盘中 MiniQMT 恢复登录后可以继续启动/恢复策略；不能因为上午无法登录导致全天策略入口不可用。

### 4.5 HMM 每日系数

目标规则：

1. 用户只选择 HMM 模型配置和 preset。
2. 平台按 `model_config_id + signal_preset + trade_date/cutoff_date + input_data_version` 自动查缓存。
3. 当日无缓存时自动计算；同一交易日后续 Selection/Paper/MiniQMT 复用缓存。
4. 计算失败是本次 run/session 的 `HMM_RUNTIME_UNAVAILABLE` 或 `ARTIFACT_GENERATION_FAILED`，不改变策略包资格。
5. UI 不再要求手工生成每日 snapshot，不展示“必须先选择已有快照”的流程。
6. HMM/模型在线重训、滚动训练、每日系数生成使用普通弹窗二次确认，不要求输入长 ID 字符串。

## 5. UI 同步整改

### 5.1 Selection 页面

必须实现：

1. 所有资产合格策略包可见、可分页、可搜索、可排序。
2. checkbox 和运行按钮只因“未选择包、trade_date 空、top_k 非法、请求正在提交”禁用。
3. `selection_health`、HMM、PIT、数据源、broker 只显示 warning，不禁用。
4. 历史日期选择后弹窗/提示显示自动 PIT cutoff 和入池价规则；用户确认后继续运行。
5. 当前日期显示 TDX 价格来源；`selection_entry_price` 与 `current_price` 分列。
6. 选股失败展示中文摘要 + 可复制诊断文本；不把 runtime failure 显示成“策略包验证失败”。
7. 选股历史支持分页、checkbox、多选删除；删除后清除 selection run/results/exclusions/link DB 记录，watchlist 不受影响。

### 5.2 Paper v2 portfolio / run console

必须实现：

1. 从策略包生成模拟盘表单完整显示，删除大块说明性文字，不让说明卡片挤占主操作区。
2. portfolio 创建成功只显示 `READY/已创建`；没有 run/session evidence 不得显示“运行成功”。
3. 组合列表分页、搜索、筛选、checkbox、多选退役和删除。
4. 退役是逻辑归档：默认隐藏、不再调度、不创建新 session，历史账本只读保留。
5. 删除是物理清理本地历史模拟盘/账本记录：必须二次弹窗确认并先做依赖检查；不要求输入长 portfolio_id。
6. run console 错误使用中文摘要和一键复制诊断文本，不使用多层表格/抽屉/JsonPanel 作为普通用户主视图。

### 5.3 模型与 HMM 页面

必须实现：

1. 策略模型重训、策略模型滚动训练、HMM 滚动训练纵向排列，确保按钮和说明完整可见。
2. 删除主视图“可用快照”长列表；改为显示每个可用 HMM 模型的最新缓存交易日和最新评分日期。
3. 当前模型状态主标题显示策略包中文名或可读名；package_id 仅作为下方小字或复制诊断字段。
4. HMM 自动缓存状态显示“已缓存/将自动计算/最近错误”，不再要求用户手工生成每日系数。

## 6. 错误分类

必须把错误从“策略包验证失败”改为可诊断分类：

| 错误码 | 含义 | 是否影响策略包资格 |
|---|---|---|
| `PACKAGE_ASSET_INVALID` | manifest/hash/core artifact/source QE/backtest 不合格 | 是 |
| `RUNTIME_CONFIG_INVALID` | top_k、trade_date、HMM 模型配置等请求参数无法解析 | 否 |
| `TRADING_CALENDAR_UNAVAILABLE` | 交易日服务或缓存无法覆盖请求日期 | 否 |
| `MARKET_DATA_UNAVAILABLE` | TDX/DB/minute/pre_close/limit/suspend 数据缺失 | 否 |
| `HMM_RUNTIME_UNAVAILABLE` | HMM 自动计算或缓存读取失败 | 否 |
| `BROKER_UNAVAILABLE` | MiniQMT/券商连接或柜台不可用 | 否 |
| `ARTIFACT_GENERATION_FAILED` | selection artifact、target、evidence 生成失败 | 否 |
| `LIVE_APPROVAL_REQUIRED` | 未来实盘缺少审批或授权 | 不适用于模拟盘 |

`STRATEGY_PACKAGE_VALIDATION_ERROR` 只能用于 `PACKAGE_ASSET_INVALID` 对应的资产硬检查，不得承载平台 runtime 问题。

## 7. 为什么之前修了十几个小时仍然遗留门禁

之前整改主要覆盖了显式策略包状态门禁，例如 `PAPER_ENABLED/PAPER_RUNNING/PAPER_PASSED`、`paper_ready`、`paper_enabled policy`、`paper_candidate variant` 和部分 UI health gate。但本次失败来自另一套“运行配置版本化治理”逻辑：

1. 它不是以 Paper 状态或 package health 名义出现，而是通过 `ensure_runtime_config_version_boundary` 和 `BEHAVIOR_CHANGING_RUNTIME_CONFIG_KEYS` 出现。
2. 它把 UI 正常发送的每日运行参数当作需要 versioned activation 的交易行为变更。
3. 它在默认 binding 自动补齐之前执行，因此后续补齐逻辑无法救回请求。
4. 它用 `STRATEGY_PACKAGE_VALIDATION_ERROR` 抛出，掩盖了“平台 runtime gate 残留”的真实性质。
5. 旧设计未把 `runtime_profile_activation` 明确列为必须清除的主路径门禁，也没有把 PIT/交易日解析明确降级为 UI 提示和自动处理。

因此，本补充设计把它列为 P0 清理项，并增加 grep 验证，避免再次遗漏。

## 8. 实施阶段

### Phase 0：设计冻结与基线扫描

- 合入本补充设计。
- 扫描 Selection/Paper/MiniQMT 主路径中所有 `ensure_runtime_config_version_boundary`、`runtime_profile_activation`、`runtime_config changes trading behavior`、`PAPER_*`、`paper_ready`、`paper_enabled`、`paper_candidate`、`packageHealthRunnable`、`selectedPackageBlocked` 命中。
- 输出“删除 / 降级 warning / 保留但仅 live path”的逐项矩阵。

### Phase 1：后端 runtime activation gate 删除

- Selection 主路径删除 versioned runtime profile activation 要求。
- Paper session/portfolio 解析路径删除 active runtime profile activation 要求。
- Evidence 自动生成 `generated_effective_runtime_config` binding/hash。
- 错误码从 package validation 拆分为 runtime config/data/broker/HMM/artifact。

### Phase 2：PIT/交易日自动解析

- 接入官方交易日服务和文件缓存。
- Selection/Paper 共用 `SelectionDatePlan` / `RuntimeResolutionPlan`。
- 历史日期自动 previous trading day cutoff。
- 非交易日和历史 cutoff 在 UI 弹窗/提示中解释，不作为门禁。

### Phase 3：HMM 自动计算和缓存

- Selection/Paper/MiniQMT 共用 HMM runtime cache resolver。
- 无当日缓存自动计算；同日复用。
- UI 删除手工 snapshot 依赖。

### Phase 4：UI gate 与错误展示清理

- Selection、Paper portfolio、run console、MiniQMT、模型与 HMM 页面同步删除 disabled gate 和 dead-end 文案。
- 新增中文错误摘要 + 可复制诊断文本。
- 添加分页、批量删除/退役、假成功防护。

### Phase 5：验证和设计合规复核

- 执行 L0-L4 自动验证。
- L5 Real MiniQMT SIM 作为受环境影响的手工验证，不阻断 AIstock LocalSim 合入，但必须记录实际状态。
- 按 `DESIGN-COMPLIANCE-001` 逐项生成矩阵，不允许交付子集版或只清理单个错误字符串。

## 9. 验证矩阵

| 层级 | 用例 | 通过标准 |
|---|---|---|
| L0 grep | `rg -n "runtime_config changes trading behavior|versioned runtime profile activation|active runtime profile activation" backend/services/selection_center backend/services/simulation_runtime backend/services/paper_trading_v2 frontend/src/app/paper-v2 -S` | Selection/Paper/MiniQMT 主路径无命中；仅未来 live/governance strict path 或测试说明可命中 |
| L0 grep | `rg -n "ensure_runtime_config_version_boundary" backend/services/selection_center backend/services/simulation_runtime backend/services/paper_trading_v2 -S` | 不在 Selection/Paper 模拟盘主路径调用 |
| L0 grep | `rg -n "PAPER_ENABLED|PAPER_RUNNING|PAPER_PASSED|PAPER_FAILED|paper_ready|paper_enabled|paper_candidate" backend/services frontend/src/app/paper-v2 -S` | 不作为业务判断、可见性、按钮禁用和准入条件 |
| L0 grep | `rg -n "packageHealthRunnable|selectedPackageBlocked|Required paper-enabled|策略包健康预检阻断|JsonPanel" frontend/src/app/paper-v2 -S` | Paper v2 主 UI 不再存在 dead-end gate 或错误主视图 |
| L1 unit | runtime config without activation | `top_k/display_top_n/st_pit_authoritative/runtime_profile` 合法请求不抛 activation gate |
| L1 unit | PIT cutoff | 选择 `2026-05-13` 时自动使用 `2026-05-12` 作为 cutoff 和历史入池价日期 |
| L1 unit | non-trading date plan | 非交易日自动映射到最近已结束交易日并生成 warning/notice，不返回 package error |
| L1 unit | HMM cache resolver | 首次自动计算并写缓存；同日第二次命中缓存；无手工 snapshot |
| L2 API | current day selection | `pkg_378eb9c91e104c64935404e257e932ee` + `2026-05-26` + UI 等价 runtime_config 不返回 `STRATEGY_PACKAGE_VALIDATION_ERROR` |
| L2 API | runtime data missing negative | 缺分钟线/昨收/TDX/HMM 时返回 runtime/data/HMM 错误，并记录 run/session 失败，不改变包资格 |
| L2 API | asset invalid negative | manifest/hash/core artifact 缺失时返回 `PACKAGE_ASSET_INVALID` |
| L2 API | Paper create | 资产合格包可创建 AIstock LocalSim portfolio，状态为 `READY`，不要求 paper_ready/paper_enabled/runtime activation |
| L2 API | Paper day run | LocalSim 可在盘中启动；真实缺数据只失败本次 session |
| L2 API | MiniQMT fake broker | 资产合格包可创建 MiniQMT SIM binding；broker unavailable 是 broker runtime error |
| L3 UI | Selection page | 资产合格包均可见可选；runtime warning 不禁用 checkbox/运行按钮 |
| L3 UI | PIT notice | 历史日期选择显示 cutoff 弹窗/提示，确认后继续运行 |
| L3 UI | Error digest | activation gate 不再出现；错误主视图是中文摘要 + 复制诊断文本 |
| L3 UI | Portfolio list | 分页、搜索、批量退役、批量删除可用；退役和删除含义清晰区分 |
| L3 UI | Model/HMM page | 无手工快照依赖；显示最新缓存日期；训练操作弹窗确认 |
| L4 E2E | Selection -> watchlist -> LocalSim | 选股、加入自选、创建 portfolio、运行 session、ledger/snapshot evidence 形成闭环 |
| L4 E2E | Historical selection price | 历史 D 选股使用 cutoff close 作为入池价，当前价单独显示且不写入 watchlist 入池价 |
| Compliance | DESIGN-COMPLIANCE-001 | 每一项设计要求都有实现引用、测试证据、状态和缺口说明 |

## 10. 验收标准

本项目实现完成后，必须同时满足：

1. 资产合格策略包无需任何 `enable-selection`、`enable-paper`、`paper_ready`、`paper_enabled`、`paper_candidate`、runtime profile activation，即可进入 Selection、AIstock LocalSim、MiniQMT SIM。
2. 当前 2026-05-26 暴露的 `runtime_config changes trading behavior without a versioned runtime profile activation` 不再可能阻断选股。
3. 历史日期选股自动使用上一交易日数据；该规则通过 UI 弹窗/提示解释，不是门禁。
4. 当前日期选股价格从 TDX 获取；盘前/无 current_price 但有 pre_close 时使用 pre_close 作为 `selection_entry_price`，source 标记为 `TDX latest close / pre_close`。
5. HMM 每日系数平台自动计算和缓存；同日复用；不要求手工 snapshot。
6. Selection/Paper/MiniQMT UI 不存在残留 disabled gate、dead-end gate 文案或多层表格/抽屉型错误主视图。
7. 平台运行能力失败只影响本次 run/session，并用 runtime/data/broker/HMM/artifact 错误码表达；不再污染 StrategyPackage 资格。
8. 未来实盘 gate 独立保留为 LiveApproval，不与模拟盘准入混用。
9. 验证矩阵 L0-L4 通过；L5 MiniQMT 真实柜台验证如果受外部环境影响，必须记录为环境状态，不影响 LocalSim 和 Selection 功能验收。
10. 合入前必须提交 DESIGN-COMPLIANCE-001 矩阵，证明不是只修一个报错字符串，而是一次性清理所有不合理门禁。
