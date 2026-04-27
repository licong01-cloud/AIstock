# Paper Trading v2 缺口收敛与运行配置审计详细设计

> 日期：2026-04-27  
> 状态：详细设计 / 下一阶段开发依据  
> 范围：StrategyPackage 语义收敛、Selection Center、Paper Trading v2 历史回放/实时模拟、V25 分钟执行策略接入、UI 与后端一致性验证。  
> 不包含：QMT、Shadow、实盘真实下单、旧 QE/RD-Agent/旧 paper_trading 主链路改造、`backend/data_service` 语义变更、日频模拟盘 fallback。

## 1. 结论

当前仓库已经有 Paper Trading v2 的完整方案体系，但分散在多份文档中，并且最新业务决策要求重新定义 `StrategyPackage` 与 Paper v2 运行配置之间的边界。

已有方案来源：

- `docs/architecture/paper_trading_v2_top_level_design.md`：顶层闭环和主流程；
- `docs/architecture/paper_trading_v2_remaining_execution_plan.md`：早期剩余开发计划；
- `docs/architecture/paper_trading_v2_runtime_profile_execution_policy_design.md`：运行配置、HMM、行业黑名单、validated execution policy、重放 reset；
- `docs/architecture/paper_trading_v2_realtime_replay_session_design.md`：`REPLAY_ONLY`、`LIVE_ONLY`、`CATCHUP_THEN_LIVE` 会话设计；
- `docs/architecture/paper_trading_v2_ui_design.md`：Paper v2 独立 UI 设计；
- `docs/architecture/paper_trading_v2_ui_e2e_validation_plan.md`：UI 全流程验证计划；
- `docs/architecture/minute_execution_algo_standard_contract.md`：日内分钟线执行策略标准代码规范；
- `docs/architecture/qe_v25_minute_execution_regression_audit_20260427.md`：V25 回归审计和市场状态处理边界。

本文件作为下一阶段开发的统一缺口收敛设计：

1. `StrategyPackage` 冻结“因子集合 + 模型族/模型资产谱系 + QE 来源证明”，不再把 Paper v2 可变运行选项锁死在 manifest 中。
2. Paper v2 的日频选股/调仓策略、日内分钟线执行策略、HMM、行业黑名单、是否在信号阶段过滤已确认停牌、TopK、回放/实时模式等，均作为可变运行配置管理。
3. 所有会影响选股、订单、成交、现金、持仓、NAV、绩效归因的配置变更必须留痕、版本化、按交易日激活，并复制到 selection run / paper run / trade session 的运行快照中。
4. 运行配置可变不等于随意可变：分钟线执行策略必须来自回测验证过的 `validated_execution_policy`；UI 只能展示后端已支持且已验证的选项。
5. 开发阶段只允许修改 Paper v2/Selection/StrategyPackage 框架代码和迁移脚本，不得静默修改任何持久化资产或训练资产。

## 2. 已实现基线与关键缺口

### 2.1 已实现基线

- `strategy_pkg.package` 保存 frozen manifest JSON 与 `manifest_sha256`；status 在 hash 外流转。
- `strategy_pkg.selection_score_artifact` 已用于权威选股分数产物；Selection Center 不接受 QE 回测 `pred.pkl` 作为实盘/模拟盘选股输入。
- `selection.run`、`selection.package_result`、`selection.aggregate_result`、`selection.excluded_result` 已支持单策略包与多策略包选股结果持久化。
- Selection Center 支持 `single_package`、`intersection`、`union`、`weighted_fusion`；多策略包进入 Paper v2 仍按设计禁用。
- `paper_v2.portfolio`、`paper_v2.run`、orders、fills、cash ledger、positions、daily snapshots、errors、run events 已存在持久化路径。
- `paper_v2.trade_session`、`session_day`、`order_execution_state`、`intraday_snapshots`、`session_events` 已支持会话化回放/实时模拟框架。
- `V25_TWO_STAGE` 已声明历史回放和实时流式能力：历史回放要求 240 根完整分钟线，实时模式允许从 1 根已观察分钟线开始并持久化 240 步计划。
- `suspend_d` 已作为停牌权威数据源之一用于信号阶段过滤和交易阶段不可交易状态判断。
- Paper v2 UI 已有 `/paper-v2` 独立入口，覆盖策略包、选股、组合、运行控制台、账本、绩效、模型/HMM 等页面。

### 2.2 仍需收敛的缺口

1. 早期契约要求 `minute_execution_policy` 是策略包必填和固定内容；最新业务要求策略包只冻结因子与模型组合，日频策略、分钟执行策略、HMM、黑名单、停牌过滤等在 Paper v2 中可动态变更。
2. 运行配置已有 `runtime_config` JSON 快照，但还缺少专门的“运行配置版本 + 激活 + 变更审计”一等对象，无法系统比较不同配置对模拟盘收益的影响。
3. V25 能执行不代表 Paper v2 全链路已验证：仍需要确认每个可选 validated policy、`day_features` 提供器、DB/TDX 分钟线、停牌/涨跌停/pre_close/交易日历、账本绩效都在同一条 UI 流程中真实生效。
4. UI 已有大量控件，但必须继续做 UI/后端矩阵核对，避免出现“前端可选、后端不支持”或“后端已实现、UI 缺入口”。
5. 需要明确资产与程序框架分离：开发 Paper v2 框架时不得修改 QE 实验资产、模型权重、HMM 快照、selection artifact、validated policy、manifest hash 或任何 DB asset row。

## 3. StrategyPackage 语义重定义

### 3.1 新定义

`StrategyPackage` 是从 QE 单次实验或 QE 演进 Loop 产生的研究 alpha 资产。它应冻结：

- 来源：`source_type`、`source_id`、`loop_id`、`run_id`；
- 因子集合：因子 ID、因子定义引用、特征 schema、数据窗口要求；
- 模型族与模型资产谱系：模型类型、训练配置摘要、原始模型资产引用、特征顺序、推理入口、模型 hash；
- alpha 组合结构：单 alpha 当前为 `identity`，未来多 alpha 由 QE 多 alpha 稳定后接入；
- QE 回测指标快照：IC、RankIC、Sharpe、年化收益、最大回撤等展示和筛选指标；
- 初始 QE 回测时使用的运行配置快照，仅作为 lineage / 默认建议，不作为 Paper v2 不可变运行约束。

`StrategyPackage` 不应冻结为 Paper v2 固定运行约束的内容：

- 每次选股的 TopK；
- 是否在信号生成阶段过滤已确认停牌股票；
- 行业黑名单；
- HMM 是否启用、HMM 模型版本、HMM 快照、signal preset；
- 日频选股/调仓策略；
- 日内分钟线执行策略；
- 尾盘未成交处理方式；
- 历史回放、实时模拟、追赶后切实盘模拟模式；
- 数据源角色：`DB_HISTORICAL`、`TDX_REALTIME`；
- Paper v2 组合初始资金、手续费、风险参数等账户级配置。

这些可变项必须由 Paper v2 Runtime Profile、Validated Execution Policy、Execution Policy Activation、Trade Session 等对象管理。

### 3.2 Manifest v1 的兼容解释

早期 `docs/contracts/strategy_package_manifest_v1.md` 和 ADR 中写明 `minute_execution_policy` 是策略包必填。下一阶段不能直接修改历史 manifest 或重算 hash，应按以下兼容策略处理：

- 旧 manifest 中的 `minute_execution_policy` 保留为 `source_backtest_runtime_defaults.minute_execution_policy` 语义；
- 不再把旧 manifest 的 `minute_execution_policy` 直接当作 Paper v2 当前执行策略；
- 进入 Paper v2 前，必须把可用执行策略导入或生成 `strategy_pkg.validated_execution_policy`，并通过 `paper_enabled=true` 与 mode capability 检查后才可选择；
- `manifest_sha256` 不因 status、model freshness、runtime profile、execution policy activation、HMM/黑名单选择而改变；
- 如未来需要正式契约，应新增 `StrategyPackage Manifest v1.1` 或 `v2` 文档，把“冻结研究 alpha 资产”和“运行时可变配置”分开描述。

### 3.3 模型重训与策略包的关系

“因子和模型组合被冻结”不等于训练资产永远不能更新。建议语义为：

- 策略包冻结的是因子集合、模型族、特征 schema、推理合约和初始模型谱系；
- 重新训练或滚动训练产生新的 `model_version` / `model_state`，属于受保护资产变更，不能在框架开发时静默写入；
- Paper v2 每次 selection run / paper run 必须记录实际使用的 `active_model_version_id`、模型 hash、训练区间、训练完成时间、是否 stale；
- 模型 stale 不默认阻断模拟盘，但 UI 必须提示；用户人工触发重训并确认后，重训完成才能更新 active model state；
- 如果模型版本改变，同一策略包可继续作为同一个研究 alpha 家族，但绩效比较必须按模型版本和运行配置版本分组。

## 4. 可变运行配置与审计设计

### 4.1 核心原则

任何影响收益或风险结果的配置都必须满足：

1. 可验证：后端有明确 schema、校验和错误码；
2. 可追溯：配置 JSON 有 canonical hash；
3. 可激活：按 portfolio / trade_date / session 明确生效；
4. 可比较：每次 run 的绩效能关联到配置版本；
5. 不改历史：修改配置只影响未来 selection run / paper run / session，不能静默改写已有账本和快照；
6. 不改 manifest：运行配置变更不更新 `strategy_pkg.package.manifest_json` 和 `manifest_sha256`。

### 4.2 Runtime Profile 建议结构

```json
{
  "profile_schema_version": "1.0",
  "selection": {
    "top_k": 20,
    "selection_strategy_code": "SCORE_TOPK_EQUAL_WEIGHT",
    "rebalance_policy_code": "TARGET_WEIGHT_REBALANCE"
  },
  "tradability": {
    "exclude_suspended": true,
    "suspend_dataset": "market.suspend_d",
    "suspend_readiness_required": true
  },
  "industry_blacklist": [],
  "hmm": {
    "enabled": false,
    "model_config_id": null,
    "model_snapshot_id": null,
    "signal_preset": null,
    "coefficients_sha256": null
  },
  "model": {
    "active_model_version_id": null,
    "allow_stale_model": true
  },
  "risk": {
    "risk_policy_id": null
  },
  "metadata": {
    "created_from": "paper_v2_ui",
    "reason": "每日开盘前配置"
  }
}
```

规则：

- `top_k` 默认 20，UI 可配置上限 50；后端也必须校验 1-50。
- `exclude_suspended=true` 时必须检查 `suspend_d` 数据审计，不允许缺数据时把股票当作可交易。
- 行业黑名单启用时必须有 PIT 行业映射，缺映射 fail-fast。
- HMM 启用时必须选择后端可验证的 config + completed snapshot + signal preset；缺系数、缺行业映射、缺交易日系数均 fail-fast。
- `selection_strategy_code` 和 `rebalance_policy_code` 不能成为 UI 自由文本；必须来自后端注册表，并标记是否已经回测验证。
- `model.allow_stale_model=true` 只表示允许继续跑，不表示模型新鲜；UI 和 run context 必须显示 stale warning。

### 4.3 建议新增/补充表

当前 `selection.run.runtime_config`、`paper_v2.run.runtime_config`、`paper_v2.trade_session.runtime_config_json` 已能保存快照，但不足以管理“变更历史”和“按日激活”。建议补充：

#### `paper_v2.runtime_profile`

```text
profile_id text primary key
portfolio_id text not null references paper_v2.portfolio(portfolio_id)
package_id text not null
profile_name text not null
status text not null -- DRAFT | ACTIVE | RETIRED
current_version_id text null
created_by text null
created_at timestamptz not null
updated_at timestamptz not null
```

#### `paper_v2.runtime_profile_version`

```text
profile_version_id text primary key
profile_id text not null references paper_v2.runtime_profile(profile_id)
version_no integer not null
config_json jsonb not null
config_sha256 text not null
validation_status text not null -- VALIDATED | INVALID
validation_errors jsonb null
created_by text null
reason text null
created_at timestamptz not null
supersedes_version_id text null
unique(profile_id, version_no)
unique(profile_id, config_sha256)
```

#### `paper_v2.runtime_config_activation`

```text
activation_id text primary key
portfolio_id text not null references paper_v2.portfolio(portfolio_id)
trade_date date not null
profile_version_id text not null references paper_v2.runtime_profile_version(profile_version_id)
status text not null -- ACTIVE | SUPERSEDED | CANCELLED
activated_at timestamptz not null
activated_by text null
reason text null
context jsonb not null default '{}'::jsonb
superseded_at timestamptz null
unique(portfolio_id, trade_date) where status = 'ACTIVE'
```

#### `paper_v2.config_change_audit`

```text
audit_id bigserial primary key
portfolio_id text null
package_id text null
object_type text not null -- runtime_profile | execution_policy_activation | model_state | reset | session
object_id text not null
change_type text not null -- CREATE | UPDATE | ACTIVATE | SUPERSEDE | RESET | RETIRE
before_json jsonb null
after_json jsonb null
before_sha256 text null
after_sha256 text null
reason text null
created_by text null
request_id text null
code_version text null
created_at timestamptz not null
```

### 4.4 与现有表的关系

- `strategy_pkg.package`：只保存 frozen manifest 和 status；不保存当前 Paper v2 运行配置。
- `strategy_pkg.validated_execution_policy`：保存回测验证过的分钟执行策略；它本身是受保护资产/证明，不因 Paper v2 激活而改变。
- `paper_v2.execution_policy_activation`：继续负责每个组合/交易日启用哪个 validated execution policy；建议也写入 `config_change_audit`。
- `selection.run.runtime_config`：复制当次选股使用的 runtime profile snapshot、profile_version_id、config_sha256。
- `paper_v2.run.runtime_config`：复制当日运行使用的 runtime profile snapshot、validated execution policy snapshot、model state snapshot、data source roles。
- `paper_v2.trade_session.runtime_config_json`：复制会话启动时的配置；`CATCHUP_THEN_LIVE` 不得在切换阶段重新读取 UI 当前配置。

## 5. 可变选项逐项设计

| 选项 | 是否可变 | 生效边界 | 必须记录 | 禁止行为 |
| --- | --- | --- | --- | --- |
| TopK | 是 | selection run / paper run | `top_k`、profile hash、实际入选/补位记录 | 空结果伪装成功、超过 50 不报错 |
| 停牌过滤 | 是 | 信号阶段 + 交易阶段 | `exclude_suspended`、`suspend_d` audit date、排除列表 | 缺 suspend_d 审计时默认可交易 |
| 行业黑名单 | 是 | 信号阶段 | 黑名单、PIT 行业、排除原因、补位记录 | 缺行业映射时中性处理 |
| HMM | 是 | 信号阶段评分调整 | config、snapshot、preset、系数 hash、调整前后分数 | 缺系数时用 1.0 中性系数 |
| active model version | 是，受保护 | artifact generation / selection | model_version、train range、model hash、stale 状态 | 框架开发时静默重训或改权重 |
| 日频选股/调仓策略 | 是，但需注册/验证 | target position / rebalance | strategy code、版本、参数、验证状态 | UI 自由文本或 Paper-only 策略 |
| 日内分钟线执行策略 | 是，但必须 backtest validated | per trade_date / session | policy_id、policy_sha256、algo capability、policy JSON | 未验证策略、算法 fallback、日频 fallback |
| 尾盘未成交处理 | 暂不扩展 | validated execution policy 内 | backtest 证明和 policy hash | Paper v2 独有新选项 |
| 历史/实时/追赶模式 | 是 | trade session | mode、source roles、cursor、status/phase | DB/TDX 静默互相 fallback |
| reset/replay | 是，需确认 | portfolio/session | confirm_text、删除计数、audit | 默认覆盖旧账本 |

### 5.1 停牌过滤的业务语义

信号阶段应过滤“选股时已经确认停牌”的股票，因为这类股票无法形成当日可执行买入，保留在 TopK 中会降低模拟盘实用性。

交易阶段仍必须再次检查停牌、临停、涨跌停、pre_close、分钟线：

```text
信号阶段：过滤已知不可交易候选，并从后续排名补位。
交易阶段：处理信号生成后发生的停牌/临停/涨跌停，记录 NO_FILL / WAITING / REJECTED，不伪造成交。
```

### 5.2 补位规则

当 TopK 候选被停牌、行业黑名单或 HMM/数据规则排除时，必须从完整排序的后续股票中补位，直到达到目标 TopK 或完整 universe 用尽。

如果完整 universe 用尽仍不足：

- 不能返回普通成功；
- 必须建模为 `valid_no_candidate` 或 `partial_candidate`，并记录原因、过滤数量、剩余数量；
- 对用户当前业务假设“全市场排序下除非全停牌，否则通常不会补位不足”，实现上仍必须保留 fail-fast/trace。

## 6. V25 与 Paper v2 回放/实时闭环设计

### 6.1 能力声明

`V25_TWO_STAGE` 的正确含义：

```text
historical_replay_supported = true
historical_requires_full_day = true
historical_min_required_bars = 240
realtime_streaming_supported = true
live_min_start_bars = 1
live_step_mode = persisted_plan
plan_horizon_bars = 240
```

历史回放要求完整 240 根分钟线；实时模拟不能要求开盘前已有 240 根未来分钟线。

### 6.2 `day_features` 缺口

V25 需要 `market_context.day_features`。下一阶段必须实现或核对 `V25DayFeatureProvider`：

- DB historical replay：从与 QE V25 回测一致的数据源/特征构造逻辑生成交易日特征；
- TDX realtime：只能使用开盘前或当前时点已知的日特征，不能读未来分钟价格/成交量；
- 特征字段、顺序、归一化、缺失处理必须与 V25 训练/回测合约一致；
- 缺特征必须 fail-fast，禁止 `allow_default_day_features` 进入权威 Paper v2 流程。

本阶段的落地边界：`V25DayFeatureProvider` 是 Paper v2 框架的数据上下文提供器，不修改 V25 策略本体、不修改 QE/RDAgent 回测资产、不修改模型权重。提供器必须只读读取 DB 中已经审计的数据，并写入 `market_context`：

```text
day_features: length=10 finite float array
day_features_schema_version: paper_v2_v25_day_features_v2
day_features_source: db_pit_previous_trading_day
day_features_trade_date: <feature_date, strictly before trade_date>
day_features_fields: ordered field names
day_features_audit: dataset refresh audit summary
```

特征生成原则：

1. `feature_date` 必须来自 `market.trading_calendar` 中严格小于 `trade_date` 的最近交易日；缺交易日历直接失败。
2. 每个输入数据集必须在 `market.dataset_date_refresh_audit` 中存在对应 `feature_date` 的 `success` 审计记录；缺审计或失败审计直接失败。
3. 使用的数据必须是 `feature_date` 当日及更早的 PIT 数据；不得读取 `trade_date` 当日尚未完成的未来分钟线。
4. 字段缺失、非有限值、除数为零、指数/行业/资金流缺行均直接失败；禁止零填充或中性填充。
5. `require_day_features=false` 时非 V25 路径不强制读取 day_features；`V25_TWO_STAGE` readiness、run-day、live tick 必须显式传入 `require_day_features=true`。

首版 10 维字段采用稳定、可审计的 PIT 日频上下文：

| 序号 | 字段 | 来源 | 失败条件 |
| --- | --- | --- | --- |
| 1 | `stock_ret_1d` | `market.kline_daily_raw` 最近两交易日收盘 | 缺任一收盘或前收盘 <= 0 |
| 2 | `stock_intraday_ret` | `market.kline_daily_raw` | 缺 open/close 或 open <= 0 |
| 3 | `stock_hl_range` | `market.kline_daily_raw` | 缺 high/low 或 low <= 0 |
| 4 | `stock_volume_log1p` | `market.kline_daily_raw.volume_hand` | 缺成交量或负数 |
| 5 | `turnover_rate` | `market.daily_basic.turnover_rate` | 缺值或非有限 |
| 6 | `free_float_turnover_rate` | `market.daily_basic.turnover_rate_f` | 缺值或非有限 |
| 7 | `pb_log1p` | `market.daily_basic.pb` | 缺值或 pb <= -1 |
| 8 | `market_ret_1d` | `market.index_daily` benchmark pct_chg | 缺指数行或 pct_chg 非有限 |
| 9 | `sector_pct_change` | `market.sector_data.sw2_pct_change` | 缺行业映射/行业日数据 |
| 10 | `moneyflow_net_ratio` | `market.moneyflow_ts.net_mf_amount / kline amount` | 缺资金流或成交额 <= 0 |

如果后续 QE/V25 训练合约确认了不同字段或归一化，应作为新的 schema version 增量接入；不得静默改变 `paper_v2_v25_day_features_v2` 的字段顺序。`paper_v2_v25_day_features_v1` 因本地 `volume_ratio` 全为空，仅作为提交历史中的未启用草案，不进入权威运行。

### 6.3 WSL UNC 与资产访问隔离

禁止后端运行时从 Windows 侧直接拼接或访问 `\\wsl$`、`\\wsl.localhost` 等 WSL UNC 路径。原因：distro 名称、权限、WSL 服务状态会让 Paper v2/StrategyPackage 模型资产解析、手工因子验证、股票池生成出现不可预测失败。

强制规则：

- Windows 后端只能访问 Windows 文件系统内的显式缓存/导入目录，或通过 `wsl` 子进程让 WSL 主动把结果写入 Windows 临时输出目录。
- StrategyPackage 模型资产解析只能解析本地 Windows 路径、`/mnt/<drive>/...` 对应的 Windows 挂载路径，或已经显式导入到缓存的文件；不得尝试 `\\wsl$` 候选路径。
- 手工因子验证等必须读取 WSL 产物时，流程应为：Windows 创建临时目录 -> 转换为 `/mnt/<drive>/...` -> WSL `cp` 结果到该目录 -> Windows 读取临时目录。
- 股票池同步可通过 `wsl -d <distro> -- bash -lc` 在 WSL 内检查/校验文件，但不得让 Windows 后端用 UNC 读取 WSL 文件。
- 静态扫描必须覆盖 `\\wsl`、`wsl.localhost`、`wsl$`，运行时代码命中即视为阻断项。

### 6.4 资产与策略一致性

V25 逻辑版本应只有一份，物理存储可以有多个位置：

- 权威 core：`backend/execution_algos/v25_core.py` 或等价 core 文件；
- QE adapter：只做 Qlib 对象转换；
- Paper historical adapter：只做 DB/TDX 历史上下文转换；
- Paper realtime adapter：只做观察分钟线、持久化 plan/state、生成 step decision；
- 禁止在 Paper adapter 中改变 V25 权重、阶段比例、停牌/涨跌停语义。

任何修改 V25 core 或 adapter 都是全局解释器变更，必须单独列出影响范围和回归验证。

## 7. UI 与后端对齐要求

UI 不能只是流程页，必须是 Paper v2 权威操作入口。每个控件必须满足：

```text
UI 控件 -> API 字段 -> 后端校验 -> DB 快照/审计 -> 服务实际使用 -> 错误展示
```

### 7.1 页面级要求

| 页面 | 必须支持 | 后端来源 | 验证重点 |
| --- | --- | --- | --- |
| 策略包中心 | QE 单次实验 / 演进 Loop 下拉；显示实验名、年化、IC、RankIC、最大回撤；只显示未创建策略包来源 | `/strategy-packages/qe-sources` | 名称不能是无意义 ID；重复来源不可再创建 |
| 策略包详情 | status、manifest hash、metrics、model state、selection artifacts、validated policies | `/strategy-packages/*` | status 不进入 hash；policy 必须可验证 |
| 选股中心 | 单包、多包交集/并集/加权融合；TopK 20 默认/50 上限；HMM/黑名单/停牌过滤；历史记录点击展示结果；加入自选池 | `/selection-center/*`、`/hmm-training/*` | 不得报缺 artifact 后让用户无入口生成；加入自选要带参考价和来源 |
| 组合中心 | 选择单策略包创建模拟盘；初始资金；runtime profile；V25/其他 validated policy；历史回放/实时/追赶后实时 | `/paper-v2/portfolios`、`/sessions` | 多策略包 Paper 执行仍禁用；UI 要解释原因 |
| 运行控制台 | readiness、run-day、session create/tick/pause/resume/stop、scheduler、reset、policy activation、runtime profile activation | `/paper-v2/*` | UI 要读取 `session-capabilities` 禁用不支持模式 |
| 账本/绩效 | orders、fills、cash、positions、snapshots、run events、errors、performance | `/paper-v2/portfolios/{id}/*` | 绩效来自持久化账本，不是前端假算 |
| 模型/HMM | 模型 stale 提示、重训 preview/trigger、HMM config/snapshot 下拉 | `/strategy-packages/*/model-*`、`/hmm-training/*` | 重训必须人工确认，完成前不标记 current |

### 7.2 UI 不允许出现的行为

- UI 提交 `algo_code` 作为 Paper-only override；
- UI 展示未由后端返回、未 backtest-validated、未 mode-compatible 的分钟执行策略；
- UI 将后端 fail-fast 错误吞掉后显示成功 toast；
- UI 允许 `CATCHUP_THEN_LIVE` 但不提交明确 historical/live source roles；
- UI 的 HMM 下拉使用本地硬编码 snapshot；
- UI 没有显示 runtime profile version / config hash / active model stale warning；
- UI 提供多策略包聚合后直接创建 Paper v2 组合，除非未来有 `SelectionBundle` 或组合策略包合同。

## 8. 代码审核与静态扫描方案

下一阶段每次代码变更都必须先做两层验证的第一层：代码审核和扫描。

### 8.1 资产保护扫描

受保护资产包括：

- `strategy_pkg.package.manifest_json`、`manifest_sha256`；
- `strategy_pkg.validated_execution_policy` 已保存的 policy JSON/hash；
- QE workspace、RD-Agent workspace、`mlruns`、`pred.pkl`、训练输出；
- 模型权重文件、V24/V25 `.pt`、HMM snapshot/coefficients；
- `strategy_pkg.selection_score_artifact`；
- 任何代表持久化策略/模型/数据资产的 DB row。

建议命令：

```powershell
# 工作区差异只允许包含本任务文件
git status --short

# 不应出现资产目录写入差异
git diff --name-only | rg "(rdagent_assets|backend/data/hmm_models|mlruns|pred\.pkl|\.pt$|\.pth$|selection_score_artifact)"
```

若扫描命中资产路径，必须停止并写影响评估，不能把资产修改混入框架提交。

### 8.2 静默兜底扫描

建议每次功能完成后执行：

```powershell
rg -n "fallback|fall back|silent|except\s+.*pass|return\s+\[\]|ok\s*[:=]\s*true|default_price|default_cash|default_position|allow_default_day_features|CLOSE_PRICE|TWAP" backend/services backend/routers backend/execution_algos frontend/src/app/paper-v2 frontend/src/lib/paper-v2
```

命中不一定都是错误，但必须逐项判断：

- 文档或错误信息中的 `fallback` 可以存在；
- 权威交易路径中“缺数据改用默认值/空成功/其他算法”必须删除；
- `allow_default_day_features` 只能存在于显式诊断路径，不能被 Paper v2 UI 或正式 session 使用；
- `CLOSE_PRICE`、`TWAP` 只能作为 backtest-validated policy 出现，不能作为 V25 失败后的 fallback。

### 8.3 业务语义扫描

```powershell
rg -n "selection_scores|pred\.pkl|scores_path|manifest_json.*UPDATE|UPDATE\s+strategy_pkg\.package|data_source.*or|TDX_REALTIME.*DB_HISTORICAL|DB_HISTORICAL.*TDX_REALTIME" backend/services backend/routers
```

重点：

- Selection Center 不得重新接受回测 `pred.pkl`；
- 不得更新 manifest hash 来承载 runtime config；
- 不得在数据源之间使用 `or` 兜底；
- 不得把缺 selection artifact 变成空结果成功。

### 8.4 最低自动测试

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
```

涉及 UI 时还必须执行前端构建和 Playwright Paper v2 测试。

## 9. UI 后台全流程验证方案

验证必须使用临时端口，不得重启 8001：

```powershell
# 端口检查
Get-NetTCPConnection -LocalPort 8011,8012,3011,3012 -ErrorAction SilentlyContinue |
  Select-Object LocalAddress,LocalPort,State,OwningProcess

# 后端示例
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8012

# 前端示例
cd frontend
$env:PAPER_V2_API_BASE='http://127.0.0.1:8012/api/v1'
$env:PAPER_V2_API_PROXY_TARGET='http://127.0.0.1:8012'
$env:PAPER_V2_FRONTEND_PORT='3012'
npm run dev -- --port 3012
```

Playwright 后台测试：

```powershell
cd frontend
$env:PAPER_V2_API_BASE='http://127.0.0.1:8012/api/v1'
$env:PAPER_V2_API_PROXY_TARGET='http://127.0.0.1:8012'
$env:PAPER_V2_FRONTEND_PORT='3012'
npx playwright test --config=playwright.paper-v2.config.ts tests/paper-v2 --reporter=list
```

### 9.1 测试用例矩阵

| 编号 | 用例 | 操作 | 预期结果 |
| --- | --- | --- | --- |
| UI-01 | 策略包来源下拉 | 打开策略包中心，选择 QE experiment / evolution loop | 只显示未创建策略包来源；名称含 QE 实验名、年化、IC、RankIC、最大回撤 |
| UI-02 | 创建策略包 | 从 `qe_20260416_002701`、`qe_20260413_084216`、`qe_20260416_082012` 创建/查看策略包 | manifest hash 固定；status 可流转；metrics 与 model stale 显示 |
| UI-03 | selection artifact 生成 | 单包选股时选择 auto-generate live inference | 不再出现“missing artifact 且无生成入口”；生成失败必须显示结构化错误 |
| UI-04 | 单包选股 | TopK 默认 20，修改到 50；开启/关闭停牌过滤、行业黑名单、HMM | 结果数量、排除记录、补位记录与 runtime profile 一致 |
| UI-05 | 选股历史 | 点击已有 selection run | 展示当次 runtime config、package_id、manifest_sha256、trade_date、结果行、排除行 |
| UI-06 | 加入自选池 | 使用周五收盘/最新可得参考价加入 TopK | 自选项包含加入时间、来源策略包名称、reference_price、source run id |
| UI-07 | 多包动态聚合 | 勾选多个策略包或多个历史 run，执行 union/intersection/weighted_fusion | 按后端结果展示 trace；按钮状态与 mode/package 数一致 |
| UI-08 | 多包进 Paper 禁用 | 对多包聚合 run 尝试创建 Paper v2 | 后端返回 `UNSUPPORTED_FEATURE`，UI 显示原因，不显示成功 |
| UI-09 | 创建单包组合 | 选择 package、initial_cash、runtime profile、validated V25 policy、start mode | Portfolio 持久化成功；重要字段冻结；runtime profile 单独版本化 |
| UI-10 | session capability | 运行控制台加载 `session-capabilities` | UI 禁用不支持 mode/source/algo，显示 backend reason |
| UI-11 | 历史回放 | 选择最近 10 个已完成交易日，`REPLAY_ONLY`，`DB_HISTORICAL` | 产生真实 run/order/fill/cash/position/snapshot 或明确业务 no-fill；不得部分静默跳过 |
| UI-12 | reset 回放 | 对测试组合选择 `reset_portfolio` 并输入 portfolio_id | 删除计数写入 audit；旧账本被显式清理；确认文本错误时拒绝 |
| UI-13 | 追赶后实时 | 选择历史起点并勾选 auto switch to live | completed days 用 DB replay；当前日切换 TDX live；状态进入 live running/waiting |
| UI-14 | 实时等待 | 非交易时间或无新分钟线 tick | `LIVE_WAITING_FOR_BAR` / `LIVE_WAITING_NEXT_TRADING_DAY`，不是成功成交 |
| UI-15 | V25 实时首分钟 | 盘中有 1 根已完成分钟线时 tick | 不要求 240 根未来分钟线；生成/恢复 persisted plan；缺 day_features/model/torch fail-fast |
| UI-16 | 账本一致性 | 打开 ledger/performance | NAV、现金、持仓、成交与 DB 快照一致；亏损照实显示 |
| UI-17 | 配置变更审计 | 开盘前修改 HMM/黑名单/执行 policy 激活 | 新 version/audit 记录存在；旧 run 不变；新 run 引用新 hash |
| UI-18 | 负向数据缺失 | 人为选择缺 calendar/pre_close/limit/minute/HMM 系数的日期或包 | 后端结构化失败；UI 显示 error_code/message/context |
| UI-19 | 资产保护 | 测试前后执行 git/DB 只读审计 | 没有模型/manifest/policy/artifact 被静默修改 |
| UI-20 | 业务价值检查 | 回放后查看收益、胜率、回撤、交易明细 | 验证策略是否有真实收益贡献；不能只以流程跑通作为通过标准 |

### 9.2 验证通过标准

- 所有 UI 操作均通过真实后端 API；
- 每个 UI 选项都能在 DB 快照或 audit 中找到对应记录；
- 后端错误全部透传到 UI，不出现 `ok=true` 假成功；
- 回放/实时模式均使用分钟线，日频模式完全不可用；
- V25 不因开盘分钟线不足而失败，但会因缺模型/依赖/day_features/市场数据而明确失败；
- 账本、绩效和自选池数据来自持久化表，不来自前端临时计算；
- 测试前后受保护资产 hash/路径/DB 记录未被框架开发静默修改。

## 10. 下一阶段实施计划

### Phase 0：文档与基线审计

- 落地本文档；
- 记录当前已有方案和冲突点；
- 执行 `git status`，确认不提交无关脏文件；
- 不启动或重启 8001。

### Phase 1：StrategyPackage 语义收敛

- 增加 manifest v1 兼容解释代码注释/校验：旧 `minute_execution_policy` 是 backtest default，不是 Paper v2 当前执行策略；
- 若需新增 `manifest_schema_version=1.1`，只影响新包，不重写旧包 hash；
- API 返回中区分 `frozen_alpha_asset`、`source_backtest_defaults`、`paper_runtime_options`；
- 测试 status/model_state/runtime config 不改变 manifest hash。

### Phase 2：Runtime Profile 与审计表

- 新增 `paper_v2.runtime_profile`、`runtime_profile_version`、`runtime_config_activation`、`config_change_audit` 迁移；
- Repository/service/API 支持创建、验证、激活、查看历史；
- selection run、paper run、trade session 写入 profile version/hash；
- 测试配置变更只影响未来 run。

### Phase 3：UI/后端配置对齐

- 策略包中心来源下拉补齐未创建来源、展示 QE 指标；
- 选股中心补齐 artifact generate、历史详情、加入自选、聚合按钮状态；
- 组合中心补齐单包启动模拟盘、当前运行组合列表、runtime profile version 显示；
- 运行控制台接入 `session-capabilities`，禁用不支持按钮。

### Phase 4：V25 Paper v2 完整接入验证

- 实现/核对 `V25DayFeatureProvider`，禁止默认 day_features；
- 为测试包导入或选择已回测验证的 V25 `validated_execution_policy`，不修改原始 manifest/模型资产；
- 历史 replay、live tick、catchup tick 均通过 capability 与 source-role 校验；
- 小数据量测试覆盖停牌、涨跌停、缺 pre_close、缺 day_features、无新分钟线等待、重复 tick 不重复成交。

### Phase 5：完整 UI E2E 与业务价值验证

- 使用 8012/3012 启动测试服务；
- 执行 Playwright 后台测试；
- 用最近 10 个交易日回放验证 orders/fills/cash/positions/snapshots/performance；
- 明确输出每个策略包是否产生实际收益、收益来自哪些选股/成交、亏损也如实展示；
- 每修复一个问题后只提交相关文件，保证 GitHub main 可追溯。

## 11. 验收清单

开发完成后必须同时满足：

- `StrategyPackage` manifest hash 不包含 status 和 Paper v2 运行时可变配置；
- Paper v2 所有可变配置均有 version/hash/audit，并在 run/session 中复制快照；
- UI 可完成：创建/选择策略包 -> 生成最新选股 artifact -> 单包选股 -> 多包聚合研究 -> 加入自选 -> 创建单包 Paper 组合 -> readiness -> replay/live/catchup session -> 查看账本绩效；
- 多包聚合直接 Paper 执行继续 fail-fast 禁用，直到有 SelectionBundle 或组合策略包契约；
- V25 历史回放和实时模拟遵守同一逻辑版本，不要求实时开盘已有 240 根分钟线；
- 缺数据/缺模型/缺依赖/缺算法/缺 HMM 系数/缺停牌审计/缺涨跌停/pre_close/分钟线，均结构化失败；
- 无日频 fallback、无算法 fallback、无默认价格/现金/持仓、无空结果假成功；
- 8001 生产后端未被开发验证重启；
- 受保护资产无静默改动。

## 12. 需要人工确认的风险点

以下不是当前文档阶段的阻塞，但进入代码开发前必须在实现记录中显式确认：

1. 是否将 `StrategyPackage Manifest v1.1` 正式落地为独立合同文档，还是先通过 Paper v2 runtime profile 解释旧 v1 字段。
2. `V25DayFeatureProvider` 的特征字段、顺序、归一化是否已经有 QE 端权威来源；如果没有，不能用默认值补齐。
3. 对已有 Paper v2 portfolio 是否需要迁移生成默认 runtime profile；若当前无历史模拟盘数据，可以只对新组合启用。
4. `created_by` / `activated_by` 在本地单用户环境的取值规范：例如 `local_user`、`codex_dev`、未来登录用户 ID。
5. 未来若允许 fee/risk policy 动态变更，也必须纳入 runtime profile version 和 audit，不能只更新 portfolio 冻结字段。
