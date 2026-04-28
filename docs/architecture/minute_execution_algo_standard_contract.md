# 日内分钟线执行策略标准代码规范

> 日期：2026-04-27
> 状态：Draft / 强制执行规范
> 适用范围：QE 分钟线回测、Paper Trading v2 历史回放、Paper Trading v2 实时分钟线模拟、未来可接入的执行策略。
> 不适用范围：QMT、Shadow、实盘真实下单；这些场景暂不实现，但未来必须复用本规范的 core/adapter 边界。

## 1. 目标

AIstock 的日内分钟线执行策略必须同时服务两个权威验证场景：

```text
QE / Qlib minute backtest
Paper Trading v2 historical replay / realtime streaming
```

规范目标：

1. 同一执行策略只有一份逻辑版本，避免 QE 与 Paper v2 执行结果漂移。
2. 执行策略严格 fail-fast，但不能把正常市场不可交易状态误判为程序错误。
3. 执行策略不能静默降级，不能从 V25、V24 或其他算法静默切换为 TWAP、日频成交、默认价格或空成功。
4. 策略代码和资产分离；程序框架修改不得静默修改模型权重、validated policy、StrategyPackage manifest、QE workspace 资产或数据库资产。
5. 所有订单、成交、未成交原因、计划、状态和异常必须可追溯。

## 2. 分层架构

所有分钟线执行策略必须按以下分层开发：

```text
ExecutionAlgoCapability / Catalog metadata
        ↓
ValidatedExecutionPolicy
        ↓
ExecutionAlgoCore        # 唯一权威算法逻辑
        ↓
Adapter Layer
  ├─ QE / Qlib Adapter
  ├─ Paper v2 Historical Adapter
  └─ Paper v2 Realtime Streaming Adapter
        ↓
ExecutionState / Plan / Events / Persistence
```

### 2.1 Core 层

Core 层只实现算法逻辑，禁止直接依赖：

- FastAPI request / response；
- PostgreSQL repository；
- Paper v2 portfolio/run 表；
- Qlib `TradeDecisionWO`、`trade_exchange` 等运行对象；
- UI 配置对象；
- 隐式文件路径；
- 任何 silent fallback。

Core 层可以依赖：

- 标准 Python / numpy；
- 明确注入的模型推理函数；
- 明确传入的 `OrderContext`、`MarketContext`、`ExecutionState`。

### 2.2 Adapter 层

Adapter 只做对象转换、状态恢复和错误封装，禁止改变核心算法语义。

允许：

- 从 Qlib exchange 读取价格后转换为 `MarketContext`；
- 从 Paper v2 `MinuteBar` 转换为标准 bar；
- 从 DB / TDX 数据源生成 `observed_only` 或 full-day context；
- 把 core 的 `StepDecision` 转成 Qlib order 或 Paper v2 `StepFill`；
- 把 reason code 写入日志、artifact、run event 或 order event。

禁止：

- 缺模型时改用 TWAP；
- 缺分钟线时改用日频成交；
- 缺价格时填默认价格；
- 缺持仓/现金时填默认持仓/默认现金；
- 隐式修改 plan 权重；
- 隐式替换 workspace 中已经复制的策略文件。

## 3. 能力声明规范

单个 `min_bars` 字段不能表达所有分钟线场景。每个算法必须声明拆分后的能力：

```json
{
  "algo_code": "V25_TWO_STAGE",
  "algo_version": "2026-04-27.v1",
  "supported_freqs": ["1m"],
  "historical_replay_supported": true,
  "historical_requires_full_day": true,
  "historical_min_required_bars": 240,
  "realtime_streaming_supported": true,
  "live_min_start_bars": 1,
  "live_step_mode": "persisted_plan",
  "catchup_then_live_supported": true,
  "plan_horizon_bars": 240,
  "warmup_bars": 0,
  "early_stage_bars": 30,
  "late_stage_bars": 210,
  "requires_prev_close": true,
  "requires_limit_price": true,
  "requires_suspend_status": true,
  "requires_trading_calendar": true,
  "runtime_asset_keys": ["early_model_path", "late_model_path"]
}
```

规则：

- `historical_min_required_bars` 表示闭日历史回放所需分钟线数量。
- `live_min_start_bars` 表示实时盘开始执行所需的最少已观察分钟线。
- `plan_horizon_bars` 表示算法计划长度。
- `early_stage_bars` / `late_stage_bars` 是算法结构，不得混用为历史数据需求。
- `execution_algorithm_catalog.min_bars` 只能作为旧兼容字段，不得作为 Paper v2 V25 权威能力来源。

## 4. 市场状态分类

执行策略必须先分类市场状态，再决定是否成交、等待、跳过或失败。

| 状态 | 类型 | 行为 |
|---|---|---|
| `tradable` | 正常市场 | 按 core 计算 step |
| `suspended_by_suspend_d` | 市场不可交易 | 不成交，记录 reason，不让整条任务崩溃 |
| `suspended_by_exchange` | 市场不可交易 | 不成交，记录 reason |
| `intraday_halt_or_no_bar` | 市场不可交易/等待 | 当前分钟不成交；实时模式等待下一根 bar |
| `limit_up_buy_blocked` | 市场约束 | 买单当前分钟不成交，保留未成交 |
| `limit_down_sell_blocked` | 市场约束 | 卖单当前分钟不成交，保留未成交 |
| `p0_limit_buy_at_down_limit` | V25/P0 有利价格 | 可按 P0 规则全量尝试买入 |
| `p0_limit_sell_at_up_limit` | V25/P0 有利价格 | 可按 P0 规则全量尝试卖出 |
| `prev_close_missing_with_suspend` | 可解释数据缺失 | 不成交，记录停牌证据 |
| `prev_close_missing_data_error` | 数据质量错误 | fail-fast |
| `limit_data_missing_due_to_suspend` | 可解释数据缺失 | 不执行依赖涨跌停的逻辑，记录 reason |
| `limit_price_missing_data_error` | 数据质量错误 | fail-fast |
| `model_missing_config_error` | 配置/资产错误 | fail-fast |
| `plan_generation_error` | 算法/资产错误 | fail-fast |

## 5. Fail-fast 边界

必须 fail-fast：

- 用户选择算法与实际执行算法不一致；
- 执行算法未注册或未声明支持当前模式；
- 模型文件不存在、为空、hash 不匹配或不能加载；
- Torch/CUDA/模型结构不满足策略要求；
- plan 没生成、plan 长度不正确、plan 有 NaN、权重不符合算法设计；
- 无停牌或临停证据但缺少 `prev_close`、涨跌停价、交易日历、分钟线；
- 执行 adapter 尝试使用未授权 fallback。

不得 fail-fast 为程序错误：

- 全日停牌；
- 盘中临停；
- 买入遇涨停；
- 卖出遇跌停；
- 有权威停牌证据导致的 `prev_close` 或 limit 字段缺失；
- 实时盘尚无新分钟线，此时应记录 `WAITING_FOR_BAR`，不是成功成交。

标准判断逻辑：

```text
if 缺的是策略运行前提:
    fail-fast
elif 缺的是行情字段，但可被权威市场状态解释:
    记录业务状态，不成交/等待/跳过当前分钟
elif 缺的是行情字段，且不能被权威市场状态解释:
    fail-fast
else:
    按策略 core 正常执行
```

## 6. MarketContext 输入规范

所有 adapter 必须把各自数据源转换成标准 context：

```text
symbol
trade_date
data_source: DB_HISTORICAL | TDX_REALTIME | QLIB_BACKTEST
prev_close
limit_up
limit_down
is_suspended
suspend_status
bars
observed_only
expected_bar_count
trading_calendar
auction_included
day_features
```

### 6.1 价格基准对齐规范

日内执行策略的市场状态判断必须使用同一套价格基准。AIstock 的标准执行基准为
`raw` / 不复权人民币价格：

- `price` / 分钟 OHLC；
- `prev_close`；
- `limit_up` / `limit_down`；
- open gap / `gap_ratio` 特征。

严禁把复权 OHLC 直接与不复权 `prev_close`、涨停价、跌停价比较。特别是 Qlib 分钟
bin 的价格约定为：

- `$open/$close/$high/$low` 是复权价格；
- `$factor` 是复权因子；
- `$prev_close/$up_limit_price/$down_limit_price` 来自 `market.stk_limit`，是不复权价格；
- Qlib adapter 必须先计算 `raw_price = adjusted_price / $factor`，再做涨跌停、P0、
  买入涨停阻塞、卖出跌停阻塞、open gap / `gap_ratio` 计算。

必须 fail-fast：

- 有有效分钟 bar 但 `$factor` 缺失、非有限或小于等于 0；
- `price_basis` 与 `limit_price_basis` 不一致；
- 代码路径中存在复权价格与不复权价格的直接比较；
- 缺少价格基准元数据且 adapter 无法证明输入同基准。

不得 fail-fast 为程序错误：

- 有权威停牌、临停或无 bar 证据导致价格或 factor 缺失；此时应记录明确 market-state reason。

规则：

- `DB_HISTORICAL` 闭日回放必须检查完整分钟线覆盖。
- `TDX_REALTIME` 只代表已观察分钟线，不能伪造未来 full-day 数据。
- `QLIB_BACKTEST` adapter 必须保留与 Paper v2 相同的市场状态语义。
- `QLIB_BACKTEST` adapter 必须显式处理复权/不复权转换，不能把 Qlib 复权 OHLC 与 raw limit/pre_close 混用。
- 缺字段时必须按第 5 节判断，不得填默认值伪装成功。

## 7. ExecutionPlan 与状态持久化

有计划型算法必须持久化 plan：

```text
algo_code
algo_version
plan_id
plan_sha256
weights
horizon_bars
generated_at
source_context_hash
model_asset_hashes
metadata
```

运行状态必须持久化：

```text
order_id
symbol
trade_date
algo_code
algo_version
step
filled_quantity
remaining_quantity
last_processed_bar_time
plan_json
plan_sha256
algo_state_json
status
```

实时/回放重启后必须从持久化 plan 和 state 恢复，禁止重新生成不同计划后继续执行。

## 8. V25 Two-Stage 强制语义

`V25_TWO_STAGE` 必须满足：

- `EARLY_LEN = 30`；
- `LATE_LEN = 210`；
- `TOTAL_LEN = 240`；
- early plan 权重为 `0.8879`；
- late plan 权重为 `0.1121`；
- historical replay 需要完整 240 根分钟线；
- realtime streaming 可从第一根已观察分钟线生成 240-step persisted plan；
- plan 一旦生成必须持久化，后续 step 使用同一 plan；
- `plan is None` 必须 fail-fast，不能 TWAP fallback；
- 买入遇涨停：当前分钟不买，记录 `limit_up_buy_blocked`；
- 卖出遇跌停：当前分钟不卖，记录 `limit_down_sell_blocked`；
- 买入遇跌停：可按 P0 全量尝试买入，记录 `p0_limit_buy_at_down_limit`；
- 卖出遇涨停：可按 P0 全量尝试卖出，记录 `p0_limit_sell_at_up_limit`；
- 停牌/临停/无 bar 不能被归类为 plan 缺失。

## 9. QE / Paper v2 一致性

QE/Qlib adapter 与 Paper v2 adapter 必须满足：

- 同一 `algo_code + algo_version + model_asset_hash + algo_config`；
- 同一标准 `MarketContext` 输入；
- 同一 core 逻辑或 normalized hash 一致的物理副本；
- 同一市场状态 reason code；
- 同一 plan 权重校验；
- 同一 no-silent-fallback 行为。

QE workspace 已经复制的策略文件是历史实验资产。恢复旧实验时必须显式选择：

1. 使用旧 workspace 文件复现实验；或
2. 重新注入当前修复后的权威模板，并记录为恢复迁移。

不能静默替换。

## 10. 准入测试矩阵

每个分钟线执行策略进入 QE/Paper v2 前必须通过：

1. 正常分钟线：生成并执行 step。
2. historical 缺完整分钟线：fail-fast。
3. realtime 第一根已观察分钟线：可启动且持久化 plan。
4. 全日停牌：不成交，记录停牌 reason，不崩溃为程序错误。
5. 盘中临停/无 bar：等待或跳过当前分钟。
6. 买入涨停、卖出跌停：当前分钟不成交。
7. 买入跌停、卖出涨停：按算法 P0 或对应规则执行。
8. 有停牌证据的 `prev_close` 缺失：业务状态处理。
9. 无停牌证据的 `prev_close` 缺失：fail-fast。
10. 涨跌停价缺失且无停牌证据：fail-fast。
11. 模型缺失：fail-fast。
12. plan 无效：fail-fast。
13. QE adapter 与 Paper adapter 同输入 plan 一致。
14. 运行中断后用持久化 state 恢复，不重复成交。

## 11. 禁止事项

- 禁止静默回退到 TWAP、VWAP、CLOSE_PRICE 或日频成交。
- 禁止用空数组、默认价格、默认现金、默认持仓伪装成功。
- 禁止在框架代码修改中静默修改模型权重、StrategyPackage manifest、validated policy、QE workspace 或 DB 资产。
- 禁止使用生产端口 `8001`/`3000` 做开发验证或重启。
- 禁止只修 AIstock 一侧而让 QE/RD-Agent 模板长期漂移；如暂不同步，必须记录风险和后续任务。
