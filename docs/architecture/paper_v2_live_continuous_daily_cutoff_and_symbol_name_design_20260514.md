# Paper v2 LIVE 连续运行、每日 Cutoff 自动滚动与股票名称持久化设计

> 日期：2026-05-14  
> 范围：Paper Trading v2 `LIVE_ONLY` / `CATCHUP_THEN_LIVE` 的连续模拟盘运行、每日选股 artifact cutoff 自动滚动、交易记录股票名称持久化。  
> 非范围：QE 回测逻辑重构、因子缓存、HMM 模型训练、miniQMT 实盘适配、券商实盘下单。

## 1. 结论

1. `cutoff_date` 在选股中心和历史回放中可以是一次性确定的运行参数，但在 LIVE 连续模拟盘中不得长期固定在 session 配置中。
2. LIVE 模拟盘每天准备新 `paper_v2.run` 时，必须按当前 `trade_date` 自动计算 `effective_cutoff_date = previous_trading_day(trade_date)`。
3. 每个交易日生成的权威 selection artifact 必须以当天 `trade_date` 和当天计算出的 `effective_cutoff_date` 单独持久化，不能复用前一交易日 artifact。
4. 当前 session 可继续运行，但现有 `selection_artifact_config.cutoff_date=2026-05-13` 必须在 LIVE 路径中被每日自动值覆盖，不能让 2026-05-15 继续使用 2026-05-13。
5. 股票名称应从 `market.stock_basic.ts_code -> name` 获取，并作为展示/审计字段持久化到 Paper v2 交易记录中；不得参与选股、下单、撮合、风控。

## 2. 已核对的当前代码事实

### 2.1 调度器与多 session

- `backend/services/paper_trading_v2/scheduler.py` 的 `run_once()` 会调用 `repository.list_tickable_sessions()`，一次最多处理 50 个 session。
- tickable 状态包含：
  - `CREATED`
  - `PREFLIGHTING`
  - `REPLAYING`
  - `CATCHING_UP`
  - `SWITCHING_TO_LIVE`
  - `LIVE_RUNNING`
  - `LIVE_WAITING_FOR_BAR`
  - `LIVE_WAITING_NEXT_TRADING_DAY`
- `backend/services/paper_trading_v2/session.py` 在同一 portfolio 创建 session 时会检查 active session，禁止同一个 portfolio 同时启动多个 active session。
- 因此平台可以同时运行多个 portfolio / 多个策略包，但当前调度器是单线程顺序 tick，不是多策略并行撮合线程。

### 2.2 LIVE 每日启动路径

- `backend/services/paper_trading_v2/live_session.py::_tick_live_intraday()`：
  - 确认当天是交易日；
  - 查找当前 portfolio + trade_date 是否已有 run；
  - 没有 run 时调用 `_prepare_live_run()`；
  - 有成功 run 时进入 `LIVE_WAITING_NEXT_TRADING_DAY`；
  - 收盘后调用 `_finalize_live_day()`，再进入 `LIVE_WAITING_NEXT_TRADING_DAY`。
- 这意味着只要 scheduler 持续运行，session 可跨交易日持续推进。

### 2.3 当前 cutoff 风险

- `backend/services/paper_trading_v2/live_session.py::_ensure_live_selection_cutoff()` 当前逻辑：
  - 如果 `selection_artifact_config.cutoff_date` 已存在，直接返回；
  - 如果没有 cutoff 且 `auto_generate=true`，才计算上一交易日。
- 当前生产 session 中已经存在固定值：
  - `selection_artifact_config.cutoff_date = 2026-05-13`
  - `auto_generate = true`
- 这对 2026-05-14 是正确的，但对 2026-05-15 会 stale。

### 2.4 股票名称数据源

- 当前 Paper v2 表 `orders` / `fills` / `positions` / `cash_ledger` 只有 `symbol`，没有 `stock_name`。
- 当前 DB 中 `market.stock_basic` 有 `ts_code` 和 `name`。
- 当前 48 个运行订单对应 symbol 在 `market.stock_basic` 中全部可匹配。

## 3. 设计目标

### 3.1 LIVE cutoff 行为

LIVE 连续模拟盘每天必须满足：

```text
run.trade_date = T
effective_cutoff_date = previous_trading_day(T)
selection artifact = package + manifest + T + DB_HISTORICAL + runtime_hash(effective_cutoff_date)
```

要求：

- 每个交易日自动生成或读取当天对应的权威 artifact。
- 不允许因为 session 创建时带了旧 `cutoff_date` 而长期复用旧 artifact。
- 不允许使用 QE 回测 `pred.pkl`、Qlib 回测产物、因子缓存产物作为 LIVE 当日选股结果。
- 当所需数据未准备好时 fail-fast 或在开盘前等待，不能静默使用旧数据。

### 3.2 选股 / 历史回放 cutoff 行为

选股中心和历史回放仍保留确定性：

- 用户显式给定 `cutoff_date` 时，历史回放可按该参数重现。
- 没有显式 cutoff 时，可按 `trade_date` 的上一交易日推导。
- 历史回放不得因为 wall-clock 日期变化自动改变历史 run 的 cutoff。

### 3.3 股票名称持久化行为

交易记录展示需要：

- 订单列表、成交列表、持仓、现金流水可显示股票名称。
- 名称应在记录创建或更新时持久化，便于之后审计，不依赖页面实时 join。
- 名称缺失不得阻断交易；缺失时 `stock_name=null`，前端显示 symbol。
- 名称字段只用于展示/审计，不参与任何交易决策。

## 4. Cutoff 配置语义

### 4.1 新增语义字段

建议在 `selection_artifact_config` 中明确区分：

```json
{
  "auto_generate": true,
  "inference_backend": "wsl",
  "cutoff_policy": "previous_trading_day",
  "cutoff_date": "2026-05-13"
}
```

字段含义：

- `cutoff_policy=previous_trading_day`：按每个 run 的 `trade_date` 自动计算上一交易日。
- `cutoff_policy=pinned`：固定使用 `cutoff_date`，只允许选股中心和历史回放使用。
- `cutoff_date`：某次 run 的有效 cutoff 快照，允许写入 per-run runtime_config 和 artifact metadata；不应作为 LIVE session 的长期锁定值。

### 4.2 LIVE 默认规则

LIVE 模式下规则如下：

1. 如果 `auto_generate=true`，默认 `cutoff_policy=previous_trading_day`。
2. LIVE 每次 `_prepare_live_run(trade_date=T)` 都重新计算 `effective_cutoff_date`。
3. 即使 session 级配置里存在旧 `cutoff_date`，也要在 per-run config 中覆盖为当天的上一交易日。
4. 不把覆盖后的 cutoff 写回 `paper_v2.trade_session.runtime_config_json`，避免 session 被某一天的 cutoff 污染。
5. 把当天 cutoff 写入：
   - `paper_v2.run.runtime_config`
   - selection artifact metadata
   - dashboard daily signal 输出

### 4.3 历史回放默认规则

历史回放规则保持确定性：

1. `selection_artifact_config.cutoff_date` 已存在时，继续尊重。
2. 没有 cutoff 且 `auto_generate=true` 时，按 replay day 的上一交易日注入。
3. 回放期间不因当前真实日期变化改变 cutoff。

## 5. 后端改造方案

### 5.1 引入运行级 cutoff 解析函数

新增或重构 helper：

```python
def resolve_selection_cutoff_for_run(
    *,
    mode: PaperSessionMode | str,
    trade_date: date,
    runtime_config: dict[str, Any],
    calendar_provider: TradeCalendarProvider,
) -> tuple[date | None, dict[str, Any]]:
    ...
```

建议位置：

- 首选：`backend/services/paper_trading_v2/selection_cutoff.py`
- 或作为 `PaperTradingLiveMinuteExecutor` / `PaperTradingDayRunner` 共用私有 helper。

LIVE 行为：

- deep copy `runtime_config`；
- 计算 `previous_trading_day(trade_date)`；
- 覆盖 copied config 中的 `selection_artifact_config.cutoff_date`；
- 写入 `paper_v2_session.selection_cutoff_date`；
- 写入 `paper_v2_session.selection_cutoff_policy = previous_trading_day`；
- 返回 copied config，不修改 session 原始 config。

历史行为：

- deep copy `runtime_config`；
- 如果有 fixed cutoff，保留；
- 如果没有 fixed cutoff 且 auto_generate，注入上一交易日；
- 返回 copied config。

### 5.2 修改 LIVE 准备路径

目标文件：

- `backend/services/paper_trading_v2/live_session.py`

当前 `_prepare_live_run()` 中：

```python
config = dict(session.runtime_config)
...
self._ensure_live_selection_cutoff(config, trade_date=trade_date)
```

建议改为：

```python
config = copy.deepcopy(session.runtime_config)
config = resolve_live_selection_cutoff_for_run(
    trade_date=trade_date,
    runtime_config=config,
    calendar_provider=self.calendar_provider,
)
```

关键点：

- 必须使用 `copy.deepcopy()`，避免 nested `selection_artifact_config` 被 session 级 config 污染。
- LIVE 下不再因为已有 `cutoff_date` 而 return。
- per-run config 中的 cutoff 参与 `selection_artifact_runtime_hash(runtime_config)`，确保每天 artifact hash 不同。

### 5.3 保留历史回放路径

目标文件：

- `backend/services/paper_trading_v2/day_runner.py`

`_ensure_authoritative_selection_artifact()` 当前通过 `_parse_selection_cutoff_date()` 读取 runtime config。历史回放可继续尊重固定 cutoff。

如果要统一 helper：

- 历史 runner 在 run_day 开始处调用 historical policy helper；
- 不改变已有测试中 pinned cutoff 的行为；
- 不允许 LIVE 改造破坏历史回放复现。

### 5.4 scheduler 启动保障

目标文件：

- `backend/main.py`

当前 Paper v2 scheduler 是 opt-in：

```python
ENABLE_PAPER_TRADING_V2_SCHEDULER=1
```

建议：

- 保持开发端口 opt-in，避免 8011/3011 advancing production durable sessions。
- 生产环境必须明确配置 `ENABLE_PAPER_TRADING_V2_SCHEDULER=1`。
- 在 `/paper-v2` 总览或 live dashboard 显示 scheduler 状态，若当前存在 `RUNNING` portfolio 但 scheduler 未运行，显示红色阻断提示。

不建议：

- 不建议所有环境默认启动 scheduler。
- 不建议前端通过轮询自动 tick 代替后端 scheduler。

## 6. 股票名称持久化方案

### 6.1 Schema 变更

新增 nullable 字段：

```sql
ALTER TABLE paper_v2.orders ADD COLUMN IF NOT EXISTS stock_name text;
ALTER TABLE paper_v2.fills ADD COLUMN IF NOT EXISTS stock_name text;
ALTER TABLE paper_v2.positions ADD COLUMN IF NOT EXISTS stock_name text;
ALTER TABLE paper_v2.cash_ledger ADD COLUMN IF NOT EXISTS stock_name text;
```

可选字段：

```sql
ALTER TABLE paper_v2.order_execution_state ADD COLUMN IF NOT EXISTS stock_name text;
```

`order_events` 可暂不加字段，因为 live dashboard 已通过 order join 得到 symbol / order status；若未来需要事件脱离订单独立审计，再追加。

### 6.2 数据源与解析

新增 repository helper：

```python
def resolve_stock_names(self, symbols: Iterable[str]) -> dict[str, str]:
    SELECT ts_code, name
    FROM market.stock_basic
    WHERE ts_code = ANY(%s)
```

写入点：

- 创建订单时：`save_order()`
- 创建成交时：`save_fill()`
- 保存持仓时：`save_positions()`
- 保存现金流水时：`save_cash_entry()` 或批量现金写入点

建议在一个 tick 内批量解析并缓存：

```python
name_map = repository.resolve_stock_names(symbols)
```

避免每个 fill 单独查 DB。

### 6.3 Backfill

对已有记录执行：

```sql
UPDATE paper_v2.orders o
SET stock_name = sb.name
FROM market.stock_basic sb
WHERE o.symbol = sb.ts_code
  AND o.stock_name IS NULL;

UPDATE paper_v2.fills f
SET stock_name = sb.name
FROM market.stock_basic sb
WHERE f.symbol = sb.ts_code
  AND f.stock_name IS NULL;

UPDATE paper_v2.positions p
SET stock_name = sb.name
FROM market.stock_basic sb
WHERE p.symbol = sb.ts_code
  AND p.stock_name IS NULL;

UPDATE paper_v2.cash_ledger c
SET stock_name = sb.name
FROM market.stock_basic sb
WHERE c.symbol = sb.ts_code
  AND c.stock_name IS NULL;
```

### 6.4 前端展示

目标文件：

- `frontend/src/app/paper-v2/portfolios/[portfolioId]/ledger/page.tsx`
- `frontend/src/app/paper-v2/portfolios/[portfolioId]/live-dashboard/page.tsx`

展示格式：

```text
603375.SH 盛景微
```

如果 `stock_name` 为空：

```text
603375.SH
```

## 7. 验证方案

### 7.1 单元测试

新增/修改：

- `backend/tests/paper_trading_v2/test_live_session.py`

必须覆盖：

1. LIVE 模式已有旧 cutoff 时，`trade_date=2024-01-05` 自动覆盖为 `2024-01-04`。
2. LIVE 模式不会把覆盖值写回 session.runtime_config。
3. 历史回放 / day runner 仍尊重显式 pinned cutoff。
4. 每个不同 trade_date 得到不同 runtime hash / artifact lookup key。
5. `LIVE_WAITING_NEXT_TRADING_DAY` session 在下一交易日会进入新 run 准备路径。

新增：

- `backend/tests/paper_trading_v2/test_repository_stock_names.py`

必须覆盖：

1. `resolve_stock_names()` 能批量返回名称。
2. `save_order()` / `save_fill()` 持久化 `stock_name`。
3. 缺失名称时不阻断交易，字段为 null。

### 7.2 DEV DB smoke

在 DEV DB / side-port 执行：

1. 应用 schema migration。
2. 对现有 48 条订单做 backfill。
3. 调用订单 / 成交 API，确认返回 `stock_name`。
4. 用测试日期模拟连续 LIVE：
   - Day1：`trade_date=T`，cutoff = `T-1 trading day`
   - Day2：`trade_date=T+1 trading day`，cutoff 自动滚动
5. 确认没有读取 QE backtest `pred.pkl` 或 Qlib 回测预测产物。

### 7.3 生产前检查

生产使用前确认：

- `ENABLE_PAPER_TRADING_V2_SCHEDULER=1`
- `session-scheduler/status.running=true`
- `market.trading_calendar` 明日为交易日
- `market.stk_limit` 明日 09:14 前完成增量更新
- 当前 RUNNING session 的 LIVE cutoff policy 生效
- 明日生成的 selection artifact metadata：
  - `trade_date=2026-05-15`
  - `cutoff_date=2026-05-14`
  - `source_type=live_qe_model_inference_v1`
  - `authority_scope=authoritative_selection`

## 8. 分阶段实施计划

### Phase 1：LIVE cutoff 自动滚动

改造点：

- `backend/services/paper_trading_v2/live_session.py`
- 可新增 `backend/services/paper_trading_v2/selection_cutoff.py`
- `backend/tests/paper_trading_v2/test_live_session.py`

验收：

- LIVE 旧 cutoff 被每日覆盖。
- 历史回放 pinned cutoff 不变。
- 单测通过。

### Phase 2：scheduler 生产保障与 UI 提示

改造点：

- `backend/main.py` 不改变 dev opt-in 语义，只补充生产说明或状态暴露。
- `frontend/src/app/paper-v2/...` 显示 scheduler 未运行时的红色提示。

验收：

- scheduler running 时不报警。
- RUNNING portfolio + scheduler stopped 时前端明确提示。

### Phase 3：股票名称 DB 持久化

改造点：

- Paper v2 schema migration。
- `backend/services/paper_trading_v2/repository.py`
- tests。

验收：

- 新订单 / 新成交写入 `stock_name`。
- 旧记录 backfill 后 API 可见。
- 缺失名称不阻断交易。

### Phase 4：前端展示股票名称

改造点：

- ledger 页面。
- live dashboard 时间轴 / 订单表。

验收：

- 展示 `symbol + stock_name`。
- 没有 `stock_name` 时仍正常显示 symbol。

### Phase 5：端到端 smoke

验收：

- DEV DB + side-port 可跑通连续两日模拟。
- 订单、成交、持仓、现金流水完整。
- selection artifact 每日 cutoff 正确。
- 不触碰生产 8001，除非单独确认。

## 9. 红线

- 不得把 QE 回测预测、Qlib 回测产物、历史 factor cache 当作 LIVE 选股输入。
- 不得因为 LIVE cutoff 计算失败而静默复用前一天 artifact。
- 不得把股票名称作为选股、交易、风控条件。
- 不得在开发端口默认启动 durable scheduler。
- 不得为多个策略并发强行引入并行 tick，除非另起调度并发设计并验证锁和资源隔离。

<!-- paper-v2-live-dashboard-contract-addendum-20260514 -->

## 10. 实时看板与仓位展示合约补充

### 10.1 目标仓位与调仓意图

当前 `live_dashboard` 的“目标仓位与调仓意图”卡片只读取 run events 中的：

- `TARGETS_GENERATED`
- `ORDER_INTENTS_GENERATED`

历史回放路径 `day_runner` 已按此合约持久化目标仓位与调仓意图数组；LIVE 路径 `_prepare_live_run()` 当前只写 `LIVE_RUN_PREPARED`，context 中只有 `target_count` / `order_intent_count` 等计数，没有 `targets` / `intents` 数组。因此 LIVE 看板可显示“已生成 48 个目标 / 48 个意图”的事实，但无法渲染表格。

设计要求：

1. LIVE 与 replay 必须使用同一可观测事件合约。
2. `_prepare_live_run()` 生成 `targets` 与 `intents` 后，补写 `TARGETS_GENERATED` 与 `ORDER_INTENTS_GENERATED` 事件，结构与 `day_runner` 保持一致。
3. `LIVE_RUN_PREPARED` 继续保留为摘要事件，记录数据就绪、信号数据源、实盘数据源、执行算法、风险过滤数量等信息。
4. 修复范围只限可观测性与 UI 数据接口，不改变 TargetPositionEngine、RebalanceEngine、分钟执行或撮合逻辑。

### 10.2 今日信号排序与字段语义

“今日信号”来自 authoritative selection artifact 的 `scores_json[:50]`，用于解释当日模型候选和追踪字段。它不是最终持仓表。

设计要求：

1. 前端表格提供字段排序，第一阶段对返回的 Top50 做 client-side sorting。
2. 可排序字段至少包括：`rank`、`symbol`、`score`、`reference_price`、`target_weight`、`reason`。
3. 排序不得触发重新选股、不得改变 artifact、不得改变目标仓位或调仓意图。
4. 若保留 artifact 的 `target_weight` 字段，列名必须标注为“候选等权预览”或“artifact 预览权重”。

### 10.3 日频策略真实权重

对 `score_weighted_topk_v2` 日频策略，真实目标仓位应由 manifest/runtime contract 驱动：

```text
StrategyPackageSelectionArtifact.scores_json
  -> StrategyPackageRuntime SignalSnapshot
  -> TargetPositionEngine.build_targets(manifest=manifest)
  -> _build_score_weighted_targets()
  -> _compute_score_weighted_weights()
  -> RebalanceEngine.build_order_intents()
```

`selection_score_artifact.scores_json[*].target_weight = 1 / topk` 只是候选 artifact 的等权预览字段。UI 不得把它解释为最终持仓权重。最终仓位需要展示 `TARGETS_GENERATED.context.targets[*].target_weight`，并与订单 metadata 中的 `target_metadata.target_value` / `qe_strategy_family` 形成可追踪链路。

### 10.4 UI 命名与对象边界

1. `paper_v2.portfolio` 当前产品语义改称“模拟账户”或“模拟盘实例”。
2. “组合”一词只保留给未来多个策略包组合后的策略组合对象。
3. Paper v2 内部采用顶部横向二级导航，不再新增自身左侧导航。
4. 策略包、模拟账户必须支持可读名称、自定义别名、描述和标签；数据库 ID 与 hash 仅用于审计和复制。
5. 大量历史 smoke/E2E 记录默认隐藏或归档，清理必须先预览，且不得影响当前 RUNNING 实例。

### 10.5 实时资产时间线

LIVE 看板的资产曲线必须基于 `paper_v2.intraday_snapshots` 的分钟快照序列，而不是单次 run 摘要。

设计要求：

1. 后端持续返回 `snapshot_time`、`nav`、`cash`、`market_value`、`positions_json` 和 `source`，并补充快照数量、首末快照时间。
2. 前端使用时间轴折线或面积图展示 NAV / 收益率；柱状 sparkline 只可作为缩略图，不应作为主图。
3. 当快照数量少于 2 时，前端显示“样本不足，等待后续分钟快照”，并展示唯一快照时间，避免误解为完整实时曲线。
4. LIVE tick 应在处理到新的 completed minute bar 后写入新的 intraday snapshot；如果长时间只有 1 条快照，应作为数据/调度可观测问题暴露。

<!-- paper-v2-live-execution-causality-p0-addendum-20260514 -->

## 11. P0：LIVE 模拟盘因果边界、资产重估与 miniQMT 预审

### 11.1 已确认问题

当前 `local_sim + TDX_REALTIME` LIVE 模拟盘存在三个 P0/P1 问题：

1. **P0 因果错误**：订单在 `09:39:43` 创建，但成交记录可落在 `09:31-09:35`，说明 LIVE runner 在订单创建后把已经完成的早盘分钟线当作新 bar 回放成交。
2. **P0 资产曲线停止**：所有订单填满后，`active_states` 为空，LIVE runner 只更新 cursor，不再按后续分钟线 mark-to-market 写 `intraday_snapshot`，导致资产曲线只有 1 个点。
3. **P1 撮合真实性不足**：TDX_REALTIME 路径当前使用分钟 K 线 close 撮合，不是 tick / 盘口 / 逐笔撮合；和真实实盘会有系统性误差。

### 11.2 P0 修复原则

1. LIVE_ONLY 严格禁止用订单创建前的分钟 bar 生成成交。
2. CATCHUP_THEN_LIVE 的历史补跑必须标记为 replay/catchup；切入 LIVE 后也必须从切换时刻之后的新 bar 开始。
3. 订单完全成交后，只要 session 仍在盘中运行，就必须继续对持仓做分钟级 mark-to-market，直到收盘或 session 停止。
4. 实时估值和审计级分钟快照分离：
   - 审计快照：使用 completed minute bar，持久化到 `paper_v2.intraday_snapshots`。
   - 指示性估值：未来可用 TDX quote/tick 或 miniQMT full_tick 计算，不直接伪装成已成交审计记录。
5. 不允许 silent fallback：缺分钟线、缺最新价、缺 limit/suspend/pre_close 时要么等待，要么 fail-fast；不得用旧价、默认价或回测价。

### 11.3 P0 后端改造

#### 11.3.1 订单起始 cursor

在 LIVE `_prepare_live_run()` 创建 `OrderExecutionState` 时写入一个明确的 `last_processed_bar_time`：

```text
strict_live_start_bar_time = latest_common_completed_bar_time(symbols, trade_date, as_of_time)
```

含义：

- `strict_live_start_bar_time` 之前或等于该时间的分钟 bar 已经在订单创建前可见，不允许用于新订单成交。
- 第一次 LIVE `_process_live_run()` 只处理 `bar_time > strict_live_start_bar_time` 的 bar。
- 如果订单在 09:39 创建，09:31-09:39 的 bar 只能用于信号上下文/估值，不得生成 fill。

需要记录：

- `OrderExecutionState.last_processed_bar_time = strict_live_start_bar_time`
- `OrderExecutionState.plan` 或 `algo_state` metadata 中记录：
  - `strict_live_start_bar_time`
  - `order_created_at`
  - `live_causality_mode = strict_no_backfill`

#### 11.3.2 LIVE 标准事件

同一处补写：

- `TARGETS_GENERATED`
- `ORDER_INTENTS_GENERATED`

并保留 `LIVE_RUN_PREPARED` 摘要事件，避免看板缺目标仓位。

#### 11.3.3 无 active order 时继续估值

`_process_live_run()` 当前逻辑在 `not active_states` 时直接保存 cursor 并返回。应改为：

1. 查询当前持仓。
2. 用所有持仓 symbol 的 latest common completed minute bar 计算 `latest_available`。
3. 如果 `latest_available > last_processed`，按最新 close 做 mark-to-market。
4. 写入 `paper_v2.intraday_snapshots`，source=`TDX_REALTIME`，metadata 标记 `snapshot_type=mark_to_market_no_active_orders`。
5. 更新 `session_day.last_processed_bar_time` 到最新估值 bar。

#### 11.3.4 快照触发频率

审计级快照至少每个新 completed minute bar 1 条；若性能不足，可采用配置化节流：

```text
intraday_snapshot_policy = every_completed_bar | every_n_minutes
```

默认 LIVE 模拟盘必须是 `every_completed_bar`。

### 11.4 P0 测试要求

必须新增/更新测试：

1. `test_live_prepare_seeds_order_cursor_to_existing_completed_bar`：订单创建前已有 09:31-09:39 bar，第一次执行不得产生 09:31-09:39 fill，只能等待 09:40 以后。
2. `test_live_tick_never_creates_fill_before_order_created_at`：所有 fill 的 `trade_time >= order.created_at` 或满足可解释的 next completed bar 规则；历史 replay 例外必须显式标记。
3. `test_live_mark_to_market_continues_after_orders_filled`：订单填满后，后续新分钟 bar 仍写 `intraday_snapshot`。
4. `test_live_dashboard_exposes_snapshot_count_and_lag`：dashboard 能显示 `latest_available_bar_time - last_processed_bar_time` 的估值 lag。
5. 现有 replay/day_runner 测试不能退化；历史回放仍允许处理全天已完成分钟线。

### 11.5 miniQMT 模拟盘预审结论

miniQMT 仿真账户可显著降低本地模拟撮合误差，但不能自动消除全部问题。

miniQMT 能改善：

- 订单撮合由 miniQMT/券商仿真系统处理，不再由 LocalSim 用分钟 close 人工生成 fill。
- 可获得更接近真实交易端的委托状态、成交回报、拒单、撤单和部分成交。
- 可用 xtdata/miniQMT 行情通道，避免 TDX 与撮合端价格源不一致。

miniQMT 不能保证：

- 仿真撮合完全等同真实交易所排队和实盘成交。
- 网络延迟、客户端断线、柜台拒单、涨跌停、停牌、资金冻结等边界天然无 bug。
- AIstock 策略层不会提前发送过早买单；执行节奏仍需策略控制。

### 11.6 miniQMT 设计方案现状

已有设计/骨架：

- `docs/discussion/paper_v2_dual_broker_pr_split_plan_20260509.md`
- `docs/standards/cross_test_framework_template_20260508.md`
- `backend/services/paper_trading_v2/broker/base.py`
- `backend/services/paper_trading_v2/broker/localsim.py`
- `backend/tests/paper_trading_v2/test_portfolio_broker_backend.py`
- `backend/tests/paper_trading_v2/test_market_data_broker_match.py`

现状判断：

- `MinuteDataSource.MINIQMT_REALTIME`、`broker_backend=minqmt_sim`、强绑定测试已有。
- LocalSim BrokerBackend 协议已有实现。
- MiniQMTSimBackend 仍是待实施项；设计文档中标为后续 PR / `blocked_by_task_minqmtsim_impl`。
- 因此 miniQMT 模拟盘可以并行设计/预审，但不能假设当前已经可替代 LocalSim LIVE。

### 11.7 miniQMT 并行实施前的预审清单

1. **强绑定**：`minqmt_sim` 只能绑定 `MINIQMT_REALTIME`，禁止 TDX/DB 驱动 miniQMT 撮合。
2. **单例与容量**：一个 miniQMT 仿真账户同一进程只允许绑定一个 active package/portfolio，超出显式拒绝。
3. **订单生命周期**：submit 返回 pending handle；真实 fill 只能来自 miniQMT 回报，不得本地伪造 fill。
4. **连接错误**：disconnect/timeout/rc=-1 必须映射 `BrokerConnectivityError`，不得静默重连后继续假装同一订单状态可靠。
5. **拒单错误**：资金不足、停牌、涨跌停、账户限制必须映射 `BrokerRejectedError` 并持久化 rejection reason。
6. **时间因果**：订单 `submitted_at`、miniQMT accepted time、fill time 必须单调；禁止 fill 早于 submitted_at。
7. **撤单一致性**：cancel accepted 不等于已撤；必须查询最终状态或等待回报。
8. **资产状态来源**：持仓、现金、可用资金以 miniQMT account/position query 为准；不得用 LocalSim ledger 覆盖。
9. **事件幂等**：重复成交回报按 broker order id / fill id 去重。
10. **UI 标识**：页面必须明确显示 broker=`minqmt_sim`、market source=`MINIQMT_REALTIME`、account mode=`SIM`，避免和本地模拟混淆。

## 12. P0 实施补充：严格按 tick 时刻建立 LIVE 成交边界

本次 P0 代码修复以用户反馈的“09:39 创建订单却在 09:31-09:35 成交”为硬约束，进一步收紧 §11.3.1：

1. `strict_live_start_bar_time` 不再仅等于订单创建时已可见的 latest common bar，而是等于 LIVE `_prepare_live_run(as_of_time)` 的 tick 时刻。
2. 订单执行状态初始化为：`OrderExecutionState.last_processed_bar_time = strict_live_start_bar_time`。
3. `_process_live_run()` 只允许处理 `bar_time > last_processed_bar_time` 的分钟 bar，因此 `09:39:43` 创建的订单不会使用 `09:39:00` 或更早的 completed minute bar 成交，只能等待 `09:40:00` 之后的新 bar。
4. `latest_prepared_bar_time` 仍写入 `LIVE_RUN_PREPARED.context`，仅用于审计“订单创建时行情最多已经到哪一分钟”，不得作为成交 cursor。
5. `algo_state` 记录并在每次增量执行后保留：
   - `live_causality_mode = strict_no_backfill`
   - `order_created_at`
   - `strict_live_start_bar_time`
6. LIVE 标准事件补齐：`TARGETS_GENERATED` 和 `ORDER_INTENTS_GENERATED` 写入 run_events，使 replay/live dashboard 使用同一数据合约。
7. 订单全部 final 后，LIVE tick 不再直接返回，而是按当前持仓 symbol 重新查询 latest common completed bar；若该时间晚于 `session_day.last_processed_bar_time`，写入新的 `intraday_snapshot` 并更新 cursor。

当前已落地的验证用例：

- `test_live_prepare_seeds_order_cursor_after_existing_completed_bars`
- `test_live_tick_never_backfills_prepared_order_with_existing_bars`
- `test_live_mark_to_market_continues_after_orders_filled`

结论：LocalSim + TDX_REALTIME 仍是分钟 close 级别模拟，不是 tick 撮合；但已禁止订单创建前历史分钟线回填成交，并恢复订单完成后的盘中资产曲线持续更新。

## 13. miniQMT 模拟盘预审补充：不能把“由 miniQMT 撮合”误解为“无误差”

miniQMT 模拟盘值得并行推进，但必须先通过以下设计审查，否则会把 LocalSim 的逻辑错误转移到外部账户适配层：

1. **因果边界**：AIstock 提交订单时写 `submitted_at`，miniQMT accepted / fill / cancel 回报必须单调；任何 fill time 早于 submitted_at 都应拒绝入账并记录严重错误。
2. **撮合来源**：`minqmt_sim` 的成交只能来自 miniQMT 委托/成交回报或权威查询结果，不能复用 LocalSim `MinuteExecutionEngine` 伪造成交。
3. **行情绑定**：`minqmt_sim` 只能配 `MINIQMT_REALTIME`，且 UI/后端都要 fail-fast 阻断 `minqmt_sim + TDX_REALTIME`、`local_sim + MINIQMT_REALTIME` 等交叉组合。
4. **账户状态来源**：现金、持仓、可用股份、冻结资金以 miniQMT 账户查询为准；AIstock 本地 ledger 只能作为审计镜像，不得覆盖账户真实状态。
5. **撤单语义**：cancel accepted 不是已撤；必须等待最终回报或查询确认，避免 UI 显示已撤但账户仍有挂单。
6. **幂等与重复回报**：按 broker order id、broker fill id、symbol、quantity、price、time 去重；重复回报不能重复改现金和持仓。
7. **连接故障**：断线、超时、rc=-1、账号未登录等必须映射 typed errors，并暂停该 session；不能静默 fallback 到 LocalSim 或 TDX。
8. **拒单分类**：资金不足、停牌、涨跌停、最小单位、账户权限、风控拒单都应有明确 reason，并落库到 order_events / session_events。
9. **容量约束**：MiniQMTSim 是单例/独占资源；并行多个策略必须走组合层或明确不支持，不能多个 portfolio 同时争用同一仿真账户。
10. **验证矩阵**：mock xtquant 单元测试覆盖 submit/cancel/query/error；真 SIM 集成测试必须用 `@pytest.mark.integration_minqmt` 本地跑，不进默认 CI。

并行实施建议：

- 可以在单独分支/窗口并行做 miniQMT 预审和 `MiniQMTSimBroker` mock 实现；不要在当前 P0 LocalSim 修复未验证前切换明早模拟盘到 miniQMT。
- 当前明早模拟盘的最短安全路径仍是：先合入 LocalSim P0 因果与资产曲线修复，再用 side-port/dev DB smoke；miniQMT 作为后续提升撮合真实性的独立里程碑。

## 14. 2026-05-14 ??????? `TDX minute data fetch failed` ??

### 14.1 ??

??????

```text
DATA_UNAVAILABLE
2026-05-14 14:45:42
TDX minute data fetch failed
symbol=300970.SZ
trade_date=2026-05-14
```

????????????? `paper_v2.errors`??? `portfolio_id=paper_8f9b5368e86344ba8d699b4995ae665e`?`session_id=psess_4ae1b8aa651b46cd9f418989a328df74`??? session ???? `FAILED`???? run ?? `RUNNING`????? LIVE tick ??????? session/run ????????

### 14.2 ????

????? `300970.SZ` ??? TDX HTTP ??????? `2026-05-14` ????????? 240 ??? bar?????? `15:00:00+08:00`??????????TDX HTTP/???????????????????????????????ST PIT?QE ??????????????

????? LIVE ?? tick / mark-to-market ??????????? symbol ? TDX ????`market_data._load_raw_bars_from_tdx()` ????????? `DataUnavailableError("TDX minute data fetch failed")`????? `PaperTradingSessionRunner.tick()` ????? `TradingCoreError` ??? session `FAILED`?????????????????????????

### 14.3 ????

- ??? fallback ? DB ????????????????????
- ? `TDX minute data fetch failed` ????????????????? tick??? `LIVE_DATA_FETCH_RETRYABLE`??? session ? `LIVE_WAITING_FOR_BAR`?????? tick ???
- ??? `stk_limit`??? `suspend_d`??? day features??? limit/pre_close??? bar ??????????????? fail-fast???????????????
- ????????????????? fill?????????????? cursor??????? run/session_day ?????? cursor?

### 14.4 ????

??????

- `PaperTradingLiveMinuteExecutor.tick()` ?? `DataUnavailableError("TDX minute data fetch failed")`?
- ?? `LIVE_DATA_FETCH_RETRYABLE` session event????????`trade_date`?`as_of_time` ? `retryable=true`?
- session ??/??? `LIVE_WAITING_FOR_BAR`?portfolio ?? `RUNNING`?run ??????
- ?? `paper_v2.errors`????????????????????UI ???? session event / data freshness ??????????????
- ???? `test_live_tdx_fetch_failure_waits_without_failing_session`????????? errors??? run ? `RUNNING` ????

### 14.5 ????????

????? 2026-05-14 ??????? session ???????????????????????????????????????? UI ?????????? reset/reopen/recreate??????????????? `paper_v2.errors` ????????? DB ???????
