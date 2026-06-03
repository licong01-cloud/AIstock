# PriceGuard / ExecutionAcceptance / 荐股生命周期 设计方案 v2

> 日期：2026-06-02
> 状态：Draft / 待审批 / 设计方案（不含代码实现）
> 取代：本文件为 v2，重排优先级并新增"荐股生命周期"能力。v1 设计 `qe_paper_price_guard_execution_acceptance_design_20260601.md` 与审核意见 `price_guard_design_review_20260602.md` 作为背景与文献依据继续有效；本文件冲突处以 v2 为准。
> 分工：**Codex 负责开发，本设计作者负责按"验收标准"逐阶段审核**。每阶段必须通过审核签核后方可进入下一阶段。
> 适用范围：选股荐股展示与生命周期（advisory）、QE 分钟线回测、StrategyPackage validated execution policy、Paper v2 模拟盘。
> 不适用范围：真实实盘自动下单、生产服务重启、生产 DB DDL 直接执行（DDL 走既有 gate）。

---

## 1. 本次方案要解决的问题（v2 重述）

在 v1 三项能力（PriceGuard 买入接受、选股买入区间展示、ExitGuard 止盈止损）的基础上，v2 做两处关键调整：

1. **优先级反转**：先在**选股功能**落地"买入区间 + 止损价"展示，并叠加**自选股票池 + 每日复评的荐股生命周期**，先在真实数据上**观察选股/荐股效果**；**之后**再进入 QE 实验做受控验证，最后才到 Paper v2 enforced。
2. **新增能力 C：荐股生命周期（advisory lifecycle）**——选中名单进入荐股/自选列表后，**每天重新运行策略**，对每只股票判断：继续持有 / 加仓 / 减仓 / 止损 / 止盈 / 排名跌出退出 / 到期。这正是机构投顾荐股的标准流程，本方案**复用 AIstock 已有的 watchlist + Selection Center 基础设施**实现，而不新建交易系统。

**三类能力的关系**：

```text
能力 B：选股买入区间 + 止损区间展示        （pre-trade guidance, 纯展示）
能力 C：荐股生命周期 = watchlist + 每日复评 （advisory monitoring, 无账户/无成交）
能力 A：PriceGuard/ExitGuard enforced       （QE 验证后, 真正改变订单）
```

能力 B/C 都是 **advisory（建议/监控），不下单、不记 ledger、不产生成交**，可立即在真实数据上跑、可观察效果；能力 A 的 enforced 仍必须经 QE A/B 验证。三者边界见 §3。

---

## 2. 复用现有基础设施（已核验，含 file 依据）

v2 **不新建独立交易系统**，在以下已存在的真实组件上扩展：

| 现有组件 | 位置 | v2 用途 |
| --- | --- | --- |
| `app.watchlist_items`（entry_price/entry_rank/entry_source/entry_task_id/entry_loop_id/entry_as_of）+ categories | `backend/db/init_watchlist_schema.py:19-51`；repo `repositories/watchlist_repo_impl.py`；router `routers/watchlist.py` | 荐股生命周期的状态容器（能力 C） |
| Selection Center：`selection.run` / `selection.package_result`（score/rank/target_weight/**reference_price**/component_scores/reason）/ `selection.aggregate_result` | `backend/db/init_trading_core_v2_schema.py:283-330`；service `services/selection_center/` | 选股结果持久化；能力 B 在此叠加区间/止损字段 |
| `selection.daily_selection_evidence`（每日不可变选股证据，evidence_payload_json） | `init_trading_core_v2_schema.py:413-450` | 每日复评的输入证据来源 |
| 导出端点 `POST /selection-center/runs/{run_id}/to-watchlist` | `routers/selection_center.py` | 候选→自选池的现成入口 |
| `ValidatedExecutionPolicy`（policy_json + policy_sha256）+ `ALLOWED_POLICY_JSON_KEYS` | `services/strategy_package/execution_policy.py:25-36,78-126` | 能力 A 的 policy 固化（扩展白名单加 `price_guard`/`exit_guard`） |
| Paper v2 day_runner（signal→risk→targets→intents→execution）+ runtime override 拒绝 | `services/paper_trading_v2/day_runner.py:291..572,2590-2604` | 能力 A enforced 落点 |
| QE ConfigComposer（NestedExecutor + inner_strategy.kwargs） | `services/quantevolver/config_composer.py` | 能力 A 的 QE 注入 |
| `backend/services/trading_core/`（oms/minute_execution/models/risk/ledger） | 已存在 | 新增 `price_guard.py`/`exit_guard.py` core evaluator 落点 |

**缺口（v2 要补）**：Paper v2 ledger 之外、没有"候选→建仓→持有→退出 + 每日复评"的轻量 advisory 生命周期层。能力 C 即补此缺口。

---

## 3. 边界与可选项（用户硬性要求，可验证）

三层严格隔离，互不串味：

| 层 | 是否下单/成交 | 是否有账户/ledger | 数据 | 验证地位 | 默认开关 |
| --- | --- | --- | --- | --- | --- |
| **Advisory（能力 B+C）** | 否 | 否（仅 watchlist + 复评记录） | 真实每日数据 | 仅展示 + **事后回顾质量评估**，不得宣称 validated PnL | 可选，独立开关 |
| **QE 回测（能力 A 验证）** | 模拟成交（Qlib） | QE account | 历史分钟线 | **enforced 的唯一验证门** | A/B 实验显式开关 |
| **Paper v2（能力 A enforced）** | 模拟成交 | `paper_v2.*` ledger | 历史回放/实时 | 只消费 QE 验证过的 policy_sha256 | 默认 disabled |

**可验证不变量（审核硬指标）**：

1. Advisory 层**永不**调用 OMS/broker/Paper ledger；代码搜索 + 测试证明 advisory 路径无 `create_order`/`position_ledger` 写入。
2. 能力 A `enabled=false` 或 `price_guard` key 缺失时，QE/Paper v2 订单流与当前 main **字节级等价**（回归测试：disabled == 现状 fills/events）。
3. Advisory 展示的任何区间/止损必须标注 `guidance_status ∈ {rule_default, bucket_calibrated, qe_validated}` 与 `policy_sha256`；未经 QE 验证一律标 `rule_default`/`bucket_calibrated`，**禁止**标 `qe_validated`。
4. `policy_sha256` 由整个 policy_json 计算；"price_guard 缺省"与"`{enabled:false}`"必须**不同 hash、相同行为**；现存 policy 不含该 key、hash 不变（向后兼容）。

---

## 4. 核心契约（一次设计完整，禁止半成品接缝）

> 采纳"不做简化版、避免推倒重写"的要求：**契约/接口/schema/reason code/DB 字段一次定型**，分阶段的只是"启用与验证"，不是"功能砍半"。Phase 1 必须交付以下全部接口（即使部分 mode 默认禁用）。

### 4.1 命名（解决 v1 ★1 冲突）

代码库 `reference_price` 已存在：watchlist=`entry_price`（成本），`selection.package_result.reference_price`（语义须在 Phase 1 第一步确认）。PriceGuard 决策基准价一律命名 **`signal_ref_price`**，禁止复用 `reference_price` 表达"信号可比价"。Phase 1 第一项任务即确认 `selection.package_result.reference_price` 现有语义并产出"复用 or 改名"决议。

### 4.2 纯函数 core evaluator（解决 v1 ★2）

`backend/services/trading_core/price_guard.py` 与 `exit_guard.py` 提供**纯函数**：输入 `PriceGuardContext` / `ExitGuardContext`，输出 decision，**零 I/O**。Advisory 层、QE inner strategy、Paper v2 day_runner **各写 adapter 取数，但调用同一 evaluator**。

**`PriceGuardContext` 字段（Phase 0 一次定全，覆盖非单调 gap 场景，避免后期加特征导致重写）**：

```text
# 价格/市场状态（raw, 经 $factor 转 raw）
signal_ref_price, prev_close, open_price/current_price, limit_up, limit_down,
open_gap_bps, current_gap_bps, dist_to_limit_up_bps, dist_to_limit_down_bps,
volume_ratio_open, amount_20d, board_type, st_flag, suspend_status
# alpha/策略上下文
score, rank, score_bucket, expected_alpha_bps, alpha_family, target_weight
# 市场 regime（解决 Q4：高开≠单调变坏）
market_regime(HMM), momentum_regime_flag, event_flag(若可得)
# 执行上下文
side, sell_reason, holding_days, prev_position
# 审计（解决未来函数）
price_basis, feature_availability_ts
```

`ExitGuardContext` 额外含：`actual_entry_cost`、`high_since_entry`、`latest_rank`、`atr/vol`、`days_since_entry`、`t1_eligible`（当日建仓当日不可卖）。

**决策模型不是单一标量阈值**：见 §4.3.1。同一组特征下，evaluator 的输出是 `residual_alpha(features) vs cost+margin` 的条件判断，gap 只是其中一个特征；`rule_v1` 的 green/yellow/red 单调带 **必须叠加突破/近涨停分支** 才能表达"高开可能是涨停前兆、应加仓"。

### 4.3 policy schema（三态 mode 一次预留，支撑 rule↔ML 不重写）

```jsonc
{
  "price_guard": {
    "contract": "execution_price_guard_v1",
    "enabled": true,
    "mode": "rule_v1 | bucket_calibrated | ml_residual_alpha_v1",  // 三态槽位 Phase 1 即在场
    "price_basis": "raw",
    "signal_ref_price": { "buy": "signal_close", "sell": "signal_close", "intraday": "arrival_price" },
    "buy":  {
      // 单调带（rule_v1 默认；生产建议被 bucket/ml 覆盖，见 §4.3.1）
      "max_open_gap_bps": 300, "yellow_open_gap_bps": 150, "yellow_size_multiplier": 0.5,
      "max_chase_bps": 100, "near_limit_up_skip_bps": 80, "allow_partial": true,
      // 突破/近涨停加仓分支（解决 Q4：高开可能是涨停前兆，须按策略族单独验证）
      "breakout_addon": {
        "enabled": false,                       // 默认关；仅动量/事件族 + QE A/B 验证后开
        "require_momentum_regime": true,
        "min_score_bucket": "top5",
        "dist_to_limit_up_lt_bps": 200,         // 距涨停足够近才考虑
        "min_volume_ratio_open": 1.5,
        "add_size_multiplier": 0.5,             // 加仓量独立上限
        "min_fill_probability": 0.6             // 成交概率护栏: 封板买不进则不追
      }
    },
    "sell": { "rebalance_max_slippage_bps": 150, "risk_exit_max_slippage_bps": 500,
              "near_limit_down_rebalance_skip_bps": 80, "allow_partial": true },
    "guidance_status": "rule_default | bucket_calibrated | qe_validated",
    "policy_sha256": "..."
  },
  "exit_guard": {
    "contract": "exit_guard_v1",
    "enabled": false,
    "mode": "rule_v1 | bucket_calibrated | ml_exit_v1",
    "stop_loss": { "enabled": true, "max_loss_bps": 600, "volatility_multiple": 2.5, "reference": "actual_entry_cost" },
    "take_profit": { "enabled": false, "take_profit_bps": 1200, "trailing_stop_bps": 500 },
    "alpha_decay_exit": { "enabled": true, "rank_drop_below": "top40%", "confirm_days": 2 },
    "time_stop": { "enabled": false, "max_holding_days": 10 },
    "t1_handling": "defer_to_next_tradable_day"   // 解决 v1 ★4
  }
}
```

### 4.3.1 决策模型：条件期望残余 alpha，而非单一标量阈值（解决 Q4）

gap 不是单调变坏：高开既可能"alpha 已被吃掉→跳过"，也可能"强动量/事件→即将涨停→残余 alpha 仍为正→应加仓"。因此 evaluator 的本质判断是：

```text
decision = f( residual_alpha(gap, score_bucket, regime, momentum, dist_to_limit_up, volume_ratio, board) , cost + margin )
```

三种 mode 对同一问题的表达：

| mode | gap 处理 | 能否表达"高开加仓" |
| --- | --- | --- |
| `rule_v1` | 单调 green/yellow/red 带 + `breakout_addon` 分支 | 能，但靠显式突破分支（人工规则）|
| `bucket_calibrated`（生产基线）| 按 (score×gap×regime×board) 桶查 forward alpha，**桶内阈值各异** | 天然能：同 8% gap，topK+动量桶=ACCEPT，中分+横盘桶=SKIP |
| `ml_residual_alpha_v1` | 模型直接回归 residual alpha，gap 为非单调特征 | 天然能 |

工程要求：

- **生产决策禁止发布全局标量 `max_open_gap_bps` 作为唯一判据**；它只是 `rule_v1` bootstrap。进入 Phase 4 enforced 的生产 policy 必须是 `bucket_calibrated` 或 `ml`，使阈值按策略族/分桶变化。
- `breakout_addon` 仅对**动量/事件策略族**开启，且必须 QE A/B 证明"扣除真实成交概率与次日反转成本后残余 alpha 为正"。核心多因子策略默认关闭，**不得以"可能涨停"为由关闭价格保护**。
- **成交概率护栏**：近涨停/封板时 `fill_probability` 急剧下降（一字板买不进），`breakout_addon.min_fill_probability` 把"想买"与"买得到"分离；这同时是机会与不可成交风险。`dist_to_limit_up_bps` 与磁吸效应（§8）为此提供依据。

### 4.4 reason code（一次定全，含 advisory 与 fail-fast）

```text
# 业务
ACCEPT_WITHIN_GREEN_ZONE / REDUCE_YELLOW_OPEN_GAP / REDUCE_YELLOW_CHASE_BAND
SKIP_OPEN_GAP_EXCEEDED / SKIP_ABOVE_MAX_BUY_PRICE / SKIP_NEAR_LIMIT_UP
SKIP_BELOW_MIN_SELL_PRICE_REBALANCE / EXECUTE_RISK_EXIT_WITH_WIDER_LIMIT / WAITING_FOR_PRICE_GUARD_INPUT
# 突破/近涨停加仓（Q4，仅动量/事件族 + QE 验证后）
ADD_BREAKOUT_NEAR_LIMIT / SKIP_BREAKOUT_LOW_FILL_PROBABILITY
# 退出/生命周期
HOLD / TAKE_PROFIT_TARGET_REACHED / TRAILING_STOP_TRIGGERED / STOP_LOSS_TRIGGERED
STOP_LOSS_DEFERRED_T1 / TIME_STOP_TRIGGERED / ALPHA_RANK_DROP_EXIT / WATCHLIST_EXPIRED
# 去重（解决 v1 ★3，区分既有涨停预过滤 vs 新增）
PRE_FILTER_LIMIT_UP (既有) / PG_SKIP_NEAR_LIMIT_UP (新增)
# fail-fast
SIGNAL_REF_PRICE_MISSING_DATA_ERROR / PRICE_BASIS_MISMATCH_ERROR / LIMIT_PRICE_MISSING_DATA_ERROR
UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR / UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR
```

`*_DATA_ERROR`/`*_CONFIG_ERROR` 必须 fail-fast，禁止伪装成 SKIP；SKIP 禁止伪装成成功成交。

---

## 5. 荐股生命周期（能力 C）状态机

```text
CANDIDATE        选股产出, 在 selection.package_result, 可带 entry band/stop guidance
   │ 用户/规则采纳 → to-watchlist
ENTERED          进入自选池, 记 planned_entry 或 actual_entry; status=ENTERED
   │ 每日复评
HOLDING          每日 re-run: 重算 rank/价/相对 band/相对 stop/alpha decay → action
   │ 触发退出
EXITED           STOP_LOSS / TAKE_PROFIT / ALPHA_RANK_DROP_EXIT / TIME_STOP / WATCHLIST_EXPIRED
```

### 5.1 数据模型（解决 Q3：复用自选池 + 是否加表）

**基数不同 → 实体表与时间序列必须分离**（架构铁律：绝不把每日变化的字段 UPDATE 到实体表，否则摧毁历史、Phase 3 评估与审计无法做）：

| 表 | 性质（dim/fact） | 内容 | 写入 |
| --- | --- | --- | --- |
| **新增 `app.advisory_program`（SCD/版本化配置）** | 荐股任务配置维表（每个可并行运行的荐股任务 1 个 program；配置变更生成新 version） | `program_id`、`program_name`、`status`、`target_count`、`package_mode(single_package/fusion_pool/sleeve_mode_future)`、`package_set_hash`、`fusion_policy_sha256`、`review_policy_sha256`、`entry_price_basis`、`exit_price_basis`、`review_schedule`、`created_by`、`version` | UPDATE 状态；配置变更生成新 version，不改历史 |
| **新增 `app.advisory_program_package`（append-only）** | program 与 StrategyPackage 绑定事实 | `program_id`、`version`、`package_id`、`package_role(primary/member)`、`weight`、`enabled`、`package_snapshot_hash` | **只 INSERT，永不 UPDATE**；StrategyPackage manifest 不被修改 |
| `app.watchlist_items`（复用，仅加 SCD 字段） | 实体/状态（每 code 1 行） | `status`、`planned_entry`、`actual_entry`、`exited_at`、`exit_reason` —— 一生只变几次 | UPDATE 状态 |
| **新增 `app.advisory_daily_review`（append-only）** | 时间序列/事实（每 item×trade_date 1 行） | 当日 `current_price`、计算出的 `entry_band(green/yellow/red)`、**当日预计 `stop_price`/`take_price`**、`action`、`reason`、`policy_sha256`、`feature_availability_ts`、`evidence_id`(FK)、`t1_note` | **只 INSERT，永不 UPDATE** |
| **新增 `app.advisory_replay_run`（append-only，Phase 3）** | 回放运行事实（每次策略包/融合策略回放 1 行） | `run_id`、`strategy_package_id` 或 `package_set`、`fusion_policy_sha256`、`start_signal_date`、`end_signal_date`、`selection_cutoff`、`entry_price_basis`、`exit_price_basis`、`review_policy_sha256`、`created_at` | **只 INSERT，永不 UPDATE** |
| **新增 `app.advisory_episode_return`（append-only，Phase 3）** | 荐股 episode 收益事实（每 run×episode 1 行；同 code 重新入选生成新 episode） | `episode_id`、`symbol`、`signal_date`、`effective_entry_date`、`entry_price`、`entry_price_basis`、`exit_signal_date`、`effective_exit_date`、`exit_price`、`exit_price_basis`、`exit_reason`、`holding_trading_days`、`return_bps`、`is_win`、`win_rate_inclusion_status`、`max_runup_bps`、`max_drawdown_bps`、`still_active_mark_price`、`price_quality_status` | **只 INSERT；更换价格口径必须生成新 run** |
| **新增 `app.advisory_program_metric_snapshot`（可为物化视图或 append-only cache）** | program 排行与统计快照（每 program×as_of_trade_date 1 行，可由 episode_return/daily_review 重算） | `program_id`、`as_of_trade_date`、`enabled_since`、`package_mode`、`package_set_hash`、`active_count`、`entered_episode_count`、`take_profit_count`、`stop_loss_count`、`win_rate`、`avg_return_bps`、`median_return_bps`、`avg_holding_days`、`turnover_rate`、`max_drawdown_bps`、`last_review_status` | 派生统计；若落库则 **只 INSERT 新快照**，不得覆盖历史；排行榜只自动记录 `last_review_status` 这一类数据质量状态 |

要点：

- **alpha 信号不重复存**：当日 score/rank/component 已在 `selection.daily_selection_evidence` / `selection.package_result`；`advisory_daily_review` 用 `evidence_id` 外键引用，只**叠加**价格/止盈止损/action 一层。
- **止盈止损价天然每天变**（trailing stop 随 `high_since_entry` 上移、stop 相对 `actual_entry_cost`、alpha-decay 阈值随 rank 变）→ 属时间序列，每天**重算并新插一行**，"当前止损价"= 最新一行；历史各日止损价全部保留可复盘。
- **结论**：需 **1 张新 append-only 日表**；watchlist 基表只加少量 SCD 状态字段。二者配合，不是二选一。DDL 走既有 gate（Phase 2 设计评审定最终列）。

补充约束（每日 job 必须满足）：

- **幂等可重跑**：同 (item, trade_date) 重跑产出相同行（或带 version），供 cron 安全重试。
- **复权/公司行动**：watchlist 记 raw code；除权除息/拆股跨日时，`actual_entry_cost` 与 stop/take 价必须按 `$factor` 调整，否则"9.40 止损"在除权后失真（v1/此前各版均未覆盖，**新增必做项**）。
- **停牌/退市/ST 转换**：item 停牌则当日复评记 `WAITING`/carry，不得用陈旧价触发止损；退市/ST 转换须能迁移 lifecycle 状态。

**每日复评（daily review）逻辑**（advisory，无成交）：

```text
for each watchlist item (status in ENTERED/HOLDING):
  ev = today's selection.daily_selection_evidence (重跑选股的最新 rank/score)
  ctx = ExitGuardContext(entry_cost, current_price(raw), high_since_entry, holding_days, latest_rank, vol, ...)
  decision = exit_guard.evaluate(ctx, exit_guard_policy)   # 复用 §4.2 纯函数
  persist advisory_daily_review(item, trade_date, action, reason, evidence, t1_note)
  if decision in {STOP_LOSS, TAKE_PROFIT, RANK_DROP, TIME_STOP}: status → EXITED (advisory)
```

**T+1 标注（v1 ★4）**：当日建仓的 STOP_LOSS 在 advisory 层只能记 `STOP_LOSS_DEFERRED_T1`，提示"次一可交易日方可执行"，不得标记为当日已退出。

#### 5.2 Top20 复评策略：重跑信号，但不全量覆盖荐股池

**设计决议**：每日复评必须每天重新运行 Selection Center，但新产生的 Top20 只代表"今日新候选/入选信号"，不得直接全量替换昨日荐股池。生命周期主池采用**原活跃荐股 + 今日候选缓冲池**的合并评估：

```text
N = 20                       # 主荐股目标数量
K = 40 或 60                 # 复评缓冲池，建议 2N 或 3N
today_signal = 今日重新运行选股，至少保留 top K 与所有 active_pool 的 rank/score evidence
active_pool = watchlist 中 status in ENTERED/HOLDING 的荐股
review_universe = active_pool ∪ today_signal.topK

for item in active_pool:
  先执行停牌/ST/退市/除权等资格与数据处理
  再执行 ExitGuard 风险退出、盈利保护、alpha/rank 衰减、持有期检查

for candidate in today_signal.top20 - active_pool:
  若主荐股未满 N，可进入 ENTERED
  若主荐股已满，仅替换已触发退出、连续弱化或跌出缓冲区的旧荐股
  若只是 rank 边界抖动（如新股 rank=19、旧股 rank=21），不得替换
```

**阈值分离（hysteresis）**：

- `rank_enter_threshold = 20`：进入 Top20 可成为新荐股候选。
- `rank_exit_threshold = 40`（或 `2N`）：已有荐股跌出持有缓冲区才进入 alpha 衰减退出候选。
- `rank_drop_confirm_days = 2`：连续确认后退出，避免单日噪声造成高换手。
- `daily_replacement_budget = 20% of N`：普通替换每日最多约 4 只；硬止损、退市、ST/资格失效不受该上限约束。

**证据要求**：

- 每日 selection evidence 不得只保存 Top20；至少要覆盖 `topK + active_pool`，否则旧荐股跌出 Top20 后无法区分 rank=21 与 rank=300。
- 若 active item 当日缺 rank/score evidence，必须记录 `RANK_MISSING_DATA_ERROR` / `EVIDENCE_MISSING_DATA_ERROR` 并 fail-fast 或 `WAITING_DATA`，不得静默退出或填默认 rank。
- UI 必须分开展示"今日策略 Top20"与"荐股生命周期池/每日复评结果"，避免用户误解为昨日荐股被无理由覆盖。

该规则对应机构指数与组合管理中的缓冲区/低换手原则：MSCI、FTSE Russell、S&P 等指数方法论普遍使用 buffer/banding 降低成分边界换手；组合再平衡文献中的 no-trade region / turnover-constrained rebalancing 也要求资产接近目标时不交易、越界后再调整。本设计只把该原则用于 advisory 生命周期状态，不产生交易、订单或 ledger。

#### 5.3 退出机制：止损、止盈、alpha 衰减与到期退出

**设计决议**：进入 Top20 后必须形成独立 `advisory_episode` 语义。同一股票从 ENTERED/HOLDING 到 EXITED 是一次荐股生命周期；未来重新进入 Top20 时必须生成新的 episode，不得复活旧生命周期。

退出不是"跌出 Top20 即退出"，而由 ExitGuard 在每日复评中按优先级判断：

```text
for each active advisory item:
  load entry_cost, entry_date, entry_rank, high_since_entry, holding_days
  load today current_price, latest_rank, latest_score, suspend/ST/delist/limit info
  apply corporate action factor to entry_cost, stop_price, take_price

  if suspended:
    action = WAITING_SUSPENDED
  elif delisted or ST/eligibility failed:
    action = EXIT_ELIGIBILITY
  elif hard_stop_loss_triggered:
    action = STOP_LOSS or STOP_LOSS_DEFERRED_T1
  elif trailing_profit_lock_triggered:
    action = TAKE_PROFIT_TRAILING
  elif alpha_decay_triggered and return_bps >= 0:
    action = TAKE_PROFIT_ALPHA_DECAY
  elif alpha_decay_triggered and return_bps < 0:
    action = ALPHA_RANK_DROP_EXIT
  elif time_stop_triggered:
    action = TIME_STOP
  else:
    action = HOLD / HOLD_STRONG / HOLD_WEAK
```

**止损目标**：限制单只荐股最大亏损，并阻止"推荐后明显破位但系统继续显示持有"。

- `hard_stop_price = adjusted_entry_cost * (1 - max_loss_bps / 10000)`，第一版建议 `max_loss_bps=600`。
- `soft_stop_price = adjusted_entry_cost - volatility_multiple * ATR`，第一版建议 `volatility_multiple=2.5`；soft stop 可先记 `SOFT_STOP_WARNING`，次日确认后退出。
- 当日建仓当日触发 hard stop 时，仅记 `STOP_LOSS_DEFERRED_T1`，status 不立即 EXITED，遵守 A 股 T+1 语义。

**止盈目标**：保护已有收益，避免盈利完全回吐；多因子荐股第一版不建议固定到价即全部止盈，默认采用 trailing stop + alpha 衰减止盈：

- `trailing_activate_bps = 800`：收益达到约 8% 后激活追踪止盈。
- `trailing_stop_bps = 400`：从 `high_since_entry` 回撤约 4% 后触发 `TAKE_PROFIT_TRAILING`。
- `alpha_decay_exit`：若 `latest_rank > rank_exit_threshold` 且连续 `confirm_days=2`，有盈利则 `TAKE_PROFIT_ALPHA_DECAY`，亏损则 `ALPHA_RANK_DROP_EXIT`。
- 固定 `take_profit_bps` 仅作为动量/事件策略族的可选 policy，不作为多因子 TopK 默认规则；启用固定止盈必须生成新的 `policy_sha256` 并进入质量报告分桶。

**第一版 policy 建议**：

```json
{
  "exit_guard": {
    "stop_loss": {
      "enabled": true,
      "max_loss_bps": 600,
      "volatility_multiple": 2.5,
      "soft_confirm_days": 1,
      "t1_handling": "defer_to_next_tradable_day"
    },
    "take_profit": {
      "enabled": true,
      "mode": "trailing_or_alpha_decay",
      "fixed_take_profit_enabled": false,
      "trailing_activate_bps": 800,
      "trailing_stop_bps": 400
    },
    "alpha_decay_exit": {
      "enabled": true,
      "rank_exit_threshold": 40,
      "confirm_days": 2
    },
    "time_stop": {
      "enabled": false,
      "max_holding_days": 20
    }
  }
}
```

**记录与 UI 要求**：

- 每次退出必须记录 `action`、`reason_code`、`policy_sha256`、`evidence_id`、当日价格、当日 rank/score、收益率、止损价、止盈/追踪止盈价。
- `EXITED` 后的 item 不再参与 active_pool；同 code 重新入选时生成新 episode，并在质量报告中独立计算。
- UI 对每只荐股展示：入选日期/入选 rank/入选价、当前 rank/当前价/收益率、当前 stop/take/trailing 价、最新复评 action、退出原因与证据链。
- 退出只改变 advisory lifecycle 状态，不写 Paper v2 ledger、不调用 OMS/broker、不产生模拟或真实成交。

#### 5.4 多策略包组合荐股：独立证据 + canonical fusion rank

**设计决议**：选股荐股允许多个 StrategyPackage 共同生成一个主荐股池，但每日复评必须同时保留每个策略包的独立评分/排名证据，并生成一个唯一的 `fusion_rank` 作为 advisory 生命周期的 canonical rank。不得把不同策略包的 raw score 直接平均后作为跨包排序依据。

当前 Selection Center 已支持 `single_package` / `intersection` / `union` / `weighted_fusion`。能力 C 的多策略包复评必须在此基础上补齐以下语义：

```text
package_set = [pkgA, pkgB, ...]
fusion_policy = {
  "method": "weighted_rank_fusion",
  "package_weights": {"pkgA": 0.5, "pkgB": 0.5},
  "candidate_top_k": 40 或 60,
  "active_pool_inclusion": true,
  "missing_rank_policy": "data_missing_fail_fast | not_selected_zero_score",
  "tie_breaker": ["fusion_score desc", "support_count desc", "best_package_rank asc", "symbol asc"]
}
fusion_policy_sha256 = sha256(canonical_json(fusion_policy))
```

**独立证据字段**（写入 `selection.daily_selection_evidence.evidence_payload_json`，并由 `advisory_daily_review.evidence_id` 引用）：

- `package_raw_scores`: 每个 package 的原始 score，仅用于解释、诊断、质量报告，不直接跨包比较。
- `package_raw_ranks`: 每个 package 的原始 rank。
- `package_rank_scores`: 每个 package 归一化后的 rank score。
- `package_presence`: `selected_topK` / `not_selected_in_full_evidence` / `eligibility_excluded` / `data_missing`。
- `package_weights` 与 `normalized_package_weights`。
- `support_count`: `package_presence=selected_topK` 的包数量。
- `rank_dispersion`: 多包 rank 分歧，例如 max(rank)-min(rank) 或 robust dispersion。
- `fusion_score`、`fusion_rank`、`fusion_policy_sha256`。

**第一版融合方法**：默认使用 `weighted_rank_fusion`，而非 raw score 平均：

```text
rank_score_p(symbol) = 1 - (rank_p(symbol) - 1) / max(candidate_count_p - 1, 1)
fusion_score(symbol) = Σ normalized_weight_p * rank_score_p(symbol)
fusion_rank = sort(symbols by fusion_score desc, support_count desc, best_package_rank asc, symbol asc)
```

缺失语义必须区分：

- `not_selected_in_full_evidence`：该 package 有完整 evidence，但该股票未入候选，可记该包贡献为 0 或最低 rank score。
- `data_missing`：该 package 当日数据/产物缺失，不能当作 0 分；必须 fail-fast 或将该 item 记 `WAITING_DATA`，不得静默降低股票排名。
- `eligibility_excluded`：该 package 因停牌/ST/流动性/黑名单等可解释规则排除，必须记录 exclusion reason。

**复评口径**：

- `latest_rank` 在多策略包 advisory 中一律指 `fusion_rank`；单包 rank 只能写入 `package_raw_ranks` / `package_rank_scores` 作为解释证据。
- Top20 入选、Top40 缓冲、rank decay 退出均基于 `fusion_rank`，避免两个 package rank 被混用。
- 独立 package rank 用于解释与二级决策：`CONSENSUS_STRONG`（多包共同靠前）、`SINGLE_PACKAGE_SUPPORT`（单包强推）、`RANK_CONFLICT`（包间显著分歧）、`PACKAGE_DATA_MISSING`（数据缺失）。
- 策略包集合、权重、融合方法、缺失处理策略任一变化，必须生成新的 `fusion_policy_sha256`；历史 episode 不得无标记跨 policy 比较。

**融合池模式 vs 策略袖珍组合模式（sleeve mode）**：

| 模式 | 适用场景 | 复评 rank | UI/报告 |
| --- | --- | --- | --- |
| `fusion_pool`（第一版默认） | 多个策略包同一 horizon、同一股票池、同一荐股目标 | 唯一 `fusion_rank` | 一张主荐股表 + 展开显示 package 证据 |
| `sleeve_mode`（保留设计接口，暂不作为 Phase 2 默认实现） | 策略包 horizon/风格/风险预算明显不同 | 每个 package 各自生命周期 rank；汇总层只做去重/总览 | 分 sleeve 展示，避免强行合成 Top20 |

第一版只要求 `fusion_pool + weighted_rank_fusion` 完整落地；若用户选择 `sleeve_mode`，必须作为新 policy/新实施阶段审批，不得复用 fusion_pool 的验收口径。

**每日复评流程（多策略包）**：

```text
for each trade_date:
  for each package in package_set:
    run Selection Center independently
    evidence must cover package.topK + active_pool symbols

  build per-symbol package evidence
  compute fusion_score/fusion_rank by fusion_policy
  review_universe = active_pool ∪ fusion_topK

  for each active item:
    use latest_rank = fusion_rank
    use package evidence for explanation / rank conflict flags
    call ExitGuard for stop loss / take profit / alpha decay / time stop
    persist advisory_daily_review with fusion_policy_sha256 + per-package evidence_id(s)

  for each new candidate in fusion_top20 - active_pool:
    apply entry hysteresis and replacement budget
    enter only when fusion rank and policy evidence are complete
```

**质量报告新增指标**：

- `overlap_at_20` / `overlap_at_40`: 多策略包候选重合度。
- `rank_corr`: 包间 rank 相关性。
- `consensus_bucket_return`: 多包共识股表现。
- `single_package_support_return`: 单包支持股表现。
- `rank_conflict_bucket_return`: 包间强分歧股票表现。
- `fusion_vs_single_package`: 融合池相对各单包 TopK 的收益、回撤、换手、退出原因差异。
- `turnover_by_fusion_policy`: 不同 package 权重/缺失策略的换手差异。

该规则参考成熟量化组合实践：多模型/多因子组合通常先做因子标准化、rank/score fusion 或 sleeve portfolio construction，再由组合层统一控制权重、换手、风险与解释；当 raw score 标尺不可比时，rank fusion 比直接分数平均更稳健。MSCI 多因子指数、Black-Litterman views、多 alpha portfolio construction、Reciprocal Rank Fusion、transaction-cost-aware rebalancing 均可作为后续 Phase 3/4 质量报告与 QE 验证的参考。

#### 5.5 荐股生命周期回放（Advisory Replay）与荐股收益记录

**设计决议**：Selection Center 已允许选择历史时间点，Phase 3 需要把它扩展为“荐股生命周期回放”：从任意历史交易日（例如 1 个月前的交易日）开始，逐日按 PIT（point-in-time）口径运行选股、复评、退出与补位，重建当时能够看到的荐股列表演化，并为每个策略包/融合策略记录 episode 级荐股收益。该能力仍是 advisory retrospective，不写 Paper v2 ledger、不模拟账户、不宣称 validated PnL。

**时间线口径**：默认把“2 日荐股选出 3 日股票”解释为：2 日收盘后或盘后任务生成信号，3 日成为可参考/可执行的推荐列表。

```text
signal_date = T                         # 选股信号生成日，数据 cutoff 不晚于 T 日收盘后已可用时间
effective_entry_date = next_trading_day(T)
entry_price_basis_default = next_open_executable

for each signal_date T in replay window:
  run each StrategyPackage with PIT cutoff(T close)
  if package_set: build weighted_rank_fusion evidence and fusion_rank
  apply pending exits whose exit_effective_date == T open
  review active_pool with T close evidence and ExitGuard decisions
  schedule EXIT decisions for next_trading_day(T) open unless T+1/suspend/limit blocks execution
  fill vacancies from today top20/fusion_top20 by hysteresis + replacement_budget
  schedule new ENTER decisions for next_trading_day(T) open
  persist daily snapshot + episode evidence + package/fusion diagnostics
```

**价格基准决议（收益计算）**：默认使用 **3 日开盘价 / `next_open_executable`** 作为入选收益的基准价；2 日收盘价和 3 日收盘价只作为辅助敏感性口径。

| 价格基准 | 是否作为默认收益基准 | 原因 |
| --- | --- | --- |
| 2 日收盘价（`signal_close`） | 否，只作信号参考/close-to-close 诊断 | 若策略用到了 2 日收盘价、成交量、因子或盘后数据，2 日收盘价在决策完成后不可成交；用它计算荐股收益会把无法执行的价格当成入场价，容易产生前视/过度乐观偏差。 |
| 3 日开盘价（`next_open_executable`） | **是，默认** | 这是 2 日信号发布后第一笔清晰、可复现、理论上可执行的日频价格；与“盘后出荐股、次日生效”的用户体验一致，也最容易和后续 QE/Paper 的可执行口径对齐。 |
| 3 日收盘价（`next_close`） | 否，只作保守/延迟执行敏感性报告 | 它跳过了 3 日整天的涨跌，若荐股在 3 日开盘前已经可见，会漏掉首日收益；仅适合“用户只能在 3 日收盘后才看到或才允许行动”的特殊场景。 |
| 3 日 VWAP / 前 30 分钟 VWAP（`next_vwap`/`next_open_window_vwap`） | 可选增强口径 | 当分钟线质量足够、需要更贴近可成交均价时使用；必须同时记录成交量上限、停牌/涨跌停不可成交、滑点假设，不替代默认口径。 |

**退出价格口径**：若某 episode 在 `exit_signal_date = X` 的复评中被淘汰，默认收益用 `effective_exit_date = next_trading_day(X)` 的 `next_open_executable` 作为退出价；若 X 当日是入选首日且触发止损/止盈，遵守 A 股 T+1，记 `STOP_LOSS_DEFERRED_T1` 或 `EXIT_DEFERRED_T1`，不得按 X 日价格假装已退出。回放结束仍未退出的 episode 记录 `still_active=true`，用 `end_signal_date` 的收盘价做 mark-to-market 未实现收益，并在报告中与已实现收益分开展示。

**收益记录语义**：

- `episode_return_bps = adjusted_exit_price / adjusted_entry_price - 1`；所有 entry/exit/mark 价格必须按复权因子调整到同一口径，同时保留原始价与 factor，便于复核。
- **荐股胜率（recommendation win rate）**：`win_rate = win_episode_count / evaluable_episode_count`，其中 `win_episode_count = count(is_win=true)`，`is_win = return_bps > 0`；`evaluable_episode_count` 是报告计算时的内部派生分母，只统计已进入荐股且有有效 entry price，并有有效 exit price 或期末 mark price 的 episode。收益为 0 的 episode 记 `flat_episode_count`，默认不计入 win，但计入分母；停牌、涨跌停不可成交、缺价导致无法确定 entry/exit/mark 的 episode 不进入分母。该分母及排除数量不作为 leaderboard / program snapshot 记录字段。
- 胜率必须至少输出三种口径：`realized_win_rate`（仅已退出 episode）、`mark_to_market_win_rate`（回放结束仍持有 episode，用期末 mark 价）、`all_episode_win_rate`（已退出 + 仍持有 mark）；未来评估策略包实盘效果时默认看 `all_episode_win_rate`，并同时展示样本数、平均/中位收益、最大回撤，避免单独胜率掩盖赔率不足。
- 每个 StrategyPackage 独立生成 `advisory_replay_run` 与 `advisory_episode_return`；多策略包融合另生成 fusion run，不得把 fusion episode 的收益重复计入单包收益。
- 同一股票退出后再入选必须生成新 `episode_id`；同一股票被多个包选中时，以 `run_id + episode_id` 区分，不做隐式去重。
- `price_quality_status` 必须显式记录 `filled_at_next_open` / `limit_up_unfillable` / `limit_down_unfillable` / `suspended` / `missing_open_price` / `deferred_to_next_tradable`，不得用缺失价、涨跌停不可成交或停牌日价格伪装收益。
- 报告层同时展示单只 episode 累计涨幅、`is_win`、胜率纳入口径、持有交易日、最大浮盈、最大回撤、退出原因，以及策略包层的胜率、胜/平/负数量、平均/中位收益、Top20 池日收益、换手率、仍持有未实现收益。
- 所有结果必须标注 `post-decision diagnostics`：它是荐股列表维护质量与价格建议质量的历史诊断，不等同 QE A/B 或 Paper v2 账户收益。

**UI/报告要求**：Paper v2 Advisory 页面应提供“生命周期回放”入口：选择单策略包或策略包组合、起止交易日、价格基准（默认 `next_open_executable`）、是否输出敏感性口径；结果分三层展示：每日荐股池快照、episode 收益表、策略包/融合策略汇总报告。

#### 5.6 UI 与配置边界：独立荐股中心 + 选股页快捷入口

**设计决议**：荐股必须有独立 UI 和独立配置，不应把每日复评、生命周期池、收益统计塞进一次性选股 UI。选股 UI 负责“产生候选与信号解释”，荐股 UI 负责“长期运行、每日复评、退出、收益/胜率与质量报告”。

```text
/paper-v2/selection        # Selection Center：临时/研究型选股运行、TopK 结果、候选导出
/paper-v2/advisory         # Advisory Center：荐股任务、独立设置、每日复评、生命周期、收益/胜率
```

**页面顶部排行榜（Advisory Program Leaderboard）**：

Advisory Center 顶部必须先显示“当前执行中的荐股任务排行榜”，覆盖所有 `status in {ENABLED, REVIEWING, WAITING_DATA}` 的 program。默认排序为 `all_episode_win_rate desc`；用户可切换按 `avg_return_bps desc`、`median_return_bps desc`、`enabled_since asc/desc`、`entered_episode_count desc`、`max_drawdown_bps asc` 排序。

| 列 | 定义 | 默认显示 |
| --- | --- | --- |
| 排名 | 当前排序后的 program 排名；小样本应显示样本警示 | 是 |
| 荐股名称/策略包 | `program_name` + 单包名称或多包组合摘要，展开显示 package ids/weights/fusion hash | 是 |
| 模式 | `single_package` / `fusion_pool` / `sleeve_mode_future` | 是 |
| 启用时间 | `enabled_since`，并显示已运行自然日/交易日数 | 是 |
| 累计荐股数 | `entered_episode_count`；同股票退出后再入选算新 episode，可补充 unique symbol 数 | 是 |
| 当前持有数 | `active_count`，反映仍在荐股池中的股票数 | 是 |
| 止盈数 | `take_profit_count`，覆盖 `TAKE_PROFIT_TRAILING` / `TAKE_PROFIT_ALPHA_DECAY` / 其他 take-profit reason | 是 |
| 止损数 | `stop_loss_count`，覆盖 `STOP_LOSS` / `STOP_LOSS_DEFERRED_T1` / hard/soft stop 退出 | 是 |
| 胜率 | 默认 `all_episode_win_rate`；tooltip 展示 realized / mark-to-market / eligible count | 是 |
| 平均涨幅 | `avg_return_bps`，按全部可评价 episode 的 return 计算；支持点击排序 | 是 |
| 中位涨幅 | `median_return_bps`，降低极端值影响 | 建议显示 |
| 平均持有天数 | `avg_holding_days`，解释高胜率是否来自过长持有 | 建议显示 |
| 最大回撤/最大不利波动 | `max_drawdown_bps` 或 episode MAE 聚合，提示风险 | 建议显示 |
| 换手率/替换率 | `turnover_rate`，衡量每日复评维护成本 | 可选 |
| 最近复评状态 | `last_review_status`；样本数/排除样本不作为排行榜记录字段 | 建议显示 |

**排序与状态字段**：

- `eligible_episode_count` / `data_excluded_count` 不作为 leaderboard 或 program metric snapshot 的记录字段；如质量报告或回放明细需要，可由 `advisory_episode_return` 临时计算并只在报告上下文展示。
- `last_review_status`：该 program 最近一次应执行复评的状态，建议枚举为 `OK` / `PARTIAL` / `WAITING_DATA` / `REVIEW_FAILED` / `STALE`。`OK` 表示最近交易日复评完成；`WAITING_DATA` 表示缺行情/证据暂不能判断；`REVIEW_FAILED` 表示任务失败；`STALE` 表示超过预期交易日未复评。
- 默认胜率排序不自动记录或展示 `LOW_SAMPLE`；如后续需要样本量护栏，应由质量报告临时计算，不落入排行榜持久字段。
- 胜率相同时，默认 tie-breaker 为 `avg_return_bps desc`、`max_drawdown_bps asc`、`enabled_since asc`。
- 平均涨幅排序必须同时显示 median return 与 max drawdown，避免少数大涨样本掩盖大多数亏损。
- 行点击进入 program detail：设置、当前荐股池、每日复评记录、收益 episode、质量报告与回放。

**交互边界**：

- Selection Center 页面保留单次运行能力：选择时间点、一个或多个 StrategyPackage、`single_package` / `weighted_fusion` / `intersection` / `union`，查看当次 TopK 与融合诊断。
- Selection Center 只提供“创建/更新荐股任务”的快捷入口：把当前 run 的 package 组合、权重、日期与 universe 预填到 Advisory Center；它不拥有每日复评状态，不直接维护荐股池。
- Advisory Center 提供独立“荐股任务（Advisory Program）”列表和设置页；每个 program 可启用/暂停/克隆/归档，并拥有独立 active_pool、review policy、entry/exit price basis、收益/胜率统计与审计链。
- 同一 StrategyPackage 可以同时参与多个荐股任务；同一股票也可以同时出现在多个 program 的荐股池中。UI 必须按 `program_id` 隔离展示，不能跨 program 默认去重或合并收益。

**每个荐股任务的策略包模式**：

| 模式 | package 数量 | canonical rank | UI 设置 |
| --- | ---: | --- | --- |
| `single_package` | 1 | 单包 rank | 选择 1 个 StrategyPackage；保留 package evidence，`fusion_policy_sha256` 可为空或记录单包 policy hash。 |
| `fusion_pool` | 2+ | `fusion_rank` | 选择多个 StrategyPackage、权重、candidate_top_k、缺失证据策略、tie-breaker；默认 `weighted_rank_fusion`。 |
| `sleeve_mode`（未来） | 2+ | 每个 sleeve 独立 rank | 仅保留设计入口；不同 horizon/风格明显不同的包不强行融合，需单独审批和验收。 |

**并行运行语义**：

```text
for each enabled advisory_program:
  load program.version + package_set + review_policy
  run single_package or fusion_pool selection independently
  write daily_selection_evidence with program_id/package_set_hash/fusion_policy_sha256
  review only this program's active_pool
  persist advisory_daily_review / replay / episode_return under this program_id
```

一个 program 失败只能把该 program 标为 `REVIEW_FAILED` 或 `WAITING_DATA`，不得阻断其他 program 的每日复评；UI 要展示每个 program 的最新运行日、状态、失败原因、样本数、胜率、平均收益和仍持有数量。

---

## 6. 实施阶段（粗粒度 5 阶段 + 严格验收）

> 每阶段：目标 / 范围（做与不做）/ 复用 / 交付物 / **验收标准（客观可测）** / **审核 checklist** / 进入下一阶段的门槛。
> 通则验收（每阶段都查）：① 无 silent fallback（搜索 except:pass/默认值成功路径 + 针对性测试）；② 缺关键输入 fail-fast；③ 新增字段/表向后兼容，现状回归不变；④ 单测 + 集成测试齐全且通过。

### Phase 0 — 契约与核心 evaluator（基础设施，一次完整）

- **目标**：交付 §4 全部契约 + rule_v1 实现，作为后续所有阶段的唯一底座。
- **范围-做**：`PriceGuardContext`/`ExitGuardContext` dataclass（全字段）；`price_guard.py`/`exit_guard.py` 纯函数（rule_v1 实现，buy+sell+green/yellow/red+全 reason code）；policy schema 三态 mode 槽位（rule/bucket/ml 接口在场，后两者可 `NotImplemented` 占位）；扩展 `ALLOWED_POLICY_JSON_KEYS` 加 `price_guard`/`exit_guard` + validator；`signal_ref_price` 命名决议（★1）。
- **范围-不做**：不接 QE、不接 Paper v2、不接选股 UI、不实现 bucket/ml。
- **验收标准（客观）**：
  1. evaluator 为纯函数：静态检查无 I/O 导入；同一 context 输入两次输出一致。
  2. 单测覆盖：green/yellow/red、SKIP_OPEN_GAP_EXCEEDED、REDUCE_YELLOW、near_limit、missing signal_ref_price → fail-fast、price_basis mismatch → fail-fast。
  3. policy hash 稳定性测试：相同 policy_json → 相同 sha256；"缺省" vs `{enabled:false}` → 不同 hash、相同行为。
  4. validator 拒绝未知字段、拒绝 `algo_config` 内夹带 `max_open_gap_bps` 等越权键。
  5. `signal_ref_price` 命名决议文档化（含对 `selection.package_result.reference_price` 现有语义的结论）。
- **审核 checklist**：纯函数性、reason code 全集在场、三态 mode 接口在场（不缺槽位）、向后兼容（现存 policy 不受影响）、★1 已解决。
- **门槛**：以上 5 条全绿 + 我签核。

### Phase 1 — 选股买入区间 + 止损区间展示（能力 B，纯展示）

- **目标**：现有选股结果从"symbol+score+rank+weight"增强为"+ 推荐理由 + 建议买入区间(green/yellow/red) + 建议止损区间(soft/hard)"。
- **复用**：`selection.package_result` 增列；Selection Center service/repo/router；前端选股页。
- **范围-做**：调用 Phase 0 evaluator 由 `signal_ref_price` + alpha budget 生成 `suggested_entry_price_band`/`suggested_stop_loss_zone`；标注 `range_source`（`alpha_budget_based`/`cost_budget_based`）、`reference_source`、`price_basis`、`policy_sha256`、`guidance_status=rule_default`；tick rounding；并显交易所涨跌停边界。
- **范围-不做**：不下单、不进 watchlist 生命周期（Phase 2）、不改 alpha score/rank、不进 QE。
- **验收标准（客观）**：
  1. selection artifact / `selection.package_result` 含全部新字段；对一批历史选股结果生成区间无报错。
  2. 缺 `signal_ref_price`/`price_basis`/`policy_sha256` → 该候选**降级为"不提供区间"或 fail-fast**，绝不填默认价（针对性测试）。
  3. 展示区间按 A 股 tick 取整；同时显示涨跌停边界；区间与委托价的免责标注存在（"最终委托价由后续 PriceGuard/执行确认"）。
  4. `guidance_status` 一律 `rule_default`，无任何 `qe_validated`（断言测试）。
  5. 不产生任何 OMS/broker 调用（搜索 + 测试）。
- **审核 checklist**：字段完整、fail-fast、无默认价、交易所边界并显、未越界标 validated、无下单。
- **门槛**：上述 + 我签核。

### Phase 2 — 自选池 + 每日复评荐股生命周期（能力 C，advisory）

- **目标**：实现 §5 状态机与每日复评闭环，"观察选股/荐股效果"的主场。
- **复用**：`app.watchlist_items`（扩列 `status`/`exited_at`/`exit_reason`/`planned_entry`/`actual_entry` 或新增 `app.advisory_lifecycle` + `app.advisory_daily_review` 表，二选一在 Phase 2 设计评审定）；`/to-watchlist` 端点；`selection.daily_selection_evidence`；Phase 0 `exit_guard` evaluator。
- **范围-做**：状态机 CANDIDATE→ENTERED→HOLDING→EXITED；独立 Advisory Center UI 与 `advisory_program` 设置，Selection Center 只作为候选/快捷创建入口；每日复评 job（读当日 daily_selection_evidence + watchlist → action+reason+evidence 落库）；Top20 重跑但不全量覆盖荐股池，采用 `active_pool ∪ topK` 合并复评、入选/退出阈值分离、替换预算；多策略包默认采用 `fusion_pool + weighted_rank_fusion`，保留 per-package rank/score evidence，并以 `fusion_rank` 作为 lifecycle canonical rank；每个 program 支持 `single_package` 与 `fusion_pool`，多个 program 可并行运行且状态隔离；ExitGuard 覆盖 stop loss、trailing/take profit、alpha/rank 衰减、time stop；T+1 标注（★4）；advisory 退出仅改 status，不成交。
- **范围-不做**：**绝不**写 Paper v2 ledger、**绝不**下单；不依赖 QE 验证（advisory 可先用 rule_default policy）。
- **验收标准（客观）**：
  1. 状态机迁移完备且单测覆盖全部合法/非法迁移。
  2. 每日复评对一段历史区间逐日运行，每个 item 每个交易日产出一条 `advisory_daily_review`（action+reason+evidence+policy_sha256）。
  3. 当日建仓 + 当日触发硬止损 → 记 `STOP_LOSS_DEFERRED_T1`，status 不立即 EXITED（针对性测试）。
  4. advisory 路径零 ledger/OMS 写入（代码搜索 + 测试）。
  5. 边界标注：生命周期记录显式标 `layer=advisory`，与 Paper v2 区分。
  6. `advisory_daily_review` 为 append-only；每日 job 对同 (item, trade_date) **幂等可重跑**（重跑产出一致或带 version）。
  7. 除权除息/拆股跨日：`actual_entry_cost` 与 stop/take 价按 `$factor` 调整正确（针对性测试）；停牌 item 记 `WAITING`/carry 而非用陈旧价触发止损。
  8. Top20 复评语义正确：每日新 Top20 不直接全量覆盖旧荐股；旧荐股在 `rank_enter=20`、`rank_exit=40`、`confirm_days=2` 等 hysteresis 规则下保留或退出；普通替换受 `daily_replacement_budget` 限制。
  9. selection evidence 覆盖 `topK + active_pool`；active item 缺 rank/score evidence 时 fail-fast 或记 `WAITING_DATA`，不得静默退出或填默认 rank。
  10. 退出机制覆盖 STOP_LOSS / STOP_LOSS_DEFERRED_T1 / TAKE_PROFIT_TRAILING / TAKE_PROFIT_ALPHA_DECAY / ALPHA_RANK_DROP_EXIT / TIME_STOP / EXIT_ELIGIBILITY；同 code 重新入选必须生成新 episode。
  11. 多策略包复评中，`latest_rank` 明确等于 `fusion_rank`；`package_raw_scores`、`package_raw_ranks`、`package_rank_scores`、`support_count`、`rank_dispersion`、`fusion_policy_sha256` 均进入 evidence；raw score 不得跨包直接平均作为排序依据。
  12. 策略包集合、权重、融合方法或缺失策略变化时生成新的 `fusion_policy_sha256`；历史 episode 不得无标记跨 policy 比较。
  13. UI 存在独立 Advisory Center 设置页，可创建至少两个并行启用的荐股任务；每个任务可选择单策略包或多策略包融合，状态、active_pool、收益/胜率与失败原因按 `program_id` 隔离。
  14. Selection Center 的“创建/更新荐股任务”只能预填 Advisory Program，不得把一次性选股 run 直接当作长期荐股生命周期状态。
  15. Advisory Center 顶部存在当前执行中荐股任务排行榜，默认按胜率排序，支持按平均涨幅/中位涨幅/启用时间/累计荐股数/最大回撤切换排序。
  16. 排行榜每行展示启用时间、累计荐股数、当前持有数、止盈数、止损数、胜率、平均涨幅，并只自动展示最近复评状态 `last_review_status`；`eligible_episode_count` / `data_excluded_count` 不作为排行榜记录字段。
- **审核 checklist**：状态机正确、独立 Advisory UI 设置正确、多个 program 并行隔离、顶部排行榜统计正确、每日复评幂等可重跑、T+1 正确、复权/停牌处理正确、零成交/零 ledger、与 Paper v2 边界清晰、reason/evidence 齐全。
- **门槛**：上述 + 我签核。

### Phase 3 — 区间/止损质量回顾评估（retrospective，"看效果"的量化）

- **目标**：把 Phase 1/2 的展示与建议变成**可检验的事后质量评估**，作为是否值得进 QE 的判断依据。
- **范围-做**：对历史 advisory 记录计算 §v1-11.4 指标：`entry_zone_hit_rate`、`entry_zone_fillable_rate`、`alpha_if_entered_zone`、`alpha_if_chased_above_zone`、`missed_alpha_if_not_entered`、`soft/hard_stop_trigger_rate`、`stop_saved_loss_bps`、`stop_whipsaw_cost_bps`、`reward_risk_realized`；新增 §5.5 `Advisory Replay`，支持从 1 个月前等历史交易日开始逐日选股复审，生成每日荐股池快照、episode 级荐股收益、策略包/融合策略汇总收益；分 score/gap/regime/liquidity/board/package/fusion_policy/entry_price_basis 桶；桶最小样本阈值 + 不足向父桶收缩（★5）。
- **范围-不做**：不据此自动启用任何 enforced 规则；不替代 QE A/B。
- **验收标准（客观）**：
  1. 质量报告产出且分桶；每个结论显式标 **post-decision diagnostics（事后归因，非 validated PnL，含选择偏差与样本量提示）**。
  2. 桶样本不足时有收缩/合并逻辑且报告标注被合并桶。
  3. 报告可复现（固定输入 → 固定输出）。
  4. Advisory Replay 可从指定历史交易日起按交易日逐日运行，严格使用 PIT cutoff；不得读取未来 rank/price/evidence 决定当日荐股。
  5. episode 收益默认使用 `effective_entry_date` 的 `next_open_executable` 与退出生效日 `next_open_executable`；`signal_close` 与 `next_close` 只能作为敏感性列，报告中不得替代默认收益口径。
  6. 停牌、涨跌停不可成交、缺开盘价、T+1 延迟退出必须进入 `price_quality_status` / `t1_note`，不得以默认价格伪装成交或收益。
  7. 单包 run、fusion run、同 code 多 episode 的收益归属可追溯，fusion 收益不得重复计入单包收益。
  8. 质量报告必须输出 `realized_win_rate`、`mark_to_market_win_rate`、`all_episode_win_rate`、`win_episode_count`、`flat_episode_count`、`loss_episode_count`；胜率分母按报告内部“可评价 episode 总数”计算，但 `eligible_episode_count` 与被缺价/不可成交排除的 episode 数不作为排行榜记录字段。
  9. 质量报告必须输出排行榜所需 program-level 聚合指标：胜率、平均/中位涨幅、累计荐股数、止盈/止损数量、当前持有数、最大回撤、平均持有天数；样本数和数据排除数量仅可作为质量报告临时诊断，不进入排行榜持久记录字段。
- **审核 checklist**：指标定义无未来函数泄漏（成交价用当时可观测价）、`next_open_executable` 默认口径与敏感性口径分离，停牌/涨跌停/T+1/缺价处理可审计、归因口径标注、可复现。
- **门槛**：上述 + 我签核。**此处产出的效果读数是是否继续进入 Phase 4 的决策依据。**

### Phase 4 — QE 注入与 A/B enforced 验证（能力 A，enforced 唯一门）

- **目标**：PriceGuard/ExitGuard 在 QE 受控 A/B 中证明价值，产出 `price_guard_policy.json` + `policy_sha256`。
- **复用**：ConfigComposer（NestedExecutor + inner_strategy）；QE config truth tests；Phase 0 evaluator。
- **范围-做**：
  - QE inner strategy 实现为**成交前 order-list modifier**：比较 `deal_price`(raw $open，经 $factor 转 raw) vs `max_buy_price`，不可接受→amount=0(SKIP)/缩放(REDUCE)，旁路记保护价+reason（**v1 ★7：Qlib Order 无 limit_price，limit 是接受阈值不是挂单限价**）。
  - `signal_close` 由 outer 写入、inner 只读（★6）。
  - 三模式 `disabled/shadow/enforced`；A/B 锁定 strategy/model/universe/data/cost/algo/seed，仅变 price_guard；候补对照 `skip_to_cash/buy_next_candidate/proportional_redistribute` 分别归因；与既有涨停预过滤去重（★3）。
  - 校准 `bucket_calibrated` + walk-forward + **PBO/Deflated Sharpe 晋级门**（★5）；`turnover_delta` + 成本 drag 列 A/B 一级指标。
- **范围-不做**：不接 Paper v2 实时；shadow 不得当收益证据。
- **验收标准（客观）**：
  1. QE config truth test 证明请求的 `price_guard.enabled/scope/参数` 真实进入执行路径 YAML 切片。
  2. A/B config diff 证明仅 `price_guard_mode/policy_sha256` 不同，其余完全相同。
  3. 产物齐全：`price_guard_policy.json`+`.sha256`、`ab_baseline_metrics.json`、`ab_price_guard_metrics.json`、`price_guard_decisions.parquet/jsonl`、`ab_comparison_report.md`，全部带 experiment_id/loop_id/数据版本/config hash。
  4. 晋级门：跨 walk-forward 窗口稳定 + 通过 PBO/DSR + 收益/回撤/尾部/成本至少一项稳定净改善且不显著破坏 IR/成交率。
  5. 候补三模式分别报告，价格保护收益与候补选择收益分离归因。
  6. 缺 reference/limit/factor 全 fail-fast，不伪装 SKIP。
- **审核 checklist**：★3/★5/★6/★7 全部落实、A/B 隔离、产物可追溯、晋级门量化、无未来函数。
- **门槛**：上述 + 我签核。**只有通过此门的 policy_sha256 才能进入 Phase 5 enforced。**

### Phase 5 — Paper v2 历史 parity + 实时落地（能力 A enforced）

- **目标**：QE 验证过的 policy 在 Paper v2 完美复用，先历史 parity，再 shadow/guarded_sim，最后 enforced_candidate。
- **复用**：day_runner（intents 后、create_order 前插 PriceGuard）；`OrderIntent.LIMIT/limit_price`；vn.py-style 限价语义；runtime override 拒绝。
- **范围-做**：历史 parity replay（同 hash 逐 symbol/date/side 对齐 decision/reason/保护价/数量）；shadow→guarded_sim→enforced_candidate 三步；**实时限价成交模型独立验证**（排队/部分成交/未触及不成交、fill-probability、集合竞价 indicative_open 可得性）——**v1 ★8：parity 只保证历史决策一致，实时成交行为须独立验证，不得用 QE 证据替代**。
- **验收标准（客观）**：
  1. parity checklist 全绿（policy hash / market context / decision / reason / guard price(tick 容差) / quantity / failure mode 一致）；不一致禁止进 enforced。
  2. runtime_config 覆盖执行策略被拒（复用既有测试）。
  3. 实时缺 quote/bar → `WAITING_FOR_PRICE_GUARD_INPUT`，非成功非失败。
  4. 实时限价成交行为有独立测试覆盖（部分成交/未触及/排队），并与 QE 全额成交假设的差异有文档化结论。
  5. enforced_candidate 仅接受通过 Phase 4 门 + 历史 parity 的 policy_sha256；每个 rejected/reduced intent 落 QE 兼容 decision row。
- **审核 checklist**：parity 一致、★8 实时差异已验证并文档化、override 拒绝、WAITING 语义、审计链完整。
- **门槛**：上述 + 我签核。

### 6.6 全功能验收矩阵（禁止简化版交付）

本节是代码开发与合入前的强制验收索引。允许分阶段交付，但每个阶段声明完成时，必须满足对应功能的完整定义、功能验证方案和客观验收标准；不得以“先做最小版 / 子集版 / 占位版 / mock-only / 仅后端无 UI / 仅 UI 不接真实 API / 只支持单策略包 / 只支持一个荐股任务 / 暂不处理失败态”等方式宣称完成。

**全局硬性验收（所有功能共同适用）**：

- 所有新增 UI 必须接真实 API；禁止用静态 JSON、mock 数据或只读占位卡片冒充功能完成。
- 所有新增 API/Job 必须有真实持久化或明确的只读派生口径；禁止用空数组、默认成功、默认价格、默认 rank、默认 policy hash 伪装成功。
- 所有业务失败必须结构化返回 `WAITING_DATA` / `REVIEW_FAILED` / `*_DATA_ERROR` / `*_CONFIG_ERROR` 等可审计状态；禁止 silent fallback。
- 所有新增表/字段若进入 DDL，必须有 PostgreSQL comment、幂等 migration、回滚/重跑语义和生产 DDL gate 记录。
- 所有每日/历史复评必须 PIT：不得读取未来价格、未来 rank、未来 evidence 决定当日荐股或退出。
- Advisory 层永不写 Paper v2 ledger、永不调用 OMS/broker、永不产生模拟/真实成交；如触碰 ledger/订单路径，本阶段验收失败。
- 每个功能完成前必须产出“设计条款 -> 实现位置 -> 测试/截图/API/DB 证据 -> 结论”的验收矩阵；缺一项不得合入 main。

| 主功能 | 完整功能定义（不得简化） | 功能验证方案 | 可合入验收标准 |
| --- | --- | --- | --- |
| Advisory Center 独立 UI | `/paper-v2/advisory` 是独立荐股中心，包含 program 列表、设置、排行榜、当前荐股池、复评记录、收益/胜率、质量报告和回放入口；Selection Center 只能作为候选/快捷创建入口 | Playwright 或等价 E2E：打开 Advisory 页面，创建/查看 program，进入详情各 tab；截图或 DOM 断言覆盖真实 API 数据 | UI 非 mock，导航存在，至少一个真实 program 可完整展示；Selection 页面快捷入口只预填 Advisory Program，不直接维护 lifecycle 状态 |
| Advisory Program 设置 | 支持 `program_name`、启用/暂停/归档、target_count、review_schedule、entry/exit price basis、review/exit policy、package_mode、package_set/version；配置变更生成新 version | API 单测 + DB/Repo 测试 + UI 表单测试；验证配置变更不覆盖历史 version | 可创建、编辑、暂停、克隆、归档 program；历史 review/episode 引用旧 version 不漂移；StrategyPackage manifest 不被修改 |
| 单策略包荐股 | 每个 program 可选择且仅选择 1 个 StrategyPackage，按单包 rank 生成 Top20、active_pool、review evidence 和收益统计 | 后端单测构造单包 run；API smoke 返回单包 evidence；UI 显示 single_package 模式 | 单包 program 可从创建到每日复评闭环；`fusion_policy_sha256` 为空或为单包 policy hash；不依赖多包代码路径伪装 |
| 多策略包融合荐股 | 每个 program 可选择 2+ StrategyPackage，使用 `weighted_rank_fusion` 生成唯一 `fusion_rank`，保留每包 raw score/rank/rank_score/presence/support/dispersion | 后端融合单测、API 集成测试、UI 权重编辑测试；构造 rank 冲突、缺包 evidence、单包缺失场景 | raw score 不直接平均；Top20/Top40/alpha decay 均使用 `fusion_rank`；per-package evidence 可展开审计 |
| 多 program 并行运行 | 多个 enabled program 可同日独立复评；program 失败不能阻断其他 program；同一股票可在不同 program 中独立 episode | Job/service 集成测试同时运行 2 个 single + 1 个 fusion program；故意让一个缺数据 | 每个 program 的 active_pool、review、returns、status 均按 `program_id` 隔离；失败 program 标 `WAITING_DATA` 或 `REVIEW_FAILED`，其他 program 正常完成 |
| 顶部排行榜 | Advisory Center 顶部展示所有执行中 program，默认按胜率，可按平均涨幅/中位涨幅/启用时间/累计荐股数/最大回撤切换；只自动保留 `last_review_status`，不记录 `eligible_episode_count` / `data_excluded_count` | API/selector 测试 + UI 排序测试；构造不同胜率、涨幅、启用时间、失败状态 | 列包含启用时间、累计荐股数、当前持有数、止盈数、止损数、胜率、平均涨幅、last_review_status；排序稳定可复现；无 LOW_SAMPLE 自动记录字段 |
| Top20 生命周期复评 | 每日重跑 Selection Center，但不得全量覆盖旧荐股；使用 `active_pool ∪ topK`、hysteresis、replacement_budget | 单元测试覆盖 rank=19/21/41、连续确认、replacement_budget、active_pool 不在 top20 | 旧荐股不会因单日跌出 Top20 被无理由替换；rank 缺失 fail-fast 或 WAITING_DATA；每日 review append-only 且幂等 |
| 状态机 | `CANDIDATE -> ENTERED -> HOLDING -> EXITED` 全迁移；同 code 重新入选生成新 episode | 状态机单测覆盖合法/非法迁移、重复 enter、exit 后重入 | 非法迁移被拒绝并给出 reason；episode_id 可追溯；状态更新不覆盖每日事实 |
| ExitGuard 退出 | 覆盖 hard/soft stop、trailing take profit、alpha decay take profit/rank drop、time stop、eligibility/ST/delist、T+1 deferred | ExitGuard 纯函数测试 + lifecycle 集成测试 + T+1 场景 | 每种退出 reason 均可触发并落库；当日入选当日止损仅标 deferred；退出不写 ledger/订单 |
| 买入区间/止损/止盈展示 | 展示 entry band、stop/take/trailing 区间、guidance_status、policy_sha256、reason；未验证不得标 `qe_validated` | API contract 测试 + UI 展示测试 + 缺字段 fail-fast | 缺 price/limit/suspend/pre_close 不显示默认成功；UI 明示 rule_default/bucket/qe_validated；不会修改下单行为 |
| 每日 evidence 持久化 | `selection.daily_selection_evidence` 覆盖 `topK + active_pool`，包含单包/多包 evidence、fusion hash、feature availability | DB/repo 测试 + 重跑幂等测试 | evidence 不可变或带 version；active item 缺 evidence 不静默 HOLD；policy/hash 可复核 |
| Replay 回放 | 从任意历史交易日逐日 PIT 运行选股、复评、退出、补位，输出每日快照、episode 收益和 program 汇总 | 固定 fixture 回放 1 个月交易日；断言无未来日期读取；重复运行结果一致 | 可选择 start/end date、单包/多包 program、价格口径；未退出 episode 与已退出分开展示；不写 ledger |
| 收益记录 | 每个 episode 记录 entry/exit/mark price basis、return_bps、is_win、flat/loss、MFE/MAE、holding days、exit reason；同股票重入独立计算 | 后端收益计算单测：next open、next close sensitivity、停牌/涨跌停、复权 | 默认 entry/exit 使用 `next_open_executable`；复权一致；缺价不伪装收益；胜率分母内部派生不进入排行榜记录字段 |
| 质量报告 | 输出 post-decision diagnostics：entry zone hit/fillable、stop saved/whipsaw、reward-risk、win rate、avg/median return、max drawdown、bucket 统计 | API 测试拒绝未来 outcome 字段作为 decision input；固定输入固定输出；桶样本不足收缩 | 报告明确标注非 validated PnL；支持 package/fusion_policy/entry_basis 分桶；结论可复现 |
| Price basis | 信号日 `T`，默认 `effective_entry_date=T+1`、`entry_price_basis=next_open_executable`；`signal_close`/`next_close` 只作敏感性 | 单测覆盖 T/T+1、停牌顺延、缺开盘价、涨跌停不可成交 | 2 日收盘价不得作为默认可执行收益基准；3 日开盘为默认；所有口径在报告中明示 |
| 公司行动/停牌/ST/涨跌停 | 复权因子调整 entry/stop/take；停牌 WAITING；ST/delist/eligibility 退出；涨跌停不可成交不能伪装 fill | fixture 覆盖除权、停牌、ST、limit up/down、缺 factor | 价格和收益口径不失真；不可成交状态可审计；缺关键数据 fail-fast |
| API 契约 | program CRUD、package binding、daily review run/preview、leaderboard、active pool、episode returns、quality report、replay run 均有 schema | FastAPI route tests + OpenAPI schema/contract tests | HTTP 状态码、错误 payload、字段名稳定；无 mock-only route；权限/参数校验完整 |
| DB/持久化 | 新表/字段 append-only 优先，program 配置版本化，daily_review/return/replay 可追溯 | migration dry-run + repo tests + idempotency tests | DDL 有 comment；重跑不重复污染；历史记录不被 UPDATE 覆盖；生产 DDL gate 明确 |
| 前端错误态 | UI 展示 `WAITING_DATA`、`REVIEW_FAILED`、`STALE`、缺包 evidence、缺价、无 program、无运行记录等状态 | E2E/组件测试覆盖错误态和空态 | 不把空表展示为成功；错误原因可读；可重试/刷新；不会诱导用户以为已完成复评 |
| 无成交边界 | Advisory 全路径不写 Paper ledger、不创建订单、不触发 broker/QMT | 代码搜索 + 单元测试 monkeypatch ledger/order repo 抛错 + 集成测试 | advisory API/job 即使运行 replay/review 也不触碰订单/ledger；任何触碰视为 P0 |
| 合入前验证链 | 变更文件 lint、单测、API 测试、UI E2E、module registry、L0、paper_v2_backend、guardrails、CI | 本地命令记录 + PR CI + 设计验收矩阵 | 所有必须项 green；未覆盖项必须明示 blocked 并不得请求代码合入 main |

**开发分阶段但不简化的执行规则**：

1. Phase 2 可优先交付 Advisory Program、单包/多包复评、排行榜、状态机、退出和真实 UI；但这些功能一旦声明完成，必须满足上表对应完整验收，不得只交付子集。
2. Phase 3 可在 Phase 2 后交付 replay、收益/胜率、质量报告和回放 UI；但不得把“只计算平均收益、没有 episode 明细/价格口径/失败态”的版本视为完成。
3. Phase 4/5 enforced 不属于 advisory UI 合入门；但 advisory 代码不得预埋会改变 Paper/QE 行为的默认启用路径。
4. 若实现过程中发现任一矩阵项当前阶段不能完成，必须更新 Issue scope 和 PR 说明为 `blocked/not implemented`，不得隐藏为“后续优化”。


---

## 7. rule vs ML（一次预留，后期不重写）

- 生产基线：**`bucket_calibrated`**（Phase 3/4 校准产出），不是主观经验值；`rule_default` 仅 bootstrap。
- ML：作为 `ml_residual_alpha_v1`/`ml_exit_v1` **candidate policy**，输出仍落 `price_guard_policy.json` 走 Phase 4 同一 A/B 门，须相对 `bucket_calibrated` 有稳定净改善才晋级；缺模型/缺特征 fail-fast，禁止 fallback 到规则后仍标 ML。
- 因 mode 槽位在 Phase 0 即在场，加 ML = 新增 policy 文件 + evaluator 分支，**不动架构、不重写**。

---

## 8. 机构/论文依据补充（在 v1 基础上新增三类）

1. 成本侵蚀 alpha：Novy-Marx & Velikov (2016) *A Taxonomy of Anomalies and Their Trading Costs*；Frazzini-Israel-Moskowitz (2018) *Trading Costs* (AQR)。
2. 回测过拟合治理：Bailey & López de Prado *Deflated Sharpe Ratio*；*PBO*。
3. A 股微观结构：涨跌停"磁吸效应"、T+1 与隔夜跳空实证。
（经典执行/定价文献 Perold/Almgren-Chriss/Bertsimas-Lo/Obizhaeva-Wang/三因子/Barra/Alphalens/LEAN/Nautilus/Qlib 见 v1 §6，继续有效。Qlib `Exchange.deal_price` 支持 `$open`/`$close`/`$vwap` 等可执行价格口径并显式处理 limit/volume/suspend；LEAN/QuantConnect 的 MarketOnOpen/MarketOnClose 与 fill model 文档可作为 `next_open_executable`、缺价/过期价格和开盘集合竞价口径的工程参考。）

---

## 9. 待审批决策点

1. 采纳"优先级反转"（能力 B/C advisory 先行，再 QE，再 Paper v2）？
2. 能力 C 数据模型（见 §5.1，**架构建议**）：`app.watchlist_items` 加 SCD 状态字段 + **新增 append-only `app.advisory_daily_review` 日表**（引用 `selection.daily_selection_evidence`），**不**把每日字段加到 watchlist 基表。请确认采纳此实体/事实分离方案。（最终列在 Phase 2 设计评审定，DDL 走既有 gate。）
3. 首个进入 Phase 4 enforced 的策略族：建议 `ScoreWeightedTopkStrategyV2 + V25_1_SMALL_CAP`。
4. 能力 C 每日复评的退出规则第一版：建议 `alpha_decay_exit(rank_drop) + soft/hard stop`，`take_profit` 默认关闭（多因子优先 alpha 衰减退出）。
5. advisory 层是否对用户开放手工建仓价/手工止损：若开放，须作为新 policy 重新验证，不得覆盖原 hash。
6. 多策略包能力 C 第一版是否采纳 `fusion_pool + weighted_rank_fusion` 为默认，`sleeve_mode` 仅保留设计接口、暂不进入 Phase 2 默认实现？建议采纳，避免不同 horizon/风格策略被强行合并。
7. 荐股生命周期回放是否采纳 `signal_date=T`、`effective_entry_date=T+1`、默认 `entry_price_basis=next_open_executable`？建议采纳；2 日收盘价仅作信号参考，3 日收盘价仅作延迟执行敏感性。
8. Phase 3 是否新增 `app.advisory_replay_run` 与 `app.advisory_episode_return` 作为 append-only 收益事实？建议采纳；它们只服务 advisory 质量报告，不写 Paper ledger，不作为 validated PnL。
9. 荐股胜率是否定义为“上涨 episode 数 / 可评价 episode 总数”？建议采纳，并同时展示已退出胜率、期末持有 mark-to-market 胜率和全样本胜率，避免单独胜率指标掩盖赔率和回撤风险；`eligible_episode_count` / `data_excluded_count` 可内部计算但不进入排行榜记录字段。
10. 荐股 UI 是否独立于选股 UI？建议采纳独立 Advisory Center：Selection Center 只负责候选/快捷创建，Advisory Center 负责 program 设置、每日复评、生命周期、收益/胜率。每个 program 必须同时支持单策略包与多策略包融合，并允许多个 program 并行运行。
11. Advisory Center 顶部是否默认展示所有执行中荐股任务的排行榜？建议采纳：默认按 `all_episode_win_rate` 排序，并支持按平均涨幅、累计荐股数、启用时间、最大回撤等切换；排行榜只自动保留 `last_review_status`，不记录 `eligible_episode_count` / `data_excluded_count`。

---

## 10. 审核机制（Codex 开发 / 本作者审核）

- 每阶段 Codex 提交后，按本文件该阶段"验收标准 + 审核 checklist"逐条核验；任一不达标退回，**不得跳阶段**。
- 通则红线（任一触发即退回）：silent fallback / 默认值伪装成功 / 缺输入不 fail-fast / advisory 层产生成交或写 ledger / 未验证 policy 标 `qe_validated` / disabled 路径改变现状行为 / 复用 `reference_price` 表达信号价。
- 审核结论落 `docs/architecture/` 对应阶段 review 记录，附通过/退回 + 证据。
