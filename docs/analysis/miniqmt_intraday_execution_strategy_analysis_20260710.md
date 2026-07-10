# MiniQMT 日内执行策略缺陷分析与解决思路

- 日期: 2026-07-10
- 范围: Path S / MiniQMT SIM event_loop(A 路线)日内下单执行层
- 目的: 为后续执行策略的设计与开发提供分析基线与研究依据(供 Codex 设计/开发)
- 状态: 分析基线(analysis baseline);不含最终结论,设计决策需评审后确定
- 关联包: multi_alpha `pkg_ma_8ec5e389fa2c5e484a1ac7e9`(binding slot `ma_8ec5e389_sim_20260703`)
- 已合入的相关执行层修复: BUG-599 / BUG-600 / BUG-604 / BUG-614

---

## 1. 背景与约束(设计前提)

- 资金规模: 1000 万,策略为 **top-k 每日固定数量换仓**(进/出 top-k 的少数股票),**不是全量换仓**。
- **市场冲击可忽略**: 单只订单规模(≈资金/topk,日常换仓更小)相对个股 ADV 占比极小,participation rate 极低 → 冲击成本 trivial。
- **数据粒度**: 数据源(xtquant `get_full_tick`)仅提供 **A 股五档 level-1 快照 tick(约 3 秒刷新)** + 历史 tick 数据集;**无 level-2 全 LOB(十档 + 逐笔委托/成交/委托队列)**。level-2 需付费订阅,当前不具备。
- 执行栈: A 路线 = SimulationLifecycleScheduler(Path S) + MiniQMTExecutionBridge.submit_event_loop_plan + qmt_strategy_ledger(broker 权威)。B(compiler)/shadow 已于 BUG-600 退役,SIM 只走 A。
- v25: 基于强化学习、**分钟线训练**的盘中交易策略,学不同时段买入比例以逼近理论最优价;无固定假设成交价。

---

## 2. 已观测的缺陷(生产 SIM 实测)

### 2.1 每日成交率系统性过低且下滑(核心问题)
top-k 换仓每日成交率(submitted/target):
- 07-07 = 0/25(0%)
- 07-08 = 11/25(44%,且部分靠人工干预)
- 07-09 = 5/31(16%)
- 07-10 = 2/32(6%)

**结论: 当前日内执行无法可靠完成每日 top-k 调仓计划。** 大量订单挂出后不成交。

### 2.2 已定位并已修复的两个直接根因(BUG-614,已合入 1f3da867)
1. **run 状态映射误判**: `lifecycle.py:494` 原 `next_status = INTRADAY_RUNNING if qmt_result.success else FAILED_RETRYABLE`。当 batch_status=SUBMITTING 且有合法 pending(failed=0)时,run 被误置 FAILED_RETRYABLE;而持续 tick 驱动的门 `scheduler.py:6632` 只认 {SUBMITTING, INTRADAY_RUNNING} → pending 算法整天不再被驱动、永不成交。
2. **被动限价定价**: `_limit_price_for_event_loop` / `_limit_price_for_runtime` 用己方最优价(BUY=ask_1、SELL=bid_1)且 timer_iterations=1 不追价 → 价格一偏离参考价即挂单不成交。

BUG-614 修复: 状态机 SUBMITTING+pending→INTRADAY_RUNNING 持续被驱动;定价改 L1 对手价穿价 marketable-limit + 保护带 + tick size/涨跌停约束 + 走 config;14:55 起尾盘激进扫单兜底。**BUG-614 是即时止血,使 A 能确定性完成换仓;本文分析的是更深的执行质量(择时/成交价优化)设计方向。**

### 2.3 更深层的执行质量缺口(本文分析重点,尚未解决)
- **v25 训练/部署频率不匹配**: v25 基于分钟线训练,实盘却按 tick(约3秒快照)驱动执行。文献公认这是部署风险(training/deployment frequency mismatch):分钟线训练时隐含的观测/延迟/成交假设在 tick 频率下不成立。当前把 tick 直接喂分钟线策略,观测频率不一致。
- **执行择时缺乏方法论**: 当前 BUG-614 的 marketable-limit + 尾盘扫单保证"成交",但**下单时机(WHEN)**仍缺乏基于研究的最优择时(仅"提交即试 + 尾盘兜底"),未针对日内价格路径优化成交价。
- **成交价优化未落地**: 冲击可忽略场景下,执行目标应是"在完成换仓前提下逼近理论最优成交价(arrival/日内择时)",当前无此优化。

---

## 3. 研究依据(近 5 年论文 + 机构实践)

### 3.1 冲击可忽略 → 最优执行退化为激进即时成交
Almgren-Chriss 框架下,当订单量×冲击系数→0,最优解退化为立即成交(冲击 trivial,仅剩时机风险,故尽快成交最优)。实证亦显示订单越小,Arrival Price/IS 越优于 VWAP;VWAP 相对 IS 在小单上一致性显著更差(订单需 >~20% ADV 差异才不显著)。

**推论: top-k 小单不应用 VWAP/TWAP 切片(那是大单减冲击工具),应用 arrival/marketable 激进成交。**

### 3.2 tick 级 RL 执行(SOTA,但需 level-2 数据)
- Cheridito & Weiss (2025, Quant Finance): 全 LOB 下单(市价+限价+撤单),actor-critic + logistic-normal,当前方法学标杆;仿真验证。
- Hafsi & Vittori (2024): 高频 LOB RL,用 ABIDES 多智能体仿真器解决"历史数据不反应"问题。
- Byun et al. (2023): PPO+LSTM,韩交所 level-3 真实数据,跨 50 股 + 动态目标量 + 可变时长泛化(最贴近可落地的 tick 版执行 RL)。
- Schnaubelt (2021): PPO 胜 DDQN,IS 最低,比即时市价省成本达 36.9%。
- 共识: PPO/actor-critic 胜纯 DQN;前沿从"拆单量调度"转向"全 LOB 下单";agent-based 仿真器(ABIDES/Queue-Reactive)取代纯历史回测以捕捉冲击反馈。
- **约束: 这些 tick-RL 均需 level-2 十档 + 逐笔数据训练,且多在仿真验证(市场冲击反事实问题)。当前数据仅五档 level-1 → 不满足。**

### 3.3 目标成交价 / 成交概率预告(fill-probability 生存分析)
- Arroyo et al. (2024, Quant Finance): 卷积-Transformer 生存分析估计限价单成交概率,聚焦执行时机而非价格方向。
- KANFormer (2025): 生存分析 + 订单行为数据(提交/撤单/改单),校准更好。
- 价格方向预测(TLOB/LiT 2025)存在,但 Prata et al. (2024) 基准显示 15 个 DL 方向模型在多样数据上性能普遍收敛到很低 → 纯方向预测对执行不可��。
- **约束: 生存模型同样需 level-2 LOB;当前数据不满足。**

### 3.4 频率不匹配的解法(针对 v25)
1. 让实盘频率匹配训练频率: 把 tick 聚合成分钟线再喂 v25(最省事,保住 v25 择时价值,不重训)。
2. 分层 RL: macro(分钟出执行意图)+ micro(tick 落单)分离信号频率与执行频率。
3. 事件驱动(price-based)步进替代固定时间步,降低延迟敏感。

---

## 4. 方案对比(针对本场景: 1000万 top-k 小单 / 冲击可忽略 / 仅五档 level-1)

| 方案 | 数据需求 | 是否适配本场景 | 说明 |
|---|---|---|---|
| VWAP / TWAP 切片 | 分钟/成交量 | ❌ 不需要 | 大单减冲击工具;小单冲击≈0,切片只增时机风险 |
| 全 LOB tick-RL(Cheridito-Weiss/Byun/Hafsi-Vittori) | level-2 十档+逐笔 | ❌ 数据不满足且过重 | 优化冲击 vs 时机权衡,本场景无冲击权衡 |
| fill-probability 生存模型(Arroyo/KANFormer) | level-2 LOB | ❌ 数据不满足 | 穿价即必成,不需预测成交概率 |
| **对手价 marketable-limit(五档 L1)** | **五档 level-1** | ✅ **数据与需求双匹配** | BUG-614 已实现;近乎必成,成本≈半个价差 |
| **v25 分钟线择时(tick 聚合喂分钟线)** | 分钟线 | ✅ 修频率不匹配 | 保住 v25 择时价值,不重训 |
| 尾盘扫单兜底 | 五档 L1 | ✅ 补充 | 保证当日换仓完成;BUG-614 已实现 |

---

## 5. 初步解决思路(供 Codex 设计,非最终决策)

### 5.1 推荐主线(数据/需求双匹配,不过度工程)
1. **执行方式(HOW)**: 保持并完善 BUG-614 的 **L1 对手价 marketable-limit 穿价**(+保护带 + tick/涨跌停约束)。冲击可忽略 → 激进即时成交是最优,五档数据正好够。
2. **执行择时(WHEN)**: 修复 v25 的频率不匹配 —— **把 tick 聚合成分钟线喂 v25**,让 v25 在其训练频率上输出"分时段买入比例/执行意图",执行层按此意图在对应分钟窗口用 marketable-limit 落单。保住 v25 的择时价值,避免分钟训练×tick 部署的失配。
3. **兜底(完成度)**: 保留 BUG-614 尾盘扫单,保证当日 top-k 换仓 100% 完成。
4. **不引入**: VWAP/TWAP 切片、全 LOB tick-RL、fill-probability 生存模型 —— 数据不满足且本场景不需要(冲击可忽略)。

### 5.2 可选增强(仅在满足前提时评估,ROI 需论证)
- 若未来订阅 level-2(付费): 可评估 Byun 式 PPO tick-RL 或 Arroyo 式 fill-probability 生存模型做更细执行。但对 1000万小单规模,ROI 存疑,不建议优先。
- 分层执行架构(macro 分钟 v25 + micro tick 落单): 比"聚合喂分钟线"更彻底,但工程量大,作为中期方向。

### 5.3 需 Codex 在设计阶段回答/验证的问题
1. v25 当前实盘调用的观测输入到底是 tick 还是已聚合分钟线?若为 tick,聚合口径(OHLCV/窗口对齐)如何与训练一致?给代码锚点。
2. v25 输出的"分时段买入比例"如何映射到执行层的下单调度(每分钟目标量)?当前是否已有此接口,还是需新建?
3. marketable-limit 的 cross_ticks / 保护带默认值对 A 股不同价位段(低价股 tick 占比大)是否需要分段?
4. 执行质量度量(TCA): 如何度量实盘成交价 vs 理论最优/arrival?需要哪些落库字段(已有 qmt_strategy.execution_child_order / order_ledger)。
5. 回测口径对齐: 多alpha/v25 回测的成交价假设与实盘执行的一致性如何验证(避免 tracking error)。

---

## 6. 红线与流程约束(设计/开发须遵守)

- 全量实现,禁止简化版/最小实现/临时占位/降级兼容(项目铁律)。
- 不破坏已合入的执行层修复: BUG-599(真下单)/BUG-600(冻结根治+Postgres 增量写)/BUG-604(tick 驱动+pending 修正)/BUG-614(状态机+对手价穿价+尾盘扫单);不回退 shadow/B。
- 错误 loud(reason_code + stage),禁静默 / except:pass / 默认值兜底。
- LIVE 硬锁不变(event_loop 仍 SIM-only)。
- 持久化走真 DB 增量写(对齐 vn.py + 现有 qmt_strategy schema),禁全量落盘 / tmp 占位。
- 数据边界: 当前仅五档 level-1,任何依赖 level-2/逐笔的方案须先确认数据订阅到位,不得假设。

---

## 7. 参考文献

- Perold (1988), The Implementation Shortfall: Paper vs. Reality
- Almgren & Chriss (2000), Optimal Execution of Portfolio Transactions
- Nevmyvaka, Feng & Kearns (2006), Reinforcement Learning for Optimized Trade Execution
- Schnaubelt (2021), Deep RL for optimal placement of cryptocurrency limit orders (EJOR)
- Byun et al. (2023), Practical Application of Deep RL to Optimal Trade Execution (MDPI)
- Arroyo et al. (2024), Deep attentive survival analysis in LOBs: estimating fill probabilities (Quant Finance)
- Hafsi & Vittori (2024), Optimal Execution with RL in a Multi-Agent Market Simulator (arXiv 2411.06389)
- Cheridito & Weiss (2025), RL for Trade Execution with Market and Limit Orders (arXiv 2507.06345)
- KANFormer (2025), Predicting Fill Probabilities via Survival Analysis in LOBs (arXiv 2512.05734)
- Prata et al. (2024), benchmark of 15 DL models for LOB stock prediction
