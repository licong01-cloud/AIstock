# PriceGuard / ExecutionAcceptance 设计方案审核意见

> 日期：2026-06-02
> 审核对象：`docs/architecture/qe_paper_price_guard_execution_acceptance_design_20260601.md`（commit 2275a6d5）
> 性质：设计评审（不含代码实现）
> 核验范围：AIstock 主仓 `F:\Dev\AIstock` 集成点；Qlib 安装版 `…/envs/AIstock/Lib/site-packages/qlib`

---

## 0. 结论摘要

1. **方向正确，可进入实施**：把问题定位为"选股层与执行层之间缺失的价格接受层（pre-trade acceptance / decision-price gate）"是对的，与 LEAN / NautilusTrader / Qlib 分层一致，且填补的是 Qlib **确实没有**的能力（见 §2.3）。
2. **文档对代码库的 10 项集成假设全部核验属实**（ValidatedExecutionPolicy、ALLOWED_POLICY_JSON_KEYS、MinuteExecutionPolicy、OrderIntent.LIMIT、Paper v2 day_runner 时序、MinuteExecutionEngine、vn.py-style 限价语义、ConfigComposer NestedExecutor、runtime override 拒绝）。`backend/services/trading_core/` 目录已存在，落点合理。
3. **采纳用户意见：不做"简化版最小闭环"**。改为 **"契约/架构一次设计完整 + 验证启用分阶段"**（见 §2.4、§6）——避免推倒重写与功能缺失。
4. **必须在写第一行代码前拍板的 8 个关键缺口**（★1–★8，见 §3），其中两个是 Qlib 调研后新增的硬约束：Qlib `Order` 无 `limit_price` 字段、QE 成交模型与实时限价模型语义不一致。
5. **rule vs ML 不是二选一**：生产基线用 **bucket_calibrated（历史分桶校准）**，ML 作为 candidate policy 必须在 QE A/B 中跑赢校准规则才能晋级；policy 接口必须从第一天起就能容纳 rule/bucket/ML 三种 mode（这正是"避免重写"的关键，见 §2.2、§2.4）。

---

## 1. 事实核验结论（grounding）

### 1.1 AIstock 集成点（全部 CONFIRMED）

| 文档假设 | 核验结果 | 证据 |
| --- | --- | --- |
| `ValidatedExecutionPolicy` 含 `policy_json` + `policy_sha256` | ✅ | `backend/services/strategy_package/execution_policy.py:78-126`，`_normalize_and_hash` 计算并校验 hash |
| `ALLOWED_POLICY_JSON_KEYS` 白名单存在，**不含** `price_guard` | ✅ | 同文件 `:25-36`；`normalize_execution_policy_json()` `:48-75` 拒绝未知字段 |
| `MinuteExecutionPolicy` 含 algo_code/algo_config/data_requirements/quality_report | ✅ | `backend/services/strategy_package/models.py:158-185`、`:253` |
| `OrderIntent` 支持 LIMIT + limit_price | ✅ | `backend/services/trading_core/models.py:19-85` |
| Paper v2 day_runner 时序 signal→risk→targets→intents→execution | ✅ | `paper_trading_v2/day_runner.py:291/311/396/434/570/572` |
| `MinuteExecutionEngine.max_participation_rate` | ✅ | `trading_core/minute_execution.py:61-111` |
| vn.py-style TWAP/SNIPER/BEST_LIMIT 限价语义 | ✅ | `execution_algos/vnpy_style/registry.py:124-157` + 各 core |
| QE ConfigComposer NestedExecutor + inner_strategy.kwargs | ✅ | `quantevolver/config_composer.py` 分钟线分支 |
| Paper v2 runtime_config 禁止覆盖执行策略 | ✅ | `day_runner.py:2590-2604` `_reject_raw_execution_overrides`，`:189` 调用 |
| 无 price_guard/exit_guard 命名冲突 | ⚠️ 部分 | 三者均无；**但 `reference_price` 已存在**（selection order builder + DB schema，含义=成本/mark price）→ 见 ★1 |

### 1.2 Qlib 能力分类（决定"填空 vs 重复造轮子"）

| 能力 | Qlib 现状 | 证据 |
| --- | --- | --- |
| 交易所涨跌停/停牌/成本/deal_price 可行性 | **BUILT-IN** | `exchange.py:187-190,338-376` `check_stock_limit`，cost model |
| **每订单限价 limit_price** | **ABSENT** | `decision.py:37-72` `Order` 仅 amount/direction/start_time/end_time，无 limit_price |
| NestedExecutor 外层日频/内层分钟 hook | **BUILT-IN** | `executor.py:310-499`（外层 decision + trade_range 传入内层）|
| 分钟级执行 RL（如何拆/择时单笔母单） | **BUILT-IN** | `rl/order_execution/`（SAOE，奖励=PA vs TWAP）= 你的 V24/V25 层 |
| **组合级买入价接受门（gap 吃掉 alpha 就跳过）** | **ABSENT** | `signal_strategy.py:117-294` TopkDropoutStrategy 仅 `forbid_all_trade_at_limit` 二元开关，无价格接受逻辑 |
| acceptance/max_chase/open_gap/arrival_price/reference_price/residual_alpha | **ABSENT** | 全 qlib 树零匹配 |

---

## 2. 对用户四个核心问题的回答

### 2.1 PriceGuard 能否独立于日频策略与日内执行策略？还是必须整合？

**答：应当是逻辑独立的"决策修改器（decision modifier）"中间层，但通过明确契约与上下游耦合——既不是孤立系统，也不是揉进任一层。**

- **对日频策略 alpha 逻辑：完全独立。** 不改 score/rank/target_weight。只**消费**日频层产出的两样东西：`signal_ref_price`（决策基准价）与 `expected_alpha_budget`（剩余 alpha 预算，**按策略族参数化**）。
- **对日内执行算法：完全独立。** 不改 TWAP/V25/V25.1 的拆单逻辑。只**产出** `decision(ACCEPT/REDUCE/SKIP/WAITING) + 保护价 + size_multiplier`，由执行算法消费。
- **但它不能脱离这两个接口存在**：`max_chase_bps` 由 `expected_alpha_bps` 推导，是策略族相关的。所以正确定位是 **"独立模块 + 每策略族一份 policy"**，而非"与策略无关的通用黑盒"。

**结论：独立成层（可单测、可复用、可禁用），在两个接缝处集成（target/rebalance 之后、execution 之前）。** 这与文档 §5 一致，我确认合理。补充一条硬约束（见 ★2）：core evaluator 必须是**纯函数**（输入 `PriceGuardContext`，输出 decision，零 I/O），QE 与 Paper v2 各写一个"市场上下文 adapter"，但调用**同一个** evaluator——这是"独立但一致"的工程保证，也是 parity 可测的前提。

**Qlib 落地的具体形态（重要）**：在 Qlib 中，PriceGuard 不是"给 Order 加 limit_price"（Order 无此字段），而是 NestedExecutor 内层策略中、在 `deal_order()` 之前的一个 **order-list 改写器**：比较 `deal_price`(raw $open) 与 `max_buy_price`，不可接受则把 `amount` 改 0（SKIP）或缩放（REDUCE），并旁路记录保护价/reason。详见 ★7。

### 2.2 买入价格评估有哪些成熟方式？经验值还是 ML？

**成熟方式谱系（由简到繁，机构均在用）：**

| 方式 | 本质 | 成熟度/出处 | 适配你场景 |
| --- | --- | --- | --- |
| 1. 规则/启发式门 | open-gap 阈值、max chase bps、距涨停 | 最广泛、最易治理 | bootstrap 起步用（`rule_default`）|
| 2. TCA / arrival-price（implementation shortfall）| 用决策价→成交价的滑点预算判断"还值不值得" | **机构标准**（Perold 1988；CFA TCA）| **概念骨架**：`max_buy_price` 即由此生成 |
| 3. 历史分桶校准（统计经验，非主观）| forward-alpha by (gap×score×regime) 桶，稳健统计+收缩 | Alphalens/Novy-Marx-Velikov 风格 | **推荐的生产基线** |
| 4. 最优执行理论 | 成本/风险前沿、交易速度 | Almgren-Chriss / Bertsimas-Lo / Obizhaeva-Wang | 提供 buffer 项，偏"如何执行"而非"是否接受" |
| 5. ML 接受模型 | residual-alpha 回归 / accept-skip 分类 / 分位尾部 / uplift | 上限高，需 feature contract + walk-forward + 无泄漏 | **candidate policy**，须跑赢规则 |
| 6. RL | 上下文 bandit / 策略学习 | Qlib RL 只做**执行**(SAOE)，不做接受 | 小资金不推荐早期上 |

**裁决：不是"经验值 vs ML"二选一，而是三层 mode 共存于同一 policy 接口。**

- **纯主观经验值不可作为生产**——只能作为 `rule_default` 引导 QE shadow 起步。
- **生产基线 = `bucket_calibrated`**（方式 3）：经验数据驱动、稳健、可解释、契合 A 股小资金 + 你换手敏感的 QE 目标（[[pending_qe_r2_9a35_20260531]] n_drop/换手是核心杠杆）。
- **ML = `ml_residual_alpha_v1`**（方式 5）：作为候选 policy，必须在 QE A/B 中相对 `bucket_calibrated` 有稳定净改善才晋级；缺模型/缺特征必须 fail-fast，不得 fallback 到规则后仍标 ML。

**这恰好支撑用户"避免重写"的诉求**：policy 接口（`price_guard_policy.json` + `policy_sha256` + `mode` 字段）必须**从第一天就为 ML 预留**——加 ML 时只是新增一个 policy 文件 + 一个 evaluator 分支，而**不是改架构**。文档 §10/§12 已有此分层，方向正确，需在 Phase 1 把三种 mode 的接口一次定死。

### 2.3 Qlib 是否已有成熟实现？

**答：Qlib 提供"底座"，但不提供 PriceGuard 的核心语义；PriceGuard 填补真实空白，不重复造轮子。**

- Qlib **已有**：交易所涨跌停/成本/deal_price 可行性层、NestedExecutor 外/内层 hook、分钟级执行 RL（=你的 V24/V25 执行算法层）。
- Qlib **没有**：① `Order` 无 `limit_price` 字段；② **组合级买入价接受门**——`TopkDropoutStrategy` 只有 `forbid_all_trade_at_limit` 二元开关，没有"高开吃掉 alpha 就跳过"的任何逻辑。全树零匹配 acceptance/max_chase/open_gap/arrival_price。

**由此引出两个必须写进设计的实现约束**（详见 ★7、★8）：

- **★7**：QE 侧 PriceGuard 必须实现为"成交前改写 order amount（skip/reduce）"的 decision modifier，**自行比较 deal_price 与保护价**；Qlib 的"limit_price"实际是**接受/拒绝阈值**，不是交易所挂单意义上的限价单。
- **★8**：Qlib 成交模型"可交易即按 deal_price 全额成交" ≠ Paper v2/MiniQMT 真实限价单（挂单/排队/部分成交/未触及不成交）。**parity replay 只能保证历史决策一致，无法保证实时限价行为一致**——这是实时阶段必须独立验证、且不能用 QE 证据替代的点。

### 2.4 关于"完整设计 vs 最小闭环"——采纳用户意见，修订建议

我**撤回**此前"先做简化版最小切片"的建议。改为：

> **架构/契约一次性设计完整；分阶段的是"验证与启用"，而非"构建"。**

具体边界：

- **必须一次设计完整（Phase 1 全部交付，不留半成品接缝）**：
  - `PriceGuardContext` dataclass（raw 价格/limit/gap/alpha budget/board_type/side/sell_reason 全字段）
  - core evaluator 纯函数签名 + 全套 reason code（§14）
  - 买入**与**卖出、auction **与** intraday、green/yellow/red 全路径
  - policy schema 支持 `mode ∈ {rule_v1, bucket_calibrated, ml_residual_alpha_v1}` 三态
  - `ExitGuard` 接口与 schema（即使默认 disabled，接口必须在场，避免后期重写）
  - QE adapter 与 Paper v2 adapter 两个外壳、DB schema 字段（§13.2）一次定义
- **分阶段的只是"哪一策略族、哪些参数先切到 `enforced`、何时迁 Paper v2"**：
  - 这是**实验设计约束**（A/B 一次只能变一个变量），不是"功能砍半"。
  - 首个进入 enforced 的实验臂仍建议聚焦 `ScoreWeightedTopkStrategyV2 + V25_1_SMALL_CAP`，但**代码不因此缺失任何能力**——其它策略族/ExitGuard/ML 只是 policy 未启用。

**这样既消除推倒重写风险（接口、schema、reason code、evaluator 签名、DB 字段一次定型），也消除功能缺失风险（卖出侧、intraday recheck、T+1、ExitGuard、ML mode 全在契约里）。** 代价是设计阶段必须把 ★1–★8 全部钉死后才动代码——这反而强化了本审核的必要性。

---

## 3. 关键风险与必须补充（实施前拍板）

### ★1 `reference_price` 命名/语义冲突（阻断级）
代码库 `reference_price` 已表示**持仓成本/mark price**。文档全篇用它表示"信号可比价"。会在 selection artifact 与 order metadata 上同名异义，污染 parity 与审计。
**补充**：PriceGuard 字段改名 `signal_ref_price` / `pg_reference_price`，文档显式声明与现有字段区别。Phase 1 DTO 前必须定。

### ★2 "同一 evaluator、两个 adapter" 写成硬不变量
core evaluator 必须纯函数（输入 context、输出 decision、零 I/O）。QE 与 Paper v2 各写市场上下文 adapter，但调用同一 evaluator。配测试："同一 context 输入 → 两路径 decision/保护价 byte 一致"。

### ★3 与现有"涨停预过滤"去重
现有策略已有涨停预过滤 + hold_thresh（[[trading_strategy]]、[[limit_threshold_analysis]]），在信号时点过滤；PriceGuard 在执行时点判断开盘 gap。两者互补但有双重跳过/双重计数风险。baseline 统计"高开仍买入分布"时必须扣除已被预过滤剔除的部分，否则 A/B 收益错误归因。
**补充**：reason code 区分 `pre_filter_limit_up`（已有）vs `pg_skip_near_limit_up`（新增）；文档新增"PriceGuard 与现有 limit pre-filter 职责边界"一节。

### ★4 T+1 对 SELL / risk-exit 的约束（被低估）
A 股 T+1：当日买入当日不可卖。`hard_stop_price` 若买入当天触发**无法执行**。文档把 risk-exit 写得像随时可成交。
**补充**：ExitGuard stop_loss 显式建模 T+1——当日买入硬止损只能记 `STOP_LOSS_DEFERRED_T1`，不得伪装成交。

### ★5 分桶样本量与回测过拟合（校准方案核心漏洞）
`top1% × gap5-9% × regime × board × holding` 交叉桶样本可能个位数，median/percentile_10 不稳；叠加 4×4×4×3 网格 + walk-forward = 典型多重检验温床。
**补充**：(a) 桶最小样本阈值，不足则向父桶**收缩（empirical Bayes/shrinkage）**；(b) 参数选择引入 **PBO / Deflated Sharpe Ratio**（López de Prado）作为晋级硬门，而非仅"跨窗口稳定"的定性判断。

### ★6 Qlib 中 signal_close 的取值路径（正确性风险，Open Q3）
inner strategy T+1 执行时读 T 日 signal_close 本无未来函数，但**从哪个字段、哪个时点取**必须钉死，否则极易误取 T+1 数据。
**补充**：由 outer strategy 在信号生成时写入 order metadata/artifact，inner strategy **只读不算**；配 config truth test。

### ★7【Qlib 调研新增】Order 无 limit_price → PriceGuard 必须以"改写 amount"实现
Qlib `Order`（`decision.py:37-72`）无 limit_price 字段，Exchange 可交易即按 deal_price 全额成交。所以 QE 侧 PriceGuard **不能**靠"传 limit_price 给执行算法"实现追价拒绝，必须：
- 在 inner_strategy / 自定义 Strategy 中，于 `deal_order()` 前比较 `deal_price`(raw $open，经 $factor 转 raw) 与 `max_buy_price`；
- 不可接受 → `amount=0`（SKIP）或缩放（REDUCE）；可接受 → 正常成交并旁路记录保护价与 reason。
- 或扩展自定义 `Order` 子类携带 `pg_limit_price` 并改 Exchange 成交判断（侵入更深，不推荐第一版）。
**文档 §8.3 需改写**：明确 QE 的"limit_price"是接受阈值而非挂单限价；明确实现为 order-list modifier。

### ★8【Qlib 调研新增】QE 成交模型 ≠ 实时限价模型，parity 不能覆盖实时
Qlib：可交易→按 deal_price 同步全额成交。Paper v2 realtime/MiniQMT：真实限价单有挂单/排队/部分成交/未触及不成交。**§9.5 的 parity replay 只能保证历史决策一致，无法保证实时限价成交行为一致。**
**补充**：把"实时限价单成交行为（排队/部分成交/未触及）"列为独立开放问题与独立验证阶段；明确实时阶段不得用 QE 证据替代成交可行性验证；fill-probability（接近涨停时成交概率塌缩）应作为实时阶段观测项，而非只看价格距离。

### 次要补充
- **disabled = 当前行为字节级等价**：作为硬不变量 + 回归测试（disabled 路径必须 no-op，与现 main 产出相同 fills/events）。这是"明确界限、可禁用"的可验证定义。
- **换手预算交互**：skip→buy_next_candidate 推高换手与最小佣金 drag，而换手是你 QE 当前核心杠杆；`turnover_delta` + 成本 drag 应列 A/B 一级指标。
- **集合竞价可得性**：`indicative_open` 在 MiniQMT 实时是否可取需验证；历史回放用 first-minute open 可，realtime adapter 缺则 `WAITING`/fail-fast，不得猜。

---

## 4. 机构实践与论文：补充文献

文档已引经典执行/资产定价（Almgren-Chriss、Bertsimas-Lo、Obizhaeva-Wang、Perold IS、三因子/动量/q-factor、Barra、Alphalens、vn.py/LEAN/Nautilus/Qlib、沪深交易所规则），质量高。**缺的三类恰是验证 PriceGuard 是否真有价值、是否被过拟合的关键：**

1. **交易成本能否吃掉 alpha（PriceGuard 核心命题，最对口却未引）**
   - Novy-Marx & Velikov (2016), *A Taxonomy of Anomalies and Their Trading Costs*, RFS — 系统量化"异象在成本后是否存活"，即"高开后还值不值得买"的学术对应。
   - Frazzini, Israel & Moskowitz (2018), *Trading Costs* (AQR) — 真实因子策略实测成本，支撑 alpha 预算扣成本的初值。
   - Korajczyk & Sadka (2004) — 动量容量与流动性成本。
2. **回测过拟合治理（对应 ★5）**
   - Bailey & López de Prado, *The Deflated Sharpe Ratio*；Bailey/Borwein/López de Prado/Zhu, *PBO（Probability of Backtest Overfitting）* — 给网格 + walk-forward 晋级门加量化护栏。
3. **A 股微观结构（文档只引规则，缺学术）**
   - 涨跌停"磁吸效应（magnet effect）"A 股实证 — 为 `near_limit_up_skip` 提供"接近涨停价格被吸附、成交概率塌缩"的理论依据（强于纯价格距离阈值）。
   - T+1 与隔夜跳空 A 股实证 — 支撑用 `signal_close` 而非 `open` 作决策基准。

---

## 5. 界限与可选项（用户硬性要求）——可验证形式

文档机制已满足（`enabled` + `mode: disabled|shadow|enforced` + `scope` + `policy_sha256`，默认 disabled，runtime 禁止覆盖且代码已确认）。补两条强约束使"界限"可验证：

1. **disabled no-op 不变量**：新增 `price_guard` key 后，`enabled=false`/key 缺失时订单流必须与当前 main **完全一致**，有回归测试证明。
2. **hash 兼容性**：`policy_sha256` 由整个 policy_json 计算；validator 须保证"price_guard 缺省"与"`{enabled:false}`"产生**不同 hash 但相同行为**，避免审计混淆；现存已验证 policy 不含该 key、hash 不变（向后兼容 ✓）。

---

## 6. 修订后的实施建议（完整架构一次设计，分阶段验证启用）

**Phase 1（一次交付完整契约，无半成品接缝）**：PriceGuardContext / 纯函数 evaluator / 全 reason code / 买卖双向 / auction+intraday / 三态 mode / ExitGuard 接口 / QE+Paper v2 两 adapter 外壳 / DB schema 字段。先解决 ★1/★2/★7。

**Phase 2（QE 注入）**：ConfigComposer 注入 price_guard；inner strategy 实现为 order-list modifier（★7）；config truth test 证明 YAML 切片正确（★6）。

**Phase 3（QE A/B）**：`disabled vs enforced`、`skip_to_cash vs buy_next_candidate`、scope 对照；bucket 校准 + walk-forward + PBO/DSR 门（★5）；产出 `price_guard_policy.json` + hash + 报告。**此阶段产出的经济读数决定后续铺开范围。**

**Phase 4（Paper v2 历史 parity）**：同 hash 逐笔对齐（★8 注意：仅历史决策 parity）。

**Phase 5（实时/MiniQMT）**：独立验证实时限价成交行为（排队/部分/未触及）、fill-probability、集合竞价可得性（★8）。

**Phase 6+（ExitGuard / ML candidate）**：接口已在 Phase 1 就位，此处只填实现 + QE A/B，不改架构。

> 关键：上面"分阶段"全部是**启用与验证**的推进，**代码能力在 Phase 1 即完整**。任一阶段不得因"先简化"而省略卖出侧、T+1、intraday recheck、ExitGuard 接口或 ML mode 槽位。

---

## 7. 对原文档开放问题（§17）的答复

- **Q2（reference_price 语义）**：见 ★1，必须改名，禁止复用现有字段。
- **Q3（Qlib signal_close 取值）**：见 ★6，outer 写入、inner 只读。
- **Q5（skip 后目标权重）**：第一版固定 `cancel_today`（最易解释）；buy_next_candidate 作独立 A/B。
- **Q6（CLOSE_PRICE 路径）**：建议完全禁用 PriceGuard（无法验证开盘追价，留着只造半成品语义）。
- **Q7（默认 scope）**：日频 T+1 多因子目标默认 `auction_and_intraday`；但首个 enforced 实验臂可先 `auction_only` 降低 parity 面，验证后再上全天（注意：这是"先启用哪个 scope"，非"砍功能"——两 scope 的代码 Phase 1 都在）。
- **新增（★8 实时成交模型）**：列为独立开放问题——实时限价单的排队/部分成交/未触及行为无法由 Qlib parity 覆盖，需 Phase 5 独立验证。

---

## 8. 审批建议

1. 批准设计方向与分层定位（PriceGuard 独立成层、填补 Qlib 空白）。
2. 批准"完整契约一次设计 + 分阶段验证启用"的实施模型（替代原文档可能隐含的渐进式构建）。
3. 实施前必须先解决 ★1（改名）、★2（单 evaluator 双 adapter）、★7（Qlib amount 改写）、★8（实时成交模型差异）四项，并把 ★3/★4/★5/★6 补进文档。
4. 生产基线锁定 `bucket_calibrated`；ML 仅作 candidate policy，接口 Phase 1 预留。
5. 补充 §4 三类文献（成本侵蚀 alpha / 回测过拟合治理 / A 股微观结构）。
6. 界限以 §5 两条可验证不变量交付（disabled no-op 回归测试 + hash 兼容 validator）。
