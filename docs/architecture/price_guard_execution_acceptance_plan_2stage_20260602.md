# PriceGuard / 投顾 / QE / 模拟盘 —— 两大阶段执行与验收计划

> 日期：2026-06-02
> 性质：执行计划（Codex 开发 / 本作者验收），非代码
> 设计依据：`qe_paper_price_guard_execution_acceptance_design_v2_20260602.md`（rationale/schema/reason code 全集在此，本文件不重复，只定"做什么 + 怎么验收"）
> 协作原则：**两个大阶段，每阶段 Codex 一次性完成全部功能，产出 1 份简短验收文档，本作者基于文档审核**。契约已在 §2 预先定死，Codex **中途无需再向审核者确认**。
>
> **审批状态（2026-06-02 已锁定）**：① §2.3 三张表列定义照此执行（DDL 经既有生产 DDL gate，不自动应用到生产）；② Stage 1 生命周期退出规则 = `alpha_decay/rank_drop + soft/hard stop`，`take_profit` 默认关闭。以上为最终决策，Codex 直接实现。

---

## 1. 协作协议（最少交互）

```text
本计划(契约已定死)
  → Codex 实现 Stage 1 全部功能(内部里程碑自排序, 不拆成多次交互)
  → Codex 产出 STAGE1_ACCEPTANCE.md(简短验收文档, 模板见 §5)
  → 审核者基于文档 + 抽查 file:line + 跑关键测试 → PASS / 退回(附具体缺口)
  → (PASS 后)Codex 实现 Stage 2 全部功能
  → Codex 产出 STAGE2_ACCEPTANCE.md
  → 审核者审核 → PASS / 退回
```

- 每个大阶段**只 1 次审核交互**（除非退回）。
- Codex 不得"先做简化版"——每阶段必须交付该阶段**完整功能清单**（§3.1 / §4.1），缺一项即退回。
- 退回时审核者给**逐条缺口清单**，Codex 修复后重交同一份验收文档（标更新）。
- 实现代码请在**独立 worktree/branch**进行（本 worktree 为设计文档分支）。DDL 经既有生产 DDL gate，不自动执行。

---

## 2. 预定义契约（已拍板，Codex 直接用，无需确认）

> 这一节消除歧义，是"最少交互"的关键。以下决策**已定**，Codex 不得另起方案。

### 2.1 命名（解决 ★1）
- 信号可比价一律新字段 **`signal_ref_price`**；**禁止**复用既有 `reference_price` 表达信号价。
- Codex 在 STAGE1 文档中记录 `selection.package_result.reference_price` 现有语义（只读不改）。

### 2.2 纯函数 core evaluator（解决 ★2）
```text
backend/services/trading_core/price_guard.py
  PriceGuardContext (dataclass, 字段见 v2 §4.2, 含 dist_to_limit_up_bps/momentum_regime/volume_ratio_open/event_flag)
  PriceGuardPolicy  (schema 见 v2 §4.3, 三态 mode: rule_v1|bucket_calibrated|ml_residual_alpha_v1)
  def evaluate(ctx, policy) -> PriceGuardDecision        # 纯函数, 零 I/O
backend/services/trading_core/exit_guard.py
  ExitGuardContext / ExitGuardPolicy / def evaluate(ctx, policy) -> ExitGuardDecision   # 纯函数, 零 I/O
```
- Advisory 层 / QE inner strategy / Paper v2 day_runner 各写 adapter 取数，但**调用同一 evaluator**。
- 三态 mode 接口 Phase 0 即在场；`bucket_calibrated`/`ml` 在 Stage 1 可 `NotImplementedError` 占位，但**类型签名/policy 字段/分支入口必须存在**（Stage 2/后续填实现，不改架构）。

### 2.3 数据库（已定结构，DDL 走 gate）
**A. `app.watchlist_items` 增列（实体/SCD，复用现表）**
```text
lifecycle_status   TEXT  CHECK in (CANDIDATE, ENTERED, HOLDING, EXITED)  DEFAULT CANDIDATE
planned_entry_price NUMERIC NULL
actual_entry_price  NUMERIC NULL
actual_entry_date   DATE    NULL
exited_at           TIMESTAMPTZ NULL
exit_reason         TEXT    NULL
advisory_enabled    BOOLEAN DEFAULT FALSE   -- 投顾监控可选开关
```
**B. 新增 `app.advisory_daily_review`（append-only 时间序列，禁止 UPDATE）**
```text
review_id        BIGSERIAL PK
watchlist_item_id BIGINT FK -> app.watchlist_items
code             TEXT
trade_date       DATE
evidence_id      TEXT NULL  FK -> selection.daily_selection_evidence  -- alpha 信号引用, 不重复存
score, rank      (引用快照, 可冗余只读)
current_price    NUMERIC
entry_band_json  JSONB   -- green/yellow/red max_buy_price
stop_price       NUMERIC -- 当日重算
take_price       NUMERIC NULL
action           TEXT    -- HOLD/ADD/REDUCE/STOP_LOSS/TAKE_PROFIT/ALPHA_RANK_DROP_EXIT/...
reason_code      TEXT
policy_sha256    TEXT
guidance_status  TEXT    -- rule_default|bucket_calibrated|qe_validated
price_basis      TEXT
feature_availability_ts TIMESTAMPTZ
t1_note          TEXT NULL
layer            TEXT DEFAULT 'advisory'
created_at       TIMESTAMPTZ DEFAULT now()
UNIQUE(watchlist_item_id, trade_date)   -- 幂等
```
**C. `selection.package_result` 增列（选股区间/止损展示）**
```text
suggested_entry_price_band JSONB NULL
suggested_stop_loss_zone   JSONB NULL
guidance_status            TEXT  NULL   -- 一律 rule_default(未过 QE)
price_guard_policy_sha256  TEXT  NULL
```

### 2.4 policy 白名单 / reason code
- 扩展 `ALLOWED_POLICY_JSON_KEYS` 增 `price_guard`、`exit_guard` + validator（拒未知键、拒 algo_config 夹带追价参数）。
- reason code 用 v2 §4.4 全集（含 `ADD_BREAKOUT_NEAR_LIMIT`/`STOP_LOSS_DEFERRED_T1`/`PRE_FILTER_LIMIT_UP` vs `PG_SKIP_NEAR_LIMIT_UP`/各 `*_ERROR`）。

### 2.5 三层边界（不可逾越）
advisory（无成交/无 ledger/不 claim validated PnL）↔ QE（验证门）↔ Paper v2（只用 QE 验证 hash）。详见 v2 §3。

---

## 3. Stage 1 —— 选股 + 投顾全功能（先完成）

### 3.1 完整功能清单（缺一即退回）

**M1 契约与核心 evaluator**
- PriceGuardContext/ExitGuardContext（全字段，含 Q4 非单调 gap 特征）；price_guard/exit_guard 纯函数（rule_v1 实现 + bucket/ml 占位入口）；reason code 全集；policy 白名单扩展 + validator；`signal_ref_price` 命名落地。

**M2 选股买入区间 + 止损展示（能力 B）**
- `selection.package_result` 增列；调用 evaluator 由 `signal_ref_price` + alpha budget 生成 green/yellow/red 买入区间 + soft/hard 止损区间；标注 range_source/reference_source/price_basis/policy_sha256/`guidance_status=rule_default`；A 股 tick 取整；并显交易所涨跌停边界；前端选股页展示 + 免责标注。
- **非单调 gap（Q4）**：rule_v1 含 `breakout_addon` 分支入口（默认关）；context 已带 dist_to_limit_up/momentum/volume 特征，使后续开启无需改架构。

**M3 自选池 + 荐股生命周期 + 每日复评（能力 C）**
- watchlist_items 增 SCD 字段；新增 `advisory_daily_review` 表；CANDIDATE→ENTERED→HOLDING→EXITED 状态机；`/to-watchlist` 接通生命周期；每日复评 job（读 daily_selection_evidence + watchlist → exit_guard.evaluate → 落 append-only 日表）；退出规则第一版 `alpha_decay/rank_drop + soft/hard stop`，take_profit 默认关。
- T+1：当日建仓硬止损记 `STOP_LOSS_DEFERRED_T1`，不立即 EXITED。
- 复权除息跨日调整 entry_cost/stop/take；停牌记 WAITING/carry；每日 job 幂等。

**M4 区间/止损质量回顾评估（能力 C 的"看效果"）**
- 对历史 advisory 记录算：entry_zone_hit_rate、entry_zone_fillable_rate、alpha_if_entered_zone、alpha_if_chased_above_zone、missed_alpha_if_not_entered、soft/hard_stop_trigger_rate、stop_saved_loss_bps、stop_whipsaw_cost_bps、reward_risk_realized；分 score/gap/regime/liquidity/board 桶 + 桶最小样本阈值/向父桶收缩；报告显式标 **post-decision diagnostics（非 validated PnL，含选择偏差/样本量提示）**。

### 3.2 客观验收标准（审核者逐条核）

| # | 标准 | 证据形式 |
| --- | --- | --- |
| S1-1 | evaluator 纯函数(无 I/O 导入)、同输入同输出 | 静态检查 + 单测 |
| S1-2 | 单测覆盖 green/yellow/red、SKIP_OPEN_GAP、REDUCE_YELLOW、near_limit、breakout_addon、缺 signal_ref_price→fail-fast、basis mismatch→fail-fast | 测试名清单 |
| S1-3 | policy hash 稳定 + "缺省"vs"{enabled:false}"不同 hash 同行为 + validator 拒未知键/拒 algo_config 夹带 | 单测 |
| S1-4 | 选股区间字段齐全；缺 signal_ref_price/basis/hash→降级"不提供区间"或 fail-fast，**绝不填默认价** | 针对性测试 |
| S1-5 | 区间 tick 取整 + 并显涨跌停边界 + 免责标注 + `guidance_status` 全 `rule_default`(无 qe_validated) | 断言测试 + 截图/payload |
| S1-6 | 生命周期状态机合法/非法迁移全覆盖 | 单测 |
| S1-7 | 每日复评对历史区间逐日运行，每 item×交易日产 1 行 append-only；同(item,date)幂等 | 集成测试 + 重跑对比 |
| S1-8 | 当日建仓+当日硬止损→`STOP_LOSS_DEFERRED_T1`，不立即 EXITED | 针对性测试 |
| S1-9 | 复权除息跨日 stop/take 调整正确；停牌→WAITING/carry | 针对性测试 |
| S1-10 | **advisory 路径零 OMS/broker/Paper ledger 写入** | 代码搜索 + 测试 |
| S1-11 | 质量报告产出 + 分桶 + 标注 post-decision diagnostics + 无未来函数(成交价用当时可观测价) | 报告样例 + 测试 |
| S1-12 | 通则：无 silent fallback、缺输入 fail-fast、现状回归不变 | 搜索 + 回归测试 |

### 3.3 Stage 1 出口门
S1-1…S1-12 全 PASS + 审核签核。**PASS 后才进 Stage 2。**

---

## 4. Stage 2 —— QE + 模拟盘（后完成）

### 4.1 完整功能清单（缺一即退回）

**M5 QE 注入 + A/B enforced 验证（enforced 唯一门）**
- ConfigComposer 注入 price_guard 进 NestedExecutor.inner_strategy；**inner strategy 实现为成交前 order-list modifier**：比较 deal_price(raw $open,经 $factor 转 raw) vs max_buy_price → amount=0(SKIP)/缩放(REDUCE)，旁路记保护价+reason（★7：Qlib Order 无 limit_price）；`signal_close` 由 outer 写 inner 只读（★6）。
- 三模式 disabled/shadow/enforced；A/B 锁定 strategy/model/universe/data/cost/algo/seed，仅变 price_guard；候补 skip_to_cash/buy_next_candidate/proportional_redistribute 分别归因；与既有涨停预过滤去重（★3）。
- `bucket_calibrated` 校准 + walk-forward + **PBO/Deflated Sharpe 晋级门**（★5）；turnover_delta + 成本 drag 列一级指标；产出 price_guard_policy.json + sha256 + A/B 报告。
- bucket_calibrated/ml evaluator 分支在此填实现（架构不变）。

**M6 Paper v2 历史 parity + 实时落地（enforced）**
- day_runner intents 后插 PriceGuard 生成 LIMIT intent / rejected / reduced；历史 parity replay（同 hash 逐 symbol/date/side 对齐 decision/reason/保护价/数量）；shadow→guarded_sim→enforced_candidate 三步。
- **实时限价成交模型独立验证**（排队/部分成交/未触及/fill_probability/集合竞价 indicative_open 可得性）——★8：parity 只保证历史决策一致，实时成交行为须独立验证，不得用 QE 证据替代。
- runtime_config 覆盖执行策略被拒（复用既有）；缺 quote/bar→WAITING。

**M7 ExitGuard enforced（可选，承接 Stage 1 advisory）**
- exit_guard QE A/B：`alpha_rebalance_only` vs `exit_guard_enabled`；通过后才进 Paper v2 shadow/guarded_sim；默认仍 disabled。

### 4.2 客观验收标准

| # | 标准 | 证据形式 |
| --- | --- | --- |
| S2-1 | QE config truth test 证明 price_guard.enabled/scope/参数真实进执行 YAML 切片 | 测试名 |
| S2-2 | A/B config diff 证明仅 price_guard_mode/policy_sha256 不同，余皆相同 | diff 报告 |
| S2-3 | inner strategy order-list modifier：deal_price>max_buy_price→amount=0/缩放，旁路记 reason；高开样例真实跳过 | 单测 + 回归样本 |
| S2-4 | signal_close outer 写 inner 只读，无未来函数 | config truth test |
| S2-5 | A/B 产物齐全(policy.json/.sha256/baseline/guard metrics/decisions.parquet/comparison.md)，带 experiment_id/数据版本/config hash | 产物清单 |
| S2-6 | 晋级门：跨 walk-forward 稳定 + 过 PBO/DSR + 收益/回撤/尾部/成本至少一项稳定净改善且不显著破坏 IR/成交率 | 报告 |
| S2-7 | 候补三模式分别报告，价格保护收益与候补选择收益分离归因 | 报告 |
| S2-8 | 与既有涨停预过滤去重(reason 区分 PRE_FILTER_LIMIT_UP vs PG_SKIP_NEAR_LIMIT_UP) | 测试 |
| S2-9 | Paper v2 parity checklist 全绿(hash/context/decision/reason/guard price tick容差/quantity/failure mode)，不一致禁进 enforced | parity 报告 |
| S2-10 | 实时限价成交行为独立测试(部分/未触及/排队)，与 QE 全额成交差异文档化 | 测试 + 文档 |
| S2-11 | runtime override 被拒；缺 quote/bar→WAITING | 测试 |
| S2-12 | disabled 路径与现 main 字节级等价(回归 fills/events 相同) | 回归测试 |
| S2-13 | 通则：无 silent fallback、缺输入 fail-fast | 搜索 + 测试 |

### 4.3 Stage 2 出口门
S2-1…S2-13 全 PASS + 审核签核。

---

## 5. Codex 验收文档模板（每阶段 1 份，简短，证据密集）

> 文件名 `STAGE1_ACCEPTANCE.md` / `STAGE2_ACCEPTANCE.md`，放实现 worktree 的 `docs/`。**简短**：每项一行，靠指针(测试名/file:line)而非长篇。

```markdown
# STAGE{N} 验收文档
分支/commit: ...

## 1. 功能清单完成度 (M{x})
| 里程碑 | 状态 | 关键文件:行 | 测试名 |
|---|---|---|---|
| M1 ... | DONE | path:line | test_xxx |

## 2. 验收标准逐条
| # | 标准 | PASS? | 证据(测试名/命令/产物路径) |
|---|---|---|---|
| S{N}-1 | ... | ✅ | tests/...::test_... |

## 3. 偏离设计之处 (如有) + 理由
- ...(无则写"无")

## 4. 如何复核 (审核者可直接跑)
- `pytest tests/... -k price_guard`
- 关键产物: docs/.../ab_comparison_report.md

## 5. 已知限制 / 下阶段衔接
- ...
```

审核者据此：核对清单↔§3.2/§4.2、抽查 file:line、跑"如何复核"命令、检查红线。PASS 或退回逐条缺口。

---

## 6. 红线（任一触发立即退回，不论其它项多完整）

1. silent fallback / 默认值伪装成功 / 缺输入不 fail-fast。
2. advisory 层产生成交或写 Paper ledger/OMS。
3. 未过 QE 的 policy 标 `qe_validated`。
4. disabled 路径改变现状行为（非字节级等价）。
5. 复用 `reference_price` 表达信号价（须 `signal_ref_price`）。
6. 把每日变化字段 UPDATE 进 watchlist 基表（须 append-only 日表）。
7. 生产决策发布全局标量 `max_open_gap_bps` 作为唯一判据（须 bucket/ml 条件决策；核心多因子不得以"可能涨停"关闭价格保护）。
8. QE 用 Qlib Order limit_price 实现追价拒绝（Qlib 无此字段，须 order-list 改写 amount）。
9. 交付简化版 / 缺该阶段功能清单任一项。
