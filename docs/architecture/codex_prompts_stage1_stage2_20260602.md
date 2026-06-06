# Codex 提示词（Stage 1 / Stage 2）—— 落盘备份，防 /clear 丢失

> 日期：2026-06-02
> 用途：发给 Codex 的现成提示词。Stage 1 先发；Stage 1 审核 PASS 并合入后再发 Stage 2。
> 依据：`price_guard_execution_acceptance_plan_2stage_20260602.md`（计划）+ `qe_paper_price_guard_execution_acceptance_design_v2_20260602.md`（v2 设计）。
> 提示词含完整 BEGIN/END 标识，直接复制其中代码块发送。

---

## Stage 1 提示词（选股 + 投顾全功能）

```text
===== BEGIN CODEX TASK: PriceGuard Stage 1 =====

【角色】你是 AIstock 后端开发。本次只实现 Stage 1（选股 + 投顾全功能），不碰 Stage 2。

【权威文档（必须先完整读取，逐字遵守）】
1. 执行验收计划：F:\Dev\AIstock_worktrees\price-guard-design-20260601\docs\architecture\price_guard_execution_acceptance_plan_2stage_20260602.md
2. v2 设计(schema/reason code/字段全集)：F:\Dev\AIstock_worktrees\price-guard-design-20260601\docs\architecture\qe_paper_price_guard_execution_acceptance_design_v2_20260602.md
冲突以"执行计划"为准；其 §2 契约已锁定，禁止另起方案，禁止中途反问需求。

【工作方式】
- 在 AIstock 主仓基于最新 main 新建独立 worktree/分支（如 codex/price-guard-stage1-20260602）实现，禁止直接写 main 工作目录。
- 遵守 AIstock 开发规范与仓库 CLAUDE.md：禁止 silent fallback / except:pass / 默认值伪装成功；缺关键输入必须 fail-fast 并传播错误；禁止启动任何服务（需重启时仅提醒）；禁止空 DB 密码默认值。
- DDL 只产出迁移脚本，经既有生产 DDL gate，禁止自动应用到生产库。

【本次必须完整交付（缺一项即不合格，禁止交付简化版）】
按计划 §3.1 实现 M1-M4 全部：
- M1 契约与核心 evaluator：price_guard.py/exit_guard.py 纯函数(零 I/O)，PriceGuardContext/ExitGuardContext 全字段(含 dist_to_limit_up_bps/momentum_regime/volume_ratio_open/event_flag)，rule_v1 实现 + bucket_calibrated/ml 占位入口(签名与 policy 字段必须在场)，reason code 全集，扩展 ALLOWED_POLICY_JSON_KEYS 加 price_guard/exit_guard + validator。
- M2 选股买入区间+止损展示：selection.package_result 增列(§2.3-C)，由 signal_ref_price+alpha budget 生成 green/yellow/red 买入区间与 soft/hard 止损，标注 range_source/reference_source/price_basis/policy_sha256，guidance_status 一律 rule_default，A 股 tick 取整，并显涨跌停边界与免责标注；含 breakout_addon 分支入口(默认关)。
- M3 自选池+荐股生命周期+每日复评：watchlist_items 增 SCD 字段(§2.3-A)，新增 append-only app.advisory_daily_review 表(§2.3-B，禁止 UPDATE，UNIQUE(item,trade_date) 幂等)，状态机 CANDIDATE→ENTERED→HOLDING→EXITED，每日复评 job(读 selection.daily_selection_evidence + watchlist → exit_guard.evaluate → 落日表)。退出规则=alpha_decay/rank_drop + soft/hard stop，take_profit 默认关。当日建仓硬止损记 STOP_LOSS_DEFERRED_T1 不立即 EXITED；复权除息跨日调整 entry_cost/stop/take；停牌记 WAITING/carry。
- M4 区间/止损质量回顾评估：计划 §3.1-M4 全部指标 + 分桶 + 桶最小样本阈值/向父桶收缩，报告显式标 post-decision diagnostics(非 validated PnL)。

【锁定决策(直接用)】
- 命名用 signal_ref_price，禁止复用既有 reference_price 表达信号价；只读记录 selection.package_result.reference_price 现有语义。
- §2.3 三张表列定义照此建。
- 退出规则与 take_profit 默认关如上。

【红线(任一触发即判不合格，见计划 §6)】
silent fallback / advisory 层产生成交或写 Paper ledger/OMS / 未过 QE 标 qe_validated / disabled 路径非字节级等价 / 复用 reference_price / 每日字段 UPDATE 进 watchlist 基表 / 发布全局标量 max_open_gap_bps 作唯一判据 / advisory 缺简化交付。

【测试要求】
按计划 §3.2 S1-1…S1-12 提供单测+集成测试，覆盖 green/yellow/red、breakout、fail-fast(缺 signal_ref_price/basis mismatch)、policy hash 稳定性、状态机合法/非法迁移、每日复评幂等、T+1 STOP_LOSS_DEFERRED_T1、复权调整、advisory 零 ledger/OMS、质量报告无未来函数、现状回归不变。

【合入前产出汇报文档(不要自行合入)】
完成开发并全部测试通过后：
1. 在实现 worktree 的 docs/ 生成 STAGE1_ACCEPTANCE.md，严格按计划 §5 模板：功能清单完成度表、S1-1…S1-12 逐条 PASS+证据(测试名/命令/产物路径)、偏离设计之处+理由、如何复核(可直接跑的命令)、已知限制。
2. 如实报告：任何失败测试、未达标项、与设计的偏离都必须写明，禁止隐瞒或谎报通过。
3. 推送分支并停下，等待审核者基于 STAGE1_ACCEPTANCE.md 审核。审核 PASS 后由审核者批准合入，禁止在审核前自行合入 main。

完成上述后，仅回复：STAGE1 开发完成，已生成 STAGE1_ACCEPTANCE.md，等待审核。

===== END CODEX TASK: PriceGuard Stage 1 =====
```

---

## Stage 2 提示词（QE + 模拟盘）—— Stage 1 审核 PASS 并合入后再发

```text
===== BEGIN CODEX TASK: PriceGuard Stage 2 =====

【角色】你是 AIstock 后端开发。本次实现 Stage 2（QE 回测 A/B + Paper v2 模拟盘 enforced + ExitGuard enforced）。前置：Stage 1（M1-M4）已审核 PASS 并合入 main。

【权威文档（必须先完整读取，逐字遵守）】
1. 执行验收计划：F:\Dev\AIstock_worktrees\price-guard-design-20260601\docs\architecture\price_guard_execution_acceptance_plan_2stage_20260602.md
2. v2 设计：F:\Dev\AIstock_worktrees\price-guard-design-20260601\docs\architecture\qe_paper_price_guard_execution_acceptance_design_v2_20260602.md
冲突以"执行计划"为准；§2 契约已锁定，禁止另起方案，禁止中途反问需求。复用 Stage 1 已建的 price_guard/exit_guard 纯函数 evaluator 与 policy schema，禁止另写第二套 evaluator。

【工作方式】
- 基于最新 main（含 Stage 1）新建独立 worktree/分支（如 codex/price-guard-stage2-20260602），禁止直接写 main 工作目录。
- 遵守 AIstock 开发规范与仓库 CLAUDE.md：禁止 silent fallback / except:pass / 默认值伪装成功；缺关键输入 fail-fast 并传播；禁止启动任何服务（需重启仅提醒）；禁止空 DB 密码默认值。DDL 只产迁移脚本，经既有 DDL gate。
- 首批落地策略族限定：ScoreWeightedTopkStrategyV2 + V25_1_SMALL_CAP。

【本次必须完整交付（缺一项即不合格，禁止简化版）】
按计划 §4.1 实现 M5-M7 全部：
- M5 QE 注入 + A/B enforced：ConfigComposer 注入 price_guard 进 NestedExecutor.inner_strategy；inner strategy 实现为"成交前 order-list modifier"——比较 deal_price(raw $open，经 $factor 转 raw) vs max_buy_price → amount=0(SKIP)/缩放(REDUCE)，旁路记保护价+reason（Qlib Order 无 limit_price，禁止靠 limit_price 实现追价拒绝）；signal_close 由 outer 写入、inner 只读，无未来函数。三模式 disabled/shadow/enforced；A/B 锁定 strategy/model/universe/data/cost/algo/seed，仅变 price_guard；候补 skip_to_cash/buy_next_candidate/proportional_redistribute 分别归因；与既有涨停预过滤去重(reason 区分 PRE_FILTER_LIMIT_UP vs PG_SKIP_NEAR_LIMIT_UP)。在此填实现 bucket_calibrated（历史分桶校准 + walk-forward + PBO/Deflated Sharpe 晋级门）；turnover_delta + 成本 drag 列一级指标。产出 price_guard_policy.json + policy_sha256 + ab_baseline_metrics.json + ab_price_guard_metrics.json + price_guard_decisions.parquet/jsonl + ab_comparison_report.md（均带 experiment_id/loop_id/数据版本/config hash）。
- M6 Paper v2 历史 parity + 实时落地：day_runner 在 build_order_intents 后、OMS.create_order 前插 PriceGuard，生成 LIMIT intent / rejected / reduced events；历史 parity replay（同 policy_sha256 逐 symbol/date/side 对齐 decision/reason/保护价/数量，tick 容差内）；shadow→guarded_sim→enforced_candidate 三步。独立验证实时限价成交模型（排队/部分成交/未触及不成交/fill_probability/集合竞价 indicative_open 可得性），并文档化其与 QE 全额成交假设的差异——parity 只保证历史决策一致，实时成交行为必须独立验证，不得用 QE 证据替代。runtime_config 覆盖执行策略必须被拒（复用既有）；实时缺 quote/bar → WAITING_FOR_PRICE_GUARD_INPUT。
- M7 ExitGuard enforced（承接 Stage 1 advisory）：exit_guard QE A/B 对比 alpha_rebalance_only vs exit_guard_enabled；通过 QE + Paper v2 parity 后才进 Paper v2 shadow/guarded_sim；默认仍 disabled，不在本阶段强制启用生产。

【红线(任一触发即判不合格，见计划 §6)】
silent fallback / 未过 QE 的 policy 标 qe_validated / disabled 路径非字节级等价(回归 fills/events 须与现 main 相同) / 发布全局标量 max_open_gap_bps 作唯一判据(须 bucket/ml 条件决策；核心多因子不得以"可能涨停"关闭价格保护) / 用 Qlib Order limit_price 实现追价拒绝(须 order-list 改写 amount) / 把 shadow 当收益证据 / 缺 reference/limit/factor 不 fail-fast / 另写第二套 evaluator / 交付简化版或缺 M5-M7 任一项。

【测试要求】
按计划 §4.2 S2-1…S2-13 提供测试，覆盖：QE config truth test(price_guard 真实进 YAML 切片)、A/B config diff(仅 price_guard_mode/policy_sha256 不同)、order-list modifier 高开真实跳过、signal_close 无未来函数、A/B 产物齐全、晋级门(walk-forward 稳定 + PBO/DSR + 至少一项稳定净改善且不破坏 IR/成交率)、候补三模式分离归因、涨停预过滤去重、Paper v2 parity checklist 全绿、实时限价成交行为(部分/未触及/排队)独立测试、runtime override 被拒、缺 quote/bar→WAITING、disabled 字节级等价回归。

【合入前产出汇报文档(不要自行合入)】
1. 在实现 worktree 的 docs/ 生成 STAGE2_ACCEPTANCE.md，严格按计划 §5 模板：M5-M7 功能完成度表、S2-1…S2-13 逐条 PASS+证据(测试名/命令/产物路径，含 ab_comparison_report.md 路径)、偏离设计之处+理由、如何复核、已知限制。
2. 如实报告任何失败测试、未达标项、与设计偏离，禁止隐瞒或谎报通过。特别说明：A/B 若未呈现稳定净改善，照实报告，不得粉饰——结论可为"PriceGuard 在该策略族暂不晋级"。
3. 推送分支并停下，等待审核者基于 STAGE2_ACCEPTANCE.md 审核。审核 PASS 后由审核者批准合入，禁止在审核前自行合入 main。

完成上述后，仅回复：STAGE2 开发完成，已生成 STAGE2_ACCEPTANCE.md，等待审核。

===== END CODEX TASK: PriceGuard Stage 2 =====
```

---

## 审核者（本作者）操作备忘
- Stage 1 收到 STAGE1_ACCEPTANCE.md → 按计划 §3.2 S1-1…S1-12 + §6 红线逐条核(抽查 file:line + 跑命令) → PASS 批准合入 → 发上面 Stage 2 提示词。
- Stage 2 收到 STAGE2_ACCEPTANCE.md → 按 §4.2 S2-1…S2-13 + §6 核 → PASS 批准合入。
- 不只信 Codex 自报，必须抽查代码与运行关键测试。
