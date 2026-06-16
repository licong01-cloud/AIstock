# 详细实施设计 · 评估口径 refactor（CAGR/MDD + Top-K 为主）

> **类型**：阶段详细设计（design / impl）· 对应分阶段计划 P-1 原则 + P1/P3 基础
> **日期**：2026-06-16
> **上游**：`multi_alpha_phased_implementation_plan_20260616.md` §1 P-1
> **状态**：待评审 → 开发（me/Codex 分工见 §8）
> ⚠️ 所有 file:line 为探查定位，**实施前必须复核当前代码**。

---

## 1. 背景与目标

**问题**：QE 现在用**全局 IC/RankIC/ICIR** 作"选最佳 loop / 选腿 / 组合权重 / 晋升门"的主指标。但荐股/topk 只部署**排名前 20-50 只**，全局 IC 与"前 20 是否更好"数学上错配（附录 B3：NDCG/Top-K top-heavy，IC 全截面等权）。

**目标**：把主考核切到 **CAGR + 最大回撤(MDD) + Top-K 指标**；**IC/RankIC/ICIR 保留但降为诊断**（看信号广度，不进晋升门、不做主排序）。backtest/paper/live 三处口径统一。

**关键利好（探查结论）**：
- CAGR/MDD/Sharpe/Calmar **已在** `v_run_leaderboard`（`run_account_summary`）—— Tier-0，零成本。
- Top-K realized 质量指标可用现有 `run_symbol_summary`（profit_pct + rank_in_list + source_list）/ `run_position`（score + rank_in_portfolio + return_contribution）**直接算，无需 pred.pkl** —— Tier-1。
- **616 存量 run 可纯 SQL 回填**（profit_pct 已入库），无需重跑回测。
- 真 precision@K / NDCG@K（全截面，预测 top-K vs 实际 top-K）需 pred.pkl —— Tier-2，**依赖 P2**，本设计先占接口位、不阻塞。

---

## 2. 指标分层与定义

### Tier-0（已存在，直接用）
`cagr` · `max_drawdown` · `sharpe` · `calmar`(=cagr/|mdd|) · `annualized_volatility` · `turnover`。来源 `run_account_summary` / `v_run_leaderboard`（`init_qe_archive_schema.py:237-240`，复核）。

### Tier-1（本期新增，无需 pred.pkl；从 `run_position` 预测排名 + 实际贡献）
> 用 `run_position`（含 `score` / `rank_in_portfolio` / `return_contribution`）—— 它同时有**预测排名**和**实际贡献**，是回答"前 K 是否更好"的正确来源。⚠️ 实施前确认 `run_position` 粒度（每日/每次再平衡）与 `run_symbol_summary.all_stocks` 的排序语义（按 profit 还是 score——若按 profit 则不能用于预测质量，须改用 run_position 的 score 排名）。

对每个 run，按 `rank_in_portfolio`（预测分数排名）升序取前 K（K∈{20,50}）：
- **`topk_return@K`** = mean(该 run 内预测 rank≤K 持仓的实际收益)；时间维上对每个再平衡日求 top-K 实际收益再跨日平均。
- **`topk_hit_rate@K`** = 预测 rank≤K 持仓中实际为正收益的占比。
- **`topk_decay`** = `topk_return@20` − `topk_return@50`（>0 说明越靠前越好，模型 top-heavy 质量好）。
- **`within_portfolio_rankic`** = Spearman(`rank_in_portfolio`, `return_contribution`) —— 组合内"预测排名 vs 实际贡献"一致性（top-heavy 版 IC）。
- **`topk_dispersion@K`** = std(top-K 实际收益) —— 集中风险（防单只彩票拉高均值）。

> 注：Tier-1 的 `topk_return@K` 与组合 CAGR 不同——CAGR 是整篮(经风险过滤+权重)净值;Tier-1 直接量"预测最靠前的 K 只的实际表现"，这才是荐股关心的"前 20 好不好"。

### Tier-2（需 pred.pkl，P2 后）——本期只占接口位
`precision@K`（全截面预测 top-K ∩ 实际 top-K / K）· `NDCG@K`（位置加权）· `topk_coverage`（全市场 vs 持仓）· 全截面 `topk_decay` 曲线。

---

## 3. 数据流

```
回测产出 enhanced_metrics{all_stocks, top_stocks, positions...} (已有)
   │
   ├─ payload_extractor._extract_account_summary → run_account_summary(cagr/mdd/sharpe) [有]
   ├─ payload_extractor._extract_symbol_summaries → run_symbol_summary(profit_pct/rank) [有]
   ├─ (位置)→ run_position(score/rank_in_portfolio/return_contribution) [有]
   └─【新】_extract_topk_metrics → run_metric(写 topk_return@K/hit_rate/decay/within_rankic) [Tier-1]
                                              │
   v_run_leaderboard / v_seed_robustness / v_promotion_candidates ←【改:join topk + 改门】
                                              │
   MCP query (order_by 扩 topk/mdd/calmar) + UI(展示 topk 列+综合分, IC 降诊断子面板)
```

`payload_extractor.py`（`_extract_account_summary:372`、`_extract_symbol_summaries:628`，复核）是写入点；新增 `_extract_topk_metrics()` 在 symbol/position 提取后聚合写 `run_metric`（JSONB pivot，无需加列，最省）。

---

## 4. DB / schema 变更（additive，不破坏既有）

1. **不改 `run_account_summary` 结构**；Tier-1 topk 指标写 `run_metric`（现有 JSONB pivot 表，键如 `topk_return_20/topk_hit_rate_20/topk_decay/within_portfolio_rankic`）。
2. **新视图 `v_topk_quality`**：按 run_id 聚合 `run_position`/`run_metric` 输出 topk_* 列。
3. **改 `v_run_leaderboard`**：left join `v_topk_quality`，新增 topk_* 列（IC 列保留）。
4. **改 `v_seed_robustness`**：聚合增加 `topk_return_20_mean/std/cv`、`topk_hit_rate_20_mean`、`mdd_mean`(已有)。
5. **改 `v_promotion_candidates` 门（核心）**：
   ```
   OLD: icir_mean>=0.5 AND ir_mean>=1.5 AND is_return_stable(CV<0.25)
   NEW: cagr_mean>=θ_cagr
        AND max_drawdown_mean>=-θ_mdd            -- |MDD|≤θ_mdd
        AND topk_return_20_mean>=θ_topk          -- 前20实际收益达标
        AND cagr_cv<θ_cv                          -- 收益稳定(更严, 0.15)
        AND NOT overfit_flag
   -- IC/ICIR 保留为列, 不进门(诊断)
   ```
   建议初始阈值(方法论, 待我用数仓分布校准): `θ_cagr=0.50, θ_mdd=0.25, θ_topk=0`(>0即正), `θ_cv=0.15`。
6. 迁移文件 `qe_archive_topk_rubric_<日期>.sql`（仿 `qe_archive_analytics_views_20260529.sql`）。

---

## 5. 选择/排序逻辑 re-point

1. **晋升(主)**：`v_promotion_candidates` 默认 `order_by` 改 `calmar`(=cagr/|mdd|) 或 `topk_return_20_mean`，IC 系列移出 order_by 白名单的"主"位（保留为可选诊断排序）。`repository.py:2115` 白名单加 `{calmar, max_drawdown_mean, cagr_cv, topk_return_20_mean, topk_hit_rate_20_mean}`。
2. **leaderboard**：`repository.py:1924` order_by 白名单加 `{max_drawdown, calmar, topk_return_20, topk_hit_rate_20, within_portfolio_rankic}`。
3. **seed_robustness**：`repository.py:1965` 同步加 topk_*。
4. **best-loop / loop_comparison**：`payload_summary.compact_loop_row`（:524，复核）当前以 IC/ICIR 为主呈现 → 改为以 CAGR/MDD/topk_return 为主、IC 降到诊断字段；`is_sota` 提示词（`register_evolution_v2_prompts.py:179`）同步把"看 IC"改"看 CAGR/MDD/topk"。
5. **不引入单一不透明加权 multi_score 作唯一选择器**（防 gaming）：用"硬门 + 主目标排序"模式（与现有一致），综合分仅作可选展示。

---

## 6. MCP 面（遵 MCP-first）

- **扩现有只读工具**：`qe_archive_query_run_leaderboard / promotion_candidates / seed_robustness` 的 `order_by` 入参增加 topk/mdd/calmar 选项（`mcp/modules/qe_archive.py:145`、`routers/qe_archive.py`，复核），返回新增 topk_* 列。
- **新只读工具** `qe_archive_query_topk_quality(run_id|task_id, k)`：直接查某 run/任务的 Tier-1 topk 指标。
- **回填工具** `qe_archive_backfill_topk_confirmed`（confirm 门控）：对存量 run 用现有 run_position/run_symbol_summary 纯 SQL 重算 topk 指标写 run_metric（无需重跑回测）。
- 全部带只读查询，确保智能助理可编排"按 topk 选腿/晋升"。

## 7. UI 面（遵观测性原则）

- leaderboard / promotion / loop 对比页：**主列改 CAGR/MDD/Calmar/topk_return@20/hit_rate**，IC/RankIC 移到"诊断"折叠子面板。
- 新增 **topk 质量卡**：topk_return@20 vs @50（decay）、hit_rate、within_portfolio_rankic、dispersion。
- 晋升候选页展示新门通过/未过 + 各阈值实际值。
- 回填任务进度可见。

---

## 8. 验收标准

- **功能**：任一 completed run 可经 MCP 查到 Tier-1 topk 指标；存量 616 run 回填完成；三视图含 topk 列；promotion 门切到 CAGR/MDD/topk。
- **正确性**：抽样 3-5 run 手工核对 topk_return@20 = 该 run 预测 rank≤20 持仓实际收益均值（与 enhanced_metrics 对账）。
- **行为变化**：promotion_candidates 排序在新口径下与旧 IC 口径**产生可解释的差异**（如某高 ICIR 低 CAGR 配置被降级）。
- **量化门槛(待校准)**：θ_cagr=0.50 / |MDD|≤0.25 / topk_return@20>0 / cagr_cv<0.15。
- **不回归**：IC 列仍可查(诊断)；既有依赖 IC 的消费方不报错(additive)。

## 9. 风险与门控

- ⚠️ `run_position`/`all_stocks` 的**粒度与排序语义须先核**（按 score 还是 profit 排序）——错了会让 hit_rate 失真。**实施第一步 = 抽样验证数据语义**。
- 阈值是方法论判断，**先用数仓现有分布校准**(我做)，避免拍脑袋。
- 迁移 additive、IC 保留 → 低回归风险。
- 禁 silent error：topk 算不出(缺 position 数据)的 run 写 null + 标记，不用 0 冒充。

## 10. 任务拆分（me / Codex）

| 任务 | owner |
|------|-------|
| topk 指标定义最终化 + 阈值用数仓分布校准 + 验收对账 | **我** |
| 数据语义核验(run_position 粒度/排序) | 我（先做）→ Codex 实施 |
| `_extract_topk_metrics` + 迁移 SQL(v_topk_quality/改3视图/门) | **Codex** |
| repository order_by 白名单 + MCP 工具(扩+新+回填) | **Codex** |
| UI(主列改 topk、IC 降诊断、topk 质量卡、回填进度) | **Codex** |
| loop_comparison / is_sota 提示词 re-point | Codex + 我评审 |

## 11. 测试

- 单测:`_extract_topk_metrics` 对构造 enhanced_metrics 的算值正确性。
- 回归:三视图 SQL 在样本库跑通 + 旧 IC 字段不变。
- MCP 冒烟:三 query 工具新 order_by + 新 topk_quality 工具 + 回填工具(confirm)。
- 对账:3-5 真实 run 手工核对 topk_return@20。
- 行为:回填后比较新旧 promotion 排序差异并人工 sanity check。

---

*落于 worktree `docs/ma1-multi-alpha-sourcing-20260615`。Tier-1 无需 pred.pkl 可立即开发;Tier-2 待 P2。实施前复核所有 file:line 与数据语义。*
