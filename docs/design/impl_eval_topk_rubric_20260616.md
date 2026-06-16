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

---

## 12. Step-1 数据语义核验 + 阈值校准结果（2026-06-16，我执行）

> 用 MCP 实测代表 run `qe_20260614_022643_edaf_L13`(C_FundVal) 的 enhanced_metrics + 数仓 leaderboard 分布。**修正了 §2 的一处可行性假设。**

### 12.1 数据语义实测（关键修正）
MCP `qe_experiment_get_enhanced_metrics` 返回的 enhanced_metrics 实际含：
- **`all_stocks`(460)**：全交易标的，每项 `{code, profit, profit_pct, avg_cost, last_price, first/last_date, holding_days}` —— **纯已实现结果，无预测 score/rank，非按预测排序**。
- **`top_stocks`/`bottom_stocks`(各10)**：**按已实现 profit 绝对额排序**（盈利冠军/亏损王），**不是预测 top-K** → 不可用于预测质量。
- `stock_trades`：per-stock 买卖流水(pnl/date/type/price/amount)。
- `prediction_diagnostics`：`{pred_std, top30_stability:0.843, pred_autocorr_1d:0.979, pred_rank_turnover:0.021}` —— 只有预测**稳定性**诊断，无 rank 分桶收益。
- `ic_series/rank_ic_series`(日,409)：**全局**。`summary`：标量。

**→ 修正结论：预测对齐的 top-K 收益（前20/前50 是否更好）MCP 层算不出**——enhanced_metrics 有"实际怎样"(all_stocks)但没有"预测排第几"。因此：
1. **Tier-1 必须后端计算**，源 = `run_position`(score/rank_in_portfolio/return_contribution，DB)**或**回测内计算 `top30_stability` 的同一步（那里有逐日预测分数，是最佳插入点——顺带把 `topk_return@20/50 / hit_rate / decay` 一起算进 `prediction_diagnostics`）。
2. **§2 Tier-1 "从 run_symbol_summary 直接算" 作废**：本 run 的 symbol summary 只会有 top10/bottom10(无 all_stocks→symbol 的全量)或 realized 排序，**不含预测 rank**。改以 `run_position` / 回测步为准。
3. **`top_stocks` 明确不可用**（realized-sorted）。

### 12.2 回填可行性（修正：从"纯SQL必可回填"→"条件可回填，需先验证")
- 存量 616 run 要回填**真·预测 top-K**，前提是 `run_position` 已存逐再平衡的**预测 rank + realized 贡献**。**Codex 第一步必须先验证 `run_position` 是否被填充、粒度、rank 语义**。
- 若 `run_position` 不含逐日预测 rank → 存量真预测 top-K **无法纯 SQL 回填**，需 pred.pkl(P2) 或重跑；新 run 则在回测步直接算(forward)。
- `prediction_diagnostics.top30_stability` 已存在 → 证明回测在跑时**确有逐日预测分数** → 新 run 的 top-K 计算成本极低(就在那一步加)。

### 12.3 阈值校准（用 leaderboard + promotion_candidates 实测分布）
- 观测：头部 run CAGR≈0.98-1.12 / MDD≈-0.13~-0.20 / Calmar≈5-7 / Sharpe≈2.3-2.7；gate 通过的**配置均值** CAGR_mean 0.745-0.962、cagr_cv 3-11%、mdd_mean -0.15~-0.19。
- **校准后的晋升门(配置级)**：
  - `cagr_mean ≥ 0.60`（生产候选；0.50 作"观察"软档）
  - `max_drawdown_mean ≥ -0.20`（|MDD|≤20%，头部实测 -0.15~-0.19）
  - `cagr_cv < 0.15`（收紧；现 is_return_stable 的 0.25 过松）
  - `topk_return@20` 门：**待指标算出后用其分布二次校准**（占位 >0 且 > 基准）
  - 排序默认 `calmar`(头部 5-7) 或 `cagr_mean`，`topk_return@20` 为 tiebreak
- IC/ICIR 退出门(仅诊断)的决定**维持**。

### 12.4 对 §8-§11 的影响
- §3/§5 数据源：Tier-1 主源改 `run_position` / 回测 prediction_diagnostics 步（非 enhanced_metrics MCP、非 run_symbol_summary）。
- §6 MCP：仍需新增 `qe_archive_query_topk_quality` 只读工具（当前完全无预测-rank-top-K 的 MCP 出口，是 MCP-first 缺口）。
- §9 风险首条升级为**硬阻塞前置**：Codex 开工第一件事 = 验证 `run_position` 是否含逐再平衡预测 rank;结果决定回填走 SQL 还是 P2。
- §2 Tier-2 不变(全截面 precision@K/NDCG@K 仍需 pred.pkl)。

---

---

## 13. Task0 核验裁决（2026-06-16，Codex 执行 + strategy session 评审）

**Task0 实测结论（Codex，带 file:line）：`run_position` 表全空——0/616 run 有 position 行**；归档写入链路从不写 run_position（schema `init_qe_archive_schema.py:473` / 写入 `archive_service.py:94` / extract payload 无 positions 字段 `payload_extractor.py:36`）；但回测 artifact 阶段 `read_exp_res.py:309` **已有 pred.pkl 的 score/rank 逻辑**。存量 616 全无、时间分布 2026-04/05/06 均 0。Codex 荐路径 **B（需 pred.pkl/重跑，不能纯 SQL 回填）**。

### 裁决（据此重构，**绕开 run_position**）：
1. **Tier-1 算法位置改定**：**直接在回测 `read_exp_res.py:309` 那一步算**（pred 分数 + 已实现收益都 live）→ 写 `enhanced_metrics.prediction_diagnostics`（topk_return@20/50、hit_rate、decay、within_portfolio_rankic、dispersion）。`payload_extractor` 仅把这些已算好的值**透传**进 `run_metric`。**§2/§3/§5 里"从 run_position/run_symbol_summary 算"作废，run_position 不再是依赖**（其全空对前向路径无影响）。
2. **不做存量 616 mass 回填**：pred.pkl 多已随 workspace 清理 → 真预测 top-K 无法重建。**前向 only**。
3. **晋升门 null-tolerant 分级（关键，让 refactor 立即可上)**：
   - **硬门(现在即可，全 run 都有 Tier-0)**：`cagr_mean≥0.60 AND |max_drawdown_mean|≤0.20 AND cagr_cv<0.15`，IC/ICIR 退诊断。
   - **软门(topk_return_20)**：present 才参与；**null → 排除出门判定，不用 0 冒充**（遵禁 silent error）。待新 run 累积 top-K 后再升为硬门。
4. **目标腿定向重跑(strategy session 任务,非 Codex)**：T1 上线后,对 ~5-7 个 alpha 腿代表配置定向重跑(确定性 seed,成本小)拿其 top-K;或并入下一 Line A 轮次。R21(b6af/f858)跑在旧码、本身无 top-K。
5. **run_position 填充**降级为**可选未来增强**(若日后要 per-position 明细)；本 refactor 不需要。

### 对 §3/§5/§8/§10 的净影响：
- 数据源：Tier-1 = 回测 read_exp_res 步(in-process pred)→ enhanced_metrics → run_metric;**删 run_position 路径**。
- T2(payload_extractor)：从"算"改"透传 enhanced_metrics 里已算的 topk_*"。
- 回填工具 `qe_archive_backfill_topk_confirmed`：**本期不建**(无源可回填);留待 P2/重跑后。
- 验收：以**新 run**(T1 上线后跑的)验 topk 指标 + 门;存量用 Tier-0(CAGR/MDD)即可。
- MCP `qe_archive_query_topk_quality`、UI topk 卡、门改造、order_by 扩展、loop/is_sota re-point：**不变,照做**。

---

*落于 worktree `docs/ma1-multi-alpha-sourcing-20260615`。§12 数据语义+阈值;§13 Task0 裁决：run_position 全空→Tier-1 改在回测 read_exp_res.py:309 步算(绕开 run_position)、前向 only 不 mass 回填、门 null-tolerant 分级、腿定向重跑。Tier-2 待 P2。实施前复核 file:line。*
