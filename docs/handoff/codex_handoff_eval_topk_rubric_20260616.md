# Codex 开发交接 · QE 评估口径 refactor（CAGR/MDD + Top-K）

> **类型**：开发交接（handoff）· 模块：quantevolver / qe_archive（**独立模块，与 research-assistant 无关**）
> **日期**：2026-06-16
> **权威设计**：`docs/design/impl_eval_topk_rubric_20260616.md`（必读，尤其 §12 数据语义核验 + 阈值校准）
> **上游**：`docs/design/multi_alpha_phased_implementation_plan_20260616.md`（P-1 原则）· PR #1152 分支 `docs/ma1-multi-alpha-sourcing-20260615`
> ⚠️ 所有 file:line 为探查定位，**实施前复核当前代码**。

## 目标
QE 选 loop / 选腿 / 组合权重 / 晋升门的主指标，从全局 IC/RankIC/ICIR 切到 **CAGR + 最大回撤(MDD) + Top-K**；IC/RankIC/ICIR 保留为**诊断列**（移出晋升门与主排序）。原因：荐股只部署前 20-50，全局 IC 与 top-K 部署目标数学错配。

## ⚠️ Task 0 — 硬前置（开工第一件，阻塞后续）
验证 `run_position` 是否被填充 + 粒度 + rank 语义：(a) 每再平衡日每持仓一行？(b) `rank_in_portfolio` = 预测分数排名（非 realized）？(c) `score` = 预测分、`return_contribution` = 已实现贡献？
- **是** → Tier-1 后端可算 + 616 存量纯 SQL 回填。
- **否**（不含逐日预测 rank）→ 存量真预测 top-K 无法 SQL 回填，需 pred.pkl(P2) 或重跑；新 run 仍可在回测步前向算。
- **把结论回报 strategy session，再定回填路径。**

> 实测依据：MCP enhanced_metrics 只有已实现数据（all_stocks=realized profit 无预测 rank；top_stocks=按盈利额排序；prediction_diagnostics 仅 top30_stability/pred_rank_turnover）→ 预测对齐 top-K **不能**从 enhanced_metrics 算，必须后端 run_position 或回测内算 top30_stability 的同一步。

## 工程任务（Codex，T1-T6 详见设计 §3-§7、§12.4）
- **T1**：回测 prediction_diagnostics 步加 Tier-1 topk 计算（`topk_return@20/50`、`topk_hit_rate@20`、`topk_decay`、`within_portfolio_rankic`、`topk_dispersion@20`）写 enhanced_metrics。
- **T2**：`payload_extractor` 新增 `_extract_topk_metrics()` → 写 `run_metric`（JSONB pivot，无需加列）。（`_extract_account_summary:372` / `_extract_symbol_summaries:628` 复核）
- **T3**：迁移 SQL（additive，仿 `qe_archive_analytics_views_20260529.sql`）：新视图 `v_topk_quality`（按 run_id 聚合）；`v_run_leaderboard`/`v_seed_robustness` left join topk_*；**改 `v_promotion_candidates` 门**（见下）。
- **T4**：`repository.py:1924/1965/2115` order_by 白名单加 `{calmar, max_drawdown(_mean), cagr_cv, topk_return_20(_mean), topk_hit_rate_20, within_portfolio_rankic}`；MCP（`mcp/modules/qe_archive.py`、`routers/qe_archive.py`）扩 order_by + **新只读工具 `qe_archive_query_topk_quality(run_id|task_id,k)`** + （Task0 通过则）**confirm 门控 `qe_archive_backfill_topk_confirmed`**（纯 SQL 重算）。
- **T5**：UI 主列改 CAGR/MDD/Calmar/topk_return@20/hit_rate，IC/RankIC 移诊断折叠；新增 topk 质量卡（@20 vs @50 decay / hit_rate / within_rankic / dispersion）；晋升候选页展示新门各阈值实际值；回填进度可见。
- **T6**：`loop_comparison`/`compact_loop_row`（`payload_summary.py:524` 复核）+ `is_sota` 提示词（`register_evolution_v2_prompts.py:179` 复核）从「看 IC」改「看 CAGR/MDD/topk」（strategy session 评审）。

## 晋升门改造（阈值已用 leaderboard+promotion 实测分布校准，§12.3）
```
OLD: icir_mean>=0.5 AND ir_mean>=1.5 AND is_return_stable(CV<0.25)
NEW: cagr_mean>=0.60 AND max_drawdown_mean>=-0.20 AND cagr_cv<0.15
     AND topk_return_20_mean>=<指标算出后由 strategy session 二次校准> AND NOT overfit
-- IC/ICIR 保留列、移出门；默认 order_by=calmar
```

## 验收标准
1. 任一 completed run 经 MCP 可查 Tier-1 topk 指标；三视图含 topk 列；promotion 门切到 CAGR/MDD/topk。
2. 抽样 3-5 run 手工对账 `topk_return@20` = 预测 rank≤20 持仓实际收益均值。
3. promotion_candidates 新旧口径排序产生可解释差异（高 ICIR 低 CAGR 配置被降级）。
4. 不回归：IC 列仍可查（诊断）；既有依赖不报错（additive）。
5. Task0 通过则 616 存量回填完成。

## 约束（硬规则）
- **禁 silent error**：topk 算不出的 run 写 null + 标记，不用 0 冒充。
- 回填工具必须 confirm 门控。
- **MCP-first**：每个新能力都要有 MCP 工具（三段式 + 只读查询），禁止只能 UI 操作。
- **走 worktree 开发，不直接写 main**；改后提醒用户重启，**不自启服务**。
- additive 迁移、保留 IC 列。

## 分工
- **Codex**：T0-T6 工程。
- **strategy session（我）**：Task0 结论评审、`topk_return@20` 门二次校准（待指标算出）、验收对账、T6 评审。

## 测试
单测 `_extract_topk_metrics` 算值；视图 SQL 回归 + 旧 IC 字段不变；MCP 冒烟（扩 order_by + 新 topk_quality + 回填 confirm）；3-5 真实 run 对账；回填后新旧 promotion 排序 diff 人工 sanity。
