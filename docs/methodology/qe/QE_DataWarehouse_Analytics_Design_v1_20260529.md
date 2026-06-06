# QE 数仓分析层设计 v1（DataWarehouse Analytics Design）

- **版本**：v1.0 ｜ **创建**：2026-05-29
- **用途**：把 `qe_archive` 从"实验历史记录"升级为"能直接产出有价值结论的数据仓库"。让智能助手 / Codex / 你 **不必全量分析所有历史 run**，而是查固定视图即可拿到：最佳配置、seed×超参性能、因子稳定性、过拟合红旗、晋升候选等。
- **配套**：方法论 `QE_Evolution_Methodology_v1`（视图是其考核指标 Part 6 的取数来源）；DDL 实现 `backend/db/migrations/qe_archive_analytics_views_20260529.sql`。
- **生产门禁**：本设计附带的 SQL **仅提交、未在生产库执行**，状态 `production_ddl_pending`。视图涉及的 `metric_key` 字符串需在 apply 前对照 `qe_archive.metric_taxonomy` 校验。

---

## 1. 现状诊断（为什么需要分析层）

`qe_archive` 底层数据其实很丰富——每个 run 存有 `run_metric`(~67 标量)、`run_curve`(~3500 NAV点)、`run_factor_importance`(~57)、`run_symbol_summary`(~1000)、`run_trade`(~4000)、`run_model_trial`、`run_reproducibility_manifest`(含 random_seed)。**真正缺的是聚合/语义层。**

### 已确认的缺陷（本设计一并记录，供后续修复）
| # | 缺陷 | 证据 | 影响 |
|---|------|------|------|
| D1 | **零个 SQL VIEW**，聚合全写死在 `repository.py` | 勘探确认 schema 无 view | 无法被 SQL/BI/其他工具复用，每次都要全量扫 run |
| D2 | `query_model_trials` 端点 **HTTP 500** | `repository.py:1708` 查询/序列化路径 | 模型试验分析不可用 |
| D3 | `score_total` 全 NULL | factor_usage/seed_trials 返回 null | `run_priority_score` 打分体系未落地，无法排序晋升 |
| D4 | `outbox` 积压 6892 条 pending | `qe_archive_health` | 新实验未及时归档，分析层数据滞后 |
| D5 | 因子重要性跨 seed 漂移未被常态化呈现 | `LargeOrder_Cost_Interaction` best_rank=1/avg_rank=40.6 | 决策者看不到"重要性不稳定"这一关键风险 |

> 视图设计同时是 D2/D3/D5 的对症解药（用视图替代 500 端点、用视图暴露 seed 稳定性）；D1/D4 需后续代码/运维修复（不在本次 SQL 范围内，已登记）。

---

## 2. 视图清单（8 个核心视图）

每个视图标注：粒度 / 源表 / 回答的问题 / 对应方法论考核项。

### V1 `v_run_leaderboard` —— 双轴运行榜
- **粒度**：run_id（仅 `research_valid=true` 且 `is_latest_attempt`）
- **源表**：`run` + `run_account_summary` + `run_metric`(透视 IC/ICIR/RankIC/RankICIR) + `run_reproducibility_manifest`(seed)
- **回答**：哪些 run 的信号轴/收益轴最佳？一行看齐双轴 + seed + 配置指纹。
- **考核项**：Part 6.1 双轴核心指标。

### V2 `v_seed_robustness` —— seed 鲁棒性（核心）
- **粒度**：配置指纹（factor_set_hash × model_type × label_horizon × undertrain_mode × topk）
- **源表**：`v_run_leaderboard` 之上按指纹聚合
- **回答**：某配置在多 seed 下的 `mean/std/cv/min`？诚实的生产预期是多少？是否稳定（cv<0.25）？
- **考核项**：Part 6.2 + 原则 1/2。**这是把"偶然冠军"挤掉、确立 Route C 集成预期的关键视图。**

### V3 `v_factor_importance_stability` —— 因子归因稳定性
- **粒度**：factor_name（× method）
- **源表**：`run_factor_importance` + `run_reproducibility_manifest`(seed)
- **回答**：每个因子跨 seed 的 `avg_rank / best_rank / std_normalized / distinct_seed_count`？哪些因子是稳定贡献者，哪些是 seed 噪声？
- **考核项**：Part 6.3 + 因子筛选 Step 4（稳定性筛选）。固化现有 MCP 同名聚合为 SQL。

### V4 `v_factor_performance` —— 因子表现足迹
- **粒度**：factor_name
- **源表**：`run_factor` + `v_run_leaderboard`（因子所在 run 的最佳/平均双轴）+（可选）`aistock_factor_metrics`
- **回答**：某因子参与过的 run 中，最佳/平均 CAGR、Sharpe、IC 是多少？使用频次？最近使用时间？
- **考核项**：因子筛选 Step 2/3（单因子体检 + 边际贡献参考）。

### V5 `v_model_hyperparam_seed_perf` —— 模型超参×SEED 性能（替代 500 端点）
- **粒度**：model_type × 超参指纹 × seed
- **源表**：`run` + `run_config`(model_params JSONB) + `run_model_trial` + `run_account_summary` + IC 指标
- **回答**：每个模型类型下，哪个超参档位 × 哪个 seed 表现最好/最稳？（直接回答用户"超参和SEED配置的性能分析"）
- **考核项**：模型演进 Part 5.2 第二层（超参搜索）+ Part 6.2。**同时是缺陷 D2 的可用替代品。**

### V6 `v_overfit_flags` —— 过拟合 / 方差尾部红旗
- **粒度**：run_id
- **源表**：`v_run_leaderboard` + `run` 的训练诊断（经 model_catalog 关联 training_failed/convergence）
- **回答**：哪些 run 收益轴爆表但信号轴平庸 / 欠训练却高收益 / 单 seed 远超集成均值？→ 标 `suspicious`。
- **考核项**：Part 6.4 红旗检测。**防 abbc/L16 陷阱的自动哨兵。**

### V7 `v_promotion_candidates` —— 晋升候选榜
- **粒度**：配置指纹
- **源表**：`v_seed_robustness` + `v_overfit_flags`（排除 suspicious）
- **回答**：哪些配置同时满足"双轴过线 + seed 稳定 + 无红旗"，可进 walk-forward / paper？
- **考核项**：Part 7 晋升漏斗（探索层→验证层 之间的闸门）。

### V8 `v_evolution_lineage` —— 演进血缘
- **粒度**：task × loop × experiment × run
- **源表**：`run`（含 task_id/loop_index/experiment_id）+ `v_run_leaderboard`
- **回答**：一个演进任务的轨迹——每轮动了什么、双轴如何变化、是否 SOTA？
- **考核项**：实验复盘 / Part 8 选基线。

---

## 3. 批量分析能力（基于视图的派生用法）

视图就位后，下列"批量分析"无需写新代码，一条 SQL 即可：
- **跨任务因子热度榜**：`v_factor_performance` ORDER BY best_sharpe —— 哪些因子在历史最佳策略里反复出现。
- **horizon 体检**：`v_run_leaderboard` GROUP BY label_horizon —— 各 horizon 的信号轴/收益轴分布（验证"h=20 信号最强"）。
- **seed 稳定性总览**：`v_seed_robustness` WHERE cv > 0.25 —— 哪些"冠军"其实不可复现。
- **超参先验**：`v_model_hyperparam_seed_perf` —— 为 Optuna 搜索空间提供历史最优中心。
- **晋升流水**：`v_promotion_candidates` —— 智能助手每次选基线/挑晋升直接读此榜，不再全量扫 run。

---

## 4. 落地与门禁

1. **SQL 文件**：`backend/db/migrations/qe_archive_analytics_views_20260529.sql`（CREATE OR REPLACE VIEW，幂等）。
2. **apply 前必须**：对照 `qe_archive.metric_taxonomy` 校验 IC/ICIR/RankIC/RankICIR 等 `metric_key` 的真实字符串（DDL 中已用占位常量并加注释）。
3. **执行**：本次**不在生产库执行**，报 `production_ddl_pending`。经你批准后由具备 DDL 权限的流程 apply 并验证（建表后 `SELECT count(*)` 抽验每个视图可查）。
4. **后续（不在本次范围，已登记）**：修 D2(model_trials 500) / D3(score_total 落地) / D4(outbox 积压消费)。

---

## 5. 视图与方法论考核项对照速查

| 方法论考核项 | 视图 |
|--------------|------|
| 6.1 双轴指标 | V1 `v_run_leaderboard` |
| 6.2 seed 鲁棒性 | V2 `v_seed_robustness` |
| 6.3 因子归因稳定性 | V3 `v_factor_importance_stability` |
| 6.4 过拟合红旗 | V6 `v_overfit_flags` |
| 因子筛选 Step2/3/4 | V3 / V4 |
| 模型演进超参×seed | V5 `v_model_hyperparam_seed_perf` |
| Part 7 晋升闸门 | V7 `v_promotion_candidates` |
| Part 8 选基线/复盘 | V1 / V8 `v_evolution_lineage` |
