# 多 Alpha 全流程 · 分阶段实施计划（统合版）

> **类型**：实施计划（design）· 把 blueprint 正文 + 附录A + 附录B 统合为**可执行的分阶段路线**
> **日期**：2026-06-16
> **设计依据**：`multi_alpha_prediction_store_mlflow_blueprint_20260616.md`（为什么）；本计划 = 做什么 / 谁做 / 怎么验
> **状态**：待用户确认 → 逐阶段出详细设计 doc → 我或 Codex 开发 → MCP + UI 验收

---

## 1. 贯穿全程的工程原则（每阶段都遵守）

### ★ P-1 评估口径：CAGR/MDD + Top-K 为主，全局 IC/RankIC 降为诊断（本轮强化）
- 荐股 / topk 的收益来自**排名前 20-50 只** → **主考核 = 年化收益(CAGR) + 最大回撤(MDD) + Top-K 指标**：`precision@K / topK-forward-return / NDCG@K / topK-turnover / topK-decay`，K∈{20…50}。
- **全局 IC / RankIC / ICIR 降级为诊断指标**（看信号广度/健康度），**不再作"选最佳 loop / 选腿 / 组合权重优化"的主目标**。
- 选 loop、选腿、定组合权重一律按 **CAGR/MDD/Sharpe + Top-K** 排序。
- **backtest / paper / live 三处口径统一**（都只部署 topk）。
- 依据：附录 B3（LambdaRankIC 等证实 NDCG/Top-K 与全局 IC 数学上错配；NDCG top-heavy，IC 全截面等权）。
- 工程动作：qe_archive/eval 新增 Top-K 指标列；`promotion_candidates` 视图增加 Top-K 排序口径；组合回测器(P3)目标函数用 Top-K。

### ★ P-2 MCP-first：每个模块都可被智能助理经 MCP 调度+管理（本轮新增）
- AIstock 今后**主要由研发中的智能助理经 MCP 执行所有任务** → **每个新模块/能力必须暴露 MCP 工具**，否则助理无法编排。
- 凡"创建 / 运行 / 晋升 / 清理 / 再训练 / 组合回测 / 绑定"等动作 → 对应 MCP 工具，遵循三段式（`plan` → `validate` → `confirmed`）+ confirm 门控（高风险）+ 幂等。
- 凡状态 / 进度 / 结果 / 血缘 → 对应**只读 MCP 查询工具**。
- **禁止"只能在 UI 点击"的能力**（会成为助理编排盲区）。新模块设计评审必查"MCP 覆盖完整性"。

### ★ P-3 UI：全操作覆盖 + 以进展/状态/详情展示为主（本轮新增）
- UI **仍提供全部操作入口**（人可手动干预/兜底）。
- 但**主定位转为可观测性**：展示所有阶段的**进展 / 状态 / 指标 / 血缘 / 告警**详情。
- 每个 MCP 能力都要有对应 **UI 展示面**（进度 / 结果 / 历史 / 对比）。
- 智能助理跑的任务，UI 要能看到**全过程**（含 MCP 调用轨迹、confirm 审批、失败原因）。

### P-4 信号边界（铁律）
live_inference 实时推理（当日重算因子 + 冻结模型）产信号；**绝不回放回测 pred.pkl**；MLflow = 模型来源/版本治理，**不产信号**。

### P-5 advisory-first 三级门控
P4（advisory 非交易）→ P5（paper）→ P6（live），每级达标才进下一级。

### P-6 数仓 / MLflow 零重复
标量指标 → qe_archive（SoT）；重产物(pred.pkl/权重) → MLflow；数仓只存指针。

### P-7 安全 / 严谨性 / 治理
embargo + purge（h20 标签重叠必 purge）+ DEV/FINAL 两段冻结 holdout；live 门控加两级不确定性（OOS 稳定性 + 预测置信度）；禁 silent error；删除 / 生产动作 confirm 门控。

---

## 2. 分阶段计划（P1–P6 + 横切）

> 每阶段：目标 / 关键交付 / MCP 面 / UI 面 / 依赖+门控 / 建议 owner

### P1 单 Alpha 演进与固化 — [复用，在跑]
- **目标**：custom_evo 找 + 夯实独立腿；按 **CAGR/MDD/Top-K + 种子CV + gate** 准入；每腿 → single-alpha 包。
- **交付**：5+ 验证腿（C_FundVal 已成包 `pkg_c4703…`）；`promotion_candidates` 改 Top-K 口径。
- **MCP 面**：`qe_custom_evo_*`（有）、`qe_archive_query_promotion_candidates`（改 Top-K）、`strategy_packages_create_*`（有）。
- **UI 面**：演进轨迹 / loop 对比 / 腿登记表 / 晋升候选（Top-K 口径）。
- **依赖/门控**：无依赖；门控 n≥5 / CV<15% / Top-K-gate。
- **owner**：我（QE 编排）。

### P2 预测存储基础设施 — [新建，关键路径前段]
- **目标**：中心化 MLflow（复用现有 PG 独立 schema + 文件 artifact）；固化 pred.pkl；qe_archive 指针接线。
- **交付**：改造点①②③；`services/model_store` 模块；MLflow tracking(PG)+artifact 目录。
- **MCP 面（新）**：`prediction_store_get/pull`（按 run_id 拉 pred 指针/产物）、`mlflow_run_list/compare`、`model_store_health`。
- **UI 面**：MLflow run 对比 / pred 产物浏览 / 模型版本（**嵌入 AIstock 前端，不起独立 mlflow UI**）。
- **依赖/门控**：**磁盘治理(WSL 95%)为前置**；门控 pred.pkl 可按 run_id 拉取。
- **owner**：Codex（工程）/ 我评审。

### P3 多 Alpha 离线组合验证 — [新建，关键路径核心，最高杠杆]
- **目标**：组合回测器（拉各腿 pred.pkl → 截面 rank/zscore 融合 → 一次组合回测，**含真实摩擦/容量/换手**）；验 3.44 落地；扫权重。**目标函数 = Top-K + CAGR/MDD**（非全局 IC）。
- **交付**：`combination_backtester`；5 腿首次组合回测；权重方案（风险平价 / Sharpe-CV / Top-K 优化）。
- **MCP 面（新）**：`combo_backtest_plan/run/get`、`combo_weight_optimize`。
- **UI 面**：组合回测结果 / 权重 / 各腿贡献 / **滚动相关漂移监控**（B4：相关非恒定）。
- **依赖/门控**：P2(pred) + P1(腿)；门控 组合 Sharpe>2.45 + Top-K 达标 + 权重锁定。
- **owner**：Codex / 我。

### P4 荐股：alpha 级融合 — [升级]
- **目标**：advisory 从"**包级 rank 融合**"升级"**alpha 级分数融合**"；多 Alpha 包(`alpha_mode=MULTI_ALPHA`) + live_inference 各腿分数 → 加权 → 统一打分 → 每日 top20-50。
- **交付**：alpha 级融合逻辑；multi-alpha advisory program；用 P3 权重。
- **MCP 面**：`advisory_*`（有融合框架，扩 alpha 级）、`selection_center_*`。
- **UI 面**：每日荐股清单 / 各腿分数贡献 / 融合权重 / 制度状态（若启）。
- **依赖/门控**：P3(权重)；门控 advisory-first 表现达标（Top-K/CAGR/MDD）。
- **owner**：Codex（advisory 模块）。

### P5 模拟盘 — [复用]
- **目标**：多 Alpha advisory 绑 paper v2；实时推理 + 执行；测真实成本/滑点/容量。
- **交付**：多 Alpha paper 组合；执行质量报告。
- **MCP 面**：`paper_v2_*`（有）、advisory binding（有）。
- **UI 面**：paper 实时仪表 / 执行质量 / 与回测对比。
- **依赖/门控**：P4；门控 paper 长期≈回测预期 + OOS 稳定性 + 置信度（两级不确定性）。
- **owner**：Codex（paper v2）。

### P6 实盘（QMT）— [复用-gated]
- **目标**：晋升 live；Registry→Production；风控 + 渐进资金。
- **MCP 面**：`qmt_*`（监控有），下单受外部门控。
- **UI 面**：实盘监控 / 持仓 / 订单 / 风控告警。
- **依赖/门控**：P5；门控 外部审批 + paper 长期达标。
- **owner**：人审批 + Codex/我。

### 横切 track A — 滚动再训练（MLflow Registry champion/challenger + 自动回滚）
并入 **P2(Registry) + P1(滚动模式)**，服务 P4-P6 模型新鲜度。定时再训练 → OOS(embargo/purge) → 新版 Top-K 须超现役 → 晋升/回滚。

### 横切 track B — 制度感知（HMM 软后验 alpha 加权 + 相关漂移监控）
**P3.5 / P4.5**，静态多 Alpha 跑通后加。权重 = f(制度后验)；与既有板块 HMM 共存。

### 横切 track C — 事件驱动信号（α 腿 + 风险 overlay + 制度输入）
并行信号源轨道，重启停滞工作；成熟后并入 P3 组合。

---

## 3. 实施次序 / 关键路径 / 资源分工

- **关键路径**：磁盘治理 → **P2 → P3**（唯一真缺段）。P1∥P2 并行；P4-P6 主要复用既有轨道。
- **每阶段流程**：先出详细设计 doc（`impl_pX_*.md`）→ 评审 → 我或 Codex 开发 → **MCP + UI 双验收**（每个能力 MCP 可调 + UI 可见才算完成）。
- **me / Codex 分工建议**：
  - **Codex（工程重）**：P2 存储 + model_store、P3 组合回测器、P4 advisory alpha 融合、P5/P6 paper/qmt 接线。
  - **我（编排/分析/评估口径/评审）**：QE 演进编排、Top-K 评估口径落地、跨模块设计评审、各阶段详细设计 doc。

---

## 4. 每阶段详细设计 doc 统一模板（后续逐阶段填）

```
背景与目标 · 数据流图 · 接口契约(MCP工具签名+payload+confirm口径) · UI展示面
· DB/schema变更 · 验收标准(Top-K+CAGR/MDD 量化门槛) · 风险与门控
· 任务拆分(me|codex 颗粒) · 测试方案(单测+回归+MCP冒烟)
```

---

## 5. 待确认

1. 认可这份统合的分阶段实施计划?
2. **评估口径正式切到 "CAGR/MDD + Top-K 为主、IC 降诊断"** —— 确认?(影响 P1 选腿 + P3 目标函数)
3. **先为哪个阶段出详细设计?** 建议二选一:(a) 评估口径 refactor(P1/P3 基础、最契合本轮强调、自包含)；(b) P2 预测存储(关键路径起点)。
4. me / Codex 分工照 §3 建议?

*本计划落于 worktree `docs/ma1-multi-alpha-sourcing-20260615`，与 blueprint 同工作流。前沿论文结论需在我方数据复核后采纳。*
