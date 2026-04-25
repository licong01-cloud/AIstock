# AIstock Multi-Alpha Production Grade v2.0.0

> 向下兼容 v1.0.0；修复 v1 三个方向性/稀释性/空心性 bug；新增方向感知、horizon argmax、
> 过拟合硬门槛、硬去重联动、multi_alpha_fitness 实质化打分。

## 核心修订（相对 v1.0.0）

### 修复 Bug
1. **方向性 bug（Bug 1）**：`economic_quality.top_excess_annual_return` 不再裸取原值；
   读取前先乘 `classification.direction`（缺失时用 `sign(rank_ic_mean)` 兜底）。
   Hard gates `s_excess_ann` / `s_monotonicity` 同样先乘方向。
2. **horizon 稀释（Bug 2）**：`horizon_strength` 从 4-horizon abs 等权平均 →
   `max(abs(rank_ic_{1,5,10,20d}))` argmax 模式；best_horizon 回写到 classification。
3. **multi_alpha_fitness 空心（Bug 3）**：从"字段存在性打分"重设计为：
   cluster_role / signal_mechanism / direction / horizon_class / data_source / sector_exposure
   六个分项实质打分，权重 15/100。

### 新增硬门槛
- **overfit_gate**: `ic_oos_is_ratio < 0.1` 强制 D；≥ A 需 ≥ 0.3；S 需 ≥ 0.5。
- **dedup_suppression**: `is_dedup_primary = false` 因子最高只能 C（追求最高收益 → 主动惩罚冗余，由 T9 硬去重脚本产出）。
- **horizon-tiered max_turnover**: S/A 级 turnover 门槛按 short/medium/long 三档分别设定。

### 新增软评分
- `best_horizon_advantage`（horizon argmax 领先幅度）
- `ic_sign_consistency_12m`（近 12 月月度 IC 方向一致性）
- `ic_oos_is_ratio`（样本外/样本内 IC 比值）

## 追求最高收益原则

- **multi_alpha_fitness 权重 15**：`IR = IC × √breadth`；单因子 IC 已触顶 ~0.06，breadth 是唯一放大器，必须重度计分。
- **overfit 严格立场（0.1 / 0.3 / 0.5 三档）**：单个过拟合因子在 regime change 时反向贡献，可吃掉 3-5 个健康因子的 alpha（Harvey-Liu 2015）。
- **硬去重强抑制（非 primary 最高 C）**：|corr|≥0.98 孪生因子是纯冗余，挤占组名额、污染聚类代表竞争，主动惩罚优于容忍。

## LLM 职责（与 v1 同）
LLM 仅基于 v2 规则引擎给出的评分做解释，不得修改 official_grade。
factor_analyst/analyze_factor_v2 prompt 同步升级以输出 direction / best_horizon /
signal_mechanism 三个新分类维度（T7）。

## 适用边界
- 启用前需完成 T3（classification 扩列 + direction 回填）、T5（月度 IC 新增指标回填）
- T9 硬去重未执行时，dedup_suppression 空跑（所有因子都是 primary）
- T10 聚类未执行时，cluster_role 全为 NULL，multi_alpha_fitness 仅给字段定义性分（最多 ~11/15）

## 回滚
UI 切换 `active_version` 到 `v1.0.0` 即回滚。v1/v2 评级结果按 `(factor_catalog_id, rule_version, snapshot_date)` 独立存储，互不覆盖。
