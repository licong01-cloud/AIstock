# 因子分析 Agent 增强方案

## 一、现状问题

### 1.1 LLM 分析链路缺陷

当前 `FactorAnalyst`（`factor_analyst.py`）的 LLM 调用存在以下问题：

| 问题 | 现状 | 影响 |
|------|------|------|
| LLM 看不到指标 | 分类/描述只传入 factor_name + expression + code_text | 分类和描述缺乏量化依据，纯靠代码结构猜测 |
| 评级纯硬编码 | 只用 IC + Sharpe 两个阈值 | 忽略 ICIR、衰减、换手率、分组单调性等关键维度 |
| 3 次独立 LLM 调用 | 分类 → 评级(规则) → 描述，上下文割裂 | 延迟高、判断不一致 |
| 组合 IC 作为 fallback | 无独立指标时用 Task 组合 IC | 组合 IC 受其他因子干扰，不可靠 |
| 描述信息密度低 | 250 字纯文本，偏代码解读 | 无法为 QE 组合演进提供有效决策依据 |

### 1.2 指标使用现状

```python
# factor_analyst.py:585-590 — 独立指标优先，组合指标兜底
ind = self._get_independent_metrics(factor_name)
if ind:
    ic = ind.get("ic_mean") or ic
    sharpe = ind.get("top_excess_sharpe") or sharpe
    ann_ret = ind.get("top_excess_annual_return") or ann_ret
```

独立指标表 `aistock_factor_metrics` 已有丰富字段（17 个），但评级只用了 3 个，LLM 一个都没看到。

### 1.3 当前数据流

```
aistock_factor_catalog (expression, code_text, ic, sharpe)
    ↓
aistock_factor_metrics (独立IC系列指标, eval_window='full')
    ↓
_classify_with_llm(name, expression, code)  ← 无指标
    ↓
_grade_by_metrics(ic, sharpe, ann_ret)      ← 硬编码规则
    ↓
_generate_description_with_llm(name, code, expression)  ← 无指标
    ↓
qe_factor_classification (category, grade, description)
```

---

## 二、设计目标

1. 将独立指标注入 LLM prompt，使分析有量化依据
2. 合并 3 次 LLM 调用为 1 次结构化输出
3. 生成双描述：结构化档案（给 LLM/QE）+ 可读文本（给用户）
4. LLM 参与评级审核，而非纯硬编码
5. 独立 IC 为唯一评级依据，无独立指标标记"待评估"

---

## 三、双描述设计

### 3.1 结构化因子档案（`factor_profile` JSONB）— 给 LLM 读取

```json
{
  "category": "VOL",
  "category_reason": "基于成交量标准差计算，属于波动率/流动性类因子",
  "grade": "B",
  "grade_reason": "IC 中等(0.025)但 ICIR 较高(1.8)，稳定性好，上调至 B",
  "dimension": "time_series",

  "metrics_summary": {
    "ic_mean": 0.025,
    "rank_ic_mean": 0.031,
    "icir": 1.8,
    "rank_icir": 2.1,
    "ic_positive_ratio": 0.58,
    "ic_decay_half_life": 8,
    "top_excess_sharpe": 1.3,
    "top_excess_annual_return": 0.12,
    "group_return_monotonicity": 0.75,
    "turnover": 0.12,
    "rank_ic_1d": 0.035,
    "rank_ic_5d": 0.028,
    "rank_ic_10d": 0.020,
    "rank_ic_20d": 0.012
  },

  "usage_guidance": {
    "optimal_holding_period": "5-10d",
    "market_regime_fit": "震荡市优于趋势市",
    "complement_categories": ["MOM", "VAL"],
    "conflict_categories": ["LIQ"],
    "combo_role": "辅助因子",
    "suggested_weight_range": [0.05, 0.15]
  },

  "risk_notes": [
    "尾部回撤集中在流动性危机期间",
    "换手率偏高(12%)，需关注交易成本"
  ]
}
```

**用途**：
- QE 演进 agent 读取 `usage_guidance` 决定因子选择和权重
- QE 演进 agent 读取 `complement_categories` 构建低相关性组合
- QE 演进 agent 读取 `optimal_holding_period` 匹配策略调仓频率
- 演进迭代时对比 `metrics_summary` 和组合实际表现，定位问题因子

### 3.2 可读文本描述（`description` TEXT）— 给用户查看

> **Vol20d_Std** — 波动率因子 | 评级 B | 时序型
>
> 基于近 20 个交易日成交量标准差，捕捉流动性异常变化信号。IC 均值 0.025，ICIR 1.8，预测力中等但稳定性良好。IC 衰减半衰期约 8 天，适合周频到双周频调仓。多空组合年化超额 12%，Sharpe 1.3。分组收益单调性 0.75，因子区分度较好。建议作为辅助因子搭配动量/价值类因子使用，权重 5%-15%。注意尾部回撤风险，建议搭配低波因子对冲。

**特点**：
- 一段话涵盖：类别 + 评级 + 核心逻辑 + 关键指标解读 + 适用场景 + 组合建议 + 风险提示
- 300-500 字，信息密度高但可读
- 用户无需看 JSON 即可快速判断因子价值

---

## 四、合并后的 LLM 调用设计

### 4.1 单次调用，结构化输出

将分类、评级审核、维度判断、双描述生成合并为 1 次 LLM 调用。

### 4.2 输入（User Prompt）

```
## 因子信息
- 名称: {factor_name}
- 来源: {factor_source}
- 表达式: {expression}
- 代码片段: {code_text[:1000]}

## 独立评测指标
- IC均值: {ic_mean} | Rank IC均值: {rank_ic_mean}
- ICIR: {icir} | Rank ICIR: {rank_icir}
- IC正比例: {ic_positive_ratio} | IC衰减半衰期: {ic_decay_half_life}天
- 多空超额Sharpe: {top_excess_sharpe} | 多空超额年化: {top_excess_annual_return}
- 分组单调性: {group_return_monotonicity} | 换手率: {turnover}
- Rank IC (1d/5d/10d/20d): {rank_ic_1d}/{rank_ic_5d}/{rank_ic_10d}/{rank_ic_20d}
- 覆盖率: {coverage} | 交易天数: {n_trading_days}

## 规则预评级: {rule_grade}（仅基于 IC={ic_mean} + Sharpe={top_excess_sharpe}）

请综合以上信息，输出 JSON：
```

### 4.3 输出格式

```json
{
  "category": "12类之一",
  "category_reason": "分类理由",
  "grade": "S/A/B/C/D",
  "grade_reason": "评级理由，可调整规则预评级",
  "dimension": "cross_section 或 time_series",
  "description": "300-500字可读文本描述",
  "usage_guidance": {
    "optimal_holding_period": "Nd 或 Nd-Md",
    "market_regime_fit": "适用市场环境",
    "complement_categories": ["互补类别"],
    "conflict_categories": ["冲突类别"],
    "combo_role": "核心因子/辅助因子/对冲因子",
    "suggested_weight_range": [min, max]
  },
  "risk_notes": ["风险提示1", "风险提示2"]
}
```

### 4.4 System Prompt 要点

- 定义 12 个因子类别的判定标准
- 定义评级维度权重（IC 30% + ICIR 25% + 衰减 15% + Sharpe 15% + 单调性 15%）
- 提供 3-5 个 few-shot 示例（覆盖 S/A/B/C/D 各等级）
- 明确评级可以偏离规则预评级，但需给出理由
- 描述要求：涵盖核心逻辑 + 指标解读 + 适用场景 + 组合建议 + 风险提示

---

## 五、数据流（改造后）

```
aistock_factor_catalog (expression, code_text)
    ↓
aistock_factor_metrics (独立IC全量指标, eval_window='full')
    ↓                          ↓
    ↓                   无独立指标 → grade="P"(待评估), 跳过LLM
    ↓
_grade_by_metrics() → rule_grade（规则预评级，仅作参考）
    ↓
单次 LLM 调用（注入: name + expression + code + 全量独立指标 + rule_grade）
    ↓
输出 JSON → 拆分存储:
    ├── qe_factor_classification.factor_profile  (JSONB, 结构化档案)
    ├── qe_factor_classification.description     (TEXT, 可读文本)
    ├── qe_factor_classification.category
    ├── qe_factor_classification.grade
    └── qe_factor_classification.grade_reason
```

---

## 六、DB Schema 变更

### 6.1 `qe_factor_classification` 表新增列

```sql
ALTER TABLE qe_factor_classification
  ADD COLUMN IF NOT EXISTS factor_profile JSONB;
```

现有 `description` TEXT 列保留，存可读文本。`factor_profile` 存结构化 JSON。

### 6.2 评级 "P"（Pending）

无独立指标的因子，grade 设为 `"P"`（待评估），不参与 QE 组合选择：

```sql
-- 查询可用于 QE 组合的因子
SELECT * FROM qe_factor_classification WHERE grade != 'P';
```

---

## 七、Prompt 管理

### 7.1 现有 Prompt 管理体系

当前系统已有完整的 prompt 管理基础设施：

| 组件 | 说明 |
|------|------|
| `qe_agent_prompts` 表 | 存储 system_prompt + user_prompt_template，支持 is_active 版本切换 |
| `qe_agent_model_config` 表 | 存储每个 agent 的模型选择（model_id） |
| `prompt_manager.py` | CRUD 服务：list/get/create/update/delete prompt |
| `prompts/page.tsx` | 前端配置页面：prompt 编辑 + 模型选择 |

### 7.2 新增 prompt 记录

| agent_type | prompt_key | 用途 |
|-----------|-----------|------|
| `factor_analyst` | `analyze_factor_v2` | 合并后的单次分析 prompt（分类+评级+双描述） |

替代原有的（设为 inactive，保留可回退）：
- `factor_classifier` / `classify_factor`
- `factor_describer` / `generate_description`

### 7.3 Prompt 版本控制

通过 `qe_agent_prompts.is_active` 字段切换版本。旧 prompt 保留但设为 inactive，可随时回退。

详细的 Agent 合并方案见第十节，Prompt 配置页面整改见第十一节。

---

## 八、对 QE 组合演进的增强

### 8.1 因子选择阶段

QE 演进 agent 在选择因子时，可以查询：

```sql
SELECT factor_name, grade,
       factor_profile->'usage_guidance'->>'combo_role' AS role,
       factor_profile->'usage_guidance'->'complement_categories' AS complements,
       factor_profile->'usage_guidance'->'suggested_weight_range' AS weight_range,
       factor_profile->'usage_guidance'->>'optimal_holding_period' AS period
FROM qe_factor_classification
WHERE grade IN ('S', 'A', 'B')
ORDER BY grade;
```

### 8.2 演进迭代阶段

每轮 loop 结束后，QE agent 可以：

1. 对比组合实际 IC 与各因子 `metrics_summary.ic_mean` 的预期
2. 检查因子间是否存在 `conflict_categories` 冲突
3. 根据 `risk_notes` 判断回撤是否符合预期
4. 调整权重时参考 `suggested_weight_range`

### 8.3 人工组合辅助

用户在前端看到的因子卡片展示 `description`（可读文本），包含：
- 一眼可见的评级标签（S/A/B/C/D）
- 核心指标摘要
- 组合建议和风险提示

---

## 九、因子实战表现追踪体系

### 9.1 三层指标体系

因子的评估数据按可靠性从低到高分为三层：

| 层级 | 数据来源 | 可靠性 | 说明 |
|------|---------|--------|------|
| L1 独立回测 | `aistock_factor_metrics` | ★★☆ | 历史回测，存在过拟合风险 |
| L2 QE实验 | **已有表** `qe_factor_experiment_metrics` | ★★★ | 组合回测，反映因子在实际组合中的贡献 |
| L3 模拟实盘 | 新表 `factor_live_track` | ★★★★★ | 真实市场验证，无前视偏差 |

**核心观点：L3 > L2 > L1**

- L1（独立回测）是静态的、理想化的，容易高估因子能力
- L2（QE实验）引入了组合效应、调仓摩擦，更接近真实
- L3（模拟实盘）是唯一没有前视偏差的数据，最具参考价值

### 9.2 L2 — QE 实验表现追踪（已有功能）

**现状**：`qe_factor_experiment_metrics` 表已存在，且前端因子列表展开时已展示"历史实验表现"。

**已有表结构**（`init_catalog_db.py:404`）：

```sql
-- 已存在，无需新建
CREATE TABLE qe_factor_experiment_metrics (
    id SERIAL PRIMARY KEY,
    factor_name TEXT NOT NULL,
    factor_source TEXT NOT NULL,
    experiment_id TEXT NOT NULL,
    experiment_name TEXT,
    -- 性能指标
    ic DOUBLE PRECISION,
    icir DOUBLE PRECISION,
    rank_ic DOUBLE PRECISION,
    rank_icir DOUBLE PRECISION,
    -- 收益（无成本）
    ann_return_no_cost DOUBLE PRECISION,
    info_ratio_no_cost DOUBLE PRECISION,
    max_drawdown_no_cost DOUBLE PRECISION,
    -- 收益（含成本）
    ann_return_with_cost DOUBLE PRECISION,
    info_ratio_with_cost DOUBLE PRECISION,
    max_drawdown_with_cost DOUBLE PRECISION,
    -- 胜率与交易
    daily_win_rate DOUBLE PRECISION,
    weekly_win_rate DOUBLE PRECISION,
    sharpe_ratio DOUBLE PRECISION,
    calmar_ratio DOUBLE PRECISION,
    avg_turnover DOUBLE PRECISION,
    -- 元信息
    model_id TEXT,
    other_factors JSONB,
    data_split JSONB,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    raw_metrics JSONB,
    UNIQUE(factor_name, factor_source, experiment_id)
);
```

**已有数据写入**：`config_composer.py:_save_factor_experiment_metrics()` 在实验完成后自动拆解写入。

**已有前端展示**：`FactorList.tsx:241` 调用 `/quantevolver/factors/{factorName}/experiment-metrics` 获取数据，展开行显示历史实验表现（含汇总统计：实验次数、平均IC、最佳IC、平均年化等）。

**本次增强**：无需新建表，仅需在 `factor_profile` JSONB 中聚合 L2 数据供 LLM 读取（见 9.4）。

### 9.3 L3 — 模拟实盘表现追踪

模拟交易上线后，按日记录每个因子的实盘信号质量。

**新表 `factor_live_track`：**

```sql
CREATE TABLE IF NOT EXISTS factor_live_track (
    id SERIAL PRIMARY KEY,
    factor_name TEXT NOT NULL,
    strategy_id TEXT NOT NULL,          -- 关联的模拟交易策略
    trade_date DATE NOT NULL,
    -- 当日因子信号质量
    daily_ic DOUBLE PRECISION,          -- 当日截面IC
    daily_rank_ic DOUBLE PRECISION,
    -- 累计滚动指标（近20日/60日）
    rolling_20d_ic DOUBLE PRECISION,
    rolling_20d_icir DOUBLE PRECISION,
    rolling_60d_ic DOUBLE PRECISION,
    rolling_60d_icir DOUBLE PRECISION,
    -- 因子信号 vs 实际持仓的一致性
    signal_hit_rate DOUBLE PRECISION,   -- 信号方向正确率
    -- 实盘特有：滑点和冲击成本
    avg_slippage DOUBLE PRECISION,
    turnover_actual DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(factor_name, strategy_id, trade_date)
);

CREATE INDEX idx_flt_factor_date ON factor_live_track(factor_name, trade_date);
```

**数据写入时机**：每个交易日收盘后，模拟交易系统自动计算并写入。

### 9.4 因子档案中的实战指标聚合

`factor_profile` JSONB 扩展，增加 L2/L3 聚合数据：

```json
{
  "metrics_summary": { ... },
  "usage_guidance": { ... },
  "risk_notes": [...],

  "experiment_track": {
    "source": "qe_factor_experiment_metrics（已有表）",
    "total_experiments": 12,
    "avg_ic": 0.028,
    "avg_sharpe_ratio": 1.5,
    "avg_ann_return_no_cost": 0.15,
    "best_experiment_id": "qe_exp_a1b2c3",
    "worst_experiment_id": "qe_exp_d4e5f6",
    "last_experiment_date": "2026-02-28"
  },

  "live_track": {
    "days_tracked": 0,
    "status": "pending",
    "rolling_20d_ic": null,
    "rolling_20d_icir": null,
    "signal_hit_rate": null,
    "avg_slippage": null,
    "last_update": null
  }
}
```

当 L3 数据积累到 20 个交易日以上时，`live_track` 的指标开始有统计意义。

### 9.5 实战数据对 LLM 决策的价值

LLM 在做因子选择时，prompt 中注入三层指标的优先级逻辑：

```
## 因子表现数据（按可靠性排序）

### 模拟实盘（最可靠，如有）
- 跟踪天数: {days_tracked}
- 近20日IC: {rolling_20d_ic} | ICIR: {rolling_20d_icir}
- 信号命中率: {signal_hit_rate}
- 实际滑点: {avg_slippage}

### QE实验表现（如有）
- 参与实验数: {total_experiments}
- 平均边际IC贡献: {avg_marginal_ic}
- 平均组合Sharpe: {avg_combo_sharpe}

### 独立回测（基准参考）
- IC均值: {ic_mean} | ICIR: {icir}
- 多空超额Sharpe: {top_excess_sharpe}
```

**LLM 决策规则建议**（写入 system prompt）：
- 有 L3 数据且 ≥20 天：以 L3 为主要依据，L1 仅作参考
- 有 L2 数据但无 L3：以 L2 + L1 综合判断
- 仅有 L1：标注"仅回测验证"，降低置信度
- L3 表现显著低于 L1：标记"回测过拟合风险"，降级处理

### 9.6 评级动态调整

随着实战数据积累，因子评级应动态更新：

| 场景 | 评级调整 |
|------|---------|
| L3 连续 20 日 IC > L1 IC均值 | 可上调一级（如 B→A） |
| L3 连续 20 日 IC < L1 IC均值 × 0.5 | 下调一级 + 标记"回测过拟合" |
| L2 中 marginal_ic 持续为负 | 标记"组合贡献为负"，QE 演进时降低优先级 |
| L3 信号命中率 < 45% | 标记"信号失效"，暂停参与新组合 |

---

## 十、Agent 合并方案

### 10.1 现状分析

当前因子分析涉及两个独立 agent，各自有独立的 prompt 和模型配置：

| agent_type | prompt_key | 用途 | 调用位置 |
|-----------|-----------|------|---------|
| `factor_classifier` | `classify_factor` | 因子分类（12 类） | `factor_analyst.py:_classify_with_llm()` |
| `factor_describer` | `generate_description` | 因子描述生成 | `factor_analyst.py:_generate_description_with_llm()` |

**问题**：
- 两次独立 LLM 调用，上下文割裂，分类和描述可能不一致
- 前端 prompt 配置页面需要分别管理两个 agent 的 prompt 和模型选择
- 评级（`_grade_by_metrics`）是纯硬编码规则，不经过 LLM，无法被 prompt 配置页面管理

### 10.2 合并目标

将 `factor_classifier` + `factor_describer` + 硬编码评级 合并为单一 `factor_analyst` agent：

| 合并前 | 合并后 |
|--------|--------|
| `factor_classifier` / `classify_factor` | ~~废弃~~ |
| `factor_describer` / `generate_description` | ~~废弃~~ |
| `_grade_by_metrics()` 硬编码 | ~~废弃~~ |
| — | `factor_analyst` / `analyze_factor_v2` **（新）** |

### 10.3 DB 操作

```sql
-- 1. 插入新的合并 prompt
INSERT INTO qe_agent_prompts (agent_type, prompt_key, display_name, description, system_prompt, user_prompt_template, is_active, version)
VALUES ('factor_analyst', 'analyze_factor_v2', '因子综合分析',
        '合并分类+评级+描述为单次结构化输出',
        '{system_prompt}', '{user_prompt_template}', true, 1);

-- 2. 旧 prompt 设为 inactive（保留可回退）
UPDATE qe_agent_prompts SET is_active = false
WHERE agent_type IN ('factor_classifier', 'factor_describer');

-- 3. 新增 agent 模型配置
INSERT INTO qe_agent_model_config (agent_type, model_id, display_name, description)
VALUES ('factor_analyst', '{default_model_id}', '因子综合分析Agent',
        '负责因子分类、评级审核、双描述生成');
```

### 10.4 代码改动（`factor_analyst.py`）

```python
# 合并前：3 步调用
category = await self._classify_with_llm(name, expression, code)      # LLM 1
grade = self._grade_by_metrics(ic, sharpe, ann_ret)                    # 硬编码
description = await self._generate_description_with_llm(name, code, expression)  # LLM 2

# 合并后：1 步调用
result = await self._analyze_factor_v2(name, expression, code, independent_metrics, rule_grade)
# result 包含: category, grade, grade_reason, dimension, description, usage_guidance, risk_notes
```

### 10.5 前端 agent 类型列表更新

`frontend/src/app/quantevolver/prompts/page.tsx` 中 `AGENT_TYPES` 需更新：

```typescript
// 移除
{ value: "factor_classifier", label: "因子分类Agent" },
{ value: "factor_describer", label: "因子描述Agent" },

// 新增（如果 factor_analyzer 已存在则复用）
{ value: "factor_analyst", label: "因子综合分析Agent" },
```

> 注：当前前端已有 `factor_analyzer`（因子特征分析Agent），与本次新增的 `factor_analyst`（因子综合分析Agent）是不同职责。`factor_analyzer` 负责因子特征提取，`factor_analyst` 负责分类+评级+描述。

---

## 十一、Prompt 配置页面整改

### 11.1 现状

前端 `quantevolver/prompts/page.tsx` 管理两类配置：

| 配置类型 | 存储表 | 管理内容 |
|---------|--------|---------|
| Prompt 内容 | `qe_agent_prompts` | system_prompt, user_prompt_template, is_active, version |
| 模型选择 | `qe_agent_model_config` | model_id, system_prompt（冗余）, display_name |

**问题**：
- `qe_agent_model_config` 中有 `system_prompt` 字段，与 `qe_agent_prompts` 中的 `system_prompt` 冗余
- 同一个 agent 的 prompt 和模型分散在两个表，管理不直观
- 合并 agent 后，旧的 `factor_classifier` / `factor_describer` 配置项需要清理

### 11.2 整改方案

**方案：保持双表结构，前端统一展示**

不做表结构合并（避免大范围迁移），而是在前端将同一 agent 的 prompt 编辑和模型选择合并到同一个卡片/面板中：

```
┌─────────────────────────────────────────────┐
│ factor_analyst — 因子综合分析Agent           │
│                                             │
│ [模型选择] deepseek-chat ▼                  │
│ [System Prompt]  ┌──────────────────────┐   │
│                  │ 你是一个量化因子分析... │   │
│                  └──────────────────────┘   │
│ [User Prompt Template]  ┌──────────────┐   │
│                         │ ## 因子信息... │   │
│                         └──────────────┘   │
│ [版本] v1  [状态] ● Active                  │
│                              [保存] [测试]  │
└─────────────────────────────────────────────┘
```

### 11.3 前端改动要点

1. **合并展示**：每个 agent 一个卡片，同时展示模型选择（来自 `qe_agent_model_config`）和 prompt 编辑（来自 `qe_agent_prompts`）
2. **统一保存**：点击保存时同时写入两个表（已有 API `POST /agent-model-config/prompt`）
3. **清理旧 agent**：从 `AGENT_TYPES` 列表中移除 `factor_classifier` 和 `factor_describer`
4. **新增 `factor_analyst`**：添加到列表，配置默认 prompt 和模型

---

## 十二、Skill vs Prompt 结论

**继续使用 Prompt，不引入 Skill。**

理由：
- 因子分析是"给定数据 → 输出判断"的单轮推理，不需要多步工具调用
- 现有 `qe_agent_prompts` 表已支持版本控制和热更新
- 合并为单次调用后，prompt 的信息密度和输出质量已足够
- Skill 适合需要搜索、计算、多轮交互的复杂任务，因子分析不属于此类

**增强方向**：
- 精心设计 system prompt（含评级标准 + 类别定义 + 权重指引）
- 提供 5 个 few-shot 示例覆盖各等级
- temperature 保持 0.2 确保一致性
- 定期根据 QE 实验反馈迭代 prompt 版本

---

## 十三、实施步骤（最终版）

### 阶段一：DB + Agent 合并

1. **DB**: `qe_factor_classification` 新增 `factor_profile JSONB` 列
2. **DB**: 创建 `factor_live_track` 表（L3，预留，暂无数据写入）
3. **DB**: 插入 `factor_analyst/analyze_factor_v2` prompt 记录 + 模型配置
4. **DB**: 旧 `factor_classifier` / `factor_describer` prompt 设为 inactive

### 阶段二：后端重构

5. **factor_analyst.py**: 合并 `_classify_with_llm` + `_grade_by_metrics` + `_generate_description_with_llm` 为 `_analyze_factor_v2`
6. **factor_analyst.py**: 注入独立指标全量数据到 LLM prompt
7. **factor_analyst.py**: 无独立指标时 grade="P"，跳过 LLM
8. **factor_analyst.py**: 解析 LLM JSON 输出，分别写入 `factor_profile` 和 `description`
9. **factor_analyst.py**: 聚合 `qe_factor_experiment_metrics` 数据写入 `factor_profile.experiment_track`

### 阶段三：前端整改

10. **prompts/page.tsx**: 更新 `AGENT_TYPES`，移除 classifier/describer，新增 factor_analyst
11. **prompts/page.tsx**: 合并模型选择和 prompt 编辑到同一卡片
12. **因子详情页**: 展示新的 `description` 格式 + 实战追踪状态

### 阶段四：QE 演进增强

13. **QE 演进 agent**: prompt 中引用 `factor_profile` 数据（usage_guidance、risk_notes）
14. **QE 演进 agent**: prompt 中注入三层指标优先级逻辑（L3 > L2 > L1）

> **注**：L2 实验追踪功能（`qe_factor_experiment_metrics` 表 + 前端展示 + 数据写入）已完整实现，无需额外开发。
