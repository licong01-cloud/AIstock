# 单 Alpha 策略设计 — 基于 1000 万小资金 + A 股市场

**作者**: Strategy session + User vision
**日期**: 2026-05-12
**类型**: 量化策略架构 + UI/Workflow 设计
**适用阶段**: 单 Alpha 起步 (不考虑多 Alpha / LLM 自动演进)
**前提**: 整改方案 (`paper_v2_qe_integration_overhaul_20260512.md`) 实施后启用

---

## §1 关键假设与设计原则

### 1.1 实际场景 (用户输入)

| 维度 | 设定 |
|---|---|
| 资金规模 | **~1000 万元** (小资金) |
| 市场 | A 股 (主板 + 中小板 + 创业板 + 科创板) |
| 交易频率 | 中频 (周/月级换仓) |
| 中性化策略 | **不做行业/市值中性化** — 反而追板块轮动 + 小市值机会 |
| 模型 | **不局限 LightGBM** — 历史 57 因子 + LSTM 表现最佳, 应保留多模型支持 |
| 配置模式 | 手动 (每 loop 自定义), 不需 LLM 自动演进 |
| 第一目标 | 赚钱: 年化收益 + 最大回撤 + 稳定性 |

### 1.2 设计原则 (赚钱第一)

1. **小资金 ≠ 机构逻辑**: 不照搬 Barra factor model, 不照搬"行业市值中性化"
2. **板块轮动 + 小市值 是 alpha 来源**, 不是 noise
3. **模型不限制**: LightGBM / LSTM / Transformer / 集成模型皆可, 视实验效果选
4. **简单可解释 ≥ 复杂高 ML**: 但**不排斥复杂模型**, 如有显著 OOS 优势
5. **稳定性优先**: Sharpe 1.5 with max DD 15% > Sharpe 2.5 with max DD 40%
6. **必须防止**: 未来信息泄露 + 过拟合 + 数据 snooping
7. **真模拟盘验证 → 才进实盘**

---

## §2 因子选择 — 调整后建议 (无中性化)

### 2.1 因子分类 (12 大类, 单 Alpha 起步选 5-8 类)

不做中性化, 因子组合策略改变:

| 类型 | 代表因子 | 1000 万小资金 + A 股 适配 |
|---|---|---|
| **板块/行业动量 Sector Mom** | 行业指数 6m / 12m return, 行业 RS rank | **核心 alpha**, 追板块轮动 |
| **小盘溢价 Size** | 自由流通市值 (倒序), log_mv | **核心 alpha**, 1000 万级可吃小盘 |
| **个股动量 Momentum** | 12-1 mom, 6m mom, sector rel mom | A 股散户主导, 动量效应强 |
| **反转 Reversal** | 1m 短期反转, 1w intraday reversal | A 股短期反转明显 |
| **资金流 Money Flow** | mf_main_net_amt_std, 北向资金, 大单买卖 | A 股大资金信号强 |
| **波动 Volatility** | 60d vol, idio_vol, vol of vol | 波动率溢价 (反向) |
| **质量 Quality** | ROE, gross_margin, OCF/NI | 长期 alpha, 稳定加分 |
| **估值 Value** | P/B, P/E, EV/EBITDA, dividend_yield | 不是主导, 但防 garbage stocks |
| **成长 Growth** | revenue_yoy, EPS_yoy, profit_yoy | 高弹性 + 高风险, 谨慎用 |
| **流动性 Liquidity** | turnover, Amihud illiquidity | 小盘必须过滤极差流动性 |
| **微结构 Microstructure** | bid-ask spread, order imbalance | 中频策略边际作用 |
| **事件驱动 Event** | 龙虎榜, 解禁, 限售解禁, 业绩快报 | A 股事件驱动有效, 但稀疏 |

### 2.2 推荐起步组合 (10-15 因子)

```
板块动量      2 个 (行业 6m return + 行业 RS rank)
小盘溢价      1 个 (log free_mv, 倒序)
个股动量      2 个 (12-1 mom + 6m mom)
反转          1 个 (1m reversal)
资金流        2 个 (mf_main_net + 北向资金净流入)
波动          1 个 (idio_vol)
质量          2 个 (ROE + gross_margin)
估值          1 个 (P/B, 仅作 garbage filter)
流动性        1 个 (turnover, 用于过滤极差流动性)

总计:         13 个因子 (跨 8 类型)
```

**与机构组合差异**:
- 没"行业中性"约束 → 允许超配热点行业
- 没"市值中性"约束 → 允许超配小盘
- 流动性过滤 (而非中性化): 剔除 ADV < 500 万的股票
- 估值因子降权 (仅防 garbage), 不主导

### 2.3 因子相关性 + 类型 + 独立指标 (UI 设计)

#### UI 需求 (用户提出)

> 因子选择应有**分类选择框**, 每类选 N 个因子。选 A 因子后, 同类且**相关性 > 0.7** 的 B 因子**自动变色** (警告), 方便筛选。

#### 推荐 UI 实现

```
┌────────────────────────────────────────────────────────────────┐
│  因子选择 — 当前 [13 / max 20]    [按类型分组 ▼] [按 IC 排序▼]│
├────────────────────────────────────────────────────────────────┤
│ ▼ 板块动量 (Sector Momentum)    选中 [2/5]                     │
│   ☑ industry_6m_return       IC 0.045 ICIR 0.72 ✓             │
│   ☑ industry_rs_rank          IC 0.038 ICIR 0.68               │
│   ☐ industry_3m_return        IC 0.041 [⚠ 与 industry_6m_return│
│                                          相关 0.85, 选 1 个]   │
│   ☐ industry_momentum_zscore  IC 0.032 ICIR 0.55               │
│   ☐ industry_12m_return       IC 0.029 ICIR 0.48               │
│                                                                 │
│ ▼ 小盘溢价 (Size)               选中 [1/3]                     │
│   ☑ log_free_mv (reversed)    IC 0.052 ICIR 0.81 ✓             │
│   ☐ log_total_mv (reversed)   IC 0.044 [⚠ 与 log_free_mv      │
│                                          相关 0.92]            │
│   ☐ size_decile               IC 0.041 [⚠ 与 log_free_mv      │
│                                          相关 0.89]            │
│                                                                 │
│ ▼ 个股动量 (Momentum)           选中 [2/5]                     │
│   ☑ momentum_12_1              IC 0.038 ICIR 0.62             │
│   ☑ sector_rel_momentum        IC 0.034 ICIR 0.58             │
│   ☐ momentum_6m                IC 0.031 [⚠ 与 momentum_12_1   │
│                                          相关 0.78]            │
│   ☐ ...                                                         │
│                                                                 │
│ ▼ 反转 (Reversal)               选中 [1/3]                     │
│   ☑ reversal_1m                IC 0.029 ICIR 0.52             │
│   ☐ ...                                                         │
│                                                                 │
│ ... (其他类型省略)                                              │
│                                                                 │
├────────────────────────────────────────────────────────────────┤
│ 当前 13 因子配置:                                               │
│ ├─ 平均 IC: 0.038                                              │
│ ├─ 平均 ICIR: 0.63                                              │
│ ├─ 类型平衡: 板块 2 / 小盘 1 / 动量 2 / 反转 1 / 资金 2 ...     │
│ └─ 最高相关: 0.61 (在阈值 0.7 内 ✓)                            │
│                                                                 │
│              [< 上一步: 投资思路]   [下一步: 模型 >]            │
└────────────────────────────────────────────────────────────────┘
```

#### 颜色规则

| 状态 | 显示 |
|---|---|
| 未选 + 无警告 | 默认 (灰字) |
| 未选 + 同类且相关 > 0.7 | **红字 + ⚠ 警告 tooltip** (显示相关 0.85, 建议选 1 个) |
| 未选 + 同类且相关 0.5-0.7 | 黄字 + ⚠ tooltip (建议组合权重低) |
| 已选 | **绿字 + ☑** |
| IC < 0.02 | 灰色 + "(IC 低)" 标注 |
| ICIR < 0.3 | 灰色 + "(不稳定)" 标注 |

#### 实时数据更新

- 选/取消因子 → 实时 recompute 平均 IC / ICIR / 类型平衡 / 最高相关
- 选超过 20 个 → "组合过大, 建议精简" 警告
- 类型分布不均 (某类 > 50% 因子) → "类型集中, 缺乏多样性" 警告

---

## §3 模型选择 — 不限制 LightGBM, 支持 LSTM/Transformer

### 3.1 模型类型 (UI 选项)

| 模型 | 适用场景 | 起步推荐度 |
|---|---|---|
| **Linear (Ridge/Lasso/ElasticNet)** | 简单解释性, 小因子 (< 20) | 🟢 baseline |
| **LightGBM / XGBoost / CatBoost** | 中小数据 + 中等因子 (10-50) | 🟢 推荐起步 |
| **Random Forest** | 简单 ensemble, 不推荐 (LightGBM 替代) | 🟡 备选 |
| **LSTM / GRU** | 时序数据 + 大因子 (50+) | 🟢 **复杂场景推荐** (用户实验证明) |
| **Transformer** | 极长时序 + 多模态 | 🟡 高级 |
| **Stacking (Linear + GBDT + LSTM)** | 多模型集成 | 🟢 高级 ensemble |
| **NN (MLP)** | 一般不推荐 (LightGBM 替代) | 🔴 不推荐 |

### 3.2 起步建议 (基于因子数量)

```
因子 5-15 个   → Linear + Ridge / LightGBM Conservative
因子 15-30 个  → LightGBM Balanced (推荐)
因子 30-60 个  → LightGBM Aggressive OR LSTM Balanced
因子 60+      → LSTM / Transformer / Stacking
```

→ 历史 57 因子 + LSTM 表现最佳验证了"高因子数 LSTM 更合适"

### 3.3 模型参数预设 (3 levels)

#### LightGBM 预设

```python
Conservative = {
  'learning_rate': 0.01, 'max_depth': 4, 'num_leaves': 15,
  'lambda_l1': 0.5, 'lambda_l2': 0.5,
  'subsample': 0.7, 'colsample_bytree': 0.7,
  'min_child_samples': 50,
  'early_stopping_rounds': 30,
}
Balanced = {
  'learning_rate': 0.02, 'max_depth': 5, 'num_leaves': 31,
  'lambda_l1': 0.1, 'lambda_l2': 0.1,
  'subsample': 0.8, 'colsample_bytree': 0.8,
  'min_child_samples': 20,
  'early_stopping_rounds': 50,
}
Aggressive = {
  'learning_rate': 0.05, 'max_depth': 7, 'num_leaves': 63,
  'lambda_l1': 0.01, 'lambda_l2': 0.01,
  'subsample': 0.9, 'colsample_bytree': 0.9,
  'min_child_samples': 10,
  'early_stopping_rounds': 100,
}
```

#### LSTM 预设

```python
Conservative = {
  'hidden_size': 32, 'num_layers': 1, 'dropout': 0.3,
  'sequence_length': 20,        # 4 周回看
  'learning_rate': 0.001, 'batch_size': 128,
  'epochs': 50, 'early_stopping_patience': 10,
}
Balanced = {
  'hidden_size': 64, 'num_layers': 2, 'dropout': 0.2,
  'sequence_length': 40,        # 8 周回看
  'learning_rate': 0.001, 'batch_size': 256,
  'epochs': 100, 'early_stopping_patience': 15,
}
Aggressive = {
  'hidden_size': 128, 'num_layers': 3, 'dropout': 0.1,
  'sequence_length': 60,        # 12 周回看
  'learning_rate': 0.0005, 'batch_size': 512,
  'epochs': 200, 'early_stopping_patience': 25,
}
```

### 3.4 LSTM 特别防呆 (用户实验场景适用)

| 风险 | 防护 |
|---|---|
| 过拟合 | Dropout 0.2-0.3 必加 + Early stopping 必开 |
| Future information leakage | sequence input 严格 historical, target 必须 future |
| Sequence length 过长 | ≤ 60 (12 周), > 60 信息衰减 + 训练慢 |
| Gradient explode | gradient_clip = 1.0 |
| 不可解释 | SHAP / Integrated Gradients 帮诊断 |
| 训练慢 | GPU 必须, 大 batch_size |
| 不稳定 | Random seed fixed + 多 seed 平均 (3-5) |

---

## §4 Loop History 持续追踪 (数仓集成)

### 4.1 现状 + 用户需求

**现状**: 每 loop 进数仓 (R6 schema 已有 `strategy_pkg.package_validation_run`)
- metrics_json (annual_return / Sharpe / DD / IC ...)
- artifact_manifest_json (因子 list + 模型参数)
- evidence_json (regime metrics)

**用户需求**: UI 持续展示历史 loop, 帮助决策"下个 loop 调什么"

### 4.2 推荐 UI: Loop History 页面

```
┌────────────────────────────────────────────────────────────────┐
│  QE 实验: qe_20260508_060509_1268 (Loop 模式)                  │
│                                                                 │
│  历次 Iteration 对比 (Loop 1 → Loop N)                         │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  性能时序图:                                                    │
│       Sharpe                                                    │
│  1.5 ┤                                          ●─●            │
│      │                              ●─●─●                       │
│  1.0 ┤                  ●─●                                     │
│      │      ●                                                   │
│  0.5 ┤  ●                                                       │
│      └─────────────────────────────────────────                │
│      L1  L2  L3  L4  L5  L6  L7  L8  L9  L10                    │
│                                                                 │
│  Iteration 列表:                                                │
│  Loop 8 (2026-05-10 14:22) ✓ APPROVED                          │
│    ├─ 因子: 13 个 (vs Loop 7: +2 板块动量, -1 反转)             │
│    ├─ 模型: LightGBM Balanced (vs Loop 7: lr 0.02→0.03)         │
│    ├─ IC: 0.045 (+0.008) ✓                                     │
│    ├─ Sharpe: 1.42 (+0.18) ✓                                   │
│    ├─ Max DD: 14% (-2%) ✓                                      │
│    └─ Annual Return: 19.2%                                      │
│                                                                 │
│  Loop 7 (2026-05-10 09:15)                                     │
│    ├─ 因子: 12 个 (vs Loop 6: +1 资金流)                        │
│    ├─ 模型: LightGBM Balanced                                   │
│    ├─ IC: 0.037                                                 │
│    ├─ Sharpe: 1.24                                              │
│    └─ Annual: 16.8%                                             │
│                                                                 │
│  ... (更多 iteration)                                           │
│                                                                 │
│  ────────────────────────────────────────────                  │
│  🎯 下个 Loop 推荐方向 (基于 trend):                            │
│  ├─ Sharpe trend ↑, 继续因子优化                                │
│  ├─ 考虑加 1 个事件驱动因子 (event-based, 未尝试)               │
│  ├─ 模型尝试 LSTM 对比 (因子已 13 个, 可能 LSTM 更优)            │
│  └─ Turnover 38% 偏高, 考虑加 turnover penalty                  │
│                                                                 │
│  [启动 Loop 9 (基于 Loop 8 配置)]                              │
└────────────────────────────────────────────────────────────────┘
```

### 4.3 Diff View (Loop N vs Loop N-1)

```
┌────────────────────────────────────────────────────────────────┐
│  Loop 7 → Loop 8 差异                                          │
├────────────────────────────────────────────────────────────────┤
│  因子组合:                                                      │
│  + 加入 industry_rs_rank (Sector Mom 类, IC 0.038)              │
│  + 加入 industry_12m_return (Sector Mom 类, IC 0.029)           │
│  - 移除 reversal_2w (Reversal 类, IC 0.018 — 过低)              │
│                                                                 │
│  模型参数:                                                      │
│  ~ learning_rate: 0.02 → 0.03 (+50%)                            │
│  ~ max_depth: 5 → 6 (+1)                                        │
│  ~ subsample: 0.8 → 0.85 (+0.05)                                │
│                                                                 │
│  Portfolio Rules:                                               │
│  ~ 持仓数: 30 → 50 (+67%)                                       │
│  ~ 行业上限: None → None (保持无中性化)                          │
│                                                                 │
│  性能变化:                                                      │
│  IC: 0.037 → 0.045 (+22%) ✓ 显著提升                            │
│  Sharpe: 1.24 → 1.42 (+15%) ✓                                  │
│  Max DD: 16% → 14% (-2%) ✓                                     │
│  Annual: 16.8% → 19.2% (+2.4%) ✓                               │
└────────────────────────────────────────────────────────────────┘
```

### 4.4 数仓 schema 增量 (利用现有)

`strategy_pkg.package_validation_run` 已有:
- `metrics_json` ✓
- `artifact_manifest_json` (存因子 + 模型参数完整 manifest) ✓
- `evidence_json` ✓

**只需 UI 加载 + 展示**, 不需 schema 改动。

### 4.5 推荐方向算法 (简单 rule-based)

```python
def recommend_next_loop_direction(loops: list[Loop]) -> dict:
    last = loops[-1]
    prev = loops[-2] if len(loops) >= 2 else None
    
    recommendations = []
    
    # Rule 1: Sharpe trend
    if last.sharpe > prev.sharpe + 0.05:
        recommendations.append("Sharpe 上升趋势, 继续当前方向")
    elif last.sharpe < prev.sharpe - 0.05:
        recommendations.append("Sharpe 下降, 考虑 revert Loop N-1 配置")
    
    # Rule 2: 因子类型缺失
    used_types = set(f.type for f in last.factors)
    missing_types = ALL_TYPES - used_types
    if missing_types:
        recommendations.append(f"未尝试类型: {missing_types}, 考虑加 1 个")
    
    # Rule 3: Turnover 过高
    if last.turnover > 0.35:
        recommendations.append("Turnover 偏高, 考虑加 turnover penalty 或减反转因子")
    
    # Rule 4: 模型类型对比
    if last.model_type == "LightGBM" and len(last.factors) > 30:
        recommendations.append("因子数较多, 可尝试 LSTM 对比")
    
    # Rule 5: OOS gap
    if last.is_metric / last.oos_metric > 1.3:
        recommendations.append("过拟合迹象 (IS/OOS > 1.3), 加正则 / 减深度")
    
    return {"recommendations": recommendations}
```

---

## §5 QE 配置 → 独立页面 (替代弹窗)

### 5.1 现状 + 用户需求

**现状**: QE 配置在弹窗, 显示受限
**用户需求**: 改为独立页面

### 5.2 页面规划

```
/qe                              QE 主页 (实验列表)
/qe/experiments/new              新建单次实验 (独立页面)
/qe/experiments/<id>             实验详情
/qe/experiments/<id>/results     回测结果
/qe/loops                        Loop 列表
/qe/loops/new                    新建 Loop (独立页面)
/qe/loops/<id>                   Loop 详情 + history
/qe/loops/<id>/iterations/new    新 iteration (独立页面)
/qe/factor-library               因子库 (含 IC ranking + 类型分类)
```

### 5.3 New Experiment 页面结构 (响应式)

```
┌────────────────────────────────────────────────────────────────┐
│ /qe/experiments/new                                            │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│ Step 1: 投资思路 (4 个 preset)                                 │
│ [● 板块轮动 + 小盘溢价 ] [○ Quality + Momentum ]                │
│ [○ Value 主导] [○ 自由配置]                                    │
│                                                                 │
│ Step 2: 因子选择                                                │
│ [展开 §2.3 描述的分类选择框 + 相关性高亮]                       │
│                                                                 │
│ Step 3: 模型选择                                                │
│ ┌──────────────────────────────────────┐                       │
│ │ 模型类型: [LightGBM ▼]              │                       │
│ │ 预设: [● Balanced  ○ Conservative   │                       │
│ │       ○ Aggressive]                  │                       │
│ │ [展开高级参数 ▼]                     │                       │
│ │   learning_rate: 0.02                │                       │
│ │   max_depth: 5                       │                       │
│ │   ... (default 填好, 可调)           │                       │
│ │ 随机 seed: 42                        │                       │
│ │ 多 seed 训练: ☐                     │                       │
│ └──────────────────────────────────────┘                       │
│                                                                 │
│ Step 4: 数据 split + 时间                                       │
│ Train: [2019-01-01 ─ 2022-12-31]                               │
│ Val:   [2023-01-01 ─ 2023-12-31]                               │
│ Test:  [2024-01-01 ─ 2024-12-31]                               │
│ Walk-forward: ☑ (推荐)                                         │
│                                                                 │
│ Step 5: Portfolio rules                                         │
│ 持仓数: [50]                                                    │
│ 行业上限: [None (不中性化)]                                    │
│ 单股上限: [3%]                                                  │
│ 流动性过滤: [ADV ≥ 500 万]                                     │
│ Universe: [HS300 + CSI500 + 小盘 PIT]                          │
│                                                                 │
│ Step 6: 启动配置                                                │
│ 计算资源: [GPU 节点] [CPU 节点]                                 │
│ 通知: [email] [webhook]                                         │
│                                                                 │
│ ─────────────────────────────────────────                      │
│ [保存草稿]              [启动实验 ▶]                            │
└────────────────────────────────────────────────────────────────┘
```

### 5.4 New Iteration (Loop 模式) 增强

基于上一 Loop 的 diff 配置:
```
┌────────────────────────────────────────────────────────────────┐
│ /qe/loops/<id>/iterations/new                                  │
├────────────────────────────────────────────────────────────────┤
│ Loop 8 基础 (vs Loop 7 配置)                                    │
│                                                                 │
│ 因子组合 (从 Loop 7 开始, 加 / 减 / 换):                        │
│ [+ 加因子] [- 减因子] [↻ 重置为 Loop 7]                         │
│                                                                 │
│ 模型参数 (修改):                                                │
│ learning_rate: 0.02 → [0.03]                                    │
│ max_depth: 5 → [6]                                              │
│                                                                 │
│ 数据范围: (保持 Loop 7) [可改]                                  │
│                                                                 │
│ 智能提示:                                                       │
│ 💡 Loop 7 IC 0.037, Sharpe 1.24                                 │
│ 💡 推荐方向: 加 1 个 Sector Momentum + 调高 lr                  │
│ 💡 警告: 当前配置 vs Loop 7 改动 3 处, 建议每次改 ≤ 2 处         │
│                                                                 │
│ [启动 Loop 8 ▶]                                                │
└────────────────────────────────────────────────────────────────┘
```

---

## §6 防过拟合 + 防未来信息 (强化)

### 6.1 未来信息检查 (强制 enforce)

| 数据类型 | 检查 |
|---|---|
| 财务因子 | 用 `ann_date` (announcement) + T+1, 不用 `report_period` |
| 价格因子 | t 时刻只能用 t-1 及之前 close |
| 因子计算 | rolling window 必须严格 trailing, 禁用 centered |
| 标签 (target) | 必须 future return (t+1 到 t+k), 严格 forward |
| Cross-sectional rank | 同一 t 时刻 cross-section, 不混 multiple t |

### 6.2 过拟合检测 (UI 实时显示)

```
训练过程中显示:
┌──────────────────────────────────────────────────────────┐
│  Epoch  Train IC   Val IC    OOS IC    Status            │
├──────────────────────────────────────────────────────────┤
│   1     0.025      0.020     0.018     OK                │
│   5     0.055      0.045     0.040     OK                │
│   10    0.078      0.058     0.050     OK                │
│   15    0.095      0.060     0.048     ⚠ Train ↑, Val ⇈   │
│   20    0.112      0.058     0.045     ❌ 过拟合, 已 stop │
└──────────────────────────────────────────────────────────┘
```

### 6.3 数据 split (严格 walk-forward)

```
Train: 2019-01 ~ 2022-12 (48 month)
  ↓
Val:   2023-01 ~ 2023-06 (6 month, 用于 early stopping + hyperparameter)
  ↓
OOS:   2023-07 ~ 2024-12 (18 month, 仅最终 evaluation, 不调参)
```

**关键**: OOS 不能用于调参, 调过就废, 必须新 OOS。

---

## §7 实验流程 (起步路径) — 调整版

### 7.1 起步路径 (针对 1000 万 + 板块轮动 + 小盘)

```
阶段 1: Baseline (1 周)
  配置: 单因子 (industry_6m_return) + LightGBM default
  目标: 跑通流程 + 验证 IC > 0.02
  预期: annual 10-13%, Sharpe 0.7-0.9, DD 18-22%

阶段 2: 板块 + 小盘组合 (1 周)
  配置: 5 因子 (板块 2 + 小盘 1 + 动量 1 + 反转 1) + LightGBM Balanced
  目标: 验证组合可行
  预期: annual 16-20%, Sharpe 1.0-1.3, DD 14-18%

阶段 3: 完整 13 因子组合 (2 周)
  配置: §2.2 推荐 13 因子 + LightGBM Balanced
  目标: production-ready baseline
  预期: annual 20-25%, Sharpe 1.3-1.6, DD 12-16%

阶段 4: Loop 优化 (持续, 用户主导)
  Loop 1-5: 因子精调 (替换低 IC 因子)
  Loop 6-10: 模型调参 (LightGBM hyperparameter)
  Loop 11-15: 尝试 LSTM 对比 (高因子数场景)
  Loop 16+: turnover / regime 鲁棒性

阶段 5: paper-v2 验证 (1 周)
  配置完全等价跑 paper-v2
  验证 paper-v2 vs backtest 收益一致性 (差异 < 10%)

阶段 6: 实盘 (远期, R-Q9 决策后)
  小资金起步 (100 万), 监控 1-3 月
  → 扩大到 1000 万
```

---

## §8 与整改方案的集成 (Task A-D 调整)

| 整改 Task | 本设计增量 |
|---|---|
| **Task A (双模式)** | LENIENT mode 默认允许 1 seed / 任意因子数 / 任意模型 (paper-v2 验证够用) |
| **Task B (直通 pipeline)** | "Send to paper-v2" 保留**完整 QE 配置** (含 factor list + model type + params + portfolio rules) |
| **Task C (等价性)** | paper-v2 score path 与 QE 回测一致, 含 LSTM/LightGBM 模型 inference 一致 |
| **Task D (走通)** | pkg_b2fac 走通 + 加 1 个新建 13 因子 QE 实验走通 |

### 新增 (整改方案后的下一步)

**Task E**: UI 因子选择 wizard (§2.3) — 分类 + 相关性高亮
**Task F**: UI 模型预设 wizard (§3.3) — LightGBM / LSTM 预设
**Task G**: Loop History 页面 (§4.2) — 时序图 + diff view + 推荐方向
**Task H**: QE 独立页面 (§5.2) — 替代弹窗

---

## §9 立即可做的具体改进 (Codex 接管设计)

### 9.1 UI 改进 (Task E + F + G + H)

按本文档 §2.3 + §3.3 + §4.2 + §5.2 实施:
1. 因子选择 wizard (分类 + 相关性高亮)
2. 模型 wizard (3 预设 / LightGBM + LSTM)
3. Loop History 页面 (时序图 + diff view + 推荐)
4. QE 独立页面 (替代弹窗)

工作量: 2-3 周 (前端 + 部分 backend)

### 9.2 Backend 改进

1. **factor metadata 表扩展**: type + IC_history + ICIR + decay + correlation_matrix
2. **model preset 配置**: 3 个 preset × 2 types (LightGBM + LSTM)
3. **lookahead bias 检查**: validation script, 任何 backfill 拒绝
4. **过拟合 detection**: 实时 IS/OOS gap, > 1.3 警告
5. **loop history aggregation API**: 返回 N loop diff + 推荐方向

工作量: 1-2 周 (Codex 主导)

### 9.3 数据 / 配置改进

1. **PIT universe + IPO filter** (你已有 365 天 ipo_filter)
2. **ADV 流动性过滤**: ≥ 500 万 (小盘策略关键)
3. **ST 股票 PIT 过滤** (已有 ST PIT universe)
4. **行业代码完整** (申万一级 / 二级)

---

## §10 总结 (基于 1000 万小资金 + 板块轮动 + 小盘)

### 核心建议 (更新版)

1. **架构层**: 整改方案 Task A-D + UI/UX 改进 (Task E-H)
2. **因子层**: 13 因子组合, 跨 8 类型, **不做中性化**, 追板块 + 小盘 alpha
3. **模型层**: **不限 LightGBM**, LSTM 在大因子数场景应用; 3 个预设让新手起步
4. **流程层**: Baseline → 5 因子 → 13 因子 → loop 优化 → paper-v2 → 实盘
5. **持续演进**: Loop history 页面 + diff view + 推荐方向 (手动, 不 LLM)
6. **防呆**: PIT + IPO + ADV + Time-series CV + Early stopping + IS/OOS 监控 + lookahead 检查
7. **UI 引导**: 因子分类选择 + 相关性高亮 + 模型 3 预设 + Loop diff view

### 预期效果 (基于本建议 + 1000 万 + 板块/小盘)

- **Baseline (5 因子)**: annual 16-20%, Sharpe 1.0-1.3, DD 14-18%
- **完整 (13 因子)**: annual 20-25%, Sharpe 1.3-1.6, DD 12-16%
- **优化后 (Loop 10+)**: annual 25-32%, Sharpe 1.5-2.0, DD 10-15%
- **A 股 production-grade strategy 水平**
- **可解释 + 持续可演进**

### 与整改方案合并的总 roadmap

```
当前 → 1 周
  Task 18 P0 hotfix cherry-pick to main
  整改 Task A/B/C (双模式 + 直通 + 等价性) Codex 设计

1-2 周
  整改 Task A/B/C 实施 + 流水线
  Task D 走通 pkg_b2fac (端到端验收)
  
2-4 周
  UI Wizard 改进 (Task E-H): 因子分类 + 模型预设 + Loop history + 独立页面
  Backend 改进 (factor metadata / overfitting detection)
  
4-6 周
  跑完整 13 因子 baseline (3-4 周 backtest history)
  Loop 优化 (5-10 个 iteration)
  
6-8 周
  paper-v2 验证 (与 backtest 一致性)
  小资金实盘 (100 万) 监控
  
8-12 周
  1000 万实盘
```

**总计 ~2-3 月** 从当前到 1000 万实盘 (单 Alpha 起步, 不含多 Alpha / LLM 演进)

### 后续 (远期, 不在本文档范围)

- 多 Alpha 架构 (横向扩展 N 个独立 Alpha)
- LLM 辅助自动演进 (因子 / 模型 / 参数自动 search)
- 实盘 (R-Q9.1 决策推翻后, miniqmt_live)
