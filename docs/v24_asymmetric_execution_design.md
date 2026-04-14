# v24 非对称日内执行策略设计

> 日期: 2026-04-06 (v2, 含归一化缺口 + 停牌处理 + PA 量化估算)
> 状态: 设计阶段
> 基线: v20 Hybrid Executor (PA=+8.04 bps)
> 目标: PA=+14~17 bps

---

## 一、设计动机

### 1.1 v23 失败的教训

v23 尝试在 v19 计划上叠加残差修正网络, PA=+1.97 bps 远低于 v20 baseline +5.10 bps。

**根因**: DP `max_steps=60` 导致标签空间与 v19 plan 时间范围 (210分钟) 严重错配:
- 85.5% 的标签打满 clamp -3, 96.9% 的 |label| > 1
- 网络学到 "全面压缩执行量" 的常数偏置

**v23 数据质量 Bug**:
- 28只零成交股 vwap=0 → feat[4]≈10^9 (0.07% 样本)
- 需要 vwap guard + 零成交日跳过

### 1.2 全量 Oracle 统计的关键发现

基于 261.6 万 stock-days (5033股 × 533天, 2024-01 ~ 2026-03):

**发现 1: 买卖 Oracle 时间分布不对称**

| 时段 | 买入 Oracle (最低价) | 卖出 Oracle (最高价) |
|------|---------------------|---------------------|
| 开盘 30m | 50.0% | 52.3% |
| 尾盘 30m | **14.3%** | **7.6%** |

**发现 2: 开盘价 vs 尾盘价的方向性偏差**

| 操作 | 开盘价 vs VWAP | 尾盘价 vs VWAP |
|------|---------------|---------------|
| 买入 | -14.8 bps (贵) | -4.3 bps (贵) |
| 卖出 | +14.8 bps (好) | +4.3 bps (好) |

**发现 3: 10% 板与 20% 板在同一绝对缺口下规律完全反转**

同样 "低开 7%":
- **10% 板**: 已走 70% 跌停距离 → 反弹概率高, 开盘买 **省 123 bps**
- **20% 板**: 只走 35% 跌停距离 → 趋势延续, 开盘买 **亏 91 bps**

同样 "高开 8%":
- **10% 板**: 已走 80% 涨停距离 → 冲板或回落, 开盘买 **亏 122 bps**
- **20% 板**: 只走 40% 涨停距离 → 继续上涨, 开盘买 **省 91 bps**

**→ 必须用归一化缺口 `gap_ratio = gap_pct / limit_pct`**

**发现 4: 归一化后规律统一**

| 归一化缺口 (占限幅%) | 10%板 买入@开盘 | 20%板 买入@开盘 | 统一含义 |
|---------------------|----------------|----------------|---------|
| 低开 > 70% 限 | -123 bps | -214 bps | **都是反转, 加量买** |
| 低开 50~70% | -1 bps | +2 bps | **中性, 不加量** |
| 低开 30~50% | +1 bps | +140 bps | **陷阱区间** |
| 高开 > 70% 限 | +186 bps | +323 bps | **都不该买** |

**发现 5: 高波动日的巨大机会**

| 振幅 | 买入 Oracle Gap | 卖出 Oracle Gap | 占比 |
|------|----------------|----------------|------|
| < 2% | 76 bps | 78 bps | 17.6% |
| > 10% | **720 bps** | **586 bps** | 4.4% |

### 1.3 PA 量化估算

基于 178 万 train stock-days 的模拟:

| 策略 | PA (bps) | 说明 |
|------|---------|------|
| TWAP | 0.0 | 基准 |
| 纯开盘执行 | +14.1 | 卖出好但买入差 |
| 纯尾盘执行 | +5.4 | 两端都接近 VWAP |
| **方向不对称** (卖80%早盘, 买80%TWAP) | **+7.1** | 仅靠方向差异 |
| 方向不对称 + 缺口条件 | **+8.0** | 叠加缺口规则 +0.9 |
| v19/v20 (实测) | **+8.0** | 模型学到的分钟级分配 |
| 3选1完美择时 | +114.3 | 粗粒度理论上界 |
| Oracle (分钟级) | +202.4 | 理论上界 |

**关键洞察**: 纯规则的方向不对称 (+7.1) 与 v19 模型 (+8.0) 几乎相当, 但两者信息来源不同:
- 规则: 利用全天方向/缺口信息 (v19 不知道方向和缺口)
- v19: 利用开盘30分钟特征的分钟级模式 (规则做不到)
- **两者应该是近似可叠加的** → v24 目标: 8 + 7 × 衰减系数 ≈ +14~17

---

## 二、v24 架构: 五层执行框架

```
┌─────────────────────────────────────────────────────────────────┐
│                      v24 Executor                                │
├───────────┬─────────────────────────────────────────────────────┤
│ Layer 0   │ 硬规则 (最高优先级)                                   │
│           │ R1-R4: 涨跌停全量执行/停止                            │
│           │ R5: 最后30分钟强制完成                                │
├───────────┼─────────────────────────────────────────────────────┤
│ Layer 1   │ 条件追价规则                                         │
│           │ R6: 冲涨停追买, R7: 砸跌停追卖, R5a/b: 接近涨跌停加速  │
├───────────┼─────────────────────────────────────────────────────┤
│ Layer 1.5 │ WARMUP 缺口感知快速分配 (v24 新增, t=0 决策)          │
│  (v24)    │ 开盘第一分钟根据 gap_ratio + 方向 决定前30m分配比例    │
│           │ 不需要模型, 查表或简单规则                             │
├───────────┼─────────────────────────────────────────────────────┤
│ Layer 2   │ 缺口感知执行计划 (v24 新增, t=30 生成)                │
│  (v24)    │ 模型根据 gap_ratio + 方向 + 30m特征 生成剩余分钟分布   │
├───────────┼─────────────────────────────────────────────────────┤
│ Layer 3   │ 分钟级修正 (v23 重设计)                               │
│  (v24)    │ DP max_steps=240 + 条件执行比例差标签                 │
├───────────┼─────────────────────────────────────────────────────┤
│ Fallback  │ TWAP 兜底                                           │
└───────────┴─────────────────────────────────────────────────────┘
```

### 2.1 Layer 0: 硬规则 (继承 v20)

| 规则 | 条件 | 动作 | 追价 |
|------|------|------|------|
| R1 | 买入 + 跌停 | 全量执行 | 30 bps |
| R2 | 卖出 + 涨停 | 全量执行 | 30 bps |
| R3 | 买入 + 涨停 | 停止 | — |
| R4 | 卖出 + 跌停 | 停止 | — |
| R5 | 剩余 ≤ 30 分钟 | 1/remaining | 5 bps |

### 2.2 Layer 1: 条件追价规则 (继承 v20, 参数可训练)

| 规则 | 条件 | 动作 |
|------|------|------|
| R5a | 卖出 + 距涨停 < 2% | 加速到 50%+ |
| R5b | 买入 + 距跌停 < 2% | 加速到 50%+ |
| R6 | 买入 + 距涨停 < chase_dist + 放量 + 快速拉升 | 涨停价全量追买 |
| R7 | 卖出 + 距跌停 < chase_dist + 放量 + 快速下跌 | 跌停价全量追卖 |

R6/R7 参数按 10%/20% 板分别设置, 通过 grid search 在 train 集优化。

### 2.3 Layer 1.5: WARMUP 缺口感知快速分配 (v24 新增)

#### 设计理念

v20 在前 30 分钟固定分配 20% 仓位 (TWAP)。但数据显示:
- 10% 板低开 > 70% 限时, 68.5% 的买入 Oracle 在前 30m → 只分配 20% 浪费低价
- 20% 板高开 3~5% 时, 57.8% 的卖出 Oracle 在前 30m → 只分配 20% 浪费高价

**解决**: 开盘第一分钟就能算出 `gap_ratio`, 无需等待 30 分钟。

#### 核心公式

```python
gap_ratio = (open_price - prev_close) / (prev_close * limit_pct)  # [-1, +1]
```

#### 分配规则

基于 train 集归一化缺口验证数据:

**买入 WARMUP 分配**:

| gap_ratio 区间 | 10%板数据 | 20%板数据 | warmup_alloc | 理由 |
|---------------|----------|----------|-------------|------|
| < -0.70 (极端低开) | 开盘买-123bps, Oracle前30m 89% | 开盘买-214bps, Oracle前30m 77% | **50~60%** | 两板块均强烈反转 |
| -0.70 ~ -0.50 | -1bps, 67% | +2bps, 46% | **20%** (不变) | 中性/陷阱区 |
| -0.50 ~ -0.30 | +1bps, 55% | +140bps, 36% | **15%** (减少) | 陷阱区, 继续杀跌 |
| -0.30 ~ +0.30 | -13~-24bps, 48~50% | -14~-26bps, 46~47% | **20%** (默认) | 基线 |
| +0.30 ~ +0.50 | -23bps, 63% | -40bps, 54% | **20%** | 买入不急 |
| > +0.70 (极端高开) | +186bps, 54% | +323bps, 29% | **10%** (减少) | 高开追买亏损巨大 |

**卖出 WARMUP 分配**:

| gap_ratio 区间 | warmup_alloc | 理由 |
|---------------|-------------|------|
| > +0.30 且 < +0.50 | **40~50%** | 10%板+32bps, 20%板+42~90bps |
| > +0.50 且 < +0.70 | **30%** | 10%板数据反常, 20%板仍好 |
| > +0.70 (极端高开) | **20%** (不变) | 10%板-186bps(冲板), 20%板-323bps(继续涨) |
| < -0.30 (低开卖出) | **20%** (默认) | 无特殊优势 |

**注意**: 以上阈值是初始值, 将通过消融实验微调。最终可用模型替代查表。

#### 实现

```python
def compute_warmup_allocation(gap_ratio: float, is_buy: bool,
                              limit_pct: float) -> float:
    """开盘第一分钟计算前30分钟应分配的仓位比例。"""
    DEFAULT = 0.20

    if is_buy:
        if gap_ratio < -0.70:
            return 0.55      # 极端低开 → 加量买入
        elif gap_ratio < -0.50:
            return DEFAULT    # 中性
        elif gap_ratio < -0.30:
            return 0.15       # 陷阱区 → 减量
        elif gap_ratio > 0.70:
            return 0.10       # 极端高开 → 买入减量
        else:
            return DEFAULT
    else:  # sell
        if 0.30 < gap_ratio < 0.50:
            return 0.45       # 中度高开 → 加量卖出
        elif 0.50 <= gap_ratio < 0.70:
            return 0.30       # 较大高开 → 适度加量
        elif gap_ratio >= 0.70:
            return DEFAULT    # 极端高开 → 不追卖 (继续冲)
        else:
            return DEFAULT
```

### 2.4 Layer 2: 缺口感知执行计划 (v24 核心创新)

#### 归一化缺口特征

所有缺口相关特征必须使用归一化值:

| 特征 | 计算方式 | 范围 | 说明 |
|------|---------|------|------|
| `gap_ratio` | gap_pct / limit_pct | [-1, +1] | 缺口占涨跌停幅度比例 |
| `gap_ratio_signed` | gap_ratio × is_buy_sign | [-1, +1] | 方向交互 |
| `gap_bucket_idx` | 归一化缺口桶索引 | [0, 7] | embedding 输入 |
| `limit_pct` | 0.10 或 0.20 | {0.10, 0.20} | 板块标识 |

#### 缺口桶定义 (归一化)

```python
GAP_RATIO_BINS = [-0.70, -0.50, -0.30, -0.10, +0.10, +0.30, +0.50, +0.70]
# 桶 0: < -0.70 (极端低开)
# 桶 1: [-0.70, -0.50)
# 桶 2: [-0.50, -0.30)
# 桶 3: [-0.30, -0.10)
# 桶 4: [-0.10, +0.10) (平开)
# 桶 5: [+0.10, +0.30)
# 桶 6: [+0.30, +0.50)
# 桶 7: [+0.50, +0.70)
# 桶 8: >= +0.70 (极端高开)
```

共 9 个桶, 边界按归一化缺口定义, 10%板和20%板自动对齐。

#### 网络架构

```python
class ExecutionPlanNetV24(nn.Module):
    """v24: 归一化缺口感知 + 买卖不对称执行计划网络。"""

    def __init__(self, minute_dim=5, day_dim=10, gap_buckets=9, gap_emb_dim=8,
                 plan_len=210, cnn_channels=64, hidden_dim=256):
        super().__init__()
        self.plan_len = plan_len

        # 1D-CNN (与 v19 相同)
        self.cnn = nn.Sequential(
            nn.Conv1d(minute_dim, cnn_channels, kernel_size=5, padding=2),
            nn.BatchNorm1d(cnn_channels),
            nn.ReLU(),
            nn.MaxPool1d(2),
            nn.Conv1d(cnn_channels, cnn_channels, kernel_size=3, padding=1),
            nn.BatchNorm1d(cnn_channels),
            nn.ReLU(),
            nn.MaxPool1d(2),
            nn.Conv1d(cnn_channels, cnn_channels, kernel_size=3, padding=1),
            nn.BatchNorm1d(cnn_channels),
            nn.ReLU(),
            nn.AdaptiveAvgPool1d(1),
        )

        # v24: 归一化缺口桶 Embedding
        self.gap_embedding = nn.Embedding(gap_buckets, gap_emb_dim)

        # MLP: CNN(64) + day_feat(10) + gap_emb(8)
        #      + gap_ratio(1) + gap_ratio_signed(1) + limit_pct(1) + is_buy(1) = 86
        fusion_dim = cnn_channels + day_dim + gap_emb_dim + 4
        self.mlp = nn.Sequential(
            nn.Linear(fusion_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_dim, plan_len),
        )

    def forward(self, minute_feats, day_feats, gap_bucket_idx,
                gap_ratio, gap_ratio_signed, limit_pct, is_buy):
        x = minute_feats.transpose(1, 2)
        cnn_out = self.cnn(x).squeeze(-1)              # [B, 64]
        gap_emb = self.gap_embedding(gap_bucket_idx)    # [B, 8]
        extra = torch.stack([gap_ratio, gap_ratio_signed,
                             limit_pct, is_buy], dim=1) # [B, 4]
        fused = torch.cat([cnn_out, day_feats, gap_emb, extra], dim=1)
        logits = self.mlp(fused)
        return F.softmax(logits, dim=-1)
```

#### 标签生成

与 v19 相同的"执行吸引力" softmax 分布, 但:
- 每个 stock-day 生成**买入和卖出两条**样本
- 吸引力计算基于当日实际 low/high (事后最优, 无跨日信息)
- **Layer 2 标签只分配 `1 - warmup_alloc` 的仓位** (warmup 部分已由 Layer 1.5 处理)

### 2.5 Layer 3: 分钟级修正层 (v23 标签重设计)

#### 标签修复: 方案 A + B 组合

**Step 1: DP max_steps=240** (根治时间窗口错配)
- 计算量 4x: O(240 × 21 × 6) ≈ 120K/order
- 200K orders × 12 workers ≈ 3-4h

**Step 2: 条件执行比例差标签**
```
label = clip(dp_cond_frac - plan_cond_frac, -0.3, +0.3)
  dp_cond_frac   = dp_frac[t] / sum(dp_frac[t:])
  plan_cond_frac = plan[t] / sum(plan[t:])
```
- 天然有界 [-0.3, +0.3], 无 log 零值爆炸
- 加性修正, 推理: `final = clip(plan_cond + correction, 0.01, 1.0)`

#### 修正网络特征 (31维 → 36维)

v23 的 31 维基础上增加 5 维:

| 索引 | 特征 | 说明 |
|------|------|------|
| 31 | `gap_ratio` | 归一化缺口 (核心新增) |
| 32 | `gap_ratio_signed` | gap_ratio × is_buy_sign |
| 33 | `limit_pct` | 涨跌停幅度 (0.10 或 0.20) |
| 34 | `plan_exec_ahead` | 计划累计 vs 实际累计的偏离 |
| 35 | `price_vs_plan_vwap` | 当前价 vs 计划执行均价 |

#### 修正网络架构 (~15K 参数)

```python
class CorrectionNetV24(nn.Module):
    def __init__(self, feat_dim=36, hidden=64):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(feat_dim, hidden),
            nn.LayerNorm(hidden),
            nn.ReLU(),
            nn.Linear(hidden, hidden // 2),
            nn.ReLU(),
            nn.Linear(hidden // 2, 1),
            nn.Tanh(),  # [-1, 1] → × 0.3 → [-0.3, +0.3]
        )

    def forward(self, x):
        return self.net(x) * 0.3
```

---

## 三、数据质量与异常处理

### 3.1 停牌与缺失数据

训练集中的异常统计:

| 异常类型 | 数量 | 占比 | 处理方式 |
|---------|------|------|---------|
| 分钟线不足 240 根 | 0 | 0% | gen_pickle_data.py 已过滤 |
| 日振幅=0 (疑似停牌) | 3,413 | 0.19% | **跳过** |
| 前后30m均零成交 | 3 | 0.00% | **跳过** |
| 缺口 > 20% | 278 | 0.02% | **clip 到 ±20%** |
| vwap=0 (零成交) | ~7,350 samples | 0.07% | **vwap = cur_price** |

#### 处理流程

```python
def should_skip_stock_day(day_df):
    """判断是否跳过该 stock-day。"""
    close = day_df["$close0"].values
    vol = day_df["$volume0"].values
    high = day_df["$high0"].values
    low = day_df["$low0"].values

    # 1. 停牌: 振幅为零
    if (high.max() - low.min()) < 1e-6:
        return True, "suspended"

    # 2. 零成交
    if vol.sum() < 1e-6:
        return True, "zero_volume"

    # 3. 分钟线不足
    if len(close) < 240:
        return True, "insufficient_bars"

    # 4. 价格异常 (收盘价<=0)
    if close[-1] <= 0 or close[0] <= 0:
        return True, "invalid_price"

    return False, "ok"
```

#### 缺口 clip

```python
GAP_PCT_CLIP = 20.0  # 超过 ±20% 的缺口 clip (极端情况: 复牌/ST)
gap_pct = np.clip(gap_pct_raw, -GAP_PCT_CLIP, GAP_PCT_CLIP)
gap_ratio = gap_pct / (limit_pct * 100)  # limit_pct = 0.10 or 0.20
```

#### 特殊股票处理

| 场景 | 处理 |
|------|------|
| ST 股票 (5% 涨跌停) | `limit_pct = 0.05`, gap_ratio 相应调整 |
| 北交所 (30% 涨跌停) | 当前 gen_pickle_data.py 已排除 .BJ |
| 复牌首日 (无涨跌停) | 用 20% 作为 limit_pct 近似 |
| 新股上市首日 | 无涨跌停, 训练数据中排除 |

#### 实时执行时的容错

```python
def get_limit_pct(stock_id: str, is_st: bool = False) -> float:
    """获取涨跌停幅度。"""
    if is_st:
        return 0.05
    code = stock_id.split(".")[0]
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    return 0.10

def safe_gap_ratio(open_price, prev_close, limit_pct):
    """安全计算归一化缺口, 处理异常值。"""
    if prev_close < 1e-4 or limit_pct < 1e-4:
        return 0.0  # 异常数据, 视为平开
    gap_pct = (open_price - prev_close) / prev_close
    gap_pct = np.clip(gap_pct, -0.20, 0.20)
    return gap_pct / limit_pct
```

---

## 四、执行流程详解

### 4.1 开盘前 (t < 0)

加载模型:
- v24 Plan 模型: `v24_plan_net.pt`
- v24 Correction 模型: `v24_correction_net.pt`
- 确认 `prev_close` 和 `limit_pct` 可用

### 4.2 开盘 (t = 0)

```
1. open_price = close_arr[0]
2. gap_ratio = safe_gap_ratio(open_price, prev_close, limit_pct)
3. warmup_alloc = compute_warmup_allocation(gap_ratio, is_buy, limit_pct)
4. warmup_per_minute = warmup_alloc / 30  (TWAP 均匀分配到前30分钟)
```

### 4.3 WARMUP 期 (t = 0~29)

每分钟:
1. 检查 Layer 0 硬规则 (涨跌停)
2. 检查 Layer 1 追价规则 (R6/R7)
3. 如果上述都不触发: 按 `warmup_per_minute` 执行

### 4.4 Plan 生成 (t = 30)

1. 提取 30 分钟特征 `[30, 5]` + 日级特征 `[10]`
2. 计算 `gap_bucket_idx`, `gap_ratio`, `gap_ratio_signed`, `limit_pct`, `is_buy`
3. 模型推理 → 210 维 softmax 分布 `plan[210]`
4. **缩放**: `plan *= (1.0 - actual_warmup_executed)` (扣除 warmup 已执行量)

### 4.5 逐分钟执行 (t = 30~239)

```
if Layer 0 硬规则命中:
    执行硬规则
elif Layer 1 条件规则命中:
    执行追价规则
else:
    plan_cond_frac = plan[t-30] / sum(plan[t-30:])

    if correction_model is not None:
        feat = compute_features_36d(t, ...)
        correction = correction_model(feat)  # [-0.3, +0.3]
        final_cond_frac = clip(plan_cond_frac + correction, 0.01, 1.0)
    else:
        final_cond_frac = plan_cond_frac

    exec_amount = remaining × final_cond_frac
```

### 4.6 决策流程图

```
每分钟 t:
│
├─ 涨停 & 卖出? ──→ 全量卖出 (R2)
├─ 跌停 & 买入? ──→ 全量买入 (R1)
├─ 涨停 & 买入? ──→ 停止 (R3)
├─ 跌停 & 卖出? ──→ 停止 (R4)
│
├─ 剩余≤30m? ────→ 强制分配 (R5)
│
├─ 买入+冲涨停? ─→ 涨停价全量追买 (R6)
├─ 卖出+砸跌停? ─→ 跌停价全量追卖 (R7)
│
├─ 接近涨停+卖出? → 加速卖出 (R5a)
├─ 接近跌停+买入? → 加速买入 (R5b)
│
├─ t < 30? ──────→ Layer 1.5 WARMUP 缺口分配
│
├─ t >= 30? ─────→ Layer 2 Plan + Layer 3 Correction
│
└─ 否则 ─────────→ TWAP 兜底
```

---

## 五、训练 Pipeline

### 5.1 数据生成

| 步骤 | 脚本 | 输入 | 输出 | 耗时 |
|------|------|------|------|------|
| 1 | `v24_gen_plan_data.py` | pkl 全量 | plan_data.pkl (523万条) | ~30m |
| 2 | `v24_gen_dp_labels.py` (max_steps=240) | pkl + orders | dp_labels_240.pkl | ~4h |
| 3 | `v24_train_plan.py` | plan_data.pkl | v24_plan_net.pt | ~20m GPU |
| 4 | `v24_gen_correction_data.py` | plan_net + dp_labels | correction_data.npz | ~3h |
| 5 | `v24_train_correction.py` | correction_data.npz | v24_correction_net.pt | ~10m GPU |

### 5.2 训练配置

**Layer 2 (Plan Net)**:
- Loss: KL divergence
- Optimizer: AdamW, lr=1e-3 → 1e-5 cosine
- Epochs: 30, batch_size=512
- 验证: val_kl, top10_overlap, **买入/卖出分组 val_kl**, **缺口分桶 val_kl**

**Layer 3 (Correction Net)**:
- Loss: MSE
- Optimizer: AdamW, lr=3e-4
- Epochs: 30, batch_size=1024
- 验证: val_mse, **分时段 mse**, **按缺口桶 mse**

### 5.3 未来数据泄露防护

| 层级 | 措施 |
|------|------|
| 数据划分 | train: 2024-01 ~ 2025-06, valid: 2025-07 ~ 2025-09, test: 2025-10 ~ 2026-03 |
| Purge gap | 模型 train_end + 3月 ≤ QE 回测起始日 |
| 标签安全 | DP 标签只用当天数据 (事后最优, 无跨日信息) |
| Oracle 统计 | 模型设计/超参仅参考 train 子集统计 |
| Layer 1.5 阈值 | 仅基于 train 集数据确定 |

---

## 六、评估方案

### 6.1 消融实验 (10 个配置)

| 编号 | 配置 | 说明 |
|------|------|------|
| A0 | TWAP | 基准线 |
| A1 | v19 plan (不分方向) | v19 原版 |
| A2 | v20 (v19 + R1-R5 + R5a) | 当前生产基线 |
| A3 | v20 + R6/R7 | 追价规则增量 |
| **B0** | **TWAP + Layer 1.5 warmup 缺口** | 仅规则, 无模型 |
| **B1** | **v24 plan (方向感知, 无缺口)** | 仅 is_buy |
| **B2** | **v24 plan (方向+归一化缺口)** | Layer 2 完整 |
| **B3** | **B2 + Layer 1.5** | Layer 1.5 + Layer 2 |
| **B4** | **B3 + Layer 3 correction** | + 修正网络 |
| **B5** | **B4 + R6/R7 优化** | 完整 v24 |

### 6.2 评估指标

| 指标 | 说明 |
|------|------|
| PA (bps) | vs VWAP, **主指标** |
| PA_buy / PA_sell | 买入/卖出分别 (**v24 重点**) |
| PA by gap_ratio_bucket | 按归一化缺口分桶 |
| PA by limit_pct | 10% 板 vs 20% 板 |
| PA by volatility | 按波动率分桶 |
| Oracle Gap (bps) | vs Oracle 最优的差距 |
| FFR (Fill Rate) | 成交率 |
| 规则触发率 | R1-R7 + Layer 1.5 各规则触发频次 |

### 6.3 预期结果 (基于量化模拟)

| 配置 | 预期 PA (bps) | 增量来源 |
|------|-------------|---------|
| A2 (v20 baseline) | +8.0 | — |
| B0 (TWAP + warmup 缺口) | +1.0~2.0 | 仅缺口规则, 无模型 |
| B1 (方向感知 plan) | +10.0~12.0 | 方向不对称: +2~4 bps |
| B2 (方向+缺口 plan) | +11.0~13.0 | 缺口 embedding: +1~2 bps |
| B3 (+ Layer 1.5) | +12.0~14.0 | warmup 缺口分配: +1 bps |
| B4 (+ correction) | +13.0~15.0 | DP 240步修正: +1~2 bps |
| **B5 (完整 v24)** | **+14.0~17.0** | R6/R7 优化: +0.5~1 bps |

**估算依据**:
- 方向不对称的纯规则 PA = +7.1 bps (模拟)
- v19 模型 PA = +8.0 bps (实测)
- 两者信息正交: v19 不知道方向/缺口, 规则不做分钟级分配
- 叠加时有衰减 (特征交叉不完全独立), 按 60~80% 效率计
- v24 = v19 基础 (+8) + 方向/缺口增量 (+7.1 × 0.6~0.8) + 修正 (+1~2) ≈ +13~17

**风险预算**:

| 风险 | 概率 | 影响 | 退出策略 |
|------|------|------|---------|
| Layer 2 无增量 (B2 ≈ A1) | 20% | -4 bps | 缺口被 v19 特征隐式捕捉, 仅保留 Layer 1.5 |
| Layer 3 劣化 (B4 < B3) | 30% | -1 bps | 标签仍有问题, 仅用 Layer 2 |
| warmup 过度分配 | 15% | -2 bps | 缩小 warmup_alloc 范围 |
| 10%/20% 拟合不平衡 | 10% | -1 bps | 分板块训练或加权 |

**保底 PA**: 即使 Layer 3 无效, B3 仍有 +12~14 bps (v20 的 1.5~1.75 倍)

---

## 七、实现计划

### Phase 0: 准备 (1 天)

- [x] 全量 Oracle 统计 (oracle_full_dist.pkl)
- [x] 缺口条件验证 + 10%/20%板分析 (oracle_full_dist_with_gap.pkl)
- [ ] 数据质量: 停牌/零成交过滤 + vwap guard
- [ ] DP solver: max_steps 参数化 + ST 5%板支持
- [ ] `get_limit_pct()` 工具函数 (统一 10%/20%/5% 判断)

### Phase 1: Layer 1.5 + Layer 2 (2 天)

- [ ] `v24_gen_plan_data.py`: 全量双方向标签 + 归一化缺口
- [ ] `ExecutionPlanNetV24`: 缺口 embedding + gap_ratio + limit_pct
- [ ] `compute_warmup_allocation()`: Layer 1.5 快速分配
- [ ] `v24_train_plan.py`: KL loss, 分方向/缺口桶验证
- [ ] 消融: A1 → B1 → B2 → B3

### Phase 2: Layer 3 (2 天)

- [ ] `v24_gen_dp_labels.py`: max_steps=240
- [ ] `v24_gen_correction_data.py`: 条件比例差标签 + 36维特征
- [ ] `CorrectionNetV24`: 加性修正 [-0.3, +0.3]
- [ ] 消融: B3 → B4

### Phase 3: 集成评估 (1 天)

- [ ] `v24_hybrid_executor.py`: 五层执行器
- [ ] `v24_evaluate.py`: 10 消融 + 分桶分析
- [ ] R6/R7 grid search → B5

### Phase 4: AIstock 集成 (待确认后)

- [ ] 替换 v20 Hybrid Executor
- [ ] 前端 rl-execution 页面
- [ ] QE 回测对比

---

## 八、代码位置

| 文件 | 说明 |
|------|------|
| `rl_execution/network/execution_plan_net_v24.py` | Layer 2 网络 |
| `rl_execution/network/correction_net_v24.py` | Layer 3 网络 |
| `rl_execution/executor/v24_hybrid_executor.py` | 五层执行器 |
| `scripts/rl_execution/v24_gen_plan_data.py` | Layer 2 训练数据 |
| `scripts/rl_execution/v24_gen_dp_labels.py` | DP max_steps=240 标签 |
| `scripts/rl_execution/v24_gen_correction_data.py` | Layer 3 训练数据 |
| `scripts/rl_execution/v24_train_plan.py` | Layer 2 训练 |
| `scripts/rl_execution/v24_train_correction.py` | Layer 3 训练 |
| `scripts/rl_execution/v24_evaluate.py` | 评估脚本 |
| `scripts/rl_execution/oracle_time_distribution.py` | Oracle 全量统计 |
| `scripts/rl_execution/oracle_gap_validate.py` | 缺口验证 |

---

## 九、与 v20/v23 的完整对比

| 维度 | v20 | v23 | v24 |
|------|-----|-----|-----|
| 买卖方向 | 不区分 | 不区分 | **分别建模** |
| 缺口条件 | 无 | 无 | **归一化缺口 embedding** |
| 10%/20%板 | R6/R7参数区分 | 不区分 | **全链路区分 (gap_ratio + limit_pct)** |
| WARMUP 期 | 固定 20% | 固定 20% | **缺口感知 10~60%** |
| Plan 生成 | v19 (无方向/缺口) | = v20 | **v24 Net (方向+缺口+板块)** |
| 修正标签 | 无 | log(dp/plan), max_steps=60 | **cond_diff, max_steps=240** |
| 修正方式 | 无 | 乘性 (exp) | **加性 (clip ±0.3)** |
| DP 覆盖 | 前 60 分钟 | 前 60 分钟 | **全天 240 分钟** |
| 停牌处理 | 无 | 无 | **skip + clip + vwap guard** |
| 预期 PA | +8.04 bps | +1.97 (失败) | **+14~17 bps** |
| vs v20 提升 | — | -6.07 (退化) | **+6~9 bps (+75~112%)** |

---

## 十、分析数据来源

本设计的所有量化依据来自以下分析:

| 数据文件 | 说明 |
|---------|------|
| `/home/lc999/data/rl_orders/oracle_full_dist.pkl` | 261.6万 stock-days 全量 Oracle 统计 |
| `/home/lc999/data/rl_orders/oracle_full_dist_with_gap.pkl` | + prev_close + gap_pct + gap_ratio |
| `scripts/rl_execution/oracle_time_distribution.py` | Oracle 分布生成脚本 |
| `scripts/rl_execution/oracle_gap_validate.py` | 缺口条件验证脚本 (10%/20%板分析) |
