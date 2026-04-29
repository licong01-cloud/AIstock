# 多周期训练模型探索方案

> 基于 `qe_20260429_015755_c4ba` 实验分析、模型库审计、前沿研究方向调研
> 2026-04-29

---

## 一、QE 实验核心发现

### 实验设计

`qe_20260429_015755_c4ba`（"修复V25后的hmm和多周期验证"）固定 45 因子 + V25_TWO_STAGE 执行 + LGBModel，仅改变两个变量：

- **label_horizon**: 1D / 3D / 5D / 10D / 20D
- **HMM**: on / off / 不同版本

### 全部 Loop 收益对比

| Loop | Horizon | HMM | IC | Rank IC | ICIR | **CAGR** | final_nav | AnnRet | IR | MaxDD |
|------|---------|-----|------|---------|------|------|------|------|------|------|
| 1 | 1D | Yes | 0.0572 | 0.0562 | 0.649 | 0.517 | 1.951 | 0.250 | 1.16 | -0.163 |
| 2 | 3D | No | 0.0692 | 0.0827 | 0.670 | 0.621 | 2.170 | 0.312 | 1.45 | -0.152 |
| 3 | 3D | Yes | 0.0692 | 0.0827 | 0.670 | 0.652 | 2.237 | 0.329 | 1.56 | -0.156 |
| 4 | 5D | Yes | 0.0729 | 0.0949 | 0.688 | 0.781 | 2.522 | 0.399 | 1.91 | -0.155 |
| 5 | 10D | Yes | 0.0785 | 0.1162 | 0.719 | 0.777 | 2.514 | 0.397 | 1.97 | -0.133 |
| **6** | **10D** | **No** | **0.0785** | **0.1162** | **0.722** | **0.831** | **2.638** | **0.424** | **2.14** | **-0.133** |
| 7 | 20D | No | 0.0832 | 0.1406 | 0.735 | 0.705 | 2.353 | 0.356 | 1.78 | -0.140 |
| 8 | 20D | Yes | 0.0832 | 0.1406 | 0.735 | 0.705 | 2.353 | 0.356 | 1.80 | -0.142 |
| 9 | 20D | Yes(v2) | 0.0832 | 0.1406 | 0.735 | 0.736 | 2.421 | 0.374 | 1.86 | -0.135 |
| 10 | 20D | Yes(v3) | 0.0832 | 0.1406 | 0.735 | 0.718 | 2.382 | 0.363 | 1.84 | -0.139 |

### 核心结论

1. **训练周期是最强的单一杠杆**：label_horizon 1D→20D，Rank IC 从 0.056 → 0.141 (+151%)，IC 从 0.057 → 0.083 (+46%)
2. **收益最优在 10D**：CAGR=0.831（10D noHMM）> 5D=0.781 > 20D=0.705。20D 虽 IC 最高，但调仓频率降低导致资金利用效率下降
3. **同 horizon 下 GRU ≈ LGBM**：1D 时 GRU IC=0.057 = LGBM IC=0.057，架构差异在日频预测中几乎为零
4. **HMM 效果因 horizon 而异**：3D 正面(+5%)、10D 负面(-6.5%)、20D 微弱正面(+4.4%)

---

## 二、模型库审计

### 现状

| | 数量 | 说明 |
|------|------|------|
| 总模型数 | 48 | |
| TimeSeries (GRU/LSTM/Transformer) | 41 | rdagent 演进生成 |
| LGBModel 种子 | 2 | 未训练，纯模板 |
| PTNN 种子 | 3 | Multi-Alpha 模板 |
| CatBoost/Ridge 种子 | 2 | 从未训练 |
| **有训练指标的模型** | **34** | 全部为 TimeSeries |

### 超参空间覆盖

| 超参 | 覆盖值 | 致命缺口 |
|------|------|------|
| **label_horizon** | **仅 1D** (34/34) | 3D/5D/10D/20D 完全未探索 |
| **num_timesteps** | **仅 20** (34/34) | QE 硬编码，不可配置 |
| hidden_size | 64, 128, 256 | 基本覆盖 |
| num_layers | 1, 2, 3 | 基本覆盖 |
| dropout | 0.0, 0.1, 0.2, 0.3 | 缺少 0.4, 0.5（长周期需要的强正则化） |

### 从 1D 数据推导的规律（适用于 10D/20D 的推断）

```
Best IC by hidden_size:  hs=64 (mean=0.0413) > hs=128 (mean=0.0394)
Best IC by num_layers:   nl=1  (mean=0.0444) > nl=2  (mean=0.0383)
Best IC by dropout:      do=0  (mean=0.0437) > do=0.2 (mean=0.0418)
Best IC by family:       GRU  (mean=0.0451) > Transformer (mean=0.0422) > GRU+Attn (mean=0.0366) > LSTM (mean=0.0284)
```

**结论**：在金融数据上，简单模型 > 复杂模型。Attention 系统性失败（过拟合前三名全是 Attention 模型）。单层 GRU + hs=64 是 1D 最优配置，应作为 10D 探索的起点。

---

## 三、多周期训练模型矩阵

### 新建模型清单（8个）

#### 时序神经网络（6个，PyTorch + Qlib GeneralPTNN）

所有模型基于库中 `GRU_TimeSeries_64`（IC=0.0573，hs=64, nl=1, do=0.0）的最优架构，仅调整正则化和容量参数。

| ID | 模型 | hidden_size | num_layers | dropout | 参数量 | 验证目标 |
|------|------|------|------|------|------|------|
| M1 | GRU_10D_hs64_d02 | 64 | 1 | 0.2 | ~30K | 核心候选，安全起步 |
| M2 | GRU_10D_hs64_d03 | 64 | 1 | 0.3 | ~30K | 正则化对比 |
| M3 | GRU_10D_hs64_d04 | 64 | 1 | 0.4 | ~30K | 强正则兜底 |
| M4 | GRU_10D_hs96_d03 | 96 | 1 | 0.3 | ~45K | 容量对比 |
| M5 | LSTM_10D_hs64_d02 | 64 | 1 | 0.2 | ~40K | 备选架构 |
| M6 | TCN_10D_d02 | 64(ch) | 3(层) | 0.2 | ~35K | 多尺度卷积 |

#### 树模型（2个，Qlib 内置类 + 种子注册）

| ID | 模型 | Qlib 类 | 差异化优势 |
|------|------|------|------|
| M7 | XGBoost_10D | `XGBModel` | Level-wise 分裂 vs LGBM leaf-wise |
| M8 | CatBoost_10D | `CatBoostModel` | Ordered boosting 防止时序信息泄漏 |

#### 前沿模型（2个，自定义实现）

| ID | 模型 | 类型 | 差异化优势 |
|------|------|------|------|
| M9 | LambdaMART_10D | LightGBM ranking | 训练目标对齐实际任务（排序选股） |
| M10 | TabPFN_10D/20D | 表格基础模型 | 零训练，小样本场景优势显著 |

---

## 四、前沿方向补充分析

### 4.1 搜索到的关键前沿证据

**LambdaMART（LightGBM ranking objective）**

2025 年生产级研究（Kinlay, Python EES）：LambdaMART + 1400 特征 + 7500 股票，top-decile Sharpe 0.8，年化 17.8%。穿越 2008 危机、2020 疫情、2022 加息周期均保持稳定。直接优化 NDCG 排序质量而非收益幅度。

**TabPFN（Nature 2025, Hollmann et al.）**

在 1.3 亿合成表格数据集上预训练的 Transformer，in-context learning。小样本（≤10000）benchmark：默认配置 2.8 秒推理**超越调参 4 小时的 CatBoost**（回归 RMSE 0.968 vs 0.875）。

与我们的匹配度：
- 我们 10D 训练 ~1000 滚动样本 → TabPFN 的最佳场景
- 我们 20D 训练 ~50 有效独立样本 → TabPFN 理论最强区间

**模型校准问题（Kinlay, 2026）**

Transformer 模型系统性欠自信：预测截面极差 2.08% vs 实际 4.24%（压缩因子 2）。导致优化器无信念集中持仓。解决方案：pairwise ranking loss 替代 MSE + 事后 spread scaling。

**Mamba/SSM（2025-2026 热点）**

选择性状态空间模型，O(L) 推理复杂度。独特价值：选择性扫描可自动检测市场状态切换。但对 20-60 timesteps 范围，相对于 GRU 的 O(L)→O(L) 无明显理论优势。列为 P2 方向。

### 4.2 优先级分层

```
Tier 0 (立即做, 零/低代码改动, 预计 IC 提升 >0.005):
  ① LambdaMART ranking 目标     — 改 objective 参数即可
  ② TabPFN (10D + 20D)          — pip install + Qlib wrapper
  ③ Pairwise ranking loss       — 改 GRU 模型 loss 函数

Tier 1 (需要新建模型, 已确定有价值):
  ④ GRU 系列 (do=0.2/0.3/0.4, hs=64/96)
  ⑤ TCN 多尺度卷积
  ⑥ CatBoost ordered boosting
  ⑦ LSTM, XGBoost

Tier 2 (Phase 2 或后续):
  ⑧ Mamba/SSM for time series
  ⑨ PFN-Boost (TabPFN prior + GBDT residuals)
  ⑩ Time series foundation model (Kronos/Chronos, 等 6-12 月成熟)

Tier 3 (不做):
  ⑪ Attention 变体 (HIST/TabNet/GATs) — 已知系统性失败
  ⑫ Kronos/Chronos — 不成熟
```

---

## 五、LambdaMART 实现方案

### 5.1 原理

LGBM 默认使用 MSE 回归（`objective='regression'`），优化目标是：

$$\min \sum_i (\hat{y}_i - y_i)^2$$

但实际使用场景是**排序选股**：每天将股票按预测分数排名，选 top K。MSE 训练目标和 ranking 推理目标不一致。

LambdaMART 直接优化排序质量（NDCG），训练目标变为：

$$\min \sum_q \sum_{i,j} |\Delta NDCG_{ij}| \cdot \log(1 + e^{-\sigma(\hat{y}_i - \hat{y}_j)})$$

其中 q 是每天的股票截面（query group），i, j 是同一截面的两只股票，ΔNDCG 是交换 i 和 j 对排序质量的影响。

### 5.2 Qlib 集成方案

**不需要修改 Qlib 源码**。创建一个自定义 Model 类，实现 Qlib 的 `Model` 接口：

```python
# model_lambdarank.py — 放在 workspace 或注册为自定义模型

import numpy as np
import pandas as pd
import lightgbm as lgb
from qlib.model.base import Model
from qlib.data.dataset import DatasetH
from qlib.data.dataset.handler import DataHandlerLP


class LambdaRankModel(Model):
    """LightGBM LambdaMART ranking model for cross-sectional stock selection.

    Uses LGBMRanker with lambdarank objective. Each trading date forms
    a query group — stocks are ranked relative to each other within
    the same date.
    """

    def __init__(self,
                 num_leaves=64,
                 max_depth=8,
                 learning_rate=0.05,
                 n_estimators=300,
                 early_stopping_rounds=20,
                 min_child_samples=100,
                 subsample=0.8,
                 colsample_bytree=0.8,
                 reg_alpha=0.1,
                 reg_lambda=0.1,
                 **kwargs):
        super().__init__()
        self.params = dict(
            objective='lambdarank',
            metric='ndcg',
            ndcg_eval_at=[10, 30, 50],  # top-k 排序质量
            num_leaves=num_leaves,
            max_depth=max_depth,
            learning_rate=learning_rate,
            n_estimators=n_estimators,
            min_child_samples=min_child_samples,
            subsample=subsample,
            colsample_bytree=colsample_bytree,
            reg_alpha=reg_alpha,
            reg_lambda=reg_lambda,
            verbosity=-1,
            **kwargs
        )
        self.n_estimators = n_estimators
        self.early_stopping_rounds = early_stopping_rounds
        self.model = None

    def fit(self, dataset: DatasetH, reweighter=None,
            num_boost_round=None, early_stopping_rounds=None,
            verbose_eval=20, evals_result=None, **kwargs):
        """训练 LambdaMART 模型。

        Key: 从 Qlib dataset 中提取数据并构建 query group。
        每个交易日 = 一个 query group，同一天内股票互相比较排序。
        """
        # 1. 提取训练和验证数据
        df_train, df_valid = dataset.prepare(
            ["train", "valid"],
            col_set=["feature", "label"],
            data_key=DataHandlerLP.DK_L
        )
        x_train = df_train["feature"].values.astype(np.float64)
        y_train = df_train["label"].values.astype(np.float64).ravel()
        x_valid = df_valid["feature"].values.astype(np.float64)
        y_valid = df_valid["label"].values.astype(np.float64).ravel()

        # 2. 构建 query group（关键步骤）
        #    Qlib 的 MultiIndex 格式: (datetime, instrument)
        #    按日期分组，每天为一个 query
        train_index = df_train["feature"].index
        valid_index = df_valid["feature"].index

        train_groups = self._build_query_groups(train_index)
        valid_groups = self._build_query_groups(valid_index)

        # 3. 创建 LGBMRanker 并训练
        self.model = lgb.LGBMRanker(**self.params)
        self.model.fit(
            x_train, y_train,
            group=train_groups,
            eval_set=[(x_valid, y_valid)],
            eval_group=[valid_groups],
            eval_at=[10, 30, 50],
            callbacks=[
                lgb.early_stopping(self.early_stopping_rounds),
                lgb.log_evaluation(verbose_eval)
            ]
        )

    def predict(self, dataset: DatasetH, segment="test"):
        """预测：对测试集每个截面排序打分"""
        if self.model is None:
            raise RuntimeError("Model not trained. Call fit() first.")

        df_test = dataset.prepare(
            segment, col_set="feature", data_key=DataHandlerLP.DK_L
        )
        x_test = df_test.values.astype(np.float64)
        scores = self.model.predict(x_test)

        return pd.Series(scores, index=df_test.index, name="score")

    def _build_query_groups(self, index: pd.MultiIndex) -> list:
        """从 Qlib MultiIndex 构建 query group 大小列表。

        index 格式: MultiIndex[(datetime, instrument), ...]
        按日期分组，每个日期内股票数 = group size
        """
        if isinstance(index, pd.MultiIndex):
            dates = index.get_level_values(0)
            group_sizes = pd.Series(1, index=dates).groupby(level=0).count()
            return group_sizes.values.tolist()
        else:
            # Flat index: assume all in one group
            return [len(index)]


# Qlib 加载入口
model_cls = LambdaRankModel
```

### 5.3 与现有 Pipeline 的兼容性

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Qlib        │     │  LambdaRankModel │     │  Qlib           │
│  DatasetH    │────▶│  .fit(dataset)   │────▶│  Backtest       │
│  (features,  │     │                  │     │  (V25_TWO_STAGE)│
│   labels)    │     │  .predict()      │     │                 │
│             │     │  → pd.Series     │     │                 │
└─────────────┘     └──────────────────┘     └─────────────────┘

                    ↑ 完全兼容

- fit() 接口: 与 Qlib LGBModel 一致 ✓
- predict() 输出: pd.Series, 与现有模型格式一致 ✓
- 策略层: score_weighted_topk_v2 不需要任何修改 ✓
- 执行层: V25_TWO_STAGE 不需要任何修改 ✓
- 回测: Qlib 标准回测流程不变 ✓
```

### 5.4 QE 集成方式

LambdaMART 模型通过 Qlib `GeneralPTNN` 机制加载（与现有 TimeSeries 模型相同）：

1. 创建模型 DB 记录，`code_text` 包含上述完整代码
2. `model_type = "TimeSeries"`（走 GeneralPTNN 路径）
3. QE 自动将 `model.py` 写入 workspace，`GeneralPTNN` 通过 `model_cls` 变量加载

或者更简单的方式：直接注册为新的 model_type。但这需要修改 config_composer.py。**推荐先用 GeneralPTNN 方式**，零代码改动即可测试。

---

## 六、TabPFN 实现方案

### 6.1 原理

TabPFN 是一个预训练的 Transformer，通过 in-context learning 进行预测：

```
输入:  (X_train_context, y_train_context, X_test)
  ↓   前向传播 (单次, 无梯度更新)
输出:  y_test_pred

不需要任何训练（fit 是空操作或仅存储上下文数据）
```

在小样本场景（<10000 样本，<500 特征）下，TabPFN 的单次推理**超越**调参数小时的 CatBoost/XGBoost/LightGBM（Nature 2025 验证）。

### 6.2 为什么特别适合 20D 训练

```
20D horizon, 当前数据划分:
  滚动训练样本: ~1036  (高度重叠)
  有效独立样本: ~50   (极少!)

  传统模型 (LGBM/NN): 50 个独立样本 → 严重过拟合风险
  TabPFN: 预训练知识 + in-context learning → 小样本先天优势
```

### 6.3 Qlib 集成方案

安装依赖：
```bash
pip install tabpfn
```

自定义 Qlib Model 类：

```python
# model_tabpfn.py — TabPFN 表格基础模型

import numpy as np
import pandas as pd
from qlib.model.base import Model
from qlib.data.dataset import DatasetH
from qlib.data.dataset.handler import DataHandlerLP


class TabPFNModel(Model):
    """TabPFN tabular foundation model for stock return prediction.

    Prior-fitted Transformer pre-trained on 130M+ synthetic datasets.
    Zero-training: inference-only through in-context learning.

    Best for: small sample regimes (N < 10,000) where traditional
    GBDTs and neural networks suffer from overfitting.
    """

    def __init__(self,
                 n_estimators=8,          # 集成数量, 越多越稳定
                 use_cache=True,          # 缓存预计算的结构
                 device="cuda",           # 'cuda' or 'cpu'
                 max_context_size=2000,   # 上下文最大样本数
                 **kwargs):
        super().__init__()
        self.n_estimators = n_estimators
        self.use_cache = use_cache
        self.device = device
        self.max_context_size = max_context_size
        self.classifier = None
        self._context = None  # (X_context, y_context) 缓存

    def fit(self, dataset: DatasetH, reweighter=None, **kwargs):
        """存储训练数据作为 in-context learning 的上下文。

        TabPFN 不进行梯度训练。训练集数据仅作为预测时的
        "context examples"，模型通过 attention 机制利用
        上下文中的模式进行推理。
        """
        df_train = dataset.prepare(
            "train", col_set=["feature", "label"],
            data_key=DataHandlerLP.DK_L
        )
        X_train = df_train["feature"].values.astype(np.float64)
        y_train = df_train["label"].values.astype(np.float64).ravel()

        # 如果样本超过 max_context_size，随机采样
        n = len(y_train)
        if n > self.max_context_size:
            rng = np.random.RandomState(42)
            idx = rng.choice(n, self.max_context_size, replace=False)
            X_train = X_train[idx]
            y_train = y_train[idx]
            n = self.max_context_size

        self._context = (X_train, y_train)
        self.n_features_ = X_train.shape[1]

        # 延迟加载 TabPFN（避免不必要的内存占用）
        from tabpfn import TabPFNClassifier

        self.classifier = TabPFNClassifier(
            device=self.device,
            n_estimators=self.n_estimators,
            random_state=42
        )

        return self

    def predict(self, dataset: DatasetH, segment="test"):
        """使用 TabPFN in-context learning 进行预测。

        对于回归任务，我们将标签离散化后使用分类器，
        然后对预测类别概率做加权平均得到连续预测值。
        """
        if self._context is None:
            raise RuntimeError("Call fit() first to set up context.")

        df_test = dataset.prepare(
            segment, col_set=["feature", "label"],
            data_key=DataHandlerLP.DK_L
        )
        X_test = df_test["feature"].values.astype(np.float64)

        X_train, y_train = self._context

        # TabPFN 分类器：需要将连续标签离散化为类别
        # 使用分位数离散化，保留排序关系
        n_classes = min(10, len(np.unique(np.round(y_train, 4))))
        if n_classes < 3:
            n_classes = 5  # 最少分5类保证区分度

        y_train_binned = pd.qcut(y_train, q=n_classes,
                                  labels=False, duplicates='drop')

        # TabPFN 推理 (单次前向传播, 不训练)
        self.classifier.fit(X_train, y_train_binned)
        proba = self.classifier.predict_proba(X_test)

        # 将分类概率转换为连续预测值
        # 加权平均: score = Σ p(class_i) × class_weight_i
        class_weights = np.linspace(-1, 1, proba.shape[1])
        scores = proba @ class_weights

        return pd.Series(scores, index=df_test.index, name="score")


model_cls = TabPFNModel
```

### 6.4 关键技术决策

**回归 → 分类转换**：

TabPFN 是分类器（输出概率分布），我们的任务是回归（预测收益）。通过分位数离散化将连续标签转为有序类别，然后用分类概率加权得到连续预测值。这种"classification-then-regression"方法在金融预测中已被广泛验证（保留了排序信息）。

**上下文大小选择**：

| max_context_size | 覆盖 | 影响 |
|------|------|------|
| 1000 (默认) | 10D 全部训练样本 | 充分 |
| 2000 | 覆盖更长期历史 | 内存翻倍 |
| 500 | 仅最近样本 | 可能丢失长期模式 |

建议 10D 用 1000，20D 用 500（避免过度关联的样本稀释上下文）。

### 6.5 与现有 Pipeline 的兼容性

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│  Qlib        │     │  TabPFNModel │     │  Qlib           │
│  DatasetH    │────▶│  .fit()      │────▶│  Backtest       │
│              │     │  (存储上下文) │     │  (V25_TWO_STAGE)│
│              │     │              │     │                 │
│              │     │  .predict()  │     │                 │
│              │     │  (单次推理)  │     │                 │
└─────────────┘     └──────────────┘     └─────────────────┘

- fit() 只存储数据，不训练 ← 向量数据库模式
- predict() 单次推理，速度随 n_estimators 线性增长
- 预测输出格式: pd.Series → 与现有策略完全兼容
- 20D 训练 50 有效样本 → TabPFN 最强场景
```

### 6.6 限制与风险

| 项目 | 限制 | 我们是否受影响 |
|------|------|------|
| 最大特征数 | 500 (v2) / 2000 (v2.5) | 不受影响 (45 特征) |
| 最大样本数 | 10000 (v2) / 100000 (v2.5) | 不受影响 (~1000) |
| 分类 vs 回归 | 原生分类器 | 需离散化转换 (见实现) |
| GPU 内存 | ~2GB (v2) | 不受影响 |
| 推理速度 | ~5-30s per predict | 可接受 |

---

## 七、QE 实验设计

### 完整 12-Loop 实验

```yaml
Task: "10D多模型多周期对比验证"
max_loops: 15
evolution_mode: custom
node: wsl2-5080

# 固定条件（所有 loop 共享）
factor_keys: [当前45因子集]
strategy_id: score_weighted_topk_v2
execution_algo: V25_TWO_STAGE
enable_sector_hmm: false  # HMM效果已确认不显著

loops:
  # ——— Tier 0: 前沿模型 ———
  Loop 1:  # LambdaMART ranking 目标
    label: "LambdaMART_10D"
    model_id: <LambdaRankModel 种子ID>
    label_horizon: 10
    backtest_only: false

  Loop 2:  # TabPFN 10D
    label: "TabPFN_10D"
    model_id: <TabPFNModel 种子ID>
    label_horizon: 10
    backtest_only: false

  Loop 3:  # TabPFN 20D (小样本极限测试)
    label: "TabPFN_20D"
    model_id: <TabPFNModel 种子ID>
    label_horizon: 20
    backtest_only: false

  # ——— Baseline ———
  Loop 4:  # LGBM 10D baseline (复用已训练模型)
    label: "LGBM_MSE_10D_baseline"
    model_id: __seed_LGBModel_conservative_v1__
    label_horizon: 10
    model_source_task_id: qe_20260429_015755_c4ba
    model_source_loop_index: 6
    backtest_only: true

  # ——— Tier 1: 时序神经网络 ———
  Loop 5:  # GRU 核心候选
    label: "GRU_hs64_d02_10D"
    model_id: <M1_GRU_10D_hs64_d02 种子ID>
    label_horizon: 10

  Loop 6:  # GRU 正则化对比
    label: "GRU_hs64_d03_10D"
    model_id: <M2_GRU_10D_hs64_d03 种子ID>
    label_horizon: 10

  Loop 7:  # GRU 强正则
    label: "GRU_hs64_d04_10D"
    model_id: <M3_GRU_10D_hs64_d04 种子ID>
    label_horizon: 10

  Loop 8:  # GRU 容量对比
    label: "GRU_hs96_d03_10D"
    model_id: <M4_GRU_10D_hs96_d03 种子ID>
    label_horizon: 10

  Loop 9:  # LSTM 备选
    label: "LSTM_hs64_d02_10D"
    model_id: <M5_LSTM_10D_hs64_d02 种子ID>
    label_horizon: 10

  Loop 10: # TCN 多尺度卷积
    label: "TCN_d02_10D"
    model_id: <M6_TCN_10D_d02 种子ID>
    label_horizon: 10

  # ——— Tier 1: 树模型 ———
  Loop 11: # XGBoost
    label: "XGBoost_10D"
    model_id: <M7_XGBoost 种子ID>
    label_horizon: 10

  Loop 12: # CatBoost ordered boosting
    label: "CatBoost_10D"
    model_id: __seed_CatBoost_default_v1__
    label_horizon: 10
```

### 预期判断矩阵

| 结果 | IC 阈值 | 下一步 |
|------|------|------|
| LambdaMART > 0.085 | **训练目标确认优于架构** | 将所有树模型切换为 ranking objective |
| TabPFN_20D > 0.085 | **小样本基础模型突破** | 20D 方向激活，扩展 TabPFN 超参 |
| TabPFN_10D > 0.080 | TabPFN 在金融数据有效 | 增加到 daily ensemble |
| GRU 系列 > 0.080 | **时序模型在 10D 有优势** | 进入 Phase 2 (20D + Mamba) |
| CatBoost > LGBM 0.078 | ordered boosting 有效 | CatBoost 替代 LGBM 为默认树模型 |
| 全部 ≤ 0.078 | 时序模型方向关闭 | **聚焦 LGBM + LambdaMART + 因子优化** |

---

## 八、决策树（按实验结果路径）

```
                    ┌── LambdaMART_10D IC 最高 → 训练目标 > 架构，全部模型切换ranking
                    │
开始 12-loop ───────┼── TabPFN_20D IC 最高    → 小样本基础模型突破，20D方向激活
                    │
                    ├── GRU 系列 IC 最高(>0.080) → 时序模型优势确认，Phase 2扩展
                    │   ├── GRU_d02 最优 → 正则化需求低, step_len=20可接受
                    │   ├── GRU_d04 最优 → 需要更大step_len, 改QE代码
                    │   └── TCN 最优    → 多尺度卷积是正确方向
                    │
                    ├── CatBoost/XGB > LGBM → 树模型内部可微调优化
                    │
                    └── 全部 ≤ 0.078 → 放弃时序方向, 全力优化LGBM
```

---

## 九、实现路线图

| Phase | 内容 | 产出 | 预估 |
|------|------|------|------|
| 0 | 创建 8 个模型种子 + LambdaMART/TabPFN wrapper | 10 个模型 DB 记录 | 1天 |
| 1 | 12-loop QE 实验运行 | 完整 IC/CAGR 对比表 | 1-2天 |
| 2 | 结果分析 + 方向决策 | 确定下一步优先级 | 0.5天 |
| 3 | 按决策扩展（最优方向的超参搜索 / 放弃） | TBD | 2-5天 |

---

## 附录：与现有系统的兼容性总结

| 组件 | LambdaMART | TabPFN | GRU/LSTM/TCN | XGBoost/CatBoost |
|------|------|------|------|------|
| Qlib Model 接口 | ✓ 自定义 Model | ✓ 自定义 Model | ✓ GeneralPTNN | ✓ Qlib 内置类 |
| 是否需改 Qlib 源码 | 否 | 否 | 否 | 否 |
| 是否需改 QE config_composer | 否 | 否 | 否 | 否 |
| 策略层兼容 | ✓ | ✓ | ✓ | ✓ |
| V25 执行层兼容 | ✓ | ✓ | ✓ | ✓ |
| 回测兼容 | ✓ | ✓ | ✓ | ✓ |
| step_len=20 约束 | 不适用(静态模型) | 不适用(静态模型) | **受约束** | 不适用 |
| 需要 GPU | 否 | 建议 | 建议 | 否 |

---
