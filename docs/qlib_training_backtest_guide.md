# Qlib 训练与回测指南

## 目录
1. [只做模型训练，不做回测](#1-只做模型训练不做回测)
2. [使用最新数据重新训练](#2-使用最新数据重新训练)
3. [模型权重导出与实盘应用](#3-模型权重导出与实盘应用)
4. [数据时间分配建议](#4-数据时间分配建议)
5. [只使用 Qlib 进行训练](#5-只使用-qlib-进行训练)
6. [量化交易对市场的影响](#6-量化交易对市场的影响)

---

## 1. 只做模型训练，不做回测

### 1.1 修改配置文件

在配置文件中找到 `record` 部分，移除 `PortAnaRecord`：

```yaml
record: 
    - class: SignalRecord
      module_path: qlib.workflow.record_temp
      kwargs: 
        model: <MODEL>
        dataset: <DATASET>
    - class: SigAnaRecord
      module_path: qlib.workflow.record_temp
      kwargs: 
        ana_long_short: False
        ann_scaler: 252
    # 移除 PortAnaRecord（回测记录）
    # - class: PortAnaRecord
    #   module_path: qlib.workflow.record_temp
    #   kwargs: 
    #     config: *port_analysis_config
```

### 1.2 具体操作步骤

```bash
# 步骤1：复制配置文件
cd /mnt/f/Dev/RD-Agent-main/app_tpl/all/v1/rdagent/scenarios/qlib/experiment/factor_template
cp conf_baseline.yaml conf_baseline_train_only.yaml

# 步骤2：修改 record 部分（移除 PortAnaRecord）

# 步骤3：使用新配置文件训练
rdagent fin_factor \
    --config_path conf_baseline_train_only.yaml \
    --market all \
    --provider_uri /mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20251209 \
    --region CN
```

### 1.3 Record 说明

| Record 类型 | 说明 | 是否回测 |
|------------|------|----------|
| SignalRecord | 记录模型预测的信号 | 否 |
| SigAnaRecord | 分析信号的 IC、ICIR 等指标 | 否 |
| PortAnaRecord | 执行回测，计算年化收益、最大回撤等 | 是 |

### 1.4 只训练不回测的影响

**优点：**
- 节省时间：跳过回测，大幅减少训练时间
- 节省资源：减少计算资源消耗
- 快速迭代：适合快速验证模型

**缺点：**
- 无法评估策略：无法知道策略的实际表现
- 无法计算收益：无法计算年化收益、夏普比率等
- 无法评估风险：无法评估最大回撤等风险指标

---

## 2. 使用最新数据重新训练

### 2.1 优势分析

| 方面 | 说明 |
|------|------|
| 市场适应性 | 最新数据更能反映当前市场环境和结构变化 |
| 因子有效性 | 因子有效性随时间衰减，最新训练能保持因子有效性 |
| 模型泛化 | 避免过拟合历史数据，提高泛化能力 |
| 实盘表现 | 理论上应该获得更好的实盘效果 |

### 2.2 风险与限制

| 风险 | 说明 |
|------|------|
| 数据量不足 | 5年数据可能不足以训练复杂模型 |
| 过拟合风险 | 短期数据容易过拟合近期市场特征 |
| 样本偏差 | 5年可能包含特殊市场事件（如疫情、政策变化） |
| 稳定性问题 | 模型可能对短期波动过于敏感 |

### 2.3 推荐策略

```yaml
# 推荐的训练数据配置
segments:
    # 训练：使用较长历史数据保证稳定性
    train: [2015-01-01, 2022-12-31]  # 8年数据
    # 验证：使用近期数据验证
    valid: [2023-01-01, 2023-12-31]
    # 测试：使用最新数据测试
    test: [2024-01-01, 2025-12-01]
```

---

## 3. 模型权重导出与实盘应用

### 3.1 Qlib 模型导出

Qlib 训练后会自动保存模型文件，通常位于：

```
mlruns/<experiment_id>/<run_id>/artifacts/model.pkl
```

### 3.2 导出模型权重的配置

在配置文件中添加模型保存配置：

```yaml
task:
    model:
        class: LGBModel
        module_path: qlib.contrib.model.gbdt
        kwargs:
            loss: mse
            device_type: cpu
            # 添加模型保存配置
            model_path: "./model.pkl"  # 模型保存路径
            save_model: true  # 保存模型
    record:
        - class: SignalRecord
          module_path: qlib.workflow.record_temp
          kwargs:
            model: <MODEL>
            dataset: <DATASET>
            # 添加信号保存配置
            save_signal: true
            signal_path: "./predictions.pkl"
```

### 3.3 在 AIstock 侧使用模型进行实盘

```python
import pandas as pd
import pickle
import qlib
from qlib.config import REG_CN
from qlib.data import D

# 1. 初始化 Qlib
qlib.init(
    provider_uri='/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20251209',
    region=REG_CN
)

# 2. 加载训练好的模型
with open('model.pkl', 'rb') as f:
    model = pickle.load(f)

# 3. 获取最新数据
today = pd.Timestamp.now().strftime('%Y-%m-%d')
df = D.features(
    instruments='all',
    fields=['$close', '$volume', '$high', '$low', '$open'],  # 根据实际因子调整
    start_time=today,
    end_time=today
)

# 4. 使用模型预测
predictions = model.predict(df)

# 5. 选股逻辑
top_stocks = predictions.nlargest(50)  # 选择预测值最高的50只股票
print("今日推荐股票：")
print(top_stocks.index.tolist())
```

### 3.4 实盘交易流程

```python
# 实盘交易示例
def execute_trading(predictions):
    """
    根据模型预测执行实盘交易
    """
    # 1. 获取当前持仓
    current_positions = get_current_positions()
    
    # 2. 选股
    top_stocks = predictions.nlargest(50)
    
    # 3. 调仓逻辑
    for stock in top_stocks.index:
        if stock not in current_positions:
            # 买入新股票
            buy_stock(stock, amount=10000)
    
    # 4. 清仓逻辑
    for stock in current_positions:
        if stock not in top_stocks.index:
            # 卖出不在推荐列表的股票
            sell_stock(stock)
```

### 3.5 定期更新模型

```python
# 定期重新训练模型（建议每月或每季度）
import schedule
import time

def retrain_model():
    """
    定期重新训练模型
    """
    # 1. 更新数据
    update_data()
    
    # 2. 重新训练
    rdagent fin_factor \
        --config_path conf_baseline_train_only.yaml \
        --market all \
        --provider_uri /mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20251209 \
        --region CN \
        --loop-n 1
    
    # 3. 导出新模型
    export_model()

# 每月1号重新训练
schedule.every().month.do(retrain_model)

while True:
    schedule.run_pending()
    time.sleep(3600)  # 每小时检查一次
```

---

## 4. 数据时间分配建议

### 4.1 当前配置分析

```yaml
segments:
    train: [2010-01-07, 2018-12-31]  # 9年
    valid: [2019-01-01, 2020-12-31]  # 2年
    test: [2021-01-01, 2025-12-01]  # 5年
```

**问题分析：**
- ✅ 训练数据充足（9年）
- ✅ 验证数据合理（2年）
- ⚠️ 回测数据较长（5年）

### 4.2 推荐配置

```yaml
segments:
    # 训练：8-10年，保证数据量充足
    train: [2015-01-07, 2022-12-31]  # 8年
    # 验证：1-2年，验证模型泛化能力
    valid: [2023-01-01, 2023-12-31]  # 1年
    # 回测：2-3年，评估实盘表现
    test: [2024-01-01, 2025-12-01]  # 2年
```

### 4.3 不同场景的配置

| 场景 | 训练 | 验证 | 回测 | 说明 |
|------|------|------|------|------|
| 快速验证 | 5年 | 1年 | 1年 | 快速迭代，节省时间 |
| 标准配置 | 8年 | 1年 | 2年 | 平衡效果与效率 |
| 稳健配置 | 10年 | 2年 | 3年 | 保证稳定性，适合生产 |
| 深度学习 | 10年 | 2年 | 2年 | 深度学习需要更多数据 |

### 4.4 回测时间选择

**当前5年回测是否合理？**

| 评估维度 | 5年回测 | 2-3年回测 |
|----------|---------|-----------|
| 市场代表性 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| 计算成本 | ⭐⭐ | ⭐⭐⭐⭐⭐ |
| 实盘相关性 | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| 策略稳定性 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

**结论：**
- ✅ 5年回测**合理**，能充分评估策略
- ✅ 2-3年回测**更高效**，适合快速迭代
- ⚠️ 建议：**先用2-3年快速验证，再用5年充分评估**

---

## 5. 只使用 Qlib 进行训练

### 5.1 Qlib 训练脚本示例

```python
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
import pickle

# 1. 初始化 Qlib
qlib.init(
    provider_uri='/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20251209',
    region=REG_CN
)

# 2. 配置数据处理器
data_handler_config = {
    "start_time": "2015-01-07",
    "end_time": "2025-12-01",
    "instruments": "all",
    "data_loader": {
        "class": "Alpha158",
        "module_path": "qlib.contrib.data.handler"
    },
    "infer_processors": [
        {
            "class": "RobustZScoreNorm",
            "kwargs": {
                "fields_group": "feature",
                "clip_outlier": True,
                "fit_start_time": "2015-01-07",
                "fit_end_time": "2020-12-31"
            }
        },
        {
            "class": "Fillna",
            "kwargs": {
                "fields_group": "feature"
            }
        }
    ],
    "learn_processors": [
        {"class": "DropnaLabel"},
        {
            "class": "CSZScoreNorm",
            "kwargs": {
                "fields_group": "label"
            }
        }
    ]
}

# 3. 配置数据集
dataset = DatasetH(
    handler={
        "class": "Alpha158",
        "module_path": "qlib.contrib.data.handler",
        "kwargs": data_handler_config
    },
    segments={
        # 训练：8年数据
        "train": ("2015-01-07", "2022-12-31"),
        # 验证：1年数据
        "valid": ("2023-01-01", "2023-12-31"),
        # 测试：2年数据
        "test": ("2024-01-01", "2025-12-01")
    }
)

# 4. 配置模型
model = LGBModel(
    loss="mse",
    device_type="cpu",
    max_bin=63,
    colsample_bytree=0.8879,
    learning_rate=0.2,
    subsample=0.8789,
    lambda_l1=205.6999,
    lambda_l2=580.9768,
    max_depth=8,
    num_leaves=210,
    num_threads=20
)

# 5. 训练模型
print("开始训练模型...")
model.fit(dataset)
print("模型训练完成")

# 6. 保存模型
with open('model.pkl', 'wb') as f:
    pickle.dump(model, f)
print("模型已保存到 model.pkl")
```

### 5.2 执行命令

```bash
# 保存训练脚本
cat > train_only.py << 'EOF'
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
import pickle

qlib.init(
    provider_uri='/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20251209',
    region=REG_CN
)

data_handler_config = {
    "start_time": "2015-01-07",
    "end_time": "2025-12-01",
    "instruments": "all",
    "data_loader": {
        "class": "Alpha158",
        "module_path": "qlib.contrib.data.handler"
    },
    "infer_processors": [
        {
            "class": "RobustZScoreNorm",
            "kwargs": {
                "fields_group": "feature",
                "clip_outlier": True,
                "fit_start_time": "2015-01-07",
                "fit_end_time": "2020-12-31"
            }
        },
        {
            "class": "Fillna",
            "kwargs": {
                "fields_group": "feature"
            }
        }
    ],
    "learn_processors": [
        {"class": "DropnaLabel"},
        {
            "class": "CSZScoreNorm",
            "kwargs": {
                "fields_group": "label"
            }
        }
    ]
}

dataset = DatasetH(
    handler={
        "class": "Alpha158",
        "module_path": "qlib.contrib.data.handler",
        "kwargs": data_handler_config
    },
    segments={
        "train": ("2015-01-07", "2022-12-31"),
        "valid": ("2023-01-01", "2023-12-31"),
        "test": ("2024-01-01", "2025-12-01")
    }
)

model = LGBModel(
    loss="mse",
    device_type="cpu",
    max_bin=63,
    colsample_bytree=0.8879,
    learning_rate=0.2,
    subsample=0.8789,
    lambda_l1=205.6999,
    lambda_l2=580.9768,
    max_depth=8,
    num_leaves=210,
    num_threads=20
)

model.fit(dataset)

with open('model.pkl', 'wb') as f:
    pickle.dump(model, f)

print("模型训练完成，已保存到 model.pkl")
EOF

# 执行训练
python train_only.py
```

### 5.3 优势

- ✅ 节省时间（跳过回测）
- ✅ 节省资源（减少计算）
- ✅ 快速迭代（适合频繁更新）
- ✅ 适合实盘（定期更新模型）

---

## 6. 量化交易对市场的影响

### 6.1 量化交易的特征

| 特征 | 说明 | 对市场的影响 |
|------|------|--------------|
| 高频交易 | 毫秒级交易 | 增加市场波动，降低传统策略有效性 |
| 算法交易 | 自动化执行 | 削弱传统技术指标信号 |
| 套利交易 | 快速捕捉价差 | 减少套利机会，降低因子收益 |
| 因子拥挤 | 相似策略集中 | 因子收益衰减，相关性增加 |
| 程序化交易 | 规则化执行 | 市场行为更规律，但更难预测 |

### 6.2 对传统因子的影响

| 因子类型 | 传统表现 | 量化交易后表现 |
|----------|----------|----------------|
| 动量因子 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐（收益衰减） |
| 反转因子 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐（效果增强） |
| 量价因子 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐（信号减弱） |
| 资金流因子 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐（更有效） |
| 情绪因子 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐（变化不大） |

### 6.3 使用最近数据训练的优势

| 变化类型 | 最近数据能否识别 | 说明 |
|----------|------------------|------|
| 市场结构变化 | ✅ 能 | 量化交易改变了市场微观结构 |
| 因子有效性变化 | ✅ 能 | 某些因子收益衰减，某些增强 |
| 交易行为变化 | ✅ 能 | 量化交易行为与传统交易不同 |
| 市场波动模式 | ✅ 能 | 量化交易增加了市场波动 |
| 长期趋势变化 | ⚠️ 部分能 | 需要更长时间观察 |

### 6.4 具体优势

```python
# 使用最近数据训练的优势
advantages = {
    "市场适应性": "更能适应当前市场环境",
    "因子有效性": "识别当前有效的因子",
    "交易行为": "捕捉量化交易行为特征",
    "实时性": "反映最新市场变化",
    "泛化能力": "避免过拟合历史数据"
}
```

### 6.5 推荐配置

```yaml
# 推荐配置：平衡稳定性与适应性
segments:
    # 训练：8年数据（2017-2025）
    train: [2017-01-07, 2024-12-31]
    # 验证：1年数据（2025）
    valid: [2025-01-01, 2025-12-01]
    # 测试：未来1年（2026）
    test: [2026-01-01, 2026-12-01]
```

### 6.6 因子选择建议

```python
# 推荐因子组合
recommended_factors = {
    "传统有效因子": [
        "动量因子（短期）",
        "反转因子（中期）",
        "量价因子（调整后）"
    ],
    "量化交易时代有效因子": [
        "资金流因子",
        "订单流因子",
        "情绪因子",
        "流动性因子"
    ],
    "避免因子": [
        "长期动量因子（>1年）",
        "简单技术指标",
        "传统量价因子（未调整）"
    ]
}
```

### 6.7 模型更新策略

```python
# 推荐更新频率
update_strategy = {
    "快速迭代": "每月更新（适合量化交易活跃期）",
    "标准更新": "每季度更新（平衡效果与成本）",
    "稳健更新": "每半年更新（适合稳定策略）"
}
```

---

## 7. 总结

### 7.1 关键要点

1. **只做训练不回测**：通过移除 `PortAnaRecord`，可以大幅节省时间和资源
2. **使用最新数据**：8年数据训练，平衡稳定性与适应性
3. **模型导出**：Qlib 训练后自动保存模型，可直接用于实盘
4. **数据配置**：训练8年，验证1年，回测2-3年
5. **量化交易影响**：使用最近数据能更好地识别市场变化

### 7.2 最佳实践

| 实践 | 建议 |
|------|------|
| 训练数据 | 8-10年（2015-2025） |
| 验证数据 | 1-2年 |
| 回测数据 | 2-3年（快速验证）或5年（充分评估） |
| 模型更新 | 每季度或每月 |
| 因子选择 | 传统因子 + 量化时代有效因子 |
| 风险控制 | 设置止损、仓位限制 |

### 7.3 实施建议

1. **使用最近数据训练**：8年数据，平衡稳定性与适应性
2. **定期更新模型**：每季度或每月更新
3. **调整因子组合**：加入资金流、订单流等新因子
4. **加强风险控制**：量化交易增加波动，需要更严格的风控
5. **对比实验**：对比传统训练与最近数据训练的效果

### 7.4 结论

- ✅ 使用最近历史数据训练**能够识别量化交易带来的市场变化**，使因子效果更好
- ✅ 只使用 Qlib 进行训练**完全可行**，不需要 RD-Agent 全流程
- ✅ 模型权重数据**可以直接导出**，用于 AIstock 侧实盘选股和交易
- ✅ 5年回测**合理**，但2-3年回测**更高效**，建议先用2-3年快速验证

---

## 附录

### A. 完整训练脚本（只训练，不回测）

```python
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
import pickle

def train_model():
    """
    只训练模型，不进行回测
    """
    # 1. 初始化 Qlib
    qlib.init(
        provider_uri='/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20251209',
        region=REG_CN
    )
    
    # 2. 配置数据处理器
    data_handler_config = {
        "start_time": "2015-01-07",
        "end_time": "2025-12-01",
        "instruments": "all",
        "data_loader": {
            "class": "Alpha158",
            "module_path": "qlib.contrib.data.handler"
        },
        "infer_processors": [
            {
                "class": "RobustZScoreNorm",
                "kwargs": {
                    "fields_group": "feature",
                    "clip_outlier": True,
                    "fit_start_time": "2015-01-07",
                    "fit_end_time": "2020-12-31"
                }
            },
            {
                "class": "Fillna",
                "kwargs": {
                    "fields_group": "feature"
                }
            }
        ],
        "learn_processors": [
            {"class": "DropnaLabel"},
            {
                "class": "CSZScoreNorm",
                "kwargs": {
                    "fields_group": "label"
                }
            }
        ]
    }
    
    # 3. 配置数据集（推荐配置）
    dataset = DatasetH(
        handler={
            "class": "Alpha158",
            "module_path": "qlib.contrib.data.handler",
            "kwargs": data_handler_config
        },
        segments={
            # 训练：8年数据
            "train": ("2015-01-07", "2022-12-31"),
            # 验证：1年数据
            "valid": ("2023-01-01", "2023-12-31"),
            # 测试：2年数据
            "test": ("2024-01-01", "2025-12-01")
        }
    )
    
    # 4. 配置模型
    model = LGBModel(
        loss="mse",
        device_type="cpu",
        max_bin=63,
        colsample_bytree=0.8879,
        learning_rate=0.2,
        subsample=0.8789,
        lambda_l1=205.6999,
        lambda_l2=580.9768,
        max_depth=8,
        num_leaves=210,
        num_threads=20
    )
    
    # 5. 训练模型
    print("开始训练模型...")
    model.fit(dataset)
    print("模型训练完成")
    
    # 6. 保存模型
    with open('model.pkl', 'wb') as f:
        pickle.dump(model, f)
    print("模型已保存到 model.pkl")
    
    # 7. 评估模型（可选）
    print("\n模型评估：")
    pred = model.predict(dataset)
    print(f"预测结果形状: {pred.shape}")
    print(f"预测结果统计:")
    print(pred.describe())
    
    return model

if __name__ == '__main__':
    model = train_model()
    print("\n训练完成！")
```

### B. AIstock 实盘选股脚本

```python
import pandas as pd
import pickle
import qlib
from qlib.config import REG_CN
from qlib.data import D

def load_and_predict():
    """
    加载模型并预测
    """
    # 1. 初始化 Qlib
    qlib.init(
        provider_uri='/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20251209',
        region=REG_CN
    )
    
    # 2. 加载模型
    with open('model.pkl', 'rb') as f:
        model = pickle.load(f)
    
    # 3. 获取最新数据
    today = pd.Timestamp.now().strftime('%Y-%m-%d')
    df = D.features(
        instruments='all',
        fields=['$close', '$volume', '$high', '$low', '$open'],
        start_time=today,
        end_time=today
    )
    
    # 4. 预测
    predictions = model.predict(df)
    
    # 5. 选股
    top_stocks = predictions.nlargest(50)
    
    return top_stocks

if __name__ == '__main__':
    stocks = load_and_predict()
    print(f"今日推荐股票（{len(stocks)}只）：")
    for i, (stock, score) in enumerate(stocks.items(), 1):
        print(f"{i}. {stock}: {score:.4f}")
```

---

**文档版本**: v1.0  
**最后更新**: 2026-01-24  
**适用版本**: RD-Agent v1, Qlib
