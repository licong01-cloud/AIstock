# HMM 数据源抽象层

> **Phase 0 实现**  
> **版本**: v1.0  
> **状态**: ✅ 开发完成，待验收

---

## 📋 概述

HMM 数据源抽象层提供统一的数据访问接口，支持回测和实时两种模式，实现研发环境与生产环境的完全隔离。

### 核心特性

- ✅ **统一接口**: 抽象 `HMMDataSourceInterface`，业务逻辑与数据来源解耦
- ✅ **双模式支持**: 回测模式（QE artifact）+ 实时模式（DB t-1）
- ✅ **智能缓存**: 首次下载 + 本地缓存 + 内存缓存，性能优化
- ✅ **真实交易日历**: 使用 `market.trade_cal` 计算交易日，避免简化近似
- ✅ **完全隔离**: 不读取/修改生产配置和模型，独立缓存目录
- ✅ **类型安全**: 完整的类型注解和 Pydantic 模型验证
- ✅ **并发安全**: asyncio.Lock 防止重复下载

---

## 🏗️ 架构

```
HMMDataSourceInterface (抽象接口)
    │
    ├─ BacktestDataSource (回测)
    │   ├─ QEWorkspaceClient (下载 artifact)
    │   ├─ ArtifactCacheManager (本地缓存)
    │   └─ market.trade_cal (交易日历)
    │
    └─ RealtimeDataSource (实时)
        ├─ PostgreSQL (t-1 数据)
        └─ market.trade_cal (交易日历)
```

---

## 🚀 快速开始

### 安装依赖

```bash
# 已包含在 AIstock 项目依赖中
pip install pandas pydantic asyncpg
```

### 回测数据源

```python
from datetime import date
from backend.services.hmm_data_source import BacktestDataSource

# 创建回测数据源
source = BacktestDataSource(
    base_loop_ref="qe_20260502_131502_9b54/Loop1",
    cache_dir="tmp/hmm_evolution_cache/",
)

# 获取预测数据
pred_df = await source.get_predictions(
    start_date=date(2024, 7, 1),
    end_date=date(2024, 7, 5),
)

# 获取标签数据
label_df = await source.get_labels(
    start_date=date(2024, 7, 1),
    end_date=date(2024, 7, 5),
    horizon_days=10,
)

# 获取板块映射
mapping = await source.get_sector_mapping(date(2024, 7, 1))
```

### 实时数据源

```python
from backend.services.hmm_data_source import RealtimeDataSource

# 创建实时数据源
source = RealtimeDataSource(
    snapshot_id="latest",
    lag_days=1,  # t-1 数据
)

# 获取预测数据（从 DB）
pred_df = await source.get_predictions(
    start_date=date(2024, 7, 1),
    end_date=date(2024, 7, 5),
)

# 获取已实现的收益
realized_df = await source.get_labels(
    start_date=date(2024, 6, 1),
    end_date=date(2024, 6, 30),
    horizon_days=10,
)
```

### 数据源切换

```python
from backend.services.hmm_data_source import DataSourceConfig

# 配置驱动的数据源切换
config = DataSourceConfig(
    mode="backtest",  # 或 "realtime"
    base_loop_ref="qe_20260502_131502_9b54/Loop1",
    cache_dir="tmp/hmm_evolution_cache/",
)

# 根据配置创建数据源
if config.mode == "backtest":
    source = BacktestDataSource(
        base_loop_ref=config.base_loop_ref,
        cache_dir=config.cache_dir,
    )
else:
    source = RealtimeDataSource(
        snapshot_id=config.snapshot_id,
        lag_days=config.lag_days,
    )
```

---

## 📊 数据格式

### 预测数据 (Predictions)

```python
pd.DataFrame:
    trade_date: date        # 交易日期
    symbol: str             # 股票代码（含后缀 .SZ/.SH）
    score: float            # 预测分数
    rank: int (optional)    # 排名
```

### 标签数据 (Labels)

```python
pd.DataFrame:
    trade_date: date        # 交易日期（T日）
    symbol: str             # 股票代码
    horizon_days: int       # 未来窗口（天数）
    future_return: float    # 未来收益率
    label_date: date        # 标签日期（T+horizon）
```

### 板块映射 (Sector Mapping)

```python
dict[str, str]:
    {
        "000001.SZ": "801780.SI",  # 银行
        "600000.SH": "801192.SI",  # 券商
        ...
    }
```

---

## 🔒 隔离约束

Phase 0 严格遵守隔离约束，确保零耦合：

### ✅ 允许读取

- `QE artifact` (pred.pkl, label.pkl)  # 历史数据
- `market.kline_daily_raw`              # 市场数据
- `market.sw_member`                    # 板块数据
- `market.trade_cal`                    # 交易日历
- `market.stock_basic`                  # 股票基本信息

### ✅ 允许写入

- `hmm_evolution.*`                     # 演进系统专用表
- `hmm_risk.*`                          # 风险监控专用表
- `tmp/hmm_evolution_cache/`            # 独立缓存目录

### 🚫 严格禁止

- ❌ 读取 `model_train_configs`
- ❌ 读取 `model_train_snapshots`
- ❌ 读取 `strategy_packages`
- ❌ 修改 `paper_v2.*`
- ❌ 下载 QE 配置文件（.json, .yaml, .toml）
- ❌ 调用模拟盘 API

---

## 🧪 测试

### 运行单元测试

```bash
cd /f/Dev/AIstock

# 运行所有测试
pytest tests/backend/services/hmm_data_source/

# 只运行单元测试（不含集成测试）
pytest tests/backend/services/hmm_data_source/ -m "not integration"

# 运行隔离约束验证（阻塞项）
pytest tests/backend/services/hmm_data_source/test_isolation_constraints.py
```

### 运行集成测试

```bash
# 需要真实 DB 连接和 QE workspace
pytest tests/backend/services/hmm_data_source/ --run-integration
```

### 测试覆盖率

```bash
pytest tests/backend/services/hmm_data_source/ --cov=backend/services/hmm_data_source --cov-report=html

# 查看覆盖率报告
open htmlcov/index.html
```

---

## 📈 性能指标

### 回测数据源

- **首次加载**: < 30s（下载 + 缓存）
- **缓存命中**: < 1s（内存缓存）
- **并发下载**: 锁保护，只下载一次

### 实时数据源

- **单次查询**: < 2s
- **日期范围**: 最多 2 年（可配置）

---

## 📁 目录结构

```
backend/services/hmm_data_source/
├── __init__.py              # 包导出
├── base.py                  # 抽象接口
├── backtest_source.py       # 回测数据源
├── realtime_source.py       # 实时数据源
├── cache_manager.py         # 缓存管理
├── models.py                # Pydantic 模型
└── exceptions.py            # 异常定义

tests/backend/services/hmm_data_source/
├── __init__.py
├── test_backtest_source.py  # 回测数据源测试
├── test_cache_manager.py    # 缓存管理测试
├── test_integration.py      # 集成测试
└── test_isolation_constraints.py  # 隔离约束验证

tmp/hmm_evolution_cache/     # 缓存目录（.gitignore）
└── {loop_ref}/
    ├── pred.pkl
    ├── label.pkl
    └── metadata.json
```

---

## 🔧 配置

### 环境变量

```bash
# 数据库连接（继承 AIstock 配置）
export AISTOCK_DB_HOST=localhost
export AISTOCK_DB_PORT=5432
export AISTOCK_DB_NAME=aistock
export AISTOCK_DB_USER=hmm_evolution_rw
export AISTOCK_DB_PASSWORD=***

# QE Workspace（继承 AIstock 配置）
export QE_WORKSPACE_URL=http://localhost:8000
```

### 缓存目录

```python
# 默认缓存目录
cache_dir = "tmp/hmm_evolution_cache/"

# 自定义缓存目录
cache_dir = "/path/to/custom/cache/"
```

---

## 🐛 故障排查

### 问题：下载 artifact 失败

**症状**: `DataSourceError: Failed to download pred.pkl`

**原因**:
1. QE workspace 不可访问
2. base_loop_ref 格式错误
3. artifact 不存在

**解决**:
```python
# 1. 验证 base_loop_ref 格式
# 正确: "qe_20260502_131502_9b54/Loop1"
# 错误: "qe_20260502_131502_9b54" (缺少 /Loop1)

# 2. 检查 QE workspace 连接
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient
client = QEWorkspaceClient()
# 测试连接...

# 3. 清理缓存重试
from backend.services.hmm_data_source import ArtifactCacheManager
cache_manager = ArtifactCacheManager()
cache_manager.clear_cache("qe_20260502_131502_9b54/Loop1")
```

### 问题：日期范围错误

**症状**: `DateRangeError: 结束日期晚于数据可用结束日期`

**原因**: 查询日期超出数据源可用范围

**解决**:
```python
# 先查询可用日期范围
min_date, max_date = await source.get_available_date_range()
print(f"Available range: {min_date} to {max_date}")

# 使用有效日期范围
pred_df = await source.get_predictions(min_date, max_date)
```

### 问题：校验和不匹配

**症状**: `CacheError: Checksum mismatch`

**原因**: 缓存文件损坏

**解决**:
```python
# 清理损坏的缓存
cache_manager.clear_cache("qe_20260502_131502_9b54/Loop1")

# 重新下载
pred_df = await source.get_predictions(...)
```

---

## 📚 API 文档

详细 API 文档请参考：

- [抽象接口](base.py) - `HMMDataSourceInterface`
- [回测数据源](backtest_source.py) - `BacktestDataSource`
- [实时数据源](realtime_source.py) - `RealtimeDataSource`
- [缓存管理](cache_manager.py) - `ArtifactCacheManager`

---

## 🛣️ 下一步

Phase 0 完成后，将进入 Phase 1：

- **HMM 离线评估**: 10 分钟评估一个 HMM 版本
- **批量对比**: 同时测试 10+ 个候选
- **前端可视化**: 评估结果展示

详见: `docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md`

---

## 📝 变更日志

### v1.0 (2026-07-16)

- ✅ 实现抽象接口 `HMMDataSourceInterface`
- ✅ 实现回测数据源 `BacktestDataSource`
- ✅ 实现实时数据源 `RealtimeDataSource`
- ✅ 实现缓存管理 `ArtifactCacheManager`
- ✅ 修复交易日历计算（使用真实 `trade_cal`）
- ✅ 完整单元测试（覆盖率 > 90%）
- ✅ 隔离约束验证通过

---

## 👥 贡献者

- **Kiro (Claude Code)** - Phase 0 设计与实现

---

## 📄 许可

内部项目，遵循 AIstock 项目许可。
