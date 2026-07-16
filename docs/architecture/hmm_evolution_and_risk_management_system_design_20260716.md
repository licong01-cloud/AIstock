# HMM 演进与风险管理系统详细设计

> **版本**: v1.0  
> **日期**: 2026-07-16  
> **状态**: 架构定稿，分 4 阶段实施  
> **范围**: HMM 快速演进、风险监控、滚动训练、数据隔离  
> **作者**: Kiro (Claude Code)

---

## 1. 执行摘要

### 1.1 背景与动机

HMM 板块轮动模型自 2026-04-04 修复以来，已有 2 个生产可用版本和 18 个实验候选。但当前研发流程存在以下瓶颈：

1. **验证成本过高**: 每次 HMM 改进需完整 QE 回测（6-12 小时），资源占用重
2. **串行瓶颈**: 一次只能验证 1-2 个版本，迭代速度慢（1 天 1-2 轮）
3. **反馈滞后**: 问题在 QE 结束后才暴露，无法快速定位
4. **风险管理缺失**: HMM 风险门控已实现但未暴露给用户，无预警机制
5. **数据混用风险**: 研发使用回测数据，但真实场景需连接 t-1 行情库

### 1.2 核心目标

**Phase 0-3 目标**:
- ✅ 离线快速评估：10 分钟评估一个 HMM 版本（vs 6-12 小时 QE）
- ✅ 批量对比筛选：同时测试 10+ 个候选，自动推荐 top-3
- ✅ 风险预警可视化：每日板块风险预警 + 状态热力图
- ✅ 滚动训练自动化：定期重训 HMM，保持模型时效性
- ✅ 数据环境隔离：研发用回测数据，生产用 t-1 实时数据

**Phase 4+ 目标** (待独立设计):
- 接入 QE 和模拟盘（需独立审批）
- 实盘自动调仓（需充分回测验证）

### 1.3 核心约束

1. **研发与生产数据隔离**: 
   - 研发/回测: 使用固定历史数据（pred.pkl, label.pkl）
   - 生产/模拟盘: 连接实时数据库（t-1 kline_daily_raw）
   
2. **不修改现有流程**: 
   - QE 实验和模拟盘的 HMM 配置保持不变
   - 研发确认有效后再考虑接入
   
3. **只做展示和分析**:
   - 前期不介入实盘/模拟盘自动操作
   - 风险预警仅为建议，决策由人工判断
   
4. **不污染项目目录**:
   - 遵循 AIstock 文档规范（`docs/architecture/`）
   - 临时文件放 `tmp/` 或 `.codex_tmp/`
   - 不新增裸 `.sql` 文件，使用 Python schema bootstrap

---

## 2. 总体架构

### 2.1 系统分层

```
┌─────────────────────────────────────────────────────────────────┐
│                    前端 UI 层（三大模块）                           │
├─────────────────────────────────────────────────────────────────┤
│  HMM Evolution Lab    │  HMM Risk Monitor   │  Research Pipeline │
│  - 快速评估            │  - 风险预警面板       │  Inspector        │
│  - 候选对比            │  - 板块热力图         │  - 实验追踪        │
│  - 批量测试            │  - 历史事件追踪       │  - 对比可视化      │
└─────────────────────────────────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────┐
│                    业务服务层（六大服务）                           │
├─────────────────────────────────────────────────────────────────┤
│  HMMEvolutionService        │  HMMRiskMonitorService            │
│  - offline_evaluate()       │  - generate_daily_alerts()        │
│  - batch_compare()          │  - get_sector_heatmap()           │
│  - submit_top_candidates()  │  - backtest_gate_effectiveness()  │
│                            │                                    │
│  HMMRollingTrainService     │  HMMDataSourceService            │
│  - plan_rolling_schedule()  │  - get_backtest_data()           │
│  - trigger_retrain()        │  - get_realtime_data()           │
│  - monitor_staleness()      │  - validate_data_freshness()     │
└─────────────────────────────────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────┐
│                    数据层（环境隔离）                               │
├─────────────────────────────────────────────────────────────────┤
│  研发/回测环境              │  生产/模拟盘环境                     │
│  - QE artifact cache       │  - market.kline_daily_raw (t-1)  │
│  - pred.pkl (固定)         │  - market.sw_daily (实时)         │
│  - label.pkl (ground truth)│  - model_train_snapshots (最新)  │
│                            │                                    │
│  元数据存储                 │  资产注册表（现有）                  │
│  - hmm_evolution.*         │  - model_train_configs           │
│  - hmm_risk.*              │  - model_train_snapshots         │
│  - research_pipeline.*     │  - strategy_packages             │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 数据流隔离设计

```python
# 数据源枚举
class HMMDataSourceMode(str, Enum):
    BACKTEST = "backtest"      # 研发：使用 QE artifact 固定数据
    REALTIME = "realtime"      # 生产：连接 t-1 数据库

# 配置示例
研发环境:
{
    "data_source_mode": "backtest",
    "base_loop_ref": "qe_20260502_131502_9b54/Loop1",
    "artifact_cache_dir": "tmp/hmm_evolution_cache/",
    "use_label_as_truth": true
}

生产环境:
{
    "data_source_mode": "realtime",
    "db_connection": "postgresql://aistock_rw@localhost/aistock",
    "lag_days": 1,  # t-1 数据
    "snapshot_id": "latest"
}
```

---

## 3. 阶段划分与实施目标

### Phase 0: 数据基础与环境隔离（Week 1）

**目标**: 建立数据抽象层，确保研发与生产数据隔离

**交付物**:
```
backend/services/hmm_data_source/
  __init__.py
  base.py              # 抽象基类 HMMDataSourceInterface
  backtest_source.py   # 回测数据源（QE artifact）
  realtime_source.py   # 实时数据源（DB t-1）
  cache_manager.py     # QE artifact 缓存管理
  models.py            # Pydantic models
```

**���收标准**:
- ✅ 回测数据源可从 QE workspace 下载并缓存 pred.pkl/label.pkl
- ✅ 实时数据源可查询 t-1 的 kline_daily_raw
- ✅ 单元测试覆盖两种数据源
- ✅ 数据源切换通过配置，无需修改业务代码

---
### Phase 1: HMM 离线评估与演进实验室（Week 2-3）

**目标**: 10 分钟评估一个 HMM 版本，批量筛选 top-3 候选

**数据库 Schema**:
```sql
-- backend/db/init_hmm_evolution_schema.py

CREATE SCHEMA IF NOT EXISTS hmm_evolution;

CREATE TABLE hmm_evolution.offline_evaluation (
    eval_id TEXT PRIMARY KEY,
    eval_batch_id TEXT,
    base_loop_ref TEXT NOT NULL,
    hmm_snapshot_id TEXT NOT NULL,
    hmm_config_id TEXT,
    
    -- 评估参数
    data_source_mode TEXT DEFAULT 'backtest',
    topk INT DEFAULT 50,
    trading_days_count INT,
    
    -- 替换质量指标
    net_label_10d DECIMAL(8,4),
    net_db_10d DECIMAL(8,4),
    
    -- 综合评分
    overall_score DECIMAL(6,2),
    
    status TEXT DEFAULT 'completed',
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

**API 端点**:
```
POST /api/v1/hmm-evolution/evaluate
POST /api/v1/hmm-evolution/batch
GET  /api/v1/hmm-evolution/evaluations/{eval_id}
```

**验收标准**:
- ✅ 单个评估 < 10 分钟
- ✅ 批量评估 10 个候选 < 30 分钟
- ✅ 前端可视化完整

---

### Phase 2: HMM 风险监控与预警系统（Week 4-5）

**目标**: 每日自动生成风险预警，板块状态可视化

**数据库 Schema**:
```sql
CREATE SCHEMA IF NOT EXISTS hmm_risk;

CREATE TABLE hmm_risk.daily_alert (
    alert_id TEXT PRIMARY KEY,
    trade_date DATE NOT NULL,
    hmm_snapshot_id TEXT NOT NULL,
    sector_code TEXT NOT NULL,
    hmm_state TEXT NOT NULL,
    severity TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

**验收标准**:
- ✅ 日度预警自动生成
- ✅ 板块热力图实时更新

---

### Phase 3: HMM 滚动训练自动化（Week 6）

**目标**: 定期重训 HMM，保持模型时效性

**已有基础**:
```python
# backend/services/hmm_training_service.py (已实现)
def build_rolling_training_plan(
    trading_days: list[date],
    latest_completed_trade_date: date,
    train_window_years: float = 3.0,
    validation_window_months: int = 3,
) -> dict:
    """
    构建滚动训练计划：
    - 训练窗口: 3 年
    - 验证窗口: 3 个月
    - 自动调整到交易日
    """
```

**新增功能**:
```python
# backend/services/hmm_rolling_train/scheduler.py

class HMMRollingTrainScheduler:
    async def plan_monthly_retrain(
        self,
        config_id: str,
        schedule: str = "0 2 1 * *",  # 每月1日凌晨2点
    ) -> RollingTrainPlan:
        """
        生成月度重训计划：
        1. 检查最新 snapshot 时效性
        2. 如果数据更新超过 30 天，触发重训
        3. 使用 latest - 3 months 作为验证集
        4. 训练完成后自动注册新 snapshot
        """
    
    async def check_model_staleness(
        self,
        snapshot_id: str,
        max_age_days: int = 90,
    ) -> StalenessReport:
        """
        检查模型时效性：
        - 训练截止日期 vs 当前日期
        - 如果超过阈值，标记为 stale
        """
```

**定时任务**:
```bash
# scripts/cron/monthly_hmm_retrain.py
# 每月1日凌晨2点执行

python scripts/cron/monthly_hmm_retrain.py \
  --config-id ce4952c1-4b0d-46a7-81f2-ae1d4a249555 \
  --auto-register
```

**验收标准**:
- ✅ 月度重训自动触发
- ✅ 模型时效性监控
- ✅ 重训完成后自动注册新 snapshot

---

### Phase 4+: 生产接入（待独立设计）

**范围**: 
- 接入 QE 实验（修改 qe_templates）
- 接入 Paper v2 模拟盘（修改 runtime_config）
- 实盘自动调仓（需充分回测）

**前提条件**:
- ✅ Phase 0-3 全部验收通过
- ✅ 离线评估与 QE 结果一致性 > 90%
- ✅ 风险门控回测保护率 > 75%
- ✅ 滚动训练稳定运行 3 个月

**审批流程**:
- 需独立设计文档
- 需风险评估报告
- 需生产环境测试方案

---

## 4. 数据环境隔离详细设计

### 4.1 抽象接口

```python
# backend/services/hmm_data_source/base.py

from abc import ABC, abstractmethod
from datetime import date
import pandas as pd

class HMMDataSourceInterface(ABC):
    """HMM 数据源抽象接口"""
    
    @abstractmethod
    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        获取预测分数
        
        Returns:
            DataFrame with columns: [trade_date, symbol, score]
        """
        pass
    
    @abstractmethod
    async def get_labels(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int = 10,
    ) -> pd.DataFrame:
        """
        获取未来收益标签
        
        Returns:
            DataFrame with columns: [trade_date, symbol, future_return]
        """
        pass
    
    @abstractmethod
    async def get_sector_mapping(
        self,
        trade_date: date,
    ) -> dict[str, str]:
        """
        获取股票板块映射
        
        Returns:
            {symbol: sector_code}
        """
        pass
    
    @property
    @abstractmethod
    def mode(self) -> str:
        """返回数据源模式: 'backtest' 或 'realtime'"""
        pass
```

### 4.2 回测数据源实现

```python
# backend/services/hmm_data_source/backtest_source.py

class BacktestDataSource(HMMDataSourceInterface):
    """基于 QE artifact 的回测数据源"""
    
    def __init__(
        self,
        base_loop_ref: str,
        cache_dir: str = "tmp/hmm_evolution_cache/",
    ):
        self.base_loop_ref = base_loop_ref
        self.cache_dir = Path(cache_dir)
        self._pred_df = None
        self._label_df = None
        self._sector_map = None
    
    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        从缓存的 pred.pkl 读取
        如果缓存不存在，从 QE workspace 下载
        """
        if self._pred_df is None:
            await self._download_and_cache_artifacts()
        
        return self._pred_df[
            (self._pred_df['trade_date'] >= start_date) &
            (self._pred_df['trade_date'] <= end_date)
        ]
    
    async def _download_and_cache_artifacts(self):
        """从 QE workspace 下载 artifact 到本地缓存"""
        # 使用 QEWorkspaceClient 下载
        # 保存到 cache_dir
        pass
    
    @property
    def mode(self) -> str:
        return "backtest"
```

### 4.3 实时数据源实现

```python
# backend/services/hmm_data_source/realtime_source.py

class RealtimeDataSource(HMMDataSourceInterface):
    """基于数据库 t-1 的实时数据源"""
    
    def __init__(
        self,
        snapshot_id: str = "latest",
        lag_days: int = 1,
    ):
        self.snapshot_id = snapshot_id
        self.lag_days = lag_days
    
    async def get_predictions(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        从 model_train_jobs 读取最新预测结果
        或从策略包的 daily predictions 读取
        """
        # 查询 DB，返回 t-1 的预测
        pass
    
    async def get_labels(
        self,
        start_date: date,
        end_date: date,
        horizon_days: int = 10,
    ) -> pd.DataFrame:
        """
        从 market.kline_daily_raw 计算未来收益
        注意：实时场景下，未来数据不可用
        返回已实现的收益（用于事后验证）
        """
        pass
    
    @property
    def mode(self) -> str:
        return "realtime"
```

---

## 5. 文件结构与规范

### 5.1 新增文件清单

```
backend/
  db/
    init_hmm_evolution_schema.py         # Phase 1
    init_hmm_risk_schema.py              # Phase 2
  
  services/
    hmm_data_source/
      __init__.py
      base.py
      backtest_source.py
      realtime_source.py
      cache_manager.py
    
    hmm_evolution/
      __init__.py
      service.py
      evaluator.py
      scorer.py
      models.py
    
    hmm_risk/
      __init__.py
      monitor_service.py
      alert_generator.py
      gate_backtester.py
    
    hmm_rolling_train/
      __init__.py
      scheduler.py
      staleness_checker.py
  
  routers/
    hmm_evolution.py
    hmm_risk.py

scripts/
  cron/
    generate_daily_hmm_alerts.py
    monthly_hmm_retrain.py

frontend/
  src/
    app/
      hmm-evolution/
        page.tsx
        [evalId]/page.tsx
        components/
      hmm-risk/
        page.tsx
        alerts/[alertId]/page.tsx
        components/

tests/
  backend/
    services/
      test_hmm_data_source.py
      test_hmm_evolution_service.py
      test_hmm_risk_monitor.py

docs/
  architecture/
    hmm_evolution_and_risk_management_system_design_20260716.md  # 本文档
```

### 5.2 不污染规范

**允许写入**:
- `docs/architecture/` - 架构设计文档
- `backend/` - 生产代码
- `frontend/` - 前端代码
- `tests/` - 测试代码

**临时文件**:
- `tmp/hmm_evolution_cache/` - QE artifact 缓存
- `.codex_tmp/hmm_offline_diag/` - 离线诊断临时文件

**禁止写入**:
- `docs/handoff/local/` - 仅本地临时文件
- 项目根目录 - 不新增散装文件
- 不新增裸 `.sql` 文件 - 使用 Python schema bootstrap

---

## 6. 风险与缓解

### 6.1 技术风险

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|---------|
| 离线评估与 QE 结果不一致 | HIGH | MEDIUM | Phase 1 验收时对比 10+ 个历史 case，一致性 > 85% |
| 数据源切换引入 bug | MEDIUM | LOW | 单元测试覆盖，隔离接口设计 |
| 风险预警误报率高 | MEDIUM | MEDIUM | 回测验证，设置合理阈值，提供 acknowledge 功能 |
| 滚动训练失败 | LOW | LOW | 监控告警，手动兜底 |

### 6.2 业务风险

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|---------|
| 用户过度依赖预警 | HIGH | MEDIUM | UI 明确标注"仅供参考，最终决策需人工判断" |
| 误杀优质标的 | MEDIUM | LOW | protect_top 参数，回测验证 |
| 数据延迟导致预警失效 | LOW | LOW | 监控数据时效性，超时告警 |

---

## 7. 实施时间表

### Week 1: Phase 0 - 数据基础
- Day 1-2: 抽象接口设计 + 单元测试框架
- Day 3-4: 回测数据源实现
- Day 5: 实时数据源实现 + 集成测试

### Week 2-3: Phase 1 - 离线评估
- Day 1-2: 数据库 schema + 基础服务
- Day 3-4: 评估核心逻辑 + 评分算法
- Day 5-6: API 端点 + 单元测试
- Day 7-8: 前端 UI 开发
- Day 9-10: 端到端测试 + 历史 case 验证

### Week 4-5: Phase 2 - 风险监控
- Day 1-2: 数据库 schema + 预警生成逻辑
- Day 3-4: 板块热力图 + 时间线追踪
- Day 5-6: 风险门控回测
- Day 7-8: 前端 UI 开发
- Day 9-10: 定时任务 + 集成测试

### Week 6: Phase 3 - 滚动训练
- Day 1-2: 滚动训练调度器
- Day 3: 时效性监控
- Day 4-5: 定时任务 + 测试

---

## 8. 验收检查清单

### Phase 0
- [ ] 回测数据源通过单元测试
- [ ] 实时数据源通过单元测试
- [ ] 数据源切换无需修改业务代码

### Phase 1
- [ ] 单个评估 < 10 分钟
- [ ] 批量评估 10 个候选 < 30 分钟
- [ ] 评分与 QE 结果一致性 > 85%
- [ ] 前端 UI 完整可用

### Phase 2
- [ ] 日度预警自动生成
- [ ] 板块热力图实时更新
- [ ] 风险门控回测保护率 > 70%
- [ ] 前端交互流畅 (<2s)

### Phase 3
- [ ] 月度重训自动执行
- [ ] 模型时效性监控正常
- [ ] 重训完成自动注册

---

## 9. 附录

### 9.1 参考文档

- `docs/architecture/research_pipeline_and_mcp_gateway_design_v2.md` - Research Pipeline 架构
- `docs/analysis/hmm_offline_diagnostic_qe_20260502_131502_9b54.md` - 离线诊断示例
- `docs/analysis/hmm_risk_gate_validation_20260517.md` - 风险门控验证
- `backend/tests/test_hmm_rolling_training.py` - 滚动训练测试

### 9.2 相关 Issue

- BUG-663: sector_data 恢复依赖安全
- BUG-312: HMM runtime cache issue
- BUG-076: HMM coefficients path

### 9.3 变更历史

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.0 | 2026-07-16 | 初始版本，定义 Phase 0-3 |

---

**文档归档路径**: `docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md`
