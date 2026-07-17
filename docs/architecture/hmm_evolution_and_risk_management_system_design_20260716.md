# HMM 演进与风险管理系统详细设计

> **版本**: v1.1
> **日期**: 2026-07-16  
> **修订日期**: 2026-07-17
> **状态**: F2 蓝图已修订，Phase 0 hardening 完成前禁止进入 Phase 1 实施
> **范围**: HMM 快速演进、风险监控、滚动训练、数据隔离  
> **作者**: Kiro (Claude Code)
> **维护者**: AIstock HMM Evolution

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

### 1.4 Scope（当前批准范围）

- Phase 0 hardening：修复数据源真实运行契约、artifact 可信缓存、测试与 CI 覆盖。
- Phase 1：只读消费 QE/市场数据，在 `hmm_evolution.*` 中保存候选、评估任务和结果；输出 top-3 **研究推荐**。
- Phase 2：在 `hmm_risk.*` 中保存日度状态、预警与解释证据；UI 只展示分析结论。
- Phase 3：在独立候选注册表中进行滚动训练编排和时效性监控；不修改现有 QE/Paper 配置和生产 snapshot 状态。
- 所有阶段均不得改变 Selection、Advisory、Paper v2、MiniQMT、StrategyPackage 或 QE 的既有业务语义。

### 1.5 Non-goals（非目标与硬边界）

- 不自动启用、替换、下架或推广任何生产 HMM。
- 不向 `model_train_configs`、`model_train_snapshots`、`strategy_packages`、`paper_v2.*` 写入数据。
- 不把风险预警接入 `can_buy`、订单、持仓、调仓或任何交易决策链。
- 不把 top-3、保护率、一致性阈值升级为未经批准的研究淘汰门禁或生产准入门禁。
- 不下载 QE 配置文件；只允许下载有可信 manifest 的 `pred.pkl`、`label.pkl` 及设计明确允许的 HMM 系数 artifact。
- Phase 4+ 的 QE/Paper/实盘接入不属于本文实施范围，必须另立 F2 设计和审批。

### 1.6 当前实现状态与进入 Phase 1 的前置条件

PR #2227（merge `5cae5861853bd4be4a07699fb332224c2bdf54c2`）只代表 Phase 0 代码已进入 main，**不代表真实路径验收完成**。当前已知 hardening 阻塞项包括 QE 下载客户端契约、同步 DB 连接适配、canonical market schema、snapshot 过滤、可信 artifact manifest、缓存边界、Realtime 单测及真实集成证据。

Phase 1 开发前必须同时满足：

1. 两种数据源的真实受控 smoke 通过，且无生产 DB 写入。
2. HMM 数据源相关单元测试进入专用 CI/nox plan，不再依赖无关 `qe_data_contract_backend`。
3. artifact manifest、路径边界、原子写、缓存生命周期和反序列化信任边界完成直接测试。
4. 本文 Design Acceptance Index 中 `F-001` 至 `F-005` 均有实现与证据。

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
│  研发/回测环境              │  实时只读数据环境                     │
│  - QE artifact cache       │  - market.kline_daily_raw         │
│  - pred.pkl (固定)         │  - market.trading_calendar       │
│  - label.pkl (ground truth)│  - market.sw_index_member        │
│                            │                                    │
│  元数据存储                 │  资产注册表（现有）                  │
│  - hmm_evolution.*         │  - model_train_configs           │
│  - hmm_risk.*              │  - model_train_snapshots         │
│  - hmm_evolution.candidate │  - 现有配置/快照表仅作隔离边界      │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 数据流隔离设计

```python
# 数据源枚举
class HMMDataSourceMode(str, Enum):
    BACKTEST = "backtest"      # 研发：使用 QE artifact 固定数据
    REALTIME = "realtime"      # 分析：只读最新完成交易日数据

# 配置示例
研发环境:
{
    "data_source_mode": "backtest",
    "base_loop_ref": "qe_20260502_131502_9b54/Loop1",
    "artifact_cache_dir": "tmp/hmm_evolution_cache/",
    "use_label_as_truth": true
}

实时分析环境:
{
    "data_source_mode": "realtime",
    "connection_profile": "hmm_evolution_ro",
    "as_of_policy": "latest_completed_trading_day",
    "candidate_id": "<hmm_evolution.candidate id>"
}
```

实时模式中的 `t-1` 指“最新完成交易日”，不得用自然日减一实现。DB 连接必须复用仓库同步 `get_conn()` 适配器，禁止伪造异步 context manager；若未来引入真正异步池，必须以独立接口和直接集成测试交付。

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

**当前状态**: 代码已合入，真实路径验收待 hardening。

**验收标准**:
- [ ] 回测数据源通过任务节点解析，以现有 QE workspace 文件接口下载白名单 artifact。
- [ ] 实时数据源使用 canonical market schema 和上一完成交易日，按候选/快照身份过滤。
- [ ] 两种数据源均使用真实 DB/client 契约测试，不以错误 mock 代替。
- [ ] artifact 具有可信 manifest、SHA256、行数、schema version、来源任务与质量状态。
- [ ] 缓存路径不可越界，写入原子化，具备跨进程互斥、TTL/max-size/clear 生命周期。
- [ ] Realtime、Backtest、cache、isolation、integration 测试进入专用 CI 计划。

---
### Phase 1: HMM 离线评估与演进实验室（Week 2-3）

**目标**: 在固定历史输入上快速评估 HMM overlay，批量给出 top-3 研究推荐，最终有效性仍由 QE 终审。

**数据库 Schema**:
```sql
-- backend/db/init_hmm_evolution_schema.py

CREATE SCHEMA IF NOT EXISTS hmm_evolution;

CREATE TABLE hmm_evolution.candidate (
    candidate_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    artifact_manifest JSONB NOT NULL,
    algorithm_version TEXT NOT NULL,
    lifecycle_status TEXT NOT NULL,
    source_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE hmm_evolution.batch_test_run (
    batch_id TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    candidate_count INT NOT NULL,
    heartbeat_at TIMESTAMPTZ,
    error_code TEXT,
    error_context JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE hmm_evolution.offline_evaluation (
    eval_id TEXT PRIMARY KEY,
    batch_id TEXT REFERENCES hmm_evolution.batch_test_run(batch_id),
    candidate_id TEXT NOT NULL REFERENCES hmm_evolution.candidate(candidate_id),
    base_loop_ref TEXT NOT NULL,
    source_manifest JSONB NOT NULL,
    evaluation_spec JSONB NOT NULL,
    evaluator_version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    universe_id TEXT NOT NULL,
    topk INT NOT NULL,
    trading_days_count INT NOT NULL,
    coverage_ratio DOUBLE PRECISION NOT NULL,
    net_label_10d DOUBLE PRECISION,
    net_db_10d DOUBLE PRECISION,
    positive_net_label_day_ratio DOUBLE PRECISION,
    replacement_count INT NOT NULL,
    recommendation_score DOUBLE PRECISION,
    recommendation_rank INT,
    status TEXT NOT NULL,
    error_code TEXT,
    error_context JSONB,
    started_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(candidate_id, input_hash, evaluator_version)
);
```

实际 bootstrap 必须为全部 schema/table/column 添加 `COMMENT ON`，并在事务内幂等执行；业务 service 不得隐式建表。

**评估契约**:

- v1 计算基准来自 `scripts/diagnostics/hmm_offline_diagnostic.py::compute_replacements`：同一交易日对 raw score 与 HMM-adjusted score 使用稳定排序，比较 raw TopK 与 adjusted TopK 的 entered/dropped 集合。
- `net_label_10d` 为逐日 `mean(entered label_10d) - mean(dropped label_10d)` 的均值；必须同时保存有效日数、覆盖率和正值日比例。
- `net_db_10d` 使用 `market.kline_daily_raw.close_li` 按交易日序列计算 10 个交易日远期收益，禁止自然日 shift。
- `recommendation_score` 的归一化、权重、缺失值和并列规则必须作为版本化 `evaluation_spec` 提交；未批准前不得用临时公式生成 top-3。
- top-3 是研究推荐，不自动提交 QE、不修改生产配置、不淘汰未入选方向。
- 每次结果必须可由 `source_manifest + candidate manifest + evaluation_spec + evaluator_version + input_hash` 重放。

**API 端点**:
```
POST /api/v1/hmm-evolution/evaluate
POST /api/v1/hmm-evolution/batch
GET  /api/v1/hmm-evolution/evaluations/{eval_id}
GET  /api/v1/hmm-evolution/batches/{batch_id}
POST /api/v1/hmm-evolution/batches/{batch_id}/cancel
```

**验收标准**:
- [ ] 单个评估在批准基准数据集、指定硬件上冷缓存 < 10 分钟。
- [ ] 批量 10 个候选复用只读输入、限制并发，在同一基准上 < 30 分钟。
- [ ] 结果可重放，失败具有结构化错误、heartbeat、取消和幂等语义。
- [ ] 与至少 10 个历史 QE case 做方向性/排序一致性对照；差异只作为证据，不作为未经批准的淘汰门禁。
- [ ] 前端以中文表格、指标卡和对比图为主，原始 JSON 仅放高级调试抽屉。

---

### Phase 2: HMM 风险监控与预警系统（Week 4-5）

**目标**: 按最新完成交易日生成可解释风险预警和板块状态视图，不接入任何交易决策。

**数据库 Schema**:
```sql
CREATE SCHEMA IF NOT EXISTS hmm_risk;

CREATE TABLE hmm_risk.daily_alert (
    alert_id TEXT PRIMARY KEY,
    trade_date DATE NOT NULL,
    as_of_date DATE NOT NULL,
    candidate_id TEXT NOT NULL,
    sector_code TEXT NOT NULL,
    hmm_state TEXT NOT NULL,
    severity TEXT NOT NULL,
    transition_type TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    source_manifest JSONB NOT NULL,
    evidence JSONB NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    revision INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**预警语义**:

- `trending -> fading`：HIGH；持续 `fading`：MEDIUM；`fading -> trending`：OPPORTUNITY。
- 规则仅产生分析记录和解释证据，不产生 `can_buy`、调仓、订单或 profile 更新。
- 可以复用 `hmm_risk_gate_v1` 的 artifact 解析与状态计算，不得复用会生成 `RiskDecision(can_buy=False)` 的 Selection provider。
- 每日任务必须先校验 market/sector/候选 coefficient 的共同完成水位；缺数据时记录 failed/partial 和 reason code，不得用中性状态或旧日结果伪装成功。
- 同一 `candidate + trade_date + sector + rule_version` 幂等；迟到数据通过 revision 重算并保留历史，不覆盖审计轨迹。

**验收标准**:
- [ ] 日度预警任务可重跑、可审计、可解释，失败不影响 Selection/Paper/QMT。
- [ ] 热力图显示数据水位、candidate、rule version、状态置信度和缺失原因。
- [ ] UI 明示“仅供研究分析，不构成交易决策”。
- [ ] 风险回测报告展示命中率、误报、漏报、样本量和分阶段稳定性，不设置未经批准的保护率硬门禁。

---

### Phase 3: HMM 滚动训练自动化（Week 6）

**目标**: 对研究候选执行可审计的滚动训练和时效性监控，产物只登记到 `hmm_evolution.*`，不自动进入生产注册表。

**已有基础**:
```python
# backend/services/hmm_training_service.py（仅允许复用纯计划函数）
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
        candidate_template_id: str,
        schedule_id: str,
    ) -> RollingTrainPlan:
        """
        生成月度重训计划：
        1. 检查独立候选的最新训练水位与输入数据完成水位
        2. 按版本化 schedule policy 建立幂等训练任务
        3. 使用 latest - 3 months 作为验证集
        4. 训练完成后登记 hmm_evolution.candidate，不写生产 snapshot
        """
    
    async def check_model_staleness(
        self,
        candidate_id: str,
        max_age_days: int = 90,
    ) -> StalenessReport:
        """
        检查模型时效性：
        - 训练截止日期 vs 当前日期
        - 如果超过阈值，标记为 stale
        """
```

**调度边界**:

- 本文不得直接复用 `backend.routers.hmm_training.rolling_training_tick`；该路径会触发既有 `model_train_configs/model_train_snapshots` 写入。
- 既有 Paper v2 设计规定 rolling retraining 为用户触发。若要启用无人值守月度调度，必须在 Phase 3 实施前提交并批准单独的调度语义修订，明确 scheduler ownership、leader election、misfire、重入、重试、取消和停用开关。
- schedule、candidate template、训练窗口和资源池必须来自独立 DB 配置；禁止在脚本中硬编码生产 config id 或 cron。
- 调度器失败只能影响独立研究任务；不得阻塞或改变 QE、Selection、Paper、MiniQMT。

**验收标准**:
- [ ] 纯计划函数按真实交易日生成可重放窗口。
- [ ] 训练任务具备幂等键、heartbeat、超时、取消、失败上下文和并发上限。
- [ ] 新产物只进入独立 candidate registry，默认状态为 `research_only`。
- [ ] 未获得调度语义修订批准前，只交付预览与人工触发，不启用自动 cron。

---

### Phase 4+: 生产接入（待独立设计）

**范围**: 
- 接入 QE 实验（修改 qe_templates）
- 接入 Paper v2 模拟盘（修改 runtime_config）
- 实盘自动调仓（需充分回测）

**未来研究证据（非当前门禁）**:
- Phase 0-3 的设计验收矩阵无未批准缺口。
- 离线评估与 QE 的一致性、风险预警有效性和滚动训练稳定性形成可复查报告。
- 任何数值阈值必须在 Phase 4 独立设计中基于样本、成本、容量和失败代价重新批准；本文中的历史 90%/75%/3 个月仅作为研究假设，不构成自动准入条件。

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

- `base_loop_ref` 必须解析为 task/loop，并通过任务元数据解析 compute node；禁止依赖固定 `localhost:9000`。
- 下载必须调用仓库真实存在的 QE workspace 文件接口；业务层白名单严格限制为 `pred.pkl`、`label.pkl` 和独立批准的 coefficient artifact。
- 远端 manifest 必须先于反序列化校验；只有来源、sha256、size、row_count、schema_version 和 quality_status 均有效才允许进入缓存。
- 预测、标签必须标准化日期、symbol、数值类型、重复键和缺失值，并返回副本，禁止原地修改下载对象。
- 交易日历和行业映射使用一次批量查询，禁止逐日期 DB round trip。
- 客户端所有权明确：内部创建的 HTTP client 必须关闭，外部注入 client 由调用方管理。

### 4.3 实时数据源实现

- 实时分析输入只能来自已登记的独立 candidate/coefficient artifact 与 canonical market 表，不得猜测不存在的 `model_train_predictions`。
- `candidate_id` 必须进入所有预测查询或 artifact 解析；禁止混合不同模型、配置或窗口结果。
- `market.trading_calendar` 决定最新完成交易日和 N 日 horizon；行情字段使用 `ts_code`、`close_li`，行业映射使用 `market.sw_index_member` 的 PIT 生效区间。
- 当前仓库 DB 池是同步 psycopg2 context manager；service 可以用同步 repository，或明确在线程执行器中封装，禁止用 `async with` 伪装异步。
- `get_predictions` 与 `get_labels` 必须共享明确的 `as_of_date`，不得依赖 `CURRENT_DATE` 产生不可重放结果。
- 无有效 candidate、共同数据水位不足或标签尚未实现时 fail-fast，并返回稳定 reason code。

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
  hmm_evolution/
    run_daily_alerts.py                 # 受控 runner；不硬编码 schedule/config

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
    lib/navigation/nav-groups.ts       # 明确导航入口和中文标签

backend/tests/
  hmm_data_source/
  hmm_evolution/
  hmm_risk/
  hmm_rolling_train/

docs/
  architecture/
    hmm_evolution_and_risk_management_system_design_20260716.md  # 本文档
```

### 5.2 不污染规范

**允许写入**:
- `docs/architecture/` - 架构设计文档
- `backend/` - 生产代码
- `frontend/` - 前端代码
- `backend/tests/`、`frontend/tests/` - 测试代码

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
| 离线评估与 QE 结果不一致 | HIGH | MEDIUM | 对比 10+ 历史 case，保存差异归因；不把单一阈值变成研究淘汰门禁 |
| 数据源契约漂移 | HIGH | HIGH | canonical schema contract test + 真实受控 smoke + 专用 CI |
| artifact 损坏或来源不可信 | HIGH | MEDIUM | 可信 manifest、原子写、来源校验、路径边界、fail-fast |
| 风险预警误报率高 | MEDIUM | MEDIUM | 展示样本量/误报/漏报/分阶段稳定性，不干预交易 |
| 滚动训练重复或越权写生产表 | HIGH | MEDIUM | 独立 registry、幂等任务、DB 最小权限、写表 allowlist |

### 6.2 业务风险

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|---------|
| 用户过度依赖预警 | HIGH | MEDIUM | UI 明确标注"仅供参考，最终决策需人工判断" |
| 预警被误解为交易指令 | HIGH | MEDIUM | UI/接口明确 advisory-only，测试断言无 RiskDecision/订单副作用 |
| 数据延迟导致预警失效 | LOW | LOW | 监控数据时效性，超时告警 |

---

## 7. 实施时间表

### Gate 0: Phase 0 hardening
- 修复 QE client/DB/canonical schema 和 candidate identity。
- 修复 artifact/cache 安全与容量边界。
- 补专用 CI、真实集成 smoke 和性能基准；完成后才允许 Gate 1。

### Gate 1: Phase 1 - 离线评估
- Day 1-2: 数据库 schema + 基础服务
- Day 3-4: 评估核心逻辑 + 评分算法
- Day 5-6: API 端点 + 单元测试
- Day 7-8: 前端 UI 开发
- Day 9-10: 端到端测试 + 历史 case 验证

### Gate 2: Phase 2 - 风险监控
- Day 1-2: 数据库 schema + 预警生成逻辑
- Day 3-4: 板块热力图 + 时间线追踪
- Day 5-6: 风险门控回测
- Day 7-8: 前端 UI 开发
- Day 9-10: 定时任务 + 集成测试

### Gate 3: Phase 3 - 滚动训练
- 先交付独立候选 registry、计划预览和人工触发。
- 自动调度必须等待调度语义修订批准；不得以安装 APScheduler 或启用旧 tick 作为实现。

---

## 8. Design Acceptance Index（设计验收索引）

- **F-001 Phase 0 QE artifact 契约**：按 task/loop 解析节点，调用真实 workspace 文件接口，只接收可信 manifest 白名单 artifact。
- **F-002 Phase 0 canonical DB 契约**：同步 DB adapter、`market.trading_calendar`、`market.sw_index_member`、`ts_code/close_li` 和 PIT 区间正确。
- **F-003 Phase 0 candidate identity**：所有预测、系数、标签和评估均绑定明确 candidate/source identity，不混用不同模型结果。
- **F-004 Phase 0 cache 安全**：路径不可越界、原子写、跨进程互斥、可信校验、安全清理、TTL/max-size/clear 生命周期完整。
- **F-005 Phase 0 测试与 CI**：Realtime/Backtest/cache/isolation/integration 有直接测试并进入专用 nox/CI；真实性能证据可追溯。
- **F-006 Phase 1 独立候选注册表**：研究候选只写 `hmm_evolution.*`，artifact manifest 和生命周期可审计。
- **F-007 Phase 1 评估可重放**：输入、窗口、universe、算法版本、指标定义和 hash 足以复算同一结果。
- **F-008 Phase 1 批处理状态机**：幂等、heartbeat、取消、超时、并发上限、部分失败和结构化错误完整。
- **F-009 Phase 1 推荐语义**：top-3 仅为研究推荐，公式版本化，不自动淘汰方向或修改 QE/Paper。
- **F-010 Phase 1 API/UI**：真实 API、中文业务视图、可读错误和高级调试抽屉完整，导航入口可达。
- **F-011 Phase 2 预警状态机**：共同数据水位、规则版本、dedupe/revision、解释证据和迟到数据重算完整。
- **F-012 Phase 2 advisory-only**：无 `RiskDecision`、`can_buy`、订单、持仓、配置或调仓副作用。
- **F-013 Phase 2 风险分析证据**：命中、误报、漏报、样本量和阶段稳定性完整，不擅自新增硬门禁。
- **F-014 Phase 3 独立训练候选**：只复用纯滚动窗口计划，训练产物仅进入独立 registry，默认 `research_only`。
- **F-015 Phase 3 调度安全**：人工触发默认不变；自动调度需独立批准，并具备 ownership、leader、misfire、重入和停用语义。
- **F-016 全阶段隔离与生产边界**：不修改既有 QE/Paper/StrategyPackage/生产 snapshot，不启服务、不隐式 DDL，生产门禁逐 PR 明确。

## 9. Implementation Plan（实施方案）

1. **P0-A 数据契约修复**：处理 F-001/F-002/F-003，先让两种数据源真实可运行。
2. **P0-B artifact/cache hardening**：处理 F-004，并补可信 manifest 与容量边界。
3. **P0-C 验证与文档收敛**：处理 F-005，修正文档、专用 CI、受控 smoke 和 benchmark。
4. **P1-A schema/repository**：交付 F-006/F-008 的 Python bootstrap、repository 和任务状态机。
5. **P1-B evaluator**：从既有诊断脚本抽取纯计算逻辑，交付 F-007/F-009；不得复制其中硬编码连接或临时 I/O。
6. **P1-C API/UI**：交付 F-010，并完成真实 API/UI 验证。
7. **P2 风险分析**：依次交付 F-011/F-012/F-013；不得触碰交易 provider 接线。
8. **P3 研究训练**：先交付 F-014；F-015 自动调度部分等待独立语义批准。

每个实现 PR 只承担一个可验证子集，并在 PR body 中列设计项、实现引用、验证证据、生产门禁与未批准缺口。Phase 0 BUG 修复走 issue workflow；Phase 1-3 新能力走 feature workflow。

## 10. Verification Plan（验证方案）

- **静态小门**：changed-file Ruff/TypeScript lint、`git diff --check`、allowed scope、guardrail scan。
- **数据源 contract test**：使用真实同步连接接口的 fake repository；断言 canonical 表/字段、交易日 horizon、candidate 过滤和 fail-fast reason code。
- **受控 integration smoke**：只读测试 DB/QE workspace；禁止生产写、DDL 和生产端口，保存 compact receipt。
- **artifact 安全测试**：远端 manifest 不匹配、metadata 缺失、恶意 `..`/反斜杠、并发半写、reparse point、安全 clear、超限淘汰。
- **评估 oracle**：与 `hmm_offline_diagnostic.compute_replacements` 固定 fixture 对齐，再用至少 10 个历史 case 比较 QE 方向/排序差异。
- **性能基准**：记录硬件、输入行数、候选数、冷/热缓存、阶段耗时、峰值内存；单候选 <10 分钟、10 候选 <30 分钟仅在批准基准上判定。
- **风险副作用测试**：调用 Phase 2 API/job 后断言 Selection/Paper/QMT 表和 `RiskDecision` 输出不变。
- **滚动训练隔离测试**：DB 写表 allowlist 仅含 `hmm_evolution.*`；生产 config/snapshot 表写权限撤销时研究训练仍能正确失败并保留审计。
- **UI 证据**：安全验证端口上的真实 API 页面/E2E 或截图；不得启用 8001/3000/19080。
- **广域回归**：交给 Validation Center/CI/nightly 去重执行；本地只保留直接修复点最小门。

## 11. Design Acceptance Matrix（设计验收矩阵）

本表记录 v1.1 设计已获用户授权进入实施；`implementation_refs` 和 `test_or_evidence` 中的“目标”不是完成声明，每个实现 PR 必须将对应行替换为真实引用和证据后才能报告该设计项完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 目标：hmm_data_source backtest/client adapter | 目标：真实 QE 受控 smoke | approved_by_user_for_implementation | 实现证据由 Phase 0 BUG PR 回填 |
| F-002 | 目标：DB repository/canonical schema | 目标：contract + read-only integration | approved_by_user_for_implementation | 实现证据由 Phase 0 BUG PR 回填 |
| F-003 | 目标：candidate/source identity contract | 目标：混用拒绝与过滤测试 | approved_by_user_for_implementation | 实现证据由 Phase 0 BUG PR 回填 |
| F-004 | 目标：ArtifactCacheManager hardening | 目标：路径/原子/锁/容量测试 | approved_by_user_for_implementation | 实现证据由 Phase 0 BUG PR 回填 |
| F-005 | 目标：HMM nox/CI plan 与验收文档 | 目标：CI receipt/benchmark | approved_by_user_for_implementation | 实现证据由 Phase 0 BUG PR 回填 |
| F-006 | 目标：hmm_evolution.candidate bootstrap/repository | 目标：schema/comment/repository test | approved_by_user_for_implementation | 实现证据由 Phase 1 PR 回填 |
| F-007 | 目标：versioned evaluator | 目标：fixture oracle + replay hash | approved_by_user_for_implementation | 实现证据由 Phase 1 PR 回填 |
| F-008 | 目标：batch/evaluation job state machine | 目标：幂等/heartbeat/cancel/failure test | approved_by_user_for_implementation | 实现证据由 Phase 1 PR 回填 |
| F-009 | 目标：recommendation scorer | 目标：版本/并列/缺失/无副作用 test | approved_by_user_for_implementation | 实现证据由 Phase 1 PR 回填 |
| F-010 | 目标：API、UI、导航 | 目标：API contract + UI E2E/截图 | approved_by_user_for_implementation | 实现证据由 Phase 1 PR 回填 |
| F-011 | 目标：hmm_risk alert state machine | 目标：watermark/dedupe/revision test | approved_by_user_for_implementation | 实现证据由 Phase 2 PR 回填 |
| F-012 | 目标：advisory-only service boundary | 目标：Selection/Paper/QMT 无副作用 test | approved_by_user_for_implementation | 实现证据由 Phase 2 PR 回填 |
| F-013 | 目标：risk evidence/report | 目标：指标、样本量、分阶段稳定性 test | approved_by_user_for_implementation | 实现证据由 Phase 2 PR 回填 |
| F-014 | 目标：research-only rolling candidate | 目标：DB write allowlist + artifact test | approved_by_user_for_implementation | 实现证据由 Phase 3 PR 回填 |
| F-015 | 目标：manual-first scheduler contract | 目标：ownership/misfire/reentry/disable test | approved_by_user_for_implementation | 自动调度实现仍需独立用户批准 |
| F-016 | 目标：全阶段 isolation guard | 目标：scope/production-gate/side-effect evidence | approved_by_user_for_implementation | 实现证据由各阶段 PR 回填 |

## 12. Rollout / Rollback（发布与回滚）

- Phase 0 hardening 先合入，不启用新 runtime；发现回归可回滚对应代码 PR，缓存格式升级须保留版本目录并支持安全重建。
- Phase 1/2 API 和 UI 默认不注册到生产调度；schema bootstrap 与运行时代码分开 gate，DDL 未显式授权时报告 `production_ddl_pending`。
- Phase 2 job 首次发布只允许人工受控执行；确认幂等、水位和副作用测试后，才可登记只读日任务。
- Phase 3 默认仅预览/人工触发；自动调度若后续获批，必须有全局 disable switch、单实例 ownership 和逐 schedule 停用能力。
- 回滚任何阶段不得删除历史评估、预警或候选审计；使用 lifecycle status 标记 retired/invalid，并保留输入 manifest。
- 任何回滚都不得修改已存在的生产 HMM、StrategyPackage、Paper portfolio 或交易记录。

## 13. Production Gates（生产门禁）

- `production_ddl_gate`：Phase 0 为 `noop`；Phase 1/2 schema PR 默认为 `pending`，仅在用户显式授权后 `applied_and_verified`。
- `production_backend_dependency_gate`：无新依赖时 `noop`；新增 scheduler/serialization 依赖必须独立列出并安装验证。
- `production_frontend_dependency_gate`：默认 `noop`；禁止为新页面传播 legacy Paper v2 UI 依赖。
- `runtime_activation_gate`：代码合入与 runtime activation 分离；不得自动启动 8001/3000/19080 或注册生产 scheduler。
- `data_write_gate`：只允许 `hmm_evolution.*`、`hmm_risk.*`；其它 schema 写入为 fail-closed。
- `design_compliance_gate`：每个实现 PR 必须执行 DESIGN-COMPLIANCE-001，四项控制均有证据：`no_simplified_delivery`、`no_silent_error`、`no_business_semantic_drift`、`no_unrequested_gate_or_approval`。

---

## 14. 附录

### 14.1 参考文档

- `docs/architecture/research_pipeline_and_mcp_gateway_design_v2.md` - Research Pipeline 架构
- `docs/analysis/hmm_offline_diagnostic_qe_20260502_131502_9b54.md` - 离线诊断示例
- `docs/analysis/hmm_risk_gate_validation_20260517.md` - 风险门控验证
- `backend/tests/test_hmm_rolling_training.py` - 滚动训练测试

### 14.2 相关 Issue

- BUG-663: sector_data 恢复依赖安全
- BUG-312: HMM runtime cache issue
- BUG-076: HMM coefficients path

### 14.3 变更历史

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.1 | 2026-07-17 | 增补 F2 scope/non-goals/DAI/矩阵/发布回滚/生产门禁；Phase 0 改为 hardening 前置 gate；收紧 Phase 1-3 隔离语义 |
| v1.0 | 2026-07-16 | 初始版本，定义 Phase 0-3 |

---

**文档归档路径**: `docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md`
