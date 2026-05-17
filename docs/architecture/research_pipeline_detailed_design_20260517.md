# Research Pipeline 模块详细设计

> 版本：v1.0
> 日期：2026-05-17
> 状态：设计文档，待评审
> 适用范围：AIstock Research Pipeline 模块（Phase 1: HMM + 事件信号）

---

## 1. 模块定位

Research Pipeline 是 AIstock 的**研究实验管理模块**，负责：
- 管理研究实验的完整生命周期（创建 → 执行 → 验证 → 晋升/拒绝）
- 自动化 stage gate 验收（程序判断通过/失败）
- 严格分离代码（git）与实验资产（DB + 文件）
- 通过 MCP server 暴露给 Claude/Codex 调度
- 通过 UI 提供研究进展可视化
- 归档所有实验结果到数仓（含失败实验）

---

## 2. 文件结构

```
backend/
├── services/research_pipeline/
│   ├── __init__.py
│   ├── models.py                    # Pydantic 模型 + dataclass
│   ├── experiment_registry.py       # 实验 CRUD + 状态机
│   ├── stage_executor.py            # Stage 执行引擎
│   ├── artifact_manager.py          # 资产管理（存储/验证/版本）
│   ├── validation_gate.py           # 自动验收判断
│   ├── promotion_service.py         # 晋升审批 + issue 创建
│   ├── scheduler.py                 # APScheduler 后台调度
│   ├── archive_handler.py           # 数仓归档 handler
│   ├── pipelines/
│   │   ├── __init__.py
│   │   ├── base.py                  # ResearchPipeline ABC
│   │   ├── hmm_research.py          # HMM 板块轮动研究
│   │   └── event_signal_research.py # 事件独立信号研究
│   └── constants.py                 # 版本号、事件类型、状态枚举
├── routers/
│   └── research_pipeline.py         # API 路由
├── db/
│   └── init_research_pipeline.sql   # Schema DDL
scripts/
└── aistock_research_mcp_server.py   # MCP Server
frontend/src/app/
└── research-pipeline/
    ├── page.tsx                      # 看板
    ├── [id]/page.tsx                 # 实验详情
    └── history/page.tsx             # 历史归档
```

---

## 3. DB Schema

```sql
-- ============================================================
-- Research Pipeline Schema
-- Version: research_pipeline_v1_20260517
-- ============================================================

CREATE SCHEMA IF NOT EXISTS research_pipeline;

-- ── 实验主表 ──
CREATE TABLE research_pipeline.experiment (
    experiment_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_type       TEXT NOT NULL,
    title               TEXT NOT NULL,
    hypothesis          TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'draft'
        CONSTRAINT ck_rp_experiment_status CHECK (
            status IN ('draft','running','stage_failed','validated','promoted','rejected','blocked')
        ),
    baseline_ref        TEXT,
    baseline_task_id    TEXT,
    acceptance_criteria JSONB NOT NULL DEFAULT '{}'::jsonb,
    config              JSONB NOT NULL DEFAULT '{}'::jsonb,
    tags                TEXT[] DEFAULT '{}',
    created_by          TEXT NOT NULL DEFAULT 'unknown',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    promoted_at         TIMESTAMPTZ,
    promoted_issue_url  TEXT,
    rejected_at         TIMESTAMPTZ,
    rejected_reason     TEXT,
    blocked_by_issue    TEXT,
    parent_experiment_id UUID,
    code_commit_hash    TEXT,
    data_version_ref    JSONB
);

CREATE INDEX idx_rp_experiment_status ON research_pipeline.experiment(status);
CREATE INDEX idx_rp_experiment_pipeline_type ON research_pipeline.experiment(pipeline_type);
CREATE INDEX idx_rp_experiment_created_at ON research_pipeline.experiment(created_at DESC);

COMMENT ON TABLE research_pipeline.experiment IS '研究实验主表，记录假设、配置、状态和验收标准';
COMMENT ON COLUMN research_pipeline.experiment.pipeline_type IS '流水线类型: hmm_risk_gate, event_signal, factor_research, execution_algo, strategy_optimization';
COMMENT ON COLUMN research_pipeline.experiment.status IS '生命周期: draft→running→validated→promoted / stage_failed→rejected / blocked';
COMMENT ON COLUMN research_pipeline.experiment.data_version_ref IS '数据血缘: {stock_pool, factor_cache_date, hmm_snapshot_id, ...}';

-- ── Stage 执行记录 ──
CREATE TABLE research_pipeline.stage_execution (
    execution_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    experiment_id       UUID NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
    stage_name          TEXT NOT NULL,
    stage_index         INT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending'
        CONSTRAINT ck_rp_stage_status CHECK (
            status IN ('pending','running','passed','failed','skipped','blocked')
        ),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    duration_seconds    FLOAT,
    input_params        JSONB DEFAULT '{}'::jsonb,
    output_metrics      JSONB DEFAULT '{}'::jsonb,
    output_verdict      TEXT
        CONSTRAINT ck_rp_stage_verdict CHECK (
            output_verdict IS NULL OR output_verdict IN ('pass','fail','inconclusive')
        ),
    verdict_reason      TEXT,
    error_message       TEXT,
    error_traceback     TEXT,
    qe_task_id          TEXT,
    retry_count         INT NOT NULL DEFAULT 0,
    UNIQUE (experiment_id, stage_name)
);

CREATE INDEX idx_rp_stage_experiment ON research_pipeline.stage_execution(experiment_id);
CREATE INDEX idx_rp_stage_status ON research_pipeline.stage_execution(status);

COMMENT ON TABLE research_pipeline.stage_execution IS 'Stage 执行记录，每个实验每个 stage 最多一条（重试覆盖）';

-- ── 资产注册表 ──
CREATE TABLE research_pipeline.artifact (
    artifact_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    experiment_id       UUID NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
    artifact_type       TEXT NOT NULL,
    artifact_path       TEXT NOT NULL,
    artifact_sha256     TEXT,
    file_size_bytes     BIGINT,
    metadata            JSONB DEFAULT '{}'::jsonb,
    status              TEXT NOT NULL DEFAULT 'candidate'
        CONSTRAINT ck_rp_artifact_status CHECK (
            status IN ('candidate','validated','production','superseded','deleted')
        ),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    promoted_at         TIMESTAMPTZ,
    superseded_at       TIMESTAMPTZ,
    superseded_by       UUID
);

CREATE INDEX idx_rp_artifact_experiment ON research_pipeline.artifact(experiment_id);
CREATE INDEX idx_rp_artifact_type_status ON research_pipeline.artifact(artifact_type, status);
CREATE UNIQUE INDEX uq_rp_artifact_sha256 ON research_pipeline.artifact(artifact_sha256) WHERE artifact_sha256 IS NOT NULL;

COMMENT ON TABLE research_pipeline.artifact IS '实验资产注册表，content-addressable by SHA256';
COMMENT ON COLUMN research_pipeline.artifact.artifact_type IS '资产类型: hmm_risk_gate_v1, event_signal_v1, factor_ic_report, ...';
COMMENT ON COLUMN research_pipeline.artifact.status IS 'candidate→validated→production / superseded / deleted';

-- ── 实验对比记录 ──
CREATE TABLE research_pipeline.comparison (
    comparison_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    experiment_id       UUID NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
    stage_name          TEXT NOT NULL,
    baseline_label      TEXT NOT NULL,
    candidate_label     TEXT NOT NULL,
    baseline_metrics    JSONB NOT NULL,
    candidate_metrics   JSONB NOT NULL,
    delta_metrics       JSONB NOT NULL,
    verdict             TEXT NOT NULL
        CONSTRAINT ck_rp_comparison_verdict CHECK (
            verdict IN ('better','worse','neutral','inconclusive')
        ),
    verdict_details     TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_rp_comparison_experiment ON research_pipeline.comparison(experiment_id);

COMMENT ON TABLE research_pipeline.comparison IS '实验与基线的指标对比记录，归档到数仓';

-- ── 实验日志（SSE 流式输出用） ──
CREATE TABLE research_pipeline.execution_log (
    log_id              BIGSERIAL PRIMARY KEY,
    experiment_id       UUID NOT NULL,
    stage_name          TEXT,
    level               TEXT NOT NULL DEFAULT 'INFO',
    message             TEXT NOT NULL,
    metadata            JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_rp_log_experiment_time ON research_pipeline.execution_log(experiment_id, created_at);

-- ── Schema 版本 ──
CREATE TABLE IF NOT EXISTS research_pipeline.schema_version (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO research_pipeline.schema_version (version)
VALUES ('research_pipeline_v1_20260517')
ON CONFLICT DO NOTHING;
```

---

## 4. 核心模型定义

### 4.1 状态枚举与常量 (`constants.py`)

```python
from enum import Enum

SCHEMA_VERSION = "research_pipeline_v1_20260517"

class ExperimentStatus(str, Enum):
    DRAFT = "draft"
    RUNNING = "running"
    STAGE_FAILED = "stage_failed"
    VALIDATED = "validated"
    PROMOTED = "promoted"
    REJECTED = "rejected"
    BLOCKED = "blocked"

class StageStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    BLOCKED = "blocked"

class StageVerdict(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    INCONCLUSIVE = "inconclusive"

class ArtifactStatus(str, Enum):
    CANDIDATE = "candidate"
    VALIDATED = "validated"
    PRODUCTION = "production"
    SUPERSEDED = "superseded"
    DELETED = "deleted"

class PipelineType(str, Enum):
    HMM_RISK_GATE = "hmm_risk_gate"
    EVENT_SIGNAL = "event_signal"
    FACTOR_RESEARCH = "factor_research"          # Phase 2
    EXECUTION_ALGO = "execution_algo"            # Phase 2
    STRATEGY_OPTIMIZATION = "strategy_optimization"  # Phase 2
    MODEL_EVOLUTION = "model_evolution"           # Phase 2

ARCHIVE_EVENT_TYPE = "research.experiment.completed"
```

### 4.2 Pydantic 模型 (`models.py`)

```python
from __future__ import annotations
from datetime import datetime
from typing import Any
from uuid import UUID
from pydantic import BaseModel, model_validator

# ── Request Models ──

class CreateExperimentRequest(BaseModel):
    pipeline_type: str
    title: str
    hypothesis: str
    baseline_ref: str | None = None
    baseline_task_id: str | None = None
    acceptance_criteria: dict[str, Any] = {}
    config: dict[str, Any] = {}
    tags: list[str] = []

    @model_validator(mode="after")
    def validate_pipeline_type(self):
        valid = {e.value for e in PipelineType}
        if self.pipeline_type not in valid:
            raise ValueError(f"pipeline_type must be one of {valid}")
        return self

class RunStageRequest(BaseModel):
    stage_name: str
    override_params: dict[str, Any] | None = None

class PromoteRequest(BaseModel):
    issue_url: str
    confirm_token: str

class RejectRequest(BaseModel):
    reason: str

# ── Response Models ──

class ExperimentSummary(BaseModel):
    experiment_id: UUID
    pipeline_type: str
    title: str
    status: str
    current_stage: str | None = None
    created_at: datetime
    updated_at: datetime
    tags: list[str] = []

class StageExecutionDetail(BaseModel):
    execution_id: UUID
    stage_name: str
    stage_index: int
    status: str
    output_verdict: str | None = None
    output_metrics: dict[str, Any] = {}
    duration_seconds: float | None = None
    error_message: str | None = None
    qe_task_id: str | None = None

class ExperimentDetail(BaseModel):
    experiment_id: UUID
    pipeline_type: str
    title: str
    hypothesis: str
    status: str
    baseline_ref: str | None
    acceptance_criteria: dict[str, Any]
    config: dict[str, Any]
    stages: list[StageExecutionDetail] = []
    artifacts: list[ArtifactDetail] = []
    comparisons: list[ComparisonDetail] = []
    created_at: datetime
    updated_at: datetime
    created_by: str

class ArtifactDetail(BaseModel):
    artifact_id: UUID
    artifact_type: str
    artifact_path: str
    artifact_sha256: str | None
    status: str
    metadata: dict[str, Any] = {}
    created_at: datetime

class ComparisonDetail(BaseModel):
    comparison_id: UUID
    stage_name: str
    baseline_label: str
    candidate_label: str
    delta_metrics: dict[str, Any]
    verdict: str
    created_at: datetime
```

---

## 5. 领域流水线接口 (`pipelines/base.py`)

```python
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

@dataclass(frozen=True)
class StageDefinition:
    name: str
    description: str
    requires_human: bool = False
    timeout_seconds: int = 7200
    auto_retry: bool = False

@dataclass(frozen=True)
class StageContext:
    experiment_id: str
    experiment_config: dict[str, Any]
    stage_params: dict[str, Any]
    previous_results: dict[str, Any]
    artifact_dir: str

@dataclass(frozen=True)
class StageResult:
    verdict: str          # 'pass' / 'fail' / 'inconclusive'
    metrics: dict[str, Any] = field(default_factory=dict)
    artifacts: list[str] = field(default_factory=list)
    qe_task_id: str | None = None
    reason: str = ""
    error: str | None = None

    def __post_init__(self):
        if self.verdict not in ("pass", "fail", "inconclusive"):
            raise ValueError(f"Invalid verdict: {self.verdict}")

class ResearchPipeline(ABC):
    """所有领域流水线的基类。"""

    pipeline_type: str

    @abstractmethod
    def stages(self) -> list[StageDefinition]:
        """返回该流水线的 stage 定义序列。"""

    @abstractmethod
    async def execute_stage(
        self, stage_name: str, context: StageContext
    ) -> StageResult:
        """执行单个 stage，返回结果。"""

    @abstractmethod
    def default_acceptance_criteria(self) -> dict[str, Any]:
        """返回该流水线的默认验收标准。"""
```

---

## 6. HMM 流水线实现 (`pipelines/hmm_research.py`)

```python
class HMMRiskGateResearchPipeline(ResearchPipeline):
    pipeline_type = "hmm_risk_gate"

    def stages(self):
        return [
            StageDefinition("artifact_gen", "生成 risk gate artifact"),
            StageDefinition("offline_validation", "离线前向收益验证"),
            StageDefinition("portfolio_simulation", "组合模拟对比"),
            StageDefinition("qe_shadow", "QE 分钟线回测对比"),
            StageDefinition("promotion", "晋升审批", requires_human=True),
        ]

    def default_acceptance_criteria(self):
        return {
            "offline_validation": {
                "spread_5d_pct": {"operator": ">", "value": 0},
                "spread_10d_pct": {"operator": ">", "value": 0},
            },
            "portfolio_simulation": {
                "annual_return_delta_pct": {"operator": ">", "value": -0.5},
                "sharpe_delta": {"operator": ">", "value": -0.01},
            },
            "qe_shadow": {
                "annual_return_delta_pct": {"operator": ">", "value": -0.5},
                "max_drawdown_delta_pct": {"operator": ">", "value": -1.0},
            },
        }

    async def execute_stage(self, stage_name, context):
        if stage_name == "artifact_gen":
            return await self._gen_artifact(context)
        elif stage_name == "offline_validation":
            return await self._offline_validate(context)
        elif stage_name == "portfolio_simulation":
            return await self._portfolio_sim(context)
        elif stage_name == "qe_shadow":
            return await self._qe_shadow(context)
        raise ValueError(f"Unknown stage: {stage_name}")

    async def _gen_artifact(self, ctx: StageContext) -> StageResult:
        """调用 precompute_hmm_risk_gate.py 生成 artifact。"""
        # 1. 构建 stdin payload
        # 2. 执行 WSL subprocess
        # 3. 验证输出文件
        # 4. 注册 artifact 到 DB
        # 5. 返回 StageResult
        ...

    async def _offline_validate(self, ctx: StageContext) -> StageResult:
        """调用 validate_hmm_risk_gate.py 验证前向收益。"""
        ...

    async def _portfolio_sim(self, ctx: StageContext) -> StageResult:
        """运行组合模拟，对比 no-gate vs risk-gate。"""
        ...

    async def _qe_shadow(self, ctx: StageContext) -> StageResult:
        """提交 QE 4-arm 任务，等待完成，拉取结果。"""
        # 1. 通过 QE API 提交 custom-tasks
        # 2. 轮询任务状态直到完成
        # 3. 拉取各 arm 的 enhanced-metrics
        # 4. 计算 delta，判断 verdict
        # 5. 写入 comparison 表
        ...
```

---

## 7. 事件信号流水线 (`pipelines/event_signal_research.py`)

```python
class EventSignalResearchPipeline(ResearchPipeline):
    pipeline_type = "event_signal"

    def stages(self):
        return [
            StageDefinition("signal_compute", "计算事件信号因子"),
            StageDefinition("ic_validation", "IC/RankIC 独立验证"),
            StageDefinition("qe_overlay", "QE overlay 回测"),
            StageDefinition("promotion", "晋升审批", requires_human=True),
        ]

    def default_acceptance_criteria(self):
        return {
            "ic_validation": {
                "mean_ic_abs": {"operator": ">", "value": 0.02},
                "ic_ir": {"operator": ">", "value": 0.5},
                "positive_ic_ratio": {"operator": ">", "value": 0.55},
            },
            "qe_overlay": {
                "annual_return_delta_pct": {"operator": ">", "value": 0},
                "sharpe_delta": {"operator": ">", "value": 0},
            },
        }

    async def execute_stage(self, stage_name, context):
        if stage_name == "signal_compute":
            return await self._compute_signal(context)
        elif stage_name == "ic_validation":
            return await self._validate_ic(context)
        elif stage_name == "qe_overlay":
            return await self._qe_overlay(context)
        raise ValueError(f"Unknown stage: {stage_name}")
```

---

## 8. API 路由 (`routers/research_pipeline.py`)

```python
router = APIRouter(prefix="/research-pipeline", tags=["research-pipeline"])

# ── 实验 CRUD ──
POST   /experiments                          # 创建实验
GET    /experiments                          # 列表（?status=&pipeline_type=&limit=&offset=）
GET    /experiments/{experiment_id}           # 详情（含 stages/artifacts/comparisons）
DELETE /experiments/{experiment_id}           # 删除（仅 draft/rejected 状态）

# ── Stage 执行 ──
POST   /experiments/{experiment_id}/stages/{stage_name}/run    # 执行 stage
POST   /experiments/{experiment_id}/stages/{stage_name}/retry  # 重试失败 stage
GET    /experiments/{experiment_id}/stages/{stage_name}/logs   # SSE 日志流

# ── 资产管理 ──
GET    /experiments/{experiment_id}/artifacts                   # 列出资产
GET    /artifacts/{artifact_id}                                 # 资产详情
POST   /artifacts/{artifact_id}/promote                        # 晋升到生产

# ── 实验决策 ──
POST   /experiments/{experiment_id}/promote                    # 晋升（需 issue_url）
POST   /experiments/{experiment_id}/reject                     # 拒绝
POST   /experiments/{experiment_id}/create-issue               # 从发现创建 GitHub Issue

# ── 对比与历史 ──
GET    /experiments/{experiment_id}/comparisons                 # 对比记录
GET    /history                                                 # 历史归档查询

# ── 流水线元数据 ──
GET    /pipeline-types                                          # 可用流水线类型
GET    /pipeline-types/{type}/stages                            # 流水线 stage 定义
GET    /pipeline-types/{type}/acceptance-criteria               # 默认验收标准
```

---

## 9. MCP Server (`scripts/aistock_research_mcp_server.py`)

```python
from fastmcp import FastMCP

mcp = FastMCP("aistock-research-pipeline")

CONFIRM_CREATE = "RESEARCH_CREATE"
CONFIRM_RUN = "RESEARCH_RUN_STAGE"
CONFIRM_PROMOTE = "RESEARCH_PROMOTE"

@mcp.tool()
def research_create_experiment(
    pipeline_type: str,
    title: str,
    hypothesis: str,
    config: dict | None = None,
    baseline_task_id: str | None = None,
    confirm: str = "",
) -> dict:
    """创建新研究实验。"""

@mcp.tool()
def research_list_experiments(
    status: str | None = None,
    pipeline_type: str | None = None,
    limit: int = 20,
) -> dict:
    """列出研究实验。"""

@mcp.tool()
def research_get_experiment(experiment_id: str) -> dict:
    """获取实验详情（含 stages、artifacts、comparisons）。"""

@mcp.tool()
def research_run_stage(
    experiment_id: str,
    stage_name: str | None = None,
    override_params: dict | None = None,
    confirm: str = "",
) -> dict:
    """执行实验的下一个 stage（或指定 stage）。"""

@mcp.tool()
def research_get_stage_result(
    experiment_id: str,
    stage_name: str,
) -> dict:
    """获取 stage 执行结果和指标。"""

@mcp.tool()
def research_compare_with_baseline(experiment_id: str) -> dict:
    """获取实验与基线的完整对比。"""

@mcp.tool()
def research_promote(
    experiment_id: str,
    issue_url: str,
    confirm: str = "",
) -> dict:
    """晋升实验到生产（需关联 GitHub Issue）。"""

@mcp.tool()
def research_reject(experiment_id: str, reason: str) -> dict:
    """拒绝实验，记录原因。"""

@mcp.tool()
def research_create_issue_from_finding(
    experiment_id: str,
    title: str,
    description: str,
    severity: str = "medium",
) -> dict:
    """从实验发现创建 GitHub Issue（不在实验中修复）。"""

@mcp.tool()
def research_list_artifacts(
    experiment_id: str | None = None,
    artifact_type: str | None = None,
    status: str | None = None,
) -> dict:
    """列出资产。"""
```

---

## 10. 数仓归档 (`archive_handler.py`)

```python
class ResearchPipelineArchiveHandler(ArchiveHandler):
    event_type: ClassVar[str] = "research.experiment.completed"
    supported_schema_versions: ClassVar[tuple[str, ...]] = (SCHEMA_VERSION,)
    batch_size: ClassVar[int] = 10

    def handle(self, event, archive_job) -> ArchiveResult:
        """归档完成的实验到 qe_archive。"""
        # 写入 qe_archive.research_experiment_archive
        # 写入 qe_archive.research_comparison_archive
        # 关联 qe_archive.run（如果有 QE 任务）
        ...
```

---

## 11. 测试策略

### 11.1 单元测试

```
backend/tests/research_pipeline/
├── test_models.py                    # Pydantic 模型验证
├── test_experiment_registry.py       # CRUD + 状态机转换
├── test_stage_executor.py            # Stage 执行逻辑
├── test_validation_gate.py           # 验收判断逻辑
├── test_artifact_manager.py          # 资产存储/SHA256/版本
├── test_hmm_pipeline.py             # HMM 流水线各 stage
├── test_event_signal_pipeline.py    # 事件信号流水线各 stage
└── test_archive_handler.py          # 归档 handler
```

### 11.2 集成测试

```
backend/tests/research_pipeline/
├── test_api_integration.py           # API 端到端
├── test_qe_submission.py            # QE 任务提交+结果拉取
└── test_mcp_server.py               # MCP tool 调用
```

### 11.3 流水线自验证（Dogfooding）

**模块合入 main 的前提条件**：以下两个真实实验必须通过流水线完整执行。

#### 验证实验 1：HMM Risk Gate

```json
{
  "pipeline_type": "hmm_risk_gate",
  "title": "HMM Risk Gate v1 自验证",
  "hypothesis": "5天 transition gate + protect_top=30 优于 no-HMM",
  "config": {
    "model_config_id": "b99c907b-873a-4173-a4ee-5eab266f8c49",
    "trigger_duration_days": 5,
    "protect_top": 30,
    "baseline_task_id": "qe_20260502_131502_9b54"
  },
  "acceptance_criteria": {
    "offline_validation": {"spread_5d_pct": {">": 0}},
    "portfolio_simulation": {"annual_return_delta_pct": {">": -0.5}},
    "qe_shadow": {"annual_return_delta_pct": {">": -0.5}}
  }
}
```

**要求**：
- artifact_gen stage 自动生成 risk gate artifact ✓
- offline_validation stage 自动运行验证脚本，5D spread > 0 ✓
- portfolio_simulation stage 自动运行组合模拟 ✓
- qe_shadow stage 自动提交 QE 任务并等待结果 ✓
- 所有 stage 的 verdict 自动判断正确 ✓
- 实验状态正确流转到 validated ✓
- 资产正确注册到 artifact 表 ✓
- 对比记录正确写入 comparison 表 ✓

#### 验证实验 2：事件信号（财务困境早期预警）

```json
{
  "pipeline_type": "event_signal",
  "title": "财务困境早期预警信号自验证",
  "hypothesis": "ST/退市预警公告后行业内股票短期跑输",
  "config": {
    "signal_type": "financial_distress_early_warning",
    "lookback_days": 60,
    "decay_halflife": 10
  },
  "acceptance_criteria": {
    "ic_validation": {"mean_ic_abs": {">": 0.02}, "ic_ir": {">": 0.5}},
    "qe_overlay": {"annual_return_delta_pct": {">": 0}}
  }
}
```

**要求**：
- signal_compute stage 自动计算事件信号 ✓
- ic_validation stage 自动计算 IC/RankIC ✓
- qe_overlay stage 自动提交 QE overlay 回测 ✓
- 所有 stage verdict 自动判断 ✓
- 实验状态正确流转 ✓

---

## 12. 验收标准（合入 main 的前提条件）

### 12.1 功能验收

| # | 验收项 | 验证方式 |
|---|--------|---------|
| F1 | 创建实验 API 正常工作 | `POST /experiments` 返回 201 |
| F2 | 实验状态机正确流转 | draft→running→validated / stage_failed→rejected |
| F3 | Stage 执行引擎正确调度 | 按序执行，前一个 pass 才执行下一个 |
| F4 | 自动验收判断正确 | 给定 metrics + criteria，verdict 正确 |
| F5 | 资产注册和 SHA256 去重 | 同一文件不重复注册 |
| F6 | QE 任务自动提交和结果拉取 | qe_shadow stage 端到端 |
| F7 | 数仓归档事件正确发出 | outbox_event 有记录 |
| F8 | MCP server 所有 tools 可调用 | 每个 tool 有 smoke test |
| F9 | UI 看板显示实验列表和状态 | 前端页面可访问 |
| F10 | 发现 bug 时 create-issue 正确创建 GitHub Issue | API 调用 gh CLI |

### 12.2 流水线自验证（Dogfooding）

| # | 验收项 | 验证方式 |
|---|--------|---------|
| D1 | HMM Risk Gate 实验通过全部 stage | experiment.status == 'validated' |
| D2 | 事件信号实验通过 signal_compute + ic_validation | 至少前两个 stage pass |
| D3 | 两个实验的 artifact 正确注册 | artifact 表有记录 |
| D4 | 两个实验的 comparison 正确写入 | comparison 表有记录 |
| D5 | MCP server 可以驱动完整实验流程 | 通过 MCP tools 创建+执行+查询 |

### 12.3 非功能验收

| # | 验收项 | 验证方式 |
|---|--------|---------|
| N1 | Stage 执行超时保护 | 超过 timeout_seconds 自动标记 failed |
| N2 | 并发安全 | 同一实验同一 stage 不能并行执行 |
| N3 | Fail-fast | artifact 缺失/验证失败时立即报错 |
| N4 | 不修改现有模块行为 | QE/HMM/Selection Center 现有测试全部通过 |
| N5 | 代码不进 git 的资产不在 git 中 | `git status` 无 artifact 文件 |
| N6 | 实验中发现的 bug 不在实验分支修复 | 代码 review 检查 |

### 12.4 回归保护

| # | 验收项 | 验证方式 |
|---|--------|---------|
| R1 | 现有 QE 实验不受影响 | 提交一个标准 QE 任务，正常完成 |
| R2 | 现有 HMM 训练不受影响 | HMM training API 正常响应 |
| R3 | 现有 Selection Center 不受影响 | risk_policy evaluate 正常 |
| R4 | backend 8001 启动无报错 | lifespan 正常完成 |
| R5 | 前端构建无报错 | `npm run build` 通过 |

---

## 13. 实施计划

### Phase 1 Sprint（5-7 天）

| Day | 任务 | 产出 |
|-----|------|------|
| 1 | DB schema 创建 + models.py | SQL applied, Pydantic models |
| 2 | experiment_registry + stage_executor | 核心服务 |
| 3 | artifact_manager + validation_gate | 资产管理 + 验收判断 |
| 4 | HMM pipeline + event signal pipeline | 两个领域流水线 |
| 5 | API router + 单元测试 | 路由 + 测试覆盖 |
| 6 | MCP server + 集成测试 | MCP tools + E2E |
| 7 | 自验证（dogfooding）+ 修复 | D1-D5 全部通过 |

### 合入 Gate

```
所有验收标准通过
  → 创建 GitHub Issue (feat: Research Pipeline module)
  → 独立分支 PR
  → Code review
  → 合入 main
  → 重启 backend
  → UI 部署
```

---

## 14. 与现有系统的集成点

| 集成点 | 方式 | 修改现有代码？ |
|--------|------|--------------|
| QE 实验提交 | 调用 `POST /api/v1/quantevolver/evolution/custom-tasks` | 否 |
| QE 结果拉取 | 调用 `GET /api/v1/quantevolver/evolution/tasks/{id}` | 否 |
| HMM 训练 | 调用 `hmm_training_service` | 否 |
| 数仓归档 | 注册 handler 到 `qe_archive/worker.py` | 是（添加 handler 注册） |
| 后端启动 | 添加 scheduler 到 `main.py` lifespan | 是（添加 opt-in 启动） |
| 前端路由 | 新增 `/research-pipeline` 页面 | 否（新增文件） |
| MCP 配置 | 添加到 `.mcp.json` | 是（添加 server 条目） |
| GitHub Issue | 调用 `gh` CLI | 否 |

**修改现有文件清单**（需要 allowed_write_scope）：
- `backend/services/qe_archive/worker.py` — 注册 ResearchPipelineArchiveHandler
- `backend/main.py` — 添加 research_pipeline_scheduler 启动
- `.mcp.json` — 添加 aistock-research MCP server
