# Research Pipeline 模块架构分析

## 1. 定位与可行性

### 是否属于 AIstock 模块？

**是的，应该作为 AIstock 的一级模块**。理由：

1. AIstock 已有完整的实验基础设施（QE engine、archive worker、outbox event、MCP server），研究流水线是这些基础设施的上层编排
2. 研究产物（HMM artifact、事件信号、因子）最终都要进入 AIstock 的 Selection Center / Paper Trading / QE 体系
3. 前端 UI 需要统一入口跟踪所有研究进展
4. 与 qe_archive 数仓天然对接（实验结果归档）

### 模块边界

```
backend/services/research_pipeline/     ← 新一级模块
backend/routers/research_pipeline.py    ← API 路由
frontend/src/app/research-pipeline/     ← UI 页面
backend/db/init_research_pipeline.sql   ← Schema
scripts/aistock_research_mcp_server.py  ← MCP Server
```

与现有模块的关系：
- **调用** quantevolver（提交 QE 实验、获取结果）
- **调用** hmm_training_service（触发 HMM 训练/预计算）
- **调用** event_signal（事件信号研究）
- **写入** qe_archive（实验结果归档到数仓）
- **被调用** 由 MCP server 暴露给 Claude/Codex

---

## 2. 核心架构

### 2.1 分层设计

```
┌─────────────────────────────────────────────────────────────────┐
│  MCP Server (Claude/Codex 调度接口)                              │
│  tools: create_experiment, run_stage, get_status, promote        │
├─────────────────────────────────────────────────────────────────┤
│  Frontend UI (研究看板)                                          │
│  pages: 实验列表 / 实验详情 / Stage 进度 / 历史对比 / 资产管理   │
├─────────────────────────────────────────────────────────────────┤
│  API Router (/api/v1/research-pipeline)                          │
├─────────────────────────────────────────────────────────────────┤
│  Research Pipeline Service (编排层)                               │
│  ├─ ExperimentRegistry     实验注册/状态管理                      │
│  ├─ StageExecutor          Stage 执行引擎                        │
│  ├─ ArtifactManager        资产管理（生成/验证/存储/版本）         │
│  ├─ ValidationGate         自动验收判断                           │
│  └─ PromotionService       晋升到生产的审批流                     │
├─────────────────────────────────────────────────────────────────┤
│  Domain Pipelines (领域流水线，可插拔)                             │
│  ├─ HMMResearchPipeline         HMM 板块轮动研究                  │
│  ├─ EventSignalPipeline         事件独立信号研究                   │
│  ├─ FactorResearchPipeline      因子研究 (Phase 2)               │
│  ├─ ExecutionAlgoPipeline       执行算法研究 (Phase 2)            │
│  └─ StrategyOptPipeline         策略参数优化 (Phase 2)            │
├─────────────────────────────────────────────────────────────────┤
│  Infrastructure                                                   │
│  ├─ DB (research_pipeline schema)                                │
│  ├─ Artifact Storage (本地文件 + DB 元数据)                       │
│  ├─ Event Bus (outbox → qe_archive)                              │
│  └─ Scheduler (APScheduler, opt-in)                              │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 代码 vs 资产的严格分离

| 类别 | 存储位置 | 进入 Git？ | 示例 |
|------|---------|-----------|------|
| 流水线代码 | `backend/services/research_pipeline/` | ✅ | pipeline 定义、executor、validator |
| 领域脚本 | `scripts/` | ✅ | `precompute_hmm_risk_gate.py` |
| 策略模板 | `scripts/` | ✅ | `score_weighted_strategy_v2.py` |
| 实验配置 | DB `research_pipeline.experiment` | ❌ | hypothesis、params、acceptance_criteria |
| 模型 artifact | `backend/data/` (.gitignore) | ❌ | HMM models.json、risk gate artifact |
| 实验结果 | DB `research_pipeline.stage_result` | ❌ | 指标、对比表、验证结论 |
| 验证报告 | DB + `docs/analysis/` (可选导出) | 按需 | 最终确认有效的报告可进 git |
| 临时产物 | `.codex_tmp/` (.gitignore) | ❌ | 中间 CSV、调试 dump |

**规则**：
- `backend/data/` 已在 `.gitignore` — 所有模型/artifact 不进 git
- 实验元数据和结果进 DB（可查询、可归档到数仓）
- 只有**确认要合入生产的代码**走 issue 流程进 git
- 实验过程中发现的 bug → 创建独立 issue，不在实验分支修复

### 2.3 DB Schema 设计

```sql
CREATE SCHEMA IF NOT EXISTS research_pipeline;

-- 实验定义
CREATE TABLE research_pipeline.experiment (
    experiment_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_type    TEXT NOT NULL,  -- 'hmm_risk_gate', 'event_signal', 'factor', ...
    hypothesis       TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'draft',
        -- draft → running → validated → promoted / rejected
    baseline_ref     TEXT,           -- 基线实验 ID 或描述
    acceptance_criteria JSONB NOT NULL,
    config           JSONB NOT NULL, -- 领域特定配置
    created_at       TIMESTAMPTZ DEFAULT now(),
    updated_at       TIMESTAMPTZ DEFAULT now(),
    created_by       TEXT,           -- 'claude_code' / 'codex' / 'user'
    promoted_at      TIMESTAMPTZ,
    promoted_issue   TEXT            -- GitHub Issue URL (合入时关联)
);

-- Stage 执行记录
CREATE TABLE research_pipeline.stage_execution (
    execution_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    experiment_id    UUID REFERENCES research_pipeline.experiment(experiment_id),
    stage_name       TEXT NOT NULL,  -- 'artifact_gen', 'offline_validation', 'qe_shadow', 'promotion'
    stage_index      INT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
        -- pending → running → passed → failed → skipped
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    input_params     JSONB,
    output_metrics   JSONB,         -- 指标结果
    output_verdict   TEXT,          -- 'pass' / 'fail' / 'inconclusive'
    verdict_reason   TEXT,
    error_message    TEXT,
    qe_task_id       TEXT,          -- 关联的 QE 任务 ID（如有）
    UNIQUE (experiment_id, stage_name)
);

-- 资产注册表
CREATE TABLE research_pipeline.artifact (
    artifact_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    experiment_id    UUID REFERENCES research_pipeline.experiment(experiment_id),
    artifact_type    TEXT NOT NULL,  -- 'hmm_risk_gate_v1', 'event_signal_v1', ...
    artifact_path    TEXT NOT NULL,  -- 本地文件路径
    artifact_hash    TEXT,           -- SHA256
    metadata         JSONB,         -- 领域特定元数据
    status           TEXT DEFAULT 'candidate',
        -- candidate → validated → production → superseded
    created_at       TIMESTAMPTZ DEFAULT now(),
    promoted_at      TIMESTAMPTZ
);

-- 实验对比记录（归档到数仓）
CREATE TABLE research_pipeline.comparison (
    comparison_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    experiment_id    UUID REFERENCES research_pipeline.experiment(experiment_id),
    baseline_metrics JSONB NOT NULL,
    candidate_metrics JSONB NOT NULL,
    delta_metrics    JSONB NOT NULL,
    verdict          TEXT NOT NULL,  -- 'better' / 'worse' / 'neutral'
    created_at       TIMESTAMPTZ DEFAULT now()
);
```

### 2.4 领域流水线接口（可插拔）

```python
class ResearchPipeline(ABC):
    """所有领域流水线的基类。"""

    pipeline_type: str  # 'hmm_risk_gate', 'event_signal', ...

    @abstractmethod
    def stages(self) -> list[StageDefinition]:
        """定义该流水线的 stage 序列。"""

    @abstractmethod
    async def execute_stage(self, stage_name: str, context: StageContext) -> StageResult:
        """执行单个 stage。"""

    @abstractmethod
    def acceptance_check(self, metrics: dict) -> tuple[bool, str]:
        """自动验收判断。返回 (passed, reason)。"""
```

**HMM 流水线示例**：

```python
class HMMResearchPipeline(ResearchPipeline):
    pipeline_type = "hmm_risk_gate"

    def stages(self):
        return [
            StageDefinition("artifact_gen", "生成 risk gate artifact"),
            StageDefinition("offline_validation", "离线前向收益验证"),
            StageDefinition("qe_shadow", "QE 分钟线回测对比"),
            StageDefinition("promotion", "晋升审批", requires_human=True),
        ]

    async def execute_stage(self, stage_name, context):
        if stage_name == "artifact_gen":
            return await self._run_precompute(context)
        elif stage_name == "offline_validation":
            return await self._run_validation(context)
        elif stage_name == "qe_shadow":
            return await self._submit_qe_and_wait(context)
        ...
```

**事件信号流水线示例**：

```python
class EventSignalPipeline(ResearchPipeline):
    pipeline_type = "event_signal"

    def stages(self):
        return [
            StageDefinition("signal_compute", "计算事件信号"),
            StageDefinition("ic_validation", "IC/RankIC 验证"),
            StageDefinition("qe_overlay", "QE overlay 回测"),
            StageDefinition("promotion", "晋升审批", requires_human=True),
        ]
```

---

## 3. MCP Server 设计

### 工具列表

```python
# scripts/aistock_research_mcp_server.py
tools = [
    # 实验管理
    "research_create_experiment",     # 创建新实验
    "research_list_experiments",      # 列出实验（按状态/类型筛选）
    "research_get_experiment",        # 获取实验详情
    "research_run_stage",             # 执行下一个 stage
    "research_get_stage_result",      # 获取 stage 结果

    # 资产管理
    "research_list_artifacts",        # 列出资产
    "research_promote_artifact",      # 晋升资产到生产

    # 对比与决策
    "research_compare_with_baseline", # 与基线对比
    "research_auto_verdict",          # 自动验收判断

    # 流水线控制
    "research_retry_stage",           # 重试失败的 stage
    "research_reject_experiment",     # 标记实验为 rejected
    "research_create_issue",          # 从实验发现创建 GitHub Issue
]
```

### 调用示例（Claude/Codex）

```
Claude: 创建一个 HMM risk gate 实验，hypothesis 是 "5天 transition gate + protect_top=30 优于 no-HMM"
→ research_create_experiment(pipeline_type="hmm_risk_gate", hypothesis="...", config={...})

Claude: 执行 artifact_gen stage
→ research_run_stage(experiment_id="...", stage_name="artifact_gen")

Claude: 检查离线验证结果
→ research_get_stage_result(experiment_id="...", stage_name="offline_validation")
→ 返回: {verdict: "pass", metrics: {spread_5d: 0.478%, spread_10d: 0.056%}}

Claude: 提交 QE shadow
→ research_run_stage(experiment_id="...", stage_name="qe_shadow")
→ 自动提交 QE 任务，等待完成，拉取结果，自动判断
```

---

## 4. UI 设计

### 页面结构

```
/research-pipeline
├── /                       研究看板（所有实验概览，按状态分组）
├── /[id]                   实验详情（stages 进度条 + 指标卡片）
├── /[id]/stages/[stage]    Stage 详情（日志 + 输出 + 对比图表）
├── /[id]/artifacts         资产列表（文件路径 + hash + 状态）
├── /[id]/comparison        与基线对比（表格 + 图表）
├── /history                历史实验归档（可搜索/筛选）
└── /settings               流水线配置（acceptance criteria 模板）
```

### 看板视图

```
┌─────────────────────────────────────────────────────────────┐
│  Research Pipeline Dashboard                                 │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Running (2)                                                 │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ [HMM] Risk Gate v1 — Stage: qe_shadow (running)      │   │
│  │ ████████████░░░░ 75%  task: qe_20260517_...          │   │
│  ├──────────────────────────────────────────────────────┤   │
│  │ [Event] 财务困境早期预警 — Stage: ic_validation       │   │
│  │ ████████░░░░░░░░ 50%                                 │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  Validated (awaiting promotion) (1)                          │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ [HMM] Risk Gate v1 — All stages passed               │   │
│  │ Annual +2.22% | Sharpe +0.036 | [Promote] [Reject]   │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  Recent Rejected (3)                                         │
│  • [HMM] Dynamic PUP strict 0.10 — QE 不及基线             │
│  • [HMM] Sector-factor overlay — 未超越 old covfix          │
│  • [Event] 涨停板预测 — IC 不显著                           │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. 与 Issue 流程的关系

```
研究流水线内部：
  - 快速迭代，不需要 PR
  - 资产不进 git
  - 失败是正常结果
  - 由 MCP server 驱动（Claude/Codex 自动执行）

发现 bug/架构问题时：
  - research_create_issue() → 自动创建 GitHub Issue
  - 标记 experiment 为 blocked_by_issue
  - Issue 修复合入后，实验可以 retry

实验通过晋升时：
  - 创建 GitHub Issue（如 #32）
  - 关联 experiment_id
  - 走标准 issue 流程（分支 → PR → review → 合入）
  - 合入后更新 experiment.status = 'promoted'
```

---

## 6. 补充意见

### 6.1 版本化资产存储

建议资产存储不用纯文件路径，而是用 **content-addressable storage**：

```
backend/data/research_artifacts/
  {artifact_type}/{sha256_prefix}/{full_sha256}.json
```

好处：
- 同一 artifact 不会重复存储
- 可以追溯任何历史版本
- 晋升到生产时只需更新 DB 指针，不需要复制文件

### 6.2 实验血缘追踪

每个实验应记录：
- 依赖的模型版本（HMM snapshot_id）
- 依赖的数据版本（stock_pool、factor_cache 日期）
- 依赖的代码版本（git commit hash）
- 父实验（如果是 fork/改进）

这样当上游数据或模型更新时，可以自动标记受影响的实验为 `stale`。

### 6.3 并发控制

多个 Claude/Codex 窗口可能同时操作研究流水线。需要：
- DB 级别的乐观锁（`updated_at` 版本检查）
- Stage 执行的排他锁（同一实验同一时间只能有一个 stage running）
- QE 资源配额（远端节点并行度限制）

### 6.4 数仓归档策略

```
实验完成（无论通过/失败）→ emit outbox event
  → ResearchPipelineArchiveHandler
  → 写入 qe_archive.research_experiment（实验元数据）
  → 写入 qe_archive.research_comparison（对比结果）
  → 关联 qe_archive.run（如果有 QE 任务）
```

这样所有历史实验都可以在数仓中查询，支持跨实验分析。

### 6.5 Phase 2 扩展点

| 领域 | Pipeline Type | 特殊需求 |
|------|--------------|---------|
| 因子研究 | `factor_research` | 需要调用 develop-factor skill、IC 计算、相关性检查 |
| 执行算法 | `execution_algo` | 需要分钟线数据、RL 训练环境、PA 指标 |
| 策略参数优化 | `strategy_optimization` | 需要网格搜索/贝叶斯优化、多目标 Pareto |
| 模型演进 | `model_evolution` | 需要 RDAgent CoSTEER 集成、SOTA 判断 |

每个领域只需实现 `ResearchPipeline` 接口的 `stages()` 和 `execute_stage()`。

---

## 7. 实施路线

### Phase 1（2-3 天）— 最小可用

- DB schema 创建
- `ExperimentRegistry` + `ArtifactManager` 核心服务
- API router（CRUD + run_stage）
- HMM pipeline 接入（复用已有脚本）
- Event signal pipeline 接入

### Phase 2（3-5 天）— MCP + UI

- MCP server 实现
- 前端看板页面
- 数仓归档 handler
- 自动验收判断逻辑

### Phase 3（5-7 天）— 扩展领域

- Factor research pipeline
- Execution algo pipeline
- Strategy optimization pipeline
- 血缘追踪 + stale 检测
