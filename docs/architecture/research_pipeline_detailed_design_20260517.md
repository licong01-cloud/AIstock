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

---

## 20. MCP 统一管理分析

### 20.1 现有 MCP 现状

| Server | 文件 | 行数 | Tools | 职责 |
|--------|------|------|-------|------|
| `aistock-validation` | `scripts/aistock_mcp_server.py` | 1,675 | 19 | 验证框架 + Bug 生命周期 + GitHub Issue 同步 |
| `aistock-qe-experiment` | `scripts/aistock_qe_experiment_mcp_server.py` | 191 | 22 | QE 实验管理 + 模板 |
| `aistock-qe-archive` | `scripts/aistock_qe_archive_mcp_server.py` | 122 | 15 | 数仓查询 + 归档控制 |
| 共享工具 | `scripts/aistock_mcp_common.py` | 110 | — | LoopbackApiClient + sanitize + confirm |

**共计**: 3 server, 56 tools, ~2,100 行代码

**架构模式**: 所有 server 都是 thin HTTP wrapper，通过 `LoopbackApiClient` 调用 `127.0.0.1:8001` 后端 API。

### 20.2 是否需要合并到一个专用模块？

**结论：不合并 server 进程，但统一管理代码结构。**

#### 不合并进程的理由

1. **进程隔离** — validation server 有 48 个 private helper（bug 生命周期），与 QE 无关。合并会导致单个 server 过于臃肿
2. **按需加载** — Claude Code 的 `settings.local.json` 只启用了 `aistock-validation`，其他按需。合并后无法按需
3. **故障隔离** — 一个 server crash 不影响其他
4. **已有 BUG** — `BUG-044` 记录了 MCP server 启动问题，合并会增加排查难度

#### 统一管理代码结构的方案

将所有 MCP 相关代码从 `scripts/` 散落文件迁移到专用模块：

```
backend/mcp/                          ← 新模块（统一管理）
├── __init__.py
├── common.py                         ← 从 scripts/aistock_mcp_common.py 迁移
├── base.py                           ← 新增：共享基类和装饰器
├── servers/
│   ├── __init__.py
│   ├── validation.py                 ← 从 scripts/aistock_mcp_server.py 迁移
│   ├── qe_experiment.py              ← 从 scripts/aistock_qe_experiment_mcp_server.py 迁移
│   ├── qe_archive.py                 ← 从 scripts/aistock_qe_archive_mcp_server.py 迁移
│   └── research.py                   ← 新增：研究流水线 + 资产库
├── helpers/
│   ├── __init__.py
│   ├── bug_lifecycle.py              ← 从 validation server 提取的 48 个 helper
│   ├── github_sync.py                ← GitHub Issue 同步逻辑
│   └── confirmation.py               ← confirm token 管理
└── tests/
    ├── test_common.py
    ├── test_validation.py
    ├── test_qe_experiment.py
    ├── test_qe_archive.py
    └── test_research.py
```

**入口脚本保留在 `scripts/`**（因为 `.mcp.json` 引用它们）：

```python
# scripts/aistock_mcp_server.py（精简为入口）
from backend.mcp.servers.validation import mcp
if __name__ == "__main__":
    mcp.run()
```

### 20.3 共享基类设计 (`backend/mcp/base.py`)

```python
"""AIstock MCP Server 共享基础设施。"""
from __future__ import annotations
from typing import Any, Callable
from functools import wraps

class AIstockMCPBase:
    """所有 AIstock MCP server 的共享能力。"""

    @staticmethod
    def confirmed_tool(confirmation_string: str):
        """装饰器：要求 confirm 参数匹配才执行。"""
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, confirm: str = "", **kwargs) -> dict[str, Any]:
                if confirm != confirmation_string:
                    return {
                        "error": f"Confirmation required. Pass confirm='{confirmation_string}' to proceed.",
                        "required_confirm": confirmation_string,
                    }
                return func(*args, **kwargs)
            return wrapper
        return decorator

    @staticmethod
    def validate_params(**validators: Callable):
        """装饰器：参数验证。"""
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(**kwargs) -> dict[str, Any]:
                for param_name, validator in validators.items():
                    if param_name in kwargs:
                        kwargs[param_name] = validator(kwargs[param_name], param_name)
                return func(**kwargs)
            return wrapper
        return decorator
```

### 20.4 统一 API Client (`backend/mcp/common.py`)

```python
"""统一 HTTP Client，支持两种 envelope 模式。"""

class AIstockApiClient:
    """统一 API Client，替代 LoopbackApiClient + ValidationCenterClient。"""

    def __init__(self, base_url: str, unwrap_data: bool = False):
        """
        Args:
            base_url: 后端 API base URL
            unwrap_data: True = 自动解包 {"data": ...} envelope（validation 模式）
                         False = 返回原始 response（qe/archive 模式）
        """
        self._base_url = base_url
        self._unwrap = unwrap_data

    def get(self, path: str, params: dict | None = None) -> dict:
        ...

    def post(self, path: str, json_body: dict | None = None, params: dict | None = None) -> dict:
        ...

    def delete(self, path: str) -> dict:
        ...
```

### 20.5 迁移计划

| 阶段 | 动作 | 影响 |
|------|------|------|
| Phase 1 | 创建 `backend/mcp/` 模块结构 | 无影响（新增文件） |
| Phase 1 | 新增 `research.py` server | 无影响（新增） |
| Phase 2 | 迁移 common.py → `backend/mcp/common.py` | 更新 import |
| Phase 2 | 迁移 3 个现有 server 到 `backend/mcp/servers/` | 更新 scripts/ 入口 |
| Phase 2 | 提取 bug_lifecycle helpers | 重构 validation server |
| Phase 3 | 统一 API Client | 替换 2 种 client |

**Phase 1 与研究流水线同步实施，Phase 2/3 作为后续治理 issue。**

### 20.6 `.mcp.json` 更新（Phase 1 后）

```json
{
  "_comment": "AIstock MCP servers. 所有 server 通过 loopback 调用 backend 8001。",
  "mcpServers": {
    "aistock-validation": {
      "command": "python",
      "args": ["scripts/aistock_mcp_server.py"],
      "env": {"AISTOCK_VALIDATION_BASE_URL": "http://127.0.0.1:8001/api/v1/validation"}
    },
    "aistock-qe-experiment": {
      "command": "python",
      "args": ["scripts/aistock_qe_experiment_mcp_server.py"],
      "env": {"AISTOCK_QE_EXPERIMENT_BASE_URL": "http://127.0.0.1:8001/api/v1"}
    },
    "aistock-qe-archive": {
      "command": "python",
      "args": ["scripts/aistock_qe_archive_mcp_server.py"],
      "env": {"AISTOCK_QE_ARCHIVE_BASE_URL": "http://127.0.0.1:8001/api/v1/qe-archive"}
    },
    "aistock-research": {
      "command": "python",
      "args": ["scripts/aistock_research_mcp_server.py"],
      "env": {"AISTOCK_RESEARCH_BASE_URL": "http://127.0.0.1:8001/api/v1/research-pipeline"}
    }
  }
}
```

### 20.7 Tool 总量控制

| 阶段 | Servers | Tools | Token 开销 | 评估 |
|------|---------|-------|-----------|------|
| 当前 | 3 | 56 | ~8,400 | 基线 |
| Phase 1（+research） | 4 | 74 | ~11,100 | +32%，可接受 |
| Phase 2（+因子/模型/执行） | 4 | 74（合并在 research 中） | ~11,100 | 不增加 |
| 上限建议 | 4-5 | <100 | <15,000 | 占 context 7.5% |

**硬性约束**：总 tools 不超过 100，否则 tool schema 占用过多 context。如果未来需要更多 tools，应通过 `action` 参数合并同类操作。

### 15.1 设计动机

研究流水线产出的因子、模型、执行算法需要统一入库管理。当前这些资产分散在：
- 因子：`factors/` 目录 + DB `factor_metrics` 表
- 模型：`backend/data/hmm_models/` + DB `model_train_*` 表
- 执行算法：`qe_strategies/` + DB `execution_algorithms` 表

需要一个统一的 MCP 接口，让 Claude/Codex 在研究完成后自动入库、计算指标、触发验证。

### 15.2 MCP Server 合并策略

**不新增独立 MCP server，而是扩展现有 server + 新增 1 个研究 MCP。**

理由见 §16 性能分析。最终 MCP 布局：

| MCP Server | 职责 | Tools 数量 |
|------------|------|-----------|
| `aistock-validation` | 验证框架 | 19（现有） |
| `aistock-qe-experiment` | QE 实验管理 | 22（现有）+ 3（资产入库） |
| `aistock-qe-archive` | 数仓查询 | 15（现有） |
| `aistock-research` | **研究流水线 + 资产库**（新增） | ~18 |

**关键决策**：因子库/模型库/执行算法库的 MCP tools 合并到 `aistock-research` 中，而不是各自独立 server。原因：
1. 这些操作都是研究流水线的下游动作（研究通过 → 入库）
2. 减少 MCP server 数量（性能考虑）
3. 统一 confirm token 和审计机制

### 15.3 资产库 Tools（在 `aistock-research` MCP 中）

```python
# ── 因子库 ──
@mcp.tool()
def factor_library_register(
    factor_name: str,
    source_code: str,
    source_type: str,           # 'manual' / 'rdagent_task_sync' / 'research_pipeline'
    experiment_id: str | None,  # 关联研究实验
    confirm: str = "",
) -> dict:
    """注册新因子到因子库（含编译验证）。"""

@mcp.tool()
def factor_library_compute_metrics(
    factor_name: str,
    metrics: list[str] | None = None,  # ['ic', 'rank_ic', 'ic_decay', 'turnover']
) -> dict:
    """计算因子独立指标（IC/RankIC/衰减/换手）。"""

@mcp.tool()
def factor_library_query(
    status: str | None = None,
    source_type: str | None = None,
    min_ic: float | None = None,
    limit: int = 50,
) -> dict:
    """查询因子库（按状态/来源/IC 筛选）。"""

@mcp.tool()
def factor_library_deprecate(
    factor_name: str,
    reason: str,
    confirm: str = "",
) -> dict:
    """下架因子（标记为 deprecated，不删除）。"""

# ── 模型库 ──
@mcp.tool()
def model_library_register(
    model_type: str,            # 'hmm_risk_gate' / 'lgbm' / 'nn'
    artifact_path: str,
    config: dict,
    experiment_id: str | None,
    confirm: str = "",
) -> dict:
    """注册模型 artifact 到模型库。"""

@mcp.tool()
def model_library_query(
    model_type: str | None = None,
    status: str | None = None,
    limit: int = 20,
) -> dict:
    """查询模型库。"""

@mcp.tool()
def model_library_promote(
    model_id: str,
    target: str,  # 'qe_selectable' / 'selection_center' / 'paper_trading'
    confirm: str = "",
) -> dict:
    """晋升模型到指定消费方。"""

# ── 执行算法库 ──
@mcp.tool()
def execution_algo_register(
    algo_code: str,
    algo_name: str,
    source_code: str,
    version: str,
    experiment_id: str | None,
    confirm: str = "",
) -> dict:
    """注册执行算法到算法库。"""

@mcp.tool()
def execution_algo_analyze(
    algo_code: str,
    analysis_type: str = "pa_attribution",  # 'pa_attribution' / 'slippage' / 'fill_rate'
) -> dict:
    """分析执行算法性能指标。"""

@mcp.tool()
def execution_algo_query(
    status: str | None = None,
    limit: int = 20,
) -> dict:
    """查询执行算法库。"""
```

### 15.4 入库流程（与研究流水线集成）

```
研究流水线 promotion stage:
  1. 实验 validated
  2. 用户确认晋升（提供 issue_url）
  3. 自动调用对应的 library_register tool:
     - HMM 实验 → model_library_register(model_type="hmm_risk_gate", ...)
     - 因子实验 → factor_library_register(factor_name=..., source_type="research_pipeline")
     - 执行算法实验 → execution_algo_register(algo_code=..., ...)
  4. 注册成功后更新 experiment.status = 'promoted'
  5. 触发数仓归档
```

---

## 16. MCP Server 数量与性能分析

### 16.1 Token 消耗机制

MCP server 对 Claude/Codex 的 token 影响来自两个方面：

**A. Tool Schema 注入（固定开销）**

每个 MCP server 连接时，其所有 tool 的 schema（名称 + 参数 + 描述）会注入到 system prompt 中。

当前 3 个 server 的 tool 数量：
- aistock-validation: 19 tools
- aistock-qe-experiment: 22 tools
- aistock-qe-archive: 15 tools
- **合计: 56 tools**

每个 tool schema 约 100-200 tokens（名称 + 参数类型 + 描述），56 tools ≈ **8,000-11,000 tokens 固定开销**。

**B. Tool 调用（按需开销）**

实际调用时的 input/output tokens 与 server 数量无关，只与调用次数和返回数据量有关。

### 16.2 新增 MCP 的影响评估

| 方案 | 新增 Tools | 总 Tools | 额外 Token 开销 | 评估 |
|------|-----------|---------|----------------|------|
| 方案 A: 每个库独立 server（4 个新 server） | ~40 | ~96 | +6,000-8,000 | ❌ 过多 |
| **方案 B: 合并为 1 个 research server** | ~18 | ~74 | +2,500-3,500 | ✅ 可接受 |
| 方案 C: 全部合并到 1 个 mega server | 0 新 server | ~74 | 同 B | ⚠️ 维护困难 |

**推荐方案 B**：新增 1 个 `aistock-research` MCP server，包含研究流水线 + 资产库 tools。

### 16.3 性能优化措施

1. **Tool 描述精简** — 每个 tool 描述控制在 1 行（<50 字），参数用 type hint 自解释
2. **按需连接** — Claude Code 只在需要时连接 MCP server（不是所有 session 都加载）
3. **Lazy tool loading** — 资产库 tools 只在 research pipeline 相关对话中激活
4. **合并同类 tools** — 用 `action` 参数区分操作，减少 tool 总数：
   ```python
   # 不推荐：3 个 tools
   factor_library_register()
   factor_library_query()
   factor_library_deprecate()
   
   # 推荐：1 个 tool + action 参数
   factor_library(action="register|query|deprecate", ...)
   ```

### 16.4 最终 MCP 布局（4 个 server，~74 tools）

```json
{
  "mcpServers": {
    "aistock-validation": {
      "command": "python",
      "args": ["scripts/aistock_mcp_server.py"],
      "tools": 19
    },
    "aistock-qe-experiment": {
      "command": "python",
      "args": ["scripts/aistock_qe_experiment_mcp_server.py"],
      "tools": 22
    },
    "aistock-qe-archive": {
      "command": "python",
      "args": ["scripts/aistock_qe_archive_mcp_server.py"],
      "tools": 15
    },
    "aistock-research": {
      "command": "python",
      "args": ["scripts/aistock_research_mcp_server.py"],
      "tools": 18
    }
  }
}
```

Token 预算：~74 tools × 150 tokens/tool ≈ **11,100 tokens 固定开销**（约占 200K context 的 5.5%）。可接受。

---

## 17. 分支策略分析

### 17.1 HMM 研究的历史分支使用情况

```
main 上的 HMM commits (2026-04 以来): 10 个
独立分支上的 HMM commits: 12 个

独立分支列表（8 个）：
  codex/hmm-evo-baseline-20260506
  codex/hmm-qe-autoretry-20260509
  codex/hmm-rd-20260511
  codex/hmm-sector-regime-20260509
  codex/qe-hmm-hotfix-handoff-20260508
  codex/qe-hmm-hotfix-integration-20260508
  codex/qe-hmm-hotfix-validation-20260508
  feature/hmm-risk-gate-20260517
```

**问题**：
- 部分 HMM 研究直接 commit 到 main（10 个），没有走分支
- 分支命名不统一（`codex/hmm-*` vs `feature/hmm-*`）
- 多个分支并行但没有统一的实验追踪

### 17.2 事件信号的历史分支使用情况

```
main 上的 event signal commits: 2 个
独立分支上的 event signal commits: 4 个

独立分支列表（6 个）：
  codex/event-signal-policy-20260507
  codex/event-signal-st-llm-design-20260506
  codex/financial-distress-multiloop-20260508
  codex/financial-distress-qe-overlay-20260508
  codex/financial-distress-rerank-20260508
  codex/financial-distress-sizebucket-20260508
```

**问题**：
- 同一研究方向拆成了 6 个分支（每个 QE 变体一个分支）
- 分支之间没有关联关系
- 无法从分支名看出实验结果（通过/失败）

### 17.3 未来分支策略

**规则：每个研究方向使用 1 个长期分支，实验迭代在分支内进行。**

```
命名规范：
  research/{pipeline_type}/{experiment_short_name}-{start_date}

示例：
  research/hmm/risk-gate-transition-20260517
  research/event-signal/financial-distress-20260507
  research/factor/momentum-decay-20260520
  research/execution/v26-adaptive-20260601
```

**生命周期**：

```
创建分支（从 main）
  → 实验代码开发（脚本、配置）
  → 流水线执行（artifact 生成、验证、QE）
  → 实验通过 → 创建 PR 合入 main
  → 实验失败 → 分支保留（归档），不合入
  → 分支上发现 bug → 创建独立 issue 分支修复，合入 main 后 rebase 研究分支
```

**与研究流水线的关系**：

| 操作 | 在研究分支上？ | 说明 |
|------|--------------|------|
| 编写预计算脚本 | ✅ | 研究代码 |
| 修改策略模板 | ✅ | 研究代码 |
| 生成 artifact | ✅（文件不进 git） | 资产不进 git |
| 运行验证脚本 | ✅ | 在分支上执行 |
| 提交 QE 实验 | ✅（通过 API） | 不需要合入 main |
| 修改 ConfigComposer | ⚠️ 需要合入 main | 走 issue 流程 |
| 修改 backend 核心代码 | ❌ 独立 issue 分支 | 不在研究分支修复 |

**关键区分**：
- **研究代码**（脚本、配置、领域逻辑）→ 在研究分支上开发
- **基础设施代码**（ConfigComposer、MCP server、DB schema）→ 独立 issue 分支，先合入 main
- **资产**（模型文件、artifact）→ 不进 git，通过 DB 管理

### 17.4 是否需要统一到一个分支？

**不需要。** 不同研究方向应保持独立分支，原因：

1. **隔离性** — HMM 研究失败不影响事件信号研究
2. **可追溯** — 每个分支对应一个实验，清晰的因果关系
3. **并行性** — 多个 Claude/Codex 窗口可以同时在不同分支工作
4. **清理简单** — 失败的实验分支可以直接归档，不污染其他分支

但需要**统一管理**：
- 研究流水线 DB 记录所有实验（无论哪个分支）
- UI 看板展示所有活跃研究分支
- 分支命名规范强制执行

### 17.5 分支与流水线的绑定

```sql
-- experiment 表中记录分支信息
ALTER TABLE research_pipeline.experiment ADD COLUMN
    git_branch TEXT;  -- 'research/hmm/risk-gate-transition-20260517'

-- 创建实验时自动记录当前分支
-- 流水线 UI 可以按分支筛选实验
```

---

## 18. MCP 互调架构（补充 §9）

### 18.1 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│  Claude / Codex                                                  │
│                                                                  │
│  可以同时使用多个 MCP server:                                     │
│  ├── aistock-research: 驱动研究流水线                             │
│  ├── aistock-qe-experiment: 直接操作 QE（细粒度检查）             │
│  ├── aistock-qe-archive: 查询数仓历史                            │
│  └── aistock-validation: 触发验证                                │
└─────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ aistock-research │  │ aistock-qe-exp  │  │ aistock-archive │
│ MCP Server       │  │ MCP Server      │  │ MCP Server      │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         │  LoopbackApiClient │  LoopbackApiClient │  LoopbackApiClient
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│  AIstock Backend (FastAPI, port 8001)                             │
│                                                                  │
│  /api/v1/research-pipeline/*    ← research MCP 调用              │
│  /api/v1/quantevolver/*         ← research MCP 内部也调用（编排） │
│  /api/v1/qe-archive/*           ← research MCP 内部也调用（归档） │
│  /api/v1/validation/*           ← research MCP 内部也调用（验证） │
└─────────────────────────────────────────────────────────────────┘
```

### 18.2 研究 MCP 内部编排（不直接调用其他 MCP，而是调用同一个 HTTP API）

```python
class ResearchStageExecutor:
    """通过 LoopbackApiClient 调用后端 API。
    
    与 QE MCP server 使用完全相同的 HTTP 端点，
    保证行为一致性和审计完整性。
    """
    
    def __init__(self):
        self._api = LoopbackApiClient(
            base_url=os.environ.get("AISTOCK_RESEARCH_BASE_URL", "http://127.0.0.1:8001/api/v1")
        )
    
    # ── QE 操作（与 qe_custom_evo_* MCP tools 相同端点） ──
    
    async def create_qe_task(self, payload: dict) -> str:
        resp = self._api.post("/quantevolver/evolution/custom-tasks", payload)
        return resp["data"]["task_id"]
    
    async def get_qe_task_status(self, task_id: str) -> dict:
        return self._api.get(f"/quantevolver/evolution/tasks/{task_id}")
    
    async def get_qe_loop_metrics(self, task_id: str, loop_index: int) -> dict:
        return self._api.get(
            f"/quantevolver/evolution/tasks/{task_id}/loops/{loop_index}/enhanced-metrics"
        )
    
    # ── 数仓操作（与 qe_archive_* MCP tools 相同端点） ──
    
    async def query_archive_runs(self, filters: dict) -> dict:
        return self._api.get("/qe-archive/runs", params=filters)
    
    async def trigger_archive(self, batch_size: int = 10) -> dict:
        return self._api.post("/qe-archive/worker/run", {"batch_size": batch_size})
    
    # ── 资产库操作 ──
    
    async def register_factor(self, factor_data: dict) -> dict:
        return self._api.post("/quantevolver/factors/manual", factor_data)
    
    async def register_model(self, model_data: dict) -> dict:
        return self._api.post("/quantevolver/model-registry/specs", model_data)
```

### 18.3 HMM 验证场景的完整 MCP 协作

```
Claude 对话：

1. Claude 调用 research MCP:
   research_run_stage(experiment_id="xxx", stage_name="qe_shadow")
   
   → 研究流水线内部：
     a. 构建 QE payload（4-arm: no-HMM / old covfix / risk gate / risk gate+P30）
     b. POST /quantevolver/evolution/custom-tasks  ← 与 qe_custom_evo_create 相同
     c. 轮询 GET /quantevolver/evolution/tasks/{id}  ← 与 qe_custom_evo_get_task 相同
     d. 完成后 GET .../loops/{i}/enhanced-metrics  ← 与 qe_experiment_get_enhanced_metrics 相同
     e. 计算 delta，写入 comparison 表
     f. POST /qe-archive/trigger  ← 与 qe_archive_trigger_worker 相同
   → 返回 StageResult

2. Claude 想看更多细节，直接调用 QE MCP:
   qe_custom_evo_get_task("qe_20260517_...")
   qe_experiment_get_enhanced_metrics("qe_20260517_..._Loop4")
   
3. Claude 想查历史对比，调用 Archive MCP:
   qe_archive_query_runs(pipeline_type="hmm_risk_gate")
   
4. Claude 确认晋升，调用 research MCP:
   research_promote(experiment_id="xxx", issue_url="https://github.com/.../issues/32")
   → 内部自动调用 model_library_register
```

---

## 19. 更新后的验收标准（补充）

### 19.1 MCP 互调验收

| # | 验收项 | 验证方式 |
|---|--------|---------|
| M1 | research MCP 的 qe_shadow stage 通过 HTTP API 成功提交 QE 任务 | 任务 ID 返回且状态为 running |
| M2 | research MCP 的 qe_shadow stage 能轮询并获取 QE 结果 | enhanced-metrics 正确返回 |
| M3 | research MCP 的 promotion 能自动调用资产库注册 | artifact 表 + 对应库表有记录 |
| M4 | Claude 可以混合使用 research + QE + archive MCP | 同一对话中三个 server 都可调用 |
| M5 | 资产库 tools 的 confirm token 机制正常 | 无 token 时拒绝执行 |

### 19.2 分支策略验收

| # | 验收项 | 验证方式 |
|---|--------|---------|
| B1 | 研究流水线模块在独立分支开发 | `research/pipeline/core-20260517` |
| B2 | 实验记录中包含 git_branch 字段 | DB 查询确认 |
| B3 | 基础设施修改（ConfigComposer 等）走独立 issue | GitHub Issue 存在 |
| B4 | 研究分支不包含 artifact 文件 | `git status` 无 .json/.pkl 模型文件 |
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
