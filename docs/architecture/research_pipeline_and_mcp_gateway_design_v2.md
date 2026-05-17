# AIstock Research Pipeline + MCP 统一网关 详细设计

> 版本：v2.0（方案 C 定稿）
> 日期：2026-05-17
> 状态：架构定稿，待实施
> 范围：Research Pipeline 模块 + MCP 统一网关 + 现有 MCP 迁移

---

## 1. 架构总览

### 1.1 核心决策

| 决策 | 选择 | 理由 |
|------|------|------|
| MCP 平台 | **统一平台骨架 + 可选统一 gateway + 旧入口并存过渡** | 新模块原生使用平台，旧模块稳定后再迁移 |
| 首期入口 | **Research MCP 独立暴露（`--modules=research`）** | 不影响现有 3 个稳定 MCP |
| 模块加载 | **动态按需（--profile/--modules）** | Token 只含已加载模块 |
| 代码组织 | **`backend/mcp/` 统一模块** | Research 从第一天就在平台上，避免二次迁移 |
| 研究流水线 | **研究编排层，不是执行引擎或资产库** | 通过现有 service/API 执行，引用现有资产库 |
| 资产引用 | **引用现有 factor_catalog / model_registry / qe_archive** | 不新建平行资产库 |
| QE 验证 | **通过 qe_templates + 标准 QE 后端执行** | 不自建 QE 执行链路 |
| 旧 MCP 迁移 | **首期不迁移，等 Research 稳定后逐个迁移** | 不把新平台风险传递给稳定路径 |
| Paper Trading | **不纳入首期** | 避免跨入 Paper v2 / trading_core 敏感边界 |

### 1.2 系统架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│  Claude Code / Codex App                                             │
│  (通过 .mcp.json 连接统一网关)                                        │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ stdio (FastMCP)
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│  AIstock MCP Gateway (单进程)                                        │
│  python -m backend.mcp.gateway --profile=research                    │
│                                                                      │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
│  │validation│ │qe_experi-│ │qe_archive│ │ research │ │  paper   │ │
│  │  module  │ │ment mod. │ │  module  │ │  module  │ │  module  │ │
│  │ 19 tools │ │ 22 tools │ │ 15 tools │ │ 18 tools │ │ 12 tools │ │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ │
│       │             │            │             │             │       │
│  ┌────┴─────────────┴────────────┴─────────────┴─────────────┴────┐ │
│  │  ModuleRegistry (共享 client / confirm / sanitize / error)      │ │
│  └─────────────────────────────┬───────────────────────────────────┘ │
└────────────────────────────────┼────────────────────────────────────┘
                                 │ HTTP (loopback 127.0.0.1:8001)
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  AIstock Backend (FastAPI)                                            │
│  /api/v1/validation/*                                                │
│  /api/v1/quantevolver/*                                              │
│  /api/v1/qe-archive/*                                                │
│  /api/v1/research-pipeline/*    ← 新增                               │
│  /api/v1/paper-trading-v2/*                                          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. 文件结构

```
backend/mcp/                              ← MCP 统一网关模块
├── __init__.py
├── gateway.py                            ← 入口：动态加载 + FastMCP 初始化
├── registry.py                           ← ModuleRegistry：共享基础设施
├── common.py                             ← AIstockApiClient（统一 HTTP client）
├── modules/
│   ├── __init__.py
│   ├── validation.py                     ← 迁移自 scripts/aistock_mcp_server.py
│   ├── validation_helpers.py             ← 迁移：bug lifecycle + github sync
│   ├── qe_experiment.py                  ← 迁移自 scripts/aistock_qe_experiment_mcp_server.py
│   ├── qe_archive.py                     ← 迁移自 scripts/aistock_qe_archive_mcp_server.py
│   ├── research.py                       ← 新增：研究流水线（首期唯一新模块）
│   └── paper_trading.py                  ← 后续独立 issue，不纳入首期
├── tests/
│   ├── __init__.py
│   ├── conftest.py                       ← 共享 fixtures（mock registry/client）
│   ├── test_gateway.py
│   ├── test_registry.py
│   ├── test_validation_module.py
│   ├── test_qe_experiment_module.py
│   ├── test_qe_archive_module.py
│   └── test_research_module.py
└── profiles.py                           ← Profile 定义

backend/services/research_pipeline/       ← 研究流水线业务服务
├── __init__.py
├── models.py                             ← Pydantic 模型
├── experiment_registry.py                ← 实验 CRUD + 状态机
├── stage_executor.py                     ← Stage 执行引擎
├── artifact_manager.py                   ← 资产管理
├── validation_gate.py                    ← 自动验收判断
├── scheduler.py                          ← APScheduler 后台调度
├── archive_handler.py                    ← 数仓归档 handler
├── pipelines/
│   ├── __init__.py
│   ├── base.py                           ← ResearchPipeline ABC
│   ├── hmm_research.py                   ← HMM 研究流水线
│   └── event_signal_research.py          ← 事件信号研究流水线
└── constants.py

backend/routers/research_pipeline.py      ← API 路由
backend/db/init_research_pipeline.sql     ← DB Schema

scripts/
├── aistock_mcp_gateway.py                ← 薄入口（兼容 .mcp.json）
├── aistock_mcp_server.py                 ← 保留（Phase 2 前的 fallback）
├── aistock_qe_experiment_mcp_server.py   ← 保留（Phase 2 前的 fallback）
└── aistock_qe_archive_mcp_server.py      ← 保留（Phase 2 前的 fallback）
```

---

## 3. MCP 网关核心实现

### 3.1 Gateway (`backend/mcp/gateway.py`)

```python
"""AIstock 统一 MCP Gateway。

通过 --profile 或 --modules 参数动态加载功能模块。
单进程运行，共享 HTTP client、confirm 机制、sanitize 逻辑。

Usage:
  python -m backend.mcp.gateway --profile=research
  python -m backend.mcp.gateway --modules=validation,qe_experiment,research
  python -m backend.mcp.gateway --profile=full
"""
from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from backend.mcp.registry import ModuleRegistry
from backend.mcp.profiles import PROFILES, resolve_modules

logger = logging.getLogger(__name__)


def create_gateway(modules: list[str]) -> FastMCP:
    """创建 MCP gateway 并注册指定模块的 tools。"""
    mcp = FastMCP("aistock")
    base_url = os.environ.get("AISTOCK_BASE_URL", "http://127.0.0.1:8001/api/v1")
    registry = ModuleRegistry(mcp=mcp, base_url=base_url)

    for module_name in modules:
        try:
            mod = importlib.import_module(f"backend.mcp.modules.{module_name}")
            mod.register(registry)
            logger.info("Loaded MCP module: %s (%d tools)", module_name, registry.tool_count(module_name))
        except Exception as exc:
            logger.error("Failed to load MCP module %s: %s", module_name, exc)
            raise

    logger.info("Gateway ready: %d modules, %d total tools", len(modules), registry.total_tool_count())
    return mcp


def main():
    parser = argparse.ArgumentParser(description="AIstock MCP Gateway")
    parser.add_argument("--profile", default=None, help="预定义 profile: minimal/research/operations/full")
    parser.add_argument("--modules", default=None, help="逗号分隔的模块列表")
    args = parser.parse_args()

    modules = resolve_modules(profile=args.profile, modules_csv=args.modules)
    mcp = create_gateway(modules)
    mcp.run()


if __name__ == "__main__":
    main()
```

### 3.2 Profiles (`backend/mcp/profiles.py`)

```python
"""MCP 模块加载 Profile 定义。"""

PROFILES: dict[str, list[str]] = {
    "minimal": [
        "validation",
    ],
    "research": [
        "qe_experiment",
        "qe_archive",
        "research",
    ],
    "operations": [
        "validation",
        "qe_experiment",
        "paper_trading",
    ],
    "full": [
        "validation",
        "qe_experiment",
        "qe_archive",
        "research",
        "paper_trading",
    ],
}

ALL_MODULES = [
    "validation",
    "qe_experiment",
    "qe_archive",
    "research",
    "paper_trading",
]


def resolve_modules(*, profile: str | None, modules_csv: str | None) -> list[str]:
    """从 profile 或 modules 参数解析要加载的模块列表。"""
    if modules_csv:
        modules = [m.strip() for m in modules_csv.split(",") if m.strip()]
    elif profile:
        if profile == "all":
            modules = list(ALL_MODULES)
        elif profile in PROFILES:
            modules = PROFILES[profile]
        else:
            raise ValueError(f"Unknown profile: {profile}. Available: {list(PROFILES.keys())}")
    else:
        modules = PROFILES.get(os.environ.get("AISTOCK_MCP_PROFILE", ""), PROFILES["full"])
    return modules
```

### 3.3 ModuleRegistry (`backend/mcp/registry.py`)

```python
"""MCP 模块注册表 — 为每个模块提供共享基础设施。"""
from __future__ import annotations

import os
from typing import Any

from mcp.server.fastmcp import FastMCP

from backend.mcp.common import AIstockApiClient, sanitize_identifier, sanitize_tail, require_confirm


class ModuleRegistry:
    """为 MCP 模块提供共享能力：client、sanitize、confirm。"""

    def __init__(self, *, mcp: FastMCP, base_url: str):
        self.mcp = mcp
        self._base_url = base_url
        self._tool_counts: dict[str, int] = {}
        self._base_client = AIstockApiClient(base_url=base_url)

    def client(self, path_prefix: str = "") -> AIstockApiClient:
        """返回带 path prefix 的 API client。"""
        if path_prefix:
            return AIstockApiClient(base_url=f"{self._base_url}/{path_prefix.strip('/')}")
        return self._base_client

    def sanitize(self, value: Any, name: str = "id") -> str:
        """统一标识符验证。"""
        return sanitize_identifier(value, name)

    def confirm(self, actual: str | None, expected: str, field: str) -> None:
        """统一确认机制。"""
        require_confirm(actual, expected, field)

    def register_tool_count(self, module_name: str, count: int) -> None:
        self._tool_counts[module_name] = count

    def tool_count(self, module_name: str) -> int:
        return self._tool_counts.get(module_name, 0)

    def total_tool_count(self) -> int:
        return sum(self._tool_counts.values())
```

### 3.4 统一 API Client (`backend/mcp/common.py`)

```python
"""AIstock MCP 共享基础设施。

从 scripts/aistock_mcp_common.py 迁移并增强。
所有 MCP 模块共享同一套 client/sanitize/confirm 逻辑。
"""
from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib.parse import urlparse

import httpx

LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
DEFAULT_TIMEOUT = 30.0


def assert_loopback_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host not in LOOPBACK_HOSTS:
        raise ValueError(f"MCP client must use loopback; got host={host!r}")
    return url.rstrip("/")


def sanitize_identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string; got {value!r}")
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"{name} contains illegal characters: {value!r}")
    return value


def sanitize_tail(value: int | None, *, default: int = 500, maximum: int = 5000) -> int:
    tail = int(default if value is None else value)
    if tail < 1 or tail > maximum:
        raise ValueError(f"tail must be between 1 and {maximum}; got {tail}")
    return tail


def require_confirm(actual: str | None, expected: str, field_name: str) -> None:
    if actual != expected:
        raise ValueError(f"{field_name} must equal {expected!r} to proceed")


class AIstockApiClient:
    """统一 HTTP client，支持 loopback 调用后端 API。"""

    def __init__(self, base_url: str, *, timeout: float | None = None):
        self.base_url = assert_loopback_url(base_url)
        self.timeout = float(timeout or os.environ.get("AISTOCK_HTTP_TIMEOUT", DEFAULT_TIMEOUT))

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        clean = {k: v for k, v in (params or {}).items() if v is not None} or None
        with httpx.Client(base_url=self.base_url, timeout=self.timeout, trust_env=False) as c:
            resp = c.get(path, params=clean)
        return self._decode(resp, "GET", path)

    def post(self, path: str, body: dict[str, Any] | None = None, params: dict[str, Any] | None = None) -> Any:
        clean = {k: v for k, v in (params or {}).items() if v is not None} or None
        with httpx.Client(base_url=self.base_url, timeout=self.timeout, trust_env=False) as c:
            resp = c.post(path, params=clean, json=body or {})
        return self._decode(resp, "POST", path)

    def delete(self, path: str, body: dict[str, Any] | None = None) -> Any:
        with httpx.Client(base_url=self.base_url, timeout=self.timeout, trust_env=False) as c:
            resp = c.request("DELETE", path, json=body or {})
        return self._decode(resp, "DELETE", path)

    @staticmethod
    def _decode(resp: httpx.Response, method: str, path: str) -> Any:
        if resp.status_code >= 400:
            raise RuntimeError(f"{method} {path} → HTTP {resp.status_code}: {resp.text[:500]}")
        try:
            return resp.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{method} {path} → non-JSON response") from exc
```

### 3.5 模块实现示例 (`backend/mcp/modules/qe_experiment.py`)

```python
"""QE 实验管理 MCP 模块。

迁移自 scripts/aistock_qe_experiment_mcp_server.py。
所有 tool 函数签名和行为保持不变，仅改变注册方式。
"""
from __future__ import annotations
from typing import Any
from backend.mcp.registry import ModuleRegistry

CONFIRM_RUN = "QE_EXPERIMENT_RUN"
CONFIRM_STOP = "QE_EXPERIMENT_STOP"
CONFIRM_CUSTOM_EVO_RUN = "QE_CUSTOM_EVO_RUN"
CONFIRM_CUSTOM_EVO_DELETE = "QE_CUSTOM_EVO_DELETE"
CONFIRM_TEMPLATE_MATERIALIZE = "QE_TEMPLATE_MATERIALIZE"


def register(registry: ModuleRegistry):
    """注册 QE 实验管理 tools 到 gateway。"""
    mcp = registry.mcp
    client = registry.client("quantevolver")

    @mcp.tool()
    def qe_experiment_list(limit: int = 50, offset: int = 0) -> dict[str, Any]:
        """列出 QE 实验。"""
        return client.get("/experiments", params={"limit": limit, "offset": offset})

    @mcp.tool()
    def qe_experiment_get(experiment_id: str) -> dict[str, Any]:
        """获取 QE 实验详情。"""
        return client.get(f"/experiments/{registry.sanitize(experiment_id)}")

    @mcp.tool()
    def qe_experiment_run_confirmed(experiment_id: str, confirm_run: str = "", node_id: str | None = None) -> dict[str, Any]:
        """运行 QE 实验（需确认）。"""
        registry.confirm(confirm_run, CONFIRM_RUN, "confirm_run")
        safe = registry.sanitize(experiment_id)
        return client.post(f"/experiments/{safe}/run", params={"engine_mode": "unified", "node_id": node_id})

    # ... 其余 19 个 tools 同样迁移 ...

    registry.register_tool_count("qe_experiment", 22)
```

---

## 4. 现有 MCP Server 迁移方案

### 4.1 首期策略：不迁移，只新增

**首期（Phase 1-5）不迁移任何现有 MCP server。** 现有 3 个 server 继续使用当前已验证脚本：

```
aistock-validation      → scripts/aistock_mcp_server.py          (不动)
aistock-qe-experiment   → scripts/aistock_qe_experiment_mcp_server.py  (不动)
aistock-qe-archive      → scripts/aistock_qe_archive_mcp_server.py    (不动)
aistock-research        → python -m backend.mcp.gateway --modules=research  (新增)
```

Research MCP 从第一天就开发在 `backend/mcp/` 统一平台上，但只以独立入口暴露。

### 4.2 后续迁移顺序（Phase 6-8，独立 issue）

等 Research Pipeline + QE shadow 闭环稳定后，逐个迁移：

| Phase | 迁移目标 | 理由 |
|-------|---------|------|
| 6 | `qe_archive` → `backend/mcp/modules/qe_archive.py` | 最简单（15 tools，无复杂 helper） |
| 7 | `qe_experiment` → `backend/mcp/modules/qe_experiment.py` | 中等（22 tools，有 confirm） |
| 8 | `validation` → `backend/mcp/modules/validation.py` + `validation_helpers.py` | 最复杂（19 tools + 48 helpers） |

每个迁移必须：
- 独立 issue + 独立分支
- 保留旧入口作为 fallback（至少一个版本周期）
- 通过 tool schema parity 测试（名称、参数、行为完全一致）
- 通过 MCP stdio contract 测试（initialize、list_tools）

### 4.3 `.mcp.json` 首期形态

```json
{
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
      "args": ["scripts/aistock_mcp_gateway.py", "--modules=research"],
      "env": {"AISTOCK_BASE_URL": "http://127.0.0.1:8001/api/v1"}
    }
  }
}
```

### 4.4 统一 API Client 设计要点

`AIstockApiClient` 必须支持：
- 不同 response envelope 策略（validation 返回 `{"data":...}`，QE 返回混合形态）
- `trust_env=False` + loopback-only 强制
- 可注入 `httpx.MockTransport` 用于测试
- env_name 友好错误提示
- `scripts/aistock_mcp_gateway.py` 薄入口包含 repo-root bootstrap（避免 import 失败）

---

## 5. Research Pipeline 服务设计

### 5.1 DB Schema

```sql
CREATE SCHEMA IF NOT EXISTS research_pipeline;

CREATE TABLE research_pipeline.experiment (
    experiment_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_type       TEXT NOT NULL,
    title               TEXT NOT NULL,
    hypothesis          TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft','running','stage_failed','validated','promoted','rejected','blocked')),
    baseline_ref        TEXT,
    baseline_task_id    TEXT,
    acceptance_criteria JSONB NOT NULL DEFAULT '{}'::jsonb,
    config              JSONB NOT NULL DEFAULT '{}'::jsonb,
    tags                TEXT[] DEFAULT '{}',
    git_branch          TEXT,
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

CREATE TABLE research_pipeline.stage_execution (
    execution_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    experiment_id       UUID NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
    stage_name          TEXT NOT NULL,
    stage_index         INT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','running','passed','failed','skipped','blocked')),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    duration_seconds    FLOAT,
    input_params        JSONB DEFAULT '{}'::jsonb,
    output_metrics      JSONB DEFAULT '{}'::jsonb,
    output_verdict      TEXT CHECK (output_verdict IN ('pass','fail','inconclusive')),
    verdict_reason      TEXT,
    error_message       TEXT,
    qe_task_id          TEXT,
    retry_count         INT NOT NULL DEFAULT 0,
    UNIQUE (experiment_id, stage_name)
);

CREATE TABLE research_pipeline.artifact (
    artifact_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    experiment_id       UUID NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
    artifact_type       TEXT NOT NULL,
    artifact_path       TEXT NOT NULL,
    artifact_sha256     TEXT,
    file_size_bytes     BIGINT,
    metadata            JSONB DEFAULT '{}'::jsonb,
    status              TEXT NOT NULL DEFAULT 'candidate'
        CHECK (status IN ('candidate','validated','production','superseded','deleted')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    promoted_at         TIMESTAMPTZ
);

CREATE TABLE research_pipeline.comparison (
    comparison_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    experiment_id       UUID NOT NULL REFERENCES research_pipeline.experiment(experiment_id) ON DELETE CASCADE,
    stage_name          TEXT NOT NULL,
    baseline_label      TEXT NOT NULL,
    candidate_label     TEXT NOT NULL,
    baseline_metrics    JSONB NOT NULL,
    candidate_metrics   JSONB NOT NULL,
    delta_metrics       JSONB NOT NULL,
    verdict             TEXT NOT NULL CHECK (verdict IN ('better','worse','neutral','inconclusive')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE research_pipeline.execution_log (
    log_id              BIGSERIAL PRIMARY KEY,
    experiment_id       UUID NOT NULL,
    stage_name          TEXT,
    level               TEXT NOT NULL DEFAULT 'INFO',
    message             TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_rp_exp_status ON research_pipeline.experiment(status);
CREATE INDEX idx_rp_exp_type ON research_pipeline.experiment(pipeline_type);
CREATE INDEX idx_rp_stage_exp ON research_pipeline.stage_execution(experiment_id);
CREATE INDEX idx_rp_artifact_exp ON research_pipeline.artifact(experiment_id);
CREATE INDEX idx_rp_log_exp_time ON research_pipeline.execution_log(experiment_id, created_at);
```

### 5.2 领域流水线（Phase 1: HMM + 事件信号）

**HMM Risk Gate Pipeline stages:**
1. `artifact_gen` — 通过 HMM training service API 生成 risk gate artifact
2. `offline_validation` — 调用验证脚本，计算前向收益 spread
3. `portfolio_simulation` — 运行组合模拟对比
4. `qe_shadow` — 通过 `qe_templates` 创建受控 QE 模板 → materialize → run
5. `comparison` — 从 `qe_archive` 读取结果，与 baseline 对比，写 verdict
6. `promotion` — 人工确认晋升（需 issue_url）

**Event Signal Pipeline stages:**
1. `signal_compute` — 通过 event_signal service 计算信号因子
2. `ic_validation` — 复用现有 IC 计算服务
3. `qe_overlay` — 通过 `qe_templates` 提交 overlay 回测
4. `comparison` — 从 `qe_archive` 读取结果
5. `promotion` — 人工确认晋升

**关键原则**：
- 所有执行类动作走现有 FastAPI endpoint（不自建执行链路）
- QE shadow 通过 `qe_templates` API（与 UI 同源）
- 结果从 `qe_archive` 读取（不直接查远端节点）
- 不直接调 RD-Agent 或 QE scheduler

### 5.3 Research MCP 模块 Tools

```python
# backend/mcp/modules/research.py — 12 tools
# Research Pipeline 是编排层，不自建资产库。
# 因子/模型/算法的注册通过现有 QE/model_registry API 完成。

# 实验管理 (6)
research_create_experiment      # 创建实验
research_list_experiments       # 列表
research_get_experiment         # 详情（含 stages/artifacts/comparisons）
research_run_stage              # 执行 stage（需 confirm）
research_promote                # 晋升（需 issue_url + confirm）
research_reject                 # 拒绝

# 辅助 (6)
research_get_stage_result       # Stage 结果和指标
research_compare_baseline       # 基线对比
research_create_issue           # 从发现创建 GitHub Issue
research_list_artifacts         # 资产引用列表
research_retry_stage            # 重试失败 stage
research_get_pipeline_types     # 可用流水线类型和默认 criteria
```

---

## 6. 分支策略

### 6.1 MCP 网关 + 研究流水线开发分支

**本模块使用单一独立分支开发**：

```
分支名: feature/mcp-gateway-research-pipeline-20260518
基于: main (d4b0b61)
```

**分支内容**（全部在此分支完成）：
- `backend/mcp/` 统一网关模块（gateway + registry + common + modules）
- `backend/services/research_pipeline/` 研究流水线服务
- `backend/routers/research_pipeline.py` API 路由
- `backend/db/init_research_pipeline.sql` DB Schema
- 现有 3 个 MCP server 的迁移（到 `backend/mcp/modules/`）
- 测试
- 文档更新

**不在此分支做的事**：
- 修复现有 QE/HMM/Selection Center 的 bug（走独立 issue 分支）
- 修改不相关的功能代码
- 生成实验 artifact（资产不进 git）

### 6.2 仓库前置状态（已确认）

```
✅ main 与 origin/main 同步
✅ 工作目录干净
✅ HMM 研究分支已全部合入或归档:
   - feature/hmm-risk-gate-20260517 → 已合入 main
   - codex/hmm-rd-20260511 → 已归档 (archive/)
   - codex/hmm-sector-regime-20260509 → 已归档 (archive/)
   - codex/hmm-evo-baseline-20260506 → 已合入 main（已删除分支）
   - codex/hmm-qe-autoretry-20260509 → 已合入 main（已删除分支）
✅ 事件信号研究分支已全部合入或归档:
   - codex/event-signal-policy-20260507 → 已合入 main（已删除分支）
   - codex/financial-distress-*-20260508 → 已合入 main（已删除分支）
   - codex/unified-event-signal-*-20260506 → 已归档 (archive/)
   - codex/financial-distress-rerank-20260508 → 已归档 (archive/)
✅ 无未合入的研究分支阻塞开发
```

### 6.3 合入条件

此分支合入 main 前必须满足 §7 全部验收标准（F1-F10, D1-D5, M1-M8, R1-R5）。

### 6.4 未来研究分支规范

MCP 网关合入后，所有研究工作使用研究流水线管理，分支命名：

```
research/{pipeline_type}/{experiment_name}-{date}
```

示例：
```
research/hmm/risk-gate-v2-20260601
research/event-signal/sector-distress-20260605
research/factor/momentum-decay-20260610
```

---

## 7. 验收标准（合入 main 前提）

### 功能验收 (F1-F10)

| # | 项目 |
|---|------|
| F1 | Gateway 启动正常，`--modules=research` 加载 research 模块 |
| F2 | Research 模块 tools 可调用（通过 MCP stdio contract） |
| F3 | 现有 3 个 MCP server 不受影响（未修改） |
| F4 | 实验状态机正确流转 |
| F5 | Stage 执行引擎按序调度 |
| F6 | 自动验收判断正确 |
| F7 | QE 任务自动提交和结果拉取 |
| F8 | 数仓归档事件正确发出 |
| F9 | 资产 SHA256 去重正确 |
| F10 | Profile 切换正确控制 tool 加载 |

### Dogfooding 验收 (D1-D5)

| # | 项目 |
|---|------|
| D1 | HMM Risk Gate 实验通过全部 stage，status=validated |
| D2 | 事件信号实验通过 signal_compute + ic_validation |
| D3 | 两个实验的 artifact 正确注册 |
| D4 | 两个实验的 comparison 正确写入 |
| D5 | MCP tools 可驱动完整实验流程 |

### 迁移验收 (M1-M8)

| # | 项目 |
|---|------|
| M1 | 56 个现有 tool 在 gateway 中可调用 |
| M2 | Tool 函数签名完全不变 |
| M3 | 确认机制行为一致 |
| M4 | Bug 文件写入路径不变 |
| M5 | GitHub Issue 同步正常 |
| M6 | QE 任务提交/停止正常 |
| M7 | Archive 查询正常 |
| M8 | 旧入口 fallback 可用 |

### 回归保护 (R1-R5)

| # | 项目 |
|---|------|
| R1 | 现有 QE 实验不受影响 |
| R2 | 现有 HMM 训练不受影响 |
| R3 | backend 8001 启动无报错 |
| R4 | 前端构建无报错 |
| R5 | 现有 MCP 测试全部通过 |

---

## 8. 实施计划

| Phase | 目标 | 范围 | 明确不做 |
|-------|------|------|----------|
| 0 | 设计确认 + 基线 | 更新 main、确认 write scope、生产影响声明 | 不写业务代码 |
| 1 | MCP 平台骨架 | `backend/mcp/common.py`、`registry.py`、`gateway.py`、`profiles.py`、`scripts/aistock_mcp_gateway.py` | 不迁移旧 MCP |
| 2 | Research MCP 独立入口 | `backend/mcp/modules/research.py`、`.mcp.json` 新增 `aistock-research` | 不替换现有 MCP |
| 3 | Research Pipeline 后端 | `backend/services/research_pipeline/`、`backend/routers/research_pipeline.py`、DB schema | 不自建平行资产库 |
| 4 | Offline dogfooding | HMM/event_signal 离线 stage、artifact_ref、comparison | 不跑生产 QE |
| 5 | QE shadow 闭环 | 调 `qe_templates`、运行 QE dev/shadow、查询 `qe_archive`、写 verdict | 不直接调 RD-Agent |
| 6 | 旧 MCP 迁移试点 | 迁移 `qe_archive` module 到 gateway，保留旧入口 | 不迁移 validation |
| 7 | 旧 MCP 全量迁移 | 迁移 `qe_experiment`，最后迁移 `validation` | 未通过 parity 前不切默认 |
| 8 | 默认统一入口 | `.mcp.json` 推荐 `aistock --profile=...` | 旧入口至少保留一个版本周期 |

**首期合入范围**：Phase 0-5（Research Pipeline 完整可用）
**后续独立 issue**：Phase 6-8（旧 MCP 迁移）

**总计**: Phase 1 约 9 天，Phase 2 约 6 天，Phase 3 约 2 天。

---

## 9. Token 预算

| Profile | 加载模块 | Tools | Token 开销 | 占 200K context |
|---------|---------|-------|-----------|----------------|
| minimal | validation | 19 | ~2,850 | 1.4% |
| research | qe+archive+research | 55 | ~8,250 | 4.1% |
| operations | validation+qe+paper | 53 | ~7,950 | 4.0% |
| full | 全部 | ~86 | ~12,900 | 6.5% |
| 硬性上限 | — | 100 | 15,000 | 7.5% |

---

## 10. 测试方案

### 10.1 测试分层

```
Layer 1: 单元测试（pytest, 无外部依赖）
Layer 2: 集成测试（pytest, 需要 DB）
Layer 3: MCP 合约测试（pytest, mock transport）
Layer 4: 流水线自验证 / Dogfooding（需要 backend + 远端节点）
Layer 5: 迁移回归测试（对比新旧 MCP 行为）
```

### 10.2 Layer 1: 单元测试用例

#### Gateway + Registry

| ID | 用例 | 预期 |
|----|------|------|
| U-GW-01 | `create_gateway(["research"])` | tool_count("research") == 18 |
| U-GW-02 | `create_gateway(["qe_experiment", "research"])` | total_tool_count() == 40 |
| U-GW-03 | `create_gateway(["nonexistent"])` | 抛出 ModuleNotFoundError |
| U-GW-04 | `resolve_modules(profile="research")` | 返回 ["qe_experiment", "qe_archive", "research"] |
| U-GW-05 | `resolve_modules(profile="unknown")` | 抛出 ValueError |

#### Common (Client + Sanitize + Confirm)

| ID | 用例 | 预期 |
|----|------|------|
| U-CM-01 | `assert_loopback_url("http://127.0.0.1:8001")` | 返回 URL |
| U-CM-02 | `assert_loopback_url("http://evil.com")` | 抛出 ValueError |
| U-CM-03 | `sanitize_identifier("qe_20260517", "id")` | 返回原值 |
| U-CM-04 | `sanitize_identifier("../passwd", "id")` | 抛出 ValueError |
| U-CM-05 | `require_confirm("TOKEN", "TOKEN", "f")` | 无异常 |
| U-CM-06 | `require_confirm("wrong", "TOKEN", "f")` | 抛出 ValueError |
| U-CM-07 | `AIstockApiClient.get()` HTTP 200 | 返回 dict |
| U-CM-08 | `AIstockApiClient.get()` HTTP 404 | 抛出 RuntimeError |

#### Experiment Registry (状态机)

| ID | 用例 | 预期 |
|----|------|------|
| U-ER-01 | draft → running | status=running |
| U-ER-02 | running → validated (all pass) | status=validated |
| U-ER-03 | running → stage_failed (one fail) | status=stage_failed |
| U-ER-04 | validated → promoted | promoted_at 非空 |
| U-ER-05 | promoted → running (非法) | 抛出 InvalidStateTransition |
| U-ER-06 | any → blocked | blocked_by_issue 非空 |

#### Validation Gate (自动验收)

| ID | 用例 | 预期 |
|----|------|------|
| U-VG-01 | metrics 满足所有 criteria | (True, "all met") |
| U-VG-02 | metrics 不满足某 criteria | (False, "field: 0.3 < 0.5") |
| U-VG-03 | metrics 缺少字段 | (False, "missing: field") |
| U-VG-04 | operator `>` 通过 | (True, ...) |
| U-VG-05 | operator `>=` 边界 | (True, ...) |
| U-VG-06 | operator `<` 不通过 | (False, ...) |

#### Artifact Manager

| ID | 用例 | 预期 |
|----|------|------|
| U-AM-01 | 注册新 artifact | SHA256 正确，返回 id |
| U-AM-02 | 重复 SHA256 去重 | 返回已有 id |
| U-AM-03 | 文件不存在 | 抛出 FileNotFoundError |
| U-AM-04 | promote artifact | status=production |
| U-AM-05 | promote 时旧版本 superseded | 旧 status=superseded |

### 10.3 Layer 2: 集成测试用例

| ID | 用例 | 方法 | 预期 | 数据验证 |
|----|------|------|------|---------|
| I-DB-01 | Schema 创建 | 执行 SQL | 5 表存在 | `information_schema.tables` count=5 |
| I-DB-02 | 创建实验 | POST /experiments | 201 | experiment 表有记录 |
| I-DB-03 | 执行 stage | POST .../run | 200 | stage_execution.started_at 非空 |
| I-DB-04 | Stage 完成 | 内部 complete | passed | duration_seconds > 0 |
| I-DB-05 | Artifact 注册 | 内部 register | 有记录 | sha256 非空, size > 0 |
| I-DB-06 | Comparison 写入 | 内部 record | 有记录 | delta_metrics 非空 |
| I-DB-07 | 归档事件 | 实验完成 | outbox 有记录 | event_type 正确 |
| I-API-01 | 列表查询 | GET /experiments | 200 | 包含已创建实验 |
| I-API-02 | 详情查询 | GET /experiments/{id} | 200 | stages 数量正确 |
| I-API-03 | 拒绝实验 | POST /reject | 200 | rejected_at 非空 |
| I-API-04 | 删除非 draft | DELETE | 400 | 记录未变 |

### 10.4 Layer 3: MCP 合约测试用例

| ID | 用例 | 预期 |
|----|------|------|
| M-RES-01 | research_create_experiment 完整参数 | 返回 experiment_id |
| M-RES-02 | research_create_experiment 缺必填 | 参数错误 |
| M-RES-03 | research_run_stage 无 confirm | 返回 required_confirm |
| M-RES-04 | research_run_stage 正确 confirm | 调用后端 |
| M-RES-05 | research_promote 无 issue_url | 参数错误 |
| M-RES-06 | research_factor_register confirm | 需要 confirm |
| M-QE-01 | qe_experiment_list 签名不变 | 参数名/类型一致 |
| M-QE-02 | qe_experiment_run_confirmed 错误 confirm | ValueError |
| M-VAL-01 | health tool 签名不变 | 完全一致 |
| M-VAL-02 | report_bug 写入路径 | `tests/aistock_validation/bugs/` |

### 10.5 Layer 4: 流水线自验证（Dogfooding）

#### HMM Risk Gate 全流程

| 步骤 | 操作 | 预期 | 数据验证 |
|------|------|------|---------|
| D-HMM-01 | MCP 创建实验 | status=draft | DB 有记录 |
| D-HMM-02 | artifact_gen | status=passed | 文件存在, SHA256 正确, sector_count=131 |
| D-HMM-03 | offline_validation | spread_5d > 0 | output_metrics 含 spread_1d~20d |
| D-HMM-04 | portfolio_simulation | delta > -0.5% | output_metrics 含 annual_return_delta |
| D-HMM-05 | qe_shadow 提交 | qe_task_id 非空 | 远端任务 running |
| D-HMM-06 | qe_shadow 完成 | 4 loop completed | comparison 表有记录 |
| D-HMM-07 | 自动验收 | verdict 正确 | experiment.status=validated |
| D-HMM-08 | artifact 注册 | artifact 表有记录 | status=validated |
| D-HMM-09 | 归档事件 | outbox 有记录 | event_type 正确 |

#### 事件信号全流程

| 步骤 | 操作 | 预期 | 数据验证 |
|------|------|------|---------|
| D-EVT-01 | MCP 创建实验 | status=draft | pipeline_type=event_signal |
| D-EVT-02 | signal_compute | status=passed | 信号文件存在, 日期连续 |
| D-EVT-03 | ic_validation | output_metrics 非空 | mean_ic/ic_ir/positive_ratio 有值 |
| D-EVT-04 | 自动验收 | verdict 正确 | 按 criteria 判断 |

### 10.6 Layer 5: 迁移回归测试

| ID | 用例 | 方法 | 预期 |
|----|------|------|------|
| R-MIG-01 | Tool 名称集合一致 | 列出旧/新 names | 集合相等 |
| R-MIG-02 | Tool 参数签名一致 | 对比 input_schema | JSON schema 相等 |
| R-MIG-03 | qe_experiment_list 返回一致 | 旧/新各调用 | JSON 结构相同 |
| R-MIG-04 | confirm 拒绝行为一致 | 传错误 confirm | 相同错误 |
| R-MIG-05 | sanitize 拒绝行为一致 | 传非法 id | 相同错误 |
| R-MIG-06 | report_bug 写入一致 | 调用 | 文件路径/格式相同 |

---

## 11. 合入 main 的详细前提条件

### 11.1 代码质量 Gate

| # | 条件 | 验证命令 | 通过标准 |
|---|------|---------|---------|
| CQ-01 | py_compile 通过 | `find backend/mcp backend/services/research_pipeline backend/routers/research_pipeline.py -name "*.py" -exec python -m py_compile {} +` | 零错误 |
| CQ-02 | git diff --check | `git diff --check` | 无 whitespace 错误 |
| CQ-03 | 改动在 scope 内 | `git diff --name-only main` 对比 allowed_write_scope | 无越界 |
| CQ-04 | 无 sweeping commit | `git log --oneline main..HEAD` | 每 commit 只含相关改动 |
| CQ-05 | DB 表有 COMMENT | 检查 SQL 文件 | 所有表和关键列有 comment |
| CQ-06 | 无静默错误 | `grep -rn "except.*pass\|except:$" backend/mcp/ backend/services/research_pipeline/` | 零匹配 |
| CQ-07 | 无空密码 | `grep -rn 'password.*=""' backend/mcp/ backend/services/research_pipeline/` | 零匹配 |
| CQ-08 | 无根目录临时文件 | `git diff --name-only main \| grep -v "^backend/\|^scripts/\|^docs/\|^frontend/\|^tests/"` | 零匹配 |

### 11.2 测试 Gate

| # | 条件 | 验证命令 | 通过标准 |
|---|------|---------|---------|
| TG-01 | Layer 1 全部通过 | `pytest backend/mcp/tests/ -q` | 0 failures, ≥30 tests |
| TG-02 | Layer 2 全部通过 | `pytest backend/tests/research_pipeline/ -q` | 0 failures, ≥10 tests |
| TG-03 | Layer 3 全部通过 | `pytest backend/mcp/tests/test_*_module.py -q` | 0 failures, ≥10 tests |
| TG-04 | Layer 5 全部通过 | `pytest backend/mcp/tests/test_migration_regression.py -q` | 0 failures |
| TG-05 | 现有测试不受影响 | `pytest backend/tests/test_aistock_mcp_server.py backend/tests/test_aistock_qe_mcp_servers.py -q` | 0 failures |

### 11.3 Dogfooding Gate

| # | 条件 | 验证方式 | 通过标准 |
|---|------|---------|---------|
| DF-01 | HMM artifact_gen 通过 | DB: stage status | passed |
| DF-02 | HMM offline_validation 通过 | DB: output_metrics | spread_5d > 0 |
| DF-03 | HMM portfolio_simulation 通过 | DB: output_metrics | annual_return_delta > -0.5% |
| DF-04 | HMM qe_shadow 提交成功 | DB: qe_task_id | 非空且远端非 failed |
| DF-05 | 事件信号 signal_compute 通过 | DB: stage status | passed |
| DF-06 | 事件信号 ic_validation 通过 | DB: output_metrics | 非空 |
| DF-07 | MCP 可驱动全流程 | 通过 MCP tools 操作 | 无需直接 HTTP |

### 11.4 回归保护 Gate

| # | 条件 | 验证命令 | 通过标准 |
|---|------|---------|---------|
| RP-01 | backend 启动无报错 | 启动 + 检查日志 | 无 ERROR/CRITICAL |
| RP-02 | QE 实验不受影响 | 提交标准 QE 任务 | 正常完成 |
| RP-03 | HMM API 正常 | `GET /api/v1/hmm-training/configs` | 200 |
| RP-04 | Selection Center 正常 | risk_policy evaluate | 无异常 |
| RP-05 | 前端构建 | `cd frontend && npm run build` | exit 0 |
| RP-06 | 旧 MCP 入口可用 | 用旧 scripts/ 启动 | 正常响应 |

### 11.5 文档 Gate

| # | 条件 | 通过标准 |
|---|------|---------|
| DC-01 | 设计文档反映最终实现 | 无过时描述 |
| DC-02 | Swagger UI 新路由可见 | `/docs` 页面有 research-pipeline |
| DC-03 | 每个 MCP tool 有 docstring | 无空 docstring |
| DC-04 | 测试证据记录存在 | `tests/aistock_validation/history/` 有文件 |

### 11.6 Issue 流程 Gate

| # | 条件 | 通过标准 |
|---|------|---------|
| IS-01 | GitHub Issue 已创建 | 标题含 "MCP Gateway + Research Pipeline" |
| IS-02 | Issue 含 allowed_write_scope | scope 列表完整 |
| IS-03 | PR 引用 Issue | `Closes #NNN` |
| IS-04 | 声明生产影响 | 明确是否需重启 8001 |
| IS-05 | 声明 DB 写入 | 明确新增 schema/表 |
| IS-06 | 发现的 bug 已创建独立 issue | 不在本 PR 修复 |

### 11.7 合入执行清单

```
□ CQ-01 ~ CQ-08 全部通过
□ TG-01 ~ TG-05 全部通过
□ DF-01 ~ DF-07 全部通过
□ RP-01 ~ RP-06 全部通过
□ DC-01 ~ DC-04 全部通过
□ IS-01 ~ IS-06 全部通过
□ 用户确认合入
□ git merge → main
□ git push origin main
□ 提醒重启 backend 8001
□ 提醒启用 aistock-research MCP
□ 关闭 GitHub Issue
□ 合入后验证: API 200 + MCP 启动 + 旧入口可用
```
