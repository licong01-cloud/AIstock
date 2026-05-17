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
| MCP 入口 | **统一网关（1 个进程）** | 50MB vs 8×50MB；共享 client/confirm/sanitize |
| 模块加载 | **动态按需（--profile/--modules）** | Token 只含已加载模块（1.4%~7.5% context） |
| 代码组织 | **`backend/mcp/` 统一模块** | 集中管理，统一测试 |
| 研究流水线 | **AIstock 一级服务模块** | 与 QE/Archive/Selection 同级 |
| 资产库 | **合并在 research MCP 模块中** | 研究产出的自然下游 |
| 代码 vs 资产 | **严格分离** | 代码进 git，资产进 DB + 文件存储 |

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
│   ├── research.py                       ← 新增：研究流水线 + 资产库
│   └── paper_trading.py                  ← Phase 3 新增
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

### 4.1 迁移对照表

| 现有文件 | 迁移目标 | 改动量 | 兼容策略 |
|---------|---------|--------|---------|
| `scripts/aistock_mcp_common.py` (110行) | `backend/mcp/common.py` | 重写（增强） | 旧文件保留，import 新模块 |
| `scripts/aistock_mcp_server.py` (1675行) | `backend/mcp/modules/validation.py` + `validation_helpers.py` | 拆分 | 旧文件改为薄入口 |
| `scripts/aistock_qe_experiment_mcp_server.py` (191行) | `backend/mcp/modules/qe_experiment.py` | 1:1 迁移 | 旧文件改为薄入口 |
| `scripts/aistock_qe_archive_mcp_server.py` (122行) | `backend/mcp/modules/qe_archive.py` | 1:1 迁移 | 旧文件改为薄入口 |

### 4.2 迁移步骤（保证零停机）

**Phase 1: 并行运行（新旧共存）**

```
Step 1: 创建 backend/mcp/ 目录结构
Step 2: 实现 gateway.py + registry.py + common.py
Step 3: 实现 research 模块（新功能）
Step 4: 创建 scripts/aistock_mcp_gateway.py 薄入口
Step 5: 在 .mcp.json 中添加 aistock-research（新 server，独立入口）
Step 6: 验证新 research server 工作正常
```

此时 `.mcp.json` 有 4 个 server（3 旧 + 1 新），互不影响。

**Phase 2: 迁移现有模块**

```
Step 7: 将 qe_experiment 逻辑迁移到 backend/mcp/modules/qe_experiment.py
Step 8: 将 qe_archive 逻辑迁移到 backend/mcp/modules/qe_archive.py
Step 9: 将 validation 逻辑迁移到 backend/mcp/modules/validation.py + helpers
Step 10: 验证所有模块通过 gateway 加载后行为一致
Step 11: 更新 .mcp.json 为统一入口
Step 12: 旧 scripts/ 文件改为 fallback 入口（import 新模块）
```

**Phase 3: 清理**

```
Step 13: 确认所有用户/Codex 已切换到新入口
Step 14: 删除旧 scripts/ 中的 MCP 实现代码（保留薄入口）
Step 15: 更新文档和测试
```

### 4.3 旧文件 Fallback 入口（迁移期间保留）

```python
# scripts/aistock_qe_experiment_mcp_server.py（迁移后）
"""Fallback 入口 — 实际逻辑已迁移到 backend/mcp/modules/qe_experiment.py"""
from backend.mcp.gateway import create_gateway
mcp = create_gateway(["qe_experiment"])
if __name__ == "__main__":
    mcp.run()
```

### 4.4 `.mcp.json` 演进

**Phase 1（新旧共存）：**
```json
{
  "mcpServers": {
    "aistock-validation": {"command": "python", "args": ["scripts/aistock_mcp_server.py"]},
    "aistock-qe-experiment": {"command": "python", "args": ["scripts/aistock_qe_experiment_mcp_server.py"]},
    "aistock-qe-archive": {"command": "python", "args": ["scripts/aistock_qe_archive_mcp_server.py"]},
    "aistock-research": {"command": "python", "args": ["-m", "backend.mcp.gateway", "--modules=research"]}
  }
}
```

**Phase 2（统一入口）：**
```json
{
  "mcpServers": {
    "aistock": {
      "command": "python",
      "args": ["-m", "backend.mcp.gateway", "--profile=full"],
      "env": {"AISTOCK_BASE_URL": "http://127.0.0.1:8001/api/v1"}
    }
  }
}
```

### 4.5 Validation Server 特殊处理

Validation server 有 48 个 private helper（bug lifecycle + GitHub sync），是最复杂的迁移：

```
scripts/aistock_mcp_server.py (1675 行)
  ├── 19 个 @mcp.tool() 函数（~300 行）→ backend/mcp/modules/validation.py
  ├── ValidationCenterClient（~50 行）→ 用 AIstockApiClient(unwrap_data=True) 替代
  ├── Bug lifecycle helpers（~800 行）→ backend/mcp/modules/validation_helpers.py
  └── GitHub sync helpers（~500 行）→ backend/mcp/modules/validation_helpers.py
```

**关键兼容点**：
- `ValidationCenterClient` 会解包 `{"data": ...}` envelope → 在 `AIstockApiClient` 中用 `unwrap_data=True` 参数实现
- Bug 文件写入路径 `tests/aistock_validation/bugs/` → 保持不变
- GitHub token 从环境变量读取 → 保持不变

### 4.6 迁移验证清单

| # | 验证项 | 方法 |
|---|--------|------|
| V1 | 所有 56 个现有 tool 在新 gateway 中可调用 | 逐个 smoke test |
| V2 | Tool 函数签名完全不变 | 对比旧/新 tool schema |
| V3 | 确认机制行为一致 | 测试 confirm token 拒绝/通过 |
| V4 | Bug 文件写入路径不变 | 写入后检查文件位置 |
| V5 | GitHub Issue 同步正常 | 创建测试 issue |
| V6 | QE 任务提交/停止正常 | 提交+停止一个测试任务 |
| V7 | Archive 查询正常 | 查询历史 runs |
| V8 | 旧入口 fallback 可用 | 用旧 scripts/ 启动验证 |

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
1. `artifact_gen` — 调用 `precompute_hmm_risk_gate.py` 生成 artifact
2. `offline_validation` — 调用 `validate_hmm_risk_gate.py` 验证前向收益
3. `portfolio_simulation` — 运行组合模拟对比
4. `qe_shadow` — 提交 QE 4-arm 任务，等待结果
5. `promotion` — 人工确认晋升（需 issue_url）

**Event Signal Pipeline stages:**
1. `signal_compute` — 计算事件信号因子
2. `ic_validation` — IC/RankIC 独立验证
3. `qe_overlay` — QE overlay 回测
4. `promotion` — 人工确认晋升

### 5.3 Research MCP 模块 Tools

```python
# backend/mcp/modules/research.py — 18 tools

# 实验管理 (6)
research_create_experiment      # 创建实验
research_list_experiments       # 列表
research_get_experiment         # 详情
research_run_stage              # 执行 stage
research_promote                # 晋升
research_reject                 # 拒绝

# 资产库 (6)
research_factor_register        # 因子入库
research_factor_query           # 因子查询
research_model_register         # 模型入库
research_model_query            # 模型查询
research_algo_register          # 执行算法入库
research_algo_query             # 执行算法查询

# 辅助 (6)
research_get_stage_result       # Stage 结果
research_compare_baseline       # 基线对比
research_create_issue           # 创建 GitHub Issue
research_list_artifacts         # 资产列表
research_retry_stage            # 重试 stage
research_get_pipeline_types     # 可用流水线类型
```

---

## 6. 分支策略

### 6.1 MCP 网关 + 研究流水线开发分支

**本模块使用单一独立分支开发**：

```
分支名: feature/mcp-gateway-research-pipeline-20260518
基于: main (ca2cc42)
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
| F1 | Gateway 启动正常，`--profile=full` 加载所有模块 |
| F2 | 所有 56 个现有 tool 在新 gateway 中行为一致 |
| F3 | Research 模块 18 个 tools 可调用 |
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

| Phase | 天数 | 内容 | 产出 |
|-------|------|------|------|
| 1a | 2 | gateway + registry + common + profiles | MCP 基础设施 |
| 1b | 3 | research 模块 + research_pipeline 服务 | 研究流水线核心 |
| 1c | 2 | HMM + event signal pipeline 实现 | 两个领域流水线 |
| 1d | 2 | API router + 测试 + dogfooding | 验收通过 |
| 2a | 2 | 迁移 qe_experiment + qe_archive 模块 | 2 个模块迁移 |
| 2b | 3 | 迁移 validation 模块（最复杂） | validation 迁移 |
| 2c | 1 | 更新 .mcp.json 为统一入口 | 切换完成 |
| 3 | 2 | paper_trading 模块 + 清理旧文件 | 全功能 |

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
