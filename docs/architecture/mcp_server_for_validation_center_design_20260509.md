# Validation Center MCP Server — 纸面设计

> **作者**：engine-design teammate
> **日期**：2026-05-09
> **任务**：Phase 2 T3 双纸面设计之 (1)
> **范围**：纸面设计；不写代码；不改 finding_store schema；不动现有 validation center 任何文件
> **依赖**：
> - `backend/services/validation/finding_store.py`（read-only API surface 的事实源）
> - `backend/services/validation/module_quality.py`（finding_store 已有消费方）
> - `docs/architecture/aistock_internal_validation_center_implementation_plan_20260504.md`（既有 validation center 设计）
> - `docs/architecture/aistock_validation_commit_module_quality_design_20260505.md`
> - `~/.claude/projects/.../memory/feedback_no_silent_errors.md`
>
> **核心约束**：
> - **read-only**：MCP server 只暴露 finding_store / module_registry / plan_catalog / history_store 既有的查询方法，**不**新增写入路径，**不**改任何 schema
> - **不动 main 业务代码**：本设计仅描述 MCP server 实现位置 + tool 契约 + 部署形态；实际实施由后续 PR 落地
> - **fail-fast**：所有错误显式抛出，禁止 silent fallback（与 `feedback_no_silent_errors` 一致）
>
> **本设计是 cross-test 模板 §4.2 点名的"Validation MCP server (参考 mempalace MCP)"** — cross-test 模板 §4.2 明确把它列为"仍需补的衔接点"之一；本设计是该项的纸面方案。

---

## 0. 修订说明（2026-05-09 本轮派单口径）

本文档 v1（前轮交付，commit `3d856f4`）仅覆盖 finding_store 的 8 个 method 镜像。本轮按 Lead 派单扩展：

| # | 本轮新增覆盖 | 落地章节 |
| --- | --- | --- |
| 1 | 参考 mempalace MCP 实现模式（实施细节） | §4.4 mempalace 参考节 |
| 2 | `list_runs` / `get_module_matrix` 等覆盖 plan_catalog / module_registry / history_store 的查询面 | §3.9-§3.13 新增 5 个 tool |
| 3 | filter by tag / status / module / date range 的统一参数 schema | §3.14 通用查询参数规范 |
| 4 | 与 Cross-testing 流程衔接（cross_test_framework_template §4 自动化路径） | §13 新增章节 |

v1 既有内容（§3.1-§3.8 finding_store 8 tool / §4-§11 实现规范 / 测试 / 部署）保留不动；本节后续 §3.9 起为本轮增量。

---

## 1. 设计目标与边界

### 1.1 目标

| # | 目标 | 验收 |
| --- | --- | --- |
| G1 | 把 `ValidationFindingStore` 的 read-only 查询能力暴露为 MCP tools，让 Claude/Codex agent 能在 session 内直接查询 findings / bugs / 健康状态 | 至少 6 个 MCP tool 一一对应 finding_store 已有方法 |
| G2 | MCP server 实现 schema 字段层"reflection only" — 不再添加 finding_store 之外的派生字段 | tool 输出 = finding_store 返回的 dict 透传（仅做 JSON 序列化） |
| G3 | 多 agent / 跨 session 能用同一 MCP server 查同一份本地证据文件（guardrail / legacy / bug 三类） | 单 server 进程支持并发 stdio + 文件读取并发安全 |
| G4 | 错误传播保持 typed：parse error / unknown finding_id / IO 失败必须可识别 | tool 返回结构含 `error.type` + `error.context`，不返回模糊字符串 |
| G5 | server 启动 / 健康检查可独立审计 | 提供 `health` tool 返回 finding_store.health() 结果 |

### 1.2 不目标

- ❌ **不修改 finding_store schema**：不加新字段、不改字段类型、不改 schema_version 常量（GUARDRAIL_SCHEMA / LEGACY_SCHEMA / BUG_SCHEMA 维持 v1）
- ❌ **不暴露写入路径**：MCP server 不提供 finding 创建 / 更新 / 删除 tool（写入由现有 guardrail 扫描器 + bug 录入流程独占）
- ❌ **不实现新数据源**：仅 reflect 现有 guardrail / legacy / bug 三类 JSON 证据文件
- ❌ **不修改 module_quality / git_status_provider 等其他 validation 子模块**
- ❌ **不暴露 agent_context 的写入**（agent_context 是 finding_store 内部派生字段，read-only）
- ❌ **不实现跨进程 cache**：每次 tool call 都从磁盘重读（与 finding_store 现有行为一致；JSON 文件不大）

---

## 2. 整体架构定位

```
┌──────────────────────────────────────────────────────────────┐
│ 写入侧（不在本设计范围；维持现状）                            │
│  - guardrail 扫描器  → tmp/validation/guardrails/*.json     │
│  - legacy inventory  → tmp/validation/legacy_inventory/*.json│
│  - bug 录入流程       → tests/aistock_validation/bugs/*.json │
└──────────────────────────────────────────────────────────────┘
                          │ JSON files on disk
                          ▼
┌──────────────────────────────────────────────────────────────┐
│ ValidationFindingStore (existing — read-only consumer)       │
│  - list_findings / get_finding / finding_summary             │
│  - list_bugs / get_bug / bug_agent_context / bug_summary     │
│  - health                                                     │
└──────────────────────────────────────────────────────────────┘
                          │ Python API
                          ▼
┌──────────────────────────────────────────────────────────────┐
│ NEW: Validation Center MCP Server (本设计)                   │
│  - thin wrapper exposing finding_store methods as MCP tools  │
│  - JSON-RPC stdio transport (per Anthropic MCP spec)         │
│  - process-local; one server per claude session              │
└──────────────────────────────────────────────────────────────┘
                          │ MCP stdio
                          ▼
┌──────────────────────────────────────────────────────────────┐
│ MCP Clients (existing infrastructure)                        │
│  - Claude Code agents (engine-design / impl-paper-v2 / ...) │
│  - future: Codex agent reading findings during PR review     │
└──────────────────────────────────────────────────────────────┘
```

**关键边界**：
- MCP server 是 finding_store 的**第二个消费方**（第一个是 `module_quality.py:line ?`）；不替代、不绕过
- finding_store 不感知 MCP server 存在；server 进程崩溃不影响 finding_store 行为
- 不新增 DB 表；本服务无持久化层

---

## 3. MCP Tool 契约（一一对应 finding_store 方法）

每个 MCP tool 命名 `validation.<method>`，参数 + 返回直接镜像 `ValidationFindingStore` 同名方法签名。

### 3.1 `validation.health`

**对应 finding_store 方法**：`health() -> dict[str, Any]`

**input schema**：无参数

**output schema**：
```json
{
  "mode": "read_only",
  "guardrail_root": "tmp/validation/guardrails",
  "guardrail_root_exists": true,
  "legacy_root": "tmp/validation/legacy_inventory",
  "legacy_root_exists": true,
  "bug_root": "tests/aistock_validation/bugs",
  "bug_root_exists": true,
  "finding_count": 42,
  "bug_count": 7,
  "parse_errors": []
}
```

**用途**：agent session 启动时的健康检查；显示有多少 findings/bugs 在本地证据中

### 3.2 `validation.list_findings`

**对应 finding_store 方法**：`list_findings(source_type, module, severity, status, search, page, page_size)`

**input schema**（全部 optional）：
```json
{
  "source_type": "guardrail | legacy | null",
  "module": "string | null",
  "severity": "CRITICAL | HIGH | MEDIUM | LOW | null",
  "status": "open | resolved | null",
  "search": "string | null",
  "page": "int (default 1)",
  "page_size": "int (default 20, max 100)"
}
```

**output schema**：
```json
{
  "items": [{ "finding_id": "...", "title": "...", "severity": "...", "module": "...", "source_type": "...", "...": "..." }],
  "total": 123,
  "page": 1,
  "page_size": 20
}
```

**fail-fast 约束**：
- 非法 page < 1 → tool 抛 `ValidationToolError(type="invalid_page")`
- page_size > 100 → 抛 `ValidationToolError(type="page_size_exceeded")`
- 不允许在校验失败时静默 fallback 为默认值

### 3.3 `validation.get_finding`

**对应**：`get_finding(finding_id) -> dict | None`

**input**：`{ "finding_id": "string" }`

**output**：
- 命中：finding dict + `agent_context` 字段（由 finding_store 内部派生）
- 未命中：`{ "error": { "type": "finding_not_found", "context": { "finding_id": "..." } } }`

**约束**：未命中**显式返回 error**，**不**返回 null（让 MCP client 必须处理；与 feedback_no_silent_errors 一致）

### 3.4 `validation.finding_summary`

**对应**：`finding_summary() -> dict`

**input**：无

**output**：finding_count / by_source_type / by_severity / by_status / by_module / latest_findings(top 10) / parse_errors

### 3.5 `validation.list_bugs`

**对应**：`list_bugs(module, severity, status, agent, search, page, page_size)`

**input**（全部 optional）：
```json
{
  "module": "string | null",
  "severity": "CRITICAL | HIGH | MEDIUM | LOW | null",
  "status": "open | resolved | null",
  "agent": "string | null",  // 匹配 finding_store 的 assigned_agent 字段
  "search": "string | null",
  "page": "int", "page_size": "int"
}
```

**output**：与 list_findings 同结构（items / total / page / page_size），item 内字段为 bug shape

### 3.6 `validation.get_bug`

**对应**：`get_bug(bug_id) -> dict | None`

约束同 `get_finding`：未命中返回 typed error。

### 3.7 `validation.bug_agent_context`

**对应**：`bug_agent_context(bug_id) -> dict | None`

**用途**：cross-test agent 接到 bug 派单后，先调本 tool 获取 agent_context（含 module / file / 修复建议等），再决定如何处理。

### 3.8 `validation.bug_summary`

**对应**：`bug_summary() -> dict`

输出同 finding_summary 结构（bug_count / by_severity / by_status / by_module / latest_bugs / parse_errors）。

---

### 3.9 `validation.list_runs`（本轮新增 R-T37.2 D2）

**对应数据源**：`backend/services/validation/history_store.py` + `tests/aistock_validation/runs/` 目录下的 run 历史记录（read-only）

**用途**：cross-tester / agent 查询某模块 / 某 plan / 某时间窗口内的测试运行历史

**input schema**（全部 optional，遵循 §3.14 统一查询参数规范）：
```json
{
  "module": "string | null",          // 例: "validation/shadow_run"
  "plan_id": "string | null",          // 关联到 plan_catalog 的 plan_id
  "status": "passed | failed | error | pending | null",
  "tag": "string | null",              // run 自带的 tag（如 'cross_test' / 'shadow_run'）
  "started_after": "ISO8601 | null",   // 过滤 run.started_at >= ...
  "started_before": "ISO8601 | null",
  "page": "int (default 1)",
  "page_size": "int (default 20, max 100)"
}
```

**output schema**：
```json
{
  "items": [
    {
      "run_id": "...",
      "plan_id": "...",
      "module": "...",
      "status": "passed | failed | error | pending",
      "tag": "...",
      "started_at": "...",
      "ended_at": "...",
      "summary": { "passed": ..., "failed": ..., "errors": ... },
      "evidence_paths": ["..."]        // 关联证据文件路径（read-only）
    }
  ],
  "total": 0,
  "page": 1,
  "page_size": 20
}
```

**fail-fast**：
- 非法 status 值 → `ValidationToolError(type="invalid_status", context={given: ..., allowed: [...]})`
- ISO8601 解析失败 → `invalid_date_format`

### 3.10 `validation.get_run`（本轮新增 R-T37.2 D2）

**对应**：history_store 单 run 查询

**input**：`{ "run_id": "string" }`

**output**：
- 命中：完整 run dict + `evidence_paths` 列表
- 未命中：`{ "error": { "type": "run_not_found", "context": { "run_id": "..." } } }`

### 3.11 `validation.get_module_matrix`（本轮新增 R-T37.2 D2）

**对应数据源**：
- `backend/services/validation/module_registry.py`（模块注册表）
- `backend/services/validation/module_quality.py`（模块质量评分）
- `tests/aistock_validation/modules/<module>.md`（模块测试矩阵文档）
- `tests/aistock_validation/catalog/file_ownership.yaml`（模块归属）
- `tests/aistock_validation/catalog/test_levels.md`（L0-L5 等级定义）

**用途**：cross-tester 接到模块级 cross-test 任务时，一次性拉到该模块的全部测试矩阵 + 当前质量画像

**input**：`{ "module": "string" }`（必填）

**output schema**：
```json
{
  "module": "validation/shadow_run",
  "owners": ["engine-design"],
  "matrix": {
    "L0_present": true,
    "L1_present": true,
    "L2_present": false,
    "L3_present": false,
    "L4_present": false,
    "L5_present": false,
    "matrix_path": "tests/aistock_validation/modules/validation_shadow_run.md"
  },
  "quality": {
    "open_findings": { "CRITICAL": 0, "HIGH": 2, "MEDIUM": 5, "LOW": 0 },
    "open_bugs": { "CRITICAL": 0, "HIGH": 1, "MEDIUM": 0, "LOW": 0 },
    "last_passed_run_at": "...",
    "last_failed_run_at": "..."
  },
  "recent_runs": [...top 5...]
}
```

**fail-fast**：
- 未注册模块 → `ValidationToolError(type="module_not_found", context={module: "..."})`
- 矩阵文件存在但 schema 不符 → `matrix_schema_error`

### 3.12 `validation.list_modules`（本轮新增 R-T37.2 D2）

**对应**：module_registry.list_modules()

**用途**：返回所有已注册模块及其顶层元数据（owners / matrix_present / open_findings_count）

**input**（全部 optional）：
```json
{
  "owner": "string | null",            // 例: "engine-design" / "impl-paper-v2"
  "tag": "string | null",
  "with_matrix_only": "bool (default false)",   // 仅返回有 matrix 的模块
  "page": "int", "page_size": "int"
}
```

**output**：items 数组（每个 item 是 get_module_matrix 输出的精简摘要 — 仅 module / owners / matrix.L*_present / quality.open_findings 计数）

### 3.13 `validation.get_plan`（本轮新增 R-T37.2 D2）

**对应**：plan_catalog.get_plan(plan_id)

**用途**：cross-tester 接到 plan_id 后查询完整 plan 描述 + 关联 module + 期望覆盖等级

**input**：`{ "plan_id": "string" }`

**output**：
- 命中：plan dict（含 plan_id / module / level / steps / expected_outcome / linked_findings）
- 未命中：typed `plan_not_found` error

### 3.14 通用查询参数规范（本轮新增 R-T37.2 D3）

为保持 tool 入参一致性，所有"列表 + 过滤"类 tool（list_findings / list_bugs / list_runs / list_modules）必须遵循统一规范：

| 参数 | 类型 | 含义 | 校验规则 |
| --- | --- | --- | --- |
| `module` | `string \| null` | 模块名（与 `tests/aistock_validation/catalog/file_ownership.yaml` 一致） | 大小写敏感；不存在时返回 0 项不抛错（除 `get_module_matrix` 外） |
| `tag` | `string \| null` | 资源 tag（runs / plans 自带；findings/bugs 不一定有） | 大小写不敏感子串匹配 |
| `status` | `string \| null` | 各 tool 的允许值不同；非法值抛 `invalid_status` | 见 tool 各自定义 |
| `severity` | `CRITICAL \| HIGH \| MEDIUM \| LOW \| null` | 仅 findings / bugs 适用 | 大小写不敏感 |
| `started_after` / `started_before` / `last_seen_after` / `last_seen_before` | ISO8601 string \| null | 时间范围（runs 用 started_*，findings 用 last_seen_*） | 解析失败抛 `invalid_date_format` |
| `search` | `string \| null` | 全局子串搜索（id / title / fingerprint / file_path） | 大小写不敏感 |
| `page` | `int` | 页码（1-based） | < 1 抛 `invalid_page` |
| `page_size` | `int` | 每页大小 | < 1 或 > 100 抛 `page_size_exceeded` |

**禁止**：
- ❌ 跨 tool 同名参数语义不一致（例如不能让 status 在 list_findings 与 list_runs 中允许集不同但不在 input schema 里说明）
- ❌ silent ignore 非法参数（必须 typed error）
- ❌ 缺省值魔法（除 page=1 / page_size=20，其他过滤参数缺省 = "不过滤" 而非"用默认值替代"）

---

## 4. 文件位置 + server 实现规范

### 4.1 新建文件

```
backend/services/validation/mcp_server.py     (新建；本设计实施期落地)
backend/services/validation/__init__.py       (无改动)
backend/services/validation/finding_store.py  (无改动 — read-only 复用)
```

### 4.2 server.py 骨架（伪代码 / docstring 化）

```python
"""Validation Center MCP server — read-only reflection of ValidationFindingStore.

This server is a THIN WRAPPER — it does not introduce business logic, does
not transform finding_store outputs, and does not write to any of the
guardrail / legacy / bug evidence directories. Each tool corresponds 1:1
to a method on ValidationFindingStore; arguments and return values are
passed through.

Run:
    python -m backend.services.validation.mcp_server

Transport: stdio (MCP standard for local Claude session integration)
"""

from typing import Any
from backend.services.validation.finding_store import ValidationFindingStore
# from mcp.server import Server, NotificationOptions  # Anthropic SDK
# from mcp.types import Tool, TextContent

class ValidationToolError(Exception):
    """Typed error for MCP tool failures.

    error_type ∈ {
        "invalid_page", "page_size_exceeded",
        "finding_not_found", "bug_not_found",
        "parse_error", "io_error",
    }
    """
    def __init__(self, error_type: str, context: dict | None = None) -> None:
        self.error_type = error_type
        self.context = context or {}
        super().__init__(f"{error_type}: {context}")


def build_server(store: ValidationFindingStore | None = None) -> "Server":
    store = store or ValidationFindingStore()
    server = Server(name="aistock-validation", version="0.1.0")

    @server.tool("validation.health")
    async def _health(_: dict) -> dict:
        return store.health()

    @server.tool("validation.list_findings")
    async def _list_findings(args: dict) -> dict:
        _validate_pagination(args)
        return store.list_findings(
            source_type=args.get("source_type"),
            module=args.get("module"),
            severity=args.get("severity"),
            status=args.get("status"),
            search=args.get("search"),
            page=args.get("page", 1),
            page_size=args.get("page_size", 20),
        )

    @server.tool("validation.get_finding")
    async def _get_finding(args: dict) -> dict:
        finding_id = args["finding_id"]   # KeyError surfaces as MCP error
        result = store.get_finding(finding_id)
        if result is None:
            raise ValidationToolError("finding_not_found", {"finding_id": finding_id})
        return result

    # ... (其他 5 个 tool 同模式)

    return server


def _validate_pagination(args: dict) -> None:
    page = args.get("page", 1)
    page_size = args.get("page_size", 20)
    if not isinstance(page, int) or page < 1:
        raise ValidationToolError("invalid_page", {"given": page})
    if not isinstance(page_size, int) or page_size < 1 or page_size > 100:
        raise ValidationToolError("page_size_exceeded", {"given": page_size, "max": 100})


def main() -> None:
    server = build_server()
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
```

**关键不变量**：
- 每个 @server.tool 函数都是**透传**：调 finding_store → 返回 dict → 让 MCP framework 序列化
- **不做**任何字段重命名 / 字段过滤 / 字段添加（保证 G2）
- 错误**只能**来自 finding_store 自然抛出 + 本文件的 `ValidationToolError`；不允许 except 后 return 默认值

### 4.3 启动脚本

`pyproject.toml` 加 entry point（不改 setup.py / 现有打包）：

```toml
[project.scripts]
validation-mcp-server = "backend.services.validation.mcp_server:main"
```

agent 端配置 MCP client 时（`.mcp.json` 或类似）：

```json
{
  "mcpServers": {
    "aistock-validation": {
      "command": "python",
      "args": ["-m", "backend.services.validation.mcp_server"],
      "transport": "stdio"
    }
  }
}
```

### 4.4 参考 mempalace MCP 实施模式（本轮新增 R-T37.2 D1）

mempalace 是用户已在用的 MCP server（参 `~/.claude/projects/.../memory/mempalace_setup.md`），可作为本设计的实施参考。**关键复用模式**：

| mempalace 模式 | 本 server 复用方式 |
| --- | --- |
| **stdio transport + Anthropic MCP SDK**（`mcp.server.Server`） | 同 — §4.2 骨架已示意 |
| **每个 tool 一个 `@server.tool` 装饰函数**，输入 dict 透传给底层 service | 同 — finding_store / module_registry / plan_catalog / history_store 各一组 method 镜像 |
| **错误向 MCP framework 抛 typed exception**（如 mempalace 的 sanitize_name 失败抛错而非吞错） | 同 — `ValidationToolError` 一致风格 |
| **server 进程不持久化业务状态**（mempalace 每次 query 都查 sqlite + ChromaDB） | 同 — 本 server 每次 tool call 让底层 service 重读磁盘（finding_store 已是此模式，参 finding_store.py:170-188） |
| **轻量启动**（mempalace 启动 < 1s） | 同 — 本 server 依赖纯 Python service，无外部连接 |
| **失败后 fallback 提示而非吞错**（mempalace 的 reconnect 机制） | 不复用——本 server 是纯 read-only，不需要 reconnect；任何 IO 失败原样抛 `io_error` |
| **session-local server 进程**（每个 Claude session 启动自己的 mempalace 实例） | 同 — §5.2 隔离形态已说明 |

**与 mempalace 的差异**（本 server **不**复用的部分）：

| mempalace 特性 | 本 server 不复用的原因 |
| --- | --- |
| 写入 tool（add_drawer / kg_add / diary_write） | 本 server 严格 read-only（G2 / 安全约束 S1） |
| 语义搜索（embedding + ChromaDB） | finding_store 是结构化数据；不需要语义检索 |
| 跨 wing tunnel / KG | 本 server 无关系图；只暴露平面 list/get tool |
| Hook 触发（autosave 等） | 本 server 不需要主动触发；仅响应 client 调用 |
| AAAK 压缩格式 | 本 server 输出原始 finding/bug dict |

**实施期具体落点参考**（mempalace 代码位置 → 本 server 对应）：

| mempalace 文件 | 本 server 对应文件 |
| --- | --- |
| `mempalace/mempalace_server.py` 顶层 main + Server 实例化 | `backend/services/validation/mcp_server.py` 同 |
| `mempalace/tools/*.py`（每类 tool 一个文件） | 本 server 单文件即可（tool 数量少；< 13 个） |
| `mempalace/db/*.py`（sqlite 包装） | 本 server **不需要**——直接调既有 service 类 |

实施 PR 提交时建议在 commit message 引用 mempalace 模式，便于 reviewer 对照。

---

## 5. 安全 / 隔离 / 部署形态

### 5.1 安全约束

| # | 约束 | 落地 |
| --- | --- | --- |
| S1 | 不暴露任何写入路径 | tool list 不含 create/update/delete 类 tool |
| S2 | 不读 finding_store 之外的文件 | server 只持 ValidationFindingStore 实例；不开 sqlite / Postgres 连接 |
| S3 | 不执行 shell / 不做 import_module 动态加载 | server 模块顶层 import 列表固定 |
| S4 | 不写日志含敏感字段（finding 内部可能有 file_path / fingerprint）| 仅 stderr 输出 server lifecycle 事件（启动 / 退出）；不打印 finding 内容 |
| S5 | 进程退出时清理 stdio 句柄 | finally 块 close stdin/stdout |
| S6 | 不依赖网络 / 不发 HTTP | 100% 本地 stdio |

### 5.2 隔离形态

- **单 session 单 server 进程**：每个 Claude session 的 MCP client 启动自己的 server 子进程。多个 session 并发时各自独立读 JSON 文件（OS-level 文件 locking 不需要，因为是 read-only）
- **server 与 main 应用解耦**：server 不依赖 FastAPI 8001 / `app_pg.py`；可独立运行
- **finding_store 调用线程安全**：`_load_findings` / `_load_bugs` 每次重读磁盘（参 finding_store.py:170-188）；无内存共享状态需要锁

### 5.3 资源限制

| 资源 | 上限 | 处置 |
| --- | --- | --- |
| 单次 list 返回项数 | 100（page_size 上限） | 超限抛 `page_size_exceeded` |
| 单个 JSON 文件大小 | 16 MB（finding_store.MAX_JSON_BYTES，参 finding_store.py:17） | 超限 finding_store 已抛错；MCP server 透传 |
| server 进程内存 | < 200 MB（典型 finding 数量下） | 不主动管控；OS 级监控 |
| 单 tool 调用延迟 | < 1s（典型；磁盘 IO 主导） | 不加 timeout（让 MCP framework 处理） |

---

## 6. 错误传播契约

按 `feedback_no_silent_errors`，**所有错误必须以 typed error 抛出**。

### 6.1 `ValidationToolError` 类型表

| error_type | 触发场景 | context 字段 |
| --- | --- | --- |
| `invalid_page` | page < 1 或非 int | `{ "given": <value> }` |
| `page_size_exceeded` | page_size < 1 或 > 100 | `{ "given": <value>, "max": 100 }` |
| `finding_not_found` | get_finding 未命中 | `{ "finding_id": "..." }` |
| `bug_not_found` | get_bug / bug_agent_context 未命中 | `{ "bug_id": "..." }` |
| `parse_error` | finding_store 内部 JSON 解析失败 | `{ "source": "guardrail/legacy/bug", "path": "...", "message": "..." }` — 透传 finding_store.parse_errors |
| `io_error` | 文件不可读 / 路径不存在 | `{ "path": "...", "errno": ... }` |

### 6.2 禁止做法

- ❌ `try: ... except: return {}` （吞错）
- ❌ `try: ... except: return { "error": "something failed" }` （丢失 type）
- ❌ 把 finding_store 的 parse_errors 数组合并到 success 返回里（必须保留为 health/summary 的独立字段）

### 6.3 强制做法

- ✅ 每 tool 入口 validate args；失败立即抛
- ✅ finding_store 异常（如 IO 失败）原样冒泡；不在 server 层 wrap 后吞掉信息
- ✅ MCP framework 把 ValidationToolError 序列化为 MCP error response（含 type + context）

---

## 7. 测试策略

### 7.1 单元测试（`backend/tests/validation/test_mcp_server.py` 新建）

| # | 测试 | 验收 |
| --- | --- | --- |
| T1 | `validation.health` 返回 finding_store.health() 完全相同的 dict | dict equality |
| T2 | `list_findings` 各 filter 组合（source_type / module / severity）参数透传正确 | mock store + assert call args |
| T3 | `list_findings` page=0 → 抛 `invalid_page` | pytest.raises ValidationToolError |
| T4 | `list_findings` page_size=200 → 抛 `page_size_exceeded` | 同上 |
| T5 | `get_finding` 未命中 → 抛 `finding_not_found` | 同上 |
| T6 | finding_store 抛 IO 异常 → server 原样冒泡 | mock store raise OSError |
| T7 | 启动 server 不依赖 8001 / DB | stub finding_store + assert no network call |

### 7.2 集成测试（`backend/tests/validation/test_mcp_server_integration.py`）

| # | 测试 | 验收 |
| --- | --- | --- |
| I1 | 启动真实 server 子进程 + MCP client stdio 连接 + 调 health → 收到健康响应 | subprocess.Popen + JSON-RPC handshake |
| I2 | client 调 `list_findings` 返回真实 fixture 目录内容 | 预置 fixture JSON |
| I3 | server 在 SIGTERM 下干净退出 | proc.terminate() + 检查无 zombie |
| I4 | 并发两个 client 同时调（避免 stdin 串口竞争） — 仅冒烟 | 不强测，记录预期行为 |

### 7.3 契约测试（与现有 finding_store 行为一致性）

每次 finding_store 输出 dict 字段变更（虽然 schema 不改，但 finding_store.py 可能内部加派生字段如 agent_context）→ 契约测试检测 MCP tool 输出与 finding_store 输出 dict 完全一致。

---

## 8. 实施依赖与归属

| 项 | 归属 | 状态 |
| --- | --- | --- |
| 本设计文档 | engine-design teammate（本任务 #37 (1)） | 交付（本文档） |
| `mcp_server.py` 实施 | 待派 impl | 依赖本设计 |
| MCP client 配置（`.mcp.json` 或同等）| agent session 配置端 | 实施期一并 |
| 单元测试 | impl PR 内 | 实施期 |
| 集成测试 | 同上 | 实施期 |

**与 finding_store 改动的关系**：
- finding_store 未来若加新 read-only 方法（如 `find_findings_by_fingerprint`）→ MCP server 加对应 tool（透传），不改本设计契约
- finding_store 若加写入方法 → **本设计明确不暴露**；写入永远走现有 guardrail / legacy / bug 录入流程（**不**走 MCP）

---

## 9. 与 Validation Center 既有设计的关系

| 既有文档 | 关系 |
| --- | --- |
| `aistock_internal_validation_center_implementation_plan_20260504.md` | 本 MCP server 是该 plan §X 的延伸（让 finding_store 可被 agent 直接消费）；不修改 plan 主体 |
| `aistock_validation_commit_module_quality_design_20260505.md` | module_quality 是 finding_store 的现有消费方；MCP server 是第二个消费方；两者不互相影响 |
| `aistock_validation_menu_route_coverage_design_20260505.md` | 与本设计正交（前端路由覆盖与 MCP 不直接相关） |
| `cross_test_framework_template_20260508.md`（standards/） | cross-test agent 接到 bug 派单后可用 `validation.bug_agent_context` tool 直接拿 context；本设计是 cross-test 的工具支撑 |

---

## 10. 不在本设计范围

- MCP server 启动与 systemd / Windows service 集成
- 跨主机的远端 MCP server（仅 stdio 本地）
- finding_store 之外的数据源（如 git_status_provider / module_registry — 它们各自独立服务）
- Claude Code agent 端 MCP client 配置示例完整化（实施期再补）
- 性能基线（typical finding count 下延迟可忽略；不需要先期 benchmarking）

---

## 13. 与 Cross-testing 流程衔接（本轮新增 R-T37.2 D4）

`docs/standards/cross_test_framework_template_20260508.md` §4.2 明确把 "Validation MCP server (参考 mempalace MCP)" 列为 cross-test 自动化路径"仍需补的衔接点"之一。本设计是该项的纸面方案。本节落到 cross-test 模板中具体的衔接动作。

### 13.1 cross-test 模板 §4.3 工作流的 MCP tool 映射

cross-test 模板 §4.3 给出 MVP 期人工 cross-test 工作流（6 步）。本 server 落地后，每步可自动化的关键 MCP 调用如下：

```
Step 1  cross-tester 跑 test plan，发现 fail
   ├─ MCP: validation.get_plan(plan_id)              # 获取 plan 定义
   ├─ MCP: validation.list_runs(plan_id=..., status=failed)  # 检查是否已有失败记录
   └─ (跑 test 本身不在 MCP 范围；由 cross-test runner 直接执行)

Step 2  cross-tester 不修代码，改为：
   a. 在 GitHub 创建 Issue (gh CLI；本 server 不暴露 GitHub 写)
   b. body 含 §2.8 yaml 中的 agent_context (机读)
   c. 通过 Validation Center API POST /findings (写入 — 不在本 server 范围；
      仍走既有 28 个 API 端点之一；本 server 只读)

Step 3  通知 developer_agent
   └─ MCP: validation.bug_agent_context(bug_id)      # cross-tester 提供 bug 上下文给 dev

Step 4  developer_agent 修复，push
   └─ MCP: validation.get_module_matrix(module=...)  # dev 修代码前查模块完整测试矩阵
       └─ 用于决定是否需要补测试用例 / 哪些用例必须复跑

Step 5  人工 re-trigger cross-tester 跑同 test_id
   ├─ MCP: validation.get_run(run_id)                # 查上次失败 run 详情
   └─ (跑 test 本身不在 MCP 范围)

Step 6  通过：Issue 标 verified；Validation Center finding 标 CLOSED
   ├─ MCP: validation.list_runs(plan_id=..., started_after=re_trigger_ts)
   │   └─ 验证新 run 已 PASS
   └─ (Issue / finding 状态写入 — 不在本 server 范围；走既有 28 API)
```

**关键不变量**：
- 本 server **只读**所有衔接点；写入仍走 GitHub API + Validation Center 既有 28 个 API 端点
- cross-test 模板 §4.2 明确"MVP 阶段用人工 cross-test"——本 server 是**人工 cross-test 的查询助手**，不替代自动路由（cross_test_router.py 仍待新建，不在本设计范围）

### 13.2 与 cross-test 模板 §2.8 yaml 字段的映射

cross-test 模板 §2.8 要求 Test Plan 与 Validation Center 衔接的 yaml 字段（含 plan_id / module / level / linked_findings / agent_context）。本 server 提供以下查询能力对齐这些字段：

| cross-test §2.8 字段 | 对应 MCP tool |
| --- | --- |
| `plan_id` | `validation.get_plan(plan_id)` 验证 plan 存在性 + 拉详情 |
| `module` | `validation.get_module_matrix(module)` 拉模块完整画像 |
| `level` (L0-L5) | 含在 get_module_matrix 输出的 matrix.L*_present 字段 |
| `linked_findings`（finding_id 数组） | 逐个 `validation.get_finding(finding_id)` 验证存在性 |
| `linked_bugs` | 逐个 `validation.get_bug(bug_id)` |
| `assigned_agent` | `validation.list_findings(...) / list_bugs(...)` 用 agent 参数过滤 |
| `agent_context`（reproduce_command / suspected_files / safety_constraints / required_verification_commands） | `validation.bug_agent_context(bug_id)` 直接拉到 |

### 13.3 cross-test 自动路由（cross_test_router.py）的预留接口

cross-test 模板 §4.2 列出"Cross-test 自动路由（`cross_test_router.py` 待新建）"。该 router 一旦落地，通过本 server 的 MCP 调用即可获得所需 read 能力：

```
cross_test_router.py 期望流程（伪代码，不在本设计实施）：
   1. 监听 Git push 事件
   2. 据 file_ownership.yaml 判断改动文件归属模块
   3. MCP: validation.get_module_matrix(module=改动模块)
        → 拉到 owners + matrix
   4. 据 owners 路由 cross-tester
   5. cross-tester 跑 plan 后：
      MCP: validation.list_runs(module=..., started_after=push_ts)
        → 等 run 完成
   6. 失败时：（写入路径不在本 server）
      POST /findings + assigned_agent
   7. 通知开发者
```

**本 server 的角色**：步骤 3 / 5 的 read 调用；写入步骤（6）走既有 28 API。

### 13.4 与 cross-test 模板 §A.5 测试矩阵的关系

cross-test 模板 §4.4 提到主体设计 §A.5 规定每个 Codex Phase PR 必须含测试矩阵。本 server 的 `validation.get_module_matrix` tool 是这些测试矩阵的**统一读取入口**：

- 矩阵文件位于 `tests/aistock_validation/modules/<module>.md`
- module_registry 已注册的模块都暴露此 tool
- cross-tester / dev / Lead 都用同一 tool 读，避免文档读取 + 路径硬编码漂移

---

## 14. 一句话总结

**Validation Center MCP Server = 多模块 read-only 透传包装**：13 个 MCP tool 覆盖 finding_store / module_registry / plan_catalog / history_store 四个 read-only service（不改 schema、不暴露写入、不绕过既有写入流程）；fail-fast typed error；进程隔离 + stdio transport；参考 mempalace 模式实施；落地 cross-test 模板 §4.2 点名的 MCP server 衔接点；让 Claude/Codex agent 能在 session 内直接查询 findings / bugs / runs / module matrices。

---

**End of MCP server design**.
