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
> - **read-only**：MCP server 只暴露 finding_store 既有的查询方法，**不**新增写入路径，**不**改 finding_store schema
> - **不动 main 业务代码**：本设计仅描述 MCP server 实现位置 + tool 契约 + 部署形态；实际实施由后续 PR 落地
> - **fail-fast**：所有错误显式抛出，禁止 silent fallback（与 `feedback_no_silent_errors` 一致）

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

## 11. 一句话总结

**Validation Center MCP Server = `ValidationFindingStore` 的 read-only 透传包装**：8 个 MCP tool 一一对应 finding_store 既有方法；不改 schema、不暴露写入、不绕过既有写入流程；fail-fast typed error；进程隔离 + stdio transport；让 Claude/Codex agent 能在 session 内直接查询 findings / bugs。

---

**End of MCP server design**.
