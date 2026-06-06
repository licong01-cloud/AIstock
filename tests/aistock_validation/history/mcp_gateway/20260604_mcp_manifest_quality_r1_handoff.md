# MCP Unified Gateway R1 Manifest Quality Handoff

## 1. 当前状态

- 任务：MCP 统一 Gateway 改进 R1 —— manifest 数据质量校准。
- 范围：纯 gateway 侧；不修改 Research Assistant service 代码；不做股票分析模块。
- 当前阶段：只读规划和交接文档已完成，功能代码尚未修改。
- Worktree：`F:\Dev\AIstock_worktrees\mcp-manifest-quality-20260604`
- Branch：`codex/mcp-manifest-quality-20260604`
- Base：`origin/main`，当前 HEAD `62153810 chore(issue): close-sync BUG-255 after merge (#719)`
- Batch ID：`BATCH-MCP-GATEWAY-R1-20260604`
- 服务边界：未启动、停止或重启 `8001/3000/19080`；后续验证如需本地后端，只能使用 `8011/8012`。
- 根目录状态提醒：`F:\Dev\AIstock` 有无关未跟踪 `.codex_tmp/qe_recovery_20260604*` 文件，不属于本任务，不要清理或提交。

## 2. 已完成的强制阅读

已阅读并用于规划：

- `docs/codex_project_memory.md`
- `docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md`
  - 重点：5.3 工具状态模型、6 失败模式与防护、10 验收矩阵、11 实施边界。
- `docs/standards/aistock_development_standard_v1.5_20260523.md`
  - 重点：P0/P1 红线、`PROD-PORT-001`、`ERR-FALLBACK-001`、`ROOT-POLLUTION-001`、`DESIGN-COMPLIANCE-001`、`PROD-DDL-001`、上下文预算。
- `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md`
  - 重点：独立 worktree/branch、batch 执行、PR checklist、MCP issue 持久化要求。
- `backend/mcp/tool_manifest.py`
- `tests/mcp/test_mcp_tool_manifest.py`
- `tests/mcp/test_mcp_inventory_diff.py`
- 关联文件只读审阅：
  - `backend/mcp/modules/validation.py`
  - `backend/mcp/profiles.py`
  - `backend/mcp/gateway.py`
  - `scripts/aistock_mcp_gateway.py`
  - `scripts/aistock_mcp_gateway_doctor.py`
  - `tests/aistock_validation/catalog/test_plans.yaml`
  - `noxfile.py`

Claude Code 继续开发前仍建议重新打开上述强制文件片段，当前文档只是交接摘要，源码与设计文档为准。

## 3. 当前代码观察

### 3.1 Manifest 现状

- `backend/mcp/tool_manifest.py` 当前 manifest 总数为 209：
  - legacy/business tools：203
  - gateway catalog tools：6
- 当前 `risk_level` / `assistant_usable` 主要由名称启发式推导。
- 当前 `migration_state` 在 `build_tool_manifest()` 中硬编码为 `"gateway"`。
- `backend/mcp/profiles.py` 当前：
  - `GATEWAY_MODULES` 包含全部 gateway module。
  - `SCRIPT_BACKED_SERVERS: set[str] = set()`，说明当前默认无 script-backed server。

### 3.2 初步风险误标

用当前启发式初步审计 209 工具，已发现硬约束 token 下的误标：

- `factor_library_plan_register` 当前为 `read_only/direct_or_catalog`，但命中 `register`，应进入 preflight。
- `factor_library_plan_deprecate` 当前为 `read_only/direct_or_catalog`，但命中 `deprecate`，应进入 preflight。
- `model_registry_plan_register` 当前为 `read_only/direct_or_catalog`，但命中 `register`，应进入 preflight。

还需要逐工具复核的 read_only 可疑项：

- `assistant_add_task_event`
- `assistant_chat_turn`
- `factor_metrics_export_result_ref`
- `research_get_backfill_run`

注意：最终 override 不能只修这几个，要先输出完整审计表，按工具真实语义补充 override。

### 3.3 validation module legacy import 现状

`backend/mcp/modules/validation.py` 当前模块顶层：

```python
from scripts import aistock_mcp_server as legacy_validation
```

这会在 import gateway validation module 时执行 legacy script 顶层代码，违反 R1 的 M3 意图。

重要细节：当前不只是 `_compact_issue_item` 依赖 legacy script，以下工具也直接调用 `legacy_validation.*`：

- `report_bug`
- `mcp_github_issue_list`
- `mcp_github_issue_search`
- `mcp_github_issue_create`
- `assign_bug`
- `update_bug_status`
- `mcp_github_issue_sync_bug`

因此实现 M3 时不要只改 compact helper 后留下模块顶层 legacy import。若要完全消除 `backend/mcp/modules/*.py` 对 `scripts` 的 import，需要把 `_compact_issue_item` 抽到 shared util，并对其余 legacy wrapper 采用明确 lazy adapter 或进一步迁移。若发现必须大范围迁移 BUG/GitHub workflow 才能满足设计，应按停止条件上报，不要缩水。

## 4. 实施计划

### M1：risk_level / assistant_usable 质量校准

目标文件：

- `backend/mcp/tool_manifest.py`
- `tests/mcp/test_mcp_tool_manifest.py`

建议实现：

- 新增数据结构：

```python
@dataclass(frozen=True)
class ToolMetadataOverride:
    risk_level: str | None = None
    assistant_usable: str | None = None
    requires_confirmation: bool | None = None
```

- 新增常量：

```python
TOOL_METADATA_OVERRIDES: dict[str, ToolMetadataOverride] = {...}
```

- 调整或新增函数：

```python
def _risk_for(tool_name: str, module: str) -> str: ...
def _assistant_usable_for(tool_name: str, risk_level: str) -> str: ...
def _requires_confirmation(tool_name: str) -> bool: ...
```

- `build_tool_manifest()` 先走启发式，再应用逐工具 override。
- `validate_manifest()` 增加一致性检查：
  - side-effect / long-running / production-adjacent / external-network 工具不得 `read_only`。
  - 这些工具不得 `assistant_usable=direct_or_catalog`。
  - 非 `catalog/read_only` 风险默认必须 `preflight_required`，除非设计明确允许且有 override 证据。

必加测试：

```python
def test_manifest_risk_no_write_as_readonly() -> None:
    ...
```

测试 token 列表必须覆盖用户要求：

- `_confirmed`
- `register`
- `deprecate`
- `promote`
- `retire`
- `bind`
- `apply`
- `toggle`
- `sync`
- `repair`
- `schedule`
- `report_bug`
- `assign`
- `update_bug`
- `start_validation_execution`
- `github_issue_create`

断言：

```python
assert entry.risk_level in {
    "write_confirmed",
    "long_running",
    "production_adjacent",
    "external_network",
}
assert entry.assistant_usable == "preflight_required"
```

### M2：migration_state 诚实化

目标文件：

- `backend/mcp/tool_manifest.py`
- `tests/mcp/test_mcp_tool_manifest.py`

建议实现：

- 新增：

```python
MIGRATION_STATE_OVERRIDES: dict[str, str] = {}
```

- 新增函数：

```python
def _migration_state_for(
    tool_name: str,
    module: str,
    *,
    gateway_modules: Iterable[str] | None = None,
    script_backed_servers: Iterable[str] | None = None,
    overrides: Mapping[str, str] | None = None,
) -> str: ...
```

规则：

- 显式 override 优先，可表达：
  - `wrapper_compat`
  - `deprecated_pending_approval`
- 否则：
  - module in `GATEWAY_MODULES` -> `gateway`
  - module in `SCRIPT_BACKED_SERVERS` -> `script_backed`
  - 其他未知状态 -> fail-fast，经 `validate_manifest()` 报错，不用默认值掩盖。

测试：

- 当前 `SCRIPT_BACKED_SERVERS == set()` 时，209 个工具均推导为 `gateway`。
- 模拟 script-backed 输入时能得到 `script_backed`。
- override 能表达 `wrapper_compat` / `deprecated_pending_approval`。
- 非法 migration_state 被 `validate_manifest()` 拦截。
- 现有 `test_manifest_counts_and_required_metadata` 不再写死 `entry.migration_state == "gateway"` 作为唯一逻辑，而是断言当前状态与推导规则一致。

### M3：validation module 不再 import legacy script 顶层

目标文件：

- `backend/mcp/modules/validation.py`
- 新增 shared util，例如：
  - `backend/mcp/validation_issue_items.py`
- 可选新增 lazy adapter：
  - `backend/mcp/legacy_validation_adapter.py`
- 可选小改：
  - `scripts/aistock_mcp_server.py`
- 测试：
  - `tests/mcp/test_mcp_inventory_diff.py`

建议实现：

1. 把 `scripts/aistock_mcp_server.py::_compact_issue_item` 的逻辑抽到 shared util：

```python
def compact_issue_item(item: Mapping[str, Any]) -> dict[str, Any]:
    ...
```

2. `backend/mcp/modules/validation.py` 改为引用 shared util：

```python
from backend.mcp.validation_issue_items import compact_issue_item
```

3. 移除 `backend/mcp/modules/validation.py` 顶层 `from scripts import ...`。

4. 对 legacy BUG/GitHub wrapper：
   - 最小可接受方向：集中到 lazy adapter，确保 import `backend.mcp.modules.validation` 不触发 legacy script 顶层执行。
   - 更彻底方向：把这些 issue workflow helper 从 script 抽为 backend/mcp 共享模块，但这可能扩大 R1 scope；若超出边界应停止并上报。

新增测试建议：

```python
def test_mcp_modules_do_not_import_scripts_or_transitive_business_code() -> None:
    ...
```

检查点：

- AST 扫描 `backend/mcp/modules/*.py` 不允许 `import scripts` / `from scripts import ...`。
- import 所有 `backend.mcp.modules.*` 后，`sys.modules` 中不应新增 `scripts.aistock_mcp_server`。
- 保持原有 `test_mcp_modules_do_not_import_backend_services_directly` 通过。

## 5. Validation Center 接入计划

目标文件：

- `tests/aistock_validation/catalog/test_plans.yaml`
- `noxfile.py`

建议新增 nox session：

```python
@nox.session(venv_backend="none")
def mcp_gateway_manifest_quality(session: nox.Session) -> None:
    ...
```

建议执行内容：

- `python -m compileall backend/mcp scripts/aistock_mcp_gateway.py scripts/aistock_mcp_gateway_doctor.py`
- `python scripts/aistock_mcp_gateway.py --self-check --profile=lite`
- `python scripts/aistock_mcp_gateway_doctor.py --json`
- `pytest tests/mcp -q -p no:cacheprovider`

建议新增 plan：

```yaml
- plan_key: mcp_gateway_manifest_quality
  title: MCP Gateway manifest quality and import-boundary validation
  module: mcp_gateway
  level: L2
  command_key: nox_mcp_gateway_manifest_quality
  nox_session: mcp_gateway_manifest_quality
  enabled: true
  requires_backend: false
  requires_frontend: false
  allowed_backend_ports: []
  allowed_frontend_ports: []
  writes_database: false
  writes_artifacts: false
  writes_business_state: false
  runner_enabled: true
  max_duration_seconds: 300
  evidence_kinds: [pytest, compileall, mcp_self_check, mcp_doctor]
```

如 catalog integrity 要求 `module` 必须存在于 `module_registry.yaml`，需要按规范补最小 module registry 映射；不得硬绕过 catalog integrity。

## 6. 必跑验证

本地 gate：

```powershell
python scripts/aistock_mcp_gateway.py --self-check --profile=lite
python scripts/aistock_mcp_gateway_doctor.py --json
pytest tests/mcp -q
python -m compileall backend/mcp scripts/aistock_mcp_gateway.py scripts/aistock_mcp_gateway_doctor.py
```

`doctor` 要求：

- `static_no_llm.findings == []`

Validation Center gate：

- 使用 `start_validation_execution` 启动：
  - `plan_key=mcp_gateway_manifest_quality`
  - `runner_enabled=true`
  - 需要时指定 `workspace_path=F:\Dev\AIstock_worktrees\mcp-manifest-quality-20260604`
  - 需要时指定 `expected_branch=codex/mcp-manifest-quality-20260604`
- 使用 `get_validation_execution_status` 轮询。
- 必须得到：
  - `exit_code=0`
  - G1 green `run_id`

注意：不允许用生产 `8001` 做启动/停止/重启。若 Validation Center 当前不可用，不要自行杀进程；按用户授权边界处理。

## 7. 三闸门交付标准

### G1

- `mcp_gateway_manifest_quality` Validation Center run 绿，记录 `run_id`。
- `pytest tests/mcp -q` 绿。
- `self-check --profile=lite` 绿。
- `doctor --json` 绿且 `static_no_llm.findings=[]`。

### G2

按 `DESIGN-COMPLIANCE-001` 输出矩阵，至少包含：

- M1：risk override + no write-as-readonly 测试。
- M2：migration_state 非硬编码 + wrapper/deprecated 表达能力。
- M3：gateway module 不再 import legacy script 顶层。
- Validation plan 已登记并 runner_enabled。
- 无 POC / 简化 / mock-only / 占位。

### G3

- 回填 `docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md` 第 10 节验收矩阵相关行。
- 建议两步提交：
  1. 实现和测试 commit。
  2. 文档验收矩阵 commit，引用第 1 个 commit hash 和 Validation Center run_id。
- 提交信息结尾加 `Co-Authored-By`。
- PR 到 `main`，PR body 必须包含：
  - G1/G2/G3 状态
  - Validation Center run_id
  - 测试命令和结果
  - `production_ddl_gate=noop`
  - `production_frontend_dependency_gate=noop`
  - `production_backend_dependency_gate=noop`

## 8. 停止条件

遇到以下任一情况必须停止并 report_bug，不得弱化测试或以 POC 充真：

- 任一 side-effect 工具无法按要求标为非 `read_only` 或无法进入 `preflight_required`。
- M3 发现必须大范围迁移 BUG/GitHub workflow，超出 R1 gateway-only 边界。
- Validation Center runner 无法按 `8011/8012` 或 no-backend runner 机制验证。
- `doctor static_no_llm.findings` 非空且无法在本轮范围内正确修复。
- DESIGN-COMPLIANCE-001 任一项无法 `done=true`。
- 需要启动/停止/重启 `8001/3000/19080`。
- 发现需要 DB DDL 或生产 runtime 改动。

## 9. 建议继续命令

从新的 worktree 继续：

```powershell
Set-Location F:\Dev\AIstock_worktrees\mcp-manifest-quality-20260604
git status --short --branch
git log --oneline -5
python - <<'PY'
from collections import Counter
from backend.mcp.tool_manifest import TOOL_MANIFEST
print(len(TOOL_MANIFEST))
print(Counter(e.risk_level for e in TOOL_MANIFEST))
for e in TOOL_MANIFEST:
    if e.risk_level == "read_only" and any(t in e.tool_name for t in ("register", "deprecate", "promote", "retire", "bind", "apply", "toggle", "sync", "repair", "schedule", "report_bug", "assign", "update_bug", "start_validation_execution", "github_issue_create")):
        print(e.module, e.tool_name, e.risk_level, e.assistant_usable)
PY
```

PowerShell 不支持 bash 风格 heredoc。若使用 PowerShell，请改成 here-string：

```powershell
@'
from collections import Counter
from backend.mcp.tool_manifest import TOOL_MANIFEST
print(len(TOOL_MANIFEST))
print(Counter(e.risk_level for e in TOOL_MANIFEST))
'@ | python -
```

## 10. 未完成项

- 尚未实现 M1/M2/M3。
- 尚未新增 `mcp_gateway_manifest_quality` validation plan。
- 尚未运行最终验证。
- 尚未回填第 10 节验收矩阵。
- 尚未提交 commit 或创建 PR。

