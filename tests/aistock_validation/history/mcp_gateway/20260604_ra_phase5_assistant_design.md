# Research Assistant Phase 5 智能助手消费统一 MCP manifest/catalog 设计（第一阶段 recon）

- 日期：2026-06-04
- Worktree：`F:\Dev\AIstock_worktrees\ra-phase5-assistant-integration-20260604`
- Branch：`codex/ra-phase5-assistant-integration-20260604`
- Base：`9e1e0c00 chore(issue): close-sync BUG-258 after merge (#729)`；包含 R1 `9158cc56 / #727`
- Batch ID：`BATCH-RA-PHASE5-20260604`
- 本阶段状态：仅 recon + A/B/C 设计；不改功能代码、不改 DB、不启动/停止/重启 `8001/3000/19080`。

## 0. 强制阅读与源码证据

| 来源 | 已核对要点 |
|---|---|
| `docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md:81-102` | RA 红线：工具接地、证据优先、fail-fast、core 不得直接耦合 AIstock。 |
| `docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md:265-285` | `assert_tool_in_catalog` 是真实能力闸门；只读自动执行，写入/高风险仅 preflight + approval；core/adapter 解耦。 |
| `docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md:531-543` | ANTI-DRIFT-02/11：新增目录/API 必须被推理链路消费，core 不得 import adapter/domain。 |
| `docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md:547-570` | §12 矩阵必须回填实现位置、测试、commit/run_id。 |
| `docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md:581-600` | DAI：DAI-GND-001/002/003、DAI-DRIFT-001 是本阶段直接约束。 |
| `docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md:119-145` | manifest 字段模型；智能助手通过 catalog/risk/profile/preflight 适配，不默认 full、不后台 LLM。 |
| `docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md:253-269` | Phase 5 实施与验收：RA 读取统一目录、preflight risk metadata、任务审计、worker 默认 lite/catalog、UI 工具健康。 |
| `docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md:349-365` | §10 当前 R1 已 PASS，Phase 5 `智能助手读取统一 catalog` 仍 pending。 |
| `docs/process/research_assistant_blueprint_execution_runbook_20260531.md:23-63` | G1/G2/G3 防缩水三闸门；任一不满足必须停下，不得 POC/mock 充真。 |
| `docs/standards/aistock_development_standard_v1.5_20260523.md:58-79` | P0/P1 红线：生产端口、默认值伪成功、设计复核、根目录污染、可测试功能必须验证。 |
| `docs/standards/aistock_development_standard_v1.5_20260523.md:448-457` | DESIGN-COMPLIANCE-001：合入前逐项矩阵，不能简化、子集或只读冒充完整。 |
| `docs/standards/aistock_issue_fix_parallel_workflow_standard_20260514.md:19-28` | 独立 worktree/branch、禁止 sweeping commit、跨工具通讯 opt-in。 |

## 1. 当前实现 recon 结论

### 1.1 当前三处目录源

| 目录源 | 当前位置/证据 | 现状问题 |
|---|---|---|
| DB cache：`assistant_mcp_tools` | schema 在 `backend/db/init_research_assistant_schema_20260521.py:405-422`，包含 `status`、schema、preflight、confirmations；service seeding 在 `backend/services/research_assistant/service.py:1290-1298`。 | 适合做运行时缓存/开关 overlay，但不应继续作为工具身份与风险单一真相；旧 DB 可能残留 pre-R1 203 工具和 legacy server key。 |
| RA 种子：`mcp_catalog_sync.default_mcp_tools()` | `backend/services/research_assistant/mcp_catalog_sync.py:21-133` 手写 13 个 server 和 203 个工具；`_tool_metadata()` 在 `:203-244` 用本地启发式。 | 与 R1 manifest 209 工具漂移；风险/approval 逻辑重复且弱于 R1；还保留 `legacy_script` server 语义。 |
| Gateway manifest：`TOOL_MANIFEST` | `backend/mcp/tool_manifest.py:16-27` dataclass 字段；`TOOL_METADATA_OVERRIDES` 在 `:346-437`；`TOOL_MANIFEST` 在 `:550-551`；`validate_manifest()` 在 `:576-621`。 | R1 后 209 工具、risk/assistant_usable 已可作为源；仍需 A2 对只读误标做二次校准。 |

结论：Phase 5 必须让 RA adapter 层消费 `TOOL_MANIFEST`，并把 DB 表降级为派生缓存/运行时 overlay；`react_grounding.py` 等 core 继续只收纯 `ToolCatalogEntry` 数据。

### 1.2 当前 RA ReAct/core 消费链路

| 链路 | 源码证据 | 设计含义 |
|---|---|---|
| `ToolCatalogEntry` 是纯 dataclass | `backend/services/research_assistant/react_grounding.py:42-50` | 可由 adapter 注入 manifest 派生字段，不需要 core import `backend.mcp`。 |
| `assert_tool_in_catalog()` | `react_grounding.py:184-200` | catalog 外拒绝；当前只在 `risk_level == "low"` + `read_only` + no approval 时自动执行。 |
| ReAct loop | `react_grounding.py:348-420` | gate 决策为 `execute_read_only` 或 `preflight_confirmation_only`。 |
| Service 构造 catalog entries | `backend/services/research_assistant/service.py:3383-3397` | 当前从 DB `mcp_tools` 读 risk/side_effect/requires_approval；Phase 5 改为统一 manifest-derived catalog + runtime overlay。 |
| 只读自动执行 eligibility | `service.py:3494-3525` | 当前要求 DB tool risk=`low` + side_effect=`read_only`；A2 修复后必须能让真实只读 list/query 进入自动执行路径。 |
| preflight | `service.py:4165-4230` | 当前只从 DB tool/server 读；Phase 5 改为同一 manifest-derived 记录，事件写 `assistant_mcp_tool_events`。 |
| core 解耦测试 | `backend/tests/research_assistant/test_core_no_adapter_import.py:23-33` | 明确禁止 core import `backend.mcp`，本阶段必须保持绿。 |

### 1.3 当前 .mcp/profile 权威映射

`.mcp.json` 已由 R1 后主线改为统一 gateway：legacy standalone validation/qe scripts 不再注册；`backend/mcp/profiles.py:24-76` 定义 profile→modules。Phase 5 不新造 server_key 映射，而从 `.mcp.json` 的 `--profile` + `profiles.resolve_modules()` 推导。

| canonical server_key | `.mcp.json` profile | modules（由 `resolve_modules()` 推导） | 说明 |
|---|---|---|---|
| `aistock-gateway-lite` | `lite` | `catalog` | catalog 平台工具的默认 canonical server。 |
| `research-assistant` | `assistant` | `catalog`, `research_assistant` | RA 自身工具；catalog 在此 profile 可用，但 canonical catalog server 仍为 lite。 |
| `aistock-research` | `research` | `research` | 研究流水线。 |
| `aistock-local-data` | `data` | `local_data` | 用户点名的 `aistock-local-data` ↔ `local_data` 权威映射。 |
| `aistock-validation` | `validation` | `validation` | R1 后 gateway validation。 |
| `aistock-qe` | `qe` | `qe_experiment`, `qe_archive`, `model_registry` | 替代旧 `aistock-qe-experiment` / `aistock-qe-archive` / `aistock-model-registry` 作为 canonical profile server。 |
| `aistock-factor` | `factor` | `factor_library`, `factor_metrics`, `factor_correlation` | 替代旧三个 factor server。 |
| `aistock-trading-ops` | `trading_ops` | `strategy_governance`, `execution_policy` | 替代旧 strategy/execution server。 |
| `aistock-external-research` | `external_research` | `external_research` | 外部网络工具仍 preflight policy。 |

兼容策略：旧 RA 种子中的 `aistock-qe-experiment`、`aistock-qe-archive`、`aistock-factor-library` 等只作为 `legacy_server_aliases` 接受输入并归一到 canonical server，不再作为新目录源或新 DB 记录的主键。

## A. 任务卡设计

### A1：目录源收敛到 manifest-derived RA adapter catalog

**目标**

- `TOOL_MANIFEST` 成为 RA 工具身份、module、profile、risk、assistant_usable、requires_confirmation、response_budget、migration_state 的单一事实源。
- `mcp_catalog_sync.default_mcp_tools()` 改为从 `TOOL_MANIFEST` 派生；不再维护 `TOOL_NAMES_BY_SERVER` 作为独立工具清单。
- `assistant_mcp_tools` / `assistant_mcp_servers` 保留为派生缓存 + runtime overlay，不退役、不做 DDL。

**DB 表保留/退役决策**

| 问题 | 决策 | 原因/证据 |
|---|---|---|
| `assistant_mcp_tools` 是否退役 | Phase 5 不退役，保留为派生 cache/runtime overlay。 | schema 有 `status`、schema、confirmations、timestamps（`init_research_assistant_schema_20260521.py:405-422`），RA service/能力 sync 已依赖 repository catalog（`service.py:1366-1400`）。直接退役会扩大 DDL/API scope。 |
| `mcp_tools` 有 manifest 没有的 enable/disable/status | 作为 overlay，且只能收紧不能放松风险：`disabled/blocked/deprecated` 可阻断；DB 不能把 manifest 高风险降为 direct。 | `preflight_mcp_tool()` 当前检查 `tool.status` 和 `server.status`（`service.py:4177-4180`）；应保留运行时开关能力。 |
| usage/approval 状态 | 不进 manifest；usage 从 `assistant_mcp_tool_events` 聚合，approval 从 `assistant_approval_requests`/task events 追溯。 | `assistant_mcp_tool_events` 已有 `request_json/response_json/action_proposal_id/approval_id/result_card_json`（schema `:425-448`）。 |
| 旧 DB stale rows | 不作为目录真相；API/list/ReAct/preflight 使用 manifest-derived view，按 canonical key 去 overlay DB status；旧 rows 可被兼容 alias 读取，但不进入主列表。 | 防止 R1 前 203/13 server 残留导致 ANTI-DRIFT-02 失败。 |

**拟改 target files / signatures**

| 文件 | 计划改动 |
|---|---|
| `backend/services/research_assistant/mcp_catalog_sync.py` | 新增 `derive_gateway_server_catalog(config_path: Path | None = None) -> GatewayServerCatalog`、`server_key_for_module(module: str) -> str`、`canonicalize_server_key(server_key: str) -> str`、`manifest_entry_to_mcp_tool(entry: ToolManifestEntry, overlay: Mapping[str, Any] | None = None) -> dict[str, Any]`；`default_mcp_servers()` / `default_mcp_tools()` / `load_catalog()` 全部由 `.mcp.json` + `profiles` + `TOOL_MANIFEST` 派生。 |
| `backend/services/research_assistant/service.py` | 新增 `_manifest_mcp_catalog_records()` / `_manifest_mcp_catalog_page(...)` / `_resolve_mcp_catalog_tool(server_key, tool_name)`；`seed_catalogs()` 写入 manifest-derived canonical rows；`_mcp_tool_catalog_snapshot()`、`_react_tool_catalog_entries()`、`_read_only_mcp_auto_execution_eligibility()`、`preflight_mcp_tool()` 使用同一派生目录。 |
| `backend/routers/research_assistant.py` | `/mcp/tools` 改走 service 专用 `list_mcp_tools(...)`，返回 `source="gateway_manifest_derived_catalog"`、`manifest_tool_count`、`risk_distribution`、`profile_distribution`、`backend_health`/`last_smoke` 占位字段（真实值来自后端 health/smoke，不伪造）。 |
| `backend/services/research_assistant/domain_ontology.py` | 将 DomainSpec 的 server_key 改为 canonical `.mcp.json` key，旧 key 放 adapter alias；或在 route 输出前统一 canonicalize。核心要求：route 不能再指向目录中不存在的 legacy server。 |
| `backend/tests/research_assistant/test_mcp_catalog_sync.py` | 203/13 断言更新为 209/9 canonical server；新增旧 alias 不作为主目录真相的 drift 断言。 |
| 新增 `backend/tests/research_assistant/test_ra_manifest_catalog_consumption.py` | 覆盖 manifest↔RA catalog↔ReAct entries↔preflight 使用同一数据源。 |

**manifest -> ToolCatalogEntry 字段映射**

| Manifest 字段 | RA/ToolCatalogEntry 字段 | 映射规则 |
|---|---|---|
| `tool_name` | `tool_name` | 原样；全局唯一。 |
| `module` + `.mcp.json` profile | `server_key` | 由上表 canonical module→server_key 推导；旧 server_key 仅 alias canonicalize。 |
| `risk_level in {read_only, catalog}` | `risk_level="low"`, `side_effect_level="read_only"` | `requires_approval=False`；可进入自动执行。 |
| `assistant_usable="direct_or_catalog"` | `requires_approval=False` | 仅当 `risk_level in {read_only,catalog}` 时允许；测试锁死。 |
| `assistant_usable="preflight_required"` | `requires_approval=True` | 无论 risk 文案如何都必须 preflight；用户要求硬约束。 |
| `requires_confirmation=True` | `requires_approval=True`, `required_confirmations=[...]` | confirmations 优先取现有 `CONFIRMATIONS_BY_TOOL`；无映射时 fail-fast 或使用显式 adapter override，不用空默认掩盖。 |
| `risk_level="write_confirmed"` | `risk_level="production_sensitive"`, `side_effect_level="production_sensitive"` | 只能 preflight/approval；ReAct 不执行。 |
| `risk_level="long_running"` | `risk_level="high"`, `side_effect_level="high_cost_compute"` | 只能 preflight/approval；适用于 QE run、metrics submit、worker/sync。 |
| `risk_level="production_adjacent"` | `risk_level="high"`, `side_effect_level="write_nonprod"`（必要时 `production_sensitive`） | 默认 write/control-plane 风险；不得 direct。 |
| `risk_level="external_network"` | `risk_level="high"`, `side_effect_level="draft_only"` 或 `write_nonprod` | 由于当前策略要求外网走 preflight，搜索/list 也不自动执行，除非后续设计单独放宽。 |
| `backend_endpoint` / `response_budget` / `migration_state` / `profile_tags` | API enrichment / `preflight_schema_json.manifest` / UI | 不进 core；作为 pure data 给 UI、preflight、审计。 |

**ANTI-DRIFT-02 测试**

1. `default_mcp_tools()` canonical `{server_key, tool_name}` 集合由 `TOOL_MANIFEST` + `.mcp.json` 推导，数量 = 209；无手写 `TOOL_NAMES_BY_SERVER` 作为独立真相。
2. `load_catalog()["tool_count"] == len(TOOL_MANIFEST)`，并校验每个 manifest tool 有 canonical server。
3. `ResearchAssistantService.seed_catalogs()` 在 in-memory repository 写入 209 canonical tools；旧 legacy key 不进入主列表。
4. `/mcp/tools` 的 `manifest_tool_count` 与 `TOOL_MANIFEST` 一致；`items` 与 `_react_tool_catalog_entries()` 同源。
5. `preflight_mcp_tool()` 对高风险工具返回 manifest risk；对只读工具返回 passed；对 alias server_key 返回 canonical server_key 并记录 alias evidence。
6. 静态测试扫描 `react_grounding.py` / `memory_tree.py` 不 import `backend.mcp`，保持 `test_core_no_adapter_import.py` 绿。

### A2：只读过度分级修复 + 闸门联动

**目标**

R1 为了安全使用子串兜底，把部分纯 GET/list/query 工具误标为 `production_adjacent` / `long_running` / `preflight_required`。Phase 5 必须在 `TOOL_METADATA_OVERRIDES` 逐工具加证据理由，把真实只读工具恢复为 `read_only/direct_or_catalog`，让 RA 能自动调用它们做 grounding；写/确认/外网/长任务不得放松。

**当前误标证据（来自当前 manifest）**

运行 `TOOL_MANIFEST_BY_NAME` 检查，以下工具当前均为 `assistant_usable="preflight_required"`，但源码语义为只读：

| 工具 | 当前误标 | 后端语义证据 | 计划 override |
|---|---|---|---|
| `qe_archive_list_runs` | `production_adjacent/preflight_required` | MCP wrapper `client.get("/runs")`：`backend/mcp/modules/qe_archive.py:93-95`；repository docstring “Return recent archived runs”：`backend/services/qe_archive/repository.py:1325-1333`。 | `read_only/direct_or_catalog`；reason 写明 GET list archived runs，无写入。 |
| `qe_archive_get_run_quality` | `production_adjacent/preflight_required` | wrapper `client.get("/runs/{id}/quality")`：`backend/mcp/modules/qe_archive.py:97-100`；repository “Return row-count based completeness checks”：`backend/services/qe_archive/repository.py:1484-1485`。 | `read_only/direct_or_catalog`。 |
| `qe_archive_query_run_leaderboard` | `production_adjacent/preflight_required` | wrapper `client.get("/analytics/run-leaderboard")`：`backend/mcp/modules/qe_archive.py:274-285`；router GET：`backend/routers/qe_archive.py:392-409`；repository query method：`backend/services/qe_archive/repository.py:1900-1908`。 | `read_only/direct_or_catalog`。 |
| `local_data_list_sync_targets` | `long_running/preflight_required` | wrapper GET `/targets`：`backend/mcp/modules/local_data.py:209-221`；service response risk `read_only`：`backend/services/local_data_management.py:204-212`。 | `read_only/direct_or_catalog`。 |
| `local_data_list_schedules` | `production_adjacent/preflight_required` | wrapper GET `/schedules`：`backend/mcp/modules/local_data.py:427-439`；service `_source(...,"read_only")`：`backend/services/local_data_management.py:357-358`。 | `read_only/direct_or_catalog`。 |
| `list_validation_runs` | `production_adjacent/preflight_required` | wrapper GET `/runs`：`backend/mcp/modules/validation.py:58-69`；router GET list history：`backend/routers/validation.py:172-193`。 | `read_only/direct_or_catalog`。 |
| `get_validation_run` | `production_adjacent/preflight_required` | wrapper GET `/runs/{id}`：`backend/mcp/modules/validation.py:71-74`；router GET detail：`backend/routers/validation.py:196-204`。 | `read_only/direct_or_catalog`。 |

**同轮建议一起修复的只读候选**

这些不是用户示例中的 7 个，但当前同样被子串误标，且源码已能证明只读；建议 A2 一次性覆盖，避免 Phase 5 RA grounding 仍被卡住。

| 工具 | 只读证据 | 计划 |
|---|---|---|
| `local_data_get_sync_target` | wrapper GET `/targets/{id}`：`backend/mcp/modules/local_data.py:223-228`；service risk `read_only`：`backend/services/local_data_management.py:214-226`。 | override read_only/direct。 |
| `local_data_list_sync_attempts` | wrapper GET `/sync-attempts`：`backend/mcp/modules/local_data.py:230-243`；service risk `read_only`：`backend/services/local_data_management.py:228-236`。 | override read_only/direct。 |
| `local_data_get_schedule_defaults` | wrapper GET `/schedules/defaults`：`backend/mcp/modules/local_data.py:441-445`；service risk `read_only`：`backend/services/local_data_management.py:360-367`。 | override read_only/direct。 |
| `local_data_list_source_test_runs` | wrapper GET `/testing/runs`：`backend/mcp/modules/local_data.py:530-542`；router GET：`backend/routers/local_data.py:334-336`；service risk `read_only`：`backend/services/local_data_management.py:461-462`。 | override read_only/direct。 |
| `local_data_list_source_test_schedules` | wrapper GET `/testing/schedules`：`backend/mcp/modules/local_data.py:544-556`；router GET：`backend/routers/local_data.py:339-341`；service risk `read_only`：`backend/services/local_data_management.py:464-465`。 | override read_only/direct。 |
| `local_data_get_repair_status` | service response risk `read_only`：`backend/services/local_data_management.py:550-557`。 | override read_only/direct。 |
| `qe_archive_list_backfill_runs` | wrapper GET `/backfill/runs`：`backend/mcp/modules/qe_archive.py:204-206`；router GET：`backend/routers/qe_archive.py:204-209`。 | override read_only/direct。 |
| `qe_archive_get_backfill_run` | wrapper GET `/backfill/runs/{id}`：`backend/mcp/modules/qe_archive.py:208-211`；router GET：`backend/routers/qe_archive.py:212-214`。 | override read_only/direct。 |

**不得放松的边界**

- `mcp_github_issue_list` 当前 `external_network/preflight_required`，虽然名称是 list，但涉及 GitHub 外网；本轮不降级，除非后续有明确外网只读自动执行设计。
- `update_bug_status` 名称含 update 且是 BUG 状态写入，不降级。
- `*_confirmed`、`register`、`deprecate`、`promote`、`retire`、`bind`、`apply`、`toggle`、`sync`、`repair`、`schedule`、`report_bug`、`assign`、`start_validation_execution`、`github_issue_create` 等写/确认/长任务/外网 token 测试不得弱化。

**拟改 target files / tests**

| 文件 | 计划改动 |
|---|---|
| `backend/mcp/tool_manifest.py` | 对上表只读工具补 `TOOL_METADATA_OVERRIDES`，每条 reason 一行后端语义证据；保持 R1 write-token guard。 |
| `tests/mcp/test_mcp_tool_manifest.py` | 扩展 `test_manifest_risk_no_write_as_readonly`：写/确认 token 不可 read_only；只读豁免必须在 override 表内且 reason 含 GET/read-only 证据，不允许无理由白名单。新增 `test_manifest_readonly_grounding_overrides_are_direct`。 |
| `backend/tests/research_assistant/test_tool_catalog_gate.py` / 新增 RA catalog consumption 测试 | 断言 A2 工具进入 `ToolCatalogEntry` 后 `risk_level="low"`、`side_effect_level="read_only"`、`requires_approval=False`，ReAct 自动执行路径被调用。 |

### A2 闸门联动：assert_tool_in_catalog / preflight 由收敛 risk 元数据驱动

**设计**

- `react_grounding.py` 不 import manifest；adapter 在 `service.py` 生成纯 `ToolCatalogEntry`。
- ReAct 自动执行唯一条件：`side_effect_level == "read_only"` 且 `requires_approval is False` 且 `risk_level == "low"`。manifest 的 `read_only/catalog + direct_or_catalog` 会映射成该组合。
- `assistant_usable="preflight_required"`、`requires_confirmation=True` 或 runtime status 非 enabled/approved/ready 均让 `assert_tool_in_catalog()` 返回 `preflight_confirmation_only` 或 rejected。
- `preflight_mcp_tool()` 与 `_react_tool_catalog_entries()` 必须调用同一个 manifest-derived resolver，防止 list 显示可用但 ReAct/preflight 走旧 DB 风险。

**消费断言**

| 断言 | 测试落点 |
|---|---|
| 高风险工具在 ReAct 内不自动执行 | 现有 `test_high_risk_tool_creates_preflight_card_without_execute` 扩展到 manifest canonical server key。 |
| A2 只读工具能自动执行 | 新增参数化测试，构造 LLM 调用 `qe_archive_query_run_leaderboard` / `local_data_list_sync_targets` / `list_validation_runs`，provider 的 `execute_read_only()` 必须被调用。 |
| preflight 返回同一 risk metadata | `assistant_preflight_mcp_tool` 对 high-risk 返回 approval_required；对 A2 readonly 返回 passed；返回 `manifest_source` / canonical profile。 |
| worker 不自动 full profile | `test_worker_tool_isolation.py` 扩展：worker scoped catalog 不含 full profile，仅含 allowed canonical server/tool；高风险仍 preflight。 |

### A4：list/preflight 重指 + 审计 + UI

**后端/API**

- `/api/v1/research-assistant/mcp/tools` 改为 unified catalog view：`source="gateway_manifest_derived_catalog"`，返回 canonical `server_key`、`module`、`profile_tags`、`risk_level`（RA 映射后）、`manifest_risk_level`、`assistant_usable`、`requires_approval`、`backend_endpoint`、`migration_state`、`response_budget`、`status`、`detail_available`。
- 列表 summary 增加 `manifest_tool_count`、`risk_distribution`、`profile_distribution`、`server_count`、`backend_health`、`recent_smoke`；若无法取得真实 health/smoke，字段必须显式 `unknown/not_run`，不能伪造 pass。
- `assistant_preflight_mcp_tool` 返回 gateway risk metadata、确认字段、profile 建议、backend dependency、预计 side effect；事件写 `assistant_mcp_tool_events.response_json`，含 `profile`、`module`、`canonical_server_key`、`legacy_server_alias`（如有）、`manifest_entry` digest、`approval_required`、`evidence_refs`。
- 任务审计：有 `task_id` 时继续写 `TaskEventCreate`，payload 中加入 `profile/tool/preflight/approval/evidence`，满足 Phase 5 §7.3/§7.5。

**前端/UI**

当前 `frontend/src/app/research-assistant/mcp-tools/page.tsx:229-242` 拉 `/mcp/servers` + `/mcp/tools`；`ToolTable` 仅显示风险/状态/审批（`:197-220`）。Phase 5 扩展为：

- 顶部显示 profile、manifest tool count、risk distribution、backend health、recent smoke。
- 工具搜索/filter 使用 unified `/mcp/tools?search=&risk_level=&server_key=`，compact 默认不展开 schema。
- Preflight 面板显示：canonical server/profile、manifest risk、requires_confirmation、missing confirmations、approval pending、event/task evidence refs。
- chat 卡片保持 summary-first：只读自动执行显示执行证据；写/高风险显示 approval pending，不显示“已执行”。
- Playwright 覆盖工具搜索、profile recommendation、preflight、approval pending、证据展示。

**PR 切分建议**

默认建议第二阶段用一个 PR、两步提交完成：

1. 后端收敛 + A2 风险修复 + tests/Validation Center。
2. UI + docs/G3 回填 + completion report。

理由：Phase 5 验收要求 API/E2E/审计一起证明，拆成两个 PR 容易出现“后端已完成但 UI/消费断言未完成”的 DESIGN-COMPLIANCE 风险。若 Claude 要求拆分，必须拆为 5a/5b 两个独立过 G1/G2/G3 的 PR，并明确 5a 不声明 Phase 5 完整完成。

## B. 验证计划草案：`plan_key=mcp_gateway_phase5_assistant`

### B1. Validation Center catalog 设计

实施阶段新增：

```yaml
- plan_key: mcp_gateway_phase5_assistant
  title: MCP Gateway Phase 5 Research Assistant manifest catalog consumption gate
  module: research_assistant
  level: L4
  command_key: nox_mcp_gateway_phase5_assistant
  nox_session: mcp_gateway_phase5_assistant
  enabled: true
  requires_backend: false
  requires_frontend: true
  allowed_backend_ports: [8011, 8012]
  allowed_frontend_ports: [3011, 3012]
  forbidden_ports: [8001, 3000, 19080]
  writes_database: false
  writes_artifacts: true
  writes_business_state: false
  runner_enabled: true
  max_duration_seconds: 2400
  evidence_kinds: [pytest, compileall, mcp_self_check, mcp_doctor, playwright, catalog_integrity, ownership]
  manual_review_controls: [DESIGN-COMPLIANCE-001, production_ddl_gate, live_smoke_boundary]
```

同步新增/更新：

- `backend/services/validation/plan_catalog.py`：`"nox_mcp_gateway_phase5_assistant": "mcp_gateway_phase5_assistant"`。
- `noxfile.py`：新增 session，至少运行 compileall、`pytest tests/mcp`、RA backend targeted tests、`test_core_no_adapter_import.py`、gateway self-check/doctor、Playwright mocked UI gate、catalog integrity、ownership scan。
- 必要时在 `module_registry.yaml` / `file_ownership.yaml` 增加新测试/adapter 文件映射；不得绕过 catalog integrity。

### B2. 锁死消费断言

| 编号 | 断言 | 目标测试 |
|---|---|---|
| B-01 | `default_mcp_tools()`、RA `/mcp/tools`、`_react_tool_catalog_entries()` 均来自统一 manifest；数量与 `TOOL_MANIFEST` 一致；旧手写 `TOOL_NAMES_BY_SERVER` 不再作为源。 | `backend/tests/research_assistant/test_ra_manifest_catalog_consumption.py::test_ra_catalog_matches_gateway_manifest_without_legacy_drift` |
| B-02 | `assistant_list_mcp_tools` 返回 canonical profile/server/module/risk distribution，且 `source="gateway_manifest_derived_catalog"`。 | API test + service in-memory test。 |
| B-03 | 写/确认/长任务/生产邻近/外网工具在 ReAct 内只产 preflight/action proposal，不自动执行。 | `test_tool_catalog_gate.py` 扩展 + worker isolation。 |
| B-04 | A2 修复的只读 list/query/get 工具 `requires_approval=False`，可进入 `execute_read_only()` 自动执行路径。 | 参数化 ReAct test：`qe_archive_query_run_leaderboard`、`local_data_list_sync_targets`、`list_validation_runs`。 |
| B-05 | `assistant_preflight_mcp_tool` 和 list 使用同一 resolver；alias server_key 会 canonicalize，risk 不能被 DB overlay 降级。 | `test_preflight_uses_manifest_risk_and_canonical_server_key`。 |
| B-06 | `chat_turn` / 后台 worker 不自动启动 full profile、Claude/Codex/LLM CLI 或独立 MCP full server。 | static grep + monkeypatch `subprocess.Popen`/`spawn` 断言未调用；runtime config/agent worker tests。 |
| B-07 | `backend/tests/research_assistant/test_core_no_adapter_import.py` 继续通过；`react_grounding.py` / memory core 不 import `backend.mcp.tool_manifest`。 | 原测试 + 新增 explicit import scan。 |
| B-08 | UI 覆盖工具搜索、profile recommendation、preflight、approval pending、执行证据展示。 | `frontend/tests/research-assistant/research-assistant.spec.ts` 或新增 phase5 spec，使用 3011/3012，不触碰 3000。 |
| B-09 | doctor/self-check 静态无 LLM/daemon/full-default findings。 | `python scripts/aistock_mcp_gateway.py --self-check --profile=lite`、`python scripts/aistock_mcp_gateway_doctor.py --json`。 |

### B3. 本地 gate 命令（第二阶段执行）

```powershell
python -m compileall -b backend/mcp backend/services/research_assistant backend/routers/research_assistant.py scripts/aistock_mcp_gateway.py scripts/aistock_mcp_gateway_doctor.py
python scripts/aistock_mcp_gateway.py --self-check --profile=lite
python scripts/aistock_mcp_gateway_doctor.py --json
pytest tests/mcp backend/tests/research_assistant/test_mcp_catalog_sync.py backend/tests/research_assistant/test_tool_catalog_gate.py backend/tests/research_assistant/test_worker_tool_isolation.py backend/tests/research_assistant/test_core_no_adapter_import.py -q -p no:cacheprovider
python -m nox -s mcp_gateway_phase5_assistant
```

Playwright 在 nox session 内或显式命令中使用 3011/3012；禁止使用生产 `3000`。

## C. closure_requirements 草案

实施阶段 completion report 必须逐项 `done=true`；第一阶段仅定义闭环，不声明完成。

| closure_id | requirement | evidence_required |
|---|---|---|
| CR-P5-01 | RA catalog 单一事实源改为 `TOOL_MANIFEST`；`default_mcp_tools()` 不再手写 203 工具。 | diff + `test_ra_catalog_matches_gateway_manifest_without_legacy_drift`。 |
| CR-P5-02 | `assistant_mcp_tools` 明确为派生缓存/runtime overlay；无 DDL；DB status 只能收紧风险，不能降级。 | service resolver tests；completion report `production_ddl_gate=noop`。 |
| CR-P5-03 | server_key↔module 从 `.mcp.json` + `profiles.resolve_modules()` 推导；旧 server key 仅兼容 alias。 | mapping test + `.mcp.json` fixture/assert。 |
| CR-P5-04 | manifest taxonomy 映射到 `ToolCatalogEntry`，`assistant_usable=preflight_required => requires_approval=True`。 | 参数化 mapping tests。 |
| CR-P5-05 | A2 只读误标工具全部按真实后端语义加 override；每条 override 有一行 reason。 | `test_manifest_readonly_grounding_overrides_are_direct` + completion A2 table。 |
| CR-P5-06 | R1 写/确认/长任务/外网 safety test 未削弱；写操作不得 read_only/direct。 | `test_manifest_risk_no_write_as_readonly` 绿。 |
| CR-P5-07 | ReAct 自动执行只允许 low/read_only/no approval；高风险只出 preflight/action proposal。 | `test_tool_catalog_gate.py` / worker tests。 |
| CR-P5-08 | `assistant_list_mcp_tools` / `assistant_preflight_mcp_tool` 使用同一 manifest-derived resolver。 | service/API tests + event payload evidence。 |
| CR-P5-09 | 任务审计记录 profile/tool/preflight/approval/evidence refs。 | `assistant_mcp_tool_events` / `TaskEventCreate` test。 |
| CR-P5-10 | 后台 worker/chat_turn 不自动启动 full profile 或 LLM CLI。 | static grep + subprocess monkeypatch tests。 |
| CR-P5-11 | core 解耦：RA core 不 import `backend.mcp` / AIstock domain adapter。 | `test_core_no_adapter_import.py`。 |
| CR-P5-12 | UI 展示 profile/tool count/risk distribution/backend health/recent smoke，覆盖搜索/preflight/approval/evidence。 | Playwright output + screenshots/log snippets if generated。 |
| CR-P5-13 | Validation Center `mcp_gateway_phase5_assistant` G1 green run_id。 | `run_id` + `exit_code=0`。 |
| CR-P5-14 | G3 回填 gateway doc §7/§10 与 RA 蓝图 §12，带 commit hash。 | doc diff + commit hash。 |
| CR-P5-15 | `production_ddl_gate=noop`、`frontend_dependency_gate=noop`、`backend_dependency_gate=noop`，生产 runtime 未触碰。 | completion report gates。 |
| CR-P5-16 | 无 POC/mock/占位/缩水；mock UI 只能补充交互，后端业务路径有真实 in-memory/service/API 断言。 | DESIGN-COMPLIANCE-001 item-by-item 矩阵。 |

## 停止条件检查（第一阶段）

- 三源收敛冲突：未触发。`assistant_mcp_tools` 的 runtime/status/approval 使用 overlay 可安放，无需 DDL。
- 只读工具真实语义无法判定：未触发。用户点名 7 个及建议补充候选均找到 GET/read_only 证据；外网/GitHub list 不放松。
- core import 适配符号：未触发。设计中 manifest 消费仅在 adapter 层，`ToolCatalogEntry` 继续纯数据。
- 生产端口/DB DDL/生产 runtime：未触发。本阶段不启动服务、不做 DDL。
- DESIGN-COMPLIANCE 任一项无法 done=true：未触发；但须在第二阶段按 C 矩阵逐项证明。

## 对后续 A3/standalone 退役的影响

- 本阶段不做 `stock_analysis` 模块（A3 后续方案）。
- Phase 5 完成后 RA 将消费 canonical `aistock-qe` / `aistock-factor` / `aistock-trading-ops` 等 profile server；旧 standalone/legacy server_key 只能作为 alias，不能再作为主目录源。
- standalone 默认退役仍按 gateway doc §10/§11 后续条目执行；Phase 5 只保证 RA 目录消费、risk/preflight、UI/审计链路收敛。

## 第一阶段交付边界

- 本文件是第一阶段唯一交付物。
- 未提交、未开 PR、未改功能代码。
- 等 Claude/用户确认 A/B/C 后，第二阶段才开始实现与验证。
