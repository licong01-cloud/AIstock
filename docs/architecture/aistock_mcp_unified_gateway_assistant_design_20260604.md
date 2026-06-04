# AIstock MCP 统一 Gateway 与智能助手适配设计方案（2026-06-04）

## 1. 背景与结论

AIstock 当前 MCP 已经部分进入统一 gateway 形态，但仍同时存在多个独立脚本型 MCP server。长期目标不是把所有工具简单塞进一个默认全量 server，而是建设“统一 gateway 平台 + 小默认 profile + 任务 profile + 工具目录/预检/验收矩阵”的 MCP 能力层。

结论：该方案同样适用于未来智能助手（Research Assistant 以及后续 AIstock 智能助手）。智能助手不应默认加载全量工具 schema，也不应在后台自动启动会消耗模型 token 的 worker；它应通过统一 gateway 的工具目录、风险元数据、profile 建议和预检结果来选择能力，并在明确任务上下文中启用对应 profile 或调用确认型工具。

## 2. 现状证据

### 2.1 当前 MCP 注册形态

当前 `.mcp.json` 同时注册三类能力：

1. 独立脚本型 MCP：
   - `aistock-validation` -> `scripts/aistock_mcp_server.py`
   - `aistock-qe-experiment` -> `scripts/aistock_qe_experiment_mcp_server.py`
   - `aistock-qe-archive` -> `scripts/aistock_qe_archive_mcp_server.py`
2. 已接入 gateway 的 profile/module：
   - `aistock-research` -> `scripts/aistock_mcp_gateway.py --modules=research`
   - `aistock-local-data` -> `--profile=local_data`
   - `research-assistant` -> `--profile=research_assistant`
   - factor/model/strategy/execution/external-research 等 profile
3. 所有 server 均指向本机 loopback 后端，符合“业务服务留在 FastAPI 审计路径内”的方向。

### 2.2 当前 gateway 基础

现有 `backend/mcp/gateway.py` 已经具备以下基础：

- `create_gateway(profile, modules, base_url, ...)` 解析 profile/module 并创建 `FastMCP`。
- 通过 `importlib.import_module("backend.mcp.modules.<module>")` 动态加载模块。
- 要求模块提供 `register(registry)`。
- `ModuleRegistry` 提供 loopback API client、参数净化、确认校验、工具数量统计。
- `backend/mcp/common.py` 已有 loopback URL 限制、响应大小保护和“过大 payload 不返回 partial”的安全策略。

### 2.3 当前差距

`backend/mcp/profiles.py` 中仍将 `validation`、`qe_experiment`、`qe_archive` 标为 `SCRIPT_BACKED_SERVERS`，说明三者尚未进入统一 gateway 模块。基于当前仓库统计：

| 分类 | 模块/脚本 | 当前工具数量 |
| --- | --- | ---: |
| gateway-backed | research、research_assistant、local_data、factor_*、model_registry、strategy_governance、execution_policy、external_research | 130 |
| script-backed | validation | 19 |
| script-backed | qe_experiment | 26 |
| script-backed | qe_archive | 28 |
| 合计 | 当前需治理的 MCP 工具全集 | 203 |

因此，如果默认暴露 full profile，会把 200+ 个工具 schema 注入客户端上下文；如果继续多个独立 server，每个 Codex/Claude/智能助手会话又会重复启动多组 Python/MCP 进程。两者都不是长期最优。

## 3. 设计目标

1. **统一入口**：所有 AIstock MCP 最终经 `scripts/aistock_mcp_gateway.py` 和 `backend/mcp` 平台注册、发现、预检和运行。
2. **低资源默认**：默认 profile 只暴露少量 `lite/catalog/health/preflight` 工具，不默认暴露 full 工具集。
3. **任务 profile 化**：按任务启用 `research`、`assistant`、`data`、`qe`、`validation`、`factor`、`trading_ops` 等 profile。
4. **智能助手适配**：智能助手通过工具目录、risk metadata、profile recommendation、preflight 和 approval gate 使用 MCP，不直接依赖散落 server。
5. **完整迁移**：当前 203 个工具必须全部纳入工具清单、profile 映射、迁移状态和验收矩阵；任何删除/合并/改名必须有明确兼容策略和用户确认。
6. **无后台 token 消耗**：AIstock MCP gateway 本身不得后台启动 Claude/Codex/其他 LLM CLI，不得无用户任务触发模型调用。
7. **安全可审计**：写操作、长任务、生产邻近能力必须保留 plan/confirmed、expected value、job status、审计事件和真实业务路径验证。
8. **不绕过后端**：MCP wrapper 保持薄层，业务状态转换走 FastAPI/backend service，不直接导入业务服务或写生产 DB。

## 4. 非目标与边界

1. 本设计文档不直接实现运行时代码、DB migration 或生产调度变更。
2. 本设计不要求所有客户端共享一个常驻进程；短期仍兼容 MCP stdio per-client 运行模型。
3. 本设计不要求默认启用 full profile；full 仅作为人工调试或受控验证 profile。
4. 本设计不改变 Research Assistant 的业务 API，只补充它和统一 gateway 的适配方式。
5. 本设计不引入 MCP 内部 LLM agent；如智能助手自身需要 LLM，应由助手任务显式触发并记录 token/任务审计。

## 5. 目标架构

### 5.1 分层

```text
Codex / Claude Code / Research Assistant / future assistants
        |
        | MCP stdio or future controlled transport
        v
scripts/aistock_mcp_gateway.py
        |
        v
backend/mcp/gateway.py
  - profile resolver
  - tool catalog
  - module manifest loader
  - risk/preflight adapter
  - response budget guard
        |
        v
backend/mcp/modules/<module>.py
  - thin wrapper
  - parameter validation
  - confirm gate
  - loopback API calls only
        |
        v
FastAPI backend 127.0.0.1:8001 /api/v1
  - permissions
  - approvals
  - audit events
  - business services
  - DB/job side effects
```

### 5.2 Profile 策略

| Profile | 默认启用 | 面向客户端 | 模块 | 设计目的 |
| --- | --- | --- | --- | --- |
| `lite` | 是 | Codex、Claude、智能助手后台 worker | `catalog`、`health`、`preflight` | 低资源默认入口，只提供发现、健康和风险预检 |
| `assistant` | 按需 | Research Assistant / 智能助手 | `research_assistant` + `catalog` | 让助手读取工具目录、构建 context pack、创建候选 issue、发起 preflight |
| `research` | 按需 | 研究任务 | `research` | Research Pipeline 实验、阶段、artifact 查询 |
| `data` | 按需 | 数据健康/同步任务 | `local_data` | 本地数据健康、同步、修复计划和确认型执行 |
| `qe` | 按需 | QE 诊断/运行任务 | `qe_experiment`、`qe_archive`、`model_registry` | QE 实验控制、archive 查询、模型注册/对比 |
| `validation` | 按需 | issue/质量门禁任务 | `validation` | validation plans/runs/findings/BUG/GitHub issue sync |
| `factor` | 按需 | 因子研究 | `factor_library`、`factor_metrics`、`factor_correlation` | 因子检索、指标任务、相关性分析 |
| `trading_ops` | 按需 | 策略治理/执行策略 | `strategy_governance`、`execution_policy` | 策略包、执行策略绑定、退役和 readiness |
| `external_research` | 按需 | 外部研究采证 | `external_research` | 外部检索、论文/网页摘要、证据保存 |
| `full` | 否 | 人工调试/覆盖率验证 | 全部模块 | 验证所有工具完整注册，不作为默认配置 |

### 5.3 工具状态模型

每个工具必须在 manifest 中登记以下字段：

| 字段 | 说明 |
| --- | --- |
| `tool_name` | MCP 工具名，必须全局唯一 |
| `module` | 所属 gateway module |
| `profile_tags` | 可被哪些 profile 暴露 |
| `risk_level` | `read_only`、`write_confirmed`、`long_running`、`production_adjacent`、`external_network` 等 |
| `backend_endpoint` | 对应 FastAPI 路径或脚本兼容入口 |
| `requires_confirmation` | 是否必须 expected/confirmed 参数 |
| `response_budget` | summary/detail/pagination 策略 |
| `assistant_usable` | 智能助手是否可直接使用，或只能 preflight/人工确认 |
| `migration_state` | `gateway`、`script_backed`、`wrapper_compat`、`deprecated_pending_approval` |
| `acceptance_refs` | 对应测试、smoke、API/DB evidence |

### 5.4 智能助手适配确认

统一 gateway 方案适合智能助手，理由如下：

1. **工具发现更安全**：智能助手先读取 `tool_catalog/search`，不用把 203 个工具 schema 全部放入对话上下文。
2. **任务上下文更明确**：助手可以按任务请求 `profile_recommendation`，例如“QE 诊断”启用 `qe`，“数据健康”启用 `data`。
3. **预检可审计**：助手调用高风险工具前必须走 `assistant_preflight_mcp_tool` 或 gateway preflight，记录风险、确认字段、预估 side effect。
4. **后台 worker 更轻**：智能助手后台任务默认只允许 `lite` 或 catalog-only 能力，不启动全量 MCP，不启动模型 CLI。
5. **长期 UI 更清晰**：Research Assistant Console 可以显示 profile、工具数量、风险等级、依赖后端健康和最近 smoke 状态。
6. **兼容当前实现**：现有 `research_assistant` 模块已经有 `assistant_list_mcp_tools` 与 `assistant_preflight_mcp_tool`，设计上只需要把数据源从散落配置升级为统一 manifest/catalog。

## 6. 失败模式与防护

| 失败模式 | 风险 | 防护 |
| --- | --- | --- |
| 默认启用 full profile | 工具 schema 过大、模型误选工具、token 增加 | 默认只启用 `lite`；full 仅调试；配置检查阻断 full 作为默认 |
| standalone 工具遗漏 | 迁移后能力缺失 | tool inventory diff：旧脚本工具名必须全部映射到 manifest 和 gateway module |
| 工具改名破坏客户端 | Claude/Codex/助手旧配置不可用 | 先保留 wrapper alias；改名必须有兼容期和 deprecation 记录 |
| MCP 内部启动 LLM | 后台 token 消耗不可控 | 禁止 gateway/module 启动 `claude`、`codex`、`bun daemon`、LLM worker；增加静态 grep gate |
| 写操作缺少确认 | 误触发生产邻近动作 | `*_confirmed`、expected id、plan/apply 分离、backend approval gate |
| 大 payload 截断误导 | 模型基于 partial 数据误判 | 继续使用 response budget guard，过大返回 `requires_refinement`，不返回 partial |
| 智能助手绕过 preflight | 高风险操作无法审计 | assistant 只能通过 preflight-approved tool call；高风险工具必须人工确认 |
| 直接导入业务服务 | 绕过 API 权限/审计 | MCP module 只走 loopback FastAPI；测试 grep 禁止业务 service locator |
| 多客户端重复进程 | CPU/内存上升 | 减少 server 数量；默认低工具 profile；未来可评估受控 HTTP transport |

## 7. 分阶段实施方案与验收标准

### Phase 0：设计交付和基线冻结

**实施内容**

1. 完成本设计文档并合入 `origin/main`。
2. 记录当前 MCP 工具全集：gateway-backed 130 个、script-backed 73 个、合计 203 个。
3. 明确智能助手适配原则：默认 `lite`、按任务 profile、preflight/approval、禁止后台 LLM。

**验收标准**

- 文档位于 `docs/architecture/`。
- 文档包含背景、范围、现状差距、目标架构、失败模式、测试方案、数据验证方式、阶段验收标准和后续实施边界。
- 本阶段只修改设计文档，不改运行时代码、不改 DB、不重启生产服务。
- GitHub `origin/main` 包含该设计文档。

### Phase 1：Manifest、Profile Registry 与 Catalog 基础

**实施内容**

1. 新增统一工具 manifest 数据结构，可用 Python dataclass 或 JSON/YAML，但必须由测试验证。
2. 给所有当前 203 个工具建立 manifest 记录。
3. 在 `backend/mcp/profiles.py` 中新增 `lite`、`assistant`、`data`、`qe`、`validation`、`factor`、`trading_ops` 等正式 profile。
4. 新增 `catalog` module，至少提供：
   - `mcp_gateway_health`
   - `mcp_gateway_list_profiles`
   - `mcp_gateway_list_modules`
   - `mcp_gateway_list_tools`
   - `mcp_gateway_search_tools`
   - `mcp_gateway_preflight_tool`
5. `catalog` 工具读取 manifest，不要求 import 所有业务 module。

**验收标准**

- `python scripts/aistock_mcp_gateway.py --profile=lite --list-tools` 能列出 lite 工具，且工具数控制在 10 个以内。
- `python scripts/aistock_mcp_gateway.py --profile=full --list-tools` 能列出 203 个既有业务工具以及新增 catalog 平台工具；输出必须分别报告 legacy/platform tool count。
- `pytest tests/mcp/test_gateway_profiles.py -q` 覆盖 profile 解析、重复工具名、未知 profile、full 非默认。
- `pytest tests/mcp/test_mcp_tool_manifest.py -q` 验证 manifest 中没有缺失 `risk_level`、`assistant_usable`、`migration_state`。
- inventory diff 证明当前旧脚本 73 个工具均已在 manifest 中登记。

### Phase 2：低资源默认配置与客户端切换方案

**实施内容**

1. 更新 `.mcp.json`，新增或保留 `aistock-gateway-lite` 作为默认推荐 server。
2. 保留任务 profile 注册项，但不把 `full` 设为默认。
3. 为 Codex/Claude/智能助手提供 profile 切换说明或脚本，避免用户手动编辑大量配置。
4. 对客户端配置增加检查脚本，识别 stale worktree、错误端口、full 默认启用、script-backed 遗留默认启用。

**验收标准**

- 默认配置只启用 `lite` 或少量任务 profile，不默认暴露 full。
- 配置检查脚本能指出：当前 profile、工具数、base_url、是否指向 loopback、是否存在 stale worktree。
- 打开一个新 Codex/Claude 会话时，默认 AIstock MCP 进程数下降到可解释范围，不再按 10+ server 重复拉起。
- 文档明确说明：切换 profile 后需要重启对应客户端会话才能重新注入 MCP tools。

### Phase 3：Validation MCP 迁移

**实施内容**

1. 将 `scripts/aistock_mcp_server.py` 中 validation/BUG/GitHub issue 相关 19 个工具迁入 `backend/mcp/modules/validation.py`。
2. 旧脚本改为兼容 wrapper 或保留只读 deprecation 提示，迁移期不破坏现有客户端。
3. 保留 `AISTOCK_VALIDATION_BASE_URL` 和 `GITHUB_REPOSITORY` 的兼容处理，统一映射到 gateway env。
4. 所有 GitHub issue 创建/同步仍走原 validation API 或受控 gh fallback，不在 MCP module 中直接扩散业务逻辑。

**验收标准**

- `--profile=validation --list-tools` 列出旧 validation 19 个工具，工具名不缺失。
- 原 `aistock-validation` 入口在兼容期仍可启动或明确提示新入口。
- targeted tests 覆盖 health、list plans/runs/findings、BUG context、GitHub issue list/search/create/sync。
- GitHub auth 缺失时返回结构化错误或 fallback 指引，不静默成功。
- DESIGN-COMPLIANCE 矩阵逐项列出 19 个工具迁移状态。

### Phase 4：QE Experiment 与 QE Archive 迁移

**实施内容**

1. 将 `scripts/aistock_qe_experiment_mcp_server.py` 迁入 `backend/mcp/modules/qe_experiment.py`。
2. 将 `scripts/aistock_qe_archive_mcp_server.py` 迁入 `backend/mcp/modules/qe_archive.py`。
3. `qe` profile 默认组合 `qe_experiment`、`qe_archive`、`model_registry`。
4. 所有 list/query 类工具默认 summary/compact/paginated，detail/full 必须显式请求。
5. 长任务类工具继续使用 `_confirmed`、job id、status/log tail 查询，不直接阻塞返回超大结果。

**验收标准**

- `--profile=qe --list-tools` 至少列出旧 QE experiment 26 个工具和 QE archive 28 个工具，并包含 model registry 工具。
- inventory diff 证明旧 QE 54 个工具没有遗漏。
- targeted tests 覆盖 list/get/status/logs/metrics/run_confirmed/stop_confirmed/archive backfill/query/promotion candidate。
- 大 payload smoke 返回 `requires_refinement` 而不是 partial payload。
- QE 相关工具不直接导入 QE 业务 service，不绕过 `/api/v1` loopback API。

### Phase 5：智能助手集成

**实施内容**

1. Research Assistant 的 `assistant_list_mcp_tools` 改为读取统一 manifest/catalog。
2. `assistant_preflight_mcp_tool` 返回 gateway risk metadata、确认字段、profile 建议、backend dependency、预计 side effect。
3. 智能助手任务卡记录本次使用的 profile、工具、preflight 结果、approval id 和 evidence refs。
4. 后台 worker 默认只能使用 `lite` 和 catalog-only 能力；需要实际执行高风险工具时必须进入用户可见任务上下文。
5. UI 显示 profile/tool health：profile 名称、工具数、风险分布、后端连通状态、最近 smoke 结果。

**验收标准**

- Research Assistant 能列出统一 gateway 工具目录，且工具数量与 manifest 一致。
- 高风险工具在助手中未通过 preflight/approval 时不可执行。
- `assistant_chat_turn` 或后台 worker 不会自动启动 MCP full profile，也不会后台启动 Claude/Codex/LLM CLI。
- UI/API/E2E 至少覆盖：工具搜索、profile recommendation、preflight、approval pending、执行证据展示。
- 任务审计记录能还原“助手为什么选择该工具、是否通过预检、调用结果是什么”。

### Phase 6：运行监控、资源审计与无后台 token 防护

**实施内容**

1. gateway 启动时输出结构化启动摘要：profile、module、tool_count、base_url、transport、manifest version。
2. 新增 doctor/smoke：检查 loopback 连通、后端健康、manifest 完整性、客户端配置 drift、full 默认启用风险。
3. 增加静态检查，禁止 AIstock MCP gateway/module 启动 LLM CLI 或后台 daemon。
4. 增加进程资源诊断说明：如何区分多个客户端会话导致的 MCP 进程与异常 worker。

**验收标准**

- `python scripts/aistock_mcp_gateway.py --self-check --profile=lite` 返回结构化 PASS/FAIL。
- 静态 grep gate 覆盖 `claude`、`codex`、`bun daemon`、`stream-json` 等禁止模式；合理例外必须有 allowlist。
- doctor 能识别 stale worktree、非 loopback base_url、full 默认 profile、script-backed 默认 server。
- 无后端运行时，gateway health 返回明确 dependency failure，不伪装成功。

### Phase 7：兼容期收敛与 standalone 退役

**实施内容**

1. 所有客户端默认配置切换到统一 gateway。
2. 独立脚本入口进入 deprecation，仅保留 wrapper 或迁移提示。
3. 更新项目文档、客户端配置、Research Assistant 文档和 issue workflow doctor。
4. 删除或停用 standalone MCP 的默认注册项，保留人工 fallback 直到用户确认退役。

**验收标准**

- `.mcp.json` 中不再默认注册 `scripts/aistock_mcp_server.py`、`scripts/aistock_qe_experiment_mcp_server.py`、`scripts/aistock_qe_archive_mcp_server.py`。
- 新开 Codex/Claude/智能助手会话时只启动统一 gateway profile。
- full profile 覆盖所有已批准保留工具；无未映射工具。
- DESIGN-COMPLIANCE 矩阵显示所有工具为 `implemented_and_verified` 或有用户批准的 `deprecated` 例外。

## 8. 测试方案

### 8.1 L0/L1 静态与单元测试

- `python -m compileall backend/mcp scripts/aistock_mcp_gateway.py`
- `pytest tests/mcp/test_gateway_profiles.py -q`
- `pytest tests/mcp/test_mcp_tool_manifest.py -q`
- `pytest tests/mcp/test_mcp_inventory_diff.py -q`
- `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1`

### 8.2 MCP smoke

- `python scripts/aistock_mcp_gateway.py --self-check --profile=lite`
- `python scripts/aistock_mcp_gateway.py --list-tools --profile=validation`
- `python scripts/aistock_mcp_gateway.py --list-tools --profile=qe`
- `python scripts/aistock_mcp_gateway.py --list-tools --profile=full`
- 使用受控 mock transport 或 local backend smoke 验证工具调用路径。

### 8.3 API/DB/业务 oracle

- read-only 工具：验证返回数据与 FastAPI endpoint 一致。
- write-confirmed 工具：验证缺少 confirmation 时 fail-fast；确认后产生 backend run record 或审计事件。
- long-running 工具：验证 submit/status/log/result 链路，不以 MCP 响应承载完整大结果。
- GitHub/BUG 工具：验证 issue candidate、sync status、失败 fallback 指引。
- Research Assistant：验证 task event、context pack、preflight、approval 记录完整。

### 8.4 人工确认项

- 是否允许删除旧 standalone 默认注册项。
- 是否允许工具改名或合并。
- 是否允许 full profile 暂时用于人工调试。
- 是否允许智能助手执行特定 production-adjacent 工具。

## 9. 数据验证方式

| 数据/证据 | 验证方式 |
| --- | --- |
| 工具全集 | 从 gateway modules 和旧 scripts 自动抽取工具名，与 manifest diff |
| profile 工具数 | `--list-tools --profile=<name>` 与 manifest profile_tags 对照 |
| 风险等级 | manifest 字段完整性测试，缺字段失败 |
| 后端 endpoint | preflight 检查 endpoint、method、requires_confirmation |
| 长任务结果 | backend job/run record、status/log tail/result ref |
| 大 payload | response budget guard 返回 refinement，不返回截断数据 |
| 智能助手调用 | task event、approval id、evidence refs 可回放 |
| 客户端资源 | 进程数、启动摘要、doctor 输出 |

## 10. 完整性验收矩阵模板

后续每个实现 PR 合入前必须填充如下矩阵，不能只以测试通过替代设计完整性复核。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| 全部 203 个工具进入 manifest | `backend/mcp/tool_manifest.*` | inventory diff + catalog count PASS | pending | 无 |
| 默认 profile 为 lite | `.mcp.json`、`backend/mcp/profiles.py` | config doctor PASS | pending | 无 |
| validation 19 工具迁移 | `backend/mcp/modules/validation.py` | profile list + targeted tests | pending | 无 |
| QE 54 工具迁移 | `backend/mcp/modules/qe_*.py` | profile list + targeted tests | pending | 无 |
| 智能助手读取统一 catalog | `backend/mcp/modules/research_assistant.py`、RA API | API/E2E evidence | pending | 无 |
| 高风险工具 preflight/approval | gateway preflight、RA approvals | negative/positive tests | pending | 无 |
| 禁止后台 LLM/daemon | static grep gate | grep gate PASS | pending | 无 |
| standalone 默认退役 | `.mcp.json` | 新会话进程和 tool list evidence | pending | 等用户确认退役窗口 |

## 11. 合入与实施边界

### 11.1 非智能助手优先实现边界

如果用户要求先跳过智能助手相关功能，可以先完成 Phase 1、Phase 2、Phase 3、Phase 4、Phase 6、Phase 7 的非智能助手范围。该范围的完成口径为 `non_assistant_unified_gateway_complete`，但不得宣称 Phase 5 或智能助手集成完成。Phase 5 条目必须在验收矩阵中标记为 `PENDING_ASSISTANT_INTEGRATION`。


1. 本文档合入 main 后，后续代码实现必须继续使用独立 worktree 和任务分支。
2. 每个阶段可以独立 PR，但不得把“部分迁移”描述为“统一 gateway 完整完成”。
3. 任一阶段发现工具遗漏、旧客户端不可兼容、测试无法证明真实业务路径，必须停止合入并报告阻塞。
4. 生产后端、前端、TDX、DB、调度器不因本文档合入而重启或变更。
5. 只有当 Phase 7 的验收矩阵全部 PASS 或具备用户批准的 deprecation 例外后，才能宣布 standalone MCP 完全退役。

## 12. Phase 0 文档交付验收

| 验收项 | 证据 | 状态 |
| --- | --- | --- |
| 文档位于架构目录 | `docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md` | PASS |
| 覆盖智能助手适配 | 第 5.4 节、Phase 5 | PASS |
| 每个阶段包含验收标准 | 第 7 节 Phase 0-7 | PASS |
| 包含测试方案和数据验证方式 | 第 8、9 节 | PASS |
| 包含完整性矩阵模板 | 第 10 节 | PASS |
| 本阶段不改运行时代码/DB | 本提交仅新增文档 | PASS |
