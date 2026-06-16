# Paper v2 稳定域 MCP Gateway 设计方案（2026-06-13）

## 1. 背景与结论

本方案面向 AIstock 统一 MCP gateway，补齐 Paper Trading v2 相邻的稳定能力接入。由于当前模拟盘运行/管理能力仍在更新且尚未稳定，本阶段不把模拟盘管理、调度、运行控制、下单、撤单、运行态变更接入 MCP；本阶段只接入已经相对稳定、可由后端 FastAPI 审计路径承载的能力：StrategyPackage 管理、Selection Center、Advisory Center、Paper v2 只读监控、MiniQMT 只读监控。

结论：该需求合理，但必须拆成当前稳定范围和未来模拟盘控制范围。当前分支只实现稳定范围；未来在 MCP 功能验证、模拟盘 API 稳定、SIM-only 风险门禁与审计补齐后，再扩展模拟盘全部功能。实盘/真实交易能力不得通过 MCP 暴露为可操作工具。

## 2. 设计原则

1. 统一入口：Codex、Claude Code、Research Assistant 与未来智能助理都通过 `scripts/aistock_mcp_gateway.py` 和 `backend/mcp` profile 调用，不再新增散落的 standalone MCP server。
2. profile 化激活：默认仍使用 `lite`；Paper v2 相关任务按需启用 `paper_v2_monitor` 或 `paper_v2_stable`，不得默认启用 `full`。
3. 薄 wrapper：MCP module 只做路径参数净化、limit 上限、确认 token、loopback FastAPI 调用；不得导入 `backend.services`、`backend.db` 或绕过 API 写库。
4. summary-first：列表/监控类工具默认使用小 `limit`，复用 `AIstockApiClient` 的响应大小保护；返回超预算时要求收窄过滤，不返回截断 JSON。
5. 写操作确认：StrategyPackage、Selection、Advisory 中会创建/修改后端业务对象的工具必须使用 `_confirmed` 工具名和固定确认 token。
6. 当前禁止范围：本阶段不得暴露 Paper v2 portfolio 创建/生命周期/auto-run/scheduler/session 控制、MiniQMT connect/order/cancel/bank/data-download、virtual strategy ledger 下单/同步/对账写入。
7. 实盘永久禁止：未来即使模拟盘 MCP 功能全部开放，真实账户或 live trading 仍必须留在人工/受控系统路径内，不通过 MCP 操作。

## 3. 当前接入范围

| 模块 | Gateway module | Profile | 范围 | 当前状态 |
| --- | --- | --- | --- | --- |
| StrategyPackage | `strategy_packages` | `strategy_package_ops`, `paper_v2_stable` | 包列表、详情、QE 来源、候选包、manifest/metrics/asset/validation/governance/paper admission；确认型创建/删除/状态/资产/验证/运行变体等管理操作 | 当前实现 |
| Selection Center | `selection_center` | `selection_advisory`, `paper_v2_stable` | selectable packages、industry tree、selection run 查询、fusion/excluded/aggregate、advisory preview/quality；确认型 selection run、聚合、删除、加入 watchlist | 当前实现 |
| Advisory Center | `advisory` | `selection_advisory`, `paper_v2_stable` | advisory program、binding、leaderboard、active pool、reviews、list versions、returns、preview/quality；确认型创建、编辑、binding、状态、review、replay | 当前实现 |
| Paper v2 监控 | `paper_v2_monitoring` | `paper_v2_monitor`, `paper_v2_stable` | portfolios/running summary/auto-run status/readiness、策略与运行配置审计、sessions/live dashboard、orders/fills/cash/positions/performance/errors 等只读监控 | 当前实现 |
| MiniQMT 监控 | `qmt_broker_monitoring` | `paper_v2_monitor`, `paper_v2_stable` | status/account/positions/snapshot/orders/trades/monitor summary/strategy summary 只读监控 | 当前实现 |

## 4. 明确排除范围

| 排除能力 | 代表接口 | 原因 | 后续条件 |
| --- | --- | --- | --- |
| 模拟盘 portfolio 管理 | `POST /paper-v2/portfolios`, lifecycle/delete | 用户明确说明模拟盘功能仍在更新，当前不接入管理功能 | Paper v2 API 稳定后重新设计 SIM-only 控制面 |
| 自动运行与调度控制 | auto-run enable/disable/config, scheduler start/stop/run-once/recover | 会改变运行态，属于高风险模拟盘控制 | 增加 kill switch、审计、幂等、确认 token、回滚策略 |
| Session 控制 | create/tick/pause/resume/stop/switch-mode | 会驱动交易会话状态 | 模拟盘控制能力统一纳入未来 Phase B |
| MiniQMT 原始交易与资金操作 | connect/disconnect/order/cancel/bank transfer | 可能影响真实券商连接或资金/委托 | MCP 永久不提供实盘操作；模拟盘需 SIM-only guard |
| MiniQMT 数据下载/补齐 | data download/catch-up/one-click update | 属于数据同步/后台任务，不属于本阶段稳定 Paper v2 监控 | 后续可评估是否归入 local_data MCP，而非 Paper v2 控制面 |
| Virtual strategy ledger 写入 | bind/preview submit/sync/reconcile | 当前命名里存在“read-only sync”但实际会写 ledger，本阶段不纳入 | 先完成账本语义治理与 SIM-only 审计 |
| 实盘交易 | live approval submit/approve、真实账户下单 | 安全红线 | 禁止通过 MCP 操作 |

## 5. Profile 设计

| Profile | 模块 | 工具数目标 | 用途 |
| --- | --- | ---: | --- |
| `paper_v2_monitor` | `paper_v2_monitoring`, `qmt_broker_monitoring` | 42 | 只读查看 AIstock Paper v2 与 MiniQMT 当前状态、持仓、成交、盈亏、错误 |
| `strategy_package_ops` | `strategy_packages` | 48 | 策略包全面管理 |
| `selection_advisory` | `selection_center`, `advisory` | 38 | 选股、荐股、advisory program 管理 |
| `paper_v2_stable` | 上述五个模块 | 128 | 当前稳定域完整 profile，供 Codex/Claude Code 按任务启用 |
| `paper_v2_ops` | 同 `paper_v2_stable` | 128 | 兼容“Paper v2 稳定操作域”的别名，未来扩展前不得混入运行控制 |

`full` 与 `research_full` 可包含这些模块用于受控验证，但不得成为客户端默认 profile。

## 6. 工具调用形态

当前新模块采用统一 spec-driven wrapper：

```text
MCP tool(payload) -> backend/mcp/modules/_gateway_specs.py
  - sanitize path fragments
  - apply default query params and limit caps
  - require confirm token for _confirmed tools
  - call AIstockApiClient loopback API
  - rely on backend service for business validation, persistence, audit
```

所有新工具建议用一个 `payload` JSON object 参数承载调用输入，避免为每个后端 API 复制大量 Pydantic schema 到 MCP 层。MCP 层负责最小边界校验，后端 API 仍是 schema 和业务语义的权威。

## 7. Codex 与 Claude Code 统一使用方式

`.mcp.json` 新增：

- `aistock-paper-v2-monitor` -> `python scripts/aistock_mcp_gateway.py --profile=paper_v2_monitor`
- `aistock-paper-v2-stable` -> `python scripts/aistock_mcp_gateway.py --profile=paper_v2_stable`

两者均指向 `AISTOCK_MCP_BASE_URL=http://127.0.0.1:8001/api/v1`。Codex 与 Claude Code 只需要加载同一项目 `.mcp.json` 或同步后的用户级 MCP 配置，即可通过统一 gateway 使用相同工具目录。新增工具不会要求启动新的 standalone server。

## 8. 未来 Phase B：模拟盘控制能力接入条件

当模拟盘管理功能稳定后，可以新增 `paper_v2_sim_control` 或扩展 `paper_v2_ops`，但必须满足：

1. SIM-only guard：每个控制工具调用前验证账户模式、后端配置、运行 provider，不允许真实账户。
2. action preview + confirmed：所有会改变状态的操作先有 preview/plan，再用固定 confirm token 执行。
3. audit ledger：记录调用者、profile、工具名、参数摘要、确认 token、后端 job/session id、结果摘要。
4. kill switch：支持系统级禁用 MCP 模拟盘控制，默认关闭或受配置控制。
5. idempotency：创建/启动/恢复/调度类工具必须支持幂等 key 或可解释的重复调用行为。
6. UI parity：MCP 创建/修改的模拟盘对象必须能在 UI 中完整查看、编辑、启动/暂停/恢复，且与人工 UI 操作共享后端路径。
7. 实盘隔离：live/real trading 审批、真实账户委托、资金划转永不进入 MCP 可操作面。

## 9. 验证计划与验收矩阵

| 验收项 | 实现位置 | 验证方式 | 结论 |
| --- | --- | --- | --- |
| 五个新 gateway modules 可加载 | `backend/mcp/modules/*.py`, `backend/mcp/profiles.py` | `--startup-summary --profile=paper_v2_stable` | 已验证 |
| Manifest 无漂移，风险元数据齐全 | `backend/mcp/tool_manifest.py` | `pytest tests/mcp/test_mcp_tool_manifest.py` | 已验证 |
| Codex/Claude Code 使用统一 gateway profile | `.mcp.json` | `scripts/aistock_mcp_gateway_doctor.py` 与 profile list | 项目级配置已验证；用户级客户端配置需在合入后同步/重载 |
| 写操作确认先于 HTTP | 新模块测试 | MockTransport 调用确认型工具，错误 confirm 不发请求 | 已验证 |
| 不暴露模拟盘控制/实盘交易 | manifest/source guard | grep 禁止工具名/endpoint，测试确认未列入 manifest | 已验证 |
| response budget 保持 summary-first | `AIstockApiClient` + limit defaults | list-tools 与 wrapper 默认 limit 检查 | 已验证 |
| 生产安全 | 无 DB migration、无服务重启 | git diff 与执行记录 | 已验证 |

## 10. 生产影响

本方案不包含 DB schema/DDL，不修改 Paper v2 运行服务，不重启 `8001/3000/19080`，不操作生产数据库，不连接或操作 MiniQMT。合入后需要客户端重启或重新加载 MCP 配置后，新 profile 才会注入到 Codex/Claude Code 会话。

## 11. 当前实现验证记录

本分支当前已完成设计与稳定域实现，关键验证如下：

- `python -m pytest tests/mcp backend/tests/mcp backend/tests/research_assistant/test_ra_manifest_catalog_consumption.py backend/tests/research_assistant/test_mcp_catalog_sync.py backend/tests/research_assistant/test_service.py backend/tests/research_assistant/test_api.py -q -p no:cacheprovider` -> `258 passed`
- `python -m pytest tests/mcp -q -p no:cacheprovider` -> `36 passed`
- `python -m pytest backend/tests/mcp -q -p no:cacheprovider` -> `145 passed`
- `python -m pytest backend/tests/research_assistant/test_ra_manifest_catalog_consumption.py backend/tests/research_assistant/test_mcp_catalog_sync.py -q -p no:cacheprovider` -> `20 passed`
- `python -m pytest backend/tests/research_assistant/test_service.py backend/tests/research_assistant/test_api.py -q -p no:cacheprovider` -> `57 passed`
- `python -m ruff check ...` 针对本次变更文件 -> `All checks passed!`
- `python -m compileall backend/mcp debug_tools/mcp scripts/aistock_mcp_gateway.py scripts/aistock_mcp_gateway_doctor.py` -> 通过
- `python scripts/aistock_mcp_gateway.py --startup-summary --profile=paper_v2_monitor` -> `tool_count=42`
- `python scripts/aistock_mcp_gateway.py --startup-summary --profile=paper_v2_stable` -> `tool_count=128`
- `python scripts/aistock_mcp_gateway.py --startup-summary --profile=strategy_package_ops` -> `tool_count=48`
- `python scripts/aistock_mcp_gateway.py --startup-summary --profile=selection_advisory` -> `tool_count=38`
- `python scripts/aistock_mcp_gateway_doctor.py --json` -> `status=pass`，项目 `.mcp.json` 注册 11 个 gateway server，无 legacy standalone server，无默认 full profile
- `python debug_tools/mcp/list_tools_smoke.py --server aistock-paper-v2-monitor` -> 静态 introspection，`tool_count=42`，`production_8001_touched=false`
- `python debug_tools/mcp/list_tools_smoke.py --server aistock-paper-v2-stable` -> 静态 introspection，`tool_count=128`，`production_8001_touched=false`
- `python scripts/aistock_module_ownership_scan.py ...` -> `files=22, mapped=22, unmapped=0, ambiguous=0`
- `git diff --check` -> 通过
