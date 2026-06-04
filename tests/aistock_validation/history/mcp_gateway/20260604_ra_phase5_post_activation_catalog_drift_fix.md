# RA Phase 5 Post-Activation Catalog Drift Fix - 2026-06-04

## 1. 范围

- Worktree: `F:\Dev\AIstock_worktrees\ra-phase5-post-activation-catalog-20260604`
- Branch: `codex/ra-phase5-post-activation-catalog-20260604`
- Base: `origin/main` at `1d450751`
- 目标: 修复 Research Assistant Phase 5 激活后，`assistant_mcp_tools` 派生缓存仍含 legacy server_key 行时，readiness 与 `assistant_list_mcp_tools` 仍可能消费 DB overlay 的漂移风险。
- 不涉及: DB DDL、生产服务重启、前端改动、TDX 改动。

## 2. 生产重启后实测现象

已由用户重启生产服务，Codex 未启动/停止/重启 `8001` / `3000` / `19080`。

- `POST /api/v1/research-assistant/catalogs/seed`: 成功，`mcp_tools=209`，用于刷新 Phase 5 manifest-derived 派生缓存。
- `GET /api/v1/research-assistant/health`: HTTP 200，`status=ok`。
- `GET /api/v1/research-assistant/catalogs/readiness`: HTTP 200，`ready=true`。
- `GET /api/v1/research-assistant/mcp/tools?limit=5`: HTTP 200，`total=209`，`source=gateway_manifest_derived_catalog`。
- `GET /api/v1/research-assistant/mcp/tools?server_key=aistock-qe-archive&search=leaderboard&limit=5`: HTTP 200，返回 canonical `server_key=aistock-qe`，`total=1`。
- Evidence artifact: `F:\Dev\AIstock_artifacts\ra_phase5_post_activation_smoke_20260604_214000.json`。

只读 DB probe 结果显示：

- `assistant_mcp_tools` 当前 `enabled=313`。
- manifest canonical expected pairs: `209`。
- missing canonical pairs: `0`。
- not_enabled canonical pairs: `0`。
- extra legacy alias pairs: `104`。

结论：运行时已可用，但 DB 派生缓存中保留了旧 key 行；任何继续直接读取 `assistant_mcp_tools` 作为事实源的路径都可能漂移。

## 3. 修复内容

### M1 - readiness 事实源收敛

文件: `backend/services/research_assistant/service.py`

- `catalog_readiness()` 对 `mcp_servers` / `mcp_tools` 改为使用 manifest-derived canonical records。
- readiness check 增加 `source` 字段：
  - `mcp_servers` / `mcp_tools`: `gateway_manifest_derived_catalog`
  - 其他目录: `repository_cache`
- 结果不再被 legacy alias DB 行抬高或误导。

### M2 - summary adapter 事实源收敛

文件: `backend/services/research_assistant/execution.py`

- `assistant_list_mcp_tools` 的 summary adapter 改为调用 `self.list_mcp_tools(...)`。
- 继承统一 manifest catalog 的 server_key canonicalize、risk filter、search、pagination。
- legacy alias 入参仍兼容，例如 `aistock-qe-archive` 会 canonicalize 到 `aistock-qe`。

### M3 - regression tests

文件:

- `backend/tests/research_assistant/test_ra_manifest_catalog_consumption.py`
- `backend/tests/research_assistant/test_natural_language_mcp_routing.py`
- `backend/tests/research_assistant/test_schema_contract.py`

新增/调整断言：

- legacy alias cache rows 不影响 readiness 的 `present=209/9`。
- `assistant_list_mcp_tools` summary adapter 总数保持 `209`，且返回 canonical server_key。
- routing 测试对齐 Phase 5 canonical server_key。
- schema contract 测试对齐实际 seed 写入逻辑：`assistant_mcp_tools` 只写 `MCP_TOOL_DB_COLUMNS`，manifest metadata 留在 adapter/catalog 层。

## 4. 验证结果

```text
python -m pytest backend/tests/research_assistant/test_ra_manifest_catalog_consumption.py -q
17 passed in 16.93s
```

```text
python -m pytest tests/mcp -q
24 passed in 8.18s
```

```text
python -m compileall backend/services/research_assistant/service.py backend/services/research_assistant/execution.py backend/tests/research_assistant/test_ra_manifest_catalog_consumption.py backend/tests/research_assistant/test_natural_language_mcp_routing.py backend/tests/research_assistant/test_schema_contract.py
compileall exit_code=0
```

```text
python -m pytest backend/tests/research_assistant -q
184 passed in 58.72s
```

```text
python scripts/aistock_mcp_gateway.py --self-check --profile=lite
status=pass; tool_count=6; manifest_tool_count=209; errors=[]; warnings=[]
```

```text
python scripts/aistock_mcp_gateway_doctor.py --json
status=pass; static_no_llm.findings=[]; no_background_llm_daemon.status=pass
```

```text
python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1
Guardrail scan completed: mode=changed_only, files=5, findings=0, blocking=0
```

```text
git diff --check
exit_code=0
```

## 5. DESIGN-COMPLIANCE-001 复核

| 项 | 结论 | 证据 |
|---|---|---|
| 单一事实源=TOOL_MANIFEST | done=true | readiness 与 `assistant_list_mcp_tools` summary adapter 均转向 manifest-derived catalog |
| legacy server_key 兼容但不作为事实源 | done=true | 新测试注入 `aistock-qe-archive` legacy cache row 后，返回 canonical `aistock-qe` |
| DB overlay 只作为缓存/收紧层，不驱动目录总量 | done=true | readiness `present=209` 而非 DB 当前 `313` |
| 无简化/POC/mock 充真 | done=true | 修复真实 service/execution 路径，测试覆盖漂移复现 |
| 无生产端口重启 | done=true | Codex 未启动/停止/重启 `8001` / `3000` / `19080` |
| 无 DDL | done=true | 本分支未修改 migration/schema 文件 |

## 6. 生产门禁

- 本分支 `production_ddl_gate=noop`。
- 已完成的生产 DDL 独立记录: `tests/aistock_validation/history/production_ddl/20260604_201711_production_ddl_gate.md`，结论 `production_ddl_gate=applied_and_verified`。
- `production_backend_dependency_gate=noop`。
- `production_frontend_dependency_gate=noop`。
- 生产运行时激活: 代码修复合入 main 后，需要用户按生产节奏重启 backend 才会激活；Codex 不执行生产重启。

## 7. 剩余风险与后续

- 当前生产 API 已通过 seed 恢复 ready，但旧 DB alias 行仍存在；本修复确保后续代码不再把这些行当作目录事实源。
- 若未来需要物理清理 legacy alias cache rows，应单独设计数据修复脚本和 dry-run 验证，不在本次无 DDL 代码修复中执行。
