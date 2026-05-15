# QE MCP v1 验证矩阵

## 验证目标

QE MCP v1 通过两个独立 MCP server 让代理可以分析和调度 QE 单次实验、自定义演进实验、QE 数仓查询和历史补齐。MCP 只作为 loopback HTTP 薄封装，所有实验创建、执行、重试、克隆、重跑和归档动作都必须进入现有 FastAPI 后端路径。

## 安全边界

- 禁止触碰或重启生产 `8001` / `3000`；验证仅允许使用 `8011/3011` 或 `8012/3012`。
- MCP 脚本必须强制 loopback URL，不得导入 backend scheduler、DB repository、RD-Agent workspace 或任何新的执行链路。
- 写入型操作必须有确认 token：单次实验执行、自定义演进执行、模板物化、历史回填执行和 worker run-once 都必须显式确认。
- 自定义演进模板物化只能生成待执行任务，必须使用 `auto_start=false`，后续执行必须通过确认后的 run endpoint。
- 本期不验证自动演进 LLM 决策和多 alpha 架构调度。

## L0/L1 静态与单元验证

```powershell
python -m nox -s qe_mcp_backend
```

覆盖要求：

- `scripts/aistock_qe_experiment_mcp_server.py` 和 `scripts/aistock_qe_archive_mcp_server.py` 只调用 loopback HTTP。
- MCP identifier sanitizer 拒绝路径穿越、查询串注入和空 ID。
- 确认 token 在发起 HTTP 请求前校验。
- `qe_execution_templates` 与 `qe_archive` 新增 schema 的表和列均有 PostgreSQL COMMENT。
- QE 模板 validator 拒绝 multi-alpha 模板，并对远端 CPU-only 限制作软告警。
- 单次实验模板物化复用现有 `/api/v1/quantevolver/config/generate` 后端逻辑，不直接绕过校验调用底层 composer。
- `archive_policy=SKIP/MANUAL_ONLY` 能从单次实验 `custom_params` 和 custom_evo `strategy_params/model_params` 解析，并进入 skip/ingest history，而不是进入 outbox/archive。

## L2 API 与数据流验证

- `/api/v1/qe-templates` 支持 create/list/get/update/validate/approve/materialize/run/supersede。
- `/api/v1/quantevolver/evolution/custom-tasks` 默认保持 UI 兼容 `auto_start=true`，模板物化路径必须传入 `false`。
- `/api/v1/quantevolver/evolution/tasks/{task_id}/custom-evo/run` 使用现有 scheduler submit 路径，必须要求 `QE_CUSTOM_EVO_RUN`。
- `/api/v1/qe-archive/backfill/preview` 只登记 preview run，不写入 archive run。
- `/api/v1/qe-archive/backfill/execute` 必须要求 `QE_ARCHIVE_BACKFILL`。
- `/api/v1/qe-archive/query/*` 能返回 factor、model、seed、hyperparam 维度统计，供 MCP 后续优化分析使用。

## L3 本地流水线

```powershell
$env:BACKEND_PORT="8011"
$env:FRONTEND_PORT="3011"
python -m nox -s qe_mcp_l3 -- 8011 3011
```

预期：

- 生成 `tests/aistock_validation/history/qe_mcp/` 下的 run record。
- guardrail 扫描新增 QE MCP / QE archive / QE template 代码，无 HIGH 级 secret、硬编码 worker workspace、静默空成功或禁止 fallback。
- 自动触发 `qe_mcp_backend` 和 `qe_archive_backend`，全部通过。
- 端口检查只允许 dev 端口；`8001/3000` 不被使用、停止或重启。

## L4/L5 数据质量与提交前验证

推荐提交前组合：

```powershell
python -m nox -s qe_mcp_l3 -- 8011 3011
python -m nox -s qe_mcp_backend
python -m nox -s qe_archive_backend
python -m nox -s qe_archive_data_quality
python -m pytest backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py -q -p no:cacheprovider
git diff --check
```

验收标准：

- `qe_archive_data_quality` 显示 schema version 为 `qe_archive_v2_20260516`，所有期望表和列存在且有 COMMENT。
- 如果存在 pending outbox，仅作为 read-only smoke informational warning，不阻断本次提交。
- 提交 feature 分支前保留中文 run record，并明确记录 dev DB schema bootstrap、生产端口影响和剩余风险。
