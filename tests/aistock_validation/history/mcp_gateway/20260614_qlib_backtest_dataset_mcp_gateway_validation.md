# Qlib H5/Bin MCP Gateway 候选导出能力验证（2026-06-14）

## 范围

- 新增统一 MCP Gateway 模块：`backend/mcp/modules/qlib_export.py`
- 新增 profile：`qlib_data` / `backtest_data` / `data_full`
- 新增项目 MCP server：`aistock-qlib-data`
- 本次仅覆盖 candidate export + validation；不支持、不执行生产 WSL 数据路径 promotion。

## 设计与实现

- 设计文档：`docs/architecture/qlib_backtest_dataset_mcp_gateway_design_20260614.md`
- MCP 模块：`backend/mcp/modules/qlib_export.py`
- Gateway profile：`backend/mcp/profiles.py`
- 静态 manifest：`backend/mcp/tool_manifest.py`
- 项目 MCP 配置：`.mcp.json`
- 测试：
  - `backend/tests/mcp/test_qlib_export_module.py`
  - `backend/tests/mcp/test_profiles_registry_gateway.py`
  - `tests/mcp/test_mcp_tool_manifest.py`
  - `tests/mcp/test_mcp_gateway_cli.py`
  - `tests/mcp/test_gateway_profiles.py`

## 关键行为

- `qlib_data` profile 暴露 15 个 Qlib H5/Bin 工具。
- `data` profile 保持原有 local_data 47 工具不变。
- `data_full` profile 暴露 local_data + qlib_export 共 62 工具。
- confirmed 工具统一要求 `confirm="RUN_QLIB_EXPORT"`，错误确认会在 HTTP 调用前拦截。
- `qlib_export_run_h5_daily_aux_incremental_all_confirmed` 明确返回 `minute_h5_included=false`，避免把后端 `incremental_all` 误解为包含 `minute_1min.h5`。
- `qlib_export_generate_backtest_candidate_confirmed` 仅生成候选，返回 `production_promotion_supported=false`。
- 写入型工具会校验 payload 内的 `snapshot_id` / `bin_snapshot_id`，拒绝路径穿越和 `qlib_bin`、`qlib_minute_bin`、`factor_data` 生产目标叶子名，保证 MCP 只做候选生成，不提供自动替换入口。

## 验证命令

```powershell
python -m compileall backend/mcp/modules/qlib_export.py backend/mcp/profiles.py backend/mcp/tool_manifest.py backend/tests/mcp/test_qlib_export_module.py backend/tests/mcp/test_profiles_registry_gateway.py tests/mcp/test_mcp_tool_manifest.py tests/mcp/test_mcp_gateway_cli.py tests/mcp/test_gateway_profiles.py
python -m pytest backend/tests/mcp/test_qlib_export_module.py backend/tests/mcp/test_profiles_registry_gateway.py tests/mcp/test_mcp_tool_manifest.py tests/mcp/test_mcp_gateway_cli.py tests/mcp/test_gateway_profiles.py -q
python -m pytest tests/mcp -q
python scripts/aistock_mcp_gateway.py --startup-summary --profile=qlib_data
python scripts/aistock_mcp_gateway.py --startup-summary --profile=data_full
python scripts/aistock_mcp_gateway.py --list-tools --profile=qlib_data
python scripts/aistock_mcp_gateway_doctor.py --json
```

## 验证结果

- `compileall`: PASS
- 目标 MCP 测试：`79 passed`
- `tests/mcp`: `38 passed`
- `git diff --check`: PASS
- `qlib_data` startup summary：PASS，modules=`["qlib_export"]`，tool_count=`15`
- `data_full` startup summary：PASS，modules=`["local_data", "qlib_export"]`，tool_count=`62`
- gateway doctor：PASS，manifest_tool_count=`360`，legacy_tool_count=`354`，platform_tool_count=`6`

## Live 只读验证

通过新模块直接调用当前 8001 后端，只执行只读/plan 工具和错误确认拦截：

- `qlib_export_get_config`: PASS，返回 snapshot root `F:\Dev\AIstock\qlib_snapshots`
- `qlib_export_list_snapshots`: PASS，total=`6`
- `qlib_export_list_bin_exports`: PASS，total=`9`
- `qlib_export_plan_dataset_update(target_end="2026-05-29")`: PASS，`status=plan_only`，`candidate_only=true`，`production_promotion_supported=false`
- `qlib_export_data_preview("000001.SZ", "2026-05-29", "2026-05-29")`: PASS，返回单股票小样本
- 错误确认调用 `qlib_export_run_bin_unified_v2_confirmed(..., confirm="WRONG")`: PASS，HTTP 前拦截，错误包含 `RUN_QLIB_EXPORT`
- 生产目录名拦截单元测试：PASS，`qlib_bin`、`qlib_minute_bin`、`factor_data` 在写入型工具 HTTP 前拒绝。

## 生产影响

- production_ddl_gate: noop
- production_frontend_dependency_gate: noop
- production_backend_dependency_gate: noop
- 未重启 backend/frontend/TDX。
- 未写生产 DB。
- 未创建 H5/Bin 候选数据。
- 未替换 `/home/lc999/data/qlib_bin`、`/home/lc999/data/qlib_minute_bin`、`/home/lc999/data/factor_data`。
- MCP 无自动替换/promotion 工具；生产目标目录名在写入型工具中被显式拦截。

## 结论

Qlib H5/Bin 回测数据集导出已接入统一 MCP Gateway 的候选生成与验证阶段。Codex 与 Claude Code 均可通过新增 `aistock-qlib-data` server/profile 使用同一 gateway；需要客户端重新加载 MCP 配置后才能看到新增工具。
