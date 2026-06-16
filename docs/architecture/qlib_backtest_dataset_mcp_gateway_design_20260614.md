# Qlib H5/Bin 回测数据集 MCP Gateway 设计方案（2026-06-14）

## 1. 背景与目标

AIstock 已经具备 Qlib H5 Snapshot 与 Qlib Bin 回测数据集导出能力，主要入口为：

- 后端：`backend/qlib_exporter/router.py`
- 前端：`frontend/src/app/qlib/page.tsx`
- 统一 MCP Gateway：`scripts/aistock_mcp_gateway.py`
- 现有本地数据 MCP 模块参考：`backend/mcp/modules/local_data.py`

本方案目标是在统一 MCP Gateway 中新增 `qlib_export` 模块，让 Codex、Claude Code 和 Research Assistant 可以通过同一个 gateway 安全地完成：

1. 查看 H5/Bin 数据集配置、候选、质量状态。
2. 生成更新计划和 dry-run 风险摘要。
3. 在显式确认后生成 H5/Bin 候选数据集。
4. 触发候选数据集验证。
5. 暂不通过 MCP 直接替换生产 WSL 数据路径，promotion 另行评估。

## 2. 当前代码事实

### 2.1 已有 H5/Bin API

`backend/qlib_exporter/router.py` 已提供下列关键能力：

- 只读配置与列表：
  - `GET /api/v1/qlib/config`
  - `GET /api/v1/qlib/snapshots`
  - `GET /api/v1/qlib/bin/exports`
  - `GET /api/v1/qlib/snapshots/{snapshot_id}/quality`
  - `GET /api/v1/qlib/snapshots/{snapshot_id}/validate`
- H5 全量导出：
  - `POST /api/v1/qlib/snapshots/daily`
  - `POST /api/v1/qlib/snapshots/minute`
  - `POST /api/v1/qlib/snapshots/daily_basic`
  - `POST /api/v1/qlib/snapshots/moneyflow`
  - `POST /api/v1/qlib/snapshots/bak_basic`
  - `POST /api/v1/qlib/snapshots/margin_detail`
  - `POST /api/v1/qlib/snapshots/cyq_perf`
  - `POST /api/v1/qlib/snapshots/sector_data`
- H5 增量导出：
  - `POST /api/v1/qlib/snapshots/minute/incremental`
  - `POST /api/v1/qlib/snapshots/daily/incremental`
  - `POST /api/v1/qlib/snapshots/moneyflow/incremental`
  - `POST /api/v1/qlib/snapshots/daily_basic/incremental`
  - `POST /api/v1/qlib/snapshots/bak_basic/incremental`
  - `POST /api/v1/qlib/snapshots/margin_detail/incremental`
  - `POST /api/v1/qlib/snapshots/cyq_perf/incremental`
  - `POST /api/v1/qlib/snapshots/sector_data/incremental`
- H5 一键日频/辅助增量：
  - `POST /api/v1/qlib/snapshots/{snapshot_id}/incremental_all`
  - 注意：当前实现不包含 `minute_1min.h5`，分钟 H5 必须单独调用 `minute/incremental`。
- 附加产物：
  - `POST /api/v1/qlib/snapshots/{snapshot_id}/static_factors`
  - `POST /api/v1/qlib/field_map/export`
- Qlib Bin：
  - `POST /api/v1/qlib/bin/unified_export_v2`
  - 支持 `stock_daily`、`stock_minute` 等 dataset，支持 `full`/`incremental`。
  - 若增量扩展 qfq basis_end 会 fail-fast，避免复权口径混合。

### 2.2 当前 MCP 缺口

当前 `backend/mcp/profiles.py` 与 `backend/mcp/tool_manifest.py` 未包含 `qlib_export` 模块。

`data` profile 当前只包含 `local_data`，它负责源数据管理，不负责 H5/Bin 候选导出。

## 3. 设计原则

1. **统一 gateway**：只接入 `scripts/aistock_mcp_gateway.py`，不新增独立 MCP server。
2. **薄 MCP 层**：MCP 只做参数校验、确认 token、loopback FastAPI 调用和响应瘦身；不直接读 DB、不直接操作 H5/Bin 文件、不调用导出 service。
3. **候选优先**：第一阶段只做 candidate export + validation，不做生产目录 promotion。
4. **显式确认**：所有会创建文件或长任务的工具必须要求 `confirm="RUN_QLIB_EXPORT"`。
5. **不返回大 payload**：工具默认返回摘要、ID、路径、日期范围、状态、计数、下一步建议；日志、CSV、完整 JSON 不直接透出。
6. **分钟 H5 单独建模**：不得把 `incremental_all` 描述成覆盖全部 H5；MCP 工具名和文档必须明确它只覆盖 daily/aux。
7. **生产替换隔离**：`/home/lc999/data/qlib_bin`、`/home/lc999/data/qlib_minute_bin`、`/home/lc999/data/factor_data` 的 promotion 后续单独设计，需 Data Doctor 全绿、路径白名单和二次确认。
8. **生产目录名拦截**：MCP 写入型工具必须把 `snapshot_id`、`bin_snapshot_id` 等目录片段限定为候选导出 ID，并拒绝 `qlib_bin`、`qlib_minute_bin`、`factor_data` 这类生产目标叶子名，避免把生成动作误用为替换动作。

## 4. MCP 模块与工具清单

新增模块：`backend/mcp/modules/qlib_export.py`

新增 profile：

- `qlib_data`: 仅包含 `qlib_export`
- `data`: 保持兼容，仅 `local_data`
- `data_full`: 包含 `local_data` + `qlib_export`
- `full`: 增加 `qlib_export`

工具分组如下。

### 4.1 只读工具

- `qlib_export_get_config`
- `qlib_export_list_snapshots`
- `qlib_export_list_bin_exports`
- `qlib_export_get_snapshot_quality`
- `qlib_export_validate_snapshot`
- `qlib_export_data_check`
- `qlib_export_data_preview`

### 4.2 计划/dry-run 工具

- `qlib_export_plan_dataset_update`

该工具第一阶段在 MCP 层生成保守计划，不写数据。输入包括 `target_end`、`snapshot_id`、`include_minute_h5`、`include_bin`、`stock_universe_mode`、`universe_key`。输出包括推荐调用顺序、是否需要全量 Bin、已知风险和确认 token。

### 4.3 confirmed 执行工具

- `qlib_export_run_h5_dataset_full_confirmed`
- `qlib_export_run_h5_dataset_incremental_confirmed`
- `qlib_export_run_h5_daily_aux_incremental_all_confirmed`
- `qlib_export_build_static_factors_confirmed`
- `qlib_export_export_field_map_confirmed`
- `qlib_export_run_bin_unified_v2_confirmed`
- `qlib_export_generate_backtest_candidate_confirmed`

`qlib_export_generate_backtest_candidate_confirmed` 为组合型工具，第一阶段顺序调用现有 API：

1. H5 daily/aux incremental_all。
2. 可选 H5 minute incremental。
3. 可选 static_factors。
4. 可选 field_map。
5. 可选 Bin unified_export_v2。

组合工具返回每步摘要，不返回完整大 payload。

## 5. 对更新到 5 月底的支持

以 2026-05-29 为 5 月底交易日目标，MCP 可执行：

1. `qlib_export_plan_dataset_update(target_end="2026-05-29")`
2. `qlib_export_run_h5_daily_aux_incremental_all_confirmed(confirm="RUN_QLIB_EXPORT")`
3. `qlib_export_run_h5_dataset_incremental_confirmed(dataset="minute", confirm="RUN_QLIB_EXPORT")`
4. `qlib_export_build_static_factors_confirmed(confirm="RUN_QLIB_EXPORT")`
5. `qlib_export_export_field_map_confirmed(confirm="RUN_QLIB_EXPORT")`
6. `qlib_export_run_bin_unified_v2_confirmed(mode="full", datasets=["stock_daily", "stock_minute"], confirm="RUN_QLIB_EXPORT")`
7. `qlib_export_validate_snapshot(...)`
8. Data Doctor / smoke validation（若后续接入 dedicated endpoint，可由 MCP 触发；第一阶段至少记录外部验证命令）。

第一阶段结论：可以通过 MCP 完成候选生成和验证，不直接完成生产替换。

## 6. 验证方案

### 6.1 单元测试

- 新增 `backend/tests/mcp/test_qlib_export_module.py`
  - 验证工具注册数量。
  - 验证所有路径只打到 `/api/v1/qlib/*` 或 `/api/v1/local-data/*` 只读检查，不越权。
  - 验证 confirmed 工具错误 confirm 不发 HTTP。
  - 验证 path fragment 拒绝 `../`、`/`、`?`。
  - 验证写入型工具拒绝 `qlib_bin`、`qlib_minute_bin`、`factor_data` 生产目标叶子名。
  - 验证组合工具不会返回超大原始 payload。

### 6.2 Gateway/manifest 测试

- 更新 `backend/tests/mcp/test_profiles_registry_gateway.py`
- 更新 `tests/mcp/test_mcp_gateway_cli.py`
- 更新 `tests/mcp/test_gateway_profiles.py`
- 更新 `tests/mcp/test_mcp_tool_manifest.py`

### 6.3 本地验证命令

- `python -m compileall backend/mcp/modules/qlib_export.py backend/mcp/profiles.py backend/mcp/tool_manifest.py`
- `python -m pytest backend/tests/mcp/test_qlib_export_module.py backend/tests/mcp/test_profiles_registry_gateway.py tests/mcp/test_mcp_tool_manifest.py tests/mcp/test_mcp_gateway_cli.py tests/mcp/test_gateway_profiles.py -q`
- `python scripts/aistock_mcp_gateway.py --startup-summary --profile=qlib_data`
- `python scripts/aistock_mcp_gateway.py --startup-summary --profile=data_full`
- `python scripts/aistock_mcp_gateway.py --list-tools --profile=qlib_data`

## 7. 验收标准

- `qlib_data` profile 能独立启动并只暴露 Qlib export 工具。
- `data` profile 不被破坏，仍保持 local_data 语义。
- `data_full` profile 同时暴露 local_data 与 qlib_export。
- Codex/Claude Code 可以通过 `.mcp.json` 中新增的统一 gateway server 激活该 profile。
- 所有写/导出类工具没有正确确认 token 时不发 HTTP 请求。
- 工具描述明确 candidate-only，不声称生产替换完成。
- 工具层拒绝把候选 ID 设置为生产 WSL 目标目录名；数据生成后只能走验证与人工替换。
- 本次不触碰生产 DB、不重启 backend、不替换 WSL 生产 Qlib 路径。
