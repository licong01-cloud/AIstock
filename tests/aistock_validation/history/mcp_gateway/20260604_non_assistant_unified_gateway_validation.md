# MCP 统一 Gateway 非智能助手范围实现验收记录（2026-06-04）

## 范围

本记录覆盖 `docs/architecture/aistock_mcp_unified_gateway_assistant_design_20260604.md` 中除 Phase 5 智能助手集成以外的实现范围：Phase 1、Phase 2、Phase 3、Phase 4、Phase 6、Phase 7 的合入前验收。

本次不修改生产 DB，不重启生产后端 `8001`、前端 `3000` 或 TDX `19080`。

## 阶段验收矩阵

| 阶段 | 设计项 | 实现位置 | 验收证据 | 状态 | 备注 |
| --- | --- | --- | --- | --- | --- |
| Phase 1 | Manifest/Profile/Catalog 基础 | `backend/mcp/tool_manifest.py`、`backend/mcp/profiles.py`、`backend/mcp/modules/catalog.py`、`backend/mcp/gateway.py`、`scripts/aistock_mcp_gateway.py` | `python scripts/aistock_mcp_gateway.py --list-tools --profile=lite` -> 6；`--profile=full` -> 209 total / 203 legacy / 6 platform；`pytest tests/mcp -q -p no:cacheprovider` -> 17 passed | PASS | 新增 6 个 catalog 平台工具，203 个既有工具全部进入 manifest |
| Phase 2 | 低资源默认配置与客户端切换 | `.mcp.json`、`scripts/aistock_mcp_gateway_doctor.py` | `python scripts/aistock_mcp_gateway_doctor.py --json` -> `status=pass`，`aistock-gateway-lite` 默认推荐，未注册 full profile | PASS | 不修改用户全局 Codex/Claude 配置；新会话需按客户端规则重新注入 MCP |
| Phase 3 | Validation MCP 迁移 | `backend/mcp/modules/validation.py`、`backend/mcp/profiles.py`、`.mcp.json` | `--profile=validation` -> 19 tools；inventory diff 测试确认旧 `scripts/aistock_mcp_server.py` 19 个工具全部映射 | PASS | 旧脚本保留为兼容入口，但 `.mcp.json` 不再注册 standalone 脚本 |
| Phase 4 | QE Experiment / QE Archive 迁移 | `backend/mcp/modules/qe_experiment.py`、`backend/mcp/modules/qe_archive.py`、`backend/mcp/profiles.py`、`.mcp.json` | `--profile=qe` -> 63 tools；inventory diff 测试确认旧 QE experiment 26 个和 archive 28 个工具全部映射；MockTransport 测试覆盖确认型 run/backfill | PASS | `qe` profile = `qe_experiment` + `qe_archive` + `model_registry` |
| Phase 6 | 资源审计与无后台 token 防护 | `scripts/aistock_mcp_gateway_doctor.py`、`tests/mcp/test_mcp_gateway_cli.py`、`tests/mcp/test_mcp_inventory_diff.py` | doctor `static_no_llm.findings=[]`；静态测试禁止 MCP module 直接导入 `backend.services` / `backend.db` | PASS | gateway/catalog/self-check 不启动 LLM CLI 或后台 daemon |
| Phase 7 | 当前客户端 standalone 默认退役 | `.mcp.json` | doctor 检查 `.mcp.json` 不包含 `scripts/aistock_mcp_server.py`、`scripts/aistock_qe_experiment_mcp_server.py`、`scripts/aistock_qe_archive_mcp_server.py` | PASS | 本阶段只退役项目级默认注册；不删除旧脚本，保留兼容窗口 |
| Phase 5 | 智能助手集成 | 暂未实现 | 不适用 | PENDING_ASSISTANT_INTEGRATION | 按用户要求，本轮暂不开发智能助手相关功能 |

## 验证命令

```powershell
python -m compileall backend/mcp scripts/aistock_mcp_gateway.py scripts/aistock_mcp_gateway_doctor.py
python scripts/aistock_mcp_gateway.py --list-tools --profile=lite
python scripts/aistock_mcp_gateway.py --list-tools --profile=validation
python scripts/aistock_mcp_gateway.py --list-tools --profile=qe
python scripts/aistock_mcp_gateway.py --list-tools --profile=full
python scripts/aistock_mcp_gateway.py --self-check --profile=lite
python scripts/aistock_mcp_gateway_doctor.py --json
python -m pytest tests/mcp -q -p no:cacheprovider
```

## 结果摘要

- `lite` profile：6 个 catalog 平台工具。
- `validation` profile：19 个工具。
- `qe` profile：63 个工具（QE experiment 26 + QE archive 28 + model registry 9）。
- `full` profile：209 个工具（203 个既有业务工具 + 6 个 catalog 平台工具）。
- `tests/mcp`：17 passed。
- `aistock_mcp_gateway_doctor.py --json`：`status=pass`，`static_no_llm.findings=[]`。

## 生产门禁

- `production_ddl_gate`: `noop`
- `production_frontend_dependency_gate`: `noop`
- `production_backend_dependency_gate`: `noop`
- 生产 runtime / DB touched：否
