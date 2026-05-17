# QE MCP v1 后端与 MCP 合约验证

- 模块：qe_mcp
- 级别：L3
- 日期：2026-05-16T03:09:52+08:00
- Git commit：d89b38d（执行验证时的基线提交，最终提交见本分支 HEAD）
- 分支：codex/qe-mcp-template-archive-20260516
- Operator：Codex

## 范围

- 新增 QE Experiment MCP 和 QE Archive MCP 两个平行独立 server，只通过 loopback FastAPI HTTP 调用现有后端能力。
- 新增 QE execution template 后端能力：单次实验与 custom_evo 配置可保存为模板，人工确认后物化，物化后再确认执行。
- 补齐 QE archive v2 自动入库控制：AUTO / SKIP / MANUAL_ONLY 策略、skip registry、ingest history、backfill run 生命周期、bootstrap marker、worker run-once 和基础分析查询。
- 覆盖 custom_evo `auto_start=false` 物化、确认后 run、loop retry/rerun/append 等现有后端入口的 MCP 封装。
- 不包含：自动演进 LLM 决策、多 alpha 架构实验调度、生产端口切换、前端 UI 改造。

## 环境

- 工作区：`F:\Dev\AIstock_worktrees\qe-mcp-template-archive-20260516`
- 后端端口检查：`8011` 已被占用但允许作为 dev 端口；未检查或使用生产 `8001`。
- 前端端口检查：`3011` 空闲；未启动或重启 `3000`。
- Conda/env：`C:/Users/lc999/miniconda3/envs/AIstock/python.exe`
- 数据库：通过 root `.env` 加载 `TDX_DB_*` / `AISTOCK_PG_*` 后执行本地/dev DB schema bootstrap 与 read-only smoke。
- 浏览器/headless：本次无 UI 变更，未执行 Playwright。

## 矩阵

| 用例 | 预期业务结果 | 证据 | 结果 |
|---|---|---|---|
| MCP loopback 合约 | MCP 只连本机 dev FastAPI，不导入 scheduler/DB/RD-Agent workspace | `qe_mcp_backend` 19 passed；MCP 脚本单测覆盖 loopback、ID sanitizer、确认 token | PASS |
| 模板物化与执行边界 | 单次实验模板复用现有 `/quantevolver/config/generate`；custom_evo 物化 `auto_start=false`，run 需确认 | `qe_mcp_backend`、`qe_archive_backend`；新增 materializer 单测 | PASS |
| Archive 策略 | `SKIP/MANUAL_ONLY` 不进入 outbox/archive，进入 skip/history；AUTO 可进入 outbox | `qe_archive_backend` 94 passed；新增单次/custom_evo policy 单测 | PASS |
| Schema/comment | 新增 archive v2 与 template schema 表/列均有 COMMENT | `qe_archive_data_quality`：32/32 tables，546/546 columns commented | PASS |
| custom_evo 既有能力回归 | retry/rerun/append 等既有自定义演进路由未破坏 | `pytest backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py` 13 passed | PASS |
| Guardrail | 无 HIGH 级 secret、硬编码 worker workspace、静默空成功、禁止 fallback | `qe_mcp_l3` guardrail 0 finding | PASS |
| 生产隔离 | 不触碰生产 `8001/3000`，不合入 main | nox port check 只检查 `8011/3011`；本分支未 merge main | PASS |

## 命令

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/qe_templates/test_template_validator.py backend/tests/test_qe_archive_repository_static.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_mcp_l3 -- 8011 3011
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_mcp_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py -q -p no:cacheprovider
# 加载 root .env 中的 TDX_DB_* / AISTOCK_PG_* 后执行：
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_data_quality
```

## 证据

- `qe_mcp_l3`：PASS；生成本记录；guardrail 0 finding；`8011 occupied`、`3011 free`；随后自动运行 `qe_mcp_backend` 与 `qe_archive_backend`。
- `qe_mcp_backend`：19 passed。
- `qe_archive_backend`：94 passed。
- custom_evo 回归：13 passed。
- `qe_archive_data_quality`：schema version `qe_archive_v2_20260516`；expected/existing table `32/32`；expected/commented column `546/546`；`run_count=16`；`pending_outbox_count=1642` 为 informational warning。
- 临时 DB smoke 输出：`tmp/qe_archive_data_quality_smoke.json`（ignored，不提交）。

## 失败与修复

| 失败 | 根因 | 修复 | 复测证据 |
|---|---|---|---|
| 初次 `qe_mcp_l3` guardrail 扫到既有 `qe_archive/handlers/_synthesize.py` 的 HIGH | 新增 L3 扫描范围包含整个 `backend/services/qe_archive`，带入非本次新增 handler 历史问题 | L3 guardrail 收敛为本次新增/修改的 archive 文件集合；既有 handler 仍由 archive 专项门禁覆盖 | `qe_mcp_l3` PASS，guardrail 0 finding |
| 单次实验模板物化绕过现有 API 校验风险 | materializer 曾直接调用底层 `ConfigComposer.compose_experiment` | 改为通过现有 `quantevolver.generate_config` / `GenerateConfigRequest` 路径，并补错误透传单测 | `qe_mcp_backend` 19 passed，`qe_archive_backend` 94 passed |
| `archive_policy` 在部分 payload 层级无法解析 | 单次实验与 custom_evo loop 的参数可能落在 `runtime_flags`、`raw_config.custom_params`、`strategy_params` 或 `model_params` | 扩展 policy resolver 与 loop assembler，补 SKIP/MANUAL_ONLY 单测和 realtime skip 单测 | `pytest ...test_qe_archive_repository_static.py` 覆盖，`qe_archive_backend` PASS |

## 结果

- 最终状态：PASS。
- 业务结果：QE MCP v1 可通过 agent 读取/分析 QE 实验、保存待执行模板、在确认后调用现有后端执行单次实验或 custom_evo，并可查询/补齐 QE archive；特殊实验可通过 archive policy 跳过数仓。
- 需要生产 backend restart：否。
- 需要生产 frontend restart：否。
- 需要 dev service restart：如要在当前已运行的 `8011` backend 中使用新增 API，需要用户后续选择重启对应 dev backend；本次未重启生产。
- 剩余风险：未执行真实长耗时 QE 训练/演进任务；远端 CPU-only 目前作为 validator warning 的软限制；pending outbox 历史积压为既有数据状态，不阻断本次提交。