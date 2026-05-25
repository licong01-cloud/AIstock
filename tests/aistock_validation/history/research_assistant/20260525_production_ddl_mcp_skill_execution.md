# Research Assistant MCP/Skill Execution 生产 DDL 验证记录

- validation_run_id: `RA-MCP-SKILL-PROD-DDL-20260525`
- 日期: 2026-05-25
- main_commit: `7ffd63c9 feat(research-assistant): add mcp skill execution closure`
- schema_version: `research_assistant_mcp_skill_execution_v1_20260525`
- 操作目录: `F:\Dev\AIstock`
- 操作范围: Research Assistant schema bootstrap，不启动、不停止、不重启 backend `8001` 或 frontend `3000`

## 目标库预检

只读预检加载 `F:\Dev\AIstock\.env` 中的 `TDX_DB_*` 配置，但未打印密码值。

| 项目 | 结果 |
| --- | --- |
| DB target | `host=127.0.0.1 port=5432 dbname=aistock user=postgres password_set=True` |
| PostgreSQL | `PostgreSQL 16.10 on x86_64-pc-linux-musl` |
| server addr | `172.17.0.3/32:5432` |
| preflight `assistant_capabilities` | 不存在 |
| preflight `assistant_action_proposals` | 不存在 |
| preflight `assistant_mcp_tool_events` | 存在，14 列，缺少 `action_proposal_id`、`approval_id`、`plan_digest`、`transport`、`timeout_ms`、`attempt_index`、`duration_ms`、`result_card_json`、`artifact_refs` |

## DDL 执行

执行顺序：

1. 对既有 `assistant_mcp_tool_events` 先执行 idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`，补齐本次 main 提交新增的 9 个字段，避免 `COMMENT ON COLUMN` 先于字段存在时报错。
2. 执行 `backend/db/init_research_assistant_schema_20260521.py` 的 `init_research_assistant_schema()`，共 262 条 DDL/comment/index/constraint 语句。
3. 补齐既有表 `assistant_mcp_tool_events` 的表级 comment。

执行结果：

| 项目 | 结果 |
| --- | --- |
| `ddl_result` | `applied` |
| duration | `0.709s` |
| production runtime | 未启动、未停止、未重启 |
| secret handling | 密码仅用于连接，未打印 |

## 后验验证

| 对象 | 结果 |
| --- | --- |
| `assistant_capabilities` | 存在，21 列，required columns 缺失 `[]`，table comment=True，column comments `21/21` |
| `assistant_action_proposals` | 存在，22 列，required columns 缺失 `[]`，table comment=True，column comments `22/22` |
| `assistant_mcp_tool_events` | 存在，23 列，本次 required columns 缺失 `[]`，table comment=True，本次新增 9 个字段均有 comment |
| `assistant_capabilities` constraints | pkey、capability_key unique、`ck_acap_type`、`ck_acap_status`、`ck_acap_risk`、`ck_acap_side_effect` |
| `assistant_action_proposals` constraints | pkey、`uq_aap_idempotency`、`ck_aap_type`、`ck_aap_status`、`ck_aap_risk`、`ck_aap_side_effect` |
| indexes | `idx_acap_status_risk`、`idx_aap_task_status` 以及主键/唯一索引存在 |

## Gate 结论

- `production_ddl_gate=applied_and_verified`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_backend_8001_touched=false`
- `production_frontend_3000_touched=false`
- `production_db_touched=true_schema_only`
- `production_runtime_activation=not_started`

## 后续边界

DDL 已满足本次 Research Assistant MCP/Skill Execution Closure 的生产 schema 前置条件，但生产 runtime 尚未重启或激活新代码。后续如果要让生产 backend 使用这些新能力，需要单独执行运行时激活/重启与 API smoke；该动作不包含在本次 DDL 操作中。
