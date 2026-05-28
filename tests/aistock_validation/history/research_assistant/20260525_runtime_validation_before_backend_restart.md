# Research Assistant MCP/Skill Execution 后端重启前验证记录

- validation_run_id: `RA-MCP-SKILL-PRE-RESTART-20260525`
- 日期: 2026-05-25
- repo: `F:\Dev\AIstock`
- main_commit: `a268506d fix(research-assistant): make production ddl idempotent`
- 验证边界: 不停止、不启动、不重启 backend `8001`；仅做自动化测试、编译、生产 DB 只读 schema gate、当前 runtime 只读探测

## Git 状态

- `main == origin/main == a268506d`
- `git status --short --branch`: clean

## 自动化验证

| 命令 | 结果 |
| --- | --- |
| `rtk python -m pytest backend/tests/research_assistant -q` | `36 passed in 13.71s` |
| `rtk python -m compileall backend/services/research_assistant backend/routers/research_assistant.py backend/db/init_research_assistant_schema_20260521.py` | passed |
| `rtk git diff --check` | passed |

## 生产 DB schema gate 只读复核

| 对象 | 结果 |
| --- | --- |
| DB target | `host=127.0.0.1 port=5432 dbname=aistock user=postgres password_set=True` |
| PostgreSQL | `16.10` |
| `assistant_capabilities` | exists=True, columns=21, missing_required=[], table_comment=True, column_comments=21/21 |
| `assistant_action_proposals` | exists=True, columns=22, missing_required=[], table_comment=True, column_comments=22/22 |
| `assistant_mcp_tool_events` | exists=True, columns=23, missing_required=[], table_comment=True, 本次新增字段 comments=9/9 |
| indexes | `idx_acap_status_risk`, `idx_aap_task_status` and primary/unique indexes present |
| schema_gate | PASS |

## 当前 backend runtime 只读探测

| 项目 | 结果 |
| --- | --- |
| Port `8001` | listening on `127.0.0.1:8001` |
| Process | `python.exe` PID `33184`, command `uvicorn backend.main:app --host 127.0.0.1 --port 8001` |
| `/api/v1/research-assistant/health` | HTTP 200, repository ok, catalog ready, phase=`phase1` |
| `/api/v1/research-assistant/capabilities?limit=5` | HTTP 404 |
| `/openapi.json` research-assistant capabilities/actions paths | count=0 |

## 结论

- 代码与 DB schema 已验证通过。
- 当前生产 backend `8001` 仍是重启前旧 runtime，尚未加载 `a268506d` 的新 Research Assistant routes 和 health phase。
- 用户重启 backend 后，需要继续执行 post-restart API smoke。

## 用户重启后的待执行 smoke

1. `GET /api/v1/research-assistant/health`：期望 phase 包含 `mcp_skill_execution_closure`，capability registry / execution gateway / QE workflow flags 可见。
2. `GET /openapi.json`：期望出现 `/api/v1/research-assistant/capabilities` 和 `/api/v1/research-assistant/actions*`。
3. `POST /api/v1/research-assistant/capabilities/sync` dry-run：确认 catalog diff 可返回。
4. `POST /api/v1/research-assistant/capabilities/sync` apply：确认 approved capability 写入 `assistant_capabilities`。
5. Action Proposal smoke：create -> confirm -> preflight -> dry-run execute；不得真实 materialize/run QE。
6. Workbench UI smoke：确认 capability selector、proposal controls、人类可读 result card 可加载。
