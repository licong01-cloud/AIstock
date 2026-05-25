# Research Assistant Prompt & Context Runtime Governance 验证记录

- validation_run_id: `RA-PCRT-20260525-L3`
- 日期: 2026-05-25
- worktree: `F:\Dev\AIstock_worktrees\research-assistant-prompt-context-runtime-20260525`
- branch: `feature/research-assistant-prompt-context-runtime-20260525`
- base: `origin/main` = `62dc1b1 fix(selection): use TDX pre-close for pre-open entry price`
- head: `2b1ab32 Merge remote-tracking branch 'origin/main' into feature/research-assistant-prompt-context-runtime-20260525`
- 关联: `BUG-117` / GitHub Issue `#186`
- 设计依据:
  - `docs/architecture/research_assistant_prompt_context_runtime_governance_design_20260524.md`
  - `docs/architecture/research_assistant_prompt_pack_runtime_design_20260524.md`
  - `docs/architecture/research_assistant_context_compression_design_20260524.md`

## 验证边界

本次验证只在独立 worktree 和本地测试仓库中执行；未合入 `main`，未关闭 GitHub Issue `#186`，未修改生产 DB，未触碰或重启生产 backend `8001` / frontend `3000`。

`BUG-117` 作为统一项目 Phase 0 处理，不再作为单独分支绕过项目验收。Issue 是否关闭仍等待用户在验收后决定。

## 设计验收矩阵

| 编号 | 设计要求 | 实现证据 | 验证证据 | 结论 |
| --- | --- | --- | --- | --- |
| A1 | 无硬编码运行参数 | `configs/research_assistant/runtime_context.yaml` 管理 model window、routing threshold、预算比例、history page、fresh tail、compaction threshold、worker temperature/max output/retry、query limits、UI 展示策略；`backend/services/research_assistant/context_budget.py` 每轮从 active config 计算预算 | 静态扫描 `_PRIOR_MESSAGES_TOKEN_BUDGET`、`_TOKEN_ESTIMATE_CHARS_PER_TOKEN`、`limit=500/200/100/50/20`、`temperature=0.2`、`max_tokens=1600` 等运行硬编码，结果 `no matches` | 通过 |
| A2 | 自动压缩 | `ResearchAssistantService._maybe_compact_prior_messages()` 基于 `ContextBudgetPlan.should_compact` 自动生成 `context_segments` 与 `context_key_facts` | `test_long_chat_auto_compacts_with_key_facts_and_fresh_tail` 通过，证明长会话触发 compact summary/key facts | 通过 |
| A3 | 用户无感继续 | reactive overflow 后注入 `context.recovery.prompt_too_long_retry`，重新装配摘要、key facts、fresh tail 和当前用户消息 | `test_reactive_context_overflow_compacts_and_retries_without_user_interruption` 通过，回复不要求用户重复背景 | 通过 |
| A4 | 关键事实不丢 | key-fact 提取提示词来自 Prompt Pack；`assistant_context_key_facts` 带 `source_message_ids`、`source_sha256`、`prompt_key` 和 confidence | pytest 断言 `fact_type=key_fact_block` 且 `fact_json.prompt_key=context.compaction.key_fact_extraction` | 通过 |
| A5 | 原文可回溯 | 原始 `assistant_conversation_messages` 保留；summary/key facts 写入 source message ids 和 source checksum | pytest 断言 context segments/key facts 生成并带 source refs；API 新增 messages/segments/key-facts/traces 查询 | 通过 |
| A6 | Prompt Pack 统一 | `prompt_packs/research_assistant/main/**` 文件化 root/governance/domain/renderer/context prompts；seed 写入 source/version/activation | pytest 断言 prompt bundle 有 `activation_id`、`version_refs`，static scan active prompt pack 不含 BUG-117 禁用短语 | 通过 |
| A7 | Config activation | seed 写入 `assistant_runtime_config_sources` 与 `assistant_runtime_config_activations`；每轮 trace 记录 runtime config activation | `test_chat_turn_uses_llm_builds_cards_and_blocks_execution` 断言 context assembly trace 有 `runtime_config_activation_id` | 通过 |
| A8 | Reactive compact | provider 返回 `prompt_too_long` / `context_length_exceeded` 时按 config `max_retries` compact + retry | `test_reactive_context_overflow_compacts_and_retries_without_user_interruption` 断言 trace 状态包含 `retry_after_compaction` | 通过 |
| A9 | 高风险 fail-fast | 高风险任务 reactive retry 后仍超限时抛出 fail-fast，不生成降级答案 | `test_high_risk_reactive_overflow_fail_fast_after_configured_retries` 断言 RuntimeError 和 `context_overflow_fail_fast` trace | 通过 |
| A10 | 生产边界 | 本轮只在 feature worktree 验证；新增 DDL 仅提交代码，不执行生产迁移 | 未触碰 `8001/3000`；未写生产 DB；`production_ddl_gate=pending_before_merge` | 通过 |

## BUG-117 验收

| Closure requirement | 状态 | 证据 |
| --- | --- | --- |
| 删除 root prompt 中未开发 mouse/keyboard、code-write 负向禁用项 | 通过 | root prompt 改为 `prompt_packs/research_assistant/main/nodes/root.assistant.md`；active prompt pack forbidden phrase scan 为 `no matches` |
| Backfill/update 当前 DB prompt node | 合入前验证通过，生产待激活 | `seed_catalogs()` 从 Prompt Pack 写入 `assistant_prompt_nodes`、version、activation；生产 DB 未写入，合入后需执行迁移/seed |
| `/health` 不暴露 `mouse_keyboard_control=false` / `code_write=false` | 通过 | `health()` 改为 `implemented_capabilities` 与真实 `governance_boundaries`；pytest 覆盖 |
| 保留 MCP/API、approval、Trace、Memory/Audit 真实边界 | 通过 | Prompt Pack governance 节点、service health、MCP preflight/approval 逻辑未弱化 |
| 后端测试覆盖 root prompt 和 prompt-node listing | 通过 | `test_bug117_prompt_and_health_do_not_expose_undeveloped_capability_bans` |

## 执行的验证命令

```powershell
python -m pytest backend/tests/research_assistant -q
# 25 passed in 13.55s

python -m compileall backend/services/research_assistant backend/routers/research_assistant.py backend/db/init_research_assistant_schema_20260521.py
# passed

python scripts/aistock_issue_workflow.py doctor
# workflow_gate=ready; repo_git clean; canonical root clean

rg -n "禁止控制鼠标键盘|禁止写代码|mouse_keyboard_control|code_write" prompt_packs\research_assistant\main configs\research_assistant
# no matches

rg -n "_PRIOR_MESSAGES_TOKEN_BUDGET|_TOKEN_ESTIMATE_CHARS_PER_TOKEN|token_budget=64000|temperature=0\.2|max_tokens=1600|content\[:500\]|limit=500|limit=200|limit=100|limit=50|limit=20|token_budget: int = Field\(16000" backend\services\research_assistant backend\routers\research_assistant.py
# no matches

npm --prefix frontend run build
# passed; only pre-existing react-hooks/exhaustive-deps warnings in unrelated pages

git diff --check origin/main...HEAD
# passed

new DDL comment coverage check
# missing_comment_count 0
```

Frontend build 使用临时 junction 将本 worktree 的 `frontend/node_modules` 指向已修复的 `F:\Dev\AIstock\frontend\node_modules`，验证完成后已删除 junction 和 `.next`；未修改依赖文件。

## 生产门禁

- `production_ddl_gate`: `pending_before_merge`。本分支修改 `backend/db/init_research_assistant_schema_20260521.py` 并新增 runtime/prompt/context tables/columns/comments；合入 `main` 后、生产 runtime activation 前必须执行并验证 committed DDL。
- `production_backend_dependency_gate`: `noop`。未修改 Python dependency files。
- `production_frontend_dependency_gate`: `noop`。未修改 frontend dependency files；仅复用已存在 `@assistant-ui/react` 依赖完成 build。
- `runtime_touch`: `none`。未启动、停止或重启生产 backend `8001` / frontend `3000`。

## 结论

当前分支已达到设计矩阵 A1-A10 的本地验收条件，具备提交 PR/合入前评审基础；但在用户决定合入前，不关闭 `BUG-117` / GitHub Issue `#186`。生产 readiness 仍取决于合入后的 DDL 执行与验证。
