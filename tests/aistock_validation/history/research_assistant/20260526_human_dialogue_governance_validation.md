# Research Assistant Human Dialogue Governance Validation - 2026-05-26

## Scope

- Worktree: `F:\Dev\AIstock_worktrees\research-assistant-human-dialogue-20260526`
- Branch: `feature/research-assistant-human-dialogue-20260526`
- Design authority: `docs/architecture/research_assistant_mcp_skill_execution_closure_design_20260525.md` section E0A-E0H.
- Production services: not touched. No backend `8001` restart, no frontend `3000` activation, no production DB writes.

## Change Summary

- Introduced intent-gated dialogue routing for `capability_inquiry`, `concept_explanation`, `status_query`, `bug_diagnosis_request`, `issue_intake_request`, `experiment_draft_request`, `experiment_validation_request`, `experiment_execution_request`, `ambiguous_request`, and `general_chat`.
- Removed default QE loop-count wording from active prompt/config/frontend/test surfaces; loop or iteration count is now accepted only from user input, runtime config, or audited context.
- Prevented QE/domain workflow prompt branches, action proposals, confirmation cards, and tool guard prompts from being selected by keyword-only capability/status/concept turns.
- Moved chat/workbench user-visible copy to `frontend/src/lib/research-assistant/ui-copy.ts`; moved backend chat event messages and human-card templates into `configs/research_assistant/runtime_context.yaml`.
- Kept plan, confirmation, trace, payload, and proposal details out of the main assistant chat bubble; details remain in side cards/admin/workbench surfaces.

## E0 Acceptance Matrix

| ID | Requirement | Implementation Evidence | Validation Evidence | Result |
|---|---|---|---|---|
| E0A | Capability inquiry answers directly and does not generate plan/confirmation/action workflow noise | `DialogueIntent.CAPABILITY_INQUIRY`; prompt selection only includes `root.assistant` and `intent.planning`; cards have empty plan steps, clarifications, and proposals | `test_prompt_tree_capability_inquiry_does_not_trigger_qe_workflow`; `test_chat_turn_capability_inquiry_answers_without_workflow_noise`; Playwright main chat test | PASS |
| E0B | Bug diagnosis is first-class and not covered by QE draft flow | `DialogueIntent.BUG_DIAGNOSIS_REQUEST`; runtime card template `bug_diagnosis_request`; no QE domain/workflow prompt branch for bug diagnosis | `test_chat_turn_bug_diagnosis_request_is_first_class_intent` | PASS |
| E0C | Keywords alone do not trigger workflow | Intent gate requires explicit task intent; capability/concept/status and ambiguous turns do not select QE workflow/tool guard | `test_prompt_tree_capability_inquiry_does_not_trigger_qe_workflow`; `test_prompt_tree_ambiguous_task_does_not_start_qe_workflow` | PASS |
| E0D | Default `10 loop` pollution is removed | Prompt pack, runtime config, backend cards, frontend copy, tests, and fixtures no longer use default `10 loop` or `10 个 loop` execution examples | Static scan for `QE 10 loop`, `10 个 loop`, `生成 10 个 loop`, related legacy text: no matches in active scoped paths | PASS |
| E0E | Only explicit task requests enter planning/preflight/execution chain | `_select_prompt_nodes()` adds QE domain/workflow only for experiment draft/validation/execution intents; tool guard only for validation/execution; governance only for issue intake/execution | Backend prompt-tree tests for capability, ambiguous, draft, and validation intents | PASS |
| E0F | Main answer bubble stays lean | `_compose_assistant_reply()` returns LLM text without appending cards; chat UI no longer has `cardText()`; details are side card/admin/workbench | Playwright verifies main chat has no raw JSON, `trace_id`, `task_id`, or appended workflow boilerplate | PASS |
| E0G | User-visible wording is governed outside Python/TSX business logic where practical | Runtime config governs backend fallback, capability summaries, event messages, card templates, status rails; UI copy config governs chat/workbench welcome, placeholders, labels, examples, and defaults | Static TSX scan for Chinese hardcoded strings in chat/workbench returned no matches; runtime config validator requires `event_messages` and dialogue templates | PASS |
| E0H | Human-like assistant style: direct, concise, contextual, restrained | Prompt Pack root/intent/renderer nodes specify direct-answer-first and no meta explanation; frontend hero/welcome and templates avoid process-heavy defaults | Capability-inquiry backend and Playwright tests verify direct answer and absence of materialize/run boilerplate | PASS |

## Validation Commands

```powershell
rtk proxy python -m pytest -q backend/tests/research_assistant/test_service.py backend/tests/research_assistant/test_api.py backend/tests/research_assistant/test_execution_closure.py backend/tests/mcp/test_research_assistant_module.py backend/tests/research_assistant/test_schema_contract.py
```

Result: `45 passed in 15.61s`.

```powershell
rtk npm --prefix frontend run lint
```

Result: exit `0`. Existing unrelated `react-hooks/exhaustive-deps` warnings remain in non-Research-Assistant modules such as `rdagent-llm`, `local-data`, `qmt`, `quantevolver`, and `scheduler`; no new Research Assistant lint error.

```powershell
rtk npm --prefix frontend run test:e2e -- tests/research-assistant/research-assistant.spec.ts
```

Result: `3 passed (10.4s)`.

```powershell
rtk rg -n "QE 10 loop|生成 10 个 loop|10 个 loop 的目标|帮我创建一个 QE 10|QE 10|我已先把本轮限制|不会在确认前执行 QE materialize/run|请先确认：|cardText\(" backend/services backend/tests frontend/src/app/research-assistant frontend/src/lib/research-assistant frontend/tests/research-assistant configs/research_assistant prompt_packs/research_assistant/main
rtk rg -n "10 个 loop" backend/services frontend/src/app/research-assistant frontend/src/lib/research-assistant configs/research_assistant prompt_packs/research_assistant/main
rtk rg -n '"[^"\n]*[\p{Han}][^"\n]*"|>[^<\n]*[\p{Han}][^<\n]*<' frontend/src/app/research-assistant/chat/page.tsx frontend/src/app/research-assistant/workbench/page.tsx
rtk rg -n "锛|銆|俙|�" frontend/src/app/research-assistant/chat/page.tsx frontend/src/app/research-assistant/workbench/page.tsx frontend/src/lib/research-assistant/ui-copy.ts
```

Result: all scans returned no matches in the scoped paths.

```powershell
rtk git diff --check
```

Result: exit `0`, no whitespace errors.

## Gates

- `production_ddl_gate=noop`: no migration, schema, or DB object changes.
- `production_backend_dependency_gate=noop`: no backend dependency file changes.
- `production_frontend_dependency_gate=noop`: no frontend dependency file changes; local ignored `node_modules` may exist only for validation.
- `production_runtime_activation=noop`: no production backend or frontend restart performed by Codex.

## Residual Notes

- This validation covers the P0 E0A-E0H human-dialogue governance slice only. It does not claim full MCP/Skill execution closure readiness.
- Work remains on the feature branch for user review and merge decision.
