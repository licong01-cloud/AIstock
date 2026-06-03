# Research Assistant Phase 3 ReAct Grounding Validation

- batch_id: `ra_phase3`
- branch: `codex/ra-react-grounding-20260601`
- implementation_commit: `5f0d7e08045f3eb7365a00eb988251113673b0dc`
- pre_rebase_g1_central_commit: `1dad08ed2acf638f4fead2d9dda88aaa903e78e3`
- validation_plan: `ra_phase3_react_grounding`
- validated_workspace: `F:/Dev/AIstock_worktrees/ra-react-grounding-20260601`
- runtime_boundary: no production `8001` / `3000` start, stop, restart, or process operation was performed.
- production_ddl_gate: `noop`; Phase 3 is pure logic/config/test wiring and adds no DDL.
- production_db_gate: `noop`; no production database write or migration was performed.

## G1-local Evidence

| Gate | Evidence | Status |
| --- | --- | --- |
| Target nox | `nox -s ra_phase3_react_grounding` | pass: 55 tests, catalog integrity 0 findings, ownership scan mapped=17/17 ambiguous=0 |
| Research Assistant service regression | `python -m pytest backend/tests/research_assistant/test_service.py -q -p no:cacheprovider` | pass: 45 tests |
| ReAct targeted suite | `python -m pytest backend/tests/research_assistant/test_react_tool_loop.py backend/tests/research_assistant/test_tool_catalog_gate.py backend/tests/research_assistant/test_evidence_guard.py backend/tests/research_assistant/test_reflexion_retry.py backend/tests/research_assistant/test_react_phase1_phase2_context_regression.py backend/tests/research_assistant/test_core_no_adapter_import.py -q -p no:cacheprovider` | pass: 10 tests |
| Production isolation | validation catalog summary reported `production_8001_touched=false`, `production_db_touched=false` | pass |

## G1-central Evidence

- G1-central run_id: `research-assistant-react-grounding_20260601_062917_l2_ra-phase3-react-grounding_0721d5b3_runner-validation__0cd2e47ef0`
- job_id: `valjob_20260601_062859_0721d5b3`
- status: `passed`
- return_code: `0`
- production_8001_touched: `false`
- production_db_touched: `false`
- workspace_scope: `worktree`
- expected_branch: `codex/ra-react-grounding-20260601`
- expected_commit: `1dad08ed2acf638f4fead2d9dda88aaa903e78e3`
- runner_run_record: `tests/aistock_validation/history/research-assistant-react-grounding/20260601_062917_l2_ra-phase3-react-grounding_0721d5b3_runner-validation.md`
- runner_log: `tests/aistock_validation/history/research-assistant-react-grounding/20260601_062917_l2_ra-phase3-react-grounding_0721d5b3_runner-runner-log.txt`
- invocation:
  - `plan_key=ra_phase3_react_grounding`
  - `workspace_path=F:/Dev/AIstock_worktrees/ra-react-grounding-20260601`
  - `expected_branch=codex/ra-react-grounding-20260601`
  - `expected_commit=1dad08ed2acf638f4fead2d9dda88aaa903e78e3`


## Terminal Review Evidence

- Independent terminal-review G1-central run_id: `research-assistant-react-grounding_20260601_065251_l2_ra-phase3-react-grounding_c1028802_runner-validation__9699ab021d`
- Independent review job_id: `valjob_20260601_065235_c1028802`
- Independent review status: `passed`; `return_code=0`; `production_8001_touched=false`.
- Independent review validated pre-rebase terminal-review commit `00561402fb9a9992e69aba4f1060c20289292614`; after rebase, G1-local was rerun against branch content on top of `origin/main`.
- Independent review runner record: `tests/aistock_validation/history/research-assistant-react-grounding/20260601_065251_l2_ra-phase3-react-grounding_c1028802_runner-validation.md`

## Implementation Verification Summary

- `backend/services/research_assistant/react_grounding.py` implements the provider-only ReAct core: `run_react_grounding_loop`, `assert_tool_in_catalog`, `compose_with_evidence_guard`, deterministic tool-call sorting, and internal-chain stripping.
- `backend/services/research_assistant/service.py:chat_turn` consumes the ReAct result inside the same turn: structured tool calls are executed through the service MCP provider, tool results are appended to messages, and a second model completion produces the final answer.
- `_ServiceReactMcpProvider.execute_read_only` uses the existing action proposal / preflight / execution path for low-risk read-only MCP calls, so tests cover the real chat-turn consumption path rather than helper-only mocks.
- `_ServiceReactMcpProvider.preflight_confirmation_only` creates action proposal and preflight cards for write/high-risk tools, and never calls `execute_action_proposal` inside ReAct.
- `_chat_messages_for_llm` injects a compact Context Pack Evidence Manifest, including Phase 1 `route_reason`/resident directive-preference memory and Phase 2 `graph_relation_refs`, and the regression test asserts those fields are consumed by ReAct prompt messages.
- `configs/research_assistant/runtime_context.yaml` injects `react_grounding.max_tool_iterations`; the core does not hard-code the iteration limit.

## Plan B H1 Determinism

- `test_react_tool_loop.py` and `test_reflexion_retry.py` use deterministic fake LLM scripts and deterministic fake/summary MCP providers; they do not call a real model, network, or production service.
- `react_grounding.py` sorts `tool_calls` and merged `tool_results` by `(server_key, tool_name, stable_call_id)`; tests assert true route/result content instead of relying on incidental order.
- Reflexion retry follows a fixed fail -> retry directive -> success script and verifies the successful sourced result.

## DESIGN-COMPLIANCE-001 Closure Requirements

| ID | Requirement | Evidence | Done |
| --- | --- | --- | --- |
| CR-P3-01 | ReAct core exists in `react_grounding.py` and is provider-only | `react_grounding.py`; `test_core_no_adapter_import.py`; `nox -s ra_phase3_react_grounding` | true |
| CR-P3-02 | `chat_turn` uses ReAct backfill instead of single-shot plus post-hoc card attachment | `service.py:chat_turn`; `_complete_chat_with_react_grounding`; `test_react_tool_loop.py` | true |
| CR-P3-03 | Structured tool calls execute real read-only MCP path, tool results enter messages, then final answer is generated | `test_react_tool_loop.py`; `test_service.py`; G1-local/G1-central | true |
| CR-P3-04 | Catalog gate rejects tools outside audited catalog and refuses hallucinated tools | `assert_tool_in_catalog`; `test_tool_catalog_gate.py::test_catalog_outside_tool_is_rejected_and_not_executed` | true |
| CR-P3-05 | Write/high-risk tools produce preflight + confirmation cards only and do not call execution | `_ServiceReactMcpProvider.preflight_confirmation_only`; `test_tool_catalog_gate.py::test_high_risk_tool_creates_preflight_card_without_execute` | true |
| CR-P3-06 | Evidence guard blocks placeholders and unsourced numeric/tool-grounded conclusions | `compose_with_evidence_guard`; `test_evidence_guard.py` | true |
| CR-P3-07 | Reflexion retry is bounded, deterministic, and fails fast when evidence remains insufficient | `run_react_grounding_loop`; `test_reflexion_retry.py` | true |
| CR-P3-08 | Main assistant bubble hides thought/observation/reflexion/internal chain | `strip_internal_chain`; `test_react_tool_loop.py` | true |
| CR-P3-09 | Tool-call execution and result merge order are deterministic | `McpToolCall.sorted_key`; `McpToolResult.sorted_key`; `test_reflexion_retry.py` | true |
| CR-P3-10 | Phase 1 memory tree route/resident directive-preference remains consumed in ReAct prompt | `test_react_phase1_phase2_context_regression.py` | true |
| CR-P3-11 | Phase 2 graph relation refs are consumed in ReAct prompt, not merely preserved in context pack | `test_react_phase1_phase2_context_regression.py` | true |
| CR-P3-12 | Runtime config injects `max_tool_iterations`; core does not hard-code domain/server assumptions | `runtime_context.yaml`; `_react_grounding_config`; `test_core_no_adapter_import.py` | true |
| CR-P3-13 | Validation plan is runner-enabled and allowlisted through catalog + nox + ownership rules | `test_plans.yaml`; `plan_catalog.py`; `noxfile.py`; `file_ownership.yaml`; G1-local | true |
| CR-P3-14 | G1-central passed with `return_code=0`, `production_8001_touched=false`, and production gates noop | primary `research-assistant-react-grounding_20260601_062917_l2_ra-phase3-react-grounding_0721d5b3_runner-validation__0cd2e47ef0`; terminal-review `research-assistant-react-grounding_20260601_065251_l2_ra-phase3-react-grounding_c1028802_runner-validation__9699ab021d` | true |

## Production Gates

- production_ddl_gate: `noop`
- production_frontend_dependency_gate: `noop`
- production_backend_dependency_gate: `noop`
- production runtime touched: false
- production DB touched: false
