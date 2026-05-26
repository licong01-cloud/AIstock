# BUG-105/BUG-109/BUG-117 Research Assistant Close-Sync Validation - 2026-05-27

## Scope

- Module: `research_assistant` / Research Assistant.
- Issues: `BUG-105` / GitHub `#168`, `BUG-109` / GitHub `#177`, `BUG-117` / GitHub `#186`.
- Worktree: `F:/Dev/AIstock_worktrees/BUG-117-research-assistant-bug-117-close-sync-20260526-20260526`.
- Branch: `bug/BUG-117-research-assistant-bug-117-close-sync-20260526-20260526`.
- Base observed for validation: `origin/main=d3f04636`.
- This pass is a governance close-sync pass: code fixes were already present on `main`; this branch only adds closure evidence, BUG JSON sync, GitHub issue sync, and one test-drift correction.
- Production runtime boundary: no backend `8001` start/stop/restart, no frontend `3000` start/stop/restart, and no production DB write in this close-sync pass.

## Fix Chain Summary

| BUG | GitHub | Status before close-sync | Fix evidence on main | Close-sync conclusion |
|---|---:|---|---|---|
| `BUG-105` | `#168` | BUG JSON `in_progress`; GitHub already closed but still had `status:open` label | `99bf5b71`, `325fd755`, `551f4dc1` preserve `conversation_id` and inject bounded sliding-window chat history | Fixed; registry and GitHub status synchronized in this pass |
| `BUG-109` | `#177` | BUG JSON `open`; GitHub open | `05e5c7d4` / `55f56bb2` human-dialogue governance removes fixed `QE 10 loop` defaults and keyword-triggered workflow noise | Fixed; registry and GitHub issue should close as `status:fixed` |
| `BUG-117` | `#186` | BUG JSON `open`; GitHub open | PR `#198` plus `aa6ebebd`, `b7649f63`, `d393d203` remove undeveloped capability bans from active prompt/runtime surfaces and verify prompt/runtime DDL activation | Fixed; registry and GitHub issue should close as `status:fixed` |

## Closure Matrix

| Requirement | Evidence | Result |
|---|---|---|
| BUG-105 chat keeps context after first turn | Frontend preserves active `conversation_id`; backend uses token-aware prior message sliding window; `python -m pytest ...` includes service/API coverage | PASS |
| BUG-105 history injection excludes raw JSON/trace/backend logs | Existing service tests and current regression suite passed; main chat E2E verifies no raw payload/trace/task IDs in primary chat | PASS |
| BUG-109 no fixed QE 10 loop default in active paths | Static scans for `QE 10 loop`, `generate 10 loops`, `10 loop target`, `create a QE 10 loop`, `cardText(` in active scoped paths returned no matches | PASS |
| BUG-109 keywords alone do not start QE workflow | `20260526_human_dialogue_governance_validation.md` E0A-E0H matrix and current backend/E2E regression passed | PASS |
| BUG-117 active prompt/runtime surfaces do not expose undeveloped capability bans | Static scans for legacy mouse/keyboard ban phrases, legacy code-write ban phrases, `mouse_keyboard_control`, and `code_write` in active scoped paths returned no matches | PASS |
| BUG-117 governance boundaries remain real | Prompt/runtime governance keeps MCP/API approval, Trace and Memory/Audit boundaries; health payload uses implemented capability summary | PASS |
| MCP gateway Research Assistant tool count is not hardcoded | `backend/tests/mcp/test_profiles_registry_gateway.py` now imports `TOOL_COUNT` from `backend.mcp.modules.research_assistant` instead of asserting stale literal `10` | PASS |

## Validation Commands

```powershell
python -m pytest -q backend/tests/research_assistant backend/tests/mcp/test_research_assistant_module.py backend/tests/mcp/test_profiles_registry_gateway.py -p no:cacheprovider
# 60 passed in 19.32s

python -m compileall backend/services/research_assistant backend/mcp/modules/research_assistant.py backend/tests/mcp/test_profiles_registry_gateway.py
# passed

npm --prefix frontend run build
# passed; only pre-existing unrelated react-hooks/exhaustive-deps warnings in non-Research-Assistant modules

cd frontend
npx playwright test tests/research-assistant/research-assistant.spec.ts --project chromium
# 3 passed in 18.5s

rg -n "QE 10 loop|generate 10 loops|10 loop target|create a QE 10 loop|cardText\(" backend/services/research_assistant prompt_packs configs frontend/src/app/research-assistant backend/mcp/modules/research_assistant.py
# no matches

rg -n "mouse_keyboard_control|code_write|legacy Chinese mouse/keyboard ban phrase|legacy Chinese code-write ban phrase" backend/services/research_assistant prompt_packs configs frontend/src/app/research-assistant backend/mcp/modules/research_assistant.py
# no matches

git diff --check
# passed
```

## Production Gates

| Gate | Result | Notes |
|---|---|---|
| `production_ddl_gate` | `noop` for this close-sync branch | No new DDL or DB object changes in this branch. Prior BUG-117 prompt/runtime schema DDL was already `applied_and_verified` in `20260526_final_runtime_activation_validation.md`. |
| `production_backend_dependency_gate` | `noop` | No Python dependency files changed. |
| `production_frontend_dependency_gate` | `noop` | No frontend dependency files changed. Frontend build temporarily reused existing root `node_modules` via a junction and the junction was removed after validation. |
| Production backend `8001` | not touched | No start/stop/restart. A later read-only GET attempt found `8001` unavailable, so this pass does not claim new live-runtime smoke. |
| Production frontend `3000` | not touched | No start/stop/restart. |
| Production DB | not touched | No write or migration during this close-sync pass. |

## Issue Workflow Notes

- `python scripts/aistock_issue_workflow.py doctor` had `workflow_gate=warning` only because canonical root contains an unrelated dirty `BUG-016` JSON; user explicitly allowed ignoring that unrelated root change and continuing in the isolated worktree.
- `start-batch` could not batch the three BUGs because required-verification signatures differ; this close-sync branch acts as a governance integrator only.
- `finish --bug-id BUG-117` initially flagged `backend/tests/mcp/test_profiles_registry_gateway.py` as out of scope; user approved scope expansion, and BUG-117 `allowed_write_scope` now records the approved test-drift correction and same-module closure records.

## Conclusion

`BUG-105`, `BUG-109`, and `BUG-117` are fixed in current `origin/main` code. This close-sync pass records validation evidence, updates BUG JSON status to `fixed`, corrects the Research Assistant MCP tool-count test drift, and prepares GitHub issue status synchronization.

## Post-Main-Sync Revalidation - 2026-05-27

After PR #237 was merged with current `origin/main` through `33c600fd` (`BUG-121 compact MCP token-heavy responses`) and prior `BUG-122` Paper v2 CI closure, the close-sync branch was revalidated without touching production `8001`, `3000`, or the production DB.

| Command | Result |
|---|---|
| `python -m pytest -q backend/tests/research_assistant backend/tests/mcp/test_research_assistant_module.py backend/tests/mcp/test_profiles_registry_gateway.py -p no:cacheprovider` | `60 passed in 17.83s` |
| `python -m pytest -q backend/tests/scripts/test_aistock_mcp_github_issue_tools.py backend/tests/scripts/test_bug_github_sync.py backend/tests/mcp/test_profiles_registry_gateway.py -p no:cacheprovider` | `64 passed in 2.42s` |
| `python -m pytest backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/strategy_package --ignore-glob=backend/tests/paper_trading_v2/*dev_db*.py --ignore=backend/tests/paper_trading_v2/test_runtime_enable_paper_compat.py -k 'not test_model_asset_resolver_uses_aistock_cache_without_wsl_unc_probe' -q -p no:cacheprovider` | `493 passed, 1 skipped, 1 deselected in 18.92s` |
| `python -m ruff check --force-exclude backend/tests/mcp/test_profiles_registry_gateway.py` | `All checks passed` |
| `python -m compileall backend/services/research_assistant backend/mcp/modules/research_assistant.py backend/tests/mcp/test_profiles_registry_gateway.py` | passed |
| Static scan for BUG-109 legacy QE/cardText patterns | no matches |
| Static scan for BUG-117 undeveloped capability patterns | no matches |
| `gh issue view 168/177/186 --json number,title,state,labels,closedAt,url` | all three GitHub issues are `CLOSED` with `status:fixed` |

Workflow notes after revalidation:

- `finish-batch` still correctly refuses to batch these BUG records because their `required_verification` signatures differ.
- Individual `finish --bug-id BUG-117 --plan-only` passes scope/pre-PR gate after the user-approved close-sync scope expansion.
- Individual `finish --bug-id BUG-105` and `finish --bug-id BUG-109` report scope-check warnings only because the governance integrator branch intentionally carries shared same-module close-sync files; their BUG JSON records now explicitly include the approved shared test-drift/sync scope and the post-main-sync evidence.
