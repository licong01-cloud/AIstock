# Selection Center 手工多包选股废弃实现验证记录

- 日期: 2026-07-03
- worktree: `F:\Dev\AIstock_worktrees\docs-selection-multipackage-deprecation-20260703`
- branch: `feature/selection-multipackage-deprecation-20260703`
- design: `docs/analysis/selection_center_manual_multipackage_deprecation_f2_design_20260703.md`
- 用户 scope override: 只在 Selection UI 的 router、frontend、MCP advertised tools 收口；不改 `SelectionCenterService.run_packages()`、不改 Advisory、不加 broad side-effect guard。

## Implemented Scope

- `backend/routers/selection_center.py`: `/selection-center/runs` 对 `len(package_ids)>1` 或 `mode!=single_package` 返回 HTTP 410，包含 `reason_code=selection_multi_package_adhoc_combine_deprecated` 和 `from-multi-alpha-combine-run` 引导；单包 `single_package` 仍原样 delegate 到 service。
- `backend/routers/selection_center.py`: `/selection-center/aggregate-runs` router 层返回 deprecated，不调用 `SelectionCenterService.aggregate_existing_runs()`，不创建 run，不写 DB。
- `frontend/src/app/paper-v2/selection/page.tsx`: 策略包选择从 checkbox 改为 radio；移除 `weighted_fusion` / `intersection` / `union` mode 选择、权重输入和 aggregate-runs 按钮；保留历史 run 批量选择/删除和只读查看。
- `frontend/src/lib/paper-v2/api.ts` 与 `frontend/src/components/paper-v2/ErrorPanel.tsx`: 移除 `aggregateRuns()` helper；保留 `detail.error_code` 优先解析并单独透传/显示 `detail.reason_code`。
- `backend/mcp/modules/selection_center.py` 与 `backend/mcp/tool_manifest.py`: 不再 advertise `selection_center_aggregate_runs_confirmed`；保留单包 `selection_center_run_confirmed`。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/routers/selection_center.py`; `backend/tests/selection_center/test_selection_center_api.py` | `test_selection_center_api_rejects_manual_multi_package_runs_with_reason_code`; `test_selection_center_api_rejects_non_single_mode_even_with_one_package`; `test_selection_center_api_keeps_single_package_runs_unchanged`; `python -m pytest backend/tests/selection_center/test_selection_center_api.py -q -p no:cacheprovider` -> 14 passed | verified | - |
| F-002 | `backend/routers/selection_center.py`; `backend/tests/selection_center/test_selection_center_api.py` | `test_selection_center_api_deprecates_aggregate_existing_runs_at_router` asserts HTTP 410, reason_code, source_run_ids context, and `service.calls == []`; full API test file -> 14 passed | verified | - |
| F-003 | no service implementation change by design; `backend/services/selection_center/service.py`; `backend/services/advisory_program.py` | `git diff -- backend/services/selection_center/service.py backend/services/advisory_program.py` -> empty; `python -m pytest backend/tests/watchlist/test_advisory_api.py backend/tests/watchlist/test_advisory_program.py -q -p no:cacheprovider` -> 27 passed; service runtime direct multi-package tests -> 3 passed | not_applicable_user approved | 用户明确批准 scope 收窄：不得在 `SelectionCenterService.run_packages()` / `aggregate_existing_runs()` 加 guard，避免误伤 Advisory。 |
| F-004 | `frontend/src/app/paper-v2/selection/page.tsx`; `frontend/tests/paper-v2/paper-v2-real-flow.spec.ts` | static diff shows radio selector, fixed single-package chip, no aggregate button/weights/mode select; Playwright spec updated to assert no `selection-aggregate-runs`, no `selection-mode`, no `selection-weight-*` | verified | - |
| F-005 | `backend/routers/selection_center.py`; `backend/tests/selection_center/test_selection_center_api.py`; `backend/tests/strategy_package/test_multi_alpha_paper_admission.py` | `test_selection_center_api_treats_multi_alpha_parent_as_single_package`; `python -m pytest backend/tests/strategy_package/test_multi_alpha_paper_admission.py::test_selection_full_path_lists_multi_alpha_without_localsim_dry_run_admission -q -p no:cacheprovider` -> 1 passed | verified | - |
| F-006 | read-only routes in `backend/routers/selection_center.py`; existing repository/service read paths unchanged | `test_selection_center_api_exposes_aggregate_results`; `test_selection_center_api_lists_runs_with_pagination`; `rg` leaves only deprecated API test reference to `/selection-center/aggregate-runs`; no history delete/migration changes | verified | - |
| F-007 | no broad side-effect guard by design; watchlist/portfolio/advisory/trading services unchanged | `git diff --name-only -- backend/services backend/routers/advisory.py frontend/src/app/paper-v2/advisory frontend/src/lib/api/advisory.ts` -> empty; Advisory API/program tests -> 27 passed | not_applicable_user approved | 用户明确批准 scope 收窄：不新增历史多包 run 的 watchlist/advisory/trading/portfolio broad guard，避免误伤 Advisory 合法多包 run。 |
| F-008 | `backend/mcp/modules/selection_center.py`; `backend/mcp/tool_manifest.py`; `frontend/src/lib/paper-v2/api.ts`; MCP count tests | `python -m pytest tests/mcp/test_mcp_tool_manifest.py tests/mcp/test_gateway_profiles.py tests/mcp/test_mcp_gateway_cli.py backend/tests/mcp/test_profiles_registry_gateway.py -q -p no:cacheprovider` -> 64 passed; RA seeded-count characterization -> 5 passed | verified | - |
| F-009 | router deprecated payload helper; `PaperV2ApiError.reasonCode`; `ErrorPanel` reason_code display | API tests assert `error_code`, `reason_code`, message guidance, context replacement route; `ErrorPanel` diagnostic includes `Reason Code`; `parseError()` preserves `detail.error_code` priority while exposing `reasonCode` | verified | - |
| F-010 | repo diff; validation commands; production gates | no `backend/migrations/`, `backend/db/init_*.py`, dependency manifest, or service runtime changes; no production DB command; no service start/restart | verified | - |

## Validation Commands

| command | result |
|---|---|
| `git diff --check` | pass |
| `python -m compileall backend/routers/selection_center.py backend/mcp/modules/selection_center.py backend/mcp/tool_manifest.py` | pass |
| `python -m pytest backend/tests/selection_center/test_selection_center_api.py -q -p no:cacheprovider` | 14 passed |
| `python -m pytest backend/tests/watchlist/test_advisory_api.py backend/tests/watchlist/test_advisory_program.py -q -p no:cacheprovider` | 27 passed |
| `python -m pytest backend/tests/strategy_package/test_multi_alpha_paper_admission.py::test_selection_full_path_lists_multi_alpha_without_localsim_dry_run_admission -q -p no:cacheprovider` | 1 passed |
| `python -m pytest tests/mcp/test_mcp_tool_manifest.py tests/mcp/test_gateway_profiles.py tests/mcp/test_mcp_gateway_cli.py backend/tests/mcp/test_profiles_registry_gateway.py -q -p no:cacheprovider` | 64 passed |
| `python -m pytest backend/tests/research_assistant/test_config_authority_characterization.py backend/tests/research_assistant/test_db_direct_read_parity_characterization.py -q -p no:cacheprovider` | 5 passed |
| `python -m pytest backend/tests/selection_center/test_runtime_selection.py::test_selection_center_intersection backend/tests/selection_center/test_runtime_selection.py::test_selection_center_weighted_fusion_uses_rank_normalized_scores backend/tests/selection_center/test_runtime_selection.py::test_selection_center_weighted_fusion_requires_exact_positive_weights -q -p no:cacheprovider` | 3 passed |
| `./node_modules/.bin/tsc.cmd --noEmit --incremental false` from `frontend/` | pass; used ignored local `frontend/node_modules` junction to existing root dependencies, no install |
| `npm run lint` from `frontend/` | pass with pre-existing repository warnings outside changed files |
| `python scripts/aistock_feature_workflow.py validate --design docs/analysis/selection_center_manual_multipackage_deprecation_f2_design_20260703.md --acceptance tests/aistock_validation/history/paper_v2_selection_center/20260703_selection_manual_multipackage_deprecation_implementation.md --tier F2` | pass |

## Production Gates

- `production_ddl_gate=noop`: no migrations, schema, DB init, DDL, or DB-object dependency changes.
- `production_backend_dependency_gate=noop`: no Python/Conda dependency manifest changes.
- `production_frontend_dependency_gate=noop`: no frontend dependency manifest changes; no `npm install`.
- `production_dml_gate=noop`: no production DB writes, backfills, deletes, or mutation scripts.
- `runtime_activation=noop`: no backend/frontend/TDX/scheduler start, stop, restart, or production port activation.

## Residual Risks

- Real-browser UI E2E was not run because the task explicitly forbids service start/restart. The Playwright spec was updated for the next safe L3 run, and static/type/lint checks cover the removed UI entry points.
- The approved design was broader than the user-authorized implementation scope. F-003 and F-007 are recorded as user-approved scope deviations, with targeted Advisory/service regression tests proving the intended zero regression.
