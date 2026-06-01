# Research Assistant Phase 4 External Research Validation

- branch: codex/ra-external-research-20260601
- batch_id: ra_phase4
- plan_key: ra_phase4_external_research
- validated_commit: 6d42caa6fe6fb5cb7f1c4f38feb71364317603aa
- g1_central_run_id: research-assistant-external-research_20260601_135812_l2-5_ra-phase4-external-research_125bc879_runner-validation__9a843fe183
- g1_central: status=passed return_code=0 production_8001_touched=false
- g1_local: nox -s ra_phase4_external_research => 74 passed; catalog_integrity passed; ownership_scan mapped=28 unmapped=0 ambiguous=0
- production_ddl_gate: noop
- touched_8001_or_3000: false

## G2 Closure Requirements
- CR-P4-01: done=true - external_research core/provider interface landed; evidence=backend/services/research_assistant/external_research.py + test_external_research_provider_contract.py
- CR-P4-02: done=true - MCP gateway module exposes four tools; evidence=backend/mcp/modules/external_research.py + test_external_research_module.py
- CR-P4-03: done=true - backend facade exposes /api/v1/external-research/*; evidence=backend/routers/external_research.py + test_external_research_evidence_first.py
- CR-P4-04: done=true - .mcp.json registers aistock-external-research on 8001 runtime config; evidence=.mcp.json + test_profiles_registry_gateway.py
- CR-P4-05: done=true - profile external_research registered in gateway profiles; evidence=backend/mcp/profiles.py + test_gateway_loads_external_research_tools
- CR-P4-06: done=true - four tools keep declared read_only/draft_only strategy; evidence=test_external_research_module.py + mcp_catalog_sync.py
- CR-P4-07: done=true - candidate branch whitelist is only external.* and personal.topic.*; evidence=test_external_research_candidate_branch_whitelist_is_external_or_personal_topic_only
- CR-P4-08: done=true - non-whitelisted branches including project.topic.* are rejected; evidence=test_external_research_candidate_rejects_project_topic_and_unknown_branches
- CR-P4-09: done=true - search results are evidence candidates, not conclusions; evidence=test_external_search_results_are_evidence_candidates_not_conclusions
- CR-P4-10: done=true - summary-first URL/as_of/provenance returned; evidence=test_external_research_summary_is_provenance_first_and_token_safe
- CR-P4-11: done=true - long content uses capped preview/detail refs; evidence=test_external_fetch_extract_returns_capped_preview_and_detail_ref
- CR-P4-12: done=true - token budget rejects heavy fields and huge strings; evidence=test_external_research_payload_guard_fails_on_full_text_or_large_values
- CR-P4-13: done=true - save_evidence produces draft candidate only; evidence=test_save_evidence_candidate_is_draft_only_and_branch_limited
- CR-P4-14: done=true - L4 handoff limited to hypothesis + low-cost metadata; no QE/high-cost wiring; evidence=test_external_research_phase4_does_not_wire_qe_or_high_cost_experiment_apis
- CR-P4-15: done=true - external results are backfilled into ReAct messages before final answer; evidence=test_external_research_tool_result_is_backfilled_into_react_messages_before_answer
- CR-P4-16: done=true - save_evidence is preflight-only; approved memory write executor is not called; evidence=test_external_save_evidence_is_preflight_only_not_approved_memory_write
- CR-P4-17: done=true - natural-language routing can select external research tools; evidence=test_new_domain_routes_select_expected_tools
- CR-P4-18: done=true - catalog sync advertises external research server/tools; evidence=test_default_catalog_contains_all_current_and_new_mcp_tools
- CR-P4-19: done=true - core has no AIstock DB/router/MCP/service imports; evidence=test_memory_core_modules_do_not_import_aistock_adapters_or_domain_services
- CR-P4-20: done=true - core does not add embedding/vector/similarity retrieval; evidence=test_memory_core_modules_do_not_use_forbidden_similarity_retrieval
- CR-P4-21: done=true - gate is self-contained; FastAPI TestClient/static smoke only; evidence=test_facade_gate_is_self_contained_and_does_not_touch_production_8001
- CR-P4-22: done=true - list_tools_smoke shows four schemas and production_8001_touched=false; evidence=test_external_research_list_tools_smoke_uses_static_introspection_not_8001
- CR-P4-23: done=true - Validation Center plan/nox/session/ownership/catalog are registered; evidence=nox -s ra_phase4_external_research + catalog_integrity
- CR-P4-24: done=true - G1/G2/G3 recorded with production_ddl_gate=noop; evidence=this validation record + blueprint section 12 row

## G3 Blueprint Traceability
- Updated docs/architecture/research_assistant_architecture_upgrade_blueprint_20260530.md section 12 row: External Research MCP.
- Also aligned Phase 4 whitelist wording to external.* / personal.topic.* only.

## Notes
- Gate is self-contained and requires_backend=false; it uses deterministic fake providers/TestClient/static tool introspection only.
- L4/QE high-cost execution is intentionally not wired in Phase 4; candidates carry hypothesis and low_cost_intent metadata only.
