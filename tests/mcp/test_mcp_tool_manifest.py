from __future__ import annotations

import importlib
from dataclasses import replace

from backend.mcp.tool_manifest import (
    MODULE_TOOL_NAMES,
    NON_DIRECT_RISK_LEVELS,
    SIDE_EFFECT_NAME_TOKENS,
    TOOL_MANIFEST,
    TOOL_MANIFEST_BY_NAME,
    TOOL_METADATA_OVERRIDES,
    legacy_tool_count,
    _migration_state_for,
    platform_tool_count,
    validate_manifest,
)


def test_manifest_counts_and_required_metadata() -> None:
    assert legacy_tool_count() == 368
    assert platform_tool_count() == 6
    assert len(TOOL_MANIFEST) == 374
    assert len(TOOL_MANIFEST_BY_NAME) == 374
    assert validate_manifest() == []
    for entry in TOOL_MANIFEST:
        assert entry.tool_name
        assert entry.module
        assert entry.profile_tags
        assert entry.risk_level
        assert entry.backend_endpoint
        assert entry.response_budget
        assert entry.assistant_usable
        assert entry.migration_state == _migration_state_for(entry.tool_name, entry.module)
    assert {entry.migration_state for entry in TOOL_MANIFEST} == {"gateway"}


def test_module_tool_names_match_module_constants() -> None:
    for module, tool_names in MODULE_TOOL_NAMES.items():
        imported = importlib.import_module(f"backend.mcp.modules.{module}")
        assert tuple(imported.TOOL_NAMES) == tuple(tool_names)
        assert imported.TOOL_COUNT == len(tool_names)


def test_high_risk_tools_have_preflight_metadata() -> None:
    for name in [
        "qe_experiment_run_confirmed",
        "qe_archive_backfill_execute_confirmed",
        "start_validation_execution",
        "mcp_github_issue_create",
    ]:
        entry = TOOL_MANIFEST_BY_NAME[name]
        assert entry.requires_confirmation or entry.risk_level in {"long_running", "external_network", "write_confirmed"}
        assert entry.assistant_usable == "preflight_required"


def test_manifest_risk_no_write_as_readonly() -> None:
    read_only_exemptions = {
        name
        for name, override in TOOL_METADATA_OVERRIDES.items()
        if override.risk_level == "read_only"
        and override.assistant_usable == "direct_or_catalog"
        and ("plan-only preview" in override.reason or "read-only" in override.reason.lower() or "GET " in override.reason)
    }
    assert {
        "factor_library_plan_register",
        "factor_library_plan_deprecate",
        "factor_metrics_plan",
        "factor_corr_plan",
        "model_registry_plan_register",
        "strategy_governance_plan_promotion",
        "strategy_governance_plan_retirement",
        "execution_policy_plan_binding",
        "advisory_list_bindings",
        "advisory_get_active_binding",
        "paper_v2_monitoring_get_scheduler_status",
        "paper_v2_monitoring_get_scheduler_bootstrap_status",
        "local_data_plan_schedule_reset",
        "local_data_plan_repair",
        "qlib_export_plan_dataset_update",
        "local_data_list_sync_targets",
        "local_data_get_sync_target",
        "local_data_list_sync_attempts",
        "local_data_list_schedules",
        "local_data_get_schedule_defaults",
        "local_data_list_source_test_runs",
        "local_data_list_source_test_schedules",
        "local_data_get_repair_status",
        "qe_archive_list_runs",
        "qe_archive_get_run_quality",
        "qe_archive_list_backfill_runs",
        "qe_archive_get_backfill_run",
        "qe_archive_query_run_leaderboard",
        "qe_archive_query_topk_quality",
        "list_validation_runs",
        "get_validation_run",
    } <= read_only_exemptions
    for entry in TOOL_MANIFEST:
        if any(token in entry.tool_name for token in SIDE_EFFECT_NAME_TOKENS):
            if entry.tool_name in read_only_exemptions:
                assert entry.risk_level == "read_only"
                assert entry.assistant_usable == "direct_or_catalog"
                assert TOOL_METADATA_OVERRIDES[entry.tool_name].reason
                continue
            assert entry.risk_level in NON_DIRECT_RISK_LEVELS
            assert entry.assistant_usable == "preflight_required"


def test_external_research_l25_read_only_retrieval_stays_direct() -> None:
    for name in [
        "external_research_search_web",
        "external_research_search_papers",
        "external_research_fetch_extract",
    ]:
        entry = TOOL_MANIFEST_BY_NAME[name]
        assert entry.risk_level == "read_only"
        assert entry.assistant_usable == "direct_or_catalog"
        assert entry.requires_confirmation is False
        assert "L2.5 evidence-first read-only retrieval" in TOOL_METADATA_OVERRIDES[name].reason

    save_evidence = TOOL_MANIFEST_BY_NAME["external_research_save_evidence"]
    assert save_evidence.risk_level in NON_DIRECT_RISK_LEVELS
    assert save_evidence.assistant_usable == "preflight_required"


def test_qlib_export_candidate_generation_is_confirmed_and_candidate_only() -> None:
    for name in [
        "qlib_export_run_h5_dataset_full_confirmed",
        "qlib_export_run_h5_dataset_incremental_confirmed",
        "qlib_export_run_h5_daily_aux_incremental_all_confirmed",
        "qlib_export_build_static_factors_confirmed",
        "qlib_export_export_field_map_confirmed",
        "qlib_export_run_bin_unified_v2_confirmed",
        "qlib_export_generate_backtest_candidate_confirmed",
    ]:
        entry = TOOL_MANIFEST_BY_NAME[name]
        assert entry.requires_confirmation is True
        assert entry.risk_level in NON_DIRECT_RISK_LEVELS
        assert entry.assistant_usable == "preflight_required"
        assert entry.backend_endpoint == "qlib/*"

    plan_entry = TOOL_MANIFEST_BY_NAME["qlib_export_plan_dataset_update"]
    assert plan_entry.risk_level == "read_only"
    assert plan_entry.requires_confirmation is False
    assert plan_entry.assistant_usable == "direct_or_catalog"


def test_manifest_metadata_override_reasons_are_required() -> None:
    for tool_name, override in TOOL_METADATA_OVERRIDES.items():
        assert tool_name in TOOL_MANIFEST_BY_NAME
        assert override.reason.strip()


def test_migration_state_is_derived_and_overrideable() -> None:
    assert _migration_state_for("health", "validation", gateway_modules={"validation"}, script_backed_servers=set()) == "gateway"
    assert _migration_state_for("health", "validation", gateway_modules=set(), script_backed_servers={"validation"}) == "script_backed"
    assert (
        _migration_state_for(
            "health",
            "validation",
            gateway_modules={"validation"},
            script_backed_servers=set(),
            overrides={"health": "wrapper_compat"},
        )
        == "wrapper_compat"
    )
    assert (
        _migration_state_for(
            "health",
            "validation",
            gateway_modules={"validation"},
            script_backed_servers=set(),
            overrides={"health": "deprecated_pending_approval"},
        )
        == "deprecated_pending_approval"
    )


def test_paper_v2_current_phase_excludes_runtime_control_tools() -> None:
    forbidden_fragments = (
        "paper_v2_create_portfolio",
        "paper_v2_enable_auto_run",
        "paper_v2_disable_auto_run",
        "paper_v2_run_day",
        "paper_v2_start_session",
        "paper_v2_pause_session",
        "paper_v2_resume_session",
        "paper_v2_stop_session",
        "paper_v2_scheduler_start",
        "paper_v2_scheduler_stop",
        "qmt_broker_place_order",
        "qmt_broker_cancel_order",
        "qmt_broker_bank",
        "qmt_virtual",
    )
    exposed = {entry.tool_name for entry in TOOL_MANIFEST}
    assert not [name for name in exposed if any(fragment in name for fragment in forbidden_fragments)]
    assert TOOL_MANIFEST_BY_NAME["paper_v2_monitoring_list_positions"].risk_level == "read_only"
    assert TOOL_MANIFEST_BY_NAME["qmt_broker_monitoring_get_snapshot"].risk_level == "read_only"


def test_manifest_validation_rejects_invalid_migration_state() -> None:
    health_entry = next(entry for entry in TOOL_MANIFEST if entry.tool_name == "mcp_gateway_health")
    bad_entry = replace(health_entry, migration_state="unknown_state")
    assert validate_manifest([bad_entry]) == ["invalid migration_state for mcp_gateway_health: unknown_state"]
