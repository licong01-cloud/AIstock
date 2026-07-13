"""Strategy Package MCP wrappers over /api/v1/strategy-packages."""

from __future__ import annotations

from ._gateway_specs import ToolSpec, register_spec_tools

CREATE_STRATEGY_PACKAGE_CONFIRM = "CREATE_STRATEGY_PACKAGE"
CREATE_CANDIDATE_STRATEGY_PACKAGE_CONFIRM = "CREATE_CANDIDATE_STRATEGY_PACKAGE"
CLONE_CANDIDATE_STRATEGY_PACKAGE_CONFIRM = "CLONE_CANDIDATE_STRATEGY_PACKAGE"
REFRESH_CANDIDATE_SNAPSHOT_CONFIRM = "REFRESH_CANDIDATE_STRATEGY_PACKAGE"
DELETE_CANDIDATE_STRATEGY_PACKAGE_CONFIRM = "DELETE_CANDIDATE_STRATEGY_PACKAGE"
REPAIR_MANIFEST_HASH_CONFIRM = "REPAIR_STRATEGY_PACKAGE_MANIFEST_HASH"
RECORD_STRATEGY_PACKAGE_ASSET_CONFIRM = "RECORD_STRATEGY_PACKAGE_ASSET"
GENERATE_SELECTION_ARTIFACTS_CONFIRM = "GENERATE_STRATEGY_PACKAGE_SELECTION_ARTIFACTS"
CREATE_EXECUTION_POLICY_CONFIRM = "CREATE_STRATEGY_PACKAGE_EXECUTION_POLICY"
TOGGLE_EXECUTION_POLICY_PAPER_CONFIRM = "TOGGLE_STRATEGY_PACKAGE_EXECUTION_POLICY_PAPER"
START_MODEL_RETRAIN_CONFIRM = "START_STRATEGY_PACKAGE_MODEL_RETRAIN"
CREATE_RUNTIME_VARIANT_CONFIRM = "CREATE_STRATEGY_PACKAGE_RUNTIME_VARIANT"
MARK_RUNTIME_VARIANT_VALIDATION_CONFIRM = "MARK_STRATEGY_PACKAGE_RUNTIME_VARIANT_VALIDATION"
CREATE_VALIDATION_RUN_CONFIRM = "CREATE_STRATEGY_PACKAGE_VALIDATION_RUN"
TRANSITION_STATUS_CONFIRM = "TRANSITION_STRATEGY_PACKAGE_STATUS"
ENABLE_SELECTION_CONFIRM = "ENABLE_STRATEGY_PACKAGE_SELECTION"
ENABLE_PAPER_CONFIRM = "ENABLE_STRATEGY_PACKAGE_PAPER"
RETIRE_STRATEGY_PACKAGE_CONFIRM = "RETIRE_STRATEGY_PACKAGE"
DELETE_STRATEGY_PACKAGE_CONFIRM = "DELETE_STRATEGY_PACKAGE"

SPECS = (
    ToolSpec("strategy_packages_list", "GET", "", query_defaults={"status": None, "limit": 20}, limit_caps={"limit": 100}),
    ToolSpec("strategy_packages_get", "GET", "/{package_id}", path_params=("package_id",)),
    ToolSpec("strategy_packages_list_components", "GET", "/{package_id}/components", path_params=("package_id",)),
    ToolSpec("strategy_packages_get_prediction_ref", "GET", "/{package_id}/prediction-ref", path_params=("package_id",)),
    ToolSpec("strategy_packages_list_qe_sources", "GET", "/qe-sources", query_defaults={"source_kind": "all", "limit": 50}, limit_caps={"limit": 200}),
    ToolSpec("strategy_packages_list_candidates", "GET", "/candidates", query_defaults={"status": "ACTIVE", "limit": 20}, limit_caps={"limit": 100}),
    ToolSpec("strategy_packages_get_candidate", "GET", "/candidates/{candidate_id}", path_params=("candidate_id",)),
    ToolSpec("strategy_packages_get_manifest_integrity", "GET", "/manifest-integrity", query_defaults={"limit": 50}, limit_caps={"limit": 500}),
    ToolSpec("strategy_packages_list_status_events", "GET", "/{package_id}/status-events", path_params=("package_id",), query_defaults={"limit": 50}, limit_caps={"limit": 200}),
    ToolSpec("strategy_packages_list_assets", "GET", "/{package_id}/assets", path_params=("package_id",), query_defaults={"protected_only": False}),
    ToolSpec("strategy_packages_get_metrics_summary", "GET", "/{package_id}/metrics-summary", path_params=("package_id",)),
    ToolSpec("strategy_packages_list_selection_artifacts", "GET", "/{package_id}/selection-artifacts", path_params=("package_id",), query_defaults={"limit": 20}, limit_caps={"limit": 100}),
    ToolSpec("strategy_packages_list_execution_policies", "GET", "/{package_id}/execution-policies", path_params=("package_id",)),
    ToolSpec("strategy_packages_get_model_state", "GET", "/{package_id}/model-state", path_params=("package_id",), query_defaults={"as_of_date": None}),
    ToolSpec("strategy_packages_preview_model_retrain", "POST", "/{package_id}/model-retrain/preview", path_params=("package_id",)),
    ToolSpec("strategy_packages_list_model_retrain_jobs", "GET", "/{package_id}/model-retrain/jobs", path_params=("package_id",), query_defaults={"limit": 20}, limit_caps={"limit": 100}),
    ToolSpec("strategy_packages_list_runtime_variants", "GET", "/{package_id}/runtime-variants", path_params=("package_id",), query_defaults={"include_retired": False, "limit": 20}, limit_caps={"limit": 100}),
    ToolSpec("strategy_packages_list_validation_runs", "GET", "/{package_id}/validation-runs", path_params=("package_id",), query_defaults={"validation_type": None, "runtime_variant_id": None, "limit": 20}, limit_caps={"limit": 100}),
    ToolSpec("strategy_packages_get_validation_run", "GET", "/{package_id}/validation-runs/{validation_run_id}", path_params=("package_id", "validation_run_id")),
    ToolSpec("strategy_packages_get_validation_stability", "GET", "/{package_id}/validation-stability", path_params=("package_id",), query_defaults={"metric_key": "annual_return", "limit": 100}, limit_caps={"limit": 500}),
    ToolSpec("strategy_packages_get_paper_admission", "GET", "/{package_id}/paper-simulation-admission", path_params=("package_id",), query_defaults={"metric_key": "annual_return", "governance_limit": 100}, limit_caps={"governance_limit": 500}),
    ToolSpec("strategy_packages_validate", "POST", "/{package_id}/validate", path_params=("package_id",)),
    ToolSpec("strategy_packages_get_delete_dependencies", "GET", "/{package_id}/delete-dependencies", path_params=("package_id",)),
    ToolSpec("strategy_packages_build_manifest_from_qe", "GET", "/from-qe-experiment/{experiment_id}/manifest", path_params=("experiment_id",)),
    ToolSpec("strategy_packages_validate_qe_paper_readiness", "GET", "/from-qe-experiment/{experiment_id}/paper-readiness", path_params=("experiment_id",)),
    ToolSpec("strategy_packages_create_from_qe_experiment_confirmed", "POST", "/from-qe-experiment", confirm_token=CREATE_STRATEGY_PACKAGE_CONFIRM),
    ToolSpec("strategy_packages_create_from_qe_loop_confirmed", "POST", "/from-qe-evolution-loop", confirm_token=CREATE_STRATEGY_PACKAGE_CONFIRM),
    ToolSpec("strategy_packages_create_from_candidate_confirmed", "POST", "/from-candidate/{candidate_id}", path_params=("candidate_id",), confirm_token=CREATE_STRATEGY_PACKAGE_CONFIRM),
    ToolSpec("strategy_packages_create_candidate_from_qe_experiment_confirmed", "POST", "/candidates/from-qe-experiment", confirm_token=CREATE_CANDIDATE_STRATEGY_PACKAGE_CONFIRM),
    ToolSpec("strategy_packages_create_candidate_from_qe_loop_confirmed", "POST", "/candidates/from-qe-loop", confirm_token=CREATE_CANDIDATE_STRATEGY_PACKAGE_CONFIRM),
    ToolSpec("strategy_packages_clone_candidate_confirmed", "POST", "/candidates/{candidate_id}/clone", path_params=("candidate_id",), confirm_token=CLONE_CANDIDATE_STRATEGY_PACKAGE_CONFIRM),
    ToolSpec("strategy_packages_refresh_candidate_snapshot_confirmed", "POST", "/candidates/{candidate_id}/refresh-snapshot", path_params=("candidate_id",), confirm_token=REFRESH_CANDIDATE_SNAPSHOT_CONFIRM),
    ToolSpec("strategy_packages_delete_candidate_confirmed", "DELETE", "/candidates/{candidate_id}", path_params=("candidate_id",), confirm_token=DELETE_CANDIDATE_STRATEGY_PACKAGE_CONFIRM),
    ToolSpec("strategy_packages_repair_manifest_hash_confirmed", "POST", "/{package_id}/repair-manifest-hash", path_params=("package_id",), confirm_token=REPAIR_MANIFEST_HASH_CONFIRM),
    ToolSpec("strategy_packages_record_asset_confirmed", "POST", "/{package_id}/assets", path_params=("package_id",), confirm_token=RECORD_STRATEGY_PACKAGE_ASSET_CONFIRM),
    ToolSpec("strategy_packages_generate_selection_artifacts_confirmed", "POST", "/{package_id}/selection-artifacts/generate", path_params=("package_id",), confirm_token=GENERATE_SELECTION_ARTIFACTS_CONFIRM),
    ToolSpec("strategy_packages_generate_diagnostic_backtest_artifacts_confirmed", "POST", "/{package_id}/selection-artifacts/generate-diagnostic-backtest", path_params=("package_id",), confirm_token=GENERATE_SELECTION_ARTIFACTS_CONFIRM),
    ToolSpec("strategy_packages_create_execution_policy_confirmed", "POST", "/{package_id}/execution-policies", path_params=("package_id",), confirm_token=CREATE_EXECUTION_POLICY_CONFIRM),
    ToolSpec("strategy_packages_enable_execution_policy_for_paper_confirmed", "POST", "/{package_id}/execution-policies/{policy_id}/enable-paper", path_params=("package_id", "policy_id"), confirm_token=TOGGLE_EXECUTION_POLICY_PAPER_CONFIRM),
    ToolSpec("strategy_packages_disable_execution_policy_for_paper_confirmed", "POST", "/{package_id}/execution-policies/{policy_id}/disable-paper", path_params=("package_id", "policy_id"), confirm_token=TOGGLE_EXECUTION_POLICY_PAPER_CONFIRM),
    ToolSpec("strategy_packages_start_model_retrain_confirmed", "POST", "/{package_id}/model-retrain/start", path_params=("package_id",), confirm_token=START_MODEL_RETRAIN_CONFIRM, body_updates={"confirm_retrain": True}),
    ToolSpec("strategy_packages_create_runtime_variant_confirmed", "POST", "/{package_id}/runtime-variants", path_params=("package_id",), confirm_token=CREATE_RUNTIME_VARIANT_CONFIRM),
    ToolSpec("strategy_packages_mark_runtime_variant_validation_confirmed", "POST", "/{package_id}/runtime-variants/{variant_id}/validation", path_params=("package_id", "variant_id"), confirm_token=MARK_RUNTIME_VARIANT_VALIDATION_CONFIRM),
    ToolSpec("strategy_packages_create_validation_run_confirmed", "POST", "/{package_id}/validation-runs", path_params=("package_id",), confirm_token=CREATE_VALIDATION_RUN_CONFIRM),
    ToolSpec("strategy_packages_transition_status_confirmed", "POST", "/{package_id}/transition-status", path_params=("package_id",), confirm_token=TRANSITION_STATUS_CONFIRM),
    ToolSpec("strategy_packages_enable_selection_confirmed", "POST", "/{package_id}/enable-selection", path_params=("package_id",), confirm_token=ENABLE_SELECTION_CONFIRM),
    ToolSpec("strategy_packages_enable_paper_confirmed", "POST", "/{package_id}/enable-paper", path_params=("package_id",), confirm_token=ENABLE_PAPER_CONFIRM),
    ToolSpec("strategy_packages_retire_confirmed", "POST", "/{package_id}/retire", path_params=("package_id",), confirm_token=RETIRE_STRATEGY_PACKAGE_CONFIRM),
    ToolSpec("strategy_packages_delete_confirmed", "DELETE", "/{package_id}", path_params=("package_id",), confirm_token=DELETE_STRATEGY_PACKAGE_CONFIRM, body_updates={"confirm_delete": True}),
)

TOOL_NAMES = tuple(spec.name for spec in SPECS)
TOOL_COUNT = len(TOOL_NAMES)


def register(registry) -> None:
    register_spec_tools(registry, module_name="strategy_packages", client_prefix="strategy-packages", specs=SPECS)
