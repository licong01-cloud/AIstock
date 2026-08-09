from __future__ import annotations

import json
from pathlib import Path

from scripts import ci_change_classifier as classifier


def _write_bug(
    path: Path,
    *,
    status: str = "fixed",
    module: str = "validation",
    allowed_write_scope: list[str] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "bug_id": "BUG-191",
                "status": status,
                "module": module,
                "allowed_write_scope": allowed_write_scope or [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_close_sync_bug_json_skips_backend_matrix(tmp_path: Path) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-example.json"
    _write_bug(bug, status="fixed")

    payload = classifier.classify_changed_files(
        ["tests/aistock_validation/bugs/20260601_BUG-191-example.json"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "close_sync_metadata_only"
    assert payload["close_sync_metadata_only"] is True
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is False
    assert payload["static_gate_required"] is True
    assert payload["pr_quality_required"] is True


def test_open_bug_registry_change_skips_unrelated_backend_matrix(tmp_path: Path) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-example.json"
    _write_bug(bug, status="open")

    payload = classifier.classify_changed_files(
        ["tests/aistock_validation/bugs/20260601_BUG-191-example.json"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "bug_registry_metadata_only"
    assert payload["backend_required"] is False
    assert payload["backend_sessions"] == []
    assert any("status=open" in reason for reason in payload["reasons"])


def test_workflow_change_with_bug_metadata_uses_workflow_lane(tmp_path: Path) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-example.json"
    _write_bug(bug, status="fixed")

    payload = classifier.classify_changed_files(
        [
            "tests/aistock_validation/bugs/20260601_BUG-191-example.json",
            "scripts/aistock_issue_workflow.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["non_bug_registry_files"] == ["scripts/aistock_issue_workflow.py"]
    assert payload["backend_sessions"] == []
    assert payload["workflow_validation_required"] is True


def test_standard_skill_workflow_and_runtime_catalog_stay_in_focused_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "docs/standards/aistock_development_standard_v1.5_20260523.md",
            "docs/standards/aistock_development_standard_v1.5_20260523.yaml",
            "docs/standards/aistock_runtime_targets_v1.yaml",
            ".codex/skills/fix-aistock-issue/SKILL.md",
            ".claude/commands/fix-aistock-issue.md",
            "scripts/aistock_issue_workflow.py",
            "scripts/aistock_guardrail_scan.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["workflow_validation_required"] is True
    assert payload["backend_required"] is False
    assert payload["frontend_required"] is False
    assert payload["workflow_test_targets"] == [
        "backend/tests/test_aistock_guardrail_scan.py",
        "backend/tests/scripts/test_aistock_issue_workflow.py",
        "backend/tests/scripts/test_issue_flow.py",
    ]


def test_backend_change_selects_relevant_backend_matrix_slice(tmp_path: Path) -> None:
    advisory_payload = classifier.classify_changed_files(
        [
            "backend/services/advisory_dev_input_onboarding/historical_onboarding.py",
            "backend/services/advisory_phase0a/historical_research_postgres.py",
            "backend/tests/advisory_dev_input_onboarding/test_o3_historical_onboarding.py",
            "scripts/advisory_real_dev_onboarding.py",
        ],
        repo_root=tmp_path,
    )

    assert advisory_payload["classification"] == "targeted_ci_required"
    assert advisory_payload["backend_sessions"] == ["advisory_dev_input_onboarding_backend"]
    assert advisory_payload["unmapped_code_files"] == []

    payload = classifier.classify_changed_files(
        ["backend/services/paper_trading_v2/runtime.py"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["backend_required"] is True
    assert payload["backend_sessions"] == ["paper_v2_backend"]

    miniqmt_payload = classifier.classify_changed_files(
        ["backend/services/miniqmt_execution_runtime/runtime.py"],
        repo_root=tmp_path,
    )

    assert miniqmt_payload["backend_sessions"] == ["miniqmt_execution_runtime_l2"]

    miniqmt_test_payload = classifier.classify_changed_files(
        ["backend/tests/miniqmt_execution_runtime/test_runtime.py"],
        repo_root=tmp_path,
    )

    assert miniqmt_test_payload["backend_sessions"] == ["miniqmt_execution_runtime_l2"]

    miniqmt_quote_ingress_payload = classifier.classify_changed_files(
        [
            "backend/infra/realtime_quote_subscriber.py",
            "backend/tests/infra/test_realtime_quote_subscriber_leases.py",
        ],
        repo_root=tmp_path,
    )

    assert miniqmt_quote_ingress_payload["backend_sessions"] == ["miniqmt_execution_runtime_l2"]
    assert miniqmt_quote_ingress_payload["unmapped_code_files"] == []

    qe_payload = classifier.classify_changed_files(
        ["backend/services/quantevolver/qe_evolution_service.py"],
        repo_root=tmp_path,
    )

    assert qe_payload["backend_sessions"] == ["qe_read_backend"]

    qe_mcp_payload = classifier.classify_changed_files(
        [
            "backend/mcp/modules/qe_experiment.py",
            "backend/tests/mcp/test_domain_modules.py",
        ],
        repo_root=tmp_path,
    )

    assert qe_mcp_payload["backend_sessions"] == ["qe_data_contract_backend"]
    assert qe_mcp_payload["unmapped_code_files"] == []

    qe_multi_alpha_p0_2_payload = classifier.classify_changed_files(
        [
            "backend/mcp/modules/qe_archive.py",
            "backend/mcp/tool_manifest.py",
            "backend/migrations/multi_alpha_p0_2_control_recovery_20260721.sql",
            "backend/migrations/qe_archive_multi_alpha_p0_2_recovery_20260721.sql",
            "backend/tests/mcp/test_qe_archive_module.py",
        ],
        repo_root=tmp_path,
    )

    assert qe_multi_alpha_p0_2_payload["classification"] == "targeted_ci_required"
    assert qe_multi_alpha_p0_2_payload["backend_sessions"] == [
        "qe_data_contract_backend",
        "qe_read_backend",
    ]
    assert qe_multi_alpha_p0_2_payload["unmapped_code_files"] == []

    qe_multi_alpha_frontend_payload = classifier.classify_changed_files(
        [
            "frontend/src/app/quantevolver/evolution/components/MultiAlphaChildGrid.tsx",
            "frontend/tests/quantevolver/multi-alpha-child-grid.spec.ts",
            "frontend/tests/quantevolver/evolution-visual-parity.spec.ts-snapshots/multi-alpha-shared-workspace-chromium-win32.png",
            "frontend/tests/multi-alpha-combine-backtest.spec.ts",
        ],
        repo_root=tmp_path,
    )

    assert qe_multi_alpha_frontend_payload["classification"] == "targeted_ci_required"
    assert qe_multi_alpha_frontend_payload["frontend_required"] is True
    assert qe_multi_alpha_frontend_payload["unmapped_code_files"] == []

    hmm_payload = classifier.classify_changed_files(
        ["backend/services/hmm_data_source/cache_manager.py"],
        repo_root=tmp_path,
    )

    assert hmm_payload["backend_sessions"] == ["hmm_data_source_backend"]

    hmm_evolution_payload = classifier.classify_changed_files(
        [
            "backend/services/hmm_evolution/repository.py",
            "backend/db/init_hmm_evolution_schema.py",
            "backend/tests/hmm_evolution/test_service.py",
        ],
        repo_root=tmp_path,
    )

    assert hmm_evolution_payload["backend_sessions"] == ["hmm_evolution_backend"]
    assert hmm_evolution_payload["unmapped_code_files"] == []

    simulation_payload = classifier.classify_changed_files(
        ["backend/services/simulation_runtime/ops.py"],
        repo_root=tmp_path,
    )

    assert simulation_payload["backend_sessions"] == ["simulation_core_l2"]
    assert simulation_payload["unmapped_code_files"] == []


def test_sector_data_materialization_files_select_only_local_data_plan(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "backend/services/sector_data_builder.py",
            "scripts/create_sw_sector_tables.py",
            "backend/db/migrations/sector_data_pit_identity_v1.sql",
            "backend/tests/services/test_sector_data_builder.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_required"] is True
    assert payload["backend_plan_keys"] == ["l0", "data_sync_autonomy_backend"]
    assert payload["backend_sessions"] == ["data_sync_autonomy_backend"]
    assert payload["catalog_impacted_modules"] == ["local_data"]
    assert payload["unmapped_code_files"] == []


def test_advisory_snapshot_blob_ref_migrations_select_historical_range_plan(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "backend/db/migrations/fix_advisory_dataset_snapshot_blob_ref_unique_scope_20260727.sql",
            "backend/db/migrations/fix_advisory_dataset_snapshot_blob_ref_unique_scope_20260727.rollback.sql",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_plan_keys"] == ["l0", "advisory_historical_range_backend"]
    assert payload["backend_sessions"] == ["advisory_historical_range_backend"]
    assert payload["catalog_impacted_modules"] == ["advisory.historical_range", "tests.backend"]
    assert payload["unmapped_code_files"] == []


def test_advisory_r4_shared_phase1_contracts_select_historical_range_plan(
    tmp_path: Path,
) -> None:
    payload = classifier.classify_changed_files(
        [
            "backend/db/migrations/add_advisory_historical_range_r4_outcome_bridge_20260723.sql",
            "backend/services/advisory_phase1/capture_foundation.py",
            "backend/services/advisory_phase1/dataset_build.py",
            "backend/services/advisory_phase1/label_builder_postgres.py",
            "backend/services/advisory_phase1/observation_capture_postgres.py",
            "backend/services/advisory_phase1/outcome_engine.py",
            "backend/services/advisory_phase1/release_schema_contract.py",
            "backend/services/advisory_phase1/retrospective_selector.py",
            "backend/services/advisory_phase1/snapshot_writer.py",
            "backend/tests/advisory_phase1/test_phase1c3_batch_d_writer.py",
            "backend/tests/advisory_phase1/test_r4_dataset_build_postgres.py",
            "backend/tests/advisory_phase1/test_release_schema.py",
            "backend/tests/advisory_phase1/test_retrospective_selector.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_plan_keys"] == ["l0", "advisory_historical_range_backend"]
    assert payload["backend_sessions"] == ["advisory_historical_range_backend"]
    assert payload["catalog_impacted_modules"] == [
        "advisory.historical_range",
        "tests.backend",
    ]
    assert payload["unmapped_code_files"] == []


def test_minute_execution_changes_select_focused_paper_v2_session(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "backend/execution_algos/twap_algo.py",
            "backend/services/trading_core/execution_algo_adapter.py",
            "backend/services/trading_core/minute_execution.py",
            "backend/tests/trading_core/test_minute_execution.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["backend_required"] is True
    assert payload["backend_sessions"] == ["paper_v2_backend"]
    assert payload["unmapped_code_files"] == []


def test_qmt_strategy_ledger_and_vnpy_asset_changes_select_existing_execution_sessions(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "backend/tests/trading_core/test_vnpy_style_execution_assets.py",
            "backend/services/qmt_strategy_ledger/order_service.py",
            "backend/services/qmt_strategy_ledger/repository.py",
            "backend/tests/qmt_strategy_ledger/test_order_service_preflight.py",
            "backend/tests/qmt_strategy_ledger/test_repository.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["backend_required"] is True
    assert payload["backend_sessions"] == ["paper_v2_backend", "miniqmt_execution_runtime_l2"]
    assert payload["unmapped_code_files"] == []


def test_qmt_client_selects_its_direct_contract_instead_of_unrelated_matrix(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["backend/infra/qmt_client.py"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_required"] is True
    assert payload["backend_sessions"] == ["qmt_client_contract"]
    assert payload["unmapped_code_files"] == []


def test_frontend_change_selects_owning_module_tests(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["frontend/src/app/hmm-evolution/page.tsx"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["frontend_required"] is True
    assert payload["frontend_test_targets"] == ["tests/hmm-evolution"]
    assert payload["backend_sessions"] == ["hmm_evolution_backend"]
    assert payload["catalog_impacted_modules"][0] == "hmm.evolution"
    assert payload["unmapped_code_files"] == []


def test_unmapped_frontend_code_blocks_instead_of_receiving_type_lint_only(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["frontend/src/app/unowned-feature/page.tsx"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "unmapped_code_blocked"
    assert payload["workflow_gate"] == "blocked"
    assert payload["frontend_required"] is True
    assert payload["frontend_test_targets"] == []
    assert payload["unmapped_code_files"] == ["frontend/src/app/unowned-feature/page.tsx"]


def test_deferred_catalog_plan_maps_data_quality_without_unrelated_pr_matrix(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["backend/tests/data_quality/test_cross_table_consistency.py"],
        repo_root=tmp_path,
    )

    assert payload["unmapped_code_files"] == []
    assert payload["backend_required"] is False
    assert payload["backend_sessions"] == []
    assert "data_quality_deep" in payload["backend_plan_keys"]


def test_feature_workflow_files_use_focused_workflow_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "scripts/aistock_feature_workflow.py",
            "backend/tests/scripts/test_aistock_feature_workflow.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["workflow_validation_required"] is True
    assert payload["backend_required"] is False
    assert payload["unmapped_code_files"] == []


def test_validation_ui_target_contract_uses_catalog_gate_only(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["backend/tests/test_validation_ui_target_catalog.py"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "catalog_validation_only"
    assert payload["catalog_validation_required"] is True
    assert payload["backend_required"] is False
    assert payload["backend_sessions"] == []
    assert payload["backend_plan_keys"] == []
    assert payload["unmapped_code_files"] == []


def test_pg_pool_source_and_regression_select_shared_platform_backend_session(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["backend/db/pg_pool.py", "backend/tests/test_pg_pool_audit.py"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_required"] is True
    assert payload["backend_sessions"] == ["platform_api_backend"]
    assert payload["unmapped_code_files"] == []


def test_backend_sessions_come_from_validation_catalog_not_classifier_rules(tmp_path: Path) -> None:
    source = Path("scripts/ci_change_classifier.py").read_text(encoding="utf-8")
    payload = classifier.classify_changed_files(
        ["backend/services/simulation_runtime/ops.py"],
        repo_root=tmp_path,
    )

    assert "BACKEND_SESSION_RULES" not in source
    assert payload["catalog_impacted_modules"][0] == "simulation_runtime"
    assert payload["backend_plan_keys"] == ["l0", "simulation_core_l2"]
    assert payload["backend_sessions"] == ["simulation_core_l2"]


def test_ci_changed_file_resolver_uses_its_direct_workflow_target(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["scripts/ci_changed_files.py"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["backend_plan_keys"] == []
    assert payload["workflow_validation_required"] is True
    assert payload["workflow_test_targets"] == ["backend/tests/scripts/test_ci_changed_files.py"]
    assert payload["unmapped_code_files"] == []


def test_frontend_uses_module_tests_while_go_uses_its_language_gate(tmp_path: Path) -> None:
    frontend = classifier.classify_changed_files(
        ["frontend/src/app/watchlist/page.tsx"],
        repo_root=tmp_path,
    )
    assert frontend["classification"] == "targeted_ci_required"
    assert frontend["frontend_required"] is True
    assert frontend["frontend_test_targets"] == ["tests/watchlist"]
    assert frontend["backend_required"] is True
    assert frontend["backend_sessions"] == ["watchlist_backend"]
    assert frontend["obsolete_surface_removal"] is False

    go = classifier.classify_changed_files(
        ["tdx-api-main/web/server.go"],
        repo_root=tmp_path,
    )
    assert go["classification"] == "go_ci_required"
    assert go["go_required"] is True
    assert go["backend_required"] is False
    assert go["backend_sessions"] == []

    go_docs = classifier.classify_changed_files(
        ["tdx-api-main/web/USAGE.md"],
        repo_root=tmp_path,
    )
    assert go_docs["go_required"] is False
    assert go_docs["backend_sessions"] == []


def test_hmm_tests_select_dedicated_backend_session(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["backend/tests/hmm_data_source/test_integration.py"],
        repo_root=tmp_path,
    )

    assert payload["backend_sessions"] == ["hmm_data_source_backend"]


def test_workflow_validation_only_uses_focused_fast_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            ".github/workflows/dependency-update-validate.yml",
            ".github/workflows/test.yml",
            ".github/requirements/pr-quality.txt",
            ".github/requirements/semgrep.txt",
            "scripts/ci_change_classifier.py",
            "scripts/validate_changed_requirements.py",
            "scripts/aistock_validation_catalog_integrity.py",
            "backend/tests/scripts/test_ci_change_classifier.py",
            "backend/tests/scripts/test_validate_changed_requirements.py",
            "backend/tests/test_validation_catalog_integrity.py",
            "docs/architecture/aistock_pr_quality_p0p1_evidence_gate_design_20260602.md",
            "docs/codex_project_memory.md",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True
    assert payload["prompt_evaluation_required"] is False
    assert payload["unmapped_code_files"] == []
    assert "backend/tests/scripts/test_validate_changed_requirements.py" in payload["workflow_test_targets"]


def test_docs_fast_update_skips_code_validation(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["docs/analysis/example.md", "docs/design/example.md"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "docs_fast_update"
    assert payload["docs_fast_tier"] == "docs_fast_update"
    assert payload["docs_fast_required"] is True
    assert payload["docs_controlled_required"] is False
    assert payload["backend_required"] is False
    assert payload["static_gate_required"] is False


def test_docs_fast_new_records_new_doc_tier(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["docs/handoff/new-handoff.md"],
        repo_root=tmp_path,
        added_files=["docs/handoff/new-handoff.md"],
    )

    assert payload["classification"] == "docs_fast_new"
    assert payload["docs_fast_tier"] == "docs_fast_new"
    assert payload["backend_required"] is False


def test_docs_controlled_keeps_normal_guardrails(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["docs/standards/aistock_issue_workflow_quickstart.md", "AGENTS.md"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "docs_controlled"
    assert payload["docs_fast_required"] is False
    assert payload["docs_controlled_required"] is True
    assert payload["backend_required"] is False
    assert payload["backend_sessions"] == []
    assert payload["static_gate_required"] is True


def test_unrelated_workflow_validation_change_does_not_run_prompt_evaluation(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            ".github/workflows/pr-quality.yml",
            "scripts/issue_flow.py",
            "backend/tests/scripts/test_issue_flow.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True
    assert payload["prompt_evaluation_required"] is False
    assert payload["close_sync_metadata_only"] is False


def test_code_intelligence_nightly_workflow_change_uses_focused_fast_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            ".github/workflows/code-intelligence-refresh.yml",
            ".github/workflows/nightly.yml",
            "scripts/code_intelligence_adapter.py",
            "backend/tests/scripts/test_code_intelligence_adapter.py",
            "docs/standards/aistock_issue_workflow_quickstart.md",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True
    assert payload["unmapped_code_files"] == []


def test_nightly_session_runner_uses_its_direct_workflow_target(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            ".github/workflows/nightly.yml",
            "scripts/nightly_session_runner.py",
            "backend/tests/scripts/test_nightly_session_runner.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True
    assert payload["workflow_test_targets"] == [
        "backend/tests/scripts/test_nightly_session_runner.py",
    ]
    assert payload["unmapped_code_files"] == []


def test_validation_llm_prompt_pack_change_uses_focused_fast_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "prompt_packs/validation_llm/triage_failure.prompt.yml",
            "prompt_packs/validation_llm/design_drift_audit.prompt.yml",
            "prompt_packs/validation_llm/silent_degradation_audit.prompt.yml",
            "prompt_packs/validation_llm/evaluation_cases/historical_failure_fixtures.json",
            "configs/validation/llm_triage.yaml",
            "configs/validation/design_drift_audit.yaml",
            "configs/validation/silent_degradation_audit.yaml",
            "docs/operations/validation_llm_guarded_rollout_runbook_20260609.md",
            "scripts/llm_provider_adapter.py",
            "scripts/nightly_adaptive_scheduler.py",
            "scripts/nightly_design_drift_audit.py",
            "scripts/nightly_silent_degradation_audit.py",
            "backend/tests/scripts/test_llm_provider_adapter.py",
            "backend/tests/scripts/test_nightly_adaptive_scheduler.py",
            "backend/tests/scripts/test_nightly_design_drift_audit.py",
            "backend/tests/scripts/test_nightly_silent_degradation_audit.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True


def test_issue_on_test_fail_workflow_change_uses_focused_fast_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            ".github/workflows/issue-on-test-fail.yml",
            "scripts/ci_failure_issue_summary.py",
            "backend/tests/scripts/test_ci_failure_issue_summary.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True


def test_workflow_validation_only_allows_same_task_bug_metadata(tmp_path: Path) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260604_BUG-257-workflow-fast-lane.json"
    allocator_rel = "tests/aistock_validation/bugs/.bug_id_allocator.json"
    bug = tmp_path / bug_rel
    allocator = tmp_path / allocator_rel
    _write_bug(
        bug,
        status="in_progress",
        module="validation",
        allowed_write_scope=[
            "scripts/ci_change_classifier.py",
            "backend/tests/scripts/test_ci_change_classifier.py",
            bug_rel,
            allocator_rel,
        ],
    )
    allocator.write_text(json.dumps({"last_allocated": 257}), encoding="utf-8")

    payload = classifier.classify_changed_files(
        [
            "scripts/ci_change_classifier.py",
            "backend/tests/scripts/test_ci_change_classifier.py",
            bug_rel,
            allocator_rel,
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True
    assert payload["workflow_bug_metadata_files"] == [bug_rel]


def test_workflow_validation_only_allows_fixed_same_task_bug_metadata_and_client_wrappers(tmp_path: Path) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260605_BUG-266-workflow-fast-lane.json"
    allocator_rel = "tests/aistock_validation/bugs/.bug_id_allocator.json"
    bug = tmp_path / bug_rel
    allocator = tmp_path / allocator_rel
    _write_bug(
        bug,
        status="fixed",
        module="validation",
        allowed_write_scope=[
            "scripts/aistock_issue_workflow.py",
            "backend/tests/scripts/test_aistock_issue_workflow.py",
            ".codex/skills/fix-aistock-issue/SKILL.md",
            ".claude/commands/fix-aistock-issue.md",
            "docs/codex_project_memory.md",
            bug_rel,
            allocator_rel,
        ],
    )
    allocator.write_text(json.dumps({"last_allocated": 266}), encoding="utf-8")

    payload = classifier.classify_changed_files(
        [
            "scripts/aistock_issue_workflow.py",
            "backend/tests/scripts/test_aistock_issue_workflow.py",
            ".codex/skills/fix-aistock-issue/SKILL.md",
            ".claude/commands/fix-aistock-issue.md",
            bug_rel,
            allocator_rel,
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True
    assert payload["workflow_bug_metadata_files"] == [bug_rel]


def test_workflow_client_instruction_cleanup_change_skips_backend_matrix(tmp_path: Path) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260702_BUG-579-cleanup-fast.json"
    allocator_rel = "tests/aistock_validation/bugs/.bug_id_allocator.json"
    bug = tmp_path / bug_rel
    allocator = tmp_path / allocator_rel
    workflow_files = [
        "scripts/aistock_issue_workflow.py",
        "scripts/ci_change_classifier.py",
        "backend/tests/scripts/test_aistock_issue_workflow.py",
        "backend/tests/scripts/test_ci_change_classifier.py",
        ".codex/skills/aistock-docs-handoff/SKILL.md",
        ".codex/skills/aistock-task-router/SKILL.md",
        ".codex/skills/fix-aistock-issue/SKILL.md",
        ".claude/commands/aistock-docs-handoff.md",
        ".claude/commands/aistock-task-router.md",
        ".claude/commands/fix-aistock-issue.md",
        "docs/codex_project_memory.md",
    ]
    _write_bug(
        bug,
        status="open",
        module="validation_llm_pipeline",
        allowed_write_scope=[*workflow_files, bug_rel, allocator_rel],
    )
    allocator.write_text(json.dumps({"last_allocated": 579}), encoding="utf-8")

    payload = classifier.classify_changed_files(
        [*workflow_files, bug_rel, allocator_rel],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["backend_sessions"] == []
    assert payload["workflow_validation_required"] is True
    assert payload["workflow_bug_metadata_files"] == [bug_rel]


def test_workflow_bug_metadata_selects_from_changed_files_not_future_scope(tmp_path: Path) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260604_BUG-258-business-scope.json"
    bug = tmp_path / bug_rel
    _write_bug(
        bug,
        status="in_progress",
        module="validation",
        allowed_write_scope=[
            "scripts/ci_change_classifier.py",
            "backend/routers/validation.py",
            bug_rel,
        ],
    )

    payload = classifier.classify_changed_files(
        [
            "scripts/ci_change_classifier.py",
            bug_rel,
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True


def test_workflow_validation_fast_lane_rejects_business_files(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "scripts/aistock_issue_workflow.py",
            "backend/routers/validation.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["backend_required"] is True
    assert payload["backend_sessions"] == ["validation_center_backend"]
    assert payload["workflow_validation_required"] is True


def test_frontend_removal_uses_relevant_language_and_backend_gates(tmp_path: Path) -> None:
    bug_rel = "tests/aistock_validation/bugs/20260703_BUG-580-remove-obsolete-surface.json"
    _write_bug(tmp_path / bug_rel, status="open", module="strategy_package")

    payload = classifier.classify_changed_files(
        [
            "frontend/src/app/strategy-package-governance/page.tsx",
            "frontend/src/lib/navigation/nav-groups.ts",
            "frontend/tests/strategy-package-governance/governance.spec.ts",
            "backend/routers/strategy_packages.py",
            "backend/tests/strategy_package/test_governance_eligibility.py",
            "tests/aistock_validation/catalog/ui_targets.yaml",
            "tests/aistock_validation/catalog/test_plans.yaml",
            "noxfile.py",
            bug_rel,
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["backend_required"] is True
    assert payload["backend_sessions"] == ["paper_v2_backend"]
    assert payload["frontend_required"] is True
    assert payload["obsolete_surface_removal"] is False
    assert payload["nightly_deferred_verification"]["required"] is False


def test_github_workflow_wires_workflow_validation_fast_lane() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    jobs = workflow["jobs"]

    assert jobs["classify-changes"]["outputs"]["workflow_validation_required"].endswith(
        "steps.classify.outputs.workflow_validation_required }}"
    )
    assert jobs["classify-changes"]["outputs"]["workflow_test_targets"].endswith(
        "steps.classify.outputs.workflow_test_targets }}"
    )
    assert jobs["classify-changes"]["outputs"]["prompt_evaluation_required"].endswith(
        "steps.classify.outputs.prompt_evaluation_required }}"
    )
    assert jobs["classify-changes"]["outputs"]["backend_sessions"].endswith(
        "steps.classify.outputs.backend_sessions }}"
    )
    assert jobs["classify-changes"]["outputs"]["frontend_required"].endswith(
        "steps.classify.outputs.frontend_required }}"
    )
    assert jobs["classify-changes"]["outputs"]["frontend_test_targets"].endswith(
        "steps.classify.outputs.frontend_test_targets }}"
    )
    assert jobs["classify-changes"]["outputs"]["catalog_validation_required"].endswith(
        "steps.classify.outputs.catalog_validation_required }}"
    )
    assert jobs["classify-changes"]["outputs"]["go_required"].endswith("steps.classify.outputs.go_required }}")
    assert jobs["backend-tests"]["if"] == "needs.classify-changes.outputs.backend_required != 'false'"
    assert (
        jobs["backend-tests"]["strategy"]["matrix"]["session"]
        == "${{ fromJson(needs.classify-changes.outputs.backend_sessions) }}"
    )
    assert jobs["workflow-validation-tests"]["if"] == (
        "needs.classify-changes.outputs.workflow_validation_required == 'true' && "
        "needs.classify-changes.outputs.workflow_test_targets != '[]'"
    )
    workflow_runs = "\n".join(str(step.get("run", "")) for step in jobs["workflow-validation-tests"]["steps"])
    assert "WORKFLOW_TEST_TARGETS" in workflow_runs
    assert 'python -m pytest "${workflow_test_targets[@]}"' in workflow_runs
    assert "backend/tests/scripts/test_llm_provider_adapter.py \\" not in workflow_runs
    assert jobs["frontend-quality"]["if"] == "needs.classify-changes.outputs.frontend_required == 'true'"
    frontend_runs = "\n".join(str(step.get("run", "")) for step in jobs["frontend-quality"]["steps"])
    assert "npm exec tsc" in frontend_runs
    assert "npm run lint" in frontend_runs
    assert "npx playwright install --with-deps chromium" in frontend_runs
    assert "FRONTEND_TEST_TARGETS" in frontend_runs
    assert 'npm run test:e2e -- "${module_test_targets[@]}"' in frontend_runs
    assert jobs["tdx-go-tests"]["if"] == "needs.classify-changes.outputs.go_required == 'true'"
    go_runs = "\n".join(str(step.get("run", "")) for step in jobs["tdx-go-tests"]["steps"])
    assert "go test ./..." in go_runs
    prompt_eval = jobs["prompt-evaluation"]
    assert prompt_eval["if"] == "needs.classify-changes.outputs.prompt_evaluation_required == 'true'"
    prompt_eval_run_steps = "\n".join(str(step.get("run", "")) for step in prompt_eval["steps"])
    assert "scripts/llm_provider_adapter.py --json prompt-evaluation" in prompt_eval_run_steps
    assert "prompt-evaluation" in jobs["failure-bug-register"]["needs"]
    assert "workflow-validation-tests" in jobs["failure-bug-register"]["needs"]


def test_github_backend_dependency_surface_installs_pinned_runtime_dependencies() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    steps = workflow["jobs"]["backend-tests"]["steps"]
    install = next(
        step for step in steps if step.get("name") == "Install backend deps via venv (no conda on hosted runners)"
    )

    assert "hmmlearn==0.3.3" in str(install["run"])
    assert "mcp[cli]==1.25.0" in str(install["run"])


def test_github_workflow_has_single_fail_closed_ci_verdict() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    jobs = workflow["jobs"]
    verdict = jobs["ci-verdict"]
    expected_needs = {
        "classify-changes",
        "static-gate",
        "docs-lite",
        "backend-tests",
        "frontend-quality",
        "tdx-go-tests",
        "workflow-validation-tests",
        "prompt-evaluation",
        "failure-bug-register",
    }

    assert verdict["name"] == "CI verdict"
    assert sum(job.get("name") == "CI verdict" for job in jobs.values()) == 1
    assert verdict["if"] == "always()"
    assert set(verdict["needs"]) == expected_needs
    run = str(verdict["steps"][0]["run"])
    assert '"classify:${CLASSIFY_RESULT}" "static:${STATIC_RESULT}"' in run
    for lane in ("docs", "backend", "frontend", "go", "workflow", "prompt", "registrar"):
        assert f'"{lane}:${{{lane.upper() if lane != "go" else "GO"}_RESULT}}"' in run
    assert 'result" != "success"' in run
    assert 'result" != "skipped"' in run
    assert "CI verdict failed" in run

    classify_steps = jobs["classify-changes"]["steps"]
    install = next(step for step in classify_steps if step.get("name") == "Install change-classifier dependency")
    assert "pyyaml" in install["run"]


def test_static_gate_uses_registry_metadata_fast_lane() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    static_gate_steps = workflow["jobs"]["static-gate"]["steps"]
    registry_steps = [
        step
        for step in static_gate_steps
        if isinstance(step, dict) and str(step.get("name") or "") == "BUG registry metadata check"
    ]

    assert len(registry_steps) == 1
    assert registry_steps[0]["if"] == "needs.classify-changes.outputs.close_sync_metadata_only == 'true'"
    assert "scripts/bug_registry_metadata_check.py" in registry_steps[0]["run"]
    assert "--close-sync-only" in registry_steps[0]["run"]

    nox_steps = [
        step
        for step in static_gate_steps
        if isinstance(step, dict) and str(step.get("name") or "").startswith("nox -s ")
    ]
    assert nox_steps
    assert all("close_sync_metadata_only != 'true'" in str(step.get("if") or "") for step in nox_steps)
    l0_step = next(step for step in nox_steps if step.get("name") == "nox -s l0 -- changed files")
    assert "l0_changed_files.txt" in l0_step["run"]
    assert 'python -m nox -s l0 -- "${changed_files[@]}"' in l0_step["run"]

    changed_files_step = next(
        step for step in static_gate_steps if step.get("name") == "Build static-gate changed-file list"
    )
    assert "scripts/ci_changed_files.py" in changed_files_step["run"]
    assert "--diff-filter ACMRT" in changed_files_step["run"]

    catalog_steps = [
        step
        for step in nox_steps
        if step.get("name") in {"nox -s validation_module_registry_l0", "nox -s validation_catalog_integrity"}
    ]
    assert len(catalog_steps) == 2
    assert all("catalog_validation_required == 'true'" in str(step.get("if") or "") for step in catalog_steps)


def test_catalog_change_uses_catalog_gate_without_workflow_suite(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["tests/aistock_validation/catalog/module_registry.yaml"],
        repo_root=tmp_path,
    )

    assert payload["catalog_validation_required"] is True
    assert payload["workflow_validation_required"] is False
    assert payload["backend_required"] is False
    assert payload["unmapped_code_files"] == []


def test_workflow_and_nox_validation_tests_route_without_product_modules(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "backend/tests/scripts/test_issue_flow_pr_quality.py",
            "backend/tests/test_noxfile_validation_env.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["workflow_validation_required"] is True
    assert payload["catalog_validation_required"] is True
    assert payload["backend_required"] is False
    assert payload["unmapped_code_files"] == []


def test_workflow_sources_select_only_their_direct_test_targets(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "scripts/aistock_issue_workflow.py",
            "scripts/issue_flow.py",
            "scripts/ci_change_classifier.py",
            "noxfile.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["workflow_test_targets"] == [
        "backend/tests/scripts/test_aistock_issue_workflow.py",
        "backend/tests/scripts/test_issue_flow.py",
        "backend/tests/scripts/test_issue_flow_pr_quality.py",
        "backend/tests/scripts/test_ci_change_classifier.py",
        "backend/tests/test_noxfile_validation_env.py",
    ]
    assert "backend/tests/scripts/test_llm_provider_adapter.py" not in payload["workflow_test_targets"]
    assert "backend/tests/scripts/test_nightly_adaptive_scheduler.py" not in payload["workflow_test_targets"]


def test_pr_quality_has_single_lane_and_registry_sync_record() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/pr-quality.yml").read_text(encoding="utf-8"))
    steps = workflow["jobs"]["pr-quality"]["steps"]
    names = [str(step.get("name") or "") for step in steps if isinstance(step, dict)]

    assert names.count("Detect PR quality lane") == 1
    assert names.count("Build registry-sync quality record") == 1
    assert names.count("Comment PR summary") == 1
    assert names.count("Upload PR quality artifacts") == 1
    assert "Semgrep AIstock guardrails (report-only phase)" not in names
    assert not any("Legacy" in name for name in names)

    registry_step = next(
        step for step in steps if isinstance(step, dict) and step.get("name") == "Build registry-sync quality record"
    )
    assert registry_step["if"] == "steps.quality_lane.outputs.registry_sync == '1'"
    assert "scripts/bug_registry_metadata_check.py" in registry_step["run"]
    assert "--close-sync-only" in registry_step["run"]

    normal_lane_steps = [
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("name")
        in {
            "Set up Python 3.12",
            "Install quality tooling",
            "Build AIstock PR quality summary",
            "Build code intelligence PR artifact",
            "Ruff changed Python files",
        }
    ]
    assert normal_lane_steps
    assert all("registry_sync != '1'" in str(step.get("if") or "") for step in normal_lane_steps)


def test_codeql_selects_only_changed_languages() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/codeql.yml").read_text(encoding="utf-8"))
    jobs = workflow["jobs"]
    fast_lane = jobs["docs-lite"]
    analyze = jobs["analyze"]
    analyze_steps = analyze["steps"]

    assert fast_lane["outputs"]["registry_sync"].endswith("steps.fast_lane.outputs.registry_sync }}")
    assert fast_lane["outputs"]["languages"].endswith("steps.fast_lane.outputs.languages }}")
    assert fast_lane["outputs"]["has_languages"].endswith("steps.fast_lane.outputs.has_languages }}")
    detect_step = next(step for step in fast_lane["steps"] if step.get("name") == "Detect CodeQL fast lane")
    assert "scripts/ci_change_classifier.py" in detect_step["run"]
    assert "close_sync_metadata_only" in detect_step["run"]
    assert "*.py) PYTHON_CHANGED=1" in detect_step["run"]
    assert "*.js|*.jsx|*.ts|*.tsx) JAVASCRIPT_CHANGED=1" in detect_step["run"]
    assert "LANGUAGES='[]'" in detect_step["run"]

    assert analyze["if"] == "needs.docs-lite.outputs.has_languages == '1'"
    assert analyze["strategy"]["matrix"]["language"] == "${{ fromJson(needs.docs-lite.outputs.languages) }}"
    assert not any(step.get("name") == "Fast-lane CodeQL no-op" for step in analyze_steps)
    gated_steps = [
        step for step in analyze_steps if step.get("name") in {"Initialize CodeQL", "Perform CodeQL Analysis"}
    ]
    assert gated_steps
    assert all("if" not in step for step in gated_steps)


def test_semgrep_uses_registry_sync_fast_lane() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/semgrep.yml").read_text(encoding="utf-8"))
    steps = workflow["jobs"]["semgrep"]["steps"]

    detect_step = next(step for step in steps if step.get("name") == "Detect Semgrep fast lane")
    assert detect_step["id"] == "fast_lane"
    assert "scripts/ci_change_classifier.py" in detect_step["run"]
    assert "close_sync_metadata_only" in detect_step["run"]

    semgrep_steps = [
        step for step in steps if step.get("name") in {"Set up Python 3.12", "Install Semgrep", "Run Semgrep"}
    ]
    assert semgrep_steps
    assert all("registry_sync != '1'" in str(step.get("if") or "") for step in semgrep_steps)
    no_op = next(step for step in steps if step.get("name") == "Emit fast-lane semgrep no-op record")
    assert "registry_sync == '1'" in str(no_op["if"])


def test_classifier_dependency_is_installed_before_detection() -> None:
    import yaml

    workflows = {
        ".github/workflows/test.yml": ("classify-changes", "Classify CI lane"),
        ".github/workflows/pr-quality.yml": ("pr-quality", "Detect PR quality lane"),
        ".github/workflows/codeql.yml": ("docs-lite", "Detect CodeQL fast lane"),
        ".github/workflows/semgrep.yml": ("semgrep", "Detect Semgrep fast lane"),
    }
    for path, (job_name, detect_name) in workflows.items():
        workflow = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        steps = workflow["jobs"][job_name]["steps"]
        install_index = next(
            index for index, step in enumerate(steps) if step.get("name") == "Install change-classifier dependency"
        )
        detect_index = next(index for index, step in enumerate(steps) if step.get("name") == detect_name)
        assert install_index < detect_index
        assert "pyyaml" in steps[install_index]["run"].lower()


def test_issue_on_test_fail_is_the_only_failure_issue_writer() -> None:
    import yaml

    ci = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    registrar = ci["jobs"]["failure-bug-register"]
    registrar_text = Path(".github/workflows/test.yml").read_text(encoding="utf-8")
    assert "issues" not in registrar.get("permissions", {})
    assert "github.rest.issues.create({" not in registrar_text
    assert "pr-ci-failure-issue-context" in registrar_text

    issue_writer = yaml.safe_load(Path(".github/workflows/issue-on-test-fail.yml").read_text(encoding="utf-8"))
    issue_writer_text = Path(".github/workflows/issue-on-test-fail.yml").read_text(encoding="utf-8")
    assert issue_writer["permissions"]["issues"] == "write"
    assert "github.rest.issues.create" in issue_writer_text
    assert "workflow_run:" in issue_writer_text

    guardrail = yaml.safe_load(Path(".github/workflows/issue-on-guardrail-fail.yml").read_text(encoding="utf-8"))
    guardrail_text = Path(".github/workflows/issue-on-guardrail-fail.yml").read_text(encoding="utf-8")
    assert "issues" not in guardrail.get("permissions", {})
    assert "workflow_run:" not in guardrail_text
    assert "github.rest.issues" not in guardrail_text
    assert "actions/upload-artifact@v4" in guardrail_text


def test_allocator_change_skips_unrelated_backend_matrix(tmp_path: Path) -> None:
    allocator = tmp_path / "tests" / "aistock_validation" / "bugs" / ".bug_id_allocator.json"
    allocator.parent.mkdir(parents=True, exist_ok=True)
    allocator.write_text(json.dumps({"last_allocated": 191}), encoding="utf-8")

    payload = classifier.classify_changed_files(
        ["tests/aistock_validation/bugs/.bug_id_allocator.json"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "bug_registry_metadata_only"
    assert payload["backend_required"] is False
    assert payload["backend_sessions"] == []
    assert any("allocator" in reason for reason in payload["reasons"])


def test_cli_writes_github_outputs(tmp_path: Path, capsys) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-example.json"
    out = tmp_path / "summary.json"
    github_out = tmp_path / "github_output.txt"
    _write_bug(bug, status="closed")

    assert (
        classifier.main(
            [
                "--repo-root",
                str(tmp_path),
                "--changed-file",
                "tests/aistock_validation/bugs/20260601_BUG-191-example.json",
                "--output-json",
                str(out),
                "--github-output",
                str(github_out),
            ]
        )
        == 0
    )

    assert json.loads(out.read_text(encoding="utf-8"))["backend_required"] is False
    assert "backend_required=false" in github_out.read_text(encoding="utf-8")
    assert "backend_sessions=[]" in github_out.read_text(encoding="utf-8")
    assert "workflow_validation_required=false" in github_out.read_text(encoding="utf-8")
    assert "prompt_evaluation_required=false" in github_out.read_text(encoding="utf-8")
    assert json.loads(capsys.readouterr().out)["classification"] == "close_sync_metadata_only"


def test_cli_blocks_unmapped_code_and_writes_diagnostics(tmp_path: Path, capsys) -> None:
    out = tmp_path / "summary.json"
    github_out = tmp_path / "github_output.txt"

    assert (
        classifier.main(
            [
                "--repo-root",
                str(tmp_path),
                "--changed-file",
                "backend/agents/stock_analysis.py",
                "--output-json",
                str(out),
                "--github-output",
                str(github_out),
            ]
        )
        == 2
    )

    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["workflow_gate"] == "blocked"
    assert payload["unmapped_code_files"] == ["backend/agents/stock_analysis.py"]
    github_payload = github_out.read_text(encoding="utf-8")
    assert "classification=unmapped_code_blocked" in github_payload
    assert 'unmapped_code_files=["backend/agents/stock_analysis.py"]' in github_payload
    stdout_payload = json.loads(capsys.readouterr().out)
    assert stdout_payload["workflow_gate"] == "blocked"
    assert stdout_payload["unmapped_code_files"] == ["backend/agents/stock_analysis.py"]


def test_workflow_instruction_skill_changes_use_focused_fast_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "AGENTS.md",
            "docs/codex_project_memory.md",
            "docs/standards/README.md",
            "docs/design/workflow_skill_token_lean_design_20260703.md",
            ".codex/skills/aistock-task-router/SKILL.md",
            ".codex/skills/fix-aistock-issue/SKILL.md",
            ".codex/skills/verify-aistock-feature/SKILL.md",
            ".codex/skills/aistock-validation-delegation/SKILL.md",
            ".codex/skills/aistock-validation-delegation/agents/openai.yaml",
            ".claude/commands/aistock-task-router.md",
            ".claude/commands/fix-aistock-issue.md",
            ".claude/commands/aistock-feature-workflow.md",
            ".claude/commands/aistock-validation-delegation.md",
            "scripts/aistock_issue_workflow.py",
            "scripts/ci_change_classifier.py",
            "backend/tests/scripts/test_aistock_issue_workflow.py",
            "backend/tests/scripts/test_ci_change_classifier.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is True
    assert payload["docs_controlled_required"] is True
