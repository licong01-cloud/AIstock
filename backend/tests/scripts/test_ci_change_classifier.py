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


def _write_test_file(root: Path, relative_path: str) -> None:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("def test_contract():\n    assert True\n", encoding="utf-8")


def _write_noxfile(root: Path, body: str) -> None:
    (root / "noxfile.py").write_text(body, encoding="utf-8")


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


def test_runtime_pending_restart_close_sync_uses_registry_fast_lane(tmp_path: Path) -> None:
    bug = tmp_path / "tests" / "aistock_validation" / "bugs" / "20260601_BUG-191-runtime.json"
    _write_bug(bug, status="fixed_source_pending_user_restart", module="qlib_data")

    payload = classifier.classify_changed_files(
        ["tests/aistock_validation/bugs/20260601_BUG-191-runtime.json"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "close_sync_metadata_only"
    assert payload["close_sync_metadata_only"] is True
    assert payload["backend_required"] is False
    assert payload["workflow_validation_required"] is False


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


def test_self_hosted_workspace_prepare_stays_in_workflow_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "scripts/ci/prepare_self_hosted_workspace.py",
            "backend/tests/scripts/test_prepare_self_hosted_workspace.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["workflow_validation_required"] is True
    assert payload["backend_required"] is False
    assert payload["unmapped_code_files"] == []


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
        "backend/tests/scripts/test_aistock_issue_workflow_fast.py",
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

    assert advisory_payload["classification"] == "dev_db_validation_required"
    assert advisory_payload["backend_required"] is False
    assert advisory_payload["backend_sessions"] == []
    assert advisory_payload["dev_db_required"] is True
    assert advisory_payload["dev_db_plan_keys"] == ["advisory_dev_input_onboarding_backend"]
    assert advisory_payload["runner_kind"] == "windows_ai_stock_ci"
    assert advisory_payload["environment_fingerprint_ref"] == "AIstock-CI"
    assert advisory_payload["install_forbidden"] is True
    advisory_routing = next(
        item for item in advisory_payload["plan_routing"] if item["plan_key"] == "advisory_dev_input_onboarding_backend"
    )
    assert advisory_routing == {
        "plan_key": "advisory_dev_input_onboarding_backend",
        "runner_kind": "windows_ai_stock_ci",
        "requires_dev_db": True,
        "environment_fingerprint_ref": "AIstock-CI",
        "install_forbidden": True,
    }
    assert advisory_payload["unmapped_code_files"] == []

    p0k_payload = classifier.classify_changed_files(
        [
            "scripts/advisory_p0k_build_training_request.py",
            "scripts/wsl/advisory_p0k_train.py",
        ],
        repo_root=tmp_path,
    )
    assert p0k_payload["classification"] == "targeted_ci_required"
    assert p0k_payload["backend_required"] is True
    assert p0k_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert p0k_payload["dev_db_required"] is False
    assert p0k_payload["unmapped_code_files"] == []

    p0l_payload = classifier.classify_changed_files(
        [
            "scripts/advisory_p0l_build_training_request.py",
            "scripts/wsl/advisory_p0l_train.py",
        ],
        repo_root=tmp_path,
    )
    assert p0l_payload["classification"] == "targeted_ci_required"
    assert p0l_payload["backend_required"] is True
    assert p0l_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert p0l_payload["dev_db_required"] is False
    assert p0l_payload["unmapped_code_files"] == []

    n1_payload = classifier.classify_changed_files(
        ["scripts/advisory_n1_tier1_oracle_learnability.py"],
        repo_root=tmp_path,
    )
    assert n1_payload["classification"] == "targeted_ci_required"
    assert n1_payload["backend_required"] is True
    assert n1_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert n1_payload["dev_db_required"] is False
    assert n1_payload["unmapped_code_files"] == []

    alpha_audit_payload = classifier.classify_changed_files(
        ["scripts/advisory_strategy_package_alpha_audit.py"],
        repo_root=tmp_path,
    )
    assert alpha_audit_payload["classification"] == "targeted_ci_required"
    assert alpha_audit_payload["backend_required"] is True
    assert alpha_audit_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert alpha_audit_payload["dev_db_required"] is False
    assert alpha_audit_payload["unmapped_code_files"] == []

    independent_alpha_audit_payload = classifier.classify_changed_files(
        ["scripts/advisory_independent_package_alpha_audit.py"],
        repo_root=tmp_path,
    )
    assert independent_alpha_audit_payload["classification"] == "targeted_ci_required"
    assert independent_alpha_audit_payload["backend_required"] is True
    assert independent_alpha_audit_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert independent_alpha_audit_payload["dev_db_required"] is False
    assert independent_alpha_audit_payload["unmapped_code_files"] == []

    entry_exit_audit_payload = classifier.classify_changed_files(
        ["scripts/advisory_entry_exit_formal_audit.py"],
        repo_root=tmp_path,
    )
    assert entry_exit_audit_payload["classification"] == "targeted_ci_required"
    assert entry_exit_audit_payload["backend_required"] is True
    assert entry_exit_audit_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert entry_exit_audit_payload["dev_db_required"] is False
    assert entry_exit_audit_payload["unmapped_code_files"] == []

    exit_learnability_payload = classifier.classify_changed_files(
        ["scripts/advisory_exit_learnability_audit.py"],
        repo_root=tmp_path,
    )
    assert exit_learnability_payload["classification"] == "targeted_ci_required"
    assert exit_learnability_payload["backend_required"] is True
    assert exit_learnability_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert exit_learnability_payload["dev_db_required"] is False
    assert exit_learnability_payload["unmapped_code_files"] == []

    qe_alpha_preparation_payload = classifier.classify_changed_files(
        ["scripts/advisory_qe_alpha_mve_prepare.py"],
        repo_root=tmp_path,
    )
    assert qe_alpha_preparation_payload["classification"] == "targeted_ci_required"
    assert qe_alpha_preparation_payload["backend_required"] is True
    assert qe_alpha_preparation_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert qe_alpha_preparation_payload["dev_db_required"] is False
    assert qe_alpha_preparation_payload["unmapped_code_files"] == []

    qe_alpha_mve_payload = classifier.classify_changed_files(
        ["scripts/advisory_qe_alpha_mve_run.py"],
        repo_root=tmp_path,
    )
    assert qe_alpha_mve_payload["classification"] == "targeted_ci_required"
    assert qe_alpha_mve_payload["backend_required"] is True
    assert qe_alpha_mve_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert qe_alpha_mve_payload["dev_db_required"] is False
    assert qe_alpha_mve_payload["unmapped_code_files"] == []

    qe_alpha_generator_payload = classifier.classify_changed_files(
        ["scripts/advisory_qe_alpha_generator_mve_run.py"],
        repo_root=tmp_path,
    )
    assert qe_alpha_generator_payload["classification"] == "targeted_ci_required"
    assert qe_alpha_generator_payload["backend_required"] is True
    assert qe_alpha_generator_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert qe_alpha_generator_payload["dev_db_required"] is False
    assert qe_alpha_generator_payload["unmapped_code_files"] == []

    parent_overlay_payload = classifier.classify_changed_files(
        ["scripts/advisory_parent_incremental_overlay_run.py"],
        repo_root=tmp_path,
    )
    assert parent_overlay_payload["classification"] == "targeted_ci_required"
    assert parent_overlay_payload["backend_required"] is True
    assert parent_overlay_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert parent_overlay_payload["dev_db_required"] is False
    assert parent_overlay_payload["unmapped_code_files"] == []

    leg_disagreement_payload = classifier.classify_changed_files(
        ["scripts/advisory_leg_disagreement_mve_run.py"],
        repo_root=tmp_path,
    )
    assert leg_disagreement_payload["classification"] == "targeted_ci_required"
    assert leg_disagreement_payload["backend_required"] is True
    assert leg_disagreement_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert leg_disagreement_payload["dev_db_required"] is False
    assert leg_disagreement_payload["unmapped_code_files"] == []

    minute_information_payload = classifier.classify_changed_files(
        ["scripts/advisory_minute_information_set_mve_run.py"],
        repo_root=tmp_path,
    )
    assert minute_information_payload["classification"] == "targeted_ci_required"
    assert minute_information_payload["backend_required"] is True
    assert minute_information_payload["backend_sessions"] == ["advisory_modeling_backend"]
    assert minute_information_payload["dev_db_required"] is False
    assert minute_information_payload["unmapped_code_files"] == []

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

    qe_candidate_payload = classifier.classify_changed_files(
        [
            "scripts/qe_alpha_candidates/sector_rotation/m_sector_participation_gap_v2.py",
            "backend/tests/quantevolver/test_sector_participation_gap_v2.py",
        ],
        repo_root=tmp_path,
    )

    assert qe_candidate_payload["classification"] == "targeted_ci_required"
    assert qe_candidate_payload["backend_sessions"] == ["qe_read_backend"]
    assert qe_candidate_payload["catalog_impacted_modules"] == [
        "qe.core",
        "qe.auto_evolution",
        "factor_library",
    ]
    assert qe_candidate_payload["unmapped_code_files"] == []

    qe_node_health_payload = classifier.classify_changed_files(
        [
            "backend/schedulers/node_health_scheduler.py",
            "backend/tests/test_dispatch_observer_quiet.py",
        ],
        repo_root=tmp_path,
    )

    assert qe_node_health_payload["classification"] == "targeted_ci_required"
    assert qe_node_health_payload["backend_sessions"] == ["qe_read_backend"]
    assert qe_node_health_payload["catalog_impacted_modules"] == [
        "qe.core",
        "platform.api",
    ]
    assert qe_node_health_payload["unmapped_code_files"] == []

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


def test_daily_basic_operator_files_select_local_data_plan(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "scripts/backfill_tushare_daily_basic_fields.py",
            "scripts/ingest_tushare_daily_basic.py",
            "backend/tests/scripts/test_backfill_tushare_daily_basic_fields.py",
            "backend/tests/scripts/test_ingest_tushare_daily_basic.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_required"] is True
    assert payload["backend_plan_keys"] == ["l0", "data_sync_autonomy_backend"]
    assert payload["backend_sessions"] == ["data_sync_autonomy_backend"]
    assert payload["catalog_impacted_modules"] == [
        "local_data",
        "qlib_data",
        "qe.core",
        "paper_v2",
        "selection_center",
    ]
    assert payload["unmapped_code_files"] == []


def test_issuer_bound_pit_files_select_local_data_plan(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "backend/tests/announcements/test_title_classifier.py",
            "backend/tests/test_announcement_event_schema.py",
            "backend/tests/event_signal/test_st_announcement_adapter.py",
            "backend/tests/scripts/test_sync_eastmoney_anns_metadata.py",
            "scripts/classify_announcement_titles_v0.py",
            "scripts/sync_eastmoney_anns_metadata.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_required"] is True
    assert payload["backend_plan_keys"] == ["l0", "data_sync_autonomy_backend"]
    assert payload["backend_sessions"] == ["data_sync_autonomy_backend"]
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


def test_qlib_exporter_tests_select_qlib_data_backend(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "backend/tests/qlib_exporter/test_db_reader_minute_query.py",
            "backend/tests/test_qlib_export_stock_universe_filters.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_required"] is True
    assert payload["backend_sessions"] == ["qlib_data_backend"]
    assert payload["unmapped_code_files"] == []


def test_dataset_refresh_audit_operator_selects_qlib_data_backend(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "scripts/seed_dataset_refresh_audit.py",
            "backend/tests/test_dataset_refresh_audit.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_required"] is True
    assert payload["backend_sessions"] == ["qlib_data_backend"]
    assert payload["catalog_impacted_modules"] == [
        "qlib_data",
        "qe.core",
        "local_data",
        "platform.api",
    ]
    assert payload["unmapped_code_files"] == []


def test_canonical_equity_pit_selects_qlib_data_backend(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "backend/services/canonical_equity_pit.py",
            "backend/tests/test_canonical_equity_pit.py",
            "configs/datasets/qe_backtest_monthly_v2.yaml",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_required"] is True
    assert payload["backend_sessions"] == ["qlib_data_backend"]
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


def test_validation_mcp_issue_files_use_focused_workflow_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "scripts/aistock_bug_id_allocator.py",
            "scripts/aistock_issue_workflow.py",
            "backend/tests/scripts/test_aistock_issue_workflow.py",
            "scripts/aistock_mcp_server.py",
            "backend/tests/scripts/test_aistock_mcp_github_issue_tools.py",
        ],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_required"] is False
    assert payload["unmapped_code_files"] == []
    assert payload["workflow_test_targets"] == [
        "backend/tests/scripts/test_aistock_issue_workflow_fast.py",
        "backend/tests/scripts/test_aistock_mcp_github_issue_tools.py",
    ]


def test_workflow_fast_contract_test_has_direct_self_mapping(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["backend/tests/scripts/test_aistock_issue_workflow_fast.py"],
        repo_root=tmp_path,
    )

    assert payload["workflow_gate"] == "passed"
    assert payload["unmapped_code_files"] == []
    assert payload["workflow_test_targets"] == ["backend/tests/scripts/test_aistock_issue_workflow_fast.py"]


def test_ci_environment_and_policy_scripts_use_direct_workflow_tests(tmp_path: Path) -> None:
    expected = {
        "scripts/ci_environment_verify.py": "backend/tests/scripts/test_ci_environment_verify.py",
        "scripts/ci_workflow_policy_scan.py": "backend/tests/scripts/test_ci_workflow_policy_scan.py",
        "scripts/aistock_runner_health.py": "backend/tests/scripts/test_aistock_runner_health.py",
        "scripts/configure_aistock_github_runner.ps1": "backend/tests/scripts/test_configure_aistock_github_runner.py",
        "scripts/start_aistock_github_runner.ps1": "backend/tests/scripts/test_start_aistock_github_runner.py",
        "scripts/supervise_aistock_github_runner.ps1": "backend/tests/scripts/test_start_aistock_github_runner.py",
    }

    for source, test_target in expected.items():
        payload = classifier.classify_changed_files([source], repo_root=tmp_path)

        assert payload["classification"] == "workflow_validation_only"
        assert payload["workflow_gate"] == "passed"
        assert payload["backend_required"] is False
        assert payload["unmapped_code_files"] == []
        assert payload["workflow_test_targets"] == [test_target]


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


def test_aistock_mcp_server_test_uses_direct_workflow_target(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["backend/tests/test_aistock_mcp_server.py"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "workflow_validation_only"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_required"] is False
    assert payload["backend_sessions"] == []
    assert payload["workflow_test_targets"] == ["backend/tests/test_aistock_mcp_server.py"]
    assert payload["unmapped_code_files"] == []


def test_announcement_issuer_binding_tests_are_blocked_when_selected_plan_omits_them() -> None:
    payload = classifier.classify_changed_files(
        [
            "backend/services/event_signal/announcement_adapter.py",
            "backend/services/event_signal/announcement_issuer_binding.py",
            "backend/services/event_signal/st_announcement_adapter.py",
            "backend/tests/event_signal/test_announcement_adapter.py",
            "backend/tests/event_signal/test_announcement_issuer_binding.py",
            "backend/tests/event_signal/test_st_announcement_adapter.py",
            "scripts/repair_announcement_event_signal_issuer_binding.py",
            "backend/tests/scripts/test_repair_announcement_event_signal_issuer_binding.py",
            "scripts/sync_stock_namechange.py",
            "backend/tests/scripts/test_sync_stock_namechange.py",
        ]
    )

    assert payload["classification"] == "unexecuted_test_blocked"
    assert payload["workflow_gate"] == "blocked"
    assert payload["unmapped_code_files"] == []
    assert "data_sync_autonomy_backend" in payload["backend_sessions"]
    assert payload["unexecuted_test_files"] == [
        "backend/tests/event_signal/test_announcement_adapter.py",
        "backend/tests/event_signal/test_announcement_issuer_binding.py",
        "backend/tests/event_signal/test_st_announcement_adapter.py",
        "backend/tests/scripts/test_repair_announcement_event_signal_issuer_binding.py",
        "backend/tests/scripts/test_sync_stock_namechange.py",
    ]


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


def test_qrun_mlflow_retry_test_uses_qe_sector_risk_overlay_lane(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["backend/tests/unified_engine/test_qrun_mlflow_metric_retry.py"],
        repo_root=tmp_path,
    )

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["backend_required"] is True
    assert payload["backend_plan_keys"] == ["l0", "qe_sector_risk_overlay_backend"]
    assert payload["backend_sessions"] == ["qe_sector_risk_overlay_backend"]
    assert payload["unmapped_code_files"] == []


def test_changed_test_is_blocked_when_selected_nox_session_does_not_execute_it(tmp_path: Path) -> None:
    test_path = "backend/tests/quantevolver/test_qe_sector_risk_overlay_direct_v2_dataset_binding.py"
    _write_test_file(tmp_path, test_path)
    _write_noxfile(
        tmp_path,
        """
def qe_sector_risk_overlay_backend(session):
    _run_pytest(session, "backend/tests/quantevolver/test_sector_risk_overlay.py")
""",
    )

    payload = classifier.classify_changed_files([test_path], repo_root=tmp_path, added_files=[test_path])

    assert payload["classification"] == "unexecuted_test_blocked"
    assert payload["workflow_gate"] == "blocked"
    assert payload["unmapped_code_files"] == []
    assert payload["unexecuted_test_files"] == [test_path]
    assert payload["changed_test_plan_coverage"]["coverage"] == {test_path: []}
    assert "not executed by any selected CI plan" in payload["blocking"][0]


def test_changed_test_passes_only_when_selected_nox_session_executes_exact_file(tmp_path: Path) -> None:
    test_path = "backend/tests/quantevolver/test_qe_sector_risk_overlay_direct_v2_dataset_binding.py"
    _write_test_file(tmp_path, test_path)
    _write_noxfile(
        tmp_path,
        f"""
def qe_sector_risk_overlay_backend(session):
    _run_pytest(session, "{test_path}", "-q")
""",
    )

    payload = classifier.classify_changed_files([test_path], repo_root=tmp_path, added_files=[test_path])

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["unexecuted_test_files"] == []
    assert payload["changed_test_plan_coverage"]["coverage"] == {
        test_path: ["qe_sector_risk_overlay_backend"]
    }


def test_changed_test_directory_target_is_reachable_but_unselected_session_is_not(tmp_path: Path) -> None:
    test_path = "backend/tests/quantevolver/test_qe_sector_risk_overlay_new_contract.py"
    _write_test_file(tmp_path, test_path)
    _write_noxfile(
        tmp_path,
        """
def qe_sector_risk_overlay_backend(session):
    _run_pytest(session, "backend/tests/quantevolver")

def qe_read_backend(session):
    _run_pytest(session, "backend/tests/quantevolver/test_qe_sector_risk_overlay_new_contract.py")
""",
    )

    reachable = classifier.classify_changed_files([test_path], repo_root=tmp_path)
    assert reachable["workflow_gate"] == "passed"
    assert reachable["changed_test_plan_coverage"]["coverage"] == {
        test_path: ["qe_sector_risk_overlay_backend"]
    }

    _write_noxfile(
        tmp_path,
        """
def qe_sector_risk_overlay_backend(session):
    _run_pytest(session, "backend/tests/quantevolver/test_sector_risk_overlay.py")

def qe_read_backend(session):
    _run_pytest(session, "backend/tests/quantevolver/test_qe_sector_risk_overlay_new_contract.py")
""",
    )
    unreachable = classifier.classify_changed_files([test_path], repo_root=tmp_path)
    assert unreachable["workflow_gate"] == "blocked"
    assert unreachable["unexecuted_test_files"] == [test_path]


def test_nox_target_resolver_handles_dynamic_path_candidates(tmp_path: Path) -> None:
    _write_noxfile(
        tmp_path,
        """
def model_registry_backend(session):
    tests_dir = ROOT / "backend" / "tests" / "model_registry"
    pytest_targets = []
    for candidate in ("test_governance.py", "test_registry.py"):
        path = tests_dir / candidate
        if path.exists():
            pytest_targets.append(f"backend/tests/model_registry/{candidate}")
    _run_pytest(session, *pytest_targets, "-q")
""",
    )

    targets, error = classifier._selected_nox_test_targets(  # noqa: SLF001
        repo_root=tmp_path,
        sessions=["model_registry_backend"],
    )

    assert error is None
    assert targets == {
        "model_registry_backend": {
            "backend/tests/model_registry/test_governance.py",
            "backend/tests/model_registry/test_registry.py",
        }
    }


def test_nox_wildcard_does_not_claim_changed_test_execution(tmp_path: Path) -> None:
    test_path = "backend/tests/quantevolver/test_qe_sector_risk_overlay_new_contract.py"
    _write_test_file(tmp_path, test_path)
    _write_noxfile(
        tmp_path,
        """
def qe_sector_risk_overlay_backend(session):
    _run_pytest(session, "backend/tests/quantevolver/test_qe_sector_risk_overlay_*.py")
""",
    )

    payload = classifier.classify_changed_files([test_path], repo_root=tmp_path)

    assert payload["classification"] == "unexecuted_test_blocked"
    assert payload["unexecuted_test_files"] == [test_path]


def test_deleted_test_path_does_not_require_execution_in_merge_tree(tmp_path: Path) -> None:
    test_path = "backend/tests/quantevolver/test_qe_sector_risk_overlay_removed_contract.py"

    payload = classifier.classify_changed_files([test_path], repo_root=tmp_path)

    assert payload["classification"] == "targeted_ci_required"
    assert payload["workflow_gate"] == "passed"
    assert payload["changed_test_plan_coverage"]["changed_test_files"] == []
    assert payload["unexecuted_test_files"] == []


def test_mixed_pr_keeps_workflow_tests_out_of_backend_collection_contract(tmp_path: Path) -> None:
    backend_test = "backend/tests/quantevolver/test_qe_sector_risk_overlay_new_contract.py"
    workflow_test = "backend/tests/scripts/test_ci_changed_files.py"
    _write_test_file(tmp_path, backend_test)
    _write_test_file(tmp_path, workflow_test)
    _write_noxfile(
        tmp_path,
        f"""
def qe_sector_risk_overlay_backend(session):
    _run_pytest(session, "{backend_test}")
""",
    )

    payload = classifier.classify_changed_files(
        [backend_test, workflow_test],
        repo_root=tmp_path,
    )

    assert payload["workflow_gate"] == "passed"
    assert payload["backend_changed_test_files"] == [backend_test]
    assert workflow_test in payload["workflow_test_targets"]


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

    assert set(jobs) == {"ci-verdict"}
    verdict = jobs["ci-verdict"]
    workflow_condition = (
        "always() && steps.classify.outcome == 'success' && "
        "steps.classify.outputs.workflow_validation_required == 'true' && "
        "steps.classify.outputs.workflow_test_targets != '[]'"
    )
    workflow_validation = next(step for step in verdict["steps"] if step.get("id") == "workflow_validation")
    workflow_policy = next(step for step in verdict["steps"] if step.get("id") == "workflow_policy")
    assert workflow_validation["if"] == workflow_condition
    assert workflow_policy["if"] == workflow_condition
    workflow_runs = "\n".join(str(step.get("run", "")) for step in verdict["steps"])
    assert "WORKFLOW_TEST_TARGETS" in workflow_runs
    assert 'python -m pytest "${workflow_test_targets[@]}"' in workflow_runs
    assert "backend/tests/scripts/test_llm_provider_adapter.py \\" not in workflow_runs
    frontend_steps = verdict["steps"]
    attach_step = next(
        step for step in frontend_steps if step.get("name") == "Attach lockfile-matched prebuilt frontend dependencies"
    )
    assert attach_step["id"] == "frontend_dependencies"
    assert attach_step["shell"] == "powershell"
    assert "--attach-frontend-only" in attach_step["run"]
    assert (
        '--frontend-node-modules-source "${env:AISTOCK_SELF_HOSTED_SOURCE}/frontend/node_modules"' in attach_step["run"]
    )
    frontend_runs = str(next(step for step in verdict["steps"] if step.get("id") == "frontend_validation")["run"])
    assert "node node_modules/typescript/bin/tsc --noEmit --incremental false" in frontend_runs
    assert "node node_modules/next/dist/bin/next lint" in frontend_runs
    assert "npx playwright install --with-deps chromium" not in frontend_runs
    assert "FRONTEND_TEST_TARGETS" in frontend_runs
    assert 'node node_modules/@playwright/test/cli.js test "${module_test_targets[@]}"' in frontend_runs
    assert "node_modules/.bin" not in frontend_runs
    assert "npm run" not in frontend_runs
    go_runs = str(next(step for step in verdict["steps"] if step.get("id") == "go_validation")["run"])
    assert "go test ./..." in go_runs
    prompt_eval_run_steps = str(next(step for step in verdict["steps"] if step.get("id") == "prompt_validation")["run"])
    assert "scripts/llm_provider_adapter.py --json prompt-evaluation" in prompt_eval_run_steps
    assert "failure-bug-register" not in jobs
    assert "actions/upload-artifact@" not in Path(".github/workflows/test.yml").read_text(encoding="utf-8")
    assert "actions/download-artifact@" not in Path(".github/workflows/test.yml").read_text(encoding="utf-8")


def test_github_backend_lane_uses_prebuilt_windows_environment_without_database_service_or_install() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    verdict = workflow["jobs"]["ci-verdict"]
    assert "aistock-ci" in verdict["runs-on"]
    assert "services" not in verdict
    steps = verdict["steps"]
    assert any(step.get("name") == "Verify prebuilt AIstock-CI environment" for step in steps)
    runs = "\n".join(str(step.get("run", "")) for step in steps)
    assert "pip install" not in runs
    assert "conda install" not in runs
    assert "scripts/ci_environment_verify.py" in runs
    assert 'python -m nox -s "${session}"' in runs


def test_github_workflow_has_single_fail_closed_ci_verdict() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    jobs = workflow["jobs"]
    verdict = jobs["ci-verdict"]
    close_sync_expression = "startsWith(github.head_ref, 'chore/BUG-') && contains(github.head_ref, '-close-sync-')"
    assert set(jobs) == {"ci-verdict"}
    assert verdict["name"] == "CI verdict"
    assert close_sync_expression in verdict["runs-on"]
    assert "ubuntu-latest" in verdict["runs-on"]
    classify_step = next(step for step in verdict["steps"] if step.get("id") == "classify")
    assert "scripts/bug_registry_metadata_check.py" in classify_step["run"]
    assert "runner_kind=github_hosted_metadata" in classify_step["run"]
    assert "close_sync_metadata_only=true" in classify_step["run"]
    assert "needs" not in verdict
    verdict_step = next(
        step for step in verdict["steps"] if step.get("name") == "Require every selected CI lane to pass"
    )
    run = str(verdict_step["run"])
    assert 'failures+=("classify_static=${CLASSIFY_RESULT}")' in run
    for lane in ("backend", "frontend", "go", "prompt"):
        assert f'"{lane}:${{{lane.upper() if lane != "go" else "GO"}_RESULT}}"' in run
    assert verdict_step["env"]["WORKFLOW_TEST_RESULT"] == "${{ steps.workflow_validation.outcome }}"
    assert verdict_step["env"]["WORKFLOW_POLICY_RESULT"] == "${{ steps.workflow_policy.outcome }}"
    assert "workflow_validation=${WORKFLOW_TEST_RESULT}" in run
    assert "workflow_policy=${WORKFLOW_POLICY_RESULT}" in run
    assert "REGISTRAR_RESULT" not in verdict_step.get("env", {})
    assert '"registrar:' not in run
    assert 'result" != "success"' in run
    assert 'result" != "skipped"' in run
    assert "CI verdict failed" in run
    assert "The failed job logs are the authoritative PR evidence" in run
    assert "failure-bug-register" not in jobs

    classify_steps = verdict["steps"]
    assert any(step.get("name") == "Classify CI lane" for step in classify_steps)


def test_classification_job_reuses_one_checkout_for_static_and_registry_gates() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    jobs = workflow["jobs"]
    assert "static-gate" not in jobs
    assert "docs-lite" not in jobs
    static_gate_steps = jobs["ci-verdict"]["steps"]
    assert sum("actions/checkout@" in str(step.get("uses") or "") for step in static_gate_steps) == 1
    assert sum(step.get("name") == "Verify prebuilt AIstock-CI environment" for step in static_gate_steps) == 1
    classify_step = next(step for step in static_gate_steps if step.get("id") == "classify")
    assert "scripts/bug_registry_metadata_check.py" in classify_step["run"]
    assert "--close-sync-only" in classify_step["run"]
    assert "tmp/validation/ci_change_classifier/changed_files.txt" in classify_step["run"]

    nox_steps = [
        step
        for step in static_gate_steps
        if isinstance(step, dict) and str(step.get("name") or "").startswith("nox -s ")
    ]
    assert nox_steps
    assert all(
        "steps.classify.outputs.close_sync_metadata_only != 'true'" in str(step.get("if") or "") for step in nox_steps
    )
    l0_step = next(step for step in nox_steps if step.get("name") == "nox -s l0 -- changed files")
    assert "tmp/validation/ci_change_classifier/changed_files.txt" in l0_step["run"]
    assert 'if [ -e "${path}" ]' in l0_step["run"]
    assert 'python -m nox -s l0 -- "${changed_files[@]}"' in l0_step["run"]
    assert not any(
        str(step.get("name") or "").startswith("Build static-gate changed-file") for step in static_gate_steps
    )

    catalog_steps = [
        step
        for step in nox_steps
        if step.get("name") in {"nox -s validation_module_registry_l0", "nox -s validation_catalog_integrity"}
    ]
    assert len(catalog_steps) == 2
    assert all(
        "steps.classify.outputs.catalog_validation_required == 'true'" in str(step.get("if") or "")
        for step in catalog_steps
    )


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
        "backend/tests/scripts/test_aistock_issue_workflow_fast.py",
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
    assert names.count("Emit compact PR quality receipt") == 1
    assert names.count("Comment PR summary") == 0
    assert names.count("Upload PR quality artifacts") == 0
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


def test_pr_quality_proves_merge_base_and_boundedly_deepens_exact_pr_refs() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/pr-quality.yml").read_text(encoding="utf-8"))
    steps = workflow["jobs"]["pr-quality"]["steps"]
    detect = next(step for step in steps if step.get("name") == "Detect PR quality lane")
    run = str(detect["run"])

    assert detect["env"]["BASE_SHA"] == "${{ github.event.pull_request.base.sha || '' }}"
    assert detect["env"]["CHECKOUT_REF"] == "${{ github.ref || '' }}"
    assert "python scripts/ci_changed_files.py" in run
    assert "--prepare-pr-merge-base-only" in run
    assert '--base-sha "${BASE_SHA}"' in run
    assert '--checkout-ref "${CHECKOUT_REF}"' in run
    assert run.index("--prepare-pr-merge-base-only") < run.index('git diff --name-only "${BASE_COMMIT}...HEAD"')


def test_codeql_selects_only_changed_languages() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/codeql.yml").read_text(encoding="utf-8"))
    jobs = workflow["jobs"]
    assert list(jobs) == ["codeql-verdict"]
    verdict = jobs["codeql-verdict"]
    verdict_steps = verdict["steps"]
    prepare_steps = [
        step for step in verdict_steps if step.get("name") == "Prepare exact local workspace (no remote actions)"
    ]

    assert verdict["name"] == "CodeQL verdict"
    assert verdict["runs-on"] == ["self-hosted", "Windows", "aistock-ci-security"]
    assert "needs" not in verdict
    assert "strategy" not in verdict
    detect_step = next(step for step in verdict_steps if step.get("name") == "Detect CodeQL fast lane")
    assert detect_step["id"] == "fast_lane"
    assert "scripts/ci_change_classifier.py" in detect_step["run"]
    assert "close_sync_metadata_only" in detect_step["run"]
    assert "codeql_pr_languages" in detect_step["run"]
    assert "codeql_languages" in detect_step["run"]
    assert detect_step["env"]["EVENT_NAME"] == "${{ github.event_name }}"
    assert "pull_request_test_only" in detect_step["run"]
    assert "PYTHON_CHANGED" not in detect_step["run"]
    assert len(prepare_steps) == 1
    assert all("--no-write-fetch-head" in step["run"] for step in prepare_steps)
    assert all("--depth=1" not in step["run"] for step in prepare_steps)
    assert all(step["run"].count("rev-parse --verify --quiet") == 2 for step in prepare_steps)
    assert all("refs/aistock-ci/codeql-" in step["run"] for step in prepare_steps)
    assert all("update-ref -d $cacheRef" in step["run"] for step in prepare_steps)
    assert all('$env:GIT_CONFIG_KEY_0 = "core.longpaths"' in step["run"] for step in prepare_steps)
    assert all("refs/pull/$env:PR_NUMBER/merge" in step["run"] for step in prepare_steps)
    assert all("exact workspace source fetch failed after 3 attempts" in step["run"] for step in prepare_steps)
    assert all("scripts/ci/prepare_self_hosted_workspace.py" in step["run"] for step in prepare_steps)
    assert not any("uses" in step for step in verdict_steps)

    direct_analysis = next(step for step in verdict_steps if step.get("name") == "Run CodeQL CLI analysis")
    assert direct_analysis["if"] == "steps.fast_lane.outputs.has_languages == '1'"
    assert direct_analysis["env"]["CODEQL_LANGUAGES"] == "${{ steps.fast_lane.outputs.languages }}"
    assert direct_analysis["env"]["GITHUB_TOKEN"] == "${{ github.token }}"
    direct_run = direct_analysis["run"]
    assert "[string[]]$languages = ($env:CODEQL_LANGUAGES | ConvertFrom-Json)" in direct_run
    assert "foreach ($language in $languages)" in direct_run
    assert "database create" in direct_run
    assert '"--build-mode=none"' in direct_run
    assert "database analyze" in direct_run
    assert "github upload-results" in direct_run
    assert '"--wait-for-processing-timeout=120"' in direct_run
    assert "ci_environment_verify.py" in direct_run
    assert not any("github/codeql-action/" in str(step.get("uses") or "") for step in verdict_steps)
    assert direct_analysis["env"]["AISTOCK_CI_CODEQL_BUNDLE_REQUIRED"] == "1"
    assert len(verdict["env"]["AISTOCK_CI_CODEQL_BUNDLE_SHA256"]) == 64

    final_verdict = next(step for step in verdict_steps if step.get("name") == "Enforce CodeQL result")
    assert final_verdict["if"] == "always()"
    assert final_verdict["env"]["CLASSIFIER_RESULT"] == "${{ steps.fast_lane.outcome }}"
    assert final_verdict["env"]["ANALYZE_RESULT"] == "${{ steps.codeql_analysis.outcome }}"
    assert "CodeQL analysis failed" in final_verdict["run"]


def test_codeql_pr_skips_test_only_languages_but_preserves_main_push_languages(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "backend/tests/scripts/test_nightly_session_runner.py",
            "tests/aistock_validation/bugs/20260827_BUG-1210-example.json",
        ],
        repo_root=tmp_path,
    )

    assert payload["codeql_languages"] == ["python"]
    assert payload["codeql_pr_languages"] == []
    assert payload["codeql_pr_test_only"] is True


def test_codeql_pr_keeps_runtime_language_when_source_and_tests_change(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        ["backend/services/example.py", "backend/tests/test_example.py"],
        repo_root=tmp_path,
    )

    assert payload["codeql_languages"] == ["python"]
    assert payload["codeql_pr_languages"] == ["python"]
    assert payload["codeql_pr_test_only"] is False


def test_codeql_pr_skips_frontend_test_only_language(tmp_path: Path) -> None:
    payload = classifier.classify_changed_files(
        [
            "frontend/src/example.spec.ts",
            "frontend/src/__tests__/helper.ts",
            "frontend/e2e/example.test.tsx",
        ],
        repo_root=tmp_path,
    )

    assert payload["codeql_languages"] == ["javascript-typescript"]
    assert payload["codeql_pr_languages"] == []
    assert payload["codeql_pr_test_only"] is True


def test_non_security_quality_workflows_do_not_repeat_on_merge_commit() -> None:
    import yaml

    ci_triggers = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))[True]
    assert "pull_request" in ci_triggers
    assert "push" not in ci_triggers

    for relative_path in (".github/workflows/pr-quality.yml", ".github/workflows/semgrep.yml"):
        triggers = yaml.safe_load(Path(relative_path).read_text(encoding="utf-8"))[True]
        assert set(triggers) == {"workflow_dispatch"}

    codeql = yaml.safe_load(Path(".github/workflows/codeql.yml").read_text(encoding="utf-8"))[True]
    assert "pull_request" in codeql
    assert codeql["push"]["branches"] == ["main"]


def test_pr_quality_and_semgrep_enforcement_share_ci_verdict_runner() -> None:
    import yaml

    workflow = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    assert set(workflow["jobs"]) == {"ci-verdict"}
    steps = workflow["jobs"]["ci-verdict"]["steps"]
    pr_quality = next(step for step in steps if step.get("id") == "pr_quality_validation")
    semgrep = next(step for step in steps if step.get("id") == "semgrep_validation")
    verdict = next(step for step in steps if step.get("name") == "Require every selected CI lane to pass")
    assert pr_quality["continue-on-error"] is True
    assert semgrep["continue-on-error"] is True
    assert "scripts/issue_flow.py pr-check" in pr_quality["run"]
    assert "ruff check --force-exclude" in pr_quality["run"]
    assert "semgrep" in semgrep["run"]
    assert "--config .semgrep.yml" in semgrep["run"]
    assert verdict["env"]["PR_QUALITY_RESULT"] == "${{ steps.pr_quality_validation.outcome }}"
    assert verdict["env"]["SEMGREP_RESULT"] == "${{ steps.semgrep_validation.outcome }}"
    assert '"pr_quality:${PR_QUALITY_RESULT}"' in verdict["run"]
    assert '"semgrep:${SEMGREP_RESULT}"' in verdict["run"]


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


def test_classifier_uses_prebuilt_tooling_without_install_steps() -> None:
    import yaml

    workflows = {
        ".github/workflows/test.yml": ("ci-verdict", "Classify CI lane"),
        ".github/workflows/pr-quality.yml": ("pr-quality", "Detect PR quality lane"),
        ".github/workflows/codeql.yml": ("codeql-verdict", "Detect CodeQL fast lane"),
        ".github/workflows/semgrep.yml": ("semgrep", "Detect Semgrep fast lane"),
    }
    for path, (job_name, detect_name) in workflows.items():
        workflow = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        steps = workflow["jobs"][job_name]["steps"]
        runs = "\n".join(str(step.get("run", "")) for step in steps)
        uses = "\n".join(str(step.get("uses", "")) for step in steps)
        assert "pip install" not in runs
        assert "setup-python" not in uses
        assert any(step.get("name") == detect_name for step in steps)


def test_issue_on_test_fail_is_the_only_failure_issue_writer() -> None:
    import yaml

    ci = yaml.safe_load(Path(".github/workflows/test.yml").read_text(encoding="utf-8"))
    registrar_text = Path(".github/workflows/test.yml").read_text(encoding="utf-8")
    assert "failure-bug-register" not in ci["jobs"]
    assert "github.rest.issues.create({" not in registrar_text
    assert "pr-ci-failure-issue-context" not in registrar_text
    assert "actions/upload-artifact@" not in registrar_text

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
    assert "actions/upload-artifact@v7" in guardrail_text


def test_javascript_actions_use_native_node24_major_versions() -> None:
    combined = "\n".join(path.read_text(encoding="utf-8") for path in sorted(Path(".github/workflows").glob("*.yml")))
    expected = {
        "actions/checkout@": "actions/checkout@v7",
        "actions/upload-artifact@": "actions/upload-artifact@v7",
        "actions/download-artifact@": "actions/download-artifact@v8",
        "actions/github-script@": "actions/github-script@v9",
    }
    for prefix, expected_ref in expected.items():
        refs = {
            line.strip().removeprefix("- ").removeprefix("uses: ") for line in combined.splitlines() if prefix in line
        }
        assert refs == {expected_ref}


def test_merge_quality_workflows_do_not_duplicate_close_sync_runner_work() -> None:
    codeql_text = Path(".github/workflows/codeql.yml").read_text(encoding="utf-8")
    assert "pull_request:" in codeql_text
    pull_request_block = codeql_text.split("  pull_request:\n", 1)[1].split("\n  ", 1)[0]
    assert "tests/aistock_validation/bugs/**" not in pull_request_block

    for relative_path in (".github/workflows/semgrep.yml", ".github/workflows/pr-quality.yml"):
        text = Path(relative_path).read_text(encoding="utf-8")
        assert "pull_request:" not in text
        assert "workflow_dispatch:" in text

    issue_link_text = Path(".github/workflows/issue-auto-link.yml").read_text(encoding="utf-8")
    assert "- 'tests/aistock_validation/bugs/**'" in issue_link_text


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
