from pathlib import Path


MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "db"
    / "migrations"
    / "add_advisory_historical_range_r4_outcome_bridge_20260723.sql"
)


def test_r4_migration_closes_outcome_summary_operation_and_lineage_contracts() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    for token in (
        "evaluation_window_type",
        "outcome_input_hash",
        "revision_reason",
        "producer_code_hash",
        "summary_policy_hash",
        "summary_input_hash",
        "maturity_coverage_hash",
        "OUTCOME_REFRESH_RECEIPT",
        "DATASET_BRIDGE_RECEIPT",
        "HISTORICAL_RANGE_RESEARCH",
        "range_lineage_identity_hash",
        "RETROSPECTIVE_RESEARCH_ONLY",
        "selector_policy_hash",
    ):
        assert token in sql
    assert "horizon_trade_days = 0" in sql
    assert "ux_advisory_hr_r4_outcome_input" in sql
    assert "ux_advisory_hr_r4_summary_input" in sql
    assert (
        "predecessor.summary_artifact_ref IS DISTINCT FROM "
        "NEW.summary_json->'predecessor_summary_ref'"
    ) in sql
    assert (
        "predecessor.outcome_artifact_ref IS DISTINCT FROM "
        "NEW.outcome_json->'predecessor_outcome_ref'"
    ) in sql
    assert "verify_advisory_hr_r4_outcome_artifact_columns" in sql
    assert "trg_verify_advisory_hr_r4_outcome_artifact_columns" in sql
    assert "ADVISORY_HR_R4_OUTCOME_ARTIFACT_COLUMNS_CONFLICT" in sql
    assert "verify_advisory_hr_r4_summary_artifact_columns" in sql
    assert "trg_verify_advisory_hr_r4_summary_artifact_columns" in sql
    assert "ADVISORY_HR_R4_SUMMARY_ARTIFACT_COLUMNS_CONFLICT" in sql
    assert "ADVISORY_HR_R4_OUTCOME_EXPLICIT_BACKFILL_REQUIRED" in sql
    assert "ADVISORY_HR_R4_SUMMARY_EXPLICIT_BACKFILL_REQUIRED" in sql
    assert "SOURCE_CORRECTION" in sql and "CALCULATION_CORRECTION" in sql
    assert "advisory_capture_batch_schema_purpose_check" in sql
    assert "advisory_phase1_retrospective_capture_batch_v1" in sql
    assert "advisory_phase1_retrospective_label_capture_batch_v1" in sql
    assert "advisory_phase1_retrospective_capture_binding_v1" in sql
    assert "advisory_phase1_retrospective_label_capture_binding_v1" in sql
    assert "required_candidate_stage" in sql
    assert "WHEN 'HISTORICAL_RANGE_OUTCOME_POLICY' THEN 'selection_effective'" in sql
    assert "pg_inherits" in sql
    assert "ALTER COLUMN phase0a_signal_context_hash DROP NOT NULL" in sql
    assert "AND program_id IS NOT NULL AND binding_version_id IS NOT NULL" in sql
    assert "AND source_run_id IS NOT NULL AND source_run_id = range_day_run_id" in sql
    assert "AND phase0a_audit_id IS NULL AND admission_scope_id IS NULL" in sql
    assert "AND program_id IS NULL AND binding_version_id IS NULL" in sql
    assert "AND selector_policy_hash IS NULL" in sql
    assert "AND selection_run_content_hash IS NOT NULL" in sql
    assert "AND selection_score_artifact_id IS NOT NULL" in sql
    assert "AND selection_score_artifact_hash IS NOT NULL" in sql
    assert "AND selection_run_content_hash IS NULL" in sql
    assert "AND selection_score_artifact_id IS NULL" in sql
    assert "AND selection_score_artifact_hash IS NULL" in sql
    observation_union = sql.split(
        "ADD CONSTRAINT ck_advisory_phase1_r4_observation_union CHECK (", 1
    )[1].split("\n    );", 1)[0]
    formal_observation_branch = observation_union.split("\n        OR\n", 1)[0]
    assert "AND range_run_id IS NULL" in formal_observation_branch
    assert "AND range_day_run_id IS NULL" in formal_observation_branch
    assert "verify_advisory_phase1_r4_lineage_payload_union" in sql
    assert "trg_verify_advisory_phase1_r4_lineage_payload_union" in sql
    assert "ADVISORY_PHASE1_R4_LINEAGE_PAYLOAD_IDENTITY_MISMATCH" in sql
    assert "operation_type = 'CREATE' AND result_ref->>'artifact_kind' = 'SOURCE_REQUIREMENT_PLAN'" in sql
    assert "operation_type = 'BUILD_SOURCE_CATALOG'" in sql
    assert "AND result_ref->>'artifact_kind' = 'SOURCE_CATALOG_CHECKPOINT'" in sql
    assert "WHEN 'CREATE' THEN 'SOURCE_REQUIREMENT_PLAN'" in sql
    assert "WHEN 'BUILD_SOURCE_CATALOG' THEN 'SOURCE_CATALOG_CHECKPOINT'" in sql
    assert "receipt_kind IS DISTINCT FROM expected_receipt_kind" in sql
    assert "AND NEW.outcome_contract_version = predecessor.outcome_contract_version" in sql
    assert "(phase0a_audit_id IS NULL) = (admission_scope_id IS NULL)" not in sql


def test_r4_migration_is_forward_only_and_adds_no_operational_gate() -> None:
    sql = MIGRATION.read_text(encoding="utf-8").upper()
    assert "DROP TABLE" not in sql
    assert "TRUNCATE " not in sql
    assert "DELETE FROM" not in sql
    assert "CREATE ROLE" not in sql
    assert "GRANT " not in sql
    assert "REVOKE " not in sql
    for forbidden in (
        "APPROVAL",
        "RBAC",
        "CANARY",
        "CHAMPION",
        "PACKAGE_HEALTH",
        "ASSET_VALIDATOR",
        "MODEL_RETEST",
    ):
        assert forbidden not in sql
