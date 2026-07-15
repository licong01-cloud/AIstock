"""Pure Phase 1F release-schema contract tests; no database connection is used."""

from __future__ import annotations

import json
import hashlib
import tempfile
from datetime import date
from pathlib import Path

import pytest

from backend.services.advisory_phase1.release_schema_contract import (
    CatalogDifference,
    DdlSessionPolicy,
    ManagedSchemaStatus,
    PrerequisiteStatus,
    ReleaseSchemaContract,
    RequestedOperation,
    TargetLabel,
    canonical_json_sha256,
    load_release_schema_contract,
    load_predecessor_release_schema_contract,
    make_release_plan_request,
    normalize_sql,
    plan_month_partitions,
    plan_month_partitions_for_contracts,
)
from backend.services.advisory_phase1.release_schema_receipt_store import (
    REASON_RECEIPT_COLLISION,
    ReleaseSchemaReceiptStore,
    ReleaseSchemaReceiptStoreError,
)
from backend.services.advisory_phase1.release_schema_verify_postgres import (
    CatalogVerification,
    REASON_ENV_CONFIG_MISSING,
    ReleaseSchemaVerificationError,
    _compare_partitions,
    _compare_specs,
    resolve_database_connection,
    expected_managed_catalog_evidence,
)
from backend.services.advisory_phase1.release_schema_apply_postgres import _pending_operations
from scripts.advisory_phase1_release_schema import EXIT_DDL, EXIT_POST_VERIFY_STORE, _exit_for_reason, _parser


def test_registry_is_full_frozen_contract_with_phase1f2_scope_identity_step() -> None:
    contract = load_release_schema_contract()
    assert contract.schema_version == "advisory_phase1f_release_schema_contract_v2"
    assert contract.release_schema_version == "advisory_phase1_dataset_foundation_v3"
    assert len(contract.required_relations) == 35
    assert len(contract.required_columns) == 637
    assert len(contract.required_constraints) == 274
    assert len(contract.required_indexes) == 102
    assert len(contract.required_functions) == 33
    assert len(contract.required_triggers) == 63
    assert len(contract.required_comments) == 151
    assert [item.order for item in contract.managed_migrations] == [10, 20, 30, 40, 50, 55, 60, 70, 80, 90]
    assert [item.transaction_mode.value for item in contract.managed_migrations] == [
        "EXECUTOR_MANAGED",
        "EXECUTOR_MANAGED",
        "FILE_WRAPPED",
        "FILE_WRAPPED",
        "EXECUTOR_MANAGED",
        "EXECUTOR_MANAGED",
        "FILE_WRAPPED",
        "EXECUTOR_MANAGED",
        "EXECUTOR_MANAGED",
        "EXECUTOR_MANAGED",
    ]
    assert [item.executor_action.value for item in contract.managed_migrations] == [
        "SQL_FILE",
        "SQL_FILE",
        "SQL_FILE",
        "SQL_FILE",
        "SQL_FILE",
        "SQL_FILE",
        "SQL_FILE",
        "CREATE_PARTITIONS",
        "CUTOVER",
        "SQL_FILE",
    ]
    assert [item.parent_relation for item in contract.partition_contracts] == [
        "advisory_outcome_label_payload",
        "advisory_signal_observation_lineage_payload",
        "advisory_signal_stage_candidate_payload",
    ]
    assert all(item.declared_object_ids for item in contract.managed_migrations)
    declared_objects = {
        object_id for migration in contract.managed_migrations for object_id in migration.declared_object_ids
    }
    assert declared_objects == contract.object_ids()
    assert [(item.object_id, item.repairable_by_orders) for item in contract.repairable_drift_variants] == [
        ("comment:app.advisory_capture_gap.__table__", (90,)),
        ("comment:app.advisory_selection_stage_trace_outbox.__table__", (90,)),
        ("constraint:app.advisory_source_revision_member.advisory_source_revision_member_check2", (55,)),
        ("function:app.verify_advisory_source_revision_member_event()", (20,)),
    ]
    assert contract.predecessor_contract is not None
    assert contract.predecessor_contract.exact_relations == (
        "app.advisory_capture_gap",
        "app.advisory_selection_stage_trace_outbox",
    )
    predecessor = load_predecessor_release_schema_contract(contract)
    assert predecessor is not None
    assert predecessor.release_schema_version == "advisory_phase1_dataset_foundation_v2"
    assert predecessor.contract_content_hash == contract.predecessor_contract.contract_content_hash
    predecessor_v1 = load_predecessor_release_schema_contract(predecessor)
    assert predecessor_v1 is not None
    assert predecessor_v1.release_schema_version == "advisory_phase1_dataset_foundation_v1"
    for migration in contract.managed_migrations:
        if migration.executor_action.value == "CREATE_PARTITIONS":
            assert migration.relative_path is None
            assert migration.file_sha256 is None
            assert migration.partition_parent_relations == (
                "app.advisory_outcome_label_payload",
                "app.advisory_signal_observation_lineage_payload",
                "app.advisory_signal_stage_candidate_payload",
            )
        else:
            assert migration.relative_path is not None
            assert migration.file_sha256 is not None
            source = Path(migration.relative_path)
            assert source.is_file()
            assert hashlib.sha256(source.read_bytes()).hexdigest() == migration.file_sha256
    assert contract.ddl_session_policy == DdlSessionPolicy(
        lock_timeout_ms=10_000,
        statement_timeout_ms=900_000,
        automatic_retry=False,
    )

    order_90 = contract.managed_migrations[-1]
    assert order_90.depends_on_orders == (80,)
    assert order_90.transaction_group == "trace_identity_scope"
    assert order_90.declared_object_ids == (
        "column:app.advisory_capture_gap.admission_scope_hash",
        "column:app.advisory_capture_gap.admission_scope_id",
        "comment:app.advisory_capture_gap.__table__",
        "comment:app.advisory_capture_gap.admission_scope_hash",
        "comment:app.advisory_capture_gap.admission_scope_id",
        "comment:app.advisory_selection_stage_trace_outbox.__table__",
        "constraint:app.advisory_capture_gap.ck_advisory_capture_gap_scope_pair",
        "constraint:app.advisory_selection_stage_trace_outbox.uq_advisory_stage_trace_outbox_scope_identity",
        "index:app.advisory_capture_gap.ux_advisory_capture_gap_legacy_identity",
        "index:app.advisory_capture_gap.ux_advisory_capture_gap_scope_v2_identity",
        "index:app.advisory_selection_stage_trace_outbox.uq_advisory_stage_trace_outbox_scope_identity",
    )
    assert all(
        [migration.order for migration in contract.managed_migrations if object_id in migration.declared_object_ids]
        == [90]
        for object_id in order_90.declared_object_ids
    )
    old_objects = {
        "constraint:app.advisory_capture_gap.advisory_capture_gap_selection_run_id_package_id_manifest_s_key",
        "constraint:app.advisory_selection_stage_trace_outbox.advisory_selection_stage_trac_selection_run_id_package_id_m_key",
        "index:app.advisory_capture_gap.advisory_capture_gap_selection_run_id_package_id_manifest_s_key",
        "index:app.advisory_selection_stage_trace_outbox.advisory_selection_stage_trac_selection_run_id_package_id_m_key",
    }
    assert old_objects.isdisjoint(contract.object_ids())
    assert {
        item.object_id for item in contract.repairable_unexpected_objects if item.repairable_by_orders == (90,)
    } == old_objects


def test_registry_hash_rejects_any_mutated_object_contract() -> None:
    path = Path("backend/services/advisory_phase1/release_schema_registry/advisory_phase1_dataset_foundation_v3.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["required_columns"][0]["data_type"] = "integer"
    with pytest.raises(Exception, match="contract_content_hash"):
        ReleaseSchemaContract.model_validate(payload)


def test_phase1f2_predecessor_contract_rejects_path_hash_version_and_relation_tamper() -> None:
    path = Path("backend/services/advisory_phase1/release_schema_registry/advisory_phase1_dataset_foundation_v3.json")

    def contract_with(mutator):  # type: ignore[no-untyped-def]
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.pop("contract_content_hash")
        mutator(payload)
        payload["contract_content_hash"] = canonical_json_sha256(payload)
        return ReleaseSchemaContract.model_validate(payload)

    with pytest.raises(Exception, match="one registry JSON filename"):
        contract_with(lambda payload: payload["predecessor_contract"].update(relative_path="../escape.json"))

    wrong_hash = contract_with(
        lambda payload: payload["predecessor_contract"].update(contract_content_hash="0" * 64)
    )
    with pytest.raises(Exception, match="content hash differs"):
        load_predecessor_release_schema_contract(wrong_hash)

    same_version = contract_with(
        lambda payload: payload.update(release_schema_version="advisory_phase1_dataset_foundation_v2")
    )
    with pytest.raises(Exception, match="release schema version must differ"):
        load_predecessor_release_schema_contract(same_version)

    missing_relation = contract_with(
        lambda payload: payload["predecessor_contract"].update(exact_relations=["app.not_present_in_v2"])
    )
    with pytest.raises(Exception, match="exact relation scope is not present"):
        load_predecessor_release_schema_contract(missing_relation)


def test_historical_v1_contract_remains_parseable_but_cannot_declare_v2_executor_semantics() -> None:
    path = Path("backend/services/advisory_phase1/release_schema_registry/advisory_phase1_dataset_foundation_v1.json")
    contract = load_release_schema_contract(path)
    assert contract.release_schema_version == "advisory_phase1_dataset_foundation_v1"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["managed_migrations"][0]["executor_action"] = "CUTOVER"
    with pytest.raises(Exception, match="v1 contract cannot declare typed executor actions"):
        ReleaseSchemaContract.model_validate(payload)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["predecessor_contract"] = {
        "relative_path": path.name,
        "contract_content_hash": contract.contract_content_hash,
        "exact_relations": ["app.advisory_signal_observation_lineage"],
    }
    with pytest.raises(Exception, match="v1 contract cannot declare a predecessor contract"):
        ReleaseSchemaContract.model_validate(payload)


def test_external_calendar_prerequisite_is_exact_and_is_not_a_managed_relation() -> None:
    contract = load_release_schema_contract()
    assert tuple(contract.phase0a_prerequisite_relations) == ()
    assert len(contract.external_readonly_prerequisite_relations) == 1
    calendar = contract.external_readonly_prerequisite_relations[0]
    assert (calendar.schema, calendar.name, calendar.relkind) == ("market", "trading_calendar", "r")
    assert [(item.name, item.data_type, item.nullable) for item in calendar.columns] == [
        ("cal_date", "date", False),
        ("is_trading", "boolean", False),
    ]
    assert all(item.schema == "app" for item in contract.required_relations)
    capture_migration = Path("backend/db/migrations/add_advisory_phase1_capture_foundation_20260713.sql")
    assert "market.trading_calendar" in capture_migration.read_text(encoding="utf-8")


def test_observer_migration_closure_is_present_in_registry_before_apply() -> None:
    contract = load_release_schema_contract()
    observer_relations = {
        item.name
        for item in contract.required_relations
        if item.name.startswith("advisory_source_observer") or item.name == "advisory_source_observation_receipt"
    }
    assert observer_relations == {"advisory_source_observer_cursor", "advisory_source_observation_receipt"}
    assert len([item for item in contract.required_columns if item.relation in observer_relations]) == 31
    assert len([item for item in contract.required_constraints if item.relation in observer_relations]) == 19
    assert len([item for item in contract.required_indexes if item.relation in observer_relations]) == 6
    assert (
        len(
            [
                item
                for item in contract.required_functions
                if item.name.endswith("observer_cursor_update") or "observation_receipt" in item.name
            ]
        )
        == 3
    )
    assert len([item for item in contract.required_triggers if item.relation in observer_relations]) == 3
    assert len([item for item in contract.required_comments if item.relation in observer_relations]) == 3


def test_month_partition_plan_is_inclusive_across_calendar_year() -> None:
    contract = load_release_schema_contract()
    partitions = plan_month_partitions(
        partition_contract=contract.partition_contract,
        history_start_trade_date=date(2025, 12, 31),
        history_end_trade_date=date(2026, 2, 1),
    )
    assert [(item.name, item.lower_bound, item.upper_bound) for item in partitions] == [
        ("advisory_outcome_label_payload_202512", date(2025, 12, 1), date(2026, 1, 1)),
        ("advisory_outcome_label_payload_202601", date(2026, 1, 1), date(2026, 2, 1)),
        ("advisory_outcome_label_payload_202602", date(2026, 2, 1), date(2026, 3, 1)),
    ]


def test_phase1f1_partition_plan_expands_each_declared_parent_without_name_collisions() -> None:
    contract = load_release_schema_contract()
    partitions = plan_month_partitions_for_contracts(
        partition_contracts=contract.partition_contracts,
        target_months=(date(2026, 6, 1), date(2026, 7, 1)),
    )
    assert len(partitions) == 6
    assert {(item.parent_relation, item.name) for item in partitions} == {
        ("advisory_outcome_label_payload", "advisory_outcome_label_payload_202606"),
        ("advisory_outcome_label_payload", "advisory_outcome_label_payload_202607"),
        ("advisory_signal_observation_lineage_payload", "advisory_signal_observation_lineage_payload_202606"),
        ("advisory_signal_observation_lineage_payload", "advisory_signal_observation_lineage_payload_202607"),
        ("advisory_signal_stage_candidate_payload", "advisory_signal_stage_candidate_payload_202606"),
        ("advisory_signal_stage_candidate_payload", "advisory_signal_stage_candidate_payload_202607"),
    }


def test_release_plan_request_is_hash_bound_to_contract_and_does_not_require_phase1e_plan() -> None:
    contract = load_release_schema_contract()
    request = make_release_plan_request(
        contract=contract,
        target_label=TargetLabel.DEV,
        history_start_trade_date=date(2026, 6, 1),
        history_end_trade_date=date(2026, 8, 31),
        capacity_request_hash="1" * 64,
        capacity_receipt_hash=None,
        phase1e_plan_hashes=(),
        requested_operation=RequestedOperation.APPLY,
    )
    assert request.phase1e_plan_hashes == ()
    assert request.request_content_hash == canonical_json_sha256(request.canonical_payload())
    assert request.target_label is TargetLabel.DEV
    multi = make_release_plan_request(
        contract=contract,
        target_label=TargetLabel.DEV,
        history_start_trade_date=date(2026, 6, 1),
        history_end_trade_date=date(2026, 8, 31),
        capacity_request_hash="1" * 64,
        capacity_receipt_hash=None,
        phase1e_plan_hashes=("b" * 64, "a" * 64, "b" * 64),
        requested_operation=RequestedOperation.APPLY,
    )
    assert multi.phase1e_plan_hashes == ("a" * 64, "b" * 64)


def test_env_resolution_requires_exact_target_key_set_without_fallback(tmp_path: Path) -> None:
    env_file = tmp_path / "release.env"
    env_file.write_text(
        "\n".join(
            (
                "TDX_DB_DEV_HOST=127.0.0.1",
                "TDX_DB_DEV_PORT=5433",
                "TDX_DB_DEV_NAME=aistock_dev",
                "TDX_DB_DEV_USER=tester",
                "TDX_DB_DEV_PASSWORD=secret",
            )
        ),
        encoding="utf-8",
    )
    resolved = resolve_database_connection(target_label=TargetLabel.DEV, env_file=env_file)
    assert resolved.target_label is TargetLabel.DEV
    with pytest.raises(ReleaseSchemaVerificationError) as error:
        resolve_database_connection(target_label=TargetLabel.PRODUCTION, env_file=env_file)
    assert error.value.reason_code == REASON_ENV_CONFIG_MISSING


def test_sql_normalizer_preserves_quoted_and_dollar_quoted_semantics() -> None:
    doubled = normalize_sql("CHECK (value = 'A  B')")
    single = normalize_sql("CHECK (value = 'A B')")
    assert doubled != single
    assert doubled == "CHECK (value = 'A  B')"
    assert normalize_sql('CHECK (value = "A  B")') == 'CHECK (value = "A  B")'
    assert normalize_sql("AS $body$ BEGIN  RETURN 'A  B'; END $body$") == "AS $body$ BEGIN  RETURN 'A  B'; END $body$"


def test_catalog_fingerprint_evidence_has_total_count_and_every_kind_hash() -> None:
    contract = load_release_schema_contract()
    partitions = plan_month_partitions_for_contracts(
        partition_contracts=contract.partition_contracts,
        target_months=(date(2026, 6, 1), date(2026, 7, 1), date(2026, 8, 1)),
    )
    evidence = expected_managed_catalog_evidence(contract=contract, expected_partitions=partitions)
    assert evidence.object_count == sum(evidence.per_kind_counts.values())
    assert evidence.per_kind_counts["relations"] == 35
    assert evidence.per_kind_counts["partitions"] == 9
    assert set(evidence.per_kind_hashes) == set(evidence.per_kind_counts)


def test_cli_operation_lineage_and_error_exit_contract_are_explicit() -> None:
    args = _parser().parse_args(
        [
            "plan",
            "--db-target",
            "dev",
            "--env-file",
            "release.env",
            "--receipt-root",
            "receipts",
            "--capacity-request",
            "capacity.json",
            "--requested-operation",
            "verify",
        ]
    )
    assert args.requested_operation == "verify"
    assert _exit_for_reason("PHASE1F_TRANSACTION_VERIFY_FAILED") == EXIT_DDL
    assert _exit_for_reason("PHASE1F_POST_COMMIT_VERIFY_FAILED") == EXIT_POST_VERIFY_STORE
    assert _exit_for_reason("ADVISORY_PHASE1F1_COPY_MISMATCH") == EXIT_DDL
    assert _exit_for_reason("ADVISORY_PHASE1F1_POST_COMMIT_VERIFY_FAILED") == EXIT_POST_VERIFY_STORE


def test_receipt_store_is_atomic_idempotent_and_rejects_same_identity_different_content() -> None:
    with tempfile.TemporaryDirectory(prefix="aistock_phase1f_receipt_") as directory:
        store = ReleaseSchemaReceiptStore(Path(directory))
        identity = "a" * 64
        payload = {"schema_version": "test", "plan_content_hash": identity, "value": 1}
        first = store.write_plan(identity=identity, payload=payload)
        second = store.write_plan(identity=identity, payload=payload)
        assert not first.idempotent and second.idempotent
        with pytest.raises(ReleaseSchemaReceiptStoreError) as error:
            store.write_plan(
                identity=identity, payload={"schema_version": "test", "plan_content_hash": identity, "value": 2}
            )
        assert error.value.reason_code == REASON_RECEIPT_COLLISION


def test_receipt_store_refuses_a_repository_root() -> None:
    with pytest.raises(ReleaseSchemaReceiptStoreError):
        ReleaseSchemaReceiptStore(Path("."))


def test_status_axes_keep_prerequisite_separate_from_managed_schema() -> None:
    verification = CatalogVerification(
        projection=None,  # type: ignore[arg-type] - this test exercises status derivation only.
        managed_schema_status=ManagedSchemaStatus.COMPATIBLE,
        prerequisite_status=PrerequisiteStatus.MISSING,
        managed_differences=(),
        prerequisite_differences=(),
    )
    assert not verification.downstream_ready


def test_missing_additive_managed_object_is_planned_even_when_prerequisite_is_missing() -> None:
    contract = load_release_schema_contract()
    partition = plan_month_partitions(
        partition_contract=contract.partition_contract,
        history_start_trade_date=date(2026, 6, 1),
        history_end_trade_date=date(2026, 6, 1),
    )[0]
    verification = CatalogVerification(
        projection=None,  # type: ignore[arg-type] - only differences are relevant here.
        managed_schema_status=ManagedSchemaStatus.PARTIAL_ADDITIVE,
        prerequisite_status=PrerequisiteStatus.MISSING,
        managed_differences=(
            CatalogDifference(
                object_id="relation:app.advisory_source_observer_cursor",
                category="MISSING",
                reason_code="PHASE1F_MANAGED_SCHEMA_MISSING",
                repairable_by_orders=(50,),
            ),
        ),
        prerequisite_differences=(
            CatalogDifference(
                object_id="prerequisite_relation:market.trading_calendar",
                category="MISSING",
                reason_code="PHASE1F_PREREQUISITE_SCHEMA_MISSING",
            ),
        ),
    )
    operations = _pending_operations(contract=contract, verification=verification, expected_partitions=(partition,))
    assert [(item.kind, item.migration_order) for item in operations] == [("MIGRATION", 50)]


def test_catalog_drift_is_explicit_for_wrong_column_trigger_and_partition_semantics() -> None:
    contract = load_release_schema_contract()
    column = contract.required_columns[0]
    wrong_column = column.model_dump(mode="python", exclude={"repairable_by_orders"})
    wrong_column["data_type"] = "integer"
    column_differences = _compare_specs(
        expected=(column,),
        actual=(wrong_column,),
        fields=("schema", "relation", "name"),
        kind="column",
        reason_missing="MISSING",
        reason_drifted="DRIFTED",
        include_unexpected=True,
    )
    trigger = contract.required_triggers[0]
    disabled_trigger = trigger.model_dump(mode="python", exclude={"repairable_by_orders"})
    disabled_trigger["enabled"] = "D"
    trigger_differences = _compare_specs(
        expected=(trigger,),
        actual=(disabled_trigger,),
        fields=("schema", "relation", "name"),
        kind="trigger",
        reason_missing="MISSING",
        reason_drifted="DRIFTED",
        include_unexpected=True,
    )
    partition = plan_month_partitions(
        partition_contract=contract.partition_contract,
        history_start_trade_date=date(2026, 6, 1),
        history_end_trade_date=date(2026, 6, 1),
    )[0]
    partition_differences = _compare_partitions(
        contract=contract,
        expected_partitions=(partition,),
        actual=(
            {
                "parent_schema": "app",
                "parent_relation": "advisory_outcome_label_payload",
                "schema": "app",
                "name": partition.name,
                "partition_bound": "FOR VALUES FROM ('2026-06-02') TO ('2026-07-01')",
            },
        ),
    )
    assert [item.category for item in column_differences] == ["DRIFTED"]
    assert [item.category for item in trigger_differences] == ["DRIFTED"]
    assert [item.category for item in partition_differences] == ["DRIFTED"]
