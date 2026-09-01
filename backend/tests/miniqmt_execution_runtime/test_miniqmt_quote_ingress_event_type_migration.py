from __future__ import annotations

import pytest

from backend.services.miniqmt_execution_runtime.quote_event_schema import (
    EXPECTED_MIGRATION_FILE_SHA256,
    EXPECTED_PREFLIGHT_FILE_SHA256,
    EXPECTED_ROLLBACK_FILE_SHA256,
    NEW_QUOTE_EVENT_TYPES,
    OLD_EVENT_SOURCES,
    OLD_EVENT_TYPES,
    P1D_EVENT_SOURCES,
    P1D_EVENT_TYPES,
    TARGET_EVENT_SOURCES,
    TARGET_EVENT_TYPES,
    QuoteEventSchemaPreflightError,
    assert_runtime_registry_matches_target,
    classify_allowlist,
    migration_path,
    preflight_path,
    rollback_path,
)


def test_preflight_accepts_only_exact_old_or_target_constraints() -> None:
    assert_runtime_registry_matches_target()
    assert classify_allowlist(OLD_EVENT_TYPES, kind="event_type") == "old"
    assert classify_allowlist(P1D_EVENT_TYPES, kind="event_type") == "predecessor"
    assert classify_allowlist(TARGET_EVENT_TYPES, kind="event_type") == "target"
    assert classify_allowlist(OLD_EVENT_SOURCES, kind="source") == "old"
    assert classify_allowlist(P1D_EVENT_SOURCES, kind="source") == "predecessor"
    assert classify_allowlist(TARGET_EVENT_SOURCES, kind="source") == "target"
    with pytest.raises(QuoteEventSchemaPreflightError):
        classify_allowlist(TARGET_EVENT_TYPES | {"UNREGISTERED_EVENT"}, kind="event_type")
    with pytest.raises(QuoteEventSchemaPreflightError):
        classify_allowlist(TARGET_EVENT_SOURCES - {"quote_ingress"} | {"unregistered"}, kind="source")


def test_apply_and_second_apply_are_idempotent_with_exact_readback() -> None:
    sql = migration_path().read_text(encoding="utf-8")
    assert "BEGIN;" in sql and "COMMIT;" in sql
    assert "SHARE ROW EXCLUSIVE" in sql
    assert "convalidated" in sql and "VALIDATE CONSTRAINT" in sql
    assert "exact successor event contract is already installed" in sql
    assert "pg_get_constraintdef" in sql
    for event_type in TARGET_EVENT_TYPES:
        assert event_type in sql
    for source in TARGET_EVENT_SOURCES:
        assert source in sql
    assert "ck_miniqmt_k2_event_composite" in sql
    assert "miniqmt_algo_start_v2" in sql and "miniqmt_command_outcome_v1" in sql
    assert ") IS TRUE) NOT VALID" in sql
    assert "post-commit exact constraint readback drift" in sql
    assert "independent_catalog_sha256" in sql
    assert "CREATE TABLE" not in sql and "CREATE INDEX" not in sql and "INSERT INTO" not in sql


def test_migration_artifact_set_has_frozen_canonical_lf_identity() -> None:
    def canonical_sha(path):  # type: ignore[no-untyped-def]
        text = path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
        import hashlib

        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    assert canonical_sha(migration_path()) == EXPECTED_MIGRATION_FILE_SHA256
    assert canonical_sha(preflight_path()) == EXPECTED_PREFLIGHT_FILE_SHA256
    assert canonical_sha(rollback_path()) == EXPECTED_ROLLBACK_FILE_SHA256


def test_rollback_and_second_rollback_are_idempotent() -> None:
    sql = rollback_path().read_text(encoding="utf-8")
    assert "convalidated" in sql and "VALIDATE CONSTRAINT" in sql
    assert "immediate P1-D/K2 predecessor contract" in sql
    assert "rollback post-DDL exact constraint readback drift" in sql
    assert "rollback post-commit exact constraint readback drift" in sql
    assert "DELETE FROM" not in sql and "UPDATE qmt_strategy.execution_runtime_event" not in sql


def test_rollback_refuses_while_new_type_or_source_rows_exist() -> None:
    sql = rollback_path().read_text(encoding="utf-8")
    assert "rollback refused" in sql
    assert "target_only_fact_count" in sql
    assert "KERNEL_V2" in sql
    for event_type in NEW_QUOTE_EVENT_TYPES:
        assert event_type in sql
