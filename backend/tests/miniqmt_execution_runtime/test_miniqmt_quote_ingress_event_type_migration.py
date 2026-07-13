from __future__ import annotations

import pytest

from backend.services.miniqmt_execution_runtime.quote_event_schema import (
    NEW_QUOTE_EVENT_TYPES,
    OLD_EVENT_SOURCES,
    OLD_EVENT_TYPES,
    TARGET_EVENT_SOURCES,
    TARGET_EVENT_TYPES,
    QuoteEventSchemaPreflightError,
    assert_runtime_registry_matches_target,
    classify_allowlist,
    migration_path,
    rollback_path,
)


def test_preflight_accepts_only_exact_old_or_target_constraints() -> None:
    assert_runtime_registry_matches_target()
    assert classify_allowlist(OLD_EVENT_TYPES, kind="event_type") == "old"
    assert classify_allowlist(TARGET_EVENT_TYPES, kind="event_type") == "target"
    assert classify_allowlist(OLD_EVENT_SOURCES, kind="source") == "old"
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
    assert "exact target CHECK allowlists already present" in sql
    assert "pg_get_constraintdef" in sql
    for event_type in NEW_QUOTE_EVENT_TYPES:
        assert event_type in sql
    assert "quote_ingress" in sql
    assert "CREATE TABLE" not in sql and "CREATE INDEX" not in sql and "INSERT INTO" not in sql


def test_rollback_and_second_rollback_are_idempotent() -> None:
    sql = rollback_path().read_text(encoding="utf-8")
    assert "convalidated" in sql and "VALIDATE CONSTRAINT" in sql
    assert "exact old CHECK allowlists already present" in sql
    assert "post-DDL CHECK readback does not match exact old allowlists" in sql
    assert "DELETE FROM" not in sql and "UPDATE qmt_strategy.execution_runtime_event" not in sql


def test_rollback_refuses_while_new_type_or_source_rows_exist() -> None:
    sql = rollback_path().read_text(encoding="utf-8")
    assert "rollback refused" in sql
    assert "new_type_count" in sql and "new_source_count" in sql
    assert "min_sequence" in sql and "max_sequence" in sql
    for event_type in NEW_QUOTE_EVENT_TYPES:
        assert event_type in sql
