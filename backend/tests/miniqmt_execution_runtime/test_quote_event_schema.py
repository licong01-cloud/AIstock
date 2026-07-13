from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.services.miniqmt_execution_runtime.quote_event_schema import (
    OLD_EVENT_SOURCES,
    OLD_EVENT_TYPES,
    TARGET_EVENT_SOURCES,
    TARGET_EVENT_TYPES,
    QuoteEventSchemaPreflightError,
    allowed_literals_from_constraint,
    read_quote_event_schema,
)


def _definition(column: str, values: frozenset[str]) -> str:
    literals = ", ".join(f"'{value}'" for value in sorted(values))
    return f"CHECK ({column} IN ({literals}))"


class _Cursor:
    def __init__(self) -> None:
        self.sql = ""

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, sql: str, _params: object | None = None) -> None:
        self.sql = sql

    def fetchone(self):  # type: ignore[no-untyped-def]
        if "current_database()" in self.sql:
            return ("aistock_dev", "tester", "127.0.0.1", 5432, datetime(2026, 7, 12, tzinfo=UTC))
        if "to_regclass" in self.sql:
            return ("qmt_strategy.execution_runtime_event",)
        if "COUNT(*)" in self.sql:
            return (3, 2, 1, 2, 1, 0)
        if "::regclass::oid" in self.sql:
            return (101,)
        raise AssertionError(self.sql)

    def fetchall(self):  # type: ignore[no-untyped-def]
        if "FROM pg_constraint" not in self.sql:
            raise AssertionError(self.sql)
        return [
            (11, "ck_miniqmt_event_source", True, _definition("source", TARGET_EVENT_SOURCES)),
            (10, "ck_miniqmt_event_type", True, _definition("event_type", TARGET_EVENT_TYPES)),
        ]


class _Connection:
    def cursor(self) -> _Cursor:
        return _Cursor()


def test_read_only_schema_readback_reports_exact_target_and_file_hashes() -> None:
    receipt = read_quote_event_schema(_Connection())
    assert receipt.state == "target"
    assert receipt.production_ddl_gate == "applied_and_verified"
    assert receipt.table_oid == 101
    assert receipt.database_identity == "aistock_dev|tester|127.0.0.1:5432"
    assert receipt.queried_at_utc == datetime(2026, 7, 12, tzinfo=UTC)
    assert receipt.old_event_type_row_count == 2
    assert receipt.new_event_type_row_count == 1
    assert receipt.event_type.allowed_values == TARGET_EVENT_TYPES
    assert receipt.event_source.allowed_values == TARGET_EVENT_SOURCES
    assert len(receipt.migration_file_sha256) == 64


def test_constraint_literal_parser_rejects_expression_drift_and_classifies_old_schema() -> None:
    assert allowed_literals_from_constraint(_definition("event_type", OLD_EVENT_TYPES), column="event_type") == OLD_EVENT_TYPES
    assert allowed_literals_from_constraint(_definition("source", OLD_EVENT_SOURCES), column="source") == OLD_EVENT_SOURCES
    with pytest.raises(QuoteEventSchemaPreflightError):
        allowed_literals_from_constraint("CHECK (event_type IN ('TICK') OR source = 'runtime')", column="event_type")
