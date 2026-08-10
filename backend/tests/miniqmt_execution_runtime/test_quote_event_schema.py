from __future__ import annotations

from datetime import UTC, datetime

import pytest

from backend.services.miniqmt_execution_runtime.quote_event_schema import (
    KERNEL_V2_EVENT_SOURCES,
    KERNEL_V2_EVENT_TYPES,
    EVENT_ROUTING_COMPOSITES_V1,
    LEGACY_KERNEL_V2_EVENT_COMPOSITES,
    OLD_EVENT_SOURCES,
    OLD_EVENT_TYPES,
    QUOTE_V1_EVENT_SOURCES,
    QUOTE_V1_EVENT_TYPES,
    TARGET_EVENT_SOURCES,
    TARGET_EVENT_TYPES,
    QuoteEventSchemaPreflightError,
    allowed_literals_from_constraint,
    classify_allowlist,
    classify_event_composites,
    event_composites_from_constraint,
    read_quote_event_schema,
)


def _definition(column: str, values: frozenset[str]) -> str:
    literals = ", ".join(f"'{value}'" for value in sorted(values))
    return f"CHECK ({column} IN ({literals}))"


def _composite_definition(composites: frozenset[tuple[str, str, str]] = EVENT_ROUTING_COMPOSITES_V1) -> str:
    branches = ["event_contract_version = 'LEGACY_V1'"]
    branches.extend(
        f"(event_type = '{event_type}' AND source = '{source}' AND payload_schema_version = '{schema}')"
        for event_type, source, schema in sorted(composites)
    )
    return "CHECK (" + " OR ".join(branches) + ")"


class _Cursor:
    def __init__(
        self,
        *,
        event_types: frozenset[str] = TARGET_EVENT_TYPES,
        event_sources: frozenset[str] = TARGET_EVENT_SOURCES,
        composites: frozenset[tuple[str, str, str]] = EVENT_ROUTING_COMPOSITES_V1,
    ) -> None:
        self.sql = ""
        self.event_types = event_types
        self.event_sources = event_sources
        self.composites = composites

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
            return (3, 2, 1, 2, 1, 0, 0)
        if "::regclass::oid" in self.sql:
            return (101,)
        raise AssertionError(self.sql)

    def fetchall(self):  # type: ignore[no-untyped-def]
        if "FROM pg_constraint" not in self.sql:
            raise AssertionError(self.sql)
        return [
            (11, "ck_miniqmt_event_source", True, _definition("source", self.event_sources)),
            (10, "ck_miniqmt_event_type", True, _definition("event_type", self.event_types)),
            (12, "ck_miniqmt_k2_event_composite", True, _composite_definition(self.composites)),
        ]


class _Connection:
    def __init__(
        self,
        *,
        event_types: frozenset[str] = TARGET_EVENT_TYPES,
        event_sources: frozenset[str] = TARGET_EVENT_SOURCES,
        composites: frozenset[tuple[str, str, str]] = EVENT_ROUTING_COMPOSITES_V1,
    ) -> None:
        self.event_types = event_types
        self.event_sources = event_sources
        self.composites = composites

    def cursor(self) -> _Cursor:
        return _Cursor(
            event_types=self.event_types,
            event_sources=self.event_sources,
            composites=self.composites,
        )


def test_read_only_schema_readback_reports_exact_target_and_file_hashes() -> None:
    receipt = read_quote_event_schema(_Connection())
    assert receipt.schema_version == "miniqmt_event_schema_readback_v2"
    assert receipt.state == "target"
    assert receipt.production_ddl_gate == "applied_and_verified"
    assert receipt.table_oid == 101
    assert receipt.database_identity == "aistock_dev|tester|127.0.0.1:5432"
    assert receipt.queried_at_utc == datetime(2026, 7, 12, tzinfo=UTC)
    assert receipt.old_event_type_row_count == 2
    assert receipt.new_event_type_row_count == 1
    assert receipt.kernel_v2_contract_row_count == 0
    assert receipt.event_type.allowed_values == TARGET_EVENT_TYPES
    assert receipt.event_source.allowed_values == TARGET_EVENT_SOURCES
    assert receipt.kernel_v2_composite.allowed_composites == EVENT_ROUTING_COMPOSITES_V1
    assert len(receipt.migration_file_sha256) == 64
    assert len(receipt.kernel_v2_migration_file_sha256) == 64


def test_p1d_only_allowlists_are_no_longer_reported_as_applied_and_verified() -> None:
    receipt = read_quote_event_schema(
        _Connection(
            event_types=QUOTE_V1_EVENT_TYPES,
            event_sources=QUOTE_V1_EVENT_SOURCES,
            composites=LEGACY_KERNEL_V2_EVENT_COMPOSITES,
        )
    )
    assert receipt.state == "quote_v1"
    assert receipt.production_ddl_gate == "pending"
    assert KERNEL_V2_EVENT_TYPES - receipt.event_type.allowed_values
    assert KERNEL_V2_EVENT_SOURCES - receipt.event_source.allowed_values


def test_composite_readback_classifies_exact_legacy_and_rejects_other_drift() -> None:
    assert (
        classify_event_composites(
            event_composites_from_constraint(_composite_definition(LEGACY_KERNEL_V2_EVENT_COMPOSITES))
        )
        == "legacy"
    )
    arbitrary_missing = EVENT_ROUTING_COMPOSITES_V1 - {
        ("SESSION", "EXCHANGE_SESSION_CLOCK", "miniqmt_session_event_v1")
    }
    with pytest.raises(QuoteEventSchemaPreflightError, match="not exact legacy/target"):
        event_composites_from_constraint(_composite_definition(arbitrary_missing))
    with pytest.raises(QuoteEventSchemaPreflightError, match="structure drift"):
        event_composites_from_constraint(
            _composite_definition().replace(
                "payload_schema_version = 'miniqmt_timer_due_v1'",
                "payload_schema_version = 'miniqmt_timer_due_v1' AND archived_at IS NULL",
            )
        )


def test_constraint_literal_parser_rejects_expression_drift_and_classifies_old_schema() -> None:
    assert (
        allowed_literals_from_constraint(_definition("event_type", OLD_EVENT_TYPES), column="event_type")
        == OLD_EVENT_TYPES
    )
    assert (
        allowed_literals_from_constraint(_definition("source", OLD_EVENT_SOURCES), column="source") == OLD_EVENT_SOURCES
    )
    with pytest.raises(QuoteEventSchemaPreflightError):
        allowed_literals_from_constraint("CHECK (event_type IN ('TICK') OR source = 'runtime')", column="event_type")


@pytest.mark.parametrize(
    ("definition", "reason"),
    (
        ("", "empty"),
        ("CHECK (other_column IN ('TICK'))", "registered column"),
        ("CHECK (event_type IS NOT NULL)", "no literal"),
        ("CHECK (event_type LIKE 'TICK')", "structure drift"),
    ),
)
def test_constraint_literal_parser_fails_loud_for_every_non_allowlist_shape(definition: str, reason: str) -> None:
    with pytest.raises(QuoteEventSchemaPreflightError, match=reason):
        allowed_literals_from_constraint(definition, column="event_type")


def test_allowlist_and_composite_classifiers_reject_unknown_carriers() -> None:
    with pytest.raises(QuoteEventSchemaPreflightError, match="unknown allowlist kind"):
        classify_allowlist(TARGET_EVENT_TYPES, kind="payload")
    with pytest.raises(QuoteEventSchemaPreflightError, match="empty"):
        event_composites_from_constraint("")
    with pytest.raises(QuoteEventSchemaPreflightError, match="LEGACY_V1"):
        event_composites_from_constraint(
            _composite_definition().replace("event_contract_version = 'LEGACY_V1' OR ", "")
        )
    first = sorted(EVENT_ROUTING_COMPOSITES_V1)[0]
    duplicated = _composite_definition().replace(
        ")",
        f") OR (event_type = '{first[0]}' AND source = '{first[1]}' AND payload_schema_version = '{first[2]}')",
        1,
    )
    with pytest.raises(QuoteEventSchemaPreflightError, match="duplicate"):
        event_composites_from_constraint(duplicated)
