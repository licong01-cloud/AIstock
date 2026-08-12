from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from psycopg2.extensions import TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_INTRANS

import backend.services.miniqmt_execution_runtime.quote_event_schema as schema_module
from backend.services.miniqmt_execution_runtime.quote_event_schema import (
    EXPECTED_MIGRATION_FILE_SHA256,
    EXPECTED_PREFLIGHT_FILE_SHA256,
    EXPECTED_ROLLBACK_FILE_SHA256,
    OLD_EVENT_SOURCES,
    OLD_EVENT_TYPES,
    P1D_EVENT_SOURCES,
    P1D_EVENT_TYPES,
    TARGET_EVENT_SOURCES,
    TARGET_EVENT_TYPES,
    TARGET_KERNEL_EVENT_COMPOSITES,
    TARGET_LEGACY_EVENT_PAIRS,
    QuoteEventSchemaPreflightError,
    allowed_literals_from_constraint,
    assert_runtime_registry_matches_target,
    classify_allowlist,
    read_quote_event_schema,
)


def _array(values: frozenset[str]) -> str:
    return ", ".join(f"'{value}'::text" for value in sorted(values))


_LEGACY_EXPRESSION = (
    "event_contract_version = 'LEGACY_V1'::text AND "
    f"(event_type = ANY (ARRAY[{_array(P1D_EVENT_TYPES)}])) AND "
    f"(source = ANY (ARRAY[{_array(P1D_EVENT_SOURCES)}]))"
)
_KERNEL_EXPRESSION = (
    "event_contract_version = 'KERNEL_V2'::text AND ("
    "event_type = 'ALGO_START'::text AND source = 'MINIQMT_EXECUTION_KERNEL'::text AND "
    "(payload_schema_version = ANY (ARRAY['miniqmt_algo_start_v1'::text, 'miniqmt_algo_start_v2'::text])) OR "
    "event_type = 'COMMAND_OUTCOME'::text AND source = 'MINIQMT_EXECUTION_KERNEL'::text AND "
    "payload_schema_version = 'miniqmt_command_outcome_v1'::text OR "
    "event_type = 'TICK'::text AND source = 'B0_QUOTE_V2'::text AND "
    "payload_schema_version = 'miniqmt_market_data_view_v2'::text OR "
    "event_type = 'TIMER'::text AND source = 'EXCHANGE_SESSION_CLOCK'::text AND "
    "payload_schema_version = 'miniqmt_timer_due_v1'::text OR "
    "event_type = 'SESSION'::text AND source = 'EXCHANGE_SESSION_CLOCK'::text AND "
    "payload_schema_version = 'miniqmt_session_event_v1'::text OR "
    "event_type = 'EOD'::text AND source = 'EXCHANGE_SESSION_CLOCK'::text AND "
    "payload_schema_version = 'miniqmt_eod_event_v1'::text OR "
    "event_type = 'ORDER'::text AND source = 'QMT_GATEWAY_CALLBACK'::text AND "
    "payload_schema_version = 'miniqmt_order_event_v1'::text OR "
    "event_type = 'TRADE'::text AND source = 'QMT_GATEWAY_CALLBACK'::text AND "
    "payload_schema_version = 'miniqmt_trade_fact_v1'::text OR "
    "event_type = 'ACCOUNT'::text AND source = 'QMT_OMS_PROJECTION'::text AND "
    "payload_schema_version = 'miniqmt_account_projection_v1'::text OR "
    "event_type = 'RECONCILE'::text AND source = 'QMT_OMS_RECONCILIATION'::text AND "
    "payload_schema_version = 'miniqmt_reconciliation_receipt_v1'::text OR "
    "event_type = 'OPERATOR'::text AND source = 'SIMULATION_RUNTIME_OPERATOR'::text AND "
    "payload_schema_version = 'miniqmt_operator_command_v1'::text)"
)
_COMPOSITE_EXPRESSION = f"({_LEGACY_EXPRESSION} OR {_KERNEL_EXPRESSION}) IS TRUE"
_EVENT_CONTRACT_DEFINITION = (
    "CHECK ((event_contract_version = 'LEGACY_V1'::text AND event_schema_version IS NULL AND "
    "payload_schema_version IS NULL AND event_key_sha256 IS NULL AND payload_sha256 IS NULL AND "
    "observed_at_utc IS NULL AND logical_at_utc IS NULL AND source_identity_json IS NULL AND "
    "correlation_json IS NULL AND ingress_receipt_json IS NULL AND ingress_receipt_sha256 IS NULL AND "
    "routing_rule_version IS NULL AND transaction_commit_identity IS NULL OR "
    "event_contract_version = 'KERNEL_V2'::text AND event_schema_version IS NOT NULL AND "
    "event_schema_version = 'miniqmt_runtime_event_envelope_v2'::text AND "
    "payload_schema_version IS NOT NULL AND event_key_sha256 IS NOT NULL AND "
    "event_key_sha256 ~ '^[0-9a-f]{64}$'::text AND payload_sha256 IS NOT NULL AND "
    "payload_sha256 ~ '^[0-9a-f]{64}$'::text AND observed_at_utc IS NOT NULL AND "
    "logical_at_utc IS NOT NULL AND source_identity_json IS NOT NULL AND correlation_json IS NOT NULL AND "
    "ingress_receipt_json IS NOT NULL AND ingress_receipt_sha256 IS NOT NULL AND "
    "ingress_receipt_sha256 ~ '^[0-9a-f]{64}$'::text AND routing_rule_version IS NOT NULL AND "
    "routing_rule_version = 'miniqmt_event_routing_v1'::text AND transaction_commit_identity IS NOT NULL) IS TRUE)"
)


def _definition(column: str, values: frozenset[str]) -> str:
    literals = ", ".join(f"'{value}'::text" for value in sorted(values))
    return f"CHECK (({column} = ANY (ARRAY[{literals}])) IS TRUE)"


class _Cursor:
    def __init__(
        self,
        *,
        connection: "_Connection",
        legacy_pairs: frozenset[tuple[str, str]],
        nullable_kernel_accept_count: int,
        invalid_envelope_contract_count: int,
        extra_check: bool,
    ) -> None:
        self.connection = connection
        self.sql = ""
        self.legacy_pairs = legacy_pairs
        self.nullable_kernel_accept_count = nullable_kernel_accept_count
        self.invalid_envelope_contract_count = invalid_envelope_contract_count
        self.extra_check = extra_check

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, sql: str, _params: object | None = None) -> None:
        self.sql = sql
        self.connection.transaction_status = TRANSACTION_STATUS_INTRANS

    def fetchone(self):  # type: ignore[no-untyped-def]
        if "SHOW transaction_isolation" in self.sql:
            return (self.connection.transaction_isolation,)
        if "SHOW transaction_read_only" in self.sql:
            return (self.connection.transaction_read_only,)
        if "current_database()" in self.sql:
            return (
                "aistock_dev",
                "tester",
                "127.0.0.1",
                5432,
                "170005",
                "C",
                datetime(2026, 7, 12, tzinfo=UTC),
            )
        if "to_regclass" in self.sql:
            return ("qmt_strategy.execution_runtime_event",)
        if "FROM candidates" in self.sql and "COUNT(*)" in self.sql:
            return (self.nullable_kernel_accept_count,)
        if "COUNT(*)" in self.sql:
            return (3, 2, 1, 2, 1, 0, 0, self.invalid_envelope_contract_count)
        if "::regclass::oid" in self.sql:
            return (101,)
        raise AssertionError(self.sql)

    def fetchall(self):  # type: ignore[no-untyped-def]
        if "FROM pg_constraint" in self.sql:
            rows = [
                (
                    13,
                    "ck_miniqmt_k2_event_contract",
                    True,
                    _EVENT_CONTRACT_DEFINITION,
                    "",
                ),
                (15, "ck_miniqmt_event_id", True, "CHECK ((btrim(event_id) <> ''::text) IS TRUE)", ""),
                (16, "ck_miniqmt_event_sequence", True, "CHECK ((sequence > 0) IS TRUE)", ""),
                (
                    12,
                    "ck_miniqmt_k2_event_composite",
                    True,
                    f"CHECK ({_COMPOSITE_EXPRESSION})",
                    _COMPOSITE_EXPRESSION,
                ),
                (11, "ck_miniqmt_event_source", True, _definition("source", TARGET_EVENT_SOURCES), ""),
                (10, "ck_miniqmt_event_type", True, _definition("event_type", TARGET_EVENT_TYPES), ""),
            ]
            if self.extra_check:
                rows.append((14, "ck_blocks_tick", True, "CHECK (event_type <> 'TICK'::text)", ""))
            return rows
        if "SELECT event_type, source, payload_schema_version" in self.sql:
            return sorted(TARGET_KERNEL_EVENT_COMPOSITES)
        if "SELECT event_type, source" in self.sql and "FROM candidates" in self.sql:
            return sorted(self.legacy_pairs)
        if "GROUP BY event_contract_version" in self.sql:
            return []
        raise AssertionError(self.sql)


class _Connection:
    def __init__(
        self,
        *,
        legacy_pairs: frozenset[tuple[str, str]] = TARGET_LEGACY_EVENT_PAIRS,
        nullable_kernel_accept_count: int = 0,
        invalid_envelope_contract_count: int = 0,
        extra_check: bool = False,
        transaction_isolation: str = "repeatable read",
        transaction_read_only: str = "on",
    ) -> None:
        self.autocommit = True
        self.transaction_status = TRANSACTION_STATUS_IDLE
        self.rollback_count = 0
        self.transaction_isolation = transaction_isolation
        self.transaction_read_only = transaction_read_only
        self.legacy_pairs = legacy_pairs
        self.nullable_kernel_accept_count = nullable_kernel_accept_count
        self.invalid_envelope_contract_count = invalid_envelope_contract_count
        self.extra_check = extra_check

    def cursor(self) -> _Cursor:
        return _Cursor(
            connection=self,
            legacy_pairs=self.legacy_pairs,
            nullable_kernel_accept_count=self.nullable_kernel_accept_count,
            invalid_envelope_contract_count=self.invalid_envelope_contract_count,
            extra_check=self.extra_check,
        )

    def get_transaction_status(self) -> int:
        return self.transaction_status

    def rollback(self) -> None:
        self.rollback_count += 1
        self.transaction_status = TRANSACTION_STATUS_IDLE


def test_read_only_schema_readback_reports_exact_target_and_file_hashes() -> None:
    connection = _Connection()
    receipt = read_quote_event_schema(connection)
    assert receipt.state == "target"
    assert receipt.schema_state == "target_verified"
    assert receipt.production_ddl_gate == "pending_full_kernel_readback"
    assert connection.autocommit is True
    assert connection.rollback_count == 1
    assert receipt.table_oid == 101
    assert receipt.database_identity == "aistock_dev|tester|127.0.0.1:5432"
    assert receipt.server_version_num == "170005"
    assert receipt.database_collation == "C"
    assert receipt.queried_at_utc == datetime(2026, 7, 12, tzinfo=UTC)
    assert receipt.old_event_type_row_count == 2
    assert receipt.new_event_type_row_count == 1
    assert receipt.event_type.allowed_values == TARGET_EVENT_TYPES
    assert receipt.event_source.allowed_values == TARGET_EVENT_SOURCES
    assert receipt.event_id.definition_sha256 == ("836b7f7ebf14ee61ec94c9df82b300b42c96ff1046de0a2e0cfb8bc0f400642d")
    assert receipt.event_sequence.definition_sha256 == (
        "a1b188a1431066f2e8f2d0d51107b8c0532830ca7b88567ba1903c4b3999a3d0"
    )
    assert receipt.kernel_event_composite.accepted_composites == TARGET_KERNEL_EVENT_COMPOSITES
    assert receipt.kernel_event_composite.accepted_legacy_pairs == TARGET_LEGACY_EVENT_PAIRS
    assert receipt.kernel_event_composite.nullable_kernel_accept_count == 0
    assert receipt.kernel_event_contract.constraint_name == "ck_miniqmt_k2_event_contract"
    assert (
        receipt.kernel_event_contract.definition_sha256
        == "888bebaf7d9540ecadae15bfb7d2944db59177b4ed2ef5e8beb231b803f9faca"
    )
    assert receipt.invalid_kernel_composite_count == 0
    assert receipt.invalid_legacy_contract_count == 0
    assert receipt.invalid_event_contract_count == 0
    assert receipt.invalid_envelope_contract_count == 0
    assert receipt.migration_file_sha256 == EXPECTED_MIGRATION_FILE_SHA256
    assert receipt.preflight_file_sha256 == EXPECTED_PREFLIGHT_FILE_SHA256
    assert receipt.rollback_file_sha256 == EXPECTED_ROLLBACK_FILE_SHA256


def test_schema_readback_rejects_legacy_widening_and_nullable_kernel_acceptance() -> None:
    widened_legacy = TARGET_LEGACY_EVENT_PAIRS | {("SESSION", "EXCHANGE_SESSION_CLOCK")}
    with pytest.raises(QuoteEventSchemaPreflightError, match="exact P1-D LEGACY_V1 registry"):
        read_quote_event_schema(_Connection(legacy_pairs=widened_legacy))
    with pytest.raises(QuoteEventSchemaPreflightError, match="NULL KERNEL_V2"):
        read_quote_event_schema(_Connection(nullable_kernel_accept_count=1))


def test_schema_readback_marks_hash_correct_target_invalid_when_durable_envelope_is_incomplete() -> None:
    receipt = read_quote_event_schema(_Connection(invalid_envelope_contract_count=1))
    assert receipt.state == "target"
    assert receipt.schema_state == "invalid"
    assert receipt.production_ddl_gate == "pending"
    assert receipt.invalid_envelope_contract_count == 1
    assert receipt.invalid_event_contract_count == 1
    with pytest.raises(QuoteEventSchemaPreflightError, match="constraint names drift"):
        read_quote_event_schema(_Connection(extra_check=True))


@pytest.mark.parametrize(
    ("transaction_isolation", "transaction_read_only"),
    (("read committed", "off"), ("repeatable read", "on")),
)
def test_schema_readback_rejects_every_existing_transaction_without_rollback(
    transaction_isolation: str,
    transaction_read_only: str,
) -> None:
    connection = _Connection(
        transaction_isolation=transaction_isolation,
        transaction_read_only=transaction_read_only,
    )
    connection.autocommit = False
    connection.transaction_status = TRANSACTION_STATUS_INTRANS
    with pytest.raises(QuoteEventSchemaPreflightError, match="requires an idle connection"):
        read_quote_event_schema(connection)
    assert connection.rollback_count == 0
    assert connection.autocommit is False


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
    with pytest.raises(QuoteEventSchemaPreflightError, match="duplicate literals"):
        allowed_literals_from_constraint("CHECK (event_type IN ('TICK','TICK'))", column="event_type")


@pytest.mark.parametrize(
    ("definition", "message"),
    (
        ("", "definition is empty"),
        ("CHECK (other IN ('TICK'))", "does not constrain"),
        ("CHECK (event_type = event_type)", "has no literal"),
        ("CHECK (lower(event_type) IN ('TICK'))", "structure drift"),
    ),
)
def test_constraint_literal_parser_rejects_empty_wrong_column_literal_or_structure(
    definition: str,
    message: str,
) -> None:
    with pytest.raises(QuoteEventSchemaPreflightError, match=message):
        allowed_literals_from_constraint(definition, column="event_type")


@pytest.mark.parametrize(
    ("values", "kind", "expected"),
    (
        (P1D_EVENT_TYPES, "event_type", "predecessor"),
        (TARGET_EVENT_TYPES, "event_type", "target"),
        (OLD_EVENT_SOURCES, "source", "old"),
        (P1D_EVENT_SOURCES, "source", "predecessor"),
        (TARGET_EVENT_SOURCES, "source", "target"),
    ),
)
def test_allowlist_classifier_covers_each_registered_schema_generation(
    values: frozenset[str],
    kind: str,
    expected: str,
) -> None:
    assert classify_allowlist(values, kind=kind) == expected


def test_allowlist_classifier_rejects_unknown_kind_and_mixed_generation() -> None:
    with pytest.raises(QuoteEventSchemaPreflightError, match="unknown CHECK allowlist kind"):
        classify_allowlist(TARGET_EVENT_TYPES, kind="unknown")
    with pytest.raises(QuoteEventSchemaPreflightError, match="not exact old/predecessor/target"):
        classify_allowlist(TARGET_EVENT_TYPES | {"FORGED"}, kind="event_type")


@pytest.mark.parametrize(
    ("attribute", "value", "message"),
    (
        ("MiniQMTExecutionEventType", (SimpleNamespace(value="FORGED"),), "P1-D runtime event registry drift"),
        ("MINIQMT_EXECUTION_EVENT_SOURCES", frozenset({"FORGED"}), "P1-D runtime source registry drift"),
        ("TARGET_EVENT_TYPES", frozenset(), "target event type registry"),
        ("TARGET_EVENT_SOURCES", frozenset(), "target event source registry"),
        ("TARGET_KERNEL_EVENT_COMPOSITES", frozenset(), "composite registry drift"),
    ),
)
def test_runtime_registry_readback_rejects_each_code_owned_authority_drift(
    monkeypatch: pytest.MonkeyPatch,
    attribute: str,
    value: object,
    message: str,
) -> None:
    monkeypatch.setattr(schema_module, attribute, value)
    with pytest.raises(QuoteEventSchemaPreflightError, match=message):
        assert_runtime_registry_matches_target()


def test_schema_readback_rejects_connection_without_transaction_authority() -> None:
    with pytest.raises(QuoteEventSchemaPreflightError, match="transaction-aware PostgreSQL connection"):
        read_quote_event_schema(object())


def test_frozen_migration_identity_rejects_code_owned_hash_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(schema_module, "EXPECTED_MIGRATION_FILE_SHA256", "0" * 64)
    with pytest.raises(QuoteEventSchemaPreflightError, match="canonical-LF identity drift"):
        schema_module.migration_file_sha256()
