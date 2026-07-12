"""Read-only P1-D CHECK migration preflight and production readback helpers.

The SQL migration is the only component allowed to change the two CHECK
constraints.  This module deliberately contains no ``ALTER`` statement: it
lets an operator prove the exact before/after schema and record the migration
file hash without giving application startup a DDL capability.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
import re
from typing import Any, Iterable

from .models import MINIQMT_EXECUTION_EVENT_SOURCES, MiniQMTExecutionEventType


MIGRATION_FILENAME = "miniqmt_quote_ingress_event_types_20260712.sql"
ROLLBACK_FILENAME = "miniqmt_quote_ingress_event_types_20260712.rollback.sql"

OLD_EVENT_TYPES = frozenset(
    {
        "RUNTIME_CREATED",
        "GATEWAY_CONNECTED",
        "GATEWAY_DISCONNECTED",
        "BROKER_SYNC_STARTED",
        "BROKER_SYNCED",
        "ALGO_INSTANCE_CREATED",
        "TIMER",
        "TICK",
        "ALGO_ACTION_EMITTED",
        "CHILD_ORDER_SUBMITTED",
        "CHILD_ORDER_REJECTED",
        "CHILD_ORDER_CANCEL_REQUESTED",
        "ORDER_EVENT",
        "TRADE_EVENT",
        "ACCOUNT_EVENT",
        "RISK_KILL_SWITCH_TRIGGERED",
        "RECONCILE_STARTED",
        "RECONCILE_COMPLETED",
        "OPERATOR_COMMAND_RECEIVED",
        "OPERATOR_COMMAND_EXECUTED",
        "OPERATOR_COMMAND_REJECTED",
        "RUNTIME_STOPPED",
    }
)
NEW_QUOTE_EVENT_TYPES = frozenset(
    {
        "QUOTE_OBSERVED",
        "QUOTE_REJECTED",
        "QUOTE_ELIGIBILITY_EVALUATED",
        "QUOTE_MARK_CAPTURED",
        "QUOTE_INGRESS_HEALTH",
    }
)
TARGET_EVENT_TYPES = OLD_EVENT_TYPES | NEW_QUOTE_EVENT_TYPES
OLD_EVENT_SOURCES = frozenset({"runtime", "gateway", "oms", "algo", "operator", "recovery"})
TARGET_EVENT_SOURCES = OLD_EVENT_SOURCES | frozenset({"quote_ingress"})


class QuoteEventSchemaPreflightError(ValueError):
    """Schema is absent, drifted, or unsafe to migrate/rollback."""


@dataclass(frozen=True)
class QuoteEventConstraintReadback:
    constraint_name: str
    constraint_oid: int
    validated: bool
    definition: str
    definition_sha256: str
    allowed_values: frozenset[str]


@dataclass(frozen=True)
class QuoteEventSchemaReceipt:
    schema_version: str
    table_oid: int
    table_identity: str
    database_identity: str
    queried_at_utc: datetime
    event_type: QuoteEventConstraintReadback
    event_source: QuoteEventConstraintReadback
    row_count: int
    old_event_type_row_count: int
    new_event_type_row_count: int
    old_source_row_count: int
    new_source_row_count: int
    unknown_value_count: int
    migration_file_sha256: str
    rollback_file_sha256: str
    state: str
    production_ddl_gate: str


def migration_path() -> Path:
    return Path(__file__).resolve().parents[2] / "migrations" / MIGRATION_FILENAME


def rollback_path() -> Path:
    return Path(__file__).resolve().parents[2] / "migrations" / ROLLBACK_FILENAME


def migration_file_sha256(path: Path | None = None) -> str:
    return sha256((path or migration_path()).read_bytes()).hexdigest()


def rollback_file_sha256(path: Path | None = None) -> str:
    return sha256((path or rollback_path()).read_bytes()).hexdigest()


def assert_runtime_registry_matches_target() -> None:
    """Fail closed if Python enum and DDL canonical registry drift."""

    actual = frozenset(item.value for item in MiniQMTExecutionEventType)
    if actual != TARGET_EVENT_TYPES:
        raise QuoteEventSchemaPreflightError(
            f"runtime event enum drifts from canonical migration registry: missing={sorted(TARGET_EVENT_TYPES - actual)}, "
            f"extra={sorted(actual - TARGET_EVENT_TYPES)}"
        )
    if MINIQMT_EXECUTION_EVENT_SOURCES != TARGET_EVENT_SOURCES:
        raise QuoteEventSchemaPreflightError(
            f"runtime event source registry drifts from canonical migration registry: "
            f"missing={sorted(TARGET_EVENT_SOURCES - MINIQMT_EXECUTION_EVENT_SOURCES)}, "
            f"extra={sorted(MINIQMT_EXECUTION_EVENT_SOURCES - TARGET_EVENT_SOURCES)}"
        )


def allowed_literals_from_constraint(definition: str, *, column: str) -> frozenset[str]:
    """Extract only an ordinary single-column allowlist CHECK definition.

    PostgreSQL may render ``IN`` as ``= ANY(ARRAY[..])``.  We allow those two
    normalized forms but reject conjunctions, disjunctions and references to
    other columns so an expression drift cannot pass merely because its quoted
    literals happen to match.
    """

    if not isinstance(definition, str) or not definition.strip():
        raise QuoteEventSchemaPreflightError(f"{column} CHECK definition is empty")
    normalized = re.sub(r"\s+", "", definition.upper())
    expected_column = column.upper()
    if expected_column not in normalized or "CHECK" not in normalized:
        raise QuoteEventSchemaPreflightError(f"{column} CHECK definition does not constrain the registered column")
    expression_without_literals = re.sub(r"'[^']+'", "", normalized)
    if "AND" in expression_without_literals or "OR" in expression_without_literals or ";" in expression_without_literals:
        raise QuoteEventSchemaPreflightError(f"{column} CHECK definition contains an unregistered expression")
    literals = frozenset(re.findall(r"'([^']+)'", definition))
    if not literals:
        raise QuoteEventSchemaPreflightError(f"{column} CHECK definition has no literal allowlist")
    stripped = expression_without_literals
    stripped = re.sub(r"::[A-Z_]+", "", stripped)
    stripped = re.sub(r"CHECK|EVENT_TYPE|SOURCE|IN|ANY|ARRAY|TEXT|VARCHAR|CHARACTER|\(|\)|\[|\]|,|=", "", stripped)
    if stripped:
        raise QuoteEventSchemaPreflightError(f"{column} CHECK definition structure drift: {definition}")
    return literals


def classify_allowlist(values: Iterable[str], *, kind: str) -> str:
    actual = frozenset(values)
    old, target = (OLD_EVENT_TYPES, TARGET_EVENT_TYPES) if kind == "event_type" else (OLD_EVENT_SOURCES, TARGET_EVENT_SOURCES)
    if actual == old:
        return "old"
    if actual == target:
        return "target"
    raise QuoteEventSchemaPreflightError(
        f"{kind} CHECK allowlist is not exact old/target: missing_target={sorted(target - actual)}, extra={sorted(actual - target)}"
    )


def read_quote_event_schema(connection: Any) -> QuoteEventSchemaReceipt:
    """Read and validate current DB schema without taking a migration action."""

    assert_runtime_registry_matches_target()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT current_database(), current_user,
                   COALESCE(inet_server_addr()::text, 'local'),
                   COALESCE(inet_server_port(), 0),
                   clock_timestamp()
            """
        )
        identity_row = cursor.fetchone()
        if identity_row is None or len(identity_row) != 5:
            raise QuoteEventSchemaPreflightError("database identity readback is unavailable")
        database_identity = f"{identity_row[0]}|{identity_row[1]}|{identity_row[2]}:{identity_row[3]}"
        queried_at_utc = identity_row[4]
        if not isinstance(queried_at_utc, datetime) or queried_at_utc.tzinfo is None or queried_at_utc.utcoffset() is None:
            raise QuoteEventSchemaPreflightError("database query timestamp is not timezone-aware")
        cursor.execute("SELECT to_regclass('qmt_strategy.execution_runtime_event')")
        row = cursor.fetchone()
        table_identity = row[0] if row else None
        if table_identity is None:
            raise QuoteEventSchemaPreflightError("qmt_strategy.execution_runtime_event does not exist")
        cursor.execute(
            """
            SELECT c.oid, c.conname, c.convalidated, pg_get_constraintdef(c.oid, true)
            FROM pg_constraint AS c
            WHERE c.conrelid = 'qmt_strategy.execution_runtime_event'::regclass
              AND c.conname IN ('ck_miniqmt_event_type', 'ck_miniqmt_event_source')
            ORDER BY c.conname
            """
        )
        rows = cursor.fetchall()
        if len(rows) != 2:
            raise QuoteEventSchemaPreflightError("both named P1-D CHECK constraints must exist exactly once")
        constraints = {str(item[1]): item for item in rows}
        if set(constraints) != {"ck_miniqmt_event_type", "ck_miniqmt_event_source"}:
            raise QuoteEventSchemaPreflightError("P1-D CHECK constraint names drift")
        type_readback = _constraint_readback(constraints["ck_miniqmt_event_type"], column="event_type")
        source_readback = _constraint_readback(constraints["ck_miniqmt_event_source"], column="source")
        type_state = classify_allowlist(type_readback.allowed_values, kind="event_type")
        source_state = classify_allowlist(source_readback.allowed_values, kind="source")
        if type_state != source_state:
            raise QuoteEventSchemaPreflightError("event type/source CHECK constraints are at different migration states")
        cursor.execute(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE event_type = ANY(%s)),
                   COUNT(*) FILTER (WHERE event_type = ANY(%s)),
                   COUNT(*) FILTER (WHERE source = ANY(%s)),
                   COUNT(*) FILTER (WHERE source = 'quote_ingress'),
                   COUNT(*) FILTER (WHERE event_type <> ALL(%s) OR source <> ALL(%s))
            FROM qmt_strategy.execution_runtime_event
            """,
            (
                sorted(OLD_EVENT_TYPES),
                sorted(NEW_QUOTE_EVENT_TYPES),
                sorted(OLD_EVENT_SOURCES),
                sorted(TARGET_EVENT_TYPES),
                sorted(TARGET_EVENT_SOURCES),
            ),
        )
        count_row = cursor.fetchone()
    return QuoteEventSchemaReceipt(
        schema_version="miniqmt_quote_event_schema_readback_v1",
        table_oid=_table_oid(connection),
        table_identity=str(table_identity),
        database_identity=database_identity,
        queried_at_utc=queried_at_utc.astimezone(UTC),
        event_type=type_readback,
        event_source=source_readback,
        row_count=int(count_row[0] or 0),
        old_event_type_row_count=int(count_row[1] or 0),
        new_event_type_row_count=int(count_row[2] or 0),
        old_source_row_count=int(count_row[3] or 0),
        new_source_row_count=int(count_row[4] or 0),
        unknown_value_count=int(count_row[5] or 0),
        migration_file_sha256=migration_file_sha256(),
        rollback_file_sha256=rollback_file_sha256(),
        state=type_state,
        production_ddl_gate="applied_and_verified" if type_state == "target" and int(count_row[5] or 0) == 0 else "pending",
    )


def _constraint_readback(row: Any, *, column: str) -> QuoteEventConstraintReadback:
    oid, name, validated, definition = row
    if not bool(validated):
        raise QuoteEventSchemaPreflightError(f"{name} is not validated")
    return QuoteEventConstraintReadback(
        constraint_name=str(name),
        constraint_oid=int(oid),
        validated=True,
        definition=str(definition),
        definition_sha256=sha256(str(definition).encode("utf-8")).hexdigest(),
        allowed_values=allowed_literals_from_constraint(str(definition), column=column),
    )


def _table_oid(connection: Any) -> int:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 'qmt_strategy.execution_runtime_event'::regclass::oid")
        row = cursor.fetchone()
    if not row:
        raise QuoteEventSchemaPreflightError("qmt_strategy.execution_runtime_event oid readback failed")
    return int(row[0])


__all__ = [
    "MIGRATION_FILENAME",
    "NEW_QUOTE_EVENT_TYPES",
    "OLD_EVENT_SOURCES",
    "OLD_EVENT_TYPES",
    "QuoteEventConstraintReadback",
    "QuoteEventSchemaPreflightError",
    "QuoteEventSchemaReceipt",
    "ROLLBACK_FILENAME",
    "TARGET_EVENT_SOURCES",
    "TARGET_EVENT_TYPES",
    "allowed_literals_from_constraint",
    "assert_runtime_registry_matches_target",
    "classify_allowlist",
    "migration_file_sha256",
    "migration_path",
    "read_quote_event_schema",
    "rollback_file_sha256",
    "rollback_path",
]
