"""Read-only MiniQMT event CHECK migration and production readback helpers.

The SQL migration is the only component allowed to change the three CHECK
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

from . import plugin_contracts as _plugin_contracts
from .models import MINIQMT_EXECUTION_EVENT_SOURCES, MiniQMTExecutionEventType
from .plugin_contracts import EventSourceV2, EventTypeV2


MIGRATION_FILENAME = "miniqmt_quote_ingress_event_types_20260712.sql"
ROLLBACK_FILENAME = "miniqmt_quote_ingress_event_types_20260712.rollback.sql"
KERNEL_V2_MIGRATION_FILENAME = "miniqmt_kernel_v2_event_allowlists_20260810.sql"
KERNEL_V2_ROLLBACK_FILENAME = "miniqmt_kernel_v2_event_allowlists_20260810.rollback.sql"

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
QUOTE_V1_EVENT_TYPES = OLD_EVENT_TYPES | NEW_QUOTE_EVENT_TYPES
OLD_EVENT_SOURCES = frozenset({"runtime", "gateway", "oms", "algo", "operator", "recovery"})
QUOTE_V1_EVENT_SOURCES = OLD_EVENT_SOURCES | frozenset({"quote_ingress"})
KERNEL_V2_EVENT_TYPES = frozenset(item.value for item in EventTypeV2)
KERNEL_V2_EVENT_SOURCES = frozenset(item.value for item in EventSourceV2)
TARGET_EVENT_TYPES = QUOTE_V1_EVENT_TYPES | KERNEL_V2_EVENT_TYPES
TARGET_EVENT_SOURCES = QUOTE_V1_EVENT_SOURCES | KERNEL_V2_EVENT_SOURCES


def _runtime_event_routing_composites_v1() -> frozenset[tuple[str, str, str]]:
    composites: set[tuple[str, str, str]] = set()
    for event_type, (source, payload_schemas, _identity_fields) in _plugin_contracts._EVENT_COMPOSITE.items():
        schemas = (payload_schemas,) if isinstance(payload_schemas, str) else payload_schemas
        for payload_schema in schemas:
            composites.add((event_type.value, source.value, payload_schema))
    return frozenset(composites)


EVENT_ROUTING_COMPOSITES_V1 = _runtime_event_routing_composites_v1()
LEGACY_KERNEL_V2_EVENT_COMPOSITES = frozenset(
    item for item in EVENT_ROUTING_COMPOSITES_V1 if item[0] != "COMMAND_OUTCOME" and item[2] != "miniqmt_algo_start_v2"
)


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
class KernelV2EventCompositeReadback:
    constraint_name: str
    constraint_oid: int
    validated: bool
    definition: str
    definition_sha256: str
    allowed_composites: frozenset[tuple[str, str, str]]
    state: str


@dataclass(frozen=True)
class QuoteEventSchemaReceipt:
    schema_version: str
    table_oid: int
    table_identity: str
    database_identity: str
    queried_at_utc: datetime
    event_type: QuoteEventConstraintReadback
    event_source: QuoteEventConstraintReadback
    kernel_v2_composite: KernelV2EventCompositeReadback
    row_count: int
    old_event_type_row_count: int
    new_event_type_row_count: int
    old_source_row_count: int
    new_source_row_count: int
    kernel_v2_contract_row_count: int
    unknown_value_count: int
    migration_file_sha256: str
    rollback_file_sha256: str
    kernel_v2_migration_file_sha256: str
    kernel_v2_rollback_file_sha256: str
    state: str
    production_ddl_gate: str


def migration_path() -> Path:
    return Path(__file__).resolve().parents[2] / "migrations" / MIGRATION_FILENAME


def rollback_path() -> Path:
    return Path(__file__).resolve().parents[2] / "migrations" / ROLLBACK_FILENAME


def kernel_v2_migration_path() -> Path:
    return Path(__file__).resolve().parents[2] / "migrations" / KERNEL_V2_MIGRATION_FILENAME


def kernel_v2_rollback_path() -> Path:
    return Path(__file__).resolve().parents[2] / "migrations" / KERNEL_V2_ROLLBACK_FILENAME


def migration_file_sha256(path: Path | None = None) -> str:
    return sha256((path or migration_path()).read_bytes()).hexdigest()


def rollback_file_sha256(path: Path | None = None) -> str:
    return sha256((path or rollback_path()).read_bytes()).hexdigest()


def kernel_v2_migration_file_sha256(path: Path | None = None) -> str:
    return _canonical_lf_file_sha256(path or kernel_v2_migration_path())


def kernel_v2_rollback_file_sha256(path: Path | None = None) -> str:
    return _canonical_lf_file_sha256(path or kernel_v2_rollback_path())


def _canonical_lf_file_sha256(path: Path) -> str:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
    return sha256(text.encode("utf-8")).hexdigest()


def assert_runtime_registry_matches_target() -> None:
    """Fail closed if Python enum and DDL canonical registry drift."""

    legacy_event_types = frozenset(item.value for item in MiniQMTExecutionEventType)
    if legacy_event_types != QUOTE_V1_EVENT_TYPES:
        raise QuoteEventSchemaPreflightError(
            "legacy/quote runtime event enum drifts from canonical migration registry: "
            f"missing={sorted(QUOTE_V1_EVENT_TYPES - legacy_event_types)}, "
            f"extra={sorted(legacy_event_types - QUOTE_V1_EVENT_TYPES)}"
        )
    if MINIQMT_EXECUTION_EVENT_SOURCES != QUOTE_V1_EVENT_SOURCES:
        raise QuoteEventSchemaPreflightError(
            "legacy/quote runtime event source registry drifts from canonical migration registry: "
            f"missing={sorted(QUOTE_V1_EVENT_SOURCES - MINIQMT_EXECUTION_EVENT_SOURCES)}, "
            f"extra={sorted(MINIQMT_EXECUTION_EVENT_SOURCES - QUOTE_V1_EVENT_SOURCES)}"
        )
    kernel_event_types = frozenset(item.value for item in EventTypeV2)
    kernel_event_sources = frozenset(item.value for item in EventSourceV2)
    if kernel_event_types != KERNEL_V2_EVENT_TYPES or kernel_event_sources != KERNEL_V2_EVENT_SOURCES:
        raise QuoteEventSchemaPreflightError("KERNEL_V2 event registry drifts from the migration authority")
    routed_types = frozenset(item[0] for item in EVENT_ROUTING_COMPOSITES_V1)
    routed_sources = frozenset(item[1] for item in EVENT_ROUTING_COMPOSITES_V1)
    if routed_types != KERNEL_V2_EVENT_TYPES or routed_sources != KERNEL_V2_EVENT_SOURCES:
        raise QuoteEventSchemaPreflightError(
            "KERNEL_V2 composite routing does not cover every event type/source exactly"
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
    if (
        "AND" in expression_without_literals
        or "OR" in expression_without_literals
        or ";" in expression_without_literals
    ):
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
    if kind == "event_type":
        old, quote_v1, target = OLD_EVENT_TYPES, QUOTE_V1_EVENT_TYPES, TARGET_EVENT_TYPES
    elif kind == "source":
        old, quote_v1, target = OLD_EVENT_SOURCES, QUOTE_V1_EVENT_SOURCES, TARGET_EVENT_SOURCES
    else:
        raise QuoteEventSchemaPreflightError(f"unknown allowlist kind: {kind}")
    if actual == old:
        return "old"
    if actual == quote_v1:
        return "quote_v1"
    if actual == target:
        return "target"
    raise QuoteEventSchemaPreflightError(
        f"{kind} CHECK allowlist is not exact old/quote_v1/target: "
        f"missing_target={sorted(target - actual)}, extra={sorted(actual - target)}"
    )


def event_composites_from_constraint(definition: str) -> frozenset[tuple[str, str, str]]:
    """Parse the exact KERNEL_V2 OR-of-three-equalities CHECK shape."""

    if not isinstance(definition, str) or not definition.strip():
        raise QuoteEventSchemaPreflightError("KERNEL_V2 composite CHECK definition is empty")
    normalized = re.sub(r"\s+", "", definition)
    normalized = re.sub(r"::(?:text|charactervarying|varchar)", "", normalized, flags=re.IGNORECASE)
    if len(re.findall(r"EVENT_CONTRACT_VERSION='LEGACY_V1'", normalized, flags=re.IGNORECASE)) != 1:
        raise QuoteEventSchemaPreflightError("KERNEL_V2 composite CHECK lacks the exact LEGACY_V1 bypass")
    pattern = re.compile(
        r"EVENT_TYPE='([^']+)'ANDSOURCE='([^']+)'ANDPAYLOAD_SCHEMA_VERSION='([^']+)'",
        flags=re.IGNORECASE,
    )
    ordered = pattern.findall(normalized)
    composites = frozenset(ordered)
    if len(ordered) != len(composites):
        raise QuoteEventSchemaPreflightError("KERNEL_V2 composite CHECK contains duplicate routing branches")
    without_literals = re.sub(r"'[^']+'", "", normalized.upper())
    stripped = re.sub(
        r"CHECK|EVENT_CONTRACT_VERSION|LEGACY_V|EVENT_TYPE|SOURCE|PAYLOAD_SCHEMA_VERSION|AND|OR|[()=]",
        "",
        without_literals,
    )
    if stripped or without_literals.count("AND") != 2 * len(ordered) or without_literals.count("OR") != len(ordered):
        raise QuoteEventSchemaPreflightError("KERNEL_V2 composite CHECK expression structure drift")
    if composites not in {LEGACY_KERNEL_V2_EVENT_COMPOSITES, EVENT_ROUTING_COMPOSITES_V1}:
        raise QuoteEventSchemaPreflightError(
            "KERNEL_V2 composite CHECK routing is not exact legacy/target: "
            f"missing={sorted(EVENT_ROUTING_COMPOSITES_V1 - composites)}, "
            f"extra={sorted(composites - EVENT_ROUTING_COMPOSITES_V1)}"
        )
    return composites


def classify_event_composites(values: Iterable[tuple[str, str, str]]) -> str:
    actual = frozenset(values)
    if actual == LEGACY_KERNEL_V2_EVENT_COMPOSITES:
        return "legacy"
    if actual == EVENT_ROUTING_COMPOSITES_V1:
        return "target"
    raise QuoteEventSchemaPreflightError("KERNEL_V2 composite routing is not exact legacy/target")


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
        if (
            not isinstance(queried_at_utc, datetime)
            or queried_at_utc.tzinfo is None
            or queried_at_utc.utcoffset() is None
        ):
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
              AND c.conname IN (
                  'ck_miniqmt_event_type',
                  'ck_miniqmt_event_source',
                  'ck_miniqmt_k2_event_composite'
              )
            ORDER BY c.conname
            """
        )
        rows = cursor.fetchall()
        if len(rows) != 3:
            raise QuoteEventSchemaPreflightError(
                "all three named MiniQMT event CHECK constraints must exist exactly once"
            )
        constraints = {str(item[1]): item for item in rows}
        if set(constraints) != {
            "ck_miniqmt_event_type",
            "ck_miniqmt_event_source",
            "ck_miniqmt_k2_event_composite",
        }:
            raise QuoteEventSchemaPreflightError("MiniQMT event CHECK constraint names drift")
        type_readback = _constraint_readback(constraints["ck_miniqmt_event_type"], column="event_type")
        source_readback = _constraint_readback(constraints["ck_miniqmt_event_source"], column="source")
        composite_readback = _composite_readback(constraints["ck_miniqmt_k2_event_composite"])
        type_state = classify_allowlist(type_readback.allowed_values, kind="event_type")
        source_state = classify_allowlist(source_readback.allowed_values, kind="source")
        if type_state != source_state:
            raise QuoteEventSchemaPreflightError(
                "event type/source CHECK constraints are at different migration states"
            )
        if (type_state == "target") != (composite_readback.state == "target"):
            raise QuoteEventSchemaPreflightError(
                "event allowlist/composite CHECK constraints are at different migration states"
            )
        cursor.execute(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE event_type = ANY(%s)),
                   COUNT(*) FILTER (WHERE event_type = ANY(%s)),
                   COUNT(*) FILTER (WHERE source = ANY(%s)),
                   COUNT(*) FILTER (WHERE source = 'quote_ingress'),
                   COUNT(*) FILTER (WHERE event_contract_version = 'KERNEL_V2'),
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
        if count_row is None or len(count_row) != 7:
            raise QuoteEventSchemaPreflightError("MiniQMT event row-count readback is unavailable")
    return QuoteEventSchemaReceipt(
        schema_version="miniqmt_event_schema_readback_v2",
        table_oid=_table_oid(connection),
        table_identity=str(table_identity),
        database_identity=database_identity,
        queried_at_utc=queried_at_utc.astimezone(UTC),
        event_type=type_readback,
        event_source=source_readback,
        kernel_v2_composite=composite_readback,
        row_count=int(count_row[0] or 0),
        old_event_type_row_count=int(count_row[1] or 0),
        new_event_type_row_count=int(count_row[2] or 0),
        old_source_row_count=int(count_row[3] or 0),
        new_source_row_count=int(count_row[4] or 0),
        kernel_v2_contract_row_count=int(count_row[5] or 0),
        unknown_value_count=int(count_row[6] or 0),
        migration_file_sha256=migration_file_sha256(),
        rollback_file_sha256=rollback_file_sha256(),
        kernel_v2_migration_file_sha256=kernel_v2_migration_file_sha256(),
        kernel_v2_rollback_file_sha256=kernel_v2_rollback_file_sha256(),
        state=type_state,
        production_ddl_gate="applied_and_verified"
        if type_state == "target" and int(count_row[6] or 0) == 0
        else "pending",
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


def _composite_readback(row: Any) -> KernelV2EventCompositeReadback:
    oid, name, validated, definition = row
    if not bool(validated):
        raise QuoteEventSchemaPreflightError(f"{name} is not validated")
    composites = event_composites_from_constraint(str(definition))
    return KernelV2EventCompositeReadback(
        constraint_name=str(name),
        constraint_oid=int(oid),
        validated=True,
        definition=str(definition),
        definition_sha256=sha256(str(definition).encode("utf-8")).hexdigest(),
        allowed_composites=composites,
        state=classify_event_composites(composites),
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
    "KERNEL_V2_EVENT_SOURCES",
    "KERNEL_V2_EVENT_TYPES",
    "EVENT_ROUTING_COMPOSITES_V1",
    "LEGACY_KERNEL_V2_EVENT_COMPOSITES",
    "KERNEL_V2_MIGRATION_FILENAME",
    "KERNEL_V2_ROLLBACK_FILENAME",
    "NEW_QUOTE_EVENT_TYPES",
    "OLD_EVENT_SOURCES",
    "OLD_EVENT_TYPES",
    "QuoteEventConstraintReadback",
    "KernelV2EventCompositeReadback",
    "QuoteEventSchemaPreflightError",
    "QuoteEventSchemaReceipt",
    "ROLLBACK_FILENAME",
    "TARGET_EVENT_SOURCES",
    "TARGET_EVENT_TYPES",
    "QUOTE_V1_EVENT_SOURCES",
    "QUOTE_V1_EVENT_TYPES",
    "allowed_literals_from_constraint",
    "assert_runtime_registry_matches_target",
    "classify_allowlist",
    "classify_event_composites",
    "event_composites_from_constraint",
    "kernel_v2_migration_file_sha256",
    "kernel_v2_migration_path",
    "kernel_v2_rollback_file_sha256",
    "kernel_v2_rollback_path",
    "migration_file_sha256",
    "migration_path",
    "read_quote_event_schema",
    "rollback_file_sha256",
    "rollback_path",
]
