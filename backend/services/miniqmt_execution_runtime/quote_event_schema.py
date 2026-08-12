"""Read-only authority for the MiniQMT runtime-event CHECK contract.

The successor migration is operator-applied DDL.  Application startup only
performs strict catalog and durable-row readback; it never repairs, relaxes or
silently accepts a mixed schema.  The target closes the legacy/P1-D event
registry and the final KERNEL_V2 composite registry in one authority.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
import re
from typing import Any, Iterable, Iterator

from psycopg2.extensions import TRANSACTION_STATUS_IDLE

from .models import MINIQMT_EXECUTION_EVENT_SOURCES, MiniQMTExecutionEventType
from .plugin_contracts import EventSourceV2, EventTypeV2, _EVENT_COMPOSITE


MIGRATION_FILENAME = "miniqmt_execution_kernel_event_contract_repair_20260811.sql"
PREFLIGHT_FILENAME = "miniqmt_execution_kernel_event_contract_repair_20260811.preflight.sql"
ROLLBACK_FILENAME = "miniqmt_execution_kernel_event_contract_repair_20260811.rollback.sql"

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
P1D_EVENT_TYPES = OLD_EVENT_TYPES | NEW_QUOTE_EVENT_TYPES
OLD_EVENT_SOURCES = frozenset({"runtime", "gateway", "oms", "algo", "operator", "recovery"})
P1D_EVENT_SOURCES = OLD_EVENT_SOURCES | frozenset({"quote_ingress"})


def _kernel_composites() -> frozenset[tuple[str, str, str]]:
    result: set[tuple[str, str, str]] = set()
    for event_type, (source, schemas, _required_identity_fields) in _EVENT_COMPOSITE.items():
        accepted_schemas = schemas if isinstance(schemas, tuple) else (schemas,)
        result.update((event_type.value, source.value, schema) for schema in accepted_schemas)
    return frozenset(result)


TARGET_KERNEL_EVENT_COMPOSITES = _kernel_composites()
PREDECESSOR_KERNEL_EVENT_COMPOSITES = frozenset(
    item
    for item in TARGET_KERNEL_EVENT_COMPOSITES
    if item[0] != EventTypeV2.COMMAND_OUTCOME.value and item[2] != "miniqmt_algo_start_v2"
)
TARGET_EVENT_TYPES = P1D_EVENT_TYPES | frozenset(item.value for item in EventTypeV2)
TARGET_EVENT_SOURCES = P1D_EVENT_SOURCES | frozenset(item.value for item in EventSourceV2)
TARGET_LEGACY_EVENT_PAIRS = frozenset(
    (event_type, source) for event_type in P1D_EVENT_TYPES for source in P1D_EVENT_SOURCES
)

# Filled from the final canonical-LF migration artifacts.  These are code-owned
# release identities, not observational hashes read from whichever checkout
# happens to be running.
EXPECTED_MIGRATION_FILE_SHA256 = "b1cf49270234af5034461fc6c6c30e6ee56c2278defb922fb3b4d879cd9c3e9a"
EXPECTED_PREFLIGHT_FILE_SHA256 = "013ca9838ff0f88bdd3c30682895114adc5a2c7d9d07832516cb63bf6f5f1217"
EXPECTED_ROLLBACK_FILE_SHA256 = "741d6cd667600d2ae09be15da28a5b928f86a4248706ff2c3a65e235ff170c96"

_PREDECESSOR_CONSTRAINT_NAMES = frozenset(
    {
        "ck_miniqmt_event_id",
        "ck_miniqmt_event_sequence",
        "ck_miniqmt_event_type",
        "ck_miniqmt_event_source",
        "ck_miniqmt_k2_event_composite",
        "ck_miniqmt_k2_event_contract",
    }
)
_EVENT_IDENTITY_CONSTRAINT_NAMES = frozenset(
    {
        "ck_miniqmt_event_id",
        "ck_miniqmt_event_sequence",
    }
)
_TARGET_CONSTRAINT_NAMES = _PREDECESSOR_CONSTRAINT_NAMES
_PREDECESSOR_EVENT_IDENTITY_CONSTRAINT_SHA256 = {
    "ck_miniqmt_event_id": "55f2f3dd015fc42bed99754d426d434e62a3456295263bbbf42c3358d8257608",
    "ck_miniqmt_event_sequence": "ddfd70c30577468691d352ae838281ec74c56efd9d5ec1c3e32967cf9ef5c6ed",
}
_TARGET_EVENT_IDENTITY_CONSTRAINT_SHA256 = {
    "ck_miniqmt_event_id": "836b7f7ebf14ee61ec94c9df82b300b42c96ff1046de0a2e0cfb8bc0f400642d",
    "ck_miniqmt_event_sequence": "a1b188a1431066f2e8f2d0d51107b8c0532830ca7b88567ba1903c4b3999a3d0",
}
_EXACT_CONSTRAINT_DEFINITION_SHA256 = {
    "predecessor": {
        **_PREDECESSOR_EVENT_IDENTITY_CONSTRAINT_SHA256,
        "ck_miniqmt_event_source": "835ad788ea103d5f0e7cca878c810331a2f1b7fdb1377a554acefa30cd209697",
        "ck_miniqmt_event_type": "148b6275debe87a7ebda2dc51385a6583a334f5a8dd6779e5124576758b4255e",
        "ck_miniqmt_k2_event_composite": "907e964380874d06918981201685af0338bef13f034c7becd5e04a9a591b06b3",
        "ck_miniqmt_k2_event_contract": "9d193860ed0de361ef590ba195b531c623afa09b42620f52e2c0938b9f6a1212",
    },
    "target": {
        **_TARGET_EVENT_IDENTITY_CONSTRAINT_SHA256,
        "ck_miniqmt_event_source": "c2f8e672b140ec88f667e251bbb5ff812cd0bea2a24f31c45d74c3f8d32eb881",
        "ck_miniqmt_event_type": "6ac3041d989166511127ec22d9379dd0ecdc09fb5055e72006100319026a6f24",
        "ck_miniqmt_k2_event_composite": "4a2d33d3fc75a4b468661e1bdbf2ecce9cd13aaab491c7c4d7605a1df3af3857",
        "ck_miniqmt_k2_event_contract": "888bebaf7d9540ecadae15bfb7d2944db59177b4ed2ef5e8beb231b803f9faca",
    },
}
_SAFE_COMPOSITE_IDENTIFIERS = frozenset(
    {
        "event_contract_version",
        "event_type",
        "source",
        "payload_schema_version",
        "and",
        "or",
        "any",
        "array",
        "text",
        "character",
        "varying",
        "in",
        "is",
        "not",
        "null",
        "true",
        "false",
    }
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
class KernelEventCompositeReadback:
    constraint_name: str
    constraint_oid: int
    validated: bool
    definition: str
    definition_sha256: str
    accepted_composites: frozenset[tuple[str, str, str]]
    accepted_legacy_pairs: frozenset[tuple[str, str]]
    nullable_kernel_accept_count: int
    state: str


@dataclass(frozen=True)
class KernelEventContractReadback:
    constraint_name: str
    constraint_oid: int
    validated: bool
    definition: str
    definition_sha256: str


@dataclass(frozen=True)
class QuoteEventSchemaReceipt:
    schema_version: str
    table_oid: int
    table_identity: str
    database_identity: str
    server_version_num: str
    database_collation: str
    queried_at_utc: datetime
    event_id: KernelEventContractReadback
    event_sequence: KernelEventContractReadback
    event_type: QuoteEventConstraintReadback
    event_source: QuoteEventConstraintReadback
    kernel_event_composite: KernelEventCompositeReadback
    kernel_event_contract: KernelEventContractReadback
    row_count: int
    old_event_type_row_count: int
    new_event_type_row_count: int
    old_source_row_count: int
    new_source_row_count: int
    kernel_v2_row_count: int
    unknown_value_count: int
    invalid_kernel_composite_count: int
    invalid_legacy_contract_count: int
    invalid_envelope_contract_count: int
    invalid_event_contract_count: int
    migration_file_sha256: str
    preflight_file_sha256: str
    rollback_file_sha256: str
    state: str
    schema_state: str

    @property
    def production_ddl_gate(self) -> str:
        """Never promote the six-CHECK slice to the full production schema gate.

        The complete gate additionally owns the frozen K2 helper body/config,
        independently recomputed catalog, and all K2/K2-D relations.  Callers
        that need ``applied_and_verified`` must use the kernel repository's
        full same-snapshot preflight.
        """

        return "pending_full_kernel_readback" if self.schema_state == "target_verified" else "pending"


def migration_path() -> Path:
    return Path(__file__).resolve().parents[2] / "migrations" / MIGRATION_FILENAME


def preflight_path() -> Path:
    return Path(__file__).resolve().parents[2] / "migrations" / PREFLIGHT_FILENAME


def rollback_path() -> Path:
    return Path(__file__).resolve().parents[2] / "migrations" / ROLLBACK_FILENAME


def _canonical_lf_sha256(path: Path) -> str:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
    return sha256(text.encode("utf-8")).hexdigest()


def _frozen_artifact_sha256(path: Path, *, expected: str, artifact: str) -> str:
    actual = _canonical_lf_sha256(path)
    if actual != expected:
        raise QuoteEventSchemaPreflightError(
            f"{artifact} canonical-LF identity drift: expected={expected}, actual={actual}"
        )
    return actual


def migration_file_sha256(path: Path | None = None) -> str:
    target = path or migration_path()
    return (
        _frozen_artifact_sha256(
            target,
            expected=EXPECTED_MIGRATION_FILE_SHA256,
            artifact=MIGRATION_FILENAME,
        )
        if path is None
        else _canonical_lf_sha256(target)
    )


def preflight_file_sha256(path: Path | None = None) -> str:
    target = path or preflight_path()
    return (
        _frozen_artifact_sha256(
            target,
            expected=EXPECTED_PREFLIGHT_FILE_SHA256,
            artifact=PREFLIGHT_FILENAME,
        )
        if path is None
        else _canonical_lf_sha256(target)
    )


def rollback_file_sha256(path: Path | None = None) -> str:
    target = path or rollback_path()
    return (
        _frozen_artifact_sha256(
            target,
            expected=EXPECTED_ROLLBACK_FILE_SHA256,
            artifact=ROLLBACK_FILENAME,
        )
        if path is None
        else _canonical_lf_sha256(target)
    )


def assert_runtime_registry_matches_target() -> None:
    """Fail closed when either runtime registry drifts from the DDL target."""

    p1d_types = frozenset(item.value for item in MiniQMTExecutionEventType)
    if p1d_types != P1D_EVENT_TYPES:
        raise QuoteEventSchemaPreflightError(
            "P1-D runtime event registry drift: "
            f"missing={sorted(P1D_EVENT_TYPES - p1d_types)}, extra={sorted(p1d_types - P1D_EVENT_TYPES)}"
        )
    if MINIQMT_EXECUTION_EVENT_SOURCES != P1D_EVENT_SOURCES:
        raise QuoteEventSchemaPreflightError(
            "P1-D runtime source registry drift: "
            f"missing={sorted(P1D_EVENT_SOURCES - MINIQMT_EXECUTION_EVENT_SOURCES)}, "
            f"extra={sorted(MINIQMT_EXECUTION_EVENT_SOURCES - P1D_EVENT_SOURCES)}"
        )
    kernel_types = frozenset(item.value for item in EventTypeV2)
    kernel_sources = frozenset(item.value for item in EventSourceV2)
    if TARGET_EVENT_TYPES != P1D_EVENT_TYPES | kernel_types:
        raise QuoteEventSchemaPreflightError("target event type registry is not the exact P1-D/KERNEL_V2 union")
    if TARGET_EVENT_SOURCES != P1D_EVENT_SOURCES | kernel_sources:
        raise QuoteEventSchemaPreflightError("target event source registry is not the exact P1-D/KERNEL_V2 union")
    if TARGET_KERNEL_EVENT_COMPOSITES != _kernel_composites():
        raise QuoteEventSchemaPreflightError("KERNEL_V2 composite registry drift")


def allowed_literals_from_constraint(definition: str, *, column: str) -> frozenset[str]:
    """Extract an exact ordinary single-column IN/ANY allowlist."""

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
    ordered_literals = re.findall(r"'([^']+)'", definition)
    literals = frozenset(ordered_literals)
    if not literals:
        raise QuoteEventSchemaPreflightError(f"{column} CHECK definition has no literal allowlist")
    if len(ordered_literals) != len(literals):
        raise QuoteEventSchemaPreflightError(f"{column} CHECK definition contains duplicate literals")
    stripped = re.sub(r"::[A-Z_]+", "", expression_without_literals)
    stripped = re.sub(
        r"CHECK|EVENT_TYPE|SOURCE|IN|ANY|ARRAY|TEXT|VARCHAR|CHARACTER|IS|TRUE|\(|\)|\[|\]|,|=",
        "",
        stripped,
    )
    if stripped:
        raise QuoteEventSchemaPreflightError(f"{column} CHECK definition structure drift: {definition}")
    return literals


def classify_allowlist(values: Iterable[str], *, kind: str) -> str:
    actual = frozenset(values)
    if kind == "event_type":
        old, predecessor, target = OLD_EVENT_TYPES, P1D_EVENT_TYPES, TARGET_EVENT_TYPES
    elif kind == "source":
        old, predecessor, target = OLD_EVENT_SOURCES, P1D_EVENT_SOURCES, TARGET_EVENT_SOURCES
    else:
        raise QuoteEventSchemaPreflightError(f"unknown CHECK allowlist kind: {kind}")
    if actual == old:
        return "old"
    if actual == predecessor:
        return "predecessor"
    if actual == target:
        return "target"
    raise QuoteEventSchemaPreflightError(
        f"{kind} CHECK allowlist is not exact old/predecessor/target: "
        f"missing_target={sorted(target - actual)}, extra={sorted(actual - target)}"
    )


@contextmanager
def _quote_event_schema_snapshot(connection: Any) -> Iterator[None]:
    """Own one locked repeatable-read snapshot for every public readback fact.

    An already active transaction is deliberately rejected.  Even when it is
    REPEATABLE READ and READ ONLY, its snapshot may have been established
    before this function acquires the relation lock and can therefore report a
    stale CHECK authority as current.  The kernel-wide preflight owns and locks
    its larger snapshot itself and calls the private in-snapshot reader.
    """

    autocommit = getattr(connection, "autocommit", None)
    get_transaction_status = getattr(connection, "get_transaction_status", None)
    rollback = getattr(connection, "rollback", None)
    if type(autocommit) is not bool or not callable(get_transaction_status) or not callable(rollback):
        raise QuoteEventSchemaPreflightError(
            "quote-event schema readback requires a transaction-aware PostgreSQL connection"
        )
    restore_autocommit = autocommit
    owns_transaction = False
    if restore_autocommit:
        connection.autocommit = False
    try:
        transaction_status = get_transaction_status()
        if transaction_status != TRANSACTION_STATUS_IDLE:
            raise QuoteEventSchemaPreflightError(
                "public quote-event schema readback requires an idle connection so it can own the locked "
                f"snapshot: transaction_status={transaction_status}"
            )
        owns_transaction = True
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL search_path = pg_catalog, qmt_strategy")
            cursor.execute("SHOW transaction_isolation")
            isolation_row = cursor.fetchone()
            cursor.execute("SHOW transaction_read_only")
            read_only_row = cursor.fetchone()
            isolation = str(isolation_row[0] if isolation_row else "").strip().lower()
            read_only = str(read_only_row[0] if read_only_row else "").strip().lower()
            if isolation != "repeatable read" or read_only not in {"on", "true"}:
                raise QuoteEventSchemaPreflightError(
                    "quote-event schema readback requires REPEATABLE READ READ ONLY: "
                    f"isolation={isolation or 'missing'}, read_only={read_only or 'missing'}"
                )
            cursor.execute("LOCK TABLE qmt_strategy.execution_runtime_event IN ACCESS SHARE MODE")
        yield
    finally:
        if owns_transaction:
            rollback()
        if restore_autocommit:
            connection.autocommit = True


def read_quote_event_schema(connection: Any) -> QuoteEventSchemaReceipt:
    """Read and validate the complete runtime-event CHECK authority."""

    with _quote_event_schema_snapshot(connection):
        return _read_quote_event_schema_in_snapshot(connection)


def _read_quote_event_schema_in_snapshot(connection: Any) -> QuoteEventSchemaReceipt:
    """Read the authority after the public wrapper has fixed one relation snapshot."""

    assert_runtime_registry_matches_target()
    migration_sha256 = migration_file_sha256()
    preflight_sha256 = preflight_file_sha256()
    rollback_sha256 = rollback_file_sha256()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT current_database(), current_user,
                   COALESCE(inet_server_addr()::text, 'local'),
                   COALESCE(inet_server_port(), 0),
                   current_setting('server_version_num'),
                   (SELECT datcollate FROM pg_database WHERE datname=current_database()),
                   clock_timestamp()
            """
        )
        identity_row = cursor.fetchone()
        if identity_row is None or len(identity_row) != 7:
            raise QuoteEventSchemaPreflightError("database identity readback is unavailable")
        database_identity = f"{identity_row[0]}|{identity_row[1]}|{identity_row[2]}:{identity_row[3]}"
        server_version_num = str(identity_row[4])
        database_collation = str(identity_row[5])
        queried_at_utc = identity_row[6]
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
            SELECT c.oid, c.conname, c.convalidated, pg_get_constraintdef(c.oid, true),
                   pg_get_expr(c.conbin, c.conrelid, true)
            FROM pg_constraint AS c
            WHERE c.conrelid = 'qmt_strategy.execution_runtime_event'::regclass
              AND c.contype = 'c'
            ORDER BY c.conname
            """
        )
        rows = cursor.fetchall()
        constraints = {str(item[1]): item for item in rows}
        constraint_names = set(constraints)
        if constraint_names != _TARGET_CONSTRAINT_NAMES:
            raise QuoteEventSchemaPreflightError("runtime-event CHECK constraint names drift")
        if len(rows) != len(constraints) or any(not bool(item[2]) for item in rows):
            raise QuoteEventSchemaPreflightError("runtime-event CHECK constraints are duplicated or not validated")
        event_id_readback = _contract_readback(constraints["ck_miniqmt_event_id"])
        event_sequence_readback = _contract_readback(constraints["ck_miniqmt_event_sequence"])
        type_readback = _constraint_readback(constraints["ck_miniqmt_event_type"], column="event_type")
        source_readback = _constraint_readback(constraints["ck_miniqmt_event_source"], column="source")
        composite_readback = _composite_readback(cursor, constraints["ck_miniqmt_k2_event_composite"])
        contract_readback = _contract_readback(constraints["ck_miniqmt_k2_event_contract"])
        type_state = classify_allowlist(type_readback.allowed_values, kind="event_type")
        source_state = classify_allowlist(source_readback.allowed_values, kind="source")
        if type_state == source_state == "target" and composite_readback.state == "target":
            state = "target"
        elif type_state == source_state == "predecessor" and composite_readback.state == "predecessor":
            state = "predecessor"
        else:
            raise QuoteEventSchemaPreflightError(
                "runtime-event CHECK constraints are at mixed migration states: "
                f"event_type={type_state}, source={source_state}, composite={composite_readback.state}"
            )
        observed_definition_hashes = {str(item[1]): sha256(str(item[3]).encode("utf-8")).hexdigest() for item in rows}
        expected_definition_hashes = _EXACT_CONSTRAINT_DEFINITION_SHA256[state]
        if observed_definition_hashes != expected_definition_hashes:
            raise QuoteEventSchemaPreflightError(
                f"runtime-event CHECK definition identity drift: state={state}, observed={observed_definition_hashes}"
            )
        cursor.execute(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE event_type = ANY(%s)),
                   COUNT(*) FILTER (WHERE event_type = ANY(%s)),
                   COUNT(*) FILTER (WHERE source = ANY(%s)),
                   COUNT(*) FILTER (WHERE source = ANY(%s)),
                   COUNT(*) FILTER (WHERE event_contract_version = 'KERNEL_V2'),
                   COUNT(*) FILTER (WHERE event_type <> ALL(%s) OR source <> ALL(%s)),
                   COUNT(*) FILTER (WHERE (
                       (event_contract_version = 'LEGACY_V1'
                        AND event_schema_version IS NULL
                        AND payload_schema_version IS NULL
                        AND event_key_sha256 IS NULL
                        AND payload_sha256 IS NULL
                        AND observed_at_utc IS NULL
                        AND logical_at_utc IS NULL
                        AND source_identity_json IS NULL
                        AND correlation_json IS NULL
                        AND ingress_receipt_json IS NULL
                        AND ingress_receipt_sha256 IS NULL
                        AND routing_rule_version IS NULL
                        AND transaction_commit_identity IS NULL)
                       OR
                       (event_contract_version = 'KERNEL_V2'
                        AND event_schema_version IS NOT NULL
                        AND event_schema_version = 'miniqmt_runtime_event_envelope_v2'
                        AND payload_schema_version IS NOT NULL
                        AND event_key_sha256 IS NOT NULL
                        AND event_key_sha256 ~ '^[0-9a-f]{64}$'
                        AND payload_sha256 IS NOT NULL
                        AND payload_sha256 ~ '^[0-9a-f]{64}$'
                        AND observed_at_utc IS NOT NULL
                        AND logical_at_utc IS NOT NULL
                        AND source_identity_json IS NOT NULL
                        AND correlation_json IS NOT NULL
                        AND ingress_receipt_json IS NOT NULL
                        AND ingress_receipt_sha256 IS NOT NULL
                        AND ingress_receipt_sha256 ~ '^[0-9a-f]{64}$'
                        AND routing_rule_version IS NOT NULL
                        AND routing_rule_version = 'miniqmt_event_routing_v1'
                        AND transaction_commit_identity IS NOT NULL)
                   ) IS NOT TRUE)
            FROM qmt_strategy.execution_runtime_event
            """,
            (
                sorted(OLD_EVENT_TYPES),
                sorted(TARGET_EVENT_TYPES - OLD_EVENT_TYPES),
                sorted(OLD_EVENT_SOURCES),
                sorted(TARGET_EVENT_SOURCES - OLD_EVENT_SOURCES),
                sorted(TARGET_EVENT_TYPES),
                sorted(TARGET_EVENT_SOURCES),
            ),
        )
        count_row = cursor.fetchone()
        if count_row is None or len(count_row) != 8:
            raise QuoteEventSchemaPreflightError("runtime-event row-count readback is unavailable")
        cursor.execute(
            """
            SELECT event_contract_version, event_type, source, payload_schema_version, COUNT(*)
            FROM qmt_strategy.execution_runtime_event
            GROUP BY event_contract_version, event_type, source, payload_schema_version
            ORDER BY event_contract_version, event_type, source, payload_schema_version
            """
        )
        durable_contract_rows = cursor.fetchall()
    invalid_kernel_composite_count = sum(
        int(item[4])
        for item in durable_contract_rows
        if str(item[0]) == "KERNEL_V2"
        and (str(item[1]), str(item[2]), str(item[3])) not in TARGET_KERNEL_EVENT_COMPOSITES
    )
    invalid_legacy_contract_count = sum(
        int(item[4])
        for item in durable_contract_rows
        if str(item[0]) == "LEGACY_V1" and (str(item[1]), str(item[2])) not in TARGET_LEGACY_EVENT_PAIRS
    )
    invalid_envelope_contract_count = int(count_row[7] or 0)
    invalid_event_contract_count = (
        invalid_kernel_composite_count
        + invalid_legacy_contract_count
        + sum(int(item[4]) for item in durable_contract_rows if str(item[0]) not in {"LEGACY_V1", "KERNEL_V2"})
        + invalid_envelope_contract_count
    )
    unknown_value_count = int(count_row[6] or 0)
    schema_state = f"{state}_verified" if unknown_value_count == 0 and invalid_event_contract_count == 0 else "invalid"
    return QuoteEventSchemaReceipt(
        schema_version="miniqmt_quote_event_schema_readback_v2",
        table_oid=_table_oid(connection),
        table_identity=str(table_identity),
        database_identity=database_identity,
        server_version_num=server_version_num,
        database_collation=database_collation,
        queried_at_utc=queried_at_utc.astimezone(UTC),
        event_id=event_id_readback,
        event_sequence=event_sequence_readback,
        event_type=type_readback,
        event_source=source_readback,
        kernel_event_composite=composite_readback,
        kernel_event_contract=contract_readback,
        row_count=int(count_row[0] or 0),
        old_event_type_row_count=int(count_row[1] or 0),
        new_event_type_row_count=int(count_row[2] or 0),
        old_source_row_count=int(count_row[3] or 0),
        new_source_row_count=int(count_row[4] or 0),
        kernel_v2_row_count=int(count_row[5] or 0),
        unknown_value_count=unknown_value_count,
        invalid_kernel_composite_count=invalid_kernel_composite_count,
        invalid_legacy_contract_count=invalid_legacy_contract_count,
        invalid_envelope_contract_count=invalid_envelope_contract_count,
        invalid_event_contract_count=invalid_event_contract_count,
        migration_file_sha256=migration_sha256,
        preflight_file_sha256=preflight_sha256,
        rollback_file_sha256=rollback_sha256,
        state=state,
        schema_state=schema_state,
    )


def _constraint_readback(row: Any, *, column: str) -> QuoteEventConstraintReadback:
    oid, name, validated, definition, _expression = row
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


def _contract_readback(row: Any) -> KernelEventContractReadback:
    oid, name, validated, definition, _expression = row
    if not bool(validated):
        raise QuoteEventSchemaPreflightError(f"{name} is not validated")
    return KernelEventContractReadback(
        constraint_name=str(name),
        constraint_oid=int(oid),
        validated=True,
        definition=str(definition),
        definition_sha256=sha256(str(definition).encode("utf-8")).hexdigest(),
    )


def _composite_readback(cursor: Any, row: Any) -> KernelEventCompositeReadback:
    oid, name, validated, definition, expression = row
    if not bool(validated):
        raise QuoteEventSchemaPreflightError(f"{name} is not validated")
    expression_text = str(expression or "").strip()
    _assert_safe_composite_expression(expression_text)
    literals = frozenset(re.findall(r"'([^']+)'", expression_text))
    target_schemas = frozenset(item[2] for item in TARGET_KERNEL_EVENT_COMPOSITES)
    unknown_literals = (
        literals
        - TARGET_EVENT_TYPES
        - TARGET_EVENT_SOURCES
        - target_schemas
        - {
            "LEGACY_V1",
            "KERNEL_V2",
        }
    )
    candidate_types = sorted(TARGET_EVENT_TYPES | unknown_literals)
    candidate_sources = sorted(TARGET_EVENT_SOURCES | unknown_literals)
    candidate_schemas = sorted(target_schemas | unknown_literals)
    cursor.execute(
        f"""
        WITH candidates AS (
            SELECT 'KERNEL_V2'::TEXT AS event_contract_version,
                   event_type, source, payload_schema_version
            FROM unnest(%s::TEXT[]) AS event_type
            CROSS JOIN unnest(%s::TEXT[]) AS source
            CROSS JOIN unnest(%s::TEXT[]) AS payload_schema_version
        )
        SELECT event_type, source, payload_schema_version
        FROM candidates
        WHERE ({expression_text})
        ORDER BY event_type, source, payload_schema_version
        """,
        (candidate_types, candidate_sources, candidate_schemas),
    )
    accepted = frozenset((str(item[0]), str(item[1]), str(item[2])) for item in cursor.fetchall())
    cursor.execute(
        f"""
        WITH candidates AS (
            SELECT 'LEGACY_V1'::TEXT AS event_contract_version,
                   event_type, source, payload_schema_version
            FROM unnest(%s::TEXT[]) AS event_type
            CROSS JOIN unnest(%s::TEXT[]) AS source
            CROSS JOIN unnest(ARRAY[NULL::TEXT]::TEXT[]) AS payload_schema_version
        )
        SELECT event_type, source
        FROM candidates
        WHERE ({expression_text}) IS TRUE
        ORDER BY event_type, source
        """,
        (candidate_types, candidate_sources),
    )
    accepted_legacy_pairs = frozenset((str(item[0]), str(item[1])) for item in cursor.fetchall())
    cursor.execute(
        f"""
        WITH candidates AS (
            SELECT 'KERNEL_V2'::TEXT AS event_contract_version,
                   event_type, source, NULL::TEXT AS payload_schema_version
            FROM unnest(%s::TEXT[]) AS event_type
            CROSS JOIN unnest(%s::TEXT[]) AS source
        )
        SELECT COUNT(*)
        FROM candidates
        WHERE ({expression_text}) IS NOT FALSE
        """,
        (candidate_types, candidate_sources),
    )
    nullable_row = cursor.fetchone()
    if nullable_row is None or len(nullable_row) != 1:
        raise QuoteEventSchemaPreflightError("K2 composite NULL acceptance readback is unavailable")
    nullable_kernel_accept_count = int(nullable_row[0] or 0)
    if accepted == PREDECESSOR_KERNEL_EVENT_COMPOSITES:
        state = "predecessor"
    elif accepted == TARGET_KERNEL_EVENT_COMPOSITES:
        state = "target"
    else:
        raise QuoteEventSchemaPreflightError(
            "K2 composite CHECK is not exact predecessor/target: "
            f"missing_target={sorted(TARGET_KERNEL_EVENT_COMPOSITES - accepted)}, "
            f"extra={sorted(accepted - TARGET_KERNEL_EVENT_COMPOSITES)}"
        )
    if state == "target":
        if accepted_legacy_pairs != TARGET_LEGACY_EVENT_PAIRS:
            raise QuoteEventSchemaPreflightError(
                "successor composite CHECK does not preserve the exact P1-D LEGACY_V1 registry: "
                f"missing={sorted(TARGET_LEGACY_EVENT_PAIRS - accepted_legacy_pairs)}, "
                f"extra={sorted(accepted_legacy_pairs - TARGET_LEGACY_EVENT_PAIRS)}"
            )
        if nullable_kernel_accept_count != 0:
            raise QuoteEventSchemaPreflightError(
                "successor composite CHECK accepts NULL KERNEL_V2 payload schema values"
            )
    elif not TARGET_LEGACY_EVENT_PAIRS <= accepted_legacy_pairs:
        raise QuoteEventSchemaPreflightError("predecessor composite CHECK no longer preserves the P1-D LEGACY_V1 path")
    return KernelEventCompositeReadback(
        constraint_name=str(name),
        constraint_oid=int(oid),
        validated=True,
        definition=str(definition),
        definition_sha256=sha256(str(definition).encode("utf-8")).hexdigest(),
        accepted_composites=accepted,
        accepted_legacy_pairs=accepted_legacy_pairs,
        nullable_kernel_accept_count=nullable_kernel_accept_count,
        state=state,
    )


def _assert_safe_composite_expression(expression: str) -> None:
    if not expression or ";" in expression or "--" in expression or "/*" in expression:
        raise QuoteEventSchemaPreflightError("K2 composite CHECK expression is empty or unsafe")
    without_literals = re.sub(r"'[^']*'", "", expression)
    identifiers = frozenset(item.lower() for item in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", without_literals))
    unexpected = identifiers - _SAFE_COMPOSITE_IDENTIFIERS
    if unexpected:
        raise QuoteEventSchemaPreflightError(
            f"K2 composite CHECK expression contains unregistered identifiers: {sorted(unexpected)}"
        )
    residual = re.sub(r"[A-Za-z_][A-Za-z0-9_]*", "", without_literals)
    residual = re.sub(r"[\s()\[\],=:<>]+", "", residual)
    if residual:
        raise QuoteEventSchemaPreflightError(f"K2 composite CHECK expression structure drift: {expression}")


def _table_oid(connection: Any) -> int:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 'qmt_strategy.execution_runtime_event'::regclass::oid")
        row = cursor.fetchone()
    if not row:
        raise QuoteEventSchemaPreflightError("qmt_strategy.execution_runtime_event oid readback failed")
    return int(row[0])


__all__ = [
    "EXPECTED_MIGRATION_FILE_SHA256",
    "EXPECTED_PREFLIGHT_FILE_SHA256",
    "EXPECTED_ROLLBACK_FILE_SHA256",
    "MIGRATION_FILENAME",
    "NEW_QUOTE_EVENT_TYPES",
    "OLD_EVENT_SOURCES",
    "OLD_EVENT_TYPES",
    "P1D_EVENT_SOURCES",
    "P1D_EVENT_TYPES",
    "PREFLIGHT_FILENAME",
    "PREDECESSOR_KERNEL_EVENT_COMPOSITES",
    "KernelEventCompositeReadback",
    "KernelEventContractReadback",
    "QuoteEventConstraintReadback",
    "QuoteEventSchemaPreflightError",
    "QuoteEventSchemaReceipt",
    "ROLLBACK_FILENAME",
    "TARGET_EVENT_SOURCES",
    "TARGET_EVENT_TYPES",
    "TARGET_KERNEL_EVENT_COMPOSITES",
    "TARGET_LEGACY_EVENT_PAIRS",
    "allowed_literals_from_constraint",
    "assert_runtime_registry_matches_target",
    "classify_allowlist",
    "migration_file_sha256",
    "migration_path",
    "preflight_file_sha256",
    "preflight_path",
    "read_quote_event_schema",
    "rollback_file_sha256",
    "rollback_path",
]
