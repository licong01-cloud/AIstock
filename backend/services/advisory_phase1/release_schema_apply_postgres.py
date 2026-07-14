"""Release-only Phase 1F plan builder and frozen PostgreSQL DDL executor.

No runtime module imports this executor.  It accepts only repository-frozen
migration bytes and deterministic month-partition operations; it has no SQL
argument, no force/skip flag, no DML, and no approval or role workflow.
"""

from __future__ import annotations

import hashlib
import logging
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator, Sequence

import psycopg2
import psycopg2.sql

from backend.services.advisory_phase1.release_schema_contract import (
    CONTRACT_SCHEMA_VERSION_V2,
    CatalogDifference,
    CatalogFingerprintEvidence,
    DatabaseIdentity,
    ExecutorAction,
    ManagedMigration,
    ManagedSchemaStatus,
    MigrationExecutionResult,
    MigrationExecutionStatus,
    MonthPartition,
    OperationStatus,
    PendingDdlOperation,
    PrerequisiteStatus,
    Phase1F1LegacyMonthInventory,
    ReleaseSchemaContract,
    ReleaseSchemaPlan,
    ReleaseSchemaPlanRequest,
    ReleaseSchemaReceipt,
    RequestedOperation,
    TransactionMode,
    canonical_json_sha256,
    plan_month_partitions,
    plan_month_partitions_for_contracts,
    REASON_DATABASE_IDENTITY_MISMATCH,
)
from backend.services.advisory_phase1.release_schema_verify_postgres import (
    CatalogVerification,
    DatabaseConnectionConfig,
    REASON_POSTGRES_VERSION_UNSUPPORTED,
    expected_managed_catalog_evidence,
    observed_managed_catalog_evidence,
    readonly_catalog_connection,
    subset_catalog_fingerprint,
    verify_catalog,
    verify_database_catalog,
)


LOGGER = logging.getLogger(__name__)

REASON_PLAN_CONTRACT_MISMATCH = "PHASE1F_PLAN_CONTRACT_MISMATCH"
REASON_PLAN_STALE = "PHASE1F_PLAN_STALE"
REASON_SCHEMA_DRIFTED = "PHASE1F_SCHEMA_DRIFTED"
REASON_DDL_LOCK_TIMEOUT = "PHASE1F_DDL_LOCK_TIMEOUT"
REASON_DDL_STATEMENT_TIMEOUT = "PHASE1F_DDL_STATEMENT_TIMEOUT"
REASON_DDL_EXECUTION_FAILED = "PHASE1F_DDL_EXECUTION_FAILED"
REASON_TRANSACTION_VERIFY_FAILED = "PHASE1F_TRANSACTION_VERIFY_FAILED"
REASON_POST_COMMIT_VERIFY_FAILED = "PHASE1F_POST_COMMIT_VERIFY_FAILED"
REASON_MIGRATION_FILE_MISSING = "PHASE1F_MIGRATION_FILE_MISSING"
REASON_MIGRATION_HASH_MISMATCH = "PHASE1F_MIGRATION_HASH_MISMATCH"
REASON_PHASE1F1_PREDECESSOR_SCHEMA_INVALID = "ADVISORY_PHASE1F1_PREDECESSOR_SCHEMA_INVALID"
REASON_PHASE1F1_PARENT_DATE_UNRESOLVED = "ADVISORY_PHASE1F1_PARENT_DATE_UNRESOLVED"
REASON_PHASE1F1_COPY_MISMATCH = "ADVISORY_PHASE1F1_COPY_MISMATCH"
REASON_PHASE1F1_VIEW_CONTRACT_MISMATCH = "ADVISORY_PHASE1F1_VIEW_CONTRACT_MISMATCH"
REASON_PHASE1F1_PARTITION_MISSING = "ADVISORY_PHASE1F1_PARTITION_MISSING"
REASON_PHASE1F1_CATALOG_DRIFTED = "ADVISORY_PHASE1F1_CATALOG_DRIFTED"
REASON_PHASE1F1_POST_COMMIT_VERIFY_FAILED = "ADVISORY_PHASE1F1_POST_COMMIT_VERIFY_FAILED"
REASON_PHASE1F1_POST_FAILURE_VERIFY_FAILED = "ADVISORY_PHASE1F1_POST_FAILURE_VERIFY_FAILED"


class ReleaseSchemaApplyError(RuntimeError):
    """Structured failure for release plan or DDL execution diagnostics."""

    def __init__(
        self,
        reason_code: str,
        detail: str,
        *,
        cause: BaseException | None = None,
        migration_order: int | None = None,
        object_id: str | None = None,
        transaction_stage: str | None = None,
        expected: dict[str, Any] | None = None,
        actual: dict[str, Any] | None = None,
    ) -> None:
        self.reason_code = reason_code
        self.cause = cause
        self.migration_order = migration_order
        self.object_id = object_id
        self.transaction_stage = transaction_stage
        self.expected = expected
        self.actual = actual
        super().__init__(f"{reason_code}: {detail}")

    def receipt_error(self) -> dict[str, Any]:
        return {
            "reason_code": self.reason_code,
            "exception_type": type(self.cause or self).__name__,
            "migration_order": self.migration_order,
            "object_id": self.object_id,
            "transaction_stage": self.transaction_stage,
            "expected": self.expected,
            "actual": self.actual,
        }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _validate_request(
    *, request: ReleaseSchemaPlanRequest, contract: ReleaseSchemaContract, config: DatabaseConnectionConfig
) -> None:
    if (
        request.release_schema_version != contract.release_schema_version
        or request.contract_content_hash != contract.contract_content_hash
    ):
        raise ReleaseSchemaApplyError(
            REASON_PLAN_CONTRACT_MISMATCH, "request contract identity does not match repository registry"
        )
    if request.ddl_session_policy_hash != contract.ddl_session_policy_hash:
        raise ReleaseSchemaApplyError(
            REASON_PLAN_CONTRACT_MISMATCH, "request DDL policy hash does not match repository registry"
        )
    if request.target_label is not config.target_label:
        raise ReleaseSchemaApplyError(
            REASON_DATABASE_IDENTITY_MISMATCH, "request target label does not match resolved env target"
        )


def _diagnostics(differences: Sequence[CatalogDifference]) -> tuple[dict[str, Any], ...]:
    return tuple(
        {
            "reason_code": item.reason_code,
            "object_id": item.object_id,
            "category": item.category,
        }
        for item in differences
    )


def _build_plan_model(payload: dict[str, Any]) -> ReleaseSchemaPlan:
    payload["plan_content_hash"] = canonical_json_sha256(payload)
    return ReleaseSchemaPlan.model_validate(payload)


def _phase1f1_month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def _phase1f1_capacity_months(*, history_start_trade_date: date, history_end_trade_date: date) -> tuple[date, ...]:
    if history_start_trade_date > history_end_trade_date:
        raise ReleaseSchemaApplyError(
            REASON_PHASE1F1_PREDECESSOR_SCHEMA_INVALID,
            "history start date must not follow history end date",
            transaction_stage="LEGACY_INVENTORY_CAPACITY",
        )
    months: list[date] = []
    current = _phase1f1_month_start(history_start_trade_date)
    final = _phase1f1_month_start(history_end_trade_date)
    while current <= final:
        months.append(current)
        current = date(current.year + 1, 1, 1) if current.month == 12 else date(current.year, current.month + 1, 1)
    return tuple(months)


def _phase1f1_predecessor_layout(cursor: Any) -> str:
    cursor.execute(
        """
        SELECT c.relname AS name, c.relkind AS relkind
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'app'
           AND c.relname = ANY(%s)
        """,
        (["advisory_signal_observation_lineage", "advisory_signal_stage_candidate"],),
    )
    rows = {str(row[0]): str(row[1]) for row in cursor.fetchall()}
    expected_names = {"advisory_signal_observation_lineage", "advisory_signal_stage_candidate"}
    if not rows:
        return "ABSENT"
    if set(rows) != expected_names:
        raise ReleaseSchemaApplyError(
            REASON_PHASE1F1_PREDECESSOR_SCHEMA_INVALID,
            "legacy lineage and stage-candidate relations must be both present or both absent",
            transaction_stage="LEGACY_INVENTORY_LAYOUT",
            actual={"relations": rows},
        )
    kinds = set(rows.values())
    if kinds == {"r"}:
        return "V1_TABLES"
    if kinds == {"v"}:
        return "V2_VIEWS"
    raise ReleaseSchemaApplyError(
        REASON_PHASE1F1_PREDECESSOR_SCHEMA_INVALID,
        "legacy relation names must be both ordinary tables or both compatibility views",
        transaction_stage="LEGACY_INVENTORY_LAYOUT",
        actual={"relations": rows},
    )


def _phase1f1_legacy_inventory_from_cursor(
    *, cursor: Any, request: ReleaseSchemaPlanRequest, treat_absent_as_empty_v1: bool = False
) -> Phase1F1LegacyMonthInventory:
    capacity_months = _phase1f1_capacity_months(
        history_start_trade_date=request.history_start_trade_date,
        history_end_trade_date=request.history_end_trade_date,
    )
    observed_layout = _phase1f1_predecessor_layout(cursor)
    predecessor_layout = observed_layout
    if observed_layout == "ABSENT" and treat_absent_as_empty_v1:
        # A fresh release plan runs the existing frozen foundation migrations before
        # the Phase 1F.1 cutover, which deterministically creates empty v1 tables.
        predecessor_layout = "V1_TABLES"
    lineage_row_count = 0
    candidate_row_count = 0
    legacy_months: tuple[date, ...] = ()
    if observed_layout == "V1_TABLES":
        cursor.execute(
            """
            WITH resolved AS (
                SELECT observation.decision_as_of_trade_date
                  FROM app.advisory_signal_observation_lineage lineage
                  LEFT JOIN app.advisory_signal_observation_version observation_version
                    ON observation_version.observation_version_id = lineage.observation_version_id
                  LEFT JOIN app.advisory_signal_observation observation
                    ON observation.canonical_signal_id = observation_version.canonical_signal_id
            )
            SELECT count(*)::bigint,
                   count(*) FILTER (WHERE decision_as_of_trade_date IS NULL)::bigint,
                   ARRAY(
                       SELECT DISTINCT date_trunc('month', decision_as_of_trade_date)::date
                         FROM resolved
                        WHERE decision_as_of_trade_date IS NOT NULL
                        ORDER BY 1
                   )
              FROM resolved
            """
        )
        lineage_count, lineage_unresolved, lineage_months = cursor.fetchone()
        cursor.execute(
            """
            WITH resolved AS (
                SELECT observation.decision_as_of_trade_date
                  FROM app.advisory_signal_stage_candidate candidate
                  LEFT JOIN app.advisory_signal_stage_evidence stage_evidence
                    ON stage_evidence.stage_evidence_id = candidate.stage_evidence_id
                  LEFT JOIN app.advisory_signal_observation_version observation_version
                    ON observation_version.observation_version_id = stage_evidence.observation_version_id
                  LEFT JOIN app.advisory_signal_observation observation
                    ON observation.canonical_signal_id = observation_version.canonical_signal_id
            )
            SELECT count(*)::bigint,
                   count(*) FILTER (WHERE decision_as_of_trade_date IS NULL)::bigint,
                   ARRAY(
                       SELECT DISTINCT date_trunc('month', decision_as_of_trade_date)::date
                         FROM resolved
                        WHERE decision_as_of_trade_date IS NOT NULL
                        ORDER BY 1
                   )
              FROM resolved
            """
        )
        candidate_count, candidate_unresolved, candidate_months = cursor.fetchone()
        if int(lineage_unresolved) or int(candidate_unresolved):
            raise ReleaseSchemaApplyError(
                REASON_PHASE1F1_PARENT_DATE_UNRESOLVED,
                "legacy lineage or candidate rows cannot resolve a canonical decision date",
                transaction_stage="LEGACY_INVENTORY_DATES",
                actual={
                    "lineage_unresolved": int(lineage_unresolved),
                    "candidate_unresolved": int(candidate_unresolved),
                },
            )
        lineage_row_count = int(lineage_count)
        candidate_row_count = int(candidate_count)
        legacy_months = tuple(sorted({*tuple(lineage_months or ()), *tuple(candidate_months or ())}))
    target_months = tuple(sorted({*capacity_months, *legacy_months}))
    payload: dict[str, Any] = {
        "schema_version": "advisory_phase1f1_legacy_month_inventory_v1",
        "predecessor_layout": predecessor_layout,
        "lineage_row_count": lineage_row_count,
        "candidate_row_count": candidate_row_count,
        "legacy_months": legacy_months,
        "target_months": target_months,
        "legacy_months_hash": canonical_json_sha256(legacy_months),
        "target_months_hash": canonical_json_sha256(target_months),
    }
    payload["legacy_inventory_hash"] = canonical_json_sha256(payload)
    return Phase1F1LegacyMonthInventory.model_validate(payload)


def _phase1f1_legacy_inventory(
    *, config: DatabaseConnectionConfig, contract: ReleaseSchemaContract, request: ReleaseSchemaPlanRequest
) -> Phase1F1LegacyMonthInventory | None:
    if contract.schema_version != CONTRACT_SCHEMA_VERSION_V2:
        return None
    with readonly_catalog_connection(config) as connection:
        with connection.cursor() as cursor:
            return _phase1f1_legacy_inventory_from_cursor(
                cursor=cursor,
                request=request,
                treat_absent_as_empty_v1=True,
            )


def build_release_schema_plan(
    *, config: DatabaseConnectionConfig, contract: ReleaseSchemaContract, request: ReleaseSchemaPlanRequest
) -> ReleaseSchemaPlan:
    """Build an immutable release plan from the current read-only catalog state."""

    _validate_request(request=request, contract=contract, config=config)
    legacy_inventory = _phase1f1_legacy_inventory(config=config, contract=contract, request=request)
    if legacy_inventory is None:
        expected_partitions = plan_month_partitions(
            partition_contract=contract.partition_contract,
            history_start_trade_date=request.history_start_trade_date,
            history_end_trade_date=request.history_end_trade_date,
        )
    else:
        expected_partitions = plan_month_partitions_for_contracts(
            partition_contracts=contract.partition_contracts,
            target_months=legacy_inventory.target_months,
        )
    verification = verify_database_catalog(config=config, contract=contract, expected_partitions=expected_partitions)
    pre_evidence = observed_managed_catalog_evidence(
        projection=verification.projection,
        contract=contract,
        expected_partitions=expected_partitions,
    )
    expected_final_evidence = expected_managed_catalog_evidence(
        contract=contract,
        expected_partitions=expected_partitions,
    )
    pending = _pending_operations(
        contract=contract,
        verification=verification,
        expected_partitions=expected_partitions,
    )
    payload: dict[str, Any] = {
        "schema_version": "advisory_phase1f_release_plan_v2"
        if contract.schema_version == CONTRACT_SCHEMA_VERSION_V2
        else "advisory_phase1f_release_plan_v1",
        "request": request.model_dump(mode="python"),
        "database_identity": verification.projection.database_identity.model_dump(mode="python"),
        "release_schema_version": contract.release_schema_version,
        "contract_content_hash": contract.contract_content_hash,
        "ddl_session_policy": contract.ddl_session_policy.model_dump(mode="python"),
        "ddl_session_policy_hash": contract.ddl_session_policy_hash,
        "ordered_migrations": [item.model_dump(mode="python") for item in contract.managed_migrations],
        "pre_catalog_fingerprint": pre_evidence.total_sha256,
        "pre_catalog_evidence": pre_evidence.model_dump(mode="python"),
        "expected_final_catalog_fingerprint": expected_final_evidence.total_sha256,
        "expected_final_catalog_evidence": expected_final_evidence.model_dump(mode="python"),
        "managed_schema_status": verification.managed_schema_status,
        "prerequisite_status": verification.prerequisite_status,
        "downstream_ready": verification.downstream_ready,
        "managed_differences": [item.model_dump(mode="python") for item in verification.managed_differences],
        "prerequisite_differences": [item.model_dump(mode="python") for item in verification.prerequisite_differences],
        "expected_partitions": [item.model_dump(mode="python") for item in expected_partitions],
        "legacy_inventory": legacy_inventory.model_dump(mode="python") if legacy_inventory is not None else None,
        "pending_ddl_operations": [item.model_dump(mode="python") for item in pending],
    }
    return _build_plan_model(payload)


def _pending_operations(
    *, contract: ReleaseSchemaContract, verification: CatalogVerification, expected_partitions: Sequence[MonthPartition]
) -> tuple[PendingDdlOperation, ...]:
    if verification.managed_schema_status in {ManagedSchemaStatus.DRIFTED, ManagedSchemaStatus.UNSUPPORTED}:
        return ()
    pending_orders: set[int] = set()
    if verification.managed_schema_status is ManagedSchemaStatus.ABSENT:
        pending_orders.update(item.order for item in contract.managed_migrations)
    else:
        known_orders = {item.order for item in contract.managed_migrations}
        for difference in verification.managed_differences:
            if difference.category not in {"MISSING", "DRIFTED", "UNEXPECTED"}:
                continue
            pending_orders.update(order for order in difference.repairable_by_orders if order in known_orders)
    # Replaying the source/capture base migration replaces two functions later
    # migrations intentionally supersede, so retain the final frozen versions.
    if 10 in pending_orders:
        pending_orders.add(20)
    if 30 in pending_orders:
        pending_orders.add(40)
    operations = [PendingDdlOperation(kind="MIGRATION", migration_order=order) for order in sorted(pending_orders)]
    missing_partition_ids = {
        difference.object_id
        for difference in verification.managed_differences
        if difference.category == "MISSING" and difference.object_id.startswith("partition:")
    }
    for partition in expected_partitions:
        if partition.object_id in missing_partition_ids:
            operations.append(
                PendingDdlOperation(
                    kind="PARTITION",
                    partition_name=partition.name,
                    lower_bound=partition.lower_bound,
                    upper_bound=partition.upper_bound,
                )
            )
    return tuple(operations)


def _database_clock(config: DatabaseConnectionConfig) -> datetime:
    with readonly_catalog_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT clock_timestamp()")
            row = cursor.fetchone()
    if row is None:
        raise ReleaseSchemaApplyError(REASON_DDL_EXECUTION_FAILED, "database clock query returned no row")
    return row[0]


def _try_database_clock(config: DatabaseConnectionConfig) -> datetime | None:
    try:
        return _database_clock(config)
    except Exception as exc:
        LOGGER.error(
            "phase1f database clock unavailable target=%s exception_type=%s",
            config.target_label.value,
            type(exc).__name__,
            exc_info=True,
        )
        return None


def _identity_matches(left: DatabaseIdentity, right: DatabaseIdentity) -> bool:
    return left.model_dump(mode="python") == right.model_dump(mode="python")


def _revalidate_plan(
    *, plan: ReleaseSchemaPlan, config: DatabaseConnectionConfig, contract: ReleaseSchemaContract
) -> ReleaseSchemaPlan:
    if (
        plan.contract_content_hash != contract.contract_content_hash
        or plan.release_schema_version != contract.release_schema_version
    ):
        raise ReleaseSchemaApplyError(REASON_PLAN_CONTRACT_MISMATCH, "plan contract identity does not match registry")
    _validate_request(request=plan.request, contract=contract, config=config)
    current = build_release_schema_plan(config=config, contract=contract, request=plan.request)
    if not _identity_matches(plan.database_identity, current.database_identity):
        raise ReleaseSchemaApplyError(
            REASON_DATABASE_IDENTITY_MISMATCH, "database identity changed since plan creation"
        )
    if plan.pre_catalog_fingerprint != current.pre_catalog_fingerprint:
        raise ReleaseSchemaApplyError(REASON_PLAN_STALE, "catalog fingerprint changed since plan creation")
    if plan.plan_content_hash != current.plan_content_hash:
        raise ReleaseSchemaApplyError(REASON_PLAN_STALE, "recomputed release plan differs from supplied plan")
    return current


def _load_frozen_migration(migration: ManagedMigration) -> bytes:
    if migration.relative_path is None or migration.file_sha256 is None:
        raise ReleaseSchemaApplyError(
            REASON_PLAN_CONTRACT_MISMATCH,
            f"migration order {migration.order} does not declare a frozen SQL source",
            migration_order=migration.order,
            transaction_stage="LOAD_MIGRATION",
        )
    root = _repo_root()
    source = (root / migration.relative_path).resolve()
    try:
        source.relative_to(root)
    except ValueError as exc:
        raise ReleaseSchemaApplyError(REASON_MIGRATION_FILE_MISSING, "migration path escapes repository") from exc
    if not source.is_file():
        raise ReleaseSchemaApplyError(
            REASON_MIGRATION_FILE_MISSING,
            f"frozen migration file is unavailable at order {migration.order}",
            migration_order=migration.order,
            transaction_stage="LOAD_MIGRATION",
        )
    content = source.read_bytes()
    if hashlib.sha256(content).hexdigest() != migration.file_sha256:
        raise ReleaseSchemaApplyError(
            REASON_MIGRATION_HASH_MISMATCH,
            f"frozen migration hash mismatch at order {migration.order}",
            migration_order=migration.order,
            transaction_stage="LOAD_MIGRATION",
            expected={"file_sha256": migration.file_sha256},
            actual={"file_sha256": hashlib.sha256(content).hexdigest()},
        )
    try:
        content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReleaseSchemaApplyError(
            REASON_MIGRATION_HASH_MISMATCH,
            f"migration is not UTF-8 at order {migration.order}",
            cause=exc,
            migration_order=migration.order,
            transaction_stage="LOAD_MIGRATION",
        ) from exc
    return content


def _timeout_reason(exc: BaseException) -> str:
    pgcode = getattr(exc, "pgcode", None)
    detail = str(exc).lower()
    if pgcode == "55P03" or "lock timeout" in detail:
        return REASON_DDL_LOCK_TIMEOUT
    if pgcode == "57014" or "statement timeout" in detail:
        return REASON_DDL_STATEMENT_TIMEOUT
    for reason in (
        REASON_PHASE1F1_PREDECESSOR_SCHEMA_INVALID,
        REASON_PHASE1F1_PARENT_DATE_UNRESOLVED,
        REASON_PHASE1F1_COPY_MISMATCH,
        REASON_PHASE1F1_VIEW_CONTRACT_MISMATCH,
        REASON_PHASE1F1_PARTITION_MISSING,
    ):
        if reason.lower() in detail:
            return reason
    return REASON_DDL_EXECUTION_FAILED


@contextmanager
def _writable_connection(config: DatabaseConnectionConfig, *, autocommit: bool) -> Iterator[Any]:
    try:
        connection = psycopg2.connect(**config.connect_kwargs())
    except Exception as exc:  # pragma: no cover - runtime environment dependent.
        raise ReleaseSchemaApplyError(REASON_DDL_EXECUTION_FAILED, type(exc).__name__, cause=exc) from exc
    connection.autocommit = autocommit
    try:
        yield connection
    finally:
        connection.close()


def _set_session_policy(cursor: Any, *, contract: ReleaseSchemaContract, local: bool) -> None:
    keyword = "SET LOCAL" if local else "SET"
    cursor.execute(f"{keyword} lock_timeout = %s", (f"{contract.ddl_session_policy.lock_timeout_ms}ms",))
    cursor.execute(f"{keyword} statement_timeout = %s", (f"{contract.ddl_session_policy.statement_timeout_ms}ms",))


def _subset_differences(
    verification: CatalogVerification, migration: ManagedMigration
) -> tuple[CatalogDifference, ...]:
    selected = set(migration.declared_object_ids)
    partition_parents = set(migration.partition_parent_relations)
    return tuple(
        item
        for item in verification.managed_differences
        if item.object_id in selected
        or (
            item.object_id.startswith("partition:")
            and item.expected is not None
            and f"{item.expected.get('parent_schema')}.{item.expected.get('parent_relation')}" in partition_parents
        )
    )


def _execute_executor_managed_migration(
    *,
    config: DatabaseConnectionConfig,
    contract: ReleaseSchemaContract,
    migration: ManagedMigration,
    expected_partitions: Sequence[MonthPartition],
) -> None:
    source = _load_frozen_migration(migration)
    try:
        text = source.decode("utf-8")
        with _writable_connection(config, autocommit=False) as connection:
            try:
                with connection.cursor() as cursor:
                    _set_session_policy(cursor, contract=contract, local=True)
                    cursor.execute(text)
                in_transaction = verify_catalog(
                    connection=connection,
                    config=config,
                    contract=contract,
                    expected_partitions=expected_partitions,
                )
                subset_differences = _subset_differences(in_transaction, migration)
                if subset_differences:
                    difference = subset_differences[0]
                    raise ReleaseSchemaApplyError(
                        REASON_TRANSACTION_VERIFY_FAILED,
                        f"subset catalog verification failed before commit at order {migration.order}",
                        migration_order=migration.order,
                        object_id=difference.object_id,
                        transaction_stage="PRE_COMMIT_SUBSET_VERIFY",
                        expected=difference.expected,
                        actual=difference.actual,
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
    except ReleaseSchemaApplyError:
        raise
    except BaseException as exc:
        raise ReleaseSchemaApplyError(
            _timeout_reason(exc),
            f"migration order {migration.order} failed",
            cause=exc,
            migration_order=migration.order,
            transaction_stage="EXECUTOR_MANAGED_DDL",
        ) from exc


def _phase1f1_bind_cutover_inventory(
    *, cursor: Any, expected: Phase1F1LegacyMonthInventory, request: ReleaseSchemaPlanRequest
) -> None:
    cursor.execute(
        "LOCK TABLE app.advisory_signal_observation_lineage, app.advisory_signal_stage_candidate, "
        "app.advisory_signal_stage_evidence IN ACCESS EXCLUSIVE MODE"
    )
    actual = _phase1f1_legacy_inventory_from_cursor(cursor=cursor, request=request)
    if actual.predecessor_layout != "V1_TABLES":
        raise ReleaseSchemaApplyError(
            REASON_PHASE1F1_PREDECESSOR_SCHEMA_INVALID,
            "cutover requires the exact v1 physical predecessor tables",
            transaction_stage="CUTOVER_PREDECESSOR_LAYOUT",
            expected={"predecessor_layout": "V1_TABLES"},
            actual={"predecessor_layout": actual.predecessor_layout},
        )
    if actual.legacy_inventory_hash != expected.legacy_inventory_hash:
        raise ReleaseSchemaApplyError(
            REASON_PLAN_STALE,
            "legacy inventory changed after the plan was frozen",
            transaction_stage="CUTOVER_LEGACY_INVENTORY",
            expected={"legacy_inventory_hash": expected.legacy_inventory_hash},
            actual={"legacy_inventory_hash": actual.legacy_inventory_hash},
        )
    values = {
        "app.phase1f1_legacy_inventory_hash": expected.legacy_inventory_hash,
        "app.phase1f1_lineage_row_count": str(expected.lineage_row_count),
        "app.phase1f1_candidate_row_count": str(expected.candidate_row_count),
        "app.phase1f1_legacy_months": ",".join(item.isoformat() for item in expected.legacy_months),
        "app.phase1f1_target_months": ",".join(item.isoformat() for item in expected.target_months),
    }
    for name, value in values.items():
        cursor.execute("SELECT set_config(%s, %s, true)", (name, value))


def _execute_phase1f1_cutover_migration(
    *,
    config: DatabaseConnectionConfig,
    contract: ReleaseSchemaContract,
    migration: ManagedMigration,
    expected_partitions: Sequence[MonthPartition],
    legacy_inventory: Phase1F1LegacyMonthInventory | None,
    request: ReleaseSchemaPlanRequest,
) -> None:
    if legacy_inventory is None:
        raise ReleaseSchemaApplyError(
            REASON_PLAN_CONTRACT_MISMATCH,
            "v2 cutover requires frozen legacy inventory",
            migration_order=migration.order,
            transaction_stage="CUTOVER_PRECONDITION",
        )
    source = _load_frozen_migration(migration)
    try:
        text = source.decode("utf-8")
        with _writable_connection(config, autocommit=False) as connection:
            try:
                with connection.cursor() as cursor:
                    _set_session_policy(cursor, contract=contract, local=True)
                    _phase1f1_bind_cutover_inventory(cursor=cursor, expected=legacy_inventory, request=request)
                    cursor.execute(text)
                in_transaction = verify_catalog(
                    connection=connection,
                    config=config,
                    contract=contract,
                    expected_partitions=expected_partitions,
                )
                subset_differences = _subset_differences(in_transaction, migration)
                if subset_differences:
                    difference = subset_differences[0]
                    reason_code = (
                        REASON_PHASE1F1_VIEW_CONTRACT_MISMATCH
                        if difference.object_id.startswith(
                            (
                                "relation:app.advisory_signal_observation_lineage",
                                "relation:app.advisory_signal_stage_candidate",
                                "column:app.advisory_signal_observation_lineage",
                                "column:app.advisory_signal_stage_candidate",
                            )
                        )
                        else REASON_TRANSACTION_VERIFY_FAILED
                    )
                    raise ReleaseSchemaApplyError(
                        reason_code,
                        f"subset catalog verification failed before commit at order {migration.order}",
                        migration_order=migration.order,
                        object_id=difference.object_id,
                        transaction_stage="CUTOVER_PRE_COMMIT_VERIFY",
                        expected=difference.expected,
                        actual=difference.actual,
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
    except ReleaseSchemaApplyError:
        raise
    except BaseException as exc:
        raise ReleaseSchemaApplyError(
            _timeout_reason(exc),
            f"migration order {migration.order} failed",
            cause=exc,
            migration_order=migration.order,
            transaction_stage="CUTOVER_DDL",
        ) from exc


def _execute_file_wrapped_migration(
    *, config: DatabaseConnectionConfig, contract: ReleaseSchemaContract, migration: ManagedMigration
) -> None:
    source = _load_frozen_migration(migration)
    try:
        text = source.decode("utf-8")
        with _writable_connection(config, autocommit=True) as connection:
            with connection.cursor() as cursor:
                _set_session_policy(cursor, contract=contract, local=False)
                # Do not rewrite BEGIN/COMMIT or function bodies.  psycopg2 accepts
                # Unicode SQL, so this is the exact UTF-8 decode of frozen bytes.
                cursor.execute(text)
    except ReleaseSchemaApplyError:
        raise
    except BaseException as exc:
        raise ReleaseSchemaApplyError(
            _timeout_reason(exc),
            f"migration order {migration.order} failed",
            cause=exc,
            migration_order=migration.order,
            transaction_stage="FILE_WRAPPED_DDL",
        ) from exc


def _execute_partitions(
    *, config: DatabaseConnectionConfig, contract: ReleaseSchemaContract, partitions: Sequence[MonthPartition]
) -> None:
    if not partitions:
        return
    try:
        with _writable_connection(config, autocommit=False) as connection:
            try:
                with connection.cursor() as cursor:
                    _set_session_policy(cursor, contract=contract, local=True)
                    for partition in partitions:
                        statement = psycopg2.sql.SQL(
                            "CREATE TABLE IF NOT EXISTS {}.{} PARTITION OF {}.{} FOR VALUES FROM ({}) TO ({})"
                        ).format(
                            psycopg2.sql.Identifier(partition.schema),
                            psycopg2.sql.Identifier(partition.name),
                            psycopg2.sql.Identifier(partition.schema),
                            psycopg2.sql.Identifier(partition.parent_relation),
                            psycopg2.sql.Literal(partition.lower_bound.isoformat()),
                            psycopg2.sql.Literal(partition.upper_bound.isoformat()),
                        )
                        cursor.execute(statement)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
    except ReleaseSchemaApplyError:
        raise
    except BaseException as exc:
        raise ReleaseSchemaApplyError(
            _timeout_reason(exc),
            "partition creation failed",
            cause=exc,
            transaction_stage="PARTITION_DDL",
        ) from exc


def _receipt(
    *,
    operation: RequestedOperation,
    requested_operation: RequestedOperation,
    identity: DatabaseIdentity,
    request_hash: str,
    plan_hash: str | None,
    contract_hash: str,
    pre_fingerprint: str | None,
    pre_evidence: CatalogFingerprintEvidence | None,
    executed_migration_hashes: Sequence[str],
    migration_results: Sequence[MigrationExecutionResult],
    executed_partitions: Sequence[MonthPartition],
    post_fingerprint: str | None,
    post_evidence: CatalogFingerprintEvidence | None,
    operation_status: OperationStatus,
    managed_status: ManagedSchemaStatus,
    prerequisite_status: PrerequisiteStatus,
    managed_differences: Sequence[CatalogDifference],
    prerequisite_differences: Sequence[CatalogDifference],
    legacy_inventory: Phase1F1LegacyMonthInventory | None,
    errors: Sequence[dict[str, Any]],
    started_at: datetime,
    finished_at: datetime,
    ddl_executed: bool,
) -> ReleaseSchemaReceipt:
    payload: dict[str, Any] = {
        "schema_version": "advisory_phase1f_release_receipt_v2"
        if legacy_inventory is not None
        else "advisory_phase1f_release_receipt_v1",
        "operation": operation,
        "requested_operation": requested_operation,
        "database_identity": identity.model_dump(mode="python"),
        "request_content_hash": request_hash,
        "plan_content_hash": plan_hash,
        "contract_content_hash": contract_hash,
        "pre_catalog_fingerprint": pre_fingerprint,
        "pre_catalog_evidence": pre_evidence.model_dump(mode="python") if pre_evidence is not None else None,
        "executed_migration_hashes": list(executed_migration_hashes),
        "per_migration_results": [item.model_dump(mode="python") for item in migration_results],
        "executed_partitions": [item.model_dump(mode="python") for item in executed_partitions],
        "post_catalog_fingerprint": post_fingerprint,
        "post_catalog_evidence": post_evidence.model_dump(mode="python") if post_evidence is not None else None,
        "operation_status": operation_status,
        "managed_schema_status": managed_status,
        "prerequisite_status": prerequisite_status,
        "downstream_ready": managed_status is ManagedSchemaStatus.COMPATIBLE
        and prerequisite_status is PrerequisiteStatus.COMPATIBLE,
        "managed_differences": [item.model_dump(mode="python") for item in managed_differences],
        "prerequisite_differences": [item.model_dump(mode="python") for item in prerequisite_differences],
        "legacy_inventory": legacy_inventory.model_dump(mode="python") if legacy_inventory is not None else None,
        "diagnostics": list(_diagnostics(prerequisite_differences)),
        "errors": list(errors),
        "started_at": started_at,
        "finished_at": finished_at,
        "ddl_executed": ddl_executed,
        "dml_executed": False,
        "runtime_activated": False,
    }
    payload["receipt_content_hash"] = canonical_json_sha256(payload)
    return ReleaseSchemaReceipt.model_validate(payload)


def plan_release_schema(
    *, config: DatabaseConnectionConfig, contract: ReleaseSchemaContract, request: ReleaseSchemaPlanRequest
) -> tuple[ReleaseSchemaPlan, ReleaseSchemaReceipt]:
    """Create a read-only plan and its immutable operation receipt."""

    started = _database_clock(config)
    plan = build_release_schema_plan(config=config, contract=contract, request=request)
    finished = _database_clock(config)
    receipt = _receipt(
        operation=RequestedOperation.PLAN,
        requested_operation=plan.request.requested_operation,
        identity=plan.database_identity,
        request_hash=plan.request.request_content_hash,
        plan_hash=plan.plan_content_hash,
        contract_hash=contract.contract_content_hash,
        pre_fingerprint=plan.pre_catalog_fingerprint,
        pre_evidence=plan.pre_catalog_evidence,
        executed_migration_hashes=(),
        migration_results=(),
        executed_partitions=(),
        post_fingerprint=plan.pre_catalog_fingerprint,
        post_evidence=plan.pre_catalog_evidence,
        operation_status=OperationStatus.SUCCESS,
        managed_status=plan.managed_schema_status,
        prerequisite_status=plan.prerequisite_status,
        managed_differences=plan.managed_differences,
        prerequisite_differences=plan.prerequisite_differences,
        legacy_inventory=plan.legacy_inventory,
        errors=(),
        started_at=started,
        finished_at=finished,
        ddl_executed=False,
    )
    return plan, receipt


def verify_release_schema_plan(
    *, plan: ReleaseSchemaPlan, config: DatabaseConnectionConfig, contract: ReleaseSchemaContract
) -> ReleaseSchemaReceipt:
    """Read-only verify one frozen plan without executing DDL."""

    started = _database_clock(config)
    current = _revalidate_plan(plan=plan, config=config, contract=contract)
    if current.request.requested_operation is not RequestedOperation.VERIFY:
        raise ReleaseSchemaApplyError(
            REASON_PLAN_CONTRACT_MISMATCH, "verify requires a plan explicitly bound to VERIFY"
        )
    verification = verify_database_catalog(
        config=config,
        contract=contract,
        expected_partitions=current.expected_partitions,
    )
    finished = _database_clock(config)
    post_evidence = observed_managed_catalog_evidence(
        projection=verification.projection,
        contract=contract,
        expected_partitions=current.expected_partitions,
    )
    status = (
        OperationStatus.SUCCESS
        if verification.managed_schema_status is not ManagedSchemaStatus.UNSUPPORTED
        else OperationStatus.FAILED
    )
    errors: tuple[dict[str, Any], ...] = ()
    if status is OperationStatus.FAILED:
        errors = ({"reason_code": REASON_POSTGRES_VERSION_UNSUPPORTED, "exception_type": "UnsupportedPostgres"},)
    return _receipt(
        operation=RequestedOperation.VERIFY,
        requested_operation=current.request.requested_operation,
        identity=verification.projection.database_identity,
        request_hash=current.request.request_content_hash,
        plan_hash=current.plan_content_hash,
        contract_hash=contract.contract_content_hash,
        pre_fingerprint=current.pre_catalog_fingerprint,
        pre_evidence=current.pre_catalog_evidence,
        executed_migration_hashes=(),
        migration_results=(),
        executed_partitions=(),
        post_fingerprint=post_evidence.total_sha256,
        post_evidence=post_evidence,
        operation_status=status,
        managed_status=verification.managed_schema_status,
        prerequisite_status=verification.prerequisite_status,
        managed_differences=verification.managed_differences,
        prerequisite_differences=verification.prerequisite_differences,
        legacy_inventory=current.legacy_inventory,
        errors=errors,
        started_at=started,
        finished_at=finished,
        ddl_executed=False,
    )


def apply_release_schema_plan(
    *, plan: ReleaseSchemaPlan, config: DatabaseConnectionConfig, contract: ReleaseSchemaContract
) -> ReleaseSchemaReceipt:
    """Apply only the frozen additive plan and emit an exact success/failure receipt."""

    started = _database_clock(config)
    current = _revalidate_plan(plan=plan, config=config, contract=contract)
    if current.request.requested_operation is not RequestedOperation.APPLY:
        raise ReleaseSchemaApplyError(REASON_PLAN_CONTRACT_MISMATCH, "apply requires a plan explicitly bound to APPLY")
    if current.managed_schema_status in {ManagedSchemaStatus.DRIFTED, ManagedSchemaStatus.UNSUPPORTED}:
        difference = current.managed_differences[0] if current.managed_differences else None
        raise ReleaseSchemaApplyError(
            REASON_PHASE1F1_CATALOG_DRIFTED
            if contract.schema_version == CONTRACT_SCHEMA_VERSION_V2
            else REASON_SCHEMA_DRIFTED,
            "drifted or unsupported managed schema cannot be applied",
            object_id=difference.object_id if difference is not None else None,
            transaction_stage="PREFLIGHT_CATALOG",
            expected=difference.expected if difference is not None else None,
            actual=difference.actual if difference is not None else None,
        )
    pending_migration_orders = {
        item.migration_order
        for item in current.pending_ddl_operations
        if item.kind == "MIGRATION" and item.migration_order is not None
    }
    requested_partition_keys = {
        (str(item.partition_name), item.lower_bound, item.upper_bound)
        for item in current.pending_ddl_operations
        if item.kind == "PARTITION"
        and item.partition_name is not None
        and item.lower_bound is not None
        and item.upper_bound is not None
    }
    pending_partitions = [
        item
        for item in current.expected_partitions
        if (item.name, item.lower_bound, item.upper_bound) in requested_partition_keys
    ]
    if len(pending_partitions) != len(requested_partition_keys):
        raise ReleaseSchemaApplyError(
            REASON_PLAN_CONTRACT_MISMATCH,
            "plan contains a partition operation not declared by the frozen partition contract",
            transaction_stage="PREFLIGHT_PARTITION_PLAN",
        )
    results: list[MigrationExecutionResult] = []
    executed_hashes: list[str] = []
    executed_partitions: list[MonthPartition] = []
    remaining_partitions = list(pending_partitions)
    ddl_executed = False
    last_verification = verify_database_catalog(
        config=config,
        contract=contract,
        expected_partitions=current.expected_partitions,
    )
    try:
        for migration in contract.managed_migrations:
            pre_subset = subset_catalog_fingerprint(
                verification=last_verification, object_ids=migration.declared_object_ids
            )
            if migration.order not in pending_migration_orders:
                now = _database_clock(config)
                results.append(
                    MigrationExecutionResult(
                        order=migration.order,
                        transaction_mode=migration.transaction_mode,
                        status=MigrationExecutionStatus.NOT_NEEDED,
                        pre_subset_fingerprint=pre_subset,
                        post_subset_fingerprint=pre_subset,
                        started_at=now,
                        finished_at=now,
                    )
                )
                continue
            migration_started = _database_clock(config)
            LOGGER.info(
                "phase1f release migration started target=%s order=%s", config.target_label.value, migration.order
            )
            committed = False
            try:
                if migration.executor_action is ExecutorAction.CREATE_PARTITIONS:
                    parent_keys = set(migration.partition_parent_relations)
                    migration_partitions = [
                        item for item in remaining_partitions if f"{item.schema}.{item.parent_relation}" in parent_keys
                    ]
                    if not migration_partitions:
                        raise ReleaseSchemaApplyError(
                            REASON_PHASE1F1_PARTITION_MISSING,
                            f"migration order {migration.order} was pending without declared partitions",
                            migration_order=migration.order,
                            transaction_stage="PARTITION_PRECONDITION",
                        )
                    _execute_partitions(config=config, contract=contract, partitions=migration_partitions)
                    executed_partitions.extend(migration_partitions)
                    remaining_partitions = [item for item in remaining_partitions if item not in migration_partitions]
                elif migration.executor_action is ExecutorAction.CUTOVER:
                    _execute_phase1f1_cutover_migration(
                        config=config,
                        contract=contract,
                        migration=migration,
                        expected_partitions=current.expected_partitions,
                        legacy_inventory=current.legacy_inventory,
                        request=current.request,
                    )
                elif migration.transaction_mode is TransactionMode.EXECUTOR_MANAGED:
                    _execute_executor_managed_migration(
                        config=config,
                        contract=contract,
                        migration=migration,
                        expected_partitions=current.expected_partitions,
                    )
                else:
                    _execute_file_wrapped_migration(config=config, contract=contract, migration=migration)
                committed = True
                if migration.file_sha256 is not None:
                    executed_hashes.append(migration.file_sha256)
                ddl_executed = True
                results.append(
                    MigrationExecutionResult(
                        order=migration.order,
                        transaction_mode=migration.transaction_mode,
                        status=MigrationExecutionStatus.COMMITTED,
                        pre_subset_fingerprint=pre_subset,
                        started_at=migration_started,
                    )
                )
                LOGGER.info(
                    "phase1f release migration committed target=%s order=%s", config.target_label.value, migration.order
                )
                last_verification = verify_database_catalog(
                    config=config,
                    contract=contract,
                    expected_partitions=current.expected_partitions,
                )
                subset_differences = _subset_differences(last_verification, migration)
                if subset_differences:
                    difference = subset_differences[0]
                    raise ReleaseSchemaApplyError(
                        REASON_PHASE1F1_POST_COMMIT_VERIFY_FAILED
                        if contract.schema_version == CONTRACT_SCHEMA_VERSION_V2
                        else REASON_POST_COMMIT_VERIFY_FAILED,
                        f"subset catalog readback failed at order {migration.order}",
                        migration_order=migration.order,
                        object_id=difference.object_id,
                        transaction_stage="POST_COMMIT_SUBSET_READBACK",
                        expected=difference.expected,
                        actual=difference.actual,
                    )
                migration_finished = _database_clock(config)
                results[-1] = results[-1].model_copy(
                    update={
                        "post_subset_fingerprint": subset_catalog_fingerprint(
                            verification=last_verification,
                            object_ids=migration.declared_object_ids,
                        ),
                        "finished_at": migration_finished,
                    }
                )
            except BaseException as exc:
                failure = (
                    exc
                    if isinstance(exc, ReleaseSchemaApplyError)
                    else ReleaseSchemaApplyError(
                        _timeout_reason(exc),
                        f"migration order {migration.order} failed",
                        cause=exc,
                        migration_order=migration.order,
                        transaction_stage="POST_COMMIT_SUBSET_READBACK" if committed else "MIGRATION_DDL",
                    )
                )
                LOGGER.error(
                    "phase1f release migration failed target=%s order=%s reason=%s exception_type=%s",
                    config.target_label.value,
                    migration.order,
                    failure.reason_code,
                    type(failure.cause or failure).__name__,
                )
                migration_finished = _try_database_clock(config)
                if committed:
                    results[-1] = results[-1].model_copy(
                        update={
                            "finished_at": migration_finished,
                            "error_code": failure.reason_code,
                            "error_type": type(failure.cause or failure).__name__,
                        }
                    )
                else:
                    results.append(
                        MigrationExecutionResult(
                            order=migration.order,
                            transaction_mode=migration.transaction_mode,
                            status=MigrationExecutionStatus.FAILED,
                            pre_subset_fingerprint=pre_subset,
                            started_at=migration_started,
                            finished_at=migration_finished,
                            error_code=failure.reason_code,
                            error_type=type(failure.cause or failure).__name__,
                        )
                    )
                raise failure
        if remaining_partitions:
            LOGGER.info(
                "phase1f release partitions started target=%s count=%s",
                config.target_label.value,
                len(remaining_partitions),
            )
            _execute_partitions(config=config, contract=contract, partitions=remaining_partitions)
            executed_partitions.extend(remaining_partitions)
            ddl_executed = True
            last_verification = verify_database_catalog(
                config=config,
                contract=contract,
                expected_partitions=current.expected_partitions,
            )
        final_verification = verify_database_catalog(
            config=config,
            contract=contract,
            expected_partitions=current.expected_partitions,
        )
        if final_verification.managed_schema_status is not ManagedSchemaStatus.COMPATIBLE:
            difference = final_verification.managed_differences[0] if final_verification.managed_differences else None
            raise ReleaseSchemaApplyError(
                REASON_PHASE1F1_POST_COMMIT_VERIFY_FAILED
                if contract.schema_version == CONTRACT_SCHEMA_VERSION_V2
                else REASON_POST_COMMIT_VERIFY_FAILED,
                "full catalog verification is not compatible after apply",
                object_id=difference.object_id if difference is not None else None,
                transaction_stage="FINAL_CATALOG_READBACK",
                expected=difference.expected if difference is not None else None,
                actual=difference.actual if difference is not None else None,
            )
        post_evidence = observed_managed_catalog_evidence(
            projection=final_verification.projection,
            contract=contract,
            expected_partitions=current.expected_partitions,
        )
        if post_evidence.total_sha256 != current.expected_final_catalog_fingerprint:
            raise ReleaseSchemaApplyError(
                REASON_PHASE1F1_POST_COMMIT_VERIFY_FAILED
                if contract.schema_version == CONTRACT_SCHEMA_VERSION_V2
                else REASON_POST_COMMIT_VERIFY_FAILED,
                "post-apply catalog fingerprint differs from contract",
                transaction_stage="FINAL_CATALOG_FINGERPRINT",
                expected={"catalog_fingerprint": current.expected_final_catalog_fingerprint},
                actual={"catalog_fingerprint": post_evidence.total_sha256},
            )
        finished = _database_clock(config)
        return _receipt(
            operation=RequestedOperation.APPLY,
            requested_operation=current.request.requested_operation,
            identity=final_verification.projection.database_identity,
            request_hash=current.request.request_content_hash,
            plan_hash=current.plan_content_hash,
            contract_hash=contract.contract_content_hash,
            pre_fingerprint=current.pre_catalog_fingerprint,
            pre_evidence=current.pre_catalog_evidence,
            executed_migration_hashes=executed_hashes,
            migration_results=results,
            executed_partitions=executed_partitions,
            post_fingerprint=post_evidence.total_sha256,
            post_evidence=post_evidence,
            operation_status=OperationStatus.SUCCESS,
            managed_status=final_verification.managed_schema_status,
            prerequisite_status=final_verification.prerequisite_status,
            managed_differences=final_verification.managed_differences,
            prerequisite_differences=final_verification.prerequisite_differences,
            legacy_inventory=current.legacy_inventory,
            errors=(),
            started_at=started,
            finished_at=finished,
            ddl_executed=ddl_executed,
        )
    except ReleaseSchemaApplyError as failure:
        LOGGER.error(
            "phase1f release apply failed target=%s database=%s contract_hash=%s plan_hash=%s reason=%s "
            "migration_order=%s object_id=%s stage=%s expected=%s actual=%s exception_type=%s",
            config.target_label.value,
            current.database_identity.current_database,
            contract.contract_content_hash,
            current.plan_content_hash,
            failure.reason_code,
            failure.migration_order,
            failure.object_id,
            failure.transaction_stage,
            failure.expected,
            failure.actual,
            type(failure.cause or failure).__name__,
            exc_info=True,
        )
        receipt_errors: list[dict[str, Any]] = [failure.receipt_error()]
        try:
            final_verification = verify_database_catalog(
                config=config,
                contract=contract,
                expected_partitions=current.expected_partitions,
            )
            post_evidence = observed_managed_catalog_evidence(
                projection=final_verification.projection,
                contract=contract,
                expected_partitions=current.expected_partitions,
            )
        except Exception as readback_error:
            LOGGER.error(
                "phase1f post-failure catalog readback failed target=%s database=%s exception_type=%s",
                config.target_label.value,
                current.database_identity.current_database,
                type(readback_error).__name__,
                exc_info=True,
            )
            receipt_errors.append(
                {
                    "reason_code": REASON_PHASE1F1_POST_FAILURE_VERIFY_FAILED,
                    "exception_type": type(readback_error).__name__,
                    "migration_order": failure.migration_order,
                    "object_id": failure.object_id,
                    "transaction_stage": "POST_FAILURE_CATALOG_READBACK",
                    "expected": None,
                    "actual": None,
                }
            )
            final_verification = last_verification
            post_evidence = None
        finished = _try_database_clock(config) or started
        return _receipt(
            operation=RequestedOperation.APPLY,
            requested_operation=current.request.requested_operation,
            identity=final_verification.projection.database_identity,
            request_hash=current.request.request_content_hash,
            plan_hash=current.plan_content_hash,
            contract_hash=contract.contract_content_hash,
            pre_fingerprint=current.pre_catalog_fingerprint,
            pre_evidence=current.pre_catalog_evidence,
            executed_migration_hashes=executed_hashes,
            migration_results=results,
            executed_partitions=executed_partitions,
            post_fingerprint=post_evidence.total_sha256 if post_evidence is not None else None,
            post_evidence=post_evidence,
            operation_status=OperationStatus.FAILED,
            managed_status=final_verification.managed_schema_status,
            prerequisite_status=final_verification.prerequisite_status,
            managed_differences=final_verification.managed_differences,
            prerequisite_differences=final_verification.prerequisite_differences,
            legacy_inventory=current.legacy_inventory,
            errors=tuple(receipt_errors),
            started_at=started,
            finished_at=finished,
            ddl_executed=ddl_executed,
        )
