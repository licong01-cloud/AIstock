"""Exact DEV StrategyPackage import planning and one-transaction execution."""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg2
import psycopg2.extras

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.phase1g_schema_guard import Phase1GReleaseSchemaGuard
from backend.services.advisory_phase1.release_schema_contract import DatabaseIdentity, TargetLabel
from backend.services.advisory_phase1.release_schema_verify_postgres import (
    DatabaseConnectionConfig,
    ReleaseSchemaVerificationError,
    resolve_database_connection,
)

from .contracts import (
    EvidenceKind,
    ImportCommitOutcome,
    ImportPlanStatus,
    ImportRowDisposition,
    ImportWriteOperation,
    OnboardingArtifactRef,
    PlannedImportRow,
    PortableAdvisoryEvidenceBundle,
    PortableRelationRowSet,
    RealDevImportPlan,
    RealDevImportReceipt,
    RealDevOnboardingError,
    REASON_DATABASE_CONNECTION_FAILED,
    REASON_ENV_INVALID,
    REASON_IMPORT_COMMIT_NOT_OBSERVED,
    REASON_IMPORT_COMMIT_STATE_UNKNOWN,
    REASON_IMPORT_PLAN_CONFLICT,
    REASON_IMPORT_PLAN_INVALID,
    REASON_IMPORT_READBACK_FAILED,
    REASON_IMPORT_TRANSACTION_FAILED,
    REASON_RELEASE_RECEIPT_INVALID,
    database_identity_hash,
    deserialize_postgres_value,
    serialize_postgres_value,
    validate_sha256,
)
from .production_projection import (
    PACKAGE_ASSET_SEMANTIC_COLUMNS,
    PACKAGE_SEMANTIC_COLUMNS,
    FixedReadOnlyProjection,
    _assert_no_reparse_from_root,
    load_exact_release_receipt,
    readonly_onboarding_connection,
)
from .store import RealDevOnboardingEvidenceStore, resolve_package_asset_roots


LOGGER = logging.getLogger(__name__)
Connector = Callable[..., Any]

RELATION_ORDER = ("strategy_pkg.package", "strategy_pkg.package_asset")
RELATION_KEY_FIELDS = {
    "strategy_pkg.package": ("package_id",),
    "strategy_pkg.package_asset": ("package_id", "asset_type", "asset_ref"),
}
RELATION_SEMANTIC_COLUMNS = {
    "strategy_pkg.package": PACKAGE_SEMANTIC_COLUMNS,
    "strategy_pkg.package_asset": PACKAGE_ASSET_SEMANTIC_COLUMNS,
}
JSONB_COLUMNS = {
    "manifest_json",
    "seed_sequence",
    "seed_contract",
    "nondeterministic_flags",
    "metadata",
}

TARGET_READ_SQL = {
    "packages": f"""
        SELECT {', '.join(PACKAGE_SEMANTIC_COLUMNS)}
        FROM strategy_pkg.package
        WHERE package_id = ANY(%s)
        ORDER BY package_id
    """,
    "package_assets": f"""
        SELECT {', '.join(PACKAGE_ASSET_SEMANTIC_COLUMNS)}
        FROM strategy_pkg.package_asset
        WHERE (package_id, asset_type, asset_ref) IN (
            SELECT * FROM unnest(%s::text[], %s::text[], %s::text[])
        )
        ORDER BY package_id, asset_type, asset_ref
    """,
}

TARGET_INSERT_SQL = {
    "strategy_pkg.package": f"""
        INSERT INTO strategy_pkg.package ({', '.join(PACKAGE_SEMANTIC_COLUMNS)})
        VALUES ({', '.join(['%s'] * len(PACKAGE_SEMANTIC_COLUMNS))})
        ON CONFLICT (package_id) DO NOTHING
    """,
    "strategy_pkg.package_asset": f"""
        INSERT INTO strategy_pkg.package_asset ({', '.join(PACKAGE_ASSET_SEMANTIC_COLUMNS)})
        VALUES ({', '.join(['%s'] * len(PACKAGE_ASSET_SEMANTIC_COLUMNS))})
        ON CONFLICT (package_id, asset_type, asset_ref) DO NOTHING
    """,
}

IDENTITY_SQL = """
    SELECT current_database() AS current_database,
           host(inet_server_addr()) AS server_address,
           inet_server_port() AS server_port,
           current_setting('server_version_num')::integer AS server_version_num,
           current_user AS current_user,
           current_setting('transaction_read_only') AS transaction_read_only
"""
LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))"
TRANSACTION_ID_SQL = "SELECT txid_current()::text"


def _validate_sql_registry() -> None:
    forbidden = (" UPDATE ", " DELETE ", " TRUNCATE ", " ALTER ", " CREATE ", " DROP ", " COPY ")
    for name, statement in TARGET_READ_SQL.items():
        normalized = f" {' '.join(statement.split()).upper()} "
        if not normalized.strip().startswith("SELECT ") or any(token in normalized for token in forbidden + (" INSERT ",)):
            raise RuntimeError(f"DEV import read registry is not SELECT-only: {name}")
    for relation, statement in TARGET_INSERT_SQL.items():
        normalized = f" {' '.join(statement.split()).upper()} "
        expected_prefix = f"INSERT INTO {relation.upper()} "
        if (
            not normalized.strip().startswith(expected_prefix)
            or " ON CONFLICT " not in normalized
            or " DO NOTHING " not in normalized
            or any(token in normalized for token in forbidden)
        ):
            raise RuntimeError(f"DEV import write registry is outside the fixed INSERT allowlist: {relation}")
    for statement in (LOCK_SQL, TRANSACTION_ID_SQL, IDENTITY_SQL):
        normalized = f" {' '.join(statement.split()).upper()} "
        if not normalized.strip().startswith("SELECT ") or any(token in normalized for token in forbidden + (" INSERT ",)):
            raise RuntimeError("DEV import control SQL is not SELECT-only")


_validate_sql_registry()


def build_import_plan(
    *,
    bundle: PortableAdvisoryEvidenceBundle,
    bundle_ref: OnboardingArtifactRef,
    target_database_identity: DatabaseIdentity,
    target_rows_by_relation: Mapping[str, list[dict[str, Any]]],
) -> RealDevImportPlan:
    """Classify every bundle row without performing DML."""

    if bundle_ref.evidence_kind is not EvidenceKind.BUNDLE or bundle_ref.semantic_content_hash != bundle.bundle_content_hash:
        raise RealDevOnboardingError(REASON_IMPORT_PLAN_INVALID, "import plan bundle ref differs from the bundle")
    if target_database_identity.target_label is not TargetLabel.DEV:
        raise RealDevOnboardingError(REASON_IMPORT_PLAN_INVALID, "import plan target is not DEV")
    row_sets = {item.relation_name: item for item in bundle.relation_row_sets}
    if tuple(sorted(row_sets)) != tuple(sorted(RELATION_ORDER)):
        raise RealDevOnboardingError(REASON_IMPORT_PLAN_INVALID, "bundle relation set differs from the fixed import allowlist")
    classified: list[PlannedImportRow] = []
    for relation in RELATION_ORDER:
        row_set = row_sets[relation]
        _validate_row_set_contract(row_set)
        current_rows = _serialized_target_rows(relation, target_rows_by_relation.get(relation, []))
        for bundle_row in row_set.sorted_rows:
            semantic_row = {name: bundle_row[name] for name in row_set.semantic_column_names}
            natural_key = {name: semantic_row[name] for name in row_set.primary_or_natural_key_fields}
            key_hash = canonical_json_sha256(natural_key)
            actual = current_rows.get(key_hash)
            expected_hash = canonical_json_sha256(semantic_row)
            actual_hash = canonical_json_sha256(actual) if actual is not None else None
            disposition = (
                ImportRowDisposition.INSERT
                if actual is None
                else ImportRowDisposition.EXACT_MATCH
                if actual_hash == expected_hash
                else ImportRowDisposition.CONFLICT
            )
            classified.append(
                PlannedImportRow(
                    relation_name=relation,
                    natural_key_fields=row_set.primary_or_natural_key_fields,
                    natural_key_values=natural_key,
                    semantic_row=semantic_row,
                    expected_row_hash=expected_hash,
                    disposition=disposition,
                    actual_row_hash=actual_hash,
                )
            )
    has_conflict = any(item.disposition is ImportRowDisposition.CONFLICT for item in classified)
    insert_rows = [item for item in classified if item.disposition is ImportRowDisposition.INSERT]
    status = (
        ImportPlanStatus.CONFLICT
        if has_conflict
        else ImportPlanStatus.EXECUTABLE
        if insert_rows
        else ImportPlanStatus.ALREADY_PRESENT
    )
    operations = (
        ()
        if has_conflict
        else tuple(
            ImportWriteOperation(
                relation_name=item.relation_name,
                row_plan_hash=str(item.row_plan_hash),
                expected_row_hash=item.expected_row_hash,
                natural_key_values=item.natural_key_values,
                semantic_row=item.semantic_row,
            )
            for item in sorted(
                insert_rows,
                key=lambda row: (RELATION_ORDER.index(row.relation_name), canonical_json_sha256(row.natural_key_values)),
            )
        )
    )
    return RealDevImportPlan(
        bundle_ref=bundle_ref,
        target_database_identity=target_database_identity,
        release_receipt_ref=bundle.request.release_receipt_ref,
        classified_rows=tuple(classified),
        insert_rows_by_relation=_group_row_plan_hashes(classified, ImportRowDisposition.INSERT),
        exact_match_rows_by_relation=_group_row_plan_hashes(classified, ImportRowDisposition.EXACT_MATCH),
        conflict_rows_by_relation=_group_row_plan_hashes(classified, ImportRowDisposition.CONFLICT),
        ordered_write_operations=operations,
        planned_write_relation_set=tuple(sorted({item.relation_name for item in operations})),
        status=status,
        reason_codes=(REASON_IMPORT_PLAN_CONFLICT,) if has_conflict else (),
    )


class RealDevPackageImporter:
    """Plan, execute and verify the fixed DEV package import protocol."""

    def __init__(
        self,
        *,
        connector: Connector = psycopg2.connect,
        schema_guard: Phase1GReleaseSchemaGuard | None = None,
    ) -> None:
        self._connector = connector
        self._schema_guard = schema_guard or Phase1GReleaseSchemaGuard()

    def plan(
        self,
        *,
        bundle: PortableAdvisoryEvidenceBundle,
        bundle_ref: OnboardingArtifactRef,
        evidence_store: RealDevOnboardingEvidenceStore,
        env_file: Path,
        release_receipt_root: Path,
    ) -> RealDevImportPlan:
        evidence_store.verify_reference_closure(bundle)
        target_config, schema_identity = self._verified_target(
            bundle=bundle,
            env_file=env_file,
            release_receipt_root=release_receipt_root,
        )
        try:
            with readonly_onboarding_connection(target_config, connector=self._connector) as connection:
                target_identity = FixedReadOnlyProjection(connection, target_config).identity()
                if database_identity_hash(target_identity) != database_identity_hash(schema_identity):
                    raise RealDevOnboardingError(
                        REASON_RELEASE_RECEIPT_INVALID,
                        "fresh DEV import plan identity differs from release schema verification",
                    )
                rows = _fetch_target_rows(connection, bundle)
        except RealDevOnboardingError:
            raise
        except Exception as exc:
            raise RealDevOnboardingError(
                REASON_IMPORT_READBACK_FAILED,
                "fresh DEV import plan readback failed",
                context={"error_type": type(exc).__name__},
            ) from exc
        return build_import_plan(
            bundle=bundle,
            bundle_ref=bundle_ref,
            target_database_identity=target_identity,
            target_rows_by_relation=rows,
        )

    def import_dev(
        self,
        *,
        bundle: PortableAdvisoryEvidenceBundle,
        bundle_ref: OnboardingArtifactRef,
        supplied_plan: RealDevImportPlan,
        evidence_store: RealDevOnboardingEvidenceStore,
        env_file: Path,
        release_receipt_root: Path,
        source_package_asset_root: Path,
        target_package_asset_root: Path,
        started_at: datetime | None = None,
    ) -> RealDevImportReceipt:
        started = (started_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
        fresh_plan = self.plan(
            bundle=bundle,
            bundle_ref=bundle_ref,
            evidence_store=evidence_store,
            env_file=env_file,
            release_receipt_root=release_receipt_root,
        )
        if not _same_plan_authority(supplied_plan, fresh_plan):
            raise RealDevOnboardingError(
                REASON_IMPORT_PLAN_INVALID,
                "supplied import plan authority differs from the fresh DEV plan",
            )
        if fresh_plan.status is ImportPlanStatus.CONFLICT:
            raise RealDevOnboardingError(REASON_IMPORT_PLAN_CONFLICT, "DEV package import plan contains immutable conflicts")
        roots = resolve_package_asset_roots(
            source_root=source_package_asset_root,
            target_root=target_package_asset_root,
        )
        _materialize_target_blobs(
            bundle=bundle,
            evidence_store=evidence_store,
            target_root=roots.target_no_replace_root,
        )
        if fresh_plan.status is ImportPlanStatus.ALREADY_PRESENT:
            return _build_receipt(
                bundle=bundle,
                bundle_ref=bundle_ref,
                plan=fresh_plan,
                transaction_id=None,
                outcome=ImportCommitOutcome.ALREADY_PRESENT,
                physical_commit_count=0,
                started_at=started,
                finished_at=datetime.now(timezone.utc),
            )
        target_config = self._target_config(env_file=env_file)
        connection = _open_writable_connection(target_config, connector=self._connector)
        transaction_id: str | None = None
        locked_plan: RealDevImportPlan | None = None
        try:
            with connection.cursor() as cursor:
                cursor.execute(LOCK_SQL, (str(bundle.bundle_content_hash),))
            locked_identity = _database_identity(connection, target_config, require_readonly=False)
            if database_identity_hash(locked_identity) != database_identity_hash(fresh_plan.target_database_identity):
                raise RealDevOnboardingError(REASON_IMPORT_PLAN_INVALID, "write transaction DEV identity differs from the plan")
            locked_plan = build_import_plan(
                bundle=bundle,
                bundle_ref=bundle_ref,
                target_database_identity=locked_identity,
                target_rows_by_relation=_fetch_target_rows(connection, bundle),
            )
            if locked_plan.status is ImportPlanStatus.CONFLICT:
                raise RealDevOnboardingError(REASON_IMPORT_PLAN_CONFLICT, "DEV package rows changed to an immutable conflict")
            if locked_plan.status is ImportPlanStatus.ALREADY_PRESENT:
                connection.rollback()
                return _build_receipt(
                    bundle=bundle,
                    bundle_ref=bundle_ref,
                    plan=locked_plan,
                    transaction_id=None,
                    outcome=ImportCommitOutcome.ALREADY_PRESENT,
                    physical_commit_count=0,
                    started_at=started,
                    finished_at=datetime.now(timezone.utc),
                )
            _verify_target_blobs(bundle=bundle, target_root=roots.target_no_replace_root)
            for operation in locked_plan.ordered_write_operations:
                _execute_insert_and_compare(connection, operation)
            post_plan = build_import_plan(
                bundle=bundle,
                bundle_ref=bundle_ref,
                target_database_identity=locked_identity,
                target_rows_by_relation=_fetch_target_rows(connection, bundle),
            )
            if post_plan.status is not ImportPlanStatus.ALREADY_PRESENT:
                raise RealDevOnboardingError(
                    REASON_IMPORT_READBACK_FAILED,
                    "DEV package rows do not match the bundle before commit",
                )
            _verify_target_blobs(bundle=bundle, target_root=roots.target_no_replace_root)
            with connection.cursor() as cursor:
                cursor.execute(TRANSACTION_ID_SQL)
                row = cursor.fetchone()
            transaction_id = str(row[0]) if row else None
            if not transaction_id:
                raise RealDevOnboardingError(REASON_IMPORT_TRANSACTION_FAILED, "DEV transaction id is unavailable")
            try:
                connection.commit()
            except Exception as exc:
                LOGGER.error(
                    "advisory_dev_import_commit_response_lost bundle_hash_prefix=%s error_type=%s",
                    str(bundle.bundle_content_hash)[:12],
                    type(exc).__name__,
                )
                try:
                    connection.close()
                except Exception:
                    LOGGER.warning("advisory_dev_import_uncertain_connection_close_failed")
                return self._resolve_commit_uncertainty(
                    bundle=bundle,
                    bundle_ref=bundle_ref,
                    execution_plan=locked_plan,
                    transaction_id=transaction_id,
                    evidence_store=evidence_store,
                    env_file=env_file,
                    release_receipt_root=release_receipt_root,
                    started_at=started,
                )
        except RealDevOnboardingError:
            try:
                connection.rollback()
            except Exception:
                LOGGER.warning("advisory_dev_import_rollback_failed")
            raise
        except Exception as exc:
            try:
                connection.rollback()
            except Exception:
                LOGGER.warning("advisory_dev_import_rollback_failed")
            raise RealDevOnboardingError(
                REASON_IMPORT_TRANSACTION_FAILED,
                "DEV package import transaction failed",
                context={"error_type": type(exc).__name__},
            ) from exc
        finally:
            if not getattr(connection, "closed", False):
                try:
                    connection.close()
                except Exception:
                    LOGGER.warning("advisory_dev_import_connection_close_failed")
        if locked_plan is None or transaction_id is None:
            raise RealDevOnboardingError(
                REASON_IMPORT_TRANSACTION_FAILED,
                "DEV import commit completed without its transaction authority",
            )
        readback = self.plan(
            bundle=bundle,
            bundle_ref=bundle_ref,
            evidence_store=evidence_store,
            env_file=env_file,
            release_receipt_root=release_receipt_root,
        )
        if readback.status is not ImportPlanStatus.ALREADY_PRESENT:
            raise RealDevOnboardingError(REASON_IMPORT_READBACK_FAILED, "fresh DEV readback differs after commit")
        return _build_receipt(
            bundle=bundle,
            bundle_ref=bundle_ref,
            plan=locked_plan,
            transaction_id=transaction_id,
            outcome=ImportCommitOutcome.COMMITTED,
            physical_commit_count=1,
            started_at=started,
            finished_at=datetime.now(timezone.utc),
        )

    def verify_import(
        self,
        *,
        bundle: PortableAdvisoryEvidenceBundle,
        bundle_ref: OnboardingArtifactRef,
        receipt: RealDevImportReceipt,
        supplied_plan: RealDevImportPlan,
        evidence_store: RealDevOnboardingEvidenceStore,
        env_file: Path,
        release_receipt_root: Path,
        source_package_asset_root: Path,
        target_package_asset_root: Path,
    ) -> None:
        if receipt.commit_outcome is ImportCommitOutcome.STATE_UNKNOWN:
            raise RealDevOnboardingError(REASON_IMPORT_COMMIT_STATE_UNKNOWN, "state-unknown receipt cannot pass verification")
        if receipt.bundle_ref != bundle_ref or receipt.bundle_hash != bundle.bundle_content_hash:
            raise RealDevOnboardingError(REASON_IMPORT_READBACK_FAILED, "import receipt differs from the bundle")
        readback = self.plan(
            bundle=bundle,
            bundle_ref=bundle_ref,
            evidence_store=evidence_store,
            env_file=env_file,
            release_receipt_root=release_receipt_root,
        )
        if readback.status is not ImportPlanStatus.ALREADY_PRESENT:
            raise RealDevOnboardingError(REASON_IMPORT_READBACK_FAILED, "fresh DEV rows do not fully match the bundle")
        _validate_receipt_authority(
            bundle=bundle,
            bundle_ref=bundle_ref,
            receipt=receipt,
            supplied_plan=supplied_plan,
            readback=readback,
        )
        roots = resolve_package_asset_roots(
            source_root=source_package_asset_root,
            target_root=target_package_asset_root,
        )
        _verify_target_blobs(bundle=bundle, target_root=roots.target_no_replace_root)
        expected_hashes = _expected_post_hashes(bundle)
        if (
            receipt.target_database_identity_hash != database_identity_hash(readback.target_database_identity)
            or receipt.post_readback_row_hashes != expected_hashes
            or receipt.post_dependency_closure_hash != bundle.dependency_closure_hash
        ):
            raise RealDevOnboardingError(REASON_IMPORT_READBACK_FAILED, "import receipt full readback evidence is stale")

    def _verified_target(
        self,
        *,
        bundle: PortableAdvisoryEvidenceBundle,
        env_file: Path,
        release_receipt_root: Path,
    ) -> tuple[DatabaseConnectionConfig, DatabaseIdentity]:
        config = self._target_config(env_file=env_file)
        release_receipt = load_exact_release_receipt(
            ref=bundle.request.release_receipt_ref,
            root=release_receipt_root,
        )
        try:
            evidence = self._schema_guard.verify(
                receipt=release_receipt,
                target_label=TargetLabel.DEV,
                connection_config=config,
            )
        except Exception as exc:
            raise RealDevOnboardingError(
                REASON_RELEASE_RECEIPT_INVALID,
                "current DEV catalog differs from the exact release receipt",
                context={"error_type": type(exc).__name__, "cause_reason_code": getattr(exc, "reason_code", None)},
            ) from exc
        return config, evidence.database_identity

    @staticmethod
    def _target_config(*, env_file: Path) -> DatabaseConnectionConfig:
        try:
            config = resolve_database_connection(target_label=TargetLabel.DEV, env_file=env_file)
        except ReleaseSchemaVerificationError as exc:
            raise RealDevOnboardingError(
                REASON_ENV_INVALID,
                "unable to resolve exact DEV connection for package import",
                context={"cause_reason_code": exc.reason_code},
            ) from exc
        return config

    def _resolve_commit_uncertainty(
        self,
        *,
        bundle: PortableAdvisoryEvidenceBundle,
        bundle_ref: OnboardingArtifactRef,
        execution_plan: RealDevImportPlan,
        transaction_id: str,
        evidence_store: RealDevOnboardingEvidenceStore,
        env_file: Path,
        release_receipt_root: Path,
        started_at: datetime,
    ) -> RealDevImportReceipt:
        try:
            fresh = self.plan(
                bundle=bundle,
                bundle_ref=bundle_ref,
                evidence_store=evidence_store,
                env_file=env_file,
                release_receipt_root=release_receipt_root,
            )
        except RealDevOnboardingError as exc:
            LOGGER.error(
                "advisory_dev_import_uncertainty_readback_unavailable bundle_hash_prefix=%s reason_code=%s",
                str(bundle.bundle_content_hash)[:12],
                exc.reason_code,
            )
            return _build_receipt(
                bundle=bundle,
                bundle_ref=bundle_ref,
                plan=execution_plan,
                transaction_id=transaction_id,
                outcome=ImportCommitOutcome.STATE_UNKNOWN,
                physical_commit_count=None,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                reason_codes=(REASON_IMPORT_COMMIT_STATE_UNKNOWN,),
                post_readback_row_hashes={},
                post_dependency_closure_hash=canonical_json_sha256(
                    {"fresh_readback": "UNAVAILABLE", "reason_code": exc.reason_code}
                ),
            )
        execution_by_key = {
            (item.relation_name, canonical_json_sha256(item.natural_key_values)): item
            for item in execution_plan.classified_rows
        }
        fresh_by_key = {
            (item.relation_name, canonical_json_sha256(item.natural_key_values)): item
            for item in fresh.classified_rows
        }
        if set(execution_by_key) == set(fresh_by_key) and all(
            item.disposition is ImportRowDisposition.EXACT_MATCH for item in fresh_by_key.values()
        ):
            return _build_receipt(
                bundle=bundle,
                bundle_ref=bundle_ref,
                plan=execution_plan,
                transaction_id=transaction_id,
                outcome=ImportCommitOutcome.COMMITTED,
                physical_commit_count=1,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
            )
        not_observed = set(execution_by_key) == set(fresh_by_key) and all(
            (
                execution.disposition is ImportRowDisposition.INSERT
                and fresh_by_key[key].disposition is ImportRowDisposition.INSERT
            )
            or (
                execution.disposition is ImportRowDisposition.EXACT_MATCH
                and fresh_by_key[key].disposition is ImportRowDisposition.EXACT_MATCH
            )
            for key, execution in execution_by_key.items()
        )
        if not_observed:
            raise RealDevOnboardingError(
                REASON_IMPORT_COMMIT_NOT_OBSERVED,
                "DEV commit response was lost and fresh readback proves the transaction was not committed",
            )
        return _build_receipt(
            bundle=bundle,
            bundle_ref=bundle_ref,
            plan=execution_plan,
            transaction_id=transaction_id,
            outcome=ImportCommitOutcome.STATE_UNKNOWN,
            physical_commit_count=None,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            reason_codes=(REASON_IMPORT_COMMIT_STATE_UNKNOWN,),
            post_readback_row_hashes=_actual_post_hashes(fresh),
            post_dependency_closure_hash=canonical_json_sha256(
                {"classified_rows": [item.model_dump(mode="json") for item in fresh.classified_rows]}
            ),
        )


def _validate_row_set_contract(row_set: PortableRelationRowSet) -> None:
    expected_keys = RELATION_KEY_FIELDS[row_set.relation_name]
    expected_columns = RELATION_SEMANTIC_COLUMNS[row_set.relation_name]
    if row_set.primary_or_natural_key_fields != tuple(sorted(expected_keys)):
        raise RealDevOnboardingError(REASON_IMPORT_PLAN_INVALID, "bundle natural key contract differs from importer")
    if row_set.semantic_column_names != tuple(sorted(expected_columns)):
        raise RealDevOnboardingError(REASON_IMPORT_PLAN_INVALID, "bundle semantic column contract differs from importer")


def _validate_receipt_authority(
    *,
    bundle: PortableAdvisoryEvidenceBundle,
    bundle_ref: OnboardingArtifactRef,
    receipt: RealDevImportReceipt,
    supplied_plan: RealDevImportPlan,
    readback: RealDevImportPlan,
) -> None:
    expected_counts = {item.relation_name: len(item.sorted_rows) for item in bundle.relation_row_sets}
    expected_inserted = {
        relation: sum(operation.relation_name == relation for operation in supplied_plan.ordered_write_operations)
        for relation in RELATION_ORDER
    }
    if receipt.commit_outcome is ImportCommitOutcome.ALREADY_PRESENT:
        expected_inserted = {relation: 0 for relation in RELATION_ORDER}
    if (
        receipt.bundle_ref != bundle_ref
        or receipt.request_hash != bundle.request.request_hash
        or receipt.bundle_hash != bundle.bundle_content_hash
        or receipt.plan_hash != supplied_plan.plan_hash
        or receipt.source_database_identity_hash != bundle.source_database_identity_hash
        or receipt.target_database_identity_hash != database_identity_hash(readback.target_database_identity)
        or not _same_plan_authority(supplied_plan, readback)
        or receipt.inserted_row_counts != expected_inserted
        or receipt.matched_row_counts != expected_counts
        or set(receipt.post_readback_row_hashes) != set(RELATION_ORDER)
    ):
        raise RealDevOnboardingError(
            REASON_IMPORT_READBACK_FAILED,
            "import receipt authority differs from the bundle, plan, or fresh DEV readback",
        )


def _same_plan_authority(left: RealDevImportPlan, right: RealDevImportPlan) -> bool:
    def authority(plan: RealDevImportPlan) -> tuple[Any, ...]:
        rows = tuple(
            sorted(
                (
                    item.relation_name,
                    canonical_json_sha256(item.natural_key_values),
                    item.expected_row_hash,
                )
                for item in plan.classified_rows
            )
        )
        return (
            plan.bundle_ref,
            database_identity_hash(plan.target_database_identity),
            plan.release_receipt_ref,
            rows,
        )

    return authority(left) == authority(right)


def _group_row_plan_hashes(
    rows: list[PlannedImportRow], disposition: ImportRowDisposition
) -> dict[str, tuple[str, ...]]:
    return {
        relation: tuple(str(item.row_plan_hash) for item in rows if item.relation_name == relation and item.disposition is disposition)
        for relation in RELATION_ORDER
        if any(item.relation_name == relation and item.disposition is disposition for item in rows)
    }


def _serialized_target_rows(relation: str, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    columns = RELATION_SEMANTIC_COLUMNS[relation]
    keys = RELATION_KEY_FIELDS[relation]
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not set(columns).issubset(row):
            raise RealDevOnboardingError(REASON_IMPORT_PLAN_INVALID, "DEV readback row lacks fixed semantic columns")
        semantic = {name: serialize_postgres_value(row[name]) for name in sorted(columns)}
        key_hash = canonical_json_sha256({name: semantic[name] for name in sorted(keys)})
        if key_hash in result:
            raise RealDevOnboardingError(REASON_IMPORT_PLAN_INVALID, "DEV readback contains duplicate natural keys")
        result[key_hash] = semantic
    return result


def _fetch_target_rows(connection: Any, bundle: PortableAdvisoryEvidenceBundle) -> dict[str, list[dict[str, Any]]]:
    row_sets = {item.relation_name: item for item in bundle.relation_row_sets}
    package_ids = [str(row["package_id"]) for row in row_sets["strategy_pkg.package"].sorted_rows]
    asset_rows = row_sets["strategy_pkg.package_asset"].sorted_rows
    asset_keys = [
        (str(row["package_id"]), str(row["asset_type"]), str(row["asset_ref"]))
        for row in asset_rows
    ]
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(TARGET_READ_SQL["packages"], (package_ids,))
        packages = [dict(row) for row in cursor.fetchall()]
        if asset_keys:
            cursor.execute(
                TARGET_READ_SQL["package_assets"],
                (
                    [item[0] for item in asset_keys],
                    [item[1] for item in asset_keys],
                    [item[2] for item in asset_keys],
                ),
            )
            assets = [dict(row) for row in cursor.fetchall()]
        else:
            assets = []
    return {"strategy_pkg.package": packages, "strategy_pkg.package_asset": assets}


def _open_writable_connection(config: DatabaseConnectionConfig, *, connector: Connector) -> Any:
    connection: Any | None = None
    try:
        connection = connector(
            **config.connect_kwargs(),
            options="-c statement_timeout=120000 -c lock_timeout=5000",
        )
        connection.set_session(readonly=False, autocommit=False, isolation_level="READ COMMITTED")
        identity = _database_identity(connection, config, require_readonly=False)
    except Exception as exc:
        try:
            if connection is not None:
                connection.close()
        except Exception:
            LOGGER.warning("advisory_dev_import_failed_connection_close_failed")
        if isinstance(exc, RealDevOnboardingError):
            raise
        raise RealDevOnboardingError(
            REASON_DATABASE_CONNECTION_FAILED,
            "unable to establish exact writable DEV transaction",
            context={"error_type": type(exc).__name__},
        ) from exc
    if identity.target_label is not TargetLabel.DEV:
        connection.close()
        raise RealDevOnboardingError(REASON_DATABASE_CONNECTION_FAILED, "writable import connection is not DEV")
    return connection


def _database_identity(
    connection: Any,
    config: DatabaseConnectionConfig,
    *,
    require_readonly: bool,
) -> DatabaseIdentity:
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(IDENTITY_SQL)
        row = cursor.fetchone()
    if row is None:
        raise RealDevOnboardingError(REASON_DATABASE_CONNECTION_FAILED, "DEV identity query returned no row")
    readonly = str(row["transaction_read_only"]).lower() in {"on", "true"}
    if readonly is not require_readonly:
        raise RealDevOnboardingError(REASON_DATABASE_CONNECTION_FAILED, "DEV transaction read-only mode is incorrect")
    return DatabaseIdentity(
        target_label=config.target_label,
        current_database=str(row["current_database"]),
        server_address=str(row["server_address"]) if row["server_address"] is not None else None,
        server_port=int(row["server_port"]),
        server_version_num=int(row["server_version_num"]),
        current_user_hash=hashlib.sha256(str(row["current_user"]).encode("utf-8")).hexdigest(),
        environment_contract_hash=config.environment_contract_hash,
    )


def _execute_insert_and_compare(connection: Any, operation: ImportWriteOperation) -> None:
    columns = RELATION_SEMANTIC_COLUMNS[operation.relation_name]
    values = tuple(_adapt_database_value(name, operation.semantic_row[name]) for name in columns)
    with connection.cursor() as cursor:
        cursor.execute(TARGET_INSERT_SQL[operation.relation_name], values)
    rows = _fetch_exact_operation_row(connection, operation)
    serialized = _serialized_target_rows(operation.relation_name, rows)
    key_hash = canonical_json_sha256(operation.natural_key_values)
    actual = serialized.get(key_hash)
    if actual is None or canonical_json_sha256(actual) != operation.expected_row_hash:
        raise RealDevOnboardingError(
            REASON_IMPORT_READBACK_FAILED,
            "DEV INSERT-or-compare full row readback differs",
            context={"relation": operation.relation_name},
        )


def _fetch_exact_operation_row(connection: Any, operation: ImportWriteOperation) -> list[dict[str, Any]]:
    values = {name: deserialize_postgres_value(value) for name, value in operation.natural_key_values.items()}
    if operation.relation_name == "strategy_pkg.package":
        params = ([values["package_id"]],)
        query = TARGET_READ_SQL["packages"]
    else:
        params = ([values["package_id"]], [values["asset_type"]], [values["asset_ref"]])
        query = TARGET_READ_SQL["package_assets"]
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def _adapt_database_value(column: str, value: Any) -> Any:
    deserialized = deserialize_postgres_value(value)
    return psycopg2.extras.Json(deserialized) if column in JSONB_COLUMNS and deserialized is not None else deserialized


def _materialize_target_blobs(
    *,
    bundle: PortableAdvisoryEvidenceBundle,
    evidence_store: RealDevOnboardingEvidenceStore,
    target_root: Path,
) -> None:
    try:
        for item in bundle.artifact_blob_refs:
            raw = evidence_store.load_blob(item.blob_ref)
            digest = validate_sha256(item.asset_sha256, field_name="asset_sha256")
            if hashlib.sha256(raw).hexdigest() != digest:
                raise RealDevOnboardingError(REASON_IMPORT_READBACK_FAILED, "bundle blob differs before DEV materialization")
            target = target_root / "blobs" / digest[:2] / digest
            target.parent.mkdir(parents=True, exist_ok=True)
            _publish_blob_no_replace(raw=raw, target=target, root=target_root)
        _verify_target_blobs(bundle=bundle, target_root=target_root)
    except RealDevOnboardingError:
        raise
    except (OSError, ValueError) as exc:
        raise RealDevOnboardingError(
            REASON_IMPORT_READBACK_FAILED,
            "target package asset materialization failed",
            context={"error_type": type(exc).__name__},
        ) from exc


def _publish_blob_no_replace(*, raw: bytes, target: Path, root: Path) -> None:
    resolved_parent = target.parent.resolve(strict=True)
    resolved_parent.relative_to(root.resolve(strict=True))
    _assert_no_reparse_from_root(root=root.resolve(strict=True), path=resolved_parent)
    if target.exists():
        existing = target.read_bytes()
        if existing != raw:
            raise RealDevOnboardingError(REASON_IMPORT_READBACK_FAILED, "target package asset CAS identity collision")
        return
    tmp_root = root / "tmp"
    tmp_root.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix="advisory_import_", suffix=".tmp", dir=tmp_root)
    temp_path = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        if os.name == "nt":
            import ctypes

            move_file_ex = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
            move_file_ex.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
            move_file_ex.restype = ctypes.c_int
            if not move_file_ex(str(temp_path), str(target), 0x00000008):
                error_code = ctypes.get_last_error()
                if error_code not in {80, 183}:
                    raise ctypes.WinError(error_code)
        else:
            try:
                os.link(temp_path, target)
            except FileExistsError:
                pass
        if target.read_bytes() != raw:
            raise RealDevOnboardingError(REASON_IMPORT_READBACK_FAILED, "target package asset full readback differs")
    finally:
        temp_path.unlink(missing_ok=True)


def _verify_target_blobs(*, bundle: PortableAdvisoryEvidenceBundle, target_root: Path) -> None:
    try:
        for item in bundle.artifact_blob_refs:
            digest = validate_sha256(item.asset_sha256, field_name="asset_sha256")
            path = (target_root / "blobs" / digest[:2] / digest).resolve(strict=True)
            resolved_root = target_root.resolve(strict=True)
            path.relative_to(resolved_root)
            _assert_no_reparse_from_root(root=resolved_root, path=path)
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != digest or len(raw) != item.size_bytes:
                raise RealDevOnboardingError(REASON_IMPORT_READBACK_FAILED, "target package asset full readback differs")
    except RealDevOnboardingError:
        raise
    except (OSError, ValueError) as exc:
        raise RealDevOnboardingError(
            REASON_IMPORT_READBACK_FAILED,
            "target package asset cannot be resolved for full readback",
            context={"error_type": type(exc).__name__},
        ) from exc


def _expected_post_hashes(bundle: PortableAdvisoryEvidenceBundle) -> dict[str, tuple[str, ...]]:
    return {
        item.relation_name: tuple(sorted(item.row_content_hashes))
        for item in sorted(bundle.relation_row_sets, key=lambda value: value.relation_name)
    }


def _actual_post_hashes(plan: RealDevImportPlan) -> dict[str, tuple[str, ...]]:
    return {
        relation: tuple(
            sorted(
                str(item.actual_row_hash)
                for item in plan.classified_rows
                if item.relation_name == relation and item.actual_row_hash is not None
            )
        )
        for relation in RELATION_ORDER
    }


def _build_receipt(
    *,
    bundle: PortableAdvisoryEvidenceBundle,
    bundle_ref: OnboardingArtifactRef,
    plan: RealDevImportPlan,
    transaction_id: str | None,
    outcome: ImportCommitOutcome,
    physical_commit_count: int | None,
    started_at: datetime,
    finished_at: datetime,
    reason_codes: tuple[str, ...] = (),
    post_readback_row_hashes: dict[str, tuple[str, ...]] | None = None,
    post_dependency_closure_hash: str | None = None,
) -> RealDevImportReceipt:
    inserted = {
        relation: sum(item.relation_name == relation for item in plan.ordered_write_operations)
        for relation in RELATION_ORDER
    }
    if outcome is ImportCommitOutcome.ALREADY_PRESENT:
        inserted = {relation: 0 for relation in RELATION_ORDER}
    matched = {
        item.relation_name: len(item.sorted_rows)
        for item in bundle.relation_row_sets
    }
    return RealDevImportReceipt(
        import_invocation_id=f"adv_real_dev_import_{uuid4().hex}",
        bundle_ref=bundle_ref,
        request_hash=str(bundle.request.request_hash),
        bundle_hash=str(bundle.bundle_content_hash),
        plan_hash=str(plan.plan_hash),
        source_database_identity_hash=bundle.source_database_identity_hash,
        target_database_identity_hash=database_identity_hash(plan.target_database_identity),
        transaction_id=transaction_id,
        inserted_row_counts=inserted,
        matched_row_counts=matched,
        write_relation_set=tuple(sorted(name for name, count in inserted.items() if count > 0)),
        post_readback_row_hashes=(
            _expected_post_hashes(bundle) if post_readback_row_hashes is None else post_readback_row_hashes
        ),
        post_dependency_closure_hash=(
            str(bundle.dependency_closure_hash)
            if post_dependency_closure_hash is None
            else post_dependency_closure_hash
        ),
        physical_commit_count=physical_commit_count,
        commit_outcome=outcome,
        started_at=started_at,
        finished_at=finished_at,
        reason_codes=reason_codes,
    )
