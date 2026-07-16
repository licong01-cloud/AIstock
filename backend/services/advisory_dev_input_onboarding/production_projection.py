"""Fixed-query, read-only production/DEV inventory for Advisory onboarding."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg2
import psycopg2.extras

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.phase1g_artifact_ref import Phase1GArtifactRootBinding
from backend.services.advisory_phase1.phase1g_contract import (
    PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY,
    Phase1GInputArtifactKind,
)
from backend.services.advisory_phase1.phase1g_schema_guard import Phase1GReleaseSchemaGuard
from backend.services.advisory_phase1.release_schema_contract import (
    DatabaseIdentity,
    ReleaseSchemaReceipt,
    TargetLabel,
)
from backend.services.advisory_phase1.release_schema_verify_postgres import (
    DatabaseConnectionConfig,
    ReleaseSchemaVerificationError,
    resolve_database_connection,
)

from .contracts import (
    AlphaComponentEvidence,
    AlphaMode,
    ALLOWED_EXPORT_PACKAGE_STATUSES,
    BundleBlobRef,
    BundlePackageRef,
    DependencyEdge,
    EvidenceKind,
    InventoryClassification,
    NativeMultiComponentRef,
    OnboardingArtifactRef,
    PackageClosureStatus,
    PackageInventoryCandidate,
    PortableAdvisoryEvidenceBundle,
    PortableRelationRowSet,
    RealDevOnboardingError,
    RealDevOnboardingInventoryReceipt,
    RealDevOnboardingInventoryQuery,
    RealDevOnboardingRequest,
    REASON_DATABASE_CONNECTION_FAILED,
    REASON_BUNDLE_EXPORT_FAILED,
    REASON_ENV_INVALID,
    REASON_MULTI_TRACK_MISSING,
    REASON_PACKAGE_MANIFEST_MISMATCH,
    REASON_PACKAGE_MISSING,
    REASON_PACKAGE_ASSET_MISSING,
    REASON_READONLY_ASSERTION_FAILED,
    REASON_PROJECTION_FAILED,
    REASON_REQUEST_INVALID,
    REASON_RELEASE_RECEIPT_INVALID,
    REASON_SINGLE_TRACK_MISSING,
    REASON_SOURCE_TARGET_IDENTITY_COLLISION,
    REASON_SOURCE_PROGRAM_MISSING,
    REASON_TARGET_CONFLICT,
    SourceFactEligibility,
    compute_portable_manifest_json_sha256,
    database_identity_hash,
    portable_manifest_runtime_asset_refs,
    validate_sha256,
)
from .store import RealDevOnboardingEvidenceStore, resolve_package_asset_roots


LOGGER = logging.getLogger(__name__)
Connector = Callable[..., Any]
InventoryInput = RealDevOnboardingRequest | RealDevOnboardingInventoryQuery

READONLY_STATEMENT_TIMEOUT_MS = 120_000
_FILE_ATTRIBUTE_REPARSE_POINT = 0x0400

PACKAGE_SEMANTIC_COLUMNS = (
    "package_id",
    "package_name",
    "package_version",
    "source_type",
    "source_id",
    "loop_id",
    "run_id",
    "package_status",
    "manifest_json",
    "manifest_sha256",
    "alpha_mode",
    "signal_domain",
    "display_name",
    "legacy_name",
    "data_vintage",
    "prediction_ref_uri",
    "prediction_ref_sha256",
    "model_artifact_uri",
    "model_artifact_sha256",
    "seed_policy",
    "master_seed",
    "seed_sequence",
    "seed_contract",
    "seed_contract_sha256",
    "reproducibility_level",
    "nondeterministic_flags",
)
PACKAGE_PROVENANCE_COLUMNS = ("paper_portfolio_count", "created_at", "updated_at")
PACKAGE_ASSET_SEMANTIC_COLUMNS = (
    "package_id",
    "asset_type",
    "asset_ref",
    "asset_sha256",
    "metadata",
    "asset_role",
    "asset_size_bytes",
    "protected_asset",
)
PACKAGE_ASSET_PROVENANCE_COLUMNS = ("asset_id", "created_at")

SQL: dict[str, str] = {
    "identity": """
        SELECT current_database() AS current_database,
               host(inet_server_addr()) AS server_address,
               inet_server_port() AS server_port,
               current_setting('server_version_num')::integer AS server_version_num,
               current_user AS current_user,
               current_setting('transaction_read_only') AS transaction_read_only
    """,
    "packages": """
        SELECT package_id, source_id, package_status, manifest_json, manifest_sha256, alpha_mode, data_vintage
        FROM strategy_pkg.package
        WHERE package_id = ANY(%s)
        ORDER BY package_id
    """,
    "package_asset_counts": """
        SELECT package_id, count(*)::bigint AS asset_count
        FROM strategy_pkg.package_asset
        WHERE package_id = ANY(%s)
        GROUP BY package_id
        ORDER BY package_id
    """,
    "source_programs": """
        SELECT program_id, target_count, review_policy
        FROM app.advisory_program
        WHERE program_id = ANY(%s)
        ORDER BY program_id
    """,
    "source_bindings": """
        SELECT program_id, package_mode, package_ids,
               effective_from_trade_date, effective_to_trade_date, activation_status
        FROM app.advisory_strategy_binding_version
        WHERE program_id = ANY(%s)
        ORDER BY program_id, effective_from_trade_date NULLS FIRST, binding_version_id
    """,
    "dse_summary": """
        SELECT package_id,
               COALESCE(evidence_payload_json->>'schema_version', 'UNKNOWN') AS schema_version,
               count(*)::bigint AS evidence_count,
               array_agg(DISTINCT target_trade_date ORDER BY target_trade_date) AS trade_dates
        FROM selection.daily_selection_evidence
        WHERE package_id = ANY(%s)
        GROUP BY package_id, COALESCE(evidence_payload_json->>'schema_version', 'UNKNOWN')
        ORDER BY package_id, schema_version
    """,
    "target_packages": """
        SELECT package_id, manifest_sha256, alpha_mode
        FROM strategy_pkg.package
        WHERE package_id = ANY(%s)
        ORDER BY package_id
    """,
    "target_programs": """
        SELECT program_id, target_count, review_policy
        FROM app.advisory_program
        WHERE program_id = ANY(%s)
        ORDER BY program_id
    """,
    "target_bindings": """
        SELECT program_id, package_mode, package_ids,
               effective_from_trade_date, effective_to_trade_date, activation_status
        FROM app.advisory_strategy_binding_version
        WHERE program_id = ANY(%s)
        ORDER BY program_id, effective_from_trade_date NULLS FIRST, binding_version_id
    """,
    "export_snapshot_identity": """
        SELECT txid_current_snapshot()::text AS snapshot_identity
    """,
    "export_packages": """
        SELECT package_id, package_name, package_version, source_type, source_id,
               loop_id, run_id, package_status, manifest_json, manifest_sha256,
               alpha_mode, signal_domain, display_name, legacy_name, data_vintage,
               prediction_ref_uri, prediction_ref_sha256,
               model_artifact_uri, model_artifact_sha256,
               seed_policy, master_seed, seed_sequence, seed_contract,
               seed_contract_sha256, reproducibility_level, nondeterministic_flags,
               paper_portfolio_count, created_at, updated_at
        FROM strategy_pkg.package
        WHERE package_id = ANY(%s)
        ORDER BY package_id
    """,
    "export_package_assets": """
        SELECT asset_id, package_id, asset_type, asset_ref, asset_sha256,
               metadata, asset_role, asset_size_bytes, protected_asset, created_at
        FROM strategy_pkg.package_asset
        WHERE package_id = ANY(%s)
        ORDER BY package_id, asset_type, asset_ref, asset_id
    """,
}


def _validate_fixed_sql() -> None:
    for name, statement in SQL.items():
        normalized = " ".join(statement.split()).upper()
        if not normalized.startswith("SELECT ") or any(
            token in normalized
            for token in (" INSERT ", " UPDATE ", " DELETE ", " TRUNCATE ", " ALTER ", " CREATE ", " DROP ", " FOR UPDATE", " FOR SHARE")
        ):
            raise RuntimeError(f"onboarding SQL registry contains a non-read-only statement: {name}")


_validate_fixed_sql()


@contextmanager
def readonly_onboarding_connection(
    config: DatabaseConnectionConfig,
    *,
    connector: Connector = psycopg2.connect,
) -> Iterator[Any]:
    """Open a server-enforced, bounded, repeatable-read transaction."""

    try:
        connection = connector(
            **config.connect_kwargs(),
            options=(
                "-c default_transaction_read_only=on "
                f"-c statement_timeout={READONLY_STATEMENT_TIMEOUT_MS} "
                "-c lock_timeout=5000"
            ),
        )
    except Exception as exc:
        raise RealDevOnboardingError(
            REASON_DATABASE_CONNECTION_FAILED,
            "unable to open exact onboarding database connection",
            context={"target_label": config.target_label.value, "error_type": type(exc).__name__},
        ) from exc
    try:
        try:
            connection.set_session(readonly=True, autocommit=False, isolation_level="REPEATABLE READ")
            with connection.cursor() as cursor:
                cursor.execute("SHOW transaction_read_only")
                row = cursor.fetchone()
        except Exception as exc:
            raise RealDevOnboardingError(
                REASON_DATABASE_CONNECTION_FAILED,
                "unable to establish the bounded read-only transaction",
                context={"target_label": config.target_label.value, "error_type": type(exc).__name__},
            ) from exc
        if row is None or str(row[0]).lower() not in {"on", "true"}:
            raise RealDevOnboardingError(
                REASON_READONLY_ASSERTION_FAILED,
                "database transaction_read_only is not enabled",
                context={"target_label": config.target_label.value},
            )
        yield connection
        connection.rollback()
    except Exception:
        try:
            connection.rollback()
        except Exception:
            LOGGER.warning("advisory_onboarding_readonly_rollback_failed target=%s", config.target_label.value)
        raise
    finally:
        try:
            connection.close()
        except Exception:
            LOGGER.warning("advisory_onboarding_readonly_close_failed target=%s", config.target_label.value)


class FixedReadOnlyProjection:
    def __init__(self, connection: Any, config: DatabaseConnectionConfig) -> None:
        self._connection = connection
        self._config = config
        self.query_count = 0
        self.write_query_count = 0

    def identity(self) -> DatabaseIdentity:
        row = self.one("identity", ())
        if str(row["transaction_read_only"]).lower() not in {"on", "true"}:
            raise RealDevOnboardingError(REASON_READONLY_ASSERTION_FAILED, "identity query observed a writable transaction")
        return DatabaseIdentity(
            target_label=self._config.target_label,
            current_database=str(row["current_database"]),
            server_address=str(row["server_address"]) if row["server_address"] is not None else None,
            server_port=int(row["server_port"]),
            server_version_num=int(row["server_version_num"]),
            current_user_hash=hashlib.sha256(str(row["current_user"]).encode("utf-8")).hexdigest(),
            environment_contract_hash=self._config.environment_contract_hash,
        )

    def all(self, name: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        statement = SQL[name]
        normalized = " ".join(statement.split()).upper()
        self.query_count += 1
        if not normalized.startswith("SELECT "):
            self.write_query_count += 1
            raise RealDevOnboardingError(REASON_READONLY_ASSERTION_FAILED, "projection attempted a non-SELECT statement")
        try:
            with self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(statement, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as exc:
            raise RealDevOnboardingError(
                REASON_PROJECTION_FAILED,
                "fixed read-only onboarding projection failed",
                context={"query_name": name, "target_label": self._config.target_label.value, "error_type": type(exc).__name__},
            ) from exc

    def one(self, name: str, params: tuple[Any, ...]) -> dict[str, Any]:
        rows = self.all(name, params)
        if len(rows) != 1:
            raise RealDevOnboardingError(REASON_READONLY_ASSERTION_FAILED, f"{name} must return exactly one row")
        return rows[0]


class RealDevOnboardingInventoryService:
    def __init__(
        self,
        *,
        connector: Connector = psycopg2.connect,
        schema_guard: Phase1GReleaseSchemaGuard | None = None,
    ) -> None:
        self._connector = connector
        self._schema_guard = schema_guard or Phase1GReleaseSchemaGuard()

    def inventory(
        self,
        *,
        input_contract: InventoryInput,
        selected_input_ref: OnboardingArtifactRef,
        env_file: Path,
        release_receipt_root: Path,
        observed_at: datetime | None = None,
    ) -> RealDevOnboardingInventoryReceipt:
        try:
            source_config = resolve_database_connection(target_label=TargetLabel.PRODUCTION, env_file=env_file)
            target_config = resolve_database_connection(target_label=TargetLabel.DEV, env_file=env_file)
        except ReleaseSchemaVerificationError as exc:
            raise RealDevOnboardingError(
                REASON_ENV_INVALID,
                "explicit production/DEV database environment is invalid",
                context={"cause_reason_code": exc.reason_code},
            ) from exc
        _validate_selected_input_ref(input_contract=input_contract, selected_input_ref=selected_input_ref)
        receipt = load_exact_release_receipt(ref=input_contract.release_receipt_ref, root=release_receipt_root)
        try:
            schema_evidence = self._schema_guard.verify(
                receipt=receipt,
                target_label=TargetLabel.DEV,
                connection_config=target_config,
            )
        except Exception as exc:
            raise RealDevOnboardingError(
                REASON_RELEASE_RECEIPT_INVALID,
                "current DEV catalog does not match the exact Phase 1F.2 release receipt",
                context={
                    "cause_reason_code": getattr(exc, "reason_code", None),
                    "error_type": type(exc).__name__,
                },
            ) from exc
        with readonly_onboarding_connection(source_config, connector=self._connector) as source_connection:
            with readonly_onboarding_connection(target_config, connector=self._connector) as target_connection:
                source = FixedReadOnlyProjection(source_connection, source_config)
                target = FixedReadOnlyProjection(target_connection, target_config)
                result = self.project(
                    input_contract=input_contract,
                    selected_input_ref=selected_input_ref,
                    source=source,
                    target=target,
                    release_catalog_fingerprint=schema_evidence.catalog_fingerprint,
                    observed_at=observed_at or datetime.now(timezone.utc),
                )
        if database_identity_hash(result.target_database_identity) != database_identity_hash(schema_evidence.database_identity):
            raise RealDevOnboardingError(
                REASON_RELEASE_RECEIPT_INVALID,
                "fresh DEV inventory identity differs from release catalog verification",
            )
        return result

    def project(
        self,
        *,
        input_contract: InventoryInput,
        selected_input_ref: OnboardingArtifactRef,
        source: FixedReadOnlyProjection,
        target: FixedReadOnlyProjection,
        release_catalog_fingerprint: str,
        observed_at: datetime,
    ) -> RealDevOnboardingInventoryReceipt:
        source_identity = source.identity()
        target_identity = target.identity()
        _assert_distinct_physical_databases(source_identity, target_identity)

        _validate_selected_input_ref(input_contract=input_contract, selected_input_ref=selected_input_ref)
        package_ids = list(input_contract.source_package_ids)
        program_ids = [item.program_id for item in input_contract.target_dev_program_specs]
        source_program_ids = list(input_contract.source_program_refs)
        package_rows = source.all("packages", (package_ids,))
        asset_count_rows = source.all("package_asset_counts", (package_ids,))
        source_program_rows = source.all("source_programs", (source_program_ids,)) if source_program_ids else []
        source_binding_rows = source.all("source_bindings", (source_program_ids,)) if source_program_ids else []
        dse_rows = source.all("dse_summary", (package_ids,))
        target_package_rows = target.all("target_packages", (package_ids,))
        target_program_rows = target.all("target_programs", (program_ids,))
        target_binding_rows = target.all("target_bindings", (program_ids,))

        if source.write_query_count or target.write_query_count:
            raise RealDevOnboardingError(REASON_READONLY_ASSERTION_FAILED, "inventory projection recorded a write query")

        packages_by_id = {str(row["package_id"]): row for row in package_rows}
        expected_manifests = (
            dict(input_contract.expected_package_manifest_sha256s)
            if isinstance(input_contract, RealDevOnboardingRequest)
            else {str(row["package_id"]): str(row["manifest_sha256"]).lower() for row in package_rows}
        )
        counts_by_id = {str(row["package_id"]): int(row["asset_count"]) for row in asset_count_rows}
        dse_by_package = _group_dse(dse_rows)
        program_packages = _source_program_packages(source_binding_rows)
        expected_modes = {item.package_id: item.alpha_mode for item in input_contract.target_dev_program_specs}
        candidates: list[PackageInventoryCandidate] = []
        top_reasons: set[str] = set()
        if {str(row["program_id"]) for row in source_program_rows} != set(source_program_ids):
            top_reasons.add(REASON_SOURCE_PROGRAM_MISSING)
        for package_id in input_contract.source_package_ids:
            row = packages_by_id.get(package_id)
            if row is None:
                top_reasons.add(REASON_PACKAGE_MISSING)
                continue
            candidate = _candidate_from_row(
                row=row,
                expected_manifest=expected_manifests[package_id],
                expected_alpha_mode=expected_modes[package_id],
                asset_count=counts_by_id.get(package_id, 0),
                source_program_refs=tuple(sorted(program_id for program_id, ids in program_packages.items() if package_id in ids)),
                binding_rows=source_binding_rows,
                dse_summary=dse_by_package.get(package_id, {}),
                decision_trade_date=input_contract.decision_trade_date,
            )
            candidates.append(candidate)
            if not candidate.package_eligible:
                top_reasons.update(candidate.reason_codes)

        if not any(item.alpha_mode is AlphaMode.SINGLE and item.package_eligible for item in candidates):
            top_reasons.add(REASON_SINGLE_TRACK_MISSING)
        if not any(item.alpha_mode is AlphaMode.MULTI and item.package_eligible for item in candidates):
            top_reasons.add(REASON_MULTI_TRACK_MISSING)

        conflicts = _target_conflicts(
            input_contract=input_contract,
            expected_manifests=expected_manifests,
            expected_modes=expected_modes,
            package_rows=target_package_rows,
            program_rows=target_program_rows,
            binding_rows=target_binding_rows,
        )
        if conflicts:
            top_reasons.add(REASON_TARGET_CONFLICT)

        classification = (
            InventoryClassification.TARGET_CONFLICT
            if conflicts
            else InventoryClassification.INPUT_INCOMPLETE
            if top_reasons
            else InventoryClassification.DUAL_TRACK_AVAILABLE
        )
        common_dates = _common_completed_dates(candidates)
        relation_counts = {
            "source.app.advisory_program": len(source_program_rows),
            "source.app.advisory_strategy_binding_version": len(source_binding_rows),
            "source.selection.daily_selection_evidence_groups": len(dse_rows),
            "source.strategy_pkg.package": len(package_rows),
            "source.strategy_pkg.package_asset": sum(counts_by_id.values()),
            "target.app.advisory_program": len(target_program_rows),
            "target.app.advisory_strategy_binding_version": len(target_binding_rows),
            "target.strategy_pkg.package": len(target_package_rows),
        }
        return RealDevOnboardingInventoryReceipt(
            inventory_invocation_id=f"adv_real_dev_inv_{uuid4().hex}",
            source_database_identity=source_identity,
            target_database_identity=target_identity,
            release_receipt_ref=input_contract.release_receipt_ref,
            release_catalog_fingerprint=release_catalog_fingerprint,
            program_candidates=tuple(candidates),
            common_completed_trade_dates=common_dates,
            selected_input_ref=selected_input_ref,
            selected_request_hash=(
                input_contract.request_hash if isinstance(input_contract, RealDevOnboardingRequest) else None
            ),
            selected_inventory_query_hash=(
                input_contract.inventory_query_hash
                if isinstance(input_contract, RealDevOnboardingInventoryQuery)
                else None
            ),
            relation_row_counts=relation_counts,
            dependency_closure_hash=None,
            classification=classification,
            reason_codes=tuple(sorted(top_reasons)),
            observed_at=observed_at,
        )


@dataclass(frozen=True)
class ProductionBundleExportResult:
    bundle: PortableAdvisoryEvidenceBundle
    bundle_ref: OnboardingArtifactRef
    idempotent: bool


class RealDevProductionPackageExporter:
    """Export the exact immutable package closure from production without DB writes."""

    def __init__(self, *, connector: Connector = psycopg2.connect) -> None:
        self._connector = connector

    def export(
        self,
        *,
        request: RealDevOnboardingRequest,
        request_ref: OnboardingArtifactRef,
        inventory: RealDevOnboardingInventoryReceipt,
        env_file: Path,
        evidence_store: RealDevOnboardingEvidenceStore,
        source_package_asset_root: Path,
        target_package_asset_root: Path,
    ) -> ProductionBundleExportResult:
        _validate_export_authority(request=request, request_ref=request_ref, inventory=inventory)
        roots = resolve_package_asset_roots(
            source_root=source_package_asset_root,
            target_root=target_package_asset_root,
        )
        try:
            source_config = resolve_database_connection(target_label=TargetLabel.PRODUCTION, env_file=env_file)
        except ReleaseSchemaVerificationError as exc:
            raise RealDevOnboardingError(
                REASON_ENV_INVALID,
                "unable to resolve exact production connection for package export",
                context={"error_type": type(exc).__name__},
            ) from exc
        with readonly_onboarding_connection(source_config, connector=self._connector) as source_connection:
            source = FixedReadOnlyProjection(source_connection, source_config)
            source_identity = source.identity()
            if database_identity_hash(source_identity) != database_identity_hash(inventory.source_database_identity):
                raise RealDevOnboardingError(
                    REASON_BUNDLE_EXPORT_FAILED,
                    "production export identity differs from the inventory authority",
                )
            snapshot = source.one("export_snapshot_identity", ())
            package_ids = list(request.source_package_ids)
            package_rows = source.all("export_packages", (package_ids,))
            asset_rows = source.all("export_package_assets", (package_ids,))
            source_program_ids = list(request.source_program_refs)
            source_program_rows = source.all("source_programs", (source_program_ids,)) if source_program_ids else []
            source_binding_rows = source.all("source_bindings", (source_program_ids,)) if source_program_ids else []
            if source_program_ids:
                if {str(row["program_id"]) for row in source_program_rows} != set(source_program_ids):
                    raise RealDevOnboardingError(
                        REASON_SOURCE_PROGRAM_MISSING,
                        "production source Program provenance changed after inventory",
                    )
                program_packages = _source_program_packages(source_binding_rows)
                referenced_packages = {package_id for values in program_packages.values() for package_id in values}
                if not set(request.source_package_ids).issubset(referenced_packages):
                    raise RealDevOnboardingError(
                        REASON_SOURCE_PROGRAM_MISSING,
                        "production source Program no longer references the requested package closure",
                    )
            actual_asset_counts = {
                package_id: sum(str(row.get("package_id")) == package_id for row in asset_rows)
                for package_id in request.source_package_ids
            }
            expected_asset_counts = {
                item.package_id: item.package_asset_count for item in inventory.program_candidates
            }
            if actual_asset_counts != expected_asset_counts:
                raise RealDevOnboardingError(
                    REASON_BUNDLE_EXPORT_FAILED,
                    "production package asset closure changed after inventory; rerun exact inventory",
                )
            if source.write_query_count:
                raise RealDevOnboardingError(
                    REASON_READONLY_ASSERTION_FAILED,
                    "production package export recorded a write query",
                )
            bundle = _build_portable_bundle(
                request=request,
                source_identity=source_identity,
                export_snapshot_identity=str(snapshot["snapshot_identity"]),
                package_rows=package_rows,
                asset_rows=asset_rows,
                source_package_asset_root=roots.source_readonly_root,
                evidence_store=evidence_store,
            )
        stored = evidence_store.publish(bundle)
        readback = evidence_store.load(stored.ref)
        evidence_store.verify_reference_closure(readback)
        if not isinstance(readback, PortableAdvisoryEvidenceBundle) or readback.bundle_content_hash != bundle.bundle_content_hash:
            raise RealDevOnboardingError(
                REASON_BUNDLE_EXPORT_FAILED,
                "portable bundle full readback differs from the exported closure",
            )
        return ProductionBundleExportResult(bundle=bundle, bundle_ref=stored.ref, idempotent=stored.idempotent)


def _validate_export_authority(
    *,
    request: RealDevOnboardingRequest,
    request_ref: OnboardingArtifactRef,
    inventory: RealDevOnboardingInventoryReceipt,
) -> None:
    if request_ref.evidence_kind is not EvidenceKind.REQUEST or request_ref.semantic_content_hash != request.request_hash:
        raise RealDevOnboardingError(REASON_REQUEST_INVALID, "bundle export request ref differs from its request")
    if (
        inventory.classification is not InventoryClassification.DUAL_TRACK_AVAILABLE
        or inventory.selected_input_ref != request_ref
        or inventory.selected_request_hash != request.request_hash
        or inventory.release_receipt_ref != request.release_receipt_ref
    ):
        raise RealDevOnboardingError(
            REASON_BUNDLE_EXPORT_FAILED,
            "bundle export requires the exact successful request-driven inventory",
        )
    candidates = {item.package_id: item for item in inventory.program_candidates if item.package_eligible}
    if set(candidates) != set(request.source_package_ids):
        raise RealDevOnboardingError(REASON_BUNDLE_EXPORT_FAILED, "inventory package closure differs from the request")
    expected_modes = {item.package_id: item.alpha_mode for item in request.target_dev_program_specs}
    for package_id, expected_sha in request.expected_package_manifest_sha256s.items():
        candidate = candidates[package_id]
        if candidate.manifest_sha256 != expected_sha or candidate.alpha_mode is not expected_modes[package_id]:
            raise RealDevOnboardingError(
                REASON_BUNDLE_EXPORT_FAILED,
                "inventory package identity differs from the request",
                context={"package_id": package_id},
            )


def _build_portable_bundle(
    *,
    request: RealDevOnboardingRequest,
    source_identity: DatabaseIdentity,
    export_snapshot_identity: str,
    package_rows: list[dict[str, Any]],
    asset_rows: list[dict[str, Any]],
    source_package_asset_root: Path,
    evidence_store: RealDevOnboardingEvidenceStore,
) -> PortableAdvisoryEvidenceBundle:
    expected_ids = set(request.source_package_ids)
    if {str(row.get("package_id")) for row in package_rows} != expected_ids or len(package_rows) != len(expected_ids):
        raise RealDevOnboardingError(REASON_PACKAGE_MISSING, "production export package rows differ from the request")
    expected_modes = {item.package_id: item.alpha_mode for item in request.target_dev_program_specs}
    package_refs: list[BundlePackageRef] = []
    component_refs: list[NativeMultiComponentRef] = []
    manifest_asset_refs: dict[str, dict[str, str]] = {}
    for row in package_rows:
        package_id = str(row["package_id"])
        manifest_sha = validate_sha256(str(row["manifest_sha256"]), field_name="manifest_sha256")
        if manifest_sha != request.expected_package_manifest_sha256s[package_id]:
            raise RealDevOnboardingError(
                REASON_PACKAGE_MANIFEST_MISMATCH,
                "production export manifest hash differs from the request",
                context={"package_id": package_id},
            )
        manifest = _json_object(row["manifest_json"], field_name="manifest_json")
        computed_manifest_sha = _compute_manifest_json_sha256(manifest)
        package_status = str(row.get("package_status") or "").upper()
        if (
            str(manifest.get("package_id") or "") != package_id
            or str(manifest.get("manifest_sha256") or "").lower() != manifest_sha
            or computed_manifest_sha != manifest_sha
            or str(manifest.get("alpha_mode") or "") != expected_modes[package_id].value
            or str(row.get("alpha_mode") or "") != expected_modes[package_id].value
            or package_status not in ALLOWED_EXPORT_PACKAGE_STATUSES
        ):
            raise RealDevOnboardingError(
                REASON_PACKAGE_MANIFEST_MISMATCH,
                "production export manifest identity is inconsistent",
                context={"package_id": package_id},
            )
        components = _components(manifest)
        if (expected_modes[package_id] is AlphaMode.SINGLE and len(components) != 1) or (
            expected_modes[package_id] is AlphaMode.MULTI and len(components) < 2
        ):
            raise RealDevOnboardingError(
                REASON_PACKAGE_MANIFEST_MISMATCH,
                "production export alpha component closure is invalid",
                context={"package_id": package_id},
            )
        package_refs.append(
            BundlePackageRef(package_id=package_id, manifest_sha256=manifest_sha, alpha_mode=expected_modes[package_id])
        )
        if expected_modes[package_id] is AlphaMode.MULTI:
            component_refs.extend(
                NativeMultiComponentRef(parent_package_id=package_id, component=component)
                for component in components
            )
        _assert_portable_payload({name: row.get(name) for name in PACKAGE_SEMANTIC_COLUMNS})
        runtime_asset_refs = _manifest_runtime_asset_refs(manifest)
        if not runtime_asset_refs:
            raise RealDevOnboardingError(
                REASON_PACKAGE_ASSET_MISSING,
                "production export manifest has no governed runtime asset closure",
                context={"package_id": package_id},
        )
        manifest_asset_refs[package_id] = runtime_asset_refs

    asset_rows_by_package_ref: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in asset_rows:
        package_id = str(row.get("package_id"))
        if package_id not in expected_ids:
            raise RealDevOnboardingError(REASON_BUNDLE_EXPORT_FAILED, "package asset row is outside the request closure")
        normalized = dict(row)
        normalized["asset_sha256"] = validate_sha256(str(row.get("asset_sha256") or ""), field_name="asset_sha256")
        size = row.get("asset_size_bytes")
        if size is not None and int(size) < 0:
            raise RealDevOnboardingError(REASON_BUNDLE_EXPORT_FAILED, "package asset size is negative")
        _assert_portable_payload({name: normalized.get(name) for name in PACKAGE_ASSET_SEMANTIC_COLUMNS})
        asset_rows_by_package_ref.setdefault((package_id, str(row["asset_ref"])), []).append(normalized)

    blob_refs: list[BundleBlobRef] = []
    normalized_assets: list[dict[str, Any]] = []
    for package_id, required_refs in manifest_asset_refs.items():
        for asset_ref, expected_sha in required_refs.items():
            ledger_rows = asset_rows_by_package_ref.get((package_id, asset_ref), [])
            matching_rows = [row for row in ledger_rows if row["asset_sha256"] == expected_sha]
            if not matching_rows:
                raise RealDevOnboardingError(
                    REASON_PACKAGE_ASSET_MISSING,
                    "manifest runtime asset is absent from the package ledger",
                    context={"package_id": package_id, "asset_ref_hash": hashlib.sha256(asset_ref.encode()).hexdigest()},
                )
            raw = _read_source_package_blob(source_package_asset_root, expected_sha)
            for row in matching_rows:
                normalized_assets.append(row)
                expected_size = row.get("asset_size_bytes")
                if expected_size is not None and int(expected_size) != len(raw):
                    raise RealDevOnboardingError(
                        REASON_BUNDLE_EXPORT_FAILED,
                        "package asset size differs from its immutable blob",
                        context={"package_id": package_id, "asset_type": str(row["asset_type"])},
                    )
                stored_blob = evidence_store.publish_blob(raw=raw, expected_sha256=expected_sha)
                blob_refs.append(
                    BundleBlobRef(
                        package_id=package_id,
                        asset_type=str(row["asset_type"]),
                        asset_ref=asset_ref,
                        blob_ref=stored_blob.ref,
                    )
                )

    package_row_set = PortableRelationRowSet(
        relation_name="strategy_pkg.package",
        primary_or_natural_key_fields=("package_id",),
        semantic_column_names=PACKAGE_SEMANTIC_COLUMNS,
        source_provenance_column_names=PACKAGE_PROVENANCE_COLUMNS,
        sorted_rows=tuple(package_rows),
    )
    asset_row_set = PortableRelationRowSet(
        relation_name="strategy_pkg.package_asset",
        primary_or_natural_key_fields=("package_id", "asset_type", "asset_ref"),
        semantic_column_names=PACKAGE_ASSET_SEMANTIC_COLUMNS,
        source_provenance_column_names=PACKAGE_ASSET_PROVENANCE_COLUMNS,
        sorted_rows=tuple(normalized_assets),
    )
    edges: list[DependencyEdge] = [
        DependencyEdge(
            parent_identity=item.parent_package_id,
            child_identity=f"alpha_component:{item.parent_package_id}:{item.component.alpha_id}",
            relation="PACKAGE_COMPONENT",
        )
        for item in component_refs
    ]
    for row in normalized_assets:
        package_id = str(row["package_id"])
        asset_identity = f"package_asset:{package_id}:{row['asset_type']}:{row['asset_ref']}"
        edges.append(
            DependencyEdge(parent_identity=package_id, child_identity=asset_identity, relation="PACKAGE_ASSET")
        )
    for item in blob_refs:
        asset_identity = f"package_asset:{item.package_id}:{item.asset_type}:{item.asset_ref}"
        edges.append(
            DependencyEdge(parent_identity=asset_identity, child_identity=f"sha256:{item.asset_sha256}", relation="ASSET_BLOB")
        )
    try:
        return PortableAdvisoryEvidenceBundle(
            request=request,
            source_database_identity_hash=database_identity_hash(source_identity),
            export_snapshot_identity=export_snapshot_identity,
            package_refs=tuple(package_refs),
            native_multi_component_refs=tuple(component_refs),
            relation_row_sets=(package_row_set, asset_row_set),
            artifact_blob_refs=tuple(blob_refs),
            dependency_edges=tuple(edges),
        )
    except ValueError as exc:
        raise RealDevOnboardingError(
            REASON_BUNDLE_EXPORT_FAILED,
            "portable production package closure is invalid",
            context={"error_type": type(exc).__name__},
        ) from exc


def _manifest_runtime_asset_refs(manifest: Mapping[str, Any]) -> dict[str, str]:
    try:
        return portable_manifest_runtime_asset_refs(manifest)
    except ValueError as exc:
        raise RealDevOnboardingError(
            REASON_PACKAGE_MANIFEST_MISMATCH,
            "manifest runtime asset closure is invalid",
            context={"error_type": type(exc).__name__},
        ) from exc


def _compute_manifest_json_sha256(manifest: Mapping[str, Any]) -> str:
    return compute_portable_manifest_json_sha256(manifest)


def _read_source_package_blob(root: Path, digest: str) -> bytes:
    normalized = validate_sha256(digest, field_name="asset_sha256")
    path = root / "blobs" / normalized[:2] / normalized
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
        _assert_no_reparse_from_root(root=root, path=resolved)
        if not resolved.is_file():
            raise OSError("package asset blob is not a regular file")
        raw = resolved.read_bytes()
    except (OSError, ValueError) as exc:
        raise RealDevOnboardingError(
            REASON_PACKAGE_ASSET_MISSING,
            "immutable package asset blob cannot be resolved from the explicit source root",
            context={"asset_sha256": normalized},
        ) from exc
    if hashlib.sha256(raw).hexdigest() != normalized:
        raise RealDevOnboardingError(
            REASON_BUNDLE_EXPORT_FAILED,
            "source package asset blob hash differs from its ledger authority",
            context={"asset_sha256": normalized},
        )
    return raw


def _assert_no_reparse_from_root(*, root: Path, path: Path) -> None:
    relative = path.relative_to(root)
    current = root
    for part in relative.parts:
        current = current / part
        attributes = os.lstat(current)
        if stat.S_ISLNK(attributes.st_mode) or (
            getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise ValueError("package asset path contains a symlink or reparse point")


def _assert_portable_payload(value: Any, *, key_name: str | None = None) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized_key = str(key).lower().replace("-", "_")
            if normalized_key in {"password", "passwd", "secret", "token", "api_key"} and item not in (None, ""):
                raise RealDevOnboardingError(REASON_BUNDLE_EXPORT_FAILED, "portable bundle contains a credential-like field")
            _assert_portable_payload(item, key_name=normalized_key)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _assert_portable_payload(item, key_name=key_name)
        return
    if not isinstance(value, str):
        return
    normalized = value.replace("\\", "/")
    if (
        normalized.startswith(("/", "//", "file://"))
        or (len(normalized) >= 3 and normalized[0].isalpha() and normalized[1:3] == ":/")
        or normalized.lower().startswith(("/mnt/", "//wsl$/", "//wsl.localhost/"))
    ):
        raise RealDevOnboardingError(
            REASON_BUNDLE_EXPORT_FAILED,
            "portable bundle contains an absolute workstation path",
            context={"field": key_name},
        )


def _candidate_from_row(
    *,
    row: Mapping[str, Any],
    expected_manifest: str,
    expected_alpha_mode: AlphaMode,
    asset_count: int,
    source_program_refs: tuple[str, ...],
    binding_rows: list[dict[str, Any]],
    dse_summary: dict[str, tuple[int, tuple[date, ...]]],
    decision_trade_date: date,
) -> PackageInventoryCandidate:
    package_id = str(row["package_id"])
    reasons: set[str] = set()
    manifest_sha256 = str(row["manifest_sha256"]).lower()
    manifest = _json_object(row["manifest_json"], field_name="manifest_json")
    embedded_hash = str(manifest.get("manifest_sha256") or "").lower()
    computed_hash = _compute_manifest_json_sha256(manifest)
    if (
        manifest_sha256 != expected_manifest
        or embedded_hash != manifest_sha256
        or computed_hash != manifest_sha256
        or str(manifest.get("package_id") or "") != package_id
        or str(manifest.get("alpha_mode") or "") != str(row["alpha_mode"])
    ):
        reasons.add(REASON_PACKAGE_MANIFEST_MISMATCH)
    try:
        alpha_mode = AlphaMode(str(row["alpha_mode"]))
    except ValueError:
        alpha_mode = AlphaMode.SINGLE
        reasons.add(REASON_PACKAGE_MANIFEST_MISMATCH)
    if alpha_mode is not expected_alpha_mode:
        reasons.add(REASON_PACKAGE_MANIFEST_MISMATCH)
    try:
        components = _components(manifest)
    except (ValueError, TypeError):
        components = ()
        reasons.add(REASON_PACKAGE_MANIFEST_MISMATCH)
    if (alpha_mode is AlphaMode.SINGLE and len(components) != 1) or (
        alpha_mode is AlphaMode.MULTI and len(components) < 2
    ):
        reasons.add(REASON_PACKAGE_MANIFEST_MISMATCH)
    package_status = str(row.get("package_status") or "UNKNOWN").upper()
    if package_status not in ALLOWED_EXPORT_PACKAGE_STATUSES:
        reasons.add(REASON_PACKAGE_MANIFEST_MISMATCH)
    runtime_asset_refs = _manifest_runtime_asset_refs(manifest)
    if asset_count <= 0:
        reasons.add(REASON_PACKAGE_ASSET_MISSING)
    if not runtime_asset_refs:
        reasons.add(REASON_PACKAGE_ASSET_MISSING)
    relevant_bindings = [
        item for item in binding_rows if package_id in tuple(str(value) for value in (item.get("package_ids") or ()))
    ]
    binding_eligible = any(
        item.get("effective_from_trade_date") is not None
        and item["effective_from_trade_date"] <= decision_trade_date
        and (item.get("effective_to_trade_date") is None or decision_trade_date < item["effective_to_trade_date"])
        and str(item.get("activation_status") or "").upper() == "ACTIVE"
        for item in relevant_bindings
    )
    dse_v2_dates = tuple(
        sorted(
            {
                trade_date
                for schema_version, (_, dates) in dse_summary.items()
                if schema_version == "daily_selection_evidence_v2"
                for trade_date in dates
            }
        )
    )
    return PackageInventoryCandidate(
        package_id=package_id,
        manifest_sha256=manifest_sha256,
        alpha_mode=alpha_mode,
        package_status=package_status,
        components=components,
        package_asset_count=asset_count,
        has_runtime_assets=bool(runtime_asset_refs),
        has_source_evidence=bool(manifest.get("source_evidence")),
        closure_status=(
            PackageClosureStatus.O2_EXPORT_VERIFICATION_REQUIRED
            if not reasons
            else PackageClosureStatus.INPUT_INCOMPLETE
        ),
        source_program_refs=source_program_refs,
        dse_schema_counts={name: count for name, (count, _) in sorted(dse_summary.items())},
        completed_dse_v2_trade_dates=dse_v2_dates,
        binding_fact_eligibility=(
            SourceFactEligibility.ELIGIBLE
            if binding_eligible
            else SourceFactEligibility.LEGACY_BINDING_INELIGIBLE
            if relevant_bindings
            else SourceFactEligibility.MISSING
        ),
        dse_fact_eligibility=(
            SourceFactEligibility.ELIGIBLE
            if decision_trade_date in dse_v2_dates
            else SourceFactEligibility.DSE_V1_INELIGIBLE
            if dse_summary
            else SourceFactEligibility.MISSING
        ),
        package_eligible=not reasons,
        reason_codes=tuple(sorted(reasons)),
    )


def _components(manifest: Mapping[str, Any]) -> tuple[AlphaComponentEvidence, ...]:
    raw_components = manifest.get("alpha_components")
    if not isinstance(raw_components, list):
        return ()
    values: list[AlphaComponentEvidence] = []
    for raw in raw_components:
        if not isinstance(raw, dict):
            continue
        values.append(
            AlphaComponentEvidence(
                alpha_id=str(raw.get("alpha_id") or ""),
                alpha_name=str(raw.get("alpha_name") or ""),
                component_weight=float(raw.get("component_weight")),
                model_id=str(raw["model_id"]) if raw.get("model_id") is not None else None,
                holding_period=str(raw.get("holding_period") or ""),
                rebalance_frequency=str(raw.get("rebalance_frequency") or ""),
                score_direction=str(raw.get("score_direction") or ""),
                score_normalization=str(raw.get("score_normalization") or ""),
                factor_ids=tuple(str(value) for value in (raw.get("factor_ids") or ())),
            )
        )
    return tuple(values)


def _group_dse(rows: list[dict[str, Any]]) -> dict[str, dict[str, tuple[int, tuple[date, ...]]]]:
    result: dict[str, dict[str, tuple[int, tuple[date, ...]]]] = {}
    for row in rows:
        package_id = str(row["package_id"])
        schema_version = str(row["schema_version"])
        dates = tuple(sorted(value for value in (row.get("trade_dates") or ()) if isinstance(value, date)))
        result.setdefault(package_id, {})[schema_version] = (int(row["evidence_count"]), dates)
    return result


def _source_program_packages(rows: list[dict[str, Any]]) -> dict[str, tuple[str, ...]]:
    result: dict[str, set[str]] = {}
    for row in rows:
        result.setdefault(str(row["program_id"]), set()).update(str(value) for value in (row.get("package_ids") or ()))
    return {key: tuple(sorted(value)) for key, value in result.items()}


def _target_conflicts(
    *,
    input_contract: InventoryInput,
    expected_manifests: Mapping[str, str],
    expected_modes: Mapping[str, AlphaMode],
    package_rows: list[dict[str, Any]],
    program_rows: list[dict[str, Any]],
    binding_rows: list[dict[str, Any]],
) -> tuple[str, ...]:
    conflicts: set[str] = set()
    for row in package_rows:
        package_id = str(row["package_id"])
        if (
            str(row["manifest_sha256"]).lower() != expected_manifests.get(package_id)
            or str(row.get("alpha_mode") or "") != expected_modes[package_id].value
        ):
            conflicts.add(f"package:{package_id}")
    specs = {item.program_id: item for item in input_contract.target_dev_program_specs}
    for row in program_rows:
        program_id = str(row["program_id"])
        spec = specs[program_id]
        if int(row["target_count"]) != spec.target_count or canonical_json_sha256(row["review_policy"]) != canonical_json_sha256(spec.review_policy):
            conflicts.add(f"program:{program_id}")
    for row in binding_rows:
        program_id = str(row["program_id"])
        expected_package = specs[program_id].package_id
        package_ids = tuple(str(value) for value in (row.get("package_ids") or ()))
        effective_from = row.get("effective_from_trade_date")
        effective_to = row.get("effective_to_trade_date")
        if str(row.get("activation_status") or "").upper() != "ACTIVE":
            continue
        overlaps = effective_from is None or (
            effective_from <= input_contract.decision_trade_date
            and (effective_to is None or input_contract.decision_trade_date < effective_to)
        )
        exact = (
            str(row.get("package_mode") or "") == "single_package"
            and package_ids == (expected_package,)
            and effective_from == input_contract.binding_effective_from_trade_date
            and effective_to is None
        )
        if overlaps and not exact:
            conflicts.add(f"binding:{program_id}")
    return tuple(sorted(conflicts))


def _common_completed_dates(candidates: list[PackageInventoryCandidate]) -> tuple[date, ...]:
    eligible = [set(item.completed_dse_v2_trade_dates) for item in candidates if item.package_eligible]
    if not eligible:
        return ()
    return tuple(sorted(set.intersection(*eligible)))


def _assert_distinct_physical_databases(source: DatabaseIdentity, target: DatabaseIdentity) -> None:
    source_key = (source.current_database, source.server_address, source.server_port)
    target_key = (target.current_database, target.server_address, target.server_port)
    if source_key == target_key:
        raise RealDevOnboardingError(
            REASON_SOURCE_TARGET_IDENTITY_COLLISION,
            "production source and DEV target resolve to the same physical database identity",
        )


def _validate_selected_input_ref(*, input_contract: InventoryInput, selected_input_ref: OnboardingArtifactRef) -> None:
    if isinstance(input_contract, RealDevOnboardingRequest):
        expected_kind = EvidenceKind.REQUEST
        expected_hash = input_contract.request_hash
    else:
        expected_kind = EvidenceKind.INVENTORY_QUERY
        expected_hash = input_contract.inventory_query_hash
    if selected_input_ref.evidence_kind is not expected_kind or selected_input_ref.semantic_content_hash != expected_hash:
        raise RealDevOnboardingError(
            REASON_REQUEST_INVALID,
            "selected onboarding input ref does not match its explicit contract",
        )


def load_exact_release_receipt(*, ref: Any, root: Path) -> ReleaseSchemaReceipt:
    try:
        binding = Phase1GArtifactRootBinding(
            artifact_kind=Phase1GInputArtifactKind.PHASE1F2_RELEASE_RECEIPT,
            root=root,
            expected_store_policy_hash=PHASE1F2_RELEASE_RECEIPT_LAYOUT_POLICY.layout_policy_hash,
        )
        relative = Path(ref.relative_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("release receipt relative path is invalid")
        candidate = binding.root / relative
        current = binding.root
        for part in relative.parts:
            current = current / part
            attributes = os.lstat(current)
            if stat.S_ISLNK(attributes.st_mode) or (
                getattr(attributes, "st_file_attributes", 0) & _FILE_ATTRIBUTE_REPARSE_POINT
            ):
                raise ValueError("release receipt path contains a symlink or reparse point")
        path = candidate.resolve(strict=True)
        path.relative_to(binding.root)
        if not path.is_file():
            raise ValueError("release receipt is not a regular file")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != ref.file_sha256:
            raise ValueError("release receipt raw hash differs")
        document = json.loads(raw.decode("utf-8"))
        receipt = ReleaseSchemaReceipt.model_validate(document)
        if receipt.receipt_content_hash != ref.semantic_content_hash:
            raise ValueError("release receipt semantic hash differs")
        return receipt
    except Exception as exc:
        raise RealDevOnboardingError(
            REASON_RELEASE_RECEIPT_INVALID,
            "exact Phase 1F.2 release receipt cannot be resolved",
            context={"error_type": type(exc).__name__},
        ) from exc


def _json_object(value: Any, *, field_name: str) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise RealDevOnboardingError(REASON_PACKAGE_MANIFEST_MISMATCH, f"{field_name} is invalid JSON") from exc
        if isinstance(parsed, dict):
            return parsed
    raise RealDevOnboardingError(REASON_PACKAGE_MANIFEST_MISMATCH, f"{field_name} must be a JSON object")
