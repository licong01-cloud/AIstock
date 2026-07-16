"""Fixed-query, read-only production/DEV inventory for Advisory onboarding."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
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
    EvidenceKind,
    InventoryClassification,
    OnboardingArtifactRef,
    PackageClosureStatus,
    PackageInventoryCandidate,
    RealDevOnboardingError,
    RealDevOnboardingInventoryReceipt,
    RealDevOnboardingInventoryQuery,
    RealDevOnboardingRequest,
    REASON_DATABASE_CONNECTION_FAILED,
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
    database_identity_hash,
)


LOGGER = logging.getLogger(__name__)
Connector = Callable[..., Any]
InventoryInput = RealDevOnboardingRequest | RealDevOnboardingInventoryQuery

READONLY_STATEMENT_TIMEOUT_MS = 120_000
_FILE_ATTRIBUTE_REPARSE_POINT = 0x0400

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
        SELECT package_id, source_id, manifest_json, manifest_sha256, alpha_mode, data_vintage
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
    if (
        manifest_sha256 != expected_manifest
        or embedded_hash != manifest_sha256
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
    package_status = str(manifest.get("package_status") or "UNKNOWN")
    if asset_count <= 0:
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
        has_runtime_assets=bool(manifest.get("runtime_assets")),
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
