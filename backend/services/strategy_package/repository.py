"""Persistence for Strategy Package Center.

The repository stores immutable frozen manifests plus a mutable package status
column. QE source tables are read-only from this layer.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterator

import psycopg2.extras
from psycopg2 import errors as pg_errors
from pydantic import BaseModel, ConfigDict, Field

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    StrategyPackageValidationError,
)

from .execution_policy import ValidatedExecutionPolicy
from .manifest import classify_manifest_hash_drift, compute_manifest_sha256, freeze_manifest
from .model_state import (
    ModelRetrainJobStatus,
    ModelStalenessStatus,
    StrategyPackageModelRetrainJob,
    StrategyPackageModelState,
)
from .models import (
    AlphaMode,
    LiveApprovalStatus,
    PackageStatus,
    StrategyPackageComponentRecord,
    StrategyPackageLiveApproval,
    StrategyPackageManifest,
)
from .package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from .runtime_variant import (
    RuntimeVariantKind,
    RuntimeVariantValidationStatus,
    StrategyPackageRuntimeVariant,
    derive_locked_core_hash,
    ensure_runtime_variant_status,
)
from .validation_run import (
    PackageValidationRetrainMode,
    PackageValidationReproducibility,
    PackageValidationStatus,
    PackageValidationType,
    StrategyPackageValidationRun,
    ensure_package_validation_run,
)

logger = logging.getLogger("aistock.strategy_package.repository")

ConnFactory = Callable[[], Iterator[Any]]


SAFE_MANIFEST_REPAIR_CLASSIFICATION = "A_schema_evolution_stale_hash"


def _manifest_drift_repair_plan(
    *,
    stored_sha256: str | None,
    computed_sha256: str | None,
    manifest_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    classification = classify_manifest_hash_drift(
        manifest_json=manifest_json,
        stored_sha256=stored_sha256,
        computed_sha256=computed_sha256,
    )
    return {
        "recommended_action": (
            "repair_manifest_hash"
            if classification["repair_allowed"]
            else "quarantine_manual_review"
        ),
        "mutates_manifest_json": False,
        "requires_operator_confirmation": True,
        "confirm_stored_sha256": stored_sha256,
        "confirm_computed_sha256": computed_sha256,
        "confirm_repair_classification": classification["classification"],
        "classification": classification,
        "rollback_restore": {
            "field": "strategy_pkg.package.manifest_sha256",
            "restore_value": stored_sha256,
            "audit_event_reason": "manifest_hash_repaired",
        },
    }


def _manifest_json_for_record_classification(record: "StrategyPackageRecord") -> dict[str, Any]:
    payload = record.current_manifest().model_dump(mode="json")
    fields_set = getattr(record.manifest, "model_fields_set", set())
    for default_key in ("source_evidence", "backtest_context"):
        if default_key not in fields_set and payload.get(default_key) == {}:
            payload.pop(default_key, None)
    return payload


def _infer_data_vintage(manifest: StrategyPackageManifest) -> date | None:
    for value in (
        manifest.backtest_summary.raw_metrics.get("data_vintage"),
        manifest.backtest_summary.raw_metrics.get("sample_end"),
        manifest.backtest_summary.raw_metrics.get("end_date"),
        manifest.source_evidence.get("data_vintage"),
        manifest.backtest_context.get("data_vintage"),
    ):
        if isinstance(value, date):
            return value
        text = str(value or "").strip()
        if not text:
            continue
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            continue
    return None


def _validate_asset_records_for_manifest(
    manifest: StrategyPackageManifest,
    assets: list[StrategyPackageAssetRecord],
) -> None:
    if not assets:
        raise StrategyPackageValidationError(
            "frozen strategy package assets are required",
            context={"reason_code": "strategy_package_assets_missing", "package_id": manifest.package_id},
        )
    expected = _expected_manifest_asset_keys(manifest)
    actual = {
        (asset.asset_type, asset.asset_ref, asset.asset_sha256)
        for asset in assets
    }
    missing = sorted(
        {
            (asset_type.value, asset_ref, sha256)
            for asset_type, asset_ref, sha256 in expected
            if (asset_type, asset_ref, sha256) not in actual
        }
    )
    if missing:
        raise StrategyPackageValidationError(
            "package_asset ledger rows do not cover frozen manifest assets",
            context={
                "reason_code": "strategy_package_assets_incomplete",
                "package_id": manifest.package_id,
                "missing_assets": missing,
            },
        )
    unexpected = sorted(
        {
            (asset.asset_type.value, asset.asset_ref, asset.asset_sha256)
            for asset in assets
            if (asset.asset_type, asset.asset_ref, asset.asset_sha256) not in expected
        }
    )
    if unexpected:
        raise StrategyPackageValidationError(
            "package_asset ledger rows must match frozen manifest assets exactly",
            context={
                "reason_code": "strategy_package_assets_unexpected",
                "package_id": manifest.package_id,
                "unexpected_assets": unexpected,
            },
        )
    for asset in assets:
        if asset.package_id != manifest.package_id:
            raise StrategyPackageValidationError(
                "package_asset package_id must match manifest package_id",
                context={
                    "reason_code": "strategy_package_asset_package_mismatch",
                    "package_id": manifest.package_id,
                    "asset_package_id": asset.package_id,
                    "asset_ref": asset.asset_ref,
                },
            )
        if asset.asset_type not in {
            StrategyPackageAssetType.MODEL_WEIGHT,
            StrategyPackageAssetType.FACTOR_CODE,
            StrategyPackageAssetType.FACTOR_SCHEMA,
            StrategyPackageAssetType.MODEL_CODE,
        }:
            raise StrategyPackageValidationError(
                "package freeze only accepts runtime-owned model, factor, schema, and model-code assets",
                context={
                    "reason_code": "strategy_package_asset_unexpected_type",
                    "package_id": manifest.package_id,
                    "asset_type": asset.asset_type.value,
                    "asset_ref": asset.asset_ref,
                },
            )
        if _looks_like_backtest_prediction(asset.asset_ref):
            raise StrategyPackageValidationError(
                "QE backtest prediction artifacts must not be recorded as package runtime assets",
                context={
                    "reason_code": "strategy_package_prediction_asset_forbidden",
                    "package_id": manifest.package_id,
                    "asset_type": asset.asset_type.value,
                    "asset_ref": asset.asset_ref,
                },
            )


def _expected_manifest_asset_keys(
    manifest: StrategyPackageManifest,
) -> set[tuple[StrategyPackageAssetType, str, str]]:
    expected: set[tuple[StrategyPackageAssetType, str, str]] = set()
    for factor in manifest.factor_set:
        if not (factor.asset_ref and factor.sha256):
            raise StrategyPackageValidationError(
                "manifest factor asset is not frozen",
                context={
                    "reason_code": "strategy_package_assets_incomplete",
                    "package_id": manifest.package_id,
                    "factor_id": factor.factor_id,
                    "factor_name": factor.factor_name,
                },
            )
        expected.add((StrategyPackageAssetType.FACTOR_CODE, factor.asset_ref, factor.sha256))
    model_assets = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    for model in model_assets:
        if not (model.asset_ref and model.sha256):
            raise StrategyPackageValidationError(
                "manifest model asset is not frozen",
                context={
                    "reason_code": "strategy_package_assets_incomplete",
                    "package_id": manifest.package_id,
                    "model_id": model.model_id,
                },
            )
        expected.add((StrategyPackageAssetType.MODEL_WEIGHT, model.asset_ref, model.sha256))
        for code_asset in model.model_code_assets:
            if not (code_asset.asset_ref and code_asset.sha256):
                raise StrategyPackageValidationError(
                    "manifest model code asset is not frozen",
                    context={
                        "reason_code": "strategy_package_assets_incomplete",
                        "package_id": manifest.package_id,
                        "model_id": model.model_id,
                        "relative_path": code_asset.relative_path,
                    },
                )
            expected.add((StrategyPackageAssetType.MODEL_CODE, code_asset.asset_ref, code_asset.sha256))
    runtime_assets = manifest.runtime_assets
    alpha158 = runtime_assets.alpha158 if runtime_assets is not None else None
    if alpha158 is not None and alpha158.enabled:
        if not (alpha158.asset_ref and alpha158.sha256):
            raise StrategyPackageValidationError(
                "manifest Alpha158 schema asset is not frozen",
                context={
                    "reason_code": "strategy_package_assets_incomplete",
                    "package_id": manifest.package_id,
                    "runtime_asset": "alpha158",
                },
            )
        expected.add((StrategyPackageAssetType.FACTOR_SCHEMA, alpha158.asset_ref, alpha158.sha256))
    return expected


def manifest_asset_keys(
    manifest: StrategyPackageManifest,
) -> set[tuple[StrategyPackageAssetType, str, str]]:
    """Public helper for callers that need parity with repository ledger validation."""

    return _expected_manifest_asset_keys(manifest)


def _looks_like_backtest_prediction(asset_ref: str) -> bool:
    text = str(asset_ref or "").lower()
    return "combined_prediction.pkl" in text or "pred.pkl" in text


def _asset_key_payload(assets: list[StrategyPackageAssetRecord]) -> list[dict[str, str | None]]:
    return [
        {
            "asset_type": asset.asset_type.value,
            "asset_ref": asset.asset_ref,
            "asset_sha256": asset.asset_sha256,
        }
        for asset in assets
    ]


class StrategyPackageRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    package_id: str
    package_name: str
    package_version: str
    source_type: str
    source_id: str
    loop_id: str | None = None
    run_id: str | None = None
    package_status: PackageStatus
    manifest: StrategyPackageManifest
    manifest_sha256: str
    alpha_mode: AlphaMode = AlphaMode.SINGLE_ALPHA
    signal_domain: str | None = None
    display_name: str | None = None
    legacy_name: str | None = None
    data_vintage: date | None = None
    prediction_ref_uri: str | None = None
    prediction_ref_sha256: str | None = None
    model_artifact_uri: str | None = None
    model_artifact_sha256: str | None = None
    paper_portfolio_count: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def current_manifest(self) -> StrategyPackageManifest:
        return self.manifest.model_copy(
            update={
                "package_status": self.package_status,
                "manifest_sha256": self.manifest_sha256,
            }
        )


class PackageStatusEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    package_id: str
    from_status: PackageStatus | None = None
    to_status: PackageStatus
    reason: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class StrategyPackageRepository:
    """PostgreSQL-backed repository for strategy packages."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def save_manifest(self, manifest: StrategyPackageManifest) -> StrategyPackageRecord:
        frozen = freeze_manifest(manifest)
        if not frozen.manifest_sha256:
            raise StrategyPackageValidationError(
                "manifest_sha256 is required before persistence",
                context={"package_id": frozen.package_id},
            )
        source_existing = self.find_by_source_version(
            source_type=frozen.source.source_type.value,
            source_id=frozen.source.source_id,
            loop_id=frozen.source.loop_id,
            package_version=frozen.package_version,
        )
        if source_existing:
            return source_existing
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT package_id, manifest_sha256, alpha_mode, signal_domain, display_name, legacy_name,
                           data_vintage, prediction_ref_uri, prediction_ref_sha256,
                           model_artifact_uri, model_artifact_sha256, paper_portfolio_count
                        FROM strategy_pkg.package
                        WHERE package_id = %s
                        """,
                        (frozen.package_id,),
                    )
                    existing = cur.fetchone()
                    if existing:
                        if existing["manifest_sha256"] != frozen.manifest_sha256:
                            raise InvalidStateTransitionError(
                                "package manifest cannot be silently replaced",
                                context={
                                    "package_id": frozen.package_id,
                                    "existing_manifest_sha256": existing["manifest_sha256"],
                                    "new_manifest_sha256": frozen.manifest_sha256,
                                    "paper_portfolio_count": existing["paper_portfolio_count"],
                                },
                            )
                        return self.get(frozen.package_id)

                    cur.execute(
                        """
                        INSERT INTO strategy_pkg.package (
                            package_id, package_name, package_version, source_type,
                            source_id, loop_id, run_id, package_status, manifest_json,
                           manifest_sha256, alpha_mode, signal_domain, display_name, legacy_name,
                           data_vintage, prediction_ref_uri, prediction_ref_sha256,
                           model_artifact_uri, model_artifact_sha256
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            frozen.package_id,
                            frozen.package_name,
                            frozen.package_version,
                            frozen.source.source_type.value,
                            frozen.source.source_id,
                            frozen.source.loop_id,
                            frozen.source.run_id,
                            frozen.package_status.value,
                            psycopg2.extras.Json(frozen.model_dump(mode="json")),
                            frozen.manifest_sha256,
                            frozen.alpha_mode.value,
                            None,
                            frozen.package_name,
                            frozen.package_name,
                            _infer_data_vintage(frozen),
                            None,
                            None,
                            None,
                            None,
                        ),
                    )
                    cur.execute(
                        """
                        INSERT INTO strategy_pkg.package_status_event (
                            package_id, from_status, to_status, reason, context
                        ) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            frozen.package_id,
                            None,
                            frozen.package_status.value,
                            "package_created",
                            psycopg2.extras.Json({"manifest_sha256": frozen.manifest_sha256}),
                        ),
                    )
        except pg_errors.UniqueViolation as exc:
            existing_by_source = self.find_by_source_version(
                source_type=frozen.source.source_type.value,
                source_id=frozen.source.source_id,
                loop_id=frozen.source.loop_id,
                package_version=frozen.package_version,
            )
            if existing_by_source:
                return existing_by_source
            raise InvalidStateTransitionError(
                "strategy package unique constraint collision",
                context={
                    "package_id": frozen.package_id,
                    "source_type": frozen.source.source_type.value,
                    "source_id": frozen.source.source_id,
                    "loop_id": frozen.source.loop_id,
                    "package_version": frozen.package_version,
                },
            ) from exc
        return self.get(frozen.package_id)

    def save_manifest_with_assets(
        self,
        manifest: StrategyPackageManifest,
        assets: list[StrategyPackageAssetRecord],
    ) -> StrategyPackageRecord:
        frozen = freeze_manifest(manifest)
        if not frozen.manifest_sha256:
            raise StrategyPackageValidationError(
                "manifest_sha256 is required before persistence",
                context={"package_id": frozen.package_id},
            )
        _validate_asset_records_for_manifest(frozen, assets)
        source_existing = self.find_by_source_version(
            source_type=frozen.source.source_type.value,
            source_id=frozen.source.source_id,
            loop_id=frozen.source.loop_id,
            package_version=frozen.package_version,
        )
        if source_existing:
            if not self._has_package_asset_rows(source_existing.package_id, assets):
                raise InvalidStateTransitionError(
                    "strategy package source version exists without required frozen asset rows",
                    context={
                        "reason_code": "strategy_package_source_existing_assets_incomplete",
                        "package_id": source_existing.package_id,
                        "required_assets": _asset_key_payload(assets),
                    },
                )
            return source_existing

        cm, managed_by_factory = self._transaction_conn()
        try:
            with cm as conn:
                original_autocommit = getattr(conn, "autocommit", None)
                if not managed_by_factory and original_autocommit is not None:
                    conn.autocommit = False
                try:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute(
                            """
                            SELECT package_id, manifest_sha256, paper_portfolio_count
                            FROM strategy_pkg.package
                            WHERE package_id = %s
                            """,
                            (frozen.package_id,),
                        )
                        existing = cur.fetchone()
                        if existing:
                            if existing["manifest_sha256"] != frozen.manifest_sha256:
                                raise InvalidStateTransitionError(
                                    "package manifest cannot be silently replaced",
                                    context={
                                        "package_id": frozen.package_id,
                                        "existing_manifest_sha256": existing["manifest_sha256"],
                                        "new_manifest_sha256": frozen.manifest_sha256,
                                        "paper_portfolio_count": existing["paper_portfolio_count"],
                                    },
                                )
                        else:
                            self._insert_manifest(cur, frozen)
                        for asset in assets:
                            self._upsert_package_asset(cur, asset)
                    if not managed_by_factory and hasattr(conn, "commit"):
                        conn.commit()
                except Exception:
                    if not managed_by_factory and hasattr(conn, "rollback"):
                        conn.rollback()
                    raise
                finally:
                    if not managed_by_factory and original_autocommit is not None:
                        conn.autocommit = original_autocommit
        except pg_errors.UniqueViolation as exc:
            existing_by_source = self.find_by_source_version(
                source_type=frozen.source.source_type.value,
                source_id=frozen.source.source_id,
                loop_id=frozen.source.loop_id,
                package_version=frozen.package_version,
            )
            if existing_by_source and self._has_package_asset_rows(existing_by_source.package_id, assets):
                return existing_by_source
            raise InvalidStateTransitionError(
                "strategy package unique constraint collision",
                context={
                    "package_id": frozen.package_id,
                    "source_type": frozen.source.source_type.value,
                    "source_id": frozen.source.source_id,
                    "loop_id": frozen.source.loop_id,
                    "package_version": frozen.package_version,
                    "required_assets": _asset_key_payload(assets),
                },
            ) from exc
        return self.get(frozen.package_id)

    def backfill_frozen_manifest_assets(
        self,
        package_id: str,
        *,
        frozen_manifest: StrategyPackageManifest,
        assets: list[StrategyPackageAssetRecord],
        operator: str,
        expected_old_manifest_sha256: str,
    ) -> StrategyPackageRecord:
        record = self.get(package_id)
        expected_old = str(expected_old_manifest_sha256 or "").strip().lower()
        if record.manifest_sha256 != expected_old:
            raise InvalidStateTransitionError(
                "strategy package asset backfill lost compare-and-set race",
                context={
                    "reason_code": "strategy_package_asset_backfill_cas_mismatch",
                    "package_id": package_id,
                    "expected_old_manifest_sha256": expected_old,
                    "actual_manifest_sha256": record.manifest_sha256,
                },
            )
        if frozen_manifest.package_id != package_id:
            raise StrategyPackageValidationError(
                "backfilled manifest package_id must match target package",
                context={
                    "reason_code": "strategy_package_asset_backfill_package_mismatch",
                    "package_id": package_id,
                    "manifest_package_id": frozen_manifest.package_id,
                },
            )
        frozen = freeze_manifest(
            frozen_manifest.model_copy(
                update={
                    "manifest_sha256": None,
                    "package_status": record.package_status,
                }
            )
        )
        _validate_asset_records_for_manifest(frozen, assets)
        cm, managed_by_factory = self._transaction_conn()
        try:
            with cm as conn:
                original_autocommit = getattr(conn, "autocommit", None)
                if not managed_by_factory and original_autocommit is not None:
                    conn.autocommit = False
                try:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute(
                            """
                            UPDATE strategy_pkg.package
                            SET manifest_json = %s,
                                manifest_sha256 = %s,
                                updated_at = NOW()
                            WHERE package_id = %s AND manifest_sha256 = %s
                            """,
                            (
                                psycopg2.extras.Json(frozen.model_dump(mode="json")),
                                frozen.manifest_sha256,
                                package_id,
                                expected_old,
                            ),
                        )
                        if cur.rowcount != 1:
                            raise InvalidStateTransitionError(
                                "strategy package asset backfill lost compare-and-set race",
                                context={
                                    "reason_code": "strategy_package_asset_backfill_cas_mismatch",
                                    "package_id": package_id,
                                    "expected_old_manifest_sha256": expected_old,
                                    "new_manifest_sha256": frozen.manifest_sha256,
                                },
                            )
                        for asset in assets:
                            self._upsert_package_asset(cur, asset)
                        cur.execute(
                            """
                            INSERT INTO strategy_pkg.package_status_event (
                                package_id, from_status, to_status, reason, context
                            ) VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                package_id,
                                record.package_status.value,
                                record.package_status.value,
                                "strategy_package_asset_backfill_freeze",
                                psycopg2.extras.Json({
                                    "operator": operator,
                                    "old_manifest_sha256": record.manifest_sha256,
                                    "new_manifest_sha256": frozen.manifest_sha256,
                                    "asset_count": len(assets),
                                    "asset_keys": _asset_key_payload(assets),
                                    "rollback_restore": {
                                        "field": "strategy_pkg.package.manifest_json, strategy_pkg.package.manifest_sha256",
                                        "manifest_sha256": record.manifest_sha256,
                                        "manifest_json": record.current_manifest().model_dump(mode="json"),
                                    },
                                }),
                            ),
                        )
                    if not managed_by_factory and hasattr(conn, "commit"):
                        conn.commit()
                except Exception:
                    if not managed_by_factory and hasattr(conn, "rollback"):
                        conn.rollback()
                    raise
                finally:
                    if not managed_by_factory and original_autocommit is not None:
                        conn.autocommit = original_autocommit
        except pg_errors.UniqueViolation as exc:
            raise InvalidStateTransitionError(
                "strategy package asset backfill audit event sequence is behind existing rows",
                context={
                    "reason_code": "strategy_package_asset_backfill_event_collision",
                    "package_id": package_id,
                    "operator": operator,
                },
            ) from exc
        return self.get(package_id)

    def _transaction_conn(self) -> tuple[Any, bool]:
        try:
            return self._conn_factory(autocommit=False, manage_transaction=True), True  # type: ignore[misc]
        except TypeError:
            return self._conn_factory(), False

    def _insert_manifest(self, cur: Any, frozen: StrategyPackageManifest) -> None:
        cur.execute(
            """
            INSERT INTO strategy_pkg.package (
                package_id, package_name, package_version, source_type,
                source_id, loop_id, run_id, package_status, manifest_json,
               manifest_sha256, alpha_mode, signal_domain, display_name, legacy_name,
               data_vintage, prediction_ref_uri, prediction_ref_sha256,
               model_artifact_uri, model_artifact_sha256
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                frozen.package_id,
                frozen.package_name,
                frozen.package_version,
                frozen.source.source_type.value,
                frozen.source.source_id,
                frozen.source.loop_id,
                frozen.source.run_id,
                frozen.package_status.value,
                psycopg2.extras.Json(frozen.model_dump(mode="json")),
                frozen.manifest_sha256,
                frozen.alpha_mode.value,
                None,
                frozen.package_name,
                frozen.package_name,
                _infer_data_vintage(frozen),
                None,
                None,
                None,
                None,
            ),
        )
        cur.execute(
            """
            INSERT INTO strategy_pkg.package_status_event (
                package_id, from_status, to_status, reason, context
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (
                frozen.package_id,
                None,
                frozen.package_status.value,
                "package_created",
                psycopg2.extras.Json({"manifest_sha256": frozen.manifest_sha256}),
            ),
        )

    def _upsert_package_asset(self, cur: Any, asset: StrategyPackageAssetRecord) -> StrategyPackageAssetRecord | None:
        cur.execute(
            """
            INSERT INTO strategy_pkg.package_asset (
                package_id, asset_type, asset_ref, asset_sha256, metadata,
                asset_role, asset_size_bytes, protected_asset, source_uri, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (package_id, asset_type, asset_ref) DO UPDATE
            SET asset_sha256 = EXCLUDED.asset_sha256,
                metadata = EXCLUDED.metadata,
                asset_role = EXCLUDED.asset_role,
                asset_size_bytes = EXCLUDED.asset_size_bytes,
                protected_asset = EXCLUDED.protected_asset,
                source_uri = EXCLUDED.source_uri
            WHERE strategy_pkg.package_asset.asset_sha256 IS NOT DISTINCT FROM EXCLUDED.asset_sha256
            RETURNING *
            """,
            (
                asset.package_id,
                asset.asset_type.value,
                asset.asset_ref,
                asset.asset_sha256,
                psycopg2.extras.Json(asset.metadata),
                asset.asset_role,
                asset.asset_size_bytes,
                asset.protected_asset,
                asset.source_uri,
                asset.created_at,
            ),
        )
        row = cur.fetchone()
        if not row:
            raise StrategyPackageValidationError(
                "existing package_asset row has a different sha256 for the same asset_ref",
                context={
                    "reason_code": "strategy_package_asset_sha_mismatch",
                    "package_id": asset.package_id,
                    "asset_type": asset.asset_type.value,
                    "asset_ref": asset.asset_ref,
                    "asset_sha256": asset.asset_sha256,
                },
            )
        return self._package_asset_from_row(dict(row))

    def _has_package_asset_rows(self, package_id: str, assets: list[StrategyPackageAssetRecord]) -> bool:
        if not assets:
            return True
        existing = {
            (asset.asset_type, asset.asset_ref, asset.asset_sha256)
            for asset in self.list_package_assets(package_id)
        }
        return all((asset.asset_type, asset.asset_ref, asset.asset_sha256) in existing for asset in assets)

    def find_by_source_version(
        self,
        *,
        source_type: str,
        source_id: str,
        loop_id: str | None,
        package_version: str,
    ) -> StrategyPackageRecord | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT package_id, package_name, package_version, source_type,
                           source_id, loop_id, run_id, package_status, manifest_json,
                           manifest_sha256, alpha_mode, signal_domain, display_name, legacy_name,
                           data_vintage, prediction_ref_uri, prediction_ref_sha256,
                           model_artifact_uri, model_artifact_sha256, paper_portfolio_count, created_at, updated_at
                    FROM strategy_pkg.package
                    WHERE source_type = %s
                      AND source_id = %s
                      AND COALESCE(loop_id, '') = COALESCE(%s, '')
                      AND package_version = %s
                    ORDER BY created_at DESC, package_id ASC
                    LIMIT 1
                    """,
                    (source_type, source_id, loop_id, package_version),
                )
                row = cur.fetchone()
        if not row:
            return None
        return self._record_from_row(dict(row))

    def get(self, package_id: str) -> StrategyPackageRecord:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT package_id, package_name, package_version, source_type,
                           source_id, loop_id, run_id, package_status, manifest_json,
                           manifest_sha256, alpha_mode, signal_domain, display_name, legacy_name,
                           data_vintage, prediction_ref_uri, prediction_ref_sha256,
                           model_artifact_uri, model_artifact_sha256, paper_portfolio_count, created_at, updated_at
                    FROM strategy_pkg.package
                    WHERE package_id = %s
                    """,
                    (package_id,),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "strategy package does not exist",
                context={"package_id": package_id},
            )
        return self._record_from_row(dict(row))

    def list(self, *, status: PackageStatus | None = None, limit: int = 100) -> list[StrategyPackageRecord]:
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        params: list[Any] = []
        where = ""
        if status is not None:
            where = "WHERE package_status = %s"
            params.append(status.value)
        params.append(limit)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT package_id, package_name, package_version, source_type,
                           source_id, loop_id, run_id, package_status, manifest_json,
                           manifest_sha256, alpha_mode, signal_domain, display_name, legacy_name,
                           data_vintage, prediction_ref_uri, prediction_ref_sha256,
                           model_artifact_uri, model_artifact_sha256, paper_portfolio_count, created_at, updated_at
                    FROM strategy_pkg.package
                    {where}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [record for record in (self._record_from_row(dict(row), quarantine=True) for row in rows) if record is not None]

    def list_single_alpha_candidates_for_seed_reuse(self, *, limit: int = 2000) -> list[StrategyPackageRecord]:
        """Return frozen single-alpha packages for deterministic seed-coverage matching."""

        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT package_id, package_name, package_version, source_type,
                           source_id, loop_id, run_id, package_status, manifest_json,
                           manifest_sha256, alpha_mode, signal_domain, display_name, legacy_name,
                           data_vintage, prediction_ref_uri, prediction_ref_sha256,
                           model_artifact_uri, model_artifact_sha256, paper_portfolio_count, created_at, updated_at
                    FROM strategy_pkg.package
                    WHERE alpha_mode = 'single_alpha'
                      AND package_status <> 'RETIRED'
                    ORDER BY created_at DESC, package_id ASC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        return [self._record_from_row(dict(row)) for row in rows]

    def package_delete_dependencies(self, package_id: str) -> dict[str, Any]:
        self.get(package_id)
        summary: dict[str, Any] = {
            "paper_portfolios": [],
            "active_paper_portfolios": [],
            "selection_run_ids": [],
            "qmt_bindings": [],
            "live_approvals": [],
            "strategy_runtime_releases": [],
            "simulation_release_bindings": [],
            "simulation_daily_runs": [],
            "execution_plans": [],
            "component_child_edges": [],
            "component_parent_edges": [],
            "daily_selection_evidence": 0,
            "selection_score_artifacts": 0,
        }
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT portfolio_id, status
                    FROM paper_v2.portfolio
                    WHERE package_id = %s
                    ORDER BY created_at DESC, portfolio_id ASC
                    """,
                    (package_id,),
                )
                portfolios = [dict(row) for row in cur.fetchall()]
                summary["paper_portfolios"] = portfolios
                summary["active_paper_portfolios"] = [
                    row for row in portfolios if str(row.get("status") or "").upper() != "RETIRED"
                ]
                cur.execute(
                    """
                    SELECT DISTINCT run_id
                    FROM (
                        SELECT run_id FROM selection.package_result WHERE package_id = %s
                        UNION
                        SELECT run_id FROM selection.excluded_result WHERE package_id = %s
                        UNION
                        SELECT run_id FROM selection.paper_portfolio_link WHERE package_id = %s
                        UNION
                        SELECT run_id FROM selection.run WHERE package_ids ? %s
                    ) refs
                    ORDER BY run_id
                    """,
                    (package_id, package_id, package_id, package_id),
                )
                summary["selection_run_ids"] = [row["run_id"] for row in cur.fetchall()]
                cur.execute("SELECT COUNT(*) AS cnt FROM selection.daily_selection_evidence WHERE package_id = %s", (package_id,))
                summary["daily_selection_evidence"] = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute("SELECT COUNT(*) AS cnt FROM strategy_pkg.selection_score_artifact WHERE package_id = %s", (package_id,))
                summary["selection_score_artifacts"] = int((cur.fetchone() or {}).get("cnt") or 0)
                cur.execute(
                    """
                    SELECT approval_id, approval_status, portfolio_id
                    FROM strategy_pkg.live_approval
                    WHERE package_id = %s
                    ORDER BY updated_at DESC, approval_id ASC
                    """,
                    (package_id,),
                )
                summary["live_approvals"] = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    """
                    SELECT release_id, validation_state
                    FROM strategy_pkg.strategy_runtime_release
                    WHERE package_id = %s
                    ORDER BY created_at DESC, release_id ASC
                    """,
                    (package_id,),
                )
                summary["strategy_runtime_releases"] = [dict(row) for row in cur.fetchall()]
                if self._table_exists(cur, "qmt_strategy.strategy_package_binding"):
                    cur.execute(
                        """
                        SELECT binding_id, strategy_id, binding_status
                        FROM qmt_strategy.strategy_package_binding
                        WHERE package_id = %s
                        ORDER BY updated_at DESC, binding_id ASC
                        """,
                        (package_id,),
                    )
                    summary["qmt_bindings"] = [dict(row) for row in cur.fetchall()]
                for table, key in (
                    ("paper_v2.simulation_release_binding", "simulation_release_bindings"),
                    ("paper_v2.simulation_daily_run", "simulation_daily_runs"),
                    ("paper_v2.execution_plan", "execution_plans"),
                ):
                    if self._table_exists(cur, table):
                        cur.execute(
                            f"""
                            SELECT *
                            FROM {table}
                            WHERE package_id = %s
                            LIMIT 50
                            """,
                            (package_id,),
                        )
                        summary[key] = [dict(row) for row in cur.fetchall()]
                if self._table_exists(cur, "strategy_pkg.strategy_package_components"):
                    cur.execute(
                        """
                        SELECT parent_package_id, child_package_id, position
                        FROM strategy_pkg.strategy_package_components
                        WHERE child_package_id = %s
                        ORDER BY parent_package_id ASC, position ASC
                        """,
                        (package_id,),
                    )
                    summary["component_child_edges"] = [dict(row) for row in cur.fetchall()]
                    cur.execute(
                        """
                        SELECT parent_package_id, child_package_id, position
                        FROM strategy_pkg.strategy_package_components
                        WHERE parent_package_id = %s
                        ORDER BY position ASC, child_package_id ASC
                        """,
                        (package_id,),
                    )
                    summary["component_parent_edges"] = [dict(row) for row in cur.fetchall()]
        return summary

    def delete_package(self, package_id: str) -> dict[str, Any]:
        record = self.get(package_id)
        dependencies = self.package_delete_dependencies(package_id)
        blockers = self._package_delete_blockers(dependencies)
        if blockers:
            raise InvalidStateTransitionError(
                "strategy package delete blocked by existing runtime references",
                context={"package_id": package_id, "blockers": blockers, "dependencies": dependencies},
            )
        counts = {
            "seed_fragility_score": 0,
            "package_validation_run": 0,
            "model_retrain_job": 0,
            "model_state": 0,
            "package_runtime_variant": 0,
            "validated_execution_policy": 0,
            "selection_score_artifact": 0,
            "package_asset": 0,
            "strategy_package_components": 0,
            "package_status_event": 0,
            "package": 0,
        }
        with self._conn_factory() as conn:
            original_autocommit = getattr(conn, "autocommit", None)
            try:
                if original_autocommit is not None:
                    conn.autocommit = False
                with conn.cursor() as cur:
                    optional_tables = (
                        ("strategy_pkg.seed_fragility_score", "seed_fragility_score"),
                        ("strategy_pkg.package_validation_run", "package_validation_run"),
                    )
                    for table, key in optional_tables:
                        if self._table_exists(cur, table):
                            cur.execute(f"DELETE FROM {table} WHERE package_id = %s", (package_id,))
                            counts[key] = cur.rowcount
                    for table, key in (
                        ("strategy_pkg.model_retrain_job", "model_retrain_job"),
                        ("strategy_pkg.model_state", "model_state"),
                        ("strategy_pkg.package_runtime_variant", "package_runtime_variant"),
                        ("strategy_pkg.validated_execution_policy", "validated_execution_policy"),
                        ("strategy_pkg.selection_score_artifact", "selection_score_artifact"),
                        ("strategy_pkg.package_asset", "package_asset"),
                        ("strategy_pkg.package_status_event", "package_status_event"),
                    ):
                        if self._table_exists(cur, table):
                            cur.execute(f"DELETE FROM {table} WHERE package_id = %s", (package_id,))
                            counts[key] = cur.rowcount
                    if self._table_exists(cur, "strategy_pkg.strategy_package_components"):
                        cur.execute(
                            "DELETE FROM strategy_pkg.strategy_package_components WHERE parent_package_id = %s",
                            (package_id,),
                        )
                        counts["strategy_package_components"] = cur.rowcount
                    cur.execute("DELETE FROM strategy_pkg.package WHERE package_id = %s", (package_id,))
                    counts["package"] = cur.rowcount
                if hasattr(conn, "commit"):
                    conn.commit()
            except Exception:
                if hasattr(conn, "rollback"):
                    conn.rollback()
                raise
            finally:
                if original_autocommit is not None:
                    conn.autocommit = original_autocommit
        return {
            "package_id": package_id,
            "manifest_sha256": record.manifest_sha256,
            "deleted_counts": counts,
            "dependencies": dependencies,
        }

    def transition_status(
        self,
        *,
        package_id: str,
        to_status: PackageStatus,
        allowed_from: set[PackageStatus],
        reason: str,
        context: dict[str, Any] | None = None,
    ) -> StrategyPackageRecord:
        record = self.get(package_id)
        if record.package_status not in allowed_from:
            raise InvalidStateTransitionError(
                "invalid strategy package status transition",
                context={
                    "package_id": package_id,
                    "from_status": record.package_status.value,
                    "to_status": to_status.value,
                    "allowed_from": sorted(item.value for item in allowed_from),
                },
            )
        with self._conn_factory() as conn:
            original_autocommit = getattr(conn, "autocommit", None)
            try:
                # Keep package status and its audit event atomic even though
                # pooled AIstock connections default to autocommit=True.
                if original_autocommit is not None:
                    conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE strategy_pkg.package
                        SET package_status = %s, updated_at = NOW()
                        WHERE package_id = %s AND package_status = %s
                        """,
                        (to_status.value, package_id, record.package_status.value),
                    )
                    if cur.rowcount != 1:
                        raise InvalidStateTransitionError(
                            "strategy package status transition lost compare-and-set race",
                            context={"package_id": package_id},
                        )
                    cur.execute(
                        """
                        INSERT INTO strategy_pkg.package_status_event (
                            package_id, from_status, to_status, reason, context
                        ) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            package_id,
                            record.package_status.value,
                            to_status.value,
                            reason,
                            psycopg2.extras.Json(context or {}),
                        ),
                    )
                if hasattr(conn, "commit"):
                    conn.commit()
            except pg_errors.UniqueViolation as exc:
                if hasattr(conn, "rollback"):
                    conn.rollback()
                raise InvalidStateTransitionError(
                    "strategy package status event sequence is behind existing rows",
                    context={
                        "package_id": package_id,
                        "from_status": record.package_status.value,
                        "to_status": to_status.value,
                        "reason": reason,
                    },
                ) from exc
            except Exception:
                if hasattr(conn, "rollback"):
                    conn.rollback()
                raise
            finally:
                if original_autocommit is not None:
                    conn.autocommit = original_autocommit
        return self.get(package_id)

    @staticmethod
    def _package_delete_blockers(dependencies: dict[str, Any]) -> list[str]:
        blockers: list[str] = []
        for key in (
            "paper_portfolios",
            "selection_run_ids",
            "qmt_bindings",
            "live_approvals",
            "strategy_runtime_releases",
            "simulation_release_bindings",
            "simulation_daily_runs",
            "execution_plans",
            "component_child_edges",
        ):
            if dependencies.get(key):
                blockers.append(key)
        for key in ("daily_selection_evidence",):
            if int(dependencies.get(key) or 0) > 0:
                blockers.append(key)
        return blockers

    @staticmethod
    def _table_exists(cur: Any, qualified_name: str) -> bool:
        cur.execute("SELECT to_regclass(%s)", (qualified_name,))
        row = cur.fetchone()
        if isinstance(row, dict):
            return bool(row.get("to_regclass"))
        return bool(row and row[0])

    def mark_paper_portfolio_created(self, package_id: str, portfolio_id: str) -> StrategyPackageRecord:
        record = self.get(package_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE strategy_pkg.package
                    SET paper_portfolio_count = paper_portfolio_count + 1,
                        updated_at = NOW()
                    WHERE package_id = %s
                    """,
                    (package_id,),
                )
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.package_status_event (
                        package_id, from_status, to_status, reason, context
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        package_id,
                        record.package_status.value,
                        record.package_status.value,
                        "paper_portfolio_created",
                        psycopg2.extras.Json({"portfolio_id": portfolio_id}),
                    ),
                )
        return self.get(package_id)

    def list_status_events(self, package_id: str, *, limit: int = 200) -> list[PackageStatusEvent]:
        self.get(package_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT package_id, from_status, to_status, reason, context, created_at
                    FROM strategy_pkg.package_status_event
                    WHERE package_id = %s
                    ORDER BY created_at ASC, event_id ASC
                    LIMIT %s
                    """,
                    (package_id, limit),
                )
                rows = cur.fetchall()
        return [
            PackageStatusEvent(
                package_id=row["package_id"],
                from_status=PackageStatus(row["from_status"]) if row["from_status"] else None,
                to_status=PackageStatus(row["to_status"]),
                reason=row["reason"],
                context=row["context"] or {},
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def update_artifact_refs(
        self,
        package_id: str,
        *,
        prediction_ref_uri: str | None = None,
        prediction_ref_sha256: str | None = None,
        model_artifact_uri: str | None = None,
        model_artifact_sha256: str | None = None,
    ) -> StrategyPackageRecord:
        self.get(package_id)
        if prediction_ref_uri is not None and not prediction_ref_sha256:
            raise StrategyPackageValidationError(
                "prediction_ref_sha256 is required when prediction_ref_uri is set",
                context={"package_id": package_id, "prediction_ref_uri": prediction_ref_uri},
            )
        if model_artifact_uri is not None and not model_artifact_sha256:
            raise StrategyPackageValidationError(
                "model_artifact_sha256 is required when model_artifact_uri is set",
                context={"package_id": package_id, "model_artifact_uri": model_artifact_uri},
            )
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE strategy_pkg.package
                    SET prediction_ref_uri = COALESCE(%s, prediction_ref_uri),
                        prediction_ref_sha256 = COALESCE(%s, prediction_ref_sha256),
                        model_artifact_uri = COALESCE(%s, model_artifact_uri),
                        model_artifact_sha256 = COALESCE(%s, model_artifact_sha256),
                        updated_at = NOW()
                    WHERE package_id = %s
                    """,
                    (
                        prediction_ref_uri,
                        prediction_ref_sha256,
                        model_artifact_uri,
                        model_artifact_sha256,
                        package_id,
                    ),
                )
        return self.get(package_id)

    def save_components(
        self,
        parent_package_id: str,
        components: list[StrategyPackageComponentRecord],
    ) -> list[StrategyPackageComponentRecord]:
        self.get(parent_package_id)
        if not components:
            raise StrategyPackageValidationError(
                "multi-alpha package requires at least one component edge",
                context={"parent_package_id": parent_package_id},
            )
        with self._conn_factory() as conn:
            original_autocommit = getattr(conn, "autocommit", None)
            try:
                if original_autocommit is not None:
                    conn.autocommit = False
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        "DELETE FROM strategy_pkg.strategy_package_components WHERE parent_package_id = %s",
                        (parent_package_id,),
                    )
                    saved: list[StrategyPackageComponentRecord] = []
                    for component in components:
                        cur.execute(
                            """
                            INSERT INTO strategy_pkg.strategy_package_components (
                                parent_package_id, child_package_id, child_manifest_sha256,
                                component_weight, score_normalization, position
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            RETURNING id, parent_package_id, child_package_id, child_manifest_sha256,
                                      component_weight, score_normalization, position, created_at
                            """,
                            (
                                parent_package_id,
                                component.child_package_id,
                                component.child_manifest_sha256,
                                component.component_weight,
                                component.score_normalization,
                                component.position,
                            ),
                        )
                        saved.append(self._component_from_row(dict(cur.fetchone())))
                if hasattr(conn, "commit"):
                    conn.commit()
            except Exception:
                if hasattr(conn, "rollback"):
                    conn.rollback()
                raise
            finally:
                if original_autocommit is not None:
                    conn.autocommit = original_autocommit
        return saved

    def list_components(self, parent_package_id: str) -> list[StrategyPackageComponentRecord]:
        self.get(parent_package_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, parent_package_id, child_package_id, child_manifest_sha256,
                           component_weight, score_normalization, position, created_at
                    FROM strategy_pkg.strategy_package_components
                    WHERE parent_package_id = %s
                    ORDER BY position ASC, id ASC
                    """,
                    (parent_package_id,),
                )
                rows = cur.fetchall()
        return [self._component_from_row(dict(row)) for row in rows]

    def list_component_parents(self, child_package_id: str) -> list[StrategyPackageComponentRecord]:
        self.get(child_package_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, parent_package_id, child_package_id, child_manifest_sha256,
                           component_weight, score_normalization, position, created_at
                    FROM strategy_pkg.strategy_package_components
                    WHERE child_package_id = %s
                    ORDER BY created_at DESC, id ASC
                    """,
                    (child_package_id,),
                )
                rows = cur.fetchall()
        return [self._component_from_row(dict(row)) for row in rows]

    def save_package_asset(self, asset: StrategyPackageAssetRecord) -> StrategyPackageAssetRecord:
        self.get(asset.package_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.package_asset (
                        package_id, asset_type, asset_ref, asset_sha256, metadata,
                        asset_role, asset_size_bytes, protected_asset, source_uri, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (package_id, asset_type, asset_ref) DO UPDATE
                    SET asset_sha256 = EXCLUDED.asset_sha256,
                        metadata = EXCLUDED.metadata,
                        asset_role = EXCLUDED.asset_role,
                        asset_size_bytes = EXCLUDED.asset_size_bytes,
                        protected_asset = EXCLUDED.protected_asset,
                        source_uri = EXCLUDED.source_uri
                    RETURNING *
                    """,
                    (
                        asset.package_id,
                        asset.asset_type.value,
                        asset.asset_ref,
                        asset.asset_sha256,
                        psycopg2.extras.Json(asset.metadata),
                        asset.asset_role,
                        asset.asset_size_bytes,
                        asset.protected_asset,
                        asset.source_uri,
                        asset.created_at,
                    ),
                )
                row = cur.fetchone()
        return self._package_asset_from_row(dict(row))

    def list_package_assets(self, package_id: str, *, protected_only: bool = False) -> list[StrategyPackageAssetRecord]:
        self.get(package_id)
        where = ["package_id = %s"]
        params: list[Any] = [package_id]
        if protected_only:
            where.append("protected_asset = TRUE")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM strategy_pkg.package_asset
                    WHERE {' AND '.join(where)}
                    ORDER BY created_at DESC, asset_id DESC
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [self._package_asset_from_row(dict(row)) for row in rows]

    def save_execution_policy(self, policy: ValidatedExecutionPolicy) -> ValidatedExecutionPolicy:
        self.get(policy.package_id)
        policy = policy.model_copy(update={"paper_enabled": False})
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.validated_execution_policy (
                        policy_id, package_id, manifest_sha256, policy_name, policy_json,
                        policy_sha256, algo_code, algo_config, unfilled_handler,
                        unfilled_handler_params, source_backtest_id, source_backtest_status,
                        validation_status, paper_enabled, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (package_id, policy_sha256) DO NOTHING
                    """,
                    (
                        policy.policy_id,
                        policy.package_id,
                        policy.manifest_sha256,
                        policy.policy_name,
                        psycopg2.extras.Json(policy.policy_json),
                        policy.policy_sha256,
                        policy.algo_code,
                        psycopg2.extras.Json(policy.algo_config),
                        policy.unfilled_handler,
                        psycopg2.extras.Json(policy.unfilled_handler_params),
                        policy.source_backtest_id,
                        policy.source_backtest_status,
                        policy.validation_status.value,
                        policy.paper_enabled,
                        policy.created_at,
                        policy.updated_at,
                    ),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        """
                        SELECT policy_id
                        FROM strategy_pkg.validated_execution_policy
                        WHERE package_id = %s AND policy_sha256 = %s
                        """,
                        (policy.package_id, policy.policy_sha256),
                    )
                    row = cur.fetchone()
                    if row:
                        policy = policy.model_copy(update={"policy_id": row[0]})
        return self.get_execution_policy(policy.package_id, policy.policy_id)

    def get_execution_policy(self, package_id: str, policy_id: str) -> ValidatedExecutionPolicy:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.validated_execution_policy
                    WHERE package_id = %s AND policy_id = %s
                    """,
                    (package_id, policy_id),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "validated execution policy does not exist",
                context={"package_id": package_id, "policy_id": policy_id},
            )
        return self._execution_policy_from_row(dict(row))

    def list_execution_policies(self, package_id: str) -> list[ValidatedExecutionPolicy]:
        self.get(package_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.validated_execution_policy
                    WHERE package_id = %s
                    ORDER BY created_at DESC, policy_id DESC
                    """,
                    (package_id,),
                )
                rows = cur.fetchall()
        return [self._execution_policy_from_row(dict(row)) for row in rows]

    def set_execution_policy_paper_enabled(
        self,
        *,
        package_id: str,
        policy_id: str,
        paper_enabled: bool,
    ) -> ValidatedExecutionPolicy:
        _ = paper_enabled
        self.get_execution_policy(package_id, policy_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE strategy_pkg.validated_execution_policy
                    SET paper_enabled = %s, updated_at = NOW()
                    WHERE package_id = %s AND policy_id = %s
                    """,
                    (False, package_id, policy_id),
                )
        return self.get_execution_policy(package_id, policy_id)

    def save_live_approval(self, approval: StrategyPackageLiveApproval) -> StrategyPackageLiveApproval:
        self.get(approval.package_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.live_approval (
                        approval_id, package_id, manifest_sha256, alpha_core_sha256,
                        portfolio_id, runtime_release_id, runtime_release_sha256,
                        runtime_profile_id, runtime_profile_version_id, runtime_profile_sha256,
                        execution_policy_id, execution_policy_sha256, tail_policy_id,
                        tail_policy_sha256, target_broker_backend, broker_account_id,
                        approval_status, sim_validation_evidence, broker_compatibility,
                        risk_note, rollback_plan, requested_by, requested_at, approved_by,
                        approved_at, rejected_by, rejected_at, rejection_reason, retired_by,
                        retired_at, retirement_reason, audit_json, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s
                    )
                    """,
                    (
                        approval.approval_id,
                        approval.package_id,
                        approval.manifest_sha256,
                        approval.alpha_core_sha256,
                        approval.portfolio_id,
                        approval.runtime_release_id,
                        approval.runtime_release_sha256,
                        approval.runtime_profile_id,
                        approval.runtime_profile_version_id,
                        approval.runtime_profile_sha256,
                        approval.execution_policy_id,
                        approval.execution_policy_sha256,
                        approval.tail_policy_id,
                        approval.tail_policy_sha256,
                        approval.target_broker_backend,
                        approval.broker_account_id,
                        approval.approval_status.value,
                        psycopg2.extras.Json(approval.sim_validation_evidence),
                        psycopg2.extras.Json(approval.broker_compatibility),
                        approval.risk_note,
                        approval.rollback_plan,
                        approval.requested_by,
                        approval.requested_at,
                        approval.approved_by,
                        approval.approved_at,
                        approval.rejected_by,
                        approval.rejected_at,
                        approval.rejection_reason,
                        approval.retired_by,
                        approval.retired_at,
                        approval.retirement_reason,
                        psycopg2.extras.Json(approval.audit_json),
                        approval.created_at,
                        approval.updated_at,
                    ),
                )
        return self.get_live_approval(approval.package_id, approval.approval_id)

    def get_live_approval(self, package_id: str, approval_id: str) -> StrategyPackageLiveApproval:
        self.get(package_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.live_approval
                    WHERE package_id = %s AND approval_id = %s
                    """,
                    (package_id, approval_id),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "strategy package live approval does not exist",
                context={"package_id": package_id, "approval_id": approval_id},
            )
        return self._live_approval_from_row(dict(row))

    def list_live_approvals(
        self,
        *,
        package_id: str | None = None,
        portfolio_id: str | None = None,
        status: LiveApprovalStatus | None = None,
        limit: int = 100,
    ) -> list[StrategyPackageLiveApproval]:
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        where: list[str] = []
        params: list[Any] = []
        if package_id is not None:
            self.get(package_id)
            where.append("package_id = %s")
            params.append(package_id)
        if portfolio_id is not None:
            where.append("portfolio_id = %s")
            params.append(portfolio_id)
        if status is not None:
            where.append("approval_status = %s")
            params.append(status.value)
        sql = "SELECT * FROM strategy_pkg.live_approval"
        if where:
            sql += f" WHERE {' AND '.join(where)}"
        sql += " ORDER BY updated_at DESC, created_at DESC, approval_id DESC LIMIT %s"
        params.append(limit)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
        return [self._live_approval_from_row(dict(row)) for row in rows]

    def update_live_approval(self, approval: StrategyPackageLiveApproval) -> StrategyPackageLiveApproval:
        self.get_live_approval(approval.package_id, approval.approval_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE strategy_pkg.live_approval
                    SET approval_status = %s,
                        risk_note = %s,
                        rollback_plan = %s,
                        requested_by = %s,
                        requested_at = %s,
                        approved_by = %s,
                        approved_at = %s,
                        rejected_by = %s,
                        rejected_at = %s,
                        rejection_reason = %s,
                        retired_by = %s,
                        retired_at = %s,
                        retirement_reason = %s,
                        audit_json = %s,
                        updated_at = %s
                    WHERE package_id = %s AND approval_id = %s
                    """,
                    (
                        approval.approval_status.value,
                        approval.risk_note,
                        approval.rollback_plan,
                        approval.requested_by,
                        approval.requested_at,
                        approval.approved_by,
                        approval.approved_at,
                        approval.rejected_by,
                        approval.rejected_at,
                        approval.rejection_reason,
                        approval.retired_by,
                        approval.retired_at,
                        approval.retirement_reason,
                        psycopg2.extras.Json(approval.audit_json),
                        approval.updated_at,
                        approval.package_id,
                        approval.approval_id,
                    ),
                )
        return self.get_live_approval(approval.package_id, approval.approval_id)

    def get_model_state(self, package_id: str) -> StrategyPackageModelState | None:
        self.get(package_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM strategy_pkg.model_state WHERE package_id = %s", (package_id,))
                row = cur.fetchone()
        return self._model_state_from_row(dict(row)) if row else None

    def upsert_model_state(self, state: StrategyPackageModelState) -> StrategyPackageModelState:
        self.get(state.package_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.model_state (
                        package_id, active_model_version_id, train_start_date, train_end_date,
                        trained_at, last_retrain_job_id, last_retrained_at, stale_after_days,
                        staleness_status, warning, last_checked_at, metadata, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (package_id) DO UPDATE SET
                        active_model_version_id = EXCLUDED.active_model_version_id,
                        train_start_date = EXCLUDED.train_start_date,
                        train_end_date = EXCLUDED.train_end_date,
                        trained_at = EXCLUDED.trained_at,
                        last_retrain_job_id = EXCLUDED.last_retrain_job_id,
                        last_retrained_at = EXCLUDED.last_retrained_at,
                        stale_after_days = EXCLUDED.stale_after_days,
                        staleness_status = EXCLUDED.staleness_status,
                        warning = EXCLUDED.warning,
                        last_checked_at = EXCLUDED.last_checked_at,
                        metadata = EXCLUDED.metadata,
                        updated_at = NOW()
                    """,
                    (
                        state.package_id,
                        state.active_model_version_id,
                        state.train_start_date,
                        state.train_end_date,
                        state.trained_at,
                        state.last_retrain_job_id,
                        state.last_retrained_at,
                        state.stale_after_days,
                        state.staleness_status.value,
                        state.warning,
                        state.last_checked_at,
                        psycopg2.extras.Json(state.metadata),
                    ),
                )
        current = self.get_model_state(state.package_id)
        if current is None:
            raise StrategyPackageValidationError("failed to upsert strategy package model state")
        return current

    def save_model_retrain_job(self, job: StrategyPackageModelRetrainJob) -> StrategyPackageModelRetrainJob:
        self.get(job.package_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.model_retrain_job (
                        job_id, package_id, job_type, requested_train_start_date,
                        requested_train_end_date, stale_after_days, config, status,
                        requires_manual_confirmation, confirmed, status_reason, error_json,
                        created_at, updated_at, started_at, completed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        job.job_id,
                        job.package_id,
                        job.job_type,
                        job.requested_train_start_date,
                        job.requested_train_end_date,
                        job.stale_after_days,
                        psycopg2.extras.Json(job.config),
                        job.status.value,
                        job.requires_manual_confirmation,
                        job.confirmed,
                        job.status_reason,
                        psycopg2.extras.Json(job.error) if job.error else None,
                        job.created_at,
                        job.updated_at,
                        job.started_at,
                        job.completed_at,
                    ),
                )
        return self.get_model_retrain_job(job.package_id, job.job_id)

    def get_model_retrain_job(self, package_id: str, job_id: str) -> StrategyPackageModelRetrainJob:
        self.get(package_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.model_retrain_job
                    WHERE package_id = %s AND job_id = %s
                    """,
                    (package_id, job_id),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "model retrain job does not exist",
                context={"package_id": package_id, "job_id": job_id},
            )
        return self._model_retrain_job_from_row(dict(row))

    def list_model_retrain_jobs(self, package_id: str, *, limit: int = 100) -> list[StrategyPackageModelRetrainJob]:
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        self.get(package_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.model_retrain_job
                    WHERE package_id = %s
                    ORDER BY created_at DESC, job_id DESC
                    LIMIT %s
                    """,
                    (package_id, limit),
                )
                rows = cur.fetchall()
        return [self._model_retrain_job_from_row(dict(row)) for row in rows]

    def save_runtime_variant(self, variant: StrategyPackageRuntimeVariant) -> StrategyPackageRuntimeVariant:
        record = self.get(variant.package_id)
        variant = variant.model_copy(update={"paper_candidate": False})
        _validate_variant_matches_package(variant, record)
        ensure_runtime_variant_status(
            validation_status=variant.validation_status,
            paper_candidate=variant.paper_candidate,
            validation_evidence=variant.validation_evidence,
        )
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.package_runtime_variant (
                        variant_id, package_id, manifest_sha256, locked_core_hash, variant_name,
                        variant_kind, variant_config, variant_hash, validation_status,
                        paper_candidate, validation_evidence, created_by, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (package_id, variant_hash) DO NOTHING
                    """,
                    (
                        variant.variant_id,
                        variant.package_id,
                        variant.manifest_sha256,
                        variant.locked_core_hash,
                        variant.variant_name,
                        variant.variant_kind.value,
                        psycopg2.extras.Json(variant.variant_config),
                        variant.variant_hash,
                        variant.validation_status.value,
                        variant.paper_candidate,
                        psycopg2.extras.Json(variant.validation_evidence),
                        variant.created_by,
                        variant.created_at,
                        variant.updated_at,
                    ),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        """
                        SELECT variant_id
                        FROM strategy_pkg.package_runtime_variant
                        WHERE package_id = %s AND variant_hash = %s
                        """,
                        (variant.package_id, variant.variant_hash),
                    )
                    row = cur.fetchone()
                    if row:
                        variant = variant.model_copy(update={"variant_id": row[0]})
        return self.get_runtime_variant(variant.package_id, variant.variant_id)

    def get_runtime_variant(self, package_id: str, variant_id: str) -> StrategyPackageRuntimeVariant:
        self.get(package_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.package_runtime_variant
                    WHERE package_id = %s AND variant_id = %s
                    """,
                    (package_id, variant_id),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "strategy package runtime variant does not exist",
                context={"package_id": package_id, "variant_id": variant_id},
            )
        return self._runtime_variant_from_row(dict(row))

    def list_runtime_variants(
        self,
        package_id: str,
        *,
        include_retired: bool = False,
        limit: int = 100,
    ) -> list[StrategyPackageRuntimeVariant]:
        self.get(package_id)
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        where = "package_id = %s"
        params: list[Any] = [package_id]
        if not include_retired:
            where += " AND validation_status <> 'RETIRED'"
        params.append(limit)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM strategy_pkg.package_runtime_variant
                    WHERE {where}
                    ORDER BY created_at DESC, variant_id DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [self._runtime_variant_from_row(dict(row)) for row in rows]

    def set_runtime_variant_validation(
        self,
        *,
        package_id: str,
        variant_id: str,
        validation_status: RuntimeVariantValidationStatus,
        paper_candidate: bool,
        validation_evidence: dict[str, Any],
    ) -> StrategyPackageRuntimeVariant:
        self.get_runtime_variant(package_id, variant_id)
        paper_candidate = False
        ensure_runtime_variant_status(
            validation_status=validation_status,
            paper_candidate=paper_candidate,
            validation_evidence=validation_evidence,
        )
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE strategy_pkg.package_runtime_variant
                    SET validation_status = %s,
                        paper_candidate = %s,
                        validation_evidence = %s,
                        updated_at = NOW()
                    WHERE package_id = %s AND variant_id = %s
                    """,
                    (
                        validation_status.value,
                        paper_candidate,
                        psycopg2.extras.Json(validation_evidence),
                        package_id,
                        variant_id,
                    ),
                )
        return self.get_runtime_variant(package_id, variant_id)

    def save_validation_run(self, run: StrategyPackageValidationRun) -> StrategyPackageValidationRun:
        record = self.get(run.package_id)
        _validate_validation_run_matches_package(run, record)
        if run.runtime_variant_id is not None:
            variant = self.get_runtime_variant(run.package_id, run.runtime_variant_id)
            _validate_validation_run_matches_variant(run, variant.variant_hash)
        ensure_package_validation_run(run)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.package_validation_run (
                        validation_run_id, package_id, manifest_sha256, runtime_variant_id,
                        runtime_variant_hash, validation_type, retrain_mode, model_version_id,
                        seed_policy, random_seed, source_data_version, target_data_version,
                        backtest_start, backtest_end, status, metrics_json, artifact_manifest_json,
                        evidence_json, reproducibility_level, created_by, created_at, completed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run.validation_run_id,
                        run.package_id,
                        run.manifest_sha256,
                        run.runtime_variant_id,
                        run.runtime_variant_hash,
                        run.validation_type.value,
                        run.retrain_mode.value,
                        run.model_version_id,
                        run.seed_policy,
                        run.random_seed,
                        run.source_data_version,
                        run.target_data_version,
                        run.backtest_start,
                        run.backtest_end,
                        run.status.value,
                        psycopg2.extras.Json(run.metrics_json),
                        psycopg2.extras.Json(run.artifact_manifest_json),
                        psycopg2.extras.Json(run.evidence_json),
                        run.reproducibility_level.value,
                        run.created_by,
                        run.created_at,
                        run.completed_at,
                    ),
                )
        return self.get_validation_run(run.package_id, run.validation_run_id)

    def get_validation_run(self, package_id: str, validation_run_id: str) -> StrategyPackageValidationRun:
        self.get(package_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.package_validation_run
                    WHERE package_id = %s AND validation_run_id = %s
                    """,
                    (package_id, validation_run_id),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "strategy package validation run does not exist",
                context={"package_id": package_id, "validation_run_id": validation_run_id},
            )
        return self._validation_run_from_row(dict(row))

    def list_validation_runs(
        self,
        package_id: str,
        *,
        validation_type: PackageValidationType | None = None,
        runtime_variant_id: str | None = None,
        limit: int | None = 100,
    ) -> list[StrategyPackageValidationRun]:
        self.get(package_id)
        if limit is not None and limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        where = ["package_id = %s"]
        params: list[Any] = [package_id]
        if validation_type is not None:
            where.append("validation_type = %s")
            params.append(validation_type.value)
        if runtime_variant_id is not None:
            where.append("runtime_variant_id = %s")
            params.append(runtime_variant_id)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                sql = f"""
                    SELECT *
                    FROM strategy_pkg.package_validation_run
                    WHERE {' AND '.join(where)}
                    ORDER BY created_at DESC, validation_run_id DESC
                """
                if limit is not None:
                    sql += " LIMIT %s"
                    params.append(limit)
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
        return [self._validation_run_from_row(dict(row)) for row in rows]

    def validate_manifest_integrity(self, *, limit: int = 500) -> dict[str, Any]:
        """Scan all packages and report manifest_sha256 drift without modifying data.

        Returns a diagnostic report with per-package drift details so operators
        can decide whether to repair or quarantine individual packages.
        """
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT package_id, package_name, package_version, source_type,
                           source_id, loop_id, run_id, package_status, manifest_json,
                           manifest_sha256, alpha_mode, signal_domain, display_name, legacy_name,
                           data_vintage, prediction_ref_uri, prediction_ref_sha256,
                           model_artifact_uri, model_artifact_sha256, paper_portfolio_count, created_at, updated_at
                    FROM strategy_pkg.package
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        drifted: list[dict[str, Any]] = []
        clean_count = 0
        for row in rows:
            row_dict = dict(row)
            manifest_json = row_dict["manifest_json"]
            try:
                manifest = StrategyPackageManifest.model_validate(manifest_json)
            except Exception:
                drifted.append({
                    "package_id": row_dict["package_id"],
                    "package_name": row_dict["package_name"],
                    "package_status": row_dict["package_status"],
                    "stored_sha256": row_dict["manifest_sha256"],
                    "computed_sha256": None,
                    "error": "manifest_json failed pydantic validation",
                    "repair_plan": _manifest_drift_repair_plan(
                        stored_sha256=row_dict["manifest_sha256"],
                        computed_sha256=None,
                        manifest_json=manifest_json if isinstance(manifest_json, dict) else None,
                    ),
                })
                continue
            stored = row_dict["manifest_sha256"]
            record = StrategyPackageRecord(
                package_id=row_dict["package_id"],
                package_name=row_dict["package_name"],
                package_version=row_dict["package_version"],
                source_type=row_dict["source_type"],
                source_id=row_dict["source_id"],
                loop_id=row_dict.get("loop_id"),
                run_id=row_dict.get("run_id"),
                package_status=PackageStatus(row_dict["package_status"]),
                manifest=manifest,
                manifest_sha256=stored,
                alpha_mode=AlphaMode(row_dict.get("alpha_mode") or manifest.alpha_mode.value),
                signal_domain=row_dict.get("signal_domain"),
                display_name=row_dict.get("display_name") or row_dict["package_name"],
                legacy_name=row_dict.get("legacy_name"),
                data_vintage=row_dict.get("data_vintage"),
                prediction_ref_uri=row_dict.get("prediction_ref_uri"),
                prediction_ref_sha256=row_dict.get("prediction_ref_sha256"),
                model_artifact_uri=row_dict.get("model_artifact_uri"),
                model_artifact_sha256=row_dict.get("model_artifact_sha256"),
                paper_portfolio_count=int(row_dict.get("paper_portfolio_count") or 0),
                created_at=row_dict["created_at"],
                updated_at=row_dict["updated_at"],
            )
            computed = compute_manifest_sha256(record.current_manifest())
            if stored != computed:
                drifted.append({
                    "package_id": record.package_id,
                    "package_name": record.package_name,
                    "package_status": record.package_status.value,
                    "stored_sha256": stored,
                    "computed_sha256": computed,
                    "impact": {
                        "paper_portfolio_count": record.paper_portfolio_count,
                        "blocks_detail_endpoint": True,
                        "excluded_from_package_list": True,
                    },
                    "repair_plan": _manifest_drift_repair_plan(
                        stored_sha256=stored,
                        computed_sha256=computed,
                        manifest_json=manifest_json if isinstance(manifest_json, dict) else None,
                    ),
                })
            else:
                clean_count += 1
        return {
            "total_scanned": len(rows),
            "clean_count": clean_count,
            "drifted_count": len(drifted),
            "drifted": drifted,
        }

    def repair_manifest_hash(
        self,
        package_id: str,
        *,
        operator: str = "repair_manifest_hash",
        confirm_stored_sha256: str | None = None,
        confirm_computed_sha256: str | None = None,
    ) -> StrategyPackageRecord:
        """Recompute and persist the correct manifest_sha256 with an audit event.

        Only repairs the hash column — the manifest JSON itself is not modified.
        The existing hash check in get() will fail first if the package has drift,
        so we bypass it by reading the raw row directly.
        """
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT package_id, package_name, package_version, source_type,
                           source_id, loop_id, run_id, package_status, manifest_json,
                           manifest_sha256, alpha_mode, signal_domain, display_name, legacy_name,
                           data_vintage, prediction_ref_uri, prediction_ref_sha256,
                           model_artifact_uri, model_artifact_sha256, paper_portfolio_count, created_at, updated_at
                    FROM strategy_pkg.package
                    WHERE package_id = %s
                    """,
                    (package_id,),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "strategy package does not exist",
                context={"package_id": package_id},
            )
        row_dict = dict(row)
        try:
            manifest = StrategyPackageManifest.model_validate(row_dict["manifest_json"])
        except Exception as exc:
            repair_plan = _manifest_drift_repair_plan(
                stored_sha256=row_dict["manifest_sha256"],
                computed_sha256=None,
                manifest_json=row_dict["manifest_json"] if isinstance(row_dict["manifest_json"], dict) else None,
            )
            raise InvalidStateTransitionError(
                "manifest hash repair blocked until manifest drift classification is safe",
                context={
                    "package_id": package_id,
                    "stored_sha256": row_dict["manifest_sha256"],
                    "computed_sha256": None,
                    "error": "manifest_json failed pydantic validation",
                    "validation_error": str(exc),
                    "repair_plan": repair_plan,
                },
            ) from exc
        record = StrategyPackageRecord(
            package_id=row_dict["package_id"],
            package_name=row_dict["package_name"],
            package_version=row_dict["package_version"],
            source_type=row_dict["source_type"],
            source_id=row_dict["source_id"],
            loop_id=row_dict.get("loop_id"),
            run_id=row_dict.get("run_id"),
            package_status=PackageStatus(row_dict["package_status"]),
            manifest=manifest,
            manifest_sha256=row_dict["manifest_sha256"],
            alpha_mode=AlphaMode(row_dict.get("alpha_mode") or manifest.alpha_mode.value),
            signal_domain=row_dict.get("signal_domain"),
            display_name=row_dict.get("display_name") or row_dict["package_name"],
            legacy_name=row_dict.get("legacy_name"),
            data_vintage=row_dict.get("data_vintage"),
            prediction_ref_uri=row_dict.get("prediction_ref_uri"),
            prediction_ref_sha256=row_dict.get("prediction_ref_sha256"),
            model_artifact_uri=row_dict.get("model_artifact_uri"),
            model_artifact_sha256=row_dict.get("model_artifact_sha256"),
            paper_portfolio_count=int(row_dict.get("paper_portfolio_count") or 0),
            created_at=row_dict["created_at"],
            updated_at=row_dict["updated_at"],
        )
        correct_hash = compute_manifest_sha256(record.current_manifest())
        if record.manifest_sha256 == correct_hash:
            return record
        repair_plan = _manifest_drift_repair_plan(
            stored_sha256=record.manifest_sha256,
            computed_sha256=correct_hash,
            manifest_json=row_dict["manifest_json"] if isinstance(row_dict["manifest_json"], dict) else None,
        )
        if repair_plan["classification"]["classification"] != SAFE_MANIFEST_REPAIR_CLASSIFICATION:
            raise InvalidStateTransitionError(
                "manifest hash repair blocked until manifest drift classification is safe",
                context={
                    "package_id": package_id,
                    "stored_sha256": record.manifest_sha256,
                    "computed_sha256": correct_hash,
                    "repair_plan": repair_plan,
                },
            )
        if confirm_stored_sha256 != record.manifest_sha256 or confirm_computed_sha256 != correct_hash:
            raise InvalidStateTransitionError(
                "manifest hash repair requires explicit stored/computed hash confirmation",
                context={
                    "package_id": package_id,
                    "stored_sha256": record.manifest_sha256,
                    "computed_sha256": correct_hash,
                    "confirm_stored_sha256": confirm_stored_sha256,
                    "confirm_computed_sha256": confirm_computed_sha256,
                    "repair_plan": repair_plan,
                },
            )
        with self._conn_factory() as conn:
            original_autocommit = getattr(conn, "autocommit", None)
            try:
                if original_autocommit is not None:
                    conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE strategy_pkg.package
                        SET manifest_sha256 = %s, updated_at = NOW()
                        WHERE package_id = %s AND manifest_sha256 = %s
                        """,
                        (correct_hash, package_id, record.manifest_sha256),
                    )
                    if cur.rowcount != 1:
                        raise InvalidStateTransitionError(
                            "manifest hash repair lost compare-and-set race",
                            context={
                                "package_id": package_id,
                                "expected_stored_sha256": record.manifest_sha256,
                                "computed_sha256": correct_hash,
                            },
                        )
                    cur.execute(
                        """
                        INSERT INTO strategy_pkg.package_status_event (
                            package_id, from_status, to_status, reason, context
                        ) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            package_id,
                            record.package_status.value,
                            record.package_status.value,
                            "manifest_hash_repaired",
                            psycopg2.extras.Json({
                                "operator": operator,
                                "old_manifest_sha256": record.manifest_sha256,
                                "new_manifest_sha256": correct_hash,
                                "repair_classification": repair_plan["classification"]["classification"],
                                "classification_reason": repair_plan["classification"]["reason"],
                                "rollback_restore": {
                                    "field": "strategy_pkg.package.manifest_sha256",
                                    "restore_value": record.manifest_sha256,
                                },
                            }),
                        ),
                    )
                if hasattr(conn, "commit"):
                    conn.commit()
            except Exception:
                if hasattr(conn, "rollback"):
                    conn.rollback()
                raise
            finally:
                if original_autocommit is not None:
                    conn.autocommit = original_autocommit
        return self.get(package_id)

    def _record_from_row(self, row: dict[str, Any], *, quarantine: bool = False) -> StrategyPackageRecord | None:
        manifest_json = row["manifest_json"]
        manifest = StrategyPackageManifest.model_validate(manifest_json)
        record = StrategyPackageRecord(
            package_id=row["package_id"],
            package_name=row["package_name"],
            package_version=row["package_version"],
            source_type=row["source_type"],
            source_id=row["source_id"],
            loop_id=row.get("loop_id"),
            run_id=row.get("run_id"),
            package_status=PackageStatus(row["package_status"]),
            manifest=manifest,
            manifest_sha256=row["manifest_sha256"],
            alpha_mode=AlphaMode(row.get("alpha_mode") or manifest.alpha_mode.value),
            signal_domain=row.get("signal_domain"),
            display_name=row.get("display_name") or row["package_name"],
            legacy_name=row.get("legacy_name"),
            data_vintage=row.get("data_vintage"),
            prediction_ref_uri=row.get("prediction_ref_uri"),
            prediction_ref_sha256=row.get("prediction_ref_sha256"),
            model_artifact_uri=row.get("model_artifact_uri"),
            model_artifact_sha256=row.get("model_artifact_sha256"),
            paper_portfolio_count=int(row.get("paper_portfolio_count") or 0),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
        computed = compute_manifest_sha256(record.current_manifest())
        if record.manifest_sha256 != computed:
            if quarantine:
                logger.warning(
                    "strategy package manifest_sha256 drift quarantined: package_id=%s stored=%s computed=%s",
                    record.package_id,
                    record.manifest_sha256,
                    computed,
                )
                return None
            raise StrategyPackageValidationError(
                "stored manifest_sha256 does not match stored manifest",
                context={
                    "package_id": record.package_id,
                    "stored_sha256": record.manifest_sha256,
                    "computed_sha256": computed,
                },
            )
        return record

    @staticmethod
    def _component_from_row(row: dict[str, Any]) -> StrategyPackageComponentRecord:
        return StrategyPackageComponentRecord(
            id=row.get("id"),
            parent_package_id=row["parent_package_id"],
            child_package_id=row["child_package_id"],
            child_manifest_sha256=row["child_manifest_sha256"],
            component_weight=float(row["component_weight"]),
            score_normalization=row["score_normalization"],
            position=int(row["position"]),
            created_at=row["created_at"],
        )

    @staticmethod
    def _execution_policy_from_row(row: dict[str, Any]) -> ValidatedExecutionPolicy:
        return ValidatedExecutionPolicy(
            policy_id=row["policy_id"],
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            policy_name=row["policy_name"],
            policy_json=row["policy_json"] or {},
            policy_sha256=row["policy_sha256"],
            algo_code=row["algo_code"],
            algo_config=row["algo_config"] or {},
            unfilled_handler=row["unfilled_handler"],
            unfilled_handler_params=row["unfilled_handler_params"] or {},
            source_backtest_id=row["source_backtest_id"],
            source_backtest_status=row["source_backtest_status"],
            validation_status=row["validation_status"],
            paper_enabled=bool(row["paper_enabled"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _live_approval_from_row(row: dict[str, Any]) -> StrategyPackageLiveApproval:
        return StrategyPackageLiveApproval(
            approval_id=row["approval_id"],
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            alpha_core_sha256=row["alpha_core_sha256"],
            portfolio_id=row["portfolio_id"],
            runtime_release_id=row["runtime_release_id"],
            runtime_release_sha256=row["runtime_release_sha256"],
            runtime_profile_id=row["runtime_profile_id"],
            runtime_profile_version_id=row["runtime_profile_version_id"],
            runtime_profile_sha256=row["runtime_profile_sha256"],
            execution_policy_id=row["execution_policy_id"],
            execution_policy_sha256=row["execution_policy_sha256"],
            tail_policy_id=row["tail_policy_id"],
            tail_policy_sha256=row["tail_policy_sha256"],
            target_broker_backend=row["target_broker_backend"],
            broker_account_id=row["broker_account_id"],
            approval_status=LiveApprovalStatus(row["approval_status"]),
            sim_validation_evidence=row["sim_validation_evidence"] or {},
            broker_compatibility=row["broker_compatibility"] or {},
            risk_note=row["risk_note"],
            rollback_plan=row["rollback_plan"],
            requested_by=row["requested_by"],
            requested_at=row["requested_at"],
            approved_by=row["approved_by"],
            approved_at=row["approved_at"],
            rejected_by=row["rejected_by"],
            rejected_at=row["rejected_at"],
            rejection_reason=row["rejection_reason"],
            retired_by=row["retired_by"],
            retired_at=row["retired_at"],
            retirement_reason=row["retirement_reason"],
            audit_json=row["audit_json"] or {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _package_asset_from_row(row: dict[str, Any]) -> StrategyPackageAssetRecord:
        return StrategyPackageAssetRecord(
            asset_id=row["asset_id"],
            package_id=row["package_id"],
            asset_type=StrategyPackageAssetType(row["asset_type"]),
            asset_ref=row["asset_ref"],
            asset_sha256=row["asset_sha256"],
            metadata=row["metadata"] or {},
            asset_role=row.get("asset_role") or "governed_asset",
            asset_size_bytes=row.get("asset_size_bytes"),
            protected_asset=bool(row.get("protected_asset", True)),
            source_uri=row.get("source_uri"),
            created_at=row["created_at"],
        )

    @staticmethod
    def _model_state_from_row(row: dict[str, Any]) -> StrategyPackageModelState:
        return StrategyPackageModelState(
            package_id=row["package_id"],
            active_model_version_id=row["active_model_version_id"],
            train_start_date=row["train_start_date"],
            train_end_date=row["train_end_date"],
            trained_at=row["trained_at"],
            last_retrain_job_id=row["last_retrain_job_id"],
            last_retrained_at=row["last_retrained_at"],
            stale_after_days=int(row["stale_after_days"]),
            staleness_status=ModelStalenessStatus(row["staleness_status"]),
            warning=row["warning"],
            last_checked_at=row["last_checked_at"],
            metadata=row["metadata"] or {},
        )

    @staticmethod
    def _model_retrain_job_from_row(row: dict[str, Any]) -> StrategyPackageModelRetrainJob:
        return StrategyPackageModelRetrainJob(
            job_id=row["job_id"],
            package_id=row["package_id"],
            job_type=row["job_type"],
            requested_train_start_date=row["requested_train_start_date"],
            requested_train_end_date=row["requested_train_end_date"],
            stale_after_days=int(row["stale_after_days"]),
            config=row["config"] or {},
            status=ModelRetrainJobStatus(row["status"]),
            requires_manual_confirmation=bool(row["requires_manual_confirmation"]),
            confirmed=bool(row["confirmed"]),
            status_reason=row["status_reason"],
            error=row["error_json"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
        )

    @staticmethod
    def _runtime_variant_from_row(row: dict[str, Any]) -> StrategyPackageRuntimeVariant:
        return StrategyPackageRuntimeVariant(
            variant_id=row["variant_id"],
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            locked_core_hash=row["locked_core_hash"],
            variant_name=row["variant_name"],
            variant_kind=RuntimeVariantKind(row["variant_kind"]),
            variant_config=row["variant_config"] or {},
            variant_hash=row["variant_hash"],
            validation_status=RuntimeVariantValidationStatus(row["validation_status"]),
            paper_candidate=bool(row["paper_candidate"]),
            validation_evidence=row["validation_evidence"] or {},
            created_by=row["created_by"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _validation_run_from_row(row: dict[str, Any]) -> StrategyPackageValidationRun:
        return StrategyPackageValidationRun(
            validation_run_id=row["validation_run_id"],
            package_id=row["package_id"],
            manifest_sha256=row["manifest_sha256"],
            runtime_variant_id=row["runtime_variant_id"],
            runtime_variant_hash=row["runtime_variant_hash"],
            validation_type=PackageValidationType(row["validation_type"]),
            retrain_mode=PackageValidationRetrainMode(row["retrain_mode"]),
            model_version_id=row["model_version_id"],
            seed_policy=row["seed_policy"],
            random_seed=row["random_seed"],
            source_data_version=row["source_data_version"],
            target_data_version=row["target_data_version"],
            backtest_start=row["backtest_start"],
            backtest_end=row["backtest_end"],
            status=PackageValidationStatus(row["status"]),
            metrics_json=row["metrics_json"] or {},
            artifact_manifest_json=row["artifact_manifest_json"] or {},
            evidence_json=row["evidence_json"] or {},
            reproducibility_level=PackageValidationReproducibility(row["reproducibility_level"]),
            created_by=row["created_by"],
            created_at=row["created_at"],
            completed_at=row["completed_at"],
        )


class InMemoryStrategyPackageRepository:
    """Test repository with the same fail-fast semantics as PostgreSQL."""

    def __init__(self) -> None:
        self.records: dict[str, StrategyPackageRecord] = {}
        self.events: list[PackageStatusEvent] = []
        self.execution_policies: dict[str, ValidatedExecutionPolicy] = {}
        self.live_approvals: dict[str, StrategyPackageLiveApproval] = {}
        self.package_assets: dict[tuple[str, StrategyPackageAssetType, str], StrategyPackageAssetRecord] = {}
        self._next_package_asset_id = 1
        self.model_states: dict[str, StrategyPackageModelState] = {}
        self.model_retrain_jobs: dict[str, StrategyPackageModelRetrainJob] = {}
        self.runtime_variants: dict[str, StrategyPackageRuntimeVariant] = {}
        self.validation_runs: dict[str, StrategyPackageValidationRun] = {}
        self.components: dict[int, StrategyPackageComponentRecord] = {}
        self._next_component_id = 1

    def save_manifest(self, manifest: StrategyPackageManifest) -> StrategyPackageRecord:
        frozen = freeze_manifest(manifest)
        existing = self.records.get(frozen.package_id)
        if existing:
            if existing.manifest_sha256 != frozen.manifest_sha256:
                raise InvalidStateTransitionError(
                    "package manifest cannot be silently replaced",
                    context={"package_id": frozen.package_id},
                )
            return existing
        now = datetime.now(timezone.utc)
        record = StrategyPackageRecord(
            package_id=frozen.package_id,
            package_name=frozen.package_name,
            package_version=frozen.package_version,
            source_type=frozen.source.source_type.value,
            source_id=frozen.source.source_id,
            loop_id=frozen.source.loop_id,
            run_id=frozen.source.run_id,
            package_status=frozen.package_status,
            manifest=frozen,
            manifest_sha256=frozen.manifest_sha256 or "",
            alpha_mode=frozen.alpha_mode,
            display_name=frozen.package_name,
            legacy_name=frozen.package_name,
            data_vintage=_infer_data_vintage(frozen),
            created_at=now,
            updated_at=now,
        )
        self.records[record.package_id] = record
        self.events.append(
            PackageStatusEvent(
                package_id=record.package_id,
                from_status=None,
                to_status=record.package_status,
                reason="package_created",
                context={"manifest_sha256": record.manifest_sha256},
            )
        )
        return record

    def save_manifest_with_assets(
        self,
        manifest: StrategyPackageManifest,
        assets: list[StrategyPackageAssetRecord],
    ) -> StrategyPackageRecord:
        frozen = freeze_manifest(manifest)
        _validate_asset_records_for_manifest(frozen, assets)
        existing = self.records.get(frozen.package_id)
        if existing:
            if existing.manifest_sha256 != frozen.manifest_sha256:
                raise InvalidStateTransitionError(
                    "package manifest cannot be silently replaced",
                    context={"package_id": frozen.package_id},
                )
            if not self._has_package_asset_rows(existing.package_id, assets):
                raise InvalidStateTransitionError(
                    "strategy package exists without required frozen asset rows",
                    context={
                        "reason_code": "strategy_package_source_existing_assets_incomplete",
                        "package_id": existing.package_id,
                        "required_assets": _asset_key_payload(assets),
                    },
                )
            return existing
        source_existing = self.find_by_source_version(
            source_type=frozen.source.source_type.value,
            source_id=frozen.source.source_id,
            loop_id=frozen.source.loop_id,
            package_version=frozen.package_version,
        )
        if source_existing:
            if not self._has_package_asset_rows(source_existing.package_id, assets):
                raise InvalidStateTransitionError(
                    "strategy package source version exists without required frozen asset rows",
                    context={
                        "reason_code": "strategy_package_source_existing_assets_incomplete",
                        "package_id": source_existing.package_id,
                        "required_assets": _asset_key_payload(assets),
                    },
                )
            return source_existing
        record = self.save_manifest(frozen)
        try:
            for asset in assets:
                self.save_package_asset(asset)
        except Exception:
            self.records.pop(record.package_id, None)
            self.events = [event for event in self.events if event.package_id != record.package_id]
            self.package_assets = {
                key: value for key, value in self.package_assets.items() if value.package_id != record.package_id
            }
            raise
        return self.get(record.package_id)

    def backfill_frozen_manifest_assets(
        self,
        package_id: str,
        *,
        frozen_manifest: StrategyPackageManifest,
        assets: list[StrategyPackageAssetRecord],
        operator: str,
        expected_old_manifest_sha256: str,
    ) -> StrategyPackageRecord:
        record = self.get(package_id)
        expected_old = str(expected_old_manifest_sha256 or "").strip().lower()
        if record.manifest_sha256 != expected_old:
            raise InvalidStateTransitionError(
                "strategy package asset backfill lost compare-and-set race",
                context={
                    "reason_code": "strategy_package_asset_backfill_cas_mismatch",
                    "package_id": package_id,
                    "expected_old_manifest_sha256": expected_old,
                    "actual_manifest_sha256": record.manifest_sha256,
                },
            )
        if frozen_manifest.package_id != package_id:
            raise StrategyPackageValidationError(
                "backfilled manifest package_id must match target package",
                context={
                    "reason_code": "strategy_package_asset_backfill_package_mismatch",
                    "package_id": package_id,
                    "manifest_package_id": frozen_manifest.package_id,
                },
            )
        frozen = freeze_manifest(
            frozen_manifest.model_copy(
                update={
                    "manifest_sha256": None,
                    "package_status": record.package_status,
                }
            )
        )
        _validate_asset_records_for_manifest(frozen, assets)
        backup_record = record
        backup_assets = dict(self.package_assets)
        backup_events = list(self.events)
        try:
            for asset in assets:
                key = (asset.package_id, asset.asset_type, asset.asset_ref)
                existing = self.package_assets.get(key)
                if existing is not None and existing.asset_sha256 != asset.asset_sha256:
                    raise StrategyPackageValidationError(
                        "existing package_asset row has a different sha256 for the same asset_ref",
                        context={
                            "reason_code": "strategy_package_asset_sha_mismatch",
                            "package_id": asset.package_id,
                            "asset_type": asset.asset_type.value,
                            "asset_ref": asset.asset_ref,
                            "asset_sha256": asset.asset_sha256,
                            "existing_asset_sha256": existing.asset_sha256,
                        },
                    )
                asset_id = existing.asset_id if existing else self._next_package_asset_id
                if existing is None:
                    self._next_package_asset_id += 1
                self.package_assets[key] = asset.model_copy(update={"asset_id": asset_id})
            updated = record.model_copy(
                update={
                    "manifest": frozen,
                    "manifest_sha256": frozen.manifest_sha256 or "",
                    "updated_at": datetime.now(timezone.utc),
                }
            )
            self.records[package_id] = updated
            self.events.append(
                PackageStatusEvent(
                    package_id=package_id,
                    from_status=record.package_status,
                    to_status=record.package_status,
                    reason="strategy_package_asset_backfill_freeze",
                    context={
                        "operator": operator,
                        "old_manifest_sha256": record.manifest_sha256,
                        "new_manifest_sha256": frozen.manifest_sha256,
                        "asset_count": len(assets),
                        "asset_keys": _asset_key_payload(assets),
                        "rollback_restore": {
                            "field": "strategy_pkg.package.manifest_json, strategy_pkg.package.manifest_sha256",
                            "manifest_sha256": record.manifest_sha256,
                            "manifest_json": record.current_manifest().model_dump(mode="json"),
                        },
                    },
                )
            )
        except Exception:
            self.records[package_id] = backup_record
            self.package_assets = backup_assets
            self.events = backup_events
            raise
        return self.get(package_id)

    def _has_package_asset_rows(self, package_id: str, assets: list[StrategyPackageAssetRecord]) -> bool:
        existing = {
            (asset.asset_type, asset.asset_ref, asset.asset_sha256)
            for asset in self.list_package_assets(package_id)
        }
        return all((asset.asset_type, asset.asset_ref, asset.asset_sha256) in existing for asset in assets)

    def find_by_source_version(
        self,
        *,
        source_type: str,
        source_id: str,
        loop_id: str | None,
        package_version: str,
    ) -> StrategyPackageRecord | None:
        loop_key = loop_id or ""
        for record in self.records.values():
            if (
                record.source_type == source_type
                and record.source_id == source_id
                and (record.loop_id or "") == loop_key
                and record.package_version == package_version
            ):
                return record
        return None

    def list_single_alpha_candidates_for_seed_reuse(self, *, limit: int = 2000) -> list[StrategyPackageRecord]:
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        records = [
            record
            for record in self.records.values()
            if record.alpha_mode == AlphaMode.SINGLE_ALPHA and record.package_status != PackageStatus.RETIRED
        ]
        return sorted(records, key=lambda record: (record.created_at, record.package_id), reverse=True)[:limit]

    def get(self, package_id: str) -> StrategyPackageRecord:
        record = self.records.get(package_id)
        if record is None:
            raise DataUnavailableError("strategy package does not exist", context={"package_id": package_id})
        return record

    def list(self, *, status: PackageStatus | None = None, limit: int = 100) -> list[StrategyPackageRecord]:
        records = list(self.records.values())
        if status is not None:
            records = [record for record in records if record.package_status == status]
        result: list[StrategyPackageRecord] = []
        for record in records[:limit]:
            computed = compute_manifest_sha256(record.current_manifest())
            if record.manifest_sha256 != computed:
                logger.warning(
                    "strategy package manifest_sha256 drift quarantined: package_id=%s stored=%s computed=%s",
                    record.package_id,
                    record.manifest_sha256,
                    computed,
                )
                continue
            result.append(record)
        return result

    def package_delete_dependencies(self, package_id: str) -> dict[str, Any]:
        self.get(package_id)
        live_approvals = [
            {
                "approval_id": approval.approval_id,
                "approval_status": approval.approval_status.value,
                "portfolio_id": approval.portfolio_id,
            }
            for approval in self.live_approvals.values()
            if approval.package_id == package_id
        ]
        component_child_edges = [
            {
                "parent_package_id": component.parent_package_id,
                "child_package_id": component.child_package_id,
                "position": component.position,
            }
            for component in self.components.values()
            if component.child_package_id == package_id
        ]
        component_parent_edges = [
            {
                "parent_package_id": component.parent_package_id,
                "child_package_id": component.child_package_id,
                "position": component.position,
            }
            for component in self.components.values()
            if component.parent_package_id == package_id
        ]
        return {
            "paper_portfolios": [],
            "active_paper_portfolios": [],
            "selection_run_ids": [],
            "qmt_bindings": [],
            "live_approvals": live_approvals,
            "strategy_runtime_releases": [],
            "simulation_release_bindings": [],
            "simulation_daily_runs": [],
            "execution_plans": [],
            "component_child_edges": component_child_edges,
            "component_parent_edges": component_parent_edges,
            "daily_selection_evidence": 0,
            "selection_score_artifacts": 0,
        }

    def delete_package(self, package_id: str) -> dict[str, Any]:
        record = self.get(package_id)
        dependencies = self.package_delete_dependencies(package_id)
        blockers = StrategyPackageRepository._package_delete_blockers(dependencies)
        if blockers:
            raise InvalidStateTransitionError(
                "strategy package delete blocked by existing runtime references",
                context={"package_id": package_id, "blockers": blockers, "dependencies": dependencies},
            )
        counts = {
            "package_validation_run": len([run for run in self.validation_runs.values() if run.package_id == package_id]),
            "model_retrain_job": len([job for job in self.model_retrain_jobs.values() if job.package_id == package_id]),
            "model_state": 1 if package_id in self.model_states else 0,
            "package_runtime_variant": len([variant for variant in self.runtime_variants.values() if variant.package_id == package_id]),
            "validated_execution_policy": len([policy for policy in self.execution_policies.values() if policy.package_id == package_id]),
            "package_asset": len([asset for asset in self.package_assets.values() if asset.package_id == package_id]),
            "strategy_package_components": len([
                component
                for component in self.components.values()
                if component.parent_package_id == package_id or component.child_package_id == package_id
            ]),
            "package_status_event": len([event for event in self.events if event.package_id == package_id]),
            "package": 1,
        }
        self.components = {
            component_id: component
            for component_id, component in self.components.items()
            if component.parent_package_id != package_id and component.child_package_id != package_id
        }
        self.validation_runs = {
            run_id: run for run_id, run in self.validation_runs.items() if run.package_id != package_id
        }
        self.model_retrain_jobs = {
            job_id: job for job_id, job in self.model_retrain_jobs.items() if job.package_id != package_id
        }
        self.model_states.pop(package_id, None)
        self.runtime_variants = {
            variant_id: variant for variant_id, variant in self.runtime_variants.items() if variant.package_id != package_id
        }
        self.execution_policies = {
            policy_id: policy for policy_id, policy in self.execution_policies.items() if policy.package_id != package_id
        }
        self.package_assets = {
            key: asset for key, asset in self.package_assets.items() if asset.package_id != package_id
        }
        self.events = [event for event in self.events if event.package_id != package_id]
        self.records.pop(package_id, None)
        return {
            "package_id": package_id,
            "manifest_sha256": record.manifest_sha256,
            "deleted_counts": counts,
            "dependencies": dependencies,
        }

    def validate_manifest_integrity(self, *, limit: int = 500) -> dict[str, Any]:
        records = list(self.records.values())[:limit]
        drifted: list[dict[str, Any]] = []
        clean_count = 0
        for record in records:
            try:
                computed = compute_manifest_sha256(record.current_manifest())
            except Exception:
                drifted.append({
                    "package_id": record.package_id,
                    "package_name": record.package_name,
                    "package_status": record.package_status.value,
                    "stored_sha256": record.manifest_sha256,
                    "computed_sha256": None,
                    "error": "manifest_json failed pydantic validation",
                    "repair_plan": _manifest_drift_repair_plan(
                        stored_sha256=record.manifest_sha256,
                        computed_sha256=None,
                        manifest_json=_manifest_json_for_record_classification(record),
                    ),
                })
                continue
            if record.manifest_sha256 != computed:
                drifted.append({
                    "package_id": record.package_id,
                    "package_name": record.package_name,
                    "package_status": record.package_status.value,
                    "stored_sha256": record.manifest_sha256,
                    "computed_sha256": computed,
                    "impact": {
                        "paper_portfolio_count": record.paper_portfolio_count,
                        "blocks_detail_endpoint": True,
                        "excluded_from_package_list": True,
                    },
                    "repair_plan": _manifest_drift_repair_plan(
                        stored_sha256=record.manifest_sha256,
                        computed_sha256=computed,
                        manifest_json=_manifest_json_for_record_classification(record),
                    ),
                })
            else:
                clean_count += 1
        return {
            "total_scanned": len(records),
            "clean_count": clean_count,
            "drifted_count": len(drifted),
            "drifted": drifted,
        }

    def repair_manifest_hash(
        self,
        package_id: str,
        *,
        operator: str = "repair_manifest_hash",
        confirm_stored_sha256: str | None = None,
        confirm_computed_sha256: str | None = None,
    ) -> StrategyPackageRecord:
        record = self.records.get(package_id)
        if record is None:
            raise DataUnavailableError("strategy package does not exist", context={"package_id": package_id})
        correct_hash = compute_manifest_sha256(record.current_manifest())
        if record.manifest_sha256 == correct_hash:
            return record
        repair_plan = _manifest_drift_repair_plan(
            stored_sha256=record.manifest_sha256,
            computed_sha256=correct_hash,
            manifest_json=_manifest_json_for_record_classification(record),
        )
        if repair_plan["classification"]["classification"] != SAFE_MANIFEST_REPAIR_CLASSIFICATION:
            raise InvalidStateTransitionError(
                "manifest hash repair blocked until manifest drift classification is safe",
                context={
                    "package_id": package_id,
                    "stored_sha256": record.manifest_sha256,
                    "computed_sha256": correct_hash,
                    "repair_plan": repair_plan,
                },
            )
        if confirm_stored_sha256 != record.manifest_sha256 or confirm_computed_sha256 != correct_hash:
            raise InvalidStateTransitionError(
                "manifest hash repair requires explicit stored/computed hash confirmation",
                context={
                    "package_id": package_id,
                    "stored_sha256": record.manifest_sha256,
                    "computed_sha256": correct_hash,
                    "confirm_stored_sha256": confirm_stored_sha256,
                    "confirm_computed_sha256": confirm_computed_sha256,
                    "repair_plan": repair_plan,
                },
            )
        updated = record.model_copy(update={"manifest_sha256": correct_hash, "updated_at": datetime.now(timezone.utc)})
        self.records[package_id] = updated
        self.events.append(
            PackageStatusEvent(
                package_id=package_id,
                from_status=record.package_status,
                to_status=record.package_status,
                reason="manifest_hash_repaired",
                context={
                    "operator": operator,
                    "old_manifest_sha256": record.manifest_sha256,
                    "new_manifest_sha256": correct_hash,
                    "repair_classification": repair_plan["classification"]["classification"],
                    "classification_reason": repair_plan["classification"]["reason"],
                    "rollback_restore": {
                        "field": "strategy_pkg.package.manifest_sha256",
                        "restore_value": record.manifest_sha256,
                    },
                },
            )
        )
        return updated

    def transition_status(
        self,
        *,
        package_id: str,
        to_status: PackageStatus,
        allowed_from: set[PackageStatus],
        reason: str,
        context: dict[str, Any] | None = None,
    ) -> StrategyPackageRecord:
        record = self.get(package_id)
        if to_status == PackageStatus.RETIRED:
            parents = [
                component.parent_package_id
                for component in self.components.values()
                if component.child_package_id == package_id
                and self.records.get(component.parent_package_id)
                and self.records[component.parent_package_id].package_status != PackageStatus.RETIRED
            ]
            if parents:
                raise InvalidStateTransitionError(
                    "referenced single-alpha child package cannot be retired",
                    context={"package_id": package_id, "active_parent_package_ids": sorted(parents)},
                )
        if record.package_status not in allowed_from:
            raise InvalidStateTransitionError(
                "invalid strategy package status transition",
                context={
                    "package_id": package_id,
                    "from_status": record.package_status.value,
                    "to_status": to_status.value,
                    "allowed_from": sorted(item.value for item in allowed_from),
                },
            )
        updated = record.model_copy(update={"package_status": to_status, "updated_at": datetime.now(timezone.utc)})
        self.records[package_id] = updated
        self.events.append(
            PackageStatusEvent(
                package_id=package_id,
                from_status=record.package_status,
                to_status=to_status,
                reason=reason,
                context=context or {},
            )
        )
        return updated

    def update_artifact_refs(
        self,
        package_id: str,
        *,
        prediction_ref_uri: str | None = None,
        prediction_ref_sha256: str | None = None,
        model_artifact_uri: str | None = None,
        model_artifact_sha256: str | None = None,
    ) -> StrategyPackageRecord:
        record = self.get(package_id)
        if prediction_ref_uri is not None and not prediction_ref_sha256:
            raise StrategyPackageValidationError(
                "prediction_ref_sha256 is required when prediction_ref_uri is set",
                context={"package_id": package_id, "prediction_ref_uri": prediction_ref_uri},
            )
        if model_artifact_uri is not None and not model_artifact_sha256:
            raise StrategyPackageValidationError(
                "model_artifact_sha256 is required when model_artifact_uri is set",
                context={"package_id": package_id, "model_artifact_uri": model_artifact_uri},
            )
        updated = record.model_copy(
            update={
                "prediction_ref_uri": prediction_ref_uri if prediction_ref_uri is not None else record.prediction_ref_uri,
                "prediction_ref_sha256": prediction_ref_sha256 if prediction_ref_sha256 is not None else record.prediction_ref_sha256,
                "model_artifact_uri": model_artifact_uri if model_artifact_uri is not None else record.model_artifact_uri,
                "model_artifact_sha256": model_artifact_sha256 if model_artifact_sha256 is not None else record.model_artifact_sha256,
                "updated_at": datetime.now(timezone.utc),
            }
        )
        self.records[package_id] = updated
        return updated

    def save_components(
        self,
        parent_package_id: str,
        components: list[StrategyPackageComponentRecord],
    ) -> list[StrategyPackageComponentRecord]:
        self.get(parent_package_id)
        if not components:
            raise StrategyPackageValidationError(
                "multi-alpha package requires at least one component edge",
                context={"parent_package_id": parent_package_id},
            )
        self.components = {
            component_id: component
            for component_id, component in self.components.items()
            if component.parent_package_id != parent_package_id
        }
        saved: list[StrategyPackageComponentRecord] = []
        for component in components:
            component_id = self._next_component_id
            self._next_component_id += 1
            stored = component.model_copy(update={"id": component_id, "created_at": datetime.now(timezone.utc)})
            self.components[component_id] = stored
            saved.append(stored)
        return saved

    def list_components(self, parent_package_id: str) -> list[StrategyPackageComponentRecord]:
        self.get(parent_package_id)
        return sorted(
            [component for component in self.components.values() if component.parent_package_id == parent_package_id],
            key=lambda item: (item.position, item.id or 0),
        )

    def list_component_parents(self, child_package_id: str) -> list[StrategyPackageComponentRecord]:
        self.get(child_package_id)
        return sorted(
            [component for component in self.components.values() if component.child_package_id == child_package_id],
            key=lambda item: (item.created_at, item.id or 0),
            reverse=True,
        )

    def list_status_events(self, package_id: str, *, limit: int = 200) -> list[PackageStatusEvent]:
        self.get(package_id)
        return [event for event in self.events if event.package_id == package_id][:limit]

    def save_package_asset(self, asset: StrategyPackageAssetRecord) -> StrategyPackageAssetRecord:
        self.get(asset.package_id)
        key = (asset.package_id, asset.asset_type, asset.asset_ref)
        existing = self.package_assets.get(key)
        asset_id = existing.asset_id if existing else self._next_package_asset_id
        if existing is None:
            self._next_package_asset_id += 1
        saved = asset.model_copy(update={"asset_id": asset_id})
        self.package_assets[key] = saved
        return saved

    def list_package_assets(self, package_id: str, *, protected_only: bool = False) -> list[StrategyPackageAssetRecord]:
        self.get(package_id)
        rows = [asset for asset in self.package_assets.values() if asset.package_id == package_id]
        if protected_only:
            rows = [asset for asset in rows if asset.protected_asset]
        rows.sort(key=lambda item: item.created_at, reverse=True)
        return rows

    def mark_paper_portfolio_created(self, package_id: str, portfolio_id: str) -> StrategyPackageRecord:
        record = self.get(package_id)
        updated = record.model_copy(
            update={
                "paper_portfolio_count": record.paper_portfolio_count + 1,
                "updated_at": datetime.now(timezone.utc),
            }
        )
        self.records[package_id] = updated
        self.events.append(
            PackageStatusEvent(
                package_id=package_id,
                from_status=record.package_status,
                to_status=record.package_status,
                reason="paper_portfolio_created",
                context={"portfolio_id": portfolio_id},
            )
        )
        return updated

    def save_execution_policy(self, policy: ValidatedExecutionPolicy) -> ValidatedExecutionPolicy:
        self.get(policy.package_id)
        policy = policy.model_copy(update={"paper_enabled": False})
        for existing in self.execution_policies.values():
            if existing.package_id == policy.package_id and existing.policy_sha256 == policy.policy_sha256:
                return existing
        self.execution_policies[policy.policy_id] = policy
        return policy

    def get_execution_policy(self, package_id: str, policy_id: str) -> ValidatedExecutionPolicy:
        policy = self.execution_policies.get(policy_id)
        if policy is None or policy.package_id != package_id:
            raise DataUnavailableError(
                "validated execution policy does not exist",
                context={"package_id": package_id, "policy_id": policy_id},
            )
        return policy

    def list_execution_policies(self, package_id: str) -> list[ValidatedExecutionPolicy]:
        self.get(package_id)
        return [policy for policy in self.execution_policies.values() if policy.package_id == package_id]

    def set_execution_policy_paper_enabled(
        self,
        *,
        package_id: str,
        policy_id: str,
        paper_enabled: bool,
    ) -> ValidatedExecutionPolicy:
        _ = paper_enabled
        policy = self.get_execution_policy(package_id, policy_id)
        updated = policy.model_copy(update={"paper_enabled": False, "updated_at": datetime.now(timezone.utc)})
        self.execution_policies[policy_id] = updated
        return updated

    def save_live_approval(self, approval: StrategyPackageLiveApproval) -> StrategyPackageLiveApproval:
        self.get(approval.package_id)
        if approval.approval_id in self.live_approvals:
            raise InvalidStateTransitionError(
                "strategy package live approval already exists",
                context={"package_id": approval.package_id, "approval_id": approval.approval_id},
            )
        self.live_approvals[approval.approval_id] = approval
        return approval

    def get_live_approval(self, package_id: str, approval_id: str) -> StrategyPackageLiveApproval:
        self.get(package_id)
        approval = self.live_approvals.get(approval_id)
        if approval is None or approval.package_id != package_id:
            raise DataUnavailableError(
                "strategy package live approval does not exist",
                context={"package_id": package_id, "approval_id": approval_id},
            )
        return approval

    def list_live_approvals(
        self,
        *,
        package_id: str | None = None,
        portfolio_id: str | None = None,
        status: LiveApprovalStatus | None = None,
        limit: int = 100,
    ) -> list[StrategyPackageLiveApproval]:
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        if package_id is not None:
            self.get(package_id)
        rows = list(self.live_approvals.values())
        if package_id is not None:
            rows = [item for item in rows if item.package_id == package_id]
        if portfolio_id is not None:
            rows = [item for item in rows if item.portfolio_id == portfolio_id]
        if status is not None:
            rows = [item for item in rows if item.approval_status == status]
        rows.sort(key=lambda item: (item.updated_at, item.created_at, item.approval_id), reverse=True)
        return rows[:limit]

    def update_live_approval(self, approval: StrategyPackageLiveApproval) -> StrategyPackageLiveApproval:
        self.get_live_approval(approval.package_id, approval.approval_id)
        self.live_approvals[approval.approval_id] = approval
        return approval

    def get_model_state(self, package_id: str) -> StrategyPackageModelState | None:
        self.get(package_id)
        return self.model_states.get(package_id)

    def upsert_model_state(self, state: StrategyPackageModelState) -> StrategyPackageModelState:
        self.get(state.package_id)
        self.model_states[state.package_id] = state
        return state

    def save_model_retrain_job(self, job: StrategyPackageModelRetrainJob) -> StrategyPackageModelRetrainJob:
        self.get(job.package_id)
        self.model_retrain_jobs[job.job_id] = job
        return job

    def get_model_retrain_job(self, package_id: str, job_id: str) -> StrategyPackageModelRetrainJob:
        self.get(package_id)
        job = self.model_retrain_jobs.get(job_id)
        if job is None or job.package_id != package_id:
            raise DataUnavailableError(
                "model retrain job does not exist",
                context={"package_id": package_id, "job_id": job_id},
            )
        return job

    def list_model_retrain_jobs(self, package_id: str, *, limit: int = 100) -> list[StrategyPackageModelRetrainJob]:
        self.get(package_id)
        rows = [job for job in self.model_retrain_jobs.values() if job.package_id == package_id]
        rows.sort(key=lambda item: item.created_at, reverse=True)
        return rows[:limit]

    def save_runtime_variant(self, variant: StrategyPackageRuntimeVariant) -> StrategyPackageRuntimeVariant:
        record = self.get(variant.package_id)
        variant = variant.model_copy(update={"paper_candidate": False})
        _validate_variant_matches_package(variant, record)
        ensure_runtime_variant_status(
            validation_status=variant.validation_status,
            paper_candidate=variant.paper_candidate,
            validation_evidence=variant.validation_evidence,
        )
        for existing in self.runtime_variants.values():
            if existing.package_id == variant.package_id and existing.variant_hash == variant.variant_hash:
                return existing
        self.runtime_variants[variant.variant_id] = variant
        return variant

    def get_runtime_variant(self, package_id: str, variant_id: str) -> StrategyPackageRuntimeVariant:
        self.get(package_id)
        variant = self.runtime_variants.get(variant_id)
        if variant is None or variant.package_id != package_id:
            raise DataUnavailableError(
                "strategy package runtime variant does not exist",
                context={"package_id": package_id, "variant_id": variant_id},
            )
        return variant

    def list_runtime_variants(
        self,
        package_id: str,
        *,
        include_retired: bool = False,
        limit: int = 100,
    ) -> list[StrategyPackageRuntimeVariant]:
        self.get(package_id)
        rows = [
            variant
            for variant in self.runtime_variants.values()
            if variant.package_id == package_id
            and (
                include_retired
                or variant.validation_status != RuntimeVariantValidationStatus.RETIRED
            )
        ]
        rows.sort(key=lambda item: item.created_at, reverse=True)
        return rows[:limit]

    def set_runtime_variant_validation(
        self,
        *,
        package_id: str,
        variant_id: str,
        validation_status: RuntimeVariantValidationStatus,
        paper_candidate: bool,
        validation_evidence: dict[str, Any],
    ) -> StrategyPackageRuntimeVariant:
        variant = self.get_runtime_variant(package_id, variant_id)
        paper_candidate = False
        ensure_runtime_variant_status(
            validation_status=validation_status,
            paper_candidate=paper_candidate,
            validation_evidence=validation_evidence,
        )
        updated = variant.model_copy(
            update={
                "validation_status": validation_status,
                "paper_candidate": paper_candidate,
                "validation_evidence": validation_evidence,
                "updated_at": datetime.now(timezone.utc),
            }
        )
        self.runtime_variants[variant_id] = updated
        return updated

    def save_validation_run(self, run: StrategyPackageValidationRun) -> StrategyPackageValidationRun:
        record = self.get(run.package_id)
        _validate_validation_run_matches_package(run, record)
        if run.runtime_variant_id is not None:
            variant = self.get_runtime_variant(run.package_id, run.runtime_variant_id)
            _validate_validation_run_matches_variant(run, variant.variant_hash)
        ensure_package_validation_run(run)
        if run.validation_run_id in self.validation_runs:
            raise StrategyPackageValidationError(
                "validation run already exists",
                context={"validation_run_id": run.validation_run_id},
            )
        self.validation_runs[run.validation_run_id] = run
        return run

    def get_validation_run(self, package_id: str, validation_run_id: str) -> StrategyPackageValidationRun:
        self.get(package_id)
        run = self.validation_runs.get(validation_run_id)
        if run is None or run.package_id != package_id:
            raise DataUnavailableError(
                "strategy package validation run does not exist",
                context={"package_id": package_id, "validation_run_id": validation_run_id},
            )
        return run

    def list_validation_runs(
        self,
        package_id: str,
        *,
        validation_type: PackageValidationType | None = None,
        runtime_variant_id: str | None = None,
        limit: int | None = 100,
    ) -> list[StrategyPackageValidationRun]:
        self.get(package_id)
        if limit is not None and limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        rows = [run for run in self.validation_runs.values() if run.package_id == package_id]
        if validation_type is not None:
            rows = [run for run in rows if run.validation_type == validation_type]
        if runtime_variant_id is not None:
            rows = [run for run in rows if run.runtime_variant_id == runtime_variant_id]
        rows.sort(key=lambda item: (item.created_at, item.validation_run_id), reverse=True)
        if limit is None:
            return rows
        return rows[:limit]


def _validate_variant_matches_package(
    variant: StrategyPackageRuntimeVariant,
    record: StrategyPackageRecord,
) -> None:
    current_manifest = record.current_manifest()
    if variant.manifest_sha256 != record.manifest_sha256:
        raise StrategyPackageValidationError(
            "runtime variant manifest_sha256 does not match current package",
            context={
                "package_id": record.package_id,
                "variant_manifest_sha256": variant.manifest_sha256,
                "package_manifest_sha256": record.manifest_sha256,
            },
        )
    expected_core_hash = derive_locked_core_hash(current_manifest)
    if variant.locked_core_hash != expected_core_hash:
        raise StrategyPackageValidationError(
            "runtime variant locked core hash does not match current package core",
            context={
                "package_id": record.package_id,
                "variant_locked_core_hash": variant.locked_core_hash,
                "expected_locked_core_hash": expected_core_hash,
            },
        )


def _validate_validation_run_matches_package(
    run: StrategyPackageValidationRun,
    record: StrategyPackageRecord,
) -> None:
    if run.manifest_sha256 != record.manifest_sha256:
        raise StrategyPackageValidationError(
            "validation run manifest_sha256 does not match current package manifest",
            context={
                "package_id": run.package_id,
                "run_manifest_sha256": run.manifest_sha256,
                "package_manifest_sha256": record.manifest_sha256,
            },
        )


def _validate_validation_run_matches_variant(run: StrategyPackageValidationRun, expected_variant_hash: str) -> None:
    if run.runtime_variant_hash != expected_variant_hash:
        raise StrategyPackageValidationError(
            "validation run runtime_variant_hash does not match current runtime variant",
            context={
                "package_id": run.package_id,
                "runtime_variant_id": run.runtime_variant_id,
                "run_runtime_variant_hash": run.runtime_variant_hash,
                "expected_runtime_variant_hash": expected_variant_hash,
            },
        )

