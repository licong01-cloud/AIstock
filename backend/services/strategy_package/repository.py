"""Persistence for Strategy Package Center.

The repository stores immutable frozen manifests plus a mutable package status
column. QE source tables are read-only from this layer.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Iterator

import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    StrategyPackageValidationError,
)

from .execution_policy import ValidatedExecutionPolicy
from .manifest import compute_manifest_sha256, freeze_manifest
from .model_state import (
    ModelRetrainJobStatus,
    ModelStalenessStatus,
    StrategyPackageModelRetrainJob,
    StrategyPackageModelState,
)
from .models import PackageStatus, StrategyPackageManifest
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

ConnFactory = Callable[[], Iterator[Any]]


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
    paper_portfolio_count: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def current_manifest(self) -> StrategyPackageManifest:
        return self.manifest.model_copy(update={"package_status": self.package_status})


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
        with self._conn_factory() as conn:
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
                    return self.get(frozen.package_id)

                cur.execute(
                    """
                    INSERT INTO strategy_pkg.package (
                        package_id, package_name, package_version, source_type,
                        source_id, loop_id, run_id, package_status, manifest_json,
                        manifest_sha256
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        return self.get(frozen.package_id)

    def get(self, package_id: str) -> StrategyPackageRecord:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT package_id, package_name, package_version, source_type,
                           source_id, loop_id, run_id, package_status, manifest_json,
                           manifest_sha256, paper_portfolio_count, created_at, updated_at
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
                           manifest_sha256, paper_portfolio_count, created_at, updated_at
                    FROM strategy_pkg.package
                    {where}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [self._record_from_row(dict(row)) for row in rows]

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
        return self.get(package_id)

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

    def save_execution_policy(self, policy: ValidatedExecutionPolicy) -> ValidatedExecutionPolicy:
        self.get(policy.package_id)
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
        self.get_execution_policy(package_id, policy_id)
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE strategy_pkg.validated_execution_policy
                    SET paper_enabled = %s, updated_at = NOW()
                    WHERE package_id = %s AND policy_id = %s
                    """,
                    (paper_enabled, package_id, policy_id),
                )
        return self.get_execution_policy(package_id, policy_id)

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
        limit: int = 100,
    ) -> list[StrategyPackageValidationRun]:
        self.get(package_id)
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        where = ["package_id = %s"]
        params: list[Any] = [package_id]
        if validation_type is not None:
            where.append("validation_type = %s")
            params.append(validation_type.value)
        if runtime_variant_id is not None:
            where.append("runtime_variant_id = %s")
            params.append(runtime_variant_id)
        params.append(limit)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM strategy_pkg.package_validation_run
                    WHERE {' AND '.join(where)}
                    ORDER BY created_at DESC, validation_run_id DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [self._validation_run_from_row(dict(row)) for row in rows]

    def _record_from_row(self, row: dict[str, Any]) -> StrategyPackageRecord:
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
            paper_portfolio_count=int(row.get("paper_portfolio_count") or 0),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
        if record.manifest_sha256 != compute_manifest_sha256(record.current_manifest()):
            raise StrategyPackageValidationError(
                "stored manifest_sha256 does not match stored manifest",
                context={"package_id": record.package_id},
            )
        return record

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
        self.model_states: dict[str, StrategyPackageModelState] = {}
        self.model_retrain_jobs: dict[str, StrategyPackageModelRetrainJob] = {}
        self.runtime_variants: dict[str, StrategyPackageRuntimeVariant] = {}
        self.validation_runs: dict[str, StrategyPackageValidationRun] = {}

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

    def get(self, package_id: str) -> StrategyPackageRecord:
        record = self.records.get(package_id)
        if record is None:
            raise DataUnavailableError("strategy package does not exist", context={"package_id": package_id})
        return record

    def list(self, *, status: PackageStatus | None = None, limit: int = 100) -> list[StrategyPackageRecord]:
        records = list(self.records.values())
        if status is not None:
            records = [record for record in records if record.package_status == status]
        return records[:limit]

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
                context={"package_id": package_id, "from_status": record.package_status.value, "to_status": to_status.value},
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

    def list_status_events(self, package_id: str, *, limit: int = 200) -> list[PackageStatusEvent]:
        self.get(package_id)
        return [event for event in self.events if event.package_id == package_id][:limit]

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
        policy = self.get_execution_policy(package_id, policy_id)
        updated = policy.model_copy(update={"paper_enabled": paper_enabled, "updated_at": datetime.now(timezone.utc)})
        self.execution_policies[policy_id] = updated
        return updated

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
        limit: int = 100,
    ) -> list[StrategyPackageValidationRun]:
        self.get(package_id)
        rows = [run for run in self.validation_runs.values() if run.package_id == package_id]
        if validation_type is not None:
            rows = [run for run in rows if run.validation_type == validation_type]
        if runtime_variant_id is not None:
            rows = [run for run in rows if run.runtime_variant_id == runtime_variant_id]
        rows.sort(key=lambda item: item.created_at, reverse=True)
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
