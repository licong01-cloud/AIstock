"""Durable Candidate StrategyPackage snapshots.

Candidate StrategyPackages are explicit user snapshots between QE evidence and
formal StrategyPackage creation. They intentionally do not depend on live QE
source rows so source cleanup cannot erase the candidate decision record.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Iterator, Protocol
from uuid import uuid4

import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    StrategyPackageValidationError,
)

from .models import StrategyPackageManifest
from .qe_source_resolver import QEExperimentSourceResolver

ConnFactory = Callable[[], Iterator[Any]]


class CandidateStrategyPackageSourceType(str, Enum):
    QE_EXPERIMENT = "qe_experiment"
    QE_EVOLUTION_LOOP = "qe_evolution_loop"
    CANDIDATE_STRATEGY_PACKAGE = "candidate_strategy_package"


class CandidateStrategyPackageStatus(str, Enum):
    ACTIVE = "ACTIVE"
    DELETED = "DELETED"


class CandidateStrategyPackageRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_id: str
    candidate_version: int = 1
    source_type: CandidateStrategyPackageSourceType
    source_id: str
    source_task_id: str | None = None
    source_loop_id: str | None = None
    source_experiment_id: str | None = None
    archive_run_id: str | None = None
    display_name: str
    status: CandidateStrategyPackageStatus = CandidateStrategyPackageStatus.ACTIVE
    snapshot_config: dict[str, Any] = Field(default_factory=dict)
    factor_manifest: dict[str, Any] = Field(default_factory=dict)
    model_manifest: dict[str, Any] = Field(default_factory=dict)
    strategy_manifest: dict[str, Any] = Field(default_factory=dict)
    metric_snapshot: dict[str, Any] = Field(default_factory=dict)
    artifact_refs: dict[str, Any] = Field(default_factory=dict)
    completeness: dict[str, Any] = Field(default_factory=dict)
    eligibility: dict[str, Any] = Field(default_factory=dict)
    audit_context: dict[str, Any] = Field(default_factory=dict)
    created_by: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    deleted_by: str | None = None
    deleted_at: datetime | None = None
    delete_reason: str | None = None


class CandidateStrategyPackageRepository(Protocol):
    def create(self, record: CandidateStrategyPackageRecord) -> CandidateStrategyPackageRecord: ...

    def get(self, candidate_id: str) -> CandidateStrategyPackageRecord: ...

    def get_active_by_source(
        self,
        *,
        source_type: CandidateStrategyPackageSourceType,
        source_id: str,
    ) -> CandidateStrategyPackageRecord | None: ...

    def list(
        self,
        *,
        status: CandidateStrategyPackageStatus | None,
        limit: int,
    ) -> list[CandidateStrategyPackageRecord]: ...

    def soft_delete(
        self,
        *,
        candidate_id: str,
        deleted_by: str,
        delete_reason: str | None,
    ) -> CandidateStrategyPackageRecord: ...


class PostgresCandidateStrategyPackageRepository:
    """PostgreSQL repository for candidate snapshots; it never runs DDL."""

    def __init__(self, conn_factory: ConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def create(self, record: CandidateStrategyPackageRecord) -> CandidateStrategyPackageRecord:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.candidate_strategy_package (
                        candidate_id, candidate_version, source_type, source_id,
                        source_task_id, source_loop_id, source_experiment_id,
                        archive_run_id, display_name, status, snapshot_config_json,
                        factor_manifest_json, model_manifest_json, strategy_manifest_json,
                        metric_snapshot_json, artifact_refs_json, completeness_json,
                        eligibility_json, audit_json, created_by
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    RETURNING *
                    """,
                    self._insert_params(record),
                )
                row = cur.fetchone()
        return self._record_from_row(dict(row))

    def get(self, candidate_id: str) -> CandidateStrategyPackageRecord:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.candidate_strategy_package
                    WHERE candidate_id = %s
                    """,
                    (candidate_id,),
                )
                row = cur.fetchone()
        if not row:
            raise DataUnavailableError(
                "candidate strategy package does not exist",
                context={"candidate_id": candidate_id},
            )
        return self._record_from_row(dict(row))

    def get_active_by_source(
        self,
        *,
        source_type: CandidateStrategyPackageSourceType,
        source_id: str,
    ) -> CandidateStrategyPackageRecord | None:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM strategy_pkg.candidate_strategy_package
                    WHERE source_type = %s
                      AND source_id = %s
                      AND status = 'ACTIVE'
                    ORDER BY candidate_version DESC, created_at DESC
                    LIMIT 1
                    """,
                    (source_type.value, source_id),
                )
                row = cur.fetchone()
        return self._record_from_row(dict(row)) if row else None

    def list(
        self,
        *,
        status: CandidateStrategyPackageStatus | None,
        limit: int,
    ) -> list[CandidateStrategyPackageRecord]:
        params: list[Any] = []
        where = ""
        if status is not None:
            where = "WHERE status = %s"
            params.append(status.value)
        params.append(limit)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM strategy_pkg.candidate_strategy_package
                    {where}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        return [self._record_from_row(dict(row)) for row in rows]

    def soft_delete(
        self,
        *,
        candidate_id: str,
        deleted_by: str,
        delete_reason: str | None,
    ) -> CandidateStrategyPackageRecord:
        current = self.get(candidate_id)
        if current.status == CandidateStrategyPackageStatus.DELETED:
            return current
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE strategy_pkg.candidate_strategy_package
                    SET status = 'DELETED',
                        deleted_by = %s,
                        deleted_at = NOW(),
                        delete_reason = %s,
                        updated_at = NOW()
                    WHERE candidate_id = %s
                      AND status = 'ACTIVE'
                    RETURNING *
                    """,
                    (deleted_by, delete_reason, candidate_id),
                )
                row = cur.fetchone()
        if row is None:
            raise InvalidStateTransitionError(
                "candidate strategy package delete lost compare-and-set race",
                context={"candidate_id": candidate_id},
            )
        return self._record_from_row(dict(row))

    @staticmethod
    def _insert_params(record: CandidateStrategyPackageRecord) -> tuple[Any, ...]:
        return (
            record.candidate_id,
            record.candidate_version,
            record.source_type.value,
            record.source_id,
            record.source_task_id,
            record.source_loop_id,
            record.source_experiment_id,
            record.archive_run_id,
            record.display_name,
            record.status.value,
            psycopg2.extras.Json(record.snapshot_config),
            psycopg2.extras.Json(record.factor_manifest),
            psycopg2.extras.Json(record.model_manifest),
            psycopg2.extras.Json(record.strategy_manifest),
            psycopg2.extras.Json(record.metric_snapshot),
            psycopg2.extras.Json(record.artifact_refs),
            psycopg2.extras.Json(record.completeness),
            psycopg2.extras.Json(record.eligibility),
            psycopg2.extras.Json(record.audit_context),
            record.created_by,
        )

    @staticmethod
    def _record_from_row(row: dict[str, Any]) -> CandidateStrategyPackageRecord:
        return CandidateStrategyPackageRecord(
            candidate_id=row["candidate_id"],
            candidate_version=row["candidate_version"],
            source_type=CandidateStrategyPackageSourceType(row["source_type"]),
            source_id=row["source_id"],
            source_task_id=row.get("source_task_id"),
            source_loop_id=row.get("source_loop_id"),
            source_experiment_id=row.get("source_experiment_id"),
            archive_run_id=row.get("archive_run_id"),
            display_name=row["display_name"],
            status=CandidateStrategyPackageStatus(row["status"]),
            snapshot_config=row.get("snapshot_config_json") or {},
            factor_manifest=row.get("factor_manifest_json") or {},
            model_manifest=row.get("model_manifest_json") or {},
            strategy_manifest=row.get("strategy_manifest_json") or {},
            metric_snapshot=row.get("metric_snapshot_json") or {},
            artifact_refs=row.get("artifact_refs_json") or {},
            completeness=row.get("completeness_json") or {},
            eligibility=row.get("eligibility_json") or {},
            audit_context=row.get("audit_json") or {},
            created_by=row["created_by"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            deleted_by=row.get("deleted_by"),
            deleted_at=row.get("deleted_at"),
            delete_reason=row.get("delete_reason"),
        )


class InMemoryCandidateStrategyPackageRepository:
    """Unit-test repository preserving lifecycle semantics without a database."""

    def __init__(self) -> None:
        self.records: dict[str, CandidateStrategyPackageRecord] = {}

    def create(self, record: CandidateStrategyPackageRecord) -> CandidateStrategyPackageRecord:
        if record.candidate_id in self.records:
            raise InvalidStateTransitionError(
                "candidate strategy package id already exists",
                context={"candidate_id": record.candidate_id},
            )
        self.records[record.candidate_id] = record
        return record

    def get(self, candidate_id: str) -> CandidateStrategyPackageRecord:
        record = self.records.get(candidate_id)
        if record is None:
            raise DataUnavailableError(
                "candidate strategy package does not exist",
                context={"candidate_id": candidate_id},
            )
        return record

    def get_active_by_source(
        self,
        *,
        source_type: CandidateStrategyPackageSourceType,
        source_id: str,
    ) -> CandidateStrategyPackageRecord | None:
        matching = [
            record
            for record in self.records.values()
            if record.source_type == source_type
            and record.source_id == source_id
            and record.status == CandidateStrategyPackageStatus.ACTIVE
        ]
        if not matching:
            return None
        return sorted(matching, key=lambda item: (item.candidate_version, item.created_at), reverse=True)[0]

    def list(
        self,
        *,
        status: CandidateStrategyPackageStatus | None,
        limit: int,
    ) -> list[CandidateStrategyPackageRecord]:
        rows = [
            record
            for record in self.records.values()
            if status is None or record.status == status
        ]
        rows.sort(key=lambda item: item.created_at, reverse=True)
        return rows[:limit]

    def soft_delete(
        self,
        *,
        candidate_id: str,
        deleted_by: str,
        delete_reason: str | None,
    ) -> CandidateStrategyPackageRecord:
        current = self.get(candidate_id)
        if current.status == CandidateStrategyPackageStatus.DELETED:
            return current
        record = current.model_copy(
            update={
                "status": CandidateStrategyPackageStatus.DELETED,
                "deleted_by": deleted_by,
                "deleted_at": datetime.now(timezone.utc),
                "delete_reason": delete_reason,
                "updated_at": datetime.now(timezone.utc),
            }
        )
        self.records[candidate_id] = record
        return record


class CandidateStrategyPackageService:
    def __init__(
        self,
        repository: CandidateStrategyPackageRepository | None = None,
        resolver: QEExperimentSourceResolver | Any | None = None,
    ) -> None:
        self.repository = repository or PostgresCandidateStrategyPackageRepository()
        self.resolver = resolver or QEExperimentSourceResolver()

    def _snapshot_with_manifest(
        self,
        snapshot: dict[str, Any],
        manifest: StrategyPackageManifest,
    ) -> dict[str, Any]:
        manifest_json = manifest.model_dump(mode="json")
        failed_asset_checks = [check.model_dump(mode="json") for check in manifest.asset_checks if not check.passed]
        model_asset_json = (
            [asset.model_dump(mode="json") for asset in manifest.model_asset]
            if isinstance(manifest.model_asset, list)
            else manifest.model_asset.model_dump(mode="json")
        )
        snapshot_config = dict(snapshot.get("snapshot_config") or {})
        snapshot_config.update(
            {
                "strategy_package_manifest": manifest_json,
                "strategy_package_manifest_source": "QEExperimentSourceResolver",
                "strategy_package_manifest_sha256": manifest.manifest_sha256,
            }
        )

        factor_manifest = {
            **dict(snapshot.get("factor_manifest") or {}),
            "factor_set": [item.model_dump(mode="json") for item in manifest.factor_set],
            "factor_ids": [item.factor_id for item in manifest.factor_set],
        }
        model_manifest = {
            **dict(snapshot.get("model_manifest") or {}),
            "model_asset": model_asset_json,
        }
        strategy_manifest = {
            **dict(snapshot.get("strategy_manifest") or {}),
            "strategy_config": manifest.strategy_config,
            "execution_policy": manifest.execution_policy.model_dump(mode="json"),
            "minute_execution_policy": manifest.minute_execution_policy.model_dump(mode="json"),
            "portfolio_policy": manifest.portfolio_policy.model_dump(mode="json"),
            "universe_policy": manifest.universe_policy.model_dump(mode="json"),
        }
        metric_snapshot = {
            **dict(snapshot.get("metric_snapshot") or {}),
            "backtest_summary": manifest.backtest_summary.model_dump(mode="json"),
        }
        completeness = {
            **dict(snapshot.get("completeness") or {}),
            "strategy_package_manifest_available": True,
            "strategy_package_manifest_sha256": manifest.manifest_sha256,
            "failed_asset_checks": failed_asset_checks,
        }
        eligibility = {
            **dict(snapshot.get("eligibility") or {}),
            "can_create_strategy_package": not failed_asset_checks,
            "selection_or_paper_requires_package_validation": True,
        }
        audit_context = {
            **dict(snapshot.get("audit_context") or {}),
            "snapshot_assembler": "QEExperimentSourceResolver",
            "snapshot_assembler_status": "assembled",
            "snapshot_assembled_at": datetime.now(timezone.utc).isoformat(),
        }
        return {
            **snapshot,
            "snapshot_config": snapshot_config,
            "factor_manifest": factor_manifest,
            "model_manifest": model_manifest,
            "strategy_manifest": strategy_manifest,
            "metric_snapshot": metric_snapshot,
            "completeness": completeness,
            "eligibility": eligibility,
            "audit_context": audit_context,
        }

    def _snapshot_with_assembler_error(
        self,
        snapshot: dict[str, Any],
        exc: Exception,
    ) -> dict[str, Any]:
        completeness = {
            **dict(snapshot.get("completeness") or {}),
            "strategy_package_manifest_available": False,
            "snapshot_assembler_error": {
                "type": type(exc).__name__,
                "message": str(exc),
                "error_code": getattr(exc, "error_code", None),
            },
        }
        audit_context = {
            **dict(snapshot.get("audit_context") or {}),
            "snapshot_assembler": "QEExperimentSourceResolver",
            "snapshot_assembler_status": "failed_non_blocking",
        }
        return {
            **snapshot,
            "completeness": completeness,
            "audit_context": audit_context,
        }

    def _assemble_from_qe_experiment(self, experiment_id: str, snapshot: dict[str, Any]) -> dict[str, Any]:
        try:
            manifest = self.resolver.build_from_experiment(experiment_id)
        except Exception as exc:
            return self._snapshot_with_assembler_error(snapshot, exc)
        return self._snapshot_with_manifest(snapshot, manifest)

    def _assemble_from_qe_loop(
        self,
        *,
        task_id: str,
        loop_id: str,
        snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            manifest = self.resolver.build_from_evolution_loop(qe_task_id=task_id, qe_loop_id=loop_id)
        except Exception as exc:
            return self._snapshot_with_assembler_error(snapshot, exc)
        return self._snapshot_with_manifest(snapshot, manifest)

    def create_candidate(
        self,
        *,
        source_type: CandidateStrategyPackageSourceType,
        source_id: str,
        created_by: str,
        display_name: str | None = None,
        source_task_id: str | None = None,
        source_loop_id: str | None = None,
        source_experiment_id: str | None = None,
        archive_run_id: str | None = None,
        snapshot_config: dict[str, Any] | None = None,
        factor_manifest: dict[str, Any] | None = None,
        model_manifest: dict[str, Any] | None = None,
        strategy_manifest: dict[str, Any] | None = None,
        metric_snapshot: dict[str, Any] | None = None,
        artifact_refs: dict[str, Any] | None = None,
        completeness: dict[str, Any] | None = None,
        eligibility: dict[str, Any] | None = None,
        audit_context: dict[str, Any] | None = None,
        manual_action: bool = True,
    ) -> CandidateStrategyPackageRecord:
        if not manual_action:
            raise StrategyPackageValidationError(
                "candidate strategy package requires explicit user action",
                context={"source_type": source_type.value, "source_id": source_id},
            )
        source_id = source_id.strip()
        created_by = created_by.strip()
        if not source_id:
            raise StrategyPackageValidationError("source_id is required for candidate strategy package")
        if not created_by:
            raise StrategyPackageValidationError("created_by is required for candidate strategy package")
        existing = self.repository.get_active_by_source(source_type=source_type, source_id=source_id)
        if existing is not None:
            return existing
        record = CandidateStrategyPackageRecord(
            candidate_id=f"csp_{uuid4().hex}",
            source_type=source_type,
            source_id=source_id,
            source_task_id=source_task_id,
            source_loop_id=source_loop_id,
            source_experiment_id=source_experiment_id,
            archive_run_id=archive_run_id,
            display_name=(display_name or source_id).strip(),
            snapshot_config=snapshot_config or {},
            factor_manifest=factor_manifest or {},
            model_manifest=model_manifest or {},
            strategy_manifest=strategy_manifest or {},
            metric_snapshot=metric_snapshot or {},
            artifact_refs=artifact_refs or {},
            completeness=completeness or {},
            eligibility=eligibility or {},
            audit_context={
                **(audit_context or {}),
                "manual_action": True,
                "paper_enabled": False,
                "live_approved": False,
            },
            created_by=created_by,
        )
        return self.repository.create(record)

    def create_from_qe_experiment(
        self,
        *,
        experiment_id: str,
        created_by: str,
        display_name: str | None = None,
        **snapshot: Any,
    ) -> CandidateStrategyPackageRecord:
        snapshot = self._assemble_from_qe_experiment(experiment_id, dict(snapshot))
        return self.create_candidate(
            source_type=CandidateStrategyPackageSourceType.QE_EXPERIMENT,
            source_id=experiment_id,
            source_experiment_id=experiment_id,
            created_by=created_by,
            display_name=display_name,
            **snapshot,
        )

    def create_from_qe_loop(
        self,
        *,
        task_id: str,
        loop_id: str,
        created_by: str,
        display_name: str | None = None,
        experiment_id: str | None = None,
        **snapshot: Any,
    ) -> CandidateStrategyPackageRecord:
        if not task_id.strip() or not loop_id.strip():
            raise StrategyPackageValidationError("task_id and loop_id are required for QE loop candidate")
        source_id = loop_id if loop_id.startswith(f"{task_id}_") else f"{task_id}_{loop_id}"
        snapshot = self._assemble_from_qe_loop(task_id=task_id, loop_id=loop_id, snapshot=dict(snapshot))
        return self.create_candidate(
            source_type=CandidateStrategyPackageSourceType.QE_EVOLUTION_LOOP,
            source_id=source_id,
            source_task_id=task_id,
            source_loop_id=source_id,
            source_experiment_id=experiment_id,
            created_by=created_by,
            display_name=display_name,
            **snapshot,
        )

    def clone_candidate(
        self,
        *,
        source_candidate_id: str,
        created_by: str,
        display_name: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> CandidateStrategyPackageRecord:
        source = self.repository.get(source_candidate_id)
        if source.status != CandidateStrategyPackageStatus.ACTIVE:
            raise InvalidStateTransitionError(
                "only active candidate strategy packages can be cloned",
                context={"candidate_id": source_candidate_id, "status": source.status.value},
            )
        overrides = overrides or {}
        return self.create_candidate(
            source_type=CandidateStrategyPackageSourceType.CANDIDATE_STRATEGY_PACKAGE,
            source_id=source_candidate_id,
            created_by=created_by,
            display_name=display_name or f"{source.display_name} copy",
            archive_run_id=source.archive_run_id,
            snapshot_config=overrides.get("snapshot_config", source.snapshot_config),
            factor_manifest=overrides.get("factor_manifest", source.factor_manifest),
            model_manifest=overrides.get("model_manifest", source.model_manifest),
            strategy_manifest=overrides.get("strategy_manifest", source.strategy_manifest),
            metric_snapshot=overrides.get("metric_snapshot", source.metric_snapshot),
            artifact_refs=overrides.get("artifact_refs", source.artifact_refs),
            completeness=overrides.get("completeness", source.completeness),
            eligibility=overrides.get("eligibility", source.eligibility),
            audit_context={"cloned_from_candidate_id": source_candidate_id},
        )

    def get_candidate(self, candidate_id: str) -> CandidateStrategyPackageRecord:
        return self.repository.get(candidate_id)

    def list_candidates(
        self,
        *,
        status: CandidateStrategyPackageStatus | None = CandidateStrategyPackageStatus.ACTIVE,
        limit: int = 100,
    ) -> list[CandidateStrategyPackageRecord]:
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        return self.repository.list(status=status, limit=limit)

    def delete_candidate(
        self,
        *,
        candidate_id: str,
        deleted_by: str,
        delete_reason: str | None = None,
    ) -> CandidateStrategyPackageRecord:
        if not deleted_by.strip():
            raise StrategyPackageValidationError("deleted_by is required for candidate delete")
        return self.repository.soft_delete(
            candidate_id=candidate_id,
            deleted_by=deleted_by,
            delete_reason=delete_reason,
        )
