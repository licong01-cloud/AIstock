"""Strategy Package Center service layer."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import InvalidStateTransitionError, StrategyPackageValidationError

from .candidate import (
    CandidateStrategyPackageService,
    CandidateStrategyPackageStatus,
)
from .asset_eligibility import StrategyPackageAssetEligibilityService
from .execution_policy import (
    BACKTEST_SUCCESS_STATUSES,
    ExecutionPolicyValidationStatus,
    ValidatedExecutionPolicy,
    normalize_execution_policy_json,
)
from .manifest import freeze_manifest
from .metrics_summary import StrategyPackageMetricsSummary, metrics_summary_from_record
from .model_state import (
    ModelRetrainJobStatus,
    ModelRetrainPreview,
    ModelStalenessStatus,
    StrategyPackageModelRetrainJob,
    StrategyPackageModelState,
    evaluate_model_staleness,
)
from .models import LiveApprovalStatus, PackageStatus, SourceType, StrategyPackageLiveApproval, StrategyPackageManifest
from .package_asset import StrategyPackageAssetRecord, StrategyPackageAssetType
from .qe_source_resolver import QEExperimentSourceResolver
from .repository import PackageStatusEvent, StrategyPackageRecord, StrategyPackageRepository
from .runtime_variant import (
    RuntimeVariantKind,
    RuntimeVariantValidationStatus,
    StrategyPackageRuntimeVariant,
    build_runtime_variant,
    derive_locked_core_hash,
)
from .validation_run import (
    PackageValidationRetrainMode,
    PackageValidationReproducibility,
    PackageValidationStatus,
    PackageValidationType,
    StrategyPackageValidationRun,
    build_package_validation_run,
)
from .validation_stability import PackageValidationStabilitySummary, StabilityStatus, summarize_validation_stability
from .validators import StrategyPackageValidator


logger = logging.getLogger(__name__)


STATUS_TRANSITIONS: dict[PackageStatus, set[PackageStatus]] = {
    PackageStatus.ASSET_VALIDATED: {PackageStatus.DRAFT},
    PackageStatus.BACKTEST_APPROVED: {PackageStatus.DRAFT, PackageStatus.ASSET_VALIDATED},
    PackageStatus.RETIRED: {
        status for status in PackageStatus if status != PackageStatus.RETIRED
    },
}

LIVE_APPROVAL_STATUS_TRANSITIONS: dict[LiveApprovalStatus, set[LiveApprovalStatus]] = {
    LiveApprovalStatus.LIVE_APPROVAL_PENDING: {LiveApprovalStatus.LIVE_CANDIDATE},
    LiveApprovalStatus.LIVE_APPROVED: {LiveApprovalStatus.LIVE_APPROVAL_PENDING},
    LiveApprovalStatus.LIVE_REJECTED: {
        LiveApprovalStatus.LIVE_CANDIDATE,
        LiveApprovalStatus.LIVE_APPROVAL_PENDING,
    },
    LiveApprovalStatus.LIVE_RETIRED: {
        LiveApprovalStatus.LIVE_CANDIDATE,
        LiveApprovalStatus.LIVE_APPROVAL_PENDING,
        LiveApprovalStatus.LIVE_APPROVED,
        LiveApprovalStatus.LIVE_REJECTED,
    },
}

SIM_VALIDATION_SUCCESS_STATUSES = {"PASSED", "SUCCEEDED", "VERIFIED", "PASS", "SUCCESS"}
REQUIRED_LIVE_SIM_VALIDATION_KEYS = {"paper_v2", "miniqmt_sim"}
BROKER_COMPATIBILITY_SUCCESS_STATUSES = {"PASSED", "VERIFIED", "COMPATIBLE"}

LIVE_STRICT_GOVERNANCE_LIMIT = 10_000


def _is_deprecated_runtime_admission_status(status: PackageStatus) -> bool:
    core_statuses = {
        PackageStatus.DRAFT,
        PackageStatus.ASSET_VALIDATED,
        PackageStatus.BACKTEST_APPROVED,
        PackageStatus.RETIRED,
    }
    return status not in core_statuses


class StrategyPackageService:
    def __init__(
        self,
        *,
        repository: StrategyPackageRepository | Any | None = None,
        resolver: QEExperimentSourceResolver | None = None,
        validator: StrategyPackageValidator | None = None,
        candidate_service: CandidateStrategyPackageService | None = None,
        asset_eligibility: StrategyPackageAssetEligibilityService | None = None,
    ) -> None:
        self.repository = repository or StrategyPackageRepository()
        self.resolver = resolver or QEExperimentSourceResolver()
        self.validator = validator or StrategyPackageValidator()
        self.candidate_service = candidate_service or CandidateStrategyPackageService()
        self.asset_eligibility = asset_eligibility or StrategyPackageAssetEligibilityService(validator=self.validator)

    def create_from_qe_experiment(
        self,
        experiment_id: str,
        *,
        resolve_runtime_assets: bool = False,
    ) -> StrategyPackageRecord:
        manifest = self.resolver.build_from_experiment(
            experiment_id,
            resolve_runtime_assets=resolve_runtime_assets,
        )
        self.validator.validate_manifest(manifest)
        return self.repository.save_manifest(manifest)

    def create_from_qe_evolution_loop(
        self,
        *,
        qe_task_id: str,
        qe_loop_id: str,
        resolve_runtime_assets: bool = False,
    ) -> StrategyPackageRecord:
        manifest = self.resolver.build_from_evolution_loop(
            qe_task_id=qe_task_id,
            qe_loop_id=qe_loop_id,
            resolve_runtime_assets=resolve_runtime_assets,
        )
        self.validator.validate_manifest(manifest)
        return self.repository.save_manifest(manifest)

    def create_from_candidate(
        self,
        candidate_id: str,
        *,
        manifest_json: dict[str, Any] | None = None,
    ) -> StrategyPackageRecord:
        candidate = self.candidate_service.get_candidate(candidate_id)
        if candidate.status != CandidateStrategyPackageStatus.ACTIVE:
            raise StrategyPackageValidationError(
                "only active candidate strategy packages can create StrategyPackage",
                context={"candidate_id": candidate_id, "status": candidate.status.value},
            )
        payload = manifest_json or candidate.snapshot_config.get("strategy_package_manifest")
        if not isinstance(payload, dict) or not payload:
            raise StrategyPackageValidationError(
                "candidate strategy package requires a strategy_package_manifest snapshot",
                context={"candidate_id": candidate_id},
            )
        manifest = StrategyPackageManifest.model_validate(payload)
        source = manifest.source.model_copy(
            update={
                "source_type": SourceType.CANDIDATE_STRATEGY_PACKAGE,
                "source_id": candidate_id,
                "loop_id": None,
                "run_id": candidate.archive_run_id,
            }
        )
        manifest = freeze_manifest(manifest.model_copy(update={"source": source, "manifest_sha256": None}))
        self.validator.validate_manifest(manifest)
        return self.repository.save_manifest(manifest)

    def save_manifest(self, manifest: StrategyPackageManifest) -> StrategyPackageRecord:
        self.validator.validate_manifest(manifest)
        return self.repository.save_manifest(manifest)

    def list_packages(self, *, status: PackageStatus | None = None, limit: int = 100) -> list[StrategyPackageRecord]:
        return self.repository.list(status=status, limit=limit)

    def get_package(self, package_id: str) -> StrategyPackageRecord:
        return self.repository.get(package_id)

    def get_metrics_summary(self, package_id: str) -> StrategyPackageMetricsSummary:
        return metrics_summary_from_record(self.repository.get(package_id))

    def metrics_summary_for_record(self, record: StrategyPackageRecord) -> StrategyPackageMetricsSummary:
        return metrics_summary_from_record(record)

    def list_qe_packaging_sources(
        self,
        *,
        source_kind: str = "all",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        normalized_kind = str(source_kind or "all").strip().lower()
        if normalized_kind not in {"all", "qe_experiment", "qe_evolution_loop"}:
            raise StrategyPackageValidationError(
                "unsupported QE packaging source kind",
                context={"source_kind": source_kind, "supported": ["all", "qe_experiment", "qe_evolution_loop"]},
            )
        rows: list[dict[str, Any]] = []
        if normalized_kind in {"all", "qe_experiment"}:
            rows.extend(self._list_unpacked_qe_experiments(limit=limit))
        if normalized_kind in {"all", "qe_evolution_loop"}:
            rows.extend(self._list_unpacked_qe_evolution_loops(limit=limit))
        rows.sort(key=lambda item: item.get("completed_at") or item.get("created_at") or "", reverse=True)
        return rows[:limit]

    def validate_readiness(self, package_id: str) -> StrategyPackageManifest:
        record = self.repository.get(package_id)
        manifest = record.current_manifest()
        self.validator.validate_for_paper_trading(manifest)
        return manifest

    @staticmethod
    def _list_unpacked_qe_experiments(*, limit: int) -> list[dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT e.experiment_id, e.experiment_name, e.status, e.alpha_mode,
                           e.qe_task_id, e.qe_loop_id, e.loop_index, e.is_evolution_loop,
                           e.result_metrics, e.ic, e.rank_ic, e.annualized_return,
                           e.max_drawdown, e.information_ratio, e.created_at, e.completed_at
                    FROM qe_experiments e
                    WHERE e.status = 'completed'
                      AND COALESCE(e.is_evolution_loop, FALSE) = FALSE
                      AND e.result_metrics IS NOT NULL
                      AND e.result_metrics <> '{}'::jsonb
                      AND NOT EXISTS (
                          SELECT 1
                          FROM strategy_pkg.package p
                          WHERE (
                              p.source_type = 'qe_experiment'
                              AND p.source_id = e.experiment_id
                          ) OR (
                              p.source_type = 'qe_evolution_loop'
                              AND p.source_id = e.qe_task_id
                              AND COALESCE(p.loop_id, '') = COALESCE(e.qe_loop_id, '')
                          )
                      )
                    ORDER BY e.completed_at DESC NULLS LAST, e.created_at DESC NULLS LAST
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = [dict(row) for row in cur.fetchall()]
        return [
            StrategyPackageService._qe_source_payload(row, source_kind="qe_experiment")
            for row in rows
        ]

    @staticmethod
    def _list_unpacked_qe_evolution_loops(*, limit: int) -> list[dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT e.experiment_id, e.experiment_name, e.status, e.alpha_mode,
                           e.qe_task_id, e.qe_loop_id, e.loop_index, e.is_evolution_loop,
                           e.result_metrics, e.ic, e.rank_ic, e.annualized_return,
                           e.max_drawdown, e.information_ratio, e.created_at, e.completed_at
                    FROM qe_experiments e
                    WHERE e.status = 'completed'
                      AND COALESCE(e.is_evolution_loop, FALSE) = TRUE
                      AND e.qe_task_id IS NOT NULL
                      AND e.qe_loop_id IS NOT NULL
                      AND e.result_metrics IS NOT NULL
                      AND e.result_metrics <> '{}'::jsonb
                      AND NOT EXISTS (
                          SELECT 1
                          FROM strategy_pkg.package p
                          WHERE (
                              p.source_type = 'qe_evolution_loop'
                              AND p.source_id = e.qe_task_id
                              AND COALESCE(p.loop_id, '') = COALESCE(e.qe_loop_id, '')
                          ) OR (
                              p.source_type = 'qe_experiment'
                              AND p.source_id = e.experiment_id
                          )
                      )
                    ORDER BY e.completed_at DESC NULLS LAST, e.created_at DESC NULLS LAST
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = [dict(row) for row in cur.fetchall()]
        return [
            StrategyPackageService._qe_source_payload(row, source_kind="qe_evolution_loop")
            for row in rows
        ]

    @staticmethod
    def _qe_source_payload(row: dict[str, Any], *, source_kind: str) -> dict[str, Any]:
        metrics = StrategyPackageService._parse_jsonish(row.get("result_metrics")) or {}
        metrics_summary = StrategyPackageService._qe_metrics_summary(row, metrics)
        experiment_name = str(row.get("experiment_name") or row.get("experiment_id") or "").strip()
        qe_task_id = str(row.get("qe_task_id") or "").strip() or None
        qe_loop_id = str(row.get("qe_loop_id") or "").strip() or None
        suffix = ""
        if source_kind == "qe_evolution_loop":
            suffix = f" / {qe_task_id or '-'} / {qe_loop_id or '-'}"
        display_name = (
            f"{experiment_name}{suffix} | 年化 {StrategyPackageService._fmt_pct(metrics_summary.get('annual_return'))} "
            f"| IC {StrategyPackageService._fmt_pct(metrics_summary.get('ic'))} "
            f"| 回撤 {StrategyPackageService._fmt_pct(metrics_summary.get('max_drawdown'))}"
        )
        return {
            "source_kind": source_kind,
            "experiment_id": row.get("experiment_id"),
            "experiment_name": experiment_name,
            "qe_task_id": qe_task_id,
            "qe_loop_id": qe_loop_id,
            "loop_index": row.get("loop_index"),
            "alpha_mode": row.get("alpha_mode"),
            "display_name": display_name,
            "metrics_summary": metrics_summary,
            "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
            "completed_at": row.get("completed_at").isoformat() if row.get("completed_at") else None,
        }

    @staticmethod
    def _parse_jsonish(value: Any) -> Any:
        if value is None or isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            if not value.strip():
                return None
            try:
                return json.loads(value)
            except json.JSONDecodeError as exc:
                logger.warning(
                    "Failed to parse JSON-like strategy package value: %s; value_snippet=%r",
                    exc,
                    value[:200],
                )
                return None
        return value

    @staticmethod
    def _qe_metric_float(row: dict[str, Any], metrics: dict[str, Any], *keys: str) -> float | None:
        for key in keys:
            value = row.get(key)
            if value is None:
                value = metrics.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _qe_metrics_summary(row: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        return {
            "ic": StrategyPackageService._qe_metric_float(row, metrics, "ic", "IC"),
            "rank_ic": StrategyPackageService._qe_metric_float(row, metrics, "rank_ic", "Rank IC", "Rank_IC"),
            "annual_return": StrategyPackageService._qe_metric_float(
                row,
                metrics,
                "annualized_return",
                "annual_return",
                "1day.excess_return_with_cost.annualized_return",
                "cagr",
            ),
            "max_drawdown": StrategyPackageService._qe_metric_float(
                row,
                metrics,
                "max_drawdown",
                "1day.excess_return_with_cost.max_drawdown",
            ),
            "sharpe": StrategyPackageService._qe_metric_float(row, metrics, "sharpe", "information_ratio", "Information Ratio"),
        }

    @staticmethod
    def _fmt_pct(value: Any) -> str:
        if value is None:
            return "-"
        try:
            return f"{float(value) * 100:.2f}%"
        except (TypeError, ValueError):
            return "-"

    def transition_status(
        self,
        *,
        package_id: str,
        to_status: PackageStatus,
        reason: str,
        context: dict[str, Any] | None = None,
    ) -> StrategyPackageRecord:
        if _is_deprecated_runtime_admission_status(to_status):
            record = self.repository.get(package_id)
            self.asset_eligibility.require_eligible(record)
            return record
        allowed = STATUS_TRANSITIONS.get(to_status)
        if not allowed:
            raise StrategyPackageValidationError(
                "unsupported strategy package target status",
                context={"package_id": package_id, "to_status": to_status.value},
            )
        return self.repository.transition_status(
            package_id=package_id,
            to_status=to_status,
            allowed_from=allowed,
            reason=reason,
            context=context or {},
        )

    def enable_selection(self, package_id: str) -> StrategyPackageRecord:
        record = self.repository.get(package_id)
        self.asset_eligibility.require_eligible(record)
        return record

    def enable_paper(self, package_id: str) -> StrategyPackageRecord:
        record = self.repository.get(package_id)
        self.asset_eligibility.require_eligible(record)
        return record

    def retire(self, package_id: str, *, reason: str = "retire_package") -> StrategyPackageRecord:
        return self.transition_status(
            package_id=package_id,
            to_status=PackageStatus.RETIRED,
            reason=reason,
        )

    def package_delete_dependencies(self, package_id: str) -> dict[str, Any]:
        return self.repository.package_delete_dependencies(package_id)

    def delete_package(self, package_id: str) -> dict[str, Any]:
        return self.repository.delete_package(package_id)

    def validate_manifest_integrity(self, *, limit: int = 500) -> dict[str, Any]:
        return self.repository.validate_manifest_integrity(limit=limit)

    def repair_manifest_hash(
        self,
        package_id: str,
        *,
        operator: str = "paper_v2_gate_decoupling",
        confirm_stored_sha256: str | None = None,
        confirm_computed_sha256: str | None = None,
    ) -> StrategyPackageRecord:
        return self.repository.repair_manifest_hash(
            package_id,
            operator=operator,
            confirm_stored_sha256=confirm_stored_sha256,
            confirm_computed_sha256=confirm_computed_sha256,
        )

    def list_status_events(self, package_id: str, *, limit: int = 200) -> list[PackageStatusEvent]:
        return self.repository.list_status_events(package_id, limit=limit)

    def record_package_asset(
        self,
        package_id: str,
        *,
        asset_type: StrategyPackageAssetType,
        asset_ref: str,
        asset_sha256: str | None = None,
        metadata: dict[str, Any] | None = None,
        asset_role: str = "governed_asset",
        asset_size_bytes: int | None = None,
        protected_asset: bool = True,
        source_uri: str | None = None,
    ) -> StrategyPackageAssetRecord:
        asset = StrategyPackageAssetRecord(
            package_id=package_id,
            asset_type=asset_type,
            asset_ref=asset_ref,
            asset_sha256=asset_sha256,
            metadata=metadata or {},
            asset_role=asset_role,
            asset_size_bytes=asset_size_bytes,
            protected_asset=protected_asset,
            source_uri=source_uri,
        )
        return self.repository.save_package_asset(asset)

    def list_package_assets(self, package_id: str, *, protected_only: bool = False) -> list[StrategyPackageAssetRecord]:
        return self.repository.list_package_assets(package_id, protected_only=protected_only)

    def create_execution_policy(
        self,
        *,
        package_id: str,
        policy_name: str,
        policy_json: dict[str, Any],
        source_backtest_id: str,
        source_backtest_status: str,
        paper_enabled: bool = False,
    ) -> ValidatedExecutionPolicy:
        _ = paper_enabled  # legacy request field; Paper readiness is decided by asset eligibility and runtime checks.
        record = self.repository.get(package_id)
        normalized = normalize_execution_policy_json(policy_json)
        normalized_status = source_backtest_status.strip().upper()
        if normalized_status not in BACKTEST_SUCCESS_STATUSES:
            raise StrategyPackageValidationError(
                "execution policy source backtest must have explicit successful evidence",
                context={
                    "package_id": package_id,
                    "source_backtest_id": source_backtest_id,
                    "source_backtest_status": source_backtest_status,
                    "allowed_source_backtest_statuses": sorted(BACKTEST_SUCCESS_STATUSES),
                },
            )
        policy = ValidatedExecutionPolicy(
            package_id=package_id,
            manifest_sha256=record.manifest_sha256,
            policy_name=policy_name,
            policy_json=normalized,
            source_backtest_id=source_backtest_id,
            source_backtest_status=normalized_status,
            validation_status=ExecutionPolicyValidationStatus.BACKTEST_VALIDATED,
            paper_enabled=False,
        )
        return self.repository.save_execution_policy(policy)

    def list_execution_policies(self, package_id: str) -> list[ValidatedExecutionPolicy]:
        return self.repository.list_execution_policies(package_id)

    def get_execution_policy(self, package_id: str, policy_id: str) -> ValidatedExecutionPolicy:
        return self.repository.get_execution_policy(package_id, policy_id)

    def enable_execution_policy_for_paper(self, package_id: str, policy_id: str) -> ValidatedExecutionPolicy:
        self.asset_eligibility.require_eligible(self.repository.get(package_id))
        return self.repository.get_execution_policy(package_id, policy_id)

    def disable_execution_policy_for_paper(self, package_id: str, policy_id: str) -> ValidatedExecutionPolicy:
        self.repository.get(package_id)
        return self.repository.get_execution_policy(package_id, policy_id)

    def get_model_state(self, package_id: str, *, as_of_date: date | None = None) -> StrategyPackageModelState:
        record = self.repository.get(package_id)
        state = self.repository.get_model_state(package_id)
        if state is None:
            state = self._initial_model_state(record)
        evaluated = evaluate_model_staleness(state, as_of_date=as_of_date or date.today())
        return self.repository.upsert_model_state(evaluated)

    def upsert_model_state(self, state: StrategyPackageModelState, *, as_of_date: date | None = None) -> StrategyPackageModelState:
        self.repository.get(state.package_id)
        evaluated = evaluate_model_staleness(state, as_of_date=as_of_date or date.today())
        return self.repository.upsert_model_state(evaluated)

    def preview_model_retrain(
        self,
        package_id: str,
        *,
        as_of_date: date | None = None,
        lookback_days: int = 756,
    ) -> ModelRetrainPreview:
        if lookback_days <= 0:
            raise StrategyPackageValidationError("lookback_days must be positive")
        state = self.get_model_state(package_id, as_of_date=as_of_date)
        end_date = as_of_date or date.today()
        start_date = end_date - timedelta(days=lookback_days)
        return ModelRetrainPreview(
            package_id=package_id,
            job_type="rolling_retrain",
            recommended_train_start_date=start_date,
            recommended_train_end_date=end_date,
            stale_after_days=state.stale_after_days,
            requires_manual_confirmation=True,
            reason=state.warning or "manual retrain preview requested",
            config={
                "lookback_days": lookback_days,
                "active_model_version_id": state.active_model_version_id,
                "current_train_start_date": state.train_start_date.isoformat() if state.train_start_date else None,
                "current_train_end_date": state.train_end_date.isoformat() if state.train_end_date else None,
            },
        )

    def start_model_retrain(
        self,
        package_id: str,
        *,
        as_of_date: date | None = None,
        lookback_days: int = 756,
        job_type: str = "rolling_retrain",
        config: dict[str, Any] | None = None,
        confirm_retrain: bool = False,
    ) -> StrategyPackageModelRetrainJob:
        if not confirm_retrain:
            raise StrategyPackageValidationError(
                "model retrain start requires explicit confirmation",
                context={
                    "package_id": package_id,
                    "confirm_retrain": confirm_retrain,
                    "confirmation_mode": "dialog_boolean",
                },
            )
        preview = self.preview_model_retrain(
            package_id,
            as_of_date=as_of_date,
            lookback_days=lookback_days,
        )
        job_config = {
            **preview.config,
            **(config or {}),
            "preview": preview.model_dump(mode="json"),
            "executor_contract": "manual_or_external_worker_required",
        }
        job = StrategyPackageModelRetrainJob(
            package_id=package_id,
            job_type=job_type or preview.job_type,
            requested_train_start_date=preview.recommended_train_start_date,
            requested_train_end_date=preview.recommended_train_end_date,
            stale_after_days=preview.stale_after_days,
            config=job_config,
            status=ModelRetrainJobStatus.QUEUED,
            requires_manual_confirmation=True,
            confirmed=True,
            status_reason="model retrain job queued; training executor is not run inside this API request",
        )
        saved = self.repository.save_model_retrain_job(job)
        state = self.get_model_state(package_id, as_of_date=as_of_date)
        metadata = dict(state.metadata or {})
        metadata["active_retrain_job"] = {
            "job_id": saved.job_id,
            "job_type": saved.job_type,
            "status": saved.status.value,
            "requested_train_start_date": saved.requested_train_start_date.isoformat()
            if saved.requested_train_start_date
            else None,
            "requested_train_end_date": saved.requested_train_end_date.isoformat(),
        }
        self.repository.upsert_model_state(
            state.model_copy(
                update={
                    "last_retrain_job_id": saved.job_id,
                    "staleness_status": ModelStalenessStatus.RETRAINING,
                    "warning": "model retrain job is queued; do not mark the model current until training succeeds",
                    "metadata": metadata,
                }
            )
        )
        return saved

    def list_model_retrain_jobs(self, package_id: str, *, limit: int = 100) -> list[StrategyPackageModelRetrainJob]:
        self.repository.get(package_id)
        return self.repository.list_model_retrain_jobs(package_id, limit=limit)

    def create_runtime_variant(
        self,
        package_id: str,
        *,
        variant_name: str,
        variant_kind: RuntimeVariantKind,
        variant_config: dict[str, Any],
        validation_status: RuntimeVariantValidationStatus = RuntimeVariantValidationStatus.DRAFT,
        paper_candidate: bool = False,
        validation_evidence: dict[str, Any] | None = None,
        created_by: str = "aistock_api",
    ) -> StrategyPackageRuntimeVariant:
        record = self.repository.get(package_id)
        variant = build_runtime_variant(
            record.current_manifest(),
            variant_name=variant_name,
            variant_kind=variant_kind,
            variant_config=variant_config,
            validation_status=validation_status,
            paper_candidate=paper_candidate,
            validation_evidence=validation_evidence,
            created_by=created_by,
        )
        return self.repository.save_runtime_variant(variant)

    def list_runtime_variants(
        self,
        package_id: str,
        *,
        include_retired: bool = False,
        limit: int = 100,
    ) -> list[StrategyPackageRuntimeVariant]:
        return self.repository.list_runtime_variants(
            package_id,
            include_retired=include_retired,
            limit=limit,
        )

    def mark_runtime_variant_validation(
        self,
        package_id: str,
        variant_id: str,
        *,
        validation_status: RuntimeVariantValidationStatus,
        paper_candidate: bool = False,
        validation_evidence: dict[str, Any] | None = None,
    ) -> StrategyPackageRuntimeVariant:
        return self.repository.set_runtime_variant_validation(
            package_id=package_id,
            variant_id=variant_id,
            validation_status=validation_status,
            paper_candidate=paper_candidate,
            validation_evidence=validation_evidence or {},
        )

    def create_validation_run(
        self,
        package_id: str,
        *,
        validation_type: PackageValidationType,
        retrain_mode: PackageValidationRetrainMode,
        runtime_variant_id: str | None = None,
        model_version_id: str | None = None,
        seed_policy: str | None = None,
        random_seed: int | None = None,
        source_data_version: str | None = None,
        target_data_version: str | None = None,
        backtest_start: date | None = None,
        backtest_end: date | None = None,
        status: PackageValidationStatus = PackageValidationStatus.REQUESTED,
        metrics_json: dict[str, Any] | None = None,
        artifact_manifest_json: dict[str, Any] | None = None,
        evidence_json: dict[str, Any] | None = None,
        reproducibility_level: PackageValidationReproducibility = PackageValidationReproducibility.UNKNOWN,
        created_by: str = "aistock_api",
        completed_at: datetime | None = None,
    ) -> StrategyPackageValidationRun:
        record = self.repository.get(package_id)
        runtime_variant_hash = None
        if runtime_variant_id is not None:
            variant = self.repository.get_runtime_variant(package_id, runtime_variant_id)
            runtime_variant_hash = variant.variant_hash
        run = build_package_validation_run(
            record.current_manifest(),
            validation_type=validation_type,
            retrain_mode=retrain_mode,
            runtime_variant_id=runtime_variant_id,
            runtime_variant_hash=runtime_variant_hash,
            model_version_id=model_version_id,
            seed_policy=seed_policy,
            random_seed=random_seed,
            source_data_version=source_data_version,
            target_data_version=target_data_version,
            backtest_start=backtest_start,
            backtest_end=backtest_end,
            status=status,
            metrics_json=metrics_json,
            artifact_manifest_json=artifact_manifest_json,
            evidence_json=evidence_json,
            reproducibility_level=reproducibility_level,
            created_by=created_by,
            completed_at=completed_at,
        )
        return self.repository.save_validation_run(run)

    def get_validation_run(self, package_id: str, validation_run_id: str) -> StrategyPackageValidationRun:
        return self.repository.get_validation_run(package_id, validation_run_id)

    def list_validation_runs(
        self,
        package_id: str,
        *,
        validation_type: PackageValidationType | None = None,
        runtime_variant_id: str | None = None,
        limit: int = 100,
    ) -> list[StrategyPackageValidationRun]:
        return self.repository.list_validation_runs(
            package_id,
            validation_type=validation_type,
            runtime_variant_id=runtime_variant_id,
            limit=limit,
        )

    def create_live_approval_candidate(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        alpha_core_sha256: str,
        runtime_release_id: str,
        runtime_release_sha256: str,
        runtime_profile_id: str,
        runtime_profile_version_id: str,
        runtime_profile_sha256: str,
        execution_policy_id: str,
        execution_policy_sha256: str,
        tail_policy_id: str,
        tail_policy_sha256: str,
        target_broker_backend: str,
        sim_validation_evidence: dict[str, Any],
        broker_compatibility: dict[str, Any],
        portfolio_id: str | None = None,
        broker_account_id: str | None = None,
        risk_note: str | None = None,
        rollback_plan: str | None = None,
        requested_by: str | None = None,
        audit_json: dict[str, Any] | None = None,
    ) -> StrategyPackageLiveApproval:
        record = self.repository.get(package_id)
        expected_alpha_core = derive_locked_core_hash(record.current_manifest())
        self._require_live_identity_match(
            record=record,
            manifest_sha256=manifest_sha256,
            alpha_core_sha256=alpha_core_sha256,
            expected_alpha_core=expected_alpha_core,
        )
        approval = StrategyPackageLiveApproval(
            package_id=package_id,
            manifest_sha256=manifest_sha256,
            alpha_core_sha256=alpha_core_sha256,
            portfolio_id=portfolio_id,
            runtime_release_id=runtime_release_id,
            runtime_release_sha256=runtime_release_sha256,
            runtime_profile_id=runtime_profile_id,
            runtime_profile_version_id=runtime_profile_version_id,
            runtime_profile_sha256=runtime_profile_sha256,
            execution_policy_id=execution_policy_id,
            execution_policy_sha256=execution_policy_sha256,
            tail_policy_id=tail_policy_id,
            tail_policy_sha256=tail_policy_sha256,
            target_broker_backend=target_broker_backend,
            broker_account_id=broker_account_id,
            sim_validation_evidence=sim_validation_evidence,
            broker_compatibility=broker_compatibility,
            risk_note=risk_note,
            rollback_plan=rollback_plan,
            requested_by=requested_by,
            audit_json={
                "created_by": requested_by,
                "approval_lifecycle": "candidate_created",
                **(audit_json or {}),
            },
        )
        self._validate_live_approval_evidence(approval)
        return self.repository.save_live_approval(approval)

    def get_live_approval(self, package_id: str, approval_id: str) -> StrategyPackageLiveApproval:
        return self.repository.get_live_approval(package_id, approval_id)

    def list_live_approvals(
        self,
        *,
        package_id: str | None = None,
        portfolio_id: str | None = None,
        status: LiveApprovalStatus | None = None,
        limit: int = 100,
    ) -> list[StrategyPackageLiveApproval]:
        return self.repository.list_live_approvals(
            package_id=package_id,
            portfolio_id=portfolio_id,
            status=status,
            limit=limit,
        )

    def submit_live_approval(
        self,
        *,
        package_id: str,
        approval_id: str,
        requested_by: str,
        risk_note: str,
        rollback_plan: str,
    ) -> StrategyPackageLiveApproval:
        approval = self.repository.get_live_approval(package_id, approval_id)
        return self._transition_live_approval(
            approval,
            to_status=LiveApprovalStatus.LIVE_APPROVAL_PENDING,
            updates={
                "requested_by": requested_by,
                "requested_at": datetime.now(timezone.utc),
                "risk_note": risk_note,
                "rollback_plan": rollback_plan,
            },
            action="submit_live_approval",
        )

    def approve_live_approval(
        self,
        *,
        package_id: str,
        approval_id: str,
        approved_by: str,
        risk_note: str | None = None,
        rollback_plan: str | None = None,
    ) -> StrategyPackageLiveApproval:
        approval = self.repository.get_live_approval(package_id, approval_id)
        return self._transition_live_approval(
            approval,
            to_status=LiveApprovalStatus.LIVE_APPROVED,
            updates={
                "approved_by": approved_by,
                "approved_at": datetime.now(timezone.utc),
                "risk_note": risk_note or approval.risk_note,
                "rollback_plan": rollback_plan or approval.rollback_plan,
            },
            action="approve_live_approval",
        )

    def reject_live_approval(
        self,
        *,
        package_id: str,
        approval_id: str,
        rejected_by: str,
        rejection_reason: str,
    ) -> StrategyPackageLiveApproval:
        approval = self.repository.get_live_approval(package_id, approval_id)
        return self._transition_live_approval(
            approval,
            to_status=LiveApprovalStatus.LIVE_REJECTED,
            updates={
                "rejected_by": rejected_by,
                "rejected_at": datetime.now(timezone.utc),
                "rejection_reason": rejection_reason,
            },
            action="reject_live_approval",
        )

    def retire_live_approval(
        self,
        *,
        package_id: str,
        approval_id: str,
        retired_by: str,
        retirement_reason: str,
    ) -> StrategyPackageLiveApproval:
        approval = self.repository.get_live_approval(package_id, approval_id)
        return self._transition_live_approval(
            approval,
            to_status=LiveApprovalStatus.LIVE_RETIRED,
            updates={
                "retired_by": retired_by,
                "retired_at": datetime.now(timezone.utc),
                "retirement_reason": retirement_reason,
            },
            action="retire_live_approval",
        )

    def require_live_approval(
        self,
        *,
        package_id: str,
        approval_id: str,
        manifest_sha256: str | None = None,
        runtime_release_sha256: str | None = None,
        target_broker_backend: str | None = None,
    ) -> StrategyPackageLiveApproval:
        approval = self.repository.get_live_approval(package_id, approval_id)
        blockers: list[str] = []
        if approval.approval_status != LiveApprovalStatus.LIVE_APPROVED:
            blockers.append(f"approval_status={approval.approval_status.value}")
        if manifest_sha256 is not None and approval.manifest_sha256 != manifest_sha256:
            blockers.append("manifest_sha256_mismatch")
        if runtime_release_sha256 is not None and approval.runtime_release_sha256 != runtime_release_sha256:
            blockers.append("runtime_release_sha256_mismatch")
        if target_broker_backend is not None and approval.target_broker_backend != target_broker_backend:
            blockers.append("target_broker_backend_mismatch")
        try:
            self._validate_live_approval_evidence(approval)
        except StrategyPackageValidationError as exc:
            blockers.append("approval_evidence_invalid")
            evidence_context = exc.context
        else:
            evidence_context = {}
        if blockers:
            raise StrategyPackageValidationError(
                "live execution requires an approved StrategyPackage live approval record",
                context={
                    "package_id": package_id,
                    "approval_id": approval_id,
                    "blockers": blockers,
                    "approval_status": approval.approval_status.value,
                    "runtime_release_sha256": approval.runtime_release_sha256,
                    **evidence_context,
                },
            )
        return approval

    def summarize_validation_stability(
        self,
        package_id: str,
        *,
        metric_key: str = "annual_return",
        limit: int = 500,
    ) -> PackageValidationStabilitySummary:
        runs = self.repository.list_validation_runs(package_id, limit=limit)
        return summarize_validation_stability(package_id, runs, metric_key=metric_key)

    def governance_eligibility(
        self,
        package_id: str,
        *,
        metric_key: str = "annual_return",
        limit: int = 500,
    ) -> dict[str, Any]:
        if limit <= 0:
            raise StrategyPackageValidationError("limit must be positive")
        record = self.repository.get(package_id)
        manifest = record.current_manifest()
        manifest_identity = self._manifest_identity_gate(record, manifest)
        original_retest = self._original_fixed_weight_retest_gate(record)
        validation_stability = self._validation_stability_gate(
            self.summarize_validation_stability(package_id, metric_key=metric_key, limit=limit),
        )
        protected_assets = self._protected_asset_gate(package_id)
        runtime_variants = self._runtime_variant_candidate_gate(package_id)

        gates = [
            ("manifest_identity", manifest_identity),
            ("original_fixed_weight_retest", original_retest),
            ("validation_stability", validation_stability),
            ("protected_assets", protected_assets),
            ("runtime_variant_candidate", runtime_variants),
        ]
        satisfied_gates: list[str] = []
        blockers: list[str] = []
        for gate_name, gate in gates:
            if gate["passed"]:
                satisfied_gates.append(gate_name)
                continue
            for blocker in gate["blockers"]:
                if blocker not in blockers:
                    blockers.append(blocker)

        return {
            "package_id": package_id,
            "manifest_sha256": record.manifest_sha256,
            "package_status": record.package_status.value,
            "live_strict_ready": not blockers,
            "readiness_semantics": "live_strict_governance_readiness",
            "does_not_block_paper_simulation": True,
            "blockers": blockers,
            "satisfied_gates": satisfied_gates,
            "manifest_identity": manifest_identity,
            "original_fixed_weight_retest": original_retest,
            "validation_stability": validation_stability,
            "protected_asset_status": protected_assets,
            "runtime_variant_candidate_status": runtime_variants,
        }

    def paper_simulation_admission(
        self,
        package_id: str,
        *,
        metric_key: str = "annual_return",
        governance_limit: int = LIVE_STRICT_GOVERNANCE_LIMIT,
    ) -> dict[str, Any]:
        """Return the Paper v2 simulation admission summary for a package.

        This is deliberately narrower than ``governance_eligibility``. Paper v2
        simulation may run a backtest-approved QE alpha core even when live-like
        governance evidence is incomplete. Runtime failures are reported by the
        concrete Selection/Paper preflight instead of invalidating the package.
        """

        record = self.repository.get(package_id)
        asset_eligibility = self.asset_eligibility.summarize(record)
        warnings: list[str] = []
        governance: dict[str, Any] | None = None
        if governance_limit > 0:
            governance = self.governance_eligibility(package_id, metric_key=metric_key, limit=governance_limit)
            for blocker in governance.get("blockers") or []:
                warning = f"live_strict_governance:{blocker}"
                if warning not in warnings:
                    warnings.append(warning)

        return {
            "package_id": package_id,
            "manifest_sha256": record.manifest_sha256,
            "package_status": record.package_status.value,
            "asset_eligible": asset_eligibility.eligible,
            "paper_simulation_allowed": asset_eligibility.eligible,
            "blockers": list(asset_eligibility.blockers),
            "warnings": warnings,
            "runtime_preflight_required": True,
            "live_strict_governance": governance,
            "asset_eligibility": asset_eligibility.to_dict(),
            "admission_policy": {
                "name": "strategy_package_asset_eligibility_v1",
                "non_blocking_governance": [
                    "original_fixed_weight_retest",
                    "seed_stability",
                    "regime_stability",
                    "protected_asset_ledger",
                    "runtime_variant_candidate",
                    "rolling_retrain_evidence",
                ],
            },
        }

    def _require_paper_simulation_admission(self, record: StrategyPackageRecord) -> None:
        self.asset_eligibility.require_eligible(record)

    @staticmethod
    def _require_live_identity_match(
        *,
        record: StrategyPackageRecord,
        manifest_sha256: str,
        alpha_core_sha256: str,
        expected_alpha_core: str,
    ) -> None:
        blockers: list[str] = []
        if record.manifest_sha256 != manifest_sha256:
            blockers.append("manifest_sha256_mismatch")
        if expected_alpha_core != alpha_core_sha256:
            blockers.append("alpha_core_sha256_mismatch")
        if blockers:
            raise StrategyPackageValidationError(
                "live approval identity must match immutable StrategyPackage alpha core",
                context={
                    "package_id": record.package_id,
                    "blockers": blockers,
                    "current_manifest_sha256": record.manifest_sha256,
                    "provided_manifest_sha256": manifest_sha256,
                    "expected_alpha_core_sha256": expected_alpha_core,
                    "provided_alpha_core_sha256": alpha_core_sha256,
                },
            )

    @staticmethod
    def _validate_live_approval_evidence(approval: StrategyPackageLiveApproval) -> None:
        missing_sim_keys = sorted(REQUIRED_LIVE_SIM_VALIDATION_KEYS.difference(approval.sim_validation_evidence))
        failing_sim_keys: list[str] = []
        for key in sorted(REQUIRED_LIVE_SIM_VALIDATION_KEYS.intersection(approval.sim_validation_evidence)):
            value = approval.sim_validation_evidence.get(key)
            status = ""
            if isinstance(value, dict):
                status = str(value.get("status") or value.get("validation_status") or "").strip().upper()
            else:
                status = str(value or "").strip().upper()
            if status not in SIM_VALIDATION_SUCCESS_STATUSES:
                failing_sim_keys.append(key)
        broker_status = str(
            approval.broker_compatibility.get("status")
            or approval.broker_compatibility.get("compatibility_status")
            or ""
        ).strip().upper()
        broker_backend = str(
            approval.broker_compatibility.get("target_broker_backend")
            or approval.broker_compatibility.get("broker_backend")
            or ""
        ).strip()
        blockers: list[str] = []
        if missing_sim_keys:
            blockers.append("missing_sim_validation_evidence")
        if failing_sim_keys:
            blockers.append("failed_sim_validation_evidence")
        if broker_status not in BROKER_COMPATIBILITY_SUCCESS_STATUSES:
            blockers.append("broker_compatibility_not_verified")
        if broker_backend != approval.target_broker_backend:
            blockers.append("broker_backend_mismatch")
        if blockers:
            raise StrategyPackageValidationError(
                "live approval requires Paper/MiniQMT SIM validation evidence and broker compatibility verification",
                context={
                    "package_id": approval.package_id,
                    "approval_id": approval.approval_id,
                    "blockers": blockers,
                    "missing_sim_validation_keys": missing_sim_keys,
                    "failing_sim_validation_keys": failing_sim_keys,
                    "broker_status": broker_status,
                    "broker_backend": broker_backend,
                    "target_broker_backend": approval.target_broker_backend,
                },
            )

    def _transition_live_approval(
        self,
        approval: StrategyPackageLiveApproval,
        *,
        to_status: LiveApprovalStatus,
        updates: dict[str, Any],
        action: str,
    ) -> StrategyPackageLiveApproval:
        allowed_from = LIVE_APPROVAL_STATUS_TRANSITIONS.get(to_status, set())
        if approval.approval_status not in allowed_from:
            raise InvalidStateTransitionError(
                "invalid live approval lifecycle transition",
                context={
                    "package_id": approval.package_id,
                    "approval_id": approval.approval_id,
                    "from_status": approval.approval_status.value,
                    "to_status": to_status.value,
                    "allowed_from": sorted(item.value for item in allowed_from),
                },
            )
        self._validate_live_approval_evidence(approval)
        now = datetime.now(timezone.utc)
        audit_json = dict(approval.audit_json or {})
        events = list(audit_json.get("events") or [])
        events.append(
            {
                "action": action,
                "from_status": approval.approval_status.value,
                "to_status": to_status.value,
                "created_at": now.isoformat(),
            }
        )
        audit_json["events"] = events
        updated = approval.model_copy(
            update={
                **updates,
                "approval_status": to_status,
                "audit_json": audit_json,
                "updated_at": now,
            }
        )
        updated = StrategyPackageLiveApproval.model_validate(updated.model_dump())
        return self.repository.update_live_approval(updated)

    def _manifest_identity_gate(self, record: StrategyPackageRecord, manifest: StrategyPackageManifest) -> dict[str, Any]:
        blockers: list[str] = []
        if record.package_status == PackageStatus.DRAFT:
            blockers.append("package_status=DRAFT")
        try:
            self.validator.validate_manifest(manifest)
        except StrategyPackageValidationError as exc:
            blockers.append(str(exc))
        passed = not blockers
        return {
            "passed": passed,
            "status": "READY" if passed else "BLOCKED",
            "package_status": record.package_status.value,
            "manifest_sha256": record.manifest_sha256,
            "blockers": blockers,
        }

    def _original_fixed_weight_retest_gate(self, record: StrategyPackageRecord) -> dict[str, Any]:
        runs = self.repository.list_validation_runs(
            record.package_id,
            validation_type=PackageValidationType.ORIGINAL_FIXED_WEIGHT,
            limit=None,
        )
        observed_runs: list[dict[str, Any]] = []
        same_manifest_run_count = 0
        matching_passed_run_id: str | None = None
        for run in runs:
            manifest_matches = run.manifest_sha256 == record.manifest_sha256
            if manifest_matches:
                same_manifest_run_count += 1
            if len(observed_runs) < 20:
                observed_runs.append(
                    {
                        "validation_run_id": run.validation_run_id,
                        "status": run.status.value,
                        "manifest_sha256": run.manifest_sha256,
                        "manifest_matches": manifest_matches,
                        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
                        "created_by": run.created_by,
                    }
                )
            if matching_passed_run_id is None and manifest_matches and run.status == PackageValidationStatus.PASSED:
                matching_passed_run_id = run.validation_run_id
        passed = matching_passed_run_id is not None
        blockers = [] if passed else ["original_fixed_weight_retest_missing_passed_run_for_current_manifest"]
        return {
            "passed": passed,
            "status": "READY" if passed else "BLOCKED",
            "package_id": record.package_id,
            "manifest_sha256": record.manifest_sha256,
            "required_validation_type": PackageValidationType.ORIGINAL_FIXED_WEIGHT.value,
            "required_status": PackageValidationStatus.PASSED.value,
            "same_manifest_run_count": same_manifest_run_count,
            "matching_passed_run_id": matching_passed_run_id,
            "observed_original_fixed_weight_run_count": len(runs),
            "observed_original_fixed_weight_runs": observed_runs,
            "observed_original_fixed_weight_runs_truncated": len(runs) > len(observed_runs),
            "blockers": blockers,
        }

    def _validation_stability_gate(self, summary: PackageValidationStabilitySummary) -> dict[str, Any]:
        blockers: list[str] = []
        if summary.seed_stability.status != StabilityStatus.STABLE:
            blockers.append(f"seed_stability={summary.seed_stability.status.value}")
        if summary.regime_stability.status != StabilityStatus.STABLE:
            blockers.append(f"regime_stability={summary.regime_stability.status.value}")
        passed = not blockers
        return {
            "passed": passed,
            "status": "READY" if passed else "BLOCKED",
            "seed_status": summary.seed_stability.status.value,
            "regime_status": summary.regime_stability.status.value,
            "seed_fragile": summary.seed_fragile,
            "regime_fragile": summary.regime_fragile,
            "warnings": summary.warnings,
            "summary": summary.model_dump(mode="json"),
            "blockers": blockers,
        }

    def _protected_asset_gate(self, package_id: str) -> dict[str, Any]:
        assets = self.list_package_assets(package_id)
        protected_assets = [asset for asset in assets if asset.protected_asset]
        unprotected_assets = [asset for asset in assets if not asset.protected_asset]
        blockers: list[str] = []
        if not assets:
            blockers.append("protected_asset_ledger_missing")
        if unprotected_assets:
            blockers.append("unprotected_assets_present")
        passed = not blockers
        return {
            "passed": passed,
            "status": "READY" if passed else "BLOCKED",
            "asset_count": len(assets),
            "protected_asset_count": len(protected_assets),
            "unprotected_asset_count": len(unprotected_assets),
            "protected_asset_refs": [asset.asset_ref for asset in protected_assets[:10]],
            "unprotected_asset_refs": [asset.asset_ref for asset in unprotected_assets[:10]],
            "blockers": blockers,
        }

    def _runtime_variant_candidate_gate(self, package_id: str) -> dict[str, Any]:
        variants = self.list_runtime_variants(package_id)
        validated = [
            variant
            for variant in variants
            if variant.validation_status == RuntimeVariantValidationStatus.VALIDATION_PASSED
        ]
        return {
            "passed": True,
            "status": "INFO",
            "runtime_variant_count": len(variants),
            "validated_variant_count": len(validated),
            "validated_variant_ids": [variant.variant_id for variant in validated[:10]],
            "validated_variant_names": [variant.variant_name for variant in validated[:10]],
            "blockers": [],
            "warnings": [] if validated else ["validated_runtime_variant_missing"],
        }

    @staticmethod
    def _initial_model_state(record: StrategyPackageRecord) -> StrategyPackageModelState:
        manifest = record.current_manifest()
        data_split = manifest.source_evidence.get("data_split") if isinstance(manifest.source_evidence, dict) else None
        if data_split is None and manifest.is_legacy_runtime_manifest:
            data_split = manifest.strategy_config.get("data_split") if isinstance(manifest.strategy_config, dict) else None
        data_split = data_split if isinstance(data_split, dict) else {}
        train_start = StrategyPackageService._parse_date(data_split.get("train_start") or data_split.get("data_start"))
        train_end = StrategyPackageService._parse_date(data_split.get("train_end") or data_split.get("backtest_end") or data_split.get("test_end"))
        return StrategyPackageModelState(
            package_id=record.package_id,
            active_model_version_id=None,
            train_start_date=train_start,
            train_end_date=train_end,
            trained_at=manifest.source.created_at if isinstance(manifest.source.created_at, datetime) else None,
            last_retrain_job_id=None,
            last_retrained_at=None,
            stale_after_days=30,
            metadata={"source": "strategy_package_backtest"},
        )

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        if value is None or value == "":
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None
