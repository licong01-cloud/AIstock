"""Strategy Package Center service layer."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import StrategyPackageValidationError

from .execution_policy import (
    ExecutionPolicyValidationStatus,
    ValidatedExecutionPolicy,
    ensure_policy_can_enter_paper,
    normalize_execution_policy_json,
)
from .backtest_contract import validate_execution_policy_matches_manifest
from .metrics_summary import StrategyPackageMetricsSummary, metrics_summary_from_record
from .model_state import (
    ModelRetrainJobStatus,
    ModelRetrainPreview,
    ModelStalenessStatus,
    StrategyPackageModelRetrainJob,
    StrategyPackageModelState,
    evaluate_model_staleness,
)
from .models import PackageStatus, StrategyPackageManifest
from .qe_source_resolver import QEExperimentSourceResolver
from .repository import PackageStatusEvent, StrategyPackageRecord, StrategyPackageRepository
from .runtime_variant import (
    RuntimeVariantKind,
    RuntimeVariantValidationStatus,
    StrategyPackageRuntimeVariant,
    build_runtime_variant,
)
from .validation_run import (
    PackageValidationRetrainMode,
    PackageValidationReproducibility,
    PackageValidationStatus,
    PackageValidationType,
    StrategyPackageValidationRun,
    build_package_validation_run,
)
from .validation_stability import PackageValidationStabilitySummary, summarize_validation_stability
from .validators import StrategyPackageValidator


STATUS_TRANSITIONS: dict[PackageStatus, set[PackageStatus]] = {
    PackageStatus.ASSET_VALIDATED: {PackageStatus.DRAFT},
    PackageStatus.BACKTEST_APPROVED: {PackageStatus.DRAFT, PackageStatus.ASSET_VALIDATED},
    PackageStatus.SELECTION_ENABLED: {PackageStatus.BACKTEST_APPROVED},
    PackageStatus.PAPER_ENABLED: {PackageStatus.BACKTEST_APPROVED, PackageStatus.SELECTION_ENABLED},
    PackageStatus.PAPER_RUNNING: {PackageStatus.PAPER_ENABLED},
    PackageStatus.PAPER_PASSED: {PackageStatus.PAPER_RUNNING},
    PackageStatus.PAPER_FAILED: {PackageStatus.PAPER_RUNNING, PackageStatus.PAPER_ENABLED},
    PackageStatus.RETIRED: {
        PackageStatus.DRAFT,
        PackageStatus.ASSET_VALIDATED,
        PackageStatus.BACKTEST_APPROVED,
        PackageStatus.SELECTION_ENABLED,
        PackageStatus.PAPER_ENABLED,
        PackageStatus.PAPER_RUNNING,
        PackageStatus.PAPER_PASSED,
        PackageStatus.PAPER_FAILED,
    },
}


class StrategyPackageService:
    def __init__(
        self,
        *,
        repository: StrategyPackageRepository | Any | None = None,
        resolver: QEExperimentSourceResolver | None = None,
        validator: StrategyPackageValidator | None = None,
    ) -> None:
        self.repository = repository or StrategyPackageRepository()
        self.resolver = resolver or QEExperimentSourceResolver()
        self.validator = validator or StrategyPackageValidator()

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
            except json.JSONDecodeError:
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
        allowed = STATUS_TRANSITIONS.get(to_status)
        if not allowed:
            raise StrategyPackageValidationError(
                "unsupported strategy package target status",
                context={"package_id": package_id, "to_status": to_status.value},
            )
        if to_status == PackageStatus.PAPER_ENABLED:
            record = self.repository.get(package_id)
            # StrategyPackage freezes factor/model lineage. Minute execution
            # policies are selected later from backtest-validated policy rows,
            # so enabling the package for Paper v2 must not validate an obsolete
            # manifest-embedded V24/V25 runtime asset path.
            self.validator.validate_manifest_identity_for_paper_trading(record.current_manifest())
        return self.repository.transition_status(
            package_id=package_id,
            to_status=to_status,
            allowed_from=allowed,
            reason=reason,
            context=context or {},
        )

    def enable_selection(self, package_id: str) -> StrategyPackageRecord:
        return self.transition_status(
            package_id=package_id,
            to_status=PackageStatus.SELECTION_ENABLED,
            reason="enable_selection",
        )

    def enable_paper(self, package_id: str) -> StrategyPackageRecord:
        return self.transition_status(
            package_id=package_id,
            to_status=PackageStatus.PAPER_ENABLED,
            reason="enable_paper",
        )

    def retire(self, package_id: str, *, reason: str = "retire_package") -> StrategyPackageRecord:
        return self.transition_status(
            package_id=package_id,
            to_status=PackageStatus.RETIRED,
            reason=reason,
        )

    def list_status_events(self, package_id: str, *, limit: int = 200) -> list[PackageStatusEvent]:
        return self.repository.list_status_events(package_id, limit=limit)

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
        record = self.repository.get(package_id)
        normalized = normalize_execution_policy_json(policy_json)
        policy = ValidatedExecutionPolicy(
            package_id=package_id,
            manifest_sha256=record.manifest_sha256,
            policy_name=policy_name,
            policy_json=normalized,
            source_backtest_id=source_backtest_id,
            source_backtest_status=source_backtest_status,
            validation_status=ExecutionPolicyValidationStatus.BACKTEST_VALIDATED,
            paper_enabled=paper_enabled,
        )
        if paper_enabled:
            ensure_policy_can_enter_paper(policy)
            self.validator.validate_execution_policy_for_paper(
                package_id=package_id,
                policy_json=policy.policy_json,
            )
            validate_execution_policy_matches_manifest(
                record.current_manifest(),
                policy.policy_json,
                context={"package_id": package_id, "policy_name": policy_name, "check": "create_execution_policy"},
            )
        return self.repository.save_execution_policy(policy)

    def list_execution_policies(self, package_id: str) -> list[ValidatedExecutionPolicy]:
        return self.repository.list_execution_policies(package_id)

    def get_execution_policy(self, package_id: str, policy_id: str) -> ValidatedExecutionPolicy:
        return self.repository.get_execution_policy(package_id, policy_id)

    def enable_execution_policy_for_paper(self, package_id: str, policy_id: str) -> ValidatedExecutionPolicy:
        policy = self.repository.get_execution_policy(package_id, policy_id)
        ensure_policy_can_enter_paper(policy)
        self.validator.validate_execution_policy_for_paper(
            package_id=package_id,
            policy_json=policy.policy_json,
        )
        record = self.repository.get(package_id)
        validate_execution_policy_matches_manifest(
            record.current_manifest(),
            policy.policy_json,
            context={"package_id": package_id, "policy_id": policy_id, "check": "enable_execution_policy_for_paper"},
        )
        return self.repository.set_execution_policy_paper_enabled(
            package_id=package_id,
            policy_id=policy_id,
            paper_enabled=True,
        )

    def disable_execution_policy_for_paper(self, package_id: str, policy_id: str) -> ValidatedExecutionPolicy:
        return self.repository.set_execution_policy_paper_enabled(
            package_id=package_id,
            policy_id=policy_id,
            paper_enabled=False,
        )

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
        confirm_text: str | None = None,
    ) -> StrategyPackageModelRetrainJob:
        if not confirm_retrain or confirm_text != package_id:
            raise StrategyPackageValidationError(
                "model retrain start requires manual confirmation text matching package_id",
                context={
                    "package_id": package_id,
                    "confirm_retrain": confirm_retrain,
                    "confirm_text_matches": confirm_text == package_id,
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

    def summarize_validation_stability(
        self,
        package_id: str,
        *,
        metric_key: str = "annual_return",
        limit: int = 500,
    ) -> PackageValidationStabilitySummary:
        runs = self.repository.list_validation_runs(package_id, limit=limit)
        return summarize_validation_stability(package_id, runs, metric_key=metric_key)

    @staticmethod
    def _initial_model_state(record: StrategyPackageRecord) -> StrategyPackageModelState:
        manifest = record.current_manifest()
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
