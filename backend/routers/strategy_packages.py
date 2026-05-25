"""Strategy Package Center API v1."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.db.pg_pool import get_conn
from backend.services.strategy_package.candidate import (
    CandidateStrategyPackageRecord,
    CandidateStrategyPackageService,
    CandidateStrategyPackageStatus,
)
from backend.services.strategy_package.metrics_summary import metrics_summary_from_record
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.package_asset import StrategyPackageAssetType
from backend.services.strategy_package.qe_source_resolver import QEExperimentSourceResolver
from backend.services.strategy_package.repository import StrategyPackageRecord
from backend.services.strategy_package.runtime_config import build_default_runtime_config_bundle
from backend.services.strategy_package.runtime_variant import RuntimeVariantKind, RuntimeVariantValidationStatus
from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactService
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.strategy_package.validation_run import (
    PackageValidationRetrainMode,
    PackageValidationReproducibility,
    PackageValidationStatus,
    PackageValidationType,
)
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    StrategyPackageValidationError,
    TradingCoreError,
    UnsupportedFeatureError,
)

router = APIRouter(prefix="/strategy-packages", tags=["strategy-packages"])


class CreateFromQEExperimentRequest(BaseModel):
    experiment_id: str = Field(min_length=1)
    resolve_runtime_assets: bool = False


class CreateFromQEEvolutionLoopRequest(BaseModel):
    qe_task_id: str = Field(min_length=1)
    qe_loop_id: str = Field(min_length=1)
    resolve_runtime_assets: bool = False


class CreateFromCandidateStrategyPackageRequest(BaseModel):
    manifest_json: dict[str, Any] | None = None


class CreateCandidateFromQEExperimentRequest(BaseModel):
    experiment_id: str = Field(min_length=1)
    created_by: str = Field(default="aistock_api", min_length=1)
    display_name: str | None = None
    archive_run_id: str | None = None
    snapshot_config: dict[str, Any] = Field(default_factory=dict)
    factor_manifest: dict[str, Any] = Field(default_factory=dict)
    model_manifest: dict[str, Any] = Field(default_factory=dict)
    strategy_manifest: dict[str, Any] = Field(default_factory=dict)
    metric_snapshot: dict[str, Any] = Field(default_factory=dict)
    artifact_refs: dict[str, Any] = Field(default_factory=dict)
    completeness: dict[str, Any] = Field(default_factory=dict)
    eligibility: dict[str, Any] = Field(default_factory=dict)
    audit_context: dict[str, Any] = Field(default_factory=dict)
    manual_action: bool = True


class CreateCandidateFromQELoopRequest(BaseModel):
    qe_task_id: str = Field(min_length=1)
    qe_loop_id: str = Field(min_length=1)
    experiment_id: str | None = None
    created_by: str = Field(default="aistock_api", min_length=1)
    display_name: str | None = None
    archive_run_id: str | None = None
    snapshot_config: dict[str, Any] = Field(default_factory=dict)
    factor_manifest: dict[str, Any] = Field(default_factory=dict)
    model_manifest: dict[str, Any] = Field(default_factory=dict)
    strategy_manifest: dict[str, Any] = Field(default_factory=dict)
    metric_snapshot: dict[str, Any] = Field(default_factory=dict)
    artifact_refs: dict[str, Any] = Field(default_factory=dict)
    completeness: dict[str, Any] = Field(default_factory=dict)
    eligibility: dict[str, Any] = Field(default_factory=dict)
    audit_context: dict[str, Any] = Field(default_factory=dict)
    manual_action: bool = True


class CloneCandidateStrategyPackageRequest(BaseModel):
    created_by: str = Field(default="aistock_api", min_length=1)
    display_name: str | None = None
    overrides: dict[str, Any] = Field(default_factory=dict)


class DeleteCandidateStrategyPackageRequest(BaseModel):
    deleted_by: str = Field(default="aistock_api", min_length=1)
    delete_reason: str | None = None


class RefreshCandidateStrategyPackageRequest(BaseModel):
    refreshed_by: str = Field(default="aistock_api", min_length=1)


class TransitionStatusRequest(BaseModel):
    reason: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)


class TransitionPackageStatusRequest(BaseModel):
    to_status: PackageStatus
    reason: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)


class RepairManifestHashRequest(BaseModel):
    operator: str = Field(default="paper_v2_gate_decoupling", min_length=1)


class CreateExecutionPolicyRequest(BaseModel):
    policy_name: str = Field(min_length=1)
    policy_json: dict[str, Any] = Field(default_factory=dict)
    source_backtest_id: str = Field(min_length=1)
    source_backtest_status: str = Field(min_length=1)
    paper_enabled: bool = False


class RecordPackageAssetRequest(BaseModel):
    asset_type: StrategyPackageAssetType
    asset_ref: str = Field(min_length=1)
    asset_sha256: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    asset_role: str = Field(default="governed_asset", min_length=1)
    asset_size_bytes: int | None = Field(default=None, ge=0)
    protected_asset: bool = True
    source_uri: str | None = None


class ModelRetrainPreviewRequest(BaseModel):
    as_of_date: date | None = None
    lookback_days: int = Field(default=756, gt=0)


class ModelRetrainStartRequest(BaseModel):
    as_of_date: date | None = None
    lookback_days: int = Field(default=756, gt=0)
    job_type: str = Field(default="rolling_retrain", min_length=1)
    config: dict[str, Any] = Field(default_factory=dict)
    confirm_retrain: bool = False
    confirm_text: str | None = None


class CreateRuntimeVariantRequest(BaseModel):
    variant_name: str = Field(min_length=1)
    variant_kind: RuntimeVariantKind
    variant_config: dict[str, Any] = Field(default_factory=dict)
    validation_status: RuntimeVariantValidationStatus = RuntimeVariantValidationStatus.DRAFT
    paper_candidate: bool = False
    validation_evidence: dict[str, Any] = Field(default_factory=dict)
    created_by: str = Field(default="aistock_api", min_length=1)


class RuntimeVariantValidationRequest(BaseModel):
    validation_status: RuntimeVariantValidationStatus
    paper_candidate: bool = False
    validation_evidence: dict[str, Any] = Field(default_factory=dict)


class CreateValidationRunRequest(BaseModel):
    validation_type: PackageValidationType
    retrain_mode: PackageValidationRetrainMode
    runtime_variant_id: str | None = None
    model_version_id: str | None = None
    seed_policy: str | None = None
    random_seed: int | None = None
    source_data_version: str | None = None
    target_data_version: str | None = None
    backtest_start: date | None = None
    backtest_end: date | None = None
    status: PackageValidationStatus = PackageValidationStatus.REQUESTED
    metrics_json: dict[str, Any] = Field(default_factory=dict)
    artifact_manifest_json: dict[str, Any] = Field(default_factory=dict)
    evidence_json: dict[str, Any] = Field(default_factory=dict)
    reproducibility_level: PackageValidationReproducibility = PackageValidationReproducibility.UNKNOWN
    created_by: str = Field(default="aistock_api", min_length=1)
    completed_at: datetime | None = None


class GenerateSelectionArtifactsRequest(BaseModel):
    trade_date: date | None = None
    start_date: date | None = None
    end_date: date | None = None
    data_source: str = "DB_HISTORICAL"
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    source_path: str | None = None
    include_reference_price: bool = True
    cutoff_date: date | None = None


def _raise_http(exc: TradingCoreError) -> None:
    status_code = 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    elif isinstance(exc, UnsupportedFeatureError):
        status_code = 422
    elif isinstance(exc, InvalidStateTransitionError):
        status_code = 409
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


def _record_payload(record: StrategyPackageRecord) -> dict[str, Any]:
    manifest = record.current_manifest()
    return {
        "package_id": record.package_id,
        "package_name": record.package_name,
        "package_version": record.package_version,
        "source_type": record.source_type,
        "source_id": record.source_id,
        "loop_id": record.loop_id,
        "run_id": record.run_id,
        "package_status": record.package_status.value,
        "manifest_sha256": record.manifest_sha256,
        "paper_portfolio_count": record.paper_portfolio_count,
        "created_at": record.created_at.isoformat(),
        "updated_at": record.updated_at.isoformat(),
        "metrics_summary": metrics_summary_from_record(record).model_dump(mode="json"),
        "manifest": manifest.model_dump(mode="json"),
        "runtime_config_contract": build_default_runtime_config_bundle(manifest),
    }


def _candidate_payload(record: CandidateStrategyPackageRecord) -> dict[str, Any]:
    return record.model_dump(mode="json")


def _execution_policy_payload(policy) -> dict[str, Any]:
    return policy.model_dump(mode="json")


def _package_asset_payload(asset) -> dict[str, Any]:
    return asset.model_dump(mode="json")


def _selection_artifact_payload(artifact) -> dict[str, Any]:
    payload = artifact.model_dump(mode="json")
    payload.pop("scores_json", None)
    payload["score_preview"] = artifact.scores_json[:10]
    return payload


def _runtime_variant_payload(variant) -> dict[str, Any]:
    return variant.model_dump(mode="json")


def _validation_run_payload(run) -> dict[str, Any]:
    return run.model_dump(mode="json")


def _validation_stability_payload(summary) -> dict[str, Any]:
    return summary.model_dump(mode="json")


def _trading_dates_between(start_date: date, end_date: date) -> list[date]:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cal_date
                    FROM market.trading_calendar
                    WHERE cal_date BETWEEN %s AND %s
                      AND is_trading = TRUE
                    ORDER BY cal_date
                    """,
                    (start_date, end_date),
                )
                rows = cur.fetchall()
    except Exception as exc:
        raise DataUnavailableError(
            "trading calendar query failed for selection artifact generation",
            context={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        ) from exc
    dates = [row[0] for row in rows]
    if not dates:
        raise DataUnavailableError(
            "no trading dates found for selection artifact generation range",
            context={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        )
    return dates


@router.post("/from-qe-experiment")
def create_package_from_qe_experiment(req: CreateFromQEExperimentRequest) -> dict[str, Any]:
    try:
        record = StrategyPackageService().create_from_qe_experiment(
            req.experiment_id,
            resolve_runtime_assets=req.resolve_runtime_assets,
        )
        return {"ok": True, "package": _record_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/from-qe-evolution-loop")
def create_package_from_qe_evolution_loop(req: CreateFromQEEvolutionLoopRequest) -> dict[str, Any]:
    try:
        record = StrategyPackageService().create_from_qe_evolution_loop(
            qe_task_id=req.qe_task_id,
            qe_loop_id=req.qe_loop_id,
            resolve_runtime_assets=req.resolve_runtime_assets,
        )
        return {"ok": True, "package": _record_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/from-candidate/{candidate_id}")
def create_package_from_candidate(
    candidate_id: str,
    req: CreateFromCandidateStrategyPackageRequest,
) -> dict[str, Any]:
    try:
        record = StrategyPackageService().create_from_candidate(
            candidate_id,
            manifest_json=req.manifest_json,
        )
        return {"ok": True, "package": _record_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("")
def list_strategy_packages(status: PackageStatus | None = None, limit: int = 100) -> dict[str, Any]:
    try:
        records = StrategyPackageService().list_packages(status=status, limit=limit)
        return {"ok": True, "packages": [_record_payload(record) for record in records]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/qe-sources")
def list_qe_strategy_package_sources(source_kind: str = "all", limit: int = 200) -> dict[str, Any]:
    try:
        sources = StrategyPackageService().list_qe_packaging_sources(
            source_kind=source_kind,
            limit=limit,
        )
        return {"ok": True, "sources": sources}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/candidates/from-qe-experiment")
def create_candidate_from_qe_experiment(req: CreateCandidateFromQEExperimentRequest) -> dict[str, Any]:
    try:
        record = CandidateStrategyPackageService().create_from_qe_experiment(
            experiment_id=req.experiment_id,
            created_by=req.created_by,
            display_name=req.display_name,
            archive_run_id=req.archive_run_id,
            snapshot_config=req.snapshot_config,
            factor_manifest=req.factor_manifest,
            model_manifest=req.model_manifest,
            strategy_manifest=req.strategy_manifest,
            metric_snapshot=req.metric_snapshot,
            artifact_refs=req.artifact_refs,
            completeness=req.completeness,
            eligibility=req.eligibility,
            audit_context=req.audit_context,
            manual_action=req.manual_action,
        )
        return {"ok": True, "candidate": _candidate_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/candidates/from-qe-loop")
def create_candidate_from_qe_loop(req: CreateCandidateFromQELoopRequest) -> dict[str, Any]:
    try:
        record = CandidateStrategyPackageService().create_from_qe_loop(
            task_id=req.qe_task_id,
            loop_id=req.qe_loop_id,
            experiment_id=req.experiment_id,
            created_by=req.created_by,
            display_name=req.display_name,
            archive_run_id=req.archive_run_id,
            snapshot_config=req.snapshot_config,
            factor_manifest=req.factor_manifest,
            model_manifest=req.model_manifest,
            strategy_manifest=req.strategy_manifest,
            metric_snapshot=req.metric_snapshot,
            artifact_refs=req.artifact_refs,
            completeness=req.completeness,
            eligibility=req.eligibility,
            audit_context=req.audit_context,
            manual_action=req.manual_action,
        )
        return {"ok": True, "candidate": _candidate_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/candidates")
def list_candidate_strategy_packages(
    status: CandidateStrategyPackageStatus | None = CandidateStrategyPackageStatus.ACTIVE,
    limit: int = 100,
) -> dict[str, Any]:
    try:
        records = CandidateStrategyPackageService().list_candidates(status=status, limit=limit)
        return {"ok": True, "candidates": [_candidate_payload(record) for record in records]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/candidates/{candidate_id}")
def get_candidate_strategy_package(candidate_id: str) -> dict[str, Any]:
    try:
        record = CandidateStrategyPackageService().get_candidate(candidate_id)
        return {"ok": True, "candidate": _candidate_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/manifest-integrity")
def get_strategy_package_manifest_integrity(limit: int = 500) -> dict[str, Any]:
    try:
        report = StrategyPackageService().validate_manifest_integrity(limit=limit)
        return {"ok": True, "report": report}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/repair-manifest-hash")
def repair_strategy_package_manifest_hash(package_id: str, req: RepairManifestHashRequest | None = None) -> dict[str, Any]:
    try:
        record = StrategyPackageService().repair_manifest_hash(
            package_id,
            operator=(req.operator if req else "paper_v2_gate_decoupling"),
        )
        return {"ok": True, "package": _record_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/candidates/{candidate_id}/clone")
def clone_candidate_strategy_package(
    candidate_id: str,
    req: CloneCandidateStrategyPackageRequest,
) -> dict[str, Any]:
    try:
        record = CandidateStrategyPackageService().clone_candidate(
            source_candidate_id=candidate_id,
            created_by=req.created_by,
            display_name=req.display_name,
            overrides=req.overrides,
        )
        return {"ok": True, "candidate": _candidate_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/candidates/{candidate_id}/refresh-snapshot")
def refresh_candidate_strategy_package_snapshot(
    candidate_id: str,
    req: RefreshCandidateStrategyPackageRequest,
) -> dict[str, Any]:
    try:
        record = CandidateStrategyPackageService().refresh_snapshot_from_source(
            candidate_id=candidate_id,
            refreshed_by=req.refreshed_by,
        )
        return {"ok": True, "candidate": _candidate_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.delete("/candidates/{candidate_id}")
def delete_candidate_strategy_package(
    candidate_id: str,
    req: DeleteCandidateStrategyPackageRequest,
) -> dict[str, Any]:
    try:
        record = CandidateStrategyPackageService().delete_candidate(
            candidate_id=candidate_id,
            deleted_by=req.deleted_by,
            delete_reason=req.delete_reason,
        )
        return {"ok": True, "candidate": _candidate_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}")
def get_strategy_package(package_id: str) -> dict[str, Any]:
    try:
        record = StrategyPackageService().get_package(package_id)
        return {"ok": True, "package": _record_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/status-events")
def list_strategy_package_status_events(package_id: str, limit: int = 200) -> dict[str, Any]:
    try:
        events = StrategyPackageService().list_status_events(package_id, limit=limit)
        return {"ok": True, "package_id": package_id, "events": [event.model_dump(mode="json") for event in events]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/assets")
def record_strategy_package_asset(package_id: str, req: RecordPackageAssetRequest) -> dict[str, Any]:
    try:
        asset = StrategyPackageService().record_package_asset(
            package_id,
            asset_type=req.asset_type,
            asset_ref=req.asset_ref,
            asset_sha256=req.asset_sha256,
            metadata=req.metadata,
            asset_role=req.asset_role,
            asset_size_bytes=req.asset_size_bytes,
            protected_asset=req.protected_asset,
            source_uri=req.source_uri,
        )
        return {"ok": True, "asset": _package_asset_payload(asset)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/assets")
def list_strategy_package_assets(package_id: str, protected_only: bool = False) -> dict[str, Any]:
    try:
        assets = StrategyPackageService().list_package_assets(package_id, protected_only=protected_only)
        return {
            "ok": True,
            "package_id": package_id,
            "assets": [_package_asset_payload(asset) for asset in assets],
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/metrics-summary")
def get_strategy_package_metrics_summary(package_id: str) -> dict[str, Any]:
    try:
        summary = StrategyPackageService().get_metrics_summary(package_id)
        return {"ok": True, "package_id": package_id, "metrics_summary": summary.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/selection-artifacts/generate")
def generate_strategy_package_selection_artifacts(
    package_id: str,
    req: GenerateSelectionArtifactsRequest,
) -> dict[str, Any]:
    try:
        if req.trade_date is not None:
            trade_dates = [req.trade_date]
        else:
            if req.start_date is None or req.end_date is None:
                raise StrategyPackageValidationError(
                    "selection artifact generation requires trade_date or start_date/end_date"
                )
            if req.end_date < req.start_date:
                raise StrategyPackageValidationError("end_date must be >= start_date")
            day_count = (req.end_date - req.start_date).days + 1
            if day_count > 370:
                raise StrategyPackageValidationError(
                    "selection artifact generation date range is too large",
                    context={"day_count": day_count, "max_day_count": 370},
                )
            trade_dates = _trading_dates_between(req.start_date, req.end_date)
        if req.source_path:
            raise StrategyPackageValidationError(
                "authoritative selection artifact generation does not accept source_path; use generate-diagnostic-backtest explicitly"
            )
        artifacts = StrategyPackageSelectionArtifactService().generate_from_live_inference_dates(
            package_id=package_id,
            trade_dates=trade_dates,
            data_source=req.data_source,
            runtime_config=req.runtime_config,
            include_reference_price=req.include_reference_price,
            cutoff_date=req.cutoff_date,
        )
        return {
            "ok": True,
            "package_id": package_id,
            "artifacts": [_selection_artifact_payload(artifact) for artifact in artifacts],
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/selection-artifacts/generate-diagnostic-backtest")
def generate_strategy_package_diagnostic_backtest_selection_artifacts(
    package_id: str,
    req: GenerateSelectionArtifactsRequest,
) -> dict[str, Any]:
    """Generate diagnostic-only artifacts from QE backtest pred.pkl.

    These artifacts are not accepted by authoritative Selection Center/Paper v2
    runtime and must not be used as current simulated/live selection.
    """

    try:
        if req.trade_date is not None:
            trade_dates = [req.trade_date]
        else:
            if req.start_date is None or req.end_date is None:
                raise StrategyPackageValidationError(
                    "diagnostic selection artifact generation requires trade_date or start_date/end_date"
                )
            if req.end_date < req.start_date:
                raise StrategyPackageValidationError("end_date must be >= start_date")
            day_count = (req.end_date - req.start_date).days + 1
            if day_count > 370:
                raise StrategyPackageValidationError(
                    "diagnostic selection artifact generation date range is too large",
                    context={"day_count": day_count, "max_day_count": 370},
                )
            trade_dates = _trading_dates_between(req.start_date, req.end_date)
        artifacts = StrategyPackageSelectionArtifactService().generate_from_qe_prediction_dates(
            package_id=package_id,
            trade_dates=trade_dates,
            data_source=req.data_source,
            runtime_config=req.runtime_config,
            source_path=req.source_path,
            include_reference_price=req.include_reference_price,
        )
        return {
            "ok": True,
            "package_id": package_id,
            "diagnostic_only": True,
            "artifacts": [_selection_artifact_payload(artifact) for artifact in artifacts],
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/selection-artifacts")
def list_strategy_package_selection_artifacts(package_id: str, limit: int = 100) -> dict[str, Any]:
    try:
        artifacts = StrategyPackageSelectionArtifactService().list_artifacts(package_id, limit=limit)
        return {
            "ok": True,
            "package_id": package_id,
            "artifacts": [_selection_artifact_payload(artifact) for artifact in artifacts],
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/execution-policies")
def create_strategy_package_execution_policy(package_id: str, req: CreateExecutionPolicyRequest) -> dict[str, Any]:
    try:
        policy = StrategyPackageService().create_execution_policy(
            package_id=package_id,
            policy_name=req.policy_name,
            policy_json=req.policy_json,
            source_backtest_id=req.source_backtest_id,
            source_backtest_status=req.source_backtest_status,
            paper_enabled=req.paper_enabled,
        )
        return {"ok": True, "execution_policy": _execution_policy_payload(policy)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/execution-policies")
def list_strategy_package_execution_policies(package_id: str) -> dict[str, Any]:
    try:
        policies = StrategyPackageService().list_execution_policies(package_id)
        return {"ok": True, "package_id": package_id, "execution_policies": [_execution_policy_payload(policy) for policy in policies]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/execution-policies/{policy_id}/enable-paper")
def enable_strategy_package_execution_policy_for_paper(package_id: str, policy_id: str) -> dict[str, Any]:
    try:
        policy = StrategyPackageService().enable_execution_policy_for_paper(package_id, policy_id)
        return {"ok": True, "execution_policy": _execution_policy_payload(policy)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/execution-policies/{policy_id}/disable-paper")
def disable_strategy_package_execution_policy_for_paper(package_id: str, policy_id: str) -> dict[str, Any]:
    try:
        policy = StrategyPackageService().disable_execution_policy_for_paper(package_id, policy_id)
        return {"ok": True, "execution_policy": _execution_policy_payload(policy)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/model-state")
def get_strategy_package_model_state(package_id: str, as_of_date: date | None = None) -> dict[str, Any]:
    try:
        state = StrategyPackageService().get_model_state(package_id, as_of_date=as_of_date)
        return {"ok": True, "model_state": state.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/model-retrain/preview")
def preview_strategy_package_model_retrain(package_id: str, req: ModelRetrainPreviewRequest) -> dict[str, Any]:
    try:
        preview = StrategyPackageService().preview_model_retrain(
            package_id,
            as_of_date=req.as_of_date,
            lookback_days=req.lookback_days,
        )
        return {"ok": True, "preview": preview.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/model-retrain/start")
def start_strategy_package_model_retrain(package_id: str, req: ModelRetrainStartRequest) -> dict[str, Any]:
    try:
        job = StrategyPackageService().start_model_retrain(
            package_id,
            as_of_date=req.as_of_date,
            lookback_days=req.lookback_days,
            job_type=req.job_type,
            config=req.config,
            confirm_retrain=req.confirm_retrain,
            confirm_text=req.confirm_text,
        )
        return {"ok": True, "job": job.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/model-retrain/jobs")
def list_strategy_package_model_retrain_jobs(package_id: str, limit: int = 100) -> dict[str, Any]:
    try:
        jobs = StrategyPackageService().list_model_retrain_jobs(package_id, limit=limit)
        return {"ok": True, "package_id": package_id, "jobs": [job.model_dump(mode="json") for job in jobs]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/runtime-variants")
def create_strategy_package_runtime_variant(package_id: str, req: CreateRuntimeVariantRequest) -> dict[str, Any]:
    try:
        variant = StrategyPackageService().create_runtime_variant(
            package_id,
            variant_name=req.variant_name,
            variant_kind=req.variant_kind,
            variant_config=req.variant_config,
            validation_status=req.validation_status,
            paper_candidate=req.paper_candidate,
            validation_evidence=req.validation_evidence,
            created_by=req.created_by,
        )
        return {"ok": True, "runtime_variant": _runtime_variant_payload(variant)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/runtime-variants")
def list_strategy_package_runtime_variants(
    package_id: str,
    include_retired: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    try:
        variants = StrategyPackageService().list_runtime_variants(
            package_id,
            include_retired=include_retired,
            limit=limit,
        )
        return {
            "ok": True,
            "package_id": package_id,
            "runtime_variants": [_runtime_variant_payload(variant) for variant in variants],
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/runtime-variants/{variant_id}/validation")
def mark_strategy_package_runtime_variant_validation(
    package_id: str,
    variant_id: str,
    req: RuntimeVariantValidationRequest,
) -> dict[str, Any]:
    try:
        variant = StrategyPackageService().mark_runtime_variant_validation(
            package_id,
            variant_id,
            validation_status=req.validation_status,
            paper_candidate=req.paper_candidate,
            validation_evidence=req.validation_evidence,
        )
        return {"ok": True, "runtime_variant": _runtime_variant_payload(variant)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/validation-runs")
def create_strategy_package_validation_run(package_id: str, req: CreateValidationRunRequest) -> dict[str, Any]:
    try:
        run = StrategyPackageService().create_validation_run(
            package_id,
            validation_type=req.validation_type,
            retrain_mode=req.retrain_mode,
            runtime_variant_id=req.runtime_variant_id,
            model_version_id=req.model_version_id,
            seed_policy=req.seed_policy,
            random_seed=req.random_seed,
            source_data_version=req.source_data_version,
            target_data_version=req.target_data_version,
            backtest_start=req.backtest_start,
            backtest_end=req.backtest_end,
            status=req.status,
            metrics_json=req.metrics_json,
            artifact_manifest_json=req.artifact_manifest_json,
            evidence_json=req.evidence_json,
            reproducibility_level=req.reproducibility_level,
            created_by=req.created_by,
            completed_at=req.completed_at,
        )
        return {"ok": True, "validation_run": _validation_run_payload(run)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/validation-runs")
def list_strategy_package_validation_runs(
    package_id: str,
    validation_type: PackageValidationType | None = None,
    runtime_variant_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    try:
        runs = StrategyPackageService().list_validation_runs(
            package_id,
            validation_type=validation_type,
            runtime_variant_id=runtime_variant_id,
            limit=limit,
        )
        return {
            "ok": True,
            "package_id": package_id,
            "validation_runs": [_validation_run_payload(run) for run in runs],
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/validation-runs/{validation_run_id}")
def get_strategy_package_validation_run(package_id: str, validation_run_id: str) -> dict[str, Any]:
    try:
        run = StrategyPackageService().get_validation_run(package_id, validation_run_id)
        return {"ok": True, "validation_run": _validation_run_payload(run)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/validation-stability")
def get_strategy_package_validation_stability(
    package_id: str,
    metric_key: str = "annual_return",
    limit: int = 500,
) -> dict[str, Any]:
    try:
        summary = StrategyPackageService().summarize_validation_stability(
            package_id,
            metric_key=metric_key,
            limit=limit,
        )
        return {"ok": True, "stability": _validation_stability_payload(summary)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/governance-eligibility")
def get_strategy_package_governance_eligibility(
    package_id: str,
    metric_key: str = "annual_return",
    limit: int = 500,
) -> dict[str, Any]:
    """Return a read-only summary of paper-stage governance eligibility."""

    try:
        eligibility = StrategyPackageService().governance_eligibility(
            package_id,
            metric_key=metric_key,
            limit=limit,
        )
        return {"ok": True, "package_id": package_id, "eligibility": eligibility}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/{package_id}/paper-simulation-admission")
def get_strategy_package_paper_simulation_admission(
    package_id: str,
    metric_key: str = "annual_return",
    governance_limit: int = 500,
) -> dict[str, Any]:
    """Return alpha-core admission for Paper v2 simulation.

    This endpoint is intentionally separate from live-strict governance so the
    UI can explain warnings without blocking backtest-approved simulation.
    """

    try:
        admission = StrategyPackageService().paper_simulation_admission(
            package_id,
            metric_key=metric_key,
            governance_limit=governance_limit,
        )
        return {"ok": True, "package_id": package_id, "admission": admission}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/validate")
def validate_strategy_package(package_id: str) -> dict[str, Any]:
    try:
        manifest = StrategyPackageService().validate_readiness(package_id)
        return {"ok": True, "manifest": manifest.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/transition-status")
def transition_strategy_package_status(package_id: str, req: TransitionPackageStatusRequest) -> dict[str, Any]:
    try:
        record = StrategyPackageService().transition_status(
            package_id=package_id,
            to_status=req.to_status,
            reason=req.reason or f"transition_to_{req.to_status.value.lower()}",
            context=req.context,
        )
        return {"ok": True, "package": _record_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/enable-selection")
def enable_strategy_package_selection(package_id: str) -> dict[str, Any]:
    try:
        record = StrategyPackageService().enable_selection(package_id)
        return {"ok": True, "package": _record_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/enable-paper")
def enable_strategy_package_paper(package_id: str) -> dict[str, Any]:
    try:
        record = StrategyPackageService().enable_paper(package_id)
        return {"ok": True, "package": _record_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/{package_id}/retire")
def retire_strategy_package(package_id: str, req: TransitionStatusRequest | None = None) -> dict[str, Any]:
    try:
        record = StrategyPackageService().retire(package_id, reason=(req.reason if req else None) or "retire_package")
        return {"ok": True, "package": _record_payload(record)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/from-qe-experiment/{experiment_id}/manifest")
def build_manifest_from_qe_experiment(experiment_id: str) -> dict[str, Any]:
    """Build a read-only StrategyPackage manifest from a completed QE experiment."""

    try:
        manifest = QEExperimentSourceResolver().build_from_experiment(experiment_id)
        StrategyPackageValidator().validate_manifest(manifest)
        return {"ok": True, "manifest": manifest.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/from-qe-experiment/{experiment_id}/paper-readiness")
def validate_qe_experiment_paper_readiness(experiment_id: str) -> dict[str, Any]:
    """Validate whether the package is ready for minute-line paper trading."""

    try:
        manifest = QEExperimentSourceResolver().build_from_experiment(
            experiment_id,
            resolve_runtime_assets=True,
        )
        StrategyPackageValidator().validate_for_paper_trading(manifest)
        return {"ok": True, "manifest": manifest.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)
