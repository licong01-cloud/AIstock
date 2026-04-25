"""Strategy Package Center API v1."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend.services.strategy_package.qe_source_resolver import QEExperimentSourceResolver
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import (
    DataUnavailableError,
    TradingCoreError,
    UnsupportedFeatureError,
)

router = APIRouter(prefix="/strategy-packages", tags=["strategy-packages"])


def _raise_http(exc: TradingCoreError) -> None:
    status_code = 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    elif isinstance(exc, UnsupportedFeatureError):
        status_code = 422
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


@router.get("/from-qe-experiment/{experiment_id}/manifest")
def build_manifest_from_qe_experiment(experiment_id: str) -> dict:
    """Build a read-only StrategyPackage manifest from a completed QE experiment."""

    try:
        manifest = QEExperimentSourceResolver().build_from_experiment(experiment_id)
        StrategyPackageValidator().validate_manifest(manifest)
        return {"ok": True, "manifest": manifest.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/from-qe-experiment/{experiment_id}/paper-readiness")
def validate_qe_experiment_paper_readiness(experiment_id: str) -> dict:
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
