"""Read-only stability scoring for StrategyPackage validation runs."""

from __future__ import annotations

import math
import statistics
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .validation_run import StrategyPackageValidationRun


class StabilityStatus(str, Enum):
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    STABLE = "STABLE"
    FRAGILE = "FRAGILE"


class StabilitySample(BaseModel):
    model_config = ConfigDict(extra="forbid")

    validation_run_id: str
    label: str
    value: float


class StabilityScore(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: StabilityStatus
    metric_key: str
    sample_count: int
    stability_score: float | None = Field(default=None, ge=0.0, le=1.0)
    sensitivity_score: float | None = Field(default=None, ge=0.0)
    mean_value: float | None = None
    std_value: float | None = None
    best_value: float | None = None
    worst_value: float | None = None
    fragile: bool | None = None
    reason: str | None = None
    samples: list[StabilitySample] = Field(default_factory=list)


class PackageValidationStabilitySummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    package_id: str
    metric_key: str
    seed_stability: StabilityScore
    regime_stability: StabilityScore
    seed_fragile: bool | None = None
    regime_fragile: bool | None = None
    warnings: list[str] = Field(default_factory=list)


def summarize_validation_stability(
    package_id: str,
    runs: list[StrategyPackageValidationRun],
    *,
    metric_key: str = "annual_return",
    fragile_threshold: float = 0.75,
    min_samples: int = 2,
) -> PackageValidationStabilitySummary:
    """Compute deterministic read-only scores without mutating validation rows."""

    seed_score = _score_samples(
        _seed_samples(runs, metric_key=metric_key),
        metric_key=metric_key,
        fragile_threshold=fragile_threshold,
        min_samples=min_samples,
        insufficient_reason="need at least two validation runs with random_seed and metric evidence",
    )
    regime_score = _score_samples(
        _regime_samples(runs, metric_key=metric_key),
        metric_key=metric_key,
        fragile_threshold=fragile_threshold,
        min_samples=min_samples,
        insufficient_reason="need at least two regime metric samples",
    )
    warnings: list[str] = []
    if seed_score.status == StabilityStatus.INSUFFICIENT_EVIDENCE:
        warnings.append("seed_stability_insufficient_evidence")
    if regime_score.status == StabilityStatus.INSUFFICIENT_EVIDENCE:
        warnings.append("regime_stability_insufficient_evidence")
    return PackageValidationStabilitySummary(
        package_id=package_id,
        metric_key=metric_key,
        seed_stability=seed_score,
        regime_stability=regime_score,
        seed_fragile=seed_score.fragile,
        regime_fragile=regime_score.fragile,
        warnings=warnings,
    )


def _score_samples(
    samples: list[StabilitySample],
    *,
    metric_key: str,
    fragile_threshold: float,
    min_samples: int,
    insufficient_reason: str,
) -> StabilityScore:
    if len(samples) < min_samples:
        return StabilityScore(
            status=StabilityStatus.INSUFFICIENT_EVIDENCE,
            metric_key=metric_key,
            sample_count=len(samples),
            reason=insufficient_reason,
            samples=samples,
        )
    values = [sample.value for sample in samples]
    mean_value = statistics.fmean(values)
    std_value = statistics.pstdev(values)
    best_value = max(values)
    worst_value = min(values)
    denominator = max(abs(mean_value), 1e-12)
    sensitivity_score = abs(best_value - worst_value) / denominator
    stability_score = max(0.0, min(1.0, 1.0 - min(1.0, sensitivity_score)))
    fragile = stability_score < fragile_threshold
    return StabilityScore(
        status=StabilityStatus.FRAGILE if fragile else StabilityStatus.STABLE,
        metric_key=metric_key,
        sample_count=len(samples),
        stability_score=stability_score,
        sensitivity_score=sensitivity_score,
        mean_value=mean_value,
        std_value=std_value,
        best_value=best_value,
        worst_value=worst_value,
        fragile=fragile,
        samples=samples,
    )


def _seed_samples(runs: list[StrategyPackageValidationRun], *, metric_key: str) -> list[StabilitySample]:
    samples: list[StabilitySample] = []
    for run in runs:
        if run.random_seed is None:
            continue
        value = _metric_value(run.metrics_json, metric_key)
        if value is None:
            continue
        samples.append(
            StabilitySample(
                validation_run_id=run.validation_run_id,
                label=str(run.random_seed),
                value=value,
            )
        )
    return samples


def _regime_samples(runs: list[StrategyPackageValidationRun], *, metric_key: str) -> list[StabilitySample]:
    samples: list[StabilitySample] = []
    for run in runs:
        regime_metrics = run.evidence_json.get("regime_metrics") or run.metrics_json.get("regime_metrics")
        if not isinstance(regime_metrics, dict):
            continue
        for regime_name, payload in sorted(regime_metrics.items()):
            value = _metric_value(payload, metric_key)
            if value is None:
                continue
            samples.append(
                StabilitySample(
                    validation_run_id=run.validation_run_id,
                    label=str(regime_name),
                    value=value,
                )
            )
    return samples


def _metric_value(payload: Any, metric_key: str) -> float | None:
    value: Any = payload
    if isinstance(payload, dict):
        for part in metric_key.split("."):
            if not isinstance(value, dict) or part not in value:
                return None
            value = value[part]
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result
