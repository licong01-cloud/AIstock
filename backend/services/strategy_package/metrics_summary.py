"""Display-only StrategyPackage metric summaries.

The summary is derived from the frozen manifest for API/UI display. It is not
written back into the manifest and therefore does not affect manifest_sha256.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .models import StrategyPackageManifest
from .repository import StrategyPackageRecord


class StrategyPackageMetricsSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    package_id: str
    manifest_sha256: str
    ic: float | None = None
    rank_ic: float | None = None
    icir: float | None = None
    sharpe: float | None = None
    annual_return: float | None = None
    max_drawdown: float | None = None
    final_nav: float | None = None
    turnover: float | None = None
    n_trading_days: int | None = None
    sample_start: date | None = None
    sample_end: date | None = None
    missing_metrics: list[str] = Field(default_factory=list)
    raw_metric_keys: list[str] = Field(default_factory=list)


SHARPE_ALIASES = (
    "sharpe",
    "Sharpe",
    "sharpe_ratio",
    "Sharpe Ratio",
    "annualized_sharpe",
    "1day.excess_return_with_cost.sharpe",
    "1day.excess_return_with_cost.sharpe_ratio",
)

TURNOVER_ALIASES = (
    "turnover",
    "Turnover",
    "1day.excess_return_with_cost.turnover",
)


def metrics_summary_from_record(record: StrategyPackageRecord) -> StrategyPackageMetricsSummary:
    manifest = record.current_manifest()
    return metrics_summary_from_manifest(manifest, manifest_sha256=record.manifest_sha256)


def metrics_summary_from_manifest(
    manifest: StrategyPackageManifest,
    *,
    manifest_sha256: str | None = None,
) -> StrategyPackageMetricsSummary:
    summary = manifest.backtest_summary
    raw_metrics = summary.raw_metrics or {}
    primary_component = manifest.alpha_components[0] if manifest.alpha_components else None
    component_metrics = primary_component.metrics_snapshot if primary_component else None

    turnover = _float_or_none(_raw_metric(raw_metrics, *TURNOVER_ALIASES))
    if turnover is None and component_metrics is not None:
        turnover = component_metrics.turnover

    result = StrategyPackageMetricsSummary(
        package_id=manifest.package_id,
        manifest_sha256=manifest_sha256 or manifest.manifest_sha256 or "",
        ic=summary.ic,
        rank_ic=summary.rank_ic,
        icir=summary.icir,
        sharpe=_float_or_none(_raw_metric(raw_metrics, *SHARPE_ALIASES)),
        annual_return=summary.annual_return,
        max_drawdown=summary.max_drawdown,
        final_nav=summary.final_nav,
        turnover=turnover,
        n_trading_days=summary.n_trading_days,
        sample_start=_date_or_none(getattr(summary, "sample_start", None) or raw_metrics.get("sample_start")),
        sample_end=_date_or_none(getattr(summary, "sample_end", None) or raw_metrics.get("sample_end")),
        raw_metric_keys=sorted(str(key) for key in raw_metrics.keys()),
    )
    missing = [
        name
        for name in ("ic", "rank_ic", "icir", "sharpe", "annual_return", "max_drawdown")
        if getattr(result, name) is None
    ]
    return result.model_copy(update={"missing_metrics": missing})


def _raw_metric(metrics: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in metrics:
            return metrics[key]
        current: Any = metrics
        found = True
        for part in key.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                found = False
                break
        if found:
            return current
    return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_or_none(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None
