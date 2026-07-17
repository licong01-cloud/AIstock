"""Pure, replayable HMM replacement evaluator.

The evaluator performs no filesystem, network or database I/O.  Callers must
freeze all dates and provide immutable input snapshots before invoking it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from statistics import fmean
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd

from .errors import (
    CoefficientDateCoverageEmptyError,
    InvalidSpecError,
    LabelHorizonMismatchError,
    MarketDataUnavailableError,
    NoCommonDatesError,
)
from .models import EvidenceQuality, canonical_json_sha256

EVALUATOR_VERSION = "hmm_replacement_evaluator_v1"
METRIC_VERSION = "hmm_replacement_metrics_v1"
RESULT_SCHEMA_VERSION = "hmm_evaluation_result_v1"


class EvaluationInputError(InvalidSpecError):
    """Fail-loud invalid or incomplete evaluator input."""


@dataclass(frozen=True)
class CandidateCoefficients:
    """Validated in-memory coefficient payload used by the pure evaluator."""

    daily_coefficients: Mapping[date, Mapping[str, float]]
    stock_sector_map: Mapping[str, str]

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "CandidateCoefficients":
        daily_raw = payload.get("daily_coefficients")
        mapping_raw = payload.get("stock_sector_map")
        if not isinstance(daily_raw, Mapping) or not daily_raw:
            raise CoefficientDateCoverageEmptyError(
                "candidate daily_coefficients must be a non-empty mapping",
            )
        if not isinstance(mapping_raw, Mapping) or not mapping_raw:
            raise EvaluationInputError(
                "candidate stock_sector_map must be a non-empty mapping",
                context={"reason": "sector_mapping_empty"},
            )

        daily: dict[date, dict[str, float]] = {}
        for raw_date, raw_sectors in daily_raw.items():
            try:
                parsed_date = raw_date if isinstance(raw_date, date) else date.fromisoformat(str(raw_date))
            except ValueError as exc:
                raise EvaluationInputError(
                    "candidate coefficient date must be ISO YYYY-MM-DD",
                    context={"date": str(raw_date)},
                ) from exc
            if not isinstance(raw_sectors, Mapping) or not raw_sectors:
                raise EvaluationInputError(
                    "each coefficient date must contain at least one sector",
                    context={"date": parsed_date.isoformat()},
                )
            sectors: dict[str, float] = {}
            for raw_sector, raw_value in raw_sectors.items():
                sector = str(raw_sector or "").strip()
                if not sector:
                    raise EvaluationInputError("coefficient sector codes must be non-empty")
                if isinstance(raw_value, bool):
                    raise EvaluationInputError("coefficient values must be numeric")
                try:
                    value = float(raw_value)
                except (TypeError, ValueError) as exc:
                    raise EvaluationInputError("coefficient values must be numeric") from exc
                if not math.isfinite(value) or value <= 0:
                    raise EvaluationInputError(
                        "coefficient values must be finite and greater than zero",
                        context={"date": parsed_date.isoformat(), "sector": sector},
                    )
                sectors[sector] = value
            daily[parsed_date] = sectors

        stock_sector_map: dict[str, str] = {}
        for raw_symbol, raw_sector in mapping_raw.items():
            symbol = str(raw_symbol or "").strip()
            sector = str(raw_sector or "").strip()
            if not symbol or not sector:
                raise EvaluationInputError("stock_sector_map entries must be non-empty strings")
            stock_sector_map[symbol] = sector
        return cls(daily_coefficients=daily, stock_sector_map=stock_sector_map)


@dataclass(frozen=True)
class DateCoveragePlan:
    evaluation_dates: tuple[date, ...]
    prediction_dates: tuple[date, ...]
    label_dates: tuple[date, ...]
    common_dates: tuple[date, ...]
    dropped_prediction_dates: tuple[date, ...]
    dropped_label_dates: tuple[date, ...]
    dropped_candidate_dates: Mapping[str, tuple[date, ...]]

    @property
    def degraded(self) -> bool:
        return bool(
            self.dropped_prediction_dates
            or self.dropped_label_dates
            or any(self.dropped_candidate_dates.values())
        )

    def as_evidence(self) -> dict[str, Any]:
        return {
            "evaluation_dates": [item.isoformat() for item in self.evaluation_dates],
            "prediction_date_count": len(self.prediction_dates),
            "label_date_count": len(self.label_dates),
            "common_date_count": len(self.common_dates),
            "dropped_prediction_dates": [item.isoformat() for item in self.dropped_prediction_dates],
            "dropped_label_dates": [item.isoformat() for item in self.dropped_label_dates],
            "dropped_candidate_dates": {
                candidate_id: [item.isoformat() for item in dates]
                for candidate_id, dates in sorted(self.dropped_candidate_dates.items())
            },
        }


@dataclass(frozen=True)
class EvaluationComputation:
    """In-memory calculation plus the bounded durable result payload."""

    result: Mapping[str, Any]
    replacement_rows: tuple[Mapping[str, Any], ...]
    daily_summary: tuple[Mapping[str, Any], ...]


def resolve_batch_common_dates(
    *,
    predictions: pd.DataFrame,
    labels: pd.DataFrame,
    candidates: Mapping[str, CandidateCoefficients],
    window_start: date,
    window_end: date,
    policy: str = "batch_common_intersection_with_evidence",
) -> DateCoveragePlan:
    """Freeze one date set shared by every candidate in a batch."""

    if window_start > window_end:
        raise EvaluationInputError("window_start must not exceed window_end")
    if not candidates:
        raise EvaluationInputError("at least one candidate is required")
    if policy not in {"batch_common_intersection_with_evidence", "strict_full"}:
        raise EvaluationInputError("unsupported date coverage policy", context={"policy": policy})

    prediction_dates = _frame_dates(predictions, "predictions", window_start, window_end)
    label_dates = _frame_dates(labels, "labels", window_start, window_end)
    candidate_dates = {
        candidate_id: {
            item for item in coefficients.daily_coefficients if window_start <= item <= window_end
        }
        for candidate_id, coefficients in candidates.items()
    }
    empty_candidates = sorted(candidate_id for candidate_id, dates in candidate_dates.items() if not dates)
    if empty_candidates:
        raise CoefficientDateCoverageEmptyError(
            "candidate coefficient coverage is empty in the requested window",
            context={"candidate_ids": empty_candidates},
        )

    common = set(prediction_dates) & set(label_dates)
    for dates in candidate_dates.values():
        common &= dates
    if not common:
        raise NoCommonDatesError(
            "prediction, label and coefficient inputs have no common dates",
        )

    if policy == "strict_full":
        missing_label = sorted(set(prediction_dates) - set(label_dates))
        missing_candidates = {
            candidate_id: sorted(set(prediction_dates) - dates)
            for candidate_id, dates in candidate_dates.items()
            if set(prediction_dates) - dates
        }
        if missing_label or missing_candidates:
            raise EvaluationInputError(
                "strict_full date coverage is incomplete",
                context={
                    "missing_label_dates": [item.isoformat() for item in missing_label],
                    "missing_candidate_dates": {
                        key: [item.isoformat() for item in value]
                        for key, value in sorted(missing_candidates.items())
                    },
                },
            )
        evaluation_dates = tuple(sorted(prediction_dates))
    else:
        evaluation_dates = tuple(sorted(common))

    common_set = set(evaluation_dates)
    return DateCoveragePlan(
        evaluation_dates=evaluation_dates,
        prediction_dates=tuple(sorted(prediction_dates)),
        label_dates=tuple(sorted(label_dates)),
        common_dates=tuple(sorted(common)),
        dropped_prediction_dates=tuple(sorted(set(prediction_dates) - common_set)),
        dropped_label_dates=tuple(sorted(set(label_dates) - common_set)),
        dropped_candidate_dates={
            candidate_id: tuple(sorted(dates - common_set))
            for candidate_id, dates in sorted(candidate_dates.items())
        },
    )


def evaluate_candidate(
    *,
    candidate_id: str,
    predictions: pd.DataFrame,
    labels: pd.DataFrame,
    coefficients: CandidateCoefficients,
    evaluation_dates: Sequence[date],
    label_horizon_days: int,
    topk: int,
    db_forward_returns: pd.DataFrame | None = None,
    market_forward_return_mode: str = "required",
    date_coverage_evidence: Mapping[str, Any] | None = None,
    checkpoint: Callable[[int, date], None] | None = None,
) -> EvaluationComputation:
    """Evaluate one candidate with deterministic, day-weighted semantics."""

    if not str(candidate_id or "").strip():
        raise EvaluationInputError("candidate_id is required")
    if topk < 1:
        raise EvaluationInputError("topk must be at least one")
    if not 1 <= label_horizon_days <= 30:
        raise EvaluationInputError("label_horizon_days must be between one and thirty")
    if market_forward_return_mode not in {"required", "disabled"}:
        raise EvaluationInputError(
            "market_forward_return mode must be required or disabled",
            context={"mode": market_forward_return_mode},
        )

    frozen_dates = tuple(sorted(set(evaluation_dates)))
    if not frozen_dates:
        raise EvaluationInputError("evaluation_dates cannot be empty")
    missing_coeff_dates = [item for item in frozen_dates if item not in coefficients.daily_coefficients]
    if missing_coeff_dates:
        raise EvaluationInputError(
            "candidate coefficient dates do not cover the frozen evaluation dates",
            context={"dates": [item.isoformat() for item in missing_coeff_dates]},
        )

    pred, pred_warning = _normalize_predictions(predictions, frozen_dates)
    label, label_warning = _normalize_labels(labels, frozen_dates, label_horizon_days)
    db_returns, db_warning = _normalize_db_returns(db_forward_returns, frozen_dates)
    if market_forward_return_mode == "required" and db_forward_returns is None:
        raise MarketDataUnavailableError(
            "market forward returns are required but were not supplied",
        )

    warnings: list[dict[str, Any]] = []
    for warning in (pred_warning, label_warning, db_warning):
        if warning is not None:
            warnings.append(warning)
    if date_coverage_evidence and _coverage_evidence_degraded(date_coverage_evidence):
        warnings.append(
            {
                "code": "hmm_evolution_common_date_intersection",
                "message": "input dates were reduced to a common batch intersection",
                "context": dict(date_coverage_evidence),
            }
        )

    label_lookup = _value_lookup(label, "future_return")
    db_lookup = _value_lookup(db_returns, "future_return") if db_returns is not None else {}
    replacement_rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    missing_sector_occurrences = 0
    missing_sector_symbols: set[str] = set()
    missing_coefficient_occurrences = 0
    missing_coefficient_pairs: set[tuple[date, str]] = set()
    neutral_coefficient_replacements = 0
    coefficient_values: list[float] = []

    for index, trade_date in enumerate(frozen_dates, start=1):
        if checkpoint is not None and (index == 1 or index % 20 == 0 or index == len(frozen_dates)):
            checkpoint(index, trade_date)
        day_pred = pred.loc[pred["trade_date"] == trade_date, ["symbol", "score"]].copy()
        if day_pred.empty:
            raise EvaluationInputError(
                "prediction data is missing for a frozen evaluation date",
                context={"date": trade_date.isoformat()},
            )
        day_coefficients = coefficients.daily_coefficients[trade_date]
        day_pred["sector_code"] = day_pred["symbol"].map(coefficients.stock_sector_map)
        day_pred["fallback_reason"] = None
        missing_sector_mask = day_pred["sector_code"].isna()
        if missing_sector_mask.any():
            missing_symbols = set(day_pred.loc[missing_sector_mask, "symbol"].astype(str))
            missing_sector_symbols.update(missing_symbols)
            missing_sector_occurrences += int(missing_sector_mask.sum())
            day_pred.loc[missing_sector_mask, "fallback_reason"] = "missing_sector"

        day_pred["coefficient"] = 1.0
        for row_index, row in day_pred.loc[~missing_sector_mask].iterrows():
            sector_code = str(row["sector_code"])
            raw_coefficient = day_coefficients.get(sector_code)
            if raw_coefficient is None:
                missing_coefficient_occurrences += 1
                missing_coefficient_pairs.add((trade_date, sector_code))
                day_pred.at[row_index, "fallback_reason"] = "missing_coefficient"
                continue
            coefficient = float(raw_coefficient)
            day_pred.at[row_index, "coefficient"] = coefficient
            coefficient_values.append(coefficient)

        day_pred["adjusted_score"] = day_pred["score"] * day_pred["coefficient"]
        raw_ranked = _rank_frame(day_pred, "score", "raw_rank")
        adjusted_ranked = _rank_frame(day_pred, "adjusted_score", "adjusted_rank")
        raw_rank = raw_ranked.set_index("symbol")["raw_rank"].to_dict()
        adjusted_rank = adjusted_ranked.set_index("symbol")["adjusted_rank"].to_dict()
        raw_top = set(raw_ranked.head(topk)["symbol"])
        adjusted_top = set(adjusted_ranked.head(topk)["symbol"])
        entered = sorted(adjusted_top - raw_top)
        dropped = sorted(raw_top - adjusted_top)

        day_replacements: list[dict[str, Any]] = []
        indexed = day_pred.set_index("symbol")
        for replacement_type, symbols in (("entered_by_hmm", entered), ("dropped_by_hmm", dropped)):
            for symbol in symbols:
                row = indexed.loc[symbol]
                fallback_reason = row["fallback_reason"]
                if isinstance(fallback_reason, str):
                    neutral_coefficient_replacements += 1
                record = {
                    "date": trade_date.isoformat(),
                    "symbol": symbol,
                    "replacement_type": replacement_type,
                    "sector_code": None if pd.isna(row["sector_code"]) else str(row["sector_code"]),
                    "coefficient": float(row["coefficient"]),
                    "fallback_reason": fallback_reason if isinstance(fallback_reason, str) else None,
                    "raw_score": float(row["score"]),
                    "adjusted_score": float(row["adjusted_score"]),
                    "raw_rank": int(raw_rank[symbol]),
                    "adjusted_rank": int(adjusted_rank[symbol]),
                    "rank_delta": int(adjusted_rank[symbol]) - int(raw_rank[symbol]),
                    "label_return": label_lookup.get((trade_date, symbol)),
                    "db_return_10d": db_lookup.get((trade_date, symbol)),
                }
                replacement_rows.append(record)
                day_replacements.append(record)

        entered_rows = [item for item in day_replacements if item["replacement_type"] == "entered_by_hmm"]
        dropped_rows = [item for item in day_replacements if item["replacement_type"] == "dropped_by_hmm"]
        daily_net_label, entered_label_count, dropped_label_count = _daily_net(
            entered_rows, dropped_rows, "label_return"
        )
        daily_net_db, entered_db_count, dropped_db_count = _daily_net(
            entered_rows, dropped_rows, "db_return_10d"
        )
        daily_rows.append(
            {
                "date": trade_date.isoformat(),
                "raw_top_count": len(raw_top),
                "adjusted_top_count": len(adjusted_top),
                "common_count": len(raw_top & adjusted_top),
                "entered_count": len(entered),
                "dropped_count": len(dropped),
                "replacement_count": len(entered) + len(dropped),
                "entered_label_count": entered_label_count,
                "dropped_label_count": dropped_label_count,
                "daily_net_label": daily_net_label,
                "entered_db_count": entered_db_count,
                "dropped_db_count": dropped_db_count,
                "daily_net_db_10d": daily_net_db,
            }
        )

    if missing_sector_occurrences:
        warnings.append(
            {
                "code": "hmm_evolution_missing_sector_neutral_fallback",
                "message": "symbols without sector mapping used neutral coefficient 1.0",
                "context": {
                    "occurrence_count": missing_sector_occurrences,
                    "unique_symbol_count": len(missing_sector_symbols),
                },
            }
        )
    if missing_coefficient_occurrences:
        warnings.append(
            {
                "code": "hmm_evolution_missing_coefficient_neutral_fallback",
                "message": "sectors without daily coefficients used neutral coefficient 1.0",
                "context": {
                    "occurrence_count": missing_coefficient_occurrences,
                    "unique_date_sector_count": len(missing_coefficient_pairs),
                },
            }
        )

    changed_rows = [item for item in daily_rows if item["replacement_count"] > 0]
    label_comparable = [item for item in changed_rows if item["daily_net_label"] is not None]
    db_comparable = [item for item in changed_rows if item["daily_net_db_10d"] is not None]
    changed_day_count = len(changed_rows)
    label_coverage = len(label_comparable) / changed_day_count if changed_day_count else None
    db_coverage = len(db_comparable) / changed_day_count if changed_day_count else None

    if changed_day_count and label_coverage is not None and label_coverage < 1:
        warnings.append(
            {
                "code": "hmm_evolution_partial_label_coverage",
                "message": "some changed days lack comparable entered and dropped labels",
                "context": {"coverage_ratio": label_coverage},
            }
        )
    if market_forward_return_mode == "required" and changed_day_count and not db_comparable:
        raise MarketDataUnavailableError(
            "market forward returns produced no comparable changed days",
            context={"changed_day_count": changed_day_count},
        )
    if (
        market_forward_return_mode == "required"
        and changed_day_count
        and db_coverage is not None
        and db_coverage < 1
    ):
        warnings.append(
            {
                "code": "hmm_evolution_partial_market_return_coverage",
                "message": "some changed days lack comparable entered and dropped market returns",
                "context": {"coverage_ratio": db_coverage},
            }
        )

    net_label_return = _optional_mean(item["daily_net_label"] for item in label_comparable)
    net_db_10d = (
        _optional_mean(item["daily_net_db_10d"] for item in db_comparable)
        if market_forward_return_mode == "required"
        else None
    )
    positive_net_label_day_ratio = (
        sum(1 for item in label_comparable if item["daily_net_label"] > 0) / len(label_comparable)
        if label_comparable
        else None
    )
    if changed_day_count and not label_comparable:
        evidence_quality = EvidenceQuality.INSUFFICIENT
    elif warnings:
        evidence_quality = EvidenceQuality.DEGRADED
    elif not changed_day_count:
        evidence_quality = EvidenceQuality.INSUFFICIENT
    else:
        evidence_quality = EvidenceQuality.COMPLETE

    durable_samples = sorted(
        replacement_rows,
        key=lambda item: (-abs(int(item["rank_delta"])), str(item["date"]), str(item["symbol"])),
    )[:100]
    missing_sector_occurrence_ratio = missing_sector_occurrences / len(pred)
    mapped_occurrence_count = len(pred) - missing_sector_occurrences
    missing_coefficient_occurrence_ratio: float | None = None
    if mapped_occurrence_count > 0:
        missing_coefficient_occurrence_ratio = (
            missing_coefficient_occurrences / mapped_occurrence_count
        )
    neutral_coefficient_replacement_ratio: float | None = None
    if replacement_rows:
        neutral_coefficient_replacement_ratio = (
            neutral_coefficient_replacements / len(replacement_rows)
        )
    coefficient_min: float | None = None
    coefficient_max: float | None = None
    if coefficient_values:
        coefficient_min = min(coefficient_values)
        coefficient_max = max(coefficient_values)
    db_comparable_day_count = 0
    db_day_coverage_ratio: float | None = None
    if market_forward_return_mode == "required":
        db_comparable_day_count = len(db_comparable)
        db_day_coverage_ratio = db_coverage
    metrics = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "metric_version": METRIC_VERSION,
        "candidate_id": candidate_id,
        "label_horizon_days": label_horizon_days,
        "market_forward_return_mode": market_forward_return_mode,
        "trading_days_count": len(frozen_dates),
        "changed_day_count": changed_day_count,
        "label_comparable_day_count": len(label_comparable),
        "db_comparable_day_count": db_comparable_day_count,
        "replacement_count": len(replacement_rows),
        "label_day_coverage_ratio": label_coverage,
        "db_day_coverage_ratio": db_day_coverage_ratio,
        "primary_coverage_ratio": label_coverage,
        "net_label_return": net_label_return,
        "net_db_10d": net_db_10d,
        "positive_net_label_day_ratio": positive_net_label_day_ratio,
        "missing_sector_occurrence_count": missing_sector_occurrences,
        "missing_sector_unique_symbol_count": len(missing_sector_symbols),
        "missing_sector_occurrence_ratio": missing_sector_occurrence_ratio,
        "missing_coefficient_occurrence_count": missing_coefficient_occurrences,
        "missing_coefficient_unique_date_sector_count": len(missing_coefficient_pairs),
        "missing_coefficient_occurrence_ratio": missing_coefficient_occurrence_ratio,
        "neutral_coefficient_replacement_count": neutral_coefficient_replacements,
        "neutral_coefficient_replacement_ratio": neutral_coefficient_replacement_ratio,
        "coefficient_min": coefficient_min,
        "coefficient_max": coefficient_max,
        "daily_summary": daily_rows,
        "replacement_samples": durable_samples,
    }
    result_hash = _result_hash(
        {
            "metrics": metrics,
            "warnings": warnings,
            "evidence_quality": evidence_quality.value,
        }
    )
    result = {
        "trading_days_count": len(frozen_dates),
        "changed_day_count": changed_day_count,
        "label_comparable_day_count": len(label_comparable),
        "db_comparable_day_count": db_comparable_day_count,
        "replacement_count": len(replacement_rows),
        "primary_coverage_ratio": label_coverage,
        "net_label_return": net_label_return,
        "net_db_10d": net_db_10d,
        "positive_net_label_day_ratio": positive_net_label_day_ratio,
        "evidence_quality": evidence_quality.value,
        "warnings_json": warnings,
        "metrics_json": metrics,
        "result_hash": result_hash,
    }
    return EvaluationComputation(
        result=result,
        replacement_rows=tuple(replacement_rows),
        daily_summary=tuple(daily_rows),
    )


def _frame_dates(frame: pd.DataFrame, label: str, start: date, end: date) -> set[date]:
    if "trade_date" not in frame.columns:
        raise EvaluationInputError(f"{label} must contain trade_date")
    parsed = pd.to_datetime(frame["trade_date"], errors="coerce")
    if parsed.isna().any():
        raise EvaluationInputError(f"{label} contains invalid trade_date values")
    return {item.date() for item in parsed if start <= item.date() <= end}


def _normalize_predictions(
    frame: pd.DataFrame,
    evaluation_dates: Sequence[date],
) -> tuple[pd.DataFrame, dict[str, Any] | None]:
    required = {"trade_date", "symbol", "score"}
    if not required <= set(frame.columns):
        raise EvaluationInputError(
            "predictions are missing required columns",
            context={"missing": sorted(required - set(frame.columns))},
        )
    result = frame.loc[:, ["trade_date", "symbol", "score"]].copy()
    result["trade_date"] = pd.to_datetime(result["trade_date"], errors="coerce").dt.date
    if result["trade_date"].isna().any():
        raise EvaluationInputError("predictions contain invalid trade_date values")
    result["symbol"] = result["symbol"].astype(str).str.strip()
    if (result["symbol"] == "").any():
        raise EvaluationInputError("predictions contain empty symbols")
    result = result.loc[result["trade_date"].isin(evaluation_dates)].copy()
    numeric = pd.to_numeric(result["score"], errors="coerce")
    finite_mask = numeric.map(lambda value: value is not None and math.isfinite(float(value)))
    dropped_count = int((~finite_mask).sum())
    result = result.loc[finite_mask].copy()
    result["score"] = numeric.loc[finite_mask].astype(float)
    if result.duplicated(["trade_date", "symbol"]).any():
        raise EvaluationInputError("predictions contain duplicate date/symbol rows")
    missing_dates = sorted(set(evaluation_dates) - set(result["trade_date"]))
    if missing_dates:
        raise EvaluationInputError(
            "predictions have no finite scores for some evaluation dates",
            context={"dates": [item.isoformat() for item in missing_dates]},
        )
    warning = None
    if dropped_count:
        warning = {
            "code": "hmm_evolution_non_finite_prediction_scores_excluded",
            "message": "non-finite prediction scores were excluded",
            "context": {"row_count": dropped_count},
        }
    return result, warning


def _normalize_labels(
    frame: pd.DataFrame,
    evaluation_dates: Sequence[date],
    expected_horizon: int,
) -> tuple[pd.DataFrame, dict[str, Any] | None]:
    required = {"trade_date", "symbol", "horizon_days", "future_return"}
    if not required <= set(frame.columns):
        raise EvaluationInputError(
            "labels are missing required columns",
            context={"missing": sorted(required - set(frame.columns))},
        )
    result = frame.loc[:, ["trade_date", "symbol", "horizon_days", "future_return"]].copy()
    result["trade_date"] = pd.to_datetime(result["trade_date"], errors="coerce").dt.date
    if result["trade_date"].isna().any():
        raise EvaluationInputError("labels contain invalid trade_date values")
    result["symbol"] = result["symbol"].astype(str).str.strip()
    horizons = {int(item) for item in result["horizon_days"].dropna().unique()}
    if horizons != {expected_horizon}:
        raise LabelHorizonMismatchError(
            "label horizon does not match the evaluation request",
            context={
                "expected_horizon": expected_horizon,
                "observed_horizons": sorted(horizons),
            },
        )
    result = result.loc[result["trade_date"].isin(evaluation_dates)].copy()
    numeric = pd.to_numeric(result["future_return"], errors="coerce")
    finite_mask = numeric.map(lambda value: value is not None and math.isfinite(float(value)))
    dropped_count = int((~finite_mask).sum())
    result = result.loc[finite_mask].copy()
    result["future_return"] = numeric.loc[finite_mask].astype(float)
    if result.duplicated(["trade_date", "symbol"]).any():
        raise EvaluationInputError("labels contain duplicate date/symbol rows")
    warning = None
    if dropped_count:
        warning = {
            "code": "hmm_evolution_non_finite_labels_excluded",
            "message": "non-finite labels were excluded",
            "context": {"row_count": dropped_count},
        }
    return result, warning


def _normalize_db_returns(
    frame: pd.DataFrame | None,
    evaluation_dates: Sequence[date],
) -> tuple[pd.DataFrame | None, dict[str, Any] | None]:
    if frame is None:
        return None, None
    required = {"trade_date", "symbol", "future_return"}
    if not required <= set(frame.columns):
        raise EvaluationInputError(
            "market forward returns are missing required columns",
            context={"missing": sorted(required - set(frame.columns))},
        )
    result = frame.loc[:, ["trade_date", "symbol", "future_return"]].copy()
    result["trade_date"] = pd.to_datetime(result["trade_date"], errors="coerce").dt.date
    if result["trade_date"].isna().any():
        raise EvaluationInputError("market forward returns contain invalid trade_date values")
    result["symbol"] = result["symbol"].astype(str).str.strip()
    result = result.loc[result["trade_date"].isin(evaluation_dates)].copy()
    numeric = pd.to_numeric(result["future_return"], errors="coerce")
    finite_mask = numeric.map(lambda value: value is not None and math.isfinite(float(value)))
    dropped_count = int((~finite_mask).sum())
    result = result.loc[finite_mask].copy()
    result["future_return"] = numeric.loc[finite_mask].astype(float)
    if result.duplicated(["trade_date", "symbol"]).any():
        raise EvaluationInputError("market forward returns contain duplicate date/symbol rows")
    warning = None
    if dropped_count:
        warning = {
            "code": "hmm_evolution_non_finite_market_returns_excluded",
            "message": "non-finite market forward returns were excluded",
            "context": {"row_count": dropped_count},
        }
    return result, warning


def _rank_frame(frame: pd.DataFrame, score_column: str, rank_column: str) -> pd.DataFrame:
    ranked = frame.sort_values(
        [score_column, "symbol"],
        ascending=[False, True],
        kind="mergesort",
    ).copy()
    ranked[rank_column] = range(1, len(ranked) + 1)
    return ranked


def _value_lookup(frame: pd.DataFrame | None, value_column: str) -> dict[tuple[date, str], float]:
    if frame is None or frame.empty:
        return {}
    return {
        (row.trade_date, str(row.symbol)): float(getattr(row, value_column))
        for row in frame.itertuples(index=False)
    }


def _daily_net(
    entered: Sequence[Mapping[str, Any]],
    dropped: Sequence[Mapping[str, Any]],
    field: str,
) -> tuple[float | None, int, int]:
    entered_values = [float(item[field]) for item in entered if item.get(field) is not None]
    dropped_values = [float(item[field]) for item in dropped if item.get(field) is not None]
    if not entered_values or not dropped_values:
        return None, len(entered_values), len(dropped_values)
    return fmean(entered_values) - fmean(dropped_values), len(entered_values), len(dropped_values)


def _optional_mean(values: Iterable[float | None]) -> float | None:
    finite_values = [float(value) for value in values if value is not None]
    return fmean(finite_values) if finite_values else None


def _coverage_evidence_degraded(evidence: Mapping[str, Any]) -> bool:
    return bool(
        evidence.get("dropped_prediction_dates")
        or evidence.get("dropped_label_dates")
        or any((evidence.get("dropped_candidate_dates") or {}).values())
    )


def _result_hash(payload: Mapping[str, Any]) -> str:
    return canonical_json_sha256(_normalize_result_floats(payload))


def _normalize_result_floats(value: Any) -> Any:
    if isinstance(value, float):
        if not math.isfinite(value):
            raise EvaluationInputError("evaluation result contains a non-finite float")
        return format(value, ".12g")
    if isinstance(value, Mapping):
        return {str(key): _normalize_result_floats(nested) for key, nested in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_result_floats(nested) for nested in value]
    return value
