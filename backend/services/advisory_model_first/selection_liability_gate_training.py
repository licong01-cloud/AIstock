from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error

from backend.services.advisory_model_first.dual_head_output_constraint_training import (
    LIABILITY_SCORE_COLUMN,
    LIABILITY_TARGET_COLUMN,
    verify_liability_head_predictions,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.selection_liability_gate_contracts import (
    SELECTION_LIABILITY_GATE_THRESHOLDS,
)


ENTRY_PRIORITY_COLUMN = "entry_priority_rank"


@dataclass(frozen=True)
class GatePriorityResult:
    priorities: pd.DataFrame
    eligible_count_by_date: dict[str, int]
    rejected_count: int
    rejection_rate: float


@dataclass(frozen=True)
class GateThresholdSelection:
    maximum_liability_threshold: float
    p0d_oof_turnover_budget: float
    p0k_oof_turnover: float
    constraint_slack: float
    evaluations: tuple[dict[str, Any], ...]


def build_selection_preserving_gate_priorities(
    predictions: pd.DataFrame,
    *,
    maximum_liability_threshold: float,
) -> GatePriorityResult:
    verify_liability_head_predictions(predictions)
    threshold = float(maximum_liability_threshold)
    if not np.isfinite(threshold) or threshold not in SELECTION_LIABILITY_GATE_THRESHOLDS:
        raise _error("liability-gate threshold is outside the frozen physical roster")
    rows = predictions.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()
    rows["instrument"] = rows["instrument"].astype(str).str.upper()
    scores = pd.to_numeric(rows[LIABILITY_SCORE_COLUMN], errors="coerce").to_numpy(float)
    if not np.isfinite(scores).all() or (scores < 0.02).any() or (scores > 0.4).any():
        raise _error("liability-gate predictions exceed frozen physical bounds")
    ranks = pd.to_numeric(rows["selection_effective_rank"], errors="coerce")
    if ranks.isna().any() or not np.array_equal(ranks.to_numpy(float), np.rint(ranks.to_numpy(float))):
        raise _error("liability-gate Selection ranks are not integral")
    rows["selection_effective_rank"] = ranks.astype(int)
    expected = tuple(range(1, 21))
    observed = rows.groupby("decision_as_of_trade_date")["selection_effective_rank"].apply(
        lambda values: tuple(sorted(values.tolist()))
    )
    if not observed.map(lambda values: values == expected).all():
        raise _error("liability-gate predictions do not preserve exact Selection Top20 ranks")
    eligible = rows[pd.to_numeric(rows[LIABILITY_SCORE_COLUMN], errors="coerce") <= threshold].copy()
    eligible = eligible.sort_values(
        ["decision_as_of_trade_date", "selection_effective_rank", "instrument"],
        ascending=[True, True, True],
    )
    eligible[ENTRY_PRIORITY_COLUMN] = eligible.groupby("decision_as_of_trade_date").cumcount().add(1)
    counts = eligible.groupby("decision_as_of_trade_date").size()
    all_dates = pd.DatetimeIndex(sorted(rows["decision_as_of_trade_date"].unique())).normalize()
    by_date = {
        value.date().isoformat(): int(counts.get(value, 0))
        for value in all_dates
    }
    columns = [
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "selection_effective_rank",
        LIABILITY_SCORE_COLUMN,
        ENTRY_PRIORITY_COLUMN,
    ]
    rejected = len(rows) - len(eligible)
    return GatePriorityResult(
        priorities=eligible.loc[:, columns].reset_index(drop=True),
        eligible_count_by_date=by_date,
        rejected_count=rejected,
        rejection_rate=float(rejected / len(rows)),
    )


def assert_widest_gate_matches_selection(
    gate_priorities: pd.DataFrame,
    selection_priorities: pd.DataFrame,
) -> None:
    keys = ["decision_as_of_trade_date", "instrument", ENTRY_PRIORITY_COLUMN]
    if not set(keys).issubset(gate_priorities) or not set(keys).issubset(selection_priorities):
        raise _error("widest-threshold equivalence inputs are incomplete")
    left = gate_priorities.loc[:, keys].copy()
    right = selection_priorities.loc[:, keys].copy()
    for frame in (left, right):
        frame["decision_as_of_trade_date"] = pd.to_datetime(
            frame["decision_as_of_trade_date"]
        ).dt.normalize()
        frame["instrument"] = frame["instrument"].astype(str).str.upper()
        frame[ENTRY_PRIORITY_COLUMN] = pd.to_numeric(
            frame[ENTRY_PRIORITY_COLUMN], errors="raise"
        ).astype(int)
        frame.sort_values(keys, inplace=True)
        frame.reset_index(drop=True, inplace=True)
    if not left.equals(right):
        raise AdvisoryModelFirstError(
            "widest liability threshold does not exactly reproduce matched Selection priorities",
            reason_code="ADVISORY_P0K_SELECTION_EQUIVALENCE_FAILED",
        )


def assert_widest_gate_metrics_match_selection(
    gate_metrics: Mapping[str, Any],
    selection_metrics: Mapping[str, Any],
) -> None:
    numeric = ("mean_turnover_fraction", "active_slot_coverage")
    exact = ("cash_day_count", "day_count")
    if any(
        abs(float(gate_metrics[key]) - float(selection_metrics[key])) > 1e-15
        for key in numeric
    ) or any(gate_metrics[key] != selection_metrics[key] for key in exact):
        raise AdvisoryModelFirstError(
            "widest liability threshold does not exactly reproduce matched Selection metrics",
            reason_code="ADVISORY_P0K_SELECTION_EQUIVALENCE_FAILED",
        )
    gate_daily = _daily_completeness_by_date(gate_metrics)
    selection_daily = _daily_completeness_by_date(selection_metrics)
    if gate_daily != selection_daily:
        raise AdvisoryModelFirstError(
            "widest liability threshold does not exactly reproduce matched Selection daily state",
            reason_code="ADVISORY_P0K_SELECTION_EQUIVALENCE_FAILED",
        )


def liability_gate_completeness_not_worse(
    gate_metrics: Mapping[str, Any],
    selection_metrics: Mapping[str, Any],
) -> bool:
    if (
        float(gate_metrics["active_slot_coverage"]) + 1e-15
        < float(selection_metrics["active_slot_coverage"])
        or int(gate_metrics["cash_day_count"]) > int(selection_metrics["cash_day_count"])
    ):
        return False
    gate_daily = _daily_completeness_by_date(gate_metrics)
    selection_daily = _daily_completeness_by_date(selection_metrics)
    if set(gate_daily) != set(selection_daily):
        return False
    return all(
        gate_daily[date][0] >= selection_daily[date][0]
        and gate_daily[date][1] <= selection_daily[date][1]
        for date in selection_daily
    )


def select_widest_feasible_liability_threshold(
    *,
    predictions: pd.DataFrame,
    thresholds: Sequence[float],
    p0d_oof_turnover_budget: float,
    evaluate: Callable[[pd.DataFrame], Mapping[str, Any]],
    target_count: int = 5,
) -> GateThresholdSelection:
    budget = float(p0d_oof_turnover_budget)
    if not np.isfinite(budget) or budget < 0.0:
        raise _error("exact P0-D OOF turnover budget is invalid")
    if target_count != 5:
        raise _error("liability-gate target count differs from frozen Top5 policy")
    if tuple(float(value) for value in thresholds) != SELECTION_LIABILITY_GATE_THRESHOLDS:
        raise _error("liability-gate selector threshold roster differs from frozen physical order")
    observed: list[dict[str, Any]] = []
    for threshold in thresholds:
        gate = build_selection_preserving_gate_priorities(
            predictions,
            maximum_liability_threshold=float(threshold),
        )
        minimum_depth = min(gate.eligible_count_by_date.values(), default=0)
        receipt: dict[str, Any] = {
            "maximum_liability_threshold": float(threshold),
            "minimum_eligible_candidate_count": int(minimum_depth),
            "rejected_count": gate.rejected_count,
            "rejection_rate": gate.rejection_rate,
        }
        if minimum_depth < target_count:
            receipt.update({"feasible": False, "reason": "CANDIDATE_DEPTH_BELOW_TARGET"})
            observed.append(receipt)
            continue
        metrics = dict(evaluate(gate.priorities))
        turnover = float(metrics.get("mean_turnover_fraction", float("nan")))
        complete = bool(metrics.get("complete", False))
        if not np.isfinite(turnover):
            raise _error("liability-gate OOF turnover is non-finite")
        receipt.update(metrics)
        receipt["feasible"] = bool(complete and turnover <= budget + 1e-15)
        receipt["constraint_slack"] = budget - turnover
        observed.append(receipt)
        if receipt["feasible"]:
            return GateThresholdSelection(
                maximum_liability_threshold=float(threshold),
                p0d_oof_turnover_budget=budget,
                p0k_oof_turnover=turnover,
                constraint_slack=budget - turnover,
                evaluations=tuple(observed),
            )
    raise AdvisoryModelFirstError(
        "no frozen physical liability threshold satisfies completeness and exact P0-D turnover",
        reason_code="ADVISORY_P0K_LIABILITY_GATE_INFEASIBLE",
        context={"threshold_evaluations": observed, "p0d_oof_turnover_budget": budget},
    )


def selection_liability_gate_candidate_metrics(
    predictions: pd.DataFrame,
    *,
    maximum_liability_threshold: float,
) -> dict[str, Any]:
    required = {
        "decision_as_of_trade_date",
        "label_status",
        "net_excess_return_bps",
        LIABILITY_TARGET_COLUMN,
        LIABILITY_SCORE_COLUMN,
    }
    if not required.issubset(predictions):
        raise _error("liability-gate diagnostic columns are incomplete")
    matured = predictions[predictions["label_status"] == "MATURED"].copy()
    if matured.empty:
        raise _error("liability-gate diagnostics have no matured rows")
    actual = pd.to_numeric(matured[LIABILITY_TARGET_COLUMN], errors="coerce").to_numpy(float)
    score = pd.to_numeric(matured[LIABILITY_SCORE_COLUMN], errors="coerce").to_numpy(float)
    returns = pd.to_numeric(matured["net_excess_return_bps"], errors="coerce").to_numpy(float)
    if not np.isfinite(actual).all() or not np.isfinite(score).all() or not np.isfinite(returns).all():
        raise _error("liability-gate diagnostics contain non-finite values")
    daily_spearman = matured.groupby("decision_as_of_trade_date", sort=True).apply(
        lambda group: group[LIABILITY_SCORE_COLUMN].corr(
            group[LIABILITY_TARGET_COLUMN], method="spearman"
        ),
        include_groups=False,
    )
    accepted = score <= float(maximum_liability_threshold)
    return {
        "liability_mae": float(mean_absolute_error(actual, score)),
        "liability_rmse": float(mean_squared_error(actual, score) ** 0.5),
        "liability_daily_spearman_mean": _finite_mean(daily_spearman),
        "liability_daily_spearman_null_count": int((~np.isfinite(daily_spearman)).sum()),
        "accepted_candidate_count": int(accepted.sum()),
        "rejected_candidate_count": int((~accepted).sum()),
        "accepted_candidate_mean_return_bps_diagnostic_only": (
            float(returns[accepted].mean()) if accepted.any() else None
        ),
        "rejected_candidate_mean_return_bps_diagnostic_only": (
            float(returns[~accepted].mean()) if (~accepted).any() else None
        ),
        "liability_clip_low_count": int((score <= 0.02).sum()),
        "liability_clip_high_count": int((score >= 0.4).sum()),
    }


def _finite_mean(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce")
    finite = numeric[np.isfinite(numeric)]
    return float(finite.mean()) if len(finite) else None


def _daily_completeness_by_date(metrics: Mapping[str, Any]) -> dict[str, tuple[int, int, float]]:
    rows = metrics.get("daily_completeness")
    if not isinstance(rows, list):
        raise _error("liability-gate completeness metrics omit daily records")
    result: dict[str, tuple[int, int, float]] = {}
    for item in rows:
        date = str(item["decision_as_of_trade_date"])
        if date in result:
            raise _error("liability-gate completeness metrics duplicate a decision date")
        result[date] = (
            int(item["active_count"]),
            int(item["cash_slot_count"]),
            float(item["turnover_fraction"]),
        )
    return result


def _error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_P0K_LIABILITY_GATE_INVALID",
        context=context or None,
    )
