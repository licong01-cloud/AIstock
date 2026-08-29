from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.dual_head_output_constraint_training import (
    LIABILITY_SCORE_COLUMN,
    verify_liability_head_predictions,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.p0g_anchored_liability_local_reranker_contracts import (
    P0L_GAIN_ROSTER,
)
from backend.services.advisory_model_first.turnover_constrained_utility_training import SCORE_COLUMN


ANCHOR_RANK_COLUMN = "anchor_entry_priority_rank"
LIABILITY_RANK_COLUMN = "predicted_liability_rank"
ENTRY_PRIORITY_COLUMN = "entry_priority_rank"
SCORE_KIND = "P0G_ANCHORED_RELATIVE_LIABILITY_LOCAL_RERANK_V1"


@dataclass(frozen=True)
class LocalRerankResult:
    priorities: pd.DataFrame
    liability_rank_gain_required: int | None
    changed_decision_count: int
    changed_candidate_row_count: int
    top5_boundary_change_count: int
    selected_swaps: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class LocalRerankSelection:
    liability_rank_gain_required: int
    p0d_oof_turnover_budget: float
    p0l_oof_turnover: float
    constraint_slack: float
    actual_entry_change_count: int
    evaluations: tuple[dict[str, Any], ...]


def build_local_rerank_priorities(
    anchor_predictions: pd.DataFrame,
    liability_predictions: pd.DataFrame,
    *,
    liability_rank_gain_required: int | None,
    target_count: int = 5,
) -> LocalRerankResult:
    if target_count != 5:
        raise _priority_error("P0-L target count differs from frozen Top5 policy")
    if liability_rank_gain_required is not None and liability_rank_gain_required not in P0L_GAIN_ROSTER:
        raise _priority_error("P0-L gain is outside the frozen roster")
    anchor = _normalize_anchor(anchor_predictions)
    liability = _normalize_liability(liability_predictions)
    keys = [
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "selection_effective_rank",
    ]
    rows = anchor.merge(
        liability[keys + [LIABILITY_SCORE_COLUMN]],
        on=keys,
        how="inner",
        validate="one_to_one",
    )
    if len(rows) != len(anchor) or len(rows) != len(liability):
        raise _priority_error("P0-L anchor/liability prediction identities differ")
    liability_order = rows.sort_values(
        ["decision_as_of_trade_date", LIABILITY_SCORE_COLUMN, ANCHOR_RANK_COLUMN, "instrument"],
        ascending=[True, True, True, True],
    ).copy()
    liability_order[LIABILITY_RANK_COLUMN] = (
        liability_order.groupby("decision_as_of_trade_date").cumcount().add(1)
    )
    rows = rows.merge(
        liability_order[
            ["decision_as_of_trade_date", "instrument", LIABILITY_RANK_COLUMN]
        ],
        on=["decision_as_of_trade_date", "instrument"],
        how="inner",
        validate="one_to_one",
    )
    rows[ENTRY_PRIORITY_COLUMN] = rows[ANCHOR_RANK_COLUMN]
    swaps: list[dict[str, Any]] = []
    if liability_rank_gain_required is not None:
        for decision_date, group in rows.groupby("decision_as_of_trade_date", sort=True):
            by_anchor = group.set_index(ANCHOR_RANK_COLUMN, drop=False)
            candidates: list[tuple[int, int, str, str]] = []
            for upper_rank in range(1, target_count + 1):
                upper = by_anchor.loc[upper_rank]
                lower = by_anchor.loc[upper_rank + 1]
                gain = int(upper[LIABILITY_RANK_COLUMN] - lower[LIABILITY_RANK_COLUMN])
                if gain >= liability_rank_gain_required:
                    candidates.append(
                        (gain, upper_rank, str(upper["instrument"]), str(lower["instrument"]))
                    )
            if not candidates:
                continue
            gain, upper_rank, upper_instrument, lower_instrument = sorted(
                candidates,
                key=lambda item: (-item[0], item[1], item[2], item[3]),
            )[0]
            upper_mask = (rows["decision_as_of_trade_date"] == decision_date) & (
                rows["instrument"] == upper_instrument
            )
            lower_mask = (rows["decision_as_of_trade_date"] == decision_date) & (
                rows["instrument"] == lower_instrument
            )
            if int(upper_mask.sum()) != 1 or int(lower_mask.sum()) != 1:
                raise _priority_error("P0-L selected swap identity is ambiguous")
            rows.loc[upper_mask, ENTRY_PRIORITY_COLUMN] = upper_rank + 1
            rows.loc[lower_mask, ENTRY_PRIORITY_COLUMN] = upper_rank
            swaps.append(
                {
                    "decision_as_of_trade_date": pd.Timestamp(decision_date).date().isoformat(),
                    "upper_anchor_rank": upper_rank,
                    "upper_instrument": upper_instrument,
                    "lower_instrument": lower_instrument,
                    "liability_rank_gain": gain,
                    "changes_top5_boundary": upper_rank == target_count,
                }
            )
    _verify_priorities(rows)
    changed = rows[ENTRY_PRIORITY_COLUMN] != rows[ANCHOR_RANK_COLUMN]
    changed_dates = rows.loc[changed, "decision_as_of_trade_date"].nunique()
    columns = keys + [
        SCORE_COLUMN,
        LIABILITY_SCORE_COLUMN,
        ANCHOR_RANK_COLUMN,
        LIABILITY_RANK_COLUMN,
        ENTRY_PRIORITY_COLUMN,
        "selection_exit_rank",
    ]
    rows["entry_priority_score_kind"] = SCORE_KIND
    columns.append("entry_priority_score_kind")
    return LocalRerankResult(
        priorities=rows.loc[:, columns].sort_values(
            ["decision_as_of_trade_date", ENTRY_PRIORITY_COLUMN, "instrument"]
        ).reset_index(drop=True),
        liability_rank_gain_required=liability_rank_gain_required,
        changed_decision_count=int(changed_dates),
        changed_candidate_row_count=int(changed.sum()),
        top5_boundary_change_count=sum(bool(item["changes_top5_boundary"]) for item in swaps),
        selected_swaps=tuple(swaps),
    )


def assert_identity_control_matches_anchor(
    identity_priorities: pd.DataFrame,
    anchor_priorities: pd.DataFrame,
) -> None:
    keys = ["decision_as_of_trade_date", "instrument", ENTRY_PRIORITY_COLUMN]
    left = _priority_identity(identity_priorities, keys)
    right = _priority_identity(anchor_priorities, keys)
    if not left.equals(right):
        raise AdvisoryModelFirstError(
            "P0-L identity control does not reproduce the P0-G anchor",
            reason_code="ADVISORY_P0L_ANCHOR_IDENTITY_FAILED",
        )


def select_minimum_feasible_gain(
    *,
    anchor_predictions: pd.DataFrame,
    liability_predictions: pd.DataFrame,
    gain_roster: Sequence[int],
    p0d_oof_turnover_budget: float,
    anchor_metrics: Mapping[str, Any],
    evaluate: Callable[[pd.DataFrame], Mapping[str, Any]],
    target_count: int = 5,
) -> LocalRerankSelection:
    roster = tuple(int(value) for value in gain_roster)
    if roster != P0L_GAIN_ROSTER:
        raise _constraint_error("P0-L gain roster differs from the frozen design")
    budget = float(p0d_oof_turnover_budget)
    if not np.isfinite(budget) or budget < 0.0:
        raise _constraint_error("P0-L P0-D OOF turnover budget is invalid")
    identity = build_local_rerank_priorities(
        anchor_predictions,
        liability_predictions,
        liability_rank_gain_required=None,
        target_count=target_count,
    )
    assert_identity_control_matches_anchor(identity.priorities, anchor_predictions)
    identity_metrics = dict(evaluate(identity.priorities))
    _assert_identity_metrics(identity_metrics, anchor_metrics)
    observed: list[dict[str, Any]] = [
        {
            "control_arm": "NO_SWAP_CONTROL_V1",
            "feasible_candidate": False,
            **_receipt_metrics(identity, identity_metrics),
        }
    ]
    for gain in roster:
        result = build_local_rerank_priorities(
            anchor_predictions,
            liability_predictions,
            liability_rank_gain_required=gain,
            target_count=target_count,
        )
        metrics = dict(evaluate(result.priorities))
        turnover = float(metrics.get("mean_turnover_fraction", float("nan")))
        actual_entry_changes = int(metrics.get("actual_entry_change_count", 0))
        complete = bool(metrics.get("complete", False))
        if not np.isfinite(turnover):
            raise _constraint_error("P0-L OOF turnover is non-finite")
        feasible = bool(
            complete
            and actual_entry_changes > 0
            and result.changed_decision_count > 0
            and turnover <= budget + 1e-15
        )
        observed.append(
            {
                "liability_rank_gain_required": gain,
                "feasible_candidate": feasible,
                "constraint_slack": budget - turnover,
                **_receipt_metrics(result, metrics),
            }
        )
        if feasible:
            return LocalRerankSelection(
                liability_rank_gain_required=gain,
                p0d_oof_turnover_budget=budget,
                p0l_oof_turnover=turnover,
                constraint_slack=budget - turnover,
                actual_entry_change_count=actual_entry_changes,
                evaluations=tuple(observed),
            )
    raise AdvisoryModelFirstError(
        "no frozen P0-L local-rerank arm satisfies real-entry and P0-D turnover constraints",
        reason_code="ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE",
        context={"evaluations": observed, "p0d_oof_turnover_budget": budget},
    )


def local_rerank_candidate_metrics(
    predictions: pd.DataFrame,
    *,
    changed_instruments: set[tuple[pd.Timestamp, str]],
) -> dict[str, Any]:
    required = {LIABILITY_SCORE_COLUMN, "net_excess_return_bps", "label_status"}
    if not required.issubset(predictions):
        raise _priority_error("P0-L candidate diagnostic input is incomplete")
    rows = predictions.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(
        rows["decision_as_of_trade_date"]
    ).dt.normalize()
    changed = rows.apply(
        lambda row: (row["decision_as_of_trade_date"], str(row["instrument"]))
        in changed_instruments,
        axis=1,
    )
    matured = rows["label_status"] == "MATURED"
    return {
        "changed_candidate_count": int(changed.sum()),
        "unchanged_candidate_count": int((~changed).sum()),
        "changed_candidate_mean_return_bps_diagnostic_only": _finite_mean(
            rows.loc[changed & matured, "net_excess_return_bps"]
        ),
        "unchanged_candidate_mean_return_bps_diagnostic_only": _finite_mean(
            rows.loc[(~changed) & matured, "net_excess_return_bps"]
        ),
        "liability_prediction_min": float(rows[LIABILITY_SCORE_COLUMN].min()),
        "liability_prediction_max": float(rows[LIABILITY_SCORE_COLUMN].max()),
    }


def compare_policy_entries_and_completeness(
    *,
    candidate_daily: pd.DataFrame,
    candidate_episodes: pd.DataFrame,
    anchor_daily: pd.DataFrame,
    anchor_episodes: pd.DataFrame,
    expected_dates: Sequence[pd.Timestamp],
    target_count: int = 5,
) -> dict[str, Any]:
    """Compare real ENTER events and fail closed on worse portfolio completeness."""
    expected = set(pd.DatetimeIndex(pd.to_datetime(list(expected_dates))).normalize())
    candidate = _normalize_policy_daily(candidate_daily, expected, target_count=target_count)
    anchor = _normalize_policy_daily(anchor_daily, expected, target_count=target_count)
    candidate_entries = _entry_identities(candidate_episodes)
    anchor_entries = _entry_identities(anchor_episodes)
    candidate_only = candidate_entries - anchor_entries
    anchor_only = anchor_entries - candidate_entries
    active_slot_coverage = float(candidate["active_count"].sum() / (len(candidate) * target_count))
    anchor_active_slot_coverage = float(anchor["active_count"].sum() / (len(anchor) * target_count))
    cash_day_count = int((candidate["cash_slot_count"] > 0).sum())
    anchor_cash_day_count = int((anchor["cash_slot_count"] > 0).sum())
    complete = bool(
        active_slot_coverage + 1e-15 >= anchor_active_slot_coverage
        and cash_day_count <= anchor_cash_day_count
        and (candidate["active_count"].to_numpy() >= anchor["active_count"].to_numpy()).all()
        and (candidate["cash_slot_count"].to_numpy() <= anchor["cash_slot_count"].to_numpy()).all()
    )
    return {
        "mean_turnover_fraction": float(candidate["turnover_fraction"].mean()),
        "active_slot_coverage": active_slot_coverage,
        "anchor_active_slot_coverage": anchor_active_slot_coverage,
        "cash_day_count": cash_day_count,
        "anchor_cash_day_count": anchor_cash_day_count,
        "day_count": len(candidate),
        "actual_entry_change_count": len(candidate_only) + len(anchor_only),
        "candidate_only_entry_count": len(candidate_only),
        "anchor_only_entry_count": len(anchor_only),
        "actual_entry_changed_decision_count": len(
            {item[0] for item in candidate_only | anchor_only}
        ),
        "complete": complete,
    }


def _normalize_anchor(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "selection_effective_rank",
        ENTRY_PRIORITY_COLUMN,
        SCORE_COLUMN,
    }
    if not required.issubset(frame):
        raise _priority_error("P0-L anchor prediction columns are incomplete")
    rows = frame.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()
    rows["target_trade_date"] = pd.to_datetime(rows["target_trade_date"]).dt.normalize()
    rows["instrument"] = rows["instrument"].astype(str).str.upper()
    rows[ANCHOR_RANK_COLUMN] = pd.to_numeric(rows[ENTRY_PRIORITY_COLUMN], errors="coerce")
    rows["selection_effective_rank"] = pd.to_numeric(
        rows["selection_effective_rank"], errors="coerce"
    )
    rows[SCORE_COLUMN] = pd.to_numeric(rows[SCORE_COLUMN], errors="coerce")
    rows["selection_exit_rank"] = rows["selection_effective_rank"]
    _verify_exact_twenty(rows, rank_column=ANCHOR_RANK_COLUMN)
    if not np.isfinite(rows[SCORE_COLUMN].to_numpy(float)).all():
        raise _priority_error("P0-L anchor scores are non-finite")
    return rows


def _normalize_policy_daily(
    frame: pd.DataFrame,
    expected_dates: set[pd.Timestamp],
    *,
    target_count: int,
) -> pd.DataFrame:
    required = {
        "decision_as_of_trade_date",
        "turnover_fraction",
        "active_count",
        "cash_slot_count",
    }
    if not required.issubset(frame):
        raise _constraint_error("P0-L policy daily output is incomplete")
    rows = frame.loc[:, sorted(required)].copy()
    if "is_candidate_decision" in frame:
        rows = frame.loc[frame["is_candidate_decision"].eq(True), sorted(required)].copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(
        rows["decision_as_of_trade_date"]
    ).dt.normalize()
    rows = rows[rows["decision_as_of_trade_date"].isin(expected_dates)].copy()
    if (
        rows.duplicated("decision_as_of_trade_date").any()
        or set(rows["decision_as_of_trade_date"]) != expected_dates
    ):
        raise _constraint_error("P0-L policy daily dates differ from the frozen evaluation dates")
    for column in ("turnover_fraction", "active_count", "cash_slot_count"):
        rows[column] = pd.to_numeric(rows[column], errors="coerce")
        if not np.isfinite(rows[column].to_numpy(float)).all():
            raise _constraint_error("P0-L policy daily output contains non-finite state")
    if (
        (rows["turnover_fraction"] < 0).any()
        or (rows["active_count"] < 0).any()
        or (rows["cash_slot_count"] < 0).any()
        or not (rows["active_count"] + rows["cash_slot_count"]).eq(target_count).all()
    ):
        raise _constraint_error("P0-L policy daily output contains invalid portfolio state")
    return rows.sort_values("decision_as_of_trade_date").reset_index(drop=True)


def _entry_identities(frame: pd.DataFrame) -> set[tuple[pd.Timestamp, str]]:
    if frame.empty:
        return set()
    required = {"entry_signal_date", "instrument"}
    if not required.issubset(frame):
        raise _constraint_error("P0-L policy episodes omit real-entry identity")
    rows = frame.loc[:, ["entry_signal_date", "instrument"]].copy()
    rows["entry_signal_date"] = pd.to_datetime(rows["entry_signal_date"]).dt.normalize()
    rows["instrument"] = rows["instrument"].astype(str).str.upper()
    if rows.isna().any().any() or rows.duplicated().any():
        raise _constraint_error("P0-L policy episodes contain invalid real-entry identity")
    return set(rows.itertuples(index=False, name=None))


def _normalize_liability(frame: pd.DataFrame) -> pd.DataFrame:
    verify_liability_head_predictions(frame)
    rows = frame.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()
    rows["target_trade_date"] = pd.to_datetime(rows["target_trade_date"]).dt.normalize()
    rows["instrument"] = rows["instrument"].astype(str).str.upper()
    rows["selection_effective_rank"] = pd.to_numeric(
        rows["selection_effective_rank"], errors="coerce"
    )
    _verify_exact_twenty(rows, rank_column="selection_effective_rank")
    return rows


def _verify_exact_twenty(rows: pd.DataFrame, *, rank_column: str) -> None:
    counts = rows.groupby("decision_as_of_trade_date").size()
    ranks = rows.groupby("decision_as_of_trade_date")[rank_column].apply(
        lambda values: tuple(sorted(values.tolist()))
    )
    if (
        counts.empty
        or not counts.eq(20).all()
        or not ranks.map(lambda values: values == tuple(range(1, 21))).all()
        or rows.duplicated(["decision_as_of_trade_date", "instrument"]).any()
    ):
        raise _priority_error("P0-L predictions do not contain exact unique Top20 ranks")


def _verify_priorities(rows: pd.DataFrame) -> None:
    _verify_exact_twenty(rows, rank_column=ENTRY_PRIORITY_COLUMN)
    displacement = (rows[ENTRY_PRIORITY_COLUMN] - rows[ANCHOR_RANK_COLUMN]).abs()
    if (displacement > 1).any():
        raise _priority_error("P0-L local reranker exceeds maximum anchor displacement")
    changed_per_date = (
        (rows[ENTRY_PRIORITY_COLUMN] != rows[ANCHOR_RANK_COLUMN])
        .groupby(rows["decision_as_of_trade_date"])
        .sum()
    )
    if (changed_per_date > 2).any():
        raise _priority_error("P0-L local reranker performs more than one adjacent swap per date")


def _priority_identity(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if not set(keys).issubset(frame):
        raise _priority_error("P0-L priority identity columns are incomplete")
    rows = frame.loc[:, keys].copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()
    rows["instrument"] = rows["instrument"].astype(str).str.upper()
    rows[ENTRY_PRIORITY_COLUMN] = pd.to_numeric(rows[ENTRY_PRIORITY_COLUMN], errors="raise").astype(int)
    return rows.sort_values(keys).reset_index(drop=True)


def _assert_identity_metrics(
    identity_metrics: Mapping[str, Any],
    anchor_metrics: Mapping[str, Any],
) -> None:
    numeric = ("mean_turnover_fraction", "active_slot_coverage")
    exact = ("cash_day_count", "day_count")
    if any(
        abs(float(identity_metrics[key]) - float(anchor_metrics[key])) > 1e-15
        for key in numeric
    ) or any(identity_metrics[key] != anchor_metrics[key] for key in exact):
        raise AdvisoryModelFirstError(
            "P0-L identity control does not reproduce P0-G policy metrics",
            reason_code="ADVISORY_P0L_ANCHOR_IDENTITY_FAILED",
        )
    if int(identity_metrics.get("actual_entry_change_count", 0)) != 0:
        raise AdvisoryModelFirstError(
            "P0-L identity control changes an actual P0-G entry",
            reason_code="ADVISORY_P0L_ANCHOR_IDENTITY_FAILED",
        )


def _receipt_metrics(result: LocalRerankResult, metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "changed_decision_count": result.changed_decision_count,
        "changed_candidate_row_count": result.changed_candidate_row_count,
        "top5_boundary_change_count": result.top5_boundary_change_count,
        "actual_entry_change_count": int(metrics.get("actual_entry_change_count", 0)),
        "mean_turnover_fraction": float(metrics.get("mean_turnover_fraction", float("nan"))),
        "active_slot_coverage": float(metrics.get("active_slot_coverage", float("nan"))),
        "cash_day_count": int(metrics.get("cash_day_count", 0)),
        "day_count": int(metrics.get("day_count", 0)),
        "complete": bool(metrics.get("complete", False)),
    }


def _finite_mean(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty or not np.isfinite(numeric.to_numpy(float)).all():
        return None
    return float(numeric.mean())


def _priority_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_P0L_LOCAL_RERANK_INVALID",
        context=context or None,
    )


def _constraint_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_P0L_LOCAL_RERANK_INFEASIBLE",
        context=context or None,
    )
