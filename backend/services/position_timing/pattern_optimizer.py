"""Bounded eight-template monthly selector for PT-NEXT-018.

The selector is a deterministic research policy, not a tuning service.  All
eight templates are fixed in :mod:`pattern_strategy`; only prior completed
training-population months may affect an outer-month choice.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .action_value import ActionValueError
from .action_value_corporate_actions import CorporateActionBook
from .action_value_data import DailyCandidate
from .contracts import canonical_sha256
from .pattern_research import (
    BLOCK_SESSIONS,
    INFERENCE_SEED,
    INITIAL_TRAINING_SESSIONS,
    mean_interval,
    replay_full_policy_symbol,
)
from .action_value_research import _has_unbound_material_factor_change
from .pattern_strategy import PATTERN_TEMPLATE_SET_SHA256, TEMPLATES, pattern_feature_frame


OPTIMIZER_ID = "POSITION_TIMING_BOUNDED_MONTHLY_TEMPLATE_SELECTOR_V1"
TIE_TOLERANCE_BPS = 1e-9
LOOKBACK_CALENDAR_MONTHS = 12
MATURITY_LAG_SESSIONS = 25
OPTIMIZER_CONTRACT: Mapping[str, Any] = {
    "schema_version": "position_timing_pattern_optimizer_contract_v1",
    "optimizer_id": OPTIMIZER_ID,
    "template_ids": tuple(item.template_id for item in TEMPLATES),
    "template_set_sha256": PATTERN_TEMPLATE_SET_SHA256,
    "training_objective": "P_MINUS_BUY_AND_HOLD_COST_AFTER_BPS_PER_DAY",
    "lookback_calendar_months": LOOKBACK_CALENDAR_MONTHS,
    "last_complete_month_lag_sessions": MATURITY_LAG_SESSIONS,
    "tie_tolerance_bps": TIE_TOLERANCE_BPS,
    "tie_break": tuple(item.template_id for item in TEMPLATES),
    "first_unavailable_fallback": "R0",
    "later_unavailable_fallback": "PRIOR_TEMPLATE",
    "outer_population": "EVALUATION_SYMBOLS_ONLY",
}
OPTIMIZER_CONTRACT_SHA256 = canonical_sha256(OPTIMIZER_CONTRACT)


@dataclass(frozen=True)
class OptimizerReplayResult:
    development_days: pd.DataFrame
    schedule: pd.DataFrame
    outer_days: pd.DataFrame
    receipt: Mapping[str, Any]


def build_template_development_paths(
    candidate: DailyCandidate,
    *,
    training_symbols: Sequence[str],
    corporate_actions: CorporateActionBook,
    start: date,
    end: date,
    parent_count: int = 1,
    additional_friction_bps: Decimal = Decimal(0),
) -> tuple[pd.DataFrame, Mapping[str, Any]]:
    if not training_symbols or len(set(training_symbols)) != len(training_symbols):
        raise ActionValueError("PATTERN_OPTIMIZER_TRAINING_POPULATION_INVALID")
    calendar_dates = tuple(timestamp.date() for timestamp in candidate.calendar)
    ordinals = {day: index for index, day in enumerate(calendar_dates)}
    if start not in ordinals or end not in ordinals:
        raise ActionValueError("PATTERN_OPTIMIZER_CALENDAR_MISMATCH")
    start_ordinal = ordinals[start] + INITIAL_TRAINING_SESSIONS
    terminal_ordinal = ordinals[end] - 5
    rows: list[dict[str, Any]] = []
    excluded: Counter[str] = Counter()
    for symbol in training_symbols:
        symbol_rows: list[dict[str, Any]] = []
        try:
            bars = candidate.bars(symbol)
            if _has_unbound_material_factor_change(
                symbol=symbol,
                bars=bars,
                start_ordinal=start_ordinal,
                end_ordinal=ordinals[end],
                corporate_actions=corporate_actions,
            ):
                raise ActionValueError("UNBOUND_MATERIAL_FACTOR_CHANGE", symbol=symbol)
            features = pattern_feature_frame(bars, symbol=symbol, corporate_actions=corporate_actions)
            for template in TEMPLATES:
                sleeve_rows, _, _ = replay_full_policy_symbol(
                    symbol=symbol,
                    bars=bars,
                    features=features,
                    calendar_dates=calendar_dates,
                    corporate_actions=corporate_actions,
                    start_ordinal=start_ordinal,
                    terminal_ordinal=terminal_ordinal,
                    template_id=template.template_id,
                    parent_count=parent_count,
                    additional_friction_bps=additional_friction_bps,
                )
                symbol_rows.extend(
                    row for row in sleeve_rows if row["comparison"] == "P_MINUS_BUY_AND_HOLD"
                )
            rows.extend(symbol_rows)
        except ActionValueError as exc:
            excluded[f"{symbol}:{exc.code}"] += 1
    frame = pd.DataFrame(rows)
    expected_pairs = len(TEMPLATES) * len(training_symbols)
    completed_pairs = frame.groupby(["template_id", "symbol"]).ngroups if not frame.empty else 0
    coverage = {
        "schema_version": "position_timing_pattern_optimizer_development_coverage_v1",
        "expected_template_symbol_pairs": expected_pairs,
        "completed_template_symbol_pairs": completed_pairs,
        "coverage_complete": completed_pairs == expected_pairs and not excluded,
        "excluded": dict(sorted(excluded.items())),
    }
    coverage["coverage_sha256"] = canonical_sha256(coverage)
    return frame, coverage


def _month_start(value: pd.Timestamp) -> pd.Timestamp:
    return value.to_period("M").start_time


def monthly_template_schedule(
    development_days: pd.DataFrame,
    *,
    calendar_dates: Sequence[date],
    evaluation_start: date,
    evaluation_end: date,
    development_coverage_complete: bool = True,
) -> pd.DataFrame:
    required = {"symbol", "valuation_date", "template_id", "incremental_net_value_bps"}
    if not development_days.empty and not required.issubset(development_days):
        raise ActionValueError("PATTERN_OPTIMIZER_DEVELOPMENT_ROWS_INVALID")
    frame = development_days.reindex(columns=sorted(required)).copy()
    frame["valuation_date"] = pd.to_datetime(frame["valuation_date"])
    if not frame["template_id"].isin([item.template_id for item in TEMPLATES]).all():
        raise ActionValueError("PATTERN_OPTIMIZER_TEMPLATE_ID_INVALID")
    if not np.isfinite(pd.to_numeric(frame["incremental_net_value_bps"], errors="coerce")).all():
        raise ActionValueError("PATTERN_OPTIMIZER_VALUE_INVALID")

    calendar = pd.DatetimeIndex(calendar_dates)
    if not calendar.is_monotonic_increasing or evaluation_start > evaluation_end:
        raise ActionValueError("PATTERN_OPTIMIZER_CALENDAR_INVALID")
    months = sorted(
        {
            timestamp.strftime("%Y-%m")
            for timestamp in calendar
            if evaluation_start <= timestamp.date() <= evaluation_end
        }
    )
    output: list[dict[str, Any]] = []
    prior_template = "R0"
    priorities = {item.template_id: index for index, item in enumerate(TEMPLATES)}
    for month in months:
        month_days = [item for item in calendar if item.strftime("%Y-%m") == month]
        first = month_days[0]
        first_ordinal = int(calendar.get_loc(first))
        latest_eligible_ordinal = first_ordinal - MATURITY_LAG_SESSIONS
        eligible_periods = []
        for period in sorted(set(calendar[calendar < first].to_period("M"))):
            sessions = calendar[calendar.to_period("M") == period]
            if len(sessions) and int(calendar.get_loc(sessions[-1])) <= latest_eligible_ordinal:
                eligible_periods.append(period)
        if eligible_periods:
            latest_month = eligible_periods[-1]
            window_end = latest_month.end_time.normalize()
            window_start = (latest_month - (LOOKBACK_CALENDAR_MONTHS - 1)).start_time.normalize()
            sample = frame.loc[frame["valuation_date"].between(window_start, window_end, inclusive="both")]
        else:
            sample = frame.iloc[0:0]
            window_start = window_end = None

        scores: dict[str, float] = {}
        if development_coverage_complete and not sample.empty:
            # Candidate source coverage must be symmetric.  Every template
            # needs the same symbol/date support before ranking.
            support = sample.groupby("template_id")[["symbol", "valuation_date"]].apply(
                lambda group: set(zip(group["symbol"], group["valuation_date"])), include_groups=False
            )
            expected_dates = {item.normalize() for item in calendar if window_start <= item.normalize() <= window_end}
            expected_symbols = set(frame["symbol"])
            expected_support = {(symbol, day) for symbol in expected_symbols for day in expected_dates}
            covered_periods = {item.to_period("M") for item in pd.DatetimeIndex(expected_dates)}
            if (
                len(covered_periods) == LOOKBACK_CALENDAR_MONTHS
                and len(support) == len(TEMPLATES)
                and all(item == expected_support for item in support)
            ):
                scores = (
                    sample.groupby(["template_id", "valuation_date"])["incremental_net_value_bps"]
                    .mean()
                    .groupby("template_id")
                    .mean()
                    .to_dict()
                )
        if scores:
            best = max(scores.values())
            selected = min(
                (key for key, value in scores.items() if best - value <= TIE_TOLERANCE_BPS),
                key=priorities.__getitem__,
            )
            status = "SELECTED_FROM_PRIOR_COMPLETED_12_MONTHS"
        else:
            selected = prior_template
            if not development_coverage_complete:
                status = (
                    "OPTIMIZER_UNAVAILABLE_DEVELOPMENT_COVERAGE_INCOMPLETE_USING_PRIOR_TEMPLATE"
                    if output
                    else "OPTIMIZER_UNAVAILABLE_DEVELOPMENT_COVERAGE_INCOMPLETE_USING_R0"
                )
            else:
                status = "OPTIMIZER_UNAVAILABLE_USING_PRIOR_TEMPLATE" if output else "OPTIMIZER_UNAVAILABLE_USING_R0"
        output.append(
            {
                "month": month,
                "selected_template_id": selected,
                "status": status,
                "training_window_start": window_start.date() if window_start is not None else None,
                "training_window_end": window_end.date() if window_end is not None else None,
                "score_bps": scores.get(selected),
                "all_scores": scores,
            }
        )
        prior_template = selected
    return pd.DataFrame(output)


def replay_optimizer_outer(
    candidate: DailyCandidate,
    *,
    evaluation_symbols: Sequence[str],
    corporate_actions: CorporateActionBook,
    start: date,
    end: date,
    schedule: pd.DataFrame,
    parent_count: int = 1,
    additional_friction_bps: Decimal = Decimal(0),
) -> tuple[pd.DataFrame, Mapping[str, Any]]:
    if schedule.empty or not {"month", "selected_template_id"}.issubset(schedule):
        raise ActionValueError("PATTERN_OPTIMIZER_SCHEDULE_INVALID")
    mapping = dict(zip(schedule["month"], schedule["selected_template_id"]))
    calendar_dates = tuple(timestamp.date() for timestamp in candidate.calendar)
    ordinals = {day: index for index, day in enumerate(calendar_dates)}
    start_ordinal = ordinals[start] + INITIAL_TRAINING_SESSIONS
    terminal_ordinal = ordinals[end] - 5
    q_rows: list[dict[str, Any]] = []
    p_rows: list[dict[str, Any]] = []
    excluded: Counter[str] = Counter()
    for symbol in evaluation_symbols:
        try:
            bars = candidate.bars(symbol)
            if _has_unbound_material_factor_change(
                symbol=symbol,
                bars=bars,
                start_ordinal=start_ordinal,
                end_ordinal=ordinals[end],
                corporate_actions=corporate_actions,
            ):
                raise ActionValueError("UNBOUND_MATERIAL_FACTOR_CHANGE", symbol=symbol)
            features = pattern_feature_frame(bars, symbol=symbol, corporate_actions=corporate_actions)
            q, _, _ = replay_full_policy_symbol(
                symbol=symbol,
                bars=bars,
                features=features,
                calendar_dates=calendar_dates,
                corporate_actions=corporate_actions,
                start_ordinal=start_ordinal,
                terminal_ordinal=terminal_ordinal,
                template_schedule=mapping,
                parent_count=parent_count,
                additional_friction_bps=additional_friction_bps,
            )
            p, _, _ = replay_full_policy_symbol(
                symbol=symbol,
                bars=bars,
                features=features,
                calendar_dates=calendar_dates,
                corporate_actions=corporate_actions,
                start_ordinal=start_ordinal,
                terminal_ordinal=terminal_ordinal,
                template_id="R0",
                parent_count=parent_count,
                additional_friction_bps=additional_friction_bps,
            )
            q_rows.extend(row for row in q if row["comparison"] == "P_MINUS_BUY_AND_HOLD")
            p_rows.extend(row for row in p if row["comparison"] == "P_MINUS_BUY_AND_HOLD")
        except ActionValueError as exc:
            excluded[exc.code] += 1
    q_frame = pd.DataFrame(q_rows).rename(columns={"incremental_net_value_bps": "q_minus_buy_hold_bps"})
    p_frame = pd.DataFrame(p_rows).rename(columns={"incremental_net_value_bps": "p_minus_buy_hold_bps"})
    if q_frame.empty or p_frame.empty:
        return pd.DataFrame(), {"coverage_complete": False, "excluded": dict(excluded), "reason_code": "NO_OUTER_ROWS"}
    merged = q_frame.merge(
        p_frame[["symbol", "valuation_date", "p_minus_buy_hold_bps"]],
        on=["symbol", "valuation_date"],
        validate="one_to_one",
    )
    merged["q_minus_p_bps"] = merged["q_minus_buy_hold_bps"] - merged["p_minus_buy_hold_bps"]
    coverage = {
        "coverage_complete": not excluded and merged["symbol"].nunique() == len(evaluation_symbols),
        "requested_symbol_count": len(evaluation_symbols),
        "completed_symbol_count": int(merged["symbol"].nunique()),
        "excluded": dict(sorted(excluded.items())),
    }
    return merged, coverage


def optimizer_comparisons(
    outer_days: pd.DataFrame, *, coverage_complete: bool, samples: int = 5000
) -> Mapping[str, Any]:
    output: dict[str, Any] = {}
    for offset, (name, column) in enumerate(
        (("Q_MINUS_P", "q_minus_p_bps"), ("Q_MINUS_BUY_AND_HOLD", "q_minus_buy_hold_bps"))
    ):
        daily = outer_days.groupby("valuation_date")[column].mean().to_numpy(float)
        inference = mean_interval(
            daily,
            block_sessions=BLOCK_SESSIONS,
            samples=samples,
            seed=INFERENCE_SEED + 20 + offset,
            confidence_level=0.99,
        )
        nominal = mean_interval(
            daily,
            block_sessions=BLOCK_SESSIONS,
            samples=samples,
            seed=INFERENCE_SEED + 20 + offset,
            confidence_level=0.95,
        )
        lower, upper = inference["lower_bps"], inference["upper_bps"]
        evidence = "INCONCLUSIVE"
        if coverage_complete and lower is not None and lower > 0:
            evidence = "SUPPORTED"
        elif coverage_complete and upper is not None and upper < 0:
            evidence = "NEGATIVE"
        output[name] = {
            "unit": "BPS_PER_TRADING_DAY",
            "inference": inference,
            "nominal_inference": nominal,
            "mde_bps": max(
                inference["point_bps"] - inference["lower_bps"],
                inference["upper_bps"] - inference["point_bps"],
            ),
            "power_status": "NOT_COMPUTABLE",
            "power_reason_code": "NO_PREREGISTERED_SAME_ESTIMAND_EFFECT_SCALE",
            "effect_evidence": evidence,
        }
    return output


__all__ = (
    "OPTIMIZER_CONTRACT",
    "OPTIMIZER_CONTRACT_SHA256",
    "OptimizerReplayResult",
    "build_template_development_paths",
    "monthly_template_schedule",
    "optimizer_comparisons",
    "replay_optimizer_outer",
)
