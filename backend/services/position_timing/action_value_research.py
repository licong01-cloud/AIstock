"""Offline PT-NEXT-004 action-value research and forward evaluation.

The module deliberately stays inside the position-timing bounded context.  It
reads an immutable daily candidate, creates action-conditioned labels with the
same execution/cost code used by advice, and performs monthly walk-forward
evaluation.  It never writes a database, card, alert, order, or global N0
registry.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import hashlib
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .action_value import (
    ActionPlan,
    ActionValueError,
    BPS,
    FEATURE_ORDER,
    MARKET_FEATURES,
    PositionState,
    action_candidates,
    apply_fill,
    cutoff_on,
    daily_fill,
    leg_fee,
    market_features,
    money,
    state_features,
)
from .action_value_data import BENCHMARK, DailyCandidate
from .action_value_model import HEADS, LocalActionModel, fit_local_model, monthly_training_windows
from .contracts import canonical_sha256


REFERENCE_CAPITAL_CNY = Decimal("100000")
PRIMARY_HORIZON = 20
TERMINAL_MAX_DEFER = 5
DEFAULT_REVIEW_STRIDE = 10
DEFAULT_SYMBOL_LIMIT = 64


@dataclass(frozen=True)
class ActionValuePopulationSpec:
    start: date
    end: date
    symbol_limit: int = DEFAULT_SYMBOL_LIMIT
    review_stride: int = DEFAULT_REVIEW_STRIDE
    seed: int = 20260907
    reference_capital_cny: Decimal = REFERENCE_CAPITAL_CNY
    primary_horizon: int = PRIMARY_HORIZON
    terminal_max_defer: int = TERMINAL_MAX_DEFER

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ActionValueError("POPULATION_DATE_RANGE_INVALID")
        if self.symbol_limit <= 0 or self.review_stride <= 0:
            raise ActionValueError("POPULATION_SAMPLE_INVALID")
        if self.primary_horizon != PRIMARY_HORIZON or self.terminal_max_defer != TERMINAL_MAX_DEFER:
            raise ActionValueError("OUTCOME_HORIZON_CONTRACT_DRIFT")
        if not self.reference_capital_cny.is_finite() or self.reference_capital_cny <= 0:
            raise ActionValueError("REFERENCE_CAPITAL_INVALID")


@dataclass(frozen=True)
class ActionValueRows:
    rows: pd.DataFrame
    coverage: dict[str, Any]


@dataclass(frozen=True)
class WalkForwardResult:
    predictions: pd.DataFrame
    models: tuple[LocalActionModel, ...]
    diagnostics: dict[str, Any]


@dataclass(frozen=True)
class ContinuousReplayResult:
    sleeve_days: pd.DataFrame
    daily_comparisons: pd.DataFrame
    receipt: dict[str, Any]


def deterministic_symbols(symbols: Iterable[str], *, limit: int, seed: int) -> tuple[str, ...]:
    """Choose a source-only sample; no return, label, or feature value is read."""

    unique = tuple(sorted(set(symbols)))
    if limit <= 0 or not unique:
        raise ActionValueError("RESEARCH_POPULATION_INVALID")
    ranked = sorted(
        unique,
        key=lambda symbol: (
            hashlib.sha256(f"{seed}:{symbol}".encode("ascii")).hexdigest(),
            symbol,
        ),
    )
    return tuple(ranked[: min(limit, len(ranked))])


def build_action_value_rows(candidate: DailyCandidate, spec: ActionValuePopulationSpec) -> ActionValueRows:
    """Build two action-conditioned supervised heads without hindsight selection.

    The first head starts from cash and evaluates legal OPEN sizes versus cash.
    The second starts from a deterministic, fully invested twenty-session
    buy/hold sleeve and evaluates REDUCE/EXIT versus HOLD.  Windows containing
    an adjustment-factor change are retained in coverage as unavailable rather
    than approximating a corporate action with a fake share quantity.
    """

    symbols = deterministic_symbols(candidate.symbols, limit=spec.symbol_limit, seed=spec.seed)
    benchmark_bars = candidate.bars(BENCHMARK)
    benchmark = benchmark_bars["close"]
    calendar = candidate.calendar
    eligible_dates = (calendar.date >= spec.start) & (calendar.date <= spec.end)
    indexes = np.flatnonzero(eligible_dates)
    if not len(indexes):
        raise ActionValueError("POPULATION_DATE_RANGE_EMPTY")

    records: list[dict[str, Any]] = []
    counts = {
        "review_candidates": 0,
        "complete_core": 0,
        "corporate_action_unavailable": 0,
        "terminal_unavailable": 0,
        "fill_unknown": 0,
        "rows": 0,
    }
    for symbol in symbols:
        bars = candidate.bars(symbol)
        features = market_features(bars, benchmark)
        for ordinal in indexes[:: spec.review_stride]:
            if ordinal < 30 or ordinal + spec.primary_horizon >= len(calendar):
                continue
            counts["review_candidates"] += 1
            if not bool(bars.iloc[ordinal].get("pit_active")):
                continue
            current_features = features.iloc[ordinal]
            if current_features.isna().any():
                continue
            counts["complete_core"] += 1
            terminal_ordinal = _effective_terminal_ordinal(
                bars,
                ordinal + spec.primary_horizon,
                max_defer=spec.terminal_max_defer,
            )
            if terminal_ordinal is None:
                counts["terminal_unavailable"] += 1
                continue
            path = bars.iloc[ordinal : terminal_ordinal + 1]
            factor = pd.to_numeric(path["factor"], errors="coerce")
            if factor.isna().any() or (factor <= 0).any() or factor.nunique(dropna=False) != 1:
                counts["corporate_action_unavailable"] += 1
                continue
            current_price = money(bars.iloc[ordinal]["close"])
            if current_price <= 0:
                continue
            label_available_at = cutoff_on(calendar[terminal_ordinal].date())

            entry_state = PositionState(
                quantity=0,
                sellable=0,
                cash=spec.reference_capital_cny,
                capital=spec.reference_capital_cny,
            )
            entry_plans = [
                plan for plan in action_candidates(symbol, entry_state, current_price) if plan.delta > 0
            ]
            for plan in entry_plans:
                row = _action_row(
                    symbol=symbol,
                    objective=HEADS[0],
                    state=entry_state,
                    plan=plan,
                    market=current_features,
                    bars=bars,
                    decision_ordinal=ordinal,
                    terminal_ordinal=terminal_ordinal,
                    label_available_at=label_available_at,
                )
                if row is None:
                    counts["fill_unknown"] += 1
                else:
                    records.append(row)

            initial_quantity = max(
                (plan.delta for plan in action_candidates(symbol, entry_state, current_price)),
                default=0,
            )
            if initial_quantity <= 0:
                continue
            initial_cash = spec.reference_capital_cny - current_price * initial_quantity
            if initial_cash < 0:
                raise ActionValueError("SYNTHETIC_STATE_CASH_INVALID", symbol=symbol)
            entry_reference = money(bars.iloc[ordinal - PRIMARY_HORIZON]["close"])
            held_state = PositionState(
                quantity=initial_quantity,
                sellable=initial_quantity,
                cash=initial_cash,
                capital=spec.reference_capital_cny,
                entry_cost=entry_reference if entry_reference > 0 else None,
                holding_age=PRIMARY_HORIZON,
            )
            exit_plans = [
                plan for plan in action_candidates(symbol, held_state, current_price) if plan.delta < 0
            ]
            for plan in exit_plans:
                row = _action_row(
                    symbol=symbol,
                    objective=HEADS[1],
                    state=held_state,
                    plan=plan,
                    market=current_features,
                    bars=bars,
                    decision_ordinal=ordinal,
                    terminal_ordinal=terminal_ordinal,
                    label_available_at=label_available_at,
                )
                if row is None:
                    counts["fill_unknown"] += 1
                else:
                    records.append(row)

    if not records:
        raise ActionValueError("ACTION_VALUE_POPULATION_EMPTY")
    rows = pd.DataFrame.from_records(records)
    rows = rows.sort_values(["decision_as_of", "symbol", "objective", "planned_delta_qty"]).reset_index(drop=True)
    counts["rows"] = len(rows)
    coverage = {
        "schema_version": "position_timing_action_value_population_v2",
        "population_spec": {
            "start": spec.start.isoformat(),
            "end": spec.end.isoformat(),
            "symbol_limit": spec.symbol_limit,
            "review_stride": spec.review_stride,
            "seed": spec.seed,
            "reference_capital_cny": str(spec.reference_capital_cny),
            "primary_horizon": spec.primary_horizon,
            "terminal_max_defer": spec.terminal_max_defer,
            "selection": "SHA256_SEED_SYMBOL_SOURCE_ONLY",
        },
        "symbols": symbols,
        "counts": counts,
        "objective_counts": rows.groupby("objective").size().astype(int).to_dict(),
    }
    coverage["coverage_sha256"] = canonical_sha256(coverage)
    return ActionValueRows(rows=rows, coverage=coverage)


def walk_forward_action_values(
    rows: pd.DataFrame,
    *,
    calendar: Sequence[date],
    source_sha256: str,
    request_sha256: str,
    source_commit: str,
) -> WalkForwardResult:
    """Fit at monthly cutoffs and score only rows available afterward."""

    windows = monthly_training_windows(calendar)
    predictions: list[pd.DataFrame] = []
    models: list[LocalActionModel] = []
    for index, window in enumerate(windows):
        start = pd.Timestamp(window["available_at"])
        end = pd.Timestamp(windows[index + 1]["available_at"]) if index + 1 < len(windows) else None
        decisions = pd.to_datetime(rows["decision_as_of"], utc=True)
        mask = decisions >= start
        if end is not None:
            mask &= decisions < end
        validation = rows.loc[mask]
        if validation.empty:
            continue
        try:
            model = fit_local_model(
                rows,
                cutoff=window["cutoff"],
                available_at=window["available_at"],
                source_sha256=source_sha256,
                request_sha256=request_sha256,
                source_commit=source_commit,
            )
        except ActionValueError as exc:
            if exc.code == "TRAINING_OBJECTIVE_UNAVAILABLE":
                continue
            raise
        values = model.predict(
            validation.loc[:, FEATURE_ORDER],
            validation["objective"].tolist(),
            decision_as_of=max(window["available_at"], pd.Timestamp(validation["decision_as_of"].max()).to_pydatetime()),
        )
        scored = validation.loc[:, [
            "symbol",
            "decision_as_of",
            "objective",
            "planned_delta_qty",
            "net_action_value_bps",
        ]].copy()
        scored["predicted_action_value_bps"] = values
        scored["model_sha256"] = model.metadata["model_sha256"]
        predictions.append(scored)
        models.append(model)
    if not predictions:
        raise ActionValueError("WALK_FORWARD_PREDICTIONS_EMPTY")
    frame = pd.concat(predictions, ignore_index=True)
    selected = _select_oof_actions(frame)
    diagnostics = {
        "schema_version": "position_timing_walk_forward_diagnostic_v2",
        "model_count": len(models),
        "prediction_rows": len(frame),
        "selected_rows": len(selected),
        "selected_mean_net_action_value_bps": float(selected["net_action_value_bps"].mean()),
        "selected_positive_ratio": float((selected["net_action_value_bps"] > 0).mean()),
        "model_hashes": [model.metadata["model_sha256"] for model in models],
        "warning": "ACTION_CONDITIONED_OOF_DIAGNOSTIC_NOT_CONTINUOUS_POLICY_RETURN",
    }
    diagnostics["diagnostic_sha256"] = canonical_sha256(diagnostics)
    return WalkForwardResult(frame, tuple(models), diagnostics)


def replay_continuous_cohorts(
    candidate: DailyCandidate,
    *,
    models: Sequence[LocalActionModel],
    symbols: Sequence[str],
    horizon: int = PRIMARY_HORIZON,
    bootstrap_samples: int = 5000,
    block_sessions: int = 25,
    seed: int = 20260907,
) -> ContinuousReplayResult:
    """Replay one continuous OOT sleeve per symbol and initial state.

    Monthly retraining only changes which already-available model is consumed;
    it never injects capital or resets a position. Policy, buy/hold, and frozen
    L1 paths share the same source bars and future execution day. Corporate-
    action paths are fail-closed until exact share/cash transformations exist.
    """

    if horizon != PRIMARY_HORIZON or bootstrap_samples <= 0 or block_sessions <= 0:
        raise ActionValueError("CONTINUOUS_REPLAY_SPEC_DRIFT")
    ordered_models = sorted(models, key=lambda item: item.metadata["available_at"])
    if not ordered_models or len(set(symbols)) != len(symbols):
        raise ActionValueError("CONTINUOUS_REPLAY_INPUT_INVALID")
    calendar_dates = [day.date() for day in candidate.calendar]
    date_to_index = {day: index for index, day in enumerate(calendar_dates)}
    first_day = datetime.fromisoformat(ordered_models[0].metadata["available_at"]).date()
    last_model_day = datetime.fromisoformat(ordered_models[-1].metadata["available_at"]).date()
    start_ordinal = date_to_index.get(first_day)
    last_model_ordinal = date_to_index.get(last_model_day)
    if start_ordinal is None or last_model_ordinal is None or start_ordinal + horizon >= len(calendar_dates):
        raise ActionValueError("CONTINUOUS_REPLAY_CALENDAR_MISMATCH")
    # The supervised horizon does not reset capital. Extend far enough for the
    # final monthly model to participate while retaining one common report end.
    end_ordinal = min(len(calendar_dates) - 2, last_model_ordinal + horizon)
    if end_ordinal <= start_ordinal:
        raise ActionValueError("CONTINUOUS_REPLAY_RANGE_EMPTY")
    benchmark = candidate.bars(BENCHMARK)["close"]
    bars_by_symbol = {symbol: candidate.bars(symbol) for symbol in symbols}
    features_by_symbol = {
        symbol: market_features(bars, benchmark) for symbol, bars in bars_by_symbol.items()
    }
    rows: list[dict[str, Any]] = []
    excluded = {
        "outside_calendar": 0,
        "pit_or_core": 0,
        "corporate_action": 0,
        "corporate_action_right_censored_sleeves": 0,
        "path_unknown": 0,
    }
    for symbol in symbols:
        bars = bars_by_symbol[symbol]
        path = bars.iloc[start_ordinal : end_ordinal + 2]
        if (
            not bool(bars.iloc[start_ordinal].get("pit_active"))
            or features_by_symbol[symbol].iloc[start_ordinal].isna().any()
        ):
            excluded["pit_or_core"] += 2
            continue
        factors = pd.to_numeric(path["factor"], errors="coerce")
        if factors.isna().any() or (factors <= 0).any():
            excluded["corporate_action"] += 2
            continue
        changes = np.flatnonzero(factors.pct_change(fill_method=None).abs().to_numpy() > 1e-6)
        symbol_end_ordinal = end_ordinal
        if len(changes):
            first_change_ordinal = start_ordinal + int(changes[0])
            symbol_end_ordinal = min(symbol_end_ordinal, first_change_ordinal - 1)
            excluded["corporate_action"] += 2
            excluded["corporate_action_right_censored_sleeves"] += 2
        if symbol_end_ordinal <= start_ordinal:
            continue
        for initial_state in ("CASH_START", "HOLDING_START"):
            try:
                rows.extend(
                    _replay_one_sleeve(
                        symbol=symbol,
                        bars=bars,
                        benchmark=benchmark,
                        calendar_dates=calendar_dates,
                        start_ordinal=start_ordinal,
                        end_ordinal=symbol_end_ordinal,
                        models=ordered_models,
                        initial_state=initial_state,
                    )
                )
            except ActionValueError:
                excluded["path_unknown"] += 1
    if not rows:
        raise ActionValueError("CONTINUOUS_REPLAY_EMPTY")
    sleeve_days = pd.DataFrame(rows)
    daily = _aggregate_daily_comparisons(sleeve_days)
    comparisons = {}
    for baseline in ("BUY_AND_HOLD", "FROZEN_L1_V1"):
        values = daily.loc[daily["baseline"] == baseline, "incremental_net_value_bps"].to_numpy(float)
        nominal = circular_block_interval(
            values,
            block_sessions=block_sessions,
            samples=bootstrap_samples,
            seed=seed + (0 if baseline == "BUY_AND_HOLD" else 1),
            alpha=0.05,
        )
        interval = circular_block_interval(
            values,
            block_sessions=block_sessions,
            samples=bootstrap_samples,
            seed=seed + (0 if baseline == "BUY_AND_HOLD" else 1),
            alpha=0.025,
        )
        comparisons[baseline] = {
            "daily_mean_incremental_bps": float(values.mean()),
            "period_cumulative_incremental_bps": float(values.sum()),
            "simultaneous_interval_level": 0.975,
            "nominal_interval_level": 0.95,
            "nominal_interval_bps": nominal,
            "adjusted_interval_bps": interval,
            "mde_bps": max(
                interval["point_bps"] - interval["lower_bps"],
                interval["upper_bps"] - interval["point_bps"],
            ),
            "power_status": "NOT_COMPUTABLE",
            "power_reason_code": "NO_PREREGISTERED_SAME_ESTIMAND_EFFECT_SCALE",
            "effect_evidence": classify_effect(interval["lower_bps"], interval["upper_bps"]),
            "effective_trading_days": int(len(values)),
        }
    unresolved_corporate_actions = excluded["corporate_action_right_censored_sleeves"] > 0
    if unresolved_corporate_actions:
        for comparison in comparisons.values():
            comparison["effect_evidence_before_coverage_constraint"] = comparison["effect_evidence"]
            comparison["effect_evidence"] = "INCONCLUSIVE"
            comparison["coverage_reason_code"] = "UNSUPPORTED_CORPORATE_ACTION_RIGHT_CENSORING"
    supported = (
        not unresolved_corporate_actions
        and all(item["effect_evidence"] == "SUPPORTED" for item in comparisons.values())
    )
    receipt = {
        "schema_version": "position_timing_continuous_policy_receipt_v2",
        "policy_id": "DAILY_ACTION_VALUE_POLICY_V2",
        "horizon_trading_days": horizon,
        "sleeve_count": int(sleeve_days["sleeve_id"].nunique()),
        "sleeve_day_count": len(sleeve_days),
        "initial_capital_per_sleeve_cny": str(REFERENCE_CAPITAL_CNY),
        "capital_injection_policy": "ONCE_PER_SYMBOL_INITIAL_STATE_FOR_FULL_OOT_PATH",
        "continuous_start": calendar_dates[start_ordinal].isoformat(),
        "continuous_end": calendar_dates[end_ordinal + 1].isoformat(),
        "monthly_retrain_resets_position": False,
        "comparisons": comparisons,
        "study_effect_evidence": "SUPPORTED" if supported else "INCONCLUSIVE",
        "excluded": excluded,
        "execution_assumption": "DAILY_SHARED_GUARD_CONSERVATIVE_V2",
        "corporate_action_policy": "FACTOR_CHANGE_WINDOW_UNAVAILABLE",
        "censoring_policy": "RIGHT_CENSOR_AT_FIRST_UNSUPPORTED_CORPORATE_ACTION",
        "coverage_can_support_policy": not unresolved_corporate_actions,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    return ContinuousReplayResult(sleeve_days=sleeve_days, daily_comparisons=daily, receipt=receipt)


def circular_block_interval(
    values: np.ndarray,
    *,
    block_sessions: int,
    samples: int,
    seed: int,
    alpha: float = 0.025,
) -> dict[str, float]:
    values = np.asarray(values, dtype=float)
    if (
        values.ndim != 1
        or not len(values)
        or not np.isfinite(values).all()
        or not 0 < alpha < 1
    ):
        raise ActionValueError("INFERENCE_VALUES_INVALID")
    rng = np.random.default_rng(seed)
    block = min(block_sessions, len(values))
    draws = np.empty(samples, dtype=float)
    block_count = int(np.ceil(len(values) / block))
    for sample in range(samples):
        starts = rng.integers(0, len(values), size=block_count)
        indexes = np.concatenate(
            [(np.arange(start, start + block) % len(values)) for start in starts]
        )[: len(values)]
        draws[sample] = values[indexes].mean()
    return {
        "lower_bps": float(np.quantile(draws, alpha / 2)),
        "point_bps": float(values.mean()),
        "upper_bps": float(np.quantile(draws, 1 - alpha / 2)),
    }


def classify_effect(lower_bps: float, upper_bps: float) -> str:
    if lower_bps > 0:
        return "SUPPORTED"
    if upper_bps <= 0:
        return "NEGATIVE"
    return "INCONCLUSIVE"


def _action_row(
    *,
    symbol: str,
    objective: str,
    state: PositionState,
    plan: ActionPlan,
    market: pd.Series,
    bars: pd.DataFrame,
    decision_ordinal: int,
    terminal_ordinal: int,
    label_available_at: datetime,
) -> dict[str, Any] | None:
    fill = daily_fill(
        plan,
        bars.iloc[decision_ordinal + 1],
        sellable=state.sellable,
        full_exit=(-plan.delta == state.quantity),
    )
    if fill.status == "UNKNOWN":
        return None
    candidate_state = apply_fill(state, fill)
    candidate_value = _terminal_net_value(candidate_state, plan.symbol, bars.iloc[terminal_ordinal])
    baseline_value = _terminal_net_value(state, plan.symbol, bars.iloc[terminal_ordinal])
    label = (candidate_value - baseline_value) / state.capital * BPS
    return {
        "symbol": symbol,
        "decision_as_of": cutoff_on(bars.index[decision_ordinal].date()),
        "label_available_at": label_available_at,
        "objective": objective,
        "planned_delta_qty": plan.delta,
        "fill_status": fill.status,
        "net_action_value_bps": float(label),
        **{name: float(market[name]) for name in MARKET_FEATURES},
        **state_features(state, plan),
    }


def _replay_one_sleeve(
    *,
    symbol: str,
    bars: pd.DataFrame,
    benchmark: pd.Series,
    calendar_dates: Sequence[date],
    start_ordinal: int,
    end_ordinal: int,
    models: Sequence[LocalActionModel],
    initial_state: str,
) -> list[dict[str, Any]]:
    from .action_value_advice import decide_stock_day

    reference = money(bars.iloc[start_ordinal]["close"])
    if reference <= 0:
        raise ActionValueError("CURRENT_RAW_PRICE_INVALID")
    cash_state = PositionState(0, 0, REFERENCE_CAPITAL_CNY, REFERENCE_CAPITAL_CNY)
    if initial_state == "CASH_START":
        policy_state = cash_state
        buy_hold_state = cash_state
        l1_state = cash_state
        buy_hold_complete = False
    elif initial_state == "HOLDING_START":
        full = max(plan.delta for plan in action_candidates(symbol, cash_state, reference))
        if full <= 0:
            raise ActionValueError("INITIAL_HOLDING_UNAVAILABLE")
        cash = REFERENCE_CAPITAL_CNY - reference * full
        entry = money(bars.iloc[start_ordinal - PRIMARY_HORIZON]["close"])
        policy_state = PositionState(full, full, cash, REFERENCE_CAPITAL_CNY, entry, PRIMARY_HORIZON)
        buy_hold_state = policy_state
        l1_state = policy_state
        buy_hold_complete = True
    else:
        raise ActionValueError("INITIAL_STATE_INVALID")
    sleeve_id = canonical_sha256(
        {"symbol": symbol, "continuous_start": calendar_dates[start_ordinal], "initial_state": initial_state}
    )[:24]
    previous_differences = {"BUY_AND_HOLD": Decimal(0), "FROZEN_L1_V1": Decimal(0)}
    output: list[dict[str, Any]] = []
    for decision_ordinal in range(start_ordinal, end_ordinal + 1):
        target_ordinal = decision_ordinal + 1
        decision_as_of = cutoff_on(calendar_dates[decision_ordinal])
        model = _model_available_for(models, decision_as_of)
        if model is None:
            raise ActionValueError("MODEL_UNAVAILABLE_RULE_FALLBACK")
        policy_state = _roll_state_to_decision(policy_state)
        buy_hold_state = _roll_state_to_decision(buy_hold_state)
        l1_state = _roll_state_to_decision(l1_state)

        decision = decide_stock_day(
            symbol=symbol,
            state=policy_state,
            bars=bars.iloc[: decision_ordinal + 1],
            benchmark=benchmark,
            decision_as_of=decision_as_of,
            model=model,
        )
        pre_policy_state = policy_state
        policy_fill = daily_fill(
            decision.plan,
            bars.iloc[target_ordinal],
            sellable=policy_state.sellable,
            full_exit=(-decision.plan.delta == policy_state.quantity),
        )
        policy_state = apply_fill(policy_state, policy_fill)

        if not buy_hold_complete:
            buy_reference = money(bars.iloc[decision_ordinal]["close"])
            candidates = action_candidates(symbol, buy_hold_state, buy_reference)
            plan = max(candidates, key=lambda item: item.delta)
            fill = daily_fill(plan, bars.iloc[target_ordinal], sellable=0)
            buy_hold_state = apply_fill(buy_hold_state, fill)
            buy_hold_complete = buy_hold_state.quantity > 0

        from .action_value import risk_exit_plan

        l1_plan = risk_exit_plan(
            symbol,
            l1_state,
            money(bars.iloc[decision_ordinal]["close"]),
        )
        if l1_plan is not None:
            l1_state = apply_fill(
                l1_state,
                daily_fill(
                    l1_plan,
                    bars.iloc[target_ordinal],
                    sellable=l1_state.sellable,
                    full_exit=(-l1_plan.delta == l1_state.quantity),
                ),
            )
        price = money(bars.iloc[target_ordinal]["close"])
        wealth = {
            "POLICY": _mark_to_market(policy_state, symbol, price),
            "BUY_AND_HOLD": _mark_to_market(buy_hold_state, symbol, price),
            "FROZEN_L1_V1": _mark_to_market(l1_state, symbol, price),
        }
        for baseline in ("BUY_AND_HOLD", "FROZEN_L1_V1"):
            difference = wealth["POLICY"] - wealth[baseline]
            increment = difference - previous_differences[baseline]
            previous_differences[baseline] = difference
            output.append(
                {
                    "sleeve_id": sleeve_id,
                    "symbol": symbol,
                    "initial_state": initial_state,
                    "continuous_start": calendar_dates[start_ordinal],
                    "decision_trade_date": calendar_dates[decision_ordinal],
                    "valuation_date": calendar_dates[target_ordinal],
                    "target_trade_date": calendar_dates[target_ordinal],
                    "baseline": baseline,
                    "policy_wealth_cny": float(wealth["POLICY"]),
                    "baseline_wealth_cny": float(wealth[baseline]),
                    "incremental_net_value_cny": float(increment),
                    "action": decision.action,
                    "fill_status": policy_fill.status,
                    "fill_reason": policy_fill.reason,
                    "fill_price_raw": float(policy_fill.price) if policy_fill.price is not None else None,
                    "planned_delta_qty": decision.plan.delta,
                    "plan_reference_raw": float(decision.plan.reference),
                    "plan_risk_exit": decision.plan.risk_exit,
                    "pre_quantity": pre_policy_state.quantity,
                    "pre_sellable_qty": pre_policy_state.sellable,
                    "model_sha256": model.metadata["model_sha256"],
                }
            )
    return output


def _roll_state_to_decision(state: PositionState) -> PositionState:
    from dataclasses import replace

    return replace(
        state,
        sellable=state.quantity,
        holding_age=(state.holding_age + 1 if state.holding_age is not None and state.quantity else state.holding_age),
    )


def _model_available_for(models: Sequence[LocalActionModel], decision_as_of: datetime) -> LocalActionModel | None:
    available = [
        model
        for model in models
        if datetime.fromisoformat(model.metadata["available_at"]) <= decision_as_of
    ]
    return available[-1] if available else None


def _mark_to_market(state: PositionState, symbol: str, price: Decimal) -> Decimal:
    fee = leg_fee(symbol, -state.quantity, price, full_exit=True) if state.quantity else Decimal(0)
    return state.cash + state.quantity * price - fee


def _aggregate_daily_comparisons(sleeve_days: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        sleeve_days.groupby(["valuation_date", "baseline"], as_index=False)
        .agg(incremental_net_value_cny=("incremental_net_value_cny", "sum"), sleeve_count=("sleeve_id", "nunique"))
    )
    grouped["incremental_net_value_bps"] = (
        grouped["incremental_net_value_cny"]
        / (grouped["sleeve_count"] * float(REFERENCE_CAPITAL_CNY))
        * 10000
    )
    return grouped


def _terminal_net_value(state: PositionState, symbol: str, terminal: Mapping[str, Any]) -> Decimal:
    price = money(terminal["close"])
    if price <= 0:
        raise ActionValueError("TERMINAL_PRICE_INVALID")
    fee = leg_fee(symbol, -state.quantity, price, full_exit=True) if state.quantity else Decimal(0)
    return state.cash + state.quantity * price - fee


def _effective_terminal_ordinal(bars: pd.DataFrame, nominal: int, *, max_defer: int) -> int | None:
    for ordinal in range(nominal, min(len(bars), nominal + max_defer + 1)):
        row = bars.iloc[ordinal]
        if not bool(row.get("pit_active")) or bool(row.get("is_suspended")):
            continue
        try:
            close = money(row["close"])
            low = money(row["low"])
            high = money(row["high"])
            down = money(row["down_limit"])
        except (KeyError, ValueError, ArithmeticError):
            continue
        if min(close, low, high, down) <= 0:
            continue
        if low == high == down:
            continue
        return ordinal
    return None


def _select_oof_actions(frame: pd.DataFrame) -> pd.DataFrame:
    choices = []
    for _, group in frame.groupby(["symbol", "decision_as_of", "objective"], sort=False):
        positive = group.loc[group["predicted_action_value_bps"] > 0]
        if positive.empty:
            continue
        ordered = positive.assign(_turnover=positive["planned_delta_qty"].abs()).sort_values(
            ["predicted_action_value_bps", "_turnover", "planned_delta_qty"],
            ascending=[False, True, True],
        )
        choices.append(ordered.iloc[0].drop(labels="_turnover"))
    if not choices:
        return frame.iloc[0:0].copy()
    return pd.DataFrame(choices).reset_index(drop=True)


__all__ = [
    "ActionValuePopulationSpec",
    "ActionValueRows",
    "ContinuousReplayResult",
    "WalkForwardResult",
    "build_action_value_rows",
    "deterministic_symbols",
    "replay_continuous_cohorts",
    "walk_forward_action_values",
]
