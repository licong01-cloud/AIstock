"""Offline PT-NEXT-004 action-value research and forward evaluation.

The module deliberately stays inside the position-timing bounded context.  It
reads an immutable daily candidate, creates action-conditioned labels with the
same execution/cost code used by advice, and performs monthly walk-forward
evaluation.  It never writes a database, card, alert, order, or global N0
registry.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from bisect import bisect_right
from datetime import date, datetime
from decimal import Decimal, ROUND_FLOOR
import hashlib
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .action_value import (
    ActionPlan,
    ActionValueError,
    BPS,
    CORE_INFORMATION_BLOCK,
    Fill,
    PositionState,
    action_candidates,
    apply_fill,
    cutoff_on,
    daily_fill,
    feature_contract,
    leg_fee,
    market_features,
    money,
    policy_sha256_for,
    state_features,
)
from .action_value_data import BENCHMARK, DailyCandidate
from .action_value_corporate_actions import (
    CorporateActionBook,
    apply_corporate_action_with_audit,
)
from .action_value_model import HEADS, LocalActionModel, fit_local_model, monthly_training_windows
from .contracts import canonical_sha256


REFERENCE_CAPITAL_CNY = Decimal("100000")
PRIMARY_HORIZON = 20
TERMINAL_MAX_DEFER = 5
DEFAULT_REVIEW_STRIDE = 10
DEFAULT_SYMBOL_LIMIT = 64
UNBOUND_FACTOR_CHANGE_TOLERANCE_BPS = Decimal("10")
LEGACY_INITIAL_HOLDING_POLICY_ID = "EXECUTABLE_BUY_AT_CONTINUOUS_START_V0"
EXOGENOUS_INITIAL_HOLDING_POLICY_ID = "EXOGENOUS_NORMALIZED_HOLDING_ENDOWMENT_V1"
EXOGENOUS_INITIAL_HOLDING_POLICY = {
    "policy_id": EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
    "estimand": "PRE_EXISTING_POSITION_MARKED_TO_COMMON_CONTINUOUS_START",
    "quantity": "FLOOR_REFERENCE_CAPITAL_DIVIDED_BY_START_RAW_CLOSE_INTEGER_SHARES",
    "cash": "REFERENCE_CAPITAL_MINUS_QUANTITY_TIMES_START_RAW_CLOSE",
    "sellable": "ALL_INITIAL_SHARES",
    "initial_trade": "NONE",
    "initial_fee_cny": "0",
    "entry_reference": "RAW_CLOSE_AT_START_MINUS_PRIMARY_HORIZON",
    "holding_age": PRIMARY_HORIZON,
    "post_start_execution": "SHARED_BOARD_LOT_GUARD_AND_COMPONENT_COST",
}
EXOGENOUS_INITIAL_HOLDING_POLICY_SHA256 = canonical_sha256(
    EXOGENOUS_INITIAL_HOLDING_POLICY
)


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


def build_action_value_rows(
    candidate: DailyCandidate,
    spec: ActionValuePopulationSpec,
    *,
    corporate_actions: CorporateActionBook | None = None,
    information_block: str = CORE_INFORMATION_BLOCK,
) -> ActionValueRows:
    """Build two action-conditioned supervised heads without hindsight selection.

    The first head starts from cash and evaluates legal OPEN sizes versus cash.
    The second starts from a deterministic, fully invested twenty-session
    buy/hold sleeve and evaluates REDUCE/EXIT versus HOLD.  Implemented
    corporate actions use a frozen source to transform quantity and cash;
    material factor changes without a matching event remain unavailable.
    """

    market_feature_names, feature_order, feature_spec_sha256 = feature_contract(information_block)
    symbols = deterministic_symbols(candidate.symbols, limit=spec.symbol_limit, seed=spec.seed)
    action_book = corporate_actions or CorporateActionBook.empty()
    benchmark_bars = candidate.bars(BENCHMARK)
    benchmark = benchmark_bars["close"]
    calendar = candidate.calendar
    calendar_dates = [day.date() for day in calendar]
    eligible_dates = (calendar.date >= spec.start) & (calendar.date <= spec.end)
    indexes = np.flatnonzero(eligible_dates)
    if not len(indexes):
        raise ActionValueError("POPULATION_DATE_RANGE_EMPTY")

    records: list[dict[str, Any]] = []
    counts = {
        "review_candidates": 0,
        "complete_core": 0,
        "corporate_action_unavailable": 0,
        "corporate_action_error_counts": {},
        "target_corporate_action_rows": 0,
        "unbound_material_factor_change": 0,
        "terminal_unavailable": 0,
        "fill_unknown": 0,
        "rows": 0,
    }
    for symbol in symbols:
        bars = candidate.bars(symbol)
        features = market_features(bars, benchmark, information_block=information_block)
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
                symbol=symbol,
                corporate_actions=action_book,
                calendar_dates=calendar_dates,
            )
            if terminal_ordinal is None:
                counts["terminal_unavailable"] += 1
                continue
            if _has_unbound_material_factor_change(
                symbol=symbol,
                bars=bars,
                start_ordinal=ordinal,
                end_ordinal=terminal_ordinal,
                corporate_actions=action_book,
            ):
                counts["corporate_action_unavailable"] += 1
                counts["unbound_material_factor_change"] += 1
                counts["corporate_action_error_counts"]["UNBOUND_MATERIAL_FACTOR_CHANGE"] = (
                    int(counts["corporate_action_error_counts"].get("UNBOUND_MATERIAL_FACTOR_CHANGE", 0)) + 1
                )
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
            try:
                entry_target_state, target_reference, target_action, entry_fractional = _project_target_state(
                    state=entry_state,
                    symbol=symbol,
                    bars=bars,
                    decision_ordinal=ordinal,
                    corporate_actions=action_book,
                    calendar_dates=calendar_dates,
                )
            except ActionValueError as exc:
                if not exc.code.startswith("CORPORATE_ACTION_"):
                    raise
                counts["corporate_action_unavailable"] += 1
                counts["corporate_action_error_counts"][exc.code] = (
                    int(counts["corporate_action_error_counts"].get(exc.code, 0)) + 1
                )
                continue
            entry_plans = [
                plan
                for plan in action_candidates(symbol, entry_target_state, target_reference)
                if plan.delta > 0
            ]
            for plan in entry_plans:
                try:
                    row = _action_row(
                        symbol=symbol,
                        objective=HEADS[0],
                        state=entry_target_state,
                        plan=plan,
                        market=current_features,
                        bars=bars,
                        decision_ordinal=ordinal,
                        terminal_ordinal=terminal_ordinal,
                        label_available_at=label_available_at,
                        corporate_actions=action_book,
                        calendar_dates=calendar_dates,
                        target_fractional_share_discarded=entry_fractional,
                        market_feature_names=market_feature_names,
                    )
                except ActionValueError as exc:
                    if not exc.code.startswith("CORPORATE_ACTION_"):
                        raise
                    counts["corporate_action_unavailable"] += 1
                    counts["corporate_action_error_counts"][exc.code] = (
                        int(counts["corporate_action_error_counts"].get(exc.code, 0)) + 1
                    )
                    continue
                if row is None:
                    counts["fill_unknown"] += 1
                else:
                    records.append(row)
                    counts["target_corporate_action_rows"] += int(target_action is not None)

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
            try:
                held_target_state, held_target_reference, held_target_action, held_fractional = _project_target_state(
                    state=held_state,
                    symbol=symbol,
                    bars=bars,
                    decision_ordinal=ordinal,
                    corporate_actions=action_book,
                    calendar_dates=calendar_dates,
                )
            except ActionValueError as exc:
                if not exc.code.startswith("CORPORATE_ACTION_"):
                    raise
                counts["corporate_action_unavailable"] += 1
                counts["corporate_action_error_counts"][exc.code] = (
                    int(counts["corporate_action_error_counts"].get(exc.code, 0)) + 1
                )
                continue
            exit_plans = [
                plan
                for plan in action_candidates(symbol, held_target_state, held_target_reference)
                if plan.delta < 0
            ]
            for plan in exit_plans:
                try:
                    row = _action_row(
                        symbol=symbol,
                        objective=HEADS[1],
                        state=held_target_state,
                        plan=plan,
                        market=current_features,
                        bars=bars,
                        decision_ordinal=ordinal,
                        terminal_ordinal=terminal_ordinal,
                        label_available_at=label_available_at,
                        corporate_actions=action_book,
                        calendar_dates=calendar_dates,
                        target_fractional_share_discarded=held_fractional,
                        market_feature_names=market_feature_names,
                    )
                except ActionValueError as exc:
                    if not exc.code.startswith("CORPORATE_ACTION_"):
                        raise
                    counts["corporate_action_unavailable"] += 1
                    counts["corporate_action_error_counts"][exc.code] = (
                        int(counts["corporate_action_error_counts"].get(exc.code, 0)) + 1
                    )
                    continue
                if row is None:
                    counts["fill_unknown"] += 1
                else:
                    records.append(row)
                    counts["target_corporate_action_rows"] += int(held_target_action is not None)

    if not records:
        raise ActionValueError("ACTION_VALUE_POPULATION_EMPTY")
    rows = pd.DataFrame.from_records(records)
    rows = rows.sort_values(["decision_as_of", "symbol", "objective", "planned_delta_qty"]).reset_index(drop=True)
    counts["rows"] = len(rows)
    counts["fractional_rounding_rows"] = int(
        rows[["candidate_fractional_share_discarded", "baseline_fractional_share_discarded"]]
        .gt(0)
        .any(axis=1)
        .sum()
    )
    counts["max_fractional_share_discarded"] = float(
        rows[["candidate_fractional_share_discarded", "baseline_fractional_share_discarded"]]
        .max(axis=1)
        .max()
    )
    coverage = {
        "schema_version": "position_timing_action_value_population_v4",
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
            "corporate_action_snapshot_sha256": action_book.snapshot_sha256,
            "unbound_factor_change_tolerance_bps": str(UNBOUND_FACTOR_CHANGE_TOLERANCE_BPS),
            "fractional_share_policy": "FLOOR_ENTITLEMENT_NO_CASH_CREDIT",
            "account_cash_policy": "MARKET_DIVIDEND_CASH_DIV_AFTER_TAX_FIELD",
        },
        "symbols": symbols,
        "counts": counts,
        "objective_counts": rows.groupby("objective").size().astype(int).to_dict(),
    }
    if information_block != CORE_INFORMATION_BLOCK:
        coverage.update(
            {
                "schema_version": "position_timing_action_value_population_optional_v1",
                "information_block": information_block,
                "feature_order": feature_order,
                "feature_spec_sha256": feature_spec_sha256,
            }
        )
    coverage["coverage_sha256"] = canonical_sha256(coverage)
    return ActionValueRows(rows=rows, coverage=coverage)


def walk_forward_action_values(
    rows: pd.DataFrame,
    *,
    calendar: Sequence[date],
    source_sha256: str,
    request_sha256: str,
    source_commit: str,
    information_block: str = CORE_INFORMATION_BLOCK,
) -> WalkForwardResult:
    """Fit at monthly cutoffs and score only rows available afterward."""

    _, feature_order, feature_spec_sha256 = feature_contract(information_block)
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
                information_block=information_block,
            )
        except ActionValueError as exc:
            if exc.code == "TRAINING_OBJECTIVE_UNAVAILABLE":
                continue
            raise
        values = model.predict(
            validation.loc[:, feature_order],
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
    if information_block != CORE_INFORMATION_BLOCK:
        diagnostics.update(
            {
                "information_block": information_block,
                "feature_spec_sha256": feature_spec_sha256,
            }
        )
    diagnostics["diagnostic_sha256"] = canonical_sha256(diagnostics)
    return WalkForwardResult(frame, tuple(models), diagnostics)


def replay_continuous_cohorts(
    candidate: DailyCandidate,
    *,
    models: Sequence[LocalActionModel],
    symbols: Sequence[str],
    corporate_actions: CorporateActionBook | None = None,
    horizon: int = PRIMARY_HORIZON,
    bootstrap_samples: int = 5000,
    block_sessions: int = 25,
    seed: int = 20260907,
    information_block: str = CORE_INFORMATION_BLOCK,
    initial_holding_policy_id: str = LEGACY_INITIAL_HOLDING_POLICY_ID,
    model_action_authority: str = "FULL_ACTION_VALUE_V4",
) -> ContinuousReplayResult:
    """Replay one continuous OOT sleeve per symbol and initial state.

    Monthly retraining only changes which already-available model is consumed;
    it never injects capital or resets a position. Policy, buy/hold, and frozen
    L1 paths share the same source bars, future execution day, and frozen
    corporate-action quantity/cash transformations.
    """

    from .action_value_advice import (
        ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
        FULL_MODEL_ACTION_AUTHORITY,
        OPEN_ONLY_MODEL_ACTION_AUTHORITY,
        action_authority_policy_sha256,
    )

    if horizon != PRIMARY_HORIZON or bootstrap_samples <= 0 or block_sessions <= 0:
        raise ActionValueError("CONTINUOUS_REPLAY_SPEC_DRIFT")
    if model_action_authority not in {
        FULL_MODEL_ACTION_AUTHORITY,
        ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
        OPEN_ONLY_MODEL_ACTION_AUTHORITY,
    }:
        raise ActionValueError("MODEL_ACTION_AUTHORITY_UNSUPPORTED")
    if initial_holding_policy_id not in {
        LEGACY_INITIAL_HOLDING_POLICY_ID,
        EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
    }:
        raise ActionValueError("INITIAL_HOLDING_POLICY_UNSUPPORTED")
    market_feature_names, _, feature_spec_sha256 = feature_contract(information_block)
    ordered_models = sorted(models, key=lambda item: item.metadata["available_at"])
    action_book = corporate_actions or CorporateActionBook.empty()
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
        symbol: market_features(bars, benchmark, information_block=information_block)
        for symbol, bars in bars_by_symbol.items()
    }
    rows: list[dict[str, Any]] = []
    excluded = {
        "outside_calendar": 0,
        "pit_or_core": 0,
        "source_factor_invalid_sleeves": 0,
        "corporate_action_unavailable_sleeves": 0,
        "corporate_action_applied_sleeve_days": 0,
        "unbound_material_factor_change_sleeves": 0,
        "decision_input_unavailable_sleeve_days": 0,
        "path_unknown": 0,
        "path_error_counts": {},
        "path_error_examples": [],
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
        invalid_factor = factors.isna() | factors.le(0)
        observed_price = path[["open", "high", "low", "close"]].notna().any(axis=1)
        unexplained_factor_gap = invalid_factor & observed_price & ~path["is_suspended"].astype(bool)
        if unexplained_factor_gap.any():
            excluded["source_factor_invalid_sleeves"] += 2
            continue
        if _has_unbound_material_factor_change(
            symbol=symbol,
            bars=bars,
            start_ordinal=start_ordinal,
            end_ordinal=end_ordinal + 1,
            corporate_actions=action_book,
        ):
            excluded["corporate_action_unavailable_sleeves"] += 2
            excluded["unbound_material_factor_change_sleeves"] += 2
            continue
        for initial_state in ("CASH_START", "HOLDING_START"):
            try:
                sleeve_rows = _replay_one_sleeve(
                    symbol=symbol,
                    bars=bars,
                    benchmark=benchmark,
                    features=features_by_symbol[symbol],
                    calendar_dates=calendar_dates,
                    start_ordinal=start_ordinal,
                    end_ordinal=end_ordinal,
                    models=ordered_models,
                    initial_state=initial_state,
                    corporate_actions=action_book,
                    information_block=information_block,
                    initial_holding_policy_id=initial_holding_policy_id,
                    model_action_authority=model_action_authority,
                )
                excluded["corporate_action_applied_sleeve_days"] += len(
                    {
                        (row["sleeve_id"], row["target_trade_date"])
                        for row in sleeve_rows
                        if row["corporate_action_applied"]
                    }
                )
                excluded["decision_input_unavailable_sleeve_days"] += len(
                    {
                        (row["sleeve_id"], row["decision_trade_date"])
                        for row in sleeve_rows
                        if row["decision_input_status"] != "AVAILABLE"
                    }
                )
                rows.extend(sleeve_rows)
            except ActionValueError as exc:
                excluded["path_unknown"] += 1
                path_errors = excluded["path_error_counts"]
                path_errors[exc.code] = int(path_errors.get(exc.code, 0)) + 1
                if len(excluded["path_error_examples"]) < 10:
                    excluded["path_error_examples"].append(
                        {
                            "symbol": symbol,
                            "initial_state": initial_state,
                            "error_code": exc.code,
                            "details": {key: str(value) for key, value in exc.details.items()},
                        }
                    )
    if not rows:
        raise ActionValueError("CONTINUOUS_REPLAY_EMPTY")
    sleeve_days = pd.DataFrame(rows)
    policy_fractional = sleeve_days.loc[
        sleeve_days["baseline"].eq("BUY_AND_HOLD"),
        "policy_fractional_share_discarded",
    ]
    baseline_fractional = sleeve_days["baseline_fractional_share_discarded"]
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
    unresolved_paths = (
        excluded["source_factor_invalid_sleeves"] > 0
        or excluded["unbound_material_factor_change_sleeves"] > 0
        or excluded["corporate_action_unavailable_sleeves"] > 0
        or excluded["path_unknown"] > 0
    )
    if unresolved_paths:
        for comparison in comparisons.values():
            comparison["effect_evidence_before_coverage_constraint"] = comparison["effect_evidence"]
            comparison["effect_evidence"] = "INCONCLUSIVE"
            comparison["coverage_reason_code"] = "SOURCE_OR_CORPORATE_ACTION_PATH_UNAVAILABLE"
    supported = (
        not unresolved_paths
        and all(item["effect_evidence"] == "SUPPORTED" for item in comparisons.values())
    )
    receipt = {
        "schema_version": (
            "position_timing_continuous_policy_receipt_v5"
            if initial_holding_policy_id == EXOGENOUS_INITIAL_HOLDING_POLICY_ID
            else "position_timing_continuous_policy_receipt_v4"
        ),
        "policy_id": (
            "DAILY_ACTION_VALUE_POLICY_V2"
            if model_action_authority == FULL_MODEL_ACTION_AUTHORITY
            else model_action_authority
        ),
        "horizon_trading_days": horizon,
        "sleeve_count": int(sleeve_days["sleeve_id"].nunique()),
        "requested_symbol_count": len(symbols),
        "population_contract": "SYMBOL_ACTIVE_WITH_COMPLETE_CORE_AT_COMMON_CONTINUOUS_START",
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
        "corporate_action_policy": "IMMUTABLE_IMPLEMENTED_DIVIDEND_QUANTITY_CASH_V1",
        "corporate_action_snapshot_sha256": action_book.snapshot_sha256,
        "unbound_factor_change_tolerance_bps": str(UNBOUND_FACTOR_CHANGE_TOLERANCE_BPS),
        "fractional_share_policy": "FLOOR_ENTITLEMENT_NO_CASH_CREDIT",
        "account_cash_policy": "MARKET_DIVIDEND_CASH_DIV_AFTER_TAX_FIELD",
        "fractional_share_sensitivity": {
            "policy_sleeve_days_affected": int(policy_fractional.gt(0).sum()),
            "baseline_sleeve_days_affected": int(baseline_fractional.gt(0).sum()),
            "max_policy_fractional_share_discarded": float(policy_fractional.max()),
            "max_baseline_fractional_share_discarded": float(baseline_fractional.max()),
            "interpretation": "CONSERVATIVE_ACCOUNT_LEVEL_ROUNDING_SOURCE_UNAVAILABLE",
        },
        "censoring_policy": "NO_SILENT_CENSOR_TYPED_PATH_UNAVAILABLE",
        "coverage_can_support_policy": not unresolved_paths,
    }
    if information_block != CORE_INFORMATION_BLOCK:
        receipt.update(
            {
                "schema_version": (
                    "position_timing_continuous_policy_receipt_optional_v2"
                    if initial_holding_policy_id == EXOGENOUS_INITIAL_HOLDING_POLICY_ID
                    else "position_timing_continuous_policy_receipt_optional_v1"
                ),
                "policy_sha256": policy_sha256_for(information_block),
                "information_block": information_block,
                "market_features": market_feature_names,
                "feature_spec_sha256": feature_spec_sha256,
            }
        )
    if initial_holding_policy_id == EXOGENOUS_INITIAL_HOLDING_POLICY_ID:
        receipt.update(
            {
                "initial_holding_policy": EXOGENOUS_INITIAL_HOLDING_POLICY,
                "initial_holding_policy_sha256": EXOGENOUS_INITIAL_HOLDING_POLICY_SHA256,
            }
        )
    if model_action_authority in {
        ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
        OPEN_ONLY_MODEL_ACTION_AUTHORITY,
    }:
        receipt.update(
            {
                "schema_version": (
                    "position_timing_entry_only_continuous_policy_receipt_v1"
                    if model_action_authority == ENTRY_ONLY_MODEL_ACTION_AUTHORITY
                    else "position_timing_open_only_continuous_policy_receipt_v1"
                ),
                "policy_sha256": action_authority_policy_sha256(
                    information_block, model_action_authority
                ),
                "model_action_authority": model_action_authority,
            }
        )
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
    corporate_actions: CorporateActionBook,
    calendar_dates: Sequence[date],
    target_fractional_share_discarded: Decimal,
    market_feature_names: Sequence[str],
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
    target_date = calendar_dates[decision_ordinal + 1]
    terminal_date = calendar_dates[terminal_ordinal]
    candidate_state, candidate_future_fractional = _apply_actions_until(
        candidate_state,
        symbol=plan.symbol,
        start_exclusive=target_date,
        end_inclusive=terminal_date,
        corporate_actions=corporate_actions,
        calendar_dates=calendar_dates,
    )
    baseline_state, baseline_future_fractional = _apply_actions_until(
        state,
        symbol=plan.symbol,
        start_exclusive=target_date,
        end_inclusive=terminal_date,
        corporate_actions=corporate_actions,
        calendar_dates=calendar_dates,
    )
    candidate_value = _terminal_net_value(candidate_state, plan.symbol, bars.iloc[terminal_ordinal])
    baseline_value = _terminal_net_value(baseline_state, plan.symbol, bars.iloc[terminal_ordinal])
    label = (candidate_value - baseline_value) / state.capital * BPS
    return {
        "symbol": symbol,
        "decision_as_of": cutoff_on(bars.index[decision_ordinal].date()),
        "label_available_at": label_available_at,
        "objective": objective,
        "planned_delta_qty": plan.delta,
        "fill_status": fill.status,
        "net_action_value_bps": float(label),
        "candidate_fractional_share_discarded": float(
            target_fractional_share_discarded + candidate_future_fractional
        ),
        "baseline_fractional_share_discarded": float(
            target_fractional_share_discarded + baseline_future_fractional
        ),
        **{name: float(market[name]) for name in market_feature_names},
        **state_features(state, plan),
    }


def _project_target_state(
    *,
    state: PositionState,
    symbol: str,
    bars: pd.DataFrame,
    decision_ordinal: int,
    corporate_actions: CorporateActionBook,
    calendar_dates: Sequence[date],
) -> tuple[PositionState, Decimal, Any | None, Decimal]:
    target_ordinal = decision_ordinal + 1
    target_date = calendar_dates[target_ordinal]
    action = corporate_actions.on(symbol, target_date)
    reference = money(bars.iloc[decision_ordinal]["close"])
    if action is None:
        return state, reference, None, Decimal(0)
    if action.source_available_at > cutoff_on(calendar_dates[decision_ordinal]):
        raise ActionValueError(
            "CORPORATE_ACTION_NOT_VISIBLE_AT_DECISION",
            symbol=symbol,
            effective_trade_date=target_date.isoformat(),
        )
    current_factor = money(bars.iloc[decision_ordinal]["factor"])
    target_factor = money(bars.iloc[target_ordinal]["factor"])
    if min(current_factor, target_factor) <= 0:
        raise ActionValueError("CORPORATE_ACTION_FACTOR_INVALID", symbol=symbol)
    target_reference = reference * current_factor / target_factor
    application = apply_corporate_action_with_audit(
        state,
        action,
        next_trade_date=(
            calendar_dates[target_ordinal + 1]
            if target_ordinal + 1 < len(calendar_dates)
            else None
        ),
    )
    return application.state, target_reference, action, application.fractional_share_discarded


def _apply_actions_until(
    state: PositionState,
    *,
    symbol: str,
    start_exclusive: date,
    end_inclusive: date,
    corporate_actions: CorporateActionBook,
    calendar_dates: Sequence[date],
) -> tuple[PositionState, Decimal]:
    result = state
    fractional_share_discarded = Decimal(0)
    for action in corporate_actions.between(symbol, start_exclusive, end_inclusive):
        next_ordinal = bisect_right(calendar_dates, action.effective_trade_date)
        if not (
            next_ordinal > 0
            and calendar_dates[next_ordinal - 1] == action.effective_trade_date
        ):
            raise ActionValueError("CORPORATE_ACTION_OUTSIDE_TRADING_CALENDAR", symbol=symbol)
        # Any T+1 lock from a preceding session has expired before this later
        # ex-date.  The action itself may create a new one-session share lock.
        result = replace(result, sellable=result.quantity)
        application = apply_corporate_action_with_audit(
            result,
            action,
            next_trade_date=(calendar_dates[next_ordinal] if next_ordinal < len(calendar_dates) else None),
        )
        result = application.state
        fractional_share_discarded += application.fractional_share_discarded
    return result, fractional_share_discarded


def _has_unbound_material_factor_change(
    *,
    symbol: str,
    bars: pd.DataFrame,
    start_ordinal: int,
    end_ordinal: int,
    corporate_actions: CorporateActionBook,
) -> bool:
    factors = pd.to_numeric(bars.iloc[start_ordinal : end_ordinal + 1]["factor"], errors="coerce")
    valid = factors.where(np.isfinite(factors) & (factors > 0)).dropna()
    changes = valid.pct_change(fill_method=None).abs() * float(BPS)
    previous_timestamp: pd.Timestamp | None = None
    for timestamp, change_bps in changes.items():
        if change_bps > float(UNBOUND_FACTOR_CHANGE_TOLERANCE_BPS):
            if previous_timestamp is None or not corporate_actions.between(
                symbol,
                previous_timestamp.date(),
                timestamp.date(),
            ):
                return True
        previous_timestamp = timestamp
    return False


def _apply_replay_fill(
    state: PositionState,
    fill: Fill,
    *,
    symbol: str,
    path_role: str,
    decision_trade_date: date,
    target_trade_date: date,
) -> PositionState:
    try:
        return apply_fill(state, fill)
    except ActionValueError as exc:
        if exc.code != "PATH_VALUATION_UNKNOWN":
            raise
        raise ActionValueError(
            exc.code,
            **exc.details,
            symbol=symbol,
            path_role=path_role,
            decision_trade_date=decision_trade_date.isoformat(),
            target_trade_date=target_trade_date.isoformat(),
        ) from exc


def _replay_one_sleeve(
    *,
    symbol: str,
    bars: pd.DataFrame,
    benchmark: pd.Series,
    features: pd.DataFrame,
    calendar_dates: Sequence[date],
    start_ordinal: int,
    end_ordinal: int,
    models: Sequence[LocalActionModel],
    initial_state: str,
    corporate_actions: CorporateActionBook,
    information_block: str,
    initial_holding_policy_id: str,
    model_action_authority: str,
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
        if initial_holding_policy_id == EXOGENOUS_INITIAL_HOLDING_POLICY_ID:
            if bool(bars.iloc[start_ordinal].get("is_suspended")):
                raise ActionValueError(
                    "INITIAL_HOLDING_REFERENCE_UNAVAILABLE",
                    symbol=symbol,
                )
            entry = _available_raw_close(bars.iloc[start_ordinal - PRIMARY_HORIZON])
            policy_state = initial_holding_endowment(
                symbol=symbol,
                reference=reference,
                entry_reference=entry,
            )
        else:
            full = max(plan.delta for plan in action_candidates(symbol, cash_state, reference))
            if full <= 0:
                raise ActionValueError("INITIAL_HOLDING_UNAVAILABLE")
            cash = REFERENCE_CAPITAL_CNY - reference * full
            entry = money(bars.iloc[start_ordinal - PRIMARY_HORIZON]["close"])
            policy_state = PositionState(
                full,
                full,
                cash,
                REFERENCE_CAPITAL_CNY,
                entry,
                PRIMARY_HORIZON,
            )
        buy_hold_state = policy_state
        l1_state = policy_state
        buy_hold_complete = True
    else:
        raise ActionValueError("INITIAL_STATE_INVALID")
    sleeve_id = canonical_sha256(
        {"symbol": symbol, "continuous_start": calendar_dates[start_ordinal], "initial_state": initial_state}
    )[:24]
    previous_differences = {"BUY_AND_HOLD": Decimal(0), "FROZEN_L1_V1": Decimal(0)}
    last_valuation_price = reference
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

        decision_bar = bars.iloc[decision_ordinal]
        decision_reference = _available_raw_close(decision_bar)
        decision_input_status = "AVAILABLE"
        decision_reason = "AVAILABLE"
        if decision_reference is None or bool(decision_bar.get("is_suspended")):
            decision_input_status = "UNAVAILABLE"
            decision_reason = "DECISION_BAR_SUSPENDED_OR_MISSING"

        target_action = corporate_actions.on(symbol, calendar_dates[target_ordinal])
        if decision_reference is None:
            target_reference = last_valuation_price
            if target_action is None:
                policy_target_state, buy_hold_target_state, l1_target_state = (
                    policy_state,
                    buy_hold_state,
                    l1_state,
                )
                policy_fractional = buy_hold_fractional = l1_fractional = Decimal(0)
            else:
                if target_action.source_available_at > decision_as_of:
                    raise ActionValueError(
                        "CORPORATE_ACTION_NOT_VISIBLE_AT_DECISION",
                        symbol=symbol,
                        effective_trade_date=calendar_dates[target_ordinal].isoformat(),
                    )
                next_trade_date = (
                    calendar_dates[target_ordinal + 1]
                    if target_ordinal + 1 < len(calendar_dates)
                    else None
                )
                applications = tuple(
                    apply_corporate_action_with_audit(
                        state,
                        target_action,
                        next_trade_date=next_trade_date,
                    )
                    for state in (policy_state, buy_hold_state, l1_state)
                )
                policy_target_state, buy_hold_target_state, l1_target_state = (
                    application.state for application in applications
                )
                policy_fractional, buy_hold_fractional, l1_fractional = (
                    application.fractional_share_discarded
                    for application in applications
                )
        else:
            policy_target_state, target_reference, target_action, policy_fractional = _project_target_state(
                state=policy_state,
                symbol=symbol,
                bars=bars,
                decision_ordinal=decision_ordinal,
                corporate_actions=corporate_actions,
                calendar_dates=calendar_dates,
            )
            buy_hold_target_state, _, _, buy_hold_fractional = _project_target_state(
                state=buy_hold_state,
                symbol=symbol,
                bars=bars,
                decision_ordinal=decision_ordinal,
                corporate_actions=corporate_actions,
                calendar_dates=calendar_dates,
            )
            l1_target_state, _, _, l1_fractional = _project_target_state(
                state=l1_state,
                symbol=symbol,
                bars=bars,
                decision_ordinal=decision_ordinal,
                corporate_actions=corporate_actions,
                calendar_dates=calendar_dates,
            )

        decision = None
        if decision_input_status == "AVAILABLE":
            try:
                decision = decide_stock_day(
                    symbol=symbol,
                    state=policy_state,
                    bars=bars.iloc[: decision_ordinal + 1],
                    benchmark=benchmark,
                    decision_as_of=decision_as_of,
                    model=model,
                    target_state=policy_target_state,
                    target_reference=target_reference,
                    current_market=features.iloc[decision_ordinal],
                    information_block=information_block,
                    model_action_authority=model_action_authority,
                )
            except ActionValueError as exc:
                if exc.code not in {"CURRENT_CORE_FEATURE_UNAVAILABLE", "CURRENT_OPTIONAL_FEATURE_UNAVAILABLE"}:
                    raise
                decision_input_status = "UNAVAILABLE"
                decision_reason = exc.code
        if decision is None:
            policy_plan = ActionPlan(symbol, 0, target_reference)
            policy_action = "HOLD" if policy_target_state.quantity else "WAIT"
            policy_authority = "SOURCE_UNAVAILABLE_NO_ACTION"
            policy_model_sha256 = None
        else:
            policy_plan = decision.plan
            policy_action = decision.action
            policy_authority = decision.authority
            policy_model_sha256 = decision.model_sha256
        pre_policy_state = policy_target_state
        policy_fill = daily_fill(
            policy_plan,
            bars.iloc[target_ordinal],
            sellable=policy_target_state.sellable,
            full_exit=(-policy_plan.delta == policy_target_state.quantity),
        )
        policy_state = _apply_replay_fill(
            policy_target_state,
            policy_fill,
            symbol=symbol,
            path_role="POLICY",
            decision_trade_date=calendar_dates[decision_ordinal],
            target_trade_date=calendar_dates[target_ordinal],
        )

        if not buy_hold_complete and decision_input_status == "AVAILABLE":
            candidates = action_candidates(symbol, buy_hold_target_state, target_reference)
            plan = max(candidates, key=lambda item: item.delta)
            fill = daily_fill(plan, bars.iloc[target_ordinal], sellable=buy_hold_target_state.sellable)
            buy_hold_state = _apply_replay_fill(
                buy_hold_target_state,
                fill,
                symbol=symbol,
                path_role="BUY_AND_HOLD",
                decision_trade_date=calendar_dates[decision_ordinal],
                target_trade_date=calendar_dates[target_ordinal],
            )
            buy_hold_complete = buy_hold_state.quantity > 0
        else:
            buy_hold_state = buy_hold_target_state

        from .action_value import risk_exit_plan

        l1_plan = (
            risk_exit_plan(symbol, l1_state, decision_reference)
            if decision_reference is not None and not bool(decision_bar.get("is_suspended"))
            else None
        )
        if l1_plan is not None:
            translated_delta = -l1_target_state.sellable
            l1_plan = (
                ActionPlan(symbol, translated_delta, target_reference, True)
                if translated_delta
                else None
            )
        if l1_plan is not None:
            l1_fill = daily_fill(
                    l1_plan,
                    bars.iloc[target_ordinal],
                    sellable=l1_target_state.sellable,
                    full_exit=(-l1_plan.delta == l1_target_state.quantity),
                )
            l1_state = _apply_replay_fill(
                l1_target_state,
                l1_fill,
                symbol=symbol,
                path_role="FROZEN_L1_V1",
                decision_trade_date=calendar_dates[decision_ordinal],
                target_trade_date=calendar_dates[target_ordinal],
            )
        else:
            l1_state = l1_target_state
        target_price = _available_raw_close(bars.iloc[target_ordinal])
        if target_price is not None:
            last_valuation_price = target_price
        price = last_valuation_price
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
                    "action": policy_action,
                    "decision_authority": policy_authority,
                    "decision_input_status": decision_input_status,
                    "decision_reason": decision_reason,
                    "fill_status": policy_fill.status,
                    "fill_reason": policy_fill.reason,
                    "fill_price_raw": float(policy_fill.price) if policy_fill.price is not None else None,
                    "planned_delta_qty": policy_plan.delta,
                    "plan_reference_raw": float(policy_plan.reference),
                    "plan_risk_exit": policy_plan.risk_exit,
                    "pre_quantity": pre_policy_state.quantity,
                    "pre_sellable_qty": pre_policy_state.sellable,
                    "model_sha256": policy_model_sha256,
                    "corporate_action_applied": target_action is not None,
                    "corporate_action_source_rows_sha256": (
                        target_action.source_rows_sha256 if target_action is not None else None
                    ),
                    "policy_fractional_share_discarded": float(policy_fractional),
                    "baseline_fractional_share_discarded": float(
                        buy_hold_fractional if baseline == "BUY_AND_HOLD" else l1_fractional
                    ),
                }
            )
    return output


def initial_holding_endowment(
    *,
    symbol: str,
    reference: Decimal,
    entry_reference: Decimal | None,
    capital: Decimal = REFERENCE_CAPITAL_CNY,
) -> PositionState:
    """Create a normalized pre-existing inventory without fabricating a BUY.

    Board-lot rules apply to orders after the common start.  The endowment is
    existing integer inventory, so a sub-minimum residual is valid but can only
    be fully sold by the shared execution rules.
    """

    if not capital.is_finite() or capital <= 0:
        raise ActionValueError("REFERENCE_CAPITAL_INVALID", symbol=symbol)
    if not reference.is_finite() or reference <= 0:
        raise ActionValueError("INITIAL_HOLDING_REFERENCE_UNAVAILABLE", symbol=symbol)
    if entry_reference is None or not entry_reference.is_finite() or entry_reference <= 0:
        raise ActionValueError("INITIAL_ENTRY_REFERENCE_UNAVAILABLE", symbol=symbol)
    quantity = int((capital / reference).to_integral_value(rounding=ROUND_FLOOR))
    if quantity <= 0:
        raise ActionValueError("INITIAL_HOLDING_UNAVAILABLE", symbol=symbol)
    cash = capital - reference * quantity
    if cash < 0 or cash >= reference:
        raise ActionValueError("INITIAL_HOLDING_NORMALIZATION_INVALID", symbol=symbol)
    return PositionState(
        quantity=quantity,
        sellable=quantity,
        cash=cash,
        capital=capital,
        entry_cost=entry_reference,
        holding_age=PRIMARY_HORIZON,
    )


def _roll_state_to_decision(state: PositionState) -> PositionState:
    from dataclasses import replace

    return replace(
        state,
        sellable=state.quantity,
        holding_age=(state.holding_age + 1 if state.holding_age is not None and state.quantity else state.holding_age),
    )


def _available_raw_close(bar: Mapping[str, Any]) -> Decimal | None:
    if bool(bar.get("is_suspended")):
        return None
    try:
        value = money(bar["close"])
    except (KeyError, TypeError, ValueError, ArithmeticError, ActionValueError):
        return None
    return value if value > 0 else None


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


def _effective_terminal_ordinal(
    bars: pd.DataFrame,
    nominal: int,
    *,
    max_defer: int,
    symbol: str,
    corporate_actions: CorporateActionBook,
    calendar_dates: Sequence[date],
) -> int | None:
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
        action = corporate_actions.on(symbol, calendar_dates[ordinal])
        if (
            action is not None
            and action.quantity_multiplier > 1
            and action.share_listing_date is not None
            and action.share_listing_date > calendar_dates[ordinal]
        ):
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
