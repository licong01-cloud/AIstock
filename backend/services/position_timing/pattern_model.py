"""Fixed two-head model conditionalization for PT-NEXT-018.

The model predicts the incremental value of the already-frozen E1-vs-E0 and
X1-vs-X0 decisions.  It does not discover events, choose a stock, search
features, tune thresholds, or publish a serving model.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import hashlib
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .action_value import (
    ActionPlan,
    ActionValueError,
    FEATURE_ORDER,
    MARKET_FEATURES,
    STATE_FEATURES,
    PositionState,
    cutoff_on,
    market_features,
    state_features,
)
from .action_value_corporate_actions import CorporateActionBook
from .action_value_data import BENCHMARK, DailyCandidate
from .action_value_model import _lightgbm, estimator_parameters, monthly_training_windows, numeric_matrix
from .action_value_research import REFERENCE_CAPITAL_CNY
from .contracts import canonical_sha256, validate_sha256
from .pattern_research import (
    BLOCK_SESSIONS,
    INFERENCE_SEED,
    INITIAL_TRAINING_SESSIONS,
    _has_unbound_pattern_factor_change,
    evaluate_entry_and_exit_mechanisms,
    mean_interval,
    replay_full_policy_symbol,
)
from .pattern_rights_issue import RightsIssueAuthority
from .pattern_strategy import pattern_feature_frame, pattern_model_features


ENTRY_HEAD = "PATTERN_ENTRY_E1_MINUS_E0_V1"
EXIT_HEAD = "PATTERN_EXIT_X1_MINUS_X0_V1"
HEADS = (ENTRY_HEAD, EXIT_HEAD)
CORE_SET = "CORE_ONLY"
ENHANCED_SET = "CORE_PLUS_PATTERN_V1"
PATTERN_COLUMNS: tuple[str, ...] = (
    "pattern_low_age",
    "pattern_distance_low_atr",
    "pattern_distance_ma5_atr",
    "pattern_distance_ma10_atr",
    "pattern_ma5_slope_atr",
    "pattern_ma10_slope_atr",
    "pattern_low_to_prior_ma10_atr",
    "pattern_acceleration_atr",
    "pattern_volume_ratio",
)
FEATURE_ORDERS = {
    CORE_SET: FEATURE_ORDER,
    ENHANCED_SET: FEATURE_ORDER + PATTERN_COLUMNS,
}
MODEL_ROW_COLUMNS: tuple[str, ...] = (
    "symbol",
    "decision_as_of",
    "label_available_at",
    "objective",
    "net_action_value_bps",
    "state_sha256",
    "path_population",
    *FEATURE_ORDERS[ENHANCED_SET],
)
MODEL_CONTRACT: Mapping[str, Any] = {
    "schema_version": "position_timing_pattern_model_contract_v1",
    "heads": HEADS,
    "feature_sets": {key: value for key, value in FEATURE_ORDERS.items()},
    "entry_decision": "PREDICT_E1_MINUS_E0_GT_ZERO_THEN_E1_ELSE_E0",
    "exit_decision": "PREDICT_X1_MINUS_X0_GT_ZERO_THEN_X1_ELSE_X0",
    "threshold_bps": 0.0,
    "training": "MONTHLY_EXPANDING_LABEL_AVAILABLE_AT_CUTOFF",
    "initial_sessions": INITIAL_TRAINING_SESSIONS,
    "estimator": {**estimator_parameters(), "verbosity": -1},
    "search": False,
    "early_stopping": False,
}
MODEL_CONTRACT_SHA256 = canonical_sha256(MODEL_CONTRACT)


@dataclass
class PatternModel:
    metadata: dict[str, Any]
    boosters: dict[str, Any]

    def predict_one(self, features: Mapping[str, Any], *, head: str, decision_as_of: datetime) -> float:
        if head not in HEADS:
            raise ActionValueError("PATTERN_MODEL_HEAD_INVALID")
        available_at = datetime.fromisoformat(self.metadata["available_at"])
        if available_at.tzinfo is None or decision_as_of.tzinfo is None or available_at > decision_as_of:
            raise ActionValueError("PATTERN_MODEL_NOT_AVAILABLE")
        order = tuple(self.metadata["feature_order"])
        frame = pd.DataFrame([{name: features.get(name) for name in order}], columns=order)
        matrix, _ = numeric_matrix(
            frame,
            self.metadata["heads"][head]["medians"],
            feature_order=order,
        )
        value = float(self.boosters[head].predict(matrix, num_threads=1)[0])
        if not np.isfinite(value):
            raise ActionValueError("PATTERN_MODEL_PREDICTION_NON_FINITE")
        return value


@dataclass(frozen=True)
class WalkForwardPatternModels:
    models: Mapping[str, tuple[PatternModel, ...]]
    fit_log: pd.DataFrame
    receipt: Mapping[str, Any]


def _row_features(
    *,
    market: pd.Series,
    state: Mapping[str, Any],
    pattern: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        **{name: market[name] for name in MARKET_FEATURES},
        **{name: state[name] for name in STATE_FEATURES},
        **{name: pattern[name] for name in PATTERN_COLUMNS},
    }


def build_pattern_model_rows(
    candidate: DailyCandidate,
    *,
    symbols: Sequence[str],
    corporate_actions: CorporateActionBook,
    start: date,
    end: date,
    rights_issues: RightsIssueAuthority | None = None,
) -> tuple[pd.DataFrame, Mapping[str, Any]]:
    calendar_dates = tuple(timestamp.date() for timestamp in candidate.calendar)
    ordinals = {day: index for index, day in enumerate(calendar_dates)}
    if start not in ordinals or end not in ordinals:
        raise ActionValueError("PATTERN_MODEL_CALENDAR_MISMATCH")
    start_ordinal = ordinals[start] + INITIAL_TRAINING_SESSIONS
    end_ordinal = ordinals[end]
    benchmark = candidate.bars(BENCHMARK)["close"]
    rows: list[dict[str, Any]] = []
    excluded: Counter[str] = Counter()
    seen_exit: set[tuple[str, date, str]] = set()
    for symbol in symbols:
        try:
            bars = candidate.bars(symbol)
            if _has_unbound_pattern_factor_change(
                symbol=symbol,
                bars=bars,
                start_ordinal=start_ordinal,
                end_ordinal=end_ordinal,
                corporate_actions=corporate_actions,
                rights_issues=rights_issues,
            ):
                raise ActionValueError("UNBOUND_MATERIAL_FACTOR_CHANGE", symbol=symbol)
            pattern_frame = pattern_feature_frame(bars, symbol=symbol, corporate_actions=corporate_actions)
            core_frame = market_features(bars, benchmark)
            events, _, counts = evaluate_entry_and_exit_mechanisms(
                symbol=symbol,
                bars=bars,
                features=pattern_frame,
                calendar_dates=calendar_dates,
                corporate_actions=corporate_actions,
                start_ordinal=start_ordinal,
                end_ordinal=end_ordinal,
            )
            for event in events:
                comparison = event["comparison"]
                if comparison not in {
                    "E1_MINUS_E0",
                    "X1_MINUS_X0",
                    "X1_MINUS_X0_R0_ENTRY_DIAGNOSTIC",
                }:
                    continue
                anchor = event["anchor_date"]
                ordinal = ordinals[anchor]
                try:
                    pattern_values = pattern_model_features(pattern_frame, ordinal)
                except ActionValueError as exc:
                    excluded[exc.code] += 1
                    continue
                market = core_frame.iloc[ordinal]
                if market.loc[list(MARKET_FEATURES)].isna().any():
                    excluded["PATTERN_MODEL_CORE_FEATURE_UNAVAILABLE"] += 1
                    continue
                if comparison == "E1_MINUS_E0":
                    plan = ActionPlan(
                        symbol,
                        int(event["planned_quantity_at_breakout"]),
                        Decimal(str(bars.iloc[ordinal]["close"])),
                    )
                    state = PositionState(0, 0, REFERENCE_CAPITAL_CNY, REFERENCE_CAPITAL_CNY)
                    state_values = state_features(state, plan)
                    head = ENTRY_HEAD
                    state_sha256 = canonical_sha256(
                        {"state": "CASH", "planned_quantity": plan.delta, "anchor": anchor.isoformat()}
                    )
                    path_population = "BREAKOUT_ALL_EVENTS"
                else:
                    state_values = {name: event[name] for name in STATE_FEATURES}
                    head = EXIT_HEAD
                    state_sha256 = str(event["state_sha256"])
                    key = (symbol, anchor, state_sha256)
                    if key in seen_exit:
                        continue
                    seen_exit.add(key)
                    path_population = (
                        "E0_CAUSAL_HOLDING" if comparison == "X1_MINUS_X0" else "R0_PULLBACK_CAUSAL_HOLDING"
                    )
                values = _row_features(market=market, state=state_values, pattern=pattern_values)
                effective = event["effective_terminal_date"]
                rows.append(
                    {
                        "symbol": symbol,
                        "decision_as_of": cutoff_on(anchor),
                        "label_available_at": cutoff_on(effective),
                        "objective": head,
                        "net_action_value_bps": float(event["net_incremental_bps"]),
                        "state_sha256": state_sha256,
                        "path_population": path_population,
                        **values,
                    }
                )
            excluded.update({f"MECHANISM_{key}": value for key, value in counts.items() if key.startswith("UNKNOWN_")})
        except ActionValueError as exc:
            excluded[exc.code] += 1
    frame = pd.DataFrame(rows, columns=MODEL_ROW_COLUMNS)
    coverage = {
        "schema_version": "position_timing_pattern_model_rows_coverage_v1",
        "requested_symbol_count": len(symbols),
        "row_count": len(frame),
        "head_counts": frame["objective"].value_counts().to_dict() if not frame.empty else {},
        "exit_population_counts": (
            frame.loc[frame["objective"].eq(EXIT_HEAD), "path_population"].value_counts().to_dict()
            if not frame.empty
            else {}
        ),
        "excluded": dict(sorted(excluded.items())),
        "coverage_complete": not excluded,
    }
    coverage["coverage_sha256"] = canonical_sha256(coverage)
    return frame, coverage


def fit_pattern_model(
    rows: pd.DataFrame,
    *,
    feature_set: str,
    cutoff: datetime,
    available_at: datetime,
    source_sha256: str,
    request_sha256: str,
    source_commit: str,
) -> PatternModel:
    if (
        feature_set not in FEATURE_ORDERS
        or cutoff.tzinfo is None
        or available_at.tzinfo is None
        or cutoff > available_at
    ):
        raise ActionValueError("PATTERN_MODEL_FIT_SPEC_INVALID")
    validate_sha256(source_sha256, field="pattern_model_source_sha256")
    validate_sha256(request_sha256, field="pattern_model_request_sha256")
    if len(source_commit) != 40 or any(character not in "0123456789abcdef" for character in source_commit):
        raise ActionValueError("PATTERN_MODEL_SOURCE_COMMIT_INVALID")
    order = FEATURE_ORDERS[feature_set]
    required = {"decision_as_of", "label_available_at", "objective", "net_action_value_bps", *order}
    if not required.issubset(rows):
        raise ActionValueError("PATTERN_MODEL_TRAINING_SCHEMA_MISSING")
    if rows.empty:
        raise ActionValueError("PATTERN_MODEL_NOT_ESTIMABLE")
    for column in ("decision_as_of", "label_available_at"):
        if rows[column].isna().any() or any(pd.Timestamp(value).tzinfo is None for value in rows[column]):
            raise ActionValueError("PATTERN_MODEL_LABEL_TIME_INVALID", column=column)
    decisions = pd.to_datetime(rows["decision_as_of"], utc=True)
    availability = pd.to_datetime(rows["label_available_at"], utc=True)
    if (availability <= decisions).any():
        raise ActionValueError("PATTERN_MODEL_LABEL_INTERVAL_INVALID")
    eligible = rows.loc[(decisions < cutoff) & (availability <= cutoff)].copy()
    if eligible.empty or set(eligible["objective"]) != set(HEADS):
        raise ActionValueError("PATTERN_MODEL_NOT_ESTIMABLE")
    library = _lightgbm()
    boosters: dict[str, Any] = {}
    head_metadata: dict[str, Any] = {}
    for head in HEADS:
        selected = eligible.loc[eligible["objective"].eq(head)]
        matrix, medians = numeric_matrix(selected.loc[:, order], feature_order=order)
        estimator = library.LGBMRegressor(**MODEL_CONTRACT["estimator"])
        estimator.fit(matrix, selected["net_action_value_bps"].to_numpy(float))
        booster = estimator.booster_
        text = booster.model_to_string()
        boosters[head] = booster
        head_metadata[head] = {
            "training_rows": len(selected),
            "medians": medians,
            "label_available_max": pd.to_datetime(selected["label_available_at"], utc=True).max().isoformat(),
            "target_mean_bps": float(selected["net_action_value_bps"].mean()),
            "text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        }
    metadata = {
        "schema_version": "position_timing_pattern_model_v1",
        "feature_set": feature_set,
        "feature_order": order,
        "model_contract_sha256": MODEL_CONTRACT_SHA256,
        "source_sha256": source_sha256,
        "request_sha256": request_sha256,
        "source_commit": source_commit,
        "training_cutoff": cutoff.isoformat(),
        "available_at": available_at.isoformat(),
        "package_version": library.__version__,
        "parameters": MODEL_CONTRACT["estimator"],
        "heads": head_metadata,
        "temporal_mode": "HISTORICAL_REPLAY_NOT_SERVING",
    }
    metadata["model_sha256"] = canonical_sha256(metadata)
    return PatternModel(metadata, boosters)


def walk_forward_pattern_models(
    rows: pd.DataFrame,
    *,
    calendar_dates: Sequence[date],
    source_sha256: str,
    request_sha256: str,
    source_commit: str,
) -> WalkForwardPatternModels:
    models: dict[str, list[PatternModel]] = {CORE_SET: [], ENHANCED_SET: []}
    log: list[dict[str, Any]] = []
    for window in monthly_training_windows(calendar_dates, initial_sessions=INITIAL_TRAINING_SESSIONS):
        for feature_set in (CORE_SET, ENHANCED_SET):
            try:
                model = fit_pattern_model(
                    rows,
                    feature_set=feature_set,
                    cutoff=window["cutoff"],
                    available_at=window["available_at"],
                    source_sha256=source_sha256,
                    request_sha256=request_sha256,
                    source_commit=source_commit,
                )
                models[feature_set].append(model)
                log.append(
                    {
                        "feature_set": feature_set,
                        "training_cutoff": window["cutoff"],
                        "available_at": window["available_at"],
                        "status": "FITTED",
                        "model_sha256": model.metadata["model_sha256"],
                        "entry_rows": model.metadata["heads"][ENTRY_HEAD]["training_rows"],
                        "exit_rows": model.metadata["heads"][EXIT_HEAD]["training_rows"],
                    }
                )
            except ActionValueError as exc:
                if exc.code != "PATTERN_MODEL_NOT_ESTIMABLE":
                    raise
                log.append(
                    {
                        "feature_set": feature_set,
                        "training_cutoff": window["cutoff"],
                        "available_at": window["available_at"],
                        "status": "MODEL_NOT_ESTIMABLE",
                        "reason_code": exc.code,
                    }
                )
    frozen = {key: tuple(value) for key, value in models.items()}
    receipt = {
        "schema_version": "position_timing_pattern_walk_forward_models_v1",
        "model_contract_sha256": MODEL_CONTRACT_SHA256,
        "fit_attempt_count": len(log),
        "fit_success_count": sum(item["status"] == "FITTED" for item in log),
        "model_counts": {key: len(value) for key, value in frozen.items()},
        "serving": False,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    return WalkForwardPatternModels(frozen, pd.DataFrame(log), receipt)


def _available_model(models: Sequence[PatternModel], decision_as_of: datetime) -> PatternModel:
    eligible = [model for model in models if datetime.fromisoformat(model.metadata["available_at"]) <= decision_as_of]
    if not eligible:
        raise ActionValueError("PATTERN_MODEL_UNAVAILABLE")
    return eligible[-1]


def _model_selectors(
    *,
    models: Sequence[PatternModel],
    feature_set: str,
    market: pd.DataFrame,
    calendar_dates: Sequence[date],
):
    order = FEATURE_ORDERS[feature_set]

    def values_for(*, ordinal: int, state: PositionState, plan: ActionPlan, features: pd.DataFrame) -> dict[str, Any]:
        core = market.iloc[ordinal]
        if core.loc[list(MARKET_FEATURES)].isna().any():
            raise ActionValueError("PATTERN_MODEL_CURRENT_CORE_UNAVAILABLE")
        pattern = pattern_model_features(features, ordinal)
        values = _row_features(market=core, state=state_features(state, plan), pattern=pattern)
        return {name: values[name] for name in order}

    def entry_selector(**kwargs: Any) -> Mapping[str, Any]:
        ordinal = int(kwargs["ordinal"])
        decision_as_of = cutoff_on(calendar_dates[ordinal])
        model = _available_model(models, decision_as_of)
        inputs = values_for(
            ordinal=ordinal,
            state=kwargs["state"],
            plan=kwargs["plan"],
            features=kwargs["features"],
        )
        value = model.predict_one(inputs, head=ENTRY_HEAD, decision_as_of=decision_as_of)
        return {
            "choice": "E1" if value > 0 else "E0",
            "authority": "PATTERN_MODEL_ENTRY",
            "predicted_incremental_bps": value,
            "model_sha256": model.metadata["model_sha256"],
        }

    def exit_selector(**kwargs: Any) -> Mapping[str, Any]:
        ordinal = int(kwargs["ordinal"])
        decision_as_of = cutoff_on(calendar_dates[ordinal])
        model = _available_model(models, decision_as_of)
        state = kwargs["state"]
        plan = ActionPlan(kwargs["symbol"], -state.sellable, kwargs["reference"])
        inputs = values_for(ordinal=ordinal, state=state, plan=plan, features=kwargs["features"])
        value = model.predict_one(inputs, head=EXIT_HEAD, decision_as_of=decision_as_of)
        return {
            "execute": value > 0,
            "authority": "PATTERN_MODEL_EXIT",
            "predicted_incremental_bps": value,
            "model_sha256": model.metadata["model_sha256"],
        }

    return entry_selector, exit_selector


def replay_model_outer(
    candidate: DailyCandidate,
    *,
    evaluation_symbols: Sequence[str],
    corporate_actions: CorporateActionBook,
    start: date,
    end: date,
    models: Mapping[str, Sequence[PatternModel]],
    rights_issues: RightsIssueAuthority | None = None,
    parent_count: int = 1,
    additional_friction_bps: Decimal = Decimal(0),
) -> tuple[pd.DataFrame, Mapping[str, Any]]:
    if not all(models.get(key) for key in (CORE_SET, ENHANCED_SET)):
        raise ActionValueError("PATTERN_MODEL_OUTER_MODELS_UNAVAILABLE")
    calendar_dates = tuple(timestamp.date() for timestamp in candidate.calendar)
    ordinals = {day: index for index, day in enumerate(calendar_dates)}
    common_available = max(
        datetime.fromisoformat(models[key][0].metadata["available_at"]).date() for key in (CORE_SET, ENHANCED_SET)
    )
    start_ordinal = max(ordinals[start] + INITIAL_TRAINING_SESSIONS, ordinals[common_available])
    terminal_ordinal = ordinals[end] - 5
    benchmark = candidate.bars(BENCHMARK)["close"]
    frames: dict[str, list[dict[str, Any]]] = {CORE_SET: [], ENHANCED_SET: [], "PROTOTYPE": []}
    excluded: Counter[str] = Counter()
    for symbol in evaluation_symbols:
        try:
            bars = candidate.bars(symbol)
            if _has_unbound_pattern_factor_change(
                symbol=symbol,
                bars=bars,
                start_ordinal=start_ordinal,
                end_ordinal=ordinals[end],
                corporate_actions=corporate_actions,
                rights_issues=rights_issues,
            ):
                raise ActionValueError("UNBOUND_MATERIAL_FACTOR_CHANGE", symbol=symbol)
            pattern = pattern_feature_frame(bars, symbol=symbol, corporate_actions=corporate_actions)
            core = market_features(bars, benchmark)
            for feature_set in (CORE_SET, ENHANCED_SET):
                entry_selector, exit_selector = _model_selectors(
                    models=models[feature_set],
                    feature_set=feature_set,
                    market=core,
                    calendar_dates=calendar_dates,
                )
                rows, _, _ = replay_full_policy_symbol(
                    symbol=symbol,
                    bars=bars,
                    features=pattern,
                    calendar_dates=calendar_dates,
                    corporate_actions=corporate_actions,
                    start_ordinal=start_ordinal,
                    terminal_ordinal=terminal_ordinal,
                    entry_selector=entry_selector,
                    exit_selector=exit_selector,
                    parent_count=parent_count,
                    additional_friction_bps=additional_friction_bps,
                )
                frames[feature_set].extend(row for row in rows if row["comparison"] == "P_MINUS_BUY_AND_HOLD")
            prototype, _, _ = replay_full_policy_symbol(
                symbol=symbol,
                bars=bars,
                features=pattern,
                calendar_dates=calendar_dates,
                corporate_actions=corporate_actions,
                start_ordinal=start_ordinal,
                terminal_ordinal=terminal_ordinal,
                parent_count=parent_count,
                additional_friction_bps=additional_friction_bps,
            )
            frames["PROTOTYPE"].extend(row for row in prototype if row["comparison"] == "P_MINUS_BUY_AND_HOLD")
        except ActionValueError as exc:
            excluded[exc.code] += 1
    renamed = {}
    for key, records in frames.items():
        column = {CORE_SET: "core_minus_bh_bps", ENHANCED_SET: "enhanced_minus_bh_bps", "PROTOTYPE": "p_minus_bh_bps"}[
            key
        ]
        frame = pd.DataFrame(records)
        if frame.empty:
            return pd.DataFrame(), {
                "coverage_complete": False,
                "requested_symbol_count": len(evaluation_symbols),
                "completed_symbol_count": 0,
                "common_prediction_start": calendar_dates[start_ordinal].isoformat(),
                "excluded": dict(sorted(excluded.items())),
                "reason_code": "NO_OUTER_ROWS",
            }
        renamed[key] = frame[["symbol", "valuation_date", "incremental_net_value_bps"]].rename(
            columns={"incremental_net_value_bps": column}
        )
    merged = renamed[ENHANCED_SET].merge(renamed[CORE_SET], on=["symbol", "valuation_date"], validate="one_to_one")
    merged = merged.merge(renamed["PROTOTYPE"], on=["symbol", "valuation_date"], validate="one_to_one")
    merged["enhanced_minus_core_bps"] = merged["enhanced_minus_bh_bps"] - merged["core_minus_bh_bps"]
    merged["enhanced_minus_p_bps"] = merged["enhanced_minus_bh_bps"] - merged["p_minus_bh_bps"]
    coverage = {
        "coverage_complete": not excluded and merged["symbol"].nunique() == len(evaluation_symbols),
        "requested_symbol_count": len(evaluation_symbols),
        "completed_symbol_count": int(merged["symbol"].nunique()),
        "common_prediction_start": calendar_dates[start_ordinal].isoformat(),
        "excluded": dict(sorted(excluded.items())),
    }
    return merged, coverage


def model_comparisons(outer: pd.DataFrame, *, coverage_complete: bool, samples: int = 5000) -> Mapping[str, Any]:
    specifications = (
        ("ENHANCED_MINUS_CORE", "enhanced_minus_core_bps"),
        ("ENHANCED_MINUS_P", "enhanced_minus_p_bps"),
        ("ENHANCED_MINUS_BUY_AND_HOLD", "enhanced_minus_bh_bps"),
    )
    output: dict[str, Any] = {}
    for offset, (name, column) in enumerate(specifications):
        daily = outer.groupby("valuation_date")[column].mean().to_numpy(float)
        inference = mean_interval(
            daily,
            block_sessions=BLOCK_SESSIONS,
            samples=samples,
            seed=INFERENCE_SEED + 30 + offset,
            confidence_level=0.99,
        )
        nominal = mean_interval(
            daily,
            block_sessions=BLOCK_SESSIONS,
            samples=samples,
            seed=INFERENCE_SEED + 30 + offset,
            confidence_level=0.95,
        )
        evidence = "INCONCLUSIVE"
        if coverage_complete and inference["lower_bps"] is not None and inference["lower_bps"] > 0:
            evidence = "SUPPORTED"
        elif coverage_complete and inference["upper_bps"] is not None and inference["upper_bps"] < 0:
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


def historical_model_event_predictions(
    rows: pd.DataFrame,
    *,
    models: Mapping[str, Sequence[PatternModel]],
    feature_set: str = ENHANCED_SET,
) -> pd.DataFrame:
    """Create deterministic historical shadow explanations, never serving cards."""

    order = FEATURE_ORDERS[feature_set]
    output: list[dict[str, Any]] = []
    for row in rows.sort_values(["decision_as_of", "symbol", "objective"]).itertuples(index=False):
        decision_as_of = pd.Timestamp(row.decision_as_of).to_pydatetime()
        try:
            model = _available_model(models[feature_set], decision_as_of)
        except ActionValueError:
            output.append(
                {
                    "symbol": row.symbol,
                    "decision_as_of": decision_as_of,
                    "objective": row.objective,
                    "status": "MODEL_UNAVAILABLE",
                    "proposed_action": "UNAVAILABLE",
                    "predicted_incremental_bps": None,
                    "model_sha256": None,
                    "realized_incremental_bps": float(row.net_action_value_bps),
                }
            )
            continue
        features = {name: getattr(row, name) for name in order}
        value = model.predict_one(features, head=row.objective, decision_as_of=decision_as_of)
        if row.objective == ENTRY_HEAD:
            action = "WAIT_FOR_PULLBACK_E1" if value > 0 else "OPEN_NEXT_SESSION_E0"
        else:
            action = "EXIT_NEXT_SESSION_X1" if value > 0 else "RISK_OR_HOLD_X0"
        output.append(
            {
                "symbol": row.symbol,
                "decision_as_of": decision_as_of,
                "objective": row.objective,
                "status": "HISTORICAL_SHADOW_NOT_SERVING",
                "proposed_action": action,
                "predicted_incremental_bps": value,
                "model_sha256": model.metadata["model_sha256"],
                "realized_incremental_bps": float(row.net_action_value_bps),
                "state_sha256": row.state_sha256,
                "path_population": row.path_population,
            }
        )
    return pd.DataFrame(output)


__all__ = (
    "CORE_SET",
    "ENHANCED_SET",
    "ENTRY_HEAD",
    "EXIT_HEAD",
    "FEATURE_ORDERS",
    "MODEL_ROW_COLUMNS",
    "MODEL_CONTRACT",
    "MODEL_CONTRACT_SHA256",
    "PatternModel",
    "WalkForwardPatternModels",
    "build_pattern_model_rows",
    "fit_pattern_model",
    "historical_model_event_predictions",
    "model_comparisons",
    "replay_model_outer",
    "walk_forward_pattern_models",
)
