from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_list_transition import (
    ACTION_EXIT,
    AdvisoryListTransitionEngine,
    AdvisoryTransitionCandidateV1,
    AdvisoryTransitionEpisodeV1,
    AdvisoryTransitionPolicyV1,
    AdvisoryTransitionRankObservationV1,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


@dataclass(frozen=True)
class PolicyEpisodeLabelResult:
    labels: pd.DataFrame
    coverage: pd.DataFrame


def build_policy_episode_labels(
    *,
    rankings: pd.DataFrame,
    daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
    policy: AdvisoryTransitionPolicyV1,
    policy_sha256: str,
    cost_policy: AdvisoryPolicyCostV1,
    request_identity: dict[str, str],
    candidate_decision_dates: Sequence[pd.Timestamp] | None = None,
    candidate_depth: int = 20,
    rank_depth: int = 40,
) -> PolicyEpisodeLabelResult:
    ranked = _normalize_rankings(rankings, rank_depth=rank_depth)
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    positions = {value: index for index, value in enumerate(calendar)}
    market = daily.sort_index()
    benchmark = _benchmark_open_table(benchmark_daily)
    suspended = {
        (pd.Timestamp(row.trade_date).normalize(), str(row.instrument).upper())
        for row in suspend_rows.itertuples(index=False)
    }
    rows_by_decision = {
        decision: group.sort_values("selection_effective_rank")
        for decision, group in ranked.groupby("decision_as_of_trade_date", sort=True)
    }
    output: list[dict[str, Any]] = []
    candidate_decision_set = (
        set(pd.DatetimeIndex(pd.to_datetime(list(candidate_decision_dates))).normalize())
        if candidate_decision_dates is not None
        else set(rows_by_decision)
    )
    missing_candidate_dates = sorted(candidate_decision_set - set(rows_by_decision))
    if missing_candidate_dates:
        raise AdvisoryModelFirstError(
            "policy candidate decision dates are absent from rank context",
            reason_code="ADVISORY_POLICY_RANK_INCOMPLETE",
            context={"dates": [value.date().isoformat() for value in missing_candidate_dates[:20]]},
        )
    for decision, group in rows_by_decision.items():
        if decision not in candidate_decision_set:
            continue
        for candidate in group[group["selection_effective_rank"] <= candidate_depth].itertuples(index=False):
            output.append(
                _build_one_episode(
                    candidate=candidate,
                    rows_by_decision=rows_by_decision,
                    market=market,
                    benchmark=benchmark,
                    suspended=suspended,
                    calendar=calendar,
                    positions=positions,
                    policy=policy,
                    policy_sha256=policy_sha256,
                    cost_policy=cost_policy,
                    request_identity=request_identity,
                    rank_depth=rank_depth,
                )
            )
    labels = pd.DataFrame(output)
    if labels.empty:
        raise AdvisoryModelFirstError(
            "policy episode builder produced no candidate labels",
            reason_code="ADVISORY_POLICY_LABEL_EMPTY",
        )
    coverage = (
        labels.groupby("decision_as_of_trade_date", sort=True)
        .agg(
            candidate_count=("instrument", "size"),
            matured_count=("label_status", lambda values: int((values == "MATURED").sum())),
            not_entered_count=("label_status", lambda values: int(values.astype(str).str.startswith("NOT_ENTERED").sum())),
            censored_count=("label_status", lambda values: int((values == "CENSORED_RIGHT_BOUNDARY").sum())),
            unavailable_count=("label_status", lambda values: int((values == "DATA_UNAVAILABLE").sum())),
        )
        .reset_index()
    )
    coverage["status"] = np.where(coverage["matured_count"] > 0, "AVAILABLE", "UNAVAILABLE")
    return PolicyEpisodeLabelResult(labels=labels, coverage=coverage)


def _build_one_episode(
    *,
    candidate: Any,
    rows_by_decision: dict[pd.Timestamp, pd.DataFrame],
    market: pd.DataFrame,
    benchmark: pd.Series,
    suspended: set[tuple[pd.Timestamp, str]],
    calendar: pd.DatetimeIndex,
    positions: dict[pd.Timestamp, int],
    policy: AdvisoryTransitionPolicyV1,
    policy_sha256: str,
    cost_policy: AdvisoryPolicyCostV1,
    request_identity: dict[str, str],
    rank_depth: int,
) -> dict[str, Any]:
    decision = pd.Timestamp(candidate.decision_as_of_trade_date).normalize()
    target = pd.Timestamp(candidate.target_trade_date).normalize()
    symbol = str(candidate.instrument).upper()
    rank = int(candidate.selection_effective_rank)
    score = float(candidate.combined_score)
    base = {
        **request_identity,
        "shadow_policy_sha256": policy_sha256,
        "cost_policy_sha256": cost_policy.policy_sha256,
        "decision_as_of_trade_date": decision,
        "target_trade_date": target,
        "instrument": symbol,
        "selection_rank": rank,
        "selection_score": score,
        "label_information_start": decision,
    }
    base["episode_label_id"] = "advpolep_" + canonical_json_sha256(
        {
            "request_id": request_identity.get("request_id"),
            "decision": decision.date().isoformat(),
            "target": target.date().isoformat(),
            "instrument": symbol,
            "policy_sha256": policy_sha256,
        }
    )[:24]
    if decision not in positions or target not in positions or positions[target] != positions[decision] + 1:
        raise AdvisoryModelFirstError(
            "policy candidate target is not the next trading day",
            reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
            context={"decision": decision.date().isoformat(), "target": target.date().isoformat()},
        )
    entry_row = _market_row(market, target, symbol)
    if entry_row is None:
        return _unavailable(base, "NOT_ENTERED_MISSING_OPEN", "entry_market_row_missing", target)
    if (target, symbol) in suspended:
        return _unavailable(base, "NOT_ENTERED_SUSPENDED", "entry_suspended", target)
    if is_one_price_limit_up(entry_row):
        return _unavailable(base, "NOT_ENTERED_LIMIT_UP", "entry_one_price_limit_up", target)
    entry_price = _finite(entry_row.get("open"))
    benchmark_entry = _series_value(benchmark, target)
    if entry_price is None or entry_price <= 0:
        return _unavailable(base, "NOT_ENTERED_MISSING_OPEN", "entry_open_invalid", target)
    if benchmark_entry is None or benchmark_entry <= 0:
        return _unavailable(base, "DATA_UNAVAILABLE", "benchmark_entry_open_missing", target)

    episode = AdvisoryTransitionEpisodeV1(
        episode_id=base["episode_label_id"],
        symbol=symbol,
        entry_signal_date=decision.date(),
        effective_entry_date=target.date(),
        entry_price=entry_price,
        entry_rank=rank,
        entry_score=score,
        current_rank=rank,
        current_score=score,
        still_active_mark_price=entry_price,
        max_runup_bps=0.0,
        max_drawdown_bps=0.0,
    )
    engine = AdvisoryListTransitionEngine()
    last_information_date = target
    for review_position in range(positions[target] + 1, len(calendar)):
        review_date = calendar[review_position]
        rank_decision = calendar[review_position - 1]
        last_information_date = review_date
        rank_frame = rows_by_decision.get(rank_decision)
        if rank_frame is None or len(rank_frame) != rank_depth:
            return _open_episode_result(
                base,
                episode,
                entry_price=entry_price,
                status="CENSORED_RIGHT_BOUNDARY" if rank_decision > max(rows_by_decision) else "DATA_UNAVAILABLE",
                reason="future_rank_not_available",
                information_end=review_date,
            )
        market_row = _market_row(market, review_date, symbol)
        if market_row is None:
            return _open_episode_result(
                base,
                episode,
                entry_price=entry_price,
                status="DATA_UNAVAILABLE",
                reason="holding_market_row_missing",
                information_end=review_date,
            )
        if (review_date, symbol) in suspended:
            continue
        review_open = _finite(market_row.get("open"))
        if review_open is None or review_open <= 0:
            return _open_episode_result(
                base,
                episode,
                entry_price=entry_price,
                status="DATA_UNAVAILABLE",
                reason="holding_open_invalid",
                information_end=review_date,
            )
        row = rank_frame[rank_frame["instrument"] == symbol]
        if row.empty:
            current_rank = rank_depth + 1
            current_score = episode.current_score
            reason_code = "NOT_IN_CURRENT_TOPK"
        else:
            current_rank = int(row.iloc[0]["selection_effective_rank"])
            current_score = float(row.iloc[0]["combined_score"])
            reason_code = None
        transition_candidate = AdvisoryTransitionCandidateV1(
            symbol=symbol,
            rank=current_rank,
            score=current_score,
            entry_mark=None,
            exit_mark=review_open,
            reason_code=reason_code,
        )
        result = engine.transition(
            policy=policy,
            decision_trade_date=review_date.date(),
            candidates=(transition_candidate,),
            active_episodes=(episode,),
            rank_observation=AdvisoryTransitionRankObservationV1(
                status="COMPLETE",
                observed_max_selection_rank=rank_depth,
                active_rank_by_symbol={symbol: current_rank},
            ),
            episode_identity_allocator=lambda _: "candidate_episode_must_not_reenter",
            effective_entry_date=lambda _: target.date(),
            effective_exit_date=lambda _: review_date.date(),
            defer_stop_before_effective_entry=True,
            historical_mode=False,
        )
        if result.blocking_diagnostics:
            return _open_episode_result(
                base,
                episode,
                entry_price=entry_price,
                status="DATA_UNAVAILABLE",
                reason=result.blocking_diagnostics[0],
                information_end=review_date,
            )
        exit_decision = next((item for item in result.decisions if item.action == ACTION_EXIT), None)
        if exit_decision is not None and is_one_price_limit_down(market_row):
            if exit_decision.episode is None:
                raise AdvisoryModelFirstError(
                    "deferred exit omitted its episode snapshot",
                    reason_code="ADVISORY_POLICY_LABEL_TRANSITION_INVALID",
                )
            episode = replace(exit_decision.episode, still_active_mark_price=review_open)
            continue
        if exit_decision is not None:
            if exit_decision.episode is None:
                raise AdvisoryModelFirstError(
                    "mature policy exit omitted its episode snapshot",
                    reason_code="ADVISORY_POLICY_LABEL_TRANSITION_INVALID",
                )
            return _mature_result(
                base,
                episode=exit_decision.episode,
                entry_price=entry_price,
                exit_price=review_open,
                exit_reason=exit_decision.reason_code,
                exit_signal_date=rank_decision,
                effective_exit_date=review_date,
                benchmark_entry=benchmark_entry,
                benchmark_exit=_series_value(benchmark, review_date),
                cost_policy=cost_policy,
            )
        if len(result.active_episodes) != 1:
            raise AdvisoryModelFirstError(
                "candidate episode transition lost its active episode",
                reason_code="ADVISORY_POLICY_LABEL_TRANSITION_INVALID",
            )
        episode = result.active_episodes[0]
    return _open_episode_result(
        base,
        episode,
        entry_price=entry_price,
        status="CENSORED_RIGHT_BOUNDARY",
        reason="data_cutoff_before_policy_exit",
        information_end=last_information_date,
    )


def _mature_result(
    base: dict[str, Any],
    *,
    episode: AdvisoryTransitionEpisodeV1,
    entry_price: float,
    exit_price: float,
    exit_reason: str,
    exit_signal_date: pd.Timestamp,
    effective_exit_date: pd.Timestamp,
    benchmark_entry: float,
    benchmark_exit: float | None,
    cost_policy: AdvisoryPolicyCostV1,
) -> dict[str, Any]:
    if benchmark_exit is None or benchmark_exit <= 0:
        return _open_episode_result(
            base,
            episode,
            entry_price=entry_price,
            status="DATA_UNAVAILABLE",
            reason="benchmark_exit_open_missing",
            information_end=effective_exit_date,
        )
    gross_bps = (exit_price / entry_price - 1.0) * 10000.0
    net_bps = (
        exit_price * (1.0 - cost_policy.sell_cost_bps / 10000.0)
        / (entry_price * (1.0 + cost_policy.buy_cost_bps / 10000.0))
        - 1.0
    ) * 10000.0
    benchmark_bps = (benchmark_exit / benchmark_entry - 1.0) * 10000.0
    net_excess_bps = net_bps - benchmark_bps
    return {
        **base,
        "entry_trade_date": base["target_trade_date"],
        "entry_price": entry_price,
        "exit_signal_date": exit_signal_date,
        "effective_exit_date": effective_exit_date,
        "exit_price": exit_price,
        "holding_trading_days": episode.holding_trading_days,
        "exit_reason": exit_reason,
        "gross_return_bps": gross_bps,
        "net_return_bps": net_bps,
        "benchmark_return_bps": benchmark_bps,
        "net_excess_return_bps": net_excess_bps,
        "take_label": int(net_excess_bps > 0.0),
        "confidence_target": net_excess_bps,
        "label_status": "MATURED",
        "label_reason": None,
        "label_information_end": effective_exit_date,
    }


def _unavailable(
    base: dict[str, Any], status: str, reason: str, information_end: pd.Timestamp
) -> dict[str, Any]:
    return {
        **base,
        "label_status": status,
        "label_reason": reason,
        "label_information_end": information_end,
        "take_label": None,
        "confidence_target": None,
    }


def _open_episode_result(
    base: dict[str, Any],
    episode: AdvisoryTransitionEpisodeV1,
    *,
    entry_price: float,
    status: str,
    reason: str,
    information_end: pd.Timestamp,
) -> dict[str, Any]:
    return {
        **base,
        "entry_trade_date": base["target_trade_date"],
        "entry_price": entry_price,
        "holding_trading_days": episode.holding_trading_days,
        "gross_return_bps": episode.return_bps,
        "label_status": status,
        "label_reason": reason,
        "label_information_end": information_end,
        "take_label": None,
        "confidence_target": None,
    }


def _normalize_rankings(rankings: pd.DataFrame, *, rank_depth: int) -> pd.DataFrame:
    required = {
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "selection_effective_rank",
        "combined_score",
    }
    if not required.issubset(rankings.columns):
        raise AdvisoryModelFirstError(
            "policy rankings omit required columns",
            reason_code="ADVISORY_POLICY_RANK_INCOMPLETE",
            context={"missing_columns": sorted(required - set(rankings.columns))},
        )
    result = rankings.copy()
    result["decision_as_of_trade_date"] = pd.to_datetime(result["decision_as_of_trade_date"]).dt.normalize()
    result["target_trade_date"] = pd.to_datetime(result["target_trade_date"]).dt.normalize()
    result["instrument"] = result["instrument"].astype(str).str.upper()
    if result.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        raise AdvisoryModelFirstError(
            "policy rankings contain duplicate date-symbol rows",
            reason_code="ADVISORY_POLICY_RANK_INCOMPLETE",
        )
    counts = result.groupby("decision_as_of_trade_date").size()
    if not (counts == rank_depth).all():
        raise AdvisoryModelFirstError(
            "policy rankings do not provide the exact required depth",
            reason_code="ADVISORY_POLICY_RANK_INCOMPLETE",
        )
    return result.sort_values(["decision_as_of_trade_date", "selection_effective_rank"])


def _benchmark_open_table(frame: pd.DataFrame) -> pd.Series:
    reset = frame.reset_index()
    if "datetime" not in reset or "open" not in reset:
        raise AdvisoryModelFirstError(
            "benchmark file input is missing datetime/open",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
        )
    reset["datetime"] = pd.to_datetime(reset["datetime"]).dt.normalize()
    if reset["datetime"].duplicated().any():
        raise AdvisoryModelFirstError(
            "benchmark file input has duplicate dates",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
        )
    return pd.to_numeric(reset.set_index("datetime")["open"], errors="coerce").sort_index()


def _market_row(frame: pd.DataFrame, value: pd.Timestamp, symbol: str) -> pd.Series | None:
    try:
        row = frame.loc[(value, symbol)]
    except KeyError:
        return None
    if isinstance(row, pd.DataFrame):
        raise AdvisoryModelFirstError(
            "policy daily market contains duplicate rows",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"date": value.date().isoformat(), "instrument": symbol},
        )
    return row


def is_one_price_limit_up(row: pd.Series) -> bool:
    return _one_price_limit(row, direction="up")


def is_one_price_limit_down(row: pd.Series) -> bool:
    return _one_price_limit(row, direction="down")


def _one_price_limit(row: pd.Series, *, direction: str) -> bool:
    flag = _finite(row.get(f"limit_{direction}"))
    factor = _finite(row.get("factor"))
    limit_price = _finite(row.get(f"{direction}_limit_price"))
    observed = _finite(row.get("low" if direction == "up" else "high"))
    if None in {flag, factor, limit_price, observed} or flag <= 0:
        return False
    adjusted = limit_price * factor
    return bool(observed >= adjusted - 1e-10 if direction == "up" else observed <= adjusted + 1e-10)


def _series_value(values: pd.Series, index: pd.Timestamp) -> float | None:
    if index not in values.index:
        return None
    return _finite(values.loc[index])


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None
