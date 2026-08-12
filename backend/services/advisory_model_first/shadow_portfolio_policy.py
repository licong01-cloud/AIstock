from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_list_transition import (
    ACTION_ENTER,
    ACTION_EXIT,
    ACTION_HOLD,
    ACTION_WAITING,
    AdvisoryListTransitionEngine,
    AdvisoryTransitionCandidateV1,
    AdvisoryTransitionEpisodeV1,
    AdvisoryTransitionPolicyV1,
    AdvisoryTransitionRankObservationV1,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1
from backend.services.advisory_model_first.policy_episode_labels import (
    is_one_price_limit_down,
    is_one_price_limit_up,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


@dataclass(frozen=True)
class ShadowPortfolioResult:
    daily: pd.DataFrame
    episodes: pd.DataFrame
    metrics: dict[str, Any]


def replay_shadow_portfolio(
    *,
    rankings: pd.DataFrame,
    daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
    policy: AdvisoryTransitionPolicyV1,
    policy_sha256: str,
    cost_policy: AdvisoryPolicyCostV1,
    request_id: str,
    rank_depth: int = 40,
    candidate_decision_dates: Sequence[pd.Timestamp] | None = None,
    entry_priorities: pd.DataFrame | None = None,
) -> ShadowPortfolioResult:
    if policy.target_count != 5 or policy.rank_enter_threshold != 5:
        raise ValueError("shadow portfolio policy must use target_count=rank_enter_threshold=5")
    ranked = rankings.copy()
    required = {
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "selection_effective_rank",
        "combined_score",
    }
    if not required.issubset(ranked.columns):
        raise AdvisoryModelFirstError(
            "shadow portfolio rankings omit required columns",
            reason_code="ADVISORY_POLICY_RANK_INCOMPLETE",
        )
    ranked["decision_as_of_trade_date"] = pd.to_datetime(ranked["decision_as_of_trade_date"]).dt.normalize()
    ranked["target_trade_date"] = pd.to_datetime(ranked["target_trade_date"]).dt.normalize()
    ranked["instrument"] = ranked["instrument"].astype(str).str.upper()
    groups = {key: value.sort_values("selection_effective_rank") for key, value in ranked.groupby("decision_as_of_trade_date")}
    candidate_decision_set = (
        set(pd.DatetimeIndex(pd.to_datetime(list(candidate_decision_dates))).normalize())
        if candidate_decision_dates is not None
        else set(groups)
    )
    if not candidate_decision_set or not candidate_decision_set.issubset(groups):
        raise AdvisoryModelFirstError(
            "shadow portfolio candidate dates are absent from rank context",
            reason_code="ADVISORY_POLICY_RANK_INCOMPLETE",
        )
    last_candidate_decision = max(candidate_decision_set)
    first_candidate_decision = min(candidate_decision_set)
    priority_by_key = _entry_priority_map(entry_priorities)
    if any(len(group) != rank_depth for group in groups.values()):
        raise AdvisoryModelFirstError(
            "shadow portfolio requires an exact Top40 for every decision",
            reason_code="ADVISORY_POLICY_RANK_INCOMPLETE",
        )
    market = daily.sort_index()
    benchmark_open = _benchmark_opens(benchmark_daily)
    suspended = {
        (pd.Timestamp(row.trade_date).normalize(), str(row.instrument).upper())
        for row in suspend_rows.itertuples(index=False)
    }
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    calendar_position = {value: index for index, value in enumerate(calendar)}
    active: tuple[AdvisoryTransitionEpisodeV1, ...] = ()
    episode_records: list[dict[str, Any]] = []
    daily_records: list[dict[str, Any]] = []
    previous_benchmark_open: float | None = None
    cumulative = 1.0
    peak = 1.0
    engine = AdvisoryListTransitionEngine()

    for decision in sorted(groups):
        if decision < first_candidate_decision:
            continue
        if decision not in candidate_decision_set and decision > last_candidate_decision and not active:
            break
        frame = groups[decision]
        target_values = pd.DatetimeIndex(frame["target_trade_date"].unique())
        if len(target_values) != 1:
            raise AdvisoryModelFirstError(
                "shadow portfolio decision maps to multiple target dates",
                reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
            )
        target = target_values[0]
        position = calendar_position.get(decision)
        if position is None or position + 1 >= len(calendar) or calendar[position + 1] != target:
            raise AdvisoryModelFirstError(
                "shadow portfolio target is not the next trading day",
                reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
            )
        candidate_rows = {str(row.instrument).upper(): row for row in frame.itertuples(index=False)}
        candidates: list[AdvisoryTransitionCandidateV1] = []
        active_symbols = {episode.symbol for episode in active}
        for row in frame.itertuples(index=False):
            symbol = str(row.instrument).upper()
            if decision not in candidate_decision_set and symbol not in active_symbols:
                continue
            market_row = _market_row(market, target, symbol)
            suspended_today = (target, symbol) in suspended
            open_price = _finite(market_row.get("open")) if market_row is not None else None
            entry_available = bool(
                decision in candidate_decision_set
                and
                open_price is not None
                and not suspended_today
                and not is_one_price_limit_up(market_row)
            )
            exit_available = bool(
                open_price is not None
                and not suspended_today
                and not is_one_price_limit_down(market_row)
            )
            candidates.append(
                AdvisoryTransitionCandidateV1(
                    symbol=symbol,
                    rank=(
                        int(row.selection_effective_rank)
                        if symbol in active_symbols
                        else priority_by_key.get((decision, symbol), int(row.selection_effective_rank))
                    ),
                    score=float(row.combined_score),
                    entry_mark=open_price if entry_available else None,
                    exit_mark=open_price if open_price is not None and not suspended_today else None,
                    entry_mark_available=entry_available,
                    exit_mark_available=exit_available,
                    reason_code=("WAITING_PRICE" if symbol in active_symbols and not exit_available else None),
                )
            )
        for episode in active:
            if episode.symbol in candidate_rows:
                continue
            market_row = _market_row(market, target, episode.symbol)
            suspended_today = (target, episode.symbol) in suspended
            open_price = _finite(market_row.get("open")) if market_row is not None else None
            exit_available = bool(
                open_price is not None
                and not suspended_today
                and not is_one_price_limit_down(market_row)
            )
            candidates.append(
                AdvisoryTransitionCandidateV1(
                    symbol=episode.symbol,
                    rank=rank_depth + 1,
                    score=episode.current_score,
                    entry_mark=None,
                    exit_mark=open_price if open_price is not None and not suspended_today else None,
                    entry_mark_available=False,
                    exit_mark_available=exit_available,
                    reason_code="NOT_IN_CURRENT_TOPK",
                )
            )
        before = {episode.symbol: episode for episode in active}
        result = engine.transition(
            policy=policy,
            decision_trade_date=target.date(),
            candidates=tuple(candidates),
            active_episodes=active,
            rank_observation=AdvisoryTransitionRankObservationV1(
                status="COMPLETE",
                observed_max_selection_rank=rank_depth,
                active_rank_by_symbol={
                    episode.symbol: (
                        int(candidate_rows[episode.symbol].selection_effective_rank)
                        if episode.symbol in candidate_rows
                        else rank_depth + 1
                    )
                    for episode in active
                },
            ),
            episode_identity_allocator=lambda candidate: _episode_id(
                request_id=request_id,
                policy_sha256=policy_sha256,
                decision=decision,
                target=target,
                symbol=candidate.symbol,
            ),
            effective_entry_date=lambda _: target.date(),
            effective_exit_date=lambda _: target.date(),
            defer_stop_before_effective_entry=True,
            historical_mode=False,
            entry_mark_unavailable_action="WAITING",
        )
        if result.blocking_diagnostics:
            raise AdvisoryModelFirstError(
                "shadow portfolio transition has incomplete evidence",
                reason_code="ADVISORY_POLICY_PORTFOLIO_DATA_UNAVAILABLE",
                context={"date": target.date().isoformat(), "diagnostics": list(result.blocking_diagnostics)},
            )
        active = result.active_episodes
        entered = [item for item in result.decisions if item.action == ACTION_ENTER]
        exited = [item for item in result.decisions if item.action == ACTION_EXIT]
        waiting = [item for item in result.decisions if item.action == ACTION_WAITING]
        held = [item for item in result.decisions if item.action == ACTION_HOLD]
        for item in entered:
            if item.episode is None:
                raise AdvisoryModelFirstError(
                    "shadow portfolio ENTER omitted episode",
                    reason_code="ADVISORY_POLICY_PORTFOLIO_INVALID",
                )
            episode_records.append(
                {
                    "episode_id": item.episode.episode_id,
                    "instrument": item.symbol,
                    "entry_signal_date": decision,
                    "entry_trade_date": target,
                    "entry_price": item.entry_price,
                    "exit_trade_date": pd.NaT,
                    "exit_price": np.nan,
                    "exit_reason": None,
                    "status": "ACTIVE",
                }
            )
        for item in exited:
            if item.exit_price is None or item.episode is None:
                raise AdvisoryModelFirstError(
                    "shadow portfolio EXIT has no matching episode",
                    reason_code="ADVISORY_POLICY_PORTFOLIO_INVALID",
                )
            index = next(
                (idx for idx, value in enumerate(episode_records) if value["episode_id"] == item.episode.episode_id),
                None,
            )
            if index is None:
                raise AdvisoryModelFirstError(
                    "shadow portfolio EXIT has no matching episode",
                    reason_code="ADVISORY_POLICY_PORTFOLIO_INVALID",
                )
            entry_price = float(episode_records[index]["entry_price"])
            net_return_bps = (
                item.exit_price * (1 - cost_policy.sell_cost_bps / 10000.0)
                / (entry_price * (1 + cost_policy.buy_cost_bps / 10000.0))
                - 1
            ) * 10000.0
            episode_records[index].update(
                {
                    "exit_trade_date": target,
                    "exit_price": item.exit_price,
                    "exit_reason": item.reason_code,
                    "net_return_bps": net_return_bps,
                    "status": "EXITED",
                }
            )
        gross_daily = _portfolio_open_return(before, active, exited, target_count=policy.target_count)
        transaction_cost_bps = (
            len(entered) * cost_policy.buy_cost_bps + len(exited) * cost_policy.sell_cost_bps
        ) / policy.target_count
        net_daily_bps = gross_daily - transaction_cost_bps
        current_benchmark_open = _series_value(benchmark_open, target)
        benchmark_daily_bps = (
            0.0
            if previous_benchmark_open is None
            else (current_benchmark_open / previous_benchmark_open - 1.0) * 10000.0
            if current_benchmark_open is not None and previous_benchmark_open > 0
            else np.nan
        )
        if current_benchmark_open is None:
            raise AdvisoryModelFirstError(
                "shadow portfolio benchmark open is missing",
                reason_code="ADVISORY_POLICY_PORTFOLIO_DATA_UNAVAILABLE",
                context={"date": target.date().isoformat()},
            )
        cumulative *= 1.0 + net_daily_bps / 10000.0
        peak = max(peak, cumulative)
        daily_records.append(
            {
                "decision_as_of_trade_date": decision,
                "target_trade_date": target,
                "active_count": len(active),
                "cash_slot_count": policy.target_count - len(active),
                "entered_count": len(entered),
                "held_count": len(held),
                "exited_count": len(exited),
                "waiting_count": len(waiting),
                "replacement_budget_used": result.replacement_budget_used,
                "turnover_fraction": (len(entered) + len(exited)) / policy.target_count,
                "gross_return_bps": gross_daily,
                "transaction_cost_bps": transaction_cost_bps,
                "net_return_bps": net_daily_bps,
                "benchmark_return_bps": benchmark_daily_bps,
                "net_excess_return_bps": net_daily_bps - benchmark_daily_bps,
                "cumulative_nav": cumulative,
                "drawdown": cumulative / peak - 1.0,
            }
        )
        previous_benchmark_open = current_benchmark_open
    daily_frame = pd.DataFrame(daily_records)
    episodes_frame = pd.DataFrame(episode_records)
    exited_frame = episodes_frame[episodes_frame["status"] == "EXITED"] if not episodes_frame.empty else episodes_frame
    metrics = {
        "schema_version": "advisory_shadow_portfolio_metrics_v1",
        "policy_sha256": policy_sha256,
        "day_count": len(daily_frame),
        "episode_count": len(episodes_frame),
        "exited_episode_count": len(exited_frame),
        "active_episode_count": int((episodes_frame["status"] == "ACTIVE").sum()) if not episodes_frame.empty else 0,
        "mean_daily_net_return_bps": _mean(daily_frame, "net_return_bps"),
        "mean_daily_net_excess_return_bps": _mean(daily_frame, "net_excess_return_bps"),
        "maximum_drawdown": float(daily_frame["drawdown"].min()) if not daily_frame.empty else None,
        "mean_turnover_fraction": _mean(daily_frame, "turnover_fraction"),
        "completed_episode_hit_rate": (
            float((exited_frame["net_return_bps"] > 0).mean()) if not exited_frame.empty else None
        ),
    }
    return ShadowPortfolioResult(daily=daily_frame, episodes=episodes_frame, metrics=metrics)


def _portfolio_open_return(
    before: dict[str, AdvisoryTransitionEpisodeV1],
    active: tuple[AdvisoryTransitionEpisodeV1, ...],
    exited: list[Any],
    *,
    target_count: int,
) -> float:
    active_after = {episode.symbol: episode for episode in active}
    exit_by_symbol = {item.symbol: item for item in exited}
    total = 0.0
    for symbol, episode in before.items():
        previous = episode.still_active_mark_price or episode.entry_price
        current: float | None = None
        if symbol in active_after:
            current = active_after[symbol].still_active_mark_price
        elif symbol in exit_by_symbol:
            current = exit_by_symbol[symbol].exit_price
        if current is not None and previous > 0:
            total += (current / previous - 1.0) * 10000.0
    return total / target_count


def _episode_id(
    *, request_id: str, policy_sha256: str, decision: pd.Timestamp, target: pd.Timestamp, symbol: str
) -> str:
    return "advshad_" + canonical_json_sha256(
        {
            "request_id": request_id,
            "policy_sha256": policy_sha256,
            "decision": decision.date().isoformat(),
            "target": target.date().isoformat(),
            "instrument": symbol,
        }
    )[:24]


def _benchmark_opens(frame: pd.DataFrame) -> pd.Series:
    reset = frame.reset_index()
    if "datetime" not in reset or "open" not in reset:
        raise AdvisoryModelFirstError(
            "shadow portfolio benchmark is missing datetime/open",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
        )
    reset["datetime"] = pd.to_datetime(reset["datetime"]).dt.normalize()
    if reset["datetime"].duplicated().any():
        raise AdvisoryModelFirstError(
            "shadow portfolio benchmark has duplicate dates",
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
            "shadow portfolio market has duplicate rows",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
        )
    return row


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


def _mean(frame: pd.DataFrame, column: str) -> float | None:
    return float(frame[column].mean()) if not frame.empty else None


def _entry_priority_map(frame: pd.DataFrame | None) -> dict[tuple[pd.Timestamp, str], int]:
    if frame is None:
        return {}
    required = {"decision_as_of_trade_date", "instrument", "entry_priority_rank"}
    if not required.issubset(frame.columns):
        raise AdvisoryModelFirstError(
            "entry priority frame omits required columns",
            reason_code="ADVISORY_META_LABEL_PRIORITY_INVALID",
            context={"missing_columns": sorted(required - set(frame.columns))},
        )
    rows = frame.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()
    rows["instrument"] = rows["instrument"].astype(str).str.upper()
    rows["entry_priority_rank"] = pd.to_numeric(rows["entry_priority_rank"], errors="raise").astype(int)
    if rows.duplicated(["decision_as_of_trade_date", "instrument"]).any() or (
        rows["entry_priority_rank"] < 1
    ).any():
        raise AdvisoryModelFirstError(
            "entry priority rows are invalid",
            reason_code="ADVISORY_META_LABEL_PRIORITY_INVALID",
        )
    return {
        (row.decision_as_of_trade_date, row.instrument): row.entry_priority_rank
        for row in rows.itertuples(index=False)
    }
