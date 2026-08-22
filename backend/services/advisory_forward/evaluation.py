from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from math import isfinite
from statistics import mean
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.advisory_forward.errors import AdvisoryForwardModelEvaluationError
from backend.services.advisory_forward.models import (
    AdvisoryForwardModelEvaluationV1,
    AdvisoryForwardModelObservationOutcomeV1,
)
from backend.services.advisory_model_first.model_binding_resolution import META_LABEL_MODEL_ROLE
from backend.services.advisory_model_first.policy_contracts import (
    AdvisoryPolicyCostV1,
    transition_policy_from_payload,
)
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio
from backend.services.advisory_program import MARKET_PRICE_UNIT_DIVISOR
from backend.services.selection_center.models import SelectionRunStatus
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


EVALUATION_CONTRACT_VERSION = "advisory_forward_model_evaluation_v1"
REASON_CONTRACT_INVALID = "ADVISORY_FORWARD_MODEL_EVALUATION_CONTRACT_INVALID"
REASON_SEQUENCE_INCOMPLETE = "ADVISORY_FORWARD_MODEL_EVALUATION_SEQUENCE_INCOMPLETE"
REASON_MARKET_UNAVAILABLE = "ADVISORY_FORWARD_MODEL_EVALUATION_MARKET_DATA_UNAVAILABLE"


@dataclass(frozen=True)
class AdvisoryForwardEvaluationMarketData:
    daily: pd.DataFrame
    benchmark_daily: pd.DataFrame
    suspend_rows: pd.DataFrame
    trading_calendar: pd.DatetimeIndex
    input_sha256: str


@dataclass(frozen=True)
class AdvisoryForwardEvaluationBuild:
    evaluation: AdvisoryForwardModelEvaluationV1
    new_outcomes: tuple[AdvisoryForwardModelObservationOutcomeV1, ...]
    unresolved_observation_ids: tuple[str, ...]


class AdvisoryForwardEvaluationMarketSource:
    """Bounded PostgreSQL market reader for mature forward evaluation only."""

    def __init__(self, *, conn_factory: Any | None = None) -> None:
        self._conn_factory = conn_factory or get_conn

    def load(
        self,
        *,
        symbols: Sequence[str],
        benchmark_instrument: str,
        start_trade_date: date,
        end_trade_date: date,
    ) -> AdvisoryForwardEvaluationMarketData:
        if start_trade_date > end_trade_date:
            raise ValueError("forward evaluation market range must satisfy start <= end")
        normalized = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
        if not normalized:
            raise _market_unavailable("forward evaluation symbol roster is empty")
        benchmark = str(benchmark_instrument).strip().upper()
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT cal_date
                    FROM market.trading_calendar
                    WHERE is_trading = TRUE
                      AND cal_date BETWEEN (
                          SELECT MAX(cal_date) FROM market.trading_calendar
                          WHERE is_trading = TRUE AND cal_date < %s
                      ) AND %s
                    ORDER BY cal_date
                    """,
                    (start_trade_date, end_trade_date),
                )
                calendar_rows = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    """
                    WITH base_adj AS (
                        SELECT DISTINCT ON (ts_code) ts_code, adj_factor AS base_adj_factor
                        FROM market.adj_factor
                        WHERE ts_code = ANY(%(symbols)s) AND trade_date <= %(end_date)s
                        ORDER BY ts_code, trade_date DESC
                    )
                    SELECT price.trade_date, TRIM(price.ts_code) AS ts_code,
                           price.open_li, price.high_li, price.low_li, price.close_li,
                           adj.adj_factor, base.base_adj_factor,
                           limits.pre_close, limits.up_limit, limits.down_limit
                    FROM market.kline_daily_raw AS price
                    LEFT JOIN market.adj_factor AS adj
                      ON adj.ts_code = price.ts_code AND adj.trade_date = price.trade_date
                    LEFT JOIN base_adj AS base ON base.ts_code = price.ts_code
                    LEFT JOIN market.stk_limit AS limits
                      ON limits.ts_code = price.ts_code AND limits.trade_date = price.trade_date
                    WHERE TRIM(price.ts_code) = ANY(%(symbols)s)
                      AND price.trade_date BETWEEN %(start_date)s AND %(end_date)s
                    ORDER BY price.trade_date, TRIM(price.ts_code)
                    """,
                    {
                        "symbols": normalized,
                        "start_date": start_trade_date,
                        "end_date": end_trade_date,
                    },
                )
                price_rows = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    """
                    SELECT trade_date, TRIM(ts_code) AS ts_code, suspend_type
                    FROM market.suspend_d
                    WHERE TRIM(ts_code) = ANY(%s)
                      AND COALESCE(TRIM(suspend_type), 'S') = 'S'
                      AND trade_date BETWEEN %s AND %s
                    ORDER BY trade_date, TRIM(ts_code)
                    """,
                    (normalized, start_trade_date, end_trade_date),
                )
                suspend_rows = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    """
                    SELECT trade_date, TRIM(ts_code) AS ts_code, open
                    FROM market.index_daily
                    WHERE TRIM(ts_code) = %s AND trade_date BETWEEN %s AND %s
                    ORDER BY trade_date
                    """,
                    (benchmark, start_trade_date, end_trade_date),
                )
                benchmark_rows = [dict(row) for row in cur.fetchall()]
        calendar = pd.DatetimeIndex(
            pd.Timestamp(row["cal_date"]).normalize() for row in calendar_rows
        )
        if len(calendar) < 2 or calendar[1].date() != start_trade_date or calendar[-1].date() != end_trade_date:
            raise _market_unavailable(
                "forward evaluation trading calendar does not cover the explicit range",
                context={"start_trade_date": start_trade_date.isoformat(), "end_trade_date": end_trade_date.isoformat()},
            )
        daily = _daily_frame(price_rows)
        benchmark_daily = _benchmark_frame(benchmark_rows, benchmark=benchmark)
        suspended = _suspend_frame(suspend_rows)
        expected_benchmark_dates = set(calendar[1:])
        actual_benchmark_dates = set(benchmark_daily.reset_index()["datetime"])
        if actual_benchmark_dates != expected_benchmark_dates:
            raise _market_unavailable(
                "forward evaluation benchmark does not cover every trading date",
                context={
                    "benchmark_instrument": benchmark,
                    "missing_dates": sorted(item.date().isoformat() for item in expected_benchmark_dates - actual_benchmark_dates),
                },
            )
        identity = {
            "schema_version": "advisory_forward_evaluation_market_input_v1",
            "start_trade_date": start_trade_date.isoformat(),
            "end_trade_date": end_trade_date.isoformat(),
            "symbols": normalized,
            "benchmark_instrument": benchmark,
            "calendar_rows": _canonical_rows(calendar_rows),
            "price_rows": _canonical_rows(price_rows),
            "suspend_rows": _canonical_rows(suspend_rows),
            "benchmark_rows": _canonical_rows(benchmark_rows),
        }
        return AdvisoryForwardEvaluationMarketData(
            daily=daily,
            benchmark_daily=benchmark_daily,
            suspend_rows=suspended,
            trading_calendar=calendar,
            input_sha256=canonical_json_sha256(identity),
        )


def build_forward_model_evaluation(
    *,
    observations: Sequence[Mapping[str, Any]],
    rank_contexts: Sequence[Mapping[str, Any]] | None = None,
    selection_runs: Mapping[str, Any],
    market: AdvisoryForwardEvaluationMarketData,
    as_of_trade_date: date,
    existing_outcome_observation_ids: set[str] | None = None,
    existing_outcome_count: int = 0,
) -> AdvisoryForwardEvaluationBuild:
    ordered = sorted(observations, key=lambda row: (row["target_trade_date"], row["observation_id"]))
    if not ordered:
        raise _contract_invalid("forward evaluation observation roster is empty")
    due = [row for row in ordered if row.get("maturity_trade_date") and row["maturity_trade_date"] <= as_of_trade_date]
    if not due:
        raise _contract_invalid("forward evaluation has no observation due at the explicit as-of date")
    contract = _evaluation_contract(ordered)
    contexts = sorted(
        rank_contexts or observations,
        key=lambda row: (row["target_trade_date"], str(row.get("forward_run_id") or "")),
    )
    if not contexts or contexts[0]["target_trade_date"] != ordered[0]["target_trade_date"]:
        raise _sequence_incomplete("forward evaluation rank context does not start with the epoch")
    expected_targets = [item.date() for item in market.trading_calendar[1:]]
    if market.trading_calendar[0].date() != contexts[0]["decision_as_of_trade_date"]:
        raise _sequence_incomplete(
            "forward evaluation calendar does not start at the first decision date",
            context={"first_decision": ordered[0]["decision_as_of_trade_date"].isoformat()},
        )
    actual_targets = [row["target_trade_date"] for row in contexts]
    if actual_targets != expected_targets:
        raise _sequence_incomplete(
            "forward evaluation observations are not continuous through the as-of watermark",
            context={"expected_targets": [item.isoformat() for item in expected_targets], "actual_targets": [item.isoformat() for item in actual_targets]},
        )
    ranking_records: list[dict[str, Any]] = []
    priority_records: list[dict[str, Any]] = []
    selection_identity: list[dict[str, Any]] = []
    required_market_keys: set[tuple[pd.Timestamp, str]] = set()
    observation_by_target = {row["target_trade_date"]: row for row in ordered}
    for context_row in contexts:
        selection_run_id = str(context_row.get("selection_run_id") or "")
        run = selection_runs.get(selection_run_id)
        if run is None:
            raise _sequence_incomplete(
                "forward evaluation Selection run is unavailable",
                context={"selection_run_id": selection_run_id},
            )
        run_status = getattr(run, "status", None)
        if run_status not in {SelectionRunStatus.SUCCEEDED, SelectionRunStatus.SUCCEEDED.value, "SUCCEEDED"}:
            raise _sequence_incomplete(
                "forward evaluation Selection run is not successful",
                context={"selection_run_id": selection_run_id, "status": str(run_status)},
            )
        if getattr(run, "trade_date", None) != context_row["target_trade_date"]:
            raise _sequence_incomplete(
                "forward evaluation Selection run target date differs from observation",
                context={"selection_run_id": selection_run_id},
            )
        rows = sorted(run.aggregate_results, key=lambda item: (int(item.rank), str(item.symbol)))
        top40 = [item for item in rows if int(item.rank) <= 40]
        ranks = [int(item.rank) for item in top40]
        if len(top40) != 40 or ranks != list(range(1, 41)) or len({str(item.symbol).upper() for item in top40}) != 40:
            raise _sequence_incomplete(
                "forward evaluation Selection run does not contain an exact Top40",
                context={"selection_run_id": selection_run_id, "ranks": ranks},
            )
        decision = context_row["decision_as_of_trade_date"]
        target = context_row["target_trade_date"]
        for item in top40:
            symbol = str(item.symbol).upper()
            ranking_records.append(
                {
                    "decision_as_of_trade_date": decision,
                    "target_trade_date": target,
                    "instrument": symbol,
                    "selection_effective_rank": int(item.rank),
                    "combined_score": float(item.score),
                }
            )
            required_market_keys.add((pd.Timestamp(target).normalize(), symbol))
        observation = observation_by_target.get(target)
        if observation is not None:
            prediction = dict(observation.get("prediction_payload_json") or {})
            candidates = list(prediction.get("candidates") or [])
            _validate_priorities(candidates, observation=observation, top40=top40)
            for candidate in candidates:
                priority_records.append(
                    {
                        "decision_as_of_trade_date": decision,
                        "instrument": str(candidate["symbol"]).upper(),
                        "entry_priority_rank": int(candidate["entry_priority_rank"]),
                    }
                )
        selection_identity.append(
            {
                "selection_run_id": selection_run_id,
                "decision_as_of_trade_date": decision.isoformat(),
                "target_trade_date": target.isoformat(),
                "top40": [
                    {"symbol": str(item.symbol).upper(), "rank": int(item.rank), "score": float(item.score)}
                    for item in top40
                ],
            }
        )
    available_market_keys = set(market.daily.index)
    missing_market_keys = sorted(required_market_keys - available_market_keys)
    if missing_market_keys:
        raise _market_unavailable(
            "forward evaluation market data does not cover every Selection Top40 row",
            context={
                "missing_rows": [
                    {"trade_date": day.date().isoformat(), "symbol": symbol}
                    for day, symbol in missing_market_keys[:20]
                ],
                "missing_row_count": len(missing_market_keys),
            },
        )
    rankings = pd.DataFrame(ranking_records)
    priorities = pd.DataFrame(priority_records)
    result = replay_shadow_portfolio(
        rankings=rankings,
        daily=market.daily,
        benchmark_daily=market.benchmark_daily,
        suspend_rows=market.suspend_rows,
        trading_calendar=market.trading_calendar,
        policy=contract["policy"],
        policy_sha256=contract["shadow_policy_sha256"],
        cost_policy=contract["cost_policy"],
        request_id=f"forward_{contract['program_id']}_{contract['model_descriptor_sha256'][:16]}",
        rank_depth=40,
        candidate_decision_dates=[row["decision_as_of_trade_date"] for row in ordered],
        entry_priorities=priorities,
    )
    daily_records = _frame_records(result.daily)
    episode_records = _frame_records(result.episodes)
    existing_ids = set(existing_outcome_observation_ids or set())
    evaluation_id = _stable_id(
        "adveval",
        contract["program_id"],
        contract["model_descriptor_sha256"],
        str(ordered[0]["observation_id"]),
        as_of_trade_date.isoformat(),
    )
    outcomes: list[AdvisoryForwardModelObservationOutcomeV1] = []
    unresolved: list[str] = []
    episodes_by_decision: dict[str, list[dict[str, Any]]] = {}
    for episode in episode_records:
        key = str(episode.get("entry_signal_date") or "")[:10]
        episodes_by_decision.setdefault(key, []).append(episode)
    for observation in due:
        observation_id = str(observation["observation_id"])
        if observation_id in existing_ids:
            continue
        cohort = episodes_by_decision.get(observation["decision_as_of_trade_date"].isoformat(), [])
        active = [episode for episode in cohort if episode.get("status") != "EXITED"]
        if active:
            unresolved.append(observation_id)
            continue
        exited = [episode for episode in cohort if episode.get("status") == "EXITED"]
        returns = [float(episode["net_return_bps"]) for episode in exited]
        status = "NO_ENTRY" if not cohort else "MATURED"
        payload = {
            "schema_version": "advisory_forward_model_observation_outcome_v1",
            "observation_id": observation_id,
            "decision_as_of_trade_date": observation["decision_as_of_trade_date"].isoformat(),
            "target_trade_date": observation["target_trade_date"].isoformat(),
            "maturity_trade_date": observation["maturity_trade_date"].isoformat(),
            "status": status,
            "episodes": cohort,
        }
        outcomes.append(
            AdvisoryForwardModelObservationOutcomeV1(
                outcome_id=_stable_id("advout", observation_id),
                observation_id=observation_id,
                evaluation_id=evaluation_id,
                program_id=contract["program_id"],
                model_descriptor_sha256=contract["model_descriptor_sha256"],
                bundle_id=contract["bundle_id"],
                target_trade_date=observation["target_trade_date"],
                maturity_trade_date=observation["maturity_trade_date"],
                status=status,
                entered_episode_count=len(cohort),
                exited_episode_count=len(exited),
                completed_episode_hit_rate=(sum(value > 0 for value in returns) / len(returns)) if returns else None,
                mean_net_return_bps=mean(returns) if returns else None,
                outcome_payload_json=payload,
            )
        )
    resolved_total = existing_outcome_count + len(outcomes)
    metrics = {
        **dict(result.metrics),
        "schema_version": "advisory_forward_model_metrics_v1",
        "evidence_status": "READY" if resolved_total else "WAITING_DATA",
        "observation_count": len(ordered),
        "due_observation_count": len(due),
        "matured_outcome_count": resolved_total,
        "unresolved_due_observation_count": len(due) - resolved_total,
        "coverage": resolved_total / len(due),
        "first_target_trade_date": ordered[0]["target_trade_date"].isoformat(),
        "as_of_trade_date": as_of_trade_date.isoformat(),
        "last_due_maturity_trade_date": due[-1]["maturity_trade_date"].isoformat(),
    }
    roster_payload = [
        {
            "observation_id": row["observation_id"],
            "forward_run_id": row["forward_run_id"],
            "decision_as_of_trade_date": row["decision_as_of_trade_date"].isoformat(),
            "target_trade_date": row["target_trade_date"].isoformat(),
            "maturity_trade_date": row["maturity_trade_date"].isoformat(),
            "payload_sha256": row["payload_sha256"],
        }
        for row in ordered
    ]
    context_roster_payload = [
        {
            "forward_run_id": row["forward_run_id"],
            "observation_id": row.get("observation_id"),
            "decision_as_of_trade_date": row["decision_as_of_trade_date"].isoformat(),
            "target_trade_date": row["target_trade_date"].isoformat(),
            "payload_sha256": row.get("payload_sha256"),
        }
        for row in contexts
    ]
    result_payload = {
        "schema_version": EVALUATION_CONTRACT_VERSION,
        "epoch": {key: contract[key] for key in (
            "program_id", "model_descriptor_sha256", "bundle_id", "shadow_policy_sha256", "cost_policy_sha256"
        )},
        "daily": daily_records,
        "episodes": episode_records,
        "metrics": metrics,
    }
    evaluation = AdvisoryForwardModelEvaluationV1(
        evaluation_id=evaluation_id,
        program_id=contract["program_id"],
        model_descriptor_sha256=contract["model_descriptor_sha256"],
        bundle_id=contract["bundle_id"],
        shadow_policy_sha256=contract["shadow_policy_sha256"],
        cost_policy_sha256=contract["cost_policy_sha256"],
        first_observation_id=str(ordered[0]["observation_id"]),
        last_due_observation_id=str(due[-1]["observation_id"]),
        first_target_trade_date=ordered[0]["target_trade_date"],
        as_of_trade_date=as_of_trade_date,
        last_due_maturity_trade_date=due[-1]["maturity_trade_date"],
        observation_count=len(ordered),
        due_observation_count=len(due),
        matured_outcome_count=resolved_total,
        observation_roster_sha256=canonical_json_sha256(
            {"epoch_observations": roster_payload, "rank_context": context_roster_payload}
        ),
        selection_input_sha256=canonical_json_sha256(selection_identity),
        market_input_sha256=market.input_sha256,
        metrics_json=metrics,
        result_payload_json=result_payload,
    )
    return AdvisoryForwardEvaluationBuild(
        evaluation=evaluation,
        new_outcomes=tuple(outcomes),
        unresolved_observation_ids=tuple(sorted(unresolved)),
    )


def _evaluation_contract(observations: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    identities: set[tuple[str, str, str, str, str]] = set()
    policy_payload: dict[str, Any] | None = None
    cost_payload: dict[str, Any] | None = None
    for row in observations:
        prediction = dict(row.get("prediction_payload_json") or {})
        if (
            row.get("status") != "EXPERIMENTAL_SHADOW"
            or prediction.get("model_role") != META_LABEL_MODEL_ROLE
            or prediction.get("evaluation_contract_version") != EVALUATION_CONTRACT_VERSION
        ):
            raise _contract_invalid(
                "forward evaluation observation is not an eligible P0-D contract",
                context={"observation_id": row.get("observation_id")},
            )
        current_policy = prediction.get("shadow_policy")
        current_cost = prediction.get("cost_policy")
        if not isinstance(current_policy, dict) or not isinstance(current_cost, dict):
            raise _contract_invalid("forward evaluation observation omits frozen policy or cost")
        shadow_hash = str(prediction.get("shadow_policy_sha256") or "")
        cost_hash = str(prediction.get("cost_policy_sha256") or "")
        if canonical_json_sha256(current_policy) != shadow_hash:
            raise _contract_invalid("forward evaluation shadow policy hash differs from payload")
        try:
            policy = transition_policy_from_payload(current_policy)
            cost = AdvisoryPolicyCostV1.model_validate(current_cost)
        except ValueError as exc:
            raise _contract_invalid("forward evaluation policy contract is invalid") from exc
        if cost.policy_sha256 != cost_hash:
            raise _contract_invalid("forward evaluation cost policy hash differs from payload")
        identity = (
            str(row["program_id"]),
            str(row.get("model_descriptor_sha256") or ""),
            str(row.get("bundle_id") or ""),
            shadow_hash,
            cost_hash,
        )
        if any(not value for value in identity):
            raise _contract_invalid("forward evaluation epoch identity is incomplete")
        identities.add(identity)
        policy_payload = current_policy if policy_payload is None else policy_payload
        cost_payload = current_cost if cost_payload is None else cost_payload
        if current_policy != policy_payload or current_cost != cost_payload:
            raise _contract_invalid("forward evaluation epoch policy payload changed")
    if len(identities) != 1 or policy_payload is None or cost_payload is None:
        raise _contract_invalid("forward evaluation observation roster crosses epoch identities")
    program_id, descriptor, bundle, shadow_hash, cost_hash = next(iter(identities))
    policy = transition_policy_from_payload(policy_payload)
    if policy.target_count != 5 or policy.rank_enter_threshold != 5 or policy.rank_exit_threshold < 40:
        raise _contract_invalid("forward evaluation shadow policy does not preserve Top5/Top40 semantics")
    return {
        "program_id": program_id,
        "model_descriptor_sha256": descriptor,
        "bundle_id": bundle,
        "shadow_policy_sha256": shadow_hash,
        "cost_policy_sha256": cost_hash,
        "policy": policy,
        "cost_policy": AdvisoryPolicyCostV1.model_validate(cost_payload),
    }


def _validate_priorities(candidates: Sequence[Mapping[str, Any]], *, observation: Mapping[str, Any], top40: Sequence[Any]) -> None:
    if len(candidates) != 20:
        raise _sequence_incomplete("forward evaluation observation does not contain exact Top20 priorities")
    top20_by_symbol = {str(item.symbol).upper(): int(item.rank) for item in top40[:20]}
    candidate_by_symbol = {str(item.get("symbol") or "").upper(): item for item in candidates}
    entry_ranks = sorted(int(item.get("entry_priority_rank") or 0) for item in candidates)
    if set(candidate_by_symbol) != set(top20_by_symbol) or entry_ranks != list(range(1, 21)):
        raise _sequence_incomplete(
            "forward evaluation Top20 priority roster differs from Selection",
            context={"observation_id": observation.get("observation_id")},
        )
    for symbol, item in candidate_by_symbol.items():
        selection_rank = int(item.get("selection_effective_rank") or 0)
        exit_rank = int(item.get("selection_exit_rank") or 0)
        if selection_rank != top20_by_symbol[symbol] or exit_rank != selection_rank:
            raise _sequence_incomplete(
                "forward evaluation entry/exit rank contract differs from Selection",
                context={"observation_id": observation.get("observation_id"), "symbol": symbol},
            )


def _daily_frame(rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for row in rows:
        factor = _required_number(row.get("adj_factor"), field="adj_factor") / _required_number(
            row.get("base_adj_factor"), field="base_adj_factor"
        )
        open_raw = _required_number(row.get("open_li"), field="open_li") / MARKET_PRICE_UNIT_DIVISOR
        high_raw = _required_number(row.get("high_li"), field="high_li") / MARKET_PRICE_UNIT_DIVISOR
        low_raw = _required_number(row.get("low_li"), field="low_li") / MARKET_PRICE_UNIT_DIVISOR
        close_raw = _required_number(row.get("close_li"), field="close_li") / MARKET_PRICE_UNIT_DIVISOR
        _required_number(row.get("pre_close"), field="pre_close")
        up_limit = _required_number(row.get("up_limit"), field="up_limit")
        down_limit = _required_number(row.get("down_limit"), field="down_limit")
        records.append(
            {
                "datetime": pd.Timestamp(row["trade_date"]).normalize(),
                "instrument": str(row["ts_code"]).strip().upper(),
                "factor": factor,
                "open": open_raw * factor,
                "high": high_raw * factor,
                "low": low_raw * factor,
                "close": close_raw * factor,
                "up_limit_price": up_limit,
                "down_limit_price": down_limit,
                "limit_up": float(open_raw >= up_limit and low_raw >= up_limit and close_raw >= up_limit),
                "limit_down": float(open_raw <= down_limit and high_raw <= down_limit and close_raw <= down_limit),
            }
        )
    if not records:
        raise _market_unavailable("forward evaluation stock market rows are empty")
    frame = pd.DataFrame(records)
    if frame.duplicated(["datetime", "instrument"]).any():
        raise _market_unavailable("forward evaluation stock market rows contain duplicates")
    return frame.set_index(["datetime", "instrument"]).sort_index()


def _benchmark_frame(rows: Sequence[Mapping[str, Any]], *, benchmark: str) -> pd.DataFrame:
    records = [
        {
            "datetime": pd.Timestamp(row["trade_date"]).normalize(),
            "instrument": benchmark,
            "open": _required_number(row.get("open"), field="benchmark.open"),
        }
        for row in rows
    ]
    if not records:
        raise _market_unavailable("forward evaluation benchmark rows are empty")
    frame = pd.DataFrame(records)
    if frame["datetime"].duplicated().any():
        raise _market_unavailable("forward evaluation benchmark rows contain duplicates")
    return frame.set_index(["datetime", "instrument"]).sort_index()


def _suspend_frame(rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["trade_date", "instrument", "suspend_type"])
    frame = pd.DataFrame(
        [
            {
                "trade_date": pd.Timestamp(row["trade_date"]).normalize(),
                "instrument": str(row["ts_code"]).strip().upper(),
                "suspend_type": str(row.get("suspend_type") or "S"),
            }
            for row in rows
        ]
    )
    if frame.duplicated(["trade_date", "instrument"]).any():
        raise _market_unavailable("forward evaluation suspend rows contain duplicates")
    return frame


def _frame_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    return [_json_ready(row) for row in frame.to_dict("records")]


def _json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if value is pd.NaT:
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        parsed = float(value)
        return parsed if isfinite(parsed) else None
    return value


def _canonical_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [_json_ready(dict(row)) for row in rows]


def _required_number(value: Any, *, field: str) -> float:
    parsed = _optional_number(value)
    if parsed is None or parsed <= 0:
        raise _market_unavailable(
            "forward evaluation market row has an invalid required value",
            context={"field": field},
        )
    return parsed


def _optional_number(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if isfinite(parsed) else None


def _stable_id(prefix: str, *parts: str) -> str:
    return f"{prefix}_{canonical_json_sha256({'parts': list(parts)})[:32]}"


def _contract_invalid(message: str, *, context: dict[str, Any] | None = None) -> AdvisoryForwardModelEvaluationError:
    return AdvisoryForwardModelEvaluationError(message, reason_code=REASON_CONTRACT_INVALID, context=context)


def _sequence_incomplete(message: str, *, context: dict[str, Any] | None = None) -> AdvisoryForwardModelEvaluationError:
    return AdvisoryForwardModelEvaluationError(message, reason_code=REASON_SEQUENCE_INCOMPLETE, context=context)


def _market_unavailable(message: str, *, context: dict[str, Any] | None = None) -> AdvisoryForwardModelEvaluationError:
    return AdvisoryForwardModelEvaluationError(message, reason_code=REASON_MARKET_UNAVAILABLE, context=context)
