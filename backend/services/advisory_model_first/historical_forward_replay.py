"""Historical virtual-forward evaluation using the production shadow-policy kernel.

This module is intentionally repository-free: it accepts immutable historical
inputs and publishes repo-external artifacts, but it cannot write production
Advisory forward facts.
"""

from __future__ import annotations

import json
import os
import tempfile
from bisect import bisect_right
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

import pandas as pd
import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.db.pg_pool import get_conn
from backend.services.advisory_forward.evaluation import (
    AdvisoryForwardEvaluationMarketData,
    build_forward_evaluation_market_data_from_rows,
)
from backend.services.advisory_program import MARKET_PRICE_UNIT_DIVISOR
from backend.services.dataset_release.a_share_limit_rule import (
    PRICE_LIMIT_RULE_VERSION,
    AShareLimitRuleError,
    derive_limit_prices,
)
from backend.services.advisory_model_first.policy_contracts import (
    AdvisoryPolicyCostV1,
    transition_policy_from_payload,
)
from backend.services.advisory_model_first.shadow_portfolio_policy import (
    replay_shadow_portfolio,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


REPLAY_SCHEMA_VERSION = "advisory_p0d_historical_forward_replay_v1"
REPLAY_PRODUCER_VERSION = "advisory_p0d_historical_forward_replay_producer_v1"
EVIDENCE_HISTORICAL_OUT_OF_TIME = "HISTORICAL_OUT_OF_TIME"
EVIDENCE_HISTORICAL_REPLAY = "HISTORICAL_REPLAY"
WINDOW_UNCONSUMED = "UNCONSUMED_FOR_MODEL_SELECTION"
WINDOW_CONSUMED_OR_UNKNOWN = "CONSUMED_OR_UNKNOWN"


class _FrozenContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class HistoricalForwardReplayRankV1(_FrozenContract):
    symbol: str = Field(min_length=1, max_length=32)
    selection_effective_rank: int = Field(ge=1, le=40)
    combined_score: float


class HistoricalForwardReplayPriorityV1(_FrozenContract):
    symbol: str = Field(min_length=1, max_length=32)
    entry_priority_rank: int = Field(ge=1, le=20)
    take_probability: float = Field(ge=0.0, le=1.0)
    skip_probability: float = Field(ge=0.0, le=1.0)
    advisory_model_confidence: float = Field(ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _probabilities(self) -> "HistoricalForwardReplayPriorityV1":
        if abs(self.take_probability + self.skip_probability - 1.0) > 1e-10:
            raise ValueError("take_probability and skip_probability must sum to one")
        expected_confidence = abs(self.take_probability - 0.5) * 2.0
        if abs(self.advisory_model_confidence - expected_confidence) > 1e-10:
            raise ValueError("advisory_model_confidence differs from take probability")
        return self


class HistoricalForwardReplayDayV1(_FrozenContract):
    decision_as_of_trade_date: date
    target_trade_date: date
    parent_candidate_artifact_hash: str = Field(min_length=64, max_length=64)
    rankings: tuple[HistoricalForwardReplayRankV1, ...] = Field(
        min_length=40, max_length=40
    )
    entry_priorities: tuple[HistoricalForwardReplayPriorityV1, ...] = ()

    @model_validator(mode="after")
    def _closed_day(self) -> "HistoricalForwardReplayDayV1":
        if self.decision_as_of_trade_date >= self.target_trade_date:
            raise ValueError("decision date must precede target date")
        ranks = sorted(item.selection_effective_rank for item in self.rankings)
        symbols = [item.symbol.upper() for item in self.rankings]
        if ranks != list(range(1, 41)) or len(set(symbols)) != 40:
            raise ValueError("historical replay day must contain one exact Top40")
        if self.entry_priorities:
            priority_ranks = sorted(
                item.entry_priority_rank for item in self.entry_priorities
            )
            priority_symbols = {item.symbol.upper() for item in self.entry_priorities}
            top20_symbols = {
                item.symbol.upper()
                for item in self.rankings
                if item.selection_effective_rank <= 20
            }
            if (
                priority_ranks != list(range(1, 21))
                or priority_symbols != top20_symbols
            ):
                raise ValueError(
                    "historical replay priority must reorder the exact Selection Top20"
                )
        return self


class HistoricalForwardReplayRequestV1(_FrozenContract):
    schema_version: Literal[REPLAY_SCHEMA_VERSION] = REPLAY_SCHEMA_VERSION
    producer_contract_version: Literal[REPLAY_PRODUCER_VERSION] = (
        REPLAY_PRODUCER_VERSION
    )
    request_id: str = Field(min_length=1, max_length=160)
    parent_range_run_id: str = Field(min_length=1, max_length=160)
    program_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    model_descriptor_sha256: str = Field(min_length=64, max_length=64)
    bundle_id: str = Field(min_length=64, max_length=64)
    bundle_manifest_sha256: str = Field(min_length=64, max_length=64)
    shadow_policy: dict[str, Any]
    shadow_policy_sha256: str = Field(min_length=64, max_length=64)
    cost_policy: dict[str, Any]
    cost_policy_sha256: str = Field(min_length=64, max_length=64)
    model_training_data_cutoff_trade_date: date
    window_usage: Literal["UNCONSUMED_FOR_MODEL_SELECTION", "CONSUMED_OR_UNKNOWN"]
    replay_as_of_trade_date: date
    maturity_horizon_trade_days: int = Field(ge=1, le=252)
    market_input_sha256: str = Field(min_length=64, max_length=64)
    implementation_sha256: str = Field(min_length=64, max_length=64)
    context_days: tuple[HistoricalForwardReplayDayV1, ...] = Field(min_length=2)
    request_sha256: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _closed_request(self) -> "HistoricalForwardReplayRequestV1":
        if canonical_json_sha256(self.shadow_policy) != self.shadow_policy_sha256:
            raise ValueError("shadow policy hash differs from its payload")
        cost = AdvisoryPolicyCostV1.model_validate(self.cost_policy)
        if cost.policy_sha256 != self.cost_policy_sha256:
            raise ValueError("cost policy hash differs from its payload")
        transition_policy_from_payload(self.shadow_policy)
        ordered = tuple(
            sorted(self.context_days, key=lambda item: item.decision_as_of_trade_date)
        )
        if ordered != self.context_days:
            raise ValueError("historical replay context days must be ordered")
        if len({item.decision_as_of_trade_date for item in ordered}) != len(ordered):
            raise ValueError("historical replay context dates must be unique")
        if self.replay_as_of_trade_date != ordered[-1].target_trade_date:
            raise ValueError(
                "replay as-of date must equal the last context target date"
            )
        decision_days = tuple(item for item in ordered if item.entry_priorities)
        if not decision_days:
            raise ValueError(
                "historical replay requires at least one scored decision day"
            )
        if decision_days != ordered[: len(decision_days)]:
            raise ValueError(
                "scored decision days must be a contiguous prefix before the tail"
            )
        if len(ordered) - len(decision_days) < self.maturity_horizon_trade_days:
            raise ValueError(
                "historical replay context tail is shorter than the maturity horizon"
            )
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"request_sha256"})
        )
        if self.request_sha256 is not None and self.request_sha256 != digest:
            raise ValueError("request_sha256 differs from replay request content")
        object.__setattr__(self, "request_sha256", digest)
        return self

    @property
    def decision_days(self) -> tuple[HistoricalForwardReplayDayV1, ...]:
        return tuple(item for item in self.context_days if item.entry_priorities)

    @property
    def evidence_classification(self) -> str:
        if self.window_usage == WINDOW_UNCONSUMED and all(
            item.decision_as_of_trade_date > self.model_training_data_cutoff_trade_date
            for item in self.decision_days
        ):
            return EVIDENCE_HISTORICAL_OUT_OF_TIME
        return EVIDENCE_HISTORICAL_REPLAY


class HistoricalForwardReplayArtifactV1(_FrozenContract):
    schema_version: Literal[REPLAY_SCHEMA_VERSION] = REPLAY_SCHEMA_VERSION
    producer_contract_version: Literal[REPLAY_PRODUCER_VERSION] = (
        REPLAY_PRODUCER_VERSION
    )
    request_sha256: str = Field(min_length=64, max_length=64)
    parent_range_run_id: str = Field(min_length=1, max_length=160)
    program_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    model_descriptor_sha256: str = Field(min_length=64, max_length=64)
    bundle_id: str = Field(min_length=64, max_length=64)
    model_training_data_cutoff_trade_date: date
    decision_start_trade_date: date
    decision_end_trade_date: date
    replay_as_of_trade_date: date
    maturity_horizon_trade_days: int = Field(ge=1, le=252)
    evidence_classification: Literal["HISTORICAL_OUT_OF_TIME", "HISTORICAL_REPLAY"]
    evidence_reason: str = Field(min_length=1)
    decision_observation_count: int = Field(ge=1)
    context_day_count: int = Field(ge=2)
    resolved_observation_count: int = Field(ge=0)
    unresolved_observation_count: int = Field(ge=0)
    coverage: float = Field(ge=0.0, le=1.0)
    metrics: dict[str, Any]
    baseline_metrics: dict[str, Any]
    comparison_metrics: dict[str, Any]
    daily: tuple[dict[str, Any], ...]
    episodes: tuple[dict[str, Any], ...]
    baseline_daily: tuple[dict[str, Any], ...]
    baseline_episodes: tuple[dict[str, Any], ...]
    artifact_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _closed_artifact(self) -> "HistoricalForwardReplayArtifactV1":
        if not (
            self.decision_start_trade_date
            <= self.decision_end_trade_date
            < self.replay_as_of_trade_date
        ):
            raise ValueError("historical artifact decision window is invalid")
        if (
            self.evidence_classification == EVIDENCE_HISTORICAL_OUT_OF_TIME
            and self.model_training_data_cutoff_trade_date
            >= self.decision_start_trade_date
        ):
            raise ValueError("historical OOT artifact date identity is invalid")
        if (
            self.resolved_observation_count + self.unresolved_observation_count
            != self.decision_observation_count
        ):
            raise ValueError("historical replay observation counts do not close")
        expected_coverage = (
            self.resolved_observation_count / self.decision_observation_count
        )
        if abs(self.coverage - expected_coverage) > 1e-12:
            raise ValueError(
                "historical replay coverage differs from observation counts"
            )
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"artifact_hash"})
        )
        if self.artifact_hash is not None and self.artifact_hash != digest:
            raise ValueError("artifact_hash differs from historical replay content")
        object.__setattr__(self, "artifact_hash", digest)
        return self


class HistoricalForwardEvaluationMarketSource:
    """Bounded historical reader with missing-only rule-derived limit prices."""

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
        normalized = sorted(
            {str(value).strip().upper() for value in symbols if str(value).strip()}
        )
        if not normalized or start_trade_date > end_trade_date:
            raise ValueError("historical forward market range or symbol set is invalid")
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
                if not calendar_rows:
                    raise ValueError("historical forward calendar is empty")
                history_start = calendar_rows[0]["cal_date"]
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
                      AND price.trade_date BETWEEN %(history_start)s AND %(end_date)s
                    ORDER BY TRIM(price.ts_code), price.trade_date
                    """,
                    {
                        "symbols": normalized,
                        "history_start": history_start,
                        "end_date": end_trade_date,
                    },
                )
                history_rows = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    """
                    SELECT TRIM(ts_code) AS ts_code, ann_date, start_date, end_date
                    FROM market.stock_st
                    WHERE TRIM(ts_code) = ANY(%s) AND ann_date <= %s
                    ORDER BY TRIM(ts_code), ann_date, start_date NULLS FIRST, end_date NULLS FIRST
                    """,
                    (normalized, end_trade_date),
                )
                stock_st_rows = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    """
                    SELECT DISTINCT ann_date
                    FROM market.stock_st
                    WHERE ann_date <= %s
                    ORDER BY ann_date
                    """,
                    (end_trade_date,),
                )
                stock_st_snapshot_dates = [row["ann_date"] for row in cur.fetchall()]
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

        by_symbol: dict[str, list[dict[str, Any]]] = {}
        for row in history_rows:
            by_symbol.setdefault(str(row["ts_code"]).upper(), []).append(row)
        st_intervals: dict[str, list[tuple[date, date | None]]] = {}
        st_snapshots: dict[str, set[date]] = {}
        for row in stock_st_rows:
            symbol = str(row["ts_code"]).upper()
            ann_date = row["ann_date"]
            start = row.get("start_date")
            end = row.get("end_date")
            if start is None and end is None:
                st_snapshots.setdefault(symbol, set()).add(ann_date)
            else:
                st_intervals.setdefault(symbol, []).append((start or ann_date, end))

        def is_st(symbol: str, trade_date: date) -> bool:
            if any(
                start <= trade_date and (end is None or end >= trade_date)
                for start, end in st_intervals.get(symbol, ())
            ):
                return True
            position = bisect_right(stock_st_snapshot_dates, trade_date)
            latest_snapshot = (
                stock_st_snapshot_dates[position - 1] if position else None
            )
            return latest_snapshot is not None and latest_snapshot in st_snapshots.get(
                symbol, set()
            )

        price_rows: list[dict[str, Any]] = []
        derived_evidence: list[dict[str, Any]] = []
        for symbol in sorted(by_symbol):
            previous: dict[str, Any] | None = None
            for row in by_symbol[symbol]:
                trade_date = row["trade_date"]
                row["is_st"] = is_st(symbol, trade_date)
                if trade_date >= start_trade_date:
                    limits = (
                        row.get("pre_close"),
                        row.get("up_limit"),
                        row.get("down_limit"),
                    )
                    present = tuple(value is not None for value in limits)
                    if not all(present):
                        if previous is None:
                            raise ValueError(
                                "historical limit derivation has no previous price"
                            )
                        try:
                            derived = derive_limit_prices(
                                ts_code=symbol,
                                trade_date=trade_date,
                                previous_close=float(previous["close_li"])
                                / MARKET_PRICE_UNIT_DIVISOR,
                                previous_adj_factor=previous["adj_factor"],
                                current_adj_factor=row["adj_factor"],
                                is_st=bool(row["is_st"]),
                            )
                        except (AShareLimitRuleError, TypeError, ValueError) as exc:
                            raise ValueError(
                                f"historical limit derivation failed for {symbol} {trade_date}"
                            ) from exc
                        derived_values = {
                            "pre_close": derived.pre_close,
                            "up_limit": derived.up_limit,
                            "down_limit": derived.down_limit,
                        }
                        missing_fields: list[str] = []
                        preserved_existing_fields: list[str] = []
                        for field, derived_value in derived_values.items():
                            if row.get(field) is None:
                                row[field] = derived_value
                                missing_fields.append(field)
                            else:
                                preserved_existing_fields.append(field)
                        final_pre_close = Decimal(str(row["pre_close"]))
                        final_up_limit = Decimal(str(row["up_limit"]))
                        final_down_limit = Decimal(str(row["down_limit"]))
                        if not (
                            final_down_limit.is_finite()
                            and final_pre_close.is_finite()
                            and final_up_limit.is_finite()
                            and Decimal("0")
                            < final_down_limit
                            <= final_pre_close
                            <= final_up_limit
                        ):
                            raise ValueError(
                                "historical stk_limit missing-field overlay has invalid final bounds: "
                                f"symbol={symbol} trade_date={trade_date}"
                            )
                        row.update(
                            {
                                "limit_price_source": "RULE_DERIVED_MISSING_FIELDS",
                                "limit_rule_version": derived.rule_version,
                                "limit_reference_trade_date": previous["trade_date"],
                            }
                        )
                        derived_evidence.append(
                            {
                                "ts_code": symbol,
                                "trade_date": trade_date.isoformat(),
                                "reference_trade_date": previous[
                                    "trade_date"
                                ].isoformat(),
                                "is_st": bool(row["is_st"]),
                                "rule_version": derived.rule_version,
                                "missing_fields": missing_fields,
                                "preserved_existing_fields": preserved_existing_fields,
                            }
                        )
                    else:
                        row.update(
                            {
                                "limit_price_source": "MARKET_STK_LIMIT",
                                "limit_rule_version": None,
                                "limit_reference_trade_date": None,
                            }
                        )
                    price_rows.append(row)
                previous = row
        price_rows.sort(key=lambda row: (row["trade_date"], str(row["ts_code"])))
        return build_forward_evaluation_market_data_from_rows(
            calendar_rows=calendar_rows,
            price_rows=price_rows,
            suspend_rows=suspend_rows,
            benchmark_rows=benchmark_rows,
            symbols=normalized,
            benchmark_instrument=benchmark,
            start_trade_date=start_trade_date,
            end_trade_date=end_trade_date,
            input_schema_version="advisory_historical_forward_evaluation_market_input_v1",
            extra_identity={
                "missing_limit_policy": "RULE_DERIVED_MISSING_FIELDS",
                "limit_rule_version": PRICE_LIMIT_RULE_VERSION,
                "derived_limit_row_count": len(derived_evidence),
                "derived_limit_evidence": derived_evidence,
            },
        )


def build_historical_forward_replay(
    *,
    request: HistoricalForwardReplayRequestV1,
    market: AdvisoryForwardEvaluationMarketData,
) -> HistoricalForwardReplayArtifactV1:
    """Replay a locked historical decision prefix plus its exit-context tail."""

    if market.input_sha256 != request.market_input_sha256:
        raise ValueError(
            "historical replay market input differs from the request identity"
        )
    calendar = pd.DatetimeIndex(market.trading_calendar).normalize()
    expected_targets = [
        pd.Timestamp(item.target_trade_date).normalize()
        for item in request.context_days
    ]
    if list(calendar[1:]) != expected_targets:
        raise ValueError(
            "historical replay market calendar differs from context target dates"
        )
    if calendar[-1].date() != request.replay_as_of_trade_date:
        raise ValueError(
            "historical replay market calendar exceeds the explicit watermark"
        )

    ranking_records: list[dict[str, Any]] = []
    priority_records: list[dict[str, Any]] = []
    for day in request.context_days:
        for item in day.rankings:
            ranking_records.append(
                {
                    "decision_as_of_trade_date": day.decision_as_of_trade_date,
                    "target_trade_date": day.target_trade_date,
                    "instrument": item.symbol.upper(),
                    "selection_effective_rank": item.selection_effective_rank,
                    "combined_score": item.combined_score,
                }
            )
        for item in day.entry_priorities:
            priority_records.append(
                {
                    "decision_as_of_trade_date": day.decision_as_of_trade_date,
                    "instrument": item.symbol.upper(),
                    "entry_priority_rank": item.entry_priority_rank,
                }
            )

    policy = transition_policy_from_payload(request.shadow_policy)
    cost_policy = AdvisoryPolicyCostV1.model_validate(request.cost_policy)
    result = replay_shadow_portfolio(
        rankings=pd.DataFrame(ranking_records),
        daily=_bounded_frame(market.daily, request.replay_as_of_trade_date),
        benchmark_daily=_bounded_frame(
            market.benchmark_daily, request.replay_as_of_trade_date
        ),
        suspend_rows=_bounded_frame(
            market.suspend_rows, request.replay_as_of_trade_date
        ),
        trading_calendar=calendar,
        policy=policy,
        policy_sha256=request.shadow_policy_sha256,
        cost_policy=cost_policy,
        request_id=request.request_id,
        rank_depth=40,
        candidate_decision_dates=[
            item.decision_as_of_trade_date for item in request.decision_days
        ],
        entry_priorities=pd.DataFrame(priority_records),
    )
    baseline_result = replay_shadow_portfolio(
        rankings=pd.DataFrame(ranking_records),
        daily=_bounded_frame(market.daily, request.replay_as_of_trade_date),
        benchmark_daily=_bounded_frame(
            market.benchmark_daily, request.replay_as_of_trade_date
        ),
        suspend_rows=_bounded_frame(
            market.suspend_rows, request.replay_as_of_trade_date
        ),
        trading_calendar=calendar,
        policy=policy,
        policy_sha256=request.shadow_policy_sha256,
        cost_policy=cost_policy,
        request_id=f"{request.request_id}_selection_baseline",
        rank_depth=40,
        candidate_decision_dates=[
            item.decision_as_of_trade_date for item in request.decision_days
        ],
        entry_priorities=None,
    )
    daily = tuple(_frame_records(result.daily))
    episodes = tuple(_frame_records(result.episodes))
    baseline_daily = tuple(_frame_records(baseline_result.daily))
    baseline_episodes = tuple(_frame_records(baseline_result.episodes))
    active_entry_dates = {
        str(item.get("entry_signal_date"))[:10]
        for item in episodes
        if item.get("status") != "EXITED"
    }
    decision_dates = {
        item.decision_as_of_trade_date.isoformat() for item in request.decision_days
    }
    unresolved = len(active_entry_dates & decision_dates)
    resolved = len(request.decision_days) - unresolved
    metrics = {
        **dict(result.metrics),
        "schema_version": "advisory_p0d_historical_forward_metrics_v1",
        "evidence_classification": request.evidence_classification,
        "decision_observation_count": len(request.decision_days),
        "context_day_count": len(request.context_days),
        "resolved_observation_count": resolved,
        "unresolved_observation_count": unresolved,
        "coverage": resolved / len(request.decision_days),
        "cumulative_net_return": (
            float(result.daily.iloc[-1]["cumulative_nav"]) - 1.0
            if not result.daily.empty
            else None
        ),
        "cumulative_net_excess_return_bps": (
            float(result.daily["net_excess_return_bps"].sum())
            if not result.daily.empty
            else None
        ),
        "mean_completed_episode_net_return_bps": (
            float(
                result.episodes.loc[
                    result.episodes["status"] == "EXITED", "net_return_bps"
                ].mean()
            )
            if not result.episodes.empty
            and (result.episodes["status"] == "EXITED").any()
            else None
        ),
        "median_completed_episode_net_return_bps": (
            float(
                result.episodes.loc[
                    result.episodes["status"] == "EXITED", "net_return_bps"
                ].median()
            )
            if not result.episodes.empty
            and (result.episodes["status"] == "EXITED").any()
            else None
        ),
    }
    baseline_metrics = _enriched_portfolio_metrics(baseline_result)
    comparison_metrics = _comparison_metrics(
        challenger=result,
        baseline=baseline_result,
    )
    reason = (
        "all decision dates are strictly after the frozen policy-dataset data cutoff and the window "
        "was locked before P0-D model selection"
        if request.evidence_classification == EVIDENCE_HISTORICAL_OUT_OF_TIME
        else "the replay window overlaps model development or its prior selection usage is not independently locked"
    )
    return HistoricalForwardReplayArtifactV1(
        request_sha256=str(request.request_sha256),
        parent_range_run_id=request.parent_range_run_id,
        program_id=request.program_id,
        package_id=request.package_id,
        model_descriptor_sha256=request.model_descriptor_sha256,
        bundle_id=request.bundle_id,
        model_training_data_cutoff_trade_date=request.model_training_data_cutoff_trade_date,
        decision_start_trade_date=request.decision_days[0].decision_as_of_trade_date,
        decision_end_trade_date=request.decision_days[-1].decision_as_of_trade_date,
        replay_as_of_trade_date=request.replay_as_of_trade_date,
        maturity_horizon_trade_days=request.maturity_horizon_trade_days,
        evidence_classification=request.evidence_classification,
        evidence_reason=reason,
        decision_observation_count=len(request.decision_days),
        context_day_count=len(request.context_days),
        resolved_observation_count=resolved,
        unresolved_observation_count=unresolved,
        coverage=resolved / len(request.decision_days),
        metrics=metrics,
        baseline_metrics=baseline_metrics,
        comparison_metrics=comparison_metrics,
        daily=daily,
        episodes=episodes,
        baseline_daily=baseline_daily,
        baseline_episodes=baseline_episodes,
    )


class HistoricalForwardReplayArtifactStore:
    """Immutable store for historical replay only; no database dependency."""

    def __init__(self, *, root: Path) -> None:
        if not root.is_absolute():
            raise ValueError("historical replay artifact root must be absolute")
        resolved_root = root.resolve()
        repository_root = Path(__file__).resolve().parents[3]
        try:
            resolved_root.relative_to(repository_root)
        except ValueError:
            pass
        else:
            raise ValueError("historical replay artifact root must be repo-external")
        resolved_root.mkdir(parents=True, exist_ok=True)
        self._root = resolved_root.resolve(strict=True)

    def publish(self, artifact: HistoricalForwardReplayArtifactV1) -> Path:
        identity = str(artifact.artifact_hash)
        destination = (
            self._root / "p0d-historical-forward" / f"{identity}.json"
        ).resolve()
        try:
            destination.relative_to(self._root)
        except ValueError as exc:
            raise ValueError(
                "historical replay artifact path escapes configured root"
            ) from exc
        destination.parent.mkdir(parents=True, exist_ok=True)
        content = (_canonical_text(artifact.model_dump(mode="json")) + "\n").encode(
            "utf-8"
        )
        if destination.exists():
            if destination.read_bytes() != content:
                raise RuntimeError("ADVISORY_HISTORICAL_FORWARD_EXACT_RETRY_CONFLICT")
            return destination
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{identity}.", suffix=".tmp", dir=destination.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, destination)
            except FileExistsError:
                if destination.read_bytes() != content:
                    raise RuntimeError(
                        "ADVISORY_HISTORICAL_FORWARD_EXACT_RETRY_CONFLICT"
                    )
            if destination.read_bytes() != content:
                raise RuntimeError(
                    "ADVISORY_HISTORICAL_FORWARD_ARTIFACT_READBACK_MISMATCH"
                )
        finally:
            temporary.unlink(missing_ok=True)
        return destination

    def load(self, *, artifact_hash: str) -> HistoricalForwardReplayArtifactV1:
        path = (
            self._root / "p0d-historical-forward" / f"{artifact_hash}.json"
        ).resolve()
        try:
            path.relative_to(self._root)
        except ValueError as exc:
            raise ValueError(
                "historical replay artifact path escapes configured root"
            ) from exc
        artifact = HistoricalForwardReplayArtifactV1.model_validate_json(
            path.read_text(encoding="utf-8")
        )
        if artifact.artifact_hash != artifact_hash:
            raise RuntimeError("ADVISORY_HISTORICAL_FORWARD_ARTIFACT_READBACK_MISMATCH")
        return artifact


def _bounded_frame(frame: pd.DataFrame, watermark: date) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    cutoff = pd.Timestamp(watermark).normalize()
    if isinstance(frame.index, pd.MultiIndex):
        dates = pd.DatetimeIndex(frame.index.get_level_values(0)).normalize()
        return frame.loc[dates <= cutoff].copy()
    if isinstance(frame.index, pd.DatetimeIndex):
        return frame.loc[frame.index.normalize() <= cutoff].copy()
    if "datetime" in frame.columns:
        dates = pd.to_datetime(frame["datetime"]).dt.normalize()
        return frame.loc[dates <= cutoff].copy()
    return frame.copy()


def _frame_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    return json.loads(
        frame.to_json(orient="records", date_format="iso", double_precision=15)
    )


def _enriched_portfolio_metrics(result: Any) -> dict[str, Any]:
    exited = (
        result.episodes.loc[result.episodes["status"] == "EXITED"]
        if not result.episodes.empty
        else result.episodes
    )
    return {
        **dict(result.metrics),
        "cumulative_net_return": (
            float(result.daily.iloc[-1]["cumulative_nav"]) - 1.0
            if not result.daily.empty
            else None
        ),
        "cumulative_net_excess_return_bps": (
            float(result.daily["net_excess_return_bps"].sum())
            if not result.daily.empty
            else None
        ),
        "mean_completed_episode_net_return_bps": (
            float(exited["net_return_bps"].mean()) if not exited.empty else None
        ),
        "median_completed_episode_net_return_bps": (
            float(exited["net_return_bps"].median()) if not exited.empty else None
        ),
    }


def _comparison_metrics(*, challenger: Any, baseline: Any) -> dict[str, Any]:
    challenger_daily = challenger.daily[
        ["target_trade_date", "net_return_bps", "net_excess_return_bps"]
    ].copy()
    baseline_daily = baseline.daily[
        ["target_trade_date", "net_return_bps", "net_excess_return_bps"]
    ].copy()
    paired = challenger_daily.merge(
        baseline_daily,
        on="target_trade_date",
        how="inner",
        validate="one_to_one",
        suffixes=("_challenger", "_baseline"),
    )

    def delta(left: Any, right: Any) -> float | None:
        return None if left is None or right is None else float(left) - float(right)

    challenger_enriched = _enriched_portfolio_metrics(challenger)
    baseline_enriched = _enriched_portfolio_metrics(baseline)
    return {
        "schema_version": "advisory_p0d_historical_forward_comparison_v1",
        "comparison": "P0D_ENTRY_PRIORITY_VS_SELECTION_TOP5_MATCHED_POLICY",
        "paired_day_count": len(paired),
        "mean_daily_net_return_lift_bps": (
            float(
                (
                    paired["net_return_bps_challenger"]
                    - paired["net_return_bps_baseline"]
                ).mean()
            )
            if not paired.empty
            else None
        ),
        "mean_daily_net_excess_return_lift_bps": (
            float(
                (
                    paired["net_excess_return_bps_challenger"]
                    - paired["net_excess_return_bps_baseline"]
                ).mean()
            )
            if not paired.empty
            else None
        ),
        "cumulative_net_return_lift": delta(
            challenger_enriched["cumulative_net_return"],
            baseline_enriched["cumulative_net_return"],
        ),
        "completed_episode_hit_rate_lift": delta(
            challenger_enriched["completed_episode_hit_rate"],
            baseline_enriched["completed_episode_hit_rate"],
        ),
        "mean_completed_episode_net_return_lift_bps": delta(
            challenger_enriched["mean_completed_episode_net_return_bps"],
            baseline_enriched["mean_completed_episode_net_return_bps"],
        ),
        "maximum_drawdown_difference": delta(
            challenger_enriched["maximum_drawdown"],
            baseline_enriched["maximum_drawdown"],
        ),
        "mean_turnover_fraction_difference": delta(
            challenger_enriched["mean_turnover_fraction"],
            baseline_enriched["mean_turnover_fraction"],
        ),
    }


def _canonical_text(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
