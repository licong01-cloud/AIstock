from __future__ import annotations

import logging
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.db.pg_pool import get_conn
from backend.services.canonical_equity_pit import require_canonical_rolling_universe_key
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.fresh_hmm import continue_sector_hmm
from backend.services.industry_code_map import encode_l2_codes, load_sw_l2_code_map
from backend.services.stock_universe_pit_service import (
    DEFAULT_ST_PIT_UNIVERSE_KEY,
    require_live_st_pit_universe_key,
)


ConnectionContextFactory = Callable[[], AbstractContextManager[Any]]
_PRICE_UNIT_DIVISOR = 1000.0
_CANDIDATE_HISTORY_TRADING_DAYS = 90
_HMM_WARMUP_TRADING_DAYS = 25
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PriceRangeRealtimeContext:
    symbol: str
    decision_raw_close: float
    decision_price_trade_date: date
    decision_price_source: str
    price_unit_divisor: float
    target_raw_price_multiplier: float
    corporate_action_source: str
    board_type: str
    list_date: date
    listed_trading_days: int
    target_is_st: bool
    tick_size: float


@dataclass(frozen=True)
class RealtimeFeatureInputs:
    candidate_daily: pd.DataFrame
    candidate_static: pd.DataFrame
    market_daily: pd.DataFrame
    benchmark_daily: pd.DataFrame
    suspend_rows: pd.DataFrame
    hmm_states: pd.DataFrame
    hmm_unavailable: tuple[dict[str, Any], ...]
    trading_calendar: pd.DatetimeIndex
    price_range_contexts: Mapping[str, PriceRangeRealtimeContext] = field(default_factory=dict)
    price_range_unavailable: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class PersistedAdvisoryReviewIdentity:
    review_run_id: str
    program_id: str
    binding_version_id: str
    trade_date: date
    selection_run_id: str | None
    selection_run_ids: tuple[str, ...]


class PostgresAdvisoryReviewSource:
    """Read the immutable ReviewRun identity referenced by a recommendation list."""

    def __init__(
        self,
        *,
        connection_context_factory: ConnectionContextFactory | None = None,
    ) -> None:
        self._connection_context_factory = connection_context_factory or (
            lambda: get_conn(autocommit=False, manage_transaction=False)
        )

    def get(self, review_run_id: str) -> PersistedAdvisoryReviewIdentity:
        normalized_id = str(review_run_id).strip()
        if not normalized_id:
            raise AdvisoryModelFirstError(
                "recommendation list does not identify its persisted Advisory review run",
                reason_code="ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE",
            )
        with self._connection_context_factory() as conn:
            cursor = conn.cursor()
            try:
                conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
                cursor.execute(
                    """
                    SELECT review_run_id, program_id, binding_version_id, trade_date,
                           selection_run_id, selection_run_ids
                    FROM app.advisory_review_run
                    WHERE review_run_id = %s
                    """,
                    (normalized_id,),
                )
                row = cursor.fetchone()
                conn.rollback()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
        if row is None:
            raise AdvisoryModelFirstError(
                "persisted Advisory review run is unavailable",
                reason_code="ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE",
                context={"review_run_id": normalized_id},
            )
        selection_run_ids = row[5]
        if not isinstance(selection_run_ids, (list, tuple)):
            raise AdvisoryModelFirstError(
                "persisted Advisory review run has an invalid Selection identity set",
                reason_code="ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE",
                context={"review_run_id": normalized_id},
            )
        return PersistedAdvisoryReviewIdentity(
            review_run_id=str(row[0]),
            program_id=str(row[1]),
            binding_version_id=str(row[2]),
            trade_date=pd.Timestamp(row[3]).date(),
            selection_run_id=str(row[4]).strip() if row[4] is not None else None,
            selection_run_ids=tuple(str(value).strip() for value in selection_run_ids),
        )


class PostgresRealtimeFeatureSource:
    """Read one decision-cutoff feature snapshot without writing or replaying selection."""

    def __init__(
        self,
        *,
        connection_context_factory: ConnectionContextFactory | None = None,
        statement_timeout_ms: int = 300_000,
    ) -> None:
        if statement_timeout_ms <= 0:
            raise ValueError("statement_timeout_ms must be positive")
        self._connection_context_factory = connection_context_factory or (
            lambda: get_conn(autocommit=False, manage_transaction=False)
        )
        self._statement_timeout_ms = int(statement_timeout_ms)

    def load(
        self,
        *,
        symbols: Sequence[str],
        decision_as_of_trade_date: date,
        target_trade_date: date,
        continuation_cutoff: date,
        hmm_models: dict[str, Any],
        pit_universe_key: str | None = None,
    ) -> RealtimeFeatureInputs:
        normalized_symbols = tuple(sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}))
        if not normalized_symbols:
            raise AdvisoryModelFirstError(
                "model inference has no candidate symbols",
                reason_code="ADVISORY_MODEL_CANDIDATE_GROUP_INCOMPLETE",
            )
        if decision_as_of_trade_date >= target_trade_date:
            raise AdvisoryModelFirstError(
                "selection decision date must precede target trade date",
                reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
                context={
                    "decision_as_of_trade_date": decision_as_of_trade_date.isoformat(),
                    "target_trade_date": target_trade_date.isoformat(),
                },
            )
        if decision_as_of_trade_date <= continuation_cutoff:
            raise AdvisoryModelFirstError(
                "model API does not rerun dates at or before the HMM continuation cutoff",
                reason_code="ADVISORY_MODEL_DECISION_BEFORE_CONTINUATION_CUTOFF",
                context={
                    "decision_as_of_trade_date": decision_as_of_trade_date.isoformat(),
                    "continuation_cutoff": continuation_cutoff.isoformat(),
                },
            )

        with self._connection_context_factory() as conn:
            cursor = conn.cursor()
            try:
                conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
                cursor.execute("SET LOCAL statement_timeout = %s", (self._statement_timeout_ms,))
                candidate_dates = self._recent_trading_dates(
                    cursor,
                    end_date=decision_as_of_trade_date,
                    limit=_CANDIDATE_HISTORY_TRADING_DAYS,
                )
                hmm_warmup_dates = self._recent_trading_dates(
                    cursor,
                    end_date=continuation_cutoff,
                    limit=_HMM_WARMUP_TRADING_DAYS,
                )
                if len(candidate_dates) < 61 or len(hmm_warmup_dates) < 21:
                    raise AdvisoryModelFirstError(
                        "realtime trading calendar does not cover required feature windows",
                        reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
                        context={
                            "candidate_calendar_rows": len(candidate_dates),
                            "hmm_warmup_rows": len(hmm_warmup_dates),
                        },
                    )
                history_start = min(candidate_dates[0].date(), hmm_warmup_dates[0].date())
                trading_calendar = self._trading_calendar(
                    cursor,
                    start_date=history_start,
                    end_date=decision_as_of_trade_date,
                )
                if pd.Timestamp(decision_as_of_trade_date) not in trading_calendar:
                    raise AdvisoryModelFirstError(
                        "selection decision date is not an open trading day",
                        reason_code="ADVISORY_MODEL_DECISION_CLOCK_MISMATCH",
                        context={"decision_as_of_trade_date": decision_as_of_trade_date.isoformat()},
                    )

                candidate_daily = self._candidate_daily(
                    cursor,
                    symbols=normalized_symbols,
                    start_date=candidate_dates[0].date(),
                    end_date=decision_as_of_trade_date,
                )
                candidate_static = self._candidate_static(
                    cursor,
                    symbols=normalized_symbols,
                    start_date=candidate_dates[0].date(),
                    end_date=decision_as_of_trade_date,
                )
                market_daily = self._market_daily(
                    cursor,
                    start_date=candidate_dates[-2].date(),
                    end_date=decision_as_of_trade_date,
                )
                benchmark_daily = self._benchmark_daily(
                    cursor,
                    start_date=history_start,
                    end_date=decision_as_of_trade_date,
                )
                candidate_sector = self._candidate_sector_static(
                    cursor,
                    candidate_index=candidate_static.index,
                    start_date=candidate_dates[0].date(),
                    end_date=decision_as_of_trade_date,
                )
                suspend_rows = self._suspend_rows(
                    cursor,
                    symbols=normalized_symbols,
                    start_date=candidate_dates[0].date(),
                    end_date=decision_as_of_trade_date,
                )
                l2_code_map = load_sw_l2_code_map(conn)
                candidate_sector = _encode_l2(candidate_sector, l2_code_map)
                candidate_static = _attach_sector_projection(candidate_static, candidate_sector)
                candidate_l2_codes = _decision_l2_codes(candidate_static, decision_as_of_trade_date)
                hmm_observations = self._hmm_observations(
                    cursor,
                    required_l2_code_map={
                        code: encoded
                        for code, encoded in l2_code_map.items()
                        if encoded in set(candidate_l2_codes)
                    },
                    start_date=history_start,
                    end_date=decision_as_of_trade_date,
                    benchmark_daily=benchmark_daily,
                )
                hmm_continuation = continue_sector_hmm(
                    static_all=candidate_sector,
                    market_daily=market_daily,
                    benchmark_daily=benchmark_daily,
                    trading_calendar=trading_calendar,
                    hmm_bundle=hmm_models,
                    continuation_cutoff=continuation_cutoff.isoformat(),
                    required_l2_code_ids=candidate_l2_codes,
                    precomputed_observations=hmm_observations,
                )
                try:
                    price_range_contexts, price_range_unavailable = self._price_range_contexts(
                        cursor,
                        symbols=normalized_symbols,
                        decision_as_of_trade_date=decision_as_of_trade_date,
                        target_trade_date=target_trade_date,
                        pit_universe_key=pit_universe_key,
                    )
                except AdvisoryModelFirstError as exc:
                    LOGGER.warning(
                        "advisory price-range realtime context unavailable "
                        "decision_as_of_trade_date=%s target_trade_date=%s "
                        "reason_code=%s context=%s",
                        decision_as_of_trade_date.isoformat(),
                        target_trade_date.isoformat(),
                        exc.reason_code,
                        exc.context,
                    )
                    price_range_contexts = {}
                    price_range_unavailable = tuple(
                        _price_context_failure(symbol, exc.reason_code, str(exc))
                        for symbol in normalized_symbols
                    )
                except Exception as exc:
                    LOGGER.exception(
                        "advisory price-range realtime context query failed "
                        "decision_as_of_trade_date=%s target_trade_date=%s",
                        decision_as_of_trade_date.isoformat(),
                        target_trade_date.isoformat(),
                    )
                    conn.rollback()
                    price_range_contexts = {}
                    price_range_unavailable = tuple(
                        _price_context_failure(
                            symbol,
                            "ADVISORY_PRICE_RANGE_PIT_ATTRIBUTE_UNAVAILABLE",
                            f"unexpected price-range context failure: {type(exc).__name__}",
                        )
                        for symbol in normalized_symbols
                    )
                conn.rollback()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

        return RealtimeFeatureInputs(
            candidate_daily=candidate_daily,
            candidate_static=candidate_static,
            market_daily=market_daily,
            benchmark_daily=benchmark_daily,
            suspend_rows=suspend_rows,
            hmm_states=hmm_continuation.states,
            hmm_unavailable=hmm_continuation.unavailable,
            trading_calendar=trading_calendar,
            price_range_contexts=price_range_contexts,
            price_range_unavailable=price_range_unavailable,
        )

    @staticmethod
    def _price_range_contexts(
        cursor: Any,
        *,
        symbols: Sequence[str],
        decision_as_of_trade_date: date,
        target_trade_date: date,
        pit_universe_key: str | None = None,
    ) -> tuple[dict[str, PriceRangeRealtimeContext], tuple[dict[str, Any], ...]]:
        universe_key = (
            require_live_st_pit_universe_key(DEFAULT_ST_PIT_UNIVERSE_KEY)
            if pit_universe_key is None
            else require_canonical_rolling_universe_key(pit_universe_key)
        )
        cursor.execute(
            """
            SELECT status, dirty, start_date, end_date
              FROM market.stock_universe_pit_state
             WHERE universe_key = %s
            """,
            (universe_key,),
        )
        state = cursor.fetchone()
        if (
            state is None
            or str(state[0] or "").lower() != "ready"
            or bool(state[1])
            or state[2] is None
            or state[3] is None
            or state[2] > decision_as_of_trade_date
            or state[3] < decision_as_of_trade_date
        ):
            raise AdvisoryModelFirstError(
                "live ST PIT authority is not ready for the decision cutoff",
                reason_code="ADVISORY_PRICE_RANGE_PIT_ATTRIBUTE_UNAVAILABLE",
                context={
                    "decision_as_of_trade_date": decision_as_of_trade_date.isoformat(),
                    "universe_key": universe_key,
                },
            )

        cursor.execute(
            """
            SELECT status, quality_status
              FROM market.dataset_date_refresh_audit
             WHERE dataset = 'dividend' AND trade_date = %s
             ORDER BY refreshed_at DESC
             LIMIT 1
            """,
            (target_trade_date,),
        )
        dividend_audit = cursor.fetchone()
        dividend_ready = bool(
            dividend_audit
            and str(dividend_audit[0] or "").lower() == "success"
            and str(dividend_audit[1] or "").lower() in {"ok", "empty_valid"}
        )

        cursor.execute(
            """
            SELECT price.ts_code, price.close_li, basic.list_date,
                   CASE
                     WHEN basic.list_date IS NULL THEN NULL
                     WHEN basic.list_date < %s - INTERVAL '14 days' THEN 99
                     ELSE (
                       SELECT COUNT(*)
                         FROM market.trading_calendar cal
                        WHERE cal.is_trading = TRUE
                          AND cal.cal_date BETWEEN basic.list_date AND %s
                     )
                   END AS listed_trading_days
              FROM market.kline_daily_raw price
              LEFT JOIN market.stock_basic basic ON basic.ts_code = price.ts_code
             WHERE price.trade_date = %s
               AND price.ts_code = ANY(%s)
             ORDER BY price.ts_code
            """,
            (
                target_trade_date,
                target_trade_date,
                decision_as_of_trade_date,
                list(symbols),
            ),
        )
        decision_rows = {
            str(row[0]).upper(): row for row in cursor.fetchall()
        }

        cursor.execute(
            """
            SELECT DISTINCT ON (ts_code)
                   ts_code, event_kind, action_date, source_pub_date,
                   source_imp_date, source_effective_date
              FROM market.stock_universe_pit_events
             WHERE universe_key = %s
               AND ts_code = ANY(%s)
               AND action_date <= %s
               AND COALESCE(source_pub_date, source_imp_date,
                            source_effective_date, action_date) <= %s
             ORDER BY ts_code, action_date DESC, event_id DESC
            """,
            (
                universe_key,
                list(symbols),
                target_trade_date,
                decision_as_of_trade_date,
            ),
        )
        st_events = {str(row[0]).upper(): row for row in cursor.fetchall()}

        dividend_rows: dict[str, list[tuple[Any, ...]]] = {}
        if dividend_ready:
            cursor.execute(
                """
                SELECT ts_code, end_date, ann_date, div_proc, stk_div,
                       stk_bo_rate, stk_co_rate, cash_div, cash_div_tax,
                       imp_ann_date
                  FROM market.dividend
                 WHERE ex_date = %s
                   AND ts_code = ANY(%s)
                   AND div_proc = '实施'
                 ORDER BY ts_code, imp_ann_date DESC NULLS LAST,
                          end_date DESC, ann_date DESC
                """,
                (target_trade_date, list(symbols)),
            )
            for row in cursor.fetchall():
                dividend_rows.setdefault(str(row[0]).upper(), []).append(row)

        contexts: dict[str, PriceRangeRealtimeContext] = {}
        unavailable: list[dict[str, Any]] = []
        for symbol in symbols:
            normalized = str(symbol).upper()
            row = decision_rows.get(normalized)
            if row is None or row[1] is None:
                unavailable.append(
                    _price_context_failure(
                        normalized,
                        "ADVISORY_PRICE_RANGE_DECISION_PRICE_UNAVAILABLE",
                        "decision raw close is unavailable",
                    )
                )
                continue
            try:
                decision_raw_close = float(row[1]) / _PRICE_UNIT_DIVISOR
            except (TypeError, ValueError, OverflowError):
                decision_raw_close = float("nan")
            if not np.isfinite(decision_raw_close) or decision_raw_close <= 0:
                unavailable.append(
                    _price_context_failure(
                        normalized,
                        "ADVISORY_PRICE_RANGE_DECISION_PRICE_UNAVAILABLE",
                        "decision raw close is invalid",
                    )
                )
                continue
            if row[2] is None or row[3] is None:
                unavailable.append(
                    _price_context_failure(
                        normalized,
                        "ADVISORY_PRICE_RANGE_PIT_ATTRIBUTE_UNAVAILABLE",
                        "listing attributes are unavailable",
                    )
                )
                continue
            if not dividend_ready:
                unavailable.append(
                    _price_context_failure(
                        normalized,
                        "ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE",
                        "target ex-date dividend refresh receipt is unavailable",
                    )
                )
                continue
            try:
                multiplier, corporate_action_source = _target_raw_price_multiplier(
                    symbol=normalized,
                    decision_raw_close=decision_raw_close,
                    rows=dividend_rows.get(normalized, []),
                    decision_as_of_trade_date=decision_as_of_trade_date,
                )
                target_is_st = _project_target_st(st_events.get(normalized))
                board_type = _board_type(normalized)
            except AdvisoryModelFirstError as exc:
                unavailable.append(
                    _price_context_failure(normalized, exc.reason_code, str(exc))
                )
                continue
            contexts[normalized] = PriceRangeRealtimeContext(
                symbol=normalized,
                decision_raw_close=decision_raw_close,
                decision_price_trade_date=decision_as_of_trade_date,
                decision_price_source="market.kline_daily_raw.close_li",
                price_unit_divisor=_PRICE_UNIT_DIVISOR,
                target_raw_price_multiplier=multiplier,
                corporate_action_source=corporate_action_source,
                board_type=board_type,
                list_date=pd.Timestamp(row[2]).date(),
                listed_trading_days=int(row[3]),
                target_is_st=target_is_st,
                tick_size=0.01,
            )
        return contexts, tuple(unavailable)

    @staticmethod
    def _recent_trading_dates(cursor: Any, *, end_date: date, limit: int) -> pd.DatetimeIndex:
        cursor.execute(
            """
            SELECT cal_date
            FROM market.trading_calendar
            WHERE is_trading = TRUE AND cal_date <= %s
            ORDER BY cal_date DESC
            LIMIT %s
            """,
            (end_date, limit),
        )
        return pd.DatetimeIndex(sorted(pd.Timestamp(row[0]).normalize() for row in cursor.fetchall()))

    @staticmethod
    def _trading_calendar(cursor: Any, *, start_date: date, end_date: date) -> pd.DatetimeIndex:
        cursor.execute(
            """
            SELECT cal_date
            FROM market.trading_calendar
            WHERE is_trading = TRUE AND cal_date BETWEEN %s AND %s
            ORDER BY cal_date
            """,
            (start_date, end_date),
        )
        return pd.DatetimeIndex(pd.Timestamp(row[0]).normalize() for row in cursor.fetchall())

    @staticmethod
    def _candidate_daily(
        cursor: Any,
        *,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        frame = _read_frame(
            cursor,
            """
            WITH base_adj AS (
                SELECT DISTINCT ON (ts_code) ts_code, adj_factor AS base_adj_factor
                FROM market.adj_factor
                WHERE ts_code = ANY(%(symbols)s) AND trade_date <= %(end_date)s
                ORDER BY ts_code, trade_date DESC
            )
            SELECT price.trade_date, price.ts_code, price.open_li, price.high_li,
                   price.low_li, price.close_li, price.volume_hand, price.amount_li,
                   adj.adj_factor, base.base_adj_factor, limits.pre_close,
                   limits.up_limit, limits.down_limit
            FROM market.kline_daily_raw AS price
            LEFT JOIN market.adj_factor AS adj
              ON adj.ts_code = price.ts_code AND adj.trade_date = price.trade_date
            LEFT JOIN base_adj AS base ON base.ts_code = price.ts_code
            LEFT JOIN market.stk_limit AS limits
              ON limits.ts_code = price.ts_code AND limits.trade_date = price.trade_date
            WHERE price.ts_code = ANY(%(symbols)s)
              AND price.trade_date BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY price.trade_date, price.ts_code
            """,
            {"symbols": list(symbols), "start_date": start_date, "end_date": end_date},
        )
        return _market_frame(frame, context="candidate_daily")

    @staticmethod
    def _candidate_static(
        cursor: Any,
        *,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        parameters = {"symbols": list(symbols), "start_date": start_date, "end_date": end_date}
        frame = _read_frame(
            cursor,
            """
            SELECT trade_date, ts_code
            FROM market.kline_daily_raw
            WHERE ts_code = ANY(%(symbols)s)
              AND trade_date BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY trade_date, ts_code
            """,
            parameters,
        )
        projections = (
            """
            SELECT trade_date, ts_code,
                   turnover_rate AS db_turnover_rate,
                   volume_ratio AS db_volume_ratio,
                   pe_ttm AS db_pe_ttm, pb AS db_pb, circ_mv AS db_circ_mv
            FROM market.daily_basic
            WHERE ts_code = ANY(%(symbols)s)
              AND trade_date BETWEEN %(start_date)s AND %(end_date)s
            """,
            """
            SELECT trade_date, ts_code,
                   buy_lg_amount AS mf_lg_buy_amt,
                   sell_lg_amount AS mf_lg_sell_amt,
                   buy_elg_amount AS mf_elg_buy_amt,
                   sell_elg_amount AS mf_elg_sell_amt
            FROM market.moneyflow_ts
            WHERE ts_code = ANY(%(symbols)s)
              AND trade_date BETWEEN %(start_date)s AND %(end_date)s
            """,
            """
            SELECT trade_date, ts_code,
                   rev_yoy AS bb_rev_yoy, profit_yoy AS bb_profit_yoy,
                   gpr AS bb_gpr, npr AS bb_npr
            FROM market.bak_basic
            WHERE ts_code = ANY(%(symbols)s)
              AND trade_date BETWEEN %(start_date)s AND %(end_date)s
            """,
            """
            SELECT trade_date, ts_code,
                   cost_5pct AS cp_cost_5pct, cost_50pct AS cp_cost_50pct,
                   cost_95pct AS cp_cost_95pct, winner_rate AS cp_winner_rate
            FROM market.cyq_perf
            WHERE ts_code = ANY(%(symbols)s)
              AND trade_date BETWEEN %(start_date)s AND %(end_date)s
            """,
            """
            SELECT trade_date, ts_code, rzye AS md_rzye
            FROM market.margin_detail
            WHERE ts_code = ANY(%(symbols)s)
              AND trade_date BETWEEN %(start_date)s AND %(end_date)s
            """,
        )
        frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.normalize()
        for sql in projections:
            projection = _read_frame(cursor, sql, parameters)
            if projection.empty:
                continue
            projection["trade_date"] = pd.to_datetime(projection["trade_date"]).dt.normalize()
            if projection.duplicated(["trade_date", "ts_code"]).any():
                raise AdvisoryModelFirstError(
                    "realtime candidate factor source contains duplicate rows",
                    reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
                )
            frame = frame.merge(
                projection,
                on=["trade_date", "ts_code"],
                how="left",
                validate="one_to_one",
            )
        return _indexed_numeric_frame(frame, context="candidate_static")

    @staticmethod
    def _market_daily(cursor: Any, *, start_date: date, end_date: date) -> pd.DataFrame:
        frame = _read_frame(
            cursor,
            """
            SELECT price.trade_date, price.ts_code, price.open_li, price.high_li,
                   price.low_li, price.close_li, adj.adj_factor,
                   limits.up_limit, limits.down_limit
            FROM market.kline_daily_raw AS price
            JOIN market.stock_basic AS stock ON stock.ts_code = price.ts_code
            JOIN market.sector_data AS eligible
              ON eligible.trade_date = price.trade_date AND eligible.ts_code = price.ts_code
            LEFT JOIN market.adj_factor AS adj
              ON adj.ts_code = price.ts_code AND adj.trade_date = price.trade_date
            LEFT JOIN market.stk_limit AS limits
              ON limits.ts_code = price.ts_code AND limits.trade_date = price.trade_date
            WHERE price.trade_date BETWEEN %(start_date)s AND %(end_date)s
              AND stock.list_date <= price.trade_date
              AND (stock.delist_date IS NULL OR stock.delist_date > price.trade_date)
              AND (price.ts_code LIKE '%%.SH' OR price.ts_code LIKE '%%.SZ')
            ORDER BY price.trade_date, price.ts_code
            """,
            {"start_date": start_date, "end_date": end_date},
        )
        return _market_breadth_frame(frame)

    @staticmethod
    def _benchmark_daily(cursor: Any, *, start_date: date, end_date: date) -> pd.DataFrame:
        frame = _read_frame(
            cursor,
            """
            SELECT trade_date, ts_code, open, high, low, close, vol AS volume, amount
            FROM market.index_daily
            WHERE ts_code = '000300.SH' AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date
            """,
            (start_date, end_date),
        )
        return _indexed_numeric_frame(frame, context="benchmark_daily")

    @staticmethod
    def _candidate_sector_static(
        cursor: Any,
        *,
        candidate_index: pd.MultiIndex,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        base = candidate_index.to_frame(index=False).rename(
            columns={"datetime": "trade_date", "instrument": "ts_code"}
        )
        base["trade_date"] = pd.to_datetime(base["trade_date"]).dt.normalize()
        base = base[
            (base["trade_date"] >= pd.Timestamp(start_date))
            & (base["trade_date"] <= pd.Timestamp(end_date))
        ].copy()
        symbols = sorted(base["ts_code"].astype(str).unique().tolist())
        members = _read_frame(
            cursor,
            """
            SELECT ts_code, l2_code, in_date, out_date
            FROM market.sw_index_member
            WHERE ts_code = ANY(%s)
              AND in_date <= %s
              AND (out_date IS NULL OR out_date >= %s)
            ORDER BY in_date, ts_code, l2_code
            """,
            (symbols, end_date, start_date),
        )
        base["l2_code"] = _asof_l2_codes(base["trade_date"], base["ts_code"], members)
        l2_codes = sorted(base["l2_code"].dropna().astype(str).unique().tolist())
        if not l2_codes:
            raise AdvisoryModelFirstError(
                "candidate symbols have no PIT L2 industry mapping",
                reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            )
        quotes = _read_frame(
            cursor,
            """
            SELECT trade_date, ts_code AS l2_code, close AS sw2_close, amount AS sw2_amount
            FROM market.sw_daily
            WHERE ts_code = ANY(%s) AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date, ts_code
            """,
            (l2_codes, start_date, end_date),
        )
        sector_members = _read_frame(
            cursor,
            """
            SELECT ts_code, l2_code, in_date, out_date
            FROM market.sw_index_member
            WHERE l2_code = ANY(%s)
              AND in_date <= %s
              AND (out_date IS NULL OR out_date >= %s)
            ORDER BY in_date, ts_code, l2_code
            """,
            (l2_codes, end_date, start_date),
        )
        sector_symbols = sorted(sector_members["ts_code"].astype(str).unique().tolist())
        flow = _read_frame(
            cursor,
            """
            SELECT source.trade_date, source.ts_code, source.net_mf_amount
            FROM market.moneyflow_ts AS source
            JOIN market.sector_data AS eligible
              ON eligible.trade_date = source.trade_date AND eligible.ts_code = source.ts_code
            WHERE source.ts_code = ANY(%s) AND source.trade_date BETWEEN %s AND %s
            ORDER BY source.trade_date, source.ts_code
            """,
            (sector_symbols, start_date, end_date),
        )
        if not quotes.empty:
            quotes["trade_date"] = pd.to_datetime(quotes["trade_date"]).dt.normalize()
        if not flow.empty:
            flow["trade_date"] = pd.to_datetime(flow["trade_date"]).dt.normalize()
            flow["l2_code"] = _asof_l2_codes(flow["trade_date"], flow["ts_code"], sector_members)
            flow["net_mf_amount"] = pd.to_numeric(flow["net_mf_amount"], errors="coerce")
            l2_flow = (
                flow[flow["l2_code"].isin(l2_codes)]
                .groupby(["trade_date", "l2_code"], as_index=False)["net_mf_amount"]
                .sum(min_count=1)
                .rename(columns={"net_mf_amount": "sw2_mf_net_amt"})
            )
        else:
            l2_flow = pd.DataFrame(columns=["trade_date", "l2_code", "sw2_mf_net_amt"])
        frame = base.merge(quotes, on=["trade_date", "l2_code"], how="left", validate="many_to_one")
        frame = frame.merge(l2_flow, on=["trade_date", "l2_code"], how="left", validate="many_to_one")
        return _indexed_numeric_frame(frame, context="sector_static", preserve=("l2_code",))

    @staticmethod
    def _hmm_observations(
        cursor: Any,
        *,
        required_l2_code_map: dict[str, int],
        start_date: date,
        end_date: date,
        benchmark_daily: pd.DataFrame,
    ) -> pd.DataFrame:
        if not required_l2_code_map:
            return pd.DataFrame(
                columns=[
                    "sector_return_1",
                    "sector_excess_20",
                    "sector_amount_share",
                    "sector_limit_up_ratio",
                ],
                index=pd.MultiIndex.from_arrays([[], []], names=["datetime", "l2_code_id"]),
            )
        l2_codes = sorted(required_l2_code_map)
        sector = _read_frame(
            cursor,
            """
            SELECT daily.trade_date, daily.ts_code AS l2_code,
                   daily.close, daily.amount
            FROM market.sw_daily AS daily
            JOIN market.sw_index_classify AS classify
              ON classify.index_code = daily.ts_code AND classify.level = 'L2'
            WHERE daily.trade_date BETWEEN %s AND %s
            ORDER BY daily.trade_date, daily.ts_code
            """,
            (start_date, end_date),
        )
        members = _read_frame(
            cursor,
            """
            SELECT ts_code, l2_code, in_date, out_date
            FROM market.sw_index_member
            WHERE l2_code = ANY(%s)
              AND in_date <= %s
              AND (out_date IS NULL OR out_date >= %s)
            ORDER BY in_date, ts_code, l2_code
            """,
            (l2_codes, end_date, start_date),
        )
        member_symbols = sorted(members["ts_code"].astype(str).unique().tolist())
        limit_rows = _read_frame(
            cursor,
            """
            SELECT price.trade_date, price.ts_code, price.open_li,
                   price.low_li, price.close_li, limits.up_limit
            FROM market.kline_daily_raw AS price
            LEFT JOIN market.stk_limit AS limits
              ON limits.trade_date = price.trade_date AND limits.ts_code = price.ts_code
            WHERE price.ts_code = ANY(%s) AND price.trade_date BETWEEN %s AND %s
            ORDER BY price.trade_date, price.ts_code
            """,
            (member_symbols, start_date, end_date),
        )
        eligible = _read_frame(
            cursor,
            """
            SELECT trade_date, ts_code
            FROM market.sector_data
            WHERE ts_code = ANY(%s) AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date, ts_code
            """,
            (member_symbols, start_date, end_date),
        )
        if sector.empty:
            raise AdvisoryModelFirstError(
                "realtime HMM sector index query returned no rows",
                reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            )
        if not limit_rows.empty:
            limit_rows["trade_date"] = pd.to_datetime(limit_rows["trade_date"]).dt.normalize()
            eligible["trade_date"] = pd.to_datetime(eligible["trade_date"]).dt.normalize()
            limit_rows = limit_rows.merge(
                eligible,
                on=["trade_date", "ts_code"],
                how="inner",
                validate="one_to_one",
            )
            limit_rows["l2_code"] = _asof_l2_codes(
                limit_rows["trade_date"], limit_rows["ts_code"], members
            )
            up_limit = pd.to_numeric(limit_rows["up_limit"], errors="coerce")
            raw_open = pd.to_numeric(limit_rows["open_li"], errors="coerce") / _PRICE_UNIT_DIVISOR
            raw_low = pd.to_numeric(limit_rows["low_li"], errors="coerce") / _PRICE_UNIT_DIVISOR
            raw_close = pd.to_numeric(limit_rows["close_li"], errors="coerce") / _PRICE_UNIT_DIVISOR
            limit_rows["valid_limit"] = up_limit.notna()
            limit_rows["is_limit_up"] = (
                up_limit.notna()
                & (raw_open >= up_limit)
                & (raw_low >= up_limit)
                & (raw_close >= up_limit)
            )
            grouped = limit_rows[limit_rows["l2_code"].isin(l2_codes)].groupby(
                ["trade_date", "l2_code"]
            )
            limits = pd.DataFrame(
                {
                    "valid_count": grouped["valid_limit"].sum(),
                    "limit_up_count": grouped["is_limit_up"].sum(),
                }
            ).reset_index()
        else:
            limits = pd.DataFrame(columns=["trade_date", "l2_code", "valid_count", "limit_up_count"])
        sector["trade_date"] = pd.to_datetime(sector["trade_date"]).dt.normalize()
        sector["close"] = pd.to_numeric(sector["close"], errors="coerce")
        sector["amount"] = pd.to_numeric(sector["amount"], errors="coerce")
        sector["sector_amount_share"] = sector["amount"] / sector.groupby("trade_date")["amount"].transform(
            "sum"
        ).where(lambda value: value > 0)
        sector = sector[sector["l2_code"].isin(l2_codes)].copy()
        sector = sector.sort_values(["l2_code", "trade_date"])
        sector["sector_return_1"] = sector.groupby("l2_code")["close"].pct_change(1, fill_method=None)
        sector_return_20 = sector.groupby("l2_code")["close"].pct_change(20, fill_method=None)
        benchmark = benchmark_daily.reset_index().copy()
        benchmark["datetime"] = pd.to_datetime(benchmark["datetime"]).dt.normalize()
        benchmark = benchmark.drop_duplicates("datetime").set_index("datetime")["close"]
        benchmark_return_20 = pd.to_numeric(benchmark, errors="coerce").pct_change(20, fill_method=None)
        sector["sector_excess_20"] = sector_return_20 - sector["trade_date"].map(benchmark_return_20)
        if not limits.empty:
            limits["trade_date"] = pd.to_datetime(limits["trade_date"]).dt.normalize()
            valid_count = pd.to_numeric(limits["valid_count"], errors="coerce")
            limits["sector_limit_up_ratio"] = (
                pd.to_numeric(limits["limit_up_count"], errors="coerce") / valid_count.where(valid_count > 0)
            ).where(valid_count >= 5)
            sector = sector.merge(
                limits[["trade_date", "l2_code", "sector_limit_up_ratio"]],
                on=["trade_date", "l2_code"],
                how="left",
                validate="one_to_one",
            )
        else:
            sector["sector_limit_up_ratio"] = np.nan
        sector["l2_code_id"] = sector["l2_code"].map(required_l2_code_map).astype("int16")
        return sector.rename(columns={"trade_date": "datetime"}).set_index(
            ["datetime", "l2_code_id"]
        )[
            [
                "sector_return_1",
                "sector_excess_20",
                "sector_amount_share",
                "sector_limit_up_ratio",
            ]
        ].sort_index()

    @staticmethod
    def _suspend_rows(
        cursor: Any,
        *,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        frame = _read_frame(
            cursor,
            """
            SELECT DISTINCT trade_date, ts_code, suspend_type
            FROM market.suspend_d
            WHERE suspend_type = 'S'
              AND ts_code = ANY(%(symbols)s)
              AND trade_date BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY trade_date, ts_code
            """,
            {"symbols": list(symbols), "start_date": start_date, "end_date": end_date},
        )
        if frame.empty:
            return pd.DataFrame(columns=["trade_date", "instrument", "suspend_type"])
        frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.normalize()
        frame["instrument"] = frame["ts_code"].astype(str).str.upper()
        return frame[["trade_date", "instrument", "suspend_type"]]


def _read_frame(cursor: Any, sql: str, parameters: Any) -> pd.DataFrame:
    cursor.execute(sql, parameters)
    columns = [str(column.name) for column in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def _price_context_failure(symbol: str, reason_code: str, message: str) -> dict[str, Any]:
    return {"symbol": symbol, "reason_code": reason_code, "message": message}


def _target_raw_price_multiplier(
    *,
    symbol: str,
    decision_raw_close: float,
    rows: Sequence[tuple[Any, ...]],
    decision_as_of_trade_date: date,
) -> tuple[float, str]:
    if not rows:
        return 1.0, "market.dividend:target_ex_date:no_visible_action"
    economic_actions: set[tuple[float, float]] = set()
    for row in rows:
        if row[9] is None:
            raise AdvisoryModelFirstError(
                "implemented corporate action lacks its implementation announcement date",
                reason_code="ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE",
                context={"symbol": symbol},
            )
        try:
            implementation_announcement_date = pd.Timestamp(row[9]).date()
        except (TypeError, ValueError, OverflowError) as exc:
            raise AdvisoryModelFirstError(
                "corporate-action implementation announcement date is invalid",
                reason_code="ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE",
                context={"symbol": symbol},
            ) from exc
        if implementation_announcement_date > decision_as_of_trade_date:
            continue
        try:
            stock_dividend = _optional_nonnegative_component(row[4])
            stock_bonus = _optional_nonnegative_component(row[5])
            stock_capitalization = _optional_nonnegative_component(row[6])
            cash_dividend = _optional_nonnegative_component(row[7])
            if row[8] is None and cash_dividend > 0:
                raise ValueError("pre-tax cash dividend is missing")
            cash_dividend_tax = _optional_nonnegative_component(row[8])
        except (TypeError, ValueError, OverflowError) as exc:
            raise AdvisoryModelFirstError(
                "corporate-action implementation values are incomplete",
                reason_code="ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE",
                context={"symbol": symbol},
            ) from exc
        values = (stock_dividend, stock_bonus, stock_capitalization, cash_dividend_tax)
        if not all(np.isfinite(value) and value >= 0 for value in values):
            raise AdvisoryModelFirstError(
                "corporate-action implementation values are invalid",
                reason_code="ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE",
                context={"symbol": symbol},
            )
        if not np.isclose(
            stock_dividend,
            stock_bonus + stock_capitalization,
            rtol=0.0,
            atol=1e-8,
        ):
            raise AdvisoryModelFirstError(
                "corporate-action stock distribution components are inconsistent",
                reason_code="ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE",
                context={"symbol": symbol},
            )
        economic_actions.add((round(stock_dividend, 12), round(cash_dividend_tax, 12)))
    if not economic_actions:
        return 1.0, "market.dividend:target_ex_date:no_visible_action"
    if len(economic_actions) != 1:
        raise AdvisoryModelFirstError(
            "multiple visible corporate-action rows have different economics",
            reason_code="ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE",
            context={"symbol": symbol, "economic_action_count": len(economic_actions)},
        )
    stock_dividend, cash_dividend_tax = next(iter(economic_actions))
    multiplier = (decision_raw_close - cash_dividend_tax) / (
        decision_raw_close * (1.0 + stock_dividend)
    )
    if not np.isfinite(multiplier) or multiplier <= 0:
        raise AdvisoryModelFirstError(
            "corporate-action target price multiplier is invalid",
            reason_code="ADVISORY_PRICE_RANGE_CORPORATE_ACTION_INPUT_UNAVAILABLE",
            context={"symbol": symbol},
        )
    return float(multiplier), "market.dividend:decision_visible_implemented_action"


def _optional_nonnegative_component(value: Any) -> float:
    if value is None or str(value).strip() == "":
        return 0.0
    parsed = float(value)
    if not np.isfinite(parsed) or parsed < 0:
        raise ValueError("corporate-action component is invalid")
    return parsed


def _project_target_st(event_row: tuple[Any, ...] | None) -> bool:
    if event_row is None:
        return False
    event_kind = str(event_row[1] or "").strip().lower()
    if event_kind == "st_negative":
        return True
    if event_kind == "st_restore":
        return False
    raise AdvisoryModelFirstError(
        "latest target-effective PIT event does not define an ST state",
        reason_code="ADVISORY_PRICE_RANGE_PIT_ATTRIBUTE_UNAVAILABLE",
        context={"event_kind": event_kind},
    )


def _board_type(symbol: str) -> str:
    code, _, exchange = symbol.partition(".")
    if exchange == "BJ":
        return "BSE"
    if exchange == "SH" and code.startswith(("688", "689")):
        return "STAR"
    if exchange == "SZ" and code.startswith(("300", "301")):
        return "CHINEXT"
    if exchange in {"SH", "SZ"}:
        return "MAIN"
    raise AdvisoryModelFirstError(
        "candidate exchange does not have a supported A-share price-limit rule",
        reason_code="ADVISORY_PRICE_RANGE_PIT_ATTRIBUTE_UNAVAILABLE",
        context={"symbol": symbol},
    )


def _market_frame(frame: pd.DataFrame, *, context: str) -> pd.DataFrame:
    if frame.empty:
        raise AdvisoryModelFirstError(
            "realtime market query returned no rows",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            context={"source": context},
        )
    numeric = frame.copy()
    for column in (
        "open_li",
        "high_li",
        "low_li",
        "close_li",
        "volume_hand",
        "amount_li",
        "adj_factor",
        "base_adj_factor",
        "pre_close",
        "up_limit",
        "down_limit",
    ):
        numeric[column] = pd.to_numeric(numeric[column], errors="coerce")
    factor = numeric["adj_factor"] / numeric["base_adj_factor"]
    result = pd.DataFrame()
    result["datetime"] = pd.to_datetime(numeric["trade_date"]).dt.normalize()
    result["instrument"] = numeric["ts_code"].astype(str).str.upper()
    result["factor"] = factor
    result["open"] = numeric["open_li"] / _PRICE_UNIT_DIVISOR * factor
    result["high"] = numeric["high_li"] / _PRICE_UNIT_DIVISOR * factor
    result["low"] = numeric["low_li"] / _PRICE_UNIT_DIVISOR * factor
    result["close"] = numeric["close_li"] / _PRICE_UNIT_DIVISOR * factor
    result["volume"] = numeric["volume_hand"] * 100.0 / factor
    result["amount"] = numeric["amount_li"] / _PRICE_UNIT_DIVISOR
    result["prev_close"] = numeric["pre_close"]
    result["up_limit_price"] = numeric["up_limit"]
    result["down_limit_price"] = numeric["down_limit"]
    raw_open = numeric["open_li"] / _PRICE_UNIT_DIVISOR
    raw_high = numeric["high_li"] / _PRICE_UNIT_DIVISOR
    raw_low = numeric["low_li"] / _PRICE_UNIT_DIVISOR
    raw_close = numeric["close_li"] / _PRICE_UNIT_DIVISOR
    valid_limits = numeric["up_limit"].notna() & numeric["down_limit"].notna()
    result["limit_up"] = np.where(
        valid_limits,
        ((raw_open >= numeric["up_limit"]) & (raw_low >= numeric["up_limit"]) & (raw_close >= numeric["up_limit"])).astype(float),
        np.nan,
    )
    result["limit_down"] = np.where(
        valid_limits,
        ((raw_open <= numeric["down_limit"]) & (raw_high <= numeric["down_limit"]) & (raw_close <= numeric["down_limit"])).astype(float),
        np.nan,
    )
    return _finalize_index(result, context=context)


def _market_breadth_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        raise AdvisoryModelFirstError(
            "realtime market breadth query returned no rows",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
        )
    result = _limit_flag_frame(frame, context="market_daily")
    raw_close = pd.to_numeric(frame["close_li"], errors="coerce") / _PRICE_UNIT_DIVISOR
    adj_factor = pd.to_numeric(frame["adj_factor"], errors="coerce")
    close = pd.DataFrame(
        {
            "datetime": pd.to_datetime(frame["trade_date"]).dt.normalize(),
            "instrument": frame["ts_code"].astype(str).str.upper(),
            "close": raw_close * adj_factor,
        }
    ).set_index(["datetime", "instrument"])["close"]
    result["close"] = close.reindex(result.index)
    return result[["close", "limit_up"]]


def _limit_flag_frame(frame: pd.DataFrame, *, context: str) -> pd.DataFrame:
    if frame.empty:
        raise AdvisoryModelFirstError(
            "realtime limit-state query returned no rows",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            context={"source": context},
        )
    raw_open = pd.to_numeric(frame["open_li"], errors="coerce") / _PRICE_UNIT_DIVISOR
    raw_low = pd.to_numeric(frame["low_li"], errors="coerce") / _PRICE_UNIT_DIVISOR
    raw_close = pd.to_numeric(frame["close_li"], errors="coerce") / _PRICE_UNIT_DIVISOR
    up_limit = pd.to_numeric(frame["up_limit"], errors="coerce")
    down_limit = pd.to_numeric(frame["down_limit"], errors="coerce")
    valid_limits = up_limit.notna() & down_limit.notna()
    result = pd.DataFrame(
        {
            "datetime": pd.to_datetime(frame["trade_date"]).dt.normalize(),
            "instrument": frame["ts_code"].astype(str).str.upper(),
            "limit_up": np.where(
                valid_limits,
                ((raw_open >= up_limit) & (raw_low >= up_limit) & (raw_close >= up_limit)).astype(float),
                np.nan,
            ),
        }
    )
    return _finalize_index(result, context=context)


def _indexed_numeric_frame(
    frame: pd.DataFrame,
    *,
    context: str,
    preserve: Sequence[str] = (),
) -> pd.DataFrame:
    if frame.empty:
        raise AdvisoryModelFirstError(
            "realtime feature query returned no rows",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            context={"source": context},
        )
    result = frame.copy()
    result["datetime"] = pd.to_datetime(result.pop("trade_date")).dt.normalize()
    result["instrument"] = result.pop("ts_code").astype(str).str.upper()
    preserved = {name: result.pop(name) for name in preserve if name in result.columns}
    for column in result.columns:
        if column not in {"datetime", "instrument"}:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    for name, values in preserved.items():
        result[name] = values
    return _finalize_index(result, context=context)


def _finalize_index(frame: pd.DataFrame, *, context: str) -> pd.DataFrame:
    if frame.duplicated(["datetime", "instrument"]).any():
        sample = frame.loc[frame.duplicated(["datetime", "instrument"], keep=False), ["datetime", "instrument"]].head(10)
        raise AdvisoryModelFirstError(
            "realtime feature query returned duplicate date-symbol rows",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            context={"source": context, "examples": sample.astype(str).to_dict("records")},
        )
    return frame.set_index(["datetime", "instrument"]).sort_index()


def _encode_l2(frame: pd.DataFrame, code_map: dict[str, int]) -> pd.DataFrame:
    result = frame.copy()
    codes = result.pop("l2_code") if "l2_code" in result.columns else pd.Series(None, index=result.index)
    result["l2_code_id"] = np.asarray(encode_l2_codes(codes.tolist(), code_map), dtype=np.int16)
    return result


def _attach_sector_projection(candidate_static: pd.DataFrame, sector_static: pd.DataFrame) -> pd.DataFrame:
    columns = ("sw2_close", "sw2_amount", "sw2_mf_net_amt", "l2_code_id")
    missing = sorted(set(columns) - set(sector_static.columns))
    if missing:
        raise AdvisoryModelFirstError(
            "realtime sector projection is incomplete",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            context={"missing_columns": missing},
        )
    result = candidate_static.copy()
    projected = sector_static.loc[:, columns].reindex(result.index)
    for column in columns:
        result[column] = projected[column]
    return result


def _asof_l2_codes(trade_dates: pd.Series, symbols: pd.Series, members: pd.DataFrame) -> list[str | None]:
    row_count = len(trade_dates)
    if members.empty:
        return [None] * row_count
    right = members.copy()
    right["ts_code"] = right["ts_code"].astype(str)
    right["l2_code"] = right["l2_code"].astype("string")
    right["in_date"] = pd.to_datetime(right["in_date"]).dt.normalize()
    right["out_date"] = pd.to_datetime(right["out_date"]).dt.normalize()
    right = right.dropna(subset=["in_date", "l2_code"])
    ambiguous = (
        right.groupby(["ts_code", "in_date"])["l2_code"].nunique(dropna=True).gt(1)
    )
    if ambiguous.any():
        examples = [
            {"ts_code": str(item[0]), "in_date": pd.Timestamp(item[1]).date().isoformat()}
            for item in ambiguous.index[ambiguous][:10]
        ]
        raise AdvisoryModelFirstError(
            "realtime PIT industry membership has ambiguous latest intervals",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            context={"ambiguous_interval_count": int(ambiguous.sum()), "examples": examples},
        )
    right = right.drop_duplicates(["ts_code", "in_date"], keep="last").sort_values(
        ["in_date", "ts_code", "l2_code"]
    )
    left = pd.DataFrame(
        {
            "_order": np.arange(row_count, dtype=np.int64),
            "trade_date": pd.to_datetime(trade_dates).dt.normalize(),
            "ts_code": symbols.astype(str).to_numpy(),
        }
    ).sort_values(["trade_date", "ts_code"])
    merged = pd.merge_asof(
        left,
        right,
        left_on="trade_date",
        right_on="in_date",
        by="ts_code",
        direction="backward",
    )
    active = merged["out_date"].isna() | (merged["out_date"] >= merged["trade_date"])
    merged.loc[~active, "l2_code"] = None
    merged = merged.sort_values("_order")
    return [str(value) if pd.notna(value) else None for value in merged["l2_code"]]


def _decision_l2_codes(frame: pd.DataFrame, decision_date: date) -> tuple[int, ...]:
    decision = pd.Timestamp(decision_date).normalize()
    try:
        rows = frame.xs(decision, level="datetime")
    except KeyError as exc:
        raise AdvisoryModelFirstError(
            "candidate static features are absent on the decision date",
            reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
            context={"decision_as_of_trade_date": decision_date.isoformat()},
        ) from exc
    return tuple(sorted({int(value) for value in rows["l2_code_id"].dropna().tolist() if int(value) >= 0}))
