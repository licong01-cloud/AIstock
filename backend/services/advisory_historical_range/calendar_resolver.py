"""Read-only trading-calendar resolution for historical-range requests."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any

import psycopg2.extras

from .canonical import canonical_json_sha256
from .models import (
    HistoricalRangeDatePlanV1,
    HistoricalRangeFrozenProgramV1,
    HistoricalRangeProgramDayWindowV1,
    HistoricalRangeProgramWarmupComponentV1,
    HistoricalRangeProgramWarmupRangeV1,
    HistoricalRangeResearchBatchRequestV1,
)


class HistoricalRangeCalendarResolver:
    def __init__(self, *, conn_factory: Callable[[], Any]) -> None:
        if conn_factory is None:
            raise ValueError("historical calendar resolver requires conn_factory")
        self._conn_factory = conn_factory

    def resolve(
        self,
        *,
        request: HistoricalRangeResearchBatchRequestV1,
        frozen_programs: tuple[HistoricalRangeFrozenProgramV1, ...],
    ) -> tuple[HistoricalRangeDatePlanV1, str]:
        max_window = max(
            component.required_window + component.buffer_trading_days
            for program in frozen_programs
            for component in program.admitted_package_projection.components
        )
        with self._conn_factory() as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT cal_date
                    FROM market.trading_calendar
                    WHERE is_trading = TRUE AND cal_date <= %s
                    ORDER BY cal_date DESC
                    LIMIT %s
                    """,
                    (request.start_trade_date, max_window),
                )
                warmup_dates = tuple(sorted(_row_date(row) for row in cur.fetchall()))
                cur.execute(
                    """
                    SELECT cal_date
                    FROM market.trading_calendar
                    WHERE is_trading = TRUE AND cal_date BETWEEN %s AND %s
                    ORDER BY cal_date
                    """,
                    (request.start_trade_date, request.end_trade_date),
                )
                ordered_dates = tuple(_row_date(row) for row in cur.fetchall())
                cur.execute(
                    "SELECT MAX(trade_date) AS completed_trade_date "
                    "FROM market.kline_daily_raw WHERE trade_date <= %s",
                    (request.end_trade_date,),
                )
                watermark_row = cur.fetchone()
            conn.rollback()
        watermark = _row_value_date(watermark_row, "completed_trade_date")
        if not ordered_dates or ordered_dates[0] != request.start_trade_date or ordered_dates[-1] != request.end_trade_date:
            raise ValueError("historical range start/end must both be completed trading dates")
        if watermark is None or request.end_trade_date > watermark:
            raise ValueError("historical range end exceeds the database completed trade-date watermark")
        if len(warmup_dates) < max_window:
            raise ValueError("historical trading calendar cannot satisfy the longest Alpha lookback")

        warmups: dict[str, HistoricalRangeProgramWarmupRangeV1] = {}
        all_calendar_dates = tuple(sorted(set(warmup_dates) | set(ordered_dates)))
        calendar_index = {trade_date: ordinal for ordinal, trade_date in enumerate(all_calendar_dates)}
        for program in frozen_programs:
            components = []
            for component in program.admitted_package_projection.components:
                required_window = component.required_window + component.buffer_trading_days
                day_windows = tuple(
                    HistoricalRangeProgramDayWindowV1(
                        decision_trade_date=trade_date,
                        window_start_trade_date=all_calendar_dates[
                            calendar_index[trade_date] - required_window + 1
                        ],
                    )
                    for trade_date in ordered_dates
                )
                components.append(
                    HistoricalRangeProgramWarmupComponentV1(
                        component_id=component.component_id,
                        warmup_start_trade_date=warmup_dates[-required_window],
                        range_start_trade_date=request.start_trade_date,
                        lookback_contract_hash=component.lookback_contract_hash,
                        day_windows=day_windows,
                    )
                )
            warmups[program.research_program_id] = HistoricalRangeProgramWarmupRangeV1(
                research_program_id=program.research_program_id,
                components=tuple(components),
            )
        calendar_identity_hash = canonical_json_sha256(
            {
                "schema_version": "advisory_historical_range_calendar_identity_v1",
                "calendar_id": "cn_a_share",
                "ordered_trade_dates": [item.isoformat() for item in ordered_dates],
                "warmup_dates": [item.isoformat() for item in warmup_dates],
                "completed_trade_date_watermark": watermark.isoformat(),
            }
        )
        return (
            HistoricalRangeDatePlanV1(
                calendar_id="cn_a_share",
                calendar_version=f"sha256:{calendar_identity_hash}",
                start_trade_date=request.start_trade_date,
                end_trade_date=request.end_trade_date,
                ordered_trade_dates=ordered_dates,
                completed_trade_date_watermark=watermark,
                per_program_input_warmup_ranges=warmups,
            ),
            calendar_identity_hash,
        )


def _row_date(row: Any) -> date:
    value = row["cal_date"] if isinstance(row, dict) else row[0]
    parsed = _as_date(value)
    if parsed is None:
        raise ValueError("trading calendar returned an invalid cal_date")
    return parsed


def _row_value_date(row: Any, key: str) -> date | None:
    if row is None:
        return None
    value = row.get(key) if isinstance(row, dict) else row[0]
    return _as_date(value)


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])
