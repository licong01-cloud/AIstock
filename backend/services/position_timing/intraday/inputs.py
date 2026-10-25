"""Explicit live and frozen-file adapters; no source fallback or data repair."""

from __future__ import annotations

import hashlib
import json
import threading
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from .contracts import CHINA, Calendar, DayFacts, MarketView, MinuteBar, Plan, digest, sessions, utc


def normalize_tdx_bars(rows: list[dict], *, trade_date: date, received_at: datetime) -> tuple[MinuteBar, ...]:
    """The shared TDX adapter already converts prices to CNY, but volume to hands."""
    result = {}
    for row in rows:
        end = utc(row["time"])
        start = end - timedelta(minutes=1)
        if end.astimezone(CHINA).date() != trade_date:
            raise ValueError("TDX_BAR_DATE_MISMATCH")
        # Auction observations and the unfinished tail are explicitly outside v1.
        if not any(left <= start < end <= right for left, right in sessions(trade_date)):
            continue
        if end > received_at - timedelta(seconds=5):
            continue
        hands = Decimal(str(row["volume"]))
        shares = Decimal(str(row["volume_shares"])) if "volume_shares" in row else hands * 100
        if "volume_shares" in row and shares // 100 != hands:
            raise ValueError("TDX_VOLUME_UNITS_MISMATCH")
        bar = MinuteBar(start=start, end=end, received_at=received_at,
                        **{k: str(row[k]) for k in ("open", "high", "low", "close")},
                        volume_shares=shares,
                        amount_cny=str(row["amount"]) if row.get("amount") is not None else None)
        key = end.isoformat()
        if key in result and result[key].content_sha256() != bar.content_sha256():
            raise ValueError("TDX_DUPLICATE_BAR_CONFLICT")
        result[key] = bar
    return tuple(result[k] for k in sorted(result))


class LiveInputs:
    """Read only existing services; freeze each day's context on first observation."""

    def __init__(self, now_provider) -> None:
        self.now = now_provider
        self._contexts: dict[tuple[str, date], tuple[Calendar, DayFacts]] = {}
        self._lock = threading.Lock()

    def __call__(self, plan: Plan) -> MarketView:
        from backend.data_service.tdx_adapter import fetch_minute_kline_tdx

        key = (plan.symbol, plan.target_trade_date)
        with self._lock:
            context = self._contexts.get(key)
        if context is None:
            context = self._load_context(plan)
            calendar, facts = context
            ready = not calendar.is_trading_day or facts.is_suspended is True or (
                facts.is_suspended is False and facts.has_daily_limit is not None and facts.reference_price is not None
            )
            if ready:
                with self._lock:
                    self._contexts = {k: v for k, v in self._contexts.items() if k[1] == plan.target_trade_date}
                    context = self._contexts.setdefault(key, context)
        calendar, facts = context
        rows = fetch_minute_kline_tdx(plan.symbol, plan.target_trade_date) if calendar.is_trading_day and not facts.is_suspended else []
        received_at = utc(self.now())
        return MarketView(symbol=plan.symbol, trade_date=plan.target_trade_date,
                          source_ref="TDX_REALTIME.fetch_minute_kline_tdx", source_version="tdx_raw_cny_hands_v1",
                          calendar=calendar, day_facts=facts,
                          bars=normalize_tdx_bars(rows, trade_date=plan.target_trade_date, received_at=received_at),
                          visibility_basis="ACTUAL_RECEPTION")

    def _load_context(self, plan: Plan) -> tuple[Calendar, DayFacts]:
        from backend.services.simulation_data.daily_context_provider import DailyTradingContextProvider
        from backend.services.simulation_data.trading_calendar import TradeCalendarProvider

        snapshot = TradeCalendarProvider().snapshot(plan.target_trade_date)
        calendar = Calendar(trade_date=plan.target_trade_date, is_trading_day=snapshot.is_trading_day,
                            source_ref=snapshot.source, source_sha256=digest(snapshot.model_dump()), available_at=self.now())
        if not calendar.is_trading_day:
            return calendar, DayFacts(trade_date=plan.target_trade_date, symbol=plan.symbol, is_suspended=None,
                                      has_daily_limit=None, available_at=self.now(), source_ref="NON_TRADING_DAY",
                                      source_sha256=digest(snapshot.model_dump()))
        provider = DailyTradingContextProvider()
        supporting = provider.load_supporting_facts(symbols=[plan.symbol], trade_date=plan.target_trade_date)
        suspend = supporting["suspend_facts"][plan.symbol]["is_suspended"]
        limits = provider.load_stk_limit_authority_attempt(symbols=[plan.symbol], trade_date=plan.target_trade_date)
        rows = limits["rows"]
        if rows and (len(rows) != 1 or rows[0]["symbol"] != plan.symbol or rows[0]["trade_date"] != plan.target_trade_date.isoformat()):
            raise ValueError("DAILY_FACT_IDENTITY_CONFLICT")
        row = rows[0] if rows else {}
        # A missing limit row never means an unbounded price regime.
        has_limit = True if row.get("up_limit") is not None and row.get("down_limit") is not None else None
        identity = {"supporting": supporting, "limits": limits}
        facts = DayFacts(trade_date=plan.target_trade_date, symbol=plan.symbol, is_suspended=suspend,
                         has_daily_limit=has_limit,
                         reference_price=str(row["pre_close"]) if row.get("pre_close") is not None else None,
                         limit_up=str(row["up_limit"]) if row.get("up_limit") is not None else None,
                         limit_down=str(row["down_limit"]) if row.get("down_limit") is not None else None,
                         available_at=self.now(), source_ref="DailyTradingContextProvider.read_only", source_sha256=digest(identity))
        return calendar, facts


def file_sha256(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def read_records(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".jsonl":
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if path.suffix.lower() == ".parquet":
        import pandas as pd

        return pd.read_parquet(path).to_dict("records")
    if path.suffix.lower() == ".json":
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, list):
            raise ValueError("FROZEN_RECORDS_MUST_BE_ARRAY")
        return value
    raise ValueError("UNSUPPORTED_FROZEN_FILE_FORMAT")


def visible_prefix(market: MarketView, as_of: datetime) -> MarketView:
    """A historical adapter may see the file; the core sees only observed rows."""
    return market.model_copy(update={"bars": tuple(b for b in market.bars if max(b.end, b.received_at, b.source_available_at or b.end) <= as_of)})
