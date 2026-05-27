"""Display/value enrichment for Selection Center results.

The enrichment is outside alpha scoring. It annotates selected symbols with the
same stock-name resolver used by Paper v2 and with explicit entry/current price
semantics so watchlist import can use the point-in-time entry price instead of
display-only realtime quotes.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Callable, Iterable, Iterator
from zoneinfo import ZoneInfo

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.models.analysis import StockQuote
from backend.services.analysis_service import get_realtime_quote
from backend.services.paper_trading_v2.market_data import PRICE_UNIT_DIVISOR
from backend.services.paper_trading_v2.symbol_names import PaperV2SymbolNameResolver
from backend.services.selection_center.models import SelectionCandidate
from backend.services.trading_core.errors import DataUnavailableError

ConnFactory = Callable[[], Iterator[Any]]
QuoteFetcher = Callable[[str], StockQuote]

DISPLAY_COMPONENT_KEY = "selection_result_display"
CHINA_TZ = ZoneInfo("Asia/Shanghai")


class SelectionResultEnrichmentService:
    def __init__(
        self,
        *,
        conn_factory: ConnFactory | None = None,
        symbol_name_resolver: PaperV2SymbolNameResolver | Any | None = None,
        quote_fetcher: QuoteFetcher | None = None,
        today_provider: Callable[[], date] | None = None,
    ) -> None:
        self._conn_factory = conn_factory or get_conn
        self._symbol_name_resolver = symbol_name_resolver or PaperV2SymbolNameResolver(self._conn_factory)
        self._quote_fetcher = quote_fetcher or get_realtime_quote
        self._today_provider = today_provider or (lambda: datetime.now(CHINA_TZ).date())

    def enrich_candidates(
        self,
        candidates: Iterable[SelectionCandidate],
        *,
        trade_date: date,
        runtime_config: dict[str, Any] | None = None,
    ) -> list[SelectionCandidate]:
        rows = list(candidates)
        if not rows:
            return []
        symbols = [row.symbol for row in rows]
        names = self._symbol_name_resolver.resolve(symbols)
        quotes = self._load_current_quotes(symbols)
        today = self._today_provider()
        reference_trade_date = self._reference_price_trade_date(runtime_config or {}, fallback=trade_date)
        historical_rows = (
            {}
            if trade_date >= today
            else self._load_daily_rows(symbols, reference_trade_date)
        )
        enriched: list[SelectionCandidate] = []
        missing_current_entry_price: list[str] = []
        for candidate in rows:
            symbol = candidate.symbol
            quote = quotes.get(symbol)
            daily = historical_rows.get(symbol) or {}
            current_price = _positive_float(getattr(quote, "current_price", None))
            quote_previous_close = _positive_float(getattr(quote, "pre_close", None))
            current_time = _iso_or_none(getattr(quote, "quote_timestamp", None))
            current_source = str(getattr(quote, "quote_source", None) or "TDX_REALTIME") if quote else None
            stock_name = (
                candidate.stock_name
                or names.get(symbol)
                or str(getattr(quote, "name", "") or "").strip()
                or None
            )

            if trade_date >= today:
                entry_price = current_price or quote_previous_close
                if current_price is not None:
                    entry_source = current_source or "TDX_REALTIME"
                else:
                    entry_source = _tdx_pre_close_entry_source(current_source)
                entry_time = current_time or datetime.now(CHINA_TZ).isoformat()
                if entry_price is None:
                    missing_current_entry_price.append(symbol)
            else:
                entry_price = _positive_float(daily.get("close")) or _positive_float(candidate.reference_price)
                entry_source = (
                    f"market.kline_daily_raw.close:{reference_trade_date.isoformat()}"
                    if daily.get("close") is not None
                    else "selection_candidate.reference_price"
                )
                entry_time = reference_trade_date.isoformat()

            previous_close = quote_previous_close or _positive_float(daily.get("close"))
            volume = _non_negative_float(getattr(quote, "volume", None))
            if volume is None:
                volume = _non_negative_float(daily.get("volume"))

            display_payload = {
                "stock_name": stock_name,
                "selection_entry_price": entry_price,
                "selection_entry_price_source": entry_source if entry_price is not None else None,
                "selection_entry_price_time": entry_time if entry_price is not None else None,
                "reference_price_trade_date": reference_trade_date.isoformat(),
                "previous_close": previous_close,
                "volume": volume,
                "current_price": current_price,
                "current_price_source": current_source if current_price is not None else None,
                "current_price_time": current_time,
                "current_price_display_only": True,
            }
            component_scores = dict(candidate.component_scores or {})
            component_scores[DISPLAY_COMPONENT_KEY] = display_payload
            enriched.append(
                candidate.model_copy(
                    update={
                        "stock_name": stock_name,
                        "selection_entry_price": entry_price,
                        "selection_entry_price_source": display_payload["selection_entry_price_source"],
                        "selection_entry_price_time": display_payload["selection_entry_price_time"],
                        "previous_close": previous_close,
                        "volume": volume,
                        "current_price": current_price,
                        "current_price_source": display_payload["current_price_source"],
                        "current_price_time": current_time,
                        "reference_price": entry_price,
                        "component_scores": component_scores,
                    }
                )
            )
        if missing_current_entry_price:
            raise DataUnavailableError(
                "current-date selection requires TDX quote price for watchlist entry price",
                context={
                    "trade_date": trade_date.isoformat(),
                    "missing_price_count": len(missing_current_entry_price),
                    "missing_price_examples": missing_current_entry_price[:20],
                    "price_role": "selection_entry_price",
                    "source": "TDX_REALTIME",
                },
            )
        return enriched

    def _load_current_quotes(self, symbols: Iterable[str]) -> dict[str, StockQuote]:
        quotes: dict[str, StockQuote] = {}
        for symbol in sorted({str(item or "").strip() for item in symbols if str(item or "").strip()}):
            try:
                quote = self._quote_fetcher(symbol)
            except Exception:
                continue
            if quote is not None:
                quotes[symbol] = quote
        return quotes

    def _load_daily_rows(self, symbols: list[str], trade_date: date) -> dict[str, dict[str, float]]:
        clean = sorted({str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()})
        if not clean:
            return {}
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT ts_code, close_li, volume_hand
                        FROM market.kline_daily_raw
                        WHERE trade_date = %s
                          AND ts_code = ANY(%s)
                          AND close_li IS NOT NULL
                          AND close_li > 0
                        """,
                        (trade_date, clean),
                    )
                    rows = cur.fetchall()
        except Exception:
            return {}
        result: dict[str, dict[str, float]] = {}
        for row in rows:
            symbol = str(row.get("ts_code") or "").strip()
            close = _positive_float(row.get("close_li"))
            if symbol and close is not None:
                result[symbol] = {
                    "close": close / PRICE_UNIT_DIVISOR,
                    "volume": _non_negative_float(row.get("volume_hand")) or 0.0,
                }
        return result

    @staticmethod
    def _reference_price_trade_date(runtime_config: dict[str, Any], *, fallback: date) -> date:
        point_in_time = runtime_config.get("point_in_time_context")
        if isinstance(point_in_time, dict):
            raw = point_in_time.get("reference_price_trade_date") or point_in_time.get("cutoff_date")
            if raw:
                try:
                    return date.fromisoformat(str(raw))
                except ValueError:
                    pass
        return fallback


def display_fields_from_component_scores(component_scores: dict[str, Any] | None) -> dict[str, Any]:
    display = (component_scores or {}).get(DISPLAY_COMPONENT_KEY)
    return dict(display) if isinstance(display, dict) else {}


def component_scores_with_display_fields(candidate: SelectionCandidate) -> dict[str, Any]:
    component_scores = dict(candidate.component_scores or {})
    display = {
        "stock_name": candidate.stock_name,
        "selection_entry_price": candidate.selection_entry_price,
        "selection_entry_price_source": candidate.selection_entry_price_source,
        "selection_entry_price_time": candidate.selection_entry_price_time,
        "previous_close": candidate.previous_close,
        "volume": candidate.volume,
        "current_price": candidate.current_price,
        "current_price_source": candidate.current_price_source,
        "current_price_time": candidate.current_price_time,
        "current_price_display_only": True,
    }
    if any(value is not None for value in display.values()):
        component_scores[DISPLAY_COMPONENT_KEY] = display
    return component_scores


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _non_negative_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _tdx_pre_close_entry_source(source: str | None) -> str:
    label = str(source or "").strip()
    if not label or "tdx" in label.casefold():
        return "TDX latest close / pre_close"
    return f"{label} latest close / pre_close"


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = str(value).strip()
    return text or None
