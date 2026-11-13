"""Read-only display quotes. Never feeds ranking, settlement or source ledgers."""
from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, time, timedelta
import math
import re
from threading import Lock, BoundedSemaphore
from time import monotonic
from zoneinfo import ZoneInfo

import requests

from backend.data_service.tdx_adapter import TDX_BASE_URL, _to_tdx_code
from .projection import WorkbenchError
from .repository import ReadRepository

CN = ZoneInfo("Asia/Shanghai")
SYMBOL = re.compile(r"^[0-9]{6}\.(SH|SZ|BJ)$")
POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix="advisory-display-close")
CAPACITY = BoundedSemaphore(2)


def positive(value):
    try:
        value = float(value) if not isinstance(value, bool) else float("nan")
        return value if math.isfinite(value) and value > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def symbols_from_query(raw):
    values = raw.split(",")
    if not 1 <= len(values) <= 100 or any(not SYMBOL.fullmatch(v) for v in values):
        raise WorkbenchError("INVALID_QUOTE_SYMBOLS", "每次报价需1至100个规范证券代码。", 422)
    return list(dict.fromkeys(values))


def calendar_days(now):
    with ReadRepository().transaction() as repo:
        return repo.calendar(now.date() - timedelta(days=40), now.date())


def session(now, days):
    today = now.date()
    facts = {str(r["cal_date"]): r["is_trading"] for r in days}
    if today.isoformat() not in facts:
        raise ValueError("calendar coverage missing")
    prior = sorted(d for d, opened in facts.items() if opened and d < today.isoformat())
    if not prior:
        raise ValueError("previous session missing")
    opened = facts[today.isoformat()] is True
    if opened and time(9, 30) <= now.time() < time(15):
        return ("PAUSED" if time(11, 30) <= now.time() < time(13) else "LIVE"), today.isoformat()
    return "CLOSE", today.isoformat() if opened and now.time() >= time(15) else prior[-1]


class TdxQuotes:
    def live(self, symbols):
        response = requests.post(TDX_BASE_URL + "/api/batch-quote",
                                 json={"codes": [_to_tdx_code(s) for s in symbols]}, timeout=4)
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0 or not isinstance(payload.get("data"), list):
            raise ValueError("invalid TDX batch")
        result, duplicates = {}, set()
        for item in payload["data"]:
            if not isinstance(item, dict):
                continue
            exchange = item.get("Exchange")
            exchange = {0: "SZ", 1: "SH", 2: "BJ"}.get(exchange) if type(exchange) is int else exchange
            symbol = f"{item.get('Code')}.{exchange}"
            if symbol not in symbols:
                continue
            if symbol in result:
                duplicates.add(symbol)
            k = item.get("K") if isinstance(item.get("K"), dict) else {}
            price = positive(k.get("Close"))
            opening, previous = positive(k.get('Open')), positive(k.get('Last'))
            result[symbol] = {"price": price / 1000 if price else None,
                              'open_price': opening / 1000 if opening else None,
                              'previous_close': previous / 1000 if previous else None,
                              "source_time_raw": str(item.get("ServerTime") or ""),
                              "has_trades": positive(item.get("TotalHand")) is not None}
        for symbol in duplicates:
            result[symbol] = {"price": None, "reason_code": "TDX_DUPLICATE_SYMBOL"}
        return result

    def close(self, symbol, cutoff):
        response = requests.get(TDX_BASE_URL + "/api/kline-all/tdx",
                                params={"code": _to_tdx_code(symbol), "type": "day", "limit": 3}, timeout=4)
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") or {}
        meta = data.get("meta") or {}
        if payload.get("code") != 0 or meta.get("source") != "tdx" or meta.get("type") != "day":
            raise ValueError("wrong raw daily source")
        bars = {}
        for bar in data.get("list", []):
            stamp = datetime.fromisoformat(bar["Time"])
            if stamp.tzinfo is None:
                raise ValueError("undated bar")
            day = stamp.astimezone(CN).date().isoformat()
            price = positive(bar.get("Close"))
            if day > cutoff or price is None:
                continue
            opening = positive(bar.get('Open')) if positive(bar.get('Volume')) else None
            value = (price, opening)
            if day in bars and bars[day] != value:
                raise ValueError("conflicting bar")
            bars[day] = value
        if not bars:
            return {"price": None, "reason_code": "TDX_CLOSE_MISSING"}
        day = max(bars)
        previous = max((d for d in bars if d < day), default=None)
        return {"price": bars[day][0] / 1000, "trade_date": day,
                'open_price': bars[day][1] / 1000 if bars[day][1] else None,
                'previous_close': bars[previous][0] / 1000 if previous else None,
                'previous_close_date': previous,
                "kind": "CLOSE" if day == cutoff else "STALE_CLOSE"}


class QuoteService:
    def __init__(self, reader=None, calendar=None, clock=None, timer=None):
        self.reader = reader or TdxQuotes()
        self.calendar = calendar or calendar_days
        self.clock = clock or (lambda: datetime.now(CN))
        self.timer = timer or monotonic
        self.cache = OrderedDict()
        self.lock = Lock()

    @staticmethod
    def unavailable(symbol, now, reason):
        return dict(symbol=symbol, source="TDX", price_basis="RAW_CNY", price=None,
                    kind="UNAVAILABLE", trade_date=None, observed_at=now.isoformat(), reason_code=reason)

    def get(self, raw):
        symbols = symbols_from_query(raw)
        now = self.clock().astimezone(CN)
        try:
            days = self.calendar(now)
            kind, cutoff = session(now, days)
        except Exception:
            return self.envelope(now, {s: self.unavailable(s, now, "CALENDAR_UNAVAILABLE") for s in symbols})
        key_prefix = (now.date().isoformat(), kind, cutoff)
        found, missing = {}, []
        with self.lock:
            for symbol in symbols:
                key = (*key_prefix, symbol)
                cached = self.cache.get(key)
                if cached and cached[0] > self.timer():
                    found[symbol] = cached[1]
                    self.cache.move_to_end(key)
                else:
                    missing.append(symbol)
        if missing and not CAPACITY.acquire(blocking=False):
            found.update({s: self.unavailable(s, now, "QUOTE_BUSY") for s in missing})
            return self.envelope(now, found)
        if missing:
            try:
                fresh = self._read(missing, kind, cutoff, now)
                with self.lock:
                    for symbol, quote in fresh.items():
                        ttl = 3 if quote["price"] is None else 10 if kind != "CLOSE" else 300
                        # Missing today's closing bar must recover promptly after the close.
                        if quote["kind"] == "STALE_CLOSE":
                            ttl = 15
                        self.cache[(*key_prefix, symbol)] = (self.timer() + ttl, quote)
                    while len(self.cache) > 2048:
                        self.cache.popitem(last=False)
                found.update(fresh)
            finally:
                CAPACITY.release()
        # Normalize prior-close dates against the official calendar, including weekends.
        trading = sorted(str(r['cal_date']) for r in days if r['is_trading'])
        enriched = {}
        for symbol, original in found.items():
            quote = dict(original)
            day = quote.get('trade_date')
            previous = max((d for d in trading if day and d < day), default=None)
            if kind != 'CLOSE' and day:
                quote['previous_close_date'] = previous
            valid_previous = previous and quote.get('previous_close_date') == previous
            prior_price = positive(quote.get('previous_close')) if valid_previous else None
            quote['today_return_bps'] = ((quote['price'] / prior_price - 1) * 10000
                if quote.get('price') and prior_price else None)
            enriched[symbol] = quote
        return self.envelope(now, enriched)

    def _read(self, symbols, kind, cutoff, now):
        result = {s: self.unavailable(s, now, "TDX_UNAVAILABLE") for s in symbols}
        if kind != "CLOSE":
            for offset in range(0, len(symbols), 50):
                chunk = symbols[offset:offset + 50]
                try:
                    rows = self.reader.live(chunk)
                except Exception:
                    continue
                for symbol in chunk:
                    row = rows.get(symbol) or {}
                    if positive(row.get("price")):
                        result[symbol].update(row, kind=kind if row.get("has_trades") else "NO_TRADES",
                                              trade_date=cutoff if row.get("has_trades") else None, reason_code=None,
                                              date_basis="CALENDAR_SESSION", source_timestamp=None)
                    else:
                        result[symbol]["reason_code"] = row.get("reason_code") or "TDX_PRICE_MISSING"
        else:
            futures = {POOL.submit(self.reader.close, symbol, cutoff): symbol for symbol in symbols}
            done, pending = wait(futures, timeout=8)
            for future in pending:
                future.cancel()
                result[futures[future]]["reason_code"] = "TDX_TIMEOUT"
            for future in done:
                symbol = futures[future]
                try:
                    row = future.result()
                    if positive(row.get("price")):
                        result[symbol].update(row, reason_code=None, date_basis="TDX_DAILY_BAR")
                    else:
                        result[symbol]["reason_code"] = row.get("reason_code") or "TDX_CLOSE_MISSING"
                except Exception:
                    result[symbol]["reason_code"] = "TDX_UNAVAILABLE"
        return result

    @staticmethod
    def envelope(now, items):
        return {"schema_version": "advisory_display_quotes_v1", "observed_at": now.isoformat(),
                "items": items, "source": "TDX", "display_only": True,
                "status": "READY" if all(q["price"] is not None for q in items.values()) else "PARTIAL"}


quote_service = QuoteService()
