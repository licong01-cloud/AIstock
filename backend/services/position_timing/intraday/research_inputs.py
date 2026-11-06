"""Read-only, producer-pinned Qlib inputs for minute timing research."""

from __future__ import annotations

import json
from collections import OrderedDict
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
import sys

import numpy as np
import pandas as pd

from backend.data_service.frozen_dataset import content_pin, raw_price_li
from backend.services.selection_center.suspension_provider import project_selection_suspension_days

from .contracts import CHINA, Calendar, DayFacts, MarketView, MinuteBar, digest, sessions
from .inputs import file_sha256
from .replay import FileRef, PlanEvent
from .store import IntradayError


def checked_file(ref: FileRef) -> Path:
    path = Path(ref.path)
    if not path.is_absolute() or ref.path.startswith(("\\\\", "//")) or not path.is_file():
        raise IntradayError("RESEARCH_SOURCE_PATH_INVALID", 422)
    if file_sha256(path) != ref.sha256:
        raise IntradayError("RESEARCH_SOURCE_IDENTITY_MISMATCH", 422)
    return path


class CandidateReader:
    """A bounded reader; no active profile, SQL, downloader or consumer impersonation."""

    MAX_WINDOW_DAYS = 20
    MAX_WINDOW_SYMBOLS = 1024
    MAX_ARRAY_BYTES = 512 * 1024**2
    MAX_INDEX_BYTES = 64 * 1024**2
    DAILY_FIELDS = ("prev_close", "up_limit_price", "down_limit_price")
    MINUTE_FIELDS = ("open", "high", "low", "close", "volume", "amount", "factor")

    def __init__(self, manifest: FileRef):
        path = checked_file(manifest)
        self.root = path.parent
        self.identity = manifest.sha256
        self.manifest = json.loads(path.read_text(encoding="utf-8"))
        if self.manifest.get("schema_version") != "qe_dataset_manifest_v1":
            raise IntradayError("RESEARCH_CANDIDATE_SCHEMA_INVALID", 422)
        self.components = {}
        for entry in self.manifest["components"].values():
            relative = entry["path"]
            if relative in self.components and self.components[relative] != entry:
                raise IntradayError("RESEARCH_COMPONENT_IDENTITY_CONFLICT", 422)
            self.components[relative] = entry
        self.pins = {}
        self.calendars = {}
        self._closed = False
        self._window = None
        self._arrays = OrderedDict()
        self._indexes = OrderedDict()
        self._array_bytes = self._index_bytes = 0
        self._cache_verified = set()
        self._stats = {"cache_hits": 0, "cache_misses": 0, "evictions": 0,
                       "prefetch_unavailable": 0, "open_mappings": 0, "peak_open_mappings": 0}

    def _open(self):
        if self._closed:
            raise IntradayError("RESEARCH_READER_CLOSED", 422)

    @property
    def window_status(self):
        """Owned cache payload budgets, not a promise about caller/Arrow RSS."""
        return {"window": self._window, "closed": self._closed, **self._stats,
                "array_bytes": self._array_bytes, "index_bytes": self._index_bytes,
                "array_limit_bytes": self.MAX_ARRAY_BYTES, "index_limit_bytes": self.MAX_INDEX_BYTES,
                "max_window_days": self.MAX_WINDOW_DAYS, "max_window_symbols": self.MAX_WINDOW_SYMBOLS,
                "retained_file_handles": 0}

    def release_window(self):
        """Discard prefetched payloads; keep consumed pins and reusable date/span indexes."""
        self._arrays.clear()
        self._array_bytes = 0
        for key in list(self._indexes):
            if key == "suspension":
                _, size = self._indexes.pop(key)
                self._index_bytes -= size
        self._window = None
        self._cache_verified.clear()

    def close(self):
        """Idempotent, no I/O; consumers must verify_pins before publishing results."""
        self.release_window()
        self._indexes.clear()
        self._index_bytes = 0
        self.calendars.clear()
        self._closed = True

    def _index_put(self, key, value, size):
        if size > self.MAX_INDEX_BYTES:
            return
        previous = self._indexes.pop(key, None)
        if previous is not None:
            self._index_bytes -= previous[1]
        while self._index_bytes + size > self.MAX_INDEX_BYTES:
            removed_key, (_, removed) = self._indexes.popitem(last=False)
            if isinstance(removed_key, tuple) and removed_key[0] == "calendar":
                self.calendars.pop(removed_key[1], None)
            self._index_bytes -= removed
            self._stats["evictions"] += 1
        self._indexes[key] = (value, size)
        self._index_bytes += size
        return True

    def _index_get(self, key):
        item = self._indexes.get(key)
        if item is not None:
            self._indexes.move_to_end(key)
            return item[0]

    def _day_range(self, frequency, day):
        calendar = self.calendar(frequency)
        key = ("dates", frequency)
        ranges = self._index_get(key)
        if ranges is None:
            days = calendar.values.astype("datetime64[D]")
            edges = np.r_[0, np.flatnonzero(days[1:] != days[:-1]) + 1, len(days)]
            ranges = {pd.Timestamp(days[left]): (int(left), int(right)) for left, right in zip(edges[:-1], edges[1:])}
            size = sys.getsizeof(ranges) + sum(sys.getsizeof(k) + sys.getsizeof(v) + 2 * sys.getsizeof(0) for k, v in ranges.items())
            self._index_put(key, ranges, size)
        result = ranges.get(pd.Timestamp(day))
        if result is None:
            raise IntradayError("RESEARCH_DATE_OUTSIDE_CALENDAR", 422)
        return result

    def _mapped_slice(self, path, frequency, left, right, *, partial=False):
        if path.stat().st_size < 8 or path.stat().st_size % 4:
            raise IntradayError("RESEARCH_BIN_HEADER_INVALID", 422)
        values = np.memmap(path, dtype="<f4", mode="r")
        self._stats["open_mappings"] += 1
        self._stats["peak_open_mappings"] = max(self._stats["peak_open_mappings"], self._stats["open_mappings"])
        try:
            header = float(values[0])
            if not np.isfinite(header) or header < 0 or not header.is_integer():
                raise IntradayError("RESEARCH_BIN_HEADER_INVALID", 422)
            first, last = int(header), int(header) + len(values) - 1
            if last > len(self.calendar(frequency)):
                raise IntradayError("RESEARCH_BIN_OUTSIDE_CALENDAR", 422)
            if not partial and (left < first or right > last):
                raise IntradayError("RESEARCH_BIN_HISTORY_INCOMPLETE", 422)
            left, right = max(left, first), min(right, last)
            # Immutable bytes own the slice: callers cannot re-enable writes or
            # retain a live mmap after release/close. No full-file float copy.
            raw = np.asarray(values[left - first + 1:right - first + 1], dtype=float).tobytes() if left < right else b""
            return left, right, np.frombuffer(raw, dtype=float)
        finally:
            values._mmap.close()
            self._stats["open_mappings"] -= 1

    def _cached_source(self, relative):
        already_pinned = relative in self.pins
        path = self.file(relative)
        if already_pinned and relative not in self._cache_verified:
            if file_sha256(path) != self.pins[relative]["sha256"]:
                raise IntradayError("RESEARCH_SOURCE_CHANGED_DURING_READ", 422)
        self._cache_verified.add(relative)
        return path

    def prepare_window(self, start: date, end: date, symbols):
        """Replace a bounded physical prefetch; never select stocks or assert availability.

        Optional missing/corrupt inputs are not consumed or pinned here. The
        original necessary-input checks run on actual values/market calls.
        """
        self._open()
        if type(start) is not date or type(end) is not date or start > end:
            raise IntradayError("RESEARCH_WINDOW_INVALID", 422)
        codes = set()
        for symbol in symbols:
            if not isinstance(symbol, str) or not symbol:
                raise IntradayError("RESEARCH_WINDOW_INVALID", 422)
            codes.add(symbol)
            if len(codes) > self.MAX_WINDOW_SYMBOLS:
                raise IntradayError("RESEARCH_WINDOW_BUDGET_EXCEEDED", 422)
        consumed = set(self.pins)
        calendar = self.calendar("day")
        days = calendar[(calendar >= pd.Timestamp(start)) & (calendar <= pd.Timestamp(end))]
        if not len(days) or len(days) > self.MAX_WINDOW_DAYS:
            raise IntradayError("RESEARCH_WINDOW_BUDGET_EXCEEDED", 422)
        self.release_window()
        self._window = (start.isoformat(), end.isoformat(), tuple(sorted(codes)))
        for frequency, fields, group in (("day", self.DAILY_FIELDS, "daily"), ("1min", self.MINUTE_FIELDS, "minute")):
            try:
                cal = self.calendar(frequency)
                left = int(cal.searchsorted(pd.Timestamp(start)))
                right = int(cal.searchsorted(pd.Timestamp(end) + pd.Timedelta(days=1)))
            except (ValueError, IntradayError, OSError):
                self._stats["prefetch_unavailable"] += 1
                continue
            if (right - left) * np.dtype(float).itemsize > self.MAX_ARRAY_BYTES:
                self._stats["prefetch_unavailable"] += len(codes) * len(fields)
                continue
            for symbol in sorted(codes):
                for field in fields:
                    relative = f"components/{group}_bin_candidate/features/{symbol.lower()}/{field}.{frequency}.bin"
                    try:
                        entry = self.components.get(relative)
                        if entry is None:
                            self._stats["prefetch_unavailable"] += 1
                            continue
                        pin = content_pin(self.root, relative, expected=entry["sha256"])
                        if pin["size"] != entry["size"]:
                            raise IntradayError("RESEARCH_COMPONENT_SIZE_MISMATCH", 422)
                        item = self._mapped_slice(self.root / relative, frequency, left, right, partial=True)
                        if file_sha256(self.root / relative) != pin["sha256"]:
                            raise IntradayError("RESEARCH_SOURCE_CHANGED_DURING_READ", 422)
                        size = item[2].nbytes
                        if size > self.MAX_ARRAY_BYTES:
                            continue
                        while self._array_bytes + size > self.MAX_ARRAY_BYTES:
                            _, removed = self._arrays.popitem(last=False)
                            self._array_bytes -= removed[2].nbytes
                            self._stats["evictions"] += 1
                        self._arrays[relative] = item
                        self._array_bytes += size
                    except (ValueError, IntradayError, OSError):
                        self._stats["prefetch_unavailable"] += 1
        relative = "components/suspend_d_daily_candidate_v2/suspend_d.parquet"
        try:
            entry = self.components[relative]
            pin = content_pin(self.root, relative, expected=entry["sha256"])
            if pin["size"] != entry["size"]:
                raise IntradayError("RESEARCH_COMPONENT_SIZE_MISMATCH", 422)
            frame = pd.read_parquet(self.root / relative, filters=[("ts_code", "in", sorted(codes)),
                                    ("trade_date", ">=", pd.Timestamp(start)), ("trade_date", "<=", pd.Timestamp(end))])
            if file_sha256(self.root / relative) != pin["sha256"]:
                raise IntradayError("RESEARCH_SOURCE_CHANGED_DURING_READ", 422)
            self._index_put("suspension", frame, int(frame.memory_usage(deep=True).sum()))
        except (KeyError, ValueError, IntradayError, OSError):
            self._stats["prefetch_unavailable"] += 1
        # Calendars read for prefetch are not yet consumed by a market/values
        # request. Actual access below re-pins them before using cached indexes.
        for relative in set(self.pins) - consumed:
            self.pins.pop(relative)
        self._cache_verified.clear()
        return self.window_status

    def file(self, relative: str) -> Path:
        self._open()
        entry = self.components.get(relative)
        if entry is None:
            raise IntradayError("RESEARCH_COMPONENT_NOT_IN_MANIFEST", 422)
        if relative not in self.pins:
            pin = content_pin(self.root, relative, expected=entry["sha256"])
            if pin["size"] != entry["size"]:
                raise IntradayError("RESEARCH_COMPONENT_SIZE_MISMATCH", 422)
            self.pins[relative] = pin
        return self.root / relative

    def calendar(self, frequency: str) -> pd.DatetimeIndex:
        self._open()
        group = "minute" if frequency == "1min" else "daily"
        relative = f"components/{group}_bin_candidate/calendars/{frequency}.txt"
        path = self._cached_source(relative)
        key = ("calendar", frequency)
        values = self._index_get(key)
        if values is None:
            values = pd.DatetimeIndex(pd.to_datetime(path.read_text(encoding="utf-8-sig").splitlines()))
            if values.empty or values.has_duplicates or not values.is_monotonic_increasing or values.tz is not None:
                raise IntradayError("RESEARCH_CALENDAR_INVALID", 422)
            values = pd.DatetimeIndex(np.frombuffer(values.asi8.tobytes(), dtype="datetime64[ns]"))
            if self._index_put(key, values, sys.getsizeof(values) + values.nbytes):
                self.calendars[frequency] = values
        return values

    def values(self, symbol: str, day, field: str, frequency: str):
        calendar = self.calendar(frequency)
        left, right = self._day_range(frequency, day)
        group = "minute" if frequency == "1min" else "daily"
        relative = f"components/{group}_bin_candidate/features/{symbol.lower()}/{field}.{frequency}.bin"
        cached = self._arrays.get(relative)
        if cached is not None and cached[0] <= left and right <= cached[1]:
            self._cached_source(relative)
            self._arrays.move_to_end(relative)
            self._stats["cache_hits"] += 1
            return calendar[left:right], cached[2][left - cached[0]:right - cached[0]]
        self._stats["cache_misses"] += 1
        path = self.file(relative)
        return calendar[left:right], self._mapped_slice(path, frequency, left, right)[2]

    def market(self, symbol: str, day) -> MarketView:
        daily_calendar = self.calendar("day")
        if pd.Timestamp(day) not in daily_calendar:
            raise IntradayError("RESEARCH_PLAN_NON_TRADING_DAY", 422)
        # This is a physical quote span check, never a PIT selection rule.
        relative = "components/minute_bin_candidate/instruments/all.txt"
        path = self._cached_source(relative)
        spans = self._index_get("spans")
        if spans is None:
            rows = [line.split("\t") for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
            if any(len(row) != 3 for row in rows):
                raise IntradayError("RESEARCH_QUOTE_SPAN_INVALID", 422)
            spans = {}
            for code, first, last in rows:
                spans.setdefault(code.upper(), []).append((first[:10], last[:10]))
            size = sys.getsizeof(spans) + sum(sys.getsizeof(k) + sys.getsizeof(v) + sum(
                sys.getsizeof(p) + sum(sys.getsizeof(x) for x in p) for p in v) for k, v in spans.items())
            self._index_put("spans", spans, size)
        if not any(first <= str(day) <= last for first, last in spans.get(symbol, ())):
            raise IntradayError("RESEARCH_QUOTE_SPAN_MISSING", 422)
        suspension = self._cached_source("components/suspend_d_daily_candidate_v2/suspend_d.parquet")
        frame = self._index_get("suspension")
        if frame is not None and self._window[0] <= str(day) <= self._window[1] and symbol in self._window[2]:
            frame = frame.loc[(frame["ts_code"] == symbol) & (frame["trade_date"] == pd.Timestamp(day))]
        else:
            frame = pd.read_parquet(suspension, filters=[("ts_code", "==", symbol), ("trade_date", "==", pd.Timestamp(day))])
        rows = [{key: (None if pd.isna(value) else value) for key, value in row.items()}
                for row in frame.to_dict("records")]
        projected, suspend_receipt = project_selection_suspension_days(
            rows, symbols=[symbol], required_dates=[day], usage="D_EFFECTIVE_MARKET_STATE")
        if any(row["state"] == "UNRESOLVED" for row in projected):
            raise IntradayError("RESEARCH_SUSPENSION_UNRESOLVED", 422)
        suspended = any(row["state"] in {"FULL_DAY_SUSPENDED", "INTRADAY_SUSPENDED"} for row in projected)
        available = datetime.combine(day, time(9), CHINA)
        calendar = Calendar(trade_date=day, is_trading_day=True, source_ref="candidate.day_calendar",
                            source_sha256=digest(self.pins["components/daily_bin_candidate/calendars/day.txt"]),
                            available_at=available)
        prices = {}
        if not suspended:
            for name in ("prev_close", "up_limit_price", "down_limit_price"):
                _, values = self.values(symbol, day, name, "day")
                value = raw_price_li(values[0], Decimal(1))
                if value is None or value <= 0:
                    raise IntradayError("RESEARCH_DAILY_PRICE_UNAVAILABLE", 422)
                prices[name] = value / 1000
            if prices["down_limit_price"] >= prices["up_limit_price"]:
                raise IntradayError("RESEARCH_DAILY_LIMIT_ORDER_INVALID", 422)
        facts = DayFacts(symbol=symbol, trade_date=day, is_suspended=suspended,
                         has_daily_limit=None if suspended else True, reference_price=prices.get("prev_close"),
                         limit_up=prices.get("up_limit_price"), limit_down=prices.get("down_limit_price"),
                         available_at=available, source_ref="candidate.raw_limits_and_suspend_d",
                         source_sha256=digest({"prices": prices, "suspension": suspend_receipt}))
        bars = []
        if not suspended:
            fields = ("open", "high", "low", "close", "volume", "amount", "factor")
            arrays = {}
            for field in fields:
                times, arrays[field] = self.values(symbol, day, field, "1min")
            for index, stamp in enumerate(times):
                end = stamp.to_pydatetime().replace(tzinfo=CHINA)
                start = end - timedelta(minutes=1)
                if not any(left <= start < end <= right for left, right in sessions(day)):
                    continue
                if not all(np.isfinite(arrays[field][index]) for field in fields):
                    raise IntradayError("RESEARCH_MINUTE_VALUES_INCOMPLETE", 422)
                factor = Decimal(str(arrays["factor"][index]))
                values = {name: raw_price_li(arrays[name][index], factor) / 1000 for name in fields[:4]}
                bars.append(MinuteBar(start=start, end=end, received_at=end + timedelta(seconds=5),
                                      volume_shares=Decimal(str(arrays["volume"][index])) * factor,
                                      amount_cny=str(arrays["amount"][index]), **values))
        return MarketView(symbol=symbol, trade_date=day, source_ref="candidate:" + self.identity,
                          source_version="qlib_raw_cny_shares_v1", calendar=calendar, day_facts=facts,
                          bars=tuple(bars), visibility_basis="ASSUMED_VISIBILITY")

    def verify_pins(self):
        self._open()
        try:
            for relative, pin in self.pins.items():
                if file_sha256(self.root / relative) != pin["sha256"]:
                    raise IntradayError("RESEARCH_SOURCE_CHANGED_DURING_READ", 422)
        finally:
            self._cache_verified.clear()


def load_markets(manifest: FileRef, plans: list[PlanEvent]) -> tuple[list[MarketView], dict]:
    reader = CandidateReader(manifest)
    keys = sorted({(event.plan.symbol, event.plan.target_trade_date) for event in plans})
    markets = [reader.market(symbol, day) for symbol, day in keys]
    reader.verify_pins()
    return markets, {"candidate_sha256": manifest.sha256, "components": reader.pins,
                     "daily_publication_time_proven": False, "minute_visibility": "ASSUMED_END_PLUS_5_SECONDS"}
