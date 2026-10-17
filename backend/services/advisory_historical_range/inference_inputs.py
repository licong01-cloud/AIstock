"""Consumer-owned read-only source partitions and cutoff-specific views.

This module does not change data-service globals. Its adapters invoke the
existing static builder with invocation-local readers, not a second factor
builder. Processed/qfq views are never sliced from a later decision date.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import update_wrapper
import hashlib
import json
from threading import RLock
from types import FunctionType
from typing import Any

import pandas as pd


def frame_identity(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update(json.dumps([list(frame.index.names), list(map(str, frame.columns)),
                              list(map(str, frame.dtypes))], ensure_ascii=False).encode())
    digest.update(pd.util.hash_pandas_object(frame, index=True).values.tobytes())
    return digest.hexdigest()


def bind_readers(function: Callable, readers: Mapping[str, Any]) -> Callable:
    """Execute the SAME builder code with private dependencies; no monkey patch.

    Intended only for the existing pure static orchestration and raw readers.
    A public signature change is visible rather than silently substituted.
    """
    bound = FunctionType(function.__code__, {**function.__globals__, **readers},
                         function.__name__, function.__defaults__, function.__closure__)
    bound.__kwdefaults__ = function.__kwdefaults__
    return update_wrapper(bound, function)


class _NoSharedCache:
    def get(self, *args: Any, **kwargs: Any) -> None:
        return None

    def set(self, *args: Any, **kwargs: Any) -> None:
        return None


@dataclass
class _Partition:
    frame: pd.DataFrame
    size: int
    bounds: dict[str, tuple[date, date]]


class InferenceInputSession:
    """Bounded source cache, never shared between consumers or runs.

    The caller supplies a source/revision identity, not an approval token. A
    changed identity naturally reloads sources. Revisions without a trustworthy
    frozen identity use a fresh session/full read, not stale persistent cache.
    """

    def __init__(self, *, consumer_scope: str, run_id: str, source_identity: str,
                 max_bytes: int = 2 * 1024**3, max_entries: int = 16) -> None:
        if consumer_scope not in {"ADVISORY", "LOCALSIM"} or not run_id or not source_identity:
            raise ValueError("input session requires explicit consumer/run/source identity")
        if max_bytes < 0 or max_entries < 1:
            raise ValueError("invalid input retention budget")
        self.consumer_scope, self.run_id = consumer_scope, run_id
        self.source_identity = source_identity
        self.max_bytes, self.max_entries = max_bytes, max_entries
        self._entries: OrderedDict[tuple, _Partition] = OrderedDict()
        self._lock = RLock()
        self.retained_bytes = self.reads = self.hits = self.append_reads = self.rows_read = 0
        self.price_reader: Callable | None = None
        self.static_builder: Callable | None = None

    def read(self, *, dataset: str, members: list[str], start: date, end: date,
             loader: Callable[[list[str], date, date], pd.DataFrame]) -> pd.DataFrame:
        if start > end or len(members) != len(set(members)):
            raise ValueError("invalid source interval or duplicate members")
        if not members:
            return pd.DataFrame(index=pd.MultiIndex.from_arrays([pd.DatetimeIndex([]), []], names=["datetime", "instrument"]))
        key = (self.source_identity, dataset)
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None and all(member in entry.bounds and start >= entry.bounds[member][0]
                                         and end <= entry.bounds[member][1] for member in members):
                self.hits += 1
                self._entries.move_to_end(key)
                return self._slice(entry.frame, members, start, end)
            # One pointwise RAW partition per dataset, with per-member interval
            # coverage. Different pools don't store duplicate full histories;
            # no processed/qfq/cross-sectional view is cached this way.
            bounds = dict(entry.bounds) if entry is not None else {}
            intervals: dict[tuple[date, date], list[str]] = {}
            tail_intervals: set[tuple[date, date]] = set()
            reset_members: set[str] = set()
            reused = False
            for member in members:
                previous = bounds.get(member)
                if previous is None:
                    intervals.setdefault((start, end), []).append(member)
                    bounds[member] = (start, end)
                    continue
                first, last = previous
                if end < first or start > last:
                    # A disjoint/reversed request must not claim the unread
                    # gap as covered, nor read beyond this request's cutoff.
                    intervals.setdefault((start, end), []).append(member)
                    bounds[member] = (start, end)
                    reset_members.add(member)
                    continue
                if start <= last and end >= first:
                    reused = True
                if start < first:
                    intervals.setdefault((start, min(end, first - timedelta(days=1))), []).append(member)
                if end > last:
                    tail = (last + timedelta(days=1), end)
                    intervals.setdefault(tail, []).append(member)
                    tail_intervals.add(tail)
                bounds[member] = (min(start, first), max(end, last))
            if reused:
                self.hits += 1
            pieces = ([entry.frame.loc[~entry.frame.index.get_level_values("instrument").isin(reset_members)]]
                      if entry is not None else [])
            for (first, last), codes in intervals.items():
                if first > last:
                    continue
                raw = loader(codes, first, last)
                if isinstance(raw, pd.DataFrame) and raw.empty and not isinstance(raw.index, pd.MultiIndex):
                    raw = raw.copy()
                    raw.index = pd.MultiIndex.from_arrays([pd.DatetimeIndex([]), []], names=["datetime", "instrument"])
                self._validate(raw, codes, first, last)
                self.reads += 1
                self.rows_read += len(raw)
                pieces.append(raw)
                if (first, last) in tail_intervals:
                    self.append_reads += 1
            if not pieces:
                return pd.DataFrame(index=pd.MultiIndex.from_arrays([pd.DatetimeIndex([]), []], names=["datetime", "instrument"]))
            fresh = (pd.concat(pieces) if len(pieces) > 1 else pieces[0]).sort_index()
            if not fresh.index.is_unique:
                raise ValueError("source partitions returned duplicate keys")
            size = int(fresh.memory_usage(index=True, deep=True).sum())
            old = self._entries.pop(key, None)
            if old is not None:
                self.retained_bytes -= old.size
            if size <= self.max_bytes:
                while self._entries and (self.retained_bytes + size > self.max_bytes
                                         or len(self._entries) >= self.max_entries):
                    _, removed = self._entries.popitem(last=False)
                    self.retained_bytes -= removed.size
                # Internal frame and returned caller frame have different owners.
                self._entries[key] = _Partition(fresh, size, bounds)
                self.retained_bytes += size
            return self._slice(fresh, members, start, end)

    @staticmethod
    def _validate(frame: pd.DataFrame, members: list[str], start: date, end: date) -> None:
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("source reader must return a DataFrame")
        if frame.empty:
            return  # An empty raw dataset is not silently fabricated.
        if not isinstance(frame.index, pd.MultiIndex) or frame.index.names != ["datetime", "instrument"]:
            raise ValueError("raw source must retain datetime/instrument keys")
        dates = frame.index.get_level_values("datetime")
        if not isinstance(dates, pd.DatetimeIndex) or dates.tz is not None or not frame.index.is_unique:
            raise ValueError("invalid raw source date/index identity")
        if dates.min().date() < start or dates.max().date() > end:
            raise ValueError("raw reader returned rows outside the requested PIT interval")
        if not set(frame.index.get_level_values("instrument")).issubset(members):
            raise ValueError("raw reader returned an unrequested member")

    @staticmethod
    def _slice(frame: pd.DataFrame, members: list[str], start: date, end: date) -> pd.DataFrame:
        if frame.empty:
            return frame.copy(deep=True)
        days = frame.index.get_level_values("datetime")
        symbols = frame.index.get_level_values("instrument")
        return frame.loc[(days >= pd.Timestamp(start)) & (days <= pd.Timestamp(end))
                         & symbols.isin(members)].copy(deep=True)

    def invalidate(self, *, source_identity: str) -> None:
        if not source_identity:
            raise ValueError("revision identity cannot be empty")
        with self._lock:
            self._entries.clear()
            self.retained_bytes = 0
            self.source_identity = source_identity

    def close(self) -> None:
        with self._lock:
            self._entries.clear()
            self.retained_bytes = 0

    def stats(self) -> dict[str, Any]:
        return {"input_partition_reads": self.reads, "input_partition_hits": self.hits,
                "input_append_reads": self.append_reads, "input_rows_read": self.rows_read,
                "input_retained_bytes": self.retained_bytes}


def _no_shared_legacy_cache(reader: Callable) -> Callable:
    return bind_readers(reader, {"_CACHE": _NoSharedCache()})


def database_input_session(*, consumer_scope: str, run_id: str, source_identity: str,
                           max_bytes: int) -> InferenceInputSession:
    """Advisory-side adapter to existing read-only daily DB contracts.

    Imports are lazy so pure contracts remain usable on the Python 3.10 node.
    Raw sources use private caches, even when their public legacy loader has a
    process-global cache. No source/DB/profile writes or network fallback.
    """
    from backend.data_service import api, qe_data_service as static
    from backend.data_service.adj_factor_provider import AdjFactorProvider

    session = InferenceInputSession(consumer_scope=consumer_scope, run_id=run_id,
                                    source_identity=source_identity, max_bytes=max_bytes)
    adjustment = AdjFactorProvider(use_tushare_fallback=False)

    def raw_price(members: list[str], start: date, end: date) -> pd.DataFrame:
        return session.read(dataset="market.kline_daily_raw", members=members, start=start, end=end,
                            loader=lambda codes, first, last: api.get_history_window(
                                codes, start=datetime.combine(first, datetime.min.time()),
                                end=datetime.combine(last, datetime.min.time()),
                                fields=["open", "high", "low", "close", "volume", "amount"],
                                adj="none", nan_policy="none", allow_xtquant_fallback=False,
                                allow_tushare_adj_fallback=False))

    def factor_view(members: list[str], start: date, end: date) -> pd.Series:
        def load(codes: list[str], first: date, last: date) -> pd.DataFrame:
            frame = adjustment.get_adj_factor(codes, first, last)
            if frame.empty:
                return pd.DataFrame(index=pd.MultiIndex.from_arrays([pd.DatetimeIndex([]), []],
                                                                   names=["datetime", "instrument"]))
            frame = frame.assign(datetime=pd.to_datetime(frame.trade_date), instrument=frame.ts_code)
            return frame.set_index(["datetime", "instrument"])[["adj_factor"]].sort_index()
        factors = session.read(dataset="market.adj_factor", members=members, start=start, end=end, loader=load)
        if factors.empty:
            raise ValueError("database adjustment factors are empty; no default factor or remote fallback")
        factors = factors.reset_index().rename(columns={"datetime": "trade_date", "instrument": "ts_code"})
        factors = adjustment.calculate_qfq_factor(factors)
        factors["datetime"] = pd.to_datetime(factors.trade_date)
        return factors.set_index(["datetime", "ts_code"])["qfq_factor"].rename_axis(["datetime", "instrument"])

    def price_view(members: list[str], start: date, end: date, *, qlib_adjusted: bool = False) -> pd.DataFrame:
        price = raw_price(members, start, end)
        factors = factor_view(members, start, end)
        price["factor"] = factors.reindex(price.index)
        if qlib_adjusted:
            # The legacy static PV reader normalizes through TODAY even for a
            # historical call. This adapter uses the same declared qfq formula,
            # but only legal <= C factors; no future buffer reaches the builder.
            price = price.sort_index()
            price["factor"] = price["factor"].groupby(level="instrument").transform(lambda x: x.ffill().bfill())
            if price["factor"].isna().any():
                raise ValueError("an instrument has no legal adjustment history")
            for column in ("open", "high", "low", "close"):
                price[column] = price[column].astype("float64") * price["factor"]
            price["volume"] = price["volume"].astype("float64") / price["factor"]
        else:
            # Preserve get_history_window's existing factor fill semantics;
            # this is not a claim of individual missing-factor completeness.
            price["factor"] = price["factor"].fillna(1.0)
            price = api._apply_nan_policy(price, "dataservice_default")
        return price

    readers: dict[str, Callable] = {}
    for name in ("load_daily_basic", "load_moneyflow", "load_bak_basic", "load_cyq_perf",
                 "load_sector_data", "load_margin_detail"):
        reader = _no_shared_legacy_cache(getattr(static, name))
        def read(members: list[str], start: date, end: date, *, _name=name, _reader=reader) -> pd.DataFrame:
            return session.read(dataset=_name, members=members, start=start, end=end, loader=_reader)
        readers[name] = read
    readers["load_daily_pv"] = lambda members, start, end: price_view(members, start, end, qlib_adjusted=True)
    session.price_reader = price_view
    session.static_builder = bind_readers(static.build_static_factors, readers)
    return session
