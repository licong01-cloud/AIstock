"""Read-only, frozen-file market adapter for Advisory historical settlement.

No DB fallback, exports, dataset activation, asset copies or selection logic.
The financial/path builder is shared with the legacy R4 source adapter.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from collections import OrderedDict
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from threading import RLock
from typing import Any

import numpy as np
import pandas as pd

from backend.data_service.frozen_dataset import FrozenDatasetError, raw_price_li, read_h5_window

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.outcome_source import (
    HistoricalRangeOutcomeSourceError,
    HistoricalRangeOutcomeSourceProvider,
)

FILE_SOURCE_VERSION = "advisory_outcome_frozen_files_v1"
_REQUIRED = (
    "day_calendar", "day_meta_export", "day_selection_universe", "factor_meta",
    "suspend_data", "sector_membership_spans", "sector_code_map",
)


def _error(message: str) -> HistoricalRangeOutcomeSourceError:
    return HistoricalRangeOutcomeSourceError("ADVISORY_HR_OUTCOME_FILE_SOURCE_INVALID", message)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class OutcomeFileVerificationSession:
    """Bounded process-local proofs for immutable published files, not path-only trust.

    Recheck fingerprints on every use; a changed file is not silently rebound.
    New processes start empty and perform complete content verification again.
    """

    def __init__(self, *, max_entries: int = 512):
        if max_entries < 1:
            raise ValueError("verification cache bound must be positive")
        self._entries = OrderedDict()
        self._max_entries = max_entries
        self._lock = RLock()

    @staticmethod
    def stamp(path):
        stat = path.stat()
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns

    def digest(self, path: Path, *, expected: str | None = None) -> str:
        path = path.resolve(strict=True)
        with self._lock:
            before = self.stamp(path)
            cached = self._entries.get(path)
            if cached is not None:
                if cached[0] != before:
                    raise _error("sealed file changed after content verification")
                digest = cached[1]
                self._entries.move_to_end(path)
            else:
                digest = _sha256(path)
                if self.stamp(path) != before:
                    raise _error("sealed file changed during content verification")
                self._entries[path] = (before, digest)
                while len(self._entries) > self._max_entries:
                    self._entries.popitem(last=False)
            if expected is not None and digest != expected:
                raise _error("frozen file hash differs")
            return digest


def _inside(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root):
        raise _error("file source component escapes its frozen root")
    return path


def freeze_outcome_file_binding(
    profile_path: Path | None = None, *, verification_session: OutcomeFileVerificationSession | None = None,
) -> dict[str, Any]:
    """Freeze the resolved profile once, before registering a new command.

    The profile pins the manifest, not necessarily daily_pv.h5 itself. The latter
    is explicitly a consumer-observed content pin, not a fabricated producer pin.
    """
    if profile_path is None:
        configured = os.getenv("AISTOCK_ACTIVE_DATASET_PROFILE_PATH")
        if not configured:
            raise _error("active dataset profile path is not configured")
        profile_path = Path(configured)
    profile = json.loads(profile_path.read_text(encoding="utf-8-sig"))
    if profile.get("schema_version") != "aistock_active_dataset_profile_v3":
        raise _error("unsupported active dataset profile schema")
    root = Path(profile["controller_paths"]["candidate_root"]).resolve()
    manifest_path = root / "qe_dataset_manifest.json"
    digest = verification_session.digest if verification_session is not None else _sha256
    if digest(manifest_path) != profile["components"]["dataset_manifest_file_sha256"]:
        raise _error("dataset manifest hash differs from frozen profile")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    if manifest.get("release_id") != profile["release_id"]:
        raise _error("dataset manifest release differs from profile")
    pins = {key: dict(manifest["components"][key]) for key in _REQUIRED}
    # This file is part of the existing factor component, not a new export.
    relative = str(Path(pins["factor_meta"]["path"]).parent / "daily_pv.h5").replace("\\", "/")
    pins["daily_pv"] = {
        "path": relative, "sha256": digest(_inside(root, relative)),
        "pin_authority": "CONSUMER_OBSERVED_CONTENT",
    }
    if "index_daily" in manifest["components"]:
        pins["index_daily"] = dict(manifest["components"]["index_daily"])
    body = {"schema_version": FILE_SOURCE_VERSION, "profile": profile, "files": pins}
    return {**body, "binding_hash": canonical_json_sha256(body)}


class FileHistoricalRangeOutcomeSourceProvider(HistoricalRangeOutcomeSourceProvider):
    """One immutable market window shared by all symbols, horizons and projections."""

    def __init__(self, *, binding: dict[str, Any], start_trade_date: date, label_as_of_trade_date: date,
                 verification_session: OutcomeFileVerificationSession | None = None,
                 context_only: bool = False) -> None:
        super().__init__()
        self._verification_session = verification_session
        self._operation_row_cache = OrderedDict()
        self._revision_cache = OrderedDict()
        body = {key: value for key, value in binding.items() if key != "binding_hash"}
        if body.get("schema_version") != FILE_SOURCE_VERSION or canonical_json_sha256(body) != binding.get("binding_hash"):
            raise _error("frozen file binding hash differs")
        self.binding_hash = binding["binding_hash"]
        self._source_revision_context = {
            "adapter": FILE_SOURCE_VERSION, "binding_hash": self.binding_hash,
            "industry_schema": "SW_L2_FILE_V1",
        }
        self._root = Path(body["profile"]["controller_paths"]["candidate_root"]).resolve()
        self._files = body["files"]
        self._stamps: dict[Path, tuple[int, ...]] = {}
        self._start, self._end = start_trade_date, label_as_of_trade_date
        self._context_only = context_only
        self.batch_read_count = 0
        if context_only:
            self._paths = {key: self._verify_file(key) for key in ("sector_membership_spans", "sector_code_map")}
            self._load_industry_context()
            self.assert_unchanged()
            return  # Summary needs PIT industry, not OHLC, limits or a price window.
        self._paths = {key: self._verify_file(key) for key in self._files}
        self._calendar = tuple(date.fromisoformat(line.strip()) for line in self._paths["day_calendar"].read_text(encoding="utf-8-sig").splitlines() if line.strip())
        if not self._calendar or tuple(sorted(set(self._calendar))) != self._calendar:
            raise _error("file calendar is empty, duplicated or unordered")
        if start_trade_date < self._calendar[0] or label_as_of_trade_date > self._calendar[-1] or start_trade_date > label_as_of_trade_date:
            raise _error("requested window is outside the frozen file calendar")
        meta = json.loads(self._paths["factor_meta"].read_text(encoding="utf-8-sig"))
        if meta.get("schema_version") != "qe_direct_factor_h5_static_v2":
            raise _error("unsupported daily price/factor file schema")
        self._daily_root = self._paths["day_meta_export"].parent
        self._calendar_positions = {day: index for index, day in enumerate(self._calendar)}
        self._limit_cache: dict[tuple[str, str], np.ndarray] = {}
        suspend = pd.read_parquet(self._paths["suspend_data"])
        full_day = (suspend["suspend_type"] == "S") & (suspend["suspend_timing"].isna() | (suspend["suspend_timing"].astype(str).str.strip() == ""))
        self._suspended = {
            (str(symbol).upper(), pd.Timestamp(day).date())
            for symbol, day in suspend.loc[full_day, ["ts_code", "trade_date"]].itertuples(index=False, name=None)
        }
        self._load_industry_context()
        self._pool = tuple(tuple(line.split()) for line in self._paths["day_selection_universe"].read_text(encoding="utf-8-sig").splitlines() if line.strip())
        self._prices = self._load_batch()
        self.assert_unchanged()

    def _load_industry_context(self):
        self._memberships = pd.read_parquet(self._paths["sector_membership_spans"])
        for field in ("start_date", "end_date"):
            self._memberships[field] = pd.to_datetime(self._memberships[field]).dt.date
        code_map = json.loads(self._paths["sector_code_map"].read_text(encoding="utf-8-sig"))
        self._sector_codes = {int(item["l2_code_id"]): str(item["canonical_l2_code"]) for item in code_map["entries"]}

    def _verify_file(self, key: str) -> Path:
        pin = self._files[key]
        path = _inside(self._root, pin["path"])
        before = self._stamp(path)
        digest = self._verification_session.digest if self._verification_session is not None else _sha256
        if digest(path) != pin["sha256"]:
            raise _error(f"frozen {key} file hash differs")
        if before != self._stamp(path):
            raise _error(f"frozen {key} file changed during verification")
        self._stamps[path] = before
        return path

    @staticmethod
    def _stamp(path: Path) -> tuple[int, ...]:
        stat = path.stat()
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns

    def assert_unchanged(self) -> None:
        for path, stamp in self._stamps.items():
            if self._stamp(path) != stamp:
                raise _error(f"frozen file changed after binding: {path.name}")

    def _load_batch(self) -> dict[str, pd.DataFrame]:
        try:
            frame = read_h5_window(self._paths["daily_pv"], start=self._start, end=self._end,
                                  columns=["open", "high", "low", "close", "factor"])
        except FrozenDatasetError as exc:
            raise _error(str(exc)) from exc
        self.batch_read_count += 1
        if frame.index.has_duplicates:
            raise _error("daily file contains duplicate stock/day rows")
        return {
            str(symbol).upper(): group.droplevel("instrument").sort_index()
            for symbol, group in frame.groupby(level="instrument", sort=False)
        }

    def begin_operation(self, request_hash: str) -> None:
        if self._context_only:
            raise _error("summary-only context cannot execute financial settlement")
        if request_hash != self._active_operation_hash:
            self.assert_unchanged()
        super().begin_operation(request_hash)

    def resolve_source_revision_bundle(self, **kwargs):
        if self._context_only:
            raise _error("summary-only context cannot resolve financial price inputs")
        key = (kwargs["symbol"].upper(), kwargs["start_trade_date"], kwargs["end_trade_date"], kwargs["label_as_of_ts"])
        cached = self._revision_cache.get(key)
        if cached is None:
            cached = super().resolve_source_revision_bundle(**kwargs)
            self._revision_cache[key] = cached
            if len(self._revision_cache) > 512:
                self._revision_cache.popitem(last=False)
        self._revision_cache.move_to_end(key)
        return cached.model_copy(deep=True)

    def load_symbol_path(self, request):
        bundle = self.resolve_source_revision_bundle(symbol=request.symbol,
            start_trade_date=request.start_trade_date, end_trade_date=request.end_trade_date,
            label_as_of_ts=request.source_available_at)
        if (bundle.source_revision_set.source_revision_set_hash != request.expected_source_revision_set_hash or
            bundle.price_source != request.price_source or bundle.adjustment_source != request.adjustment_source or
            bundle.tradability_source != request.tradability_source):
            raise _error("file source revision identity differs from the frozen request")
        return super().load_symbol_path(request)

    def _load_symbol_rows_as_of(self, **kwargs):
        rows = super()._load_symbol_rows_as_of(**kwargs)
        key = (kwargs["symbol"].upper(), kwargs["start_trade_date"], kwargs["label_as_of_trade_date"])
        self._operation_row_cache.move_to_end(key)
        if len(self._operation_row_cache) > 256:
            self._operation_row_cache.popitem(last=False)
        return rows

    def _limit(self, symbol: str, field: str, day: date) -> Decimal | None:
        key = (symbol, field)
        array = self._limit_cache.get(key)
        if array is None:
            path = _inside(self._root, str(self._daily_root.relative_to(self._root) / "features" / symbol.lower() / f"{field}.day.bin"))
            stamp = self._stamp(path)
            array = np.fromfile(path, dtype="<f4")
            if self._stamp(path) != stamp or not len(array) or not math.isfinite(float(array[0])) or float(array[0]) != int(array[0]):
                raise _error("invalid or changing daily limit bin")
            self._stamps[path] = stamp
            self._limit_cache[key] = array
        offset = self._calendar_positions[day] - int(array[0]) + 1
        if offset < 1 or offset >= len(array) or not math.isfinite(float(array[offset])):
            return None
        return Decimal(str(float(array[offset]))).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)

    @staticmethod
    def _raw_li(adjusted: Any, factor: Decimal) -> Decimal | None:
        try:
            return raw_price_li(adjusted, factor)
        except FrozenDatasetError as exc:
            raise _error(str(exc)) from exc

    def _load_symbol_rows(self, *, symbol: str, start_trade_date: date, end_trade_date: date) -> tuple[dict[str, Any], ...]:
        if start_trade_date < self._start or end_trade_date > self._end:
            raise _error("symbol request exceeds frozen batch window")
        frame = self._prices.get(symbol.upper())
        rows = []
        for day in self._trading_dates(start_trade_date, end_trade_date):
            suspended = (symbol.upper(), day) in self._suspended
            raw = None if frame is None or pd.Timestamp(day) not in frame.index else frame.loc[pd.Timestamp(day)]
            if raw is None and not suspended:
                raise _error(f"ordinary tradable stock has missing price: {symbol}/{day}")
            factor = None if raw is None or not math.isfinite(float(raw["factor"])) else Decimal(str(raw["factor"]))
            if factor is not None and factor <= 0:
                raise _error("non-positive adjustment factor")
            prices = {name + "_li": self._raw_li(raw[name], factor) if raw is not None and factor is not None else None for name in ("open", "high", "low", "close")}
            if not suspended and any(value is None for value in prices.values()):
                raise _error(f"ordinary tradable stock has missing price/factor: {symbol}/{day}")
            upper = self._limit(symbol, "up_limit_price", day) if not suspended else None
            lower = self._limit(symbol, "down_limit_price", day) if not suspended else None
            rows.append({"trade_date": day, **prices, "adj_factor": factor, "adjustment_present": factor is not None, "suspended": suspended, "up_limit": upper, "down_limit": lower, "limit_present": upper is not None and lower is not None})
        return tuple(rows)

    def _trading_dates(self, start: date, end: date) -> tuple[date, ...]:
        return tuple(day for day in self._calendar if start <= day <= end)

    def load_industry_at_t(self, *, symbol: str, decision_trade_date: date) -> tuple[str, str]:
        industry, digest = self.load_optional_industry_at_t(symbol=symbol, decision_trade_date=decision_trade_date)
        if industry is None:
            raise _error("PIT L2 file membership is missing or conflicting (missing)")
        return industry, digest

    def load_optional_industry_at_t(self, *, symbol: str, decision_trade_date: date) -> tuple[str | None, str]:
        frame = self._memberships
        rows = frame[(frame.instrument.str.upper() == symbol.upper()) & (frame.start_date <= decision_trade_date) & (frame.end_date >= decision_trade_date)]
        ids = set(int(value) for value in rows.l2_code_id)
        if len(ids) > 1 or not ids.issubset(self._sector_codes):
            raise _error("PIT L2 file membership is conflicting or unmapped")
        industry = "SW_L2_FILE_V1:" + self._sector_codes[next(iter(ids))] if ids else None
        return industry, canonical_json_sha256({"binding_hash": self.binding_hash, "symbol": symbol.upper(), "decision_trade_date": decision_trade_date, "industry": industry})

    def load_pit_eligible_symbols(self, *, universe_key: str, decision_trade_date: date) -> tuple[tuple[str, ...], str]:
        declared = self._files.get("day_selection_universe")
        if not declared or universe_key != "aistock_equity_pit_canonical_v2":
            raise _error("requested PIT universe does not match frozen file authority")
        symbols = tuple(sorted({symbol.upper() for symbol, start, end in self._pool if date.fromisoformat(start) <= decision_trade_date <= date.fromisoformat(end)}))
        if not symbols:
            raise _error("PIT file universe is empty at decision date")
        return symbols, canonical_json_sha256({"binding_hash": self.binding_hash, "universe_key": universe_key, "decision_trade_date": decision_trade_date, "symbols": symbols})
