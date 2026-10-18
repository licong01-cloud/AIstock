"""Consumer-owned, read-only views of a frozen market-file release.

No SQL, network, training, business results or cross-consumer caches live here.
Only locators are translated between Windows and WSL; content identities stay
unchanged. A missing value remains missing, including an entirely empty day.
"""
from __future__ import annotations

from collections import OrderedDict
import base64
from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import math
import os
from pathlib import Path
import re
import zlib
from typing import Any, Iterable

import numpy as np
import pandas as pd


SCHEMA = "frozen_market_dataset_files_v1"
H5_FILES = ("daily_pv", "daily_basic", "moneyflow", "bak_basic", "cyq_perf", "margin_detail", "sector_data")


class FrozenDatasetError(ValueError):
    def __init__(self, message: str, *, reason_code="FROZEN_DATASET_INVALID", **context):
        super().__init__(message)
        self.reason_code = reason_code
        self.context = {"reason_code": reason_code, **context}


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def stamp(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def file_sha256(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024**2), b""):
            result.update(block)
    return result.hexdigest()


def inside(root: Path, relative: str) -> Path:
    if Path(relative).is_absolute() or "\\" in relative or ":" in relative:
        raise FrozenDatasetError("dataset component must use a relative POSIX locator")
    try:
        path = (root / relative).resolve(strict=True)
    except OSError as exc:
        raise FrozenDatasetError("frozen dataset component is unavailable", reason_code="FROZEN_DATASET_COMPONENT_MISSING",
                                 component=relative) from exc
    if not path.is_relative_to(root.resolve(strict=True)) or not path.is_file():
        raise FrozenDatasetError("dataset file escapes frozen root")
    return path


def content_pin(root: Path, relative: str, *, expected: str | None = None) -> dict[str, Any]:
    path = inside(root, relative)
    before = stamp(path)
    observed = file_sha256(path)
    if before != stamp(path) or (expected is not None and observed != expected):
        raise FrozenDatasetError("dataset content differs while freezing", path=relative)
    return {"path": relative, "sha256": observed, "size": before[0],
            "pin_authority": "PRODUCER_MANIFEST" if expected else "CONSUMER_OBSERVED_CONTENT"}


def _bin_lease(root, files):
    """Freeze cheap file-generation guards; read slices separately carry byte hashes.

    This is explicitly a stat lease, not a producer content manifest. It avoids
    hashing/copying a multi-GB minute tree for each replay registration.
    """
    rows = []
    for role, frequency, fields in (
        ("day_meta_export", "day", {"close", "factor", "prev_close", "up_limit_price", "down_limit_price"}),
        ("minute_meta_export", "1min", {"open", "high", "low", "close", "volume", "amount", "factor"}),
    ):
        features = inside(root, files[role]["path"]).parent / "features"
        with os.scandir(features) as directories:
            for directory in directories:
                if not directory.is_dir():
                    continue
                with os.scandir(directory.path) as entries:
                    for entry in entries:
                        if entry.name not in {f"{field}.{frequency}.bin" for field in fields}:
                            continue
                        path = inside(root, Path(entry.path).relative_to(root).as_posix())
                        rows.append([path.relative_to(root).as_posix(), *stamp(path)])
    raw = json.dumps(sorted(rows), separators=(",", ":")).encode()
    return {"schema_version": "consumer_bin_stat_lease_v1", "entry_count": len(rows),
            "sha256": hashlib.sha256(raw).hexdigest(), "zlib_base64": base64.b64encode(zlib.compress(raw)).decode()}


def read_h5_window(path: Path, *, start: date, end: date, columns: list[str],
                   members: Iterable[str] | None = None) -> pd.DataFrame:
    """Bounded date/column scan; symbols filtered in bounded chunks, no full H5 load."""
    if start > end:
        raise FrozenDatasetError("reversed H5 input window")
    low, high = start.isoformat(), end.isoformat()
    wanted = None if members is None else set(members)
    frames = []
    before = stamp(path)
    with pd.HDFStore(path, mode="r") as store:
        for part in store.select("data", where=f"(datetime >= '{low}') & (datetime <= '{high}')",
                                 columns=columns, chunksize=100_000):
            if wanted is not None:
                part = part.loc[part.index.get_level_values("instrument").isin(wanted)]
            if not part.empty:
                frames.append(part)
    if before != stamp(path):
        raise FrozenDatasetError("H5 changed during bounded read")
    frame = pd.concat(frames).sort_index() if frames else pd.DataFrame(
        columns=columns, index=pd.MultiIndex.from_arrays([pd.DatetimeIndex([]), []], names=["datetime", "instrument"]))
    if frame.index.names != ["datetime", "instrument"]:
        frame = frame.reorder_levels(["datetime", "instrument"]).sort_index()
    if frame.index.has_duplicates:
        raise FrozenDatasetError("duplicate stock/date keys in file input")
    return frame


def raw_price_li(adjusted: Any, factor: Decimal) -> Decimal | None:
    """Recover the exporter's integer-li price once, with its declared float tolerance."""
    if adjusted is None or not math.isfinite(float(adjusted)):
        return None
    if not factor.is_finite() or factor <= 0:
        raise FrozenDatasetError("invalid file adjustment factor")
    raw = Decimal(str(adjusted)) / factor
    nearest = (raw * 1000).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    if abs(raw - nearest / 1000) > max(Decimal("0.00001"), abs(raw) * Decimal("0.0000002")):
        raise FrozenDatasetError("raw price cannot be restored at declared li precision")
    return nearest


def freeze_dataset_binding(profile_path: Path | str | None = None) -> dict[str, Any]:
    """Resolve active input once at command registration, never during exact retry."""
    value = profile_path or os.getenv("AISTOCK_ACTIVE_DATASET_PROFILE_PATH")
    if not value:
        raise FrozenDatasetError("active dataset profile is not configured")
    path = Path(value)
    if not path.is_absolute():
        raise FrozenDatasetError("active dataset profile locator must be absolute")
    payload = path.read_bytes()
    profile = json.loads(payload.decode("utf-8-sig"))
    if profile.get("schema_version") not in {"aistock_active_dataset_profile_v3", "aistock_active_dataset_profile_v4"}:
        raise FrozenDatasetError("unsupported active dataset profile schema")
    root = Path(profile["controller_paths"]["candidate_root"]).resolve(strict=True)
    manifest_pin = content_pin(root, "qe_dataset_manifest.json", expected=profile["components"]["dataset_manifest_file_sha256"])
    manifest = json.loads(inside(root, manifest_pin["path"]).read_text(encoding="utf-8-sig"))
    if manifest.get("release_id") != profile.get("release_id"):
        raise FrozenDatasetError("profile/manifest release mismatch")
    required = ("day_calendar", "day_meta_export", "day_selection_universe", "factor_meta", "suspend_data",
                "sector_membership_spans", "sector_code_map", "minute_calendar", "minute_meta_export", "minute_provider_catalog")
    files = {"manifest": manifest_pin}
    for key in required:
        declared = manifest["components"].get(key)
        if not declared:
            raise FrozenDatasetError("required file component is absent", component=key)
        files[key] = content_pin(root, declared["path"], expected=declared["sha256"])
    factor_root = Path(files["factor_meta"]["path"]).parent
    # Some repaired releases do not have producer pins for every H5. Record
    # actual consumer-observed bytes, never mislabel a stale inventory as proof.
    for name in H5_FILES:
        files[name] = content_pin(root, (factor_root / (name + ".h5")).as_posix())
    body = {"schema_version": SCHEMA, "profile": profile, "profile_sha256": hashlib.sha256(payload).hexdigest(),
            "files": files, "bin_stat_lease": _bin_lease(root, files)}
    if path.read_bytes() != payload:
        raise FrozenDatasetError("active profile changed during registration")
    return {**body, "binding_hash": digest(body)}


class FrozenDataset:
    """One consumer/run's file validation and bounded raw-read cache."""

    def __init__(self, binding: dict[str, Any], *, consumer_scope: str, run_id: str,
                 root_override: str | Path | None = None):
        if consumer_scope not in {"ADVISORY", "LOCALSIM"} or not run_id:
            raise FrozenDatasetError("explicit consumer/run required")
        self.binding = deepcopy(binding)
        body = {k: v for k, v in self.binding.items() if k != "binding_hash"}
        if body.get("schema_version") != SCHEMA or digest(body) != binding.get("binding_hash"):
            raise FrozenDatasetError("frozen file binding digest mismatch")
        self.consumer_scope, self.run_id = consumer_scope, run_id
        self.identity = binding["binding_hash"]
        self.root = Path(root_override or body["profile"]["controller_paths"]["candidate_root"]).resolve(strict=True)
        self._verified: dict[Path, tuple[int, int]] = {}
        self._bin_stamps: OrderedDict[Path, tuple[int, int]] = OrderedDict()
        self.read_receipts: list[dict[str, Any]] = []
        self.bytes_read = 0
        lease = body["bin_stat_lease"]
        decoder = zlib.decompressobj()
        raw = decoder.decompress(base64.b64decode(lease["zlib_base64"], validate=True), 64 * 1024**2)
        if not decoder.eof or decoder.unused_data or hashlib.sha256(raw).hexdigest() != lease["sha256"]:
            raise FrozenDatasetError("invalid frozen bin stat lease")
        rows = json.loads(raw)
        self._bin_lease = {row[0]: tuple(row[1:]) for row in rows}
        if lease["schema_version"] != "consumer_bin_stat_lease_v1" or len(rows) != len(self._bin_lease) or len(rows) != lease["entry_count"]:
            raise FrozenDatasetError("invalid frozen bin stat inventory")
        manifest = self.file("manifest")
        if json.loads(manifest.read_text(encoding="utf-8-sig")).get("release_id") != body["profile"]["release_id"]:
            raise FrozenDatasetError("resolved locator has a different dataset release")
        self.calendar = tuple(date.fromisoformat(v) for v in self.file("day_calendar").read_text(encoding="utf-8-sig").splitlines())
        if not self.calendar or tuple(sorted(set(self.calendar))) != self.calendar:
            raise FrozenDatasetError("daily file calendar is not strictly ordered")
        self._day_positions = {day: i for i, day in enumerate(self.calendar)}
        self._pool = tuple(tuple(line.split()) for line in self.file("day_selection_universe").read_text(encoding="utf-8-sig").splitlines() if line.strip())
        if any(len(row) != 3 for row in self._pool):
            raise FrozenDatasetError("invalid PIT membership interval")
        meta = json.loads(self.file("day_meta_export").read_text(encoding="utf-8-sig"))
        self.universe_key = meta["universe_key"]
        self.rule_version = meta["rule_version"]
        self.factor_meta = json.loads(self.file("factor_meta").read_text(encoding="utf-8-sig"))
        if self.factor_meta.get("schema_version") != "qe_direct_factor_h5_static_v2":
            raise FrozenDatasetError("unsupported factor file schema")
        self._minute_calendar: pd.DatetimeIndex | None = None

    def file(self, key: str) -> Path:
        pin = self.binding["files"].get(key)
        if pin is None:
            raise FrozenDatasetError("frozen file component missing", component=key)
        path = inside(self.root, pin["path"])
        before = stamp(path)
        if path not in self._verified:
            if file_sha256(path) != pin["sha256"] or before != stamp(path):
                raise FrozenDatasetError("frozen content hash differs", component=key)
            self._verified[path] = before
        elif self._verified[path] != before:
            raise FrozenDatasetError("frozen file changed after validation", component=key)
        return path

    def assert_unchanged(self):
        for path, before in (*self._verified.items(), *self._bin_stamps.items()):
            if stamp(path) != before:
                raise FrozenDatasetError("frozen file changed during operation", path=str(path))

    def require_day(self, day: date):
        if day not in self._day_positions:
            raise FrozenDatasetError("date outside frozen calendar", reason_code="FROZEN_DATASET_DATE_UNCOVERED", trade_date=day.isoformat())

    def members(self, day: date, *, universe_key: str | None = None) -> list[str]:
        self.require_day(day)
        if universe_key is not None and universe_key != self.universe_key:
            raise FrozenDatasetError("native universe differs from frozen file authority")
        result = sorted({symbol.upper() for symbol, start, end in self._pool if start <= day.isoformat() <= end})
        if not result:
            raise FrozenDatasetError("frozen PIT universe is empty", trade_date=day.isoformat())
        self.assert_unchanged()
        return result

    def spans(self, symbols: list[str], day: date, universe_key: str, *, rule_version=None):
        self.members(day, universe_key=universe_key)
        if rule_version is not None and rule_version != self.rule_version:
            raise FrozenDatasetError("native PIT rule differs from frozen file authority")
        wanted = set(symbols)
        return [(s.upper(), date.fromisoformat(a), date.fromisoformat(b), None, None, self.rule_version,
                 {"file_binding_hash": self.identity}) for s, a, b in self._pool
                if s.upper() in wanted and a <= day.isoformat() <= b]

    def h5(self, name: str, members: list[str], start: date, end: date, columns: list[str]) -> pd.DataFrame:
        if start < date.fromisoformat(self.factor_meta["start"]) or end > date.fromisoformat(self.factor_meta["end"]):
            raise FrozenDatasetError("H5 request exceeds frozen factor coverage", start=str(start), end=str(end))
        frame = read_h5_window(self.file(name), start=start, end=end, members=members, columns=columns)
        self.assert_unchanged()
        self.read_receipts.append({"component": name, "start": str(start), "end": str(end), "row_count": len(frame),
                                   "file_sha256": self.binding["files"][name]["sha256"]})
        return frame

    def suspension_rows(self, symbols: list[str], start: date, end: date):
        if start > end or start < self.calendar[0] or end > self.calendar[-1]:
            raise FrozenDatasetError("suspension request exceeds frozen calendar coverage")
        frame = pd.read_parquet(self.file("suspend_data"), columns=["ts_code", "trade_date", "suspend_type", "suspend_timing"],
                                filters=[("trade_date", ">=", pd.Timestamp(start)), ("trade_date", "<=", pd.Timestamp(end)),
                                         ("ts_code", "in", symbols)])
        self.assert_unchanged()
        return [(str(r.ts_code), pd.Timestamp(r.trade_date).date(), str(r.suspend_type),
                 None if pd.isna(r.suspend_timing) else str(r.suspend_timing)) for r in frame.itertuples(index=False)]

    @staticmethod
    def _symbol(symbol: str):
        if not re.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)", symbol):
            raise FrozenDatasetError("invalid file instrument", symbol=symbol)

    def bin_slice(self, symbol: str, field: str, day: date, *, minute=False) -> tuple[pd.DatetimeIndex, np.ndarray]:
        self._symbol(symbol)
        self.require_day(day)
        if not re.fullmatch(r"[a-z_]+", field):
            raise FrozenDatasetError("invalid file field")
        if minute:
            if self._minute_calendar is None:
                self._minute_calendar = pd.DatetimeIndex(pd.to_datetime(self.file("minute_calendar").read_text(encoding="utf-8-sig").splitlines()))
                if self._minute_calendar.has_duplicates or not self._minute_calendar.is_monotonic_increasing:
                    raise FrozenDatasetError("invalid frozen minute calendar")
            calendar = self._minute_calendar
            indices = np.flatnonzero(calendar.normalize() == pd.Timestamp(day))
            root = self.file("minute_meta_export").parent
            frequency = "1min"
        else:
            calendar = pd.DatetimeIndex(self.calendar)
            indices = np.asarray([self._day_positions[day]])
            root = self.file("day_meta_export").parent
            frequency = "day"
        if not len(indices) or len(indices) > 300:
            raise FrozenDatasetError("requested day has no bounded file calendar slice", trade_date=str(day))
        relative = (root.relative_to(self.root) / "features" / symbol.lower() / f"{field}.{frequency}.bin").as_posix()
        candidate = self.root / relative
        if not candidate.is_file():
            raise FrozenDatasetError("required bin file missing", reason_code="FROZEN_DATASET_FIELD_MISSING", symbol=symbol, field=field)
        path = inside(self.root, relative)
        before = stamp(path)
        if self._bin_lease.get(relative) != before:
            raise FrozenDatasetError("bin differs from frozen generation", reason_code="FROZEN_DATASET_BIN_GENERATION_CHANGED",
                                     symbol=symbol, field=field, trade_date=str(day))
        if path in self._bin_stamps and self._bin_stamps[path] != before:
            raise FrozenDatasetError("bin changed after first use")
        if before[0] < 4 or before[0] % 4:
            raise FrozenDatasetError("truncated float32 bin")
        result = np.full(len(indices), np.nan)
        with path.open("rb") as stream:
            header = float(np.frombuffer(stream.read(4), dtype="<f4")[0])
            if not math.isfinite(header) or header < 0 or header != int(header):
                raise FrozenDatasetError("invalid bin calendar offset")
            start, length = int(header), before[0] // 4 - 1
            if start + length > len(calendar):
                raise FrozenDatasetError("bin exceeds frozen calendar")
            selected = (indices >= start) & (indices < start + length)
            valid = indices[selected]
            content = b""
            if len(valid):
                if np.any(np.diff(valid) != 1):
                    raise FrozenDatasetError("noncontiguous day slice")
                stream.seek(4 * (1 + int(valid[0]) - start))
                content = stream.read(4 * len(valid))
                if len(content) != 4 * len(valid):
                    raise FrozenDatasetError("truncated day slice")
                result[selected] = np.frombuffer(content, dtype="<f4")
        if stamp(path) != before:
            raise FrozenDatasetError("bin changed during read")
        self._bin_stamps[path] = before
        self.bytes_read += len(content) + 4
        self.read_receipts.append({"component": frequency, "symbol": symbol, "field": field, "trade_date": str(day),
                                   "slice_sha256": hashlib.sha256(content).hexdigest(), "observed_values": int(np.isfinite(result).sum())})
        # Metadata only; do not retain all full bin arrays for a history replay.
        if len(self._bin_stamps) > 100_000 or len(self.read_receipts) > 100_000:
            raise FrozenDatasetError("file input observation budget exceeded")
        return calendar[indices], result

    def daily_value(self, symbol: str, field: str, day: date) -> float | None:
        _, values = self.bin_slice(symbol, field, day)
        value = float(values[0])
        return value if math.isfinite(value) else None

    def calendar_rows(self, start: date, end: date):
        if start < self.calendar[0] or end > self.calendar[-1]:
            raise FrozenDatasetError("calendar reference gap is outside frozen coverage")
        days = set(self.calendar)
        return [(start + timedelta(days=i), start + timedelta(days=i) in days) for i in range((end-start).days+1)]

    def raw_daily_close_li(self, symbol: str, day: date):
        close = self.daily_value(symbol, "close", day)
        if close is None:
            return None
        factor = self.daily_value(symbol, "factor", day)
        if factor is None:
            raise FrozenDatasetError("observed daily close lacks its adjustment factor", symbol=symbol, trade_date=str(day))
        return raw_price_li(close, Decimal(str(factor)))

    def reference_rows(self, role: str, *parameters):
        """Rows for the existing reference algorithm, never another valuation rule."""
        if role == "current_close":
            day, symbols = parameters
            return [(symbol, value) for symbol in symbols if (value := self.raw_daily_close_li(symbol, day)) is not None]
        if role == "suspensions":
            return self.suspension_rows(*parameters)
        if role == "calendar":
            return self.calendar_rows(*parameters)
        if role == "previous_close":
            day, symbols = parameters
            end = day - timedelta(days=1)
            start = date.fromisoformat(self.factor_meta["start"])
            frame = self.h5("daily_pv", symbols, start, end, ["close", "factor"])
            result = []
            for symbol, group in frame.groupby(level="instrument", sort=False):
                observed = group.loc[group["close"].notna()]
                if not observed.empty:
                    row = observed.iloc[-1]
                    value = raw_price_li(row["close"], Decimal(str(row["factor"])))
                    result.append((symbol, observed.index[-1][0].date(), value))
            return result
        raise FrozenDatasetError("unsupported reference row contract", role=role)

    def raw_minute_rows(self, symbol: str, day: date) -> list[dict[str, Any]]:
        fields = ("open", "high", "low", "close", "volume", "amount", "factor")
        arrays = {}
        slots = None
        for field in fields:
            times, values = self.bin_slice(symbol, field, day, minute=True)
            if slots is not None and not times.equals(slots):
                raise FrozenDatasetError("minute fields have different calendars")
            slots, arrays[field] = times, values
        prices = np.column_stack([arrays[name] for name in fields[:4]])
        absent = np.isnan(prices).all(axis=1)
        invalid = ~absent & ~np.isfinite(prices).all(axis=1)
        if invalid.any():
            raise FrozenDatasetError("observed minute has partial OHLC", symbol=symbol, trade_date=str(day))
        rows = []
        for index in np.flatnonzero(~absent):
            factor = Decimal(str(float(arrays["factor"][index])))
            if not factor.is_finite() or factor <= 0:
                raise FrozenDatasetError("observed minute lacks an authoritative factor")
            row = {name: float(raw_price_li(float(arrays[name][index]), factor) / 1000) for name in fields[:4]}
            volume = float(arrays["volume"][index]) * float(factor) / 100
            amount = float(arrays["amount"][index])
            if not all(math.isfinite(value) and value >= 0 for value in (volume, amount)):
                raise FrozenDatasetError("observed minute lacks valid activity units")
            rows.append({"time": slots[index].to_pydatetime(), **row, "volume": volume, "amount": amount})
        self.assert_unchanged()
        if not rows:
            raise FrozenDatasetError("historical files returned no minute bars", reason_code="FROZEN_DATASET_MINUTE_MISSING",
                                     source="DATASET_FILES", symbol=symbol, trade_date=str(day))
        return rows

    def close(self):
        self._verified.clear()
        self._bin_stamps.clear()
        self.read_receipts.clear()
        self._minute_calendar = None
