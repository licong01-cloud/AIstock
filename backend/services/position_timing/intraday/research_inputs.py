"""Read-only, producer-pinned Qlib inputs for minute timing research."""

from __future__ import annotations

import json
from datetime import datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

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

    def file(self, relative: str) -> Path:
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
        if frequency not in self.calendars:
            group = "minute" if frequency == "1min" else "daily"
            path = self.file(f"components/{group}_bin_candidate/calendars/{frequency}.txt")
            values = pd.DatetimeIndex(pd.to_datetime(path.read_text(encoding="utf-8-sig").splitlines()))
            if values.empty or values.has_duplicates or not values.is_monotonic_increasing or values.tz is not None:
                raise IntradayError("RESEARCH_CALENDAR_INVALID", 422)
            self.calendars[frequency] = values
        return self.calendars[frequency]

    def values(self, symbol: str, day, field: str, frequency: str):
        calendar = self.calendar(frequency)
        indexes = np.flatnonzero(calendar.normalize() == pd.Timestamp(day))
        if not len(indexes):
            raise IntradayError("RESEARCH_DATE_OUTSIDE_CALENDAR", 422)
        group = "minute" if frequency == "1min" else "daily"
        path = self.file(f"components/{group}_bin_candidate/features/{symbol.lower()}/{field}.{frequency}.bin")
        if path.stat().st_size < 8 or path.stat().st_size % 4:
            raise IntradayError("RESEARCH_BIN_HEADER_INVALID", 422)
        values = np.memmap(path, dtype="<f4", mode="r")
        header = float(values[0])
        if not np.isfinite(header) or header < 0 or not header.is_integer():
            raise IntradayError("RESEARCH_BIN_HEADER_INVALID", 422)
        if int(header) + len(values) - 1 > len(calendar):
            raise IntradayError("RESEARCH_BIN_OUTSIDE_CALENDAR", 422)
        offsets = indexes - int(header) + 1
        if (offsets < 1).any() or (offsets >= len(values)).any():
            raise IntradayError("RESEARCH_BIN_HISTORY_INCOMPLETE", 422)
        return calendar[indexes], np.asarray(values[offsets], dtype=float)

    def market(self, symbol: str, day) -> MarketView:
        daily_calendar = self.calendar("day")
        if pd.Timestamp(day) not in daily_calendar:
            raise IntradayError("RESEARCH_PLAN_NON_TRADING_DAY", 422)
        # This is a physical quote span check, never a PIT selection rule.
        path = self.file("components/minute_bin_candidate/instruments/all.txt")
        spans = [line.split("\t") for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
        if any(len(row) != 3 for row in spans):
            raise IntradayError("RESEARCH_QUOTE_SPAN_INVALID", 422)
        if not any(row[0].upper() == symbol and row[1][:10] <= str(day) <= row[2][:10] for row in spans):
            raise IntradayError("RESEARCH_QUOTE_SPAN_MISSING", 422)
        suspension = self.file("components/suspend_d_daily_candidate_v2/suspend_d.parquet")
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
        for relative, pin in self.pins.items():
            if file_sha256(self.root / relative) != pin["sha256"]:
                raise IntradayError("RESEARCH_SOURCE_CHANGED_DURING_READ", 422)


def load_markets(manifest: FileRef, plans: list[PlanEvent]) -> tuple[list[MarketView], dict]:
    reader = CandidateReader(manifest)
    keys = sorted({(event.plan.symbol, event.plan.target_trade_date) for event in plans})
    markets = [reader.market(symbol, day) for symbol, day in keys]
    reader.verify_pins()
    return markets, {"candidate_sha256": manifest.sha256, "components": reader.pins,
                     "daily_publication_time_proven": False, "minute_visibility": "ASSUMED_END_PLUS_5_SECONDS"}
