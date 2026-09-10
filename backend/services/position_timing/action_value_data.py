"""Read-only daily source adapters and immutable research source manifests.

No database writes and no candidate mutation. The raw/adjusted conversion is
shared with runtime features; historical availability is explicitly declared.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from .action_value import (
    CORE_INFORMATION_BLOCK,
    ActionValueError,
    cutoff_on,
    feature_contract,
    market_features,
    normalize_export_bars,
)
from .contracts import canonical_sha256


DAILY_FIELDS = ("open", "high", "low", "close", "volume", "factor", "up_limit_price", "down_limit_price")
BENCHMARK = "000300.SH"


def file_reference(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ActionValueError("SOURCE_FILE_UNAVAILABLE", path=path.as_posix())
    digest = hashlib.sha256()
    before = path.stat()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ActionValueError("SOURCE_CHANGED_WHILE_READING", path=path.as_posix())
    return {"path": path.resolve().as_posix(), "sha256": digest.hexdigest(), "size_bytes": after.st_size}


@dataclass
class DailyCandidate:
    root: Path
    calendar: pd.DatetimeIndex
    spans: pd.DataFrame
    suspension_keys: set[tuple[str, date]]
    references: dict[str, dict[str, Any]]

    @classmethod
    def open(cls, root: Path) -> "DailyCandidate":
        root = root.resolve()
        daily = root / "components" / "daily_bin_candidate"
        paths = {
            "candidate": root / "direct_monthly_state.json", "daily_meta": daily / "meta_export.json",
            "calendar": daily / "calendars" / "day.txt", "pit": daily / "instruments" / "stock_universe.txt",
            "suspend": root / "components" / "suspend_d_daily_candidate_v2" / "suspend_d.parquet",
        }
        refs = {role: file_reference(path) for role, path in paths.items()}
        metadata = json.loads(paths["daily_meta"].read_text(encoding="utf-8"))
        if (metadata.get("export_mode") != "authoritative_aistock_dump_bin" or metadata.get("st_pit") is not True
                or metadata.get("stock_universe_mode") != "pit_spans"):
            raise ActionValueError("DAILY_EXPORT_AUTHORITY_MISMATCH")
        calendar = pd.DatetimeIndex(pd.to_datetime(paths["calendar"].read_text().splitlines()))
        if not len(calendar) or not calendar.is_unique or not calendar.is_monotonic_increasing:
            raise ActionValueError("SOURCE_CALENDAR_INVALID")
        spans = pd.read_csv(paths["pit"], sep="\t", names=["symbol", "start", "end"], dtype=str)
        spans["symbol"] = spans.symbol.str.upper()
        if not spans.symbol.str.fullmatch(r"\d{6}\.(SH|SZ)").all() or BENCHMARK in set(spans.symbol):
            raise ActionValueError("PIT_SYMBOL_INVALID")
        spans["start"], spans["end"] = pd.to_datetime(spans.start), pd.to_datetime(spans.end)
        if spans.empty or spans.isna().any(axis=None) or (spans.start > spans.end).any():
            raise ActionValueError("PIT_SPAN_INVALID")
        suspend = pd.read_parquet(paths["suspend"])
        if not {"ts_code", "trade_date", "suspend_type"}.issubset(suspend):
            raise ActionValueError("SUSPEND_SOURCE_SCHEMA_INVALID")
        suspension_keys = set(zip(suspend.loc[suspend.suspend_type.eq("S"), "ts_code"],
                                  pd.to_datetime(suspend.loc[suspend.suspend_type.eq("S"), "trade_date"]).dt.date))
        if any(file_reference(paths[role]) != reference for role, reference in refs.items()):
            raise ActionValueError("SOURCE_CHANGED_WHILE_READING")
        return cls(root, calendar, spans, suspension_keys, refs)

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self.spans.symbol.unique()))

    def bars(self, symbol: str) -> pd.DataFrame:
        # Offline-only reader reuse. Importing the runtime adapter must not load
        # the historical minute research pipeline into the API process.
        from .minute_execution_pipeline import _read_bin_values

        if symbol != BENCHMARK and symbol not in self.symbols:
            raise ActionValueError("SYMBOL_OUTSIDE_RESEARCH_POPULATION", symbol=symbol)
        folder = self.root / "components" / "daily_bin_candidate" / "features" / symbol.lower()
        fields = ("open", "high", "low", "close", "volume") if symbol == BENCHMARK else DAILY_FIELDS
        data = {}
        for field in fields:
            path = folder / f"{field}.day.bin"
            reference = file_reference(path)
            data[field] = _read_bin_values(path, range(len(self.calendar)))
            if file_reference(path) != reference:
                raise ActionValueError("SOURCE_CHANGED_WHILE_READING", symbol=symbol)
            self.references[f"{symbol}:{field}"] = reference
        result = normalize_export_bars(pd.DataFrame(data, index=self.calendar), benchmark=symbol == BENCHMARK)
        result = result.rename(columns={"up_limit_price": "up_limit", "down_limit_price": "down_limit"})
        # Float32 bin storage introduces sub-tick noise. Restore the existing
        # A-share raw-price precision before comparing directional price limits.
        price_fields = [field for field in ("open", "high", "low", "close", "up_limit", "down_limit") if field in result]
        result[price_fields] = result[price_fields].round(2)
        result["is_suspended"] = [(symbol, day.date()) in self.suspension_keys for day in self.calendar]
        active = np.zeros(len(self.calendar), dtype=bool)
        for row in self.spans.loc[self.spans.symbol.eq(symbol)].itertuples():
            active |= (self.calendar >= row.start) & (self.calendar <= row.end)
        result["pit_active"] = active if symbol != BENCHMARK else True
        # Price bars are end-of-day observations. This is an explicit export
        # convention, not evidence that optional event/flow data were available.
        result["available_at"] = [cutoff_on(day.date()) for day in self.calendar]
        return result

    def coverage(
        self,
        symbols: Sequence[str] | None = None,
        *,
        information_block: str = CORE_INFORMATION_BLOCK,
    ) -> dict[str, Any]:
        selected = tuple(symbols) if symbols is not None else self.symbols
        if not selected or len(set(selected)) != len(selected):
            raise ActionValueError("RESEARCH_POPULATION_INVALID")
        benchmark = self.bars(BENCHMARK).close
        core_market_features, _, _ = feature_contract(CORE_INFORMATION_BLOCK)
        market_feature_names, feature_order, feature_spec_sha256 = feature_contract(information_block)
        counts: dict[str, dict[str, Any]] = {}
        for symbol in selected:
            bars = self.bars(symbol)
            features = market_features(bars, benchmark, information_block=information_block)
            eligible = bars.pit_active
            complete_core = eligible & features.loc[:, core_market_features].notna().all(axis=1)
            complete_selected = eligible & features.notna().all(axis=1)
            counts[symbol] = {
                "pit_sessions": int(eligible.sum()),
                "complete_core_sessions": int(complete_core.sum()),
                "feature_nonmissing": {
                    name: int(features.loc[eligible, name].notna().sum())
                    for name in market_feature_names
                },
                "factor_change_sessions": int((bars.factor.pct_change(fill_method=None).abs() > 1e-6).sum()),
            }
            if information_block != CORE_INFORMATION_BLOCK:
                counts[symbol]["complete_selected_feature_sessions"] = int(complete_selected.sum())
        payload = {
            "schema_version": "position_timing_core_source_coverage_v2",
            "symbols": selected, "calendar_start": self.calendar[0].date().isoformat(),
            "calendar_end": self.calendar[-1].date().isoformat(), "coverage": counts,
            "source_references": deepcopy(self.references), "source_sha256": canonical_sha256(self.references),
            "source_available_at_policy": "DAILY_EXPORT_EOD_CUTOFF_ASSUMPTION",
            "historical_ingestion_timestamps_verified": False,
            "optional_blocks": [], "outcomes_read": False,
        }
        if information_block != CORE_INFORMATION_BLOCK:
            payload.update(
                {
                    "schema_version": "position_timing_optional_source_coverage_v1",
                    "information_block": information_block,
                    "feature_order": feature_order,
                    "feature_spec_sha256": feature_spec_sha256,
                    "optional_blocks": [information_block],
                }
            )
        return payload

    def publish_coverage(self, *, timing_root: Path) -> Path:
        """Full population, source-only preflight; not a training/effect receipt."""
        from .artifact_store import PositionTimingArtifactStore
        from .contracts import canonical_json_bytes

        source_paths = {"reader_source": Path(__file__), "feature_source": Path(__file__).with_name("action_value.py")}
        source_refs = {role: file_reference(path) for role, path in source_paths.items()}
        coverage = self.coverage()
        if any(file_reference(source_paths[role]) != reference for role, reference in source_refs.items()):
            raise ActionValueError("SOURCE_CODE_CHANGED_WHILE_READING")
        coverage.update(source_refs)
        digest = canonical_sha256(coverage)
        path = (timing_root.resolve() / "research" / "action_value_v2" / "source_coverage" / f"{digest}.json").resolve()
        if not path.is_relative_to(timing_root.resolve()):
            raise ActionValueError("COVERAGE_PATH_OUTSIDE_OWNER")
        PositionTimingArtifactStore._publish_immutable(path, canonical_json_bytes(coverage))
        return path


def runtime_core_frame(snapshot: dict[str, Any], symbol: str, *, calendar: Sequence[date], cutoff: datetime) -> pd.DataFrame:
    """Adapt the existing raw outcome loader without replacing source identity.

    The loader's date-range snapshot includes bars and adjustment identities;
    the caller persists that snapshot identity and its capture time separately.
    """
    rows = snapshot.get("rows", {}).get(symbol)
    if not rows or not snapshot.get("identity") or not snapshot.get("adjustment_identity"):
        raise ActionValueError("RUNTIME_CORE_SOURCE_UNAVAILABLE", symbol=symbol)
    if (cutoff.tzinfo is None or list(calendar) != sorted(set(calendar)) or not calendar):
        raise ActionValueError("RUNTIME_CORE_CALENDAR_OR_CUTOFF_INVALID")
    captured_at = snapshot.get("captured_at")
    if captured_at is None:
        raise ActionValueError("RUNTIME_CORE_CAPTURE_TIME_UNAVAILABLE")
    captured = datetime.fromisoformat(str(captured_at))
    if captured.tzinfo is None:
        raise ActionValueError("RUNTIME_CORE_PIT_UNAVAILABLE", symbol=symbol)
    identity = snapshot["identity"]
    expected_rows = canonical_sha256(
        {"rows": snapshot["rows"], "benchmark_rows": snapshot.get("benchmark_rows")}
    )
    if not isinstance(identity, dict) or identity.get("rows_sha256") != expected_rows:
        raise ActionValueError("RUNTIME_CORE_IDENTITY_MISMATCH")
    records = []
    for day in calendar:
        if day > cutoff.date():
            raise ActionValueError("RUNTIME_CORE_FUTURE_DATE")
        row = dict(rows.get(day.isoformat()) or {})
        declared = row.get("feature_available_at")
        if row and declared is None:
            raise ActionValueError("RUNTIME_CORE_FEATURE_TIME_UNAVAILABLE", symbol=symbol)
        if declared is not None:
            available = datetime.fromisoformat(str(declared))
            if available.tzinfo is None or available > cutoff:
                raise ActionValueError("RUNTIME_CORE_PIT_UNAVAILABLE", symbol=symbol)
        row["factor"] = row.get("adj_factor")
        records.append(row)
    frame = pd.DataFrame(records, index=pd.DatetimeIndex(calendar))
    if not {"open", "high", "low", "close", "volume", "factor"}.issubset(frame):
        raise ActionValueError("RUNTIME_CORE_SCHEMA_MISSING")
    for name in ("open", "high", "low", "close", "volume", "factor"):
        frame[name] = pd.to_numeric(frame[name], errors="coerce")
    present = frame[["open", "high", "low", "close"]].notna().any(axis=1)
    if ((~np.isfinite(frame.factor) | frame.factor.le(0)) & present).any():
        raise ActionValueError("ADJUSTMENT_FACTOR_INVALID")
    return frame
