"""Exact frozen database corrections accompanying an ordinary month tail."""
from __future__ import annotations

from datetime import date, datetime
import hashlib
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .canonical import canonical_json_bytes
from .index_contract import merge_index_rows_missing_only
from .index_materializer import _index_csv_frames, _to_context_frame
from .monthly_legacy_prefix import _plain_chain, _signature


def index_correction_rows(payload: Mapping[str, Any] | None, *, before: date) -> list[dict[str, Any]]:
    value = (payload or {}).get("index_history_repairs")
    if value is None:
        return []
    if not isinstance(value, Mapping) or set(value) != {"schema_version", "source_dataset", "captured_at", "rows"}:
        raise ValueError("exact index correction fields differ")
    if value["schema_version"] != "aistock_monthly_index_history_repair_v1" or value["source_dataset"] != "market.index_daily":
        raise ValueError("exact index correction source differs")
    captured = datetime.fromisoformat(value["captured_at"])
    if captured.tzinfo is None or not isinstance(value["rows"], list) or not value["rows"]:
        raise ValueError("exact index correction capture is incomplete")
    rows, _ = merge_index_rows_missing_only(value["rows"], [])
    if any(row["trade_date"] > before for row in rows):
        raise ValueError("exact index correction escapes inherited dates")
    return rows


def correct_raw_frame(frame: pd.DataFrame, rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    """Fill exact missing raw facts; reject changes to existing finite facts."""
    result = frame.copy()
    for row in rows:
        values = {"trade_date": pd.Timestamp(row["trade_date"]), "ts_code": row["ts_code"],
                  **{field: float(row[field]) for field in ("open", "high", "low", "close", "amount")},
                  "volume": float(row["vol"]) * 100.0}
        mask = (result.ts_code == values["ts_code"]) & (pd.to_datetime(result.trade_date) == values["trade_date"])
        if int(mask.sum()) > 1:
            raise ValueError("historical index correction target is duplicated")
        if mask.any():
            for field in ("open", "high", "low", "close", "volume", "amount"):
                existing = float(result.loc[mask, field].iloc[0])
                if np.isfinite(existing) and existing != values[field]:
                    raise ValueError("historical index correction conflicts with finite raw fact")
                result.loc[mask, field] = values[field]
        else:
            result = pd.concat([result, pd.DataFrame([values], columns=frame.columns).astype(frame.dtypes.to_dict())], ignore_index=True)
    return result


def correct_index_bins(provider: Path, rows: Sequence[Mapping[str, Any]], *, native: Mapping[str, Any]) -> dict[str, Any]:
    """Rewrite only the small affected index files in a PRIVATE successor.

    No date-range scan, provider access, stock rewrite or interpolation. A
    missing prefix is structural NaN, never fabricated historical quotes.
    """
    if not rows:
        return dict(native)
    from .monthly_quote_catalog import _replace
    from .qlib_bounded_update import read_calendar
    calendar = read_calendar(provider / "calendars/day.txt")
    positions = {day: index for index, day in enumerate(calendar)}
    frames = _index_csv_frames(_to_context_frame(rows))
    lookup = {(str(item["instrument"]).upper(), item["feature"]): dict(item) for item in native["feature_receipts"]}
    changed = []
    for code, frame in frames.items():
        keys = [(positions[str(row["date"])], row) for row in frame.to_dict("records")]
        for field in frame.columns[2:]:
            path = provider / "features" / code.lower() / f"{field}.day.bin"
            _plain_chain(path)
            if not path.is_file() or path.stat().st_nlink != 1 or (code, field) not in lookup:
                raise ValueError("index correction requires a private native index feature")
            signature = _signature(path)
            raw = path.read_bytes()
            values = np.frombuffer(raw, dtype="<f4")
            if len(values) < 2 or not np.isfinite(values[0]) or int(values[0]) != values[0]:
                raise ValueError("index correction feature header differs")
            old_start = int(values[0])
            start = min(old_start, min(index for index, _ in keys))
            end = old_start + len(values)-2
            output = np.full(end-start+1, np.nan, dtype="<f4")
            output[old_start-start:] = values[1:]
            for index, row in keys:
                if not 0 <= index <= end:
                    raise ValueError("index correction escapes native calendar bounds")
                value = np.float32(row[field])
                existing = output[index-start]
                if np.isfinite(existing) and existing != value:
                    raise ValueError("index correction conflicts with finite feature")
                output[index-start] = value
            payload = np.array([start], dtype="<f4").tobytes() + output.tobytes()
            if _signature(path) != signature:
                raise ValueError("index feature changed during correction")
            _replace(path, payload)
            item = {**lookup[(code, field)], "start_index": start, "end_index": end,
                    "target_sha256": hashlib.sha256(payload).hexdigest(), "target_size": len(payload),
                    "target_signature": list(_signature(path))}
            lookup[(code, field)] = item
            changed.append(item)
    return {**native, "feature_receipts": list(lookup.values()), "historical_index_correction": {
        "keys": len(rows), "rows_sha256": hashlib.sha256(canonical_json_bytes(rows)).hexdigest(),
        "files": changed, "database_access": False, "historical_business_audit_performed": False}}
