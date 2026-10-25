"""Frozen, consumer-neutral receipt adapter for the sole public day projector."""

from __future__ import annotations

from copy import deepcopy
from datetime import date
import hashlib
from pathlib import Path
from typing import Any, Mapping, Sequence

from backend.data_service import suspension_day_authority as public_suspension


_LOADED_AUTHORITY = public_suspension.load_authority()
_IMPLEMENTATION_SHA256 = hashlib.sha256(Path(public_suspension.__file__).read_bytes()).hexdigest()
CONTRACT = "shared_selection_suspension_authority_v1"


def project_selection_suspension_days(
    rows: Sequence[Mapping[str, Any]], *, symbols: Sequence[str],
    required_dates: Sequence[date], usage: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Keep policy, raw facts and resolutions together without business state."""
    dates = set(required_dates)
    codes = set(symbols)
    raw = [dict(row, trade_date=public_suspension.day_value(row["trade_date"]).isoformat()) for row in rows]
    if any(row["ts_code"] not in codes or date.fromisoformat(row["trade_date"]) not in dates for row in raw):
        raise ValueError("suspension read exceeds requested symbols/dates")
    projected = [
        row for row in public_suspension.project_suspension_days(
            raw, authority=_LOADED_AUTHORITY, required_dates=dates,
        ) if row["ts_code"] in codes and date.fromisoformat(row["trade_date"]) in dates
    ]
    receipt = {
        "schema_version": CONTRACT, "usage": usage,
        "effective_dates": sorted(day.isoformat() for day in dates),
        "historical_publication_time_proven": False,
        "policy_identity": {
            "schema_version": _LOADED_AUTHORITY["schema_version"], "policy": _LOADED_AUTHORITY["policy"],
            "authority_sha256": public_suspension.digest(_LOADED_AUTHORITY),
            "implementation_sha256": _IMPLEMENTATION_SHA256,
        },
        "authority_payload": deepcopy(_LOADED_AUTHORITY),
        "raw_events": sorted(raw, key=public_suspension.canonical_bytes), "projected_days": projected,
    }
    receipt["data_identity"] = public_suspension.digest(receipt)
    return projected, receipt
