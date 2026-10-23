"""Shared market-day interpretation; raw provider events are never rewritten.

Official disclosures resolve only their explicitly verified stock/date keys.
This is effective market state, not proof of historical information availability.
No price, missing trade, or S/R ordering is used to manufacture a suspension.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping

DEFAULT_PATH = Path(__file__).with_name("suspension_day_authority_v1.json")
SCHEMA = "aistock_suspension_day_authority_v1"
POLICY = "provider_events_with_disclosure_resolution_v1"


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def day_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value)
    return datetime.strptime(raw, "%Y%m%d").date() if re.fullmatch(r"\d{8}", raw) else date.fromisoformat(raw)


def load_authority(payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    value = json.loads(DEFAULT_PATH.read_bytes()) if payload is None else dict(payload)
    if set(value) != {"schema_version", "policy", "entries"} or value["schema_version"] != SCHEMA or value["policy"] != POLICY:
        raise ValueError("suspension authority schema differs")
    if not isinstance(value["entries"], list):
        raise ValueError("suspension authority entries differ")
    keys = []
    for entry in value["entries"]:
        required = {"ts_code", "trade_date", "state", "announcement_id", "disclosure_date", "source_url",
                    "source_content_sha256", "source_file_size", "historical_publication_time_proven"}
        if entry.get("state") == "INTRADAY_SUSPENDED":
            required |= {"provider_suspend_timing", "effective_suspend_timing"}
            if (not isinstance(entry.get("provider_suspend_timing"), str)
                or not entry["provider_suspend_timing"].strip()
                or not isinstance(entry.get("effective_suspend_timing"), str)
                or not _intraday(entry["effective_suspend_timing"])):
                raise ValueError("suspension intraday disclosure timing differs")
        if set(entry) != required or not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", entry["ts_code"]):
            raise ValueError("suspension disclosure fields differ")
        if (entry["state"] not in {"FULL_DAY_SUSPENDED", "INTRADAY_SUSPENDED"} or not entry["announcement_id"]
            or not re.fullmatch(r"[0-9a-f]{64}", entry["source_content_sha256"])
            or not entry["source_url"].startswith("https://")
            or type(entry["source_file_size"]) is not int or entry["source_file_size"] <= 0
            or entry["historical_publication_time_proven"] is not False):
            raise ValueError("suspension disclosure identity differs")
        day_value(entry["disclosure_date"])
        keys.append((entry["ts_code"], day_value(entry["trade_date"])))
    if keys != sorted(set(keys)):
        raise ValueError("suspension authority keys are duplicated or unordered")
    return value


def frozen_authority(cas: Any, snapshot: Any) -> dict[str, Any]:
    """Never borrow current rules for an older immutable SOURCE snapshot."""
    manifest = cas.get_json_bounded(snapshot.source_manifest_ref, max_bytes=16 * 1024 * 1024)
    if not isinstance(manifest, Mapping):
        raise ValueError("suspension source manifest differs")
    payload = manifest.get("suspension_day_authority")
    # Legacy snapshots may prove ordinary S-only days, but cannot gain new
    # disclosure exceptions from a later application checkout.
    return load_authority(payload if payload is not None else {"schema_version": SCHEMA, "policy": POLICY, "entries": []})


def _intraday(timing: str) -> bool:
    previous_end = None
    for span in timing.split(","):
        if not re.fullmatch(r"\d{1,2}:\d{2}-\d{1,2}:\d{2}", span):
            return False
        try:
            left, right = (datetime.strptime(part, "%H:%M").time() for part in span.split("-"))
        except ValueError:
            return False
        if not time(9, 30) <= left < right <= time(15) or (previous_end is not None and left < previous_end):
            return False
        previous_end = right
    return True


def project_suspension_days(rows: Iterable[Mapping[str, Any]], *, authority: Mapping[str, Any] | None = None,
                            required_dates: Iterable[date] = ()) -> list[dict[str, Any]]:
    """One decision per key, with source identity and unresolved events retained."""
    authority = load_authority(authority)
    authority_sha256 = digest(authority)
    official = {(entry["ts_code"], day_value(entry["trade_date"])): entry for entry in authority["entries"]}
    grouped: dict[tuple[str, date], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (str(row["ts_code"]), day_value(row["trade_date"]))
        grouped[key].append({"ts_code": key[0], "trade_date": key[1].isoformat(),
                             "suspend_type": row.get("suspend_type"), "suspend_timing": row.get("suspend_timing")})
    dates = set(required_dates)
    for key in official:
        if key[1] in dates:
            grouped.setdefault(key, [])
    result = []
    for (symbol, day), raw in sorted(grouped.items()):
        entry = official.get((symbol, day))
        state, basis = "UNRESOLVED", "PROVIDER_EVENTS"
        effective_timing = None
        valid = all(row["suspend_type"] in {"S", "R"} for row in raw)
        # A disclosure settles an ambiguous day or the provider's zero-length
        # opening S marker, never a positive intraday span or duplicate event.
        # The marker alone does NOT prove suspension (ordinary path below).
        disclosure_compatible = valid and len({row["suspend_type"] for row in raw}) == len(raw) and all(
            row["suspend_timing"] is None or row["suspend_timing"] == ""
            or (row["suspend_type"] == "S" and row["suspend_timing"] in ("09:30-09:30", "9:30-9:30"))
            for row in raw
        )
        # A provider clock pair without dates is not, by itself, an ordered
        # same-day interval. Only an exact-key disclosure can establish the
        # effective session. Never swap clocks or infer a next-day resumption.
        intraday_compatible = (entry is not None and entry["state"] == "INTRADAY_SUSPENDED"
            and len(raw) == 1 and raw[0]["suspend_type"] == "S"
            and raw[0]["suspend_timing"] in (entry["provider_suspend_timing"], entry["effective_suspend_timing"]))
        if intraday_compatible:
            state, basis = entry["state"], "OFFICIAL_DISCLOSURE"
            effective_timing = entry["effective_suspend_timing"]
        elif entry is not None and entry["state"] == "FULL_DAY_SUSPENDED" and disclosure_compatible:
            state, basis = entry["state"], "OFFICIAL_DISCLOSURE"
        elif entry is None and valid and len({row["suspend_type"] for row in raw}) == len(raw):
            events = {row["suspend_type"]: str(row["suspend_timing"] or "").strip() for row in raw}
            if "S" in events and _intraday(events["S"]) and not events.get("R"):
                # An explicit S interval proves intraday suspension without
                # claiming the order of a same-day blank R event.
                state = "INTRADAY_SUSPENDED"
            elif len(events) == 1:
                if events == {"S": ""}:
                    state = "FULL_DAY_SUSPENDED"
                elif events == {"R": ""}:
                    state = "RESUMED"
        result.append({"ts_code": symbol, "trade_date": day.isoformat(), "state": state, "basis": basis,
                       **({"effective_suspend_timing": effective_timing} if effective_timing else {}),
                       "raw_events_sha256": digest(sorted(raw, key=canonical_bytes)), "raw_event_count": len(raw),
                       "authority_sha256": authority_sha256, "historical_publication_time_proven": False,
                       "source_content_sha256": entry["source_content_sha256"] if entry else None,
                       "announcement_id": entry["announcement_id"] if entry else None})
    return result


def full_day_keys(rows: Iterable[Mapping[str, Any]], *, authority: Mapping[str, Any] | None = None,
                  required_dates: Iterable[date] = ()) -> frozenset[tuple[str, date]]:
    return frozenset((row["ts_code"], day_value(row["trade_date"]))
                     for row in project_suspension_days(rows, authority=authority, required_dates=required_dates)
                     if row["state"] == "FULL_DAY_SUSPENDED")


def publication_quality(rows: list[dict[str, Any]], day: date) -> tuple[dict[str, Any], dict[str, Any]]:
    authority = load_authority()
    states = project_suspension_days(rows, authority=authority, required_dates=(day,))
    unresolved = [row for row in states if row["state"] == "UNRESOLVED"]
    raw = [dict(row, trade_date=day_value(row["trade_date"]).isoformat()) for row in rows]
    receipt = {"schema_version": "aistock_suspension_day_readback_v1", "trade_date": day.isoformat(),
               "authority": authority, "authority_sha256": digest(authority), "raw_events": raw,
               "raw_events_sha256": digest(raw), "resolutions": states, "unresolved_count": len(unresolved),
               "raw_events_modified": False}
    quality = {"quality_status": "low_coverage", "failure_category": "suspension_day_state_unresolved"} if unresolved else {"quality_status": "ok" if rows else "empty_valid"}
    return receipt, quality
