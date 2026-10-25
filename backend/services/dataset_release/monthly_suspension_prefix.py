"""Carry frozen suspension evidence across months; revisit reviewed keys only."""
from __future__ import annotations

from backend.data_service.suspension_day_authority import day_value, digest, load_authority, project_suspension_days


def merge_authorities(previous, approved):
    entries = {(r["ts_code"], r["trade_date"]): r for r in previous["entries"]}
    for row in approved["entries"]:
        key = row["ts_code"], row["trade_date"]
        if key in entries and entries[key] != row:
            raise ValueError("suspension supplement conflicts with frozen disclosure")
        entries[key] = row
    return load_authority({**previous, "entries": [entries[k] for k in sorted(entries)]})


def refresh_prefix(frame, metadata, *, authority, calendar, cutoff):
    """Reproject explicit reviewed keys without auditing/recreating old facts.

    Legacy normalized rows can be used as projection inputs, but must never be
    relabelled as raw provider evidence. An absent row is supplied only by a
    reviewed disclosure, not by an inferred continuous interval.
    """
    import pandas as pd

    empty = {**authority, "entries": []}
    previous = load_authority(metadata.get("suspension_day_authority", empty))
    if metadata.get("suspension_day_authority_sha256", digest(previous)) != digest(previous):
        raise ValueError("inherited suspension authority identity differs")
    authority = merge_authorities(previous, authority)
    raw = list(metadata.get("raw_provider_events", []))
    if metadata.get("raw_provider_events_sha256", digest(raw)) != digest(raw):
        raise ValueError("inherited suspension raw events identity differs")
    allowed = set(calendar)
    touched = {(r["ts_code"], day_value(r["trade_date"])) for r in authority["entries"]
               if day_value(r["trade_date"]) <= cutoff and day_value(r["trade_date"]) in allowed}
    resolutions = {(r["ts_code"], r["trade_date"]): r for r in metadata.get("day_resolutions", [])}
    if not touched:
        return frame, authority, raw, list(resolutions.values()), [], []
    inputs = [r for r in raw if (r["ts_code"], day_value(r["trade_date"])) in touched]
    raw_keys = {(r["ts_code"], day_value(r["trade_date"])) for r in inputs}
    legacy_inputs = []
    for row in frame.to_dict("records"):
        key = row["ts_code"], day_value(row["trade_date"])
        if key in touched and key not in raw_keys:
            legacy_inputs.append(dict(row, trade_date=key[1].isoformat()))
    projected = [r for r in project_suspension_days(inputs + legacy_inputs, authority=authority,
                 required_dates={k[1] for k in touched}) if (r["ts_code"], day_value(r["trade_date"])) in touched]
    if any(r["state"] == "UNRESOLVED" for r in projected):
        raise ValueError("unresolved historical suspension supplement")
    timing = {(r["ts_code"], day_value(r["trade_date"])): r.get("suspend_timing") for r in inputs + legacy_inputs
              if r.get("suspend_type") == "S"}
    keep = [(r.ts_code, day_value(r.trade_date)) not in touched for r in frame.itertuples(index=False)]
    replacement = []
    for resolution in projected:
        key = resolution["ts_code"], day_value(resolution["trade_date"])
        resolutions[key[0], key[1].isoformat()] = resolution
        if resolution["state"] != "RESUMED":
            replacement.append({"trade_date": pd.Timestamp(key[1]), "ts_code": key[0], "suspend_type": "S",
                "suspend_timing": None if resolution["state"] == "FULL_DAY_SUSPENDED" else timing[key]})
    changed = pd.DataFrame(replacement, columns=frame.columns)
    retained = frame.loc[keep]
    result = pd.concat([part for part in (retained, changed) if not part.empty], ignore_index=True)
    result = result.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
    keys = [{"ts_code": symbol, "trade_date": day.isoformat()} for symbol, day in sorted(touched)]
    fallback = [{"ts_code": r["ts_code"], "trade_date": r["trade_date"]} for r in legacy_inputs]
    return result, authority, raw, list(resolutions.values()), keys, fallback
