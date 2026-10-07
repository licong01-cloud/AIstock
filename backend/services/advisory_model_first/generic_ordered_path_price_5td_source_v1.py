"""Bounded current-provider D slices; fixed clocks, not observed-bar sequence compression."""
from contextlib import ExitStack
import hashlib
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_pipeline import file_sha256
from backend.services.advisory_model_first.generic_minute_price_5td_source_v1 import (
    aggregate_d_minute_features_v1, _json, _inside, _stamp,
)
from backend.services.advisory_model_first.generic_minute_price_5td_contracts_v1 import FIELDS as VALIDATOR_FIELDS
from backend.services.advisory_model_first.generic_ordered_path_price_5td_contracts_v1 import (
    FIELDS, KEY, ORDERED_FEATURES, PINS, MinuteSourceIdentityV1, check_resource_budget_v1,
)


def expected_clock_v1(d):
    d = pd.Timestamp(d).normalize()
    return pd.date_range(d+pd.Timedelta(hours=9, minutes=30), d+pd.Timedelta(hours=11, minutes=30), freq="min").append(
        pd.date_range(d+pd.Timedelta(hours=13, minutes=1), d+pd.Timedelta(hours=15), freq="min"))


def aggregate_ordered_path_v1(*, slots, arrays):
    slots = pd.DatetimeIndex(slots)
    if set(arrays) != set(FIELDS):
        raise ValueError("ordered path field schema differs")
    a = {name: np.asarray(value, dtype=float) for name, value in arrays.items()}
    # Existing known-OHLC validator; absent amount and flags are unrelated normal UNKNOWN.
    aggregate_d_minute_features_v1(slots=slots, arrays={
        name: a[name] if name in a else np.full(len(slots), np.nan) for name in VALIDATOR_FIELDS})
    tolerance = 1e-5*np.maximum(a["high"], a["low"])
    both = np.isfinite(a["high"]) & np.isfinite(a["low"])
    if (a["low"][both] > a["high"][both]+tolerance[both]).any():
        raise ValueError("ordered known high/low contradicts itself even with other UNKNOWN prices")
    result = {name: np.nan for name in ORDERED_FEATURES}
    if not len(slots):
        return {**result, "ordered_reason": "NO_D_BARS", "ordered_known_fields": 0, "ordered_calendar_slots": 0,
                "ordered_0930_present": False, "ordered_1300_present": False}
    expected = expected_clock_v1(slots[0])
    afternoon_anchor = pd.DatetimeIndex([expected[0].normalize()+pd.Timedelta(hours=13)])
    if not slots.isin(expected.union(afternoon_anchor)).all():
        raise ValueError("ordered path clock is outside exchange session")
    full = pd.DataFrame(a, index=slots).reindex(expected)
    valid = full.loc[:, FIELDS[:4]].notna().all(axis=1).to_numpy()
    # Optional phase-boundary anchors are not replacements for fixed return endpoints.
    # Volume denominator includes every declared D slot; every core trading minute must be present.
    volume = a["volume"]
    total = float(volume.sum()) if expected[1:].isin(slots).all() and np.isfinite(volume).all() else np.nan
    minutes = np.asarray(slots.hour*60+slots.minute)
    ordinals = np.where(minutes <= 690, np.clip((minutes-571)//15, 0, 7),
                        8+np.clip((minutes-781)//15, 0, 7))
    boundaries = [(0, 16)] + [(16+15*i, 31+15*i) for i in range(7)] + [(121+15*i, 136+15*i) for i in range(8)]
    for i, (start, stop) in enumerate(boundaries):
        if valid[start] and valid[stop-1]:
            result[ORDERED_FEATURES[2*i]] = float(10000*(full.close.iloc[stop-1]/full.open.iloc[start]-1))
        if np.isfinite(total) and total > 0:
            result[ORDERED_FEATURES[2*i+1]] = float(volume[ordinals == i].sum()/total)
    known = sum(np.isfinite(value) for value in result.values())
    return {**result, "ordered_reason": "OBSERVED_D_FIXED_CLOCKS" if known == 32 else "PARTIAL_D_FIXED_CLOCKS",
            "ordered_known_fields": int(known), "ordered_calendar_slots": len(slots),
            "ordered_0930_present": bool(expected[0] in slots), "ordered_1300_present": bool(afternoon_anchor[0] in slots)}


def read_d_ordered_path_features_v1(*, roster, identity, active_profile_path):
    started = time.monotonic()
    check_resource_budget_v1(started)
    identity = MinuteSourceIdentityV1.model_validate(identity)
    root = Path(identity.minute_root).resolve()
    profile_path = Path(active_profile_path)
    if not profile_path.is_absolute() or profile_path.stat().st_size > 2*1024**2:
        raise ValueError("minute active profile must be an explicit bounded path")
    before_profile = profile_path.read_bytes()
    profile = _json(profile_path)
    actual = (Path(profile["controller_paths"]["candidate_root"])/"components/minute_bin_candidate").resolve()
    if (not Path(identity.minute_root).is_absolute() or actual != root
            or profile["generation"] != identity.generation or not set(PINS).issubset(identity.pins)
            or profile["components"]["minute_pins"] != identity.pins):
        raise ValueError("minute actual active profile identity differs")
    for name, relative in PINS.items():
        path = _inside(root, root/relative)
        if path.stat().st_size > 16*1024**2 or file_sha256(path) != identity.pins[name]:
            raise ValueError("minute component metadata hash differs")
    meta = _json(root/"meta_export.json")
    if any(meta.get(name) != identity.pins[name] for name in ("rule_version", "snapshot_id", "universe_key") if name in identity.pins):
        raise ValueError("minute component semantic identity differs")
    if "1min" not in meta.get("freq_types", ()):
        raise ValueError("minute source frequency differs")
    calendar = pd.DatetimeIndex(pd.to_datetime((root/PINS["calendar_sha256"]).read_text().splitlines()))
    if not len(calendar) or calendar.tz is not None or not calendar.is_monotonic_increasing or calendar.duplicated().any():
        raise ValueError("minute provider calendar is not strictly ordered")
    start, end = pd.Timestamp(meta.get("start")), pd.Timestamp(meta.get("end"))
    if pd.isna(start) or pd.isna(end) or calendar.min().normalize() < start or calendar.max().normalize() > end:
        raise ValueError("minute metadata and calendar ranges differ")
    if (not isinstance(roster, pd.DataFrame) or len(roster) > 7720 or not roster.columns.is_unique
            or not set(KEY).issubset(roster.columns) or roster.duplicated(list(KEY)).any()
            or roster.duplicated([KEY[0], "instrument"]).any()):
        raise ValueError("minute original roster differs")
    keys = roster.loc[:, KEY].copy()
    for name in KEY[:2]:
        keys[name] = pd.to_datetime(keys[name])
        if keys[name].isna().any() or not keys[name].eq(keys[name].dt.normalize()).all():
            raise ValueError("minute original date is invalid")
    if not keys[KEY[1]].gt(keys[KEY[0]]).all() or keys.instrument.nunique() > 5000:
        raise ValueError("minute target/calendar or original roster budget differs")
    normalized = calendar.normalize()
    positions = {pd.Timestamp(d): np.flatnonzero(normalized == d) for d in keys[KEY[0]].unique()}
    if len(positions) > 386:
        raise ValueError("ordered-path original decision calendar exceeds declared budget")
    results, slices, bytes_read = {}, [], 0
    for instrument, group in keys.groupby("instrument", sort=True):
        check_resource_budget_v1(started)
        if not isinstance(instrument, str) or not re.fullmatch(r"[0-9]{6}\.(SZ|SH|BJ)", instrument):
            raise ValueError("minute instrument cannot form a safe source path")
        with ExitStack() as stack:
            files = {}
            for field in FIELDS:
                path = _inside(root, root/"features"/instrument.lower()/(field+".1min.bin"))
                if not path.exists():
                    files[field] = None
                    continue
                stamp = _stamp(path)
                if stamp[0] < 4 or stamp[0] % 4:
                    raise ValueError("minute float32 bin is truncated")
                stream = stack.enter_context(path.open("rb"))
                start = float(np.frombuffer(stream.read(4), dtype="<f4")[0])
                length = stamp[0]//4-1
                if not np.isfinite(start) or start < 0 or start != int(start) or start+length > len(calendar):
                    raise ValueError("minute bin start/header range differs")
                files[field] = (path, stamp, stream, int(start), length)
            for row in group.itertuples(index=False, name=None):
                d, _, _ = row
                indices = positions[d]
                slots = calendar[indices]
                if len(slots) > 300 or (len(slots) and (slots.normalize() != d).any()):
                    raise ValueError("minute slice cannot contain a future day")
                arrays, fingerprints = {}, {}
                for field, packet in files.items():
                    values = np.full(len(indices), np.nan)
                    if packet is not None:
                        path, stamp, stream, start, length = packet
                        selected = (indices >= start) & (indices < start+length)
                        if selected.any():
                            selected_indices = indices[selected]
                            if np.any(np.diff(selected_indices) != 1):
                                raise ValueError("minute D slice is not contiguous in provider calendar")
                            stream.seek(4*(1+int(selected_indices[0])-start))
                            content = stream.read(4*len(selected_indices))
                            if len(content) != 4*len(selected_indices):
                                raise ValueError("minute D slice changed or truncated during read")
                            bytes_read += len(content)
                            values[selected] = np.frombuffer(content, dtype="<f4")
                            fingerprints[field] = dict(size_bytes=stamp[0], mtime_ns=stamp[1], start=start,
                                slice_first_index=int(selected_indices[0]), slice_count=len(selected_indices),
                                slice_sha256=hashlib.sha256(content).hexdigest())
                    arrays[field] = values
                results[row] = aggregate_ordered_path_v1(slots=slots, arrays=arrays)
                slices.append(dict(D=str(d.date()), instrument=instrument, fields=fingerprints))
            if any(_stamp(packet[0]) != packet[1] for packet in files.values() if packet is not None):
                raise ValueError("minute source changed during D-only reads")
    for name, relative in PINS.items():
        if file_sha256(_inside(root, root/relative)) != identity.pins[name]:
            raise ValueError("minute provider identity changed during prepare")
    if profile_path.read_bytes() != before_profile:
        raise ValueError("active profile changed during minute prepare")
    check_resource_budget_v1(started)
    frame = pd.DataFrame([{**dict(zip(KEY, row, strict=True)), **results[row]}
                          for row in keys.itertuples(index=False, name=None)])
    if frame.empty:
        frame = keys.copy()
        for name in (*ORDERED_FEATURES, "ordered_reason", "ordered_known_fields", "ordered_calendar_slots",
                     "ordered_0930_present", "ordered_1300_present"):
            frame[name] = pd.Series(dtype=object)
    return frame, dict(identity=identity.model_dump(), read_scope="D_PRICE_SLICES_ONLY",
        decoded_bytes=bytes_read, future_price_bars_decoded=0, original_keys=len(keys),
        rows_with_full_ordered_fields=int(frame.ordered_known_fields.eq(32).sum()), slices=slices,
        source_evidence="CURRENT_RELEASE_NON_VINTAGE_D_PRICE_VALUES", database_written=False,
        selection_regenerated=False, dataset_modified=False)
