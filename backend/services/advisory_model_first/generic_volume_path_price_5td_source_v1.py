"""Explicit current-profile provider, bounded D-only bin slices and measured missingness."""
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import re
import time

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_pipeline import file_sha256
from backend.services.advisory_model_first.generic_volume_path_price_5td_contracts_v1 import (
    FIELDS, KEY, MINUTE_FEATURES, PINS, MinuteSourceIdentityV1, check_resource_budget_v1,
)


from backend.services.advisory_model_first.generic_minute_price_5td_source_v1 import aggregate_d_minute_features_v1


def _json(path):
    path = Path(path)
    if path.stat().st_size > 2*1024**2:
        raise ValueError("minute metadata exceeds bounded JSON budget")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("minute metadata has duplicate keys")
            result[key] = value
        return result
    return json.loads(path.read_bytes(), object_pairs_hook=unique)


def _inside(root, path):
    resolved = path.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError("minute source path escapes explicit provider")
    return resolved


def _stamp(path):
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def aggregate_d_volume_path_features_v1(*, slots, arrays):
    """Same D raw bars for both arms; no amount price-coordinate assumptions in new features."""
    # Existing validator measures the seven non-VWAP controls and checks known contradictions.
    result = aggregate_d_minute_features_v1(slots=slots, arrays=arrays)
    slots = pd.DatetimeIndex(slots)
    a = {name: np.asarray(value, dtype=float) for name, value in arrays.items()}
    valid = np.logical_and.reduce([np.isfinite(a[name]) for name in FIELDS[:4]])
    joint = valid & np.isfinite(a["volume"])
    coverage = float(joint.mean()) if len(joint) else 0.
    result.update({name: np.nan for name in MINUTE_FEATURES[-3:]})
    result.update(volume_path_coverage=coverage, volume_path_partial=bool(len(joint) and not joint.all()),
                  volume_path_reason="OBSERVED_D_VOLUME" if len(joint) and joint.all() else "PARTIAL_D_VOLUME")
    if result["minute_coverage_fraction"] < .8 or coverage < .8:
        result["volume_path_reason"] = "INSUFFICIENT_D_JOINT_COVERAGE"
        return result
    positive = joint & (a["volume"] > 0)
    total = a["volume"][positive].sum()
    if total > 0 and valid[-1]:
        proxy = np.dot(a["close"][positive], a["volume"][positive])/total
        result[MINUTE_FEATURES[-3]] = 10000*(a["close"][-1]/proxy-1)
    adjacent = valid[1:] & valid[:-1] & np.isfinite(a["volume"][1:])
    adjacent &= np.diff(slots.asi8) == pd.Timedelta(minutes=1).value
    weights = a["volume"][1:][adjacent]
    if weights.sum() > 0:
        directions = np.sign(np.log(a["close"][1:][adjacent]/a["close"][:-1][adjacent]))
        result[MINUTE_FEATURES[-2]] = float(np.dot(directions, weights)/weights.sum())
    closing = np.asarray(slots.hour*60+slots.minute) >= 870
    if total > 0 and closing.any() and joint[closing].all():
        result[MINUTE_FEATURES[-1]] = float(a["volume"][closing].sum()/total)
    if any(np.isinf(result[name]) for name in MINUTE_FEATURES[-3:]):
        raise ValueError("D volume-path arithmetic is nonfinite")
    return result


def read_d_volume_path_features_v1(*, roster, identity, active_profile_path):
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
        raise ValueError("volume-path original decision calendar exceeds declared budget")
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
                results[row] = aggregate_d_volume_path_features_v1(slots=slots, arrays=arrays)
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
        for name in (*MINUTE_FEATURES, "minute_coverage_fraction", "minute_valid_bars", "minute_calendar_slots",
                     "minute_activity_coverage", "minute_partial", "minute_reason", "volume_path_coverage",
                     "volume_path_partial", "volume_path_reason"):
            frame[name] = pd.Series(dtype=object)
    return frame, dict(identity=identity.model_dump(), read_scope="D_PRICE_SLICES_ONLY",
        decoded_bytes=bytes_read, future_price_bars_decoded=0, original_keys=len(keys),
        rows_with_full_ohlc=int(frame.minute_coverage_fraction.eq(1).sum()), slices=slices,
        source_evidence="CURRENT_RELEASE_NON_VINTAGE_D_PRICE_VALUES", database_written=False,
        selection_regenerated=False, dataset_modified=False)
