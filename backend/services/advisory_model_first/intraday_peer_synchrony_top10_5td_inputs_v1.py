"""Frozen same-D Top50 minute dependence; no labels, SQL or source repair."""

from contextlib import ExitStack
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.data_service.frozen_dataset import FrozenDataset, stamp
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import node_path
from backend.services.advisory_model_first.serial_price_pressure_top10_5td_inputs_v1 import (
    minute_features,
    OLD_MINUTE,
    FIELDS,
)
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY, INPUT_FEATURES
from backend.services.advisory_model_first.peer_residual_top10_5td_v1 import validate_population, GROUP, KEY

SCREEN_ROOT = "X:/AIstock_temp/ADVISORY/p9-intraday-peer-synchrony-20261023"
SCREEN_SHA = "6b6e248df4c82db7b5b01b9da9f69eb7d55ac338632d93271f383625bac832f3"

CONTROL = (
    "sync_own_log_return_bps",
    "sync_peer_log_return_bps",
    "sync_own_rv_bps",
    "sync_peer_rv_bps",
    "sync_joint_coverage",
    "sync_min_peer_fraction",
)
NEW = ("sync_correlation", "sync_directional_participation")
OLD = (*OLD_MINUTE, "serial_correlation", "serial_closing_pressure", "serial_coverage", "nonzero_return_fraction")


def file_sha(p):
    h = hashlib.sha256()
    with Path(p).open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024**2), b""):
            h.update(block)
    return h.hexdigest()


def synchrony(slots, close, *, day, minimum_peers=40, minimum_coverage=0.8):
    """One original package/day, columns are its unchanged 50 unique stocks."""
    slots = pd.DatetimeIndex(slots)
    a = np.asarray(close, float)
    minute = np.asarray(slots.hour * 60 + slots.minute)
    legal = ((minute >= 570) & (minute <= 690)) | ((minute >= 780) & (minute <= 900))
    if (
        a.shape != (len(slots), 50)
        or slots.has_duplicates
        or slots.hasnans
        or slots.tz is not None
        or not slots.is_monotonic_increasing
        or len(slots) > 300
        or not (slots.normalize() == pd.Timestamp(day).normalize()).all()
        or not legal.all()
        or (slots.second != 0).any()
        or (slots.microsecond != 0).any()
        or (slots.nanosecond != 0).any()
    ):
        raise ValueError("same-minute original Top50/D clock differs")
    if np.isinf(a).any() or (a[np.isfinite(a)] <= 0).any():
        raise ValueError("invalid known peer close")
    adjacent = np.diff(slots.asi8) == pd.Timedelta(minutes=1).value
    valid = np.isfinite(a[1:]) & np.isfinite(a[:-1]) & adjacent[:, None]
    r = np.full((max(len(slots) - 1, 0), 50), np.nan)
    if len(slots) > 1:
        r[valid] = (np.log(a[1:]) - np.log(a[:-1]))[valid]
    counts = valid.sum(axis=1)[:, None] - valid
    sums = np.nansum(r, axis=1)[:, None] - np.where(valid, r, 0.0)
    peer = np.divide(sums, counts, out=np.full_like(sums, np.nan), where=counts >= minimum_peers)
    records = []
    for i in range(50):
        joint = valid[:, i] & np.isfinite(peer[:, i])
        n = int(joint.sum())
        row = {name: np.nan for name in (*CONTROL, *NEW)}
        row.update(
            sync_joint_coverage=n / int(adjacent.sum()) if adjacent.any() else 0.0,
            sync_min_peer_fraction=float(counts[joint, i].min() / 49) if n else np.nan,
            sync_observed_edges=n,
            sync_possible_edges=int(adjacent.sum()),
            sync_reason="UNKNOWN_INSUFFICIENT_JOINT_EDGES",
        )
        if n >= 2 and row["sync_joint_coverage"] >= minimum_coverage:
            y, p = r[joint, i], peer[joint, i]
            row.update(
                sync_own_log_return_bps=float(y.sum() * 10000),
                sync_peer_log_return_bps=float(p.sum() * 10000),
                sync_own_rv_bps=float(np.linalg.norm(y) * 10000),
                sync_peer_rv_bps=float(np.linalg.norm(p) * 10000),
            )
            yc, pc = y - y.mean(), p - p.mean()
            denominator = float(np.linalg.norm(yc) * np.linalg.norm(pc))
            if denominator == 0:
                row["sync_reason"] = "UNKNOWN_ZERO_OWN_OR_PEER_VARIANCE"
            else:
                corr = float(yc @ pc / denominator)
                if not np.isfinite(corr) or abs(corr) > 1 + 1e-12:
                    raise ValueError("invalid synchrony arithmetic")
                row.update(
                    sync_correlation=float(np.clip(corr, -1, 1)),
                    sync_directional_participation=float(np.clip(corr, -1, 1) * y.sum()),
                    sync_reason="AVAILABLE",
                )
        records.append(row)
    return pd.DataFrame(records)


def read_panel(plan, inputs, expected=None, budget=lambda: None):
    """Read each requested unique stock/D once, retain absolute shared clocks."""
    binding = plan["binding"]
    sources(plan, expected)
    validate_population(inputs)
    inputs = inputs.loc[:, [*IDENTITY, "source_id", "parent_score", *INPUT_FEATURES]].reset_index(drop=True)
    if len(inputs) > 30500 or inputs[KEY[1]].nunique() > 305:
        raise ValueError("minute population budget exceeded")
    pairs = inputs[["instrument", KEY[1]]].drop_duplicates()
    closes, singles, files, missing = {}, [], [], []
    digest = hashlib.sha256()
    byte_count = 0
    dataset = FrozenDataset(
        binding,
        consumer_scope="ADVISORY",
        run_id="P9_FROZEN_INPUT",
        root_override=node_path(binding["profile"]["controller_paths"]["candidate_root"]),
    )
    try:
        day_calendar = pd.DatetimeIndex(dataset.calendar)
        for (day, target), group in inputs.groupby([KEY[1], KEY[2]], sort=False):
            if (
                day not in day_calendar
                or day_calendar.get_loc(day) + 1 >= len(day_calendar)
                or target != day_calendar[day_calendar.get_loc(day) + 1]
            ):
                raise ValueError("original D/T calendar differs")
        cal_path = dataset.file("minute_calendar")
        calendar = pd.DatetimeIndex(pd.to_datetime(cal_path.read_text(encoding="utf-8-sig").splitlines()))
        if calendar.hasnans or calendar.has_duplicates or not calendar.is_monotonic_increasing:
            raise ValueError("invalid shared minute calendar")
        dates = calendar.normalize()
        starts = np.r_[0, np.flatnonzero(dates[1:].to_numpy() != dates[:-1].to_numpy()) + 1]
        positions = {dates[a]: (int(a), int(b)) for a, b in zip(starts, np.r_[starts[1:], len(calendar)], strict=True)}
        minute_root = dataset.file("minute_meta_export").parent
        day_slots = {}
        for number, (symbol, g) in enumerate(pairs.groupby("instrument", sort=True), 1):
            budget()
            with ExitStack() as stack:
                streams = {}
                for field in FIELDS:
                    relative = (
                        minute_root.relative_to(dataset.root) / "features" / symbol.lower() / (field + ".1min.bin")
                    ).as_posix()
                    path = (dataset.root / relative).resolve()
                    if not path.is_relative_to(dataset.root):
                        raise ValueError("minute path escapes frozen root")
                    expected = dataset._bin_lease.get(relative)
                    if expected is None and not path.exists():
                        streams[field] = None
                        missing.append(relative)
                        continue
                    before = stamp(path)
                    if before != expected or before[0] < 4 or before[0] % 4:
                        raise ValueError("minute generation differs from frozen lease")
                    stream = stack.enter_context(path.open("rb"))
                    raw = stream.read(4)
                    start = float(np.frombuffer(raw, dtype="<f4")[0])
                    length = before[0] // 4 - 1
                    if not np.isfinite(start) or start < 0 or int(start) != start or start + length > len(calendar):
                        raise ValueError("invalid minute header/range")
                    streams[field] = (stream, int(start), length)
                    files.append(dict(path=relative, stat=list(before)))
                    digest.update(relative.encode() + raw)
                    byte_count += 4
                for day in sorted(g[KEY[1]]):
                    if day not in positions:
                        raise ValueError("required D missing from shared minute calendar")
                    lo, hi = positions[day]
                    slots = calendar[lo:hi]
                    day_slots[day] = slots
                    indices = np.arange(lo, hi)
                    arrays = {}
                    digest.update(pd.Timestamp(day).isoformat().encode())
                    for field, stream_spec in streams.items():
                        values = np.full(len(indices), np.nan)
                        digest.update(field.encode())
                        if stream_spec is not None:
                            stream, start, length = stream_spec
                            selected = (indices >= start) & (indices < start + length)
                            digest.update(selected.tobytes())
                            if selected.any():
                                actual = indices[selected]
                                stream.seek(4 * (1 + int(actual[0]) - start))
                                raw = stream.read(4 * len(actual))
                                if len(raw) != 4 * len(actual):
                                    raise ValueError("truncated minute slice")
                                values[selected] = np.frombuffer(raw, dtype="<f4")
                                digest.update(raw)
                                byte_count += len(raw)
                        arrays[field] = values
                    if byte_count > 300_000_000:
                        raise ValueError("minute byte budget exceeded")
                    singles.append(dict(instrument=symbol, **{KEY[1]: day}, **minute_features(slots, arrays, day=day)))
                    closes[(symbol, day)] = arrays["close"]
            if number % 200 == 0:
                print(json.dumps(dict(phase="MINUTE_SYMBOLS", symbols=number)), flush=True)
        for item in files:
            if stamp(dataset.root / item["path"]) != tuple(item["stat"]):
                raise ValueError("minute source changed during read")
        dataset.assert_unchanged()
        result = inputs.merge(
            pd.DataFrame(singles)[["instrument", KEY[1], *OLD]],
            on=["instrument", KEY[1]],
            how="left",
            validate="many_to_one",
        )
        contexts = []
        for (package, day), g in result.groupby(GROUP, sort=True):
            budget()
            matrix = np.column_stack([closes[(s, day)] for s in g.instrument])
            measured = synchrony(day_slots[day], matrix, day=day)
            measured.index = g.index
            contexts.append(measured)
        result = result.join(pd.concat(contexts).sort_index())
        if not result[list(IDENTITY)].equals(inputs[list(IDENTITY)].reset_index(drop=True)):
            raise ValueError("original input identity/order changed")
        return result, dict(
            binding_hash=binding["binding_hash"],
            files=files,
            missing_files=missing,
            metadata_pins={name: binding["files"][name] for name in ("minute_calendar", "minute_meta_export")},
            metadata_stamps={
                name: list(stamp(dataset.file(name))) for name in ("minute_calendar", "minute_meta_export")
            },
            consumed_bin_bytes=byte_count,
            consumed_slice_sha256=digest.hexdigest(),
            unique_stock_days=len(pairs),
            minimum_consumed_D=str(pairs[KEY[1]].min().date()),
            maximum_consumed_D=str(pairs[KEY[1]].max().date()),
            identity_semantics="FROZEN_METADATA_STAT_LEASE_AND_CONSUMED_D_SLICES_NOT_WHOLE_BIN_HASH",
            sql_count=0,
            label_reads=0,
        )
    finally:
        dataset.close()


def screen_evidence():
    root = node_path(SCREEN_ROOT)
    if file_sha(root / "screen_receipt.json") != SCREEN_SHA:
        raise ValueError("input screen receipt identity differs")
    receipt = json.loads((root / "screen_receipt.json").read_text(encoding="utf-8"))
    names = {
        "input_spec.json": "spec_sha256",
        "screen.py": "code_sha256",
        "screen_rows.parquet": "output_sha256",
        "factor_audit.json": "factor_audit_sha256",
        "synthetic_receipt.json": "synthetic_sha256",
    }
    if any(file_sha(root / name) != receipt[key] for name, key in names.items()):
        raise ValueError("input screen evidence drift")
    if receipt["decision"] != "INPUT_SUPPORTED_FOR_F1" or receipt["fit_count"] or receipt["label_reads"]:
        raise ValueError("expected registered label-free supported screen")
    return receipt


def sources(plan, expected=None):
    evidence = screen_evidence()
    binding = plan["binding"]
    if binding["binding_hash"] != evidence["binding_hash"]:
        raise ValueError("screen and study binding differ")
    dataset = FrozenDataset(
        binding,
        consumer_scope="ADVISORY",
        run_id="P9_SOURCE_CHECK",
        root_override=node_path(binding["profile"]["controller_paths"]["candidate_root"]),
    )
    try:
        names = ("day_calendar", "minute_calendar", "minute_meta_export")
        paths = {n: dataset.file(n) for n in names}
        pins = {n: binding["files"][n] for n in names}
        current = {n: list(stamp(p)) for n, p in paths.items()}
        if expected is not None and current != expected:
            raise ValueError("source generation differs")
        for n in ("minute_calendar", "minute_meta_export"):
            if pins[n] != evidence["metadata_pins"][n] or current[n] != evidence["metadata_stamps"][n]:
                raise ValueError("source no longer matches screen generation")
        dataset.assert_unchanged()
        return paths, pins, current
    finally:
        dataset.close()


def verify_consumed(plan, receipt):
    sources(plan)
    root = node_path(plan["binding"]["profile"]["controller_paths"]["candidate_root"]).resolve()
    for item in receipt["files"]:
        path = (root / item["path"]).resolve()
        if not path.is_relative_to(root) or list(stamp(path)) != item["stat"]:
            raise ValueError("consumed minute generation differs")
    for name in receipt["missing_files"]:
        path = (root / name).resolve()
        if not path.is_relative_to(root) or path.exists():
            raise ValueError("missing minute source changed")


def verify_screen_alignment(frame):
    evidence = screen_evidence()
    screen = pd.read_parquet(node_path(SCREEN_ROOT) / "screen_rows.parquet")
    part = frame.loc[frame[KEY[1]].le(pd.Timestamp("2025-03-31"))].reset_index(drop=True)
    if len(part) != evidence["original_rows"] or not part[list(IDENTITY)].equals(screen[list(IDENTITY)]):
        raise ValueError("screen original population differs")
    columns = [*OLD, *CONTROL, *NEW, "sync_observed_edges", "sync_possible_edges"]
    actual, expected = part[columns].to_numpy(float), screen[columns].to_numpy(float)
    if not np.allclose(
        actual, expected, rtol=1e-12, atol=1e-10, equal_nan=True
    ) or not part.sync_reason.equals(screen.sync_reason):
        raise ValueError("screen measurements differ")
    return dict(rows=len(part), columns=len(columns),
        exact_equal_including_nan=bool(np.array_equal(actual, expected, equal_nan=True)),
        values_equal_with_declared_tolerance=True, rtol=1e-12, atol=1e-10, screen_sha256=SCREEN_SHA)
