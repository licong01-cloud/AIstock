"""Selective, copy-on-write repair of QE Qlib adjustment-factor outputs.

The producer compares an immutable direct-v2 baseline with the canonical
production ``market.adj_factor`` table, clones all unaffected files as
same-volume hardlinks, and rewrites only adjustment-sensitive values.  It does
not update a dataset pointer, profile, database, service, or model artifact.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

import numpy as np


SCHEMA_VERSION = "qe_adj_factor_selective_repair_v1"
INVENTORY_SCHEMA = "qe_adj_factor_selective_repair_inventory_v1"
RECEIPT_SCHEMA = "qe_adj_factor_selective_repair_receipt_v1"
STATE_SCHEMA = "qe_adj_factor_selective_repair_state_v1"
ADJUSTED_FIELDS = ("open", "high", "low", "close", "volume", "factor")
PRICE_FIELDS = ("open", "high", "low", "close")


class AdjFactorCandidateRepairError(RuntimeError):
    """Raised when selective repair cannot prove a safe immutable result."""


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=_json_default,
    ).encode("utf-8")


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_canonical_json_atomic(path: Path, value: Mapping[str, Any]) -> str:
    payload = canonical_json_bytes(value) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return sha256_bytes(payload)


def _write_array_atomic(path: Path, value: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        value.astype("<f4", copy=False).tofile(temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _assert_plain_directory(path: Path, *, label: str) -> Path:
    resolved = path.expanduser().resolve(strict=True)
    if not resolved.is_dir() or resolved.is_symlink():
        raise AdjFactorCandidateRepairError(f"{label} must be an existing plain directory")
    return resolved


def _read_calendar(path: Path) -> tuple[str, ...]:
    try:
        values = tuple(line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    except (OSError, UnicodeDecodeError) as exc:
        raise AdjFactorCandidateRepairError(f"calendar is unreadable: {path}") from exc
    if not values or tuple(sorted(set(values))) != values:
        raise AdjFactorCandidateRepairError(f"calendar is empty, duplicated, or unordered: {path}")
    return values


def _symbol_from_dir(path: Path) -> str:
    symbol = path.name.upper()
    if len(symbol) != 9 or symbol[6:] not in {".SH", ".SZ", ".BJ"} or not symbol[:6].isdigit():
        raise AdjFactorCandidateRepairError(f"invalid feature directory: {path.name}")
    return symbol


def _feature_symbols(root: Path, *, frequency: str) -> tuple[str, ...]:
    features = root / "features"
    if not features.is_dir():
        raise AdjFactorCandidateRepairError(f"feature root is missing: {features}")
    name = f"factor.{frequency}.bin"
    symbols = sorted(
        _symbol_from_dir(path)
        for path in features.iterdir()
        if path.is_dir() and not path.is_symlink() and (path / name).is_file()
    )
    if not symbols:
        raise AdjFactorCandidateRepairError(f"no factor bins found under {features}")
    return tuple(symbols)


def _read_bin(path: Path) -> np.ndarray:
    value = np.fromfile(path, dtype="<f4")
    if len(value) < 2 or not np.isfinite(value[0]):
        raise AdjFactorCandidateRepairError(f"bin is empty or lacks a start offset: {path}")
    return value


def _query_adj_rows(connection: Any, codes: Sequence[str], start: date, end: date) -> dict[str, list[tuple[str, float]]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT ts_code,trade_date,adj_factor
            FROM market.adj_factor
            WHERE ts_code=ANY(%s) AND trade_date BETWEEN %s AND %s
            ORDER BY ts_code,trade_date
            """,
            (list(codes), start, end),
        )
        grouped = {code: [] for code in codes}
        for symbol, trade_date, factor in cursor.fetchall():
            numeric = float(factor)
            if not math.isfinite(numeric) or numeric <= 0:
                raise AdjFactorCandidateRepairError(f"invalid production adj_factor: {symbol}/{trade_date}")
            grouped.setdefault(str(symbol).upper(), []).append((trade_date.isoformat(), numeric))
    return grouped


def _expected_factors(rows: Sequence[tuple[str, float]]) -> dict[str, np.float32]:
    if not rows:
        return {}
    denominator = max(value for _day, value in rows)
    if not math.isfinite(denominator) or denominator <= 0:
        raise AdjFactorCandidateRepairError("invalid qfq denominator")
    return {day: np.float32(value / denominator) for day, value in rows}


def _calendar_day_spans(calendar: Sequence[str]) -> tuple[tuple[str, int, int], ...]:
    """Return half-open calendar offsets grouped by trade date."""

    spans: list[tuple[str, int, int]] = []
    active_day = calendar[0][:10]
    active_start = 0
    for offset, value in enumerate(calendar[1:], start=1):
        day = value[:10]
        if day == active_day:
            continue
        spans.append((active_day, active_start, offset))
        active_day = day
        active_start = offset
    spans.append((active_day, active_start, len(calendar)))
    return tuple(spans)


def _mismatch_summary(
    factor_path: Path,
    calendar: Sequence[str],
    expected: Mapping[str, np.float32],
    *,
    day_spans: Sequence[tuple[str, int, int]] | None = None,
) -> tuple[tuple[str, ...], int]:
    value = _read_bin(factor_path)
    start = int(value[0])
    current = value[1:]
    stop = start + len(current)
    if start < 0 or stop > len(calendar):
        raise AdjFactorCandidateRepairError(f"bin offset falls outside calendar: {factor_path}")
    mismatch_dates: list[str] = []
    mismatch_cells = 0
    for day, span_start, span_stop in day_spans or _calendar_day_spans(calendar):
        overlap_start = max(span_start, start)
        overlap_stop = min(span_stop, stop)
        if overlap_start >= overlap_stop:
            continue
        local = current[overlap_start - start : overlap_stop - start]
        finite = np.isfinite(local)
        if not bool(finite.any()):
            continue
        target = expected.get(day)
        if target is None:
            raise AdjFactorCandidateRepairError(f"production factor is missing for materialized bin: {factor_path}:{day}")
        count = int(np.count_nonzero(finite & (local != target)))
        if count:
            mismatch_dates.append(day)
            mismatch_cells += count
    return tuple(mismatch_dates), mismatch_cells


def _chunked(values: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def scan_inventory(
    *,
    connection: Any,
    baseline_root: Path,
    start: date,
    cutoff: date,
    production_job_id: str,
    production_job_snapshot_sha256: str,
    batch_size: int = 100,
) -> dict[str, Any]:
    """Compare baseline factor bins with production and return exact repair scope."""

    baseline = _assert_plain_directory(baseline_root, label="baseline root")
    daily_root = baseline / "components" / "daily_bin_candidate"
    minute_root = baseline / "components" / "minute_bin_candidate"
    daily_calendar = _read_calendar(daily_root / "calendars" / "day.txt")
    minute_calendar = _read_calendar(minute_root / "calendars" / "1min.txt")
    daily_spans = _calendar_day_spans(daily_calendar)
    minute_spans = _calendar_day_spans(minute_calendar)
    if daily_calendar[-1][:10] != cutoff.isoformat() or minute_calendar[-1][:10] != cutoff.isoformat():
        raise AdjFactorCandidateRepairError("baseline calendars do not reach the requested cutoff")
    daily_symbols = _feature_symbols(daily_root, frequency="day")
    minute_symbols = set(_feature_symbols(minute_root, frequency="1min"))
    records: list[dict[str, Any]] = []
    mismatch_cells = 0
    minute_cells = 0
    for batch in _chunked(daily_symbols, batch_size):
        grouped = _query_adj_rows(connection, batch, start, cutoff)
        for symbol in batch:
            expected = _expected_factors(grouped.get(symbol, ()))
            if not expected:
                raise AdjFactorCandidateRepairError(f"production adj_factor history is empty: {symbol}")
            dates, daily_cell_count = _mismatch_summary(
                daily_root / "features" / symbol.lower() / "factor.day.bin",
                daily_calendar,
                expected,
                day_spans=daily_spans,
            )
            if not dates:
                continue
            minute_dates: tuple[str, ...] = ()
            minute_cell_count = 0
            if symbol in minute_symbols:
                minute_dates, minute_cell_count = _mismatch_summary(
                    minute_root / "features" / symbol.lower() / "factor.1min.bin",
                    minute_calendar,
                    expected,
                    day_spans=minute_spans,
                )
            record = {
                "symbol": symbol,
                "daily_mismatch_count": daily_cell_count,
                "daily_start": dates[0],
                "daily_end": dates[-1],
                "daily_dates_sha256": sha256_bytes(canonical_json_bytes(list(dates))),
                "minute_mismatch_date_count": len(minute_dates),
                "minute_mismatch_cell_count": minute_cell_count,
                "minute_start": minute_dates[0] if minute_dates else None,
                "minute_end": minute_dates[-1] if minute_dates else None,
                "minute_dates_sha256": sha256_bytes(canonical_json_bytes(list(minute_dates))),
            }
            records.append(record)
            mismatch_cells += daily_cell_count
            minute_cells += minute_cell_count
    unsigned = {
        "schema_version": INVENTORY_SCHEMA,
        "baseline_root": str(baseline),
        "start": start.isoformat(),
        "cutoff": cutoff.isoformat(),
        "production_job_id": production_job_id,
        "production_job_snapshot_sha256": production_job_snapshot_sha256,
        "baseline_daily_symbol_count": len(daily_symbols),
        "affected_symbol_count": len(records),
        "daily_mismatch_cell_count": mismatch_cells,
        "minute_mismatch_cell_count": minute_cells,
        "records": records,
    }
    return {**unsigned, "canonical_sha256": sha256_bytes(canonical_json_bytes(unsigned))}


def validate_inventory(value: Mapping[str, Any]) -> None:
    canonical = str(value.get("canonical_sha256") or "")
    unsigned = {key: item for key, item in value.items() if key != "canonical_sha256"}
    if value.get("schema_version") != INVENTORY_SCHEMA or canonical != sha256_bytes(canonical_json_bytes(unsigned)):
        raise AdjFactorCandidateRepairError("repair inventory identity differs")
    records = value.get("records")
    if not isinstance(records, list) or int(value.get("affected_symbol_count", -1)) != len(records):
        raise AdjFactorCandidateRepairError("repair inventory count differs")
    symbols = [str(item.get("symbol")) for item in records if isinstance(item, Mapping)]
    if symbols != sorted(set(symbols)):
        raise AdjFactorCandidateRepairError("repair inventory symbols are duplicated or unordered")


def mutable_relative_paths(inventory: Mapping[str, Any]) -> set[str]:
    validate_inventory(inventory)
    mutable = {
        "direct_monthly_state.json",
        "qe_dataset_manifest.json",
        "components/factor_h5_static_candidate_v2/daily_pv.h5",
        "components/factor_h5_static_candidate_v2/static_factors.parquet",
    }
    for record in inventory["records"]:
        symbol = str(record["symbol"]).lower()
        for field in ADJUSTED_FIELDS:
            mutable.add(f"components/daily_bin_candidate/features/{symbol}/{field}.day.bin")
        if int(record.get("minute_mismatch_cell_count", 0)):
            for field in ADJUSTED_FIELDS:
                mutable.add(f"components/minute_bin_candidate/features/{symbol}/{field}.1min.bin")
    return mutable


def clone_baseline_copy_on_write(
    *,
    baseline_root: Path,
    candidate_root: Path,
    inventory: Mapping[str, Any],
) -> dict[str, Any]:
    """Create/resume a same-volume clone while omitting every mutable file."""

    baseline = _assert_plain_directory(baseline_root, label="baseline root")
    candidate = candidate_root.expanduser().resolve(strict=False)
    if candidate == baseline or candidate in baseline.parents or baseline in candidate.parents:
        raise AdjFactorCandidateRepairError("candidate and baseline roots must be separate siblings")
    if candidate.parent != baseline.parent:
        raise AdjFactorCandidateRepairError("copy-on-write candidate must share the baseline parent volume")
    candidate.mkdir(parents=True, exist_ok=True)
    mutable = mutable_relative_paths(inventory)
    linked = 0
    skipped_mutable = 0
    logical_bytes = 0
    for source in baseline.rglob("*"):
        relative = source.relative_to(baseline).as_posix()
        target = candidate / source.relative_to(baseline)
        if source.is_symlink():
            raise AdjFactorCandidateRepairError(f"baseline contains a symlink: {relative}")
        if source.is_dir():
            if relative == "work" or relative.startswith("work/"):
                continue
            target.mkdir(parents=True, exist_ok=True)
            continue
        if not source.is_file():
            raise AdjFactorCandidateRepairError(f"baseline contains an unsupported entry: {relative}")
        if relative == "work" or relative.startswith("work/") or relative in mutable:
            skipped_mutable += 1
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if not target.is_file() or os.path.samefile(source, target) is False:
                raise AdjFactorCandidateRepairError(f"existing clone entry is not the baseline hardlink: {relative}")
        else:
            os.link(source, target)
        linked += 1
        logical_bytes += source.stat().st_size
    return {
        "linked_file_count": linked,
        "logical_reused_bytes": logical_bytes,
        "omitted_mutable_file_count": skipped_mutable,
        "mutable_path_count": len(mutable),
    }


def _load_expected_for_symbols(
    connection: Any,
    symbols: Sequence[str],
    start: date,
    cutoff: date,
) -> dict[str, dict[str, np.float32]]:
    grouped = _query_adj_rows(connection, symbols, start, cutoff)
    return {symbol: _expected_factors(grouped.get(symbol, ())) for symbol in symbols}


def _query_raw_rows(
    connection: Any,
    *,
    symbol: str,
    frequency: str,
    dates: Sequence[str],
) -> dict[str, tuple[float, float, float, float, float]]:
    requested_dates = sorted({value[:10] for value in dates})
    if not requested_dates:
        return {}
    if frequency == "day":
        sql = """
            SELECT trade_date,open_li,high_li,low_li,close_li,volume_hand
            FROM market.kline_daily_raw
            WHERE ts_code=%s AND trade_date=ANY(%s::date[])
            ORDER BY trade_date
        """
        params = (symbol, requested_dates)
    else:
        sql = """
            SELECT trade_time AT TIME ZONE 'Asia/Shanghai',
                   open_li,high_li,low_li,close_li,volume_hand
            FROM market.kline_minute_raw
            JOIN unnest(%s::date[]) AS requested(trade_date)
              ON trade_time >= (requested.trade_date::timestamp AT TIME ZONE 'Asia/Shanghai')
             AND trade_time < ((requested.trade_date + 1)::timestamp AT TIME ZONE 'Asia/Shanghai')
            WHERE ts_code=%s AND freq='1m'
            ORDER BY trade_time
        """
        params = (requested_dates, symbol)
    rows: dict[str, tuple[float, float, float, float, float]] = {}
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        for key, raw_open, raw_high, raw_low, raw_close, raw_volume in cursor:
            normalized = key.isoformat() if frequency == "day" else key.strftime("%Y-%m-%d %H:%M:%S")
            if normalized in rows:
                raise AdjFactorCandidateRepairError(f"duplicate production raw row: {symbol}/{frequency}/{normalized}")
            values = (raw_open, raw_high, raw_low, raw_close, raw_volume)
            if any(value is None or not math.isfinite(float(value)) for value in values):
                raise AdjFactorCandidateRepairError(f"invalid production raw row: {symbol}/{frequency}/{normalized}")
            rows[normalized] = tuple(float(value) for value in values)
    return rows


def _patch_one_frequency(
    *,
    baseline_component: Path,
    candidate_component: Path,
    symbol: str,
    frequency: str,
    calendar: Sequence[str],
    expected: Mapping[str, np.float32],
    raw_rows: Mapping[str, tuple[float, float, float, float, float]] | None = None,
    connection: Any | None = None,
    day_spans: Sequence[tuple[str, int, int]] | None = None,
) -> dict[str, Any]:
    suffix = "day" if frequency == "day" else "1min"
    baseline_dir = baseline_component / "features" / symbol.lower()
    candidate_dir = candidate_component / "features" / symbol.lower()
    arrays = {field: _read_bin(baseline_dir / f"{field}.{suffix}.bin") for field in ADJUSTED_FIELDS}
    lengths = {len(value) for value in arrays.values()}
    starts = {int(value[0]) for value in arrays.values()}
    if len(lengths) != 1 or len(starts) != 1:
        raise AdjFactorCandidateRepairError(f"adjusted bin shapes differ: {symbol}/{frequency}")
    start_offset = starts.pop()
    current_factor = arrays["factor"][1:]
    changed = np.zeros(len(current_factor), dtype=bool)
    target_factor = current_factor.copy()
    stop_offset = start_offset + len(current_factor)
    if start_offset < 0 or stop_offset > len(calendar):
        raise AdjFactorCandidateRepairError(f"bin offset falls outside calendar: {symbol}/{frequency}")
    for day, span_start, span_stop in day_spans or _calendar_day_spans(calendar):
        overlap_start = max(span_start, start_offset)
        overlap_stop = min(span_stop, stop_offset)
        if overlap_start >= overlap_stop:
            continue
        local_start = overlap_start - start_offset
        local_stop = overlap_stop - start_offset
        current = current_factor[local_start:local_stop]
        finite = np.isfinite(current)
        if not bool(finite.any()):
            continue
        target = expected.get(day)
        if target is None:
            raise AdjFactorCandidateRepairError(f"production factor is missing: {symbol}/{day}")
        local_changed = finite & (current != target)
        if bool(local_changed.any()):
            changed[local_start:local_stop] = local_changed
            target_slice = target_factor[local_start:local_stop]
            target_slice[local_changed] = target
    if not bool(changed.any()):
        raise AdjFactorCandidateRepairError(f"inventory target no longer differs: {symbol}/{frequency}")
    new_factor = target_factor[changed].astype("float64")
    if bool((new_factor <= 0).any()):
        raise AdjFactorCandidateRepairError(f"non-positive factor in bin patch: {symbol}/{frequency}")
    changed_indices = np.flatnonzero(changed)
    changed_keys = [calendar[start_offset + int(value)] for value in changed_indices]
    if raw_rows is None:
        if connection is None:
            raise AdjFactorCandidateRepairError("production raw connection is required")
        raw_rows = _query_raw_rows(
            connection,
            symbol=symbol,
            frequency=frequency,
            dates=changed_keys,
        )
    missing_raw_keys: set[str] = set()
    for field_index, field in enumerate((*PRICE_FIELDS, "volume")):
        selected = arrays[field][1:]
        finite = np.isfinite(selected[changed_indices])
        finite_indices = changed_indices[finite]
        finite_keys = [changed_keys[index] for index in np.flatnonzero(finite)]
        missing_raw_keys.update(key for key in finite_keys if key not in raw_rows)
        if missing_raw_keys:
            continue
        raw = np.asarray([raw_rows[key][field_index] for key in finite_keys], dtype="float64")
        factor = target_factor[finite_indices].astype("float64")
        if field == "volume":
            selected[finite_indices] = np.float32(raw * 100.0 / factor)
        else:
            selected[finite_indices] = np.float32(raw / 1000.0 * factor)
    arrays["factor"][1:][changed] = target_factor[changed]
    if missing_raw_keys:
        sample = sorted(missing_raw_keys)[:5]
        raise AdjFactorCandidateRepairError(f"production raw rows are missing: {symbol}/{frequency}/{sample}")
    files = []
    for field, value in arrays.items():
        target = candidate_dir / f"{field}.{suffix}.bin"
        _write_array_atomic(target, value)
        if os.path.samefile(baseline_dir / target.name, target):
            raise AdjFactorCandidateRepairError(f"mutable output aliases baseline: {target}")
        files.append({"path": str(target), "sha256": sha256_file(target), "size": target.stat().st_size})
    changed_dates = [calendar[start_offset + int(value)][:10] for value in changed_indices]
    return {
        "symbol": symbol,
        "frequency": frequency,
        "changed_factor_cells": int(changed.sum()),
        "changed_start": changed_dates[0],
        "changed_end": changed_dates[-1],
        "files": files,
    }


def _existing_patch_receipt(
    *,
    baseline_component: Path,
    candidate_component: Path,
    symbol: str,
    frequency: str,
    calendar: Sequence[str],
    expected: Mapping[str, np.float32],
    day_spans: Sequence[tuple[str, int, int]],
    changed_cells: int,
    changed_start: str,
    changed_end: str,
) -> dict[str, Any] | None:
    """Read back a complete atomic symbol patch so resume need not rewrite it."""

    suffix = "day" if frequency == "day" else "1min"
    baseline_dir = baseline_component / "features" / symbol.lower()
    candidate_dir = candidate_component / "features" / symbol.lower()
    paths = [candidate_dir / f"{field}.{suffix}.bin" for field in ADJUSTED_FIELDS]
    if not all(path.is_file() for path in paths):
        return None
    if any(os.path.samefile(baseline_dir / path.name, path) for path in paths):
        return None
    _dates, cells = _mismatch_summary(
        candidate_dir / f"factor.{suffix}.bin",
        calendar,
        expected,
        day_spans=day_spans,
    )
    if cells:
        return None
    return {
        "symbol": symbol,
        "frequency": frequency,
        "changed_factor_cells": changed_cells,
        "changed_start": changed_start,
        "changed_end": changed_end,
        "resumed_from_complete_output": True,
        "files": [
            {"path": str(path), "sha256": sha256_file(path), "size": path.stat().st_size}
            for path in paths
        ],
    }


def patch_qlib_bins(
    *,
    connection: Any,
    baseline_root: Path,
    candidate_root: Path,
    inventory: Mapping[str, Any],
    start: date,
    cutoff: date,
    batch_size: int = 100,
    connection_factory: Callable[[], Any] | None = None,
    max_workers: int = 1,
) -> dict[str, Any]:
    validate_inventory(inventory)
    baseline = baseline_root.resolve(strict=True)
    candidate = candidate_root.resolve(strict=True)
    daily_baseline = baseline / "components" / "daily_bin_candidate"
    daily_candidate = candidate / "components" / "daily_bin_candidate"
    minute_baseline = baseline / "components" / "minute_bin_candidate"
    minute_candidate = candidate / "components" / "minute_bin_candidate"
    daily_calendar = _read_calendar(daily_baseline / "calendars" / "day.txt")
    minute_calendar = _read_calendar(minute_baseline / "calendars" / "1min.txt")
    daily_spans = _calendar_day_spans(daily_calendar)
    minute_spans = _calendar_day_spans(minute_calendar)
    records = list(inventory["records"])
    if max_workers < 1 or max_workers > 16:
        raise AdjFactorCandidateRepairError("max_workers must be between 1 and 16")
    if max_workers > 1 and connection_factory is None:
        raise AdjFactorCandidateRepairError("parallel bin repair requires a read-only connection factory")

    def patch_record(record: Mapping[str, Any]) -> list[dict[str, Any]]:
        worker_connection = connection_factory() if connection_factory is not None else connection
        try:
            symbol = str(record["symbol"])
            expected = _load_expected_for_symbols(worker_connection, [symbol], start, cutoff)[symbol]
            daily_existing = _existing_patch_receipt(
                baseline_component=daily_baseline,
                candidate_component=daily_candidate,
                symbol=symbol,
                frequency="day",
                calendar=daily_calendar,
                expected=expected,
                day_spans=daily_spans,
                changed_cells=int(record["daily_mismatch_count"]),
                changed_start=str(record["daily_start"]),
                changed_end=str(record["daily_end"]),
            )
            result = [
                daily_existing
                or _patch_one_frequency(
                    baseline_component=daily_baseline,
                    candidate_component=daily_candidate,
                    symbol=symbol,
                    frequency="day",
                    calendar=daily_calendar,
                    expected=expected,
                    connection=worker_connection,
                    day_spans=daily_spans,
                )
            ]
            if int(record.get("minute_mismatch_cell_count", 0)):
                minute_existing = _existing_patch_receipt(
                    baseline_component=minute_baseline,
                    candidate_component=minute_candidate,
                    symbol=symbol,
                    frequency="1min",
                    calendar=minute_calendar,
                    expected=expected,
                    day_spans=minute_spans,
                    changed_cells=int(record["minute_mismatch_cell_count"]),
                    changed_start=str(record["minute_start"]),
                    changed_end=str(record["minute_end"]),
                )
                result.append(
                    minute_existing
                    or _patch_one_frequency(
                        baseline_component=minute_baseline,
                        candidate_component=minute_candidate,
                        symbol=symbol,
                        frequency="1min",
                        calendar=minute_calendar,
                        expected=expected,
                        connection=worker_connection,
                        day_spans=minute_spans,
                    )
                )
            return result
        finally:
            if connection_factory is not None:
                worker_connection.close()

    if max_workers == 1:
        grouped_outputs = [patch_record(record) for record in records]
    else:
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="qe-adj-repair") as executor:
            grouped_outputs = list(executor.map(patch_record, records))
    outputs = [item for group in grouped_outputs for item in group]
    return {
        "symbol_frequency_count": len(outputs),
        "daily_symbol_count": sum(item["frequency"] == "day" for item in outputs),
        "minute_symbol_count": sum(item["frequency"] == "1min" for item in outputs),
        "changed_factor_cells": sum(int(item["changed_factor_cells"]) for item in outputs),
        "outputs": outputs,
    }


def load_expected_factor_frame(
    *,
    connection: Any,
    symbols: Sequence[str],
    start: date,
    cutoff: date,
):
    import pandas as pd

    frames = []
    for batch in _chunked(symbols, 200):
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT trade_date AS datetime,ts_code AS instrument,adj_factor
                FROM market.adj_factor
                WHERE ts_code=ANY(%s) AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date,ts_code
                """,
                (list(batch), start, cutoff),
            )
            rows = cursor.fetchall()
        frame = pd.DataFrame(rows, columns=["datetime", "instrument", "adj_factor"])
        if frame.empty:
            raise AdjFactorCandidateRepairError(f"production factor batch is empty: {batch[:3]}")
        frame["datetime"] = pd.to_datetime(frame["datetime"])
        frame["adj_factor"] = pd.to_numeric(frame["adj_factor"], errors="raise")
        frame["factor"] = frame["adj_factor"] / frame.groupby("instrument")["adj_factor"].transform("max")
        frames.append(frame[["datetime", "instrument", "factor"]])
    result = pd.concat(frames, ignore_index=True).set_index(["datetime", "instrument"])["factor"].sort_index()
    if result.index.duplicated().any() or result.isna().any() or bool((result <= 0).any()):
        raise AdjFactorCandidateRepairError("expected factor frame is invalid")
    return result


def rebuild_daily_pv_h5(
    *,
    baseline_path: Path,
    candidate_path: Path,
    baseline_daily_component: Path,
    corrected_daily_component: Path,
    affected_symbols: Sequence[str],
    chunksize: int = 200_000,
) -> dict[str, Any]:
    import pandas as pd

    calendar = _read_calendar(corrected_daily_component / "calendars" / "day.txt")
    corrections = []
    for symbol in affected_symbols:
        baseline_dir = baseline_daily_component / "features" / symbol.lower()
        corrected_dir = corrected_daily_component / "features" / symbol.lower()
        old_factor = _read_bin(baseline_dir / "factor.day.bin")
        new_factor = _read_bin(corrected_dir / "factor.day.bin")
        if len(old_factor) != len(new_factor) or int(old_factor[0]) != int(new_factor[0]):
            raise AdjFactorCandidateRepairError(f"daily factor bin shapes differ: {symbol}")
        changed = np.isfinite(old_factor[1:]) & (old_factor[1:] != new_factor[1:])
        indices = np.flatnonzero(changed)
        if not len(indices):
            raise AdjFactorCandidateRepairError(f"daily correction scope disappeared: {symbol}")
        start_offset = int(new_factor[0])
        frame = pd.DataFrame(
            {
                field: _read_bin(corrected_dir / f"{field}.day.bin")[1:][indices].astype("float64")
                for field in ADJUSTED_FIELDS
            },
            index=pd.MultiIndex.from_arrays(
                [
                    pd.to_datetime([calendar[start_offset + int(index)][:10] for index in indices]),
                    np.repeat(symbol, len(indices)),
                ],
                names=["datetime", "instrument"],
            ),
        )
        corrections.append(frame)
    corrected_rows = pd.concat(corrections).sort_index()
    if corrected_rows.index.duplicated().any():
        raise AdjFactorCandidateRepairError("daily bin corrections contain duplicate rows")
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = candidate_path.with_name(f".{candidate_path.name}.tmp.{os.getpid()}")
    temporary.unlink(missing_ok=True)
    changed_rows = 0
    rows = 0
    try:
        with pd.HDFStore(baseline_path, mode="r") as source, pd.HDFStore(temporary, mode="w") as target:
            for frame in source.select("data", chunksize=chunksize):
                rows += len(frame)
                replacement = corrected_rows.reindex(frame.index)
                mask = replacement["factor"].notna().to_numpy()
                if bool(mask.any()):
                    frame.loc[mask, list(ADJUSTED_FIELDS)] = replacement.loc[
                        mask, list(ADJUSTED_FIELDS)
                    ].to_numpy(dtype="float64")
                    changed_rows += int(mask.sum())
                target.append("data", frame, format="table", data_columns=["datetime", "instrument"], index=False)
        os.replace(temporary, candidate_path)
    finally:
        temporary.unlink(missing_ok=True)
    if changed_rows <= 0 or changed_rows > len(corrected_rows):
        raise AdjFactorCandidateRepairError("daily H5 correction intersection is invalid")
    return {
        "path": str(candidate_path),
        "rows": rows,
        "changed_rows": changed_rows,
        "daily_bin_correction_rows": len(corrected_rows),
        "daily_bin_rows_not_materialized_in_h5": len(corrected_rows) - changed_rows,
        "correction_source": "corrected_daily_qlib_bins",
        "sha256": sha256_file(candidate_path),
        "size": candidate_path.stat().st_size,
    }


def rebuild_static_factors(
    *,
    baseline_path: Path,
    candidate_path: Path,
    corrected_daily_h5: Path,
    affected_symbols: set[str],
) -> dict[str, Any]:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    with pd.HDFStore(corrected_daily_h5, mode="r") as store:
        close = store.select("data", columns=["close"])["close"]
    expected_strength = close.groupby(level="instrument").pct_change(10)
    source = pq.ParquetFile(baseline_path)
    temporary = candidate_path.with_name(f".{candidate_path.name}.tmp.{os.getpid()}")
    temporary.unlink(missing_ok=True)
    writer: pq.ParquetWriter | None = None
    changed_rows = 0
    rows = 0
    try:
        for index in range(source.num_row_groups):
            table = source.read_row_group(index)
            frame_index = pd.MultiIndex.from_arrays(
                [
                    table.column("datetime").to_pandas(),
                    table.column("instrument").to_pandas().astype(str),
                ],
                names=["datetime", "instrument"],
            )
            current = table.column("PriceStrength_10D").to_numpy(zero_copy_only=False).astype("float32")
            expected = expected_strength.reindex(frame_index)
            selected = frame_index.get_level_values("instrument").isin(affected_symbols) & expected.notna().to_numpy()
            replacement = current.copy()
            replacement[selected] = expected.to_numpy(dtype="float64", na_value=np.nan)[selected].astype("float32")
            changed = selected & ~np.isclose(current, replacement, rtol=0.0, atol=0.0, equal_nan=True)
            changed_rows += int(changed.sum())
            rows += table.num_rows
            field_index = table.schema.get_field_index("PriceStrength_10D")
            table = table.set_column(field_index, table.schema.field(field_index), pa.array(replacement, type=pa.float32()))
            if writer is None:
                writer = pq.ParquetWriter(temporary, table.schema, compression="snappy")
            writer.write_table(table, row_group_size=table.num_rows)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise AdjFactorCandidateRepairError("baseline static factors are empty")
    os.replace(temporary, candidate_path)
    return {
        "path": str(candidate_path),
        "rows": rows,
        "changed_rows": changed_rows,
        "source_daily_h5_sha256": sha256_file(corrected_daily_h5),
        "sha256": sha256_file(candidate_path),
        "size": candidate_path.stat().st_size,
    }


def verify_zero_factor_drift(
    *,
    connection: Any,
    candidate_root: Path,
    start: date,
    cutoff: date,
    symbols: Sequence[str],
    batch_size: int = 100,
) -> dict[str, Any]:
    daily = candidate_root / "components" / "daily_bin_candidate"
    minute = candidate_root / "components" / "minute_bin_candidate"
    daily_calendar = _read_calendar(daily / "calendars" / "day.txt")
    minute_calendar = _read_calendar(minute / "calendars" / "1min.txt")
    daily_spans = _calendar_day_spans(daily_calendar)
    minute_spans = _calendar_day_spans(minute_calendar)
    daily_mismatches = 0
    minute_mismatches = 0
    for batch in _chunked(tuple(symbols), batch_size):
        expected = _load_expected_for_symbols(connection, batch, start, cutoff)
        for symbol in batch:
            _daily_dates, daily_cells = _mismatch_summary(
                daily / "features" / symbol.lower() / "factor.day.bin",
                daily_calendar,
                expected[symbol],
                day_spans=daily_spans,
            )
            daily_mismatches += daily_cells
            minute_path = minute / "features" / symbol.lower() / "factor.1min.bin"
            if minute_path.is_file():
                _minute_dates, minute_cells = _mismatch_summary(
                    minute_path,
                    minute_calendar,
                    expected[symbol],
                    day_spans=minute_spans,
                )
                minute_mismatches += minute_cells
    return {
        "symbols": len(symbols),
        "daily_factor_mismatch_count": daily_mismatches,
        "minute_factor_mismatch_count": minute_mismatches,
        "status": "PASS" if daily_mismatches == 0 and minute_mismatches == 0 else "FAIL",
    }


def build_repair_receipt(
    *,
    baseline_root: Path,
    candidate_root: Path,
    inventory: Mapping[str, Any],
    clone: Mapping[str, Any],
    bins: Mapping[str, Any],
    daily_h5: Mapping[str, Any],
    static: Mapping[str, Any],
    verification: Mapping[str, Any],
    active_profile_path: Path,
    active_profile_sha256_before: str,
) -> dict[str, Any]:
    active_after = sha256_file(active_profile_path)
    if active_after != active_profile_sha256_before:
        raise AdjFactorCandidateRepairError("active profile changed during candidate repair")
    unsigned = {
        "schema_version": RECEIPT_SCHEMA,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "baseline_root": str(baseline_root.resolve(strict=True)),
        "candidate_root": str(candidate_root.resolve(strict=True)),
        "inventory_canonical_sha256": inventory["canonical_sha256"],
        "production_job_id": inventory["production_job_id"],
        "production_job_snapshot_sha256": inventory["production_job_snapshot_sha256"],
        "clone": dict(clone),
        "bin_repair": dict(bins),
        "daily_h5": dict(daily_h5),
        "static_factors": dict(static),
        "verification": dict(verification),
        "active_profile_path": str(active_profile_path.resolve(strict=True)),
        "active_profile_sha256": active_after,
        "baseline_modified": False,
        "database_write_performed": False,
        "production_activation": False,
        "runtime_action_performed": False,
        "qe_model_or_strategy_modified": False,
    }
    return {**unsigned, "canonical_sha256": sha256_bytes(canonical_json_bytes(unsigned))}


def build_dataset_manifest(
    *,
    candidate_root: Path,
    baseline_manifest: Mapping[str, Any],
    revision: str,
    component_replacements: Mapping[str, str],
    component_additions: Mapping[str, str],
    source_contract: Mapping[str, Any],
    availability_status: str = "LOCAL_CANDIDATE_VALIDATED_PENDING_NODE_SYNC",
) -> dict[str, Any]:
    """Build a self-verifying QE manifest from files already frozen in a candidate."""

    root = candidate_root.resolve(strict=True)
    components = dict(baseline_manifest.get("components") or {})
    for name, relative_path in {**component_replacements, **component_additions}.items():
        path = (root / relative_path).resolve(strict=True)
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise AdjFactorCandidateRepairError(f"manifest component escapes candidate: {relative_path}") from exc
        if not path.is_file():
            raise AdjFactorCandidateRepairError(f"manifest component is not a file: {relative_path}")
        components[name] = {
            "path": Path(relative_path).as_posix(),
            "sha256": sha256_file(path),
            "size": path.stat().st_size,
        }
    missing = [
        name
        for name, value in components.items()
        if isinstance(value, Mapping)
        and str(value.get("path") or "").strip()
        and not str(value["path"]).startswith("external:")
        and not (root / str(value["path"])).is_file()
    ]
    if missing:
        raise AdjFactorCandidateRepairError(f"manifest references missing components: {missing}")

    payload = {
        key: value
        for key, value in baseline_manifest.items()
        if key not in {"dataset_manifest_sha256", "deployment_content_sha256", "deployment_snapshot_id"}
    }
    payload.update(
        {
            "schema_version": "qe_dataset_manifest_v1",
            "availability_status": availability_status,
            "revision": revision,
            "components": components,
            "source_contract": dict(source_contract),
        }
    )
    deployment_content = {
        "release_id": payload["release_id"],
        "revision": revision,
        "cutoff_trade_date": payload["cutoff_trade_date"],
        "qlib_calendar_sha256": payload["qlib_calendar_sha256"],
        "qlib_instruments_sha256": payload["qlib_instruments_sha256"],
        "st_pit_manifest_sha256": payload["st_pit_manifest_sha256"],
        "components": components,
    }
    deployment_sha = sha256_bytes(canonical_json_bytes(deployment_content))
    payload["deployment_content_sha256"] = deployment_sha
    payload["deployment_snapshot_id"] = f"{payload['release_id']}_{deployment_sha[:16]}"
    payload["dataset_manifest_sha256"] = sha256_bytes(canonical_json_bytes(payload))
    return payload


__all__ = [
    "ADJUSTED_FIELDS",
    "AdjFactorCandidateRepairError",
    "build_repair_receipt",
    "build_dataset_manifest",
    "canonical_json_bytes",
    "clone_baseline_copy_on_write",
    "load_expected_factor_frame",
    "mutable_relative_paths",
    "patch_qlib_bins",
    "rebuild_daily_pv_h5",
    "rebuild_static_factors",
    "scan_inventory",
    "sha256_file",
    "validate_inventory",
    "verify_zero_factor_drift",
    "write_canonical_json_atomic",
]
