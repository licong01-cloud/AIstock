"""Bounded-memory append/update writer for an immutable Qlib successor.

The upstream Qlib ``DumpDataUpdate`` implementation loads every CSV into one
process and cannot express multiple PIT spans.  This writer handles one stock
CSV at a time, creates private replacements for changed feature files and
commits the shared calendar only after all stock features are complete.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
import hashlib
import math
import os
from pathlib import Path
import shutil
import struct
import tempfile
from typing import Iterable, Mapping, Sequence


FLOAT32 = struct.Struct("<f")


class QlibBoundedUpdateError(RuntimeError):
    pass


def _is_link_or_junction(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _require_plain_existing_chain(path: Path, *, label: str) -> None:
    requested = path.expanduser().absolute()
    current = Path(requested.anchor)
    for part in requested.parts[1:]:
        current /= part
        if not current.exists():
            break
        if _is_link_or_junction(current):
            raise QlibBoundedUpdateError(f"{label} traverses a link or junction")


def _require_plain_tree(root: Path, *, label: str) -> None:
    """Reject link/reparse entries before copy-on-write traverses a baseline."""

    _require_plain_existing_chain(root, label=label)
    for directory, directory_names, file_names in os.walk(root, followlinks=False):
        parent = Path(directory)
        for name in (*directory_names, *file_names):
            child = parent / name
            if _is_link_or_junction(child):
                raise QlibBoundedUpdateError(f"{label} contains a link or junction")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_calendar(path: Path) -> tuple[str, ...]:
    if not path.is_file() or _is_link_or_junction(path):
        raise QlibBoundedUpdateError("calendar must be a regular non-link file")
    rows = tuple(line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    if not rows or len(rows) != len(set(rows)) or tuple(sorted(rows)) != rows:
        raise QlibBoundedUpdateError("calendar is empty, duplicated, or unordered")
    return rows


def validate_calendar_extension(old: Sequence[str], new: Sequence[str]) -> tuple[str, ...]:
    if len(new) <= len(old) or tuple(new[: len(old)]) != tuple(old):
        raise QlibBoundedUpdateError("new calendar must be a strict append-only extension")
    if len(new) != len(set(new)) or tuple(sorted(new)) != tuple(new):
        raise QlibBoundedUpdateError("new calendar is duplicated or unordered")
    return tuple(new[len(old) :])


@dataclass(frozen=True, slots=True)
class FeatureExtension:
    instrument: str
    feature: str
    source_path: Path | None
    target_path: Path
    values_by_index: Mapping[int, float]


def _read_feature_bounds(path: Path) -> tuple[int, int]:
    size = path.stat().st_size
    if size < FLOAT32.size or size % FLOAT32.size:
        raise QlibBoundedUpdateError(f"feature file has invalid byte length: {path}")
    with path.open("rb") as handle:
        raw = handle.read(FLOAT32.size)
    start_value = FLOAT32.unpack(raw)[0]
    if not math.isfinite(start_value) or int(start_value) != start_value or start_value < 0:
        raise QlibBoundedUpdateError(f"feature start index is invalid: {path}")
    start = int(start_value)
    value_count = size // FLOAT32.size - 1
    return start, start + value_count - 1


def write_feature_extension(extension: FeatureExtension, *, calendar_size: int) -> dict[str, object]:
    """Create one private feature file from an optional immutable predecessor."""

    if not extension.values_by_index:
        raise QlibBoundedUpdateError("feature extension is empty")
    ordered = sorted((int(index), float(value)) for index, value in extension.values_by_index.items())
    if any(math.isinf(value) for _, value in ordered):
        raise QlibBoundedUpdateError("feature extension contains infinity")
    if ordered[0][0] < 0 or ordered[-1][0] >= calendar_size:
        raise QlibBoundedUpdateError("feature extension index is outside calendar")
    if len(ordered) != len({index for index, _ in ordered}):
        raise QlibBoundedUpdateError("feature extension contains duplicate calendar indices")
    predecessor_sha = None
    if extension.source_path is not None:
        source = extension.source_path
        if not source.is_file() or _is_link_or_junction(source):
            raise QlibBoundedUpdateError("feature predecessor must be a regular non-link file")
        predecessor_sha = sha256_file(source)
        old_start, old_end = _read_feature_bounds(source)
        if ordered[0][0] <= old_end:
            raise QlibBoundedUpdateError("feature extension overlaps predecessor values")
        start_index = old_start
    else:
        old_end = ordered[0][0] - 1
        start_index = ordered[0][0]
    extension.target_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, raw = tempfile.mkstemp(
        prefix=f".{extension.target_path.name}.", suffix=".tmp", dir=extension.target_path.parent
    )
    temporary = Path(raw)
    written_values = 0
    try:
        with os.fdopen(descriptor, "wb") as target:
            if extension.source_path is not None:
                with extension.source_path.open("rb") as source:
                    shutil.copyfileobj(source, target, length=1024 * 1024)
            else:
                target.write(FLOAT32.pack(float(start_index)))
            next_index = old_end + 1
            for index, value in ordered:
                while next_index < index:
                    target.write(FLOAT32.pack(float("nan")))
                    next_index += 1
                    written_values += 1
                target.write(FLOAT32.pack(value))
                next_index += 1
                written_values += 1
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, extension.target_path)
    finally:
        temporary.unlink(missing_ok=True)
    if predecessor_sha is not None and sha256_file(extension.source_path) != predecessor_sha:
        raise QlibBoundedUpdateError("feature predecessor changed during private write")
    return {
        "instrument": extension.instrument,
        "feature": extension.feature,
        "start_index": start_index,
        "end_index": ordered[-1][0],
        "written_value_count": written_values,
        "predecessor_sha256": predecessor_sha,
        "target_sha256": sha256_file(extension.target_path),
        "target_size": extension.target_path.stat().st_size,
    }


def rewrite_feature_values(
    *,
    instrument: str,
    feature: str,
    source_path: Path,
    target_path: Path,
    values_by_index: Mapping[int, float],
    calendar_size: int,
) -> dict[str, object]:
    """Stream a selective historical rewrite into a private successor file."""

    if not values_by_index:
        raise QlibBoundedUpdateError("selective feature rewrite is empty")
    if not source_path.is_file() or _is_link_or_junction(source_path):
        raise QlibBoundedUpdateError("selective feature predecessor is invalid")
    source_sha = sha256_file(source_path)
    start, end = _read_feature_bounds(source_path)
    replacements = {int(index): float(value) for index, value in values_by_index.items()}
    if len(replacements) != len(values_by_index):
        raise QlibBoundedUpdateError("selective feature rewrite contains duplicate indices")
    if min(replacements) < start or max(replacements) >= calendar_size:
        raise QlibBoundedUpdateError("selective feature rewrite is outside the feature calendar")
    if any(math.isinf(value) for value in replacements.values()):
        raise QlibBoundedUpdateError("selective feature rewrite contains infinity")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, raw = tempfile.mkstemp(prefix=f".{target_path.name}.", suffix=".tmp", dir=target_path.parent)
    temporary = Path(raw)
    try:
        with source_path.open("rb") as source, os.fdopen(descriptor, "wb") as target:
            target.write(source.read(FLOAT32.size))
            index = start
            while index <= end:
                raw_value = source.read(FLOAT32.size)
                if len(raw_value) != FLOAT32.size:
                    raise QlibBoundedUpdateError("feature predecessor ended unexpectedly")
                target.write(FLOAT32.pack(replacements[index]) if index in replacements else raw_value)
                index += 1
            while index <= max(replacements):
                target.write(FLOAT32.pack(replacements.get(index, float("nan"))))
                index += 1
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, target_path)
    finally:
        temporary.unlink(missing_ok=True)
    if sha256_file(source_path) != source_sha:
        raise QlibBoundedUpdateError("feature predecessor changed during selective rewrite")
    return {
        "instrument": instrument,
        "feature": feature,
        "action": "SELECTIVE_REBUILD",
        "start_index": start,
        "end_index": max(end, max(replacements)),
        "rewritten_index_count": len(replacements),
        "predecessor_sha256": source_sha,
        "target_sha256": sha256_file(target_path),
        "target_size": target_path.stat().st_size,
    }


def _link_tree(source: Path, target: Path) -> None:
    if target.exists():
        raise FileExistsError(f"target Qlib root already exists: {target}")

    def link_or_copy(src: str, dst: str) -> str:
        try:
            os.link(src, dst)
        except OSError:
            shutil.copy2(src, dst)
        return dst

    shutil.copytree(source, target, copy_function=link_or_copy)


def _normalize_instrument(csv_path: Path) -> str:
    token = csv_path.stem.strip().lower()
    if not token or any(char not in "0123456789abcdefghijklmnopqrstuvwxyz._-" for char in token):
        raise QlibBoundedUpdateError(f"CSV instrument filename is invalid: {csv_path.name}")
    return token


def _parse_datetime(value: str, *, frequency: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError as exc:
        raise QlibBoundedUpdateError(f"CSV datetime is invalid: {value}") from exc
    if frequency == "day":
        return parsed.date().isoformat()
    if frequency == "1min":
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    raise QlibBoundedUpdateError("frequency must be day or 1min")


def extend_qlib_dataset(
    *,
    baseline_root: Path,
    target_root: Path,
    csv_dir: Path,
    new_calendar_path: Path,
    frequency: str,
    instruments_all_path: Path,
    allowed_fields: Iterable[str],
    expected_instruments: Iterable[str],
) -> dict[str, object]:
    """Build one immutable successor using one-stock-at-a-time CSV input."""

    if _is_link_or_junction(baseline_root):
        raise QlibBoundedUpdateError("baseline Qlib root is invalid")
    baseline = baseline_root.resolve(strict=True)
    if not baseline.is_dir():
        raise QlibBoundedUpdateError("baseline Qlib root is invalid")
    _require_plain_tree(baseline, label="baseline Qlib root")
    target = target_root.expanduser().absolute()
    _require_plain_existing_chain(target.parent, label="target Qlib parent")
    if target == baseline or target.is_relative_to(baseline) or baseline.is_relative_to(target):
        raise QlibBoundedUpdateError("target Qlib root must be separate from the baseline")
    if _is_link_or_junction(csv_dir):
        raise QlibBoundedUpdateError("CSV input root is invalid")
    csv_root = csv_dir.resolve(strict=True)
    if not csv_root.is_dir():
        raise QlibBoundedUpdateError("CSV input root is invalid")
    _require_plain_tree(csv_root, label="CSV input root")
    calendar_name = "day.txt" if frequency == "day" else "1min.txt"
    old_calendar_path = baseline / "calendars" / calendar_name
    old_calendar = read_calendar(old_calendar_path)
    new_calendar = read_calendar(new_calendar_path)
    appended = validate_calendar_extension(old_calendar, new_calendar)
    calendar_index = {value: index for index, value in enumerate(new_calendar)}
    fields = tuple(sorted({str(value).strip().lower() for value in allowed_fields if str(value).strip()}))
    if not fields:
        raise QlibBoundedUpdateError("allowed feature set is empty")
    expected = {str(value).strip().lower() for value in expected_instruments if str(value).strip()}
    if not expected:
        raise QlibBoundedUpdateError("expected instrument set is empty")
    csv_paths = sorted(csv_root.glob("*.csv"))
    csv_instruments = [_normalize_instrument(path) for path in csv_paths]
    if len(csv_instruments) != len(set(csv_instruments)) or set(csv_instruments) != expected:
        raise QlibBoundedUpdateError("CSV instrument population differs from the frozen expected set")
    _link_tree(baseline, target)
    feature_receipts: list[dict[str, object]] = []
    processed = 0
    try:
        for csv_path in csv_paths:
            instrument = _normalize_instrument(csv_path)
            if _is_link_or_junction(csv_path) or not csv_path.is_file():
                raise QlibBoundedUpdateError(f"CSV input must be a regular non-link file: {csv_path}")
            values: dict[str, dict[int, float]] = {field: {} for field in fields}
            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames is None or "datetime" not in reader.fieldnames:
                    raise QlibBoundedUpdateError(f"CSV lacks datetime: {csv_path}")
                actual_fields = set(reader.fieldnames).intersection(fields)
                if actual_fields != set(fields):
                    raise QlibBoundedUpdateError(f"CSV lacks required allowed features: {csv_path}")
                seen_indices: set[int] = set()
                for row in reader:
                    timestamp = _parse_datetime(str(row["datetime"]), frequency=frequency)
                    try:
                        index = calendar_index[timestamp]
                    except KeyError as exc:
                        raise QlibBoundedUpdateError(
                            f"CSV timestamp is absent from frozen calendar: {timestamp}"
                        ) from exc
                    if index < len(old_calendar):
                        raise QlibBoundedUpdateError("payload CSV overlaps frozen calendar prefix")
                    if index in seen_indices:
                        raise QlibBoundedUpdateError(f"CSV contains duplicate timestamp: {timestamp}")
                    seen_indices.add(index)
                    for field in actual_fields:
                        raw = str(row.get(field) or "").strip()
                        value = float(raw) if raw else float("nan")
                        if math.isinf(value):
                            raise QlibBoundedUpdateError(f"CSV feature is infinite: {instrument}.{field}")
                        values[field][index] = value
            if not seen_indices:
                raise QlibBoundedUpdateError(f"CSV contains no payload rows: {csv_path}")
            feature_dir = target / "features" / instrument
            baseline_dir = baseline / "features" / instrument
            for field in sorted(actual_fields):
                target_path = feature_dir / f"{field}.{frequency}.bin"
                source_path = baseline_dir / f"{field}.{frequency}.bin"
                receipt = write_feature_extension(
                    FeatureExtension(
                        instrument=instrument,
                        feature=field,
                        source_path=source_path if source_path.is_file() else None,
                        target_path=target_path,
                        values_by_index=values[field],
                    ),
                    calendar_size=len(new_calendar),
                )
                feature_receipts.append(receipt)
                if source_path.is_file() and os.path.samefile(source_path, target_path):
                    raise QlibBoundedUpdateError("changed feature still shares the predecessor inode")
            processed += 1
        if processed == 0:
            raise QlibBoundedUpdateError("CSV input root contains no stock files")
        _require_plain_existing_chain(instruments_all_path, label="PIT instruments sidecar")
        if _is_link_or_junction(instruments_all_path) or not instruments_all_path.is_file():
            raise QlibBoundedUpdateError("PIT instruments sidecar is empty or linked")
        instruments_payload = instruments_all_path.read_bytes()
        if not instruments_payload.strip():
            raise QlibBoundedUpdateError("PIT instruments sidecar is empty or linked")
        instruments_target = target / "instruments" / "all.txt"
        calendar_target = target / "calendars" / calendar_name
        for commit_target, payload in (
            (instruments_target, instruments_payload),
            (calendar_target, new_calendar_path.read_bytes()),
        ):
            descriptor, raw = tempfile.mkstemp(
                prefix=f".{commit_target.name}.", suffix=".tmp", dir=commit_target.parent
            )
            temporary = Path(raw)
            try:
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, commit_target)
            finally:
                temporary.unlink(missing_ok=True)
    except Exception:
        # A private, unpublished target may be removed by the caller's exact
        # operation cleanup.  Do not delete it here; it is resume evidence.
        raise
    return {
        "schema_version": "aistock_qlib_bounded_update_receipt_v1",
        "frequency": frequency,
        "baseline_root": str(baseline),
        "target_root": str(target),
        "calendar_prefix_count": len(old_calendar),
        "calendar_append_count": len(appended),
        "instrument_csv_count": processed,
        "feature_file_count": len(feature_receipts),
        "calendar_sha256": sha256_file(target / "calendars" / calendar_name),
        "instruments_sha256": sha256_file(target / "instruments" / "all.txt"),
        "feature_receipts": feature_receipts,
        "bounded_memory_unit": "one_instrument_csv",
        "baseline_mutated": False,
    }


__all__: Sequence[str] = (
    "FeatureExtension",
    "QlibBoundedUpdateError",
    "extend_qlib_dataset",
    "read_calendar",
    "rewrite_feature_values",
    "sha256_file",
    "validate_calendar_extension",
    "write_feature_extension",
)
