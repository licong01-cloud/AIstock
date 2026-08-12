"""Run RD-Agent Qlib writes in small, audited, restart-safe batches.

The monthly release path gives this wrapper an identity-bound manifest whose
batches contain at most twenty instruments.  Every upstream child runs
serially and every CSV cell is compared with all twelve generated Qlib fields
before the next batch starts.  Historical overrides use ``dump_fix`` for
*every* batch.  FULL rebuilds first receive one parent-seeded frozen global
calendar and instrument authority, then also use ``dump_fix`` for every batch;
no batch may discover a private calendar from its own listing-date subset.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import math
import mmap
import os
import re
import stat
import struct
import subprocess
import sys
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "dataset_release_qlib_batched_dump_manifest_v1"
RECEIPT_SCHEMA = "dataset_release_qlib_batched_dump_receipt_v1"
INFLIGHT_SCHEMA = "dataset_release_qlib_batched_dump_inflight_v1"
FAILURE_SCHEMA = "dataset_release_qlib_batched_dump_failure_v1"
RESOURCE_SIGNAL_SCHEMA = "dataset_release_resource_checkpoint_signal_v1"
RESOURCE_CHECKPOINT_EXIT = 75
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_RECEIPT_BYTES = 8 * 1024 * 1024
MAX_CODES_PER_BATCH = 20
MAX_BATCH_TIMEOUT_SECONDS = 1800
MAX_LINE_BYTES = 1024 * 1024
FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "factor",
    "up_limit_price",
    "down_limit_price",
    "prev_close",
    "limit_up",
    "limit_down",
)
_CODE = re.compile(r"^[0-9]{6}\.(?:SH|SZ|CSI)$")
_IDENTITY = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)


class BatchedDumpError(RuntimeError):
    """Manifest, child, output, or recovery evidence is invalid."""


class BatchedDumpTimeout(BatchedDumpError):
    """One bounded upstream child exceeded its declared timeout."""


class BatchedDumpCheckpoint(BatchedDumpError):
    """The resource guardian requested a retry at a safe batch boundary."""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--dump-script", required=True)
    parser.add_argument("--qlib-dir", required=True)
    parser.add_argument("--freq", required=True, choices=("day", "1min"))
    parser.add_argument("--max-workers", required=True, type=int)
    return parser


def _assert_plain(path: Path) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise BatchedDumpError(f"path is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or (int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise BatchedDumpError(f"path traverses symlink/reparse: {path}")


def _plain_file(path: Path, *, max_bytes: int | None = None) -> Path:
    requested = Path(path)
    if not requested.is_absolute():
        raise BatchedDumpError("file path must be absolute")
    resolved = requested.resolve(strict=True)
    _assert_plain(resolved)
    if not resolved.is_file():
        raise BatchedDumpError(f"plain file is required: {resolved}")
    if max_bytes is not None and not 0 < resolved.stat().st_size <= max_bytes:
        raise BatchedDumpError(f"file exceeds the bounded size contract: {resolved}")
    return resolved


def _plain_dir(path: Path, *, must_exist: bool = True) -> Path:
    requested = Path(path)
    if not requested.is_absolute():
        raise BatchedDumpError("directory path must be absolute")
    if must_exist:
        resolved = requested.resolve(strict=True)
        _assert_plain(resolved)
        if not resolved.is_dir():
            raise BatchedDumpError(f"plain directory is required: {resolved}")
        return resolved
    parent = requested.parent.resolve(strict=True)
    _assert_plain(parent)
    if requested.exists():
        return _plain_dir(requested)
    return parent / requested.name


def _contained(root: Path, relative: str, *, directory: bool = False) -> Path:
    candidate = Path(relative)
    if candidate.is_absolute() or not candidate.parts or ".." in candidate.parts or "\x00" in relative:
        raise BatchedDumpError("manifest relative path is unsafe")
    resolved = (root / candidate).resolve(strict=True)
    if root not in resolved.parents:
        raise BatchedDumpError("manifest relative path escapes its root")
    _assert_plain(resolved)
    if directory and not resolved.is_dir():
        raise BatchedDumpError("manifest batch path is not a directory")
    if not directory and not resolved.is_file():
        raise BatchedDumpError("manifest file path is not a file")
    return resolved


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _identity(payload: Mapping[str, Any]) -> str:
    value = {key: item for key, item in payload.items() if key != "manifest_identity"}
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load(path: Path) -> Mapping[str, Any]:
    manifest_path = _plain_file(path, max_bytes=MAX_MANIFEST_BYTES)
    try:
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BatchedDumpError("batch manifest is unreadable") from exc
    if not isinstance(value, Mapping) or value.get("schema_version") != SCHEMA:
        raise BatchedDumpError("batch manifest identity differs")
    if set(value) != {
        "schema_version",
        "manifest_identity",
        "dataset",
        "freq",
        "fields",
        "max_codes_per_batch",
        "per_batch_timeout_seconds",
        "resource_checkpoint_identity",
        "phases",
        "expected_total_code_writes",
        "expected_total_rows",
    }:
        raise BatchedDumpError("batch manifest schema differs")
    if value.get("manifest_identity") != _identity(value):
        raise BatchedDumpError("batch manifest digest differs")
    if value.get("dataset") not in {"daily_bin", "minute_bin"}:
        raise BatchedDumpError("batch manifest dataset is invalid")
    if value.get("freq") not in {"day", "1min"}:
        raise BatchedDumpError("batch manifest frequency is invalid")
    if tuple(value.get("fields") or ()) != FIELDS:
        raise BatchedDumpError("batch manifest field contract differs")
    max_codes = value.get("max_codes_per_batch")
    timeout = value.get("per_batch_timeout_seconds")
    if type(max_codes) is not int or not 0 < max_codes <= MAX_CODES_PER_BATCH:
        raise BatchedDumpError("batch code bound is invalid")
    if type(timeout) is not int or not 0 < timeout <= MAX_BATCH_TIMEOUT_SECONDS:
        raise BatchedDumpError("per-batch timeout is invalid")
    checkpoint = value.get("resource_checkpoint_identity")
    if (
        not isinstance(checkpoint, Mapping)
        or set(checkpoint) != {"attempt_id", "fence", "execution_id"}
        or not _IDENTITY.fullmatch(str(checkpoint.get("attempt_id", "")))
        or type(checkpoint.get("fence")) is not int
        or int(checkpoint["fence"]) <= 0
        or not _IDENTITY.fullmatch(str(checkpoint.get("execution_id", "")))
    ):
        raise BatchedDumpError("resource checkpoint identity is invalid")
    phases = value.get("phases")
    if not isinstance(phases, list) or not phases:
        raise BatchedDumpError("batch manifest phases are invalid")
    seen_phases: set[str] = set()
    seen_writes: set[tuple[str, str]] = set()
    total_writes = 0
    total_rows = 0
    observed_kinds: list[str] = []
    for phase in phases:
        if not isinstance(phase, Mapping) or set(phase) != {
            "phase_id",
            "kind",
            "batches",
        }:
            raise BatchedDumpError("batch phase schema differs")
        phase_id = str(phase.get("phase_id", ""))
        kind = str(phase.get("kind", ""))
        if not _IDENTITY.fullmatch(phase_id) or phase_id in seen_phases or kind not in {"tail", "override", "full"}:
            raise BatchedDumpError("batch phase identity differs")
        seen_phases.add(phase_id)
        observed_kinds.append(kind)
        batches = phase.get("batches")
        if not isinstance(batches, list) or not batches:
            raise BatchedDumpError("batch phase is empty")
        for ordinal, batch in enumerate(batches):
            if not isinstance(batch, Mapping) or set(batch) != {
                "ordinal",
                "role",
                "mode",
                "relative_path",
                "files",
            }:
                raise BatchedDumpError("batch entry schema differs")
            mode = str(batch.get("mode", ""))
            role = str(batch.get("role", ""))
            expected_mode = "dump_update" if kind == "tail" else "dump_fix"
            if batch.get("ordinal") != ordinal or mode != expected_mode or role not in {"stock", "index"}:
                raise BatchedDumpError("batch ordinal/mode differs from its phase")
            files = batch.get("files")
            if not isinstance(files, list) or not files or len(files) > max_codes:
                raise BatchedDumpError("batch file count exceeds its bound")
            for item in files:
                if not isinstance(item, Mapping) or set(item) != {
                    "code",
                    "role",
                    "relative_path",
                    "rows",
                    "sha256",
                    "start",
                    "end",
                }:
                    raise BatchedDumpError("batch file evidence schema differs")
                code = str(item.get("code", "")).upper()
                rows = item.get("rows")
                digest = str(item.get("sha256", ""))
                relative = str(item.get("relative_path", ""))
                if (
                    _CODE.fullmatch(code) is None
                    or item.get("role") != role
                    or type(rows) is not int
                    or rows <= 0
                    or _SHA256.fullmatch(digest) is None
                    or relative != f"{code.casefold()}.csv"
                    or (phase_id, code) in seen_writes
                    or not str(item.get("start", ""))
                    or str(item.get("end", "")) < str(item.get("start", ""))
                ):
                    raise BatchedDumpError("batch file evidence is invalid")
                seen_writes.add((phase_id, code))
                total_writes += 1
                total_rows += rows
    if "override" in observed_kinds and observed_kinds[-1] != "override":
        raise BatchedDumpError("override phase must be last")
    if observed_kinds.count("full") and observed_kinds != ["full"]:
        raise BatchedDumpError("full phase cannot be combined with patch phases")
    if value.get("expected_total_code_writes") != total_writes or value.get("expected_total_rows") != total_rows:
        raise BatchedDumpError("batch manifest totals differ")
    return value


def _calendar(path: Path) -> list[str]:
    calendar_path = _plain_file(path, max_bytes=128 * 1024 * 1024)
    rows: list[str] = []
    previous: str | None = None
    with calendar_path.open("r", encoding="utf-8", newline="") as handle:
        for ordinal, raw in enumerate(handle, start=1):
            if len(raw.encode("utf-8")) > MAX_LINE_BYTES:
                raise BatchedDumpError("Qlib calendar line exceeds memory bound")
            value = raw.strip()
            if not value or (previous is not None and value <= previous):
                raise BatchedDumpError(f"Qlib calendar is empty/duplicated/unordered: {ordinal}")
            rows.append(value)
            previous = value
    if not rows:
        raise BatchedDumpError("Qlib calendar is empty")
    return rows


def _scan_csv(path: Path, expected: Mapping[str, Any]) -> tuple[str, int, str, str]:
    csv_path = _plain_file(path)
    if _sha256(csv_path) != expected.get("sha256"):
        raise BatchedDumpError(f"batch CSV bytes differ: {csv_path.name}")
    code: str | None = None
    first: str | None = None
    previous: str | None = None
    rows = 0
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != ("date", "symbol", *FIELDS):
            raise BatchedDumpError(f"batch CSV header differs: {csv_path.name}")
        for row in reader:
            observed = str(row["symbol"]).strip().upper()
            timestamp = str(row["date"]).strip()
            if code is None:
                code = observed
            if (
                observed != code
                or _CODE.fullmatch(observed) is None
                or not timestamp
                or (previous is not None and timestamp <= previous)
            ):
                raise BatchedDumpError(f"batch CSV rows are mixed/duplicated/unordered: {csv_path.name}")
            first = first or timestamp
            previous = timestamp
            rows += 1
    if (
        code != str(expected.get("code", "")).upper()
        or rows != expected.get("rows")
        or first != expected.get("start")
        or previous != expected.get("end")
    ):
        raise BatchedDumpError(f"batch CSV identity differs: {csv_path.name}")
    assert first is not None and previous is not None and code is not None
    return code, rows, first, previous


def _float_file(path: Path) -> tuple[int, int]:
    feature = _plain_file(path)
    size = feature.stat().st_size
    if size < 8 or size % 4:
        raise BatchedDumpError(f"Qlib feature file is truncated: {feature}")
    with feature.open("rb") as handle:
        raw = handle.read(4)
    start = struct.unpack("<f", raw)[0]
    if not math.isfinite(start) or start < 0 or int(start) != start:
        raise BatchedDumpError(f"Qlib feature start header is invalid: {feature}")
    return int(start), size // 4


def _instrument_authority(qlib: Path, code: str) -> None:
    authorities = _plain_dir(qlib / "instruments")
    seen = False
    for path in sorted(authorities.iterdir(), key=lambda item: item.name.casefold()):
        _assert_plain(path)
        if not path.is_file() or path.suffix.casefold() != ".txt":
            raise BatchedDumpError("Qlib instruments contains a non-authority entry")
        if path.stat().st_size > 64 * 1024 * 1024:
            raise BatchedDumpError("Qlib instrument authority exceeds size bound")
        with path.open("r", encoding="utf-8") as handle:
            for raw in handle:
                if raw.split("\t", 1)[0].strip().upper() == code:
                    seen = True
                    break
        if seen:
            break
    if not seen:
        raise BatchedDumpError(f"Qlib instrument authority omitted batch code: {code}")


def _batch_entries(root: Path, batch: Mapping[str, Any]) -> tuple[Path, list[tuple[Path, Mapping[str, Any]]]]:
    batch_dir = _contained(root, str(batch["relative_path"]), directory=True)
    expected_names = {str(item["relative_path"]) for item in batch["files"]}
    observed_names: set[str] = set()
    for path in batch_dir.iterdir():
        _assert_plain(path)
        if not path.is_file() or path.suffix.casefold() != ".csv":
            raise BatchedDumpError("batch directory contains an extra/non-CSV entry")
        observed_names.add(path.name)
    if observed_names != expected_names:
        raise BatchedDumpError("batch directory files differ from manifest")
    entries: list[tuple[Path, Mapping[str, Any]]] = []
    for item in batch["files"]:
        path = _contained(batch_dir, str(item["relative_path"]))
        entries.append((path, item))
    return batch_dir, entries


def _before_inventory(
    qlib: Path,
    entries: Sequence[tuple[Path, Mapping[str, Any]]],
    *,
    freq: str,
) -> dict[tuple[str, str], tuple[int, int] | None]:
    suffix = "day" if freq == "day" else "1min"
    output: dict[tuple[str, str], tuple[int, int] | None] = {}
    for path, item in entries:
        code, _rows, _first, _last = _scan_csv(path, item)
        for field in FIELDS:
            feature = qlib / "features" / code.casefold() / f"{field}.{suffix}.bin"
            output[(code, field)] = _float_file(feature) if feature.exists() else None
    return output


def _audit_csv_values(
    path: Path,
    *,
    code: str,
    qlib: Path,
    suffix: str,
    positions: Mapping[str, int],
) -> None:
    with ExitStack() as stack:
        features: dict[str, tuple[int, mmap.mmap]] = {}
        for field in FIELDS:
            feature_path = qlib / "features" / code.casefold() / f"{field}.{suffix}.bin"
            start, _floats = _float_file(feature_path)
            handle = stack.enter_context(feature_path.open("rb"))
            view = stack.enter_context(mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ))
            features[field] = (start, view)
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for ordinal, row in enumerate(reader, start=2):
                timestamp = str(row["date"]).strip()
                position = positions.get(timestamp)
                if position is None:
                    raise BatchedDumpError(f"Qlib calendar omitted batch date: {code}:{timestamp}")
                for field, (start, view) in features.items():
                    offset = (position - start + 1) * 4
                    if offset < 4 or offset + 4 > len(view):
                        raise BatchedDumpError(f"Qlib feature coverage differs: {code}:{field}:{timestamp}")
                    actual = struct.unpack_from("<f", view, offset)[0]
                    try:
                        expected = float(row[field])
                    except (TypeError, ValueError) as exc:
                        raise BatchedDumpError(f"batch CSV value is invalid: {code}:{field}:{ordinal}") from exc
                    if not math.isclose(actual, expected, rel_tol=2e-6, abs_tol=1e-3):
                        raise BatchedDumpError(f"Qlib feature value differs: {code}:{field}:{timestamp}")


def _audit_batch(
    *,
    qlib: Path,
    entries: Sequence[tuple[Path, Mapping[str, Any]]],
    freq: str,
    before: Mapping[tuple[str, str], tuple[int, int] | None],
) -> dict[str, Any]:
    suffix = "day" if freq == "day" else "1min"
    calendar = _calendar(qlib / "calendars" / f"{suffix}.txt")
    positions = {value: ordinal for ordinal, value in enumerate(calendar)}
    rows = 0
    codes: list[str] = []
    for csv_path, evidence in entries:
        code, observed_rows, first, last = _scan_csv(csv_path, evidence)
        first_position = positions.get(first)
        last_position = positions.get(last)
        if first_position is None or last_position is None:
            raise BatchedDumpError(f"Qlib calendar omitted batch range: {code}")
        for field in FIELDS:
            feature = qlib / "features" / code.casefold() / f"{field}.{suffix}.bin"
            start, floats = _float_file(feature)
            expected_floats = last_position - start + 2
            if first_position < start or floats != expected_floats:
                raise BatchedDumpError(f"Qlib feature coverage/end differs after batch: {code}:{field}")
            previous = before.get((code, field))
            if previous is not None:
                previous_start, previous_floats = previous
                if start != previous_start or floats < previous_floats:
                    raise BatchedDumpError(f"Qlib feature append regressed: {code}:{field}")
        _audit_csv_values(
            csv_path,
            code=code,
            qlib=qlib,
            suffix=suffix,
            positions=positions,
        )
        _instrument_authority(qlib, code)
        rows += observed_rows
        codes.append(code)
    return {
        "codes": len(codes),
        "code_list": codes,
        "rows": rows,
        "calendar_rows": len(calendar),
        "calendar_end": calendar[-1],
    }


def _authority_snapshot(qlib: Path, *, freq: str) -> dict[str, str]:
    suffix = "day" if freq == "day" else "1min"
    paths = {
        "calendar": qlib / "calendars" / f"{suffix}.txt",
        "all": qlib / "instruments" / "all.txt",
    }
    index = qlib / "instruments" / "index.txt"
    if index.exists():
        paths["index"] = index
    return {name: _sha256(_plain_file(path)) for name, path in paths.items()}


def _instrument_authority_bytes(qlib: Path) -> dict[Path, bytes]:
    paths = [qlib / "instruments" / "all.txt"]
    index = qlib / "instruments" / "index.txt"
    if index.exists():
        paths.append(index)
    output: dict[Path, bytes] = {}
    for path in paths:
        plain = _plain_file(path, max_bytes=64 * 1024 * 1024)
        output[plain] = plain.read_bytes()
    return output


def _restore_instrument_authority(values: Mapping[Path, bytes]) -> None:
    for path, payload in values.items():
        temporary = path.parent / f".{path.name}.{os.getpid()}.authority.partial"
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def _batch_state(
    qlib: Path,
    entries: Sequence[tuple[Path, Mapping[str, Any]]],
    *,
    freq: str,
    hash_existing_features: bool,
) -> dict[str, Any]:
    suffix = "day" if freq == "day" else "1min"
    features: dict[str, Any] = {}
    for csv_path, evidence in entries:
        code, _rows, _first, _last = _scan_csv(csv_path, evidence)
        for field in FIELDS:
            path = qlib / "features" / code.casefold() / f"{field}.{suffix}.bin"
            key = f"{code}:{field}"
            if not path.exists():
                features[key] = None
                continue
            start, floats = _float_file(path)
            features[key] = {
                "start": start,
                "floats": floats,
                "size_bytes": path.stat().st_size,
                "sha256": _sha256(path) if hash_existing_features else None,
            }
    return {
        "features": features,
        "authority": _authority_snapshot(qlib, freq=freq),
    }


def _encoded_instrument_authority(qlib: Path) -> dict[str, str]:
    root = qlib.resolve(strict=True)
    return {
        path.relative_to(root).as_posix(): base64.b64encode(payload).decode("ascii")
        for path, payload in _instrument_authority_bytes(qlib).items()
    }


def _restore_encoded_instrument_authority(qlib: Path, values: Mapping[str, Any]) -> None:
    root = qlib.resolve(strict=True)
    decoded: dict[Path, bytes] = {}
    for relative, raw in values.items():
        candidate = Path(str(relative))
        if candidate.is_absolute() or ".." in candidate.parts:
            raise BatchedDumpError("inflight authority path is unsafe")
        path = (root / candidate).resolve(strict=True)
        if root not in path.parents:
            raise BatchedDumpError("inflight authority path escapes Qlib")
        try:
            decoded[path] = base64.b64decode(str(raw), validate=True)
        except ValueError as exc:
            raise BatchedDumpError("inflight authority bytes are invalid") from exc
    _restore_instrument_authority(decoded)


def _load_inflight(path: Path, *, manifest_identity: str) -> Mapping[str, Any] | None:
    if not path.exists():
        return None
    value = json.loads(_plain_file(path, max_bytes=MAX_RECEIPT_BYTES).read_text(encoding="utf-8"))
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != INFLIGHT_SCHEMA
        or value.get("manifest_identity") != manifest_identity
        or type(value.get("sequence")) is not int
        or not isinstance(value.get("before_state"), Mapping)
        or not isinstance(value.get("preserved_instrument_authority"), Mapping)
    ):
        raise BatchedDumpError("batch inflight journal identity differs")
    return value


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    if len(encoded) > MAX_RECEIPT_BYTES:
        raise BatchedDumpError("batch receipt exceeds size bound")
    temporary = path.parent / f".{path.name}.{os.getpid()}.partial"
    with temporary.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _load_progress(path: Path, *, manifest_identity: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    value = json.loads(_plain_file(path, max_bytes=MAX_RECEIPT_BYTES).read_text(encoding="utf-8"))
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != RECEIPT_SCHEMA
        or value.get("manifest_identity") != manifest_identity
        or value.get("status") not in {"IN_PROGRESS", "PASS"}
        or not isinstance(value.get("completed_batches"), list)
    ):
        raise BatchedDumpError("batch recovery receipt identity differs")
    return [dict(item) for item in value["completed_batches"]]


def _checkpoint_requested(manifest: Mapping[str, Any]) -> bool:
    supplied = os.environ.get("DATASET_RESOURCE_CHECKPOINT_FILE")
    if not supplied:
        return False
    path = Path(supplied)
    if not path.exists():
        return False
    signal = json.loads(_plain_file(path, max_bytes=64 * 1024).read_text(encoding="utf-8"))
    expected = manifest["resource_checkpoint_identity"]
    if (
        not isinstance(signal, Mapping)
        or signal.get("schema_version") != RESOURCE_SIGNAL_SCHEMA
        or signal.get("attempt_id") != expected["attempt_id"]
        or signal.get("fence") != expected["fence"]
        or signal.get("execution_id") != expected["execution_id"]
    ):
        raise BatchedDumpError("resource checkpoint signal identity differs")
    return True


def _receipt(
    manifest: Mapping[str, Any],
    completed: Sequence[Mapping[str, Any]],
    *,
    status: str,
) -> dict[str, Any]:
    peak_codes = max((int(item["codes"]) for item in completed), default=0)
    peak_rows = max((int(item["rows"]) for item in completed), default=0)
    return {
        "schema_version": RECEIPT_SCHEMA,
        "manifest_identity": manifest["manifest_identity"],
        "status": status,
        "dataset": manifest["dataset"],
        "completed_batches": [dict(item) for item in completed],
        "completed_batch_count": len(completed),
        "peak_codes_per_batch": peak_codes,
        "peak_rows_per_batch": peak_rows,
        "all_market_frames_retained": 0,
        "upstream_silent_code_failures": 0,
    }


def _flatten(manifest: Mapping[str, Any]) -> list[tuple[str, str, Mapping[str, Any]]]:
    return [
        (str(phase["phase_id"]), str(phase["kind"]), batch)
        for phase in manifest["phases"]
        for batch in phase["batches"]
    ]


def run(args: argparse.Namespace) -> Mapping[str, Any]:
    if type(args.max_workers) is not int or not 0 < args.max_workers <= 8:
        raise BatchedDumpError("max workers must be in [1,8]")
    manifest_path = _plain_file(Path(args.manifest), max_bytes=MAX_MANIFEST_BYTES)
    manifest = _load(manifest_path)
    if manifest["freq"] != args.freq:
        raise BatchedDumpError("command frequency differs from manifest")
    root = _plain_dir(manifest_path.parent)
    dump_script = _plain_file(Path(args.dump_script))
    qlib = _plain_dir(Path(args.qlib_dir))
    flattened = _flatten(manifest)
    receipt_path = root / "batched_dump_receipt.json"
    inflight_path = root / ".batched_dump_inflight.json"
    completed = _load_progress(
        receipt_path,
        manifest_identity=str(manifest["manifest_identity"]),
    )
    if len(completed) > len(flattened):
        raise BatchedDumpError("batch recovery receipt exceeds manifest")
    # Re-audit the completed prefix.  Later batches are allowed to extend the
    # shared calendar, but cannot change already-sealed per-code cells.
    for index, prior in enumerate(completed):
        phase_id, kind, batch = flattened[index]
        if (
            prior.get("phase_id") != phase_id
            or prior.get("phase_kind") != kind
            or prior.get("ordinal") != batch["ordinal"]
            or prior.get("role") != batch["role"]
            or prior.get("mode") != batch["mode"]
        ):
            raise BatchedDumpError("batch recovery prefix differs")
        _batch_dir, entries = _batch_entries(root, batch)
        _audit_batch(
            qlib=qlib,
            entries=entries,
            freq=args.freq,
            before={},
        )
    inflight = _load_inflight(
        inflight_path,
        manifest_identity=str(manifest["manifest_identity"]),
    )
    if inflight is not None:
        sequence = int(inflight["sequence"])
        if sequence >= len(flattened):
            raise BatchedDumpError("batch inflight sequence exceeds manifest")
        phase_id, kind, batch = flattened[sequence]
        if (
            inflight.get("phase_id") != phase_id
            or inflight.get("phase_kind") != kind
            or inflight.get("ordinal") != batch["ordinal"]
            or inflight.get("role") != batch["role"]
            or inflight.get("mode") != batch["mode"]
        ):
            raise BatchedDumpError("batch inflight identity differs")
        _batch_dir, entries = _batch_entries(root, batch)
        if sequence < len(completed):
            if sequence != len(completed) - 1:
                raise BatchedDumpError("stale inflight journal is not the receipt tail")
            inflight_path.unlink()
            inflight = None
        elif sequence > len(completed):
            raise BatchedDumpError("batch inflight journal skips the completed prefix")
        else:
            current_state = _batch_state(
                qlib,
                entries,
                freq=args.freq,
                hash_existing_features=kind == "override",
            )
            if current_state != inflight["before_state"]:
                preserved = inflight["preserved_instrument_authority"]
                if preserved:
                    _restore_encoded_instrument_authority(qlib, preserved)
                authority_before = inflight["before_state"].get("authority")
                if kind in {"override", "full"} and _authority_snapshot(qlib, freq=args.freq) != authority_before:
                    raise BatchedDumpError("recovered dump_fix authority differs from inflight baseline")
                summary = _audit_batch(
                    qlib=qlib,
                    entries=entries,
                    freq=args.freq,
                    before={},
                )
                completed.append(
                    {
                        "sequence": sequence,
                        "phase_id": phase_id,
                        "phase_kind": kind,
                        "ordinal": batch["ordinal"],
                        "role": batch["role"],
                        "mode": batch["mode"],
                        "instrument_authority_restored": bool(preserved),
                        "recovered_from_inflight": True,
                        **summary,
                    }
                )
                _atomic_json(
                    receipt_path,
                    _receipt(manifest, completed, status="IN_PROGRESS"),
                )
                inflight_path.unlink()
                inflight = None
                if _checkpoint_requested(manifest):
                    raise BatchedDumpCheckpoint("resource checkpoint requested after recovered audited batch")
    if len(completed) == len(flattened):
        value = _receipt(manifest, completed, status="PASS")
        _atomic_json(receipt_path, value)
        return value
    for index in range(len(completed), len(flattened)):
        phase_id, kind, batch = flattened[index]
        batch_dir, entries = _batch_entries(root, batch)
        before = _before_inventory(qlib, entries, freq=args.freq)
        authority_before = _authority_snapshot(qlib, freq=args.freq) if kind in {"override", "full"} else None
        preserved_instruments = (
            _instrument_authority_bytes(qlib) if kind in {"override", "full"} or batch["role"] == "index" else None
        )
        before_state = _batch_state(
            qlib,
            entries,
            freq=args.freq,
            # Tail writes must grow each feature and FULL starts from an empty
            # feature namespace.  Hashing their complete multi-year history
            # would add a market-wide read pass solely for crash detection.
            # Historical dump_fix is same-size and therefore retains the
            # stronger full-content before-state hash for its bounded codes.
            hash_existing_features=kind == "override",
        )
        journal = {
            "schema_version": INFLIGHT_SCHEMA,
            "manifest_identity": manifest["manifest_identity"],
            "sequence": index,
            "phase_id": phase_id,
            "phase_kind": kind,
            "ordinal": batch["ordinal"],
            "role": batch["role"],
            "mode": batch["mode"],
            "before_state": before_state,
            "preserved_instrument_authority": (
                _encoded_instrument_authority(qlib) if preserved_instruments is not None else {}
            ),
        }
        if inflight is None:
            _atomic_json(inflight_path, journal)
        elif dict(inflight) != journal:
            raise BatchedDumpError("existing inflight journal before-state differs")
        command = [
            sys.executable,
            str(dump_script),
            str(batch["mode"]),
            "--data_path",
            str(batch_dir),
            "--qlib_dir",
            str(qlib),
            "--freq",
            args.freq,
            "--date_field_name",
            "date",
            "--symbol_field_name",
            "symbol",
            "--exclude_fields",
            "date,symbol",
            "--max_workers",
            str(args.max_workers),
        ]
        try:
            child = subprocess.run(
                command,
                check=False,
                timeout=int(manifest["per_batch_timeout_seconds"]),
            )
        except subprocess.TimeoutExpired as exc:
            raise BatchedDumpTimeout(
                f"upstream Qlib dump timed out: phase={phase_id} batch={batch['ordinal']}"
            ) from exc
        if child.returncode != 0:
            raise BatchedDumpError(
                f"upstream Qlib dump failed: phase={phase_id} batch={batch['ordinal']} rc={child.returncode}"
            )
        if preserved_instruments is not None:
            _restore_instrument_authority(preserved_instruments)
        if authority_before is not None and _authority_snapshot(qlib, freq=args.freq) != authority_before:
            raise BatchedDumpError("dump_fix changed frozen calendar/instrument authority")
        summary = _audit_batch(
            qlib=qlib,
            entries=entries,
            freq=args.freq,
            before=before,
        )
        completed.append(
            {
                "sequence": index,
                "phase_id": phase_id,
                "phase_kind": kind,
                "ordinal": batch["ordinal"],
                "role": batch["role"],
                "mode": batch["mode"],
                "instrument_authority_restored": preserved_instruments is not None,
                "recovered_from_inflight": False,
                **summary,
            }
        )
        _atomic_json(
            receipt_path,
            _receipt(manifest, completed, status="IN_PROGRESS"),
        )
        inflight_path.unlink()
        inflight = None
        if _checkpoint_requested(manifest):
            raise BatchedDumpCheckpoint("resource checkpoint requested after an audited batch")
    value = _receipt(manifest, completed, status="PASS")
    _atomic_json(receipt_path, value)
    return value


def main() -> int:
    try:
        receipt = run(_parser().parse_args())
    except BatchedDumpCheckpoint as exc:
        print(
            json.dumps(
                {
                    "schema_version": FAILURE_SCHEMA,
                    "status": "RETRYABLE_CHECKPOINT",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                    "data_scope_changed": False,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return RESOURCE_CHECKPOINT_EXIT
    except Exception as exc:
        print(
            json.dumps(
                {
                    "schema_version": FAILURE_SCHEMA,
                    "status": "FAIL",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
