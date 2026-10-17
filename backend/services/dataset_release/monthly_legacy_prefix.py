"""Physical reuse of an immutable release prefix, without history re-audits.

This is a file-copy primitive, not a baseline attestation or a release-ready
receipt. The caller owns the month delta and the final manifest. Mutable files
are copied, never hardlinked, before any append/update writer is invoked.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import errno
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import stat
import struct
import time
from typing import Any, Callable, Mapping, Sequence

from .canonical import canonical_json_bytes
from .index_contract import DOMESTIC_INDEX_DEFINITIONS


class LegacyMonthlyPrefixError(RuntimeError):
    """The exact predecessor or private copy could not be safely opened."""


def is_monthly_provider_instrument(value: str, *, dataset: str) -> bool:
    if not isinstance(value, str):
        return False
    code = value.upper()
    return re.fullmatch(r"[0-9]{6}\.(SH|SZ)", code) is not None or (
        dataset == "daily_bin" and code in {item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS}
    )


@dataclass(frozen=True, slots=True)
class LegacyMonthlyPrefix:
    root: Path
    manifest: Mapping[str, Any]
    manifest_sha256: str
    manifest_file_sha256: str
    cutoff: date
    release_id: str


def _plain_chain(path: Path) -> None:
    for current in (path, *path.parents):
        if not current.exists():
            raise LegacyMonthlyPrefixError("legacy prefix path is unavailable")
        metadata = current.lstat()
        if stat.S_ISLNK(metadata.st_mode) or int(getattr(metadata, "st_file_attributes", 0)) & 0x0400:
            raise LegacyMonthlyPrefixError("legacy prefix path contains a link or reparse point")


def _relative(value: str) -> PurePosixPath:
    if not isinstance(value, str) or not value:
        raise LegacyMonthlyPrefixError("legacy prefix relative path is invalid")
    path = PurePosixPath(value)
    if (
        "\\" in value or ":" in value
        or "\x00" in value or path.is_absolute() or ".." in path.parts
        or path.as_posix() != value or not path.parts
    ):
        raise LegacyMonthlyPrefixError("legacy prefix relative path is invalid")
    return path


def _copy_exclusive(source: Path, destination: Path, checkpoint: Callable[[], None]) -> None:
    # copyfile would overwrite a destination created by another actor between
    # enumeration and copying. An append writer must receive a private file.
    with source.open("rb") as reader, destination.open("xb") as writer:
        last_pulse = time.monotonic()
        while block := reader.read(1024 * 1024):
            writer.write(block)
            now = time.monotonic()
            if now - last_pulse >= 2:
                checkpoint()
                last_pulse = now


def _signature(path: Path) -> tuple[int, int, int, int]:
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode):
        raise LegacyMonthlyPrefixError("legacy prefix entry must be a regular file")
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns


def load_legacy_prefix(
    root: Path, *, expected_manifest_sha256: str, expected_file_sha256: str,
    expected_cutoff: date, expected_release_id: str,
) -> LegacyMonthlyPrefix:
    """Read the controller-bound manifest only; do not hash component trees."""
    root = Path(os.path.abspath(root))
    manifest_path = root / "qe_dataset_manifest.json"
    _plain_chain(manifest_path)
    before = _signature(manifest_path)
    raw = manifest_path.read_bytes()
    if _signature(manifest_path) != before:
        raise LegacyMonthlyPrefixError("legacy prefix manifest changed during read")
    try:
        manifest = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise LegacyMonthlyPrefixError("legacy prefix manifest is unreadable") from exc
    if not isinstance(manifest, Mapping):
        raise LegacyMonthlyPrefixError("legacy prefix manifest is invalid")
    unsigned = dict(manifest)
    unsigned.pop("dataset_manifest_sha256", None)
    if (
        raw != canonical_json_bytes(manifest) + b"\n"
        or hashlib.sha256(raw).hexdigest() != expected_file_sha256
        or hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest() != expected_manifest_sha256
        or manifest.get("dataset_manifest_sha256") != expected_manifest_sha256
        or manifest.get("cutoff_trade_date") != expected_cutoff.isoformat()
        or manifest.get("release_id") != expected_release_id
        or not isinstance(manifest.get("components"), Mapping)
    ):
        raise LegacyMonthlyPrefixError("legacy prefix manifest identity differs")
    return LegacyMonthlyPrefix(root, manifest, expected_manifest_sha256, expected_file_sha256, expected_cutoff, expected_release_id)


def clone_legacy_prefix(
    prefix: LegacyMonthlyPrefix, target: Path, *, relative_roots: Sequence[str],
    mutable_paths: Sequence[str], checkpoint: Callable[[], None] = lambda: None,
    target_roots: Mapping[str, str] | None = None,
) -> Mapping[str, Any]:
    """Clone declared physical paths into a new task-owned staging directory.

    No old feature content is read for QA or hashing. Hardlink failure on a
    different volume falls back to copying bytes. A failure leaves only an
    unpublished task-owned staging tree; it never overwrites an old release.
    """
    prefix = load_legacy_prefix(
        prefix.root, expected_manifest_sha256=prefix.manifest_sha256,
        expected_file_sha256=prefix.manifest_file_sha256,
        expected_cutoff=prefix.cutoff, expected_release_id=prefix.release_id,
    )
    target = Path(os.path.abspath(target))
    if target.exists() or target.is_relative_to(prefix.root) or prefix.root.is_relative_to(target):
        raise LegacyMonthlyPrefixError("legacy prefix target must be new and separate from its source")
    _plain_chain(target.parent)
    roots = tuple(_relative(value) for value in relative_roots)
    if not roots:
        raise LegacyMonthlyPrefixError("legacy prefix physical scope is empty")
    folded = tuple(PurePosixPath(value.as_posix().casefold()) for value in roots)
    if any(left.is_relative_to(right) for i, left in enumerate(folded) for j, right in enumerate(folded) if i != j):
        raise LegacyMonthlyPrefixError("legacy prefix physical scopes overlap")
    if target_roots is None:
        destinations = roots
    else:
        if not isinstance(target_roots, Mapping) or set(target_roots) != set(relative_roots):
            raise LegacyMonthlyPrefixError("legacy prefix destination map differs from physical scope")
        destinations = tuple(_relative(target_roots[value.as_posix()]) for value in roots)
    folded_destinations = tuple(PurePosixPath(value.as_posix().casefold()) for value in destinations)
    if any(
        left.is_relative_to(right)
        for i, left in enumerate(folded_destinations)
        for j, right in enumerate(folded_destinations) if i != j
    ):
        raise LegacyMonthlyPrefixError("legacy prefix destination scopes overlap")
    mutable = tuple(_relative(value) for value in mutable_paths)
    if len({value.as_posix().casefold() for value in mutable}) != len(mutable):
        raise LegacyMonthlyPrefixError("legacy prefix mutation paths are duplicated")
    for value in (*roots, *mutable):
        _plain_chain(prefix.root / value)
    for value in mutable:
        if not any(value.is_relative_to(root) for root in roots) or not (prefix.root / value).is_file():
            raise LegacyMonthlyPrefixError("legacy prefix mutation path escaped its declared files")
    mutable_keys = {value.as_posix().casefold() for value in mutable}
    target.mkdir(exist_ok=False)
    copied = linked = byte_count = file_count = 0

    def copy_file(source: Path, destination: Path) -> None:
        nonlocal copied, linked, byte_count, file_count
        _plain_chain(source)
        before = _signature(source)
        relative = source.relative_to(prefix.root)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if relative.as_posix().casefold() in mutable_keys:
            _copy_exclusive(source, destination, checkpoint)
            copied += 1
        else:
            try:
                os.link(source, destination)
                linked += 1
            except OSError as exc:
                if exc.errno not in {errno.EXDEV, errno.EPERM, errno.EACCES, errno.ENOSYS, errno.ENOTSUP}:
                    raise
                _copy_exclusive(source, destination, checkpoint)
                copied += 1
        if _signature(source) != before or destination.stat().st_size != before[2]:
            raise LegacyMonthlyPrefixError("legacy prefix file changed during copy")
        byte_count += before[2]
        file_count += 1
        if file_count % 256 == 0:
            checkpoint()

    for relative, destination_relative in zip(roots, destinations, strict=True):
        selected = prefix.root / relative
        destination_root = target / destination_relative
        checkpoint()
        if selected.is_file():
            copy_file(selected, destination_root)
        elif selected.is_dir():
            for directory, subdirs, files in os.walk(selected, followlinks=False):
                current = Path(directory)
                _plain_chain(current)
                for child in subdirs:
                    _plain_chain(current / child)
                mapped_current = destination_root / current.relative_to(selected)
                mapped_current.mkdir(parents=True, exist_ok=True)
                for filename in files:
                    copy_file(current / filename, mapped_current / filename)
        else:
            raise LegacyMonthlyPrefixError("legacy prefix physical entry is unsupported")
    load_legacy_prefix(
        prefix.root, expected_manifest_sha256=prefix.manifest_sha256,
        expected_file_sha256=prefix.manifest_file_sha256,
        expected_cutoff=prefix.cutoff, expected_release_id=prefix.release_id,
    )
    checkpoint()
    return {
        "schema_version": "aistock_monthly_legacy_prefix_copy_v1",
        "predecessor_manifest_sha256": prefix.manifest_sha256,
        "predecessor_manifest_file_sha256": prefix.manifest_file_sha256,
        "predecessor_cutoff": prefix.cutoff.isoformat(),
        "relative_roots": [value.as_posix() for value in roots],
        "target_roots": {root.as_posix(): destination.as_posix() for root, destination in zip(roots, destinations, strict=True)},
        "mutable_paths": [value.as_posix() for value in mutable],
        "file_count": file_count, "byte_count": byte_count,
        "hardlinked_file_count": linked, "copied_file_count": copied,
        "full_content_revalidated": False, "publication_allowed": False,
        "database_read": False, "database_write": False,
    }


def prepare_legacy_qlib_append(
    prefix: LegacyMonthlyPrefix, target: Path, *, dataset: str,
    instruments: Sequence[str], fields: Sequence[str],
    checkpoint: Callable[[], None] = lambda: None,
) -> Mapping[str, Any]:
    """Prepare private targets for the existing shared Qlib append writer.

    This only prepares physical files. The caller must supply coherent QFQ
    bases, a calendar extension, frozen delta CSVs and any declared repairs;
    neither this receipt nor the old metadata declares those requirements met.
    No old CSV pathname embedded in export metadata is used as an authority.
    """
    component = {
        "daily_bin": ("day_meta_export", "day"),
        "minute_bin": ("minute_meta_export", "1min"),
    }.get(dataset)
    if component is None:
        raise LegacyMonthlyPrefixError("legacy Qlib append dataset is invalid")
    if (
        not instruments or not fields
        or any(not is_monthly_provider_instrument(code, dataset=dataset) for code in instruments)
        or len({code.casefold() for code in instruments}) != len(instruments)
        or any(not isinstance(field, str) or re.fullmatch(r"[a-z][a-z0-9_]*", field) is None for field in fields)
        or len(set(fields)) != len(fields)
    ):
        raise LegacyMonthlyPrefixError("legacy Qlib append writer targets are invalid")
    key, frequency = component
    pin = prefix.manifest["components"].get(key)
    if not isinstance(pin, Mapping):
        raise LegacyMonthlyPrefixError("legacy Qlib append metadata pin is unavailable")
    relative = _relative(pin.get("path"))
    metadata = prefix.root / relative
    _plain_chain(metadata)
    signature = _signature(metadata)
    if signature[2] > 8 * 1024 * 1024 or signature[2] != pin.get("size"):
        raise LegacyMonthlyPrefixError("legacy Qlib append metadata size differs")
    raw = metadata.read_bytes()
    if _signature(metadata) != signature or hashlib.sha256(raw).hexdigest() != pin.get("sha256"):
        raise LegacyMonthlyPrefixError("legacy Qlib append metadata identity differs")
    provider_relative = relative.parent
    if not provider_relative.parts:
        raise LegacyMonthlyPrefixError("legacy Qlib provider scope is invalid")
    provider = prefix.root / provider_relative
    calendar = provider / "calendars" / f"{frequency}.txt"
    _plain_chain(calendar)
    _signature(calendar)
    instrument_root = provider / "instruments"
    _plain_chain(instrument_root)
    if not instrument_root.is_dir():
        raise LegacyMonthlyPrefixError("legacy Qlib instruments scope is invalid")
    _plain_chain(instrument_root / "all.txt")
    _signature(instrument_root / "all.txt")
    feature_root = provider / "features"
    _plain_chain(feature_root)
    if not feature_root.is_dir():
        raise LegacyMonthlyPrefixError("legacy Qlib features scope is invalid")
    # All sidecars are private: the existing dump writer may update all.txt,
    # while release assembly also refreshes stock/index/benchmark PIT sidecars.
    mutable = [relative.as_posix(), calendar.relative_to(prefix.root).as_posix()]
    for path in instrument_root.iterdir():
        _plain_chain(path)
        _signature(path)
        mutable.append(path.relative_to(prefix.root).as_posix())
    existing_feature_count = new_instrument_count = 0
    for position, code in enumerate(instruments):
        if position % 128 == 0:
            checkpoint()
        directory = provider / "features" / code.casefold()
        if not directory.exists():
            new_instrument_count += 1
            continue
        _plain_chain(directory)
        if not directory.is_dir():
            raise LegacyMonthlyPrefixError("legacy Qlib instrument directory is invalid")
        for field in fields:
            feature = directory / f"{field}.{frequency}.bin"
            _plain_chain(feature)
            _signature(feature)
            mutable.append(feature.relative_to(prefix.root).as_posix())
            existing_feature_count += 1
    destination = f"{dataset}/qlib"
    receipt = clone_legacy_prefix(
        prefix, target, relative_roots=(provider_relative.as_posix(),),
        target_roots={provider_relative.as_posix(): destination},
        mutable_paths=tuple(mutable), checkpoint=checkpoint,
    )
    return {
        "schema_version": "aistock_monthly_legacy_qlib_append_preparation_v1",
        "dataset": dataset, "frequency": frequency,
        "qlib_relative_path": destination, "copy_receipt": dict(receipt),
        "instruments": list(instruments), "fields": list(fields),
        "existing_mutable_feature_file_count": existing_feature_count,
        "new_instrument_count": new_instrument_count,
        "publication_allowed": False, "append_performed": False,
        "database_read": False, "database_write": False,
    }


def read_legacy_qfq_anchors(
    prefix: LegacyMonthlyPrefix, *, dataset: str, instruments: Sequence[str],
    checkpoint: Callable[[], None] = lambda: None,
) -> Mapping[str, Mapping[str, Any]]:
    """Read the last real factor value as a construction seed, not a QA pass.

    The inherited manifest binds the native file. No historical feature hash
    or whole-series factor comparison is performed. The caller must bind each
    anchor date to a real raw adj_factor fact from sealed SOURCE; this helper
    never infers an account action or invents a raw denominator.
    """
    from .qlib_bounded_update import read_calendar

    key = {"daily_bin": "day_meta_export", "minute_bin": "minute_meta_export"}.get(dataset)
    if key is None:
        raise LegacyMonthlyPrefixError("legacy QFQ anchor dataset is invalid")
    frequency = "day" if dataset == "daily_bin" else "1min"
    pin = prefix.manifest["components"].get(key)
    if not isinstance(pin, Mapping):
        raise LegacyMonthlyPrefixError("legacy provider pin is unavailable")
    metadata = prefix.root / _relative(pin.get("path"))
    _plain_chain(metadata)
    if metadata.stat().st_size != pin.get("size") or metadata.stat().st_size > 8 * 1024 * 1024 or hashlib.sha256(metadata.read_bytes()).hexdigest() != pin.get("sha256"):
        raise LegacyMonthlyPrefixError("legacy metadata pin differs")
    provider = metadata.parent
    _plain_chain(provider / f"calendars/{frequency}.txt")
    calendar = read_calendar(provider / f"calendars/{frequency}.txt")
    if calendar[-1][:10] != prefix.cutoff.isoformat():
        raise LegacyMonthlyPrefixError("legacy calendar cutoff differs")
    result: dict[str, Mapping[str, Any]] = {}
    seen: set[str] = set()
    last_pulse = time.monotonic()
    checkpoint()
    for code in instruments:
        if not isinstance(code, str) or re.fullmatch(r"[0-9]{6}\.(SH|SZ)", code) is None or code in seen:
            raise LegacyMonthlyPrefixError("legacy QFQ anchor instrument is invalid")
        seen.add(code)
        if time.monotonic() - last_pulse >= 2:
            checkpoint()
            last_pulse = time.monotonic()
        path = provider / "features" / code.casefold() / f"factor.{frequency}.bin"
        if not path.exists():
            # A genuinely new physical instrument has no prefix to rescale.
            if (provider / "features" / code.casefold()).exists():
                raise LegacyMonthlyPrefixError("existing legacy instrument lacks its factor file")
            continue
        _plain_chain(path)
        before = _signature(path)
        size = before[2]
        if size < 8 or size % 4:
            raise LegacyMonthlyPrefixError("legacy factor file dimensions are invalid")
        value_count = size // 4 - 1
        anchor: tuple[int, float] | None = None
        with path.open("rb") as reader:
            offset = struct.unpack("<f", reader.read(4))[0]
            if not math.isfinite(offset) or offset != int(offset) or offset < 0 or offset + value_count > len(calendar):
                raise LegacyMonthlyPrefixError("legacy factor calendar offset is invalid")
            remaining = value_count
            while remaining and anchor is None:
                if time.monotonic() - last_pulse >= 2:
                    checkpoint()
                    last_pulse = time.monotonic()
                first = max(0, remaining - 1024)
                reader.seek(4 + first * 4)
                block = struct.unpack(f"<{remaining - first}f", reader.read((remaining - first) * 4))
                for position in range(len(block) - 1, -1, -1):
                    value = block[position]
                    if math.isnan(value):
                        continue
                    if not math.isfinite(value) or value <= 0:
                        raise LegacyMonthlyPrefixError("legacy factor anchor is invalid")
                    anchor = (int(offset) + first + position, value)
                    break
                remaining = first
        if _signature(path) != before or anchor is None:
            raise LegacyMonthlyPrefixError("legacy factor anchor is missing or changed during read")
        result[code] = {
            "trade_date": calendar[anchor[0]][:10], "timestamp": calendar[anchor[0]],
            "normalized_factor": anchor[1],
            "relative_path": path.relative_to(prefix.root).as_posix(),
            "predecessor_manifest_sha256": prefix.manifest_sha256,
        }
    checkpoint()
    return result


def append_legacy_qlib_month(
    prefix: LegacyMonthlyPrefix, staging_root: Path, *, dataset: str,
    target_cutoff: date, csv_root: Path, calendar_path: Path,
    instruments_all_path: Path, instruments: Sequence[str],
    source_receipt_sha256: str,
    qfq_basis_changes: Mapping[str, tuple[float, float]] | None = None,
    checkpoint: Callable[[], None] = lambda: None,
    progress: Callable[[Mapping[str, object]], None] = lambda _value: None,
) -> Mapping[str, Any]:
    """Append sealed canonical month CSVs through the existing shared writer.

    This emits real native Qlib bins in the shared monthly layout, not a second
    private data format. SOURCE and release validation stay with the caller.
    The old CSV lineage is neither required nor fabricated. Only explicit QFQ
    basis changes are applied; historical restatements require their own exact
    repair input and may not be represented as a scalar basis change.
    """
    # Local import avoids a cycle: the lower-level writer accepts the validated
    # prefix type but must not depend on monthly producer orchestration.
    from .qlib_bounded_update import extend_qlib_dataset, read_calendar
    from .stock_schema import QLIB_STOCK_FIELDS

    key = {"daily_bin": "day_meta_export", "minute_bin": "minute_meta_export"}.get(dataset)
    if key is None or not isinstance(source_receipt_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", source_receipt_sha256) is None:
        raise LegacyMonthlyPrefixError("monthly Qlib source identity is invalid")
    if not prefix.cutoff < target_cutoff or (prefix.cutoff.year, prefix.cutoff.month) >= (target_cutoff.year, target_cutoff.month):
        raise LegacyMonthlyPrefixError("monthly Qlib cutoff must advance to a new month")
    staging = Path(os.path.abspath(staging_root))
    _plain_chain(staging)
    if not staging.is_dir() or staging.is_relative_to(prefix.root) or prefix.root.is_relative_to(staging):
        raise LegacyMonthlyPrefixError("monthly Qlib staging must be separate from its immutable predecessor")
    _plain_chain(calendar_path)
    calendar = read_calendar(calendar_path)
    if calendar[-1][:10] != target_cutoff.isoformat():
        raise LegacyMonthlyPrefixError("monthly Qlib calendar target cutoff differs")
    for stamp in calendar:
        day = date.fromisoformat(stamp[:10])
        if day > prefix.cutoff and (day.year, day.month) != (target_cutoff.year, target_cutoff.month):
            raise LegacyMonthlyPrefixError("monthly Qlib tail contains dates outside the target month")
    pin = prefix.manifest["components"].get(key)
    if not isinstance(pin, Mapping):
        raise LegacyMonthlyPrefixError("monthly Qlib provider pin is unavailable")
    provider = prefix.root / _relative(pin.get("path")).parent
    component = staging / dataset
    component.mkdir(exist_ok=False)
    output = component / "qlib"
    receipt = extend_qlib_dataset(
        baseline_root=provider, target_root=output, csv_dir=csv_root,
        new_calendar_path=calendar_path,
        frequency="day" if dataset == "daily_bin" else "1min",
        instruments_all_path=instruments_all_path, allowed_fields=QLIB_STOCK_FIELDS,
        expected_instruments=instruments, inherited_prefix=prefix,
        qfq_basis_changes=qfq_basis_changes, datetime_field="date", checkpoint=checkpoint,
        progress=progress,
    )
    return {
        "schema_version": "aistock_monthly_legacy_qlib_append_v1",
        "dataset": dataset, "predecessor_manifest_sha256": prefix.manifest_sha256,
        "source_receipt_sha256": source_receipt_sha256,
        "target_cutoff": target_cutoff.isoformat(), "qlib_relative_path": f"{dataset}/qlib",
        "receipt": receipt, "append_performed": True,
        "historical_source_rows_read": 0, "historical_business_audit_performed": False,
        "publication_allowed": False, "database_read": False, "database_write": False,
    }
