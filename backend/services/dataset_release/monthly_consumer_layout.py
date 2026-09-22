"""Publish the sealed monthly build under the shared QE/HMM consumer layout."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import os
from pathlib import Path
import stat
from typing import Any, Mapping, Sequence

from .canonical import canonical_json_bytes, digest_named_fields
from .factor_materializer import FACTOR_H5_DATASETS
from .index_contract import DOMESTIC_INDEX_DEFINITIONS, HMM_BENCHMARK_CODE
from .profile import DatasetProfile


CONSUMER_LAYOUT_RECEIPT_SCHEMA = "aistock_monthly_consumer_layout_v1"
QE_COVERAGE_SCHEMA = "qe_index_pool_coverage_receipt_v1"
FACTOR_META_SCHEMA = "qe_direct_factor_h5_static_v2"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_POOLS = {
    "stock_universe": "stock_universe.txt",
    "csi300": "index_pool__csi300.txt",
    "csi500": "index_pool__csi500.txt",
    "csi1000": "index_pool__csi1000.txt",
    "star50": "index_pool__star50.txt",
    "star100": "index_pool__star100.txt",
}


class MonthlyConsumerLayoutError(RuntimeError):
    """The internal monthly build cannot be published without byte copying."""


@dataclass(frozen=True, slots=True)
class ConsumerReleaseLayout:
    required_files: tuple[Path, ...]
    day_calendar_path: Path
    day_instruments_path: Path
    coverage_receipt_path: Path
    receipt_path: Path


def _is_link(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(metadata.st_mode) or bool(
        int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _content_ref(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "sha256",
        "size",
        "relative_path",
    }:
        raise MonthlyConsumerLayoutError(f"{label} is not a complete content reference")
    digest = value.get("sha256")
    size = value.get("size")
    relative_path = value.get("relative_path")
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or type(size) is not int
        or size < 0
        or not isinstance(relative_path, str)
        or not relative_path.startswith("cas/sha256/")
        or ".." in Path(relative_path).parts
    ):
        raise MonthlyConsumerLayoutError(f"{label} content reference is invalid")
    return dict(value)


def _plain_file(root: Path, relative: str, *, label: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise MonthlyConsumerLayoutError(f"{label} path is invalid")
    requested = root / path
    current = root
    for part in path.parts:
        current /= part
        if _is_link(current):
            raise MonthlyConsumerLayoutError(f"{label} traverses a link")
    try:
        resolved = requested.resolve(strict=True)
    except OSError as exc:
        raise MonthlyConsumerLayoutError(f"{label} is unavailable") from exc
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise MonthlyConsumerLayoutError(f"{label} must be a regular release file")
    return resolved


def _write_json(path: Path, value: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes(value) + b"\n"
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise MonthlyConsumerLayoutError(f"consumer target exists: {path}") from exc
    return path


def _write_bytes(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise MonthlyConsumerLayoutError(f"consumer target exists: {path}") from exc
    return path


def _hardlink(source: Path, target: Path) -> Path:
    if _is_link(source) or not source.is_file() or target.exists():
        raise MonthlyConsumerLayoutError("consumer hardlink source/target is invalid")
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(source, target)
    except OSError as exc:
        raise MonthlyConsumerLayoutError(
            "consumer layout requires a same-filesystem hardlink"
        ) from exc
    if not target.is_file() or _is_link(target) or not os.path.samefile(source, target):
        raise MonthlyConsumerLayoutError("consumer hardlink readback differs")
    return target


def _hardlink_tree(source: Path, target: Path) -> tuple[Path, ...]:
    if _is_link(source) or not source.is_dir() or target.exists():
        raise MonthlyConsumerLayoutError("consumer tree source/target is invalid")
    target.mkdir(parents=True, exist_ok=False)
    files: list[Path] = []
    for base, directories, names in os.walk(source):
        directories.sort()
        names.sort()
        base_path = Path(base)
        if _is_link(base_path):
            raise MonthlyConsumerLayoutError("consumer source tree contains a link")
        relative = base_path.relative_to(source)
        for directory in directories:
            child = base_path / directory
            if _is_link(child):
                raise MonthlyConsumerLayoutError("consumer source tree contains a link")
            (target / relative / directory).mkdir(exist_ok=False)
        for name in names:
            files.append(_hardlink(base_path / name, target / relative / name))
    if not files:
        raise MonthlyConsumerLayoutError("consumer source tree is empty")
    return tuple(files)


def _benchmark_payload(index_path: Path, *, cutoff: date) -> bytes:
    matches = []
    for raw in index_path.read_text(encoding="utf-8").splitlines():
        fields = raw.split("\t")
        if fields and fields[0].strip().upper() == HMM_BENCHMARK_CODE:
            matches.append(fields)
    expected_start = next(
        item.required_from for item in DOMESTIC_INDEX_DEFINITIONS if item.hmm_benchmark
    )
    if matches != [
        [HMM_BENCHMARK_CODE, expected_start.isoformat(), cutoff.isoformat()]
    ]:
        raise MonthlyConsumerLayoutError("daily benchmark authority differs")
    return ("\t".join(matches[0]) + "\n").encode("utf-8")


def _component_summary(root: Path, files: Sequence[Path]) -> dict[str, Any]:
    rows = {
        path.relative_to(root).as_posix(): {
            "sha256": _sha256(path),
            "size": path.stat().st_size,
        }
        for path in sorted(set(files), key=lambda item: item.relative_to(root).as_posix())
    }
    return {
        "file_count": len(rows),
        "logical_bytes": sum(int(item["size"]) for item in rows.values()),
        "content_digest": digest_named_fields(
            "aistock_monthly_consumer_component_v1", rows
        ),
    }


def publish_consumer_layout(
    *,
    root: Path,
    profile: DatasetProfile,
    cutoff: date,
    release_id: str,
    st_pit_manifest: Mapping[str, Any],
    validation_authority: Mapping[str, Any],
) -> ConsumerReleaseLayout:
    """Create the consumer tree as hardlinks to one validated internal build."""

    try:
        resolved = root.resolve(strict=True)
    except OSError as exc:
        raise MonthlyConsumerLayoutError("candidate root is unavailable") from exc
    if _is_link(root) or not resolved.is_dir() or not release_id.strip():
        raise MonthlyConsumerLayoutError("candidate or release identity is invalid")
    components_root = resolved / "components"
    if components_root.exists():
        if _is_link(components_root) or not components_root.is_dir():
            raise MonthlyConsumerLayoutError("consumer component root is invalid")
    else:
        components_root.mkdir(exist_ok=False)
    expected_validation_fields = {
        "validation_ref",
        "component_artifact_manifest_ref",
    }
    if set(validation_authority) != expected_validation_fields:
        raise MonthlyConsumerLayoutError("consumer validation authority is incomplete")
    validation_refs = {
        field: _content_ref(validation_authority[field], label=field)
        for field in sorted(expected_validation_fields)
    }

    day_root = components_root / "daily_bin_candidate"
    minute_root = components_root / "minute_bin_candidate"
    factor_root = components_root / "factor_h5_static_candidate_v2"
    index_root = components_root / "index_context"
    day_files = list(_hardlink_tree(resolved / "daily_bin" / "qlib", day_root))
    minute_files = list(
        _hardlink_tree(resolved / "minute_bin" / "qlib", minute_root)
    )

    factor_root.mkdir(exist_ok=False)
    factor_files = [
        _hardlink(
            _plain_file(resolved, f"factor_bundle/{dataset}.h5", label=dataset),
            factor_root / f"{dataset}.h5",
        )
        for dataset in FACTOR_H5_DATASETS
    ]
    factor_files.append(
        _hardlink(
            _plain_file(
                resolved,
                "factor_bundle/static_factors.parquet",
                label="static factors",
            ),
            factor_root / "static_factors.parquet",
        )
    )

    index_root.mkdir(exist_ok=False)
    index_data = _hardlink(
        _plain_file(resolved, "index_context/index_daily.h5", label="index H5"),
        index_root / "index_daily.h5",
    )
    index_files = [index_data]

    sidecars = st_pit_manifest.get("index_membership_sidecars")
    if (
        st_pit_manifest.get("cutoff_trade_date") != cutoff.isoformat()
        or st_pit_manifest.get("universe_key") != profile.universe_key
        or not isinstance(sidecars, Mapping)
        or set(sidecars) != set(_POOLS)
    ):
        raise MonthlyConsumerLayoutError("ST-PIT identity differs from consumer layout")
    stock_pin = sidecars["stock_universe"]
    if not isinstance(stock_pin, Mapping):
        raise MonthlyConsumerLayoutError("stock-universe pin is invalid")
    stock_source = _plain_file(
        resolved, str(stock_pin.get("path") or ""), label="stock universe"
    )
    if (
        _sha256(stock_source) != stock_pin.get("sha256")
        or stock_source.stat().st_size != stock_pin.get("size")
    ):
        raise MonthlyConsumerLayoutError("stock-universe bytes differ")
    stock_target = _hardlink(
        stock_source, day_root / "instruments" / "stock_universe.txt"
    )
    day_files.append(stock_target)
    benchmark = _write_bytes(
        day_root / "instruments" / "benchmark.txt",
        _benchmark_payload(day_root / "instruments" / "index.txt", cutoff=cutoff),
    )
    day_files.append(benchmark)

    identity = {
        "universe_key": profile.universe_key,
        "rule_version": str(st_pit_manifest.get("rule_version") or ""),
        "start": profile.start_date.isoformat(),
        "end": cutoff.isoformat(),
        "source_freeze": True,
        "full_history_content_hash": True,
    }
    if not identity["rule_version"]:
        raise MonthlyConsumerLayoutError("ST-PIT rule version is empty")
    day_meta = _write_json(
        day_root / "meta_export.json",
        {"snapshot_id": "daily_bin_candidate", **identity},
    )
    minute_meta = _write_json(
        minute_root / "meta_export.json",
        {"snapshot_id": "minute_bin_candidate", **identity},
    )
    factor_meta = _write_json(
        factor_root / "meta.json",
        {
            "schema_version": FACTOR_META_SCHEMA,
            "start": profile.start_date.isoformat(),
            "end": cutoff.isoformat(),
            "universe_key": profile.universe_key,
            "files": [f"{name}.h5" for name in FACTOR_H5_DATASETS]
            + ["static_factors.parquet"],
            "source_freeze": True,
            "full_history_content_hash": True,
        },
    )
    index_meta = _write_json(
        index_root / "meta.json",
        {
            "schema_version": "qe_index_context_v1",
            "start": profile.start_date.isoformat(),
            "end": cutoff.isoformat(),
            "codes": sorted(item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS),
            "source_freeze": True,
            "full_history_content_hash": True,
        },
    )
    day_files.append(day_meta)
    minute_files.append(minute_meta)
    factor_files.append(factor_meta)
    index_files.append(index_meta)

    coverage = _write_json(
        resolved / "reports" / "qe_index_pool_coverage_receipt.json",
        {
            "schema_version": QE_COVERAGE_SCHEMA,
            "release_id": release_id,
            "cutoff": cutoff.isoformat(),
            "pools": {
                pool_id: {
                    "available_start": profile.start_date.isoformat(),
                    "available_end": cutoff.isoformat(),
                    "gaps": [],
                }
                for pool_id in _POOLS
            },
        },
    )
    summaries = {
        "day": _component_summary(resolved, day_files),
        "minute": _component_summary(resolved, minute_files),
        "factor": _component_summary(resolved, factor_files),
        "index": _component_summary(resolved, index_files),
    }
    receipt_body = {
        "schema_version": CONSUMER_LAYOUT_RECEIPT_SCHEMA,
        "release_id": release_id,
        "cutoff": cutoff.isoformat(),
        "publication_mode": "same_filesystem_hardlink_v1",
        "components": summaries,
        "coverage_receipt": {
            "path": coverage.relative_to(resolved).as_posix(),
            "sha256": _sha256(coverage),
            "size": coverage.stat().st_size,
        },
        "validation_authority": {
            field: validation_refs[field] for field in sorted(validation_refs)
        },
        "source_freeze": True,
        "database_read": False,
        "database_write": False,
        "runtime_action": False,
    }
    receipt = _write_json(
        resolved / "reports" / "monthly_consumer_layout_receipt.json",
        {
            **receipt_body,
            "consumer_layout_digest": digest_named_fields(
                CONSUMER_LAYOUT_RECEIPT_SCHEMA, receipt_body
            ),
        },
    )
    required = tuple(
        sorted(
            {*day_files, *minute_files, *factor_files, *index_files, coverage, receipt},
            key=lambda path: path.relative_to(resolved).as_posix(),
        )
    )
    return ConsumerReleaseLayout(
        required_files=required,
        day_calendar_path=day_root / "calendars" / "day.txt",
        day_instruments_path=day_root / "instruments" / "all.txt",
        coverage_receipt_path=coverage,
        receipt_path=receipt,
    )


__all__: Sequence[str] = (
    "CONSUMER_LAYOUT_RECEIPT_SCHEMA",
    "ConsumerReleaseLayout",
    "MonthlyConsumerLayoutError",
    "publish_consumer_layout",
)
