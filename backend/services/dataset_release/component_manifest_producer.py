"""Produce exact component artifact lineage from one unpublished candidate.

The producer is deliberately filesystem/CAS only.  It never queries source
systems and never mutates candidate bytes; validation supplies the already
frozen artifact-ready and PIT authorities.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

from .canonical_stock_transformer import QfqDenominatorAuthority
from .canonical import digest_named_fields
from .canonical_lineage import (
    CANONICAL_LINEAGE_SCHEMA,
    is_lineage_v3,
    validate_lineage_descriptor,
)
from .cas_store import CASRef, CASStore
from .component_artifact_manifest import (
    COMPONENT_ARTIFACT_MANIFEST_SCHEMA,
    load_component_artifact_manifest,
    seal_component_artifact_manifest,
)
from .contracts import Component
from .errors import DatasetReleaseError
from .index_contract import DOMESTIC_INDEX_DEFINITIONS
from .index_context_candidate_manifest import (
    INDEX_CONTEXT_CANDIDATE_MANIFEST_SCHEMA,
    validate_index_context_candidate_manifest,
)
from .mixed_planner import pit_span_digest_by_code
from .pit import FrozenPitSnapshot
from .profile import DatasetProfile
from .stock_schema import QLIB_STOCK_FIELDS


class ComponentManifestProductionError(DatasetReleaseError):
    code = "BLOCKED_COMPONENT_MANIFEST_PRODUCTION_INVALID"


_COMPONENT_ROOTS = {
    Component.DAILY_BIN: "daily_bin",
    Component.MINUTE_BIN: "minute_bin",
    Component.FACTOR_H5_STATIC: "factor_bundle",
    Component.DOMESTIC_INDEX_CONTEXT: "index_context",
}
_DATASETS = {
    Component.DAILY_BIN: (
        "trading_calendar",
        "kline_daily_raw",
        "adj_factor",
        "stk_limit",
        "suspend_d",
        "index_daily_merged",
    ),
    Component.MINUTE_BIN: (
        "trading_calendar",
        "kline_minute_raw",
        "kline_daily_raw",
        "adj_factor",
        "stk_limit",
        "suspend_d",
    ),
    Component.FACTOR_H5_STATIC: (
        "kline_daily_raw",
        "adj_factor",
        "daily_basic",
        "moneyflow_ts",
        "bak_basic",
        "cyq_perf",
        "sector_data",
        "margin_detail",
    ),
    Component.DOMESTIC_INDEX_CONTEXT: ("trading_calendar", "index_daily"),
}
_STOCK = re.compile(r"^[0-9]{6}\.(?:SH|SZ)$")
_INDEX = re.compile(r"^[0-9]{6}\.(?:SH|SZ|CSI)$")
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_COMMITTED_MARKER = ".dataset_release_committed.json"
_ZERO_SAFETY = {
    "database_writes": 0,
    "provider_database_writes": 0,
    "candidate_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
}


@dataclass(frozen=True, slots=True)
class CandidateArtifactFileSnapshot:
    relative_path: str
    size_bytes: int
    sha256: str
    device: int
    inode: int
    file_index: int | None
    mtime_ns: int
    ctime_ns: int
    file_attributes: int

    @property
    def stat_identity(self) -> tuple[int, int, int | None, int, int, int, int]:
        return (
            self.device,
            self.inode,
            self.file_index,
            self.size_bytes,
            self.mtime_ns,
            self.ctime_ns,
            self.file_attributes,
        )


@dataclass(frozen=True, slots=True)
class CandidateArtifactSnapshot:
    root: Path
    artifact_root: str
    files: tuple[CandidateArtifactFileSnapshot, ...]
    file_count: int
    total_bytes: int
    content_read_passes: int = 1

    def receipt(self) -> dict[str, Any]:
        return {
            "schema_version": "dataset_release_candidate_artifact_snapshot_v1",
            "artifact_root": self.artifact_root,
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "content_read_passes": self.content_read_passes,
            "identity_readbacks": 2,
        }


def snapshot_candidate_artifacts(candidate_root: Path) -> CandidateArtifactSnapshot:
    """Read every candidate byte once using the publisher v1 root encoding."""

    requested = Path(candidate_root).absolute()
    _assert_plain_chain(requested)
    root = requested.resolve(strict=True)
    if not root.is_dir():
        raise ComponentManifestProductionError("candidate artifact root is not a directory")
    aggregate = hashlib.sha256()
    records: list[CandidateArtifactFileSnapshot] = []
    total_bytes = 0
    for base, directories, filenames in os.walk(root):
        directories.sort()
        filenames.sort()
        base_path = Path(base)
        _assert_plain_node(base_path)
        for directory in directories:
            _assert_plain_node(base_path / directory)
        for filename in filenames:
            if filename == _COMMITTED_MARKER or filename.startswith(".committed."):
                continue
            path = base_path / filename
            before = _file_stat(path)
            digest, observed_size = _hash_file_once(path)
            after = _file_stat(path)
            if before != after or observed_size != before[3]:
                raise ComponentManifestProductionError("candidate artifact changed during its content snapshot")
            relative_text = path.relative_to(root).as_posix()
            relative = relative_text.encode("utf-8")
            row = len(relative).to_bytes(4, "big") + relative + observed_size.to_bytes(8, "big") + bytes.fromhex(digest)
            aggregate.update(len(row).to_bytes(4, "big"))
            aggregate.update(row)
            records.append(
                CandidateArtifactFileSnapshot(
                    relative_path=relative_text,
                    size_bytes=observed_size,
                    sha256=digest,
                    device=before[0],
                    inode=before[1],
                    file_index=before[2],
                    mtime_ns=before[4],
                    ctime_ns=before[5],
                    file_attributes=before[6],
                )
            )
            total_bytes += observed_size
    if not records:
        raise ComponentManifestProductionError("candidate artifact tree is empty")
    return CandidateArtifactSnapshot(
        root=root,
        artifact_root=aggregate.hexdigest(),
        files=tuple(records),
        file_count=len(records),
        total_bytes=total_bytes,
    )


def produce_component_artifact_manifest(
    cas: CASStore,
    *,
    candidate_root: Path,
    profile: DatasetProfile,
    scope: str,
    cutoff: date,
    candidate_identity: str,
    artifact_root: str,
    producer_fingerprint: str,
    artifact_fingerprint: str,
    validation_fingerprint: str,
    source_content_root: str,
    artifact_ready_content_root: str,
    pit_snapshot: FrozenPitSnapshot,
    source_partitions: Mapping[Component, Sequence[Mapping[str, Any]]],
    qfq_authority: QfqDenominatorAuthority,
    require_index_context_candidate_manifest: bool = False,
    artifact_snapshot: CandidateArtifactSnapshot | None = None,
) -> CASRef:
    """Seal a sharded, exact-file component manifest and read it back."""

    root = Path(candidate_root).resolve(strict=True)
    if not root.is_dir() or cutoff != pit_snapshot.cutoff:
        raise ComponentManifestProductionError("candidate/PIT identity differs")
    snapshot = artifact_snapshot or snapshot_candidate_artifacts(root)
    _verify_candidate_artifact_snapshot(root, snapshot)
    if snapshot.artifact_root != artifact_root:
        raise ComponentManifestProductionError("candidate artifact root differs")
    candidate_metadata: dict[str, Mapping[str, Any]] = {}
    if require_index_context_candidate_manifest:
        index_metadata = validate_index_context_candidate_manifest(
            root,
            profile=profile,
            cutoff=cutoff,
            pit_snapshot_digest=pit_snapshot.spans_sha256,
            producer_fingerprint=producer_fingerprint,
            artifact_fingerprint=artifact_fingerprint,
            validation_fingerprint=validation_fingerprint,
            expected_source_content_root=source_content_root,
            expected_artifact_ready_content_root=artifact_ready_content_root,
            max_rows=profile.resource_policy.validation_read_chunk_rows,
        )
        metadata = _snapshot_file(snapshot, "metadata/index_context_manifest.json")
        candidate_metadata["metadata/index_context_manifest.json"] = {
            "schema_version": INDEX_CONTEXT_CANDIDATE_MANIFEST_SCHEMA,
            "manifest_identity": index_metadata["manifest_identity"],
            "sha256": metadata.sha256,
            "size_bytes": metadata.size_bytes,
        }
    for dataset in ("daily_bin", "minute_bin"):
        relative = f"{dataset}/materialization_receipt.json"
        try:
            receipt_file = _snapshot_file(snapshot, relative)
        except ComponentManifestProductionError:
            # Unit-level producers may be used before the candidate layout is
            # complete. The validated release path always supplies receipts;
            # absence simply leaves the baseline on the legacy planner path.
            continue
        if receipt_file.size_bytes > 4 * 1024 * 1024:
            raise ComponentManifestProductionError(f"{dataset} materialization receipt exceeds bounded metadata size")
        receipt_path = root / Path(relative)
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ComponentManifestProductionError(f"{dataset} materialization receipt is unreadable") from exc
        sealed = receipt.get("sealed_canonical_rows") if isinstance(receipt, Mapping) else None
        if not isinstance(sealed, Mapping):
            raise ComponentManifestProductionError(f"{dataset} materialization receipt lacks canonical authority")
        if is_lineage_v3(sealed):
            validated = validate_lineage_descriptor(root / dataset, sealed)
            schema_version = CANONICAL_LINEAGE_SCHEMA
            manifest_identity = str(validated["lineage_root"])
        else:
            schema_version = str(sealed.get("schema_version", ""))
            manifest_identity = digest_named_fields(
                "dataset_release_legacy_canonical_receipt_identity_v1",
                dict(sealed),
            )
        candidate_metadata[relative] = {
            "schema_version": schema_version,
            "manifest_identity": manifest_identity,
            "sha256": receipt_file.sha256,
            "size_bytes": receipt_file.size_bytes,
        }
    stock_codes = tuple(sorted({span.ts_code for span in pit_snapshot.spans}))
    qfq_codes = tuple(code for code, _value in qfq_authority.values)
    if stock_codes != qfq_codes:
        raise ComponentManifestProductionError("component QFQ/PIT code sets differ")
    pit_digests = _pit_span_digests(pit_snapshot)
    components: dict[str, Any] = {}
    for component in Component:
        relative_root = _COMPONENT_ROOTS[component]
        component_root = (root / relative_root).resolve(strict=True)
        if root not in component_root.parents or not component_root.is_dir():
            raise ComponentManifestProductionError(f"candidate component root is missing: {component.value}")
        files = _artifact_files(snapshot, relative_root)
        if not files:
            raise ComponentManifestProductionError(f"candidate component is empty: {component.value}")
        instruments = (
            tuple(item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS)
            if component is Component.DOMESTIC_INDEX_CONTEXT
            else stock_codes
        )
        source = [dict(item) for item in source_partitions.get(component, ())]
        if not source:
            raise ComponentManifestProductionError(f"component source evidence is empty: {component.value}")
        shared = _mutable_shared_targets(files)
        instrument_targets = _mutable_instrument_targets(files)
        append_rule = _mutation_rule(
            component,
            files=files,
            shared_targets=shared,
            rule_id="monthly-tail",
        )
        pit_rule = (
            None
            if component is Component.DOMESTIC_INDEX_CONTEXT
            else _mutation_rule(
                component,
                files=files,
                shared_targets=shared,
                rule_id="pit-change",
            )
        )
        adj = (
            None
            if component is Component.DOMESTIC_INDEX_CONTEXT
            else {
                "complete": True,
                "qfq_denominator_by_code": {code: str(value) for code, value in qfq_authority.values},
                "ordered_adj_digest_by_code": {code: digest for code, _rows, digest in qfq_authority.per_code_series},
                "adj_row_count_by_code": {code: rows for code, rows, _digest in qfq_authority.per_code_series},
                "monthly_ordered_adj_by_code": {},
                "writer_targets_by_code": {
                    code: list(instrument_targets[code]) for code in stock_codes if code in instrument_targets
                },
                "shared_writer_targets": list(shared),
                "writer_target_policy": "artifact_file_instrument_index_v1",
            }
        )
        components[component.value] = {
            "status": "COMPLETE",
            "component": component.value,
            "component_root_relative_path": relative_root,
            "source_partitions": source,
            "artifact_partitions": [
                {
                    "partition_key": "full-profile",
                    "source_partition_identities": sorted(str(item["identity"]) for item in source),
                    "dependency_edges": [f"{dataset}->{component.value}" for dataset in _DATASETS[component]],
                    "instruments": list(instruments),
                    "start": profile.start_date.isoformat(),
                    "end": cutoff.isoformat(),
                    "files": files,
                }
            ],
            "append_rules": [append_rule],
            "pit_mutation_rule": pit_rule,
            "pit_instruments": ([] if component is Component.DOMESTIC_INDEX_CONTEXT else list(stock_codes)),
            "pit_span_digest_by_code": ({} if component is Component.DOMESTIC_INDEX_CONTEXT else pit_digests),
            "adj_series": adj,
        }
    _verify_candidate_artifact_snapshot(root, snapshot)
    reference = seal_component_artifact_manifest(
        cas,
        {
            "schema_version": COMPONENT_ARTIFACT_MANIFEST_SCHEMA,
            "profile": profile.profile,
            "scope": scope,
            "cutoff": cutoff.isoformat(),
            "candidate_identity": candidate_identity,
            "artifact_root": artifact_root,
            "semantic_profile_digest": profile.semantic_profile_digest,
            "producer_fingerprint": producer_fingerprint,
            "artifact_fingerprint": artifact_fingerprint,
            "validation_fingerprint": validation_fingerprint,
            "source_content_root": source_content_root,
            "artifact_ready_content_root": artifact_ready_content_root,
            "pit_snapshot_digest": pit_snapshot.spans_sha256,
            "components": components,
            **({"candidate_metadata": candidate_metadata} if candidate_metadata else {}),
            "safety": dict(_ZERO_SAFETY),
        },
    )
    loaded = load_component_artifact_manifest(cas, reference)
    if (
        loaded.candidate_identity != candidate_identity
        or loaded.artifact_root != artifact_root
        or loaded.artifact_ready_content_root != artifact_ready_content_root
        or any(not loaded.component(component).complete for component in Component)
    ):
        raise ComponentManifestProductionError("component manifest readback differs")
    return reference


def _hash_file_once(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


def _file_stat(path: Path) -> tuple[int, int, int | None, int, int, int, int]:
    _assert_plain_node(path)
    value = os.stat(path, follow_symlinks=False)
    if not stat.S_ISREG(value.st_mode):
        raise ComponentManifestProductionError("candidate artifact contains a non-regular file")
    inode = int(value.st_ino)
    return (
        int(value.st_dev),
        inode,
        inode if os.name == "nt" else None,
        int(value.st_size),
        int(value.st_mtime_ns),
        int(value.st_ctime_ns),
        int(getattr(value, "st_file_attributes", 0)),
    )


def _assert_plain_node(path: Path) -> None:
    try:
        value = os.lstat(path)
    except OSError as exc:
        raise ComponentManifestProductionError("candidate artifact path is unavailable") from exc
    if stat.S_ISLNK(value.st_mode) or (int(getattr(value, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise ComponentManifestProductionError("candidate artifact traverses a link/reparse point")


def _assert_plain_chain(path: Path) -> None:
    absolute = Path(path).absolute()
    current = Path(absolute.anchor)
    if current.exists():
        _assert_plain_node(current)
    for part in absolute.parts[1:]:
        current /= part
        _assert_plain_node(current)


def _observed_snapshot_stats(
    root: Path,
) -> dict[str, tuple[int, int, int | None, int, int, int, int]]:
    observed: dict[str, tuple[int, int, int | None, int, int, int, int]] = {}
    for base, directories, filenames in os.walk(root):
        directories.sort()
        filenames.sort()
        base_path = Path(base)
        _assert_plain_node(base_path)
        for directory in directories:
            _assert_plain_node(base_path / directory)
        for filename in filenames:
            if filename == _COMMITTED_MARKER or filename.startswith(".committed."):
                continue
            path = base_path / filename
            relative = path.relative_to(root).as_posix()
            if relative in observed:
                raise ComponentManifestProductionError("candidate artifact path is duplicated")
            observed[relative] = _file_stat(path)
    return observed


def _verify_candidate_artifact_snapshot(
    root: Path,
    snapshot: CandidateArtifactSnapshot,
) -> None:
    resolved = Path(root).resolve(strict=True)
    if (
        snapshot.root != resolved
        or snapshot.content_read_passes != 1
        or snapshot.file_count != len(snapshot.files)
        or snapshot.total_bytes != sum(item.size_bytes for item in snapshot.files)
        or snapshot.artifact_root != _artifact_root_from_records(snapshot.files)
    ):
        raise ComponentManifestProductionError("candidate artifact snapshot identity differs")
    expected = {item.relative_path: item for item in snapshot.files}
    if len(expected) != len(snapshot.files):
        raise ComponentManifestProductionError("candidate artifact snapshot paths are duplicated")
    observed = _observed_snapshot_stats(resolved)
    if tuple(observed) != tuple(expected):
        raise ComponentManifestProductionError("candidate artifact snapshot path set changed")
    if any(observed[relative] != expected[relative].stat_identity for relative in expected):
        raise ComponentManifestProductionError("candidate artifact snapshot stat identity changed")


def verify_candidate_artifact_snapshot(
    root: Path,
    snapshot: CandidateArtifactSnapshot,
) -> None:
    """Verify snapshot path/stat identity without rereading candidate content."""

    _verify_candidate_artifact_snapshot(root, snapshot)


def _artifact_root_from_records(
    files: Sequence[CandidateArtifactFileSnapshot],
) -> str:
    aggregate = hashlib.sha256()
    for item in files:
        candidate = Path(item.relative_path)
        if (
            item.size_bytes < 0
            or candidate.is_absolute()
            or not candidate.parts
            or ".." in candidate.parts
            or len(item.sha256) != 64
        ):
            raise ComponentManifestProductionError("candidate artifact snapshot file record is invalid")
        try:
            digest = bytes.fromhex(item.sha256)
        except ValueError as exc:
            raise ComponentManifestProductionError("candidate artifact snapshot file digest is invalid") from exc
        if len(digest) != 32:
            raise ComponentManifestProductionError("candidate artifact snapshot file digest is invalid")
        relative = item.relative_path.encode("utf-8")
        row = len(relative).to_bytes(4, "big") + relative + item.size_bytes.to_bytes(8, "big") + digest
        aggregate.update(len(row).to_bytes(4, "big"))
        aggregate.update(row)
    return aggregate.hexdigest()


def _snapshot_file(
    snapshot: CandidateArtifactSnapshot,
    relative_path: str,
) -> CandidateArtifactFileSnapshot:
    matches = [item for item in snapshot.files if item.relative_path == relative_path]
    if len(matches) != 1:
        raise ComponentManifestProductionError("candidate metadata is absent from the artifact snapshot")
    return matches[0]


def _artifact_files(
    snapshot: CandidateArtifactSnapshot,
    component_root: str,
) -> list[dict[str, Any]]:
    prefix = f"{component_root}/"
    files: list[dict[str, Any]] = []
    for item in snapshot.files:
        if not item.relative_path.startswith(prefix):
            continue
        relative = item.relative_path.removeprefix(prefix)
        files.append(
            {
                "relative_path": relative,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
                "instrument": _file_instrument(relative),
            }
        )
    return files


def _file_instrument(relative: str) -> str | None:
    parts = relative.split("/")
    candidates: list[str] = []
    if "features" in parts:
        position = parts.index("features")
        if position + 1 < len(parts):
            candidates.append(parts[position + 1].upper())
    if "csv" in parts or "csv_deltas" in parts or "csv_overrides" in parts or "index_csv" in parts:
        candidates.append(Path(parts[-1]).stem.upper())
    valid = [value for value in candidates if _INDEX.fullmatch(value)]
    if len(set(valid)) > 1:
        raise ComponentManifestProductionError(f"artifact file instrument is ambiguous: {relative}")
    return valid[0] if valid else None


def _mutation_rule(
    component: Component,
    *,
    files: Sequence[Mapping[str, Any]],
    shared_targets: Sequence[str],
    rule_id: str,
) -> dict[str, Any]:
    instrument_targets = _mutable_instrument_targets(files)
    templates: list[str] = []
    if component in {Component.DAILY_BIN, Component.MINUTE_BIN}:
        suffix = "day" if component is Component.DAILY_BIN else "1min"
        templates = ["csv/{instrument}.csv"] + [
            f"qlib/features/{{instrument}}/{field}.{suffix}.bin" for field in QLIB_STOCK_FIELDS
        ]
    if component in {
        Component.DOMESTIC_INDEX_CONTEXT,
    }:
        replace = [str(item["relative_path"]) for item in files]
    elif component is Component.FACTOR_H5_STATIC:
        replace = [str(item["relative_path"]) for item in files if "/" not in str(item["relative_path"])]
    else:
        replace = list(shared_targets)
    return {
        "rule_id": rule_id,
        "datasets": list(_DATASETS[component]),
        "replace_existing_targets": sorted(replace),
        "create_new_targets": [],
        "create_target_templates": templates,
        "writer_targets_by_instrument": {code: list(paths) for code, paths in instrument_targets.items()},
        "writer_target_policy": "artifact_file_instrument_index_v1",
        "dependency_edges": [f"{dataset}.{rule_id}->{component.value}" for dataset in _DATASETS[component]],
    }


def _instrument_targets(
    files: Sequence[Mapping[str, Any]],
) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for item in files:
        instrument = item.get("instrument")
        if instrument is not None:
            grouped.setdefault(str(instrument).upper(), []).append(str(item["relative_path"]))
    return {code: tuple(sorted(set(paths))) for code, paths in sorted(grouped.items())}


def _mutable_instrument_targets(
    files: Sequence[Mapping[str, Any]],
) -> dict[str, tuple[str, ...]]:
    """Return actual future writer targets, excluding immutable CSV lineage."""

    return _instrument_targets([item for item in files if not _immutable_lineage_path(item["relative_path"])])


def _mutable_shared_targets(
    files: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    """Return shared mutable artifacts, never historical segment partitions."""

    return tuple(
        sorted(
            str(item["relative_path"])
            for item in files
            if item["instrument"] is None and not _immutable_lineage_path(item["relative_path"])
        )
    )


def _immutable_lineage_path(relative_path: object) -> bool:
    normalized = str(relative_path).replace("\\", "/").casefold()
    return (
        normalized.startswith("csv_deltas/")
        or normalized.startswith("csv_overrides/")
        or normalized.startswith("csv_lineage/")
        or normalized.startswith("partitions/")
    )


def _pit_span_digests(snapshot: FrozenPitSnapshot) -> dict[str, str]:
    return pit_span_digest_by_code(snapshot)


__all__ = [
    "CandidateArtifactSnapshot",
    "ComponentManifestProductionError",
    "produce_component_artifact_manifest",
    "snapshot_candidate_artifacts",
    "verify_candidate_artifact_snapshot",
]
