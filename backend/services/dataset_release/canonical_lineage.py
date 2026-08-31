"""Bounded candidate-local canonical CSV lineage.

The legacy daily/minute receipts repeat every active CSV segment in each new
monthly receipt.  This module stores immutable event objects below the
candidate component instead.  A small descriptor points at a persistent
per-code head index, while each event writes only its changed code buckets.

All paths are component-root relative.  Existing event objects are immutable;
idempotent retries may only re-observe byte-identical objects at the same path.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping, Sequence

from .canonical import (
    canonical_json_bytes,
    digest_named_fields,
    ensure_sha256,
    merkle_root_from_named_digests,
    normalize_root_relative_path,
    sha256_hex,
)
from .errors import DatasetReleaseError


CANONICAL_LINEAGE_SCHEMA = "dataset_release_sealed_qlib_csv_lineage_v3"
CANONICAL_LINEAGE_CAPABILITY = "candidate_local_persistent_code_head_merkle_v3"
CANONICAL_LINEAGE_REF_SCHEMA = "dataset_release_canonical_lineage_ref_v1"
CANONICAL_LINEAGE_EVENT_SCHEMA = "dataset_release_canonical_lineage_event_v3"
CANONICAL_LINEAGE_CHANGE_BUCKET_SCHEMA = "dataset_release_canonical_lineage_change_bucket_v3"
CANONICAL_LINEAGE_HEAD_BUCKET_SCHEMA = "dataset_release_canonical_lineage_head_bucket_v3"
CANONICAL_LINEAGE_HEAD_INDEX_SCHEMA = "dataset_release_canonical_lineage_head_index_v3"
CANONICAL_LINEAGE_NODE_SCHEMA = "dataset_release_canonical_lineage_node_v3"
CANONICAL_LINEAGE_SEGMENT_SCHEMA = "dataset_release_canonical_lineage_segment_v1"
CANONICAL_LINEAGE_NAMESPACE_MANIFEST_SCHEMA = "dataset_release_csv_segment_manifest_v3"
CANONICAL_LINEAGE_EVENT_KEY_SCHEMA = "dataset_release_canonical_lineage_event_key_v1"
CANONICAL_LINEAGE_ROOT_SCHEMA = "dataset_release_canonical_lineage_root_v3"
CANONICAL_LINEAGE_ACTIVE_ROOT_SCHEMA = "dataset_release_canonical_lineage_active_segments_v1"
CANONICAL_LINEAGE_INVENTORY_ROOT_SCHEMA = "dataset_release_canonical_lineage_inventory_v1"

LINEAGE_BUCKET_COUNT = 256
MAX_LINEAGE_OBJECT_BYTES = 4 * 1024 * 1024
MAX_LINEAGE_DESCRIPTOR_BYTES = 128 * 1024

_CODE = re.compile(r"^[0-9]{6}\.(?:SH|SZ|CSI)$")
_STOCK_CODE = re.compile(r"^[0-9]{6}\.(?:SH|SZ)$")
_KEY = re.compile(r"^[0-9a-f]{32}$")
_BUCKET = re.compile(r"^[0-9a-f]{2}$")
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_EVENT_KINDS = {"GENESIS", "LEGACY_ANCHOR", "APPEND", "SELECTIVE"}
_NODE_KINDS = {"GENESIS", "ANCHOR", "CREATE", "APPEND", "OVERRIDE"}


class CanonicalLineageError(DatasetReleaseError):
    code = "BLOCKED_CANONICAL_LINEAGE_INVALID"


@dataclass(frozen=True, slots=True)
class LineageRef:
    relative_path: str
    size_bytes: int
    sha256: str
    identity: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "relative_path": self.relative_path,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "identity": self.identity,
        }

    @classmethod
    def from_value(cls, value: Mapping[str, Any]) -> "LineageRef":
        if set(value) != {"relative_path", "size_bytes", "sha256", "identity"}:
            raise CanonicalLineageError("lineage reference fields differ")
        relative = _relative(value.get("relative_path"), label="lineage reference")
        size = value.get("size_bytes")
        if isinstance(size, bool) or not isinstance(size, int) or not 0 < size <= MAX_LINEAGE_OBJECT_BYTES:
            raise CanonicalLineageError("lineage reference size is invalid")
        sha = ensure_sha256(str(value.get("sha256", "")), field="lineage_ref_sha256")
        expected = digest_named_fields(
            CANONICAL_LINEAGE_REF_SCHEMA,
            {"relative_path": relative, "size_bytes": size, "sha256": sha},
        )
        if value.get("identity") != expected:
            raise CanonicalLineageError("lineage reference identity differs")
        return cls(relative, size, sha, expected)


@dataclass(frozen=True, slots=True)
class LineageWriteResult:
    descriptor: Mapping[str, Any]
    created_paths: tuple[str, ...]
    event_ref: LineageRef
    event_key: str
    inventory_roots: Mapping[str, str]


def is_lineage_v3(value: Mapping[str, Any]) -> bool:
    return value.get("schema_version") == CANONICAL_LINEAGE_SCHEMA


def lineage_bucket(instrument: str) -> str:
    code = _instrument(instrument)
    return hashlib.sha256(code.encode("ascii")).hexdigest()[:2]


def lineage_event_key(
    *,
    dataset: str,
    cutoff: str,
    action: str,
    baseline_identity: str,
    mutation_identity: str,
) -> str:
    _dataset(dataset)
    if not str(cutoff).strip() or not str(action).strip():
        raise CanonicalLineageError("lineage event cutoff/action is invalid")
    return digest_named_fields(
        CANONICAL_LINEAGE_EVENT_KEY_SCHEMA,
        {
            "dataset": dataset,
            "cutoff": str(cutoff),
            "action": str(action),
            "baseline_identity": str(baseline_identity),
            "mutation_identity": str(mutation_identity),
            "capability": CANONICAL_LINEAGE_CAPABILITY,
        },
    )[:32]


def lineage_event_relative_path(event_key: str, *, anchor: bool = False) -> str:
    key = _event_key(event_key)
    namespace = "anchors" if anchor else "events"
    return f"csv_lineage/{namespace}/{key}/event.json"


def planned_lineage_paths(*, event_key: str, instruments: Sequence[str], anchor: bool = False) -> tuple[str, ...]:
    key = _event_key(event_key)
    namespace = "anchors" if anchor else "events"
    base = f"csv_lineage/{namespace}/{key}"
    buckets = sorted({lineage_bucket(value) for value in instruments})
    return tuple(
        sorted(
            {
                f"{base}/event.json",
                f"{base}/head_index.json",
                *(f"{base}/changes/{bucket}.json" for bucket in buckets),
                *(f"{base}/heads/{bucket}.json" for bucket in buckets),
            }
        )
    )


def legacy_active_segments(value: Mapping[str, Any], *, dataset: str) -> tuple[dict[str, Any], ...]:
    """Normalize legacy rows-v1/composite-v1 active segments."""

    _dataset(dataset)
    schema = value.get("schema_version")
    if schema == "dataset_release_sealed_qlib_csv_rows_v1":
        root = _relative(value.get("root_relative_path"), label="legacy CSV root")
        raw = value.get("files")
        if not isinstance(raw, list) or not raw:
            raise CanonicalLineageError("legacy canonical files are empty")
        segments = [{**dict(item), "root_relative_path": root} for item in raw]
    elif schema == "dataset_release_sealed_qlib_csv_rows_composite_v1":
        raw = value.get("segments")
        if not isinstance(raw, list) or not raw:
            raise CanonicalLineageError("legacy composite segments are empty")
        segments = [dict(item) for item in raw]
    else:
        raise CanonicalLineageError("legacy canonical schema is unsupported")
    return tuple(_segment(item, dataset=dataset) for item in segments)


def write_genesis(
    component_root: Path,
    *,
    dataset: str,
    ordered_fields: Sequence[str],
    segments: Sequence[Mapping[str, Any]],
    cutoff: str,
    mutation_identity: str,
) -> LineageWriteResult:
    normalized = tuple(_segment(item, dataset=dataset) for item in segments)
    key = lineage_event_key(
        dataset=dataset,
        cutoff=cutoff,
        action="FULL_REBUILD",
        baseline_identity="<GENESIS>",
        mutation_identity=mutation_identity,
    )
    return _write_event(
        component_root,
        dataset=dataset,
        ordered_fields=ordered_fields,
        event_key=key,
        event_kind="GENESIS",
        action="FULL_REBUILD",
        cutoff=cutoff,
        current_segments=normalized,
        prior_descriptor=None,
        legacy_binding=None,
        scopes=(),
        inventory=(),
        planned_buckets=None,
        anchor=False,
    )


def write_legacy_anchor(
    component_root: Path,
    *,
    dataset: str,
    ordered_fields: Sequence[str],
    legacy_source: Mapping[str, Any],
    cutoff: str,
    baseline_identity: str,
    baseline_binding: Mapping[str, Any],
    event_key_override: str | None = None,
) -> LineageWriteResult:
    segments = legacy_active_segments(legacy_source, dataset=dataset)
    key = (
        _event_key(event_key_override)
        if event_key_override is not None
        else lineage_event_key(
            dataset=dataset,
            cutoff=cutoff,
            action="LEGACY_ANCHOR",
            baseline_identity=baseline_identity,
            mutation_identity=digest_named_fields(
                "dataset_release_canonical_lineage_legacy_binding_v1",
                dict(baseline_binding),
            ),
        )
    )
    return _write_event(
        component_root,
        dataset=dataset,
        ordered_fields=ordered_fields,
        event_key=key,
        event_kind="LEGACY_ANCHOR",
        action="LEGACY_ANCHOR",
        cutoff=cutoff,
        current_segments=segments,
        prior_descriptor=None,
        legacy_binding=dict(baseline_binding),
        scopes=(),
        inventory=(),
        planned_buckets=None,
        anchor=True,
    )


def write_transition(
    component_root: Path,
    *,
    dataset: str,
    ordered_fields: Sequence[str],
    baseline_descriptor: Mapping[str, Any],
    current_segments: Sequence[Mapping[str, Any]],
    cutoff: str,
    action: str,
    mutation_identity: str,
    scopes: Sequence[Mapping[str, Any]],
    inventory: Sequence[Mapping[str, Any]] = (),
    planned_instruments: Sequence[str] | None = None,
    prior_active_segments: Sequence[Mapping[str, Any]] | None = None,
) -> LineageWriteResult:
    validated = validate_lineage_descriptor(component_root, baseline_descriptor)
    key = lineage_event_key(
        dataset=dataset,
        cutoff=cutoff,
        action=action,
        baseline_identity=str(validated["lineage_root"]),
        mutation_identity=mutation_identity,
    )
    normalized = tuple(_segment(item, dataset=dataset) for item in current_segments)
    planned = None if planned_instruments is None else {lineage_bucket(value) for value in planned_instruments}
    return _write_event(
        component_root,
        dataset=dataset,
        ordered_fields=ordered_fields,
        event_key=key,
        event_kind="SELECTIVE" if action == "SELECTIVE_REBUILD" else "APPEND",
        action=action,
        cutoff=cutoff,
        current_segments=normalized,
        prior_descriptor=validated,
        legacy_binding=None,
        scopes=tuple(dict(item) for item in scopes),
        inventory=tuple(dict(item) for item in inventory),
        planned_buckets=planned,
        anchor=False,
        prior_active_segments=prior_active_segments,
    )


def write_transition_updates(
    component_root: Path,
    *,
    dataset: str,
    ordered_fields: Sequence[str],
    baseline_descriptor: Mapping[str, Any],
    updates: Sequence[Mapping[str, Any]],
    cutoff: str,
    action: str,
    mutation_identity: str,
    scopes: Sequence[Mapping[str, Any]],
    inventory: Sequence[Mapping[str, Any]] = (),
    planned_instruments: Sequence[str] | None = None,
    event_key_override: str | None = None,
) -> LineageWriteResult:
    """Write one transition from per-code deltas without replaying history.

    Each update has exactly ``instrument``, ``mode`` and ``segments``.  APPEND
    segments are only the newly appended suffix; REPLACE and CREATE segments
    are the complete new active sequence for that code.  Persistent active
    roots and head summaries make the work O(all current heads + this event),
    rather than O(all historical segments).
    """

    validated = validate_lineage_descriptor(component_root, baseline_descriptor)
    key = (
        _event_key(event_key_override)
        if event_key_override is not None
        else lineage_event_key(
            dataset=dataset,
            cutoff=cutoff,
            action=action,
            baseline_identity=str(validated["lineage_root"]),
            mutation_identity=mutation_identity,
        )
    )
    planned = None if planned_instruments is None else {lineage_bucket(value) for value in planned_instruments}
    return _write_event(
        component_root,
        dataset=dataset,
        ordered_fields=ordered_fields,
        event_key=key,
        event_kind="SELECTIVE" if action == "SELECTIVE_REBUILD" else "APPEND",
        action=action,
        cutoff=cutoff,
        current_segments=(),
        prior_descriptor=validated,
        legacy_binding=None,
        scopes=tuple(dict(item) for item in scopes),
        inventory=tuple(dict(item) for item in inventory),
        planned_buckets=planned,
        anchor=False,
        segment_updates=tuple(dict(item) for item in updates),
    )


def migrate_legacy_and_write_transition(
    component_root: Path,
    *,
    dataset: str,
    ordered_fields: Sequence[str],
    legacy_source: Mapping[str, Any],
    current_segments: Sequence[Mapping[str, Any]],
    baseline_cutoff: str,
    cutoff: str,
    baseline_identity: str,
    baseline_binding: Mapping[str, Any],
    action: str,
    mutation_identity: str,
    scopes: Sequence[Mapping[str, Any]],
    inventory: Sequence[Mapping[str, Any]] = (),
    planned_instruments: Sequence[str] | None = None,
) -> LineageWriteResult:
    anchor = write_legacy_anchor(
        component_root,
        dataset=dataset,
        ordered_fields=ordered_fields,
        legacy_source=legacy_source,
        cutoff=baseline_cutoff,
        baseline_identity=baseline_identity,
        baseline_binding=baseline_binding,
    )
    result = write_transition(
        component_root,
        dataset=dataset,
        ordered_fields=ordered_fields,
        baseline_descriptor=anchor.descriptor,
        current_segments=current_segments,
        cutoff=cutoff,
        action=action,
        mutation_identity=mutation_identity,
        scopes=scopes,
        inventory=inventory,
        planned_instruments=planned_instruments,
        prior_active_segments=legacy_active_segments(legacy_source, dataset=dataset),
    )
    descriptor = dict(result.descriptor)
    descriptor["legacy_anchor"] = {
        "event_ref": anchor.event_ref.as_dict(),
        "lineage_root": anchor.descriptor["lineage_root"],
    }
    _validate_descriptor_size(descriptor)
    return LineageWriteResult(
        descriptor=descriptor,
        created_paths=tuple(sorted({*anchor.created_paths, *result.created_paths})),
        event_ref=result.event_ref,
        event_key=result.event_key,
        inventory_roots=result.inventory_roots,
    )


def migrate_legacy_and_write_transition_updates(
    component_root: Path,
    *,
    dataset: str,
    ordered_fields: Sequence[str],
    legacy_source: Mapping[str, Any],
    updates: Sequence[Mapping[str, Any]],
    baseline_cutoff: str,
    cutoff: str,
    baseline_identity: str,
    baseline_binding: Mapping[str, Any],
    action: str,
    mutation_identity: str,
    scopes: Sequence[Mapping[str, Any]],
    inventory: Sequence[Mapping[str, Any]] = (),
    planned_instruments: Sequence[str] | None = None,
    anchor_event_key: str | None = None,
    transition_event_key: str | None = None,
) -> LineageWriteResult:
    anchor = write_legacy_anchor(
        component_root,
        dataset=dataset,
        ordered_fields=ordered_fields,
        legacy_source=legacy_source,
        cutoff=baseline_cutoff,
        baseline_identity=baseline_identity,
        baseline_binding=baseline_binding,
        event_key_override=anchor_event_key,
    )
    result = write_transition_updates(
        component_root,
        dataset=dataset,
        ordered_fields=ordered_fields,
        baseline_descriptor=anchor.descriptor,
        updates=updates,
        cutoff=cutoff,
        action=action,
        mutation_identity=mutation_identity,
        scopes=scopes,
        inventory=inventory,
        planned_instruments=planned_instruments,
        event_key_override=transition_event_key,
    )
    descriptor = dict(result.descriptor)
    descriptor["legacy_anchor"] = {
        "event_ref": anchor.event_ref.as_dict(),
        "lineage_root": anchor.descriptor["lineage_root"],
    }
    _validate_descriptor_size(descriptor)
    return LineageWriteResult(
        descriptor=descriptor,
        created_paths=tuple(sorted({*anchor.created_paths, *result.created_paths})),
        event_ref=result.event_ref,
        event_key=result.event_key,
        inventory_roots=result.inventory_roots,
    )


def active_segments(
    component_root: Path,
    descriptor: Mapping[str, Any],
    *,
    instruments: Sequence[str] | None = None,
) -> tuple[dict[str, Any], ...]:
    validated = validate_lineage_descriptor(component_root, descriptor)
    heads = _load_heads(component_root, validated)
    requested = set(heads) if instruments is None else {_instrument(value) for value in instruments}
    if not requested.issubset(heads):
        raise CanonicalLineageError("lineage active segment request contains an unknown instrument")
    event_cache: dict[str, Mapping[str, Any]] = {}
    bucket_cache: dict[tuple[str, str], Mapping[str, Any]] = {}
    output: list[dict[str, Any]] = []
    for code in sorted(requested):
        resolved, resolved_root = _resolve_node_segments(
            component_root,
            heads[code]["node_ref"],
            event_cache=event_cache,
            bucket_cache=bucket_cache,
        )
        if (
            resolved_root != heads[code]["active_root"]
            or sum(int(item["rows"]) for item in resolved) != int(heads[code]["rows"])
            or len(resolved) != int(heads[code]["segment_count"])
        ):
            raise CanonicalLineageError("lineage head summary differs from resolved segments")
        output.extend(resolved)
    return tuple(
        sorted(
            output,
            key=lambda item: (
                str(item["instrument"]),
                str(item["start"]),
                _segment_path_key(item),
            ),
        )
    )


def instrument_summaries(component_root: Path, descriptor: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    validated = validate_lineage_descriptor(component_root, descriptor)
    heads = _load_heads(component_root, validated)
    return tuple(
        {
            "instrument": code,
            "rows": int(value["rows"]),
            "segments": int(value["segment_count"]),
            "start": str(value["start"]),
            "end": str(value["end"]),
        }
        for code, value in sorted(heads.items())
    )


def _validate_event_root(event: Mapping[str, Any]) -> str:
    fields = {
        "schema_version",
        "capability",
        "dataset",
        "event_key",
        "event_kind",
        "component_action",
        "cutoff",
        "parent_event_ref",
        "parent_lineage_root",
        "change_bucket_refs",
        "head_index_ref",
        "head_index_root",
        "inventory_roots",
        "legacy_binding",
        "invalidation_scopes",
        "event_root",
    }
    if (
        set(event) != fields
        or event.get("schema_version") != CANONICAL_LINEAGE_EVENT_SCHEMA
        or event.get("capability") != CANONICAL_LINEAGE_CAPABILITY
        or event.get("event_kind") not in _EVENT_KINDS
    ):
        raise CanonicalLineageError("lineage event fields differ")
    _dataset(str(event.get("dataset", "")))
    _event_key(event.get("event_key"))
    ensure_sha256(str(event.get("head_index_root", "")), field="head_index_root")
    body = dict(event)
    declared = body.pop("event_root", None)
    expected = digest_named_fields(CANONICAL_LINEAGE_EVENT_SCHEMA, body)
    if declared != expected:
        raise CanonicalLineageError("lineage event root differs")
    refs = event.get("change_bucket_refs")
    if not isinstance(refs, list):
        raise CanonicalLineageError("lineage event change refs are invalid")
    paths = [LineageRef.from_value(_mapping(value, "change bucket ref")).relative_path for value in refs]
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise CanonicalLineageError("lineage event change refs are unordered")
    parent = event.get("parent_event_ref")
    parent_root = event.get("parent_lineage_root")
    if (parent is None) != (parent_root is None):
        raise CanonicalLineageError("lineage event parent binding differs")
    if parent is not None:
        LineageRef.from_value(_mapping(parent, "parent event ref"))
        ensure_sha256(str(parent_root), field="parent_lineage_root")
    return expected


def validate_lineage_descriptor(component_root: Path, descriptor: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "schema_version",
        "capability",
        "dataset",
        "ordered_fields",
        "rows",
        "instrument_count",
        "active_segment_count",
        "latest_event_ref",
        "latest_event_root",
        "head_index_ref",
        "head_index_root",
        "lineage_root",
        "merge_contract",
    }
    allowed = required.union({"legacy_anchor"})
    if (
        set(descriptor).difference(allowed)
        or not required.issubset(descriptor)
        or descriptor.get("schema_version") != CANONICAL_LINEAGE_SCHEMA
        or descriptor.get("capability") != CANONICAL_LINEAGE_CAPABILITY
        or descriptor.get("merge_contract") != "persistent_code_head_merkle_v3"
    ):
        raise CanonicalLineageError("lineage descriptor contract differs")
    _validate_descriptor_size(descriptor)
    dataset = _dataset(str(descriptor.get("dataset", "")))
    fields = descriptor.get("ordered_fields")
    if not isinstance(fields, list) or not fields or any(not isinstance(item, str) for item in fields):
        raise CanonicalLineageError("lineage ordered fields are invalid")
    for name in ("rows", "instrument_count", "active_segment_count"):
        value = descriptor.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise CanonicalLineageError(f"lineage {name} is invalid")
    root = _component_root(component_root)
    event_ref = LineageRef.from_value(_mapping(descriptor.get("latest_event_ref"), "latest event ref"))
    event = _read_json_ref(root, event_ref, expected_schema=CANONICAL_LINEAGE_EVENT_SCHEMA)
    event_root = _validate_event_root(event)
    if event_root != descriptor.get("latest_event_root"):
        raise CanonicalLineageError("lineage latest event root differs")
    head_ref = LineageRef.from_value(_mapping(descriptor.get("head_index_ref"), "head index ref"))
    head = _read_json_ref(root, head_ref, expected_schema=CANONICAL_LINEAGE_HEAD_INDEX_SCHEMA)
    head_buckets = head.get("buckets")
    if (
        set(head)
        != {
            "schema_version",
            "dataset",
            "event_key",
            "bucket_count",
            "buckets",
            "head_index_root",
        }
        or head.get("dataset") != dataset
        or head.get("event_key") != event.get("event_key")
        or head.get("bucket_count") != LINEAGE_BUCKET_COUNT
        or not isinstance(head_buckets, list)
        or not head_buckets
    ):
        raise CanonicalLineageError("lineage head index contract differs")
    head_pairs: list[tuple[str, str]] = []
    prior_bucket = ""
    for item in head_buckets:
        if not isinstance(item, Mapping) or set(item) != {"bucket", "ref"}:
            raise CanonicalLineageError("lineage head index bucket fields differ")
        bucket = str(item.get("bucket", ""))
        if _BUCKET.fullmatch(bucket) is None or bucket <= prior_bucket:
            raise CanonicalLineageError("lineage head index buckets are unordered")
        prior_bucket = bucket
        head_pairs.append(
            (
                bucket,
                LineageRef.from_value(_mapping(item.get("ref"), "head bucket ref")).identity,
            )
        )
    computed_head_root = merkle_root_from_named_digests(
        CANONICAL_LINEAGE_HEAD_INDEX_SCHEMA,
        head_pairs,
    )
    if (
        head_ref.as_dict() != event.get("head_index_ref")
        or computed_head_root != head.get("head_index_root")
        or head.get("head_index_root") != descriptor.get("head_index_root")
        or event.get("head_index_root") != descriptor.get("head_index_root")
        or event.get("dataset") != dataset
    ):
        raise CanonicalLineageError("lineage event/head index binding differs")
    expected_lineage = digest_named_fields(
        CANONICAL_LINEAGE_ROOT_SCHEMA,
        {
            "dataset": dataset,
            "parent_lineage_root": event.get("parent_lineage_root"),
            "event_root": event_root,
            "head_index_root": head["head_index_root"],
        },
    )
    if descriptor.get("lineage_root") != expected_lineage:
        raise CanonicalLineageError("lineage root differs")
    legacy_anchor = descriptor.get("legacy_anchor")
    if legacy_anchor is not None:
        if not isinstance(legacy_anchor, Mapping) or set(legacy_anchor) != {
            "event_ref",
            "lineage_root",
        }:
            raise CanonicalLineageError("lineage legacy anchor fields differ")
        anchor_ref = LineageRef.from_value(_mapping(legacy_anchor.get("event_ref"), "legacy anchor event ref"))
        anchor_event = _read_json_ref(
            root,
            anchor_ref,
            expected_schema=CANONICAL_LINEAGE_EVENT_SCHEMA,
        )
        anchor_event_root = _validate_event_root(anchor_event)
        if anchor_event.get("event_kind") != "LEGACY_ANCHOR":
            raise CanonicalLineageError("lineage legacy anchor kind differs")
        anchor_lineage_root = digest_named_fields(
            CANONICAL_LINEAGE_ROOT_SCHEMA,
            {
                "dataset": dataset,
                "parent_lineage_root": None,
                "event_root": anchor_event_root,
                "head_index_root": anchor_event["head_index_root"],
            },
        )
        if anchor_lineage_root != legacy_anchor.get("lineage_root"):
            raise CanonicalLineageError("lineage legacy anchor root differs")
    output = dict(descriptor)
    output["latest_event_ref"] = event_ref.as_dict()
    output["head_index_ref"] = head_ref.as_dict()
    return output


def namespace_manifest(
    *,
    dataset: str,
    component_action: str,
    phase: str,
    segment_key: str,
    namespace_root_relative_path: str,
    lineage: LineageWriteResult,
    patch_actual_work: Mapping[str, Any],
) -> dict[str, Any]:
    namespace = _relative(namespace_root_relative_path, label="lineage namespace root")
    inventory_root = lineage.inventory_roots.get(namespace)
    if inventory_root is None:
        raise CanonicalLineageError("lineage namespace inventory root is missing")
    body = {
        "schema_version": CANONICAL_LINEAGE_NAMESPACE_MANIFEST_SCHEMA,
        "dataset": _dataset(dataset),
        "component_action": str(component_action),
        "phase": str(phase),
        "segment_key": str(segment_key),
        "namespace_root_relative_path": namespace,
        "event_ref": lineage.event_ref.as_dict(),
        "event_root": lineage.descriptor["latest_event_root"],
        "lineage_root": lineage.descriptor["lineage_root"],
        "inventory_root": inventory_root,
        "patch_actual_work": dict(patch_actual_work),
        "capability": CANONICAL_LINEAGE_CAPABILITY,
    }
    body["manifest_identity"] = digest_named_fields(CANONICAL_LINEAGE_NAMESPACE_MANIFEST_SCHEMA, body)
    if len(canonical_json_bytes(body)) > MAX_LINEAGE_OBJECT_BYTES:
        raise CanonicalLineageError("lineage namespace manifest exceeds bound")
    return body


def event_inventory(
    component_root: Path,
    event_ref_value: Mapping[str, Any],
    *,
    namespace_root_relative_path: str,
) -> tuple[dict[str, Any], ...]:
    root = _component_root(component_root)
    event_ref = LineageRef.from_value(event_ref_value)
    event = _read_json_ref(root, event_ref, expected_schema=CANONICAL_LINEAGE_EVENT_SCHEMA)
    namespace = _relative(namespace_root_relative_path, label="lineage inventory root")
    output: list[dict[str, Any]] = []
    for raw_ref in event.get("change_bucket_refs") or ():
        ref = LineageRef.from_value(_mapping(raw_ref, "change bucket ref"))
        payload = _read_json_ref(root, ref, expected_schema=CANONICAL_LINEAGE_CHANGE_BUCKET_SCHEMA)
        for item in payload.get("inventory") or ():
            if item.get("root_relative_path") == namespace:
                raw = dict(_mapping(item, "lineage inventory item"))
                declared = raw.pop("inventory_identity", None)
                normalized = _inventory_item(raw, dataset=str(event["dataset"]))
                if declared != normalized["inventory_identity"]:
                    raise CanonicalLineageError("lineage inventory item identity differs")
                output.append(normalized)
    ordered = tuple(
        sorted(
            output,
            key=lambda item: (
                str(item["instrument"]),
                str(item["relative_path"]),
            ),
        )
    )
    roots = _inventory_roots(ordered)
    declared_roots = event.get("inventory_roots")
    if (
        not isinstance(declared_roots, Mapping)
        or namespace not in declared_roots
        or declared_roots.get(namespace) != roots.get(namespace)
    ):
        raise CanonicalLineageError("lineage namespace inventory root differs")
    return ordered


def latest_event_inventory(
    component_root: Path,
    descriptor: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    validated = validate_lineage_descriptor(component_root, descriptor)
    root = _component_root(component_root)
    event_ref = LineageRef.from_value(_mapping(validated["latest_event_ref"], "latest event ref"))
    event = _read_json_ref(
        root,
        event_ref,
        expected_schema=CANONICAL_LINEAGE_EVENT_SCHEMA,
    )
    roots = event.get("inventory_roots")
    if not isinstance(roots, Mapping):
        raise CanonicalLineageError("lineage event inventory roots are invalid")
    output: list[dict[str, Any]] = []
    for namespace in sorted(roots):
        output.extend(
            event_inventory(
                root,
                event_ref.as_dict(),
                namespace_root_relative_path=str(namespace),
            )
        )
    identities = [str(item["inventory_identity"]) for item in output]
    if len(identities) != len(set(identities)):
        raise CanonicalLineageError("lineage latest inventory is duplicated")
    return tuple(output)


def lineage_inventory_history(
    component_root: Path,
    descriptor: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Return all append-only namespace inventory rows in event order."""

    validated = validate_lineage_descriptor(component_root, descriptor)
    root = _component_root(component_root)
    current: Mapping[str, Any] | None = dict(validated["latest_event_ref"])
    seen: set[str] = set()
    output: list[dict[str, Any]] = []
    while current is not None:
        ref = LineageRef.from_value(_mapping(current, "lineage history event ref"))
        if ref.identity in seen:
            raise CanonicalLineageError("lineage event history is cyclic")
        seen.add(ref.identity)
        event = _read_json_ref(
            root,
            ref,
            expected_schema=CANONICAL_LINEAGE_EVENT_SCHEMA,
        )
        _validate_event_root(event)
        roots = event.get("inventory_roots")
        if not isinstance(roots, Mapping):
            raise CanonicalLineageError("lineage history inventory roots are invalid")
        for namespace in sorted(roots):
            output.extend(
                event_inventory(
                    root,
                    ref.as_dict(),
                    namespace_root_relative_path=str(namespace),
                )
            )
        parent = event.get("parent_event_ref")
        if parent is None:
            current = None
        elif isinstance(parent, Mapping):
            current = parent
        else:
            raise CanonicalLineageError("lineage history parent event reference is invalid")
    return tuple(output)


def lineage_object_paths(
    component_root: Path,
    descriptor: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return the exact reachable candidate-local lineage object namespace."""

    validated = validate_lineage_descriptor(component_root, descriptor)
    root = _component_root(component_root)
    current: Mapping[str, Any] | None = dict(validated["latest_event_ref"])
    seen_events: set[str] = set()
    paths: set[str] = set()
    while current is not None:
        event_ref = LineageRef.from_value(_mapping(current, "lineage object event ref"))
        if event_ref.identity in seen_events:
            raise CanonicalLineageError("lineage object graph is cyclic")
        seen_events.add(event_ref.identity)
        paths.add(event_ref.relative_path)
        event = _read_json_ref(
            root,
            event_ref,
            expected_schema=CANONICAL_LINEAGE_EVENT_SCHEMA,
        )
        _validate_event_root(event)
        for value in event["change_bucket_refs"]:
            paths.add(LineageRef.from_value(_mapping(value, "lineage object change ref")).relative_path)
        head_ref = LineageRef.from_value(_mapping(event["head_index_ref"], "lineage object head index ref"))
        paths.add(head_ref.relative_path)
        head = _read_json_ref(
            root,
            head_ref,
            expected_schema=CANONICAL_LINEAGE_HEAD_INDEX_SCHEMA,
        )
        for item in head.get("buckets") or ():
            paths.add(LineageRef.from_value(_mapping(item.get("ref"), "lineage object head bucket ref")).relative_path)
        parent = event.get("parent_event_ref")
        current = None if parent is None else _mapping(parent, "parent event ref")
    return tuple(sorted(paths))


def _write_event(
    component_root: Path,
    *,
    dataset: str,
    ordered_fields: Sequence[str],
    event_key: str,
    event_kind: str,
    action: str,
    cutoff: str,
    current_segments: Sequence[Mapping[str, Any]],
    prior_descriptor: Mapping[str, Any] | None,
    legacy_binding: Mapping[str, Any] | None,
    scopes: Sequence[Mapping[str, Any]],
    inventory: Sequence[Mapping[str, Any]],
    planned_buckets: set[str] | None,
    anchor: bool,
    prior_active_segments: Sequence[Mapping[str, Any]] | None = None,
    segment_updates: Sequence[Mapping[str, Any]] | None = None,
) -> LineageWriteResult:
    root = _component_root(component_root)
    dataset = _dataset(dataset)
    key = _event_key(event_key)
    if event_kind not in _EVENT_KINDS:
        raise CanonicalLineageError("lineage event kind is invalid")
    fields = [str(value) for value in ordered_fields]
    if len(fields) != len(set(fields)) or not fields:
        raise CanonicalLineageError("lineage ordered fields are invalid")
    if segment_updates is not None and current_segments:
        raise CanonicalLineageError("lineage transition cannot mix full state and per-code updates")
    current_by_code = {} if segment_updates is not None else _group_segments(current_segments, dataset=dataset)
    if segment_updates is None and not current_by_code:
        raise CanonicalLineageError("lineage current active segments are empty")
    prior_heads: dict[str, dict[str, Any]] = {}
    prior_by_code: dict[str, tuple[dict[str, Any], ...]] = {}
    parent_event_ref: Mapping[str, Any] | None = None
    parent_lineage_root: str | None = None
    legacy_anchor: Mapping[str, Any] | None = None
    if prior_descriptor is not None:
        validated = validate_lineage_descriptor(root, prior_descriptor)
        prior_heads = _load_heads(root, validated)
        if segment_updates is None:
            prior_segments = (
                active_segments(root, validated)
                if prior_active_segments is None
                else tuple(_segment(item, dataset=dataset) for item in prior_active_segments)
            )
            prior_by_code = _group_segments(prior_segments, dataset=dataset)
            if set(prior_by_code) != set(prior_heads):
                raise CanonicalLineageError("lineage supplied prior active instruments differ from heads")
            for code, segments in prior_by_code.items():
                head = prior_heads[code]
                if (
                    sum(int(item["rows"]) for item in segments) != int(head["rows"])
                    or len(segments) != int(head["segment_count"])
                    or str(segments[0]["start"]) != str(head["start"])
                    or str(segments[-1]["end"]) != str(head["end"])
                ):
                    raise CanonicalLineageError("lineage supplied prior active segments differ from heads")
        parent_event_ref = dict(validated["latest_event_ref"])
        parent_lineage_root = str(validated["lineage_root"])
        if isinstance(validated.get("legacy_anchor"), Mapping):
            legacy_anchor = dict(validated["legacy_anchor"])
    elif event_kind not in {"GENESIS", "LEGACY_ANCHOR"}:
        raise CanonicalLineageError("lineage transition lacks a prior descriptor")

    node_specs: dict[str, dict[str, Any]] = {}
    if segment_updates is not None:
        if prior_descriptor is None:
            raise CanonicalLineageError("lineage per-code update lacks a prior descriptor")
        normalized_updates = _normalize_transition_updates(
            segment_updates,
            dataset=dataset,
            prior_heads=prior_heads,
        )
        if not normalized_updates:
            raise CanonicalLineageError("lineage per-code updates are empty")
        total_rows = int(prior_descriptor["rows"])
        total_segments = int(prior_descriptor["active_segment_count"])
        instrument_count = int(prior_descriptor["instrument_count"])
        for code, update in normalized_updates.items():
            mode = str(update["mode"])
            emitted = tuple(update["segments"])
            prior_head = prior_heads.get(code)
            if mode == "APPEND":
                assert prior_head is not None
                node_kind = "APPEND"
                resets = False
                parent_head = dict(prior_head["node_ref"])
                prior_root = str(prior_head["active_root"])
                rows = int(prior_head["rows"]) + sum(int(item["rows"]) for item in emitted)
                segment_count = int(prior_head["segment_count"]) + len(emitted)
                start = str(prior_head["start"])
                end = str(emitted[-1]["end"])
                total_rows += rows - int(prior_head["rows"])
                total_segments += segment_count - int(prior_head["segment_count"])
            elif mode == "REPLACE":
                assert prior_head is not None
                node_kind = "OVERRIDE"
                resets = True
                parent_head = dict(prior_head["node_ref"])
                prior_root = str(prior_head["active_root"])
                rows = sum(int(item["rows"]) for item in emitted)
                segment_count = len(emitted)
                start = str(emitted[0]["start"])
                end = str(emitted[-1]["end"])
                total_rows += rows - int(prior_head["rows"])
                total_segments += segment_count - int(prior_head["segment_count"])
            else:
                node_kind = "CREATE"
                resets = True
                parent_head = None
                prior_root = None
                rows = sum(int(item["rows"]) for item in emitted)
                segment_count = len(emitted)
                start = str(emitted[0]["start"])
                end = str(emitted[-1]["end"])
                total_rows += rows
                total_segments += segment_count
                instrument_count += 1
            node_specs[code] = {
                "node_kind": node_kind,
                "emitted": emitted,
                "parent_head": parent_head,
                "prior_active_root": prior_root,
                "resets": resets,
                "active_root": _next_active_root(
                    prior_active_root=prior_root,
                    segments=emitted,
                    resets_history=resets,
                ),
                "rows": rows,
                "segment_count": segment_count,
                "start": start,
                "end": end,
            }
        changed_codes = set(node_specs)
    else:
        changed_codes = {
            code
            for code in set(prior_by_code).union(current_by_code)
            if prior_by_code.get(code) != current_by_code.get(code)
        }
        if set(prior_by_code).difference(current_by_code):
            raise CanonicalLineageError("lineage transition removed an active instrument")
        total_rows = sum(int(item["rows"]) for values in current_by_code.values() for item in values)
        total_segments = sum(len(values) for values in current_by_code.values())
        instrument_count = len(current_by_code)
        for code in sorted(changed_codes):
            before = prior_by_code.get(code, ())
            after = current_by_code[code]
            if not before:
                node_kind = (
                    "ANCHOR" if event_kind == "LEGACY_ANCHOR" else ("GENESIS" if event_kind == "GENESIS" else "CREATE")
                )
                emitted = after
                parent_head = None
                prior_root = None
                resets = True
            elif len(after) > len(before) and after[: len(before)] == before:
                node_kind = "APPEND"
                emitted = after[len(before) :]
                parent_head = dict(prior_heads[code]["node_ref"])
                prior_root = str(prior_heads[code]["active_root"])
                resets = False
            else:
                node_kind = "OVERRIDE"
                emitted = after
                parent_head = dict(prior_heads[code]["node_ref"])
                prior_root = str(prior_heads[code]["active_root"])
                resets = True
            node_specs[code] = {
                "node_kind": node_kind,
                "emitted": emitted,
                "parent_head": parent_head,
                "prior_active_root": prior_root,
                "resets": resets,
                "active_root": _next_active_root(
                    prior_active_root=prior_root,
                    segments=emitted,
                    resets_history=resets,
                ),
                "rows": sum(int(item["rows"]) for item in after),
                "segment_count": len(after),
                "start": str(after[0]["start"]),
                "end": str(after[-1]["end"]),
            }

    normalized_inventory = tuple(_inventory_item(item, dataset=dataset) for item in inventory)
    inventory_by_bucket: dict[str, list[dict[str, Any]]] = {}
    for item in normalized_inventory:
        inventory_by_bucket.setdefault(lineage_bucket(item["instrument"]), []).append(item)

    changed_buckets = {lineage_bucket(code) for code in changed_codes}
    changed_buckets.update(inventory_by_bucket)
    if planned_buckets is not None:
        if not changed_buckets.issubset(planned_buckets):
            raise CanonicalLineageError("lineage actual buckets exceed frozen targets")
        changed_buckets = set(planned_buckets)
    if not changed_buckets:
        raise CanonicalLineageError("lineage event contains no changed buckets")

    base = f"csv_lineage/{'anchors' if anchor else 'events'}/{key}"
    created: list[str] = []
    change_refs: list[LineageRef] = []
    nodes_by_bucket: dict[str, list[dict[str, Any]]] = {}
    for code in sorted(changed_codes):
        spec = node_specs[code]
        node_kind = str(spec["node_kind"])
        emitted = tuple(spec["emitted"])
        parent_head = spec["parent_head"]
        resets = bool(spec["resets"])
        node_body = {
            "instrument": code,
            "kind": node_kind,
            "segments": [dict(item) for item in emitted],
            "parent_head": parent_head,
            "prior_active_root": spec["prior_active_root"],
            "active_root": spec["active_root"],
            "resets_history": resets,
            "invalidation_scopes": [dict(item) for item in scopes] if node_kind == "OVERRIDE" else [],
        }
        node = {
            **node_body,
            "node_identity": digest_named_fields(CANONICAL_LINEAGE_NODE_SCHEMA, node_body),
        }
        nodes_by_bucket.setdefault(lineage_bucket(code), []).append(node)

    change_payload_by_bucket: dict[str, Mapping[str, Any]] = {}
    for bucket in sorted(changed_buckets):
        nodes = sorted(nodes_by_bucket.get(bucket, ()), key=lambda item: item["instrument"])
        inventory_rows = sorted(
            inventory_by_bucket.get(bucket, ()),
            key=lambda item: (item["instrument"], item["root_relative_path"], item["relative_path"]),
        )
        body = {
            "schema_version": CANONICAL_LINEAGE_CHANGE_BUCKET_SCHEMA,
            "dataset": dataset,
            "event_key": key,
            "bucket": bucket,
            "nodes": nodes,
            "inventory": inventory_rows,
        }
        body["bucket_root"] = digest_named_fields(CANONICAL_LINEAGE_CHANGE_BUCKET_SCHEMA, body)
        relative = f"{base}/changes/{bucket}.json"
        ref = _seal_json(root, relative, body)
        created.append(relative)
        change_refs.append(ref)
        change_payload_by_bucket[bucket] = body

    heads_by_bucket: dict[str, dict[str, dict[str, Any]]] = {}
    for code, value in prior_heads.items():
        heads_by_bucket.setdefault(lineage_bucket(code), {})[code] = dict(value)
    for code in sorted(changed_codes):
        spec = node_specs[code]
        bucket = lineage_bucket(code)
        node = next(item for item in nodes_by_bucket[bucket] if item["instrument"] == code)
        heads_by_bucket.setdefault(bucket, {})[code] = {
            "instrument": code,
            "node_ref": {
                "event_path": lineage_event_relative_path(key, anchor=anchor),
                "bucket": bucket,
                "node_identity": node["node_identity"],
            },
            "active_root": node["active_root"],
            "rows": int(spec["rows"]),
            "segment_count": int(spec["segment_count"]),
            "start": str(spec["start"]),
            "end": str(spec["end"]),
        }

    prior_head_refs: dict[str, LineageRef] = {}
    if prior_descriptor is not None:
        prior_index = _read_json_ref(
            root,
            LineageRef.from_value(_mapping(prior_descriptor["head_index_ref"], "prior head index ref")),
            expected_schema=CANONICAL_LINEAGE_HEAD_INDEX_SCHEMA,
        )
        prior_head_refs = {
            str(item["bucket"]): LineageRef.from_value(_mapping(item["ref"], "prior head bucket ref"))
            for item in prior_index.get("buckets") or ()
        }
    head_refs: dict[str, LineageRef] = dict(prior_head_refs)
    for bucket in sorted(changed_buckets):
        entries = [heads_by_bucket.get(bucket, {})[code] for code in sorted(heads_by_bucket.get(bucket, {}))]
        body = {
            "schema_version": CANONICAL_LINEAGE_HEAD_BUCKET_SCHEMA,
            "dataset": dataset,
            "event_key": key,
            "bucket": bucket,
            "heads": entries,
        }
        body["bucket_root"] = digest_named_fields(CANONICAL_LINEAGE_HEAD_BUCKET_SCHEMA, body)
        relative = f"{base}/heads/{bucket}.json"
        ref = _seal_json(root, relative, body)
        created.append(relative)
        head_refs[bucket] = ref

    head_index_body = {
        "schema_version": CANONICAL_LINEAGE_HEAD_INDEX_SCHEMA,
        "dataset": dataset,
        "event_key": key,
        "bucket_count": LINEAGE_BUCKET_COUNT,
        "buckets": [{"bucket": bucket, "ref": head_refs[bucket].as_dict()} for bucket in sorted(head_refs)],
    }
    head_index_body["head_index_root"] = merkle_root_from_named_digests(
        CANONICAL_LINEAGE_HEAD_INDEX_SCHEMA,
        ((bucket, head_refs[bucket].identity) for bucket in sorted(head_refs)),
    )
    head_index_relative = f"{base}/head_index.json"
    head_index_ref = _seal_json(root, head_index_relative, head_index_body)
    created.append(head_index_relative)

    inventory_roots = _inventory_roots(normalized_inventory)
    event_body = {
        "schema_version": CANONICAL_LINEAGE_EVENT_SCHEMA,
        "capability": CANONICAL_LINEAGE_CAPABILITY,
        "dataset": dataset,
        "event_key": key,
        "event_kind": event_kind,
        "component_action": action,
        "cutoff": str(cutoff),
        "parent_event_ref": parent_event_ref,
        "parent_lineage_root": parent_lineage_root,
        "change_bucket_refs": [ref.as_dict() for ref in change_refs],
        "head_index_ref": head_index_ref.as_dict(),
        "head_index_root": head_index_body["head_index_root"],
        "inventory_roots": dict(sorted(inventory_roots.items())),
        "legacy_binding": dict(legacy_binding) if legacy_binding is not None else None,
        "invalidation_scopes": [dict(item) for item in scopes],
    }
    event_body["event_root"] = digest_named_fields(CANONICAL_LINEAGE_EVENT_SCHEMA, event_body)
    event_relative = lineage_event_relative_path(key, anchor=anchor)
    event_ref = _seal_json(root, event_relative, event_body)
    created.append(event_relative)

    lineage_root = digest_named_fields(
        CANONICAL_LINEAGE_ROOT_SCHEMA,
        {
            "dataset": dataset,
            "parent_lineage_root": parent_lineage_root,
            "event_root": event_body["event_root"],
            "head_index_root": head_index_body["head_index_root"],
        },
    )
    descriptor: dict[str, Any] = {
        "schema_version": CANONICAL_LINEAGE_SCHEMA,
        "capability": CANONICAL_LINEAGE_CAPABILITY,
        "dataset": dataset,
        "ordered_fields": fields,
        "rows": total_rows,
        "instrument_count": instrument_count,
        "active_segment_count": total_segments,
        "latest_event_ref": event_ref.as_dict(),
        "latest_event_root": event_body["event_root"],
        "head_index_ref": head_index_ref.as_dict(),
        "head_index_root": head_index_body["head_index_root"],
        "lineage_root": lineage_root,
        "merge_contract": "persistent_code_head_merkle_v3",
    }
    if legacy_anchor is not None:
        descriptor["legacy_anchor"] = legacy_anchor
    _validate_descriptor_size(descriptor)
    validate_lineage_descriptor(root, descriptor)
    return LineageWriteResult(
        descriptor=descriptor,
        created_paths=tuple(sorted(created)),
        event_ref=event_ref,
        event_key=key,
        inventory_roots=inventory_roots,
    )


def _load_heads(component_root: Path, descriptor: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    root = _component_root(component_root)
    index_ref = LineageRef.from_value(_mapping(descriptor.get("head_index_ref"), "head index ref"))
    index = _read_json_ref(root, index_ref, expected_schema=CANONICAL_LINEAGE_HEAD_INDEX_SCHEMA)
    if (
        set(index)
        != {
            "schema_version",
            "dataset",
            "event_key",
            "bucket_count",
            "buckets",
            "head_index_root",
        }
        or index.get("dataset") != descriptor.get("dataset")
        or index.get("bucket_count") != LINEAGE_BUCKET_COUNT
    ):
        raise CanonicalLineageError("lineage head index fields differ")
    _event_key(index.get("event_key"))
    buckets = index.get("buckets")
    if not isinstance(buckets, list) or not buckets:
        raise CanonicalLineageError("lineage head index buckets are empty")
    expected_root = merkle_root_from_named_digests(
        CANONICAL_LINEAGE_HEAD_INDEX_SCHEMA,
        (
            (
                str(item.get("bucket", "")),
                LineageRef.from_value(_mapping(item.get("ref"), "head bucket ref")).identity,
            )
            for item in buckets
        ),
    )
    if expected_root != index.get("head_index_root"):
        raise CanonicalLineageError("lineage head index Merkle differs")
    output: dict[str, dict[str, Any]] = {}
    prior_bucket = ""
    for item in buckets:
        if not isinstance(item, Mapping) or set(item) != {"bucket", "ref"}:
            raise CanonicalLineageError("lineage head bucket ref fields differ")
        bucket = str(item.get("bucket", ""))
        if _BUCKET.fullmatch(bucket) is None or bucket <= prior_bucket:
            raise CanonicalLineageError("lineage head buckets are unordered/invalid")
        prior_bucket = bucket
        ref = LineageRef.from_value(_mapping(item.get("ref"), "head bucket ref"))
        payload = _read_json_ref(root, ref, expected_schema=CANONICAL_LINEAGE_HEAD_BUCKET_SCHEMA)
        body = dict(payload)
        declared_bucket_root = body.pop("bucket_root", None)
        if (
            set(payload)
            != {
                "schema_version",
                "dataset",
                "event_key",
                "bucket",
                "heads",
                "bucket_root",
            }
            or payload.get("dataset") != descriptor.get("dataset")
            or payload.get("bucket") != bucket
            or declared_bucket_root != digest_named_fields(CANONICAL_LINEAGE_HEAD_BUCKET_SCHEMA, body)
        ):
            raise CanonicalLineageError("lineage head bucket identity differs")
        heads = payload.get("heads")
        if not isinstance(heads, list):
            raise CanonicalLineageError("lineage head bucket rows are invalid")
        codes = [str(value.get("instrument", "")) for value in heads]
        if codes != sorted(codes) or len(codes) != len(set(codes)):
            raise CanonicalLineageError("lineage heads are unordered/duplicate")
        for value in heads:
            if set(value) != {
                "instrument",
                "node_ref",
                "active_root",
                "rows",
                "segment_count",
                "start",
                "end",
            }:
                raise CanonicalLineageError("lineage head fields differ")
            code = _instrument(value.get("instrument"))
            if lineage_bucket(code) != bucket or code in output:
                raise CanonicalLineageError("lineage head instrument bucket differs")
            node_ref = value.get("node_ref")
            if not isinstance(node_ref, Mapping) or set(node_ref) != {
                "event_path",
                "bucket",
                "node_identity",
            }:
                raise CanonicalLineageError("lineage head node reference is invalid")
            _relative(node_ref.get("event_path"), label="head node event path")
            if node_ref.get("bucket") != bucket:
                raise CanonicalLineageError("lineage head node bucket differs")
            ensure_sha256(str(node_ref.get("node_identity", "")), field="head_node_identity")
            ensure_sha256(str(value.get("active_root", "")), field="lineage_active_root")
            for name in ("rows", "segment_count"):
                number = value.get(name)
                if isinstance(number, bool) or not isinstance(number, int) or number <= 0:
                    raise CanonicalLineageError("lineage head summary is invalid")
            if (
                not isinstance(value.get("start"), str)
                or not isinstance(value.get("end"), str)
                or value["end"] < value["start"]
            ):
                raise CanonicalLineageError("lineage head range is invalid")
            output[code] = dict(value)
    if (
        len(output) != int(descriptor.get("instrument_count", -1))
        or sum(int(item["rows"]) for item in output.values()) != int(descriptor.get("rows", -1))
        or sum(int(item["segment_count"]) for item in output.values())
        != int(descriptor.get("active_segment_count", -1))
    ):
        raise CanonicalLineageError("lineage descriptor/head summaries differ")
    return output


def _resolve_node_segments(
    root: Path,
    node_ref: Mapping[str, Any],
    *,
    event_cache: dict[str, Mapping[str, Any]],
    bucket_cache: dict[tuple[str, str], Mapping[str, Any]],
) -> tuple[tuple[dict[str, Any], ...], str]:
    current: Mapping[str, Any] | None = dict(node_ref)
    suffix: list[dict[str, Any]] = []
    seen: set[str] = set()
    latest_root: str | None = None
    expected_parent_root: str | None = None
    while current is not None:
        event_path = _relative(current.get("event_path"), label="lineage node event path")
        bucket = str(current.get("bucket", ""))
        identity = ensure_sha256(str(current.get("node_identity", "")), field="lineage_node_identity")
        if identity in seen or _BUCKET.fullmatch(bucket) is None:
            raise CanonicalLineageError("lineage node chain is cyclic/invalid")
        seen.add(identity)
        event = event_cache.get(event_path)
        if event is None:
            event = _read_json_path(root, event_path, expected_schema=CANONICAL_LINEAGE_EVENT_SCHEMA)
            _validate_event_root(event)
            event_cache[event_path] = event
        change_refs: dict[str, LineageRef] = {}
        for raw_ref in event.get("change_bucket_refs") or ():
            ref = LineageRef.from_value(_mapping(raw_ref, "change bucket ref"))
            candidate_bucket = PurePosixPath(ref.relative_path).stem
            if _BUCKET.fullmatch(candidate_bucket) is None or candidate_bucket in change_refs:
                raise CanonicalLineageError("lineage event change bucket references are invalid")
            change_refs[candidate_bucket] = ref
        ref = change_refs.get(bucket)
        if ref is None:
            raise CanonicalLineageError("lineage node change bucket is missing")
        cache_key = (event_path, bucket)
        payload = bucket_cache.get(cache_key)
        if payload is None:
            payload = _read_json_ref(root, ref, expected_schema=CANONICAL_LINEAGE_CHANGE_BUCKET_SCHEMA)
            payload_body = dict(payload)
            declared_bucket_root = payload_body.pop("bucket_root", None)
            if (
                set(payload)
                != {
                    "schema_version",
                    "dataset",
                    "event_key",
                    "bucket",
                    "nodes",
                    "inventory",
                    "bucket_root",
                }
                or payload.get("dataset") != event.get("dataset")
                or payload.get("event_key") != event.get("event_key")
                or payload.get("bucket") != bucket
                or declared_bucket_root
                != digest_named_fields(
                    CANONICAL_LINEAGE_CHANGE_BUCKET_SCHEMA,
                    payload_body,
                )
            ):
                raise CanonicalLineageError("lineage change bucket identity differs")
            bucket_cache[cache_key] = payload
        matches = [item for item in payload.get("nodes") or () if item.get("node_identity") == identity]
        if len(matches) != 1:
            raise CanonicalLineageError("lineage node identity is missing/ambiguous")
        node = matches[0]
        if set(node) != {
            "instrument",
            "kind",
            "segments",
            "parent_head",
            "prior_active_root",
            "active_root",
            "resets_history",
            "invalidation_scopes",
            "node_identity",
        }:
            raise CanonicalLineageError("lineage node fields differ")
        code = _instrument(node.get("instrument"))
        if lineage_bucket(code) != bucket or node.get("kind") not in _NODE_KINDS:
            raise CanonicalLineageError("lineage node kind/bucket differs")
        body = {key: value for key, value in node.items() if key != "node_identity"}
        if digest_named_fields(CANONICAL_LINEAGE_NODE_SCHEMA, body) != identity:
            raise CanonicalLineageError("lineage node content identity differs")
        segments = tuple(_segment(item, dataset=str(event["dataset"])) for item in node.get("segments") or ())
        if not segments:
            raise CanonicalLineageError("lineage node segments are empty")
        resets = node.get("resets_history")
        if type(resets) is not bool:
            raise CanonicalLineageError("lineage node reset flag is invalid")
        prior_root_value = node.get("prior_active_root")
        prior_root = (
            None
            if prior_root_value is None
            else ensure_sha256(str(prior_root_value), field="lineage_prior_active_root")
        )
        active_root = ensure_sha256(str(node.get("active_root", "")), field="lineage_node_active_root")
        kind = str(node["kind"])
        parent_head = node.get("parent_head")
        if kind == "APPEND":
            if resets or not isinstance(parent_head, Mapping) or prior_root is None:
                raise CanonicalLineageError("lineage append node contract differs")
        elif kind == "OVERRIDE":
            if not resets or not isinstance(parent_head, Mapping) or prior_root is None:
                raise CanonicalLineageError("lineage override node contract differs")
        elif not resets or parent_head is not None or prior_root is not None:
            raise CanonicalLineageError("lineage reset node contract differs")
        if not isinstance(node.get("invalidation_scopes"), list):
            raise CanonicalLineageError("lineage node scopes are invalid")
        if active_root != _next_active_root(
            prior_active_root=prior_root,
            segments=segments,
            resets_history=resets,
        ):
            raise CanonicalLineageError("lineage node active root differs")
        if latest_root is None:
            latest_root = active_root
        if expected_parent_root is not None and active_root != expected_parent_root:
            raise CanonicalLineageError("lineage node parent active root differs")
        if resets:
            resolved = (*segments, *reversed(suffix))
            ordered = tuple(sorted(resolved, key=lambda item: (str(item["start"]), _segment_path_key(item))))
            assert latest_root is not None
            return ordered, latest_root
        suffix.extend(reversed(segments))
        if prior_root is None:
            raise CanonicalLineageError("lineage append node lacks prior active root")
        expected_parent_root = prior_root
        parent = node.get("parent_head")
        if not isinstance(parent, Mapping):
            raise CanonicalLineageError("lineage append node lacks parent head")
        current = parent
    raise CanonicalLineageError("lineage node chain has no reset anchor")


def _normalize_transition_updates(
    values: Sequence[Mapping[str, Any]],
    *,
    dataset: str,
    prior_heads: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for raw in values:
        if not isinstance(raw, Mapping) or set(raw) != {
            "instrument",
            "mode",
            "segments",
        }:
            raise CanonicalLineageError("lineage update fields differ")
        code = _instrument(raw.get("instrument"))
        mode = str(raw.get("mode", "")).upper()
        if mode not in {"APPEND", "REPLACE", "CREATE"}:
            raise CanonicalLineageError("lineage update mode is invalid")
        segment_values = raw.get("segments")
        if not isinstance(segment_values, list) or not segment_values:
            raise CanonicalLineageError("lineage update segments are empty")
        grouped = _group_segments(segment_values, dataset=dataset)
        if set(grouped) != {code} or code in output:
            raise CanonicalLineageError("lineage update instrument is duplicated or differs")
        segments = grouped[code]
        prior = prior_heads.get(code)
        if mode in {"APPEND", "REPLACE"} and prior is None:
            raise CanonicalLineageError("lineage update references an unknown prior instrument")
        if mode == "CREATE" and prior is not None:
            raise CanonicalLineageError("lineage create update already has a prior instrument")
        if mode == "APPEND" and str(segments[0]["start"]) <= str(prior["end"]):
            raise CanonicalLineageError("lineage append update overlaps prior history")
        output[code] = {"mode": mode, "segments": segments}
    return {code: output[code] for code in sorted(output)}


def _group_segments(values: Iterable[Mapping[str, Any]], *, dataset: str) -> dict[str, tuple[dict[str, Any], ...]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for raw in values:
        item = _segment(raw, dataset=dataset)
        grouped.setdefault(item["instrument"], []).append(item)
    output: dict[str, tuple[dict[str, Any], ...]] = {}
    for code, rows in grouped.items():
        ordered = tuple(sorted(rows, key=lambda item: (str(item["start"]), _segment_path_key(item))))
        path_keys = [_segment_path_key(item) for item in ordered]
        if len(path_keys) != len(set(path_keys)):
            raise CanonicalLineageError(f"lineage active segment path is duplicated: {code}")
        for before, after in zip(ordered, ordered[1:]):
            if str(after["start"]) <= str(before["end"]):
                raise CanonicalLineageError(f"lineage active segments overlap: {code}")
        output[code] = ordered
    return output


def _segment(value: Mapping[str, Any], *, dataset: str) -> dict[str, Any]:
    code = _stock_instrument(value.get("instrument"))
    root = _relative(value.get("root_relative_path"), label="lineage segment root")
    relative = _relative(value.get("relative_path"), label="lineage segment file")
    if len(PurePosixPath(relative).parts) != 1 or not relative.endswith(".csv"):
        raise CanonicalLineageError("lineage segment file path is invalid")
    allowed = (
        root == f"{dataset}/csv"
        or root.startswith(f"{dataset}/csv_deltas/")
        or root.startswith(f"{dataset}/csv_overrides/")
    )
    rows = value.get("rows")
    size = value.get("size_bytes")
    start = value.get("start")
    end = value.get("end")
    if (
        not allowed
        or isinstance(rows, bool)
        or not isinstance(rows, int)
        or rows <= 0
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size <= 0
        or not isinstance(start, str)
        or not isinstance(end, str)
        or not start
        or end < start
    ):
        raise CanonicalLineageError("lineage segment metadata is invalid")
    sha = ensure_sha256(str(value.get("sha256", "")), field="lineage_segment_sha256")
    body = {
        "instrument": code,
        "root_relative_path": root,
        "relative_path": relative,
        "rows": rows,
        "sha256": sha,
        "size_bytes": size,
        "start": start,
        "end": end,
    }
    return {
        **body,
        "segment_identity": digest_named_fields(CANONICAL_LINEAGE_SEGMENT_SCHEMA, body),
    }


def _inventory_item(value: Mapping[str, Any], *, dataset: str) -> dict[str, Any]:
    code = _instrument(value.get("instrument"))
    root = _relative(value.get("root_relative_path"), label="lineage inventory root")
    relative = _relative(value.get("relative_path"), label="lineage inventory file")
    if (
        not (
            root == f"{dataset}/csv"
            or root.startswith(f"{dataset}/csv_deltas/")
            or root.startswith(f"{dataset}/csv_overrides/")
        )
        or len(PurePosixPath(relative).parts) != 1
        or relative != f"{code.casefold()}.csv"
    ):
        raise CanonicalLineageError("lineage inventory path differs")
    rows = value.get("rows")
    size = value.get("size_bytes")
    active = value.get("active")
    if (
        isinstance(rows, bool)
        or not isinstance(rows, int)
        or rows < 0
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size <= 0
        or type(active) is not bool
    ):
        raise CanonicalLineageError("lineage inventory metadata is invalid")
    sha = ensure_sha256(str(value.get("sha256", "")), field="lineage_inventory_sha256")
    body = {
        "instrument": code,
        "root_relative_path": root,
        "relative_path": relative,
        "rows": rows,
        "sha256": sha,
        "size_bytes": size,
        "start": value.get("start"),
        "end": value.get("end"),
        "active": active,
    }
    return {
        **body,
        "inventory_identity": digest_named_fields(CANONICAL_LINEAGE_INVENTORY_ROOT_SCHEMA, body),
    }


def _next_active_root(
    *,
    prior_active_root: str | None,
    segments: Sequence[Mapping[str, Any]],
    resets_history: bool,
) -> str:
    if not segments:
        raise CanonicalLineageError("lineage active-root segment set is empty")
    prior = (
        None if prior_active_root is None else ensure_sha256(str(prior_active_root), field="lineage_prior_active_root")
    )
    if not resets_history and prior is None:
        raise CanonicalLineageError("lineage append active root lacks a parent")
    return digest_named_fields(
        CANONICAL_LINEAGE_ACTIVE_ROOT_SCHEMA,
        {
            "prior_active_root": prior,
            "resets_history": resets_history,
            "segments": [
                {
                    "path": _segment_path_key(item),
                    "segment_identity": str(item["segment_identity"]),
                }
                for item in segments
            ],
        },
    )


def _inventory_roots(values: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for item in values:
        grouped.setdefault(str(item["root_relative_path"]), []).append(item)
    for root, rows in grouped.items():
        identities = [f"{item['instrument']}:{item['relative_path']}" for item in rows]
        if len(identities) != len(set(identities)):
            raise CanonicalLineageError(f"lineage inventory path is duplicated: {root}")
    return {
        root: merkle_root_from_named_digests(
            CANONICAL_LINEAGE_INVENTORY_ROOT_SCHEMA,
            (
                (
                    f"{item['instrument']}:{item['relative_path']}",
                    str(item["inventory_identity"]),
                )
                for item in rows
            ),
        )
        for root, rows in sorted(grouped.items())
    }


def _segment_path_key(item: Mapping[str, Any]) -> str:
    return f"{item['root_relative_path']}/{item['relative_path']}".casefold()


def _seal_json(root: Path, relative_path: str, value: Mapping[str, Any]) -> LineageRef:
    relative = _relative(relative_path, label="lineage object path")
    payload = canonical_json_bytes(value)
    if not payload or len(payload) > MAX_LINEAGE_OBJECT_BYTES:
        raise CanonicalLineageError("lineage object exceeds bounded size")
    target = root / PurePosixPath(relative)
    target.parent.mkdir(parents=True, exist_ok=True)
    _assert_plain_chain(target.parent)

    def matches_existing() -> bool:
        _assert_plain_node(target)
        size = target.stat().st_size
        if size != len(payload) or size > MAX_LINEAGE_OBJECT_BYTES:
            return False
        with target.open("rb") as handle:
            observed = handle.read(MAX_LINEAGE_OBJECT_BYTES + 1)
        return observed == payload

    if target.exists():
        if not matches_existing():
            raise CanonicalLineageError("lineage append-only object conflicts")
    else:
        temporary = target.parent / f".{target.name}.{uuid.uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                if not matches_existing():
                    raise CanonicalLineageError("lineage append-only object conflicts")
        finally:
            temporary.unlink(missing_ok=True)
    sha = sha256_hex(payload)
    identity = digest_named_fields(
        CANONICAL_LINEAGE_REF_SCHEMA,
        {"relative_path": relative, "size_bytes": len(payload), "sha256": sha},
    )
    return LineageRef(relative, len(payload), sha, identity)


def _read_json_ref(root: Path, ref: LineageRef, *, expected_schema: str) -> dict[str, Any]:
    path = root / PurePosixPath(ref.relative_path)
    _assert_plain_chain(path)
    metadata = path.stat()
    if metadata.st_size != ref.size_bytes or metadata.st_size > MAX_LINEAGE_OBJECT_BYTES:
        raise CanonicalLineageError("lineage object size differs")
    raw = path.read_bytes()
    if sha256_hex(raw) != ref.sha256:
        raise CanonicalLineageError("lineage object digest differs")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalLineageError("lineage object is unreadable") from exc
    if not isinstance(value, dict) or value.get("schema_version") != expected_schema:
        raise CanonicalLineageError("lineage object schema differs")
    return value


def _read_json_path(root: Path, relative: str, *, expected_schema: str) -> dict[str, Any]:
    path = root / PurePosixPath(_relative(relative, label="lineage object path"))
    _assert_plain_chain(path)
    if path.stat().st_size > MAX_LINEAGE_OBJECT_BYTES:
        raise CanonicalLineageError("lineage object exceeds bounded size")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalLineageError("lineage object is unreadable") from exc
    if not isinstance(value, dict) or value.get("schema_version") != expected_schema:
        raise CanonicalLineageError("lineage object schema differs")
    return value


def _component_root(path: Path) -> Path:
    requested = Path(path).absolute()
    _assert_plain_chain(requested)
    resolved = requested.resolve(strict=True)
    if not resolved.is_dir():
        raise CanonicalLineageError("lineage component root is not a directory")
    return resolved


def _assert_plain_chain(path: Path) -> None:
    absolute = Path(path).absolute()
    current = Path(absolute.anchor)
    if current.exists():
        _assert_plain_node(current)
    for part in absolute.parts[1:]:
        current /= part
        _assert_plain_node(current)


def _assert_plain_node(path: Path) -> None:
    try:
        value = os.lstat(path)
    except OSError as exc:
        raise CanonicalLineageError("lineage path is unavailable") from exc
    if stat.S_ISLNK(value.st_mode) or (int(getattr(value, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise CanonicalLineageError("lineage path traverses a link/reparse point")


def _validate_descriptor_size(value: Mapping[str, Any]) -> None:
    if len(canonical_json_bytes(value)) > MAX_LINEAGE_DESCRIPTOR_BYTES:
        raise CanonicalLineageError("lineage descriptor exceeds 128 KiB")


def _relative(value: Any, *, label: str) -> str:
    try:
        relative = normalize_root_relative_path(str(value))
    except Exception as exc:
        raise CanonicalLineageError(f"{label} is invalid") from exc
    if relative != str(value).replace("\\", "/").casefold():
        raise CanonicalLineageError(f"{label} is not canonical lowercase")
    return relative


def _instrument(value: Any) -> str:
    code = str(value).upper()
    if _CODE.fullmatch(code) is None:
        raise CanonicalLineageError("lineage instrument is invalid")
    return code


def _stock_instrument(value: Any) -> str:
    code = str(value).upper()
    if _STOCK_CODE.fullmatch(code) is None:
        raise CanonicalLineageError("lineage stock instrument is invalid")
    return code


def _event_key(value: Any) -> str:
    key = str(value).lower()
    if _KEY.fullmatch(key) is None:
        raise CanonicalLineageError("lineage event key is invalid")
    return key


def _dataset(value: str) -> str:
    if value not in {"daily_bin", "minute_bin"}:
        raise CanonicalLineageError("lineage dataset is invalid")
    return value


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CanonicalLineageError(f"{label} is invalid")
    return value


__all__ = [
    "CANONICAL_LINEAGE_CAPABILITY",
    "CANONICAL_LINEAGE_EVENT_SCHEMA",
    "CANONICAL_LINEAGE_NAMESPACE_MANIFEST_SCHEMA",
    "CANONICAL_LINEAGE_SCHEMA",
    "CanonicalLineageError",
    "LineageRef",
    "LineageWriteResult",
    "MAX_LINEAGE_DESCRIPTOR_BYTES",
    "MAX_LINEAGE_OBJECT_BYTES",
    "active_segments",
    "event_inventory",
    "instrument_summaries",
    "is_lineage_v3",
    "latest_event_inventory",
    "legacy_active_segments",
    "lineage_inventory_history",
    "lineage_object_paths",
    "lineage_bucket",
    "lineage_event_key",
    "lineage_event_relative_path",
    "migrate_legacy_and_write_transition",
    "migrate_legacy_and_write_transition_updates",
    "namespace_manifest",
    "planned_lineage_paths",
    "validate_lineage_descriptor",
    "write_genesis",
    "write_legacy_anchor",
    "write_transition",
    "write_transition_updates",
]
