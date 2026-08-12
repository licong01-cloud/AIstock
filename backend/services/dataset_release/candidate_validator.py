"""Bounded, value-level validation for an unpublished QE candidate staging tree."""

from __future__ import annotations

import bisect
import csv
import hashlib
import io
import json
import math
import os
import re
import stat
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Iterator, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.data_service.moneyflow_contract import (
    MONEYFLOW_FACTOR_COLUMNS,
    MONEYFLOW_UNIT_CONTRACT_VERSION,
)

from .canonical import (
    digest_named_fields,
    ensure_sha256,
    merkle_root_from_named_digests,
    normalize_root_relative_path,
)
from .canonical_lineage import (
    CANONICAL_LINEAGE_CAPABILITY,
    CANONICAL_LINEAGE_NAMESPACE_MANIFEST_SCHEMA,
    CANONICAL_LINEAGE_SCHEMA,
    active_segments as lineage_active_segments,
    event_inventory as lineage_event_inventory,
    instrument_summaries as lineage_instrument_summaries,
    is_lineage_v3,
    lineage_inventory_history,
    lineage_object_paths,
    validate_lineage_descriptor,
)
from .candidate_consumer_smoke import (
    CandidateConsumerSmokeError,
    validate_candidate_consumer_smoke_receipt,
)
from .contracts import Component, ComponentAction
from .component_artifact_manifest import (
    COMPONENT_ARTIFACT_FILE_SCHEMA,
    ComponentArtifactEvidence,
)
from .component_manifest_producer import (
    CandidateArtifactSnapshot,
    ComponentManifestProductionError,
    snapshot_candidate_artifacts,
    verify_candidate_artifact_snapshot,
)
from .copy_on_write import CopyOnWriteError, tree_merkle
from .daily_minute_materializer import (
    DAILY_FIELDS,
    MINUTE_FIELDS,
    SEALED_QLIB_CSV_COMPOSITE_SCHEMA,
    SEALED_QLIB_CSV_ROWS_SCHEMA,
)
from .decision import DECISION_SCHEMA_VERSION
from .errors import DatasetReleaseError
from .factor_materializer import (
    FACTOR_H5_DATASETS,
    FACTOR_H5_DENSITY_CONTRACTS,
    FACTOR_H5_DTYPES,
    FACTOR_H5_SCHEMAS,
    FACTOR_H5_SCHEMA_VERSION,
    STATIC_DATASET,
)
from .index_contract import (
    DOMESTIC_INDEX_DEFINITIONS,
    HMM_BENCHMARK_CODE,
    INDEX_H5_COLUMNS,
    INDEX_H5_DTYPES,
    INDEX_QLIB_FIELDS,
    INDEX_SCHEMA_VERSION,
    INDEX_UNIVERSE_VERSION,
    index_contract_digest,
    index_contract_payload,
)
from .minute_overlay import canonical_session_times
from .pit import FrozenPitSnapshot
from .profile import DatasetProfile
from .static_schema import (
    STATIC_COLUMN_DTYPES,
    STATIC_MONEYFLOW_DERIVED_COLUMNS,
    STATIC_SCHEMA_VERSION,
    static_schema_digest,
)
from .signoff import ComponentSignoff, ValidationResult, ValidationStatus
from .streaming_artifacts import (
    ArtifactSchemaDrift,
    iter_hdf_frames,
    iter_parquet_frames,
    sha256_file,
)


CANDIDATE_VALIDATION_SCHEMA = "dataset_release_candidate_validation_v1"
_MAX_JSON_EVIDENCE_BYTES = 32 * 1024 * 1024
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_DAILY_VALUE_MAP = {
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "amount": "amount",
    "factor": "factor",
}
_INDEX_VALUE_MAP = {
    "open": "idx_open_point",
    "high": "idx_high_point",
    "low": "idx_low_point",
    "close": "idx_close_point",
    "volume": "idx_volume_share_equiv",
    "amount": "idx_amount_cny",
    "up_limit_price": "idx_pre_close_point",
    "down_limit_price": "idx_pre_close_point",
    "prev_close": "idx_pre_close_point",
}
_INDEX_CONSTANT_VALUES = {"factor": 1.0, "limit_up": 0.0, "limit_down": 0.0}
_COMPOSITE_APPEND_CONTRACT = "instrument_datetime_strict_append_segments_v1"
_COMPOSITE_OVERRIDE_CONTRACT = "instrument_active_segments_with_explicit_overrides_v1"
_COMPOSITE_SEGMENT_FIELDS = frozenset(
    {
        "instrument",
        "root_relative_path",
        "relative_path",
        "rows",
        "sha256",
        "size_bytes",
        "start",
        "end",
    }
)
_COMPOSITE_SUMMARY_FIELDS = frozenset({"instrument", "rows", "segments", "start", "end"})
_SUPERSEDED_SEGMENT_FIELDS = frozenset({"root_relative_path", "relative_path", "sha256"})
_MONTH_KEY = re.compile(r"[0-9]{6}\Z")
_OVERRIDE_KEY = re.compile(r"[0-9a-f]{16}\Z")
_STOCK_CODE = re.compile(r"[0-9]{6}\.(?:SH|SZ)\Z")
_CANONICAL_CODE = re.compile(r"[0-9]{6}\.(?:SH|SZ|CSI)\Z")
_MONTH_TEXT = re.compile(r"[0-9]{4}-(?:0[1-9]|1[0-2])\Z")
_PARTITION_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_ACTION_ENTRY_FIELDS = frozenset(
    {
        "component",
        "partition_key",
        "action",
        "reason",
        "changed_fingerprints",
        "invalidation_edges",
        "estimated_work",
        "frozen_reuse",
    }
)
_BUILD_ACTIONS = frozenset(
    {
        ComponentAction.REUSE,
        ComponentAction.INCREMENTAL,
        ComponentAction.SELECTIVE_REBUILD,
        ComponentAction.FULL_REBUILD,
    }
)


@dataclass(frozen=True, slots=True)
class _CalendarBoundaryIndex:
    unique_dates: tuple[str, ...]
    first_positions: tuple[int, ...]
    last_positions: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class _IndexArtifactAudit:
    rows: int
    codes: frozenset[str]
    coverage: Mapping[str, frozenset[str]]
    content_digest: str
    file_sha256: str
    size_bytes: int


@dataclass(slots=True)
class _OrderedArtifactCursor:
    frames: Iterator[pd.DataFrame]
    label: str
    current: pd.DataFrame | None = None

    def take(
        self,
        target_index: pd.MultiIndex,
        *,
        columns: Sequence[str],
    ) -> pd.DataFrame:
        target_keys = [_index_key(value) for value in target_index]
        if not target_keys:
            return pd.DataFrame(columns=list(columns), index=target_index)
        if any(current <= previous for previous, current in zip(target_keys, target_keys[1:])):
            raise CandidateValidationError(f"{self.label} lookup keys are not strictly ordered")
        parts: list[pd.DataFrame] = []
        position = 0
        while position < len(target_keys):
            if self.current is None:
                self.current = next(self.frames, None)
                while self.current is not None and self.current.empty:
                    self.current = next(self.frames, None)
            if self.current is None:
                raise CandidateValidationError(f"{self.label} omits moneyflow lookup keys")
            missing_columns = sorted(set(columns).difference(self.current.columns))
            if missing_columns:
                raise CandidateValidationError(f"{self.label} lookup columns are missing: {missing_columns}")
            first = _index_key(self.current.index[0])
            last = _index_key(self.current.index[-1])
            target = target_keys[position]
            if last < target:
                self.current = None
                continue
            if first > target:
                raise CandidateValidationError(f"{self.label} omits moneyflow lookup key: {target}")
            stop = bisect.bisect_right(target_keys, last, lo=position)
            requested = pd.MultiIndex.from_tuples(target_keys[position:stop], names=["datetime", "instrument"])
            locations = self.current.index.get_indexer(requested)
            if bool((locations < 0).any()):
                raise CandidateValidationError(f"{self.label} omits moneyflow lookup keys")
            part = self.current.iloc[locations].loc[:, list(columns)].copy()
            part.index = requested
            parts.append(part)
            position = stop
            if position < len(target_keys):
                self.current = None
        combined = pd.concat(parts) if len(parts) > 1 else parts[0]
        if not combined.index.equals(target_index):
            raise CandidateValidationError(f"{self.label} lookup order differs from moneyflow")
        return combined


class CandidateValidationError(DatasetReleaseError):
    code = "BLOCKED_CANDIDATE_VALIDATION_FAILED"


@dataclass(frozen=True, slots=True)
class CandidateComponentTransitionAuthority:
    """Identity-bound authority for one component's candidate transition.

    ``action_entry`` is the exact immutable action-plan row whose digest is
    bound by :class:`CandidateValidationSpec`.  Reused baselines are supplied
    as the CAS-normalized component evidence plus the exact component root
    that was verified before copy-on-write preparation.  A current candidate
    manifest is never accepted as evidence for its own historical lineage.
    """

    component: Component
    action: ComponentAction
    action_entry: Mapping[str, Any]
    frozen_reuse: Mapping[str, Any] | None = None
    baseline_component_root: Path | None = None
    baseline_evidence: ComponentArtifactEvidence | None = None

    def __post_init__(self) -> None:
        if self.action not in _BUILD_ACTIONS:
            raise CandidateValidationError(f"candidate transition action is not build-capable: {self.action.value}")
        if set(self.action_entry) != _ACTION_ENTRY_FIELDS:
            raise CandidateValidationError(f"candidate transition action entry fields drifted: {self.component.value}")
        if (
            self.action_entry.get("component") != self.component.value
            or self.action_entry.get("action") != self.action.value
            or self.action_entry.get("frozen_reuse") != self.frozen_reuse
        ):
            raise CandidateValidationError(
                f"candidate transition action entry identity differs: {self.component.value}"
            )
        partition_key = self.action_entry.get("partition_key")
        if not isinstance(partition_key, str) or not partition_key.strip():
            raise CandidateValidationError(f"candidate transition partition key is invalid: {self.component.value}")
        if self.action is ComponentAction.FULL_REBUILD:
            if any(
                value is not None
                for value in (
                    self.frozen_reuse,
                    self.baseline_component_root,
                    self.baseline_evidence,
                )
            ):
                raise CandidateValidationError(f"FULL component carries reuse authority: {self.component.value}")
            return
        if (
            not isinstance(self.frozen_reuse, Mapping)
            or self.baseline_component_root is None
            or self.baseline_evidence is None
            or not self.baseline_evidence.complete
            or self.baseline_evidence.component is not self.component
        ):
            raise CandidateValidationError(
                f"candidate transition baseline authority is missing: {self.component.value}"
            )
        if self.frozen_reuse.get("component_partition_key") != partition_key:
            raise CandidateValidationError(f"candidate frozen baseline partition key differs: {self.component.value}")
        evidence = self.baseline_evidence
        assert evidence.component_identity is not None
        assert evidence.file_identity is not None
        assert evidence.filesystem_tree_merkle is not None
        assert evidence.component_root_relative_path is not None
        for field, expected in (
            ("artifact_id", evidence.component_identity),
            ("file_identity", evidence.file_identity),
            ("manifest_root", evidence.filesystem_tree_merkle),
            (
                "component_root_relative_path",
                evidence.component_root_relative_path,
            ),
        ):
            if self.frozen_reuse.get(field) != expected:
                raise CandidateValidationError(f"candidate frozen baseline {field} differs: {self.component.value}")
        if self.action is ComponentAction.REUSE and any(
            self.frozen_reuse.get(field)
            for field in (
                "mutation_set",
                "replace_existing_targets",
                "create_new_targets",
                "invalidation_scopes",
            )
        ):
            raise CandidateValidationError(f"REUSE component carries mutation authority: {self.component.value}")
        if self.action in {
            ComponentAction.INCREMENTAL,
            ComponentAction.SELECTIVE_REBUILD,
        }:
            replace = self.frozen_reuse.get("replace_existing_targets")
            create = self.frozen_reuse.get("create_new_targets")
            mutation = self.frozen_reuse.get("mutation_set")
            scopes = self.frozen_reuse.get("invalidation_scopes")
            if not all(isinstance(value, list) for value in (replace, create, mutation, scopes)):
                raise CandidateValidationError(
                    f"candidate frozen mutation authority is invalid: {self.component.value}"
                )
            if (
                any(not isinstance(value, str) for value in (*replace, *create, *mutation))
                or any(not isinstance(value, Mapping) for value in scopes)
                or len(set((*replace, *create))) != len((*replace, *create))
                or len(set(mutation)) != len(mutation)
                or sorted((*replace, *create)) != sorted(mutation)
            ):
                raise CandidateValidationError(f"candidate frozen mutation set differs: {self.component.value}")
            if self.component in {Component.DAILY_BIN, Component.MINUTE_BIN}:
                lineage = self.frozen_reuse.get("canonical_lineage")
                has_lineage_targets = any(str(value).replace("\\", "/").startswith("csv_lineage/") for value in create)
                if lineage is None and not has_lineage_targets:
                    return
                if (
                    not isinstance(lineage, Mapping)
                    or lineage.get("capability") != CANONICAL_LINEAGE_CAPABILITY
                    or not isinstance(lineage.get("planned_buckets"), list)
                    or not has_lineage_targets
                ):
                    raise CandidateValidationError(
                        f"candidate frozen lineage authority is invalid: {self.component.value}"
                    )


@dataclass(frozen=True, slots=True)
class _CanonicalTransitionContext:
    component: Component
    action: ComponentAction
    baseline_root: Path
    baseline_source: Mapping[str, Any]
    baseline_evidence: ComponentArtifactEvidence
    frozen_reuse: Mapping[str, Any]
    authorized_create_paths: frozenset[str]
    invalidation_scopes: tuple[Mapping[str, Any], ...]
    verified_candidate_files: Mapping[str, tuple[int, str]] | None = None


@dataclass(frozen=True, slots=True)
class _ComponentTreeTransition:
    authority: CandidateComponentTransitionAuthority
    baseline_root: Path
    candidate_root: Path
    evidence_files: Mapping[str, Any]
    baseline_files: Mapping[str, Any]
    candidate_files: Mapping[str, Any]


_COMPONENT_CANDIDATE_ROOT = {
    Component.DAILY_BIN: "daily_bin",
    Component.MINUTE_BIN: "minute_bin",
    Component.FACTOR_H5_STATIC: "factor_bundle",
    Component.DOMESTIC_INDEX_CONTEXT: "index_context",
}


@dataclass(frozen=True, slots=True)
class CandidateValidationSpec:
    candidate_root: Path
    profile: DatasetProfile
    cutoff: date
    trading_dates: tuple[date, ...]
    pit_snapshot: FrozenPitSnapshot
    factor_receipt: Mapping[str, Any]
    daily_receipt: Mapping[str, Any]
    minute_receipt: Mapping[str, Any]
    daily_materialization_receipt_file: Mapping[str, Any]
    minute_materialization_receipt_file: Mapping[str, Any]
    index_materialization_receipt_file: Mapping[str, Any]
    daily_preparation_receipt: Mapping[str, Any] | None
    minute_preparation_receipt: Mapping[str, Any] | None
    minute_canonical_source: Mapping[str, Any]
    index_receipt: Mapping[str, Any]
    external_consumer_smoke: Mapping[str, Any]
    minute_overlay_summary: Mapping[str, Any]
    actions: Mapping[str, str]
    component_fingerprints: Mapping[str, str]
    validation_fingerprint: str
    action_plan_digest: str
    transition_authority: Mapping[str, CandidateComponentTransitionAuthority]
    require_production_consumer_smoke: bool = True

    def __post_init__(self) -> None:
        if self.cutoff != self.pit_snapshot.cutoff:
            raise CandidateValidationError("candidate/PIT cutoff differs")
        if not self.trading_dates or tuple(sorted(set(self.trading_dates))) != self.trading_dates:
            raise CandidateValidationError("trading dates must be unique and ordered")
        if self.trading_dates[-1] != self.cutoff:
            raise CandidateValidationError("trading calendar does not end at cutoff")
        if tuple(self.profile.components) != tuple(Component):
            raise CandidateValidationError("candidate profile component contract drifted")
        expected = {item.value for item in Component}
        if set(self.actions) != expected or set(self.component_fingerprints) != expected:
            raise CandidateValidationError("candidate signoff inputs omit components")
        for component, value in self.actions.items():
            try:
                ComponentAction(str(value))
            except ValueError as exc:
                raise CandidateValidationError(f"invalid component action: {component}={value}") from exc
        ensure_sha256(self.validation_fingerprint, field="validation_fingerprint")
        for component, value in self.component_fingerprints.items():
            ensure_sha256(value, field=f"component_fingerprint:{component}")
        ensure_sha256(self.action_plan_digest, field="action_plan_digest")
        if set(self.transition_authority) != expected:
            raise CandidateValidationError("candidate transition authority omits components")
        action_entries: list[Mapping[str, Any]] = []
        for component in Component:
            authority = self.transition_authority[component.value]
            if (
                not isinstance(authority, CandidateComponentTransitionAuthority)
                or authority.component is not component
                or authority.action.value != self.actions[component.value]
            ):
                raise CandidateValidationError(f"candidate transition authority differs: {component.value}")
            action_entries.append(dict(authority.action_entry))
        observed_action_plan_digest = digest_named_fields(
            DECISION_SCHEMA_VERSION,
            {
                "actions": sorted(
                    action_entries,
                    key=lambda value: (
                        str(value["component"]),
                        str(value["partition_key"]),
                    ),
                )
            },
        )
        if observed_action_plan_digest != self.action_plan_digest:
            raise CandidateValidationError("candidate transition authority/action-plan digest differs")
        for label, value in (
            ("daily", self.daily_preparation_receipt),
            ("minute", self.minute_preparation_receipt),
        ):
            if value is not None and not isinstance(value, Mapping):
                raise CandidateValidationError(f"{label} preparation receipt authority is invalid")
        if (
            not isinstance(self.daily_materialization_receipt_file, Mapping)
            or not isinstance(self.minute_materialization_receipt_file, Mapping)
            or not isinstance(self.index_materialization_receipt_file, Mapping)
        ):
            raise CandidateValidationError("materialization receipt file authority is invalid")
        if (
            self.minute_canonical_source.get("schema_version")
            not in {
                SEALED_QLIB_CSV_ROWS_SCHEMA,
                SEALED_QLIB_CSV_COMPOSITE_SCHEMA,
                CANONICAL_LINEAGE_SCHEMA,
            }
            or self.minute_canonical_source.get("dataset") != "minute_bin"
        ):
            raise CandidateValidationError("sealed minute canonical source contract is missing or invalid")
        if type(self.require_production_consumer_smoke) is not bool:
            raise CandidateValidationError("consumer smoke requirement is invalid")


@dataclass(frozen=True, slots=True)
class CandidateValidationReport:
    payload: Mapping[str, Any]
    components: tuple[ComponentSignoff, ...]
    validations: tuple[ValidationResult, ...]
    artifact_snapshot: CandidateArtifactSnapshot


def _validate_component_tree_transition(
    spec: CandidateValidationSpec,
    *,
    candidate_root: Path,
    component: Component,
    artifact_snapshot: CandidateArtifactSnapshot,
) -> _ComponentTreeTransition | None:
    authority = spec.transition_authority[component.value]
    if authority.action is ComponentAction.FULL_REBUILD:
        return None
    assert authority.frozen_reuse is not None
    assert authority.baseline_component_root is not None
    assert authority.baseline_evidence is not None
    expected_root = _COMPONENT_CANDIDATE_ROOT[component]
    evidence = authority.baseline_evidence
    if evidence.component_root_relative_path != expected_root:
        raise CandidateValidationError(f"{component.value} baseline component root identity differs")
    evidence_files = {
        item.relative_path: item for partition in evidence.artifact_partitions for item in partition.files
    }
    if not evidence_files or sum(len(partition.files) for partition in evidence.artifact_partitions) != len(
        evidence_files
    ):
        raise CandidateValidationError(f"{component.value} baseline component file authority is empty/duplicate")
    for relative, item in evidence_files.items():
        expected_identity = digest_named_fields(
            COMPONENT_ARTIFACT_FILE_SCHEMA,
            {
                "relative_path": relative,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
                "instrument": item.instrument,
            },
        )
        if item.file_identity != expected_identity:
            raise CandidateValidationError(f"{component.value} baseline file identity differs: {relative}")
    expected_file_identity = merkle_root_from_named_digests(
        "dataset_release_component_files_v1",
        ((relative, evidence_files[relative].file_identity) for relative in sorted(evidence_files)),
    )
    if expected_file_identity != evidence.file_identity:
        raise CandidateValidationError(f"{component.value} baseline aggregate file identity differs")
    baseline_root = _plain_root(authority.baseline_component_root)
    component_root = _plain_root(candidate_root / expected_root)
    baseline_files, baseline_merkle = _normalized_tree_file_map(baseline_root, label=f"{component.value} baseline")
    candidate_files = _normalized_snapshot_component_file_map(
        artifact_snapshot,
        component_root_name=expected_root,
        label=f"{component.value} candidate",
    )
    if set(baseline_files) != set(evidence_files) or baseline_merkle != evidence.filesystem_tree_merkle:
        raise CandidateValidationError(f"{component.value} baseline component path/Merkle authority differs")
    for relative, expected in evidence_files.items():
        observed = baseline_files[relative]
        if observed.size_bytes != expected.size_bytes or observed.sha256 != expected.sha256:
            raise CandidateValidationError(f"{component.value} baseline component bytes differ: {relative}")
    frozen = authority.frozen_reuse
    replace_values = frozen.get("replace_existing_targets")
    create_values = frozen.get("create_new_targets")
    if not isinstance(replace_values, list) or not isinstance(create_values, list):
        raise CandidateValidationError(f"{component.value} frozen replace/create authority is invalid")
    replace = frozenset(
        _component_relative_path(value, label=f"{component.value} frozen replace target") for value in replace_values
    )
    create = frozenset(
        _component_relative_path(value, label=f"{component.value} frozen create target") for value in create_values
    )
    if (
        len(replace) != len(replace_values)
        or len(create) != len(create_values)
        or replace.intersection(create)
        or not replace.issubset(baseline_files)
        or create.intersection(baseline_files)
    ):
        raise CandidateValidationError(f"{component.value} frozen mutation namespace differs from baseline")
    expected_candidate_paths = set(baseline_files).union(create)
    if set(candidate_files) != expected_candidate_paths:
        raise CandidateValidationError(f"{component.value} candidate namespace differs from baseline plus creates")
    if authority.action is ComponentAction.REUSE:
        if replace or create:
            raise CandidateValidationError(f"{component.value} REUSE candidate differs from baseline")
    for relative in set(baseline_files).difference(replace):
        before = baseline_files[relative]
        after = candidate_files[relative]
        if before.size_bytes != after.size_bytes or before.sha256 != after.sha256:
            raise CandidateValidationError(f"{component.value} changed outside frozen replace scope: {relative}")
    return _ComponentTreeTransition(
        authority=authority,
        baseline_root=baseline_root,
        candidate_root=component_root,
        evidence_files=evidence_files,
        baseline_files=baseline_files,
        candidate_files=candidate_files,
    )


def _normalized_tree_file_map(root: Path, *, label: str) -> tuple[dict[str, Any], str]:
    try:
        files, merkle = tree_merkle(root)
    except CopyOnWriteError as exc:
        raise CandidateValidationError(f"{label} component tree is unsafe") from exc
    output: dict[str, Any] = {}
    for item in files:
        relative = normalize_root_relative_path(item.relative_path)
        if relative in output:
            raise CandidateValidationError(f"{label} has case-insensitive path collisions")
        output[relative] = item
    return output, merkle


def _normalized_snapshot_component_file_map(
    snapshot: CandidateArtifactSnapshot,
    *,
    component_root_name: str,
    label: str,
) -> dict[str, Any]:
    prefix = f"{component_root_name}/"
    output: dict[str, Any] = {}
    for item in snapshot.files:
        if not item.relative_path.startswith(prefix):
            continue
        physical_relative = item.relative_path.removeprefix(prefix)
        relative = normalize_root_relative_path(physical_relative)
        if relative in output:
            raise CandidateValidationError(f"{label} has case-insensitive path collisions")
        output[relative] = item
    if not output:
        raise CandidateValidationError(f"{label} component tree is empty")
    return output


def _prepare_canonical_transition_context(
    spec: CandidateValidationSpec,
    *,
    component: Component,
    dataset: str,
    tree_transition: _ComponentTreeTransition | None,
) -> _CanonicalTransitionContext | None:
    authority = spec.transition_authority[component.value]
    if authority.action is ComponentAction.FULL_REBUILD:
        if tree_transition is not None:
            raise CandidateValidationError(f"{dataset} FULL transition unexpectedly has baseline tree authority")
        return None
    if tree_transition is None or tree_transition.authority is not authority:
        raise CandidateValidationError(f"{dataset} component tree transition authority is missing")
    assert authority.frozen_reuse is not None
    baseline_root = tree_transition.baseline_root
    evidence = tree_transition.authority.baseline_evidence
    assert evidence is not None
    if evidence.component_root_relative_path != dataset:
        raise CandidateValidationError(f"{dataset} baseline component root identity differs")
    evidence_files = tree_transition.evidence_files
    receipt_relative = "materialization_receipt.json"
    if receipt_relative not in evidence_files:
        raise CandidateValidationError(f"{dataset} baseline materialization authority is missing")
    baseline_receipt = _load_json(baseline_root / receipt_relative)
    baseline_source = baseline_receipt.get("sealed_canonical_rows")
    if (
        baseline_receipt.get("status") != "PASS"
        or not isinstance(baseline_source, Mapping)
        or baseline_source.get("dataset") != dataset
        or baseline_source.get("schema_version")
        not in {
            SEALED_QLIB_CSV_ROWS_SCHEMA,
            SEALED_QLIB_CSV_COMPOSITE_SCHEMA,
            CANONICAL_LINEAGE_SCHEMA,
        }
    ):
        raise CandidateValidationError(f"{dataset} baseline canonical authority is invalid")
    replace_values = authority.frozen_reuse.get("replace_existing_targets")
    create_values = authority.frozen_reuse.get("create_new_targets")
    scopes = authority.frozen_reuse.get("invalidation_scopes")
    if (
        not isinstance(replace_values, list)
        or not isinstance(create_values, list)
        or not isinstance(scopes, list)
        or any(not isinstance(item, Mapping) for item in scopes)
    ):
        raise CandidateValidationError(f"{dataset} frozen replace/create/scope authority is invalid")
    authorized_replace_paths = frozenset(
        _component_relative_path(value, label=f"{dataset} frozen replace target") for value in replace_values
    )
    authorized_create_paths = frozenset(
        _component_relative_path(value, label=f"{dataset} frozen create target") for value in create_values
    )
    if len(authorized_create_paths) != len(create_values):
        raise CandidateValidationError(f"{dataset} frozen create authority is duplicate")
    baseline_lineage = {
        relative: evidence_files[relative] for relative in evidence_files if _is_canonical_lineage_file(relative)
    }
    if not baseline_lineage:
        raise CandidateValidationError(f"{dataset} baseline canonical file authority is empty")
    authorized_lineage_create = {value for value in authorized_create_paths if _is_canonical_lineage_file(value)}
    if set(baseline_lineage).intersection(authorized_lineage_create):
        raise CandidateValidationError(f"{dataset} frozen create authority targets baseline lineage")
    if set(baseline_lineage).intersection(authorized_replace_paths):
        raise CandidateValidationError(f"{dataset} frozen replace authority targets immutable baseline lineage")
    return _CanonicalTransitionContext(
        component=component,
        action=authority.action,
        baseline_root=baseline_root,
        baseline_source=dict(baseline_source),
        baseline_evidence=evidence,
        frozen_reuse=authority.frozen_reuse,
        authorized_create_paths=authorized_create_paths,
        invalidation_scopes=tuple(dict(item) for item in scopes),
        verified_candidate_files={
            relative: (item.size_bytes, item.sha256) for relative, item in tree_transition.candidate_files.items()
        },
    )


def _component_relative_path(value: Any, *, label: str) -> str:
    relative = _canonical_relative_path(value, label=label)
    normalized = relative.casefold()
    if normalized != relative:
        raise CandidateValidationError(f"{label} is not lowercase canonical")
    return normalized


def _is_canonical_lineage_file(relative: str) -> bool:
    parts = PurePosixPath(relative).parts
    if len(parts) == 2 and parts[0] == "csv":
        return parts[1].endswith(".csv")
    if len(parts) == 3 and parts[0] in {"csv_deltas", "csv_overrides"}:
        pattern = _MONTH_KEY if parts[0] == "csv_deltas" else _OVERRIDE_KEY
        return pattern.fullmatch(parts[1]) is not None and (parts[2] == "manifest.json" or parts[2].endswith(".csv"))
    return False


def _canonical_lineage_file_tree(component_root: Path) -> dict[str, Path]:
    output: dict[str, Path] = {}
    for namespace in ("csv", "csv_deltas", "csv_overrides"):
        root = component_root / namespace
        if not root.exists():
            continue
        _plain_root(root)
        for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(component_root).as_posix()):
            _assert_plain(path)
            if path.is_dir():
                continue
            physical_relative = path.relative_to(component_root).as_posix()
            relative = normalize_root_relative_path(physical_relative)
            if not path.is_file() or not _is_canonical_lineage_file(relative):
                raise CandidateValidationError(f"canonical namespace contains unauthorized entry: {physical_relative}")
            if relative in output:
                raise CandidateValidationError(f"canonical namespace path is duplicate: {relative}")
            output[relative] = path
    return output


def _validate_plain_directory_entries(
    root: Path,
    *,
    expected_directories: set[str],
    expected_files: set[str],
    label: str,
) -> None:
    observed_directories: set[str] = set()
    observed_files: set[str] = set()
    for child in root.iterdir():
        _assert_plain(child)
        if child.is_dir():
            observed_directories.add(child.name)
        elif child.is_file():
            observed_files.add(child.name)
        else:
            raise CandidateValidationError(f"{label} namespace contains a non-regular entry")
    if observed_directories != expected_directories or observed_files != expected_files:
        raise CandidateValidationError(
            f"{label} namespace differs: directories={sorted(observed_directories)} files={sorted(observed_files)}"
        )


def _validate_candidate_root_namespace(root: Path) -> None:
    component_directories = set(_COMPONENT_CANDIDATE_ROOT.values())
    observed_directories: set[str] = set()
    observed_files: set[str] = set()
    for child in root.iterdir():
        _assert_plain(child)
        if child.is_dir():
            observed_directories.add(child.name)
        elif child.is_file():
            observed_files.add(child.name)
        else:
            raise CandidateValidationError("candidate root namespace contains a non-regular entry")
    allowed_directories = component_directories.union({"metadata"})
    if (
        observed_files
        or not component_directories.issubset(observed_directories)
        or not observed_directories.issubset(allowed_directories)
    ):
        raise CandidateValidationError("candidate root namespace differs from the four components and metadata")
    if "metadata" in observed_directories:
        metadata = _plain_root(root / "metadata")
        _validate_plain_directory_entries(
            metadata,
            expected_directories=set(),
            expected_files={"index_context_manifest.json"},
            label="candidate metadata",
        )


def _validate_bin_component_namespace(
    root: Path,
    *,
    dataset: str,
    materialization_receipt: Mapping[str, Any],
    preparation_receipt: Mapping[str, Any] | None,
) -> None:
    root = _plain_root(root)
    observed_directories: set[str] = set()
    observed_files: set[str] = set()
    for child in root.iterdir():
        _assert_plain(child)
        if child.is_dir():
            observed_directories.add(child.name)
        elif child.is_file():
            observed_files.add(child.name)
        else:
            raise CandidateValidationError(f"{dataset} component namespace contains a non-regular entry")
    required_directories = {"qlib", "csv"}
    allowed_directories = required_directories.union({"csv_deltas", "csv_overrides", "csv_lineage"})
    expected_files = {"materialization_receipt.json"}
    if preparation_receipt is not None:
        expected_files.add("csv_preparation_receipt.json")
    if (
        not required_directories.issubset(observed_directories)
        or not observed_directories.issubset(allowed_directories)
        or observed_files != expected_files
    ):
        raise CandidateValidationError(f"{dataset} component namespace differs")
    if _load_json(root / "materialization_receipt.json") != dict(materialization_receipt):
        raise CandidateValidationError(f"{dataset} materialization receipt file differs from frozen authority")
    if preparation_receipt is not None and _load_json(root / "csv_preparation_receipt.json") != dict(
        preparation_receipt
    ):
        raise CandidateValidationError(f"{dataset} preparation receipt file differs from frozen authority")


class CandidateValidator:
    def validate(self, spec: CandidateValidationSpec) -> CandidateValidationReport:
        root = _plain_root(spec.candidate_root)
        _validate_candidate_root_namespace(root)
        try:
            artifact_snapshot = snapshot_candidate_artifacts(root)
            verify_candidate_artifact_snapshot(root, artifact_snapshot)
        except ComponentManifestProductionError as exc:
            raise CandidateValidationError("candidate artifact snapshot failed before validation") from exc
        max_rows = spec.profile.resource_policy.validation_read_chunk_rows
        _validate_bin_component_namespace(
            root / "daily_bin",
            dataset="daily_bin",
            materialization_receipt=spec.daily_materialization_receipt_file,
            preparation_receipt=spec.daily_preparation_receipt,
        )
        _validate_bin_component_namespace(
            root / "minute_bin",
            dataset="minute_bin",
            materialization_receipt=spec.minute_materialization_receipt_file,
            preparation_receipt=spec.minute_preparation_receipt,
        )
        daily_tree_transition = _validate_component_tree_transition(
            spec,
            candidate_root=root,
            component=Component.DAILY_BIN,
            artifact_snapshot=artifact_snapshot,
        )
        daily_transition = _prepare_canonical_transition_context(
            spec,
            component=Component.DAILY_BIN,
            dataset="daily_bin",
            tree_transition=daily_tree_transition,
        )
        del daily_tree_transition
        minute_tree_transition = _validate_component_tree_transition(
            spec,
            candidate_root=root,
            component=Component.MINUTE_BIN,
            artifact_snapshot=artifact_snapshot,
        )
        minute_transition = _prepare_canonical_transition_context(
            spec,
            component=Component.MINUTE_BIN,
            dataset="minute_bin",
            tree_transition=minute_tree_transition,
        )
        del minute_tree_transition
        _validate_component_tree_transition(
            spec,
            candidate_root=root,
            component=Component.FACTOR_H5_STATIC,
            artifact_snapshot=artifact_snapshot,
        )
        _validate_component_tree_transition(
            spec,
            candidate_root=root,
            component=Component.DOMESTIC_INDEX_CONTEXT,
            artifact_snapshot=artifact_snapshot,
        )
        expected_dates = tuple(value.isoformat() for value in spec.trading_dates)
        minute_dates = tuple(
            value.isoformat() for value in spec.trading_dates if value >= spec.profile.minute_start_date
        )
        spans = _span_map(spec.pit_snapshot)
        daily_expected_spans = _expected_pit_spans(spec.pit_snapshot, lower_bound=spec.profile.start_date)
        minute_expected_spans = _expected_pit_spans(spec.pit_snapshot, lower_bound=spec.profile.minute_start_date)

        daily = _validate_bin(
            root / "daily_bin" / "qlib",
            dataset="daily_bin",
            cutoff=spec.cutoff,
            expected_dates=expected_dates,
            expected_index_codes=spec.profile.index_codes,
            expected_stock_spans=daily_expected_spans,
        )
        minute = _validate_bin(
            root / "minute_bin" / "qlib",
            dataset="minute_bin",
            cutoff=spec.cutoff,
            expected_dates=minute_dates,
            expected_index_codes=(),
            expected_stock_spans=minute_expected_spans,
        )
        factor = _validate_factor_bundle(
            root / "factor_bundle",
            receipt=spec.factor_receipt,
            artifact_snapshot=artifact_snapshot,
            profile=spec.profile,
            cutoff=spec.cutoff,
            expected_dates=expected_dates,
            spans=spans,
            max_rows=max_rows,
        )
        index = _validate_index_context(
            root / "index_context",
            receipt=spec.index_receipt,
            receipt_file_authority=spec.index_materialization_receipt_file,
            profile=spec.profile,
            cutoff=spec.cutoff,
            expected_dates=expected_dates,
            max_rows=max_rows,
        )
        moneyflow = _validate_moneyflow_static_partitions(
            root / "factor_bundle",
            receipt=spec.factor_receipt,
            max_rows=max_rows,
        )
        moneyflow_derived = _validate_moneyflow_derived_formula_parity(
            root / "factor_bundle",
            max_rows=max_rows,
        )
        daily_source_parity = _validate_daily_source_bin_parity(
            candidate_root=root,
            daily_receipt=spec.daily_receipt,
            bin_root=root / "daily_bin" / "qlib",
            calendar=daily["calendar"],
            expected_instruments=set(daily["stock_codes"]),
            expected_stock_spans=daily_expected_spans,
            max_chunk_rows=max_rows,
            transition=daily_transition,
            expected_index_codes=set(spec.profile.index_codes),
        )
        daily_parity = _validate_h5_bin_parity(
            h5_path=root / "factor_bundle" / "daily_pv.h5",
            bin_root=root / "daily_bin" / "qlib",
            calendar=daily["calendar"],
            field_map=_DAILY_VALUE_MAP,
            max_rows=max_rows,
        )
        index_parity = _validate_h5_bin_parity(
            h5_path=root / "index_context" / "index_daily.h5",
            bin_root=root / "daily_bin" / "qlib",
            calendar=daily["calendar"],
            field_map=_INDEX_VALUE_MAP,
            constant_values=_INDEX_CONSTANT_VALUES,
            max_rows=max_rows,
        )
        overlay = _validate_minute_overlay(spec.minute_overlay_summary)
        minute_source_parity = _validate_minute_source_bin_parity(
            candidate_root=root,
            source=spec.minute_canonical_source,
            minute_receipt=spec.minute_receipt,
            bin_root=root / "minute_bin" / "qlib",
            calendar=minute["calendar"],
            expected_instruments=set(minute["stock_codes"]),
            expected_rows=int(overlay["source_rows"]),
            expected_stock_spans=minute_expected_spans,
            max_chunk_rows=max_rows,
            transition=minute_transition,
            expected_index_codes=set(),
        )
        format_smoke = _validate_candidate_format_smoke(
            root=root,
            cutoff=spec.cutoff,
            daily_calendar=daily["calendar"],
            minute_calendar=minute["calendar"],
            daily_codes=set(daily["stock_codes"]),
            minute_codes=set(minute["stock_codes"]),
            max_rows=max_rows,
        )
        consumer_smoke = _validate_external_consumer_smoke(
            spec.external_consumer_smoke,
            profile=spec.profile,
            cutoff=spec.cutoff,
            require_production=spec.require_production_consumer_smoke,
        )

        evidence = {
            "daily_bin": daily,
            "minute_bin": minute,
            "factor_h5_static": factor,
            "domestic_index_context": index,
            "moneyflow_static_parity": moneyflow,
            "moneyflow_derived_formula_parity": moneyflow_derived,
            "daily_source_bin_parity": daily_source_parity,
            "daily_h5_bin_parity": daily_parity,
            "index_h5_bin_parity": index_parity,
            "minute_overlay": overlay,
            "minute_source_bin_parity": minute_source_parity,
            "candidate_format_reader_smoke": format_smoke,
            "qe_hmm_consumer_smoke": consumer_smoke,
        }
        validations = tuple(
            ValidationResult(
                name=name,
                status=ValidationStatus.PASS,
                required=True,
                details_ref=None,
            )
            for name in evidence
        )
        component_rows = {
            Component.DAILY_BIN: (
                int(daily_source_parity["rows_checked"]),
                int(daily_source_parity["values_checked"]),
            ),
            Component.MINUTE_BIN: (
                int(minute_source_parity["rows_checked"]),
                int(minute_source_parity["values_checked"]),
            ),
            Component.FACTOR_H5_STATIC: (
                sum(int(value["rows"]) for value in factor["datasets"].values()),
                int(factor["static_rows"]),
            ),
            Component.DOMESTIC_INDEX_CONTEXT: (int(index["rows"]), int(index_parity["values_checked"])),
        }
        components = tuple(
            ComponentSignoff(
                component=component,
                action=ComponentAction(spec.actions[component.value]),
                partition_key=str(spec.transition_authority[component.value].action_entry["partition_key"]),
                status=ValidationStatus.PASS,
                manifest_root=digest_named_fields(
                    "dataset_release_component_validation_manifest_v1",
                    evidence[component.value],
                ),
                fingerprint_digest=spec.component_fingerprints[component.value],
                source_rows=component_rows[component][0],
                artifact_rows=component_rows[component][1],
            )
            for component in Component
        )
        payload = {
            "schema_version": CANDIDATE_VALIDATION_SCHEMA,
            "status": "PASS",
            "profile": spec.profile.profile,
            "cutoff": spec.cutoff.isoformat(),
            "pit_snapshot_digest": spec.pit_snapshot.spans_sha256,
            "validation_fingerprint": spec.validation_fingerprint,
            "evidence": evidence,
            "components": [item.as_dict() for item in components],
            "required_validations": [item.name for item in validations],
            "hmm": {
                "benchmark": HMM_BENCHMARK_CODE,
                "shared_index_contract": "validated",
                "existing_consumer_activation": "not_activated_not_switched",
            },
            "safety": {
                "database_writes": 0,
                "production_writes": 0,
                "production_deletes": 0,
                "production_pointer_changes": 0,
                "service_process_controls": 0,
            },
        }
        try:
            verify_candidate_artifact_snapshot(root, artifact_snapshot)
        except ComponentManifestProductionError as exc:
            raise CandidateValidationError("candidate artifact snapshot identity changed during validation") from exc
        return CandidateValidationReport(
            payload,
            components,
            validations,
            artifact_snapshot,
        )


def _validate_bin(
    root: Path,
    *,
    dataset: str,
    cutoff: date,
    expected_dates: Sequence[str],
    expected_index_codes: Sequence[str],
    expected_stock_spans: Mapping[str, Sequence[tuple[date, date]]],
) -> dict[str, Any]:
    root = _plain_root(root)
    frequency = "day" if dataset == "daily_bin" else "1min"
    _validate_plain_directory_entries(
        root,
        expected_directories={"calendars", "features", "instruments"},
        expected_files=set(),
        label=f"{dataset} Qlib",
    )
    calendar_root = _plain_root(root / "calendars")
    _validate_plain_directory_entries(
        calendar_root,
        expected_directories=set(),
        expected_files={f"{frequency}.txt"},
        label=f"{dataset} Qlib calendar",
    )
    instruments_root = _plain_root(root / "instruments")
    _validate_plain_directory_entries(
        instruments_root,
        expected_directories=set(),
        expected_files=({"all.txt", "index.txt"} if dataset == "daily_bin" else {"all.txt"}),
        label=f"{dataset} Qlib instruments",
    )
    calendar_path = root / "calendars" / f"{frequency}.txt"
    all_path = root / "instruments" / "all.txt"
    if not calendar_path.is_file() or not all_path.is_file():
        raise CandidateValidationError(f"{dataset} omits calendar/all.txt")
    _assert_plain(calendar_path)
    _assert_plain(all_path)
    calendar = tuple(value.strip() for value in calendar_path.read_text(encoding="utf-8").splitlines() if value.strip())
    if not calendar or tuple(sorted(set(calendar))) != calendar:
        raise CandidateValidationError(f"{dataset} calendar is empty/duplicate/unsorted")
    boundary_index = _build_calendar_boundary_index(calendar)
    observed_dates = boundary_index.unique_dates
    if observed_dates != tuple(expected_dates) or calendar[-1][:10] != cutoff.isoformat():
        raise CandidateValidationError(f"{dataset} calendar differs from frozen trading dates")
    if dataset == "minute_bin":
        invalid = {
            day: last - first + 1
            for day, first, last in zip(
                boundary_index.unique_dates,
                boundary_index.first_positions,
                boundary_index.last_positions,
                strict=True,
            )
            if last - first + 1 != 240
        }
        if invalid:
            raise CandidateValidationError(f"minute calendar is not 240 bars/day: {dict(list(invalid.items())[:20])}")
        invalid_sessions: list[dict[str, str]] = []
        for day, first, last in zip(
            boundary_index.unique_dates,
            boundary_index.first_positions,
            boundary_index.last_positions,
            strict=True,
        ):
            expected_session = tuple(
                value.isoformat(sep=" ", timespec="seconds")
                for value in canonical_session_times(date.fromisoformat(day))
            )
            observed_session = tuple(calendar[first : last + 1])
            if observed_session != expected_session:
                mismatch = next(
                    (
                        position
                        for position, (observed, expected) in enumerate(
                            zip(observed_session, expected_session, strict=True)
                        )
                        if observed != expected
                    ),
                    0,
                )
                invalid_sessions.append(
                    {
                        "trade_date": day,
                        "observed": observed_session[mismatch],
                        "expected": expected_session[mismatch],
                    }
                )
                if len(invalid_sessions) == 20:
                    break
        if invalid_sessions:
            raise CandidateValidationError(
                f"minute calendar differs from canonical Shanghai sessions: {invalid_sessions}"
            )
        cutoff_position = bisect.bisect_left(boundary_index.unique_dates, cutoff.isoformat())
        first = boundary_index.first_positions[cutoff_position]
        last = boundary_index.last_positions[cutoff_position]
        times = [value[11:] for value in calendar[first : last + 1]]
        if not times or times[0] != "09:31:00" or times[-1] != "15:00:00":
            raise CandidateValidationError("minute cutoff session boundary differs")
    stock_rows = _parse_instrument_rows(all_path, allow_multiple=True)
    stock_codes = set(stock_rows)
    if not stock_codes:
        raise CandidateValidationError(f"{dataset} stock universe is empty")
    normalized_expected_spans = {str(code).upper(): list(value) for code, value in expected_stock_spans.items()}
    if stock_rows != normalized_expected_spans:
        missing = sorted(set(normalized_expected_spans).difference(stock_rows))
        extra = sorted(set(stock_rows).difference(normalized_expected_spans))
        differing = sorted(
            code
            for code in set(stock_rows).intersection(normalized_expected_spans)
            if stock_rows[code] != normalized_expected_spans[code]
        )
        raise CandidateValidationError(
            f"{dataset} all.txt differs from frozen PIT spans: "
            f"missing={missing[:20]} extra={extra[:20]} differing={differing[:20]}"
        )
    index_codes: tuple[str, ...] = ()
    index_definitions: dict[str, Any] = {}
    if dataset == "daily_bin":
        index_path = root / "instruments" / "index.txt"
        if not index_path.is_file():
            raise CandidateValidationError("daily index.txt is missing")
        _assert_plain(index_path)
        index_rows = _parse_instrument_rows(index_path, allow_multiple=False)
        index_codes = tuple(index_rows)
        expected_code_set = set(expected_index_codes)
        index_definitions = {
            item.daily_code: item for item in DOMESTIC_INDEX_DEFINITIONS if item.daily_code in expected_code_set
        }
        expected_index_rows = {
            item.daily_code: [(item.required_from, cutoff)]
            for item in DOMESTIC_INDEX_DEFINITIONS
            if item.daily_code in expected_code_set
        }
        if (
            index_codes != tuple(expected_index_codes)
            or index_rows != expected_index_rows
            or stock_codes.intersection(index_codes)
        ):
            raise CandidateValidationError("daily stock/index instruments are not exact/isolated")

    feature_files = 0
    suffix = ".day.bin" if dataset == "daily_bin" else ".1min.bin"
    required_codes = stock_codes.union(index_codes)
    features_root = _plain_root(root / "features")
    _validate_plain_directory_entries(
        features_root,
        expected_directories={code.lower() for code in required_codes},
        expected_files=set(),
        label=f"{dataset} Qlib features",
    )
    stock_required = DAILY_FIELDS if dataset == "daily_bin" else MINUTE_FIELDS
    index_required = INDEX_QLIB_FIELDS
    for code in sorted(required_codes):
        feature_root = root / "features" / code.lower()
        if not feature_root.is_dir():
            raise CandidateValidationError(f"feature directory missing: {dataset}:{code}")
        feature_root = _plain_root(feature_root)
        expected_fields = index_required if code in index_codes else stock_required
        expected_names = {f"{field}{suffix}" for field in expected_fields}
        observed_names: set[str] = set()
        for child in feature_root.iterdir():
            _assert_plain(child)
            if not child.is_file():
                raise CandidateValidationError(f"feature namespace contains a non-file: {dataset}:{code}")
            observed_names.add(child.name)
        if observed_names != expected_names:
            observed_fields = {(name[: -len(suffix)] if name.endswith(suffix) else name) for name in observed_names}
            missing_fields = sorted(set(expected_fields).difference(observed_fields))
            extra_fields = sorted(observed_fields.difference(expected_fields))
            raise CandidateValidationError(
                f"feature field contract differs: {dataset}:{code}:missing={missing_fields} extra={extra_fields}"
            )
        paths = sorted(feature_root / name for name in expected_names)
        exact_coverage: tuple[int, int] | None = None
        if code in index_codes:
            definition = index_definitions[code]
            start_ordinal = bisect.bisect_left(
                boundary_index.unique_dates,
                definition.required_from.isoformat(),
            )
            if start_ordinal >= len(boundary_index.unique_dates):
                raise CandidateValidationError(f"index required-from has no trading rows: {code}")
            exact_coverage = (
                boundary_index.first_positions[start_ordinal],
                boundary_index.last_positions[-1],
            )
            required_positions = exact_coverage
        else:
            required_positions = _span_boundary_positions(
                boundary_index,
                spans=stock_rows[code],
                dataset=dataset,
            )
        for path in paths:
            _audit_float_bin(
                path,
                calendar_rows=len(calendar),
                required_positions=required_positions,
                require_finite=True,
                exact_coverage=exact_coverage,
            )
        feature_files += len(paths)
    return {
        "rows": len(calendar),
        "calendar": list(calendar),
        "calendar_sha256": sha256_file(calendar_path),
        "stock_instruments": len(stock_codes),
        "stock_codes": sorted(stock_codes),
        "stock_span_lines": sum(len(value) for value in stock_rows.values()),
        "index_codes": list(index_codes),
        "feature_files": feature_files,
        "cutoff": cutoff.isoformat(),
    }


def _validate_factor_bundle(
    root: Path,
    *,
    receipt: Mapping[str, Any],
    artifact_snapshot: CandidateArtifactSnapshot,
    profile: DatasetProfile,
    cutoff: date,
    expected_dates: Sequence[str],
    spans: Mapping[str, Sequence[tuple[date, date]]],
    max_rows: int,
) -> dict[str, Any]:
    root = _plain_root(root)
    raw_chunks = receipt.get("chunks")
    if not isinstance(raw_chunks, list) or not raw_chunks or any(not isinstance(item, Mapping) for item in raw_chunks):
        raise CandidateValidationError("factor chunk authority is missing")
    expected_partition_files: set[str] = set()
    snapshot_files = {item.relative_path: item for item in artifact_snapshot.files}
    chunk_order: list[tuple[str, str]] = []
    for item in raw_chunks:
        if set(item) != {
            "dataset",
            "partition_key",
            "relative_path",
            "sha256",
            "rows",
            "ordered_columns",
            "candidate_relative_path",
            "max_row_group_rows",
            "size_bytes",
        }:
            raise CandidateValidationError("factor chunk receipt fields drifted")
        dataset = str(item.get("dataset", ""))
        partition_key = str(item.get("partition_key", ""))
        if dataset not in {*FACTOR_H5_DATASETS, STATIC_DATASET} or _PARTITION_KEY.fullmatch(partition_key) is None:
            raise CandidateValidationError("factor chunk identity differs")
        relative_path = _canonical_relative_path(item.get("relative_path"), label="factor chunk source path")
        expected_relative = f"{dataset}/{partition_key}.parquet"
        if relative_path != expected_relative:
            raise CandidateValidationError("factor chunk source path differs")
        candidate_relative = _canonical_relative_path(
            item.get("candidate_relative_path"),
            label="factor chunk candidate path",
        )
        expected_candidate = f"factor_bundle/partitions/{expected_relative}"
        if candidate_relative != expected_candidate:
            raise CandidateValidationError("factor chunk candidate path differs")
        component_relative = candidate_relative.removeprefix("factor_bundle/")
        sha = str(item.get("sha256", ""))
        ensure_sha256(sha, field="factor_chunk_sha256")
        rows = item.get("rows")
        size_bytes = item.get("size_bytes")
        max_row_group_rows = item.get("max_row_group_rows")
        ordered_columns = tuple(str(value) for value in item.get("ordered_columns") or ())
        expected_columns = (
            tuple(profile.static_ordered_columns) if dataset == STATIC_DATASET else tuple(FACTOR_H5_SCHEMAS[dataset])
        )
        snapshot_file = snapshot_files.get(candidate_relative)
        if (
            isinstance(rows, bool)
            or not isinstance(rows, int)
            or rows <= 0
            or isinstance(size_bytes, bool)
            or not isinstance(size_bytes, int)
            or size_bytes <= 0
            or isinstance(max_row_group_rows, bool)
            or not isinstance(max_row_group_rows, int)
            or not 0 < max_row_group_rows <= max_rows
            or ordered_columns != expected_columns
            or snapshot_file is None
            or snapshot_file.size_bytes != size_bytes
            or snapshot_file.sha256 != sha
        ):
            raise CandidateValidationError("factor chunk receipt identity differs")
        chunk_order.append((dataset, partition_key))
        expected_partition_files.add(component_relative)
    if len(expected_partition_files) != len(raw_chunks) or chunk_order != sorted(chunk_order):
        raise CandidateValidationError("factor chunk candidate paths are duplicate/unsorted")
    _validate_plain_directory_entries(
        root,
        expected_directories={"partitions"},
        expected_files={
            "factor_checkpoint.json",
            "static_factors.parquet",
            *(f"{dataset}.h5" for dataset in FACTOR_H5_DATASETS),
        },
        label="factor bundle",
    )
    partitions_root = _plain_root(root / "partitions")
    observed_partition_files: set[str] = set()
    observed_partition_directories: set[str] = {"partitions"}
    for path in partitions_root.rglob("*"):
        _assert_plain(path)
        relative = path.relative_to(root).as_posix()
        if path.is_dir():
            observed_partition_directories.add(relative)
        elif path.is_file():
            observed_partition_files.add(relative)
        else:
            raise CandidateValidationError("factor partition namespace contains a non-regular entry")
    expected_partition_directories = {"partitions"}
    for relative in expected_partition_files:
        parent = PurePosixPath(relative).parent
        while parent.as_posix() != ".":
            expected_partition_directories.add(parent.as_posix())
            parent = parent.parent
    if (
        observed_partition_files != expected_partition_files
        or observed_partition_directories != expected_partition_directories
    ):
        raise CandidateValidationError("factor partition namespace differs")
    if receipt.get("status") != "PASS":
        raise CandidateValidationError("factor materialization receipt is not PASS")
    if _load_json(root / "factor_checkpoint.json") != dict(receipt):
        raise CandidateValidationError("factor checkpoint file differs from frozen authority")
    authority = receipt.get("schema_authority")
    if (
        not isinstance(authority, Mapping)
        or authority.get("factor_h5_schema_version") != FACTOR_H5_SCHEMA_VERSION
        or authority.get("factor_h5_schemas") != {key: list(value) for key, value in FACTOR_H5_SCHEMAS.items()}
        or authority.get("factor_h5_dtypes") != FACTOR_H5_DTYPES
        or authority.get("factor_h5_density_contracts") != FACTOR_H5_DENSITY_CONTRACTS
        or authority.get("static_schema_version") != STATIC_SCHEMA_VERSION
        or authority.get("static_schema_digest") != static_schema_digest()
        or authority.get("static_ordered_columns") != list(profile.static_ordered_columns)
        or authority.get("static_column_dtypes") != STATIC_COLUMN_DTYPES
    ):
        raise CandidateValidationError("factor/static schema authority receipt drifted")
    contract = receipt.get("moneyflow_unit_contract") or {}
    if contract.get("version") != MONEYFLOW_UNIT_CONTRACT_VERSION:
        raise CandidateValidationError("moneyflow unit contract drifted")
    outputs = receipt.get("outputs") or {}
    expected_outputs = {*FACTOR_H5_DATASETS, STATIC_DATASET}
    if set(outputs) != expected_outputs:
        raise CandidateValidationError("factor output receipt set differs")
    datasets: dict[str, Any] = {}
    observed_dates: set[str] = set()
    for dataset in FACTOR_H5_DATASETS:
        path = root / f"{dataset}.h5"
        expected = outputs[dataset]
        audit = _audit_h5(
            path,
            max_rows=max_rows,
            spans=spans,
            collect_dates=(dataset == "daily_pv"),
            expected_columns=FACTOR_H5_SCHEMAS[dataset],
            expected_dtypes=FACTOR_H5_DTYPES[dataset],
            exact_expected_keys=(_iter_expected_pit_keys(expected_dates, spans) if dataset == "daily_pv" else None),
        )
        _match_artifact_receipt(path, expected, audit)
        datasets[dataset] = audit
        if dataset == "daily_pv":
            observed_dates.update(audit["dates"])
    if tuple(sorted(observed_dates)) != tuple(expected_dates):
        raise CandidateValidationError("daily_pv dates differ from frozen trading calendar")
    if max(observed_dates) != cutoff.isoformat():
        raise CandidateValidationError("daily_pv cutoff differs")

    static_path = root / "static_factors.parquet"
    static = _audit_static(
        static_path,
        max_rows=max_rows,
        expected_columns=profile.static_ordered_columns,
        expected_dtypes=STATIC_COLUMN_DTYPES,
        spans=spans,
        exact_expected_keys=_iter_expected_pit_keys(expected_dates, spans),
    )
    _match_artifact_receipt(static_path, outputs[STATIC_DATASET], static)
    return {
        "datasets": datasets,
        "static_rows": static["rows"],
        "static_columns": static["columns"],
        "static_sha256": static["sha256"],
        "moneyflow_contract": MONEYFLOW_UNIT_CONTRACT_VERSION,
    }


def _audit_h5(
    path: Path,
    *,
    max_rows: int,
    spans: Mapping[str, Sequence[tuple[date, date]]],
    collect_dates: bool,
    expected_columns: Sequence[str],
    expected_dtypes: Mapping[str, str],
    exact_expected_keys: Iterable[tuple[pd.Timestamp, str]] | None,
) -> dict[str, Any]:
    rows = 0
    columns: tuple[str, ...] | None = None
    dtypes: dict[str, str] | None = None
    previous: tuple[pd.Timestamp, str] | None = None
    dates: set[str] = set()
    expected_iterator = iter(exact_expected_keys) if exact_expected_keys is not None else None
    expected_key = next(expected_iterator, None) if expected_iterator is not None else None
    for frame in iter_hdf_frames(path, chunksize=max_rows):
        actual_columns = tuple(str(value) for value in frame.columns)
        actual_dtypes = {str(column): str(dtype) for column, dtype in frame.dtypes.items()}
        if actual_columns != tuple(expected_columns):
            raise CandidateValidationError(f"H5 ordered columns drifted: {path.name}")
        if actual_dtypes != dict(expected_dtypes):
            raise CandidateValidationError(f"H5 authority dtypes drifted: {path.name}")
        if columns is None:
            columns, dtypes = actual_columns, actual_dtypes
        elif columns != actual_columns or dtypes != actual_dtypes:
            raise ArtifactSchemaDrift(f"H5 schema/dtype drift across chunks: {path}")
        if not frame.empty:
            first = _index_key(frame.index[0])
            last = _index_key(frame.index[-1])
            if previous is not None and first <= previous:
                raise ArtifactSchemaDrift(f"H5 chunks are not globally ordered: {path}")
            previous = last
            if expected_iterator is not None:
                for raw_key in frame.index:
                    actual_key = _index_key(raw_key)
                    if expected_key is None or actual_key != expected_key:
                        raise CandidateValidationError(f"{path.name} differs from exact PIT trading-day keys")
                    expected_key = next(expected_iterator, None)
            outside = _outside_pit(frame.index, spans)
            if outside:
                raise CandidateValidationError(f"H5 contains {outside} rows outside PIT: {path}")
            if collect_dates:
                dates.update(pd.to_datetime(frame.index.get_level_values("datetime")).strftime("%Y-%m-%d").tolist())
        rows += len(frame)
    if rows <= 0 or columns is None:
        raise CandidateValidationError(f"H5 contains no rows: {path}")
    if expected_iterator is not None and expected_key is not None:
        raise CandidateValidationError(f"{path.name} omits required PIT trading-day keys")
    return {
        "rows": rows,
        "columns": list(columns),
        "dtypes": dtypes or {},
        "sha256": sha256_file(path),
        "size_bytes": int(path.stat().st_size),
        "dates": sorted(dates),
        "max_rows_in_memory": max_rows,
    }


def _audit_static(
    path: Path,
    *,
    max_rows: int,
    expected_columns: Sequence[str],
    expected_dtypes: Mapping[str, str],
    spans: Mapping[str, Sequence[tuple[date, date]]],
    exact_expected_keys: Iterable[tuple[pd.Timestamp, str]],
) -> dict[str, Any]:
    rows = 0
    columns: tuple[str, ...] | None = None
    previous: tuple[pd.Timestamp, str] | None = None
    expected_iterator = iter(exact_expected_keys)
    expected_key = next(expected_iterator, None)
    for frame in iter_parquet_frames([path], max_rows=max_rows):
        actual_columns = tuple(str(value) for value in frame.columns)
        if columns is None:
            columns = actual_columns
        elif columns != actual_columns:
            raise ArtifactSchemaDrift("static schema drift across row groups")
        if actual_columns != tuple(expected_columns):
            raise CandidateValidationError("static 121-column/l2 contract drifted")
        actual_dtypes = {str(column): str(dtype) for column, dtype in frame.dtypes.items()}
        if actual_dtypes != dict(expected_dtypes):
            raise CandidateValidationError("static 121-column dtype contract drifted")
        if not frame.empty:
            first = _index_key(frame.index[0])
            last = _index_key(frame.index[-1])
            if previous is not None and first <= previous:
                raise ArtifactSchemaDrift("static row groups are not globally ordered")
            previous = last
            for raw_key in frame.index:
                actual_key = _index_key(raw_key)
                if expected_key is None or actual_key != expected_key:
                    raise CandidateValidationError("static differs from exact PIT trading-day keys")
                expected_key = next(expected_iterator, None)
            outside = _outside_pit(frame.index, spans)
            if outside:
                raise CandidateValidationError(f"static contains {outside} rows outside PIT")
        rows += len(frame)
    if rows <= 0:
        raise CandidateValidationError("static contains no rows")
    if expected_key is not None:
        raise CandidateValidationError("static omits required PIT trading-day keys")
    return {
        "rows": rows,
        "columns": len(columns or ()),
        "column_names": list(columns or ()),
        "dtypes": dict(expected_dtypes),
        "sha256": sha256_file(path),
        "size_bytes": int(path.stat().st_size),
    }


def _validate_index_context(
    root: Path,
    *,
    receipt: Mapping[str, Any],
    receipt_file_authority: Mapping[str, Any],
    profile: DatasetProfile,
    cutoff: date,
    expected_dates: Sequence[str],
    max_rows: int,
) -> dict[str, Any]:
    root = _plain_root(root)
    _validate_plain_directory_entries(
        root,
        expected_directories={"index_csv"},
        expected_files={
            "index_daily.h5",
            "index_context.parquet",
            "index_materialization_receipt.json",
        },
        label="index context",
    )
    index_csv_root = _plain_root(root / "index_csv")
    _validate_plain_directory_entries(
        index_csv_root,
        expected_directories=set(),
        expected_files={f"{code}.csv" for code in profile.index_codes},
        label="index context CSV",
    )
    h5_path = root / "index_daily.h5"
    parquet_path = root / "index_context.parquet"
    h5 = _audit_index_artifact(
        h5_path,
        frames=iter_hdf_frames(h5_path, chunksize=max_rows),
        label="index H5",
    )
    parquet = _audit_index_artifact(
        parquet_path,
        frames=iter_parquet_frames([parquet_path], max_rows=max_rows),
        label="index parquet",
    )
    if (
        h5.rows != parquet.rows
        or h5.codes != parquet.codes
        or h5.coverage != parquet.coverage
        or h5.content_digest != parquet.content_digest
    ):
        raise CandidateValidationError("index H5/parquet full value parity differs")
    if h5.codes != set(profile.index_codes):
        raise CandidateValidationError("index H5 code set differs from 12-code contract")
    expected_set = set(expected_dates)
    for definition in profile.indices:
        required = {value for value in expected_set if value >= definition.required_from.isoformat()}
        if h5.coverage[definition.daily_code] != required:
            raise CandidateValidationError(f"index calendar coverage differs: {definition.daily_code}")
    if HMM_BENCHMARK_CODE not in h5.codes or cutoff.isoformat() not in h5.coverage[HMM_BENCHMARK_CODE]:
        raise CandidateValidationError("HMM 000300.SH benchmark/cutoff missing")
    materialization = _load_json(root / "index_materialization_receipt.json")
    if materialization != dict(receipt_file_authority):
        raise CandidateValidationError("index materialization receipt file differs from frozen authority")
    _validate_index_materialization_receipts(
        materialization=materialization,
        stage_receipt=receipt,
        profile=profile,
        cutoff=cutoff,
        expected_dates=expected_set,
        h5=h5,
        parquet=parquet,
    )
    return {
        "rows": h5.rows,
        "codes": sorted(h5.codes),
        "benchmark": HMM_BENCHMARK_CODE,
        "cutoff": cutoff.isoformat(),
        "sha256": h5.file_sha256,
        "parquet_sha256": parquet.file_sha256,
        "h5_parquet_content_digest": h5.content_digest,
        "dtypes": dict(INDEX_H5_DTYPES),
        "schema_version": INDEX_SCHEMA_VERSION,
        "universe_version": INDEX_UNIVERSE_VERSION,
    }


def _audit_index_artifact(
    path: Path,
    *,
    frames: Iterable[pd.DataFrame],
    label: str,
) -> _IndexArtifactAudit:
    rows = 0
    codes: set[str] = set()
    coverage: dict[str, set[str]] = {item.daily_code: set() for item in DOMESTIC_INDEX_DEFINITIONS}
    previous: tuple[pd.Timestamp, str] | None = None
    content = hashlib.sha256()
    for frame in frames:
        actual_columns = tuple(str(value) for value in frame.columns)
        actual_dtypes = {str(column): str(dtype) for column, dtype in frame.dtypes.items()}
        if actual_columns != tuple(INDEX_H5_COLUMNS):
            raise CandidateValidationError(f"{label} ordered columns drifted")
        if actual_dtypes != dict(INDEX_H5_DTYPES):
            raise CandidateValidationError(f"{label} exact float32 dtypes drifted")
        numeric = frame.loc[:, list(INDEX_H5_COLUMNS)].apply(pd.to_numeric, errors="coerce")
        if not np.isfinite(numeric.to_numpy(dtype=float)).all():
            raise CandidateValidationError(f"{label} contains NULL/non-finite values")
        if frame.empty:
            continue
        first = _index_key(frame.index[0])
        last = _index_key(frame.index[-1])
        if previous is not None and first <= previous:
            raise CandidateValidationError(f"{label} is not globally strictly ordered")
        previous = last
        index_frame = frame.index.to_frame(index=False)
        timestamps = pd.to_datetime(index_frame["datetime"], errors="raise")
        if bool((timestamps != timestamps.dt.normalize()).any()):
            raise CandidateValidationError(f"{label} contains non-daily timestamps")
        instruments = index_frame["instrument"].astype(str)
        upper = instruments.str.upper()
        if not bool((instruments == upper).all()):
            raise CandidateValidationError(f"{label} instrument codes are non-canonical")
        index_frame["datetime"] = timestamps.dt.strftime("%Y-%m-%d")
        index_frame["instrument"] = upper
        for code, group in index_frame.groupby("instrument", sort=False):
            codes.add(code)
            if code not in coverage:
                raise CandidateValidationError(f"unexpected {label} code: {code}")
            coverage[code].update(group["datetime"].tolist())
        row_hashes = pd.util.hash_pandas_object(frame, index=True, categorize=False).to_numpy(dtype="<u8", copy=False)
        content.update(row_hashes.tobytes(order="C"))
        rows += len(frame)
    if rows <= 0:
        raise CandidateValidationError(f"{label} contains no rows")
    return _IndexArtifactAudit(
        rows=rows,
        codes=frozenset(codes),
        coverage={code: frozenset(values) for code, values in coverage.items()},
        content_digest=content.hexdigest(),
        file_sha256=sha256_file(path),
        size_bytes=int(path.stat().st_size),
    )


def _validate_index_materialization_receipts(
    *,
    materialization: Mapping[str, Any],
    stage_receipt: Mapping[str, Any],
    profile: DatasetProfile,
    cutoff: date,
    expected_dates: set[str],
    h5: _IndexArtifactAudit,
    parquet: _IndexArtifactAudit,
) -> None:
    details = materialization.get("details")
    if (
        materialization.get("schema_version") != "dataset_release_index_materialization_v1"
        or materialization.get("index_schema_version") != INDEX_SCHEMA_VERSION
        or materialization.get("index_universe_version") != INDEX_UNIVERSE_VERSION
        or materialization.get("contract_digest") != index_contract_digest()
        or materialization.get("contract") != index_contract_payload()
        or materialization.get("cutoff") != cutoff.isoformat()
        or int(materialization.get("rows", -1)) != h5.rows
        or materialization.get("database_writes") != 0
        or materialization.get("production_writes") != 0
        or not isinstance(details, Mapping)
        or set(details) != set(profile.index_codes)
    ):
        raise CandidateValidationError("index materialization receipt contract drifted")
    provider_fill_rows = 0
    for definition in profile.indices:
        value = details[definition.daily_code]
        expected_rows = sum(observed >= definition.required_from.isoformat() for observed in expected_dates)
        if (
            not isinstance(value, Mapping)
            or value.get("required_from") != definition.required_from.isoformat()
            or value.get("cutoff") != cutoff.isoformat()
            or int(value.get("expected_rows", -1)) != expected_rows
            or value.get("source_precedence") != "database_then_provider_missing_keys_conflict_fail_v1"
            or int(value.get("overlap_mismatch_cells", -1)) != 0
            or int(value.get("database_rows", -1)) + int(value.get("provider_fill_rows", -1)) != expected_rows
        ):
            raise CandidateValidationError(f"index materialization coverage receipt differs: {definition.daily_code}")
        provider_fill_rows += int(value["provider_fill_rows"])
    if int(materialization.get("provider_fill_rows", -1)) != provider_fill_rows:
        raise CandidateValidationError("index provider-fill receipt total differs")
    _match_index_file_receipt(
        materialization.get("h5"),
        audit=h5,
        schema="dataset_release_hdf_table_v1",
        label="index H5",
        extra={
            "columns": list(INDEX_H5_COLUMNS),
            "dtypes": dict(INDEX_H5_DTYPES),
            "format": "pandas_hdf_table_v1",
        },
    )
    _match_index_file_receipt(
        materialization.get("parquet"),
        audit=parquet,
        schema="dataset_release_parquet_aggregate_v1",
        label="index parquet",
        extra={},
    )
    if (
        stage_receipt.get("schema_version") != "dataset_release_index_materialization_v1"
        or stage_receipt.get("status") != "PASS"
        or int(stage_receipt.get("rows", -1)) != h5.rows
        or int(stage_receipt.get("provider_fill_rows", -1)) != provider_fill_rows
        or stage_receipt.get("contract_digest") != index_contract_digest()
        or stage_receipt.get("details") != details
        or stage_receipt.get("root_relative_path") != "index_context"
        or stage_receipt.get("h5_relative_path") != "index_context/index_daily.h5"
        or stage_receipt.get("parquet_relative_path") != "index_context/index_context.parquet"
        or stage_receipt.get("database_writes") != 0
        or stage_receipt.get("production_writes") != 0
    ):
        raise CandidateValidationError("index stage receipt contract drifted")


def _match_index_file_receipt(
    value: Any,
    *,
    audit: _IndexArtifactAudit,
    schema: str,
    label: str,
    extra: Mapping[str, Any],
) -> None:
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != schema
        or value.get("sha256") != audit.file_sha256
        or int(value.get("rows", -1)) != audit.rows
        or int(value.get("size_bytes", -1)) != audit.size_bytes
        or any(value.get(field) != expected for field, expected in extra.items())
    ):
        raise CandidateValidationError(f"{label} receipt/hash/schema differs")


def _validate_moneyflow_static_partitions(root: Path, *, receipt: Mapping[str, Any], max_rows: int) -> dict[str, Any]:
    chunks = receipt.get("chunks") or []
    moneyflow = {
        item["partition_key"]: root.parent / item["candidate_relative_path"]
        for item in chunks
        if item.get("dataset") == "moneyflow"
    }
    static = {
        item["partition_key"]: root.parent / item["candidate_relative_path"]
        for item in chunks
        if item.get("dataset") == STATIC_DATASET
    }
    if not moneyflow or set(moneyflow) != set(static):
        raise CandidateValidationError("moneyflow/static partition keys differ")
    checked = 0
    for key in sorted(moneyflow):
        checked += _stream_partition_parity(moneyflow[key], static[key], max_rows=max_rows)
    if checked <= 0:
        raise CandidateValidationError("moneyflow/static parity checked no rows")
    return {
        "rows": checked,
        "partitions": len(moneyflow),
        "columns": list(MONEYFLOW_FACTOR_COLUMNS),
        "unit_contract": MONEYFLOW_UNIT_CONTRACT_VERSION,
        "max_rows_in_memory": max_rows,
    }


def _stream_partition_parity(moneyflow_path: Path, static_path: Path, *, max_rows: int) -> int:
    static_iterator = iter(iter_parquet_frames([static_path], max_rows=max_rows))
    static_frame = next(static_iterator, None)
    checked = 0
    for moneyflow in iter_parquet_frames([moneyflow_path], max_rows=max_rows):
        missing = sorted(set(MONEYFLOW_FACTOR_COLUMNS).difference(moneyflow.columns))
        if missing:
            raise CandidateValidationError(f"moneyflow fields missing: {missing}")
        matches: list[pd.DataFrame] = []
        minimum = _index_key(moneyflow.index[0]) if not moneyflow.empty else None
        maximum = _index_key(moneyflow.index[-1]) if not moneyflow.empty else None
        while static_frame is not None and minimum is not None and maximum is not None:
            if static_frame.empty:
                static_frame = next(static_iterator, None)
                continue
            static_first = _index_key(static_frame.index[0])
            static_last = _index_key(static_frame.index[-1])
            if static_last < minimum:
                static_frame = next(static_iterator, None)
                continue
            if static_first > maximum:
                break
            common = moneyflow.index.intersection(static_frame.index)
            if len(common):
                matches.append(static_frame.loc[common, list(MONEYFLOW_FACTOR_COLUMNS)])
            if static_last <= maximum:
                static_frame = next(static_iterator, None)
            else:
                break
        combined = pd.concat(matches).sort_index() if matches else pd.DataFrame()
        if len(combined) != len(moneyflow) or not combined.index.equals(moneyflow.index):
            raise CandidateValidationError("moneyflow rows are missing from static factors")
        for column in MONEYFLOW_FACTOR_COLUMNS:
            left = pd.to_numeric(moneyflow[column], errors="coerce").to_numpy(dtype=float)
            right = pd.to_numeric(combined[column], errors="coerce").to_numpy(dtype=float)
            if not np.allclose(left, right, rtol=2e-6, atol=1e-3, equal_nan=True):
                raise CandidateValidationError(f"moneyflow/static parity differs: {column}")
        checked += len(moneyflow)
    return checked


def _validate_moneyflow_derived_formula_parity(
    root: Path,
    *,
    max_rows: int,
) -> dict[str, Any]:
    if not 0 < max_rows <= 1_000_000:
        raise CandidateValidationError("moneyflow derived validation bound is invalid")
    moneyflow_path = root / "moneyflow.h5"
    daily_path = root / "daily_pv.h5"
    static_path = root / "static_factors.parquet"
    daily_cursor = _OrderedArtifactCursor(
        iter_hdf_frames(daily_path, chunksize=max_rows),
        label="daily_pv H5",
    )
    static_cursor = _OrderedArtifactCursor(
        iter_parquet_frames([static_path], max_rows=max_rows),
        label="static factors",
    )
    rolling_state: dict[str, np.ndarray] = {}
    rows_checked = 0
    values_checked = 0
    maximum_delta = 0.0
    chunks = 0
    peak_chunk_rows = 0
    peak_rolling_state_values = 0
    for moneyflow in iter_hdf_frames(moneyflow_path, chunksize=max_rows):
        if moneyflow.empty:
            continue
        if len(moneyflow) > max_rows:
            raise CandidateValidationError("moneyflow H5 exceeded validation bound")
        daily = daily_cursor.take(
            moneyflow.index,
            columns=("amount", "volume", "factor"),
        )
        actual = static_cursor.take(
            moneyflow.index,
            columns=STATIC_MONEYFLOW_DERIVED_COLUMNS,
        )
        expected = _derive_moneyflow_chunk(
            moneyflow,
            daily,
            rolling_state=rolling_state,
        )
        for field in STATIC_MONEYFLOW_DERIVED_COLUMNS:
            expected_values = expected[field]
            actual_values = pd.to_numeric(actual[field], errors="coerce").to_numpy(dtype=np.float64)
            expected_nan = np.isnan(expected_values)
            actual_nan = np.isnan(actual_values)
            if not np.array_equal(expected_nan, actual_nan):
                raise CandidateValidationError(f"moneyflow derived NaN mask differs: {field}")
            if bool((~np.isfinite(actual_values) & ~actual_nan).any()):
                raise CandidateValidationError(f"moneyflow derived value is infinite: {field}")
            finite = ~expected_nan
            if finite.any():
                if not np.isclose(
                    actual_values[finite],
                    expected_values[finite],
                    rtol=2e-6,
                    atol=1e-3,
                ).all():
                    raise CandidateValidationError(f"moneyflow derived formula parity differs: {field}")
                maximum_delta = max(
                    maximum_delta,
                    float(np.max(np.abs(actual_values[finite] - expected_values[finite]))),
                )
            values_checked += len(expected_values)
        rows_checked += len(moneyflow)
        chunks += 1
        peak_chunk_rows = max(peak_chunk_rows, len(moneyflow))
        peak_rolling_state_values = max(
            peak_rolling_state_values,
            sum(len(value) * value.shape[1] for value in rolling_state.values()),
        )
        del moneyflow, daily, actual, expected
    if rows_checked <= 0 or values_checked != rows_checked * len(STATIC_MONEYFLOW_DERIVED_COLUMNS):
        raise CandidateValidationError("moneyflow derived formula parity checked incomplete values")
    return {
        "formula_authority": "backend.data_service.moneyflow_contract.derive_moneyflow_factors",
        "formula_fields": list(STATIC_MONEYFLOW_DERIVED_COLUMNS),
        "rows_checked": rows_checked,
        "values_checked": values_checked,
        "max_abs_delta": maximum_delta,
        "nan_contract": "exact_nan_mask_then_float32_tolerance_v1",
        "rolling_contract": "per_instrument_5_20_observations_cross_chunk_state_v1",
        "sample_policy": "full_moneyflow_rows_no_sampling",
        "unit_provenance": {
            "candidate_formula_input": "canonical_moneyflow_h5_share_cny",
            "source_hand_10k_to_share_cny_independently_reproven_here": False,
            "source_conversion_authority": ("artifact_ready_and_factor_producer_receipt_hash_chain"),
        },
        "memory_contract": {
            "mode": "three_bounded_streams_plus_19x4_per_instrument_state_v1",
            "configured_chunk_rows": max_rows,
            "peak_chunk_rows": peak_chunk_rows,
            "chunks": chunks,
            "rolling_state_instruments": len(rolling_state),
            "peak_rolling_state_values": peak_rolling_state_values,
            "whole_market_frames_retained": 0,
        },
    }


def _derive_moneyflow_chunk(
    moneyflow: pd.DataFrame,
    daily: pd.DataFrame,
    *,
    rolling_state: dict[str, np.ndarray],
) -> dict[str, np.ndarray]:
    required = {
        "mf_net_amt",
        "mf_net_vol",
        "mf_lg_buy_amt",
        "mf_lg_sell_amt",
        "mf_elg_buy_amt",
        "mf_elg_sell_amt",
        "mf_lg_buy_vol",
        "mf_lg_sell_vol",
        "mf_elg_buy_vol",
        "mf_elg_sell_vol",
    }
    missing = sorted(required.difference(moneyflow.columns))
    if missing:
        raise CandidateValidationError(f"moneyflow formula inputs are missing: {missing}")
    if not moneyflow.index.equals(daily.index):
        raise CandidateValidationError("moneyflow/daily formula keys differ")

    def values(frame: pd.DataFrame, field: str) -> np.ndarray:
        return pd.to_numeric(frame[field], errors="coerce").to_numpy(dtype=np.float64)

    amount = values(daily, "amount")
    raw_volume = values(daily, "volume") * values(daily, "factor")
    total_amt = values(moneyflow, "mf_net_amt")
    total_vol = values(moneyflow, "mf_net_vol")
    # Preserve the producer's float32 Series arithmetic before its explicit
    # float64 cast.  Promoting each operand first can change cancellation
    # residuals for large CNY values and would create validator-only results.
    main_amt = (
        moneyflow["mf_lg_buy_amt"]
        + moneyflow["mf_elg_buy_amt"]
        - moneyflow["mf_lg_sell_amt"]
        - moneyflow["mf_elg_sell_amt"]
    ).to_numpy(dtype=np.float64)
    main_vol = (
        moneyflow["mf_lg_buy_vol"]
        + moneyflow["mf_elg_buy_vol"]
        - moneyflow["mf_lg_sell_vol"]
        - moneyflow["mf_elg_sell_vol"]
    ).to_numpy(dtype=np.float64)
    elg_amt = (moneyflow["mf_elg_buy_amt"] - moneyflow["mf_elg_sell_amt"]).to_numpy(dtype=np.float64)
    elg_vol = (moneyflow["mf_elg_buy_vol"] - moneyflow["mf_elg_sell_vol"]).to_numpy(dtype=np.float64)
    finite_inputs = (
        amount,
        raw_volume,
        total_amt,
        total_vol,
        main_amt,
        main_vol,
        elg_amt,
        elg_vol,
    )
    if any(not np.isfinite(value).all() for value in finite_inputs):
        raise CandidateValidationError("moneyflow formula input contains NULL/non-finite values")
    expected = {
        "mf_total_net_amt": total_amt,
        "mf_total_net_vol": total_vol,
        "mf_total_net_amt_ratio": _safe_array_div(total_amt, amount),
        "mf_total_net_vol_ratio": _safe_array_div(total_vol, raw_volume),
        "mf_main_net_amt": main_amt,
        "mf_main_net_vol": main_vol,
        "mf_main_net_amt_ratio": _safe_array_div(main_amt, amount),
        "mf_main_net_vol_ratio": _safe_array_div(main_vol, raw_volume),
        "mf_elg_net_amt": elg_amt,
        "mf_elg_net_vol": elg_vol,
        "mf_elg_net_amt_ratio": _safe_array_div(elg_amt, amount),
        "mf_elg_net_vol_ratio": _safe_array_div(elg_vol, raw_volume),
        "mf_elg_share_in_main_amt": _safe_array_div(elg_amt, main_amt),
        "mf_elg_share_in_main_vol": _safe_array_div(elg_vol, main_vol),
    }
    for window in (5, 20):
        for prefix in ("total", "main", "elg"):
            expected[f"mf_{prefix}_net_amt_{window}d"] = np.full(len(moneyflow), np.nan, dtype=np.float64)
            expected[f"mf_{prefix}_net_amt_ratio_{window}d"] = np.full(len(moneyflow), np.nan, dtype=np.float64)
    grouped = moneyflow.groupby(level="instrument", sort=False).indices
    for raw_instrument, raw_positions in grouped.items():
        instrument = str(raw_instrument).upper()
        positions = np.asarray(raw_positions, dtype=np.int64)
        current = np.column_stack(
            (
                amount[positions],
                total_amt[positions],
                main_amt[positions],
                elg_amt[positions],
            )
        )
        history = rolling_state.get(instrument)
        combined = np.vstack((history, current)) if history is not None and len(history) else current
        for window in (5, 20):
            rolled = _complete_rolling_sums(combined, window)[-len(current) :]
            for offset, prefix in enumerate(("total", "main", "elg"), start=1):
                net = rolled[:, offset]
                expected[f"mf_{prefix}_net_amt_{window}d"][positions] = net
                expected[f"mf_{prefix}_net_amt_ratio_{window}d"][positions] = _safe_array_div(net, rolled[:, 0])
        rolling_state[instrument] = combined[-19:].copy()
    if set(expected) != set(STATIC_MONEYFLOW_DERIVED_COLUMNS):
        raise CandidateValidationError("moneyflow derived formula authority field set drifted")
    return expected


def _safe_array_div(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    output = np.full(len(numerator), np.nan, dtype=np.float64)
    valid = denominator != 0
    np.divide(numerator, denominator, out=output, where=valid)
    return output


def _complete_rolling_sums(values: np.ndarray, window: int) -> np.ndarray:
    output = np.full(values.shape, np.nan, dtype=np.float64)
    if len(values) < window:
        return output
    cumulative = np.vstack((np.zeros((1, values.shape[1]), dtype=np.float64), np.cumsum(values, axis=0)))
    output[window - 1 :] = cumulative[window:] - cumulative[:-window]
    return output


def _validate_h5_bin_parity(
    *,
    h5_path: Path,
    bin_root: Path,
    calendar: Sequence[str],
    field_map: Mapping[str, str],
    constant_values: Mapping[str, float] | None = None,
    max_rows: int,
) -> dict[str, Any]:
    constants = dict(constant_values or {})
    if set(field_map).intersection(constants):
        raise CandidateValidationError("Qlib parity field contracts overlap")
    positions = {value[:10]: ordinal for ordinal, value in enumerate(calendar)}
    checked = 0
    maximum_delta = 0.0
    for frame in iter_hdf_frames(h5_path, chunksize=max_rows):
        for instrument, group in frame.groupby(level="instrument", sort=False):
            dates = pd.to_datetime(group.index.get_level_values("datetime")).strftime("%Y-%m-%d")
            calendar_positions = np.asarray([positions.get(value, -1) for value in dates], dtype=np.int64)
            if (calendar_positions < 0).any():
                raise CandidateValidationError(f"H5 date missing from Qlib calendar: {instrument}")
            for bin_field, h5_field in field_map.items():
                path = bin_root / "features" / str(instrument).lower() / f"{bin_field}.day.bin"
                start, values = _read_float_bin(path)
                offsets = calendar_positions - start + 1
                if (offsets < 1).any() or (offsets >= len(values)).any():
                    raise CandidateValidationError(f"Qlib bin does not cover H5 keys: {instrument}:{bin_field}")
                actual = np.asarray(values[offsets], dtype=float)
                expected = pd.to_numeric(group[h5_field], errors="coerce").to_numpy(dtype=float)
                equal = np.isclose(actual, expected, rtol=2e-6, atol=1e-3, equal_nan=True)
                if not equal.all():
                    raise CandidateValidationError(f"Qlib bin/H5 value parity differs: {instrument}:{bin_field}")
                finite = np.isfinite(actual) & np.isfinite(expected)
                if finite.any():
                    maximum_delta = max(
                        maximum_delta,
                        float(np.max(np.abs(actual[finite] - expected[finite]))),
                    )
                checked += len(expected)
            for bin_field, expected_constant in constants.items():
                path = bin_root / "features" / str(instrument).lower() / f"{bin_field}.day.bin"
                start, values = _read_float_bin(path)
                offsets = calendar_positions - start + 1
                if (offsets < 1).any() or (offsets >= len(values)).any():
                    raise CandidateValidationError(f"Qlib bin does not cover H5 keys: {instrument}:{bin_field}")
                actual = np.asarray(values[offsets], dtype=float)
                expected = np.full(len(group), float(expected_constant), dtype=np.float64)
                equal = np.isclose(actual, expected, rtol=0.0, atol=1e-7)
                if not equal.all():
                    raise CandidateValidationError(
                        f"Qlib bin/H5 value parity differs: {instrument}:{bin_field}:constant={expected_constant}"
                    )
                maximum_delta = max(
                    maximum_delta,
                    float(np.max(np.abs(actual - expected))),
                )
                checked += len(expected)
    if checked <= 0:
        raise CandidateValidationError("Qlib bin/H5 parity checked no values")
    return {"values_checked": checked, "max_abs_delta": maximum_delta}


def _plain_v1_csv_namespace(root: Path, *, dataset_label: str) -> set[str]:
    observed: set[str] = set()
    for child in root.iterdir():
        _assert_plain(child)
        if not child.is_file() or child.name in observed:
            raise CandidateValidationError(f"{dataset_label} v1 canonical CSV namespace contains an unexpected entry")
        observed.add(child.name)
    return observed


def _validate_v1_csv_namespace(root: Path, *, expected_names: set[str], dataset_label: str) -> None:
    observed = _plain_v1_csv_namespace(root, dataset_label=dataset_label)
    if observed != expected_names:
        missing = sorted(expected_names.difference(observed))
        extra = sorted(observed.difference(expected_names))
        raise CandidateValidationError(
            f"{dataset_label} v1 canonical CSV namespace differs: missing={missing[:20]} extra={extra[:20]}"
        )


def _validate_index_csv_mirror(
    *,
    candidate_root: Path,
    daily_csv_root: Path,
    expected_index_codes: set[str],
    compare_daily_mirror: bool,
) -> None:
    index_root = _plain_root(candidate_root / "index_context" / "index_csv")
    expected_names = {f"{code}.csv" for code in expected_index_codes}
    _validate_v1_csv_namespace(
        index_root,
        expected_names=expected_names,
        dataset_label="index context",
    )
    if not compare_daily_mirror:
        return
    for name in sorted(expected_names):
        daily_path = daily_csv_root / name
        index_path = index_root / name
        _assert_plain(daily_path)
        _assert_plain(index_path)
        if daily_path.stat().st_size != index_path.stat().st_size or sha256_file(daily_path) != sha256_file(index_path):
            raise CandidateValidationError(f"daily/index-context canonical CSV mirror differs: {name}")


def _validate_daily_source_bin_parity(
    *,
    candidate_root: Path,
    daily_receipt: Mapping[str, Any],
    bin_root: Path,
    calendar: Sequence[str],
    expected_instruments: set[str],
    expected_stock_spans: Mapping[str, Sequence[tuple[date, date]]],
    max_chunk_rows: int,
    transition: _CanonicalTransitionContext | None,
    expected_index_codes: set[str],
) -> dict[str, Any]:
    if daily_receipt.get("status") != "PASS" or daily_receipt.get("dataset") not in {
        None,
        "daily_bin",
    }:
        raise CandidateValidationError("daily materialization receipt is not PASS")
    source = daily_receipt.get("sealed_canonical_rows")
    if not isinstance(source, Mapping):
        raise CandidateValidationError("daily materialization receipt omits sealed canonical rows")
    if is_lineage_v3(source):
        return _validate_lineage_source_bin_parity(
            dataset="daily_bin",
            candidate_root=candidate_root,
            source=source,
            bin_root=bin_root,
            calendar=calendar,
            expected_instruments=expected_instruments,
            expected_rows=None,
            expected_stock_spans=expected_stock_spans,
            max_chunk_rows=max_chunk_rows,
            transition=transition,
            expected_index_codes=expected_index_codes,
        )
    if source.get("schema_version") == SEALED_QLIB_CSV_COMPOSITE_SCHEMA:
        return _validate_composite_source_bin_parity(
            dataset="daily_bin",
            candidate_root=candidate_root,
            source=source,
            bin_root=bin_root,
            calendar=calendar,
            expected_instruments=expected_instruments,
            expected_rows=None,
            expected_stock_spans=expected_stock_spans,
            max_chunk_rows=max_chunk_rows,
            transition=transition,
            expected_index_codes=expected_index_codes,
        )
    if source.get("schema_version") != SEALED_QLIB_CSV_ROWS_SCHEMA:
        raise CandidateValidationError("daily canonical source schema drifted")
    if source.get("dataset") != "daily_bin":
        raise CandidateValidationError("daily canonical source dataset drifted")
    if tuple(source.get("ordered_fields") or ()) != ("date", "symbol", *DAILY_FIELDS):
        raise CandidateValidationError("daily canonical source fields drifted")
    if source.get("root_relative_path") != "daily_bin/csv":
        raise CandidateValidationError("daily canonical source root is not candidate-local")
    source_root = _plain_root(candidate_root / "daily_bin" / "csv")
    if candidate_root not in source_root.parents:
        raise CandidateValidationError("daily canonical source escapes candidate root")

    files = source.get("files")
    if not isinstance(files, list) or not files:
        raise CandidateValidationError("daily canonical source file list is empty")
    if any(not isinstance(item, Mapping) for item in files):
        raise CandidateValidationError("daily canonical source file receipt is invalid")
    instruments = [str(item.get("instrument", "")).upper() for item in files]
    if instruments != sorted(expected_instruments) or len(instruments) != len(set(instruments)):
        raise CandidateValidationError("daily canonical source instruments differ from frozen PIT/Qlib instruments")
    stock_names = {f"{instrument}.csv" for instrument in instruments}
    index_names = {f"{code}.csv" for code in expected_index_codes}
    current_names = stock_names.union(index_names)
    observed_names = _plain_v1_csv_namespace(source_root, dataset_label="daily")
    if observed_names not in (stock_names, current_names):
        missing = sorted(current_names.difference(observed_names))
        extra = sorted(observed_names.difference(current_names))
        raise CandidateValidationError(
            "daily v1 canonical CSV namespace differs from both supported exact "
            f"layouts: missing={missing[:20]} extra={extra[:20]}"
        )
    compare_daily_mirror = observed_names == current_names
    _validate_index_csv_mirror(
        candidate_root=candidate_root,
        daily_csv_root=source_root,
        expected_index_codes=expected_index_codes,
        compare_daily_mirror=compare_daily_mirror,
    )
    if not 0 < max_chunk_rows <= 1_000_000:
        raise CandidateValidationError("daily validation chunk bound is invalid")
    calendar_text = tuple(str(value)[:10] for value in calendar)
    if tuple(sorted(set(calendar_text))) != calendar_text:
        raise CandidateValidationError("daily parity calendar is duplicate/unsorted")
    calendar_days = tuple(date.fromisoformat(value) for value in calendar_text)
    calendar_index = pd.Index(calendar_text, dtype="object")
    rows_checked = 0
    values_checked = 0
    maximum_delta = 0.0
    stock_days_checked = 0
    parity_chunks = 0
    peak_chunk_rows = 0
    for item, instrument in zip(files, instruments, strict=True):
        expected_relative = f"{instrument}.csv"
        if item.get("relative_path") != expected_relative:
            raise CandidateValidationError(f"daily canonical source path differs: {instrument}")
        logical_path = source_root / expected_relative
        _assert_plain(logical_path)
        path = logical_path.resolve(strict=True)
        if path.parent != source_root or not path.is_file():
            raise CandidateValidationError(f"daily canonical source file is unavailable: {instrument}")
        if int(item.get("size_bytes", -1)) != path.stat().st_size:
            raise CandidateValidationError(f"daily canonical source size differs: {instrument}")
        feature_values = {
            field: _read_float_bin(bin_root / "features" / instrument.lower() / f"{field}.day.bin")
            for field in DAILY_FIELDS
        }
        stream_metrics: dict[str, int] = {"chunks": 0, "peak_chunk_rows": 0}
        file_rows, file_values, file_delta, day_counts = _stream_daily_csv_parity(
            path,
            instrument=instrument,
            expected_sha256=str(item.get("sha256", "")),
            expected_rows=int(item.get("rows", -1)),
            calendar_index=calendar_index,
            feature_values=feature_values,
            max_chunk_rows=max_chunk_rows,
            metrics=stream_metrics,
        )
        parity_chunks += stream_metrics["chunks"]
        peak_chunk_rows = max(peak_chunk_rows, stream_metrics["peak_chunk_rows"])
        expected_days: list[str] = []
        for start, end in expected_stock_spans[instrument]:
            left = bisect.bisect_left(calendar_days, start)
            right = bisect.bisect_right(calendar_days, end)
            expected_days.extend(calendar_text[left:right])
        if tuple(day_counts) != tuple(expected_days) or any(count != 1 for count in day_counts.values()):
            raise CandidateValidationError(f"daily canonical PIT stock-day contract differs: {instrument}")
        expected_start = f"{expected_days[0]} 00:00:00" if expected_days else None
        expected_end = f"{expected_days[-1]} 00:00:00" if expected_days else None
        if item.get("start") != expected_start or item.get("end") != expected_end:
            raise CandidateValidationError(f"daily canonical source range receipt differs: {instrument}")
        stock_days_checked += len(day_counts)
        rows_checked += file_rows
        values_checked += file_values
        maximum_delta = max(maximum_delta, file_delta)
        del feature_values
    if rows_checked != int(source.get("rows", -1)):
        raise CandidateValidationError(
            f"daily canonical/Qlib row counts differ: canonical={rows_checked} receipt={source.get('rows')}"
        )
    if rows_checked <= 0 or values_checked != rows_checked * len(DAILY_FIELDS):
        raise CandidateValidationError("daily canonical parity checked incomplete values")
    return {
        "source_schema_version": SEALED_QLIB_CSV_ROWS_SCHEMA,
        "source_manifest_digest": digest_named_fields("dataset_release_daily_canonical_source_manifest_v1", source),
        "instruments": len(instruments),
        "rows_checked": rows_checked,
        "values_checked": values_checked,
        "fields": list(DAILY_FIELDS),
        "max_abs_delta": maximum_delta,
        "sample_policy": "full_required_rows_no_sampling",
        "stock_day_contract": "pit_stock_day_one_row_v1",
        "stock_days_checked": stock_days_checked,
        "memory_contract": {
            "mode": "vectorized_csv_chunk_vs_12_memmaps_v1",
            "configured_chunk_rows": max_chunk_rows,
            "peak_chunk_rows": peak_chunk_rows,
            "chunks": parity_chunks,
            "whole_market_frames_retained": 0,
        },
        "expected_day_contract": "one_calendar_scan_then_per_code_bisect_v1",
    }


def _stream_daily_csv_parity(
    path: Path,
    *,
    instrument: str,
    expected_sha256: str,
    expected_rows: int,
    calendar_index: pd.Index,
    feature_values: Mapping[str, tuple[int, np.memmap]],
    max_chunk_rows: int,
    metrics: dict[str, Any] | None = None,
    boundaries: dict[str, str] | None = None,
) -> tuple[int, int, float, dict[str, int]]:
    ensure_sha256(expected_sha256, field=f"daily_source_sha256:{instrument}")
    digest = hashlib.sha256()
    rows = 0
    values_checked = 0
    maximum_delta = 0.0
    previous_date: str | None = None
    day_counts: dict[str, int] = {}
    expected_header = ["date", "symbol", *DAILY_FIELDS]
    try:
        with path.open("rb") as raw:
            hashing = _DigestingRawReader(
                raw,
                digest=digest,
                max_line_bytes=1024 * 1024,
                dataset="daily",
            )
            with io.BufferedReader(hashing, buffer_size=1024 * 1024) as buffered:
                chunks = pd.read_csv(
                    buffered,
                    chunksize=max_chunk_rows,
                    dtype={"date": "string", "symbol": "string"},
                )
                for chunk in chunks:
                    if metrics is not None:
                        metrics["chunks"] = metrics.get("chunks", 0) + 1
                        metrics["peak_chunk_rows"] = max(metrics.get("peak_chunk_rows", 0), len(chunk))
                    if list(chunk.columns) != expected_header or len(chunk) > max_chunk_rows:
                        raise CandidateValidationError(
                            f"daily canonical CSV field/chunk contract drifted: {instrument}"
                        )
                    symbols = chunk["symbol"].str.strip().str.upper()
                    if not bool((symbols == instrument).all()):
                        raise CandidateValidationError(f"daily canonical CSV symbol differs: {instrument}")
                    timestamps = pd.to_datetime(chunk["date"], errors="raise")
                    date_text = timestamps.dt.strftime("%Y-%m-%d")
                    if not bool((date_text == chunk["date"]).all()):
                        raise CandidateValidationError(f"daily canonical date is non-canonical: {instrument}")
                    date_values = date_text.to_numpy(dtype=str)
                    if (previous_date is not None and date_values[0] <= previous_date) or (
                        len(date_values) > 1 and bool((date_values[1:] <= date_values[:-1]).any())
                    ):
                        raise CandidateValidationError(f"daily canonical dates are duplicate/unsorted: {instrument}")
                    previous_date = str(date_values[-1])
                    if boundaries is not None:
                        boundaries.setdefault("start", str(date_values[0]))
                        boundaries["end"] = str(date_values[-1])
                    positions = calendar_index.get_indexer(date_values)
                    if bool((positions < 0).any()):
                        raise CandidateValidationError(
                            f"daily canonical date is absent from Qlib calendar: {instrument}"
                        )
                    expected = (
                        chunk.loc[:, list(DAILY_FIELDS)].apply(pd.to_numeric, errors="raise").to_numpy(dtype=np.float64)
                    )
                    actual_columns: list[np.ndarray] = []
                    for field in DAILY_FIELDS:
                        start, values_bin = feature_values[field]
                        offsets = positions - start + 1
                        if bool((offsets < 1).any()) or bool((offsets >= len(values_bin)).any()):
                            raise CandidateValidationError(
                                f"daily Qlib bin does not cover canonical keys: {instrument}:{field}"
                            )
                        actual_columns.append(np.asarray(values_bin[offsets], dtype=np.float64))
                    actual = np.column_stack(actual_columns)
                    if not np.isfinite(expected).all() or not np.isfinite(actual).all():
                        raise CandidateValidationError(f"daily canonical/bin value is non-finite: {instrument}")
                    equal = np.isclose(actual, expected, rtol=2e-6, atol=1e-3)
                    if not bool(equal.all()):
                        location = np.argwhere(~equal)[0]
                        raise CandidateValidationError(
                            "daily canonical/bin value parity differs: "
                            f"{instrument}:{date_values[int(location[0])]}:"
                            f"{DAILY_FIELDS[int(location[1])]}"
                        )
                    maximum_delta = max(maximum_delta, float(np.max(np.abs(actual - expected))))
                    for day in date_values:
                        value = str(day)
                        day_counts[value] = day_counts.get(value, 0) + 1
                    rows += len(chunk)
                    values_checked += int(expected.size)
    except (UnicodeDecodeError, pd.errors.ParserError, ValueError) as exc:
        if isinstance(exc, CandidateValidationError):
            raise
        raise CandidateValidationError(f"daily canonical CSV parse failed: {instrument}") from exc
    if rows != expected_rows:
        raise CandidateValidationError(f"daily canonical source row receipt differs: {instrument}")
    if digest.hexdigest() != expected_sha256:
        raise CandidateValidationError(f"daily canonical source digest differs: {instrument}")
    return rows, values_checked, maximum_delta, day_counts


def _validate_minute_source_bin_parity(
    *,
    candidate_root: Path,
    source: Mapping[str, Any],
    minute_receipt: Mapping[str, Any],
    bin_root: Path,
    calendar: Sequence[str],
    expected_instruments: set[str],
    expected_rows: int,
    expected_stock_spans: Mapping[str, Sequence[tuple[date, date]]],
    max_chunk_rows: int,
    transition: _CanonicalTransitionContext | None,
    expected_index_codes: set[str],
) -> dict[str, Any]:
    if minute_receipt.get("status") != "PASS" or minute_receipt.get("dataset") not in {
        None,
        "minute_bin",
    }:
        raise CandidateValidationError("minute materialization receipt is not PASS")
    receipt_source = minute_receipt.get("sealed_canonical_rows")
    if receipt_source is not None and receipt_source != source:
        raise CandidateValidationError("minute canonical source differs from materialization receipt")
    if is_lineage_v3(source):
        return _validate_lineage_source_bin_parity(
            dataset="minute_bin",
            candidate_root=candidate_root,
            source=source,
            bin_root=bin_root,
            calendar=calendar,
            expected_instruments=expected_instruments,
            expected_rows=expected_rows,
            expected_stock_spans=expected_stock_spans,
            max_chunk_rows=max_chunk_rows,
            transition=transition,
            expected_index_codes=expected_index_codes,
        )
    if source.get("schema_version") == SEALED_QLIB_CSV_COMPOSITE_SCHEMA:
        return _validate_composite_source_bin_parity(
            dataset="minute_bin",
            candidate_root=candidate_root,
            source=source,
            bin_root=bin_root,
            calendar=calendar,
            expected_instruments=expected_instruments,
            expected_rows=expected_rows,
            expected_stock_spans=expected_stock_spans,
            max_chunk_rows=max_chunk_rows,
            transition=transition,
            expected_index_codes=expected_index_codes,
        )
    if source.get("schema_version") != SEALED_QLIB_CSV_ROWS_SCHEMA:
        raise CandidateValidationError("minute canonical source schema drifted")
    if source.get("dataset") != "minute_bin":
        raise CandidateValidationError("minute canonical source dataset drifted")
    if tuple(source.get("ordered_fields") or ()) != ("date", "symbol", *MINUTE_FIELDS):
        raise CandidateValidationError("minute canonical source fields drifted")
    if source.get("root_relative_path") != "minute_bin/csv":
        raise CandidateValidationError("minute canonical source root is not candidate-local")
    source_root = _plain_root(candidate_root / "minute_bin" / "csv")
    if candidate_root not in source_root.parents:
        raise CandidateValidationError("minute canonical source escapes candidate root")

    files = source.get("files")
    if not isinstance(files, list) or not files:
        raise CandidateValidationError("minute canonical source file list is empty")
    if any(not isinstance(item, Mapping) for item in files):
        raise CandidateValidationError("minute canonical source file receipt is invalid")
    instruments = [str(item.get("instrument", "")).upper() for item in files]
    if instruments != sorted(expected_instruments) or len(instruments) != len(set(instruments)):
        raise CandidateValidationError("minute canonical source instruments differ from frozen PIT/Qlib instruments")
    _validate_v1_csv_namespace(
        source_root,
        expected_names={f"{instrument}.csv" for instrument in instruments},
        dataset_label="minute",
    )
    if not 0 < max_chunk_rows <= 1_000_000:
        raise CandidateValidationError("minute validation chunk bound is invalid")
    calendar_index = pd.Index(calendar, dtype="object")
    calendar_day_text = tuple(dict.fromkeys(value[:10] for value in calendar))
    calendar_days = tuple(date.fromisoformat(value) for value in calendar_day_text)
    rows_checked = 0
    values_checked = 0
    maximum_delta = 0.0
    stock_days_checked = 0
    parity_chunks = 0
    peak_chunk_rows = 0
    for item, instrument in zip(files, instruments, strict=True):
        expected_relative = f"{instrument}.csv"
        if item.get("relative_path") != expected_relative:
            raise CandidateValidationError(f"minute canonical source path differs: {instrument}")
        logical_path = source_root / expected_relative
        _assert_plain(logical_path)
        path = logical_path.resolve(strict=True)
        if path.parent != source_root or not path.is_file():
            raise CandidateValidationError(f"minute canonical source file is unavailable: {instrument}")
        if int(item.get("size_bytes", -1)) != path.stat().st_size:
            raise CandidateValidationError(f"minute canonical source size differs: {instrument}")
        feature_values = {
            field: _read_float_bin(bin_root / "features" / instrument.lower() / f"{field}.1min.bin")
            for field in MINUTE_FIELDS
        }
        stream_metrics: dict[str, Any] = {"chunks": 0, "peak_chunk_rows": 0}
        boundaries: dict[str, str] = {}
        file_rows, file_values, file_delta, day_counts = _stream_minute_csv_parity(
            path,
            instrument=instrument,
            expected_sha256=str(item.get("sha256", "")),
            expected_rows=int(item.get("rows", -1)),
            calendar_index=calendar_index,
            feature_values=feature_values,
            max_chunk_rows=max_chunk_rows,
            metrics=stream_metrics,
            boundaries=boundaries,
        )
        if item.get("start") != boundaries.get("start") or item.get("end") != boundaries.get("end"):
            raise CandidateValidationError(f"minute canonical source range receipt differs: {instrument}")
        parity_chunks += stream_metrics["chunks"]
        peak_chunk_rows = max(peak_chunk_rows, stream_metrics["peak_chunk_rows"])
        expected_days: list[str] = []
        for start, end in expected_stock_spans[instrument]:
            left = bisect.bisect_left(calendar_days, start)
            right = bisect.bisect_right(calendar_days, end)
            expected_days.extend(calendar_day_text[left:right])
        if tuple(day_counts) != tuple(expected_days) or any(count != 240 for count in day_counts.values()):
            raise CandidateValidationError(f"minute canonical PIT stock-day 240-bar contract differs: {instrument}")
        stock_days_checked += len(day_counts)
        rows_checked += file_rows
        values_checked += file_values
        maximum_delta = max(maximum_delta, file_delta)
        del feature_values
    if rows_checked != int(source.get("rows", -1)) or rows_checked != expected_rows:
        raise CandidateValidationError(
            "minute canonical/Qlib/overlay row counts differ: "
            f"canonical={rows_checked} receipt={source.get('rows')} overlay={expected_rows}"
        )
    if rows_checked <= 0 or values_checked != rows_checked * len(MINUTE_FIELDS):
        raise CandidateValidationError("minute canonical parity checked incomplete values")
    return {
        "source_schema_version": SEALED_QLIB_CSV_ROWS_SCHEMA,
        "source_manifest_digest": digest_named_fields("dataset_release_minute_canonical_source_manifest_v1", source),
        "instruments": len(instruments),
        "rows_checked": rows_checked,
        "values_checked": values_checked,
        "fields": list(MINUTE_FIELDS),
        "max_abs_delta": maximum_delta,
        "sample_policy": "full_required_rows_no_sampling",
        "stock_day_contract": "pit_stock_day_240_bars_suspend_synthesized_v1",
        "stock_days_checked": stock_days_checked,
        "memory_contract": {
            "mode": "vectorized_csv_chunk_vs_12_memmaps_v1",
            "configured_chunk_rows": max_chunk_rows,
            "peak_chunk_rows": peak_chunk_rows,
            "chunks": parity_chunks,
            "whole_market_frames_retained": 0,
        },
        "expected_day_contract": "one_calendar_scan_then_per_code_bisect_v1",
    }


def _validate_lineage_source_bin_parity(
    *,
    dataset: str,
    candidate_root: Path,
    source: Mapping[str, Any],
    bin_root: Path,
    calendar: Sequence[str],
    expected_instruments: set[str],
    expected_rows: int | None,
    expected_stock_spans: Mapping[str, Sequence[tuple[date, date]]],
    max_chunk_rows: int,
    transition: _CanonicalTransitionContext | None,
    expected_index_codes: set[str],
) -> dict[str, Any]:
    component_root = _plain_root(candidate_root / dataset)
    try:
        validated = validate_lineage_descriptor(component_root, source)
        summaries = tuple(lineage_instrument_summaries(component_root, validated))
        segments = tuple(lineage_active_segments(component_root, validated))
    except DatasetReleaseError as exc:
        raise CandidateValidationError(f"{dataset} canonical lineage descriptor is invalid") from exc
    fields = DAILY_FIELDS if dataset == "daily_bin" else MINUTE_FIELDS
    if validated.get("dataset") != dataset or tuple(validated.get("ordered_fields") or ()) != (
        "date",
        "symbol",
        *fields,
    ):
        raise CandidateValidationError(f"{dataset} canonical lineage fields drifted")
    summary_codes = [str(item["instrument"]) for item in summaries]
    if summary_codes != sorted(expected_instruments):
        raise CandidateValidationError(f"{dataset} canonical lineage instruments differ from frozen PIT")
    prevalidated = _validate_lineage_v3_contract(
        dataset=dataset,
        candidate_root=candidate_root,
        source=validated,
        active_segments=segments,
        expected_instruments=expected_instruments,
        expected_index_codes=expected_index_codes,
        transition=transition,
    )
    segment_fields = _COMPOSITE_SEGMENT_FIELDS
    synthetic = {
        "schema_version": SEALED_QLIB_CSV_COMPOSITE_SCHEMA,
        "dataset": dataset,
        "ordered_fields": list(validated["ordered_fields"]),
        "rows": int(validated["rows"]),
        "files": [dict(item) for item in summaries],
        "segments": [{key: item[key] for key in segment_fields} for item in segments],
        "merge_contract": _COMPOSITE_APPEND_CONTRACT,
    }
    result = _validate_composite_source_bin_parity(
        dataset=dataset,
        candidate_root=candidate_root,
        source=synthetic,
        bin_root=bin_root,
        calendar=calendar,
        expected_instruments=expected_instruments,
        expected_rows=expected_rows,
        expected_stock_spans=expected_stock_spans,
        max_chunk_rows=max_chunk_rows,
        transition=None,
        expected_index_codes=expected_index_codes,
        prevalidated_lineage=prevalidated,
    )
    return {
        **result,
        "source_schema_version": CANONICAL_LINEAGE_SCHEMA,
        "source_manifest_digest": validated["lineage_root"],
        "lineage_contract": prevalidated,
    }


def _validate_lineage_v3_contract(
    *,
    dataset: str,
    candidate_root: Path,
    source: Mapping[str, Any],
    active_segments: Sequence[Mapping[str, Any]],
    expected_instruments: set[str],
    expected_index_codes: set[str],
    transition: _CanonicalTransitionContext | None,
) -> dict[str, Any]:
    component_root = _plain_root(candidate_root / dataset)
    try:
        history = lineage_inventory_history(component_root, source)
        reachable_objects = set(lineage_object_paths(component_root, source))
    except DatasetReleaseError as exc:
        raise CandidateValidationError(f"{dataset} canonical lineage history is invalid") from exc
    lineage_root = _plain_root(component_root / "csv_lineage")
    observed_objects: set[str] = set()
    for path in sorted(lineage_root.rglob("*")):
        _assert_plain(path)
        if path.is_dir():
            continue
        if not path.is_file() or path.suffix.casefold() != ".json":
            raise CandidateValidationError(f"{dataset} lineage namespace contains an unexpected entry")
        observed_objects.add(path.relative_to(component_root).as_posix())
    if observed_objects != reachable_objects:
        missing = sorted(reachable_objects.difference(observed_objects))
        extra = sorted(observed_objects.difference(reachable_objects))
        raise CandidateValidationError(
            f"{dataset} lineage object namespace differs: missing={missing[:1]} extra={extra[:1]}"
        )

    path_sha: dict[str, str] = {}
    for item in (*active_segments, *history):
        root_relative = str(item["root_relative_path"]).replace("\\", "/")
        relative = str(item["relative_path"]).replace("\\", "/")
        path_key = f"{root_relative}/{relative}".casefold()
        sha = ensure_sha256(str(item["sha256"]), field=f"lineage_csv_sha256:{path_key}")
        prior = path_sha.setdefault(path_key, sha)
        if prior != sha:
            raise CandidateValidationError(f"{dataset} lineage binds one path to different bytes")
    physical_csv, manifests = _canonical_csv_tree(
        candidate_root=candidate_root,
        dataset=dataset,
    )
    allowed = set(path_sha)
    if dataset == "daily_bin":
        possible_index = {f"daily_bin/csv/{code.casefold()}.csv" for code in expected_index_codes}
        observed_index = set(physical_csv).intersection(possible_index)
        if observed_index not in (set(), possible_index):
            raise CandidateValidationError("daily lineage contains a partial index CSV mirror")
        allowed.update(observed_index)
    if set(physical_csv) != allowed:
        missing = sorted(allowed.difference(physical_csv))
        extra = sorted(set(physical_csv).difference(allowed))
        raise CandidateValidationError(
            f"{dataset} lineage physical CSV namespace differs: missing={missing[:1]} extra={extra[:1]}"
        )
    for path_key, expected_sha in path_sha.items():
        path = physical_csv[path_key]
        if sha256_file(path) != expected_sha:
            raise CandidateValidationError(f"{dataset} lineage CSV bytes differ: {path_key}")

    manifest_count = 0
    historical_v2 = 0
    for namespace, _key, path in manifests:
        if path.stat().st_size > 128 * 1024:
            historical_v2 += 1
            continue
        value = _load_json(path)
        if value.get("schema_version") == "dataset_release_csv_segment_manifest_v2":
            historical_v2 += 1
            continue
        if value.get("schema_version") != CANONICAL_LINEAGE_NAMESPACE_MANIFEST_SCHEMA:
            raise CandidateValidationError(f"{dataset} lineage namespace manifest schema differs")
        body = dict(value)
        declared_identity = body.pop("manifest_identity", None)
        if (
            declared_identity != digest_named_fields(CANONICAL_LINEAGE_NAMESPACE_MANIFEST_SCHEMA, body)
            or value.get("dataset") != dataset
            or value.get("capability") != CANONICAL_LINEAGE_CAPABILITY
        ):
            raise CandidateValidationError(f"{dataset} lineage namespace manifest identity differs")
        namespace_root = str(value.get("namespace_root_relative_path", ""))
        expected_namespace = f"{dataset}/{namespace}/{path.parent.name}"
        if namespace_root != expected_namespace:
            raise CandidateValidationError(f"{dataset} lineage namespace root differs")
        event_ref = value.get("event_ref")
        if not isinstance(event_ref, Mapping):
            raise CandidateValidationError(f"{dataset} lineage namespace event reference is invalid")
        try:
            inventory = lineage_event_inventory(
                component_root,
                event_ref,
                namespace_root_relative_path=namespace_root,
            )
        except DatasetReleaseError as exc:
            raise CandidateValidationError(f"{dataset} lineage namespace inventory is invalid") from exc
        inventory_paths = {f"{item['root_relative_path']}/{item['relative_path']}".casefold() for item in inventory}
        physical_namespace = {key for key in physical_csv if key.startswith(namespace_root + "/")}
        if inventory_paths != physical_namespace:
            raise CandidateValidationError(f"{dataset} lineage namespace inventory differs from files")
        manifest_count += 1

    transition_evidence = _validate_lineage_v3_transition(
        dataset=dataset,
        candidate_root=candidate_root,
        source=source,
        transition=transition,
        reachable_objects=reachable_objects,
    )
    return {
        "merge_contract": "persistent_code_head_merkle_v3",
        "active_segments": len(active_segments),
        "inventory_rows": len(history),
        "physical_csv_files": len(physical_csv),
        "namespace_manifests": manifest_count,
        "historical_v2_manifests_skipped": historical_v2,
        "reachable_lineage_objects": len(reachable_objects),
        "expected_instruments": len(expected_instruments),
        "transition_authority": transition_evidence,
    }


def _validate_lineage_v3_transition(
    *,
    dataset: str,
    candidate_root: Path,
    source: Mapping[str, Any],
    transition: _CanonicalTransitionContext | None,
    reachable_objects: set[str],
) -> str:
    component_root = candidate_root / dataset
    if transition is None:
        latest = _load_json(component_root / str(source["latest_event_ref"]["relative_path"]))
        if latest.get("event_kind") != "GENESIS":
            raise CandidateValidationError(f"{dataset} FULL lineage is not a genesis event")
        return "full_genesis_v3"
    lineage_plan = transition.frozen_reuse.get("canonical_lineage")
    if not isinstance(lineage_plan, Mapping) or lineage_plan.get("capability") != CANONICAL_LINEAGE_CAPABILITY:
        raise CandidateValidationError(f"{dataset} frozen lineage transition authority is missing")
    event_key = str(lineage_plan.get("event_key", ""))
    latest_path = str(source["latest_event_ref"]["relative_path"])
    if f"/events/{event_key}/" not in f"/{latest_path}":
        raise CandidateValidationError(f"{dataset} lineage event path differs from the frozen plan")
    latest = _load_json(component_root / latest_path)
    baseline_source = transition.baseline_source
    if is_lineage_v3(baseline_source):
        baseline = validate_lineage_descriptor(
            transition.baseline_root,
            baseline_source,
        )
        if (
            lineage_plan.get("baseline_schema_version") != CANONICAL_LINEAGE_SCHEMA
            or lineage_plan.get("baseline_lineage_root") != baseline["lineage_root"]
            or latest.get("parent_lineage_root") != baseline["lineage_root"]
            or latest.get("parent_event_ref") != baseline["latest_event_ref"]
        ):
            raise CandidateValidationError(f"{dataset} lineage parent transition proof differs")
        baseline_objects = set(lineage_object_paths(transition.baseline_root, baseline))
    else:
        anchor = source.get("legacy_anchor")
        anchor_key = lineage_plan.get("anchor_key")
        if not isinstance(anchor, Mapping) or not isinstance(anchor_key, str):
            raise CandidateValidationError(f"{dataset} legacy lineage anchor proof is missing")
        anchor_path = str(anchor.get("event_ref", {}).get("relative_path", ""))
        if f"/anchors/{anchor_key}/" not in f"/{anchor_path}":
            raise CandidateValidationError(f"{dataset} legacy lineage anchor path differs")
        anchor_event = _load_json(component_root / anchor_path)
        receipt_path = transition.baseline_root / "materialization_receipt.json"
        expected_binding = {
            "source_release_id": transition.frozen_reuse["source_release_id"],
            "source_release_digest": transition.frozen_reuse["source_release_digest"],
            "component_file_identity": transition.baseline_evidence.file_identity,
            "component_manifest_root": transition.baseline_evidence.component_manifest_root,
            "materialization_receipt_sha256": sha256_file(receipt_path),
            "materialization_receipt_size_bytes": receipt_path.stat().st_size,
        }
        if (
            anchor_event.get("legacy_binding") != expected_binding
            or latest.get("parent_lineage_root") != anchor.get("lineage_root")
            or latest.get("parent_event_ref") != anchor.get("event_ref")
        ):
            raise CandidateValidationError(f"{dataset} legacy lineage anchor binding differs")
        baseline_objects = set()
    planned_new = {
        str(value).replace("\\", "/")
        for value in transition.authorized_create_paths
        if str(value).replace("\\", "/").startswith("csv_lineage/")
    }
    if reachable_objects.difference(baseline_objects) != planned_new:
        raise CandidateValidationError(f"{dataset} lineage object delta differs from frozen create targets")
    return "frozen_parent_and_event_paths_v3"


def _validate_composite_source_bin_parity(
    *,
    dataset: str,
    candidate_root: Path,
    source: Mapping[str, Any],
    bin_root: Path,
    calendar: Sequence[str],
    expected_instruments: set[str],
    expected_rows: int | None,
    expected_stock_spans: Mapping[str, Sequence[tuple[date, date]]],
    max_chunk_rows: int,
    transition: _CanonicalTransitionContext | None = None,
    expected_index_codes: set[str] | None = None,
    prevalidated_lineage: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not 0 < max_chunk_rows <= 1_000_000:
        raise CandidateValidationError("composite validation chunk bound is invalid")
    if dataset == "daily_bin":
        fields = DAILY_FIELDS
        suffix = "day"
        expected_rows_per_day = 1
        stream_parity = _stream_daily_csv_parity
        dataset_label = "daily"
    elif dataset == "minute_bin":
        fields = MINUTE_FIELDS
        suffix = "1min"
        expected_rows_per_day = 240
        stream_parity = _stream_minute_csv_parity
        dataset_label = "minute"
    else:
        raise CandidateValidationError("composite canonical dataset is invalid")
    merge_contract = source.get("merge_contract")
    if (
        source.get("dataset") != dataset
        or tuple(source.get("ordered_fields") or ()) != ("date", "symbol", *fields)
        or merge_contract not in {_COMPOSITE_APPEND_CONTRACT, _COMPOSITE_OVERRIDE_CONTRACT}
    ):
        raise CandidateValidationError(f"{dataset_label} composite canonical contract drifted")
    files = source.get("files")
    segments = source.get("segments")
    if not isinstance(files, list) or not isinstance(segments, list) or not segments:
        raise CandidateValidationError(f"{dataset_label} composite canonical evidence is empty")
    expected_source_fields = {
        "schema_version",
        "dataset",
        "ordered_fields",
        "rows",
        "files",
        "segments",
        "merge_contract",
    }
    if merge_contract == _COMPOSITE_OVERRIDE_CONTRACT:
        expected_source_fields.add("overrides")
    if set(source) != expected_source_fields:
        raise CandidateValidationError(f"{dataset_label} composite canonical fields drifted")
    if any(not isinstance(item, Mapping) or set(item) != _COMPOSITE_SUMMARY_FIELDS for item in files):
        raise CandidateValidationError(f"{dataset_label} composite summary is invalid")
    lineage = (
        dict(prevalidated_lineage)
        if prevalidated_lineage is not None
        else _validate_composite_lineage_contract(
            dataset=dataset,
            candidate_root=candidate_root,
            source=source,
            expected_instruments=expected_instruments,
            max_chunk_rows=max_chunk_rows,
            transition=transition,
            expected_index_codes=expected_index_codes or set(),
            calendar=calendar,
        )
    )
    summary_codes = [str(item.get("instrument", "")).upper() for item in files]
    summaries = {code: item for code, item in zip(summary_codes, files, strict=True)}
    if (
        summary_codes != sorted(expected_instruments)
        or set(summaries) != expected_instruments
        or len(summaries) != len(files)
    ):
        raise CandidateValidationError(f"{dataset_label} composite instruments differ from frozen PIT")
    grouped: dict[str, list[Mapping[str, Any]]] = {code: [] for code in expected_instruments}
    observed_segment_keys: list[tuple[str, str, str, str, str]] = []
    for item in segments:
        if not isinstance(item, Mapping):
            raise CandidateValidationError(f"{dataset_label} composite segment is invalid")
        code = str(item.get("instrument", "")).upper()
        if code not in grouped:
            raise CandidateValidationError(f"{dataset_label} composite segment code differs")
        start = str(item.get("start", ""))
        end = str(item.get("end", ""))
        root_relative = str(item.get("root_relative_path", "")).replace("\\", "/")
        relative = str(item.get("relative_path", "")).replace("\\", "/")
        key = (code, start, end, root_relative, relative)
        if not start or end < start or (observed_segment_keys and key <= observed_segment_keys[-1]):
            raise CandidateValidationError(f"{dataset_label} composite segment manifest is not strictly ordered")
        observed_segment_keys.append(key)
        grouped[code].append(item)
    if any(not grouped[code] for code in expected_instruments):
        raise CandidateValidationError(f"{dataset_label} composite segment coverage omits instruments")
    calendar_index = pd.Index(calendar, dtype="object")
    calendar_day_text = tuple(dict.fromkeys(value[:10] for value in calendar))
    calendar_days = tuple(date.fromisoformat(value) for value in calendar_day_text)
    rows_checked = 0
    values_checked = 0
    maximum_delta = 0.0
    stock_days_checked = 0
    parity_chunks = 0
    peak_chunk_rows = 0
    for instrument in sorted(expected_instruments):
        feature_values = {
            field: _read_float_bin(bin_root / "features" / instrument.lower() / f"{field}.{suffix}.bin")
            for field in fields
        }
        day_counts: dict[str, int] = {}
        prior_end: str | None = None
        instrument_rows = 0
        ordered = grouped[instrument]
        first_segment_start: str | None = None
        for item in ordered:
            start = str(item.get("start", ""))
            end = str(item.get("end", ""))
            if not start or end < start or (prior_end is not None and start <= prior_end):
                raise CandidateValidationError(f"{dataset_label} composite segment ordering differs: {instrument}")
            first_segment_start = first_segment_start or start
            prior_end = end
            root_relative = str(item.get("root_relative_path", "")).replace("\\", "/")
            relative = str(item.get("relative_path", "")).replace("\\", "/")
            root, path, _path_key = _resolve_canonical_csv_path(
                candidate_root=candidate_root,
                root_relative=root_relative,
                relative=relative,
                dataset_label=dataset_label,
            )
            if path.name.casefold() != f"{instrument.casefold()}.csv":
                raise CandidateValidationError(f"{dataset_label} composite segment escapes candidate: {instrument}")
            if int(item.get("size_bytes", -1)) != path.stat().st_size:
                raise CandidateValidationError(f"{dataset_label} composite segment size differs: {instrument}")
            metrics: dict[str, Any] = {"chunks": 0, "peak_chunk_rows": 0}
            boundaries: dict[str, str] = {}
            segment_rows, segment_values, segment_delta, segment_days = stream_parity(
                path,
                instrument=instrument,
                expected_sha256=str(item.get("sha256", "")),
                expected_rows=int(item.get("rows", -1)),
                calendar_index=calendar_index,
                feature_values=feature_values,
                max_chunk_rows=max_chunk_rows,
                metrics=metrics,
                boundaries=boundaries,
            )
            observed_start = boundaries.get("start")
            observed_end = boundaries.get("end")
            if dataset == "daily_bin":
                observed_start = f"{observed_start} 00:00:00" if observed_start else None
                observed_end = f"{observed_end} 00:00:00" if observed_end else None
            if start != observed_start or end != observed_end:
                raise CandidateValidationError(f"{dataset_label} composite segment range differs: {instrument}")
            for day, count in segment_days.items():
                if day in day_counts:
                    raise CandidateValidationError(f"{dataset_label} composite segments overlap: {instrument}:{day}")
                day_counts[day] = count
            instrument_rows += segment_rows
            rows_checked += segment_rows
            values_checked += segment_values
            maximum_delta = max(maximum_delta, segment_delta)
            parity_chunks += metrics["chunks"]
            peak_chunk_rows = max(peak_chunk_rows, metrics["peak_chunk_rows"])
        summary = summaries[instrument]
        if (
            instrument_rows != int(summary.get("rows", -1))
            or len(ordered) != int(summary.get("segments", -1))
            or summary.get("start") != first_segment_start
            or summary.get("end") != prior_end
        ):
            raise CandidateValidationError(f"{dataset_label} composite summary differs: {instrument}")
        expected_days: list[str] = []
        for start, end in expected_stock_spans[instrument]:
            left = bisect.bisect_left(calendar_days, start)
            right = bisect.bisect_right(calendar_days, end)
            expected_days.extend(calendar_day_text[left:right])
        if tuple(day_counts) != tuple(expected_days) or any(
            count != expected_rows_per_day for count in day_counts.values()
        ):
            raise CandidateValidationError(f"{dataset_label} composite PIT stock-day coverage differs: {instrument}")
        stock_days_checked += len(day_counts)
        del feature_values
    if rows_checked != int(source.get("rows", -1)) or (expected_rows is not None and rows_checked != expected_rows):
        raise CandidateValidationError(f"{dataset_label} composite row count differs")
    if rows_checked <= 0 or values_checked != rows_checked * len(fields):
        raise CandidateValidationError(f"{dataset_label} composite parity is incomplete")
    return {
        "source_schema_version": SEALED_QLIB_CSV_COMPOSITE_SCHEMA,
        "source_manifest_digest": digest_named_fields(
            f"dataset_release_{dataset_label}_canonical_composite_manifest_v1",
            source,
        ),
        "instruments": len(expected_instruments),
        "rows_checked": rows_checked,
        "values_checked": values_checked,
        "fields": list(fields),
        "max_abs_delta": maximum_delta,
        "sample_policy": "full_required_rows_no_sampling",
        "stock_day_contract": (
            "pit_stock_day_one_row_v1" if dataset == "daily_bin" else "pit_stock_day_240_bars_suspend_synthesized_v1"
        ),
        "stock_days_checked": stock_days_checked,
        "memory_contract": {
            "mode": "vectorized_composite_segment_csv_chunk_vs_12_memmaps_v1",
            "configured_chunk_rows": max_chunk_rows,
            "chunks": parity_chunks,
            "peak_chunk_rows": peak_chunk_rows,
            "whole_market_frames_retained": 0,
        },
        "expected_day_contract": "one_calendar_scan_then_per_code_bisect_v1",
        "lineage_contract": lineage,
    }


def _validate_composite_lineage_contract(
    *,
    dataset: str,
    candidate_root: Path,
    source: Mapping[str, Any],
    expected_instruments: set[str],
    max_chunk_rows: int,
    transition: _CanonicalTransitionContext | None = None,
    expected_index_codes: set[str] | None = None,
    calendar: Sequence[str] = (),
) -> dict[str, Any]:
    """Validate the immutable CSV lineage before consuming active authority.

    The top-level ``segments`` list is the sole active authority.  Override
    entries are an append-only event log: each replacement must either remain
    active or be superseded exactly once by a later event.  Retired bytes are
    hashed and structurally scanned, while active bytes are hashed once by the
    value-level Qlib parity pass that follows this check.
    """

    dataset_label = "daily" if dataset == "daily_bin" else "minute"
    if expected_index_codes is None:
        expected_index_codes = {definition.daily_code for definition in DOMESTIC_INDEX_DEFINITIONS}
    calendar_keys = frozenset(str(value) for value in calendar)
    merge_contract = str(source.get("merge_contract", ""))
    raw_segments = source.get("segments")
    if not isinstance(raw_segments, list) or not raw_segments:
        raise CandidateValidationError(f"{dataset_label} composite active segments are missing")
    if any(not isinstance(item, Mapping) or set(item) != _COMPOSITE_SEGMENT_FIELDS for item in raw_segments):
        raise CandidateValidationError(f"{dataset_label} composite active segment fields drifted")
    active_records = [
        _validate_composite_segment_record(
            item,
            dataset=dataset,
            candidate_root=candidate_root,
            role="active",
        )
        for item in raw_segments
    ]
    active_ids = {item["identity"] for item in active_records}
    active_paths = {item["path_key"] for item in active_records}
    if len(active_ids) != len(active_records) or len(active_paths) != len(active_records):
        raise CandidateValidationError(f"{dataset_label} composite active path authority is duplicate")
    observed_order = [
        (
            str(item["instrument"]),
            str(item["start"]),
            str(item["end"]),
            str(item["path_key"]),
        )
        for item in active_records
    ]
    if observed_order != sorted(observed_order):
        raise CandidateValidationError(f"{dataset_label} composite segment manifest is not strictly ordered")
    prior_by_code: dict[str, str] = {}
    for item in active_records:
        code = str(item["instrument"])
        prior_end = prior_by_code.get(code)
        if prior_end is not None and str(item["start"]) <= prior_end:
            raise CandidateValidationError(f"{dataset_label} composite active segments overlap: {code}")
        prior_by_code[code] = str(item["end"])

    raw_overrides = source.get("overrides")
    if merge_contract == _COMPOSITE_APPEND_CONTRACT:
        if raw_overrides is not None and raw_overrides != () and raw_overrides != []:
            raise CandidateValidationError(f"{dataset_label} strict-append composite carries override lineage")
        overrides: list[Mapping[str, Any]] = []
    else:
        if not isinstance(raw_overrides, list) or not raw_overrides:
            raise CandidateValidationError(f"{dataset_label} active-override composite omits override lineage")
        if any(not isinstance(item, Mapping) for item in raw_overrides):
            raise CandidateValidationError(f"{dataset_label} override lineage entry is invalid")
        overrides = raw_overrides

    replacement_records: list[dict[str, Any]] = []
    replacement_ids: dict[tuple[str, str], int] = {}
    superseded_owner: dict[tuple[str, str], int] = {}
    superseded_records: dict[tuple[str, str], dict[str, Any]] = {}
    path_sha: dict[str, str] = {}
    for record in active_records:
        _bind_lineage_path(path_sha, record, dataset_label=dataset_label)

    for ordinal, override in enumerate(overrides):
        expected_keys = _COMPOSITE_SEGMENT_FIELDS.union({"superseded_segments", "invalidation_scopes"})
        if set(override) != expected_keys:
            raise CandidateValidationError(f"{dataset_label} override lineage fields drifted")
        replacement = _validate_composite_segment_record(
            override,
            dataset=dataset,
            candidate_root=candidate_root,
            role="override replacement",
        )
        if replacement["root_kind"] != "override":
            raise CandidateValidationError(f"{dataset_label} override replacement is outside csv_overrides")
        if replacement["identity"] in replacement_ids:
            raise CandidateValidationError(f"{dataset_label} override replacement is duplicate")
        replacement_ids[replacement["identity"]] = ordinal
        replacement_records.append(replacement)
        _bind_lineage_path(path_sha, replacement, dataset_label=dataset_label)

        scopes = override.get("invalidation_scopes")
        _validate_override_invalidation_scopes(
            scopes,
            instrument=replacement["instrument"],
            expected_instruments=expected_instruments,
            dataset_label=dataset_label,
        )
        raw_superseded = override.get("superseded_segments")
        if not isinstance(raw_superseded, list) or not raw_superseded:
            raise CandidateValidationError(f"{dataset_label} override has no superseded authority")
        local_ids: set[tuple[str, str]] = set()
        for item in raw_superseded:
            record = _validate_superseded_segment_record(
                item,
                dataset=dataset,
                candidate_root=candidate_root,
                instrument=replacement["instrument"],
                max_chunk_rows=max_chunk_rows,
                allowed_calendar=calendar_keys or None,
            )
            identity = record["identity"]
            if identity == replacement["identity"] or identity in local_ids:
                raise CandidateValidationError(f"{dataset_label} override superseded authority is duplicate/self")
            if identity in active_ids:
                raise CandidateValidationError(f"{dataset_label} superseded authority remains active")
            if identity in superseded_owner:
                raise CandidateValidationError(f"{dataset_label} authority is superseded more than once")
            local_ids.add(identity)
            superseded_owner[identity] = ordinal
            superseded_records.setdefault(identity, record)
            _bind_lineage_path(path_sha, record, dataset_label=dataset_label)

    for identity, owner in superseded_owner.items():
        replacement_ordinal = replacement_ids.get(identity)
        if replacement_ordinal is not None and replacement_ordinal >= owner:
            raise CandidateValidationError(f"{dataset_label} override lineage is cyclic/out of order")
    active_by_id = {item["identity"]: item for item in active_records}
    for replacement in replacement_records:
        identity = replacement["identity"]
        if identity not in active_ids and identity not in superseded_owner:
            raise CandidateValidationError(f"{dataset_label} override replacement is orphaned from active lineage")
        authority = active_by_id.get(identity) or superseded_records.get(identity)
        if authority is None or any(
            replacement.get(field) != authority.get(field)
            for field in (
                "instrument",
                "root_relative_path",
                "relative_path",
                "rows",
                "sha256",
                "size_bytes",
                "start",
                "end",
            )
        ):
            raise CandidateValidationError(f"{dataset_label} override replacement metadata differs from authority")
    latest_by_code: dict[str, tuple[str, str]] = {}
    for replacement in replacement_records:
        latest_by_code[replacement["instrument"]] = replacement["identity"]
    for code, identity in latest_by_code.items():
        if identity not in active_ids:
            raise CandidateValidationError(f"{dataset_label} latest override replacement is not active: {code}")

    physical_csv, manifests = _canonical_csv_tree(
        candidate_root=candidate_root,
        dataset=dataset,
    )
    manifest_evidence = _validate_canonical_namespace_manifests(
        candidate_root=candidate_root,
        dataset=dataset,
        manifests=manifests,
        physical_csv=physical_csv,
        top_path_sha=path_sha,
        lineage_records={
            str(item["path_key"]): item
            for item in (
                *active_records,
                *replacement_records,
                *superseded_records.values(),
            )
        },
        expected_instruments=expected_instruments,
        expected_index_codes=expected_index_codes,
        max_chunk_rows=max_chunk_rows,
        allowed_calendar=calendar_keys or None,
        cutoff=(str(calendar[-1])[:10] if calendar else None),
        transition=transition,
        current_source=source,
    )
    if transition is not None:
        _validate_durable_lineage_transition(
            dataset=dataset,
            candidate_root=candidate_root,
            source=source,
            transition=transition,
            active_records=active_records,
            replacement_records=replacement_records,
            superseded_records=superseded_records,
            overrides=overrides,
            manifest_evidence=manifest_evidence,
            expected_instruments=expected_instruments,
            expected_index_codes=expected_index_codes,
            cutoff=date.fromisoformat(str(calendar[-1])[:10]),
        )
    referenced_paths = set(path_sha)
    declared_paths = set(manifest_evidence["inventory_path_sha"])
    for path_key, path in physical_csv.items():
        if path_key in referenced_paths or path_key in declared_paths:
            continue
        relative = path.relative_to(candidate_root).as_posix()
        raise CandidateValidationError(f"{dataset_label} composite contains ghost CSV: {relative}")
    missing = sorted(referenced_paths.union(declared_paths).difference(physical_csv))
    if missing:
        raise CandidateValidationError(f"{dataset_label} composite lineage references missing CSV: {missing[0]}")
    return {
        "merge_contract": merge_contract,
        "active_segments": len(active_records),
        "override_events": len(overrides),
        "superseded_segments": len(superseded_records),
        "physical_csv_files": len(physical_csv),
        "header_only_tombstones": manifest_evidence["header_only_tombstones"],
        "inactive_index_csv": manifest_evidence["inactive_index_csv"],
        "inherited_inventory_files": manifest_evidence["inherited_inventory_files"],
        "historical_manifest_replays_skipped": manifest_evidence["historical_manifest_replays_skipped"],
        "namespace_manifests": len(manifests),
        "authority": "top_level_segments_only_unique_code_datetime_v1",
        "retired_bytes": "full_sha256_and_bounded_structural_scan_v1",
        "ghost_policy": "active_or_override_lineage_or_header_only_delta_v1",
        "transition_authority": ("durable_baseline_replay_v1" if transition is not None else "not_applicable_full"),
    }


def _validate_durable_lineage_transition(
    *,
    dataset: str,
    candidate_root: Path,
    source: Mapping[str, Any],
    transition: _CanonicalTransitionContext,
    active_records: Sequence[Mapping[str, Any]],
    replacement_records: Sequence[Mapping[str, Any]],
    superseded_records: Mapping[tuple[str, str], Mapping[str, Any]],
    overrides: Sequence[Mapping[str, Any]],
    manifest_evidence: Mapping[str, Any],
    expected_instruments: set[str],
    expected_index_codes: set[str],
    cutoff: date,
) -> None:
    dataset_label = "daily" if dataset == "daily_bin" else "minute"
    if transition.component.value != dataset:
        raise CandidateValidationError(f"{dataset_label} transition component identity differs")
    candidate_component = _plain_root(candidate_root / dataset)
    baseline_file_evidence = {
        item.relative_path: item
        for partition in transition.baseline_evidence.artifact_partitions
        for item in partition.files
        if _is_canonical_lineage_file(item.relative_path)
    }
    # The generic component transition has already compared the trusted
    # one-pass candidate snapshot against every baseline file SHA.  Canonical
    # baseline paths are forbidden from the replace set, so rereading their
    # content here would only duplicate that sealed comparison.
    if transition.verified_candidate_files is None:
        for relative, item in baseline_file_evidence.items():
            logical = candidate_component / Path(relative)
            try:
                _assert_plain(logical)
            except CandidateValidationError as exc:
                raise CandidateValidationError(
                    f"{dataset_label} immutable baseline lineage path is missing/unsafe: {relative}"
                ) from exc
            if (
                not logical.is_file()
                or logical.stat().st_size != item.size_bytes
                or sha256_file(logical) != item.sha256
            ):
                raise CandidateValidationError(f"{dataset_label} immutable baseline lineage bytes differ: {relative}")
    expected_lineage_paths = set(baseline_file_evidence).union(
        value for value in transition.authorized_create_paths if _is_canonical_lineage_file(value)
    )
    if set(_canonical_lineage_file_tree(candidate_component)) != expected_lineage_paths:
        raise CandidateValidationError(
            f"{dataset_label} physical lineage namespace differs from baseline plus frozen creates"
        )
    baseline_source = transition.baseline_source
    if transition.action is ComponentAction.REUSE:
        if source != baseline_source:
            raise CandidateValidationError(f"{dataset_label} REUSE canonical authority differs from baseline")
        return
    baseline_candidate_root = _plain_root(transition.baseline_root.parent)
    if transition.baseline_root != baseline_candidate_root / dataset:
        raise CandidateValidationError(f"{dataset_label} baseline component path differs")
    baseline_segments, baseline_overrides = _baseline_lineage_authority(
        dataset=dataset,
        candidate_root=baseline_candidate_root,
        source=baseline_source,
    )
    if list(overrides[: len(baseline_overrides)]) != baseline_overrides:
        raise CandidateValidationError(f"{dataset_label} historical override event prefix differs")
    if len(overrides) < len(baseline_overrides):
        raise CandidateValidationError(f"{dataset_label} historical override events were removed")
    new_overrides = list(overrides[len(baseline_overrides) :])
    authorized_lineage = {value for value in transition.authorized_create_paths if _is_canonical_lineage_file(value)}
    delta_keys = {
        PurePosixPath(value).parts[1] for value in authorized_lineage if PurePosixPath(value).parts[0] == "csv_deltas"
    }
    if delta_keys and delta_keys != {cutoff.strftime("%Y%m")}:
        raise CandidateValidationError(f"{dataset_label} delta namespace key differs from planner cutoff")
    selective_codes = _planner_selective_codes(
        transition.invalidation_scopes,
        expected_instruments=expected_instruments,
        expected_index_codes=expected_index_codes,
        dataset_label=dataset_label,
    )
    override_keys = {
        PurePosixPath(value).parts[1]
        for value in authorized_lineage
        if PurePosixPath(value).parts[0] == "csv_overrides"
    }
    if override_keys:
        expected_key = digest_named_fields(
            "dataset_release_csv_selective_override_v1",
            {
                "component": transition.component.value,
                "cutoff": cutoff,
                "codes": sorted(selective_codes),
                "scopes": list(transition.invalidation_scopes),
            },
        )[:16]
        if override_keys != {expected_key}:
            raise CandidateValidationError(f"{dataset_label} override namespace key differs from planner authority")
    elif new_overrides:
        raise CandidateValidationError(f"{dataset_label} override event lacks frozen create authority")
    if transition.action is ComponentAction.INCREMENTAL and (new_overrides or override_keys):
        raise CandidateValidationError(f"{dataset_label} INCREMENTAL transition inserted override lineage")

    state: dict[str, list[dict[str, Any]]] = {}
    for record in baseline_segments:
        state.setdefault(str(record["instrument"]), []).append(_public_segment_record(record))
    for code in state:
        state[code].sort(key=lambda item: (str(item["start"]), str(item["path_key"])))

    all_current_records: dict[str, dict[str, Any]] = {}
    for record in (
        *active_records,
        *replacement_records,
        *superseded_records.values(),
    ):
        path_key = str(record["path_key"])
        public = _public_segment_record(record)
        previous = all_current_records.setdefault(path_key, public)
        if previous != public:
            raise CandidateValidationError(f"{dataset_label} lineage path metadata conflicts during replay")
    baseline_paths = {
        f"{dataset}/{relative}".casefold() for relative in _baseline_canonical_paths(transition.baseline_evidence)
    }
    authorized_csv_paths = {
        f"{dataset}/{relative}".casefold() for relative in authorized_lineage if relative.endswith(".csv")
    }
    if any(path not in baseline_paths and path not in authorized_csv_paths for path in all_current_records):
        raise CandidateValidationError(f"{dataset_label} lineage inserted a path outside frozen transition authority")

    # Add only the newly authorized base/tail segments to the prior active
    # state.  Override replacements are replayed in event order below.
    new_non_override: list[dict[str, Any]] = []
    for path_key in sorted(authorized_csv_paths):
        record = all_current_records.get(path_key)
        if record is None:
            # Header-only tombstones and inactive index CSVs are sealed by the
            # namespace inventory but deliberately never become stock authority.
            inventory = manifest_evidence.get("inventory_records", {}).get(path_key)
            if inventory is None:
                raise CandidateValidationError(f"{dataset_label} frozen CSV create is absent from lineage/inventory")
            if int(inventory.get("rows", -1)) != 0 and str(inventory.get("instrument", "")) not in expected_index_codes:
                raise CandidateValidationError(f"{dataset_label} frozen CSV create is unbound")
            continue
        parts = PurePosixPath(str(record["root_relative_path"])).parts
        if len(parts) >= 2 and parts[1] == "csv_overrides":
            continue
        if str(record["instrument"]) not in expected_instruments:
            raise CandidateValidationError(f"{dataset_label} non-stock CSV entered active authority")
        new_non_override.append(record)
    for record in sorted(
        new_non_override,
        key=lambda item: (
            str(item["instrument"]),
            str(item["start"]),
            str(item["path_key"]),
        ),
    ):
        code = str(record["instrument"])
        current = state.setdefault(code, [])
        if current and str(record["start"]) <= str(current[-1]["end"]):
            raise CandidateValidationError(f"{dataset_label} frozen delta does not strictly append: {code}")
        current.append(record)

    new_replacements = list(replacement_records[len(baseline_overrides) :])
    if len(new_replacements) != len(new_overrides):
        raise CandidateValidationError(f"{dataset_label} override replay event/replacement counts differ")
    expected_event_codes = sorted(selective_codes.intersection(expected_instruments))
    observed_event_codes = [str(item["instrument"]) for item in new_replacements]
    if observed_event_codes != expected_event_codes:
        raise CandidateValidationError(f"{dataset_label} override event instruments differ from frozen scopes")
    for raw_event, replacement in zip(new_overrides, new_replacements, strict=True):
        code = str(replacement["instrument"])
        prior = state.get(code)
        if not prior:
            raise CandidateValidationError(f"{dataset_label} override supersedes a never-active segment: {code}")
        expected_superseded = [
            {
                "root_relative_path": item["root_relative_path"],
                "relative_path": item["relative_path"],
                "sha256": item["sha256"],
            }
            for item in prior
        ]
        if raw_event.get("superseded_segments") != expected_superseded:
            raise CandidateValidationError(
                f"{dataset_label} override superseded authority differs from prior active state"
            )
        expected_scopes = [
            dict(scope) for scope in transition.invalidation_scopes if _scope_authorizes_transition(scope, code)
        ]
        if raw_event.get("invalidation_scopes") != expected_scopes:
            raise CandidateValidationError(f"{dataset_label} override scopes differ from frozen invalidation authority")
        state[code] = [_public_segment_record(replacement)]

    replayed = sorted(
        (item for values in state.values() for item in values),
        key=lambda item: (
            str(item["instrument"]),
            str(item["start"]),
            str(item["end"]),
            str(item["path_key"]),
        ),
    )
    observed_active = sorted(
        (_public_segment_record(item) for item in active_records),
        key=lambda item: (
            str(item["instrument"]),
            str(item["start"]),
            str(item["end"]),
            str(item["path_key"]),
        ),
    )
    if replayed != observed_active:
        raise CandidateValidationError(f"{dataset_label} final active authority differs from durable transition replay")


def _baseline_lineage_authority(
    *, dataset: str, candidate_root: Path, source: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[Mapping[str, Any]]]:
    fields = DAILY_FIELDS if dataset == "daily_bin" else MINUTE_FIELDS
    if source.get("dataset") != dataset or tuple(source.get("ordered_fields") or ()) != ("date", "symbol", *fields):
        raise CandidateValidationError("baseline canonical source contract differs")
    if source.get("schema_version") == SEALED_QLIB_CSV_ROWS_SCHEMA:
        expected_root = f"{dataset}/csv"
        if source.get("root_relative_path") != expected_root:
            raise CandidateValidationError("baseline canonical source root differs")
        raw_files = source.get("files")
        if not isinstance(raw_files, list) or not raw_files:
            raise CandidateValidationError("baseline canonical source files are empty")
        raw_segments = [
            {**dict(item), "root_relative_path": expected_root} for item in raw_files if isinstance(item, Mapping)
        ]
        if len(raw_segments) != len(raw_files):
            raise CandidateValidationError("baseline canonical source file evidence is invalid")
        overrides: list[Mapping[str, Any]] = []
    elif source.get("schema_version") == SEALED_QLIB_CSV_COMPOSITE_SCHEMA:
        raw_segments = source.get("segments")
        overrides = source.get("overrides") or []
        if (
            not isinstance(raw_segments, list)
            or not raw_segments
            or not isinstance(overrides, list)
            or any(not isinstance(item, Mapping) for item in overrides)
        ):
            raise CandidateValidationError("baseline composite lineage authority is invalid")
    else:
        raise CandidateValidationError("baseline canonical schema is unsupported")
    records = [
        _validate_composite_segment_record(
            item,
            dataset=dataset,
            candidate_root=candidate_root,
            role="baseline active",
        )
        for item in raw_segments
    ]
    ordered = sorted(
        records,
        key=lambda item: (
            str(item["instrument"]),
            str(item["start"]),
            str(item["end"]),
            str(item["path_key"]),
        ),
    )
    if records != ordered:
        raise CandidateValidationError("baseline active segment authority is not canonical")
    return records, [dict(item) for item in overrides]


def _public_segment_record(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field: item[field]
        for field in (
            "instrument",
            "root_relative_path",
            "relative_path",
            "rows",
            "sha256",
            "size_bytes",
            "start",
            "end",
            "path_key",
        )
    }


def _baseline_canonical_paths(
    evidence: ComponentArtifactEvidence,
) -> frozenset[str]:
    return frozenset(
        item.relative_path
        for partition in evidence.artifact_partitions
        for item in partition.files
        if _is_canonical_lineage_file(item.relative_path)
    )


def _scope_authorizes_transition(scope: Mapping[str, Any], code: str) -> bool:
    kind = str(scope.get("kind", ""))
    if kind in {
        "qfq_denominator_change",
        "qfq_historical_numerator_revision",
    }:
        return str(scope.get("instrument", "")).upper() == code
    if kind == "pit_span_change":
        return code in {str(value).upper() for value in scope.get("changed_instruments") or ()}
    if kind == "historical_source_revision":
        return code in {str(value).upper() for value in scope.get("affected_instruments") or ()}
    return False


def _planner_selective_codes(
    scopes: Sequence[Mapping[str, Any]],
    *,
    expected_instruments: set[str],
    expected_index_codes: set[str],
    dataset_label: str,
) -> set[str]:
    allowed = set(expected_instruments).union(expected_index_codes)
    values: set[str] = set()
    for scope in scopes:
        kind = str(scope.get("kind", ""))
        if kind in {
            "qfq_denominator_change",
            "qfq_historical_numerator_revision",
        }:
            code = str(scope.get("instrument", "")).upper()
            if code:
                values.add(code)
        elif kind == "pit_span_change":
            values.update(str(value).upper() for value in scope.get("changed_instruments") or ())
        elif kind == "historical_source_revision":
            values.update(str(value).upper() for value in scope.get("affected_instruments") or ())
    if not values.issubset(allowed):
        raise CandidateValidationError(
            f"{dataset_label} frozen selective codes differ from PIT/profile index authority"
        )
    return values


def _validate_composite_segment_record(
    item: Any,
    *,
    dataset: str,
    candidate_root: Path,
    role: str,
) -> dict[str, Any]:
    dataset_label = "daily" if dataset == "daily_bin" else "minute"
    if not isinstance(item, Mapping) or not _COMPOSITE_SEGMENT_FIELDS.issubset(item):
        raise CandidateValidationError(f"{dataset_label} composite {role} segment fields drifted")
    code = str(item.get("instrument", "")).upper()
    if _STOCK_CODE.fullmatch(code) is None:
        raise CandidateValidationError(f"{dataset_label} composite {role} instrument is invalid")
    root_relative = _canonical_relative_path(item.get("root_relative_path"), label=f"{dataset_label} {role} root")
    relative = _canonical_relative_path(item.get("relative_path"), label=f"{dataset_label} {role} file")
    if "/" in relative or PurePosixPath(relative).name.casefold() != f"{code.casefold()}.csv":
        raise CandidateValidationError(f"{dataset_label} composite {role} filename differs")
    root_kind = _canonical_csv_root_kind(root_relative, dataset=dataset)
    ensure_sha256(str(item.get("sha256", "")), field=f"{dataset_label}_{role}_sha256")
    rows = item.get("rows")
    size_bytes = item.get("size_bytes")
    if (
        isinstance(rows, bool)
        or not isinstance(rows, int)
        or rows <= 0
        or isinstance(size_bytes, bool)
        or not isinstance(size_bytes, int)
        or size_bytes <= 0
    ):
        raise CandidateValidationError(f"{dataset_label} composite {role} size/rows are invalid")
    start = _canonical_segment_timestamp(item.get("start"), dataset=dataset, label=f"{dataset_label} {role} start")
    end = _canonical_segment_timestamp(item.get("end"), dataset=dataset, label=f"{dataset_label} {role} end")
    if end < start:
        raise CandidateValidationError(f"{dataset_label} composite {role} range is invalid")
    root, path, path_key = _resolve_canonical_csv_path(
        candidate_root=candidate_root,
        root_relative=root_relative,
        relative=relative,
        dataset_label=dataset_label,
    )
    if path.stat().st_size != size_bytes:
        raise CandidateValidationError(f"{dataset_label} composite {role} size differs")
    return {
        "instrument": code,
        "root_relative_path": root_relative,
        "relative_path": relative,
        "rows": rows,
        "sha256": str(item["sha256"]),
        "size_bytes": size_bytes,
        "start": start,
        "end": end,
        "root": root,
        "path": path,
        "path_key": path_key,
        "root_kind": root_kind,
        "identity": (path_key, str(item["sha256"])),
    }


def _validate_superseded_segment_record(
    item: Any,
    *,
    dataset: str,
    candidate_root: Path,
    instrument: str,
    max_chunk_rows: int,
    allowed_calendar: frozenset[str] | None = None,
) -> dict[str, Any]:
    dataset_label = "daily" if dataset == "daily_bin" else "minute"
    if not isinstance(item, Mapping) or set(item) != _SUPERSEDED_SEGMENT_FIELDS:
        raise CandidateValidationError(f"{dataset_label} superseded segment fields drifted")
    root_relative = _canonical_relative_path(item.get("root_relative_path"), label=f"{dataset_label} superseded root")
    relative = _canonical_relative_path(item.get("relative_path"), label=f"{dataset_label} superseded file")
    if "/" in relative or PurePosixPath(relative).name.casefold() != f"{instrument.casefold()}.csv":
        raise CandidateValidationError(f"{dataset_label} superseded segment instrument differs")
    root_kind = _canonical_csv_root_kind(root_relative, dataset=dataset)
    expected_sha = str(item.get("sha256", ""))
    ensure_sha256(expected_sha, field=f"{dataset_label}_superseded_sha256")
    root, path, path_key = _resolve_canonical_csv_path(
        candidate_root=candidate_root,
        root_relative=root_relative,
        relative=relative,
        dataset_label=dataset_label,
    )
    evidence = _stream_retired_canonical_csv(
        path,
        dataset=dataset,
        instrument=instrument,
        expected_sha256=expected_sha,
        max_chunk_rows=max_chunk_rows,
        allowed_calendar=allowed_calendar,
    )
    return {
        "instrument": instrument,
        "root_relative_path": root_relative,
        "relative_path": relative,
        "sha256": expected_sha,
        "root": root,
        "path": path,
        "path_key": path_key,
        "root_kind": root_kind,
        "identity": (path_key, expected_sha),
        "size_bytes": path.stat().st_size,
        **evidence,
    }


def _canonical_relative_path(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        raise CandidateValidationError(f"{label} is not a canonical relative path")
    parsed = PurePosixPath(value)
    if parsed.is_absolute() or any(part in {"", ".", ".."} for part in parsed.parts) or parsed.as_posix() != value:
        raise CandidateValidationError(f"{label} is not a canonical relative path")
    return value


def _canonical_csv_root_kind(root_relative: str, *, dataset: str) -> str:
    parts = PurePosixPath(root_relative).parts
    if parts == (dataset, "csv"):
        return "base"
    if len(parts) == 3 and parts[:2] == (dataset, "csv_deltas"):
        if _MONTH_KEY.fullmatch(parts[2]) is None:
            raise CandidateValidationError("composite CSV delta key is invalid")
        return "delta"
    if len(parts) == 3 and parts[:2] == (dataset, "csv_overrides"):
        if _OVERRIDE_KEY.fullmatch(parts[2]) is None:
            raise CandidateValidationError("composite CSV override key is invalid")
        return "override"
    raise CandidateValidationError("composite CSV root is outside canonical namespaces")


def _canonical_segment_timestamp(value: Any, *, dataset: str, label: str) -> str:
    if not isinstance(value, str):
        raise CandidateValidationError(f"{label} is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise CandidateValidationError(f"{label} is invalid") from exc
    canonical = parsed.isoformat(sep=" ", timespec="seconds")
    if value != canonical or (dataset == "daily_bin" and parsed.time().isoformat() != "00:00:00"):
        raise CandidateValidationError(f"{label} is non-canonical")
    return canonical


def _resolve_canonical_csv_path(
    *,
    candidate_root: Path,
    root_relative: str,
    relative: str,
    dataset_label: str,
) -> tuple[Path, Path, str]:
    logical_root = candidate_root / Path(root_relative)
    logical_path = logical_root / relative
    try:
        _plain_root(logical_root)
        _assert_plain(logical_path)
        root = logical_root.resolve(strict=True)
        path = logical_path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise CandidateValidationError(f"{dataset_label} composite CSV path is unavailable") from exc
    if candidate_root not in root.parents or path.parent != root or not path.is_file():
        raise CandidateValidationError(f"{dataset_label} composite CSV path escapes candidate")
    return root, path, path.relative_to(candidate_root).as_posix().casefold()


def _bind_lineage_path(values: dict[str, str], record: Mapping[str, Any], *, dataset_label: str) -> None:
    path_key = str(record["path_key"])
    sha = str(record["sha256"])
    prior = values.setdefault(path_key, sha)
    if prior != sha:
        raise CandidateValidationError(f"{dataset_label} immutable CSV path has conflicting hashes")


def _validate_override_invalidation_scopes(
    scopes: Any,
    *,
    instrument: str,
    expected_instruments: set[str],
    dataset_label: str,
) -> None:
    if not isinstance(scopes, list) or not scopes:
        raise CandidateValidationError(f"{dataset_label} override invalidation scopes are missing")
    seen: set[str] = set()
    authorized = False
    for scope in scopes:
        if not isinstance(scope, Mapping):
            raise CandidateValidationError(f"{dataset_label} override invalidation scope is invalid")
        digest = digest_named_fields("dataset_release_override_scope_v1", scope)
        if digest in seen:
            raise CandidateValidationError(f"{dataset_label} override invalidation scope is duplicate")
        seen.add(digest)
        kind = str(scope.get("kind", ""))
        if kind in {"qfq_denominator_change", "qfq_series_tail"}:
            _require_scope_fields(
                scope,
                (
                    {"kind", "instrument", "start", "end"}
                    if kind == "qfq_denominator_change"
                    else {"kind", "instrument", "old_row_count", "new_row_count"}
                ),
                dataset_label=dataset_label,
            )
            code = _scope_instrument(scope, dataset_label=dataset_label)
            if kind == "qfq_denominator_change":
                _scope_date_range(scope, dataset_label=dataset_label)
                authorized = authorized or code == instrument
            else:
                old_rows = scope.get("old_row_count")
                new_rows = scope.get("new_row_count")
                if (
                    isinstance(old_rows, bool)
                    or not isinstance(old_rows, int)
                    or isinstance(new_rows, bool)
                    or not isinstance(new_rows, int)
                    or old_rows < 0
                    or new_rows <= old_rows
                ):
                    raise CandidateValidationError(f"{dataset_label} qfq tail scope is invalid")
        elif kind == "qfq_historical_numerator_revision":
            _require_scope_fields(
                scope,
                {
                    "kind",
                    "instrument",
                    "months",
                    "downstream_observations",
                    "fallback_scope",
                },
                dataset_label=dataset_label,
            )
            code = _scope_instrument(scope, dataset_label=dataset_label)
            _scope_months(scope.get("months"), dataset_label=dataset_label)
            downstream = scope.get("downstream_observations")
            if isinstance(downstream, bool) or not isinstance(downstream, int) or downstream < 0:
                raise CandidateValidationError(f"{dataset_label} qfq historical scope is invalid")
            if scope.get("fallback_scope") not in {
                "instrument_full_history",
                "changed_months",
            }:
                raise CandidateValidationError(f"{dataset_label} qfq historical fallback scope is invalid")
            authorized = authorized or code == instrument
        elif kind == "pit_span_change":
            _require_scope_fields(
                scope,
                {
                    "kind",
                    "new_instruments",
                    "removed_instruments",
                    "changed_instruments",
                    "same_instrument_span_revision",
                },
                dataset_label=dataset_label,
            )
            changed = _scope_code_list(
                scope.get("changed_instruments"),
                dataset_label=dataset_label,
                field="changed_instruments",
            )
            new_codes = _scope_code_list(
                scope.get("new_instruments"),
                dataset_label=dataset_label,
                field="new_instruments",
            )
            removed_codes = _scope_code_list(
                scope.get("removed_instruments"),
                dataset_label=dataset_label,
                field="removed_instruments",
            )
            if scope.get("same_instrument_span_revision") is not bool(changed):
                raise CandidateValidationError(f"{dataset_label} PIT invalidation scope is inconsistent")
            if (
                not set(changed).issubset(expected_instruments)
                or not set(new_codes).issubset(expected_instruments)
                or removed_codes
            ):
                raise CandidateValidationError(f"{dataset_label} PIT invalidation instruments differ from current PIT")
            authorized = authorized or instrument in changed
        elif kind == "historical_source_revision":
            _require_scope_fields(
                scope,
                {"kind", "source_partition", "months", "affected_instruments"},
                dataset_label=dataset_label,
            )
            if not isinstance(scope.get("source_partition"), str) or not scope.get("source_partition"):
                raise CandidateValidationError(f"{dataset_label} historical source scope is invalid")
            _scope_months(scope.get("months"), dataset_label=dataset_label)
            affected = _scope_code_list(
                scope.get("affected_instruments"),
                dataset_label=dataset_label,
                field="affected_instruments",
            )
            if not set(affected).issubset(expected_instruments):
                raise CandidateValidationError(f"{dataset_label} historical invalidation instruments differ from PIT")
            authorized = authorized or instrument in affected
        else:
            raise CandidateValidationError(f"{dataset_label} override invalidation kind is unsupported: {kind}")
    if instrument not in expected_instruments or not authorized:
        raise CandidateValidationError(
            f"{dataset_label} override is not authorized by code-local invalidation: {instrument}"
        )


def _require_scope_fields(scope: Mapping[str, Any], expected: set[str], *, dataset_label: str) -> None:
    if set(scope) != expected:
        raise CandidateValidationError(f"{dataset_label} override invalidation scope fields drifted")


def _scope_instrument(scope: Mapping[str, Any], *, dataset_label: str) -> str:
    code = str(scope.get("instrument", "")).upper()
    if _STOCK_CODE.fullmatch(code) is None or scope.get("instrument") != code:
        raise CandidateValidationError(f"{dataset_label} invalidation instrument is invalid")
    return code


def _scope_date_range(scope: Mapping[str, Any], *, dataset_label: str) -> None:
    try:
        start = date.fromisoformat(str(scope.get("start", "")))
        end = date.fromisoformat(str(scope.get("end", "")))
    except ValueError as exc:
        raise CandidateValidationError(f"{dataset_label} invalidation date range is invalid") from exc
    if start.isoformat() != scope.get("start") or end.isoformat() != scope.get("end") or end < start:
        raise CandidateValidationError(f"{dataset_label} invalidation date range is invalid")


def _scope_months(value: Any, *, dataset_label: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise CandidateValidationError(f"{dataset_label} invalidation months are invalid")
    months = tuple(str(item) for item in value)
    if tuple(sorted(set(months))) != months or any(_MONTH_TEXT.fullmatch(item) is None for item in months):
        raise CandidateValidationError(f"{dataset_label} invalidation months are invalid")
    return months


def _scope_code_list(
    value: Any,
    *,
    dataset_label: str,
    field: str,
    allow_missing: bool = False,
) -> tuple[str, ...]:
    if value is None and allow_missing:
        return ()
    if not isinstance(value, list):
        raise CandidateValidationError(f"{dataset_label} invalidation {field} is invalid")
    codes = tuple(str(item) for item in value)
    if tuple(sorted(set(codes))) != codes or any(_STOCK_CODE.fullmatch(item) is None for item in codes):
        raise CandidateValidationError(f"{dataset_label} invalidation {field} is invalid")
    return codes


def _stream_retired_canonical_csv(
    path: Path,
    *,
    dataset: str,
    instrument: str,
    expected_sha256: str,
    max_chunk_rows: int,
    allowed_calendar: frozenset[str] | None = None,
    minimum_date: str | None = None,
) -> dict[str, Any]:
    fields = DAILY_FIELDS if dataset == "daily_bin" else MINUTE_FIELDS
    dataset_label = "daily" if dataset == "daily_bin" else "minute"
    digest = hashlib.sha256()
    rows = 0
    first_value: str | None = None
    prior_value: str | None = None
    last_value: str | None = None
    try:
        with path.open("rb") as raw:
            hashing = _DigestingRawReader(
                raw,
                digest=digest,
                max_line_bytes=1024 * 1024,
                dataset=f"{dataset_label} retired",
            )
            with io.BufferedReader(hashing, buffer_size=1024 * 1024) as buffered:
                for chunk in pd.read_csv(
                    buffered,
                    chunksize=max_chunk_rows,
                    dtype={"date": "string", "symbol": "string"},
                ):
                    if list(chunk.columns) != ["date", "symbol", *fields] or len(chunk) > max_chunk_rows:
                        raise CandidateValidationError(f"{dataset_label} retired CSV fields/chunk differ: {instrument}")
                    symbols = chunk["symbol"].str.strip().str.upper()
                    if not bool((symbols == instrument).all()):
                        raise CandidateValidationError(f"{dataset_label} retired CSV symbol differs: {instrument}")
                    timestamps = pd.to_datetime(chunk["date"], errors="raise")
                    values = (
                        timestamps.dt.strftime("%Y-%m-%d")
                        if dataset == "daily_bin"
                        else timestamps.dt.strftime("%Y-%m-%d %H:%M:%S")
                    )
                    if not bool((values == chunk["date"]).all()):
                        raise CandidateValidationError(
                            f"{dataset_label} retired CSV timestamp is non-canonical: {instrument}"
                        )
                    ordered = values.to_numpy(dtype=str)
                    if allowed_calendar is not None and any(value not in allowed_calendar for value in ordered):
                        raise CandidateValidationError(
                            f"{dataset_label} retired CSV key is outside frozen calendar: {instrument}"
                        )
                    if minimum_date is not None and any(value[:10] < minimum_date for value in ordered):
                        raise CandidateValidationError(
                            f"{dataset_label} retired CSV predates required_from: {instrument}"
                        )
                    if (prior_value is not None and ordered[0] <= prior_value) or (
                        len(ordered) > 1 and bool((ordered[1:] <= ordered[:-1]).any())
                    ):
                        raise CandidateValidationError(
                            f"{dataset_label} retired CSV keys are duplicate/unsorted: {instrument}"
                        )
                    numeric = chunk.loc[:, list(fields)].apply(pd.to_numeric, errors="raise")
                    if not np.isfinite(numeric.to_numpy(dtype=np.float64)).all():
                        raise CandidateValidationError(f"{dataset_label} retired CSV value is non-finite: {instrument}")
                    first_value = first_value or str(ordered[0])
                    prior_value = str(ordered[-1])
                    last_value = prior_value
                    rows += len(chunk)
    except (UnicodeDecodeError, pd.errors.ParserError, ValueError) as exc:
        if isinstance(exc, CandidateValidationError):
            raise
        raise CandidateValidationError(f"{dataset_label} retired CSV parse failed: {instrument}") from exc
    if rows <= 0 or digest.hexdigest() != expected_sha256:
        raise CandidateValidationError(f"{dataset_label} retired CSV immutable hash/rows differ: {instrument}")
    if dataset == "daily_bin":
        first_value = f"{first_value} 00:00:00"
        last_value = f"{last_value} 00:00:00"
    return {"rows": rows, "start": first_value, "end": last_value}


def _canonical_csv_tree(*, candidate_root: Path, dataset: str) -> tuple[dict[str, Path], list[tuple[str, str, Path]]]:
    component_root = _plain_root(candidate_root / dataset)
    files: dict[str, Path] = {}
    manifests: list[tuple[str, str, Path]] = []
    base = component_root / "csv"
    if not base.is_dir():
        raise CandidateValidationError("composite canonical base CSV root is missing")
    _plain_root(base)
    for child in sorted(base.iterdir(), key=lambda item: item.name.casefold()):
        _assert_plain(child)
        if not child.is_file() or child.suffix.casefold() != ".csv":
            raise CandidateValidationError("canonical base CSV root contains an unexpected entry")
        key = child.relative_to(candidate_root).as_posix().casefold()
        if key in files:
            raise CandidateValidationError("canonical CSV physical path is duplicate")
        files[key] = child
    for namespace, pattern in (("csv_deltas", _MONTH_KEY), ("csv_overrides", _OVERRIDE_KEY)):
        root = component_root / namespace
        if not root.exists():
            continue
        _plain_root(root)
        for key_root in sorted(root.iterdir(), key=lambda item: item.name):
            _assert_plain(key_root)
            if not key_root.is_dir() or pattern.fullmatch(key_root.name) is None:
                raise CandidateValidationError(f"canonical {namespace} contains an invalid namespace")
            _plain_root(key_root)
            manifest = key_root / "manifest.json"
            if not manifest.is_file():
                raise CandidateValidationError(f"canonical {namespace} namespace omits manifest")
            _assert_plain(manifest)
            manifests.append((namespace, key_root.name, manifest))
            for child in sorted(key_root.iterdir(), key=lambda item: item.name.casefold()):
                _assert_plain(child)
                if child == manifest:
                    continue
                if not child.is_file() or child.suffix.casefold() != ".csv":
                    raise CandidateValidationError(f"canonical {namespace} namespace contains an unexpected entry")
                path_key = child.relative_to(candidate_root).as_posix().casefold()
                if path_key in files:
                    raise CandidateValidationError("canonical CSV physical path is duplicate")
                files[path_key] = child
    return files, manifests


def _is_header_only_canonical_csv(path: Path, *, dataset: str) -> bool:
    try:
        if path.stat().st_size > 1024 * 1024:
            return False
        with path.open("rb") as handle:
            raw = handle.read(1024 * 1024 + 1)
        text = raw.decode("utf-8")
    except (OSError, UnicodeDecodeError):
        return False
    if len(raw) > 1024 * 1024:
        return False
    lines = text.splitlines()
    if len(lines) != 1:
        return False
    fields = DAILY_FIELDS if dataset == "daily_bin" else MINUTE_FIELDS
    try:
        parsed = next(csv.reader([lines[0]]))
    except csv.Error:
        return False
    return parsed == ["date", "symbol", *fields]


def _validate_canonical_namespace_manifests(
    *,
    candidate_root: Path,
    dataset: str,
    manifests: Sequence[tuple[str, str, Path]],
    physical_csv: Mapping[str, Path],
    top_path_sha: Mapping[str, str],
    lineage_records: Mapping[str, Mapping[str, Any]],
    expected_instruments: set[str],
    expected_index_codes: set[str],
    max_chunk_rows: int,
    allowed_calendar: frozenset[str] | None,
    cutoff: str | None,
    transition: _CanonicalTransitionContext | None,
    current_source: Mapping[str, Any],
) -> dict[str, Any]:
    dataset_label = "daily" if dataset == "daily_bin" else "minute"
    fields = DAILY_FIELDS if dataset == "daily_bin" else MINUTE_FIELDS
    allowed_index_codes = set(expected_index_codes)
    index_required_from = {
        definition.daily_code: definition.required_from.isoformat()
        for definition in DOMESTIC_INDEX_DEFINITIONS
        if definition.daily_code in allowed_index_codes
    }
    inventory_path_sha: dict[str, str] = {}
    inventory_records: dict[str, dict[str, Any]] = {}
    manifest_paths: set[str] = set()
    header_only_tombstones = 0
    inactive_index_csv = 0
    inherited_inventory_files = 0
    historical_manifest_replays_skipped = 0
    baseline_lineage_evidence: dict[str, Any] = {}
    can_inherit_historical = transition is not None and transition.verified_candidate_files is not None
    if can_inherit_historical:
        assert transition is not None
        baseline_lineage_evidence = {
            item.relative_path: item
            for partition in transition.baseline_evidence.artifact_partitions
            for item in partition.files
            if _is_canonical_lineage_file(item.relative_path)
        }
    for namespace, key, path in manifests:
        manifest_relative = normalize_root_relative_path(path.relative_to(candidate_root / dataset).as_posix())
        manifest_paths.add(manifest_relative)
        is_new_manifest = transition is not None and manifest_relative in transition.authorized_create_paths
        if can_inherit_historical and not is_new_manifest:
            manifest_evidence = baseline_lineage_evidence.get(manifest_relative)
            if manifest_evidence is None or not manifest_relative.endswith("/manifest.json"):
                raise CandidateValidationError(
                    f"{dataset_label} historical namespace manifest lacks baseline authority"
                )
            verified = transition.verified_candidate_files.get(manifest_relative)
            if verified != (
                manifest_evidence.size_bytes,
                manifest_evidence.sha256,
            ):
                raise CandidateValidationError(f"{dataset_label} historical namespace manifest identity differs")
            local_prefix = path.parent.relative_to(candidate_root).as_posix().casefold() + "/"
            local = {path_key: value for path_key, value in physical_csv.items() if path_key.startswith(local_prefix)}
            if not local:
                raise CandidateValidationError(f"{dataset_label} historical namespace inventory is empty")
            for path_key, local_path in local.items():
                component_relative = normalize_root_relative_path(
                    local_path.relative_to(candidate_root / dataset).as_posix()
                )
                evidence = baseline_lineage_evidence.get(component_relative)
                verified = transition.verified_candidate_files.get(component_relative)
                if evidence is None or verified != (evidence.size_bytes, evidence.sha256):
                    raise CandidateValidationError(f"{dataset_label} historical namespace CSV lacks baseline authority")
                prior = inventory_path_sha.setdefault(path_key, evidence.sha256)
                if prior != evidence.sha256:
                    raise CandidateValidationError(f"{dataset_label} historical namespace hash conflicts")
                top_sha = top_path_sha.get(path_key)
                if top_sha is not None and top_sha != evidence.sha256:
                    raise CandidateValidationError(f"{dataset_label} historical inventory differs from top lineage")
                if top_sha is None:
                    inherited_inventory_files += 1
            historical_manifest_replays_skipped += 1
            continue

        payload = _load_json(path)
        phase = "tail" if namespace == "csv_deltas" else "override"
        if (
            set(payload)
            != {
                "schema_version",
                "dataset",
                "component_action",
                "phase",
                "segment_key",
                "files",
                "canonical",
                "patch_actual_work",
            }
            or payload.get("schema_version") != "dataset_release_csv_segment_manifest_v2"
            or payload.get("dataset") != dataset
            or payload.get("phase") != phase
            or payload.get("segment_key") != key
            or payload.get("component_action") not in {"INCREMENTAL", "SELECTIVE_REBUILD"}
            or not isinstance(payload.get("patch_actual_work"), Mapping)
        ):
            raise CandidateValidationError(f"{dataset_label} canonical namespace manifest contract drifted")
        canonical = payload.get("canonical")
        if (
            not isinstance(canonical, Mapping)
            or canonical.get("schema_version") != SEALED_QLIB_CSV_COMPOSITE_SCHEMA
            or canonical.get("dataset") != dataset
            or tuple(canonical.get("ordered_fields") or ()) != ("date", "symbol", *fields)
            or canonical.get("merge_contract") not in {_COMPOSITE_APPEND_CONTRACT, _COMPOSITE_OVERRIDE_CONTRACT}
        ):
            raise CandidateValidationError(f"{dataset_label} canonical namespace manifest snapshot drifted")
        if is_new_manifest and (
            payload.get("component_action") != transition.action.value or canonical != current_source
        ):
            raise CandidateValidationError(f"{dataset_label} new namespace manifest is not bound to current transition")
        active_snapshot = _manifest_segment_path_sha(canonical.get("segments"))
        replacement_snapshot: dict[str, str] = {}
        retired_snapshot: dict[str, str] = {}
        for override in canonical.get("overrides") or ():
            if not isinstance(override, Mapping):
                raise CandidateValidationError(f"{dataset_label} canonical namespace override snapshot is invalid")
            replacement_snapshot.update(_manifest_segment_path_sha([override]))
            retired_snapshot.update(_manifest_segment_path_sha(override.get("superseded_segments")))
        local_prefix = path.parent.relative_to(candidate_root).as_posix().casefold() + "/"
        local = {key_value: value for key_value, value in physical_csv.items() if key_value.startswith(local_prefix)}
        inventory = payload.get("files")
        if not isinstance(inventory, list) or any(not isinstance(item, Mapping) for item in inventory):
            raise CandidateValidationError(f"{dataset_label} namespace file inventory is invalid")
        if not inventory:
            raise CandidateValidationError(f"{dataset_label} canonical namespace inventory is empty")
        if [str(item.get("relative_path", "")) for item in inventory] != sorted(
            str(item.get("relative_path", "")) for item in inventory
        ):
            raise CandidateValidationError(f"{dataset_label} namespace file inventory is not ordered")
        observed_local: set[str] = set()
        for item in inventory:
            if set(item) != {
                "instrument",
                "relative_path",
                "rows",
                "sha256",
                "size_bytes",
                "start",
                "end",
                "active",
            }:
                raise CandidateValidationError(f"{dataset_label} namespace file inventory fields drifted")
            code = str(item.get("instrument", ""))
            relative = _canonical_relative_path(item.get("relative_path"), label="namespace inventory file")
            if (
                _CANONICAL_CODE.fullmatch(code) is None
                or code != code.upper()
                or "/" in relative
                or PurePosixPath(relative).name.casefold() != f"{code.casefold()}.csv"
            ):
                raise CandidateValidationError(f"{dataset_label} namespace inventory instrument/path differs")
            path_key = f"{local_prefix}{relative}".casefold()
            if path_key in observed_local or path_key not in local:
                raise CandidateValidationError(f"{dataset_label} namespace inventory path differs")
            observed_local.add(path_key)
            sha = str(item.get("sha256", ""))
            ensure_sha256(sha, field=f"{dataset_label}_namespace_file_sha256")
            prior_sha = inventory_path_sha.setdefault(path_key, sha)
            if prior_sha != sha:
                raise CandidateValidationError(f"{dataset_label} namespace inventory hash conflicts")
            rows = item.get("rows")
            size_bytes = item.get("size_bytes")
            active = item.get("active")
            if (
                isinstance(rows, bool)
                or not isinstance(rows, int)
                or rows < 0
                or isinstance(size_bytes, bool)
                or not isinstance(size_bytes, int)
                or size_bytes <= 0
                or type(active) is not bool
                or local[path_key].stat().st_size != size_bytes
            ):
                raise CandidateValidationError(f"{dataset_label} namespace inventory rows/size/active differ")
            inventory_start = _inventory_timestamp(item.get("start"), dataset=dataset, rows=rows, field="start")
            inventory_end = _inventory_timestamp(item.get("end"), dataset=dataset, rows=rows, field="end")
            if rows > 0 and inventory_end < inventory_start:
                raise CandidateValidationError(f"{dataset_label} namespace inventory range is invalid")
            if rows > 0 and allowed_calendar is not None:
                start_key = str(inventory_start)
                end_key = str(inventory_end)
                if dataset == "daily_bin":
                    start_key = start_key[:10]
                    end_key = end_key[:10]
                if (
                    cutoff is None
                    or end_key[:10] > cutoff
                    or start_key not in allowed_calendar
                    or end_key not in allowed_calendar
                ):
                    raise CandidateValidationError(
                        f"{dataset_label} namespace inventory is outside frozen calendar/cutoff"
                    )
            top_sha = top_path_sha.get(path_key)
            if top_sha is not None and top_sha != sha:
                raise CandidateValidationError(f"{dataset_label} namespace inventory differs from top lineage")
            if active:
                if active_snapshot.get(path_key) != sha:
                    raise CandidateValidationError(f"{dataset_label} namespace active inventory differs from snapshot")
            elif active_snapshot.get(path_key) is not None:
                raise CandidateValidationError(f"{dataset_label} namespace inactive inventory remains active")
            lineage = lineage_records.get(path_key)
            inventory_records[path_key] = {
                "instrument": code,
                "root_relative_path": path.parent.relative_to(candidate_root).as_posix(),
                "relative_path": relative,
                "rows": rows,
                "sha256": sha,
                "size_bytes": size_bytes,
                "start": inventory_start,
                "end": inventory_end,
                "active": active,
            }
            if lineage is not None:
                if (
                    lineage.get("instrument") != code
                    or lineage.get("sha256") != sha
                    or int(lineage.get("rows", -1)) != rows
                    or int(lineage.get("size_bytes", -1)) != size_bytes
                    or lineage.get("start") != inventory_start
                    or lineage.get("end") != inventory_end
                ):
                    raise CandidateValidationError(f"{dataset_label} namespace inventory metadata differs from lineage")
                continue
            if rows == 0:
                if (
                    namespace != "csv_deltas"
                    or code not in expected_instruments
                    or inventory_start is not None
                    or inventory_end is not None
                    or active
                    or not _is_header_only_canonical_csv(local[path_key], dataset=dataset)
                    or sha256_file(local[path_key]) != sha
                ):
                    raise CandidateValidationError(f"{dataset_label} namespace tombstone is invalid")
                header_only_tombstones += 1
                continue
            if (
                dataset != "daily_bin"
                or code not in allowed_index_codes
                or code in expected_instruments
                or active
                or path_key in replacement_snapshot
                or path_key in retired_snapshot
            ):
                raise CandidateValidationError(f"{dataset_label} namespace inventory contains unbound CSV")
            required_from = index_required_from[code]
            if inventory_start is None or inventory_start[:10] < required_from:
                raise CandidateValidationError(f"{dataset_label} inactive index CSV predates required_from: {code}")
            evidence = _stream_retired_canonical_csv(
                local[path_key],
                dataset=dataset,
                instrument=code,
                expected_sha256=sha,
                max_chunk_rows=max_chunk_rows,
                allowed_calendar=allowed_calendar,
                minimum_date=required_from,
            )
            if evidence["rows"] != rows or evidence["start"] != inventory_start or evidence["end"] != inventory_end:
                raise CandidateValidationError(f"{dataset_label} inactive index CSV inventory differs")
            inactive_index_csv += 1
        if observed_local != set(local):
            raise CandidateValidationError(f"{dataset_label} namespace inventory does not match physical CSVs")
        snapshot_all = {**active_snapshot, **replacement_snapshot, **retired_snapshot}
        for path_key, sha in snapshot_all.items():
            if path_key.startswith(local_prefix) and top_path_sha.get(path_key) != sha:
                raise CandidateValidationError(f"{dataset_label} namespace manifest lineage differs from top-level")
    return {
        "inventory_path_sha": inventory_path_sha,
        "inventory_records": inventory_records,
        "manifest_paths": sorted(manifest_paths),
        "header_only_tombstones": header_only_tombstones,
        "inactive_index_csv": inactive_index_csv,
        "inherited_inventory_files": inherited_inventory_files,
        "historical_manifest_replays_skipped": historical_manifest_replays_skipped,
    }


def _inventory_timestamp(value: Any, *, dataset: str, rows: int, field: str) -> str | None:
    if rows == 0:
        if value is not None:
            raise CandidateValidationError(f"namespace zero-row inventory {field} is not null")
        return None
    if not isinstance(value, str):
        raise CandidateValidationError(f"namespace inventory {field} is invalid")
    normalized = value
    if dataset == "daily_bin" and len(value) == 10:
        try:
            normalized = f"{date.fromisoformat(value).isoformat()} 00:00:00"
        except ValueError as exc:
            raise CandidateValidationError(f"namespace inventory {field} is invalid") from exc
    return _canonical_segment_timestamp(
        normalized,
        dataset=dataset,
        label=f"namespace inventory {field}",
    )


def _manifest_segment_path_sha(value: Any) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, list) or any(not isinstance(item, Mapping) for item in value):
        raise CandidateValidationError("canonical namespace segment snapshot is invalid")
    result: dict[str, str] = {}
    for item in value:
        root = _canonical_relative_path(item.get("root_relative_path"), label="manifest segment root")
        relative = _canonical_relative_path(item.get("relative_path"), label="manifest segment file")
        sha = str(item.get("sha256", ""))
        ensure_sha256(sha, field="manifest_segment_sha256")
        path_key = f"{root}/{relative}".casefold()
        prior = result.setdefault(path_key, sha)
        if prior != sha:
            raise CandidateValidationError("canonical namespace snapshot has conflicting path hashes")
    return result


def _stream_minute_csv_parity(
    path: Path,
    *,
    instrument: str,
    expected_sha256: str,
    expected_rows: int,
    calendar_index: pd.Index,
    feature_values: Mapping[str, tuple[int, np.memmap]],
    max_chunk_rows: int,
    metrics: dict[str, Any] | None = None,
    boundaries: dict[str, str] | None = None,
) -> tuple[int, int, float, dict[str, int]]:
    ensure_sha256(expected_sha256, field=f"minute_source_sha256:{instrument}")
    digest = hashlib.sha256()
    rows = 0
    values_checked = 0
    maximum_delta = 0.0
    previous_timestamp: str | None = None
    day_counts: dict[str, int] = {}
    expected_header = ["date", "symbol", *MINUTE_FIELDS]
    try:
        with path.open("rb") as raw:
            hashing = _DigestingRawReader(
                raw,
                digest=digest,
                max_line_bytes=1024 * 1024,
                dataset="minute",
            )
            with io.BufferedReader(hashing, buffer_size=1024 * 1024) as buffered:
                chunks = pd.read_csv(
                    buffered,
                    chunksize=max_chunk_rows,
                    dtype={"date": "string", "symbol": "string"},
                )
                for chunk in chunks:
                    if metrics is not None:
                        metrics["chunks"] = metrics.get("chunks", 0) + 1
                        metrics["peak_chunk_rows"] = max(metrics.get("peak_chunk_rows", 0), len(chunk))
                    if list(chunk.columns) != expected_header or len(chunk) > max_chunk_rows:
                        raise CandidateValidationError(
                            f"minute canonical CSV field/chunk contract drifted: {instrument}"
                        )
                    symbols = chunk["symbol"].str.strip().str.upper()
                    if not bool((symbols == instrument).all()):
                        raise CandidateValidationError(f"minute canonical CSV symbol differs: {instrument}")
                    timestamps = pd.to_datetime(chunk["date"], errors="raise")
                    timestamp_text = timestamps.dt.strftime("%Y-%m-%d %H:%M:%S")
                    if not bool((timestamp_text == chunk["date"]).all()):
                        raise CandidateValidationError(f"minute canonical timestamp is non-canonical: {instrument}")
                    timestamp_values = timestamp_text.to_numpy(dtype=str)
                    if (previous_timestamp is not None and timestamp_values[0] <= previous_timestamp) or (
                        len(timestamp_values) > 1 and bool((timestamp_values[1:] <= timestamp_values[:-1]).any())
                    ):
                        raise CandidateValidationError(
                            f"minute canonical timestamps are duplicate/unsorted: {instrument}"
                        )
                    previous_timestamp = str(timestamp_values[-1])
                    if boundaries is not None:
                        boundaries.setdefault("start", str(timestamp_values[0]))
                        boundaries["end"] = str(timestamp_values[-1])
                    positions = calendar_index.get_indexer(timestamp_values)
                    if bool((positions < 0).any()):
                        raise CandidateValidationError(
                            f"minute canonical timestamp is absent from Qlib calendar: {instrument}"
                        )
                    expected = (
                        chunk.loc[:, list(MINUTE_FIELDS)]
                        .apply(pd.to_numeric, errors="raise")
                        .to_numpy(dtype=np.float64)
                    )
                    actual_columns: list[np.ndarray] = []
                    for field in MINUTE_FIELDS:
                        start, values_bin = feature_values[field]
                        offsets = positions - start + 1
                        if bool((offsets < 1).any()) or bool((offsets >= len(values_bin)).any()):
                            raise CandidateValidationError(
                                f"minute Qlib bin does not cover canonical keys: {instrument}:{field}"
                            )
                        actual_columns.append(np.asarray(values_bin[offsets], dtype=np.float64))
                    actual = np.column_stack(actual_columns)
                    if not np.isfinite(expected).all() or not np.isfinite(actual).all():
                        raise CandidateValidationError(f"minute canonical/bin value is non-finite: {instrument}")
                    equal = np.isclose(actual, expected, rtol=2e-6, atol=1e-3)
                    if not bool(equal.all()):
                        location = np.argwhere(~equal)[0]
                        raise CandidateValidationError(
                            "minute canonical/bin value parity differs: "
                            f"{instrument}:{timestamp_values[int(location[0])]}:"
                            f"{MINUTE_FIELDS[int(location[1])]}"
                        )
                    maximum_delta = max(maximum_delta, float(np.max(np.abs(actual - expected))))
                    unique_days, counts = np.unique(timestamp_values.astype("U10"), return_counts=True)
                    for day, count in zip(unique_days, counts):
                        day_counts[str(day)] = day_counts.get(str(day), 0) + int(count)
                    rows += len(chunk)
                    values_checked += int(expected.size)
    except (UnicodeDecodeError, pd.errors.ParserError, ValueError) as exc:
        if isinstance(exc, CandidateValidationError):
            raise
        raise CandidateValidationError(f"minute canonical CSV parse failed: {instrument}") from exc
    if rows != expected_rows:
        raise CandidateValidationError(f"minute canonical source row receipt differs: {instrument}")
    if digest.hexdigest() != expected_sha256:
        raise CandidateValidationError(f"minute canonical source digest differs: {instrument}")
    return rows, values_checked, maximum_delta, day_counts


class _DigestingRawReader(io.RawIOBase):
    """Hash and enforce a line bound while pandas performs bounded chunk reads."""

    def __init__(self, raw, *, digest: Any, max_line_bytes: int, dataset: str) -> None:
        self.raw = raw
        self.digest = digest
        self.max_line_bytes = max_line_bytes
        self.dataset = dataset
        self.trailing_line_bytes = 0

    def readable(self) -> bool:
        return True

    def readinto(self, target) -> int:
        data = self.raw.read(len(target))
        if not data:
            if self.trailing_line_bytes > self.max_line_bytes:
                raise CandidateValidationError(f"{self.dataset} canonical CSV line exceeds bound")
            return 0
        self.digest.update(data)
        parts = data.split(b"\n")
        if len(parts) == 1:
            self.trailing_line_bytes += len(data)
        else:
            if self.trailing_line_bytes + len(parts[0]) > self.max_line_bytes or any(
                len(value) > self.max_line_bytes for value in parts[1:-1]
            ):
                raise CandidateValidationError(f"{self.dataset} canonical CSV line exceeds bound")
            self.trailing_line_bytes = len(parts[-1])
        target[: len(data)] = data
        return len(data)


def _validate_candidate_format_smoke(
    *,
    root: Path,
    cutoff: date,
    daily_calendar: Sequence[str],
    minute_calendar: Sequence[str],
    daily_codes: set[str],
    minute_codes: set[str],
    max_rows: int,
) -> dict[str, Any]:
    common = sorted(daily_codes.intersection(minute_codes))
    if not common:
        raise CandidateValidationError("candidate format smoke has no common daily/minute stock instrument")
    instrument = common[0]
    daily_position = _exact_calendar_position(daily_calendar, cutoff.isoformat(), label="QE daily cutoff")
    minute_timestamp = minute_calendar[-1]
    if minute_timestamp[:10] != cutoff.isoformat():
        raise CandidateValidationError("candidate minute format smoke cutoff differs")
    minute_position = len(minute_calendar) - 1
    daily_values = {
        field: _read_consumer_value(
            root / "daily_bin" / "qlib" / "features" / instrument.lower() / f"{field}.day.bin",
            daily_position,
        )
        for field in DAILY_FIELDS
    }
    minute_values = {
        field: _read_consumer_value(
            root / "minute_bin" / "qlib" / "features" / instrument.lower() / f"{field}.1min.bin",
            minute_position,
        )
        for field in MINUTE_FIELDS
    }
    if not all(math.isfinite(value) for value in (*daily_values.values(), *minute_values.values())):
        raise CandidateValidationError("candidate format smoke loaded non-finite features")

    benchmark_rows = 0
    benchmark_cutoff = False
    benchmark_columns: tuple[str, ...] | None = None
    for frame in iter_hdf_frames(root / "index_context" / "index_daily.h5", chunksize=max_rows):
        try:
            benchmark = frame.xs(HMM_BENCHMARK_CODE, level="instrument", drop_level=False)
        except KeyError:
            continue
        if benchmark.empty:
            continue
        required = ("idx_close_point", "idx_return_1d")
        if any(field not in benchmark for field in required):
            raise CandidateValidationError("HMM consumer smoke fields are missing")
        numeric = benchmark.loc[:, list(required)].apply(pd.to_numeric, errors="coerce")
        if not np.isfinite(numeric.to_numpy(dtype=float)).all():
            raise CandidateValidationError("HMM consumer smoke loaded non-finite values")
        dates = pd.to_datetime(benchmark.index.get_level_values("datetime"))
        benchmark_cutoff = benchmark_cutoff or bool((dates.strftime("%Y-%m-%d") == cutoff.isoformat()).any())
        benchmark_rows += len(benchmark)
        benchmark_columns = required
    if benchmark_rows <= 0 or not benchmark_cutoff or benchmark_columns is None:
        raise CandidateValidationError("HMM 000300.SH consumer smoke does not reach cutoff")
    return {
        "qe": {
            "status": "PASS",
            "reader": "candidate_internal_float_bin_format_reader_v1",
            "instrument": instrument,
            "daily_cutoff": cutoff.isoformat(),
            "minute_cutoff": minute_timestamp,
            "daily_fields": sorted(daily_values),
            "minute_fields": sorted(minute_values),
        },
        "hmm": {
            "status": "PASS",
            "reader": "candidate_internal_index_h5_format_reader_v1",
            "benchmark": HMM_BENCHMARK_CODE,
            "rows": benchmark_rows,
            "fields": list(benchmark_columns),
            "cutoff": cutoff.isoformat(),
        },
        "consumer_activation": "not_activated",
    }


def _validate_external_consumer_smoke(
    value: Mapping[str, Any],
    *,
    profile: DatasetProfile,
    cutoff: date,
    require_production: bool,
) -> dict[str, Any]:
    try:
        return validate_candidate_consumer_smoke_receipt(
            value,
            profile=profile.profile,
            cutoff=cutoff,
            expected_index_codes=profile.index_codes,
            require_production=require_production,
            expected_stage_timeout_seconds=profile.stage_timeouts_seconds["consumer"],
        )
    except (CandidateConsumerSmokeError, TypeError, ValueError) as exc:
        raise CandidateValidationError(f"external Qlib/HMM consumer smoke is invalid: {exc}") from exc


def _exact_calendar_position(calendar: Sequence[str], value: str, *, label: str) -> int:
    try:
        return tuple(calendar).index(value)
    except ValueError as exc:
        raise CandidateValidationError(f"{label} is absent") from exc


def _read_consumer_value(path: Path, position: int) -> float:
    start, values = _read_float_bin(path)
    offset = position - start + 1
    if offset < 1 or offset >= len(values):
        raise CandidateValidationError(f"consumer smoke bin does not cover key: {path.name}")
    return float(values[offset])


def _validate_minute_overlay(value: Mapping[str, Any]) -> dict[str, Any]:
    expected_policy = "tdx_then_tushare_missing_keys_conflict_fail_v1"
    required_zero = (
        "missing_keys",
        "duplicate_keys",
        "overlap_mismatch_cells",
        "database_writes",
        "production_writes",
    )
    if value.get("source_policy") != expected_policy:
        raise CandidateValidationError("minute overlay source policy drifted")
    if any(int(value.get(field, -1)) != 0 for field in required_zero):
        raise CandidateValidationError("minute overlay contains missing/conflicting/mutating evidence")
    if int(value.get("provider_concurrency", -1)) != 1:
        raise CandidateValidationError("minute provider concurrency must be one")
    synthesized = int(value.get("synthesized_suspend_rows", 0))
    if synthesized < 0 or synthesized % 240:
        raise CandidateValidationError("minute synthesized suspension row count is invalid")
    source_rows = int(value.get("database_rows", 0)) + int(value.get("overlay_rows", 0)) + synthesized
    if source_rows <= 0:
        raise CandidateValidationError("minute overlay/source row count is empty")
    return {
        "source_policy": expected_policy,
        "source_rows": source_rows,
        "tdx_rows": int(value.get("tdx_rows", 0)),
        "tushare_rows": int(value.get("tushare_rows", 0)),
        "synthesized_suspend_rows": synthesized,
        **{field: 0 for field in required_zero},
        "provider_concurrency": 1,
    }


def _match_artifact_receipt(path: Path, expected: Mapping[str, Any], audit: Mapping[str, Any]) -> None:
    relative = expected.get("artifact_relative_path")
    if relative is None or Path(str(relative)).name != path.name:
        raise CandidateValidationError(f"artifact receipt path differs: {path.name}")
    for field in ("sha256", "rows", "size_bytes"):
        if field in expected and expected[field] != audit[field]:
            raise CandidateValidationError(f"artifact receipt differs: {path.name}:{field}")


def _audit_float_bin(
    path: Path,
    *,
    calendar_rows: int,
    required_positions: Sequence[int],
    require_finite: bool,
    exact_coverage: tuple[int, int] | None = None,
) -> None:
    start, values = _read_float_bin(path)
    if start < 0 or start >= calendar_rows:
        raise CandidateValidationError(f"Qlib bin start index is invalid: {path}")
    if not required_positions:
        raise CandidateValidationError(f"Qlib bin has no required PIT boundary: {path}")
    end = start + len(values) - 2
    if exact_coverage is not None and (start, end) != exact_coverage:
        raise CandidateValidationError(f"Qlib bin differs from exact required-from span: {path}")
    if start > min(required_positions) or end < max(required_positions):
        raise CandidateValidationError(f"Qlib bin does not cover PIT boundaries: {path}")
    body = values[1:]
    if np.isinf(body).any():
        raise CandidateValidationError(f"Qlib bin contains infinite values: {path}")
    if require_finite:
        for position in required_positions:
            value = float(values[position - start + 1])
            if not math.isfinite(value):
                raise CandidateValidationError(f"Qlib OHLC boundary is not readable: {path.name}:{position}")


def _build_calendar_boundary_index(
    calendar: Sequence[str],
) -> _CalendarBoundaryIndex:
    unique_dates: list[str] = []
    first_positions: list[int] = []
    last_positions: list[int] = []
    for position, value in enumerate(calendar):
        observed_date = value[:10]
        if not unique_dates or observed_date != unique_dates[-1]:
            unique_dates.append(observed_date)
            first_positions.append(position)
            last_positions.append(position)
        else:
            last_positions[-1] = position
    return _CalendarBoundaryIndex(
        unique_dates=tuple(unique_dates),
        first_positions=tuple(first_positions),
        last_positions=tuple(last_positions),
    )


def _span_boundary_positions(
    boundary_index: _CalendarBoundaryIndex,
    *,
    spans: Sequence[tuple[date, date]],
    dataset: str,
) -> tuple[int, ...]:
    boundaries: list[int] = []
    for start, end in spans:
        left = bisect.bisect_left(boundary_index.unique_dates, start.isoformat())
        right = bisect.bisect_right(boundary_index.unique_dates, end.isoformat()) - 1
        if left >= len(boundary_index.unique_dates) or right < left:
            raise CandidateValidationError(f"{dataset} PIT span has no trading calendar rows: {start}:{end}")
        boundaries.extend(
            (
                boundary_index.first_positions[left],
                boundary_index.last_positions[right],
            )
        )
    return tuple(sorted(set(boundaries)))


def _read_float_bin(path: Path) -> tuple[int, np.memmap]:
    _assert_plain(path)
    if not path.is_file() or path.stat().st_size < 8 or path.stat().st_size % 4:
        raise CandidateValidationError(f"Qlib bin file is missing/truncated: {path}")
    values = np.memmap(path, dtype="<f4", mode="r")
    start_value = float(values[0])
    if not math.isfinite(start_value) or start_value != math.floor(start_value):
        raise CandidateValidationError(f"Qlib bin start value is invalid: {path}")
    return int(start_value), values


def _parse_instrument_rows(path: Path, *, allow_multiple: bool) -> dict[str, list[tuple[date, date]]]:
    result: dict[str, list[tuple[date, date]]] = {}
    for ordinal, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        fields = line.split("\t")
        if len(fields) != 3:
            raise CandidateValidationError(f"instrument row {ordinal} is invalid")
        code = fields[0].strip().upper()
        start, end = date.fromisoformat(fields[1]), date.fromisoformat(fields[2])
        if start > end:
            raise CandidateValidationError(f"instrument row {ordinal} start>end")
        rows = result.setdefault(code, [])
        if rows and (not allow_multiple or start <= rows[-1][1]):
            raise CandidateValidationError(f"instrument spans overlap: {code}")
        rows.append((start, end))
    return result


def _outside_pit(index: pd.MultiIndex, spans: Mapping[str, Sequence[tuple[date, date]]]) -> int:
    frame = index.to_frame(index=False)
    frame["datetime"] = pd.to_datetime(frame["datetime"]).dt.date
    frame["instrument"] = frame["instrument"].astype(str).str.upper()
    outside = 0
    for code, group in frame.groupby("instrument", sort=False):
        ranges = spans.get(code)
        if not ranges:
            outside += len(group)
            continue
        dates = group["datetime"].to_numpy()
        keep = np.zeros(len(group), dtype=bool)
        for start, end in ranges:
            keep |= (dates >= start) & (dates <= end)
        outside += int((~keep).sum())
    return outside


def _span_map(snapshot: FrozenPitSnapshot) -> dict[str, tuple[tuple[date, date], ...]]:
    output: dict[str, list[tuple[date, date]]] = {}
    for span in snapshot.spans:
        output.setdefault(span.ts_code, []).append((span.eligible_start, span.eligible_end))
    return {key: tuple(value) for key, value in output.items()}


def _iter_expected_pit_keys(
    trading_dates: Sequence[str],
    spans: Mapping[str, Sequence[tuple[date, date]]],
) -> Iterable[tuple[pd.Timestamp, str]]:
    """Yield exact dense PIT keys in the artifact's datetime,instrument order."""

    ordered = tuple(sorted(spans.items()))
    for text in trading_dates:
        current = date.fromisoformat(str(text))
        timestamp = pd.Timestamp(current)
        for code, ranges in ordered:
            if any(start <= current <= end for start, end in ranges):
                yield timestamp, code


def _expected_pit_spans(snapshot: FrozenPitSnapshot, *, lower_bound: date) -> dict[str, tuple[tuple[date, date], ...]]:
    output: dict[str, list[tuple[date, date]]] = {}
    for span in snapshot.spans:
        start = max(span.eligible_start, lower_bound)
        end = min(span.eligible_end, snapshot.cutoff)
        if start <= end:
            output.setdefault(span.ts_code, []).append((start, end))
    return {key: tuple(value) for key, value in output.items()}


def _index_key(value: Any) -> tuple[pd.Timestamp, str]:
    return pd.Timestamp(value[0]), str(value[1])


def _load_json(path: Path) -> dict[str, Any]:
    try:
        _assert_plain(path)
        if path.stat().st_size > _MAX_JSON_EVIDENCE_BYTES:
            raise CandidateValidationError(f"JSON evidence exceeds bounded read limit: {path.name}")
        with path.open("rb") as handle:
            raw = handle.read(_MAX_JSON_EVIDENCE_BYTES + 1)
        if len(raw) > _MAX_JSON_EVIDENCE_BYTES:
            raise CandidateValidationError(f"JSON evidence exceeds bounded read limit: {path.name}")
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CandidateValidationError(f"JSON evidence is unreadable: {path.name}") from exc
    if not isinstance(payload, dict):
        raise CandidateValidationError(f"JSON evidence is not an object: {path.name}")
    return payload


def _plain_root(path: Path) -> Path:
    requested = Path(path).expanduser()
    requested = Path(os.path.abspath(os.fspath(requested)))
    current = Path(requested.anchor)
    if not current.exists():
        raise CandidateValidationError(f"candidate path anchor is unavailable: {requested.anchor}")
    _assert_plain(current)
    for part in requested.parts[1:]:
        current = current / part
        _assert_plain(current)
    resolved = requested.resolve(strict=True)
    if not resolved.is_dir():
        raise CandidateValidationError(f"candidate component is not a directory: {resolved}")
    return resolved


def _assert_plain(path: Path) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise CandidateValidationError(f"candidate path is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or (int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise CandidateValidationError(f"candidate path traverses symlink/reparse: {path}")


__all__ = [
    "CANDIDATE_VALIDATION_SCHEMA",
    "CandidateComponentTransitionAuthority",
    "CandidateValidationError",
    "CandidateValidationReport",
    "CandidateValidationSpec",
    "CandidateValidator",
]
