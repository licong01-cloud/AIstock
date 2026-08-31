"""Immutable component-level artifact evidence for mixed monthly rebuild plans.

The source manifest proves what was read.  This independent contract proves
which candidate files were produced from each exact source partition and which
private candidate paths a future incremental/selective writer may replace or
create.  A planner must fail closed per component when this evidence is absent
or incomplete; a source-content match alone never authorizes file reuse.
"""

from __future__ import annotations

import re
import hashlib
import json
from dataclasses import dataclass, field, replace
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator, Mapping, Sequence

from .canonical import (
    digest_named_fields,
    ensure_sha256,
    merkle_root_from_named_digests,
    normalize_root_relative_path,
)
from .cas_store import CASRef, CASStore, canonical_json_bytes
from .contracts import Component
from .errors import DatasetReleaseError


COMPONENT_ARTIFACT_MANIFEST_SCHEMA = "dataset_release_component_artifact_manifest_v1"
COMPONENT_ARTIFACT_COMPONENT_SCHEMA = "dataset_release_component_artifact_component_v1"
COMPONENT_ARTIFACT_COMPONENT_SHARD_SCHEMA = "dataset_release_component_artifact_component_shard_v1"
COMPONENT_ARTIFACT_MANIFEST_STORAGE_SCHEMA_V2 = "dataset_release_component_artifact_manifest_v2"
COMPONENT_ARTIFACT_COMPONENT_INDEX_SCHEMA_V2 = "dataset_release_component_artifact_component_index_v2"
COMPONENT_ARTIFACT_SECTION_SHARD_SCHEMA_V2 = "dataset_release_component_artifact_section_shard_v2"
COMPONENT_ARTIFACT_PARTITION_SCHEMA = "dataset_release_component_artifact_partition_v1"
COMPONENT_ARTIFACT_FILE_SCHEMA = "dataset_release_component_artifact_file_v1"
COMPONENT_SOURCE_PARTITION_SCHEMA = "dataset_release_component_source_partition_v1"
COMPONENT_MUTATION_RULE_SCHEMA = "dataset_release_component_mutation_rule_v1"
COMPONENT_ADJ_SERIES_SCHEMA = "dataset_release_component_adj_series_v1"
SOURCE_MONTH_CONTENT_LEAF_SCHEMA = "dataset_release_source_month_content_leaf_v1"
MAX_COMPONENT_MANIFEST_BYTES = 32 * 1024 * 1024
TARGET_COMPONENT_SECTION_SHARD_BYTES = 8 * 1024 * 1024
MAX_COMPONENT_SECTION_ROWS = 128

_V2_SECTIONS = (
    "source_partitions",
    "artifact_partition_headers",
    "artifact_files",
    "artifact_instruments",
    "mutation_rule_targets",
    "pit_authority",
    "adj_authority",
)

_INSTRUMENT = re.compile(r"^[0-9]{6}\.(?:SH|SZ|CSI)$")
_STOCK_INSTRUMENT = re.compile(r"^[0-9]{6}\.(?:SH|SZ)$")
_SAFE_REASON = re.compile(r"^[A-Z0-9_]{1,96}$")
_INSTRUMENT_PLACEHOLDER = "{instrument}"
_ZERO_SAFETY = {
    "database_writes": 0,
    "provider_database_writes": 0,
    "candidate_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
}


class ComponentArtifactManifestError(DatasetReleaseError):
    code = "BLOCKED_COMPONENT_ARTIFACT_MANIFEST_INVALID"


@dataclass(frozen=True, slots=True)
class SourcePartitionEvidence:
    identity: str
    dataset: str
    partition_key: str
    row_count: int
    content_digest: str
    schema_digest: str
    source_table_schema_digest: str | None
    source_code_membership_digest: str | None
    min_key: Any
    max_key: Any
    monthly_content_leaves: tuple[Mapping[str, Any], ...]
    partition_identity: str
    affected_instruments: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        value = {
            "identity": self.identity,
            "dataset": self.dataset,
            "partition_key": self.partition_key,
            "row_count": self.row_count,
            "content_digest": self.content_digest,
            "schema_digest": self.schema_digest,
            "source_table_schema_digest": self.source_table_schema_digest,
            "source_code_membership_digest": self.source_code_membership_digest,
            "min_key": self.min_key,
            "max_key": self.max_key,
            "monthly_content_leaves": [dict(value) for value in self.monthly_content_leaves],
            "partition_identity": self.partition_identity,
        }
        if self.affected_instruments:
            value["affected_instruments"] = list(self.affected_instruments)
        return value


@dataclass(frozen=True, slots=True)
class ArtifactFileEvidence:
    relative_path: str
    size_bytes: int
    sha256: str
    instrument: str | None
    file_identity: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "relative_path": self.relative_path,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "instrument": self.instrument,
            "file_identity": self.file_identity,
        }


@dataclass(frozen=True, slots=True)
class ArtifactPartitionEvidence:
    partition_key: str
    source_partition_identities: tuple[str, ...]
    dependency_edges: tuple[str, ...]
    instruments: tuple[str, ...]
    start: date | None
    end: date | None
    files: tuple[ArtifactFileEvidence, ...]
    partition_identity: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "partition_key": self.partition_key,
            "source_partition_identities": list(self.source_partition_identities),
            "dependency_edges": list(self.dependency_edges),
            "instruments": list(self.instruments),
            "start": self.start.isoformat() if self.start else None,
            "end": self.end.isoformat() if self.end else None,
            "files": [item.as_dict() for item in self.files],
            "partition_identity": self.partition_identity,
        }


@dataclass(frozen=True, slots=True)
class MutationRuleEvidence:
    rule_id: str
    datasets: tuple[str, ...]
    replace_existing_targets: tuple[str, ...]
    create_new_targets: tuple[str, ...]
    create_target_templates: tuple[str, ...]
    writer_targets_by_instrument: Mapping[str, tuple[str, ...]]
    writer_target_policy: str
    dependency_edges: tuple[str, ...]
    rule_identity: str
    _artifact_file_targets: Mapping[str, tuple[str, ...]] | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "datasets": list(self.datasets),
            "replace_existing_targets": list(self.replace_existing_targets),
            "create_new_targets": list(self.create_new_targets),
            "create_target_templates": list(self.create_target_templates),
            "writer_targets_by_instrument": {
                code: list(targets) for code, targets in self.writer_targets_by_instrument.items()
            },
            "writer_target_policy": self.writer_target_policy,
            "dependency_edges": list(self.dependency_edges),
            "rule_identity": self.rule_identity,
        }

    def targets_for_instruments(
        self,
        instruments: Sequence[str],
        *,
        create_for_instruments: Sequence[str] = (),
        instrument_file_targets: Mapping[str, tuple[str, ...]] | None = None,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        requested = tuple(sorted({_instrument(value) for value in instruments}))
        create_codes = tuple(sorted({_instrument(value) for value in create_for_instruments}))
        replace = set(self.replace_existing_targets)
        for code in requested:
            targets = self.writer_targets_by_instrument.get(code)
            fallback_targets = (
                instrument_file_targets if instrument_file_targets is not None else self._artifact_file_targets
            )
            if (
                not targets
                and self.writer_target_policy == "artifact_file_instrument_index_v1"
                and fallback_targets is not None
            ):
                targets = tuple(path for path in fallback_targets.get(code, ()) if _mutable_writer_target(path))
            if not targets:
                raise ComponentArtifactManifestError("mutation rule lacks exact existing-instrument targets")
            replace.update(targets)
        creates = set(self.create_new_targets)
        for code in create_codes:
            for template in self.create_target_templates:
                creates.add(normalize_root_relative_path(template.replace(_INSTRUMENT_PLACEHOLDER, code.casefold())))
        if create_codes and not self.create_target_templates:
            raise ComponentArtifactManifestError("mutation rule lacks new-instrument target templates")
        return tuple(sorted(replace)), tuple(sorted(creates))


@dataclass(frozen=True, slots=True)
class AdjSeriesEvidence:
    complete: bool
    qfq_denominator_by_code: Mapping[str, str]
    ordered_adj_digest_by_code: Mapping[str, str]
    adj_row_count_by_code: Mapping[str, int]
    monthly_ordered_adj_by_code: Mapping[str, Mapping[str, Mapping[str, Any]]]
    writer_targets_by_code: Mapping[str, tuple[str, ...]]
    shared_writer_targets: tuple[str, ...]
    writer_target_policy: str
    evidence_identity: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "qfq_denominator_by_code": dict(self.qfq_denominator_by_code),
            "ordered_adj_digest_by_code": dict(self.ordered_adj_digest_by_code),
            "adj_row_count_by_code": dict(self.adj_row_count_by_code),
            "monthly_ordered_adj_by_code": {
                code: {month: dict(leaf) for month, leaf in months.items()}
                for code, months in self.monthly_ordered_adj_by_code.items()
            },
            "writer_targets_by_code": {code: list(targets) for code, targets in self.writer_targets_by_code.items()},
            "shared_writer_targets": list(self.shared_writer_targets),
            "writer_target_policy": self.writer_target_policy,
            "evidence_identity": self.evidence_identity,
        }


@dataclass(frozen=True, slots=True)
class ComponentArtifactEvidence:
    component: Component
    status: str
    reason_code: str | None
    component_root_relative_path: str | None = None
    source_partitions: tuple[SourcePartitionEvidence, ...] = ()
    artifact_partitions: tuple[ArtifactPartitionEvidence, ...] = ()
    append_rules: tuple[MutationRuleEvidence, ...] = ()
    pit_mutation_rule: MutationRuleEvidence | None = None
    pit_instruments: tuple[str, ...] = ()
    pit_span_digest_by_code: Mapping[str, str] | None = None
    adj_series: AdjSeriesEvidence | None = None
    component_identity: str | None = None
    file_identity: str | None = None
    component_manifest_root: str | None = None
    filesystem_tree_merkle: str | None = None

    @property
    def complete(self) -> bool:
        return self.status == "COMPLETE"

    @property
    def all_file_paths(self) -> tuple[str, ...]:
        return tuple(sorted(file.relative_path for partition in self.artifact_partitions for file in partition.files))

    @property
    def source_by_identity(self) -> dict[str, SourcePartitionEvidence]:
        return {item.identity: item for item in self.source_partitions}

    @property
    def instrument_file_targets(self) -> dict[str, tuple[str, ...]]:
        return _artifact_instrument_file_targets(self.artifact_partitions)

    def adj_writer_targets(self, instrument: str) -> tuple[str, ...]:
        if self.adj_series is None:
            return ()
        code = _stock_instrument(instrument)
        values = {path for path in self.adj_series.shared_writer_targets if _mutable_writer_target(path)}
        values.update(
            path for path in self.adj_series.writer_targets_by_code.get(code, ()) if _mutable_writer_target(path)
        )
        if self.adj_series.writer_target_policy == "artifact_file_instrument_index_v1":
            values.update(path for path in self.instrument_file_targets.get(code, ()) if _mutable_writer_target(path))
        return tuple(sorted(values))

    def as_dict(self) -> dict[str, Any]:
        if not self.complete:
            return {"status": self.status, "reason_code": self.reason_code}
        assert self.component_identity is not None
        assert self.file_identity is not None
        assert self.component_manifest_root is not None
        assert self.component_root_relative_path is not None
        assert self.filesystem_tree_merkle is not None
        return {
            "status": self.status,
            "reason_code": None,
            "component": self.component.value,
            "component_root_relative_path": self.component_root_relative_path,
            "source_partitions": [item.as_dict() for item in self.source_partitions],
            "artifact_partitions": [item.as_dict() for item in self.artifact_partitions],
            "append_rules": [item.as_dict() for item in self.append_rules],
            "pit_mutation_rule": (self.pit_mutation_rule.as_dict() if self.pit_mutation_rule else None),
            "pit_instruments": list(self.pit_instruments),
            "pit_span_digest_by_code": dict(self.pit_span_digest_by_code or {}),
            "adj_series": self.adj_series.as_dict() if self.adj_series else None,
            "component_identity": self.component_identity,
            "file_identity": self.file_identity,
            "component_manifest_root": self.component_manifest_root,
            "filesystem_tree_merkle": self.filesystem_tree_merkle,
        }


@dataclass(frozen=True, slots=True)
class ComponentArtifactManifest:
    reference: CASRef
    profile: str
    scope: str
    cutoff: date
    candidate_identity: str
    artifact_root: str
    semantic_profile_digest: str
    producer_fingerprint: str
    artifact_fingerprint: str
    validation_fingerprint: str
    source_content_root: str
    artifact_ready_content_root: str
    pit_snapshot_digest: str
    candidate_metadata: Mapping[str, Mapping[str, Any]]
    components: Mapping[Component, ComponentArtifactEvidence]
    manifest_root: str

    def component(self, component: Component) -> ComponentArtifactEvidence:
        return self.components[component]


def seal_component_artifact_manifest(
    cas: CASStore,
    value: Mapping[str, Any],
) -> CASRef:
    """Seal semantic v1 evidence into bounded, independently verified v2 shards."""

    normalized, components = _normalize_manifest(value, require_identities=False)
    compacted, compacted_any = _compact_duplicate_writer_targets(components)
    if compacted_any:
        semantic = dict(normalized)
        semantic["components"] = {component.value: compacted[component].as_dict() for component in Component}
        del normalized, components, value
        normalized, components = _normalize_manifest(semantic, require_identities=False)
        del semantic, compacted
    else:
        del value, compacted

    component_index: dict[str, Mapping[str, Any]] = {}
    for component in Component:
        evidence = components[component]
        if not evidence.complete:
            component_index[component.value] = evidence.as_dict()
            continue
        component_index[component.value] = _seal_v2_component_index(
            cas,
            profile=str(normalized["profile"]),
            candidate_identity=str(normalized["candidate_identity"]),
            artifact_root=str(normalized["artifact_root"]),
            evidence=evidence,
        )

    top_body = {
        "schema_version": COMPONENT_ARTIFACT_MANIFEST_STORAGE_SCHEMA_V2,
        "semantic_schema_version": COMPONENT_ARTIFACT_MANIFEST_SCHEMA,
        "semantic_manifest_root": normalized["manifest_root"],
        **{
            key: normalized[key]
            for key in (
                "profile",
                "scope",
                "cutoff",
                "candidate_identity",
                "artifact_root",
                "semantic_profile_digest",
                "producer_fingerprint",
                "artifact_fingerprint",
                "validation_fingerprint",
                "source_content_root",
                "artifact_ready_content_root",
                "pit_snapshot_digest",
                "safety",
            )
        },
        "components": component_index,
    }
    if "candidate_metadata" in normalized:
        top_body["candidate_metadata"] = normalized["candidate_metadata"]
    manifest_root = digest_named_fields(COMPONENT_ARTIFACT_MANIFEST_STORAGE_SCHEMA_V2, top_body)
    reference = cas.put_json({**top_body, "manifest_root": manifest_root})
    if reference.size > MAX_COMPONENT_MANIFEST_BYTES:
        raise ComponentArtifactManifestError("component artifact v2 top index exceeds bound")
    del component_index, components, normalized
    loaded = load_component_artifact_manifest(cas, reference)
    if loaded.manifest_root != manifest_root:
        raise ComponentArtifactManifestError("component manifest readback differs")
    return reference


def _seal_component_artifact_manifest_v1(
    cas: CASStore,
    value: Mapping[str, Any],
) -> CASRef:
    """Legacy writer retained only to prove the public dual-reader contract."""

    normalized, components = _normalize_manifest(value, require_identities=False)
    component_index: dict[str, Mapping[str, Any]] = {}
    for component in Component:
        evidence = components[component]
        if not evidence.complete:
            component_index[component.value] = evidence.as_dict()
            continue
        shard_body = {
            "schema_version": COMPONENT_ARTIFACT_COMPONENT_SHARD_SCHEMA,
            "profile": normalized["profile"],
            "candidate_identity": normalized["candidate_identity"],
            "artifact_root": normalized["artifact_root"],
            "component": component.value,
            "evidence": evidence.as_dict(),
            "safety": dict(_ZERO_SAFETY),
        }
        shard_root = digest_named_fields(COMPONENT_ARTIFACT_COMPONENT_SHARD_SCHEMA, shard_body)
        shard_ref = cas.put_json({**shard_body, "shard_root": shard_root})
        if shard_ref.size > MAX_COMPONENT_MANIFEST_BYTES:
            raise ComponentArtifactManifestError(f"component artifact shard exceeds bound: {component.value}")
        component_index[component.value] = {
            "status": "COMPLETE",
            "component_manifest_ref": shard_ref.as_dict(),
            "shard_root": shard_root,
            "component_manifest_root": evidence.component_manifest_root,
            "filesystem_tree_merkle": evidence.filesystem_tree_merkle,
            "component_root_relative_path": evidence.component_root_relative_path,
        }
    top_body = {
        key: normalized[key]
        for key in (
            "schema_version",
            "profile",
            "scope",
            "cutoff",
            "candidate_identity",
            "artifact_root",
            "semantic_profile_digest",
            "producer_fingerprint",
            "artifact_fingerprint",
            "validation_fingerprint",
            "source_content_root",
            "artifact_ready_content_root",
            "pit_snapshot_digest",
            "safety",
        )
    }
    if "candidate_metadata" in normalized:
        top_body["candidate_metadata"] = normalized["candidate_metadata"]
    top_body["components"] = component_index
    manifest_root = digest_named_fields(COMPONENT_ARTIFACT_MANIFEST_SCHEMA, top_body)
    reference = cas.put_json({**top_body, "manifest_root": manifest_root})
    loaded = load_component_artifact_manifest(cas, reference)
    if loaded.manifest_root != manifest_root:
        raise ComponentArtifactManifestError("component manifest readback differs")
    return reference


def load_component_artifact_manifest(
    cas: CASStore,
    reference: CASRef | Mapping[str, Any] | str,
) -> ComponentArtifactManifest:
    supplied = CASRef.from_value(reference)
    if supplied.size < 0:
        raise ComponentArtifactManifestError("component artifact manifest requires a complete CAS reference")
    verified = cas.verify(supplied)
    if verified.relative_path != supplied.relative_path:
        raise ComponentArtifactManifestError("component artifact manifest CAS path is non-canonical")
    value = cas.get_json_bounded(verified, max_bytes=MAX_COMPONENT_MANIFEST_BYTES)
    if not isinstance(value, Mapping):
        raise ComponentArtifactManifestError("component artifact manifest is not an object")
    schema_version = value.get("schema_version")
    if schema_version == COMPONENT_ARTIFACT_MANIFEST_STORAGE_SCHEMA_V2:
        return _load_component_artifact_manifest_v2(cas, verified, value)
    if schema_version != COMPONENT_ARTIFACT_MANIFEST_SCHEMA:
        raise ComponentArtifactManifestError("component artifact index schema differs")
    top_required = {
        "schema_version",
        "profile",
        "scope",
        "cutoff",
        "candidate_identity",
        "artifact_root",
        "semantic_profile_digest",
        "producer_fingerprint",
        "artifact_fingerprint",
        "validation_fingerprint",
        "source_content_root",
        "artifact_ready_content_root",
        "pit_snapshot_digest",
        "components",
        "safety",
        "manifest_root",
    }
    top_allowed = top_required | {"candidate_metadata"}
    if set(value).difference(top_allowed) or not top_required.issubset(value):
        raise ComponentArtifactManifestError("component artifact index fields differ")
    raw_index = value.get("components")
    if not isinstance(raw_index, Mapping) or set(raw_index) != {item.value for item in Component}:
        raise ComponentArtifactManifestError("component artifact index is incomplete")
    expanded_components: dict[str, Mapping[str, Any]] = {}
    for component in Component:
        entry = raw_index[component.value]
        if not isinstance(entry, Mapping):
            raise ComponentArtifactManifestError("component artifact index entry is invalid")
        if entry.get("status") == "UNAVAILABLE":
            expanded_components[component.value] = dict(entry)
            continue
        expected_index_fields = {
            "status",
            "component_manifest_ref",
            "shard_root",
            "component_manifest_root",
            "filesystem_tree_merkle",
            "component_root_relative_path",
        }
        if set(entry) != expected_index_fields or entry.get("status") != "COMPLETE":
            raise ComponentArtifactManifestError("component artifact shard index differs")
        shard_ref = _complete_ref(cas, entry["component_manifest_ref"])
        shard = cas.get_json_bounded(shard_ref, max_bytes=MAX_COMPONENT_MANIFEST_BYTES)
        if not isinstance(shard, Mapping):
            raise ComponentArtifactManifestError("component artifact shard is invalid")
        shard_body = dict(shard)
        declared_shard_root = shard_body.pop("shard_root", None)
        expected_shard_fields = {
            "schema_version",
            "profile",
            "candidate_identity",
            "artifact_root",
            "component",
            "evidence",
            "safety",
        }
        shard_root = digest_named_fields(COMPONENT_ARTIFACT_COMPONENT_SHARD_SCHEMA, shard_body)
        if (
            set(shard_body) != expected_shard_fields
            or shard_body.get("schema_version") != COMPONENT_ARTIFACT_COMPONENT_SHARD_SCHEMA
            or shard_body.get("profile") != value["profile"]
            or shard_body.get("candidate_identity") != value["candidate_identity"]
            or shard_body.get("artifact_root") != value["artifact_root"]
            or shard_body.get("component") != component.value
            or shard_body.get("safety") != _ZERO_SAFETY
            or declared_shard_root != shard_root
            or entry.get("shard_root") != shard_root
            or not isinstance(shard_body.get("evidence"), Mapping)
        ):
            raise ComponentArtifactManifestError("component artifact shard identity differs")
        expanded_components[component.value] = dict(shard_body["evidence"])
    expanded = {key: value[key] for key in top_required.difference({"components", "manifest_root"})}
    if "candidate_metadata" in value:
        expanded["candidate_metadata"] = value["candidate_metadata"]
    expanded["components"] = expanded_components
    preliminary, _ = _normalize_manifest(expanded, require_identities=False)
    expanded["manifest_root"] = preliminary["manifest_root"]
    normalized, components = _normalize_manifest(expanded, require_identities=True)
    for component in Component:
        evidence = components[component]
        entry = raw_index[component.value]
        if evidence.complete and (
            entry.get("component_manifest_root") != evidence.component_manifest_root
            or entry.get("filesystem_tree_merkle") != evidence.filesystem_tree_merkle
            or entry.get("component_root_relative_path") != evidence.component_root_relative_path
        ):
            raise ComponentArtifactManifestError("component artifact shard index root differs")
    top_body = {key: value[key] for key in top_required.difference({"manifest_root"})}
    if "candidate_metadata" in value:
        top_body["candidate_metadata"] = value["candidate_metadata"]
    top_root = digest_named_fields(COMPONENT_ARTIFACT_MANIFEST_SCHEMA, top_body)
    if value.get("safety") != _ZERO_SAFETY or value.get("manifest_root") != top_root:
        raise ComponentArtifactManifestError("component artifact index root/safety differs")
    return ComponentArtifactManifest(
        reference=verified,
        profile=str(normalized["profile"]),
        scope=str(normalized["scope"]),
        cutoff=date.fromisoformat(str(normalized["cutoff"])),
        candidate_identity=str(normalized["candidate_identity"]),
        artifact_root=str(normalized["artifact_root"]),
        semantic_profile_digest=str(normalized["semantic_profile_digest"]),
        producer_fingerprint=str(normalized["producer_fingerprint"]),
        artifact_fingerprint=str(normalized["artifact_fingerprint"]),
        validation_fingerprint=str(normalized["validation_fingerprint"]),
        source_content_root=str(normalized["source_content_root"]),
        artifact_ready_content_root=str(normalized["artifact_ready_content_root"]),
        pit_snapshot_digest=str(normalized["pit_snapshot_digest"]),
        candidate_metadata=dict(normalized.get("candidate_metadata") or {}),
        components=components,
        manifest_root=top_root,
    )


def _compact_duplicate_writer_targets(
    components: Mapping[Component, ComponentArtifactEvidence],
) -> tuple[dict[Component, ComponentArtifactEvidence], bool]:
    """Drop producer-duplicated indexes after proving exact fallback parity."""

    output: dict[Component, ComponentArtifactEvidence] = {}
    compacted_any = False
    for component in Component:
        evidence = components[component]
        if not evidence.complete:
            output[component] = evidence
            continue
        mutable_targets = {
            code: tuple(path for path in paths if _mutable_writer_target(path))
            for code, paths in evidence.instrument_file_targets.items()
        }
        component_compacted = False

        def compact_rule(rule: MutationRuleEvidence) -> MutationRuleEvidence:
            nonlocal component_compacted, compacted_any
            if rule.writer_target_policy != "artifact_file_instrument_index_v1":
                return rule
            for code, targets in rule.writer_targets_by_instrument.items():
                if not set(targets).issubset(mutable_targets.get(code, ())):
                    raise ComponentArtifactManifestError("artifact-index mutation targets differ from mutable files")
            if not rule.writer_targets_by_instrument:
                return rule
            component_compacted = True
            compacted_any = True
            return replace(rule, writer_targets_by_instrument={})

        append_rules = tuple(compact_rule(rule) for rule in evidence.append_rules)
        pit_rule = compact_rule(evidence.pit_mutation_rule) if evidence.pit_mutation_rule is not None else None
        adj = evidence.adj_series
        if adj is not None and adj.writer_target_policy == "artifact_file_instrument_index_v1":
            for code, targets in adj.writer_targets_by_code.items():
                if not set(targets).issubset(mutable_targets.get(code, ())):
                    raise ComponentArtifactManifestError("artifact-index adj targets differ from mutable files")
            if adj.writer_targets_by_code:
                adj = replace(adj, writer_targets_by_code={})
                component_compacted = True
                compacted_any = True
        output[component] = (
            replace(
                evidence,
                append_rules=append_rules,
                pit_mutation_rule=pit_rule,
                adj_series=adj,
            )
            if component_compacted
            else evidence
        )
    return output, compacted_any


def _seal_v2_component_index(
    cas: CASStore,
    *,
    profile: str,
    candidate_identity: str,
    artifact_root: str,
    evidence: ComponentArtifactEvidence,
) -> Mapping[str, Any]:
    assert evidence.component_manifest_root is not None
    assert evidence.filesystem_tree_merkle is not None
    assert evidence.component_root_relative_path is not None
    sections: dict[str, list[Mapping[str, Any]]] = {}
    section_row_counts: dict[str, int] = {}
    for section in _V2_SECTIONS:
        refs, row_count = _seal_v2_section(
            cas,
            profile=profile,
            candidate_identity=candidate_identity,
            artifact_root=artifact_root,
            component=evidence.component,
            section=section,
            values=_iter_v2_section_values(evidence, section),
        )
        sections[section] = refs
        section_row_counts[section] = row_count

    index_body = {
        "schema_version": COMPONENT_ARTIFACT_COMPONENT_INDEX_SCHEMA_V2,
        "profile": profile,
        "candidate_identity": candidate_identity,
        "artifact_root": artifact_root,
        "component": evidence.component.value,
        "core": _v2_component_core(evidence),
        "sections": sections,
        "section_row_counts": section_row_counts,
        "component_manifest_root": evidence.component_manifest_root,
        "filesystem_tree_merkle": evidence.filesystem_tree_merkle,
        "component_root_relative_path": evidence.component_root_relative_path,
        "safety": dict(_ZERO_SAFETY),
    }
    index_root = digest_named_fields(COMPONENT_ARTIFACT_COMPONENT_INDEX_SCHEMA_V2, index_body)
    index_ref = cas.put_json({**index_body, "component_index_root": index_root})
    if index_ref.size > MAX_COMPONENT_MANIFEST_BYTES:
        raise ComponentArtifactManifestError(f"component artifact v2 index exceeds bound: {evidence.component.value}")
    return {
        "status": "COMPLETE",
        "component_index_ref": index_ref.as_dict(),
        "component_index_root": index_root,
        "component_manifest_root": evidence.component_manifest_root,
        "filesystem_tree_merkle": evidence.filesystem_tree_merkle,
        "component_root_relative_path": evidence.component_root_relative_path,
    }


def _v2_component_core(
    evidence: ComponentArtifactEvidence,
) -> Mapping[str, Any]:
    append_rules = []
    for rule in evidence.append_rules:
        value = rule.as_dict()
        value.pop("writer_targets_by_instrument")
        append_rules.append(value)
    pit_rule = None
    if evidence.pit_mutation_rule is not None:
        pit_rule = evidence.pit_mutation_rule.as_dict()
        pit_rule.pop("writer_targets_by_instrument")
    adj = None
    if evidence.adj_series is not None:
        adj = evidence.adj_series.as_dict()
        for key in (
            "qfq_denominator_by_code",
            "ordered_adj_digest_by_code",
            "adj_row_count_by_code",
            "monthly_ordered_adj_by_code",
            "writer_targets_by_code",
        ):
            adj.pop(key)
    return {
        "status": "COMPLETE",
        "reason_code": None,
        "component": evidence.component.value,
        "component_root_relative_path": evidence.component_root_relative_path,
        "append_rules": append_rules,
        "pit_mutation_rule": pit_rule,
        "adj_series": adj,
        "component_identity": evidence.component_identity,
        "file_identity": evidence.file_identity,
        "component_manifest_root": evidence.component_manifest_root,
        "filesystem_tree_merkle": evidence.filesystem_tree_merkle,
    }


def _iter_v2_section_values(
    evidence: ComponentArtifactEvidence,
    section: str,
) -> Iterator[Mapping[str, Any]]:
    if section == "source_partitions":
        for item in evidence.source_partitions:
            yield item.as_dict()
        return
    if section == "artifact_partition_headers":
        for partition in evidence.artifact_partitions:
            value = partition.as_dict()
            value.pop("files")
            value.pop("instruments")
            yield value
        return
    if section == "artifact_files":
        for partition in evidence.artifact_partitions:
            for file in partition.files:
                yield {
                    "partition_key": partition.partition_key,
                    "file": file.as_dict(),
                }
        return
    if section == "artifact_instruments":
        for partition in evidence.artifact_partitions:
            for instrument in partition.instruments:
                yield {
                    "partition_key": partition.partition_key,
                    "instrument": instrument,
                }
        return
    if section == "mutation_rule_targets":
        scoped_rules = [("append", rule) for rule in evidence.append_rules]
        if evidence.pit_mutation_rule is not None:
            scoped_rules.append(("pit", evidence.pit_mutation_rule))
        for scope, rule in scoped_rules:
            for instrument in sorted(rule.writer_targets_by_instrument):
                yield {
                    "scope": scope,
                    "rule_id": rule.rule_id,
                    "instrument": instrument,
                    "targets": list(rule.writer_targets_by_instrument[instrument]),
                }
        return
    if section == "pit_authority":
        digests = evidence.pit_span_digest_by_code or {}
        for instrument in evidence.pit_instruments:
            yield {
                "instrument": instrument,
                "span_digest": digests[instrument],
            }
        return
    if section == "adj_authority":
        adj = evidence.adj_series
        if adj is None:
            return
        for instrument in sorted(adj.qfq_denominator_by_code):
            has_monthly = instrument in adj.monthly_ordered_adj_by_code
            yield {
                "instrument": instrument,
                "qfq_denominator": adj.qfq_denominator_by_code[instrument],
                "ordered_adj_digest": adj.ordered_adj_digest_by_code[instrument],
                "adj_row_count": adj.adj_row_count_by_code[instrument],
                "has_monthly_authority": has_monthly,
                "monthly_ordered_adj": {
                    month: dict(leaf) for month, leaf in adj.monthly_ordered_adj_by_code.get(instrument, {}).items()
                },
                "writer_targets": list(adj.writer_targets_by_code.get(instrument, ())),
            }
        return
    raise ComponentArtifactManifestError(f"component artifact v2 section is unsupported: {section}")


def _seal_v2_section(
    cas: CASStore,
    *,
    profile: str,
    candidate_identity: str,
    artifact_root: str,
    component: Component,
    section: str,
    values: Iterator[Mapping[str, Any]],
) -> tuple[list[Mapping[str, Any]], int]:
    refs: list[Mapping[str, Any]] = []
    rows: list[Mapping[str, Any]] = []
    row_bytes = 0
    total_rows = 0

    def payload(candidate_rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
        body = {
            "schema_version": COMPONENT_ARTIFACT_SECTION_SHARD_SCHEMA_V2,
            "profile": profile,
            "candidate_identity": candidate_identity,
            "artifact_root": artifact_root,
            "component": component.value,
            "section": section,
            "ordinal": len(refs),
            "first_key": candidate_rows[0]["key"],
            "last_key": candidate_rows[-1]["key"],
            "row_count": len(candidate_rows),
            "rows": list(candidate_rows),
            "safety": dict(_ZERO_SAFETY),
        }
        root = digest_named_fields(COMPONENT_ARTIFACT_SECTION_SHARD_SCHEMA_V2, body)
        return {**body, "shard_root": root}

    def persist(candidate_rows: Sequence[Mapping[str, Any]]) -> None:
        sealed = payload(candidate_rows)
        serialized_size = len(canonical_json_bytes(sealed))
        if serialized_size > TARGET_COMPONENT_SECTION_SHARD_BYTES:
            if len(candidate_rows) == 1:
                raise ComponentArtifactManifestError("component artifact v2 section row exceeds target bound")
            midpoint = len(candidate_rows) // 2
            persist(candidate_rows[:midpoint])
            persist(candidate_rows[midpoint:])
            return
        reference = cas.put_json(sealed)
        if reference.size != serialized_size or reference.size > TARGET_COMPONENT_SECTION_SHARD_BYTES:
            raise ComponentArtifactManifestError("component artifact v2 section shard exceeds target bound")
        refs.append(
            {
                "ordinal": sealed["ordinal"],
                "first_key": sealed["first_key"],
                "last_key": sealed["last_key"],
                "row_count": sealed["row_count"],
                "section_shard_ref": reference.as_dict(),
                "shard_root": sealed["shard_root"],
            }
        )

    def flush() -> None:
        nonlocal row_bytes, rows
        if not rows:
            return
        persist(rows)
        rows = []
        row_bytes = 0

    for value in values:
        row = {
            "key": f"{total_rows:012d}",
            "value": dict(value),
        }
        serialized_row_size = len(canonical_json_bytes(row))
        if rows and (
            len(rows) >= MAX_COMPONENT_SECTION_ROWS
            or row_bytes + serialized_row_size > TARGET_COMPONENT_SECTION_SHARD_BYTES - (64 * 1024)
        ):
            flush()
        rows.append(row)
        row_bytes += serialized_row_size
        total_rows += 1
    flush()
    return refs, total_rows


def _load_component_artifact_manifest_v2(
    cas: CASStore,
    verified: CASRef,
    value: Mapping[str, Any],
) -> ComponentArtifactManifest:
    required = {
        "schema_version",
        "semantic_schema_version",
        "semantic_manifest_root",
        "profile",
        "scope",
        "cutoff",
        "candidate_identity",
        "artifact_root",
        "semantic_profile_digest",
        "producer_fingerprint",
        "artifact_fingerprint",
        "validation_fingerprint",
        "source_content_root",
        "artifact_ready_content_root",
        "pit_snapshot_digest",
        "components",
        "safety",
        "manifest_root",
    }
    allowed = required | {"candidate_metadata"}
    if (
        set(value).difference(allowed)
        or not required.issubset(value)
        or value.get("semantic_schema_version") != COMPONENT_ARTIFACT_MANIFEST_SCHEMA
        or value.get("safety") != _ZERO_SAFETY
    ):
        raise ComponentArtifactManifestError("component artifact v2 top index fields differ")
    top_body = dict(value)
    declared_root = top_body.pop("manifest_root", None)
    top_root = digest_named_fields(COMPONENT_ARTIFACT_MANIFEST_STORAGE_SCHEMA_V2, top_body)
    if declared_root != top_root:
        raise ComponentArtifactManifestError("component artifact v2 top index root differs")
    raw_index = value.get("components")
    if not isinstance(raw_index, Mapping) or set(raw_index) != {item.value for item in Component}:
        raise ComponentArtifactManifestError("component artifact v2 component index is incomplete")

    expanded_components: dict[str, Mapping[str, Any]] = {}
    for component in Component:
        entry = raw_index[component.value]
        if not isinstance(entry, Mapping):
            raise ComponentArtifactManifestError("component artifact v2 component entry is invalid")
        if entry.get("status") == "UNAVAILABLE":
            if set(entry) != {"status", "reason_code"}:
                raise ComponentArtifactManifestError("component artifact v2 unavailable entry differs")
            expanded_components[component.value] = dict(entry)
            continue
        expanded_components[component.value] = _load_v2_component_index(
            cas,
            top=value,
            component=component,
            entry=entry,
        )

    semantic = {
        "schema_version": COMPONENT_ARTIFACT_MANIFEST_SCHEMA,
        **{
            key: value[key]
            for key in (
                "profile",
                "scope",
                "cutoff",
                "candidate_identity",
                "artifact_root",
                "semantic_profile_digest",
                "producer_fingerprint",
                "artifact_fingerprint",
                "validation_fingerprint",
                "source_content_root",
                "artifact_ready_content_root",
                "pit_snapshot_digest",
                "safety",
            )
        },
        "components": expanded_components,
    }
    if "candidate_metadata" in value:
        semantic["candidate_metadata"] = value["candidate_metadata"]
    preliminary, _ = _normalize_manifest(semantic, require_identities=False)
    semantic["manifest_root"] = preliminary["manifest_root"]
    normalized, components = _normalize_manifest(semantic, require_identities=True)
    if normalized["manifest_root"] != value.get("semantic_manifest_root"):
        raise ComponentArtifactManifestError("component artifact v2 semantic root differs")
    return ComponentArtifactManifest(
        reference=verified,
        profile=str(normalized["profile"]),
        scope=str(normalized["scope"]),
        cutoff=date.fromisoformat(str(normalized["cutoff"])),
        candidate_identity=str(normalized["candidate_identity"]),
        artifact_root=str(normalized["artifact_root"]),
        semantic_profile_digest=str(normalized["semantic_profile_digest"]),
        producer_fingerprint=str(normalized["producer_fingerprint"]),
        artifact_fingerprint=str(normalized["artifact_fingerprint"]),
        validation_fingerprint=str(normalized["validation_fingerprint"]),
        source_content_root=str(normalized["source_content_root"]),
        artifact_ready_content_root=str(normalized["artifact_ready_content_root"]),
        pit_snapshot_digest=str(normalized["pit_snapshot_digest"]),
        candidate_metadata=dict(normalized.get("candidate_metadata") or {}),
        components=components,
        manifest_root=top_root,
    )


def _load_v2_component_index(
    cas: CASStore,
    *,
    top: Mapping[str, Any],
    component: Component,
    entry: Mapping[str, Any],
) -> Mapping[str, Any]:
    entry_fields = {
        "status",
        "component_index_ref",
        "component_index_root",
        "component_manifest_root",
        "filesystem_tree_merkle",
        "component_root_relative_path",
    }
    if set(entry) != entry_fields or entry.get("status") != "COMPLETE":
        raise ComponentArtifactManifestError("component artifact v2 component index entry differs")
    index_ref = _complete_ref(cas, entry["component_index_ref"])
    index = cas.get_json_bounded(index_ref, max_bytes=MAX_COMPONENT_MANIFEST_BYTES)
    if not isinstance(index, Mapping):
        raise ComponentArtifactManifestError("component artifact v2 component index is invalid")
    fields = {
        "schema_version",
        "profile",
        "candidate_identity",
        "artifact_root",
        "component",
        "core",
        "sections",
        "section_row_counts",
        "component_manifest_root",
        "filesystem_tree_merkle",
        "component_root_relative_path",
        "safety",
        "component_index_root",
    }
    body = dict(index)
    declared_root = body.pop("component_index_root", None)
    expected_root = digest_named_fields(COMPONENT_ARTIFACT_COMPONENT_INDEX_SCHEMA_V2, body)
    if (
        set(index) != fields
        or index.get("schema_version") != COMPONENT_ARTIFACT_COMPONENT_INDEX_SCHEMA_V2
        or index.get("profile") != top["profile"]
        or index.get("candidate_identity") != top["candidate_identity"]
        or index.get("artifact_root") != top["artifact_root"]
        or index.get("component") != component.value
        or index.get("safety") != _ZERO_SAFETY
        or declared_root != expected_root
        or entry.get("component_index_root") != expected_root
        or entry.get("component_manifest_root") != index.get("component_manifest_root")
        or entry.get("filesystem_tree_merkle") != index.get("filesystem_tree_merkle")
        or entry.get("component_root_relative_path") != index.get("component_root_relative_path")
    ):
        raise ComponentArtifactManifestError("component artifact v2 component index identity differs")
    sections = index.get("sections")
    counts = index.get("section_row_counts")
    if (
        not isinstance(sections, Mapping)
        or set(sections) != set(_V2_SECTIONS)
        or not isinstance(counts, Mapping)
        or set(counts) != set(_V2_SECTIONS)
        or any(type(counts[name]) is not int or counts[name] < 0 for name in _V2_SECTIONS)
    ):
        raise ComponentArtifactManifestError("component artifact v2 section index differs")
    core = index.get("core")
    if not isinstance(core, Mapping):
        raise ComponentArtifactManifestError("component artifact v2 component core is invalid")
    expanded = _expand_v2_component(
        cas,
        top=top,
        component=component,
        core=core,
        sections=sections,
        counts=counts,
    )
    if (
        expanded.get("component_manifest_root") != index.get("component_manifest_root")
        or expanded.get("filesystem_tree_merkle") != index.get("filesystem_tree_merkle")
        or expanded.get("component_root_relative_path") != index.get("component_root_relative_path")
    ):
        raise ComponentArtifactManifestError("component artifact v2 component semantic root differs")
    return expanded


def _expand_v2_component(
    cas: CASStore,
    *,
    top: Mapping[str, Any],
    component: Component,
    core: Mapping[str, Any],
    sections: Mapping[str, Any],
    counts: Mapping[str, Any],
) -> Mapping[str, Any]:
    core_fields = {
        "status",
        "reason_code",
        "component",
        "component_root_relative_path",
        "append_rules",
        "pit_mutation_rule",
        "adj_series",
        "component_identity",
        "file_identity",
        "component_manifest_root",
        "filesystem_tree_merkle",
    }
    if (
        set(core) != core_fields
        or core.get("status") != "COMPLETE"
        or core.get("reason_code") is not None
        or core.get("component") != component.value
    ):
        raise ComponentArtifactManifestError("component artifact v2 component core fields differ")

    def section_values(name: str) -> Iterator[Mapping[str, Any]]:
        return _iter_loaded_v2_section_values(
            cas,
            top=top,
            component=component,
            section=name,
            raw_refs=sections[name],
            expected_count=counts[name],
        )

    source_partitions = [dict(value) for value in section_values("source_partitions")]
    partition_headers: dict[str, dict[str, Any]] = {}
    header_fields = {
        "partition_key",
        "source_partition_identities",
        "dependency_edges",
        "start",
        "end",
        "partition_identity",
    }
    for value in section_values("artifact_partition_headers"):
        if set(value) != header_fields:
            raise ComponentArtifactManifestError("component artifact v2 partition header fields differ")
        key = str(value["partition_key"])
        if key in partition_headers:
            raise ComponentArtifactManifestError("component artifact v2 partition header is duplicated")
        partition_headers[key] = dict(value)

    files_by_partition: dict[str, list[Mapping[str, Any]]] = {key: [] for key in partition_headers}
    seen_files: set[tuple[str, str]] = set()
    for value in section_values("artifact_files"):
        if set(value) != {"partition_key", "file"} or not isinstance(value.get("file"), Mapping):
            raise ComponentArtifactManifestError("component artifact v2 file row fields differ")
        key = str(value["partition_key"])
        if key not in files_by_partition:
            raise ComponentArtifactManifestError("component artifact v2 file references unknown partition")
        relative_path = str(value["file"].get("relative_path", ""))
        identity = (key, relative_path)
        if identity in seen_files:
            raise ComponentArtifactManifestError("component artifact v2 file row is duplicated")
        seen_files.add(identity)
        files_by_partition[key].append(dict(value["file"]))

    instruments_by_partition: dict[str, list[str]] = {key: [] for key in partition_headers}
    seen_instruments: set[tuple[str, str]] = set()
    for value in section_values("artifact_instruments"):
        if set(value) != {"partition_key", "instrument"}:
            raise ComponentArtifactManifestError("component artifact v2 instrument row fields differ")
        key = str(value["partition_key"])
        instrument = str(value["instrument"])
        if key not in instruments_by_partition or (key, instrument) in seen_instruments:
            raise ComponentArtifactManifestError("component artifact v2 instrument partition differs")
        seen_instruments.add((key, instrument))
        instruments_by_partition[key].append(instrument)

    rule_fields = {
        "rule_id",
        "datasets",
        "replace_existing_targets",
        "create_new_targets",
        "create_target_templates",
        "writer_target_policy",
        "dependency_edges",
        "rule_identity",
    }
    raw_append = core.get("append_rules")
    if not isinstance(raw_append, list) or not all(
        isinstance(item, Mapping) and set(item) == rule_fields for item in raw_append
    ):
        raise ComponentArtifactManifestError("component artifact v2 append-rule core differs")
    raw_pit = core.get("pit_mutation_rule")
    if raw_pit is not None and (not isinstance(raw_pit, Mapping) or set(raw_pit) != rule_fields):
        raise ComponentArtifactManifestError("component artifact v2 PIT-rule core differs")
    rule_targets: dict[tuple[str, str], dict[str, list[str]]] = {}
    known_rule_keys = {("append", str(rule["rule_id"])) for rule in raw_append}
    if raw_pit is not None:
        known_rule_keys.add(("pit", str(raw_pit["rule_id"])))
    for value in section_values("mutation_rule_targets"):
        if set(value) != {"scope", "rule_id", "instrument", "targets"}:
            raise ComponentArtifactManifestError("component artifact v2 mutation-target row fields differ")
        rule_key = (str(value["scope"]), str(value["rule_id"]))
        instrument = str(value["instrument"])
        if rule_key not in known_rule_keys:
            raise ComponentArtifactManifestError("component artifact v2 mutation target references unknown rule")
        by_code = rule_targets.setdefault(rule_key, {})
        if instrument in by_code or not isinstance(value["targets"], list):
            raise ComponentArtifactManifestError("component artifact v2 mutation target is duplicated")
        by_code[instrument] = list(value["targets"])

    append_rules = []
    for rule in raw_append:
        rebuilt = dict(rule)
        rebuilt["writer_targets_by_instrument"] = rule_targets.get(("append", str(rule["rule_id"])), {})
        append_rules.append(rebuilt)
    pit_rule = None
    if raw_pit is not None:
        pit_rule = dict(raw_pit)
        pit_rule["writer_targets_by_instrument"] = rule_targets.get(("pit", str(raw_pit["rule_id"])), {})

    pit_instruments: list[str] = []
    pit_digests: dict[str, Any] = {}
    for value in section_values("pit_authority"):
        if set(value) != {"instrument", "span_digest"}:
            raise ComponentArtifactManifestError("component artifact v2 PIT authority row fields differ")
        instrument = str(value["instrument"])
        if instrument in pit_digests:
            raise ComponentArtifactManifestError("component artifact v2 PIT authority is duplicated")
        pit_instruments.append(instrument)
        pit_digests[instrument] = value["span_digest"]

    adj_core = core.get("adj_series")
    adj_rows: dict[str, Mapping[str, Any]] = {}
    for value in section_values("adj_authority"):
        expected = {
            "instrument",
            "qfq_denominator",
            "ordered_adj_digest",
            "adj_row_count",
            "has_monthly_authority",
            "monthly_ordered_adj",
            "writer_targets",
        }
        if set(value) != expected:
            raise ComponentArtifactManifestError("component artifact v2 adj authority row fields differ")
        instrument = str(value["instrument"])
        if instrument in adj_rows:
            raise ComponentArtifactManifestError("component artifact v2 adj authority is duplicated")
        adj_rows[instrument] = value
    adj = None
    if adj_core is None:
        if adj_rows:
            raise ComponentArtifactManifestError("component artifact v2 adj rows lack core")
    else:
        adj_core_fields = {
            "complete",
            "shared_writer_targets",
            "writer_target_policy",
            "evidence_identity",
        }
        if not isinstance(adj_core, Mapping) or set(adj_core) != adj_core_fields:
            raise ComponentArtifactManifestError("component artifact v2 adj core fields differ")
        adj = dict(adj_core)
        adj["qfq_denominator_by_code"] = {code: row["qfq_denominator"] for code, row in adj_rows.items()}
        adj["ordered_adj_digest_by_code"] = {code: row["ordered_adj_digest"] for code, row in adj_rows.items()}
        adj["adj_row_count_by_code"] = {code: row["adj_row_count"] for code, row in adj_rows.items()}
        adj["monthly_ordered_adj_by_code"] = {
            code: row["monthly_ordered_adj"] for code, row in adj_rows.items() if row["has_monthly_authority"] is True
        }
        if any(
            type(row["has_monthly_authority"]) is not bool
            or not isinstance(row["monthly_ordered_adj"], Mapping)
            or not isinstance(row["writer_targets"], list)
            for row in adj_rows.values()
        ):
            raise ComponentArtifactManifestError("component artifact v2 adj authority value differs")
        adj["writer_targets_by_code"] = {
            code: list(row["writer_targets"]) for code, row in adj_rows.items() if row["writer_targets"]
        }

    artifact_partitions = []
    for key, header in partition_headers.items():
        artifact_partitions.append(
            {
                **header,
                "instruments": instruments_by_partition[key],
                "files": files_by_partition[key],
            }
        )
    return {
        **dict(core),
        "source_partitions": source_partitions,
        "artifact_partitions": artifact_partitions,
        "append_rules": append_rules,
        "pit_mutation_rule": pit_rule,
        "pit_instruments": pit_instruments,
        "pit_span_digest_by_code": pit_digests,
        "adj_series": adj,
    }


def _iter_loaded_v2_section_values(
    cas: CASStore,
    *,
    top: Mapping[str, Any],
    component: Component,
    section: str,
    raw_refs: Any,
    expected_count: int,
) -> Iterator[Mapping[str, Any]]:
    if not isinstance(raw_refs, list):
        raise ComponentArtifactManifestError("component artifact v2 section refs must be a list")
    total_rows = 0
    previous_last: str | None = None
    ref_fields = {
        "ordinal",
        "first_key",
        "last_key",
        "row_count",
        "section_shard_ref",
        "shard_root",
    }
    shard_fields = {
        "schema_version",
        "profile",
        "candidate_identity",
        "artifact_root",
        "component",
        "section",
        "ordinal",
        "first_key",
        "last_key",
        "row_count",
        "rows",
        "safety",
        "shard_root",
    }
    for ordinal, raw_entry in enumerate(raw_refs):
        if (
            not isinstance(raw_entry, Mapping)
            or set(raw_entry) != ref_fields
            or type(raw_entry.get("ordinal")) is not int
            or type(raw_entry.get("row_count")) is not int
            or not isinstance(raw_entry.get("first_key"), str)
            or not isinstance(raw_entry.get("last_key"), str)
        ):
            raise ComponentArtifactManifestError("component artifact v2 section ref fields differ")
        shard_ref = _complete_ref(cas, raw_entry["section_shard_ref"])
        if shard_ref.size > TARGET_COMPONENT_SECTION_SHARD_BYTES:
            raise ComponentArtifactManifestError("component artifact v2 section shard exceeds target bound")
        shard = cas.get_json_bounded(shard_ref, max_bytes=MAX_COMPONENT_MANIFEST_BYTES)
        if not isinstance(shard, Mapping):
            raise ComponentArtifactManifestError("component artifact v2 section shard is invalid")
        body = dict(shard)
        declared_root = body.pop("shard_root", None)
        expected_root = digest_named_fields(COMPONENT_ARTIFACT_SECTION_SHARD_SCHEMA_V2, body)
        rows = shard.get("rows")
        if (
            set(shard) != shard_fields
            or shard.get("schema_version") != COMPONENT_ARTIFACT_SECTION_SHARD_SCHEMA_V2
            or shard.get("profile") != top["profile"]
            or shard.get("candidate_identity") != top["candidate_identity"]
            or shard.get("artifact_root") != top["artifact_root"]
            or shard.get("component") != component.value
            or shard.get("section") != section
            or type(shard.get("ordinal")) is not int
            or shard.get("ordinal") != ordinal
            or raw_entry.get("ordinal") != ordinal
            or not isinstance(shard.get("first_key"), str)
            or not isinstance(shard.get("last_key"), str)
            or shard.get("safety") != _ZERO_SAFETY
            or declared_root != expected_root
            or raw_entry.get("shard_root") != expected_root
            or not isinstance(rows, list)
            or not rows
            or type(shard.get("row_count")) is not int
            or shard.get("row_count") != len(rows)
            or raw_entry.get("row_count") != len(rows)
            or len(rows) > MAX_COMPONENT_SECTION_ROWS
        ):
            raise ComponentArtifactManifestError("component artifact v2 section shard identity differs")
        first_key = f"{total_rows:012d}"
        last_key = f"{total_rows + len(rows) - 1:012d}"
        if (
            shard.get("first_key") != first_key
            or shard.get("last_key") != last_key
            or raw_entry.get("first_key") != first_key
            or raw_entry.get("last_key") != last_key
            or (previous_last is not None and first_key <= previous_last)
        ):
            raise ComponentArtifactManifestError("component artifact v2 section shard boundary differs")
        for offset, row in enumerate(rows):
            expected_key = f"{total_rows + offset:012d}"
            if (
                not isinstance(row, Mapping)
                or set(row) != {"key", "value"}
                or row.get("key") != expected_key
                or not isinstance(row.get("value"), Mapping)
            ):
                raise ComponentArtifactManifestError("component artifact v2 section row order differs")
            yield row["value"]
        total_rows += len(rows)
        previous_last = last_key
    if total_rows != expected_count or (expected_count == 0 and raw_refs):
        raise ComponentArtifactManifestError("component artifact v2 section row count differs")


def _complete_ref(cas: CASStore, value: Any) -> CASRef:
    try:
        supplied = CASRef.from_value(value)
    except Exception as exc:
        raise ComponentArtifactManifestError("component shard CAS reference is invalid") from exc
    if supplied.size < 0:
        raise ComponentArtifactManifestError("component shard CAS reference is incomplete")
    verified = cas.verify(supplied)
    if supplied.relative_path != verified.relative_path:
        raise ComponentArtifactManifestError("component shard CAS path is non-canonical")
    return verified


def normalize_current_source_partition(
    value: Mapping[str, Any],
) -> SourcePartitionEvidence:
    """Validate one live artifact-ready descriptor using the baseline schema."""

    allowed = {
        "identity",
        "dataset",
        "partition_key",
        "row_count",
        "content_digest",
        "schema_digest",
        "source_table_schema_digest",
        "source_code_membership_digest",
        "min_key",
        "max_key",
        "monthly_content_leaves",
        "affected_instruments",
    }
    projected = {key: value.get(key) for key in allowed if key in value}
    projected["identity"] = value.get("identity", f"{value.get('dataset')}:{value.get('partition_key')}")
    return _normalize_source_partition(projected, require_identity=False)


def _normalize_manifest(
    value: Mapping[str, Any],
    *,
    require_identities: bool,
) -> tuple[dict[str, Any], dict[Component, ComponentArtifactEvidence]]:
    required = {
        "profile",
        "scope",
        "cutoff",
        "candidate_identity",
        "artifact_root",
        "semantic_profile_digest",
        "producer_fingerprint",
        "artifact_fingerprint",
        "validation_fingerprint",
        "source_content_root",
        "artifact_ready_content_root",
        "pit_snapshot_digest",
        "components",
    }
    allowed = required | {
        "schema_version",
        "manifest_root",
        "safety",
        "candidate_metadata",
    }
    if set(value).difference(allowed) or not required.issubset(value):
        raise ComponentArtifactManifestError("component artifact top-level fields differ")
    if value.get("schema_version", COMPONENT_ARTIFACT_MANIFEST_SCHEMA) != COMPONENT_ARTIFACT_MANIFEST_SCHEMA:
        raise ComponentArtifactManifestError("component artifact schema differs")
    profile = str(value["profile"]).strip()
    scope = str(value["scope"]).strip()
    if not profile or scope not in {"sample", "full"}:
        raise ComponentArtifactManifestError("component artifact profile/scope is invalid")
    try:
        cutoff = date.fromisoformat(str(value["cutoff"]))
    except ValueError as exc:
        raise ComponentArtifactManifestError("component artifact cutoff is invalid") from exc
    digest_fields = (
        "candidate_identity",
        "artifact_root",
        "semantic_profile_digest",
        "producer_fingerprint",
        "artifact_fingerprint",
        "validation_fingerprint",
        "source_content_root",
        "artifact_ready_content_root",
        "pit_snapshot_digest",
    )
    digests = {field: ensure_sha256(str(value[field]), field=field) for field in digest_fields}
    raw_components = value["components"]
    if not isinstance(raw_components, Mapping) or set(raw_components) != {item.value for item in Component}:
        raise ComponentArtifactManifestError("component artifact manifest must name every required component")
    components: dict[Component, ComponentArtifactEvidence] = {}
    for component in Component:
        raw = raw_components[component.value]
        if not isinstance(raw, Mapping):
            raise ComponentArtifactManifestError("component artifact entry is invalid")
        components[component] = _normalize_component(component, raw, require_identities=require_identities)
    roots = [value.component_root_relative_path for value in components.values() if value.complete]
    if len(roots) != len(set(roots)):
        raise ComponentArtifactManifestError("component artifact roots are duplicated")
    metadata = _normalize_candidate_metadata(value.get("candidate_metadata"))
    body = {
        "schema_version": COMPONENT_ARTIFACT_MANIFEST_SCHEMA,
        "profile": profile,
        "scope": scope,
        "cutoff": cutoff.isoformat(),
        **digests,
        "components": {component.value: components[component].as_dict() for component in Component},
        "safety": dict(_ZERO_SAFETY),
    }
    if metadata:
        body["candidate_metadata"] = metadata
    manifest_root = digest_named_fields(COMPONENT_ARTIFACT_MANIFEST_SCHEMA, body)
    if require_identities:
        if value.get("safety") != _ZERO_SAFETY or value.get("manifest_root") != manifest_root:
            raise ComponentArtifactManifestError("component artifact manifest root/safety differs")
    return {**body, "manifest_root": manifest_root}, components


def _normalize_component(
    component: Component,
    value: Mapping[str, Any],
    *,
    require_identities: bool,
) -> ComponentArtifactEvidence:
    status = str(value.get("status", "")).upper()
    if status == "UNAVAILABLE":
        if set(value) != {"status", "reason_code"}:
            raise ComponentArtifactManifestError("unavailable component evidence has unexpected fields")
        reason = str(value.get("reason_code", ""))
        if _SAFE_REASON.fullmatch(reason) is None:
            raise ComponentArtifactManifestError("component unavailable reason is invalid")
        return ComponentArtifactEvidence(component, status, reason)
    required = {
        "status",
        "component",
        "component_root_relative_path",
        "source_partitions",
        "artifact_partitions",
        "append_rules",
        "pit_mutation_rule",
        "pit_instruments",
        "pit_span_digest_by_code",
        "adj_series",
    }
    derived = {
        "reason_code",
        "component_identity",
        "file_identity",
        "component_manifest_root",
        "filesystem_tree_merkle",
    }
    if status != "COMPLETE" or set(value).difference(required | derived) or not required.issubset(value):
        raise ComponentArtifactManifestError("complete component evidence fields differ")
    if value.get("component") != component.value or value.get("reason_code") not in {None, ""}:
        raise ComponentArtifactManifestError("component evidence identity differs")
    component_root = normalize_root_relative_path(str(value["component_root_relative_path"]))
    source = tuple(
        _normalize_source_partition(item, require_identity=require_identities)
        for item in _mapping_sequence(value["source_partitions"], field="source_partitions")
    )
    source = tuple(sorted(source, key=lambda item: item.identity))
    source_ids = [item.identity for item in source]
    if not source or len(source_ids) != len(set(source_ids)):
        raise ComponentArtifactManifestError("component source identities are empty/duplicated")
    artifacts = tuple(
        _normalize_artifact_partition(component, item, source_ids=source_ids, require_identity=require_identities)
        for item in _mapping_sequence(value["artifact_partitions"], field="artifact_partitions")
    )
    artifacts = tuple(sorted(artifacts, key=lambda item: item.partition_key))
    artifact_keys = [item.partition_key for item in artifacts]
    if not artifacts or len(artifact_keys) != len(set(artifact_keys)):
        raise ComponentArtifactManifestError("artifact partition identities are empty/duplicated")
    paths = [file.relative_path for item in artifacts for file in item.files]
    if len(paths) != len(set(paths)):
        raise ComponentArtifactManifestError("artifact file path is assigned more than once")
    artifact_file_targets = _artifact_instrument_file_targets(artifacts)
    mapped_source_ids = {identity for item in artifacts for identity in item.source_partition_identities}
    if mapped_source_ids != set(source_ids):
        raise ComponentArtifactManifestError("component source-to-artifact dependency graph is incomplete")
    append_rules = tuple(
        _normalize_rule(item, known_files=set(paths), require_identity=require_identities)
        for item in _mapping_sequence(value["append_rules"], field="append_rules")
    )
    append_rules = tuple(sorted(append_rules, key=lambda item: item.rule_id))
    if len({item.rule_id for item in append_rules}) != len(append_rules):
        raise ComponentArtifactManifestError("component append rule is duplicated")
    pit_raw = value["pit_mutation_rule"]
    pit_rule = (
        None
        if pit_raw is None
        else _normalize_rule(
            _mapping(pit_raw, field="pit_mutation_rule"),
            known_files=set(paths),
            require_identity=require_identities,
        )
    )
    append_rules = tuple(
        replace(rule, _artifact_file_targets=artifact_file_targets)
        if rule.writer_target_policy == "artifact_file_instrument_index_v1"
        else rule
        for rule in append_rules
    )
    if pit_rule is not None and pit_rule.writer_target_policy == "artifact_file_instrument_index_v1":
        pit_rule = replace(pit_rule, _artifact_file_targets=artifact_file_targets)
    pit_instruments = tuple(
        sorted(
            {_stock_instrument(item) for item in _string_sequence(value["pit_instruments"], field="pit_instruments")}
        )
    )
    pit_span_digest_by_code = _digest_map(value["pit_span_digest_by_code"], field="PIT span digest")
    if set(pit_span_digest_by_code) != set(pit_instruments):
        raise ComponentArtifactManifestError("PIT instruments and per-code span digests differ")
    source_affected = {code for item in source for code in item.affected_instruments}
    if source_affected.difference(pit_instruments):
        raise ComponentArtifactManifestError("source affected instruments are outside the frozen PIT universe")
    adj_raw = value["adj_series"]
    adj = (
        None
        if adj_raw is None
        else _normalize_adj_series(
            _mapping(adj_raw, field="adj_series"),
            known_files=set(paths),
            require_identity=require_identities,
        )
    )
    file_identity = merkle_root_from_named_digests(
        "dataset_release_component_files_v1",
        ((file.relative_path, file.file_identity) for item in artifacts for file in item.files),
    )
    filesystem_tree_merkle = _filesystem_tree_merkle(artifacts)
    component_body = {
        "component": component.value,
        "component_root_relative_path": component_root,
        "source_partitions": [item.as_dict() for item in source],
        "artifact_partitions": [item.as_dict() for item in artifacts],
        "append_rules": [item.as_dict() for item in append_rules],
        "pit_mutation_rule": pit_rule.as_dict() if pit_rule else None,
        "pit_instruments": list(pit_instruments),
        "pit_span_digest_by_code": pit_span_digest_by_code,
        "adj_series": adj.as_dict() if adj else None,
        "file_identity": file_identity,
        "filesystem_tree_merkle": filesystem_tree_merkle,
    }
    component_identity = digest_named_fields(
        COMPONENT_ARTIFACT_COMPONENT_SCHEMA,
        {"component": component.value, "artifact_partitions": component_body["artifact_partitions"]},
    )
    root = digest_named_fields(COMPONENT_ARTIFACT_COMPONENT_SCHEMA, component_body)
    if require_identities and (
        value.get("component_identity") != component_identity
        or value.get("file_identity") != file_identity
        or value.get("component_manifest_root") != root
        or value.get("filesystem_tree_merkle") != filesystem_tree_merkle
    ):
        raise ComponentArtifactManifestError("component derived identity differs")
    return ComponentArtifactEvidence(
        component=component,
        status="COMPLETE",
        reason_code=None,
        component_root_relative_path=component_root,
        source_partitions=source,
        artifact_partitions=artifacts,
        append_rules=append_rules,
        pit_mutation_rule=pit_rule,
        pit_instruments=pit_instruments,
        pit_span_digest_by_code=pit_span_digest_by_code,
        adj_series=adj,
        component_identity=component_identity,
        file_identity=file_identity,
        component_manifest_root=root,
        filesystem_tree_merkle=filesystem_tree_merkle,
    )


def _filesystem_tree_merkle(
    partitions: Sequence[ArtifactPartitionEvidence],
) -> str:
    """Match :func:`copy_on_write.tree_merkle` byte-for-byte."""

    leaves = [
        {
            "relative_path": file.relative_path,
            "size_bytes": file.size_bytes,
            "sha256": file.sha256,
        }
        for partition in partitions
        for file in partition.files
    ]
    leaves.sort(key=lambda item: str(item["relative_path"]))
    payload = (
        json.dumps(
            leaves,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _artifact_instrument_file_targets(
    partitions: Sequence[ArtifactPartitionEvidence],
) -> dict[str, tuple[str, ...]]:
    values: dict[str, list[str]] = {}
    for partition in partitions:
        for file in partition.files:
            if file.instrument is not None:
                values.setdefault(file.instrument, []).append(file.relative_path)
    return {code: tuple(sorted(set(paths))) for code, paths in sorted(values.items())}


def _normalize_source_partition(value: Mapping[str, Any], *, require_identity: bool) -> SourcePartitionEvidence:
    value = _mapping(value, field="source_partition")
    base_fields = {
        "identity",
        "dataset",
        "partition_key",
        "row_count",
        "content_digest",
        "schema_digest",
        "source_table_schema_digest",
        "source_code_membership_digest",
        "min_key",
        "max_key",
        "monthly_content_leaves",
    }
    optional_fields = {"affected_instruments"}
    if set(value).difference(base_fields | optional_fields | {"partition_identity"}) or not base_fields.issubset(value):
        raise ComponentArtifactManifestError("source partition evidence fields differ")
    dataset = str(value["dataset"]).strip()
    partition_key = str(value["partition_key"]).strip()
    identity = str(value["identity"]).strip()
    if not dataset or not partition_key or identity != f"{dataset}:{partition_key}":
        raise ComponentArtifactManifestError("source partition identity differs")
    row_count = value["row_count"]
    if type(row_count) is not int or row_count < 0:
        raise ComponentArtifactManifestError("source partition row count is invalid")
    table_schema = _optional_digest(value["source_table_schema_digest"], "source_table_schema_digest")
    membership = _optional_digest(value["source_code_membership_digest"], "source_code_membership_digest")
    monthly_leaves = _monthly_content_leaves(value["monthly_content_leaves"])
    affected: tuple[str, ...] = ()
    if "affected_instruments" in value:
        affected_raw = tuple(
            _stock_instrument(item)
            for item in _string_sequence(value["affected_instruments"], field="affected_instruments")
        )
        affected = tuple(sorted(set(affected_raw)))
        if affected_raw != affected:
            raise ComponentArtifactManifestError("affected instruments must be sorted and unique")
        if dataset != "stk_limit_rule_coverage":
            raise ComponentArtifactManifestError("affected instruments are only valid for rule-derived limit coverage")
    body = {
        "identity": identity,
        "dataset": dataset,
        "partition_key": partition_key,
        "row_count": row_count,
        "content_digest": ensure_sha256(str(value["content_digest"]), field="content_digest"),
        "schema_digest": ensure_sha256(str(value["schema_digest"]), field="schema_digest"),
        "source_table_schema_digest": table_schema,
        "source_code_membership_digest": membership,
        "min_key": value["min_key"],
        "max_key": value["max_key"],
        "monthly_content_leaves": [dict(item) for item in monthly_leaves],
    }
    if affected:
        body["affected_instruments"] = list(affected)
    identity_digest = digest_named_fields(COMPONENT_SOURCE_PARTITION_SCHEMA, body)
    if require_identity and value.get("partition_identity") != identity_digest:
        raise ComponentArtifactManifestError("source partition derived identity differs")
    return SourcePartitionEvidence(
        identity=identity,
        dataset=dataset,
        partition_key=partition_key,
        row_count=row_count,
        content_digest=body["content_digest"],
        schema_digest=body["schema_digest"],
        source_table_schema_digest=table_schema,
        source_code_membership_digest=membership,
        min_key=body["min_key"],
        max_key=body["max_key"],
        monthly_content_leaves=monthly_leaves,
        partition_identity=identity_digest,
        affected_instruments=affected,
    )


def _normalize_artifact_partition(
    component: Component,
    value: Mapping[str, Any],
    *,
    source_ids: Sequence[str],
    require_identity: bool,
) -> ArtifactPartitionEvidence:
    value = _mapping(value, field="artifact_partition")
    base_fields = {
        "partition_key",
        "source_partition_identities",
        "dependency_edges",
        "instruments",
        "start",
        "end",
        "files",
    }
    if set(value).difference(base_fields | {"partition_identity"}) or not base_fields.issubset(value):
        raise ComponentArtifactManifestError("artifact partition evidence fields differ")
    key = str(value["partition_key"]).strip()
    if not key:
        raise ComponentArtifactManifestError("artifact partition key is empty")
    dependencies = tuple(
        sorted(set(_string_sequence(value["source_partition_identities"], field="source_partition_identities")))
    )
    if not dependencies or not set(dependencies).issubset(source_ids):
        raise ComponentArtifactManifestError("artifact source dependencies are invalid")
    edges = _nonempty_strings(value["dependency_edges"], field="dependency_edges")
    instruments = tuple(
        sorted({_instrument(item) for item in _string_sequence(value["instruments"], field="instruments")})
    )
    start = _optional_date(value["start"], field="artifact start")
    end = _optional_date(value["end"], field="artifact end")
    if (start is None) != (end is None) or (start is not None and end is not None and end < start):
        raise ComponentArtifactManifestError("artifact partition date range is invalid")
    files = tuple(
        sorted(
            (
                _normalize_file(item, require_identity=require_identity)
                for item in _mapping_sequence(value["files"], field="files")
            ),
            key=lambda item: item.relative_path,
        )
    )
    if not files or len({item.relative_path for item in files}) != len(files):
        raise ComponentArtifactManifestError("artifact partition files are empty/duplicated")
    body = {
        "component": component.value,
        "partition_key": key,
        "source_partition_identities": list(dependencies),
        "dependency_edges": list(edges),
        "instruments": list(instruments),
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "files": [item.as_dict() for item in files],
    }
    identity = digest_named_fields(COMPONENT_ARTIFACT_PARTITION_SCHEMA, body)
    if require_identity and value.get("partition_identity") != identity:
        raise ComponentArtifactManifestError("artifact partition derived identity differs")
    return ArtifactPartitionEvidence(
        partition_key=key,
        source_partition_identities=dependencies,
        dependency_edges=edges,
        instruments=instruments,
        start=start,
        end=end,
        files=files,
        partition_identity=identity,
    )


def _normalize_file(value: Mapping[str, Any], *, require_identity: bool) -> ArtifactFileEvidence:
    value = _mapping(value, field="artifact_file")
    base = {"relative_path", "size_bytes", "sha256", "instrument"}
    if set(value).difference(base | {"file_identity"}) or not base.issubset(value):
        raise ComponentArtifactManifestError("artifact file evidence fields differ")
    relative = normalize_root_relative_path(str(value["relative_path"]))
    size = value["size_bytes"]
    if type(size) is not int or size < 0:
        raise ComponentArtifactManifestError("artifact file size is invalid")
    sha = ensure_sha256(str(value["sha256"]), field="artifact_file_sha256")
    instrument = None if value["instrument"] is None else _instrument(value["instrument"])
    identity = digest_named_fields(
        COMPONENT_ARTIFACT_FILE_SCHEMA,
        {
            "relative_path": relative,
            "size_bytes": size,
            "sha256": sha,
            "instrument": instrument,
        },
    )
    if require_identity and value.get("file_identity") != identity:
        raise ComponentArtifactManifestError("artifact file derived identity differs")
    return ArtifactFileEvidence(relative, size, sha, instrument, identity)


def _normalize_rule(
    value: Mapping[str, Any],
    *,
    known_files: set[str],
    require_identity: bool,
) -> MutationRuleEvidence:
    base = {
        "rule_id",
        "datasets",
        "replace_existing_targets",
        "create_new_targets",
        "create_target_templates",
        "writer_targets_by_instrument",
        "writer_target_policy",
        "dependency_edges",
    }
    if set(value).difference(base | {"rule_identity"}) or not base.issubset(value):
        raise ComponentArtifactManifestError("component mutation rule fields differ")
    rule_id = str(value["rule_id"]).strip()
    if not rule_id:
        raise ComponentArtifactManifestError("component mutation rule id is empty")
    datasets = _nonempty_strings(value["datasets"], field="mutation datasets")
    replace = _paths(value["replace_existing_targets"], field="replace targets")
    create = _paths(value["create_new_targets"], field="create targets")
    templates = tuple(
        sorted(
            {
                _target_template(item)
                for item in _string_sequence(value["create_target_templates"], field="create templates")
            }
        )
    )
    raw_by_instrument = _mapping(
        value["writer_targets_by_instrument"],
        field="mutation writer targets by instrument",
    )
    by_instrument = {
        _instrument(code): _paths(targets, field=f"mutation writer targets:{code}")
        for code, targets in raw_by_instrument.items()
    }
    writer_target_policy = str(value["writer_target_policy"])
    if writer_target_policy not in {
        "explicit_by_instrument_v1",
        "artifact_file_instrument_index_v1",
    }:
        raise ComponentArtifactManifestError("component mutation writer-target policy is invalid")
    edges = _nonempty_strings(value["dependency_edges"], field="mutation dependency edges")
    if not replace and not create and not templates and not by_instrument:
        raise ComponentArtifactManifestError("component mutation rule has no exact targets")
    if (
        not set(replace).issubset(known_files)
        or set(create).intersection(known_files)
        or set(replace).intersection(create)
    ):
        raise ComponentArtifactManifestError("component mutation replace/create targets are inconsistent")
    if any(not targets or not set(targets).issubset(known_files) for targets in by_instrument.values()):
        raise ComponentArtifactManifestError("component mutation per-instrument target is not an existing file")
    body = {
        "rule_id": rule_id,
        "datasets": list(datasets),
        "replace_existing_targets": list(replace),
        "create_new_targets": list(create),
        "create_target_templates": list(templates),
        "writer_targets_by_instrument": {code: list(by_instrument[code]) for code in sorted(by_instrument)},
        "writer_target_policy": writer_target_policy,
        "dependency_edges": list(edges),
    }
    identity = digest_named_fields(COMPONENT_MUTATION_RULE_SCHEMA, body)
    if require_identity and value.get("rule_identity") != identity:
        raise ComponentArtifactManifestError("component mutation rule identity differs")
    return MutationRuleEvidence(
        rule_id,
        datasets,
        replace,
        create,
        templates,
        {code: by_instrument[code] for code in sorted(by_instrument)},
        writer_target_policy,
        edges,
        identity,
    )


def _normalize_adj_series(
    value: Mapping[str, Any],
    *,
    known_files: set[str],
    require_identity: bool,
) -> AdjSeriesEvidence:
    base = {
        "complete",
        "qfq_denominator_by_code",
        "ordered_adj_digest_by_code",
        "adj_row_count_by_code",
        "monthly_ordered_adj_by_code",
        "writer_targets_by_code",
        "shared_writer_targets",
        "writer_target_policy",
    }
    if set(value).difference(base | {"evidence_identity"}) or not base.issubset(value):
        raise ComponentArtifactManifestError("adj-series evidence fields differ")
    if value["complete"] is not True:
        raise ComponentArtifactManifestError("adj-series evidence must be omitted rather than marked incomplete")
    denominator = _decimal_map(value["qfq_denominator_by_code"], field="qfq denominator")
    ordered = _digest_map(value["ordered_adj_digest_by_code"], field="ordered adj digest")
    row_counts = _positive_int_map(value["adj_row_count_by_code"], field="adj row count")
    monthly = _normalize_adj_monthly(value["monthly_ordered_adj_by_code"])
    if monthly and not set(monthly).issubset(denominator):
        raise ComponentArtifactManifestError("adj-series monthly code evidence exceeds denominator authority")
    raw_targets = _mapping(value["writer_targets_by_code"], field="adj writer targets")
    targets = {
        _stock_instrument(code): _paths(paths, field=f"adj writer targets:{code}")
        for code, paths in raw_targets.items()
    }
    shared_targets = _paths(value["shared_writer_targets"], field="adj shared writer targets")
    writer_target_policy = str(value["writer_target_policy"])
    if writer_target_policy not in {
        "explicit_by_instrument_v1",
        "artifact_file_instrument_index_v1",
    }:
        raise ComponentArtifactManifestError("adj writer-target policy is invalid")
    if (
        not denominator
        or set(denominator) != set(ordered)
        or set(denominator) != set(row_counts)
        or (writer_target_policy == "explicit_by_instrument_v1" and set(denominator) != set(targets))
    ):
        raise ComponentArtifactManifestError("adj-series per-code evidence is incomplete")
    if any(not values or not set(values).issubset(known_files) for values in targets.values()):
        raise ComponentArtifactManifestError("adj-series writer target is not an existing artifact file")
    if not set(shared_targets).issubset(known_files):
        raise ComponentArtifactManifestError("adj-series shared writer target is not an existing artifact file")
    body = {
        "complete": True,
        "qfq_denominator_by_code": denominator,
        "ordered_adj_digest_by_code": ordered,
        "adj_row_count_by_code": row_counts,
        "monthly_ordered_adj_by_code": monthly,
        "writer_targets_by_code": {code: list(targets[code]) for code in sorted(targets)},
        "shared_writer_targets": list(shared_targets),
        "writer_target_policy": writer_target_policy,
    }
    identity = digest_named_fields(COMPONENT_ADJ_SERIES_SCHEMA, body)
    if require_identity and value.get("evidence_identity") != identity:
        raise ComponentArtifactManifestError("adj-series evidence identity differs")
    return AdjSeriesEvidence(
        True,
        denominator,
        ordered,
        row_counts,
        monthly,
        targets,
        shared_targets,
        writer_target_policy,
        identity,
    )


def _mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ComponentArtifactManifestError(f"{field} must be an object")
    return value


def _normalize_candidate_metadata(
    value: Any,
) -> dict[str, dict[str, Any]]:
    if value is None:
        return {}
    raw = _mapping(value, field="candidate_metadata")
    output: dict[str, dict[str, Any]] = {}
    for path, evidence in raw.items():
        relative = normalize_root_relative_path(str(path))
        item = _mapping(evidence, field=f"candidate_metadata:{relative}")
        if set(item) != {
            "schema_version",
            "manifest_identity",
            "sha256",
            "size_bytes",
        }:
            raise ComponentArtifactManifestError("candidate metadata evidence fields differ")
        size = item["size_bytes"]
        if type(size) is not int or size <= 0:
            raise ComponentArtifactManifestError("candidate metadata size is invalid")
        output[relative] = {
            "schema_version": str(item["schema_version"]),
            "manifest_identity": ensure_sha256(
                str(item["manifest_identity"]),
                field="candidate_metadata_manifest_identity",
            ),
            "sha256": ensure_sha256(str(item["sha256"]), field="candidate_metadata_sha256"),
            "size_bytes": size,
        }
    return {key: output[key] for key in sorted(output)}


def _mapping_sequence(value: Any, *, field: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise ComponentArtifactManifestError(f"{field} must be an object list")
    return tuple(value)


def _string_sequence(value: Any, *, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
        raise ComponentArtifactManifestError(f"{field} must be a non-empty string list")
    return tuple(item.strip() for item in value)


def _nonempty_strings(value: Any, *, field: str) -> tuple[str, ...]:
    result = tuple(sorted(set(_string_sequence(value, field=field))))
    if not result:
        raise ComponentArtifactManifestError(f"{field} cannot be empty")
    return result


def _paths(value: Any, *, field: str) -> tuple[str, ...]:
    paths = tuple(sorted(normalize_root_relative_path(item) for item in _string_sequence(value, field=field)))
    if len(paths) != len(set(paths)):
        raise ComponentArtifactManifestError(f"{field} contains duplicate paths")
    return paths


def _target_template(value: str) -> str:
    raw = str(value).replace("\\", "/").casefold().strip()
    if (
        raw.count(_INSTRUMENT_PLACEHOLDER) != 1
        or "{" in raw.replace(_INSTRUMENT_PLACEHOLDER, "")
        or "}" in raw.replace(_INSTRUMENT_PLACEHOLDER, "")
    ):
        raise ComponentArtifactManifestError("create target template token is invalid")
    probe = normalize_root_relative_path(raw.replace(_INSTRUMENT_PLACEHOLDER, "000001.sz"))
    if probe != raw.replace(_INSTRUMENT_PLACEHOLDER, "000001.sz"):
        raise ComponentArtifactManifestError("create target template is non-canonical")
    return raw


def _instrument(value: Any) -> str:
    code = str(value).strip().upper()
    if _INSTRUMENT.fullmatch(code) is None:
        raise ComponentArtifactManifestError("component instrument code is invalid")
    return code


def _stock_instrument(value: Any) -> str:
    code = str(value).strip().upper()
    if _STOCK_INSTRUMENT.fullmatch(code) is None:
        raise ComponentArtifactManifestError("PIT/QFQ stock instrument code is invalid")
    return code


def _mutable_writer_target(relative_path: str) -> bool:
    """Keep immutable CSV segment history out of every future mutation set."""

    normalized = str(relative_path).replace("\\", "/").casefold()
    return not (
        normalized.startswith("csv_deltas/")
        or normalized.startswith("csv_overrides/")
        or normalized.startswith("csv_lineage/")
    )


def _optional_digest(value: Any, field: str) -> str | None:
    return None if value is None else ensure_sha256(str(value), field=field)


def _optional_date(value: Any, *, field: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ComponentArtifactManifestError(f"{field} is invalid") from exc


def _digest_map(value: Any, *, field: str) -> dict[str, str]:
    raw = _mapping(value, field=field)
    result = {
        _stock_instrument(code): ensure_sha256(str(digest), field=f"{field}:{code}") for code, digest in raw.items()
    }
    return {code: result[code] for code in sorted(result)}


def _decimal_map(value: Any, *, field: str) -> dict[str, str]:
    raw = _mapping(value, field=field)
    result: dict[str, str] = {}
    for code, untrusted in raw.items():
        try:
            decimal = Decimal(str(untrusted))
        except (InvalidOperation, ValueError) as exc:
            raise ComponentArtifactManifestError(f"{field} value is invalid") from exc
        if not decimal.is_finite() or decimal <= 0:
            raise ComponentArtifactManifestError(f"{field} value must be finite and positive")
        normalized = decimal.normalize()
        text = format(normalized, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        result[_stock_instrument(code)] = text
    return {code: result[code] for code in sorted(result)}


def _positive_int_map(value: Any, *, field: str) -> dict[str, int]:
    raw = _mapping(value, field=field)
    result: dict[str, int] = {}
    for code, untrusted in raw.items():
        if type(untrusted) is not int or untrusted <= 0:
            raise ComponentArtifactManifestError(f"{field} value must be a positive integer")
        result[_stock_instrument(code)] = untrusted
    return {code: result[code] for code in sorted(result)}


def _normalize_adj_monthly(
    value: Any,
) -> dict[str, Mapping[str, Mapping[str, Any]]]:
    raw = _mapping(value, field="adj monthly evidence")
    result: dict[str, Mapping[str, Mapping[str, Any]]] = {}
    for raw_code, raw_months in raw.items():
        code = _stock_instrument(raw_code)
        months = _mapping(raw_months, field=f"adj monthly evidence:{code}")
        normalized: dict[str, Mapping[str, Any]] = {}
        for raw_month, raw_leaf in months.items():
            month = str(raw_month)
            try:
                parsed = date.fromisoformat(f"{month}-01")
            except ValueError as exc:
                raise ComponentArtifactManifestError("adj monthly evidence month is invalid") from exc
            if month != f"{parsed.year:04d}-{parsed.month:02d}":
                raise ComponentArtifactManifestError("adj monthly evidence month is non-canonical")
            leaf = _mapping(raw_leaf, field=f"adj monthly leaf:{code}:{month}")
            if set(leaf) != {"ordered_digest", "row_count", "min_date", "max_date"}:
                raise ComponentArtifactManifestError("adj monthly evidence leaf fields differ")
            rows = leaf["row_count"]
            if type(rows) is not int or rows <= 0:
                raise ComponentArtifactManifestError("adj monthly evidence row count is invalid")
            start = _optional_date(leaf["min_date"], field="adj monthly min_date")
            end = _optional_date(leaf["max_date"], field="adj monthly max_date")
            if (
                start is None
                or end is None
                or end < start
                or start.strftime("%Y-%m") != month
                or end.strftime("%Y-%m") != month
            ):
                raise ComponentArtifactManifestError("adj monthly evidence date range differs from month")
            normalized[month] = {
                "ordered_digest": ensure_sha256(
                    str(leaf["ordered_digest"]), field=f"adj monthly digest:{code}:{month}"
                ),
                "row_count": rows,
                "min_date": start.isoformat(),
                "max_date": end.isoformat(),
            }
        result[code] = {month: normalized[month] for month in sorted(normalized)}
    return {code: result[code] for code in sorted(result)}


def _monthly_content_leaves(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        raise ComponentArtifactManifestError("source monthly content leaves must be a list")
    required = {
        "schema_version",
        "month",
        "row_count",
        "min_key",
        "max_key",
        "merkle_root",
        "content_digest",
        "leaf_identity",
    }
    result: list[Mapping[str, Any]] = []
    previous: str | None = None
    for raw in value:
        if not isinstance(raw, Mapping) or set(raw) != required:
            raise ComponentArtifactManifestError("source monthly content leaf fields differ")
        month = str(raw["month"])
        try:
            parsed = date.fromisoformat(f"{month}-01")
        except ValueError as exc:
            raise ComponentArtifactManifestError("source monthly content leaf month is invalid") from exc
        if month != f"{parsed.year:04d}-{parsed.month:02d}" or (previous is not None and month <= previous):
            raise ComponentArtifactManifestError("source monthly content leaves are duplicated or unordered")
        rows = raw["row_count"]
        if type(rows) is not int or rows <= 0:
            raise ComponentArtifactManifestError("source monthly content leaf row count is invalid")
        body = {
            "schema_version": SOURCE_MONTH_CONTENT_LEAF_SCHEMA,
            "month": month,
            "row_count": rows,
            "min_key": raw["min_key"],
            "max_key": raw["max_key"],
            "merkle_root": ensure_sha256(str(raw["merkle_root"]), field="monthly_merkle_root"),
            "content_digest": ensure_sha256(str(raw["content_digest"]), field="monthly_content_digest"),
        }
        identity = digest_named_fields(SOURCE_MONTH_CONTENT_LEAF_SCHEMA, body)
        if raw["schema_version"] != SOURCE_MONTH_CONTENT_LEAF_SCHEMA or raw["leaf_identity"] != identity:
            raise ComponentArtifactManifestError("source monthly content leaf identity differs")
        result.append({**body, "leaf_identity": identity})
        previous = month
    return tuple(result)


__all__ = [
    "AdjSeriesEvidence",
    "ArtifactFileEvidence",
    "ArtifactPartitionEvidence",
    "COMPONENT_ARTIFACT_MANIFEST_SCHEMA",
    "ComponentArtifactEvidence",
    "ComponentArtifactManifest",
    "ComponentArtifactManifestError",
    "MutationRuleEvidence",
    "SourcePartitionEvidence",
    "load_component_artifact_manifest",
    "normalize_current_source_partition",
    "seal_component_artifact_manifest",
]
