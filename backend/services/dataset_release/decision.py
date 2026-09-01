from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any, Iterable, Mapping, Sequence

from .canonical import digest_named_fields, ensure_sha256
from .contracts import (
    REQUIRED_COMPONENTS,
    Component,
    ComponentAction,
    ValidationCompatibility,
)
from .errors import DecisionError


DECISION_SCHEMA_VERSION = "dataset_release_decision_v1"


@dataclass(frozen=True)
class BaselineCandidate:
    release_id: str
    release_digest: str
    attestation_key: str
    cutoff: date
    artifact_root: str
    semantic_compatible: bool
    artifact_compatible: bool
    current_source_equivalent: bool

    def __post_init__(self) -> None:
        for name in ("release_digest", "attestation_key", "artifact_root"):
            ensure_sha256(getattr(self, name), field=name)


def select_reuse_baseline(
    candidates: Sequence[BaselineCandidate],
    *,
    target_cutoff: date,
) -> BaselineCandidate | None:
    eligible = [
        item
        for item in candidates
        if item.cutoff <= target_cutoff
        and item.semantic_compatible
        and item.artifact_compatible
        and item.current_source_equivalent
    ]
    if not eligible:
        return None
    latest_cutoff = max(item.cutoff for item in eligible)
    latest = [item for item in eligible if item.cutoff == latest_cutoff]
    roots = {item.artifact_root for item in latest}
    if len(roots) != 1:
        raise DecisionError(
            "multiple highest-cutoff baselines have different artifact roots",
            code="REUSE_BASELINE_CONFLICT",
            context={
                "cutoff": latest_cutoff.isoformat(),
                "release_ids": sorted(item.release_id for item in latest),
                "artifact_roots": sorted(roots),
            },
        )
    return sorted(latest, key=lambda item: (item.release_digest, item.release_id))[0]


@dataclass(frozen=True)
class FrozenReuseEvidence:
    source_release_id: str
    source_release_digest: str
    source_attestation_key: str
    artifact_id: str
    component_partition_key: str
    manifest_root: str
    file_identity: str
    reuse_mode: str
    mutation_set: tuple[str, ...]
    compatibility_reason: str
    replace_existing_targets: tuple[str, ...] = ()
    create_new_targets: tuple[str, ...] = ()
    invalidation_scopes: tuple[Mapping[str, Any], ...] = ()
    component_root_relative_path: str = ""
    canonical_lineage: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        for name in (
            "source_release_digest",
            "source_attestation_key",
            "manifest_root",
            "file_identity",
        ):
            ensure_sha256(getattr(self, name), field=name)
        if not self.compatibility_reason.strip():
            raise DecisionError("reuse evidence requires an explicit compatibility reason")
        replace = tuple(sorted(set(self.replace_existing_targets)))
        create = tuple(sorted(set(self.create_new_targets)))
        if set(replace).intersection(create):
            raise DecisionError("reuse evidence replace/create targets overlap")
        if replace or create:
            if tuple(sorted(set(self.mutation_set))) != tuple(sorted((*replace, *create))):
                raise DecisionError("reuse evidence split targets differ from the mutation set")
        if self.canonical_lineage is not None:
            lineage = self.canonical_lineage
            required = {
                "capability",
                "baseline_schema_version",
                "baseline_lineage_root",
                "event_key",
                "mutation_identity",
                "planned_buckets",
                "anchor_key",
            }
            if set(lineage) != required:
                raise DecisionError("canonical lineage planning fields differ")
            if lineage.get("capability") != "candidate_local_persistent_code_head_merkle_v3" or not isinstance(
                lineage.get("planned_buckets"), tuple
            ):
                raise DecisionError("canonical lineage planning capability differs")
            ensure_sha256(
                str(lineage.get("mutation_identity", "")),
                field="canonical_lineage_mutation_identity",
            )
            baseline_root = lineage.get("baseline_lineage_root")
            if baseline_root is not None:
                ensure_sha256(
                    str(baseline_root),
                    field="canonical_lineage_baseline_root",
                )
            for field_name in ("event_key", "anchor_key"):
                value = lineage.get(field_name)
                if value is not None and (
                    not isinstance(value, str)
                    or len(value) != 32
                    or any(char not in "0123456789abcdef" for char in value)
                ):
                    raise DecisionError(f"canonical lineage {field_name} is invalid")


@dataclass(frozen=True)
class ComponentDecisionInput:
    component: Component
    partition_key: str
    required: bool = True
    component_identity_equal: bool = False
    manifest_root_equal: bool = False
    source_equivalence_current: bool = False
    validation_current: bool = False
    semantic_compatible: bool = True
    artifact_compatible: bool = True
    producer_compatible: bool = True
    validation_compatibility: ValidationCompatibility = ValidationCompatibility.UNCHANGED
    checkpoint_valid: bool = False
    fingerprints_equal: bool = False
    appended_source_partitions: tuple[str, ...] = ()
    invalidated_scopes: tuple[str, ...] = ()
    frozen_reuse: FrozenReuseEvidence | None = None
    estimated_work: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class ComponentPlan:
    component: Component
    partition_key: str
    action: ComponentAction
    reason: str
    changed_fingerprints: tuple[str, ...]
    invalidation_edges: tuple[str, ...]
    estimated_work: Mapping[str, int]
    frozen_reuse: FrozenReuseEvidence | None = None

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["component"] = self.component.value
        payload["action"] = self.action.value
        frozen = payload.get("frozen_reuse")
        if isinstance(frozen, dict) and frozen.get("canonical_lineage") is None:
            # Preserve byte-for-byte v1 action-plan digests for legacy plans.
            frozen.pop("canonical_lineage", None)
        return payload


@dataclass(frozen=True)
class ActionPlan:
    actions: tuple[ComponentPlan, ...]

    def __post_init__(self) -> None:
        covered = {item.component for item in self.actions}
        missing = [item.value for item in REQUIRED_COMPONENTS if item not in covered]
        if missing:
            raise DecisionError(f"action plan omits required components: {missing}")
        identities = [(item.component, item.partition_key) for item in self.actions]
        if len(identities) != len(set(identities)):
            raise DecisionError("action plan contains duplicate component partitions")

    @property
    def digest(self) -> str:
        return digest_named_fields(
            DECISION_SCHEMA_VERSION,
            {
                "actions": [
                    item.as_dict()
                    for item in sorted(
                        self.actions,
                        key=lambda value: (value.component.value, value.partition_key),
                    )
                ]
            },
        )

    def by_component(self, component: Component) -> tuple[ComponentPlan, ...]:
        return tuple(item for item in self.actions if item.component is component)


def decide_component(value: ComponentDecisionInput) -> ComponentPlan:
    if not value.required:
        raise DecisionError(f"optional component input is outside qe_hmm_full_v1: {value.component}")
    changed: list[str] = []
    if not value.semantic_compatible:
        changed.append("semantic_fingerprint")
    if not value.producer_compatible:
        changed.append("producer_fingerprint")
    if not value.artifact_compatible:
        changed.append("artifact_fingerprint")
    if value.invalidated_scopes or value.appended_source_partitions:
        changed.append("source_input_digest")
    if not value.validation_current:
        changed.append("validation_fingerprint")

    if (
        value.component_identity_equal
        and value.manifest_root_equal
        and value.source_equivalence_current
        and value.validation_current
    ):
        action = ComponentAction.NOOP
        reason = "component identity, manifest, validation and current source equivalence match"
    elif (
        value.semantic_compatible
        and value.artifact_compatible
        and value.producer_compatible
        and value.source_equivalence_current
        and value.validation_compatibility is ValidationCompatibility.VALIDATOR_STRENGTHENING_COMPATIBLE
        and not value.invalidated_scopes
        and not value.appended_source_partitions
    ):
        action = ComponentAction.REATTEST
        reason = "validator strengthening is explicitly artifact-compatible"
    elif value.checkpoint_valid:
        action = ComponentAction.RESUME
        reason = "same lineage has a fence-valid contiguous checkpoint"
    elif value.fingerprints_equal:
        action = ComponentAction.REUSE
        reason = "component partition fingerprints are identical"
    elif (
        value.appended_source_partitions
        and not value.invalidated_scopes
        and value.semantic_compatible
        and value.artifact_compatible
        and value.producer_compatible
    ):
        action = ComponentAction.INCREMENTAL
        reason = "only new source partitions were appended"
    elif (
        value.invalidated_scopes
        and value.semantic_compatible
        and value.artifact_compatible
        and value.producer_compatible
    ):
        action = ComponentAction.SELECTIVE_REBUILD
        reason = "dependency graph identified a bounded invalidation scope"
    else:
        action = ComponentAction.FULL_REBUILD
        reason = "semantic, producer, artifact, or dependency compatibility is not proven"

    if (
        action
        in {
            ComponentAction.REUSE,
            ComponentAction.INCREMENTAL,
            ComponentAction.SELECTIVE_REBUILD,
        }
        and value.frozen_reuse is None
    ):
        raise DecisionError(f"{action.value} requires frozen baseline evidence")
    return ComponentPlan(
        component=value.component,
        partition_key=value.partition_key,
        action=action,
        reason=reason,
        changed_fingerprints=tuple(sorted(set(changed))),
        invalidation_edges=tuple(value.invalidated_scopes),
        estimated_work=dict(value.estimated_work),
        frozen_reuse=value.frozen_reuse,
    )


def build_action_plan(values: Iterable[ComponentDecisionInput]) -> ActionPlan:
    actions = tuple(decide_component(value) for value in values)
    return ActionPlan(actions=actions)
