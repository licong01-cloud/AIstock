"""Compile one unified monthly SOURCE snapshot for the existing BUILD engine.

The monthly product contract names eight release components, while the mature
provider-free materializer owns four physical components.  This bridge is the
only translation boundary.  It reloads the formal frozen source receipt from
CAS, verifies the exact source bundle bytes, and emits the legacy physical
action/build-input contract without reopening PostgreSQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .artifact_ready_source import load_artifact_ready_contract
from .canonical import canonical_json_bytes, digest_named_fields, ensure_sha256
from .cas_store import CASRef, CASStore
from .contracts import Component, ComponentAction as PhysicalAction
from .decision import ActionPlan, ComponentPlan
from .errors import CanonicalizationError
from .monthly_postgres_source import FROZEN_SOURCE_BUNDLE_SCHEMA
from .monthly_unified import COMPONENTS, ComponentAction as MonthlyAction
from .profile import DatasetProfile
from .resolution import BUILD_INPUTS_SCHEMA_VERSION
from .resolution_processor import (
    CANDIDATE_OUTPUT_PREDICTED_BYTES,
    SAMPLE_POLICY,
    monthly_build_fingerprints,
)
from .source_authority import load_source_stage_receipt


MONTHLY_BUILD_BRIDGE_SCHEMA = "aistock_monthly_physical_build_bridge_v1"
_PHYSICAL_COMPONENTS: Mapping[str, Component] = {
    "day": Component.DAILY_BIN,
    "minute": Component.MINUTE_BIN,
    "factor": Component.FACTOR_H5_STATIC,
    "index": Component.DOMESTIC_INDEX_CONTEXT,
}
_ACTION_MAP: Mapping[MonthlyAction, PhysicalAction] = {
    MonthlyAction.REUSE: PhysicalAction.REUSE,
    MonthlyAction.INCREMENTAL: PhysicalAction.INCREMENTAL,
    MonthlyAction.SELECTIVE_REBUILD: PhysicalAction.SELECTIVE_REBUILD,
    MonthlyAction.COMPONENT_REBUILD: PhysicalAction.FULL_REBUILD,
}
_ZERO_SAFETY = {
    "database_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
    "provider_database_writes": 0,
    "candidate_writes": 0,
}


class MonthlyBuildBridgeError(RuntimeError):
    """The monthly SOURCE evidence cannot safely drive the physical build."""


@dataclass(frozen=True, slots=True)
class CompiledMonthlyBuild:
    source_bundle_path: Path
    source_bundle_sha256: str
    source_stage_receipt_ref: CASRef
    monthly_actions: Mapping[str, str]
    physical_plan: Mapping[str, Any]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _is_link(path: Path) -> bool:
    return path.is_symlink() or bool(getattr(path, "is_junction", lambda: False)())


def _contains_link(root: Path, relative: Path) -> bool:
    current = root
    if _is_link(current):
        return True
    for part in relative.parts:
        current = current / part
        if _is_link(current):
            return True
    return False


def _resolve_ref(
    reference: Mapping[str, Any],
    *,
    roots: Sequence[Path],
    label: str,
) -> Path:
    if set(reference) != {"id", "sha256", "size"}:
        raise MonthlyBuildBridgeError(f"{label} reference fields differ")
    relative = Path(str(reference.get("id") or ""))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise MonthlyBuildBridgeError(f"{label} reference path is invalid")
    try:
        digest = ensure_sha256(
            str(reference.get("sha256") or ""),
            field=f"{label}.sha256",
        )
    except CanonicalizationError as exc:
        raise MonthlyBuildBridgeError(f"{label} reference digest is invalid") from exc
    size = reference.get("size")
    if type(size) is not int or size < 0:
        raise MonthlyBuildBridgeError(f"{label} reference size is invalid")
    matches: list[Path] = []
    for root in roots:
        if _contains_link(root, relative):
            raise MonthlyBuildBridgeError(
                f"{label} path must not contain a symlink or junction"
            )
        resolved_root = root.resolve(strict=True)
        candidate = resolved_root / relative
        if not candidate.exists():
            continue
        resolved = candidate.resolve(strict=True)
        if (
            not resolved.is_relative_to(resolved_root)
            or _is_link(candidate)
            or not resolved.is_file()
        ):
            raise MonthlyBuildBridgeError(f"{label} must be a regular file under its root")
        matches.append(resolved)
    if len(matches) != 1:
        raise MonthlyBuildBridgeError(f"{label} must resolve under exactly one artifact root")
    path = matches[0]
    if path.stat().st_size != size or _sha256(path) != digest:
        raise MonthlyBuildBridgeError(f"{label} bytes differ")
    return path


def _source_bundle(
    source_receipt: Mapping[str, Any],
    *,
    artifact_roots: Sequence[Path],
) -> tuple[Path, Mapping[str, Any], str]:
    input_refs = source_receipt.get("input_refs")
    if not isinstance(input_refs, list):
        raise MonthlyBuildBridgeError("SOURCE input references are missing")
    matches = [
        item
        for item in input_refs
        if isinstance(item, Mapping)
        and Path(str(item.get("id") or "")).name == "frozen-source-bundle.json"
    ]
    if len(matches) != 1:
        raise MonthlyBuildBridgeError("SOURCE frozen bundle is missing or ambiguous")
    path = _resolve_ref(matches[0], roots=artifact_roots, label="frozen source bundle")
    raw = path.read_bytes()
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyBuildBridgeError("frozen source bundle is unreadable") from exc
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != FROZEN_SOURCE_BUNDLE_SCHEMA
        or raw != canonical_json_bytes(value) + b"\n"
    ):
        raise MonthlyBuildBridgeError("frozen source bundle schema/canonical bytes differ")
    return path, value, str(matches[0]["sha256"])


def _physical_action_plan(
    actions: Mapping[str, Any],
    *,
    force_full_rebuild: bool = False,
) -> ActionPlan:
    if set(actions) != set(COMPONENTS):
        raise MonthlyBuildBridgeError("monthly component action set is incomplete")
    plans: list[ComponentPlan] = []
    for monthly_name, component in _PHYSICAL_COMPONENTS.items():
        try:
            monthly_action = MonthlyAction(str(actions[monthly_name]))
        except ValueError as exc:
            raise MonthlyBuildBridgeError(
                f"monthly component action is invalid: {monthly_name}"
            ) from exc
        action = (
            PhysicalAction.FULL_REBUILD
            if force_full_rebuild
            else _ACTION_MAP[monthly_action]
        )
        reason = (
            f"monthly_v2_initial_migration:{monthly_name}:{monthly_action.value}"
            if force_full_rebuild
            else f"monthly_v2:{monthly_name}:{monthly_action.value}"
        )
        plans.append(
            ComponentPlan(
                component=component,
                partition_key="full",
                action=action,
                reason=reason,
                changed_fingerprints=("source_input_digest",),
                invalidation_edges=(),
                estimated_work={},
            )
        )
    return ActionPlan(tuple(plans))


def compile_initial_monthly_build(
    *,
    context_plan: Mapping[str, Any],
    source_receipt: Mapping[str, Any],
    profile: DatasetProfile,
    cas: CASStore,
    artifact_roots: Sequence[Path],
) -> CompiledMonthlyBuild:
    """Compile the first unified-v2 physical build from sealed SOURCE evidence.

    Initial migration deliberately promotes physical REUSE/incremental actions
    to full rebuilds: an active pre-v2 candidate has no compatible
    component-artifact authority.  Later successor reuse must supply that exact
    baseline authority rather than silently adopting the predecessor tree.
    """

    try:
        target_cutoff = date.fromisoformat(
            str(context_plan.get("target_cutoff") or "")
        )
    except ValueError as exc:
        raise MonthlyBuildBridgeError("monthly target cutoff is invalid") from exc
    predecessor = context_plan.get("predecessor")
    if not isinstance(predecessor, Mapping):
        raise MonthlyBuildBridgeError("monthly predecessor identity is missing")
    try:
        predecessor_manifest_sha256 = ensure_sha256(
            str(predecessor.get("dataset_manifest_sha256") or ""),
            field="predecessor.dataset_manifest_sha256",
        )
    except CanonicalizationError as exc:
        raise MonthlyBuildBridgeError(
            "monthly predecessor manifest identity is invalid"
        ) from exc
    source_scope = source_receipt.get("scope")
    if not isinstance(source_scope, Mapping):
        raise MonthlyBuildBridgeError("SOURCE scope is invalid")
    monthly_actions = source_scope.get("component_actions")
    if not isinstance(monthly_actions, Mapping):
        raise MonthlyBuildBridgeError("SOURCE component actions are missing")
    action_plan = _physical_action_plan(monthly_actions, force_full_rebuild=True)

    bundle_path, bundle, bundle_sha = _source_bundle(
        source_receipt,
        artifact_roots=artifact_roots,
    )
    if bundle.get("cutoff") != target_cutoff.isoformat():
        raise MonthlyBuildBridgeError("frozen source bundle cutoff differs")
    try:
        source_stage_ref = cas.verify(CASRef.from_value(bundle["source_stage_receipt_ref"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise MonthlyBuildBridgeError("formal source-stage receipt is missing") from exc
    frozen = load_source_stage_receipt(
        cas,
        source_stage_ref,
        expected_profile=profile.profile,
        expected_cutoff=target_cutoff,
        profile=profile,
    )
    if (
        bundle.get("source_content_root") != frozen.source_content_root
        or bundle.get("pit_snapshot_digest") != frozen.pit_snapshot_digest
        or bundle.get("artifact_ready_content_root") != frozen.artifact_ready_content_root
        or bundle.get("artifact_ready_provenance_root")
        != frozen.artifact_ready_provenance_root
    ):
        raise MonthlyBuildBridgeError("frozen source bundle identity differs")
    if frozen.artifact_ready_contract_ref is None:
        raise MonthlyBuildBridgeError("frozen source lacks artifact-ready authority")
    artifact_ready = load_artifact_ready_contract(
        cas,
        profile,
        frozen.artifact_ready_contract_ref,
        expected_source_content_root=frozen.source_content_root,
        expected_pit_snapshot_digest=frozen.pit_snapshot_digest,
    )
    effective_partitions: dict[str, list[dict[str, Any]]] = {}
    for component in Component:
        manifest = cas.get_json_bounded(
            artifact_ready.component_manifest_refs[component.value],
            max_bytes=32 * 1024 * 1024,
        )
        rows = manifest.get("effective_partitions") if isinstance(manifest, Mapping) else None
        if not isinstance(rows, list):
            raise MonthlyBuildBridgeError(
                f"artifact-ready component partitions are missing: {component.value}"
            )
        effective_partitions[component.value] = [dict(item) for item in rows]

    fingerprints = monthly_build_fingerprints(profile)
    source_predicted = frozen.source_cas_usage.get("predicted_remaining_new_bytes")
    if type(source_predicted) is not int or source_predicted < 0:
        raise MonthlyBuildBridgeError("frozen source byte estimate is invalid")
    predicted = max(source_predicted, CANDIDATE_OUTPUT_PREDICTED_BYTES)
    build_inputs = {
        "schema_version": BUILD_INPUTS_SCHEMA_VERSION,
        "profile": profile.profile,
        "scope": "full",
        "cutoff": target_cutoff.isoformat(),
        "logical_request_key": digest_named_fields(
            "aistock_monthly_build_logical_request_v1",
            {
                "profile": profile.profile,
                "target_cutoff": target_cutoff,
                "predecessor_manifest_sha256": predecessor_manifest_sha256,
            },
        ),
        "resolved_intent_key": digest_named_fields(
            "aistock_monthly_build_resolved_intent_v1",
            {
                "source_content_root": frozen.source_content_root,
                "artifact_ready_content_root": frozen.artifact_ready_content_root,
                "pit_snapshot_digest": frozen.pit_snapshot_digest,
            },
        ),
        "semantic_profile_digest": profile.semantic_profile_digest,
        "predicted_new_bytes": predicted,
        "source_manifest_ref": frozen.source_manifest_ref.as_dict(),
        "artifact_ready_contract_ref": frozen.artifact_ready_contract_ref.as_dict(),
        "artifact_ready_content_root": frozen.artifact_ready_content_root,
        "artifact_ready_provenance_root": frozen.artifact_ready_provenance_root,
        "provider_receipt_refs": [item.as_dict() for item in frozen.provider_receipt_refs],
        "artifact_ready_derived_source_receipt_refs": [
            item.as_dict() for item in frozen.artifact_ready_derived_source_receipt_refs
        ],
        "pit_snapshot_ref": frozen.pit_snapshot_ref.as_dict(),
        "source_snapshot": {
            "source_content_root": frozen.artifact_ready_content_root,
            "raw_source_content_root": frozen.source_content_root,
            "artifact_ready_content_root": frozen.artifact_ready_content_root,
            "artifact_ready_provenance_root": frozen.artifact_ready_provenance_root,
            "pit_snapshot_digest": frozen.pit_snapshot_digest,
        },
        "source_probe": {
            "subject_kind": "NEW_BUILD",
            "subject_identity": frozen.source_content_root,
            "candidate_identity": None,
            "artifact_root": None,
        },
        "partitions": [
            item.as_build_input()
            for item in sorted(frozen.partitions, key=lambda value: value.spec.identity)
        ],
        "artifact_ready_effective_partitions": effective_partitions,
        "baseline": None,
        "fingerprints": {
            **fingerprints,
            "sample_policy": SAMPLE_POLICY,
            "decision_schema": "dataset_release_decision_v1",
        },
        "safety": dict(_ZERO_SAFETY),
    }
    action_rows = [
        item.as_dict()
        for item in sorted(
            action_plan.actions,
            key=lambda value: (value.component.value, value.partition_key),
        )
    ]
    physical_plan = {
        "schema_version": MONTHLY_BUILD_BRIDGE_SCHEMA,
        "actions": action_rows,
        "action_plan_digest": action_plan.digest,
        "build_inputs": build_inputs,
        "monthly_component_actions": {
            name: str(monthly_actions[name]) for name in COMPONENTS
        },
        "source_bundle_sha256": bundle_sha,
        "source_stage_receipt_ref": source_stage_ref.as_dict(),
        "database_read_performed": False,
        "database_write_performed": False,
    }
    return CompiledMonthlyBuild(
        source_bundle_path=bundle_path,
        source_bundle_sha256=bundle_sha,
        source_stage_receipt_ref=source_stage_ref,
        monthly_actions=physical_plan["monthly_component_actions"],
        physical_plan=physical_plan,
    )


__all__ = (
    "CompiledMonthlyBuild",
    "MONTHLY_BUILD_BRIDGE_SCHEMA",
    "MonthlyBuildBridgeError",
    "compile_initial_monthly_build",
)
