"""Provider-free candidate build stages executed by the supervised Worker.

Every data-bearing input is an immutable CAS artifact frozen by resolution.
This module writes only the new candidate staging tree and CAS receipts.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import psutil

from .artifact_ready_build_source import ArtifactReadyBuildSource
from .canonical import digest_named_fields, ensure_sha256, normalize_root_relative_path
from .canonical_lineage import (
    CANONICAL_LINEAGE_CAPABILITY,
    CANONICAL_LINEAGE_SCHEMA,
    instrument_summaries as lineage_instrument_summaries,
    is_lineage_v3,
    legacy_active_segments,
    lineage_bucket,
    migrate_legacy_and_write_transition_updates,
    namespace_manifest as lineage_namespace_manifest,
    validate_lineage_descriptor,
    write_transition_updates,
)
from .canonical_stock_transformer import (
    CanonicalStockTransformMetrics,
    CanonicalStockTransformSpec,
    CanonicalStockTransformer,
)
from .candidate_validator import (
    CandidateComponentTransitionAuthority,
    CandidateValidationSpec,
    CandidateValidator,
)
from .cas_store import CASRef, CASStore
from .component_artifact_manifest import (
    ComponentArtifactEvidence,
    ComponentArtifactManifest,
    load_component_artifact_manifest,
)
from .component_manifest_producer import (
    produce_component_artifact_manifest,
)
from .contracts import (
    CandidateIdentity,
    Component,
    ComponentAction,
    PitProvenanceState,
    ProducerProvenanceState,
    Scope,
)
from .control_store import build_candidate_registration_id, volume_identity
from .copy_on_write import (
    CopyOnWritePlan,
    adopt_deferred_writer_outputs,
    adopt_isolated_writer_patch,
    atomic_write_mutation,
    clone_sealed_tree_for_reuse,
    prepare_copy_on_write_tree,
    restore_copy_on_write_plan,
    tree_merkle,
    writer_target_manifest,
)
from .daily_minute_materializer import (
    DAILY_MINUTE_CSV_PREPARATION_SUPPORTED_SCHEMAS,
    DailyMinuteBinFinalizer,
    DailyMinuteCsvPreparer,
    DailyMinuteIncrementalFinalizer,
    DailyMinuteMaterializationSpec,
    DailyMinutePatchCsvPreparer,
    build_composite_canonical_rows,
    build_selective_override_canonical_rows,
)
from .decision import DECISION_SCHEMA_VERSION
from .errors import DatasetReleaseError
from .external_ordered_rows import (
    ExternalOrderedRowsMetrics,
    external_merge_ordered_rows,
)
from .factor_materializer import (
    FactorBundleMaterializer,
    FactorMaterializationSpec,
    FactorPartitionProducer,
    FactorPartitionProducerSpec,
    FactorSourcePartition,
    SealedFactorChunk,
    merge_factor_partition_by_instrument,
    merge_rolling_factor_states_by_instrument,
    restore_rolling_factor_state_from_bundle,
    restore_rolling_factor_state_from_produced_partition,
)
from .index_contract import IndexDefinition
from .index_materializer import (
    IncrementalIndexContextMaterializer,
    IndexContextMaterializer,
    IndexMaterializationReceipt,
    SelectiveIndexContextMaterializer,
)
from .index_context_candidate_manifest import (
    produce_index_context_candidate_manifest,
    validate_index_context_candidate_manifest,
)
from .minute_overlay import canonical_session_times
from .pit import (
    DATASET_CANDIDATE_MANIFEST_SCHEMA,
    FrozenPitSnapshot,
    build_dataset_pit_binding,
    frozen_pit_snapshot_from_mapping,
)
from .profile import DatasetProfile
from .stock_schema import QLIB_STOCK_FIELDS


BUILD_STAGE_RESULT_SCHEMA = "dataset_release_build_stage_result_v1"
BUILD_RESOURCE_RECEIPT_SCHEMA = "dataset_release_build_resource_receipt_v1"
BUILD_PREPARE_RECEIPT_SCHEMA = "dataset_release_build_prepare_v1"
BUILD_FINALIZE_RECEIPT_SCHEMA = "dataset_release_build_finalize_v1"


class CandidateBuildStageError(DatasetReleaseError):
    code = "BLOCKED_CANDIDATE_BUILD_STAGE_INVALID"


_ZERO_SAFETY = {
    "database_writes": 0,
    "provider_database_writes": 0,
    "production_writes": 0,
    "production_deletes": 0,
    "production_pointer_changes": 0,
    "service_process_controls": 0,
}
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)


@dataclass(frozen=True, slots=True)
class BuildStageInvocation:
    stage: str
    run_id: str
    attempt_id: str
    attempt_fence: int
    pressure_rung: int
    stage_timeout_seconds: int
    release_id: str
    release_digest: str
    staging_relative_path: str
    project_root: Path
    candidate_root: Path
    staging_root: Path
    profile: DatasetProfile
    cas: CASStore
    plan: Mapping[str, Any]
    prerequisites: Mapping[str, str]

    @property
    def build_inputs(self) -> Mapping[str, Any]:
        value = self.plan.get("build_inputs")
        if not isinstance(value, Mapping):
            raise CandidateBuildStageError("build inputs are missing")
        return value


class StageResourceReceipt:
    """Small semantic rollup; the parent supervisor remains authoritative."""

    def __init__(self, invocation: BuildStageInvocation) -> None:
        self.invocation = invocation
        self.checkpoints: list[dict[str, Any]] = []
        self.peak = 0
        self._record("admission")

    def chunk(self, chunk_id: str) -> None:
        self._record("chunk", chunk_id=chunk_id)

    def finish(self) -> Mapping[str, Any]:
        self._record("final")
        profile = self.invocation.profile
        index = self.invocation.pressure_rung
        effective = {
            "index": index,
            "h5_batch": _rung(profile, "h5_batch", index),
            "minute_batch": _rung(profile, "minute_batch", index),
            "chunk_months": _rung(profile, "date_chunk_months", index),
            "row_group_rows": _rung(profile, "row_group_rows", index),
            "dump_workers": _rung(profile, "dump_workers", index),
        }
        return {
            "schema_version": BUILD_RESOURCE_RECEIPT_SCHEMA,
            "policy_digest": profile.resource_policy_digest,
            "stage": self.invocation.stage,
            "admission_checked": True,
            "all_chunks_checked": True,
            "checkpoints": self.checkpoints,
            "chunks_completed": sum(item["kind"] == "chunk" for item in self.checkpoints),
            "peak_owned_private_commit_bytes": self.peak,
            "effective_rung": effective,
            "memory_control_semantics": {
                "factor_h5": "bounded_date_slice_plus_row_group_rows_v1",
                "h5_batch": "reserved_profile_telemetry_not_consumed_v1",
                "minute_batch": "child_manifest_plus_parent_bound_v1",
            },
        }

    def _record(self, kind: str, *, chunk_id: str | None = None) -> None:
        available = int(psutil.virtual_memory().available)
        process = psutil.Process()
        details = process.memory_full_info()
        owned = int(getattr(details, "uss", details.rss))
        self.peak = max(self.peak, owned)
        item: dict[str, Any] = {
            "sequence": len(self.checkpoints),
            "kind": kind,
            "decision": "READY",
            "pressure_rung": self.invocation.pressure_rung,
            "host_available_bytes": available,
            "owned_private_commit_bytes": owned,
        }
        if chunk_id is not None:
            item["chunk_id"] = chunk_id
        self.checkpoints.append(item)


def run_build_stage(
    invocation: BuildStageInvocation,
    *,
    checkpoint: Callable[[], None] = lambda: None,
) -> Mapping[str, Any]:
    if invocation.stage not in {"prepare", "finalize-bins", "validate"}:
        raise CandidateBuildStageError("unsupported build stage")
    if invocation.stage_timeout_seconds != invocation.profile.stage_timeouts_seconds["full_build"]:
        raise CandidateBuildStageError("build stage timeout differs from profile")
    _validate_build_transition_preflight(invocation)
    ledger = StageResourceReceipt(invocation)
    if invocation.stage == "prepare":
        evidence = _prepare(invocation, ledger=ledger, checkpoint=checkpoint)
    elif invocation.stage == "finalize-bins":
        evidence = _finalize(invocation, ledger=ledger, checkpoint=checkpoint)
    else:
        evidence = _validate(invocation, ledger=ledger, checkpoint=checkpoint)
    return {
        "schema_version": BUILD_STAGE_RESULT_SCHEMA,
        "stage": invocation.stage,
        "status": "PASS",
        "run_id": invocation.run_id,
        "attempt_id": invocation.attempt_id,
        "attempt_fence": invocation.attempt_fence,
        "release_id": invocation.release_id,
        "release_digest": invocation.release_digest,
        "staging_relative_path": invocation.staging_relative_path,
        "stage_timeout_seconds": invocation.stage_timeout_seconds,
        "resource_receipt": ledger.finish(),
        **evidence,
        "safety": dict(_ZERO_SAFETY),
    }


def _validate_build_transition_preflight(invocation: BuildStageInvocation) -> None:
    raw_actions = invocation.plan.get("actions")
    if not isinstance(raw_actions, list) or any(not isinstance(item, Mapping) for item in raw_actions):
        raise CandidateBuildStageError("action plan rows are missing/invalid")
    observed_digest = digest_named_fields(
        DECISION_SCHEMA_VERSION,
        {
            "actions": sorted(
                (dict(item) for item in raw_actions),
                key=lambda value: (
                    str(value.get("component", "")),
                    str(value.get("partition_key", "")),
                ),
            )
        },
    )
    expected_digest = ensure_sha256(
        str(invocation.plan.get("action_plan_digest", "")),
        field="action_plan_digest",
    )
    if observed_digest != expected_digest:
        raise CandidateBuildStageError("action plan digest differs before build")
    actions = _actions(invocation.plan)
    reuse_rows = [dict(item) for item in raw_actions if isinstance(item.get("frozen_reuse"), Mapping)]
    baseline = invocation.build_inputs.get("baseline")
    if not isinstance(baseline, Mapping):
        if reuse_rows:
            raise CandidateBuildStageError("build baseline authority is missing")
        return
    declared_reuse = baseline.get("reuse_evidence")
    if not isinstance(declared_reuse, list) or any(not isinstance(item, Mapping) for item in declared_reuse):
        raise CandidateBuildStageError("baseline reuse evidence is invalid")

    def ordered(values):
        return sorted(
            (dict(item) for item in values),
            key=lambda value: (
                str(value.get("component", "")),
                str(value.get("partition_key", "")),
            ),
        )

    if ordered(declared_reuse) != ordered(reuse_rows):
        raise CandidateBuildStageError("baseline reuse evidence differs from immutable action plan")
    if not reuse_rows:
        return
    manifest = load_component_artifact_manifest(invocation.cas, baseline.get("component_artifact_manifest_ref"))
    expected_manifest_fields = {
        "candidate_identity": manifest.candidate_identity,
        "artifact_root": manifest.artifact_root,
        "profile": manifest.profile,
        "scope": manifest.scope,
        "cutoff": manifest.cutoff.isoformat(),
        "semantic_profile_digest": manifest.semantic_profile_digest,
        "producer_fingerprint": manifest.producer_fingerprint,
        "artifact_fingerprint": manifest.artifact_fingerprint,
        "validation_fingerprint": manifest.validation_fingerprint,
        "source_content_root": manifest.source_content_root,
        "artifact_ready_content_root": manifest.artifact_ready_content_root,
        "pit_snapshot_digest": manifest.pit_snapshot_digest,
    }
    if any(baseline.get(field) != value for field, value in expected_manifest_fields.items()):
        raise CandidateBuildStageError("baseline catalog identity differs from component artifact manifest")
    if (
        manifest.profile != invocation.profile.profile
        or manifest.semantic_profile_digest != invocation.profile.semantic_profile_digest
        or invocation.build_inputs.get("profile") != manifest.profile
        or invocation.build_inputs.get("scope") != manifest.scope
        or date.fromisoformat(str(invocation.build_inputs.get("cutoff"))) < manifest.cutoff
    ):
        raise CandidateBuildStageError("baseline profile/scope/cutoff is incompatible")
    fingerprints = invocation.build_inputs.get("fingerprints")
    if (
        not isinstance(fingerprints, Mapping)
        or fingerprints.get("producer_fingerprint") != manifest.producer_fingerprint
        or fingerprints.get("artifact_fingerprint") != manifest.artifact_fingerprint
        or fingerprints.get("validation_fingerprint") != manifest.validation_fingerprint
    ):
        raise CandidateBuildStageError("baseline producer/artifact/validation fingerprints differ")
    release_id = baseline.get("release_id")
    release_digest = baseline.get("release_digest")
    attestation_key = baseline.get("attestation_key")
    if (
        not isinstance(release_id, str)
        or not release_id.strip()
        or ensure_sha256(str(release_digest), field="baseline_release_digest") != release_digest
        or ensure_sha256(str(attestation_key), field="baseline_attestation_key") != attestation_key
    ):
        raise CandidateBuildStageError("baseline release/attestation identity is invalid")
    seen_components: set[Component] = set()
    for item in reuse_rows:
        component = Component(str(item.get("component")))
        frozen = item.get("frozen_reuse")
        assert isinstance(frozen, Mapping)
        if (
            component in seen_components
            or actions[component] is ComponentAction.FULL_REBUILD
            or frozen.get("source_release_id") != release_id
            or frozen.get("source_release_digest") != release_digest
            or frozen.get("source_attestation_key") != attestation_key
            or frozen.get("component_partition_key") != item.get("partition_key")
            or not isinstance(frozen.get("reuse_mode"), str)
            or not str(frozen.get("reuse_mode")).strip()
            or not isinstance(frozen.get("compatibility_reason"), str)
            or not str(frozen.get("compatibility_reason")).strip()
        ):
            raise CandidateBuildStageError("frozen component release/partition identity differs")
        seen_components.add(component)
    expected_reuse_components = {
        component for component, action in actions.items() if action is not ComponentAction.FULL_REBUILD
    }
    if seen_components != expected_reuse_components:
        raise CandidateBuildStageError("frozen component release identity is incomplete")
    root_relative = str(baseline.get("root_relative_path", ""))
    if normalize_root_relative_path(root_relative) != root_relative:
        raise CandidateBuildStageError("baseline root-relative path is non-canonical")
    candidate_root = _resolve_plain_directory(Path(invocation.candidate_root), label="candidate catalog root")
    baseline_candidate = _resolve_plain_directory(candidate_root / Path(root_relative), label="baseline candidate root")
    if candidate_root not in baseline_candidate.parents:
        raise CandidateBuildStageError("baseline candidate root escapes catalog root")
    for component in seen_components:
        _resolve_plain_directory(
            baseline_candidate / _COMPONENT_ROOT[component],
            label=f"baseline component:{component.value}",
        )


def _prepare(
    invocation: BuildStageInvocation,
    *,
    ledger: StageResourceReceipt,
    checkpoint: Callable[[], None],
) -> Mapping[str, Any]:
    actions = _actions(invocation.plan)
    unsupported = {
        component: action
        for component, action in actions.items()
        if action not in {ComponentAction.FULL_REBUILD, ComponentAction.REUSE}
        and action not in {ComponentAction.INCREMENTAL, ComponentAction.SELECTIVE_REBUILD}
    }
    if unsupported:
        raise CandidateBuildStageError("action plan contains an unsupported non-bounded component action")
    staging = invocation.staging_root
    if staging.exists():
        raise CandidateBuildStageError("new fenced prepare staging already exists")
    staging.mkdir(parents=True, exist_ok=False)
    source, pit = _build_source(invocation)
    toolchain = invocation.profile.qlib_toolchain.build_verified(invocation.project_root)
    trading_days = source.trading_days()
    index_source = _FrozenIndexSource(
        trading_days=trading_days,
        rows=tuple(source.index_rows()),
    )
    reuse_refs: dict[str, CASRef] = {}
    index_action = actions[Component.DOMESTIC_INDEX_CONTEXT]
    if index_action is ComponentAction.FULL_REBUILD:
        index_receipt = IndexContextMaterializer(index_source, definitions=invocation.profile.indices).materialize(
            staging / "index_context",
            cutoff=pit.cutoff,
            row_group_rows=_rung(invocation.profile, "row_group_rows", invocation.pressure_rung),
        )
        index_payload = _index_receipt_payload(index_receipt, staging=staging)
    elif index_action is ComponentAction.REUSE:
        receipt = _clone_reused_component(invocation, Component.DOMESTIC_INDEX_CONTEXT)
        reuse_refs[Component.DOMESTIC_INDEX_CONTEXT.value] = invocation.cas.put_json(receipt)
        index_payload = _existing_index_receipt(staging)
    else:
        index_receipt, receipt = _patch_index_component(
            invocation,
            source=index_source,
            cutoff=pit.cutoff,
        )
        reuse_refs[Component.DOMESTIC_INDEX_CONTEXT.value] = invocation.cas.put_json(receipt)
        index_payload = _index_receipt_payload(index_receipt, staging=staging)
    ledger.chunk("index-context")
    refs: dict[str, CASRef] = {
        "index_receipt": invocation.cas.put_json(index_payload),
        "index_materialization_receipt_file": invocation.cas.put_json(
            _load_component_json(staging / "index_context" / "index_materialization_receipt.json")
        ),
        **{f"reuse_{key}": value for key, value in reuse_refs.items()},
    }
    dump_operations: list[dict[str, Any]] = []
    if actions[Component.DAILY_BIN] is ComponentAction.REUSE:
        receipt = _clone_reused_component(invocation, Component.DAILY_BIN)
        refs["daily_reuse_receipt"] = invocation.cas.put_json(
            _load_component_json(staging / "daily_bin" / "materialization_receipt.json")
        )
        refs["reuse_daily_bin"] = invocation.cas.put_json(receipt)
        ledger.chunk("daily-reuse")
    elif actions[Component.DAILY_BIN] in {
        ComponentAction.INCREMENTAL,
        ComponentAction.SELECTIVE_REBUILD,
    }:
        daily_preparation = _prepare_bin_patch(
            invocation,
            source=source,
            pit=pit,
            dataset="daily_bin",
            toolchain=toolchain,
            checkpoint=checkpoint,
        )
        refs["daily_preparation"] = invocation.cas.put_json(daily_preparation)
        dump_operations.extend(daily_preparation["qlib_dump_operations"])
        ledger.chunk("daily-bounded-patch")
    else:
        transform = CanonicalStockTransformSpec(
            cutoff=pit.cutoff,
            pit_snapshot=pit,
            trading_days=trading_days,
            qfq_denominators=source.qfq_authority,
        )
        daily_metrics = CanonicalStockTransformMetrics("daily_bin")
        daily_rows = CanonicalStockTransformer().transform_daily(
            transform,
            daily_rows=_merged_rows(
                source,
                Component.DAILY_BIN,
                "kline_daily_raw",
                staging=staging,
                key=lambda row: (str(row["ts_code"]), _as_date(row["trade_date"])),
                checkpoint=checkpoint,
            ),
            adj_factor_rows=_merged_rows(
                source,
                Component.DAILY_BIN,
                "adj_factor",
                staging=staging,
                key=lambda row: (str(row["ts_code"]), _as_date(row["trade_date"])),
                checkpoint=checkpoint,
            ),
            stk_limit_rows=_merged_rows(
                source,
                Component.DAILY_BIN,
                "stk_limit",
                staging=staging,
                key=lambda row: (str(row["ts_code"]), _as_date(row["trade_date"])),
                checkpoint=checkpoint,
            ),
            suspend_rows=_merged_rows(
                source,
                Component.DAILY_BIN,
                "suspend_d",
                staging=staging,
                key=lambda row: (
                    str(row["ts_code"]),
                    _as_date(row["trade_date"]),
                    str(row.get("suspend_type", "")),
                ),
                checkpoint=checkpoint,
            ),
            checkpoint=checkpoint,
            metrics=daily_metrics,
        )
        daily_preparation = (
            DailyMinuteCsvPreparer()
            .prepare(
                _bin_spec(
                    invocation,
                    pit=pit,
                    dataset="daily_bin",
                    toolchain=toolchain,
                    index_csv_root=staging / "index_context" / "index_csv",
                ),
                rows=daily_rows,
                checkpoint=checkpoint,
            )
            .receipt
        )
        _remove_component_merge_spool(staging, Component.DAILY_BIN)
        daily_preparation, daily_operation = _prepare_full_batched_dump(
            invocation,
            dataset="daily_bin",
            pit=pit,
            preparation=daily_preparation,
            frozen_trading_days=trading_days,
        )
        refs["daily_preparation"] = invocation.cas.put_json(daily_preparation)
        refs["daily_transform_metrics"] = invocation.cas.put_json(daily_metrics.as_dict())
        dump_operations.append(daily_operation)
        ledger.chunk("daily-csv")
    if actions[Component.MINUTE_BIN] is ComponentAction.REUSE:
        receipt = _clone_reused_component(invocation, Component.MINUTE_BIN)
        refs["minute_reuse_receipt"] = invocation.cas.put_json(
            _load_component_json(staging / "minute_bin" / "materialization_receipt.json")
        )
        refs["reuse_minute_bin"] = invocation.cas.put_json(receipt)
        ledger.chunk("minute-reuse")
    elif actions[Component.MINUTE_BIN] in {
        ComponentAction.INCREMENTAL,
        ComponentAction.SELECTIVE_REBUILD,
    }:
        minute_preparation = _prepare_bin_patch(
            invocation,
            source=source,
            pit=pit,
            dataset="minute_bin",
            toolchain=toolchain,
            checkpoint=checkpoint,
        )
        refs["minute_preparation"] = invocation.cas.put_json(minute_preparation)
        dump_operations.extend(minute_preparation["qlib_dump_operations"])
        ledger.chunk("minute-bounded-patch")
    else:
        minute_days = tuple(value for value in trading_days if value >= invocation.profile.minute_start_date)
        minute_transform = CanonicalStockTransformSpec(
            cutoff=pit.cutoff,
            pit_snapshot=pit,
            trading_days=minute_days,
            qfq_denominators=source.qfq_authority,
        )
        minute_metrics = CanonicalStockTransformMetrics("minute_bin")
        minute_rows = CanonicalStockTransformer().transform_minute(
            minute_transform,
            minute_rows=_merged_rows(
                source,
                Component.MINUTE_BIN,
                "kline_minute_raw",
                staging=staging,
                key=lambda row: (
                    str(row["ts_code"]),
                    _as_datetime(row["trade_time"]),
                    str(row.get("freq", "1m")),
                ),
                checkpoint=checkpoint,
            ),
            adj_factor_rows=_merged_rows(
                source,
                Component.MINUTE_BIN,
                "adj_factor",
                staging=staging,
                key=lambda row: (str(row["ts_code"]), _as_date(row["trade_date"])),
                checkpoint=checkpoint,
            ),
            stk_limit_rows=_merged_rows(
                source,
                Component.MINUTE_BIN,
                "stk_limit",
                staging=staging,
                key=lambda row: (str(row["ts_code"]), _as_date(row["trade_date"])),
                checkpoint=checkpoint,
            ),
            suspend_rows=_merged_rows(
                source,
                Component.MINUTE_BIN,
                "suspend_d",
                staging=staging,
                key=lambda row: (
                    str(row["ts_code"]),
                    _as_date(row["trade_date"]),
                    str(row.get("suspend_type", "")),
                ),
                checkpoint=checkpoint,
            ),
            checkpoint=checkpoint,
            metrics=minute_metrics,
        )
        minute_preparation = (
            DailyMinuteCsvPreparer()
            .prepare(
                _bin_spec(
                    invocation,
                    pit=pit,
                    dataset="minute_bin",
                    toolchain=toolchain,
                    index_csv_root=None,
                ),
                rows=minute_rows,
                checkpoint=checkpoint,
            )
            .receipt
        )
        _remove_component_merge_spool(staging, Component.MINUTE_BIN)
        minute_preparation, minute_operation = _prepare_full_batched_dump(
            invocation,
            dataset="minute_bin",
            pit=pit,
            preparation=minute_preparation,
            frozen_trading_days=trading_days,
        )
        refs["minute_preparation"] = invocation.cas.put_json(minute_preparation)
        refs["minute_transform_metrics"] = invocation.cas.put_json(minute_metrics.as_dict())
        dump_operations.append(minute_operation)
        ledger.chunk("minute-csv")
    if actions[Component.FACTOR_H5_STATIC] is ComponentAction.REUSE:
        receipt = _clone_reused_component(invocation, Component.FACTOR_H5_STATIC)
        factor_receipt = _load_component_json(staging / "factor_bundle" / "factor_checkpoint.json")
        refs["reuse_factor_h5_static"] = invocation.cas.put_json(receipt)
    elif actions[Component.FACTOR_H5_STATIC] is ComponentAction.FULL_REBUILD:
        factor_partitions = tuple(FactorSourcePartition(**item) for item in source.factor_partition_plan())
        produced = FactorPartitionProducer().produce(
            FactorPartitionProducerSpec(
                output_root=staging,
                partitions=factor_partitions,
                pit_snapshot=pit,
                qfq_denominator_authority=source.qfq_authority,
                static_ordered_columns=invocation.profile.static_ordered_columns,
                row_group_rows=_rung(invocation.profile, "row_group_rows", invocation.pressure_rung),
                max_source_partition_rows=max(
                    250_000,
                    invocation.profile.resource_policy.validation_read_chunk_rows,
                ),
                qfq_source_summary=source.qfq_source_summary,
                overlay_summary=source.factor_overlay_summary,
            ),
            reader=_FactorReader(source),
            checkpoint=checkpoint,
        )
        factor_receipt = (
            FactorBundleMaterializer()
            .materialize(
                FactorMaterializationSpec(
                    source_root=produced.source_root,
                    staging_root=staging,
                    chunks=produced.chunks,
                    static_ordered_columns=invocation.profile.static_ordered_columns,
                    row_group_rows=_rung(invocation.profile, "row_group_rows", invocation.pressure_rung),
                ),
                checkpoint=checkpoint,
            )
            .receipt
        )
        _remove_owned_scratch(staging, "factor_source_chunks")
    else:
        factor_receipt, factor_adoption = _patch_factor_component(
            invocation,
            source=source,
            pit=pit,
            checkpoint=checkpoint,
        )
        refs["reuse_factor_h5_static"] = invocation.cas.put_json(factor_adoption)
    refs["factor_receipt"] = invocation.cas.put_json(factor_receipt)
    ledger.chunk("factor-bundle")
    prepare_receipt = invocation.cas.put_json(
        {
            "schema_version": BUILD_PREPARE_RECEIPT_SCHEMA,
            "profile": invocation.profile.profile,
            "cutoff": pit.cutoff.isoformat(),
            "pit_snapshot_digest": pit.spans_sha256,
            "artifact_ready_content_root": source.artifact_ready_content_root,
            "actions": {component.value: action.value for component, action in actions.items()},
            "refs": {key: value.as_dict() for key, value in refs.items()},
            "safety": dict(_ZERO_SAFETY),
        }
    )
    active = sorted(span.ts_code for span in pit.spans if span.eligible_start <= pit.cutoff <= span.eligible_end)
    if not active:
        raise CandidateBuildStageError("no PIT stock is active at consumer cutoff")
    return {
        "prepare_receipt_ref": prepare_receipt.as_dict(),
        "consumer_smoke_instrument": active[0],
        "qlib_dump_operations": dump_operations,
    }


def _finalize(
    invocation: BuildStageInvocation,
    *,
    ledger: StageResourceReceipt,
    checkpoint: Callable[[], None],
) -> Mapping[str, Any]:
    source, pit = _build_source(invocation)
    del source
    prepare_stage = _prerequisite_json(invocation, "prepare")
    prepare = _cas_json(invocation.cas, prepare_stage.get("prepare_receipt_ref"))
    refs = prepare.get("refs")
    if not isinstance(refs, Mapping):
        raise CandidateBuildStageError("prepare receipt refs are missing")
    toolchain = invocation.profile.qlib_toolchain.build_verified(invocation.project_root)
    actions = _actions(invocation.plan)
    finalized: dict[str, CASRef] = {}
    for component, dataset, operation_id in (
        (Component.DAILY_BIN, "daily_bin", "daily"),
        (Component.MINUTE_BIN, "minute_bin", "minute"),
    ):
        if actions[component] is ComponentAction.REUSE:
            receipt = _cas_json(invocation.cas, refs[f"{operation_id}_reuse_receipt"])
            finalized[operation_id] = invocation.cas.put_json(receipt)
            ledger.chunk(f"finalize-{operation_id}-reuse")
            continue
        preparation = _cas_json(invocation.cas, refs[f"{operation_id}_preparation"])
        if actions[component] in {
            ComponentAction.INCREMENTAL,
            ComponentAction.SELECTIVE_REBUILD,
        }:
            receipt = _finalize_bin_patch(
                invocation,
                dataset=dataset,
                pit=pit,
                toolchain=toolchain,
                preparation=preparation,
                checkpoint=checkpoint,
            )
            finalized[operation_id] = invocation.cas.put_json(receipt)
            ledger.chunk(f"finalize-{operation_id}-bounded-patch")
            continue
        child = _prerequisite_json(invocation, f"qlib_dump_{operation_id}")
        private_root = invocation.staging_root / dataset / ".writer-private" / operation_id
        private_qlib = private_root / "qlib"
        working = invocation.staging_root / dataset / ".qlib.working"
        if not private_qlib.is_dir() or working.exists():
            raise CandidateBuildStageError("private Qlib writer output is missing/conflicting")
        manifest_path = private_root / "csv" / "batch_manifest.json"
        batch_manifest = _load_component_json(manifest_path)
        batched_dump = _load_component_json(private_root / "csv" / "batched_dump_receipt.json")
        operations = preparation.get("qlib_dump_operations")
        if not isinstance(operations, list) or len(operations) != 1:
            raise CandidateBuildStageError("full Qlib operation binding is missing")
        operation = operations[0]
        if (
            not isinstance(operation, Mapping)
            or operation.get("dataset") != dataset
            or operation.get("mode") != "batched_full"
            or operation.get("batch_manifest_identity") != batch_manifest.get("manifest_identity")
            or operation.get("batch_manifest_sha256") != _sha256_path(manifest_path)
        ):
            raise CandidateBuildStageError("full Qlib operation binding differs")
        batched_dump = _validate_batched_dump_receipt(
            batch_manifest,
            batched_dump,
            expected_dataset=dataset,
            expected_manifest_identity=str(operation["batch_manifest_identity"]),
        )
        sealed = preparation.get("sealed_canonical_rows")
        if not isinstance(sealed, Mapping):
            raise CandidateBuildStageError("full canonical CSV receipt is missing")
        authority_rewrite = _rewrite_private_qlib_authorities(
            invocation,
            dataset=dataset,
            pit=pit,
            qlib_root=private_qlib,
            sealed=sealed,
            frozen_trading_days=tuple(preparation.get("frozen_trading_days") or ()),
        )
        os.rename(private_qlib, working)
        receipt = (
            DailyMinuteBinFinalizer()
            .finalize(
                _bin_spec(
                    invocation,
                    pit=pit,
                    dataset=dataset,
                    toolchain=toolchain,
                    index_csv_root=(
                        invocation.staging_root / "index_context" / "index_csv" if dataset == "daily_bin" else None
                    ),
                ),
                preparation=preparation,
                supervised_child=child,
                batched_dump={
                    "schema_version": "dataset_release_batched_full_audit_v1",
                    "receipt": batched_dump,
                    "authority_rewrite": authority_rewrite,
                },
                checkpoint=checkpoint,
            )
            .receipt
        )
        _remove_owned_scratch(invocation.staging_root / dataset, ".writer-private")
        finalized[operation_id] = invocation.cas.put_json(receipt)
        ledger.chunk(f"finalize-{operation_id}")
    preparation_receipts: dict[str, CASRef | None] = {}
    materialization_file_receipts: dict[str, CASRef] = {}
    for operation_id, dataset in (
        ("daily", "daily_bin"),
        ("minute", "minute_bin"),
    ):
        path = invocation.staging_root / dataset / "csv_preparation_receipt.json"
        preparation_receipts[operation_id] = (
            invocation.cas.put_json(_load_component_json(path)) if path.is_file() else None
        )
        materialization_file_receipts[operation_id] = invocation.cas.put_json(
            _load_component_json(invocation.staging_root / dataset / "materialization_receipt.json")
        )
    index_manifest = produce_index_context_candidate_manifest(
        candidate_root=invocation.staging_root,
        profile=invocation.profile,
        cutoff=pit.cutoff,
        pit_snapshot=pit,
        release_id=invocation.release_id,
        release_digest=invocation.release_digest,
        source_content_root=str(invocation.build_inputs["source_snapshot"]["raw_source_content_root"]),
        artifact_ready_content_root=str(invocation.build_inputs["artifact_ready_content_root"]),
        producer_fingerprint=str(invocation.build_inputs["fingerprints"]["producer_fingerprint"]),
        artifact_fingerprint=str(invocation.build_inputs["fingerprints"]["artifact_fingerprint"]),
        validation_fingerprint=str(invocation.build_inputs["fingerprints"]["validation_fingerprint"]),
        max_rows=invocation.profile.resource_policy.validation_read_chunk_rows,
    )
    index_manifest_ref = invocation.cas.put_json(index_manifest)
    final_ref = invocation.cas.put_json(
        {
            "schema_version": BUILD_FINALIZE_RECEIPT_SCHEMA,
            "profile": invocation.profile.profile,
            "cutoff": pit.cutoff.isoformat(),
            "daily_receipt_ref": finalized["daily"].as_dict(),
            "minute_receipt_ref": finalized["minute"].as_dict(),
            "daily_materialization_receipt_file_ref": (materialization_file_receipts["daily"].as_dict()),
            "minute_materialization_receipt_file_ref": (materialization_file_receipts["minute"].as_dict()),
            "daily_preparation_receipt_ref": (
                preparation_receipts["daily"].as_dict() if preparation_receipts["daily"] is not None else None
            ),
            "minute_preparation_receipt_ref": (
                preparation_receipts["minute"].as_dict() if preparation_receipts["minute"] is not None else None
            ),
            "index_context_manifest_ref": index_manifest_ref.as_dict(),
            "safety": dict(_ZERO_SAFETY),
        }
    )
    return {"finalize_receipt_ref": final_ref.as_dict()}


def _validate(
    invocation: BuildStageInvocation,
    *,
    ledger: StageResourceReceipt,
    checkpoint: Callable[[], None],
) -> Mapping[str, Any]:
    source, pit = _build_source(invocation)
    prepare_stage = _prerequisite_json(invocation, "prepare")
    prepare = _cas_json(invocation.cas, prepare_stage["prepare_receipt_ref"])
    refs = prepare["refs"]
    finalized_stage = _prerequisite_json(invocation, "finalize_bins")
    finalized = _cas_json(invocation.cas, finalized_stage["finalize_receipt_ref"])
    daily_receipt = _cas_json(invocation.cas, finalized["daily_receipt_ref"])
    minute_receipt = _cas_json(invocation.cas, finalized["minute_receipt_ref"])
    daily_materialization_receipt_file = _cas_json(
        invocation.cas,
        finalized["daily_materialization_receipt_file_ref"],
    )
    minute_materialization_receipt_file = _cas_json(
        invocation.cas,
        finalized["minute_materialization_receipt_file_ref"],
    )
    daily_preparation_receipt = (
        _cas_json(invocation.cas, finalized["daily_preparation_receipt_ref"])
        if finalized.get("daily_preparation_receipt_ref") is not None
        else None
    )
    minute_preparation_receipt = (
        _cas_json(invocation.cas, finalized["minute_preparation_receipt_ref"])
        if finalized.get("minute_preparation_receipt_ref") is not None
        else None
    )
    factor_receipt = _cas_json(invocation.cas, refs["factor_receipt"])
    index_receipt = _cas_json(invocation.cas, refs["index_receipt"])
    index_materialization_receipt_file = _cas_json(
        invocation.cas,
        refs["index_materialization_receipt_file"],
    )
    consumer = _prerequisite_json(invocation, "consumer_smoke")
    actions = _actions(invocation.plan)
    transition_authority: dict[str, CandidateComponentTransitionAuthority] = {}
    for component in Component:
        action = actions[component]
        action_entry = _action_entry(invocation, component)
        if action is ComponentAction.FULL_REBUILD:
            authority = CandidateComponentTransitionAuthority(
                component=component,
                action=action,
                action_entry=action_entry,
            )
        else:
            baseline_root, _manifest, baseline_evidence, frozen = _baseline_component(
                invocation, component, verify_merkle=False
            )
            authority = CandidateComponentTransitionAuthority(
                component=component,
                action=action,
                action_entry=action_entry,
                frozen_reuse=frozen,
                baseline_component_root=baseline_root,
                baseline_evidence=baseline_evidence,
            )
        transition_authority[component.value] = authority
    component_fingerprints = {
        component.value: digest_named_fields(
            "dataset_release_component_build_fingerprint_v1",
            {
                "component": component.value,
                "action": actions[component].value,
                "effective_partitions": invocation.build_inputs["artifact_ready_effective_partitions"][component.value],
            },
        )
        for component in Component
    }
    report = CandidateValidator().validate(
        CandidateValidationSpec(
            candidate_root=invocation.staging_root,
            profile=invocation.profile,
            cutoff=pit.cutoff,
            trading_dates=source.trading_days(),
            pit_snapshot=pit,
            factor_receipt=factor_receipt,
            daily_receipt=daily_receipt,
            minute_receipt=minute_receipt,
            daily_materialization_receipt_file=(daily_materialization_receipt_file),
            minute_materialization_receipt_file=(minute_materialization_receipt_file),
            daily_preparation_receipt=daily_preparation_receipt,
            minute_preparation_receipt=minute_preparation_receipt,
            minute_canonical_source=minute_receipt["sealed_canonical_rows"],
            index_receipt=index_receipt,
            index_materialization_receipt_file=(index_materialization_receipt_file),
            external_consumer_smoke=consumer,
            minute_overlay_summary=source.minute_overlay_summary,
            actions={key.value: value.value for key, value in actions.items()},
            component_fingerprints=component_fingerprints,
            validation_fingerprint=str(invocation.build_inputs["fingerprints"]["validation_fingerprint"]),
            action_plan_digest=str(invocation.plan["action_plan_digest"]),
            transition_authority=transition_authority,
            require_production_consumer_smoke=bool(
                invocation.build_inputs.get("require_production_consumer_smoke", True)
            ),
        )
    )
    index_context_manifest = validate_index_context_candidate_manifest(
        invocation.staging_root,
        profile=invocation.profile,
        cutoff=pit.cutoff,
        pit_snapshot_digest=pit.spans_sha256,
        producer_fingerprint=str(invocation.build_inputs["fingerprints"]["producer_fingerprint"]),
        artifact_fingerprint=str(invocation.build_inputs["fingerprints"]["artifact_fingerprint"]),
        validation_fingerprint=str(invocation.build_inputs["fingerprints"]["validation_fingerprint"]),
        expected_release_id=invocation.release_id,
        expected_release_digest=invocation.release_digest,
        expected_source_content_root=str(invocation.build_inputs["source_snapshot"]["raw_source_content_root"]),
        expected_artifact_ready_content_root=str(invocation.build_inputs["artifact_ready_content_root"]),
        max_rows=invocation.profile.resource_policy.validation_read_chunk_rows,
    )
    report.payload["evidence"]["index_context_candidate_manifest"] = {
        "manifest_identity": index_context_manifest["manifest_identity"],
        "hmm_consumer_activation": index_context_manifest["hmm_consumer_activation"],
    }
    ledger.chunk("candidate-validation")
    artifact_snapshot = report.artifact_snapshot
    artifact_root = artifact_snapshot.artifact_root
    candidate_identity = _candidate_identity(
        invocation,
        artifact_root=artifact_root,
        pit_snapshot=pit,
    )
    component_ref = produce_component_artifact_manifest(
        invocation.cas,
        candidate_root=invocation.staging_root,
        profile=invocation.profile,
        scope=str(invocation.build_inputs["scope"]),
        cutoff=pit.cutoff,
        candidate_identity=candidate_identity,
        artifact_root=artifact_root,
        producer_fingerprint=str(invocation.build_inputs["fingerprints"]["producer_fingerprint"]),
        artifact_fingerprint=str(invocation.build_inputs["fingerprints"]["artifact_fingerprint"]),
        validation_fingerprint=str(invocation.build_inputs["fingerprints"]["validation_fingerprint"]),
        source_content_root=str(invocation.build_inputs["source_snapshot"]["raw_source_content_root"]),
        artifact_ready_content_root=str(invocation.build_inputs["artifact_ready_content_root"]),
        pit_snapshot=pit,
        source_partitions={component: source.source_partition_evidence(component) for component in Component},
        qfq_authority=source.qfq_authority,
        require_index_context_candidate_manifest=True,
        artifact_snapshot=artifact_snapshot,
    )
    component_manifest = _cas_json(invocation.cas, component_ref)
    validation_ref = invocation.cas.put_json(report.payload)
    candidate_manifest = _build_candidate_manifest(
        invocation=invocation,
        pit=pit,
        artifact_root=artifact_root,
        component_manifest=component_manifest,
        component_ref=component_ref,
    )
    manifest_ref = invocation.cas.put_json(candidate_manifest)
    return {
        "validation_status": "PASS",
        "required_validation_failures": 0,
        "validation_ref": validation_ref.as_dict(),
        "manifest_ref": manifest_ref.as_dict(),
        "artifact_root": artifact_root,
        "manifest_root": component_manifest["manifest_root"],
        "component_artifact_manifest_ref": component_ref.as_dict(),
        "artifact_ready_content_root": invocation.build_inputs["artifact_ready_content_root"],
        "artifact_ready_contract_ref": invocation.build_inputs["artifact_ready_contract_ref"],
        "artifact_ready_provenance_root": invocation.build_inputs["artifact_ready_provenance_root"],
        "producer_provenance_digest": invocation.build_inputs["fingerprints"]["producer_fingerprint"],
        "artifact_snapshot": artifact_snapshot.receipt(),
        "runtime_real_data_evidence": "not_run_not_authorized",
    }


def _build_candidate_manifest(
    *,
    invocation: BuildStageInvocation,
    pit: FrozenPitSnapshot,
    artifact_root: str,
    component_manifest: Mapping[str, Any],
    component_ref: CASRef,
) -> dict[str, Any]:
    candidate_manifest: dict[str, Any] = {
        "schema_version": DATASET_CANDIDATE_MANIFEST_SCHEMA,
        "profile": invocation.profile.profile,
        "scope": str(invocation.build_inputs["scope"]),
        "cutoff": pit.cutoff.isoformat(),
        "release_id": invocation.release_id,
        "release_digest": invocation.release_digest,
        "semantic_profile_digest": invocation.profile.semantic_profile_digest,
        "source_content_root": invocation.build_inputs["source_snapshot"]["raw_source_content_root"],
        "pit_snapshot_digest": pit.spans_sha256,
        "artifact_root": artifact_root,
        "manifest_root": component_manifest["manifest_root"],
        "component_artifact_manifest_ref": component_ref.as_dict(),
        "artifact_ready_content_root": invocation.build_inputs["artifact_ready_content_root"],
        "safety": {
            "database_writes": 0,
            "production_writes": 0,
            "production_deletes": 0,
            "production_pointer_changes": 0,
            "service_process_controls": 0,
        },
    }
    if invocation.profile.pit_authority_status == "ACTIVE_CANONICAL":
        pit_binding = build_dataset_pit_binding(
            pit,
            release_id=invocation.release_id,
            rolling_cutoff_spans_sha256=str(invocation.build_inputs["source_snapshot"]["pit_snapshot_digest"]),
            scope=str(invocation.build_inputs["scope"]),
        )
        candidate_manifest["pit_binding"] = pit_binding.as_dict()
    if "initial_migration_plan" in invocation.build_inputs:
        candidate_manifest["initial_migration_plan"] = dict(invocation.build_inputs["initial_migration_plan"])
    return candidate_manifest


def _build_source(
    invocation: BuildStageInvocation,
) -> tuple[ArtifactReadyBuildSource, FrozenPitSnapshot]:
    build = invocation.build_inputs
    pit_value = _cas_json(invocation.cas, build["pit_snapshot_ref"])
    pit = frozen_pit_snapshot_from_mapping(pit_value)
    source = ArtifactReadyBuildSource(
        cas=invocation.cas,
        profile=invocation.profile,
        cutoff=pit.cutoff,
        pit_snapshot=pit,
        source_content_root=str(build["source_snapshot"]["raw_source_content_root"]),
        source_partitions=tuple(build["partitions"]),
        artifact_ready_contract_ref=build["artifact_ready_contract_ref"],
    )
    if source.artifact_ready_content_root != build["artifact_ready_content_root"]:
        raise CandidateBuildStageError("artifact-ready effective root differs")
    return source, pit


def _actions(plan: Mapping[str, Any]) -> dict[Component, ComponentAction]:
    values = plan.get("actions")
    if not isinstance(values, list):
        raise CandidateBuildStageError("action plan is missing")
    output: dict[Component, ComponentAction] = {}
    for value in values:
        if not isinstance(value, Mapping):
            raise CandidateBuildStageError("component action is invalid")
        component = Component(str(value.get("component")))
        action = ComponentAction(str(value.get("action")))
        if component in output:
            raise CandidateBuildStageError("component action is duplicated")
        output[component] = action
    if set(output) != set(Component):
        raise CandidateBuildStageError("component actions are incomplete")
    return output


_COMPONENT_ROOT = {
    Component.DAILY_BIN: "daily_bin",
    Component.MINUTE_BIN: "minute_bin",
    Component.FACTOR_H5_STATIC: "factor_bundle",
    Component.DOMESTIC_INDEX_CONTEXT: "index_context",
}


def _action_entry(invocation: BuildStageInvocation, component: Component) -> Mapping[str, Any]:
    values = invocation.plan.get("actions")
    if not isinstance(values, list):
        raise CandidateBuildStageError("action plan is missing")
    matches = [value for value in values if isinstance(value, Mapping) and value.get("component") == component.value]
    if len(matches) != 1:
        raise CandidateBuildStageError("component action evidence is missing/ambiguous")
    return matches[0]


def _baseline_component(
    invocation: BuildStageInvocation,
    component: Component,
    *,
    verify_merkle: bool = True,
) -> tuple[Path, ComponentArtifactManifest, ComponentArtifactEvidence, Mapping[str, Any]]:
    baseline = invocation.build_inputs.get("baseline")
    action = _action_entry(invocation, component)
    frozen = action.get("frozen_reuse")
    if not isinstance(baseline, Mapping) or not isinstance(frozen, Mapping):
        raise CandidateBuildStageError("component reuse authority is missing")
    manifest = load_component_artifact_manifest(invocation.cas, baseline.get("component_artifact_manifest_ref"))
    evidence = manifest.component(component)
    relative = _COMPONENT_ROOT[component]
    root_relative = str(baseline.get("root_relative_path", "")).replace("\\", "/")
    if (
        not evidence.complete
        or evidence.component_root_relative_path != relative
        or frozen.get("component_root_relative_path") != relative
        or frozen.get("manifest_root") != evidence.filesystem_tree_merkle
        or frozen.get("artifact_id") != evidence.component_identity
        or frozen.get("file_identity") != evidence.file_identity
        or baseline.get("artifact_root") != manifest.artifact_root
        or manifest.profile != invocation.profile.profile
        or manifest.semantic_profile_digest != invocation.profile.semantic_profile_digest
        or not root_relative
        or Path(root_relative).is_absolute()
        or ".." in Path(root_relative).parts
    ):
        raise CandidateBuildStageError("frozen component reuse identity differs")
    candidate_root = _resolve_plain_directory(Path(invocation.candidate_root), label="candidate catalog root")
    candidate = _resolve_plain_directory(candidate_root / Path(root_relative), label="baseline candidate root")
    if candidate_root not in candidate.parents:
        raise CandidateBuildStageError("baseline candidate path escapes candidate root")
    component_root = _resolve_plain_directory(candidate / relative, label=f"baseline component:{component.value}")
    if candidate not in component_root.parents:
        raise CandidateBuildStageError("baseline component root is missing")
    if verify_merkle:
        actual_merkle = tree_merkle(component_root)[1]
        if actual_merkle != evidence.filesystem_tree_merkle:
            raise CandidateBuildStageError("baseline component Merkle differs")
    return component_root, manifest, evidence, frozen


def _clone_reused_component(invocation: BuildStageInvocation, component: Component) -> Mapping[str, Any]:
    source, _manifest, evidence, frozen = _baseline_component(
        invocation,
        component,
        verify_merkle=False,
    )
    if (
        tuple(frozen.get("replace_existing_targets") or ())
        or tuple(frozen.get("create_new_targets") or ())
        or tuple(frozen.get("mutation_set") or ())
    ):
        raise CandidateBuildStageError("exact REUSE unexpectedly declares mutations")
    return clone_sealed_tree_for_reuse(
        source,
        invocation.staging_root / _COMPONENT_ROOT[component],
        source_sealed=True,
        expected_source_merkle=str(evidence.filesystem_tree_merkle),
    )


def _patch_factor_component(
    invocation: BuildStageInvocation,
    *,
    source: ArtifactReadyBuildSource,
    pit: FrozenPitSnapshot,
    checkpoint: Callable[[], None],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    component = Component.FACTOR_H5_STATIC
    baseline, manifest, evidence, frozen = _baseline_component(
        invocation,
        component,
        verify_merkle=False,
    )
    action = ComponentAction(str(_action_entry(invocation, component)["action"]))
    if action not in {
        ComponentAction.INCREMENTAL,
        ComponentAction.SELECTIVE_REBUILD,
    }:
        raise CandidateBuildStageError("factor patch action is invalid")
    replace = tuple(str(value) for value in frozen.get("replace_existing_targets") or ())
    create = tuple(str(value) for value in frozen.get("create_new_targets") or ())
    if not replace or set(frozen.get("mutation_set") or ()) != set((*replace, *create)):
        raise CandidateBuildStageError("factor patch mutation authority differs")
    deferred_aggregates = tuple(
        value
        for value in replace
        if "/" not in value.replace("\\", "/")
        and (value.casefold().endswith(".h5") or value.replace("\\", "/").casefold() == "static_factors.parquet")
    )
    plan = prepare_copy_on_write_tree(
        baseline,
        invocation.staging_root / "factor_bundle",
        replace_existing_targets=replace,
        create_new_targets=create,
        defer_replace_targets=deferred_aggregates,
        source_sealed=True,
        expected_source_merkle=str(evidence.filesystem_tree_merkle),
    )
    all_partitions = {
        str(item["partition_key"]): FactorSourcePartition(**item) for item in source.factor_partition_plan()
    }
    selected_months = _factor_patch_months(frozen)
    selected = [all_partitions[month] for month in selected_months if month in all_partitions]
    if len(selected) != len(selected_months) or not selected:
        raise CandidateBuildStageError("factor patch output months differ from source plan")
    scratch = invocation.staging_root / ".factor-mixed"
    scratch.mkdir(exist_ok=False)
    tail_months = _factor_tail_months(frozen)
    affected_codes, unbounded_months = _factor_affected_scope(frozen, selected_months=selected_months)
    produced_chunks: dict[tuple[str, str], tuple[Path, SealedFactorChunk, tuple[str, ...]]] = {}
    production_receipts: list[Mapping[str, Any]] = []
    row_group_rows = _rung(invocation.profile, "row_group_rows", invocation.pressure_rung)
    rolling_state = None
    previous_partition: FactorSourcePartition | None = None
    previous_filter: tuple[str, ...] | None = None
    for partition in selected:
        instrument_filter = (
            ()
            if action is ComponentAction.INCREMENTAL
            or partition.partition_key in tail_months
            or partition.partition_key in unbounded_months
            else affected_codes
        )
        if (
            action is ComponentAction.SELECTIVE_REBUILD
            and not instrument_filter
            and (partition.partition_key not in tail_months and partition.partition_key not in unbounded_months)
        ):
            raise CandidateBuildStageError("factor selective partition lacks code/full-month authority")
        run_root = scratch / "producer" / partition.partition_key
        run_root.mkdir(parents=True, exist_ok=False)
        contiguous = previous_partition is not None and partition.start == previous_partition.end + date.resolution
        if contiguous and previous_filter == instrument_filter:
            initial = rolling_state
        elif contiguous and previous_filter and not instrument_filter and rolling_state is not None:
            baseline_full = restore_rolling_factor_state_from_bundle(
                baseline,
                qfq_denominator_authority=source.qfq_authority,
                before=partition.start,
                max_rows=row_group_rows,
            )
            initial = merge_rolling_factor_states_by_instrument(
                baseline_full,
                rolling_state,
                affected_instruments=previous_filter,
            )
        elif partition.start <= invocation.profile.start_date:
            initial = None
        else:
            initial = restore_rolling_factor_state_from_bundle(
                baseline,
                qfq_denominator_authority=source.qfq_authority,
                before=partition.start,
                max_rows=row_group_rows,
                instrument_filter=instrument_filter,
            )
        produced = FactorPartitionProducer().produce(
            FactorPartitionProducerSpec(
                output_root=run_root,
                partitions=(partition,),
                pit_snapshot=pit,
                qfq_denominator_authority=source.qfq_authority,
                static_ordered_columns=invocation.profile.static_ordered_columns,
                row_group_rows=row_group_rows,
                max_source_partition_rows=max(
                    250_000,
                    invocation.profile.resource_policy.validation_read_chunk_rows,
                ),
                qfq_source_summary=source.qfq_source_summary,
                overlay_summary=source.factor_overlay_summary,
                allow_partial_ranges=True,
                instrument_filter=instrument_filter,
            ),
            reader=_FactorReader(source, instrument_filter=instrument_filter),
            initial_state=initial,
            checkpoint=checkpoint,
        )
        production_receipts.append(produced.receipt)
        for chunk in produced.chunks:
            produced_chunks[(chunk.dataset, chunk.partition_key)] = (
                produced.source_root,
                chunk,
                instrument_filter,
            )
        rolling_state = restore_rolling_factor_state_from_produced_partition(
            produced.source_root,
            partition_key=partition.partition_key,
            max_rows=row_group_rows,
        )
        previous_partition = partition
        previous_filter = instrument_filter
    baseline_checkpoint = _load_component_json(baseline / "factor_checkpoint.json")
    raw_chunks = baseline_checkpoint.get("chunks")
    if not isinstance(raw_chunks, list) or not raw_chunks:
        raise CandidateBuildStageError("baseline factor chunk receipt is missing")
    combined = scratch / "combined-source"
    combined.mkdir()
    chunks: dict[tuple[str, str], SealedFactorChunk] = {}
    baseline_paths: dict[tuple[str, str], Path] = {}
    baseline_candidate = baseline.parent
    for raw in raw_chunks:
        if not isinstance(raw, Mapping):
            raise CandidateBuildStageError("baseline factor chunk receipt is invalid")
        key = (str(raw["dataset"]), str(raw["partition_key"]))
        source_path = (baseline_candidate / Path(str(raw["candidate_relative_path"]))).resolve(strict=True)
        baseline_paths[key] = source_path
        target = combined / key[0] / f"{key[1]}.parquet"
        target.parent.mkdir(exist_ok=True)
        os.link(source_path, target)
        chunks[key] = SealedFactorChunk(
            dataset=key[0],
            partition_key=key[1],
            relative_path=target.relative_to(combined).as_posix(),
            sha256=str(raw["sha256"]),
            rows=int(raw["rows"]),
            ordered_columns=tuple(str(value) for value in raw["ordered_columns"]),
        )
    merge_receipts: list[Mapping[str, Any]] = []
    for key, (produced_root, chunk, instrument_filter) in produced_chunks.items():
        target = combined / key[0] / f"{key[1]}.parquet"
        target.parent.mkdir(exist_ok=True)
        target.unlink(missing_ok=True)
        source_path = (produced_root / chunk.relative_path).resolve(strict=True)
        if instrument_filter:
            baseline_path = baseline_paths.get(key)
            if baseline_path is None:
                raise CandidateBuildStageError("selective factor merge lacks baseline month")
            merged, merge_receipt = merge_factor_partition_by_instrument(
                baseline_path=baseline_path,
                replacement_path=source_path,
                target_path=target,
                dataset=chunk.dataset,
                partition_key=chunk.partition_key,
                affected_instruments=instrument_filter,
                row_group_rows=row_group_rows,
                max_rows=max(
                    250_000,
                    invocation.profile.resource_policy.validation_read_chunk_rows,
                ),
            )
            chunks[key] = SealedFactorChunk(
                dataset=merged.dataset,
                partition_key=merged.partition_key,
                relative_path=target.relative_to(combined).as_posix(),
                sha256=merged.sha256,
                rows=merged.rows,
                ordered_columns=merged.ordered_columns,
            )
            merge_receipts.append(merge_receipt)
        else:
            os.link(source_path, target)
            chunks[key] = SealedFactorChunk(
                dataset=chunk.dataset,
                partition_key=chunk.partition_key,
                relative_path=target.relative_to(combined).as_posix(),
                sha256=chunk.sha256,
                rows=chunk.rows,
                ordered_columns=chunk.ordered_columns,
            )
    output_stage = scratch / "aggregate-output"
    output_stage.mkdir()
    receipt = (
        FactorBundleMaterializer()
        .materialize(
            FactorMaterializationSpec(
                source_root=combined,
                staging_root=output_stage,
                chunks=tuple(chunks[key] for key in sorted(chunks)),
                static_ordered_columns=invocation.profile.static_ordered_columns,
                row_group_rows=row_group_rows,
            ),
            checkpoint=checkpoint,
        )
        .receipt
    )
    patch_root = output_stage / "factor_bundle"
    receipt = _remap_factor_receipt(
        receipt,
        staging_root=invocation.staging_root,
    )
    (patch_root / "factor_checkpoint.json").write_text(
        json.dumps(
            receipt,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    patch_merkle = tree_merkle(patch_root)[1]
    nondeferred = plan.mutation_paths.difference(plan.deferred_existing_paths)
    ordinary_adoption = adopt_isolated_writer_patch(
        plan,
        patch_root,
        patch_targets={path: path for path in nondeferred},
    )
    deferred_adoption = adopt_deferred_writer_outputs(
        plan,
        patch_root,
        patch_targets={path: path for path in plan.deferred_existing_paths},
        baseline_copy_count=0,
    )
    targets = writer_target_manifest(plan)
    final_merkle = tree_merkle(invocation.staging_root / "factor_bundle")[1]
    if final_merkle != patch_merkle or targets["target_merkle"] != final_merkle:
        raise CandidateBuildStageError("factor mixed aggregate adoption differs")
    actual_work = {
        "action": action.value,
        "recomputed_source_partitions": selected_months,
        "recomputed_partition_count": len(selected_months),
        "reused_partition_count": len(chunks) - len(produced_chunks),
        "whole_file_aggregate_rewrite": True,
        "aggregate_inputs_streamed": len(chunks),
        "whole_market_frames_retained": 0,
        "deferred_aggregate_targets": sorted(plan.deferred_existing_paths),
        "aggregate_baseline_copy_count": deferred_adoption["baseline_copy_count"],
        "aggregate_final_recopy_count": deferred_adoption["final_recopy_count"],
        "producer_receipts": production_receipts,
        "affected_instruments": list(affected_codes),
        "tail_full_market_months": sorted(tail_months),
        "unbounded_full_market_months": sorted(unbounded_months),
        "selective_partition_merges": merge_receipts,
    }
    receipt = {**receipt, "mixed_actual_work": actual_work}
    # Keep the final durable checkpoint byte-identical to the receipt returned
    # to the build stage after adding actual-work evidence.
    atomic_write_mutation(
        plan,
        "factor_checkpoint.json",
        (
            json.dumps(
                receipt,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8"),
    )
    targets = writer_target_manifest(plan)
    baseline_after = tree_merkle(baseline)[1]
    if baseline_after != evidence.filesystem_tree_merkle:
        raise CandidateBuildStageError("factor patch changed baseline component")
    _remove_owned_scratch(invocation.staging_root, ".factor-mixed")
    return receipt, {
        "schema_version": "dataset_release_incremental_component_adoption_v1",
        "component": component.value,
        "action": action.value,
        "baseline_cutoff": manifest.cutoff.isoformat(),
        "cutoff": pit.cutoff.isoformat(),
        "copy_on_write": plan.receipt(),
        "adoption": {
            "ordinary": ordinary_adoption,
            "deferred_aggregates": deferred_adoption,
        },
        "writer_target_manifest": targets,
        "actual_work": actual_work,
        "baseline_merkle_after": baseline_after,
        "safety": dict(_ZERO_SAFETY),
    }


@dataclass(frozen=True, slots=True)
class _BinPatchScope:
    tail_date_ranges: tuple[tuple[date, date], ...]
    override_date_ranges: tuple[tuple[date, date], ...]
    new_date_ranges: tuple[tuple[date, date], ...]
    tail_stock_codes: tuple[str, ...]
    tail_index_codes: tuple[str, ...]
    override_stock_codes: tuple[str, ...]
    override_index_codes: tuple[str, ...]
    new_stock_codes: tuple[str, ...]
    delta_key: str | None
    override_key: str | None
    pit_authority_changed: bool


def _prepare_bin_patch(
    invocation: BuildStageInvocation,
    *,
    source: ArtifactReadyBuildSource,
    pit: FrozenPitSnapshot,
    dataset: str,
    toolchain,
    checkpoint: Callable[[], None],
) -> Mapping[str, Any]:
    component = Component.DAILY_BIN if dataset == "daily_bin" else Component.MINUTE_BIN
    baseline, manifest, evidence, frozen = _baseline_component(
        invocation,
        component,
        verify_merkle=False,
    )
    action = ComponentAction(str(_action_entry(invocation, component)["action"]))
    if action not in {
        ComponentAction.INCREMENTAL,
        ComponentAction.SELECTIVE_REBUILD,
    }:
        raise CandidateBuildStageError("bounded bin action is invalid")
    replace = tuple(str(value) for value in frozen.get("replace_existing_targets") or ())
    create = tuple(str(value) for value in frozen.get("create_new_targets") or ())
    if not replace or set(frozen.get("mutation_set") or ()) != set((*replace, *create)):
        raise CandidateBuildStageError("bounded bin mutation authority differs")
    plan = prepare_copy_on_write_tree(
        baseline,
        invocation.staging_root / dataset,
        replace_existing_targets=replace,
        create_new_targets=create,
        defer_replace_targets=(path for path in replace if path.replace("\\", "/").startswith("qlib/")),
        source_sealed=True,
        expected_source_merkle=str(evidence.filesystem_tree_merkle),
    )
    scope = _bin_patch_scope(
        invocation,
        component=component,
        frozen=frozen,
        cutoff=pit.cutoff,
    )
    private = invocation.staging_root / dataset / ".writer-private" / dataset.removesuffix("_bin")
    private.mkdir(parents=True, exist_ok=False)
    private_qlib = private / "qlib"
    # Both tail and historical fix phases operate on one private baseline.
    # This is the sole full Qlib copy; final adoption atomically moves only the
    # declared writer targets and never recopies the full candidate.
    _copy_private_tree(baseline / "qlib", private_qlib)
    baseline_receipt = _load_component_json(baseline / "materialization_receipt.json")
    baseline_sealed = baseline_receipt.get("sealed_canonical_rows")
    if not isinstance(baseline_sealed, Mapping):
        raise CandidateBuildStageError("baseline canonical CSV receipt is missing")
    phase_specs = (
        (
            "tail",
            scope.tail_date_ranges,
            scope.tail_stock_codes,
            scope.tail_index_codes,
            private / "csv_source_tail",
            True,
        ),
        (
            "new",
            scope.new_date_ranges,
            scope.new_stock_codes,
            (),
            private / "csv_source_new",
            False,
        ),
        (
            "override",
            scope.override_date_ranges,
            scope.override_stock_codes,
            scope.override_index_codes,
            private / "csv_source_override",
            False,
        ),
    )
    phases: dict[str, Mapping[str, Any]] = {}
    metrics: dict[str, Mapping[str, Any]] = {}
    phase_roots: dict[str, Path] = {}
    for phase_id, ranges, stocks, indices, output_root, use_seed in phase_specs:
        if not stocks and not indices:
            continue
        patch, phase_metrics = _prepare_bin_patch_phase(
            invocation,
            source=source,
            pit=pit,
            component=component,
            dataset=dataset,
            toolchain=toolchain,
            baseline_candidate_root=baseline.parent,
            baseline_sealed=baseline_sealed,
            date_ranges=ranges,
            stock_codes=stocks,
            index_codes=indices,
            output_root=output_root,
            seed_from_baseline=use_seed,
            checkpoint=checkpoint,
        )
        phases[phase_id] = patch
        metrics[phase_id] = phase_metrics
        phase_roots[phase_id] = output_root
    if not phases:
        raise CandidateBuildStageError("bounded bin patch contains no phase")
    batch_manifest = _prepare_csv_batches(
        private / "csv",
        phases=tuple(
            (
                phase_id,
                "override" if phase_id == "override" else "tail",
                phase_roots[phase_id],
                phases[phase_id],
            )
            for phase_id in ("tail", "new", "override")
            if phase_id in phases
        ),
        dataset=dataset,
        max_codes=min(
            invocation.profile.resource_policy.minute_code_batch_size,
            _rung(
                invocation.profile,
                "minute_batch",
                invocation.pressure_rung,
            ),
        ),
        timeout_seconds=min(
            1800,
            int(invocation.profile.stage_timeouts_seconds["qlib_dump"]),
        ),
        attempt_id=invocation.attempt_id,
        attempt_fence=invocation.attempt_fence,
        execution_id=f"build-dump-{dataset.removesuffix('_bin')}",
    )
    lineage_plan = frozen.get("canonical_lineage")
    legacy_plan = lineage_plan is None
    legacy_composite: Mapping[str, Any] | None = None
    if legacy_plan:
        legacy_composite = build_composite_canonical_rows(
            dataset=dataset,
            baseline=baseline_sealed,
            patch_preparation={"csv": {"files": [], "rows": 0}},
            delta_root_relative_path="unused",
        )
        if "new" in phases:
            legacy_composite = build_composite_canonical_rows(
                dataset=dataset,
                baseline=legacy_composite,
                patch_preparation=phases["new"],
                delta_root_relative_path=f"{dataset}/csv",
            )
        if "tail" in phases:
            if scope.delta_key is None:
                raise CandidateBuildStageError("tail phase lacks a delta key")
            legacy_composite = build_composite_canonical_rows(
                dataset=dataset,
                baseline=legacy_composite,
                patch_preparation=phases["tail"],
                delta_root_relative_path=f"{dataset}/csv_deltas/{scope.delta_key}",
            )
        if "override" in phases and phases["override"]["csv"]["files"]:
            if scope.override_key is None:
                raise CandidateBuildStageError("selective bin patch lacks an override key")
            legacy_composite = build_selective_override_canonical_rows(
                dataset=dataset,
                baseline=legacy_composite,
                patch_preparation=phases["override"],
                override_root_relative_path=(f"{dataset}/csv_overrides/{scope.override_key}"),
                invalidation_scopes=tuple(
                    dict(value) for value in frozen.get("invalidation_scopes") or () if isinstance(value, Mapping)
                ),
            )
        baseline_codes = set()
    elif not isinstance(lineage_plan, Mapping) or lineage_plan.get("capability") != CANONICAL_LINEAGE_CAPABILITY:
        raise CandidateBuildStageError("bounded bin patch has an invalid frozen canonical lineage capability")
    elif is_lineage_v3(baseline_sealed):
        validated_baseline_lineage = validate_lineage_descriptor(
            baseline,
            baseline_sealed,
        )
        if (
            lineage_plan.get("baseline_schema_version") != CANONICAL_LINEAGE_SCHEMA
            or lineage_plan.get("baseline_lineage_root") != validated_baseline_lineage["lineage_root"]
        ):
            raise CandidateBuildStageError("frozen canonical lineage baseline root differs")
        baseline_codes = {
            str(item["instrument"])
            for item in lineage_instrument_summaries(
                baseline,
                validated_baseline_lineage,
            )
        }
    else:
        if (
            lineage_plan.get("baseline_schema_version") != "legacy_v1_or_composite_v1"
            or lineage_plan.get("baseline_lineage_root") is not None
        ):
            raise CandidateBuildStageError("frozen legacy canonical lineage capability differs")
        baseline_codes = {
            str(item["instrument"])
            for item in legacy_active_segments(
                baseline_sealed,
                dataset=dataset,
            )
        }
    adoption_root = invocation.staging_root / ".isolated-bin-adoption" / dataset
    adoption_root.mkdir(parents=True, exist_ok=False)
    patch_files: dict[tuple[str, str], Path] = {}
    patch_evidence: dict[tuple[str, str], Mapping[str, Any]] = {}
    patch_is_stock: dict[tuple[str, str], bool] = {}
    for phase_id, patch in phases.items():
        for item in patch["csv"]["files"]:
            patch_files[(phase_id, str(item["instrument"]).casefold())] = (
                phase_roots[phase_id] / f"{item['instrument']}.csv"
            )
            patch_evidence[(phase_id, str(item["instrument"]).casefold())] = item
            patch_is_stock[(phase_id, str(item["instrument"]).casefold())] = True
        for item in patch["indices"]["files"]:
            patch_files[(phase_id, str(item["code"]).casefold())] = phase_roots[phase_id] / f"{item['code']}.csv"
            patch_evidence[(phase_id, str(item["code"]).casefold())] = item
            patch_is_stock[(phase_id, str(item["code"]).casefold())] = False
    baseline_csv_by_stem = {path.stem.casefold(): path for path in (baseline / "csv").glob("*.csv")}
    legacy_active_paths = {
        (
            str(item["root_relative_path"]).casefold(),
            str(item["relative_path"]).casefold(),
        )
        for item in ((legacy_composite or {}).get("segments") or ())
    }
    segment_files: dict[tuple[str, str], list[dict[str, Any]]] = {}
    manifest_targets: list[tuple[Path, str, str]] = []
    for relative in create:
        normalized = relative.replace("\\", "/").casefold()
        parts = normalized.split("/")
        if len(parts) == 3 and parts[0] == "csv_deltas":
            phase_id = "tail"
            segment_key = parts[1]
        elif len(parts) == 3 and parts[0] == "csv_overrides":
            phase_id = "override"
            segment_key = parts[1]
        elif len(parts) == 2 and parts[0] == "csv":
            phase_id = "new"
            segment_key = "base"
        else:
            continue
        target = adoption_root / Path(normalized)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.name == "manifest.json":
            manifest_targets.append((target, phase_id, segment_key))
            continue
        code = target.stem.casefold()
        source_file = patch_files.get((phase_id, code))
        item = patch_evidence.get((phase_id, code))
        is_stock = patch_is_stock.get((phase_id, code), True)
        if source_file is None:
            if phase_id != "tail":
                raise CandidateBuildStageError(f"declared {phase_id} CSV lacks exact source: {target.stem}")
            # Historical PIT instruments with no eligible row in this month
            # are deliberately represented by an empty, header-only delta and
            # excluded from the active composite segments.
            baseline_csv = baseline_csv_by_stem.get(target.stem.casefold())
            if baseline_csv is None:
                raise CandidateBuildStageError(f"declared CSV delta lacks source/header authority: {target.stem}")
            with baseline_csv.open("rb") as handle:
                header = handle.readline(1024 * 1024 + 1)
            if not header or len(header) > 1024 * 1024:
                raise CandidateBuildStageError("declared CSV delta header authority is invalid")
            target.write_bytes(header)
            rows = 0
            start = None
            end = None
        else:
            shutil.copyfile(source_file, target)
            assert item is not None
            rows = int(item["rows"])
            start = str(item["start"])
            end = str(item["end"])
        root_relative = f"{dataset}/{target.parent.relative_to(adoption_root).as_posix()}"
        segment_files.setdefault((phase_id, segment_key), []).append(
            {
                "instrument": target.stem.upper(),
                "root_relative_path": root_relative,
                "relative_path": target.name,
                "rows": rows,
                "sha256": _sha256_path(target),
                "size_bytes": target.stat().st_size,
                "start": start,
                "end": end,
                "active": (
                    (
                        root_relative.casefold(),
                        target.name.casefold(),
                    )
                    in legacy_active_paths
                    if legacy_plan
                    else rows > 0 and is_stock
                ),
            }
        )
    if legacy_plan:
        assert legacy_composite is not None
        for target, phase_id, segment_key in manifest_targets:
            _atomic_private_json(
                target,
                {
                    "schema_version": "dataset_release_csv_segment_manifest_v2",
                    "dataset": dataset,
                    "component_action": action.value,
                    "phase": phase_id,
                    "segment_key": segment_key,
                    "files": [
                        {key: value for key, value in item.items() if key != "root_relative_path"}
                        for item in sorted(
                            segment_files.get((phase_id, segment_key), ()),
                            key=lambda value: str(value["relative_path"]),
                        )
                    ],
                    "canonical": legacy_composite,
                    "patch_actual_work": phases[phase_id]["actual_work"],
                },
            )
        legacy_operation = _dump_operation(
            dataset,
            action=action.value,
            batch_manifest=batch_manifest,
            batch_manifest_path=private / "csv" / "batch_manifest.json",
        )
        legacy_combined_patch = {
            "schema_version": "dataset_release_daily_minute_composite_patch_v1",
            "dataset": dataset,
            "csv": {
                "files": [dict(item) for phase in phases.values() for item in phase["csv"]["files"]],
                "rows": sum(int(phase["csv"]["rows"]) for phase in phases.values()),
            },
            "indices": {
                "files": [dict(item) for phase in phases.values() for item in phase["indices"]["files"]],
                "codes": sorted({str(item["code"]) for phase in phases.values() for item in phase["indices"]["files"]}),
                "rows": sum(int(phase["indices"].get("rows", 0)) for phase in phases.values()),
            },
            "actual_work": {phase_id: dict(patch["actual_work"]) for phase_id, patch in phases.items()},
        }
        return {
            "schema_version": "dataset_release_bin_patch_preparation_v1",
            "dataset": dataset,
            "action": action.value,
            "baseline_cutoff": manifest.cutoff.isoformat(),
            "cutoff": pit.cutoff.isoformat(),
            "copy_on_write": plan.receipt(),
            "patch_preparation": legacy_combined_patch,
            "patch_phases": {key: dict(value) for key, value in phases.items()},
            "sealed_canonical_rows": dict(legacy_composite),
            "scope": {
                "tail_date_ranges": _portable_ranges(scope.tail_date_ranges),
                "override_date_ranges": _portable_ranges(scope.override_date_ranges),
                "new_date_ranges": _portable_ranges(scope.new_date_ranges),
                "tail_stock_codes": list(scope.tail_stock_codes),
                "tail_index_codes": list(scope.tail_index_codes),
                "override_stock_codes": list(scope.override_stock_codes),
                "override_index_codes": list(scope.override_index_codes),
                "new_stock_codes": list(scope.new_stock_codes),
                "delta_key": scope.delta_key,
                "override_key": scope.override_key,
                "pit_authority_changed": scope.pit_authority_changed,
            },
            "transform_metrics": metrics,
            "frozen_trading_days": [value.isoformat() for value in source.trading_days()],
            "qlib_dump_operations": [legacy_operation],
            "baseline_merkle_before": evidence.filesystem_tree_merkle,
            "safety": dict(_ZERO_SAFETY),
        }
    active_by_phase: dict[str, dict[str, dict[str, Any]]] = {}
    inventory: list[dict[str, Any]] = []
    for (phase_id, _segment_key), values in sorted(segment_files.items()):
        for value in values:
            item = dict(value)
            inventory.append(item)
            if item["active"] is True:
                active_by_phase.setdefault(phase_id, {})[str(item["instrument"])] = {
                    key: item[key]
                    for key in (
                        "instrument",
                        "root_relative_path",
                        "relative_path",
                        "rows",
                        "sha256",
                        "size_bytes",
                        "start",
                        "end",
                    )
                }
    override_codes = set(active_by_phase.get("override", {}))
    updates: list[dict[str, Any]] = []
    for code, item in sorted(active_by_phase.get("new", {}).items()):
        if code in baseline_codes or code in override_codes:
            raise CandidateBuildStageError("new canonical CSV code conflicts with baseline/override")
        updates.append({"instrument": code, "mode": "CREATE", "segments": [item]})
    for code, item in sorted(active_by_phase.get("tail", {}).items()):
        if code in override_codes:
            continue
        if code not in baseline_codes:
            raise CandidateBuildStageError("tail canonical CSV code is absent from the baseline")
        updates.append({"instrument": code, "mode": "APPEND", "segments": [item]})
    for code, item in sorted(active_by_phase.get("override", {}).items()):
        if code not in baseline_codes:
            raise CandidateBuildStageError("override canonical CSV code is absent from the baseline")
        updates.append({"instrument": code, "mode": "REPLACE", "segments": [item]})
    if not updates:
        raise CandidateBuildStageError("bounded bin patch produced no active canonical lineage update")
    planned_codes = tuple(sorted({str(item["instrument"]) for item in inventory}))
    planned_buckets = tuple(sorted({lineage_bucket(code) for code in planned_codes}))
    if tuple(lineage_plan.get("planned_buckets") or ()) != planned_buckets:
        raise CandidateBuildStageError("actual canonical lineage buckets differ from the frozen plan")
    scopes = tuple(dict(value) for value in frozen.get("invalidation_scopes") or () if isinstance(value, Mapping))
    mutation_identity = ensure_sha256(
        str(lineage_plan.get("mutation_identity", "")),
        field="canonical_lineage_mutation_identity",
    )
    baseline_receipt_path = baseline / "materialization_receipt.json"
    baseline_binding = {
        "source_release_id": str(frozen.get("source_release_id", "")),
        "source_release_digest": str(frozen.get("source_release_digest", "")),
        "component_file_identity": str(evidence.file_identity),
        "component_manifest_root": str(evidence.component_manifest_root),
        "materialization_receipt_sha256": _sha256_path(baseline_receipt_path),
        "materialization_receipt_size_bytes": baseline_receipt_path.stat().st_size,
    }
    component_root = invocation.staging_root / dataset
    if is_lineage_v3(baseline_sealed):
        lineage = write_transition_updates(
            component_root,
            dataset=dataset,
            ordered_fields=baseline_sealed["ordered_fields"],
            baseline_descriptor=baseline_sealed,
            updates=updates,
            cutoff=pit.cutoff.isoformat(),
            action=action.value,
            mutation_identity=mutation_identity,
            scopes=scopes,
            inventory=inventory,
            planned_instruments=planned_codes,
            event_key_override=str(lineage_plan.get("event_key", "")),
        )
    else:
        lineage = migrate_legacy_and_write_transition_updates(
            component_root,
            dataset=dataset,
            ordered_fields=baseline_sealed["ordered_fields"],
            legacy_source=baseline_sealed,
            updates=updates,
            baseline_cutoff=manifest.cutoff.isoformat(),
            cutoff=pit.cutoff.isoformat(),
            baseline_identity=str(evidence.component_manifest_root),
            baseline_binding=baseline_binding,
            action=action.value,
            mutation_identity=mutation_identity,
            scopes=scopes,
            inventory=inventory,
            planned_instruments=planned_codes,
            anchor_event_key=str(lineage_plan.get("anchor_key", "")),
            transition_event_key=str(lineage_plan.get("event_key", "")),
        )
    expected_lineage_paths = {
        str(value).replace("\\", "/") for value in create if str(value).replace("\\", "/").startswith("csv_lineage/")
    }
    if set(lineage.created_paths) != expected_lineage_paths:
        raise CandidateBuildStageError("canonical lineage writes differ from frozen create targets")
    lineage_summaries = tuple(
        dict(item)
        for item in lineage_instrument_summaries(
            component_root,
            lineage.descriptor,
        )
    )
    for relative in lineage.created_paths:
        source_path = component_root / Path(relative)
        adoption_path = adoption_root / Path(relative)
        adoption_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, adoption_path)
        source_path.unlink()
    sealed_canonical_rows = dict(lineage.descriptor)
    for target, phase_id, segment_key in manifest_targets:
        namespace = f"{dataset}/{target.parent.relative_to(adoption_root).as_posix()}"
        _atomic_private_json(
            target,
            lineage_namespace_manifest(
                dataset=dataset,
                component_action=action.value,
                phase=phase_id,
                segment_key=segment_key,
                namespace_root_relative_path=namespace,
                lineage=lineage,
                patch_actual_work=phases[phase_id]["actual_work"],
            ),
        )
    operation = _dump_operation(
        dataset,
        action=action.value,
        batch_manifest=batch_manifest,
        batch_manifest_path=private / "csv" / "batch_manifest.json",
    )
    combined_patch = {
        "schema_version": "dataset_release_daily_minute_composite_patch_v1",
        "dataset": dataset,
        "csv": {
            "files": [dict(item) for phase in phases.values() for item in phase["csv"]["files"]],
            "rows": sum(int(phase["csv"]["rows"]) for phase in phases.values()),
        },
        "indices": {
            "files": [dict(item) for phase in phases.values() for item in phase["indices"]["files"]],
            "codes": sorted({str(item["code"]) for phase in phases.values() for item in phase["indices"]["files"]}),
            "rows": sum(int(phase["indices"].get("rows", 0)) for phase in phases.values()),
        },
        "actual_work": {phase_id: dict(patch["actual_work"]) for phase_id, patch in phases.items()},
    }
    return {
        "schema_version": "dataset_release_bin_patch_preparation_v1",
        "dataset": dataset,
        "action": action.value,
        "baseline_cutoff": manifest.cutoff.isoformat(),
        "cutoff": pit.cutoff.isoformat(),
        "copy_on_write": plan.receipt(),
        "patch_preparation": combined_patch,
        "patch_phases": {key: dict(value) for key, value in phases.items()},
        "sealed_canonical_rows": sealed_canonical_rows,
        "canonical_instrument_summaries": list(lineage_summaries),
        "canonical_lineage": dict(lineage_plan),
        "scope": {
            "tail_date_ranges": _portable_ranges(scope.tail_date_ranges),
            "override_date_ranges": _portable_ranges(scope.override_date_ranges),
            "new_date_ranges": _portable_ranges(scope.new_date_ranges),
            "tail_stock_codes": list(scope.tail_stock_codes),
            "tail_index_codes": list(scope.tail_index_codes),
            "override_stock_codes": list(scope.override_stock_codes),
            "override_index_codes": list(scope.override_index_codes),
            "new_stock_codes": list(scope.new_stock_codes),
            "delta_key": scope.delta_key,
            "override_key": scope.override_key,
            "pit_authority_changed": scope.pit_authority_changed,
        },
        "transform_metrics": metrics,
        "frozen_trading_days": [value.isoformat() for value in source.trading_days()],
        "qlib_dump_operations": [operation],
        "baseline_merkle_before": evidence.filesystem_tree_merkle,
        "safety": dict(_ZERO_SAFETY),
    }


def _prepare_bin_patch_phase(
    invocation: BuildStageInvocation,
    *,
    source: ArtifactReadyBuildSource,
    pit: FrozenPitSnapshot,
    component: Component,
    dataset: str,
    toolchain,
    baseline_candidate_root: Path,
    baseline_sealed: Mapping[str, Any],
    date_ranges: Sequence[tuple[date, date]],
    stock_codes: Sequence[str],
    index_codes: Sequence[str],
    output_root: Path,
    seed_from_baseline: bool,
    checkpoint: Callable[[], None],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    ranges = tuple(date_ranges)
    selected_days = tuple(
        value
        for value in source.trading_days()
        if any(start <= value <= end for start, end in ranges)
        and (dataset == "daily_bin" or value >= invocation.profile.minute_start_date)
    )
    if not ranges or not selected_days:
        raise CandidateBuildStageError("bounded bin phase trading days are empty")
    codes = tuple(sorted({str(value).upper() for value in stock_codes}))
    seeds = (
        _baseline_adj_factor_seeds(
            baseline_candidate_root,
            baseline_sealed,
            codes=codes,
            before=selected_days[0],
            denominators=source.qfq_authority.by_code,
        )
        if seed_from_baseline and codes
        else {}
    )
    transform = CanonicalStockTransformSpec(
        cutoff=pit.cutoff,
        pit_snapshot=pit,
        trading_days=selected_days,
        qfq_denominators=source.qfq_authority,
        instrument_filter=(codes or None),
        initial_adj_factors=seeds,
    )
    metrics = CanonicalStockTransformMetrics(dataset)
    merge_metrics: dict[str, ExternalOrderedRowsMetrics] = {}

    def source_metrics(name: str) -> ExternalOrderedRowsMetrics:
        value = ExternalOrderedRowsMetrics()
        merge_metrics[name] = value
        return value

    if not codes:
        rows: Iterable[Mapping[str, Any]] = ()
    elif dataset == "daily_bin":
        rows = CanonicalStockTransformer().transform_daily(
            transform,
            daily_rows=_merged_rows(
                source,
                component,
                "kline_daily_raw",
                staging=invocation.staging_root,
                key=lambda row: (str(row["ts_code"]), _as_date(row["trade_date"])),
                checkpoint=checkpoint,
                date_ranges=ranges,
                instruments=codes,
                metrics=source_metrics("kline_daily_raw"),
            ),
            adj_factor_rows=_merged_rows(
                source,
                component,
                "adj_factor",
                staging=invocation.staging_root,
                key=lambda row: (str(row["ts_code"]), _as_date(row["trade_date"])),
                checkpoint=checkpoint,
                date_ranges=ranges,
                instruments=codes,
                metrics=source_metrics("adj_factor"),
            ),
            stk_limit_rows=_merged_rows(
                source,
                component,
                "stk_limit",
                staging=invocation.staging_root,
                key=lambda row: (str(row["ts_code"]), _as_date(row["trade_date"])),
                checkpoint=checkpoint,
                date_ranges=ranges,
                instruments=codes,
                metrics=source_metrics("stk_limit"),
            ),
            suspend_rows=_merged_rows(
                source,
                component,
                "suspend_d",
                staging=invocation.staging_root,
                key=lambda row: (
                    str(row["ts_code"]),
                    _as_date(row["trade_date"]),
                    str(row.get("suspend_type", "")),
                ),
                checkpoint=checkpoint,
                date_ranges=ranges,
                instruments=codes,
                metrics=source_metrics("suspend_d"),
            ),
            checkpoint=checkpoint,
            metrics=metrics,
        )
    else:
        rows = CanonicalStockTransformer().transform_minute(
            transform,
            minute_rows=_merged_rows(
                source,
                component,
                "kline_minute_raw",
                staging=invocation.staging_root,
                key=lambda row: (
                    str(row["ts_code"]),
                    _as_datetime(row["trade_time"]),
                    str(row.get("freq", "1m")),
                ),
                checkpoint=checkpoint,
                date_ranges=ranges,
                instruments=codes,
                metrics=source_metrics("kline_minute_raw"),
            ),
            adj_factor_rows=_merged_rows(
                source,
                component,
                "adj_factor",
                staging=invocation.staging_root,
                key=lambda row: (str(row["ts_code"]), _as_date(row["trade_date"])),
                checkpoint=checkpoint,
                date_ranges=ranges,
                instruments=codes,
                metrics=source_metrics("adj_factor"),
            ),
            stk_limit_rows=_merged_rows(
                source,
                component,
                "stk_limit",
                staging=invocation.staging_root,
                key=lambda row: (str(row["ts_code"]), _as_date(row["trade_date"])),
                checkpoint=checkpoint,
                date_ranges=ranges,
                instruments=codes,
                metrics=source_metrics("stk_limit"),
            ),
            suspend_rows=_merged_rows(
                source,
                component,
                "suspend_d",
                staging=invocation.staging_root,
                key=lambda row: (
                    str(row["ts_code"]),
                    _as_date(row["trade_date"]),
                    str(row.get("suspend_type", "")),
                ),
                checkpoint=checkpoint,
                date_ranges=ranges,
                instruments=codes,
                metrics=source_metrics("suspend_d"),
            ),
            checkpoint=checkpoint,
            metrics=metrics,
        )
    patch = (
        DailyMinutePatchCsvPreparer()
        .prepare(
            _bin_spec(
                invocation,
                pit=pit,
                dataset=dataset,
                toolchain=toolchain,
                index_csv_root=(
                    invocation.staging_root / "index_context" / "index_csv" if dataset == "daily_bin" else None
                ),
            ),
            rows=rows,
            output_root=output_root,
            index_codes=tuple(index_codes),
            index_date_ranges=(ranges if index_codes else ()),
            checkpoint=checkpoint,
        )
        .receipt
    )
    _remove_component_merge_spool(invocation.staging_root, component)
    return patch, {
        "transform": metrics.as_dict(),
        "source_merges": {name: value.as_dict() for name, value in sorted(merge_metrics.items())},
    }


def _prepare_csv_batches(
    root: Path,
    *,
    phases: Sequence[tuple[str, str, Path, Mapping[str, Any]]],
    dataset: str,
    max_codes: int,
    timeout_seconds: int,
    attempt_id: str,
    attempt_fence: int,
    execution_id: str,
) -> Mapping[str, Any]:
    if (
        dataset not in {"daily_bin", "minute_bin"}
        or type(max_codes) is not int
        or not 0 < max_codes <= 20
        or type(timeout_seconds) is not int
        or not 0 < timeout_seconds <= 1800
    ):
        raise CandidateBuildStageError("bounded Qlib batch policy is invalid")
    root.mkdir(parents=True, exist_ok=False)
    manifest_phases: list[dict[str, Any]] = []
    total_writes = 0
    total_rows = 0
    for phase_id, kind, source_root, patch in phases:
        if kind not in {"tail", "override", "full"}:
            raise CandidateBuildStageError("bounded Qlib batch phase is invalid")
        evidence_by_role: dict[str, list[tuple[str, Path, Mapping[str, Any]]]] = {"stock": [], "index": []}
        seen: set[str] = set()
        for key, identity_field, role in (
            ("csv", "instrument", "stock"),
            ("indices", "code", "index"),
        ):
            value = patch.get(key)
            if not isinstance(value, Mapping):
                raise CandidateBuildStageError("bounded Qlib patch evidence is missing")
            for item in value.get("files") or ():
                if not isinstance(item, Mapping):
                    raise CandidateBuildStageError("bounded Qlib CSV evidence is invalid")
                code = str(item.get(identity_field, "")).upper()
                if code in seen:
                    raise CandidateBuildStageError("bounded Qlib phase code is duplicated")
                seen.add(code)
                source_path = (source_root / f"{code}.csv").resolve(strict=True)
                if source_root.resolve(strict=True) not in source_path.parents:
                    raise CandidateBuildStageError("bounded Qlib CSV escapes phase root")
                _assert_plain(source_path)
                evidence_by_role[role].append((code, source_path, item))
        evidence = [item for values in evidence_by_role.values() for item in values]
        if not evidence:
            raise CandidateBuildStageError("bounded Qlib batch phase is empty")
        batches: list[dict[str, Any]] = []
        for role in ("stock", "index"):
            role_evidence = evidence_by_role[role]
            for offset in range(0, len(role_evidence), max_codes):
                ordinal = len(batches)
                batch_root = root / phase_id / f"batch-{ordinal:04d}-{role}"
                batch_root.mkdir(parents=True, exist_ok=False)
                files: list[dict[str, Any]] = []
                for code, source_path, item in role_evidence[offset : offset + max_codes]:
                    target = batch_root / f"{code.casefold()}.csv"
                    os.link(source_path, target)
                    rows = int(item.get("rows", 0))
                    if rows <= 0:
                        raise CandidateBuildStageError("bounded Qlib CSV row count is invalid")
                    files.append(
                        {
                            "code": code,
                            "role": role,
                            "relative_path": target.name,
                            "rows": rows,
                            "sha256": str(item["sha256"]),
                            "start": str(item["start"]),
                            "end": str(item["end"]),
                        }
                    )
                    total_writes += 1
                    total_rows += rows
                batches.append(
                    {
                        "ordinal": ordinal,
                        "role": role,
                        "mode": ("dump_update" if kind == "tail" else "dump_fix"),
                        "relative_path": batch_root.relative_to(root).as_posix(),
                        "files": files,
                    }
                )
        manifest_phases.append({"phase_id": phase_id, "kind": kind, "batches": batches})
    payload: dict[str, Any] = {
        "schema_version": "dataset_release_qlib_batched_dump_manifest_v1",
        "dataset": dataset,
        "freq": "day" if dataset == "daily_bin" else "1min",
        "fields": list(QLIB_STOCK_FIELDS),
        "max_codes_per_batch": max_codes,
        "per_batch_timeout_seconds": timeout_seconds,
        "resource_checkpoint_identity": {
            "attempt_id": attempt_id,
            "fence": attempt_fence,
            "execution_id": execution_id,
        },
        "phases": manifest_phases,
        "expected_total_code_writes": total_writes,
        "expected_total_rows": total_rows,
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    payload["manifest_identity"] = hashlib.sha256(encoded).hexdigest()
    _atomic_private_json(root / "batch_manifest.json", payload)
    return payload


def _prepare_full_batched_dump(
    invocation: BuildStageInvocation,
    *,
    dataset: str,
    pit: FrozenPitSnapshot,
    preparation: Mapping[str, Any],
    frozen_trading_days: Sequence[date],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    """Preseed exact global authority, then expose only <=20-code dump_fix batches."""

    if (
        preparation.get("schema_version") not in DAILY_MINUTE_CSV_PREPARATION_SUPPORTED_SCHEMAS
        or preparation.get("dataset") != dataset
        or preparation.get("cutoff") != pit.cutoff.isoformat()
    ):
        raise CandidateBuildStageError("full Qlib CSV preparation differs")
    private = invocation.staging_root / dataset / ".writer-private" / dataset.removesuffix("_bin")
    canonical_root = invocation.staging_root / dataset / "csv"
    batch_manifest = _prepare_csv_batches(
        private / "csv",
        phases=(("full", "full", canonical_root, preparation),),
        dataset=dataset,
        max_codes=min(
            invocation.profile.resource_policy.minute_code_batch_size,
            _rung(
                invocation.profile,
                "minute_batch",
                invocation.pressure_rung,
            ),
        ),
        timeout_seconds=min(
            1800,
            int(invocation.profile.stage_timeouts_seconds["qlib_dump"]),
        ),
        attempt_id=invocation.attempt_id,
        attempt_fence=invocation.attempt_fence,
        execution_id=f"build-dump-{dataset.removesuffix('_bin')}",
    )
    qlib_root = private / "qlib"
    if qlib_root.exists():
        raise CandidateBuildStageError("full private Qlib authority already exists")
    (qlib_root / "calendars").mkdir(parents=True)
    (qlib_root / "instruments").mkdir()
    (qlib_root / "features").mkdir()
    effective_start = invocation.profile.start_date if dataset == "daily_bin" else invocation.profile.minute_start_date
    eligible_days = tuple(value for value in frozen_trading_days if effective_start <= value <= pit.cutoff)
    if not eligible_days or eligible_days != tuple(sorted(set(eligible_days))):
        raise CandidateBuildStageError("full frozen trading-day authority differs")
    suffix = "day" if dataset == "daily_bin" else "1min"
    calendar = (
        [value.isoformat() for value in eligible_days]
        if dataset == "daily_bin"
        else [
            timestamp.strftime("%Y-%m-%d %H:%M:%S")
            for value in eligible_days
            for timestamp in canonical_session_times(value)
        ]
    )
    _atomic_private_bytes(
        qlib_root / "calendars" / f"{suffix}.txt",
        ("\n".join(calendar) + "\n").encode("utf-8"),
    )
    sealed = preparation.get("sealed_canonical_rows")
    if not isinstance(sealed, Mapping):
        raise CandidateBuildStageError("full canonical CSV receipt is missing")
    authority_seed = _rewrite_private_qlib_authorities(
        invocation,
        dataset=dataset,
        pit=pit,
        qlib_root=qlib_root,
        sealed=sealed,
        frozen_trading_days=tuple(value.isoformat() for value in frozen_trading_days),
    )
    operation = _dump_operation(
        dataset,
        action=ComponentAction.FULL_REBUILD.value,
        batch_manifest=batch_manifest,
        batch_manifest_path=private / "csv" / "batch_manifest.json",
    )
    enriched = {
        **dict(preparation),
        "frozen_trading_days": [value.isoformat() for value in frozen_trading_days],
        "qlib_dump_operations": [operation],
        "batched_full_preparation": {
            "schema_version": "dataset_release_batched_full_preparation_v1",
            "manifest_identity": batch_manifest["manifest_identity"],
            "manifest_sha256": _sha256_path(private / "csv" / "batch_manifest.json"),
            "authority_seed": authority_seed,
            "global_calendar_preseeded": True,
            "per_code_global_calendar_discovery": 0,
            "max_codes_per_batch": batch_manifest["max_codes_per_batch"],
        },
    }
    return enriched, operation


def _atomic_private_json(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    temporary = path.parent / f".{path.name}.{os.getpid()}.partial"
    with temporary.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _portable_ranges(
    values: Sequence[tuple[date, date]],
) -> list[list[str]]:
    return [[start.isoformat(), end.isoformat()] for start, end in values]


def _bin_patch_scope(
    invocation: BuildStageInvocation,
    *,
    component: Component,
    frozen: Mapping[str, Any],
    cutoff: date,
) -> _BinPatchScope:
    tail_months: set[str] = set()
    pit_authority_changed = False
    for scope in frozen.get("invalidation_scopes") or ():
        if not isinstance(scope, Mapping):
            continue
        if scope.get("kind") in {
            "monthly_tail_extension",
            "source_partition_append",
        }:
            tail_months.update(str(value) for value in scope.get("new_months") or ())
        pit_authority_changed = pit_authority_changed or scope.get("kind") == "pit_span_change"
    action = ComponentAction(str(_action_entry(invocation, component)["action"]))
    tail_ranges: list[tuple[date, date]] = []
    for month in sorted(tail_months):
        start = date.fromisoformat(f"{month}-01")
        following = date(start.year + 1, 1, 1) if start.month == 12 else date(start.year, start.month + 1, 1)
        tail_ranges.append((start, min(following - date.resolution, cutoff)))
    full_start = (
        invocation.profile.start_date if component is Component.DAILY_BIN else invocation.profile.minute_start_date
    )
    index_authority = set(invocation.profile.index_codes)
    delta_codes: set[str] = set()
    override_codes: set[str] = set()
    new_codes: set[str] = set()
    delta_keys: set[str] = set()
    override_keys: set[str] = set()
    for relative in frozen.get("create_new_targets") or ():
        parts = str(relative).replace("\\", "/").split("/")
        if len(parts) == 3 and parts[0] == "csv_deltas":
            delta_keys.add(parts[1])
            if parts[2] != "manifest.json" and parts[2].endswith(".csv"):
                delta_codes.add(parts[2].removesuffix(".csv").upper())
        elif len(parts) == 3 and parts[0] == "csv_overrides":
            override_keys.add(parts[1])
            if parts[2] != "manifest.json" and parts[2].endswith(".csv"):
                override_codes.add(parts[2].removesuffix(".csv").upper())
        elif len(parts) == 2 and parts[0] == "csv" and parts[1].endswith(".csv"):
            new_codes.add(parts[1].removesuffix(".csv").upper())
    if action is ComponentAction.INCREMENTAL and len(delta_keys) > 1:
        raise CandidateBuildStageError("bin patch delta path authority is ambiguous")
    if action is ComponentAction.SELECTIVE_REBUILD and len(override_keys) != 1:
        raise CandidateBuildStageError("bin patch override path authority is ambiguous")
    if delta_codes and not tail_ranges:
        # A delta always belongs to a declared monthly append scope; guessing
        # the target month from the cutoff would make recovery non-identical.
        raise CandidateBuildStageError("bin patch delta lacks monthly scope authority")
    if component is Component.MINUTE_BIN and (
        delta_codes.intersection(index_authority)
        or override_codes.intersection(index_authority)
        or new_codes.intersection(index_authority)
    ):
        raise CandidateBuildStageError("minute patch contains index codes")
    return _BinPatchScope(
        tuple(tail_ranges),
        ((full_start, cutoff),) if override_codes else (),
        ((full_start, cutoff),) if new_codes else (),
        tuple(sorted(delta_codes.difference(index_authority))),
        tuple(sorted(delta_codes.intersection(index_authority))),
        tuple(sorted(override_codes.difference(index_authority))),
        tuple(sorted(override_codes.intersection(index_authority))),
        tuple(sorted(new_codes.difference(index_authority))),
        next(iter(delta_keys), None),
        next(iter(override_keys), None),
        pit_authority_changed,
    )


def _copy_private_tree(source: Path, target: Path) -> None:
    source = source.resolve(strict=True)
    if target.exists():
        raise CandidateBuildStageError("private writer baseline already exists")
    target.mkdir(parents=True, exist_ok=False)
    for path in sorted(source.rglob("*"), key=lambda item: item.relative_to(source).as_posix()):
        _assert_plain(path)
        relative = path.relative_to(source)
        destination = target / relative
        if path.is_dir():
            destination.mkdir(exist_ok=True)
        elif path.is_file():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, destination)
        else:
            raise CandidateBuildStageError("private baseline contains non-plain entry")


def _baseline_adj_factor_seeds(
    candidate_root: Path,
    sealed: Mapping[str, Any],
    *,
    codes: Sequence[str],
    before: date,
    denominators: Mapping[str, float],
) -> dict[str, float]:
    segments: dict[str, list[Mapping[str, Any]]] = {}
    if sealed.get("schema_version") == "dataset_release_sealed_qlib_csv_rows_v1":
        for item in sealed.get("files") or ():
            segments.setdefault(str(item["instrument"]).upper(), []).append(
                {**dict(item), "root_relative_path": sealed["root_relative_path"]}
            )
    else:
        for item in sealed.get("segments") or ():
            segments.setdefault(str(item["instrument"]).upper(), []).append(item)
    output: dict[str, float] = {}
    for code in codes:
        eligible = [item for item in segments.get(code, ()) if str(item.get("end", ""))[:10] < before.isoformat()]
        if not eligible:
            continue
        item = max(eligible, key=lambda value: str(value["end"]))
        path = (candidate_root / str(item["root_relative_path"]) / str(item["relative_path"])).resolve(strict=True)
        record = _last_csv_record(path)
        numerator = float(record["factor"]) * float(denominators[code])
        if numerator <= 0:
            raise CandidateBuildStageError("baseline adj-factor seed is invalid")
        output[code] = numerator
    return output


def _last_csv_record(path: Path) -> Mapping[str, str]:
    import csv

    with path.open("rb") as handle:
        header = handle.readline(1024 * 1024 + 1)
        if not header or len(header) > 1024 * 1024:
            raise CandidateBuildStageError("baseline canonical CSV header is invalid")
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        buffered = b""
        while position > len(header) and buffered.count(b"\n") < 2:
            size = min(64 * 1024, position - len(header))
            position -= size
            handle.seek(position)
            buffered = handle.read(size) + buffered
        lines = [line for line in buffered.splitlines() if line.strip()]
    if not lines:
        raise CandidateBuildStageError("baseline canonical CSV is empty")
    reader = csv.DictReader(
        [
            header.decode("utf-8"),
            lines[-1].decode("utf-8"),
        ]
    )
    last = next(reader, None)
    if last is None:
        raise CandidateBuildStageError("baseline canonical CSV tail is invalid")
    return last


def _rewrite_private_qlib_authorities(
    invocation: BuildStageInvocation,
    *,
    dataset: str,
    pit: FrozenPitSnapshot,
    qlib_root: Path,
    sealed: Mapping[str, Any],
    frozen_trading_days: Sequence[str],
    canonical_summaries: Any = None,
) -> Mapping[str, Any]:
    effective_start = invocation.profile.start_date if dataset == "daily_bin" else invocation.profile.minute_start_date
    days = tuple(date.fromisoformat(str(value)) for value in frozen_trading_days)
    if not days or days != tuple(sorted(set(days))) or days[-1] != pit.cutoff:
        raise CandidateBuildStageError("frozen Qlib trading-day authority differs")
    eligible_days = tuple(value for value in days if effective_start <= value <= pit.cutoff)
    if dataset == "daily_bin":
        calendar = [value.isoformat() for value in eligible_days]
        suffix = "day"
    else:
        calendar = [
            timestamp.strftime("%Y-%m-%d %H:%M:%S")
            for value in eligible_days
            for timestamp in canonical_session_times(value)
        ]
        suffix = "1min"
    calendar_path = qlib_root / "calendars" / f"{suffix}.txt"
    observed_calendar = [
        value.strip() for value in calendar_path.read_text(encoding="utf-8").splitlines() if value.strip()
    ]
    if observed_calendar != calendar:
        raise CandidateBuildStageError("private Qlib calendar differs from frozen source authority")
    if is_lineage_v3(sealed):
        raw_ranges = canonical_summaries
        if raw_ranges is None:
            raw_ranges = list(
                lineage_instrument_summaries(
                    invocation.staging_root / dataset,
                    sealed,
                )
            )
    else:
        raw_ranges = sealed.get("files")
    if not isinstance(raw_ranges, list) or not raw_ranges:
        raise CandidateBuildStageError("canonical stock range summaries are missing")
    ranges = {
        str(item["instrument"]).upper(): (
            date.fromisoformat(str(item["start"])[:10]),
            date.fromisoformat(str(item["end"])[:10]),
        )
        for item in raw_ranges
        if isinstance(item, Mapping)
    }
    if len(ranges) != len(raw_ranges) or (
        is_lineage_v3(sealed)
        and (
            len(ranges) != int(sealed.get("instrument_count", -1))
            or sum(int(item.get("rows", -1)) for item in raw_ranges) != int(sealed.get("rows", -1))
        )
    ):
        raise CandidateBuildStageError("canonical stock range summaries differ")
    lines: list[str] = []
    expected_codes: set[str] = set()
    for span in pit.spans:
        start = max(span.eligible_start, effective_start)
        end = min(span.eligible_end, pit.cutoff)
        if start > end:
            continue
        observed = ranges.get(span.ts_code)
        if observed is None or observed[0] > start or observed[1] < end:
            raise CandidateBuildStageError(f"canonical rows do not cover frozen PIT span: {span.ts_code}")
        expected_codes.add(span.ts_code)
        lines.append(f"{span.ts_code}\t{start.isoformat()}\t{end.isoformat()}")
    if not lines or set(ranges) != expected_codes:
        raise CandidateBuildStageError("canonical stock authority differs from frozen PIT instruments")
    all_path = qlib_root / "instruments" / "all.txt"
    _atomic_private_bytes(all_path, ("\n".join(lines) + "\n").encode("utf-8"))
    index_path = qlib_root / "instruments" / "index.txt"
    if dataset == "daily_bin":
        index_lines = [
            f"{item.daily_code}\t{item.required_from.isoformat()}\t{pit.cutoff.isoformat()}"
            for item in invocation.profile.indices
        ]
        _atomic_private_bytes(index_path, ("\n".join(index_lines) + "\n").encode("utf-8"))
    elif index_path.exists():
        raise CandidateBuildStageError("minute Qlib unexpectedly contains index.txt")
    # Re-publish the exact frozen calendar bytes only after equality proved;
    # this gives the trusted parent sole authority without masking a writer
    # calendar defect that would misalign feature offsets.
    _atomic_private_bytes(calendar_path, ("\n".join(calendar) + "\n").encode("utf-8"))
    return {
        "schema_version": "dataset_release_private_qlib_authority_rewrite_v1",
        "dataset": dataset,
        "calendar_rows": len(calendar),
        "calendar_sha256": _sha256_path(calendar_path),
        "stock_codes": len(expected_codes),
        "all_txt_sha256": _sha256_path(all_path),
        "index_txt_sha256": (_sha256_path(index_path) if dataset == "daily_bin" else None),
        "pit_spans_sha256": pit.spans_sha256,
    }


def _atomic_private_bytes(path: Path, payload: bytes) -> None:
    temporary = path.parent / f".{path.name}.{os.getpid()}.partial"
    with temporary.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_batched_dump_receipt(
    manifest: Mapping[str, Any],
    receipt: Mapping[str, Any],
    *,
    expected_dataset: str,
    expected_manifest_identity: str,
) -> Mapping[str, Any]:
    """Bind one PASS receipt to every exact manifest batch.

    The supervised child result proves only process completion.  This receipt
    is the data-plane proof that every bounded batch was audited.  Accepting a
    PASS envelope with an empty, shortened, reordered, or forged batch list
    would silently finalize partially written Qlib features.
    """

    manifest_fields = {
        "schema_version",
        "dataset",
        "freq",
        "fields",
        "max_codes_per_batch",
        "per_batch_timeout_seconds",
        "resource_checkpoint_identity",
        "phases",
        "expected_total_code_writes",
        "expected_total_rows",
        "manifest_identity",
    }
    if (
        set(manifest) != manifest_fields
        or manifest.get("schema_version") != "dataset_release_qlib_batched_dump_manifest_v1"
        or manifest.get("dataset") != expected_dataset
        or manifest.get("manifest_identity") != expected_manifest_identity
    ):
        raise CandidateBuildStageError("bounded Qlib manifest identity differs")
    identity_payload = dict(manifest)
    identity_payload.pop("manifest_identity", None)
    observed_identity = hashlib.sha256(
        json.dumps(
            identity_payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    if observed_identity != expected_manifest_identity:
        raise CandidateBuildStageError("bounded Qlib manifest digest differs")

    phases = manifest.get("phases")
    if not isinstance(phases, list) or not phases:
        raise CandidateBuildStageError("bounded Qlib manifest phases are invalid")
    expected: list[dict[str, Any]] = []
    expected_writes = 0
    expected_rows = 0
    sequence = 0
    for phase in phases:
        if not isinstance(phase, Mapping) or set(phase) != {
            "phase_id",
            "kind",
            "batches",
        }:
            raise CandidateBuildStageError("bounded Qlib manifest phase differs")
        phase_id = str(phase.get("phase_id", ""))
        phase_kind = str(phase.get("kind", ""))
        batches = phase.get("batches")
        if (
            not phase_id
            or phase_kind not in {"tail", "override", "full"}
            or not isinstance(batches, list)
            or not batches
        ):
            raise CandidateBuildStageError("bounded Qlib manifest phase is invalid")
        for ordinal, batch in enumerate(batches):
            if not isinstance(batch, Mapping) or set(batch) != {
                "ordinal",
                "role",
                "mode",
                "relative_path",
                "files",
            }:
                raise CandidateBuildStageError("bounded Qlib manifest batch differs")
            files = batch.get("files")
            role = str(batch.get("role", ""))
            mode = str(batch.get("mode", ""))
            expected_mode = "dump_update" if phase_kind == "tail" else "dump_fix"
            if (
                batch.get("ordinal") != ordinal
                or role not in {"stock", "index"}
                or mode != expected_mode
                or not isinstance(files, list)
                or not files
            ):
                raise CandidateBuildStageError("bounded Qlib batch identity differs")
            code_list: list[str] = []
            rows = 0
            for item in files:
                if not isinstance(item, Mapping):
                    raise CandidateBuildStageError("bounded Qlib batch file differs")
                code = str(item.get("code", "")).upper()
                item_rows = item.get("rows")
                if not code or item.get("role") != role or type(item_rows) is not int or item_rows <= 0:
                    raise CandidateBuildStageError("bounded Qlib batch file identity differs")
                code_list.append(code)
                rows += item_rows
            expected.append(
                {
                    "sequence": sequence,
                    "phase_id": phase_id,
                    "phase_kind": phase_kind,
                    "ordinal": ordinal,
                    "role": role,
                    "mode": mode,
                    "instrument_authority_restored": (phase_kind in {"override", "full"} or role == "index"),
                    "codes": len(code_list),
                    "code_list": code_list,
                    "rows": rows,
                }
            )
            expected_writes += len(code_list)
            expected_rows += rows
            sequence += 1
    if (
        manifest.get("expected_total_code_writes") != expected_writes
        or manifest.get("expected_total_rows") != expected_rows
    ):
        raise CandidateBuildStageError("bounded Qlib manifest totals differ")

    receipt_fields = {
        "schema_version",
        "manifest_identity",
        "status",
        "dataset",
        "completed_batches",
        "completed_batch_count",
        "peak_codes_per_batch",
        "peak_rows_per_batch",
        "all_market_frames_retained",
        "upstream_silent_code_failures",
    }
    completed = receipt.get("completed_batches")
    if (
        set(receipt) != receipt_fields
        or receipt.get("schema_version") != "dataset_release_qlib_batched_dump_receipt_v1"
        or receipt.get("manifest_identity") != expected_manifest_identity
        or receipt.get("status") != "PASS"
        or receipt.get("dataset") != expected_dataset
        or not isinstance(completed, list)
        or len(completed) != len(expected)
        or receipt.get("completed_batch_count") != len(expected)
        or receipt.get("all_market_frames_retained") != 0
        or receipt.get("upstream_silent_code_failures") != 0
    ):
        raise CandidateBuildStageError("bounded Qlib batch receipt differs")
    completed_fields = {
        "sequence",
        "phase_id",
        "phase_kind",
        "ordinal",
        "role",
        "mode",
        "instrument_authority_restored",
        "recovered_from_inflight",
        "codes",
        "code_list",
        "rows",
        "calendar_rows",
        "calendar_end",
    }
    for observed, declared in zip(completed, expected, strict=True):
        if not isinstance(observed, Mapping) or set(observed) != completed_fields:
            raise CandidateBuildStageError("bounded Qlib batch evidence differs")
        if any(observed.get(key) != value for key, value in declared.items()):
            raise CandidateBuildStageError("bounded Qlib completed batch differs")
        if (
            type(observed.get("recovered_from_inflight")) is not bool
            or type(observed.get("calendar_rows")) is not int
            or int(observed["calendar_rows"]) <= 0
            or not str(observed.get("calendar_end", ""))
        ):
            raise CandidateBuildStageError("bounded Qlib batch audit is incomplete")
    expected_peak_codes = max(item["codes"] for item in expected)
    expected_peak_rows = max(item["rows"] for item in expected)
    max_codes = manifest.get("max_codes_per_batch")
    if (
        type(max_codes) is not int
        or not 0 < max_codes <= 20
        or expected_peak_codes > max_codes
        or receipt.get("peak_codes_per_batch") != expected_peak_codes
        or receipt.get("peak_rows_per_batch") != expected_peak_rows
    ):
        raise CandidateBuildStageError("bounded Qlib batch peak differs")
    return dict(receipt)


def _finalize_bin_patch(
    invocation: BuildStageInvocation,
    *,
    dataset: str,
    pit: FrozenPitSnapshot,
    toolchain,
    preparation: Mapping[str, Any],
    checkpoint: Callable[[], None],
) -> Mapping[str, Any]:
    if (
        preparation.get("schema_version") != "dataset_release_bin_patch_preparation_v1"
        or preparation.get("dataset") != dataset
        or preparation.get("cutoff") != pit.cutoff.isoformat()
    ):
        raise CandidateBuildStageError("bounded bin preparation identity differs")
    component = Component.DAILY_BIN if dataset == "daily_bin" else Component.MINUTE_BIN
    frozen = _action_entry(invocation, component).get("frozen_reuse")
    if not isinstance(frozen, Mapping) or preparation.get("canonical_lineage") != frozen.get("canonical_lineage"):
        raise CandidateBuildStageError("bounded bin resume lineage capability differs from frozen plan")
    operation_id = dataset.removesuffix("_bin")
    child = _prerequisite_json(invocation, f"qlib_dump_{operation_id}")
    cow = preparation.get("copy_on_write")
    if not isinstance(cow, Mapping):
        raise CandidateBuildStageError("bounded bin COW receipt is missing")
    plan = restore_copy_on_write_plan(
        cow,
        expected_source_root=Path(str(cow["source_root"])),
        expected_target_root=invocation.staging_root / dataset,
    )
    private = invocation.staging_root / dataset / ".writer-private" / operation_id
    private_qlib = private / "qlib"
    adoption_root = invocation.staging_root / ".isolated-bin-adoption" / dataset
    if not private_qlib.is_dir() or not adoption_root.is_dir():
        raise CandidateBuildStageError("bounded private Qlib output is missing")
    batch_manifest = _load_component_json(private / "csv" / "batch_manifest.json")
    batched_dump = _load_component_json(private / "csv" / "batched_dump_receipt.json")
    operations = preparation.get("qlib_dump_operations")
    if not isinstance(operations, list) or len(operations) != 1:
        raise CandidateBuildStageError("bounded Qlib operation binding is missing")
    operation = operations[0]
    manifest_path = private / "csv" / "batch_manifest.json"
    if (
        not isinstance(operation, Mapping)
        or operation.get("dataset") != dataset
        or operation.get("mode") != "batched_patch"
        or operation.get("batch_manifest_identity") != batch_manifest.get("manifest_identity")
        or operation.get("batch_manifest_sha256") != _sha256_path(manifest_path)
    ):
        raise CandidateBuildStageError("bounded Qlib operation binding differs")
    batched_dump = _validate_batched_dump_receipt(
        batch_manifest,
        batched_dump,
        expected_dataset=dataset,
        expected_manifest_identity=str(operation["batch_manifest_identity"]),
    )
    sealed = preparation.get("sealed_canonical_rows")
    if not isinstance(sealed, Mapping):
        raise CandidateBuildStageError("bounded bin canonical receipt is missing")
    authority_rewrite = _rewrite_private_qlib_authorities(
        invocation,
        dataset=dataset,
        pit=pit,
        qlib_root=private_qlib,
        sealed=sealed,
        frozen_trading_days=tuple(preparation.get("frozen_trading_days") or ()),
        canonical_summaries=preparation.get("canonical_instrument_summaries"),
    )
    placeholder = (
        json.dumps(
            {
                "schema_version": "dataset_release_bin_patch_placeholder_v1",
                "dataset": dataset,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    nondeferred = plan.mutation_paths.difference(plan.deferred_existing_paths)
    for relative in sorted(nondeferred):
        target = adoption_root / Path(relative)
        if target.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if relative in {
            "materialization_receipt.json",
            "csv_preparation_receipt.json",
        }:
            target.write_bytes(placeholder)
        else:
            raise CandidateBuildStageError(f"bounded bin patch lacks declared target: {relative}")
    adoption = adopt_isolated_writer_patch(
        plan,
        adoption_root,
        patch_targets={path: path for path in nondeferred},
    )
    deferred_adoption = adopt_deferred_writer_outputs(
        plan,
        private_qlib,
        patch_targets={path: path.removeprefix("qlib/") for path in plan.deferred_existing_paths},
        baseline_copy_count=1,
    )
    patch = preparation.get("patch_preparation")
    if not isinstance(patch, Mapping):
        raise CandidateBuildStageError("bounded bin canonical receipt is missing")
    receipt = DailyMinuteIncrementalFinalizer().audit(
        _bin_spec(
            invocation,
            pit=pit,
            dataset=dataset,
            toolchain=toolchain,
            index_csv_root=(
                invocation.staging_root / "index_context" / "index_csv" if dataset == "daily_bin" else None
            ),
        ),
        sealed_canonical_rows=sealed,
        supervised_child=child,
        patch_preparation=patch,
        adoption={
            "action": preparation["action"],
            "external_writer_baseline_visibility": 0,
            "copy_on_write_source_merkle": plan.source_merkle_before,
            "adopted_target_count": len(adoption["adopted"]) + len(deferred_adoption["adopted"]),
            "baseline_qlib_copy_count": deferred_adoption["baseline_copy_count"],
            "final_qlib_recopy_count": deferred_adoption["final_recopy_count"],
            "batched_dump": batched_dump,
            "authority_rewrite": authority_rewrite,
        },
    )
    preparation_receipt = {
        **dict(patch),
        "sealed_canonical_rows": dict(sealed),
        "component_action": preparation["action"],
        "copy_on_write_source_merkle": plan.source_merkle_before,
    }
    atomic_write_mutation(
        plan,
        "csv_preparation_receipt.json",
        (
            json.dumps(
                preparation_receipt,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8"),
    )
    atomic_write_mutation(
        plan,
        "materialization_receipt.json",
        (
            json.dumps(
                receipt,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8"),
    )
    _remove_owned_scratch(invocation.staging_root / dataset, ".writer-private")
    _remove_owned_scratch(invocation.staging_root / ".isolated-bin-adoption", dataset)
    adoption_parent = invocation.staging_root / ".isolated-bin-adoption"
    if adoption_parent.exists() and not any(adoption_parent.iterdir()):
        adoption_parent.rmdir()
    targets = writer_target_manifest(plan)
    baseline_after = tree_merkle(plan.source_root)[1]
    if baseline_after != preparation.get("baseline_merkle_before"):
        raise CandidateBuildStageError("bounded bin patch changed baseline")
    checkpoint()
    return {
        **receipt,
        "writer_target_manifest": targets,
        "baseline_merkle_after": baseline_after,
    }


def _factor_patch_months(frozen: Mapping[str, Any]) -> tuple[str, ...]:
    values: set[str] = set()
    for relative in (
        *(frozen.get("replace_existing_targets") or ()),
        *(frozen.get("create_new_targets") or ()),
    ):
        parts = str(relative).replace("\\", "/").split("/")
        if len(parts) == 3 and parts[0] == "partitions" and parts[2].endswith(".parquet"):
            values.add(parts[2].removesuffix(".parquet"))
    return tuple(sorted(values))


def _factor_tail_months(frozen: Mapping[str, Any]) -> set[str]:
    return {
        str(month)
        for scope in frozen.get("invalidation_scopes") or ()
        if isinstance(scope, Mapping) and scope.get("kind") in {"monthly_tail_extension", "source_partition_append"}
        for month in scope.get("new_months") or ()
    }


def _factor_affected_scope(
    frozen: Mapping[str, Any],
    *,
    selected_months: Sequence[str],
) -> tuple[tuple[str, ...], set[str]]:
    codes: set[str] = set()
    unbounded_all = False
    for scope in frozen.get("invalidation_scopes") or ():
        if not isinstance(scope, Mapping):
            continue
        kind = str(scope.get("kind", ""))
        if kind in {
            "qfq_denominator_change",
            "qfq_historical_numerator_revision",
        }:
            instrument = str(scope.get("instrument", "")).upper()
            if instrument:
                codes.add(instrument)
        elif kind == "pit_span_change":
            codes.update(
                str(value).upper()
                for field in ("changed_instruments", "new_instruments")
                for value in scope.get(field) or ()
            )
        elif kind == "historical_source_revision":
            affected = tuple(str(value).upper() for value in scope.get("affected_instruments") or ())
            if affected:
                codes.update(affected)
            else:
                unbounded_all = True
    return tuple(sorted(codes)), (set(selected_months) if unbounded_all else set())


def _remap_factor_receipt(receipt: Mapping[str, Any], *, staging_root: Path) -> dict[str, Any]:
    output = json.loads(json.dumps(receipt))
    for item in output.get("chunks") or ():
        item["candidate_relative_path"] = f"factor_bundle/partitions/{item['dataset']}/{item['partition_key']}.parquet"
    for dataset, item in (output.get("outputs") or {}).items():
        item["artifact_relative_path"] = (
            f"factor_bundle/{dataset}.h5" if dataset != "static_factors" else "factor_bundle/static_factors.parquet"
        )
    del staging_root
    return output


def _patch_index_component(
    invocation: BuildStageInvocation,
    *,
    source: _FrozenIndexSource,
    cutoff: date,
):
    component = Component.DOMESTIC_INDEX_CONTEXT
    baseline, manifest, evidence, frozen = _baseline_component(invocation, component)
    action = ComponentAction(str(_action_entry(invocation, component)["action"]))
    replace = tuple(str(value) for value in frozen.get("replace_existing_targets") or ())
    create = tuple(str(value) for value in frozen.get("create_new_targets") or ())
    if (
        action not in {ComponentAction.INCREMENTAL, ComponentAction.SELECTIVE_REBUILD}
        or not replace
        or set(frozen.get("mutation_set") or ()) != set((*replace, *create))
    ):
        raise CandidateBuildStageError("incremental index mutation authority differs")
    patch_parent = invocation.staging_root / ".isolated-patches"
    patch_parent.mkdir(exist_ok=False)
    patch = patch_parent / "index_context"
    row_group_rows = _rung(invocation.profile, "row_group_rows", invocation.pressure_rung)
    if action is ComponentAction.INCREMENTAL:
        if manifest.cutoff >= cutoff:
            raise CandidateBuildStageError("incremental index cutoff is not a tail")
        receipt = IncrementalIndexContextMaterializer(source, definitions=invocation.profile.indices).materialize(
            baseline,
            patch,
            baseline_cutoff=manifest.cutoff,
            cutoff=cutoff,
            row_group_rows=row_group_rows,
        )
    else:
        if manifest.cutoff != cutoff:
            raise CandidateBuildStageError("selective index cutoff differs from baseline")
        receipt = SelectiveIndexContextMaterializer(source, definitions=invocation.profile.indices).materialize(
            baseline,
            patch,
            cutoff=cutoff,
            date_ranges=_index_selective_ranges(frozen, cutoff=cutoff),
            row_group_rows=row_group_rows,
        )
    plan = prepare_copy_on_write_tree(
        baseline,
        invocation.staging_root / "index_context",
        replace_existing_targets=replace,
        create_new_targets=create,
        source_sealed=True,
        expected_source_merkle=str(evidence.filesystem_tree_merkle),
    )
    _assert_patch_equivalence(plan, patch)
    adoption = adopt_isolated_writer_patch(
        plan,
        patch,
        patch_targets={path: path for path in plan.mutation_paths},
    )
    target = writer_target_manifest(plan)
    patch_merkle = tree_merkle(patch)[1]
    if target["target_merkle"] != patch_merkle:
        raise CandidateBuildStageError("incremental index adoption differs from patch")
    final_root = invocation.staging_root / "index_context"
    durable_receipt = IndexMaterializationReceipt(
        root=final_root,
        h5_path=final_root / "index_daily.h5",
        parquet_path=final_root / "index_context.parquet",
        csv_root=final_root / "index_csv",
        rows=receipt.rows,
        provider_fill_rows=receipt.provider_fill_rows,
        contract_digest=receipt.contract_digest,
        details=receipt.details,
    )
    _remove_owned_scratch(patch_parent, "index_context")
    patch_parent.rmdir()
    return durable_receipt, {
        "schema_version": "dataset_release_incremental_component_adoption_v1",
        "component": component.value,
        "action": action.value,
        "baseline_cutoff": manifest.cutoff.isoformat(),
        "cutoff": cutoff.isoformat(),
        "copy_on_write": plan.receipt(),
        "adoption": adoption,
        "writer_target_manifest": target,
        "patch_merkle": patch_merkle,
        "baseline_merkle_after": tree_merkle(baseline)[1],
        "safety": dict(_ZERO_SAFETY),
    }


def _index_selective_ranges(frozen: Mapping[str, Any], *, cutoff: date) -> tuple[tuple[date, date], ...]:
    months: set[str] = set()
    scopes = frozen.get("invalidation_scopes")
    if not isinstance(scopes, list):
        raise CandidateBuildStageError("selective index invalidation scopes are missing")
    for scope in scopes:
        if not isinstance(scope, Mapping) or scope.get("kind") != "historical_source_revision":
            continue
        raw_months = scope.get("months")
        if not isinstance(raw_months, list):
            raise CandidateBuildStageError("selective index months are invalid")
        months.update(str(value) for value in raw_months)
    ranges: list[tuple[date, date]] = []
    for value in sorted(months):
        try:
            start = date.fromisoformat(f"{value}-01")
        except ValueError as exc:
            raise CandidateBuildStageError("selective index month is invalid") from exc
        following = date(start.year + 1, 1, 1) if start.month == 12 else date(start.year, start.month + 1, 1)
        end = min(cutoff, following - date.resolution)
        if start <= end:
            ranges.append((start, end))
    if not ranges:
        raise CandidateBuildStageError("selective index has no bounded month range")
    return tuple(ranges)


def _assert_patch_equivalence(plan: CopyOnWritePlan, patch_root: Path) -> None:
    baseline = {item.relative_path.casefold(): item for item in plan.source_files}
    observed_patch_files = tree_merkle(patch_root)[0]
    patch_files = {item.relative_path.casefold(): item for item in observed_patch_files}
    if len(baseline) != len(plan.source_files) or len(patch_files) != len(observed_patch_files):
        raise CandidateBuildStageError("isolated patch contains a case-insensitive path collision")
    expected = set(baseline).union(value.casefold() for value in plan.create_new_paths)
    if set(patch_files) != expected:
        raise CandidateBuildStageError("isolated patch path set differs from baseline plus declared creates")
    mutations = {value.casefold() for value in plan.mutation_paths}
    for relative in sorted(expected.difference(mutations)):
        if patch_files[relative].content_identity() != baseline[relative].content_identity():
            raise CandidateBuildStageError(f"isolated patch changed an undeclared file: {relative}")


def _load_component_json(path: Path) -> Mapping[str, Any]:
    try:
        import json

        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CandidateBuildStageError("reused component receipt is unreadable") from exc
    if not isinstance(value, Mapping):
        raise CandidateBuildStageError("reused component receipt is invalid")
    return value


def _existing_index_receipt(staging: Path) -> Mapping[str, Any]:
    root = staging / "index_context"
    value = dict(_load_component_json(root / "index_materialization_receipt.json"))
    if value.get("schema_version") != "dataset_release_index_materialization_v1":
        raise CandidateBuildStageError("reused index receipt schema differs")
    return {
        **value,
        "status": "PASS",
        "root_relative_path": "index_context",
        "h5_relative_path": "index_context/index_daily.h5",
        "parquet_relative_path": "index_context/index_context.parquet",
    }


def _merged_rows(
    source: ArtifactReadyBuildSource,
    component: Component,
    dataset: str,
    *,
    staging: Path,
    key: Callable[[Mapping[str, Any]], tuple[Any, ...]],
    checkpoint: Callable[[], None],
    date_ranges: Sequence[tuple[date, date]] = (),
    instruments: Sequence[str] = (),
    metrics: ExternalOrderedRowsMetrics | None = None,
) -> Iterable[Mapping[str, Any]]:
    spool = staging / ".merge_spool" / component.value / dataset
    spool.mkdir(parents=True, exist_ok=False)
    report = metrics or ExternalOrderedRowsMetrics()
    partitions = (
        source.ordered_partitions(
            component,
            dataset,
            date_ranges=date_ranges,
            instruments=instruments,
        )
        if date_ranges or instruments
        else source.ordered_partitions(component, dataset)
    )

    def iter_rows() -> Iterable[Mapping[str, Any]]:
        try:
            yield from external_merge_ordered_rows(
                partitions,
                key=key,
                spool_root=spool,
                max_open_streams=64,
                checkpoint=checkpoint,
                metrics=report,
            )
        finally:
            if spool.exists():
                relative = spool.relative_to(staging).as_posix()
                _remove_owned_scratch(staging, relative)
            for parent in (
                staging / ".merge_spool" / component.value,
                staging / ".merge_spool",
            ):
                if parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()

    return iter_rows()


def _bin_spec(
    invocation: BuildStageInvocation,
    *,
    pit: FrozenPitSnapshot,
    dataset: str,
    toolchain,
    index_csv_root: Path | None,
) -> DailyMinuteMaterializationSpec:
    return DailyMinuteMaterializationSpec(
        dataset=dataset,
        staging_root=invocation.staging_root,
        project_root=invocation.project_root,
        cutoff=pit.cutoff,
        effective_start=(
            invocation.profile.start_date if dataset == "daily_bin" else invocation.profile.minute_start_date
        ),
        pit_snapshot=pit,
        dump_workers=_rung(invocation.profile, "dump_workers", invocation.pressure_rung),
        toolchain=toolchain,
        index_csv_root=index_csv_root,
        child_timeout_seconds=float(invocation.profile.stage_timeouts_seconds["qlib_dump"]),
    )


def _dump_operation(
    dataset: str,
    *,
    action: str,
    batch_manifest: Mapping[str, Any] | None = None,
    batch_manifest_path: Path | None = None,
) -> dict[str, Any]:
    operation_id = dataset.removesuffix("_bin")
    private = f"{dataset}/.writer-private/{operation_id}"
    mode = (
        "batched_patch"
        if action
        in {
            ComponentAction.INCREMENTAL.value,
            ComponentAction.SELECTIVE_REBUILD.value,
        }
        else "batched_full"
    )
    if mode in {"batched_patch", "batched_full"}:
        if (
            not isinstance(batch_manifest, Mapping)
            or batch_manifest_path is None
            or batch_manifest.get("manifest_identity") is None
        ):
            raise CandidateBuildStageError("batched Qlib operation lacks manifest identity")
        batch_identity: str | None = str(batch_manifest["manifest_identity"])
        batch_sha256: str | None = _sha256_path(batch_manifest_path)
    else:
        if batch_manifest is not None or batch_manifest_path is not None:
            raise CandidateBuildStageError("unbatched Qlib operation has a manifest")
        batch_identity = None
        batch_sha256 = None
    return {
        "operation_id": operation_id,
        "dataset": dataset,
        "mode": mode,
        "component_action": action,
        "csv_relative_path": f"{private}/csv",
        "qlib_relative_path": f"{private}/qlib",
        "writer_targets_digest": digest_named_fields(
            "dataset_release_qlib_dump_writer_targets_v1",
            {"dataset": dataset, "mode": mode, "target": f"{private}/qlib"},
        ),
        "batch_manifest_identity": batch_identity,
        "batch_manifest_sha256": batch_sha256,
    }


class _FrozenIndexSource:
    def __init__(
        self,
        *,
        trading_days: Sequence[date],
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        self.days = tuple(trading_days)
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row["ts_code"]).upper(), []).append(dict(row))
        self.rows = grouped

    def trading_dates(self, start: date, end: date) -> Sequence[date]:
        return tuple(value for value in self.days if start <= value <= end)

    def database_rows(self, definition: IndexDefinition, start: date, end: date) -> Iterable[Mapping[str, Any]]:
        return tuple(
            row for row in self.rows.get(definition.daily_code, ()) if start <= _as_date(row["trade_date"]) <= end
        )

    def provider_rows(self, definition: IndexDefinition, start: date, end: date) -> Iterable[Mapping[str, Any]]:
        del definition, start, end
        return ()


class _FactorReader:
    def __init__(
        self,
        source: ArtifactReadyBuildSource,
        *,
        instrument_filter: Sequence[str] = (),
    ) -> None:
        self.source = source
        self.instrument_filter = tuple(instrument_filter)

    def iter_frames(
        self,
        dataset: str,
        partition_key: str,
        *,
        start: date,
        end: date,
        max_rows: int,
    ):
        return self.source.iter_factor_frames(
            dataset,
            partition_key,
            start=start,
            end=end,
            max_rows=max_rows,
            instruments=self.instrument_filter,
        )


def _candidate_identity(
    invocation: BuildStageInvocation,
    *,
    artifact_root: str,
    pit_snapshot: FrozenPitSnapshot,
) -> str:
    build = invocation.build_inputs
    return CandidateIdentity(
        registration_uuid=build_candidate_registration_id(invocation.release_digest),
        allowlisted_root_id=invocation.profile.candidate_root_id,
        volume_serial=volume_identity(invocation.candidate_root),
        root_relative_path=invocation.release_id,
        profile=invocation.profile.profile,
        scope=Scope(str(build["scope"])),
        cutoff=pit_snapshot.cutoff,
        lineage_anchor=f"BUILD_RELEASE_DIGEST:{invocation.release_digest}",
        pit_provenance_state=PitProvenanceState.KNOWN,
        pit_provenance_digest_or_sentinel=pit_snapshot.spans_sha256,
        artifact_root=artifact_root,
        producer_provenance_state=ProducerProvenanceState.KNOWN,
        producer_provenance_digest_or_sentinel=str(build["fingerprints"]["producer_fingerprint"]),
    ).key


def _index_receipt_payload(receipt, *, staging: Path) -> Mapping[str, Any]:
    return {
        "schema_version": "dataset_release_index_materialization_v1",
        "status": "PASS",
        "rows": receipt.rows,
        "provider_fill_rows": receipt.provider_fill_rows,
        "contract_digest": receipt.contract_digest,
        "details": dict(receipt.details),
        "root_relative_path": receipt.root.relative_to(staging).as_posix(),
        "h5_relative_path": receipt.h5_path.relative_to(staging).as_posix(),
        "parquet_relative_path": receipt.parquet_path.relative_to(staging).as_posix(),
        "database_writes": 0,
        "production_writes": 0,
    }


def _prerequisite_json(invocation: BuildStageInvocation, name: str) -> Mapping[str, Any]:
    reference = invocation.prerequisites.get(name)
    if not reference:
        raise CandidateBuildStageError(f"stage prerequisite is missing: {name}")
    return _cas_json(invocation.cas, reference)


def _cas_json(cas: CASStore, reference: Any) -> Mapping[str, Any]:
    value = cas.get_json_bounded(reference, max_bytes=32 * 1024 * 1024)
    if not isinstance(value, Mapping):
        raise CandidateBuildStageError("stage CAS receipt is not an object")
    return value


def _rung(profile: DatasetProfile, field: str, index: int) -> int:
    values = profile.pressure_ladder[field]
    if not 0 <= index < max(len(item) for item in profile.pressure_ladder.values()):
        raise CandidateBuildStageError("pressure rung is invalid")
    return int(values[min(index, len(values) - 1)])


def _remove_owned_scratch(staging: Path, relative: str) -> None:
    root = (staging / relative).resolve(strict=True)
    if staging.resolve(strict=True) not in root.parents:
        raise CandidateBuildStageError("owned scratch escapes staging")
    for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        _assert_plain(path)
        if path.is_dir():
            path.rmdir()
        elif path.is_file():
            path.unlink()
        else:
            raise CandidateBuildStageError("owned scratch contains non-plain entry")
    root.rmdir()


def _remove_component_merge_spool(staging: Path, component: Component) -> None:
    relative = f".merge_spool/{component.value}"
    component_root = staging / Path(relative)
    if component_root.exists():
        _remove_owned_scratch(staging, relative)
    merge_root = staging / ".merge_spool"
    if merge_root.exists() and not any(merge_root.iterdir()):
        merge_root.rmdir()


def _copy_private_csv_tree(source: Path, target: Path) -> None:
    source = source.resolve(strict=True)
    if target.exists():
        raise CandidateBuildStageError("private writer CSV root already exists")
    target.mkdir(parents=True, exist_ok=False)
    for path in sorted(source.iterdir(), key=lambda item: item.name):
        _assert_plain(path)
        if not path.is_file():
            raise CandidateBuildStageError("canonical CSV root contains non-files")
        shutil.copyfile(path, target / path.name)


def _resolve_plain_directory(path: Path, *, label: str) -> Path:
    requested = Path(os.path.abspath(os.fspath(Path(path).expanduser())))
    current = Path(requested.anchor)
    try:
        _assert_plain(current)
        for part in requested.parts[1:]:
            current = current / part
            _assert_plain(current)
        resolved = requested.resolve(strict=True)
    except OSError as exc:
        raise CandidateBuildStageError(f"{label} is unavailable") from exc
    if not resolved.is_dir():
        raise CandidateBuildStageError(f"{label} is not a directory")
    return resolved


def _assert_plain(path: Path) -> None:
    value = os.lstat(path)
    if stat.S_ISLNK(value.st_mode) or (int(getattr(value, "st_file_attributes", 0)) & _REPARSE_POINT):
        raise CandidateBuildStageError("build path contains a link/reparse point")


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)


__all__ = [
    "BUILD_FINALIZE_RECEIPT_SCHEMA",
    "BUILD_PREPARE_RECEIPT_SCHEMA",
    "BuildStageInvocation",
    "CandidateBuildStageError",
    "run_build_stage",
]
