"""Production PostgreSQL SOURCE adapter for unified monthly releases.

The adapter reuses the audited dataset-release source authority, but imports
every data-bearing read into the snapshot exported by
``AuditedMonthlySourceProducer``.  The result is a provider-free frozen bundle
which later stages can consume without reopening PostgreSQL.

This module deliberately does not repair source tables.  Missing or invalid
source data remains a SOURCE failure in the existing source authority.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
import hashlib
import os
from pathlib import Path
import re
from typing import Any, Callable, Mapping, Sequence

from .artifact_ready_source import ArtifactReadySourceBuilder, load_artifact_ready_contract
from .canonical import canonical_json_bytes, digest_named_fields
from .cas_store import CASRef, CASStore
from .contracts import Scope
from .control_store import ControlStore, SourceSnapshotCatalogSpec
from .index_sources import independent_postgres_connection_factory
from .monthly_snapshot import MonthlySnapshotIdentity, SnapshotConnection
from .monthly_component_preparation import component_dependencies, preparation_plan
from .monthly_preparation_source import PreparationSourceSnapshot, freeze_preparation_source
from .monthly_source_audit import close_source_audit
from .monthly_frozen_source_audit import AUDIT_SCHEMA, audit_frozen_source
from .monthly_source_producer import (
    MonthlySourceReadSet,
    MonthlySourcePreparationReadSet,
    SourceArtifact,
)
from .monthly_unified import SOURCE_GATES, MonthlyReleaseSourceBlocked, SourceChange
from .monthly_worker import ProducerContext
from .profile import CANONICAL_PROFILE_ID, DatasetProfile
from .source_authority import (
    FrozenSourceAuthoritySnapshot,
    MonthlySourceAuthority,
    SourceAuditIncomplete,
    SOURCE_REUSE_MANIFEST_SCHEMA,
    MONTHLY_SECTOR_SOURCE_POLICY,
    imported_source_session_factory,
    seal_source_stage_receipt,
)


FROZEN_SOURCE_BUNDLE_SCHEMA = "aistock_monthly_frozen_source_bundle_v1"
SOURCE_DIFF_SCHEMA = "aistock_monthly_frozen_source_diff_v1"
POSTGRES_SOURCE_ADAPTER_VERSION = "6"
REFRESH_READINESS_POLICY = "same_snapshot_all_dated_ranges_before_payload_v1"
_PARTITION_DATE = re.compile(r"(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})")

_CHANGE_DATASET_ALIASES = {
    "moneyflow_ts": "moneyflow",
    "sector_data": "industry_classification",
    "sw_index_classify": "industry_classification",
    "sw_index_member": "industry_classification",
}


class MonthlyPostgresSourceError(RuntimeError):
    """The frozen PostgreSQL source handoff is incomplete or ambiguous."""


def _preflight_refresh_readiness(
    authority: MonthlySourceAuthority,
    session_factory: Any,
    *,
    profile: DatasetProfile,
    cutoff: date,
    operation_id: str | None = None,
) -> None:
    """Check the existing audit policy before streaming any source payload.

    Both this small control read and the subsequent freeze import the same
    coordinator snapshot. This is an ordering optimization, not proof of fact
    completeness: partition checks and the nine frozen-source gates still run.
    """
    with session_factory(profile.resource_policy) as session:
        ledger = authority._freeze_refresh_audit(session, cutoff=cutoff, checkpoint=lambda: None)
    ranges = sorted(
        {
            (
                str(query.audit_dataset),
                profile.minute_start_date if query.start_policy == "minute" else profile.start_date,
            )
            for query in authority._database_query_specs()
            if query.date_expression is not None
        }
    )
    blockers = []
    for dataset, start in ranges:
        try:
            ledger.partition_digest(dataset, start, cutoff)
        except SourceAuditIncomplete as exc:
            # Only emit the registered ledger's non-secret typed diagnostics.
            blockers.append(
                {
                    key: value
                    for key, value in exc.context.items()
                    if key
                    in {
                        "dataset",
                        "start",
                        "end",
                        "missing_count",
                        "missing_sample",
                        "unusable_count",
                        "unusable_sample",
                        "eligible_sources",
                        "eligible_quality_statuses",
                    }
                }
            )
    if blockers:
        preparation = (
            preparation_plan(
                operation_id=operation_id,
                cutoff=cutoff,
                blocking_datasets=tuple(str(item.get("dataset") or "unknown_source") for item in blockers),
            )
            if operation_id is not None
            else None
        )
        raise MonthlyReleaseSourceBlocked(
            "monthly source refresh-audit readiness is incomplete",
            context={
                "reason_code": "BLOCKED_SOURCE_REFRESH_AUDIT_INCOMPLETE",
                "requested_cutoff": cutoff.isoformat(),
                "readiness_policy": REFRESH_READINESS_POLICY,
                "blocker_count": len(blockers),
                "blockers": blockers,
                "database_write_performed": False,
                "source_payload_materialized": False,
                "component_preparation_plan": preparation,
                "component_preparation_execution": "NOT_STARTED",
            },
        )


@dataclass(frozen=True, slots=True)
class _SourceDiff:
    dataset: str
    kind: str
    start: date
    end: date
    partition_keys: tuple[str, ...]

    def payload(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "kind": self.kind,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "partition_keys": list(self.partition_keys),
        }


def _write_canonical_exclusive(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_json_bytes(value) + b"\n"
    with path.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _cas_artifact(cas: CASStore, reference: CASRef) -> SourceArtifact:
    verified = cas.verify(reference)
    return SourceArtifact(
        artifact_id=verified.relative_path,
        path=cas.root / verified.relative_path,
    )


def _partition_bounds(partition_key: str, *, fallback_start: date, fallback_end: date) -> tuple[date, date]:
    match = _PARTITION_DATE.search(partition_key)
    if match is None:
        return fallback_start, fallback_end
    return date.fromisoformat(match.group("start")), date.fromisoformat(match.group("end"))


def _partition_index(value: Mapping[str, Any]) -> dict[tuple[str, str], Mapping[str, Any]]:
    rows = value.get("partitions")
    if not isinstance(rows, list) or not all(isinstance(item, Mapping) for item in rows):
        raise MonthlyPostgresSourceError("source reuse manifest partitions are invalid")
    result: dict[tuple[str, str], Mapping[str, Any]] = {}
    for raw in rows:
        key = (str(raw.get("dataset") or ""), str(raw.get("partition_key") or ""))
        if not all(key) or key in result:
            raise MonthlyPostgresSourceError("source partition identity is empty or duplicated")
        result[key] = raw
    return result


def _source_diffs(
    *,
    baseline: Mapping[str, Any] | None,
    current: Mapping[str, Any],
    predecessor_cutoff: date,
    target_cutoff: date,
    pit_changed: bool,
) -> tuple[_SourceDiff, ...]:
    current_rows = _partition_index(current)
    baseline_rows = _partition_index(baseline) if baseline is not None else {}
    grouped: dict[tuple[str, str, date, date], list[str]] = {}

    for identity, row in sorted(current_rows.items()):
        dataset, partition_key = identity
        previous = baseline_rows.get(identity)
        start, end = _partition_bounds(
            partition_key,
            fallback_start=predecessor_cutoff + (date.resolution),
            fallback_end=target_cutoff,
        )
        if previous is None:
            kind = "TAIL_APPEND" if start > predecessor_cutoff else "NEW_SECURITY_HISTORY"
        elif previous.get("schema_digest") != row.get("schema_digest"):
            kind = "SCHEMA_CHANGE"
        elif previous.get("content_digest") == row.get("content_digest") and previous.get("row_count") == row.get(
            "row_count"
        ):
            continue
        else:
            kind = "HISTORICAL_REPAIR"
        normalized = _CHANGE_DATASET_ALIASES.get(dataset, dataset)
        grouped.setdefault((normalized, kind, start, end), []).append(partition_key)

    removed = sorted(set(baseline_rows).difference(current_rows))
    for dataset, partition_key in removed:
        start, end = _partition_bounds(
            partition_key,
            fallback_start=predecessor_cutoff,
            fallback_end=target_cutoff,
        )
        normalized = _CHANGE_DATASET_ALIASES.get(dataset, dataset)
        grouped.setdefault((normalized, "SCHEMA_CHANGE", start, end), []).append(partition_key)

    if baseline is None:
        # The first unified-v2 run has no compatible frozen source lineage.
        # Force every source-owned component through a complete, evidenced
        # rebuild instead of adopting an unproven predecessor tree.
        required = {
            "kline_daily_raw",
            "kline_minute_raw",
            "adj_factor",
            "daily_basic",
            "moneyflow",
            "suspend_d",
            "stk_limit",
            "index_daily",
            "stock_universe_pit",
            "index_membership_pit",
            "industry_classification",
        }
        for dataset in sorted(required):
            grouped.setdefault(
                (dataset, "SCHEMA_CHANGE", predecessor_cutoff + date.resolution, target_cutoff),
                [],
            )
    if pit_changed:
        grouped.setdefault(
            ("stock_universe_pit", "PIT_REVISION", predecessor_cutoff + date.resolution, target_cutoff),
            [],
        )
    return tuple(
        _SourceDiff(dataset, kind, start, end, tuple(sorted(keys)))
        for (dataset, kind, start, end), keys in sorted(grouped.items())
    )


@dataclass(slots=True)
class PostgresMonthlySourceAdapter:
    """Freeze production source rows and translate them to the v2 SOURCE contract."""

    profile: DatasetProfile
    cas: CASStore
    artifact_root: Path
    source_catalog: ControlStore
    mvcc_partition_reuse: bool = False
    preparation_executor: (
        Callable[
            [ProducerContext, PreparationSourceSnapshot, Mapping[str, Any], MonthlySnapshotIdentity], Mapping[str, Any]
        ]
        | None
    ) = None

    adapter_id: str = "aistock.monthly.postgres_source"
    adapter_version: str = POSTGRES_SOURCE_ADAPTER_VERSION

    def __post_init__(self) -> None:
        if not self.artifact_root.is_absolute() or not self.artifact_root.is_dir():
            raise ValueError("monthly source artifact root must be an existing absolute directory")
        resolved_artifact_root = self.artifact_root.resolve(strict=True)
        if self.cas.root != self.source_catalog.root or self.cas.root != resolved_artifact_root:
            raise ValueError("monthly source CAS, catalog and artifact root must share one control root")

    @property
    def contract_sha256(self) -> str:
        return digest_named_fields(
            "aistock_monthly_postgres_source_adapter_v1",
            {
                "profile": self.profile.profile,
                "semantic_profile_digest": self.profile.semantic_profile_digest,
                "source_authority_policy": "dataset_release_source_authority_v1",
                "sector_source_policy": MONTHLY_SECTOR_SOURCE_POLICY,
                "artifact_ready_contract": "dataset_release_artifact_ready_contract_v1",
                "snapshot_policy": "postgres_exported_repeatable_read_read_only_v1",
                "pit_readiness_policy": "same_snapshot_pre_materialization_v1",
                "refresh_audit_readiness_policy": REFRESH_READINESS_POLICY,
                "component_preparation_dependency_digest": digest_named_fields(
                    "aistock_monthly_component_dependency_v1",
                    component_dependencies(),
                ),
                "mvcc_partition_reuse": self.mvcc_partition_reuse,
                "gates": list(SOURCE_GATES),
                "source_audit_contract": AUDIT_SCHEMA,
            },
        )

    def read(
        self,
        connection: SnapshotConnection,
        identity: MonthlySnapshotIdentity,
        context: ProducerContext,
    ) -> MonthlySourceReadSet | MonthlySourcePreparationReadSet:
        predecessor_cutoff = date.fromisoformat(str(context.plan["predecessor"]["cutoff"]))
        target_cutoff = date.fromisoformat(str(context.plan["target_cutoff"]))
        if target_cutoff <= predecessor_cutoff:
            raise MonthlyPostgresSourceError("monthly source cutoff did not advance")
        if self.profile.profile == CANONICAL_PROFILE_ID:
            self._require_pit_coverage(connection, target_cutoff)

        baseline_row = self.source_catalog.latest_source_snapshot(
            profile=self.profile.profile,
            scope=Scope.FULL.value,
            cutoff_on_or_before=predecessor_cutoff,
        )
        baseline_manifest: Mapping[str, Any] | None = None
        baseline_partitions: Sequence[Mapping[str, Any]] = ()
        if baseline_row is not None and baseline_row.get("cutoff") == predecessor_cutoff.isoformat():
            raw = self.cas.get_json_bounded(
                str(baseline_row["source_reuse_manifest_ref"]),
                max_bytes=64 * 1024 * 1024,
            )
            if (
                not isinstance(raw, Mapping)
                or raw.get("schema_version") != SOURCE_REUSE_MANIFEST_SCHEMA
                or raw.get("profile") != self.profile.profile
                or raw.get("cutoff") != predecessor_cutoff.isoformat()
            ):
                raise MonthlyPostgresSourceError("source reuse baseline differs from predecessor")
            baseline_manifest = raw
            baseline_partitions = tuple(_partition_index(raw).values())
        else:
            baseline_row = None

        session_factory = imported_source_session_factory(
            identity.snapshot_id,
            connection_factory=independent_postgres_connection_factory,
        )
        authority = MonthlySourceAuthority(
            self.profile,
            self.cas,
            session_factory=session_factory,
            mvcc_reuse_capability=self.mvcc_partition_reuse,
            sector_source_policy=MONTHLY_SECTOR_SOURCE_POLICY,
        )
        try:
            _preflight_refresh_readiness(
                authority,
                session_factory,
                profile=self.profile,
                cutoff=target_cutoff,
                operation_id=context.operation_id,
            )
        except MonthlyReleaseSourceBlocked as blocked:
            # No optional caller PASS flags: only the code-owned registry may
            # install the preparation executor. Catch within snapshot.read so
            # the coordinator can still perform its repair-watermark seal.
            if self.preparation_executor is None:
                raise
            plan = blocked.context.get("component_preparation_plan")
            if not isinstance(plan, Mapping) or not plan.get("eligible_component_count"):
                raise
            frozen_private = freeze_preparation_source(
                authority,
                operation_id=context.operation_id,
                cutoff=target_cutoff,
                blocking_datasets=tuple(plan["blocking_datasets"]),
                # The same-snapshot preflight owns the exact unusable count.
                # A truncated sample or a historical hole cannot authorize
                # omitting only the tail. Keep those whole domains deferred.
                deferred_cutoff_datasets=tuple(
                    sorted(
                        {
                            str(item["dataset"])
                            for item in blocked.context.get("blockers", ())
                            if isinstance(item, Mapping)
                            and type(item.get("unusable_count")) is int
                            and item["unusable_count"] == 1
                            and item.get("unusable_sample") == [target_cutoff.isoformat()]
                            and item.get("dataset") in plan["blocking_datasets"]
                        }
                    )
                ),
            )
            private_root = (
                self.artifact_root
                / "monthly"
                / context.operation_id
                / "preparation-inputs"
                / f"attempt-{context.attempt}"
            )
            private_root.mkdir(parents=True, exist_ok=False)
            # These are ordinary SOURCE domain checks over frozen facts, not
            # an artificial all-gate PASS. The omitted financing gate stays
            # failed and cannot seal a full SOURCE or enter its reuse catalog.
            gates, audits = audit_frozen_source(
                cas=self.cas,
                frozen=frozen_private,
                profile=self.profile,
                input_root=private_root,
                artifact_root=self.artifact_root,
                snapshot_group_id=f"postgres:{identity.snapshot_id}",
                changes=(),
                predecessor_cutoff=self.profile.start_date - date.resolution,
            )
            audit_path = private_root / "private-source-audit.json"
            audit_body = {
                "schema_version": "aistock_monthly_preparation_source_audit_v1",
                "operation_id": context.operation_id,
                "cutoff": target_cutoff.isoformat(),
                "audit_start": self.profile.start_date.isoformat(),
                "source_manifest_ref": frozen_private.source_manifest_ref.as_dict(),
                "plan": dict(plan),
                "gates": [gate.payload() for gate in gates],
                "consistent_input_set_complete": False,
                "publication_allowed": False,
                "database_write_performed": False,
            }
            _write_canonical_exclusive(audit_path, audit_body)
            return MonthlySourcePreparationReadSet(
                snapshot_group_id=f"postgres:{identity.snapshot_id}",
                input_artifacts=(
                    *tuple(
                        _cas_artifact(self.cas, ref)
                        for ref in (
                            frozen_private.source_manifest_ref,
                            frozen_private.source_audit_ref,
                            frozen_private.pit_snapshot_ref,
                        )
                    ),
                    *audits,
                    SourceArtifact(audit_path.relative_to(self.artifact_root).as_posix(), audit_path),
                ),
                blocking_context=dict(blocked.context),
                preparation_token=(frozen_private, audit_body),
            )
        frozen = authority.freeze(
            cutoff=target_cutoff,
            baseline_partitions=baseline_partitions,
        )
        current_reuse = self.cas.get_json_bounded(
            frozen.source_reuse_manifest_ref,
            max_bytes=64 * 1024 * 1024,
        )
        if not isinstance(current_reuse, Mapping):
            raise MonthlyPostgresSourceError("current source reuse manifest is invalid")
        pit_changed = baseline_row is None or baseline_row.get("pit_snapshot_digest") != frozen.pit_snapshot_digest
        diffs = _source_diffs(
            baseline=baseline_manifest,
            current=current_reuse,
            predecessor_cutoff=predecessor_cutoff,
            target_cutoff=target_cutoff,
            pit_changed=pit_changed,
        )
        if not diffs:
            raise MonthlyPostgresSourceError("advanced cutoff produced an empty source change set")

        input_root = (
            self.artifact_root / "monthly" / context.operation_id / "source-inputs" / f"attempt-{context.attempt}"
        )
        input_root.mkdir(parents=True, exist_ok=False)

        def artifact(path: Path) -> SourceArtifact:
            return SourceArtifact(path.relative_to(self.artifact_root).as_posix(), path)

        diff_path = input_root / "source-diff.json"
        _write_canonical_exclusive(
            diff_path,
            {
                "schema_version": SOURCE_DIFF_SCHEMA,
                "predecessor_cutoff": predecessor_cutoff.isoformat(),
                "target_cutoff": target_cutoff.isoformat(),
                "baseline_source_content_root": (
                    baseline_row.get("source_content_root") if baseline_row is not None else None
                ),
                "current_source_content_root": frozen.source_content_root,
                "changes": [item.payload() for item in diffs],
                "database_write_performed": False,
            },
        )
        diff_sha = _sha256(diff_path)
        changes = tuple(
            SourceChange(
                dataset=item.dataset,
                fields=("*",),
                instruments=(),
                start=item.start,
                end=item.end,
                kind=item.kind,
                source_receipt_sha256=diff_sha,
            )
            for item in diffs
        )

        gates, audit_artifacts = audit_frozen_source(
            cas=self.cas,
            frozen=frozen,
            profile=self.profile,
            input_root=input_root,
            artifact_root=self.artifact_root,
            snapshot_group_id=f"postgres:{identity.snapshot_id}",
            changes=changes,
            predecessor_cutoff=predecessor_cutoff,
        )
        # Persist real blocked gate readbacks before any provider materialization
        # or seal. Failed audit evidence must never enter the reuse catalog.
        close_source_audit(cutoff=target_cutoff, predecessor_cutoff=predecessor_cutoff, gates=gates, changes=changes)
        ready = ArtifactReadySourceBuilder(self.profile, self.cas).build(frozen)
        loaded = load_artifact_ready_contract(
            self.cas,
            self.profile,
            ready.artifact_ready_contract_ref,
            expected_source_content_root=frozen.source_content_root,
            expected_pit_snapshot_digest=frozen.pit_snapshot_digest,
        )
        frozen = replace(
            frozen,
            artifact_ready_contract_ref=ready.artifact_ready_contract_ref,
            artifact_ready_content_root=ready.artifact_ready_content_root,
            artifact_ready_provenance_root=loaded.artifact_ready_provenance_root,
            provider_receipt_refs=ready.provider_receipt_refs,
            artifact_ready_derived_source_receipt_refs=ready.derived_source_receipt_refs,
        )
        source_stage_ref = seal_source_stage_receipt(self.cas, frozen, profile=self.profile.profile)

        bundle_path = input_root / "frozen-source-bundle.json"
        bundle = self._bundle(
            frozen,
            identity=identity,
            predecessor_cutoff=predecessor_cutoff,
            baseline_row=baseline_row,
            source_stage_ref=source_stage_ref,
        )
        _write_canonical_exclusive(bundle_path, bundle)

        artifacts: list[SourceArtifact] = [
            artifact(diff_path),
            artifact(bundle_path),
            *audit_artifacts,
        ]

        cas_refs = self._all_refs(frozen, source_stage_ref)
        artifacts.extend(_cas_artifact(self.cas, reference) for reference in cas_refs)
        return MonthlySourceReadSet(
            gates=tuple(gates),
            changes=changes,
            input_artifacts=tuple(artifacts),
            seal_token=self._catalog_spec(frozen, identity=identity),
        )

    def _require_pit_coverage(self, connection: SnapshotConnection, cutoff: date) -> None:
        # The coordinator has already imported its read-only snapshot. Check
        # readiness before any CAS partition, freeze or baseline materialization.
        # This does not extend spans or replace the later exact PIT validation.
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT start_date,end_date,status,dirty FROM market.stock_universe_pit_state WHERE universe_key=%s",
                (self.profile.universe_key,),
            )
            row = cursor.fetchone()
        if row is not None and len(row) == 4:
            start, end, status, dirty = row
            if (
                type(start) is date
                and type(end) is date
                and start <= self.profile.start_date
                and end >= cutoff
                and status == "ready"
                and dirty is False
            ):
                return
        else:
            start, end, status, dirty = None, None, None, None
        raise MonthlyReleaseSourceBlocked(
            "canonical PIT authority is not ready for the requested source window",
            context={
                "reason_code": "BLOCKED_PIT_STATE_NOT_READY",
                "universe_key": self.profile.universe_key,
                "requested_start": self.profile.start_date.isoformat(),
                "requested_cutoff": cutoff.isoformat(),
                "state_start": start.isoformat() if type(start) is date else None,
                "state_end": end.isoformat() if type(end) is date else None,
                "state_status": status if isinstance(status, str) else None,
                "state_dirty": dirty if type(dirty) is bool else None,
                "operator_script": "scripts/prepare_canonical_pit_monthly.py",
                "production_apply_requires_authorization": True,
                "database_write_performed": False,
            },
        )

    def _bundle(
        self,
        frozen: FrozenSourceAuthoritySnapshot,
        *,
        identity: MonthlySnapshotIdentity,
        predecessor_cutoff: date,
        baseline_row: Mapping[str, Any] | None,
        source_stage_ref: CASRef,
    ) -> dict[str, Any]:
        if frozen.artifact_ready_contract_ref is None:
            raise MonthlyPostgresSourceError("frozen source lacks artifact-ready authority")
        return {
            "schema_version": FROZEN_SOURCE_BUNDLE_SCHEMA,
            "profile": self.profile.profile,
            "cutoff": frozen.official_cutoff.isoformat(),
            "predecessor_cutoff": predecessor_cutoff.isoformat(),
            "snapshot_group_id": f"postgres:{identity.snapshot_id}",
            "source_as_of": identity.source_as_of,
            "source_content_root": frozen.source_content_root,
            "source_provenance_root": frozen.source_provenance_root,
            "stable_source_provenance_root": frozen.stable_source_provenance_root,
            "pit_snapshot_digest": frozen.pit_snapshot_digest,
            "source_manifest_ref": frozen.source_manifest_ref.as_dict(),
            "source_reuse_manifest_ref": frozen.source_reuse_manifest_ref.as_dict(),
            "source_audit_ref": frozen.source_audit_ref.as_dict(),
            "source_provenance_ref": frozen.source_provenance_ref.as_dict(),
            "pit_snapshot_ref": frozen.pit_snapshot_ref.as_dict(),
            "artifact_ready_contract_ref": frozen.artifact_ready_contract_ref.as_dict(),
            "source_stage_receipt_ref": source_stage_ref.as_dict(),
            "artifact_ready_content_root": frozen.artifact_ready_content_root,
            "artifact_ready_provenance_root": frozen.artifact_ready_provenance_root,
            "provider_receipt_refs": [item.as_dict() for item in frozen.provider_receipt_refs],
            "derived_source_receipt_refs": [
                item.as_dict()
                for item in (
                    *frozen.derived_source_receipt_refs,
                    *frozen.artifact_ready_derived_source_receipt_refs,
                )
            ],
            "baseline": (
                {
                    "cutoff": baseline_row["cutoff"],
                    "source_content_root": baseline_row["source_content_root"],
                    "source_reuse_manifest_ref": baseline_row["source_reuse_manifest_ref"],
                    "pit_snapshot_digest": baseline_row["pit_snapshot_digest"],
                }
                if baseline_row is not None
                else None
            ),
            "source_cas_usage": dict(frozen.source_cas_usage),
            "database_write_performed": False,
            "runtime_fallback": False,
        }

    @staticmethod
    def _all_refs(
        frozen: FrozenSourceAuthoritySnapshot,
        source_stage_ref: CASRef,
    ) -> tuple[CASRef, ...]:
        values = [
            frozen.source_manifest_ref,
            frozen.source_reuse_manifest_ref,
            frozen.source_audit_ref,
            frozen.source_provenance_ref,
            frozen.pit_snapshot_ref,
            source_stage_ref,
            *frozen.derived_source_receipt_refs,
            *frozen.provider_receipt_refs,
            *frozen.artifact_ready_derived_source_receipt_refs,
        ]
        if frozen.artifact_ready_contract_ref is not None:
            values.append(frozen.artifact_ready_contract_ref)
        by_digest = {item.sha256: item for item in values}
        return tuple(by_digest[key] for key in sorted(by_digest))

    def _catalog_spec(
        self,
        frozen: FrozenSourceAuthoritySnapshot,
        *,
        identity: MonthlySnapshotIdentity,
    ) -> SourceSnapshotCatalogSpec:
        observed_at = datetime.fromisoformat(identity.source_as_of)
        return SourceSnapshotCatalogSpec(
            observation_id=digest_named_fields(
                "aistock_monthly_source_snapshot_observation_v2",
                {
                    "profile": self.profile.profile,
                    "cutoff": frozen.official_cutoff,
                    "source_content_root": frozen.source_content_root,
                    "pit_snapshot_digest": frozen.pit_snapshot_digest,
                    "snapshot_group_id": f"postgres:{identity.snapshot_id}",
                },
            ),
            profile=self.profile.profile,
            scope=Scope.FULL.value,
            cutoff=frozen.official_cutoff,
            source_content_root=frozen.source_content_root,
            source_provenance_root=frozen.source_provenance_root,
            stable_source_provenance_root=frozen.stable_source_provenance_root,
            source_content_manifest_ref=frozen.source_manifest_ref.sha256,
            source_reuse_manifest_ref=frozen.source_reuse_manifest_ref.sha256,
            source_refresh_audit_ref=frozen.source_audit_ref.sha256,
            source_provenance_ref=frozen.source_provenance_ref.sha256,
            pit_snapshot_digest=frozen.pit_snapshot_digest,
            pit_snapshot_ref=frozen.pit_snapshot_ref.sha256,
            observed_at=observed_at,
        )

    def snapshot_sealed(self, context: ProducerContext, token: object) -> None:
        """Register reuse lineage only after the outer repair-overlap seal passes."""

        del context
        if not isinstance(token, SourceSnapshotCatalogSpec):
            raise MonthlyPostgresSourceError("sealed monthly source lacks catalog evidence")
        self.source_catalog.register_source_snapshot(token)

    def preparation_snapshot_sealed(
        self,
        context: ProducerContext,
        token: object,
        identity: MonthlySnapshotIdentity,
    ) -> Mapping[str, Any]:
        """Execute only from the outer coordinator's post-overlap handoff."""
        if (
            self.preparation_executor is None
            or not isinstance(token, tuple)
            or len(token) != 2
            or not isinstance(token[0], PreparationSourceSnapshot)
            or not isinstance(token[1], Mapping)
            or token[0].operation_id != context.operation_id
            or token[0].official_cutoff.isoformat() != context.plan.get("target_cutoff")
            or token[1].get("schema_version") != "aistock_monthly_preparation_source_audit_v1"
            or token[1].get("operation_id") != context.operation_id
            or token[1].get("source_manifest_ref") != token[0].source_manifest_ref.as_dict()
            or not isinstance(token[1].get("gates"), list)
            or len(token[1]["gates"]) != len(SOURCE_GATES)
            or {gate.get("gate_id") for gate in token[1]["gates"]} != set(SOURCE_GATES)
            or any(
                gate.get("snapshot_group_id") != f"postgres:{identity.snapshot_id}"
                for gate in token[1].get("gates", ())
            )
        ):
            raise MonthlyPostgresSourceError("private source executor handoff identity differs")
        return self.preparation_executor(context, token[0], token[1], identity)


__all__: Sequence[str] = (
    "FROZEN_SOURCE_BUNDLE_SCHEMA",
    "MonthlyPostgresSourceError",
    "PostgresMonthlySourceAdapter",
)
