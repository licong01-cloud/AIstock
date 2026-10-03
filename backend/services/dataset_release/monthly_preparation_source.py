"""Private, incomplete source freezes for dependency-level preparation.

This composes the production authority's SQL, bounded row sealer, PIT and
writer/control checks. It never creates a full SOURCE manifest, stage receipt
or reuse-catalog entry. A lost snapshot invalidates the entire private view.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from typing import Any, Callable, Mapping

from .canonical import digest_named_fields
from .cas_store import CASRef
from .source_manifest import SourceManifest
from .monthly_component_preparation import ComponentPreparationError, preparation_plan
from .sector_enrichment import FrozenSectorEnricher
from .source_authority import (
    MONTHLY_SECTOR_SOURCE_POLICY,
    PRODUCTION_QUERY_SPECS,
    MonthlySourceAuthority,
    SealedSourcePartition,
    SourceCASBudgetTracker,
    SourceRequiredDatasetEmpty,
    SourceSnapshotDriftBlocked,
    _as_date,
    _validate_core_index_membership_authority,
)


PREPARATION_SOURCE_SCHEMA = "aistock_monthly_preparation_source_v1"


@dataclass(frozen=True, slots=True)
class PreparationSourceSnapshot:
    """Deliberately not FrozenSourceAuthoritySnapshot or a full SOURCE token."""

    operation_id: str
    official_cutoff: date
    pit_snapshot: Any
    pit_snapshot_ref: CASRef
    source_manifest_ref: CASRef
    source_audit_ref: CASRef
    partitions: tuple[SealedSourcePartition, ...]
    pit_partitions: tuple[SealedSourcePartition, ...]
    snapshot_tokens: tuple[str, ...]
    omitted_datasets: tuple[str, ...]
    control_digest: str
    manifest: SourceManifest
    deferred_cutoff_datasets: tuple[str, ...] = ()

    @property
    def source_content_root(self) -> str:
        return self.manifest.source_content_root

    @property
    def pit_snapshot_digest(self) -> str:
        return self.pit_snapshot.spans_sha256


def freeze_preparation_source(
    authority: MonthlySourceAuthority,
    *,
    operation_id: str,
    cutoff: date,
    blocking_datasets: tuple[str, ...],
    deferred_cutoff_datasets: tuple[str, ...] = (),
    checkpoint: Callable[[], None] = lambda: None,
    disk_checkpoint: Callable[[int | None], Any] | None = None,
) -> PreparationSourceSnapshot:
    """Freeze healthy raw datasets once in the imported coordinator snapshot.

    No refresh row is accepted as content proof. Every included dataset still
    uses its production refresh contract and exact row/key/schema validation.
    The blocked domain is omitted, not assigned a provider-absence or zero.
    """
    plan = preparation_plan(
        operation_id=operation_id,
        cutoff=cutoff,
        blocking_datasets=blocking_datasets,
    )
    omitted = tuple(plan["blocking_datasets"])
    if (
        not isinstance(deferred_cutoff_datasets, tuple)
        or any(not isinstance(item, str) for item in deferred_cutoff_datasets)
        or len(set(deferred_cutoff_datasets)) != len(deferred_cutoff_datasets)
        or not set(deferred_cutoff_datasets) <= set(omitted)
    ):
        raise ComponentPreparationError("private deferred cutoff datasets differ from blockers")
    if (
        not omitted
        or plan["unknown_blocking_datasets"]
        or not plan["eligible_component_count"]
        or authority.uses_p3a_sector_source
        or authority._sector_source_policy != MONTHLY_SECTOR_SOURCE_POLICY
        or authority._mvcc_reuse_capability
    ):
        raise ComponentPreparationError("private preparation source policy is not supported")
    # A preparation-only omission may not disable the common control authority
    # or the classification authority required by the official source audit.
    if set(omitted) & {"trading_calendar", "stock_universe_pit", "sw_index_classify", "sw_index_member"}:
        raise ComponentPreparationError("private preparation control authority is incomplete")
    chunk = authority.profile.resource_policy.validation_read_chunk_rows
    budget = SourceCASBudgetTracker(disk_checkpoint=disk_checkpoint)
    before = authority._capture_control_snapshot(
        cutoff=cutoff,
        pulse=checkpoint,
        budget=budget,
        read_chunk_rows=chunk,
        recheck_by_identity=None,
        selected_stock_codes=(),
    )
    sealed: list[SealedSourcePartition] = []
    tokens = list(before.snapshot_tokens)
    classify_rows: list[Mapping[str, Any]] = []
    member_rows: list[Mapping[str, Any]] = []
    core_rows: list[Mapping[str, Any]] = []
    enricher: FrozenSectorEnricher | None = None
    ordered = (
        PRODUCTION_QUERY_SPECS["sw_index_classify"],
        PRODUCTION_QUERY_SPECS["sw_index_member"],
        *(
            query
            for key, query in PRODUCTION_QUERY_SPECS.items()
            if key not in {"sw_index_classify", "sw_index_member"}
        ),
    )
    for query in ordered:
        if query.query_id in omitted and query.query_id not in deferred_cutoff_datasets:
            continue
        if query.query_id == "sector_data":
            enricher = FrozenSectorEnricher.build(classify_rows, member_rows)
            query = replace(query, query_version=f"{query.query_version}:{MONTHLY_SECTOR_SOURCE_POLICY}")
        schema = before.schemas[query.query_id]
        observed_rows = 0
        for key, params in authority._partition_requests(query, cutoff, pit_snapshot=before.pit_snapshot):
            checkpoint()
            if query.query_id in deferred_cutoff_datasets:
                if query.date_expression is None:
                    raise ComponentPreparationError("private deferred cutoff source is not dated")
                left, right = _as_date(params["start"]), _as_date(params["end"])
                if right >= cutoff:
                    right = cutoff - date.resolution
                    if left > right:
                        continue
                    original = f"{_as_date(params['start']).isoformat()}_{_as_date(params['end']).isoformat()}"
                    if original not in key:
                        raise ComponentPreparationError("private deferred source partition boundary differs")
                    key = key.replace(original, f"{left.isoformat()}_{right.isoformat()}", 1)
                    params = {**params, "end": right}
            audit_digest = (
                before.audit.partition_digest(
                    str(query.audit_dataset), _as_date(params["start"]), _as_date(params["end"])
                )
                if query.date_expression is not None
                else None
            )
            with authority._session_factory(authority.profile.resource_policy) as session:
                session_tokens = authority._session_tokens(session)
                if session.describe(query.query_id).digest != schema.digest:
                    raise SourceSnapshotDriftBlocked(
                        "preparation source schema changed", context={"query_id": query.query_id}
                    )
                partition = authority._seal_query_partition(
                    session,
                    query=query,
                    partition_key=key,
                    params=params,
                    tokens=session_tokens,
                    table_schema=schema,
                    refresh_audit_digest=audit_digest,
                    checkpoint=checkpoint,
                    budget=budget,
                    read_chunk_rows=chunk,
                    payload_enricher=enricher.enrich if query.query_id == "sector_data" and enricher else None,
                )
            sealed.append(partition)
            observed_rows += partition.summary.row_count
            tokens.extend(session_tokens)
            if query.query_id in {"sw_index_classify", "sw_index_member", "index_membership_pit"}:
                from .sealed_source_reader import CASSealedPartitionReader

                reader = CASSealedPartitionReader(
                    authority.cas, [partition.as_build_input()], max_partition_rows=query.max_partition_rows
                )
                target = (
                    classify_rows
                    if query.query_id == "sw_index_classify"
                    else member_rows
                    if query.query_id == "sw_index_member"
                    else core_rows
                )
                with reader.iter_rows(query.query_id, key) as rows:
                    target.extend(rows)
        if observed_rows == 0 and query.query_id not in deferred_cutoff_datasets:
            raise SourceRequiredDatasetEmpty(
                "preparation required dataset is empty", context={"query_id": query.query_id}
            )
        if query.query_id == "index_membership_pit":
            _validate_core_index_membership_authority(core_rows, start=authority.profile.start_date, cutoff=cutoff)
        with authority._session_factory(authority.profile.resource_policy) as session:
            ledger_digest, _ = authority._freeze_writer_ledger(
                session, cutoff=cutoff, checkpoint=checkpoint, read_chunk_rows=chunk
            )
            tokens.extend(authority._session_tokens(session))
        if ledger_digest != before.writer_ledger_digest:
            raise SourceSnapshotDriftBlocked(
                "preparation source writer ledger changed", context={"query_id": query.query_id}
            )
    after = authority._capture_control_snapshot(
        cutoff=cutoff,
        pulse=checkpoint,
        budget=None,
        read_chunk_rows=chunk,
        recheck_by_identity=None,
        selected_stock_codes=(),
    )
    if after.consistency_digest != before.consistency_digest:
        raise SourceSnapshotDriftBlocked("preparation source control bracket changed")
    tokens.extend(after.snapshot_tokens)
    manifest = SourceManifest(tuple(item.summary for item in sealed))
    pit_ref = authority.cas.put_bytes(before.pit_snapshot.canonical_bytes())
    audit_ref = authority.cas.put_json(before.audit.as_receipt(profile=authority.profile.profile, cutoff=cutoff))
    body = {
        "schema_version": PREPARATION_SOURCE_SCHEMA,
        "operation_id": operation_id,
        "profile": authority.profile.profile,
        "cutoff": cutoff.isoformat(),
        "omitted_datasets": list(omitted),
        "deferred_cutoff_datasets": list(sorted(deferred_cutoff_datasets)),
        "partitions": [item.as_build_input() for item in sorted(sealed, key=lambda item: item.spec.identity)],
        "source_content_root": manifest.source_content_root,
        "pit_snapshot_ref": pit_ref.as_dict(),
        "pit_snapshot_digest": before.pit_snapshot.spans_sha256,
        "source_refresh_audit_ref": audit_ref.as_dict(),
        "control_digest": before.consistency_digest,
        "snapshot_tokens": tokens,
        "consistent_input_set_complete": False,
        "publication_allowed": False,
        "database_write_performed": False,
    }
    source_ref = authority.cas.put_json(
        {**body, "canonical_digest": digest_named_fields(PREPARATION_SOURCE_SCHEMA, body)}
    )
    for ref in (pit_ref, audit_ref, source_ref):
        authority.cas.verify(ref)
    return PreparationSourceSnapshot(
        operation_id,
        cutoff,
        before.pit_snapshot,
        pit_ref,
        source_ref,
        audit_ref,
        tuple(sealed),
        tuple(before.pit_partitions),
        tuple(tokens),
        omitted,
        before.consistency_digest,
        manifest,
        tuple(sorted(deferred_cutoff_datasets)),
    )
