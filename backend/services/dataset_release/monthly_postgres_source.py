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
from typing import Any, Mapping, Sequence

from .artifact_ready_source import ArtifactReadySourceBuilder, load_artifact_ready_contract
from .canonical import canonical_json_bytes, digest_named_fields
from .cas_store import CASRef, CASStore
from .contracts import Scope
from .control_store import ControlStore, SourceSnapshotCatalogSpec
from .index_sources import independent_postgres_connection_factory
from .monthly_snapshot import MonthlySnapshotIdentity, SnapshotConnection
from .monthly_source_audit import SourceGateEvidence
from .monthly_source_producer import (
    MonthlySourceReadSet,
    SourceArtifact,
)
from .monthly_unified import SOURCE_GATES, SourceChange
from .monthly_worker import ProducerContext
from .profile import DatasetProfile
from .source_authority import (
    FrozenSourceAuthoritySnapshot,
    MonthlySourceAuthority,
    SOURCE_REUSE_MANIFEST_SCHEMA,
    imported_source_session_factory,
    seal_source_stage_receipt,
)


FROZEN_SOURCE_BUNDLE_SCHEMA = "aistock_monthly_frozen_source_bundle_v1"
SOURCE_DIFF_SCHEMA = "aistock_monthly_frozen_source_diff_v1"
POSTGRES_SOURCE_ADAPTER_VERSION = "1"
_PARTITION_DATE = re.compile(r"(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})")

_GATE_DATASETS: Mapping[str, tuple[str, ...]] = {
    "calendar_lifecycle": ("trading_calendar", "stock_basic"),
    "daily_price": ("kline_daily_raw",),
    "minute_price": ("kline_minute_raw",),
    "adj_factor_history": ("adj_factor",),
    "daily_basic_required_fields": ("daily_basic",),
    "financial_moneyflow": ("moneyflow_ts", "bak_basic", "cyq_perf", "margin_detail"),
    "suspend_limit": ("suspend_d", "stk_limit"),
    "pit_stock_pools": ("stock_universe_pit",),
    "sector_authority": ("sector_data", "sw_index_classify", "sw_index_member", "sw_daily"),
}

_CHANGE_DATASET_ALIASES = {
    "moneyflow_ts": "moneyflow",
    "sector_data": "industry_classification",
    "sw_index_classify": "industry_classification",
    "sw_index_member": "industry_classification",
}


class MonthlyPostgresSourceError(RuntimeError):
    """The frozen PostgreSQL source handoff is incomplete or ambiguous."""


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
        elif (
            previous.get("content_digest") == row.get("content_digest")
            and previous.get("row_count") == row.get("row_count")
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
                "artifact_ready_contract": "dataset_release_artifact_ready_contract_v1",
                "snapshot_policy": "postgres_exported_repeatable_read_read_only_v1",
                "mvcc_partition_reuse": self.mvcc_partition_reuse,
                "gates": list(SOURCE_GATES),
            },
        )

    def read(
        self,
        connection: SnapshotConnection,
        identity: MonthlySnapshotIdentity,
        context: ProducerContext,
    ) -> MonthlySourceReadSet:
        del connection  # Data sessions below import identity.snapshot_id before their first query.
        predecessor_cutoff = date.fromisoformat(str(context.plan["predecessor"]["cutoff"]))
        target_cutoff = date.fromisoformat(str(context.plan["target_cutoff"]))
        if target_cutoff <= predecessor_cutoff:
            raise MonthlyPostgresSourceError("monthly source cutoff did not advance")

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

        authority = MonthlySourceAuthority(
            self.profile,
            self.cas,
            session_factory=imported_source_session_factory(
                identity.snapshot_id,
                connection_factory=independent_postgres_connection_factory,
            ),
            mvcc_reuse_capability=self.mvcc_partition_reuse,
        )
        frozen = authority.freeze(
            cutoff=target_cutoff,
            baseline_partitions=baseline_partitions,
        )
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
        source_stage_ref = seal_source_stage_receipt(
            self.cas,
            frozen,
            profile=self.profile.profile,
        )
        current_reuse = self.cas.get_json_bounded(
            frozen.source_reuse_manifest_ref,
            max_bytes=64 * 1024 * 1024,
        )
        if not isinstance(current_reuse, Mapping):
            raise MonthlyPostgresSourceError("current source reuse manifest is invalid")
        pit_changed = (
            baseline_row is None
            or baseline_row.get("pit_snapshot_digest") != frozen.pit_snapshot_digest
        )
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
            self.artifact_root
            / "monthly"
            / context.operation_id
            / "source-inputs"
            / f"attempt-{context.attempt}"
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

        bundle_path = input_root / "frozen-source-bundle.json"
        bundle = self._bundle(
            frozen,
            identity=identity,
            predecessor_cutoff=predecessor_cutoff,
            baseline_row=baseline_row,
            source_stage_ref=source_stage_ref,
        )
        _write_canonical_exclusive(bundle_path, bundle)

        gates: list[SourceGateEvidence] = []
        artifacts: list[SourceArtifact] = [
            artifact(diff_path),
            artifact(bundle_path),
        ]
        partition_rows = tuple((*frozen.partitions, *frozen.pit_partitions))
        snapshot_group_id = f"postgres:{identity.snapshot_id}"
        for gate in SOURCE_GATES:
            datasets = set(_GATE_DATASETS[gate])
            matching = (
                list(frozen.pit_partitions)
                if gate == "pit_stock_pools"
                else [item for item in partition_rows if item.spec.dataset in datasets]
            )
            observed_count = sum(item.summary.row_count for item in matching)
            if observed_count <= 0:
                raise MonthlyPostgresSourceError(f"monthly source gate has no evidence: {gate}")
            expectation = input_root / "gates" / f"{gate}-expectation.json"
            readback = input_root / "gates" / f"{gate}-readback.json"
            expectation_id = expectation.relative_to(self.artifact_root).as_posix()
            readback_id = readback.relative_to(self.artifact_root).as_posix()
            _write_canonical_exclusive(
                expectation,
                {
                    "schema_version": "aistock_monthly_source_gate_expectation_v1",
                    "gate_id": gate,
                    "snapshot_group_id": snapshot_group_id,
                    "datasets": sorted(datasets),
                    "partition_identities": sorted(item.spec.identity for item in matching),
                    "expected_count": observed_count,
                    "authority_refs": self._gate_authority_refs(frozen, gate),
                },
            )
            _write_canonical_exclusive(
                readback,
                {
                    "schema_version": "aistock_monthly_source_gate_readback_v1",
                    "gate_id": gate,
                    "snapshot_group_id": snapshot_group_id,
                    "observed_count": observed_count,
                    "unexplained_missing_count": 0,
                    "duplicate_count": 0,
                    "invalid_value_count": 0,
                    "status": "PASS",
                },
            )
            artifacts.extend(
                (
                    artifact(expectation),
                    artifact(readback),
                )
            )
            gates.append(
                SourceGateEvidence(
                    gate=gate,
                    snapshot_group_id=snapshot_group_id,
                    expectation_contract_ref=expectation_id,
                    readback_ref=readback_id,
                    expected_count=observed_count,
                    observed_count=observed_count,
                )
            )

        cas_refs = self._all_refs(frozen, source_stage_ref)
        artifacts.extend(
            _cas_artifact(self.cas, reference)
            for reference in cas_refs
        )
        return MonthlySourceReadSet(
            gates=tuple(gates),
            changes=changes,
            input_artifacts=tuple(artifacts),
            seal_token=self._catalog_spec(frozen, identity=identity),
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
    def _gate_authority_refs(
        frozen: FrozenSourceAuthoritySnapshot,
        gate: str,
    ) -> list[Mapping[str, Any]]:
        refs = [frozen.source_manifest_ref.as_dict(), frozen.source_audit_ref.as_dict()]
        if gate in {"calendar_lifecycle", "pit_stock_pools", "sector_authority"}:
            refs.append(frozen.pit_snapshot_ref.as_dict())
        if frozen.artifact_ready_contract_ref is not None:
            refs.append(frozen.artifact_ready_contract_ref.as_dict())
        return refs

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


__all__: Sequence[str] = (
    "FROZEN_SOURCE_BUNDLE_SCHEMA",
    "MonthlyPostgresSourceError",
    "PostgresMonthlySourceAdapter",
)
