"""Code-owned SOURCE producer for unified monthly releases.

The adapter reads every registered source domain through one exported
PostgreSQL snapshot.  It emits canonical, content-addressed evidence directly
from the typed audit objects; callers cannot submit PASS strings or replace
the managed repair-watermark callbacks.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import os
from pathlib import Path
import time
from typing import Any, Callable, Mapping, Protocol, Sequence

from .canonical import canonical_json_bytes, ensure_sha256
from .monthly_snapshot import (
    MonthlySnapshotCoordinator,
    MonthlySnapshotIdentity,
    SnapshotConnection,
    managed_monthly_snapshot,
)
from .monthly_source_audit import SourceGateEvidence, close_source_audit
from .monthly_unified import SOURCE_GATES, SourceChange, classify_component_actions
from .monthly_worker import PRODUCER_EVIDENCE_SCHEMA, ProducerContext


class MonthlySourceProducerError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SourceArtifact:
    artifact_id: str
    path: Path

    def __post_init__(self) -> None:
        relative = Path(self.artifact_id)
        if (
            not self.artifact_id
            or relative.is_absolute()
            or ".." in relative.parts
            or "\\" in self.artifact_id
        ):
            raise ValueError("source input artifact id is not portable")
        if not self.path.is_absolute():
            raise ValueError("source input artifact path must be absolute")


@dataclass(frozen=True, slots=True)
class MonthlySourceReadSet:
    gates: tuple[SourceGateEvidence, ...]
    changes: tuple[SourceChange, ...]
    input_artifacts: tuple[SourceArtifact, ...]
    repair_receipts: tuple[Mapping[str, Any], ...] = ()
    seal_token: object | None = None

    def __post_init__(self) -> None:
        if {item.gate for item in self.gates} != set(SOURCE_GATES) or len(self.gates) != len(
            SOURCE_GATES
        ):
            raise ValueError("monthly source read set must cover each gate exactly once")


class MonthlySourceAdapter(Protocol):
    adapter_id: str
    adapter_version: str
    contract_sha256: str

    def read(
        self,
        connection: SnapshotConnection,
        identity: MonthlySnapshotIdentity,
        context: ProducerContext,
    ) -> MonthlySourceReadSet: ...


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _validated_source_artifacts(
    artifacts: Sequence[SourceArtifact],
) -> tuple[list[dict[str, str]], dict[str, str]]:
    """Resolve and hash the exact evidence set consumed by SOURCE.

    Gate contracts, readbacks, repair receipts and change receipts are not
    trusted merely because an adapter names them.  Every provenance digest
    used to close SOURCE must resolve to one regular, non-link input artifact.
    """

    if not artifacts:
        raise MonthlySourceProducerError("source adapter returned no input artifacts")
    by_id: dict[str, str] = {}
    by_path: set[Path] = set()
    result: list[dict[str, str]] = []
    for artifact in artifacts:
        if artifact.artifact_id in by_id:
            raise MonthlySourceProducerError("source input artifact id is duplicated")
        resolved = artifact.path.resolve(strict=True)
        if not resolved.is_file() or resolved.is_symlink():
            raise MonthlySourceProducerError("source input artifact is missing or linked")
        if resolved in by_path:
            raise MonthlySourceProducerError("source input artifact path is duplicated")
        digest = _sha256(resolved)
        by_id[artifact.artifact_id] = digest
        by_path.add(resolved)
        result.append({"id": artifact.artifact_id, "path": str(resolved)})
    return result, by_id


def _require_source_provenance(
    read_set: MonthlySourceReadSet,
    *,
    artifact_hashes: Mapping[str, str],
) -> None:
    content_hashes = set(artifact_hashes.values())
    for gate in read_set.gates:
        for reference in (gate.expectation_contract_ref, gate.readback_ref):
            if reference not in artifact_hashes:
                raise MonthlySourceProducerError(
                    f"source gate provenance is not pinned: {gate.gate}"
                )
        for exception in gate.exception_refs:
            if exception.authority_sha256 not in content_hashes:
                raise MonthlySourceProducerError(
                    f"source exception authority is not pinned: {gate.gate}"
                )
    for change in read_set.changes:
        if change.source_receipt_sha256 not in content_hashes:
            raise MonthlySourceProducerError(
                f"source change receipt is not pinned: {change.dataset}"
            )
    for repair in read_set.repair_receipts:
        if not isinstance(repair, Mapping):
            raise MonthlySourceProducerError("source repair receipt is invalid")
        for field in ("dev_validation_sha256", "apply_sha256", "readback_sha256"):
            digest = str(repair.get(field) or "")
            ensure_sha256(digest, field=f"repair_receipt.{field}")
            if digest not in content_hashes:
                raise MonthlySourceProducerError(
                    f"source repair evidence is not pinned: {field}"
                )


def _write_exclusive(path: Path, value: Mapping[str, Any]) -> dict[str, Any]:
    payload = canonical_json_bytes(value) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    return {
        "id": "",
        "path": str(path),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size": len(payload),
    }


@dataclass(frozen=True, slots=True)
class AuditedMonthlySourceProducer:
    producer_id: str
    producer_version: str
    artifact_root: Path
    connection_factory: Callable[[], SnapshotConnection]
    adapter: MonthlySourceAdapter
    snapshot_factory: Callable[
        [Callable[[], SnapshotConnection]], MonthlySnapshotCoordinator
    ] = managed_monthly_snapshot

    def __post_init__(self) -> None:
        if not self.producer_id.strip() or not self.producer_version.strip():
            raise ValueError("source producer identity is empty")
        if not self.artifact_root.is_absolute() or not self.artifact_root.is_dir():
            raise ValueError("source producer artifact root must be an existing absolute directory")
        ensure_sha256(self.adapter.contract_sha256, field="source_adapter.contract_sha256")

    def contract_identity(self) -> Mapping[str, Any]:
        return {
            "id": self.producer_id,
            "version": self.producer_version,
            "adapter_id": self.adapter.adapter_id,
            "adapter_version": self.adapter.adapter_version,
            "adapter_contract_sha256": self.adapter.contract_sha256,
        }

    def produce(self, context: ProducerContext) -> Mapping[str, Any]:
        if context.stage != "SOURCE":
            raise MonthlySourceProducerError("audited source producer received a non-SOURCE stage")
        started = time.monotonic()
        operation_root = (
            self.artifact_root
            / "monthly"
            / context.operation_id
            / "source"
            / f"attempt-{context.attempt}"
        )
        operation_root.mkdir(parents=True, exist_ok=False)
        predecessor_cutoff = date.fromisoformat(str(context.plan["predecessor"]["cutoff"]))
        target_cutoff = date.fromisoformat(str(context.plan["target_cutoff"]))

        coordinator = self.snapshot_factory(self.connection_factory)
        with coordinator as snapshot:
            if snapshot.identity is None:  # pragma: no cover - guarded by coordinator
                raise MonthlySourceProducerError("monthly snapshot identity is unavailable")
            read_set = snapshot.read(
                lambda connection, identity: self.adapter.read(connection, identity, context)
            )
            snapshot_group_id = f"postgres:{snapshot.identity.snapshot_id}"
            if any(item.snapshot_group_id != snapshot_group_id for item in read_set.gates):
                raise MonthlySourceProducerError("source gate snapshot identity differs")
            audit = close_source_audit(
                cutoff=target_cutoff,
                predecessor_cutoff=predecessor_cutoff,
                gates=read_set.gates,
                changes=read_set.changes,
            )
            snapshot.assert_no_overlapping_repairs()
            identity = snapshot.identity

        input_artifacts, artifact_hashes = _validated_source_artifacts(
            read_set.input_artifacts
        )
        _require_source_provenance(read_set, artifact_hashes=artifact_hashes)
        actions = classify_component_actions(read_set.changes)
        outputs: list[dict[str, str]] = []

        def write(relative: str, value: Mapping[str, Any]) -> dict[str, Any]:
            result = _write_exclusive(operation_root / relative, value)
            result["id"] = (operation_root / relative).relative_to(self.artifact_root).as_posix()
            outputs.append({"id": str(result["id"]), "path": str(operation_root / relative)})
            return {"id": result["id"], "sha256": result["sha256"], "size": result["size"]}

        snapshot_ref = write(
            "snapshot-identity.json",
            {
                "schema_version": "aistock_monthly_source_snapshot_identity_v1",
                "snapshot_group_id": snapshot_group_id,
                "snapshot_id": identity.snapshot_id,
                "source_as_of": identity.source_as_of,
                "repair_watermark": identity.initial_repair_watermark,
            },
        )
        overlap_ref = write(
            "repair-overlap.json",
            {
                "schema_version": "aistock_monthly_repair_overlap_check_v1",
                "snapshot_group_id": snapshot_group_id,
                "initial_repair_watermark": identity.initial_repair_watermark,
                "overlapping_repair_ids": [],
                "status": "PASS",
            },
        )
        changes_payload = [item.payload() for item in read_set.changes]
        changes_ref = write(
            "change-scope.json",
            {
                "schema_version": "aistock_monthly_source_change_scope_v1",
                "changes": changes_payload,
                "component_actions": actions,
            },
        )
        gate_refs = [
            write(f"gates/{gate}.json", next(item for item in read_set.gates if item.gate == gate).payload())
            for gate in SOURCE_GATES
        ]
        producer_contract_ref = write(
            "producer-contract.json",
            {
                "schema_version": "aistock_monthly_source_producer_contract_v1",
                "producer_id": self.adapter.adapter_id,
                "producer_version": self.adapter.adapter_version,
                "contract_sha256": self.adapter.contract_sha256,
            },
        )
        write("source-audit.json", audit)
        bytes_written = sum(Path(item["path"]).stat().st_size for item in outputs)
        source_rows = sum(item.observed_count for item in read_set.gates)
        elapsed_ms = max(0, int((time.monotonic() - started) * 1000))
        sealed_hook = getattr(self.adapter, "snapshot_sealed", None)
        if callable(sealed_hook):
            sealed_hook(context, read_set.seal_token)
        return {
            "schema_version": PRODUCER_EVIDENCE_SCHEMA,
            "scope": {
                "gates": list(SOURCE_GATES),
                "changes": changes_payload,
                "component_actions": actions,
                "database_read_performed": True,
                "database_write_performed": False,
                "repair_receipts": [dict(item) for item in read_set.repair_receipts],
                "source_as_of": identity.source_as_of,
                "snapshot_group_id": snapshot_group_id,
                "consistent_input_set_complete": True,
                "snapshot_identity_refs": [snapshot_ref],
                "repair_overlap_check_refs": [overlap_ref],
                "change_scope_refs": [changes_ref],
                "source_gate_refs": gate_refs,
                "producer_contract_refs": [producer_contract_ref],
            },
            "input_artifacts": input_artifacts,
            "output_artifacts": outputs,
            "counts": {
                "unexplained_gap_count": 0,
                "source_rows_read": source_rows,
                "computed_rows": source_rows,
                "files_written": len(outputs),
                "bytes_written": bytes_written,
                "bytes_transferred": 0,
                "bytes_hashed": sum(Path(item["path"]).stat().st_size for item in input_artifacts),
                "elapsed_ms": elapsed_ms,
            },
            "errors": [],
        }


__all__: Sequence[str] = (
    "AuditedMonthlySourceProducer",
    "MonthlySourceAdapter",
    "MonthlySourceProducerError",
    "MonthlySourceReadSet",
    "SourceArtifact",
)
