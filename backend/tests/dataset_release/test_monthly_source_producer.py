from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import hashlib
from pathlib import Path

import pytest

from backend.services.dataset_release.monthly_snapshot import (
    MonthlySnapshotCoordinator,
    MonthlySnapshotError,
)
from backend.services.dataset_release.monthly_source_audit import SourceGateEvidence
from backend.services.dataset_release.monthly_source_producer import (
    AuditedMonthlySourceProducer,
    MonthlySourceProducerError,
    MonthlySourceReadSet,
    SourceArtifact,
)
from backend.services.dataset_release.monthly_unified import SOURCE_GATES, STAGES, SourceChange
from backend.services.dataset_release.monthly_worker import ProducerContext, RegisteredMonthlyPipeline


SHA = "a" * 64


class Cursor:
    def __init__(self, connection: "Connection") -> None:
        self.connection = connection

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str) -> None:
        self.connection.commands.append(sql)

    def fetchone(self):  # type: ignore[no-untyped-def]
        return ("00000003-0000001B-1", datetime(2026, 10, 1, tzinfo=UTC))


class Connection:
    def __init__(self) -> None:
        self.autocommit = True
        self.commands: list[str] = []

    def cursor(self) -> Cursor:
        return Cursor(self)

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


@dataclass
class Adapter:
    source: Path
    evidence_root: Path
    adapter_id: str = "aistock.monthly.source"
    adapter_version: str = "2"
    contract_sha256: str = SHA

    def read(self, _connection, identity, _context):  # type: ignore[no-untyped-def]
        snapshot_group = f"postgres:{identity.snapshot_id}"
        artifacts = [SourceArtifact("inputs/source.json", self.source)]
        for gate in SOURCE_GATES:
            for kind in ("contracts", "readbacks"):
                path = self.evidence_root / kind / f"{gate}.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("{}\n", encoding="utf-8")
                artifacts.append(SourceArtifact(f"{kind}/{gate}.json", path))
        gates = tuple(
            SourceGateEvidence(
                gate=gate,
                snapshot_group_id=snapshot_group,
                expectation_contract_ref=f"contracts/{gate}.json",
                readback_ref=f"readbacks/{gate}.json",
                expected_count=1,
                observed_count=1,
            )
            for gate in SOURCE_GATES
        )
        return MonthlySourceReadSet(
            gates=gates,
            changes=(
                SourceChange(
                    dataset="daily_basic",
                    fields=("volume_ratio",),
                    instruments=("000001.SZ",),
                    start=date(2026, 9, 1),
                    end=date(2026, 9, 30),
                    kind="TAIL_APPEND",
                    source_receipt_sha256=hashlib.sha256(self.source.read_bytes()).hexdigest(),
                ),
            ),
            input_artifacts=tuple(artifacts),
        )


def test_audited_source_producer_emits_strong_gate_artifacts(tmp_path: Path) -> None:
    source = tmp_path / "inputs" / "source.json"
    source.parent.mkdir()
    source.write_text("{}\n", encoding="utf-8")

    def snapshot_factory(factory):  # type: ignore[no-untyped-def]
        return MonthlySnapshotCoordinator(
            factory,
            repair_watermark_reader=lambda _connection: "repair-1",
            overlapping_repair_reader=lambda _connection, _watermark: (),
        )

    producer = AuditedMonthlySourceProducer(
        producer_id="aistock.monthly.source",
        producer_version="2",
        artifact_root=tmp_path,
        connection_factory=Connection,
        adapter=Adapter(source, tmp_path),
        snapshot_factory=snapshot_factory,
    )
    evidence = producer.produce(
        ProducerContext(
            stage="SOURCE",
            operation_id=f"dmr_{'1' * 32}",
            attempt=1,
            request={},
            plan={
                "target_cutoff": "2026-09-30",
                "predecessor": {"cutoff": "2026-08-31"},
                "repair_authorization_refs": [],
            },
            prior_receipts={},
        )
    )
    assert evidence["errors"] == []
    assert evidence["scope"]["consistent_input_set_complete"] is True
    assert len(evidence["scope"]["source_gate_refs"]) == len(SOURCE_GATES)
    assert evidence["counts"]["source_rows_read"] == len(SOURCE_GATES)
    gate_path = tmp_path / evidence["output_artifacts"][3]["id"]
    assert '"schema_version":"aistock_monthly_source_gate_v2"' in gate_path.read_text(encoding="utf-8")
    pipeline = RegisteredMonthlyPipeline({stage: producer for stage in STAGES}, artifact_roots=(tmp_path,))
    receipt = pipeline.run_stage(
        stage="SOURCE",
        operation_id=f"dmr_{'2' * 32}",
        attempt=1,
        request={},
        plan={
            "target_cutoff": "2026-09-30",
            "predecessor": {"cutoff": "2026-08-31"},
            "repair_authorization_refs": [],
        },
        prior_receipts={},
    )
    assert receipt["scope"]["snapshot_group_id"].startswith("postgres:")


def test_audited_source_producer_rejects_unpinned_change_receipt(tmp_path: Path) -> None:
    source = tmp_path / "inputs" / "source.json"
    source.parent.mkdir()
    source.write_text("{}\n", encoding="utf-8")

    @dataclass
    class DriftAdapter(Adapter):
        def read(self, connection, identity, context):  # type: ignore[no-untyped-def]
            original = super().read(connection, identity, context)
            change = original.changes[0]
            return MonthlySourceReadSet(
                gates=original.gates,
                changes=(
                    SourceChange(
                        dataset=change.dataset,
                        fields=change.fields,
                        instruments=change.instruments,
                        start=change.start,
                        end=change.end,
                        kind=change.kind,
                        source_receipt_sha256="f" * 64,
                    ),
                ),
                input_artifacts=original.input_artifacts,
            )

    producer = AuditedMonthlySourceProducer(
        producer_id="aistock.monthly.source",
        producer_version="2",
        artifact_root=tmp_path,
        connection_factory=Connection,
        adapter=DriftAdapter(source, tmp_path),
        snapshot_factory=lambda factory: MonthlySnapshotCoordinator(
            factory,
            repair_watermark_reader=lambda _connection: "repair-1",
            overlapping_repair_reader=lambda _connection, _watermark: (),
        ),
    )
    with pytest.raises(MonthlySourceProducerError, match="change receipt is not pinned"):
        producer.produce(
            ProducerContext(
                stage="SOURCE",
                operation_id=f"dmr_{'3' * 32}",
                attempt=1,
                request={},
                plan={
                    "target_cutoff": "2026-09-30",
                    "predecessor": {"cutoff": "2026-08-31"},
                    "repair_authorization_refs": [],
                },
                prior_receipts={},
            )
        )


def test_audited_source_producer_commits_adapter_only_after_overlap_seal(tmp_path: Path) -> None:
    source = tmp_path / "inputs" / "source.json"
    source.parent.mkdir()
    source.write_text("{}\n", encoding="utf-8")
    seal_marker = object()

    @dataclass
    class SealedAdapter(Adapter):
        sealed: list[object] | None = None

        def read(self, connection, identity, context):  # type: ignore[no-untyped-def]
            value = super().read(connection, identity, context)
            return MonthlySourceReadSet(
                gates=value.gates,
                changes=value.changes,
                input_artifacts=value.input_artifacts,
                seal_token=seal_marker,
            )

        def snapshot_sealed(self, _context, token):  # type: ignore[no-untyped-def]
            assert self.sealed is not None
            self.sealed.append(token)

    sealed: list[object] = []
    adapter = SealedAdapter(source, tmp_path, sealed=sealed)
    producer = AuditedMonthlySourceProducer(
        producer_id="aistock.monthly.source",
        producer_version="2",
        artifact_root=tmp_path,
        connection_factory=Connection,
        adapter=adapter,
        snapshot_factory=lambda factory: MonthlySnapshotCoordinator(
            factory,
            repair_watermark_reader=lambda _connection: "repair-1",
            overlapping_repair_reader=lambda _connection, _watermark: ("repair-2",),
        ),
    )
    with pytest.raises(MonthlySnapshotError, match="repairs overlapped"):
        producer.produce(
            ProducerContext(
                stage="SOURCE",
                operation_id=f"dmr_{'4' * 32}",
                attempt=1,
                request={},
                plan={
                    "target_cutoff": "2026-09-30",
                    "predecessor": {"cutoff": "2026-08-31"},
                    "repair_authorization_refs": [],
                },
                prior_receipts={},
            )
        )
    assert sealed == []
