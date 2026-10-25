from __future__ import annotations

from dataclasses import dataclass, replace
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
    MonthlySourcePreparationReadSet,
    MonthlySourceProducerError,
    MonthlySourceReadSet,
    SourceArtifact,
)
from backend.services.dataset_release.monthly_unified import (
    SOURCE_GATES,
    STAGES,
    SourceChange,
    MonthlyReleaseSourceBlocked,
)
from backend.services.dataset_release.monthly_worker import (
    MonthlyProducerError, ProducerContext, RegisteredMonthlyPipeline,
)
from backend.services.dataset_release.canonical import canonical_json_bytes


SHA = "a" * 64


def _authorized_warning_pipeline(tmp_path, mutation=None):
    from backend.tests.dataset_release.test_monthly_source_quality import acceptance
    from backend.tests.dataset_release.test_monthly_unified_v2 import _request, _service, Pipeline
    from backend.services.dataset_release.monthly_source_quality import accepted_parity_warning

    control = tmp_path / "control"
    control.mkdir()
    service = _service(control, Pipeline())
    operation_id = service.submit(_request(), principal="operator")["operation_id"]
    value = acceptance(tmp_path)
    value["operation_id"] = operation_id
    value["predecessor_dataset_manifest_sha256"] = service.store.read_plan(operation_id)["predecessor"]["dataset_manifest_sha256"]
    service.bind_source_quality_inputs(operation_id, inputs=value, principal="operator")
    plan = service.store.read_plan(operation_id)
    source = tmp_path / "inputs" / "source.json"
    source.parent.mkdir()
    source.write_bytes(b"{}\n")

    class WarningAdapter(Adapter):
        def read(self, connection, identity, context):
            result = super().read(connection, identity, context)
            authority = tmp_path / "quality-acceptance.json"
            authority.write_bytes(canonical_json_bytes(plan["monthly_source_quality_inputs"]) + b"\n")
            warning = accepted_parity_warning(plan["monthly_source_quality_inputs"],
                symbol="688799.SH", trade_date=date(2026, 9, 14),
                mismatches={"open": {"daily": 10.0, "minute": 11.0}})
            return replace(result,
                gates=tuple(replace(gate, quality_warning_refs=(warning,))
                    if gate.gate == "minute_price" else gate for gate in result.gates),
                input_artifacts=(*result.input_artifacts, SourceArtifact("quality-acceptance.json", authority)))

    producer = AuditedMonthlySourceProducer("aistock.monthly.source", "2", tmp_path,
        Connection, WarningAdapter(source, tmp_path),
        snapshot_factory=lambda factory, *, cutoff: MonthlySnapshotCoordinator(factory,
            repair_watermark_reader=lambda _: "repair-1", overlapping_repair_reader=lambda *_: ()))

    class MutatingProducer:
        producer_id, producer_version = producer.producer_id, producer.producer_version

        def produce(self, context):
            evidence = producer.produce(context)
            if mutation == "unbound":
                context.plan.pop("monthly_source_quality_inputs")
                context.plan.pop("monthly_source_quality_inputs_ref")
            if mutation == "unpinned":
                evidence["input_artifacts"] = [item for item in evidence["input_artifacts"]
                    if item["id"] != "quality-acceptance.json"]
            ref = next(item for item in evidence["scope"]["source_gate_refs"] if item["id"].endswith("/minute_price.json"))
            path = tmp_path / ref["id"]
            import json
            gate = json.loads(path.read_bytes())
            if mutation == "partial":
                gate.pop("quality_warning_count")
            elif mutation == "count":
                gate["quality_warning_count"] = True
            elif mutation == "empty":
                gate["quality_warning_count"], gate["quality_warning_refs"] = 0, []
            elif mutation == "authority":
                gate["quality_warning_refs"][0]["authority_sha256"] = "f" * 64
            elif mutation == "values":
                gate["quality_warning_refs"][0]["mismatches"]["open"]["minute"] = 12.0
            elif mutation == "duplicate":
                gate["quality_warning_refs"] *= 2
                gate["quality_warning_count"] = 2
                gate["expected_count"] = gate["observed_count"] = 2
            elif mutation == "unknown":
                gate["skip_missing"] = True
            elif mutation == "gap":
                gate["expected_count"], gate["unexplained_missing_count"] = 2, 1
            elif mutation == "invalid":
                gate["invalid_value_count"] = 1
            elif mutation == "wrong_gate":
                ref = next(item for item in evidence["scope"]["source_gate_refs"] if item["id"].endswith("/daily_price.json"))
                path = tmp_path / ref["id"]
                other = json.loads(path.read_bytes())
                other.update({key: gate[key] for key in ("quality_status", "quality_warning_count", "quality_warning_refs")})
                gate = other
            payload = canonical_json_bytes(gate) + b"\n"
            path.write_bytes(payload)
            ref.update(sha256=hashlib.sha256(payload).hexdigest(), size=len(payload))
            return evidence

    pipeline = RegisteredMonthlyPipeline({stage: MutatingProducer() for stage in STAGES}, artifact_roots=(tmp_path,))
    return pipeline, operation_id, plan


def test_authorized_quality_warning_closes_actual_source_producer_worker_boundary(tmp_path):
    pipeline, operation_id, plan = _authorized_warning_pipeline(tmp_path)
    receipt = pipeline.run_stage(stage="SOURCE", operation_id=operation_id, attempt=1,
        request={}, plan=plan, prior_receipts={})
    assert receipt["status"] == "PASS"
    assert receipt["counts"]["unexplained_gap_count"] == 0
    import json
    ref = next(item for item in receipt["output_refs"] if item["id"].endswith("/minute_price.json"))
    gate = json.loads((tmp_path / ref["id"]).read_bytes())
    assert (gate["quality_status"], gate["quality_warning_count"]) == ("ACCEPTED_WITH_WARNINGS", 1)
    assert gate["quality_warning_refs"][0]["data_modified"] is False


@pytest.mark.parametrize("mutation", ["partial", "count", "empty", "authority", "values",
    "duplicate", "unknown", "gap", "invalid", "wrong_gate", "unbound", "unpinned"])
def test_source_worker_warning_cannot_waive_physical_gaps_or_unapproved_drift(tmp_path, mutation):
    pipeline, operation_id, plan = _authorized_warning_pipeline(tmp_path, mutation)
    with pytest.raises(MonthlyProducerError):
        pipeline.run_stage(stage="SOURCE", operation_id=operation_id, attempt=1,
            request={}, plan=plan, prior_receipts={})


@pytest.mark.parametrize("overlap", [False, True])
def test_private_source_is_sealed_only_after_repair_overlap_check(tmp_path, overlap):
    source = tmp_path / "partial-source.json"
    source.write_text("{}\n", encoding="utf-8")
    calls = []

    class PreparationAdapter:
        adapter_id = "aistock.monthly.source"
        adapter_version = "6"
        contract_sha256 = SHA

        def read(self, _connection, identity, context):
            return MonthlySourcePreparationReadSet(
                snapshot_group_id=f"postgres:{identity.snapshot_id}",
                input_artifacts=(SourceArtifact("partial-source.json", source),),
                blocking_context={"reason_code": "BLOCKED_SOURCE_REFRESH_AUDIT_INCOMPLETE"},
                preparation_token=object(),
            )

        def preparation_snapshot_sealed(self, context, token, identity):
            calls.append((context.operation_id, token, identity.snapshot_id))
            return {"status": "SOURCE_PREPARED_UNPUBLISHED", "prepared_component_count": 0}

        def snapshot_sealed(self, *_args):
            pytest.fail("partial SOURCE must never enter the full source reuse catalog")

    producer = AuditedMonthlySourceProducer(
        "aistock.monthly.source",
        "2",
        tmp_path,
        Connection,
        PreparationAdapter(),
        snapshot_factory=lambda factory, *, cutoff: MonthlySnapshotCoordinator(
            factory,
            repair_watermark_reader=lambda _: "repair-1",
            overlapping_repair_reader=lambda *_: ("repair-2",) if overlap else (),
        ),
    )
    context = ProducerContext(
        stage="SOURCE",
        operation_id=f"dmr_{'8' * 32}",
        attempt=1,
        request={},
        plan={"target_cutoff": "2026-09-30", "predecessor": {"cutoff": "2026-08-31"}},
        prior_receipts={},
    )
    if overlap:
        with pytest.raises(MonthlySnapshotError, match="repairs overlapped"):
            producer.produce(context)
        assert calls == []
    else:
        with pytest.raises(MonthlyReleaseSourceBlocked) as error:
            producer.produce(context)
        assert len(calls) == 1
        assert error.value.context["consistent_input_set_complete"] is False
        assert error.value.context["publication_allowed"] is False
        assert error.value.context["component_preparation_execution"] == "SOURCE_PREPARED_UNPUBLISHED"
        assert not (tmp_path / "monthly" / context.operation_id / "source" / "attempt-1" / "source-audit.json").exists()


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

    def snapshot_factory(factory, *, cutoff):  # type: ignore[no-untyped-def]
        assert cutoff == date(2026, 9, 30)
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
        snapshot_factory=lambda factory, *, cutoff: MonthlySnapshotCoordinator(
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
        snapshot_factory=lambda factory, *, cutoff: MonthlySnapshotCoordinator(
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
