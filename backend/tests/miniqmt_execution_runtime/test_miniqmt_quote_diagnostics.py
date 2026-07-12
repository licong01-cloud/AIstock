from __future__ import annotations

from datetime import UTC, date, datetime

from backend.execution_algos.adaptive_is.contracts import EvidenceCaptureType, canonical_sha256
from backend.tests.miniqmt_execution_runtime.test_quote_evidence import _evidence
from backend.services.miniqmt_execution_runtime.models import (
    MiniQMTExecutionEventType,
    MiniQMTExecutionEvent,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTExecutionRuntimeRecord,
)
from backend.services.miniqmt_execution_runtime.repository import (
    InMemoryMiniQMTExecutionRuntimeRepository,
    QuoteEvidenceEventCandidate,
)
from backend.services.simulation_runtime.ops import SimulationRuntimeOpsService
from backend.services.simulation_runtime.repository import InMemorySimulationRuntimeRepository


def _runtime(repo: InMemoryMiniQMTExecutionRuntimeRepository) -> None:
    repo.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            **MiniQMTExecutionRuntimeConfig(
                runtime_id="runtime-diagnostics",
                account_group_id="account-never-exposed",
                trade_date=date(2026, 7, 13),
                runtime_config_hash="a" * 64,
            ).model_dump()
        )
    )


class _ReadOnlySpyRepository(InMemoryMiniQMTExecutionRuntimeRepository):
    def __init__(self) -> None:
        super().__init__()
        self.append_calls = 0

    def append_evidence_event_idempotent(self, candidate: QuoteEvidenceEventCandidate):  # type: ignore[override]
        self.append_calls += 1
        return super().append_evidence_event_idempotent(candidate)

    def list_events(self, runtime_id: str, *, include_archived: bool = False):  # type: ignore[no-untyped-def, override]
        raise AssertionError("quote diagnostics must use repository pagination, not load the full runtime journal")


def test_quote_diagnostics_and_evidence_readback_are_paginated_and_strictly_read_only() -> None:
    repo = _ReadOnlySpyRepository()
    _runtime(repo)
    evidence_contract = _evidence(runtime_id="runtime-diagnostics", market_data_id="md_diagnostics")
    repo.append_evidence_event_idempotent(
        QuoteEvidenceEventCandidate(
            event_id="mqrtevt_" + canonical_sha256({"schema": "miniqmt_quote_event_v1", "evidence_id": evidence_contract.evidence_id}),
            runtime_id="runtime-diagnostics",
            event_type=MiniQMTExecutionEventType.QUOTE_ELIGIBILITY_EVALUATED,
            event_time=evidence_contract.event_time_utc,
            payload=evidence_contract.runtime_payload(),
            evidence_sha256=evidence_contract.evidence_sha256,
            evidence_contract=evidence_contract,
        )
    )
    repo.append_event(
        MiniQMTExecutionEvent(
            event_id="child-event-diagnostics",
            runtime_id="runtime-diagnostics",
            sequence=2,
            event_type=MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED,
            event_time=datetime(2026, 7, 13, 1, 31, tzinfo=UTC),
            source="oms",
            payload={"child_order_id": "child-1"},
        )
    )
    child_receipt = _evidence(
        EvidenceCaptureType.CHILD_RECEIPT,
        runtime_id="runtime-diagnostics",
        market_data_id="md_receipt_diagnostics",
        action_evidence_id=evidence_contract.evidence_id,
        source_child_event_id="child-event-diagnostics",
    )
    repo.append_evidence_event_idempotent(
        QuoteEvidenceEventCandidate(
            event_id="mqrtevt_" + canonical_sha256({"schema": "miniqmt_quote_event_v1", "evidence_id": child_receipt.evidence_id}),
            runtime_id="runtime-diagnostics",
            event_type=MiniQMTExecutionEventType.QUOTE_MARK_CAPTURED,
            event_time=child_receipt.event_time_utc,
            payload=child_receipt.runtime_payload(),
            evidence_sha256=child_receipt.evidence_sha256,
            evidence_contract=child_receipt,
        )
    )
    service = SimulationRuntimeOpsService(repository=InMemorySimulationRuntimeRepository())
    before = repo.append_calls
    diagnostics = service.list_miniqmt_quote_diagnostics(
        runtime_repository=repo,
        runtime_id="runtime-diagnostics",
        limit=10,
    )
    evidence = service.list_miniqmt_quote_evidence(
        runtime_repository=repo,
        runtime_id="runtime-diagnostics",
        market_data_id="md_diagnostics",
        limit=10,
    )
    assert repo.append_calls == before
    assert diagnostics["schema_version"] == "miniqmt_quote_diagnostics_v1"
    assert diagnostics["read_only"] is True
    assert diagnostics["events"][0]["symbol"] == "000001.SZ"
    assert "account-never-exposed" not in str(diagnostics)
    assert evidence["schema_version"] == "miniqmt_quote_evidence_readback_v1"
    assert evidence["records"][0]["durable_receipt"]["durable_ack"] is True
    assert evidence["records"][0]["link_complete"] is True
    assert {record["evidence"]["capture_type"] for record in evidence["records"]} == {"ACTION_INPUT", "CHILD_RECEIPT"}
    child_record = next(record for record in evidence["records"] if record["evidence"]["capture_type"] == "CHILD_RECEIPT")
    assert child_record["link_complete"] is True
    assert child_record["missing_links"] == []
