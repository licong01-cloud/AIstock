from __future__ import annotations

from datetime import UTC, date, datetime

from backend.execution_algos.adaptive_is.contracts import EvidenceCaptureType, canonical_sha256
from backend.tests.miniqmt_execution_runtime.test_quote_evidence import _evidence
from backend.services.miniqmt_execution_runtime.models import (
    MiniQMTExecutionEventType,
    MiniQMTExecutionEvent,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTExecutionRuntimeRecord,
    MiniQMTExecutionRuntimeState,
    MiniQMTGatewayState,
    MiniQMTOmsState,
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


class _HealthyScheduler:
    def status(self) -> dict[str, object]:
        return {
            "miniqmt_quote_ingress_activation": {
                "status": "READY",
                "ingress": {
                    "subscription": {
                        "status": "ACTIVE",
                        "generation": 7,
                        "callback_total": 19,
                    },
                    "writer": {
                        "status": "ACTIVE",
                        "thread_alive": True,
                        "accepted_count": 18,
                        "ordering_rejected_count": 1,
                    },
                },
            },
            "b0_quote_v2_controllers": {
                "status": "READY",
                "controllers": {
                    "runtime-diagnostics": {
                        "status": "HEALTHY",
                        "pending_action_count": 0,
                    }
                },
            },
        }


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
    assert diagnostics["health"]["authority"] == "simulation_runtime_miniqmt_quote_diagnostics"
    assert diagnostics["health"]["authoritative"] is True
    assert diagnostics["health"]["durable_health"]["reported"] is False
    assert diagnostics["events"][0]["symbol"] == "000001.SZ"
    assert "account-never-exposed" not in str(diagnostics)
    assert evidence["schema_version"] == "miniqmt_quote_evidence_readback_v1"
    assert evidence["records"][0]["durable_receipt"]["durable_ack"] is True
    assert evidence["records"][0]["link_complete"] is True
    assert {record["evidence"]["capture_type"] for record in evidence["records"]} == {"ACTION_INPUT", "CHILD_RECEIPT"}
    child_record = next(record for record in evidence["records"] if record["evidence"]["capture_type"] == "CHILD_RECEIPT")
    assert child_record["link_complete"] is True
    assert child_record["missing_links"] == []


def test_quote_diagnostics_unifies_durable_live_controller_gateway_and_oms_health() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            **MiniQMTExecutionRuntimeConfig(
                runtime_id="runtime-diagnostics",
                account_group_id="account-never-exposed",
                trade_date=date(2026, 7, 13),
                runtime_config_hash="a" * 64,
            ).model_dump(),
            event_loop_state=MiniQMTExecutionRuntimeState.RUNNING,
            gateway_state=MiniQMTGatewayState.CONNECTED,
            oms_state=MiniQMTOmsState.RECONCILED,
        )
    )
    health_time = datetime(2026, 7, 13, 1, 31, tzinfo=UTC)
    repo.append_event(
        MiniQMTExecutionEvent(
            event_id="quote-health-diagnostics",
            runtime_id="runtime-diagnostics",
            sequence=1,
            event_type=MiniQMTExecutionEventType.QUOTE_INGRESS_HEALTH,
            event_time=health_time,
            source="quote_ingress",
            payload={
                "schema_version": "miniqmt_quote_ingress_health_payload_v1",
                "health_or_aggregate": {
                    "status": "HEALTHY",
                    "health_sha256": "b" * 64,
                    "accepted": 18,
                    "rejected": 1,
                },
            },
        )
    )
    service = SimulationRuntimeOpsService(
        repository=InMemorySimulationRuntimeRepository(),
        scheduler=_HealthyScheduler(),  # type: ignore[arg-type]
    )

    diagnostics = service.list_miniqmt_quote_diagnostics(
        runtime_repository=repo,
        runtime_id="runtime-diagnostics",
        limit=10,
    )

    health = diagnostics["health"]
    assert health["status"] == "HEALTHY"
    assert health["reason_codes"] == []
    assert health["durable_health"]["durable_ack"] is True
    assert health["durable_health"]["readback_verified"] is True
    assert health["durable_health"]["event_id"] == "quote-health-diagnostics"
    assert health["durable_health"]["event_time"] == health_time.isoformat()
    assert health["live_components"]["callback_subscription"]["callback_total"] == 19
    assert health["live_components"]["writer"]["accepted_count"] == 18
    assert health["live_components"]["controller"]["status"] == "HEALTHY"
    assert health["runtime_projection"]["gateway_state"] == "CONNECTED"
    assert health["runtime_projection"]["oms_state"] == "RECONCILED"
    assert health["legacy_status"]["authoritative"] is False
    assert "account-never-exposed" not in str(diagnostics)
