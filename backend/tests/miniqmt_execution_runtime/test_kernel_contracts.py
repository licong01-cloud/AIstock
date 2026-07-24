from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    ConsumedLineageRefV1,
    ExecutionProjectionRefV1,
    ExecutionProjectionSetV1,
    KernelErrorEvidenceV1,
    KernelProjectionTypeV1,
    MiniQMTRiskDecisionReceiptV1,
    RiskDecisionActionV1,
    RuntimeEventIngressReceiptV1,
)


def _sha(char: str) -> str:
    return char * 64


def test_ingress_receipt_closes_identity_delivery_set_transaction_and_readback() -> None:
    receipt = RuntimeEventIngressReceiptV1.create(
        runtime_id="runtime_k2",
        event_id="mqrtevt_event_a",
        event_key_sha256=_sha("1"),
        runtime_sequence=7,
        ordered_target_algo_instance_ids=("mqalgo_a", "mqalgo_b"),
        ordered_delivery_ids=("mqdelivery_a", "mqdelivery_b"),
        transaction_commit_identity="mqtx_ingress_a",
    )

    assert receipt.ingress_receipt_id.startswith("mqingress_")
    assert receipt.delivery_set_sha256 == hash_hex_v1(
        "miniqmt_event_delivery_set_v1",
        {
            "event_id": "mqrtevt_event_a",
            "routing_rule_version": "miniqmt_event_routing_v1",
            "ordered_target_algo_instance_ids": ["mqalgo_a", "mqalgo_b"],
            "ordered_delivery_ids": ["mqdelivery_a", "mqdelivery_b"],
        },
    )
    assert RuntimeEventIngressReceiptV1.model_validate(receipt.model_dump(mode="python"), strict=True) == receipt
    assert RuntimeEventIngressReceiptV1.model_validate_json(receipt.model_dump_json()) == receipt

    drift = {**receipt.model_dump(mode="python"), "receipt_sha256": _sha("f")}
    with pytest.raises(ValidationError, match="receipt_sha256"):
        RuntimeEventIngressReceiptV1.model_validate(drift, strict=True)


@pytest.mark.parametrize(
    ("targets", "deliveries", "message"),
    [
        (("mqalgo_b", "mqalgo_a"), ("mqdelivery_b", "mqdelivery_a"), "sorted"),
        (("mqalgo_a", "mqalgo_a"), ("mqdelivery_a", "mqdelivery_b"), "duplicate"),
        (("mqalgo_a",), ("mqdelivery_a", "mqdelivery_b"), "cardinality"),
    ],
)
def test_ingress_receipt_rejects_noncanonical_or_incomplete_delivery_sets(
    targets: tuple[str, ...], deliveries: tuple[str, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        RuntimeEventIngressReceiptV1.create(
            runtime_id="runtime_k2",
            event_id="mqrtevt_event_a",
            event_key_sha256=_sha("1"),
            runtime_sequence=7,
            ordered_target_algo_instance_ids=targets,
            ordered_delivery_ids=deliveries,
            transaction_commit_identity="mqtx_ingress_a",
        )


def test_projection_set_is_exact_sorted_immutable_and_hash_closed() -> None:
    contract = ExecutionProjectionRefV1.create(
        projection_type=KernelProjectionTypeV1.CONTRACT,
        projection_id="contract_a",
        projection_version="v1",
        payload_sha256=_sha("2"),
        source_event_id=None,
        logical_at_utc="2026-07-25T01:30:00Z",
    )
    risk = ExecutionProjectionRefV1.create(
        projection_type=KernelProjectionTypeV1.RISK_DECISION,
        projection_id="risk_a",
        projection_version="v1",
        payload_sha256=_sha("3"),
        source_event_id="mqrtevt_risk_a",
        logical_at_utc="2026-07-25T01:30:00Z",
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id="runtime_k2",
        algo_instance_id="mqalgo_a",
        event_id="mqrtevt_event_a",
        delivery_id="mqdelivery_a",
        projection_refs=(contract, risk),
    )

    assert tuple(item.projection_type for item in projection_set.ordered_projection_refs) == (
        KernelProjectionTypeV1.CONTRACT,
        KernelProjectionTypeV1.RISK_DECISION,
    )
    with pytest.raises(ValidationError, match="projection_set_sha256"):
        ExecutionProjectionSetV1.model_validate(
            {**projection_set.model_dump(mode="python"), "projection_set_sha256": _sha("f")}, strict=True
        )
    with pytest.raises(ValueError, match="sorted"):
        ExecutionProjectionSetV1.create(
            runtime_id="runtime_k2",
            algo_instance_id="mqalgo_a",
            event_id="mqrtevt_event_a",
            delivery_id="mqdelivery_a",
            projection_refs=(risk, contract),
        )
    with pytest.raises(ValueError, match="duplicate projection_type"):
        second_contract = ExecutionProjectionRefV1.create(
            projection_type=KernelProjectionTypeV1.CONTRACT,
            projection_id="contract_b",
            projection_version="v1",
            payload_sha256=_sha("5"),
            source_event_id=None,
            logical_at_utc="2026-07-25T01:30:00Z",
        )
        ExecutionProjectionSetV1.create(
            runtime_id="runtime_k2",
            algo_instance_id="mqalgo_a",
            event_id="mqrtevt_event_a",
            delivery_id="mqdelivery_a",
            projection_refs=(contract, second_contract),
        )


def test_consumed_lineage_and_risk_receipt_have_strict_writer_readback_parity() -> None:
    lineage = ConsumedLineageRefV1.create(
        lineage_type="MARKET_DATA",
        identity="market_data_a",
        payload_sha256=_sha("4"),
    )
    assert ConsumedLineageRefV1.model_validate(lineage.model_dump(mode="python"), strict=True) == lineage

    receipt = MiniQMTRiskDecisionReceiptV1.create(
        runtime_id="runtime_k2",
        algo_instance_id="mqalgo_a",
        event_id="mqrtevt_event_a",
        child_order_id="mqchild_a",
        decision_stage="PRE_SUBMIT",
        action=RiskDecisionActionV1.PASS,
        reason_code="MINIQMT_RISK_PASS",
        reason="configured risk engine passed",
        metadata={"active_child_count": 0, "runtime_id": "runtime_k2"},
        logical_at_utc="2026-07-25T01:30:00Z",
    )
    dumped = receipt.model_dump(mode="json")
    assert MiniQMTRiskDecisionReceiptV1.model_validate_json(receipt.model_dump_json()) == receipt
    assert json.dumps(dumped, sort_keys=True)

    with pytest.raises(ValidationError, match="metadata_sha256"):
        MiniQMTRiskDecisionReceiptV1.model_validate(
            {**receipt.model_dump(mode="python"), "metadata_sha256": _sha("f")}, strict=True
        )


class _BrokenTextError(RuntimeError):
    def __str__(self) -> str:
        raise RuntimeError("renderer broke")


def test_kernel_error_evidence_keeps_primary_failure_json_safe_and_bounded() -> None:
    evidence = KernelErrorEvidenceV1.create(
        stage="DELIVERY_APPLY",
        stable_reason_code="MINIQMT_ALGO_TRANSITION_FAILED",
        exception=_BrokenTextError(),
        message="plugin transition failed",
        retryable=False,
        terminal=True,
        broker_called=None,
        primary_context={"runtime_id": "runtime_k2", "algo_instance_id": "mqalgo_a"},
        secondary_errors=[{"index": index, "message": f"secondary-{index}"} for index in range(20)],
    )

    dumped = evidence.model_dump(mode="json")
    assert dumped["exception_type"].endswith("._BrokenTextError")
    assert dumped["message"] == "plugin transition failed"
    assert len(dumped["bounded_secondary_errors"]) == 16
    assert dumped["bounded_secondary_errors"][0]["reason_code"] == "MINIQMT_KERNEL_EXCEPTION_RENDER_FAILED"
    assert dumped["bounded_secondary_errors"][-1]["reason_code"] == "MINIQMT_KERNEL_SECONDARY_ERRORS_TRUNCATED"
    assert json.dumps(dumped, sort_keys=True)
    assert KernelErrorEvidenceV1.model_validate_json(evidence.model_dump_json()) == evidence
