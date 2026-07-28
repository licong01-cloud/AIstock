from __future__ import annotations

import json

import pytest

from backend.services.miniqmt_execution_runtime.kernel_current_three_contracts import (
    CurrentThreeFailureV1,
    CurrentThreeLegacyEvidenceRefV1,
    CurrentThreeParityBusinessEffectV1,
    CurrentThreeParityEventRefV1,
    CurrentThreeParityInputV1,
    CurrentThreeParityReceiptV1,
    CurrentThreeParityTimerEffectV1,
    CurrentThreeParityTraceStepV1,
    CurrentThreeParityTraceV1,
    CurrentThreeShadowFactRefV1,
    CurrentThreeShadowSourceSnapshotV1,
    bounded_failures_v1,
    legacy_evidence_set_sha256_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_current_three_parity import (
    build_current_three_parity_receipt_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1
from backend.tests.miniqmt_execution_runtime.test_current_three_parity_contracts import _input, _trace
from backend.tests.miniqmt_execution_runtime.test_current_three_shadow_source import _algo, _child, _events, _runtime
from backend.services.miniqmt_execution_runtime.kernel_current_three_shadow_source import (
    build_current_three_shadow_source_snapshot_v1,
)


def _reject(model: object, model_type: type, **updates: object) -> None:
    payload = model.model_dump(mode="json")  # type: ignore[attr-defined]
    payload.update(updates)
    with pytest.raises((TypeError, ValueError)):
        model_type.model_validate_json(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def test_failure_and_evidence_ref_readback_reject_hash_order_duplicate_and_bad_marker() -> None:
    failure = CurrentThreeFailureV1.create(field_path="field", reason_code="REASON", context={"a": 1})
    _reject(failure, CurrentThreeFailureV1, context_sha256="0" * 64)
    assert bounded_failures_v1([failure]) == (failure,)
    failures = [
        CurrentThreeFailureV1.create(field_path=f"field_{index:03d}", reason_code="REASON", context={"i": index})
        for index in range(300)
    ]
    bounded = bounded_failures_v1(failures)
    assert len(bounded) == 256
    assert bounded[-1].field_path == "__truncated__"

    first = CurrentThreeLegacyEvidenceRefV1.create(
        identity="first", payload_sha256="1" * 64, logical_time_utc="2026-07-29T01:30:00Z"
    )
    second = CurrentThreeLegacyEvidenceRefV1.create(
        identity="second", payload_sha256="2" * 64, logical_time_utc="2026-07-29T01:30:01Z"
    )
    _reject(first, CurrentThreeLegacyEvidenceRefV1, ref_sha256="0" * 64)
    with pytest.raises(ValueError):
        legacy_evidence_set_sha256_v1("domain", (second, first))
    with pytest.raises(ValueError):
        legacy_evidence_set_sha256_v1("domain", (first, first))


def test_shadow_snapshot_readback_rejects_commit_date_count_order_and_hash_drift() -> None:
    snapshot = build_current_three_shadow_source_snapshot_v1(
        repository_commit_sha="a" * 40,
        runtime=_runtime(),
        events=_events(),
        algos=(_algo(),),
        children=(_child(),),
        database_snapshot_at_utc="2026-07-29T01:30:00Z",
    )
    _reject(snapshot, CurrentThreeShadowSourceSnapshotV1, repository_commit_sha="not-a-commit")
    _reject(snapshot, CurrentThreeShadowSourceSnapshotV1, trade_date="20260729")
    _reject(snapshot, CurrentThreeShadowSourceSnapshotV1, event_count=99)
    _reject(snapshot, CurrentThreeShadowSourceSnapshotV1, source_set_sha256="0" * 64)
    _reject(
        snapshot,
        CurrentThreeShadowSourceSnapshotV1,
        ordered_legacy_event_refs=list(reversed(snapshot.model_dump(mode="json")["ordered_legacy_event_refs"])),
    )
    fact = snapshot.ordered_child_fact_refs[0]
    _reject(fact, CurrentThreeShadowFactRefV1, payload_sha256="0" * 64)
    _reject(fact, CurrentThreeShadowFactRefV1, ref_sha256="0" * 64)


def test_parity_event_input_effect_timer_trace_and_receipt_readback_are_strict() -> None:
    parity_input = _input()
    event = parity_input.ordered_event_refs[0]
    _reject(event, CurrentThreeParityEventRefV1, market_data_projection_sha256=None)
    _reject(event, CurrentThreeParityEventRefV1, event_ref_sha256="0" * 64)
    _reject(parity_input, CurrentThreeParityInputV1, plugin_config_sha256="0" * 64)
    _reject(parity_input, CurrentThreeParityInputV1, event_set_sha256="0" * 64)
    _reject(parity_input, CurrentThreeParityInputV1, input_sha256="0" * 64)

    with pytest.raises(ValueError):
        CurrentThreeParityBusinessEffectV1(
            schema_version="miniqmt_current_three_parity_business_effect_v1",
            kind="SUBMIT_LIMIT",
            side="BUY",
            symbol="600000.SH",
            canonical_price="10",
            quantity=100,
            cancel_target_ordinal=1,
            reason_code="reason",
            market_data_lineage_sha256="1" * 64,
        )
    with pytest.raises(ValueError):
        CurrentThreeParityTimerEffectV1(
            schema_version="miniqmt_current_three_parity_timer_effect_v1",
            mutation_type="CANCEL",
            timer_name="timer",
            schedule_epoch="epoch",
            due_at_exchange_utc="2026-07-29T01:30:00Z",
            catch_up_policy="SKIP_MISSED",
        )

    trace = _trace(price="10")
    step = trace.ordered_steps[0]
    _reject(step, CurrentThreeParityTraceStepV1, step_sha256="0" * 64)
    _reject(trace, CurrentThreeParityTraceV1, trace_sha256="0" * 64)
    receipt = build_current_three_parity_receipt_v1(
        parity_input=parity_input,
        legacy_source_attribution_sha256="5" * 64,
        plugin_id="aistock.vnpy.sniper",
        plugin_version="3.0.0",
        plugin_manifest_sha256="6" * 64,
        legacy_trace=trace,
        kernel_trace=trace,
    )
    _reject(receipt, CurrentThreeParityReceiptV1, event_set_sha256="0" * 64)
    _reject(receipt, CurrentThreeParityReceiptV1, status="FAILED")
    _reject(receipt, CurrentThreeParityReceiptV1, receipt_sha256="0" * 64)


def test_shadow_snapshot_fact_payload_hash_domain_is_not_interchangeable() -> None:
    snapshot = build_current_three_shadow_source_snapshot_v1(
        repository_commit_sha="a" * 40,
        runtime=_runtime(),
        events=_events(),
        algos=(_algo(),),
        children=(_child(),),
        database_snapshot_at_utc="2026-07-29T01:30:00Z",
    )
    fact = snapshot.ordered_child_fact_refs[0]
    wrong_domain_hash = hash_hex_v1("wrong_domain", fact.model_dump(mode="json")["payload"])
    _reject(fact, CurrentThreeShadowFactRefV1, payload_sha256=wrong_domain_hash)
