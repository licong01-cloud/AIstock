from __future__ import annotations

import hashlib

import pytest
from pydantic import ValidationError

from backend.services.miniqmt_execution_runtime.deterministic_context import (
    DeterministicIdKindV1,
    best_limit_quantity_v1,
    derive_id_v1,
    draw_u53_v1,
    validate_contiguous_ordinals_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import canonical_json_bytes_v1, hash_hex_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    DeterministicExecutionContextV1,
    SessionPhaseV1,
)


def _context() -> DeterministicExecutionContextV1:
    return DeterministicExecutionContextV1.create(
        runtime_id="runtime_20260722_a",
        algo_instance_id="mqalgo_a",
        event_id="mqrtevt_a",
        delivery_id="mqdelivery_a",
        plugin_manifest_sha256="a" * 64,
        transition_sequence=11,
        logical_time_utc="2026-07-22T13:01:02.123456+08:00",
        exchange_trade_date="2026-07-22",
        session_epoch="session_20260722_pm",
        session_phase=SessionPhaseV1.CONTINUOUS_PM,
        input_projection_sha256="b" * 64,
    )


def test_context_hash_is_exact_keyed_and_logical_time_only() -> None:
    context = _context()
    assert context.logical_time_utc == "2026-07-22T05:01:02.123456Z"
    assert context.context_sha256 == hash_hex_v1(
        "miniqmt_deterministic_execution_context_v1",
        context.hash_payload_v1(),
    )

    raw = context.model_dump(mode="python")
    with pytest.raises(ValidationError, match="context_sha256"):
        DeterministicExecutionContextV1.model_validate({**raw, "context_sha256": "c" * 64})
    with pytest.raises(ValidationError):
        DeterministicExecutionContextV1.model_validate({**raw, "transition_sequence": True})


def test_derive_id_is_byte_stable_across_retry_and_sensitive_to_ordinal() -> None:
    context = _context()
    business_hash = hash_hex_v1("business", {"price": "10.01", "quantity": 100})
    first = derive_id_v1(
        context=context,
        kind=DeterministicIdKindV1.ACTION,
        ordinal=0,
        business_payload_sha256=business_hash,
    )
    retry = derive_id_v1(
        context=DeterministicExecutionContextV1.model_validate(context.model_dump(mode="python")),
        kind=DeterministicIdKindV1.ACTION,
        ordinal=0,
        business_payload_sha256=business_hash,
    )
    second = derive_id_v1(
        context=context,
        kind=DeterministicIdKindV1.ACTION,
        ordinal=1,
        business_payload_sha256=business_hash,
    )
    assert first == retry
    assert first.startswith("mqaction_")
    assert first != second


def test_draw_u53_uses_raw_digest_bits_not_hex_ascii() -> None:
    context = _context()
    payload = {"context_sha256": context.context_sha256, "draw_ordinal": 0}
    raw = hashlib.sha256(b"miniqmt_plugin_draw_v1\x00" + canonical_json_bytes_v1(payload)).digest()
    expected = (int.from_bytes(raw[:7], "big") >> 3) / (2**53)

    observed = draw_u53_v1(context=context, draw_ordinal=0)
    assert observed == expected
    assert 0 <= observed < 1
    assert observed != (int.from_bytes(raw.hex().encode()[:7], "big") >> 3) / (2**53)


def test_best_limit_draw_and_ordinal_contract_are_restart_stable() -> None:
    context = _context()
    first = best_limit_quantity_v1(context=context, min_volume=100, max_volume=1000, draw_ordinal=3)
    replay = best_limit_quantity_v1(context=context, min_volume=100, max_volume=1000, draw_ordinal=3)
    assert first == replay
    assert 100 <= first < 1000

    validate_contiguous_ordinals_v1((0, 1, 2, 3))
    with pytest.raises(ValueError, match="duplicate"):
        validate_contiguous_ordinals_v1((0, 1, 1))
    with pytest.raises(ValueError, match="gap"):
        validate_contiguous_ordinals_v1((0, 2))
    with pytest.raises(TypeError):
        draw_u53_v1(context=context, draw_ordinal=True)


def test_context_has_no_wall_clock_uuid_or_random_defaults() -> None:
    fields = DeterministicExecutionContextV1.model_fields
    for field in fields.values():
        assert field.default_factory is None


def test_generic_deterministic_id_kind_cannot_compete_with_persisted_dto_identities() -> None:
    assert tuple(item.value for item in DeterministicIdKindV1) == ("ACTION",)


def test_deterministic_helpers_reject_coercion_invalid_hash_and_range_drift() -> None:
    context = _context()
    business_hash = hash_hex_v1("business", {"quantity": 100})
    with pytest.raises(TypeError, match="context"):
        derive_id_v1(
            context=object(),
            kind=DeterministicIdKindV1.ACTION,
            ordinal=0,
            business_payload_sha256=business_hash,
        )
    with pytest.raises(TypeError, match="kind"):
        derive_id_v1(
            context=context,
            kind="ACTION",
            ordinal=0,
            business_payload_sha256=business_hash,
        )
    with pytest.raises(ValueError, match="non-negative"):
        derive_id_v1(
            context=context,
            kind=DeterministicIdKindV1.ACTION,
            ordinal=-1,
            business_payload_sha256=business_hash,
        )
    with pytest.raises(ValueError, match="sha256"):
        derive_id_v1(
            context=context,
            kind=DeterministicIdKindV1.ACTION,
            ordinal=0,
            business_payload_sha256="bad",
        )
    with pytest.raises(ValueError, match="positive"):
        best_limit_quantity_v1(context=context, min_volume=0, max_volume=100, draw_ordinal=0)
    with pytest.raises(ValueError, match="greater"):
        best_limit_quantity_v1(context=context, min_volume=200, max_volume=100, draw_ordinal=0)
    with pytest.raises(TypeError, match="tuple"):
        validate_contiguous_ordinals_v1([0, 1])
    with pytest.raises(ValueError, match="non-negative"):
        validate_contiguous_ordinals_v1((0, -1))
