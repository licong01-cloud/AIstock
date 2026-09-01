from __future__ import annotations

import pytest

from backend.services.dataset_release.cursor import CursorBinding, CursorCodec, CursorInvalid


def _binding(**changes) -> CursorBinding:
    values = {
        "endpoint": "runs",
        "principal": "operator:one",
        "filters": {"state": "SUCCEEDED"},
        "order": "created_at_desc_run_id_desc",
        "generation": "log-generation-1",
    }
    values.update(changes)
    return CursorBinding(**values)


def test_cursor_round_trip_is_opaque_and_bounded() -> None:
    codec = CursorCodec(b"a" * 32)
    cursor = codec.encode(
        binding=_binding(),
        position={"created_at": "2026-08-01T00:00:00Z", "run_id": "run_1"},
    )
    assert "run_1" not in cursor
    assert codec.decode(cursor, binding=_binding())["run_id"] == "run_1"


@pytest.mark.parametrize(
    "binding",
    [
        _binding(endpoint="events"),
        _binding(principal="operator:two"),
        _binding(filters={"state": "FAILED"}),
        _binding(order="event_id_asc"),
        _binding(generation="log-generation-2"),
    ],
)
def test_cursor_rejects_cross_contract_reuse(binding: CursorBinding) -> None:
    codec = CursorCodec(b"a" * 32)
    cursor = codec.encode(binding=_binding(), position={"run_id": "run_1"})
    with pytest.raises(CursorInvalid, match="does not match"):
        codec.decode(cursor, binding=binding)


def test_cursor_rejects_tamper_and_wrong_key() -> None:
    cursor = CursorCodec(b"a" * 32).encode(binding=_binding(), position={"run_id": "run_1"})
    payload, signature = cursor.split(".")
    tampered = f"{payload[:-1]}A.{signature}"
    with pytest.raises(CursorInvalid):
        CursorCodec(b"a" * 32).decode(tampered, binding=_binding())
    with pytest.raises(CursorInvalid, match="signature"):
        CursorCodec(b"b" * 32).decode(cursor, binding=_binding())


def test_cursor_normalizes_tuple_filters_to_canonical_json() -> None:
    codec = CursorCodec(b"a" * 32)
    binding = _binding(filters={"states": ("FAILED", "SUCCEEDED")})
    cursor = codec.encode(binding=binding, position={"run_id": "run_1"})
    assert codec.decode(cursor, binding=binding) == {"run_id": "run_1"}
