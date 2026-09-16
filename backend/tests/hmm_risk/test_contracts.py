from __future__ import annotations

import hashlib

import pytest

from backend.services.hmm_risk.contracts import (
    ALL_CORE_FEATURES,
    BASE_FEATURES,
    canonical_json_bytes,
    canonical_sha256,
)


def test_canonical_json_contract_is_ordered_compact_and_unicode_preserving() -> None:
    value = {"z": [2, 1], "label": "申万一级", "a": {"ok": True}}

    payload = canonical_json_bytes(value)

    assert payload == b'{"a":{"ok":true},"label":"\xe7\x94\xb3\xe4\xb8\x87\xe4\xb8\x80\xe7\xba\xa7","z":[2,1]}'
    assert canonical_sha256(value) == hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_canonical_json_contract_rejects_non_finite_values(value: float) -> None:
    with pytest.raises(ValueError, match="Out of range float values"):
        canonical_json_bytes({"value": value})


def test_active_feature_contract_is_unique_and_extends_the_base_in_order() -> None:
    assert len(BASE_FEATURES) == 7
    assert len(ALL_CORE_FEATURES) == 20
    assert ALL_CORE_FEATURES[: len(BASE_FEATURES)] == BASE_FEATURES
    assert len(set(ALL_CORE_FEATURES)) == len(ALL_CORE_FEATURES)
