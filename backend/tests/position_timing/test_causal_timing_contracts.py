from __future__ import annotations

from datetime import date

import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing.causal_timing_contracts import (
    CONTRACT, CONTRACT_SHA256, FEATURE_ORDER, split_for_decision, validate_contract,
)
from backend.services.position_timing.contracts import canonical_sha256


def test_contract_identity_and_market_only_feature_order() -> None:
    assert CONTRACT_SHA256 == canonical_sha256(CONTRACT)
    assert len(FEATURE_ORDER) == 14
    assert "holding_exposure" not in FEATURE_ORDER
    validate_contract({"contract": CONTRACT, "contract_sha256": CONTRACT_SHA256})


def test_contract_drift_fails_closed() -> None:
    with pytest.raises(ActionValueError, match="CAUSAL_TIMING_CONTRACT_DRIFT"):
        validate_contract({"contract": CONTRACT, "contract_sha256": "0" * 64})


@pytest.mark.parametrize(
    ("decision", "available", "expected"),
    [
        (date(2022, 12, 1), date(2022, 12, 29), "TRAIN"),
        (date(2022, 12, 20), date(2023, 1, 20), "IMMATURE"),
        (date(2024, 6, 1), date(2024, 6, 28), "VALIDATION"),
        (date(2024, 7, 1), date(2024, 8, 1), "TEST"),
    ],
)
def test_label_maturity_is_checked_against_split_boundary(
    decision: date, available: date, expected: str,
) -> None:
    assert split_for_decision(decision, label_available_at=available) == expected
