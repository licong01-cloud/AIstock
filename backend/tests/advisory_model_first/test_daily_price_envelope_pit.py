from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.daily_price_envelope_contracts import (
    AdvisoryDailyPriceEnvelopeV1,
)
from backend.tests.advisory_model_first.test_daily_price_envelope_contract import (
    _envelope,
)


def test_candidate_reference_price_cannot_come_from_after_decision_date() -> None:
    payload = _envelope()
    candidate = deepcopy(payload["candidates"][0])
    candidate["decision_price_trade_date"] = "2026-07-21"
    payload["candidates"] = [candidate]

    with pytest.raises(ValidationError, match="cannot exceed decision trade date"):
        AdvisoryDailyPriceEnvelopeV1(**payload)


def test_target_market_observations_cannot_enter_frozen_contract() -> None:
    payload = _envelope()
    payload["target_open"] = 10.25

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        AdvisoryDailyPriceEnvelopeV1(**payload)
