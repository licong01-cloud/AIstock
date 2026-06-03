from __future__ import annotations

from datetime import date

import pytest

from backend.services.selection_center.models import SelectionCandidate
from backend.services.selection_center.price_guidance import (
    GUIDANCE_STATUS_RULE_DEFAULT,
    PRICE_GUIDANCE_COMPONENT_KEY,
    attach_price_guidance,
    build_price_guidance,
)
from backend.services.trading_core.errors import RuntimeConfigInvalidError


def _candidate(**updates) -> SelectionCandidate:
    base = {
        "symbol": "000001.SZ",
        "score": 0.9,
        "rank": 1,
        "target_weight": 0.03,
        "reference_price": 10.0,
        "selection_entry_price": 10.0,
        "selection_entry_price_source": "market.kline_daily_raw.close:2026-06-01",
        "selection_entry_price_time": "2026-06-01",
        "previous_close": 10.0,
    }
    base.update(updates)
    return SelectionCandidate(**base)


def test_s1_4_s1_5_selection_guidance_fields_tick_limits_disclaimer_and_status() -> None:
    row = build_price_guidance(_candidate(), trade_date=date(2026, 6, 2), runtime_config={})

    assert row.signal_ref_price == pytest.approx(10.0)
    assert row.guidance_status == GUIDANCE_STATUS_RULE_DEFAULT
    assert row.price_guard_policy_sha256
    band = row.suggested_entry_price_band
    stop = row.suggested_stop_loss_zone
    assert band is not None
    assert stop is not None
    assert band["green"]["max_price"] == pytest.approx(10.15)
    assert band["yellow"]["max_price"] == pytest.approx(10.2)
    assert round(band["limit_up"] * 100) == band["limit_up"] * 100
    assert band["limit_down"] == pytest.approx(9.0)
    assert "最终委托价" in band["disclaimer"]
    assert stop["soft_stop_price"] == pytest.approx(9.6)
    assert stop["hard_stop_price"] == pytest.approx(9.4)
    assert stop["take_profit_enabled"] is False
    assert PRICE_GUIDANCE_COMPONENT_KEY in row.component_scores


def test_s1_4_missing_signal_ref_price_degrades_without_default_price() -> None:
    row = build_price_guidance(
        _candidate(selection_entry_price=None, reference_price=88.0),
        trade_date=date(2026, 6, 2),
        runtime_config={},
    )

    assert row.signal_ref_price is None
    assert row.suggested_entry_price_band is None
    assert row.suggested_stop_loss_zone is None
    payload = row.component_scores[PRICE_GUIDANCE_COMPONENT_KEY]
    assert payload["guidance_unavailable_reason"] == "signal_ref_price_missing"
    assert payload["signal_ref_price"] is None


def test_s1_4_basis_mismatch_fails_fast() -> None:
    with pytest.raises(RuntimeConfigInvalidError, match="raw price_basis"):
        build_price_guidance(
            _candidate(),
            trade_date=date(2026, 6, 2),
            runtime_config={"price_guidance": {"price_basis": "adjusted"}},
        )


def test_s1_5_attach_guidance_keeps_all_generated_rows_rule_default() -> None:
    rows = attach_price_guidance(
        [_candidate(symbol="000001.SZ", rank=1), _candidate(symbol="300001.SZ", rank=6)],
        trade_date=date(2026, 6, 2),
        runtime_config={},
    )

    assert {row.guidance_status for row in rows} == {GUIDANCE_STATUS_RULE_DEFAULT}
    assert rows[1].suggested_entry_price_band["limit_up"] == pytest.approx(12.0)


def test_s1_12_price_guidance_disabled_returns_candidate_unchanged() -> None:
    candidate = _candidate()

    row = build_price_guidance(
        candidate,
        trade_date=date(2026, 6, 2),
        runtime_config={"price_guidance": {"enabled": False}},
    )

    assert row == candidate
