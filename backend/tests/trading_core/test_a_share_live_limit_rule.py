from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import inspect

import pytest

from backend.services.dataset_release.a_share_limit_rule import (
    PRICE_LIMIT_RULE_VERSION,
    AShareBoard,
)
from backend.services.trading_core.a_share_live_limit_rule import (
    LIVE_REFERENCE_LIMIT_RULE_VERSION,
    LiveReferenceLimitRuleError,
    derive_live_reference_limit_prices,
)


EVIDENCE_HASH = "a" * 64


@pytest.mark.parametrize(
    ("ts_code", "trade_date", "is_st", "expected_board", "expected_rate"),
    [
        ("600000.SH", date(2026, 7, 5), False, AShareBoard.SH_MAIN, Decimal("0.10")),
        ("600000.SH", date(2026, 7, 5), True, AShareBoard.SH_MAIN, Decimal("0.05")),
        ("600000.SH", date(2026, 7, 6), True, AShareBoard.SH_MAIN, Decimal("0.10")),
        ("300001.SZ", date(2026, 7, 6), True, AShareBoard.CHINEXT, Decimal("0.20")),
        ("688001.SH", date(2026, 7, 6), False, AShareBoard.STAR, Decimal("0.20")),
    ],
)
def test_live_reference_rule_reuses_versioned_board_and_rate_semantics(
    ts_code: str,
    trade_date: date,
    is_st: bool,
    expected_board: AShareBoard,
    expected_rate: Decimal,
) -> None:
    result = derive_live_reference_limit_prices(
        ts_code=ts_code,
        trade_date=trade_date,
        reference_pre_close="10.00",
        reference_evidence_hash=EVIDENCE_HASH,
        price_tick="0.01",
        is_st=is_st,
    )

    assert LIVE_REFERENCE_LIMIT_RULE_VERSION == PRICE_LIMIT_RULE_VERSION
    assert result.board is expected_board
    assert result.limit_rate == expected_rate
    assert result.up_limit == Decimal("10.00") * (Decimal("1") + expected_rate)
    assert result.down_limit == Decimal("10.00") * (Decimal("1") - expected_rate)
    assert result.derivation_hash and len(result.derivation_hash) == 64


def test_live_reference_rule_rounds_half_up_to_explicit_tick() -> None:
    result = derive_live_reference_limit_prices(
        ts_code="600000.SH",
        trade_date=date(2026, 7, 6),
        reference_pre_close="10.05",
        reference_evidence_hash=EVIDENCE_HASH,
        price_tick="0.01",
        is_st=False,
    )

    assert result.up_limit == Decimal("11.06")
    assert result.down_limit == Decimal("9.05")


def test_live_reference_rule_models_proven_no_limit_without_fake_bounds() -> None:
    result = derive_live_reference_limit_prices(
        ts_code="688001.SH",
        trade_date=date(2026, 7, 6),
        reference_pre_close="10.00",
        reference_evidence_hash=EVIDENCE_HASH,
        price_tick="0.01",
        is_st=False,
        no_daily_limit=True,
        no_daily_limit_reason="IPO_FIRST_FIVE_TRADING_DAYS_V1",
    )

    assert result.has_daily_limit is False
    assert result.up_limit is None
    assert result.down_limit is None
    assert result.limit_rate is None
    assert result.derivation_hash is None
    assert result.no_daily_limit_reason == "IPO_FIRST_FIVE_TRADING_DAYS_V1"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"ts_code": "430001.BJ"}, "unsupported A-share board"),
        ({"reference_pre_close": "10.005"}, "align to price_tick"),
        ({"reference_pre_close": "nan"}, "positive and finite"),
        ({"price_tick": "0"}, "positive and finite"),
        ({"reference_evidence_hash": "bad"}, "SHA-256"),
        ({"is_st": 1}, "exact booleans"),
        ({"trade_date": datetime(2026, 7, 6)}, "exact date"),
        ({"no_daily_limit": True}, "versioned reason"),
        ({"no_daily_limit_reason": "UNEXPECTED"}, "forbidden"),
    ],
)
def test_live_reference_rule_rejects_unknown_or_ambiguous_inputs(kwargs: dict[str, object], message: str) -> None:
    payload: dict[str, object] = {
        "ts_code": "600000.SH",
        "trade_date": date(2026, 7, 6),
        "reference_pre_close": "10.00",
        "reference_evidence_hash": EVIDENCE_HASH,
        "price_tick": "0.01",
        "is_st": False,
        "no_daily_limit": False,
        "no_daily_limit_reason": None,
    }
    payload.update(kwargs)

    with pytest.raises(LiveReferenceLimitRuleError, match=message):
        derive_live_reference_limit_prices(**payload)  # type: ignore[arg-type]


def test_live_reference_rule_has_no_adjustment_factor_or_historical_price_input() -> None:
    parameters = set(inspect.signature(derive_live_reference_limit_prices).parameters)

    assert "previous_adj_factor" not in parameters
    assert "current_adj_factor" not in parameters
    assert "historical_close" not in parameters
    assert parameters >= {"reference_pre_close", "reference_evidence_hash", "price_tick"}
