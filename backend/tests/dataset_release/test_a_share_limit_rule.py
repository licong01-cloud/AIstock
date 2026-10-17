from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from backend.services.dataset_release.a_share_limit_rule import (
    PRICE_LIMIT_RULE_VERSION,
    AShareBoard,
    AShareLimitRuleError,
    classify_a_share_board,
    derive_limit_prices,
    resolve_limit_rate,
)


@pytest.mark.parametrize(
    ("ts_code", "expected"),
    [
        ("600000.SH", AShareBoard.SH_MAIN),
        ("605001.SH", AShareBoard.SH_MAIN),
        ("000001.SZ", AShareBoard.SZ_MAIN),
        ("002001.SZ", AShareBoard.SZ_MAIN),
        ("300001.SZ", AShareBoard.CHINEXT),
        ("301001.SZ", AShareBoard.CHINEXT),
        ("302132.SZ", AShareBoard.CHINEXT),
        ("688001.SH", AShareBoard.STAR),
        ("689001.SH", AShareBoard.STAR),
    ],
)
def test_board_classification_is_explicit_and_shsz_a_only(ts_code, expected) -> None:
    assert classify_a_share_board(ts_code) is expected


@pytest.mark.parametrize("ts_code", ["430001.BJ", "900901.SH", "200001.SZ", "000300.SH", "ABC"])
def test_unknown_or_non_a_share_board_fails_closed(ts_code) -> None:
    with pytest.raises(AShareLimitRuleError, match="unsupported A-share board"):
        classify_a_share_board(ts_code)


@pytest.mark.parametrize(
    ("ts_code", "trade_date", "is_st", "expected"),
    [
        ("600000.SH", date(2026, 7, 5), False, Decimal("0.10")),
        ("600000.SH", date(2026, 7, 5), True, Decimal("0.05")),
        ("600000.SH", date(2026, 7, 6), True, Decimal("0.10")),
        ("000001.SZ", date(2026, 7, 5), True, Decimal("0.05")),
        ("000001.SZ", date(2026, 7, 6), True, Decimal("0.10")),
        ("300001.SZ", date(2020, 8, 23), False, Decimal("0.10")),
        ("300001.SZ", date(2020, 8, 23), True, Decimal("0.05")),
        ("300001.SZ", date(2020, 8, 24), True, Decimal("0.20")),
        ("302132.SZ", date(2026, 5, 8), False, Decimal("0.20")),
        ("688001.SH", date(2024, 7, 23), True, Decimal("0.20")),
    ],
)
def test_limit_rate_uses_board_st_and_effective_date(ts_code, trade_date, is_st, expected) -> None:
    decision = resolve_limit_rate(ts_code=ts_code, trade_date=trade_date, is_st=is_st)

    assert decision.limit_rate == expected
    assert decision.rule_version == PRICE_LIMIT_RULE_VERSION
    assert decision.has_daily_limit is True


def test_explicit_no_daily_limit_is_typed_and_cannot_be_materialized() -> None:
    decision = resolve_limit_rate(
        ts_code="688001.SH",
        trade_date=date(2026, 7, 31),
        is_st=False,
        no_daily_limit=True,
    )
    assert decision.has_daily_limit is False
    assert decision.limit_rate is None

    with pytest.raises(AShareLimitRuleError, match="no daily price limit"):
        derive_limit_prices(
            ts_code="688001.SH",
            trade_date=date(2026, 7, 31),
            previous_close="10.00",
            previous_adj_factor="1.0",
            current_adj_factor="1.0",
            is_st=False,
            no_daily_limit=True,
        )


def test_derive_limit_prices_uses_adjustment_ratio_and_exchange_rounding() -> None:
    derived = derive_limit_prices(
        ts_code="600000.SH",
        trade_date=date(2024, 7, 23),
        previous_close="10.00",
        previous_adj_factor="1.0",
        current_adj_factor="2.0",
        is_st=False,
    )

    assert derived.pre_close == Decimal("5.00")
    assert derived.up_limit == Decimal("5.50")
    assert derived.down_limit == Decimal("4.50")
    assert derived.limit_rate == Decimal("0.10")
    assert derived.rule_version == PRICE_LIMIT_RULE_VERSION


def test_round_half_up_and_minimum_one_tick_are_deterministic() -> None:
    rounded = derive_limit_prices(
        ts_code="000001.SZ",
        trade_date=date(2024, 7, 23),
        previous_close=Decimal("10.05"),
        previous_adj_factor=Decimal("1"),
        current_adj_factor=Decimal("1"),
        is_st=True,
    )
    assert rounded.pre_close == Decimal("10.05")
    assert rounded.up_limit == Decimal("10.55")
    assert rounded.down_limit == Decimal("9.55")

    penny = derive_limit_prices(
        ts_code="600000.SH",
        trade_date=date(2024, 7, 23),
        previous_close=Decimal("0.01"),
        previous_adj_factor=Decimal("1"),
        current_adj_factor=Decimal("1"),
        is_st=True,
    )
    assert penny.up_limit == Decimal("0.02")
    assert penny.down_limit == Decimal("0.01")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("previous_close", "0"),
        ("previous_close", "NaN"),
        ("previous_adj_factor", "0"),
        ("current_adj_factor", "Infinity"),
    ],
)
def test_non_positive_or_non_finite_reference_inputs_fail_closed(field, value) -> None:
    kwargs = {
        "previous_close": "10",
        "previous_adj_factor": "1",
        "current_adj_factor": "1",
    }
    kwargs[field] = value
    with pytest.raises(AShareLimitRuleError, match=field):
        derive_limit_prices(
            ts_code="600000.SH",
            trade_date=date(2024, 7, 23),
            is_st=False,
            **kwargs,
        )
