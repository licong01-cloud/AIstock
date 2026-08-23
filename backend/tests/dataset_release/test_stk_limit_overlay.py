from __future__ import annotations

from datetime import date

import pytest

from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.services.dataset_release.stk_limit_overlay import (
    StkLimitRuleOverlayError,
    build_stk_limit_rule_overlay,
)


CODE = "600000.SH"
CODE2 = "300001.SZ"
DAY1 = date(2024, 7, 22)
DAY2 = date(2024, 7, 23)
DAY3 = date(2024, 7, 24)


def _pit(code: str = CODE):
    return freeze_pit_snapshot(
        [
            {
                "ts_code": code,
                "eligible_start": DAY1,
                "eligible_end": DAY3,
                "entry_reason": None,
                "exit_reason": None,
            }
        ],
        universe_key="shsz_st_pit_active_v1",
        rule_version="st_pub_next_trade_restore_active_l_v1",
        scope_start=DAY1,
        cutoff=DAY3,
        state_identity="pit-fixture",
        source_fingerprint_sha256="a" * 64,
        parameter_hash="b" * 64,
    )


def _daily(day: date, close_li: int):
    return {"ts_code": CODE, "trade_date": day, "close_li": close_li}


def _adj(day: date, value: str):
    return {"ts_code": CODE, "trade_date": day, "adj_factor": value}


def _limit(day: date):
    return {
        "ts_code": CODE,
        "trade_date": day,
        "pre_close": "9.00",
        "up_limit": "9.90",
        "down_limit": "8.10",
    }


def test_missing_only_overlay_derives_from_previous_close_and_adjustment_ratio() -> None:
    result = build_stk_limit_rule_overlay(
        pit_snapshot=_pit(),
        trading_dates=(DAY1, DAY2, DAY3),
        partition_start=DAY1,
        partition_end=DAY3,
        database_limit_rows=[_limit(DAY1), _limit(DAY3)],
        daily_rows=[_daily(DAY1, 10_000), _daily(DAY2, 5_100), _daily(DAY3, 5_200)],
        adj_factor_rows=[_adj(DAY1, "1"), _adj(DAY2, "2"), _adj(DAY3, "2")],
    )

    assert result.expected_pit_keys == 3
    assert result.database_rows == 2
    assert result.rule_derived_rows == 1
    assert result.database_completion_rows == 0
    assert result.database_override_rows == 0
    assert result.unresolved_keys == 0
    assert result.overlay_rows == (
        {
            "ts_code": CODE,
            "trade_date": DAY2.isoformat(),
            "pre_close": "5.00",
            "up_limit": "5.50",
            "down_limit": "4.50",
        },
    )


def test_reference_state_crosses_partitions_without_retaining_full_panel() -> None:
    first = build_stk_limit_rule_overlay(
        pit_snapshot=_pit(),
        trading_dates=(DAY1, DAY2, DAY3),
        partition_start=DAY1,
        partition_end=DAY1,
        database_limit_rows=[_limit(DAY1)],
        daily_rows=[_daily(DAY1, 10_000)],
        adj_factor_rows=[_adj(DAY1, "1")],
    )
    second = build_stk_limit_rule_overlay(
        pit_snapshot=_pit(),
        trading_dates=(DAY1, DAY2, DAY3),
        partition_start=DAY2,
        partition_end=DAY3,
        database_limit_rows=[_limit(DAY3)],
        daily_rows=[_daily(DAY2, 10_100), _daily(DAY3, 10_200)],
        adj_factor_rows=[_adj(DAY2, "1"), _adj(DAY3, "1")],
        reference_state=first.reference_state,
    )

    assert second.overlay_rows[0]["trade_date"] == DAY2.isoformat()
    assert second.overlay_rows[0]["pre_close"] == "10.00"
    assert second.peak_code_partition_rows == 5


def test_complete_database_partition_creates_no_overlay() -> None:
    result = build_stk_limit_rule_overlay(
        pit_snapshot=_pit(),
        trading_dates=(DAY1, DAY2, DAY3),
        partition_start=DAY1,
        partition_end=DAY3,
        database_limit_rows=[_limit(DAY1), _limit(DAY2), _limit(DAY3)],
        daily_rows=[_daily(DAY1, 10_000), _daily(DAY2, 10_100), _daily(DAY3, 10_200)],
        adj_factor_rows=[_adj(DAY1, "1"), _adj(DAY2, "1"), _adj(DAY3, "1")],
    )
    assert result.overlay_rows == ()
    assert result.rule_derived_rows == 0
    assert result.database_completion_rows == 0


def test_incomplete_database_row_is_completed_when_non_null_values_match() -> None:
    partial = {
        "ts_code": CODE,
        "trade_date": DAY2,
        "pre_close": None,
        "up_limit": "11.00",
        "down_limit": "9.00",
    }
    result = build_stk_limit_rule_overlay(
        pit_snapshot=_pit(),
        trading_dates=(DAY1, DAY2, DAY3),
        partition_start=DAY1,
        partition_end=DAY3,
        database_limit_rows=[_limit(DAY1), partial, _limit(DAY3)],
        daily_rows=[_daily(DAY1, 10_000), _daily(DAY2, 10_100), _daily(DAY3, 10_200)],
        adj_factor_rows=[_adj(DAY1, "1"), _adj(DAY2, "1"), _adj(DAY3, "1")],
    )

    assert result.database_rows == 3
    assert result.database_completion_rows == 1
    assert result.overlay_rows == (
        {
            "ts_code": CODE,
            "trade_date": DAY2.isoformat(),
            "pre_close": "10.00",
            "up_limit": "11.00",
            "down_limit": "9.00",
        },
    )


def test_incomplete_database_non_null_conflict_fails_closed() -> None:
    partial = {
        "ts_code": CODE,
        "trade_date": DAY2,
        "pre_close": None,
        "up_limit": "10.99",
        "down_limit": "9.00",
    }
    with pytest.raises(StkLimitRuleOverlayError, match="conflicts with rule derivation"):
        build_stk_limit_rule_overlay(
            pit_snapshot=_pit(),
            trading_dates=(DAY1, DAY2, DAY3),
            partition_start=DAY1,
            partition_end=DAY3,
            database_limit_rows=[_limit(DAY1), partial, _limit(DAY3)],
            daily_rows=[_daily(DAY1, 10_000), _daily(DAY2, 10_100), _daily(DAY3, 10_200)],
            adj_factor_rows=[_adj(DAY1, "1"), _adj(DAY2, "1"), _adj(DAY3, "1")],
        )


def test_limit_daily_and_adj_streams_merge_one_code_at_a_time() -> None:
    pit = freeze_pit_snapshot(
        [
            {
                "ts_code": code,
                "eligible_start": DAY1,
                "eligible_end": DAY3,
                "entry_reason": None,
                "exit_reason": None,
            }
            for code in (CODE2, CODE)
        ],
        universe_key="shsz_st_pit_active_v1",
        rule_version="st_pub_next_trade_restore_active_l_v1",
        scope_start=DAY1,
        cutoff=DAY3,
        state_identity="pit-two-code-fixture",
        source_fingerprint_sha256="a" * 64,
        parameter_hash="b" * 64,
    )
    limits = [
        {**_limit(day), "ts_code": code}
        for code in sorted((CODE2, CODE))
        for day in (DAY1, DAY3)
    ]
    daily = [
        {**_daily(day, 10_000), "ts_code": code}
        for code in sorted((CODE2, CODE))
        for day in (DAY1, DAY2, DAY3)
    ]
    adj = [
        {**_adj(day, "1"), "ts_code": code}
        for code in sorted((CODE2, CODE))
        for day in (DAY1, DAY2, DAY3)
    ]

    result = build_stk_limit_rule_overlay(
        pit_snapshot=pit,
        trading_dates=(DAY1, DAY2, DAY3),
        partition_start=DAY1,
        partition_end=DAY3,
        database_limit_rows=limits,
        daily_rows=daily,
        adj_factor_rows=adj,
    )

    assert [(row["ts_code"], row["up_limit"]) for row in result.overlay_rows] == [
        (CODE2, "12.00"),
        (CODE, "11.00"),
    ]
    assert result.database_rows == 4
    assert result.peak_code_partition_rows == 8


def test_first_partition_missing_reference_fails_closed() -> None:
    with pytest.raises(StkLimitRuleOverlayError, match="unresolved PIT keys"):
        build_stk_limit_rule_overlay(
            pit_snapshot=_pit(),
            trading_dates=(DAY1, DAY2, DAY3),
            partition_start=DAY1,
            partition_end=DAY1,
            database_limit_rows=[],
            daily_rows=[_daily(DAY1, 10_000)],
            adj_factor_rows=[_adj(DAY1, "1")],
        )


def test_overlay_hard_limit_blocks_before_unbounded_growth() -> None:
    with pytest.raises(StkLimitRuleOverlayError, match="hard limit"):
        build_stk_limit_rule_overlay(
            pit_snapshot=_pit(),
            trading_dates=(DAY1, DAY2, DAY3),
            partition_start=DAY1,
            partition_end=DAY3,
            database_limit_rows=[_limit(DAY1)],
            daily_rows=[_daily(DAY1, 10_000), _daily(DAY2, 10_100), _daily(DAY3, 10_200)],
            adj_factor_rows=[_adj(DAY1, "1"), _adj(DAY2, "1"), _adj(DAY3, "1")],
            max_overlay_rows=1,
        )
