from __future__ import annotations

from datetime import date, datetime

import pytest

from backend.services.dataset_release.canonical_stock_transformer import (
    MINUTE_SESSION_TIMES,
    CanonicalStockTransformError,
    CanonicalStockTransformMetrics,
    CanonicalStockTransformSpec,
    CanonicalStockTransformer,
    build_qfq_denominator_authority,
    qfq_denominator_authority_from_mapping,
)
from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS


CODE = "600000.SH"
DAY1 = date(2026, 7, 30)
DAY2 = date(2026, 7, 31)


def _pit(*, start: date = DAY1, end: date = DAY2):
    return freeze_pit_snapshot(
        [
            {
                "ts_code": CODE,
                "eligible_start": start,
                "eligible_end": end,
                "entry_reason": None,
                "exit_reason": None,
            }
        ],
        universe_key="shsz_st_pit_active_v1",
        rule_version="st_pub_next_trade_restore_active_l_v1",
        scope_start=start,
        cutoff=end,
        state_identity="pit-fixture",
        source_fingerprint_sha256="a" * 64,
        parameter_hash="b" * 64,
    )


def _adj(*, future_factor: float = 4.0):
    return [
        {"ts_code": CODE, "trade_date": DAY1, "adj_factor": 2.0},
        {"ts_code": CODE, "trade_date": DAY2, "adj_factor": future_factor},
    ]


def _spec(*, start: date = DAY1, adj=None):
    pit = _pit(start=start)
    factors = list(adj if adj is not None else _adj())
    authority = build_qfq_denominator_authority(
        factors,
        pit_snapshot=pit,
        cutoff=DAY2,
    )
    return CanonicalStockTransformSpec(
        cutoff=DAY2,
        pit_snapshot=pit,
        trading_days=tuple(day for day in (DAY1, DAY2) if day >= start),
        qfq_denominators=authority,
    )


def _raw(code: str, **extra):
    return {
        "ts_code": code,
        "open_li": 9_500.0,
        "high_li": 10_000.0,
        "low_li": 9_000.0,
        "close_li": 10_000.0,
        "volume_hand": 100.0,
        "amount_li": 123_000.0,
        **extra,
    }


def _daily(day: date, *, code: str = CODE):
    return _raw(code, trade_date=day)


def _minute_day(day: date, *, code: str = CODE):
    return [
        _raw(
            code,
            trade_time=datetime.combine(day, minute).isoformat(sep=" ") + "+08:00",
            freq="1m",
        )
        for minute in MINUTE_SESSION_TIMES
    ]


def _limits(*days: date):
    return [
        {
            "ts_code": CODE,
            "trade_date": day,
            "pre_close": 9.5,
            "up_limit": 10.0,
            "down_limit": 9.0,
        }
        for day in days
    ]


def _full_day_suspend(day: date):
    return [
        {
            "ts_code": CODE,
            "trade_date": day,
            "suspend_type": "S",
            "suspend_timing": None,
        }
    ]


def _assert_exact_schema(row):
    assert tuple(row) == ("datetime", "instrument", *QLIB_STOCK_FIELDS)


def test_daily_raw_values_match_qfq_unit_limit_oracle_and_exact_12_fields() -> None:
    spec = _spec()
    rows = list(
        CanonicalStockTransformer().transform_daily(
            spec,
            daily_rows=[_daily(DAY1), _daily(DAY2)],
            adj_factor_rows=_adj(),
            stk_limit_rows=_limits(DAY1, DAY2),
            suspend_rows=[],
        )
    )

    assert len(rows) == 2
    _assert_exact_schema(rows[0])
    assert rows[0]["datetime"] == DAY1.isoformat()
    assert rows[0]["instrument"] == CODE
    assert {field: rows[0][field] for field in QLIB_STOCK_FIELDS} == pytest.approx(
        {
            "open": 4.75,
            "high": 5.0,
            "low": 4.5,
            "close": 5.0,
            "volume": 20_000.0,
            "amount": 123.0,
            "factor": 0.5,
            "up_limit_price": 10.0,
            "down_limit_price": 9.0,
            "prev_close": 9.5,
            "limit_up": 1.0,
            "limit_down": 0.0,
        }
    )


def test_minute_transform_is_exact_240_session_with_same_12_value_semantics() -> None:
    spec = _spec(start=DAY2)
    metrics = CanonicalStockTransformMetrics("minute_bin")
    rows = list(
        CanonicalStockTransformer().transform_minute(
            spec,
            minute_rows=_minute_day(DAY2),
            adj_factor_rows=_adj(),
            stk_limit_rows=_limits(DAY2),
            suspend_rows=[],
            metrics=metrics,
        )
    )

    assert len(rows) == 240
    assert rows[0]["datetime"] == "2026-07-31 09:31:00"
    assert rows[-1]["datetime"] == "2026-07-31 15:00:00"
    assert len({row["datetime"] for row in rows}) == 240
    _assert_exact_schema(rows[0])
    assert rows[0]["factor"] == pytest.approx(1.0)
    assert rows[0]["volume"] == pytest.approx(10_000.0)
    assert rows[0]["amount"] == pytest.approx(123.0)
    assert rows[0]["limit_up"] == 1.0
    assert metrics.peak_minute_stock_day_rows == 240
    assert metrics.full_frames_materialized == 0


def test_full_day_suspend_synthesizes_daily_and_exact_240_zero_volume_minutes() -> None:
    spec = _spec(start=DAY2)
    daily_metrics = CanonicalStockTransformMetrics("daily_bin")
    daily = list(
        CanonicalStockTransformer().transform_daily(
            spec,
            daily_rows=[],
            adj_factor_rows=_adj(),
            stk_limit_rows=_limits(DAY2),
            suspend_rows=_full_day_suspend(DAY2),
            metrics=daily_metrics,
        )
    )
    minute_metrics = CanonicalStockTransformMetrics("minute_bin")
    minute = list(
        CanonicalStockTransformer().transform_minute(
            spec,
            minute_rows=[],
            adj_factor_rows=_adj(),
            stk_limit_rows=_limits(DAY2),
            suspend_rows=_full_day_suspend(DAY2),
            metrics=minute_metrics,
        )
    )

    assert len(daily) == 1
    assert len(minute) == 240
    assert daily[0]["open"] == daily[0]["close"] == pytest.approx(9.5)
    assert daily[0]["volume"] == daily[0]["amount"] == 0.0
    assert minute[0]["open"] == minute[-1]["close"] == pytest.approx(9.5)
    assert {row["volume"] for row in minute} == {0.0}
    assert {row["amount"] for row in minute} == {0.0}
    assert {row["limit_up"] for row in minute} == {0.0}
    assert {row["limit_down"] for row in minute} == {0.0}
    assert daily_metrics.synthesized_stock_days == 1
    assert minute_metrics.synthesized_stock_days == 1


@pytest.mark.parametrize(
    ("minute_rows", "suspend_rows", "pattern"),
    [
        ([], [], "without_full_day_suspend"),
        (_minute_day(DAY2)[:-1], [], "exact 240-row"),
        (
            [],
            [
                {
                    "ts_code": CODE,
                    "trade_date": DAY2,
                    "suspend_type": "S",
                    "suspend_timing": "09:30-10:30",
                }
            ],
            "without_full_day_suspend",
        ),
    ],
)
def test_minute_missing_partial_or_intraday_suspend_gaps_fail_closed(minute_rows, suspend_rows, pattern) -> None:
    spec = _spec(start=DAY2)
    with pytest.raises(CanonicalStockTransformError, match=pattern):
        list(
            CanonicalStockTransformer().transform_minute(
                spec,
                minute_rows=minute_rows,
                adj_factor_rows=_adj(),
                stk_limit_rows=_limits(DAY2),
                suspend_rows=suspend_rows,
            )
        )


def test_rows_outside_pit_are_excluded_but_index_rows_are_rejected() -> None:
    spec = _spec(start=DAY2)
    output = list(
        CanonicalStockTransformer().transform_daily(
            spec,
            daily_rows=[_daily(DAY1), _daily(DAY2)],
            adj_factor_rows=_adj(),
            stk_limit_rows=_limits(DAY1, DAY2),
            suspend_rows=[],
        )
    )
    assert [row["datetime"] for row in output] == [DAY2.isoformat()]

    with pytest.raises(CanonicalStockTransformError, match="index code entered stock"):
        list(
            CanonicalStockTransformer().transform_daily(
                spec,
                daily_rows=[_daily(DAY2), _daily(DAY2, code="000300.SH")],
                adj_factor_rows=_adj(),
                stk_limit_rows=_limits(DAY2),
                suspend_rows=[],
            )
        )


def test_new_cutoff_qfq_denominator_revalues_prior_history() -> None:
    pit = _pit(start=DAY1)
    old_adj = [{"ts_code": CODE, "trade_date": DAY1, "adj_factor": 2.0}]
    old_authority = build_qfq_denominator_authority(
        old_adj,
        pit_snapshot=pit,
        cutoff=DAY2,
    )
    old_spec = CanonicalStockTransformSpec(
        cutoff=DAY2,
        pit_snapshot=pit,
        trading_days=(DAY1, DAY2),
        qfq_denominators=old_authority,
    )
    old = list(
        CanonicalStockTransformer().transform_daily(
            old_spec,
            daily_rows=[_daily(DAY1), _daily(DAY2)],
            adj_factor_rows=old_adj,
            stk_limit_rows=_limits(DAY1, DAY2),
            suspend_rows=[],
        )
    )
    new = list(
        CanonicalStockTransformer().transform_daily(
            _spec(),
            daily_rows=[_daily(DAY1), _daily(DAY2)],
            adj_factor_rows=_adj(),
            stk_limit_rows=_limits(DAY1, DAY2),
            suspend_rows=[],
        )
    )

    assert old[0]["factor"] == 1.0
    assert new[0]["factor"] == 0.5
    assert old[0]["close"] == pytest.approx(10.0)
    assert new[0]["close"] == pytest.approx(5.0)


def test_qfq_denominator_authority_requires_every_pit_code_and_global_order() -> None:
    pit = _pit()
    with pytest.raises(CanonicalStockTransformError, match="every PIT stock"):
        build_qfq_denominator_authority([], pit_snapshot=pit, cutoff=DAY2)
    with pytest.raises(CanonicalStockTransformError, match="globally code/date ordered"):
        build_qfq_denominator_authority(list(reversed(_adj())), pit_snapshot=pit, cutoff=DAY2)


def test_qfq_denominator_canonical_receipt_round_trip_and_tamper_rejection() -> None:
    pit = _pit()
    authority = build_qfq_denominator_authority(_adj(), pit_snapshot=pit, cutoff=DAY2)
    receipt = authority.as_dict()

    restored = qfq_denominator_authority_from_mapping(
        receipt,
        expected_cutoff=DAY2,
        expected_pit_spans_sha256=pit.spans_sha256,
    )
    assert restored == authority

    receipt["values"][0]["denominator"] = 999.0
    with pytest.raises(CanonicalStockTransformError, match="digest differs"):
        qfq_denominator_authority_from_mapping(
            receipt,
            expected_cutoff=DAY2,
            expected_pit_spans_sha256=pit.spans_sha256,
        )
