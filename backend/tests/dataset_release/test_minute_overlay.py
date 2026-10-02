from __future__ import annotations

import hashlib
import json
from datetime import date, datetime

import pandas as pd
import pytest

from backend.services.dataset_release.profile import ResourcePolicy
from backend.services.dataset_release.minute_overlay import (
    MinuteGap,
    MinuteOverlayBuilder,
    MinuteOverlayError,
    MinuteProviderInvalid,
    MinuteProviderRateLimitTerminal,
    MinuteProviderTerminal,
    MinuteProviderUnavailable,
    MinuteSourceConflict,
    canonical_session_times,
    normalize_database_rows,
    normalize_provider_rows,
)


DAY = date(2026, 7, 31)
CODE = "000001.SZ"
POLICY = ResourcePolicy()


def _tdx_rows(day: date = DAY):
    return [
        {
            "TradeTime": value.isoformat(sep=" "),
            "Open": 10_000,
            "High": 11_000,
            "Low": 9_000,
            "Close": 10_500,
            "Volume": 100,
            "Amount": 100_000,
        }
        for value in canonical_session_times(day)
    ]


def _tushare_rows(day: date = DAY):
    return [
        {
            "trade_time": value.isoformat(sep=" "),
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "vol": 10_000,
            "amount": 100.0,
        }
        for value in canonical_session_times(day)
    ]


def _database(rows=239, day: date = DAY, code: str = CODE):
    return pd.DataFrame(
        [
            {
                "trade_time": value,
                "ts_code": code,
                "open_li": 10_000,
                "high_li": 11_000,
                "low_li": 9_000,
                "close_li": 10_500,
                "volume_hand": 100,
                "amount_li": 100_000,
            }
            for value in canonical_session_times(day)[:rows]
        ]
    )


def _database_with_auction(rows=240, day: date = DAY, code: str = CODE):
    auction = pd.DataFrame(
        [
            {
                "trade_time": datetime.combine(day, datetime.min.time()).replace(hour=9, minute=30),
                "ts_code": code,
                "open_li": 9_800,
                "high_li": 10_100,
                "low_li": 9_700,
                "close_li": 10_000,
                "volume_hand": 50,
                "amount_li": 50_000,
            }
        ]
    )
    return pd.concat([auction, _database(rows=rows, day=day, code=code)], ignore_index=True)


class FakeCAS:
    def __init__(self):
        self.payloads = []

    def put_json(self, payload):
        self.payloads.append(payload)
        raw = json.dumps(payload, sort_keys=True).encode()
        return {"sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw)}


def test_tdx_is_first_single_concurrency_and_only_missing_key_enters_cas() -> None:
    calls = []
    cas = FakeCAS()

    def tdx(code, start_date, end_date):
        calls.append(("tdx", code, start_date, end_date))
        return _tdx_rows()

    def tushare(code, day):
        calls.append(("tushare", code, day))
        raise AssertionError("Tushare must not run after a valid TDX response")

    builder = MinuteOverlayBuilder(
        fetch_tdx_rows=tdx,
        fetch_tushare_rows=tushare,
        policy=POLICY,
        cas=cas,
    )
    result = builder.build_one(MinuteGap(CODE, DAY), _database())

    assert calls == [("tdx", CODE, DAY, DAY)]
    assert result.provider == "tdx"
    assert result.provider_rows == 240
    assert result.overlap_rows_verified == 239
    assert len(result.overlay_rows) == 1
    assert result.overlay_rows[0]["trade_time"] == "2026-07-31 15:00:00"
    assert result.provider_cas_ref is not None
    assert result.overlay_cas_ref is not None
    assert len(cas.payloads) == 2
    assert cas.payloads[1]["database_writes"] == 0
    assert builder.peak_provider_calls == 1


def test_incomplete_tdx_is_rejected_then_tushare_units_are_normalized() -> None:
    calls = []

    def tdx(_code, _start_date, _end_date):
        calls.append("tdx")
        return _tdx_rows()[:-1]

    def tushare(_code, _day):
        calls.append("tushare")
        return _tushare_rows()

    result = MinuteOverlayBuilder(
        fetch_tdx_rows=tdx,
        fetch_tushare_rows=tushare,
        policy=POLICY,
    ).build_one(MinuteGap(CODE, DAY), _database())

    assert calls == ["tdx", "tushare"]
    assert result.provider == "tushare"
    assert result.overlay_rows[0]["close_li"] == 10_500
    assert result.overlay_rows[0]["volume_hand"] == 100
    assert result.overlay_rows[0]["amount_li"] == 100_000
    assert [attempt.status for attempt in result.attempts] == ["REJECTED", "PASS"]


def test_provider_attempt_receipt_hashes_secret_like_exception_text() -> None:
    sensitive_text = "SENSITIVE_VALUE=" + "https://example.invalid/" + "?field=abc"

    result = MinuteOverlayBuilder(
        fetch_tdx_rows=lambda *_args: (_ for _ in ()).throw(RuntimeError(sensitive_text)),
        fetch_tushare_rows=lambda *_args: _tushare_rows(),
        policy=POLICY,
    ).build_one(MinuteGap(CODE, DAY), _database())

    encoded = json.dumps(result.as_dict(), sort_keys=True)
    assert sensitive_text not in encoded
    rejected = result.attempts[0].as_dict()
    assert "error" not in rejected
    assert rejected["error_type"] == "RuntimeError"
    assert len(rejected["message_sha256"]) == 64


def test_equal_normalized_provider_bytes_have_provider_independent_content_hash() -> None:
    tdx_result = MinuteOverlayBuilder(
        fetch_tdx_rows=lambda *_args: _tdx_rows(),
        fetch_tushare_rows=lambda *_args: (),
        policy=POLICY,
    ).build_one(MinuteGap(CODE, DAY), _database())
    tushare_result = MinuteOverlayBuilder(
        fetch_tdx_rows=lambda *_args: _tdx_rows()[:-1],
        fetch_tushare_rows=lambda *_args: _tushare_rows(),
        policy=POLICY,
    ).build_one(MinuteGap(CODE, DAY), _database())

    assert tdx_result.provider_content_sha256 == tushare_result.provider_content_sha256
    assert tdx_result.overlay_content_sha256 == tushare_result.overlay_content_sha256


def test_provider_database_overlap_mismatch_is_terminal_without_fallback() -> None:
    rows = _tdx_rows()
    rows[0] = {**rows[0], "Close": 10_600}
    tushare_calls = []
    builder = MinuteOverlayBuilder(
        fetch_tdx_rows=lambda *_args: rows,
        fetch_tushare_rows=lambda *_args: tushare_calls.append(True),
        policy=POLICY,
    )

    with pytest.raises(MinuteSourceConflict, match="overlap mismatch"):
        builder.build_one(MinuteGap(CODE, DAY), _database())
    assert tushare_calls == []


def test_duplicate_nonfinite_and_non240_rows_are_never_accepted() -> None:
    gap = MinuteGap(CODE, DAY)
    duplicate = _tdx_rows()
    duplicate[-1] = dict(duplicate[-2])
    with pytest.raises(MinuteProviderInvalid, match="duplicate"):
        normalize_provider_rows(duplicate, provider="tdx", gap=gap)

    nonfinite = _tdx_rows()
    nonfinite[0] = {**nonfinite[0], "Open": float("inf")}
    with pytest.raises(MinuteProviderInvalid, match="not finite"):
        normalize_provider_rows(nonfinite, provider="tdx", gap=gap)

    with pytest.raises(MinuteProviderInvalid, match="not 240"):
        normalize_provider_rows(_tdx_rows()[:-1], provider="tdx", gap=gap)

    database = _database()
    database.iloc[-1] = database.iloc[-2]
    with pytest.raises(MinuteProviderInvalid, match="duplicate"):
        normalize_database_rows(database, gap)


def test_database_single_0930_auction_row_normalizes_to_same_240_core_session() -> None:
    gap = MinuteGap(CODE, DAY)
    core = normalize_database_rows(_database(rows=240), gap)
    with_auction = normalize_database_rows(_database_with_auction(), gap)

    pd.testing.assert_frame_equal(with_auction, core)
    assert len(with_auction) == 240
    assert with_auction.iloc[0]["trade_time"] == datetime(2026, 7, 31, 9, 31)


def test_database_duplicate_auction_and_other_out_of_session_rows_fail_closed() -> None:
    gap = MinuteGap(CODE, DAY)
    duplicate_auction = pd.concat(
        [_database_with_auction(rows=239), _database_with_auction(rows=0)],
        ignore_index=True,
    )
    with pytest.raises(MinuteProviderInvalid, match="duplicate"):
        normalize_database_rows(duplicate_auction, gap)

    unexpected = _database(rows=240)
    unexpected.loc[len(unexpected)] = {
        "trade_time": datetime(2026, 7, 31, 13, 0),
        "ts_code": CODE,
        "open_li": 10_000,
        "high_li": 11_000,
        "low_li": 9_000,
        "close_li": 10_500,
        "volume_hand": 100,
        "amount_li": 100_000,
    }
    with pytest.raises(MinuteProviderInvalid, match="out-of-session"):
        normalize_database_rows(unexpected, gap)


def test_tushare_40203_is_retryable_without_busy_loop() -> None:
    class RateLimited(RuntimeError):
        code = 40203

    calls = []

    def rate_limited(_code, _day):
        calls.append("tushare")
        raise RateLimited("provider code=40203 one request per hour")

    builder = MinuteOverlayBuilder(
        fetch_tdx_rows=lambda *_args: _tdx_rows()[:-1],
        fetch_tushare_rows=rate_limited,
        policy=POLICY,
    )
    with pytest.raises(MinuteProviderRateLimitTerminal, match="40203") as raised:
        builder.build_one(MinuteGap(CODE, DAY), _database())
    assert raised.value.code == "WAITING_PROVIDER_RATE_LIMIT_40203"
    assert raised.value.retryable is True
    assert calls == ["tushare"]


def test_both_invalid_providers_fail_closed() -> None:
    builder = MinuteOverlayBuilder(
        fetch_tdx_rows=lambda *_args: _tdx_rows()[:-1],
        fetch_tushare_rows=lambda *_args: _tushare_rows()[:-1],
        policy=POLICY,
    )
    with pytest.raises(MinuteProviderTerminal, match="invalid"):
        builder.build_one(MinuteGap(CODE, DAY), _database())


def test_both_provider_transport_failures_are_retryable() -> None:
    def unavailable(*_args):
        raise ConnectionError("provider unavailable")

    builder = MinuteOverlayBuilder(
        fetch_tdx_rows=unavailable,
        fetch_tushare_rows=unavailable,
        policy=POLICY,
    )
    with pytest.raises(MinuteProviderUnavailable) as raised:
        builder.build_one(MinuteGap(CODE, DAY), _database())
    assert raised.value.code == "WAITING_MINUTE_PROVIDER_UNAVAILABLE"
    assert raised.value.retryable is True


def test_tushare_fetch_terminal_error_is_not_reclassified_as_retryable() -> None:
    builder = MinuteOverlayBuilder(
        fetch_tdx_rows=lambda *_args: _tdx_rows()[:-1],
        fetch_tushare_rows=lambda *_args: (_ for _ in ()).throw(
            MinuteProviderTerminal("invalid bounded provider payload")
        ),
        policy=POLICY,
    )

    with pytest.raises(MinuteProviderTerminal) as raised:
        builder.build_one(MinuteGap(CODE, DAY), _database())

    assert raised.value.retryable is False


def test_provider_concurrency_has_no_bypass_parameter() -> None:
    with pytest.raises(TypeError, match="unexpected keyword"):
        MinuteOverlayBuilder(
            fetch_tdx_rows=lambda *_args: (),
            fetch_tushare_rows=lambda *_args: (),
            policy=POLICY,
            provider_concurrency=2,
        )  # type: ignore[call-arg]


def test_ordered_gap_stream_fetches_one_bounded_window_per_current_code() -> None:
    prior = date(2026, 7, 30)
    second_code = "000002.SZ"
    calls = []

    def tdx(code, start_date, end_date):
        calls.append((code, start_date, end_date))
        if code == CODE:
            return _tdx_rows(prior) + _tdx_rows(DAY)
        return _tdx_rows(DAY)

    builder = MinuteOverlayBuilder(
        fetch_tdx_rows=tdx,
        fetch_tushare_rows=lambda *_args: (),
        policy=POLICY,
    )
    results = list(
        builder.iter_many(
            [
                (MinuteGap(CODE, prior), _database(day=prior)),
                (MinuteGap(CODE, DAY), _database()),
                (MinuteGap(second_code, DAY), _database(code=second_code)),
            ]
        )
    )

    assert [(result.ts_code, result.trade_date) for result in results] == [
        (CODE, prior),
        (CODE, DAY),
        (second_code, DAY),
    ]
    assert calls == [
        (CODE, prior, DAY),
        (second_code, DAY, DAY),
    ]
    assert builder.peak_provider_calls == 1


def test_code_window_normalizes_each_tdx_row_once_across_multiple_days() -> None:
    days = [date(2026, 7, 29), date(2026, 7, 30), DAY]
    raw_rows = [row for day in days for row in _tdx_rows(day)]
    visits = 0
    calls = []

    class CountingRows:
        def __iter__(self):
            nonlocal visits
            for row in raw_rows:
                visits += 1
                yield row

    def tdx(code, start_date, end_date):
        calls.append((code, start_date, end_date))
        return CountingRows()

    builder = MinuteOverlayBuilder(
        fetch_tdx_rows=tdx,
        fetch_tushare_rows=lambda *_args: (_ for _ in ()).throw(AssertionError("valid TDX dates must not fall back")),
        policy=POLICY,
    )
    results = list(builder.iter_many((MinuteGap(CODE, day), _database(day=day)) for day in days))

    assert calls == [(CODE, days[0], days[-1])]
    assert visits == len(days) * 240
    assert [result.provider for result in results] == ["tdx"] * len(days)
    assert [len(result.overlay_rows) for result in results] == [1] * len(days)


def test_code_window_keeps_tdx_rejection_and_tushare_fallback_per_day() -> None:
    prior = date(2026, 7, 30)
    tdx_window = _tdx_rows(prior)[:-1] + _tdx_rows(DAY)
    tdx_calls = []
    tushare_calls = []

    def tdx(code, start_date, end_date):
        tdx_calls.append((code, start_date, end_date))
        return tdx_window

    def tushare(code, trade_date):
        tushare_calls.append((code, trade_date))
        return _tushare_rows(trade_date)

    results = list(
        MinuteOverlayBuilder(
            fetch_tdx_rows=tdx,
            fetch_tushare_rows=tushare,
            policy=POLICY,
        ).iter_many(
            [
                (MinuteGap(CODE, prior), _database(day=prior)),
                (MinuteGap(CODE, DAY), _database()),
            ]
        )
    )

    assert tdx_calls == [(CODE, prior, DAY)]
    assert tushare_calls == [(CODE, prior)]
    assert [result.provider for result in results] == ["tushare", "tdx"]
    assert [attempt.status for attempt in results[0].attempts] == [
        "REJECTED",
        "PASS",
    ]
    assert [attempt.status for attempt in results[1].attempts] == ["PASS"]

    reference_builder = MinuteOverlayBuilder(
        fetch_tdx_rows=lambda *_args: tdx_window,
        fetch_tushare_rows=lambda _code, trade_date: _tushare_rows(trade_date),
        policy=POLICY,
    )
    reference = [
        reference_builder.build_one(MinuteGap(CODE, prior), _database(day=prior)),
        reference_builder.build_one(MinuteGap(CODE, DAY), _database()),
    ]
    assert [result.as_dict() for result in results] == [result.as_dict() for result in reference]
    assert [result.overlay_rows for result in results] == [result.overlay_rows for result in reference]


def test_gap_stream_rejects_order_drift_before_fetching_code_window() -> None:
    prior = date(2026, 7, 30)
    calls = []
    builder = MinuteOverlayBuilder(
        fetch_tdx_rows=lambda *args: calls.append(args) or (),
        fetch_tushare_rows=lambda *_args: (),
        policy=POLICY,
    )

    with pytest.raises(MinuteOverlayError, match="strictly ordered"):
        list(
            builder.iter_many(
                [
                    (MinuteGap(CODE, DAY), _database()),
                    (MinuteGap(CODE, prior), _database(day=prior)),
                ]
            )
        )
    assert calls == []
