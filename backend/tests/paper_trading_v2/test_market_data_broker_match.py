"""Tests for BrokerBackend <-> MinuteDataSource strong binding.

Strategy Engine design 2026-05-08 §3.6.4 (R-Q9 D3) requires that minute data
sources are strongly bound to broker backends. Cross-pairing must fail fast
with a typed BrokerMarketSourceMismatchError; no silent fallback.
"""

from __future__ import annotations

import pytest

from backend.services.paper_trading_v2.market_data import (
    ALLOWED_MARKET_SOURCES,
    MinuteDataSource,
    assert_broker_market_source_match,
)
from backend.services.trading_core.errors import BrokerMarketSourceMismatchError


def test_miniqmt_realtime_enum_value_exists() -> None:
    assert MinuteDataSource.MINIQMT_REALTIME.value == "MINIQMT_REALTIME"
    assert MinuteDataSource("MINIQMT_REALTIME") is MinuteDataSource.MINIQMT_REALTIME


def test_allowed_market_sources_matches_design_3_6_4() -> None:
    assert ALLOWED_MARKET_SOURCES == {
        "local_sim": {MinuteDataSource.TDX_REALTIME, MinuteDataSource.DB_HISTORICAL},
        "minqmt_sim": {MinuteDataSource.MINIQMT_REALTIME},
        "minqmt_live": {MinuteDataSource.MINIQMT_REALTIME},
    }


@pytest.mark.parametrize(
    "broker_id,source",
    [
        ("local_sim", MinuteDataSource.TDX_REALTIME),
        ("local_sim", MinuteDataSource.DB_HISTORICAL),
        ("minqmt_sim", MinuteDataSource.MINIQMT_REALTIME),
        ("minqmt_live", MinuteDataSource.MINIQMT_REALTIME),
    ],
)
def test_assert_broker_market_source_match_accepts_allowed_pairs(
    broker_id: str, source: MinuteDataSource
) -> None:
    assert_broker_market_source_match(broker_id, source)


@pytest.mark.parametrize(
    "broker_id,source",
    [
        ("local_sim", MinuteDataSource.MINIQMT_REALTIME),
        ("minqmt_sim", MinuteDataSource.TDX_REALTIME),
        ("minqmt_sim", MinuteDataSource.DB_HISTORICAL),
        ("minqmt_live", MinuteDataSource.TDX_REALTIME),
        ("minqmt_live", MinuteDataSource.DB_HISTORICAL),
    ],
)
def test_assert_broker_market_source_match_rejects_cross_pairing(
    broker_id: str, source: MinuteDataSource
) -> None:
    with pytest.raises(BrokerMarketSourceMismatchError) as exc_info:
        assert_broker_market_source_match(broker_id, source)
    err = exc_info.value
    assert err.context["broker_id"] == broker_id
    assert err.context["given_source"] == source.value
    assert source.value not in err.context["allowed"]


def test_assert_broker_market_source_match_rejects_unknown_broker_id() -> None:
    with pytest.raises(BrokerMarketSourceMismatchError) as exc_info:
        assert_broker_market_source_match("vnpy_ctp", MinuteDataSource.TDX_REALTIME)
    err = exc_info.value
    assert err.context["broker_id"] == "vnpy_ctp"
    assert "known_broker_ids" in err.context
    assert "local_sim" in err.context["known_broker_ids"]


def test_assert_broker_market_source_match_rejects_non_enum_source() -> None:
    with pytest.raises(BrokerMarketSourceMismatchError) as exc_info:
        assert_broker_market_source_match("local_sim", "TDX_REALTIME")  # type: ignore[arg-type]
    err = exc_info.value
    assert err.context["broker_id"] == "local_sim"


def test_broker_market_source_mismatch_error_code() -> None:
    err = BrokerMarketSourceMismatchError("test", context={"k": "v"})
    assert err.error_code == "BROKER_MARKET_SOURCE_MISMATCH"
    payload = err.to_dict()
    assert payload["error_code"] == "BROKER_MARKET_SOURCE_MISMATCH"
    assert payload["context"] == {"k": "v"}
