from __future__ import annotations

import sys
import types
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.quantevolver.qe_custom_loaders import DynamicFactorsOnlyLoader


def _write_factors(path: Path) -> pd.MultiIndex:
    index = pd.MultiIndex.from_product(
        [
            pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06"]),
            ["000001.SZ", "000002.SZ", "000003.SZ"],
        ],
        names=["datetime", "instrument"],
    )
    pd.DataFrame({"factor": range(len(index))}, index=index).to_parquet(path)
    return index


class _FakeProvider:
    def __init__(self, membership: object) -> None:
        self.membership = membership
        self.market_requests: list[str] = []
        self.label_requests: list[object] = []

    def instruments(self, market: str) -> dict[str, object]:
        self.market_requests.append(market)
        if isinstance(self.membership, Exception):
            raise self.membership
        return {"market": market, "filter_pipe": []}

    def features(
        self,
        instruments: object,
        fields: list[str],
        start_time: object = None,
        end_time: object = None,
    ) -> pd.DataFrame:
        del fields
        self.label_requests.append(instruments)
        dates = pd.date_range(start_time, end_time, freq="B")
        if isinstance(instruments, dict) and "market" in instruments:
            rows = []
            for symbol, spans in dict(self.membership).items():
                for date in dates:
                    if any(
                        pd.Timestamp(start) <= date <= pd.Timestamp(end)
                        for start, end in spans
                    ):
                        rows.append((date, symbol))
            index = pd.MultiIndex.from_tuples(
                rows, names=["datetime", "instrument"]
            )
        else:
            index = pd.MultiIndex.from_product(
                [dates, list(instruments)], names=["datetime", "instrument"]
            )
        return pd.DataFrame({"label": 0.01}, index=index)


def _install_fake_provider(monkeypatch: pytest.MonkeyPatch, provider: object) -> None:
    qlib_module = types.ModuleType("qlib")
    data_module = types.ModuleType("qlib.data")
    data_module.D = provider
    qlib_module.data = data_module
    monkeypatch.setitem(sys.modules, "qlib", qlib_module)
    monkeypatch.setitem(sys.modules, "qlib.data", data_module)


def test_market_name_filters_dynamic_factors_by_pit_membership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    factors = tmp_path / "factors.parquet"
    _write_factors(factors)
    provider = _FakeProvider(
        {
            "000001.SZ": [("2026-01-02", "2026-01-05")],
            "000002.SZ": [("2026-01-06", "2026-01-06")],
        }
    )
    _install_fake_provider(monkeypatch, provider)

    loaded = DynamicFactorsOnlyLoader(str(factors), label_horizon=20).load(
        instruments="index_pool__csi300",
        start_time="2026-01-02",
        end_time="2026-01-06",
    )

    assert provider.market_requests == ["index_pool__csi300"]
    assert set(loaded.index.tolist()) == {
        (pd.Timestamp("2026-01-02"), "000001.SZ"),
        (pd.Timestamp("2026-01-05"), "000001.SZ"),
        (pd.Timestamp("2026-01-06"), "000002.SZ"),
    }
    assert provider.label_requests == [
        {"market": "index_pool__csi300", "filter_pipe": []}
    ]


def test_explicit_instrument_list_filters_without_market_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    factors = tmp_path / "factors.parquet"
    _write_factors(factors)
    provider = _FakeProvider({})
    _install_fake_provider(monkeypatch, provider)

    loaded = DynamicFactorsOnlyLoader(str(factors)).load(
        instruments=["000003.SZ"],
        start_time="2026-01-02",
        end_time="2026-01-06",
    )

    assert loaded.index.get_level_values("instrument").unique().tolist() == [
        "000003.SZ"
    ]
    assert provider.market_requests == []
    assert provider.label_requests == [["000003.SZ"]]


@pytest.mark.parametrize(
    "requested",
    [pd.Index(["000002.SZ"]), np.asarray(["000002.SZ"])],
)
def test_qlib_static_collection_types_remain_supported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, requested: object
) -> None:
    factors = tmp_path / "factors.parquet"
    _write_factors(factors)
    provider = _FakeProvider({})
    _install_fake_provider(monkeypatch, provider)

    loaded = DynamicFactorsOnlyLoader(str(factors)).load(
        instruments=requested,
        start_time="2026-01-02",
        end_time="2026-01-06",
    )

    assert loaded.index.get_level_values("instrument").unique().tolist() == [
        "000002.SZ"
    ]


def test_none_preserves_legacy_unfiltered_loader_semantics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    factors = tmp_path / "factors.parquet"
    source_index = _write_factors(factors)
    provider = _FakeProvider({})
    _install_fake_provider(monkeypatch, provider)

    loaded = DynamicFactorsOnlyLoader(str(factors)).load(
        instruments=None,
        start_time="2026-01-02",
        end_time="2026-01-06",
    )

    assert loaded.index.equals(source_index)
    assert provider.label_requests == [["000001.SZ", "000002.SZ", "000003.SZ"]]


def test_market_resolution_failure_is_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    factors = tmp_path / "factors.parquet"
    _write_factors(factors)
    provider = _FakeProvider(KeyError("missing market"))
    _install_fake_provider(monkeypatch, provider)

    with pytest.raises(
        RuntimeError, match="qe_dynamic_loader_instrument_resolution_failed"
    ):
        DynamicFactorsOnlyLoader(str(factors)).load(
            instruments="index_pool__missing",
            start_time="2026-01-02",
            end_time="2026-01-06",
        )


def test_resolved_market_without_factor_overlap_is_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    factors = tmp_path / "factors.parquet"
    _write_factors(factors)
    provider = _FakeProvider(
        {"999999.SH": [("2026-01-02", "2026-01-06")]}
    )
    _install_fake_provider(monkeypatch, provider)

    with pytest.raises(ValueError, match="qe_dynamic_loader_instrument_filter_empty"):
        DynamicFactorsOnlyLoader(str(factors)).load(
            instruments="index_pool__empty",
            start_time="2026-01-02",
            end_time="2026-01-06",
        )


@pytest.mark.parametrize("requested", [{}, object()])
def test_invalid_instrument_request_is_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, requested: object
) -> None:
    factors = tmp_path / "factors.parquet"
    _write_factors(factors)
    provider = _FakeProvider({})
    _install_fake_provider(monkeypatch, provider)

    with pytest.raises(
        ValueError,
        match="qe_dynamic_loader_instrument_(resolution_empty|contract_invalid)",
    ):
        DynamicFactorsOnlyLoader(str(factors)).load(
            instruments=requested,
            start_time="2026-01-02",
            end_time="2026-01-06",
        )
