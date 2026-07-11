from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.data_service.moneyflow_contract import (
    MONEYFLOW_FIELD_MAP,
    MONEYFLOW_FACTOR_AMOUNT_COLUMNS,
    MONEYFLOW_FACTOR_VOLUME_COLUMNS,
    TUSHARE_MONEYFLOW_AMOUNT_COLUMNS,
    TUSHARE_MONEYFLOW_VOLUME_COLUMNS,
    assert_moneyflow_frame_parity,
    derive_moneyflow_factors,
    normalize_tushare_moneyflow_units,
)
from backend.data_service.preprocessor import compute_precomputed_factors
from backend.data_service.qe_data_service import compute_moneyflow_derived_factors


def _index(periods: int = 20) -> pd.MultiIndex:
    return pd.MultiIndex.from_product(
        [pd.date_range("2026-06-01", periods=periods, freq="D"), ["000001.SZ"]],
        names=["datetime", "instrument"],
    )


def _canonical_moneyflow(periods: int = 20) -> pd.DataFrame:
    idx = _index(periods)
    data = {column: np.full(periods, 1_000.0) for column in MONEYFLOW_FACTOR_VOLUME_COLUMNS}
    data.update({column: np.full(periods, 10_000.0) for column in MONEYFLOW_FACTOR_AMOUNT_COLUMNS})
    frame = pd.DataFrame(data, index=idx)
    frame["mf_lg_buy_amt"] = 400_000.0
    frame["mf_lg_sell_amt"] = 250_000.0
    frame["mf_elg_buy_amt"] = 300_000.0
    frame["mf_elg_sell_amt"] = 200_000.0
    frame["mf_lg_buy_vol"] = 4_000.0
    frame["mf_lg_sell_vol"] = 2_500.0
    frame["mf_elg_buy_vol"] = 3_000.0
    frame["mf_elg_sell_vol"] = 2_000.0
    frame["mf_net_amt"] = 100_000.0
    frame["mf_net_vol"] = 2_000.0
    return frame


def _daily(periods: int = 20) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "close": np.full(periods, 10.0),
            "amount": np.full(periods, 1_000_000.0),
            # qfq-adjusted volume=raw shares/factor, so raw shares=20,000.
            "volume": np.full(periods, 40_000.0),
            "factor": np.full(periods, 0.5),
        },
        index=_index(periods),
    )


def test_normalize_tushare_moneyflow_units_converts_all_18_source_fields() -> None:
    raw = pd.DataFrame(
        {
            **{column: [2.0] for column in TUSHARE_MONEYFLOW_VOLUME_COLUMNS},
            **{column: [3.0] for column in TUSHARE_MONEYFLOW_AMOUNT_COLUMNS},
        }
    )

    normalized = normalize_tushare_moneyflow_units(raw).rename(columns=MONEYFLOW_FIELD_MAP)

    assert all(normalized[column].iat[0] == 200.0 for column in MONEYFLOW_FACTOR_VOLUME_COLUMNS)
    assert all(normalized[column].iat[0] == 30_000.0 for column in MONEYFLOW_FACTOR_AMOUNT_COLUMNS)


def test_derived_total_net_uses_tushare_net_fields_and_raw_turnover_volume() -> None:
    moneyflow = _canonical_moneyflow()
    derived = derive_moneyflow_factors(moneyflow, _daily())

    row = derived.iloc[-1]
    assert row["mf_total_net_amt"] == 100_000.0
    assert row["mf_total_net_vol"] == 2_000.0
    assert row["mf_total_net_amt_ratio"] == pytest.approx(0.1)
    assert row["mf_total_net_vol_ratio"] == pytest.approx(0.1)
    assert row["mf_main_net_amt"] == 250_000.0
    assert row["mf_elg_net_amt"] == 100_000.0
    assert row["mf_elg_share_in_main_amt"] == pytest.approx(0.4)
    assert row["mf_total_net_amt_5d"] == 500_000.0
    assert row["mf_total_net_amt_ratio_5d"] == pytest.approx(0.1)


def test_qe_and_live_preprocessor_share_identical_moneyflow_formulas() -> None:
    moneyflow = _canonical_moneyflow()
    daily = _daily()

    qe = compute_moneyflow_derived_factors(moneyflow, daily)
    live = compute_precomputed_factors(moneyflow, daily)

    pd.testing.assert_frame_equal(qe, live[qe.columns], check_dtype=False)


def test_moneyflow_parity_rejects_legacy_static_units() -> None:
    canonical = _canonical_moneyflow()
    legacy = canonical.copy()
    legacy.loc[:, list(MONEYFLOW_FACTOR_VOLUME_COLUMNS)] /= 100.0
    legacy.loc[:, list(MONEYFLOW_FACTOR_AMOUNT_COLUMNS)] /= 10_000.0

    with pytest.raises(ValueError, match="unit parity failed"):
        assert_moneyflow_frame_parity(canonical, legacy)


def test_moneyflow_ratios_require_adjustment_factor() -> None:
    with pytest.raises(ValueError, match="amount, volume and factor"):
        derive_moneyflow_factors(_canonical_moneyflow(), _daily().drop(columns="factor"))
