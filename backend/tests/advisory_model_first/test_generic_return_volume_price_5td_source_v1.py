"""New information, D boundary and normal missingness, one shared small path."""
import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.generic_return_volume_price_5td_contracts_v1 import LAG_FEATURES
from backend.services.advisory_model_first.generic_return_volume_price_5td_source_v1 import build_return_volume_lag_features_v1


@pytest.fixture
def source():
    days = pd.bdate_range("2024-01-02", periods=22)
    close = np.array([100,101,100,101,102,101,100,101,102,101,102,101,100,101,100,101,100,101,102,101], dtype=float)
    prices = pd.DataFrame(dict(trade_date=days[:20], instrument="000001.SZ", raw_close_cny=close, adj_factor=1.))
    volumes = prices.loc[:, ["trade_date", "instrument"]].assign(volume_hand=1.)
    volumes.loc[1, "volume_hand"] = 5.
    roster = pd.DataFrame([dict(decision_as_of_trade_date=days[19], target_trade_date=days[20], instrument="000001.SZ")])
    return dict(roster=roster, calendar=days, prices=prices, volumes=volumes)


def test_lag_identifies_information_with_identical_old_volume_summaries_and_scale(source):
    original, receipt = build_return_volume_lag_features_v1(**source)
    q, c = source["volumes"].volume_hand.to_numpy(), source["prices"].raw_close_cny.to_numpy()
    r = np.diff(np.log(c))
    def old(v):
        return [float(c@v/v.sum()), float(np.sign(r)@v[1:]/v[1:].sum()), float(np.sum((v/v.sum())**2)), float(v[-5:].mean()/v.mean())]
    alternate = source["volumes"].copy()
    alternate.loc[[1,3], "volume_hand"] = [1.,5.]
    second, _ = build_return_volume_lag_features_v1(**{**source, "volumes": alternate})
    assert old(q) == old(alternate.volume_hand.to_numpy())
    assert original.iloc[0].lag_volume_next_return_cov18 == pytest.approx(-.0018091510642123411)
    assert second.iloc[0].lag_volume_next_return_cov18 == pytest.approx(.001791326626002)
    scaled, _ = build_return_volume_lag_features_v1(**{**source, "volumes": source["volumes"].assign(volume_hand=q*1e200)})
    np.testing.assert_allclose(scaled.loc[:, LAG_FEATURES], original.loc[:, LAG_FEATURES], atol=1e-12)
    assert receipt["future_feature_values_parsed"] == 0


def test_future_poison_missing_zero_and_no_downside_preserve_original_key(source):
    extra = pd.DataFrame([dict(trade_date=source["calendar"][20], instrument="000001.SZ", raw_close_cny=True, adj_factor=True)])
    base, _ = build_return_volume_lag_features_v1(**source)
    actual, _ = build_return_volume_lag_features_v1(**{**source, "prices": pd.concat([source["prices"], extra], ignore_index=True)})
    pd.testing.assert_frame_equal(base, actual)
    for volumes in (source["volumes"].drop(index=2), source["volumes"].assign(volume_hand=None), source["volumes"].assign(volume_hand=0.)):
        value, _ = build_return_volume_lag_features_v1(**{**source, "volumes": volumes})
        assert len(value) == 1 and value.loc[:, LAG_FEATURES].isna().all().all()
    flat, _ = build_return_volume_lag_features_v1(**{**source, "prices": source["prices"].assign(raw_close_cny=100.)})
    assert list(flat.loc[0, list(LAG_FEATURES[:2])]) == [0.,0.]
    assert pd.isna(flat.loc[0, LAG_FEATURES[2]]) and "NO_OBSERVED_DOWNSIDE" in flat.return_volume_reason.iloc[0]
    final_only = source["volumes"].assign(volume_hand=0.)
    final_only.loc[19, "volume_hand"] = 1.
    result, _ = build_return_volume_lag_features_v1(**{**source, "volumes": final_only})
    assert pd.isna(result.loc[0, LAG_FEATURES[0]]) and pd.notna(result.loc[0, LAG_FEATURES[1]])


def test_requested_contradictions_and_extreme_weights_are_not_silently_filled(source):
    bad = source["prices"].copy()
    bad.loc[1, "raw_close_cny"], bad.loc[2, "adj_factor"] = None, -1.
    with pytest.raises(AdvisoryModelFirstError):
        build_return_volume_lag_features_v1(**{**source, "prices": bad})
    with pytest.raises(ValueError, match="duplicate"):
        build_return_volume_lag_features_v1(**{**source, "volumes": pd.concat([source["volumes"], source["volumes"].iloc[:1]])})
    extreme = source["volumes"].copy()
    extreme.loc[1, "volume_hand"], extreme.loc[2, "volume_hand"] = 1e308, 1e-308
    result, receipt = build_return_volume_lag_features_v1(**{**source, "volumes": extreme})
    assert receipt["quantity_encoding_underflow_count"] > 0 and np.isfinite(result.loc[0, LAG_FEATURES[0]])
    altered = source["roster"].assign(target_trade_date=source["calendar"][21])
    with pytest.raises(ValueError, match="next session"):
        build_return_volume_lag_features_v1(**{**source, "roster": altered})
