"""The eligibility input is a side asset: it must retain every frozen score key."""
import numpy as np
import pandas as pd
import pytest

from scripts.qe_build_frozen_trend_entry import build_panel
from scripts.qe_trend_entry_admission import TrendEntryContractError, TrendEntryInputs, panel_index_sha


def source_data():
    dates = pd.to_datetime(["2026-09-28", "2026-09-29", "2026-09-30", "2026-10-08"])
    r20 = pd.DataFrame([[.1, -.2], [np.nan, 0]], index=dates[:2],
                       columns=["000001.SZ", "000002.SZ"], dtype=np.float32)
    r60 = pd.DataFrame([[.3, .4], [.3, .4]], index=dates[:2], columns=r20.columns, dtype=np.float32)
    links = pd.DataFrame([{"signal_date": "2026-09-29", "stock": "000001.SZ", "endpoint_date": "2026-09-01"}])
    resolved = pd.DataFrame([{"stock": "000001.SZ", "endpoint_date": "2026-09-01", "declared_suspension": True,
                              "finite_physical_value": np.nan,
                              "resolved_reason": "BOUND_PHYSICAL_NA_WITH_DECLARED_SUSPENSION"}])
    index = pd.MultiIndex.from_product([dates[:2], ["SZ000002", "SZ000001"]], names=["datetime", "instrument"])
    return index, r20, r60, links, resolved, dates


def test_all_keys_retained_zero_negative_na_not_filtered(tmp_path):
    inputs = source_data()
    receipt = build_panel(*inputs, tmp_path / "panel.npz")
    assert receipt["rows"] == 4 and receipt["legal_na_rows"] == 1 and receipt["available_rows"] == 3
    with np.load(tmp_path / "panel.npz", allow_pickle=False) as z:
        assert z["return20"].shape == (4,)
        np.testing.assert_array_equal(z["return20"], np.array([.1, -.2, np.nan, 0.], dtype=np.float32))
        assert z["execution_dates"].tolist() == ["2026-09-29", "2026-09-30"]
        assert receipt["index_sha256"] == panel_index_sha(z["dates"], z["codes"], z["offsets"], z["stock_indices"])


def test_different_prediction_order_same_panel_and_no_value_modification(tmp_path):
    inputs = list(source_data())
    one = build_panel(*inputs, tmp_path / "one.npz")
    inputs[0] = inputs[0][::-1]
    two = build_panel(*inputs, tmp_path / "two.npz")
    assert one["index_sha256"] == two["index_sha256"]
    with np.load(tmp_path / "one.npz") as a, np.load(tmp_path / "two.npz") as b:
        for field in a.files:
            np.testing.assert_array_equal(a[field], b[field])


def test_unproven_new_panel_na_fails_before_output(tmp_path):
    inputs = list(source_data())
    inputs[3] = inputs[3].iloc[:0]
    with pytest.raises(TrendEntryContractError, match="UNPROVEN_NA"):
        build_panel(*inputs, tmp_path / "absent.npz")
    assert not (tmp_path / "absent.npz").exists()


@pytest.mark.parametrize("defect", ["inf", "duplicate", "missing_source", "finite_na_evidence"])
def test_broken_input_cannot_become_cash(tmp_path, defect):
    inputs = list(source_data())
    if defect == "inf":
        inputs[1].iloc[0, 0] = np.inf
    elif defect == "duplicate":
        inputs[0] = inputs[0].append(inputs[0][:1])
    elif defect == "missing_source":
        inputs[1] = inputs[1].drop(columns="000002.SZ")
    else:
        inputs[4].loc[0, "finite_physical_value"] = 12.
    with pytest.raises(TrendEntryContractError):
        build_panel(*inputs, tmp_path / "absent.npz")
    assert not (tmp_path / "absent.npz").exists()


def test_actual_raw_scores_must_match_complete_reference_before_st_filters(tmp_path):
    inputs = source_data()
    build_panel(*inputs, tmp_path / "panel.npz")
    obj = TrendEntryInputs.__new__(TrendEntryInputs)
    with np.load(tmp_path / "panel.npz") as z:
        obj.arrays = {k: z[k] for k in z.files}
    obj._dates = {str(d): i for i, d in enumerate(obj.arrays["dates"])}
    obj._cache_day, obj._cache = None, {}
    obj.manifest = {"account": {"start": "2026-09-28"}}
    predictions = pd.Series([.1, .2], index=["SZ000002", "SZ000001"])
    obj.check_prediction(predictions, "2026-09-28", "2026-09-29")
    for bad in (predictions.iloc[:1], pd.Series([1, 2], index=["SZ000001", "000001.SZ"])):
        with pytest.raises(TrendEntryContractError, match="COMMON_PREDICTION_KEYS"):
            obj.check_prediction(bad, "2026-09-28", "2026-09-29")
    assert obj.evaluate("SZ000001", "2026-09-29")["reason"] == "BOUND_PHYSICAL_NA_WITH_DECLARED_SUSPENSION"
    assert not obj.evaluate("SZ000002", "2026-09-29")["allowed"]
