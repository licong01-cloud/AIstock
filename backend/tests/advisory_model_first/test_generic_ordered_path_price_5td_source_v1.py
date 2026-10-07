"""Fixed clocks/normal missing/native D-only read in one minimal source matrix."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_ordered_path_price_5td_contracts_v1 import FIELDS, ORDERED_FEATURES
from backend.services.advisory_model_first.generic_ordered_path_price_5td_source_v1 import (
    aggregate_ordered_path_v1, expected_clock_v1, read_d_ordered_path_features_v1,
)
from backend.tests.advisory_model_first.test_generic_minute_price_5td_source_v1 import minute_provider as minute_provider


def test_fixed_clock_hand_values_lunch_scale_missing_and_zero_volume():
    slots = expected_clock_v1("2024-01-02")
    close = np.linspace(10., 11., len(slots))
    arrays = dict(open=close, high=close+.01, low=close-.01, close=close, volume=np.ones(len(slots)))
    original = aggregate_ordered_path_v1(slots=slots, arrays=arrays)
    assert original[ORDERED_FEATURES[0]] == pytest.approx(10000*(close[15]/close[0]-1))
    assert original[ORDERED_FEATURES[16]] == pytest.approx(10000*(close[135]/close[121]-1))
    assert original[ORDERED_FEATURES[1]] == pytest.approx(16/241)
    assert original[ORDERED_FEATURES[17]] == pytest.approx(15/241)
    scaled = {name: a*(100 if name == "volume" else 7) for name, a in arrays.items()}
    actual = aggregate_ordered_path_v1(slots=slots, arrays=scaled)
    np.testing.assert_allclose([actual[k] for k in ORDERED_FEATURES], [original[k] for k in ORDERED_FEATURES])
    # Missing an internal slot does not shift the endpoint or lunch; full volume denominator becomes unknown.
    mask = np.arange(len(slots)) != 5
    partial = aggregate_ordered_path_v1(slots=slots[mask], arrays={k: a[mask] for k, a in arrays.items()})
    assert partial[ORDERED_FEATURES[0]] == original[ORDERED_FEATURES[0]]
    assert all(np.isnan(partial[k]) for k in ORDERED_FEATURES[1::2])
    mask = np.arange(len(slots)) != 0
    missing_endpoint = aggregate_ordered_path_v1(slots=slots[mask], arrays={k: a[mask] for k, a in arrays.items()})
    assert np.isnan(missing_endpoint[ORDERED_FEATURES[0]])
    assert sum(missing_endpoint[k] for k in ORDERED_FEATURES[1::2]) == pytest.approx(1.)
    alternate = pd.DataFrame(arrays, index=slots).iloc[1:].copy()
    alternate.loc[pd.Timestamp("2024-01-02 13:00")] = [close[121], close[121]+.01, close[121]-.01, close[121], 1.]
    alternate = alternate.sort_index()
    measured = aggregate_ordered_path_v1(slots=alternate.index, arrays={k: alternate[k].to_numpy() for k in FIELDS})
    assert np.isnan(measured[ORDERED_FEATURES[0]]) and measured[ORDERED_FEATURES[16]] == original[ORDERED_FEATURES[16]]
    assert sum(measured[k] for k in ORDERED_FEATURES[1::2]) == pytest.approx(1.)
    assert not measured["ordered_0930_present"] and measured["ordered_1300_present"]
    arrays["volume"][:] = 0.
    zero = aggregate_ordered_path_v1(slots=slots, arrays=arrays)
    assert all(np.isnan(zero[k]) for k in ORDERED_FEATURES[1::2])
    bad = deepcopy(arrays)
    bad["high"][0] = 1.
    with pytest.raises(ValueError, match="OHLC"):
        aggregate_ordered_path_v1(slots=slots, arrays=bad)
    bad["open"][0] = bad["close"][0] = np.nan
    with pytest.raises(ValueError, match="high/low"):
        aggregate_ordered_path_v1(slots=slots, arrays=bad)


def test_native_original_roster_future_poison_missing_file_identity_and_stamp(minute_provider, monkeypatch):
    _, profile, identity, roster = minute_provider()
    keys = pd.concat([roster, roster.assign(instrument="000002.SZ")], ignore_index=True)
    rows, receipt = read_d_ordered_path_features_v1(roster=keys, identity=identity, active_profile_path=profile)
    assert rows.instrument.tolist() == keys.instrument.tolist() and receipt["decoded_bytes"] == 5*len(FIELDS)*4
    assert rows.loc[1, list(ORDERED_FEATURES)].isna().all() and receipt["future_price_bars_decoded"] == 0
    import backend.services.advisory_model_first.generic_ordered_path_price_5td_source_v1 as module
    original, counts = module._stamp, {}
    def changing(path):
        size, stamp = original(path)
        counts[path] = counts.get(path, 0)+1
        return size, stamp+int(counts[path] > 1)
    monkeypatch.setattr(module, "_stamp", changing)
    with pytest.raises(ValueError, match="changed"):
        read_d_ordered_path_features_v1(roster=roster, identity=identity, active_profile_path=profile)
    monkeypatch.setattr(module, "_stamp", original)
    profile.write_text(profile.read_text().replace("unit-current", "changed-current"))
    with pytest.raises(ValueError, match="identity"):
        read_d_ordered_path_features_v1(roster=roster, identity=identity, active_profile_path=profile)
