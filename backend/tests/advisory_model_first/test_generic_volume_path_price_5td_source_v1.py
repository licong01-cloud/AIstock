"""New volume-path definition contracts; reuse one established bounded native provider fixture."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_volume_path_price_5td_contracts_v1 import VOLUME_FEATURES
from backend.services.advisory_model_first.generic_volume_path_price_5td_source_v1 import (
    aggregate_d_volume_path_features_v1, read_d_volume_path_features_v1,
)
from backend.tests.advisory_model_first.test_generic_minute_price_5td_source_v1 import (
    bars, minute_provider as minute_provider,
)


def test_hand_new_features_scale_invariance_and_amount_coordinate_independence():
    slots, arrays = bars()
    result = aggregate_d_volume_path_features_v1(slots=slots, arrays=arrays)
    assert result[VOLUME_FEATURES[0]] == pytest.approx(10000*(22/15-1))
    assert result[VOLUME_FEATURES[1]] == pytest.approx(1.)  # Only 09:31 and 14:31 pairs; no lunch bridge.
    assert result[VOLUME_FEATURES[2]] == pytest.approx(.4)
    scaled = deepcopy(arrays)
    for name in ("open", "high", "low", "close"):
        scaled[name] *= 7
    scaled["volume"] *= 100
    scaled["amount"] *= 13  # Deliberately incompatible amount coordinate.
    changed = aggregate_d_volume_path_features_v1(slots=slots, arrays=scaled)
    for name in VOLUME_FEATURES:
        assert changed[name] == pytest.approx(result[name])
    assert np.isnan(changed["close_to_vwap_bps"])


def test_partial_original_tail_missing_and_zero_volume_not_fabricated():
    slots, arrays = bars()
    missing = deepcopy(arrays)
    missing["volume"][-1] = np.nan
    partial = aggregate_d_volume_path_features_v1(slots=slots, arrays=missing)
    assert partial["volume_path_coverage"] == .8 and partial["volume_path_partial"]
    assert np.isfinite(partial[VOLUME_FEATURES[0]]) and np.isnan(partial[VOLUME_FEATURES[2]])
    missing["volume"][0] = np.nan
    insufficient = aggregate_d_volume_path_features_v1(slots=slots, arrays=missing)
    assert all(np.isnan(insufficient[name]) for name in VOLUME_FEATURES)
    assert np.isfinite(insufficient["closing_30m_return_bps"])
    arrays["volume"][:] = 0.
    zero = aggregate_d_volume_path_features_v1(slots=slots, arrays=arrays)
    assert zero["volume_path_coverage"] == 1.
    assert all(np.isnan(zero[name]) for name in VOLUME_FEATURES)
    for name in ("open", "high", "low", "close"):
        arrays[name][:] = 10.
    arrays["volume"][:] = 1.
    flat = aggregate_d_volume_path_features_v1(slots=slots, arrays=arrays)
    assert flat[VOLUME_FEATURES[0]] == 0 and flat[VOLUME_FEATURES[1]] == 0


@pytest.mark.parametrize("field,value", [("volume", -1.), ("volume", np.inf), ("close", 0.)])
def test_bad_known_volume_inputs_fail(field, value):
    slots, arrays = bars()
    arrays[field][0] = value
    with pytest.raises(ValueError):
        aggregate_d_volume_path_features_v1(slots=slots, arrays=arrays)


def test_native_D_only_future_poison_missing_stock_and_identity(minute_provider):
    _, profile, identity, roster = minute_provider()
    rows, receipt = read_d_volume_path_features_v1(
        roster=pd.concat([roster, roster.assign(instrument="000002.SZ")], ignore_index=True),
        identity=identity, active_profile_path=profile)
    assert rows.iloc[0][list(VOLUME_FEATURES)].notna().all()
    assert rows.iloc[1][list(VOLUME_FEATURES)].isna().all()
    assert list(rows.instrument) == ["000001.SZ", "000002.SZ"]
    assert receipt["decoded_bytes"] == 5*8*4 and receipt["future_price_bars_decoded"] == 0
    profile.write_text(profile.read_text().replace("unit-current", "other-current"))
    with pytest.raises(ValueError, match="identity"):
        read_d_volume_path_features_v1(roster=roster, identity=identity, active_profile_path=profile)


@pytest.mark.parametrize("content", [b"abc", np.array([-1., 1.], dtype="<f4").tobytes()])
def test_bad_header_or_truncated_native_read(minute_provider, content):
    root, profile, identity, roster = minute_provider()
    (root/"features/000001.sz/volume.1min.bin").write_bytes(content)
    with pytest.raises(ValueError, match="truncated|header"):
        read_d_volume_path_features_v1(roster=roster, identity=identity, active_profile_path=profile)


def test_changed_source_is_detected_after_D_only_reads(minute_provider, monkeypatch):
    _, profile, identity, roster = minute_provider()
    import backend.services.advisory_model_first.generic_volume_path_price_5td_source_v1 as module
    original, counts = module._stamp, {}
    def changed(path):
        size, modified = original(path)
        counts[path] = counts.get(path, 0)+1
        return size, modified+int(counts[path] > 1)
    monkeypatch.setattr(module, "_stamp", changed)
    with pytest.raises(ValueError, match="changed"):
        read_d_volume_path_features_v1(roster=roster, identity=identity, active_profile_path=profile)
