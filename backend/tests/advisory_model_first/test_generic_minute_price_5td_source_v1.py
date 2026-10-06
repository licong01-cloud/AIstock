"""Measured D paths, original slots, current identity and no interpreted future values."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_pipeline import file_sha256
from backend.services.advisory_model_first.generic_minute_price_5td_contracts_v1 import FIELDS, KEY, MINUTE_FEATURES, PINS
from backend.services.advisory_model_first.generic_minute_price_5td_source_v1 import (
    _inside, aggregate_d_minute_features_v1, read_d_minute_features_v1,
)


def bars(day="2024-01-02"):
    slots = pd.DatetimeIndex([day+" "+t for t in ("09:30", "09:31", "11:30", "14:30", "14:31")])
    close = np.array([10., 11., 12., 20., 22.])
    arrays = dict(open=close-.1, high=close+.1, low=close-.2, close=close,
                  volume=np.ones(5)*100, amount=close*100, limit_up=np.zeros(5), limit_down=np.zeros(5))
    return slots, arrays


@pytest.fixture
def minute_provider(tmp_path):
    def factory(day="2024-01-02"):
        root = tmp_path/"dataset"/"components"/"minute_bin_candidate"
        for child in ("calendars", "instruments", "features/000001.sz"):
            (root/child).mkdir(parents=True, exist_ok=True)
        slots, arrays = bars(day)
        future = slots[0]+pd.Timedelta(days=1)
        (root/"calendars/1min.txt").write_text("\n".join(str(v) for v in [*slots, future])+"\n")
        (root/"instruments/all.txt").write_text("000001.SZ\n")
        (root/"meta_export.json").write_text(json.dumps(dict(freq_types=["1min"], start=day, end=str(future.date()))))
        for field in FIELDS:
            # Infinity on T proves that the provider does not decode the later price.
            (root/"features/000001.sz"/(field+".1min.bin")).write_bytes(
                np.r_[0., arrays[field], np.inf].astype("<f4").tobytes())
        identity = dict(generation="unit-current", minute_root=str(root),
                        pins={name: file_sha256(root/relative) for name, relative in PINS.items()})
        profile = tmp_path/"active.json"
        profile.write_text(json.dumps(dict(generation="unit-current", controller_paths=dict(candidate_root=str(root.parent.parent)),
                                           components=dict(minute_pins=identity["pins"]))))
        roster = pd.DataFrame([dict(zip(KEY, (slots[0].normalize(), future.normalize(), "000001.SZ"), strict=True))])
        return root, profile, identity, roster
    return factory


def test_hand_minute_values_lunch_and_activity_coordinate():
    slots, arrays = bars()
    result = aggregate_d_minute_features_v1(slots=slots, arrays=arrays)
    assert result["opening_30m_return_bps"] == pytest.approx(10000*(11/9.9-1))
    assert result["closing_30m_return_bps"] == pytest.approx(10000*(22/19.9-1))
    assert result["realized_volatility_bps"] == pytest.approx(10000*np.sqrt(2)*np.log(1.1))
    assert result["directional_efficiency"] == pytest.approx(1)
    assert result["close_to_vwap_bps"] == pytest.approx(10000*(22/15-1))
    assert result["opening_30m_amount_share"] == pytest.approx(21/75)
    assert result["closing_30m_amount_share"] == pytest.approx(42/75)
    mismatch = deepcopy(arrays)
    mismatch["amount"] *= 100
    unknown = aggregate_d_minute_features_v1(slots=slots, arrays=mismatch)
    assert np.isnan(unknown["close_to_vwap_bps"]) and "UNKNOWN_ACTIVITY_COORDINATE" in unknown["minute_reason"]
    assert unknown["opening_30m_return_bps"] == result["opening_30m_return_bps"]


def test_original_denominator_endpoints_missing_activity_and_normal_halt():
    slots, arrays = bars()
    absent = deepcopy(arrays)
    for field in FIELDS[:4]:
        absent[field][0] = np.nan
    result = aggregate_d_minute_features_v1(slots=slots, arrays=absent)
    assert result["minute_coverage_fraction"] == .8 and result["minute_calendar_slots"] == 5
    assert np.isnan(result["opening_30m_return_bps"])  # Do not shorten the original window.
    assert np.isfinite(result["closing_30m_return_bps"])
    absent["amount"][:] = np.nan
    result = aggregate_d_minute_features_v1(slots=slots, arrays=absent)
    assert np.isnan(result["close_to_vwap_bps"]) and np.isnan(result["opening_30m_amount_share"])
    halted = {field: np.full(5, np.nan) for field in FIELDS}
    result = aggregate_d_minute_features_v1(slots=slots, arrays=halted)
    assert all(np.isnan(result[name]) for name in MINUTE_FEATURES) and result["minute_calendar_slots"] == 5


@pytest.mark.parametrize("field,value", [("close", np.inf), ("close", 0.), ("volume", -1.), ("limit_up", 2.)])
def test_bad_known_bars_not_silent_missing(field, value):
    slots, arrays = bars()
    arrays[field][0] = value
    with pytest.raises(ValueError):
        aggregate_d_minute_features_v1(slots=slots, arrays=arrays)


def test_reader_only_D_bytes_keeps_missing_stocks_and_pin_errors(minute_provider):
    root, profile, identity, roster = minute_provider()
    missing = roster.assign(instrument="000002.SZ")
    rows, receipt = read_d_minute_features_v1(roster=pd.concat([roster, missing], ignore_index=True), identity=identity,
                                            active_profile_path=profile)
    assert list(rows.instrument) == ["000001.SZ", "000002.SZ"]
    assert rows.iloc[0].minute_coverage_fraction == 1 and rows.iloc[1].minute_coverage_fraction == 0
    assert receipt["decoded_bytes"] == 5*8*4 and receipt["future_price_bars_decoded"] == 0
    assert receipt["original_keys"] == 2 and not receipt["dataset_modified"]
    with pytest.raises(ValueError, match="escapes"):
        _inside(root.resolve(), root/".."/"outside")
    (root/"meta_export.json").write_bytes((root/"meta_export.json").read_bytes()+b" ")
    with pytest.raises(ValueError, match="hash"):
        read_d_minute_features_v1(roster=roster, identity=identity, active_profile_path=profile)


@pytest.mark.parametrize("content", [b"abc", np.array([-1., 1.], dtype="<f4").tobytes()])
def test_truncated_or_invalid_header_rejected(minute_provider, content):
    root, profile, identity, roster = minute_provider()
    (root/"features/000001.sz/open.1min.bin").write_bytes(content)
    with pytest.raises(ValueError, match="truncated|header"):
        read_d_minute_features_v1(roster=roster, identity=identity, active_profile_path=profile)


def test_source_change_detected_without_lookahead(minute_provider, monkeypatch):
    _, profile, identity, roster = minute_provider()
    import backend.services.advisory_model_first.generic_minute_price_5td_source_v1 as module
    original, counts = module._stamp, {}
    def changing(path):
        value = original(path)
        counts[path] = counts.get(path, 0)+1
        return (value[0], value[1]+int(counts[path] > 1))
    monkeypatch.setattr(module, "_stamp", changing)
    with pytest.raises(ValueError, match="changed"):
        read_d_minute_features_v1(roster=roster, identity=identity, active_profile_path=profile)
