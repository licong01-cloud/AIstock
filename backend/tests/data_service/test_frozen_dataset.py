"""Real temporary H5/parquet/float32 files, with no market database."""
from datetime import date
import hashlib
import json

import numpy as np
import pandas as pd
import pytest

from backend.data_service.frozen_dataset import FrozenDataset, FrozenDatasetError, H5_FILES, freeze_dataset_binding


DAYS = [date(2026, 1, d) for d in (19, 20, 21)]
SYMBOL = "000001.SZ"
HALT = "603616.SH"


def make_dataset(root):
    day, minute, factors = root / "day", root / "minute", root / "factors"
    for directory in (day / "calendars", day / "instruments", minute / "calendars", factors):
        directory.mkdir(parents=True, exist_ok=True)
    (day / "calendars/day.txt").write_text("\n".join(map(str, DAYS)) + "\n")
    (day / "meta_export.json").write_text(json.dumps({"universe_key": "TEST_PIT", "rule_version": "test-v1"}))
    (day / "instruments/stock_universe.txt").write_text(
        f"{SYMBOL}\t2026-01-19\t2026-01-21\n{HALT}\t2026-01-19\t2026-01-20\n")
    times = pd.DatetimeIndex([pd.Timestamp(d) + pd.Timedelta(hours=9, minutes=m) for d in DAYS for m in (31, 32)])
    (minute / "calendars/minute.txt").write_text("\n".join(map(str, times)) + "\n")
    (minute / "meta_export.json").write_text("{}")
    (minute / "provider_catalog.json").write_text("{}")
    (factors / "meta.json").write_text(json.dumps({"schema_version": "qe_direct_factor_h5_static_v2", "start": str(DAYS[0]), "end": str(DAYS[-1])}))
    index = pd.MultiIndex.from_product([pd.to_datetime(DAYS), [SYMBOL, HALT]], names=["datetime", "instrument"])
    # Future corporate action poisons any accidental cross-cutoff factor reuse.
    frame = pd.DataFrame({"open": [5., 5., 5.5, np.nan, 999., 999.],
                          "high": [5.5, 5.5, 6., np.nan, 1000., 1000.],
                          "low": [4.5, 4.5, 5., np.nan, 998., 998.],
                          "close": [5.25, 5.25, 5.75, np.nan, 999., 999.],
                          "volume": 2000., "amount": 10000., "factor": [.5, .5, .5, .5, 1., 1.]}, index=index)
    for name in H5_FILES:
        from backend.data_service.moneyflow_contract import MONEYFLOW_FACTOR_COLUMNS
        static = pd.DataFrame({column: 1. for column in MONEYFLOW_FACTOR_COLUMNS}, index=index) if name == "moneyflow" else pd.DataFrame({"value": range(6)}, index=index)
        (frame if name == "daily_pv" else static).to_hdf(
            factors / (name + ".h5"), key="data", format="table", data_columns=True)
    for symbol in (SYMBOL, HALT):
        target = day / "features" / symbol.lower()
        target.mkdir(parents=True)
        fields = {"close": [5.25, 5.75, 999.], "factor": [.5, .5, 1.],
                  "prev_close": [10., 10.5, 11.5], "up_limit_price": [11., 11.55, 12.65],
                  "down_limit_price": [9., 9.45, 10.35]}
        if symbol == HALT:
            fields["close"][1] = np.nan
            for field in ("prev_close", "up_limit_price", "down_limit_price"):
                fields[field][1] = np.nan
        for name, values in fields.items():
            np.array([0, *values], dtype="<f4").tofile(target / (name + ".day.bin"))
        target = minute / "features" / symbol.lower()
        target.mkdir(parents=True)
        for name, value in {"open": 5., "high": 5.5, "low": 4.75, "close": 5.25,
                            "volume": 2000., "amount": 10000., "factor": .5}.items():
            values = [value] * 6
            if symbol == HALT:
                values[2:4] = [np.nan, np.nan]
            np.array([0, *values], dtype="<f4").tofile(target / (name + ".1min.bin"))
    pd.DataFrame({"ts_code": [HALT], "trade_date": pd.to_datetime([DAYS[1]]),
                  "suspend_type": ["S"], "suspend_timing": [None]}).to_parquet(root / "suspend.parquet")
    pd.DataFrame({"instrument": [SYMBOL, HALT], "start_date": [str(DAYS[0])] * 2,
                  "end_date": [str(DAYS[-1])] * 2, "l2_code_id": [1, 1]}).to_parquet(root / "sector.parquet")
    (root / "sector.json").write_text(json.dumps({"entries": [{"l2_code_id": 1, "canonical_l2_code": "801011.SI"}]}))
    paths = {"day_calendar": "day/calendars/day.txt", "day_meta_export": "day/meta_export.json",
             "day_selection_universe": "day/instruments/stock_universe.txt", "factor_meta": "factors/meta.json",
             "minute_calendar": "minute/calendars/minute.txt", "minute_meta_export": "minute/meta_export.json",
             "minute_provider_catalog": "minute/provider_catalog.json", "suspend_data": "suspend.parquet",
             "sector_membership_spans": "sector.parquet", "sector_code_map": "sector.json"}
    components = {key: {"path": value, "sha256": hashlib.sha256((root / value).read_bytes()).hexdigest()}
                  for key, value in paths.items()}
    manifest = root / "qe_dataset_manifest.json"
    manifest.write_text(json.dumps({"release_id": "test-release", "components": components}))
    profile = root / "active.json"
    profile.write_text(json.dumps({"schema_version": "aistock_active_dataset_profile_v3", "release_id": "test-release",
        "generation": "test-1", "controller_paths": {"candidate_root": str(root)},
        "components": {"dataset_manifest_file_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest()}}))
    return profile


def test_frozen_files_are_bounded_by_members_day_and_identity(tmp_path):
    binding = freeze_dataset_binding(make_dataset(tmp_path))
    dataset = FrozenDataset(binding, consumer_scope="LOCALSIM", run_id="run-1")
    frame = dataset.h5("daily_pv", [SYMBOL], DAYS[0], DAYS[1], ["close"])
    assert frame.close.tolist() == [5.25, 5.75]
    assert dataset.members(DAYS[2], universe_key="TEST_PIT") == [SYMBOL]
    with pytest.raises(FrozenDatasetError):
        dataset.members(DAYS[1], universe_key="different")
    with pytest.raises(FrozenDatasetError):
        dataset.h5("daily_pv", [SYMBOL], DAYS[0], date(2026, 1, 22), ["close"])
    (tmp_path / "factors/daily_pv.h5").touch()
    with pytest.raises(FrozenDatasetError, match="changed"):
        dataset.assert_unchanged()


def test_raw_minute_units_and_empty_day_are_not_fabricated(tmp_path):
    dataset = FrozenDataset(freeze_dataset_binding(make_dataset(tmp_path)), consumer_scope="LOCALSIM", run_id="minute")
    rows = dataset.raw_minute_rows(SYMBOL, DAYS[1])
    assert len(rows) == 2
    assert rows[0]["close"] == 10.5 and rows[0]["volume"] == 10. and rows[0]["amount"] == 10000.
    with pytest.raises(FrozenDatasetError) as failure:
        dataset.raw_minute_rows(HALT, DAYS[1])
    assert failure.value.context["reason_code"] == "FROZEN_DATASET_MINUTE_MISSING"


def test_rebinding_locator_and_mutating_manifest_cannot_replace_content(tmp_path):
    binding = freeze_dataset_binding(make_dataset(tmp_path))
    binding["profile"]["generation"] = "different"
    with pytest.raises(FrozenDatasetError, match="digest"):
        FrozenDataset(binding, consumer_scope="LOCALSIM", run_id="retry")
