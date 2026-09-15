from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.dataset_release import adj_factor_candidate_repair as repair


def _write_bin(path: Path, start: int, values: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    np.asarray([float(start), *values], dtype="<f4").tofile(path)


def _make_feature(component: Path, symbol: str, suffix: str, factor: list[float]) -> None:
    feature = component / "features" / symbol.lower()
    for field, values in {
        "open": [10.0 * value for value in factor],
        "high": [11.0 * value for value in factor],
        "low": [9.0 * value for value in factor],
        "close": [10.5 * value for value in factor],
        "volume": [1000.0 / value for value in factor],
        "factor": factor,
    }.items():
        _write_bin(feature / f"{field}.{suffix}.bin", 0, values)


def test_mismatch_summary_counts_minute_cells_without_expanding_dates(tmp_path: Path) -> None:
    path = tmp_path / "factor.1min.bin"
    _write_bin(path, 0, [0.5, 0.5, np.nan, 0.75, 0.75])
    calendar = (
        "2026-08-28 09:31:00",
        "2026-08-28 09:32:00",
        "2026-08-28 09:33:00",
        "2026-08-31 09:31:00",
        "2026-08-31 09:32:00",
    )

    dates, cells = repair._mismatch_summary(
        path,
        calendar,
        {"2026-08-28": np.float32(0.6), "2026-08-31": np.float32(0.75)},
    )

    assert dates == ("2026-08-28",)
    assert cells == 2


def test_patch_one_frequency_preserves_raw_values_and_detaches_outputs(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline"
    candidate = tmp_path / "candidate"
    symbol = "000001.SZ"
    factor = [0.5, 0.5, 1.0, 1.0]
    _make_feature(baseline, symbol, "1min", factor)
    calendar = (
        "2026-08-28 09:31:00",
        "2026-08-28 09:32:00",
        "2026-08-31 09:31:00",
        "2026-08-31 09:32:00",
    )

    receipt = repair._patch_one_frequency(
        baseline_component=baseline,
        candidate_component=candidate,
        symbol=symbol,
        frequency="1min",
        calendar=calendar,
        expected={"2026-08-28": np.float32(0.75), "2026-08-31": np.float32(1.0)},
        raw_rows={
            value: (10000.0, 11000.0, 9000.0, 10500.0, 10.0)
            for value in calendar
        },
    )

    assert receipt["changed_factor_cells"] == 2
    before = repair._read_bin(baseline / "features" / symbol.lower() / "close.1min.bin")
    after = repair._read_bin(candidate / "features" / symbol.lower() / "close.1min.bin")
    old_factor = repair._read_bin(baseline / "features" / symbol.lower() / "factor.1min.bin")
    new_factor = repair._read_bin(candidate / "features" / symbol.lower() / "factor.1min.bin")
    np.testing.assert_array_equal(
        np.rint(before[1:] / old_factor[1:] * 1000.0),
        np.rint(after[1:] / new_factor[1:] * 1000.0),
    )
    assert not os.path.samefile(
        baseline / "features" / symbol.lower() / "close.1min.bin",
        candidate / "features" / symbol.lower() / "close.1min.bin",
    )


def test_copy_on_write_clone_links_unchanged_and_omits_mutable(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline"
    candidate = tmp_path / "candidate"
    unchanged = baseline / "components" / "index_context" / "index_daily.h5"
    unchanged.parent.mkdir(parents=True)
    unchanged.write_bytes(b"index")
    mutable = baseline / "components" / "daily_bin_candidate" / "features" / "000001.sz" / "factor.day.bin"
    mutable.parent.mkdir(parents=True)
    mutable.write_bytes(b"factor")
    inventory_unsigned = {
        "schema_version": repair.INVENTORY_SCHEMA,
        "affected_symbol_count": 1,
        "records": [{"symbol": "000001.SZ", "minute_mismatch_cell_count": 0}],
    }
    inventory = {
        **inventory_unsigned,
        "canonical_sha256": repair.sha256_bytes(repair.canonical_json_bytes(inventory_unsigned)),
    }

    receipt = repair.clone_baseline_copy_on_write(
        baseline_root=baseline,
        candidate_root=candidate,
        inventory=inventory,
    )

    assert receipt["linked_file_count"] == 1
    assert os.path.samefile(unchanged, candidate / unchanged.relative_to(baseline))
    assert not (candidate / mutable.relative_to(baseline)).exists()


def test_rebuild_daily_h5_updates_only_affected_rows(tmp_path: Path) -> None:
    baseline = tmp_path / "daily_pv.h5"
    candidate = tmp_path / "candidate.h5"
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2026-08-28"), "000001.SZ"),
            (pd.Timestamp("2026-08-31"), "000001.SZ"),
            (pd.Timestamp("2026-08-31"), "000002.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame(
        {
            "open": [5.0, 10.0, 20.0],
            "high": [5.5, 11.0, 22.0],
            "low": [4.5, 9.0, 18.0],
            "close": [5.25, 10.5, 21.0],
            "volume": [2000.0, 1000.0, 500.0],
            "factor": [0.5, 1.0, 1.0],
        },
        index=index,
    )
    frame.to_hdf(baseline, key="data", format="table", data_columns=["datetime", "instrument"])
    baseline_component = tmp_path / "baseline_component"
    corrected_component = tmp_path / "corrected_component"
    (corrected_component / "calendars").mkdir(parents=True)
    (corrected_component / "calendars" / "day.txt").write_text(
        "2026-08-28\n2026-08-31\n", encoding="utf-8"
    )
    _make_feature(baseline_component, "000001.SZ", "day", [0.5, 1.0])
    _make_feature(corrected_component, "000001.SZ", "day", [0.75, 1.0])

    receipt = repair.rebuild_daily_pv_h5(
        baseline_path=baseline,
        candidate_path=candidate,
        baseline_daily_component=baseline_component,
        corrected_daily_component=corrected_component,
        affected_symbols=["000001.SZ"],
        chunksize=2,
    )

    rebuilt = pd.read_hdf(candidate, key="data")
    assert receipt["changed_rows"] == 1
    assert rebuilt.loc[index[0], "factor"] == 0.75
    assert rebuilt.loc[index[2], "close"] == frame.loc[index[2], "close"]
    assert round(rebuilt.loc[index[0], "close"] / rebuilt.loc[index[0], "factor"], 3) == round(
        frame.loc[index[0], "close"] / frame.loc[index[0], "factor"], 3
    )


def test_rebuild_static_factors_preserves_parquet_multiindex(tmp_path: Path) -> None:
    dates = pd.date_range("2026-08-01", periods=11, freq="D")
    index = pd.MultiIndex.from_product(
        [dates, ["000001.SZ", "000002.SZ"]], names=["datetime", "instrument"]
    )
    baseline = tmp_path / "static.parquet"
    candidate = tmp_path / "candidate.parquet"
    daily_h5 = tmp_path / "daily.h5"
    static = pd.DataFrame(
        {"other": np.arange(len(index), dtype="float32"), "PriceStrength_10D": np.zeros(len(index), dtype="float32")},
        index=index,
    )
    static.to_parquet(baseline)
    close = pd.DataFrame(
        {"close": np.arange(1, len(index) + 1, dtype="float64")},
        index=index,
    )
    close.to_hdf(daily_h5, key="data", format="table", data_columns=["datetime", "instrument"])

    receipt = repair.rebuild_static_factors(
        baseline_path=baseline,
        candidate_path=candidate,
        corrected_daily_h5=daily_h5,
        affected_symbols={"000001.SZ"},
    )

    rebuilt = pd.read_parquet(candidate)
    assert rebuilt.index.names == ["datetime", "instrument"]
    assert receipt["changed_rows"] == 1
    assert receipt["source_daily_h5_sha256"] == repair.sha256_file(daily_h5)
    assert rebuilt.loc[(dates[-1], "000001.SZ"), "PriceStrength_10D"] != 0.0
    assert rebuilt.loc[(dates[-1], "000002.SZ"), "PriceStrength_10D"] == 0.0


def test_build_dataset_manifest_rehashes_replaced_and_added_components(tmp_path: Path) -> None:
    (tmp_path / "stable.bin").write_bytes(b"stable")
    (tmp_path / "replacement.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "validation.json").write_text('{"status":"PASS"}\n', encoding="utf-8")
    baseline = {
        "schema_version": "qe_dataset_manifest_v1",
        "release_id": "qe_hmm_full_v2_20260831",
        "revision": "old",
        "cutoff_trade_date": "2026-08-31",
        "qlib_calendar_sha256": "a" * 64,
        "qlib_instruments_sha256": "b" * 64,
        "st_pit_manifest_sha256": "c" * 64,
        "components": {
            "stable": {
                "path": "stable.bin",
                "sha256": repair.sha256_file(tmp_path / "stable.bin"),
                "size": 6,
            },
            "replace_me": {"path": "missing-old.json", "sha256": "d" * 64, "size": 1},
        },
    }

    manifest = repair.build_dataset_manifest(
        candidate_root=tmp_path,
        baseline_manifest=baseline,
        revision="20260915-r5",
        component_replacements={"replace_me": "replacement.json"},
        component_additions={"validation": "validation.json"},
        source_contract={"adj_factor": "reconciled production whole-series snapshot", "no_fabrication": True},
    )

    unsigned = {key: value for key, value in manifest.items() if key != "dataset_manifest_sha256"}
    assert manifest["dataset_manifest_sha256"] == repair.sha256_bytes(repair.canonical_json_bytes(unsigned))
    assert manifest["deployment_snapshot_id"].endswith(manifest["deployment_content_sha256"][:16])
    assert manifest["components"]["replace_me"]["sha256"] == repair.sha256_file(
        tmp_path / "replacement.json"
    )
    assert manifest["availability_status"] == "LOCAL_CANDIDATE_VALIDATED_PENDING_NODE_SYNC"
