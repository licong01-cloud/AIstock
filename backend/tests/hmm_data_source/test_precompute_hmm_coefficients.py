from __future__ import annotations

import hashlib
import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.dataset_release.canonical import digest_named_fields
from scripts.precompute_hmm_coefficients import (
    build_input_data_max_dates_by_date,
    build_stock_sector_membership_spans,
    build_stock_sector_maps_by_date,
    load_frozen_coefficient_inputs,
    main,
    resolve_coefficient_membership_maps,
)


def test_build_stock_sector_maps_by_date_uses_membership_visible_on_each_day() -> None:
    rows = [
        {
            "ts_code": "000001.SZ",
            "l2_code": "OLD.SI",
            "in_date": date(2026, 1, 1),
            "out_date": date(2026, 6, 1),
        },
        {
            "ts_code": "000001.SZ",
            "l2_code": "NEW.SI",
            "in_date": date(2026, 6, 2),
            "out_date": None,
        },
    ]

    result = build_stock_sector_maps_by_date(
        rows,
        [date(2026, 6, 1), date(2026, 6, 2)],
    )

    assert result["2026-06-01"]["000001.SZ"] == "OLD.SI"
    assert result["2026-06-02"]["000001.SZ"] == "NEW.SI"


def test_build_stock_sector_maps_by_date_rejects_conflicting_visible_membership() -> None:
    rows = [
        {"ts_code": "000001.SZ", "l2_code": "A.SI", "in_date": date(2026, 1, 1), "out_date": None},
        {"ts_code": "000001.SZ", "l2_code": "B.SI", "in_date": date(2026, 1, 1), "out_date": None},
    ]

    with pytest.raises(ValueError, match="conflicting stock-sector memberships"):
        build_stock_sector_maps_by_date(rows, [date(2026, 6, 1)])


def test_build_input_data_max_dates_by_date_closes_each_causal_watermark() -> None:
    result = build_input_data_max_dates_by_date(
        trade_dates=[date(2026, 6, 1), date(2026, 6, 2)],
        sector_dates=[date(2026, 5, 29), date(2026, 6, 2)],
        index_dates=[date(2026, 6, 1), date(2026, 6, 2)],
        market_volume_dates=[date(2026, 5, 29), date(2026, 6, 1)],
    )

    assert result["2026-06-01"] == {
        "sector_data": "2026-05-29",
        "index_daily": "2026-06-01",
        "sw_daily": "2026-06-01",
        "sw_index_member_effective_as_of": "2026-06-01",
    }
    assert result["2026-06-02"]["sector_data"] == "2026-06-02"


def test_build_stock_sector_membership_spans_closes_changes_and_exits() -> None:
    result = build_stock_sector_membership_spans(
        {
            "2026-06-01": {"000001.SZ": "A.SI", "000002.SZ": "B.SI"},
            "2026-06-02": {"000001.SZ": "C.SI", "000002.SZ": "B.SI"},
            "2026-06-03": {"000001.SZ": "C.SI"},
        }
    )

    assert result == {
        "000001.SZ": [
            {"start_date": "2026-06-01", "end_date": "2026-06-01", "sector_code": "A.SI"},
            {"start_date": "2026-06-02", "end_date": "2026-06-03", "sector_code": "C.SI"},
        ],
        "000002.SZ": [
            {"start_date": "2026-06-01", "end_date": "2026-06-02", "sector_code": "B.SI"},
        ],
    }


def test_resolve_daily_output_uses_as_of_membership_without_future_row() -> None:
    result = resolve_coefficient_membership_maps(
        {"2026-06-01": {"000001.SZ": "A.SI"}},
        {"2026-06-02": {"A.SI": 1.1}},
        output_trade_date="2026-06-02",
        as_of_trade_date="2026-06-01",
        backtest_end="2026-06-01",
    )

    assert result == {"2026-06-02": {"000001.SZ": "A.SI"}}


def test_resolve_membership_rejects_sector_without_daily_coefficient() -> None:
    with pytest.raises(ValueError, match="without daily coefficients"):
        resolve_coefficient_membership_maps(
            {"2026-06-01": {"000001.SZ": "A.SI"}},
            {"2026-06-01": {"B.SI": 1.1}},
            output_trade_date=None,
            as_of_trade_date=None,
            backtest_end="2026-06-01",
        )


def _sha256(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_frozen_input_fixture(tmp_path, *, omit_sector_day: bool = False):
    root = tmp_path / "release"
    root.mkdir()
    dates = pd.to_datetime(["2026-06-01", "2026-06-02", "2026-06-03"])
    sector_dates = dates[1:] if omit_sector_day else dates
    sector_index = pd.MultiIndex.from_product(
        [sector_dates, ["000001.SZ"]], names=["datetime", "instrument"]
    )
    sector = pd.DataFrame(
        {
            "l2_code_id": [0] * len(sector_index),
            "sw2_pct_change": [0.1, 0.2, 0.3][-len(sector_index) :],
            "sw2_vol": 100.0,
            "sw2_amount": 1000.0,
            "sw2_mf_net_amt": 10.0,
            "sw2_mf_buy_elg_amt": 20.0,
            "sw2_mf_sell_elg_amt": 10.0,
        },
        index=sector_index,
    )
    index = pd.DataFrame(
        {
            "trade_date": dates,
            "ts_code": ["000300.SH"] * len(dates),
            "close": [100.0, 101.0, 102.0],
        }
    )
    paths = {
        "sector_data_h5": root / "sector_data.h5",
        "index_daily_h5": root / "index_daily.h5",
        "sector_code_map_json": root / "sector_code_map.json",
        "market_context_parquet": root / "market_context.parquet",
    }
    sector.to_hdf(paths["sector_data_h5"], key="data", format="table", data_columns=True)
    index.to_hdf(paths["index_daily_h5"], key="data", format="fixed")
    ordered_codes = ["A.SI"]
    paths["sector_code_map_json"].write_text(
        json.dumps(
            {
                "schema_version": "qe_sw_l2_code_map_v1",
                "ordered_codes": ordered_codes,
                "code_map_digest": digest_named_fields(
                    "dataset_release_sw_l2_code_map_v1",
                    {"ordered_codes": ordered_codes},
                ),
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        {"trade_date": dates, "sw_daily_total_vol": [1000.0, 1100.0, 1200.0]}
    ).to_parquet(paths["market_context_parquet"], index=False)
    bundle = {
        "schema_version": "qe_hmm_frozen_input_v1",
        "dataset_root": str(root),
        "dataset_identity": {"generation": "fixture"},
        "market_volume_definition": "sum_market_sw_daily_vol_all_rows_v1",
        "files": {
            key: {"relative_path": path.name, "sha256": _sha256(path)}
            for key, path in paths.items()
        },
    }
    return bundle


def test_load_frozen_coefficient_inputs_uses_hash_pinned_files_only(tmp_path) -> None:
    result = load_frozen_coefficient_inputs(
        _write_frozen_input_fixture(tmp_path),
        history_start=date(2026, 6, 1),
        test_start=date(2026, 6, 1),
        backtest_end=date(2026, 6, 3),
    )

    assert result["dataset_identity"] == {"generation": "fixture"}
    assert result["sector_data"]["A.SI"][date(2026, 6, 1)]["sw2_pct_change"] == 0.1
    assert result["stock_sector_membership_spans"]["000001.SZ"] == [
        {"start_date": "2026-06-01", "end_date": "2026-06-03", "sector_code": "A.SI"}
    ]
    assert date(2026, 6, 2) in result["csi300"]


def test_load_frozen_coefficient_inputs_rejects_missing_membership_day(tmp_path) -> None:
    with pytest.raises(ValueError, match="no rows for trading dates"):
        load_frozen_coefficient_inputs(
            _write_frozen_input_fixture(tmp_path, omit_sector_day=True),
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_hash_drift(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(tmp_path)
    bundle["files"]["sector_data_h5"]["sha256"] = "0" * 64

    with pytest.raises(ValueError, match="sha256 mismatch"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_load_frozen_coefficient_inputs_rejects_code_map_digest_drift(tmp_path) -> None:
    bundle = _write_frozen_input_fixture(tmp_path)
    spec = bundle["files"]["sector_code_map_json"]
    path = Path(bundle["dataset_root"]) / spec["relative_path"]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["code_map_digest"] = "0" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")
    spec["sha256"] = _sha256(path)

    with pytest.raises(ValueError, match="code-map digest mismatch"):
        load_frozen_coefficient_inputs(
            bundle,
            history_start=date(2026, 6, 1),
            test_start=date(2026, 6, 1),
            backtest_end=date(2026, 6, 3),
        )


def test_main_frozen_mode_never_imports_database_driver(monkeypatch, tmp_path, capsys) -> None:
    import builtins
    import numpy as np
    import scripts.precompute_hmm_coefficients as module

    days = pd.bdate_range("2026-05-01", periods=30).date.tolist()
    coefficient_days = [day for day in days if day >= date(2026, 6, 1)]
    model_path = tmp_path / "models.json"
    model_path.write_text(
        json.dumps(
            {
                "A.SI": {
                    "n_states": 2,
                    "means": [[0.0] * 4, [1.0] * 4],
                    "state_labels": {"0": "neutral", "1": "trending"},
                }
            }
        ),
        encoding="utf-8",
    )
    params = {
        "model_path": str(model_path),
        "model_sha256": _sha256(model_path),
        "test_start": "2026-06-01",
        "backtest_end": coefficient_days[-1].isoformat(),
        "frozen_input_bundle": {"schema_version": "qe_hmm_frozen_input_v1"},
    }
    maps_by_date = {
        day.isoformat(): {"000001.SZ": "A.SI"} for day in coefficient_days
    }
    monkeypatch.setattr(module, "parse_stdin", lambda: params)
    monkeypatch.setattr(
        module,
        "load_frozen_coefficient_inputs",
        lambda *_args, **_kwargs: {
            "dataset_root": str(tmp_path),
            "dataset_identity": {"generation": "fixture"},
            "file_sha256": {key: "1" * 64 for key in module.FROZEN_FILE_KEYS},
            "sector_code_map_digest": "2" * 64,
            "sector_data": {"A.SI": {day: {} for day in days}},
            "sector_rows": {"A.SI": []},
            "sector_dates": days,
            "csi300": {day: 0.0 for day in days},
            "index_dates": days,
            "market_vol": {day: 1.0 for day in days},
            "market_volume_dates": days,
            "stock_sector_maps_by_date": maps_by_date,
            "stock_sector_membership_spans": build_stock_sector_membership_spans(maps_by_date),
        },
    )
    monkeypatch.setattr(
        module,
        "restore_hmm",
        lambda _info: SimpleNamespace(means_=np.zeros((2, 4))),
    )
    monkeypatch.setattr(
        module,
        "build_legacy_observations",
        lambda *_args, **_kwargs: (np.ones((len(days), 4)), days),
    )
    monkeypatch.setattr(
        module,
        "forward_filter_posteriors",
        lambda _hmm, obs: np.tile(np.asarray([[1.0, 0.0]]), (len(obs), 1)),
    )
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == "psycopg2" or name.startswith("psycopg2."):
            raise AssertionError("frozen QE mode must not import psycopg2")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(sys, "argv", ["precompute_hmm_coefficients.py"])

    main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["data_source"] == "frozen_qe_release"
    assert "stock_sector_map_by_date" not in payload
    assert payload["stock_sector_membership_spans"] == {
        "000001.SZ": [
            {
                "start_date": coefficient_days[0].isoformat(),
                "end_date": coefficient_days[-1].isoformat(),
                "sector_code": "A.SI",
            }
        ]
    }


def test_precompute_source_has_no_database_data_plane() -> None:
    import scripts.precompute_hmm_coefficients as module

    source = Path(module.__file__).read_text(encoding="utf-8")

    for forbidden in ("psycopg2", "market.", "TDX_DB", "DATABASE_URL"):
        assert forbidden not in source
