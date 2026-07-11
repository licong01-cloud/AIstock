from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = ROOT / "scripts" / "validate_qe_qlib_candidate.py"
SPEC = importlib.util.spec_from_file_location("validate_qe_qlib_candidate", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_baseline_static_columns_accepts_explicit_source(tmp_path) -> None:
    source = tmp_path / "schema.parquet"
    pd.DataFrame(
        {"beta": [1.0], "l2_code_id": [1], "alpha": [2.0]}
    ).to_parquet(source)

    assert MODULE.baseline_static_columns(source) == [
        "beta",
        "l2_code_id",
        "alpha",
    ]


def test_get_h5_pool_uses_same_inclusive_st_cutoff_as_export(monkeypatch) -> None:
    captured = {}

    def fake_run_df(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return pd.DataFrame(
            {"ts_code": ["000001.SZ"], "list_date": [date(1991, 4, 3)]}
        )

    monkeypatch.setattr(MODULE, "run_df", fake_run_df)

    result = MODULE.get_h5_pool(date(2026, 6, 30))

    assert "st.ann_date <= %(end)s" in captured["sql"]
    assert captured["params"] == {"end": date(2026, 6, 30)}
    assert result["ts_code"].tolist() == ["000001.SZ"]


def test_expected_official_universe_applies_ipo_365_day_rule() -> None:
    pool = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "001312.SZ"],
            "list_date": [date(2018, 1, 1), date(2026, 4, 14)],
        }
    )
    ranges = pd.DataFrame(
        {
            "instrument": ["000001.SZ", "001312.SZ"],
            "data_start": [date(2018, 8, 1), date(2026, 4, 14)],
            "data_end": [date(2026, 6, 30), date(2026, 6, 30)],
        }
    )

    result = MODULE.expected_official_universe(
        pool,
        ranges,
        date(2018, 8, 1),
        date(2026, 6, 30),
    )

    assert result["instrument"].tolist() == ["000001.SZ"]
