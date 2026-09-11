from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest

from backend.services.factor_research.full_evaluation import (
    build_standard_windows,
    compute_correlation_views,
    load_reference_values,
    validate_full_evaluation_spec,
)
from backend.services.factor_research.models import ResearchError
from backend.services.factor_research.runner import (
    CANONICAL_UNIVERSE,
    execute,
    load_values,
    validate_spec,
)


def _write_values(path: Path, name: str, dates: pd.DatetimeIndex, instruments: list[str]) -> None:
    index = pd.MultiIndex.from_product(
        [dates, instruments], names=["datetime", "instrument"]
    )
    values = np.tile(np.arange(1.0, len(instruments) + 1.0), len(dates))
    pd.DataFrame({name: values}, index=index).to_hdf(path, key="data")


def _write_reference_values(
    path: Path, dates: pd.DatetimeIndex, instruments: list[str]
) -> None:
    index = pd.MultiIndex.from_product(
        [dates, instruments], names=["datetime", "instrument"]
    )
    values = np.tile(np.arange(1.0, len(instruments) + 1.0), len(dates))
    pd.DataFrame({"value": values}, index=index).to_parquet(path)


def test_reference_loader_accepts_only_official_value_schema(tmp_path: Path) -> None:
    dates = pd.bdate_range("2024-01-02", periods=3)
    instruments = ["000001.SZ", "000002.SZ"]
    valid = tmp_path / "valid.parquet"
    _write_reference_values(valid, dates, instruments)

    loaded = load_reference_values(valid, "m_reference")

    assert list(loaded.columns) == ["m_reference"]
    assert loaded.index.names == ["datetime", "instrument"]
    assert loaded["m_reference"].tolist() == [1.0, 2.0] * len(dates)

    wrong_column = tmp_path / "wrong-column.parquet"
    pd.read_parquet(valid).rename(columns={"value": "m_reference"}).to_parquet(
        wrong_column
    )
    with pytest.raises(ResearchError, match="official value column"):
        load_reference_values(wrong_column, "m_reference")

    multiple_columns = tmp_path / "multiple-columns.parquet"
    frame = pd.read_parquet(valid)
    frame.assign(other=frame["value"]).to_parquet(multiple_columns)
    with pytest.raises(ResearchError, match="official value column"):
        load_reference_values(multiple_columns, "m_reference")

    invalid_index = tmp_path / "invalid-index.parquet"
    frame.reset_index(drop=True).to_parquet(invalid_index, index=False)
    with pytest.raises(ResearchError, match="MultiIndex"):
        load_reference_values(invalid_index, "m_reference")

    with pytest.raises(ResearchError, match="named factor column"):
        load_values(valid, "m_candidate")


def test_standard_windows_use_actual_calendar_boundaries() -> None:
    dates = pd.bdate_range("2023-12-27", "2026-08-31")
    windows = build_standard_windows(
        dates, signal_start="2023-12-27", signal_end="2026-08-31"
    )

    assert windows["full"] == {
        "start": "2023-12-27",
        "end": "2026-08-31",
        "required_days": 0,
    }
    assert windows["from_2024"]["start"] == "2024-01-01"
    assert windows["year_2024"]["end"] == "2024-12-31"
    assert windows["year_2026"]["end"] == "2026-08-31"
    assert windows["recent_1m"]["start"] == "2026-07-31"
    assert windows["month_2024_01"]["start"] == "2024-01-01"
    assert windows["month_2026_08"]["end"] == "2026-08-31"


def test_full_evaluation_requires_explicit_regular_reference_artifacts(tmp_path: Path) -> None:
    reference = tmp_path / "reference.h5"
    reference.write_bytes(b"not-read-by-validation")
    value = {
        "reference_value_artifacts": {
            "m_reference": str(reference),
            "CORD10": str(reference),
        },
        "correlation_batch_size": 2,
        "correlation_half_life": 4,
        "correlation_min_stocks": 3,
        "correlation_min_effective_days": 2,
    }

    normalized = validate_full_evaluation_spec(
        value, candidate_names={"m_candidate"}, repo_root=Path(__file__).parents[3]
    )
    assert normalized["reference_value_artifacts"] == {
        "CORD10": str(reference.resolve()),
        "m_reference": str(reference.resolve())
    }

    value["correlation_min_effective_days"] = 0
    with pytest.raises(ResearchError, match="positive integer"):
        validate_full_evaluation_spec(
            value, candidate_names={"m_candidate"}, repo_root=Path(__file__).parents[3]
        )


def test_full_evaluation_reports_candidate_pairs_without_reference_pairs(tmp_path: Path) -> None:
    dates = pd.bdate_range("2024-01-02", periods=4)
    instruments = ["000001.SZ", "000002.SZ", "000003.SZ"]
    paths = {}
    for name in ("m_candidate_a", "m_candidate_b"):
        path = tmp_path / f"{name}.h5"
        _write_values(path, name, dates, instruments)
        paths[name] = path
    reference_path = tmp_path / "m_reference.parquet"
    _write_reference_values(reference_path, dates, instruments)
    paths["m_reference"] = reference_path
    windows = build_standard_windows(
        dates, signal_start=str(dates[0].date()), signal_end=str(dates[-1].date())
    )
    result = compute_correlation_views(
        {name: paths[name] for name in ("m_candidate_a", "m_candidate_b")},
        {
            "reference_value_artifacts": {"m_reference": str(paths["m_reference"])},
            "correlation_batch_size": 1,
            "correlation_half_life": 2,
            "correlation_min_stocks": 3,
            "correlation_min_effective_days": 2,
        },
        windows,
    )

    assert result["reference_reference_pairs_computed"] == 0
    assert result["reference_names"] == ["m_reference"]
    assert all(not name.startswith("month_") for name in result["window_names"])
    assert all(
        window["requested_pairs"] == 1
        and window["records"][0]["candidate"] == "m_candidate_a"
        and window["records"][0]["reference"] == "m_candidate_b"
        for window in result["candidate_candidate_windows"]
    )


def test_runner_full_evaluation_is_opt_in_and_computes_only_selected_pairs(
    tmp_path: Path,
) -> None:
    dates = pd.bdate_range("2024-01-02", periods=20)
    instruments = ["000001.SZ", "000002.SZ", "000003.SZ"]
    data_dir = tmp_path / "data"
    qlib_dir = tmp_path / "qlib"
    data_dir.mkdir()
    qlib_dir.mkdir()
    reference_path = tmp_path / "reference.parquet"
    _write_reference_values(reference_path, dates, instruments)

    script = tmp_path / "candidate.py"
    script.write_text(
        """
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd
p = argparse.ArgumentParser()
for key in ('data-dir', 'output', 'start-date', 'end-date', 'instruments'):
    p.add_argument('--' + key, required=True)
a = p.parse_args()
dates = pd.bdate_range(a.start_date, a.end_date)
symbols = json.loads(a.instruments)
index = pd.MultiIndex.from_product([dates, symbols], names=['datetime', 'instrument'])
values = np.tile(np.arange(1.0, len(symbols) + 1.0), len(dates))
pd.DataFrame({'m_candidate': values}, index=index).to_hdf(Path(a.output), key='data')
""",
        encoding="utf-8",
    )
    raw = {
        "task_id": str(uuid4()),
        "record_id": str(uuid4()),
        "attempt_id": str(uuid4()),
        "expected_revision": 1,
        "universe_key": CANONICAL_UNIVERSE,
        "method_version": "2.1",
        "read_start": str(dates[0].date()),
        "signal_start": str(dates[0].date()),
        "signal_end": str(dates[-1].date()),
        "read_end": str(dates[-1].date()),
        "cutoff": str(dates[-1].date()),
        "instruments": instruments,
        "data_dir": str(data_dir),
        "qlib_bin_path": str(qlib_dir),
        "artifact_root": str(tmp_path / "artifacts"),
        "candidates": [{"factor_name": "m_candidate", "script": str(script)}],
        "full_evaluation": {
            "reference_value_artifacts": {"m_reference": str(reference_path)},
            "correlation_batch_size": 1,
            "correlation_half_life": 4,
            "correlation_min_stocks": 3,
            "correlation_min_effective_days": 2,
        },
    }
    spec, output = validate_spec(raw)
    close = pd.DataFrame(100.0, index=dates, columns=instruments)
    ctx = {
        "close_unstacked": close,
        "fwd_ret_mats": {name: close * 0.01 for name in ("1d", "5d", "10d", "20d")},
        "dates": dates,
        "st_pit_eligible_mask": close.notna(),
        "data_start": str(dates[0].date()),
        "data_end": str(dates[-1].date()),
        "instrument_coverage": {
            "requested_instrument_count": len(instruments),
            "physical_price_instrument_count": len(instruments),
            "missing_price_instrument_count": 0,
            "missing_price_instruments": [],
        },
    }
    calls: list[dict] = []

    def compute(name, frame, _ctx, **kwargs):
        calls.append({"name": name, "rows": len(frame), **kwargs})
        return {"factor_name": name, "metrics": {"full": {"status": "ok"}}}

    result = execute(spec, output, prepare=lambda **_kwargs: ctx, compute=compute)

    assert calls[0]["include_horizon_metrics"] is True
    assert "from_2024" in calls[0]["evaluation_windows"]
    full = result["full_evaluation"]
    assert full["scope"] == "research_only_not_official_metrics_correlations_or_qe_result"
    assert full["official_database_writes"] == 0
    assert full["all_requested_instruments_have_physical_prices"] is True
    assert full["correlations"]["reference_reference_pairs_computed"] == 0
    assert all(
        window["requested_pairs"] == 1
        and window["available_pairs"] == 1
        and window["records"][0]["correlation"] == 1.0
        for window in full["correlations"]["windows"]
    )
    assert json.loads((output / "execution.json").read_text(encoding="utf-8"))["request"][
        "full_evaluation"
    ] == spec["full_evaluation"]
