from datetime import date
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError, cutoff_on
from backend.services.position_timing.action_value_data import DailyCandidate, file_reference, runtime_core_frame
from backend.services.position_timing.contracts import canonical_sha256


@pytest.fixture
def candidate(tmp_path: Path):
    daily = tmp_path / "components/daily_bin_candidate"
    (daily / "calendars").mkdir(parents=True)
    (daily / "instruments").mkdir()
    days = pd.bdate_range("2024-01-01", periods=50)
    (daily / "calendars/day.txt").write_text("\n".join(str(day.date()) for day in days), encoding="utf-8")
    (daily / "instruments/stock_universe.txt").write_text(
        f"000001.SZ\t{days[3].date()}\t{days[-1].date()}\n", encoding="utf-8")
    (daily / "meta_export.json").write_text(json.dumps({"export_mode": "authoritative_aistock_dump_bin",
                                                       "st_pit": True, "stock_universe_mode": "pit_spans"}))
    (tmp_path / "direct_monthly_state.json").write_text("{}")
    suspend = tmp_path / "components/suspend_d_daily_candidate_v2"
    suspend.mkdir()
    pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": [str(days[5].date())],
                  "suspend_type": ["S"]}).to_parquet(suspend / "suspend_d.parquet")
    for symbol in ("000001.sz", "000300.sh"):
        folder = daily / "features" / symbol
        folder.mkdir(parents=True)
        for name, value in {"open": 5.0, "high": 5.5, "low": 4.5, "close": 5., "volume": 2000.,
                            "factor": .5, "up_limit_price": 11., "down_limit_price": 9.}.items():
            np.asarray([0, *([value] * 50)], dtype="<f4").tofile(folder / f"{name}.day.bin")
    return DailyCandidate.open(tmp_path)


def test_daily_source_raw_units_pit_spans_and_suspension(candidate):
    bars = candidate.bars("000001.SZ")
    assert bars.open.eq(10).all() and bars.volume.eq(1000).all()
    assert bars.up_limit.eq(11).all() and bars.down_limit.eq(9).all()
    assert bars.pit_active.sum() == 47
    assert not bars.pit_active.iloc[0] and bars.is_suspended.iloc[5]
    assert candidate.bars("000300.SH").close.eq(5).all()
    with pytest.raises(ActionValueError, match="OUTSIDE_RESEARCH"):
        candidate.bars("../../elsewhere")


def test_coverage_freezes_input_hashes_without_outcomes(candidate):
    result = candidate.coverage()
    assert result["outcomes_read"] is False
    assert result["historical_ingestion_timestamps_verified"] is False
    assert result["coverage"]["000001.SZ"]["complete_core_sessions"] == 21
    assert len(result["source_references"]) == 18
    assert result["source_sha256"] == canonical_sha256(result["source_references"])
    with pytest.raises(ActionValueError, match="SOURCE_FILE_UNAVAILABLE"):
        file_reference(candidate.root / "missing")
    candidate.references["later_read"] = {"sha256": "0" * 64}
    assert "later_read" not in result["source_references"]


def test_coverage_artifact_is_immutable_source_only(candidate, tmp_path):
    root = tmp_path / "timing_owned"
    path = candidate.publish_coverage(timing_root=root)
    content = path.read_bytes()
    assert candidate.publish_coverage(timing_root=root) == path
    assert path.read_bytes() == content
    payload = json.loads(content)
    assert payload["outcomes_read"] is False and "heads" not in payload
    assert payload["reader_source"]["sha256"] == file_reference(Path(DailyCandidate.__module__.replace('.', '/')).with_suffix('.py'))["sha256"]


def test_float32_noise_cannot_make_one_word_limit_tradable(candidate):
    folder = candidate.root / "components/daily_bin_candidate/features/000001.sz"
    for field in ("open", "high", "low", "close"):
        np.asarray([0, *([5.5 + 1e-7] * 50)], dtype="<f4").tofile(folder / f"{field}.day.bin")
    bars = candidate.bars("000001.SZ")
    assert bars.open.eq(bars.up_limit).all()


def runtime_snapshot():
    day = date(2026, 9, 7)
    rows = {"000001.SZ": {str(day): {"open": 10, "high": 11, "low": 9, "close": 10,
                                      "volume": 1000, "adj_factor": "1.2",
                                      "feature_available_at": cutoff_on(day).isoformat()}}}
    combined = {"rows": rows, "benchmark_rows": None}
    return {"rows": rows, "identity": {"rows_sha256": canonical_sha256(combined)},
            "adjustment_identity": {"source": "market.adj_factor"}, "captured_at": cutoff_on(day).isoformat()}


def test_runtime_keeps_raw_units_and_rejects_unproven_capture_or_identity():
    snapshot = runtime_snapshot()
    args = {"symbol": "000001.SZ", "calendar": [date(2026, 9, 7)], "cutoff": cutoff_on(date(2026, 9, 7))}
    result = runtime_core_frame(snapshot, **args)
    assert result.close.iloc[0] == 10 and result.factor.iloc[0] == 1.2
    with pytest.raises(ActionValueError, match="CAPTURE_TIME_UNAVAILABLE"):
        runtime_core_frame({**snapshot, "captured_at": None}, **args)
    # Capture provenance may be later than the logical EOD feature timestamp;
    # row-level feature_available_at, when present, remains the causal clock.
    assert runtime_core_frame(
        {**snapshot, "captured_at": args["cutoff"].replace(hour=21).isoformat()}, **args
    ).close.iloc[0] == 10
    with pytest.raises(ActionValueError, match="IDENTITY_MISMATCH"):
        runtime_core_frame({**snapshot, "identity": {"rows_sha256": "0" * 64}}, **args)


def test_runtime_unknown_session_not_forward_filled():
    snapshot = runtime_snapshot()
    result = runtime_core_frame(snapshot, "000001.SZ", calendar=[date(2026, 9, 4), date(2026, 9, 7)],
                                cutoff=cutoff_on(date(2026, 9, 7)))
    assert result.iloc[0].isna().all()
    assert result.close.iloc[1] == 10
