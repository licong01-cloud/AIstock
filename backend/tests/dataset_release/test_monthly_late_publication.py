import numpy as np
import pandas as pd
import pytest

from backend.services.dataset_release.monthly_late_publication import (
    MARGIN_COLUMNS, amend_margin_files, effective_margin_deferral, resolve_margin_deferral,
    validate_margin_source,
)


def _frame(days, symbols, values):
    index = pd.MultiIndex.from_arrays([pd.to_datetime(days), symbols], names=["datetime", "instrument"])
    return pd.DataFrame({name: np.array(values, dtype="float32") for name in MARGIN_COLUMNS}, index=index)


def test_margin_amendment_is_bounded_keeps_nulls_and_preserves_source_files(tmp_path):
    index_days = ["2026-09-29", "2026-09-30", "2026-09-30"]
    old = _frame(index_days, ["000001.SZ", "000001.SZ", "600293.SH"], [1, 1, 3])
    old["untouched"] = np.array([8, 9, 10], dtype="float32")
    original = tmp_path / "old"
    original.mkdir()
    old.iloc[:1].drop(columns="untouched").to_hdf(original / "margin_detail.h5", key="data", format="table", data_columns=True)
    old.to_parquet(original / "static_factors.parquet")
    source = _frame(["2026-09-30"] * 3, ["000001.SZ", "600293.SH", "830001.BJ"], [2, 4, 99])
    source.loc[(pd.Timestamp("2026-09-30"), "600293.SH"), "md_rqye"] = np.nan
    before = {p.name: p.read_bytes() for p in original.iterdir()}
    target = tmp_path / "new"
    receipt = amend_margin_files(original, target, source=source, trade_date="2026-09-30")
    raw = pd.read_hdf(target / "margin_detail.h5", "data")
    static = pd.read_parquet(target / "static_factors.parquet")
    assert receipt["source_rows"] == 3 and receipt["applicable_rows"] == 2
    assert receipt["raw_rows_added"] == 2
    assert len(raw) == 3 and np.isnan(raw.loc[(pd.Timestamp("2026-09-30"), "600293.SH"), "md_rqye"])
    assert static.loc[(pd.Timestamp("2026-09-30"), "000001.SZ"), "md_rzye"] == 2
    # Existing static schema deliberately applies per-field as-of filling, never to raw H5.
    assert static.loc[(pd.Timestamp("2026-09-30"), "600293.SH"), "md_rqye"] == 3
    pd.testing.assert_series_equal(static["untouched"], old["untouched"])
    pd.testing.assert_frame_equal(static.iloc[:1], old.iloc[:1])
    assert {p.name: p.read_bytes() for p in original.iterdir()} == before
    with pytest.raises(FileExistsError):
        amend_margin_files(original, target, source=source, trade_date="2026-09-30")


@pytest.mark.parametrize("bad", ["duplicate", "future", "infinite"])
def test_invalid_late_source_does_not_start_output(tmp_path, bad):
    source = _frame(["2026-09-30"], ["000001.SZ"], [1])
    if bad == "duplicate":
        source = pd.concat([source, source])
    elif bad == "future":
        source.index = pd.MultiIndex.from_tuples([(pd.Timestamp("2026-10-01"), "000001.SZ")], names=source.index.names)
    else:
        source.iloc[0, 0] = np.inf
    with pytest.raises(ValueError):
        amend_margin_files(tmp_path / "old", tmp_path / "new", source=source, trade_date="2026-09-30")
    assert not (tmp_path / "new").exists()


@pytest.mark.parametrize("available,expected", [(False, "DEFERRED"), (True, "AVAILABLE")])
def test_available_late_facts_override_only_effective_not_historical_deferral(available, expected):
    payload = {"deferred_source_dates": [{"dataset": "margin_detail", "trade_date": "2026-09-30", "reason_code": "USER_DEFERRED_COLLECTION"}]}
    result = resolve_margin_deferral(payload, trade_date="2026-09-30", source_available=available)
    assert result["status"] == expected
    assert payload["deferred_source_dates"]  # Immutable authorization is never rewritten.
    assert result["deferred_trade_date"] == (None if available else "2026-09-30")


def test_effective_deferral_reads_only_exact_day_in_existing_source_snapshot():
    from unittest.mock import MagicMock

    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (True,)
    payload = {"deferred_source_dates": [{"dataset": "margin_detail", "trade_date": "2026-09-30", "reason_code": "USER_DEFERRED_COLLECTION"}]}
    assert effective_margin_deferral(connection, payload, trade_date="2026-09-30")["deferred_trade_date"] is None
    assert cursor.execute.call_count == 1
    assert "WHERE trade_date=%s" in cursor.execute.call_args.args[0]
    connection.reset_mock()
    effective_margin_deferral(connection, {}, trade_date="2026-09-30")
    connection.cursor.assert_not_called()


def test_signed_provider_correction_is_not_rejected_as_missing():
    source = _frame(["2026-09-30"], ["300980.SZ"], [1])
    source["md_rqchl"] = -4520.0
    assert validate_margin_source(source, trade_date="2026-09-30")["md_rqchl"].iloc[0] == -4520.0
