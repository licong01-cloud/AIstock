from __future__ import annotations

import json
from datetime import date, timedelta

import pandas as pd
import pytest

from backend.services.dataset_release.errors import IndexContractError, IndexOverlapConflict
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.index_materializer import (
    IncrementalIndexContextMaterializer,
    IndexContextMaterializer,
    SelectiveIndexContextMaterializer,
)
from backend.services.dataset_release.streaming_artifacts import sha256_file
from backend.services.dataset_release.stock_schema import QLIB_STOCK_FIELDS


def _row(code: str, day: date, close: float = 10.0) -> dict[str, object]:
    return {
        "ts_code": code,
        "trade_date": day,
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "pre_close": close - 0.5,
        "pct_chg": 5.0,
        "vol": 123.0,
        "amount": 456.0,
    }


class FakeSource:
    def __init__(self, *, conflict: bool = False, omit_provider: bool = False) -> None:
        self.conflict = conflict
        self.omit_provider = omit_provider

    def trading_dates(self, start: date, end: date):
        return [start, start + timedelta(days=1)]

    def database_rows(self, definition, start: date, end: date):
        return [_row(definition.daily_code, start)]

    def provider_rows(self, definition, start: date, end: date):
        if self.omit_provider:
            return [_row(definition.daily_code, start)]
        overlap = _row(definition.daily_code, start, 11.0 if self.conflict else 10.0)
        return [overlap, _row(definition.daily_code, start + timedelta(days=1))]


def test_index_materializer_adds_tushare_missing_keys_with_frozen_units(tmp_path) -> None:
    root = tmp_path / "index-context"
    receipt = IndexContextMaterializer(FakeSource()).materialize(root, cutoff=date(2026, 7, 31), row_group_rows=100)

    assert receipt.rows == 24
    assert receipt.provider_fill_rows == 12
    frame = pd.read_hdf(receipt.h5_path, key="data")
    assert set(frame.index.get_level_values("instrument")) == {item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS}
    first = frame.iloc[0]
    assert first["idx_return_1d"] == pytest.approx(0.05)
    assert first["idx_volume_hand_source"] == pytest.approx(123.0)
    assert first["idx_volume_share_equiv"] == pytest.approx(12_300.0)
    assert first["idx_amount_cny"] == pytest.approx(456_000.0)
    payload = json.loads((root / "index_materialization_receipt.json").read_text())
    assert payload["database_writes"] == 0
    assert payload["production_writes"] == 0
    assert len(list(receipt.csv_root.glob("*.csv"))) == 12
    first_csv = pd.read_csv(sorted(receipt.csv_root.glob("*.csv"))[0])
    assert tuple(first_csv.columns) == ("date", "symbol", *QLIB_STOCK_FIELDS)


def test_index_materializer_rejects_provider_overlap_conflict_before_outputs(tmp_path) -> None:
    with pytest.raises(IndexOverlapConflict):
        IndexContextMaterializer(FakeSource(conflict=True)).materialize(
            tmp_path / "index-context", cutoff=date(2026, 7, 31)
        )


def test_index_materializer_reports_unfilled_calendar_gap(tmp_path) -> None:
    with pytest.raises(IndexContractError, match="coverage mismatch"):
        IndexContextMaterializer(FakeSource(omit_provider=True)).materialize(
            tmp_path / "index-context", cutoff=date(2026, 7, 31)
        )


class TailSource:
    def trading_dates(self, start: date, end: date):
        return [end]

    def database_rows(self, definition, start: date, end: date):
        return [_row(definition.daily_code, end)]

    def provider_rows(self, definition, start: date, end: date):
        return []


def test_incremental_index_materializer_reads_only_tail_and_preserves_baseline(
    tmp_path,
) -> None:
    baseline_cutoff = date(2026, 6, 30)
    cutoff = date(2026, 7, 31)
    baseline = IndexContextMaterializer(TailSource()).materialize(
        tmp_path / "baseline", cutoff=baseline_cutoff, row_group_rows=100
    )
    before = {
        path.relative_to(baseline.root).as_posix(): sha256_file(path)
        for path in baseline.root.rglob("*")
        if path.is_file()
    }

    result = IncrementalIndexContextMaterializer(TailSource()).materialize(
        baseline.root,
        tmp_path / "incremental",
        baseline_cutoff=baseline_cutoff,
        cutoff=cutoff,
        row_group_rows=100,
    )

    after = {
        path.relative_to(baseline.root).as_posix(): sha256_file(path)
        for path in baseline.root.rglob("*")
        if path.is_file()
    }
    frame = pd.read_hdf(result.h5_path, key="data")
    receipt = json.loads((result.root / "index_materialization_receipt.json").read_text())
    assert before == after
    assert len(frame) == result.rows == 24
    assert receipt["incremental"] == {
        "baseline_cutoff": baseline_cutoff.isoformat(),
        "tail_start": "2026-07-01",
        "tail_rows": 12,
        "baseline_rows_retransformed": 0,
    }


class RevisedTailSource(TailSource):
    def database_rows(self, definition, start: date, end: date):
        return [_row(definition.daily_code, end, close=12.0)]


def test_selective_index_materializer_transforms_only_declared_dates(tmp_path) -> None:
    cutoff = date(2026, 7, 31)
    baseline = IndexContextMaterializer(TailSource()).materialize(
        tmp_path / "baseline-selective", cutoff=cutoff, row_group_rows=5
    )
    before = {
        path.relative_to(baseline.root).as_posix(): sha256_file(path)
        for path in baseline.root.rglob("*")
        if path.is_file()
    }

    result = SelectiveIndexContextMaterializer(RevisedTailSource()).materialize(
        baseline.root,
        tmp_path / "selective",
        cutoff=cutoff,
        date_ranges=((cutoff, cutoff),),
        row_group_rows=5,
    )

    frame = pd.read_hdf(result.h5_path, key="data")
    receipt = json.loads((result.root / "index_materialization_receipt.json").read_text())
    after = {
        path.relative_to(baseline.root).as_posix(): sha256_file(path)
        for path in baseline.root.rglob("*")
        if path.is_file()
    }
    assert before == after
    assert set(frame["idx_close_point"].astype(float)) == {12.0}
    assert receipt["selective"]["source_rows_transformed"] == 12
    assert receipt["selective"]["baseline_source_rows_retransformed"] == 0
