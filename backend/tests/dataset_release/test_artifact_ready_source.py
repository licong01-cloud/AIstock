from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from backend.services.dataset_release.artifact_ready_source import (
    ArtifactReadyProviderTerminal,
    _bounded_tushare_records,
    _normalize_tushare_daily_row,
)


def test_tushare_daily_normalization_matches_database_units() -> None:
    normalized = _normalize_tushare_daily_row(
        {
            "ts_code": "000001.SZ",
            "trade_date": "20260831",
            "open": "10.0014",
            "high": "10.5015",
            "low": "9.5004",
            "close": "10.2505",
            "vol": "123.5",
            "amount": "456.789001",
        },
        expected_code="000001.SZ",
    )
    assert normalized == {
        "ts_code": "000001.SZ",
        "trade_date": "2026-08-31",
        "open_li": 10001,
        "high_li": 10502,
        "low_li": 9500,
        "close_li": 10251,
        "volume_hand": 124,
        "amount_li": 456789001,
    }


def test_provider_frame_is_bounded_before_record_materialization() -> None:
    frame = pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260831", "close": 10.0}])
    rows = _bounded_tushare_records(
        frame,
        dataset="daily",
        expected_columns=("ts_code", "trade_date", "close"),
        date_column="trade_date",
        start=date(2026, 8, 31),
        end=date(2026, 8, 31),
        max_rows=1,
    )
    assert rows == ({"ts_code": "000001.SZ", "trade_date": "20260831", "close": 10.0},)

    with pytest.raises(ArtifactReadyProviderTerminal, match="row count exceeds"):
        _bounded_tushare_records(
            pd.concat([frame, frame], ignore_index=True),
            dataset="daily",
            expected_columns=("ts_code", "trade_date", "close"),
            date_column="trade_date",
            start=date(2026, 8, 31),
            end=date(2026, 8, 31),
            max_rows=1,
        )
