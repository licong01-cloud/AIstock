from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.outcome_split import OutcomeDateSplit
from backend.services.advisory_model_first.price_range_labels import (
    apply_price_range_split,
    build_price_range_labels,
)


def _candidate(symbol: str = "000001.SZ") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "decision_as_of_trade_date": [pd.Timestamp("2026-01-05")],
            "target_trade_date": [pd.Timestamp("2026-01-06")],
            "instrument": [symbol],
        }
    )


def _daily(symbol: str = "000001.SZ") -> pd.DataFrame:
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2026-01-05"), symbol),
            (pd.Timestamp("2026-01-06"), symbol),
        ],
        names=["datetime", "instrument"],
    )
    return pd.DataFrame(
        {
            "open": [9.8, 10.2],
            "low": [9.7, 10.1],
            "close": [10.0, 10.3],
            "factor": [1.0, 1.0],
            "up_limit_price": [11.0, 11.0],
            "prev_close": [9.5, 10.0],
            "limit_up": [0.0, 0.0],
        },
        index=index,
    )


def _empty_suspend() -> pd.DataFrame:
    return pd.DataFrame(columns=["trade_date", "instrument", "suspend_type"])


def test_price_range_labels_keep_authoritative_negative_separate_from_missing_data() -> None:
    calendar = pd.to_datetime(["2026-01-05", "2026-01-06"])
    valid = build_price_range_labels(
        candidates=_candidate(),
        daily=_daily(),
        suspend_rows=_empty_suspend(),
        trading_calendar=calendar,
    )
    row = valid.labels.iloc[0]
    assert row["entry_label_status"] == "AVAILABLE"
    assert row["entry_executable"] == 1
    assert row["entry_gap_return"] == pytest.approx(0.02)

    missing = build_price_range_labels(
        candidates=_candidate(),
        daily=_daily().iloc[:1],
        suspend_rows=_empty_suspend(),
        trading_calendar=calendar,
    )
    row = missing.labels.iloc[0]
    assert row["entry_label_status"] == "UNAVAILABLE"
    assert row["entry_label_reason"] == "target_market_row_missing_unexplained"
    assert np.isnan(row["entry_executable"])

    suspend = pd.DataFrame(
        {
            "trade_date": [pd.Timestamp("2026-01-06")],
            "instrument": ["000001.SZ"],
            "suspend_type": ["S"],
        }
    )
    suspended = build_price_range_labels(
        candidates=_candidate(),
        daily=_daily().iloc[:1],
        suspend_rows=suspend,
        trading_calendar=calendar,
    )
    row = suspended.labels.iloc[0]
    assert row["entry_label_status"] == "AVAILABLE"
    assert row["entry_executable"] == 0
    assert row["entry_label_reason"] == "target_authoritatively_suspended"
    assert suspended.coverage.iloc[0].to_dict()["authoritative_negative_count"] == 1


def test_one_price_limit_up_is_authoritative_negative_and_split_is_exact() -> None:
    daily = _daily()
    daily.loc[(pd.Timestamp("2026-01-06"), "000001.SZ"), ["low", "limit_up"]] = [
        11.0,
        1.0,
    ]
    result = build_price_range_labels(
        candidates=_candidate(),
        daily=daily,
        suspend_rows=_empty_suspend(),
        trading_calendar=pd.to_datetime(["2026-01-05", "2026-01-06"]),
    )
    date = pd.Timestamp("2026-01-05")
    split = OutcomeDateSplit((date,), (), (), (), ())
    labels = apply_price_range_split(result.labels, split)

    assert labels.loc[0, "entry_label_reason"] == "target_one_price_limit_up"
    assert labels.loc[0, "entry_executable"] == 0
    assert labels.loc[0, "split"] == "train"
    assert bool(labels.loc[0, "binary_modelable"])
    assert not bool(labels.loc[0, "gap_modelable"])
