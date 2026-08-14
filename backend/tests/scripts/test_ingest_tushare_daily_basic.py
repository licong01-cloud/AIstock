from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd
import pytest

import scripts.ingest_tushare_daily_basic as ingestion


NUMERIC_FIELDS = (
    "close",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
)


class _Cursor:
    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _Connection:
    def cursor(self) -> _Cursor:
        return _Cursor()


def test_upsert_normalizes_non_finite_provider_values(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    def fake_execute_values(_cursor: object, sql: str, values: list[tuple[Any, ...]]) -> None:
        captured["sql"] = sql
        captured["values"] = values

    monkeypatch.setattr(ingestion.pgx, "execute_values", fake_execute_values)
    row = {
        "trade_date": dt.date(2026, 6, 16),
        "ts_code": "000001.SZ",
        "close": Decimal("10.25"),
        "turnover_rate": float("nan"),
        "turnover_rate_f": Decimal("NaN"),
        "volume_ratio": float("inf"),
        "pe": float("-inf"),
    }

    assert ingestion._upsert_daily_basic(_Connection(), [row]) == 1

    values = captured["values"][0]
    assert values[:3] == (dt.date(2026, 6, 16), "000001.SZ", Decimal("10.25"))
    assert values[3:7] == (None, None, None, None)
    assert all(value is None for value in values[7:])


def test_upsert_preserves_finite_target_when_provider_value_is_invalid(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    def fake_execute_values(_cursor: object, sql: str, values: list[tuple[Any, ...]]) -> None:
        captured["sql"] = sql
        captured["values"] = values

    monkeypatch.setattr(ingestion.pgx, "execute_values", fake_execute_values)

    ingestion._upsert_daily_basic(
        _Connection(),
        [{"trade_date": dt.date(2026, 6, 16), "ts_code": "000001.SZ", "free_share": Decimal("Infinity")}],
    )

    assert "free_share=COALESCE(EXCLUDED.free_share, target.free_share)" in captured["sql"]
    assert "INSERT INTO market.daily_basic AS target" in captured["sql"]
    assert captured["values"][0][15] is None


@pytest.mark.parametrize(
    "invalid",
    [
        None,
        pd.NA,
        np.float64("nan"),
        np.float32("inf"),
        float("-inf"),
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        "NaN",
        "Infinity",
        "-Infinity",
        "nan",
        "inf",
        "-inf",
    ],
)
def test_upsert_normalizes_every_numeric_field_and_keeps_idempotent_sql(
    monkeypatch: Any,
    invalid: object,
) -> None:
    captured: list[tuple[str, list[tuple[Any, ...]]]] = []

    def fake_execute_values(_cursor: object, sql: str, values: list[tuple[Any, ...]]) -> None:
        captured.append((sql, values))

    monkeypatch.setattr(ingestion.pgx, "execute_values", fake_execute_values)
    row = {
        "trade_date": dt.date(2026, 6, 16),
        "ts_code": "000001.SZ",
        **{field: invalid for field in NUMERIC_FIELDS},
    }

    assert ingestion._upsert_daily_basic(_Connection(), [row]) == 1
    assert ingestion._upsert_daily_basic(_Connection(), [row]) == 1

    first_sql, first_values = captured[0]
    assert captured[1] == captured[0]
    assert len(first_values[0]) == 2 + len(NUMERIC_FIELDS)
    assert all(value is None for value in first_values[0][2:])
    for field in NUMERIC_FIELDS:
        assert f"{field}=COALESCE(EXCLUDED.{field}, target.{field})" in first_sql
