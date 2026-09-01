from __future__ import annotations

import datetime as dt
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

import scripts.backfill_tushare_daily_basic_fields as backfill


def _snapshot(rows: int = 5000, *, non_null: int | None = None) -> pd.DataFrame:
    non_null = rows if non_null is None else non_null
    data: dict[str, object] = {
        "trade_date": ["20260713"] * rows,
        "ts_code": [f"{index:06d}.SZ" for index in range(rows)],
    }
    for column in backfill.DATA_COLUMNS:
        data[column] = [1.0] * rows
    data["turnover_rate_f"] = [1.0] * non_null + [None] * (rows - non_null)
    data["volume_ratio"] = [1.0] * non_null + [None] * (rows - non_null)
    return pd.DataFrame(data)


def test_validate_snapshot_requires_critical_field_coverage() -> None:
    frame = _snapshot(non_null=4000)

    with pytest.raises(backfill.DailyBasicBackfillError, match="turnover_rate_f non_null_ratio"):
        backfill.validate_snapshot(
            frame,
            trade_date=dt.date(2026, 7, 13),
            fill_fields=backfill.DEFAULT_FIELDS,
            min_rows=5000,
            min_non_null_ratio=0.95,
        )


def test_build_upsert_sql_only_fills_null_or_non_finite_target_values() -> None:
    sql = backfill.build_upsert_sql(backfill.DEFAULT_FIELDS)

    assert "ON CONFLICT (trade_date, ts_code) DO UPDATE" in sql
    assert "turnover_rate_f = CASE WHEN" in sql
    assert "volume_ratio = CASE WHEN" in sql
    assert "target.turnover_rate_f::text IN ('NaN', 'Infinity', '-Infinity')" in sql
    assert "EXCLUDED.turnover_rate_f::text IN ('NaN', 'Infinity', '-Infinity')" in sql
    assert "target.volume_ratio::text IN ('NaN', 'Infinity', '-Infinity')" in sql
    assert "THEN EXCLUDED.turnover_rate_f ELSE target.turnover_rate_f END" in sql
    assert "AND NOT (EXCLUDED.turnover_rate_f IS NULL OR" in sql
    assert "DELETE" not in sql


def test_validate_snapshot_rejects_non_finite_provider_coverage() -> None:
    frame = _snapshot()
    frame.loc[:499, "turnover_rate_f"] = float("inf")

    with pytest.raises(backfill.DailyBasicBackfillError, match="turnover_rate_f non_null_ratio"):
        backfill.validate_snapshot(
            frame,
            trade_date=dt.date(2026, 7, 13),
            fill_fields=("turnover_rate_f",),
            min_rows=5000,
            min_non_null_ratio=0.95,
        )


@pytest.mark.parametrize(
    "value",
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
def test_finite_numeric_value_rejects_all_supported_non_finite_forms(value: object) -> None:
    assert backfill._is_finite_numeric_value(value) is False


@pytest.mark.parametrize(
    "value",
    [Decimal("1.25"), np.float64("1.25"), np.int64(1), 1.25, "1.25"],
)
def test_finite_numeric_value_accepts_finite_numeric_forms(value: object) -> None:
    assert backfill._is_finite_numeric_value(value) is True


def test_snapshot_rows_and_source_codes_exclude_non_finite_provider_values() -> None:
    frame = _snapshot(rows=3)
    frame["turnover_rate_f"] = [Decimal("1.25"), "NaN", np.float64("inf")]

    rows = backfill.snapshot_rows(frame, dt.date(2026, 7, 13))

    assert rows[0][4] == Decimal("1.25")
    assert rows[1][4] is None
    assert rows[2][4] is None
    assert backfill._source_non_null_codes(frame, "turnover_rate_f") == {"000000.SZ"}


def test_preview_database_counts_postgres_numeric_special_values_as_missing() -> None:
    class Cursor:
        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, *_args: object) -> None:
            return None

        def fetchall(self) -> list[tuple[object, ...]]:
            return [
                ("000001.SZ", Decimal("NaN"), Decimal("1.25")),
                ("000002.SZ", Decimal("Infinity"), Decimal("-Infinity")),
                ("000003.SZ", Decimal("2.5"), None),
            ]

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

    preview = backfill.preview_database(
        Connection(),
        dt.date(2026, 7, 13),
        ("turnover_rate_f", "free_share"),
    )

    assert preview.stats.non_null == {"turnover_rate_f": 1, "free_share": 1}
    assert preview.missing_by_field == {"turnover_rate_f": 2, "free_share": 2}
    assert preview.non_null_codes_by_field["turnover_rate_f"] == {"000003.SZ"}
    assert preview.non_null_codes_by_field["free_share"] == {"000001.SZ"}


def test_parse_fields_rejects_unknown_columns() -> None:
    with pytest.raises(backfill.DailyBasicBackfillError, match="unsupported"):
        backfill.parse_fields("turnover_rate_f,not_a_column")


def test_verify_after_requires_every_provider_non_null_code() -> None:
    frame = _snapshot(rows=3)
    before = backfill.DatabasePreview(
        stats=backfill.SnapshotStats(
            row_count=3,
            non_null={"turnover_rate_f": 0, "volume_ratio": 0},
        ),
        existing_codes=set(frame["ts_code"].astype(str)),
        missing_by_field={"turnover_rate_f": 3, "volume_ratio": 3},
        non_null_codes_by_field={"turnover_rate_f": set(), "volume_ratio": set()},
        values_by_field={"turnover_rate_f": {}, "volume_ratio": {}},
    )
    after = backfill.DatabasePreview(
        stats=backfill.SnapshotStats(
            row_count=3,
            non_null={"turnover_rate_f": 2, "volume_ratio": 3},
        ),
        existing_codes=set(frame["ts_code"].astype(str)),
        missing_by_field={"turnover_rate_f": 1, "volume_ratio": 0},
        non_null_codes_by_field={
            "turnover_rate_f": set(frame["ts_code"].astype(str).iloc[:2]),
            "volume_ratio": set(frame["ts_code"].astype(str)),
        },
        values_by_field={
            "turnover_rate_f": {
                code: 1.0 for code in frame["ts_code"].astype(str).iloc[:2]
            },
            "volume_ratio": {code: 1.0 for code in frame["ts_code"].astype(str)},
        },
    )

    with pytest.raises(backfill.DailyBasicBackfillError, match="turnover_rate_f"):
        backfill.verify_after(frame, before, after, fields=backfill.DEFAULT_FIELDS)


def test_verify_after_rejects_wrong_filled_value() -> None:
    frame = _snapshot(rows=1)
    code = str(frame.iloc[0]["ts_code"])
    before = backfill.DatabasePreview(
        stats=backfill.SnapshotStats(row_count=1, non_null={"turnover_rate_f": 0}),
        existing_codes={code},
        missing_by_field={"turnover_rate_f": 1},
        non_null_codes_by_field={"turnover_rate_f": set()},
        values_by_field={"turnover_rate_f": {}},
    )
    after = backfill.DatabasePreview(
        stats=backfill.SnapshotStats(row_count=1, non_null={"turnover_rate_f": 1}),
        existing_codes={code},
        missing_by_field={"turnover_rate_f": 0},
        non_null_codes_by_field={"turnover_rate_f": {code}},
        values_by_field={"turnover_rate_f": {code: 2.0}},
    )

    with pytest.raises(backfill.DailyBasicBackfillError, match="differs from provider"):
        backfill.verify_after(frame, before, after, fields=("turnover_rate_f",))


def test_main_apply_requires_exact_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def fail_execute(**_kwargs: object) -> dict[str, object]:
        nonlocal called
        called = True
        raise AssertionError("apply guard must run before external access")

    monkeypatch.setattr(backfill, "execute_backfill", fail_execute)

    assert backfill.main(["--trade-date", "2026-07-13", "--apply"]) == 2
    assert called is False


def test_main_defaults_to_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_execute(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"schema_version": backfill.SCHEMA_VERSION, "status": "preview"}

    monkeypatch.setattr(backfill, "execute_backfill", fake_execute)

    assert backfill.main(["--trade-date", "2026-07-13"]) == 0
    assert captured["apply"] is False
    assert captured["fields"] == backfill.DEFAULT_FIELDS
