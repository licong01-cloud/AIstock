from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.dataset_release.artifact_ready_source import (
    ArtifactReadyProviderTerminal,
    ArtifactReadySourceBuilder,
    _bounded_tushare_records,
    _normalize_tushare_daily_row,
)
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.errors import DatasetReleaseError
from backend.services.dataset_release.minute_overlay import canonical_session_times


def _suspended_source(tmp_path, *, daily_present=True):
    day = date(2026, 9, 1)
    code = "002870.SZ"
    daily = {"ts_code": code, "trade_date": day.isoformat(), "open_li": 53000,
             "high_li": 53000, "low_li": 53000, "close_li": 53000,
             "volume_hand": 0, "amount_li": 0}
    minutes = [dict(daily, trade_time=stamp.isoformat(), freq="1m")
               for stamp in canonical_session_times(day)]
    minutes[119]["trade_time"] = f"{day.isoformat()}T13:00:00"
    minutes.sort(key=lambda row: row["trade_time"])
    data = {"kline_daily_raw": [daily] if daily_present else [], "kline_minute_raw": minutes}
    reads = []

    class View:
        def descriptors(self, dataset):
            return [{"dataset": dataset, "partition_key": "2026-09-01_2026-09-30"
                     + ("_bucket-0000" if dataset == "kline_minute_raw" else ""),
                     "content_digest": "a" * 64, "schema_digest": "b" * 64}]

        def iter_partition_rows(self, descriptor):
            reads.append(descriptor["dataset"])
            return iter(data[descriptor["dataset"]])

    cas = CASStore(ControlStore.initialize(tmp_path / "control").root)
    profile = SimpleNamespace(start_date=date(2018, 8, 1), minute_start_date=date(2018, 8, 1),
                              minute_code_bucket_count=1)
    snapshot = SimpleNamespace(pit_snapshot=SimpleNamespace(spans=[SimpleNamespace(
        ts_code=code, eligible_start=day, eligible_end=day)]))
    builder = ArtifactReadySourceBuilder(profile, cas, month_start=day)
    return builder, View(), snapshot, data, reads, (code, day)


@pytest.mark.parametrize("daily_present", [True, False])
def test_minute_preparation_preserves_proven_suspended_placeholder(tmp_path, daily_present):
    builder, view, snapshot, data, reads, key = _suspended_source(tmp_path, daily_present=daily_present)
    before = [dict(row) for row in data["kline_minute_raw"]]
    entries, providers, derived, summary = builder._minute_entries(
        view, snapshot=snapshot, trading_dates=(key[1],), suspended=frozenset({key}), checkpoint=lambda: None)
    coverage = builder.cas.get_json(entries[0]["rows_ref"])
    actual = coverage["days"][0]
    assert actual["status"] == "SUSPENDED_FULL_DAY"
    assert actual["database_rows"] == 240
    assert actual["excluded_zero_turnover_placeholder_rows"] == 240
    assert actual["final_rows"] == 0
    proof = builder.cas.get_json(actual["evidence_ref"])
    assert proof["raw_partition_content_digest"] == "a" * 64
    assert proof["daily_raw_row_count"] == int(daily_present)
    assert len(proof["source_rows_sha256"]) == 64
    assert providers == () and len(derived) == 2
    assert summary["suspended_full_day"] == 1
    assert summary["excluded_zero_turnover_placeholder_rows"] == 240
    assert reads == ["kline_daily_raw", "kline_minute_raw"]
    assert data["kline_minute_raw"] == before


@pytest.mark.parametrize("defect", [
    "daily_volume", "daily_amount", "daily_precision", "daily_duplicate", "minute_volume", "minute_amount",
    "minute_null", "minute_precision", "precision_provenance", "duplicate", "other_label", "price_null",
    "price_bounds", "too_many", "no_suspend", "no_daily_partition", "old_daily_partition_only",
])
def test_minute_preparation_cannot_exempt_unproven_rows(tmp_path, defect):
    builder, view, snapshot, data, _, key = _suspended_source(tmp_path)
    daily = data["kline_daily_raw"][0]
    at_1300 = next(row for row in data["kline_minute_raw"] if "T13:00:" in row["trade_time"])
    suspended = frozenset({key})
    if defect.startswith("daily_"):
        if defect == "daily_duplicate":
            data["kline_daily_raw"].append(dict(daily))
        elif defect == "daily_precision":
            daily.update(volume_shares=1, volume_shares_source="tushare_daily", volume_shares_sha256="c" * 64)
        else:
            daily["volume_hand" if defect == "daily_volume" else "amount_li"] = 1
    elif defect == "minute_precision":
        at_1300.update(volume_shares=1, volume_shares_source="tdx_decoded", volume_shares_sha256="c" * 64)
    elif defect == "precision_provenance":
        at_1300.update(volume_shares=0, volume_shares_source="tdx_decoded", volume_shares_sha256="invalid")
    elif defect.startswith("minute_"):
        at_1300["amount_li" if defect == "minute_amount" else "volume_hand"] = None if defect == "minute_null" else 1
    elif defect == "duplicate":
        data["kline_minute_raw"].append(dict(data["kline_minute_raw"][0]))
    elif defect == "other_label":
        data["kline_minute_raw"][0]["trade_time"] = f"{key[1].isoformat()}T09:00:00"
    elif defect == "price_null":
        at_1300["close_li"] = None
    elif defect == "price_bounds":
        at_1300["high_li"] = 1
    elif defect == "too_many":
        data["kline_minute_raw"].extend(dict(at_1300) for _ in range(2))
    elif defect == "no_suspend":
        suspended = frozenset()
    elif defect == "no_daily_partition":
        original = view.descriptors
        view.descriptors = lambda dataset: [] if dataset == "kline_daily_raw" else original(dataset)
    elif defect == "old_daily_partition_only":
        original = view.descriptors
        view.descriptors = lambda dataset: ([{"dataset": dataset, "partition_key": "2020-01-01_2020-01-31"}]
                                            if dataset == "kline_daily_raw" else original(dataset))
    data["kline_minute_raw"].sort(key=lambda row: row["trade_time"])
    with pytest.raises(DatasetReleaseError) as error:
        builder._minute_entries(view, snapshot=snapshot, trading_dates=(key[1],),
                                suspended=suspended, checkpoint=lambda: None)
    if defect not in {"no_suspend", "no_daily_partition", "old_daily_partition_only"}:
        assert error.value.context["ts_code"] == key[0]
        assert error.value.context["trade_date"] == key[1].isoformat()


def test_empty_suspended_source_keeps_formal_na_and_does_not_open_old_daily_partitions(tmp_path):
    builder, view, snapshot, data, reads, key = _suspended_source(tmp_path)
    data["kline_minute_raw"] = []
    original = view.descriptors
    view.descriptors = lambda dataset: ([{"dataset": dataset, "partition_key": "2020-01-01_2020-01-31"}]
                                        if dataset == "kline_daily_raw" else []) + original(dataset)
    entries, providers, _, summary = builder._minute_entries(
        view, snapshot=snapshot, trading_dates=(key[1],), suspended=frozenset({key}), checkpoint=lambda: None)
    actual = builder.cas.get_json(entries[0]["rows_ref"])["days"][0]
    assert actual["database_rows"] == actual["final_rows"] == 0
    assert summary["suspended_full_day"] == 1
    assert summary["excluded_zero_turnover_placeholder_rows"] == 0 and providers == ()
    assert reads == ["kline_daily_raw", "kline_minute_raw"]


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
