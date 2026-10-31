from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.dataset_release.artifact_ready_source import (
    ArtifactReadyProviderTerminal,
    ArtifactReadySourceBuilder,
    _FrozenProviderReplay,
    _bounded_tushare_records,
    _normalize_tushare_daily_row,
)
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.errors import DatasetReleaseError
from backend.services.dataset_release.minute_overlay import canonical_session_times


def _adj_factor_provider_case(tmp_path, *, extras=(), required=1.0, duplicate=False, missing=False, overlap=False, outside_pit=False, native_adapter=False, provider_override=None):
    day = date(2026, 10, 22)
    code = "301718.SZ"
    codes = [code, "000001.SZ"] if overlap or outside_pit else [code]
    values = {"adj_factor": ([{"ts_code": "000001.SZ", "trade_date": day, "adj_factor": 1.0}] if overlap or outside_pit else []),
              "adj_factor_construction": [],
              "kline_daily_raw": [{"ts_code": item, "trade_date": day,
                  "open_li": 1000, "high_li": 1000, "low_li": 1000, "close_li": 1000,
                  "volume_hand": 1, "amount_li": 100000} for item in codes]}

    class View:
        def descriptors(self, dataset):
            return [{"dataset": dataset, "partition_key": "2026-10-01_2026-10-30",
                     "content_digest": "a" * 64, "schema_digest": "b" * 64}]

        def iter_partition_rows(self, descriptor):
            return iter(values[descriptor["dataset"]])

    rows = [] if missing else [{"ts_code": code, "trade_date": day, "adj_factor": required}]
    if duplicate:
        rows += [dict(rows[0])]
    if overlap:
        rows += [{"ts_code": "000001.SZ", "trade_date": day, "adj_factor": 2.0}]
    rows += list(extras)
    cas = CASStore(ControlStore.initialize(tmp_path / "control").root)
    calls = []

    def provider(stamp):
        calls.append(stamp)
        return rows

    builder = ArtifactReadySourceBuilder(SimpleNamespace(start_date=date(2018, 8, 1)), cas,
        fetch_tushare_adj_factor_rows=None if native_adapter else provider_override or provider,
        month_start=date(2026, 10, 1) if outside_pit else None)
    if native_adapter:
        def dated_response(**kwargs):
            assert kwargs == {"trade_date": "20261022", "fields": "ts_code,trade_date,adj_factor"}
            calls.append(day)
            return pd.DataFrame(rows, columns=["ts_code", "trade_date", "adj_factor"])

        builder._tushare = SimpleNamespace(provider=lambda: SimpleNamespace(adj_factor=dated_response))
    pit_codes = ["000001.SZ"] if outside_pit else codes
    snapshot = SimpleNamespace(official_cutoff=date(2026, 10, 30), pit_snapshot_digest="c" * 64,
                               pit_snapshot=SimpleNamespace(spans=[SimpleNamespace(ts_code=c) for c in pit_codes]))
    result = builder._adj_factor_entries(View(), snapshot=snapshot, checkpoint=lambda: None)
    return builder, result, calls


@pytest.mark.parametrize("code", ["920072.BJ", "200001.SZ", "900901.SH", "510300.SH", "000002.SZ"])
def test_provider_projects_valid_unrelated_identity_before_economic_validation(tmp_path, code):
    builder, result, calls = _adj_factor_provider_case(tmp_path, extras=[{"ts_code": code, "trade_date": None, "adj_factor": None}])
    _, refs, _, summary, _ = result
    assert calls == [date(2026, 10, 22)]
    assert summary["provider_fill_rows"] == 1
    assert builder.cas.get_json(refs[0])["rows"] == [
        {"ts_code": "301718.SZ", "trade_date": "2026-10-22", "adj_factor": 1.0}]


@pytest.mark.parametrize("value", [None, 0, -1, float("nan"), float("inf")])
def test_provider_still_rejects_invalid_required_values(tmp_path, value):
    with pytest.raises(DatasetReleaseError):
        _adj_factor_provider_case(tmp_path, required=value)


@pytest.mark.parametrize("defect", ["duplicate", "missing", "overlap"])
def test_provider_still_rejects_required_missing_duplicate_or_conflict(tmp_path, defect):
    with pytest.raises(DatasetReleaseError):
        _adj_factor_provider_case(tmp_path, **{defect: True}, extras=[{"ts_code": "920072.BJ", "adj_factor": None}])


@pytest.mark.parametrize("row", [{}, {"ts_code": "broken"}, {"ts_code": "000001.HK"}, None])
def test_provider_malformed_identity_is_not_silently_ignored(tmp_path, row):
    with pytest.raises(DatasetReleaseError):
        _adj_factor_provider_case(tmp_path, extras=[row])


def test_required_factual_ipo_outside_buy_pit_keeps_its_factor(tmp_path):
    builder, result, _ = _adj_factor_provider_case(tmp_path, outside_pit=True,
                              extras=[{"ts_code": "920072.BJ", "adj_factor": None}])
    _, _, _, summary, ref = result
    assert summary["provider_fill_rows"] == 1
    assert {item["ts_code"] for item in builder.cas.get_json(ref)["values"]} == {"000001.SZ", "301718.SZ"}


@pytest.mark.parametrize("code", ["920072.BJ", "200001.SZ", "900901.SH", "510300.SH", "000002.SZ"])
@pytest.mark.parametrize("defect", ["null_date", "nan_factor", "infinite_factor", "null_both"])
def test_native_factor_adapter_projects_unrelated_rows_before_value_checks(tmp_path, code, defect):
    extra = {"ts_code": code, "trade_date": date(2026, 10, 22), "adj_factor": 1.0}
    if defect in {"null_date", "null_both"}:
        extra["trade_date"] = None
    if defect in {"nan_factor", "null_both"}:
        extra["adj_factor"] = None
    if defect == "infinite_factor":
        extra["adj_factor"] = float("inf")
    builder, result, calls = _adj_factor_provider_case(tmp_path, extras=[extra], native_adapter=True)
    _, refs, _, summary, _ = result
    assert calls == [date(2026, 10, 22)]
    assert summary["provider_fill_rows"] == 1
    assert builder.cas.get_json(refs[0])["rows"] == [
        {"ts_code": "301718.SZ", "trade_date": "2026-10-22", "adj_factor": 1.0}]


@pytest.mark.parametrize("value", [None, 0, -1, float("nan"), float("inf")])
def test_native_factor_adapter_keeps_required_values_strict(tmp_path, value):
    with pytest.raises(DatasetReleaseError):
        _adj_factor_provider_case(tmp_path, required=value, native_adapter=True)


@pytest.mark.parametrize("defect", ["duplicate", "missing", "overlap"])
def test_native_factor_adapter_keeps_required_keys_strict(tmp_path, defect):
    with pytest.raises(DatasetReleaseError):
        _adj_factor_provider_case(tmp_path, native_adapter=True, **{defect: True})


@pytest.mark.parametrize("code", [None, "broken", "000001.HK"])
def test_native_factor_adapter_rejects_malformed_identity(tmp_path, code):
    with pytest.raises(DatasetReleaseError):
        _adj_factor_provider_case(tmp_path, native_adapter=True, extras=[
            {"ts_code": code, "trade_date": date(2026, 10, 22), "adj_factor": 1.0}])


def test_native_factor_adapter_keeps_ipo_fact_outside_buy_pit(tmp_path):
    builder, result, _ = _adj_factor_provider_case(tmp_path, native_adapter=True, outside_pit=True,
        extras=[{"ts_code": "920072.BJ", "trade_date": None, "adj_factor": None},
                {"ts_code": "000001.SZ", "trade_date": date(2026, 10, 22), "adj_factor": 1.0}])
    _, refs, _, summary, ref = result
    assert summary["provider_fill_rows"] == 1
    assert summary["overlap_rows_verified"] == 1
    assert {item["ts_code"] for item in builder.cas.get_json(ref)["values"]} == {"000001.SZ", "301718.SZ"}
    assert {item["ts_code"] for item in builder.cas.get_json(refs[0])["rows"]} == {"000001.SZ", "301718.SZ"}


def test_native_factor_adapter_seals_projected_rows_for_offline_replay(tmp_path):
    builder, result, _ = _adj_factor_provider_case(tmp_path, native_adapter=True, extras=[
        {"ts_code": "920072.BJ", "trade_date": None, "adj_factor": None}])
    replay = _FrozenProviderReplay(builder.cas, result[1])
    replay_builder, replay_result, calls = _adj_factor_provider_case(
        tmp_path, provider_override=replay.fetch_tushare_adj_factor_rows)
    assert calls == []
    assert result == replay_result
    assert builder.cas.get_json(result[4]) == replay_builder.cas.get_json(replay_result[4])


@pytest.mark.parametrize("defect", ["row_count", "columns", "required_date"])
def test_factor_frame_projection_does_not_bypass_response_contract(defect):
    frame = pd.DataFrame([
        {"ts_code": "301718.SZ", "trade_date": "20261022", "adj_factor": 1.0},
        {"ts_code": "920072.BJ", "trade_date": None, "adj_factor": None},
    ])
    if defect == "columns":
        frame["unexpected"] = 1
    if defect == "required_date":
        frame.loc[0, "trade_date"] = "20261021"
    with pytest.raises(ArtifactReadyProviderTerminal):
        _bounded_tushare_records(
            frame, dataset="adj_factor", expected_columns=("ts_code", "trade_date", "adj_factor"),
            date_column="trade_date", start=date(2026, 10, 22), end=date(2026, 10, 22),
            max_rows=1 if defect == "row_count" else 2, stock_codes=frozenset({"301718.SZ"}))


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
    # One independent factual-population read, one suspension proof read.
    assert reads == ["kline_daily_raw", "kline_daily_raw", "kline_minute_raw"]
    assert data["kline_minute_raw"] == before


def test_minute_source_keeps_observed_never_pit_security(tmp_path):
    builder, view, snapshot, data, _, key = _suspended_source(tmp_path)
    other = "002871.SZ"
    day = key[1]
    data["kline_daily_raw"][0].update(volume_hand=240, amount_li=240000)
    first = data["kline_daily_raw"][0]
    data["kline_daily_raw"].append({**first, "ts_code": other})
    data["kline_minute_raw"] = [
        {**first, "ts_code": code, "trade_time": stamp.isoformat(), "freq": "1m",
         "volume_hand": 1, "amount_li": 1000}
        for code in (key[0], other) for stamp in canonical_session_times(day)
    ]
    entries, providers, _, summary = builder._minute_entries(
        view, snapshot=snapshot, trading_dates=(day,), suspended=frozenset(), checkpoint=lambda: None)
    assert summary["expected_days"] == summary["database_complete"] == 2
    assert summary["pit_excluded_stock_days"] == 0
    assert providers == ()
    coverage = builder.cas.get_json(entries[0]["rows_ref"])
    assert {row["ts_code"] for row in coverage["days"]} == {key[0], other}
    assert {span.ts_code for span in snapshot.pit_snapshot.spans} == {key[0]}


@pytest.mark.parametrize("defect", [
    "daily_volume", "daily_amount", "daily_precision", "daily_duplicate", "minute_volume", "minute_amount",
    "minute_null", "minute_precision", "duplicate", "other_label", "price_null",
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
    assert reads == ["kline_daily_raw", "kline_daily_raw", "kline_minute_raw"]


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
