import datetime as dt
import uuid

import pytest
import backend.services.event_signal.tushare_event_raw_sync as raw_sync

from backend.services.event_signal.tushare_event_raw_sync import (
    DATASET_CONFIGS,
    build_raw_values,
    fetch_period_rows,
    normalize_row,
    parse_tushare_date,
    source_record_key,
    source_row_hash,
)


class _FakeDataFrame:
    empty = False

    def __init__(self, rows):
        self._rows = rows

    def iterrows(self):
        for idx, row in enumerate(self._rows):
            yield idx, row


class _FakePro:
    def forecast_vip(self, **params):
        self.params = params
        if params.get("offset", 0):
            return _FakeDataFrame([])
        return _FakeDataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "ann_date": "20240131",
                    "end_date": "20231231",
                    "type": "pre_increase",
                    "p_change_min": 50.0,
                    "p_change_max": 80.0,
                    "first_ann_date": "20240131",
                }
            ]
        )


def test_parse_tushare_date_accepts_ymd_and_iso():
    assert parse_tushare_date("20240131") == dt.date(2024, 1, 31)
    assert parse_tushare_date("2024-01-31") == dt.date(2024, 1, 31)


def test_source_row_hash_is_stable_after_normalization():
    left = normalize_row({"b": 2, "a": 1})
    right = normalize_row({"a": 1, "b": 2})

    assert source_row_hash(left) == source_row_hash(right)


def test_source_record_key_uses_business_identity_not_source_api():
    config = DATASET_CONFIGS["forecast"]
    payload = {
        "ts_code": "000001.SZ",
        "ann_date": "20240131",
        "end_date": "20231231",
        "type": "pre_increase",
        "first_ann_date": "20240131",
    }

    assert source_record_key(config, payload) == (
        "tushare_forecast_raw:000001.SZ:20240131:20231231:type=pre_increase:first_ann_date=20240131"
    )


def test_fetch_period_rows_requests_vip_fields_and_returns_normalized_rows():
    pro = _FakePro()
    api_name, fetch_params, rows = fetch_period_rows(
        pro,
        DATASET_CONFIGS["forecast"],
        period="20231231",
    )

    assert api_name == "forecast_vip"
    assert fetch_params["period"] == "20231231"
    assert "ts_code,ann_date,end_date,type" in fetch_params["fields"]
    assert rows[0]["ts_code"] == "000001.SZ"
    assert rows[0]["ann_date"] == "20240131"


def test_build_raw_values_skips_rows_missing_required_dates():
    config = DATASET_CONFIGS["forecast"]
    rows = [
        {
            "ts_code": "000001.SZ",
            "ann_date": "20240131",
            "end_date": "20231231",
            "type": "pre_increase",
        },
        {"ts_code": "000002.SZ", "ann_date": None, "end_date": "20231231"},
    ]

    values, skipped = build_raw_values(
        config,
        rows,
        source_api="forecast_vip",
        fetch_params={"period": "20231231"},
        observed_at=dt.datetime(2026, 5, 6, tzinfo=dt.timezone.utc),
    )

    assert skipped == 1
    assert len(values) == 1
    assert values[0][0] == "forecast_vip"
    assert values[0][2].startswith("tushare_forecast_raw:000001.SZ:20240131:20231231")
    assert values[0][3] == "000001.SZ"
    assert values[0][4] == dt.date(2024, 1, 31)
    assert values[0][5] == dt.date(2023, 12, 31)
    assert values[0][7].adapted["type"] == "pre_increase"


def test_build_raw_values_deduplicates_same_source_key_and_hash_in_one_batch():
    config = DATASET_CONFIGS["forecast"]
    row = {
        "ts_code": "000001.SZ",
        "ann_date": "20240131",
        "end_date": "20231231",
        "type": "pre_increase",
    }

    values, skipped = build_raw_values(
        config,
        [dict(row), dict(row)],
        source_api="forecast_vip",
        fetch_params={"period": "20231231"},
        observed_at=dt.datetime(2026, 5, 6, tzinfo=dt.timezone.utc),
    )

    assert len(values) == 1
    assert skipped == 1


def test_build_raw_values_serializes_uuid_job_id_for_psycopg2():
    config = DATASET_CONFIGS["forecast"]
    job_id = uuid.uuid4()

    values, skipped = build_raw_values(
        config,
        [
            {
                "ts_code": "000001.SZ",
                "ann_date": "20240131",
                "end_date": "20231231",
                "type": "pre_increase",
            }
        ],
        source_api="forecast_vip",
        fetch_params={"period": "20231231"},
        observed_at=dt.datetime(2026, 5, 6, tzinfo=dt.timezone.utc),
        job_id=job_id,
    )

    assert skipped == 0
    assert values[0][9] == str(job_id)
    assert values[0][10] == str(job_id)
    assert values[0][11] == str(job_id)


def test_financial_pages_keep_all_versions_until_terminal_empty(monkeypatch):
    calls = []
    row = {"ts_code": "002475.SZ", "ann_date": "20261030", "end_date": "20261231"}
    pages = [[row], [{**row, "eps": 2.0}], []]

    def query(api, params, fields, *, token, with_page_info=False):
        calls.append(params)
        return pages.pop(0), None

    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    monkeypatch.setattr("backend.services.tushare_sync_engine._query_tushare_dataapi", query)
    _, params, rows = fetch_period_rows(None, DATASET_CONFIGS["forecast"], period="20261231")
    assert len(rows) == 2
    assert [p["offset"] for p in calls] == [0, 1, 2]
    assert params["pagination"]["terminal_empty"] is True


def test_financial_offset_ignored_is_failure_not_complete(monkeypatch):
    row = {"ts_code": "002475.SZ", "ann_date": "20261030", "end_date": "20261231"}
    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    monkeypatch.setattr("backend.services.tushare_sync_engine._query_tushare_dataapi", lambda *a, **kw: ([row], True))
    with pytest.raises(RuntimeError, match="repeated page"):
        fetch_period_rows(None, DATASET_CONFIGS["forecast"], period="20261231")


@pytest.mark.parametrize("dataset,params,report", [
    ("forecast", {"start_date": "20261030", "end_date": "20261030"}, "20271231"),
    ("express", {"start_date": "20261030", "end_date": "20261030"}, "20241231"),
    ("fina_indicator", {"ann_date": "20261030"}, "20181231"),
])
def test_announcement_queries_use_api_specific_clock(monkeypatch, dataset, params, report):
    calls = []
    row = {"ts_code": "002475.SZ", "ann_date": "20261030", "end_date": report}

    def query(api, actual, fields, *, token, with_page_info=False):
        calls.append(actual)
        return ([row] if actual["offset"] == 0 else []), None

    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    monkeypatch.setattr("backend.services.tushare_sync_engine._query_tushare_dataapi", query)
    _, _, rows = raw_sync.fetch_announcement_rows(None, DATASET_CONFIGS[dataset],
        start_date=dt.date(2026, 10, 30), end_date=dt.date(2026, 10, 30))
    assert rows == [row]
    assert {k:v for k,v in calls[0].items() if k not in ("offset", "limit")} == params


def test_fina_announcement_query_rejects_multi_day_report_date_substitution():
    with pytest.raises(ValueError, match="one announcement date"):
        raw_sync.fetch_announcement_rows(None, DATASET_CONFIGS["fina_indicator"],
            start_date=dt.date(2026, 10, 29), end_date=dt.date(2026, 10, 30))


def test_announcement_wrong_day_response_cannot_be_recorded_as_empty(monkeypatch):
    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    pages = [[{"ts_code": "002475.SZ", "ann_date": "20261029", "end_date": "20260930"}], []]
    monkeypatch.setattr("backend.services.tushare_sync_engine._query_tushare_dataapi", lambda *a, **kw: (pages.pop(0), None))
    with pytest.raises(ValueError, match="announcement date"):
        raw_sync.fetch_announcement_rows(None, DATASET_CONFIGS["fina_indicator"],
            start_date=dt.date(2026, 10, 30), end_date=dt.date(2026, 10, 30))


def test_later_page_failure_never_opens_database(monkeypatch):
    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    opened = []

    def query(api, params, fields, **kwargs):
        if params["offset"]:
            raise RuntimeError("page two network failure")
        return [{"ts_code": "002475.SZ", "ann_date": "20261030", "end_date": "20261231"}], True

    monkeypatch.setattr("backend.services.tushare_sync_engine._query_tushare_dataapi", query)
    monkeypatch.setattr(raw_sync, "get_conn", lambda **kw: opened.append(kw))
    with pytest.raises(RuntimeError, match="page two"):
        raw_sync.TushareEventRawSyncService().sync_ann_window("forecast",
            start_date=dt.date(2026, 10, 30), end_date=dt.date(2026, 10, 30))
    assert opened == []


def test_empty_with_has_more_is_not_success(monkeypatch):
    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    monkeypatch.setattr("backend.services.tushare_sync_engine._query_tushare_dataapi", lambda *a, **kw: ([], True))
    with pytest.raises(RuntimeError, match="empty page"):
        fetch_period_rows(None, DATASET_CONFIGS["forecast"], period="20261231")


@pytest.mark.parametrize("inserted_rows", [[], [(dt.date(2026, 10, 30),)]])
def test_raw_write_count_uses_insert_returning_not_attempts(monkeypatch, inserted_rows):
    from backend.tests.test_tushare_sync_engine import _FakeConn

    conn = _FakeConn()
    markers = []
    monkeypatch.setattr("backend.services.data_integrity.repository.mark_changed",
        lambda *a, **kw: markers.append(a[2]))
    monkeypatch.setattr(raw_sync.psycopg2.extras, "execute_values",
        lambda cur, query, values, **kw: inserted_rows if "RETURNING ann_date" in query else None)
    dates = []
    written, _ = raw_sync.upsert_raw_rows(conn, DATASET_CONFIGS["forecast"],
        [{"ts_code": "002475.SZ", "ann_date": "20261030", "end_date": "20261231"}],
        source_api="forecast_vip", fetch_params={}, changed_dates=dates)
    assert written == len(inserted_rows)
    assert dates == [row[0] for row in inserted_rows]
    assert conn.autocommit is True
    assert conn.commits == 1
    assert markers == [sorted(set(dates))]
