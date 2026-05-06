import datetime as dt
import uuid

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
