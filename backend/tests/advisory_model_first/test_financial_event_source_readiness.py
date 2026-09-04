from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

from backend.services.advisory_model_first.financial_event_source_readiness import (
    NOT_READY_STATE,
    PARENT_DECISION_DAY_COUNT,
    PARENT_INSTRUMENT_COUNT,
    PARENT_ROW_COUNT,
    READY_STATE,
    SourceThresholds,
    calculate_source_support,
    evaluate_readiness,
    load_parent_projection,
    project_earliest_raw_versions,
)
from backend.services.event_signal.tushare_event_raw_sync import source_row_hash


UTC = dt.timezone.utc


def _raw_row(
    *,
    key: str,
    raw_id: int,
    instrument: str,
    event_date: dt.date,
    report_period: dt.date,
    first_seen: dt.datetime,
    payload: dict,
) -> dict:
    return {
        "source_record_key": key,
        "raw_observation_id": raw_id,
        "source_row_hash": source_row_hash(payload),
        "ts_code": instrument,
        "ann_date": event_date,
        "report_period": report_period,
        "first_seen_at": first_seen,
        "raw_payload": payload,
    }


def _calendar() -> list[dt.date]:
    return [
        dt.date(2024, 7, 4),
        dt.date(2024, 7, 5),
        dt.date(2024, 7, 8),
        dt.date(2024, 7, 9),
        dt.date(2024, 7, 10),
    ]


def test_parent_reader_projects_only_allowed_columns(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "parent.parquet"
    frame = pd.DataFrame(
        {
            "arm_id": ["CURRENT_IC_PARENT", "OTHER"],
            "decision_as_of_trade_date": [dt.date(2024, 7, 8)] * 2,
            "instrument": ["000001.SZ", "000002.SZ"],
            "score": [1.0, 2.0],
            "economic_net_excess_bps": [999.0, -999.0],
        }
    )
    frame.to_parquet(path, index=False)
    original = pd.read_parquet
    observed: dict[str, object] = {}

    def spy(target: Path, *, columns: list[str]) -> pd.DataFrame:
        observed["columns"] = columns
        return original(target, columns=columns)

    monkeypatch.setattr(pd, "read_parquet", spy)
    parent, identity = load_parent_projection(path, expectation=None)
    assert observed["columns"] == [
        "arm_id",
        "decision_as_of_trade_date",
        "instrument",
        "score",
    ]
    assert list(parent["instrument"]) == ["000001.SZ"]
    assert "economic_net_excess_bps" not in parent
    assert identity["target_columns_read"] == []


def test_earliest_version_is_selected_and_revision_is_only_diagnostic() -> None:
    calendar = _calendar()
    first_seen = dt.datetime(2026, 5, 6, tzinfo=UTC)
    raw = {
        "tushare_forecast": [
            _raw_row(
                key="forecast-key",
                raw_id=2,
                instrument="000001.SZ",
                event_date=dt.date(2024, 7, 5),
                report_period=dt.date(2024, 6, 30),
                first_seen=first_seen + dt.timedelta(days=1),
                payload={"type": "首亏"},
            ),
            _raw_row(
                key="forecast-key",
                raw_id=1,
                instrument="000001.SZ",
                event_date=dt.date(2024, 7, 5),
                report_period=dt.date(2024, 6, 30),
                first_seen=first_seen,
                payload={"type": "预增", "p_change_min": 60, "p_change_max": 80},
            ),
        ],
        "tushare_express": [],
        "tushare_fina_indicator": [],
    }
    projection, revision = project_earliest_raw_versions(
        raw,
        trading_calendar=calendar,
        source_start=dt.date(2024, 7, 4),
        source_end=dt.date(2024, 7, 10),
    )
    assert len(projection) == 1
    assert projection.iloc[0]["raw_observation_id"] == 1
    assert projection.iloc[0]["event_type"] == "financial_forecast_large_growth"
    assert projection.iloc[0]["effective_trade_date"] == dt.date(2024, 7, 8)
    source = revision["sources"]["tushare_forecast"]
    assert source["multi_version_key_count"] == 1
    assert source["event_type_drift_key_count"] == 1


def test_same_day_event_is_not_visible_until_next_trading_day() -> None:
    raw = {
        "tushare_forecast": [
            _raw_row(
                key="forecast-key",
                raw_id=1,
                instrument="000001.SZ",
                event_date=dt.date(2024, 7, 8),
                report_period=dt.date(2024, 6, 30),
                first_seen=dt.datetime(2026, 5, 6, tzinfo=UTC),
                payload={"type": "首亏"},
            )
        ],
        "tushare_express": [],
        "tushare_fina_indicator": [],
    }
    projection, _ = project_earliest_raw_versions(
        raw,
        trading_calendar=_calendar(),
        source_start=dt.date(2024, 7, 4),
        source_end=dt.date(2024, 7, 10),
    )
    assert projection.iloc[0]["effective_trade_date"] == dt.date(2024, 7, 9)


def test_future_poison_outside_source_end_does_not_change_projection() -> None:
    base = _raw_row(
        key="forecast-key",
        raw_id=1,
        instrument="000001.SZ",
        event_date=dt.date(2024, 7, 5),
        report_period=dt.date(2024, 6, 30),
        first_seen=dt.datetime(2026, 5, 6, tzinfo=UTC),
        payload={"type": "首亏"},
    )
    poison = _raw_row(
        key="future-key",
        raw_id=2,
        instrument="000001.SZ",
        event_date=dt.date(2024, 7, 10),
        report_period=dt.date(2024, 9, 30),
        first_seen=dt.datetime(2026, 5, 7, tzinfo=UTC),
        payload={"type": "预增", "p_change_min": 100},
    )
    kwargs = {
        "trading_calendar": _calendar(),
        "source_start": dt.date(2024, 7, 4),
        "source_end": dt.date(2024, 7, 9),
    }
    one, _ = project_earliest_raw_versions(
        {"tushare_forecast": [base], "tushare_express": [], "tushare_fina_indicator": []},
        **kwargs,
    )
    two, _ = project_earliest_raw_versions(
        {"tushare_forecast": [base, poison], "tushare_express": [], "tushare_fina_indicator": []},
        **kwargs,
    )
    pd.testing.assert_frame_equal(one, two)


def test_neutral_disclosure_is_seen_but_is_not_qualifying() -> None:
    parent = pd.DataFrame(
        {
            "decision_as_of_trade_date": [dt.date(2024, 7, 8)],
            "instrument": ["000001.SZ"],
            "score": [1.0],
            "parent_rank": [1],
        }
    )
    projection = pd.DataFrame(
        {
            "instrument": ["000001.SZ"],
            "effective_trade_date": [dt.date(2024, 7, 8)],
            "source_type": ["tushare_forecast"],
            "should_signal": [False],
        }
    )
    _, support = calculate_source_support(parent, projection, trading_calendar=_calendar())
    assert support["lookbacks"]["disclosure"]["0"]["top20_fraction"] == 1.0
    assert support["lookbacks"]["qualifying"]["0"]["top20_fraction"] == 0.0


def test_readiness_routes_ready_and_revision_drift_routes_not_ready() -> None:
    projection = pd.DataFrame(
        {
            "source_type": [
                "tushare_forecast",
                "tushare_express",
                "tushare_fina_indicator",
            ],
            "report_period": [dt.date(2023, 6, 30)] * 3,
            "should_signal": [True, True, True],
            "severity_score": [0.2, 0.2, 0.2],
            "confidence": [0.6, 0.6, 0.6],
        }
    )
    # Add all required period/source combinations without affecting economic evidence.
    projection = pd.concat(
        [
            projection,
            pd.DataFrame(
                {
                    "source_type": [source for source in projection.source_type for _ in range(10)],
                    "report_period": [
                        period
                        for _source in projection.source_type
                        for period in (
                            dt.date(2023, 9, 30),
                            dt.date(2023, 12, 31),
                            dt.date(2024, 3, 31),
                            dt.date(2024, 6, 30),
                            dt.date(2024, 9, 30),
                            dt.date(2024, 12, 31),
                            dt.date(2025, 3, 31),
                            dt.date(2025, 6, 30),
                            dt.date(2025, 9, 30),
                            dt.date(2025, 12, 31),
                        )
                    ],
                    "should_signal": [True] * 30,
                    "severity_score": [0.2] * 30,
                    "confidence": [0.6] * 30,
                }
            ),
        ],
        ignore_index=True,
    )
    identity = {
        "row_count": PARENT_ROW_COUNT,
        "decision_day_count": PARENT_DECISION_DAY_COUNT,
        "instrument_count": PARENT_INSTRUMENT_COUNT,
    }
    support = {
        "lookbacks": {"disclosure": {"120": {"top20_fraction": 1.0}}},
        "daily": {
            "top20_supported_days_ge_min": 386,
            "top20_disclosure_120d_min": 20,
            "top50_mixed_qualifying_days": 386,
        },
    }
    revision = {
        "sources": {
            source: {"event_type_drift_fraction": 0.0}
            for source in ("tushare_forecast", "tushare_express", "tushare_fina_indicator")
        }
    }
    zero_thresholds = SourceThresholds(min_projection_rows=0, min_qualifying_rows=0)
    state, failures = evaluate_readiness(
        parent_identity=identity,
        projection=projection,
        support=support,
        revision_report=revision,
        diagnostic_pit_mismatch_count=0,
        thresholds=zero_thresholds,
    )
    assert state == READY_STATE
    assert failures == ()

    revision["sources"]["tushare_forecast"]["event_type_drift_fraction"] = 0.5
    state, failures = evaluate_readiness(
        parent_identity=identity,
        projection=projection,
        support=support,
        revision_report=revision,
        diagnostic_pit_mismatch_count=0,
        thresholds=zero_thresholds,
    )
    assert state == NOT_READY_STATE
    assert "EVENT_TYPE_REVISION_DRIFT:tushare_forecast" in failures
