import datetime as dt
from dataclasses import dataclass

import pytest

from backend.services.event_signal.financial_event_backfill import (
    FinancialEventBackfillService,
    default_first_report_period,
    default_last_report_period,
    generate_report_periods,
    parse_date_or_period,
    period_to_tushare,
)


@dataclass(frozen=True)
class _FakeRawSummary:
    dataset: str
    period: str
    fetched_rows: int = 1
    written_rows: int = 1
    skipped_rows: int = 0


@dataclass(frozen=True)
class _FakeSignalSummary:
    run_id: str
    rule_version: str = "unified_event_signal_rules_v0_20260506"
    time_mode: str = "backtest"
    processed_rows: int = 1
    fact_rows: int = 1
    relation_rows: int = 0
    signal_rows: int = 1
    status: str = "SUCCESS"


class _FakeRawService:
    def __init__(self, *, fail_period: str | None = None) -> None:
        self.calls: list[tuple[str, str]] = []
        self.fail_period = fail_period

    def sync_period(self, dataset: str, *, period: str):
        self.calls.append((dataset, period))
        if period == self.fail_period:
            raise RuntimeError(f"raw failure for {period}")
        return _FakeRawSummary(dataset=dataset, period=period)


class _FakeSignalSync:
    def __init__(self, *, fail_period: dt.date | None = None) -> None:
        self.calls: list[dict] = []
        self.fail_period = fail_period

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs["report_period"] == self.fail_period:
            raise RuntimeError(f"signal failure for {kwargs['report_period']}")
        return _FakeSignalSummary(run_id=f"run-{period_to_tushare(kwargs['report_period'])}")


def test_parse_date_or_period_accepts_ymd_and_iso():
    assert parse_date_or_period("20231231") == dt.date(2023, 12, 31)
    assert parse_date_or_period("2023-12-31") == dt.date(2023, 12, 31)


def test_report_period_generation_includes_prior_period_for_august_baseline():
    periods = generate_report_periods(dt.date(2018, 8, 1), dt.date(2019, 4, 30))

    assert [period_to_tushare(period) for period in periods] == [
        "20180630",
        "20180930",
        "20181231",
        "20190331",
    ]


def test_default_period_bounds_use_source_announcement_window():
    assert default_first_report_period(dt.date(2018, 8, 1)) == dt.date(2018, 6, 30)
    assert default_last_report_period(dt.date(2024, 2, 1)) == dt.date(2023, 12, 31)
    assert default_last_report_period(dt.date(2024, 3, 31)) == dt.date(2024, 3, 31)


def test_backfill_runs_raw_datasets_then_financial_signal_generation():
    raw_service = _FakeRawService()
    signal_sync = _FakeSignalSync()
    service = FinancialEventBackfillService(raw_service=raw_service, signal_sync=signal_sync)

    summary = service.backfill(
        periods=[dt.date(2023, 9, 30), dt.date(2023, 12, 31)],
        raw_datasets=["forecast", "express"],
        time_mode="backtest",
        run_mode="smoke",
    )

    assert summary.success_periods == 2
    assert summary.failed_periods == 0
    assert raw_service.calls == [
        ("forecast", "20230930"),
        ("express", "20230930"),
        ("forecast", "20231231"),
        ("express", "20231231"),
    ]
    assert [call["report_period"] for call in signal_sync.calls] == [
        dt.date(2023, 9, 30),
        dt.date(2023, 12, 31),
    ]
    assert signal_sync.calls[0]["source_types"] == ["tushare_forecast", "tushare_express"]
    assert signal_sync.calls[0]["time_mode"] == "backtest"
    assert signal_sync.calls[0]["run_mode"] == "smoke"
    assert summary.results[0].raw_summaries[0]["dataset"] == "forecast"
    assert summary.results[0].signal_summary["run_id"] == "run-20230930"


def test_backfill_respects_skip_raw_skip_signals_and_max_periods():
    raw_service = _FakeRawService()
    signal_sync = _FakeSignalSync()
    service = FinancialEventBackfillService(raw_service=raw_service, signal_sync=signal_sync)

    summary = service.backfill(
        start_date=dt.date(2023, 8, 1),
        end_date=dt.date(2024, 4, 30),
        skip_raw=True,
        skip_signals=True,
        max_periods=2,
    )

    assert summary.periods == ["20230630", "20230930"]
    assert summary.success_periods == 2
    assert summary.results[0].raw_summaries == []
    assert summary.results[0].signal_summary is None
    assert raw_service.calls == []
    assert signal_sync.calls == []


def test_backfill_records_failed_periods_when_continue_on_error_is_true():
    raw_service = _FakeRawService(fail_period="20231231")
    signal_sync = _FakeSignalSync()
    service = FinancialEventBackfillService(raw_service=raw_service, signal_sync=signal_sync)

    summary = service.backfill(
        periods=[dt.date(2023, 9, 30), dt.date(2023, 12, 31)],
        raw_datasets=["forecast"],
        continue_on_error=True,
    )

    assert summary.success_periods == 1
    assert summary.failed_periods == 1
    assert summary.results[1].status == "FAILED"
    assert "raw failure for 20231231" in summary.results[1].error


def test_backfill_stops_after_first_failure_when_requested():
    raw_service = _FakeRawService(fail_period="20230930")
    signal_sync = _FakeSignalSync()
    service = FinancialEventBackfillService(raw_service=raw_service, signal_sync=signal_sync)

    summary = service.backfill(
        periods=[dt.date(2023, 9, 30), dt.date(2023, 12, 31)],
        raw_datasets=["forecast"],
        continue_on_error=False,
    )

    assert summary.success_periods == 0
    assert summary.failed_periods == 1
    assert len(summary.results) == 1
    assert raw_service.calls == [("forecast", "20230930")]
    assert signal_sync.calls == []


def test_backfill_rejects_unknown_dataset_and_bad_max_periods():
    service = FinancialEventBackfillService(raw_service=_FakeRawService(), signal_sync=_FakeSignalSync())

    with pytest.raises(ValueError, match="unknown raw datasets"):
        service.backfill(raw_datasets=["forecast", "unknown"])

    with pytest.raises(ValueError, match="max_periods must be positive"):
        service.backfill(max_periods=0)
