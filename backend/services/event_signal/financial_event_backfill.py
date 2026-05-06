"""Historical backfill orchestration for Tushare financial event signals."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Optional

from dotenv import load_dotenv

from backend.services.event_signal.financial_event_adapter import (
    sync_financial_event_signals,
)
from backend.services.event_signal.tushare_event_raw_sync import TushareEventRawSyncService


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_START_DATE = dt.date(2018, 8, 1)
RAW_DATASET_TO_SOURCE_TYPE = {
    "forecast": "tushare_forecast",
    "express": "tushare_express",
    "fina_indicator": "tushare_fina_indicator",
}


@dataclass(frozen=True)
class PeriodBackfillResult:
    period: str
    raw_summaries: list[dict] = field(default_factory=list)
    signal_summary: Optional[dict] = None
    status: str = "SUCCESS"
    error: Optional[str] = None


@dataclass(frozen=True)
class BackfillSummary:
    start_date: str
    end_date: str
    periods: list[str]
    raw_datasets: list[str]
    time_mode: str
    run_mode: str
    skip_raw: bool
    skip_signals: bool
    success_periods: int
    failed_periods: int
    results: list[PeriodBackfillResult]


def _json_dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def parse_date_or_period(value: str) -> dt.date:
    text = value.strip()
    if len(text) == 8 and text.isdigit():
        return dt.date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return dt.date.fromisoformat(text)


def quarter_end_for_date(value: dt.date) -> dt.date:
    if value.month <= 3:
        return dt.date(value.year, 3, 31)
    if value.month <= 6:
        return dt.date(value.year, 6, 30)
    if value.month <= 9:
        return dt.date(value.year, 9, 30)
    return dt.date(value.year, 12, 31)


def previous_quarter_end(value: dt.date) -> dt.date:
    if value.month <= 3:
        return dt.date(value.year - 1, 12, 31)
    if value.month <= 6:
        return dt.date(value.year, 3, 31)
    if value.month <= 9:
        return dt.date(value.year, 6, 30)
    return dt.date(value.year, 9, 30)


def next_quarter_end(value: dt.date) -> dt.date:
    if value.month <= 3:
        return dt.date(value.year, 6, 30)
    if value.month <= 6:
        return dt.date(value.year, 9, 30)
    if value.month <= 9:
        return dt.date(value.year, 12, 31)
    return dt.date(value.year + 1, 3, 31)


def default_first_report_period(start_date: dt.date) -> dt.date:
    """Include the report period whose announcements can appear after start_date.

    For the project baseline 2018-08-01 this returns 2018-06-30, because
    2018H1 announcements can be published after August 1.
    """

    return previous_quarter_end(start_date)


def default_last_report_period(end_date: dt.date) -> dt.date:
    return previous_quarter_end(end_date) if quarter_end_for_date(end_date) > end_date else quarter_end_for_date(end_date)


def generate_report_periods(start_date: dt.date, end_date: dt.date) -> list[dt.date]:
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")
    current = default_first_report_period(start_date)
    last = default_last_report_period(end_date)
    periods: list[dt.date] = []
    while current <= last:
        periods.append(current)
        current = next_quarter_end(current)
    return periods


def period_to_tushare(period: dt.date) -> str:
    return period.strftime("%Y%m%d")


class FinancialEventBackfillService:
    """Coordinate raw Tushare period pulls and derived financial signal runs."""

    def __init__(
        self,
        *,
        raw_service: Optional[TushareEventRawSyncService] = None,
        signal_sync: Callable = sync_financial_event_signals,
    ) -> None:
        self.raw_service = raw_service or TushareEventRawSyncService()
        self.signal_sync = signal_sync

    def backfill(
        self,
        *,
        start_date: dt.date = DEFAULT_START_DATE,
        end_date: Optional[dt.date] = None,
        periods: Optional[list[dt.date]] = None,
        raw_datasets: Optional[list[str]] = None,
        time_mode: str = "backtest",
        run_mode: str = "backfill",
        skip_raw: bool = False,
        skip_signals: bool = False,
        continue_on_error: bool = True,
        max_periods: Optional[int] = None,
    ) -> BackfillSummary:
        if end_date is None:
            end_date = dt.date.today()
        datasets = raw_datasets or list(RAW_DATASET_TO_SOURCE_TYPE)
        unknown = [dataset for dataset in datasets if dataset not in RAW_DATASET_TO_SOURCE_TYPE]
        if unknown:
            raise ValueError(f"unknown raw datasets: {unknown}")
        selected_periods = list(periods) if periods is not None else generate_report_periods(start_date, end_date)
        if max_periods is not None:
            if max_periods <= 0:
                raise ValueError("max_periods must be positive when provided")
            selected_periods = selected_periods[:max_periods]

        source_types = [RAW_DATASET_TO_SOURCE_TYPE[dataset] for dataset in datasets]
        results: list[PeriodBackfillResult] = []
        for period in selected_periods:
            period_text = period_to_tushare(period)
            raw_summaries: list[dict] = []
            try:
                if not skip_raw:
                    for dataset in datasets:
                        raw_summary = self.raw_service.sync_period(dataset, period=period_text)
                        raw_summaries.append(asdict(raw_summary))
                signal_summary = None
                if not skip_signals:
                    signal_summary = asdict(
                        self.signal_sync(
                            time_mode=time_mode,
                            run_mode=run_mode,
                            report_period=period,
                            source_types=source_types,
                        )
                    )
                results.append(
                    PeriodBackfillResult(
                        period=period_text,
                        raw_summaries=raw_summaries,
                        signal_summary=signal_summary,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                result = PeriodBackfillResult(
                    period=period_text,
                    raw_summaries=raw_summaries,
                    signal_summary=None,
                    status="FAILED",
                    error=str(exc),
                )
                results.append(result)
                if not continue_on_error:
                    break

        success_periods = sum(1 for item in results if item.status == "SUCCESS")
        failed_periods = sum(1 for item in results if item.status != "SUCCESS")
        return BackfillSummary(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            periods=[period_to_tushare(period) for period in selected_periods],
            raw_datasets=datasets,
            time_mode=time_mode,
            run_mode=run_mode,
            skip_raw=skip_raw,
            skip_signals=skip_signals,
            success_periods=success_periods,
            failed_periods=failed_periods,
            results=results,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Tushare financial raw rows and unified event signals")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE.isoformat())
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--period", action="append", default=None, help="Explicit report period YYYYMMDD; can repeat")
    parser.add_argument("--dataset", action="append", choices=sorted(RAW_DATASET_TO_SOURCE_TYPE), default=None)
    parser.add_argument("--time-mode", choices=["backtest", "paper", "live", "observed"], default="backtest")
    parser.add_argument("--run-mode", choices=["backfill", "incremental", "smoke", "repair", "research"], default="backfill")
    parser.add_argument("--skip-raw", action="store_true")
    parser.add_argument("--skip-signals", action="store_true")
    parser.add_argument("--stop-on-error", action="store_true")
    parser.add_argument("--max-periods", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(ROOT / ".env", override=True)
    load_dotenv(override=False)
    periods = [parse_date_or_period(item) for item in args.period] if args.period else None
    service = FinancialEventBackfillService()
    summary = service.backfill(
        start_date=parse_date_or_period(args.start_date),
        end_date=parse_date_or_period(args.end_date) if args.end_date else None,
        periods=periods,
        raw_datasets=args.dataset,
        time_mode=args.time_mode,
        run_mode=args.run_mode,
        skip_raw=args.skip_raw,
        skip_signals=args.skip_signals,
        continue_on_error=not args.stop_on_error,
        max_periods=args.max_periods,
    )
    print(_json_dumps(asdict(summary)))
    return 1 if summary.failed_periods else 0


if __name__ == "__main__":
    raise SystemExit(main())
