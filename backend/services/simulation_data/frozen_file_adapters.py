"""Thin native contract adapters over the shared frozen-file reader."""
from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal
from functools import wraps
import json
import math

import pandas as pd

from backend.data_service.frozen_dataset import FrozenDataset, FrozenDatasetError, digest, raw_price_li
from backend.services.selection_center.industry_provider import IndustryInfo
from backend.services.selection_center.suspension_provider import project_selection_suspension_days
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.simulation_data.contracts import DailySuspendStatus, MinuteDataSource, TradingCalendarSnapshot
from backend.services.simulation_data.minute_provider import SimulationMinuteMarketDataProvider
from backend.services.strategy_package.reference_prices import load_reference_prices
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.limit_price_provider import DailyLimitPrice


def no_market_sql():
    raise DataUnavailableError("file market input cannot access a database", context={"reason_code": "FILE_MARKET_SQL_FORBIDDEN"})


def _file_errors(function):
    @wraps(function)
    def call(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except FrozenDatasetError as exc:
            raise DataUnavailableError(str(exc), context=exc.context) from exc
    return call


class FileTradingCalendar:
    def __init__(self, dataset: FrozenDataset):
        self.dataset = dataset

    def ensure_trading_day(self, trade_date):
        self.dataset.require_day(trade_date)

    def list_trading_days(self, start_date, end_date):
        if start_date < self.dataset.calendar[0] or end_date > self.dataset.calendar[-1]:
            raise FrozenDatasetError("requested calendar interval exceeds frozen release")
        self.dataset.assert_unchanged()
        return [day for day in self.dataset.calendar if start_date <= day <= end_date]

    def latest_trading_day_on_or_before(self, as_of_date):
        if as_of_date > self.dataset.calendar[-1]:
            raise FrozenDatasetError("frozen calendar cannot prove the latest completed day")
        return next((day for day in reversed(self.dataset.calendar) if day <= as_of_date), None)

    def snapshot(self, trade_date):
        self.dataset.require_day(trade_date)
        days = self.dataset.calendar
        index = days.index(trade_date)
        return TradingCalendarSnapshot.build(trade_date=trade_date, is_trading_day=True,
            previous_trading_date=days[index - 1] if index else None,
            next_trading_date=days[index + 1] if index + 1 < len(days) else None,
            source="dataset_files.day_calendar:" + self.dataset.identity)


class FileLimitPriceProvider:
    def __init__(self, dataset):
        self.dataset = dataset

    def get_limit_price(self, symbol, trade_date):
        return self.get_limit_prices([symbol], trade_date)[symbol]

    @_file_errors
    def get_limit_prices(self, symbols, trade_date):
        result = {}
        for symbol in symbols:
            previous, upper, lower = (self.dataset.daily_value(symbol, field, trade_date)
                                      for field in ("prev_close", "up_limit_price", "down_limit_price"))
            # These three fields are native raw CNY, stored as float32. Restore
            # the declared li precision before native price/limit comparisons.
            previous, upper, lower = (float(raw_price_li(value, Decimal(1)) / 1000) if value is not None else None
                                      for value in (previous, upper, lower))
            if previous is not None and (previous <= 0 or upper is None or lower is None or not 0 < lower < previous < upper):
                raise DataUnavailableError("native file limit/pre_close values are invalid", context={
                    "reason_code": "STK_LIMIT_PRE_CLOSE_INVALID", "symbol": symbol, "trade_date": str(trade_date),
                    "file_binding_hash": self.dataset.identity})
            result[symbol] = DailyLimitPrice(symbol=symbol, trade_date=trade_date,
                                             pre_close=previous, up_limit=upper, down_limit=lower)
        return result


class _FileSuspensionLookup(dict):
    def __init__(self, receipt):
        super().__init__()
        self.receipt = receipt


class FileSuspendProvider:
    def __init__(self, dataset):
        self.dataset = dataset

    @_file_errors
    def get_suspended_symbols(self, symbols, trade_date):
        rows = self.dataset.suspension_rows(symbols, trade_date, trade_date)
        projected, receipt = project_selection_suspension_days(
            [dict(zip(("ts_code", "trade_date", "suspend_type", "suspend_timing"), row)) for row in rows],
            symbols=symbols, required_dates=[trade_date], usage="D_EFFECTIVE_MARKET_STATE")
        if any(row["state"] == "UNRESOLVED" for row in projected):
            raise DataUnavailableError("file suspension day is unresolved", context={"reason_code": "SUSPENSION_DAY_UNRESOLVED"})
        result = _FileSuspensionLookup(receipt)
        for row in projected:
            if row["state"] in {"FULL_DAY_SUSPENDED", "INTRADAY_SUSPENDED"}:
                timings = [fact[3] for fact in rows if fact[0] == row["ts_code"] and fact[2] == "S"]
                result[row["ts_code"]] = {"suspend_type": "S", "suspend_timing": next((t for t in timings if t), None),
                    "source": "dataset_files.suspend_d", "suspension_day_authority": row}
        return result

    def get_suspend_status(self, symbol, trade_date):
        value = self.get_suspended_symbols([symbol], trade_date).get(symbol)
        return DailySuspendStatus(symbol=symbol, trade_date=trade_date, is_suspended=value is not None,
                                  source="dataset_files.suspend_d", suspend_type="S" if value else None,
                                  suspend_timing=value.get("suspend_timing") if value else None)


class FileIndustryProvider:
    def __init__(self, dataset):
        self.dataset = dataset

    @_file_errors
    def get_industries(self, symbols, trade_date):
        frame = pd.read_parquet(self.dataset.file("sector_membership_spans"),
                                filters=[("instrument", "in", symbols)])
        frame["start_date"] = pd.to_datetime(frame.start_date).dt.date
        frame["end_date"] = pd.to_datetime(frame.end_date).dt.date
        frame = frame.loc[(frame.start_date <= trade_date) & (frame.end_date >= trade_date)]
        mapping = json.loads(self.dataset.file("sector_code_map").read_text(encoding="utf-8-sig"))
        codes = {int(row["l2_code_id"]): str(row["canonical_l2_code"]) for row in mapping["entries"]}
        result = {}
        for symbol, group in frame.groupby("instrument"):
            ids = set(int(value) for value in group.l2_code_id)
            if len(ids) != 1 or next(iter(ids)) not in codes:
                raise FrozenDatasetError("PIT industry file is missing or conflicting", symbol=symbol)
            result[symbol] = IndustryInfo(symbol=symbol, l2_code=codes[next(iter(ids))],
                in_date=min(group.start_date), out_date=max(group.end_date), source="dataset_files.SW_L2_FILE_V1")
        self.dataset.assert_unchanged()
        return result

    def require_policy_coverage(self, blacklist):
        mapping = json.loads(self.dataset.file("sector_code_map").read_text(encoding="utf-8-sig"))
        available = {str(row["canonical_l2_code"]).strip().upper() for row in mapping["entries"]}
        unsupported = [value for value in blacklist or [] if str(value).strip().upper() not in available]
        if unsupported:
            raise DataUnavailableError("file release does not cover the native industry policy", context={
                "reason_code": "FROZEN_DATASET_INDUSTRY_POLICY_UNCOVERED", "uncovered_policy_values": unsupported})


class FileTradabilityFilter(TradabilityFilter):
    """Validate available industry dimensions, then invoke the unique filter."""

    def __init__(self, dataset):
        super().__init__(FileSuspendProvider(dataset), FileIndustryProvider(dataset))

    @_file_errors
    def filter_candidates_with_receipt(self, **kwargs):
        self.industry_provider.require_policy_coverage(kwargs.get("industry_blacklist"))
        return super().filter_candidates_with_receipt(**kwargs)


@_file_errors
def file_reference_prices(dataset, *, symbols, as_of_date, **_kwargs):
    prices = load_reference_prices(symbols=symbols, as_of_date=as_of_date, row_reader=dataset.reference_rows)
    for observation in prices.observations.values():
        observation["file_binding_hash"] = dataset.identity
    return prices


class FileMinuteMarketDataProvider(SimulationMinuteMarketDataProvider):
    def __init__(self, dataset):
        self.dataset = dataset
        # No default ST fixture is installed. Historical file bars consume the
        # authoritative explicit limit prices and never infer ST from a ratio.
        super().__init__(limit_price_provider=FileLimitPriceProvider(dataset),
                         suspend_status_provider=FileSuspendProvider(dataset),
                         st_status_provider=self, conn_factory=no_market_sql)

    def get_st_status(self, symbol, trade_date):
        raise FrozenDatasetError("file release has no native ST daily-status component", symbol=symbol, trade_date=str(trade_date))

    def _load_raw_bars(self, symbol, trade_date, source):
        if source is not MinuteDataSource.DATASET_FILES:
            raise DataUnavailableError("file minute provider requires DATASET_FILES")
        if trade_date >= date.today():
            raise DataUnavailableError("file historical source cannot read a current/future day")
        try:
            return self.dataset.raw_minute_rows(symbol, trade_date)
        except FrozenDatasetError as exc:
            raise DataUnavailableError(str(exc), context=exc.context) from exc

    def _stk_limit_price_with_required_pre_close(self, symbol, trade_date):
        value = self.limit_price_provider.get_limit_price(symbol, trade_date)
        if value.pre_close is None or not math.isfinite(value.pre_close) or value.pre_close <= 0:
            raise DataUnavailableError("file pre_close is required and cannot be derived", context={
                "reason_code": "STK_LIMIT_PRE_CLOSE_INVALID", "symbol": symbol, "trade_date": str(trade_date),
                "file_binding_hash": self.dataset.identity})
        return value, "dataset_files.prev_close", "dataset_files.limit_price"

    @_file_errors
    def load_symbol_input(self, **kwargs):
        if kwargs["source"] is not MinuteDataSource.DATASET_FILES or kwargs["trade_date"] >= date.today():
            raise DataUnavailableError("file history requires a completed day and DATASET_FILES", context={
                "reason_code": "CURRENT_DAY_HISTORICAL_MINUTE_FORBIDDEN", "trade_date": str(kwargs["trade_date"])})
        result = super().load_symbol_input(**kwargs)
        context = dict(result.market_context)
        context["file_binding_hash"] = self.dataset.identity
        context["file_read_receipts_hash"] = digest(self.dataset.read_receipts)
        return replace(result, market_context=context)
