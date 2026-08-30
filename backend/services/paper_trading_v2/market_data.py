"""Minute market data input builder for Paper Trading v2.

The provider is read-only. It does not modify ``backend/data_service`` and it
does not silently fall back between data sources. The caller must choose the
source explicitly; missing minute bars, limit prices, or previous close fail
the run.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import TYPE_CHECKING, Any, Mapping


from backend.data_service.tdx_adapter import fetch_minute_kline_tdx
from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import (
    DataUnavailableError,
)
from backend.services.trading_core.limit_price_provider import (
    DailyLimitPrice,
    StkLimitPriceProvider,
)
from backend.services.trading_core.models import MinuteBar
from backend.services.simulation_data.contracts import (
    MINUTE_VOLUME_HAND_SIZE,
    PRICE_TICK,
    PRICE_UNIT_DIVISOR,
    ConnFactory,
    DailyStStatus,
    DailySuspendStatus,
    LimitPriceProvider,
    MinuteDataSource,
    MinuteExecutionMarketInput,
    StStatusProvider,
    SuspendStatusProvider,
    TdxMinuteFetcher,
)
from backend.services.simulation_data.daily_context_provider import (
    DbStStatusProvider,
    DbSuspendStatusProvider,
)
from backend.services.simulation_data.tdx_causal_minute import (
    _require_st_status,
)

if TYPE_CHECKING:
    pass


class _InjectedLimitProviderStStatusProvider:
    """Keep legacy unit-test limit-provider fixtures deterministic without DB access."""

    def get_st_status(self, symbol: str, trade_date: date) -> DailyStStatus:
        return DailyStStatus(
            symbol=symbol,
            trade_date=trade_date,
            is_st=False,
            source="injected_limit_provider.non_st_unit_test_fixture",
        )


class PaperV2MinuteMarketDataProvider:
    """Build strict minute execution inputs from TDX or historical DB data."""

    def __init__(
        self,
        *,
        limit_price_provider: LimitPriceProvider | None = None,
        suspend_status_provider: SuspendStatusProvider | None = None,
        st_status_provider: StStatusProvider | None = None,
        tdx_fetcher: TdxMinuteFetcher | None = None,
        conn_factory: ConnFactory | None = None,
    ) -> None:
        self.conn_factory = conn_factory or get_conn
        self.limit_price_provider = limit_price_provider or StkLimitPriceProvider()
        self.suspend_status_provider = suspend_status_provider or DbSuspendStatusProvider(
            conn_factory=self.conn_factory
        )
        if st_status_provider is not None:
            self.st_status_provider = st_status_provider
        elif limit_price_provider is not None and not isinstance(limit_price_provider, StkLimitPriceProvider):
            self.st_status_provider = _InjectedLimitProviderStStatusProvider()
        else:
            self.st_status_provider = DbStStatusProvider(conn_factory=self.conn_factory)
        self.tdx_fetcher = tdx_fetcher or fetch_minute_kline_tdx

    def load_symbol_input(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource = MinuteDataSource.TDX_REALTIME,
        min_bars: int = 1,
        require_suspend_status: bool = False,
        frozen_daily_fact: Mapping[str, Any] | None = None,
    ) -> MinuteExecutionMarketInput:
        symbol = str(symbol or "").strip()
        if not symbol:
            raise DataUnavailableError("symbol is required for minute market data")
        if min_bars <= 0:
            raise DataUnavailableError(
                "min_bars must be positive",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "min_bars": min_bars},
            )

        if source == MinuteDataSource.TDX_REALTIME:
            if frozen_daily_fact is not None:
                (
                    limit_price,
                    suspend_status,
                    frozen_reference,
                    pre_close_source,
                    limit_price_source,
                ) = self._frozen_realtime_daily_inputs(
                    symbol=symbol,
                    trade_date=trade_date,
                    frozen_daily_fact=frozen_daily_fact,
                )
            else:
                # Explicit completed-day Paper v2 compatibility adapter. The
                # scheduler-owned LocalSIM hot path never calls this method;
                # it uses load_observed_intraday with a frozen symbol fact.
                limit_price, pre_close_source, limit_price_source = self._stk_limit_price_with_required_pre_close(
                    symbol, trade_date
                )
                suspend_status = None
                if require_suspend_status:
                    if self.suspend_status_provider is None:
                        raise DataUnavailableError(
                            "suspend status provider is required",
                            context={"symbol": symbol, "trade_date": trade_date.isoformat()},
                        )
                    suspend_status = self.suspend_status_provider.get_suspend_status(symbol, trade_date)
                frozen_reference = None
        else:
            limit_price, pre_close_source, limit_price_source = self._stk_limit_price_with_required_pre_close(
                symbol,
                trade_date,
            )
            suspend_status = None
            if require_suspend_status:
                if self.suspend_status_provider is None:
                    raise DataUnavailableError(
                        "suspend status provider is required",
                        context={"symbol": symbol, "trade_date": trade_date.isoformat()},
                    )
                suspend_status = self.suspend_status_provider.get_suspend_status(symbol, trade_date)

        raw_bars = self._load_raw_bars(symbol, trade_date, source)
        minute_bars = self._build_minute_bars(
            symbol=symbol,
            trade_date=trade_date,
            raw_bars=raw_bars,
            limit_price=limit_price,
            source=source,
            require_suspend_status=require_suspend_status,
            suspend_status=suspend_status,
        )
        if len(minute_bars) < min_bars:
            raise DataUnavailableError(
                "insufficient minute bars for requested execution context",
                context={
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "source": source.value,
                    "bar_count": len(minute_bars),
                    "min_bars": min_bars,
                },
            )

        context = self._build_market_context(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=minute_bars,
            limit_price=limit_price,
            pre_close_source=pre_close_source,
            limit_price_source=limit_price_source,
            suspend_status=suspend_status,
        )
        if source == MinuteDataSource.TDX_REALTIME and frozen_reference is not None:
            context["daily_trading_context"] = frozen_reference
        return MinuteExecutionMarketInput(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=minute_bars,
            market_context=context,
        )

    def load_completed_day(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        expected_bars: int,
        require_suspend_status: bool = False,
    ) -> MinuteExecutionMarketInput:
        """Load a completed historical day with no realtime fallback."""

        if source != MinuteDataSource.DB_HISTORICAL:
            raise DataUnavailableError(
                "completed-day minute feed requires an explicit historical DB source",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "source": source.value},
            )
        return self.load_symbol_input(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            min_bars=expected_bars,
            require_suspend_status=require_suspend_status,
        )

    def load_observed_intraday(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        until_time: datetime,
        require_suspend_status: bool = False,
        frozen_daily_fact: Mapping[str, Any] | None = None,
    ) -> MinuteExecutionMarketInput:
        """Load only observed intraday bars up to ``until_time``.

        Empty observed bars are returned as an explicit waiting input. Fetch
        failures still raise; this method never falls back to historical DB.
        """

        symbol = str(symbol or "").strip()
        if not symbol:
            raise DataUnavailableError("symbol is required for observed intraday minute feed")
        if source != MinuteDataSource.TDX_REALTIME:
            raise DataUnavailableError(
                "observed intraday minute feed requires TDX_REALTIME",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "source": source.value},
            )
        if until_time.date() != trade_date:
            raise DataUnavailableError(
                "observed intraday until_time must match trade_date",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "until_time": until_time.isoformat()},
            )

        (
            limit_price,
            suspend_status,
            frozen_reference,
            pre_close_source,
            limit_price_source,
        ) = self._frozen_realtime_daily_inputs(
            symbol=symbol,
            trade_date=trade_date,
            frozen_daily_fact=frozen_daily_fact,
        )

        raw_bars = self._load_raw_bars_from_tdx(symbol, trade_date, allow_empty=True)
        minute_bars = self._build_minute_bars(
            symbol=symbol,
            trade_date=trade_date,
            raw_bars=raw_bars,
            limit_price=limit_price,
            source=source,
            require_suspend_status=require_suspend_status,
            suspend_status=suspend_status,
        )
        until_cmp = self._naive_for_compare(until_time)
        observed = [bar for bar in minute_bars if self._naive_for_compare(bar.bar_time) <= until_cmp]
        context = self._build_market_context(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=observed,
            limit_price=limit_price,
            pre_close_source=pre_close_source,
            limit_price_source=limit_price_source,
            suspend_status=suspend_status,
        )
        context["daily_trading_context"] = frozen_reference
        context["until_time"] = until_time.isoformat()
        context["feed_mode"] = "observed_intraday"
        return MinuteExecutionMarketInput(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=observed,
            market_context=context,
        )

    @staticmethod
    def _frozen_realtime_daily_inputs(
        *,
        symbol: str,
        trade_date: date,
        frozen_daily_fact: Mapping[str, Any] | None,
    ) -> tuple[DailyLimitPrice, DailySuspendStatus, dict[str, Any], str, str]:
        if not isinstance(frozen_daily_fact, Mapping):
            raise DataUnavailableError(
                "LocalSIM live minute feed requires a frozen daily trading fact",
                context={
                    "reason_code": "LOCALSIM_DAILY_TRADING_FACT_MISSING",
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                },
            )
        reference = dict(frozen_daily_fact)
        raw_fact = reference.get("symbol_fact")
        schema_version = reference.get("schema_version")
        if (
            schema_version not in {"daily_trading_context_reference_v1", "daily_trading_context_reference_v2"}
            or not reference.get("context_id")
            or not reference.get("context_hash")
            or not isinstance(raw_fact, Mapping)
        ):
            raise DataUnavailableError(
                "LocalSIM frozen daily trading fact reference is invalid",
                context={
                    "reason_code": "LOCALSIM_DAILY_TRADING_FACT_INVALID",
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                },
            )
        try:
            from backend.services.simulation_data.daily_context import (
                DailyTradingAuthorityStateV2,
                DailyTradingSymbolFactV1,
                DailyTradingSymbolFactV2,
                SimulationBrokerBackend,
            )

            if schema_version == "daily_trading_context_reference_v1":
                if reference.get("source") != "market.stk_limit":
                    raise ValueError("V1 daily trading reference requires market.stk_limit")
                fact = DailyTradingSymbolFactV1.model_validate(dict(raw_fact))
            else:
                fact = DailyTradingSymbolFactV2.model_validate(dict(raw_fact))
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSIM frozen daily trading symbol fact is invalid",
                context={
                    "reason_code": "LOCALSIM_DAILY_TRADING_FACT_INVALID",
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                },
            ) from exc
        if fact.symbol != symbol or fact.trade_date != trade_date:
            raise DataUnavailableError(
                "LocalSIM frozen daily trading symbol fact identity conflicts with the stream",
                context={
                    "reason_code": "LOCALSIM_DAILY_TRADING_FACT_IDENTITY_CONFLICT",
                    "symbol": symbol,
                    "fact_symbol": fact.symbol,
                    "trade_date": trade_date.isoformat(),
                    "fact_trade_date": fact.trade_date.isoformat(),
                },
            )
        reference_conflict = reference.get("trade_date") != trade_date.isoformat()
        if isinstance(fact, DailyTradingSymbolFactV1):
            reference_conflict = reference_conflict or reference.get("stk_limit_row_hash") != fact.stk_limit_row_hash
            pre_close_source = f"{fact.pre_close_source}:frozen_daily_trading_context_v1"
            limit_price_source = "market.stk_limit:frozen_daily_trading_context_v1"
        else:
            reference_conflict = reference_conflict or (
                reference.get("broker_backend") != SimulationBrokerBackend.LOCAL_SIM.value
                or reference.get("authority_state") != fact.authority_state.value
                or reference.get("limit_authority") != fact.limit_authority.value
                or reference.get("source_evidence_hash") != fact.source_evidence_hash
            )
            pre_close_source = f"{fact.limit_authority.value}:frozen_daily_trading_context_v2"
            limit_price_source = pre_close_source
            if fact.authority_state is DailyTradingAuthorityStateV2.NO_DAILY_LIMIT:
                limit_price_source = f"{limit_price_source}:no_daily_limit"
        if reference_conflict:
            raise DataUnavailableError(
                "LocalSIM frozen daily trading fact reference conflicts with its symbol fact",
                context={
                    "reason_code": "LOCALSIM_DAILY_TRADING_FACT_IDENTITY_CONFLICT",
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                },
            )
        if (
            isinstance(fact, DailyTradingSymbolFactV2)
            and fact.authority_state is DailyTradingAuthorityStateV2.SYMBOL_FAILED
        ):
            raise DataUnavailableError(
                "LocalSIM frozen daily trading authority is unavailable for the symbol",
                context={
                    "reason_code": str(fact.authority_reason_code),
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                },
            )
        return (
            DailyLimitPrice(
                symbol=symbol,
                trade_date=trade_date,
                pre_close=fact.pre_close,
                up_limit=fact.up_limit,
                down_limit=fact.down_limit,
            ),
            DailySuspendStatus(
                symbol=symbol,
                trade_date=trade_date,
                is_suspended=fact.is_suspended,
                suspend_type=fact.suspend_type,
                suspend_timing=fact.suspend_timing,
                source=fact.suspend_source,
            ),
            {key: deepcopy(value) for key, value in reference.items() if key != "context"},
            pre_close_source,
            limit_price_source,
        )

    def load_new_bars(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        after_time: datetime | None,
        until_time: datetime,
        require_suspend_status: bool = False,
        frozen_daily_fact: Mapping[str, Any] | None = None,
    ) -> list[MinuteBar]:
        """Load new observed bars after a persisted cursor."""

        observed = self.load_observed_intraday(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            until_time=until_time,
            require_suspend_status=require_suspend_status,
            frozen_daily_fact=frozen_daily_fact,
        ).minute_bars
        if after_time is None:
            return observed
        after_cmp = self._naive_for_compare(after_time)
        return [bar for bar in observed if self._naive_for_compare(bar.bar_time) > after_cmp]

    def latest_available_bar_time(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        source: MinuteDataSource,
        as_of_time: datetime,
    ) -> datetime | None:
        """Return the latest common observed bar time across symbols."""

        normalized_symbols = [str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()]
        if not normalized_symbols:
            raise DataUnavailableError(
                "symbols are required for latest available minute bar time",
                context={"trade_date": trade_date.isoformat(), "source": source.value},
            )
        if source != MinuteDataSource.TDX_REALTIME:
            raise DataUnavailableError(
                "latest available live minute bar time requires TDX_REALTIME",
                context={"trade_date": trade_date.isoformat(), "source": source.value},
            )
        as_of_cmp = self._naive_for_compare(as_of_time)
        latest_by_symbol: list[datetime] = []
        for symbol in normalized_symbols:
            raw_bars = self._load_raw_bars_from_tdx(symbol, trade_date, allow_empty=True)
            bar_times = []
            for raw in raw_bars:
                bar_time = raw.get("time") or raw.get("bar_time") or raw.get("trade_time")
                if not isinstance(bar_time, datetime):
                    raise DataUnavailableError(
                        "minute bar time is missing or invalid",
                        context={"symbol": symbol, "trade_date": trade_date.isoformat(), "source": source.value},
                    )
                if bar_time.date() != trade_date:
                    raise DataUnavailableError(
                        "minute bar date does not match requested trade_date",
                        context={
                            "symbol": symbol,
                            "trade_date": trade_date.isoformat(),
                            "bar_time": bar_time.isoformat(),
                            "source": source.value,
                        },
                    )
                if self._naive_for_compare(bar_time) <= as_of_cmp:
                    bar_times.append(bar_time)
            if not bar_times:
                return None
            latest_by_symbol.append(max(bar_times))
        return min(latest_by_symbol)

    def _load_raw_bars(
        self,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
    ) -> list[dict[str, Any]]:
        if source == MinuteDataSource.TDX_REALTIME:
            return self._load_raw_bars_from_tdx(symbol, trade_date, allow_empty=False)

        if source == MinuteDataSource.DB_HISTORICAL:
            return self._load_raw_bars_from_db(symbol, trade_date)

        raise DataUnavailableError(
            "unsupported minute data source",
            context={"symbol": symbol, "source": str(source)},
        )

    def _load_raw_bars_from_tdx(
        self,
        symbol: str,
        trade_date: date,
        *,
        allow_empty: bool,
    ) -> list[dict[str, Any]]:
        try:
            raw_bars = self.tdx_fetcher(symbol, trade_date)
        except Exception as exc:
            raise DataUnavailableError(
                "TDX minute data fetch failed",
                context={"symbol": symbol, "trade_date": trade_date.isoformat()},
            ) from exc
        if not raw_bars and not allow_empty:
            raise DataUnavailableError(
                "TDX returned no minute bars",
                context={"symbol": symbol, "trade_date": trade_date.isoformat()},
            )
        return raw_bars or []

    def _load_raw_bars_from_db(self, symbol: str, trade_date: date) -> list[dict[str, Any]]:
        if trade_date >= date.today():
            raise DataUnavailableError(
                "historical minute source is forbidden for the current or future calendar day",
                context={
                    "reason_code": "CURRENT_DAY_HISTORICAL_MINUTE_FORBIDDEN",
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "current_date": date.today().isoformat(),
                },
            )
        with self.conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT trade_time, open_li, high_li, low_li, close_li,
                           volume_hand, amount_li
                    FROM market.kline_minute_raw
                    WHERE ts_code = %s
                      AND trade_time >= %s::date
                      AND trade_time < %s::date + interval '1 day'
                    ORDER BY trade_time ASC
                    """,
                    (symbol, trade_date, trade_date),
                )
                rows = cur.fetchall()
        if not rows:
            raise DataUnavailableError(
                "historical DB returned no minute bars",
                context={"symbol": symbol, "trade_date": trade_date.isoformat()},
            )
        return [
            {
                "time": row[0],
                "open": self._positive_price_from_li(row[1], "open_li", symbol, trade_date),
                "high": self._positive_price_from_li(row[2], "high_li", symbol, trade_date),
                "low": self._positive_price_from_li(row[3], "low_li", symbol, trade_date),
                "close": self._positive_price_from_li(row[4], "close_li", symbol, trade_date),
                "volume": row[5],
                "amount": float(row[6]) / PRICE_UNIT_DIVISOR if row[6] is not None else None,
            }
            for row in rows
        ]

    def _build_minute_bars(
        self,
        *,
        symbol: str,
        trade_date: date,
        raw_bars: list[dict[str, Any]],
        limit_price: DailyLimitPrice,
        source: MinuteDataSource,
        require_suspend_status: bool = False,
        suspend_status: DailySuspendStatus | None = None,
    ) -> list[MinuteBar]:
        minute_bars: list[MinuteBar] = []
        for raw in raw_bars:
            bar_time = raw.get("time") or raw.get("bar_time") or raw.get("trade_time")
            if not isinstance(bar_time, datetime):
                raise DataUnavailableError(
                    "minute bar time is missing or invalid",
                    context={"symbol": symbol, "trade_date": trade_date.isoformat(), "source": source.value},
                )
            if bar_time.date() != trade_date:
                raise DataUnavailableError(
                    "minute bar date does not match requested trade_date",
                    context={
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                        "bar_time": bar_time.isoformat(),
                        "source": source.value,
                    },
                )
            minute_bars.append(
                MinuteBar(
                    symbol=symbol,
                    bar_time=bar_time,
                    open=self._positive_float(raw.get("open"), "open", symbol, bar_time),
                    high=self._positive_float(raw.get("high"), "high", symbol, bar_time),
                    low=self._positive_float(raw.get("low"), "low", symbol, bar_time),
                    close=self._positive_float(raw.get("close"), "close", symbol, bar_time),
                    volume=self._volume_hands_to_shares(raw.get("volume"), symbol, bar_time),
                    amount=self._optional_non_negative_float(raw.get("amount"), "amount", symbol, bar_time),
                    is_suspended=(
                        bool(suspend_status.is_suspended)
                        if suspend_status is not None
                        else self._parse_suspend_status(
                            raw,
                            symbol=symbol,
                            bar_time=bar_time,
                            require_suspend_status=require_suspend_status,
                        )
                    ),
                    limit_up=limit_price.up_limit,
                    limit_down=limit_price.down_limit,
                )
            )

        minute_bars.sort(key=lambda item: item.bar_time)
        for prev, cur in zip(minute_bars, minute_bars[1:]):
            if cur.bar_time <= prev.bar_time:
                raise DataUnavailableError(
                    "minute bars must be strictly increasing",
                    context={
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                        "bar_time": cur.bar_time.isoformat(),
                        "source": source.value,
                    },
                )
        return minute_bars

    def _build_market_context(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        minute_bars: list[MinuteBar],
        limit_price: DailyLimitPrice,
        pre_close_source: str,
        limit_price_source: str,
        suspend_status: DailySuspendStatus | None = None,
    ) -> dict[str, Any]:
        # The V24 implementation currently consumes these legacy "full_day_*"
        # names. In realtime TDX mode they mean "observed bars so far"; callers
        # can enforce min_bars=31 before invoking V24.
        context = {
            "stock_id": symbol,
            "trade_date": trade_date.isoformat(),
            "data_source": source.value,
            "price_basis": "raw",
            "limit_price_basis": "raw",
            "prev_close_basis": "raw",
            "generated_at": datetime.now(UTC).isoformat(),
            "observed_bar_count": len(minute_bars),
            "observed_only": source == MinuteDataSource.TDX_REALTIME,
            "prev_close": limit_price.pre_close,
            "prev_close_source": pre_close_source,
            "limit_up": limit_price.up_limit,
            "limit_down": limit_price.down_limit,
            "limit_price_source": limit_price_source,
            "suspend_status": (
                None
                if suspend_status is None
                else {
                    "is_suspended": suspend_status.is_suspended,
                    "suspend_type": suspend_status.suspend_type,
                    "suspend_timing": suspend_status.suspend_timing,
                    "source": suspend_status.source,
                }
            ),
            "full_day_open": [bar.open for bar in minute_bars],
            "full_day_close": [bar.close for bar in minute_bars],
            "full_day_volume": [bar.volume for bar in minute_bars],
            "full_day_high": [bar.high for bar in minute_bars],
            "full_day_low": [bar.low for bar in minute_bars],
        }
        return context

    def _stk_limit_price_with_required_pre_close(
        self, symbol: str, trade_date: date
    ) -> tuple[DailyLimitPrice, str, str]:
        limit_price = self.limit_price_provider.get_limit_price(symbol, trade_date)
        if limit_price.pre_close is not None and float(limit_price.pre_close) > 0:
            return limit_price, "market.stk_limit.pre_close", "market.stk_limit.limit_price"
        raise DataUnavailableError(
            "market.stk_limit pre_close is required and cannot be derived",
            context={
                "reason_code": "STK_LIMIT_PRE_CLOSE_INVALID",
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "source": "market.stk_limit",
            },
        )

    def _required_st_status(self, symbol: str, trade_date: date, *, context_source: str) -> DailyStStatus:
        return _require_st_status(
            self.st_status_provider,
            symbol=symbol,
            trade_date=trade_date,
            context_source=context_source,
        )

    @staticmethod
    def _round_price_tick(value: float) -> float:
        return float(Decimal(str(value)).quantize(PRICE_TICK, rounding=ROUND_HALF_UP))

    @staticmethod
    def _positive_price_from_li(value: Any, column: str, symbol: str, trade_date: date) -> float:
        try:
            parsed = float(value) / PRICE_UNIT_DIVISOR
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                f"invalid {column} in market.kline_minute_raw",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "value": value},
            ) from exc
        if parsed <= 0:
            raise DataUnavailableError(
                f"invalid {column} in market.kline_minute_raw",
                context={"symbol": symbol, "trade_date": trade_date.isoformat(), "value": value},
            )
        return parsed

    @staticmethod
    def _positive_float(value: Any, field: str, symbol: str, bar_time: datetime) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                f"minute bar {field} is invalid",
                context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
            ) from exc
        if parsed <= 0:
            raise DataUnavailableError(
                f"minute bar {field} must be positive",
                context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
            )
        return parsed

    @staticmethod
    def _optional_non_negative_float(
        value: Any,
        field: str,
        symbol: str,
        bar_time: datetime,
    ) -> float | None:
        if value is None:
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                f"minute bar {field} is invalid",
                context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
            ) from exc
        if parsed < 0:
            raise DataUnavailableError(
                f"minute bar {field} must be non-negative",
                context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
            )
        return parsed

    @staticmethod
    def _volume_hands_to_shares(value: Any, symbol: str, bar_time: datetime) -> int:
        try:
            volume_hand = float(value)
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                "minute bar volume is invalid",
                context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
            ) from exc
        if volume_hand < 0:
            raise DataUnavailableError(
                "minute bar volume must be non-negative",
                context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
            )
        return int(round(volume_hand * MINUTE_VOLUME_HAND_SIZE))

    @staticmethod
    def _parse_suspend_status(
        raw: dict[str, Any],
        *,
        symbol: str,
        bar_time: datetime,
        require_suspend_status: bool,
    ) -> bool:
        for key in ("is_suspended", "suspended", "suspend_status"):
            if key in raw:
                value = raw[key]
                if isinstance(value, bool):
                    return value
                if isinstance(value, (int, float)):
                    return bool(value)
                normalized = str(value).strip().lower()
                if normalized in {"1", "true", "yes", "y", "suspended"}:
                    return True
                if normalized in {"0", "false", "no", "n", "active", "trading"}:
                    return False
                raise DataUnavailableError(
                    "minute bar suspend status is invalid",
                    context={"symbol": symbol, "bar_time": bar_time.isoformat(), "value": value},
                )
        if require_suspend_status:
            raise DataUnavailableError(
                "minute bar suspend status is required",
                context={"symbol": symbol, "bar_time": bar_time.isoformat()},
            )
        return False

    @staticmethod
    def _naive_for_compare(value: datetime) -> datetime:
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
