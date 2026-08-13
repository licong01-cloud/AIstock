from __future__ import annotations

import os
from datetime import date
from typing import Any, Callable, Mapping, Protocol, Sequence

from .errors import DatasetReleaseError, IndexContractError
from .index_contract import IndexDefinition
from .profile import ResourcePolicy
from .source_pool import ReadOnlySourcePool


class IndexProviderUnavailable(DatasetReleaseError):
    code = "DATASET_RELEASE_INDEX_PROVIDER_UNAVAILABLE"
    retryable = True


class IndexProviderRateLimitTerminal(DatasetReleaseError):
    """Tushare 40203 is terminal for the current provider window."""

    code = "BLOCKED_PROVIDER_TERMINAL_40203"
    retryable = False


class TushareIndexApi(Protocol):
    def index_daily(self, **kwargs: Any) -> Any: ...


_INDEX_SQL = """
SELECT ts_code, trade_date, open, high, low, close, pre_close, pct_chg, vol, amount
FROM market.index_daily
WHERE ts_code = %(ts_code)s
  AND trade_date >= %(start)s
  AND trade_date <= %(end)s
ORDER BY trade_date
"""

_CALENDAR_SQL = """
SELECT cal_date
FROM market.trading_calendar
WHERE cal_date >= %(start)s
  AND cal_date <= %(end)s
  AND is_trading = TRUE
ORDER BY cal_date
"""

_INDEX_COLUMNS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "pct_chg",
    "vol",
    "amount",
)


def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:]))
    return date.fromisoformat(text)


class DatabaseTushareIndexSource:
    """Read DB first and call Tushare only when the materializer detects missing keys."""

    def __init__(
        self,
        pool: ReadOnlySourcePool,
        *,
        provider_factory: Callable[[], TushareIndexApi] | None = None,
    ) -> None:
        self.pool = pool
        self._provider_factory = provider_factory or _default_tushare_provider
        self._provider: TushareIndexApi | None = None

    def trading_dates(self, start: date, end: date) -> Sequence[date]:
        rows = self.pool.fetch_all_small(
            _CALENDAR_SQL,
            {"start": start, "end": end},
            max_rows=min(10_000, self.pool.policy.validation_read_chunk_rows),
        )
        try:
            dates = tuple(_as_date(row[0]) for row in rows)
        except (IndexError, TypeError, ValueError) as exc:
            raise IndexContractError("trading calendar returned an invalid row") from exc
        if dates != tuple(sorted(set(dates))):
            raise IndexContractError("trading calendar rows are not unique and ordered")
        return dates

    def database_rows(self, definition: IndexDefinition, start: date, end: date) -> Sequence[Mapping[str, Any]]:
        rows = self.pool.fetch_all_small(
            _INDEX_SQL,
            {"ts_code": definition.daily_code, "start": start, "end": end},
            max_rows=min(10_000, self.pool.policy.validation_read_chunk_rows),
        )
        output: list[dict[str, Any]] = []
        for row in rows:
            if len(row) != len(_INDEX_COLUMNS):
                raise IndexContractError("database index query returned an invalid row width")
            payload = dict(zip(_INDEX_COLUMNS, row))
            payload["trade_date"] = _as_date(payload["trade_date"])
            output.append(payload)
        return output

    def provider_rows(self, definition: IndexDefinition, start: date, end: date) -> Sequence[Mapping[str, Any]]:
        if self._provider is None:
            try:
                self._provider = self._provider_factory()
            except IndexProviderUnavailable:
                raise
            except Exception as exc:
                if _is_40203(exc):
                    raise IndexProviderRateLimitTerminal(
                        "Tushare provider returned terminal code 40203 during initialization"
                    ) from exc
                raise IndexProviderUnavailable("Tushare provider initialization failed") from exc
        try:
            frame = self._provider.index_daily(
                ts_code=definition.daily_code,
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
                fields=",".join(_INDEX_COLUMNS),
            )
        except Exception as exc:
            if _is_40203(exc):
                raise IndexProviderRateLimitTerminal(
                    f"Tushare terminal code 40203 for index {definition.daily_code}"
                ) from exc
            raise IndexProviderUnavailable(f"Tushare index_daily failed for {definition.daily_code}") from exc
        if frame is None:
            raise IndexProviderUnavailable(f"Tushare index_daily returned no response for {definition.daily_code}")
        try:
            records = frame.to_dict(orient="records")
        except (AttributeError, TypeError) as exc:
            raise IndexProviderUnavailable("Tushare index_daily response is not tabular") from exc
        output: list[dict[str, Any]] = []
        for record in records:
            missing = [column for column in _INDEX_COLUMNS if column not in record]
            if missing:
                raise IndexProviderUnavailable(f"Tushare index_daily response is missing fields: {missing}")
            payload = {column: record[column] for column in _INDEX_COLUMNS}
            payload["trade_date"] = _as_date(payload["trade_date"])
            output.append(payload)
        return sorted(output, key=lambda row: (str(row["ts_code"]), row["trade_date"]))


def _default_tushare_provider() -> TushareIndexApi:
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise IndexProviderUnavailable("TUSHARE_TOKEN is not configured")
    try:
        import tushare as ts

        return ts.pro_api(token)
    except Exception as exc:
        raise IndexProviderUnavailable("Tushare provider initialization failed") from exc


def _is_40203(exc: BaseException) -> bool:
    return str(getattr(exc, "code", "")) == "40203" or "40203" in str(exc)


def independent_postgres_connection_factory() -> Any:
    """Create one worker-owned connection without importing the backend global pool."""

    import psycopg2

    return psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", ""),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
        application_name="AIstock-dataset-release-worker",
        options="-c client_encoding=utf8",
    )


def build_index_source(
    policy: ResourcePolicy,
    *,
    provider_factory: Callable[[], TushareIndexApi] | None = None,
    connection_factory: Callable[[], Any] = independent_postgres_connection_factory,
) -> DatabaseTushareIndexSource:
    return DatabaseTushareIndexSource(
        ReadOnlySourcePool(connection_factory, policy),
        provider_factory=provider_factory,
    )
