"""Official AIstock trading-day status service with filesystem cache."""

from __future__ import annotations

import calendar
import hashlib
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterator
from zoneinfo import ZoneInfo

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError

ConnFactory = Callable[[], Iterator[Any]]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CACHE_PATH = PROJECT_ROOT / "tmp" / "trading_calendar_status_cache.json"
CHINA_TZ = ZoneInfo("Asia/Shanghai")


class TradingCalendarStatusService:
    """Single backend authority for trading-day checks.

    The database remains the source of truth, but normal API calls read a
    monthly refreshed filesystem cache. Missing calendar rows fail fast; local
    weekend/weekday inference is never used as a substitute.
    """

    def __init__(
        self,
        *,
        conn_factory: ConnFactory | None = None,
        cache_path: str | Path | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._conn_factory = conn_factory or get_conn
        env_path = os.getenv("AISTOCK_TRADING_CALENDAR_CACHE")
        self._cache_path = Path(cache_path or env_path or DEFAULT_CACHE_PATH)
        self._now_provider = now_provider or (lambda: datetime.now(CHINA_TZ))

    def status(self, *, as_of_date: date | None = None) -> dict[str, Any]:
        as_of = as_of_date or self._now_provider().date()
        cache = self._load_or_refresh_cache(as_of)
        calendar_map = self._calendar_map(cache)
        if as_of not in calendar_map:
            raise DataUnavailableError(
                "trading calendar row is missing for current date",
                context={
                    "as_of_date": as_of.isoformat(),
                    "cache_coverage_start": cache.get("coverage_start"),
                    "cache_coverage_end": cache.get("coverage_end"),
                },
            )
        trading_days = sorted(day for day, is_trading in calendar_map.items() if is_trading)
        latest_completed = _max_day([day for day in trading_days if day <= as_of])
        previous_trading = _max_day([day for day in trading_days if day < as_of])
        next_trading = _min_day([day for day in trading_days if day > as_of])
        warnings = self._coverage_warnings(cache, as_of)
        return {
            "ok": True,
            "as_of_date": as_of.isoformat(),
            "timezone": "Asia/Shanghai",
            "is_trading_day": bool(calendar_map[as_of]),
            "latest_completed_trading_day": latest_completed.isoformat() if latest_completed else None,
            "previous_trading_day": previous_trading.isoformat() if previous_trading else None,
            "next_trading_day": next_trading.isoformat() if next_trading else None,
            "source": "market.trading_calendar:file_cache",
            "warnings": warnings,
            "cache": {
                "path": str(self._cache_path),
                "generated_at": cache.get("generated_at"),
                "coverage_start": cache.get("coverage_start"),
                "coverage_end": cache.get("coverage_end"),
                "calendar_row_count": len(cache.get("calendar") or []),
                "checksum": cache.get("checksum"),
                "refresh_reason": cache.get("_refresh_reason"),
            },
        }

    def ensure_trading_day(self, trade_date: date) -> None:
        cache = self._load_or_refresh_cache(trade_date)
        calendar_map = self._calendar_map(cache)
        if trade_date not in calendar_map:
            raise DataUnavailableError(
                "trade calendar row is required for trading-day check",
                context={"trade_date": trade_date.isoformat()},
            )
        if not calendar_map[trade_date]:
            raise DataUnavailableError(
                "trade_date is not a trading day",
                context={"trade_date": trade_date.isoformat()},
            )

    def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
        if start_date > end_date:
            raise DataUnavailableError(
                "start_date cannot be after end_date",
                context={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            )
        cache = self._load_or_refresh_cache(end_date)
        calendar_map = self._calendar_map(cache)
        missing_start = start_date < date.fromisoformat(str(cache["coverage_start"]))
        missing_end = end_date > date.fromisoformat(str(cache["coverage_end"]))
        if missing_start or missing_end:
            cache = self._refresh_cache(f"range_miss:{start_date.isoformat()}:{end_date.isoformat()}")
            calendar_map = self._calendar_map(cache)
        days = [day for day, is_trading in sorted(calendar_map.items()) if start_date <= day <= end_date and is_trading]
        if not days:
            raise DataUnavailableError(
                "trading calendar has no trading days in range",
                context={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            )
        return days

    def _load_or_refresh_cache(self, as_of: date) -> dict[str, Any]:
        cache = self._read_cache()
        reason = self._refresh_reason(cache, as_of)
        if reason:
            return self._refresh_cache(reason)
        return cache

    def _read_cache(self) -> dict[str, Any] | None:
        try:
            payload = json.loads(self._cache_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except Exception:
            return None
        if not isinstance(payload, dict) or not isinstance(payload.get("calendar"), list):
            return None
        return payload

    def _refresh_reason(self, cache: dict[str, Any] | None, as_of: date) -> str | None:
        if cache is None:
            return "missing_cache"
        try:
            generated_at = datetime.fromisoformat(str(cache.get("generated_at")).replace("Z", "+00:00"))
            coverage_start = date.fromisoformat(str(cache.get("coverage_start")))
            coverage_end = date.fromisoformat(str(cache.get("coverage_end")))
        except Exception:
            return "invalid_cache_metadata"
        now = self._now_provider()
        if generated_at.year != now.year or generated_at.month != now.month:
            return "monthly_refresh"
        if now - generated_at > timedelta(days=31):
            return "stale_cache"
        if as_of < coverage_start or as_of > coverage_end:
            return "as_of_outside_cache_coverage"
        return None

    def _refresh_cache(self, reason: str) -> dict[str, Any]:
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT cal_date, is_trading
                        FROM market.trading_calendar
                        ORDER BY cal_date
                        """
                    )
                    rows = cur.fetchall()
        except Exception as exc:
            raise DataUnavailableError(
                "trading calendar cache refresh failed",
                context={"cache_path": str(self._cache_path), "reason": reason, "error": str(exc)},
            ) from exc
        if not rows:
            raise DataUnavailableError("market.trading_calendar has no rows")
        calendar_rows = [
            {"date": row["cal_date"].isoformat(), "is_trading": bool(row["is_trading"])}
            for row in rows
        ]
        checksum = hashlib.sha256(json.dumps(calendar_rows, sort_keys=True).encode("utf-8")).hexdigest()
        payload = {
            "schema_version": 1,
            "source": "market.trading_calendar",
            "generated_at": self._now_provider().isoformat(),
            "coverage_start": calendar_rows[0]["date"],
            "coverage_end": calendar_rows[-1]["date"],
            "calendar": calendar_rows,
            "checksum": checksum,
            "_refresh_reason": reason,
        }
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._cache_path.with_suffix(self._cache_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(self._cache_path)
        return payload

    @staticmethod
    def _calendar_map(cache: dict[str, Any]) -> dict[date, bool]:
        result: dict[date, bool] = {}
        for row in cache.get("calendar") or []:
            result[date.fromisoformat(str(row["date"]))] = bool(row["is_trading"])
        return result

    @staticmethod
    def _coverage_warnings(cache: dict[str, Any], as_of: date) -> list[dict[str, Any]]:
        warnings: list[dict[str, Any]] = []
        coverage_end = date.fromisoformat(str(cache["coverage_end"]))
        next_month_year = as_of.year + (1 if as_of.month == 12 else 0)
        next_month = 1 if as_of.month == 12 else as_of.month + 1
        last_day = calendar.monthrange(next_month_year, next_month)[1]
        required_end = date(next_month_year, next_month, last_day)
        if coverage_end < required_end:
            warnings.append(
                {
                    "code": "TRADING_CALENDAR_NEXT_MONTH_INCOMPLETE",
                    "message": "market.trading_calendar does not cover the full next month; update the yearly trading calendar table",
                    "coverage_end": coverage_end.isoformat(),
                    "required_end": required_end.isoformat(),
                }
            )
        return warnings


def _max_day(days: list[date]) -> date | None:
    return max(days) if days else None


def _min_day(days: list[date]) -> date | None:
    return min(days) if days else None
