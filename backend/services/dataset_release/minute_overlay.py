"""Candidate-local minute gap overlays with strict TDX-first semantics.

Provider responses are normalized, compared with frozen database rows, and may
be written to an injected immutable CAS.  This module has no database writer,
sync-engine, or production-path dependency.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from .profile import ResourcePolicy, validate_resource_policy

from .errors import DatasetReleaseError


RAW_MINUTE_COLUMNS = (
    "trade_time",
    "ts_code",
    "open_li",
    "high_li",
    "low_li",
    "close_li",
    "volume_hand",
    "amount_li",
)
RAW_VALUE_COLUMNS = RAW_MINUTE_COLUMNS[2:]
CHINA_TZ = ZoneInfo("Asia/Shanghai")
EXPECTED_FULL_DAY_BARS = 240


TdxFetchRows = Callable[[str, date, date], Sequence[Mapping[str, Any]]]
TushareFetchRows = Callable[[str, date], Sequence[Mapping[str, Any]]]


class JsonCAS(Protocol):
    def put_json(self, payload: Mapping[str, Any] | list[Any]) -> Any: ...


class MinuteOverlayError(DatasetReleaseError):
    """Base class for minute overlay failures."""

    code = "DATASET_RELEASE_MINUTE_OVERLAY_INVALID"


class MinuteSourceConflict(MinuteOverlayError):
    """Database/provider values disagree on an existing key."""

    code = "BLOCKED_MINUTE_PROVIDER_CONFLICT"


class MinuteProviderInvalid(MinuteOverlayError):
    """A provider response is incomplete or violates the canonical schema."""

    code = "BLOCKED_MINUTE_PROVIDER_INVALID"


class MinuteProviderTerminal(MinuteOverlayError):
    """A provider condition must not be retried or hidden by fallback."""

    code = "BLOCKED_MINUTE_PROVIDER_TERMINAL"


class MinuteProviderRateLimitTerminal(MinuteProviderTerminal):
    """Tushare 40203 pauses the durable run until the provider window resets."""

    code = "WAITING_PROVIDER_RATE_LIMIT_40203"
    retryable = True


class MinuteProviderUnavailable(MinuteProviderTerminal):
    """Both bounded provider calls failed for a transient availability reason."""

    code = "WAITING_MINUTE_PROVIDER_UNAVAILABLE"
    retryable = True


@dataclass(frozen=True, slots=True)
class MinuteGap:
    ts_code: str
    trade_date: date
    expected_bars: int = EXPECTED_FULL_DAY_BARS

    def __post_init__(self) -> None:
        code = str(self.ts_code).strip().upper()
        if not (len(code) == 9 and code[:6].isdigit() and code[6:] in {".SH", ".SZ"}):
            raise ValueError(f"invalid SH/SZ minute code: {self.ts_code!r}")
        if int(self.expected_bars) != EXPECTED_FULL_DAY_BARS:
            raise ValueError("full A-share minute session must contain 240 bars")
        object.__setattr__(self, "ts_code", code)

    @property
    def request_id(self) -> str:
        return hashlib.sha256(
            _canonical_json_bytes(
                {
                    "schema_version": "dataset_release_minute_request_v1",
                    "ts_code": self.ts_code,
                    "trade_date": self.trade_date.isoformat(),
                    "expected_bars": self.expected_bars,
                    "provider_precedence": "tdx_then_tushare_missing_keys_v1",
                }
            )
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class ProviderAttempt:
    provider: str
    status: str
    error_type: str | None = None
    provider_code: str | None = None
    http_status: int | None = None
    message_sha256: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "status": self.status,
            "error_type": self.error_type,
            "provider_code": self.provider_code,
            "http_status": self.http_status,
            "message_sha256": self.message_sha256,
        }


@dataclass(frozen=True, slots=True)
class MinuteOverlayResult:
    request_id: str
    ts_code: str
    trade_date: date
    provider: str
    database_rows: int
    provider_rows: int
    overlap_rows_verified: int
    overlay_rows: tuple[Mapping[str, Any], ...]
    provider_content_sha256: str | None
    overlay_content_sha256: str
    provider_cas_ref: Mapping[str, Any] | str | None
    overlay_cas_ref: Mapping[str, Any] | str | None
    attempts: tuple[ProviderAttempt, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "dataset_release_minute_overlay_result_v1",
            "request_id": self.request_id,
            "ts_code": self.ts_code,
            "trade_date": self.trade_date.isoformat(),
            "provider": self.provider,
            "database_rows": self.database_rows,
            "provider_rows": self.provider_rows,
            "overlap_rows_verified": self.overlap_rows_verified,
            "overlay_rows": len(self.overlay_rows),
            "provider_content_sha256": self.provider_content_sha256,
            "overlay_content_sha256": self.overlay_content_sha256,
            "provider_cas_ref": self.provider_cas_ref,
            "overlay_cas_ref": self.overlay_cas_ref,
            "attempts": [attempt.as_dict() for attempt in self.attempts],
            "database_writes": 0,
            "production_writes": 0,
        }


class MinuteOverlayBuilder:
    """Sequential provider resolver with a hard concurrency value of one."""

    def __init__(
        self,
        *,
        fetch_tdx_rows: TdxFetchRows,
        fetch_tushare_rows: TushareFetchRows,
        policy: ResourcePolicy,
        cas: JsonCAS | None = None,
    ) -> None:
        self.policy = validate_resource_policy(policy)
        if not callable(fetch_tdx_rows) or not callable(fetch_tushare_rows):
            raise TypeError("minute provider fetchers must be callable")
        self.fetch_tdx_rows = fetch_tdx_rows
        self.fetch_tushare_rows = fetch_tushare_rows
        self.cas = cas
        self.provider_concurrency = self.policy.provider_request_concurrency
        self._provider_lock = threading.Lock()
        self._active_provider_calls = 0
        self._peak_provider_calls = 0

    @property
    def peak_provider_calls(self) -> int:
        return self._peak_provider_calls

    def build_one(
        self,
        gap: MinuteGap,
        database_rows: pd.DataFrame | Iterable[Mapping[str, Any]],
        *,
        prefetched_tdx_rows: Sequence[Mapping[str, Any]] | None = None,
        prefetched_tdx_frame: pd.DataFrame | None = None,
        prefetched_tdx_error: Exception | None = None,
    ) -> MinuteOverlayResult:
        database = normalize_database_rows(database_rows, gap)
        if len(database) == gap.expected_bars:
            empty_payload = _overlay_payload(gap, "database_complete", database.iloc[0:0])
            return MinuteOverlayResult(
                request_id=gap.request_id,
                ts_code=gap.ts_code,
                trade_date=gap.trade_date,
                provider="database_complete",
                database_rows=len(database),
                provider_rows=0,
                overlap_rows_verified=0,
                overlay_rows=(),
                provider_content_sha256=None,
                overlay_content_sha256=_frame_content_sha256(gap, "overlay", database.iloc[0:0]),
                provider_cas_ref=None,
                overlay_cas_ref=_put_json(self.cas, empty_payload),
                attempts=(),
            )

        attempts: list[ProviderAttempt] = []
        try:
            if prefetched_tdx_error is not None:
                raise prefetched_tdx_error
            if prefetched_tdx_frame is not None:
                provider = _validate_normalized_provider_frame(
                    prefetched_tdx_frame,
                    provider="tdx",
                    gap=gap,
                )
            else:
                raw_tdx = (
                    prefetched_tdx_rows
                    if prefetched_tdx_rows is not None
                    else self._provider_call(
                        self.fetch_tdx_rows,
                        gap.ts_code,
                        gap.trade_date,
                        gap.trade_date,
                    )
                )
                provider = normalize_provider_rows(raw_tdx, provider="tdx", gap=gap)
            overlay, overlap = missing_key_overlay(database, provider, gap)
            attempts.append(ProviderAttempt("tdx", "PASS"))
            return self._result(gap, database, provider, overlay, overlap, "tdx", attempts)
        except MinuteSourceConflict:
            # A conflicting TDX observation is source ambiguity, not an
            # availability problem that another provider may conceal.
            raise
        except MinuteProviderRateLimitTerminal:
            raise
        except Exception as exc:  # rejected TDX attempt, then explicit fallback
            if _is_40203(exc):
                raise MinuteProviderRateLimitTerminal(f"provider 40203 for {gap.ts_code} {gap.trade_date}") from exc
            attempts.append(_failed_attempt("tdx", exc))

        try:
            raw_tushare = self._provider_call(self.fetch_tushare_rows, gap.ts_code, gap.trade_date)
        except MinuteProviderTerminal:
            raise
        except Exception as exc:
            if _is_40203(exc):
                raise MinuteProviderRateLimitTerminal(
                    f"Tushare 40203 for {gap.ts_code} {gap.trade_date}; "
                    "request is terminal for the current provider window"
                ) from exc
            attempts.append(_failed_attempt("tushare", exc))
            raise MinuteProviderUnavailable(
                f"both minute providers failed for {gap.ts_code} {gap.trade_date}; "
                f"attempts={[item.as_dict() for item in attempts]}"
            ) from exc
        try:
            provider = normalize_provider_rows(raw_tushare, provider="tushare", gap=gap)
            overlay, overlap = missing_key_overlay(database, provider, gap)
        except MinuteSourceConflict:
            raise
        except Exception as exc:
            attempts.append(_failed_attempt("tushare", exc))
            raise MinuteProviderTerminal(
                f"Tushare minute response is invalid for {gap.ts_code} "
                f"{gap.trade_date}; response details are sealed as a digest"
            ) from exc
        attempts.append(ProviderAttempt("tushare", "PASS"))
        return self._result(gap, database, provider, overlay, overlap, "tushare", attempts)

    def build_many(
        self,
        requests: Iterable[tuple[MinuteGap, pd.DataFrame | Iterable[Mapping[str, Any]]]],
    ) -> tuple[MinuteOverlayResult, ...]:
        """Compatibility collector over :meth:`iter_many` for small request sets."""

        return tuple(self.iter_many(requests))

    def iter_many(
        self,
        requests: Iterable[tuple[MinuteGap, pd.DataFrame | Iterable[Mapping[str, Any]]]],
    ) -> Iterable[MinuteOverlayResult]:
        """Resolve an ordered stream while retaining at most one TDX payload.

        Callers pass ``(ts_code, trade_date)`` order from the frozen gap
        manifest.  Rejecting order drift avoids silently materializing and
        sorting every database frame in memory.
        """

        pending: list[tuple[MinuteGap, pd.DataFrame | Iterable[Mapping[str, Any]]]] = []
        previous_key: tuple[str, date] | None = None
        for gap, database in requests:
            key = (gap.ts_code, gap.trade_date)
            if previous_key is not None and key <= previous_key:
                raise MinuteOverlayError("minute gap stream must be strictly ordered by code,date")
            previous_key = key
            if pending and gap.ts_code != pending[0][0].ts_code:
                yield from self._resolve_tdx_code_window(pending)
                pending.clear()
            pending.append((gap, database))
        if pending:
            yield from self._resolve_tdx_code_window(pending)

    def _resolve_tdx_code_window(
        self,
        requests: Sequence[tuple[MinuteGap, pd.DataFrame | Iterable[Mapping[str, Any]]]],
    ) -> Iterable[MinuteOverlayResult]:
        code = requests[0][0].ts_code
        start_date = requests[0][0].trade_date
        end_date = requests[-1][0].trade_date
        cached_frames: dict[date, pd.DataFrame] = {}
        cached_day_errors: dict[date, Exception] = {}
        cached_error: Exception | None = None
        try:
            cached_rows = self._provider_call(
                self.fetch_tdx_rows,
                code,
                start_date,
                end_date,
            )
            try:
                cached_frames, cached_day_errors = _normalize_provider_window_rows(
                    cached_rows,
                    provider="tdx",
                    gaps=tuple(gap for gap, _database in requests),
                )
            finally:
                del cached_rows
        except Exception as exc:  # build_one records/falls back per date
            cached_error = exc
        for gap, database in requests:
            day_error = cached_error or cached_day_errors.get(gap.trade_date)
            if day_error is not None:
                yield self.build_one(
                    gap,
                    database,
                    prefetched_tdx_error=day_error,
                )
            else:
                yield self.build_one(
                    gap,
                    database,
                    prefetched_tdx_frame=cached_frames[gap.trade_date],
                )

    def _provider_call(self, callable_: Callable[..., Any], *args: Any) -> Any:
        with self._provider_lock:
            self._active_provider_calls += 1
            self._peak_provider_calls = max(self._peak_provider_calls, self._active_provider_calls)
            try:
                return callable_(*args)
            finally:
                self._active_provider_calls -= 1

    def _result(
        self,
        gap: MinuteGap,
        database: pd.DataFrame,
        provider: pd.DataFrame,
        overlay: pd.DataFrame,
        overlap_rows: int,
        provider_name: str,
        attempts: Sequence[ProviderAttempt],
    ) -> MinuteOverlayResult:
        provider_payload = _provider_payload(gap, provider_name, provider)
        overlay_payload = _overlay_payload(gap, provider_name, overlay)
        return MinuteOverlayResult(
            request_id=gap.request_id,
            ts_code=gap.ts_code,
            trade_date=gap.trade_date,
            provider=provider_name,
            database_rows=len(database),
            provider_rows=len(provider),
            overlap_rows_verified=overlap_rows,
            overlay_rows=tuple(_frame_records(overlay)),
            provider_content_sha256=_frame_content_sha256(gap, "provider", provider),
            overlay_content_sha256=_frame_content_sha256(gap, "overlay", overlay),
            provider_cas_ref=_put_json(self.cas, provider_payload),
            overlay_cas_ref=_put_json(self.cas, overlay_payload),
            attempts=tuple(attempts),
        )


def canonical_session_times(target: date) -> tuple[datetime, ...]:
    morning = tuple(datetime.combine(target, datetime_time(9, 31)) + timedelta(minutes=offset) for offset in range(120))
    afternoon = tuple(
        datetime.combine(target, datetime_time(13, 1)) + timedelta(minutes=offset) for offset in range(120)
    )
    return morning + afternoon


def normalize_database_rows(
    rows: pd.DataFrame | Iterable[Mapping[str, Any]],
    gap: MinuteGap,
) -> pd.DataFrame:
    frame = rows.copy() if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
    if frame.empty:
        return pd.DataFrame(columns=RAW_MINUTE_COLUMNS)
    missing = [column for column in RAW_MINUTE_COLUMNS if column not in frame.columns]
    if missing:
        raise MinuteSourceConflict(f"database minute rows missing columns: {missing}")
    frame = frame.loc[:, RAW_MINUTE_COLUMNS].copy()
    frame["trade_time"] = frame["trade_time"].map(_parse_trade_time)
    frame["ts_code"] = frame["ts_code"].astype(str).str.upper()
    if set(frame["ts_code"]) != {gap.ts_code}:
        raise MinuteSourceConflict("database minute rows contain another instrument")
    if any(value.date() != gap.trade_date for value in frame["trade_time"]):
        raise MinuteSourceConflict("database minute rows contain another trade date")
    _validate_raw_values(frame, source="database")
    _validate_keys(frame, source="database", expected_full=False, gap=gap)
    if len(frame) > gap.expected_bars:
        raise MinuteSourceConflict("database minute rows exceed canonical session size")
    return frame.sort_values(["ts_code", "trade_time"]).reset_index(drop=True)


def normalize_provider_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    provider: str,
    gap: MinuteGap,
) -> pd.DataFrame:
    normalized: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise MinuteProviderInvalid(f"{provider} returned a non-object row")
        trade_time = _parse_trade_time(_extract(raw, "trade_time", "TradeTime", "time", "Time"))
        if trade_time.date() != gap.trade_date:
            continue
        normalized.append(
            _normalize_provider_row(
                raw,
                provider=provider,
                ts_code=gap.ts_code,
                trade_time=trade_time,
            )
        )
    frame = pd.DataFrame(normalized, columns=RAW_MINUTE_COLUMNS)
    _validate_raw_values(frame, source=provider)
    _validate_keys(frame, source=provider, expected_full=True, gap=gap)
    return frame.sort_values(["ts_code", "trade_time"]).reset_index(drop=True)


def _normalize_provider_window_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    provider: str,
    gaps: Sequence[MinuteGap],
) -> tuple[dict[date, pd.DataFrame], dict[date, Exception]]:
    """Normalize one provider window with one pass over its raw rows.

    Timestamp/non-object failures apply to the whole response just as they did
    when every requested day rescanned the whole response.  Failures that can
    be attributed to one Shanghai trade date remain isolated to that date so
    the established per-day Tushare fallback contract is unchanged.
    """

    gaps_by_date = {gap.trade_date: gap for gap in gaps}
    if len(gaps_by_date) != len(gaps):
        raise MinuteOverlayError("provider window contains duplicate trade dates")
    rows_by_date: dict[date, list[dict[str, Any]]] = {trade_date: [] for trade_date in gaps_by_date}
    errors_by_date: dict[date, Exception] = {}

    for raw in rows:
        if not isinstance(raw, Mapping):
            global_error: Exception = MinuteProviderInvalid(f"{provider} returned a non-object row")
            break
        try:
            trade_time = _parse_trade_time(_extract(raw, "trade_time", "TradeTime", "time", "Time"))
        except Exception as exc:
            global_error = exc
            break
        trade_date = trade_time.date()
        gap = gaps_by_date.get(trade_date)
        if gap is None or trade_date in errors_by_date:
            continue
        try:
            rows_by_date[trade_date].append(
                _normalize_provider_row(
                    raw,
                    provider=provider,
                    ts_code=gap.ts_code,
                    trade_time=trade_time,
                )
            )
        except Exception as exc:
            # normalize_provider_rows stopped at the first attributable row
            # error for this date.  Preserve that error while still preparing
            # unaffected dates from the same bounded provider response.
            errors_by_date[trade_date] = exc
    else:
        global_error = None

    if global_error is not None:
        for trade_date in gaps_by_date:
            errors_by_date.setdefault(trade_date, global_error)
        return {}, errors_by_date

    frames_by_date: dict[date, pd.DataFrame] = {}
    for trade_date in gaps_by_date:
        if trade_date in errors_by_date:
            continue
        frame = pd.DataFrame(rows_by_date[trade_date], columns=RAW_MINUTE_COLUMNS)
        frames_by_date[trade_date] = frame.sort_values(["ts_code", "trade_time"]).reset_index(drop=True)
    return frames_by_date, errors_by_date


def _normalize_provider_row(
    raw: Mapping[str, Any],
    *,
    provider: str,
    ts_code: str,
    trade_time: datetime,
) -> dict[str, Any]:
    raw_code = _extract(raw, "ts_code", "TsCode", "code", "Code")
    if raw_code is not None and not _provider_code_matches(raw_code, ts_code):
        raise MinuteProviderInvalid(f"{provider} response contains another instrument: {raw_code!r}")
    if provider == "tdx":
        values = {
            "open_li": _integer(_extract(raw, "open_li", "Open", "open"), "open"),
            "high_li": _integer(_extract(raw, "high_li", "High", "high"), "high"),
            "low_li": _integer(_extract(raw, "low_li", "Low", "low"), "low"),
            "close_li": _integer(_extract(raw, "close_li", "Close", "close"), "close"),
            "volume_hand": _integer(
                _extract(raw, "volume_hand", "Volume", "volume", "vol"),
                "volume",
            ),
            "amount_li": _integer(_extract(raw, "amount_li", "Amount", "amount"), "amount"),
        }
    elif provider == "tushare":
        values = {
            "open_li": _scaled(raw.get("open"), Decimal("1000"), "open"),
            "high_li": _scaled(raw.get("high"), Decimal("1000"), "high"),
            "low_li": _scaled(raw.get("low"), Decimal("1000"), "low"),
            "close_li": _scaled(raw.get("close"), Decimal("1000"), "close"),
            "volume_hand": _scaled(raw.get("vol"), Decimal("0.01"), "vol"),
            "amount_li": _scaled(raw.get("amount"), Decimal("1000"), "amount"),
        }
    else:
        raise ValueError(f"unsupported minute provider: {provider}")
    return {"trade_time": trade_time, "ts_code": ts_code, **values}


def _validate_normalized_provider_frame(
    frame: pd.DataFrame,
    *,
    provider: str,
    gap: MinuteGap,
) -> pd.DataFrame:
    if tuple(frame.columns) != RAW_MINUTE_COLUMNS:
        raise MinuteProviderInvalid(f"{provider} normalized columns differ")
    observed_codes = set(frame["ts_code"].astype(str))
    if observed_codes and observed_codes != {gap.ts_code}:
        raise MinuteProviderInvalid(f"{provider} response contains another instrument")
    _validate_raw_values(frame, source=provider)
    _validate_keys(frame, source=provider, expected_full=True, gap=gap)
    return frame.sort_values(["ts_code", "trade_time"]).reset_index(drop=True)


def missing_key_overlay(
    database: pd.DataFrame,
    provider: pd.DataFrame,
    gap: MinuteGap,
) -> tuple[pd.DataFrame, int]:
    overlap = database.merge(
        provider,
        on=["ts_code", "trade_time"],
        how="inner",
        suffixes=("_db", "_provider"),
        validate="one_to_one",
    )
    mismatches: list[dict[str, Any]] = []
    for column in RAW_VALUE_COLUMNS:
        unequal = overlap[f"{column}_db"].ne(overlap[f"{column}_provider"])
        for row in (
            overlap.loc[
                unequal,
                ["trade_time", f"{column}_db", f"{column}_provider"],
            ]
            .head(max(0, 10 - len(mismatches)))
            .itertuples(index=False)
        ):
            mismatches.append(
                {
                    "trade_time": str(row[0]),
                    "field": column,
                    "database": int(row[1]),
                    "provider": int(row[2]),
                }
            )
    if mismatches:
        raise MinuteSourceConflict(
            f"minute provider/database overlap mismatch for {gap.ts_code} {gap.trade_date}: {mismatches}"
        )
    database_keys = pd.MultiIndex.from_frame(database[["ts_code", "trade_time"]])
    provider_keys = pd.MultiIndex.from_frame(provider[["ts_code", "trade_time"]])
    overlay = provider.loc[~provider_keys.isin(database_keys)].copy()
    combined = pd.concat([database, overlay], ignore_index=True)
    _validate_keys(combined, source="database_plus_overlay", expected_full=True, gap=gap)
    return overlay.sort_values(["ts_code", "trade_time"]).reset_index(drop=True), len(overlap)


def _validate_keys(
    frame: pd.DataFrame,
    *,
    source: str,
    expected_full: bool,
    gap: MinuteGap,
) -> None:
    if frame.duplicated(["ts_code", "trade_time"]).any():
        raise MinuteProviderInvalid(f"{source} contains duplicate minute keys")
    observed = set(frame["trade_time"].tolist()) if not frame.empty else set()
    expected = set(canonical_session_times(gap.trade_date))
    unexpected = observed.difference(expected)
    if unexpected:
        raise MinuteProviderInvalid(f"{source} contains out-of-session minute keys: {sorted(unexpected)[:3]}")
    if expected_full and (len(frame) != gap.expected_bars or observed != expected):
        raise MinuteProviderInvalid(
            f"{source} canonical session is not 240 unique bars: "
            f"rows={len(frame)} unique={len(observed)} missing={len(expected - observed)}"
        )


def _validate_raw_values(frame: pd.DataFrame, *, source: str) -> None:
    if frame.empty:
        return
    numeric = frame.loc[:, RAW_VALUE_COLUMNS].apply(pd.to_numeric, errors="coerce")
    values = numeric.to_numpy(dtype=np.float64)
    if not np.isfinite(values).all():
        raise MinuteProviderInvalid(f"{source} contains NULL or non-finite values")
    for column in RAW_VALUE_COLUMNS:
        if not np.equal(
            values[:, RAW_VALUE_COLUMNS.index(column)], np.floor(values[:, RAW_VALUE_COLUMNS.index(column)])
        ).all():
            raise MinuteProviderInvalid(f"{source} {column} is not integral raw storage")
        frame[column] = numeric[column].astype("int64")
    if (frame[["open_li", "high_li", "low_li", "close_li"]] <= 0).any().any():
        raise MinuteProviderInvalid(f"{source} contains non-positive OHLC")
    if (frame[["volume_hand", "amount_li"]] < 0).any().any():
        raise MinuteProviderInvalid(f"{source} contains negative volume/amount")
    if (frame["high_li"] < frame[["open_li", "close_li"]].max(axis=1)).any() or (
        frame["low_li"] > frame[["open_li", "close_li"]].min(axis=1)
    ).any():
        raise MinuteProviderInvalid(f"{source} OHLC ordering is invalid")


def _parse_trade_time(value: Any) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise MinuteProviderInvalid("minute row has no trade_time")
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        for pattern in ("%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
    if parsed is None:
        raise MinuteProviderInvalid(f"invalid minute timestamp: {value!r}")
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(CHINA_TZ).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def _extract(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in row:
            return row[name]
    return None


def _decimal(value: Any, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise MinuteProviderInvalid(f"minute {field} is not numeric: {value!r}") from exc
    if not number.is_finite():
        raise MinuteProviderInvalid(f"minute {field} is not finite: {value!r}")
    return number


def _integer(value: Any, field: str) -> int:
    number = _decimal(value, field)
    integral = number.to_integral_value()
    if number != integral:
        raise MinuteProviderInvalid(f"TDX {field} is not an exact raw integer")
    return int(integral)


def _scaled(value: Any, multiplier: Decimal, field: str) -> int:
    return int((_decimal(value, field) * multiplier).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _provider_payload(
    gap: MinuteGap,
    provider: str,
    frame: pd.DataFrame,
) -> dict[str, Any]:
    payload = {
        "schema_version": "dataset_release_minute_provider_snapshot_v1",
        "request_id": gap.request_id,
        "provider": provider,
        "ts_code": gap.ts_code,
        "trade_date": gap.trade_date.isoformat(),
        "rows": _frame_records(frame),
    }
    payload["content_sha256"] = _frame_content_sha256(gap, "provider", frame)
    return payload


def _overlay_payload(
    gap: MinuteGap,
    provider: str,
    frame: pd.DataFrame,
) -> dict[str, Any]:
    payload = {
        "schema_version": "dataset_release_minute_missing_key_overlay_v1",
        "request_id": gap.request_id,
        "provider": provider,
        "ts_code": gap.ts_code,
        "trade_date": gap.trade_date.isoformat(),
        "rows": _frame_records(frame),
        "database_writes": 0,
        "production_writes": 0,
    }
    payload["content_sha256"] = _frame_content_sha256(gap, "overlay", frame)
    return payload


def _frame_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in frame.loc[:, RAW_MINUTE_COLUMNS].itertuples(index=False):
        records.append(
            {
                "trade_time": row.trade_time.isoformat(sep=" "),
                "ts_code": str(row.ts_code),
                "open_li": int(row.open_li),
                "high_li": int(row.high_li),
                "low_li": int(row.low_li),
                "close_li": int(row.close_li),
                "volume_hand": int(row.volume_hand),
                "amount_li": int(row.amount_li),
            }
        )
    return records


def _canonical_json_bytes(payload: Any) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _payload_sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _frame_content_sha256(gap: MinuteGap, kind: str, frame: pd.DataFrame) -> str:
    """Hash normalized bytes without provider/provenance identity."""

    return _payload_sha256(
        {
            "schema_version": "dataset_release_minute_content_v1",
            "kind": kind,
            "ts_code": gap.ts_code,
            "trade_date": gap.trade_date.isoformat(),
            "rows": _frame_records(frame),
        }
    )


def _put_json(cas: JsonCAS | None, payload: Mapping[str, Any]) -> Mapping[str, Any] | str | None:
    if cas is None:
        return None
    reference = cas.put_json(payload)
    if hasattr(reference, "as_dict"):
        return reference.as_dict()
    if isinstance(reference, Mapping):
        return dict(reference)
    return str(reference)


def _failed_attempt(provider: str, exc: Exception) -> ProviderAttempt:
    raw_code = str(getattr(exc, "code", "")).strip()
    provider_code = raw_code if re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", raw_code) else None
    raw_status = getattr(exc, "status_code", getattr(exc, "status", None))
    http_status = (
        int(raw_status)
        if isinstance(raw_status, int) and not isinstance(raw_status, bool) and 100 <= raw_status <= 599
        else None
    )
    return ProviderAttempt(
        provider=provider,
        status="REJECTED",
        error_type=type(exc).__name__,
        provider_code=provider_code,
        http_status=http_status,
        message_sha256=hashlib.sha256(f"{type(exc).__name__}\0{exc}".encode("utf-8", errors="replace")).hexdigest(),
    )


def _is_40203(exc: BaseException) -> bool:
    return str(getattr(exc, "code", "")) == "40203" or "40203" in str(exc)


def _provider_code_matches(raw: Any, expected: str) -> bool:
    value = str(raw).strip().upper()
    if value == expected:
        return True
    if len(value) == 8 and value[:2] in {"SH", "SZ"}:
        value = f"{value[2:]}.{value[:2]}"
    if value == expected:
        return True
    # Some TDX payloads expose only the six-digit code; the request identity
    # already freezes the exchange and the digits still must match exactly.
    return value == expected[:6]
