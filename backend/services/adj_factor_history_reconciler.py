"""Deterministic Tushare adj-factor history reconciliation.

The daily Tushare feed is mutable: a later provider correction can rewrite an
instrument's older adjustment-factor rows.  A trade-date-only upsert cannot
observe that change and may splice two provider revisions together.  This
module mirrors complete, double-read-stable histories for every locally
relevant instrument and replaces all changed instruments in one transaction.

No model, search, or heuristic company-action inference participates in this
path.  Provider instability or incomplete date coverage fails closed before
any persistent row is changed.
"""

from __future__ import annotations

import datetime as dt
import threading
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

import psycopg2.extras as pgx

from backend.services.dataset_release.canonical import canonical_json_bytes, sha256_hex


RECEIPT_SCHEMA = "adj_factor_history_reconciliation_receipt_v1"
SOURCE_API = "tushare.adj_factor"
DOWNSTREAM_COMPONENTS = ("daily_bin", "factor_h5_static", "minute_bin")
_STAGE_TABLE = "aistock_adj_factor_history_stage"
_EMPTY_PAGE_MAX_ATTEMPTS = 8


class AdjFactorHistoryReconcileError(RuntimeError):
    """Raised when a complete, stable provider history cannot be established."""


@dataclass(frozen=True)
class AdjFactorRow:
    symbol: str
    trade_date: dt.date
    value: Decimal


@dataclass(frozen=True)
class AdjFactorSnapshot:
    symbol: str
    rows: tuple[AdjFactorRow, ...]
    canonical_sha256: str
    qfq_sha256: str
    first_date: dt.date
    last_date: dt.date
    provider_call_count: int = 0


@dataclass(frozen=True)
class SymbolScope:
    symbol: str
    expected_start: dt.date


class AdjFactorHistoryRepository(Protocol):
    def list_symbol_scopes(
        self, *, end_date: dt.date, symbols: Sequence[str] | None = None
    ) -> Sequence[SymbolScope]: ...

    def load_snapshot(self, symbol: str, *, end_date: dt.date) -> AdjFactorSnapshot: ...

    def load_price_dates(self, symbol: str, *, end_date: dt.date) -> frozenset[dt.date]: ...

    def begin_stage(self) -> None: ...

    def stage_rows(self, rows: Sequence[AdjFactorRow]) -> None: ...

    def apply_staged(
        self,
        *,
        symbols: Sequence[str],
        replace_symbols: Sequence[str],
        end_date: dt.date,
        expected_local_sha256: Mapping[str, str],
        expected_row_counts: Mapping[str, int],
    ) -> int: ...


class _RateLimiter:
    def __init__(self, calls_per_minute: int) -> None:
        if calls_per_minute < 0:
            raise ValueError("calls_per_minute must be non-negative")
        self._interval = 0.0 if calls_per_minute == 0 else 60.0 / calls_per_minute
        self._next = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        if self._interval == 0:
            return
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next - now)
            self._next = max(now, self._next) + self._interval
        if wait:
            time.sleep(wait)


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _rows_payload(rows: Iterable[AdjFactorRow]) -> list[dict[str, str]]:
    return [
        {
            "ts_code": row.symbol,
            "trade_date": row.trade_date.isoformat(),
            "adj_factor": _decimal_text(row.value),
        }
        for row in rows
    ]


def _snapshot(symbol: str, rows: Iterable[AdjFactorRow], *, provider_call_count: int = 0) -> AdjFactorSnapshot:
    ordered = tuple(sorted(rows, key=lambda item: item.trade_date))
    if not ordered:
        raise AdjFactorHistoryReconcileError(f"{symbol}: adj-factor history is empty")
    dates = [row.trade_date for row in ordered]
    if len(dates) != len(set(dates)):
        raise AdjFactorHistoryReconcileError(f"{symbol}: duplicate provider trade date")
    if any(row.symbol != symbol for row in ordered):
        raise AdjFactorHistoryReconcileError(f"{symbol}: provider returned another instrument")
    if any(not row.value.is_finite() or row.value <= 0 for row in ordered):
        raise AdjFactorHistoryReconcileError(f"{symbol}: factor must be finite and positive")
    # AIstock/Qlib QFQ normalization is anchored to the last factor at the
    # requested cutoff, not to the numerical maximum factor in the series.
    denominator = ordered[-1].value
    qfq_payload = [
        {
            "trade_date": row.trade_date.isoformat(),
            "qfq_factor": _decimal_text(row.value / denominator),
        }
        for row in ordered
    ]
    return AdjFactorSnapshot(
        symbol=symbol,
        rows=ordered,
        canonical_sha256=sha256_hex(canonical_json_bytes(_rows_payload(ordered))),
        qfq_sha256=sha256_hex(canonical_json_bytes(qfq_payload)),
        first_date=ordered[0].trade_date,
        last_date=ordered[-1].trade_date,
        provider_call_count=provider_call_count,
    )


def _coerce_provider_rows(payload: Any, *, symbol: str, end_date: dt.date) -> tuple[AdjFactorRow, ...]:
    if hasattr(payload, "to_dict"):
        raw_rows = payload.to_dict("records")
    elif isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        raw_rows = payload
    else:
        raise AdjFactorHistoryReconcileError(f"{symbol}: provider payload is not tabular")

    rows: list[AdjFactorRow] = []
    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            raise AdjFactorHistoryReconcileError(f"{symbol}: provider row is not an object")
        raw_symbol = str(raw.get("ts_code") or "").strip().upper()
        if raw_symbol != symbol:
            raise AdjFactorHistoryReconcileError(f"{symbol}: provider instrument mismatch")
        raw_date = str(raw.get("trade_date") or "").strip().replace("-", "")
        try:
            trade_date = dt.datetime.strptime(raw_date, "%Y%m%d").date()
            value = Decimal(str(raw.get("adj_factor")))
        except (ValueError, InvalidOperation) as exc:
            raise AdjFactorHistoryReconcileError(f"{symbol}: provider date or factor is invalid") from exc
        if trade_date > end_date:
            raise AdjFactorHistoryReconcileError(f"{symbol}: provider returned a future row")
        if not value.is_finite() or value <= 0:
            raise AdjFactorHistoryReconcileError(f"{symbol}: provider factor must be finite and positive")
        rows.append(AdjFactorRow(symbol, trade_date, value))
    return tuple(rows)


def _call_provider(
    provider: Any,
    *,
    symbol: str,
    expected_start: dt.date,
    end_date: dt.date,
) -> Any:
    return provider.adj_factor(
        ts_code=symbol,
        start_date=expected_start.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
        fields="ts_code,trade_date,adj_factor",
    )


def _fetch_complete_history(
    provider: Any,
    *,
    symbol: str,
    expected_start: dt.date,
    end_date: dt.date,
    limiter: _RateLimiter,
    max_pages: int,
) -> AdjFactorSnapshot:
    values: dict[dt.date, AdjFactorRow] = {}
    cursor = end_date
    calls = 0
    for _ in range(max_pages):
        page: tuple[AdjFactorRow, ...] = ()
        for empty_attempt in range(_EMPTY_PAGE_MAX_ATTEMPTS):
            limiter.acquire()
            calls += 1
            try:
                raw = _call_provider(
                    provider,
                    symbol=symbol,
                    expected_start=expected_start,
                    end_date=cursor,
                )
            except Exception:
                if empty_attempt + 1 >= _EMPTY_PAGE_MAX_ATTEMPTS:
                    raise
                time.sleep(min(2**empty_attempt, 8))
                continue
            page = _coerce_provider_rows(raw, symbol=symbol, end_date=end_date)
            if page:
                break
            if empty_attempt + 1 < _EMPTY_PAGE_MAX_ATTEMPTS:
                time.sleep(min(2**empty_attempt, 8))
        if not page:
            break
        for row in page:
            previous = values.get(row.trade_date)
            if previous is not None and previous.value != row.value:
                raise AdjFactorHistoryReconcileError(f"{symbol}: provider pagination returned conflicting values")
            values[row.trade_date] = row
        earliest = min(row.trade_date for row in page)
        if earliest <= expected_start:
            break
        next_cursor = earliest - dt.timedelta(days=1)
        if next_cursor >= cursor:
            raise AdjFactorHistoryReconcileError(f"{symbol}: provider pagination did not advance")
        cursor = next_cursor
    snapshot = _snapshot(symbol, values.values(), provider_call_count=calls)
    if snapshot.first_date > expected_start:
        raise AdjFactorHistoryReconcileError(
            f"{symbol}: provider history begins {snapshot.first_date} after required {expected_start}"
        )
    return snapshot


def _first_difference(local: AdjFactorSnapshot, provider: AdjFactorSnapshot) -> tuple[dt.date, dt.date]:
    left = {row.trade_date: row.value for row in local.rows}
    right = {row.trade_date: row.value for row in provider.rows}
    changed = sorted(day for day in set(left) | set(right) if left.get(day) != right.get(day))
    if not changed:
        raise AdjFactorHistoryReconcileError(f"{local.symbol}: differing snapshot hashes have no differing rows")
    return changed[0], changed[-1]


def _retain_bounded_nontrading_rows(
    local: AdjFactorSnapshot,
    provider: AdjFactorSnapshot,
    *,
    traded_price_dates: frozenset[dt.date],
) -> tuple[AdjFactorSnapshot, tuple[AdjFactorRow, ...]]:
    """Retain a source-omitted suspension row only when both neighbours prove it.

    Tushare occasionally omits an adj-factor row for a suspended open-market
    date even though an earlier daily pull persisted that row.  Such a row is
    safe to retain only when it has no traded price and its value is identical
    to the nearest current-provider values on both sides.  A traded date,
    unbounded date, or factor transition remains fail-closed.
    """

    provider_values = {row.trade_date: row.value for row in provider.rows}
    provider_dates = tuple(sorted(provider_values))
    retained: list[AdjFactorRow] = []
    for row in local.rows:
        if row.trade_date in provider_values:
            continue
        if row.trade_date in traded_price_dates:
            raise AdjFactorHistoryReconcileError(
                f"{local.symbol}: provider history omits a traded local date {row.trade_date}"
            )
        before = next((day for day in reversed(provider_dates) if day < row.trade_date), None)
        after = next((day for day in provider_dates if day > row.trade_date), None)
        if (
            before is None
            or after is None
            or provider_values[before] != row.value
            or provider_values[after] != row.value
        ):
            raise AdjFactorHistoryReconcileError(
                f"{local.symbol}: provider history omits an unbounded or changed nontrading date {row.trade_date}"
            )
        retained.append(row)
    if not retained:
        return provider, ()
    return (
        _snapshot(
            local.symbol,
            (*provider.rows, *retained),
            provider_call_count=provider.provider_call_count,
        ),
        tuple(retained),
    )


def _strict_append_rows(local: AdjFactorSnapshot, provider: AdjFactorSnapshot) -> tuple[AdjFactorRow, ...] | None:
    """Return only a new suffix when every existing local row is unchanged."""

    if not local.rows:
        return None
    local_values = {row.trade_date: row.value for row in local.rows}
    provider_values = {row.trade_date: row.value for row in provider.rows}
    if any(provider_values.get(day) != value for day, value in local_values.items()):
        return None
    additions = tuple(row for row in provider.rows if row.trade_date not in local_values)
    if not additions or any(row.trade_date <= local.last_date for row in additions):
        return None
    return additions


class PostgresAdjFactorHistoryRepository:
    """Session-scoped staging plus one atomic production-table replacement."""

    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def list_symbol_scopes(self, *, end_date: dt.date, symbols: Sequence[str] | None = None) -> Sequence[SymbolScope]:
        requested = tuple(sorted({str(value).strip().upper() for value in symbols or () if value}))
        params: list[Any] = [end_date, end_date]
        predicate = ""
        if requested:
            predicate = "WHERE ts_code = ANY(%s)"
            params.append(list(requested))
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                WITH observed AS (
                    SELECT upper(ts_code) AS ts_code, min(trade_date) AS first_date
                    FROM market.adj_factor
                    WHERE trade_date <= %s
                    GROUP BY upper(ts_code)
                    UNION ALL
                    SELECT upper(ts_code) AS ts_code, min(trade_date) AS first_date
                    FROM market.kline_daily_raw
                    WHERE trade_date <= %s
                      AND (volume_hand > 0 OR amount_li > 0)
                    GROUP BY upper(ts_code)
                ), scoped AS (
                    SELECT ts_code, min(first_date) AS expected_start
                    FROM observed
                    WHERE ts_code ~ '^[0-9]{{6}}\\.(SZ|SH|BJ)$'
                    GROUP BY ts_code
                )
                SELECT ts_code, expected_start
                FROM scoped
                {predicate}
                ORDER BY ts_code
                """,
                params,
            )
            scopes = [SymbolScope(str(code), first_date) for code, first_date in cur.fetchall()]
        if requested:
            found = {scope.symbol for scope in scopes}
            missing = sorted(set(requested) - found)
            if missing:
                raise AdjFactorHistoryReconcileError(
                    f"requested symbols are absent from local price/factor scope: {missing}"
                )
        if not scopes:
            raise AdjFactorHistoryReconcileError("local adj-factor reconciliation universe is empty")
        return scopes

    def load_snapshot(self, symbol: str, *, end_date: dt.date) -> AdjFactorSnapshot:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date, adj_factor
                FROM market.adj_factor
                WHERE ts_code = %s AND trade_date <= %s
                ORDER BY trade_date
                """,
                (symbol, end_date),
            )
            rows = [AdjFactorRow(symbol, day, Decimal(str(value))) for day, value in cur.fetchall()]
        if not rows:
            # A raw-price-only symbol is allowed to be repaired from the provider.
            empty_hash = sha256_hex(canonical_json_bytes([]))
            return AdjFactorSnapshot(
                symbol=symbol,
                rows=(),
                canonical_sha256=empty_hash,
                qfq_sha256=empty_hash,
                first_date=end_date,
                last_date=end_date,
            )
        return _snapshot(symbol, rows)

    def load_price_dates(self, symbol: str, *, end_date: dt.date) -> frozenset[dt.date]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date
                FROM market.kline_daily_raw
                WHERE ts_code = %s AND trade_date <= %s
                  AND (volume_hand > 0 OR amount_li > 0)
                ORDER BY trade_date
                """,
                (symbol, end_date),
            )
            return frozenset(row[0] for row in cur.fetchall())

    def begin_stage(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TEMP TABLE IF NOT EXISTS {_STAGE_TABLE} (
                    ts_code text NOT NULL,
                    trade_date date NOT NULL,
                    adj_factor numeric NOT NULL,
                    PRIMARY KEY (ts_code, trade_date)
                ) ON COMMIT PRESERVE ROWS
                """
            )
            cur.execute(f"TRUNCATE TABLE {_STAGE_TABLE}")

    def stage_rows(self, rows: Sequence[AdjFactorRow]) -> None:
        values = [(row.symbol, row.trade_date, row.value) for row in rows]
        if not values:
            raise AdjFactorHistoryReconcileError("cannot stage an empty adj-factor row set")
        with self.conn.cursor() as cur:
            pgx.execute_values(
                cur,
                f"INSERT INTO {_STAGE_TABLE} (ts_code, trade_date, adj_factor) VALUES %s",
                values,
                page_size=2_000,
            )

    def apply_staged(
        self,
        *,
        symbols: Sequence[str],
        replace_symbols: Sequence[str],
        end_date: dt.date,
        expected_local_sha256: Mapping[str, str],
        expected_row_counts: Mapping[str, int],
    ) -> int:
        ordered = tuple(sorted(set(symbols)))
        replace = tuple(sorted(set(replace_symbols)))
        if not ordered:
            return 0
        if not set(replace).issubset(ordered):
            raise AdjFactorHistoryReconcileError("replace symbols are outside staged symbol scope")
        previous_autocommit = getattr(self.conn, "autocommit", None)
        try:
            if previous_autocommit is True:
                # ``stage_rows`` may leave a real psycopg2 session inside a
                # transaction even when the caller normally uses autocommit.
                # The temp table is ON COMMIT PRESERVE ROWS, so close that
                # staging transaction before opening the atomic replacement
                # transaction.  Assigning ``autocommit = False`` while the
                # session is active raises ``set_session cannot be used inside
                # a transaction``.
                self.conn.commit()
                self.conn.autocommit = False
            with self.conn.cursor() as cur:
                cur.execute("LOCK TABLE market.adj_factor IN SHARE ROW EXCLUSIVE MODE")
            for symbol in ordered:
                current = self.load_snapshot(symbol, end_date=end_date)
                if current.canonical_sha256 != expected_local_sha256[symbol]:
                    raise AdjFactorHistoryReconcileError(
                        f"{symbol}: local history changed while reconciliation was running"
                    )
            with self.conn.cursor() as cur:
                if replace:
                    cur.execute(
                        "DELETE FROM market.adj_factor WHERE ts_code = ANY(%s) AND trade_date <= %s",
                        (list(replace), end_date),
                    )
                cur.execute(
                    f"""
                    INSERT INTO market.adj_factor (ts_code, trade_date, adj_factor)
                    SELECT ts_code, trade_date, adj_factor
                    FROM {_STAGE_TABLE}
                    WHERE ts_code = ANY(%s) AND trade_date <= %s
                    ORDER BY ts_code, trade_date
                    """,
                    (list(ordered), end_date),
                )
                inserted = int(cur.rowcount)
            expected = sum(expected_row_counts[symbol] for symbol in ordered)
            if inserted != expected:
                raise AdjFactorHistoryReconcileError(
                    f"staged row count mismatch: expected {expected}, inserted {inserted}"
                )
            self.conn.commit()
            return inserted
        except Exception:
            self.conn.rollback()
            raise
        finally:
            if previous_autocommit is True:
                self.conn.autocommit = True


class AdjFactorHistoryReconciler:
    def __init__(
        self,
        *,
        repository: AdjFactorHistoryRepository,
        provider_factory: Callable[[], Any],
        workers: int = 4,
        calls_per_minute: int = 480,
        max_pages: int = 4,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be positive")
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        self.repository = repository
        self.provider_factory = provider_factory
        self.workers = workers
        self.max_pages = max_pages
        self.limiter = _RateLimiter(calls_per_minute)

    def _stable_provider_snapshot(self, scope: SymbolScope, *, end_date: dt.date) -> AdjFactorSnapshot:
        provider = self.provider_factory()
        first = _fetch_complete_history(
            provider,
            symbol=scope.symbol,
            expected_start=scope.expected_start,
            end_date=end_date,
            limiter=self.limiter,
            max_pages=self.max_pages,
        )
        second = _fetch_complete_history(
            provider,
            symbol=scope.symbol,
            expected_start=scope.expected_start,
            end_date=end_date,
            limiter=self.limiter,
            max_pages=self.max_pages,
        )
        if first.canonical_sha256 != second.canonical_sha256:
            raise AdjFactorHistoryReconcileError(f"{scope.symbol}: provider history changed between stability reads")
        return AdjFactorSnapshot(
            **{
                **second.__dict__,
                "provider_call_count": first.provider_call_count + second.provider_call_count,
            }
        )

    def reconcile(
        self,
        *,
        end_date: dt.date,
        symbols: Sequence[str] | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        scopes = tuple(self.repository.list_symbol_scopes(end_date=end_date, symbols=symbols))
        self.repository.begin_stage()
        changes: list[dict[str, Any]] = []
        expected_local: dict[str, str] = {}
        expected_rows: dict[str, int] = {}
        provider_identities: list[dict[str, Any]] = []
        retained_nontrading_rows: list[dict[str, str]] = []
        provider_calls = 0

        # Bound outstanding histories so a fast provider cannot retain the full
        # market snapshot in process memory while DB comparison catches up.
        batch_size = max(self.workers * 4, self.workers)
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            for offset in range(0, len(scopes), batch_size):
                batch = scopes[offset : offset + batch_size]
                futures = {
                    pool.submit(self._stable_provider_snapshot, scope, end_date=end_date): scope for scope in batch
                }
                for future in as_completed(futures):
                    scope = futures[future]
                    source_provider = future.result()
                    provider_calls += source_provider.provider_call_count
                    local = self.repository.load_snapshot(scope.symbol, end_date=end_date)
                    traded_price_dates = self.repository.load_price_dates(scope.symbol, end_date=end_date)
                    provider, retained = _retain_bounded_nontrading_rows(
                        local,
                        source_provider,
                        traded_price_dates=traded_price_dates,
                    )
                    provider_dates = {row.trade_date for row in provider.rows}
                    missing_prices = sorted(traded_price_dates - provider_dates)
                    if missing_prices:
                        raise AdjFactorHistoryReconcileError(
                            f"{scope.symbol}: provider history omits required traded price dates "
                            f"(price={len(missing_prices)})"
                        )
                    retained_nontrading_rows.extend(
                        {
                            "symbol": row.symbol,
                            "trade_date": row.trade_date.isoformat(),
                            "adj_factor": _decimal_text(row.value),
                        }
                        for row in retained
                    )
                    provider_identities.append(
                        {
                            "symbol": scope.symbol,
                            "row_count": len(source_provider.rows),
                            "canonical_sha256": source_provider.canonical_sha256,
                        }
                    )
                    if provider.canonical_sha256 == local.canonical_sha256:
                        continue
                    if local.rows:
                        difference_start, difference_end = _first_difference(local, provider)
                        invalidation_start = min(local.first_date, provider.first_date)
                    else:
                        difference_start = provider.first_date
                        difference_end = provider.last_date
                        invalidation_start = provider.first_date
                    append_rows = _strict_append_rows(local, provider)
                    write_mode = "append" if append_rows is not None else "full_replace"
                    staged_rows = append_rows if append_rows is not None else provider.rows
                    qfq_changed = provider.qfq_sha256 != local.qfq_sha256
                    self.repository.stage_rows(staged_rows)
                    expected_local[scope.symbol] = local.canonical_sha256
                    expected_rows[scope.symbol] = len(staged_rows)
                    changes.append(
                        {
                            "symbol": scope.symbol,
                            "write_mode": write_mode,
                            "staged_row_count": len(staged_rows),
                            "local_row_count": len(local.rows),
                            "provider_row_count": len(provider.rows),
                            "local_sha256": local.canonical_sha256,
                            "provider_sha256": provider.canonical_sha256,
                            "local_qfq_sha256": local.qfq_sha256,
                            "provider_qfq_sha256": provider.qfq_sha256,
                            "qfq_changed": qfq_changed,
                            "difference_start": difference_start.isoformat(),
                            "difference_end": difference_end.isoformat(),
                            "invalidation_start": invalidation_start.isoformat(),
                            "invalidation_end": end_date.isoformat(),
                        }
                    )

        changes.sort(key=lambda item: item["symbol"])
        provider_identities.sort(key=lambda item: item["symbol"])
        retained_nontrading_rows.sort(key=lambda item: (item["symbol"], item["trade_date"]))
        changed_symbols = [item["symbol"] for item in changes]
        replaced_symbols = [item["symbol"] for item in changes if item["write_mode"] == "full_replace"]
        qfq_changed_symbols = [item["symbol"] for item in changes if item["qfq_changed"]]
        written = 0
        if changed_symbols and not dry_run:
            written = self.repository.apply_staged(
                symbols=changed_symbols,
                replace_symbols=replaced_symbols,
                end_date=end_date,
                expected_local_sha256=expected_local,
                expected_row_counts=expected_rows,
            )

        return {
            "schema_version": RECEIPT_SCHEMA,
            "status": "dry_run" if dry_run else ("reconciled" if changes else "unchanged"),
            "source_api": SOURCE_API,
            "end_date": end_date.isoformat(),
            "stable_read_count": 2,
            "provider_call_count": provider_calls,
            "scanned_symbol_count": len(scopes),
            "changed_symbol_count": len(changed_symbols),
            "changed_symbols": changed_symbols,
            "append_symbol_count": len(changed_symbols) - len(replaced_symbols),
            "full_replace_symbol_count": len(replaced_symbols),
            "changes": changes,
            "provider_snapshot_sha256": sha256_hex(canonical_json_bytes(provider_identities)),
            "retained_nontrading_row_count": len(retained_nontrading_rows),
            "retained_nontrading_rows": retained_nontrading_rows,
            "database_write_performed": bool(changed_symbols and not dry_run),
            "written_row_count": written,
            "downstream_invalidation": {
                "required": bool(qfq_changed_symbols),
                "components": list(DOWNSTREAM_COMPONENTS) if qfq_changed_symbols else [],
                "instruments": qfq_changed_symbols,
                "reason": ("adj_factor.denominator_or_history->qfq_history" if qfq_changed_symbols else None),
            },
            "llm_used": False,
            "online_search_used": False,
        }


__all__ = [
    "AdjFactorHistoryReconcileError",
    "AdjFactorHistoryReconciler",
    "AdjFactorRow",
    "AdjFactorSnapshot",
    "PostgresAdjFactorHistoryRepository",
    "SymbolScope",
]
