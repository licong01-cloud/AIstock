"""Bounded frozen-source transformation for QE daily and minute stock rows.

The transformer has no database, provider, Qlib or filesystem dependency.  A
build stage supplies globally code/time ordered iterables read from immutable
source CAS partitions.  Only one source row per stream and, for minute data,
one 240-row stock-day are retained at a time.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from bisect import bisect_left, bisect_right
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from types import MappingProxyType
from typing import Any, Callable
from zoneinfo import ZoneInfo

from .canonical import digest_named_fields
from .errors import DatasetReleaseError
from .index_contract import DOMESTIC_INDEX_DEFINITIONS
from .pit import FrozenPitSnapshot
from .stock_schema import QLIB_STOCK_FIELDS, qlib_stock_schema_digest


CANONICAL_STOCK_TRANSFORM_SCHEMA = "dataset_release_canonical_stock_transform_v1"
QFQ_DENOMINATOR_AUTHORITY_SCHEMA = "dataset_release_qfq_denominator_authority_v1"
PRICE_UNIT_DIVISOR = 1000.0
VALUE_COMPARE_ABS_TOL = 1e-4
MINUTE_FREQ = "1m"
_STOCK_CODE = re.compile(r"[0-9]{6}\.(?:SH|SZ)\Z")
_INDEX_CODES = frozenset(item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS)
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_RAW_VALUE_FIELDS = (
    "open_li",
    "high_li",
    "low_li",
    "close_li",
    "volume_hand",
    "amount_li",
)


class CanonicalStockTransformError(DatasetReleaseError):
    code = "BLOCKED_CANONICAL_STOCK_TRANSFORM_INVALID"


def _minute_session_times() -> tuple[time, ...]:
    output: list[time] = []
    current = datetime.combine(date(2000, 1, 1), time(9, 31))
    morning_end = datetime.combine(date(2000, 1, 1), time(11, 30))
    while current <= morning_end:
        output.append(current.time())
        current += timedelta(minutes=1)
    current = datetime.combine(date(2000, 1, 1), time(13, 1))
    afternoon_end = datetime.combine(date(2000, 1, 1), time(15, 0))
    while current <= afternoon_end:
        output.append(current.time())
        current += timedelta(minutes=1)
    if len(output) != 240:  # pragma: no cover - constant construction guard
        raise RuntimeError("canonical A-share minute session must contain 240 rows")
    return tuple(output)


MINUTE_SESSION_TIMES = _minute_session_times()


@dataclass(frozen=True, slots=True)
class QfqDenominatorAuthority:
    cutoff: date
    pit_spans_sha256: str
    values: tuple[tuple[str, float], ...]
    per_code_series: tuple[tuple[str, int, str], ...]
    source_row_count: int
    source_rows_sha256: str
    schema_version: str = QFQ_DENOMINATOR_AUTHORITY_SCHEMA

    def __post_init__(self) -> None:
        if self.source_row_count <= 0 or len(self.source_rows_sha256) != 64:
            raise CanonicalStockTransformError("QFQ denominator authority is incomplete")
        codes = [code for code, _value in self.values]
        if not codes or codes != sorted(codes) or len(codes) != len(set(codes)):
            raise CanonicalStockTransformError("QFQ denominator codes are invalid")
        for code, value in self.values:
            _require_stock_code(code)
            if not math.isfinite(value) or value <= 0:
                raise CanonicalStockTransformError(f"QFQ denominator is invalid: {code}")
        series_codes = [code for code, _rows, _digest in self.per_code_series]
        if series_codes != codes:
            raise CanonicalStockTransformError("QFQ per-code series identities differ from denominators")
        for code, row_count, digest in self.per_code_series:
            if row_count <= 0 or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise CanonicalStockTransformError(f"QFQ per-code ordered series is invalid: {code}")
        if sum(row_count for _code, row_count, _digest in self.per_code_series) != self.source_row_count:
            raise CanonicalStockTransformError("QFQ per-code row counts differ from global source row count")

    @property
    def by_code(self) -> Mapping[str, float]:
        return MappingProxyType(dict(self.values))

    @property
    def digest(self) -> str:
        return digest_named_fields(
            QFQ_DENOMINATOR_AUTHORITY_SCHEMA,
            {
                "cutoff": self.cutoff,
                "pit_spans_sha256": self.pit_spans_sha256,
                "values": self.values,
                "per_code_series": self.per_code_series,
                "source_row_count": self.source_row_count,
                "source_rows_sha256": self.source_rows_sha256,
            },
        )

    def as_dict(self) -> dict[str, Any]:
        series = {
            code: {"adj_row_count": rows, "ordered_adj_series_sha256": digest}
            for code, rows, digest in self.per_code_series
        }
        return {
            "schema_version": self.schema_version,
            "cutoff": self.cutoff.isoformat(),
            "pit_spans_sha256": self.pit_spans_sha256,
            "values": [
                {
                    "ts_code": code,
                    "denominator": value,
                    **series[code],
                }
                for code, value in self.values
            ],
            "source_row_count": self.source_row_count,
            "ordered_adj_series_sha256": self.source_rows_sha256,
            "authority_digest": self.digest,
            "safety": {
                "database_writes": 0,
                "provider_database_writes": 0,
                "candidate_writes": 0,
                "production_writes": 0,
                "production_deletes": 0,
                "production_pointer_changes": 0,
                "service_process_controls": 0,
            },
        }


def qfq_denominator_authority_from_mapping(
    value: Mapping[str, Any],
    *,
    expected_cutoff: date,
    expected_pit_spans_sha256: str,
) -> QfqDenominatorAuthority:
    """Reconstruct and digest-check an artifact-ready QFQ authority receipt."""

    rows = value.get("values")
    if (
        value.get("schema_version") != QFQ_DENOMINATOR_AUTHORITY_SCHEMA
        or value.get("cutoff") != expected_cutoff.isoformat()
        or value.get("pit_spans_sha256") != expected_pit_spans_sha256
        or not isinstance(rows, list)
        or value.get("safety")
        != {
            "database_writes": 0,
            "provider_database_writes": 0,
            "candidate_writes": 0,
            "production_writes": 0,
            "production_deletes": 0,
            "production_pointer_changes": 0,
            "service_process_controls": 0,
        }
    ):
        raise CanonicalStockTransformError("QFQ denominator receipt identity differs")
    try:
        authority = QfqDenominatorAuthority(
            cutoff=expected_cutoff,
            pit_spans_sha256=expected_pit_spans_sha256,
            values=tuple((str(row["ts_code"]), float(row["denominator"])) for row in rows),
            per_code_series=tuple(
                (
                    str(row["ts_code"]),
                    int(row["adj_row_count"]),
                    str(row["ordered_adj_series_sha256"]),
                )
                for row in rows
            ),
            source_row_count=int(value["source_row_count"]),
            source_rows_sha256=str(value["ordered_adj_series_sha256"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise CanonicalStockTransformError("QFQ denominator receipt payload is invalid") from exc
    if value.get("authority_digest") != authority.digest:
        raise CanonicalStockTransformError("QFQ denominator receipt digest differs")
    return authority


@dataclass(frozen=True, slots=True)
class CanonicalStockTransformSpec:
    cutoff: date
    pit_snapshot: FrozenPitSnapshot
    trading_days: tuple[date, ...]
    qfq_denominators: QfqDenominatorAuthority
    instrument_filter: tuple[str, ...] | None = None
    initial_adj_factors: Mapping[str, float] | None = None

    def __post_init__(self) -> None:
        if self.pit_snapshot.cutoff != self.cutoff:
            raise CanonicalStockTransformError("PIT cutoff differs from transform cutoff")
        if (
            self.qfq_denominators.cutoff != self.cutoff
            or self.qfq_denominators.pit_spans_sha256 != self.pit_snapshot.spans_sha256
        ):
            raise CanonicalStockTransformError("QFQ denominator authority differs from PIT/cutoff")
        days = self.trading_days
        if (
            not days
            or tuple(sorted(set(days))) != days
            or days[0] < self.pit_snapshot.scope_start
            or days[-1] > self.cutoff
        ):
            raise CanonicalStockTransformError("frozen trading days must be sorted, unique and scope-bounded")
        codes = {span.ts_code for span in self.pit_snapshot.spans}
        if codes.intersection(_INDEX_CODES):
            raise CanonicalStockTransformError("domestic index codes cannot enter the stock PIT contract")
        if set(self.qfq_denominators.by_code) != codes:
            raise CanonicalStockTransformError("QFQ denominator code set differs from frozen PIT")
        if self.instrument_filter is not None:
            selected = tuple(sorted(set(self.instrument_filter)))
            if selected != self.instrument_filter or not selected or not set(selected).issubset(codes):
                raise CanonicalStockTransformError("transform instrument filter is invalid")
        seeds = dict(self.initial_adj_factors or {})
        if not set(seeds).issubset(codes) or any(
            not math.isfinite(float(value)) or float(value) <= 0 for value in seeds.values()
        ):
            raise CanonicalStockTransformError("transform initial adj-factor seed is invalid")

    @property
    def digest(self) -> str:
        return digest_named_fields(
            CANONICAL_STOCK_TRANSFORM_SCHEMA,
            {
                "cutoff": self.cutoff,
                "pit_spans_sha256": self.pit_snapshot.spans_sha256,
                "trading_days": self.trading_days,
                "qfq_denominators_digest": self.qfq_denominators.digest,
                "instrument_filter": list(self.instrument_filter or ()),
                "initial_adj_factors": {
                    code: float(value) for code, value in sorted((self.initial_adj_factors or {}).items())
                },
                "qlib_stock_schema_digest": qlib_stock_schema_digest(),
            },
        )


@dataclass(slots=True)
class CanonicalStockTransformMetrics:
    dataset: str
    source_rows: dict[str, int] = field(default_factory=dict)
    output_rows: int = 0
    synthesized_stock_days: int = 0
    peak_minute_stock_day_rows: int = 0
    full_frames_materialized: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": CANONICAL_STOCK_TRANSFORM_SCHEMA,
            "dataset": self.dataset,
            "source_rows": {key: self.source_rows[key] for key in sorted(self.source_rows)},
            "output_rows": self.output_rows,
            "synthesized_stock_days": self.synthesized_stock_days,
            "peak_minute_stock_day_rows": self.peak_minute_stock_day_rows,
            "memory_contract": {
                "mode": "one_row_per_source_plus_one_stock_day_v1",
                "max_minute_stock_day_rows": 240,
                "full_frames_materialized": self.full_frames_materialized,
            },
        }


def build_qfq_denominator_authority(
    rows: Iterable[Mapping[str, Any]],
    *,
    pit_snapshot: FrozenPitSnapshot,
    cutoff: date,
) -> QfqDenominatorAuthority:
    """Compute exact per-stock QFQ bases in one O(instruments) streaming pass."""

    if pit_snapshot.cutoff != cutoff:
        raise CanonicalStockTransformError("QFQ cutoff differs from PIT cutoff")
    expected_codes = {span.ts_code for span in pit_snapshot.spans}
    maxima: dict[str, float] = {}
    per_code_digests: dict[str, Any] = {}
    per_code_counts: dict[str, int] = {}
    source_row_count = 0
    source_digest = hashlib.sha256()
    previous: tuple[str, date] | None = None
    for ordinal, raw in enumerate(rows):
        row = _normalize_adj_row(raw, source="adj_factor", ordinal=ordinal)
        key = (row["ts_code"], row["trade_date"])
        if previous is not None and key <= previous:
            raise CanonicalStockTransformError("adj_factor rows must be globally code/date ordered and unique")
        previous = key
        code = row["ts_code"]
        observed_date = row["trade_date"]
        if code not in expected_codes or observed_date > cutoff:
            continue
        value = row["adj_factor"]
        maxima[code] = max(value, maxima.get(code, value))
        source_row_count += 1
        canonical_row = (
            json.dumps(
                [code, observed_date.isoformat(), value],
                ensure_ascii=True,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("ascii")
            + b"\n"
        )
        source_digest.update(canonical_row)
        per_code_digests.setdefault(code, hashlib.sha256()).update(canonical_row)
        per_code_counts[code] = per_code_counts.get(code, 0) + 1
    missing = sorted(expected_codes.difference(maxima))
    if missing:
        raise CanonicalStockTransformError(
            "adj_factor cannot establish every PIT stock denominator",
            context={"missing_sample": missing[:20], "missing_count": len(missing)},
        )
    return QfqDenominatorAuthority(
        cutoff=cutoff,
        pit_spans_sha256=pit_snapshot.spans_sha256,
        values=tuple(sorted(maxima.items())),
        per_code_series=tuple(
            (
                code,
                per_code_counts[code],
                per_code_digests[code].hexdigest(),
            )
            for code in sorted(maxima)
        ),
        source_row_count=source_row_count,
        source_rows_sha256=source_digest.hexdigest(),
    )


class CanonicalStockTransformer:
    """Transform frozen raw streams into the exact twelve-field Qlib rows."""

    def transform_daily(
        self,
        spec: CanonicalStockTransformSpec,
        *,
        daily_rows: Iterable[Mapping[str, Any]],
        adj_factor_rows: Iterable[Mapping[str, Any]],
        stk_limit_rows: Iterable[Mapping[str, Any]],
        suspend_rows: Iterable[Mapping[str, Any]],
        checkpoint: Callable[[], None] = lambda: None,
        checkpoint_rows: int = 10_000,
        metrics: CanonicalStockTransformMetrics | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        report = metrics or CanonicalStockTransformMetrics("daily_bin")
        _validate_metrics(report, dataset="daily_bin", checkpoint_rows=checkpoint_rows)
        context = _TransformContext(spec)
        raw = _Cursor(
            _iter_normalized(
                daily_rows,
                source="kline_daily_raw",
                normalizer=_normalize_daily_row,
                sort_key=lambda row: (row["ts_code"], row["trade_date"]),
                include=lambda row: context.is_expected_day(row["ts_code"], row["trade_date"]),
                report=report,
            ),
            key=lambda row: (row["ts_code"], row["trade_date"]),
        )
        adj = _Cursor(
            _iter_normalized(
                adj_factor_rows,
                source="adj_factor",
                normalizer=_normalize_adj_row,
                sort_key=lambda row: (row["ts_code"], row["trade_date"]),
                include=lambda row: row["ts_code"] in context.codes and row["trade_date"] <= spec.cutoff,
                report=report,
            ),
            key=lambda row: (row["ts_code"], row["trade_date"]),
        )
        limits = _Cursor(
            _iter_normalized(
                stk_limit_rows,
                source="stk_limit",
                normalizer=_normalize_limit_row,
                sort_key=lambda row: (row["ts_code"], row["trade_date"]),
                include=lambda row: context.is_expected_day(row["ts_code"], row["trade_date"]),
                report=report,
            ),
            key=lambda row: (row["ts_code"], row["trade_date"]),
        )
        suspends = _Cursor(
            _iter_normalized(
                suspend_rows,
                source="suspend_d",
                normalizer=_normalize_suspend_row,
                sort_key=lambda row: (
                    row["ts_code"],
                    row["trade_date"],
                    row["suspend_type"],
                ),
                include=lambda row: context.is_expected_day(row["ts_code"], row["trade_date"]),
                report=report,
            ),
            key=lambda row: (row["ts_code"], row["trade_date"]),
        )
        latest_adj_by_code = {code: float(value) for code, value in (spec.initial_adj_factors or {}).items()}
        for code, trading_day in context.expected_code_days():
            target = (code, trading_day)
            _consume_adj_through(adj, target, latest_adj_by_code)
            qfq = _qfq_for_day(spec, latest_adj_by_code, code, trading_day)
            limit = _pop_exact_one(limits, target, source="stk_limit")
            if limit is None:
                _missing_day("stk_limit", code, trading_day)
            suspend = _pop_group(suspends, target)
            full_day_suspend = _is_full_day_suspend(suspend)
            row = _pop_exact_one(raw, target, source="kline_daily_raw")
            if row is None:
                if not full_day_suspend:
                    _missing_day("kline_daily_raw_without_full_day_suspend", code, trading_day)
                output = _synthesized_row(
                    code=code,
                    timestamp=datetime.combine(trading_day, time()),
                    qfq=qfq,
                    limit=limit,
                )
                report.synthesized_stock_days += 1
            else:
                output = _transform_raw_row(
                    row,
                    timestamp=datetime.combine(trading_day, time()),
                    qfq=qfq,
                    limit=limit,
                )
            report.output_rows += 1
            yield output
            if report.output_rows % checkpoint_rows == 0:
                checkpoint()
        _finish_cursors(raw, limits, suspends)
        adj.drain()
        checkpoint()

    def transform_minute(
        self,
        spec: CanonicalStockTransformSpec,
        *,
        minute_rows: Iterable[Mapping[str, Any]],
        adj_factor_rows: Iterable[Mapping[str, Any]],
        stk_limit_rows: Iterable[Mapping[str, Any]],
        suspend_rows: Iterable[Mapping[str, Any]],
        checkpoint: Callable[[], None] = lambda: None,
        checkpoint_rows: int = 10_000,
        metrics: CanonicalStockTransformMetrics | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        report = metrics or CanonicalStockTransformMetrics("minute_bin")
        _validate_metrics(report, dataset="minute_bin", checkpoint_rows=checkpoint_rows)
        context = _TransformContext(spec)
        raw = _Cursor(
            _iter_normalized(
                minute_rows,
                source="kline_minute_raw",
                normalizer=_normalize_minute_row,
                sort_key=lambda row: (row["ts_code"], row["trade_time"], row["freq"]),
                include=lambda row: context.is_expected_day(row["ts_code"], row["trade_time"].date()),
                report=report,
            ),
            key=lambda row: (row["ts_code"], row["trade_time"].date()),
        )
        adj = _Cursor(
            _iter_normalized(
                adj_factor_rows,
                source="adj_factor",
                normalizer=_normalize_adj_row,
                sort_key=lambda row: (row["ts_code"], row["trade_date"]),
                include=lambda row: row["ts_code"] in context.codes and row["trade_date"] <= spec.cutoff,
                report=report,
            ),
            key=lambda row: (row["ts_code"], row["trade_date"]),
        )
        limits = _Cursor(
            _iter_normalized(
                stk_limit_rows,
                source="stk_limit",
                normalizer=_normalize_limit_row,
                sort_key=lambda row: (row["ts_code"], row["trade_date"]),
                include=lambda row: context.is_expected_day(row["ts_code"], row["trade_date"]),
                report=report,
            ),
            key=lambda row: (row["ts_code"], row["trade_date"]),
        )
        suspends = _Cursor(
            _iter_normalized(
                suspend_rows,
                source="suspend_d",
                normalizer=_normalize_suspend_row,
                sort_key=lambda row: (
                    row["ts_code"],
                    row["trade_date"],
                    row["suspend_type"],
                ),
                include=lambda row: context.is_expected_day(row["ts_code"], row["trade_date"]),
                report=report,
            ),
            key=lambda row: (row["ts_code"], row["trade_date"]),
        )
        latest_adj_by_code = {code: float(value) for code, value in (spec.initial_adj_factors or {}).items()}
        for code, trading_day in context.expected_code_days():
            target = (code, trading_day)
            _consume_adj_through(adj, target, latest_adj_by_code)
            qfq = _qfq_for_day(spec, latest_adj_by_code, code, trading_day)
            limit = _pop_exact_one(limits, target, source="stk_limit")
            if limit is None:
                _missing_day("stk_limit", code, trading_day)
            suspend = _pop_group(suspends, target)
            full_day_suspend = _is_full_day_suspend(suspend)
            day_rows = _pop_group(raw, target, hard_limit=240)
            report.peak_minute_stock_day_rows = max(
                report.peak_minute_stock_day_rows,
                len(day_rows),
            )
            if not day_rows:
                if not full_day_suspend:
                    _missing_day("kline_minute_raw_without_full_day_suspend", code, trading_day)
                report.synthesized_stock_days += 1
                for session_time in MINUTE_SESSION_TIMES:
                    output = _synthesized_row(
                        code=code,
                        timestamp=datetime.combine(trading_day, session_time),
                        qfq=qfq,
                        limit=limit,
                    )
                    report.output_rows += 1
                    yield output
                    if report.output_rows % checkpoint_rows == 0:
                        checkpoint()
                continue
            observed_times = tuple(row["trade_time"].time() for row in day_rows)
            if len(day_rows) != 240 or observed_times != MINUTE_SESSION_TIMES:
                raise CanonicalStockTransformError(
                    "minute stock-day is not the exact 240-row Shanghai session",
                    context={
                        "ts_code": code,
                        "trade_date": trading_day.isoformat(),
                        "row_count": len(day_rows),
                        "first": day_rows[0]["trade_time"].isoformat(),
                        "last": day_rows[-1]["trade_time"].isoformat(),
                    },
                )
            for row in day_rows:
                output = _transform_raw_row(
                    row,
                    timestamp=row["trade_time"],
                    qfq=qfq,
                    limit=limit,
                )
                report.output_rows += 1
                yield output
                if report.output_rows % checkpoint_rows == 0:
                    checkpoint()
        _finish_cursors(raw, limits, suspends)
        adj.drain()
        checkpoint()


class _TransformContext:
    def __init__(self, spec: CanonicalStockTransformSpec) -> None:
        self.spec = spec
        all_codes = frozenset(span.ts_code for span in spec.pit_snapshot.spans)
        self.codes = frozenset(spec.instrument_filter) if spec.instrument_filter is not None else all_codes
        self._trading_days = spec.trading_days
        self._trading_day_set = frozenset(spec.trading_days)
        spans: dict[str, list[tuple[date, date]]] = {}
        for span in spec.pit_snapshot.spans:
            if span.ts_code not in self.codes:
                continue
            spans.setdefault(span.ts_code, []).append((span.eligible_start, span.eligible_end))
        self._spans = {code: tuple(values) for code, values in spans.items()}

    def is_expected_day(self, code: str, observed: date) -> bool:
        if code not in self.codes or observed not in self._trading_day_set:
            return False
        return any(start <= observed <= end for start, end in self._spans[code])

    def expected_code_days(self) -> Iterator[tuple[str, date]]:
        for code in sorted(self._spans):
            for start, end in self._spans[code]:
                left = bisect_left(self._trading_days, start)
                right = bisect_right(self._trading_days, end)
                for observed in self._trading_days[left:right]:
                    yield code, observed


class _Cursor:
    def __init__(
        self,
        rows: Iterable[Mapping[str, Any]],
        *,
        key: Callable[[Mapping[str, Any]], tuple[str, date]],
    ) -> None:
        self._iterator = iter(rows)
        self._key = key
        self.current: Mapping[str, Any] | None = None
        self.advance()

    @property
    def current_key(self) -> tuple[str, date] | None:
        return None if self.current is None else self._key(self.current)

    def advance(self) -> None:
        try:
            self.current = next(self._iterator)
        except StopIteration:
            self.current = None

    def drain(self) -> None:
        while self.current is not None:
            self.advance()


def _iter_normalized(
    rows: Iterable[Mapping[str, Any]],
    *,
    source: str,
    normalizer: Callable[..., dict[str, Any]],
    sort_key: Callable[[Mapping[str, Any]], tuple[Any, ...]],
    include: Callable[[Mapping[str, Any]], bool],
    report: CanonicalStockTransformMetrics,
) -> Iterator[Mapping[str, Any]]:
    previous: tuple[Any, ...] | None = None
    for ordinal, raw in enumerate(rows):
        report.source_rows[source] = report.source_rows.get(source, 0) + 1
        row = normalizer(raw, source=source, ordinal=ordinal)
        key = sort_key(row)
        if previous is not None and key <= previous:
            raise CanonicalStockTransformError(f"{source} rows must be globally ordered and unique")
        previous = key
        if include(row):
            yield row


def _normalize_daily_row(raw: Mapping[str, Any], *, source: str, ordinal: int) -> dict[str, Any]:
    row = _normalize_raw_values(raw, source=source, ordinal=ordinal)
    row["trade_date"] = _as_date(raw.get("trade_date"), source=source, ordinal=ordinal)
    return row


def _normalize_minute_row(raw: Mapping[str, Any], *, source: str, ordinal: int) -> dict[str, Any]:
    row = _normalize_raw_values(raw, source=source, ordinal=ordinal)
    row["trade_time"] = _as_shanghai_datetime(raw.get("trade_time"), source=source, ordinal=ordinal)
    frequency = str(raw.get("freq", "")).strip().lower()
    if frequency != MINUTE_FREQ:
        raise CanonicalStockTransformError(f"{source} row has non-authoritative frequency at ordinal {ordinal}")
    row["freq"] = frequency
    return row


def _normalize_raw_values(raw: Mapping[str, Any], *, source: str, ordinal: int) -> dict[str, Any]:
    code = _require_stock_code(raw.get("ts_code"), source=source, ordinal=ordinal)
    output: dict[str, Any] = {"ts_code": code}
    for field_name in _RAW_VALUE_FIELDS:
        value = _finite_float(
            raw.get(field_name),
            field=field_name,
            source=source,
            ordinal=ordinal,
        )
        if field_name in {"open_li", "high_li", "low_li", "close_li"} and value <= 0:
            raise CanonicalStockTransformError(f"{source} {field_name} must be positive at ordinal {ordinal}")
        if field_name in {"volume_hand", "amount_li"} and value < 0:
            raise CanonicalStockTransformError(f"{source} {field_name} must be nonnegative at ordinal {ordinal}")
        output[field_name] = value
    return output


def _normalize_adj_row(raw: Mapping[str, Any], *, source: str, ordinal: int) -> dict[str, Any]:
    value = _finite_float(
        raw.get("adj_factor"),
        field="adj_factor",
        source=source,
        ordinal=ordinal,
    )
    if value <= 0:
        raise CanonicalStockTransformError(f"{source} adj_factor must be positive at ordinal {ordinal}")
    return {
        "ts_code": _require_stock_code(raw.get("ts_code"), source=source, ordinal=ordinal),
        "trade_date": _as_date(raw.get("trade_date"), source=source, ordinal=ordinal),
        "adj_factor": value,
    }


def _normalize_limit_row(raw: Mapping[str, Any], *, source: str, ordinal: int) -> dict[str, Any]:
    output = {
        "ts_code": _require_stock_code(raw.get("ts_code"), source=source, ordinal=ordinal),
        "trade_date": _as_date(raw.get("trade_date"), source=source, ordinal=ordinal),
    }
    for field_name in ("pre_close", "up_limit", "down_limit"):
        value = _finite_float(
            raw.get(field_name),
            field=field_name,
            source=source,
            ordinal=ordinal,
        )
        if value <= 0:
            raise CanonicalStockTransformError(f"{source} {field_name} must be positive at ordinal {ordinal}")
        output[field_name] = value
    if not output["down_limit"] <= output["pre_close"] <= output["up_limit"]:
        raise CanonicalStockTransformError(f"{source} price bounds are invalid at ordinal {ordinal}")
    return output


def _normalize_suspend_row(raw: Mapping[str, Any], *, source: str, ordinal: int) -> dict[str, Any]:
    suspend_type = str(raw.get("suspend_type", "")).strip().upper()
    if suspend_type not in {"S", "R"}:
        raise CanonicalStockTransformError(f"{source} suspend_type is invalid at ordinal {ordinal}")
    timing = raw.get("suspend_timing")
    if timing is not None:
        timing = str(timing).strip()
        if not timing:
            raise CanonicalStockTransformError(f"{source} empty suspend_timing is ambiguous at ordinal {ordinal}")
    return {
        "ts_code": _require_stock_code(raw.get("ts_code"), source=source, ordinal=ordinal),
        "trade_date": _as_date(raw.get("trade_date"), source=source, ordinal=ordinal),
        "suspend_type": suspend_type,
        "suspend_timing": timing,
    }


def _require_stock_code(
    value: Any,
    *,
    source: str = "stock_contract",
    ordinal: int = 0,
) -> str:
    code = str(value or "").strip().upper()
    if code in _INDEX_CODES:
        raise CanonicalStockTransformError(
            f"{source} domestic index code entered stock stream at ordinal {ordinal}: {code}"
        )
    if not _STOCK_CODE.fullmatch(code):
        raise CanonicalStockTransformError(f"{source} stock code is invalid at ordinal {ordinal}: {code}")
    return code


def _as_date(value: Any, *, source: str, ordinal: int) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError) as exc:
        raise CanonicalStockTransformError(f"{source} date is invalid at ordinal {ordinal}") from exc


def _as_shanghai_datetime(value: Any, *, source: str, ordinal: int) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise CanonicalStockTransformError(f"{source} trade_time is invalid at ordinal {ordinal}") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_SHANGHAI).replace(tzinfo=None)
    if parsed.second or parsed.microsecond:
        raise CanonicalStockTransformError(f"{source} trade_time is not minute-aligned at ordinal {ordinal}")
    return parsed


def _finite_float(value: Any, *, field: str, source: str, ordinal: int) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise CanonicalStockTransformError(f"{source} {field} is not numeric at ordinal {ordinal}") from exc
    if not math.isfinite(number):
        raise CanonicalStockTransformError(f"{source} {field} is non-finite at ordinal {ordinal}")
    return number


def _consume_adj_through(
    cursor: _Cursor,
    target: tuple[str, date],
    latest_by_code: dict[str, float],
) -> None:
    code, observed_date = target
    while cursor.current is not None:
        current_code, current_date = cursor.current_key or ("", date.min)
        if current_code < code or (current_code == code and current_date <= observed_date):
            if current_code == code:
                latest_by_code[code] = float(cursor.current["adj_factor"])
            cursor.advance()
            continue
        break


def _qfq_for_day(
    spec: CanonicalStockTransformSpec,
    latest_adj_by_code: Mapping[str, float],
    code: str,
    trading_day: date,
) -> float:
    numerator = latest_adj_by_code.get(code)
    denominator = spec.qfq_denominators.by_code[code]
    if numerator is None or numerator <= 0 or numerator > denominator:
        raise CanonicalStockTransformError(
            "adj_factor is missing or exceeds the frozen QFQ denominator",
            context={"ts_code": code, "trade_date": trading_day.isoformat()},
        )
    qfq = numerator / denominator
    if not math.isfinite(qfq) or qfq <= 0 or qfq > 1.0 + 1e-12:
        raise CanonicalStockTransformError(f"QFQ factor is invalid: {code}:{trading_day.isoformat()}")
    return qfq


def _pop_exact_one(
    cursor: _Cursor,
    target: tuple[str, date],
    *,
    source: str,
) -> Mapping[str, Any] | None:
    current_key = cursor.current_key
    if current_key is not None and current_key < target:
        raise CanonicalStockTransformError(f"{source} contains an unexpected PIT/trading key: {current_key}")
    if current_key != target:
        return None
    row = cursor.current
    cursor.advance()
    if cursor.current_key == target:
        raise CanonicalStockTransformError(f"{source} contains a duplicate day key")
    return row


def _pop_group(
    cursor: _Cursor,
    target: tuple[str, date],
    *,
    hard_limit: int = 2,
) -> list[Mapping[str, Any]]:
    current_key = cursor.current_key
    if current_key is not None and current_key < target:
        raise CanonicalStockTransformError(f"source contains an unexpected PIT/trading key: {current_key}")
    output: list[Mapping[str, Any]] = []
    while cursor.current_key == target:
        if len(output) >= hard_limit:
            raise CanonicalStockTransformError(f"source stock-day exceeds hard row bound: {target}")
        assert cursor.current is not None
        output.append(cursor.current)
        cursor.advance()
    return output


def _is_full_day_suspend(rows: Iterable[Mapping[str, Any]]) -> bool:
    values = tuple(rows)
    starts = [row for row in values if row["suspend_type"] == "S"]
    return len(starts) == 1 and starts[0]["suspend_timing"] is None


def _missing_day(source: str, code: str, trading_day: date) -> None:
    raise CanonicalStockTransformError(
        f"required stock-day source is missing: {source}",
        context={"ts_code": code, "trade_date": trading_day.isoformat()},
    )


def _transform_raw_row(
    row: Mapping[str, Any],
    *,
    timestamp: datetime,
    qfq: float,
    limit: Mapping[str, Any],
) -> dict[str, Any]:
    raw_open = float(row["open_li"]) / PRICE_UNIT_DIVISOR
    raw_high = float(row["high_li"]) / PRICE_UNIT_DIVISOR
    raw_low = float(row["low_li"]) / PRICE_UNIT_DIVISOR
    raw_close = float(row["close_li"]) / PRICE_UNIT_DIVISOR
    output = {
        "datetime": _format_timestamp(timestamp),
        "instrument": str(row["ts_code"]),
        "open": raw_open * qfq,
        "high": raw_high * qfq,
        "low": raw_low * qfq,
        "close": raw_close * qfq,
        "volume": float(row["volume_hand"]) * 100.0 / qfq,
        "amount": float(row["amount_li"]) / PRICE_UNIT_DIVISOR,
        "factor": qfq,
        "up_limit_price": float(limit["up_limit"]),
        "down_limit_price": float(limit["down_limit"]),
        "prev_close": float(limit["pre_close"]),
        "limit_up": float(raw_close >= float(limit["up_limit"]) - VALUE_COMPARE_ABS_TOL),
        "limit_down": float(raw_close <= float(limit["down_limit"]) + VALUE_COMPARE_ABS_TOL),
    }
    _validate_output(output)
    return output


def _synthesized_row(
    *,
    code: str,
    timestamp: datetime,
    qfq: float,
    limit: Mapping[str, Any],
) -> dict[str, Any]:
    adjusted = float(limit["pre_close"]) * qfq
    output = {
        "datetime": _format_timestamp(timestamp),
        "instrument": code,
        "open": adjusted,
        "high": adjusted,
        "low": adjusted,
        "close": adjusted,
        "volume": 0.0,
        "amount": 0.0,
        "factor": qfq,
        "up_limit_price": float(limit["up_limit"]),
        "down_limit_price": float(limit["down_limit"]),
        "prev_close": float(limit["pre_close"]),
        "limit_up": 0.0,
        "limit_down": 0.0,
    }
    _validate_output(output)
    return output


def _format_timestamp(value: datetime) -> str:
    return value.date().isoformat() if value.time() == time() else value.isoformat(sep=" ")


def _validate_output(row: Mapping[str, Any]) -> None:
    if tuple(key for key in row if key not in {"datetime", "instrument"}) != QLIB_STOCK_FIELDS:
        raise CanonicalStockTransformError("canonical stock output field order differs")
    if any(not math.isfinite(float(row[field])) for field in QLIB_STOCK_FIELDS):
        raise CanonicalStockTransformError("canonical stock output contains non-finite values")


def _finish_cursors(*cursors: _Cursor) -> None:
    for cursor in cursors:
        if cursor.current is not None:
            raise CanonicalStockTransformError(f"unconsumed expected source key remains: {cursor.current_key}")
        cursor.drain()


def _validate_metrics(
    report: CanonicalStockTransformMetrics,
    *,
    dataset: str,
    checkpoint_rows: int,
) -> None:
    if report.dataset != dataset or checkpoint_rows <= 0:
        raise CanonicalStockTransformError("transform metrics/checkpoint contract differs")
    if report.output_rows or report.source_rows or report.synthesized_stock_days:
        raise CanonicalStockTransformError("transform metrics object is not fresh")


__all__ = [
    "CANONICAL_STOCK_TRANSFORM_SCHEMA",
    "MINUTE_SESSION_TIMES",
    "CanonicalStockTransformError",
    "CanonicalStockTransformMetrics",
    "CanonicalStockTransformSpec",
    "CanonicalStockTransformer",
    "QfqDenominatorAuthority",
    "build_qfq_denominator_authority",
    "qfq_denominator_authority_from_mapping",
]
