"""Shared PIT resolver for core-index stock universes.

The database table stores published index membership.  Trading consumers use
the intersection of that membership with the canonical equity PIT universe.
No price, factor, or dataset component is read or rewritten here.
"""

from __future__ import annotations

import bisect
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

import psycopg2.extras as pgx

from backend.db.pg_pool import get_conn
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_UNIVERSE_KEY,
)


_TS_CODE_RE = re.compile(r"^[0-9]{6}\.(?:SH|SZ)$")
_INDEX_CODE_RE = re.compile(r"^[0-9]{6}\.(?:SH|SZ|CSI)$")


@dataclass(frozen=True, slots=True)
class PoolDefinition:
    pool_id: str
    index_code: str
    source_provider: str
    launch_date: date
    history_start: date
    priority: str


_DATASET_START = date(2018, 8, 1)
POOL_DEFINITIONS: Mapping[str, PoolDefinition] = MappingProxyType(
    {
        "csi300": PoolDefinition("csi300", "000300.SH", "CSI", date(2005, 4, 8), _DATASET_START, "P0"),
        "csi500": PoolDefinition("csi500", "000905.SH", "CSI", date(2007, 1, 15), _DATASET_START, "P0"),
        "csi1000": PoolDefinition("csi1000", "000852.SH", "CSI", date(2014, 10, 17), _DATASET_START, "P0"),
        "star50": PoolDefinition("star50", "000688.SH", "SSE", date(2020, 7, 22), date(2020, 7, 22), "P0"),
        "star100": PoolDefinition("star100", "000698.SH", "SSE", date(2023, 8, 7), date(2023, 8, 7), "P0"),
        "sse50": PoolDefinition("sse50", "000016.SH", "SSE", date(2004, 1, 2), _DATASET_START, "P1"),
        "chinext": PoolDefinition("chinext", "399006.SZ", "CNINDEX", date(2010, 6, 1), _DATASET_START, "P1"),
        "csi_a500": PoolDefinition("csi_a500", "000510.SH", "CSI", date(2024, 9, 23), date(2024, 9, 23), "P1"),
        "csi2000": PoolDefinition("csi2000", "932000.CSI", "CSI", date(2023, 8, 11), date(2023, 8, 11), "P1"),
        "csi800": PoolDefinition("csi800", "000906.SH", "CSI", date(2007, 1, 15), _DATASET_START, "P2"),
        "szse_component": PoolDefinition(
            "szse_component", "399001.SZ", "CNINDEX", date(1995, 1, 23), _DATASET_START, "P2"
        ),
        "sse180": PoolDefinition("sse180", "000010.SH", "SSE", date(2002, 7, 1), _DATASET_START, "P2"),
        "szse100": PoolDefinition("szse100", "399330.SZ", "CNINDEX", date(2006, 1, 24), _DATASET_START, "P2"),
        "chinext50": PoolDefinition("chinext50", "399673.SZ", "CNINDEX", date(2014, 6, 18), _DATASET_START, "P2"),
        "csi_all_share": PoolDefinition("csi_all_share", "000985.CSI", "CSI", date(2005, 1, 4), _DATASET_START, "P2"),
    }
)
P0_POOL_IDS = tuple(pool.pool_id for pool in POOL_DEFINITIONS.values() if pool.priority == "P0")


class UniverseMode(str, Enum):
    STOCK_UNIVERSE = "stock_universe"
    SINGLE_INDEX = "single_index"
    INDEX_UNION = "index_union"


class UniverseUnavailableReason(str, Enum):
    UNKNOWN_POOL_ID = "unknown_pool_id"
    MEMBERSHIP_HISTORY_UNAVAILABLE = "membership_history_unavailable"
    MEMBERSHIP_INTERVAL_INVALID = "membership_interval_invalid"
    CANONICAL_EQUITY_PIT_UNAVAILABLE = "canonical_equity_pit_unavailable"


class CoreIndexMembershipUnavailable(RuntimeError):
    """Typed failure returned instead of a fallback or an empty success."""

    def __init__(self, reason: UniverseUnavailableReason, message: str) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class UniverseSelection:
    mode: UniverseMode = UniverseMode.STOCK_UNIVERSE
    pool_ids: tuple[str, ...] = ()
    benchmark_code: str = "000300.SH"

    def __post_init__(self) -> None:
        try:
            mode = self.mode if isinstance(self.mode, UniverseMode) else UniverseMode(str(self.mode))
        except ValueError as exc:
            raise CoreIndexMembershipUnavailable(
                UniverseUnavailableReason.UNKNOWN_POOL_ID,
                f"unknown universe mode: {self.mode!r}",
            ) from exc
        pools = tuple(sorted({str(value).strip().lower() for value in self.pool_ids if str(value).strip()}))
        unknown = sorted(set(pools) - set(POOL_DEFINITIONS))
        if unknown:
            raise CoreIndexMembershipUnavailable(
                UniverseUnavailableReason.UNKNOWN_POOL_ID,
                f"unknown core-index pool ids: {unknown}",
            )
        if mode is UniverseMode.STOCK_UNIVERSE and pools:
            raise CoreIndexMembershipUnavailable(
                UniverseUnavailableReason.UNKNOWN_POOL_ID,
                "stock_universe mode must not declare pool_ids",
            )
        if mode is UniverseMode.SINGLE_INDEX and len(pools) != 1:
            raise CoreIndexMembershipUnavailable(
                UniverseUnavailableReason.UNKNOWN_POOL_ID,
                "single_index mode requires exactly one pool_id",
            )
        if mode is UniverseMode.INDEX_UNION and not pools:
            raise CoreIndexMembershipUnavailable(
                UniverseUnavailableReason.UNKNOWN_POOL_ID,
                "index_union mode requires at least one pool_id",
            )
        benchmark = str(self.benchmark_code or "").strip().upper()
        if not _INDEX_CODE_RE.fullmatch(benchmark):
            raise CoreIndexMembershipUnavailable(
                UniverseUnavailableReason.UNKNOWN_POOL_ID,
                f"invalid benchmark_code: {self.benchmark_code!r}",
            )
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "pool_ids", pools)
        object.__setattr__(self, "benchmark_code", benchmark)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "UniverseSelection":
        if not value:
            return cls()
        raw_mode = value.get("mode") or UniverseMode.STOCK_UNIVERSE
        raw_pools = value.get("pool_ids") or ()
        if isinstance(raw_pools, str):
            raw_pools = (raw_pools,)
        if not isinstance(raw_pools, Sequence):
            raise CoreIndexMembershipUnavailable(
                UniverseUnavailableReason.UNKNOWN_POOL_ID,
                "pool_ids must be a sequence",
            )
        return cls(
            mode=raw_mode,
            pool_ids=tuple(str(item) for item in raw_pools),
            benchmark_code=str(value.get("benchmark_code") or "000300.SH"),
        )


@dataclass(frozen=True, slots=True)
class MembershipInterval:
    pool_id: str
    index_code: str
    ts_code: str
    effective_from: date
    effective_to_exclusive: date | None
    source_provider: str
    source_reference: str
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class CanonicalEquityInterval:
    ts_code: str
    eligible_start: date
    eligible_end: date


@dataclass(frozen=True, slots=True)
class UniverseInterval:
    ts_code: str
    eligible_start: date
    eligible_end: date


@dataclass(frozen=True, slots=True)
class PoolCoverage:
    pool_id: str
    first_effective_from: date
    row_count: int
    revision: datetime


@dataclass(frozen=True, slots=True)
class ResolvedUniverse:
    mode: UniverseMode
    pool_ids: tuple[str, ...]
    benchmark_code: str
    membership_revision: str
    intervals: tuple[UniverseInterval, ...]
    source_pool_ids_by_symbol: Mapping[str, tuple[str, ...]]


class MembershipRepository(Protocol):
    def fetch_pool_coverage(self, pool_ids: Sequence[str]) -> Mapping[str, PoolCoverage]: ...

    def fetch_membership_intervals(
        self, pool_ids: Sequence[str], start_date: date, end_date: date
    ) -> Sequence[MembershipInterval]: ...

    def fetch_canonical_intervals(self, start_date: date, end_date: date) -> Sequence[CanonicalEquityInterval]: ...

    def fetch_trading_dates(self, start_date: date, end_date: date) -> Sequence[date]: ...


class CoreIndexMembershipRepository:
    """Read the shared membership table and canonical PIT from PostgreSQL."""

    def __init__(self, connection_factory: Callable[[], Any] = get_conn) -> None:
        self._connection_factory = connection_factory

    def fetch_pool_coverage(self, pool_ids: Sequence[str]) -> Mapping[str, PoolCoverage]:
        with self._connection_factory() as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT pool_id, MIN(effective_from) AS first_effective_from,
                           COUNT(*)::BIGINT AS row_count, MAX(updated_at) AS revision
                      FROM market.core_index_membership_pit
                     WHERE pool_id = ANY(%s)
                     GROUP BY pool_id
                    """,
                    (list(pool_ids),),
                )
                rows = list(cur.fetchall())
        return {
            str(row["pool_id"]): PoolCoverage(
                pool_id=str(row["pool_id"]),
                first_effective_from=_require_date(row["first_effective_from"], "first_effective_from"),
                row_count=int(row["row_count"]),
                revision=_require_datetime(row["revision"], "revision"),
            )
            for row in rows
        }

    def fetch_membership_intervals(
        self, pool_ids: Sequence[str], start_date: date, end_date: date
    ) -> Sequence[MembershipInterval]:
        with self._connection_factory() as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT pool_id, index_code, ts_code, effective_from,
                           effective_to_exclusive, source_provider,
                           source_reference, updated_at
                      FROM market.core_index_membership_pit
                     WHERE pool_id = ANY(%s)
                       AND effective_from <= %s
                       AND (effective_to_exclusive IS NULL OR effective_to_exclusive > %s)
                     ORDER BY ts_code, effective_from, pool_id
                    """,
                    (list(pool_ids), end_date, start_date),
                )
                rows = list(cur.fetchall())
        return tuple(_membership_from_row(row) for row in rows)

    def fetch_canonical_intervals(self, start_date: date, end_date: date) -> Sequence[CanonicalEquityInterval]:
        with self._connection_factory() as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT ts_code, eligible_start, eligible_end
                      FROM market.stock_universe_pit_spans
                     WHERE universe_key = %s
                       AND rule_version = %s
                       AND eligible_start <= %s
                       AND eligible_end >= %s
                     ORDER BY ts_code, eligible_start, eligible_end
                    """,
                    (CANONICAL_PIT_UNIVERSE_KEY, CANONICAL_PIT_RULE_VERSION, end_date, start_date),
                )
                rows = list(cur.fetchall())
        return tuple(
            CanonicalEquityInterval(
                ts_code=_normalize_ts_code(row["ts_code"]),
                eligible_start=_require_date(row["eligible_start"], "eligible_start"),
                eligible_end=_require_date(row["eligible_end"], "eligible_end"),
            )
            for row in rows
        )

    def fetch_trading_dates(self, start_date: date, end_date: date) -> Sequence[date]:
        with self._connection_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT trade_date
                      FROM market.kline_daily_raw
                     WHERE trade_date BETWEEN %s AND %s
                     ORDER BY trade_date
                    """,
                    (start_date, end_date),
                )
                rows = list(cur.fetchall())
        return tuple(_require_date(row[0], "trade_date") for row in rows)


def resolve_universe(
    selection: UniverseSelection,
    start_date: date,
    end_date: date,
    *,
    repository: MembershipRepository | None = None,
) -> ResolvedUniverse:
    """Resolve an order-invariant tradable universe for a date window."""

    if not isinstance(start_date, date) or not isinstance(end_date, date) or start_date > end_date:
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID,
            "requested universe date window is invalid",
        )
    repo = repository or CoreIndexMembershipRepository()
    calendar = tuple(sorted(set(repo.fetch_trading_dates(start_date, end_date))))
    if not calendar:
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.CANONICAL_EQUITY_PIT_UNAVAILABLE,
            "trading calendar is empty for requested window",
        )
    canonical = tuple(repo.fetch_canonical_intervals(start_date, end_date))
    if not canonical:
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.CANONICAL_EQUITY_PIT_UNAVAILABLE,
            "canonical equity PIT contains no intervals for requested window",
        )

    if selection.mode is UniverseMode.STOCK_UNIVERSE:
        intervals = _normalize_final_intervals(canonical, calendar, start_date, end_date)
        return ResolvedUniverse(
            mode=selection.mode,
            pool_ids=(),
            benchmark_code=selection.benchmark_code,
            membership_revision="canonical_equity_pit_v2",
            intervals=intervals,
            source_pool_ids_by_symbol=MappingProxyType({}),
        )

    coverage = repo.fetch_pool_coverage(selection.pool_ids)
    missing = tuple(pool_id for pool_id in selection.pool_ids if pool_id not in coverage)
    if missing:
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.MEMBERSHIP_HISTORY_UNAVAILABLE,
            f"membership rows are unavailable for pools: {list(missing)}",
        )
    for pool_id in selection.pool_ids:
        definition = POOL_DEFINITIONS[pool_id]
        observed = coverage[pool_id]
        required_from = max(start_date, definition.launch_date)
        if observed.first_effective_from > required_from:
            raise CoreIndexMembershipUnavailable(
                UniverseUnavailableReason.MEMBERSHIP_HISTORY_UNAVAILABLE,
                f"{pool_id} membership starts at {observed.first_effective_from}, "
                f"after the requested ready_from {required_from}",
            )

    membership = tuple(repo.fetch_membership_intervals(selection.pool_ids, start_date, end_date))
    raw_union, sources = _union_membership_intervals(
        membership,
        selected_pool_ids=selection.pool_ids,
        start_date=start_date,
        end_date=end_date,
    )
    intervals = _intersect_with_canonical(raw_union, canonical, calendar, start_date, end_date)
    if not intervals:
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.MEMBERSHIP_HISTORY_UNAVAILABLE,
            "selected index membership and canonical equity PIT have no common trading interval",
        )
    revision = max(coverage[pool_id].revision for pool_id in selection.pool_ids).isoformat()
    resolved_symbols = {row.ts_code for row in intervals}
    return ResolvedUniverse(
        mode=selection.mode,
        pool_ids=selection.pool_ids,
        benchmark_code=selection.benchmark_code,
        membership_revision=revision,
        intervals=intervals,
        source_pool_ids_by_symbol=MappingProxyType(
            {symbol: tuple(sorted(values)) for symbol, values in sorted(sources.items()) if symbol in resolved_symbols}
        ),
    )


def _membership_from_row(row: Mapping[str, Any]) -> MembershipInterval:
    pool_id = str(row["pool_id"]).strip().lower()
    definition = POOL_DEFINITIONS.get(pool_id)
    if definition is None or str(row["index_code"]).strip().upper() != definition.index_code:
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID,
            f"membership pool/index identity is invalid for {pool_id!r}",
        )
    start = _require_date(row["effective_from"], "effective_from")
    raw_end = row.get("effective_to_exclusive")
    end = None if raw_end is None else _require_date(raw_end, "effective_to_exclusive")
    if end is not None and end <= start:
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID,
            f"membership interval ends before it starts for {pool_id}",
        )
    provider = str(row["source_provider"]).strip().upper()
    if provider != definition.source_provider:
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID,
            f"membership source provider is invalid for {pool_id}",
        )
    reference = str(row["source_reference"]).strip()
    if not reference:
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID,
            f"membership source reference is empty for {pool_id}",
        )
    return MembershipInterval(
        pool_id=pool_id,
        index_code=definition.index_code,
        ts_code=_normalize_ts_code(row["ts_code"]),
        effective_from=start,
        effective_to_exclusive=end,
        source_provider=provider,
        source_reference=reference,
        updated_at=_require_datetime(row["updated_at"], "updated_at"),
    )


def _union_membership_intervals(
    rows: Iterable[MembershipInterval],
    *,
    selected_pool_ids: Sequence[str],
    start_date: date,
    end_date: date,
) -> tuple[tuple[tuple[str, date, date], ...], dict[str, set[str]]]:
    selected = set(selected_pool_ids)
    window_end_exclusive = end_date + timedelta(days=1)
    by_symbol: dict[str, list[tuple[date, date]]] = {}
    sources: dict[str, set[str]] = {}
    identities: set[tuple[str, str, date]] = set()
    normalized_rows = sorted(rows, key=lambda item: (item.pool_id, item.ts_code, item.effective_from))
    prior_by_pool_symbol: dict[tuple[str, str], date | None] = {}
    seen_pool_symbol: set[tuple[str, str]] = set()
    for row in normalized_rows:
        if row.pool_id not in selected:
            raise CoreIndexMembershipUnavailable(
                UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID,
                f"repository returned unrequested pool {row.pool_id}",
            )
        identity = (row.pool_id, row.ts_code, row.effective_from)
        if identity in identities:
            raise CoreIndexMembershipUnavailable(
                UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID,
                f"duplicate membership identity: {identity}",
            )
        identities.add(identity)
        pool_symbol = (row.pool_id, row.ts_code)
        if pool_symbol in seen_pool_symbol:
            prior_end = prior_by_pool_symbol[pool_symbol]
            if prior_end is None or row.effective_from < prior_end:
                raise CoreIndexMembershipUnavailable(
                    UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID,
                    f"overlapping membership intervals for {row.pool_id}/{row.ts_code}",
                )
        seen_pool_symbol.add(pool_symbol)
        prior_by_pool_symbol[pool_symbol] = row.effective_to_exclusive
        start = max(row.effective_from, start_date)
        end = min(row.effective_to_exclusive or window_end_exclusive, window_end_exclusive)
        if end <= start:
            continue
        by_symbol.setdefault(row.ts_code, []).append((start, end))
        sources.setdefault(row.ts_code, set()).add(row.pool_id)

    merged: list[tuple[str, date, date]] = []
    for symbol, ranges in sorted(by_symbol.items()):
        current_start: date | None = None
        current_end: date | None = None
        for start, end in sorted(ranges):
            if current_start is None:
                current_start, current_end = start, end
                continue
            assert current_end is not None
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                merged.append((symbol, current_start, current_end))
                current_start, current_end = start, end
        if current_start is not None and current_end is not None:
            merged.append((symbol, current_start, current_end))
    return tuple(merged), sources


def _intersect_with_canonical(
    membership: Sequence[tuple[str, date, date]],
    canonical: Sequence[CanonicalEquityInterval],
    calendar: Sequence[date],
    start_date: date,
    end_date: date,
) -> tuple[UniverseInterval, ...]:
    canonical_by_symbol: dict[str, list[CanonicalEquityInterval]] = {}
    for row in canonical:
        if row.eligible_end < row.eligible_start:
            raise CoreIndexMembershipUnavailable(
                UniverseUnavailableReason.CANONICAL_EQUITY_PIT_UNAVAILABLE,
                f"canonical PIT interval is invalid for {row.ts_code}",
            )
        canonical_by_symbol.setdefault(row.ts_code, []).append(row)

    output: list[UniverseInterval] = []
    for symbol, member_start, member_end_exclusive in membership:
        for pit in canonical_by_symbol.get(symbol, ()):  # no PIT row means correctly excluded
            start = max(member_start, pit.eligible_start, start_date)
            end = min(member_end_exclusive - timedelta(days=1), pit.eligible_end, end_date)
            normalized = _clip_to_calendar(start, end, calendar)
            if normalized is not None:
                output.append(UniverseInterval(symbol, normalized[0], normalized[1]))
    return _merge_final_intervals(output, calendar)


def _normalize_final_intervals(
    canonical: Sequence[CanonicalEquityInterval],
    calendar: Sequence[date],
    start_date: date,
    end_date: date,
) -> tuple[UniverseInterval, ...]:
    rows: list[UniverseInterval] = []
    for row in canonical:
        normalized = _clip_to_calendar(max(row.eligible_start, start_date), min(row.eligible_end, end_date), calendar)
        if normalized is not None:
            rows.append(UniverseInterval(row.ts_code, normalized[0], normalized[1]))
    if not rows:
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.CANONICAL_EQUITY_PIT_UNAVAILABLE,
            "canonical equity PIT has no trading dates in requested window",
        )
    return _merge_final_intervals(rows, calendar)


def _clip_to_calendar(start: date, end: date, calendar: Sequence[date]) -> tuple[date, date] | None:
    if end < start:
        return None
    left = bisect.bisect_left(calendar, start)
    right = bisect.bisect_right(calendar, end) - 1
    if left >= len(calendar) or right < left:
        return None
    return calendar[left], calendar[right]


def _merge_final_intervals(rows: Iterable[UniverseInterval], calendar: Sequence[date]) -> tuple[UniverseInterval, ...]:
    calendar_index = {value: index for index, value in enumerate(calendar)}
    by_symbol: dict[str, list[UniverseInterval]] = {}
    for row in rows:
        by_symbol.setdefault(row.ts_code, []).append(row)
    merged: list[UniverseInterval] = []
    for symbol, values in sorted(by_symbol.items()):
        current: UniverseInterval | None = None
        for row in sorted(values, key=lambda item: (item.eligible_start, item.eligible_end)):
            if current is None:
                current = row
                continue
            prior_index = calendar_index[current.eligible_end]
            next_index = calendar_index[row.eligible_start]
            if row.eligible_start <= current.eligible_end or next_index <= prior_index + 1:
                current = UniverseInterval(
                    symbol,
                    current.eligible_start,
                    max(current.eligible_end, row.eligible_end),
                )
            else:
                merged.append(current)
                current = row
        if current is not None:
            merged.append(current)
    return tuple(merged)


def _normalize_ts_code(value: Any) -> str:
    code = str(value or "").strip().upper()
    if not _TS_CODE_RE.fullmatch(code):
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID,
            f"invalid A-share ts_code: {value!r}",
        )
    return code


def _require_date(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID,
            f"{field} is not a valid date",
        ) from exc


def _require_datetime(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise CoreIndexMembershipUnavailable(
            UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID,
            f"{field} is not a valid datetime",
        ) from exc
