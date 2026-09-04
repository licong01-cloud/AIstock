from __future__ import annotations

from datetime import date, datetime

import pytest

from backend.services.core_index_membership import (
    CanonicalEquityInterval,
    CoreIndexMembershipUnavailable,
    MembershipInterval,
    PoolCoverage,
    UniverseMode,
    UniverseSelection,
    UniverseUnavailableReason,
    resolve_universe,
)


REVISION = datetime(2026, 9, 4, 8, 0, 0)
CALENDAR = tuple(date(2024, 1, day) for day in (2, 3, 4, 5, 8, 9, 10))


class Repository:
    def __init__(self, membership=(), canonical=(), coverage=None, calendar=CALENDAR):
        self.membership = tuple(membership)
        self.canonical = tuple(canonical)
        self.coverage = coverage or {
            "csi300": PoolCoverage("csi300", date(2018, 8, 1), 1, REVISION),
            "csi500": PoolCoverage("csi500", date(2018, 8, 1), 1, REVISION),
        }
        self.calendar = tuple(calendar)

    def fetch_pool_coverage(self, pool_ids):
        return {pool_id: self.coverage[pool_id] for pool_id in pool_ids if pool_id in self.coverage}

    def fetch_membership_intervals(self, pool_ids, start_date, end_date):
        del start_date, end_date
        return tuple(row for row in self.membership if row.pool_id in pool_ids)

    def fetch_canonical_intervals(self, start_date, end_date):
        del start_date, end_date
        return self.canonical

    def fetch_trading_dates(self, start_date, end_date):
        return tuple(day for day in self.calendar if start_date <= day <= end_date)


def membership(pool_id, symbol, start, end=None):
    catalog = {
        "csi300": ("000300.SH", "CSI"),
        "csi500": ("000905.SH", "CSI"),
    }
    index_code, provider = catalog[pool_id]
    return MembershipInterval(
        pool_id=pool_id,
        index_code=index_code,
        ts_code=symbol,
        effective_from=start,
        effective_to_exclusive=end,
        source_provider=provider,
        source_reference="official:test",
        updated_at=REVISION,
    )


def canonical(symbol, start=date(2024, 1, 2), end=date(2024, 1, 10)):
    return CanonicalEquityInterval(symbol, start, end)


def test_default_selection_preserves_canonical_stock_universe() -> None:
    repo = Repository(canonical=(canonical("000001.SZ"),))

    result = resolve_universe(UniverseSelection(), date(2024, 1, 2), date(2024, 1, 10), repository=repo)

    assert result.mode is UniverseMode.STOCK_UNIVERSE
    assert result.pool_ids == ()
    assert [(row.ts_code, row.eligible_start, row.eligible_end) for row in result.intervals] == [
        ("000001.SZ", date(2024, 1, 2), date(2024, 1, 10))
    ]


def test_multi_index_union_is_order_invariant_and_deduplicates_symbol() -> None:
    rows = (
        membership("csi300", "000001.SZ", date(2024, 1, 2), date(2024, 1, 5)),
        membership("csi500", "000001.SZ", date(2024, 1, 5), date(2024, 1, 9)),
        membership("csi500", "600000.SH", date(2024, 1, 3), date(2024, 1, 9)),
    )
    repo = Repository(membership=rows, canonical=(canonical("000001.SZ"), canonical("600000.SH")))

    first = resolve_universe(
        UniverseSelection(mode=UniverseMode.INDEX_UNION, pool_ids=("csi500", "csi300", "csi300")),
        date(2024, 1, 2),
        date(2024, 1, 10),
        repository=repo,
    )
    second = resolve_universe(
        UniverseSelection(mode=UniverseMode.INDEX_UNION, pool_ids=("csi300", "csi500")),
        date(2024, 1, 2),
        date(2024, 1, 10),
        repository=repo,
    )

    assert first.intervals == second.intervals
    assert first.pool_ids == ("csi300", "csi500")
    assert [(row.ts_code, row.eligible_start, row.eligible_end) for row in first.intervals] == [
        ("000001.SZ", date(2024, 1, 2), date(2024, 1, 8)),
        ("600000.SH", date(2024, 1, 3), date(2024, 1, 8)),
    ]
    assert first.source_pool_ids_by_symbol["000001.SZ"] == ("csi300", "csi500")


def test_index_membership_is_intersected_with_canonical_pit_multi_spans() -> None:
    repo = Repository(
        membership=(membership("csi300", "000001.SZ", date(2024, 1, 2), None),),
        canonical=(
            canonical("000001.SZ", date(2024, 1, 2), date(2024, 1, 3)),
            canonical("000001.SZ", date(2024, 1, 5), date(2024, 1, 10)),
        ),
    )

    result = resolve_universe(
        UniverseSelection(mode=UniverseMode.SINGLE_INDEX, pool_ids=("csi300",)),
        date(2024, 1, 2),
        date(2024, 1, 10),
        repository=repo,
    )

    assert [(row.eligible_start, row.eligible_end) for row in result.intervals] == [
        (date(2024, 1, 2), date(2024, 1, 3)),
        (date(2024, 1, 5), date(2024, 1, 10)),
    ]


def test_future_membership_mutation_does_not_change_historical_window() -> None:
    base = membership("csi300", "000001.SZ", date(2024, 1, 2), date(2024, 1, 9))
    future = membership("csi300", "600000.SH", date(2025, 1, 2), None)
    repo = Repository(membership=(base, future), canonical=(canonical("000001.SZ"), canonical("600000.SH")))

    result = resolve_universe(
        UniverseSelection(mode=UniverseMode.SINGLE_INDEX, pool_ids=("csi300",)),
        date(2024, 1, 2),
        date(2024, 1, 10),
        repository=repo,
    )

    assert {row.ts_code for row in result.intervals} == {"000001.SZ"}


def test_missing_pool_history_returns_typed_unavailable() -> None:
    repo = Repository(canonical=(canonical("000001.SZ"),), coverage={})

    with pytest.raises(CoreIndexMembershipUnavailable) as captured:
        resolve_universe(
            UniverseSelection(mode=UniverseMode.SINGLE_INDEX, pool_ids=("star100",)),
            date(2024, 1, 2),
            date(2024, 1, 10),
            repository=repo,
        )

    assert captured.value.reason is UniverseUnavailableReason.MEMBERSHIP_HISTORY_UNAVAILABLE


def test_pool_with_later_ready_from_allows_only_later_request_windows() -> None:
    ready_from = date(2020, 12, 14)
    repo = Repository(
        membership=(membership("csi300", "000001.SZ", ready_from, None),),
        canonical=(canonical("000001.SZ", date(2018, 8, 1), date(2024, 1, 10)),),
        coverage={"csi300": PoolCoverage("csi300", ready_from, 1, REVISION)},
    )

    later = resolve_universe(
        UniverseSelection(mode=UniverseMode.SINGLE_INDEX, pool_ids=("csi300",)),
        date(2024, 1, 2),
        date(2024, 1, 10),
        repository=repo,
    )
    assert {row.ts_code for row in later.intervals} == {"000001.SZ"}

    with pytest.raises(CoreIndexMembershipUnavailable) as captured:
        resolve_universe(
            UniverseSelection(mode=UniverseMode.SINGLE_INDEX, pool_ids=("csi300",)),
            date(2018, 8, 1),
            date(2024, 1, 10),
            repository=repo,
        )
    assert captured.value.reason is UniverseUnavailableReason.MEMBERSHIP_HISTORY_UNAVAILABLE


def test_overlapping_same_pool_intervals_fail_closed() -> None:
    rows = (
        membership("csi300", "000001.SZ", date(2024, 1, 2), date(2024, 1, 9)),
        membership("csi300", "000001.SZ", date(2024, 1, 5), None),
    )
    repo = Repository(membership=rows, canonical=(canonical("000001.SZ"),))

    with pytest.raises(CoreIndexMembershipUnavailable) as captured:
        resolve_universe(
            UniverseSelection(mode=UniverseMode.SINGLE_INDEX, pool_ids=("csi300",)),
            date(2024, 1, 2),
            date(2024, 1, 10),
            repository=repo,
        )

    assert captured.value.reason is UniverseUnavailableReason.MEMBERSHIP_INTERVAL_INVALID


def test_unknown_pool_and_invalid_empty_union_do_not_fallback() -> None:
    with pytest.raises(CoreIndexMembershipUnavailable) as unknown:
        UniverseSelection(mode=UniverseMode.SINGLE_INDEX, pool_ids=("unknown",))
    assert unknown.value.reason is UniverseUnavailableReason.UNKNOWN_POOL_ID

    with pytest.raises(CoreIndexMembershipUnavailable):
        UniverseSelection(mode=UniverseMode.INDEX_UNION, pool_ids=())


def test_selection_mapping_accepts_typed_enum_without_string_coercion() -> None:
    selection = UniverseSelection.from_mapping({"mode": UniverseMode.INDEX_UNION, "pool_ids": ["csi500", "csi300"]})

    assert selection.mode is UniverseMode.INDEX_UNION
    assert selection.pool_ids == ("csi300", "csi500")


def test_selection_mapping_unknown_mode_returns_typed_unavailable() -> None:
    with pytest.raises(CoreIndexMembershipUnavailable) as captured:
        UniverseSelection.from_mapping({"mode": "not-a-mode"})

    assert captured.value.reason is UniverseUnavailableReason.UNKNOWN_POOL_ID
