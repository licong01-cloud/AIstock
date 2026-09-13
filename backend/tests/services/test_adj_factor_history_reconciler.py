from __future__ import annotations

import datetime as dt
from collections import defaultdict
from decimal import Decimal
from typing import Any, Sequence

import pytest

from backend.services.adj_factor_history_reconciler import (
    AdjFactorHistoryReconcileError,
    AdjFactorHistoryReconciler,
    AdjFactorRow,
    AdjFactorSnapshot,
    SymbolScope,
    _snapshot,
)


DAY_1 = dt.date(2026, 7, 8)
DAY_2 = dt.date(2026, 7, 9)
DAY_3 = dt.date(2026, 7, 10)


def _rows(symbol: str, values: Sequence[tuple[dt.date, str]]) -> tuple[AdjFactorRow, ...]:
    return tuple(AdjFactorRow(symbol, day, Decimal(value)) for day, value in values)


def _provider_rows(symbol: str, values: Sequence[tuple[dt.date, str]]) -> list[dict[str, str]]:
    return [
        {
            "ts_code": symbol,
            "trade_date": day.strftime("%Y%m%d"),
            "adj_factor": value,
        }
        for day, value in values
    ]


class _Repository:
    def __init__(
        self,
        snapshots: dict[str, AdjFactorSnapshot],
        price_dates: dict[str, frozenset[dt.date]],
    ) -> None:
        self.snapshots = snapshots
        self.price_dates = price_dates
        self.staged: dict[str, AdjFactorSnapshot] = {}
        self.apply_calls = 0

    def list_symbol_scopes(self, *, end_date: dt.date, symbols: Sequence[str] | None = None) -> Sequence[SymbolScope]:
        requested = set(symbols or self.snapshots)
        return [
            SymbolScope(symbol, min(self.price_dates[symbol] or {self.snapshots[symbol].first_date}))
            for symbol in sorted(requested)
        ]

    def load_snapshot(self, symbol: str, *, end_date: dt.date) -> AdjFactorSnapshot:
        return self.snapshots[symbol]

    def load_price_dates(self, symbol: str, *, end_date: dt.date) -> frozenset[dt.date]:
        return self.price_dates[symbol]

    def begin_stage(self) -> None:
        self.staged.clear()

    def stage_rows(self, rows: Sequence[AdjFactorRow]) -> None:
        assert rows
        symbol = rows[0].symbol
        self.staged[symbol] = _snapshot(symbol, rows)

    def apply_staged(
        self,
        *,
        symbols: Sequence[str],
        replace_symbols: Sequence[str],
        end_date: dt.date,
        expected_local_sha256: dict[str, str],
        expected_row_counts: dict[str, int],
    ) -> int:
        self.apply_calls += 1
        for symbol in symbols:
            assert self.snapshots[symbol].canonical_sha256 == expected_local_sha256[symbol]
            assert len(self.staged[symbol].rows) == expected_row_counts[symbol]
            if symbol in replace_symbols:
                self.snapshots[symbol] = self.staged[symbol]
            else:
                self.snapshots[symbol] = _snapshot(
                    symbol,
                    (*self.snapshots[symbol].rows, *self.staged[symbol].rows),
                )
        return sum(expected_row_counts[symbol] for symbol in symbols)


class _SequencedProvider:
    def __init__(self, responses: dict[str, Sequence[list[dict[str, str]]]]) -> None:
        self.responses = responses
        self.call_counts: defaultdict[str, int] = defaultdict(int)

    def adj_factor(self, *, ts_code: str, end_date: str, fields: str) -> list[dict[str, str]]:
        assert fields == "ts_code,trade_date,adj_factor"
        index = self.call_counts[ts_code]
        self.call_counts[ts_code] += 1
        configured = self.responses[ts_code]
        return configured[min(index, len(configured) - 1)]


class _PagedProvider:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.call_count = 0

    def adj_factor(self, *, ts_code: str, end_date: str, fields: str) -> list[dict[str, str]]:
        self.call_count += 1
        end = dt.datetime.strptime(end_date, "%Y%m%d").date()
        values = [
            (dt.date(2026, 7, 1), "1.0"),
            (dt.date(2026, 7, 2), "1.0"),
            (dt.date(2026, 7, 3), "1.0"),
            (dt.date(2026, 7, 4), "1.0"),
            (dt.date(2026, 7, 5), "1.0"),
        ]
        eligible = [item for item in values if item[0] <= end]
        return _provider_rows(ts_code, eligible[-3:])


def _reconciler(repository: _Repository, provider: Any) -> AdjFactorHistoryReconciler:
    return AdjFactorHistoryReconciler(
        repository=repository,
        provider_factory=lambda: provider,
        workers=1,
        calls_per_minute=0,
        max_pages=4,
    )


def test_reconcile_replaces_only_stable_changed_history_and_emits_invalidation() -> None:
    symbol = "688109.SH"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "1.5415"), (DAY_2, "1.5459")]))
    provider_rows = _provider_rows(symbol, [(DAY_1, "1.5459"), (DAY_2, "1.5459")])
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_1, DAY_2})})
    provider = _SequencedProvider({symbol: [provider_rows, provider_rows]})

    receipt = _reconciler(repository, provider).reconcile(end_date=DAY_2)

    assert receipt["status"] == "reconciled"
    assert receipt["changed_symbols"] == [symbol]
    assert receipt["database_write_performed"] is True
    assert receipt["written_row_count"] == 2
    assert receipt["changes"][0]["difference_start"] == DAY_1.isoformat()
    assert receipt["changes"][0]["write_mode"] == "full_replace"
    assert receipt["downstream_invalidation"]["instruments"] == [symbol]
    assert receipt["llm_used"] is False
    assert receipt["online_search_used"] is False
    assert repository.apply_calls == 1
    assert repository.snapshots[symbol].qfq_sha256 != local.qfq_sha256


def test_reconcile_unchanged_history_performs_no_database_write() -> None:
    symbol = "300506.SZ"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "6.4229"), (DAY_2, "6.4229")]))
    provider_rows = _provider_rows(symbol, [(DAY_1, "6.4229"), (DAY_2, "6.4229")])
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_1, DAY_2})})

    receipt = _reconciler(
        repository,
        _SequencedProvider({symbol: [provider_rows, provider_rows]}),
    ).reconcile(end_date=DAY_2)

    assert receipt["status"] == "unchanged"
    assert receipt["changed_symbol_count"] == 0
    assert receipt["database_write_performed"] is False
    assert repository.apply_calls == 0


def test_reconcile_new_trade_date_stages_only_append_suffix() -> None:
    symbol = "300506.SZ"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "6.4229"), (DAY_2, "6.4229")]))
    provider_rows = _provider_rows(
        symbol,
        [(DAY_1, "6.4229"), (DAY_2, "6.4229"), (DAY_3, "6.4229")],
    )
    repository = _Repository(
        {symbol: local},
        {symbol: frozenset({DAY_1, DAY_2, DAY_3})},
    )

    receipt = _reconciler(
        repository,
        _SequencedProvider({symbol: [provider_rows, provider_rows]}),
    ).reconcile(end_date=DAY_3)

    assert receipt["append_symbol_count"] == 1
    assert receipt["full_replace_symbol_count"] == 0
    assert receipt["changes"][0]["write_mode"] == "append"
    assert receipt["changes"][0]["staged_row_count"] == 1
    assert receipt["written_row_count"] == 1
    assert len(repository.snapshots[symbol].rows) == 3


def test_reconcile_fails_closed_when_provider_changes_between_reads() -> None:
    symbol = "300506.SZ"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "4.8440"), (DAY_2, "6.4229")]))
    first = _provider_rows(symbol, [(DAY_1, "6.4229"), (DAY_2, "6.4229")])
    second = _provider_rows(symbol, [(DAY_1, "6.4230"), (DAY_2, "6.4229")])
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_1, DAY_2})})

    with pytest.raises(AdjFactorHistoryReconcileError, match="changed between stability reads"):
        _reconciler(repository, _SequencedProvider({symbol: [first, second]})).reconcile(end_date=DAY_2)

    assert repository.apply_calls == 0


def test_reconcile_fails_closed_when_provider_omits_a_raw_price_date() -> None:
    symbol = "000970.SZ"
    local = _snapshot(
        symbol,
        _rows(symbol, [(DAY_1, "2.0"), (DAY_2, "2.0"), (DAY_3, "2.0")]),
    )
    incomplete = _provider_rows(symbol, [(DAY_1, "2.0"), (DAY_3, "2.0")])
    repository = _Repository(
        {symbol: local},
        {symbol: frozenset({DAY_1, DAY_2, DAY_3})},
    )

    with pytest.raises(AdjFactorHistoryReconcileError, match=r"factor=1, price=1"):
        _reconciler(
            repository,
            _SequencedProvider({symbol: [incomplete, incomplete]}),
        ).reconcile(end_date=DAY_3)

    assert repository.apply_calls == 0


def test_reconcile_dry_run_reports_change_without_applying_it() -> None:
    symbol = "688109.SH"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "1.5415"), (DAY_2, "1.5459")]))
    complete = _provider_rows(symbol, [(DAY_1, "1.5459"), (DAY_2, "1.5459")])
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_1, DAY_2})})

    receipt = _reconciler(
        repository,
        _SequencedProvider({symbol: [complete, complete]}),
    ).reconcile(end_date=DAY_2, dry_run=True)

    assert receipt["status"] == "dry_run"
    assert receipt["changed_symbols"] == [symbol]
    assert receipt["database_write_performed"] is False
    assert receipt["written_row_count"] == 0
    assert repository.apply_calls == 0


def test_reconcile_paginates_back_to_required_start_on_both_stability_reads() -> None:
    symbol = "600008.SH"
    days = tuple(dt.date(2026, 7, day) for day in range(1, 6))
    local = _snapshot(symbol, _rows(symbol, [(day, "1.0") for day in days]))
    repository = _Repository({symbol: local}, {symbol: frozenset(days)})
    provider = _PagedProvider(symbol)

    receipt = _reconciler(repository, provider).reconcile(end_date=days[-1])

    assert receipt["status"] == "unchanged"
    assert receipt["provider_call_count"] == 4
    assert provider.call_count == 4


def test_all_provider_histories_are_validated_before_any_persistent_apply() -> None:
    first_symbol = "300506.SZ"
    second_symbol = "688109.SH"
    snapshots = {
        first_symbol: _snapshot(first_symbol, _rows(first_symbol, [(DAY_1, "1.0")])),
        second_symbol: _snapshot(second_symbol, _rows(second_symbol, [(DAY_1, "1.0")])),
    }
    repository = _Repository(
        snapshots,
        {first_symbol: frozenset({DAY_1}), second_symbol: frozenset({DAY_1})},
    )
    provider = _SequencedProvider(
        {
            first_symbol: [
                _provider_rows(first_symbol, [(DAY_1, "2.0")]),
                _provider_rows(first_symbol, [(DAY_1, "2.0")]),
            ],
            second_symbol: [
                _provider_rows(second_symbol, [(DAY_1, "2.0")]),
                _provider_rows(second_symbol, [(DAY_1, "3.0")]),
            ],
        }
    )

    with pytest.raises(AdjFactorHistoryReconcileError, match="changed between stability reads"):
        _reconciler(repository, provider).reconcile(end_date=DAY_1)

    assert repository.apply_calls == 0
