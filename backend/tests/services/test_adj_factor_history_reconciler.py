from __future__ import annotations

import datetime as dt
from collections import defaultdict
from decimal import Decimal
from fractions import Fraction
from typing import Any, Sequence

import pytest

from backend.services.adj_factor_history_reconciler import (
    AdjFactorHistoryReconcileError,
    AdjFactorHistoryReconciler,
    AdjFactorRow,
    AdjFactorSnapshot,
    PostgresAdjFactorHistoryRepository,
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
        self.basis_normalizations: list[dict[str, Any]] = []

    def list_symbol_scopes(self, *, end_date: dt.date, symbols: Sequence[str] | None = None) -> Sequence[SymbolScope]:
        requested = set(symbols or self.snapshots)
        return [
            SymbolScope(symbol, min({self.snapshots[symbol].first_date, *self.price_dates[symbol]}))
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
        basis_normalizations: Sequence[dict[str, Any]] = (),
    ) -> int:
        self.apply_calls += 1
        self.basis_normalizations.extend(basis_normalizations)
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
    def __init__(self, responses: dict[str, Sequence[object]]) -> None:
        self.responses = responses
        self.call_counts: defaultdict[str, int] = defaultdict(int)

    def adj_factor(
        self,
        *,
        ts_code: str,
        start_date: str,
        end_date: str,
        fields: str,
    ) -> list[dict[str, str]]:
        assert fields == "ts_code,trade_date,adj_factor"
        assert start_date
        index = self.call_counts[ts_code]
        self.call_counts[ts_code] += 1
        configured = self.responses[ts_code]
        response = configured[min(index, len(configured) - 1)]
        if isinstance(response, BaseException):
            raise response
        return response


class _PagedProvider:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.call_count = 0

    def adj_factor(
        self,
        *,
        ts_code: str,
        start_date: str,
        end_date: str,
        fields: str,
    ) -> list[dict[str, str]]:
        self.call_count += 1
        assert start_date == "20260701"
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


class _ListingProvider:
    def __init__(self, symbol: str, *, listing_date: dt.date) -> None:
        self.symbol = symbol
        self.listing_date = listing_date

    def adj_factor(
        self,
        *,
        ts_code: str,
        start_date: str,
        end_date: str,
        fields: str,
    ) -> list[dict[str, str]]:
        assert ts_code == self.symbol
        start = dt.datetime.strptime(start_date, "%Y%m%d").date()
        end = dt.datetime.strptime(end_date, "%Y%m%d").date()
        values = [(DAY_2, "1.0"), (DAY_3, "1.0")]
        return _provider_rows(ts_code, [item for item in values if start <= item[0] <= end])

    def stock_basic(
        self,
        *,
        ts_code: str,
        list_status: str,
        fields: str,
    ) -> list[dict[str, str]]:
        assert ts_code == self.symbol
        assert list_status == "L"
        assert fields == "ts_code,list_date"
        return [{"ts_code": ts_code, "list_date": self.listing_date.strftime("%Y%m%d")}]


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
    assert receipt["downstream_invalidation"]["required"] is False


def test_independent_reconcile_commits_good_symbol_and_returns_exact_failure() -> None:
    good, bad = "300506.SZ", "688109.SH"
    repository = _Repository(
        {code: _snapshot(code, _rows(code, [(DAY_1, "1")])) for code in (good, bad)},
        {code: frozenset({DAY_1}) for code in (good, bad)},
    )
    provider = _SequencedProvider({
        good: [_provider_rows(good, [(DAY_1, "2")])],
        bad: [_provider_rows(bad, [(DAY_1, "2")]), _provider_rows(bad, [(DAY_1, "3")])],
    })
    progress = []
    receipt = _reconciler(repository, provider).reconcile(
        end_date=DAY_1, independent=True, progress=progress.append,
    )
    assert receipt["status"] == "partial"
    assert receipt["failed_symbols"] == [bad]
    assert receipt["completed_symbols"] == [good]
    assert repository.snapshots[good].rows[0].value == 2
    assert repository.snapshots[bad].rows[0].value == 1
    assert repository.apply_calls == 1
    assert len(progress) == 2 and receipt["written_row_count"] == 1


def test_qfq_restatement_is_not_inferred_from_uniform_factor_rescaling() -> None:
    symbol = "920047.BJ"
    repository = _Repository(
        {symbol: _snapshot(symbol, _rows(symbol, [(DAY_1, "433.618"), (DAY_2, "433.618")]))},
        {symbol: frozenset({DAY_1, DAY_2, DAY_3})},
    )
    provider = _SequencedProvider({symbol: [_provider_rows(symbol, [(d, "1") for d in (DAY_1, DAY_2, DAY_3)])]})
    receipt = _reconciler(repository, provider).reconcile(end_date=DAY_3)
    assert receipt["full_replace_symbol_count"] == 1
    assert receipt["downstream_invalidation"]["required"] is False


def test_independent_dry_run_does_not_apply_good_or_bad_histories() -> None:
    symbol = "300506.SZ"
    repository = _Repository(
        {symbol: _snapshot(symbol, _rows(symbol, [(DAY_1, "1")]))},
        {symbol: frozenset({DAY_1})},
    )
    receipt = _reconciler(repository, _SequencedProvider({symbol: [_provider_rows(symbol, [(DAY_1, "2")])]})).reconcile(
        end_date=DAY_1, independent=True, dry_run=True,
    )
    assert receipt["status"] == "dry_run"
    assert receipt["database_write_performed"] is False
    assert repository.apply_calls == 0


@pytest.mark.parametrize("local_changed", [False, True])
def test_independent_resume_reuses_only_the_exact_committed_local_snapshot(local_changed) -> None:
    symbol = "300506.SZ"
    verified = _snapshot(symbol, _rows(symbol, [(DAY_1, "1")]))
    current = _snapshot(symbol, _rows(symbol, [(DAY_1, "2" if local_changed else "1")]))
    repository = _Repository({symbol: current}, {symbol: frozenset({DAY_1})})
    provider = _SequencedProvider({symbol: [_provider_rows(symbol, [(DAY_1, "1")])]})
    receipt = _reconciler(repository, provider).reconcile(
        end_date=DAY_1, independent=True, completed={symbol: verified.canonical_sha256},
    )
    assert provider.call_counts[symbol] == (2 if local_changed else 0)
    assert receipt["resumed_symbols"] == ([] if local_changed else [symbol])
    assert repository.snapshots[symbol].canonical_sha256 == verified.canonical_sha256


def test_independent_provider_library_error_does_not_discard_other_commits(monkeypatch) -> None:
    good, bad = "300506.SZ", "688109.SH"
    repository = _Repository(
        {code: _snapshot(code, _rows(code, [(DAY_1, "1")])) for code in (good, bad)},
        {code: frozenset({DAY_1}) for code in (good, bad)},
    )
    monkeypatch.setattr("backend.services.adj_factor_history_reconciler.time.sleep", lambda _: None)
    receipt = _reconciler(repository, _SequencedProvider({
        good: [_provider_rows(good, [(DAY_1, "2")])], bad: [RuntimeError("provider unavailable")],
    })).reconcile(end_date=DAY_1, independent=True)
    assert receipt["status"] == "partial" and receipt["failed_symbols"] == [bad]
    assert repository.snapshots[good].rows[0].value == 2


def test_real_new_factor_event_reprices_existing_dates_but_constant_extension_does_not() -> None:
    symbol = "000970.SZ"
    repository = _Repository({symbol: _snapshot(symbol, _rows(symbol, [(DAY_1, "1")]))},
                             {symbol: frozenset({DAY_1, DAY_2})})
    receipt = _reconciler(repository, _SequencedProvider({symbol: [
        _provider_rows(symbol, [(DAY_1, "1"), (DAY_2, "1.2")]),
    ]})).reconcile(end_date=DAY_2)
    assert receipt["append_symbol_count"] == 1 and receipt["downstream_invalidation"]["required"]


def test_existing_qfq_ratio_comparison_is_exact_beyond_default_decimal_precision() -> None:
    from backend.services.adj_factor_history_reconciler import _existing_qfq_changed
    symbol = "300506.SZ"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "1.00000000000000000000000000001"), (DAY_2, "1")]))
    provider = _snapshot(symbol, _rows(symbol, [(DAY_1, "1.00000000000000000000000000002"), (DAY_2, "1")]))
    assert _existing_qfq_changed(local, provider)


def test_newly_available_historical_date_requires_refresh_even_without_qfq_repricing() -> None:
    symbol = "300506.SZ"
    repository = _Repository({symbol: _snapshot(symbol, _rows(symbol, [(DAY_1, "1"), (DAY_3, "1")]))},
                             {symbol: frozenset({DAY_1, DAY_2, DAY_3})})
    receipt = _reconciler(repository, _SequencedProvider({symbol: [
        _provider_rows(symbol, [(d, "1") for d in (DAY_1, DAY_2, DAY_3)]),
    ]})).reconcile(end_date=DAY_3)
    assert receipt["changes"][0]["qfq_changed"] is False
    assert receipt["changes"][0]["historical_coverage_extended"] is True
    assert receipt["downstream_invalidation"]["required"] is True


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

    with pytest.raises(AdjFactorHistoryReconcileError, match="omits a traded local date"):
        _reconciler(
            repository,
            _SequencedProvider({symbol: [incomplete, incomplete]}),
        ).reconcile(end_date=DAY_3)

    assert repository.apply_calls == 0


def test_reconcile_retains_only_a_bounded_nontrading_source_omission() -> None:
    symbol = "002231.SZ"
    local = _snapshot(
        symbol,
        _rows(symbol, [(DAY_1, "3.094"), (DAY_2, "3.094"), (DAY_3, "3.094")]),
    )
    provider_rows = _provider_rows(symbol, [(DAY_1, "3.094"), (DAY_3, "3.094")])
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_1, DAY_3})})

    receipt = _reconciler(
        repository,
        _SequencedProvider({symbol: [provider_rows, provider_rows]}),
    ).reconcile(end_date=DAY_3)

    assert receipt["status"] == "unchanged"
    assert receipt["retained_nontrading_row_count"] == 1
    assert receipt["retained_nontrading_rows"] == [
        {"symbol": symbol, "trade_date": DAY_2.isoformat(), "adj_factor": "3.094"}
    ]
    assert repository.apply_calls == 0


def test_reconcile_rejects_an_unbounded_nontrading_source_omission() -> None:
    symbol = "002231.SZ"
    local = _snapshot(
        symbol,
        _rows(symbol, [(DAY_1, "3.094"), (DAY_2, "3.094"), (DAY_3, "3.094")]),
    )
    provider_rows = _provider_rows(symbol, [(DAY_1, "3.094"), (DAY_3, "3.095")])
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_1, DAY_3})})

    with pytest.raises(AdjFactorHistoryReconcileError, match="unbounded or changed nontrading date"):
        _reconciler(
            repository,
            _SequencedProvider({symbol: [provider_rows, provider_rows]}),
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


def test_reconcile_receipts_pre_authority_predecessor_price_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    symbol = "600018.SH"
    local = _snapshot(symbol, _rows(symbol, [(DAY_2, "1.0"), (DAY_3, "1.0")]))
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_1, DAY_2, DAY_3})})
    monkeypatch.setattr("backend.services.adj_factor_history_reconciler.time.sleep", lambda _seconds: None)

    receipt = _reconciler(repository, _ListingProvider(symbol, listing_date=DAY_2)).reconcile(end_date=DAY_3)

    assert receipt["status"] == "unchanged"
    assert receipt["pre_authority_price_exclusion_count"] == 1
    assert receipt["pre_authority_price_exclusions"] == [
        {
            "symbol": symbol,
            "reason": "traded_price_precedes_official_listing_adj_factor_authority",
            "authority_start": DAY_2.isoformat(),
            "excluded_start": DAY_1.isoformat(),
            "excluded_end": DAY_1.isoformat(),
            "excluded_date_count": 1,
        }
    ]
    assert repository.apply_calls == 0


def test_reconcile_rejects_a_history_start_that_differs_from_official_listing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    symbol = "600018.SH"
    local = _snapshot(symbol, _rows(symbol, [(DAY_2, "1.0"), (DAY_3, "1.0")]))
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_1, DAY_2, DAY_3})})
    monkeypatch.setattr("backend.services.adj_factor_history_reconciler.time.sleep", lambda _seconds: None)

    with pytest.raises(AdjFactorHistoryReconcileError, match="official listing start"):
        _reconciler(repository, _ListingProvider(symbol, listing_date=DAY_3)).reconcile(end_date=DAY_3)


def test_predecessor_basis_normalization_preserves_history_and_is_idempotent():
    symbol = "920047.BJ"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "30"), (DAY_2, "433.618"), (DAY_3, "433.618")]))
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_2, DAY_3})})
    provider = _ListingProvider(symbol, listing_date=DAY_2)
    reconciler = _reconciler(repository, provider)
    receipt = reconciler.reconcile(end_date=DAY_3, independent=True)

    assert receipt["failures"] == []
    assert receipt["basis_normalization_count"] == 1
    proof = receipt["basis_normalizations"][0]
    assert (proof["scale_numerator"], proof["scale_denominator"]) == ("500", "216809")
    assert proof["overlap_row_count"] == 2
    assert proof["original_rows"] == [
        {"ts_code": r.symbol, "trade_date": r.trade_date.isoformat(), "adj_factor": str(r.value)}
        for r in local.rows
    ]
    assert repository.basis_normalizations == [proof]
    normalized = repository.snapshots[symbol]
    assert {r.trade_date for r in normalized.rows} == {DAY_1, DAY_2, DAY_3}
    assert float(normalized.rows[0].value) == float(Fraction(30) * Fraction(500, 216809))
    assert normalized.rows[-1].value == Decimal(1)
    assert receipt["downstream_invalidation"]["required"] is False  # units, not account economics
    repeat = reconciler.reconcile(end_date=DAY_3, independent=True)
    assert repeat["failures"] == [] and repeat["written_row_count"] == 0
    assert repeat["basis_normalization_count"] == 0
    assert repeat["retained_predecessor_row_count"] == 1
    assert repository.snapshots[symbol] == normalized


@pytest.mark.parametrize("boundary_value", ["433.618", "1"])
def test_predecessor_boundary_preserves_real_provider_restatements(boundary_value):
    symbol = "920047.BJ"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "30"), (DAY_2, boundary_value), (DAY_3, "434")]))
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_2, DAY_3})})
    reconciler = _reconciler(repository, _ListingProvider(symbol, listing_date=DAY_2))

    dry = reconciler.reconcile(end_date=DAY_3, independent=True, dry_run=True)
    assert dry["failures"] == []
    assert repository.snapshots[symbol] == local and repository.basis_normalizations == []
    receipt = reconciler.reconcile(end_date=DAY_3, independent=True)
    assert receipt["failures"] == []
    actual = repository.snapshots[symbol]
    assert actual.rows[0].value == Decimal(str(float(Fraction(30) / Fraction(boundary_value))))
    assert [r.value for r in actual.rows[1:]] == [Decimal(1), Decimal(1)]
    assert receipt["changes"][0]["qfq_changed"] is True
    proof = receipt["basis_normalizations"][0]
    assert proof["policy"] == "certified_boundary_rebase_and_provider_restatement_v1"
    assert proof["boundary_date"] == DAY_2.isoformat()
    assert proof["boundary_local_value"] == boundary_value
    assert proof["boundary_provider_value"] == "1"
    assert proof["restated_overlap_row_count"] == 1
    assert proof["original_rows"][-1]["adj_factor"] == "434"
    assert repository.basis_normalizations == [proof]
    repeat = reconciler.reconcile(end_date=DAY_3, independent=True)
    assert repeat["failures"] == [] and repeat["written_row_count"] == 0
    assert repeat["basis_normalization_count"] == 0


@pytest.mark.parametrize("missing_kind", ["no_boundary", "missing_boundary_row", "one_overlap", "traded_hole"])
def test_predecessor_basis_does_not_authorize_unexplained_gaps(missing_kind, monkeypatch):
    monkeypatch.setattr("backend.services.adj_factor_history_reconciler.time.sleep", lambda _: None)
    symbol = "920047.BJ"
    day4 = DAY_3 + dt.timedelta(days=1)
    values = [(DAY_1, "30"), (DAY_2, "433.618"), (DAY_3, "433.618")]
    if missing_kind == "missing_boundary_row":
        values = [(DAY_1, "30"), (DAY_3, "433.618"), (day4, "433.618")]
    if missing_kind == "one_overlap":
        values = values[:2]
    if missing_kind == "traded_hole":
        values.append((day4, "433.618"))
    local = _snapshot(symbol, _rows(symbol, values))
    repository = _Repository({symbol: local}, {symbol: frozenset({d for d, _ in values})})
    provider = (_SequencedProvider({symbol: [_provider_rows(symbol, [(DAY_2, "1"), (DAY_3, "1")])]})
                if missing_kind == "no_boundary" else _ListingProvider(symbol, listing_date=DAY_2))
    if missing_kind == "missing_boundary_row":
        provider.adj_factor = lambda **_: _provider_rows(symbol, [(d, "1") for d in (DAY_2, DAY_3, day4)])
    receipt = _reconciler(repository, provider).reconcile(end_date=day4, independent=True)
    assert receipt["failed_symbols"] == [symbol]
    assert receipt["written_row_count"] == 0
    assert repository.snapshots[symbol] == local
    assert repository.basis_normalizations == []


@pytest.mark.parametrize("latest_factor,invalidated", [("1", False), ("2", True)])
def test_predecessor_basis_allows_real_append_but_not_a_fake_factor_jump(latest_factor, invalidated):
    symbol = "920047.BJ"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "30"), (DAY_2, "433.618"), (DAY_3, "433.618")]))
    day4 = DAY_3 + dt.timedelta(days=1)
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_2, DAY_3, day4})})

    class Provider(_ListingProvider):
        def adj_factor(self, **_kwargs):
            return _provider_rows(symbol, [(DAY_2, "1"), (DAY_3, "1"), (day4, latest_factor)])

    receipt = _reconciler(repository, Provider(symbol, listing_date=DAY_2)).reconcile(end_date=day4)
    assert receipt["written_row_count"] == 4
    assert receipt["changes"][0]["qfq_changed"] is invalidated
    assert receipt["changes"][0]["local_sha256"] == local.canonical_sha256
    assert repository.snapshots[symbol].rows[-1].value == Decimal(latest_factor)


def test_predecessor_normalization_dry_run_keeps_originals():
    symbol = "920047.BJ"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "30"), (DAY_2, "433.618"), (DAY_3, "433.618")]))
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_2, DAY_3})})
    receipt = _reconciler(repository, _ListingProvider(symbol, listing_date=DAY_2)).reconcile(
        end_date=DAY_3, dry_run=True)
    assert receipt["basis_normalization_count"] == 1
    assert receipt["written_row_count"] == 0
    assert repository.snapshots[symbol] == local
    assert repository.basis_normalizations == []

    assert repository.apply_calls == 0


def test_reconcile_retries_a_transient_empty_provider_page(monkeypatch: pytest.MonkeyPatch) -> None:
    symbol = "000040.SZ"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "1.0")]))
    complete = _provider_rows(symbol, [(DAY_1, "1.0")])
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_1})})
    provider = _SequencedProvider({symbol: [[], complete, complete]})
    monkeypatch.setattr("backend.services.adj_factor_history_reconciler.time.sleep", lambda _seconds: None)

    receipt = _reconciler(repository, provider).reconcile(end_date=DAY_1)

    assert receipt["status"] == "unchanged"
    assert receipt["provider_call_count"] == 3
    assert provider.call_counts[symbol] == 3


def test_reconcile_retries_a_transient_provider_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    symbol = "002117.SZ"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "1.0")]))
    complete = _provider_rows(symbol, [(DAY_1, "1.0")])
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_1})})
    provider = _SequencedProvider({symbol: [TimeoutError("provider timeout"), complete, complete]})
    monkeypatch.setattr("backend.services.adj_factor_history_reconciler.time.sleep", lambda _seconds: None)

    receipt = _reconciler(repository, provider).reconcile(end_date=DAY_1)

    assert receipt["status"] == "unchanged"
    assert receipt["provider_call_count"] == 3
    assert provider.call_counts[symbol] == 3


def test_reconcile_still_fails_closed_after_persistent_provider_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    symbol = "002117.SZ"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "1.0")]))
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_1})})
    provider = _SequencedProvider({symbol: [TimeoutError("provider timeout")]})
    monkeypatch.setattr("backend.services.adj_factor_history_reconciler.time.sleep", lambda _seconds: None)

    with pytest.raises(TimeoutError, match="provider timeout"):
        _reconciler(repository, provider).reconcile(end_date=DAY_1)

    assert provider.call_counts[symbol] == 8
    assert repository.apply_calls == 0


def test_reconcile_still_fails_closed_after_persistent_empty_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    symbol = "000005.SZ"
    local = _snapshot(symbol, _rows(symbol, [(DAY_1, "1.0")]))
    repository = _Repository({symbol: local}, {symbol: frozenset({DAY_1})})
    provider = _SequencedProvider({symbol: [[]]})
    monkeypatch.setattr("backend.services.adj_factor_history_reconciler.time.sleep", lambda _seconds: None)

    with pytest.raises(AdjFactorHistoryReconcileError, match="history is empty"):
        _reconciler(repository, provider).reconcile(end_date=DAY_1)

    assert provider.call_counts[symbol] == 8
    assert repository.apply_calls == 0


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


class _TransactionCursor:
    def __init__(self, connection: "_TransactionConnection") -> None:
        self.connection = connection
        self.rowcount = 2

    def __enter__(self) -> "_TransactionCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, _params: object = None) -> None:
        self.connection.in_transaction = True
        self.connection.sql.append(" ".join(sql.split()))

    def fetchone(self):
        return (False,)  # fixture models an installation before the additive ledger migration

    def fetchall(self):
        sql = self.connection.sql[-1]
        # Model the query's provider population, not a strategy eligibility pool.
        rows = [("000001.SZ", DAY_1), ("302132.SZ", DAY_1), ("600001.SH", DAY_1),
                ("920196.BJ", DAY_1), ("200011.SZ", DAY_1), ("900901.SH", DAY_1)]
        if "'^(00|30)[0-9]{4}\\.SZ$'" in sql:
            return rows[:4]
        return rows


class _TransactionConnection:
    def __init__(self, *, autocommit: bool, in_transaction: bool) -> None:
        self._autocommit = autocommit
        self.in_transaction = in_transaction
        self.autocommit_assignments: list[bool] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.sql: list[str] = []

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        if self.in_transaction:
            raise RuntimeError("set_session cannot be used inside a transaction")
        self.autocommit_assignments.append(value)
        self._autocommit = value

    def cursor(self) -> _TransactionCursor:
        return _TransactionCursor(self)

    def commit(self) -> None:
        self.commit_count += 1
        self.in_transaction = False

    def rollback(self) -> None:
        self.rollback_count += 1
        self.in_transaction = False


def test_adj_factor_scope_uses_a_share_provider_population_not_all_quotes():
    conn = _TransactionConnection(autocommit=False, in_transaction=False)
    scopes = PostgresAdjFactorHistoryRepository(conn).list_symbol_scopes(end_date=DAY_1)
    assert [scope.symbol for scope in scopes] == ["000001.SZ", "302132.SZ", "600001.SH", "920196.BJ"]
    assert "stock_universe" not in conn.sql[-1]  # ST, delisted and aliases remain in history.


def test_explicit_b_share_adj_factor_request_does_not_silently_skip():
    conn = _TransactionConnection(autocommit=False, in_transaction=False)
    with pytest.raises(AdjFactorHistoryReconcileError, match="absent from local price/factor scope"):
        PostgresAdjFactorHistoryRepository(conn).list_symbol_scopes(end_date=DAY_1, symbols=["200011.SZ"])


@pytest.mark.parametrize("initial_autocommit", [False, True])
def test_postgres_apply_staged_respects_an_active_real_transaction(initial_autocommit: bool) -> None:
    symbol = "300506.SZ"
    connection = _TransactionConnection(autocommit=initial_autocommit, in_transaction=True)
    repository = PostgresAdjFactorHistoryRepository(connection)
    current = _snapshot(symbol, _rows(symbol, [(DAY_1, "1.0"), (DAY_2, "1.0")]))
    repository.load_snapshot = lambda *_args, **_kwargs: current  # type: ignore[method-assign]

    inserted = repository.apply_staged(
        symbols=[symbol],
        replace_symbols=[symbol],
        end_date=DAY_2,
        expected_local_sha256={symbol: current.canonical_sha256},
        expected_row_counts={symbol: 2},
    )

    assert inserted == 2
    assert connection.rollback_count == 0
    assert connection.in_transaction is False
    assert connection._autocommit is initial_autocommit
    if initial_autocommit:
        assert connection.commit_count == 2
        assert connection.autocommit_assignments == [False, True]
    else:
        assert connection.commit_count == 1
        assert connection.autocommit_assignments == []
