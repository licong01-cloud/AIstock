"""Native filter date splitting; no new eligibility/TopK implementation."""

from contextlib import contextmanager
from datetime import date, timedelta

import pytest

from backend.services.selection_center.industry_provider import IndustryInfo
from backend.services.selection_center.models import SelectionCandidate
from backend.services.selection_center.tradability import DbSuspendLookupProvider, TradabilityFilter
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError


def test_native_filter_splits_only_industry_clock_and_keeps_old_default():
    day = date(2026, 6, 2)
    cutoff = day-timedelta(days=1)
    reads = []

    class Suspension:
        def get_suspended_symbols(self, symbols, trade_date):
            reads.append(("suspension", trade_date))
            return {}

    class Industry:
        def get_industries(self, symbols, trade_date):
            reads.append(("industry", trade_date))
            return {s: IndustryInfo(s, l1_name="BLOCK" if trade_date == cutoff else "OK") for s in symbols}

    native = TradabilityFilter(suspend_provider=Suspension(), industry_provider=Industry())
    kwargs = dict(candidates=[SelectionCandidate(symbol="000001.SZ", score=.5, rank=1)],
                  trade_date=day, top_k=1, package_id="any-package", manifest_sha256="a"*64,
                  industry_blacklist=["BLOCK"], enabled=True, allow_empty=True, exhaustive_evidence=True)
    old = native.filter_candidates_with_receipt(**kwargs)
    assert reads == [("suspension", day), ("industry", day)] and len(old.candidates) == 1
    reads.clear()
    new = native.filter_candidates_with_receipt(**kwargs, industry_trade_date=cutoff)
    assert reads == [("suspension", day), ("industry", cutoff)] and not new.candidates
    assert new.exclusions[0].context["trade_date"] == day.isoformat()
    assert new.exclusions[0].context["industry_trade_date"] == cutoff.isoformat()
    assert new.receipt.semantic_payload["industry_trade_date"] == cutoff.isoformat()


def test_native_filter_rejects_future_industry_date_before_lookup():
    day = date(2026, 6, 2)
    with pytest.raises(RuntimeConfigInvalidError, match="industry observation date"):
        TradabilityFilter().filter_candidates_with_receipt(candidates=[], trade_date=day, top_k=1,
                                                          package_id="any-package", manifest_sha256="a"*64,
                                                          industry_trade_date=day+timedelta(days=1))


class _SuspensionMarket:
    def __init__(self, rows):
        self.rows, self.reads = rows, []

    @contextmanager
    def connection(self):
        yield self

    @contextmanager
    def cursor(self):
        yield self

    def execute(self, sql, params):
        self.reads.append(params)
        self.filtered = [row for row in self.rows if row[0] in params[0]]
        if "suspend_type = 'S'" in sql:
            self.filtered = [row for row in self.filtered if row[1] == "S"]

    def fetchall(self):
        return self.filtered


def test_native_filter_uses_official_day_authority_and_preserves_backfill_and_raw_rank():
    symbol, day = "603616.SH", date(2026, 1, 22)
    market = _SuspensionMarket([(symbol, "S", None), (symbol, "R", None)])
    rows = [SelectionCandidate(symbol=symbol, score=.8, rank=1),
            SelectionCandidate(symbol="000001.SZ", score=.6, rank=2)]
    result = TradabilityFilter(suspend_provider=DbSuspendLookupProvider(market.connection)).filter_candidates_with_receipt(
        candidates=rows, trade_date=day, top_k=1, package_id="native-package", manifest_sha256="a" * 64,
        exhaustive_evidence=True, industry_trade_date=day - timedelta(days=1),
    )
    assert [(r.symbol, r.score, r.rank) for r in rows] == [(symbol, .8, 1), ("000001.SZ", .6, 2)]
    assert [r.symbol for r in result.candidates] == ["000001.SZ"]
    assert result.candidates[0].component_scores["raw_rank"] == 2
    assert result.exclusions[0].reason == "suspended_by_suspend_d"
    assert market.reads == [([symbol, "000001.SZ"], day)]
    receipt = result.receipt.semantic_payload["suspension_day_authority"]
    assert receipt["usage"] == "D_EFFECTIVE_MARKET_STATE"
    assert receipt["historical_publication_time_proven"] is False
    assert receipt["projected_days"][0]["basis"] == "OFFICIAL_DISCLOSURE"
    assert result.exclusions[0].context["suspension_day_authority"]["raw_event_count"] == 2


@pytest.mark.parametrize("rows", [
    [("000001.SZ", "S", None), ("000001.SZ", "R", None)],
    [("603616.SH", "S", None), ("603616.SH", "S", None)],
    [("603616.SH", "S", "09:30-10:30"), ("603616.SH", "R", None)],
    [("603616.SH", "X", None)],
])
def test_db_suspend_lookup_fails_closed_for_unresolved_provider_events(rows):
    market = _SuspensionMarket(rows)
    with pytest.raises(DataUnavailableError, match="effective day is unresolved"):
        DbSuspendLookupProvider(market.connection).get_suspended_symbols([row[0] for row in rows], date(2026, 1, 22))


@pytest.mark.parametrize("kind,timing,excluded", [("R", None, False), ("S", None, True), ("S", "09:30-10:30", True)])
def test_db_suspend_lookup_keeps_native_resume_and_intraday_semantics(kind, timing, excluded):
    market = _SuspensionMarket([("000001.SZ", kind, timing)])
    result = DbSuspendLookupProvider(market.connection).get_suspended_symbols(["000001.SZ"], date(2026, 1, 22))
    assert ("000001.SZ" in result) is excluded
    assert len(result.receipt["raw_events"]) == 1


def test_shared_authority_is_process_frozen_and_does_not_read_later_file(monkeypatch):
    from backend.services.selection_center import suspension_provider as shared
    before = shared.project_selection_suspension_days([], symbols=["603616.SH"],
        required_dates=[date(2026, 1, 22)], usage="D_EFFECTIVE_MARKET_STATE")
    monkeypatch.setattr(shared.public_suspension, "DEFAULT_PATH", None)
    after = shared.project_selection_suspension_days([], symbols=["603616.SH"],
        required_dates=[date(2026, 1, 22)], usage="D_EFFECTIVE_MARKET_STATE")
    assert before == after


def test_shared_authority_rejects_out_of_scope_rows():
    from backend.services.selection_center.suspension_provider import project_selection_suspension_days
    with pytest.raises(ValueError, match="exceeds requested"):
        project_selection_suspension_days([dict(ts_code="603616.SH", trade_date="2026-01-23", suspend_type="S")],
            symbols=["603616.SH"], required_dates=[date(2026, 1, 22)], usage="C_EFFECTIVE_SUSPENSION_REFERENCE")
