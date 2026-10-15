"""Native filter date splitting; no new eligibility/TopK implementation."""

from datetime import date, timedelta

import pytest

from backend.services.selection_center.industry_provider import IndustryInfo
from backend.services.selection_center.models import SelectionCandidate
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.trading_core.errors import RuntimeConfigInvalidError


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
