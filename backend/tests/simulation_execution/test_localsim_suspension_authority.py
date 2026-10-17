from contextlib import contextmanager
from copy import deepcopy
from datetime import date
from types import SimpleNamespace

import pytest

from backend.data_service import suspension_day_authority as public
from backend.services.simulation_execution.localsim import suspension as consumer
from backend.services.simulation_data.daily_context import canonical_json_sha256
from backend.services.strategy_package.reference_prices import ReferencePrices
from backend.services.trading_core.errors import DataUnavailableError


DAY, CUTOFF = date(2026, 1, 22), date(2026, 1, 21)
SYMBOL = "603616.SH"


def _provider(*, rows=None, authority=None, pre_close=None, reference_missing=False):
    rows = [(SYMBOL, DAY, "S", None), (SYMBOL, DAY, "R", None)] if rows is None else rows
    calls = []

    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def execute(self, query, params):
            assert "market.suspend_d" in query
            assert params[0] == DAY
        def fetchall(self): return deepcopy(rows)

    @contextmanager
    def connection():
        yield SimpleNamespace(cursor=Cursor)

    def limits(symbols, day):
        calls.append(("limits", symbols, day))
        return {s: SimpleNamespace(symbol=s, trade_date=day, pre_close=pre_close,
                                   up_limit=7.0, down_limit=5.0) for s in symbols}

    def references(**kwargs):
        calls.append(("reference", kwargs))
        prices = ReferencePrices()
        if not reference_missing:
            prices[SYMBOL] = 6.23
        prices.observations[SYMBOL] = {
            "contract_version": "strategy_package_reference_price_availability_v1",
            "state": "UNAVAILABLE" if reference_missing else "SUSPENSION_LAST_OBSERVED_CLOSE",
            "as_of_date": CUTOFF.isoformat(), "quote_date": None if reference_missing else "2026-01-20",
            "suspension_dates": [CUTOFF.isoformat()],
            "reason_code": "REFERENCE_CLOSE_MISSING" if reference_missing else None,
        }
        return prices

    provider = consumer.LocalSimHistoricalSuspensionProvider(
        conn_factory=connection, authority=authority, reference_loader=references,
        calendar=SimpleNamespace(snapshot=lambda _: SimpleNamespace(is_trading_day=True, previous_trading_date=CUTOFF)),
        limit_provider=SimpleNamespace(get_limit_prices=limits),
    )
    return provider, calls


def test_disclosure_resolved_SR_uses_public_projection_without_reordering_or_a_price():
    provider, calls = _provider()
    status = provider.load_statuses(symbols=[SYMBOL], trade_date=DAY)[SYMBOL]
    assert status["is_tradable"] is False and calls == []
    fact = status[consumer.SUSPENSION_KEY]
    assert [r["suspend_type"] for r in fact["suspension_facts"]] == ["S", "R"]
    frozen = fact[consumer.DAY_AUTHORITY_KEY]
    expected = next(row for row in public.project_suspension_days([
        {"ts_code": SYMBOL, "trade_date": DAY, "suspend_type": "S", "suspend_timing": None},
        {"ts_code": SYMBOL, "trade_date": DAY, "suspend_type": "R", "suspend_timing": None},
    ], required_dates=[DAY]) if row["ts_code"] == SYMBOL)
    assert frozen["projected_day"] == expected
    assert frozen["projected_day"]["state"] == "FULL_DAY_SUSPENDED"
    assert frozen["projected_day"]["historical_publication_time_proven"] is False
    assert frozen["usage"] == "D_EFFECTIVE_MARKET_STATE"
    assert frozen["effective_trade_date"] == DAY.isoformat() and frozen["cutoff_date"] == CUTOFF.isoformat()
    assert consumer.validate_suspension_fact(fact, symbol=SYMBOL, trade_date=DAY) == fact


def test_disclosure_halt_valuation_retains_C_reference_and_native_pre_close_priority():
    provider, calls = _provider()
    status = provider.load_statuses(symbols=[SYMBOL], trade_date=DAY, valuation_symbols=[SYMBOL])[SYMBOL]
    evidence = status[consumer.REFERENCE_KEY]
    assert evidence["reference_close"] == 6.23 and evidence["reference_price_availability"]["quote_date"] == "2026-01-20"
    assert calls[1][1]["as_of_date"] == CUTOFF and evidence[consumer.DAY_AUTHORITY_KEY]["projected_day"]["raw_event_count"] == 2
    assert consumer.validate_suspension_reference(evidence, symbol=SYMBOL, trade_date=DAY) == evidence
    provider, calls = _provider(pre_close=6.2)
    status = provider.load_statuses(symbols=[SYMBOL], trade_date=DAY, valuation_symbols=[SYMBOL])[SYMBOL]
    assert status[consumer.NATIVE_PRE_CLOSE_KEY]["pre_close"] == 6.2
    assert consumer.REFERENCE_KEY not in status and [c[0] for c in calls] == ["limits"]


def test_legal_halt_does_not_fill_a_missing_authoritative_valuation_reference():
    provider, _ = _provider(reference_missing=True)
    with pytest.raises(DataUnavailableError) as caught:
        provider.load_statuses(symbols=[SYMBOL], trade_date=DAY, valuation_symbols=[SYMBOL])
    assert caught.value.context["reason_code"] == "LOCALSIM_SUSPENSION_REFERENCE_INVALID"


def test_frozen_projection_validates_its_own_payload_after_current_authority_changes(monkeypatch):
    provider, _ = _provider()
    fact = provider.load_statuses(symbols=[SYMBOL], trade_date=DAY)[SYMBOL][consumer.SUSPENSION_KEY]
    empty = {"schema_version": public.SCHEMA, "policy": public.POLICY, "entries": []}
    monkeypatch.setattr(consumer, "_LOADED_AUTHORITY", empty)
    assert consumer.validate_suspension_fact(fact, symbol=SYMBOL, trade_date=DAY) == fact
    changed, _ = _provider()
    with pytest.raises(DataUnavailableError, match="conflict"):
        changed.load_statuses(symbols=[SYMBOL], trade_date=DAY)


@pytest.mark.parametrize("change", ["raw", "authority", "projection", "identity", "implementation", "date", "usage"])
def test_frozen_evidence_refuses_changed_data_rules_or_clock_even_with_rehashed_carrier(change):
    provider, _ = _provider()
    fact = provider.load_statuses(symbols=[SYMBOL], trade_date=DAY)[SYMBOL][consumer.SUSPENSION_KEY]
    frozen = fact[consumer.DAY_AUTHORITY_KEY]
    if change == "raw":
        fact["suspension_facts"][0]["symbol"] = "000001.SZ"
    elif change == "authority":
        frozen["authority_payload"]["entries"] = []
    elif change == "projection":
        frozen["projected_day"]["basis"] = "guessed"
    elif change == "identity":
        frozen["data_identity"] = "0" * 64
    elif change == "implementation":
        frozen["policy_identity"]["implementation_sha256"] = "0" * 64
    elif change == "date":
        frozen["effective_trade_date"] = CUTOFF.isoformat()
    else:
        frozen["usage"] = "C_KNOWN_INFORMATION"
    fact["evidence_sha256"] = canonical_json_sha256({k: v for k, v in fact.items() if k != "evidence_sha256"})
    with pytest.raises(DataUnavailableError) as caught:
        consumer.validate_suspension_fact(fact, symbol=SYMBOL, trade_date=DAY)
    assert caught.value.context["reason_code"] == "LOCALSIM_SUSPENSION_FACT_INVALID"


def test_legacy_SR_evidence_does_not_borrow_current_disclosure_and_legacy_single_S_stays_valid():
    from backend.tests.simulation_execution.test_localsim_runtime import _halt_reference

    fact = _halt_reference(SYMBOL, DAY)
    assert consumer.validate_suspension_reference(fact, symbol=SYMBOL, trade_date=DAY) == fact
    fact["suspension_facts"].append({"symbol": SYMBOL, "trade_date": DAY.isoformat(), "suspend_type": "R", "suspend_timing": None})
    fact["evidence_sha256"] = canonical_json_sha256({k: v for k, v in fact.items() if k != "evidence_sha256"})
    with pytest.raises(DataUnavailableError):
        consumer.validate_suspension_reference(fact, symbol=SYMBOL, trade_date=DAY)


def test_non_disclosure_SR_stays_unresolved_and_normal_no_event_stock_is_not_halted():
    empty = {"schema_version": public.SCHEMA, "policy": public.POLICY, "entries": []}
    provider, _ = _provider(authority=empty)
    with pytest.raises(DataUnavailableError, match="conflict"):
        provider.load_statuses(symbols=[SYMBOL], trade_date=DAY)
    provider, calls = _provider(rows=[], authority=empty)
    status = provider.load_statuses(symbols=[SYMBOL], trade_date=DAY)[SYMBOL]
    assert status["is_tradable"] is True and consumer.SUSPENSION_KEY not in status and calls == []


def test_disclosure_and_contradictory_intraday_event_remain_unresolved():
    provider, calls = _provider(rows=[(SYMBOL, DAY, "S", "09:30-10:30")])
    with pytest.raises(DataUnavailableError, match="conflict"):
        provider.load_statuses(symbols=[SYMBOL], trade_date=DAY)
    assert calls == []
