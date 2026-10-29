from datetime import datetime, date
from zoneinfo import ZoneInfo
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.advisory_workbench.quotes import QuoteService, TdxQuotes, session, symbols_from_query
from backend.services.advisory_workbench.projection import WorkbenchError

CN = ZoneInfo("Asia/Shanghai")
DAYS = [dict(cal_date=date(2026, 10, d), is_trading=d != 25) for d in [23, 25, 26]]


@pytest.mark.parametrize("stamp,kind,cutoff", [
    ("2026-10-25T10:00", "CLOSE", "2026-10-23"),
    ("2026-10-26T09:20", "CLOSE", "2026-10-23"),
    ("2026-10-26T09:30", "LIVE", "2026-10-26"),
    ("2026-10-26T11:30", "PAUSED", "2026-10-26"),
    ("2026-10-26T13:00", "LIVE", "2026-10-26"),
    ("2026-10-26T15:00", "CLOSE", "2026-10-26"),
])
def test_calendar_clock_boundary(stamp, kind, cutoff):
    assert session(datetime.fromisoformat(stamp).replace(tzinfo=CN), DAYS) == (kind, cutoff)


@pytest.mark.parametrize("raw", ["", "000001", "000001.SZ,", "../../etc.SH", "123456.XY", ",".join(["000001.SZ"] * 101)])
def test_invalid_symbols_never_reach_tdx(raw):
    with pytest.raises(WorkbenchError):
        symbols_from_query(raw)


class Reader:
    def __init__(self):
        self.calls = []

    def live(self, symbols):
        self.calls.append(symbols)
        return {s: dict(price=12.34, has_trades=True) for s in symbols}

    def close(self, symbol, cutoff):
        self.calls.append((symbol, cutoff))
        if symbol == "000002.SZ":
            raise RuntimeError("private source failure")
        return dict(price=12.3, trade_date=cutoff, kind="CLOSE")


def make(reader=None, stamp="2026-10-26T10:00", tick=None):
    return QuoteService(reader or Reader(), lambda now: DAYS,
                        lambda: datetime.fromisoformat(stamp).replace(tzinfo=CN), timer=tick)


def test_batch_fifty_dedup_cache_and_missing_after_expiry():
    reader = Reader()
    clock = [0]
    service = make(reader, tick=lambda: clock[0])
    symbols = [f"{i:06}.SZ" for i in range(75)]
    result = service.get(",".join(symbols + [symbols[0]]))
    assert [len(c) for c in reader.calls] == [50, 25]
    assert len(result["items"]) == 75 and result["display_only"]
    assert result["items"][symbols[0]]["source_timestamp"] is None
    service.get(symbols[0])
    assert len(reader.calls) == 2
    clock[0] = 11
    reader.live = lambda symbols: {}
    quote = service.get(symbols[0])["items"][symbols[0]]
    assert quote["price"] is None and quote["reason_code"] == "TDX_PRICE_MISSING"


def test_nontrading_close_is_date_bound_and_partial_failure_isolated():
    reader = Reader()
    result = make(reader, stamp="2026-10-25T10:00").get("000001.SZ,000002.SZ")
    assert result["status"] == "PARTIAL"
    assert result["items"]["000001.SZ"]["trade_date"] == "2026-10-23"
    assert result["items"]["000001.SZ"]["price"] == 12.3
    assert result["items"]["000002.SZ"]["reason_code"] == "TDX_UNAVAILABLE"
    assert all(isinstance(c, tuple) for c in reader.calls)


def test_calendar_failure_does_not_infer_weekend_or_read_prices():
    reader = Reader()
    service = make(reader)
    service.calendar = lambda now: []
    assert service.get("000001.SZ")["items"]["000001.SZ"]["reason_code"] == "CALENDAR_UNAVAILABLE"
    assert not reader.calls


def test_no_trades_does_not_invent_a_source_trade_date():
    reader = Reader()
    reader.live = lambda symbols: {s: dict(price=12.3, has_trades=False) for s in symbols}
    quote = make(reader).get("000001.SZ")["items"]["000001.SZ"]
    assert quote["kind"] == "NO_TRADES" and quote["price"] == 12.3
    assert quote["trade_date"] is None


def mock_response(payload):
    return SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)


def test_raw_close_uses_tdx_1000_units_and_ignores_future_and_partial_day(monkeypatch):
    seen = []
    def get(url, **kwargs):
        seen.append((url, kwargs))
        return mock_response(dict(code=0, data=dict(meta=dict(source="tdx", type="day"), list=[
            dict(Time="2026-10-23T15:00:00+08:00", Close=12345),
            dict(Time="2026-10-26T15:00:00+08:00", Close=20000)])))
    monkeypatch.setattr("backend.services.advisory_workbench.quotes.requests.get", get)
    assert TdxQuotes().close("000001.SZ", "2026-10-23") == dict(price=12.345, trade_date="2026-10-23", kind="CLOSE", open_price=None, previous_close=None, previous_close_date=None)
    assert seen[0][0].endswith("/api/kline-all/tdx")
    assert seen[0][1]["params"] == dict(code="SZ000001", type="day", limit=3)


def test_close_rejects_adjusted_other_provider(monkeypatch):
    monkeypatch.setattr("backend.services.advisory_workbench.quotes.requests.get",
                        lambda *a, **k: mock_response(dict(code=0, data=dict(meta=dict(source="ths", type="day")))))
    with pytest.raises(ValueError):
        TdxQuotes().close("000001.SZ", "2026-10-23")


def test_batch_uses_close_not_previous_last_and_rejects_duplicate_and_zero(monkeypatch):
    rows = [dict(Code="000001", Exchange=0, K=dict(Close=12340, Last=9990, Open=11000), TotalHand=1),
            dict(Code="000002", Exchange=0, K=dict(Close=0)),
            dict(Code="000003", Exchange=0, K=dict(Close=1000)),
            dict(Code="000003", Exchange=0, K=dict(Close=2000)),
            dict(Code="000004", Exchange=0, K=dict(Close=float("nan")))]
    monkeypatch.setattr("backend.services.advisory_workbench.quotes.requests.post",
                        lambda *a, **k: mock_response(dict(code=0, data=rows)))
    result = TdxQuotes().live([f"00000{i}.SZ" for i in range(1, 5)])
    assert result["000001.SZ"]["price"] == 12.34
    assert result['000001.SZ']['open_price'] == 11
    assert result['000001.SZ']['previous_close'] == 9.99
    assert result["000002.SZ"]["price"] is None
    assert result["000003.SZ"]["reason_code"] == "TDX_DUPLICATE_SYMBOL"
    assert result["000004.SZ"]["price"] is None


def test_route_get_only_bounded_and_does_not_call_program_actions():
    from backend.routers.advisory_workbench import get_quotes, router
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_quotes] = lambda: make()
    client = TestClient(app)
    assert client.get("/workbench/quotes?symbols=000001.SZ").json()["items"]["000001.SZ"]["price"] == 12.34
    assert client.get("/workbench/quotes?symbols=bad").status_code == 422
    assert client.post("/workbench/quotes?symbols=000001.SZ").status_code == 405


def test_today_change_uses_yesterday_not_the_segment_open():
    reader = Reader()
    reader.live = lambda symbols: {s: dict(price=12, open_price=11, previous_close=10, has_trades=True) for s in symbols}
    row = make(reader).get('000001.SZ')['items']['000001.SZ']
    assert row['today_return_bps'] == pytest.approx(2000)
    assert row['previous_close_date'] == '2026-10-23'
    reader.live = lambda symbols: {s: dict(price=12, open_price=11, previous_close=0, has_trades=True) for s in symbols}
    assert make(reader).get('000001.SZ')['items']['000001.SZ']['today_return_bps'] is None


def test_close_day_change_requires_exact_previous_trading_date(monkeypatch):
    payload = dict(code=0, data=dict(meta=dict(source='tdx', type='day'), list=[
        dict(Time='2026-10-22T15:00:00+08:00', Close=10000, Open=9900, Volume=10),
        dict(Time='2026-10-23T15:00:00+08:00', Close=12000, Open=11000, Volume=10)]))
    monkeypatch.setattr('backend.services.advisory_workbench.quotes.requests.get', lambda *a, **k: mock_response(payload))
    service = make(TdxQuotes(), stamp='2026-10-25T10:00')
    service.calendar = lambda _: [dict(cal_date=date(2026, 10, 22), is_trading=True), *DAYS]
    row = service.get('000001.SZ')['items']['000001.SZ']
    assert row['open_price'] == 11 and row['today_return_bps'] == pytest.approx(2000)
    payload['data']['list'][0]['Time'] = '2026-10-21T15:00:00+08:00'
    row = make(TdxQuotes(), stamp='2026-10-25T10:00').get('000001.SZ')['items']['000001.SZ']
    assert row['today_return_bps'] is None
