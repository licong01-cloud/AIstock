from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers.advisory_workbench import get_stop_alerts, router
from backend.services.advisory_workbench.stop_alerts import CLOSE_DELAY, MinuteStopAlerts, minute_stop
from backend.services.position_timing.intraday.contracts import CHINA


DAY = date(2026, 11, 19)
PREVIOUS = DAY - timedelta(days=1)
HOLD, OUTSIDE, NEW, EXIT, WATCH, REPLAY, WRONG_BINDING = [f"60000{i}.SH" for i in range(1, 8)]


def at(hour, minute, second=0):
    return datetime.combine(DAY, time(hour, minute, second), CHINA)


def bar(when, close, *, low=8, volume=1):
    return dict(time=when, open=10, high=10, low=low, close=close, volume=volume)


def evaluate(rows, *, now=None, after=None):
    now = now or at(9, 32, 5)
    return minute_stop(rows, day=DAY, cutoff=now - CLOSE_DELAY, received_at=now,
                       entry_price=Decimal("10"), stop_bps=800, after=after)


def test_close_at_threshold_fires_once_and_uses_first_closed_minute():
    result = evaluate([bar(at(9, 31), 9.3), bar(at(9, 32), 9.2), bar(at(9, 33), 8.7)])
    assert result["status"] == "TRIGGERED"
    assert result["triggered_at"] == at(9, 32)
    assert result["trigger_price"] == Decimal("9.2")
    assert result["return_bps"] == -800
    assert result["first_trigger_verified"] is True
    assert result["observed_bars"] == result["expected_bars"] == 2


def test_low_unfinished_and_auction_minutes_cannot_trigger():
    result = evaluate([bar(at(9, 30), 9), bar(at(9, 31), 9.3), bar(at(9, 32), 9)], now=at(9, 32, 4))
    assert result["status"] == "NOT_TRIGGERED"
    assert result["observed_through"] == at(9, 31)
    assert result["expected_bars"] == 1


def test_lunch_and_closing_auction_are_outside_the_session():
    result = evaluate([bar(at(11, 30), 9.3), bar(at(12, 0), 8), bar(at(13, 1), 8)], now=at(12, 10))
    assert result["status"] == "UNVERIFIED"
    assert result["expected_bars"] == 120
    assert result["observed_through"] == at(11, 30)
    result = evaluate([bar(at(14, 57), 9.3), bar(at(14, 58), 8), bar(at(15, 0), 8)], now=at(15, 5))
    assert result["status"] == "UNVERIFIED"
    assert result["expected_bars"] == 237
    assert result["observed_through"] == at(14, 57)


def test_missing_or_stale_bars_fail_closed_and_later_hit_is_visible():
    reading = evaluate([bar(at(9, 31), 9.3)])
    assert (reading["status"], reading["reason_code"]) == ("UNVERIFIED", "MINUTE_COVERAGE_INCOMPLETE")
    reading = evaluate([bar(at(9, 32), 9)])
    assert reading["status"] == "TRIGGERED"
    assert reading["first_trigger_verified"] is False and reading["coverage"] == "INCOMPLETE"


def test_same_day_confirmation_and_zero_volume_gate():
    rows = [bar(at(9, 31), 9), bar(at(9, 32), 9, volume=0)]
    reading = evaluate(rows, after=at(9, 31))
    assert (reading["status"], reading["reason_code"]) == ("UNVERIFIED", "NO_TRADED_MINUTES")
    reading = evaluate(rows, after=at(9, 31, 4))
    assert reading["reason_code"] == "NO_CLOSED_MINUTES"


def item(symbol, episode_id, *, version="v1", action="HOLD", reason=None):
    return dict(symbol=symbol, stock_name=symbol, episode_id=episode_id, program_id="p",
                list_version_id=version, binding_version_id="b", action=action, reason_code=reason,
                item_state="ACTIVE")


class Repository:
    status = "ENABLED"
    is_trading = True
    stop_bps = 800
    binding = "b"
    list_id = "v1"
    exit_signal = None
    entry_confirmed_at = at(9, 30)
    in_snapshot = False

    @contextmanager
    def transaction(self):
        assert not self.in_snapshot
        self.in_snapshot = True
        try:
            yield self
        finally:
            self.in_snapshot = False

    def program(self, program_id):
        assert self.in_snapshot and program_id == "p"
        return dict(status=self.status, program_payload_json={"review_policy": {"stop_loss_bps": self.stop_bps}})

    def calendar(self, start, end):
        assert start == end == DAY
        return [dict(cal_date=DAY, is_trading=self.is_trading)]

    def bindings(self, program_id):
        return [dict(binding_version_id=self.binding, activation_status="ACTIVE")]

    def rows(self, sql, params):
        assert self.in_snapshot
        if "market.suspend_d" in sql:
            return [dict(ts_code=OUTSIDE)]
        if "ORDER BY created_at" in sql:
            return [dict(list_version_id=self.list_id, binding_version_id="b", previous_list_version_id="old")]
        assert params == ("p", "old", "b", DAY)
        return [dict(list_version_id="old")]

    def items(self, program_id, versions):
        assert versions == ["v1", "old"]
        return [
            item(HOLD, "e1"), item(OUTSIDE, "e2", version="old"),
            item(NEW, None, action="WATCH", reason="PENDING_TARGET_OPEN_ENTRY"),
            item(EXIT, "e4"), item(WATCH, None, action="WATCH", reason="OUTSIDE_ENTRY_THRESHOLD"),
            item(REPLAY, "e6"), item(WRONG_BINDING, "e7"),
            item("600099.SH", None, action="WATCH", reason="PENDING_TARGET_OPEN_ENTRY"),
        ]

    def entry_links(self, program_id, version, symbols):
        assert symbols == [NEW, "600099.SH"]
        return [dict(symbol=NEW, episode_id="e3")]

    def episodes(self, program_id, filters):
        assert filters == dict(episode_ids=["e1", "e2", "e3", "e4", "e6", "e7"],
                               binding="b", source="FORWARD", status="ACTIVE")
        rows = []
        for index, symbol in enumerate((HOLD, OUTSIDE, NEW, EXIT), start=1):
            rows.append(dict(episode_id=f"e{index}", symbol=symbol, episode_status="ACTIVE",
                             effective_entry_date=DAY if symbol == NEW else PREVIOUS, effective_exit_date=None,
                             exit_signal_date=DAY if symbol == EXIT else self.exit_signal if symbol == HOLD else None,
                             entry_price=Decimal("10"), created_at=self.entry_confirmed_at if symbol == NEW else None))
        return rows, len(rows)


def make_service(repo, fetch=None, now=None):
    return MinuteStopAlerts(repository=repo, clock=lambda: now or at(9, 32, 5), fetch=fetch)


def client(service):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_stop_alerts] = lambda: service
    return TestClient(app)


def request(service, *, day=DAY, version="v1"):
    return client(service).get(f"/workbench/programs/p/minute-stop-alerts?target_date={day}&list_version_id={version}")


def test_route_reads_both_groups_and_separates_sellability_from_triggers():
    repo = Repository()
    calls = []

    def fetch(symbol, day):
        assert not repo.in_snapshot and day == DAY
        calls.append(symbol)
        return [bar(at(9, 31), 9), bar(at(9, 32), 8.5)]

    service = make_service(repo, fetch)
    first, second = request(service), request(service)
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    assert sorted(calls) == sorted([HOLD, OUTSIDE, NEW] * 2)
    report = first.json()
    assert report["eligible_count"] == report["evaluated_count"] == 3
    assert report["stop_loss_bps"] == 800 and report["list_version_id"] == "v1"
    assert len({row["alert_id"] for row in report["items"]}) == 3
    assert [(row["membership"], row["status"], row["sellability"]) for row in report["items"]] == [
        ("IN_SCOPE", "TRIGGERED", "UNVERIFIED"),
        ("OUTSIDE_PENDING", "TRIGGERED", "SUSPENSION_RECORDED"),
        ("IN_SCOPE", "TRIGGERED", "T1_RESTRICTED"),
    ]


def test_snapshot_change_during_minutes_read_invalidates_alert():
    repo = Repository()

    def fetch(symbol, day):
        repo.exit_signal = DAY
        return [bar(at(9, 31), 9)]

    response = request(make_service(repo, fetch))
    assert response.status_code == 409
    assert response.json()["detail"]["reason_code"] == "STOP_ALERT_SNAPSHOT_CHANGED"


@pytest.mark.parametrize(("attribute", "value", "expected"), [
    ("is_trading", None, "CALENDAR_UNVERIFIED"),
    ("is_trading", False, "NON_TRADING_DAY"),
    ("status", "PAUSED", "PROGRAM_DISABLED"),
    ("binding", "other", "BINDING_UNVERIFIED"),
    ("stop_bps", 0, "RULE_DISABLED"),
    ("stop_bps", None, "RULE_UNVERIFIED"),
])
def test_unverified_or_inactive_context_does_not_read_bars(attribute, value, expected):
    repo = Repository()
    setattr(repo, attribute, value)
    response = request(make_service(repo, lambda *_: pytest.fail("unexpected minute fetch")))
    assert response.status_code == 200
    assert response.json()["status"] == expected


def test_historical_and_list_identity_requests_do_not_read_bars():
    service = make_service(Repository(), lambda *_: pytest.fail("unexpected minute fetch"))
    assert request(service, day=PREVIOUS).json()["status"] == "NOT_CURRENT_DAY"
    drift = request(service, version="old")
    assert drift.status_code == 409
    assert drift.json()["detail"]["reason_code"] == "STOP_ALERT_LIST_CHANGED"
    c = client(service)
    assert c.get(f"/workbench/programs/p/minute-stop-alerts?target_date={DAY}").status_code == 422
    assert c.post(f"/workbench/programs/p/minute-stop-alerts?target_date={DAY}&list_version_id=v1").status_code == 405


def test_missing_market_source_is_visible_with_other_rows_intact():
    def fetch(symbol, day):
        if symbol == HOLD:
            raise RuntimeError("upstream unavailable")
        return [bar(at(9, 31), 9.3), bar(at(9, 32), 9.5)]

    response = request(make_service(Repository(), fetch))
    report = response.json()
    assert response.status_code == 200 and report["evaluated_count"] == 2
    assert report["items"][0]["reason_code"] == "MINUTE_SOURCE_UNAVAILABLE"


def test_conflicting_minute_identity_is_unverified_in_real_route():
    def fetch(symbol, day):
        return [bar(at(9, 31), 9), bar(at(9, 31), 9.1)]

    report = request(make_service(Repository(), fetch)).json()
    assert report["evaluated_count"] == 0
    assert {row["reason_code"] for row in report["items"]} == {"MINUTE_DATA_INVALID"}
