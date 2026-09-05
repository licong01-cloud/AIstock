from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from backend.services.position_timing.alerts import fetch_quotes_in_contract_chunks, parse_alert_quote
from backend.services.position_timing.contracts import AlertClaimRequest


CHINA_TZ = ZoneInfo("Asia/Shanghai")


def _quote(clock: list[datetime], *, price: str = "9.00", observed_delta_seconds: int = 2):
    def load(symbols: list[str]):
        observed = clock[0] - timedelta(seconds=observed_delta_seconds)
        return {
            symbol: {
                "quote_price_raw": price,
                "quote_open_raw": price,
                "quote_observed_at": observed,
                "price_basis": "raw_cny",
            }
            for symbol in symbols
        }

    return load


def _claim_request(item: dict) -> AlertClaimRequest:
    return AlertClaimRequest.model_validate(
        {
            key: item[key]
            for key in (
                "card_id",
                "eligibility_identity",
                "quote_price_raw",
                "quote_open_raw",
                "quote_observed_at",
                "alert_evaluated_at",
                "quote_source",
                "position_snapshot_sha256",
                "intent_snapshot_sha256",
            )
        }
    )


def test_alert_poll_is_zero_write_and_atomic_claim_is_exact_once(service_factory) -> None:
    clock = [datetime(2026, 9, 3, 16, 0, tzinfo=CHINA_TZ)]
    service = service_factory(now=lambda: clock[0], quote=_quote(clock))
    service.materialize()
    clock[0] = datetime(2026, 9, 4, 9, 31, tzinfo=CHINA_TZ)
    before = sorted(
        str(path.relative_to(service.store.root))
        for path in service.store.root.rglob("*")
        if path.is_file()
    )

    poll = service.poll_alerts()
    after = sorted(
        str(path.relative_to(service.store.root))
        for path in service.store.root.rglob("*")
        if path.is_file()
    )
    assert after == before
    edge = poll["items"][0]
    assert edge["status"] == "ELIGIBLE"
    assert edge["system_edge_eligibility"] is True
    assert edge["already_alerted"] is False

    request = _claim_request(edge)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: service.claim_alert(trigger_id=edge["trigger_id"], request=request), range(2)))
    assert sorted(result["granted"] for result in results) == [False, True]
    assert service.store.event_counts() == {
        "ALERT_EMISSION_AUTHORIZED": 1,
        "CARD_ISSUED": 1,
    }
    repeated = service.poll_alerts()["items"][0]
    assert repeated["already_alerted"] is True
    assert repeated["system_edge_eligibility"] is True


def test_alert_poll_reports_stale_future_and_missing_quotes_without_events(service_factory) -> None:
    cases = [
        (-301, "QUOTE_STALE"),
        (31, "QUOTE_FUTURE_SKEW"),
    ]
    for offset_seconds, expected in cases:
        clock = [datetime(2026, 9, 3, 16, 0, tzinfo=CHINA_TZ)]

        def quote_loader(symbols, *, offset=offset_seconds):
            return {
                symbol: {
                    "quote_price_raw": "9",
                    "quote_open_raw": "9",
                    "quote_observed_at": clock[0] + timedelta(seconds=offset),
                    "price_basis": "raw_cny",
                }
                for symbol in symbols
            }

        service = service_factory(now=lambda: clock[0], quote=quote_loader)
        service.materialize()
        clock[0] = datetime(2026, 9, 4, 9, 31, tzinfo=CHINA_TZ)
        assert service.poll_alerts()["items"][0]["status"] == expected
        assert service.store.event_counts() == {"CARD_ISSUED": 1}

    clock = [datetime(2026, 9, 3, 16, 0, tzinfo=CHINA_TZ)]
    service = service_factory(now=lambda: clock[0], quote=lambda _symbols: {})
    service.materialize()
    clock[0] = datetime(2026, 9, 4, 9, 31, tzinfo=CHINA_TZ)
    assert service.poll_alerts()["items"][0]["status"] == "QUOTE_UNAVAILABLE"


def test_alert_poll_invalidates_position_and_intent_drift(service_factory, holding_rows) -> None:
    clock = [datetime(2026, 9, 3, 16, 0, tzinfo=CHINA_TZ)]
    service = service_factory(now=lambda: clock[0], holdings=holding_rows, quote=_quote(clock))
    service.materialize()
    clock[0] = datetime(2026, 9, 4, 9, 31, tzinfo=CHINA_TZ)
    holding_rows[0]["quantity"] = 900
    assert service.poll_alerts()["items"][0]["status"] == "POSITION_SNAPSHOT_CHANGED"

    holding_rows[0]["quantity"] = 1000
    service.put_intent(
        raw_symbol="000001.SZ",
        planned_full_notional_cny="100000",
        desired_target_exposure="0.5",
    )
    assert service.poll_alerts()["items"][0]["status"] == "INTENT_SNAPSHOT_CHANGED"


def test_quote_loader_is_called_in_fifty_symbol_chunks() -> None:
    calls: list[list[str]] = []

    def loader(symbols: list[str]):
        calls.append(symbols)
        return {symbol: {"quote_price_raw": 1} for symbol in symbols}

    symbols = [f"{index:06d}.SZ" for index in range(101)]
    result = fetch_quotes_in_contract_chunks(symbols=symbols, quote_loader=loader)
    assert [len(chunk) for chunk in calls] == [50, 50, 1]
    assert len(result) == 101


def test_tdx_li_quote_parser_preserves_exact_age_and_future_boundaries() -> None:
    evaluated = datetime(2026, 9, 4, 9, 35, tzinfo=CHINA_TZ)
    parsed, status, _ = parse_alert_quote(
        symbol="000001.SZ",
        raw_quote={
            "K": {"Open": 9_000, "Close": 9_100},
            "time": "20260904093000",
        },
        evaluated_at=evaluated,
    )
    assert status == "FRESH"
    assert parsed is not None
    assert str(parsed.open_raw) == "9"
    assert str(parsed.price_raw) == "9.1"

    future, future_status, _ = parse_alert_quote(
        symbol="000001.SZ",
        raw_quote={
            "K": {"Open": 9_000, "Close": 9_100},
            "quote_observed_at": evaluated + timedelta(seconds=30),
        },
        evaluated_at=evaluated,
    )
    assert future_status == "FRESH"
    assert future is not None


def test_alert_module_has_no_minute_history_or_notification_dependency() -> None:
    source = (
        Path(__file__).resolve().parents[2]
        / "services"
        / "position_timing"
        / "alerts.py"
    ).read_text(encoding="utf-8")
    assert "fetch_minute_kline_tdx" not in source
    assert "notification_service" not in source
    assert "from backend.services.simulation_runtime.scheduler" not in source
