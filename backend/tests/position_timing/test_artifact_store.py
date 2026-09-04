import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from backend.services.position_timing.artifact_store import (
    CardSetIdentityConflict,
    ImmutableArtifactConflict,
    PositionTimingArtifactError,
)
from backend.services.position_timing.contracts import (
    AlertEmissionAuthorizedEventV1,
    MaturityStatus,
    OutcomeEvaluatedEventV1,
    PolicyFillStatus,
    alert_event_idempotency_key,
    canonical_sha256,
    outcome_event_idempotency_key,
)


def _card_set_path(service, card_set):
    folder = service.store.root / "cards" / card_set.decision_trade_date.isoformat() / card_set.card_set_id
    return next(folder.glob("card_set-*.json"))


def test_card_set_and_card_events_are_immutable_and_retry_safe(service_factory) -> None:
    service = service_factory()
    first = service.materialize()
    second = service.materialize()
    assert first["created"] is True
    assert second["created"] is False
    assert service.store.event_counts() == {"CARD_ISSUED": 2}
    date_root = service.store.root / "cards" / "2026-09-03"
    assert len(list(date_root.glob("*/card_set-*.json"))) == 1


def test_concurrent_first_materialization_publishes_one_card_set_and_one_event_per_card(service_factory) -> None:
    service = service_factory()
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: service.materialize(), range(4)))
    assert sum(1 for result in results if result["created"]) == 1
    assert len({result["card_set_artifact_sha256"] for result in results}) == 1
    assert service.store.event_counts() == {"CARD_ISSUED": 2}
    date_root = service.store.root / "cards" / "2026-09-03"
    assert len(list(date_root.glob("*/card_set-*.json"))) == 1


def test_conflicting_card_set_identity_is_rejected(service_factory) -> None:
    service = service_factory()
    card_set = service.materialize()["card_set"]
    conflicting = card_set.model_copy(
        update={
            "card_set_id": "ptset_conflicting",
            "semantic_identity_sha256": "f" * 64,
        }
    )
    with pytest.raises(CardSetIdentityConflict):
        service.store.publish_card_set(conflicting)


def test_intent_write_stays_inside_injected_timing_root(service_factory) -> None:
    service = service_factory()
    service.put_intent(
        raw_symbol="000001.SZ",
        planned_full_notional_cny=Decimal("100000"),
        desired_target_exposure=Decimal("1"),
    )
    paths = [path.relative_to(service.store.root).as_posix() for path in service.store.root.rglob("*") if path.is_file()]
    assert paths == ["intents/000001.SZ.json"]


def test_card_set_read_fails_closed_on_semantic_identity_tamper(service_factory) -> None:
    service = service_factory()
    card_set = service.materialize()["card_set"]
    path = _card_set_path(service, card_set)
    raw = path.read_text(encoding="utf-8")
    path.write_text(raw.replace(card_set.input_identity_sha256, "e" * 64), encoding="utf-8")
    with pytest.raises(PositionTimingArtifactError, match="input identity mismatch"):
        service.store.get_card_set(decision_trade_date=card_set.decision_trade_date)


def test_card_set_read_fails_closed_on_card_content_tamper(service_factory) -> None:
    service = service_factory()
    card_set = service.materialize()["card_set"]
    path = _card_set_path(service, card_set)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["cards"][0]["reason_codes"].append("TAMPERED")
    payload["cards_sha256"] = canonical_sha256(payload["cards"])
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(PositionTimingArtifactError, match="content-addressed filename mismatch"):
        service.store.get_card_set(decision_trade_date=card_set.decision_trade_date)


def test_intent_read_fails_closed_on_semantic_tamper(service_factory) -> None:
    service = service_factory()
    intent, _ = service.put_intent(
        raw_symbol="000001.SZ",
        planned_full_notional_cny=Decimal("100000"),
        desired_target_exposure=Decimal("1"),
    )
    path = service.store.root / "intents" / "000001.SZ.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["planned_full_notional_cny"] = "200000"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(PositionTimingArtifactError, match="intent semantic identity mismatch"):
        service.store.get_intent(intent.canonical_symbol)


def test_intent_read_fails_closed_on_timestamp_tamper(service_factory) -> None:
    service = service_factory()
    intent, _ = service.put_intent(
        raw_symbol="000001.SZ",
        planned_full_notional_cny=Decimal("100000"),
        desired_target_exposure=Decimal("1"),
    )
    path = service.store.root / "intents" / "000001.SZ.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["updated_at"] = "2026-09-03T16:00:01+08:00"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(PositionTimingArtifactError, match="intent semantic identity mismatch"):
        service.store.get_intent(intent.canonical_symbol)


def test_event_idempotency_is_global_across_month_partitions(service_factory) -> None:
    service = service_factory()
    service.materialize()
    event_path = service.store.root / "events" / "2026-09.jsonl"
    payload = json.loads(event_path.read_text(encoding="utf-8").splitlines()[0])
    payload["occurred_at"] = "2026-10-01T09:00:00+08:00"
    with pytest.raises(ImmutableArtifactConflict, match="conflicting payload"):
        service.store.append_event(payload, idempotency_key=payload["idempotency_key"])
    assert not (service.store.root / "events" / "2026-10.jsonl").exists()


def test_card_issued_log_fails_closed_when_frozen_l2_field_is_removed(service_factory) -> None:
    service = service_factory()
    service.materialize()
    event_path = service.store.root / "events" / "2026-09.jsonl"
    lines = event_path.read_text(encoding="utf-8").splitlines()
    payload = json.loads(lines[0])
    payload["event_payload"].pop("delist_context_status")
    lines[0] = json.dumps(payload, ensure_ascii=False)
    event_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with pytest.raises(PositionTimingArtifactError, match="invalid event contract"):
        service.store.event_counts()


def test_frozen_alert_event_contract_binds_composite_key_hashes_and_timezones() -> None:
    tz = ZoneInfo("Asia/Shanghai")
    card_id = "ptcard_example"
    trigger_id = "pttrg_example"
    event = AlertEmissionAuthorizedEventV1(
        event_id="evt_alert_example",
        idempotency_key=alert_event_idempotency_key(card_id, trigger_id),
        occurred_at=datetime(2026, 9, 4, 9, 31, tzinfo=tz),
        card_id=card_id,
        card_artifact_sha256="a" * 64,
        trigger_id=trigger_id,
        eligibility_identity="b" * 64,
        quote_price_raw=Decimal("10.01"),
        quote_open_raw=Decimal("9.98"),
        quote_observed_at=datetime(2026, 9, 4, 9, 30, 58, tzinfo=tz),
        alert_evaluated_at=datetime(2026, 9, 4, 9, 31, tzinfo=tz),
        quote_source="TDX_REALTIME_BATCH_QUOTE",
        staleness_state="FRESH",
        quote_age_seconds=Decimal("2"),
    )
    assert event.user_seen_evidence is False
    with pytest.raises(ValueError, match="idempotency_key"):
        AlertEmissionAuthorizedEventV1.model_validate({**event.model_dump(), "idempotency_key": "wrong"})


def test_frozen_outcome_event_contract_binds_horizon_and_maturity_clock() -> None:
    tz = ZoneInfo("Asia/Shanghai")
    card_id = "ptcard_example"
    base = {
        "event_id": "evt_outcome_example",
        "idempotency_key": outcome_event_idempotency_key(card_id, 20),
        "occurred_at": datetime(2026, 10, 12, 16, 0, tzinfo=tz),
        "card_id": card_id,
        "card_artifact_sha256": "a" * 64,
        "horizon_trading_days": 20,
        "policy_fill_status": PolicyFillStatus.FILLED,
        "maturity_status": MaturityStatus.MATURED,
        "selected_trigger_id": "pttrg_example",
        "planned_delta_qty": -100,
        "effective_target_exposure": Decimal("0.5"),
        "fill_price_raw": Decimal("10"),
        "fill_time_policy": "DAILY_OHLC_CONSERVATIVE_FILL_V1",
        "nominal_terminal_trade_date": date(2026, 10, 12),
        "effective_terminal_trade_date": date(2026, 10, 12),
        "deferred_trading_days": 0,
        "reason_codes": (),
        "candidate_path": {"status": "PAIRED"},
        "do_nothing_path": {"status": "PAIRED"},
        "candidate_net_value_cny": Decimal("1000"),
        "do_nothing_net_value_cny": Decimal("990"),
        "net_lift_bps": Decimal("10"),
        "dataset_identity_sha256": "b" * 64,
        "calendar_identity_sha256": "c" * 64,
        "limit_identity_sha256": "d" * 64,
        "board_lot_identity_sha256": "e" * 64,
        "adjustment_identity_sha256": "f" * 64,
        "cost_policy_sha256": "1" * 64,
    }
    OutcomeEvaluatedEventV1.model_validate(base)
    with pytest.raises(ValueError, match="idempotency_key"):
        OutcomeEvaluatedEventV1.model_validate({**base, "idempotency_key": "wrong"})
    with pytest.raises(ValueError, match="nominal terminal date"):
        OutcomeEvaluatedEventV1.model_validate(
            {**base, "effective_terminal_trade_date": date(2026, 10, 13)}
        )
