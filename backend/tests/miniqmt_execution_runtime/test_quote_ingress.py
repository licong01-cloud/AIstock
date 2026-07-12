from __future__ import annotations

import ast
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

import pytest

import backend.infra.realtime_quote_subscriber as subscriber_module
from backend.execution_algos.adaptive_is.contracts import QuoteSourceMethod
from backend.execution_algos.adaptive_is.reasons import QuoteContractError, QuoteContractReasonCode, QuoteContractStage
from backend.infra.realtime_quote_subscriber import PhaseOneQuoteDelivery, RealtimeQuoteSubscriber
from backend.miniqmt_quote_contract_config import QuoteIngressRuntimeConfig
from backend.services.miniqmt_execution_runtime.quote_ingress import (
    PhaseOneRawQuoteSnapshotStore,
    QuoteIngressSupervisor,
    QuoteIngressWorker,
    ReservedSymbolMailbox,
)
from backend.services.miniqmt_execution_runtime.quote_normalizer import capture_raw_quote_frame


class _FakeXtData:
    def __init__(self) -> None:
        self.next_sequence = 200
        self.callbacks: dict[int, Any] = {}
        self.subscribe_calls: list[int] = []
        self.unsubscribe_calls: list[int] = []

    def subscribe_whole_quote(self, code_list, callback):  # noqa: ANN001
        self.next_sequence += 1
        self.callbacks[self.next_sequence] = callback
        self.subscribe_calls.append(self.next_sequence)
        return self.next_sequence

    def unsubscribe_quote(self, sequence: int) -> None:
        self.unsubscribe_calls.append(sequence)

    def run(self) -> None:
        return None

    def emit_last(self, payload: Mapping[str, Any]) -> None:
        self.callbacks[self.subscribe_calls[-1]](payload)


@pytest.fixture
def fake_xtdata(monkeypatch: pytest.MonkeyPatch) -> _FakeXtData:
    fake = _FakeXtData()
    monkeypatch.setattr(subscriber_module, "_load_xtdata", lambda: fake)
    return fake


def _config(**overrides: object) -> QuoteIngressRuntimeConfig:
    values: dict[str, object] = {
        "enabled": True,
        "owner_mode": "simulation_scheduler",
        "max_symbols": 4,
        "drain_budget": 4,
        "heartbeat_timeout_ms": 100,
        "restart_backoff_ms": 1,
        "restart_max_backoff_ms": 8,
        "loud_interval_seconds": 1,
        "evidence_outbox_max_events": 8,
        "evidence_flush_batch_size": 4,
        "restart_max_attempts": 2,
    }
    values.update(overrides)
    return QuoteIngressRuntimeConfig(**values)  # type: ignore[arg-type]


def _payload(last_price: float) -> dict[str, object]:
    return {
        "time": "09300000",
        "lastPrice": last_price,
        "preClose": 9.8,
        "bidPrice": [last_price - 0.01],
        "askPrice": [last_price + 0.01],
        "bidVol": [100],
        "askVol": [100],
        "volume": 1000,
        "amount": 10000,
    }


def _frame(*, generation: int, sequence: int, last_price: float) -> object:
    return capture_raw_quote_frame(
        _payload(last_price),
        callback_symbol="000001.SZ",
        source_session_id="test-session",
        ingress_generation=generation,
        ingress_sequence=sequence,
        received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        received_monotonic_ns=sequence + 1,
        clock_domain_id="test-clock",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )


def _wait_until(predicate, *, timeout_seconds: float = 1.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("timed out waiting for quote ingress worker")


def test_reserved_symbol_mailbox_coalesces_per_symbol_without_dropping_admitted_symbols() -> None:
    mailbox = ReservedSymbolMailbox(max_symbols=2)
    mailbox.admit(("000001.SZ", "000002.SZ"))
    mailbox.activate_generation(3)

    assert mailbox.offer(_frame(generation=3, sequence=1, last_price=10.0)) == "ACCEPTED"
    assert mailbox.offer(_frame(generation=3, sequence=2, last_price=10.1)) == "COALESCED"
    frames = mailbox.wait_and_drain(budget=2, timeout_seconds=0)

    assert len(frames) == 1
    assert frames[0].ingress_sequence == 2
    assert mailbox.telemetry()["coalesced_count"] == 1
    with pytest.raises(QuoteContractError) as unexpected:
        mailbox.offer(
            capture_raw_quote_frame(
                _payload(10.0),
                callback_symbol="000003.SZ",
                source_session_id="test-session",
                ingress_generation=3,
                ingress_sequence=3,
                received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
                received_monotonic_ns=4,
                clock_domain_id="test-clock",
                source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
            )
        )
    assert unexpected.value.reason_code == QuoteContractReasonCode.UNEXPECTED_SYMBOL


def test_reserved_symbol_mailbox_rejects_new_capacity_and_fences_old_generations() -> None:
    mailbox = ReservedSymbolMailbox(max_symbols=1)
    mailbox.admit(("000001.SZ",))
    with pytest.raises(QuoteContractError) as capacity:
        mailbox.admit(("000002.SZ",))
    assert capacity.value.reason_code == QuoteContractReasonCode.CAPACITY_EXCEEDED

    mailbox.activate_generation(2)
    assert mailbox.offer(_frame(generation=1, sequence=1, last_price=10.0)) == "STALE_GENERATION"
    assert mailbox.offer(_frame(generation=2, sequence=2, last_price=10.0)) == "ACCEPTED"
    assert mailbox.offer(_frame(generation=2, sequence=2, last_price=10.1)) == "ORDERING_REJECTED"
    mailbox.fence_generation(2)
    assert mailbox.offer(_frame(generation=2, sequence=3, last_price=10.2)) == "STALE_GENERATION"


def test_mailbox_and_snapshot_store_reject_invalid_lifecycle_inputs_without_fallback() -> None:
    with pytest.raises(QuoteContractError):
        ReservedSymbolMailbox(max_symbols=0)
    mailbox = ReservedSymbolMailbox(max_symbols=1)
    with pytest.raises(QuoteContractError) as empty_symbols:
        mailbox.admit(())
    assert empty_symbols.value.reason_code == QuoteContractReasonCode.SYMBOL_INVALID
    mailbox.admit(("000001.SZ",))
    mailbox.activate_generation(2)
    with pytest.raises(QuoteContractError) as old_generation:
        mailbox.activate_generation(1)
    assert old_generation.value.reason_code == QuoteContractReasonCode.ORDERING_REJECTED
    with pytest.raises(ValueError, match="budget"):
        mailbox.wait_and_drain(budget=0, timeout_seconds=0)

    store = PhaseOneRawQuoteSnapshotStore(max_symbols=1)
    store.replace_admitted(("000001.SZ",))
    newer = _frame(generation=2, sequence=2, last_price=10.2)
    store.update(newer)  # type: ignore[arg-type]
    store.update(_frame(generation=2, sequence=1, last_price=10.1))  # type: ignore[arg-type]
    assert store.get("000001.SZ") == newer


def test_bootstrap_callback_race_keeps_the_newer_callback_after_generation_publish(fake_xtdata: _FakeXtData) -> None:
    callback_price = 10.2

    def bootstrap(symbols: list[str]) -> Mapping[str, Mapping[str, Any]]:
        fake_xtdata.emit_last({"000001.SZ": _payload(callback_price)})
        return {symbol: _payload(10.0) for symbol in symbols}

    supervisor = QuoteIngressSupervisor(
        subscriber=RealtimeQuoteSubscriber(),
        config=_config(),
        data_session_key="SIM:quote-session",
        owner="scheduler-owner-A",
        bootstrap_fetcher=bootstrap,
    )
    supervisor.acquire_consumer(consumer_id="quote-observer", symbols=["000001.SZ"])

    _wait_until(lambda: supervisor.snapshot_store.get("000001.SZ") is not None)
    frame = supervisor.snapshot_store.get("000001.SZ")
    assert frame is not None
    assert frame.source_method is QuoteSourceMethod.WHOLE_QUOTE_CALLBACK
    assert frame.ingress_sequence == 1
    assert frame.whitelisted_raw_fields["lastPrice"] == callback_price
    health = supervisor.health()
    assert health["subscription"]["bootstrap_coverage_ratio"] == 1.0
    assert health["subscription"]["callback_total"] == 1
    assert health["writer"]["writer_heartbeat_age_ms"] is not None
    supervisor.shutdown()


def test_capacity_rejects_new_consumer_without_disturbing_existing_admission(fake_xtdata: _FakeXtData) -> None:
    supervisor = QuoteIngressSupervisor(
        subscriber=RealtimeQuoteSubscriber(),
        config=_config(max_symbols=1),
        data_session_key="SIM:quote-session",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])
    before = supervisor.health()

    with pytest.raises(QuoteContractError) as exc_info:
        supervisor.acquire_consumer(consumer_id="second", symbols=["000002.SZ"])
    assert exc_info.value.reason_code == QuoteContractReasonCode.CAPACITY_EXCEEDED
    after = supervisor.health()
    assert after["subscription"]["generation"] == before["subscription"]["generation"]
    assert after["subscription"]["symbols"] == ["000001.SZ"]
    supervisor.shutdown()


def test_writer_failure_is_fenced_and_requests_bounded_generation_rebuild() -> None:
    failures: list[QuoteContractError] = []
    worker = QuoteIngressWorker(
        consumer_id="writer-test",
        config=_config(),
        frame_sink=lambda _frame: (_ for _ in ()).throw(RuntimeError("sink failure")),
        loud_sink=failures.append,
    )
    worker.admit_symbols(("000001.SZ",))
    worker.on_generation_published("SIM:quote-session", 1)
    delivery = PhaseOneQuoteDelivery(
        data_session_key="SIM:quote-session",
        lease_id="lease",
        owner="scheduler-owner-A",
        consumer_id="writer-test",
        symbol="000001.SZ",
        payload=_payload(10.0),
        generation=1,
        ingress_sequence=1,
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
        received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
        received_monotonic_ns=1,
    )
    worker.capture_delivery(delivery, source_session_id="session", clock_domain_id="clock")

    _wait_until(lambda: worker.health()["last_failure"] is not None)
    request = worker.watchdog()

    assert request is not None
    assert request.generation == 1
    assert worker.health()["status"] == "RESTART_PENDING"
    assert failures[-1].reason_code == QuoteContractReasonCode.CONSUMER_FAILURE
    worker.shutdown()


def test_worker_preserves_pending_raw_frame_until_publish_and_rejects_fenced_republish() -> None:
    captured: list[object] = []
    worker = QuoteIngressWorker(
        consumer_id="pending-capture-test",
        config=_config(),
        frame_sink=captured.append,
    )
    worker.admit_symbols(("000001.SZ",))
    worker.ingest_frame(_frame(generation=2, sequence=1, last_price=10.0))  # type: ignore[arg-type]
    assert captured == []
    worker.on_generation_published("SIM:quote-session", 2)
    _wait_until(lambda: len(captured) == 1)
    worker.on_generation_fenced("SIM:quote-session", 2)
    worker.on_generation_published("SIM:quote-session", 2)
    assert worker.health()["last_failure"]["reason_code"] == QuoteContractReasonCode.ORDERING_REJECTED.value
    worker.shutdown()


def test_worker_reaches_restart_cap_then_a_later_lifecycle_epoch_can_retry() -> None:
    worker = QuoteIngressWorker(
        consumer_id="restart-cap-test",
        config=_config(restart_backoff_ms=1, restart_max_backoff_ms=2, restart_max_attempts=2),
        frame_sink=lambda _frame: (_ for _ in ()).throw(RuntimeError("sink failure")),
    )
    worker.admit_symbols(("000001.SZ",))
    worker.on_generation_published("SIM:quote-session", 1)
    worker.capture_delivery(
        PhaseOneQuoteDelivery(
            data_session_key="SIM:quote-session",
            lease_id="lease",
            owner="scheduler-owner-A",
            consumer_id="restart-cap-test",
            symbol="000001.SZ",
            payload=_payload(10.0),
            generation=1,
            ingress_sequence=1,
            source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
            received_at_utc=datetime(2026, 7, 12, 1, 30, tzinfo=UTC),
            received_monotonic_ns=1,
        ),
        source_session_id="session",
        clock_domain_id="clock",
    )
    _wait_until(lambda: worker.health()["last_failure"] is not None)
    first = worker.watchdog(now_monotonic_ns=time.monotonic_ns())
    assert first is not None
    second = worker.watchdog(now_monotonic_ns=time.monotonic_ns() + 10_000_000)
    assert second is not None
    assert worker.watchdog(now_monotonic_ns=time.monotonic_ns() + 20_000_000) is None
    assert worker.health()["status"] == "FAILED"
    worker.begin_lifecycle_epoch()
    assert worker.health()["status"] == "DEGRADED"
    worker.on_generation_fenced("SIM:quote-session", 1)
    assert worker.health()["status"] == "FENCED"
    worker.shutdown()


def test_loud_rate_limit_preserves_failure_first_last_and_occurrence_count() -> None:
    failures: list[QuoteContractError] = []
    worker = QuoteIngressWorker(
        consumer_id="loud-test",
        config=_config(loud_interval_seconds=60),
        frame_sink=lambda _frame: None,
        loud_sink=failures.append,
    )
    error = QuoteContractError(
        reason_code=QuoteContractReasonCode.PAYLOAD_INVALID,
        stage=QuoteContractStage.INGRESS,
        message="bad payload",
        context={"symbol": "000001.SZ", "generation": 1},
    )

    worker.record_loud_failure(error)
    worker.record_loud_failure(error)

    last_failure = worker.health()["last_failure"]
    assert last_failure["occurrence_count"] == 2
    assert last_failure["first_observed_at"] <= last_failure["last_observed_at"]
    assert failures == [error]


def test_supervisor_is_default_off_and_release_isolated_from_the_subscriber(fake_xtdata: _FakeXtData) -> None:
    with pytest.raises(QuoteContractError) as disabled:
        QuoteIngressSupervisor(
            subscriber=RealtimeQuoteSubscriber(),
            config=QuoteIngressRuntimeConfig.from_mapping({}),
            data_session_key="SIM:disabled",
            owner="scheduler-owner-A",
            bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
        )
    assert disabled.value.reason_code == QuoteContractReasonCode.SUBSCRIPTION_UNAVAILABLE

    subscriber = RealtimeQuoteSubscriber()
    supervisor = QuoteIngressSupervisor(
        subscriber=subscriber,
        config=_config(),
        data_session_key="SIM:quote-session",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    lease = supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])
    with pytest.raises(QuoteContractError) as duplicate:
        supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])
    assert duplicate.value.reason_code == QuoteContractReasonCode.LEASE_REBUILD_FAILED
    assert supervisor.release_consumer(consumer_id="first") is True
    assert supervisor.release_consumer(consumer_id="first") is False
    assert subscriber.phase_one_health(data_session_key="SIM:quote-session")["status"] == "INACTIVE"
    assert lease.status == "ACTIVE"
    assert supervisor.watchdog_tick()["consumers"] == {}
    supervisor.shutdown()


def test_invalid_bootstrap_raw_frame_is_loud_and_never_published(fake_xtdata: _FakeXtData) -> None:
    supervisor = QuoteIngressSupervisor(
        subscriber=RealtimeQuoteSubscriber(),
        config=_config(),
        data_session_key="SIM:invalid-bootstrap",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {
            symbol: {"time": object(), "lastPrice": object()} for symbol in symbols
        },
    )

    with pytest.raises(QuoteContractError) as exc_info:
        supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])

    assert exc_info.value.reason_code == QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE
    health = supervisor.health()
    assert health["subscription"]["status"] == "INACTIVE"
    assert health["subscription"]["bootstrap_coverage_ratio"] == 0.0
    assert supervisor.snapshot_store.snapshot() == {}
    supervisor.shutdown()


def test_multiple_logical_consumers_share_one_writer_and_release_purges_snapshot(fake_xtdata: _FakeXtData) -> None:
    supervisor = QuoteIngressSupervisor(
        subscriber=RealtimeQuoteSubscriber(),
        config=_config(max_symbols=2),
        data_session_key="SIM:single-writer",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])
    supervisor.acquire_consumer(consumer_id="second", symbols=["000002.SZ"])
    _wait_until(lambda: len(supervisor.snapshot_store.snapshot()) == 2)

    health = supervisor.health()
    assert set(health["consumers"]) == {"first", "second"}
    assert health["writer"]["admitted_symbols"] == ["000001.SZ", "000002.SZ"]
    assert health["writer"]["ordering_rejected_count"] == 0

    assert supervisor.release_consumer(consumer_id="second") is True
    assert set(supervisor.snapshot_store.snapshot()) == {"000001.SZ"}
    assert supervisor.health()["writer"]["admitted_symbols"] == ["000001.SZ"]
    supervisor.shutdown()


def test_consumer_callback_failure_fences_writer_and_supervisor_rebuilds_generation(fake_xtdata: _FakeXtData) -> None:
    supervisor = QuoteIngressSupervisor(
        subscriber=RealtimeQuoteSubscriber(),
        config=_config(restart_backoff_ms=1, restart_max_backoff_ms=1),
        data_session_key="SIM:consumer-recovery",
        owner="scheduler-owner-A",
        bootstrap_fetcher=lambda symbols: {symbol: _payload(10.0) for symbol in symbols},
    )
    supervisor.acquire_consumer(consumer_id="first", symbols=["000001.SZ"])
    initial_generation = supervisor.health()["subscription"]["generation"]
    fake_xtdata.emit_last({"000001.SZ": {"time": "bad", "lastPrice": object()}})
    _wait_until(lambda: supervisor.health()["writer"]["status"] == "FAILED")
    _wait_until(lambda: not supervisor.health()["writer"]["thread_alive"])

    health = supervisor.watchdog_tick()

    assert health["subscription"]["generation"] == initial_generation + 1
    assert health["writer"]["status"] in {"STARTING", "ACTIVE"}
    supervisor.shutdown()


def test_stale_alive_writer_is_epoch_fenced_and_requests_rebuild() -> None:
    captured: list[object] = []
    worker = QuoteIngressWorker(
        consumer_id="heartbeat-stale",
        config=_config(heartbeat_timeout_ms=10, restart_backoff_ms=1, restart_max_backoff_ms=1),
        frame_sink=captured.append,
    )
    worker.admit_symbols(("000001.SZ",))
    assert worker.on_generation_published("SIM:heartbeat", 1) is True
    _wait_until(lambda: worker.health()["writer_heartbeat_age_ms"] is not None)
    now = time.monotonic_ns() + 100_000_000

    request = worker.watchdog(now_monotonic_ns=now)

    assert request is not None
    assert request.generation == 1
    assert worker.health()["status"] == "RESTART_PENDING"
    _wait_until(lambda: not worker.health()["thread_alive"])
    assert worker.on_generation_published("SIM:heartbeat", 2) is True
    worker.shutdown()


def test_quote_ingress_isolation_has_no_database_broker_or_strategy_imports() -> None:
    tree = ast.parse(Path("backend/services/miniqmt_execution_runtime/quote_ingress.py").read_text(encoding="utf-8"))
    modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]

    assert not any(module.startswith("backend.db") for module in modules)
    assert not any("qmt_client" in module or "execution_runtime.client" in module for module in modules)
    assert not any("strategy" in module for module in modules)
