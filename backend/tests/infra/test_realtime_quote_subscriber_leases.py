from __future__ import annotations

from collections.abc import Callable, Mapping
import threading
from typing import Any

import pytest

import backend.infra.realtime_quote_subscriber as subscriber_module
from backend.execution_algos.adaptive_is.reasons import QuoteContractError, QuoteContractReasonCode
from backend.infra.realtime_quote_subscriber import (
    PhaseOneLeaseCallbacks,
    PhaseOneQuoteDelivery,
    RealtimeQuoteSubscriber,
)


class _FakeXtData:
    def __init__(self) -> None:
        self.next_sequence = 100
        self.subscribe_calls: list[dict[str, Any]] = []
        self.unsubscribe_calls: list[int] = []
        self.callbacks: dict[int, Callable[[Mapping[str, Any]], None]] = {}

    def subscribe_whole_quote(self, code_list, callback):  # noqa: ANN001
        self.next_sequence += 1
        sequence = self.next_sequence
        self.subscribe_calls.append({"sequence": sequence, "symbols": list(code_list), "callback": callback})
        self.callbacks[sequence] = callback
        return sequence

    def unsubscribe_quote(self, sequence: int) -> None:
        self.unsubscribe_calls.append(sequence)

    def run(self) -> None:
        return None

    def emit(self, sequence: int, payload: Mapping[str, Any]) -> None:
        self.callbacks[sequence](payload)


class _Recorder:
    def __init__(self) -> None:
        self.deliveries: list[PhaseOneQuoteDelivery] = []
        self.published: list[int] = []
        self.fenced: list[int] = []
        self.failures: list[QuoteContractError] = []

    def callbacks(self) -> PhaseOneLeaseCallbacks:
        return PhaseOneLeaseCallbacks(
            on_quote=self._on_quote,
            on_generation_prepared=lambda _session, _generation: True,
            on_generation_published=self._on_generation_published,
            on_generation_fenced=lambda _session, generation: self.fenced.append(generation),
            on_loud_failure=self.failures.append,
        )

    def _on_quote(self, delivery: PhaseOneQuoteDelivery) -> bool:
        self.deliveries.append(delivery)
        return True

    def _on_generation_published(self, _session: str, generation: int) -> None:
        self.published.append(generation)


@pytest.fixture
def fake_xtdata(monkeypatch: pytest.MonkeyPatch) -> _FakeXtData:
    fake = _FakeXtData()
    monkeypatch.setattr(subscriber_module, "_load_xtdata", lambda: fake)
    return fake


def _bootstrap(symbols: list[str]) -> Mapping[str, Mapping[str, Any]]:
    return {symbol: {"lastPrice": 10.0, "time": "09300000"} for symbol in symbols}


def test_phase_one_leases_are_independently_released_and_never_touch_legacy_maps(fake_xtdata: _FakeXtData) -> None:
    subscriber = RealtimeQuoteSubscriber()
    first = _Recorder()
    second = _Recorder()

    lease_one = subscriber.acquire_phase_one_lease(
        data_session_key="session-A",
        owner="scheduler-A",
        consumer_id="consumer-one",
        symbols=["000001.SZ"],
        callbacks=first.callbacks(),
        bootstrap_fetcher=_bootstrap,
        max_symbols=4,
    )
    lease_two = subscriber.acquire_phase_one_lease(
        data_session_key="session-A",
        owner="scheduler-A",
        consumer_id="consumer-two",
        symbols=["000002.SZ"],
        callbacks=second.callbacks(),
        bootstrap_fetcher=_bootstrap,
        max_symbols=4,
    )

    assert lease_one.status == "ACTIVE"
    assert lease_two.status == "ACTIVE"
    assert [call["symbols"] for call in fake_xtdata.subscribe_calls] == [["000001.SZ"], ["000001.SZ", "000002.SZ"]]
    assert fake_xtdata.unsubscribe_calls == [101]
    assert subscriber.subscriptions == {}
    assert subscriber.callbacks == {}
    assert subscriber.managed_subscriptions == {}

    assert subscriber.release_phase_one_lease(data_session_key="session-A", lease_id=lease_two.lease_id, max_symbols=4)
    health = subscriber.phase_one_health(data_session_key="session-A")
    assert health["symbols"] == ["000001.SZ"]
    assert health["lease_count"] == 1
    assert health["bootstrap_coverage_ratio"] == 1.0
    assert health["callback_total"] == 0
    assert second.fenced
    assert first.published[-1] == health["generation"]
    subscriber.shutdown_phase_one_leases(data_session_key="session-A")


def test_same_feed_lease_reuses_physical_subscription_then_rebuilds_and_fences_only_that_consumer(
    fake_xtdata: _FakeXtData,
) -> None:
    subscriber = RealtimeQuoteSubscriber()
    first = _Recorder()
    second = _Recorder()
    lease_one = subscriber.acquire_phase_one_lease(
        data_session_key="session-same-feed",
        owner="scheduler-A",
        consumer_id="consumer-one",
        symbols=["000001.SZ"],
        callbacks=first.callbacks(),
        bootstrap_fetcher=_bootstrap,
        max_symbols=4,
    )
    lease_two = subscriber.acquire_phase_one_lease(
        data_session_key="session-same-feed",
        owner="scheduler-A",
        consumer_id="consumer-two",
        symbols=["000001.SZ"],
        callbacks=second.callbacks(),
        bootstrap_fetcher=_bootstrap,
        max_symbols=4,
    )

    assert len(fake_xtdata.subscribe_calls) == 1
    assert lease_two.generation == lease_one.generation
    assert subscriber.release_phase_one_lease(data_session_key="session-same-feed", lease_id=lease_two.lease_id, max_symbols=4)
    assert second.fenced == [lease_two.generation]
    assert subscriber.release_phase_one_lease(data_session_key="session-same-feed", lease_id="missing", max_symbols=4) is False
    rebuilt_generation = subscriber.rebuild_phase_one_leases(data_session_key="session-same-feed", owner="scheduler-A", max_symbols=4)
    assert rebuilt_generation == lease_one.generation + 1
    subscriber.shutdown_phase_one_leases(data_session_key="session-same-feed")
    assert subscriber.phase_one_health(data_session_key="session-same-feed")["status"] == "INACTIVE"


def test_phase_one_replacement_failure_preserves_the_old_feed_and_owner_conflict_is_loud(fake_xtdata: _FakeXtData) -> None:
    subscriber = RealtimeQuoteSubscriber()
    first = _Recorder()
    subscriber.acquire_phase_one_lease(
        data_session_key="session-A",
        owner="scheduler-A",
        consumer_id="consumer-one",
        symbols=["000001.SZ"],
        callbacks=first.callbacks(),
        bootstrap_fetcher=_bootstrap,
        max_symbols=4,
    )
    before = subscriber.phase_one_health(data_session_key="session-A")

    with pytest.raises(QuoteContractError) as exc_info:
        subscriber.acquire_phase_one_lease(
            data_session_key="session-A",
            owner="scheduler-A",
            consumer_id="consumer-two",
            symbols=["000002.SZ"],
            callbacks=_Recorder().callbacks(),
            bootstrap_fetcher=lambda _symbols: {"000001.SZ": {"lastPrice": 10.0}},
            max_symbols=4,
        )
    assert exc_info.value.reason_code == QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE
    after = subscriber.phase_one_health(data_session_key="session-A")
    assert after["generation"] == before["generation"]
    assert after["symbols"] == before["symbols"]
    assert fake_xtdata.unsubscribe_calls == [102]

    with pytest.raises(QuoteContractError) as owner_exc:
        subscriber.acquire_phase_one_lease(
            data_session_key="session-A",
            owner="scheduler-B",
            consumer_id="consumer-other-owner",
            symbols=["000001.SZ"],
            callbacks=_Recorder().callbacks(),
            bootstrap_fetcher=_bootstrap,
            max_symbols=4,
        )
    assert owner_exc.value.reason_code == QuoteContractReasonCode.OWNER_CONFLICT

    second_subscriber = RealtimeQuoteSubscriber()
    with pytest.raises(QuoteContractError) as process_owner_exc:
        second_subscriber.acquire_phase_one_lease(
            data_session_key="session-A",
            owner="scheduler-A",
            consumer_id="consumer-second-instance",
            symbols=["000001.SZ"],
            callbacks=_Recorder().callbacks(),
            bootstrap_fetcher=_bootstrap,
            max_symbols=4,
        )
    assert process_owner_exc.value.reason_code == QuoteContractReasonCode.OWNER_CONFLICT
    assert (
        subscriber.phase_one_health(data_session_key="session-A")["process_last_failure"]["reason_code"]
        == QuoteContractReasonCode.OWNER_CONFLICT.value
    )
    subscriber.shutdown_phase_one_leases(data_session_key="session-A")


def test_stale_generation_callback_is_fenced_after_successful_replacement(fake_xtdata: _FakeXtData, caplog: pytest.LogCaptureFixture) -> None:
    subscriber = RealtimeQuoteSubscriber()
    first = _Recorder()
    subscriber.acquire_phase_one_lease(
        data_session_key="session-A",
        owner="scheduler-A",
        consumer_id="consumer-one",
        symbols=["000001.SZ"],
        callbacks=first.callbacks(),
        bootstrap_fetcher=_bootstrap,
        max_symbols=4,
    )
    subscriber.acquire_phase_one_lease(
        data_session_key="session-A",
        owner="scheduler-A",
        consumer_id="consumer-two",
        symbols=["000002.SZ"],
        callbacks=_Recorder().callbacks(),
        bootstrap_fetcher=_bootstrap,
        max_symbols=4,
    )

    fake_xtdata.emit(101, {"000001.SZ": {"lastPrice": 9.0}})

    assert any("STALE_GENERATION" in record.message for record in caplog.records)
    assert all(delivery.generation != 1 or delivery.source_method.value == "BOOTSTRAP_FULL_TICK" for delivery in first.deliveries)
    subscriber.shutdown_phase_one_leases(data_session_key="session-A")


def test_phase_one_capacity_rejects_only_the_new_union_and_keeps_existing_feed(fake_xtdata: _FakeXtData) -> None:
    subscriber = RealtimeQuoteSubscriber()
    subscriber.acquire_phase_one_lease(
        data_session_key="session-A",
        owner="scheduler-A",
        consumer_id="consumer-one",
        symbols=["000001.SZ"],
        callbacks=_Recorder().callbacks(),
        bootstrap_fetcher=_bootstrap,
        max_symbols=1,
    )
    before = subscriber.phase_one_health(data_session_key="session-A")

    with pytest.raises(QuoteContractError) as exc_info:
        subscriber.acquire_phase_one_lease(
            data_session_key="session-A",
            owner="scheduler-A",
            consumer_id="consumer-two",
            symbols=["000002.SZ"],
            callbacks=_Recorder().callbacks(),
            bootstrap_fetcher=_bootstrap,
            max_symbols=1,
        )
    assert exc_info.value.reason_code == QuoteContractReasonCode.CAPACITY_EXCEEDED
    after = subscriber.phase_one_health(data_session_key="session-A")
    assert after["generation"] == before["generation"]
    assert after["symbols"] == before["symbols"]
    assert after["capacity_rejected_total"] == before["capacity_rejected_total"] + 1
    subscriber.shutdown_phase_one_leases(data_session_key="session-A")


def test_bootstrap_capture_rejection_never_publishes_or_reports_false_coverage(fake_xtdata: _FakeXtData) -> None:
    subscriber = RealtimeQuoteSubscriber()
    recorder = _Recorder()

    def reject_capture(_delivery: PhaseOneQuoteDelivery) -> bool:
        return False

    callbacks = PhaseOneLeaseCallbacks(
        on_quote=reject_capture,
        on_generation_prepared=lambda _session, _generation: True,
        on_generation_published=recorder._on_generation_published,
        on_generation_fenced=lambda _session, generation: recorder.fenced.append(generation),
        on_loud_failure=recorder.failures.append,
    )
    with pytest.raises(QuoteContractError) as exc_info:
        subscriber.acquire_phase_one_lease(
            data_session_key="session-bootstrap-ack",
            owner="scheduler-A",
            consumer_id="rejecting-consumer",
            symbols=["000001.SZ"],
            callbacks=callbacks,
            bootstrap_fetcher=_bootstrap,
            max_symbols=4,
        )

    assert exc_info.value.reason_code == QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE
    health = subscriber.phase_one_health(data_session_key="session-bootstrap-ack")
    assert health["status"] == "INACTIVE"
    assert health["bootstrap_coverage_ratio"] == 0.0
    assert recorder.published == []
    assert fake_xtdata.unsubscribe_calls == [101]


def test_generation_publication_requires_explicit_ack_and_rolls_back_feed(fake_xtdata: _FakeXtData) -> None:
    subscriber = RealtimeQuoteSubscriber()
    recorder = _Recorder()
    callbacks = PhaseOneLeaseCallbacks(
        on_quote=recorder._on_quote,
        on_generation_prepared=lambda _session, _generation: False,
        on_generation_published=recorder._on_generation_published,
        on_generation_fenced=lambda _session, generation: recorder.fenced.append(generation),
        on_loud_failure=recorder.failures.append,
    )

    with pytest.raises(QuoteContractError) as exc_info:
        subscriber.acquire_phase_one_lease(
            data_session_key="session-publication-ack",
            owner="scheduler-A",
            consumer_id="rejecting-consumer",
            symbols=["000001.SZ"],
            callbacks=callbacks,
            bootstrap_fetcher=_bootstrap,
            max_symbols=4,
        )

    assert exc_info.value.reason_code == QuoteContractReasonCode.CONSUMER_FAILURE
    assert subscriber.phase_one_health(data_session_key="session-publication-ack")["status"] == "INACTIVE"
    assert recorder.failures[-1].reason_code == QuoteContractReasonCode.CONSUMER_FAILURE


def test_release_replacement_failure_is_atomic_and_preserves_released_lease(fake_xtdata: _FakeXtData) -> None:
    subscriber = RealtimeQuoteSubscriber()
    first = _Recorder()
    second = _Recorder()
    fail_bootstrap = False

    def mutable_bootstrap(symbols: list[str]) -> Mapping[str, Mapping[str, Any]]:
        if fail_bootstrap:
            return {}
        return _bootstrap(symbols)

    subscriber.acquire_phase_one_lease(
        data_session_key="session-release-atomic",
        owner="scheduler-A",
        consumer_id="first",
        symbols=["000001.SZ"],
        callbacks=first.callbacks(),
        bootstrap_fetcher=mutable_bootstrap,
        max_symbols=4,
    )
    second_lease = subscriber.acquire_phase_one_lease(
        data_session_key="session-release-atomic",
        owner="scheduler-A",
        consumer_id="second",
        symbols=["000002.SZ"],
        callbacks=second.callbacks(),
        bootstrap_fetcher=mutable_bootstrap,
        max_symbols=4,
    )
    fail_bootstrap = True

    with pytest.raises(QuoteContractError) as exc_info:
        subscriber.release_phase_one_lease(
            data_session_key="session-release-atomic",
            lease_id=second_lease.lease_id,
            max_symbols=4,
        )

    assert exc_info.value.reason_code == QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE
    health = subscriber.phase_one_health(data_session_key="session-release-atomic")
    assert health["lease_count"] == 2
    assert health["symbols"] == ["000001.SZ", "000002.SZ"]
    assert second.fenced == []
    subscriber.shutdown_phase_one_leases(data_session_key="session-release-atomic")


def test_same_session_acquire_and_release_are_serialized_without_lease_resurrection(fake_xtdata: _FakeXtData) -> None:
    subscriber = RealtimeQuoteSubscriber()
    first = _Recorder()
    second = _Recorder()
    bootstrap_started = threading.Event()
    allow_bootstrap = threading.Event()
    block_bootstrap = False

    def blocking_bootstrap(symbols: list[str]) -> Mapping[str, Mapping[str, Any]]:
        if block_bootstrap:
            bootstrap_started.set()
            assert allow_bootstrap.wait(timeout=2)
        return _bootstrap(symbols)

    first_lease = subscriber.acquire_phase_one_lease(
        data_session_key="session-concurrent",
        owner="scheduler-A",
        consumer_id="first",
        symbols=["000001.SZ"],
        callbacks=first.callbacks(),
        bootstrap_fetcher=blocking_bootstrap,
        max_symbols=4,
    )
    block_bootstrap = True

    acquired: list[object] = []
    released: list[bool] = []
    acquire_thread = threading.Thread(
        target=lambda: acquired.append(
            subscriber.acquire_phase_one_lease(
                data_session_key="session-concurrent",
                owner="scheduler-A",
                consumer_id="second",
                symbols=["000001.SZ"],
                callbacks=second.callbacks(),
                bootstrap_fetcher=blocking_bootstrap,
                max_symbols=4,
            )
        )
    )
    release_thread = threading.Thread(
        target=lambda: released.append(
            subscriber.release_phase_one_lease(
                data_session_key="session-concurrent",
                lease_id=first_lease.lease_id,
                max_symbols=4,
            )
        )
    )
    acquire_thread.start()
    assert bootstrap_started.wait(timeout=2)
    release_thread.start()
    assert release_thread.is_alive()
    allow_bootstrap.set()
    acquire_thread.join(timeout=2)
    release_thread.join(timeout=2)

    assert len(acquired) == 1
    assert released == [True]
    health = subscriber.phase_one_health(data_session_key="session-concurrent")
    assert health["lease_count"] == 1
    assert health["symbols"] == ["000001.SZ"]
    subscriber.shutdown_phase_one_leases(data_session_key="session-concurrent")


def test_shutdown_waits_for_inflight_bootstrap_then_leaves_no_resurrected_feed(fake_xtdata: _FakeXtData) -> None:
    subscriber = RealtimeQuoteSubscriber()
    recorder = _Recorder()
    bootstrap_started = threading.Event()
    allow_bootstrap = threading.Event()
    acquired: list[object] = []

    def blocking_bootstrap(symbols: list[str]) -> Mapping[str, Mapping[str, Any]]:
        bootstrap_started.set()
        assert allow_bootstrap.wait(timeout=2)
        return _bootstrap(symbols)

    acquire_thread = threading.Thread(
        target=lambda: acquired.append(
            subscriber.acquire_phase_one_lease(
                data_session_key="session-shutdown-race",
                owner="scheduler-A",
                consumer_id="first",
                symbols=["000001.SZ"],
                callbacks=recorder.callbacks(),
                bootstrap_fetcher=blocking_bootstrap,
                max_symbols=4,
            )
        )
    )
    shutdown_thread = threading.Thread(
        target=lambda: subscriber.shutdown_phase_one_leases(data_session_key="session-shutdown-race")
    )
    acquire_thread.start()
    assert bootstrap_started.wait(timeout=2)
    shutdown_thread.start()
    assert shutdown_thread.is_alive()
    allow_bootstrap.set()
    acquire_thread.join(timeout=2)
    shutdown_thread.join(timeout=2)

    assert len(acquired) == 1
    assert subscriber.phase_one_health(data_session_key="session-shutdown-race")["status"] == "INACTIVE"
    assert recorder.fenced == [1]
