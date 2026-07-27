"""BUG-861: recovery-orchestration adoption regressions.

A new bridge operation derives a new deterministic capture batch id while the
economic capture content is unchanged.  The repository correctly refuses a
second active batch for the same content, so ``_prepare_capture_batch`` must
resolve the persisted same-content recovery chain instead of dying on
``ADVISORY_PHASE1_CAPTURE_BATCH_CONFLICT``:

1. a new bridge operation adopts the unique exact-content PLANNED recovery
   successor;
2. the adopted batch id may differ from the id the new operation derived;
3. after adoption the repository-authoritative request/batch identity is
   used;
4. multiple active candidates are rejected;
5. plans/binding/scope/policy/lineage mismatches are rejected;
6. an active RUNNING successor with an unexpired lease is rejected;
7. a successor already carrying membership/receipt facts is rejected per the
   state contract, never silently overridden;
8. a terminally FAILED bridge operation is never reopened;
9. an exact retry creates no new batch;
10. the formal same-id capture and non-retrospective paths are unchanged.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from backend.services.advisory_historical_range.artifact_store import (
    HistoricalRangeArtifactStore,
)
from backend.services.advisory_historical_range.dataset_bridge import (
    HistoricalRangeBridgeResultStatus,
    HistoricalRangeDatasetBridgeApplicationService,
    HistoricalRangeDatasetBridgeError,
)
from backend.services.advisory_historical_range.dataset_bridge_postgres import (
    PostgresHistoricalRangeBridgeAdapters,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeContractError,
    HistoricalRangeOperationStatus,
    REASON_DATABASE_CAPACITY_EXHAUSTED,
)
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatch,
    CaptureBatchStatus,
    InMemoryCaptureBatchRepository,
)
from backend.tests.advisory_historical_range.test_r4_capture_readback import (
    _plan_variant,
)
from backend.tests.advisory_historical_range.test_r4_dataset_bridge import (
    _OperationBridge,
    _OperationRepository,
    _request,
)
from backend.tests.advisory_historical_range.test_r4_dataset_bridge_postgres import (
    _adapter_with_repository,
    _capture_request,
    _label_capture_request,
)

_BASE_TIME = datetime(2026, 7, 25, tzinfo=UTC)
_OBS_ID_PREFIX = "ahr_obs_cap"
_LBL_ID_PREFIX = "ahr_lbl_cap"


def _clock() -> dict[str, datetime]:
    return {"now": _BASE_TIME}


def _repository(clock: dict[str, datetime]) -> InMemoryCaptureBatchRepository:
    return InMemoryCaptureBatchRepository(now_provider=lambda: clock["now"])


def _rekeyed(request, new_id: str):
    """Same economic content under a new operation-derived batch id."""

    return PostgresHistoricalRangeBridgeAdapters._capture_recovery_request(
        request=request,
        capture_batch_id=new_id,
    )


def _expire_predecessor(
    repository: InMemoryCaptureBatchRepository,
    clock: dict[str, datetime],
    request,
) -> CaptureBatch:
    """Drive the original batch through the formal state machine to EXPIRED."""

    planned = repository.create(request)
    running = repository.acquire(
        capture_batch_id=request.capture_batch_id,
        expected_row_version=planned.row_version,
        lease_seconds=1,
    )
    clock["now"] += timedelta(seconds=2)
    expired = repository.expire(
        capture_batch_id=request.capture_batch_id,
        expected_row_version=running.row_version,
        fencing_token=running.fencing_token,
    )
    assert expired.status is CaptureBatchStatus.EXPIRED
    return expired


def _recover_successor(
    repository: InMemoryCaptureBatchRepository,
    request,
    predecessor: CaptureBatch,
    successor_id: str,
) -> CaptureBatch:
    """Formally recover a PLANNED successor through the repository contract."""

    successor = repository.recover(
        request=_rekeyed(request, successor_id),
        predecessor_capture_batch_id=predecessor.request.capture_batch_id,
        expected_predecessor_row_version=predecessor.row_version,
        predecessor_fencing_token=predecessor.fencing_token,
    )
    assert successor.status is CaptureBatchStatus.PLANNED
    return successor


def _expired_chain_with_planned_successor(
    clock: dict[str, datetime],
    *,
    successor_id: str = "ahr_obs_cap_legacy_successor",
):
    """Production state: EXPIRED root + formally recovered PLANNED successor."""

    repository = _repository(clock)
    request = _capture_request()
    expired = _expire_predecessor(repository, clock, request)
    successor = _recover_successor(repository, request, expired, successor_id)
    return repository, request, expired, successor


def test_new_operation_adopts_unique_exact_content_planned_successor() -> None:
    clock = _clock()
    repository, request, _, successor = _expired_chain_with_planned_successor(clock)
    adapter = _adapter_with_repository(repository)
    new_operation_request = _rekeyed(request, "ahr_obs_cap_new_operation_derived")

    adopted = adapter._prepare_capture_batch(
        request=new_operation_request,
        id_prefix=_OBS_ID_PREFIX,
    )

    # (1) the unique exact-content PLANNED successor is adopted.
    assert adopted.status is CaptureBatchStatus.PLANNED
    assert adopted.request.capture_batch_id == successor.request.capture_batch_id
    # (2) the adopted id differs from the id the new operation derived.
    assert adopted.request.capture_batch_id != new_operation_request.capture_batch_id
    # (3) the repository-authoritative identity is returned, not the caller's.
    persisted = repository.get(successor.request.capture_batch_id)
    assert adopted == persisted
    assert adopted.request == persisted.request
    assert adopted.request.capture_request_hash == request.capture_request_hash
    assert adopted.request.plans == request.plans
    assert adopted.request.binding.range_scope == request.binding.range_scope
    # No second active same-content batch was created.
    chain = repository.list_by_capture_request_hash(request.capture_request_hash)
    assert len(chain) == 2
    # (9) an exact retry adopts the same successor without creating batches.
    retry = adapter._prepare_capture_batch(
        request=new_operation_request,
        id_prefix=_OBS_ID_PREFIX,
    )
    assert retry == adopted
    assert (
        len(repository.list_by_capture_request_hash(request.capture_request_hash))
        == 2
    )


def test_new_operation_recovers_successor_when_chain_is_all_terminal() -> None:
    clock = _clock()
    repository = _repository(clock)
    request = _capture_request()
    expired = _expire_predecessor(repository, clock, request)
    adapter = _adapter_with_repository(repository)
    new_operation_request = _rekeyed(request, "ahr_obs_cap_new_operation_derived")

    recovered = adapter._prepare_capture_batch(
        request=new_operation_request,
        id_prefix=_OBS_ID_PREFIX,
    )

    assert recovered.status is CaptureBatchStatus.PLANNED
    assert recovered.capture_attempt_no == 2
    assert recovered.predecessor_capture_batch_id == expired.request.capture_batch_id
    assert recovered.request.capture_request_hash == request.capture_request_hash
    assert recovered.request.capture_batch_id != new_operation_request.capture_batch_id
    chain = repository.list_by_capture_request_hash(request.capture_request_hash)
    assert len(chain) == 2
    retry = adapter._prepare_capture_batch(
        request=new_operation_request,
        id_prefix=_OBS_ID_PREFIX,
    )
    assert retry == recovered
    assert (
        len(repository.list_by_capture_request_hash(request.capture_request_hash))
        == 2
    )


def test_new_operation_expires_and_recovers_lease_expired_running_successor() -> None:
    clock = _clock()
    repository, request, _, successor = _expired_chain_with_planned_successor(clock)
    running = repository.acquire(
        capture_batch_id=successor.request.capture_batch_id,
        expected_row_version=successor.row_version,
        lease_seconds=1,
    )
    clock["now"] += timedelta(seconds=2)
    assert running.lease_expires_at is not None
    assert running.lease_expires_at < clock["now"]
    adapter = _adapter_with_repository(repository)
    new_operation_request = _rekeyed(request, "ahr_obs_cap_new_operation_derived")

    recovered = adapter._prepare_capture_batch(
        request=new_operation_request,
        id_prefix=_OBS_ID_PREFIX,
    )

    # The lease-expired RUNNING successor went through the formal expire()
    # contract and the chain was extended exactly once.
    expired_successor = repository.get(successor.request.capture_batch_id)
    assert expired_successor.status is CaptureBatchStatus.EXPIRED
    assert recovered.status is CaptureBatchStatus.PLANNED
    assert recovered.capture_attempt_no == 3
    assert (
        recovered.predecessor_capture_batch_id == successor.request.capture_batch_id
    )
    assert recovered.request.capture_request_hash == request.capture_request_hash
    chain = repository.list_by_capture_request_hash(request.capture_request_hash)
    assert len(chain) == 3
    active = [
        item
        for item in chain
        if item.status in {CaptureBatchStatus.PLANNED, CaptureBatchStatus.RUNNING}
    ]
    assert [item.request.capture_batch_id for item in active] == [
        recovered.request.capture_batch_id
    ]


def test_new_operation_rejects_running_successor_with_active_lease() -> None:
    clock = _clock()
    repository, request, _, successor = _expired_chain_with_planned_successor(clock)
    # The lease must be unexpired under both the repository clock and the
    # wall clock the bridge uses for its RUNNING guard.
    repository.acquire(
        capture_batch_id=successor.request.capture_batch_id,
        expected_row_version=successor.row_version,
        lease_seconds=7 * 24 * 3600,
    )
    adapter = _adapter_with_repository(repository)
    new_operation_request = _rekeyed(request, "ahr_obs_cap_new_operation_derived")

    with pytest.raises(HistoricalRangeContractError) as error:
        adapter._prepare_capture_batch(
            request=new_operation_request,
            id_prefix=_OBS_ID_PREFIX,
        )
    assert error.value.reason_code == REASON_DATABASE_CAPACITY_EXHAUSTED
    # Fail-closed: the RUNNING successor is untouched and nothing was created.
    persisted = repository.get(successor.request.capture_batch_id)
    assert persisted.status is CaptureBatchStatus.RUNNING
    assert (
        len(repository.list_by_capture_request_hash(request.capture_request_hash))
        == 2
    )


def test_new_operation_rejects_multiple_active_candidates() -> None:
    clock = _clock()
    repository, request, _, successor = _expired_chain_with_planned_successor(clock)
    # A second active same-content batch cannot be produced through the formal
    # contracts; inject the anomalous state directly to prove the adoption
    # path stays fail-closed instead of picking one silently.
    rogue = CaptureBatch(
        request=_rekeyed(request, "ahr_obs_cap_rogue_active"),
        status=CaptureBatchStatus.PLANNED,
        row_version=1,
        fencing_token=1,
        capture_attempt_no=3,
        predecessor_capture_batch_id=successor.request.capture_batch_id,
        created_at=clock["now"],
        updated_at=clock["now"],
    )
    repository._batches[rogue.request.capture_batch_id] = rogue
    repository._by_request_hash[request.capture_request_hash].append(
        rogue.request.capture_batch_id
    )
    adapter = _adapter_with_repository(repository)
    new_operation_request = _rekeyed(request, "ahr_obs_cap_new_operation_derived")

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="multiple active batches|not in a terminal state",
    ):
        adapter._prepare_capture_batch(
            request=new_operation_request,
            id_prefix=_OBS_ID_PREFIX,
        )


@pytest.mark.parametrize("tamper", ("plans", "range_scope", "binding_fencing"))
def test_new_operation_rejects_payload_mismatch_in_chain(tamper: str) -> None:
    clock = _clock()
    repository, request, _, successor = _expired_chain_with_planned_successor(clock)
    plan = request.plans[0]
    if tamper == "plans":
        # Plans/lineage drift: the persisted successor carries a different
        # economic plan set under the same content hash claim.
        tampered_request = successor.request.model_copy(
            update={
                "plans": (
                    plan.model_copy(
                        update={"canonical_signal_id": "acs_tampered_signal"}
                    ),
                )
            }
        )
    elif tamper == "range_scope":
        tampered_request = successor.request.model_copy(
            update={
                "plans": (
                    plan.model_copy(
                        update={
                            "range_scope": plan.range_scope.model_copy(
                                update={"range_run_id": "run-tampered"}
                            )
                        }
                    ),
                )
            }
        )
    else:
        tampered_request = successor.request.model_copy(
            update={
                "binding": successor.request.binding.model_copy(
                    update={"capture_fencing_token": 99}
                )
            }
        )
    repository._batches[successor.request.capture_batch_id] = successor.model_copy(
        update={"request": tampered_request}
    )
    adapter = _adapter_with_repository(repository)
    new_operation_request = _rekeyed(request, "ahr_obs_cap_new_operation_derived")

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="member payload differs",
    ):
        adapter._prepare_capture_batch(
            request=new_operation_request,
            id_prefix=_OBS_ID_PREFIX,
        )


@pytest.mark.parametrize(
    "partial_facts",
    (
        {"membership_count": 1, "membership_hash": "a" * 64},
        {"capture_receipt_hash": "b" * 64},
    ),
)
def test_new_operation_rejects_successor_with_partial_capture_facts(
    partial_facts: dict[str, object],
) -> None:
    clock = _clock()
    repository, request, _, successor = _expired_chain_with_planned_successor(clock)
    # The CaptureBatch model contract forbids this state; construct it through
    # model_copy (no revalidation) to prove the adoption path enforces the
    # contract instead of silently overriding the anomalous successor.
    repository._batches[successor.request.capture_batch_id] = successor.model_copy(
        update=partial_facts
    )
    adapter = _adapter_with_repository(repository)
    new_operation_request = _rekeyed(request, "ahr_obs_cap_new_operation_derived")

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="partial capture facts",
    ):
        adapter._prepare_capture_batch(
            request=new_operation_request,
            id_prefix=_OBS_ID_PREFIX,
        )


def test_failed_bridge_operation_stays_terminal_and_is_never_reopened(
    tmp_path: Path,
) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "failed-terminal-artifacts")
    repository = _OperationRepository()
    bridge = _OperationBridge(
        store=store,
        effects=(
            HistoricalRangeDatasetBridgeError(
                "R4_INJECTED_TERMINAL_FAILURE",
                "injected terminal failure",
            ),
        ),
    )
    service = HistoricalRangeDatasetBridgeApplicationService(
        repository=repository,
        artifact_store=store,
        bridge_service=bridge,
    )
    request = _request()

    failed, failed_ref = service.build_until_stable_boundary(
        request=request,
        resolved_request_hash="8" * 64,
        worker_id="worker-1",
    )
    retry, retry_ref = service.build_until_stable_boundary(
        request=request,
        resolved_request_hash="8" * 64,
        worker_id="worker-2",
    )

    assert failed.result_status is HistoricalRangeBridgeResultStatus.FAILED
    assert retry == failed
    assert retry_ref == failed_ref
    # The terminally FAILED operation returned its durable receipt; the bridge
    # was never claimed or reopened.
    assert bridge.calls == 1
    assert repository.operation["status"] == HistoricalRangeOperationStatus.FAILED.value
    assert [item.status for item in repository.attempts] == ["FAILED"]


def test_exact_retry_after_adoption_and_completion_creates_no_new_batch() -> None:
    clock = _clock()
    repository, request, _, successor = _expired_chain_with_planned_successor(clock)
    adapter = _adapter_with_repository(repository)
    new_operation_request = _rekeyed(request, "ahr_obs_cap_new_operation_derived")
    adopted = adapter._prepare_capture_batch(
        request=new_operation_request,
        id_prefix=_OBS_ID_PREFIX,
    )
    running = repository.acquire(
        capture_batch_id=adopted.request.capture_batch_id,
        expected_row_version=adopted.row_version,
        lease_seconds=60,
    )
    completed = repository.complete(
        capture_batch_id=adopted.request.capture_batch_id,
        expected_row_version=running.row_version,
        fencing_token=running.fencing_token,
    )
    assert completed.status is CaptureBatchStatus.COMPLETE

    first = adapter._prepare_capture_batch(
        request=new_operation_request,
        id_prefix=_OBS_ID_PREFIX,
    )
    second = adapter._prepare_capture_batch(
        request=new_operation_request,
        id_prefix=_OBS_ID_PREFIX,
    )

    assert first == second == completed
    assert first.status is CaptureBatchStatus.COMPLETE
    assert first.request.capture_batch_id == successor.request.capture_batch_id
    assert first.membership_count == 0
    assert first.membership_hash is not None
    assert first.capture_receipt_hash is not None
    assert (
        len(repository.list_by_capture_request_hash(request.capture_request_hash))
        == 2
    )


def test_same_id_and_same_content_create_stays_idempotent() -> None:
    clock = _clock()
    repository = _repository(clock)
    adapter = _adapter_with_repository(repository)
    request = _capture_request()

    first = adapter._prepare_capture_batch(request=request, id_prefix=_OBS_ID_PREFIX)
    second = adapter._prepare_capture_batch(request=request, id_prefix=_OBS_ID_PREFIX)

    # The formal same-id path is unchanged: idempotent create, no adoption.
    assert first == second
    assert first.status is CaptureBatchStatus.PLANNED
    assert first.request.capture_batch_id == request.capture_batch_id
    assert (
        len(repository.list_by_capture_request_hash(request.capture_request_hash))
        == 1
    )


def test_same_id_with_different_content_fails_closed_without_adoption() -> None:
    clock = _clock()
    repository = _repository(clock)
    adapter = _adapter_with_repository(repository)
    request = _capture_request()
    repository.create(request)
    # Rebuild through model validation so the content hash is recomputed;
    # model_copy would keep a stale hash and never exercise the conflict.
    conflicting_plan = _plan_variant(
        request.plans[0],
        decision=date(2026, 7, 3),
        target=date(2026, 7, 6),
    )
    request_payload = request.model_dump(
        mode="python",
        exclude={"capture_request_hash"},
    )
    request_payload["plans"] = (conflicting_plan,)
    conflicting = type(request).model_validate(request_payload)
    assert conflicting.capture_batch_id == request.capture_batch_id
    assert conflicting.capture_request_hash != request.capture_request_hash

    # Same batch id with different content has no persisted recovery chain for
    # its content hash; the bridge must fail closed, never adopt the original.
    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="no persisted recovery chain",
    ):
        adapter._prepare_capture_batch(request=conflicting, id_prefix=_OBS_ID_PREFIX)


def test_label_capture_adopts_unique_exact_content_planned_successor() -> None:
    clock = _clock()
    repository = _repository(clock)
    request = _label_capture_request()
    expired = _expire_predecessor(repository, clock, request)
    successor = _recover_successor(
        repository,
        request,
        expired,
        "ahr_lbl_cap_legacy_successor",
    )
    adapter = _adapter_with_repository(repository)
    new_operation_request = _rekeyed(request, "ahr_lbl_cap_new_operation_derived")

    adopted = adapter._prepare_capture_batch(
        request=new_operation_request,
        id_prefix=_LBL_ID_PREFIX,
    )

    assert adopted.status is CaptureBatchStatus.PLANNED
    assert adopted.request.capture_batch_id == successor.request.capture_batch_id
    assert adopted.request.capture_batch_id != new_operation_request.capture_batch_id
    assert adopted.request.capture_request_hash == request.capture_request_hash
    assert adopted.request.selector_policy_hash == request.selector_policy_hash
    assert (
        adopted.request.historical_range_policy_bundle_ref
        == request.historical_range_policy_bundle_ref
    )
    assert (
        adopted.request.source_observation_capture_batch_id
        == request.source_observation_capture_batch_id
    )
    assert (
        len(repository.list_by_capture_request_hash(request.capture_request_hash))
        == 2
    )
