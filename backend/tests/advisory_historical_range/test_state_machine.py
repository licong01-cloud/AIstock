from __future__ import annotations

import pytest

from backend.services.advisory_historical_range.models import (
    BATCH_TRANSITIONS,
    DAY_TRANSITIONS,
    OPERATION_TRANSITIONS,
    OUTCOME_TRANSITIONS,
    PROGRAM_TRANSITIONS,
    HistoricalRangeBatchStatus,
    HistoricalRangeContractError,
    HistoricalRangeDayStatus,
    HistoricalRangeOperationStatus,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeProgramStatus,
    require_batch_transition,
    require_state_transition,
)


@pytest.mark.parametrize(
    ("current", "target", "mapping", "entity"),
    [
        (HistoricalRangeBatchStatus.QUEUED, HistoricalRangeBatchStatus.RUNNING, BATCH_TRANSITIONS, "batch"),
        (HistoricalRangeProgramStatus.PARTIAL, HistoricalRangeProgramStatus.RUNNING, PROGRAM_TRANSITIONS, "run"),
        (
            HistoricalRangeDayStatus.RETRYABLE_FAILED,
            HistoricalRangeDayStatus.WAITING_PREVIOUS_DAY,
            DAY_TRANSITIONS,
            "day",
        ),
        (HistoricalRangeOutcomeStatus.NOT_DUE, HistoricalRangeOutcomeStatus.COMPLETE, OUTCOME_TRANSITIONS, "outcome"),
        (
            HistoricalRangeOperationStatus.RETRYABLE_FAILED,
            HistoricalRangeOperationStatus.RUNNING,
            OPERATION_TRANSITIONS,
            "operation",
        ),
    ],
)
def test_approved_transitions_are_reachable(current, target, mapping, entity) -> None:  # type: ignore[no-untyped-def]
    require_state_transition(current, target, mapping, entity=entity)


@pytest.mark.parametrize(
    ("current", "target", "mapping", "entity"),
    [
        (HistoricalRangeBatchStatus.COMPLETED, HistoricalRangeBatchStatus.RUNNING, BATCH_TRANSITIONS, "batch"),
        (HistoricalRangeProgramStatus.FAILED, HistoricalRangeProgramStatus.RUNNING, PROGRAM_TRANSITIONS, "run"),
        (HistoricalRangeDayStatus.COMPLETE, HistoricalRangeDayStatus.RUNNING, DAY_TRANSITIONS, "day"),
        (HistoricalRangeOutcomeStatus.COMPLETE, HistoricalRangeOutcomeStatus.MATURING, OUTCOME_TRANSITIONS, "outcome"),
        (
            HistoricalRangeOperationStatus.QUEUED,
            HistoricalRangeOperationStatus.COMPLETED,
            OPERATION_TRANSITIONS,
            "operation",
        ),
    ],
)
def test_illegal_or_terminal_rewrites_fail_explicitly(current, target, mapping, entity) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(HistoricalRangeContractError) as exc_info:
        require_state_transition(current, target, mapping, entity=entity)
    assert exc_info.value.reason_code == "ADVISORY_HISTORICAL_RANGE_STATE_TRANSITION_INVALID"


def test_partial_cannot_be_rewritten_failed_after_any_success() -> None:
    with pytest.raises(HistoricalRangeContractError):
        require_batch_transition(
            HistoricalRangeBatchStatus.PARTIAL,
            HistoricalRangeBatchStatus.FAILED,
            successful_day_count=1,
            program_count=2,
            failed_program_count=2,
            recoverable_program_count=0,
        )


def test_failed_requires_every_program_terminal_and_no_recoverable_program() -> None:
    with pytest.raises(HistoricalRangeContractError):
        require_batch_transition(
            HistoricalRangeBatchStatus.RUNNING,
            HistoricalRangeBatchStatus.FAILED,
            successful_day_count=0,
            program_count=2,
            failed_program_count=1,
            recoverable_program_count=1,
        )

    require_batch_transition(
        HistoricalRangeBatchStatus.RUNNING,
        HistoricalRangeBatchStatus.FAILED,
        successful_day_count=0,
        program_count=2,
        failed_program_count=2,
        recoverable_program_count=0,
    )
