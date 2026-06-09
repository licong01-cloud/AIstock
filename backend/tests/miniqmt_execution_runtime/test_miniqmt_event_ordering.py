from __future__ import annotations

from datetime import date

import pytest

from backend.services.miniqmt_execution_runtime import (
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeRecord,
)


def test_repository_rejects_non_monotonic_event_sequence() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    runtime = repo.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            runtime_id="mqrt_phase2_ordering",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase2_ordering",
        )
    )
    repo.append_event(
        MiniQMTExecutionEvent(
            runtime_id=runtime.runtime_id,
            sequence=1,
            event_type=MiniQMTExecutionEventType.RUNTIME_CREATED,
            source="runtime",
        )
    )

    with pytest.raises(ValueError, match="event sequence must be monotonic"):
        repo.append_event(
            MiniQMTExecutionEvent(
                runtime_id=runtime.runtime_id,
                sequence=3,
                event_type=MiniQMTExecutionEventType.TICK,
                source="gateway",
                payload={"symbol": "000001.SZ", "price": 10.2},
            )
        )


def test_next_event_sequence_tracks_persisted_append_only_events() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    runtime = repo.upsert_runtime(
        MiniQMTExecutionRuntimeRecord(
            runtime_id="mqrt_phase2_next_sequence",
            account_group_id="ag_minqmt_main_sim",
            trade_date=date(2026, 6, 9),
            runtime_config_hash="runtime_hash_phase2_next_sequence",
        )
    )

    assert repo.next_event_sequence(runtime.runtime_id) == 1
    repo.append_event(
        MiniQMTExecutionEvent(
            runtime_id=runtime.runtime_id,
            sequence=1,
            event_type=MiniQMTExecutionEventType.RUNTIME_CREATED,
            source="runtime",
        )
    )
    assert repo.next_event_sequence(runtime.runtime_id) == 2
    stored = repo.get_runtime(runtime.runtime_id)
    assert stored is not None
    assert stored.last_event_sequence == 1
