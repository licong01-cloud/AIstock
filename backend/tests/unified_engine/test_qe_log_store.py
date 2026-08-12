from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.quantevolver.qe_log_store import (
    QE_LIVE_LOG_FILE_COUNT,
    QELiveLogStore,
)


def test_live_log_store_uses_exactly_five_bounded_slots(tmp_path: Path) -> None:
    store = QELiveLogStore(tmp_path, max_file_bytes=320)
    for index in range(40):
        store.append(
            {
                "task_id": "task-1",
                "node_id": "node-1",
                "source_cursor": f"source-{index}",
                "broker_seq": index,
                "payload": {"logs": [f"line-{index}"]},
            }
        )

    paths = store.slot_paths()
    assert len(paths) == QE_LIVE_LOG_FILE_COUNT
    assert [path.name for path in paths] == [f"qe-live-{index}.jsonl" for index in range(5)]
    assert all(path.exists() for path in paths)
    assert all(path.stat().st_size <= 320 for path in paths)
    assert sorted(path.name for path in tmp_path.iterdir()) == sorted(path.name for path in paths)


def test_live_log_store_tail_is_task_filtered_and_read_only_when_empty(tmp_path: Path) -> None:
    store = QELiveLogStore(tmp_path, max_file_bytes=4096)
    assert store.read_task_tail("missing", tail=10)["logs"] == []
    assert list(tmp_path.iterdir()) == []

    store.append({"task_id": "other", "payload": {"logs": ["ignore"]}})
    store.append({"task_id": "task-1", "payload": {"logs": ["one", "two"]}})
    store.append({"task_id": "task-1", "payload": {"logs": ["three"]}})

    result = store.read_task_tail("task-1", tail=2)
    assert result["logs"] == ["two", "three"]
    assert result["source"] == "qe_live_log_ring"


def test_live_log_store_rejects_sixth_slot_or_oversized_record(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exactly five"):
        QELiveLogStore(tmp_path, file_count=6)

    store = QELiveLogStore(tmp_path, max_file_bytes=64)
    with pytest.raises(ValueError, match="exceeds"):
        store.append({"task_id": "task-1", "payload": {"logs": ["x" * 100]}})
