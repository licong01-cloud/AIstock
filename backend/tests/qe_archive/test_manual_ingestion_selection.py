from __future__ import annotations

from backend.services.qe_archive.backfill_service import QEArchiveBackfillOptions, QEArchiveBackfillService
from backend.services.qe_archive.source_assembler import _archive_status_from_policy, _loop_row_archive_status


def test_archive_status_from_policy_maps_design_states() -> None:
    base_payload = {"source_system": "qe", "config": {}}

    assert _archive_status_from_policy(
        source_type="loop",
        source_id="task_1",
        source_sub_id="loop_1",
        source_status="completed",
        payload=base_payload,
        run_ids=["run_1"],
    )["archive_status"] == "archived"

    auto = _archive_status_from_policy(
        source_type="loop",
        source_id="task_1",
        source_sub_id="loop_1",
        source_status="completed",
        payload=base_payload,
        run_ids=[],
    )
    assert auto["archive_status"] == "recommended"
    assert auto["eligible"] is True
    assert auto["recommended"] is True

    manual = _archive_status_from_policy(
        source_type="loop",
        source_id="task_1",
        source_sub_id="loop_2",
        source_status="completed",
        payload={"source_system": "qe", "config": {"archive_policy": "MANUAL_ONLY"}},
        run_ids=[],
    )
    assert manual["archive_status"] == "manual_only"
    assert manual["eligible"] is True
    assert manual["recommended"] is False

    skipped = _archive_status_from_policy(
        source_type="loop",
        source_id="task_1",
        source_sub_id="loop_3",
        source_status="completed",
        payload={"source_system": "qe", "config": {"archive_policy": "SKIP"}},
        run_ids=[],
    )
    assert skipped["archive_status"] == "skipped"
    assert skipped["eligible"] is False

    running = _archive_status_from_policy(
        source_type="loop",
        source_id="task_1",
        source_sub_id="loop_4",
        source_status="running",
        payload=base_payload,
        run_ids=[],
    )
    assert running["archive_status"] == "not_recommended"
    assert running["reason"] == "source_status:running"


def test_loop_row_archive_status_reads_archive_policy_from_config_json() -> None:
    status = _loop_row_archive_status(
        {
            "task_id": "task_1",
            "loop_id": "task_1_Loop1",
            "loop_index": 1,
            "status": "completed",
            "config_json": {"archive_policy": "MANUAL_ONLY", "archive_reason": "baseline_duplicate"},
        },
        run_ids=[],
    )

    assert status["archive_status"] == "manual_only"
    assert status["reason"] == "baseline_duplicate"


class _FakeAssembler:
    def list_loop_refs_for_task_indices(self, task_id, loop_indices, *, status, include_archived):
        assert task_id == "task_1"
        assert loop_indices == [1, 2, 3]
        assert status == "completed"
        assert include_archived is False
        return [
            {"task_id": "task_1", "loop_id": "task_1_Loop1", "loop_index": 1},
            {"task_id": "task_1", "loop_id": "task_1_Loop3", "loop_index": 3},
        ]

    def assemble_loop_payload(self, *, loop_id=None, task_id=None, loop_index=None):
        return {
            "source_system": "qe_evolution",
            "source_id": task_id or "task_1",
            "source_sub_id": loop_id,
            "task_id": task_id or "task_1",
            "loop_id": loop_id,
            "loop_index": loop_index,
            "status": "completed",
            "config": {},
        }


def test_build_candidates_returns_missing_loop_indices_in_preview_order() -> None:
    service = QEArchiveBackfillService(assembler=_FakeAssembler())  # type: ignore[arg-type]

    candidates = service._build_candidates(  # noqa: SLF001 - regression covers the selection expander.
        QEArchiveBackfillOptions(task_id="task_1", loop_indices=[1, 2, 3], status="completed", include_archived=False),
        source="loop",
    )

    assert [candidate["event_type"] for candidate in candidates] == [
        "qe.loop.completed",
        "qe.loop.completed",
        "qe.loop.missing",
    ]
    assert candidates[-1]["payload"]["loop_index"] == 2
    assert candidates[-1]["payload"]["missing_reason"] == "loop_not_found_or_filtered"
