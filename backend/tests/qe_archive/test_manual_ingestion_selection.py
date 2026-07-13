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
    def __init__(self) -> None:
        self.global_loop_ref_calls = 0

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

    def list_loop_refs(self, **kwargs):  # type: ignore[no-untyped-def]
        self.global_loop_ref_calls += 1
        return [{"task_id": "unrelated_task", "loop_id": "unrelated_Loop1", "loop_index": 1}]


def test_build_candidates_returns_missing_loop_indices_in_preview_order() -> None:
    assembler = _FakeAssembler()
    service = QEArchiveBackfillService(assembler=assembler)  # type: ignore[arg-type]

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
    assert assembler.global_loop_ref_calls == 0


def test_build_candidates_does_not_fallback_to_global_when_scoped_task_has_no_matches() -> None:
    class EmptyScopedAssembler:
        def __init__(self) -> None:
            self.global_calls = 0

        def list_loop_refs_for_tasks(self, task_ids, *, status, include_archived):  # type: ignore[no-untyped-def]
            assert task_ids == ["task_1"]
            return []

        def list_loop_refs(self, **kwargs):  # type: ignore[no-untyped-def]
            self.global_calls += 1
            return [{"task_id": "unrelated_task", "loop_id": "unrelated_Loop1", "loop_index": 1}]

    assembler = EmptyScopedAssembler()
    service = QEArchiveBackfillService(assembler=assembler)  # type: ignore[arg-type]

    candidates = service._build_candidates(  # noqa: SLF001 - regression covers selected-scope isolation.
        QEArchiveBackfillOptions(source="loop", task_ids=["task_1"], status="completed", include_archived=False),
        source="loop",
    )

    assert candidates == []
    assert assembler.global_calls == 0
