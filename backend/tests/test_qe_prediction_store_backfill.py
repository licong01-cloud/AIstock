from __future__ import annotations

from pathlib import Path

from backend.services.qe_archive.backfill_service import (
    QEArchiveArtifactLinkBackfillOptions,
    QEArchiveBackfillService,
)
from scripts import qe_prediction_store_backfill as backfill


def test_backfill_plan_finds_pred_label_and_params(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "qe_task" / "Loop3" / "mlruns" / "exp" / "rec" / "artifacts"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "pred.pkl").write_bytes(b"pred")
    (artifact_dir / "label.pkl").write_bytes(b"label")
    (artifact_dir / "params.pkl").write_bytes(b"params")

    target = backfill.build_targets(
        [],
        workspace_root=tmp_path,
        task_ids=["qe_task"],
        loops=[3],
        run_key_template="{task_id}_L{loop_index}",
    )[0]
    plan = backfill.plan_target(target)

    assert plan.status == "ready"
    assert target.run_key == "qe_task_L3"
    assert set(plan.artifacts) == {"prediction", "label", "model_params"}


def test_backfill_plan_reports_missing_pred(tmp_path: Path) -> None:
    (tmp_path / "qe_task" / "Loop1" / "mlruns" / "exp" / "rec" / "artifacts").mkdir(parents=True)

    target = backfill.build_targets(
        [],
        workspace_root=tmp_path,
        task_ids=["qe_task"],
        loops=[1],
        run_key_template="{task_id}_L{loop_index}",
    )[0]
    plan = backfill.plan_target(target)

    assert plan.status == "missing"
    assert plan.error == "pred.pkl not found"


class _ArtifactCandidateRepository:
    rows = [
        {"run_id": "qear_run_1", "task_id": "task", "loop_index": 1},
        {"run_id": "qear_run_2", "task_id": "task", "loop_index": 2},
        {"run_id": "qear_run_3", "task_id": "task", "loop_index": 3},
    ]

    def list_prediction_artifact_link_candidates(
        self,
        *,
        run_ids=(),
        task_ids=(),
        after_run_id=None,
        limit=200,
    ):
        selected = [row for row in self.rows if not after_run_id or row["run_id"] > after_run_id]
        if run_ids:
            selected = [row for row in selected if row["run_id"] in run_ids]
        if task_ids:
            selected = [row for row in selected if row["task_id"] in task_ids]
        return selected[:limit]


class _ArtifactLinkService:
    def link_prediction_artifacts_for_run(self, run, *, dry_run, verify_sha256):
        assert verify_sha256 is True
        if run["run_id"] == "qear_run_2":
            return {
                **run,
                "resolution_status": "missing",
                "action_status": "missing",
                "artifact_count": 0,
                "written_count": 0,
                "errors": [],
                "dry_run": dry_run,
            }
        action = "would_link" if dry_run else ("linked" if run["run_id"] == "qear_run_1" else "already_linked")
        return {
            **run,
            "resolution_status": "available",
            "action_status": action,
            "artifact_count": 3,
            "written_count": 3 if action == "linked" else 0,
            "errors": [],
            "dry_run": dry_run,
        }


def test_archive_artifact_link_backfill_pages_and_reports_all_outcomes() -> None:
    service = QEArchiveBackfillService(
        assembler=object(),
        archive_service=_ArtifactLinkService(),
        multi_alpha_handler=object(),
        repository=_ArtifactCandidateRepository(),
    )

    summary = service.backfill_prediction_artifact_links(
        QEArchiveArtifactLinkBackfillOptions(
            task_ids=["task"],
            page_size=2,
            write=True,
        )
    )

    assert summary["status"] == "completed"
    assert summary["scanned"] == 3
    assert summary["linked"] == 1
    assert summary["already_linked"] == 1
    assert summary["missing"] == 1
    assert summary["artifact_rows_written"] == 3
    assert summary["exhausted"] is True
