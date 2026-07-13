from __future__ import annotations

from pathlib import Path

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
