
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from backend.services.research_pipeline.hmm_backtest_recorder import (
    BACKTEST_RECORDING_STAGE,
    HMMBacktestRecorder,
    stable_json_hash,
)
from backend.services.research_pipeline.models import BackfillRunRecord, BacktestRecord, StageAttemptRecord, StagePlanRecord


class FakeRecorderRepository:
    def __init__(self) -> None:
        self.records: dict[str, dict[str, Any]] = {}
        self.backfill_runs: dict[str, dict[str, Any]] = {}
        self.stages: dict[tuple[str, str], dict[str, Any]] = {}
        self.attempts: list[dict[str, Any]] = []
        self.artifacts: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []

    def upsert_backtest_record(self, record: BacktestRecord) -> dict[str, Any]:
        row = record.model_dump()
        existing = self.records.get(row["record_key_sha256"])
        if existing:
            existing.update(row)
            return existing
        self.records[row["record_key_sha256"]] = row
        return row

    def list_backtest_records(self, experiment_id: str, **kwargs: Any) -> list[dict[str, Any]]:
        rows = [row for row in self.records.values() if row["experiment_id"] == experiment_id]
        offset = int(kwargs.get("offset") or 0)
        limit = int(kwargs.get("limit") or 100)
        return rows[offset : offset + limit]

    def create_backfill_run(self, record: BackfillRunRecord) -> dict[str, Any]:
        row = record.model_dump()
        self.backfill_runs[row["backfill_run_id"]] = row
        return row

    def update_backfill_run(self, backfill_run_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        self.backfill_runs[backfill_run_id].update(updates)
        return self.backfill_runs[backfill_run_id]

    def get_backfill_run(self, backfill_run_id: str) -> dict[str, Any] | None:
        return self.backfill_runs.get(backfill_run_id)

    def get_stage_plan(self, experiment_id: str, stage_name: str) -> dict[str, Any] | None:
        return self.stages.get((experiment_id, stage_name))

    def list_stage_plans(self, experiment_id: str) -> list[dict[str, Any]]:
        return [row for (exp_id, _), row in self.stages.items() if exp_id == experiment_id]

    def create_stage_plan(self, record: StagePlanRecord) -> dict[str, Any]:
        row = record.model_dump()
        self.stages[(row["experiment_id"], row["stage_name"])] = row
        return row

    def next_attempt_no(self, experiment_id: str, stage_name: str) -> int:
        attempts = [row for row in self.attempts if row["experiment_id"] == experiment_id and row["stage_name"] == stage_name]
        return max([row["attempt_no"] for row in attempts], default=0) + 1

    def create_stage_attempt(self, record: StageAttemptRecord) -> dict[str, Any]:
        row = record.model_dump()
        self.attempts.append(row)
        return row

    def update_stage_attempt(self, stage_attempt_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        for row in self.attempts:
            if row["stage_attempt_id"] == stage_attempt_id:
                row.update(updates)
                return row
        raise ValueError(stage_attempt_id)

    def update_stage_plan(self, stage_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        for row in self.stages.values():
            if row["stage_id"] == stage_id:
                row.update(updates)
                return row
        raise ValueError(stage_id)

    def create_artifact_ref(self, record: Any) -> dict[str, Any]:
        row = record.model_dump()
        self.artifacts.append(row)
        return row

    def create_pipeline_event(self, record: Any) -> dict[str, Any]:
        row = record.model_dump()
        self.events.append(row)
        return row


def _historical_file(tmp_path: Path) -> Path:
    payload = {
        "selected_representatives": [
            {
                "task_id": "task_a",
                "loop_id": "loop_a_1",
                "loop_index": 1,
                "metrics": {"ann": 0.45, "mdd": -0.12, "ir": 1.8, "ic": 0.04, "rank_ic": 0.06},
                "config_summary": {
                    "model_id": "model_a",
                    "strategy_id": "score_weighted_topk_v2",
                    "execution_algo": "V25_TWO_STAGE",
                    "stock_pool": "zz500",
                    "label_horizon": 10,
                    "factor_sig": "factor_family_a",
                    "factor_count": 57,
                },
                "hmm_config_summary": {"hmm_model_version_id": "hmm_v1", "hmm_signal_preset": "risk_on"},
                "archive_family_sig": "archive_family_a",
                "strict_family_sig": "strict_a_1",
            }
        ],
        "duplicate_rejected_loops": [
            {
                "task_id": "task_a",
                "loop_id": "loop_a_2",
                "loop_index": 2,
                "metrics": {"ann": 0.38, "mdd": -0.16, "ir": 1.4, "ic": 0.03, "rank_ic": 0.05},
                "config_summary": {
                    "model_id": "model_a",
                    "strategy_id": "score_weighted_topk_v2",
                    "execution_algo": "V25_TWO_STAGE",
                    "stock_pool": "zz500",
                    "label_horizon": 10,
                    "factor_sig": "factor_family_a",
                    "factor_count": 57,
                },
                "hmm_config_summary": {"hmm_model_version_id": "hmm_v2", "hmm_signal_preset": "risk_off"},
                "archive_family_sig": "archive_family_a",
                "strict_family_sig": "strict_a_2",
            }
        ],
        "excluded_loops": [{"task_id": "task_noise", "reason": "not_hmm"}],
    }
    path = tmp_path / "hmm_history.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_stable_signature_and_record_key_are_deterministic() -> None:
    recorder = HMMBacktestRecorder(FakeRecorderRepository())
    payload_a = {"b": [2, 1], "a": {"x": 1}}
    payload_b = {"a": {"x": 1}, "b": [2, 1]}

    assert stable_json_hash(payload_a) == stable_json_hash(payload_b)
    assert recorder.build_record_key(
        experiment_id="rp_exp_1",
        source_type="historical_file",
        source_task_id="task_a",
        source_loop_id="loop_a_1",
        source_loop_index=1,
    ) == recorder.build_record_key(
        experiment_id="rp_exp_1",
        source_type="historical_file",
        source_task_id="task_a",
        source_loop_id="loop_a_1",
        source_loop_index=1,
    )


def test_historical_record_normalization_classifies_representative_and_hmm_variant() -> None:
    recorder = HMMBacktestRecorder(FakeRecorderRepository())
    representative = recorder.normalize_historical_record(
        {
            "task_id": "task_a",
            "loop_id": "loop_1",
            "loop_index": 1,
            "metrics": {"annualized_return": 0.2, "max_drawdown": -0.1, "information_ratio": 1.2},
            "config_summary": {"model_id": "m1", "factor_sig": "f1"},
            "hmm_config_summary": {"hmm_model_version_id": "h1"},
        },
        experiment_id="rp_exp_1",
        selected_representative=True,
    )
    variant = recorder.normalize_historical_record(
        {
            "task_id": "task_a",
            "loop_id": "loop_2",
            "loop_index": 2,
            "metrics": {"ann": 0.18},
            "config_summary": {"model_id": "m1", "factor_sig": "f1"},
            "hmm_config_summary": {"hmm_model_version_id": "h2"},
        },
        experiment_id="rp_exp_1",
        duplicate=True,
    )

    assert representative.qe_archive_representative is True
    assert representative.dedup_status == "primary"
    assert representative.ann == 0.2
    assert representative.mdd == -0.1
    assert variant.dedup_status == "hmm_variant"
    assert variant.rejection_reason == "hmm_only_config_sweep_preserved_in_research_pipeline"

    duplicate_without_hmm = recorder.normalize_historical_record(
        {
            "task_id": "task_a",
            "loop_id": "loop_3",
            "loop_index": 3,
            "metrics": {"ann": 0.12},
            "config_summary": {"model_id": "m1", "factor_sig": "f1"},
        },
        experiment_id="rp_exp_1",
        duplicate=True,
    )
    assert duplicate_without_hmm.dedup_status == "duplicate_same_config"


def test_preview_and_execute_are_idempotent(tmp_path: Path) -> None:
    repo = FakeRecorderRepository()
    recorder = HMMBacktestRecorder(repo)
    source_file = _historical_file(tmp_path)
    payload = {"source_scope": {"source_file": str(source_file)}, "dry_run": False}

    preview = recorder.create_backfill_preview("rp_exp_1", payload, require_enabled=False)
    assert preview["counts"]["candidate_count"] == 3
    assert preview["counts"]["research_timeline_count"] == 2
    assert preview["counts"]["qe_archive_representative_count"] == 1
    assert len(repo.records) == 0

    first = recorder.execute_backfill("rp_exp_1", payload, require_enabled=False)
    assert first["status"] == "completed"
    assert first["counts"]["inserted"] == 2
    assert first["counts"]["excluded"] == 1
    assert len(repo.records) == 2
    assert repo.get_stage_plan("rp_exp_1", BACKTEST_RECORDING_STAGE)["status"] == "passed"

    second = recorder.execute_backfill("rp_exp_1", payload, require_enabled=False)
    assert second["status"] == "completed"
    assert second["counts"]["inserted"] == 0
    assert second["counts"]["updated"] == 2
    assert len(repo.records) == 2


def test_backfill_feature_flags_are_closed_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = HMMBacktestRecorder(FakeRecorderRepository())
    source_file = _historical_file(tmp_path)
    monkeypatch.delenv("RESEARCH_PIPELINE_HMM_BACKFILL_ENABLED", raising=False)

    with pytest.raises(ValueError, match="HMM backfill preview disabled"):
        recorder.create_backfill_preview("rp_exp_1", {"source_scope": {"source_file": str(source_file)}})



def test_execute_can_use_preview_id_source_scope_and_records_audit_artifacts(tmp_path: Path) -> None:
    repo = FakeRecorderRepository()
    recorder = HMMBacktestRecorder(repo)
    source_file = _historical_file(tmp_path)
    preview = recorder.create_backfill_preview(
        "rp_exp_1",
        {"source_scope": {"source_file": str(source_file)}},
        require_enabled=False,
    )

    result = recorder.execute_backfill(
        "rp_exp_1",
        {"preview_id": preview["preview_id"], "dry_run": False},
        require_enabled=False,
    )

    assert result["status"] == "completed"
    assert result["counts"]["inserted"] == 2
    assert len(repo.artifacts) == 1
    assert repo.artifacts[0]["domain_type"] == "file"
    assert repo.artifacts[0]["artifact_uri"] == str(source_file.resolve())
    assert repo.events[-1]["event_type"] == "hmm_backtest_backfill_completed"


def test_execute_dry_run_does_not_write_records_or_stage_attempt(tmp_path: Path) -> None:
    repo = FakeRecorderRepository()
    recorder = HMMBacktestRecorder(repo)
    source_file = _historical_file(tmp_path)

    result = recorder.execute_backfill(
        "rp_exp_1",
        {"source_scope": {"source_file": str(source_file)}, "dry_run": True},
        require_enabled=False,
    )

    assert result["status"] == "previewed"
    assert result["counts"]["would_insert"] == 2
    assert repo.records == {}
    assert repo.attempts == []
    assert repo.artifacts == []
    assert repo.events == []
