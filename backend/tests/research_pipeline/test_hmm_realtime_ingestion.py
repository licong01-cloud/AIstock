from __future__ import annotations

import inspect
from typing import Any

from backend.services.quantevolver.qe_evolution_service import AutoEvolutionScheduler
from backend.services.research_pipeline import realtime_ingestion as realtime_module
from backend.services.research_pipeline.models import BacktestRecord
from backend.services.research_pipeline.realtime_ingestion import (
    ResearchPipelineRealtimeIngestion,
    safe_record_hmm_backtest_completed,
)


class FakeRecorderRepository:
    def __init__(self) -> None:
        self.records: list[BacktestRecord] = []

    def upsert_backtest_record(self, record: BacktestRecord) -> dict[str, Any]:
        self.records.append(record)
        return record.model_dump()


class FakeRecorder:
    def __init__(self) -> None:
        self._repo = FakeRecorderRepository()

    def normalize_historical_record(self, payload: dict[str, Any], *, experiment_id: str, **kwargs: Any) -> BacktestRecord:
        return BacktestRecord(
            experiment_id=experiment_id,
            source_task_id=str(payload["task_id"]),
            source_loop_id=str(payload["loop_id"]),
            source_loop_index=payload.get("loop_index"),
            source_experiment_id=payload.get("experiment_id"),
            record_key_sha256="record-key-1",
            non_hmm_config_sig="non-hmm-sig",
            hmm_config_sig="hmm-sig",
            metrics_json=payload.get("metrics") or {},
            hmm_config_summary_json=payload.get("hmm_config_summary") or {},
            config_summary_json=payload.get("config_summary") or {},
            source_payload_json={"loop_id": payload["loop_id"]},
            recorded_by=kwargs.get("recorded_by", "auto_hook"),
            source_type=kwargs.get("source_type", "qe_loop"),
        )

    def _config_summary_from_loop(self, config: dict[str, Any], custom_params: dict[str, Any]) -> dict[str, Any]:
        return {"model_id": config.get("model_id"), "strategy_id": config.get("strategy_id")}

    def _hmm_summary_from_params(self, params: dict[str, Any]) -> dict[str, Any]:
        return {key: params[key] for key in ("hmm_model_version_id", "hmm_signal_preset") if key in params}


def test_safe_record_disabled_is_noop_without_db(monkeypatch) -> None:
    def forbidden_get_conn() -> Any:
        raise AssertionError("disabled hook must not access DB")

    monkeypatch.delenv("RESEARCH_PIPELINE_HMM_RECORDING_ENABLED", raising=False)
    monkeypatch.setattr(realtime_module, "get_conn", forbidden_get_conn)

    result = safe_record_hmm_backtest_completed(task_id="task_1", loop_id="task_1_Loop1", loop_index=1)

    assert result == {"recorded": False, "skipped_reason": "disabled"}


def test_safe_record_hmm_backtest_completed_swallows_runtime_errors(monkeypatch) -> None:
    class BrokenIngestion:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def record_hmm_backtest_completed(self, **_kwargs: Any) -> dict[str, Any]:
            raise RuntimeError("boom")

    monkeypatch.setenv("RESEARCH_PIPELINE_HMM_RECORDING_ENABLED", "true")
    monkeypatch.setattr(realtime_module, "ResearchPipelineRealtimeIngestion", BrokenIngestion)

    result = safe_record_hmm_backtest_completed(task_id="task_1", loop_id="task_1_Loop1", loop_index=1)

    assert result["recorded"] is False
    assert result["skipped_reason"] == "error"
    assert "RuntimeError: boom" in result["error"]


def test_enabled_ingestion_skips_missing_research_experiment(monkeypatch) -> None:
    recorder = FakeRecorder()
    ingestion = ResearchPipelineRealtimeIngestion(recorder=recorder, enabled=True)

    monkeypatch.setattr(
        ingestion,
        "_fetch_loop_context",
        lambda **_kwargs: {
            "loop_id": "task_1_Loop1",
            "task_id": "task_1",
            "loop_index": 1,
            "config_json": {"record_backtest_to_research_pipeline": True, "research_domain": "hmm"},
            "metrics_json": {},
        },
    )

    result = ingestion.record_hmm_backtest_completed(task_id="task_1", loop_id="task_1_Loop1", loop_index=1)

    assert result == {"recorded": False, "skipped_reason": "missing_research_experiment_id"}
    assert recorder._repo.records == []


def test_enabled_ingestion_skips_non_hmm_research_loop(monkeypatch) -> None:
    recorder = FakeRecorder()
    ingestion = ResearchPipelineRealtimeIngestion(recorder=recorder, enabled=True)

    monkeypatch.setattr(
        ingestion,
        "_fetch_loop_context",
        lambda **_kwargs: {
            "loop_id": "task_1_Loop1",
            "task_id": "task_1",
            "loop_index": 1,
            "config_json": {
                "record_backtest_to_research_pipeline": True,
                "research_experiment_id": "rp_exp_1",
                "research_domain": "event_signal",
            },
            "metrics_json": {},
        },
    )

    result = ingestion.record_hmm_backtest_completed(task_id="task_1", loop_id="task_1_Loop1", loop_index=1)

    assert result == {"recorded": False, "skipped_reason": "not_hmm_research_loop"}
    assert recorder._repo.records == []


def test_enabled_ingestion_records_explicit_hmm_research_loop(monkeypatch) -> None:
    recorder = FakeRecorder()
    ingestion = ResearchPipelineRealtimeIngestion(recorder=recorder, enabled=True)

    monkeypatch.setattr(
        ingestion,
        "_fetch_loop_context",
        lambda **_kwargs: {
            "loop_id": "task_1_Loop1",
            "task_id": "task_1",
            "loop_index": 1,
            "config_json": {
                "model_id": "model_1",
                "strategy_id": "score_weighted_topk_v2",
                "record_backtest_to_research_pipeline": True,
                "research_experiment_id": "rp_exp_1",
                "research_domain": "hmm",
                "hmm_model_version_id": "hmm_v1",
            },
            "metrics_json": {"ann": 0.2},
            "source_experiment_id": "qe_exp_1",
        },
    )

    result = ingestion.record_hmm_backtest_completed(task_id="task_1", loop_id="task_1_Loop1", loop_index=1)

    assert result["recorded"] is True
    assert result["record_key_sha256"] == "record-key-1"
    assert result["experiment_id"] == "rp_exp_1"
    assert len(recorder._repo.records) == 1
    assert recorder._repo.records[0].hmm_config_summary_json == {"hmm_model_version_id": "hmm_v1"}


def test_enabled_ingestion_does_not_treat_qe_experiment_id_as_research_experiment(monkeypatch) -> None:
    recorder = FakeRecorder()
    ingestion = ResearchPipelineRealtimeIngestion(recorder=recorder, enabled=True)

    monkeypatch.setattr(
        ingestion,
        "_fetch_loop_context",
        lambda **_kwargs: {
            "loop_id": "task_1_Loop1",
            "task_id": "task_1",
            "loop_index": 1,
            "config_json": {
                "record_backtest_to_research_pipeline": True,
                "research_domain": "hmm",
                "hmm_model_version_id": "hmm_v1",
            },
            "metrics_json": {"ann": 0.2},
            "source_experiment_id": "qe_exp_1",
        },
    )

    result = ingestion.record_hmm_backtest_completed(task_id="task_1", loop_id="task_1_Loop1", loop_index=1)

    assert result == {"recorded": False, "skipped_reason": "missing_research_experiment_id"}
    assert recorder._repo.records == []


def test_qe_scheduler_research_hook_delegates_safely(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_safe_record_hmm_backtest_completed(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"recorded": True}

    monkeypatch.setattr(
        realtime_module,
        "safe_record_hmm_backtest_completed",
        fake_safe_record_hmm_backtest_completed,
    )

    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)
    scheduler._record_research_backtest_best_effort("task_1", "task_1_Loop1", 1)

    assert calls == [{"task_id": "task_1", "loop_id": "task_1_Loop1", "loop_index": 1, "experiment_id": None}]


def test_qe_scheduler_has_research_hook_after_both_archive_hooks() -> None:
    source = inspect.getsource(AutoEvolutionScheduler)

    archive_call = "self._archive_completed_loop_best_effort(task_id, evolution_loop_db_id, loop_index)"
    research_call = "self._record_research_backtest_best_effort(task_id, evolution_loop_db_id, loop_index)"
    assert source.count(archive_call) == 2
    assert source.count(research_call) == 2

    search_from = 0
    for _ in range(2):
        archive_index = source.index(archive_call, search_from)
        research_index = source.index(research_call, archive_index)
        assert archive_index < research_index
        search_from = research_index + len(research_call)
