from __future__ import annotations

import inspect
from pathlib import Path

from backend.services.qe_archive.archive_service import QEArchiveService
from backend.services.qe_archive.models import (
    ArchiveJobRecord,
    ClaimedOutboxEvent,
    OutboxEventRecord,
    RunConfigRecord,
    build_factor_set_hash,
    canonical_json_dumps,
    sha256_json,
)
from backend.services.qe_archive.event_capture import QEArchiveEventCapture
from backend.services.qe_archive.payload_extractor import QEArchivePayloadExtractor
from backend.services.qe_archive.repository import QEArchiveRepository
from backend.services.qe_archive.source_assembler import QEArchiveSourceAssembler
from backend.services.qe_archive.worker import ArchiveWorkerEventResult, QEArchiveWorker


REPO_ROOT = Path(__file__).resolve().parents[2]
QE_ARCHIVE_FILES = (
    REPO_ROOT / "backend" / "db" / "init_qe_archive_schema.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "__init__.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "archive_service.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "event_capture.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "models.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "payload_extractor.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "repository.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "source_assembler.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "worker.py",
    REPO_ROOT / "scripts" / "qe_archive_backfill.py",
)


def test_canonical_json_hash_is_stable() -> None:
    left = {"b": 2, "a": {"d": 4, "c": 3}}
    right = {"a": {"c": 3, "d": 4}, "b": 2}

    assert canonical_json_dumps(left) == canonical_json_dumps(right)
    assert sha256_json(left) == sha256_json(right)


def test_factor_set_hash_preserves_feature_order() -> None:
    first = [{"name": "factor_a"}, {"name": "factor_b"}]
    second = [{"name": "factor_b"}, {"name": "factor_a"}]

    assert build_factor_set_hash(first) != build_factor_set_hash(second)


def test_run_config_record_computes_config_and_factor_hashes() -> None:
    record = RunConfigRecord(
        run_id="qe_test_loop_1",
        config_schema_version="test",
        canonical_config={"model": {"type": "LSTM"}, "params": {"lr": 0.001}},
        raw_config={"source": "unit"},
        factor_list=["alpha_001", "alpha_002"],
        config_capture_complete=True,
    )

    assert record.config_sha256 == sha256_json(record.canonical_config)
    assert record.factor_set_hash == build_factor_set_hash(["alpha_001", "alpha_002"])
    assert record.config_capture_complete is True
    assert record.missing_config_items == []


def test_outbox_event_id_is_deterministic() -> None:
    first = OutboxEventRecord(
        event_type="qe.loop.completed",
        source_system="qe",
        source_id="task",
        source_sub_id="loop_1",
    )
    second = OutboxEventRecord(
        event_type="qe.loop.completed",
        source_system="qe",
        source_id="task",
        source_sub_id="loop_1",
    )

    assert first.event_id == second.event_id
    assert first.event_id is not None
    assert first.event_id.startswith("qear_evt_")


def test_repository_exposes_phase_one_write_methods() -> None:
    expected_methods = (
        "upsert_run",
        "mark_latest_attempt",
        "upsert_run_source",
        "upsert_run_config",
        "upsert_data_context",
        "upsert_account_summary",
        "upsert_reproducibility_manifest",
        "insert_raw_payload",
        "replace_raw_payloads",
        "insert_outbox_event",
        "upsert_metric_batch",
        "replace_run_curves",
        "replace_run_factors",
        "upsert_artifact_manifest",
        "claim_outbox_events",
        "complete_outbox_event",
        "fail_outbox_event",
        "create_archive_job",
        "complete_archive_job",
        "fail_archive_job",
    )

    for method_name in expected_methods:
        method = getattr(QEArchiveRepository, method_name)
        assert inspect.isfunction(method)


def test_archive_job_record_generates_id() -> None:
    record = ArchiveJobRecord(event_id="qear_evt_1", job_type="qe.loop.completed")

    assert record.job_id is not None
    assert record.job_id.startswith("qear_job_")
    assert record.status == "running"
    assert record.level == "A"
    assert record.stats == {}


def _sample_qe_payload() -> dict:
    return {
        "task_id": "task_a",
        "loop_id": "Loop1",
        "loop_index": 1,
        "experiment_id": "qe_exp_1",
        "status": "completed",
        "model_type": "LSTM",
        "freq": "day",
        "limit_suspend_authoritative": "false",
        "config": {
            "model": {"model_type": "LSTM"},
            "params": {"lr": 0.001},
            "strategy": {"topk": 20},
            "backtest": {"start_time": "2025-01-01", "end_time": "2025-12-31"},
            "data_split": {"train_start": "2022-01-01", "test_end": "2025-12-31"},
            "execution": {"limit_handling": "none", "suspend_handling": "none"},
            "factor_list": ["alpha_001", "alpha_002"],
        },
        "metrics": {
            "IC": 0.04,
            "ICIR": 1.2,
            "1day.excess_return_with_cost.annualized_return": 0.18,
            "1day.excess_return_with_cost.max_drawdown": -0.07,
            "enhanced_metrics": {
                "summary": {"Rank IC": 0.05, "sharpe": 1.6},
                "absolute_returns": {
                    "initial_capital": 1000000,
                    "final_total_value": 1120000,
                    "total_return": 0.12,
                    "max_drawdown": -0.07,
                    "final_cash": 120000,
                    "final_stock_value": 1000000,
                    "final_cash_ratio": 0.107142857,
                    "n_trading_days": 240,
                },
                "position_summary": {
                    "position_count_avg": 12.5,
                    "position_count_max": 20,
                },
                "ic_diagnostics": {
                    "dates": ["2025-01-02", "2025-01-03"],
                    "ic_series": [0.01, 0.02],
                    "rank_ic_series": [0.03, 0.04],
                },
                "return_curves": {
                    "dates": ["2025-01-02", "2025-01-03"],
                    "cumulative_excess_with_cost": [0.01, 0.015],
                    "drawdown_series": [0.0, -0.01],
                },
                "training_diagnostics": {
                    "train_loss_curve": [0.9, 0.7],
                    "val_loss_curve": [1.0, 0.8],
                },
            },
        },
    }


def test_payload_extractor_captures_reproducible_config_metrics_account_and_curves() -> None:
    extracted = QEArchivePayloadExtractor().extract(
        _sample_qe_payload(),
        event_type="qe.loop.completed",
        source_system="qe",
        source_id="task_a",
        source_sub_id="Loop1",
    )

    assert extracted.run.run_type == "evolution_loop"
    assert extracted.run.model_type == "LSTM"
    assert extracted.run.factor_count == 2
    assert extracted.run.research_valid is False
    assert extracted.run.invalid_reason == "daily_backtest_without_authoritative_limit_suspend"
    assert extracted.config.config_capture_complete is True
    assert extracted.config.factor_list == ["alpha_001", "alpha_002"]
    assert extracted.reproducibility_manifest.reproducibility_level == "full"
    assert extracted.data_contexts[0].backtest_start.isoformat() == "2025-01-01"
    assert extracted.data_contexts[0].backtest_end.isoformat() == "2025-12-31"
    assert extracted.account_summary is not None
    assert extracted.account_summary.initial_capital == 1000000.0
    assert extracted.account_summary.final_total_value == 1120000.0
    assert extracted.account_summary.max_drawdown == -0.07
    metric_keys = {metric.metric_key for metric in extracted.metrics}
    assert {"ic", "icir", "annualized_return", "max_drawdown", "rank_ic", "sharpe"}.issubset(metric_keys)
    curve_keys = {curve.curve_key for curve in extracted.curves}
    assert {"ic_series", "rank_ic_series", "cumulative_excess_with_cost", "drawdown_series", "train_loss_curve", "val_loss_curve"}.issubset(curve_keys)
    assert [factor.factor_name for factor in extracted.factors] == ["alpha_001", "alpha_002"]
    assert {payload.payload_type for payload in extracted.raw_payloads} == {
        "qe_completion_payload",
        "qe_metrics_payload",
        "qe_enhanced_metrics_payload",
    }


def test_archive_service_dry_run_does_not_write() -> None:
    class FakeRepository:
        def __getattr__(self, name):  # type: ignore[no-untyped-def]
            raise AssertionError(f"dry run must not call repository method {name}")

    result = QEArchiveService(repository=FakeRepository()).process_payload(  # type: ignore[arg-type]
        _sample_qe_payload(),
        event_type="qe.loop.completed",
        source_system="qe",
        source_id="task_a",
        source_sub_id="Loop1",
        dry_run=True,
    )

    assert result.dry_run is True
    assert result.stats["written"] is False
    assert result.stats["metric_count"] >= 6
    assert result.stats["curve_count"] >= 8


def test_archive_service_write_calls_repository_without_runtime_hooks() -> None:
    class FakeRepository:
        def __init__(self) -> None:
            self.calls: list[tuple[str, object]] = []

        def upsert_run(self, run):  # type: ignore[no-untyped-def]
            self.calls.append(("upsert_run", run))
            return run.run_id

        def upsert_run_source(self, source):  # type: ignore[no-untyped-def]
            self.calls.append(("upsert_run_source", source))

        def upsert_run_config(self, config):  # type: ignore[no-untyped-def]
            self.calls.append(("upsert_run_config", config))

        def upsert_reproducibility_manifest(self, manifest):  # type: ignore[no-untyped-def]
            self.calls.append(("upsert_reproducibility_manifest", manifest))

        def upsert_data_context(self, context):  # type: ignore[no-untyped-def]
            self.calls.append(("upsert_data_context", context))

        def upsert_account_summary(self, summary):  # type: ignore[no-untyped-def]
            self.calls.append(("upsert_account_summary", summary))

        def upsert_metric_batch(self, metrics, *, replace_existing):  # type: ignore[no-untyped-def]
            self.calls.append(("upsert_metric_batch", list(metrics)))
            assert replace_existing is True

        def replace_run_curves(self, run_id, curves):  # type: ignore[no-untyped-def]
            self.calls.append(("replace_run_curves", list(curves)))

        def replace_run_factors(self, run_id, factors):  # type: ignore[no-untyped-def]
            self.calls.append(("replace_run_factors", list(factors)))

        def replace_raw_payloads(self, run_id, raw_payloads):  # type: ignore[no-untyped-def]
            self.calls.append(("replace_raw_payloads", list(raw_payloads)))

    repository = FakeRepository()
    result = QEArchiveService(repository=repository).process_payload(  # type: ignore[arg-type]
        _sample_qe_payload(),
        event_type="qe.loop.completed",
        source_system="qe",
        source_id="task_a",
        source_sub_id="Loop1",
        dry_run=False,
    )

    assert result.stats["written"] is True
    call_names = [name for name, _ in repository.calls]
    assert call_names[:6] == [
        "upsert_run",
        "upsert_run_source",
        "upsert_run_config",
        "upsert_reproducibility_manifest",
        "upsert_data_context",
        "upsert_account_summary",
    ]
    assert "upsert_metric_batch" in call_names
    assert "replace_run_curves" in call_names
    assert "replace_run_factors" in call_names
    assert call_names.count("replace_raw_payloads") == 1
    raw_payload_call = repository.calls[call_names.index("replace_raw_payloads")]
    assert len(raw_payload_call[1]) == 3


def test_source_assembler_builds_single_experiment_payload_without_worker_paths() -> None:
    payload = QEArchiveSourceAssembler.build_experiment_payload(
        {
            "experiment_id": "qe_exp_a",
            "experiment_name": "single",
            "status": "completed",
            "factor_names": ["alpha_001", "alpha_002"],
            "model_id": "LSTM",
            "strategy_id": "TopkDropout",
            "data_split": {"train_start": "2022-01-01", "test_end": "2025-12-31"},
            "custom_params": {"label_horizon": 5, "execution_algo": "V25_TWO_STAGE"},
            "result_metrics": {"IC": 0.031},
            "qe_task_id": "task_a",
            "qe_loop_id": "Loop1",
            "loop_index": 1,
            "is_evolution_loop": False,
            "annualized_return": 0.18,
            "max_drawdown": -0.08,
        }
    )

    assert payload["source_system"] == "qe"
    assert payload["source_id"] == "qe_exp_a"
    assert payload["run_type"] == "single_experiment"
    assert payload["factor_list"] == ["alpha_001", "alpha_002"]
    assert payload["freq"] == "1min"
    assert payload["limit_suspend_authoritative"] is True
    assert payload["metrics"]["IC"] == 0.031
    assert payload["metrics"]["1day.excess_return_with_cost.annualized_return"] == 0.18
    assert payload["config"]["data_context"]["label_horizon"] == 5
    assert payload["source_config_paths"] == {"worker_artifact_paths_omitted": True}


def test_source_assembler_builds_loop_payload_for_archive_service() -> None:
    payload = QEArchiveSourceAssembler.build_loop_payload(
        {
            "loop_id": "task_b_Loop2",
            "task_id": "task_b",
            "loop_index": 2,
            "action_type": "param_tune",
            "config_json": {
                "factor_list": ["factor_a"],
                "runtime_flags": {"label_horizon": 3},
                "model_type": "LSTM",
                "backtest": {"start_time": "2025-01-01", "end_time": "2025-12-31"},
            },
            "metrics_json": {
                "IC": 0.042,
                "enhanced_metrics": {
                    "return_curves": {"dates": ["2025-01-02"], "drawdown_series": [-0.01]}
                },
            },
            "status": "completed",
            "is_sota": True,
        },
        {
            "task_id": "task_b",
            "task_name": "custom evo",
            "label_horizon": 3,
            "node_id": "node-a",
        },
    )
    extracted = QEArchivePayloadExtractor().extract(
        payload,
        event_type="qe.loop.completed",
        source_system=payload["source_system"],
        source_id=payload["source_id"],
        source_sub_id=payload["source_sub_id"],
    )

    assert payload["source_system"] == "qe_evolution"
    assert payload["run_type"] == "evolution_loop"
    assert payload["factor_list"] == ["factor_a"]
    assert payload["config"]["data_context"]["freq"] == "day"
    assert extracted.run.research_valid is False
    assert extracted.config.config_capture_complete is True
    assert any(curve.curve_key == "drawdown_series" for curve in extracted.curves)


def test_event_capture_is_disabled_by_default_and_does_not_write(monkeypatch) -> None:
    class FakeRepository:
        def __init__(self) -> None:
            self.events: list[OutboxEventRecord] = []

        def insert_outbox_event(self, event: OutboxEventRecord) -> bool:
            self.events.append(event)
            return True

    monkeypatch.delenv("QE_ARCHIVE_EVENT_CAPTURE_ENABLED", raising=False)
    repository = FakeRepository()
    capture = QEArchiveEventCapture(repository=repository)  # type: ignore[arg-type]

    inserted = capture.enqueue_loop_completed(
        task_id="qe_task",
        loop_id="loop_1",
        loop_index=1,
        payload={"status": "completed"},
    )

    assert inserted is False
    assert repository.events == []


def test_event_capture_writes_outbox_event_only_when_explicitly_enabled() -> None:
    class FakeRepository:
        def __init__(self) -> None:
            self.events: list[OutboxEventRecord] = []

        def insert_outbox_event(self, event: OutboxEventRecord) -> bool:
            self.events.append(event)
            return True

    repository = FakeRepository()
    capture = QEArchiveEventCapture(repository=repository, enabled=True)  # type: ignore[arg-type]

    inserted = capture.enqueue_experiment_completed(
        experiment_id="qe_exp",
        payload={"status": "completed"},
    )

    assert inserted is True
    assert len(repository.events) == 1
    event = repository.events[0]
    assert event.event_type == "qe.experiment.completed"
    assert event.source_system == "qe"
    assert event.source_id == "qe_exp"
    assert event.payload["experiment_id"] == "qe_exp"


def test_worker_is_disabled_by_default_and_does_not_claim(monkeypatch) -> None:
    class FakeRepository:
        def __init__(self) -> None:
            self.claims = 0

        def claim_outbox_events(self, **kwargs):  # type: ignore[no-untyped-def]
            self.claims += 1
            return []

    monkeypatch.delenv("QE_ARCHIVE_WORKER_ENABLED", raising=False)
    repository = FakeRepository()
    worker = QEArchiveWorker(
        repository=repository,  # type: ignore[arg-type]
        handlers={"qe.loop.completed": lambda event: ArchiveWorkerEventResult(success=True)},
    )

    result = worker.run_once()

    assert result.skipped_reason == "disabled"
    assert result.claimed == 0
    assert repository.claims == 0


def test_worker_enabled_without_handlers_does_not_claim() -> None:
    class FakeRepository:
        def __init__(self) -> None:
            self.claims = 0

        def claim_outbox_events(self, **kwargs):  # type: ignore[no-untyped-def]
            self.claims += 1
            return []

    repository = FakeRepository()
    worker = QEArchiveWorker(repository=repository, enabled=True)  # type: ignore[arg-type]

    result = worker.run_once()

    assert result.skipped_reason == "no_handlers"
    assert repository.claims == 0


def test_worker_processes_supported_event_and_completes_job() -> None:
    event = ClaimedOutboxEvent(
        event_id="evt_1",
        event_type="qe.loop.completed",
        source_system="qe",
        source_id="task_1",
        source_sub_id="loop_1",
        payload={"loop_index": 1},
    )

    class FakeRepository:
        def __init__(self) -> None:
            self.completed_events: list[str] = []
            self.completed_jobs: list[tuple[str, str | None, dict]] = []
            self.failed_events: list[str] = []
            self.claim_event_types: tuple[str, ...] | None = None

        def claim_outbox_events(self, *, worker_id, limit, event_types):  # type: ignore[no-untyped-def]
            self.claim_event_types = tuple(event_types)
            return [event]

        def create_archive_job(self, job):  # type: ignore[no-untyped-def]
            assert job.event_id == "evt_1"
            assert job.job_type == "qe.loop.completed"
            return "job_1"

        def complete_archive_job(self, job_id, *, run_id=None, stats=None):  # type: ignore[no-untyped-def]
            self.completed_jobs.append((job_id, run_id, dict(stats or {})))

        def complete_outbox_event(self, event_id):  # type: ignore[no-untyped-def]
            self.completed_events.append(event_id)

        def fail_archive_job(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("should not fail job")

        def fail_outbox_event(self, event_id, *args, **kwargs):  # type: ignore[no-untyped-def]
            self.failed_events.append(event_id)

    repository = FakeRepository()
    worker = QEArchiveWorker(
        repository=repository,  # type: ignore[arg-type]
        enabled=True,
        handlers={
            "qe.loop.completed": lambda item: ArchiveWorkerEventResult(
                success=True,
                run_id=f"run_{item.source_sub_id}",
                stats={"archived": True},
            )
        },
    )

    result = worker.run_once(limit=5)

    assert result.claimed == 1
    assert result.completed == 1
    assert result.failed == 0
    assert repository.claim_event_types == ("qe.loop.completed",)
    assert repository.completed_events == ["evt_1"]
    assert repository.completed_jobs == [("job_1", "run_loop_1", {"archived": True})]
    assert repository.failed_events == []


def test_worker_handler_exception_fails_job_and_retries_outbox() -> None:
    event = ClaimedOutboxEvent(
        event_id="evt_2",
        event_type="qe.experiment.completed",
        source_system="qe",
        source_id="experiment_1",
    )

    class FakeRepository:
        def __init__(self) -> None:
            self.failed_job: tuple[str, str] | None = None
            self.failed_event: tuple[str, str, int, int] | None = None

        def claim_outbox_events(self, **kwargs):  # type: ignore[no-untyped-def]
            return [event]

        def create_archive_job(self, job):  # type: ignore[no-untyped-def]
            return "job_2"

        def complete_archive_job(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("should not complete job")

        def complete_outbox_event(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("should not complete event")

        def fail_archive_job(self, job_id, error, *, stats=None):  # type: ignore[no-untyped-def]
            self.failed_job = (job_id, error)

        def fail_outbox_event(
            self,
            event_id,
            error,
            *,
            retry_after_seconds,
            max_retries,
        ):  # type: ignore[no-untyped-def]
            self.failed_event = (event_id, error, retry_after_seconds, max_retries)

    def boom(event):  # type: ignore[no-untyped-def]
        raise RuntimeError("archive failed")

    repository = FakeRepository()
    worker = QEArchiveWorker(
        repository=repository,  # type: ignore[arg-type]
        enabled=True,
        handlers={"qe.experiment.completed": boom},
        retry_after_seconds=7,
        max_retries=3,
    )

    result = worker.run_once()

    assert result.claimed == 1
    assert result.completed == 0
    assert result.failed == 1
    assert repository.failed_job is not None
    assert repository.failed_job[0] == "job_2"
    assert "RuntimeError: archive failed" in repository.failed_job[1]
    assert repository.failed_event is not None
    assert repository.failed_event[0] == "evt_2"
    assert "RuntimeError: archive failed" in repository.failed_event[1]
    assert repository.failed_event[2:] == (7, 3)


def test_qe_archive_implementation_does_not_read_worker_workspace_paths() -> None:
    banned_tokens = (
        "workspace_path",
        "/mnt/f",
        "\\\\" + "wsl$",
        "\\\\" + "wsl.localhost",
        "QE_WORKSPACE_WIN",
        "RDAGENT_WORKSPACE_WIN",
    )
    bad_locations: list[str] = []

    for path in QE_ARCHIVE_FILES:
        text = path.read_text(encoding="utf-8")
        for token in banned_tokens:
            if token in text:
                bad_locations.append(f"{path.relative_to(REPO_ROOT).as_posix()}: contains {token!r}")

    assert not bad_locations, "Forbidden worker workspace access token: " + ", ".join(bad_locations)
