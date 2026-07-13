from __future__ import annotations

import inspect
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import qe_archive as qe_archive_router
from backend.services.qe_archive.archive_service import QEArchiveService
from backend.services.qe_archive.backfill_service import (
    BACKFILL_CONFIRM_TEXT,
    QEArchiveBackfillOptions,
    QEArchiveBackfillRunOptions,
    QEArchiveBackfillService,
    WRITE_CONFIRM_TEXT,
)
from backend.services.qe_archive.models import (
    ArchiveJobRecord,
    ClaimedOutboxEvent,
    IngestHistoryRecord,
    OutboxEventRecord,
    RunConfigRecord,
    SkipRegistryRecord,
    build_factor_set_hash,
    canonical_json_dumps,
    sha256_json,
)
from backend.services.qe_archive.event_capture import QEArchiveEventCapture
from backend.services.qe_archive.payload_extractor import QEArchivePayloadExtractor
from backend.services.qe_archive.policy import resolve_archive_policy
from backend.services.qe_archive import realtime_ingestion as realtime_ingestion_module
from backend.services.qe_archive.realtime_ingestion import QEArchiveRealtimeIngestion
from backend.services.qe_archive.repository import QEArchiveRepository
from backend.services.qe_archive.source_assembler import QEArchiveSourceAssembler
from backend.services.qe_archive.worker import ArchiveWorkerEventResult, QEArchiveWorker
from backend.services.qe_archive.worker_service import (
    QEArchiveWorkerService,
    WORKER_CONFIRM_TEXT,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
QE_ARCHIVE_FILES = (
    REPO_ROOT / "backend" / "db" / "init_qe_archive_schema.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "__init__.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "archive_service.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "backfill_service.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "event_capture.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "ingest_history.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "models.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "payload_extractor.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "policy.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "realtime_ingestion.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "repository.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "skip_registry.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "source_assembler.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "worker.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "worker_loop.py",
    REPO_ROOT / "backend" / "services" / "qe_archive" / "worker_service.py",
    REPO_ROOT / "backend" / "routers" / "qe_archive.py",
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
        "replace_run_symbol_summaries",
        "replace_run_trades",
        "replace_run_execution_events",
        "upsert_artifact_manifest",
        "claim_outbox_events",
        "complete_outbox_event",
        "fail_outbox_event",
        "skip_outbox_event",
        "create_archive_job",
        "complete_archive_job",
        "fail_archive_job",
        "list_outbox_events",
        "list_archive_jobs",
        "list_runs",
        "get_archive_summary",
        "get_run_quality_summary",
        "upsert_skip_registry",
        "list_skips",
        "insert_ingest_history",
        "upsert_backfill_run",
        "update_backfill_run_status",
        "upsert_backfill_run_item",
        "list_backfill_runs",
        "get_backfill_run",
        "upsert_bootstrap_marker",
        "get_bootstrap_marker",
        "query_factor_usage",
        "query_factor_importance",
        "query_factor_importance_stability",
        "query_model_trials",
        "query_seed_trials",
        "query_hyperparam_history",
        "get_analytics_view_status",
        "query_run_leaderboard",
        "query_topk_quality",
        "query_seed_robustness",
        "query_factor_performance",
        "query_model_hyperparam_seed_perf",
        "query_overfit_flags",
        "query_promotion_candidates",
        "query_evolution_lineage",
    )

    for method_name in expected_methods:
        method = getattr(QEArchiveRepository, method_name)
        assert inspect.isfunction(method)


def test_archive_policy_audit_records_have_stable_ids() -> None:
    skip = SkipRegistryRecord(
        source_system="qe",
        source_type="loop",
        source_id="task_1",
        source_sub_id="loop_1",
        archive_policy="SKIP",
        archive_policy_source="unit",
        skip_reason="unit skip",
        trigger_reason="realtime",
    )
    hist = IngestHistoryRecord(
        source_system="qe",
        source_type="loop",
        source_id="task_1",
        source_sub_id="loop_1",
        trigger_reason="realtime",
        ingest_status="skipped",
    )
    assert skip.skip_id is not None and skip.skip_id.startswith("qear_skip_")
    assert hist.history_id is not None and hist.history_id.startswith("qear_hist_")


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
        "environment": {
            "python_version": "3.11.9",
            "package_versions": {"torch": "2.5.1", "qlib": "0.9.6"},
            "deterministic_flags": {"torch_deterministic": True, "cudnn_deterministic": True},
        },
        "git_commit": "abc123",
        "runner_script": "scripts/qrun_limit_minute.py",
        "source_config_paths": {"conf_yaml": "artifact://qe_exp_1/conf.yaml"},
        "config": {
            "model": {"model_type": "LSTM"},
            "params": {"lr": 0.001},
            "strategy": {"topk": 20},
            "backtest": {"start_time": "2025-01-01", "end_time": "2025-12-31"},
            "data_split": {"train_start": "2022-01-01", "test_end": "2025-12-31"},
            "execution": {"limit_handling": "none", "suspend_handling": "none"},
            "factor_list": ["alpha_001", "alpha_002"],
            "runtime_flags": {"random_seed": 20260522},
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
                "all_stocks": [
                    {
                        "code": "000001.SZ",
                        "profit": 1200.5,
                        "profit_pct": 0.12,
                        "avg_cost": 10.0,
                        "last_price": 11.2,
                        "holding_days": 5,
                        "first_date": "2025-01-02",
                        "last_date": "2025-01-08",
                    },
                    {
                        "code": "000002.SZ",
                        "profit": -300.0,
                        "profit_pct": -0.03,
                        "avg_cost": 20.0,
                        "last_price": 19.4,
                        "holding_days": 3,
                        "first_date": "2025-01-03",
                        "last_date": "2025-01-06",
                    },
                ],
                "top_stocks": [
                    {
                        "code": "000001.SZ",
                        "profit": 1200.5,
                        "profit_pct": 0.12,
                    }
                ],
                "bottom_stocks": [
                    {
                        "code": "000002.SZ",
                        "profit": -300.0,
                        "profit_pct": -0.03,
                    }
                ],
                "stock_trades": {
                    "000001.SZ": [
                        {"date": "2025-01-02", "type": "buy", "price": 10.0, "amount": 100000.0, "pnl": None},
                        {"date": "2025-01-08", "type": "sell", "price": 11.2, "amount": 112000.0, "pnl": 12000.0},
                    ]
                },
                "trade_diagnostics": {
                    "avg_turnover": 0.2,
                    "daily_trade_count_avg": 4.5,
                },
                "pytorch_correlation": {
                    "alpha_001": {"weight_pct": 62.5, "raw_value": 0.31},
                    "alpha_002": {"weight_pct": 37.5, "raw_value": 0.19},
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
    assert extracted.reproducibility_manifest.verification_status == "not_verified"
    assert extracted.reproducibility_manifest.random_seed == 20260522
    assert extracted.reproducibility_manifest.manifest_json["seed_policy"] == "fixed"
    assert extracted.reproducibility_manifest.deterministic_flags["seed_policy"] == "fixed"
    assert extracted.reproducibility_manifest.deterministic_flags["torch_deterministic"] is True
    assert extracted.reproducibility_manifest.package_versions["torch"] == "2.5.1"
    assert extracted.reproducibility_manifest.git_commit == "abc123"
    assert extracted.reproducibility_manifest.runner_script == "scripts/qrun_limit_minute.py"
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
    assert len(extracted.symbol_summaries) == 4
    assert extracted.symbol_summaries[0].symbol == "000001.SZ"
    assert extracted.symbol_summaries[0].source_list == "all_stocks"
    assert len(extracted.trades) == 2
    assert extracted.trades[0].symbol == "000001.SZ"
    assert extracted.trades[0].side == "buy"
    assert extracted.trades[0].trade_uid is not None
    assert extracted.trades[0].source_payload_path == "enhanced_metrics.stock_trades.000001.SZ[0]"
    importance_by_factor = {item.factor_name: item for item in extracted.factor_importance}
    assert importance_by_factor["alpha_001"].method == "pytorch_correlation"
    assert importance_by_factor["alpha_001"].importance_value == 62.5
    assert importance_by_factor["alpha_001"].normalized_value == 0.625
    assert importance_by_factor["alpha_001"].weight_pct == 62.5
    assert importance_by_factor["alpha_001"].rank_in_run == 1
    assert len(extracted.execution_events) >= 2
    assert extracted.stats["symbol_summary_count"] == 4
    assert extracted.stats["trade_count"] == 2
    assert extracted.stats["execution_event_count"] >= 2
    assert {payload.payload_type for payload in extracted.raw_payloads} == {
        "qe_completion_payload",
        "qe_metrics_payload",
        "qe_enhanced_metrics_payload",
    }


def test_payload_extractor_passes_through_topk_prediction_diagnostics() -> None:
    payload = _sample_qe_payload()
    payload["metrics"] = dict(payload["metrics"])
    enhanced = dict(payload["metrics"]["enhanced_metrics"])
    enhanced["prediction_diagnostics"] = {
        "topk_return_20": 0.0123,
        "topk_return_50": 0.0061,
        "topk_hit_rate_20": 0.55,
        "topk_hit_rate_50": 0.51,
        "topk_decay": 0.0062,
        "within_portfolio_rankic": -0.11,
        "topk_dispersion_20": 0.018,
        "topk_quality_status": "ok",
        "topk_source": "pred_label_artifacts",
        "topk_k_values": [20, 50],
        "topk_date_count": 2,
    }
    payload["metrics"]["enhanced_metrics"] = enhanced

    extracted = QEArchivePayloadExtractor().extract(
        payload,
        event_type="qe.loop.completed",
        source_system="qe",
        source_id="task_topk",
        source_sub_id="Loop1",
    )

    by_key = {metric.metric_key: metric for metric in extracted.metrics if metric.metric_scope == "prediction_topk"}
    assert by_key["topk_return_20"].value_num == 0.0123
    assert by_key["topk_hit_rate_20"].value_num == 0.55
    assert by_key["topk_quality_status"].value_text == "ok"
    assert by_key["topk_k_values"].value_json == [20, 50]
    assert by_key["topk_return_20"].quality_flag == "ok"


def test_payload_extractor_preserves_null_topk_with_quality_flag() -> None:
    payload = _sample_qe_payload()
    payload["metrics"] = dict(payload["metrics"])
    enhanced = dict(payload["metrics"]["enhanced_metrics"])
    enhanced["prediction_diagnostics"] = {
        "topk_return_20": None,
        "topk_hit_rate_20": None,
        "topk_quality_status": "missing_label",
        "topk_error": "label.pkl not found",
    }
    payload["metrics"]["enhanced_metrics"] = enhanced

    extracted = QEArchivePayloadExtractor().extract(
        payload,
        event_type="qe.loop.completed",
        source_system="qe",
        source_id="task_topk_missing",
        source_sub_id="Loop1",
    )

    by_key = {metric.metric_key: metric for metric in extracted.metrics if metric.metric_scope == "prediction_topk"}
    assert by_key["topk_return_20"].value_num is None
    assert by_key["topk_hit_rate_20"].value_num is None
    assert by_key["topk_quality_status"].value_text == "missing_label"
    assert by_key["topk_return_20"].quality_flag == "topk_missing_label"
    assert by_key["topk_error"].value_text == "label.pkl not found"


def test_payload_extractor_marks_seedless_payload_audit_only_unset_legacy() -> None:
    payload = _sample_qe_payload()
    payload["config"] = dict(payload["config"])
    payload["config"]["runtime_flags"] = {}
    payload.pop("random_seed", None)
    payload.pop("seed", None)

    extracted = QEArchivePayloadExtractor().extract(
        payload,
        event_type="qe.loop.completed",
        source_system="qe",
        source_id="legacy_task",
        source_sub_id="Loop16",
    )

    manifest = extracted.reproducibility_manifest
    assert manifest.random_seed is None
    assert manifest.reproducibility_level == "audit_only"
    assert manifest.verification_status == "not_reproducible"
    assert manifest.manifest_json["seed_policy"] == "unset_legacy"
    assert manifest.deterministic_flags["seed_policy"] == "unset_legacy"


def test_payload_extractor_reads_enhanced_feature_importance_gain_pct() -> None:
    payload = _sample_qe_payload()
    payload["metrics"] = dict(payload["metrics"])
    payload["metrics"]["enhanced_metrics"] = {
        "factor_analysis": {
            "feature_importance": [
                {"name": "alpha_a", "gain": 0.3, "gain_pct": 75.0, "method": "pytorch_correlation"},
                {"name": "alpha_b", "gain": 0.1, "gain_pct": 25.0, "method": "pytorch_correlation"},
            ]
        },
        "summary": {"Rank IC": 0.05, "sharpe": 1.6},
        "return_curves": {"dates": ["2025-01-02", "2025-01-03"]},
    }

    extracted = QEArchivePayloadExtractor().extract(
        payload,
        event_type="qe.loop.completed",
        source_system="qe",
        source_id="task_importance",
        source_sub_id="Loop1",
    )

    importance_by_factor = {item.factor_name: item for item in extracted.factor_importance}
    assert importance_by_factor["alpha_a"].method == "pytorch_correlation"
    assert importance_by_factor["alpha_a"].importance_value == 0.3
    assert importance_by_factor["alpha_a"].normalized_value == 0.75
    assert importance_by_factor["alpha_a"].weight_pct == 75.0
    assert importance_by_factor["alpha_a"].rank_in_run == 1


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
    assert result.extracted.run.archived_at is None
    assert result.stats["written"] is False
    assert result.stats["metric_count"] >= 6
    assert result.stats["curve_count"] >= 8
    assert result.stats["symbol_summary_count"] == 4
    assert result.stats["trade_count"] == 2


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

        def upsert_artifact_manifest(self, run_id, artifact_manifest, *, replace_existing):  # type: ignore[no-untyped-def]
            self.calls.append(("upsert_artifact_manifest", (run_id, list(artifact_manifest))))
            assert replace_existing is True

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

        def replace_run_factor_importance(self, run_id, importances):  # type: ignore[no-untyped-def]
            self.calls.append(("replace_run_factor_importance", list(importances)))

        def replace_run_symbol_summaries(self, run_id, summaries):  # type: ignore[no-untyped-def]
            self.calls.append(("replace_run_symbol_summaries", list(summaries)))

        def replace_run_trades(self, run_id, trades):  # type: ignore[no-untyped-def]
            self.calls.append(("replace_run_trades", list(trades)))

        def replace_run_execution_events(self, run_id, events):  # type: ignore[no-untyped-def]
            self.calls.append(("replace_run_execution_events", list(events)))

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
    run_call = repository.calls[call_names.index("upsert_run")]
    assert run_call[1].archived_at is not None
    assert run_call[1].archived_at.tzinfo is not None
    assert call_names[:7] == [
        "upsert_run",
        "upsert_run_source",
        "upsert_run_config",
        "upsert_reproducibility_manifest",
        "upsert_artifact_manifest",
        "upsert_data_context",
        "upsert_account_summary",
    ]
    assert "upsert_metric_batch" in call_names
    assert "replace_run_curves" in call_names
    assert "replace_run_factors" in call_names
    assert "replace_run_symbol_summaries" in call_names
    assert "replace_run_trades" in call_names
    assert "replace_run_execution_events" in call_names
    assert call_names.count("replace_raw_payloads") == 1
    symbol_call = repository.calls[call_names.index("replace_run_symbol_summaries")]
    trade_call = repository.calls[call_names.index("replace_run_trades")]
    event_call = repository.calls[call_names.index("replace_run_execution_events")]
    assert len(symbol_call[1]) == 4
    assert len(trade_call[1]) == 2
    assert len(event_call[1]) >= 2
    raw_payload_call = repository.calls[call_names.index("replace_raw_payloads")]
    assert len(raw_payload_call[1]) == 3


def test_repository_upsert_run_preserves_archived_at_when_incoming_value_is_null() -> None:
    executed: list[tuple[str, list[object] | None]] = []

    class FakeCursor:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, sql, params=None):  # type: ignore[no-untyped-def]
            executed.append((sql, params))

    class FakeConnection:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def cursor(self):  # type: ignore[no-untyped-def]
            return FakeCursor()

    repository = QEArchiveRepository(connection_provider=lambda: FakeConnection())
    repository.upsert_run(
        {
            "run_id": "run_archived_at_preserve",
            "logical_experiment_id": "qe_exp_archived_at",
            "source_system": "qe",
            "run_type": "evolution_loop",
            "status": "completed",
            "is_latest_attempt": False,
            "archived_at": None,
        }
    )

    assert executed, "upsert_run should execute an INSERT ... ON CONFLICT statement"
    assert "archived_at = COALESCE(EXCLUDED.archived_at, qe_archive.run.archived_at)" in executed[-1][0]


def test_factor_importance_query_includes_repro_config_source_and_return_context() -> None:
    executed: list[tuple[str, list[object] | None]] = []

    class FakeCursor:
        description = []

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, sql, params=None):  # type: ignore[no-untyped-def]
            executed.append((sql, params))

        def fetchall(self):  # type: ignore[no-untyped-def]
            return []

    class FakeConnection:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def cursor(self):  # type: ignore[no-untyped-def]
            return FakeCursor()

    repository = QEArchiveRepository(connection_provider=lambda: FakeConnection())
    repository.query_factor_importance(task_id="task_a", loop_index=16, limit=10)
    sql = executed[-1][0]

    assert "LEFT JOIN qe_archive.run_config c ON c.run_id = i.run_id" in sql
    assert "LEFT JOIN qe_archive.run_reproducibility_manifest repro ON repro.run_id = i.run_id" in sql
    assert "LEFT JOIN qe_archive.run_account_summary acc ON acc.run_id = i.run_id" in sql
    assert "LEFT JOIN qe_archive.run_data_context dc ON dc.run_id = i.run_id" in sql
    assert "LEFT JOIN qe_archive.run_source s ON s.run_id = i.run_id" in sql
    for field in (
        "repro.random_seed",
        "seed_policy",
        "repro.reproducibility_level",
        "repro.verification_status",
        "repro.deterministic_flags",
        "repro.package_versions",
        "repro.artifact_manifest_sha256",
        "c.config_sha256",
        "c.factor_set_hash",
        "c.runtime_flags",
        "c.model_params",
        "c.strategy_config",
        "c.data_split",
        "c.execution_config",
        "dc.train_start",
        "dc.backtest_end",
        "i.weight_pct",
        "s.mlflow_artifact_uri",
        "acc.cagr",
        "acc.total_return",
        "acc.max_drawdown",
        "acc.sharpe",
        "acc.avg_cash_ratio",
        "acc.final_cash_ratio",
    ):
        assert field in sql


def test_factor_importance_stability_query_includes_seed_hmm_and_return_risk_aggregation() -> None:
    executed: list[tuple[str, list[object] | None]] = []

    class FakeCursor:
        description = []

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, sql, params=None):  # type: ignore[no-untyped-def]
            executed.append((sql, params))

        def fetchall(self):  # type: ignore[no-untyped-def]
            return []

    class FakeConnection:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def cursor(self):  # type: ignore[no-untyped-def]
            return FakeCursor()

    repository = QEArchiveRepository(connection_provider=lambda: FakeConnection())
    repository.query_factor_importance_stability(factor_name="alpha_001", min_runs=2)
    sql = executed[-1][0]

    assert "LEFT JOIN qe_archive.run_config c ON c.run_id = i.run_id" in sql
    assert "LEFT JOIN qe_archive.run_reproducibility_manifest repro ON repro.run_id = i.run_id" in sql
    assert "LEFT JOIN qe_archive.run_account_summary acc ON acc.run_id = i.run_id" in sql
    assert "LEFT JOIN qe_archive.run_data_context dc ON dc.run_id = i.run_id" in sql
    assert "LEFT JOIN qe_archive.run_source s ON s.run_id = i.run_id" in sql
    for field in (
        "COUNT(DISTINCT repro.random_seed)",
        "ARRAY_AGG(DISTINCT repro.random_seed)",
        "repro.reproducibility_level",
        "repro.verification_status",
        "hmm_enabled_run_count",
        "no_hmm_run_count",
        "AVG(acc.cagr)",
        "AVG(i.weight_pct)",
        "AVG(acc.total_return)",
        "AVG(acc.max_drawdown)",
        "AVG(acc.sharpe)",
        "AVG(acc.avg_cash_ratio)",
        "AVG(acc.final_cash_ratio)",
    ):
        assert field in sql


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


def test_archive_policy_resolves_single_experiment_template_skip() -> None:
    payload = QEArchiveSourceAssembler.build_experiment_payload(
        {
            "experiment_id": "qe_exp_skip",
            "experiment_name": "single skip",
            "status": "completed",
            "factor_names": ["alpha_001"],
            "model_id": "LGBModel",
            "custom_params": {
                "archive_policy": "SKIP",
                "archive_reason": "operator opted out",
            },
        }
    )

    decision = resolve_archive_policy(
        source_system=payload["source_system"],
        source_type="experiment",
        source_id=payload["source_id"],
        payload=payload,
        runtime_config=payload["config"],
    )

    assert decision.archive_policy == "SKIP"
    assert decision.should_archive is False
    assert decision.archive_policy_source == "runtime_config.runtime_flags"
    assert decision.reason == "operator opted out"


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
    assert extracted.data_contexts[0].backtest_start.isoformat() == "2025-01-02"
    assert extracted.data_contexts[0].backtest_end.isoformat() == "2025-01-02"
    assert any(curve.curve_key == "drawdown_series" for curve in extracted.curves)


def test_archive_policy_resolves_custom_evo_strategy_params_manual_only() -> None:
    payload = QEArchiveSourceAssembler.build_loop_payload(
        {
            "loop_id": "task_c_Loop1",
            "task_id": "task_c",
            "loop_index": 1,
            "config_json": {
                "factor_list": ["factor_a"],
                "strategy_params": {
                    "archive_policy": "MANUAL_ONLY",
                    "archive_reason": "needs manual review",
                },
                "model_params": {"label_horizon": 3},
            },
            "metrics_json": {"IC": 0.01},
            "status": "completed",
        },
        {"task_id": "task_c", "task_name": "custom evo"},
    )
    decision = resolve_archive_policy(
        source_system=payload["source_system"],
        source_type="loop",
        source_id=payload["source_id"],
        source_sub_id=payload["source_sub_id"],
        payload=payload,
        runtime_config=payload["config"],
    )

    assert payload["config"]["runtime_flags"]["archive_policy"] == "MANUAL_ONLY"
    assert decision.archive_policy == "MANUAL_ONLY"
    assert decision.should_archive is False
    assert decision.is_manual_only is True
    assert decision.archive_policy_source == "runtime_config.runtime_flags"


def test_backfill_service_requires_confirmation_for_writes() -> None:
    service = QEArchiveBackfillService(
        assembler=SimpleNamespace(),
        archive_service=SimpleNamespace(),
        repository=SimpleNamespace(),
    )

    try:
        service.process_backfill(QEArchiveBackfillOptions(source="loop", write=True))
    except ValueError as exc:
        assert WRITE_CONFIRM_TEXT in str(exc)
    else:
        raise AssertionError("write mode must require explicit confirmation text")


def test_backfill_service_processes_explicit_loop_ids_without_manual_script() -> None:
    class FakeAssembler:
        def __init__(self) -> None:
            self.loop_ids: list[str] = []

        def assemble_loop_payload(self, *, loop_id=None, task_id=None, loop_index=None):  # type: ignore[no-untyped-def]
            self.loop_ids.append(loop_id)
            return {
                "source_system": "qe_evolution",
                "source_id": "task_1",
                "source_sub_id": loop_id,
            }

    class FakeArchiveService:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        def process_payload(self, payload, *, event_type, source_system, source_id, source_sub_id, dry_run):  # type: ignore[no-untyped-def]
            self.calls.append(
                {
                    "payload": payload,
                    "event_type": event_type,
                    "source_system": source_system,
                    "source_id": source_id,
                    "source_sub_id": source_sub_id,
                    "dry_run": dry_run,
                }
            )
            return SimpleNamespace(
                run_id=f"run_{source_sub_id}",
                stats={"written": not dry_run, "metric_count": 3},
            )

    class FakeRepository:
        def get_archive_summary(self):  # type: ignore[no-untyped-def]
            return {"run_count": 1, "pending_outbox_count": 0}

        def get_run_quality_summary(self, run_id):  # type: ignore[no-untyped-def]
            return {
                "run_id": run_id,
                "exists": True,
                "metric_count": 3,
                "curve_count": 4,
                "factor_count_rows": 2,
                "account_summary_count": 1,
            }

    assembler = FakeAssembler()
    archive_service = FakeArchiveService()
    result = QEArchiveBackfillService(
        assembler=assembler,  # type: ignore[arg-type]
        archive_service=archive_service,  # type: ignore[arg-type]
        repository=FakeRepository(),  # type: ignore[arg-type]
    ).process_backfill(
        QEArchiveBackfillOptions(
            source="loop",
            loop_ids=["loop_a", "loop_a", "loop_b"],
            write=True,
            confirm_write=WRITE_CONFIRM_TEXT,
            min_metrics=1,
            min_curves=1,
            min_factors=1,
            require_account_summary=True,
        )
    )

    assert assembler.loop_ids == ["loop_a", "loop_b"]
    assert result["processed_count"] == 2
    assert result["write_enabled"] is True
    assert result["archive_summary"] == {"run_count": 1, "pending_outbox_count": 0}
    assert [call["dry_run"] for call in archive_service.calls] == [False, False]
    assert result["results"][0]["quality"]["passed"] is True


def test_backfill_service_task_ids_expand_to_unarchived_completed_loops_by_default() -> None:
    class FakeAssembler:
        def __init__(self) -> None:
            self.task_ids: list[str] = []
            self.loop_ids: list[str] = []

        def list_loop_refs_for_tasks(self, task_ids, *, status, include_archived):  # type: ignore[no-untyped-def]
            self.task_ids = list(task_ids)
            assert status == "completed"
            assert include_archived is False
            return [
                {"task_id": "task_1", "loop_id": "loop_3", "loop_index": 3},
            ]

        def assemble_loop_payload(self, *, loop_id=None, task_id=None, loop_index=None):  # type: ignore[no-untyped-def]
            self.loop_ids.append(loop_id)
            return {
                "source_system": "qe_evolution",
                "source_id": task_id,
                "source_sub_id": loop_id,
            }

    class FakeArchiveService:
        def process_payload(self, payload, *, event_type, source_system, source_id, source_sub_id, dry_run):  # type: ignore[no-untyped-def]
            return SimpleNamespace(run_id=f"run_{source_sub_id}", stats={"written": not dry_run})

    assembler = FakeAssembler()
    result = QEArchiveBackfillService(
        assembler=assembler,  # type: ignore[arg-type]
        archive_service=FakeArchiveService(),  # type: ignore[arg-type]
        repository=SimpleNamespace(get_archive_summary=lambda: {"run_count": 2}),
    ).process_backfill(
        QEArchiveBackfillOptions(
            source="task",
            task_ids=["task_1"],
            write=False,
        )
    )

    assert assembler.task_ids == ["task_1"]
    assert assembler.loop_ids == ["loop_3"]
    assert result["processed_count"] == 1
    assert [item["source_sub_id"] for item in result["results"]] == ["loop_3"]


def test_backfill_service_task_loop_indices_expand_selected_loops() -> None:
    class FakeAssembler:
        def __init__(self) -> None:
            self.refs_args = None
            self.loop_ids: list[str] = []

        def list_loop_refs_for_task_indices(self, task_id, loop_indices, *, status, include_archived):  # type: ignore[no-untyped-def]
            self.refs_args = {
                "task_id": task_id,
                "loop_indices": list(loop_indices),
                "status": status,
                "include_archived": include_archived,
            }
            return [
                {"task_id": "task_1", "loop_id": "loop_1", "loop_index": 1},
                {"task_id": "task_1", "loop_id": "loop_3", "loop_index": 3},
            ]

        def assemble_loop_payload(self, *, loop_id=None, task_id=None, loop_index=None):  # type: ignore[no-untyped-def]
            self.loop_ids.append(loop_id)
            return {
                "source_system": "qe_evolution",
                "source_id": task_id,
                "source_sub_id": loop_id,
            }

    class FakeArchiveService:
        def process_payload(self, payload, *, event_type, source_system, source_id, source_sub_id, dry_run):  # type: ignore[no-untyped-def]
            return SimpleNamespace(run_id=f"run_{source_sub_id}", stats={"written": not dry_run})

    assembler = FakeAssembler()
    result = QEArchiveBackfillService(
        assembler=assembler,  # type: ignore[arg-type]
        archive_service=FakeArchiveService(),  # type: ignore[arg-type]
        repository=SimpleNamespace(get_archive_summary=lambda: {"run_count": 2}),
    ).process_backfill(
        QEArchiveBackfillOptions(
            source="loop",
            task_id="task_1",
            loop_indices=[1, 3, 3, 99],
            write=False,
        )
    )

    assert assembler.refs_args == {
        "task_id": "task_1",
        "loop_indices": [1, 3, 99],
        "status": "completed",
        "include_archived": False,
    }
    assert assembler.loop_ids == ["loop_1", "loop_3"]
    assert result["processed_count"] == 3
    assert [item["source_sub_id"] for item in result["results"][:2]] == ["loop_1", "loop_3"]
    assert result["results"][2]["source_sub_id"] == "Loop99"
    assert result["results"][2]["skipped_reason"] == "loop_not_found_or_filtered"


def test_backfill_all_includes_multi_alpha_runs() -> None:
    class FakeAssembler:
        def list_experiment_ids(self, *, status, limit, include_archived):  # type: ignore[no-untyped-def]
            return ["exp_1"]

        def list_loop_refs(self, *, status, limit, include_archived):  # type: ignore[no-untyped-def]
            return [{"task_id": "task_1", "loop_id": "loop_1", "loop_index": 1}]

        def assemble_experiment_payload(self, experiment_id):  # type: ignore[no-untyped-def]
            return {"source_system": "qe", "source_id": experiment_id, "experiment_id": experiment_id}

        def assemble_loop_payload(self, *, loop_id=None, task_id=None, loop_index=None):  # type: ignore[no-untyped-def]
            return {"source_system": "qe_evolution", "source_id": task_id, "source_sub_id": loop_id}

    class FakeArchiveService:
        def process_payload(self, payload, *, event_type, source_system, source_id, source_sub_id, dry_run):  # type: ignore[no-untyped-def]
            return SimpleNamespace(run_id=f"run_{source_id}_{source_sub_id or 'exp'}", stats={"written": not dry_run})

    class FakeRepository:
        def get_archive_summary(self):  # type: ignore[no-untyped-def]
            return {"run_count": 2}

        def list_multi_alpha_combine_run_ids(self, *, include_archived, limit):  # type: ignore[no-untyped-def]
            assert include_archived is False
            assert limit == 10
            return ["macb_1", "macb_2"]

    class FakeMultiAlphaHandler:
        def archive_run(self, run_id, *, dry_run=False):  # type: ignore[no-untyped-def]
            return {
                "run_id": run_id,
                "dry_run": dry_run,
                "written": not dry_run,
                "leg_count": 1,
                "provenance_complete_leg_count": 1,
                "leg_source_count": 1,
                "resolved_source_count": 1,
            }

    result = QEArchiveBackfillService(
        assembler=FakeAssembler(),  # type: ignore[arg-type]
        archive_service=FakeArchiveService(),  # type: ignore[arg-type]
        repository=FakeRepository(),  # type: ignore[arg-type]
        multi_alpha_handler=FakeMultiAlphaHandler(),  # type: ignore[arg-type]
    ).process_backfill(QEArchiveBackfillOptions(source="all", limit=10, write=False))

    assert result["processed_count"] == 4
    assert result["candidate_count"] == 4
    assert result["multi_alpha_report"]["processed_count"] == 2
    assert result["multi_alpha_report"]["provenance_report"]["source_resolve_rate"] == 1.0


def test_backfill_execute_records_audit_run_items_and_separate_counts() -> None:
    class FakeAssembler:
        def list_loop_refs_for_task_indices(self, task_id, loop_indices, *, status, include_archived):  # type: ignore[no-untyped-def]
            assert task_id == "task_audit"
            assert loop_indices == [1, 2]
            return [{"task_id": "task_audit", "loop_id": "task_audit_Loop1", "loop_index": 1}]

        def assemble_loop_payload(self, *, loop_id=None, task_id=None, loop_index=None):  # type: ignore[no-untyped-def]
            return {
                "source_system": "qe_evolution",
                "source_id": task_id,
                "source_sub_id": loop_id,
                "task_id": task_id,
                "loop_id": loop_id,
                "loop_index": loop_index,
                "status": "completed",
                "config": {},
            }

    class FakeArchiveService:
        def process_payload(self, payload, *, event_type, source_system, source_id, source_sub_id, dry_run):  # type: ignore[no-untyped-def]
            return SimpleNamespace(run_id=f"run_{source_sub_id}", stats={"written": not dry_run})

    class FakeRepository:
        def __init__(self) -> None:
            self.backfill_runs: list[object] = []
            self.status_updates: list[dict[str, object]] = []
            self.items: list[object] = []

        def upsert_backfill_run(self, record):  # type: ignore[no-untyped-def]
            self.backfill_runs.append(record)
            return record.backfill_run_id

        def update_backfill_run_status(self, backfill_run_id, **kwargs):  # type: ignore[no-untyped-def]
            self.status_updates.append({"backfill_run_id": backfill_run_id, **kwargs})

        def upsert_backfill_run_item(self, record):  # type: ignore[no-untyped-def]
            self.items.append(record)
            return record.item_id

        def get_run_quality_summary(self, run_id):  # type: ignore[no-untyped-def]
            return {"run_id": run_id, "exists": True}

        def get_archive_summary(self):  # type: ignore[no-untyped-def]
            return {"run_count": 1}

    repository = FakeRepository()
    result = QEArchiveBackfillService(
        assembler=FakeAssembler(),  # type: ignore[arg-type]
        archive_service=FakeArchiveService(),  # type: ignore[arg-type]
        repository=repository,  # type: ignore[arg-type]
    ).execute_backfill(
        QEArchiveBackfillRunOptions(
            source_mode="specific_ids",
            task_id="task_audit",
            loop_indices=[1, 2],
            confirm_backfill=BACKFILL_CONFIRM_TEXT,
        )
    )

    assert result["backfill_run_id"]
    assert result["candidate_count"] == 2
    assert result["processed_count"] == 2
    assert result["ingested_count"] == 1
    assert result["skipped_count"] == 1
    assert result["failed_count"] == 0
    assert [item.status for item in repository.items] == ["ingested", "skipped"]
    assert repository.status_updates[-1]["ingested_count"] == 1
    assert repository.status_updates[-1]["skipped_count"] == 1
    assert repository.status_updates[-1]["failed_count"] == 0


def test_backfill_service_candidate_listing_uses_page_and_page_size() -> None:
    class FakeAssembler:
        def list_backfill_candidates(self, *, status, limit, offset, include_archived):  # type: ignore[no-untyped-def]
            assert status == "completed"
            assert limit == 21
            assert offset == 20
            assert include_archived is False
            return [
                {
                    "candidate_id": f"task:task_{idx}",
                    "candidate_type": "evolution_task",
                    "task_id": f"task_{idx}",
                    "pending_run_count": 1,
                }
                for idx in range(21)
            ]

    result = QEArchiveBackfillService(
        assembler=FakeAssembler(),  # type: ignore[arg-type]
        archive_service=SimpleNamespace(),
        repository=SimpleNamespace(),
    ).list_backfill_candidates(status="completed", page=2, page_size=20, include_archived=False)

    assert result["page"] == 2
    assert result["page_size"] == 20
    assert result["offset"] == 20
    assert result["count"] == 20
    assert result["has_more"] is True
    assert len(result["candidates"]) == 20


def test_realtime_ingestion_is_disabled_by_default_and_does_not_archive() -> None:
    class FakeBackfillService:
        def __init__(self) -> None:
            self.calls = 0

        def archive_loop_completed(self, **kwargs):  # type: ignore[no-untyped-def]
            self.calls += 1
            return {"unexpected": True}

    service = FakeBackfillService()
    ingestion = QEArchiveRealtimeIngestion(service=service, enabled=False)  # type: ignore[arg-type]

    result = ingestion.archive_loop_completed(task_id="task_1", loop_id="task_1_Loop1")

    assert result == {"archived": False, "skipped_reason": "disabled"}
    assert service.calls == 0


def test_realtime_ingestion_enabled_queues_outbox_by_default() -> None:
    class FakeEventCapture:
        def __init__(self) -> None:
            self.kwargs = None

        def enqueue_experiment_completed_result(self, **kwargs):  # type: ignore[no-untyped-def]
            self.kwargs = kwargs
            return {
                "inserted": True,
                "event_id": "qear_evt_1",
                "event_type": "qe.experiment.completed",
                "source_id": "qe_exp_1",
            }

    class FakeBackfillService:
        def archive_experiment_completed(self, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("default realtime mode should enqueue outbox instead of direct archive write")

    capture = FakeEventCapture()
    ingestion = QEArchiveRealtimeIngestion(
        service=FakeBackfillService(),  # type: ignore[arg-type]
        event_capture=capture,  # type: ignore[arg-type]
        enabled=True,
    )

    result = ingestion.archive_experiment_completed(experiment_id="qe_exp_1")

    assert result["archived"] is False
    assert result["queued"] is True
    assert result["mode"] == "outbox"
    assert result["event_id"] == "qear_evt_1"
    assert capture.kwargs["experiment_id"] == "qe_exp_1"
    assert capture.kwargs["payload"]["capture_reason"] == "qe_experiment_completed_hook"
    assert capture.kwargs["archive_policy"] == "AUTO"
    assert capture.kwargs["trigger_reason"] == "realtime"


def test_realtime_ingestion_skip_policy_records_skip_without_outbox(monkeypatch) -> None:
    skips: list[str] = []
    histories: list[str] = []

    class FakeAssembler:
        def assemble_experiment_payload(self, experiment_id):  # type: ignore[no-untyped-def]
            return {
                "source_system": "qe",
                "source_id": experiment_id,
                "experiment_id": experiment_id,
                "config": {
                    "runtime_flags": {
                        "archive_policy": "SKIP",
                        "archive_reason": "do not warehouse",
                    }
                },
            }

    class FakeEventCapture:
        def enqueue_experiment_completed_result(self, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("SKIP policy must not enqueue outbox events")

    def fake_record_policy_skip(decision, **kwargs):  # type: ignore[no-untyped-def]
        skips.append(decision.archive_policy)
        return "skip_1"

    def fake_record_decision_skip(decision, **kwargs):  # type: ignore[no-untyped-def]
        histories.append(decision.reason)
        return "hist_1"

    monkeypatch.setattr(realtime_ingestion_module, "record_policy_skip", fake_record_policy_skip)
    monkeypatch.setattr(realtime_ingestion_module, "record_decision_skip", fake_record_decision_skip)

    ingestion = QEArchiveRealtimeIngestion(
        event_capture=FakeEventCapture(),  # type: ignore[arg-type]
        assembler=FakeAssembler(),  # type: ignore[arg-type]
        enabled=True,
    )

    result = ingestion.archive_experiment_completed(experiment_id="qe_exp_skip")

    assert result["queued"] is False
    assert result["skipped_reason"] == "SKIP"
    assert result["skip_id"] == "skip_1"
    assert skips == ["SKIP"]
    assert histories == ["do not warehouse"]


def test_realtime_ingestion_direct_mode_calls_backfill_service() -> None:
    class FakeBackfillService:
        def __init__(self) -> None:
            self.kwargs = None

        def archive_experiment_completed(self, **kwargs):  # type: ignore[no-untyped-def]
            self.kwargs = kwargs
            return {"processed_count": 1}

    service = FakeBackfillService()
    ingestion = QEArchiveRealtimeIngestion(service=service, enabled=True, mode="direct")  # type: ignore[arg-type]

    result = ingestion.archive_experiment_completed(experiment_id="qe_exp_1")

    assert result == {"processed_count": 1}
    assert service.kwargs == {"experiment_id": "qe_exp_1"}


def test_qe_archive_backfill_api_requires_confirmation(monkeypatch) -> None:
    class FakeService:
        called = False

        def process_backfill(self, options):  # type: ignore[no-untyped-def]
            self.called = True
            return {}

    fake = FakeService()
    monkeypatch.setattr(qe_archive_router, "get_backfill_service", lambda: fake)
    app = FastAPI()
    app.include_router(qe_archive_router.router, prefix="/api/v1")
    client = TestClient(app)

    response = client.post(
        "/api/v1/qe-archive/backfill",
        json={"source": "loop", "loop_ids": ["loop_1"], "write": True},
    )

    assert response.status_code == 400
    assert WRITE_CONFIRM_TEXT in response.json()["detail"]
    assert fake.called is False


def test_qe_archive_backfill_api_returns_service_report(monkeypatch) -> None:
    class FakeService:
        def process_backfill(self, options):  # type: ignore[no-untyped-def]
            assert options.loop_ids == ["loop_1"]
            assert options.loop_indices == [1, 3]
            assert options.include_archived is False
            assert options.write is False
            return {"processed_count": 1, "results": [{"run_id": "run_loop_1"}]}

    monkeypatch.setattr(qe_archive_router, "get_backfill_service", lambda: FakeService())
    app = FastAPI()
    app.include_router(qe_archive_router.router, prefix="/api/v1")
    client = TestClient(app)

    response = client.post(
        "/api/v1/qe-archive/backfill",
        json={"source": "loop", "loop_ids": ["loop_1"], "loop_indices": [1, 3], "write": False, "include_archived": False},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["processed_count"] == 1
    assert body["data"]["results"][0]["run_id"] == "run_loop_1"


def test_qe_archive_source_status_api_returns_coverage(monkeypatch) -> None:
    class FakeService:
        def get_source_status(self, *, experiment_ids, task_ids, loop_ids, include_recommendation):  # type: ignore[no-untyped-def]
            assert experiment_ids == ["qe_exp_1"]
            assert task_ids == ["task_1"]
            assert loop_ids == ["loop_1"]
            assert include_recommendation is True
            return {
                "experiments": {"qe_exp_1": {"archive_status": "archived", "run_ids": ["run_1"]}},
                "tasks": {"task_1": {"archive_status": "partially_archived", "archived_loop_count": 1}},
                "loops": {"loop_1": {"archive_status": "archived", "run_ids": ["run_1"]}},
            }

    monkeypatch.setattr(qe_archive_router, "get_backfill_service", lambda: FakeService())
    app = FastAPI()
    app.include_router(qe_archive_router.router, prefix="/api/v1")
    client = TestClient(app)

    response = client.post(
        "/api/v1/qe-archive/source-status",
        json={"experiment_ids": ["qe_exp_1"], "task_ids": ["task_1"], "loop_ids": ["loop_1"]},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["experiments"]["qe_exp_1"]["archive_status"] == "archived"
    assert data["tasks"]["task_1"]["archive_status"] == "partially_archived"


def test_qe_archive_backfill_candidates_api_returns_selectable_sources(monkeypatch) -> None:
    class FakeService:
        def list_backfill_candidates(self, *, status, limit, page, page_size, include_archived):  # type: ignore[no-untyped-def]
            assert status == "completed"
            assert limit == 50
            assert page == 2
            assert page_size == 50
            assert include_archived is False
            return {
                "status": status,
                "page": page,
                "page_size": page_size,
                "has_more": False,
                "count": 1,
                "candidates": [
                    {
                        "candidate_id": "task:task_1",
                        "candidate_type": "evolution_task",
                        "task_id": "task_1",
                        "display_name": "demo",
                        "selected_run_count": 2,
                        "pending_run_count": 2,
                    }
                ],
            }

    monkeypatch.setattr(qe_archive_router, "get_backfill_service", lambda: FakeService())
    app = FastAPI()
    app.include_router(qe_archive_router.router, prefix="/api/v1")
    client = TestClient(app)

    response = client.get("/api/v1/qe-archive/backfill-candidates?page=2&page_size=50")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["page"] == 2
    assert data["page_size"] == 50
    assert data["count"] == 1
    assert data["candidates"][0]["candidate_id"] == "task:task_1"


def test_qe_archive_runs_api_returns_selectable_quality_sources(monkeypatch) -> None:
    class FakeRepository:
        def list_runs(self, *, status, run_type, search, limit):  # type: ignore[no-untyped-def]
            assert status == "completed"
            assert run_type is None
            assert search == "task_1"
            assert limit == 25
            return [
                {
                    "run_id": "qear_run_1",
                    "task_id": "task_1",
                    "loop_id": "loop_3",
                    "run_type": "evolution_loop",
                    "status": "completed",
                    "metric_count": 80,
                }
            ]

    monkeypatch.setattr(qe_archive_router, "get_repository", lambda: FakeRepository())
    app = FastAPI()
    app.include_router(qe_archive_router.router, prefix="/api/v1")
    client = TestClient(app)

    response = client.get("/api/v1/qe-archive/runs?status=completed&search=task_1&limit=25")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data[0]["run_id"] == "qear_run_1"
    assert data[0]["loop_id"] == "loop_3"




def test_qe_archive_analytics_api_exposes_compact_view_queries(monkeypatch) -> None:
    class FakeRepository:
        def get_analytics_view_status(self):  # type: ignore[no-untyped-def]
            return [{"view_name": "v_run_leaderboard", "available": True, "row_count": 2}]

        def query_run_leaderboard(self, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs == {"model_type": "LSTM", "min_icir": 0.5, "min_ir": None, "limit": 7, "order_by": "icir"}
            return [{"run_id": "run_1", "icir": 0.6}]

        def query_topk_quality(self, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs == {"run_id": "run_1", "task_id": None, "k": 20, "limit": 6}
            return [{"run_id": "run_1", "topk_return_20": 0.012}]

        def query_seed_robustness(self, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs == {"model_type": None, "min_seed_count": 3, "stable_only": True, "limit": 5, "order_by": "cagr_mean"}
            return [{"factor_set_hash": "hash", "distinct_seed_count": 3}]

        def query_factor_performance(self, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs == {"factor_name": "alpha_001", "min_runs": 2, "limit": 4, "order_by": "avg_icir"}
            return [{"factor_name": "alpha_001", "run_count": 2}]

        def query_model_hyperparam_seed_perf(self, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs == {"model_type": "LSTM", "hyperparam_hash": None, "limit": 6, "order_by": "cagr"}
            return [{"model_type": "LSTM", "hyperparam_hash": "abc"}]

        def query_overfit_flags(self, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs == {"suspicious_only": True, "model_type": None, "limit": 3}
            return [{"run_id": "run_2", "is_suspicious": True}]

        def query_promotion_candidates(self, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs == {"model_type": None, "min_seed_count": 5, "limit": 8, "order_by": "ir_mean"}
            return [{"factor_set_hash": "hash", "passes_gate": True}]

        def query_evolution_lineage(self, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs == {"task_id": "task_1", "experiment_id": None, "model_type": None, "limit": 9}
            return [{"task_id": "task_1", "loop_index": 1}]

    monkeypatch.setattr(qe_archive_router, "get_repository", lambda: FakeRepository())
    app = FastAPI()
    app.include_router(qe_archive_router.router, prefix="/api/v1")
    client = TestClient(app)

    cases = [
        ("/api/v1/qe-archive/analytics/views", None, "v_run_leaderboard"),
        ("/api/v1/qe-archive/analytics/run-leaderboard?model_type=LSTM&min_icir=0.5&limit=7&order_by=icir", "run_id", "run_1"),
        ("/api/v1/qe-archive/analytics/topk-quality?run_id=run_1&k=20&limit=6", "run_id", "run_1"),
        ("/api/v1/qe-archive/analytics/seed-robustness?min_seed_count=3&stable_only=true&limit=5", "factor_set_hash", "hash"),
        ("/api/v1/qe-archive/analytics/factor-performance?factor_name=alpha_001&min_runs=2&limit=4&order_by=avg_icir", "factor_name", "alpha_001"),
        ("/api/v1/qe-archive/analytics/model-hyperparam-seed-perf?model_type=LSTM&limit=6", "model_type", "LSTM"),
        ("/api/v1/qe-archive/analytics/overfit-flags?limit=3", "run_id", "run_2"),
        ("/api/v1/qe-archive/analytics/promotion-candidates?min_seed_count=5&limit=8&order_by=ir_mean", "factor_set_hash", "hash"),
        ("/api/v1/qe-archive/analytics/evolution-lineage?task_id=task_1&limit=9", "task_id", "task_1"),
    ]
    for url, key, expected in cases:
        response = client.get(url)
        assert response.status_code == 200
        data = response.json()["data"]
        if key is None:
            assert data[0]["view_name"] == expected
        else:
            assert data[0][key] == expected

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
    assert event.payload["routing_class"] == "archive"


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

        def claim_outbox_events(self, *, worker_id, limit, event_types, routing_class):  # type: ignore[no-untyped-def]
            self.claim_event_types = tuple(event_types)
            assert routing_class == "archive"
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


def test_repository_claim_outbox_events_filters_archive_routing_class() -> None:
    executed: list[tuple[str, list[object] | None]] = []

    class FakeCursor:
        description = []

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, sql, params=None):  # type: ignore[no-untyped-def]
            executed.append((sql, params))

        def fetchall(self):  # type: ignore[no-untyped-def]
            return []

    class FakeConnection:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def cursor(self):  # type: ignore[no-untyped-def]
            return FakeCursor()

    repository = QEArchiveRepository(connection_provider=lambda: FakeConnection())
    repository.claim_outbox_events(worker_id="worker_1", limit=3, event_types=("qe.loop.completed",))

    sql, params = executed[-1]
    assert "AND event_type = ANY(%s)" in sql
    assert "payload->>'routing_class' = %s OR NOT (payload ? 'routing_class')" in sql
    assert params == [["qe.loop.completed"], "archive", 3, "worker_1"]


def test_repository_claim_outbox_events_can_filter_source_systems() -> None:
    executed: list[tuple[str, list[object] | None]] = []

    class FakeCursor:
        description = []

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, sql, params=None):  # type: ignore[no-untyped-def]
            executed.append((sql, params))

        def fetchall(self):  # type: ignore[no-untyped-def]
            return []

    class FakeConnection:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def cursor(self):  # type: ignore[no-untyped-def]
            return FakeCursor()

    repository = QEArchiveRepository(connection_provider=lambda: FakeConnection())
    repository.claim_outbox_events(
        worker_id="worker_1",
        limit=5,
        source_systems=("paper_v2.daemon", "paper_v2"),
        routing_class=None,
    )

    sql, params = executed[-1]
    assert "AND source_system = ANY(%s)" in sql
    assert "payload->>'routing_class' = %s" not in sql
    assert params == [["paper_v2.daemon", "paper_v2"], 5, "worker_1"]


def test_outbox_skipped_status_is_not_claimed_or_counted_as_active() -> None:
    claim_source = inspect.getsource(QEArchiveRepository.claim_outbox_events)
    summary_source = inspect.getsource(QEArchiveRepository.get_archive_summary)

    assert "WHERE status = 'pending'" in claim_source
    assert "status = 'pending'" in summary_source
    assert "status IN ('pending', 'processing')" in summary_source
    assert "status = 'skipped'" not in claim_source
    assert "status IN ('pending', 'processing', 'skipped')" not in summary_source


def test_worker_skips_paper_v2_archive_events_with_audit_reason() -> None:
    """PaperV2 archive handler registration is deferred because paper_v2 is
    throwaway debug data; those archive rows must become audited skips instead
    of permanent pending blackholes.
    """

    event = ClaimedOutboxEvent(
        event_id="evt_paper",
        event_type="paper.portfolio_run.completed",
        source_system="paper_v2",
        source_id="prun_1",
        source_sub_id="prun_1",
        payload={"routing_class": "archive", "run_id": "prun_1"},
    )

    class FakeRepository:
        def __init__(self) -> None:
            self.claims: list[dict] = []
            self.skipped: list[tuple[ClaimedOutboxEvent, str, str]] = []

        def claim_outbox_events(self, **kwargs):  # type: ignore[no-untyped-def]
            self.claims.append(dict(kwargs))
            if set(kwargs.get("source_systems") or ()) >= {"paper_v2.daemon", "paper_v2"}:
                return [event]
            return []

        def skip_outbox_event(self, event, *, reason_code, trigger_reason="realtime"):  # type: ignore[no-untyped-def]
            self.skipped.append((event, reason_code, trigger_reason))

    repository = FakeRepository()
    worker = QEArchiveWorker(
        repository=repository,  # type: ignore[arg-type]
        enabled=True,
        handlers={"qe.loop.completed": lambda item: ArchiveWorkerEventResult(success=True)},
    )

    result = worker.run_once(limit=2)

    assert result.claimed == 1
    assert result.completed == 0
    assert result.failed == 0
    assert result.skipped == 1
    assert repository.claims[1]["routing_class"] is None
    assert set(repository.claims[1]["source_systems"]) >= {"paper_v2.daemon", "paper_v2"}
    assert repository.skipped == [(event, "paper_v2_archive_deferred_throwaway", "realtime")]


def test_worker_skips_unsupported_paper_outbox_events_loudly() -> None:
    event = ClaimedOutboxEvent(
        event_id="evt_unknown",
        event_type="paper_v2.coldstart_sentinel",
        source_system="paper_v2",
        source_id="run_1",
        source_sub_id="fill_1",
        payload={"routing_class": "telemetry"},
    )

    class FakeRepository:
        def __init__(self) -> None:
            self.skipped: list[tuple[str, str]] = []

        def claim_outbox_events(self, **kwargs):  # type: ignore[no-untyped-def]
            if set(kwargs.get("source_systems") or ()) >= {"paper_v2.daemon", "paper_v2"}:
                return [event]
            return []

        def skip_outbox_event(self, event, *, reason_code, trigger_reason="realtime"):  # type: ignore[no-untyped-def]
            self.skipped.append((event.event_id, reason_code))

    repository = FakeRepository()
    result = QEArchiveWorker(
        repository=repository,  # type: ignore[arg-type]
        enabled=True,
        handlers={"qe.loop.completed": lambda item: ArchiveWorkerEventResult(success=True)},
    ).run_once(limit=1)

    assert result.skipped == 1
    assert repository.skipped == [("evt_unknown", "unsupported_outbox_event_type")]


def test_repository_skip_outbox_event_writes_skip_history_and_terminal_status() -> None:
    executed: list[tuple[str, object]] = []

    class FakeCursor:
        description = [("table_name",), ("column_name",)]
        rowcount = 1

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, sql, params=None):  # type: ignore[no-untyped-def]
            executed.append((sql, params))
            if "UPDATE qe_archive.outbox_event" in sql:
                self.rowcount = 1

        def fetchall(self):  # type: ignore[no-untyped-def]
            last_sql = executed[-1][0]
            if "information_schema.columns" in last_sql:
                return [
                    ("skip_registry", "source_system"),
                    ("skip_registry", "source_type"),
                    ("skip_registry", "source_id"),
                    ("skip_registry", "source_sub_id"),
                    ("skip_registry", "event_type"),
                    ("skip_registry", "archive_policy"),
                    ("skip_registry", "archive_policy_source"),
                    ("skip_registry", "skip_reason"),
                    ("skip_registry", "trigger_reason"),
                    ("skip_registry", "metadata"),
                    ("ingest_history", "source_system"),
                    ("ingest_history", "source_type"),
                    ("ingest_history", "source_id"),
                    ("ingest_history", "source_sub_id"),
                    ("ingest_history", "trigger_reason"),
                    ("ingest_history", "archive_policy"),
                    ("ingest_history", "ingest_status"),
                    ("ingest_history", "stats"),
                    ("ingest_history", "error_message"),
                    ("outbox_event", "event_id"),
                    ("outbox_event", "status"),
                    ("outbox_event", "error_message"),
                    ("outbox_event", "locked_by"),
                    ("outbox_event", "locked_at"),
                ]
            if "ck_qear_ingest_trigger" in last_sql:
                return [
                    (
                        "CHECK ((trigger_reason = ANY (ARRAY['realtime'::text, "
                        "'backfill'::text, 'retry'::text, 'manual'::text, "
                        "'rebootstrap'::text])))",
                    )
                ]
            return []

    class FakeConnection:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def cursor(self):  # type: ignore[no-untyped-def]
            return FakeCursor()

    event = ClaimedOutboxEvent(
        event_id="evt_paper",
        event_type="paper.portfolio_run.completed",
        source_system="paper_v2",
        source_id="prun_1",
        source_sub_id="prun_1",
        payload={"routing_class": "archive"},
    )
    repository = QEArchiveRepository(connection_provider=lambda: FakeConnection())

    repository.skip_outbox_event(
        event,
        reason_code="paper_v2_archive_deferred_throwaway",
        trigger_reason="realtime",
    )

    sql_text = "\n".join(sql for sql, _ in executed)
    assert "INSERT INTO qe_archive.skip_registry" in sql_text
    assert "INSERT INTO qe_archive.ingest_history" in sql_text
    assert "UPDATE qe_archive.outbox_event" in sql_text
    assert "SET status = 'skipped'" in sql_text
    flattened_params = repr([params for _, params in executed])
    assert "paper_v2_throwaway_policy" in flattened_params
    assert "paper_v2_archive_deferred_throwaway" in flattened_params
    assert "skipped" in flattened_params


def test_worker_service_archives_loop_outbox_event_through_backfill_handler() -> None:
    event = ClaimedOutboxEvent(
        event_id="evt_loop",
        event_type="qe.loop.completed",
        source_system="qe",
        source_id="task_1",
        source_sub_id="loop_1",
        payload={"loop_index": 2},
    )

    class FakeRepository:
        def __init__(self) -> None:
            self.completed_events: list[str] = []
            self.completed_jobs: list[tuple[str, str | None, dict]] = []

        def claim_outbox_events(self, **kwargs):  # type: ignore[no-untyped-def]
            assert kwargs["event_types"] == (
                "qe.loop.completed",
                "qe.experiment.completed",
                "qe.multi_alpha.combine.completed",
            )
            return [event]

        def create_archive_job(self, job):  # type: ignore[no-untyped-def]
            assert job.event_id == "evt_loop"
            return "job_loop"

        def complete_archive_job(self, job_id, *, run_id=None, stats=None):  # type: ignore[no-untyped-def]
            self.completed_jobs.append((job_id, run_id, dict(stats or {})))

        def complete_outbox_event(self, event_id):  # type: ignore[no-untyped-def]
            self.completed_events.append(event_id)

        def fail_archive_job(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("should not fail job")

        def fail_outbox_event(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("should not retry outbox")

    class FakeBackfillService:
        def __init__(self) -> None:
            self.kwargs = None

        def archive_loop_completed(self, **kwargs):  # type: ignore[no-untyped-def]
            self.kwargs = kwargs
            return {
                "processed_count": 1,
                "results": [{"run_id": "run_loop_1", "quality": {"passed": True}}],
            }

    repository = FakeRepository()
    backfill_service = FakeBackfillService()
    result = QEArchiveWorkerService(
        repository=repository,  # type: ignore[arg-type]
        backfill_service=backfill_service,  # type: ignore[arg-type]
        enabled=True,
    ).run_once(limit=1)

    assert result["claimed"] == 1
    assert result["completed"] == 1
    assert result["failed"] == 0
    assert backfill_service.kwargs == {"task_id": "task_1", "loop_id": "loop_1", "loop_index": 2}
    assert repository.completed_events == ["evt_loop"]
    assert repository.completed_jobs[0][0] == "job_loop"
    assert repository.completed_jobs[0][1] == "run_loop_1"


def test_qe_archive_worker_api_requires_confirmation(monkeypatch) -> None:
    class FakeWorkerService:
        called = False

        def run_once(self, *, limit):  # type: ignore[no-untyped-def]
            self.called = True
            return {"claimed": limit}

    fake = FakeWorkerService()
    monkeypatch.setattr(qe_archive_router, "get_worker_service", lambda **kwargs: fake)
    app = FastAPI()
    app.include_router(qe_archive_router.router, prefix="/api/v1")
    client = TestClient(app)

    response = client.post("/api/v1/qe-archive/worker/run-once", json={"limit": 1})

    assert response.status_code == 400
    assert WORKER_CONFIRM_TEXT in response.json()["detail"]
    assert fake.called is False


def test_qe_archive_worker_api_returns_worker_report(monkeypatch) -> None:
    class FakeWorkerService:
        def run_once(self, *, limit):  # type: ignore[no-untyped-def]
            assert limit == 3
            return {"claimed": 3, "completed": 2, "failed": 1, "skipped_reason": None}

    monkeypatch.setattr(qe_archive_router, "get_worker_service", lambda **kwargs: FakeWorkerService())
    app = FastAPI()
    app.include_router(qe_archive_router.router, prefix="/api/v1")
    client = TestClient(app)

    response = client.post(
        "/api/v1/qe-archive/worker/run-once",
        json={"limit": 3, "confirm_run": WORKER_CONFIRM_TEXT},
    )

    assert response.status_code == 200
    assert response.json()["data"]["completed"] == 2


def test_qe_archive_resource_phase_api_forwards_bounded_filters(monkeypatch) -> None:
    captured = {}

    class FakeResourceService:
        def list_resource_phases(self, **kwargs):  # type: ignore[no-untyped-def]
            captured.update(kwargs)
            return [{"session_id": "qers_1", "phases": []}]

    monkeypatch.setattr(qe_archive_router, "QEResourcePhaseService", lambda: FakeResourceService())
    app = FastAPI()
    app.include_router(qe_archive_router.router, prefix="/api/v1")
    client = TestClient(app)

    response = client.get(
        "/api/v1/qe-archive/resource-phases",
        params={"task_id": "qe_task", "loop_index": 2, "source_run_key": "qe_task_L2", "limit": 25},
    )

    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert captured == {
        "run_id": None,
        "task_id": "qe_task",
        "loop_index": 2,
        "source_run_key": "qe_task_L2",
        "limit": 25,
    }


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
