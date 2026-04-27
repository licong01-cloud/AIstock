from __future__ import annotations

from datetime import date, datetime

from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.metrics_summary import metrics_summary_from_record
from backend.services.strategy_package.model_state import ModelRetrainJobStatus, ModelStalenessStatus, StrategyPackageModelState
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, StrategyPackageValidationError, UnsupportedFeatureError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest

import pytest


def test_strategy_package_repository_persists_frozen_manifest_and_status_flow() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED}))
    saved = repo.save_manifest(manifest)

    service = StrategyPackageService(repository=repo)
    selected = service.enable_selection(saved.package_id)
    paper = service.enable_paper(saved.package_id)
    repo.mark_paper_portfolio_created(saved.package_id, "paper_1")

    assert saved.manifest_sha256 == manifest.manifest_sha256
    assert selected.package_status == PackageStatus.SELECTION_ENABLED
    assert paper.package_status == PackageStatus.PAPER_ENABLED
    assert repo.get(saved.package_id).paper_portfolio_count == 1
    assert [event.reason for event in repo.list_status_events(saved.package_id)] == [
        "package_created",
        "enable_selection",
        "enable_paper",
        "paper_portfolio_created",
    ]


def test_strategy_package_repository_rejects_silent_manifest_replacement() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    changed = freeze_manifest(manifest.model_copy(update={"package_name": "changed"}))

    with pytest.raises(InvalidStateTransitionError, match="silently replaced"):
        repo.save_manifest(changed)


def test_strategy_package_execution_policy_requires_backtest_contract_and_hash() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_status": PackageStatus.PAPER_ENABLED}))
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    policy = service.create_execution_policy(
        package_id=manifest.package_id,
        policy_name="qe default twap",
        policy_json=manifest.minute_execution_policy.model_dump(mode="json"),
        source_backtest_id="bt_1",
        source_backtest_status="COMPLETED",
    )
    enabled = service.enable_execution_policy_for_paper(manifest.package_id, policy.policy_id)

    assert policy.manifest_sha256 == manifest.manifest_sha256
    assert policy.algo_code == "TWAP"
    assert policy.policy_sha256
    assert enabled.paper_enabled is True
    assert service.list_execution_policies(manifest.package_id)[0].policy_id == policy.policy_id


def test_strategy_package_execution_policy_rejects_unknown_fields() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    with pytest.raises(StrategyPackageValidationError, match="outside the backtest contract"):
        service.create_execution_policy(
            package_id=manifest.package_id,
            policy_name="paper only override",
            policy_json={"algo_code": "TWAP", "paper_only_tail_mode": "BOOST_THEN_SUBSTITUTE"},
            source_backtest_id="bt_1",
            source_backtest_status="COMPLETED",
        )


def test_strategy_package_execution_policy_accepts_registered_v25_contract_without_paper_runtime() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    policy = service.create_execution_policy(
        package_id=manifest.package_id,
        policy_name="qe v25 two stage",
        policy_json={"algo_code": "V25_TWO_STAGE", "algo_config": {"early_model_path": "missing_early.pt", "late_model_path": "missing_late.pt"}},
        source_backtest_id="bt_1",
        source_backtest_status="COMPLETED",
        paper_enabled=False,
    )

    assert policy.algo_code == "V25_TWO_STAGE"

    with pytest.raises(DataUnavailableError, match="early_model_path"):
        service.enable_execution_policy_for_paper(manifest.package_id, policy.policy_id)


def test_strategy_package_rejects_v25_default_day_features_for_paper() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    policy = service.create_execution_policy(
        package_id=manifest.package_id,
        policy_name="qe v25 diagnostic defaults",
        policy_json={
            "algo_code": "V25_TWO_STAGE",
            "algo_config": {
                "early_model_path": "missing_early.pt",
                "late_model_path": "missing_late.pt",
                "allow_default_day_features": True,
            },
        },
        source_backtest_id="bt_1",
        source_backtest_status="COMPLETED",
        paper_enabled=False,
    )

    with pytest.raises(StrategyPackageValidationError, match="diagnostic-only"):
        service.enable_execution_policy_for_paper(manifest.package_id, policy.policy_id)


def test_strategy_package_execution_policy_rejects_unregistered_algo_for_paper() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    with pytest.raises(UnsupportedFeatureError, match="not registered"):
        service.create_execution_policy(
            package_id=manifest.package_id,
            policy_name="unknown",
            policy_json={"algo_code": "NOT_A_QE_ALGO", "algo_config": {}},
            source_backtest_id="bt_1",
            source_backtest_status="COMPLETED",
            paper_enabled=True,
        )


def test_strategy_package_model_state_defaults_backtest_model_to_stale_warning() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    state = service.get_model_state(manifest.package_id, as_of_date=date(2026, 4, 26))
    preview = service.preview_model_retrain(manifest.package_id, as_of_date=date(2026, 4, 26), lookback_days=365)

    assert state.staleness_status == ModelStalenessStatus.STALE_INITIAL_BACKTEST_MODEL
    assert state.warning
    assert preview.requires_manual_confirmation is True
    assert preview.recommended_train_end_date == date(2026, 4, 26)
    assert preview.config["lookback_days"] == 365


def test_strategy_package_model_state_marks_recent_retrained_model_current() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    state = StrategyPackageModelState(
        package_id=manifest.package_id,
        active_model_version_id="model_v2",
        train_start_date=date(2025, 1, 1),
        train_end_date=date(2026, 4, 10),
        trained_at=datetime(2026, 4, 11),
        last_retrain_job_id="job_1",
        last_retrained_at=datetime(2026, 4, 11),
    )
    saved = service.upsert_model_state(state, as_of_date=date(2026, 4, 26))

    assert saved.staleness_status == ModelStalenessStatus.CURRENT
    assert saved.warning is None


def test_strategy_package_model_retrain_start_requires_confirmation() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    with pytest.raises(StrategyPackageValidationError, match="manual confirmation"):
        service.start_model_retrain(
            manifest.package_id,
            as_of_date=date(2026, 4, 26),
            lookback_days=365,
            confirm_retrain=False,
            confirm_text=manifest.package_id,
        )


def test_strategy_package_model_retrain_start_queues_job_and_marks_state_retraining() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    job = service.start_model_retrain(
        manifest.package_id,
        as_of_date=date(2026, 4, 26),
        lookback_days=365,
        config={"requested_by": "unit_test"},
        confirm_retrain=True,
        confirm_text=manifest.package_id,
    )
    jobs = service.list_model_retrain_jobs(manifest.package_id)
    state = service.get_model_state(manifest.package_id, as_of_date=date(2026, 4, 26))

    assert job.status == ModelRetrainJobStatus.QUEUED
    assert job.confirmed is True
    assert job.requested_train_end_date == date(2026, 4, 26)
    assert job.config["requested_by"] == "unit_test"
    assert job.config["executor_contract"] == "manual_or_external_worker_required"
    assert jobs[0].job_id == job.job_id
    assert state.staleness_status == ModelStalenessStatus.RETRAINING
    assert state.last_retrain_job_id == job.job_id
    assert state.last_retrained_at is None


def test_strategy_package_metrics_summary_is_display_only_and_extracts_sharpe() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = make_manifest().model_copy(
        update={
            "backtest_summary": make_manifest().backtest_summary.model_copy(
                update={
                    "raw_metrics": {
                        "IC": 0.05,
                        "Rank IC": 0.04,
                        "ICIR": 1.2,
                        "sharpe": 1.35,
                        "turnover": 0.27,
                    }
                }
            )
        }
    )
    frozen = freeze_manifest(manifest)
    record = repo.save_manifest(frozen)

    summary = metrics_summary_from_record(record)

    assert summary.package_id == frozen.package_id
    assert summary.manifest_sha256 == frozen.manifest_sha256
    assert summary.ic == 0.05
    assert summary.rank_ic == 0.04
    assert summary.sharpe == 1.35
    assert summary.turnover == 0.27
    assert "sharpe" not in summary.missing_metrics
    assert repo.get(frozen.package_id).manifest_sha256 == frozen.manifest_sha256
