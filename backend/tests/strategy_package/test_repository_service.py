from __future__ import annotations

import logging
from datetime import date, datetime, timezone

import psycopg2
from backend.services.strategy_package.manifest import compute_manifest_json_sha256, freeze_manifest
from backend.services.strategy_package.metrics_summary import metrics_summary_from_record
from backend.services.strategy_package.model_state import ModelRetrainJobStatus, ModelStalenessStatus, StrategyPackageModelState
from backend.services.strategy_package.models import LiveApprovalStatus, PackageStatus, StrategyPackageLiveApproval
from backend.services.strategy_package.package_asset import StrategyPackageAssetType
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository, StrategyPackageRepository
from backend.services.strategy_package.runtime_variant import RuntimeVariantKind, RuntimeVariantValidationStatus
from backend.services.strategy_package.service import LIVE_STRICT_GOVERNANCE_LIMIT, StrategyPackageService
from backend.services.strategy_package.validation_run import (
    PackageValidationRetrainMode,
    PackageValidationStatus,
    PackageValidationType,
)
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, RuntimeConfigInvalidError, StrategyPackageValidationError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest

import pytest


def _record_passed_original_retest(
    service: StrategyPackageService,
    package_id: str,
    *,
    completed_at: datetime | None = None,
) -> None:
    service.create_validation_run(
        package_id,
        validation_type=PackageValidationType.ORIGINAL_FIXED_WEIGHT,
        retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
        status=PackageValidationStatus.PASSED,
        metrics_json={"annual_return": 0.12, "max_drawdown": -0.08},
        artifact_manifest_json={"artifact_sha256": "sha256:unit-test-original-retest"},
        evidence_json={
            "commands": ["pytest synthetic original retest"],
            "mode": "A",
            "regime_metrics": {
                "bull": {"annual_return": 0.101},
                "bear": {"annual_return": 0.102},
            },
        },
        completed_at=completed_at or datetime.now(timezone.utc),
        created_by="unit_test",
    )


def _record_stable_seed_runs(
    service: StrategyPackageService,
    package_id: str,
    *,
    completed_at: datetime,
) -> None:
    for seed, annual_return in ((101, 0.101), (202, 0.102)):
        service.create_validation_run(
            package_id,
            validation_type=PackageValidationType.ORIGINAL_RETRAIN,
            retrain_mode=PackageValidationRetrainMode.FIXED_SEED_RETRAIN,
            seed_policy="fixed",
            random_seed=seed,
            metrics_json={"annual_return": annual_return},
            artifact_manifest_json={"artifact_sha256": f"sha256:seed-{seed}"},
            evidence_json={
                "regime_metrics": {
                    "bull": {"annual_return": annual_return},
                    "bear": {"annual_return": annual_return + 0.0001},
                }
            },
            completed_at=completed_at,
            created_by="unit_test",
        )


def _record_runtime_candidate(service: StrategyPackageService, package_id: str) -> None:
    variant = service.create_runtime_variant(
        package_id,
        variant_name="risk cap",
        variant_kind=RuntimeVariantKind.RISK_POLICY,
        variant_config={"risk_policy": {"max_position_weight": 0.04}},
        created_by="unit_test",
    )
    service.mark_runtime_variant_validation(
        package_id,
        variant.variant_id,
        validation_status=RuntimeVariantValidationStatus.VALIDATION_PASSED,
        paper_candidate=True,
        validation_evidence={"validation_run_id": "vr_candidate", "status": "passed"},
    )


def _seed_paper_ready_package(service: StrategyPackageService, package_id: str) -> None:
    completed_at = datetime.now(timezone.utc)
    service.record_package_asset(
        package_id,
        asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
        asset_ref="weights/frozen.pkl",
        asset_sha256="sha256:weights",
    )
    _record_runtime_candidate(service, package_id)
    _record_passed_original_retest(service, package_id, completed_at=completed_at)
    _record_stable_seed_runs(service, package_id, completed_at=completed_at)


def test_strategy_package_repository_persists_frozen_manifest_and_status_flow() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED}))
    saved = repo.save_manifest(manifest)

    service = StrategyPackageService(repository=repo)
    selected = service.enable_selection(saved.package_id)
    _seed_paper_ready_package(service, saved.package_id)
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
    lifecycle_events = repo.list_status_events(saved.package_id)
    assert (lifecycle_events[1].from_status, lifecycle_events[1].to_status) == (
        PackageStatus.BACKTEST_APPROVED,
        PackageStatus.SELECTION_ENABLED,
    )
    assert (lifecycle_events[2].from_status, lifecycle_events[2].to_status) == (
        PackageStatus.SELECTION_ENABLED,
        PackageStatus.PAPER_ENABLED,
    )


def test_enable_paper_does_not_validate_manifest_minute_runtime_asset() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest(algo_code="V24_PLAN").model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)
    _seed_paper_ready_package(service, manifest.package_id)

    paper = service.enable_paper(manifest.package_id)

    assert paper.package_status == PackageStatus.PAPER_ENABLED


def test_enable_paper_rejects_draft_direct_transition_with_context() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.DRAFT})
    )
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        service.enable_paper(manifest.package_id)

    err = exc_info.value
    assert err.context["package_id"] == manifest.package_id
    assert err.context["from_status"] == PackageStatus.DRAFT.value
    assert err.context["to_status"] == PackageStatus.PAPER_ENABLED.value
    assert err.context["allowed_from"] == [
        PackageStatus.BACKTEST_APPROVED.value,
        PackageStatus.SELECTION_ENABLED.value,
    ]
    assert repo.get(manifest.package_id).package_status == PackageStatus.DRAFT
    assert [event.reason for event in repo.list_status_events(manifest.package_id)] == ["package_created"]


def test_enable_paper_does_not_require_live_strict_governance_ready() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)
    _record_passed_original_retest(service, manifest.package_id)

    governance = service.governance_eligibility(manifest.package_id)
    paper = service.enable_paper(manifest.package_id)

    assert governance["live_strict_ready"] is False
    assert governance["does_not_block_paper_simulation"] is True
    assert "protected_asset_ledger_missing" in governance["blockers"]
    assert paper.package_status == PackageStatus.PAPER_ENABLED
    assert [event.reason for event in repo.list_status_events(manifest.package_id)][-1] == "enable_paper"


def test_paper_simulation_admission_uses_large_governance_history_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)
    observed: dict[str, object] = {}

    def fake_governance_eligibility(
        package_id: str,
        *,
        metric_key: str = "annual_return",
        limit: int = 500,
    ) -> dict[str, object]:
        observed.update({"package_id": package_id, "metric_key": metric_key, "limit": limit})
        return {"paper_ready": False, "blockers": ["seed_stability=INSUFFICIENT_EVIDENCE"]}

    monkeypatch.setattr(service, "governance_eligibility", fake_governance_eligibility)

    admission = service.paper_simulation_admission(manifest.package_id)

    assert admission["paper_simulation_allowed"] is True
    assert admission["warnings"] == ["live_strict_governance:seed_stability=INSUFFICIENT_EVIDENCE"]
    assert observed == {
        "package_id": manifest.package_id,
        "metric_key": "annual_return",
        "limit": LIVE_STRICT_GOVERNANCE_LIMIT,
    }


def test_enable_paper_allows_missing_original_fixed_weight_retest_as_warning() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    admission = service.paper_simulation_admission(manifest.package_id)
    paper = service.enable_paper(manifest.package_id)

    assert admission["paper_simulation_allowed"] is True
    assert "live_strict_governance:original_fixed_weight_retest_missing_passed_run_for_current_manifest" in admission["warnings"]
    assert paper.package_status == PackageStatus.PAPER_ENABLED


def test_governance_reports_failed_original_retest_but_enable_paper_still_marks_intent() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)
    failed = service.create_validation_run(
        manifest.package_id,
        validation_type=PackageValidationType.ORIGINAL_FIXED_WEIGHT,
        retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
        status=PackageValidationStatus.FAILED,
        evidence_json={"reason": "nav drift exceeded tolerance"},
        completed_at=datetime.now(timezone.utc),
        created_by="unit_test",
    )

    context = service.governance_eligibility(manifest.package_id)
    paper = service.enable_paper(manifest.package_id)

    original_retest = context["original_fixed_weight_retest"]
    assert context["live_strict_ready"] is False
    assert context["does_not_block_paper_simulation"] is True
    assert "original_fixed_weight_retest_missing_passed_run_for_current_manifest" in context["blockers"]
    assert original_retest["required_validation_type"] == PackageValidationType.ORIGINAL_FIXED_WEIGHT.value
    assert original_retest["required_status"] == PackageValidationStatus.PASSED.value
    assert original_retest["same_manifest_run_count"] == 1
    assert original_retest["observed_original_fixed_weight_runs"] == [
        {
            "validation_run_id": failed.validation_run_id,
            "status": PackageValidationStatus.FAILED.value,
            "manifest_sha256": manifest.manifest_sha256,
            "manifest_matches": True,
            "completed_at": failed.completed_at.isoformat(),
            "created_by": "unit_test",
        }
    ]
    assert paper.package_status == PackageStatus.PAPER_ENABLED


def test_governance_does_not_fall_back_to_latest_fixed_weight_validation() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)
    service.create_validation_run(
        manifest.package_id,
        validation_type=PackageValidationType.LATEST_FIXED_WEIGHT,
        retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
        target_data_version="qlib_2026q2_latest",
        status=PackageValidationStatus.PASSED,
        metrics_json={"annual_return": 0.2},
        artifact_manifest_json={"artifact_sha256": "sha256:latest-only"},
        evidence_json={"commands": ["pytest synthetic latest validation"]},
        completed_at=datetime.now(timezone.utc),
        created_by="unit_test",
    )

    context = service.governance_eligibility(manifest.package_id)
    paper = service.enable_paper(manifest.package_id)

    original_retest = context["original_fixed_weight_retest"]
    assert original_retest["required_validation_type"] == PackageValidationType.ORIGINAL_FIXED_WEIGHT.value
    assert original_retest["same_manifest_run_count"] == 0
    assert original_retest["observed_original_fixed_weight_runs"] == []
    assert context["does_not_block_paper_simulation"] is True
    assert paper.package_status == PackageStatus.PAPER_ENABLED


def test_enable_paper_finds_passed_original_retest_even_after_many_newer_runs() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)
    service.record_package_asset(
        manifest.package_id,
        asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
        asset_ref="weights/frozen.pkl",
        asset_sha256="sha256:weights",
    )
    _record_runtime_candidate(service, manifest.package_id)
    passed = service.create_validation_run(
        manifest.package_id,
        validation_type=PackageValidationType.ORIGINAL_FIXED_WEIGHT,
        retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
        status=PackageValidationStatus.PASSED,
        metrics_json={"annual_return": 0.12, "max_drawdown": -0.08},
        artifact_manifest_json={"artifact_sha256": "sha256:unit-test-original-retest"},
        evidence_json={"commands": ["pytest synthetic original retest"], "mode": "A"},
        completed_at=datetime.now(timezone.utc),
        created_by="unit_test",
    )
    for index in range(100):
        service.create_validation_run(
            manifest.package_id,
            validation_type=PackageValidationType.ORIGINAL_FIXED_WEIGHT,
            retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
            status=PackageValidationStatus.REQUESTED,
            created_by=f"noise_{index}",
        )
    _record_stable_seed_runs(service, manifest.package_id, completed_at=datetime.now(timezone.utc))

    report = service.governance_eligibility(manifest.package_id)
    paper = service.enable_paper(manifest.package_id)

    assert report["original_fixed_weight_retest"]["matching_passed_run_id"] == passed.validation_run_id
    assert report["original_fixed_weight_retest"]["same_manifest_run_count"] == 101
    assert paper.package_status == PackageStatus.PAPER_ENABLED


def test_qe_source_payload_warns_on_malformed_result_metrics(caplog: pytest.LogCaptureFixture) -> None:
    row = {
        "experiment_id": "exp_bad_metrics",
        "result_metrics": "{\"annual_return\":",
    }

    with caplog.at_level(logging.WARNING, logger="backend.services.strategy_package.service"):
        payload = StrategyPackageService._qe_source_payload(row, source_kind="qe_experiment")

    assert payload["experiment_id"] == "exp_bad_metrics"
    assert payload["metrics_summary"]["annual_return"] is None
    assert "Failed to parse JSON-like strategy package value" in caplog.text
    assert "value_snippet" in caplog.text


def test_postgres_repository_wraps_status_event_sequence_collision(monkeypatch) -> None:
    repo = StrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    state = {
        "autocommit": True,
        "commits": 0,
        "rollbacks": 0,
    }

    class Cursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            if "INSERT INTO strategy_pkg.package_status_event" in sql:
                raise psycopg2.errors.UniqueViolation("duplicate event_id")

    class Conn:
        @property
        def autocommit(self):
            return state["autocommit"]

        @autocommit.setter
        def autocommit(self, value):
            state["autocommit"] = value

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self, *args, **kwargs):
            return Cursor()

        def commit(self):
            state["commits"] += 1

        def rollback(self):
            state["rollbacks"] += 1

    monkeypatch.setattr(repo, "get", lambda package_id: manifest_to_record(manifest))
    monkeypatch.setattr(repo, "_conn_factory", lambda: Conn())

    with pytest.raises(InvalidStateTransitionError, match="status event sequence is behind"):
        repo.transition_status(
            package_id=manifest.package_id,
            to_status=PackageStatus.PAPER_ENABLED,
            allowed_from={PackageStatus.BACKTEST_APPROVED},
            reason="enable_paper",
        )
    assert state == {
        "autocommit": True,
        "commits": 0,
        "rollbacks": 1,
    }


def test_postgres_repository_commits_status_transition_atomically(monkeypatch) -> None:
    repo = StrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    state = {
        "autocommit": True,
        "commits": 0,
        "rollbacks": 0,
        "sql": [],
    }

    class Cursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            state["sql"].append(sql)

    class Conn:
        @property
        def autocommit(self):
            return state["autocommit"]

        @autocommit.setter
        def autocommit(self, value):
            state["autocommit"] = value

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self, *args, **kwargs):
            return Cursor()

        def commit(self):
            state["commits"] += 1

        def rollback(self):
            state["rollbacks"] += 1

    monkeypatch.setattr(repo, "get", lambda package_id: manifest_to_record(manifest))
    monkeypatch.setattr(repo, "_conn_factory", lambda: Conn())

    repo.transition_status(
        package_id=manifest.package_id,
        to_status=PackageStatus.PAPER_ENABLED,
        allowed_from={PackageStatus.BACKTEST_APPROVED},
        reason="enable_paper",
    )

    assert state["autocommit"] is True
    assert state["commits"] == 1
    assert state["rollbacks"] == 0
    assert any("UPDATE strategy_pkg.package" in sql for sql in state["sql"])
    assert any("INSERT INTO strategy_pkg.package_status_event" in sql for sql in state["sql"])


def manifest_to_record(manifest):
    saved = InMemoryStrategyPackageRepository().save_manifest(manifest)
    return saved


def _legacy_schema_manifest_sha(record) -> str:
    payload = record.current_manifest().model_dump(mode="json")
    for key in ("source_evidence", "backtest_context"):
        if payload.get(key) == {}:
            payload.pop(key)
    return compute_manifest_json_sha256(payload)


def _force_schema_evolution_hash_drift(repo: InMemoryStrategyPackageRepository, package_id: str) -> tuple[str, str]:
    record = repo.records[package_id]
    current_hash = record.manifest_sha256
    legacy_hash = _legacy_schema_manifest_sha(record)
    assert legacy_hash != current_hash, "fixture must emulate pre-source_evidence/backtest_context schema hash"
    repo.records[package_id] = record.model_copy(update={"manifest_sha256": legacy_hash})
    return legacy_hash, current_hash


def test_strategy_package_repository_rejects_silent_manifest_replacement() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    changed = freeze_manifest(manifest.model_copy(update={"package_name": "changed"}))

    with pytest.raises(InvalidStateTransitionError, match="silently replaced"):
        repo.save_manifest(changed)


def test_postgres_repository_recovers_duplicate_source_version_race(monkeypatch) -> None:
    repo = StrategyPackageRepository()
    manifest = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_id": "pkg_existing",
                "package_name": "existing",
            }
        )
    )
    existing = manifest_to_record(manifest)
    duplicate = freeze_manifest(
        make_manifest().model_copy(
            update={
                "package_id": "pkg_duplicate",
                "package_name": "duplicate",
                "package_version": manifest.package_version,
                "source": manifest.source,
            }
        )
    )
    find_calls = 0

    def fake_find_by_source_version(**kwargs):
        nonlocal find_calls
        find_calls += 1
        return None if find_calls == 1 else existing

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            if "INSERT INTO strategy_pkg.package (" in sql:
                raise psycopg2.errors.UniqueViolation("duplicate source version")

        def fetchone(self):
            return None

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self, *args, **kwargs):
            return Cursor()

    monkeypatch.setattr(repo, "find_by_source_version", fake_find_by_source_version)
    monkeypatch.setattr(repo, "_conn_factory", lambda: Conn())

    saved_again = repo.save_manifest(duplicate)

    assert saved_again.package_id == existing.package_id
    assert find_calls == 2


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
    assert enabled.paper_enabled is False
    assert service.list_execution_policies(manifest.package_id)[0].policy_id == policy.policy_id


def test_strategy_package_execution_policy_rejects_unknown_fields() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    with pytest.raises(RuntimeConfigInvalidError, match="outside the backtest contract"):
        service.create_execution_policy(
            package_id=manifest.package_id,
            policy_name="paper only override",
            policy_json={"algo_code": "TWAP", "paper_only_tail_mode": "BOOST_THEN_SUBSTITUTE"},
            source_backtest_id="bt_1",
            source_backtest_status="COMPLETED",
        )


def test_strategy_package_execution_policy_requires_successful_source_evidence() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    with pytest.raises(StrategyPackageValidationError, match="explicit successful evidence"):
        service.create_execution_policy(
            package_id=manifest.package_id,
            policy_name="failed source policy",
            policy_json=manifest.minute_execution_policy.model_dump(mode="json"),
            source_backtest_id="bt_failed",
            source_backtest_status="FAILED",
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

    enabled = service.enable_execution_policy_for_paper(manifest.package_id, policy.policy_id)
    assert enabled.paper_enabled is False


def test_strategy_package_keeps_v25_default_day_features_as_runtime_diagnostic() -> None:
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

    enabled = service.enable_execution_policy_for_paper(manifest.package_id, policy.policy_id)

    assert enabled.policy_id == policy.policy_id
    assert enabled.paper_enabled is False


def test_strategy_package_execution_policy_stores_unregistered_algo_as_runtime_config() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    policy = service.create_execution_policy(
        package_id=manifest.package_id,
        policy_name="unknown",
        policy_json={"algo_code": "NOT_A_QE_ALGO", "algo_config": {}},
        source_backtest_id="bt_1",
        source_backtest_status="COMPLETED",
        paper_enabled=True,
    )

    assert policy.algo_code == "NOT_A_QE_ALGO"
    assert policy.paper_enabled is False


def test_asset_eligibility_accepts_paper_status_as_formal_lifecycle_state() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_status": PackageStatus.PAPER_ENABLED}))
    record = repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    result = service.asset_eligibility.summarize(record)

    assert result.eligible is True
    assert result.legacy_status == PackageStatus.PAPER_ENABLED.value
    assert result.legacy_status_normalized_to == PackageStatus.PAPER_ENABLED.value
    assert result.blockers == []


def test_asset_eligibility_blocks_retired_package_only() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_status": PackageStatus.RETIRED}))
    record = repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    result = service.asset_eligibility.summarize(record)

    assert result.eligible is False
    assert "package_lifecycle" in result.blockers


def test_strategy_package_physical_delete_removes_package_center_records() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)
    policy = service.create_execution_policy(
        package_id=manifest.package_id,
        policy_name="qe default twap",
        policy_json=manifest.minute_execution_policy.model_dump(mode="json"),
        source_backtest_id="bt_1",
        source_backtest_status="COMPLETED",
    )
    service.record_package_asset(
        manifest.package_id,
        asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
        asset_ref="weights/frozen.pkl",
        asset_sha256="sha256:weights",
    )
    service.start_model_retrain(
        manifest.package_id,
        as_of_date=date(2026, 4, 26),
        lookback_days=365,
        confirm_retrain=True,
    )

    result = service.delete_package(manifest.package_id)

    assert result["package_id"] == manifest.package_id
    assert result["deleted_counts"]["package"] == 1
    assert result["deleted_counts"]["validated_execution_policy"] == 1
    assert result["deleted_counts"]["package_asset"] == 1
    assert result["deleted_counts"]["model_retrain_job"] == 1
    with pytest.raises(DataUnavailableError):
        repo.get(manifest.package_id)
    assert policy.policy_id not in repo.execution_policies


def test_strategy_package_physical_delete_blocks_live_references() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    repo.save_live_approval(
        StrategyPackageLiveApproval(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256,
            alpha_core_sha256="alpha_core",
            runtime_release_id="rel_1",
            runtime_release_sha256="rel_sha",
            runtime_profile_id="profile_1",
            runtime_profile_version_id="profile_v1",
            runtime_profile_sha256="profile_sha",
            execution_policy_id="exec_1",
            execution_policy_sha256="exec_sha",
            tail_policy_id="tail_1",
            tail_policy_sha256="tail_sha",
            target_broker_backend="minqmt_live",
            approval_status=LiveApprovalStatus.LIVE_CANDIDATE,
            sim_validation_evidence={"portfolio_id": "paper_1"},
            broker_compatibility={"ok": True},
        )
    )
    service = StrategyPackageService(repository=repo)

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        service.delete_package(manifest.package_id)

    assert "live_approvals" in exc_info.value.context["blockers"]
    assert repo.get(manifest.package_id).package_id == manifest.package_id


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

    with pytest.raises(StrategyPackageValidationError, match="explicit confirmation"):
        service.start_model_retrain(
            manifest.package_id,
            as_of_date=date(2026, 4, 26),
            lookback_days=365,
            confirm_retrain=False,
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

def test_list_quarantines_corrupt_manifest_hash():
    """list() skips a package whose stored manifest_sha256 does not match."""
    repo = InMemoryStrategyPackageRepository()
    m1 = freeze_manifest(make_manifest().model_copy(update={"package_id": "pkg-ok", "package_name": "ok"}))
    m2 = freeze_manifest(make_manifest().model_copy(update={"package_id": "pkg-bad", "package_name": "bad"}))
    repo.save_manifest(m1)
    repo.save_manifest(m2)
    # Corrupt the stored hash for pkg-bad
    repo.records["pkg-bad"] = repo.records["pkg-bad"].model_copy(update={"manifest_sha256": "0" * 64})
    records = repo.list()
    pkg_ids = [r.package_id for r in records]
    assert "pkg-ok" in pkg_ids
    assert "pkg-bad" not in pkg_ids
    assert len(records) == 1


def test_get_still_raises_on_corrupt_manifest_hash():
    """get() in PostgreSQL raises on corrupt hash (via _record_from_row quarantine=False)."""
    repo = InMemoryStrategyPackageRepository()
    m = freeze_manifest(make_manifest().model_copy(update={"package_id": "pkg", "package_name": "test"}))
    repo.save_manifest(m)
    repo.records["pkg"] = repo.records["pkg"].model_copy(update={"manifest_sha256": "0" * 64})
    # InMemory repo is lenient on get() (test double); PostgreSQL validates via _record_from_row.
    # The validate_manifest_integrity() method covers drift detection for both.
    report = repo.validate_manifest_integrity()
    assert report["drifted_count"] == 1
    assert report["drifted"][0]["package_id"] == "pkg"


def test_validate_manifest_integrity_classifies_safe_schema_evolution_drift():
    """validate_manifest_integrity marks model-default drift as A-class repairable."""
    repo = InMemoryStrategyPackageRepository()
    for i in range(3):
        m = freeze_manifest(make_manifest().model_copy(update={"package_id": f"pkg-{i}", "package_name": f"pkg-{i}"}))
        repo.save_manifest(m)
    legacy_hash, _current_hash = _force_schema_evolution_hash_drift(repo, "pkg-0")

    report = repo.validate_manifest_integrity()

    assert report["total_scanned"] == 3
    assert report["clean_count"] == 2
    assert report["drifted_count"] == 1
    drift = report["drifted"][0]
    assert drift["package_id"] == "pkg-0"
    assert "computed_sha256" in drift
    assert drift["impact"] == {
        "paper_portfolio_count": 0,
        "blocks_detail_endpoint": True,
        "excluded_from_package_list": True,
    }
    plan = drift["repair_plan"]
    assert plan["recommended_action"] == "repair_manifest_hash"
    assert plan["mutates_manifest_json"] is False
    assert plan["requires_operator_confirmation"] is True
    assert plan["confirm_stored_sha256"] == legacy_hash
    assert plan["confirm_computed_sha256"] == drift["computed_sha256"]
    assert plan["confirm_repair_classification"] == "A_schema_evolution_stale_hash"
    assert plan["classification"]["repair_allowed"] is True
    assert plan["classification"]["stored_equals_raw_manifest_json"] is True
    assert plan["classification"]["missing_current_model_default_keys"] == [
        "backtest_context",
        "source_evidence",
    ]
    assert plan["rollback_restore"] == {
        "field": "strategy_pkg.package.manifest_sha256",
        "restore_value": legacy_hash,
        "audit_event_reason": "manifest_hash_repaired",
    }


def test_validate_manifest_integrity_blocks_dirty_manifest_json_repair():
    """B-class drift is reported but never recommended for automatic repair."""
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_id": "pkg", "package_name": "pkg"}))
    repo.save_manifest(manifest)
    dirty_hash = "d" * 64
    repo.records["pkg"] = repo.records["pkg"].model_copy(update={"manifest_sha256": dirty_hash})

    report = repo.validate_manifest_integrity()

    plan = report["drifted"][0]["repair_plan"]
    assert plan["recommended_action"] == "quarantine_manual_review"
    assert plan["classification"]["classification"] == "B_manifest_json_dirty_or_unknown"
    assert plan["classification"]["repair_allowed"] is False
    assert plan["classification"]["stored_equals_raw_manifest_json"] is False


def test_validate_manifest_integrity_blocks_invalid_manifest_json_repair():
    """Invalid manifest_json drift includes an explicit quarantine repair plan."""

    repo = StrategyPackageRepository()

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def execute(self, _sql, _params=None):  # noqa: ANN001
            return None

        def fetchall(self):
            return [
                {
                    "package_id": "pkg-invalid",
                    "package_name": "pkg-invalid",
                    "package_version": "1.0.0",
                    "source_type": "qe_experiment",
                    "source_id": "qe_invalid",
                    "loop_id": None,
                    "run_id": None,
                    "package_status": "BACKTEST_APPROVED",
                    "manifest_json": {"package_id": "pkg-invalid"},
                    "manifest_sha256": "a" * 64,
                    "alpha_mode": "single_alpha",
                    "signal_domain": None,
                    "display_name": "pkg-invalid",
                    "legacy_name": None,
                    "data_vintage": None,
                    "prediction_ref_uri": None,
                    "prediction_ref_sha256": None,
                    "model_artifact_uri": None,
                    "model_artifact_sha256": None,
                    "paper_portfolio_count": 0,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                }
            ]

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def cursor(self, *args, **kwargs):  # noqa: ANN001
            return Cursor()

    repo._conn_factory = lambda: Conn()  # noqa: SLF001

    report = repo.validate_manifest_integrity()

    assert report["drifted_count"] == 1
    plan = report["drifted"][0]["repair_plan"]
    assert plan["recommended_action"] == "quarantine_manual_review"
    assert plan["classification"]["classification"] == "B_manifest_json_invalid_or_unknown"
    assert plan["classification"]["repair_allowed"] is False


def test_validate_manifest_integrity_clean_when_all_match():
    """validate_manifest_integrity returns 0 drift when all hashes match."""
    repo = InMemoryStrategyPackageRepository()
    for i in range(3):
        m = freeze_manifest(make_manifest().model_copy(update={"package_id": f"pkg-{i}", "package_name": f"pkg-{i}"}))
        repo.save_manifest(m)
    report = repo.validate_manifest_integrity()
    assert report["total_scanned"] == 3
    assert report["clean_count"] == 3
    assert report["drifted_count"] == 0
    assert report["drifted"] == []


def test_repair_manifest_hash_fixes_a_class_drift():
    """repair_manifest_hash updates only A-class schema-evolution hash drift."""
    repo = InMemoryStrategyPackageRepository()
    m = freeze_manifest(make_manifest().model_copy(update={"package_id": "pkg", "package_name": "test"}))
    repo.save_manifest(m)
    legacy_hash, correct_hash = _force_schema_evolution_hash_drift(repo, "pkg")
    report_before = repo.validate_manifest_integrity()
    assert report_before["drifted_count"] == 1

    repaired = repo.repair_manifest_hash(
        "pkg",
        operator="test_runner",
        confirm_stored_sha256=legacy_hash,
        confirm_computed_sha256=correct_hash,
    )

    assert repaired.manifest_sha256 == correct_hash
    after = repo.get("pkg")
    assert after.manifest_sha256 == correct_hash
    repair_events = [e for e in repo.events if e.reason == "manifest_hash_repaired"]
    assert len(repair_events) == 1
    assert repair_events[0].context["operator"] == "test_runner"
    assert repair_events[0].context["old_manifest_sha256"] == legacy_hash
    assert repair_events[0].context["new_manifest_sha256"] == correct_hash
    assert repair_events[0].context["repair_classification"] == "A_schema_evolution_stale_hash"
    assert repair_events[0].context["rollback_restore"] == {
        "field": "strategy_pkg.package.manifest_sha256",
        "restore_value": legacy_hash,
    }


def test_repair_manifest_hash_requires_explicit_confirmation():
    """repair_manifest_hash refuses silent hash overwrites without exact operator confirmation."""
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_id": "pkg", "package_name": "test"}))
    repo.save_manifest(manifest)
    legacy_hash, correct_hash = _force_schema_evolution_hash_drift(repo, "pkg")

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.repair_manifest_hash("pkg", operator="test_runner")

    assert exc_info.value.context["package_id"] == "pkg"
    assert exc_info.value.context["stored_sha256"] == legacy_hash
    assert exc_info.value.context["computed_sha256"] == correct_hash
    assert exc_info.value.context["repair_plan"]["rollback_restore"]["restore_value"] == legacy_hash
    assert repo.records["pkg"].manifest_sha256 == legacy_hash



def test_strategy_package_summary_listing_omits_heavy_manifest_and_runtime_contract() -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_id": "pkg_summary", "package_name": "summary"}))
    repo.save_manifest(manifest)
    service = StrategyPackageService(repository=repo)

    rows = service.list_package_summaries(limit=10)

    assert rows[0]["package_id"] == "pkg_summary"
    assert rows[0]["metrics_summary"]["package_id"] == "pkg_summary"
    assert rows[0]["asset_eligibility"] == {"eligible": True, "summary_only": True, "blockers": []}
    assert "manifest" not in rows[0]
    assert "runtime_config_contract" not in rows[0]


def test_strategy_package_summary_listing_does_not_hash_quarantine(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_id": "pkg_no_hash_on_list"}))
    repo.save_manifest(manifest)

    def fail_hash(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("summary listing must not compute manifest hash")

    monkeypatch.setattr("backend.services.strategy_package.repository.compute_manifest_sha256", fail_hash)

    rows = StrategyPackageService(repository=repo).list_package_summaries(limit=10)

    assert rows[0]["package_id"] == "pkg_no_hash_on_list"
