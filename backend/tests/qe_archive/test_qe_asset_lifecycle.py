from __future__ import annotations

from copy import deepcopy

import pytest

from backend.services.qe_archive.archive_service import QEArchiveService
from backend.services.qe_archive.asset_publisher import QEArchiveAssetPublishError
from backend.services.qe_archive.asset_lifecycle import (
    QEValueClass,
    build_business_identity,
    build_result_digest,
    classify_qe_result,
)
from backend.services.qe_archive.source_assembler import (
    _archive_status_from_policy,
    _archived_status,
)


def _payload(*, purpose: str = "research", status: str = "completed", annualized: float = 0.25) -> dict:
    return {
        "source_system": "qe_evolution",
        "source_id": "task_lifecycle",
        "source_sub_id": "task_lifecycle_Loop1",
        "task_id": "task_lifecycle",
        "loop_id": "task_lifecycle_Loop1",
        "loop_index": 1,
        "logical_experiment_id": "task_lifecycle:task_lifecycle_Loop1",
        "status": status,
        "run_type": "evolution_loop",
        "model_type": "LGBModel",
        "factor_list": ["alpha_a", "alpha_b"],
        "config": {
            "factor_list": ["alpha_a", "alpha_b"],
            "model": {"model_id": "LGBModel", "num_leaves": 31},
            "data_split": {
                "train_start": "2018-08-01",
                "train_end": "2022-12-30",
                "valid_start": "2023-01-03",
                "valid_end": "2024-06-28",
                "test_start": "2024-07-01",
                "test_end": "2026-08-31",
            },
            "execution": {"execution_algo": "TWAP", "topk": 20, "open_cost": 0.0005},
            "runtime_flags": {
                "_qe_run_registration": {
                    "schema_version": "qe_run_registration_v1",
                    "purpose": purpose,
                    "run_kind": "custom_evolution_loop",
                    "source_type": "scheduler",
                    "dataset_release_id": "qe_hmm_full_v2_20260831",
                    "dataset_manifest_sha256": "a" * 64,
                    "random_seed": 123,
                    "label_horizon": 20,
                    "universe_pool_ids": ["all_a_pit"],
                    "execution_algo": "TWAP",
                }
            },
            "data_context": {
                "freq": "1min",
                "dataset_snapshot_id": "qe_hmm_full_v2_20260831",
                "data_version_hash": "a" * 64,
                "benchmark": "000300.SH",
                "limit_suspend_authoritative": True,
            },
        },
        "metrics": {
            "annualized_return": annualized,
            "max_drawdown": -0.18,
            "information_ratio": 1.1,
            "IC": 0.031,
        },
    }


class _Repo:
    def __init__(self, survivor: str | None = None) -> None:
        self.survivor = survivor
        self.run_writes: list[str] = []
        self.raw_writes: list[object] = []
        self.artifact_writes: list[list[dict]] = []
        self.survivor_queries: list[dict] = []

    def find_lifecycle_survivor(self, **kwargs):  # type: ignore[no-untyped-def]
        self.survivor_queries.append(dict(kwargs))
        return self.survivor

    def upsert_run(self, run):  # type: ignore[no-untyped-def]
        self.run_writes.append(run.run_id)
        return run.run_id

    def upsert_run_source(self, source):  # type: ignore[no-untyped-def]
        return 1

    def upsert_run_config(self, config):  # type: ignore[no-untyped-def]
        return 1

    def upsert_reproducibility_manifest(self, manifest):  # type: ignore[no-untyped-def]
        return 1

    def upsert_artifact_manifest(self, run_id, artifacts, *, replace_existing=True):  # type: ignore[no-untyped-def]
        self.artifact_writes.append([dict(item) for item in artifacts])
        return len(artifacts)

    def upsert_data_context(self, context):  # type: ignore[no-untyped-def]
        return 1

    def upsert_account_summary(self, summary):  # type: ignore[no-untyped-def]
        return 1

    def upsert_metric_batch(self, metrics, *, replace_existing=True):  # type: ignore[no-untyped-def]
        return len(metrics)

    def replace_run_curves(self, run_id, rows):  # type: ignore[no-untyped-def]
        return len(rows)

    def replace_run_factors(self, run_id, rows):  # type: ignore[no-untyped-def]
        return len(rows)

    def replace_run_factor_importance(self, run_id, rows):  # type: ignore[no-untyped-def]
        return len(rows)

    def replace_run_symbol_summaries(self, run_id, rows):  # type: ignore[no-untyped-def]
        return len(rows)

    def replace_run_trades(self, run_id, rows):  # type: ignore[no-untyped-def]
        return len(rows)

    def replace_run_execution_events(self, run_id, rows):  # type: ignore[no-untyped-def]
        return len(rows)

    def replace_raw_payloads(self, run_id, rows):  # type: ignore[no-untyped-def]
        self.raw_writes = list(rows)
        return len(rows)


class _ModelStore:
    def resolve_archive_manifest(self, **kwargs):  # type: ignore[no-untyped-def]
        return {"status": "missing", "artifact_count": 0, "errors": []}


class _ResourcePhase:
    def bind_archive_run(self, **kwargs):  # type: ignore[no-untyped-def]
        return None


def _service(repo: _Repo) -> QEArchiveService:
    return QEArchiveService(
        repository=repo,  # type: ignore[arg-type]
        model_store_service=_ModelStore(),  # type: ignore[arg-type]
        resource_phase_service=_ResourcePhase(),  # type: ignore[arg-type]
    )


@pytest.mark.parametrize("annualized", [0.42, -0.35])
def test_valid_positive_and_negative_results_default_to_c_and_persist(annualized: float) -> None:
    repo = _Repo()
    result = _service(repo).process_payload(_payload(annualized=annualized), dry_run=False)

    lifecycle = result.stats["asset_lifecycle"]
    assert lifecycle["value_class"] == "C"
    assert lifecycle["warehouse_status"] == "persisted"
    assert lifecycle["asset_status"] == "not_required"
    assert repo.run_writes == [result.run_id]
    assert result.stats["written"] is True


@pytest.mark.parametrize(
    ("source_system", "run_type", "registered"),
    [
        ("qe", "single_experiment", False),
        ("qe", "single_experiment", True),
        ("qe_evolution", "custom_evolution_loop", True),
        ("multi_alpha", "multi_alpha_combine", True),
        ("database_custom_strategy", "database_custom_strategy", True),
    ],
)
def test_all_supported_qe_result_paths_share_the_same_archive_lifecycle(
    source_system: str,
    run_type: str,
    registered: bool,
) -> None:
    payload = _payload()
    payload["source_system"] = source_system
    payload["run_type"] = run_type
    if not registered:
        payload["config"]["runtime_flags"].pop("_qe_run_registration")

    repo = _Repo()
    result = _service(repo).process_payload(payload, dry_run=False)

    assert result.stats["asset_lifecycle"]["value_class"] == "C"
    assert result.stats["asset_lifecycle"]["warehouse_status"] == "persisted"
    assert result.stats["written"] is True
    assert len(repo.run_writes) == 1


@pytest.mark.parametrize("purpose", ["validation", "fixture", "smoke", "test"])
def test_x_purposes_create_zero_archive_rows(purpose: str) -> None:
    repo = _Repo()
    result = _service(repo).process_payload(_payload(purpose=purpose), dry_run=False)

    assert result.stats["asset_lifecycle"]["value_class"] == "X"
    assert result.stats["not_eligible"] is True
    assert result.stats["written"] is False
    assert repo.run_writes == []


def test_failed_parent_does_not_swallow_evaluable_child() -> None:
    payload = _payload(status="failed", annualized=-0.12)
    payload["parent_status"] = "failed"
    decision = classify_qe_result(payload)

    assert decision.value_class == QEValueClass.C
    assert decision.archive_eligible is True


def test_failed_child_with_evaluable_result_remains_archive_eligible_in_ui_projection() -> None:
    payload = _payload(status="failed", annualized=-0.12)

    status = _archive_status_from_policy(
        source_type="loop",
        source_id="task_lifecycle",
        source_sub_id="task_lifecycle_Loop1",
        source_status="failed",
        payload=payload,
        run_ids=[],
    )

    assert status["archive_status"] == "recommended"
    assert status["eligible"] is True
    assert status["value_class"] == "C"


@pytest.mark.parametrize(
    ("value_class", "retention_class", "asset_status"),
    [
        ("A", "protected", "pending"),
        ("B", "research_180d", "pending"),
        ("C", "research_180d", "not_required"),
    ],
)
def test_predeclared_value_class_controls_retention_not_archive_admission(
    value_class: str,
    retention_class: str,
    asset_status: str,
) -> None:
    payload = _payload(annualized=-0.35)
    payload["config"]["runtime_flags"]["_qe_run_registration"]["value_class"] = (
        value_class
    )

    decision = classify_qe_result(payload)

    assert decision.value_class.value == value_class
    assert decision.archive_eligible is True
    assert decision.retention_class == retention_class
    assert decision.asset_status.value == asset_status
    assert decision.asset_reason_code == (
        "qe_asset_publish_manifest_missing" if value_class in {"A", "B"} else None
    )


def test_exact_duplicate_uses_business_identity_and_result_digest_and_keeps_survivor() -> None:
    repo = _Repo(survivor="qear_run_survivor")
    payload = _payload()
    payload["result_digest_sha256"] = "c" * 64
    result = _service(repo).process_payload(payload, dry_run=False)

    lifecycle = result.stats["asset_lifecycle"]
    assert lifecycle["value_class"] == "X"
    assert lifecycle["duplicate_of"] == "qear_run_survivor"
    assert lifecycle["reason_code"] == "qe_lifecycle_exact_duplicate"
    assert repo.run_writes == []


def test_different_execution_policy_is_matched_comparison_not_duplicate() -> None:
    baseline = _payload()
    variant = deepcopy(baseline)
    variant["config"]["execution"]["topk"] = 50

    assert build_business_identity(baseline) != build_business_identity(variant)
    assert classify_qe_result(baseline).business_identity_sha256 != classify_qe_result(variant).business_identity_sha256


def test_same_config_different_result_is_retained_for_determinism_diagnosis() -> None:
    first = _payload(annualized=0.2)
    second = _payload(annualized=0.21)

    assert build_business_identity(first) == build_business_identity(second)
    assert build_result_digest(first) != build_result_digest(second)
    assert classify_qe_result(first).archive_eligible is True
    assert classify_qe_result(second).archive_eligible is True


def test_combination_result_is_not_projected_as_an_individual_factor_score() -> None:
    payload = _payload()
    payload["run_type"] = "multi_alpha_combine"
    payload["metrics"]["combination_return"] = 0.8
    decision = classify_qe_result(payload)

    assert decision.archive_eligible is True
    assert "factor_official_score" not in decision.as_dict()


def test_registered_formal_result_missing_dataset_identity_is_pending_not_x() -> None:
    payload = _payload()
    registration = payload["config"]["runtime_flags"]["_qe_run_registration"]
    registration.pop("dataset_release_id")
    registration.pop("dataset_manifest_sha256")
    payload["config"]["data_context"].pop("dataset_snapshot_id")
    payload["config"]["data_context"].pop("data_version_hash")

    decision = classify_qe_result(payload)
    assert decision.value_class == QEValueClass.CLASSIFICATION_PENDING
    assert decision.archive_eligible is False
    assert decision.reason_code == "qe_lifecycle_identity_incomplete"


def test_technical_invalid_daily_result_is_x_even_when_metrics_exist() -> None:
    payload = _payload()
    payload["config"]["data_context"] = {
        "freq": "day",
        "limit_suspend_authoritative": False,
    }

    decision = classify_qe_result(payload)

    assert decision.value_class == QEValueClass.X
    assert decision.reason_code == (
        "qe_lifecycle_technical_invalid_daily_backtest_without_authoritative_limit_suspend"
    )


def test_duplicate_lookup_excludes_the_idempotent_target_run() -> None:
    repo = _Repo()
    payload = _payload()
    payload["result_digest_sha256"] = "d" * 64
    result = _service(repo).process_payload(payload, dry_run=False)

    assert repo.survivor_queries == [
        {
            "business_identity_sha256": result.stats["asset_lifecycle"]["business_identity_sha256"],
            "result_digest_sha256": result.stats["asset_lifecycle"]["result_digest_sha256"],
            "exclude_run_id": result.run_id,
        }
    ]


def test_metrics_only_digest_is_not_used_for_exact_duplicate_collapse() -> None:
    repo = _Repo(survivor="qear_run_metrics_collision")

    result = _service(repo).process_payload(_payload(), dry_run=False)

    assert result.stats["asset_lifecycle"]["result_digest_authoritative"] is False
    assert repo.survivor_queries == []
    assert result.stats["written"] is True


def test_asset_publish_failure_persists_separate_failed_asset_state() -> None:
    class FailedPublisher:
        def publish(self, **kwargs):  # type: ignore[no-untyped-def]
            raise QEArchiveAssetPublishError(
                "failed",
                reason_code="qe_asset_publish_readback_failed",
            )

    repo = _Repo()
    payload = _payload()
    payload["qe_asset_publish_manifest"] = [
        {
            "artifact_type": "model_weight",
            "artifact_name": "params.pkl",
            "source_path": "unused",
            "sha256": "b" * 64,
            "size_bytes": 1,
            "media_type": "application/octet-stream",
            "logical_role": "model_weight",
            "producer_identity": "task_lifecycle:loop1",
            "source_receipt": {"node_id": "wsl2-5080"},
        }
    ]
    service = QEArchiveService(
        repository=repo,  # type: ignore[arg-type]
        model_store_service=_ModelStore(),  # type: ignore[arg-type]
        resource_phase_service=_ResourcePhase(),  # type: ignore[arg-type]
        asset_publisher=FailedPublisher(),  # type: ignore[arg-type]
    )

    with pytest.raises(QEArchiveAssetPublishError):
        service.process_payload(payload, dry_run=False)

    lifecycle_payload = next(
        item.payload_json
        for item in repo.raw_writes
        if item.payload_type == "qe_completion_payload"
    )
    assert lifecycle_payload["_qe_asset_lifecycle"]["warehouse_status"] == "persisted"
    assert lifecycle_payload["_qe_asset_lifecycle"]["asset_status"] == "failed"
    assert lifecycle_payload["_qe_asset_lifecycle"]["asset_reason_code"] == (
        "qe_asset_publish_readback_failed"
    )


def test_archived_ui_status_uses_persisted_lifecycle_instead_of_reclassification() -> None:
    status = _archived_status(
        ["qear_run_1"],
        {
            "value_class": "A",
            "warehouse_status": "persisted",
            "asset_status": "published",
            "asset_reason_code": "qe_asset_publish_complete",
            "workspace_status": "grace_period",
            "protected_owner_count": 2,
            "retention_class": "protected",
            "reason_code": "qe_lifecycle_valid_unique_a",
        },
    )

    assert status["value_class"] == "A"
    assert status["asset_status"] == "published"
    assert status["asset_reason_code"] == "qe_asset_publish_complete"
    assert status["protected_owner_count"] == 2
