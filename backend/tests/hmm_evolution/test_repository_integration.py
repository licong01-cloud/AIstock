from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from threading import Lock
import time
from typing import Any

import pytest

from backend.services.hmm_evolution import repository as repository_module
from backend.services.hmm_evolution.errors import IdempotencyConflictError
from backend.services.hmm_evolution.input_adapter import BatchExecutionInputs
from backend.services.hmm_evolution.models import (
    CandidateCoverage,
    CandidateManifest,
    CandidatePreview,
    CandidateSourceType,
    CoefficientStats,
)
from backend.services.hmm_evolution.repository import HMMEvolutionRepository
from backend.services.hmm_evolution.worker import (
    HMMEvolutionWorker,
    WorkerConfig,
)


class _ScriptedCursor:
    def __init__(self, steps: list[dict[str, Any]]) -> None:
        self.steps = list(steps)
        self.current: dict[str, Any] = {}
        self.queries: list[tuple[str, Any]] = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query: str, params: Any = None) -> None:
        normalized = " ".join(query.split())
        self.queries.append((normalized, params))
        if not self.steps:
            raise AssertionError(f"unexpected SQL: {normalized}")
        self.current = self.steps.pop(0)
        expected = self.current.get("contains")
        if expected is not None:
            assert expected in normalized
        self.rowcount = int(self.current.get("rowcount", 0))

    def fetchone(self):
        return self.current.get("one")

    def fetchall(self):
        return self.current.get("all", [])


class _Connection:
    def __init__(self, cursor: _ScriptedCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, **kwargs):
        return self._cursor


def _repository(steps: list[dict[str, Any]]) -> tuple[HMMEvolutionRepository, _ScriptedCursor]:
    cursor = _ScriptedCursor(steps)
    return HMMEvolutionRepository(lambda: _Connection(cursor)), cursor


def test_repository_default_connection_is_an_atomic_managed_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    @contextmanager
    def fake_get_conn(**kwargs: Any):
        calls.append(kwargs)
        yield object()

    monkeypatch.setattr(repository_module, "get_conn", fake_get_conn)
    repository = HMMEvolutionRepository()

    with repository._conn_factory():  # noqa: SLF001 - verifies the durable DB boundary.
        pass

    assert calls == [{"autocommit": False, "manage_transaction": True}]


def _preview(
    *,
    source_type: CandidateSourceType,
    source_ref: dict[str, Any],
    artifact_uri: str,
) -> CandidatePreview:
    manifest = CandidateManifest(
        source_type=source_type,
        source_ref=source_ref,
        artifact_uri=artifact_uri,
        artifact_sha256="a" * 64,
        size_bytes=100,
        detected_format="hmm_sector_coefficients_legacy_v1",
        coverage=CandidateCoverage(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 2),
            date_count=2,
            sector_count_min=1,
            sector_count_max=2,
            stock_sector_map_count=2,
        ),
        coefficient_stats=CoefficientStats(min=0.9, max=1.2),
    )
    return CandidatePreview(
        candidate_id=manifest.candidate_id,
        manifest_hash=manifest.manifest_hash,
        manifest=manifest,
    )


def _candidate_row(preview: CandidatePreview) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "candidate_id": preview.candidate_id,
        "manifest_hash": preview.manifest_hash,
        "display_name": "candidate",
        "description": None,
        "source_type": preview.manifest.source_type.value,
        "source_ref": dict(preview.manifest.source_ref),
        "artifact_manifest": preview.manifest.model_dump(mode="json"),
        "algorithm_version": preview.manifest.algorithm_version,
        "lifecycle_status": "research_only",
        "invalid_reason_code": None,
        "invalid_context": None,
        "created_by": "tester",
        "row_version": 1,
        "created_at": now,
        "updated_at": now,
        "retired_at": None,
    }


def test_cross_source_alias_is_accepted_after_atomic_candidate_conflict() -> None:
    primary = _preview(
        source_type=CandidateSourceType.CONFIGURED_LOCAL,
        source_ref={"root_alias": "research", "relative_path": "candidate.json"},
        artifact_uri="configured-local://research/candidate.json",
    )
    alias = _preview(
        source_type=CandidateSourceType.EXISTING_SNAPSHOT,
        source_ref={"snapshot_id": "snapshot-1", "artifact_name": "candidate.json"},
        artifact_uri="snapshot://snapshot-1/candidate.json",
    )
    assert primary.candidate_id == alias.candidate_id
    assert primary.manifest_hash != alias.manifest_hash
    existing = _candidate_row(primary)
    updated = dict(existing)
    updated["source_ref"] = {
        **existing["source_ref"],
        "aliases": [alias.manifest.source_ref],
    }
    updated["row_version"] = 2
    repository, cursor = _repository(
        [
            {"contains": "ON CONFLICT DO NOTHING", "one": None},
            {"contains": "WHERE candidate_id = %s OR manifest_hash = %s", "one": existing},
            {"contains": "UPDATE hmm_evolution.candidate", "one": updated},
        ]
    )

    candidate, created = repository.register_candidate(
        alias,
        display_name="candidate alias",
        description=None,
        created_by="tester",
    )

    assert created is False
    assert candidate.manifest_hash == primary.manifest_hash
    assert candidate.source_ref["aliases"] == (alias.manifest.source_ref,)
    assert cursor.steps == []


def test_evaluation_create_or_get_uses_atomic_unique_insert() -> None:
    existing = {
        "eval_id": "hmme_existing",
        "logical_evaluation_key": "a" * 64,
        "run_generation": 1,
        "status": "queued",
    }
    repository, cursor = _repository(
        [
            {"contains": "ON CONFLICT (logical_evaluation_key, run_generation) DO NOTHING", "one": None},
            {"contains": "ORDER BY run_generation DESC", "one": existing},
        ]
    )

    row, created = repository.create_or_get_evaluation(
        candidate_id="hmmc_test",
        logical_evaluation_key="a" * 64,
        base_loop_ref="qe_task/Loop8",
        source_manifest={"source": "test"},
        source_manifest_hash="b" * 64,
        candidate_manifest_hash="c" * 64,
        evaluation_spec={"window": "test"},
        evaluation_spec_hash="d" * 64,
        evaluator_version="v1",
        as_of_date=date(2026, 4, 15),
        window_start=date(2026, 1, 1),
        window_end=date(2026, 3, 31),
        label_horizon_days=20,
        universe_id="prediction_artifact_all",
        universe_hash="e" * 64,
        topk=50,
    )

    assert created is False
    assert row["eval_id"] == "hmme_existing"
    assert cursor.steps == []


def test_batch_atomic_insert_preserves_idempotency_conflict_semantics() -> None:
    repository, cursor = _repository(
        [
            {"contains": "ON CONFLICT DO NOTHING", "one": None},
            {
                "contains": "WHERE idempotency_key = %s",
                "one": {"batch_id": "hmmb_existing", "request_hash": "f" * 64},
            },
        ]
    )

    with pytest.raises(IdempotencyConflictError, match="different request"):
        repository.create_or_get_batch(
            request_hash="a" * 64,
            items=[
                {
                    "candidate_id": "hmmc_test",
                    "eval_id": "hmme_test",
                    "item_status": "queued",
                }
            ],
            recommendation_spec={"schema_version": "v1"},
            recommendation_version="v1",
            created_by="tester",
            idempotency_key="same-key",
        )
    assert cursor.steps == []


def test_recompute_shared_batch_uses_item_truth_and_releases_lease() -> None:
    repository, cursor = _repository(
        [
            {
                "contains": "WHERE batch_id = %s FOR UPDATE",
                "one": {"batch_id": "hmmb_shared", "status": "running"},
            },
            {
                "contains": "GROUP BY item_status",
                "all": [{"item_status": "succeeded", "count": 1}],
            },
            {"contains": "AS has_queued_evaluation", "one": {"has_queued_evaluation": False}},
            {
                "contains": "UPDATE hmm_evolution.batch_test_run",
                "one": {"batch_id": "hmmb_shared", "status": "completed", "owner_id": None},
            },
        ]
    )

    batch = repository._recompute_batch_state_with_cursor(
        cursor,
        "hmmb_shared",
        release_lease=True,
    )

    assert batch["status"] == "completed"
    update_params = cursor.queries[-1][1]
    assert update_params[0] == "completed"
    assert update_params[7:11] == (True, True, True, True)


def test_finalize_worker_cycle_requeues_remaining_items_and_releases_lease() -> None:
    repository, cursor = _repository(
        [
            {
                "contains": "status IN ('running', 'cancel_requested')",
                "one": {
                    "batch_id": "hmmb_shared",
                    "status": "running",
                    "owner_id": "worker-1",
                },
            },
            {"contains": "WHERE eval_id = ANY", "all": [{"batch_id": "hmmb_shared"}]},
            {
                "contains": "GROUP BY item_status",
                "all": [
                    {"item_status": "succeeded", "count": 2},
                    {"item_status": "queued", "count": 1},
                ],
            },
            {"contains": "AS has_queued_evaluation", "one": {"has_queued_evaluation": True}},
            {
                "contains": "UPDATE hmm_evolution.batch_test_run",
                "one": {"batch_id": "hmmb_shared", "status": "queued", "owner_id": None},
            },
        ]
    )

    batch = repository.finalize_worker_cycle(
        batch_id="hmmb_shared",
        eval_ids=["eval-1", "eval-2"],
        owner_id="worker-1",
        fencing_token=5,
        expected_row_version=12,
    )

    assert batch["status"] == "queued"
    update_params = cursor.queries[-1][1]
    assert update_params[0] == "queued"
    assert update_params[7:11] == (False, True, True, True)


class _EmptyClaimRepository:
    def __init__(self) -> None:
        self.release_args: dict[str, Any] | None = None
        self.reaper_calls = 0

    def mark_expired_leases_timed_out(self):
        self.reaper_calls += 1
        return {"evaluations": 0, "batches": 0}

    def claim_batch(self, **kwargs):
        return {
            "batch_id": "hmmb_shared",
            "fencing_token": 4,
            "row_version": 9,
        }

    def claim_evaluation(self, **kwargs):
        return None

    def release_batch_after_empty_claim(self, **kwargs):
        self.release_args = kwargs
        return {"batch_id": kwargs["batch_id"], "status": "running"}


class _Executor:
    def execute_and_finalize(self, **kwargs):  # pragma: no cover - no evaluation exists.
        raise AssertionError("executor must not run without a claimed evaluation")


def test_worker_releases_batch_when_shared_evaluation_is_claimed_elsewhere() -> None:
    repository = _EmptyClaimRepository()
    worker = HMMEvolutionWorker(
        repository,  # type: ignore[arg-type]
        owner_id="worker-1",
        config=WorkerConfig(runtime_mode="api_worker"),
        executor=_Executor(),
    )

    assert worker.run_once() is True
    assert repository.reaper_calls == 1
    assert repository.release_args == {
        "batch_id": "hmmb_shared",
        "owner_id": "worker-1",
        "fencing_token": 4,
        "expected_row_version": 9,
    }


class _NoBatchRepository:
    def __init__(self) -> None:
        self.reaper_calls = 0

    def mark_expired_leases_timed_out(self):
        self.reaper_calls += 1
        return {"evaluations": 1, "batches": 1}

    def claim_batch(self, **_kwargs):
        return None


def test_worker_reaps_expired_leases_before_looking_for_new_work() -> None:
    repository = _NoBatchRepository()
    worker = HMMEvolutionWorker(
        repository,  # type: ignore[arg-type]
        owner_id="worker-1",
        config=WorkerConfig(runtime_mode="api_worker"),
        executor=_Executor(),
    )

    assert worker.run_once() is False
    assert repository.reaper_calls == 1


class _ConcurrentRepository:
    def __init__(self) -> None:
        self.batch_version = 1
        self.eval_versions = {"eval-1": 1, "eval-2": 1, "eval-3": 1}
        self.pending = ["eval-1", "eval-2", "eval-3"]
        self.finalize_args: dict[str, Any] | None = None
        self.lock = Lock()

    def mark_expired_leases_timed_out(self):
        return {"evaluations": 0, "batches": 0}

    def claim_batch(self, **_kwargs):
        return {
            "batch_id": "batch-1",
            "fencing_token": 5,
            "row_version": self.batch_version,
            "status": "running",
        }

    def claim_evaluation(self, **_kwargs):
        if not self.pending:
            return None
        eval_id = self.pending.pop(0)
        return {
            "eval_id": eval_id,
            "candidate_id": f"candidate-{eval_id}",
            "fencing_token": 7,
            "row_version": self.eval_versions[eval_id],
        }

    def heartbeat_batch(self, **kwargs):
        with self.lock:
            self.batch_version += 1
            return {
                "batch_id": kwargs["batch_id"],
                "fencing_token": kwargs["fencing_token"],
                "row_version": self.batch_version,
                "status": "running",
            }

    def heartbeat_evaluation(self, **kwargs):
        with self.lock:
            eval_id = kwargs["eval_id"]
            self.eval_versions[eval_id] += 1
            return {
                "eval_id": eval_id,
                "fencing_token": kwargs["fencing_token"],
                "row_version": self.eval_versions[eval_id],
                "cancel_requested_at": None,
            }

    def finalize_worker_cycle(self, **kwargs):
        self.finalize_args = kwargs
        return {"batch_id": kwargs["batch_id"], "status": "queued"}


class _ConcurrentExecutor:
    def __init__(self) -> None:
        self.prepared_count = 0
        self.active = 0
        self.max_active = 0
        self.lock = Lock()

    def prepare_batch_inputs(self, **kwargs):
        self.prepared_count += 1
        assert kwargs["candidate_concurrency"] == 2
        kwargs["checkpoint"]("shared-inputs")
        return BatchExecutionInputs(
            inputs_by_eval_id={item["eval_id"]: object() for item in kwargs["evaluations"]},
            errors_by_eval_id={},
        )

    def execute_and_finalize(self, **kwargs):
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        kwargs["checkpoint"]("candidate")
        time.sleep(0.03)
        with self.lock:
            self.active -= 1

    def fail_preparation(self, **_kwargs):  # pragma: no cover - success path only.
        raise AssertionError("preparation must not fail")


def test_worker_uses_shared_inputs_and_bounded_candidate_concurrency() -> None:
    repository = _ConcurrentRepository()
    executor = _ConcurrentExecutor()
    worker = HMMEvolutionWorker(
        repository,  # type: ignore[arg-type]
        owner_id="worker-1",
        config=WorkerConfig(runtime_mode="api_worker", candidate_concurrency=2),
        executor=executor,
    )

    assert worker.run_once() is True
    assert executor.prepared_count == 1
    assert executor.max_active == 2
    assert repository.pending == ["eval-3"]
    assert repository.finalize_args is not None
    assert repository.finalize_args["eval_ids"] == ["eval-1", "eval-2"]


def test_batch_recommendations_persist_only_on_batch_items() -> None:
    repository, cursor = _repository(
        [
            {
                "contains": "SELECT recommendation_version",
                "one": {"recommendation_version": "hmm_recommendation_v1"},
            },
            {
                "contains": "JOIN hmm_evolution.offline_evaluation",
                "all": [
                    {
                        "candidate_id": "c_low",
                        "net_label_return": 0.01,
                        "net_db_10d": 0.02,
                        "positive_net_label_day_ratio": 0.4,
                        "primary_coverage_ratio": 0.8,
                    },
                    {
                        "candidate_id": "c_high",
                        "net_label_return": 0.03,
                        "net_db_10d": 0.04,
                        "positive_net_label_day_ratio": 0.8,
                        "primary_coverage_ratio": 1.0,
                    },
                ],
            },
            {"contains": "recommendation_score = NULL"},
            {"contains": "SET recommendation_score", "one": {"candidate_id": "c_low"}},
            {"contains": "SET recommendation_score", "one": {"candidate_id": "c_high"}},
        ]
    )

    repository._apply_recommendations_with_cursor(cursor, "hmmb_1")  # noqa: SLF001

    update_queries = [(query, params) for query, params in cursor.queries if "SET recommendation_score = %s" in query]
    assert update_queries[0][1][2] == 2
    assert update_queries[0][1][3] is True
    assert update_queries[1][1][2] == 1
    assert update_queries[1][1][3] is True
    assert all("UPDATE hmm_evolution.offline_evaluation" not in query for query, _ in cursor.queries)
    assert cursor.steps == []
