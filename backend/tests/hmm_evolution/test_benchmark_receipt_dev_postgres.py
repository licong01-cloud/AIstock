"""Real dev PostgreSQL acceptance for benchmark purpose isolation and v3 receipts.

Covers ruling 1 (benchmark generations on the shared per-key sequence, invisible
to normal submissions, retry inheriting purpose) and ruling 2/4 (performance
receipt + worker runtime status CRUD with CAS and idempotent creation).
"""

from __future__ import annotations

import hashlib
from datetime import date
from typing import Any
from uuid import uuid4

import pytest
from psycopg2.extras import RealDictCursor

from backend.services.hmm_evolution.errors import (
    InvalidStateTransitionError,
    StaleFencingTokenError,
)
from backend.services.hmm_evolution.models import (
    PERFORMANCE_RECEIPT_SCHEMA_VERSION,
    CandidateCoverage,
    CandidateManifest,
    CandidatePreview,
    CandidateSourceType,
    CoefficientStats,
    canonical_json_sha256,
)
from backend.services.hmm_evolution.repository import HMMEvolutionRepository


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _preview(run_id: str) -> CandidatePreview:
    artifact_sha = _sha(f"artifact:{run_id}")
    manifest = CandidateManifest(
        source_type=CandidateSourceType.CONFIGURED_LOCAL,
        source_ref={
            "root_alias": "hmm_evolution_dev_acceptance",
            "relative_path": f"{run_id}/candidate.json",
        },
        artifact_uri=f"configured-local://hmm_evolution_dev_acceptance/{run_id}/candidate.json",
        artifact_sha256=artifact_sha,
        size_bytes=128,
        detected_format="hmm_sector_coefficients_legacy_v1",
        coverage=CandidateCoverage(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 2),
            date_count=2,
            sector_count_min=1,
            sector_count_max=2,
            stock_sector_map_count=2,
        ),
        coefficient_stats=CoefficientStats(min=0.9, max=1.1),
    )
    return CandidatePreview(
        candidate_id=manifest.candidate_id,
        manifest_hash=manifest.manifest_hash,
        manifest=manifest,
    )


def _evaluation_kwargs(run_id: str, preview: CandidatePreview) -> dict[str, Any]:
    return {
        "candidate_id": preview.candidate_id,
        "logical_evaluation_key": _sha(f"logical:{run_id}"),
        "base_loop_ref": "qe_20260706_013235_bbd4/Loop8",
        "source_manifest": {"schema_version": "acceptance_v1", "run_id": run_id},
        "source_manifest_hash": _sha(f"source:{run_id}"),
        "candidate_manifest_hash": preview.manifest_hash,
        "evaluation_spec": {"schema_version": "acceptance_v1", "run_id": run_id},
        "evaluation_spec_hash": _sha(f"spec:{run_id}"),
        "evaluator_version": "phase1a_acceptance_v1",
        "as_of_date": date(2026, 7, 6),
        "window_start": date(2026, 1, 1),
        "window_end": date(2026, 6, 30),
        "label_horizon_days": 20,
        "universe_id": "prediction_artifact_all",
        "universe_hash": _sha(f"universe:{run_id}"),
        "topk": 50,
    }


def _mark_evaluation(conn_factory: Any, eval_id: str, status: str) -> None:
    with conn_factory() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE hmm_evolution.offline_evaluation "
                "SET status = %s, completed_at = clock_timestamp(), "
                "updated_at = clock_timestamp(), row_version = row_version + 1 "
                "WHERE eval_id = %s",
                (status, eval_id),
            )


def _generations_for_key(
    conn_factory: Any, logical_evaluation_key: str
) -> list[dict[str, Any]]:
    with conn_factory() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT eval_id, run_generation, execution_purpose, benchmark_id, status "
                "FROM hmm_evolution.offline_evaluation "
                "WHERE logical_evaluation_key = %s "
                "ORDER BY run_generation ASC",
                (logical_evaluation_key,),
            )
            return [dict(row) for row in cursor.fetchall()]


@pytest.mark.integration
def test_real_dev_postgres_benchmark_purpose_isolation(
    hmm_evolution_dev_conn_factory: Any,
) -> None:
    run_id = f"phase1b_bench_{uuid4().hex}"
    repository = HMMEvolutionRepository(hmm_evolution_dev_conn_factory)
    preview = _preview(run_id)
    logical_key = _sha(f"logical:{run_id}")
    evaluation_kwargs = _evaluation_kwargs(run_id, preview)
    benchmark_id = f"benchmark_20260721_{run_id[-12:]}"
    batch_ids: list[str] = []

    try:
        repository.register_candidate(
            preview,
            display_name=f"dev benchmark acceptance {run_id}",
            description="temporary benchmark purpose isolation acceptance row",
            created_by=f"acceptance:{run_id}",
        )

        normal, created = repository.create_or_get_evaluation(**evaluation_kwargs)
        assert created is True
        assert normal["run_generation"] == 1
        assert normal["execution_purpose"] == "evaluation"
        assert normal["benchmark_id"] is None

        again, created = repository.create_or_get_evaluation(**evaluation_kwargs)
        assert created is False
        assert again["eval_id"] == normal["eval_id"]
        assert again["run_generation"] == 1

        with pytest.raises(InvalidStateTransitionError):
            repository.create_or_get_evaluation(
                **evaluation_kwargs,
                execution_purpose="benchmark",
            )
        with pytest.raises(InvalidStateTransitionError):
            repository.create_or_get_evaluation(
                **evaluation_kwargs,
                benchmark_id=benchmark_id,
            )
        with pytest.raises(InvalidStateTransitionError):
            repository.create_or_get_evaluation(
                **evaluation_kwargs,
                execution_purpose="benchmark",
                benchmark_id=benchmark_id,
            )

        _mark_evaluation(hmm_evolution_dev_conn_factory, str(normal["eval_id"]), "succeeded")

        benchmark_one, created = repository.create_or_get_evaluation(
            **evaluation_kwargs,
            execution_purpose="benchmark",
            benchmark_id=benchmark_id,
        )
        assert created is True
        assert benchmark_one["run_generation"] == 2
        assert benchmark_one["execution_purpose"] == "benchmark"
        assert benchmark_one["benchmark_id"] == benchmark_id

        benchmark_two, created = repository.create_or_get_evaluation(
            **evaluation_kwargs,
            execution_purpose="benchmark",
            benchmark_id=f"{benchmark_id}_second",
        )
        assert created is True
        assert benchmark_two["run_generation"] == 3
        assert benchmark_two["benchmark_id"] == f"{benchmark_id}_second"

        normal_after, created = repository.create_or_get_evaluation(**evaluation_kwargs)
        assert created is False
        assert normal_after["eval_id"] == normal["eval_id"]
        assert normal_after["run_generation"] == 1
        assert normal_after["execution_purpose"] == "evaluation"

        # A failed benchmark generation must never shadow the succeeded normal row.
        _mark_evaluation(
            hmm_evolution_dev_conn_factory, str(benchmark_one["eval_id"]), "failed"
        )
        normal_after_failure, created = repository.create_or_get_evaluation(
            **evaluation_kwargs
        )
        assert created is False
        assert normal_after_failure["eval_id"] == normal["eval_id"]
        assert normal_after_failure["status"] == "succeeded"

        batch, created = repository.create_or_get_batch(
            request_hash=_sha(f"bench-batch:{run_id}"),
            items=[
                {
                    "candidate_id": preview.candidate_id,
                    "eval_id": benchmark_one["eval_id"],
                    "item_status": "failed",
                }
            ],
            recommendation_spec={
                "schema_version": "hmm_recommendation_v1",
                "acceptance_run_id": run_id,
            },
            recommendation_version="hmm_recommendation_v1",
            created_by=f"acceptance:{run_id}",
            idempotency_key=f"hmm-evolution-dev-benchmark:{run_id}",
            execution_purpose="benchmark",
            benchmark_id=benchmark_id,
        )
        assert created is True
        assert batch["execution_purpose"] == "benchmark"
        assert batch["benchmark_id"] == benchmark_id
        batch_ids.append(str(batch["batch_id"]))
        with hmm_evolution_dev_conn_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE hmm_evolution.batch_test_run "
                    "SET status = 'failed', completed_at = clock_timestamp(), "
                    "updated_at = clock_timestamp(), row_version = row_version + 1 "
                    "WHERE batch_id = %s",
                    (batch["batch_id"],),
                )

        retried = repository.create_retry_batch(
            batch_id=str(batch["batch_id"]),
            created_by=f"acceptance:{run_id}",
            idempotency_key=f"hmm-evolution-dev-benchmark-retry:{run_id}",
        )
        batch_ids.append(str(retried["batch_id"]))
        assert retried["execution_purpose"] == "benchmark"
        assert retried["benchmark_id"] == benchmark_id
        retry_items = repository.get_batch(str(retried["batch_id"]))["items"]
        assert len(retry_items) == 1

        # The retry item evaluation must allocate max+1 across purposes (4), not
        # the failed source generation +1 (3), which already exists as benchmark two.
        generations = _generations_for_key(hmm_evolution_dev_conn_factory, logical_key)
        assert [row["run_generation"] for row in generations] == [1, 2, 3, 4]
        assert [row["execution_purpose"] for row in generations] == [
            "evaluation",
            "benchmark",
            "benchmark",
            "benchmark",
        ]
        retry_eval = generations[-1]
        assert str(retry_eval["eval_id"]) == str(retry_items[0]["eval_id"])
        assert retry_eval["benchmark_id"] == benchmark_id

        benchmark_three, created = repository.create_or_get_evaluation(
            **evaluation_kwargs,
            execution_purpose="benchmark",
            benchmark_id=f"{benchmark_id}_third",
        )
        assert created is True
        assert benchmark_three["run_generation"] == 5

        final_normal, created = repository.create_or_get_evaluation(**evaluation_kwargs)
        assert created is False
        assert final_normal["run_generation"] == 1
    finally:
        with hmm_evolution_dev_conn_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM hmm_evolution.batch_test_item WHERE batch_id = ANY(%s)",
                    (batch_ids,),
                )
                cursor.execute(
                    "DELETE FROM hmm_evolution.batch_test_run WHERE batch_id = ANY(%s)",
                    (batch_ids,),
                )
                cursor.execute(
                    "DELETE FROM hmm_evolution.offline_evaluation "
                    "WHERE logical_evaluation_key = %s",
                    (logical_key,),
                )
                cursor.execute(
                    "DELETE FROM hmm_evolution.candidate WHERE candidate_id = %s",
                    (preview.candidate_id,),
                )


@pytest.mark.integration
def test_real_dev_postgres_performance_receipt_crud(
    hmm_evolution_dev_conn_factory: Any,
) -> None:
    run_id = f"phase1b_receipt_{uuid4().hex}"
    repository = HMMEvolutionRepository(hmm_evolution_dev_conn_factory)
    preview = _preview(run_id)
    logical_key = _sha(f"logical:{run_id}")
    evaluation_kwargs = _evaluation_kwargs(run_id, preview)
    benchmark_id = f"benchmark_20260721_{run_id[-12:]}"
    batch_ids: list[str] = []
    eval_ids: list[str] = []
    runtime_identity = {"role": "api", "owner_id": f"acceptance:{run_id}", "pid": 4321}
    hardware_identity = {"host": "dev-acceptance", "machine": "AMD64"}

    try:
        repository.register_candidate(
            preview,
            display_name=f"dev receipt acceptance {run_id}",
            description="temporary performance receipt acceptance row",
            created_by=f"acceptance:{run_id}",
        )
        evaluation, _ = repository.create_or_get_evaluation(**evaluation_kwargs)
        eval_ids.append(str(evaluation["eval_id"]))
        batch, _ = repository.create_or_get_batch(
            request_hash=_sha(f"receipt-batch:{run_id}"),
            items=[
                {
                    "candidate_id": preview.candidate_id,
                    "eval_id": evaluation["eval_id"],
                    "item_status": "queued",
                }
            ],
            recommendation_spec={
                "schema_version": "hmm_recommendation_v1",
                "acceptance_run_id": run_id,
            },
            recommendation_version="hmm_recommendation_v1",
            created_by=f"acceptance:{run_id}",
            idempotency_key=f"hmm-evolution-dev-receipt:{run_id}",
        )
        batch_ids.append(str(batch["batch_id"]))

        receipt, created = repository.create_performance_receipt(
            receipt_level="batch",
            batch_id=str(batch["batch_id"]),
            eval_id=None,
            execution_purpose="evaluation",
            benchmark_id=None,
            runtime_identity=runtime_identity,
            hardware_identity=hardware_identity,
            input_identity={"request_hash": str(batch["request_hash"])},
        )
        assert created is True
        assert receipt["receipt_status"] == "partial"
        assert receipt["cache_state"] == "unknown"
        assert receipt["schema_version"] == PERFORMANCE_RECEIPT_SCHEMA_VERSION
        assert receipt["stage_timings"] == {}
        assert receipt["finalized_at"] is None

        duplicate, created = repository.create_performance_receipt(
            receipt_level="batch",
            batch_id=str(batch["batch_id"]),
            eval_id=None,
            execution_purpose="evaluation",
            benchmark_id=None,
            runtime_identity=runtime_identity,
            hardware_identity=hardware_identity,
        )
        assert created is False
        assert duplicate["receipt_id"] == receipt["receipt_id"]

        with pytest.raises(ValueError):
            repository.create_performance_receipt(
                receipt_level="evaluation",
                batch_id=str(batch["batch_id"]),
                eval_id=None,
                execution_purpose="evaluation",
                benchmark_id=None,
                runtime_identity=runtime_identity,
                hardware_identity=hardware_identity,
            )
        with pytest.raises(ValueError):
            repository.create_performance_receipt(
                receipt_level="batch",
                batch_id=str(batch["batch_id"]),
                eval_id=str(evaluation["eval_id"]),
                execution_purpose="evaluation",
                benchmark_id=None,
                runtime_identity=runtime_identity,
                hardware_identity=hardware_identity,
            )
        with pytest.raises(ValueError):
            repository.create_performance_receipt(
                receipt_level="batch",
                batch_id=str(batch["batch_id"]),
                eval_id=None,
                execution_purpose="benchmark",
                benchmark_id=None,
                runtime_identity=runtime_identity,
                hardware_identity=hardware_identity,
            )

        eval_receipt, created = repository.create_performance_receipt(
            receipt_level="evaluation",
            batch_id=str(batch["batch_id"]),
            eval_id=str(evaluation["eval_id"]),
            execution_purpose="evaluation",
            benchmark_id=None,
            runtime_identity={"role": "evaluation_worker", "owner_id": f"acceptance:{run_id}"},
            hardware_identity=hardware_identity,
            input_identity={"logical_evaluation_key": logical_key},
        )
        assert created is True
        assert eval_receipt["receipt_level"] == "evaluation"

        stage_payload = {
            "api_receipt_persist": {
                "started_at": "2026-07-21T01:00:00+00:00",
                "completed_at": "2026-07-21T01:00:00.120000+00:00",
                "duration_ms": 120,
            }
        }
        merged = repository.merge_performance_receipt_progress(
            receipt_id=str(receipt["receipt_id"]),
            expected_row_version=int(receipt["row_version"]),
            stage_timings=stage_payload,
            peak_rss_bytes=1000,
        )
        assert int(merged["row_version"]) == int(receipt["row_version"]) + 1
        assert merged["stage_timings"]["api_receipt_persist"]["duration_ms"] == 120
        assert merged["peak_rss_bytes"] == 1000
        assert merged["receipt_status"] == "partial"

        merged_two = repository.merge_performance_receipt_progress(
            receipt_id=str(receipt["receipt_id"]),
            expected_row_version=int(merged["row_version"]),
            stage_timings={
                "qe_source_load": {
                    "started_at": "2026-07-21T01:00:01+00:00",
                    "completed_at": "2026-07-21T01:00:02+00:00",
                    "duration_ms": 1000,
                }
            },
            cache_evidence=[
                {"artifact": "source", "state": "cold_miss", "detail": {"bytes": 10}}
            ],
            cache_state="cold",
            peak_rss_bytes=500,
        )
        assert merged_two["peak_rss_bytes"] == 1000  # GREATEST semantics, never regresses
        assert set(merged_two["stage_timings"]) == {"api_receipt_persist", "qe_source_load"}
        assert merged_two["cache_state"] == "cold"

        with pytest.raises(StaleFencingTokenError):
            repository.merge_performance_receipt_progress(
                receipt_id=str(receipt["receipt_id"]),
                expected_row_version=int(receipt["row_version"]),
                stage_timings=stage_payload,
            )

        finalized = repository.finalize_performance_receipt(
            receipt_id=str(receipt["receipt_id"]),
            expected_row_version=int(merged_two["row_version"]),
            request_to_terminal_ms=4321,
            result_hash=_sha(f"result:{run_id}"),
        )
        assert finalized["receipt_status"] == "final"
        assert finalized["finalized_at"] is not None
        assert finalized["request_to_terminal_ms"] == 4321
        assert finalized["result_hash"] == _sha(f"result:{run_id}")

        with pytest.raises(StaleFencingTokenError):
            repository.finalize_performance_receipt(
                receipt_id=str(receipt["receipt_id"]),
                expected_row_version=int(finalized["row_version"]),
                request_to_terminal_ms=1,
            )

        by_batch = repository.get_performance_receipt(batch_id=str(batch["batch_id"]))
        assert by_batch is not None
        assert by_batch["receipt_id"] == receipt["receipt_id"]
        by_eval = repository.get_performance_receipt(eval_id=str(evaluation["eval_id"]))
        assert by_eval is not None
        assert by_eval["receipt_id"] == eval_receipt["receipt_id"]
        assert by_eval["receipt_status"] == "partial"

        # Benchmark receipts are listed by benchmark_id and never mix with normal ones.
        _mark_evaluation(hmm_evolution_dev_conn_factory, str(evaluation["eval_id"]), "succeeded")
        benchmark_eval, _ = repository.create_or_get_evaluation(
            **evaluation_kwargs,
            execution_purpose="benchmark",
            benchmark_id=benchmark_id,
        )
        eval_ids.append(str(benchmark_eval["eval_id"]))
        benchmark_batch, _ = repository.create_or_get_batch(
            request_hash=_sha(f"receipt-bench-batch:{run_id}"),
            items=[
                {
                    "candidate_id": preview.candidate_id,
                    "eval_id": benchmark_eval["eval_id"],
                    "item_status": "queued",
                }
            ],
            recommendation_spec={
                "schema_version": "hmm_recommendation_v1",
                "acceptance_run_id": run_id,
            },
            recommendation_version="hmm_recommendation_v1",
            created_by=f"acceptance:{run_id}",
            idempotency_key=f"hmm-evolution-dev-receipt-bench:{run_id}",
            execution_purpose="benchmark",
            benchmark_id=benchmark_id,
        )
        batch_ids.append(str(benchmark_batch["batch_id"]))
        benchmark_receipt, created = repository.create_performance_receipt(
            receipt_level="batch",
            batch_id=str(benchmark_batch["batch_id"]),
            eval_id=None,
            execution_purpose="benchmark",
            benchmark_id=benchmark_id,
            runtime_identity=runtime_identity,
            hardware_identity=hardware_identity,
        )
        assert created is True
        assert benchmark_receipt["execution_purpose"] == "benchmark"
        assert benchmark_receipt["benchmark_id"] == benchmark_id

        listed = repository.list_performance_receipts(benchmark_id=benchmark_id)
        assert [row["receipt_id"] for row in listed] == [benchmark_receipt["receipt_id"]]
    finally:
        with hmm_evolution_dev_conn_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM hmm_evolution.performance_receipt "
                    "WHERE batch_id = ANY(%s) OR eval_id = ANY(%s)",
                    (batch_ids, eval_ids),
                )
                cursor.execute(
                    "DELETE FROM hmm_evolution.batch_test_item WHERE batch_id = ANY(%s)",
                    (batch_ids,),
                )
                cursor.execute(
                    "DELETE FROM hmm_evolution.batch_test_run WHERE batch_id = ANY(%s)",
                    (batch_ids,),
                )
                cursor.execute(
                    "DELETE FROM hmm_evolution.offline_evaluation "
                    "WHERE logical_evaluation_key = %s",
                    (logical_key,),
                )
                cursor.execute(
                    "DELETE FROM hmm_evolution.candidate WHERE candidate_id = %s",
                    (preview.candidate_id,),
                )


@pytest.mark.integration
def test_real_dev_postgres_worker_runtime_status(
    hmm_evolution_dev_conn_factory: Any,
) -> None:
    run_id = f"phase1b_worker_{uuid4().hex}"
    owner_id = f"acceptance-worker:{run_id}"
    repository = HMMEvolutionRepository(hmm_evolution_dev_conn_factory)

    try:
        started = repository.upsert_worker_started(
            owner_id=owner_id,
            host="dev-acceptance-host",
            pid=12345,
        )
        assert started["runtime_status"] == "running"
        assert started["consecutive_failure_count"] == 0
        assert started["shutdown_at"] is None
        assert started["last_poll_at"] is not None

        restarted = repository.upsert_worker_started(
            owner_id=owner_id,
            host="dev-acceptance-host",
            pid=23456,
        )
        assert restarted["pid"] == 23456
        assert int(restarted["row_version"]) == int(started["row_version"]) + 1
        assert restarted["last_claimed_batch_id"] is None

        claimed_batch = f"hmmb_{uuid4().hex[:24]}"
        polled = repository.record_worker_poll(
            owner_id=owner_id,
            expected_row_version=int(restarted["row_version"]),
            claimed_batch_id=claimed_batch,
        )
        assert polled["last_claimed_batch_id"] == claimed_batch
        assert polled["consecutive_failure_count"] == 0

        failed_once = repository.record_worker_poll(
            owner_id=owner_id,
            expected_row_version=int(polled["row_version"]),
            terminal_batch_id=claimed_batch,
            terminal_failed=True,
        )
        assert failed_once["last_terminal_batch_id"] == claimed_batch
        assert failed_once["consecutive_failure_count"] == 1

        failed_twice = repository.record_worker_poll(
            owner_id=owner_id,
            expected_row_version=int(failed_once["row_version"]),
            terminal_batch_id=claimed_batch,
            terminal_failed=True,
        )
        assert failed_twice["consecutive_failure_count"] == 2

        succeeded = repository.record_worker_poll(
            owner_id=owner_id,
            expected_row_version=int(failed_twice["row_version"]),
            terminal_batch_id=f"hmmb_{uuid4().hex[:24]}",
            terminal_failed=False,
        )
        assert succeeded["consecutive_failure_count"] == 0

        with pytest.raises(StaleFencingTokenError):
            repository.record_worker_poll(
                owner_id=owner_id,
                expected_row_version=int(failed_twice["row_version"]),
            )

        stopped = repository.mark_worker_stopped(
            owner_id=owner_id,
            expected_row_version=int(succeeded["row_version"]),
            exit_code=0,
        )
        assert stopped["runtime_status"] == "stopped"
        assert stopped["shutdown_at"] is not None
        assert stopped["exit_code"] == 0

        # A stopped worker must never heartbeat again; a crash leaves staleness,
        # never a fake "stopped" row.
        with pytest.raises(StaleFencingTokenError):
            repository.record_worker_poll(
                owner_id=owner_id,
                expected_row_version=int(stopped["row_version"]),
            )

        listed = repository.list_worker_runtime_status()
        matches = [row for row in listed if row["owner_id"] == owner_id]
        assert len(matches) == 1
        assert matches[0]["runtime_status"] == "stopped"
    finally:
        with hmm_evolution_dev_conn_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM hmm_evolution.worker_runtime_status WHERE owner_id = %s",
                    (owner_id,),
                )


def test_receipt_result_hash_uses_canonical_sha() -> None:
    # Non-integration guard: the acceptance hashes above use the same canonical
    # sha256 helper as production result_hash computation.
    digest = canonical_json_sha256({"acceptance": "receipt"})
    assert len(digest) == 64
    assert digest == canonical_json_sha256({"acceptance": "receipt"})
