"""Real dev PostgreSQL acceptance for idempotency, CAS, leases, and fencing."""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Any
from uuid import uuid4

import pytest
from psycopg2.extras import RealDictCursor

from backend.services.hmm_evolution.errors import StaleFencingTokenError
from backend.services.hmm_evolution.models import (
    CandidateCoverage,
    CandidateManifest,
    CandidatePreview,
    CandidateSourceType,
    CoefficientStats,
    canonical_json_sha256,
)
from backend.services.hmm_evolution.repository import HMMEvolutionRepository

WORKERS = 8


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


def _parallel(callable_: Any) -> list[Any]:
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [executor.submit(callable_) for _ in range(WORKERS)]
        return [future.result(timeout=30) for future in futures]


@pytest.mark.integration
def test_real_dev_postgres_idempotency_cas_lease_and_fencing(
    hmm_evolution_dev_conn_factory: Any,
) -> None:
    run_id = f"phase1a_{uuid4().hex}"
    repository = HMMEvolutionRepository(hmm_evolution_dev_conn_factory)
    preview = _preview(run_id)
    eval_ids: set[str] = set()
    batch_ids: set[str] = set()

    evaluation_kwargs = {
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

    try:
        candidate_results = _parallel(
            lambda: repository.register_candidate(
                preview,
                display_name=f"dev acceptance {run_id}",
                description="temporary real-dev concurrency acceptance row",
                created_by=f"acceptance:{run_id}",
            )
        )
        assert {row.candidate_id for row, _created in candidate_results} == {
            preview.candidate_id
        }
        assert sum(created for _row, created in candidate_results) == 1

        evaluation_results = _parallel(
            lambda: repository.create_or_get_evaluation(**evaluation_kwargs)
        )
        eval_ids = {str(row["eval_id"]) for row, _created in evaluation_results}
        assert len(eval_ids) == 1
        assert sum(created for _row, created in evaluation_results) == 1
        eval_id = eval_ids.pop()
        eval_ids.add(eval_id)

        request_hash = _sha(f"batch:{run_id}")
        idempotency_key = f"hmm-evolution-dev-acceptance:{run_id}"
        batch_results = _parallel(
            lambda: repository.create_or_get_batch(
                request_hash=request_hash,
                items=[
                    {
                        "candidate_id": preview.candidate_id,
                        "eval_id": eval_id,
                        "item_status": "queued",
                    }
                ],
                recommendation_spec={
                    "schema_version": "hmm_recommendation_v1",
                    "acceptance_run_id": run_id,
                },
                recommendation_version="hmm_recommendation_v1",
                created_by=f"acceptance:{run_id}",
                idempotency_key=idempotency_key,
            )
        )
        batch_ids = {str(row["batch_id"]) for row, _created in batch_results}
        assert len(batch_ids) == 1
        assert sum(created for _row, created in batch_results) == 1
        batch_id = batch_ids.pop()
        batch_ids.add(batch_id)

        with hmm_evolution_dev_conn_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE hmm_evolution.batch_test_run "
                    "SET created_at = TIMESTAMPTZ '2000-01-01 00:00:00+00' "
                    "WHERE batch_id = %s",
                    (batch_id,),
                )
                cursor.execute(
                    "UPDATE hmm_evolution.offline_evaluation "
                    "SET created_at = TIMESTAMPTZ '2000-01-01 00:00:00+00' "
                    "WHERE eval_id = %s",
                    (eval_id,),
                )

        batch_claims = _parallel(
            lambda: repository.claim_batch(owner_id=f"batch-worker:{run_id}")
        )
        claimed_batches = [row for row in batch_claims if row is not None]
        assert len(claimed_batches) == 1
        batch_claim = claimed_batches[0]
        assert batch_claim["batch_id"] == batch_id

        evaluation_claims = _parallel(
            lambda: repository.claim_evaluation(
                owner_id=f"eval-worker:{run_id}",
                batch_id=batch_id,
            )
        )
        claimed_evaluations = [row for row in evaluation_claims if row is not None]
        assert len(claimed_evaluations) == 1
        evaluation_claim = claimed_evaluations[0]
        assert evaluation_claim["eval_id"] == eval_id

        batch_heartbeat = repository.heartbeat_batch(
            batch_id=batch_id,
            owner_id=f"batch-worker:{run_id}",
            fencing_token=int(batch_claim["fencing_token"]),
            expected_row_version=int(batch_claim["row_version"]),
        )
        with pytest.raises(StaleFencingTokenError):
            repository.heartbeat_batch(
                batch_id=batch_id,
                owner_id=f"batch-worker:{run_id}",
                fencing_token=int(batch_claim["fencing_token"]),
                expected_row_version=int(batch_claim["row_version"]),
            )

        evaluation_heartbeat = repository.heartbeat_evaluation(
            eval_id=eval_id,
            owner_id=f"eval-worker:{run_id}",
            fencing_token=int(evaluation_claim["fencing_token"]),
            expected_row_version=int(evaluation_claim["row_version"]),
        )
        with pytest.raises(StaleFencingTokenError):
            repository.heartbeat_evaluation(
                eval_id=eval_id,
                owner_id=f"eval-worker:{run_id}",
                fencing_token=int(evaluation_claim["fencing_token"]),
                expected_row_version=int(evaluation_claim["row_version"]),
            )

        result = {
            "trading_days_count": 80,
            "changed_day_count": 12,
            "label_comparable_day_count": 80,
            "db_comparable_day_count": 80,
            "replacement_count": 100,
            "primary_coverage_ratio": 1.0,
            "net_label_return": 0.08,
            "net_db_10d": 0.05,
            "positive_net_label_day_ratio": 0.6,
            "evidence_quality": "complete",
            "warnings_json": [],
            "metrics_json": {"acceptance_run_id": run_id},
            "result_hash": canonical_json_sha256({"acceptance_run_id": run_id}),
        }
        completed = repository.complete_evaluation(
            eval_id=eval_id,
            owner_id=f"eval-worker:{run_id}",
            fencing_token=int(evaluation_heartbeat["fencing_token"]),
            expected_row_version=int(evaluation_heartbeat["row_version"]),
            result=result,
        )
        assert completed["status"] == "succeeded"
        with pytest.raises(StaleFencingTokenError):
            repository.complete_evaluation(
                eval_id=eval_id,
                owner_id=f"eval-worker:{run_id}",
                fencing_token=int(evaluation_claim["fencing_token"]),
                expected_row_version=int(evaluation_claim["row_version"]),
                result=result,
            )

        batch = repository.get_batch(batch_id)
        assert batch["status"] == "completed"
        assert batch["owner_id"] is None
        assert batch["fencing_token"] == batch_heartbeat["fencing_token"]
        assert [item["item_status"] for item in batch["items"]] == ["succeeded"]

        timeout_preview = _preview(f"{run_id}_timeout")
        timeout_candidate, _ = repository.register_candidate(
            timeout_preview,
            display_name=f"dev timeout acceptance {run_id}",
            description="temporary lease-expiry acceptance row",
            created_by=f"acceptance:{run_id}",
        )
        timeout_eval, _ = repository.create_or_get_evaluation(
            **{
                **evaluation_kwargs,
                "candidate_id": timeout_candidate.candidate_id,
                "logical_evaluation_key": _sha(f"timeout-logical:{run_id}"),
                "candidate_manifest_hash": timeout_preview.manifest_hash,
            }
        )
        eval_ids.add(str(timeout_eval["eval_id"]))
        timeout_batch, _ = repository.create_or_get_batch(
            request_hash=_sha(f"timeout-batch:{run_id}"),
            items=[
                {
                    "candidate_id": timeout_candidate.candidate_id,
                    "eval_id": timeout_eval["eval_id"],
                    "item_status": "queued",
                }
            ],
            recommendation_spec={
                "schema_version": "hmm_recommendation_v1",
                "acceptance_timeout_run_id": run_id,
            },
            recommendation_version="hmm_recommendation_v1",
            created_by=f"acceptance:{run_id}",
            idempotency_key=f"hmm-evolution-dev-timeout:{run_id}",
        )
        batch_ids.add(str(timeout_batch["batch_id"]))
        with hmm_evolution_dev_conn_factory() as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "UPDATE hmm_evolution.offline_evaluation "
                    "SET status='running', owner_id=%s, fencing_token=1, "
                    "lease_expires_at=clock_timestamp() - interval '1 second', "
                    "row_version=row_version+1 WHERE eval_id=%s RETURNING eval_id",
                    (f"expired-worker:{run_id}", timeout_eval["eval_id"]),
                )
                assert cursor.fetchone() is not None
                cursor.execute(
                    "UPDATE hmm_evolution.batch_test_run "
                    "SET status='running', owner_id=%s, fencing_token=1, "
                    "lease_expires_at=clock_timestamp() - interval '1 second', "
                    "row_version=row_version+1 WHERE batch_id=%s RETURNING batch_id",
                    (f"expired-worker:{run_id}", timeout_batch["batch_id"]),
                )
                assert cursor.fetchone() is not None
        expired = repository.mark_expired_leases_timed_out()
        assert expired["evaluations"] >= 1
        assert expired["batches"] >= 1
        assert repository.get_batch(str(timeout_batch["batch_id"]))["status"] == "timed_out"
    finally:
        with hmm_evolution_dev_conn_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM hmm_evolution.batch_test_item "
                    "WHERE batch_id = ANY(%s) OR candidate_id IN (%s, %s)",
                    (
                        list(batch_ids),
                        preview.candidate_id,
                        _preview(f"{run_id}_timeout").candidate_id,
                    ),
                )
                cursor.execute(
                    "DELETE FROM hmm_evolution.batch_test_run WHERE batch_id = ANY(%s)",
                    (list(batch_ids),),
                )
                cursor.execute(
                    "DELETE FROM hmm_evolution.offline_evaluation WHERE eval_id = ANY(%s)",
                    (list(eval_ids),),
                )
                cursor.execute(
                    "DELETE FROM hmm_evolution.candidate WHERE candidate_id IN (%s, %s)",
                    (preview.candidate_id, _preview(f"{run_id}_timeout").candidate_id),
                )
