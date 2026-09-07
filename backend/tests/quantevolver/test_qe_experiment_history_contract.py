"""Minimal UI/API history contract for Batch A registered runs."""

from __future__ import annotations

from backend.services.quantevolver.config_composer import _qe_history_filter_sql
from backend.services.quantevolver.payload_summary import compact_experiment_row
from backend.services.quantevolver.qe_run_registry import QERunRegistry, canonical_status_counts


def test_registered_history_summary_keeps_business_identity_and_progress() -> None:
    row = {
        "experiment_id": "qe-registered",
        "experiment_name": "MA-E20",
        "status": "running",
        "canonical_status": "running",
        "model_id": "LSTM",
        "strategy_id": "TWAP",
        "factor_count": 18,
        "custom_params": {
            "_qe_run_registration": {
                "schema_version": "qe_run_registration_v1",
                "source_type": "mcp",
                "purpose": "research",
                "node_id": "rdagent-node1",
                "dataset_release_id": "qe-20260831",
                "dataset_cutoff": "2026-08-31",
                "execution_algo": "TWAP",
            }
        },
        "progress_summary": {
            "kind": "evolution",
            "total": 4,
            "current": 1,
            "counts": {"pending": 2, "running": 1, "failed": 1},
        },
    }

    item = compact_experiment_row(row, include_config_summary=True)

    assert item["factor_count"] == 18
    assert item["canonical_status"] == "running"
    assert item["registration_summary"]["source_type"] == "mcp"
    assert item["registration_summary"]["dataset_release_id"] == "qe-20260831"
    assert item["registration_summary"]["execution_algo"] == "TWAP"
    assert item["progress_summary"]["counts"]["failed"] == 1
    assert "custom_params" not in item


def test_registered_history_summary_exposes_artifact_retention() -> None:
    item = compact_experiment_row(
        {
            "experiment_id": "qe-cleaned",
            "status": "completed",
            "custom_params": {
                "_qe_run_registration": {"source_type": "ui", "purpose": "research"},
                "_qe_artifact_retention": {
                    "schema_version": "qe_artifact_retention_v1",
                    "status": "cleaned",
                    "cleaned_at": "2026-09-07T12:00:00+08:00",
                },
            },
        }
    )

    assert item["artifact_retention"]["status"] == "cleaned"
    assert "custom_params" not in item


def test_history_business_filters_are_parameterized_and_exclude_internal_ids() -> None:
    sql, params = _qe_history_filter_sql(
        {
            "created_from": "2026-08-01",
            "created_to": "2026-08-31",
            "source_type": "mcp",
            "run_kind": "custom_evolution",
            "purpose": "research",
            "status": "completed",
            "node_id": "rdagent-node1",
            "model": "LSTM",
            "factor": "leadership",
            "dataset_release": "qe-full-v2-20260831",
            "universe_pool": "CSI300",
            "execution_algo": "TWAP",
            "query": "MA-E19",
        }
    )

    assert "experiment_id" not in sql
    assert "workspace_path" not in sql
    assert "custom_params->'_qe_run_registration'" in sql
    assert "run_kind" in sql
    assert "factor_names" in sql
    assert "ILIKE" in sql
    assert params[0] == "2026-08-01"
    assert "qe-full-v2-20260831" in params
    assert '["CSI300"]' in params
    assert ["completed", "success", "succeeded"] in params


def test_partial_parent_counts_preserve_successful_and_failed_children() -> None:
    counts = canonical_status_counts(
        ["completed", "completed", "failed", "cancelled", "running", "pending"]
    )

    assert counts == {
        "cancelled": 1,
        "completed": 2,
        "failed": 1,
        "queued": 1,
        "running": 1,
    }


def test_history_projection_prefers_task_identity_and_canonicalizes_progress() -> None:
    query_rows = iter(
        [
            [
                {
                    "task_id": "task-1",
                    "base_experiment_id": "qe-base",
                    "max_loops": 3,
                    "current_loop": 1,
                    "task_status": "pending",
                    "loop_status": "pending",
                    "count": 2,
                },
                {
                    "task_id": "task-1",
                    "base_experiment_id": "qe-base",
                    "max_loops": 3,
                    "current_loop": 1,
                    "task_status": "pending",
                    "loop_status": "processing",
                    "count": 1,
                },
            ],
            [
                {
                    "task_id": "task-1",
                    "base_experiment_id": "qe-base",
                    "strategy_evo_config": {
                        "_qe_run_registration": {
                            "schema_version": "qe_run_registration_v1",
                            "run_kind": "custom_evolution",
                            "source_type": "mcp",
                            "purpose": "research",
                        }
                    },
                }
            ],
            [],
        ]
    )

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, *_args, **_kwargs):
            self.rows = next(query_rows)

        def fetchall(self):
            return self.rows

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def cursor(self, *_args, **_kwargs):
            return Cursor()

    registry = QERunRegistry(connection_factory=lambda: Connection())
    [projected] = registry.project_history(
        [
            {
                "experiment_id": "qe-base",
                "qe_task_id": "task-1",
                "status": "created",
                "is_evolution_loop": False,
                "custom_params": {
                    "_qe_run_registration": {
                        "schema_version": "qe_run_registration_v1",
                        "run_kind": "single",
                        "source_type": "ui",
                        "purpose": "research",
                    }
                },
            }
        ]
    )

    assert projected["registration_summary"]["run_kind"] == "custom_evolution"
    assert projected["canonical_status"] == "planned"
    assert projected["progress_summary"]["status"] == "queued"
    assert projected["progress_summary"]["counts"] == {"queued": 2, "running": 1}
