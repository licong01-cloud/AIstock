from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.services.multi_alpha.durable_identity import build_execution_identity
from backend.services.qe_archive.handlers.multi_alpha_combine_archive_handler import (
    MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V2,
    MultiAlphaCombineArchiveHandler,
)
from backend.services.qe_archive.models import sha256_json
from backend.services.qe_archive.repository import QEArchiveRepository


def _identity() -> dict[str, Any]:
    return dict(
        build_execution_identity(
            dataset={
                "deployment_snapshot_id": "qe_data_20260721",
                "dataset_manifest_sha256": "a" * 64,
                "cutoff_trade_date": "2026-06-30",
                "qlib_calendar_sha256": "b" * 64,
                "qlib_instruments_sha256": "c" * 64,
                "st_pit_snapshot_id": "qe_st_pit_20260630",
                "st_pit_manifest_sha256": "d" * 64,
                "resolved_node_id": "wsl2-5080",
                "resolved_data_root_uri": "/home/lc999/data/factor_data",
            },
            prediction_sources=[
                {
                    "leg_id": "leg_a",
                    "seed_run_id": "qe_a_L1",
                    "artifact_uri": "prediction-store://qe_a_L1",
                    "artifact_sha256": "e" * 64,
                }
            ],
            runtime={
                "qlib_runtime_template_sha256": "f" * 64,
                "conda_environment_lock_sha256": "1" * 64,
                "execution_environment_snapshot_id": "qeenv_snapshot",
                "execution_environment_manifest_sha256": "2" * 64,
                "executor_code_commit": "3" * 40,
                "executor_file_set_sha256": "4" * 64,
                "backtest_config_sha256": "5" * 64,
            },
            materializer={
                "aistock_commit": "6" * 40,
                "planner_version": "multi_alpha_child_plan_v1",
                "combiner_file_sha256": "7" * 64,
                "panel_builder_file_sha256": "8" * 64,
                "materializer_file_set_sha256": "9" * 64,
            },
            business_formula={
                "formula_version": "multi_alpha_combine_formula_v1",
                "assembler_file_sha256": "a" * 64,
                "delta_formula_sha256": "b" * 64,
            },
        ).payload
    )


class _ArchiveRepository:
    def __init__(self, source: dict[str, Any]) -> None:
        self.source = source
        self.writes: list[dict[str, Any]] = []

    def fetch_multi_alpha_combine_run(self, run_id: str) -> dict[str, Any] | None:
        return self.source if run_id == "macb_successor" else None

    def archive_multi_alpha_bundle(self, **kwargs: Any) -> dict[str, Any]:
        self.writes.append(kwargs)
        return {
            "run_id": kwargs["run_header"]["run_id"],
            "run_rows": 1,
            "leg_rows": len(kwargs["legs"]),
            "leg_source_rows": len(kwargs["leg_sources"]),
            "scheme_rows": len(kwargs["schemes"]),
            "loo_rows": len(kwargs["loo"]),
            "recovery_child_rows": len(kwargs["recovery_children"]),
            "recovery_attempt_rows": len(kwargs["recovery_attempts"]),
        }

    def fetch_archive_run_for_seed(self, _seed: str) -> None:
        return None

    def resolve_evolution_loop_seed(self, *, task_id: str, loop_index: int) -> None:
        return None


def test_archive_v2_preserves_partial_recovery_children_attempts_and_identity() -> None:
    identity = _identity()
    evidence = {
        "schema_version": "multi_alpha_execution_identity_evidence_v1",
        "complete": True,
        "reason_code": None,
        "missing": [],
        "acquisition_suggestions": [],
    }
    result_manifest = {"schema_version": "multi_alpha_child_result_manifest_v1", "metrics": {"sharpe": 1.2}}
    source = {
        "run": {
            "id": "macb_successor",
            "roster_hash": "roster_hash",
            "roster_json": [],
            "oos_start": "2024-07-01",
            "oos_end": "2026-06-29",
            "normalize_method": "rank",
            "walk_forward_json": {"enabled": True},
            "baseline_leg_id": None,
            "status": "partial_recovered",
            "reason": {"logical_status": "partial_recovered"},
            "retry_of_run_id": "macb_source",
            "recovery_kind": "child_targeted",
            "recovery_scope_json": {"scope": "frozen"},
            "recovery_scope_hash": sha256_json({"scope": "frozen"}),
            "execution_identity_json": identity,
            "execution_identity_hash": sha256_json(identity),
            "execution_identity_evidence_json": evidence,
            "created_at": datetime(2026, 7, 21, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 7, 21, tzinfo=timezone.utc),
        },
        "scheme_results": [],
        "loo": [],
        "children": [
            {
                "child_id": "macbc_successor_baseline",
                "child_key": "baseline:leg_a",
                "child_kind": "baseline",
                "status": "succeeded",
                "execution_disposition": "reuse_result",
                "selected_attempt_id": "macba_successor_baseline_1",
                "source_child_id": "macbc_source_baseline",
                "source_lineage_json": {"source": "baseline"},
                "source_lineage_hash": sha256_json({"source": "baseline"}),
                "input_manifest_json": {
                    "execution_identity": identity,
                    "execution_identity_evidence": evidence,
                },
                "input_manifest_hash": "c" * 64,
            },
            {
                "child_id": "macbc_successor_unavailable",
                "child_key": "scheme:equal",
                "child_kind": "scheme",
                "status": "not_recovered",
                "execution_disposition": "preserve_unavailable",
                "selected_attempt_id": None,
                "source_child_id": "macbc_source_scheme",
                "source_lineage_json": {"source": "scheme"},
                "source_lineage_hash": sha256_json({"source": "scheme"}),
                "input_manifest_json": {
                    "execution_identity": identity,
                    "execution_identity_evidence": evidence,
                },
                "input_manifest_hash": "d" * 64,
            },
        ],
        "attempts": [
            {
                "child_id": "macbc_successor_baseline",
                "attempt_id": "macba_successor_baseline_1",
                "attempt_no": 1,
                "retry_mode": "results_only",
                "execution_kind": "reference_result",
                "status": "succeeded",
                "source_attempt_id": "macba_source_baseline_1",
                "artifact_manifest_json": {
                    "execution_identity": identity,
                    "execution_identity_evidence": evidence,
                },
                "result_manifest_json": result_manifest,
                "result_manifest_hash": sha256_json(result_manifest),
            }
        ],
    }
    repository = _ArchiveRepository(source)
    handler = MultiAlphaCombineArchiveHandler(
        repository=repository,  # type: ignore[arg-type]
        clock=lambda: datetime(2026, 7, 21, tzinfo=timezone.utc),
    )

    result = handler.archive_run("macb_successor")

    assert result["archive_schema_version"] == MULTI_ALPHA_COMBINE_SCHEMA_VERSION_V2
    assert result["status"] == "partial_recovered"
    assert result["recovery_child_count"] == 2
    assert result["recovery_attempt_count"] == 1
    write = repository.writes[-1]
    assert write["run_header"]["execution_identity_hash"] == sha256_json(identity)
    assert write["run_header"]["execution_identity_evidence_json"]["complete"] is True
    unavailable = next(row for row in write["recovery_children"] if row["status"] == "not_recovered")
    assert unavailable["execution_disposition"] == "preserve_unavailable"
    assert write["recovery_attempts"][0]["execution_kind"] == "reference_result"


def test_archive_readback_is_independent_of_deleted_source_and_preserves_v2_history() -> None:
    """Archive detail must be reconstructed only from immutable archive rows."""

    identity = _identity()

    class FakeCursor:
        def __init__(self) -> None:
            self._results = iter(
                (
                    [
                        {
                            "archive_run_id": "macb_successor",
                            "logical_experiment_id": "macb_successor",
                            "archive_attempt_no": 1,
                            "is_latest_attempt": True,
                            "source_system": "multi_alpha",
                            "run_type": "multi_alpha_combine",
                            "archive_status": "partial_recovered",
                            "research_valid": False,
                            "invalid_reason": "multi_alpha_partial_recovered",
                            "completed_at": None,
                            "run_archived_at": datetime(2026, 7, 21, tzinfo=timezone.utc),
                            "run_id": "macb_successor",
                            "archive_schema_version": "v2",
                            "execution_identity_json": identity,
                            "execution_identity_hash": sha256_json(identity),
                        }
                    ],
                    [{"run_id": "macb_successor", "source_id": "macb_successor"}],
                    [{"run_id": "macb_successor", "leg_id": "leg_a", "leg_order": 0}],
                    [],
                    [],
                    [],
                    [
                        {
                            "recovery_child_table": "qe_archive.multi_alpha_recovery_child",
                            "recovery_attempt_table": "qe_archive.multi_alpha_recovery_attempt",
                        }
                    ],
                    [
                        {
                            "run_id": "macb_successor",
                            "child_id": "macbc_successor_unavailable",
                            "status": "not_recovered",
                            "execution_disposition": "preserve_unavailable",
                            "selected_attempt_id": None,
                        }
                    ],
                    [
                        {
                            "run_id": "macb_successor",
                            "child_id": "macbc_successor_baseline",
                            "attempt_id": "macba_successor_baseline_1",
                            "attempt_no": 1,
                            "execution_kind": "reference_result",
                            "status": "succeeded",
                        }
                    ],
                )
            )

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, _sql: str, _params: Any = None) -> None:
            return None

        def fetchall(self):  # type: ignore[no-untyped-def]
            return next(self._results)

    class FakeConnection:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def cursor(self):  # type: ignore[no-untyped-def]
            return FakeCursor()

    snapshot = QEArchiveRepository(connection_provider=lambda: FakeConnection()).fetch_archived_multi_alpha_combine_run(
        "macb_successor"
    )

    assert snapshot is not None
    assert snapshot["archive_run"]["archive_status"] == "partial_recovered"
    assert snapshot["run"]["execution_identity_hash"] == sha256_json(identity)
    assert snapshot["recovery_children"][0]["status"] == "not_recovered"
    assert snapshot["recovery_attempts"][0]["execution_kind"] == "reference_result"
    assert snapshot["recovery_readback_evidence"]["available"] is True


def test_archive_readback_keeps_v1_readable_when_additive_recovery_tables_are_absent() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self._results = iter(
                (
                    [
                        {
                            "archive_run_id": "macb_v1",
                            "logical_experiment_id": "macb_v1",
                            "archive_attempt_no": 1,
                            "is_latest_attempt": True,
                            "source_system": "multi_alpha",
                            "run_type": "multi_alpha_combine",
                            "archive_status": "succeeded",
                            "research_valid": True,
                            "invalid_reason": None,
                            "completed_at": None,
                            "run_archived_at": None,
                            "run_id": "macb_v1",
                        }
                    ],
                    [],
                    [],
                    [],
                    [],
                    [],
                    [{"recovery_child_table": None, "recovery_attempt_table": None}],
                )
            )

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def execute(self, _sql: str, _params: Any = None) -> None:
            return None

        def fetchall(self):  # type: ignore[no-untyped-def]
            return next(self._results)

    class FakeConnection:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

        def cursor(self):  # type: ignore[no-untyped-def]
            return FakeCursor()

    snapshot = QEArchiveRepository(connection_provider=lambda: FakeConnection()).fetch_archived_multi_alpha_combine_run(
        "macb_v1"
    )

    assert snapshot is not None
    assert snapshot["run"].get("archive_schema_version", "v1") == "v1"
    assert snapshot["recovery_children"] == []
    assert snapshot["recovery_attempts"] == []
    assert snapshot["recovery_readback_evidence"]["available"] is False
    assert snapshot["recovery_readback_evidence"]["reason_code"] == "qe_archive_multi_alpha_p0_2_schema_unavailable"


def test_archive_v2_migration_preflight_and_rollback_cover_recovery_contract_without_export() -> None:
    root = Path(__file__).resolve().parents[3]
    forward = (root / "backend/migrations/qe_archive_multi_alpha_p0_2_recovery_20260721.sql").read_text(
        encoding="utf-8"
    )
    preflight = (
        root / "backend/migrations/qe_archive_multi_alpha_p0_2_recovery_20260721.preflight.sql"
    ).read_text(encoding="utf-8")
    rollback = (
        root / "backend/migrations/qe_archive_multi_alpha_p0_2_recovery_20260721.rollback.sql"
    ).read_text(encoding="utf-8")

    for fragment in (
        "CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_recovery_child",
        "CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_recovery_attempt",
        "partial_recovered",
        "cancelled",
        "execution_identity_evidence_json",
        "fk_qear_macb_recovery_child_selected_attempt",
        "uq_qear_macb_recovery_attempt_child_no",
    ):
        assert fragment in forward
    for fragment in (
        "qe_archive_multi_alpha_p0_2_partial_schema_detected",
        "qe_archive_multi_alpha_p0_2_required_constraint_missing",
        "qe_archive_multi_alpha_p0_2_required_index_missing",
    ):
        assert fragment in preflight
    assert "INSERT INTO" not in preflight
    assert "UPDATE " not in preflight
    assert "DELETE FROM" not in preflight
    assert "pg_dump" not in (forward + preflight + rollback).lower()
    assert "qe_archive_multi_alpha_p0_2_rollback_v2_data_present" in rollback
