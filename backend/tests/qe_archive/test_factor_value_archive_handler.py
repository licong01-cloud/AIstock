"""BUG-1001: FactorValueArchiveHandler retirement regression tests.

The ``factor.recompute.completed`` producer (T15 emit hook) and its consumer
``FactorValueArchiveHandler`` were retired. ``qe_archive.factor_value`` is a
write-only table with no production reader, and the archive worker never
registered ``factor.recompute.completed``, so every emitted event accumulated
as an orphan pending outbox row.

These tests assert the retired state is durable and fail-closed:

  - the handler module is gone (removed, not a dormant-but-registered path)
  - no production call chain writes to ``qe_archive.factor_value``
  - the archive worker still does NOT consume ``factor.recompute.completed``
  - the generic ``ArchiveHandler`` contract (used by the surviving multi-alpha
    handler) is unaffected

RED: the first two tests fail against pre-BUG-1001 code where the handler
module exists and is importable.
"""

from __future__ import annotations

import importlib
import sys
import uuid
from contextlib import contextmanager

import psycopg2
import pytest
from psycopg2 import sql


# ---------------------------------------------------------------------------
# Retirement: the handler module and event contract are removed.
# ---------------------------------------------------------------------------

def test_factor_value_archive_handler_module_retired():
    """Requirement 3: the production handler for ``factor.recompute.completed``
    is removed, not left as a seemingly-usable never-consumed parallel path."""
    # Remove from sys.modules in case another test imported it.
    sys.modules.pop("backend.services.qe_archive.handlers.factor_value_archive_handler", None)
    with pytest.raises(ImportError):
        importlib.import_module(
            "backend.services.qe_archive.handlers.factor_value_archive_handler"
        )


def test_worker_service_does_not_consume_factor_recompute():
    """Requirement 9 / production fact: the archive worker's supported event
    set never includes ``factor.recompute.completed`` (that is what turned the
    historical rows into an orphan backlog)."""
    from backend.services.qe_archive.worker_service import SUPPORTED_WORKER_EVENT_TYPES

    assert "factor.recompute.completed" not in SUPPORTED_WORKER_EVENT_TYPES, (
        "the archive worker must not consume factor.recompute.completed"
    )


def test_no_production_chain_writes_qe_archive_factor_value():
    """RED-8 / requirement 8: no production call chain writes to
    ``qe_archive.factor_value``. Scans the production (non-test) backend tree
    for an INSERT into that table — only DDL/comments may mention it."""
    from pathlib import Path

    backend_root = Path(__file__).resolve().parents[2]  # backend/
    offenders: list[str] = []
    for py in backend_root.rglob("*.py"):
        rel = py.relative_to(backend_root.parent)
        parts = rel.parts
        if any(p == "tests" for p in parts):
            continue  # test code is allowed to reference the retired table
        text = py.read_text(encoding="utf-8", errors="replace")
        if "INSERT INTO qe_archive.factor_value" in text or "INTO qe_archive.factor_value" in text:
            offenders.append(str(rel))
    assert not offenders, (
        "production backend code must not write to qe_archive.factor_value; "
        f"found: {offenders}"
    )


# ---------------------------------------------------------------------------
# Historical run snapshot authority is preserved (run completion backfill).
# ---------------------------------------------------------------------------

def test_run_factor_frozen_snapshot_capability_intact():
    """Requirement 5/6/7: the run-completion archive still persists the frozen
    ``independent_metrics_snapshot`` per historical QE run via the repository's
    ``run_factor`` write columns. BUG-1001 removed the *current-metrics* archive
    side effect; it must NOT remove the historical frozen-snapshot capability."""
    from backend.services.qe_archive.repository import FACTOR_COLUMNS, QEArchiveRepository

    assert "independent_metrics_snapshot" in FACTOR_COLUMNS, (
        "run_factor.independent_metrics_snapshot must remain a run-completion "
        "frozen snapshot column"
    )
    assert "official_rating_snapshot" in FACTOR_COLUMNS
    # The repository still exposes the run-completion write entry point.
    assert hasattr(QEArchiveRepository, "replace_run_factors")


def test_current_metric_save_cannot_overwrite_historical_run_snapshot():
    """Requirement 5/6: a current factor metrics recompute never writes to
    ``qe_archive.run_factor`` (the frozen historical snapshot table). The two
    authorities are separate: current metrics live in aistock_factor_metrics,
    historical run snapshots live in run_factor.independent_metrics_snapshot."""
    from pathlib import Path

    backend_root = Path(__file__).resolve().parents[2]  # backend/
    service = backend_root / "services" / "quantevolver" / "factor_official_evaluation_service.py"
    text = service.read_text(encoding="utf-8")
    # The current-metrics save path must not contain a run_factor / qe_archive
    # write (all qe_archive references were removed by BUG-1001).
    assert "qe_archive" not in text, (
        "factor_official_evaluation_service must not reference qe_archive at all; "
        "current-metrics authority stays in aistock_factor_metrics"
    )


# ---------------------------------------------------------------------------
# Generic handler contract is unaffected by the retirement.
# ---------------------------------------------------------------------------

def test_generic_archive_handler_contract_still_importable():
    """The generic ArchiveHandler contract + surviving multi-alpha handler
    remain importable (retirement did not remove shared contract code)."""
    from backend.services.qe_archive.handlers.contract import ArchiveHandler, ArchiveResult, HandlerStatus
    from backend.services.qe_archive.handlers.multi_alpha_combine_archive_handler import (
        MultiAlphaCombineArchiveHandler,
    )

    assert ArchiveHandler is not None
    assert ArchiveResult is not None
    assert HandlerStatus is not None
    assert MultiAlphaCombineArchiveHandler is not None


def test_dev_postgres_metric_transaction_is_archive_independent_and_snapshot_stable(
    dev_db_creds,
    dev_db_available,
    monkeypatch,
):
    """Exercise the real DEV PostgreSQL seam without touching shared DEV facts.

    The metric writer is redirected with ``search_path`` to a disposable schema
    that clones the authoritative metric table. The qualified archive tables
    remain the real DEV readback authority, so this proves that a successful
    metric transaction does not enqueue an archive event or mutate a frozen run
    snapshot. A NOT NULL failure after the writer's DELETE also proves rollback
    restores the preceding committed metric generation.
    """
    from backend.services.quantevolver import factor_official_evaluation_service as service_module
    from backend.services.quantevolver.factor_official_evaluation_service import (
        FactorOfficialEvaluationService,
    )

    assert dev_db_available is True
    schema_name = f"bug1001_{uuid.uuid4().hex[:12]}"
    factor_name = f"BUG1001_DEV_{uuid.uuid4().hex[:12]}"

    def _archive_readback():
        with psycopg2.connect(**dev_db_creds) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM qe_archive.outbox_event
                    WHERE event_type = 'factor.recompute.completed'
                      AND source_system = 'qe_factor_official_evaluation'
                    """
                )
                outbox_count = int(cur.fetchone()[0])
                cur.execute(
                    """
                    SELECT id, run_id, factor_name,
                           independent_metrics_snapshot::text
                    FROM qe_archive.run_factor
                    ORDER BY id DESC
                    LIMIT 50
                    """
                )
                snapshots = tuple(cur.fetchall())
        return outbox_count, snapshots

    with psycopg2.connect(**dev_db_creds) as setup_conn:
        with setup_conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
            cur.execute(
                sql.SQL(
                    "CREATE TABLE {}.aistock_factor_metrics "
                    "(LIKE public.aistock_factor_metrics INCLUDING ALL)"
                ).format(sql.Identifier(schema_name))
            )
            # The DEV base table can legitimately lag an unapplied additive
            # H20 migration.  The disposable schema must nevertheless expose
            # the exact current production-writer contract; adding these
            # nullable columns here exercises the real PostgreSQL statement
            # without mutating shared DEV authority.
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {}.aistock_factor_metrics "
                    "ADD COLUMN IF NOT EXISTS h20_return_horizon TEXT, "
                    "ADD COLUMN IF NOT EXISTS h20_ic_mean DOUBLE PRECISION, "
                    "ADD COLUMN IF NOT EXISTS h20_ic_std DOUBLE PRECISION, "
                    "ADD COLUMN IF NOT EXISTS h20_rank_ic_mean DOUBLE PRECISION, "
                    "ADD COLUMN IF NOT EXISTS h20_rank_ic_std DOUBLE PRECISION, "
                    "ADD COLUMN IF NOT EXISTS h20_icir DOUBLE PRECISION, "
                    "ADD COLUMN IF NOT EXISTS h20_rank_icir DOUBLE PRECISION, "
                    "ADD COLUMN IF NOT EXISTS h20_icir_hac DOUBLE PRECISION, "
                    "ADD COLUMN IF NOT EXISTS h20_rank_icir_hac DOUBLE PRECISION, "
                    "ADD COLUMN IF NOT EXISTS h20_ic_positive_ratio DOUBLE PRECISION, "
                    "ADD COLUMN IF NOT EXISTS h20_n_obs INTEGER, "
                    "ADD COLUMN IF NOT EXISTS h20_hac_lag INTEGER"
                ).format(sql.Identifier(schema_name))
            )
            cur.execute(
                sql.SQL("CREATE SEQUENCE {}.aistock_factor_metrics_id_seq").format(
                    sql.Identifier(schema_name)
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER TABLE {}.aistock_factor_metrics ALTER COLUMN id "
                    "SET DEFAULT nextval(%s::regclass)"
                ).format(sql.Identifier(schema_name)),
                (f"{schema_name}.aistock_factor_metrics_id_seq",),
            )

    @contextmanager
    def _metric_connection():
        conn = psycopg2.connect(**dev_db_creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SET search_path TO {}, public").format(
                        sql.Identifier(schema_name)
                    )
                )
            yield conn
        finally:
            conn.close()

    monkeypatch.setattr(service_module, "get_conn", _metric_connection)
    service = object.__new__(FactorOfficialEvaluationService)
    metric = {
        "factor_name": factor_name,
        "eval_window": "full",
        "data_start": "2024-01-02",
        "data_end": "2026-08-07",
        "ic_mean": 0.01,
        "rank_ic_mean": 0.02,
        "icir": 0.3,
        "rank_icir": 0.4,
    }

    before_outbox, before_snapshots = _archive_readback()
    try:
        result = service._save_metrics(
            {"calc_batch_id": "bug1001_dev_success", "metrics": [metric]},
            snapshot_date="2026-08-07",
            factor_ids={factor_name: 1},
        )
        assert result["inserted"] == 1

        with psycopg2.connect(**dev_db_creds) as readback_conn:
            with readback_conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT factor_name, calc_engine, data_start::text, data_end::text "
                        "FROM {}.aistock_factor_metrics"
                    ).format(sql.Identifier(schema_name))
                )
                assert cur.fetchall() == [
                    (factor_name, "qe_eval_v2", metric["data_start"], metric["data_end"])
                ]

        invalid_metric = dict(metric)
        invalid_metric["data_start"] = None
        with pytest.raises(psycopg2.errors.NotNullViolation):
            service._save_metrics(
                {"calc_batch_id": "bug1001_dev_rollback", "metrics": [invalid_metric]},
                snapshot_date="2026-08-07",
                factor_ids={factor_name: 1},
            )

        with psycopg2.connect(**dev_db_creds) as rollback_readback_conn:
            with rollback_readback_conn.cursor() as cur:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.aistock_factor_metrics").format(
                        sql.Identifier(schema_name)
                    )
                )
                assert int(cur.fetchone()[0]) == 1

        after_outbox, after_snapshots = _archive_readback()
        assert after_outbox == before_outbox
        assert after_snapshots == before_snapshots
    finally:
        with psycopg2.connect(**dev_db_creds) as cleanup_conn:
            with cleanup_conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
                )
        with psycopg2.connect(**dev_db_creds) as cleanup_readback_conn:
            with cleanup_readback_conn.cursor() as cur:
                cur.execute("SELECT to_regnamespace(%s)", (schema_name,))
                assert cur.fetchone()[0] is None
