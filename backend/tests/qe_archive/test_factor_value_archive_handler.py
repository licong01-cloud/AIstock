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

import pytest


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
