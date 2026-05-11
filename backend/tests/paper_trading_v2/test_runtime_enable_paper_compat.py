"""Phase 3 INT-6: enable_paper() fail-fast compat against the dev DB.

测试范围: **pre-d1ca0ba** enable_paper narrow gate
(manifest_identity + original_fixed_weight_retest), 对应 commit
9cd4c9b (in-memory invariants) + 4528a32 (T8-C router 409).

注意: codex/qe-governance-integration-20260509 上的 d1ca0ba 引入了
governance hard gate (paper_ready=true 才能 enable). d1ca0ba 决定不
merge 到 main 在 Phase 3 全绿前. 因此本测试 **不覆盖** d1ca0ba 路径.

TODO INT-7: d1ca0ba 合 main 后启用
``backend/tests/paper_trading_v2/test_runtime_enable_paper_strict_gate_compat.py``
测试新路径 (paper_ready=false → StrategyPackageValidationError +
governance_eligibility detail).

REV-1 P1.1: Codex review noted INT-6 不测 d1ca0ba 路径; this docstring
clarifies that gap and INT-7 placeholder reserves the slot.

---

These tests exercise StrategyPackageService.enable_paper end-to-end against
the real PG repository (``aistock_dev``). They mirror the in-memory
invariants in ``backend/tests/strategy_package/test_enable_paper_invariants.py``
(commit 9cd4c9b) but with the actual database-backed transition_status path
(repository.py line 188-237).

Boundary: every test inserts records tagged with ``package_id LIKE 'pkg_test_int6_%'``
and DELETEs them on teardown. No existing dev-DB packages are mutated.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterator
from uuid import uuid4

import psycopg2
import pytest

from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import (
    AssetCheck,
    PackageStatus,
)
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import (
    InvalidStateTransitionError,
    StrategyPackageValidationError,
)
from backend.tests.paper_trading_v2.fixtures_dev_db import _dev_dsn
from backend.tests.strategy_package.test_enable_paper_router_409 import (
    _seed_paper_ready_package,
)
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def _dev_conn_factory():
    """Return a context-manager-yielding callable for dev DB.

    psycopg2 connections are NOT context managers in the way the repo expects
    (entering returns the connection, exiting commits/rollbacks). We provide
    that shape via a contextmanager closure.
    """
    from contextlib import contextmanager

    @contextmanager
    def _ctx():
        conn = psycopg2.connect(**_dev_dsn())
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    return _ctx


@pytest.fixture
def dev_pkg_repo() -> Iterator[StrategyPackageRepository]:
    """Yield a real PG-backed repo wired to dev DB; cleanup test rows on teardown.

    Cleanup is scoped strictly to ``package_id LIKE 'pkg_test_int6_%'`` and
    cascades to ``strategy_pkg.package_status_event`` rows that reference
    those package IDs. NEVER unscoped-deletes.
    """
    repo = StrategyPackageRepository(conn_factory=_dev_conn_factory())
    yield repo
    # Teardown: drop all rows tagged with our test prefix.
    # R6: governance gate prereq seed writes to package_asset,
    # package_runtime_variant, and package_validation_run; clean those too.
    conn = psycopg2.connect(**_dev_dsn())
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM strategy_pkg.package_status_event "
                "WHERE package_id LIKE 'pkg_test_int6_%%'"
            )
            cur.execute(
                "DELETE FROM strategy_pkg.package_validation_run "
                "WHERE package_id LIKE 'pkg_test_int6_%%'"
            )
            cur.execute(
                "DELETE FROM strategy_pkg.package_runtime_variant "
                "WHERE package_id LIKE 'pkg_test_int6_%%'"
            )
            cur.execute(
                "DELETE FROM strategy_pkg.package_asset "
                "WHERE package_id LIKE 'pkg_test_int6_%%'"
            )
            cur.execute(
                "DELETE FROM strategy_pkg.package WHERE package_id LIKE 'pkg_test_int6_%%'"
            )
    finally:
        conn.close()


def _seed_test_package(
    repo: StrategyPackageRepository,
    *,
    persisted_status: PackageStatus,
    asset_checks_passing: bool = True,
):
    """Seed a synthetic test package into dev DB.

    Returns the inserted ``StrategyPackageRecord``. The package_id is
    overridden to ``pkg_test_int6_<uuid>`` for cleanup boundary safety.
    Persisted ``package_status`` is set independently of the manifest's
    declared status — the manifest carries BACKTEST_APPROVED (so it passes
    the validator's identity gate), while the row is then UPDATEd to
    ``persisted_status`` to drive the state-machine path.
    """
    base_manifest = make_manifest()
    if not asset_checks_passing:
        base_manifest = base_manifest.model_copy(
            update={
                "asset_checks": [
                    AssetCheck(
                        check_name="synthetic_failure",
                        passed=False,
                        message="injected by INT-6 test",
                        context={"reason": "force validator failure"},
                    )
                ]
            }
        )
    test_pkg_id = f"pkg_test_int6_{uuid4().hex[:16]}"
    manifest = base_manifest.model_copy(
        update={
            "package_id": test_pkg_id,
            "package_status": PackageStatus.BACKTEST_APPROVED,
        }
    )
    record = repo.save_manifest(manifest)

    # Now coerce the persisted package_status if it differs from BACKTEST_APPROVED.
    if persisted_status != PackageStatus.BACKTEST_APPROVED:
        conn = psycopg2.connect(**_dev_dsn())
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE strategy_pkg.package "
                    "SET package_status = %s WHERE package_id = %s",
                    (persisted_status.value, test_pkg_id),
                )
        finally:
            conn.close()
    return test_pkg_id


# ---------------------------------------------------------------------------
# INT-6a — InvalidStateTransitionError on already-PAPER_ENABLED package
# ---------------------------------------------------------------------------


def test_runtime_handles_invalid_state_409(dev_pkg_repo: StrategyPackageRepository) -> None:
    """enable_paper on an already-PAPER_ENABLED package raises
    InvalidStateTransitionError — the state-machine compare-and-set in
    repository.transition_status (line 198-207) blocks re-entry.

    This is the exception that T8-C (commit 4528a32) maps to HTTP 409 in
    the router layer. We test the service-layer raise here; HTTP wiring is
    covered by ``backend/tests/strategy_package/test_enable_paper_router_409.py``.
    """
    pkg_id = _seed_test_package(
        dev_pkg_repo,
        persisted_status=PackageStatus.PAPER_ENABLED,
        asset_checks_passing=True,
    )
    service = StrategyPackageService(repository=dev_pkg_repo)
    # R6: governance gate prereq seed; reaches legacy state-machine raise
    # post-gate. Without this, _require_governance_paper_ready would block
    # before the InvalidStateTransitionError state-machine compare-and-set.
    _seed_paper_ready_package(service, pkg_id)

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        service.enable_paper(pkg_id)

    err = exc_info.value
    context = getattr(err, "context", {}) or {}
    # Either the error message or the context must surface the from_status
    # so an operator can diagnose without database access.
    msg = str(err)
    surfaces_state = (
        PackageStatus.PAPER_ENABLED.value in msg
        or context.get("from_status") == PackageStatus.PAPER_ENABLED.value
    )
    assert surfaces_state, (
        f"InvalidStateTransitionError must surface from_status; got msg={msg!r} "
        f"context={context!r}"
    )

    # Also verify no silent state mutation: the row must remain PAPER_ENABLED.
    conn = psycopg2.connect(**_dev_dsn())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT package_status FROM strategy_pkg.package WHERE package_id = %s",
                (pkg_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == PackageStatus.PAPER_ENABLED.value, (
        f"enable_paper must NOT mutate state on raise; got {row[0]!r}"
    )


# ---------------------------------------------------------------------------
# INT-6b — strict-gate validator failure (asset_checks)
# ---------------------------------------------------------------------------


def test_runtime_handles_enable_paper_strict_gate_failure(
    dev_pkg_repo: StrategyPackageRepository,
) -> None:
    """enable_paper on a SELECTION_ENABLED package whose manifest has a
    failed asset_check raises StrategyPackageValidationError.

    The validator path runs BEFORE the state-machine compare-and-set
    (service.py line 320-326), so even though the persisted state
    transition (SELECTION_ENABLED -> PAPER_ENABLED) IS allowed, the
    validator's manifest gate fails fast.

    This is what audit-grade observability means: the failure surfaces the
    failed-checks context for downstream operator triage rather than
    silently no-op'ing the transition.
    """
    pkg_id = _seed_test_package(
        dev_pkg_repo,
        persisted_status=PackageStatus.SELECTION_ENABLED,
        asset_checks_passing=False,
    )
    service = StrategyPackageService(repository=dev_pkg_repo)
    # R6: governance gate prereq seed; the asset-check failure remains the
    # operative blocker (caught inside _manifest_identity_gate), but seeding
    # the other gates avoids unrelated blockers crowding the eligibility dict.
    _seed_paper_ready_package(service, pkg_id)

    with pytest.raises(StrategyPackageValidationError) as exc_info:
        service.enable_paper(pkg_id)

    err = exc_info.value
    context = getattr(err, "context", {}) or {}
    msg = str(err)
    # R6: governance gate wraps the validator's asset-check failure as a
    # blocker string inside ``context["manifest_identity"]["blockers"]``.
    # Audit-grade observability: the failed asset-check must remain
    # discoverable so operators know which gate fired.
    manifest_identity = context.get("manifest_identity") or {}
    blockers_str = " ".join(manifest_identity.get("blockers") or [])
    surfaces_asset_check = (
        "asset" in msg.lower()
        or "asset" in blockers_str.lower()
        or "asset_checks" in str(context)
        or "failed_checks" in str(context)
    )
    assert surfaces_asset_check, (
        f"strict gate failure must surface asset-check context; "
        f"got msg={msg!r} context={context!r}"
    )

    # Verify no silent state mutation: row stays SELECTION_ENABLED.
    conn = psycopg2.connect(**_dev_dsn())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT package_status FROM strategy_pkg.package WHERE package_id = %s",
                (pkg_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == PackageStatus.SELECTION_ENABLED.value, (
        f"validator failure must NOT mutate state; got {row[0]!r}"
    )
