"""Dev-DB coverage for StrategyPackageService.enable_paper lifecycle semantics.

These tests exercise the real PostgreSQL-backed repository against
``aistock_dev``. They mirror the in-memory lifecycle invariants in
``backend/tests/strategy_package/test_enable_paper_invariants.py``: an already
``PAPER_ENABLED`` package must fail fast with ``InvalidStateTransitionError``,
and asset-check failures must keep explicit validation context without silent
state mutation.

Boundary: every test inserts records tagged with ``package_id LIKE
'pkg_test_int6_%'`` and DELETEs them on teardown. No existing dev-DB packages
are mutated.
"""

from __future__ import annotations

from typing import Iterator
from uuid import uuid4

import psycopg2
import pytest

from backend.services.strategy_package.models import (
    AssetCheck,
    PackageStatus,
)
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import InvalidStateTransitionError, StrategyPackageValidationError
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
    repo.save_manifest(manifest)

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
# INT-6a - persisted PAPER_ENABLED rejects re-entry explicitly
# ---------------------------------------------------------------------------


def test_runtime_enable_paper_enabled_status_reentry_fails_fast(
    dev_pkg_repo: StrategyPackageRepository,
) -> None:
    """PAPER_ENABLED rows are formal lifecycle state; re-entry is rejected."""
    pkg_id = _seed_test_package(
        dev_pkg_repo,
        persisted_status=PackageStatus.PAPER_ENABLED,
        asset_checks_passing=True,
    )
    service = StrategyPackageService(repository=dev_pkg_repo)
    _seed_paper_ready_package(service, pkg_id)

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        service.enable_paper(pkg_id)

    err = exc_info.value
    assert err.context["package_id"] == pkg_id
    assert err.context["from_status"] == PackageStatus.PAPER_ENABLED.value
    assert err.context["to_status"] == PackageStatus.PAPER_ENABLED.value
    assert err.context["allowed_from"] == [
        PackageStatus.BACKTEST_APPROVED.value,
        PackageStatus.SELECTION_ENABLED.value,
    ]

    # The failed re-entry must not mutate the row.
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
    assert row[0] == PackageStatus.PAPER_ENABLED.value


# ---------------------------------------------------------------------------
# INT-6b — strict-gate validator failure (asset_checks)
# ---------------------------------------------------------------------------


def test_runtime_handles_enable_paper_strict_gate_failure(
    dev_pkg_repo: StrategyPackageRepository,
) -> None:
    """enable_paper on a SELECTION_ENABLED package whose manifest has a
    failed asset_check raises StrategyPackageValidationError.

    Asset eligibility runs before the repository compare-and-set, so even
    though the persisted transition (SELECTION_ENABLED -> PAPER_ENABLED) is
    allowed, the manifest asset-check gate fails fast.

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
    # Seed governance fixtures; the asset-check failure remains the operative
    # blocker, but seeding avoids unrelated blockers crowding the context.
    _seed_paper_ready_package(service, pkg_id)

    with pytest.raises(StrategyPackageValidationError) as exc_info:
        service.enable_paper(pkg_id)

    err = exc_info.value
    context = getattr(err, "context", {}) or {}
    msg = str(err)
    # Paper simulation admission wraps the validator's asset-check failure as a
    # blocker string inside the alpha-core admission context.
    # Audit-grade observability: the failed asset-check must remain
    # discoverable so operators know which gate fired.
    alpha_core_identity = context.get("alpha_core_identity") or {}
    blockers_str = " ".join([*(context.get("blockers") or []), *(alpha_core_identity.get("blockers") or [])])
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
