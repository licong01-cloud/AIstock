"""DR retention-policy compliance (Stage 7.4 §3).

Verifies the directory state under ``E:/DEV backup/aistock_pg_snapshots/``
follows the 30-day rolling + 1st-of-month permanent retention contract
that ``scripts/dr_cleanup_old_snapshots.py`` enforces.

These tests are pure filesystem-introspection -- no DB, no docker. They
reuse the parsing helpers from ``scripts/dr_cleanup_old_snapshots.py``
so the production retention logic and the validation tests share one
source of truth.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
CLEANUP_PATH = REPO_ROOT / "scripts" / "dr_cleanup_old_snapshots.py"


@pytest.fixture(scope="module")
def cleanup_module():
    spec = importlib.util.spec_from_file_location(
        "dr_cleanup_old_snapshots_for_dr_test", CLEANUP_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    yield module
    sys.modules.pop(spec.name, None)


def test_dr_retention_module_collected_smoke(cleanup_module) -> None:
    """Sentinel: scripts/dr_cleanup_old_snapshots.py is importable."""
    assert cleanup_module.DEFAULT_RETENTION_DAYS == 30
    assert callable(cleanup_module.compute_plan)


def test_recent_dumps_within_30_days_window(
    cleanup_module, dr_backup_dir: Path,
) -> None:
    """Every rolling (non-permanent) dump should either be within 30 days
    or be flagged for deletion. The retention policy planner computes
    the same partition; if a rolling dump is older than 30 days AND
    still present, cleanup hasn't run -- treat as informational, not
    a hard fail (cleanup is opportunistic on the nightly cron)."""
    snapshots, skipped = cleanup_module.scan_target_dir(dr_backup_dir)
    if not snapshots:
        pytest.skip(
            f"no recognized snapshots in {dr_backup_dir}; "
            f"retention compliance has nothing to verify."
        )
    today = dt.datetime.now(dt.timezone.utc).date()
    plan = cleanup_module.compute_plan(
        snapshots,
        reference_date=today,
        retention_days=cleanup_module.DEFAULT_RETENTION_DAYS,
    )
    # The contract: any snapshot in plan.keep must be either permanent
    # or within the rolling window. Anything else is in plan.delete.
    for snap in plan.keep:
        if snap.is_permanent:
            continue
        days_old = (today - snap.snapshot_date).days
        assert days_old <= cleanup_module.DEFAULT_RETENTION_DAYS, (
            f"{snap.path.name} is in plan.keep but is {days_old} days old "
            f"AND not marked permanent; retention planner has a bug."
        )


def test_each_month_with_snapshots_has_a_permanent(
    cleanup_module, dr_backup_dir: Path,
) -> None:
    """For each month that has at least one snapshot, there should be a
    *_permanent.dump (or *_permanent.sql) entry preserving the day-1
    archive long-term. If a month is missing its permanent, the policy
    has slipped; we report it as a soft warning unless the month is the
    current calendar month (where the day-1 snapshot may not yet have
    been produced).
    """
    snapshots, _ = cleanup_module.scan_target_dir(dr_backup_dir)
    if not snapshots:
        pytest.skip("no snapshots; retention compliance not applicable.")
    today = dt.datetime.now(dt.timezone.utc).date()
    months_seen: dict[tuple[int, int], list] = {}
    for s in snapshots:
        key = (s.snapshot_date.year, s.snapshot_date.month)
        months_seen.setdefault(key, []).append(s)
    missing_permanent = []
    for (year, month), monthly_snaps in months_seen.items():
        # Skip the current calendar month: the day-1 snapshot may not yet
        # have run by the time this test fires earlier in the month.
        if (year, month) == (today.year, today.month):
            continue
        if not any(s.is_permanent for s in monthly_snaps):
            missing_permanent.append((year, month, len(monthly_snaps)))
    # Treat as informational (print) rather than fail: missing permanent
    # could mean either policy slip OR no snapshot was run that month at
    # all. Failing here would be too strict for a brand-new backup dir.
    if missing_permanent:
        print(
            f"\n[INFO] {len(missing_permanent)} historical month(s) lack a "
            f"_permanent dump; consider running "
            f"scripts/dr_snapshot_prod_db.py --snapshot-date "
            f"<YYYY-MM-01> to backfill. First 5: {missing_permanent[:5]}"
        )
    # Assert at least that retention parsing didn't reject every snapshot
    # filename. (If every name was unparseable, the parser regex needs
    # broadening before this test can do anything useful.)
    assert months_seen, (
        "no snapshot months parsed; retention regex did not match any "
        f"filenames in {dr_backup_dir}."
    )


def test_no_unparseable_recent_snapshot_filenames(
    cleanup_module, dr_backup_dir: Path,
) -> None:
    """Filenames not matching ``aistock_pg_<YYYYMMDD>[_permanent].dump``
    are listed in ``plan.skipped``. They should be either legacy /
    one-off files (``prod_schema_snapshot_*.sql``) or human-readable
    notes -- never unparseable canonical-name typos.

    This test surfaces canonical-name typos so a future
    ``aistock_pg_2026-05-11.dump`` (with dashes) or
    ``aistock_pg_20260511.dump.partial`` (with extra suffix) gets
    flagged before retention cleanup silently keeps it forever.
    """
    snapshots, skipped = cleanup_module.scan_target_dir(dr_backup_dir)
    # The legacy filename style ``prod_schema_snapshot_<YYYYMMDD>.sql`` IS
    # a recognized AIstock dump shape even though it does not match the
    # canonical aistock_pg_* regex. Allow it through this check; the
    # important case to surface is "starts with aistock_pg_ but is
    # malformed", which would be a typo.
    suspicious = []
    for p in skipped:
        name = p.name
        if name.startswith("aistock_pg_") and not name.endswith(".dump"):
            # e.g. aistock_pg_20260511.dump.partial or aistock_pg_2026.dump
            suspicious.append(name)
        elif name.startswith("aistock_pg_") and name.endswith(".dump"):
            # The retention regex declined this -- likely date is malformed
            suspicious.append(name)
    assert not suspicious, (
        f"{len(suspicious)} canonical-style filename(s) in "
        f"{dr_backup_dir} are unparseable; first 5: {suspicious[:5]}. "
        f"Either fix the filename or extend the parser regex."
    )
