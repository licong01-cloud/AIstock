"""Apply the DR retention policy to ``E:/DEV backup/`` (T-PIPE-5.2).

Pipeline-foundation Stage 5 deliverable.

Retention policy
----------------
Per the user-confirmed D4 decision (cross-tool drawer 2026-05-10):

- 30-day rolling: keep every ``aistock_pg_<YYYYMMDD>.dump`` whose snapshot
  date is within 30 days of "today" (defaults to UTC today; override with
  ``--reference-date``)
- 1st-of-month permanent: any file ending ``_permanent.dump`` is kept
  forever, regardless of age. ``dr_snapshot_prod_db.py`` produces these
  on day 1 of each month.

Anything else under ``--target-dir`` matching ``aistock_pg_*.dump`` that
falls outside the rolling window AND is not marked ``_permanent`` is
deleted.

Defaults to ``--dry-run``: prints the plan, deletes nothing. Pass
``--apply`` to actually unlink.

Usage
-----
    # Default: dry run -- print what would be deleted
    python scripts/dr_cleanup_old_snapshots.py

    # Apply
    python scripts/dr_cleanup_old_snapshots.py --apply

    # Override the reference date (useful for tests / what-if)
    python scripts/dr_cleanup_old_snapshots.py --reference-date 2026-06-15
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import re
import sys
from pathlib import Path
from typing import Iterable, Sequence

DEFAULT_TARGET_DIR = Path("E:/DEV backup/aistock_pg_snapshots")
DEFAULT_RETENTION_DAYS = 30
SNAPSHOT_FILENAME_RE = re.compile(
    r"^aistock_pg_(?P<date>\d{8})(?P<permanent>_permanent)?\.dump$"
)


@dataclasses.dataclass(frozen=True)
class SnapshotFile:
    path: Path
    snapshot_date: dt.date
    is_permanent: bool

    @property
    def name(self) -> str:
        return self.path.name


@dataclasses.dataclass(frozen=True)
class CleanupPlan:
    keep: list[SnapshotFile]
    delete: list[SnapshotFile]
    skipped: list[Path]  # files that did not match the naming convention


def parse_snapshot_filename(path: Path) -> SnapshotFile | None:
    m = SNAPSHOT_FILENAME_RE.match(path.name)
    if not m:
        return None
    try:
        snap_date = dt.datetime.strptime(m.group("date"), "%Y%m%d").date()
    except ValueError:
        return None
    return SnapshotFile(
        path=path,
        snapshot_date=snap_date,
        is_permanent=bool(m.group("permanent")),
    )


def scan_target_dir(target_dir: Path) -> tuple[list[SnapshotFile], list[Path]]:
    if not target_dir.exists():
        return [], []
    snapshots: list[SnapshotFile] = []
    skipped: list[Path] = []
    for entry in target_dir.iterdir():
        if not entry.is_file():
            continue
        if not entry.name.startswith("aistock_pg_"):
            continue
        parsed = parse_snapshot_filename(entry)
        if parsed is None:
            skipped.append(entry)
            continue
        snapshots.append(parsed)
    return snapshots, skipped


def compute_plan(
    snapshots: Sequence[SnapshotFile],
    *,
    reference_date: dt.date,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    skipped: Iterable[Path] = (),
) -> CleanupPlan:
    keep: list[SnapshotFile] = []
    delete: list[SnapshotFile] = []
    threshold = reference_date - dt.timedelta(days=retention_days)
    for snap in snapshots:
        if snap.is_permanent:
            keep.append(snap)
            continue
        if snap.snapshot_date >= threshold:
            keep.append(snap)
            continue
        delete.append(snap)
    return CleanupPlan(keep=sorted(keep, key=lambda s: s.snapshot_date),
                      delete=sorted(delete, key=lambda s: s.snapshot_date),
                      skipped=list(skipped))


def render_plan(plan: CleanupPlan, *, reference_date: dt.date, retention_days: int) -> str:
    lines = [
        f"DR cleanup plan (reference_date={reference_date.isoformat()}, "
        f"retention_days={retention_days})",
        "",
        f"KEEP ({len(plan.keep)}):",
    ]
    for snap in plan.keep:
        lines.append(
            f"  {snap.name}  ({'PERMANENT' if snap.is_permanent else 'rolling'}, "
            f"{snap.snapshot_date.isoformat()})"
        )
    lines.append("")
    lines.append(f"DELETE ({len(plan.delete)}):")
    for snap in plan.delete:
        lines.append(f"  {snap.name}  ({snap.snapshot_date.isoformat()}, age out of window)")
    if plan.skipped:
        lines.append("")
        lines.append(f"SKIPPED ({len(plan.skipped)}, not matching aistock_pg_<YYYYMMDD>.dump):")
        for p in plan.skipped:
            lines.append(f"  {p.name}")
    return "\n".join(lines)


def apply_plan(plan: CleanupPlan) -> tuple[int, int]:
    """Delete files in ``plan.delete``. Returns ``(deleted, errors)``."""
    deleted = 0
    errors = 0
    for snap in plan.delete:
        try:
            snap.path.unlink()
            deleted += 1
        except OSError as exc:  # noqa: BLE001 - want exception text in stderr
            print(f"ERROR deleting {snap.path}: {exc}", file=sys.stderr)
            errors += 1
    return deleted, errors


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--target-dir", default=str(DEFAULT_TARGET_DIR), type=Path)
    p.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Rolling-window length in days (default {DEFAULT_RETENTION_DAYS}).",
    )
    p.add_argument(
        "--reference-date",
        default=None,
        help="Pivot date for the rolling window (YYYY-MM-DD). Defaults to today UTC.",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete files. Without this flag the script is a dry run.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    reference_date = (
        dt.date.fromisoformat(args.reference_date)
        if args.reference_date
        else dt.datetime.now(dt.timezone.utc).date()
    )
    snapshots, skipped = scan_target_dir(Path(args.target_dir))
    plan = compute_plan(
        snapshots,
        reference_date=reference_date,
        retention_days=args.retention_days,
        skipped=skipped,
    )
    print(render_plan(plan, reference_date=reference_date, retention_days=args.retention_days))
    if not args.apply:
        print("\nDRY RUN. Pass --apply to actually delete the listed files.")
        return 0
    deleted, errors = apply_plan(plan)
    print(f"\nApplied: deleted={deleted}, errors={errors}")
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
