"""Regression tests for scripts/dr_cleanup_old_snapshots.py (T-PIPE-5.2).

Covers:
- filename parsing (rolling vs permanent vs malformed)
- 30-day rolling window math (boundary inclusive)
- ``_permanent`` files always kept
- skipped (non-matching) filenames are surfaced but not deleted
- ``apply_plan`` actually unlinks files in dry mode dirs
- ``main()`` honors --dry-run by default and --apply flag
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DR_CLEANUP_PATH = REPO_ROOT / "scripts" / "dr_cleanup_old_snapshots.py"


@pytest.fixture
def cleanup_module():
    spec = importlib.util.spec_from_file_location("dr_cleanup_old_snapshots", DR_CLEANUP_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["dr_cleanup_old_snapshots"] = module
    spec.loader.exec_module(module)
    yield module
    sys.modules.pop("dr_cleanup_old_snapshots", None)


def _touch(p: Path) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"placeholder")
    return p


def test_parse_filename_recognizes_rolling(cleanup_module, tmp_path):
    f = _touch(tmp_path / "aistock_pg_20260510.dump")
    parsed = cleanup_module.parse_snapshot_filename(f)
    assert parsed is not None
    assert parsed.snapshot_date == dt.date(2026, 5, 10)
    assert parsed.is_permanent is False


def test_parse_filename_recognizes_permanent(cleanup_module, tmp_path):
    f = _touch(tmp_path / "aistock_pg_20260501_permanent.dump")
    parsed = cleanup_module.parse_snapshot_filename(f)
    assert parsed is not None
    assert parsed.snapshot_date == dt.date(2026, 5, 1)
    assert parsed.is_permanent is True


def test_parse_filename_rejects_malformed(cleanup_module, tmp_path):
    bad = _touch(tmp_path / "aistock_pg_2026-05-10.dump")
    assert cleanup_module.parse_snapshot_filename(bad) is None
    bad2 = _touch(tmp_path / "aistock_pg_random_string.dump")
    assert cleanup_module.parse_snapshot_filename(bad2) is None
    bad3 = _touch(tmp_path / "aistock_pg_20260230.dump")  # invalid Feb 30
    assert cleanup_module.parse_snapshot_filename(bad3) is None


def test_scan_target_dir_collects_snapshots(cleanup_module, tmp_path):
    _touch(tmp_path / "aistock_pg_20260510.dump")
    _touch(tmp_path / "aistock_pg_20260501_permanent.dump")
    _touch(tmp_path / "aistock_pg_2026-05-09.dump")  # malformed
    _touch(tmp_path / "unrelated_file.txt")
    snapshots, skipped = cleanup_module.scan_target_dir(tmp_path)
    names = sorted(s.path.name for s in snapshots)
    assert names == [
        "aistock_pg_20260501_permanent.dump",
        "aistock_pg_20260510.dump",
    ]
    skipped_names = sorted(p.name for p in skipped)
    assert skipped_names == ["aistock_pg_2026-05-09.dump"]


def test_compute_plan_keeps_within_30_days(cleanup_module, tmp_path):
    today = dt.date(2026, 6, 15)
    files = [
        _touch(tmp_path / "aistock_pg_20260616.dump"),  # future
        _touch(tmp_path / "aistock_pg_20260616_permanent.dump"),
        _touch(tmp_path / "aistock_pg_20260615.dump"),
        _touch(tmp_path / "aistock_pg_20260516.dump"),  # exactly 30 days back -> kept
        _touch(tmp_path / "aistock_pg_20260515.dump"),  # 31 days -> deleted
        _touch(tmp_path / "aistock_pg_20260501_permanent.dump"),
        _touch(tmp_path / "aistock_pg_20260101.dump"),  # 5+ months -> deleted
    ]
    snaps = [cleanup_module.parse_snapshot_filename(f) for f in files]
    snaps = [s for s in snaps if s is not None]
    plan = cleanup_module.compute_plan(snaps, reference_date=today, retention_days=30)
    keep_names = sorted(s.path.name for s in plan.keep)
    delete_names = sorted(s.path.name for s in plan.delete)
    assert "aistock_pg_20260616_permanent.dump" in keep_names
    assert "aistock_pg_20260501_permanent.dump" in keep_names  # permanent always kept
    assert "aistock_pg_20260615.dump" in keep_names
    assert "aistock_pg_20260516.dump" in keep_names  # boundary inclusive
    assert "aistock_pg_20260515.dump" in delete_names
    assert "aistock_pg_20260101.dump" in delete_names


def test_compute_plan_permanent_overrides_age(cleanup_module, tmp_path):
    today = dt.date(2027, 1, 1)
    f = _touch(tmp_path / "aistock_pg_20260501_permanent.dump")
    snap = cleanup_module.parse_snapshot_filename(f)
    plan = cleanup_module.compute_plan([snap], reference_date=today, retention_days=30)
    assert plan.delete == []
    assert plan.keep == [snap]


def test_apply_plan_unlinks_files(cleanup_module, tmp_path):
    today = dt.date(2026, 6, 15)
    keep_file = _touch(tmp_path / "aistock_pg_20260615.dump")
    del_file = _touch(tmp_path / "aistock_pg_20260101.dump")
    snaps = [
        cleanup_module.parse_snapshot_filename(keep_file),
        cleanup_module.parse_snapshot_filename(del_file),
    ]
    plan = cleanup_module.compute_plan(snaps, reference_date=today)
    deleted, errors = cleanup_module.apply_plan(plan)
    assert deleted == 1
    assert errors == 0
    assert keep_file.exists()
    assert not del_file.exists()


def test_main_dry_run_does_not_delete(cleanup_module, tmp_path, capsys):
    _touch(tmp_path / "aistock_pg_20260101.dump")
    rc = cleanup_module.main(["--target-dir", str(tmp_path), "--reference-date", "2026-06-15"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "DRY RUN" in out
    assert (tmp_path / "aistock_pg_20260101.dump").exists()


def test_main_apply_deletes(cleanup_module, tmp_path, capsys):
    _touch(tmp_path / "aistock_pg_20260101.dump")
    _touch(tmp_path / "aistock_pg_20260615.dump")
    rc = cleanup_module.main(
        [
            "--target-dir",
            str(tmp_path),
            "--reference-date",
            "2026-06-15",
            "--apply",
        ]
    )
    assert rc == 0
    assert not (tmp_path / "aistock_pg_20260101.dump").exists()
    assert (tmp_path / "aistock_pg_20260615.dump").exists()


def test_main_handles_missing_target_dir(cleanup_module, tmp_path, capsys):
    rc = cleanup_module.main(["--target-dir", str(tmp_path / "nonexistent")])
    assert rc == 0
    out = capsys.readouterr().out
    assert "KEEP (0)" in out
    assert "DELETE (0)" in out


def test_render_plan_lists_skipped(cleanup_module, tmp_path):
    today = dt.date(2026, 6, 15)
    skipped_path = tmp_path / "aistock_pg_garbage.dump"
    plan = cleanup_module.CleanupPlan(keep=[], delete=[], skipped=[skipped_path])
    text = cleanup_module.render_plan(plan, reference_date=today, retention_days=30)
    assert "aistock_pg_garbage.dump" in text
    assert "SKIPPED" in text
