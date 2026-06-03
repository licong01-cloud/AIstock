"""DR snapshot of the prod AIstock PostgreSQL database (T-PIPE-5.1).

Pipeline-foundation Stage 5 deliverable.

Strategy
--------
1. Read prod connection details from F:/Dev/AIstock/.env (TDX_DB_HOST/PORT/...).
2. Refuse to run unless TDX_DB_PORT == 5432 and TDX_DB_NAME == 'aistock'
   (defends against accidentally snapshotting a dev DB).
3. Locate the local docker container that hosts that prod DB. The default
   name is ``aistock-pg`` for historical hosts, but ``DR_PG_CONTAINER``
   or ``--container`` may override it for hosts whose canonical container
   is named differently. Run pg_dump *inside* that container so the dump
   is produced with the exact pg version that wrote the rows.
4. ``docker cp`` the dump out to ``--target-dir`` (default ``E:/DEV backup/``)
   with filename ``aistock_pg_<YYYYMMDD>.dump`` (or
   ``aistock_pg_<YYYYMM01>_permanent.dump`` on the 1st of each month).
5. Validate the dump via ``pg_restore --list`` and assert it lists at least
   one table; fail fast otherwise.
6. Clean up the temp file inside the container unless ``--keep-temp`` is
   passed.

The actual rotation / retention policy lives in
``scripts/dr_cleanup_old_snapshots.py`` (T-PIPE-5.2). Run that after this
script (the nightly workflow chains them; locally you can run them in
sequence).

Authorization
-------------
This script connects to the production database (read-only -- pg_dump is
SELECT-only by definition). Per AIstock cross-tool protocol, the FIRST
prod snapshot run requires explicit user authorization. Subsequent runs
under the nightly cron + the same target dir are pre-authorized.

Usage
-----
    # Default: run + write to E:/DEV backup/
    python scripts/dr_snapshot_prod_db.py

    # Dry run -- print the plan, no docker / pg_dump invocation
    python scripts/dr_snapshot_prod_db.py --dry-run

    # Custom target dir + container
    python scripts/dr_snapshot_prod_db.py \
        --target-dir D:/dr/aistock --container aistock-pg-prod

    # Keep the in-container temp file for inspection
    python scripts/dr_snapshot_prod_db.py --keep-temp
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Sequence

ENV_FILE = Path("F:/Dev/AIstock/.env")
# Canonical snapshot directory. Per Codex Lane A r3 review (drawer
# a25cd473): the snapshot writer and the dr_validate reader MUST agree on
# this path. The reader (backend/tests/dr/conftest.py) uses the same value;
# updating both keeps the nightly workflow chain consistent.
DEFAULT_TARGET_DIR = Path("E:/DEV backup/aistock_pg_snapshots")
DEFAULT_CONTAINER = "aistock-pg"
CONTAINER_ENV_VAR = "DR_PG_CONTAINER"
DEFAULT_DB_USER_INSIDE = "postgres"
PROD_PORT = 5432
PROD_DBNAME = "aistock"
DUMP_FORMAT = "custom"
DUMP_COMPRESS = "9"
MIN_EXPECTED_DUMP_BYTES = 1024  # anything below this is suspect


@dataclasses.dataclass(frozen=True)
class SnapshotPlan:
    container: str
    pg_user: str
    pg_dbname: str
    target_path: Path
    in_container_path: str
    is_permanent: bool
    snapshot_date: dt.date


def parse_env(env_file: Path = ENV_FILE) -> dict[str, str]:
    if not env_file.exists():
        raise FileNotFoundError(f".env not found at {env_file}")
    cfg: dict[str, str] = {}
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        cfg[key.strip()] = value.strip().strip('"').strip("'")
    return cfg


def assert_prod_target(host: str, port: int, dbname: str) -> None:
    """Refuse to snapshot anything except the canonical production DB."""
    if int(port) != PROD_PORT:
        raise SystemExit(
            f"FATAL: refusing to snapshot port {port}; expected prod {PROD_PORT}. "
            "Check TDX_DB_PORT in .env."
        )
    if dbname != PROD_DBNAME:
        raise SystemExit(
            f"FATAL: refusing to snapshot dbname {dbname!r}; expected {PROD_DBNAME!r}. "
            "Check TDX_DB_NAME in .env."
        )
    if host not in {"127.0.0.1", "localhost", "::1"}:
        # Production is local-loopback in this environment. Remote prod hosts
        # would need an explicit --i-know-what-im-doing override.
        raise SystemExit(
            f"FATAL: refusing to snapshot host {host!r}; expected loopback. "
            "Production is reachable on 127.0.0.1:5432 in this deployment."
        )


def derive_filename(snapshot_date: dt.date) -> tuple[str, bool]:
    """Return ``(filename, is_permanent)`` for the snapshot date.

    Files dated to the 1st of the month are tagged ``_permanent`` so the
    cleanup helper can keep them past the 30-day rolling window.
    """
    yyyymmdd = snapshot_date.strftime("%Y%m%d")
    if snapshot_date.day == 1:
        return f"aistock_pg_{yyyymmdd}_permanent.dump", True
    return f"aistock_pg_{yyyymmdd}.dump", False


def make_plan(
    *,
    cfg: dict[str, str],
    target_dir: Path,
    container: str,
    pg_user: str,
    snapshot_date: dt.date,
) -> SnapshotPlan:
    host = cfg.get("TDX_DB_HOST") or "127.0.0.1"
    port = int(cfg.get("TDX_DB_PORT") or PROD_PORT)
    dbname = cfg.get("TDX_DB_NAME") or PROD_DBNAME
    assert_prod_target(host, port, dbname)
    filename, is_permanent = derive_filename(snapshot_date)
    target_path = target_dir / filename
    in_container_path = f"/tmp/{filename}"
    return SnapshotPlan(
        container=container,
        pg_user=pg_user,
        pg_dbname=dbname,
        target_path=target_path,
        in_container_path=in_container_path,
        is_permanent=is_permanent,
        snapshot_date=snapshot_date,
    )


def run(cmd: Sequence[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    """Thin subprocess wrapper that forwards stdout/stderr by default.

    Tests stub this in to assert command shape without invoking docker/pg_dump.
    """
    if capture:
        return subprocess.run(cmd, check=check, capture_output=True, text=True)
    return subprocess.run(cmd, check=check)


def execute_pg_dump_inside_container(plan: SnapshotPlan) -> None:
    cmd = [
        "docker",
        "exec",
        plan.container,
        "pg_dump",
        "-U",
        plan.pg_user,
        "-d",
        plan.pg_dbname,
        f"--format={DUMP_FORMAT}",
        f"--compress={DUMP_COMPRESS}",
        "--no-owner",
        "--no-acl",
        f"--file={plan.in_container_path}",
    ]
    run(cmd)


def copy_dump_out(plan: SnapshotPlan) -> None:
    plan.target_path.parent.mkdir(parents=True, exist_ok=True)
    src = f"{plan.container}:{plan.in_container_path}"
    cmd = ["docker", "cp", src, str(plan.target_path)]
    run(cmd)


def cleanup_in_container(plan: SnapshotPlan, keep_temp: bool) -> None:
    if keep_temp:
        return
    cmd = ["docker", "exec", plan.container, "rm", "-f", plan.in_container_path]
    run(cmd, check=False)  # cleanup failure is non-fatal


def validate_dump(plan: SnapshotPlan) -> dict[str, object]:
    """Run pg_restore --list against the produced dump and return summary."""
    if not plan.target_path.exists():
        raise SystemExit(f"FATAL: dump not found at {plan.target_path} after docker cp")
    size = plan.target_path.stat().st_size
    if size < MIN_EXPECTED_DUMP_BYTES:
        raise SystemExit(
            f"FATAL: dump at {plan.target_path} is suspiciously small ({size} bytes); "
            "aborting before publishing snapshot."
        )
    # Pipe the file in via stdin. We could also docker cp it back in but
    # streaming via stdin is simpler and avoids round-tripping the file.
    list_cmd = ["docker", "exec", "-i", plan.container, "pg_restore", "--list"]
    with plan.target_path.open("rb") as fh:
        proc = subprocess.run(list_cmd, input=fh.read(), capture_output=True, check=True)
    listing = proc.stdout.decode("utf-8", errors="replace")
    table_lines = [line for line in listing.splitlines() if "TABLE DATA" in line]
    if not table_lines:
        raise SystemExit(
            "FATAL: pg_restore --list returned 0 TABLE DATA entries; "
            "dump is incomplete."
        )
    return {
        "dump_path": str(plan.target_path),
        "dump_bytes": size,
        "table_data_entries": len(table_lines),
        "snapshot_date": plan.snapshot_date.isoformat(),
        "is_permanent": plan.is_permanent,
    }


def render_dry_run(plan: SnapshotPlan) -> str:
    return "\n".join(
        [
            "DRY RUN -- no docker / pg_dump invocation",
            f"  container         : {plan.container}",
            f"  pg user           : {plan.pg_user}",
            f"  pg dbname         : {plan.pg_dbname}",
            f"  in-container path : {plan.in_container_path}",
            f"  target path       : {plan.target_path}",
            f"  is permanent      : {plan.is_permanent}",
            f"  snapshot date     : {plan.snapshot_date.isoformat()}",
        ]
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--target-dir", default=str(DEFAULT_TARGET_DIR), type=Path)
    p.add_argument("--container", default=os.environ.get(CONTAINER_ENV_VAR, DEFAULT_CONTAINER))
    p.add_argument("--pg-user", default=DEFAULT_DB_USER_INSIDE)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--keep-temp", action="store_true")
    p.add_argument(
        "--snapshot-date",
        default=None,
        help="Override snapshot date (YYYY-MM-DD). Defaults to today UTC.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = parse_env()
    snapshot_date = (
        dt.date.fromisoformat(args.snapshot_date)
        if args.snapshot_date
        else dt.datetime.now(dt.timezone.utc).date()
    )
    target_dir = Path(args.target_dir)
    plan = make_plan(
        cfg=cfg,
        target_dir=target_dir,
        container=args.container,
        pg_user=args.pg_user,
        snapshot_date=snapshot_date,
    )
    if args.dry_run:
        print(render_dry_run(plan))
        return 0

    print(f"DR snapshot starting -> {plan.target_path}")
    try:
        execute_pg_dump_inside_container(plan)
        copy_dump_out(plan)
        summary = validate_dump(plan)
    finally:
        cleanup_in_container(plan, keep_temp=args.keep_temp)

    print(json.dumps(summary, indent=2))
    print("\nIf this is the FIRST prod snapshot of the day, please post a")
    print("[INFO] cross-tool drawer recording the dump_path + dump_bytes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
