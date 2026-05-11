"""Shared fixtures for Stage 7.4 DR validation tests.

Locates the local DR backup directory and the most recent dump file,
plus a thin abstraction over the two dump formats this project produces:

- ``aistock_pg_<YYYYMMDD>.dump`` / ``..._permanent.dump`` — custom format
  written by ``scripts/dr_snapshot_prod_db.py`` (pg_dump --format=custom).
  Requires ``pg_restore`` to introspect.
- ``prod_schema_snapshot_<YYYYMMDD>.sql`` and similar — plain-SQL dumps
  (pg_dump default format). Can be inspected with text parsing.

All fixtures skip cleanly when the backup directory / dump file / pg
tooling is absent, so the ``dr_validate`` nox session is safe to run
on fresh CI hosts that have no local DR state.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pytest


DEFAULT_BACKUP_DIR = Path("E:/DEV backup/aistock_pg_snapshots")
# Canonical path is DEFAULT_BACKUP_DIR (matches dr_snapshot_prod_db.py
# DEFAULT_TARGET_DIR + dr_cleanup_old_snapshots.py DEFAULT_TARGET_DIR after
# the Codex Lane A r3 review; drawer a25cd473). LEGACY_BACKUP_DIR is kept
# as a fallback only for hosts that still have dumps in the old parent
# directory; new snapshots all land under aistock_pg_snapshots/.
LEGACY_BACKUP_DIR = Path("E:/DEV backup")

DUMP_SUFFIXES = (".dump", ".sql")
DUMP_NAME_RE = re.compile(
    r"^(?:aistock_pg|prod_schema_snapshot|prod_data_snapshot)_"
    r"(?P<date>\d{8})(?P<permanent>_permanent)?\.(?:dump|sql)$"
)


@dataclass(frozen=True)
class DumpInfo:
    path: Path
    is_plain_sql: bool
    is_custom: bool
    size_bytes: int


def _resolve_backup_dir() -> Path:
    """Return the configured DR backup directory.

    Resolution order:
      1. ``DR_BACKUP_DIR`` env override (absolute path).
      2. ``E:/DEV backup/aistock_pg_snapshots`` (canonical local location).
      3. ``E:/DEV backup`` (legacy / current layout).
    """
    env = os.environ.get("DR_BACKUP_DIR")
    if env:
        return Path(env)
    if DEFAULT_BACKUP_DIR.exists():
        return DEFAULT_BACKUP_DIR
    return LEGACY_BACKUP_DIR


@pytest.fixture(scope="session")
def dr_backup_dir() -> Path:
    """The DR backup directory. Skip when it does not exist."""
    p = _resolve_backup_dir()
    if not p.exists():
        pytest.skip(
            f"DR backup directory {p} does not exist on this host; set "
            f"DR_BACKUP_DIR or create the directory to enable DR tests."
        )
    return p


@pytest.fixture
def all_dump_files(dr_backup_dir: Path) -> list[Path]:
    """All dump files in the backup directory (recursive into immediate
    subdirs only). Sorted by mtime descending."""
    candidates: list[Path] = []
    for p in dr_backup_dir.iterdir():
        if p.is_file() and p.suffix.lower() in DUMP_SUFFIXES:
            candidates.append(p)
        elif p.is_dir():
            for q in p.iterdir():
                if q.is_file() and q.suffix.lower() in DUMP_SUFFIXES:
                    candidates.append(q)
    return sorted(candidates, key=lambda f: f.stat().st_mtime, reverse=True)


@pytest.fixture
def latest_dump(all_dump_files: list[Path]) -> DumpInfo:
    """Most recent dump file. Skip when none found."""
    if not all_dump_files:
        pytest.skip(
            "no dump file found in DR backup directory (.dump or .sql); "
            "run scripts/dr_snapshot_prod_db.py once to populate."
        )
    path = all_dump_files[0]
    is_plain_sql = path.suffix.lower() == ".sql"
    is_custom = path.suffix.lower() == ".dump"
    return DumpInfo(
        path=path,
        is_plain_sql=is_plain_sql,
        is_custom=is_custom,
        size_bytes=path.stat().st_size,
    )


# Per Codex Lane A r3 review (drawer a25cd473): the docker fallback must
# match exact container names rather than scanning for "any container with
# timescale/postgres in its image". Random hosts may have unrelated
# postgres containers running (e.g. a different project's test fixture),
# and pg_restore against the wrong cluster would silently produce garbage
# results.
#
# Resolution order:
#   1. DR_PG_CONTAINER env var (exact name; user opt-in).
#   2. Canonical AIstock container names below (exact match against
#      `docker ps --format '{{.Names}}'`).
# No image-substring matching.
CANONICAL_PG_CONTAINER_NAMES = ("aistock-pg", "aistock-pg-dev", "timescaledb")


def _docker_pg_container() -> str | None:
    """Return the name of the canonical AIstock postgres docker container,
    or None if docker is unreachable / no canonical container is running."""
    if shutil.which("docker") is None:
        return None
    explicit = os.environ.get("DR_PG_CONTAINER")
    try:
        proc = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=5,
        )
    except (subprocess.SubprocessError, OSError):
        return None
    if proc.returncode != 0:
        return None
    running = {line.strip() for line in proc.stdout.splitlines() if line.strip()}
    if explicit and explicit in running:
        return explicit
    for name in CANONICAL_PG_CONTAINER_NAMES:
        if name in running:
            return name
    return None


@pytest.fixture(scope="session")
def pg_restore_runner():
    """Return a callable ``(args, stdin_bytes) -> CompletedProcess`` that
    invokes ``pg_restore`` -- or ``None`` if neither PATH ``pg_restore`` nor
    a canonical docker container is available.

    Per Codex Lane A r3 review (drawer a25cd473): this fixture MUST NOT
    eagerly ``pytest.skip()`` when pg_restore is missing, because:

      - ``.sql`` plain-text dumps do not need pg_restore for validation;
        they parse with regex. The previous eager-skip caused legacy
        ``.sql`` tests to skip on every fresh host instead of running
        their actual text-based checks.
      - ``.dump`` custom-format dumps DO need pg_restore. The .dump-handling
        path inside each test checks ``runner is None`` and pytest.skip()s
        locally with an actionable reason.

    Resolution order (same as before, but None on failure):
      1. ``pg_restore`` on PATH.
      2. ``docker exec -i <canonical-container> pg_restore`` -- name MUST
         match either ``$DR_PG_CONTAINER`` or one of
         ``CANONICAL_PG_CONTAINER_NAMES``. Arbitrary timescale/postgres
         containers are no longer accepted.
    """
    direct = shutil.which("pg_restore") or shutil.which("pg_restore.exe")
    if direct:
        def _runner(args: list[str], stdin_bytes: bytes | None = None):
            return subprocess.run(
                [direct, *args],
                input=stdin_bytes,
                capture_output=True,
                timeout=60,
            )
        return _runner

    container = _docker_pg_container()
    if container is None:
        return None  # explicit None so .sql tests still run

    def _docker_runner(args: list[str], stdin_bytes: bytes | None = None):
        # Stream the dump bytes via stdin so the file does not need to
        # exist inside the container; pg_restore --list on stdin requires
        # the -i (interactive) docker exec flag.
        cmd = ["docker", "exec", "-i", container, "pg_restore", *args]
        return subprocess.run(
            cmd,
            input=stdin_bytes,
            capture_output=True,
            timeout=120,
        )

    return _docker_runner
