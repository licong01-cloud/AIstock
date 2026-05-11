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


def _docker_pg_container() -> str | None:
    """Return the name of a local docker container running postgres/timescaledb,
    or None if docker is unreachable / no matching container."""
    if shutil.which("docker") is None:
        return None
    try:
        proc = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Image}}"],
            capture_output=True, text=True, timeout=5,
        )
    except (subprocess.SubprocessError, OSError):
        return None
    if proc.returncode != 0:
        return None
    for line in proc.stdout.splitlines():
        if "\t" not in line:
            continue
        name, image = line.split("\t", 1)
        if "timescale" in image.lower() or "postgres" in image.lower():
            return name.strip()
    return None


@pytest.fixture(scope="session")
def pg_restore_runner():
    """Return a callable ``(args: list[str], stdin_bytes: bytes | None) -> CompletedProcess``
    that invokes ``pg_restore`` with the given args. Resolution order:

      1. ``pg_restore`` on PATH.
      2. ``docker exec -i <postgres-container> pg_restore`` if a postgres /
         timescaledb container is running locally.

    Skips when neither is available.
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
        pytest.skip(
            "pg_restore not on PATH and no local docker postgres/timescaledb "
            "container is running; DR file-validity probe deferred."
        )

    def _docker_runner(args: list[str], stdin_bytes: bytes | None = None):
        # We stream the dump bytes via stdin so the file does not need to
        # exist inside the container. pg_restore --list on stdin requires
        # /dev/stdin via the -i (interactive) flag.
        cmd = ["docker", "exec", "-i", container, "pg_restore", *args]
        return subprocess.run(
            cmd,
            input=stdin_bytes,
            capture_output=True,
            timeout=120,
        )

    return _docker_runner
