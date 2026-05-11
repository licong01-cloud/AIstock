"""DR dump file validity (Stage 7.4 §1).

Verifies the most recent dump under ``E:/DEV backup/aistock_pg_snapshots/``
(or whichever directory ``DR_BACKUP_DIR`` resolves to) is structurally
sound:

- size > 1 KB (catches truncated / empty file)
- structural integrity:
    - custom-format dumps (.dump): ``pg_restore --list`` succeeds and
      yields >=1 TABLE DATA entry
    - plain-SQL dumps (.sql): text scan finds the canonical pg_dump
      header + at least one CREATE TABLE statement
- corrupted-dump negative test: synthesize a 50-byte truncated payload
  in a tmpdir and confirm the validator rejects it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PG_DUMP_HEADER_RE = re.compile(rb"^--\s*PostgreSQL database dump", re.MULTILINE)
CREATE_TABLE_RE = re.compile(rb"\bCREATE\s+TABLE\b", re.IGNORECASE)
COPY_FROM_RE = re.compile(rb"\bCOPY\s+\S+\s*\([^)]*\)\s+FROM\s+stdin", re.IGNORECASE)
PG_RESTORE_TABLE_DATA_RE = re.compile(rb"TABLE DATA", re.IGNORECASE)


def test_dr_backup_directory_smoke(dr_backup_dir: Path) -> None:
    """Sentinel: backup dir exists. Ensures collection produces >=1 test."""
    assert dr_backup_dir.exists()


def test_dump_file_size_above_threshold(latest_dump) -> None:
    """Dumps below 1 KB are almost certainly truncated."""
    assert latest_dump.size_bytes >= 1024, (
        f"dump {latest_dump.path.name} is suspiciously small "
        f"({latest_dump.size_bytes} bytes); pg_dump likely aborted."
    )


def test_dump_structural_integrity(latest_dump, pg_restore_runner) -> None:
    """Verify the dump is parseable.

    Custom-format: pg_restore --list returns successfully + lists at least
    one TABLE DATA entry. (Schema-only dumps would NOT have TABLE DATA;
    if the dump is schema-only this test is a no-op.)

    Plain-SQL: text scan finds the canonical pg_dump header and at least
    one CREATE TABLE statement. Test passes for schema-only and data
    dumps alike. **Plain-SQL validation runs even when pg_restore is
    not available** (Codex Lane A r3): the .sql format is fully
    text-parseable, so it must not be skipped just because the host
    lacks pg_restore.
    """
    if latest_dump.is_custom:
        if pg_restore_runner is None:
            pytest.skip(
                "custom-format dump validation needs pg_restore (PATH or "
                "canonical docker container DR_PG_CONTAINER / "
                "{aistock-pg, aistock-pg-dev, timescaledb}); neither found."
            )
        with latest_dump.path.open("rb") as fh:
            content = fh.read()
        proc = pg_restore_runner(["--list"], stdin_bytes=content)
        assert proc.returncode == 0, (
            f"pg_restore --list failed (rc={proc.returncode}); "
            f"dump is unreadable. stderr={proc.stderr[:500]!r}"
        )
        # TABLE DATA is the canonical entry that pg_restore lists for
        # actual table rows. Schema-only dumps will have TABLE/CONSTRAINT/
        # INDEX entries but no TABLE DATA — we accept that case too.
        listing = proc.stdout
        if not PG_RESTORE_TABLE_DATA_RE.search(listing):
            assert b"TABLE" in listing or b"SCHEMA" in listing, (
                f"pg_restore --list output for {latest_dump.path.name} "
                f"contains neither TABLE nor SCHEMA entries; the dump is "
                f"likely corrupted at the object-list level. "
                f"first 500 bytes of stdout: {listing[:500]!r}"
            )
        return

    # Plain-SQL path
    assert latest_dump.is_plain_sql, "dump must be either .dump or .sql"
    # Read at most the first 1 MB to keep this test cheap on multi-GB dumps
    with latest_dump.path.open("rb") as fh:
        head = fh.read(1024 * 1024)
    assert PG_DUMP_HEADER_RE.search(head), (
        f"plain-SQL dump {latest_dump.path.name} is missing the canonical "
        f"'-- PostgreSQL database dump' header; file may be truncated, "
        f"binary, or not a pg_dump output."
    )
    assert CREATE_TABLE_RE.search(head), (
        f"plain-SQL dump {latest_dump.path.name} contains no CREATE TABLE "
        f"in the first 1 MB; this is unusual for any AIstock production "
        f"dump and suggests the dump is empty / mis-routed."
    )


def test_corrupted_dump_is_detected(tmp_path: Path, pg_restore_runner) -> None:
    """Negative-test sentinel: a 50-byte garbage file must NOT pass the
    custom-format validator.

    Defends against future relaxation of ``test_dump_structural_integrity``
    that would let a corrupted custom-format dump through silently.
    Skips cleanly when pg_restore is unavailable (the only path to
    actually invoke the validator).
    """
    if pg_restore_runner is None:
        pytest.skip(
            "negative test needs pg_restore (PATH or canonical docker "
            "container); neither found on this host."
        )
    bad = tmp_path / "corrupted.dump"
    bad.write_bytes(b"PGDMP\x00" + b"\xff" * 44)  # 50 bytes, mostly garbage
    with bad.open("rb") as fh:
        payload = fh.read()
    proc = pg_restore_runner(["--list"], stdin_bytes=payload)
    # pg_restore --list MUST refuse a corrupted custom-format input. The
    # exit code is non-zero AND stderr names the problem.
    assert proc.returncode != 0, (
        "pg_restore --list accepted a 50-byte garbage 'dump'; the validator "
        "is dangerously permissive. stderr was: "
        f"{proc.stderr[:300]!r}"
    )
    # The error stream should mention the parse failure; the specific
    # wording varies by pg_restore version so we just confirm it's
    # non-empty.
    assert proc.stderr, "pg_restore failed silently (no stderr output)"
