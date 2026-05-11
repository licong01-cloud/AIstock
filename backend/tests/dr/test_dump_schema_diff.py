"""DR dump schema vs dev DB schema (Stage 7.4 §2).

Each test extracts the set of ``schema.table`` identifiers from the
latest dump and compares against ``information_schema.tables`` on the
dev DB. The invariants are:

- The dump must declare at least one schema-qualified table (sanity).
- Every table the dump declares must currently exist on dev DB
  (dev mirrors dump-time prod plus net-new tables; **missing** tables
  on dev would mean a regressive drop).
- dev DB MAY have extra tables vs the dump (Phase 3 / T12 additions
  applied after the dump was taken). This is expected and must NOT
  fail.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from psycopg2.extras import RealDictCursor


# Match ``CREATE TABLE [IF NOT EXISTS] [ONLY] schema.table`` on a single line.
# The table identifier may be quoted with double quotes.
CREATE_TABLE_RE = re.compile(
    rb"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:ONLY\s+)?"
    rb'(?P<schema>"?[A-Za-z_][A-Za-z0-9_]*"?)\.'
    rb'(?P<table>"?[A-Za-z_][A-Za-z0-9_]*"?)',
    re.IGNORECASE,
)
# pg_restore --list entries for tables look like
#   "1234; 5678 TABLE public foo aistock"
PG_RESTORE_TABLE_LINE_RE = re.compile(
    rb"^\d+;\s*\d+\s+TABLE\s+(?P<schema>\S+)\s+(?P<table>\S+)\s+",
    re.MULTILINE,
)
# Schemas we consider "user-owned" and worth diffing. Excludes pg_catalog,
# information_schema, etc.
USER_SCHEMAS = (
    "public", "qe_archive", "paper_v2", "strategy_pkg", "market",
    "model_registry", "trading_core", "selection_center",
)


def _strip_quotes(ident: bytes) -> str:
    s = ident.decode("utf-8", errors="replace")
    return s.strip('"')


def _read_first_mb(path: Path, size_mb: int = 8) -> bytes:
    """Read first ``size_mb`` MB of the dump; ample for the schema header
    portion of any production-size dump."""
    with path.open("rb") as fh:
        return fh.read(size_mb * 1024 * 1024)


def _extract_tables_from_dump(latest_dump, pg_restore_runner) -> set[tuple[str, str]]:
    if latest_dump.is_custom:
        with latest_dump.path.open("rb") as fh:
            content = fh.read()
        proc = pg_restore_runner(["--list"], stdin_bytes=content)
        if proc.returncode != 0:
            pytest.skip(
                f"pg_restore --list failed on custom dump "
                f"{latest_dump.path.name}; cannot diff schema. "
                f"stderr={proc.stderr[:300]!r}"
            )
        tables: set[tuple[str, str]] = set()
        for m in PG_RESTORE_TABLE_LINE_RE.finditer(proc.stdout):
            tables.add((_strip_quotes(m["schema"]), _strip_quotes(m["table"])))
        return tables

    head = _read_first_mb(latest_dump.path)
    tables = set()
    for m in CREATE_TABLE_RE.finditer(head):
        schema = _strip_quotes(m["schema"])
        table = _strip_quotes(m["table"])
        tables.add((schema, table))
    return tables


def test_dump_declares_at_least_one_user_table(latest_dump, pg_restore_runner) -> None:
    """Sanity: the dump's object catalog has >=1 table in a user schema."""
    tables = _extract_tables_from_dump(latest_dump, pg_restore_runner)
    user_tables = [t for t in tables if t[0] in USER_SCHEMAS]
    assert user_tables, (
        f"dump {latest_dump.path.name} declares no tables in user schemas "
        f"{USER_SCHEMAS}; all declared tables were: {sorted(tables)[:10]}"
    )


def test_dev_db_contains_every_dump_table(
    latest_dump, pg_restore_runner, dev_conn,
) -> None:
    """For every (schema, table) the dump declares in a user schema, the
    dev DB must currently have it. Missing-on-dev means a regressive drop.
    """
    dump_tables = {
        t for t in _extract_tables_from_dump(latest_dump, pg_restore_runner)
        if t[0] in USER_SCHEMAS
    }
    if not dump_tables:
        pytest.skip(
            f"dump {latest_dump.path.name} declared no user-schema tables; "
            f"per-table existence diff has nothing to check."
        )
    with dev_conn.cursor() as cur:
        cur.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema = ANY(%s)",
            (list(USER_SCHEMAS),),
        )
        dev_tables = {(r[0], r[1]) for r in cur.fetchall()}
    missing_on_dev = sorted(dump_tables - dev_tables)
    assert not missing_on_dev, (
        f"{len(missing_on_dev)} table(s) declared in dump "
        f"{latest_dump.path.name} are missing on dev DB; first 10: "
        f"{missing_on_dev[:10]}. Either (a) dev DB had a regressive drop, "
        f"or (b) the dump was taken from a different deployment."
    )


def test_dev_db_extra_tables_are_allowed(
    latest_dump, pg_restore_runner, dev_conn,
) -> None:
    """dev DB MAY have tables that the dump does NOT (Phase 3 / T12 additions
    landed after the dump was taken). This test asserts the *direction* of
    the diff is exactly one-way: dev superset-of dump.
    """
    dump_tables = {
        t for t in _extract_tables_from_dump(latest_dump, pg_restore_runner)
        if t[0] in USER_SCHEMAS
    }
    if not dump_tables:
        pytest.skip("dump declared no user-schema tables; nothing to diff.")
    with dev_conn.cursor() as cur:
        cur.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema = ANY(%s)",
            (list(USER_SCHEMAS),),
        )
        dev_tables = {(r[0], r[1]) for r in cur.fetchall()}
    extra_on_dev = dev_tables - dump_tables
    # This is informational only — never a fail. We report it so the
    # reviewer can spot-check that "extra" is in expected Phase 3 territory.
    if extra_on_dev:
        sample = sorted(extra_on_dev)[:10]
        print(
            f"\n[INFO] dev DB has {len(extra_on_dev)} table(s) not in dump "
            f"{latest_dump.path.name}; first 10: {sample}. "
            f"This is the expected forward direction (Phase 3 / T12 / etc.)."
        )
    # The real assertion: the direction is one-way only (dump ⊆ dev).
    # That's already covered by ``test_dev_db_contains_every_dump_table``;
    # this test pairs with it to surface the asymmetric tolerance
    # explicitly so future readers see the intentional design.
    assert dump_tables.issubset(dev_tables) or len(dump_tables) == 0, (
        "dump tables are not a subset of dev tables; "
        "see test_dev_db_contains_every_dump_table for the diff."
    )


# Reuse the dev DB fixture from the data_quality tree without re-implementing
# it. pytest will auto-discover ``conftest.py`` in sibling directories only
# if they share an ancestor, so we import the helper functions directly.
@pytest.fixture
def dev_conn():
    """Per-test dev DB connection. Skip when creds / DB unreachable.

    Re-implemented here (small duplication) rather than importing from
    backend/tests/data_quality/conftest.py because pytest fixture discovery
    is scoped per-directory; a parent conftest would be cleaner but
    would change other tests' resolution semantics.
    """
    import psycopg2
    from .conftest import _resolve_backup_dir  # noqa: F401 - shared sentinel
    from backend.tests.data_quality.conftest import _dev_db_creds  # type: ignore

    creds = _dev_db_creds()
    if creds is None:
        pytest.skip(
            "dev DB credentials missing or unsafe; DR schema-diff test "
            "skipped. Set TDX_DB_DEV_* env to enable."
        )
    try:
        conn = psycopg2.connect(connect_timeout=3, **creds)
    except Exception as exc:  # noqa: BLE001 - skip with reason on any failure
        pytest.skip(f"dev DB unreachable: {exc}")
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass
