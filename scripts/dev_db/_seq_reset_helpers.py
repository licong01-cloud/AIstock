"""Sequence reset + FK validation helpers for dev-DB import scripts (T22).

P1.1 (Codex REV-6): Batch A used psycopg2 BINARY COPY ... FROM STDIN to bulk-load
  paper_v2.* / strategy_pkg.* / market.index_daily / qe_archive.* rows.
  COPY does NOT advance OWNED sequences — so any subsequent INSERT relying on
  DEFAULT nextval() collides with imported PK values. This module exposes
  reset_owned_sequences() to walk all BIGSERIAL/SERIAL columns under target
  schemas and run setval(seq_name, GREATEST(MAX(col), 1)) on each.

P1.2 (Codex REV-6): Batch A wrapped INSERTs in session_replication_role='replica'
  to bypass FK trigger checks for the bulk load. Per Codex review feedback this
  must be followed by an integrity sweep:
    (a) ALTER TABLE ... VALIDATE CONSTRAINT for any FK currently NOT VALID
    (b) explicit referential-integrity check via FK column joined to parent PK
        — catches violations that the bypass would otherwise hide

Both helpers are dev-DB-only; they REFUSE to run against port 5432 or any dbname
that doesn't contain 'dev' (defense-in-depth assertion).
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Any, Iterable

# Schemas in scope for sequence reset + FK validation. Only paper_v2 /
# strategy_pkg / market are touched by Batch A's COPY path; qe_archive is
# loaded via worker handlers (not COPY) so its sequences advance naturally.
TARGET_SCHEMAS = ("paper_v2", "strategy_pkg", "market")


@dataclass
class SeqResetResult:
    schema: str
    table: str
    column: str
    sequence: str | None
    max_value: int | None
    new_setval: int | None
    note: str = ""


@dataclass
class FkValidationResult:
    schema: str
    table: str
    constraint: str
    status: str  # 'validated' | 'already_valid' | 'failed' | 'orphan_rows'
    orphan_count: int = 0
    note: str = ""


@dataclass
class SeqResetReport:
    results: list[SeqResetResult] = field(default_factory=list)

    def render(self) -> str:
        ok = sum(1 for r in self.results if r.new_setval is not None)
        skipped = sum(1 for r in self.results if r.new_setval is None and r.note)
        lines = [f"Sequence reset: {ok} updated, {skipped} skipped"]
        for r in self.results:
            if r.new_setval is not None:
                lines.append(f"  {r.schema}.{r.table}.{r.column} -> setval({r.sequence}, {r.new_setval})")
            else:
                lines.append(f"  {r.schema}.{r.table}.{r.column}: SKIPPED ({r.note})")
        return "\n".join(lines)


@dataclass
class FkValidationReport:
    results: list[FkValidationResult] = field(default_factory=list)

    def render(self) -> str:
        validated = sum(1 for r in self.results if r.status == "validated")
        already = sum(1 for r in self.results if r.status == "already_valid")
        failed = [r for r in self.results if r.status in ("failed", "orphan_rows")]
        lines = [f"FK validation: {validated} validated, {already} already valid, {len(failed)} failed"]
        for r in failed:
            lines.append(
                f"  FAIL {r.schema}.{r.table}.{r.constraint}: {r.status} {r.note}"
            )
        return "\n".join(lines)


def _assert_dev(conn: Any) -> None:
    """Hard refuse to run against prod. Identification is done by dbname only;
    inet_server_port() returns the PG server's internal listening port (often
    5432 inside containerized PG even when the host-side port mapping is 5433),
    so it is unreliable as a prod/dev signal. dbname must contain 'dev' AND
    must NOT match prod-known names."""
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        dbname = cur.fetchone()[0] or ""
    if "dev" not in dbname:
        sys.exit(f"REFUSED: helper called on dbname={dbname!r} (no 'dev' substring)")
    if dbname in ("aistock", "tdx_db", "production"):
        sys.exit(f"REFUSED: helper called on prod-looking dbname={dbname!r}")


def list_serial_columns(
    conn: Any, schemas: Iterable[str] = TARGET_SCHEMAS,
) -> list[tuple[str, str, str]]:
    """Return [(schema, table, column)] for every column whose default is a
    nextval() reference (covers SERIAL, BIGSERIAL, IDENTITY-via-sequence)."""
    out: list[tuple[str, str, str]] = []
    schema_list = tuple(schemas)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.nspname, c.relname, a.attname
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            WHERE n.nspname = ANY(%s)
              AND c.relkind = 'r'
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND pg_get_expr(ad.adbin, ad.adrelid) LIKE 'nextval(%%'
            ORDER BY n.nspname, c.relname, a.attname
            """,
            (list(schema_list),),
        )
        out = [(r[0], r[1], r[2]) for r in cur.fetchall()]
    return out


def reset_owned_sequences(
    conn: Any, schemas: Iterable[str] = TARGET_SCHEMAS,
) -> SeqResetReport:
    """For every owned sequence under target schemas, run
    setval(seq, GREATEST(MAX(col), 1)) so the next INSERT does not collide
    with COPY-loaded rows.

    Caller controls transaction lifecycle (commit/rollback); this helper
    issues SELECT setval() within the caller's open transaction.
    """
    _assert_dev(conn)
    report = SeqResetReport()
    cols = list_serial_columns(conn, schemas)
    with conn.cursor() as cur:
        for schema, table, column in cols:
            seq_lookup = f"{schema}.{table}"
            cur.execute(
                "SELECT pg_get_serial_sequence(%s, %s)",
                (seq_lookup, column),
            )
            seq_name = cur.fetchone()[0]
            if not seq_name:
                report.results.append(
                    SeqResetResult(schema, table, column, None, None, None,
                                   note="pg_get_serial_sequence returned NULL")
                )
                continue
            # SETVAL(seq, GREATEST(MAX(col), 1)) — using FALSE so next nextval
            # returns the value, but we use TRUE (default) so next nextval
            # returns max+1. Default behavior is what we want.
            try:
                cur.execute(
                    f'SELECT setval(%s, GREATEST(COALESCE(MAX("{column}"), 0), 1)) '
                    f'FROM "{schema}"."{table}"',
                    (seq_name,),
                )
                new_val = cur.fetchone()[0]
                # MAX value used (not new_val which is the setval return)
                cur.execute(f'SELECT MAX("{column}") FROM "{schema}"."{table}"')
                max_val = cur.fetchone()[0]
                report.results.append(SeqResetResult(
                    schema, table, column, seq_name, max_val, new_val,
                ))
            except Exception as e:
                report.results.append(SeqResetResult(
                    schema, table, column, seq_name, None, None,
                    note=f"{type(e).__name__}: {str(e)[:120]}",
                ))
    return report


def list_foreign_keys(
    conn: Any, schemas: Iterable[str] = TARGET_SCHEMAS,
) -> list[dict[str, Any]]:
    """Return all FK constraints under target schemas with their convalidated
    flag, parent/child columns. Used by the FK validation sweep."""
    schema_list = list(schemas)
    out: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                con.conname,
                n.nspname  AS schema,
                c.relname  AS table,
                con.convalidated,
                pg_get_constraintdef(con.oid, true) AS definition
            FROM pg_constraint con
            JOIN pg_class c     ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE con.contype = 'f' AND n.nspname = ANY(%s)
            ORDER BY n.nspname, c.relname, con.conname
            """,
            (schema_list,),
        )
        for r in cur.fetchall():
            out.append({
                "conname": r[0], "schema": r[1], "table": r[2],
                "convalidated": r[3], "definition": r[4],
            })
    return out


def validate_foreign_keys(
    conn: Any, schemas: Iterable[str] = TARGET_SCHEMAS,
) -> FkValidationReport:
    """Two-step FK integrity sweep:

    Step 1 — for any constraint marked NOT VALID, run ALTER TABLE ... VALIDATE.
    Step 2 — explicit referential check: for every FK, count rows whose FK
             column is non-NULL and the parent PK doesn't exist. Reports
             orphan_rows>0 as 'orphan_rows' status (REV-6 catches what the
             session_replication_role bypass might have hidden).

    Caller owns the transaction.
    """
    _assert_dev(conn)
    report = FkValidationReport()
    fks = list_foreign_keys(conn, schemas)

    with conn.cursor() as cur:
        for fk in fks:
            schema, table, conname = fk["schema"], fk["table"], fk["conname"]
            sp = f"sp_fkval_{conname}"[:60]
            cur.execute(f"SAVEPOINT {sp}")
            try:
                # Step 1 - VALIDATE if NOT VALID
                if not fk["convalidated"]:
                    cur.execute(
                        f'ALTER TABLE "{schema}"."{table}" VALIDATE CONSTRAINT "{conname}"'
                    )
                    status = "validated"
                else:
                    status = "already_valid"

                # Step 2 - explicit orphan check via JOIN
                # Parse FK definition: FOREIGN KEY (child_cols) REFERENCES parent_schema.parent_table (parent_cols)
                # We re-query pg_constraint for column lists since regex on definition is fragile.
                cur.execute(
                    """
                    SELECT
                        ARRAY(
                            SELECT a.attname
                            FROM unnest(con.conkey) WITH ORDINALITY ck(k, ord)
                            JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ck.k
                            ORDER BY ck.ord
                        ) AS child_cols,
                        n2.nspname  AS parent_schema,
                        c2.relname  AS parent_table,
                        ARRAY(
                            SELECT a2.attname
                            FROM unnest(con.confkey) WITH ORDINALITY fk(k, ord)
                            JOIN pg_attribute a2 ON a2.attrelid = con.confrelid AND a2.attnum = fk.k
                            ORDER BY fk.ord
                        ) AS parent_cols
                    FROM pg_constraint con
                    JOIN pg_class c2 ON c2.oid = con.confrelid
                    JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
                    WHERE con.conname = %s AND con.conrelid = %s::regclass
                    """,
                    (conname, f'"{schema}"."{table}"'),
                )
                row = cur.fetchone()
                if not row:
                    report.results.append(FkValidationResult(
                        schema, table, conname, status,
                        note="constraint metadata lookup returned no row",
                    ))
                    cur.execute(f"RELEASE SAVEPOINT {sp}")
                    continue
                child_cols, parent_schema, parent_table, parent_cols = row

                # Build orphan-count query: child rows where any child col is
                # NOT NULL AND no parent row matches the join.
                child_qual = " AND ".join(f'c."{cc}" IS NOT NULL' for cc in child_cols)
                join_qual = " AND ".join(
                    f'c."{cc}" = p."{pc}"' for cc, pc in zip(child_cols, parent_cols)
                )
                orphan_sql = (
                    f'SELECT COUNT(*) FROM "{schema}"."{table}" c '
                    f'WHERE {child_qual} AND NOT EXISTS ('
                    f'  SELECT 1 FROM "{parent_schema}"."{parent_table}" p '
                    f'  WHERE {join_qual})'
                )
                cur.execute(orphan_sql)
                orphan_count = cur.fetchone()[0]

                if orphan_count > 0:
                    status = "orphan_rows"
                report.results.append(FkValidationResult(
                    schema, table, conname, status,
                    orphan_count=orphan_count,
                    note=f"parent={parent_schema}.{parent_table} child_cols={list(child_cols)}"
                         if orphan_count > 0 else "",
                ))
                cur.execute(f"RELEASE SAVEPOINT {sp}")
            except Exception as e:
                cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                report.results.append(FkValidationResult(
                    schema, table, conname, "failed",
                    note=f"{type(e).__name__}: {str(e)[:200]}",
                ))
    return report
