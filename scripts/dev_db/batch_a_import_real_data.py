"""Batch A — real prod data import to dev DB (T17).

Per docs/process/dev_db_test_data_plan_20260510.md §2 Batch A.

Safety:
- prod connection is opened with default_transaction_read_only=on so any accidental
  INSERT/UPDATE/DELETE/COPY-FROM raises immediately
- dev port is asserted to be 5433 and dev dbname must contain 'dev'
- Each table is TRUNCATEd before INSERT (idempotent re-run)
- session_replication_role='replica' on dev only, scoped to the import session,
  so FK ordering does not need to be perfect; constraint validation is
  re-armed at session end (transaction-bound)
- Failures on a single table ROLLBACK that table's transaction; subsequent tables continue

Scope:
  market.index_daily      5y, CSI300/CSI500/CSI1000/中证全指/上证综指
  public.aistock_model_catalog   full
  strategy_pkg.*          7 prod tables (4 dev-only Phase 1A tables skipped)
  paper_v2.*              all 21 tables, full history
  qe_archive baseline     samples per design §2: outbox_event/archive_job 30d, others 7d

Run: python scripts/dev_db/batch_a_import_real_data.py
"""
from __future__ import annotations

import io
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2 import errors as pg_errors
from psycopg2.extras import RealDictCursor

ENV_FILE = Path("F:/Dev/AIstock/.env")

# index_daily filter
INDEX_CODES = ("000300.SH", "000905.SH", "000852.SH", "000985.SH", "000001.SH")
INDEX_LOOKBACK_YEARS = 5

# strategy_pkg tables present in BOTH prod and dev (4 Phase 1A tables are dev-only and skipped)
STRATEGY_PKG_INTERSECTION = (
    "model_retrain_job", "model_state", "package", "package_asset",
    "package_status_event", "selection_score_artifact", "validated_execution_policy",
)
STRATEGY_PKG_DEV_ONLY_SKIP = (
    "package_runtime_variant", "package_validation_run",
    "promotion_review", "seed_fragility_score",
)

# paper_v2 tables in FK-friendly insert order (with replica role we don't strictly need this,
# but it makes failure modes clearer)
PAPER_V2_ORDER = (
    "portfolio", "runtime_profile", "runtime_profile_version",
    "runtime_config_activation", "execution_policy_activation",
    "config_change_audit", "reset_audit",
    "run", "trade_session", "session_day", "session_events", "run_events",
    "orders", "order_execution_state", "order_events",
    "fills", "positions", "daily_snapshots", "intraday_snapshots",
    "cash_ledger", "errors",
)

# qe_archive sampling spec: (table, time_column or None, days)
# None = full table (small lookups), days=N = WHERE time_col >= NOW() - N days
QE_ARCHIVE_SAMPLE = (
    ("outbox_event", "created_at", 30),
    ("archive_job", "created_at", 30),
    ("run", "created_at", 7),
    ("run_metric", "created_at", 7),
    ("run_factor", "created_at", 7),
    ("run_curve", "created_at", 7),
    ("run_trade", "created_at", 7),
    ("run_symbol_summary", "created_at", 7),
    ("run_data_context", "created_at", 7),
    ("run_source", None, None),  # tiny lookup
    ("schema_version", None, None),  # tiny
    ("metric_taxonomy", None, None),  # tiny
    ("raw_payload", "created_at", 7),
)


@dataclass
class TableResult:
    schema: str
    name: str
    status: str  # ok / skipped / failed
    rows_copied: int = 0
    elapsed_sec: float = 0.0
    note: str = ""


@dataclass
class ImportReport:
    started_at: float = field(default_factory=time.time)
    results: list[TableResult] = field(default_factory=list)

    def add(self, r: TableResult):
        self.results.append(r)

    def render(self) -> str:
        elapsed = time.time() - self.started_at
        lines = [f"Total elapsed: {elapsed:.1f}s\n"]
        ok = sum(1 for r in self.results if r.status == "ok")
        skipped = sum(1 for r in self.results if r.status == "skipped")
        failed = sum(1 for r in self.results if r.status == "failed")
        total_rows = sum(r.rows_copied for r in self.results if r.status == "ok")
        lines.append(f"Tables: ok={ok} skipped={skipped} failed={failed}, total_rows_copied={total_rows}\n")
        lines.append(f"{'schema':<14}{'table':<40}{'status':<10}{'rows':>10}  {'elapsed':>8}  note")
        lines.append("-" * 110)
        for r in self.results:
            lines.append(
                f"{r.schema:<14}{r.name:<40}{r.status:<10}{r.rows_copied:>10}  "
                f"{r.elapsed_sec:>7.2f}s  {r.note}"
            )
        return "\n".join(lines)


def parse_env() -> dict:
    cfg = {}
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip().strip('"').strip("'")
    return cfg


def assert_safe_targets(cfg: dict) -> tuple[dict, dict]:
    prod = {
        "host": cfg["TDX_DB_HOST"], "port": int(cfg["TDX_DB_PORT"]),
        "dbname": cfg["TDX_DB_NAME"], "user": cfg["TDX_DB_USER"],
        "password": cfg["TDX_DB_PASSWORD"],
    }
    dev = {
        "host": cfg["TDX_DB_DEV_HOST"], "port": int(cfg["TDX_DB_DEV_PORT"]),
        "dbname": cfg["TDX_DB_DEV_NAME"], "user": cfg["TDX_DB_DEV_USER"],
        "password": cfg["TDX_DB_DEV_PASSWORD"],
    }
    if prod["port"] != 5432:
        sys.exit(f"REFUSED: prod port expected 5432, got {prod['port']}")
    if dev["port"] != 5433:
        sys.exit(f"REFUSED: dev port expected 5433, got {dev['port']}")
    if "dev" not in dev["dbname"]:
        sys.exit(f"REFUSED: dev dbname must contain 'dev', got {dev['dbname']}")
    if "dev" in prod["dbname"]:
        sys.exit(f"REFUSED: prod dbname looks like dev: {prod['dbname']}")
    return prod, dev


def get_columns(cur, schema: str, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]


def column_intersection(prod_cur, dev_cur, schema: str, table: str) -> tuple[list[str], list[str]]:
    p = get_columns(prod_cur, schema, table)
    d = get_columns(dev_cur, schema, table)
    common = [c for c in p if c in d]
    only_prod = [c for c in p if c not in d]
    only_dev = [c for c in d if c not in p]
    return common, only_prod, only_dev


def quote_ident(s: str) -> str:
    if not s.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier: {s!r}")
    return f'"{s}"'


def copy_table(
    prod_cur, dev_cur, schema: str, table: str,
    where_sql: Optional[str] = None,
    expected_max_bytes: int = 200 * 1024 * 1024,
) -> TableResult:
    """COPY a single table from prod to dev, INTERSECT(prod, dev) columns only."""
    t0 = time.time()
    fq = f"{quote_ident(schema)}.{quote_ident(table)}"

    common, only_prod, only_dev = column_intersection(prod_cur, dev_cur, schema, table)
    if not common:
        return TableResult(schema, table, "skipped", note=f"no common columns")

    cols_sql = ", ".join(quote_ident(c) for c in common)

    if where_sql:
        copy_to_sql = f"COPY (SELECT {cols_sql} FROM {fq} WHERE {where_sql}) TO STDOUT WITH (FORMAT BINARY)"
    else:
        copy_to_sql = f"COPY {fq} ({cols_sql}) TO STDOUT WITH (FORMAT BINARY)"

    copy_from_sql = f"COPY {fq} ({cols_sql}) FROM STDIN WITH (FORMAT BINARY)"

    # truncate dev side first (idempotent re-run)
    dev_cur.execute(f"TRUNCATE TABLE {fq} CASCADE")

    buf = io.BytesIO()
    prod_cur.copy_expert(copy_to_sql, buf)
    size = buf.tell()
    if size > expected_max_bytes:
        # don't blow up dev; flag and skip
        return TableResult(
            schema, table, "skipped",
            note=f"buffer {size//1024//1024}MB exceeds soft cap {expected_max_bytes//1024//1024}MB",
        )
    buf.seek(0)
    dev_cur.copy_expert(copy_from_sql, buf)

    # actual rows count
    dev_cur.execute(f"SELECT COUNT(*) FROM {fq}")
    rows = dev_cur.fetchone()[0]

    note_parts = []
    if only_prod:
        note_parts.append(f"prod_only_cols={','.join(only_prod)}")
    if only_dev:
        note_parts.append(f"dev_only_cols={','.join(only_dev)}")
    return TableResult(
        schema, table, "ok", rows_copied=rows,
        elapsed_sec=time.time() - t0, note="; ".join(note_parts),
    )


def safe_copy(prod_conn, dev_conn, schema: str, table: str, where_sql: Optional[str] = None) -> TableResult:
    """Wrap copy_table with per-table savepoint so a failure doesn't abort the whole import."""
    sp = f"sp_{schema}_{table}".replace(".", "_")[:50]
    with prod_conn.cursor() as pc, dev_conn.cursor() as dc:
        dc.execute(f"SAVEPOINT {sp}")
        try:
            r = copy_table(pc, dc, schema, table, where_sql=where_sql)
            if r.status == "ok":
                dc.execute(f"RELEASE SAVEPOINT {sp}")
            else:
                dc.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            return r
        except Exception as e:
            dc.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            return TableResult(schema, table, "failed", note=f"{type(e).__name__}: {str(e)[:200]}")


def has_column(cur, schema: str, table: str, col: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s",
        (schema, table, col),
    )
    return cur.fetchone() is not None


def main():
    cfg = parse_env()
    prod_cfg, dev_cfg = assert_safe_targets(cfg)

    print(f"PROD (read-only): {prod_cfg['host']}:{prod_cfg['port']}/{prod_cfg['dbname']}")
    print(f"DEV  (writable) : {dev_cfg['host']}:{dev_cfg['port']}/{dev_cfg['dbname']}")
    print()

    prod_conn = psycopg2.connect(
        host=prod_cfg["host"], port=prod_cfg["port"], dbname=prod_cfg["dbname"],
        user=prod_cfg["user"], password=prod_cfg["password"],
        options="-c default_transaction_read_only=on",
    )
    prod_conn.autocommit = False
    dev_conn = psycopg2.connect(
        host=dev_cfg["host"], port=dev_cfg["port"], dbname=dev_cfg["dbname"],
        user=dev_cfg["user"], password=dev_cfg["password"],
    )
    dev_conn.autocommit = False

    # Sanity: confirm prod is read-only
    with prod_conn.cursor() as pc:
        pc.execute("SHOW transaction_read_only")
        ro = pc.fetchone()[0]
        print(f"prod transaction_read_only = {ro}")
        if ro != "on":
            sys.exit("FATAL: prod connection is NOT read-only, aborting before any writes")

    # Bypass FK + triggers on dev side for bulk import (still in single transaction)
    with dev_conn.cursor() as dc:
        dc.execute("SET session_replication_role = 'replica'")
        dc.execute("SHOW session_replication_role")
        print(f"dev  session_replication_role = {dc.fetchone()[0]}")
    print()

    report = ImportReport()

    # ---- 1. market.index_daily (5y, 5 indexes) ----
    print("[1/5] market.index_daily (5y filter)")
    with prod_conn.cursor() as pc:
        # dynamically detect filter column name in prod
        if has_column(pc, "market", "index_daily", "ts_code"):
            code_col = "ts_code"
        elif has_column(pc, "market", "index_daily", "index_code"):
            code_col = "index_code"
        elif has_column(pc, "market", "index_daily", "code"):
            code_col = "code"
        else:
            code_col = None
        if has_column(pc, "market", "index_daily", "trade_date"):
            date_col = "trade_date"
        else:
            date_col = None
    if code_col and date_col:
        codes_sql = ",".join(f"'{c}'" for c in INDEX_CODES)
        where = f"{date_col} >= (CURRENT_DATE - INTERVAL '{INDEX_LOOKBACK_YEARS} years') AND {code_col} IN ({codes_sql})"
        r = safe_copy(prod_conn, dev_conn, "market", "index_daily", where_sql=where)
    else:
        r = TableResult("market", "index_daily", "skipped", note=f"missing filter cols: code_col={code_col} date_col={date_col}")
    print(f"  -> {r.status} rows={r.rows_copied} {r.note}")
    report.add(r)

    # ---- 2. public.aistock_model_catalog ----
    print("[2/5] public.aistock_model_catalog (full)")
    r = safe_copy(prod_conn, dev_conn, "public", "aistock_model_catalog")
    print(f"  -> {r.status} rows={r.rows_copied} {r.note}")
    report.add(r)

    # ---- 3. strategy_pkg.* (7 intersection tables) ----
    print(f"[3/5] strategy_pkg.* — {len(STRATEGY_PKG_INTERSECTION)} intersection tables (skipping {len(STRATEGY_PKG_DEV_ONLY_SKIP)} dev-only)")
    for t in STRATEGY_PKG_INTERSECTION:
        r = safe_copy(prod_conn, dev_conn, "strategy_pkg", t)
        print(f"  {t}: {r.status} rows={r.rows_copied} {r.note}")
        report.add(r)
    for t in STRATEGY_PKG_DEV_ONLY_SKIP:
        r = TableResult("strategy_pkg", t, "skipped", note="dev-only Phase 1A table, no prod source")
        report.add(r)

    # ---- 4. paper_v2.* (21 tables, FK-ordered) ----
    print(f"[4/5] paper_v2.* — {len(PAPER_V2_ORDER)} tables, full history")
    for t in PAPER_V2_ORDER:
        r = safe_copy(prod_conn, dev_conn, "paper_v2", t)
        print(f"  {t}: {r.status} rows={r.rows_copied} {r.note}")
        report.add(r)

    # ---- 5. qe_archive baseline samples ----
    print(f"[5/5] qe_archive baseline samples ({len(QE_ARCHIVE_SAMPLE)} tables)")
    for tbl, time_col, days in QE_ARCHIVE_SAMPLE:
        with prod_conn.cursor() as pc:
            if time_col and not has_column(pc, "qe_archive", tbl, time_col):
                r = TableResult("qe_archive", tbl, "skipped", note=f"missing column {time_col}")
                report.add(r)
                print(f"  {tbl}: skipped ({r.note})")
                continue
        if time_col and days:
            where = f"{time_col} >= (NOW() - INTERVAL '{days} days')"
        else:
            where = None
        r = safe_copy(prod_conn, dev_conn, "qe_archive", tbl, where_sql=where)
        print(f"  {tbl}: {r.status} rows={r.rows_copied} {r.note}")
        report.add(r)

    # ---- finalize ----
    failed = [r for r in report.results if r.status == "failed"]
    if failed:
        print(f"\n{len(failed)} table(s) failed — committing successful tables, leaving failed ones rolled back to savepoint.")
    dev_conn.commit()
    prod_conn.rollback()  # explicitly close prod txn (was read-only anyway)
    print("\nCOMMIT done on dev. Prod connection was read-only throughout.")

    # ---- final validation queries ----
    print("\n=== Post-import validation (per design §5) ===")
    with dev_conn.cursor() as dc:
        for q in [
            ("paper_v2.run", "SELECT COUNT(*) FROM paper_v2.run"),
            ("strategy_pkg.package", "SELECT COUNT(*) FROM strategy_pkg.package"),
            ("market.index_daily", "SELECT COUNT(*) FROM market.index_daily"),
            ("public.aistock_model_catalog", "SELECT COUNT(*) FROM public.aistock_model_catalog"),
            ("qe_archive.outbox_event", "SELECT COUNT(*) FROM qe_archive.outbox_event"),
            ("qe_archive.archive_job", "SELECT COUNT(*) FROM qe_archive.archive_job"),
        ]:
            label, sql = q
            try:
                dc.execute(sql)
                n = dc.fetchone()[0]
                print(f"  {label}: {n}")
            except Exception as e:
                print(f"  {label}: ERROR {e}")

    print("\n" + "=" * 80)
    print(report.render())

    prod_conn.close()
    dev_conn.close()

    if failed:
        sys.exit(2)


if __name__ == "__main__":
    main()
