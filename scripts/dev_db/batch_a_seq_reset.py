"""Reset PostgreSQL sequences after Batch A real-data import (BUG-022).

Background
----------
``scripts/dev_db/batch_a_import_real_data.py`` (T17, on
``origin/claude/dw-foundation-20260510``) COPYs production rows into the dev
DB but does not run ``setval()`` on the IDENTITY/SERIAL sequences afterward.
The first nextval() call then collides with an existing primary-key value and
raises ``UniqueViolation``, which surfaced during Codex's Phase 3 governance
live smoke as ``HTTP 500 enable_paper`` (cross-tool drawer
dd17c102a3a16e087d453364).

This script is the **canonical seq-hygiene fix for BUG-022**. Run it after
``batch_a_import_real_data.py`` (or ``batch_c_synthetic_fixtures.py``):

    python scripts/dev_db/batch_a_seq_reset.py

It scans every ``schema.table`` listed in ``RESET_TARGETS`` for the table's
default IDENTITY/SERIAL column and runs::

    SELECT setval(
        pg_get_serial_sequence('schema.table', 'id_col'),
        GREATEST(COALESCE(MAX(id_col), 0), 1)
    ) FROM schema.table;

Idempotent: rerunning is safe. Returns exit 0 on success, exit 2 if any
target failed (sequences left at their previous value, no rollback hazard).

Safety
------
- Refuses to run unless dev DB host == 127.0.0.1 and port == 5433.
- Reads credentials from ``F:/Dev/AIstock/.env`` (same as the import script).
- Runs each setval in its own transaction; partial failures don't taint the
  successful resets.
- No production read or write paths.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ENV_FILE = Path("F:/Dev/AIstock/.env")

# (schema, table, id_column). id_column omitted -> psycopg2 introspection picks
# the table's primary key. Order does not matter for setval; we just iterate.
RESET_TARGETS: tuple[tuple[str, str, str | None], ...] = (
    ("strategy_pkg", "package_status_event", "event_id"),
    ("strategy_pkg", "package_validation_run", "id"),
    ("strategy_pkg", "package_runtime_variant", "id"),
    ("strategy_pkg", "promotion_review", "id"),
    ("strategy_pkg", "seed_fragility_score", "id"),
    ("strategy_pkg", "model_retrain_job", "id"),
    ("strategy_pkg", "model_state", "id"),
    ("strategy_pkg", "selection_score_artifact", "id"),
    ("strategy_pkg", "validated_execution_policy", "id"),
    ("strategy_pkg", "package_asset", "id"),
    ("paper_v2", "session_events", "event_id"),
    ("paper_v2", "run_events", "event_id"),
    ("paper_v2", "order_events", "event_id"),
    ("paper_v2", "cash_ledger", "cash_id"),
    ("paper_v2", "reset_audit", "audit_id"),
    ("qe_archive", "outbox_event", None),
    ("qe_archive", "archive_job", None),
)


@dataclass
class ResetResult:
    schema: str
    table: str
    id_column: str | None
    status: str  # ok / skipped / failed
    new_value: int | None = None
    note: str = ""


def parse_env(env_file: Path = ENV_FILE) -> dict[str, str]:
    if not env_file.exists():
        raise FileNotFoundError(f".env not found at {env_file}")
    cfg: dict[str, str] = {}
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if not line or line.strip().startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        cfg[key.strip()] = value.strip().strip('"').strip("'")
    return cfg


def assert_dev_target(host: str, port: int, dbname: str) -> None:
    if host not in {"127.0.0.1", "localhost"}:
        raise SystemExit(f"FATAL: refusing to run against host {host!r}; expected loopback.")
    if int(port) != 5433:
        raise SystemExit(f"FATAL: refusing to run against port {port}; expected dev DB on 5433.")
    if "dev" not in dbname.lower():
        raise SystemExit(f"FATAL: refusing to run against dbname {dbname!r}; must contain 'dev'.")


def detect_id_column(cur, schema: str, table: str) -> str | None:
    cur.execute(
        """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass
          AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum::int)
        LIMIT 1
        """,
        (f'"{schema}"."{table}"',),
    )
    row = cur.fetchone()
    return row[0] if row else None


def reset_sequence_for(
    conn,
    schema: str,
    table: str,
    explicit_id_column: str | None,
) -> ResetResult:
    """Run ``setval`` for one (schema, table). Each call uses its own transaction."""
    fq = f'"{schema}"."{table}"'
    try:
        with conn.cursor() as cur:
            id_col = explicit_id_column or detect_id_column(cur, schema, table)
            if not id_col:
                conn.rollback()
                return ResetResult(schema, table, None, "skipped", note="no primary key found")
            cur.execute(
                f"""
                SELECT pg_get_serial_sequence(%s, %s),
                       (SELECT GREATEST(COALESCE(MAX({id_col}), 0), 1) FROM {fq})
                """,
                (f"{schema}.{table}", id_col),
            )
            seq, max_id = cur.fetchone()
            if not seq:
                conn.rollback()
                return ResetResult(
                    schema, table, id_col, "skipped",
                    note=f"{id_col} has no associated sequence (probably manually-assigned id)",
                )
            cur.execute("SELECT setval(%s, %s, true)", (seq, int(max_id)))
            new_value = int(max_id)
            conn.commit()
            return ResetResult(schema, table, id_col, "ok", new_value=new_value, note=seq)
    except Exception as exc:  # noqa: BLE001 - want exception text in note
        conn.rollback()
        return ResetResult(schema, table, explicit_id_column, "failed", note=str(exc).splitlines()[0])


def render_results(results: Iterable[ResetResult]) -> str:
    lines = ["table".ljust(48) + "id".ljust(16) + "status".ljust(10) + "new_value  note"]
    for r in results:
        lines.append(
            f'{r.schema + "." + r.table:48s}'
            f'{(r.id_column or "-"):16s}'
            f'{r.status:10s}'
            f'{("" if r.new_value is None else str(r.new_value)):10s} '
            f'{r.note}'
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    import psycopg2  # noqa: PLC0415 - imported here so unit tests can stub via monkeypatch

    cfg = parse_env()
    host = cfg.get("TDX_DB_DEV_HOST") or cfg.get("TDX_DB_HOST") or "127.0.0.1"
    port = int(cfg.get("TDX_DB_DEV_PORT") or cfg.get("TDX_DB_PORT") or 5433)
    dbname = cfg.get("TDX_DB_DEV_NAME") or cfg.get("TDX_DB_NAME") or "aistock_dev"
    user = cfg.get("TDX_DB_DEV_USER") or cfg.get("TDX_DB_USER") or os.environ.get("USER") or "aistock"
    password = cfg.get("TDX_DB_DEV_PASSWORD") or cfg.get("TDX_DB_PASSWORD")

    assert_dev_target(host, port, dbname)

    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    conn.autocommit = False

    results: list[ResetResult] = []
    for schema, table, id_col in RESET_TARGETS:
        results.append(reset_sequence_for(conn, schema, table, id_col))

    conn.close()

    print(render_results(results))
    failed = [r for r in results if r.status == "failed"]
    if failed:
        print(f"\n{len(failed)} sequence reset(s) failed", file=sys.stderr)
        return 2
    print(f"\nAll {len(results)} sequence resets ok ({sum(1 for r in results if r.status == 'ok')} reset, {sum(1 for r in results if r.status == 'skipped')} skipped).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
