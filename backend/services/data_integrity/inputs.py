"""Capture revision references and actual inputs in the SAME short snapshot."""
from contextlib import contextmanager
from datetime import date

from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import capture_read_snapshot
from .contracts import digest
from .repository import schema_installed


def partition_pins(conn, start: date, end: date, datasets=None):
    if not schema_installed(conn):
        return None  # legacy input remains explicitly not ledger-certified
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""SELECT dataset,trade_date,data_revision,validated_revision,
            validation_status,validation_id,scope_version,rule_version
            FROM market.dataset_date_refresh_audit WHERE data_source='canonical_db'
              AND trade_date BETWEEN %s AND %s
              AND (%s::text[] IS NULL OR dataset=ANY(%s::text[]))
            ORDER BY dataset,trade_date""", (start, end, datasets, datasets))
        return [{**dict(row), "trade_date": row["trade_date"].isoformat()} for row in cur.fetchall()]


@contextmanager
def capture_inputs(start: date, end: date, *, datasets=None):
    """Callers consume/copy frames inside, then compute only AFTER this closes."""
    if start > end:
        raise ValueError("invalid source capture interval")
    with capture_read_snapshot() as conn:
        pins = partition_pins(conn, start, end, datasets)
        # A token is not certification: unknown/missing validations stay unknown.
        receipt = {"schema_version": "aistock_input_capture_v1", "start": str(start), "end": str(end),
                   "partition_refs": pins, "ledger_available": pins is not None,
                   "snapshot_kind": "REPEATABLE_READ_READ_ONLY", "model_compute_in_transaction": False}
        receipt["input_revision_sha256"] = digest(receipt)
        yield receipt
