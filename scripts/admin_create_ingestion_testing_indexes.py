import os
import sys
from pathlib import Path

# Ensure project root on sys.path BEFORE importing next_app
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from next_app.backend.db.pg_pool import get_conn, init_db_pool


def _load_db_env() -> None:
    """Load TDX_DB_* variables from next_app/.env if present."""

    root = Path(__file__).resolve().parents[1]
    env_path = root / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in {'"', "'"}:
            val = val[1:-1]
        if key.startswith("TDX_DB_") and key not in os.environ:
            os.environ[key] = val


def main() -> None:
    _load_db_env()
    init_db_pool()

    sqls = [
        "CREATE INDEX IF NOT EXISTS idx_ingestion_logs_ts ON market.ingestion_logs (ts DESC)",
        "CREATE INDEX IF NOT EXISTS idx_ingestion_logs_job_ts ON market.ingestion_logs (job_id, ts DESC)",
        "CREATE INDEX IF NOT EXISTS idx_testing_runs_started_at ON market.testing_runs (started_at DESC)",
    ]

    with get_conn() as conn:
        with conn.cursor() as cur:
            for stmt in sqls:
                cur.execute(stmt)
    print("created/ensured indexes:")
    for s in sqls:
        print(" -", s)


if __name__ == "__main__":
    main()
