import os
import sys
import json
from pathlib import Path
import argparse

# Ensure project root is on sys.path so `import next_app` works even when this
# script is executed via a relative path like `python next_app/scripts/...`.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.pg_pool import get_conn, init_db_pool


def _load_db_env() -> None:
    """Load TDX_DB_* variables from next_app/.env if present.

    This mimics how the app configures DB access, so the script can
    reuse the same credentials.
    """

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
        # Strip surrounding single/double quotes to handle entries like
        # TDX_DB_PORT="5432" gracefully.
        if len(val) >= 2 and val[0] == val[-1] and val[0] in {'"', "'"}:
            val = val[1:-1]
        if key.startswith("TDX_DB_") and key not in os.environ:
            os.environ[key] = val


def _list_queued_stock_moneyflow_jobs() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT job_id,
                       status,
                       summary->>'dataset'    AS dataset,
                       summary->>'mode'       AS mode,
                       summary->>'start_date' AS start_date,
                       summary->>'end_date'   AS end_date,
                       created_at
                  FROM market.ingestion_jobs
                 WHERE status = 'queued'
                   AND summary->>'dataset' = 'stock_moneyflow'
                 ORDER BY created_at
                """
            )
            rows = cur.fetchall()
    out: list[dict] = []
    for r in rows:
        out.append(
            {
                "job_id": str(r[0]),
                "status": r[1],
                "dataset": r[2],
                "mode": r[3],
                "start_date": r[4],
                "end_date": r[5],
                "created_at": str(r[6]),
            }
        )
    return out


def _mark_job_failed(job_id: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE market.ingestion_jobs SET status='failed' WHERE job_id=%s",
                (job_id,),
            )
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fix-job-id", help="Job ID to mark as failed before listing")
    args = parser.parse_args()

    _load_db_env()
    init_db_pool()

    if args.fix_job_id:
        _mark_job_failed(args.fix_job_id)

    jobs = _list_queued_stock_moneyflow_jobs()
    print(json.dumps(jobs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
