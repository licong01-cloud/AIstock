import os
import psycopg2
from dotenv import load_dotenv


def _db_cfg():
    return dict(
        host=os.getenv("TDX_DB_HOST", "localhost"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", ""),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
    )


def main() -> None:
    load_dotenv(override=True)
    cfg = _db_cfg()
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    with conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM market.ingestion_jobs WHERE status='queued'")
        queued_jobs = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM market.ingestion_jobs WHERE status='running'")
        running_jobs = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM market.ingestion_job_tasks WHERE status NOT IN ('success','failed')")
        open_tasks = cur.fetchone()[0]
        print(f"Before cleanup: queued_jobs={queued_jobs}, running_jobs={running_jobs}, open_tasks={open_tasks}")

        cur.execute("DELETE FROM market.ingestion_jobs WHERE status='queued'")
        cur.execute("DELETE FROM market.ingestion_job_tasks WHERE status NOT IN ('success','failed')")

        cur.execute("SELECT count(*) FROM market.ingestion_jobs WHERE status='queued'")
        queued_jobs_after = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM market.ingestion_jobs WHERE status='running'")
        running_jobs_after = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM market.ingestion_job_tasks WHERE status NOT IN ('success','failed')")
        open_tasks_after = cur.fetchone()[0]
        print(
            f"After cleanup: queued_jobs={queued_jobs_after}, "
            f"running_jobs={running_jobs_after}, open_tasks={open_tasks_after}"
        )


if __name__ == "__main__":
    main()
