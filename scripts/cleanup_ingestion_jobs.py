from __future__ import annotations

import os
from typing import Iterable, Sequence

import psycopg2
import psycopg2.extras as pgx


pgx.register_uuid()

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    # 为兼容当前环境，若未显式设置 TDX_DB_PASSWORD，则使用与 ingest_full_minute.py 相同的默认值
    password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)


def _cleanup_jobs(statuses: Sequence[str]) -> None:
    """Delete ingestion jobs with given statuses and all related records.

    删除逻辑与 /api/ingestion/job/{job_id} 路由保持一致：
    - 通过 ingestion_runs.params->>'job_id' 反查 run_id；
    - 删除 checkpoints / errors / runs；
    - 删除 logs / tasks / jobs。
    """

    with psycopg2.connect(**DB_CFG) as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT job_id, status, summary
                  FROM market.ingestion_jobs
                 WHERE status = ANY(%s)
                ORDER BY created_at DESC
                """,
                (list(statuses),),
            )
            rows = cur.fetchall() or []
            print(f"[INFO] found {len(rows)} ingestion_jobs with status in {list(statuses)}")

            for row in rows:
                job_id = row["job_id"]
                status = row["status"]
                summary = row.get("summary")
                print("[INFO] deleting job", job_id, "status=", status, "summary=", summary)

                # 1) 反查所有关联的 run_id
                cur.execute(
                    """
                    SELECT run_id
                      FROM market.ingestion_runs
                     WHERE params->>'job_id' = %s
                    """,
                    (str(job_id),),
                )
                run_rows = cur.fetchall() or []
                run_ids = [r["run_id"] for r in run_rows]

                # 2) 删除 run 级别相关记录
                for rid in run_ids:
                    cur.execute(
                        "DELETE FROM market.ingestion_checkpoints WHERE run_id=%s",
                        (rid,),
                    )
                    cur.execute(
                        "DELETE FROM market.ingestion_errors WHERE run_id=%s",
                        (rid,),
                    )
                    cur.execute(
                        "DELETE FROM market.ingestion_runs WHERE run_id=%s",
                        (rid,),
                    )

                # 3) 删除与 job 直接关联的 logs / tasks / job 本身
                cur.execute(
                    "DELETE FROM market.ingestion_logs WHERE job_id=%s",
                    (job_id,),
                )
                cur.execute(
                    "DELETE FROM market.ingestion_job_tasks WHERE job_id=%s",
                    (job_id,),
                )
                cur.execute(
                    "DELETE FROM market.ingestion_jobs WHERE job_id=%s",
                    (job_id,),
                )

                print(
                    f"[OK] deleted job {job_id} and {len(run_ids)} linked runs"
                )


def cleanup_stuck_ingestion_jobs(
    statuses: Iterable[str] = ("running", "queued", "pending"),
) -> None:
    """Entry point: clean up stuck ingestion_jobs with given statuses.

    默认仅清理运行中 / 排队 / 待处理的任务，避免误删历史成功/失败记录。
    """

    unique_statuses = list(dict.fromkeys([str(s).lower() for s in statuses]))
    if not unique_statuses:
        print("[WARN] no statuses provided, nothing to do")
        return
    _cleanup_jobs(unique_statuses)


if __name__ == "__main__":
    # 默认清理所有 running/queued/pending 的任务
    cleanup_stuck_ingestion_jobs()
