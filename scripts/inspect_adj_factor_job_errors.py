import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv(override=True)

JOB_ID = "bc8091d0-0544-4570-9776-2413a84eae54"  # 当前 adj_factor init 任务的 job_id

host = os.getenv("TDX_DB_HOST", "127.0.0.1")
port = int(os.getenv("TDX_DB_PORT", "5432"))
user = os.getenv("TDX_DB_USER", "postgres")
password = os.getenv("TDX_DB_PASSWORD", "")
dbname = os.getenv("TDX_DB_NAME", "aistock")

conn = psycopg2.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    dbname=dbname,
    cursor_factory=RealDictCursor,
)
conn.autocommit = True

with conn, conn.cursor() as cur:
    # 查 job 日志里的 WARN/ERROR 记录，看看哪一天失败以及具体原因
    cur.execute(
        """
        SELECT ts, level, message
          FROM market.ingestion_logs
         WHERE job_id = %s::uuid
           AND level IN ('ERROR', 'WARN')
         ORDER BY ts
        """,
        (JOB_ID,),
    )
    rows = cur.fetchall()

print("Found", len(rows), "log rows (WARN/ERROR) for job", JOB_ID)
for r in rows[:50]:  # 最多打印前 50 条，避免刷屏
    print(r["ts"], r["level"], "-", r["message"])

conn.close()
