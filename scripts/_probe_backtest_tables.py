"""扫描启用因子里的'稳定噪声'候选 — Rule B 漏掉但实际无 alpha 的因子."""
import os
from dotenv import load_dotenv
load_dotenv(r"F:\Dev\AIstock\.env")
import psycopg2

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ.get("TDX_DB_PORT", "5432")),
    user=os.environ["TDX_DB_USER"], password=os.environ["TDX_DB_PASSWORD"],
    dbname=os.environ["TDX_DB_NAME"],
)

# 先看有哪些存 backtest 指标的表
with conn.cursor() as cur:
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND (
            table_name LIKE '%metric%' OR table_name LIKE '%backtest%'
            OR table_name LIKE '%excess%' OR table_name LIKE '%perf%')
        ORDER BY table_name
    """)
    print("候选表:")
    for (t,) in cur.fetchall():
        print(f"  {t}")

    # 查 qe_factor_classification 所有 backtest 相关字段
    cur.execute("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name='qe_factor_classification'
        ORDER BY ordinal_position
    """)
    print("\nqe_factor_classification columns:")
    for c, t in cur.fetchall():
        print(f"  {c}: {t}")

conn.close()
