import os
import psycopg2
import psycopg2.extras as pgx


def get_db_conn():
    pgx.register_uuid()
    conn = psycopg2.connect(
        host=os.getenv("TDX_DB_HOST", "localhost"),
        port=int(os.getenv("TDX_DB_PORT", "5432")),
        user=os.getenv("TDX_DB_USER", "postgres"),
        password=os.getenv("TDX_DB_PASSWORD", "lc78080808"),
        dbname=os.getenv("TDX_DB_NAME", "aistock"),
        application_name="AIstock-check-minute-gaps",
    )
    return conn


TARGET_DATES = ["2025-04-30", "2025-12-01"]


def main() -> int:
    """检查 kline_minute_raw 在 TARGET_DATES 上是否有数据，并输出每个交易日的行数。

    注意：kline_minute_raw 表中使用 trade_time（或等价时间戳）字段，
    这里通过 trade_time::date 派生交易日维度进行统计。
    """

    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=pgx.DictCursor) as cur:
            sql = """
            SELECT trade_time::date AS trade_date, COUNT(*) AS row_count
              FROM market.kline_minute_raw
             WHERE trade_time::date = ANY(%s::date[])
             GROUP BY trade_time::date
             ORDER BY trade_time::date
            """
            cur.execute(sql, (TARGET_DATES,))
            rows = cur.fetchall()

        found = {str(r["trade_date"]): int(r["row_count"]) for r in rows}

        print("=== minute gaps check for kline_minute_raw ===")
        for d in TARGET_DATES:
            if d in found:
                print(f"{d}: {found[d]} rows")
            else:
                print(f"{d}: NO ROWS FOUND")

        print("\n说明：")
        print("- 本脚本是直接查询 market.kline_minute_raw 表，按 trade_time::date 统计精确行数。")
        print("- 与前端数据看板的数据检查相比：")
        print("  · 数据看板通常基于预聚合或统计表，关注整段时间是否有缺口或异常；")
        print("  · 本脚本是针对具体日期的底层检查，可用于核对某几天是否真的存在分钟数据，以及大致条数是否正常。")

    finally:
        conn.close()

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
