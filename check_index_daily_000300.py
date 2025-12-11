import os
import sys
import textwrap
from datetime import datetime

from app_pg import get_conn


def main() -> None:
  print(
    textwrap.dedent(
      f"""
      === 检查 market.index_daily 指数日线概况 ===
      时间: {datetime.now().isoformat()}
      数据库: host={os.getenv('TDX_DB_HOST','localhost')} db={os.getenv('TDX_DB_NAME','aistock')}
      说明: 先统计整张表的总记录数，再统计每个 ts_code 的最小/最大 trade_date 及行数
      """
    )
  )

  with get_conn() as conn:  # type: ignore[attr-defined]
    with conn.cursor() as cur:
      # 1) 整张表的总记录数
      cur.execute("SELECT COUNT(*) FROM market.index_daily")
      total_rows = cur.fetchone()[0]
      print(f"market.index_daily 总记录数: {total_rows}\n")

      # 2) 表中所有 ts_code 的日期区间及行数
      sql = """
        SELECT
          ts_code,
          MIN(trade_date) AS min_date,
          MAX(trade_date) AS max_date,
          COUNT(*)        AS row_count
        FROM market.index_daily
        GROUP BY ts_code
        ORDER BY ts_code
      """
      cur.execute(sql)
      rows = cur.fetchall()

  if not rows:
    print("market.index_daily 表当前没有任何记录。")
    return

  print(f"共找到 {len(rows)} 个不同的 ts_code:\n")
  print("ts_code, min_date, max_date, row_count")
  print("------------------------------------")
  for ts_code, min_date, max_date, row_count in rows:
    print(f"{ts_code}, {min_date}, {max_date}, {row_count}")


if __name__ == "__main__":
  try:
    main()
  except Exception as e:  # noqa: BLE001
    print("脚本执行异常:", repr(e))
    sys.exit(1)
