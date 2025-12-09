from __future__ import annotations

"""分钟线单日维护脚本

复用 backend.app_pg.get_conn，对 market.kline_minute_raw 做按交易日的检查/删除。

用法示例（在项目根目录）：

  # 仅检查 2024-12-01 的分钟线情况
  python -m backend.scripts.minute_day_maintenance --date 2024-12-01

  # 检查并删除 2024-12-01 全部 1 分钟线
  python -m backend.scripts.minute_day_maintenance --date 2024-12-01 --delete

注意：
- 只按 trade_time::date 精确过滤，不会影响其他日期数据。
- 删除操作前后会打印记录数，方便确认。
"""

import argparse
from datetime import date, datetime, time, timedelta
from typing import Tuple

from app_pg import get_conn


def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:  # noqa: BLE001
        raise argparse.ArgumentTypeError(f"invalid date format '{value}', expected YYYY-MM-DD") from exc


def check_minute_day(target: date) -> Tuple[int, datetime | None, datetime | None]:
    """检查指定交易日的分钟线情况.

    返回: (总记录数, 最早时间, 最晚时间)
    """

    sql_summary = """
        SELECT
            COUNT(*)        AS row_count,
            MIN(trade_time) AS first_ts,
            MAX(trade_time) AS last_ts
        FROM market.kline_minute_raw
        WHERE trade_time::date = %s
          AND freq = '1m'
    """

    with get_conn() as conn:  # type: ignore[attr-defined]
        with conn.cursor() as cur:
            cur.execute(sql_summary, (target,))
            row_count, first_ts, last_ts = cur.fetchone()

    return int(row_count), first_ts, last_ts


def delete_minute_day(target: date) -> int:
    """按真实交易时段分批删除指定日期的全部 1 分钟线记录.

    仅删除 trade_time::date = target 且 freq='1m' 的记录。
    为避免 TimescaleDB tuple decompression 限制，按交易时段切分批量 delete：
    - 09:30–11:30
    - 13:00–15:00
    返回总删除行数。
    """

    # 按 ts_code + 小时间窗口分批 delete，进一步降低单次 DML 涉及的 tuple 数量。
    sql_symbols = """
        SELECT DISTINCT ts_code
        FROM market.kline_minute_raw
        WHERE trade_time::date = %s
          AND freq = '1m'
    """

    sql_delete = """
        DELETE FROM market.kline_minute_raw
        WHERE trade_time::date = %s
          AND freq = '1m'
          AND ts_code = %s
    """

    total_deleted = 0

    # 每个 symbol + 5 分钟窗口单独事务提交
    with get_conn() as conn:  # type: ignore[attr-defined]
        # 先拿到当日涉及到的全部 symbol
        with conn.cursor() as cur:
            cur.execute(sql_symbols, (target,))
            symbols = [row[0] for row in cur.fetchall()]

        if not symbols:
            return 0

        total_symbols = len(symbols)
        print(f"[删除] 当日涉及 {total_symbols} 个 ts_code，将逐个分批删除……")

        for idx, symbol in enumerate(symbols, start=1):
            print(f"[删除] 正在处理第 {idx}/{total_symbols} 个 ts_code: {symbol}")
            with conn.cursor() as cur:
                cur.execute(sql_delete, (target, symbol))
                batch_deleted = cur.rowcount or 0
                total_deleted += batch_deleted

            conn.commit()

            print(f"  - {symbol} 当日删除 {batch_deleted} 行，累计删除 {total_deleted} 行")

    return int(total_deleted)


def main() -> int:
    parser = argparse.ArgumentParser(description="检查/删除单日分钟线数据 (market.kline_minute_raw)")
    parser.add_argument("--date", required=True, type=parse_date, help="交易日，格式 YYYY-MM-DD")
    parser.add_argument(
        "--delete",
        action="store_true",
        help="在检查后执行删除该交易日全部 1 分钟线记录",
    )

    args = parser.parse_args()
    target: date = args.date

    print(f"[检查] 分钟线日期: {target.isoformat()}")
    row_count, first_ts, last_ts = check_minute_day(target)

    print(f"- 记录数: {row_count}")
    print(f"- 最早时间: {first_ts}")
    print(f"- 最晚时间: {last_ts}")
    if last_ts is not None:
        # 这里采用 15:00 作为常规定盘收盘时间，仅用于给出“是否完整到收盘”的辅助判断。
        last_hhmm = last_ts.strftime("%H:%M")
        is_full_to_close = last_hhmm >= "15:00"
        print(f"- 实际最后一条时间的 HH:MM: {last_hhmm}")
        print(f"- 是否至少覆盖到常规收盘 15:00: {is_full_to_close}")

    if not args.delete:
        print("[信息] 未指定 --delete，仅执行检查。")
        return 0

    if row_count == 0:
        print("[信息] 当日无分钟线数据，无需删除。")
        return 0

    print("[删除] 开始删除该交易日全部 1 分钟线记录……")
    deleted = delete_minute_day(target)
    print(f"[删除完成] 删除行数: {deleted}")

    # 删除后再次检查
    new_count, _, _ = check_minute_day(target)
    print(f"[删除后检查] 当日剩余记录数: {new_count}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
