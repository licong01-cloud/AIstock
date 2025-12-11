import argparse
import sys
from datetime import datetime, date, timedelta
from typing import List, Tuple

from app_pg import get_conn  # type: ignore[attr-defined]

TDX_INDEX_DAILY_TABLE = "market.index_daily_tdx"
TRADING_CALENDAR_TABLE = "market.trading_calendar"


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def fetch_dates_tdx(index_code_tdx: str) -> List[date]:
    sql = f"""
        SELECT DISTINCT trade_date
        FROM {TDX_INDEX_DAILY_TABLE}
        WHERE index_code = %(index_code)s
        ORDER BY trade_date
    """
    with get_conn() as conn:  # type: ignore[attr-defined]
        with conn.cursor() as cur:
            cur.execute(sql, {"index_code": index_code_tdx})
            rows = cur.fetchall()
    return [r[0] for r in rows]


def fetch_trading_calendar(start: date, end: date) -> List[date]:
    """从交易日历表读取区间内的所有交易日.

    表结构以本地定义为准：
    - cal_date date (PK)
    - is_trading boolean

    仅返回 is_trading = TRUE 且 cal_date 在给定区间内的日期。
    """

    sql = f"""
        SELECT cal_date
          FROM {TRADING_CALENDAR_TABLE}
         WHERE is_trading = TRUE
           AND cal_date >= %(start)s
           AND cal_date <= %(end)s
         ORDER BY cal_date
    """

    params = {"start": start, "end": end}

    with get_conn() as conn:  # type: ignore[attr-defined]
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [r[0] for r in rows]


def summarize_dates(name: str, dates: List[date], start: date, end: date) -> None:
    print(f"\n=== {name} 覆盖情况 ===")
    if not dates:
        print("无任何记录")
        return

    print("总交易日数:", len(dates))
    print("最早日期:", dates[0])
    print("最晚日期:", dates[-1])

    # 统计在目标区间内的天数
    in_range = [d for d in dates if start <= d <= end]
    print(f"在区间 {start} ~ {end} 内的记录数:", len(in_range))
    if not in_range:
        return

    # 检查区间内是否有缺口（仅以连续自然日为基准，不考虑节假日）
    missing: List[date] = []
    day = start
    all_set = set(in_range)
    while day <= end:
        if day not in all_set:
            missing.append(day)
        day += timedelta(days=1)

    if missing:
        print("区间内自然日缺失数量:", len(missing))
        print("前 20 个缺失日期:", missing[:20])
    else:
        print("区间内自然日无缺失（注意：未区分是否为实际交易日）")


def main() -> None:
    parser = argparse.ArgumentParser(description="检查 TDX 指数原始表的时间覆盖情况，并对比交易日历 (market.trading_calendar)")
    parser.add_argument("--index-code-tdx", required=True, help="TDX 风格指数代码，如 sh000300")
    parser.add_argument("--start", required=True, help="目标开始日期 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="目标结束日期 YYYY-MM-DD")

    args = parser.parse_args()
    index_code_tdx = args.index_code_tdx.strip()
    start = parse_date(args.start)
    end = parse_date(args.end)

    if start > end:
        print("start 不能晚于 end")
        sys.exit(1)

    print(
        f"=== 指数数据覆盖检查 ===\n"
        f"TDX_INDEX_DAILY_TABLE = {TDX_INDEX_DAILY_TABLE}\n"
        f"TRADING_CALENDAR_TABLE = {TRADING_CALENDAR_TABLE}\n"
        f"index_code_tdx        = {index_code_tdx}\n"
        f"目标区间              = {start} ~ {end}\n"
        f"时间                  = {datetime.now().isoformat()}\n"
    )

    dates_tdx = fetch_dates_tdx(index_code_tdx)
    summarize_dates("TDX 原始表", dates_tdx, start, end)

    # 对比交易日历
    dates_cal = fetch_trading_calendar(start, end)
    print("\n=== 交易日历覆盖情况 ===")
    if not dates_cal:
        print("在指定区间和市场下，交易日历无记录，请检查 TRADING_CALENDAR_TABLE 配置。")
    else:
        print("交易日历交易日总数:", len(dates_cal))
        print("最早交易日:", dates_cal[0])
        print("最晚交易日:", dates_cal[-1])

        set_cal = set(dates_cal)
        set_tdx = {d for d in dates_tdx if start <= d <= end}
        missing_trading_days = sorted(list(set_cal - set_tdx))
        if missing_trading_days:
            print("\n⚠ 在交易日历中存在但 TDX 指数表缺失的交易日数量:", len(missing_trading_days))
            print("前 20 个缺失交易日:", missing_trading_days[:20])
        else:
            print("\nTDX 指数表在目标区间内覆盖了所有交易日历中的交易日。")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        print("脚本执行异常:", repr(e))
        sys.exit(1)
