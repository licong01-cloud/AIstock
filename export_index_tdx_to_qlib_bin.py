import argparse
import os
import sys
from datetime import datetime, date
from typing import List, Tuple

import requests
from psycopg2.extras import execute_values

from app_pg import get_conn  # type: ignore[attr-defined]


# ===========================
# 配置区（可通过参数或环境变量覆盖）
# ===========================
# 后端 FastAPI 地址：用于调用 /api/v1/qlib/index/bin/export
BACKEND_BASE = os.environ.get("TDX_BACKEND_BASE", "http://127.0.0.1:8001").rstrip("/")

# TDX 指数原始表 & 标准 index_daily 表
TDX_INDEX_DAILY_TABLE = "market.index_daily_tdx"
INDEX_DAILY_TABLE = "market.index_daily"  # 与 DBReader.INDEX_DAILY_TABLE 一致


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


# ---------------------------------------------------------------------
# 第 1 步：从 TDX 原始表读取数据
# ---------------------------------------------------------------------


def load_tdx_index_daily(
    index_code_tdx: str,
    start: date,
    end: date,
) -> List[Tuple[date, float, float, float, float, float, float]]:
    """从 market.index_daily_tdx 读取指定指数在区间内的日线.

    返回列表：[(trade_date, open_yuan, high_yuan, low_yuan, close_yuan, vol_shares, amount_yuan), ...]
    单位已转换：
    - 价格/金额：厘 -> 元
    - 成交量：手 -> 股
    """

    sql = f"""
        SELECT
            trade_date,
            open_li,
            high_li,
            low_li,
            close_li,
            volume_hand,
            amount_li
        FROM {TDX_INDEX_DAILY_TABLE}
        WHERE index_code = %(index_code)s
          AND trade_date >= %(start)s
          AND trade_date <= %(end)s
        ORDER BY trade_date
    """
    params = {"index_code": index_code_tdx, "start": start, "end": end}

    rows: List[Tuple[date, int, int, int, int, int, int]] = []
    with get_conn() as conn:  # type: ignore[attr-defined]
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for r in cur.fetchall():
                rows.append(r)

    if not rows:
        print("WARNING: 在 TDX 原始表中没有找到任何记录，index_code=", index_code_tdx)
        return []

    result: List[Tuple[date, float, float, float, float, float, float]] = []
    for trade_date, open_li, high_li, low_li, close_li, volume_hand, amount_li in rows:
        # 单位转换
        open_yuan = open_li / 1000.0
        high_yuan = high_li / 1000.0
        low_yuan = low_li / 1000.0
        close_yuan = close_li / 1000.0
        vol_shares = volume_hand * 100.0
        amount_yuan = amount_li / 1000.0
        result.append(
            (
                trade_date,
                open_yuan,
                high_yuan,
                low_yuan,
                close_yuan,
                vol_shares,
                amount_yuan,
            )
        )

    print(
        f"从 {TDX_INDEX_DAILY_TABLE} 读取到 {len(result)} 条记录，"
        f"日期范围: {result[0][0]} -> {result[-1][0]}"
    )
    return result


# ---------------------------------------------------------------------
# 第 2 步：写入标准 index_daily 表（供现有 qlib 导出代码使用）
# ---------------------------------------------------------------------


def upsert_into_index_daily(
    ts_code: str,
    rows: List[Tuple[date, float, float, float, float, float, float]],
) -> None:
    """将 TDX 转换后的数据写入 market.index_daily.

    - ts_code: Tushare 风格代码，如 000300.SH
    - rows: [(trade_date, open_yuan, high_yuan, low_yuan, close_yuan, vol_shares, amount_yuan)]

    假设 index_daily 表结构包含：
    (trade_date, ts_code, open, high, low, close, vol, amount)
    并以 (ts_code, trade_date) 作为唯一约束/主键。
    """

    if not rows:
        print("无数据可写入 index_daily，跳过。")
        return

    values = [
        (
            r[0],  # trade_date
            ts_code,
            r[1],  # open
            r[2],  # high
            r[3],  # low
            r[4],  # close
            r[5],  # vol (shares)
            r[6],  # amount (yuan)
        )
        for r in rows
    ]

    insert_sql = f"""
        INSERT INTO {INDEX_DAILY_TABLE} (
            trade_date,
            ts_code,
            open,
            high,
            low,
            close,
            vol,
            amount
        ) VALUES %s
        ON CONFLICT (ts_code, trade_date) DO UPDATE SET
            open   = EXCLUDED.open,
            high   = EXCLUDED.high,
            low    = EXCLUDED.low,
            close  = EXCLUDED.close,
            vol    = EXCLUDED.vol,
            amount = EXCLUDED.amount
    """

    with get_conn() as conn:  # type: ignore[attr-defined]
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values, page_size=1000)
    print(f"已写入/更新 {len(values)} 行到 {INDEX_DAILY_TABLE}，ts_code={ts_code}。")


# ---------------------------------------------------------------------
# 第 3 步：调用现有后端导出接口生成 Qlib bin
# ---------------------------------------------------------------------


def backend_url(path: str) -> str:
    return BACKEND_BASE.rstrip("/") + path


def export_index_bin_via_backend(
    snapshot_id: str,
    ts_code: str,
    start: date,
    end: date,
) -> None:
    """调用后端 /api/v1/qlib/index/bin/export 接口，生成 Qlib bin.

    这里的 index_code 使用 ts_code（例如 000300.SH），
    后端会在内部转换成 Qlib instrument（如 SH000300），并写入指定 snapshot。
    """

    url = backend_url("/api/v1/qlib/index/bin/export")
    payload = {
        "snapshot_id": snapshot_id,
        "index_code": ts_code,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "run_health_check": True,
    }

    print("调用后端导出接口:")
    print("POST", url)
    print("Payload:", payload)

    resp = requests.post(url, json=payload, timeout=600)
    print("HTTP", resp.status_code, resp.reason)

    if not resp.ok:
        print("Body:", resp.text)
        raise SystemExit("导出接口调用失败")

    data = resp.json()
    print("导出返回:")
    print(data)

    if not data.get("dump_bin_ok", False):
        print("WARNING: dump_bin_ok = False，dump_bin 失败，建议检查 stdout/stderr")
    else:
        print("dump_bin_ok = True")

    if data.get("check_ok") is False:
        print("WARNING: check_data_health.py 报告存在问题，请查看 stdout/stderr 详细信息")


# ---------------------------------------------------------------------
# main
# ---------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "从 TDX 指数原始表 (market.index_daily_tdx) 读取数据，"
            "写入标准 index_daily 表，并调用现有后端导出接口生成 Qlib bin。"
        )
    )
    parser.add_argument(
        "--index-code-tdx",
        required=True,
        help="TDX 风格指数代码，例如 sh000300",
    )
    parser.add_argument(
        "--ts-code",
        required=True,
        help="Tushare 风格指数代码，例如 000300.SH (后端导出接口的 index_code 参数)",
    )
    parser.add_argument(
        "--snapshot-id",
        required=True,
        help=(
            "Qlib snapshot ID，例如 qlib_bin_20251209；"
            "后端会在 QLIB_BIN_ROOT_WIN/<snapshot-id> 下生成/更新 bin 文件"
        ),
    )
    parser.add_argument("--start", required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="结束日期 YYYY-MM-DD")

    args = parser.parse_args()

    index_code_tdx = args.index_code_tdx.strip()
    ts_code = args.ts_code.strip()
    snapshot_id = args.snapshot_id.strip()
    start = parse_date(args.start)
    end = parse_date(args.end)

    if start > end:
        print("start 日期不能晚于 end")
        sys.exit(1)

    print(
        f"=== 从 TDX 表导出指数到 Qlib bin ===\n"
        f"TDX_INDEX_DAILY_TABLE = {TDX_INDEX_DAILY_TABLE}\n"
        f"INDEX_DAILY_TABLE     = {INDEX_DAILY_TABLE}\n"
        f"BACKEND_BASE          = {BACKEND_BASE}\n"
        f"index_code_tdx        = {index_code_tdx}\n"
        f"ts_code               = {ts_code}\n"
        f"snapshot_id           = {snapshot_id}\n"
        f"date range            = {start} ~ {end}\n"
        f"time                  = {datetime.now().isoformat()}\n"
    )

    # 1. 从 TDX 原始表加载数据并做单位转换
    rows = load_tdx_index_daily(index_code_tdx, start, end)
    if not rows:
        print("没有可用的数据，直接退出，不调用导出接口。")
        return

    # 2. 写入标准 index_daily 表
    upsert_into_index_daily(ts_code, rows)

    # 3. 调用现有后端接口生成 Qlib bin
    export_index_bin_via_backend(snapshot_id, ts_code, start, end)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        print("脚本执行异常:", repr(e))
        sys.exit(1)
