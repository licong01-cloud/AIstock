import argparse
import os
import sys
from datetime import datetime, date
from typing import List, Tuple

import requests
from psycopg2 import sql as pg_sql
from psycopg2.extras import execute_values

from app_pg import get_conn


# 默认 tdx-api 地址，可通过环境变量 TDX_API_BASE 覆盖
TDX_API_BASE = os.environ.get("TDX_API_BASE", "http://localhost:19080").rstrip("/")


CREATE_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.index_daily_tdx (
    trade_date      date        NOT NULL, -- 交易日期，对应 Time 的日期部分（本地时区）
    index_code      text        NOT NULL, -- 指数代码，如 sh000300（TDX 风格），由请求参数填入
    open_li         bigint      NOT NULL, -- 开盘价，单位：厘（1 元 = 1000 厘），对应 Open
    high_li         bigint      NOT NULL, -- 最高价，单位：厘，对应 High
    low_li          bigint      NOT NULL, -- 最低价，单位：厘，对应 Low
    close_li        bigint      NOT NULL, -- 收盘价，单位：厘，对应 Close
    last_close_li   bigint      NOT NULL, -- 昨收价，单位：厘，对应 Last
    volume_hand     bigint      NOT NULL, -- 成交量，单位：手（1 手 = 100 股），对应 Volume
    amount_li       bigint      NOT NULL, -- 成交额，单位：厘，对应 Amount
    source          text        NOT NULL DEFAULT 'tdx', -- 数据来源标识
    created_at      timestamptz NOT NULL DEFAULT now(), -- 入库时间
    PRIMARY KEY (index_code, trade_date)
);

COMMENT ON TABLE  market.index_daily_tdx IS 'TDX 指数日线原始数据（未复权，价格/金额单位：厘，成交量单位：手）';

COMMENT ON COLUMN market.index_daily_tdx.trade_date    IS '交易日期，对应 TDX 返回 Time 字段的本地日期';
COMMENT ON COLUMN market.index_daily_tdx.index_code    IS '指数代码（TDX 风格，例如 sh000300、sz399001），由请求参数填入';
COMMENT ON COLUMN market.index_daily_tdx.open_li       IS '开盘价，单位：厘（1 元 = 1000 厘），对应 Open';
COMMENT ON COLUMN market.index_daily_tdx.high_li       IS '最高价，单位：厘，对应 High';
COMMENT ON COLUMN market.index_daily_tdx.low_li        IS '最低价，单位：厘，对应 Low';
COMMENT ON COLUMN market.index_daily_tdx.close_li      IS '收盘价，单位：厘，对应 Close';
COMMENT ON COLUMN market.index_daily_tdx.last_close_li IS '昨收价，单位：厘，对应 Last';
COMMENT ON COLUMN market.index_daily_tdx.volume_hand   IS '成交量，单位：手（1 手 = 100 股），对应 Volume';
COMMENT ON COLUMN market.index_daily_tdx.amount_li     IS '成交额，单位：厘，对应 Amount';
COMMENT ON COLUMN market.index_daily_tdx.source        IS '数据来源标识，例如 tdx、ths 等';
COMMENT ON COLUMN market.index_daily_tdx.created_at    IS '入库时间戳';

CREATE INDEX IF NOT EXISTS idx_index_daily_tdx_trade_date
    ON market.index_daily_tdx (trade_date);
"""


def ensure_table_and_hypertable() -> None:
    """创建表并尝试转为 hypertable（若安装了 TimescaleDB）。"""
    with get_conn() as conn:  # type: ignore[attr-defined]
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)

            # 尝试启用 timescaledb 扩展并创建 hypertable（若已存在则跳过）
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            except Exception as exc:  # noqa: BLE001
                print("警告: 创建 timescaledb 扩展失败，可能未安装 TimescaleDB:", repr(exc))

            try:
                cur.execute(
                    "SELECT create_hypertable(\n"
                    "  'market.index_daily_tdx',\n"
                    "  'trade_date',\n"
                    "  'index_code',\n"
                    "  2,\n"
                    "  if_not_exists => TRUE\n"
                    ")"
                )
                print("已将 market.index_daily_tdx 注册为 Timescale hypertable（如支持）。")
            except Exception as exc:  # noqa: BLE001
                # 若数据库不是 TimescaleDB，忽略即可
                print("提示: create_hypertable 调用失败，可能未使用 TimescaleDB:", repr(exc))


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def fetch_index_kline_all(index_code: str) -> List[dict]:
    """从 tdx-api 获取指定指数的全量日线 K 线。index_code 形如 sh000300。"""
    url = f"{TDX_API_BASE}/api/index/all"
    params = {"code": index_code, "type": "day"}
    print("请求 tdx-api:", url, "params=", params)
    resp = requests.get(url, params=params, timeout=120)
    print("HTTP", resp.status_code, resp.reason)
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"tdx-api 返回错误: {data}")
    payload = data.get("data") or {}
    items = payload.get("list") or payload.get("List") or []
    print(f"tdx-api 返回 K 线条数: {len(items)}")
    return items


def filter_and_build_rows(
    items: List[dict],
    index_code: str,
    start: date,
    end: date,
) -> List[Tuple]:
    """按日期过滤 K 线并构造待插入的行元组列表。"""
    rows: List[Tuple] = []
    for k in items:
        t = k.get("Time")
        if not isinstance(t, str):
            continue
        try:
            dt = datetime.fromisoformat(t.replace("Z", "+00:00")).date()
        except Exception:  # noqa: BLE001
            continue
        if not (start <= dt <= end):
            continue

        open_li = int(k.get("Open", 0))
        high_li = int(k.get("High", 0))
        low_li = int(k.get("Low", 0))
        close_li = int(k.get("Close", 0))
        last_close_li = int(k.get("Last", 0))
        volume_hand = int(k.get("Volume", 0))
        amount_li = int(k.get("Amount", 0))

        rows.append(
            (
                dt,
                index_code,
                open_li,
                high_li,
                low_li,
                close_li,
                last_close_li,
                volume_hand,
                amount_li,
                "tdx",
            )
        )

    print(f"过滤后落在区间 {start} ~ {end} 的记录数: {len(rows)}")
    return rows


def upsert_rows(rows: List[Tuple]) -> None:
    if not rows:
        print("无可写入记录，跳过插入。")
        return

    insert_sql = pg_sql.SQL(
        """
        INSERT INTO market.index_daily_tdx (
            trade_date,
            index_code,
            open_li,
            high_li,
            low_li,
            close_li,
            last_close_li,
            volume_hand,
            amount_li,
            source
        ) VALUES %s
        ON CONFLICT (index_code, trade_date) DO UPDATE SET
            open_li       = EXCLUDED.open_li,
            high_li       = EXCLUDED.high_li,
            low_li        = EXCLUDED.low_li,
            close_li      = EXCLUDED.close_li,
            last_close_li = EXCLUDED.last_close_li,
            volume_hand   = EXCLUDED.volume_hand,
            amount_li     = EXCLUDED.amount_li,
            source        = EXCLUDED.source
        """
    )

    with get_conn() as conn:  # type: ignore[attr-defined]
        with conn.cursor() as cur:
            execute_values(cur, insert_sql.as_string(conn), rows, page_size=1000)
    print(f"已写入/更新 {len(rows)} 行到 market.index_daily_tdx。")


def main() -> None:
    parser = argparse.ArgumentParser(description="初始化 TDX 指数日线表并导入数据")
    parser.add_argument("--code", required=True, help="指数代码（TDX 风格，如 sh000300）")
    parser.add_argument("--start", required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="结束日期 YYYY-MM-DD")

    args = parser.parse_args()
    index_code = args.code.strip()
    start = parse_date(args.start)
    end = parse_date(args.end)

    if start > end:
        print("start 日期不能晚于 end")
        sys.exit(1)

    print(
        f"=== 初始化并导入 TDX 指数日线 ===\n"
        f"DB 通过 app_pg.get_conn 使用环境变量连接\n"
        f"TDX_API_BASE = {TDX_API_BASE}\n"
        f"index_code  = {index_code}\n"
        f"date range  = {start} ~ {end}\n"
    )

    ensure_table_and_hypertable()
    items = fetch_index_kline_all(index_code)
    rows = filter_and_build_rows(items, index_code, start, end)
    upsert_rows(rows)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        print("脚本执行异常:", repr(e))
        sys.exit(1)
