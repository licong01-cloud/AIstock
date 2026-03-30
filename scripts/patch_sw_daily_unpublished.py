"""补齐 sw_daily 中 is_pub=0 的 L2 行业日线数据.

从 sw_index_classify 动态读取 is_pub='0' 的 L2 行业，通过东方财富搜索 API
按行业名称查找对应的 BK 板块代码，再从东财 push2his API 获取 OHLC 数据。
pe/pb/total_mv/float_mv 通过 daily_basic 成分股整体法聚合。

数据来源：
  - OHLC/vol/amount/pct_change: 东方财富行业板块 K 线 API (按名称动态匹配)
  - pe/pb/total_mv/float_mv: daily_basic 成分股整体法聚合

用法：
  python scripts/patch_sw_daily_unpublished.py                     # 增量（自动推断日期）
  python scripts/patch_sw_daily_unpublished.py --start 20180801 --end 20260311  # 指定范围
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── 配置 ──────────────────────────────────────────────────────────

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    application_name="patch-sw-daily-unpublished",
)

EM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/",
}

UPSERT_SQL = """\
INSERT INTO market.sw_daily
    (ts_code, trade_date, name, open, low, high, close, change, pct_change, vol, amount, pe, pb, float_mv, total_mv)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ts_code, trade_date) DO UPDATE SET
    name = EXCLUDED.name, open = EXCLUDED.open, low = EXCLUDED.low,
    high = EXCLUDED.high, close = EXCLUDED.close, change = EXCLUDED.change,
    pct_change = EXCLUDED.pct_change, vol = EXCLUDED.vol, amount = EXCLUDED.amount,
    pe = EXCLUDED.pe, pb = EXCLUDED.pb, float_mv = EXCLUDED.float_mv, total_mv = EXCLUDED.total_mv
"""


# ── 从数据库动态读取未发布 L2 行业 ──────────────────────────────

def load_unpublished_l2(cur) -> List[Tuple[str, str]]:
    """从 sw_index_classify 读取 is_pub='0' 且有成分股的 L2 行业.

    跳过 sw_index_member 中无任何成分股的空壳行业。
    返回 [(ts_code, name), ...].
    """
    cur.execute("""
        SELECT c.index_code, c.industry_name
        FROM market.sw_index_classify c
        WHERE c.level = 'L2' AND c.is_pub = '0'
          AND EXISTS (
              SELECT 1 FROM market.sw_index_member m WHERE m.l2_code = c.index_code
          )
        ORDER BY c.index_code
    """)
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError("sw_index_classify 中未找到 is_pub='0' 且有成分股的 L2 行业")
    return [(r[0], r[1]) for r in rows]


# ── 东财搜索 API: 按名称查找 BK 代码 ─────────────────────────────

def resolve_bk_code(industry_name: str) -> str:
    """通过东财搜索 API 按行业名称查找对应的 BK 板块代码.

    搜索名称时去除 "Ⅱ" 后缀以提高匹配率，
    然后在结果中精确匹配完整名称。
    """
    search_name = industry_name.replace("Ⅱ", "").strip()
    url = "https://searchadapter.eastmoney.com/api/suggest/get"
    params = {
        "input": search_name,
        "type": "14",  # 板块类型
        "token": "D43BF722C8E33BDC906FB84D85E326E8",
        "count": "10",
    }
    resp = requests.get(url, params=params, headers=EM_HEADERS, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("QuotationCodeTable", {}).get("Data") or []

    bk_results = [(r.get("Code", ""), r.get("Name", "")) for r in results if r.get("Code", "").startswith("BK")]

    # 1. 精确匹配: 东财名称 == 申万名称
    for code, name in bk_results:
        if name == industry_name:
            return code

    # 2. 去Ⅱ匹配: 东财名称 == 申万名称去掉Ⅱ后缀 (东财部分板块不含Ⅱ)
    base_name = industry_name.replace("Ⅱ", "").strip()
    for code, name in bk_results:
        if name == base_name:
            logger.warning("东财板块 %s '%s' 与申万 '%s' 名称略有差异 (缺少Ⅱ后缀), 视为匹配",
                           code, name, industry_name)
            return code

    # 未匹配时报错
    raise RuntimeError(
        f"东财搜索未匹配行业 '{industry_name}' (搜索词: '{search_name}'), "
        f"候选: {bk_results}"
    )


# ── 东财 K 线获取 ─────────────────────────────────────────────────

def fetch_em_klines(bk_code: str, start: str, end: str) -> List[dict]:
    """从东财 push2his API 获取行业板块日 K 线."""
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": f"90.{bk_code}",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "beg": start,
        "end": end,
        "lmt": "10000",
        "ut": "fa5fd1943c7b386f172d6893dbbd1d0c",
    }
    resp = requests.get(url, params=params, headers=EM_HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    # 验证返回的板块名称包含搜索关键词
    returned_name = data.get("data", {}).get("name", "")
    returned_code = data.get("data", {}).get("code", "")
    if returned_code != bk_code:
        raise RuntimeError(f"东财返回代码 {returned_code} != 请求 {bk_code}")

    klines = data.get("data", {}).get("klines", [])
    rows = []
    for k in klines:
        f = k.split(",")
        # 日期,开盘,收盘,最高,最低,成交量(手),成交额(元),振幅,涨跌幅,涨跌额,换手率
        rows.append({
            "trade_date": datetime.strptime(f[0], "%Y-%m-%d").date(),
            "open": float(f[1]),
            "close": float(f[2]),
            "high": float(f[3]),
            "low": float(f[4]),
            "vol": float(f[5]) / 100,       # 手 → 万股 (1万股=100手)
            "amount": float(f[6]) / 10000,   # 元 → 万元
            "pct_change": float(f[8]),
            "change": float(f[9]),
        })
    return rows


# ── pe/pb/mv 聚合 ────────────────────────────────────────────────

def calc_fundamental_agg(
    cur, sw_codes: List[str], start_date: date, end_date: date,
) -> Dict[Tuple[str, date], dict]:
    """从 daily_basic + sw_index_member PIT 映射聚合 pe/pb/total_mv/float_mv."""
    cur.execute("""
        WITH trade_dates AS (
            SELECT cal_date AS trade_date
            FROM market.trading_calendar
            WHERE is_trading = true
              AND cal_date BETWEEN %s AND %s
        ),
        pit AS (
            SELECT DISTINCT ON (m.ts_code, td.trade_date)
                td.trade_date, m.ts_code, m.l2_code
            FROM trade_dates td
            CROSS JOIN LATERAL (
                SELECT ts_code, l2_code, in_date
                FROM market.sw_index_member
                WHERE l2_code = ANY(%s)
                  AND in_date <= td.trade_date
                  AND (out_date >= td.trade_date OR out_date IS NULL)
            ) m
            ORDER BY m.ts_code, td.trade_date, m.in_date DESC
        )
        SELECT
            pit.l2_code,
            pit.trade_date,
            SUM(db.total_mv) FILTER (WHERE NOT (db.total_mv = 'NaN'::numeric)),
            SUM(db.circ_mv)  FILTER (WHERE NOT (db.circ_mv  = 'NaN'::numeric)),
            SUM(db.total_mv) FILTER (WHERE NOT (db.total_mv = 'NaN'::numeric)) /
                NULLIF(SUM(db.total_mv / db.pe_ttm) FILTER (
                    WHERE NOT (db.pe_ttm = 'NaN'::numeric) AND db.pe_ttm > 0
                    AND NOT (db.total_mv = 'NaN'::numeric)
                ), 0),
            SUM(db.total_mv) FILTER (WHERE NOT (db.total_mv = 'NaN'::numeric)) /
                NULLIF(SUM(db.total_mv / db.pb) FILTER (
                    WHERE NOT (db.pb = 'NaN'::numeric) AND db.pb > 0
                    AND NOT (db.total_mv = 'NaN'::numeric)
                ), 0)
        FROM pit
        JOIN market.daily_basic db ON pit.ts_code = db.ts_code AND pit.trade_date = db.trade_date
        GROUP BY pit.l2_code, pit.trade_date
    """, (start_date, end_date, sw_codes))

    result: Dict[Tuple[str, date], dict] = {}
    for l2, td, mv, fmv, pe, pb in cur.fetchall():
        result[(l2, td)] = {
            "total_mv": float(mv) if mv is not None else None,
            "float_mv": float(fmv) if fmv is not None else None,
            "pe": float(pe) if pe is not None else None,
            "pb": float(pb) if pb is not None else None,
        }
    return result


# ── 主流程 ────────────────────────────────────────────────────────

def patch(start_date: Optional[str] = None, end_date: Optional[str] = None) -> int:
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()

    # Step 0: 从数据库读取未发布 L2 行业
    unpublished = load_unpublished_l2(cur)
    sw_codes = [code for code, _ in unpublished]
    logger.info("未发布 L2 行业: %d 个", len(unpublished))
    for code, name in unpublished:
        logger.info("  %s %s", code, name)

    # Step 0b: 按名称动态解析 BK 代码
    sw_to_bk: Dict[str, str] = {}  # sw_code → bk_code
    for sw_code, sw_name in unpublished:
        bk_code = resolve_bk_code(sw_name)
        sw_to_bk[sw_code] = bk_code
        logger.info("  %s %s → %s", sw_code, sw_name, bk_code)
        time.sleep(0.3)

    # 结束日期：对齐到已发布行业的 max(trade_date)
    if end_date:
        e = datetime.strptime(end_date, "%Y%m%d").date()
    else:
        cur.execute(
            "SELECT MAX(trade_date) FROM market.sw_daily WHERE ts_code != ALL(%s)",
            (sw_codes,),
        )
        e = cur.fetchone()[0] or date.today()

    # 已发布行业的 min(trade_date)，用于首次补齐的起始对齐
    cur.execute(
        "SELECT MIN(trade_date) FROM market.sw_daily WHERE ts_code != ALL(%s)",
        (sw_codes,),
    )
    pub_min = cur.fetchone()[0] or date(2018, 8, 1)

    # 交易日历
    cur.execute("""
        SELECT cal_date FROM market.trading_calendar
        WHERE is_trading = true AND cal_date BETWEEN %s AND %s
    """, (pub_min, e))
    trading_dates = {r[0] for r in cur.fetchall()}

    # 逐个行业独立推断起始日期并补齐
    total = 0
    for sw_code, sw_name in unpublished:
        if start_date:
            s = datetime.strptime(start_date, "%Y%m%d").date()
        else:
            cur.execute(
                "SELECT MAX(trade_date) FROM market.sw_daily WHERE ts_code = %s",
                (sw_code,),
            )
            row = cur.fetchone()
            if row[0]:
                s = row[0] + timedelta(days=1)
            else:
                s = pub_min

        if s > e:
            logger.info("  %s %s: 已是最新 (max=%s)", sw_code, sw_name, s - timedelta(days=1))
            continue

        bk_code = sw_to_bk[sw_code]
        start_str = s.strftime("%Y%m%d")
        end_str = e.strftime("%Y%m%d")
        logger.info("  %s %s (%s): 补齐 %s ~ %s", sw_code, sw_name, bk_code, s, e)

        # Step 1: 获取东财 OHLC
        rows = fetch_em_klines(bk_code, start_str, end_str)
        ohlc_by_date = {r["trade_date"]: r for r in rows}
        logger.info("    OHLC: %d 天", len(rows))
        time.sleep(0.3)

        # Step 2: 聚合 pe/pb/mv
        fund_map = calc_fundamental_agg(cur, [sw_code], s, e)
        logger.info("    pe/pb/mv: %d 天", len(fund_map))

        # Step 3: 合并写入
        batch = []
        for td, ohlc in sorted(ohlc_by_date.items()):
            if td not in trading_dates:
                continue
            fund = fund_map.get((sw_code, td), {})
            batch.append((
                sw_code, td, sw_name,
                ohlc["open"], ohlc["low"], ohlc["high"], ohlc["close"],
                ohlc["change"], ohlc["pct_change"], ohlc["vol"], ohlc["amount"],
                fund.get("pe"), fund.get("pb"), fund.get("float_mv"), fund.get("total_mv"),
            ))
        cur.executemany(UPSERT_SQL, batch)
        conn.commit()
        total += len(batch)
        logger.info("    写入: %d 行", len(batch))

    conn.close()
    logger.info("补齐完成，共 %d 行", total)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="补齐 sw_daily 未发布行业数据")
    parser.add_argument("--start", help="起始日期 YYYYMMDD")
    parser.add_argument("--end", help="结束日期 YYYYMMDD")
    args = parser.parse_args()
    patch(args.start, args.end)


if __name__ == "__main__":
    main()
