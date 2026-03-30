"""SectorDataBuilder — 从原始表预计算 market.sector_data (22 列行业展开到个股).

核心逻辑: sw_index_member (PIT映射) + sw_daily (行业日线) + moneyflow_ts (个股资金流)
→ 按 PIT L2 映射聚合 → 展开到个股级别 → upsert sector_data

Usage:
    builder = SectorDataBuilder()
    rows = builder.build_range(date(2018, 8, 1), date(2025, 12, 1))
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import List

from ..db.pg_pool import get_conn

logger = logging.getLogger(__name__)

# Single-day upsert SQL — PIT mapping + sw_daily join + moneyflow aggregation
_BUILD_DAY_SQL = """\
WITH pit AS (
    SELECT DISTINCT ON (ts_code)
        ts_code,
        l2_code
    FROM market.sw_index_member
    WHERE in_date <= %(trade_date)s
      AND (out_date >= %(trade_date)s OR out_date IS NULL)
    ORDER BY ts_code, in_date DESC
),
l2_mf AS (
    SELECT
        pit.l2_code,
        SUM(mf.buy_sm_amount)   AS agg_buy_sm_amt,
        SUM(mf.sell_sm_amount)  AS agg_sell_sm_amt,
        SUM(mf.buy_md_amount)   AS agg_buy_md_amt,
        SUM(mf.sell_md_amount)  AS agg_sell_md_amt,
        SUM(mf.buy_lg_amount)   AS agg_buy_lg_amt,
        SUM(mf.sell_lg_amount)  AS agg_sell_lg_amt,
        SUM(mf.buy_elg_amount)  AS agg_buy_elg_amt,
        SUM(mf.sell_elg_amount) AS agg_sell_elg_amt,
        SUM(mf.net_mf_amount)   AS agg_net_amt,
        SUM(mf.buy_elg_vol)     AS agg_buy_elg_vol,
        SUM(mf.sell_elg_vol)    AS agg_sell_elg_vol,
        SUM(mf.net_mf_vol)      AS agg_net_vol
    FROM market.moneyflow_ts mf
    JOIN pit ON mf.ts_code = pit.ts_code
    WHERE mf.trade_date = %(trade_date)s
    GROUP BY pit.l2_code
)
INSERT INTO market.sector_data (
    trade_date, ts_code,
    sw2_open, sw2_high, sw2_low, sw2_close, sw2_pct_change,
    sw2_vol, sw2_amount, sw2_pe, sw2_pb, sw2_total_mv,
    sw2_mf_buy_sm_amt, sw2_mf_sell_sm_amt,
    sw2_mf_buy_md_amt, sw2_mf_sell_md_amt,
    sw2_mf_buy_lg_amt, sw2_mf_sell_lg_amt,
    sw2_mf_buy_elg_amt, sw2_mf_sell_elg_amt,
    sw2_mf_net_amt,
    sw2_mf_buy_elg_vol, sw2_mf_sell_elg_vol,
    sw2_mf_net_vol
)
SELECT
    %(trade_date)s,
    pit.ts_code,
    sd.open,  sd.high,  sd.low,  sd.close,  sd.pct_change,
    sd.vol,   sd.amount, sd.pe,   sd.pb,    sd.total_mv,
    l2_mf.agg_buy_sm_amt,  l2_mf.agg_sell_sm_amt,
    l2_mf.agg_buy_md_amt,  l2_mf.agg_sell_md_amt,
    l2_mf.agg_buy_lg_amt,  l2_mf.agg_sell_lg_amt,
    l2_mf.agg_buy_elg_amt, l2_mf.agg_sell_elg_amt,
    l2_mf.agg_net_amt,
    l2_mf.agg_buy_elg_vol, l2_mf.agg_sell_elg_vol,
    l2_mf.agg_net_vol
FROM pit
LEFT JOIN market.sw_daily sd
    ON pit.l2_code = sd.ts_code AND sd.trade_date = %(trade_date)s
LEFT JOIN l2_mf
    ON pit.l2_code = l2_mf.l2_code
ON CONFLICT (trade_date, ts_code) DO UPDATE SET
    sw2_open            = EXCLUDED.sw2_open,
    sw2_high            = EXCLUDED.sw2_high,
    sw2_low             = EXCLUDED.sw2_low,
    sw2_close           = EXCLUDED.sw2_close,
    sw2_pct_change      = EXCLUDED.sw2_pct_change,
    sw2_vol             = EXCLUDED.sw2_vol,
    sw2_amount          = EXCLUDED.sw2_amount,
    sw2_pe              = EXCLUDED.sw2_pe,
    sw2_pb              = EXCLUDED.sw2_pb,
    sw2_total_mv        = EXCLUDED.sw2_total_mv,
    sw2_mf_buy_sm_amt   = EXCLUDED.sw2_mf_buy_sm_amt,
    sw2_mf_sell_sm_amt  = EXCLUDED.sw2_mf_sell_sm_amt,
    sw2_mf_buy_md_amt   = EXCLUDED.sw2_mf_buy_md_amt,
    sw2_mf_sell_md_amt  = EXCLUDED.sw2_mf_sell_md_amt,
    sw2_mf_buy_lg_amt   = EXCLUDED.sw2_mf_buy_lg_amt,
    sw2_mf_sell_lg_amt  = EXCLUDED.sw2_mf_sell_lg_amt,
    sw2_mf_buy_elg_amt  = EXCLUDED.sw2_mf_buy_elg_amt,
    sw2_mf_sell_elg_amt = EXCLUDED.sw2_mf_sell_elg_amt,
    sw2_mf_net_amt      = EXCLUDED.sw2_mf_net_amt,
    sw2_mf_buy_elg_vol  = EXCLUDED.sw2_mf_buy_elg_vol,
    sw2_mf_sell_elg_vol = EXCLUDED.sw2_mf_sell_elg_vol,
    sw2_mf_net_vol      = EXCLUDED.sw2_mf_net_vol
"""

_TRADE_DATES_SQL = """\
SELECT DISTINCT trade_date
FROM market.moneyflow_ts
WHERE trade_date BETWEEN %(start)s AND %(end)s
ORDER BY trade_date
"""


class SectorDataBuilder:
    """从 sw_index_member + sw_daily + moneyflow_ts 预计算 sector_data 表。"""

    def build_date(self, trade_date: dt.date) -> int:
        """计算单个交易日的 sector_data 并 upsert。返回插入/更新行数。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_BUILD_DAY_SQL, {"trade_date": trade_date})
                rows = cur.rowcount
            conn.commit()
        return rows

    def build_range(self, start_date: dt.date, end_date: dt.date) -> int:
        """计算指定日期范围的 sector_data。返回总行数。"""
        trade_dates = self._get_trade_dates(start_date, end_date)
        if not trade_dates:
            logger.warning("sector_data build_range: no trade dates in %s ~ %s",
                           start_date, end_date)
            return 0

        total = 0
        for i, td in enumerate(trade_dates, 1):
            rows = self.build_date(td)
            total += rows
            if rows > 0 and i % 50 == 0:
                logger.info("sector_data progress: %d/%d dates, %d total rows (latest: %s = %d rows)",
                            i, len(trade_dates), total, td, rows)

        logger.info("sector_data build_range complete: %d dates, %d total rows", len(trade_dates), total)
        return total

    def _get_trade_dates(self, start: dt.date, end: dt.date) -> List[dt.date]:
        """从 moneyflow_ts 获取日期范围内的交易日列表。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_TRADE_DATES_SQL, {"start": start, "end": end})
                return [row[0] for row in cur.fetchall()]
