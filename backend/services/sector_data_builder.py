"""Build stock-level Shenwan sector facts from PIT membership and source data."""

from __future__ import annotations

import datetime as dt
import logging
from typing import List

from ..db.pg_pool import get_conn
from .stock_universe_pit_service import (
    DEFAULT_ST_PIT_UNIVERSE_KEY,
    IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX,
)


logger = logging.getLogger(__name__)


class SectorDataBuildContractError(RuntimeError):
    """Raised when a sector_data day cannot be built without ambiguity."""


_PREFLIGHT_DAY_SQL = """\
WITH authoritative_universes AS (
    SELECT universe_key
    FROM market.stock_universe_pit_state
    WHERE status = 'ready'
      AND dirty = FALSE
      AND start_date <= %(trade_date)s
      AND end_date >= %(trade_date)s
      AND (
          universe_key = %(live_universe_key)s
          OR universe_key LIKE %(qe_universe_pattern)s
      )
),
eligible AS (
    SELECT DISTINCT spans.ts_code
    FROM market.stock_universe_pit_spans AS spans
    JOIN authoritative_universes USING (universe_key)
    WHERE spans.eligible_start <= %(trade_date)s
      AND spans.eligible_end >= %(trade_date)s
),
active AS (
    SELECT
        member.ts_code,
        member.l1_code,
        member.l2_code,
        member.in_date,
        MAX(member.in_date) OVER (PARTITION BY member.ts_code) AS latest_in_date
    FROM market.sw_index_member AS member
    JOIN eligible USING (ts_code)
    WHERE member.in_date <= %(trade_date)s
      AND (member.out_date >= %(trade_date)s OR member.out_date IS NULL)
),
latest AS (
    SELECT ts_code, l1_code, l2_code, in_date
    FROM active
    WHERE in_date = latest_in_date
),
mapping_summary AS (
    SELECT
        ts_code,
        COUNT(*) AS latest_mapping_count,
        COUNT(*) FILTER (
            WHERE NULLIF(BTRIM(l1_code), '') IS NULL
               OR NULLIF(BTRIM(l2_code), '') IS NULL
               OR in_date IS NULL
        ) AS invalid_identity_count
    FROM latest
    GROUP BY ts_code
),
canonical_pit AS (
    SELECT
        latest.ts_code,
        MIN(latest.l1_code) AS l1_code,
        MIN(latest.l2_code) AS l2_code,
        MIN(latest.in_date) AS in_date
    FROM latest
    JOIN mapping_summary USING (ts_code)
    WHERE mapping_summary.latest_mapping_count = 1
      AND mapping_summary.invalid_identity_count = 0
    GROUP BY latest.ts_code
),
l2_moneyflow AS (
    SELECT
        pit.l2_code,
        SUM(mf.buy_sm_amount) AS buy_sm_amount,
        SUM(mf.sell_sm_amount) AS sell_sm_amount,
        SUM(mf.buy_md_amount) AS buy_md_amount,
        SUM(mf.sell_md_amount) AS sell_md_amount,
        SUM(mf.buy_lg_amount) AS buy_lg_amount,
        SUM(mf.sell_lg_amount) AS sell_lg_amount,
        SUM(mf.buy_elg_amount) AS buy_elg_amount,
        SUM(mf.sell_elg_amount) AS sell_elg_amount,
        SUM(mf.net_mf_amount) AS net_mf_amount,
        SUM(mf.buy_elg_vol) AS buy_elg_vol,
        SUM(mf.sell_elg_vol) AS sell_elg_vol,
        SUM(mf.net_mf_vol) AS net_mf_vol
    FROM canonical_pit pit
    JOIN market.moneyflow_ts mf
      ON mf.ts_code = pit.ts_code
     AND mf.trade_date = %(trade_date)s
    GROUP BY pit.l2_code
)
SELECT
    (SELECT CASE WHEN EXISTS (SELECT 1 FROM authoritative_universes) THEN 0 ELSE 1 END),
    (
        SELECT COUNT(*)
        FROM eligible
        LEFT JOIN mapping_summary USING (ts_code)
        WHERE mapping_summary.ts_code IS NULL
    ),
    (SELECT COUNT(*) FROM mapping_summary WHERE latest_mapping_count <> 1),
    (SELECT COUNT(*) FROM mapping_summary WHERE invalid_identity_count <> 0),
    (
        SELECT COUNT(*)
        FROM canonical_pit pit
        LEFT JOIN market.sw_daily sd
          ON sd.ts_code = pit.l2_code
         AND sd.trade_date = %(trade_date)s
        WHERE sd.ts_code IS NULL
           OR sd.open IS NULL
           OR sd.high IS NULL
           OR sd.low IS NULL
           OR sd.close IS NULL
           OR sd.pct_change IS NULL
           OR sd.vol IS NULL
           OR sd.amount IS NULL
           OR sd.pe IS NULL
           OR sd.pb IS NULL
           OR sd.total_mv IS NULL
    ),
    (
        SELECT COUNT(*)
        FROM (SELECT DISTINCT l2_code FROM canonical_pit) pit
        LEFT JOIN l2_moneyflow mf USING (l2_code)
        WHERE mf.l2_code IS NULL
           OR mf.buy_sm_amount IS NULL
           OR mf.sell_sm_amount IS NULL
           OR mf.buy_md_amount IS NULL
           OR mf.sell_md_amount IS NULL
           OR mf.buy_lg_amount IS NULL
           OR mf.sell_lg_amount IS NULL
           OR mf.buy_elg_amount IS NULL
           OR mf.sell_elg_amount IS NULL
           OR mf.net_mf_amount IS NULL
           OR mf.buy_elg_vol IS NULL
           OR mf.sell_elg_vol IS NULL
           OR mf.net_mf_vol IS NULL
    )
"""


_DELETE_STALE_DAY_SQL = """\
WITH authoritative_universes AS (
    SELECT universe_key
    FROM market.stock_universe_pit_state
    WHERE status = 'ready'
      AND dirty = FALSE
      AND start_date <= %(trade_date)s
      AND end_date >= %(trade_date)s
      AND (
          universe_key = %(live_universe_key)s
          OR universe_key LIKE %(qe_universe_pattern)s
      )
),
eligible AS (
    SELECT DISTINCT spans.ts_code
    FROM market.stock_universe_pit_spans AS spans
    JOIN authoritative_universes USING (universe_key)
    WHERE spans.eligible_start <= %(trade_date)s
      AND spans.eligible_end >= %(trade_date)s
),
active AS (
    SELECT
        member.ts_code,
        member.in_date,
        MAX(member.in_date) OVER (PARTITION BY member.ts_code) AS latest_in_date
    FROM market.sw_index_member AS member
    JOIN eligible USING (ts_code)
    WHERE member.in_date <= %(trade_date)s
      AND (member.out_date >= %(trade_date)s OR member.out_date IS NULL)
),
pit AS (
    SELECT ts_code
    FROM active
    WHERE in_date = latest_in_date
)
DELETE FROM market.sector_data target
WHERE target.trade_date = %(trade_date)s
  AND NOT EXISTS (
      SELECT 1
      FROM pit
      WHERE pit.ts_code = target.ts_code
  )
"""


_BUILD_DAY_SQL = """\
WITH authoritative_universes AS (
    SELECT universe_key
    FROM market.stock_universe_pit_state
    WHERE status = 'ready'
      AND dirty = FALSE
      AND start_date <= %(trade_date)s
      AND end_date >= %(trade_date)s
      AND (
          universe_key = %(live_universe_key)s
          OR universe_key LIKE %(qe_universe_pattern)s
      )
),
eligible AS (
    SELECT DISTINCT spans.ts_code
    FROM market.stock_universe_pit_spans AS spans
    JOIN authoritative_universes USING (universe_key)
    WHERE spans.eligible_start <= %(trade_date)s
      AND spans.eligible_end >= %(trade_date)s
),
active AS (
    SELECT
        member.ts_code,
        member.l2_code,
        member.in_date,
        MAX(member.in_date) OVER (PARTITION BY member.ts_code) AS latest_in_date
    FROM market.sw_index_member AS member
    JOIN eligible USING (ts_code)
    WHERE member.in_date <= %(trade_date)s
      AND (member.out_date >= %(trade_date)s OR member.out_date IS NULL)
),
pit AS (
    SELECT ts_code, l2_code
    FROM active
    WHERE in_date = latest_in_date
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
    sd.open, sd.high, sd.low, sd.close, sd.pct_change,
    sd.vol, sd.amount, sd.pe, sd.pb, sd.total_mv,
    l2_mf.agg_buy_sm_amt, l2_mf.agg_sell_sm_amt,
    l2_mf.agg_buy_md_amt, l2_mf.agg_sell_md_amt,
    l2_mf.agg_buy_lg_amt, l2_mf.agg_sell_lg_amt,
    l2_mf.agg_buy_elg_amt, l2_mf.agg_sell_elg_amt,
    l2_mf.agg_net_amt,
    l2_mf.agg_buy_elg_vol, l2_mf.agg_sell_elg_vol,
    l2_mf.agg_net_vol
FROM pit
JOIN market.sw_daily sd
  ON pit.l2_code = sd.ts_code
 AND sd.trade_date = %(trade_date)s
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
    """Build market.sector_data from PIT membership and source market facts."""

    def build_date(self, trade_date: dt.date) -> int:
        """Build one day atomically and return the inserted or updated row count."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                params = {
                    "trade_date": trade_date,
                    "live_universe_key": DEFAULT_ST_PIT_UNIVERSE_KEY,
                    "qe_universe_pattern": f"{IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX}%",
                }
                cur.execute(_PREFLIGHT_DAY_SQL, params)
                (
                    universe_not_ready,
                    missing_mapping,
                    ambiguous,
                    invalid_identity,
                    missing_sector_facts,
                    missing_moneyflow_facts,
                ) = cur.fetchone()
                if (
                    universe_not_ready
                    or missing_mapping
                    or ambiguous
                    or invalid_identity
                    or missing_sector_facts
                    or missing_moneyflow_facts
                ):
                    raise SectorDataBuildContractError(
                        "SECTOR_DATA_PIT_CONTRACT_INVALID: "
                        f"trade_date={trade_date}, "
                        f"universe_not_ready={universe_not_ready}, "
                        f"missing_pit_mappings={missing_mapping}, "
                        f"ambiguous_latest_mappings={ambiguous}, "
                        f"invalid_mapping_identities={invalid_identity}, "
                        f"missing_sw_daily_facts={missing_sector_facts}, "
                        f"missing_l2_moneyflow_facts={missing_moneyflow_facts}"
                    )
                cur.execute(_DELETE_STALE_DAY_SQL, params)
                cur.execute(_BUILD_DAY_SQL, params)
                rows = cur.rowcount
            conn.commit()
        return rows

    def build_range(self, start_date: dt.date, end_date: dt.date) -> int:
        """Build an inclusive date range and return the total written row count."""
        trade_dates = self._get_trade_dates(start_date, end_date)
        if not trade_dates:
            logger.warning(
                "sector_data build_range: no trade dates in %s ~ %s",
                start_date,
                end_date,
            )
            return 0

        total = 0
        for index, trade_date in enumerate(trade_dates, 1):
            rows = self.build_date(trade_date)
            total += rows
            if rows > 0 and index % 50 == 0:
                logger.info(
                    "sector_data progress: %d/%d dates, %d total rows (latest: %s = %d rows)",
                    index,
                    len(trade_dates),
                    total,
                    trade_date,
                    rows,
                )

        logger.info(
            "sector_data build_range complete: %d dates, %d total rows",
            len(trade_dates),
            total,
        )
        return total

    def _get_trade_dates(self, start: dt.date, end: dt.date) -> List[dt.date]:
        """Return source trading dates in the requested range."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_TRADE_DATES_SQL, {"start": start, "end": end})
                return [row[0] for row in cur.fetchall()]
