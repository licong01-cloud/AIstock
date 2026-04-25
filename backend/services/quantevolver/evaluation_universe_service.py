from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from ...db.pg_pool import get_conn

UNIVERSE_POLICY_VERSION = "official_v1"


class EvaluationUniverseService:
    """官方评估股票池服务。"""

    def get_official_universe(self, as_of_date: Optional[str] = None) -> List[str]:
        return self.get_official_universe_with_meta(as_of_date=as_of_date)["instruments"]

    def get_official_universe_with_meta(self, as_of_date: Optional[str] = None) -> Dict[str, Any]:
        effective_as_of = as_of_date or datetime.now().date().isoformat()

        sql = """
            SELECT s.ts_code
            FROM market.stock_basic s
            WHERE s.list_status = 'L'
              AND s.exchange IN ('SSE', 'SZSE')
              AND s.list_date + INTERVAL '365 days' <= %s
              AND NOT EXISTS (
                  SELECT 1
                  FROM market.stock_st st
                  WHERE st.ts_code = s.ts_code
                    AND st.ann_date <= %s
              )
            ORDER BY s.ts_code
        """

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (effective_as_of, effective_as_of))
                instruments = [row[0] for row in cur.fetchall()]

        return {
            "policy_version": UNIVERSE_POLICY_VERSION,
            "as_of_date": effective_as_of,
            "count": len(instruments),
            "rules": [
                "list_status=L",
                "exchange in (SSE,SZSE)",
                "listed_at_least_365_days",
                "exclude_current_and_historical_ST_by_stock_st",
            ],
            "instruments": instruments,
        }
