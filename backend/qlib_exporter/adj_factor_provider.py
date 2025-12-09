"""复权因子提供者模块.

支持从以下来源获取复权因子：
1. 本地数据库表 (market.adj_factor) - 优先
2. Tushare API - 备用

复权因子计算：
- Tushare adj_factor 是后复权因子
- 前复权因子 = adj_factor / 最新adj_factor
- $close = 不复权价格 × 前复权因子
"""

from __future__ import annotations

import os
from datetime import date
from typing import Dict, List, Optional

import pandas as pd

from app_pg import get_conn  # type: ignore[attr-defined]

from .config import ADJ_FACTOR_TABLE


class AdjFactorProvider:
    """复权因子提供者."""

    def __init__(self, use_tushare_fallback: bool = True):
        """初始化.

        Args:
            use_tushare_fallback: 当本地数据库无数据时，是否使用 Tushare API
        """
        self.use_tushare_fallback = use_tushare_fallback
        self._tushare_pro = None
        self._table_exists: Optional[bool] = None

    def _check_table_exists(self) -> bool:
        """检查复权因子表是否存在."""
        if self._table_exists is not None:
            return self._table_exists

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'market' 
                            AND table_name = 'adj_factor'
                        )
                    """)
                    self._table_exists = cur.fetchone()[0]
        except Exception:
            self._table_exists = False

        return self._table_exists

    def _get_tushare_pro(self):
        """获取 Tushare Pro API 实例."""
        if self._tushare_pro is None:
            try:
                import tushare as ts
                from dotenv import load_dotenv

                load_dotenv()
                token = os.getenv("TUSHARE_TOKEN")
                if not token:
                    raise ValueError("TUSHARE_TOKEN not found in environment")
                ts.set_token(token)
                self._tushare_pro = ts.pro_api()
            except Exception as e:
                raise RuntimeError(f"Failed to initialize Tushare: {e}")

        return self._tushare_pro

    def get_adj_factor_from_db(
        self,
        ts_codes: List[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """从数据库获取复权因子.

        Returns:
            DataFrame with columns: ts_code, trade_date, adj_factor
        """
        if not self._check_table_exists():
            return pd.DataFrame()

        codes = list(ts_codes)
        if not codes:
            return pd.DataFrame()

        sql = f"""
            SELECT ts_code, trade_date, adj_factor
            FROM {ADJ_FACTOR_TABLE}
            WHERE ts_code = ANY(%(codes)s)
              AND trade_date >= %(start)s
              AND trade_date <= %(end)s
            ORDER BY ts_code, trade_date
        """
        params = {"codes": codes, "start": start, "end": end}

        with get_conn() as conn:
            df = pd.read_sql(sql, conn, params=params)

        return df

    def get_adj_factor_from_tushare(
        self,
        ts_codes: List[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """从 Tushare API 获取复权因子.

        Returns:
            DataFrame with columns: ts_code, trade_date, adj_factor
        """
        pro = self._get_tushare_pro()

        all_data = []
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")

        for ts_code in ts_codes:
            try:
                df = pro.adj_factor(
                    ts_code=ts_code,
                    start_date=start_str,
                    end_date=end_str,
                )
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                print(f"Warning: Failed to get adj_factor for {ts_code}: {e}")
                continue

        if not all_data:
            return pd.DataFrame()

        result = pd.concat(all_data, ignore_index=True)
        result["trade_date"] = pd.to_datetime(result["trade_date"])
        return result

    def get_adj_factor(
        self,
        ts_codes: List[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """获取复权因子（优先从数据库，备用 Tushare）.

        Returns:
            DataFrame with columns: ts_code, trade_date, adj_factor
        """
        # 尝试从数据库获取
        df = self.get_adj_factor_from_db(ts_codes, start, end)

        if not df.empty:
            return df

        # 数据库无数据，尝试 Tushare
        if self.use_tushare_fallback:
            return self.get_adj_factor_from_tushare(ts_codes, start, end)

        return pd.DataFrame()

    def calculate_qfq_factor(
        self,
        adj_factor_df: pd.DataFrame,
        base_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """计算前复权因子.

        前复权因子 = adj_factor / 基准日adj_factor

        Args:
            adj_factor_df: 包含 ts_code, trade_date, adj_factor 的 DataFrame
            base_date: 基准日期，默认使用每只股票的最新日期

        Returns:
            DataFrame with additional column: qfq_factor
        """
        if adj_factor_df.empty:
            return adj_factor_df

        df = adj_factor_df.copy()

        if base_date is not None:
            # 使用指定的基准日期
            base_factors = df[df["trade_date"] == pd.Timestamp(base_date)].set_index("ts_code")["adj_factor"]
            df["qfq_factor"] = df.apply(
                lambda row: row["adj_factor"] / base_factors.get(row["ts_code"], row["adj_factor"]),
                axis=1,
            )
        else:
            # 使用每只股票的最新日期作为基准
            # 计算每只股票的最大 adj_factor（最新日期）
            max_adj_by_code = df.groupby("ts_code")["adj_factor"].transform("max")
            df["qfq_factor"] = df["adj_factor"] / max_adj_by_code

        return df

    def get_latest_adj_factor(
        self,
        ts_codes: List[str],
    ) -> Dict[str, float]:
        """获取每只股票的最新复权因子.

        Returns:
            Dict mapping ts_code to latest adj_factor
        """
        if self._check_table_exists():
            sql = f"""
                SELECT DISTINCT ON (ts_code) ts_code, adj_factor
                FROM {ADJ_FACTOR_TABLE}
                WHERE ts_code = ANY(%(codes)s)
                ORDER BY ts_code, trade_date DESC
            """
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, {"codes": list(ts_codes)})
                    rows = cur.fetchall()
            return {row[0]: row[1] for row in rows}

        # Tushare fallback - 获取最近的数据
        if self.use_tushare_fallback:
            from datetime import timedelta

            today = date.today()
            df = self.get_adj_factor_from_tushare(
                list(ts_codes),
                today - timedelta(days=30),
                today,
            )
            if not df.empty:
                latest = df.sort_values("trade_date").groupby("ts_code").last()
                return latest["adj_factor"].to_dict()

        return {}
