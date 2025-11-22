from __future__ import annotations

import os
import datetime as dt
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from ..db.pg_pool import get_conn


TDX_API_BASE = os.getenv("TDX_API_BASE", "http://localhost:8080")


@dataclass
class StrategyFilterConfig:
    trade_date: str  # YYYYMMDD
    pct_chg_min: float = -1.5
    pct_chg_max: float = 2.5
    turnover_min: float = 3.0
    volume_hand_min: int = 50_000
    float_share_max: float = 1_500_000_000.0
    float_mv_max: float = 50_000_000_000.0
    net_inflow_today_min: float = 2_000_000.0
    net_inflow_10d_min: float = 2_000_000.0
    top_n: int = 100


@dataclass
class StrategyResult:
    success: bool
    error: Optional[str]
    filters_applied: List[str]
    filters_skipped: List[str]
    trade_date: str
    total_candidates: int
    selected_count: int
    df: Optional[pd.DataFrame]

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        if self.df is not None:
            out["df"] = self.df.to_dict(orient="records")
        else:
            out["df"] = []
        return out


class IndicatorScreeningService:
    """Implements indicator-based screening strategies for next_app.

    逻辑基于根目录 indicator_screening.IndicatorScreeningService 拷贝，并做少量调整：
    - 使用 next_app.backend.db.pg_pool.get_conn 访问 PostgreSQL/TimescaleDB；
    - 其余行为与旧实现保持一致，以便结果对齐。
    """

    def __init__(self) -> None:
        self._ts_pro = None
        self._today_cache: Optional[str] = None

    # ------------------------------------------------------------------
    # Tushare helpers
    # ------------------------------------------------------------------
    def _ensure_pro(self):
        if self._ts_pro is not None:
            return self._ts_pro
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            raise RuntimeError("TUSHARE_TOKEN is not set; cannot run indicator screening strategy")
        import importlib

        ts = importlib.import_module("tushare")
        self._ts_pro = ts.pro_api(token)
        return self._ts_pro

    def _load_daily(self, trade_date: str) -> pd.DataFrame:
        pro = self._ensure_pro()
        df = pro.daily(trade_date=trade_date)
        if df is None or df.empty:
            return pd.DataFrame()
        return df.copy()

    def _load_stock_basic_map(self) -> Dict[str, str]:
        """Load a mapping ts_code -> name from Tushare stock_basic."""

        pro = self._ensure_pro()
        try:
            df = pro.stock_basic(list_status="L", fields="ts_code,name")
        except Exception:
            return {}
        if df is None or df.empty:
            return {}
        df = df.dropna(subset=["ts_code"]).copy()
        df["ts_code"] = df["ts_code"].astype(str)
        return dict(zip(df["ts_code"], df["name"].astype(str)))

    # ------------------------------------------------------------------
    # Minute data helpers (TDX + local DB)
    # ------------------------------------------------------------------
    def _is_today(self, trade_date: str) -> bool:
        if not trade_date:
            return False
        if self._today_cache is None:
            self._today_cache = dt.date.today().strftime("%Y%m%d")
        return trade_date == self._today_cache

    def _fetch_minute_from_db(self, ts_code: str, trade_date: str) -> List[Dict[str, Any]]:
        """Fetch 1-minute bars from local TimescaleDB for given ts_code + trade_date."""

        try:
            target = dt.datetime.strptime(trade_date, "%Y%m%d").date()
        except ValueError:
            return []

        start_dt = dt.datetime.combine(target, dt.time(9, 30))
        end_dt = dt.datetime.combine(target, dt.time(15, 0))

        sql = (
            "SELECT trade_time, close_li, volume_hand "
            "FROM market.kline_minute_raw "
            "WHERE ts_code=%s AND trade_time >= %s AND trade_time <= %s "
            "ORDER BY trade_time"
        )

        rows: List[Dict[str, Any]] = []
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (ts_code, start_dt, end_dt))
                    for trade_time, close_li, volume_hand in cur.fetchall():
                        if not trade_time:
                            continue
                        t_str = trade_time.strftime("%H:%M")
                        price = int(close_li) if close_li is not None else None
                        number = int(volume_hand) if volume_hand is not None else 0
                        rows.append({"Time": t_str, "Price": price, "Number": number})
        except Exception:
            return []

        return rows

    def _fetch_minute_from_tdx(self, code: str, trade_date: str) -> List[Dict[str, Any]]:
        """Fetch minute data from TDX /api/minute for a single stock."""

        params = {"code": code, "date": trade_date}
        url = TDX_API_BASE.rstrip("/") + "/api/minute"
        try:
            resp = requests.get(url, params=params, timeout=5)
            resp.raise_for_status()
            payload = resp.json()
        except Exception:
            return []

        if not isinstance(payload, dict) or payload.get("code") != 0:
            return []

        data = payload.get("data") or {}
        items = data.get("List") or data.get("list") or []
        if isinstance(items, dict):
            items = items.get("List") or items.get("list") or []
        result: List[Dict[str, Any]] = []
        for row in items or []:
            if not isinstance(row, dict):
                continue
            t_str = str(row.get("Time") or row.get("time") or "").strip()
            price = row.get("Price") or row.get("price")
            number = row.get("Number") or row.get("number")
            result.append({"Time": t_str, "Price": price, "Number": number})
        return result

    def _get_minute_bars(self, ts_code: str, trade_date: str) -> List[Dict[str, Any]]:
        """Unified minute data accessor with analysis-date-aware source selection."""

        code = (ts_code or "").split(".")[0]
        if not code:
            return []

        if self._is_today(trade_date):
            return self._fetch_minute_from_tdx(code, trade_date)

        local_rows = self._fetch_minute_from_db(ts_code, trade_date)
        if local_rows:
            return local_rows
        return self._fetch_minute_from_tdx(code, trade_date)

    @staticmethod
    def _compute_0935_metrics(minute_bars: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Compute 9:35 window metrics from minute bars.

        当前策略实现仍然使用日线 pct_chg 近似 9:35 涨跌幅，本方法预留给后续精细化版本。
        """

        if not minute_bars:
            return None

        window = [b for b in minute_bars if "09:30" <= str(b.get("Time", "")) <= "09:35"]
        if not window:
            return None

        window_sorted = sorted(window, key=lambda x: str(x.get("Time", "")))
        first = window_sorted[0]
        last = window_sorted[-1]

        p_open_li = first.get("Price")
        p_0935_li = last.get("Price")
        if p_open_li is None or p_0935_li is None:
            return None

        try:
            p_open = float(p_open_li) / 1000.0
            p_0935 = float(p_0935_li) / 1000.0
        except Exception:
            return None

        if p_open <= 0:
            return None

        pct_chg_0935 = (p_0935 - p_open) / p_open * 100.0
        vol_0935 = 0
        for b in window_sorted:
            try:
                vol_0935 += int(b.get("Number") or 0)
            except Exception:
                continue

        return {
            "price_open": p_open,
            "price_0935": p_0935,
            "pct_chg_0935": pct_chg_0935,
            "vol_0935": vol_0935,
        }

    def _load_daily_basic(self, trade_date: str) -> pd.DataFrame:
        pro = self._ensure_pro()
        df = pro.daily_basic(trade_date=trade_date)
        if df is None or df.empty:
            return pd.DataFrame()
        return df.copy()

    def _load_daily_window(self, end_date: str, window: int = 25) -> pd.DataFrame:
        """Load recent daily data for MA20 computation."""

        pro = self._ensure_pro()
        from datetime import datetime, timedelta

        dt_end = datetime.strptime(end_date, "%Y%m%d")
        dt_start = dt_end - timedelta(days=max(40, window * 2))
        start = dt_start.strftime("%Y%m%d")
        df = pro.daily(start_date=start, end_date=end_date)
        if df is None or df.empty:
            return pd.DataFrame()
        return df.copy()

    def _load_moneyflow(self, trade_date: str) -> pd.DataFrame:
        pro = self._ensure_pro()
        try:
            df = pro.moneyflow_ind_dc(trade_date=trade_date)
        except Exception:
            return pd.DataFrame()
        if df is None or df.empty:
            return pd.DataFrame()
        return df.copy()

    def _load_moneyflow_window(self, end_date: str, window: int = 10) -> pd.DataFrame:
        from datetime import datetime, timedelta

        pro = self._ensure_pro()
        dt_end = datetime.strptime(end_date, "%Y%m%d")
        dt_start = dt_end - timedelta(days=window * 2)
        start = dt_start.strftime("%Y%m%d")
        try:
            df = pro.moneyflow_ind_dc(start_date=start, end_date=end_date)
        except Exception:
            return pd.DataFrame()
        if df is None or df.empty:
            return pd.DataFrame()
        return df.copy()

    # ------------------------------------------------------------------
    def run_open_0935_strategy(self, cfg: StrategyFilterConfig) -> StrategyResult:
        """开盘 9:35 指标选股策略骨架实现。"""

        filters_applied: List[str] = []
        filters_skipped: List[str] = []

        try:
            trade_date = cfg.trade_date
            df_daily = self._load_daily(trade_date)
            df_basic = self._load_daily_basic(trade_date)
            if df_daily.empty or df_basic.empty:
                return StrategyResult(
                    success=False,
                    error=f"daily/daily_basic empty for {trade_date}",
                    filters_applied=filters_applied,
                    filters_skipped=filters_skipped,
                    trade_date=trade_date,
                    total_candidates=0,
                    selected_count=0,
                    df=pd.DataFrame(),
                )

            df = pd.merge(df_daily, df_basic, on="ts_code", how="inner", suffixes=("", "_basic"))

            # 剔除 ST / 退市 / 非沪深京普通 A 股
            if "name" in df.columns:
                mask_not_st = ~df["name"].astype(str).str.contains("ST", case=False, na=False)
            else:
                mask_not_st = True
            suffix_ok = df["ts_code"].str.endswith((".SH", ".SZ", ".BJ"))
            prefix_ok = df["ts_code"].str.match(r"^(0|3|6)\d{5}\.")
            df = df[suffix_ok & prefix_ok & mask_not_st].copy()

            # 涨跌幅区间（近似 9:35 前涨跌幅）
            if "pct_chg" in df.columns:
                mask_pct = (df["pct_chg"] >= cfg.pct_chg_min) & (df["pct_chg"] <= cfg.pct_chg_max)
                df = df[mask_pct].copy()
                filters_applied.append(
                    f"当日涨跌幅介于 {cfg.pct_chg_min}% 和 {cfg.pct_chg_max}% 之间（使用日线 pct_chg 近似 9:35 前涨跌幅）"
                )
            else:
                filters_skipped.append("缺少 pct_chg 列，无法过滤涨跌幅区间")

            # 换手率 / 量能
            if "turnover_rate" in df.columns:
                df = df[df["turnover_rate"] >= cfg.turnover_min].copy()
                filters_applied.append(f"换手率 ≥ {cfg.turnover_min}%")
            else:
                filters_skipped.append("缺少 turnover_rate，未做换手率过滤")

            if "vol" in df.columns:
                df = df[df["vol"] >= cfg.volume_hand_min].copy()
                filters_applied.append(f"当日成交量 ≥ {cfg.volume_hand_min} 手")
            else:
                filters_skipped.append("缺少 vol 列，未做量能过滤")

            # 流通股本 / 流通市值
            if "float_share" in df.columns:
                df = df[df["float_share"] <= cfg.float_share_max].copy()
                filters_applied.append(f"流通股本 ≤ {cfg.float_share_max:.0f} 股（≈150亿）")
            else:
                filters_skipped.append("缺少 float_share，未做流通股本过滤")

            if "circ_mv" in df.columns:
                mv_limit_10k = cfg.float_mv_max / 10_000.0
                df = df[df["circ_mv"] * 10_000 <= cfg.float_mv_max].copy()
                filters_applied.append(
                    f"流通市值 ≤ {cfg.float_mv_max/1e8:.0f} 亿（通过 circ_mv≤{mv_limit_10k:.0f} 万元近似）"
                )
            else:
                filters_skipped.append("缺少 circ_mv，未做流通市值过滤")

            # 股价在 20 日均线上方
            df_win = self._load_daily_window(trade_date, window=20)
            if not df_win.empty:
                df_win = df_win.sort_values(["ts_code", "trade_date"])
                df_win["ma20"] = (
                    df_win.groupby("ts_code")["close"]
                    .rolling(window=20, min_periods=1)
                    .mean()
                    .reset_index(level=0, drop=True)
                )
                last = df_win[df_win["trade_date"] == trade_date][["ts_code", "ma20"]].copy()
                df = df.merge(last, on="ts_code", how="left")
                if "ma20" in df.columns:
                    df = df[df["close"] >= df["ma20"]].copy()
                    filters_applied.append("收盘价在 20 日均线上方")
                else:
                    filters_skipped.append("无法计算 ma20，未做均线过滤")
            else:
                filters_skipped.append("未能获取足够日线窗口数据，未做 20 日均线过滤")

            # 资金流过滤（当日 + 近 10 日净流入）
            mf_today = self._load_moneyflow(trade_date)
            if not mf_today.empty:
                mf_today = (
                    mf_today[["ts_code", "net_mf_amount"]].copy()
                    if "net_mf_amount" in mf_today.columns
                    else mf_today
                )
                if "net_mf_amount" in mf_today.columns:
                    df = df.merge(
                        mf_today.rename(columns={"net_mf_amount": "net_mf_today"}),
                        on="ts_code",
                        how="left",
                    )
                    df = df[df["net_mf_today"].fillna(0) >= cfg.net_inflow_today_min].copy()
                    filters_applied.append(
                        f"当日主力净流入 ≥ {cfg.net_inflow_today_min:.0f} 元（Tushare moneyflow_ind_dc.net_mf_amount）"
                    )
                else:
                    filters_skipped.append("moneyflow_ind_dc 缺少 net_mf_amount 列，未做当日资金流过滤")
            else:
                filters_skipped.append("moneyflow_ind_dc 当日数据为空，未做当日资金流过滤")

            mf_win = self._load_moneyflow_window(trade_date, window=10)
            if (
                not mf_win.empty
                and "net_mf_amount" in mf_win.columns
                and "trade_date" in mf_win.columns
            ):
                win_sum = (
                    mf_win.groupby("ts_code")["net_mf_amount"]
                    .sum()
                    .reset_index()
                    .rename(columns={"net_mf_amount": "net_mf_10d"})
                )
                df = df.merge(win_sum, on="ts_code", how="left")
                df = df[df["net_mf_10d"].fillna(0) >= cfg.net_inflow_10d_min].copy()
                filters_applied.append(f"近 10 日主力净流入 ≥ {cfg.net_inflow_10d_min:.0f} 元")
            else:
                filters_skipped.append("未能计算近10日净流入（moneyflow_ind_dc 窗口不足或缺少 net_mf_amount）")

            filters_skipped.extend(
                [
                    "分时 9:35 前涨跌幅 / 量比精确过滤暂未实现（当前使用日线 pct_chg 近似）",
                    "分时图今昨成交比（盘口结构）暂未实现",
                    "股吧人气排名暂未接入数据源",
                    "昨日是否涨停暂未通过日线涨停价精确判定（可在后续版本中补充）",
                ]
            )

            total_candidates = len(df)
            if "pct_chg" in df.columns:
                df = df.sort_values("pct_chg", ascending=False)
            df = df.head(cfg.top_n).reset_index(drop=True)

            if "name" not in df.columns or df["name"].isna().any():
                name_map = self._load_stock_basic_map()
                if name_map:
                    if "name" not in df.columns:
                        df["name"] = df["ts_code"].map(name_map)
                    else:
                        df["name"] = df["name"].fillna(df["ts_code"].map(name_map))
            if "name" not in df.columns:
                df["name"] = df["ts_code"].astype(str)

            return StrategyResult(
                success=True,
                error=None,
                filters_applied=filters_applied,
                filters_skipped=filters_skipped,
                trade_date=trade_date,
                total_candidates=total_candidates,
                selected_count=len(df),
                df=df,
            )

        except Exception as exc:
            return StrategyResult(
                success=False,
                error=str(exc),
                filters_applied=filters_applied,
                filters_skipped=filters_skipped,
                trade_date=cfg.trade_date,
                total_candidates=0,
                selected_count=0,
                df=pd.DataFrame(),
            )


_service_singleton: Optional[IndicatorScreeningService] = None


def get_indicator_screening_service() -> IndicatorScreeningService:
    global _service_singleton
    if _service_singleton is None:
        _service_singleton = IndicatorScreeningService()
    return _service_singleton
