"""
QuantEvolver data service layer

与 task 选股（qlib_exporter/db_reader.py）完全隔离，互不干扰。

数据一致性保证（与 qlib_exporter/db_reader.py 的 load_qlib_daily_data 完全一致）：
- 行情数据：market.kline_daily_raw（未复权原始数据）+ market.adj_factor（复权因子）实时计算前复权
- 前复权公式：price_qfq = raw_li / 1000 * (adj_t / adj_latest)
  - adj_t：当日复权因子（Tushare后复权因子）
  - adj_latest：该股票最新复权因子（按每只股票分组取最大值）
  - qfq_factor = adj_t / adj_latest（前复权因子，最新日 = 1.0）
- 价格单位：数据库存储为厘（×1000），输出为元（÷1000）
- 成交量：volume_hand（手），输出保持手为单位（与 daily_pv.h5 一致）
- 成交额：amount_li（厘），输出为元（÷1000），不做复权
- 字段命名：不带 $ 前缀（open/close/high/low/volume/amount/factor）
- bak_basic：排除上市日期前的数据和停牌期间的数据（JOIN kline_daily_raw 过滤）
- 预计算因子：从 moneyflow_ts 实时计算 mf_* 派生因子（与 generate_static_factors_bundle.py 逻辑一致）

日线数据统一通过 kline_daily_raw + adj_factor 实时计算前复权。
"""
from __future__ import annotations

import hashlib
import logging
import threading
import time
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from ..db.pg_pool import get_conn

logger = logging.getLogger("aistock.qe_data_service")
LIVE_ASOF_STATIC_PREFIXES = ("bb_", "cp_", "md_", "sw2_")
LIVE_ASOF_STATIC_COLUMNS = ("db_dv_ratio", "db_dv_ttm")

# ============================================================
# 内存缓存（选股执行期间避免重复查询数据库）
# ============================================================

class _DataCache:
    """
    轻量级内存缓存，用于选股执行期间缓存常用数据集。

    设计原则：
    - 同一次选股执行（同一批 instruments + 日期范围）只查询一次数据库
    - TTL 默认 30 分钟，超时自动失效
    - 线程安全（使用 threading.Lock）
    - 手动调用 clear() 可立即清除所有缓存

    缓存 Key 格式："{func_name}:{instruments_hash}:{start_date}:{end_date}"
    """

    def __init__(self, default_ttl_seconds: int = 1800):
        self._store: Dict[str, Tuple[pd.DataFrame, float]] = {}
        self._lock = threading.Lock()
        self._default_ttl = default_ttl_seconds

    def _make_key(self, func_name: str, instruments: List[str], start_date: str, end_date: str) -> str:
        instr_hash = hashlib.md5("|".join(sorted(instruments)).encode()).hexdigest()[:8]
        return f"{func_name}:{instr_hash}:{start_date}:{end_date}"

    def get(self, func_name: str, instruments: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        key = self._make_key(func_name, instruments, start_date, end_date)
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            df, expire_at = entry
            if time.time() > expire_at:
                del self._store[key]
                return None
            return df

    def set(self, func_name: str, instruments: List[str], start_date: str, end_date: str,
            df: pd.DataFrame, ttl_seconds: Optional[int] = None) -> None:
        key = self._make_key(func_name, instruments, start_date, end_date)
        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl
        with self._lock:
            self._store[key] = (df, time.time() + ttl)

    def clear(self) -> int:
        with self._lock:
            count = len(self._store)
            self._store.clear()
        logger.info(f"QE数据缓存已清除，共清除 {count} 条记录")
        return count

    def stats(self) -> Dict:
        with self._lock:
            now = time.time()
            valid = sum(1 for _, (_, exp) in self._store.items() if now <= exp)
            return {"total": len(self._store), "valid": valid, "expired": len(self._store) - valid}


# 全局缓存实例（进程级别，跨请求共享）
_CACHE = _DataCache(default_ttl_seconds=1800)


def clear_data_cache() -> int:
    """手动清除所有数据缓存（可在选股任务开始前调用）"""
    return _CACHE.clear()


def get_cache_stats() -> Dict:
    """获取缓存统计信息"""
    return _CACHE.stats()


# ============================================================
# 常量配置
# ============================================================

PRICE_UNIT_DIVISOR = 1000.0

DAILY_BASIC_FIELD_MAP: Dict[str, str] = {
    "close":           "db_close",
    "turnover_rate":   "db_turnover_rate",
    "turnover_rate_f": "db_turnover_rate_f",
    "volume_ratio":    "db_volume_ratio",
    "pe":              "db_pe",
    "pe_ttm":          "db_pe_ttm",
    "pb":              "db_pb",
    "ps":              "db_ps",
    "ps_ttm":          "db_ps_ttm",
    "dv_ratio":        "db_dv_ratio",
    "dv_ttm":          "db_dv_ttm",
    "total_share":     "db_total_share",
    "float_share":     "db_float_share",
    "free_share":      "db_free_share",
    "total_mv":        "db_total_mv",
    "circ_mv":         "db_circ_mv",
}

MONEYFLOW_FIELD_MAP: Dict[str, str] = {
    "buy_sm_vol":      "mf_sm_buy_vol",
    "buy_sm_amount":   "mf_sm_buy_amt",
    "sell_sm_vol":     "mf_sm_sell_vol",
    "sell_sm_amount":  "mf_sm_sell_amt",
    "buy_md_vol":      "mf_md_buy_vol",
    "buy_md_amount":   "mf_md_buy_amt",
    "sell_md_vol":     "mf_md_sell_vol",
    "sell_md_amount":  "mf_md_sell_amt",
    "buy_lg_vol":      "mf_lg_buy_vol",
    "buy_lg_amount":   "mf_lg_buy_amt",
    "sell_lg_vol":     "mf_lg_sell_vol",
    "sell_lg_amount":  "mf_lg_sell_amt",
    "buy_elg_vol":     "mf_elg_buy_vol",
    "buy_elg_amount":  "mf_elg_buy_amt",
    "sell_elg_vol":    "mf_elg_sell_vol",
    "sell_elg_amount": "mf_elg_sell_amt",
    "net_mf_vol":      "mf_net_vol",
    "net_mf_amount":   "mf_net_amt",
}

BAK_BASIC_FIELD_MAP: Dict[str, str] = {
    "pe_dyn":            "bb_pe_dyn",
    "total_assets":      "bb_total_assets",
    "liquid_assets":     "bb_liquid_assets",
    "fixed_assets":      "bb_fixed_assets",
    "reserved":          "bb_reserved",
    "reserved_pershare": "bb_reserved_pershare",
    "eps":               "bb_eps",
    "bvps":              "bb_bvps",
    "undp":              "bb_undp",
    "per_undp":          "bb_per_undp",
    "rev_yoy":           "bb_rev_yoy",
    "profit_yoy":        "bb_profit_yoy",
    "gpr":               "bb_gpr",
    "npr":               "bb_npr",
    "holder_num":        "bb_holder_num",
}

MARGIN_DETAIL_FIELD_MAP: Dict[str, str] = {
    "rzye":   "md_rzye",
    "rqye":   "md_rqye",
    "rzmre":  "md_rzmre",
    "rqyl":   "md_rqyl",
    "rzche":  "md_rzche",
    "rqchl":  "md_rqchl",
    "rqmcl":  "md_rqmcl",
    "rzrqye": "md_rzrqye",
}

CYQ_PERF_FIELD_MAP: Dict[str, str] = {
    "his_low":    "cp_his_low",
    "his_high":   "cp_his_high",
    "cost_5pct":  "cp_cost_5pct",
    "cost_15pct": "cp_cost_15pct",
    "cost_50pct": "cp_cost_50pct",
    "cost_85pct": "cp_cost_85pct",
    "cost_95pct": "cp_cost_95pct",
    "weight_avg": "cp_weight_avg",
    "winner_rate":"cp_winner_rate",
}

# sector_data 列在数据库中已经是 sw2_* 前缀，无需重命名
SECTOR_DATA_COLUMNS: List[str] = [
    "sw2_open", "sw2_high", "sw2_low", "sw2_close", "sw2_pct_change",
    "sw2_vol", "sw2_amount", "sw2_pe", "sw2_pb", "sw2_total_mv",
    "sw2_mf_buy_sm_amt", "sw2_mf_sell_sm_amt",
    "sw2_mf_buy_md_amt", "sw2_mf_sell_md_amt",
    "sw2_mf_buy_lg_amt", "sw2_mf_sell_lg_amt",
    "sw2_mf_buy_elg_amt", "sw2_mf_sell_elg_amt",
    "sw2_mf_net_amt",
    "sw2_mf_buy_elg_vol", "sw2_mf_sell_elg_vol",
    "sw2_mf_net_vol",
]


def _normalize_instrument(code: str) -> str:
    s = str(code).strip()
    if not s:
        return s
    if "." in s:
        return s.upper()
    up = s.upper()
    if len(up) >= 8 and up[:2] in {"SH", "SZ", "BJ"}:
        return f"{up[2:]}.{up[:2]}"
    return up


def _to_date(d: Union[str, date, datetime]) -> date:
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d), "%Y-%m-%d").date()


def _safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    n = numer.astype("float64")
    d = denom.astype("float64").mask(denom.astype("float64") == 0, np.nan)
    return (n / d).astype("float64")


def _rolling_sum_by_instrument(s: pd.Series, window: int) -> pd.Series:
    out = (
        s.groupby(level="instrument")
        .rolling(window=window, min_periods=window)
        .sum()
        .reset_index(level=0, drop=True)
    )
    return out.astype("float64")


def _build_multiindex_df(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    df["datetime"] = pd.to_datetime(df["trade_date"])
    df["instrument"] = df["ts_code"].apply(_normalize_instrument)
    df = df.set_index(["datetime", "instrument"])
    cols = [c for c in df.columns if c.startswith(prefix)]
    result = df[cols].copy()
    for c in cols:
        result[c] = pd.to_numeric(result[c], errors="coerce").astype("float32")
    return result.sort_index()


# ============================================================
# 核心数据加载函数
# ============================================================

def load_daily_pv(
    instruments: List[str],
    start_date: Union[str, date],
    end_date: Union[str, date],
) -> pd.DataFrame:
    """
    加载日线行情数据（前复权），与 daily_pv.h5 完全一致。

    数据源：market.kline_daily_raw（未复权原始数据）+ market.adj_factor（复权因子）
    前复权公式（与 qlib_exporter/db_reader.py 的 load_qlib_daily_data 完全一致）：
      qfq_factor = adj_t / adj_latest（按每只股票分组，adj_latest = 该股票最大adj_factor值）
      price_qfq  = raw_li / 1000 * qfq_factor
      volume_qfq = volume_hand * 100 / qfq_factor（手->股，再按因子反向调整）
      amount     = amount_li / 1000（不复权，仅单位转换）
      factor     = qfq_factor（前复权因子，最新日 = 1.0）

    注意：adj_factor 表最新日期可能比 kline_daily_raw 晚1天（adj_factor 每日盘后更新）。
    对于 kline_daily_raw 中有数据但 adj_factor 中无对应记录的日期，使用前向填充（ffill）。

    Returns:
        MultiIndex(datetime, instrument) DataFrame
        列：open, high, low, close, volume, amount, factor
    """
    start = _to_date(start_date)
    end = _to_date(end_date)
    ts_codes = [_normalize_instrument(i) for i in instruments]

    if not ts_codes:
        return pd.DataFrame()

    # 缓存命中检查
    cached = _CACHE.get("load_daily_pv", ts_codes, start.isoformat(), end.isoformat())
    if cached is not None:
        logger.debug(f"load_daily_pv: 缓存命中 {len(ts_codes)} 支股票 {start}~{end}")
        return cached

    placeholders = ",".join(["%s"] * len(ts_codes))

    # 1. 加载未复权原始行情
    sql_raw = f"""
        SELECT trade_date, ts_code,
               open_li, high_li, low_li, close_li, volume_hand, amount_li
        FROM market.kline_daily_raw
        WHERE ts_code IN ({placeholders})
          AND trade_date >= %s AND trade_date <= %s
        ORDER BY trade_date, ts_code
    """
    params_raw = ts_codes + [start.isoformat(), end.isoformat()]

    with get_conn() as conn:
        df_raw = pd.read_sql(sql_raw, conn, params=params_raw)

    if df_raw.empty:
        return pd.DataFrame()

    # 2. 加载复权因子（需要扩展到最新日期以获取 adj_latest）
    #    查询范围：start 到今天（确保能获取到最新的 adj_factor 用于归一化）
    from datetime import date as date_type
    today = date_type.today()
    sql_adj = f"""
        SELECT ts_code, trade_date, adj_factor
        FROM market.adj_factor
        WHERE ts_code IN ({placeholders})
          AND trade_date >= %s AND trade_date <= %s
        ORDER BY ts_code, trade_date
    """
    params_adj = ts_codes + [start.isoformat(), today.isoformat()]

    with get_conn() as conn:
        df_adj = pd.read_sql(sql_adj, conn, params=params_adj)

    if df_adj.empty:
        raise RuntimeError(
            f"market.adj_factor 中没有找到复权因子数据，"
            f"股票: {ts_codes[:5]}...，日期范围: {start} ~ {today}。"
            f"请确认 adj_factor 表数据是否正常。"
        )

    # 3. 计算前复权因子：qfq_factor = adj_t / adj_latest
    #    adj_latest = 每只股票的最大 adj_factor（对应最新日期）
    df_adj["adj_factor"] = pd.to_numeric(df_adj["adj_factor"], errors="coerce")
    adj_latest = df_adj.groupby("ts_code")["adj_factor"].transform("max")
    df_adj["qfq_factor"] = df_adj["adj_factor"] / adj_latest

    # 4. 将复权因子 merge 到原始行情
    #    先确保日期类型一致
    df_raw["trade_date"] = pd.to_datetime(df_raw["trade_date"]).dt.date
    df_adj["trade_date"] = pd.to_datetime(df_adj["trade_date"]).dt.date

    df = df_raw.merge(
        df_adj[["ts_code", "trade_date", "qfq_factor"]],
        on=["ts_code", "trade_date"],
        how="left",
    )

    # 5. 对 adj_factor 缺失的日期（kline_daily_raw 有数据但 adj_factor 无记录）
    #    按股票分组前向填充（ffill），再后向填充（bfill）
    missing_count = df["qfq_factor"].isna().sum()
    if missing_count > 0:
        logger.warning(
            f"load_daily_pv: {missing_count} 条记录缺少复权因子，"
            f"将按股票分组前向/后向填充。"
            f"（通常是 adj_factor 表最新日期滞后于 kline_daily_raw）"
        )
        df = df.sort_values(["ts_code", "trade_date"])
        df["qfq_factor"] = (
            df.groupby("ts_code")["qfq_factor"]
            .transform(lambda s: s.ffill().bfill())
        )

    # 填充后仍有缺失则报错（说明该股票完全没有复权因子数据）
    still_missing = df["qfq_factor"].isna()
    if still_missing.any():
        missing_stocks = df.loc[still_missing, "ts_code"].unique().tolist()
        raise RuntimeError(
            f"以下股票在 adj_factor 表中完全没有复权因子数据，无法计算前复权价格: "
            f"{missing_stocks[:10]}。请检查 market.adj_factor 表。"
        )

    # 6. 计算前复权价格（与 db_reader.py 第1288行完全一致）
    #    price_qfq = raw_li / 1000 * qfq_factor
    qfq = df["qfq_factor"].astype("float64")
    df["open"]   = df["open_li"].astype("float64")   / PRICE_UNIT_DIVISOR * qfq
    df["high"]   = df["high_li"].astype("float64")   / PRICE_UNIT_DIVISOR * qfq
    df["low"]    = df["low_li"].astype("float64")    / PRICE_UNIT_DIVISOR * qfq
    df["close"]  = df["close_li"].astype("float64")  / PRICE_UNIT_DIVISOR * qfq

    # 成交量：手->股（×100），再按前复权因子反向调整（÷qfq_factor）
    df["volume"] = df["volume_hand"].astype("float64") * 100.0 / qfq

    # 成交额：仅单位转换（厘->元），不复权
    df["amount"] = df["amount_li"].astype("float64") / PRICE_UNIT_DIVISOR

    # 前复权因子（最新日 = 1.0）
    df["factor"] = qfq

    df["datetime"]   = pd.to_datetime(df["trade_date"])
    df["instrument"] = df["ts_code"].apply(_normalize_instrument)
    df = df.set_index(["datetime", "instrument"])
    result = df[["open", "high", "low", "close", "volume", "amount", "factor"]].sort_index()
    _CACHE.set("load_daily_pv", ts_codes, start.isoformat(), end.isoformat(), result)
    logger.debug(f"load_daily_pv: 已缓存 shape={result.shape}")
    return result


def load_daily_basic(
    instruments: List[str],
    start_date: Union[str, date],
    end_date: Union[str, date],
) -> pd.DataFrame:
    """
    加载 daily_basic 数据，输出 db_* 前缀字段。
    数据源：market.daily_basic
    """
    start = _to_date(start_date)
    end = _to_date(end_date)
    ts_codes = [_normalize_instrument(i) for i in instruments]

    cached = _CACHE.get("load_daily_basic", ts_codes, start.isoformat(), end.isoformat())
    if cached is not None:
        logger.debug(f"load_daily_basic: 缓存命中")
        return cached

    placeholders = ",".join(["%s"] * len(ts_codes))
    sql = f"""
        SELECT trade_date, ts_code,
               close, turnover_rate, turnover_rate_f, volume_ratio,
               pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm,
               total_share, float_share, free_share, total_mv, circ_mv
        FROM market.daily_basic
        WHERE ts_code IN ({placeholders})
          AND trade_date >= %s AND trade_date <= %s
        ORDER BY trade_date, ts_code
    """
    params = ts_codes + [start.isoformat(), end.isoformat()]

    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return pd.DataFrame()

    df = df.rename(columns=DAILY_BASIC_FIELD_MAP)
    result = _build_multiindex_df(df, "db_")
    _CACHE.set("load_daily_basic", ts_codes, start.isoformat(), end.isoformat(), result)
    return result


def load_moneyflow(
    instruments: List[str],
    start_date: Union[str, date],
    end_date: Union[str, date],
) -> pd.DataFrame:
    """
    加载资金流向原始数据，输出 mf_* 前缀字段。
    数据源：market.moneyflow_ts
    字段映射：buy_sm_vol -> mf_sm_buy_vol, buy_lg_amount -> mf_lg_buy_amt 等
    """
    start = _to_date(start_date)
    end = _to_date(end_date)
    ts_codes = [_normalize_instrument(i) for i in instruments]

    cached = _CACHE.get("load_moneyflow", ts_codes, start.isoformat(), end.isoformat())
    if cached is not None:
        logger.debug(f"load_moneyflow: 缓存命中")
        return cached

    placeholders = ",".join(["%s"] * len(ts_codes))
    sql = f"""
        SELECT trade_date, ts_code,
               buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount,
               buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount,
               buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount,
               buy_elg_vol, buy_elg_amount, sell_elg_vol, sell_elg_amount,
               net_mf_vol, net_mf_amount
        FROM market.moneyflow_ts
        WHERE ts_code IN ({placeholders})
          AND trade_date >= %s AND trade_date <= %s
        ORDER BY trade_date, ts_code
    """
    params = ts_codes + [start.isoformat(), end.isoformat()]

    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return pd.DataFrame()

    df = df.rename(columns=MONEYFLOW_FIELD_MAP)
    result = _build_multiindex_df(df, "mf_")
    _CACHE.set("load_moneyflow", ts_codes, start.isoformat(), end.isoformat(), result)
    return result


def load_bak_basic(
    instruments: List[str],
    start_date: Union[str, date],
    end_date: Union[str, date],
) -> pd.DataFrame:
    """
    加载历史基本面数据，输出 bb_* 前缀字段。
    数据源：market.bak_basic

    关键处理（与 generate_static_factors_bundle.py 一致）：
    1. 排除股票上市日期前的数据（JOIN stock_basic.list_date）
    2. 排除停牌期间的数据（INNER JOIN kline_daily_raw，只保留有行情的交易日）
    """
    start = _to_date(start_date)
    end = _to_date(end_date)
    ts_codes = [_normalize_instrument(i) for i in instruments]

    cached = _CACHE.get("load_bak_basic", ts_codes, start.isoformat(), end.isoformat())
    if cached is not None:
        logger.debug(f"load_bak_basic: 缓存命中")
        return cached

    placeholders = ",".join(["%s"] * len(ts_codes))
    sql = f"""
        SELECT b.trade_date, b.ts_code,
               b.pe_dyn, b.total_assets, b.liquid_assets, b.fixed_assets,
               b.reserved, b.reserved_pershare, b.eps, b.bvps,
               b.undp, b.per_undp, b.rev_yoy, b.profit_yoy,
               b.gpr, b.npr, b.holder_num
        FROM market.bak_basic b
        INNER JOIN market.stock_basic s ON b.ts_code = s.ts_code
        WHERE b.ts_code IN ({placeholders})
          AND b.trade_date >= %s AND b.trade_date <= %s
          AND b.trade_date >= s.list_date + INTERVAL '365 days'
        ORDER BY b.trade_date, b.ts_code
    """
    params = ts_codes + [start.isoformat(), end.isoformat()]

    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return pd.DataFrame()

    df = df.rename(columns=BAK_BASIC_FIELD_MAP)
    result = _build_multiindex_df(df, "bb_")
    _CACHE.set("load_bak_basic", ts_codes, start.isoformat(), end.isoformat(), result)
    return result


def load_cyq_perf(
    instruments: List[str],
    start_date: Union[str, date],
    end_date: Union[str, date],
) -> pd.DataFrame:
    """
    加载筹码分布数据，输出 cp_* 前缀字段。
    数据源：market.cyq_perf
    """
    start = _to_date(start_date)
    end = _to_date(end_date)
    ts_codes = [_normalize_instrument(i) for i in instruments]

    cached = _CACHE.get("load_cyq_perf", ts_codes, start.isoformat(), end.isoformat())
    if cached is not None:
        logger.debug(f"load_cyq_perf: 缓存命中")
        return cached

    placeholders = ",".join(["%s"] * len(ts_codes))
    sql = f"""
        SELECT trade_date, ts_code,
               his_low, his_high, cost_5pct, cost_15pct, cost_50pct,
               cost_85pct, cost_95pct, weight_avg, winner_rate
        FROM market.cyq_perf
        WHERE ts_code IN ({placeholders})
          AND trade_date >= %s AND trade_date <= %s
        ORDER BY trade_date, ts_code
    """
    params = ts_codes + [start.isoformat(), end.isoformat()]

    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return pd.DataFrame()

    df = df.rename(columns=CYQ_PERF_FIELD_MAP)
    result = _build_multiindex_df(df, "cp_")
    _CACHE.set("load_cyq_perf", ts_codes, start.isoformat(), end.isoformat(), result)
    return result


def load_sector_data(
    instruments: List[str],
    start_date: Union[str, date],
    end_date: Union[str, date],
) -> pd.DataFrame:
    """
    加载申万 L2 行业板块数据（展开到个股级别），输出 sw2_* 前缀字段。
    数据源：market.sector_data
    """
    start = _to_date(start_date)
    end = _to_date(end_date)
    ts_codes = [_normalize_instrument(i) for i in instruments]

    cached = _CACHE.get("load_sector_data", ts_codes, start.isoformat(), end.isoformat())
    if cached is not None:
        logger.debug("load_sector_data: 缓存命中")
        return cached

    placeholders = ",".join(["%s"] * len(ts_codes))
    col_list = ", ".join(SECTOR_DATA_COLUMNS)
    sql = f"""
        SELECT trade_date, ts_code, {col_list}
        FROM market.sector_data
        WHERE ts_code IN ({placeholders})
          AND trade_date >= %s AND trade_date <= %s
        ORDER BY trade_date, ts_code
    """
    params = ts_codes + [start.isoformat(), end.isoformat()]

    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return pd.DataFrame()

    # 列已经是 sw2_* 前缀，无需重命名
    result = _build_multiindex_df(df, "sw2_")
    _CACHE.set("load_sector_data", ts_codes, start.isoformat(), end.isoformat(), result)
    return result


def load_margin_detail(
    instruments: List[str],
    start_date: Union[str, date],
    end_date: Union[str, date],
) -> pd.DataFrame:
    """
    加载融资融券数据，输出 md_* 前缀字段。
    数据源：market.margin_detail
    """
    start = _to_date(start_date)
    end = _to_date(end_date)
    ts_codes = [_normalize_instrument(i) for i in instruments]

    cached = _CACHE.get("load_margin_detail", ts_codes, start.isoformat(), end.isoformat())
    if cached is not None:
        logger.debug("load_margin_detail: 缓存命中")
        return cached

    placeholders = ",".join(["%s"] * len(ts_codes))
    sql = f"""
        SELECT trade_date, ts_code,
               rzye, rqye, rzmre, rqyl, rzche, rqchl, rqmcl, rzrqye
        FROM market.margin_detail
        WHERE ts_code IN ({placeholders})
          AND trade_date >= %s AND trade_date <= %s
        ORDER BY trade_date, ts_code
    """
    params = ts_codes + [start.isoformat(), end.isoformat()]

    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return pd.DataFrame()

    df = df.rename(columns=MARGIN_DETAIL_FIELD_MAP)
    result = _build_multiindex_df(df, "md_")
    _CACHE.set("load_margin_detail", ts_codes, start.isoformat(), end.isoformat(), result)
    return result


# ============================================================
# 预计算因子生成（与 generate_static_factors_bundle.py 逻辑完全一致）
# ============================================================

def compute_moneyflow_derived_factors(
    df_mf: pd.DataFrame,
    df_pv: pd.DataFrame,
) -> pd.DataFrame:
    """
    从原始资金流向数据派生 mf_* 因子，逻辑与 generate_static_factors_bundle.py
    的 _derive_moneyflow_features() 完全一致。

    对应 factors/moneyflow_factors/result.pkl 中的14个字段，
    并额外计算5日/20日滚动窗口因子（generate_static_factors_bundle.py 中也有）。

    Args:
        df_mf: load_moneyflow() 返回的 DataFrame（含 mf_sm_buy_vol 等原始字段）
        df_pv: load_daily_pv() 返回的 DataFrame（需要 amount 和 volume 列）

    Returns:
        MultiIndex(datetime, instrument) DataFrame，包含所有派生 mf_* 因子
    """
    if df_mf is None or df_mf.empty:
        return pd.DataFrame()

    required_raw = [
        "mf_sm_buy_amt", "mf_sm_sell_amt",
        "mf_md_buy_amt", "mf_md_sell_amt",
        "mf_lg_buy_amt", "mf_lg_sell_amt",
        "mf_elg_buy_amt", "mf_elg_sell_amt",
        "mf_sm_buy_vol", "mf_sm_sell_vol",
        "mf_md_buy_vol", "mf_md_sell_vol",
        "mf_lg_buy_vol", "mf_lg_sell_vol",
        "mf_elg_buy_vol", "mf_elg_sell_vol",
    ]
    missing = [c for c in required_raw if c not in df_mf.columns]
    if missing:
        logger.warning(f"moneyflow 原始字段缺失: {missing}，跳过派生因子计算")
        return pd.DataFrame(index=df_mf.index)

    if df_pv is None or df_pv.empty or "amount" not in df_pv.columns or "volume" not in df_pv.columns:
        logger.warning("daily_pv 数据缺失 amount/volume，跳过 mf_*_ratio 派生因子")
        return pd.DataFrame(index=df_mf.index)

    df = df_mf.sort_index()
    amount = df_pv["amount"].astype("float64").reindex(df.index).mask(
        df_pv["amount"].astype("float64").reindex(df.index) == 0, np.nan
    )
    volume = df_pv["volume"].astype("float64").reindex(df.index).mask(
        df_pv["volume"].astype("float64").reindex(df.index) == 0, np.nan
    )

    buy_amt_total  = df["mf_sm_buy_amt"]  + df["mf_md_buy_amt"]  + df["mf_lg_buy_amt"]  + df["mf_elg_buy_amt"]
    sell_amt_total = df["mf_sm_sell_amt"] + df["mf_md_sell_amt"] + df["mf_lg_sell_amt"] + df["mf_elg_sell_amt"]
    buy_vol_total  = df["mf_sm_buy_vol"]  + df["mf_md_buy_vol"]  + df["mf_lg_buy_vol"]  + df["mf_elg_buy_vol"]
    sell_vol_total = df["mf_sm_sell_vol"] + df["mf_md_sell_vol"] + df["mf_lg_sell_vol"] + df["mf_elg_sell_vol"]

    main_buy_amt  = df["mf_lg_buy_amt"]  + df["mf_elg_buy_amt"]
    main_sell_amt = df["mf_lg_sell_amt"] + df["mf_elg_sell_amt"]
    main_buy_vol  = df["mf_lg_buy_vol"]  + df["mf_elg_buy_vol"]
    main_sell_vol = df["mf_lg_sell_vol"] + df["mf_elg_sell_vol"]

    total_net_amt = (buy_amt_total  - sell_amt_total).astype("float64")
    total_net_vol = (buy_vol_total  - sell_vol_total).astype("float64")
    main_net_amt  = (main_buy_amt   - main_sell_amt).astype("float64")
    main_net_vol  = (main_buy_vol   - main_sell_vol).astype("float64")
    elg_net_amt   = (df["mf_elg_buy_amt"] - df["mf_elg_sell_amt"]).astype("float64")
    elg_net_vol   = (df["mf_elg_buy_vol"] - df["mf_elg_sell_vol"]).astype("float64")

    out = pd.DataFrame(index=df.index)

    # 全档净流入
    out["mf_total_net_amt"]       = total_net_amt
    out["mf_total_net_vol"]       = total_net_vol
    out["mf_total_net_amt_ratio"] = _safe_div(total_net_amt, amount)
    out["mf_total_net_vol_ratio"] = _safe_div(total_net_vol, volume)

    # 主力净流入（大单+超大单）
    out["mf_main_net_amt"]        = main_net_amt
    out["mf_main_net_vol"]        = main_net_vol
    out["mf_main_net_amt_ratio"]  = _safe_div(main_net_amt, amount)
    out["mf_main_net_vol_ratio"]  = _safe_div(main_net_vol, volume)

    # 超大单净流入
    out["mf_elg_net_amt"]         = elg_net_amt
    out["mf_elg_net_vol"]         = elg_net_vol
    out["mf_elg_net_amt_ratio"]   = _safe_div(elg_net_amt, amount)
    out["mf_elg_net_vol_ratio"]   = _safe_div(elg_net_vol, volume)

    # 超大单占主力比
    out["mf_elg_share_in_main_amt"] = _safe_div(
        df["mf_elg_buy_amt"],
        df["mf_lg_buy_amt"] + df["mf_elg_buy_amt"],
    )
    out["mf_elg_share_in_main_vol"] = _safe_div(
        df["mf_elg_buy_vol"],
        df["mf_lg_buy_vol"] + df["mf_elg_buy_vol"],
    )

    # 滚动窗口（5日、20日）
    for w in (5, 20):
        out[f"mf_total_net_amt_{w}d"] = _rolling_sum_by_instrument(total_net_amt, w)
        out[f"mf_main_net_amt_{w}d"]  = _rolling_sum_by_instrument(main_net_amt, w)
        out[f"mf_elg_net_amt_{w}d"]   = _rolling_sum_by_instrument(elg_net_amt, w)

        amount_w = _rolling_sum_by_instrument(amount, w)
        out[f"mf_total_net_amt_ratio_{w}d"] = _safe_div(out[f"mf_total_net_amt_{w}d"], amount_w)
        out[f"mf_main_net_amt_ratio_{w}d"]  = _safe_div(out[f"mf_main_net_amt_{w}d"], amount_w)
        out[f"mf_elg_net_amt_ratio_{w}d"]   = _safe_div(out[f"mf_elg_net_amt_{w}d"], amount_w)

    return out.sort_index()


def compute_daily_basic_precomputed_factors(
    df_db: pd.DataFrame,
) -> pd.DataFrame:
    """
    从 daily_basic 数据计算预计算因子，与 factors/daily_basic_factors/result.pkl 完全一致。

    预计算因子列表（5个）：
    - value_pe_inv:       倒数市盈率 = 1/db_pe_ttm（优先）或 1/db_pe；分母为0=>NaN
    - value_pb_inv:       倒数市净率 = 1/db_pb；分母为0=>NaN
    - size_log_mv:        市值对数 = log(db_circ_mv 优先，否则 db_total_mv)；<=0=>NaN
    - liquidity_turnover: 换手率 = db_turnover_rate（直接映射）
    - liquidity_vol_ratio:量比 = db_volume_ratio（直接映射）

    Args:
        df_db: load_daily_basic() 返回的 DataFrame（含 db_* 前缀字段）

    Returns:
        MultiIndex(datetime, instrument) DataFrame，包含5个预计算因子
    """
    if df_db is None or df_db.empty:
        return pd.DataFrame()

    out = pd.DataFrame(index=df_db.index)

    # value_pe_inv: 优先用 pe_ttm，其次用 pe
    if "db_pe_ttm" in df_db.columns:
        pe_series = df_db["db_pe_ttm"].astype("float64")
    elif "db_pe" in df_db.columns:
        pe_series = df_db["db_pe"].astype("float64")
    else:
        pe_series = pd.Series(np.nan, index=df_db.index, dtype="float64")

    out["value_pe_inv"] = _safe_div(
        pd.Series(1.0, index=df_db.index, dtype="float64"),
        pe_series,
    )

    # value_pb_inv
    if "db_pb" in df_db.columns:
        out["value_pb_inv"] = _safe_div(
            pd.Series(1.0, index=df_db.index, dtype="float64"),
            df_db["db_pb"].astype("float64"),
        )
    else:
        out["value_pb_inv"] = np.nan

    # size_log_mv: 优先 circ_mv，其次 total_mv
    if "db_circ_mv" in df_db.columns:
        mv_series = df_db["db_circ_mv"].astype("float64")
    elif "db_total_mv" in df_db.columns:
        mv_series = df_db["db_total_mv"].astype("float64")
    else:
        mv_series = pd.Series(np.nan, index=df_db.index, dtype="float64")

    out["size_log_mv"] = np.where(mv_series > 0, np.log(mv_series), np.nan).astype("float32")

    # liquidity_turnover
    if "db_turnover_rate" in df_db.columns:
        out["liquidity_turnover"] = df_db["db_turnover_rate"].astype("float32")
    else:
        out["liquidity_turnover"] = np.nan

    # liquidity_vol_ratio
    if "db_volume_ratio" in df_db.columns:
        out["liquidity_vol_ratio"] = df_db["db_volume_ratio"].astype("float32")
    else:
        out["liquidity_vol_ratio"] = np.nan

    return out.sort_index()


def _asof_fill_static_prefixes_to_target_index(
    df: pd.DataFrame,
    target_index: pd.MultiIndex,
    *,
    prefixes: Iterable[str] = LIVE_ASOF_STATIC_PREFIXES,
    columns: Iterable[str] = LIVE_ASOF_STATIC_COLUMNS,
) -> pd.DataFrame:
    """Fill slow PIT datasets onto the live daily-bar index from DB rows only."""
    if df is None or df.empty or not isinstance(df.index, pd.MultiIndex):
        return df
    if not isinstance(target_index, pd.MultiIndex) or target_index.empty:
        return df

    fill_prefixes = tuple(prefixes)
    fill_columns = {str(col) for col in columns}
    fill_cols = [
        col
        for col in df.columns
        if str(col) in fill_columns or any(str(col).startswith(prefix) for prefix in fill_prefixes)
    ]
    if not fill_cols:
        return df.reindex(target_index)

    target_index = target_index.drop_duplicates()
    union_index = df.index.union(target_index).drop_duplicates()
    filled_source = df[fill_cols].reindex(union_index)

    if "datetime" not in filled_source.index.names or "instrument" not in filled_source.index.names:
        return df.reindex(target_index)

    filled = (
        filled_source.reorder_levels(["instrument", "datetime"])
        .sort_index()
        .groupby(level="instrument", group_keys=False)
        .ffill()
        .reorder_levels(["datetime", "instrument"])
        .sort_index()
        .reindex(target_index)
    )
    result = df.reindex(target_index)
    result.loc[:, fill_cols] = filled
    return result.sort_index()


def build_static_factors(
    instruments: List[str],
    start_date: Union[str, date],
    end_date: Union[str, date],
    *,
    asof_fill_slow_static: bool = False,
) -> pd.DataFrame:
    """
    构建完整的 static_factors 数据集，与 generate_static_factors_bundle.py 生成的
    static_factors.parquet 完全一致（内存版，无文件 I/O）。

    合并顺序（与 generate_static_factors_bundle.py main() 完全一致）：
    1. df_db_raw       - daily_basic 原始字段（db_* 前缀）
    2. df_mf_raw       - moneyflow 原始字段（mf_* 前缀）
    3. df_bb_raw       - bak_basic 原始字段（bb_* 前缀，已过滤上市前+停牌）
    4. df_cp_raw       - cyq_perf 原始字段（cp_* 前缀）
    5. df_sd_raw       - sector_data 原始字段（sw2_* 前缀，申万 L2 行业板块）
    6. df_mf_derived   - moneyflow 派生因子（mf_total_net_amt 等）
    7. df_db_precomp   - daily_basic 预计算因子（value_pe_inv 等5个）
    8. df_price_momentum - 价格动量因子（PriceStrength_10D）

    注意：ae_recon_error_10d 需要预训练模型，不在此处计算（该因子未出现在任何 RDAgent 工作区 schema 中）。

    Args:
        instruments: 股票代码列表（'000001.SZ' 格式）
        start_date:  开始日期
        end_date:    结束日期

    Returns:
        MultiIndex(datetime, instrument) DataFrame，包含所有 static_factors 字段
    """
    logger.info(f"build_static_factors: {len(instruments)} instruments, {start_date} ~ {end_date}")

    # 1. 加载各原始数据表
    df_db_raw = load_daily_basic(instruments, start_date, end_date)
    df_mf_raw = load_moneyflow(instruments, start_date, end_date)
    df_bb_raw = load_bak_basic(instruments, start_date, end_date)
    df_cp_raw = load_cyq_perf(instruments, start_date, end_date)
    df_sd_raw = load_sector_data(instruments, start_date, end_date)
    df_md_raw = load_margin_detail(instruments, start_date, end_date)
    df_pv     = load_daily_pv(instruments, start_date, end_date)

    # 2. 计算派生因子
    df_mf_derived  = compute_moneyflow_derived_factors(df_mf_raw, df_pv)
    df_db_precomp  = compute_daily_basic_precomputed_factors(df_db_raw)

    # 2.5 计算 PriceStrength_10D（10日价格动量）
    df_price_momentum = pd.DataFrame()
    if df_pv is not None and not df_pv.empty and "close" in df_pv.columns:
        df_price_momentum = pd.DataFrame(index=df_pv.index)
        df_price_momentum["PriceStrength_10D"] = (
            df_pv["close"].groupby(level="instrument").pct_change(10)
        )
        logger.debug(f"  PriceStrength_10D: {df_price_momentum.shape}")

    # 3. 合并（与 generate_static_factors_bundle.py 的 left join 逻辑一致）
    dfs: List[pd.DataFrame] = []
    for name, df in [
        ("daily_basic_raw",  df_db_raw),
        ("moneyflow_raw",    df_mf_raw),
        ("bak_basic_raw",    df_bb_raw),
        ("cyq_perf_raw",     df_cp_raw),
        ("sector_data_raw",  df_sd_raw),
        ("margin_detail_raw", df_md_raw),
        ("mf_derived",       df_mf_derived),
        ("db_precomputed",   df_db_precomp),
        ("price_momentum",   df_price_momentum),
    ]:
        if df is not None and not df.empty:
            dfs.append(df)
            logger.debug(f"  {name}: {df.shape}")
        else:
            logger.warning(f"  {name}: empty, skipped")

    if not dfs:
        logger.error("build_static_factors: 所有数据表均为空")
        return pd.DataFrame()

    df_merged = dfs[0].sort_index()
    for df_next in dfs[1:]:
        df_next = df_next.sort_index()
        overlap = df_merged.columns.intersection(df_next.columns)
        if len(overlap) > 0:
            df_next = df_next.drop(columns=overlap)
        if df_next.empty:
            continue
        df_merged = df_merged.join(df_next, how="left")

    if asof_fill_slow_static and df_pv is not None and not df_pv.empty and isinstance(df_pv.index, pd.MultiIndex):
        df_merged = _asof_fill_static_prefixes_to_target_index(df_merged, df_pv.index)

    df_merged = df_merged.sort_index()
    logger.info(f"build_static_factors: 完成，shape={df_merged.shape}")
    return df_merged


def get_instruments_universe(
    exchanges: List[str] = None,
    exclude_st: bool = False,
    as_of_date: str | None = None,
) -> List[str]:
    """
    从数据库获取股票池（不硬编码）。

    Args:
        exchanges: 交易所列表，支持两种格式：
                   - Tushare 格式：['SSE', 'SZSE', 'BSE']（数据库实际存储值）
                   - 简写格式：['SH', 'SZ', 'BJ']（自动映射到 SSE/SZSE/BSE）
                   None 表示全部
        exclude_st: 是否排除 ST 股票
        as_of_date: 历史时点 (YYYY-MM-DD)；为空则使用当前日期

    Returns:
        股票代码列表（'000001.SZ' 格式）
    """
    # 简写 -> 数据库实际值映射
    _EXCHANGE_MAP = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}

    conditions = ["list_status = 'L'"]
    params: list = []
    as_of_sql = "%s"
    as_of_value = as_of_date or datetime.now().date().isoformat()
    # 上市满1年才纳入股票池（与 db_reader/snapshot_writer 一致）
    conditions.append(f"list_date + INTERVAL '365 days' <= {as_of_sql}")
    params.append(as_of_value)

    if exchanges:
        normalized = [_EXCHANGE_MAP.get(e.upper(), e.upper()) for e in exchanges]
        placeholders = ",".join(["%s"] * len(normalized))
        conditions.append(f"exchange IN ({placeholders})")
        params.extend(normalized)

    where_clause = " AND ".join(conditions)
    sql = f"SELECT ts_code, name FROM market.stock_basic WHERE {where_clause} ORDER BY ts_code"

    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=params or None)

    if df.empty:
        return []

    if exclude_st:
        st_mask = df["name"].str.contains("ST|退", na=False)
        df = df[~st_mask]

    return df["ts_code"].tolist()
