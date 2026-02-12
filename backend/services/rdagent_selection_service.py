from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import Any, Dict, List, Optional, Tuple

from ..data_service.api import DataSourceError, get_history_window, get_realtime_snapshot
from ..db.pg_pool import get_conn
from ..inference_engine import InferenceEngine
from ..data_service import timescaledb_adapter
from ..core.data_source_manager_impl import data_source_manager

logger = logging.getLogger("aistock.rdagent_selection")


@dataclass
class SelectionRow:
    symbol: str
    name: Optional[str]
    price: Optional[float]
    pct_change: Optional[float]
    score: Optional[float]
    rank: Optional[int]
    quote_source: str
    quote_time: Optional[str]


def _deterministic_strategy_id(task_run_id: str, loop_id: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"rdagent_loop:{task_run_id}:{loop_id}"))


def _get_latest_trade_date(target_dt: datetime) -> datetime:
    sql = (
        "SELECT cal_date FROM market.trading_calendar "
        "WHERE cal_date <= %s AND is_trading = TRUE "
        "ORDER BY cal_date DESC LIMIT 1"
    )
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (target_dt.date(),))
            row = cur.fetchone()
    if row and row[0]:
        return datetime.combine(row[0], datetime.min.time())
    return target_dt


def _is_trading_day(d: datetime) -> bool:
    sql = "SELECT 1 FROM market.trading_calendar WHERE cal_date = %s AND is_trading = TRUE LIMIT 1"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (d.date(),))
            row = cur.fetchone()
    return bool(row)


def _is_market_open(now: datetime) -> bool:
    if not _is_trading_day(now):
        return False
    t = now.time()
    am_open = dtime(9, 30) <= t <= dtime(11, 30)
    pm_open = dtime(13, 0) <= t <= dtime(15, 0)
    return am_open or pm_open


def _load_topk_signals(*, strategy_id: str, version_tag: str, trade_date: datetime, k: int) -> List[Dict[str, Any]]:
    # 修复：移除对 strategy_version_id 的子查询依赖，直接通过 strategy_id 和 trade_date 查询最新信号
    # 因为推理引擎写入时 version_tag 固定为 'replay'
    sql = """
        SELECT symbol, rank, score
        FROM trading.rdagent_signal
        WHERE strategy_id = %s
          AND trade_date = %s
        ORDER BY
          CASE WHEN rank IS NULL THEN 1 ELSE 0 END,
          rank ASC NULLS LAST,
          score DESC NULLS LAST
        LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (strategy_id, trade_date.date(), k))
            rows = cur.fetchall()
    
    if not rows:
        # 兜底：如果 trade_date 严格匹配不到，查询该策略最近一天的信号
        sql_fallback = """
            SELECT symbol, rank, score, trade_date
            FROM trading.rdagent_signal
            WHERE strategy_id = %s
            ORDER BY trade_date DESC, rank ASC NULLS LAST, score DESC NULLS LAST
            LIMIT %s
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_fallback, (strategy_id, k))
                rows = cur.fetchall()
                if rows:
                    logger.info(f"Fallback to signals from trade_date: {rows[0][3]}")

    return [
        {"symbol": r[0], "rank": r[1], "score": r[2]}
        for r in rows
    ]


def _tdx_name(symbol: str) -> Optional[str]:
    base = data_source_manager._convert_from_ts_code(symbol) if "." in str(symbol) else str(symbol)
    base = (base or "").strip()
    if len(base) != 6:
        return None
    try:
        nm = data_source_manager._search_name_via_tdx(base)
        return (nm or None)
    except Exception:
        return None


def _fallback_name(symbol: str) -> Optional[str]:
    try:
        info = data_source_manager.get_stock_basic_info(symbol)
        if isinstance(info, dict):
            nm = info.get("name")
            if isinstance(nm, str) and nm.strip():
                return nm.strip()
    except Exception:
        return None
    return None


def _compute_pct(close: Optional[float], pre_close: Optional[float]) -> Optional[float]:
    if close is None or pre_close in (None, 0):
        return None
    try:
        return (float(close) - float(pre_close)) / float(pre_close) * 100.0
    except Exception:
        return None


def _get_recent_trade_day_quote(universe: List[str], as_of: datetime) -> Tuple[Dict[str, float], Dict[str, Optional[float]]]:
    # 修复：移除对 get_history_window 的硬性依赖，避免由于 TimescaleDB 历史数据不全导致的 500 错误
    # 这里我们只尝试获取行情，如果失败，返回空字典，外层会处理为 None
    close_map: Dict[str, float] = {}
    pct_map: Dict[str, Optional[float]] = {}
    
    try:
        end = _get_latest_trade_date(as_of)
        df = get_history_window(universe, end=end, bars=2, fields=["close"], freq="1d", adj="front")
        if df is None or df.empty:
            return {}, {}

        for inst in universe:
            try:
                s = df.xs(inst, level="instrument")["close"].dropna()
            except Exception:
                continue
            if s.empty:
                continue
            last_close = float(s.iloc[-1])
            close_map[inst] = last_close
            pre_close = float(s.iloc[-2]) if len(s) >= 2 else None
            pct_map[inst] = _compute_pct(last_close, pre_close)
    except Exception as e:
        logger.warning(f"获取历史行情失败 (非致命): {e}")

    return close_map, pct_map


def build_loop_selection(
    *,
    task_run_id: str,
    loop_id: int,
    trade_date: Optional[str] = None,
    cutoff_date: Optional[str] = None,
    top_k: int = 50,
) -> Dict[str, Any]:
    t0 = datetime.now()
    now = datetime.now()
    t_date = now
    if trade_date:
        try:
            t_date = datetime.strptime(trade_date, "%Y-%m-%d")
        except Exception:
            raise ValueError(f"trade_date 格式错误: {trade_date}，要求 YYYY-MM-DD")

    c_date: Optional[datetime] = None
    if cutoff_date:
        try:
            c_date = datetime.strptime(cutoff_date, "%Y-%m-%d")
        except Exception:
            raise ValueError(f"cutoff_date 格式错误: {cutoff_date}，要求 YYYY-MM-DD")

    effective_date = t_date
    if c_date is not None and c_date.date() < t_date.date():
        effective_date = c_date

    strict_inf = (os.environ.get("AISTOCK_STRICT_INFERENCE") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    if strict_inf and trade_date:
        # 严格模式：请求的 trade_date 必须是交易日，不允许把周末/节假日隐式映射到上一交易日。
        if not _is_trading_day(t_date):
            raise ValueError(f"trade_date={t_date.date()} 非交易日，严格模式下禁止回退/归一到上一交易日")

    logger.info(
        "loop_selection_start"
        f" task_run_id={task_run_id}"
        f" loop_id={loop_id}"
        f" trade_date={t_date.date()}"
        f" cutoff_date={c_date.date() if c_date else None}"
        f" top_k={top_k}"
    )

    engine = InferenceEngine()
    logger.info(
        "loop_selection_infer_call"
        f" task_run_id={task_run_id}"
        f" loop_id={loop_id}"
        f" trade_date={effective_date.date()}"
        f" cutoff_date={c_date.date() if c_date else None}"
    )
    engine.run_inference(
        strategy_id="",
        version_tag="replay",
        trade_date=effective_date,
        task_run_id=task_run_id,
        loop_id=loop_id,
        cutoff_date=c_date,
    )
    logger.info(
        "loop_selection_infer_done"
        f" task_run_id={task_run_id}"
        f" loop_id={loop_id}"
        f" trade_date={effective_date.date()}"
        f" cutoff_date={c_date.date() if c_date else None}"
        f" elapsed_ms={int((datetime.now() - t0).total_seconds() * 1000)}"
    )

    inference_meta: Dict[str, Any] = {}
    try:
        last_assets = getattr(engine, "_last_assets", None)
        if isinstance(last_assets, dict):
            inference_meta["asset_bundle_id"] = last_assets.get("asset_bundle_id")
            inference_meta["freq"] = last_assets.get("freq")
            inference_meta["source"] = getattr(engine, "_last_source_desc", None)
            inference_meta["use_config_universe"] = bool(
                str(os.environ.get("AISTOCK_USE_CONFIG_UNIVERSE", "")).strip().lower() in {"1", "true", "yes", "y", "on"}
            )
            uni = last_assets.get("universe_spec")
            if isinstance(uni, (list, tuple)):
                ulist = [str(x) for x in uni if x is not None]
                inference_meta["universe_size"] = len(ulist)
                inference_meta["universe_preview"] = ulist[:50]
            else:
                inference_meta["universe_size"] = None
                inference_meta["universe_preview"] = []
    except Exception:
        inference_meta = {}

    # 强制补齐 UI 需要的关键元信息
    inference_meta["top_k"] = int(top_k)
    inference_meta["cutoff_date"] = c_date.strftime("%Y-%m-%d") if c_date is not None else None
    inference_meta["effective_trade_date"] = effective_date.strftime("%Y-%m-%d")

    # 获取最新的市场数据日期，作为行情查询的基准
    try:
        latest_dates = timescaledb_adapter.fetch_latest_market_dates_ts()
        inference_meta["data_dates"] = latest_dates
        # 行情基准日期取 K 线数据的最新日期
        kline_date_str = latest_dates.get("kline_daily_raw")
        if kline_date_str:
            as_of = datetime.strptime(kline_date_str, "%Y-%m-%d")
        else:
            as_of = _get_latest_trade_date(effective_date)
    except Exception:
        inference_meta["data_dates"] = {}
        as_of = _get_latest_trade_date(effective_date)
    inference_meta["as_of"] = as_of.strftime("%Y-%m-%d")

    if strict_inf and trade_date:
        # 严格模式：即便 trade_date 是交易日，也不允许由于日历/库内缺口导致 as_of 变成更早日期。
        if as_of.date() != t_date.date():
            raise ValueError(
                "严格模式下禁止将 trade_date 归一到更早交易日。"
                f" trade_date={t_date.date()} as_of={as_of.date()}"
            )

    if strict_inf and c_date is not None:
        # 严格模式：不允许 as_of 晚于 cutoff_date
        if as_of.date() > c_date.date():
            raise ValueError(
                "严格模式下禁止使用 cutoff_date 之后的数据。"
                f" cutoff_date={c_date.date()} as_of={as_of.date()}"
            )
    if strict_inf:
        dd = inference_meta.get("data_dates") or {}
        lagging = []
        for k in ("kline_daily_raw", "daily_basic", "moneyflow_ts"):
            v = dd.get(k)
            if not v:
                lagging.append(f"{k}=NULL")
                continue
            try:
                if str(v) < as_of.strftime("%Y-%m-%d"):
                    lagging.append(f"{k}={v}")
            except Exception:
                lagging.append(f"{k}={v}")
        if lagging:
            raise ValueError(
                "关键数据未入库到推理日期，已按严格模式中止选股。"
                f" as_of={as_of.strftime('%Y-%m-%d')} lagging={lagging}"
            )
    strategy_id = _deterministic_strategy_id(task_run_id, loop_id)
    t1 = datetime.now()
    rows = _load_topk_signals(strategy_id=strategy_id, version_tag="replay", trade_date=as_of, k=top_k)
    logger.info(
        "loop_selection_topk_loaded"
        f" as_of={as_of.date()}"
        f" k={top_k}"
        f" rows={len(rows)}"
        f" elapsed_ms={int((datetime.now() - t1).total_seconds() * 1000)}"
    )
    symbols = [r["symbol"] for r in rows if r.get("symbol")]

    name_map: Dict[str, Optional[str]] = {}
    t2 = datetime.now()
    for s in symbols:
        nm = _tdx_name(s)
        if not nm:
            nm = _fallback_name(s)
        name_map[s] = nm
    logger.info(
        "loop_selection_names_done"
        f" symbols={len(symbols)}"
        f" elapsed_ms={int((datetime.now() - t2).total_seconds() * 1000)}"
    )

    force_realtime = (os.environ.get("AISTOCK_FORCE_REALTIME_QUOTE") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    market_open = _is_market_open(now)
    realtime_allowed = bool(force_realtime or market_open)
    logger.info(
        "loop_selection_realtime_gate"
        f" now={now.isoformat(timespec='seconds')}"
        f" market_open={market_open}"
        f" force_realtime={force_realtime}"
        f" realtime_allowed={realtime_allowed}"
    )

    quote_source = "fallback_trade_day"
    quote_time: Optional[str] = None
    price_map: Dict[str, Optional[float]] = {s: None for s in symbols}
    pct_map: Dict[str, Optional[float]] = {s: None for s in symbols}

    hist_close_map, hist_pct_map = _get_recent_trade_day_quote(symbols, as_of)
    logger.info(
        "loop_selection_hist_quote_done"
        f" as_of={as_of.date()}"
        f" symbols={len(symbols)}"
        f" close_hit={len(hist_close_map)}"
    )

    if realtime_allowed and symbols:
        try:
            snap = get_realtime_snapshot(symbols, fields=None, freq="1d")
            if snap is not None and not snap.empty:
                quote_source = "miniqmt" if "last_close" in snap.columns else "tdx"
                if "datetime" in snap.columns:
                    try:
                        dt0 = snap["datetime"].dropna().iloc[0]
                        if isinstance(dt0, datetime):
                            quote_time = dt0.isoformat()
                        else:
                            quote_time = str(dt0)
                    except Exception as e:
                        logger.error(f"获取实时快照时间失败: {e}")
                        import traceback
                        logger.error(f"错误堆栈:\n{traceback.format_exc()}")
                        quote_time = None

                for s in symbols:
                    if s not in snap.index:
                        continue
                    close = snap.loc[s].get("close")
                    last_close = snap.loc[s].get("last_close") if "last_close" in snap.columns else None
                    price = float(close) if close is not None else None
                    price_map[s] = price
                    if last_close is None:
                        last_close = hist_close_map.get(s)
                    pct_map[s] = _compute_pct(price, float(last_close) if last_close is not None else None)
        except DataSourceError:
            quote_source = "fallback_trade_day"
        except Exception:
            quote_source = "fallback_trade_day"

    if quote_source == "fallback_trade_day":
        for s in symbols:
            price_map[s] = float(hist_close_map.get(s)) if s in hist_close_map else None
            pct_map[s] = hist_pct_map.get(s)

    out_rows: List[Dict[str, Any]] = []
    for r in rows:
        s = r["symbol"]
        out_rows.append(
            {
                "symbol": s,
                "name": name_map.get(s),
                "price": price_map.get(s),
                "pct_change": pct_map.get(s),
                "score": float(r["score"]) if r.get("score") is not None else None,
                "rank": int(r["rank"]) if r.get("rank") is not None else None,
                "quote_source": quote_source,
                "quote_time": quote_time,
            }
        )

    payload = {
        "task_run_id": task_run_id,
        "loop_id": loop_id,
        "as_of": as_of.strftime("%Y-%m-%d"),
        "cutoff_date": c_date.strftime("%Y-%m-%d") if c_date is not None else None,
        "effective_trade_date": effective_date.strftime("%Y-%m-%d"),
        "top_k": top_k,
        "items": out_rows,
        "inference_meta": inference_meta,
    }

    logger.info(
        "loop_selection_quote_done"
        f" realtime_allowed={realtime_allowed}"
        f" quote_source={quote_source}"
        f" symbols={len(symbols)}"
    )

    logger.info(
        "loop_selection_done"
        f" as_of={as_of.date()}"
        f" items={len(out_rows)}"
        f" elapsed_ms={int((datetime.now() - t0).total_seconds() * 1000)}"
    )
    return payload
