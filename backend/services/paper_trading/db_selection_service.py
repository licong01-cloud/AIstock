"""DB-based selection service for paper trading.

Replaces file-based inference (daily_pv.h5 + static_factors.parquet) with
direct DB reads via RealtimeFactorDataLoader + build_static_factors.

All dynamic factors must have transformation_status='SUCCESS' in
aistock_factor_catalog before they can be used.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ...db.pg_pool import get_conn
from ...inference_engine import (
    InferenceEngine,
    _safe_get_datetime_level,
    assemble_features,
    load_model_from_pkl,
    predict_scores,
    save_signals_to_db,
)

logger = logging.getLogger("aistock.paper_trading.db_selection")


# ============================================================
# Helper: trade date normalisation
# ============================================================

def _get_latest_trade_date(target: str) -> date:
    """Normalise *target* (YYYY-MM-DD) to the most recent trading day <= target."""
    sql = (
        "SELECT cal_date FROM market.trading_calendar "
        "WHERE cal_date <= %s AND is_trading = TRUE "
        "ORDER BY cal_date DESC LIMIT 1"
    )
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (target,))
            row = cur.fetchone()
    if row and row[0]:
        return row[0] if isinstance(row[0], date) else datetime.strptime(str(row[0]), "%Y-%m-%d").date()
    return datetime.strptime(target, "%Y-%m-%d").date()


# ============================================================
# Helper: factor catalogue validation
# ============================================================

def _validate_dynamic_factors(factor_names: List[str]) -> None:
    """Raise RuntimeError if any factor has not been successfully transformed."""
    if not factor_names:
        return

    placeholders = ",".join(["%s"] * len(factor_names))
    sql = f"""
        SELECT factor_name, transformation_status, qe_code_path
        FROM aistock_factor_catalog
        WHERE factor_name IN ({placeholders})
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, factor_names)
            rows = cur.fetchall()

    found = {r[0]: (r[1], r[2]) for r in rows}

    missing = [f for f in factor_names if f not in found]
    if missing:
        raise RuntimeError(
            f"以下动态因子在 aistock_factor_catalog 中不存在: {missing}。"
            f"必须先完成因子改造才能用于 DB 选股。"
        )

    not_success = [f for f in factor_names if found[f][0] != "SUCCESS"]
    if not_success:
        detail = {f: found[f][0] for f in not_success}
        raise RuntimeError(
            f"以下动态因子尚未改造成功: {detail}。"
            f"transformation_status 必须为 'SUCCESS'。"
        )


def _load_factor_code(factor_name: str) -> str:
    """Load the transformed factor code text from DB or file."""
    sql = """
        SELECT qe_code_path, realtime_code_text
        FROM aistock_factor_catalog
        WHERE factor_name = %s AND transformation_status = 'SUCCESS'
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (factor_name,))
            row = cur.fetchone()

    if not row:
        raise RuntimeError(f"因子 {factor_name} 不在 catalog 中或未改造成功")

    code_path, code_text = row
    if code_text:
        return code_text

    if code_path and os.path.isfile(code_path):
        with open(code_path, "r", encoding="utf-8") as f:
            return f.read()

    raise RuntimeError(
        f"因子 {factor_name} 既没有 realtime_code_text 也找不到代码文件 {code_path}"
    )


# ============================================================
# Helper: sandbox factor execution
# ============================================================

def _execute_factor(
    factor_name: str,
    code: str,
    instruments: List[str],
    start_date: str,
    end_date: str,
    loader,
    static_loader,
    timeout: int = 300,
) -> pd.DataFrame:
    """Execute a single transformed factor in a sandboxed thread."""
    exec_globals = {
        "__name__": "__db_selection__",
        "__builtins__": __builtins__,
        "pd": pd,
        "np": np,
        "_REALTIME_LOADER": loader,
        "_STATIC_FACTORS_LOADER": static_loader,
    }

    container: Dict[str, Any] = {"result": None, "error": None}

    def _run():
        try:
            compiled = compile(code, f"<factor_{factor_name}>", "exec")
            exec(compiled, exec_globals)

            calc_func_name = f"calculate_{factor_name}"
            if calc_func_name not in exec_globals:
                calc_funcs = [k for k in exec_globals if k.startswith("calculate_")]
                if not calc_funcs:
                    container["error"] = f"未找到 calculate_{factor_name} 函数"
                    return
                calc_func_name = calc_funcs[0]

            container["result"] = exec_globals[calc_func_name](
                instruments=instruments,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as e:
            import traceback
            container["error"] = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout=timeout)

    if thread.is_alive():
        raise RuntimeError(f"因子 {factor_name} 执行超时(>{timeout}s)")

    if container["error"]:
        raise RuntimeError(f"因子 {factor_name} 执行失败: {container['error']}")

    result = container["result"]
    if not isinstance(result, pd.DataFrame):
        raise RuntimeError(
            f"因子 {factor_name} 返回类型错误: {type(result)}，期望 pd.DataFrame"
        )

    return result


# ============================================================
# Helper: quote enrichment (reuse pattern from rdagent_selection_service)
# ============================================================

def _enrich_with_quotes(
    signals: List[Dict[str, Any]],
    actual_date: date,
) -> List[Dict[str, Any]]:
    """Add name / price / pct_change to signal rows.

    价格来源：DB kline_daily_raw 最近收盘价（单条 SQL，~20ms）。
    涨跌幅：取最近 2 个交易日的 close_li 计算。
    """
    symbols = [r["symbol"] for r in signals]
    if not symbols:
        return signals

    placeholders = ",".join(["%s"] * len(symbols))

    # 1. 股票名称
    name_map: Dict[str, Optional[str]] = {}
    sql_name = f"SELECT ts_code, name FROM market.stock_basic WHERE ts_code IN ({placeholders})"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_name, symbols)
            for row in cur.fetchall():
                name_map[row[0]] = row[1]

    # 2. 最近 2 个交易日的收盘价（用于计算涨跌幅）
    #    使用窗口函数一次查出每只股票最近 2 条 kline
    close_map: Dict[str, float] = {}
    pct_map: Dict[str, Optional[float]] = {}
    try:
        sql_prices = f"""
            WITH recent AS (
                SELECT ts_code, close_li,
                       ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                FROM market.kline_daily_raw
                WHERE ts_code IN ({placeholders})
                  AND trade_date <= %s
                  AND trade_date >= %s::date - INTERVAL '30 days'
            )
            SELECT ts_code, rn, close_li
            FROM recent
            WHERE rn <= 2
            ORDER BY ts_code, rn
        """
        # 每只股票出 2 行：rn=1 最新收盘价，rn=2 前一日收盘价
        price_data: Dict[str, Dict[int, float]] = {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                actual_str = actual_date.isoformat()
                cur.execute(sql_prices, symbols + [actual_str, actual_str])
                for row in cur.fetchall():
                    ts_code, rn, close_li = row
                    if close_li and int(close_li) > 0:
                        price_data.setdefault(ts_code, {})[rn] = float(close_li) / 1000.0

        for ts_code, prices in price_data.items():
            if 1 in prices:
                close_map[ts_code] = prices[1]
                if 2 in prices and prices[2] > 0:
                    pct_map[ts_code] = (prices[1] - prices[2]) / prices[2] * 100.0
    except Exception as e:
        logger.warning(f"获取行情失败(非致命): {e}")

    for item in signals:
        s = item["symbol"]
        item["name"] = name_map.get(s)
        item["price"] = close_map.get(s)
        item["pct_change"] = pct_map.get(s)
        item["quote_source"] = "db_kline"
        item["quote_time"] = None

    return signals


# ============================================================
# Pre-flight check: factor readiness
# ============================================================

def check_factor_readiness(
    *,
    signal_source: str,
    signal_source_id: str,
    signal_loop_id: Optional[int] = None,
) -> Dict[str, Any]:
    """检查指定 task/experiment 的因子改造状态。

    Returns
    -------
    {
        "ready": bool,
        "total_dynamic": int,
        "transformed": [...],     # 已改造成功
        "not_transformed": [...], # 未改造或改造失败
        "missing": [...],         # catalog 中不存在
        "message": str,           # 人类可读状态消息
    }
    """
    # 1. 确定 workspace
    if signal_source == "rdagent_task":
        assets_root = (Path(__file__).resolve().parents[3] / "rdagent_assets" / "rdagent_tasks").resolve()
        task_dir = assets_root / signal_source_id
        if not task_dir.exists():
            return {
                "ready": False,
                "total_dynamic": 0,
                "transformed": [],
                "not_transformed": [],
                "missing": [],
                "message": f"Task 资产目录不存在: {task_dir}",
            }
    elif signal_source in ("qe_experiment", "qe_evolution"):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT workspace_path FROM qe_experiments WHERE experiment_id = %s",
                    (signal_source_id,),
                )
                row = cur.fetchone()
        if not row or not row[0]:
            return {
                "ready": False,
                "total_dynamic": 0,
                "transformed": [],
                "not_transformed": [],
                "missing": [],
                "message": f"实验 {signal_source_id} 不存在或无 workspace",
            }
        task_dir = Path(row[0])
        if not task_dir.exists():
            return {
                "ready": False,
                "total_dynamic": 0,
                "transformed": [],
                "not_transformed": [],
                "missing": [],
                "message": f"实验工作目录不存在: {task_dir}",
            }
    else:
        return {
            "ready": False,
            "total_dynamic": 0,
            "transformed": [],
            "not_transformed": [],
            "missing": [],
            "message": f"未知信号来源: {signal_source}",
        }

    # 2. 加载 manifest → 解析 factor_order
    engine = InferenceEngine()
    try:
        if signal_source == "rdagent_task":
            manifest = engine._load_task_manifest(signal_source_id)
        else:
            manifest = engine._load_experiment_manifest(str(task_dir))
    except Exception as exc:
        return {
            "ready": False,
            "total_dynamic": 0,
            "transformed": [],
            "not_transformed": [],
            "missing": [],
            "message": f"加载 manifest 失败: {exc}",
        }

    if not manifest:
        return {
            "ready": False,
            "total_dynamic": 0,
            "transformed": [],
            "not_transformed": [],
            "missing": [],
            "message": "未找到 manifest",
        }

    try:
        _factor_order, _alpha158_feats, dynamic_feats = engine._infer_expected_features(task_dir, manifest)
    except Exception as exc:
        return {
            "ready": False,
            "total_dynamic": 0,
            "transformed": [],
            "not_transformed": [],
            "missing": [],
            "message": f"解析 factor_order 失败: {exc}",
        }

    if not dynamic_feats:
        return {
            "ready": True,
            "total_dynamic": 0,
            "transformed": [],
            "not_transformed": [],
            "missing": [],
            "message": "该 Task 仅使用 Alpha158 基线因子，无需因子改造",
        }

    # 3. 查询 catalog 改造状态
    placeholders = ",".join(["%s"] * len(dynamic_feats))
    sql = f"""
        SELECT factor_name, transformation_status
        FROM aistock_factor_catalog
        WHERE factor_name IN ({placeholders})
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, dynamic_feats)
            rows = cur.fetchall()

    found = {r[0]: r[1] for r in rows}

    transformed = [f for f in dynamic_feats if found.get(f) == "SUCCESS"]
    not_transformed = [f for f in dynamic_feats if f in found and found[f] != "SUCCESS"]
    missing = [f for f in dynamic_feats if f not in found]

    ready = len(not_transformed) == 0 and len(missing) == 0

    if ready:
        msg = f"全部 {len(dynamic_feats)} 个动态因子已完成改造，可以执行选股"
    else:
        parts = []
        if missing:
            parts.append(f"{len(missing)} 个因子未录入 catalog: {missing}")
        if not_transformed:
            detail = {f: found[f] for f in not_transformed}
            parts.append(f"{len(not_transformed)} 个因子改造未完成: {detail}")
        msg = f"共 {len(dynamic_feats)} 个动态因子，{len(transformed)} 个已改造。" + "；".join(parts)

    return {
        "ready": ready,
        "total_dynamic": len(dynamic_feats),
        "transformed": transformed,
        "not_transformed": not_transformed,
        "missing": missing,
        "message": msg,
    }


# ============================================================
# Main entry point
# ============================================================

def build_db_selection(
    *,
    signal_source: str,
    signal_source_id: str,
    signal_loop_id: Optional[int] = None,
    trade_date: str,
    top_k: int = 50,
) -> Dict[str, Any]:
    """Build stock selection using DB data service (zero file I/O).

    Parameters
    ----------
    signal_source : "rdagent_task" | "qe_experiment" | "qe_evolution"
    signal_source_id : task_run_id or experiment_id
    signal_loop_id : loop id (for rdagent_task)
    trade_date : YYYY-MM-DD
    top_k : number of top stocks to return

    Returns
    -------
    {"items": [...], "metadata": {...}} compatible with build_loop_selection output.
    """
    t0 = time.time()
    logger.info(
        f"build_db_selection: source={signal_source} id={signal_source_id} "
        f"loop={signal_loop_id} trade_date={trade_date} top_k={top_k}"
    )

    # ── Step 1: normalise trade_date ──
    actual_date = _get_latest_trade_date(trade_date)
    logger.info(f"Step1: actual_date={actual_date}")

    # ── Step 2: determine workspace ──
    if signal_source == "rdagent_task":
        # 与 InferenceEngine.assets_root 保持一致
        assets_root = (Path(__file__).resolve().parents[3] / "rdagent_assets" / "rdagent_tasks").resolve()
        task_dir = assets_root / signal_source_id
        if not task_dir.exists():
            raise RuntimeError(f"Task 资产目录不存在: {task_dir}")
    elif signal_source in ("qe_experiment", "qe_evolution"):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT workspace_path FROM qe_experiments WHERE experiment_id = %s",
                    (signal_source_id,),
                )
                row = cur.fetchone()
        if not row or not row[0]:
            raise RuntimeError(f"实验 {signal_source_id} 不存在或无 workspace")
        task_dir = Path(row[0])
        if not task_dir.exists():
            raise RuntimeError(f"实验工作目录不存在: {task_dir}")
    else:
        raise ValueError(f"未知信号来源: {signal_source}")

    logger.info(f"Step2: task_dir={task_dir}")

    # ── Step 3: load manifest + factor_order ──
    engine = InferenceEngine()
    if signal_source == "rdagent_task":
        manifest = engine._load_task_manifest(signal_source_id)
    else:
        manifest = engine._load_experiment_manifest(str(task_dir))

    if not manifest:
        raise RuntimeError(f"未找到 manifest: {task_dir}")

    factor_order, alpha158_feats, dynamic_feats = engine._infer_expected_features(task_dir, manifest)
    logger.info(
        f"Step3: factor_order={len(factor_order)} "
        f"(alpha158={len(alpha158_feats)}, dynamic={len(dynamic_feats)})"
    )

    # ── Step 4: validate dynamic factors ──
    _validate_dynamic_factors(dynamic_feats)
    logger.info(f"Step4: 所有 {len(dynamic_feats)} 个动态因子验证通过")

    # ── Step 5: load model ──
    primary = manifest.get("primary_assets", {})
    model_file = task_dir / primary["model_weight_relpath"]
    model, model_kind, inner_model, num_features_expected = load_model_from_pkl(model_file)
    logger.info(f"Step5: model_kind={model_kind}, expected_features={num_features_expected}")

    # ── Step 6: get universe ──
    from ...data_service.realtime_factor_data_loader import RealtimeFactorDataLoader
    loader = RealtimeFactorDataLoader()
    universe = loader.get_stock_universe(actual_date, exclude_st=True)
    logger.info(f"Step6: universe={len(universe)} stocks")

    # ── Step 7: compute features (DB-based, zero file I/O) ──

    # 7.1 Data window calculation
    from ...data_service.preprocessor import get_required_data_window
    required_window = get_required_data_window(factor_order)
    natural_days_needed = int(required_window * 1.5) + 10
    start_date_dt = actual_date - timedelta(days=natural_days_needed)
    start_date_str = start_date_dt.isoformat()
    end_date_str = actual_date.isoformat()

    # 7.2 Load OHLCV from DB (replaces daily_pv.h5)
    t_ohlcv = time.time()
    df_ohlcv = loader.load(
        instruments=universe,
        start_date=start_date_str,
        end_date=end_date_str,
        fields=["open", "close", "high", "low", "volume", "amount", "factor"],
        adjust="qfq",
    )
    logger.info(f"Step7.2: OHLCV loaded, shape={df_ohlcv.shape}, elapsed={time.time()-t_ohlcv:.1f}s")

    if df_ohlcv.empty:
        raise RuntimeError(f"OHLCV 数据为空: {start_date_str} ~ {end_date_str}")

    # 7.3 Build static factors from DB (replaces static_factors.parquet)
    from ...data_service.qe_data_service import build_static_factors as _build_sf

    class _StaticFactorsLoader:
        def __init__(self):
            self._cache = None
            self._cache_key = None

        def load(self, instruments, start_date, end_date, columns=None):
            key = (
                tuple(sorted(instruments)) if isinstance(instruments, list) else instruments,
                str(start_date),
                str(end_date),
            )
            if self._cache is not None and self._cache_key == key:
                df = self._cache
            else:
                df = _build_sf(instruments, start_date, end_date)
                self._cache = df
                self._cache_key = key
            if columns and not df.empty:
                available = [c for c in columns if c in df.columns]
                return df[available].copy() if available else df.copy()
            return df.copy()

    static_loader = _StaticFactorsLoader()

    # Pre-warm static factors cache (needed by factor sandbox within timeout)
    t_static = time.time()
    static_loader.load(universe, start_date_str, end_date_str)
    logger.info(f"Step7.3: static factors cached, elapsed={time.time()-t_static:.1f}s")

    # 7.4 Execute dynamic factor code via sandbox
    df_dynamic_parts: List[pd.DataFrame] = []
    if dynamic_feats:
        logger.info(f"Step7.4: 执行 {len(dynamic_feats)} 个动态因子")
        for factor_name in dynamic_feats:
            t_f = time.time()
            code = _load_factor_code(factor_name)
            df_part = _execute_factor(
                factor_name=factor_name,
                code=code,
                instruments=universe,
                start_date=start_date_str,
                end_date=end_date_str,
                loader=loader,
                static_loader=static_loader,
                timeout=300,
            )
            logger.info(
                f"  因子 {factor_name}: shape={df_part.shape}, "
                f"elapsed={time.time()-t_f:.1f}s"
            )
            df_dynamic_parts.append(df_part)

    # Combine dynamic factors
    if df_dynamic_parts:
        df_factors = pd.concat(df_dynamic_parts, axis=1)
        df_factors = df_factors.loc[:, ~df_factors.columns.duplicated()]
    else:
        df_factors = pd.DataFrame()

    # Extract only actual_date data from dynamic factors
    if not df_factors.empty:
        factor_dates = _safe_get_datetime_level(df_factors)
        actual_ts = pd.Timestamp(actual_date)
        if actual_ts in factor_dates.unique():
            df_factors = df_factors.loc[actual_ts]
        else:
            available = factor_dates.unique()
            closest = max([d for d in available if d <= actual_ts], default=available[-1])
            df_factors = df_factors.loc[closest]
            logger.warning(f"动态因子无 {actual_date} 数据，使用 {closest.date()}")

        # Ensure MultiIndex
        if not isinstance(df_factors.index, pd.MultiIndex):
            used_date = actual_ts if actual_ts in factor_dates.unique() else closest
            df_factors.index = pd.MultiIndex.from_product(
                [[used_date], df_factors.index],
                names=["datetime", "instrument"],
            )

    # 7.5 Compute Alpha158 baseline factors
    if alpha158_feats:
        t_alpha = time.time()
        alpha_subset = engine._compute_alpha158_last_day_only(df_ohlcv, alpha158_feats)
        logger.info(
            f"Step7.5: Alpha158 computed, shape={alpha_subset.shape}, "
            f"elapsed={time.time()-t_alpha:.1f}s"
        )
    else:
        if not df_factors.empty:
            alpha_subset = pd.DataFrame(index=df_factors.index)
        else:
            raise RuntimeError("alpha158_feats 和 dynamic_feats 都为空")

    # 7.6 Assemble features in factor_order
    df_factors_combined = assemble_features(
        alpha_subset, df_factors, factor_order, alpha158_feats, dynamic_feats
    )
    logger.info(f"Step7.6: assembled features={len(df_factors_combined.columns)}")

    # Handle feature count mismatch
    actual_count = len(df_factors_combined.columns)
    if num_features_expected > 0 and actual_count != num_features_expected:
        logger.warning(
            f"特征数量不匹配: 模型期望 {num_features_expected}，实际 {actual_count}"
        )
        if actual_count > num_features_expected:
            df_factors_combined = df_factors_combined.iloc[:, :num_features_expected]
        else:
            for i in range(num_features_expected - actual_count):
                df_factors_combined[f"padding_{i}"] = 0.0

    # Select actual_date rows
    unique_dates = _safe_get_datetime_level(df_factors_combined).unique()
    actual_ts = pd.Timestamp(actual_date)
    if actual_ts not in unique_dates:
        earlier = unique_dates[unique_dates <= actual_ts]
        if earlier.empty:
            raise RuntimeError(f"推理日期 {actual_date} 无有效数据")
        actual_ts = earlier.max()

    df_today = df_factors_combined[_safe_get_datetime_level(df_factors_combined) == actual_ts]
    if df_today.empty:
        raise RuntimeError(f"推理日期 {actual_date} 无有效因子数据")

    # ── Step 8: model prediction ──
    t_pred = time.time()
    scores = predict_scores(model, inner_model, model_kind, df_today)

    if len(scores) != len(df_today) and scores.ndim > 1:
        scores = scores[:, -1]

    df_scores = pd.DataFrame(index=df_today.index)
    df_scores["score"] = scores
    logger.info(f"Step8: prediction done, {len(df_scores)} stocks, elapsed={time.time()-t_pred:.1f}s")

    # ── Step 9: save signals + build result ──
    # 9.1 Save signals to DB
    loop_id = signal_loop_id if signal_loop_id is not None else 0
    trade_date_dt = datetime.combine(actual_date, datetime.min.time())
    save_signals_to_db(signal_source_id, loop_id, trade_date_dt, df_scores)

    # 9.2 Load top-K from saved signals
    if signal_source == "rdagent_task":
        strategy_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"rdagent_loop:{signal_source_id}:{loop_id}"))
    else:
        strategy_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"rdagent_loop:{signal_source_id}:{loop_id}"))

    sql_topk = """
        SELECT symbol, rank, score
        FROM trading.rdagent_signal
        WHERE strategy_id = %s AND trade_date = %s
        ORDER BY rank ASC NULLS LAST, score DESC NULLS LAST
        LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_topk, (strategy_id, actual_date, top_k))
            signal_rows = cur.fetchall()

    items = []
    for row in signal_rows:
        items.append({
            "symbol": row[0],
            "rank": row[1],
            "score": float(row[2]) if row[2] is not None else None,
        })

    # 9.3 Enrich with quotes
    items = _enrich_with_quotes(items, actual_date)

    elapsed_ms = int((time.time() - t0) * 1000)
    logger.info(f"build_db_selection 完成: {len(items)} items, elapsed={elapsed_ms}ms")

    return {
        "items": items,
        "metadata": {
            "signal_source": signal_source,
            "signal_source_id": signal_source_id,
            "signal_loop_id": signal_loop_id,
            "actual_trade_date": actual_date.isoformat(),
            "top_k": top_k,
            "model_kind": model_kind,
            "num_features": len(factor_order),
            "universe_size": len(universe),
            "elapsed_ms": elapsed_ms,
            "data_path": "db_direct",
        },
    }
