"""实盘演练 API 路由 — 对应 v4 §6.3 全部端点."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..services.paper_trading.portfolio_manager import PortfolioManager
from ..services.paper_trading.signal_generator import SignalGenerator
from ..services.paper_trading.trade_executor import TradeExecutor
from ..services.paper_trading.stock_pnl_tracker import StockPnLTracker
from ..services.paper_trading.performance_calculator import PerformanceCalculator
from ..services.paper_trading.training_service import TrainingService
from ..services.paper_trading.scheduler import paper_trading_scheduler
from ..db.pg_pool import get_conn

logger = logging.getLogger("aistock.paper_trading.router")

router = APIRouter(prefix="/paper-trading", tags=["paper-trading"])


# ── Pydantic Models ──


class PortfolioCreateRequest(BaseModel):
    portfolio_name: str
    signal_source: str
    signal_source_id: str
    signal_loop_id: Optional[int] = None
    model_source: str = "original"
    training_job_id: Optional[str] = None
    initial_capital: float = 1000000
    max_positions: int = 20
    max_position_pct: float = 0.10
    trade_freq: str = "daily"
    max_turnover_pct: float = 0.30
    fee_config: Optional[Dict[str, Any]] = None
    benchmark: str = "000300.SH"
    auto_run: bool = True
    execute_time: str = "17:30"
    enable_factor_attribution: bool = True
    enable_live_ic: bool = True
    factor_list: Optional[List[Any]] = None
    model_catalog_id: Optional[int] = None
    asset_bundle_id: Optional[str] = None
    intraday_strategy: str = "CLOSE_PRICE"
    intraday_config: Optional[Dict[str, Any]] = None
    intraday_freq: str = "5m"
    enable_intraday: bool = False
    intraday_exec_mode: str = "replay"
    start_date: Optional[str] = None  # 追赶模式起始日期 (YYYY-MM-DD)


class PortfolioUpdateRequest(BaseModel):
    portfolio_name: Optional[str] = None
    max_positions: Optional[int] = None
    max_position_pct: Optional[float] = None
    max_turnover_pct: Optional[float] = None
    fee_config: Optional[Dict[str, Any]] = None
    benchmark: Optional[str] = None
    auto_run: Optional[bool] = None
    execute_time: Optional[str] = None
    enable_factor_attribution: Optional[bool] = None
    enable_live_ic: Optional[bool] = None
    model_source: Optional[str] = None
    training_job_id: Optional[str] = None
    model_catalog_id: Optional[int] = None
    asset_bundle_id: Optional[str] = None
    factor_list: Optional[List[Any]] = None
    intraday_strategy: Optional[str] = None
    intraday_config: Optional[Dict[str, Any]] = None
    intraday_freq: Optional[str] = None
    enable_intraday: Optional[bool] = None
    intraday_exec_mode: Optional[str] = None
    start_date: Optional[str] = None


class SelectionRequest(BaseModel):
    signal_source: str
    signal_source_id: str
    signal_loop_id: Optional[int] = None
    trade_date: Optional[str] = None
    top_k: int = 50


class WatchlistAddItem(BaseModel):
    code: str
    rank: Optional[int] = None
    name: Optional[str] = None
    price: Optional[float] = None


class WatchlistAddRequest(BaseModel):
    symbols: List[str] = []
    items: Optional[List[WatchlistAddItem]] = None
    signal_source: Optional[str] = None
    signal_source_id: Optional[str] = None
    signal_loop_id: Optional[int] = None
    category_id: int = 1
    entry_price_date: Optional[str] = None  # 指定以该日期收盘价作为入场价 (YYYY-MM-DD)


class TrainingStartRequest(BaseModel):
    signal_source: str
    signal_source_id: str
    signal_loop_id: Optional[int] = None
    train_start: str
    train_end: str
    valid_start: str
    valid_end: str
    n_epochs: Optional[int] = None
    batch_size: Optional[int] = None
    lr: Optional[float] = None
    early_stop: Optional[int] = None
    source_config_path: Optional[str] = None
    workspace_path: Optional[str] = None


# ── 模拟盘 CRUD (5个) ──


@router.post("/portfolios")
def create_portfolio(req: PortfolioCreateRequest) -> Dict[str, Any]:
    # 创建前检查因子改造状态
    from ..services.paper_trading.db_selection_service import check_factor_readiness
    readiness = check_factor_readiness(
        signal_source=req.signal_source,
        signal_source_id=req.signal_source_id,
        signal_loop_id=req.signal_loop_id,
    )
    if not readiness["ready"]:
        not_done = readiness.get("not_transformed", []) + readiness.get("missing", [])
        raise HTTPException(
            422,
            f"因子未就绪，无法创建模拟盘: {readiness['message']}。"
            f"未改造因子: {not_done}"
        )
    return PortfolioManager.create_portfolio(req.model_dump(exclude_none=True))


@router.get("/portfolios")
def list_portfolios() -> List[Dict[str, Any]]:
    return PortfolioManager.list_portfolios()


@router.get("/portfolios/{portfolio_id}")
def get_portfolio(portfolio_id: int) -> Dict[str, Any]:
    result = PortfolioManager.get_portfolio(portfolio_id)
    if result is None:
        raise HTTPException(404, f"模拟盘 {portfolio_id} 不存在")
    return result


@router.put("/portfolios/{portfolio_id}")
def update_portfolio(portfolio_id: int, req: PortfolioUpdateRequest) -> Dict[str, Any]:
    result = PortfolioManager.update_portfolio(portfolio_id, req.model_dump(exclude_none=True))
    if result is None:
        raise HTTPException(404, f"模拟盘 {portfolio_id} 不存在")
    return result


@router.delete("/portfolios/{portfolio_id}")
def delete_portfolio(portfolio_id: int) -> Dict[str, str]:
    try:
        PortfolioManager.delete_portfolio(portfolio_id)
        return {"status": "deleted"}
    except ValueError as e:
        raise HTTPException(400, str(e))


# ── 运行控制 (4个) ──


@router.post("/portfolios/{portfolio_id}/start")
def start_portfolio(portfolio_id: int) -> Dict[str, Any]:
    try:
        return PortfolioManager.start_portfolio(portfolio_id)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/portfolios/{portfolio_id}/pause")
def pause_portfolio(portfolio_id: int) -> Dict[str, Any]:
    try:
        return PortfolioManager.pause_portfolio(portfolio_id)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/portfolios/{portfolio_id}/stop")
def stop_portfolio(portfolio_id: int) -> Dict[str, Any]:
    try:
        return PortfolioManager.stop_portfolio(portfolio_id)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/portfolios/{portfolio_id}/run-once")
def run_once(portfolio_id: int) -> Dict[str, Any]:
    config = PortfolioManager.get_portfolio(portfolio_id)
    if config is None:
        raise HTTPException(404, f"模拟盘 {portfolio_id} 不存在")
    if config.get("status") != "running":
        raise HTTPException(400, f"模拟盘未运行 (status={config.get('status')})")
    return paper_trading_scheduler.run_once(portfolio_id)


@router.post("/portfolios/{portfolio_id}/start-catchup")
def start_catchup(portfolio_id: int) -> Dict[str, Any]:
    """启动历史追赶模式: created → catching_up."""
    import threading
    try:
        result = PortfolioManager.start_catchup(portfolio_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    # 在后台线程启动追赶
    t = threading.Thread(
        target=paper_trading_scheduler.run_catchup,
        args=(portfolio_id,),
        name=f"catchup-{portfolio_id}",
        daemon=True,
    )
    t.start()
    return {"status": "catching_up", "message": "追赶已启动", "portfolio_id": portfolio_id}


@router.post("/portfolios/{portfolio_id}/go-live")
def go_live(portfolio_id: int) -> Dict[str, Any]:
    """手工切换到实盘模式: caught_up → running.

    前提: caught_up 状态 + 非交易时段
    """
    try:
        return PortfolioManager.go_live(portfolio_id)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.get("/portfolios/{portfolio_id}/catchup-progress")
def get_catchup_progress(portfolio_id: int) -> Dict[str, Any]:
    """查询追赶进度."""
    config = PortfolioManager.get_portfolio(portfolio_id)
    if config is None:
        raise HTTPException(404, f"模拟盘 {portfolio_id} 不存在")

    status = config.get("status", "")
    intraday_config = config.get("intraday_config", {})
    if isinstance(intraday_config, str):
        import json as _json
        intraday_config = _json.loads(intraday_config)

    if status == "catching_up":
        result = {
            "status": "catching_up",
            "total_days": intraday_config.get("total_days", 0),
            "completed_days": intraday_config.get("completed_days", 0),
            "current_date": intraday_config.get("current_date"),
            "pct": intraday_config.get("pct", 0),
        }
        if "catchup_error" in intraday_config:
            result["error"] = intraday_config["catchup_error"]
            result["error_time"] = intraday_config.get("catchup_error_time")
        return result
    elif status == "caught_up":
        return {
            "status": "caught_up",
            "total_days": intraday_config.get("total_days", 0),
            "completed_days": intraday_config.get("total_days", 0),
            "pct": 100.0,
        }
    else:
        return {"status": status, "message": "非追赶状态"}


# ── 执行算法查询 (2个) ──


@router.get("/execution-algorithms")
def list_execution_algorithms() -> List[Dict[str, Any]]:
    """启用的执行算法列表（从 execution_algorithm_catalog 读取）."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT algo_code, algo_name, category, description,
                       param_schema, is_enabled
                FROM execution_algorithm_catalog
                WHERE is_enabled = TRUE
                ORDER BY id
                """
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@router.get("/execution-algorithms/{algo_code}")
def get_execution_algorithm(algo_code: str) -> Dict[str, Any]:
    """获取指定执行算法详情."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT algo_code, algo_name, category, description,
                       param_schema, default_config, is_enabled,
                       created_at, updated_at
                FROM execution_algorithm_catalog
                WHERE algo_code = %s
                """,
                (algo_code,),
            )
            if cur.description is None:
                raise HTTPException(404, f"算法 {algo_code} 不存在")
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            if row is None:
                raise HTTPException(404, f"算法 {algo_code} 不存在")
            return dict(zip(cols, row))


# ── 选股 (3个) ──


class PreflightRequest(BaseModel):
    signal_source: str
    signal_source_id: str
    signal_loop_id: Optional[int] = None


@router.get("/next-trade-date")
def get_next_trade_date(trade_date: str = Query(..., description="YYYY-MM-DD")) -> Dict[str, Any]:
    """给定日期，返回规范化交易日(数据日)和下一个交易日(选股目标日)。"""
    from ..services.paper_trading.db_selection_service import _get_latest_trade_date
    from ..services.paper_trading.signal_generator import _get_next_trade_date
    actual_date = _get_latest_trade_date(trade_date)
    next_date = _get_next_trade_date(actual_date)
    return {
        "input_date": trade_date,
        "actual_trade_date": actual_date.isoformat(),
        "next_trade_date": next_date.isoformat() if next_date else None,
    }


@router.post("/selection/preflight")
def selection_preflight(req: PreflightRequest) -> Dict[str, Any]:
    """检查指定 task/experiment 的因子改造状态，选股前调用。"""
    from ..services.paper_trading.db_selection_service import check_factor_readiness
    return check_factor_readiness(
        signal_source=req.signal_source,
        signal_source_id=req.signal_source_id,
        signal_loop_id=req.signal_loop_id,
    )


@router.post("/selection")
def run_selection(req: SelectionRequest) -> Dict[str, Any]:
    from ..services.paper_trading.signal_generator import SignalGenerator
    config = {
        "signal_source": req.signal_source,
        "signal_source_id": req.signal_source_id,
        "signal_loop_id": req.signal_loop_id,
        "top_k": req.top_k,
    }
    trade_date = req.trade_date or str(date.today())
    try:
        return SignalGenerator._run_selection(config, trade_date)
    except RuntimeError as e:
        raise HTTPException(422, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/selection/add-watchlist")
def add_to_watchlist(req: WatchlistAddRequest) -> Dict[str, Any]:
    # 优先使用 items（含 rank/name），兼容旧版 symbols
    if req.items:
        raw_items = req.items
    elif req.symbols:
        raw_items = [WatchlistAddItem(code=s) for s in req.symbols]
    else:
        raise HTTPException(400, "symbols 或 items 列表为空")

    items = []
    for it in raw_items:
        items.append({
            "code": it.code,
            "rank": it.rank,
            "name": it.name,
            "entry_price": it.price,
            "task_id": req.signal_source_id,
            "loop_id": req.signal_loop_id,
            "entry_source": req.signal_source or "paper_trading",
        })

    from ..services.watchlist_service import add_items_bulk_from_task_selection
    result = add_items_bulk_from_task_selection(
        items=items,
        category_id=req.category_id,
        on_conflict="ignore",
        entry_source=req.signal_source or "paper_trading",
        entry_price_date=req.entry_price_date,
    )

    if not result.get("ok"):
        errors = result.get("errors", [])
        raise HTTPException(500, f"加入自选失败: {errors}")

    return {
        "added": result.get("added", 0),
        "skipped": result.get("skipped", 0),
        "errors": result.get("errors", []),
        "total": len(req.symbols),
    }


# ── 模型训练 (5个) ──


@router.post("/training/start")
def start_training(req: TrainingStartRequest) -> Dict[str, Any]:
    try:
        job_id = TrainingService.start_training(req.model_dump(exclude_none=True))
        return {"job_id": job_id, "status": "running"}
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))


@router.get("/training/status")
def get_training_status() -> Dict[str, Any]:
    result = TrainingService.get_training_status()
    if result is None:
        return {"status": "idle"}
    return result


@router.get("/training/{job_id}/logs")
def stream_training_logs(job_id: str):
    return StreamingResponse(
        TrainingService.stream_logs(job_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/training/{job_id}/cancel")
def cancel_training(job_id: str) -> Dict[str, Any]:
    ok = TrainingService.cancel_training(job_id)
    if not ok:
        raise HTTPException(404, f"训练任务 {job_id} 不存在或已完成")
    return {"status": "cancelled", "job_id": job_id}


@router.get("/training/history")
def get_training_history() -> List[Dict[str, Any]]:
    return TrainingService.get_training_history()


# ── 信号查询 (2个) ──


@router.get("/portfolios/{portfolio_id}/signals")
def get_signals(
    portfolio_id: int,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM paper_trading.trade_signals
                WHERE portfolio_id = %s
                ORDER BY trade_date DESC, symbol
                LIMIT %s OFFSET %s
                """,
                (portfolio_id, limit, offset),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@router.get("/portfolios/{portfolio_id}/signals/pending")
def get_pending_signals(portfolio_id: int) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM paper_trading.trade_signals
                WHERE portfolio_id = %s AND status = 'pending'
                ORDER BY trade_date, side DESC, score DESC
                """,
                (portfolio_id,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── 数据查询 (6个) ──


@router.get("/portfolios/{portfolio_id}/snapshots")
def get_snapshots(
    portfolio_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    conditions = ["portfolio_id = %s"]
    params: list = [portfolio_id]
    if start_date:
        conditions.append("trade_date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("trade_date <= %s")
        params.append(end_date)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM paper_trading.daily_snapshot WHERE {' AND '.join(conditions)} ORDER BY trade_date",
                params,
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@router.get("/portfolios/{portfolio_id}/positions")
def get_current_positions(portfolio_id: int) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM paper_trading.positions
                WHERE portfolio_id = %s AND trade_date = (
                    SELECT MAX(trade_date) FROM paper_trading.positions WHERE portfolio_id = %s
                )
                ORDER BY weight DESC NULLS LAST
                """,
                (portfolio_id, portfolio_id),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@router.get("/portfolios/{portfolio_id}/positions/{trade_date}")
def get_historical_positions(portfolio_id: int, trade_date: str) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM paper_trading.positions
                WHERE portfolio_id = %s AND trade_date = %s
                ORDER BY weight DESC NULLS LAST
                """,
                (portfolio_id, trade_date),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@router.get("/portfolios/{portfolio_id}/trades")
def get_trades(
    portfolio_id: int,
    symbol: Optional[str] = None,
    limit: int = Query(200, ge=1, le=5000),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:
    conditions = ["portfolio_id = %s"]
    params: list = [portfolio_id]
    if symbol:
        conditions.append("symbol = %s")
        params.append(symbol)
    params.extend([limit, offset])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT * FROM paper_trading.trades
                WHERE {' AND '.join(conditions)}
                ORDER BY trade_date DESC, created_at DESC
                LIMIT %s OFFSET %s
                """,
                params,
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@router.get("/portfolios/{portfolio_id}/stock-pnl")
def get_stock_pnl(portfolio_id: int) -> List[Dict[str, Any]]:
    return StockPnLTracker.get_stock_pnl_list(portfolio_id)


@router.get("/portfolios/{portfolio_id}/stock-pnl/{symbol}")
def get_stock_trade_history(portfolio_id: int, symbol: str) -> List[Dict[str, Any]]:
    return StockPnLTracker.get_stock_trade_history(portfolio_id, symbol)


# ── 报表 (3个) ──


@router.get("/reports/compare")
def compare_portfolios(
    ids: str = Query(..., description="逗号分隔的 portfolio_id 列表"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    portfolio_ids = [int(x.strip()) for x in ids.split(",") if x.strip()]
    sd = date.fromisoformat(start_date) if start_date else None
    ed = date.fromisoformat(end_date) if end_date else None
    return PerformanceCalculator.compute_compare_report(portfolio_ids, sd, ed)


@router.get("/reports/{portfolio_id}/metrics")
def get_portfolio_metrics(
    portfolio_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    sd = date.fromisoformat(start_date) if start_date else None
    ed = date.fromisoformat(end_date) if end_date else None
    return PerformanceCalculator.compute_portfolio_metrics(portfolio_id, sd, ed)


@router.get("/reports/{portfolio_id}/attribution")
def get_factor_attribution(
    portfolio_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    conditions = ["portfolio_id = %s"]
    params: list = [portfolio_id]
    if start_date:
        conditions.append("trade_date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("trade_date <= %s")
        params.append(end_date)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT * FROM paper_trading.factor_attribution
                WHERE {' AND '.join(conditions)}
                ORDER BY trade_date, factor_prefix
                """,
                params,
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── 反馈闭环 (3个) ──


@router.get("/portfolios/{portfolio_id}/factor-ic")
def get_factor_ic_series(
    portfolio_id: int,
    factor_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    conditions = ["strategy_id = %s"]
    params: list = [str(portfolio_id)]
    if factor_name:
        conditions.append("factor_name = %s")
        params.append(factor_name)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT * FROM factor_live_track
                WHERE {' AND '.join(conditions)}
                ORDER BY trade_date
                """,
                params,
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@router.get("/live-metrics/factors")
def get_factor_live_summary() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM factor_live_summary")
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@router.get("/live-metrics/models")
def get_model_live_summary() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM model_live_summary")
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
