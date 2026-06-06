from __future__ import annotations

import os
import uuid
import logging
from dataclasses import asdict
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, BackgroundTasks

from ..infra.qmt_client import (
    BaseQMTClient,
    QMTNotAvailableError,
    get_qmt_client_singleton,
    reset_qmt_client_singleton,
)
from ..monitor.qmt_monitor import (
    MonitorConfigModel,
    build_monitor_summary,
    build_single_strategy_summary,
    build_strategy_summaries,
    get_monitor_config_model,
    update_monitor_config_model,
)
from ..data_service.dataset_stats_service import DatasetStatsService


router = APIRouter(prefix="/qmt", tags=["qmt"])
logger = logging.getLogger(__name__)

# 数据集统计服务实例
_dataset_stats_service: DatasetStatsService | None = None


def _get_trade_password() -> str:
    return (os.getenv("QMT_TRADE_PASSWORD") or "138730").strip()


def _verify_trade_password(password: str | None) -> None:
    expected = _get_trade_password()
    if not password or password != expected:
        raise HTTPException(status_code=403, detail="交易密码错误")


RAW_ORDER_DIAGNOSTIC_ENV = "AISTOCK_ALLOW_QMT_RAW_ORDER_DIAGNOSTICS"
RAW_ORDER_DIAGNOSTIC_WARNING = (
    "raw MiniQMT order APIs are administrator/POC diagnostics only; normal "
    "multi-strategy execution must use /api/v1/qmt/virtual-strategies/orders "
    "so AIstock can create order_intent, cash, lot, and attribution records "
    "before broker submission"
)


def _raw_order_diagnostics_enabled() -> bool:
    return (os.getenv(RAW_ORDER_DIAGNOSTIC_ENV) or "").strip().lower() in {"1", "true", "yes", "on"}


def _require_raw_order_diagnostics_enabled() -> None:
    if not _raw_order_diagnostics_enabled():
        raise HTTPException(
            status_code=403,
            detail=(
                f"{RAW_ORDER_DIAGNOSTIC_WARNING}; set {RAW_ORDER_DIAGNOSTIC_ENV}=1 "
                "explicitly only for controlled administrator/POC diagnostics"
            ),
        )


def get_dataset_stats_service() -> DatasetStatsService:
    """获取数据集统计服务实例（延迟初始化）"""
    global _dataset_stats_service
    if _dataset_stats_service is None:
        client = get_qmt_client_singleton()
        _dataset_stats_service = DatasetStatsService(client)
    return _dataset_stats_service


def _get_client() -> BaseQMTClient:
    """获取QMT客户端实例（延迟初始化）
    
    第一次调用时会创建客户端实例。如果配置错误，会抛出明确的异常。
    """
    try:
        return get_qmt_client_singleton()
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"初始化QMT客户端失败: {e}", exc_info=True)
        # 严格模式：不允许fallback，直接抛出异常
        raise RuntimeError(
            f"QMT客户端初始化失败: {e}\n"
            f"请检查 .env 文件中的配置：\n"
            f"- MINIQMT_ENABLED\n"
            f"- MINIQMT_ACCOUNT_ID\n"
            f"- MINIQMT_USERDATA_PATH\n"
            f"- MINIQMT_SESSION_ID (可选)\n"
            f"\n如果不需要QMT功能，请设置 MINIQMT_ENABLED=false"
        ) from e


@router.post("/reload", summary="重新加载 QMT 配置并重建客户端")
def reload_client() -> Dict[str, Any]:
    """尝试通过断开+重连的方式刷新 QMT 连接.

    注意：由于客户端现为进程级单例，本接口不会重新构建实例，只会在
    现有实例上执行 disconnect/connect，以避免打破其他调用方的连接状态
    抽象。
    """

    try:
        old_client = _get_client()
        try:
            old_client.disconnect()
        except Exception as exc:
            logger.debug("QMT disconnect during reload failed; rebuilding singleton anyway: %r", exc, exc_info=True)

        reset_qmt_client_singleton()
        client = _get_client()
        ok2, msg2 = client.connect()
        return {
            "ok": bool(ok2),
            "message": f"connect: {msg2}",
            "status": client.status().__dict__,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"重新加载QMT配置失败: {e}\n请检查 .env 文件中的配置。",
        ) from e


@router.get("/status", summary="QMT/xtquant 连接状态")
def get_status() -> Dict[str, Any]:
    client = _get_client()
    return {
        **client.status().__dict__,
        "pid": os.getpid(),
        "client_object_id": hex(id(client)),
        "client_class": f"{client.__class__.__module__}.{client.__class__.__name__}",
    }


@router.post("/connect", summary="连接 QMT（模拟盘/实盘取决于账户与 MINIQMT_MODE）")
def connect() -> Dict[str, Any]:
    client = _get_client()
    ok, msg = client.connect()
    return {
        "success": bool(ok),
        "message": msg,
        "status": {
            **client.status().__dict__,
            "pid": os.getpid(),
            "client_object_id": hex(id(client)),
            "client_class": f"{client.__class__.__module__}.{client.__class__.__name__}",
        },
    }


@router.post("/disconnect", summary="断开 QMT 连接")
def disconnect() -> Dict[str, Any]:
    client = _get_client()
    ok, msg = client.disconnect()
    return {
        "success": bool(ok),
        "message": msg,
        "status": {
            **client.status().__dict__,
            "pid": os.getpid(),
            "client_object_id": hex(id(client)),
            "client_class": f"{client.__class__.__module__}.{client.__class__.__name__}",
        },
    }


@router.get("/account", summary="获取 QMT 资金信息（快照）")
def get_account_info() -> Dict[str, Any]:
    try:
        return _get_client().get_account_info()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/positions", summary="获取 QMT 持仓列表（快照）")
def get_positions() -> List[Dict[str, Any]]:
    try:
        return _get_client().get_positions()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/snapshot", summary="获取 QMT 资金+持仓组合快照")
def get_snapshot() -> Dict[str, Any]:
    try:
        client = _get_client()
        return {
            "status": client.status().__dict__,
            "account": client.get_account_info(),
            "positions": client.get_positions(),
        }
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/orders", summary="获取 QMT 当日委托列表")
def get_orders(cancelable_only: bool = False) -> List[Dict[str, Any]]:
    try:
        return _get_client().get_orders(cancelable_only=cancelable_only)
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/trades", summary="获取 QMT 当日成交列表")
def get_trades() -> List[Dict[str, Any]]:
    try:
        return _get_client().get_trades()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/order", summary="Admin/POC raw MiniQMT order diagnostic")
def place_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Raw administrator/POC order endpoint.

    Normal multi-strategy execution must use `/qmt/virtual-strategies/orders`.
    This route is disabled by default and only opens when
    AISTOCK_ALLOW_QMT_RAW_ORDER_DIAGNOSTICS=1 is set explicitly.
    
    参数：
    - stock_code: 股票代码，如 '600000.SH' 或 '000001.SZ'
    - order_type: 委托类型，23=买入，24=卖出
    - order_volume: 委托数量（股）
    - price_type: 报价类型，详见文档
    - price: 委托价格（限价时填写，市价时填0）
    - strategy_name: 策略名称（可选）
    - order_remark: 委托备注（可选）
    """
    try:
        _verify_trade_password(payload.get("trade_password"))
        _require_raw_order_diagnostics_enabled()

        stock_code = payload.get("stock_code", "").strip()
        order_type = payload.get("order_type")
        order_volume = payload.get("order_volume")
        price_type = payload.get("price_type")
        price = float(payload.get("price", 0.0))
        strategy_name = payload.get("strategy_name", "").strip()
        order_remark = payload.get("order_remark", "").strip()

        if not stock_code:
            raise HTTPException(status_code=400, detail="股票代码不能为空")
        if order_type not in [23, 24]:
            raise HTTPException(status_code=400, detail="委托类型错误，23=买入，24=卖出")
        if not order_volume or order_volume <= 0:
            raise HTTPException(status_code=400, detail="委托数量必须大于0")
        if price_type is None:
            raise HTTPException(status_code=400, detail="报价类型不能为空")

        client = _get_client()
        order_id, message = client.place_order(
            stock_code=stock_code,
            order_type=int(order_type),
            order_volume=int(order_volume),
            price_type=int(price_type),
            price=price,
            strategy_name=strategy_name,
            order_remark=order_remark,
        )
        diagnostic_getter = getattr(client, "get_last_order_diagnostic", None)
        diagnostic = diagnostic_getter() if callable(diagnostic_getter) else None

        return {
            "success": order_id > 0,
            "order_id": order_id,
            "message": message,
            "diagnostic": diagnostic,
            "diagnostic_warning": RAW_ORDER_DIAGNOSTIC_WARNING,
        }
    except HTTPException:
        raise
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"下单失败: {e!r}") from e


@router.get("/monitor/config", summary="获取 QMT 持仓监控配置")
def get_qmt_monitor_config() -> Dict[str, Any]:
    """返回当前 QMT 监控阈值配置.

    - global: 账户级阈值（总回撤、当日亏损、单票最大权重、最小现金比例）
    - per_symbol: 按股票代码的个性化阈值
    """

    model = get_monitor_config_model()
    return model.dict(by_alias=True)


@router.post("/monitor/config", summary="更新 QMT 持仓监控配置")
def update_qmt_monitor_config(payload: MonitorConfigModel) -> Dict[str, Any]:
    """更新 QMT 监控阈值配置.

    直接提交完整配置对象（global + per_symbol），后端进行覆盖写入。
    """

    model = update_monitor_config_model(payload)
    return model.dict(by_alias=True)


@router.get("/monitor/summary", summary="获取 QMT 持仓监控摘要")
def get_qmt_monitor_summary() -> Dict[str, Any]:
    """基于当前 QMT 快照，返回账户/持仓 PnL 指标与告警列表.

    - account: total_asset / market_value / available_cash / total_position_profit / total_daily_profit
    - positions: 每只持仓的 PnL 指标 + 权重 + 本地 alerts
    - alerts: 聚合告警列表
    """

    try:
        return build_monitor_summary()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/monitor/strategies", summary="获取所有策略的 QMT 监控摘要")
def get_qmt_monitor_strategies() -> Dict[str, Any]:
    """按 strategy_id 维度返回简要监控摘要.

    - items: 每个元素包含 strategy_id、orders_count、trades_count、
      total_position_profit、total_daily_profit、market_value、weight_asset 等。
    """

    try:
        return build_strategy_summaries()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/monitor/strategy/{strategy_id}/summary", summary="获取单个策略的 QMT 监控摘要")
def get_qmt_monitor_strategy_summary(strategy_id: str) -> Dict[str, Any]:
    """返回指定 strategy_id 对应的详细监控摘要.

    - pnl: 该策略相关持仓的总持仓盈亏 / 当日盈亏 / 市值及资产权重
    - positions: 涉及的持仓明细
    - orders / trades: 该策略的当日委托与成交
    """

    try:
        return build_single_strategy_summary(strategy_id)
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/cancel", summary="撤单")
def cancel_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    """撤单接口
    
    参数（二选一）：
    - order_id: 订单编号
    或
    - market: 交易市场（0=上海，1=深圳）
    - order_sysid: 柜台合同编号
    """
    try:
        _verify_trade_password(payload.get("trade_password"))

        order_id = payload.get("order_id")
        market = payload.get("market")
        order_sysid = payload.get("order_sysid")

        client = _get_client()
        if order_id:
            success, message = client.cancel_order(str(order_id))
        elif market is not None and order_sysid:
            success, message = client.cancel_order_by_sysid(int(market), str(order_sysid))
        else:
            raise HTTPException(status_code=400, detail="请提供 order_id 或 (market, order_sysid)")
        diagnostic_getter = getattr(client, "get_last_cancel_diagnostic", None)
        diagnostic = diagnostic_getter() if callable(diagnostic_getter) else None

        return {
            "success": success,
            "message": message,
            "diagnostic": diagnostic,
        }
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"撤单失败: {e!r}") from e


@router.post("/order/batch", summary="Admin/POC raw MiniQMT batch order diagnostic")
def batch_place_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Raw administrator/POC batch order endpoint.

    Normal multi-strategy execution must use `/qmt/virtual-strategies/orders/batch`.
    This route is disabled by default and only opens when
    AISTOCK_ALLOW_QMT_RAW_ORDER_DIAGNOSTICS=1 is set explicitly.
    
    参数：
    - orders: 订单列表，每个订单包含下单接口的所有参数
    """
    try:
        _verify_trade_password(payload.get("trade_password"))
        _require_raw_order_diagnostics_enabled()

        orders = payload.get("orders", [])
        if not isinstance(orders, list) or len(orders) == 0:
            raise HTTPException(status_code=400, detail="订单列表不能为空")

        results = []
        for order_payload in orders:
            try:
                stock_code = order_payload.get("stock_code", "").strip()
                order_type = order_payload.get("order_type")
                order_volume = order_payload.get("order_volume")
                price_type = order_payload.get("price_type")
                price = float(order_payload.get("price", 0.0))
                strategy_name = order_payload.get("strategy_name", "").strip()
                order_remark = order_payload.get("order_remark", "").strip()

                if not stock_code or order_type not in [23, 24] or not order_volume or price_type is None:
                    results.append({
                        "success": False,
                        "stock_code": stock_code,
                        "message": "参数错误",
                    })
                    continue

                order_id, message = _get_client().place_order(
                    stock_code=stock_code,
                    order_type=int(order_type),
                    order_volume=int(order_volume),
                    price_type=int(price_type),
                    price=price,
                    strategy_name=strategy_name,
                    order_remark=order_remark,
                )

                results.append({
                    "success": order_id > 0,
                    "stock_code": stock_code,
                    "order_id": order_id,
                    "message": message,
                })
            except Exception as e:
                results.append({
                    "success": False,
                    "stock_code": order_payload.get("stock_code", ""),
                    "message": f"下单失败: {e!r}",
                })

        success_count = sum(1 for r in results if r.get("success"))
        return {
            "success": True,
            "total": len(results),
            "succeeded": success_count,
            "failed": len(results) - success_count,
            "results": results,
            "diagnostic_warning": RAW_ORDER_DIAGNOSTIC_WARNING,
        }
    except HTTPException:
        raise
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批量下单失败: {e!r}") from e


@router.get("/ipo/limit", summary="查询新股申购额度")
def get_new_purchase_limit() -> Dict[str, Any]:
    try:
        return _get_client().query_new_purchase_limit()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/ipo/list", summary="查询新股信息")
def get_ipo_data() -> List[Dict[str, Any]]:
    try:
        return _get_client().query_ipo_data()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/bank/transfer-in", summary="银行转证券")
def bank_transfer_in(payload: Dict[str, Any]) -> Dict[str, Any]:
    """银行转证券
    
    参数：
    - bank_no: 银行编号
    - bank_account: 银行账号
    - bank_pwd: 银行密码
    - amount: 转账金额
    """
    try:
        _verify_trade_password(payload.get("trade_password"))

        bank_no = payload.get("bank_no", "").strip()
        bank_account = payload.get("bank_account", "").strip()
        bank_pwd = payload.get("bank_pwd", "").strip()
        amount = float(payload.get("amount", 0.0))

        if not bank_no or not bank_account or not bank_pwd:
            raise HTTPException(status_code=400, detail="银行信息不能为空")
        if amount <= 0:
            raise HTTPException(status_code=400, detail="转账金额必须大于0")

        success, message = _get_client().bank_transfer_in(bank_no, bank_account, bank_pwd, amount)
        return {
            "success": success,
            "message": message,
        }
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"银行转证券失败: {e!r}") from e


@router.post("/bank/transfer-out", summary="证券转银行")
def bank_transfer_out(payload: Dict[str, Any]) -> Dict[str, Any]:
    """证券转银行
    
    参数：
    - bank_no: 银行编号
    - bank_account: 银行账号
    - bank_pwd: 银行密码
    - amount: 转账金额
    """
    try:
        _verify_trade_password(payload.get("trade_password"))

        bank_no = payload.get("bank_no", "").strip()
        bank_account = payload.get("bank_account", "").strip()
        bank_pwd = payload.get("bank_pwd", "").strip()
        amount = float(payload.get("amount", 0.0))

        if not bank_no or not bank_account or not bank_pwd:
            raise HTTPException(status_code=400, detail="银行信息不能为空")
        if amount <= 0:
            raise HTTPException(status_code=400, detail="转账金额必须大于0")

        success, message = _get_client().bank_transfer_out(bank_no, bank_account, bank_pwd, amount)
        return {
            "success": success,
            "message": message,
        }
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"证券转银行失败: {e!r}") from e


@router.get("/bank/info", summary="查询银行信息")
def get_bank_info() -> List[Dict[str, Any]]:
    try:
        return _get_client().query_bank_info()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/data/range", summary="查询 miniQMT 本地数据范围")
def get_local_data_range(stock_code: str, period: str) -> Dict[str, Any]:
    try:
        return _get_client().get_local_data_range(stock_code, period)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/data/download", summary="下载 miniQMT 历史数据")
def download_history_data(payload: Dict[str, Any], background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """
    异步下载历史数据。
    参数:
    - stock_list: 股票代码列表
    - period: 周期 (1d, 1m, 5m, 1h)
    - start_time: 开始时间 (YYYYMMDD)
    - end_time: 结束时间 (YYYYMMDD)
    """
    try:
        stock_list = payload.get("stock_list", [])
        period = payload.get("period", "1d")
        start_time = payload.get("start_time", "")
        end_time = payload.get("end_time", "")

        if not stock_list:
            raise HTTPException(status_code=400, detail="股票列表不能为空")

        task_id = str(uuid.uuid4())
        client = _get_client()

        def _run_download():
            try:
                client.download_history_data(
                    stock_list=stock_list,
                    period=period,
                    start_time=start_time,
                    end_time=end_time,
                    task_id=task_id
                )
            except Exception as e:
                import logging
                logging.getLogger(__name__).error(f"Async download failed: {e}")

        background_tasks.add_task(_run_download)

        return {
            "success": True, 
            "task_id": task_id,
            "message": f"已启动 {len(stock_list)} 只股票的 {period} 数据异步下载任务"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/data/download-financial", summary="下载 miniQMT 财务数据")
def download_financial_data(payload: Dict[str, Any], background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """
    异步下载财务数据。
    参数:
    - stock_list: 股票代码列表
    - table_list: 财务表名列表 (如 ['Balance', 'Income'])
    """
    try:
        stock_list = payload.get("stock_list", [])
        table_list = payload.get("table_list", [])

        if not stock_list:
            raise HTTPException(status_code=400, detail="股票列表不能为空")

        task_id = str(uuid.uuid4())
        client = _get_client()

        def _run_financial_download():
            try:
                client.download_financial_data(stock_list=stock_list, table_list=table_list, task_id=task_id)
            except Exception as e:
                import logging
                logging.getLogger(__name__).error(f"Async financial download failed: {e}")

        background_tasks.add_task(_run_financial_download)

        return {
            "success": True, 
            "task_id": task_id,
            "message": f"已启动 {len(stock_list)} 只股票的财务数据异步下载任务"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/data/task/{task_id}/progress", summary="查询异步任务进度（QMT + Ingestion）")
def get_task_progress(task_id: str) -> Dict[str, Any]:
    try:
        # 优先从 QMT 内存任务中查询
        try:
            progress = _get_client().get_task_progress(task_id)
            if progress:
                return progress
        except Exception as exc:
            logger.debug("QMT in-memory task progress lookup failed; checking ingestion jobs: %r", exc, exc_info=True)

        # Fallback: 从 ingestion_jobs 表查询（Tushare 数据集补齐任务）
        from ..db.pg_pool import get_conn
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT job_id, job_type, status, summary
                             FROM market.ingestion_jobs
                            WHERE job_id = %s::uuid""",
                        (task_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        import json
                        job_id, job_type, status, summary_raw = row
                        summary_data = {}
                        if summary_raw:
                            if isinstance(summary_raw, str):
                                try:
                                    summary_data = json.loads(summary_raw)
                                except Exception:
                                    summary_data = {}
                            elif isinstance(summary_raw, dict):
                                summary_data = summary_raw

                        # 从 summary 中提取进度信息
                        # 脚本通过 _update_job_progress 写入 counters/progress/total_days/done_days
                        counters = summary_data.get("counters", {})
                        pct = summary_data.get("progress", 0)
                        total_items = counters.get("total", summary_data.get("total_days", 0))
                        done_items = counters.get("done", summary_data.get("done_days", 0))
                        inserted_rows = counters.get("inserted_rows", 0)

                        # 状态映射：ingestion_jobs 的 status 可能是 queued/running/success/failed
                        mapped_status = status or "unknown"
                        if mapped_status == "queued":
                            mapped_status = "running"  # 前端只识别 running/success/failed

                        if mapped_status == "success":
                            pct = 100

                        dataset_name = summary_data.get("dataset", job_type or "")
                        msg = f"{dataset_name} 增量补齐"
                        if done_items and total_items:
                            msg = f"{dataset_name}: {done_items}/{total_items} 天已完成"

                        return {
                            "status": mapped_status,
                            "progress": int(pct),
                            "message": msg,
                            "total": total_items,
                            "finished": done_items,
                            "inserted_rows": inserted_rows,
                            "job_id": str(job_id),
                            "source": "ingestion_job",
                        }
        except Exception as exc:
            logger.debug("ingestion_jobs task progress lookup failed: %r", exc, exc_info=True)

        raise HTTPException(status_code=404, detail="任务不存在或已过期")
    except HTTPException as e:
        raise e
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Progress query failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/data/latest-day", summary="获取 miniQMT 最新交易日")
def get_latest_trading_day() -> Dict[str, Any]:
    try:
        client = _get_client()
        latest_day = client.get_latest_trading_day()
        return {"latest_day": latest_day}
    except Exception as e:
        import datetime
        return {"latest_day": datetime.date.today().strftime("%Y%m%d"), "error": str(e)}


@router.post("/data/one-click-update", summary="一键更新 miniQMT 历史数据")
def one_click_update(payload: Dict[str, Any], background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """
    一键更新常用周期数据到最新（异步）。
    """
    try:
        periods = payload.get("periods", ["1d", "1m", "5m", "1h"])
        scope = payload.get("scope", "all")
        start_time = payload.get("start_time", "")

        client = _get_client()
        if scope == "all":
            stock_list = client.get_stock_list_in_sector("沪深A股")
        else:
            stock_list = client.get_stock_list_in_sector("沪深A股")

        if not stock_list:
            raise HTTPException(status_code=400, detail="无法获取股票列表，请确认 miniQMT 已启动且 xtdata 路径正确")

        task_id = str(uuid.uuid4())

        client.update_task_status(task_id, {
            "status": "queued",
            "progress": 0,
            "message": "任务已排队，等待执行",
        })

        def _run_one_click():
            try:
                # 获取最新交易日
                calendar = client.get_trading_calendar("SH")
                if calendar:
                    latest_day = calendar[-1]
                else:
                    import datetime
                    latest_day = datetime.date.today().strftime("%Y%m%d")

                total_steps = len(periods) + 1
                current_step = 0

                for p in periods:
                    client.download_history_data(stock_list, p, start_time=start_time, end_time=latest_day)
                    current_step += 1
                    # 更新总任务进度
                    progress = int((current_step / total_steps) * 100)
                    client.update_task_status(task_id, {
                        "status": "downloading",
                        "progress": progress,
                        "message": f"正在下载 {p} 历史数据...",
                    })
                
                # 同步复权因子
                client.download_financial_data(stock_list, ["Capital"])
                
                client.update_task_status(task_id, {
                    "status": "success",
                    "progress": 100,
                    "message": "一键更新完成",
                })
            except Exception as e:
                import logging
                logging.getLogger(__name__).error(f"Async one-click update failed: {e}", exc_info=True)
                client.update_task_status(task_id, {
                    "status": "failed",
                    "error": str(e),
                })

        background_tasks.add_task(_run_one_click)

        return {
            "success": True, 
            "task_id": task_id,
            "message": f"已启动 {len(stock_list)} 只股票的一键更新任务"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/data/datasets/{dataset_id}/check-gap", summary="检查数据集缺口（直接查询数据表）")
def check_dataset_gap(dataset_id: str) -> Dict[str, Any]:
    """
    直接查询数据集自己的表获取 max_date，与最新交易日对比，返回缺口信息。
    不使用 refresh_data_stats()，避免超时和映射错误。
    """
    import logging
    _logger = logging.getLogger(__name__)

    from ..db.pg_pool import get_conn

    try:
        # 1) 从 data_stats_config 获取该数据集的表名和日期列
        table_name = None
        date_column = None
        extra_info = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT table_name, date_column, extra_info FROM market.data_stats_config WHERE data_kind = %s",
                    (dataset_id,),
                )
                row = cur.fetchone()
                if row:
                    table_name, date_column, extra_info = row

        if not table_name or not date_column:
            raise HTTPException(
                status_code=404,
                detail=f"数据集 {dataset_id} 未在 data_stats_config 中注册"
            )

        # 2) 直接查询该表的 max_date（使用 statement_timeout 防止大表超时）
        current_max_date = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '10s'")
                try:
                    cur.execute(f"SELECT MAX({date_column})::date FROM {table_name}")
                    row = cur.fetchone()
                    if row:
                        current_max_date = row[0]
                except Exception as e:
                    _logger.warning(f"[{dataset_id}] 查询 MAX({date_column}) 失败: {e}")
                    conn.rollback()

        # 3) 获取最新交易日
        latest_trading_date = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(cal_date) AS latest FROM market.trading_calendar "
                    "WHERE is_trading = TRUE AND cal_date <= CURRENT_DATE"
                )
                row = cur.fetchone()
                if row:
                    latest_trading_date = row[0]

        if latest_trading_date is None:
            raise HTTPException(
                status_code=400,
                detail="无法获取最新交易日，请先同步交易日历"
            )

        # 4) 计算缺口
        has_gap = False
        gap_start = None
        gap_end = str(latest_trading_date)

        if current_max_date is None:
            has_gap = True
            gap_start = gap_end  # 无数据时从最新交易日开始
        elif current_max_date < latest_trading_date:
            has_gap = True
            # 找到 max_date 之后的下一个交易日作为起始
            next_trading = None
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT MIN(cal_date) FROM market.trading_calendar "
                        "WHERE is_trading = TRUE AND cal_date > %s",
                        (current_max_date,),
                    )
                    row = cur.fetchone()
                    if row:
                        next_trading = row[0]
            gap_start = str(next_trading) if next_trading else str(current_max_date)

        # 5) 确定数据源
        source = "Tushare"
        if extra_info and isinstance(extra_info, dict):
            src = extra_info.get("source", "")
            if src:
                source = src.capitalize()
        elif dataset_id.startswith("kline_") or dataset_id.startswith("tdx_"):
            source = "TDX"
        elif dataset_id.startswith("xtquant"):
            source = "xtquant"

        return {
            "dataset_id": dataset_id,
            "table_name": table_name,
            "date_column": date_column,
            "current_max_date": str(current_max_date) if current_max_date else None,
            "latest_trading_date": str(latest_trading_date),
            "has_gap": has_gap,
            "gap_start": gap_start,
            "gap_end": gap_end,
            "source": source,
        }
    except HTTPException:
        raise
    except Exception as e:
        _logger.error(f"check-gap 失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


# ==================== 数据集统计接口 ====================

@router.get("/data/datasets", summary="获取所有数据集统计信息")
def get_all_datasets() -> Dict[str, Any]:
    """
    获取所有数据集的统计信息，包括状态、数据范围、股票范围等。
    """
    try:
        service = get_dataset_stats_service()
        datasets = service.get_all_datasets()
        
        return {
            "datasets": [asdict(d) for d in datasets],
            "total": len(datasets)
        }
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"获取数据集统计信息失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/data/datasets/{dataset_id}", summary="获取指定数据集的详细统计")
def get_dataset_detail(dataset_id: str) -> Dict[str, Any]:
    """
    获取指定数据集的详细统计信息，包括日期范围、股票范围、质量指标等。
    """
    try:
        service = get_dataset_stats_service()
        detail = service.get_dataset_detail(dataset_id)
        
        if not detail:
            raise HTTPException(status_code=404, detail=f"数据集 {dataset_id} 不存在")
        
        return asdict(detail)
    except HTTPException:
        raise
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"获取数据集 {dataset_id} 详细信息失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/data/datasets/{dataset_id}/catch-up", summary="一键补齐数据集到当前日期")
def catch_up_dataset(dataset_id: str, background_tasks: BackgroundTasks) -> Dict[str, Any]:
    """
    一键补齐指定数据集到当前日期。
    支持 QMT 数据集（kline_1d/1m/5m/1h/tick）和 Tushare 数据集（daily_basic/bak_basic/stock_moneyflow_ts/cyq_perf/cyq_chips）。
    """
    import logging
    import datetime as dt
    _logger = logging.getLogger(__name__)

    try:
        # ============================================================
        # QMT 数据集：通过 miniQMT 下载
        # ============================================================
        dataset_periods = {
            "kline_1d": "1d",
            "kline_1m": "1m",
            "kline_5m": "5m",
            "kline_1h": "1h",
            "tick": "tick",
        }

        if dataset_id in dataset_periods:
            period = dataset_periods[dataset_id]
            client = _get_client()

            stock_list = client.get_stock_list_in_sector("沪深A股")
            if not stock_list:
                raise HTTPException(status_code=400, detail="无法获取股票列表，请确认 miniQMT 已启动")

            calendar = client.get_trading_calendar("SH")
            if calendar:
                latest_day = calendar[-1]
            else:
                latest_day = dt.date.today().strftime("%Y%m%d")

            task_id = str(uuid.uuid4())

            def _run_catch_up():
                try:
                    client.update_task_status(task_id, {
                        "status": "queued",
                        "progress": 0,
                        "message": "任务已排队，等待执行",
                        "total": len(stock_list),
                        "finished": 0,
                    })
                    client.update_task_status(task_id, {
                        "status": "running",
                        "message": f"开始补齐 {dataset_id} 数据...",
                    })
                    for idx, stock_code in enumerate(stock_list):
                        try:
                            client.download_history_data([stock_code], period, end_time=latest_day)
                            finished = idx + 1
                            progress = int((finished / len(stock_list)) * 100)
                            client.update_task_status(task_id, {
                                "status": "running",
                                "progress": progress,
                                "message": f"正在处理 {stock_code}",
                                "total": len(stock_list),
                                "finished": finished,
                                "last_stock": stock_code,
                            })
                        except Exception as e:
                            _logger.warning(f"处理 {stock_code} 失败: {e}")
                    client.update_task_status(task_id, {
                        "status": "success",
                        "progress": 100,
                        "message": f"{dataset_id} 数据补齐完成",
                        "total": len(stock_list),
                        "finished": len(stock_list),
                    })
                except Exception as e:
                    _logger.error(f"数据集 {dataset_id} 补齐失败: {e}", exc_info=True)
                    client.update_task_status(task_id, {
                        "status": "failed",
                        "error": str(e),
                    })

            background_tasks.add_task(_run_catch_up)
            return {
                "success": True,
                "task_id": task_id,
                "message": f"已启动 {dataset_id} 数据补齐任务"
            }

        # ============================================================
        # 非 QMT 数据集：调用 check-gap API 获取缺口信息并返回
        # 前端收到后跳转到增量页面，由用户确认后执行增量同步
        # ============================================================
        from ..db.pg_pool import get_conn

        # 从 data_stats_config 获取该数据集的表名和日期列
        table_name = None
        date_column = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT table_name, date_column FROM market.data_stats_config WHERE data_kind = %s",
                    (dataset_id,),
                )
                row = cur.fetchone()
                if row:
                    table_name, date_column = row

        if not table_name or not date_column:
            raise HTTPException(status_code=400, detail=f"数据集 {dataset_id} 不支持一键补齐（未在 data_stats_config 中注册）")

        # 直接查询该表的 max_date（使用 statement_timeout 防止大表超时）
        current_max_date = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '10s'")
                try:
                    cur.execute(f"SELECT MAX({date_column})::date FROM {table_name}")
                    row = cur.fetchone()
                    if row:
                        current_max_date = row[0]
                except Exception as e:
                    _logger.warning(f"[{dataset_id}] 查询 MAX({date_column}) from {table_name} 失败: {e}")
                    conn.rollback()

        # 获取最新交易日
        latest_trading_date = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(cal_date) AS latest FROM market.trading_calendar "
                    "WHERE is_trading = TRUE AND cal_date <= CURRENT_DATE"
                )
                row = cur.fetchone()
                if row:
                    latest_trading_date = row[0]

        if latest_trading_date is None:
            raise HTTPException(
                status_code=400,
                detail="无法获取最新交易日，请先同步交易日历"
            )

        # 计算缺口
        has_gap = False
        gap_start = None
        gap_end = str(latest_trading_date)

        if current_max_date is None:
            has_gap = True
            gap_start = gap_end
        elif current_max_date < latest_trading_date:
            has_gap = True
            next_trading = None
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT MIN(cal_date) FROM market.trading_calendar "
                        "WHERE is_trading = TRUE AND cal_date > %s",
                        (current_max_date,),
                    )
                    row = cur.fetchone()
                    if row:
                        next_trading = row[0]
            gap_start = str(next_trading) if next_trading else str(current_max_date)

        if not has_gap:
            return {
                "success": True,
                "action": "no_gap",
                "message": f"{dataset_id} 数据已是最新（max_date={current_max_date}，latest_trading={latest_trading_date}）",
                "current_max_date": str(current_max_date),
                "latest_trading_date": str(latest_trading_date),
            }

        # 返回缺口信息，让前端跳转到增量页面
        return {
            "success": True,
            "action": "redirect_to_incremental",
            "dataset_id": dataset_id,
            "current_max_date": str(current_max_date) if current_max_date else None,
            "latest_trading_date": str(latest_trading_date),
            "gap_start": gap_start,
            "gap_end": gap_end,
            "message": f"{dataset_id} 数据缺口: {gap_start} → {gap_end}",
        }

    except HTTPException:
        raise
    except Exception as e:
        _logger.error(f"一键补齐失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/data/tasks", summary="获取所有同步任务")
def get_all_tasks(active_only: bool = False, limit: int = 50) -> Dict[str, Any]:
    return {"tasks": [], "total": 0}


@router.get("/data/tasks/{task_id}/progress", summary="获取任务详细进度")
def get_task_progress_detail(task_id: str) -> Dict[str, Any]:
    """
    获取指定任务的详细进度信息，包括进度、日志、当前处理的股票等。
    """
    try:
        client = _get_client()
        progress = client.get_task_progress(task_id)
        
        if not progress:
            raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在或已过期")
        
        return progress
    except HTTPException:
        raise
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"获取任务进度失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e

