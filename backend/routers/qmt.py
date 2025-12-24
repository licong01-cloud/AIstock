from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from ..infra.qmt_client import BaseQMTClient, QMTNotAvailableError, build_qmt_client_from_env


router = APIRouter(prefix="/qmt", tags=["qmt"])

# Process-wide singleton. For the "直连" phase this is sufficient.
# If you update .env at runtime, call /qmt/reload to rebuild.
# 延迟初始化：不在模块导入时创建客户端，避免启动失败
_client: Optional[BaseQMTClient] = None


def _get_client() -> BaseQMTClient:
    """获取QMT客户端实例（延迟初始化）
    
    第一次调用时会创建客户端实例。如果配置错误，会抛出明确的异常。
    """
    global _client
    if _client is None:
        try:
            _client = build_qmt_client_from_env()
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
    return _client


@router.post("/reload", summary="重新加载 QMT 配置并重建客户端")
async def reload_client() -> Dict[str, Any]:
    global _client
    try:
        _client = None  # 清除旧客户端
        _client = build_qmt_client_from_env()
        return {"ok": True, "status": _get_client().status().__dict__}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"重新加载QMT配置失败: {e}\n请检查 .env 文件中的配置。"
        ) from e


@router.get("/status", summary="QMT/xtquant 连接状态")
async def get_status() -> Dict[str, Any]:
    return _get_client().status().__dict__


@router.post("/connect", summary="连接 QMT（模拟盘/实盘取决于账户与 MINIQMT_MODE）")
async def connect() -> Dict[str, Any]:
    client = _get_client()
    ok, msg = client.connect()
    return {"success": bool(ok), "message": msg, "status": client.status().__dict__}


@router.post("/disconnect", summary="断开 QMT 连接")
async def disconnect() -> Dict[str, Any]:
    client = _get_client()
    ok, msg = client.disconnect()
    return {"success": bool(ok), "message": msg, "status": client.status().__dict__}


@router.get("/account", summary="获取 QMT 资金信息（快照）")
async def get_account_info() -> Dict[str, Any]:
    try:
        return _get_client().get_account_info()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/positions", summary="获取 QMT 持仓列表（快照）")
async def get_positions() -> List[Dict[str, Any]]:
    try:
        return _get_client().get_positions()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/snapshot", summary="获取 QMT 资金+持仓组合快照")
async def get_snapshot() -> Dict[str, Any]:
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
async def get_orders(cancelable_only: bool = False) -> List[Dict[str, Any]]:
    try:
        return _get_client().get_orders(cancelable_only=cancelable_only)
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/trades", summary="获取 QMT 当日成交列表")
async def get_trades() -> List[Dict[str, Any]]:
    try:
        return _get_client().get_trades()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/order", summary="股票下单")
async def place_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    """股票下单接口
    
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

        order_id, message = _get_client().place_order(
            stock_code=stock_code,
            order_type=int(order_type),
            order_volume=int(order_volume),
            price_type=int(price_type),
            price=price,
            strategy_name=strategy_name,
            order_remark=order_remark,
        )

        return {
            "success": order_id > 0,
            "order_id": order_id,
            "message": message,
        }
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"下单失败: {e!r}") from e


@router.post("/cancel", summary="撤单")
async def cancel_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    """撤单接口
    
    参数（二选一）：
    - order_id: 订单编号
    或
    - market: 交易市场（0=上海，1=深圳）
    - order_sysid: 柜台合同编号
    """
    try:
        order_id = payload.get("order_id")
        market = payload.get("market")
        order_sysid = payload.get("order_sysid")

        if order_id:
            success, message = _get_client().cancel_order(str(order_id))
        elif market is not None and order_sysid:
            success, message = _get_client().cancel_order_by_sysid(int(market), str(order_sysid))
        else:
            raise HTTPException(status_code=400, detail="请提供 order_id 或 (market, order_sysid)")

        return {
            "success": success,
            "message": message,
        }
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"撤单失败: {e!r}") from e


@router.post("/order/batch", summary="批量下单")
async def batch_place_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    """批量下单接口
    
    参数：
    - orders: 订单列表，每个订单包含下单接口的所有参数
    """
    try:
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
        }
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批量下单失败: {e!r}") from e


@router.get("/ipo/limit", summary="查询新股申购额度")
async def get_new_purchase_limit() -> Dict[str, Any]:
    try:
        return _get_client().query_new_purchase_limit()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/ipo/list", summary="查询新股信息")
async def get_ipo_data() -> List[Dict[str, Any]]:
    try:
        return _get_client().query_ipo_data()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/bank/transfer-in", summary="银行转证券")
async def bank_transfer_in(payload: Dict[str, Any]) -> Dict[str, Any]:
    """银行转证券
    
    参数：
    - bank_no: 银行编号
    - bank_account: 银行账号
    - bank_pwd: 银行密码
    - amount: 转账金额
    """
    try:
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
async def bank_transfer_out(payload: Dict[str, Any]) -> Dict[str, Any]:
    """证券转银行
    
    参数：
    - bank_no: 银行编号
    - bank_account: 银行账号
    - bank_pwd: 银行密码
    - amount: 转账金额
    """
    try:
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
async def get_bank_info() -> List[Dict[str, Any]]:
    try:
        return _get_client().query_bank_info()
    except QMTNotAvailableError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


