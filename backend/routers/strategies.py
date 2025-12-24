"""策略交易 API 路由（QMT 交易系统）"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..infra.strategy_executor import SimpleStrategyExecutor, build_qmt_client_from_env

router = APIRouter(prefix="/api/v1/strategies", tags=["strategies"])


class ExecuteSignalRequest(BaseModel):
    """执行交易信号请求"""

    strategy_id: str = Field(..., description="策略ID")
    symbol: str = Field(..., description="股票代码（如 600519.SH）")
    side: str = Field(..., description="交易方向：BUY / SELL")
    quantity: int = Field(..., gt=0, description="数量（股）")
    price_type: str = Field(
        default="LIMIT", description="价格类型：LIMIT / MARKET"
    )
    price: float = Field(default=0.0, ge=0, description="价格（限价单时使用）")
    reason: str = Field(default="", description="交易原因")
    idempotency_key: Optional[str] = Field(
        default=None, description="幂等键（可选）"
    )
    signal_data: Optional[Dict[str, Any]] = Field(
        default=None, description="原始信号数据（可选）"
    )


@router.post("/execute", summary="执行交易信号")
def execute_signal(request: ExecuteSignalRequest) -> Dict[str, Any]:
    """执行交易信号（策略调用）

    此接口供策略调用，执行交易信号。
    包含幂等性保护、风控检查等功能。
    """
    try:
        executor = SimpleStrategyExecutor()

        result = executor.execute_signal(
            strategy_id=request.strategy_id,
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            price_type=request.price_type,
            price=request.price,
            reason=request.reason,
            idempotency_key=request.idempotency_key,
            signal_data=request.signal_data,
        )

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"执行失败: {str(e)}")


@router.get("/list", summary="获取策略列表")
def list_strategies(enabled_only: bool = Query(False, description="是否只返回启用的策略")) -> Dict[str, Any]:
    """获取策略列表（从数据库查询）

    Args:
        enabled_only: 是否只返回启用的策略
    """
    try:
        from ..db.pg_pool import get_conn
        import json

        with get_conn() as conn:
            with conn.cursor() as cur:
                if enabled_only:
                    cur.execute(
                        """
                        SELECT strategy_id, strategy_name, strategy_type,
                               description, enabled, config_json, schedule_config,
                               risk_config, created_at, updated_at
                        FROM trading.strategy_config
                        WHERE enabled = TRUE
                        ORDER BY created_at DESC
                        """
                    )
                else:
                    cur.execute(
                        """
                        SELECT strategy_id, strategy_name, strategy_type,
                               description, enabled, config_json, schedule_config,
                               risk_config, created_at, updated_at
                        FROM trading.strategy_config
                        ORDER BY created_at DESC
                        """
                    )

                rows = cur.fetchall()
                strategies = []
                for row in rows:
                    try:
                        # PostgreSQL JSONB 字段可能已经是 dict，需要判断类型
                        def parse_jsonb(value):
                            if isinstance(value, dict):
                                return value
                            elif isinstance(value, str):
                                return json.loads(value) if value else {}
                            else:
                                return {}
                        
                        strategies.append(
                            {
                                "strategy_id": row[0],
                                "strategy_name": row[1],
                                "strategy_type": row[2],
                                "description": row[3] or "",
                                "enabled": row[4],
                                "config": parse_jsonb(row[5]),
                                "schedule_config": parse_jsonb(row[6]),
                                "risk_config": parse_jsonb(row[7]),
                                "created_at": row[8].isoformat() if row[8] else None,
                                "updated_at": row[9].isoformat() if row[9] else None,
                            }
                        )
                    except Exception as e:
                        # 跳过有问题的记录，记录错误但继续处理其他记录
                        import logging
                        logging.error(f"解析策略配置失败 {row[0] if row else 'unknown'}: {e}", exc_info=True)
                        continue

                return {"success": True, "strategies": strategies, "total": len(strategies)}

    except Exception as e:
        import logging
        import traceback
        logging.error(f"查询策略列表失败: {e}\n{traceback.format_exc()}")
        # 如果表不存在，返回空列表而不是错误
        if "does not exist" in str(e).lower() or "relation" in str(e).lower():
            return {"success": True, "strategies": [], "total": 0, "message": "数据库表尚未初始化，请先运行数据库初始化脚本"}
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


class StrategyConfigRequest(BaseModel):
    """策略配置请求"""

    strategy_id: str = Field(..., description="策略ID（唯一标识）")
    strategy_name: str = Field(..., description="策略名称")
    strategy_type: str = Field(..., description="策略类型：MA_CROSS, TREND_FOLLOWING, GRID, etc.")
    description: Optional[str] = Field(default="", description="策略描述")
    enabled: bool = Field(default=True, description="是否启用")
    config_json: Dict[str, Any] = Field(..., description="策略参数配置（JSON格式）")
    schedule_config: Optional[Dict[str, Any]] = Field(default=None, description="调度配置")
    risk_config: Optional[Dict[str, Any]] = Field(default=None, description="风控配置")


@router.post("/config", summary="创建或更新策略配置")
def create_or_update_strategy_config(request: StrategyConfigRequest) -> Dict[str, Any]:
    """创建或更新策略配置"""
    try:
        from ..db.pg_pool import get_conn
        import json
        from datetime import datetime

        with get_conn() as conn:
            with conn.cursor() as cur:
                # 检查是否存在
                cur.execute(
                    """
                    SELECT id FROM trading.strategy_config
                    WHERE strategy_id = %s
                    """,
                    (request.strategy_id,),
                )
                exists = cur.fetchone()

                if exists:
                    # 更新
                    cur.execute(
                        """
                        UPDATE trading.strategy_config
                        SET strategy_name = %s, strategy_type = %s, description = %s,
                            enabled = %s, config_json = %s, schedule_config = %s,
                            risk_config = %s, updated_at = %s
                        WHERE strategy_id = %s
                        RETURNING id
                        """,
                        (
                            request.strategy_name,
                            request.strategy_type,
                            request.description,
                            request.enabled,
                            json.dumps(request.config_json),
                            json.dumps(request.schedule_config) if request.schedule_config else None,
                            json.dumps(request.risk_config) if request.risk_config else None,
                            datetime.now(),
                            request.strategy_id,
                        ),
                    )
                    config_id = cur.fetchone()[0]
                    conn.commit()
                    return {"success": True, "message": "策略配置已更新", "id": config_id}
                else:
                    # 创建
                    cur.execute(
                        """
                        INSERT INTO trading.strategy_config
                        (strategy_id, strategy_name, strategy_type, description,
                         enabled, config_json, schedule_config, risk_config)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            request.strategy_id,
                            request.strategy_name,
                            request.strategy_type,
                            request.description,
                            request.enabled,
                            json.dumps(request.config_json),
                            json.dumps(request.schedule_config) if request.schedule_config else None,
                            json.dumps(request.risk_config) if request.risk_config else None,
                        ),
                    )
                    config_id = cur.fetchone()[0]
                    conn.commit()
                    return {"success": True, "message": "策略配置已创建", "id": config_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"操作失败: {str(e)}")


@router.delete("/config/{strategy_id}", summary="删除策略配置")
def delete_strategy_config(strategy_id: str) -> Dict[str, Any]:
    """删除策略配置"""
    try:
        from ..db.pg_pool import get_conn

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM trading.strategy_config
                    WHERE strategy_id = %s
                    RETURNING id
                    """,
                    (strategy_id,),
                )
                deleted = cur.fetchone()
                conn.commit()

                if deleted:
                    return {"success": True, "message": "策略配置已删除"}
                else:
                    raise HTTPException(status_code=404, detail="策略配置不存在")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除失败: {str(e)}")


@router.patch("/config/{strategy_id}/enable", summary="启用/禁用策略")
def toggle_strategy_enabled(strategy_id: str, enabled: bool = Query(..., description="是否启用")) -> Dict[str, Any]:
    """启用或禁用策略"""
    try:
        from ..db.pg_pool import get_conn
        from datetime import datetime

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE trading.strategy_config
                    SET enabled = %s, updated_at = %s
                    WHERE strategy_id = %s
                    RETURNING id
                    """,
                    (enabled, datetime.now(), strategy_id),
                )
                updated = cur.fetchone()
                conn.commit()

                if updated:
                    return {"success": True, "message": f"策略已{'启用' if enabled else '禁用'}"}
                else:
                    raise HTTPException(status_code=404, detail="策略配置不存在")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"操作失败: {str(e)}")


@router.get("/executions", summary="获取策略执行记录")
def get_strategy_executions(
    strategy_id: Optional[str] = Query(None, description="策略ID（可选）"),
    limit: int = Query(100, ge=1, le=1000, description="返回数量限制"),
) -> Dict[str, Any]:
    """获取策略执行记录"""
    try:
        from ..db.pg_pool import get_conn
        import json

        with get_conn() as conn:
            with conn.cursor() as cur:
                if strategy_id:
                    cur.execute(
                        """
                        SELECT id, strategy_id, execution_type, trigger_source,
                               status, start_time, end_time, duration_seconds,
                               symbols_processed, signals_generated, signals_executed,
                               error_message, metrics_json
                        FROM trading.strategy_execution
                        WHERE strategy_id = %s
                        ORDER BY start_time DESC
                        LIMIT %s
                        """,
                        (strategy_id, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, strategy_id, execution_type, trigger_source,
                               status, start_time, end_time, duration_seconds,
                               symbols_processed, signals_generated, signals_executed,
                               error_message, metrics_json
                        FROM trading.strategy_execution
                        ORDER BY start_time DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )

                rows = cur.fetchall()
                executions = []
                for row in rows:
                    executions.append(
                        {
                            "id": row[0],
                            "strategy_id": row[1],
                            "execution_type": row[2],
                            "trigger_source": row[3],
                            "status": row[4],
                            "start_time": row[5].isoformat() if row[5] else None,
                            "end_time": row[6].isoformat() if row[6] else None,
                            "duration_seconds": row[7],
                            "symbols_processed": row[8],
                            "signals_generated": row[9],
                            "signals_executed": row[10],
                            "error_message": row[11],
                            "metrics": json.loads(row[12]) if row[12] else {},
                        }
                    )

                return {"success": True, "executions": executions, "total": len(executions)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/intents", summary="获取交易意图记录")
def get_trade_intents(
    strategy_id: Optional[str] = Query(None, description="策略ID（可选）"),
    status: Optional[str] = Query(None, description="状态过滤（可选）"),
    limit: int = Query(100, ge=1, le=1000, description="返回数量限制"),
) -> Dict[str, Any]:
    """获取交易意图记录"""
    try:
        from ..db.pg_pool import get_conn
        import json

        with get_conn() as conn:
            with conn.cursor() as cur:
                conditions = []
                params = []

                if strategy_id:
                    conditions.append("strategy_id = %s")
                    params.append(strategy_id)

                if status:
                    conditions.append("status = %s")
                    params.append(status)

                where_clause = " AND ".join(conditions) if conditions else "1=1"
                params.append(limit)

                cur.execute(
                    f"""
                    SELECT id, strategy_id, symbol, side, quantity, price_type, price,
                           reason, status, order_id, order_sysid, error_message,
                           created_at, executed_at, signal_data
                    FROM trading.trade_intent
                    WHERE {where_clause}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    params,
                )

                rows = cur.fetchall()
                intents = []
                for row in rows:
                    intents.append(
                        {
                            "id": row[0],
                            "strategy_id": row[1],
                            "symbol": row[2],
                            "side": row[3],
                            "quantity": row[4],
                            "price_type": row[5],
                            "price": float(row[6]) if row[6] else None,
                            "reason": row[7],
                            "status": row[8],
                            "order_id": row[9],
                            "order_sysid": row[10],
                            "error_message": row[11],
                            "created_at": row[12].isoformat() if row[12] else None,
                            "executed_at": row[13].isoformat() if row[13] else None,
                            "signal_data": json.loads(row[14]) if row[14] else {},
                        }
                    )

                return {"success": True, "intents": intents, "total": len(intents)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")

