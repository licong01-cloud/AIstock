"""轻量级策略执行器（QMT 交易系统）.

提供统一的策略信号执行接口，包括：
- 幂等性保护（数据库）
- 基础风控检查
- 统一错误处理
"""
from __future__ import annotations

import hashlib
import threading
from datetime import date
from typing import Any, Dict, Optional

from dotenv import load_dotenv

from .qmt_client import BaseQMTClient, get_qmt_client_singleton
from .risk_control import RiskControlService

load_dotenv(override=True)


class SimpleStrategyExecutor:
    """轻量级策略执行器"""

    def __init__(self, qmt_client: Optional[BaseQMTClient] = None, db_conn=None):
        """初始化策略执行器

        Args:
            qmt_client: QMT 客户端实例（如果为 None，则使用进程级单例）
            db_conn: 数据库连接（如果为 None，则按需获取）
        """
        self.qmt_client = qmt_client or get_qmt_client_singleton()
        self.db_conn = db_conn
        self.risk_control = RiskControlService()
        self._lock = threading.RLock()  # 串行执行锁

    def execute_signal(
        self,
        strategy_id: str,
        symbol: str,
        side: str,  # "BUY" / "SELL"
        quantity: int,
        price_type: str = "LIMIT",  # "LIMIT" / "MARKET"
        price: float = 0.0,
        reason: str = "",
        idempotency_key: Optional[str] = None,
        signal_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """执行交易信号

        Args:
            strategy_id: 策略ID
            symbol: 股票代码（如 "600519.SH"）
            side: 交易方向 "BUY" / "SELL"
            quantity: 数量（股）
            price_type: 价格类型 "LIMIT" / "MARKET"
            price: 价格（限价单时使用）
            reason: 交易原因
            idempotency_key: 幂等键（如果为 None，则自动生成）
            signal_data: 原始信号数据（可选）

        Returns:
            执行结果字典，包含：
            - success: 是否成功
            - order_id: 订单ID（如果成功）
            - message: 消息
            - intent_id: 交易意图ID
        """
        with self._lock:
            try:
                # 1. 生成幂等键
                if idempotency_key is None:
                    idempotency_key = self._generate_idempotency_key(
                        strategy_id, symbol, side
                    )

                # 2. 幂等性检查（查询数据库）
                existing_intent = self._check_idempotency(idempotency_key)
                if existing_intent:
                    if existing_intent.get("status") == "EXECUTED":
                        return {
                            "success": True,
                            "order_id": existing_intent.get("order_id"),
                            "message": "该信号已执行（幂等性保护）",
                            "intent_id": existing_intent.get("id"),
                        }
                    elif existing_intent.get("status") == "EXECUTING":
                        return {
                            "success": False,
                            "message": "该信号正在执行中",
                            "intent_id": existing_intent.get("id"),
                        }

                # 3. 创建交易意图记录（数据库）
                intent_id = self._create_trade_intent(
                    strategy_id=strategy_id,
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    price_type=price_type,
                    price=price,
                    reason=reason,
                    idempotency_key=idempotency_key,
                    signal_data=signal_data,
                )

                # 4. 风控检查
                risk_passed, risk_reason = self._check_risk(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    price=price,
                )

                if not risk_passed:
                    self._update_trade_intent_status(
                        intent_id, "FAILED", error_message=risk_reason
                    )
                    return {
                        "success": False,
                        "message": f"风控检查失败: {risk_reason}",
                        "intent_id": intent_id,
                    }

                # 5. 更新交易意图状态为 EXECUTING
                self._update_trade_intent_status(intent_id, "EXECUTING")

                # 6. 调用 QMT 下单
                order_type = 23 if side == "BUY" else 24  # 23=买入，24=卖出
                price_type_int = self._convert_price_type(price_type)

                order_id, order_msg = self.qmt_client.place_order(
                    stock_code=symbol,
                    order_type=order_type,
                    order_volume=quantity,
                    price_type=price_type_int,
                    price=price,
                    strategy_name=strategy_id,
                    order_remark=reason,
                )

                if order_id <= 0:
                    # 下单失败
                    self._update_trade_intent_status(
                        intent_id, "FAILED", error_message=order_msg
                    )
                    return {
                        "success": False,
                        "message": f"下单失败: {order_msg}",
                        "intent_id": intent_id,
                    }

                # 7. 更新交易意图状态为 EXECUTED
                self._update_trade_intent_status(
                    intent_id,
                    "EXECUTED",
                    order_id=str(order_id),
                )

                return {
                    "success": True,
                    "order_id": order_id,
                    "message": "下单成功",
                    "intent_id": intent_id,
                }

            except Exception as e:
                # 异常处理
                error_msg = str(e)
                if "intent_id" in locals():
                    self._update_trade_intent_status(
                        intent_id, "FAILED", error_message=error_msg
                    )
                return {
                    "success": False,
                    "message": f"执行异常: {error_msg}",
                    "intent_id": intent_id if "intent_id" in locals() else None,
                }

    def _generate_idempotency_key(
        self, strategy_id: str, symbol: str, side: str
    ) -> str:
        """生成幂等键

        格式: {strategy_id}:{date}:{symbol}:{side}:{hash}
        """
        today = date.today().isoformat()
        content = f"{strategy_id}:{today}:{symbol}:{side}"
        hash_value = hashlib.md5(content.encode()).hexdigest()[:8]
        return f"{content}:{hash_value}"

    def _check_idempotency(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        """检查幂等性（查询数据库）"""
        if self.db_conn is None:
            from ..db.pg_pool import get_conn

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, status, order_id, order_sysid
                        FROM trading.trade_intent
                        WHERE idempotency_key = %s
                        LIMIT 1
                        """,
                        (idempotency_key,),
                    )
                    row = cur.fetchone()
                    if row:
                        return {
                            "id": row[0],
                            "status": row[1],
                            "order_id": row[2],
                            "order_sysid": row[3],
                        }
        else:
            with self.db_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, status, order_id, order_sysid
                    FROM trading.trade_intent
                    WHERE idempotency_key = %s
                    LIMIT 1
                    """,
                    (idempotency_key,),
                )
                row = cur.fetchone()
                if row:
                    return {
                        "id": row[0],
                        "status": row[1],
                        "order_id": row[2],
                        "order_sysid": row[3],
                    }
        return None

    def _create_trade_intent(
        self,
        strategy_id: str,
        symbol: str,
        side: str,
        quantity: int,
        price_type: str,
        price: float,
        reason: str,
        idempotency_key: str,
        signal_data: Optional[Dict[str, Any]] = None,
    ) -> int:
        """创建交易意图记录（数据库）"""
        import json

        if self.db_conn is None:
            from ..db.pg_pool import get_conn

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO trading.trade_intent
                        (strategy_id, symbol, side, quantity, price_type, price,
                         reason, idempotency_key, status, signal_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', %s)
                        RETURNING id
                        """,
                        (
                            strategy_id,
                            symbol,
                            side,
                            quantity,
                            price_type,
                            price,
                            reason,
                            idempotency_key,
                            json.dumps(signal_data) if signal_data else None,
                        ),
                    )
                    intent_id = cur.fetchone()[0]
                    conn.commit()
                    return intent_id
        else:
            with self.db_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO trading.trade_intent
                    (strategy_id, symbol, side, quantity, price_type, price,
                     reason, idempotency_key, status, signal_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', %s)
                    RETURNING id
                    """,
                    (
                        strategy_id,
                        symbol,
                        side,
                        quantity,
                        price_type,
                        price,
                        reason,
                        idempotency_key,
                        json.dumps(signal_data) if signal_data else None,
                    ),
                )
                intent_id = cur.fetchone()[0]
                self.db_conn.commit()
                return intent_id

    def _update_trade_intent_status(
        self,
        intent_id: int,
        status: str,
        order_id: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """更新交易意图状态"""
        from datetime import datetime

        if self.db_conn is None:
            from ..db.pg_pool import get_conn

            with get_conn() as conn:
                with conn.cursor() as cur:
                    if status == "EXECUTED":
                        cur.execute(
                            """
                            UPDATE trading.trade_intent
                            SET status = %s, order_id = %s, executed_at = %s
                            WHERE id = %s
                            """,
                            (status, order_id, datetime.now(), intent_id),
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE trading.trade_intent
                            SET status = %s, error_message = %s
                            WHERE id = %s
                            """,
                            (status, error_message, intent_id),
                        )
                    conn.commit()
        else:
            with self.db_conn.cursor() as cur:
                if status == "EXECUTED":
                    cur.execute(
                        """
                        UPDATE trading.trade_intent
                        SET status = %s, order_id = %s, executed_at = %s
                        WHERE id = %s
                        """,
                        (status, order_id, datetime.now(), intent_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE trading.trade_intent
                        SET status = %s, error_message = %s
                        WHERE id = %s
                        """,
                        (status, error_message, intent_id),
                    )
                self.db_conn.commit()

    def _check_risk(
        self, symbol: str, side: str, quantity: int, price: float
    ) -> tuple[bool, str]:
        """执行风控检查"""
        if side == "BUY":
            account_info = self.qmt_client.get_account_info()
            return self.risk_control.check_buy_signal(
                symbol, quantity, price, account_info
            )
        else:  # SELL
            positions = self.qmt_client.get_positions()
            return self.risk_control.check_sell_signal(symbol, quantity, positions)

    def _convert_price_type(self, price_type: str) -> int:
        """转换价格类型字符串为 xtquant 常量值"""
        price_type_map = {
            "LIMIT": 11,  # FIX_PRICE
            "MARKET": 5,  # LATEST_PRICE
            "MARKET_PEER_PRICE_FIRST": 44,
            "MARKET_MINE_PRICE_FIRST": 45,
        }
        return price_type_map.get(price_type.upper(), 11)

