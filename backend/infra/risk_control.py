"""基础风控服务（QMT 交易系统）.

提供买入/卖出信号的基础风控检查。
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple


class RiskControlService:
    """基础风控服务"""

    def __init__(self):
        pass

    def check_buy_signal(
        self,
        symbol: str,
        quantity: int,
        price: float,
        account_info: Dict[str, Any],
    ) -> Tuple[bool, str]:
        """检查买入信号是否通过风控

        Args:
            symbol: 股票代码（如 "600519.SH"）
            quantity: 买入数量（股）
            price: 买入价格
            account_info: 账户信息（包含 available_cash 等）

        Returns:
            (passed, reason): 是否通过，失败原因
        """
        # 1. 基本参数检查
        if quantity <= 0:
            return False, "买入数量必须大于0"

        if price <= 0:
            return False, "买入价格必须大于0"

        # 2. 股票代码格式检查
        if not self._validate_symbol(symbol):
            return False, f"股票代码格式错误: {symbol}"

        # 3. 资金检查
        required_amount = quantity * price
        available_cash = account_info.get("available_cash", 0.0)

        if available_cash < required_amount:
            return False, (
                f"可用资金不足: 需要 {required_amount:.2f}，"
                f"可用 {available_cash:.2f}"
            )

        return True, "通过"

    def check_sell_signal(
        self,
        symbol: str,
        quantity: int,
        positions: List[Dict[str, Any]],
    ) -> Tuple[bool, str]:
        """检查卖出信号是否通过风控

        Args:
            symbol: 股票代码（如 "600519.SH"）
            quantity: 卖出数量（股）
            positions: 持仓列表

        Returns:
            (passed, reason): 是否通过，失败原因
        """
        # 1. 基本参数检查
        if quantity <= 0:
            return False, "卖出数量必须大于0"

        # 2. 股票代码格式检查
        if not self._validate_symbol(symbol):
            return False, f"股票代码格式错误: {symbol}"

        # 3. 持仓检查
        position = self._find_position(symbol, positions)
        if position is None:
            return False, f"未持有股票: {symbol}"

        # 4. 可卖数量检查
        can_sell = position.get("can_sell", 0)
        if can_sell < quantity:
            return False, (
                f"可卖数量不足: 需要 {quantity}，"
                f"可卖 {can_sell}"
            )

        return True, "通过"

    def _validate_symbol(self, symbol: str) -> bool:
        """验证股票代码格式

        格式: {code}.{market}
        例如: 600519.SH, 000001.SZ
        """
        if not symbol or "." not in symbol:
            return False

        parts = symbol.split(".")
        if len(parts) != 2:
            return False

        code, market = parts
        if not code.isdigit():
            return False

        if market not in ("SH", "SZ"):
            return False

        return True

    def _find_position(self, symbol: str, positions: List[Dict[str, Any]]) -> Dict[str, Any] | None:
        """在持仓列表中查找指定股票"""
        for pos in positions:
            if pos.get("stock_code") == symbol:
                return pos
        return None

