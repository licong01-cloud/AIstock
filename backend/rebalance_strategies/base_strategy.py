"""持仓再平衡策略抽象基类."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Dict, List, Optional


class BaseRebalanceStrategy(ABC):
    """持仓再平衡策略抽象基类.

    平台层（signal_generator）负责：
      - 从 DB 读取 portfolio_config
      - 调用推理管线获取全量评分列表
      - 获取当前持仓、总资产、收盘价
      - 将信号写入 trade_signals 表

    策略层（子类）负责：
      - 根据评分和持仓决定买哪些、卖哪些、各买多少
      - 返回 signal 列表
    """

    def __init__(self, config: Dict[str, Any] | None = None, **kwargs) -> None:
        self.config = config or {}
        # Qlib passes all YAML strategy kwargs to __init__; merge them
        self.config.update(kwargs)

    @abstractmethod
    def generate_orders(
        self,
        score_items: List[Dict[str, Any]],
        current_positions: Dict[str, Dict[str, Any]],
        portfolio_value: float,
        config: Dict[str, Any],
        signal_date: date,
        next_trade_date: date,
        portfolio_id: int,
        close_price_fn,
    ) -> List[Dict[str, Any]]:
        """生成买卖信号.

        Parameters
        ----------
        score_items:
            全量股票评分列表，已按 score 降序排列。
            每条: {symbol, score, rank, ...}
        current_positions:
            当前持仓字典 {symbol: {quantity, avg_cost, market_value, entry_date}}
        portfolio_value:
            当前组合总资产估算（元）
        config:
            portfolio_config 完整字典（来自 DB）
        signal_date:
            信号生成日（T-1）
        next_trade_date:
            信号执行日（T）
        portfolio_id:
            组合 ID
        close_price_fn:
            callable(symbol, date) -> Optional[float]，查最近收盘价

        Returns
        -------
        List of signal dicts, each containing:
            portfolio_id, signal_date, trade_date, symbol, side,
            target_quantity, target_weight, score
        """

    # ── 共享工具方法（供子类调用）──────────────────────────────────

    @staticmethod
    def _calc_target_weight(config: Dict[str, Any]) -> float:
        """计算单只股票目标权重（等权，受 max_position_pct 和 risk_degree 约束）."""
        topk = config.get("max_positions", 20) or 20
        max_pct = float(config.get("max_position_pct", 0.10) or 0.10)
        risk_degree = float(config.get("risk_degree", 0.95) or 0.95)
        equal_weight = 1.0 / topk
        return min(equal_weight, max_pct) * risk_degree

    @staticmethod
    def _calc_quantity(
        total_value: float,
        target_weight: float,
        price: Optional[float],
        lot_size: int = 100,
    ) -> int:
        """根据总资产、目标权重、收盘价估算买入数量（向下取整到整手）."""
        if price and price > 0:
            raw_qty = int(total_value * target_weight / price)
            qty = (raw_qty // lot_size) * lot_size
            return max(qty, lot_size)
        return lot_size  # 价格缺失时默认 1 手
