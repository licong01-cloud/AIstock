"""
QE Unified Engine — MetricsNormalizer

唯一的指标映射定义，消除 process_completed_loop 和
process_strategy_evo_completed_loop 中完全重复的 _METRIC_ALIASES 字典。
"""
from __future__ import annotations

from typing import Any

# 唯一的指标映射定义 — 所有路径共享
# 与 qe_evolution_service.py:1212-1222 和 3116-3126 完全一致
METRIC_ALIASES: dict[str, str] = {
    "Rank IC": "Rank_IC",
    "IC": "IC",
    "1day.excess_return_with_cost.information_ratio": "sharpe",
    "1day.excess_return_with_cost.annualized_return": "annualized_return",
    "1day.excess_return_without_cost.annualized_return": "annualized_return_no_cost",
    "1day.excess_return_with_cost.max_drawdown": "max_drawdown",
    "1day.excess_return_without_cost.max_drawdown": "max_drawdown_no_cost",
    "1day.excess_return_without_cost.information_ratio": "sharpe_no_cost",
    "1day.excess_return_with_cost.mean": "daily_return",
    "1day.excess_return_without_cost.mean": "daily_return_no_cost",
}


def normalize_metrics(raw_metrics: dict[str, Any]) -> dict[str, Any]:
    """将 QLib 原始指标键名转换为前端友好的短键名。

    与现有代码行为完全一致：
    - 原始长键保留（不删除）
    - 短键仅在不存在时才写入（避免覆盖已有短键）
    """
    result = dict(raw_metrics)
    for src, dst in METRIC_ALIASES.items():
        if src in result and dst not in result:
            result[dst] = result[src]
    return result
