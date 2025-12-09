from __future__ import annotations

"""Qlib 数据导出配置.

复权策略说明：
- 使用不复权价格 + 复权因子的方式
- $close = 不复权价格(元) × 前复权因子
- $factor = 前复权因子 = adj_factor / 最新adj_factor
- 原始价格 = $close / $factor

数据格式：
- 价格单位：元（数据库存储为厘，需要 /1000）
- 数据类型：float32（Qlib 标准）
- instrument 格式：SH600000（交易所前缀 + 代码）
"""

from pathlib import Path
from typing import Dict

from app_pg import get_conn  # type: ignore[attr-defined]

# Snapshot 根目录（可改为从环境变量或集中配置读取）
QLIB_SNAPSHOT_ROOT = Path("qlib_snapshots")

# 市场标识（供将来兼容 Qlib 使用）
QLIB_MARKET = "aistock"

# =========================================================================
# 数据库表配置
# =========================================================================

# 不复权日线表（主要数据源）
DAILY_RAW_TABLE = "market.kline_daily_raw"

# 前复权日线表（兼容旧逻辑，逐步废弃）
DAILY_QFQ_TABLE = "market.kline_daily_qfq"

# 复权因子表（Tushare adj_factor）
# 如果表不存在，将使用 Tushare API 实时获取
ADJ_FACTOR_TABLE = "market.adj_factor"

# 分钟线表名（原始 1m 行情）
MINUTE_RAW_TABLE = "market.kline_minute_raw"
MINUTE_QFQ_TABLE = "market.kline_minute_raw"  # 兼容旧配置

# TDX 板块相关表
TDX_BOARD_INDEX_TABLE = "market.tdx_board_index"
TDX_BOARD_MEMBER_TABLE = "market.tdx_board_member"
TDX_BOARD_DAILY_TABLE = "market.tdx_board_daily"

# 因子数据表（用于存储自定义因子）
FACTOR_DATA_TABLE = "market.qlib_factors"

# =========================================================================
# 价格单位转换
# =========================================================================

# 数据库价格单位：厘（元 × 1000）
PRICE_UNIT_DIVISOR = 1000.0

# RD-Agent 因子数据格式字段映射
FIELD_MAPPING_FACTOR: Dict[str, str] = {
    "datetime": "trade_date",
    "$open": "open_li",
    "$close": "close_li",
    "$high": "high_li",
    "$low": "low_li",
    "$volume": "volume_hand",
    "$factor": "adj_factor",  # 复权因子
}

# 从 DB 列名 -> 逻辑字段名 的映射配置（按频率区分）
FIELD_MAPPING_DB_DAILY: Dict[str, str] = {
    "datetime": "trade_date",
    "open": "open_li",
    "high": "high_li",
    "low": "low_li",
    "close": "close_li",
    "volume": "volume_hand",
    "amount": "amount_li",
}

FIELD_MAPPING_DB_MINUTE: Dict[str, str] = {
    "datetime": "trade_time",
    "open": "open_li",
    "high": "high_li",
    "low": "low_li",
    "close": "close_li",
    "volume": "volume_hand",
    "amount": "amount_li",
}


def ensure_snapshot_root() -> Path:
    """确保 Snapshot 根目录存在并返回路径."""

    QLIB_SNAPSHOT_ROOT.mkdir(parents=True, exist_ok=True)
    return QLIB_SNAPSHOT_ROOT
