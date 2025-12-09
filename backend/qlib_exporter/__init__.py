from __future__ import annotations

"""Qlib 数据导出模块.

该包负责：
- 从本地数据库读取不复权价格数据；
- 获取复权因子（本地数据库或 Tushare API）；
- 计算 Qlib 格式数据（$close = 不复权价格 × 前复权因子）；
- 导出为 Qlib/RD-Agent 可使用的 Snapshot（如 daily_pv.h5 宽表）；
- 提供 FastAPI 路由供前端触发导出与监控进度。

复权策略：
- $close = 不复权价格(元) × 前复权因子
- $factor = 前复权因子 = adj_factor / 最新adj_factor
- 原始价格 = $close / $factor

支持的数据类型：
- 日线数据（股票）
- 分钟线数据（股票）
- 板块数据（TDX）

具体设计详见 docs/qlib_daily_qfq_design.md。
"""

__all__ = [
    "config",
    "db_reader",
    "adj_factor_provider",
    "meta_repo",
    "snapshot_writer",
    "exporter",
]
