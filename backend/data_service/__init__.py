"""Core data service package for AIstock.

This package provides a unified data access layer for:
- xtquant (行情)
- miniQMT (交易)
- TimescaleDB (历史数据)
- TDX 本地数据
- Tushare 在线数据

Only define stable interfaces here; concrete implementations live in the
corresponding adapter modules.
"""

__all__ = [
    "api",
]
