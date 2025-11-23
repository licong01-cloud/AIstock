"""next_app 专用的统一数据访问封装层。

当前版本已经在 ``next_app.backend.core.unified_data_access_impl`` 中
实现了完整的 ``UnifiedDataAccess``，因此这里不再依赖项目根目录
的 ``unified_data_access.py``，避免 sys.path hack 和跨应用耦合。

上层只需要通过 ``NextUnifiedDataAccess`` 访问统一数据接口即可。
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from ..core.unified_data_access_impl import UnifiedDataAccess


class NextUnifiedDataAccess:
    """新后端使用的统一数据访问入口。

    暂时委托给旧应用的 UnifiedDataAccess，实现层解耦在本类内部，
    方便未来完全移除对旧模块的依赖而不改动上层业务代码。
    """

    def __init__(self) -> None:
        self._uda = UnifiedDataAccess()

    def get_stock_info(self, symbol: str, analysis_date: Optional[str] = None) -> Dict[str, Any]:
        return self._uda.get_stock_info(symbol, analysis_date=analysis_date)

    def get_realtime_quotes(self, symbol: str) -> Dict[str, Any]:
        """获取实时行情（通常由 TDX 接口提供）。"""

        return self._uda.get_realtime_quotes(symbol)

    def get_stock_data(self, symbol: str, period: str = "1y", analysis_date: Optional[str] = None):
        """兼容旧接口的封装，优先尝试带 analysis_date 的新签名。"""

        try:
            return self._uda.get_stock_data(symbol, period, analysis_date=analysis_date)
        except TypeError as e:
            if "analysis_date" in str(e):
                return self._uda.get_stock_data(symbol, period)
            raise

    def get_financial_data(self, symbol: str, analysis_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return self._uda.get_financial_data(symbol, analysis_date=analysis_date)

    def get_fund_flow_data(self, symbol: str, analysis_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return self._uda.get_fund_flow_data(symbol, analysis_date=analysis_date)

    def get_risk_data(self, symbol: str, analysis_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return self._uda.get_risk_data(symbol, analysis_date=analysis_date)

    def get_market_sentiment_data(
        self,
        symbol: str,
        stock_data_with_indicators,
        analysis_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取市场情绪数据，直接代理 core.UnifiedDataAccess 实现。"""

        return self._uda.get_market_sentiment_data(
            symbol, stock_data_with_indicators, analysis_date=analysis_date
        )

    def get_news_data(
        self,
        symbol: str,
        analysis_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取新闻数据，直接代理 core.UnifiedDataAccess 实现。"""

        return self._uda.get_news_data(symbol, analysis_date=analysis_date)

    def get_stock_news(
        self,
        symbol: str,
        analysis_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取股票新闻（别名方法，兼容旧接口）。"""

        return self.get_news_data(symbol, analysis_date=analysis_date)

    def get_research_reports_data(
        self,
        symbol: str,
        days: int = 180,
        analysis_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取机构研报数据，直接代理 core.UnifiedDataAccess 实现。"""

        return self._uda.get_research_reports_data(
            symbol,
            days=days,
            analysis_date=analysis_date,
        )

    def get_announcement_data(
        self,
        symbol: str,
        days: int = 30,
        analysis_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取上市公司公告数据，直接代理 core.UnifiedDataAccess 实现。"""

        return self._uda.get_announcement_data(
            symbol,
            days=days,
            analysis_date=analysis_date,
        )

    def get_chip_distribution_data(
        self,
        symbol: str,
        trade_date: str | None = None,
        current_price: float | None = None,
        analysis_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取筹码分布数据（cyq_perf + cyq_chips），直接代理 core.UnifiedDataAccess 实现。"""

        return self._uda.get_chip_distribution_data(
            symbol,
            trade_date=trade_date,
            current_price=current_price,
            analysis_date=analysis_date,
        )

    # 暴露技术指标相关的辅助对象，便于上层复用现有算法
    @property
    def stock_data_fetcher(self):  # type: ignore[override]
        return self._uda.stock_data_fetcher
