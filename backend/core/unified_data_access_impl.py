"""Unified data access implementation for next_app.

This module re-implements the subset of legacy UnifiedDataAccess that the
new FastAPI backend actually uses, so that next_app no longer needs to
import the root-level unified_data_access module.

The goal is to preserve behaviour and result structure as much as
possible while keeping the implementation self-contained under next_app.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, List, Tuple
import os
import time as time_module
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import re
import zipfile

import pandas as pd
import requests

from .data_source_manager_impl import data_source_manager
from ..infra.network_optimizer import network_optimizer
from ..infra.debug_logger import debug_logger


class UnifiedDataAccess:
    """Unified data access facade used by next_app.

    This class mirrors the public API that the new backend depends on:

    - get_stock_info
    - get_stock_data
    - get_realtime_quotes
    - get_financial_data
    - get_fund_flow_data
    - get_risk_data
    - stock_data_fetcher (for technical indicators)

    Internally it still talks to the same low-level data_source_manager
    and helper fetcher classes as the legacy implementation, but the
    orchestration code lives entirely inside next_app.
    """

    def __init__(self) -> None:
        """Initialise unified data access.

        This follows the original implementation: it exposes a
        StockDataFetcher instance via stock_data_fetcher and prepares
        basic DB / API configuration used by some methods.
        """

        from .stock_data_impl import StockDataFetcher

        self.stock_data_fetcher = StockDataFetcher()

        # Local DB config for minute / weekly tables, shared with ingest
        # scripts. Kept for compatibility even if some methods are not
        # used yet by next_app.
        self._db_cfg: Dict[str, Any] = dict(
            host=os.getenv("TDX_DB_HOST", "localhost"),
            port=int(os.getenv("TDX_DB_PORT", "5432")),
            user=os.getenv("TDX_DB_USER", "postgres"),
            password=os.getenv("TDX_DB_PASSWORD", ""),
            dbname=os.getenv("TDX_DB_NAME", "aistock"),
        )
        self._tdx_api_base = os.getenv("TDX_API_BASE", "http://localhost:8080")

    # ------------------------------------------------------------------
    # 基础代理：直接走数据源管理器
    # ------------------------------------------------------------------

    def get_stock_hist_data(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adjust: str = "qfq",
    ):
        """Proxy to data_source_manager.get_stock_hist_data.

        This keeps the same signature and behaviour as the legacy
        implementation.
        """

        return data_source_manager.get_stock_hist_data(symbol, start_date, end_date, adjust)

    def get_stock_basic_info(self, symbol: str) -> Dict[str, Any]:
        """Get basic stock info via data_source_manager."""

        return data_source_manager.get_stock_basic_info(symbol)

    # ------------------------------------------------------------------
    # 股票信息与历史数据
    # ------------------------------------------------------------------

    def get_stock_info(self, symbol: str, analysis_date: Optional[str] = None) -> Dict[str, Any]:
        """获取股票完整信息（包含基本信息、实时行情、估值指标等）.

        This is a direct adaptation of the legacy implementation with the
        same logging and fallback logic, so that upstream analysis
        behaves consistently.
        """

        debug_logger.info(
            "get_stock_info开始",
            symbol=symbol,
            analysis_date=analysis_date,
            method="get_stock_info",
        )

        # 获取基本信息
        info = self.get_stock_basic_info(symbol)
        if not info:
            info = {
                "symbol": symbol,
                "name": "未知",
                "industry": "未知",
                "market": "未知",
            }

        # 初始化估值和行情字段
        info.setdefault("current_price", "N/A")
        info.setdefault("change_percent", "N/A")
        info.setdefault("pe_ratio", "N/A")
        info.setdefault("pb_ratio", "N/A")
        info.setdefault("market_cap", "N/A")
        info.setdefault("dividend_yield", "N/A")
        info.setdefault("ps_ratio", "N/A")
        info.setdefault("beta", "N/A")
        info.setdefault("52_week_high", "N/A")
        info.setdefault("52_week_low", "N/A")
        info.setdefault("open_price", "N/A")
        info.setdefault("high_price", "N/A")
        info.setdefault("low_price", "N/A")
        info.setdefault("pre_close", "N/A")
        info.setdefault("volume", "N/A")
        info.setdefault("amount", "N/A")
        info.setdefault("quote_source", "N/A")
        info.setdefault("quote_timestamp", "N/A")

        # 优先使用 Tushare 获取实时行情和估值数据
        if data_source_manager.tushare_available:
            try:
                debug_logger.debug(
                    "尝试从Tushare获取实时行情和估值",
                    symbol=symbol,
                    analysis_date=analysis_date,
                )
                ts_code = data_source_manager._convert_to_ts_code(symbol)

                # 根据日期和时间判断，获取合适的交易日
                trade_date = self._get_appropriate_trade_date(analysis_date=analysis_date)
                debug_logger.debug(
                    "选择的交易日",
                    trade_date=trade_date,
                    symbol=symbol,
                    analysis_date=analysis_date,
                )

                try:
                    # 获取 daily_basic（包含市盈率、市净率、市值等）
                    with network_optimizer.apply():
                        daily_basic = data_source_manager.tushare_api.daily_basic(
                            ts_code=ts_code,
                            trade_date=trade_date,
                        )

                    if daily_basic is not None and not daily_basic.empty:
                        row = daily_basic.iloc[0]

                        # 市盈率、市净率、市值
                        if row.get("pe") and pd.notna(row.get("pe")) and row.get("pe") > 0:
                            info["pe_ratio"] = round(float(row["pe"]), 2)
                        if row.get("pb") and pd.notna(row.get("pb")) and row.get("pb") > 0:
                            info["pb_ratio"] = round(float(row["pb"]), 2)
                        if row.get("total_mv") and pd.notna(row.get("total_mv")):
                            # Tushare 单位：万元，转换为元
                            info["market_cap"] = float(row["total_mv"]) * 10000

                        debug_logger.debug(
                            "Tushare获取daily_basic成功",
                            symbol=symbol,
                            trade_date=trade_date,
                            pe=info.get("pe_ratio"),
                            pb=info.get("pb_ratio"),
                        )

                        # 获取 daily 数据（当前价格、涨跌幅）
                        with network_optimizer.apply():
                            daily = data_source_manager.tushare_api.daily(
                                ts_code=ts_code,
                                start_date=trade_date,
                                end_date=trade_date,
                            )

                        if daily is not None and not daily.empty:
                            daily_row = daily.iloc[0]
                            info["current_price"] = round(float(daily_row["close"]), 2)
                            info["change_percent"] = round(float(daily_row["pct_chg"]), 2)

                            debug_logger.debug(
                                "Tushare获取daily成功",
                                symbol=symbol,
                                trade_date=trade_date,
                                price=info.get("current_price"),
                                change_pct=info.get("change_percent"),
                            )
                        else:
                            # 如果当日数据不可用，尝试回退到最近几个交易日
                            debug_logger.debug(
                                "当日数据不可用，尝试回退查找",
                                trade_date=trade_date,
                            )
                            for days_back in range(1, 5):
                                fallback_date = (
                                    datetime.now() - timedelta(days=days_back)
                                ).strftime("%Y%m%d")
                                try:
                                    with network_optimizer.apply():
                                        daily = data_source_manager.tushare_api.daily(
                                            ts_code=ts_code,
                                            start_date=fallback_date,
                                            end_date=fallback_date,
                                        )
                                    if daily is not None and not daily.empty:
                                        daily_row = daily.iloc[0]
                                        info["current_price"] = round(
                                            float(daily_row["close"]), 2
                                        )
                                        info["change_percent"] = round(
                                            float(daily_row["pct_chg"]), 2
                                        )
                                        debug_logger.debug(
                                            "回退获取数据成功",
                                            symbol=symbol,
                                            fallback_date=fallback_date,
                                            price=info.get("current_price"),
                                        )
                                        break
                                except Exception as e:  # noqa: BLE001
                                    debug_logger.debug(
                                        f"回退获取{fallback_date}数据失败",
                                        error=str(e),
                                    )
                                    continue

                except Exception as e:  # noqa: BLE001
                    debug_logger.warning(
                        f"Tushare获取{trade_date}数据失败，尝试回退",
                        error=str(e),
                        symbol=symbol,
                    )
                    # 如果选择的交易日数据获取失败，回退到最近几个交易日
                    for days_back in range(1, 5):
                        fallback_date = (
                            datetime.now() - timedelta(days=days_back)
                        ).strftime("%Y%m%d")
                        try:
                            with network_optimizer.apply():
                                daily_basic = data_source_manager.tushare_api.daily_basic(
                                    ts_code=ts_code,
                                    trade_date=fallback_date,
                                )
                            if daily_basic is not None and not daily_basic.empty:
                                row = daily_basic.iloc[0]
                                if (
                                    row.get("pe")
                                    and pd.notna(row.get("pe"))
                                    and row.get("pe") > 0
                                ):
                                    info["pe_ratio"] = round(float(row["pe"]), 2)
                                if (
                                    row.get("pb")
                                    and pd.notna(row.get("pb"))
                                    and row.get("pb") > 0
                                ):
                                    info["pb_ratio"] = round(float(row["pb"]), 2)
                                if row.get("total_mv") and pd.notna(row.get("total_mv")):
                                    info["market_cap"] = float(row["total_mv"]) * 10000

                                daily = data_source_manager.tushare_api.daily(
                                    ts_code=ts_code,
                                    start_date=fallback_date,
                                    end_date=fallback_date,
                                )
                                if daily is not None and not daily.empty:
                                    daily_row = daily.iloc[0]
                                    info["current_price"] = round(
                                        float(daily_row["close"]), 2
                                    )
                                    info["change_percent"] = round(
                                        float(daily_row["pct_chg"]), 2
                                    )
                                debug_logger.debug(
                                    "回退获取成功",
                                    fallback_date=fallback_date,
                                    symbol=symbol,
                                )
                                break
                        except Exception as e2:  # noqa: BLE001
                            debug_logger.debug(
                                f"回退获取{fallback_date}失败", error=str(e2)
                            )
                            continue

            except Exception as e:  # noqa: BLE001
                debug_logger.warning(
                    "Tushare获取实时数据失败", error=e, symbol=symbol
                )

        # Tushare 失败或数据不完整，使用 Akshare 备用（仅实时模式，历史模式不使用 Akshare）
        if (info["current_price"] == "N/A" or info["pe_ratio"] == "N/A") and not analysis_date:
            try:
                debug_logger.debug("尝试从Akshare获取详细信息", symbol=symbol)
                with network_optimizer.apply():
                    import akshare as ak  # type: ignore

                    stock_info_df = ak.stock_individual_info_em(symbol=symbol)

                if stock_info_df is not None and not stock_info_df.empty:
                    for _, row in stock_info_df.iterrows():
                        key = row["item"]
                        value = row["value"]

                        if key == "股票简称" and info["name"] == "未知":
                            info["name"] = value
                        elif key == "总市值":
                            try:
                                if value and value != "-":
                                    info["market_cap"] = float(value)
                            except Exception:  # noqa: BLE001
                                pass
                        elif key == "市盈率-动态" and info["pe_ratio"] == "N/A":
                            try:
                                if value and value != "-":
                                    pe_val = float(value)
                                    if 0 < pe_val <= 1000:
                                        info["pe_ratio"] = pe_val
                            except Exception:  # noqa: BLE001
                                pass
                        elif key == "市净率" and info["pb_ratio"] == "N/A":
                            try:
                                if value and value != "-":
                                    pb_val = float(value)
                                    if 0 < pb_val <= 100:
                                        info["pb_ratio"] = pb_val
                            except Exception:  # noqa: BLE001
                                pass

                    debug_logger.debug("Akshare获取详细信息成功", symbol=symbol)
            except Exception as e:  # noqa: BLE001
                debug_logger.warning("Akshare获取详细信息失败", error=e, symbol=symbol)

        # 实时模式下优先使用实时行情刷新价格/涨跌幅等字段
        if not analysis_date:
            try:
                debug_logger.debug("尝试从实时行情获取价格", symbol=symbol)
                quotes = self.get_realtime_quotes(symbol)
                if quotes and isinstance(quotes, dict):
                    price_val = quotes.get("price")
                    if price_val is not None:
                        info["current_price"] = round(float(price_val), 2)
                    change_pct_val = quotes.get("change_percent")
                    if change_pct_val is not None:
                        info["change_percent"] = round(float(change_pct_val), 2)
                    open_val = quotes.get("open")
                    if open_val is not None:
                        info["open_price"] = round(float(open_val), 2)
                    high_val = quotes.get("high")
                    if high_val is not None:
                        info["high_price"] = round(float(high_val), 2)
                    low_val = quotes.get("low")
                    if low_val is not None:
                        info["low_price"] = round(float(low_val), 2)
                    pre_close_val = quotes.get("pre_close")
                    if pre_close_val is not None:
                        info["pre_close"] = round(float(pre_close_val), 2)
                    volume_val = quotes.get("volume")
                    if volume_val is not None:
                        try:
                            info["volume"] = int(volume_val)
                        except (TypeError, ValueError):
                            info["volume"] = volume_val
                    amount_val = quotes.get("amount")
                    if amount_val is not None:
                        info["amount"] = round(float(amount_val), 2)
                    if quotes.get("source"):
                        info["quote_source"] = quotes["source"]
                    if quotes.get("timestamp"):
                        info["quote_timestamp"] = quotes["timestamp"]
                    debug_logger.debug(
                        "实时行情获取成功",
                        symbol=symbol,
                        source=quotes.get("source"),
                    )
            except Exception as e:  # noqa: BLE001
                debug_logger.debug("实时行情获取失败", error=e, symbol=symbol)

        # 如果还是没有，尝试从历史数据获取最新收盘价
        if info["current_price"] == "N/A":
            try:
                debug_logger.debug(
                    "尝试从历史数据获取最新价格",
                    symbol=symbol,
                    analysis_date=analysis_date,
                )
                # 如果提供了 analysis_date，使用它作为结束日期；否则使用当前日期
                if analysis_date:
                    end_date = analysis_date
                    base_date = datetime.strptime(analysis_date, "%Y%m%d")
                else:
                    end_date = datetime.now().strftime("%Y%m%d")
                    base_date = datetime.now()

                start_date = (base_date - timedelta(days=30)).strftime("%Y%m%d")

                hist_data = self.get_stock_hist_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                )

                if (
                    hist_data is not None
                    and not hist_data.empty
                    and isinstance(hist_data, pd.DataFrame)
                ):
                    if "close" in hist_data.columns:
                        info["current_price"] = round(
                            float(hist_data.iloc[-1]["close"]), 2
                        )
                        # 计算涨跌幅
                        if len(hist_data) > 1:
                            prev_close = hist_data.iloc[-2]["close"]
                            change_pct = (
                                (hist_data.iloc[-1]["close"] - prev_close)
                                / prev_close
                            ) * 100
                            info["change_percent"] = round(change_pct, 2)
                        debug_logger.debug("历史数据获取成功", symbol=symbol)
            except Exception as e:  # noqa: BLE001
                debug_logger.debug("历史数据获取失败", error=e, symbol=symbol)

        # 获取 Beta 系数（仅 A 股，在获取完基本信息后）
        if info.get("beta") == "N/A" and self._is_chinese_stock(symbol):
            try:
                debug_logger.debug("尝试获取Beta系数", symbol=symbol)
                beta = self.get_beta_coefficient(symbol)
                if beta is not None:
                    info["beta"] = round(float(beta), 4)
                    debug_logger.debug(
                        "Beta系数获取成功", symbol=symbol, beta=info["beta"]
                    )
            except Exception as e:  # noqa: BLE001
                debug_logger.debug("Beta系数获取失败", error=e, symbol=symbol)

        # 获取 52 周高低位（仅 A 股，在获取完基本信息后）
        if (
            info.get("52_week_high") == "N/A"
            or info.get("52_week_low") == "N/A"
        ) and self._is_chinese_stock(symbol):
            try:
                debug_logger.debug("尝试获取52周高低位", symbol=symbol)
                week52_data = self.get_52week_high_low(symbol)
                if week52_data and week52_data.get("success"):
                    info["52_week_high"] = week52_data.get("high_52w", "N/A")
                    info["52_week_low"] = week52_data.get("low_52w", "N/A")
                    debug_logger.debug(
                        "52周高低位获取成功",
                        symbol=symbol,
                        high=info.get("52_week_high"),
                        low=info.get("52_week_low"),
                    )
            except Exception as e:  # noqa: BLE001
                debug_logger.debug("52周高低位获取失败", error=e, symbol=symbol)

        debug_logger.info(
            "get_stock_info完成",
            symbol=symbol,
            has_price=(info.get("current_price") != "N/A"),
            has_pe=(info.get("pe_ratio") != "N/A"),
            has_pb=(info.get("pb_ratio") != "N/A"),
            has_beta=(info.get("beta") != "N/A"),
            has_52week=(info.get("52_week_high") != "N/A"),
        )

        return info

    def get_stock_data(
        self,
        symbol: str,
        period: str = "1y",
        analysis_date: Optional[str] = None,
    ):
        """获取股票历史数据（别名方法，兼容旧接口）.

        The logic is kept identical to the legacy implementation so that
        callers relying on date range and normalisation behave the same.
        """

        debug_logger.info(
            "UnifiedDataAccess.get_stock_data调用",
            symbol=symbol,
            period=period,
            analysis_date=analysis_date,
            method="get_stock_data",
        )

        # 根据 period 计算日期范围
        # 如果提供了 analysis_date，使用它作为截止日期；否则使用当前日期
        if analysis_date:
            end_date = analysis_date  # 已经是 'YYYYMMDD' 格式
            base_date = datetime.strptime(analysis_date, "%Y%m%d")
        else:
            end_date = datetime.now().strftime("%Y%m%d")
            base_date = datetime.now()

        period_map = {
            "1mo": 30,
            "3mo": 90,
            "6mo": 180,
            "1y": 365,
            "2y": 730,
            "5y": 1825,
            "max": 3650,
        }
        days = period_map.get(period, 365)
        start_date = (base_date - timedelta(days=days)).strftime("%Y%m%d")

        debug_logger.debug(
            "计算日期范围",
            start_date=start_date,
            end_date=end_date,
            days=days,
        )

        result = self.get_stock_hist_data(symbol, start_date, end_date)

        debug_logger.data_info("get_stock_hist_data返回", result)

        # 处理返回结果
        if result is None:
            debug_logger.warning(
                "get_stock_hist_data返回None", symbol=symbol, period=period
            )
            return None

        # 如果是字典，尝试转换为 DataFrame 或返回错误
        if isinstance(result, dict):
            # 检查是否是错误响应
            if "error" in result:
                debug_logger.error(
                    "数据源返回错误",
                    error=result.get("error"),
                    symbol=symbol,
                    period=period,
                )
                return None

            # 尝试将字典转换为 DataFrame
            try:
                debug_logger.warning(
                    "尝试将dict转换为DataFrame",
                    symbol=symbol,
                    dict_keys=list(result.keys()),
                )
                # 如果是单行数据字典，转换为 DataFrame
                if all(
                    not isinstance(v, (list, pd.Series)) for v in result.values()
                ):
                    df = pd.DataFrame([result])
                    debug_logger.info(
                        "成功将单行dict转换为DataFrame", symbol=symbol, rows=1
                    )
                    return df
                # 多行数据字典，尝试直接转换
                df = pd.DataFrame(result)
                debug_logger.info(
                    "成功将多行dict转换为DataFrame", symbol=symbol, rows=len(df)
                )
                return df
            except Exception as e:  # noqa: BLE001
                debug_logger.error(
                    "无法将dict转换为DataFrame",
                    error=e,
                    symbol=symbol,
                    dict_keys=list(result.keys())[:5],
                )
                return None

        # 验证返回类型 - 必须是 DataFrame
        if not isinstance(result, pd.DataFrame):
            debug_logger.error(
                "get_stock_hist_data返回类型错误",
                expected_type="DataFrame or None",
                actual_type=type(result).__name__,
                symbol=symbol,
                period=period,
                result_preview=str(result)[:200],
            )
            return None

        # 数据标准化：确保列名正确
        try:
            # 标准化列名（统一为大写）
            column_mapping = {
                "date": "Date",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
                "amount": "Amount",
            }

            # 重命名列
            result = result.rename(columns=column_mapping)

            # 确保 Date 列为 datetime 类型并设置为索引
            if "Date" in result.columns:
                result["Date"] = pd.to_datetime(result["Date"])
                result = result.set_index("Date")
            elif result.index.name == "date" or (
                hasattr(result.index, "dtype")
                and "datetime" in str(result.index.dtype)
            ):
                # 索引已经是日期类型
                result.index.name = "Date"

            # 确保数值列为 float 类型
            numeric_columns = ["Open", "High", "Low", "Close", "Volume"]
            for col in numeric_columns:
                if col in result.columns:
                    result[col] = pd.to_numeric(result[col], errors="coerce")

            # 按日期排序
            result = result.sort_index()

            debug_logger.debug(
                "数据标准化完成",
                symbol=symbol,
                rows=len(result),
                columns=list(result.columns),
                date_range=f"{result.index.min()} ~ {result.index.max()}",
            )

        except Exception as e:  # noqa: BLE001
            debug_logger.error(
                "数据标准化失败",
                error=e,
                symbol=symbol,
                columns=list(result.columns)
                if hasattr(result, "columns")
                else "N/A",
            )
            # 即使标准化失败，也返回原始数据

        return result

    # ------------------------------------------------------------------
    # 其它数据接口：财务、资金流、风险、情绪、新闻等
    # ------------------------------------------------------------------

    def get_realtime_quotes(self, symbol: str) -> Dict[str, Any]:
        """实时行情直接代理 data_source_manager."""

        return data_source_manager.get_realtime_quotes(symbol)

    def get_financial_data(
        self,
        symbol: str,
        report_type: str = "income",
        analysis_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """获取财务数据（包装为字典格式）。"""

        debug_logger.info(
            "开始获取财务数据",
            symbol=symbol,
            report_type=report_type,
            analysis_date=analysis_date,
            method="get_financial_data",
        )

        result: Dict[str, Any] = {
            "symbol": symbol,
            "data_success": False,
            "income_statement": None,
            "balance_sheet": None,
            "cash_flow": None,
            "source": None,
        }

        try:
            # 注意：data_source_manager.get_financial_data() 目前不支持 analysis_date 参数
            df = data_source_manager.get_financial_data(symbol, report_type)

            if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                records = df.to_dict("records")

                if report_type == "income":
                    result["income_statement"] = {
                        "data": records,
                        "periods": len(records),
                        "columns": df.columns.tolist(),
                    }
                elif report_type == "balance":
                    result["balance_sheet"] = {
                        "data": records,
                        "periods": len(records),
                        "columns": df.columns.tolist(),
                    }
                elif report_type == "cashflow":
                    result["cash_flow"] = {
                        "data": records,
                        "periods": len(records),
                        "columns": df.columns.tolist(),
                    }

                result["source"] = (
                    "tushare" if data_source_manager.tushare_available else "akshare"
                )
                result["data_success"] = True

                debug_logger.info(
                    "财务数据获取成功",
                    symbol=symbol,
                    report_type=report_type,
                    periods=len(records),
                    source=result["source"],
                )
            else:
                result["error"] = f"未能获取{report_type}财务数据"
                debug_logger.warning("财务数据为空", symbol=symbol, report_type=report_type)

        except Exception as e:  # noqa: BLE001
            result["error"] = str(e)
            debug_logger.error(
                "获取财务数据失败", error=e, symbol=symbol, report_type=report_type
            )

        return result

    def get_quarterly_reports(
        self, symbol: str, analysis_date: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """获取季度报表数据（统一封装）。"""

        try:
            from .quarterly_report_data_impl import QuarterlyReportDataFetcher

            with network_optimizer.apply():
                return QuarterlyReportDataFetcher().get_quarterly_reports(
                    symbol, analysis_date=analysis_date
                )
        except Exception as e:  # noqa: BLE001
            return {"symbol": symbol, "data_success": False, "error": str(e)}

    def get_fund_flow_data(
        self, symbol: str, analysis_date: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """获取资金流向数据（Akshare 实现）。"""

        try:
            from .fund_flow_akshare_impl import FundFlowAkshareDataFetcher

            with network_optimizer.apply():
                return FundFlowAkshareDataFetcher().get_fund_flow_data(
                    symbol, analysis_date=analysis_date
                )
        except Exception as e:  # noqa: BLE001
            debug_logger.error(
                "获取资金流向数据失败",
                symbol=symbol,
                error=str(e),
                analysis_date=analysis_date,
            )
            return {"symbol": symbol, "data_success": False, "error": str(e)}

    def get_market_sentiment_data(
        self,
        symbol: str,
        stock_data,
        analysis_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取市场情绪数据。"""

        try:
            from .market_sentiment_data_impl import MarketSentimentDataFetcher

            with network_optimizer.apply():
                return MarketSentimentDataFetcher().get_market_sentiment_data(
                    symbol, stock_data, analysis_date=analysis_date
                )
        except Exception as e:  # noqa: BLE001
            debug_logger.error(
                "获取市场情绪数据失败",
                symbol=symbol,
                error=str(e),
                analysis_date=analysis_date,
            )
            return {"symbol": symbol, "data_success": False, "error": str(e)}

    def get_margin_trading_history(
        self,
        symbol: str,
        days: int = 5,
        analysis_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取个股融资融券历史数据。"""

        try:
            from .market_sentiment_data_impl import MarketSentimentDataFetcher

            with network_optimizer.apply():
                return MarketSentimentDataFetcher()._get_margin_trading_history(
                    symbol, days=days, analysis_date=analysis_date
                )
        except Exception as e:  # noqa: BLE001
            debug_logger.error(
                "获取融资融券历史数据失败",
                symbol=symbol,
                error=str(e),
                analysis_date=analysis_date,
            )
            return {"symbol": symbol, "data_success": False, "error": str(e)}

    def get_index_daily_metrics(
        self, analysis_date: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """获取重点指数每日指标数据。"""

        try:
            from .market_sentiment_data_impl import MarketSentimentDataFetcher

            with network_optimizer.apply():
                return MarketSentimentDataFetcher()._get_index_daily_metrics(
                    analysis_date=analysis_date
                )
        except Exception as e:  # noqa: BLE001
            debug_logger.error(
                "获取指数每日指标失败", error=str(e), analysis_date=analysis_date
            )
            return {"data_success": False, "error": str(e)}

    def get_news_data(
        self, symbol: str, analysis_date: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """获取新闻数据。"""

        try:
            from .qstock_news_data_impl import QStockNewsDataFetcher

            with network_optimizer.apply():
                return QStockNewsDataFetcher().get_stock_news(
                    symbol, analysis_date=analysis_date
                )
        except Exception as e:  # noqa: BLE001
            debug_logger.error(
                "获取新闻数据失败",
                symbol=symbol,
                error=str(e),
                analysis_date=analysis_date,
            )
            return {"symbol": symbol, "data_success": False, "error": str(e)}

    def get_stock_news(
        self, symbol: str, analysis_date: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """获取股票新闻（别名方法，兼容旧接口）。"""

        return self.get_news_data(symbol, analysis_date=analysis_date)

    def get_risk_data(
        self, symbol: str, analysis_date: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """获取风险数据（限售解禁、大股东减持等）。"""

        try:
            from .risk_data_fetcher_impl import RiskDataFetcher

            with network_optimizer.apply():
                return RiskDataFetcher().get_risk_data(
                    symbol, analysis_date=analysis_date
                )
        except Exception as e:  # noqa: BLE001
            return {"symbol": symbol, "data_success": False, "error": str(e)}

    def get_research_reports_data(
        self,
        symbol: str,
        days: int = 180,
        analysis_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """获取机构研报数据 (Tushare 优先，包含研报内容，基于内容分析)。"""

        start_time = time_module.time()
        debug_logger.info(
            "开始获取研报数据", symbol=symbol, days=days, analysis_date=analysis_date
        )
        print(
            f"📑 [UnifiedDataAccess] 正在获取 {symbol} 机构研报数据（最近{days}天，包含内容）..."
        )

        data: Dict[str, Any] = {
            "symbol": symbol,
            "research_reports": [],
            "data_success": False,
            "source": None,
            "report_count": 0,
            "analysis_summary": {},
            "content_analysis": {},
        }

        # 只支持 A 股
        if not self._is_chinese_stock(symbol):
            data["error"] = "机构研报数据仅支持中国A股股票"
            print("   ⚠️ 机构研报数据仅支持A股")
            debug_logger.warning("研报数据仅支持A股", symbol=symbol)
            return data

        # 1. 优先使用 Tushare report_rc 接口
        if data_source_manager.tushare_available:
            try:
                print("   [方法1-Tushare] 正在获取研报数据（report_rc接口，包含内容）...")
                ts_code = data_source_manager._convert_to_ts_code(symbol)

                # 计算日期范围
                if analysis_date:
                    end_date = analysis_date
                    base_date = datetime.strptime(analysis_date, "%Y%m%d")
                else:
                    end_date = datetime.now().strftime("%Y%m%d")
                    base_date = datetime.now()
                start_date = (base_date - timedelta(days=days)).strftime("%Y%m%d")

                with network_optimizer.apply():
                    df_reports = data_source_manager.tushare_api.report_rc(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date,
                    )

                if df_reports is not None and not df_reports.empty:
                    print(f"   ✓ 获取到 {len(df_reports)} 条Tushare研报数据（含内容）")

                    # 去重：基于日期+机构+标题
                    if len(df_reports) > 0:
                        df_reports["_unique_key"] = (
                            df_reports["report_date"].astype(str)
                            + "_"
                            + df_reports["org_name"].astype(str)
                            + "_"
                            + df_reports["report_title"].astype(str)
                        )
                        df_reports = df_reports.drop_duplicates(
                            subset=["_unique_key"], keep="first"
                        )
                        df_reports = df_reports.drop(columns=["_unique_key"])
                        print(
                            f"   ✓ 去重后剩余 {len(df_reports)} 条研报数据"
                        )

                    analysis = self._analyze_research_reports(df_reports)

                    # 再次在字典层面去重
                    seen_keys = set()
                    reports: List[Dict[str, Any]] = []
                    for report_data in analysis.get("reports_data", []):
                        unique_key = (
                            str(report_data.get("report_date", ""))
                            + "_"
                            + str(report_data.get("org_name", ""))
                            + "_"
                            + str(report_data.get("report_title", ""))
                        )
                        if unique_key in seen_keys:
                            continue
                        seen_keys.add(unique_key)

                        reports.append(
                            {
                                "日期": report_data.get("report_date", ""),
                                "研报标题": report_data.get("report_title", ""),
                                "机构名称": report_data.get("org_name", ""),
                                "研究员": report_data.get("author_name", ""),
                                "评级": report_data.get("rating", ""),
                                "目标价": str(
                                    report_data.get("target_price_max")
                                    or report_data.get("target_price_min")
                                    or "N/A"
                                ),
                                "研报类型": report_data.get("report_type", ""),
                                "研报内容": report_data.get("content", ""),
                                "内容摘要": report_data.get(
                                    "content_summary", ""
                                ),
                            }
                        )

                    data["research_reports"] = reports
                    data["report_count"] = analysis.get("total_reports", 0)
                    data["analysis_summary"] = analysis.get("summary", {})
                    data["content_analysis"] = analysis.get(
                        "content_analysis", {}
                    )
                    data["data_success"] = True
                    data["source"] = "tushare"

                    print(
                        f"   ✅ 成功获取 {len(reports)} 条机构研报（含内容和内容分析）"
                    )
                    debug_logger.info(
                        "研报数据获取成功（Tushare，含内容）",
                        symbol=symbol,
                        count=len(reports),
                        source="tushare",
                    )

                    elapsed_time = time_module.time() - start_time
                    debug_logger.info(
                        "研报数据获取完成",
                        symbol=symbol,
                        success=True,
                        count=len(reports),
                        elapsed=f"{elapsed_time:.2f}s",
                    )
                    return data
                else:
                    print("   ℹ️ Tushare未找到研报数据")
            except Exception as e:  # noqa: BLE001
                debug_logger.warning("Tushare获取研报失败", error=e, symbol=symbol)
                print(f"   ⚠️ Tushare获取失败: {e}")

        # 2. 备选使用 Akshare
        try:
            print("   [方法2-Akshare] 正在获取研报数据（备用数据源）...")
            with network_optimizer.apply():
                import akshare as ak  # type: ignore

                df = ak.stock_research_report_em(symbol=symbol)

            if df is not None and not df.empty:
                seen_keys = set()
                reports = []
                for _, row in df.iterrows():
                    date = str(row.get("日期", ""))
                    org = str(row.get("机构名称", ""))
                    title = str(row.get("研报标题", ""))
                    unique_key = f"{date}_{org}_{title}"
                    if unique_key in seen_keys:
                        continue
                    seen_keys.add(unique_key)

                    report = {
                        "日期": date,
                        "研报标题": title,
                        "机构名称": org,
                        "研究员": str(row.get("研究员", "")),
                        "评级": str(row.get("评级", "")),
                        "目标价": str(row.get("目标价", "N/A")),
                        "相关股票": str(row.get("相关股票", "")),
                        "研报内容": "",
                        "内容摘要": "",
                    }
                    reports.append(report)

                rating_list = [r["评级"] for r in reports if r["评级"]]
                total = len(reports)
                buy_count = sum(
                    1
                    for r in rating_list
                    if any(
                        keyword in str(r)
                        for keyword in ["买入", "增持", "推荐", "强推"]
                    )
                )
                neutral_count = sum(
                    1
                    for r in rating_list
                    if any(
                        keyword in str(r)
                        for keyword in ["持有", "中性", "观望"]
                    )
                )
                sell_count = sum(
                    1
                    for r in rating_list
                    if any(
                        keyword in str(r)
                        for keyword in ["卖出", "减持", "回避"]
                    )
                )

                data["research_reports"] = reports
                data["report_count"] = len(reports)
                data["analysis_summary"] = {
                    "rating_ratio": {
                        "buy_ratio": round(buy_count / total * 100, 2)
                        if total > 0
                        else 0,
                        "neutral_ratio": round(neutral_count / total * 100, 2)
                        if total > 0
                        else 0,
                        "sell_ratio": round(sell_count / total * 100, 2)
                        if total > 0
                        else 0,
                    }
                }
                data["data_success"] = True
                data["source"] = "akshare"

                print(f"   ✅ 成功获取 {len(reports)} 条机构研报（Akshare）")
                debug_logger.info(
                    "研报数据获取成功（Akshare）",
                    symbol=symbol,
                    count=len(reports),
                    source="akshare",
                )
            else:
                print("   ℹ️ 未找到机构研报数据")
                data["error"] = "未找到机构研报数据"

        except Exception as e:  # noqa: BLE001
            debug_logger.error("获取机构研报失败", error=e, symbol=symbol)
            print(f"   ❌ 获取机构研报失败: {e}")
            data["error"] = str(e)

        elapsed_time = time_module.time() - start_time
        debug_logger.info(
            "研报数据获取完成",
            symbol=symbol,
            success=data.get("data_success", False),
            count=data.get("report_count", 0),
            elapsed=f"{elapsed_time:.2f}s",
        )

        return data

    def get_announcement_data(
        self,
        symbol: str,
        days: int = 30,
        analysis_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """获取公告数据 - 过去 N 天的上市公司公告 (东方财富优先，其次 Tushare)。"""

        start_time = time_module.time()
        debug_logger.info(
            "开始获取公告数据",
            symbol=symbol,
            days=days,
            analysis_date=analysis_date,
            method="get_announcement_data",
        )
        print(f"📢 [UnifiedDataAccess] 正在获取 {symbol} 最近{days}天的公告数据...")

        data: Dict[str, Any] = {
            "symbol": symbol,
            "announcements": [],
            "pdf_analysis": [],
            "data_success": False,
            "source": None,
            "days": days,
            "date_range": None,
        }

        # 只支持 A 股
        if not self._is_chinese_stock(symbol):
            data["error"] = "公告数据仅支持中国A股股票"
            debug_logger.warning("公告数据仅支持A股", symbol=symbol, is_chinese=False)
            print("   ⚠️ 公告数据仅支持A股")
            return data

        def _normalize_url(url: Optional[str]) -> Optional[str]:
            if not url:
                return None
            url = url.strip()
            if not url:
                return None
            if url.startswith("//"):
                return "https:" + url
            if url.startswith("/"):
                return "https://static.cninfo.com.cn" + url
            return url

        def _resolve_pdf_url(
            row: Dict[str, Any], ts_code_value: str, ann_date_value: str
        ) -> Optional[str]:
            key_priority = [
                "pdf_url",
                "file_url",
                "adjunct_url",
                "page_pdf_url",
                "ann_pdf_url",
                "url",
                "page_url",
                "doc_url",
                "src",
            ]
            for key in key_priority:
                value = row.get(key)
                normalized = (
                    _normalize_url(value) if isinstance(value, str) else None
                )
                if normalized:
                    return normalized

            ann_id = row.get("announcement_id") or row.get("attachment_id")
            org_id = row.get("org_id") or row.get("orgId")
            announcement_type = row.get("announcement_type") or row.get("plate")
            if ann_id and org_id:
                if not announcement_type:
                    if ts_code_value.endswith(".SH"):
                        announcement_type = "sse"
                    elif ts_code_value.endswith(".SZ"):
                        announcement_type = "szse"
                    elif ts_code_value.endswith(".BJ"):
                        announcement_type = "bj"
                return (
                    "https://www.cninfo.com.cn/new/disclosure/detail"
                    f"?plate={announcement_type or ''}&orgId={org_id}"
                    f"&stockCode={ts_code_value.replace('.', '')}"
                    f"&announcementId={ann_id}"
                    + (
                        f"&announcementTime={ann_date_value}" if ann_date_value else ""
                    )
                )

            return None

        def _extract_pdf_text(pdf_bytes: bytes) -> Optional[str]:
            text_candidates: List[str] = []
            # 优先尝试 PyPDF2
            try:
                import PyPDF2  # type: ignore

                reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
                page_texts = []
                for page in reader.pages[:20]:
                    extracted = page.extract_text() or ""
                    page_texts.append(extracted.strip())
                combined = "\n".join(filter(None, page_texts)).strip()
                if combined:
                    text_candidates.append(combined)
            except Exception as e:  # noqa: BLE001
                debug_logger.debug("PyPDF2解析公告PDF失败", error=str(e))

            # 备用 pdfplumber
            if not text_candidates:
                try:
                    import pdfplumber  # type: ignore

                    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                        page_texts = []
                        for page in pdf.pages[:20]:
                            page_texts.append(page.extract_text() or "")
                        combined = "\n".join(filter(None, page_texts)).strip()
                        if combined:
                            text_candidates.append(combined)
                except Exception as e:  # noqa: BLE001
                    debug_logger.debug(
                        "pdfplumber解析公告PDF失败", error=str(e)
                    )

            if text_candidates:
                text = text_candidates[0]
                if len(text) > 8000:
                    return text[:8000] + "..."
                return text
            return None

        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})

        def _em_cookies() -> Dict[str, str]:
            """构造东方财富 pdf.dfcfw.com 反爬脚本设置的 Cookie。"""

            status = 208722705 + 1275103711 + 1998477227
            return {
                "__tst_status": f"{status}#",
                "EO_Bot_Ssid": "212402176",
            }

        def _cninfo_download_url(detail_url: str) -> Optional[str]:
            try:
                parsed = urlparse(detail_url)
                qs = parse_qs(parsed.query)
                ann_id = qs.get("announcementId") or qs.get("bulletinId")
                ann_time = qs.get("announcementTime") or qs.get("announceTime")
                if ann_id and ann_time:
                    return (
                        "https://www.cninfo.com.cn/new/announcement/download"
                        f"?bulletinId={ann_id[0]}&announceTime={ann_time[0]}"
                    )
            except Exception:  # noqa: BLE001
                pass
            return None

        def _download_pdf_bytes(
            url: str, origin_detail: Optional[str] = None, depth: int = 0
        ) -> Optional[bytes]:
            if not url or not isinstance(url, str) or depth > 2:
                return None
            try:
                try:
                    parsed = urlparse(url)
                    host = parsed.netloc.lower()
                except Exception:  # noqa: BLE001
                    host = ""

                if "pdf.dfcfw.com" in host:
                    headers_em = {
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) "
                            "Gecko/20100101 Firefox/140.0"
                        ),
                        "Accept": "application/pdf,*/*;q=0.9",
                        "Accept-Language": "zh-CN,zh;q=0.9",
                    }
                    cookies_em = _em_cookies()
                    response = requests.get(
                        url,
                        headers=headers_em,
                        cookies=cookies_em,
                        timeout=20,
                        allow_redirects=True,
                        proxies={},
                    )
                else:
                    headers = {"User-Agent": "Mozilla/5.0"}
                    if origin_detail and depth == 0:
                        headers["Referer"] = origin_detail
                        with network_optimizer.apply():
                            session.get(
                                origin_detail,
                                headers=headers,
                                timeout=25,
                                allow_redirects=True,
                            )
                    cninfo_download = _cninfo_download_url(url)
                    request_url = cninfo_download or url
                    if origin_detail:
                        headers["Referer"] = origin_detail
                    with network_optimizer.apply():
                        response = session.get(
                            request_url,
                            headers=headers,
                            timeout=25,
                            allow_redirects=True,
                        )
                if response.status_code != 200:
                    debug_logger.debug(
                        "公告PDF下载失败", url=url, status=response.status_code
                    )
                    return None

                content = response.content
                content_type = response.headers.get("Content-Type", "").lower()
                if content.startswith(b"%PDF") or "application/pdf" in content_type:
                    return content
                if content.startswith(b"PK"):
                    try:
                        with zipfile.ZipFile(BytesIO(content)) as zf:
                            for name in zf.namelist():
                                if name.lower().endswith(".pdf"):
                                    return zf.read(name)
                    except Exception as zip_error:  # noqa: BLE001
                        debug_logger.debug(
                            "公告PDF解压失败", url=url, error=str(zip_error)
                        )

                text_snippet = content[:1024].decode("utf-8", errors="ignore")
                if "<html" in text_snippet.lower():
                    html_text = response.text
                    pdf_match = re.search(
                        r"https?://static\\.cninfo\\.com\\.cn/[^\\\"'<>]+\\.pdf",
                        html_text,
                        re.I,
                    )
                    if pdf_match:
                        next_url = pdf_match.group(0)
                        debug_logger.debug(
                            "公告PDF链接重定向",
                            original=url,
                            extracted=next_url,
                        )
                        return _download_pdf_bytes(
                            next_url, origin_detail or url, depth + 1
                        )

                    ann_id_match = re.search(
                        r"announcementId=([A-Za-z0-9]+)", url
                    )
                    org_id_match = re.search(r"orgId=([A-Za-z0-9]+)", url)
                    if ann_id_match and org_id_match:
                        ann_id = ann_id_match.group(1)
                        org_id = org_id_match.group(1)
                        api_url = (
                            "https://www.cninfo.com.cn/new/disclosure/detail"
                            f"?plate=&orgId={org_id}&stockCode=&announcementId={ann_id}&lang=zh"
                        )
                        with network_optimizer.apply():
                            api_resp = requests.get(
                                api_url,
                                headers=headers,
                                timeout=25,
                                allow_redirects=True,
                            )
                        if api_resp.status_code == 200:
                            api_text = api_resp.text
                            pdf_match_api = re.search(
                                r"https?://static\\.cninfo\\.com\\.cn/[^\\\"'<>]+\\.pdf",
                                api_text,
                                re.I,
                            )
                            if pdf_match_api:
                                next_url = pdf_match_api.group(0)
                                debug_logger.debug(
                                    "公告PDF链接(AJAX)重定向",
                                    original=url,
                                    extracted=next_url,
                                )
                                return _download_pdf_bytes(
                                    next_url, origin_detail or url, depth + 1
                                )
                    pdf_match_rel = re.search(
                        r"data-pdf=\"([^\"]+\.pdf)\"", html_text
                    )
                    if pdf_match_rel:
                        next_url = _normalize_url(pdf_match_rel.group(1))
                        if next_url:
                            debug_logger.debug(
                                "公告PDF链接重定向(data-pdf)",
                                original=url,
                                extracted=next_url,
                            )
                            return _download_pdf_bytes(
                                next_url, origin_detail or url, depth + 1
                            )
                    href_match = re.search(
                        r'href="([^\"]+\.pdf)"', html_text
                    )
                    if href_match:
                        next_url = _normalize_url(href_match.group(1))
                        if next_url:
                            debug_logger.debug(
                                "公告PDF链接重定向(href)",
                                original=url,
                                extracted=next_url,
                            )
                            return _download_pdf_bytes(
                                next_url, origin_detail or url, depth + 1
                            )
                return None
            except Exception as e:  # noqa: BLE001
                debug_logger.debug("公告PDF下载异常", url=url, error=str(e))
                return None

        def _download_and_parse_pdf(
            url: str, ann_meta: Optional[Dict[str, Any]] = None
        ) -> Tuple[Optional[str], Optional[str]]:
            detail_url = None
            if ann_meta:
                detail_url = (
                    ann_meta.get("detail_url")
                    if ann_meta.get("detail_url") != "N/A"
                    else None
                )
            pdf_bytes = _download_pdf_bytes(url, detail_url)
            if not pdf_bytes:
                return None, None
            text = _extract_pdf_text(pdf_bytes)

            saved_path = None
            if pdf_bytes:
                title = ann_meta.get("公告标题") if ann_meta else "announcement"
                trade_date = (
                    ann_meta.get("日期")
                    if ann_meta
                    else datetime.now().strftime("%Y-%m-%d")
                )
                safe_title = re.sub(r"[\\/:*?\"<>|]", "_", str(title))
                safe_date = re.sub(r"[\\/:*?\"<>|]", "_", str(trade_date))
                symbol_dir = Path("data") / "announcements" / symbol
                symbol_dir.mkdir(parents=True, exist_ok=True)
                filename = f"{safe_date}_{safe_title}.pdf"
                saved_path = str(symbol_dir / filename)
                with open(saved_path, "wb") as f:
                    f.write(pdf_bytes)

            return text, saved_path

        def _fetch_announcements_from_eastmoney(
            symbol: str,
        ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
            """使用东方财富公告接口作为兜底数据源，返回公告列表和 PDF 解析结果。"""

            base_url = "https://np-anotice-stock.eastmoney.com/api/security/ann"
            content_api = "https://np-cnotice-stock.eastmoney.com/api/content/ann"

            def _clean_symbol(code: str) -> str:
                stock = code.strip()
                if "." in stock:
                    stock = stock.split(".")[0]
                for prefix in ("sh", "sz", "gb_", "us", "us_"):
                    if stock.startswith(prefix):
                        stock = stock[len(prefix) :]
                        break
                return stock

            headers_list = {
                "Host": "np-anotice-stock.eastmoney.com",
                "Referer": "https://data.eastmoney.com/notices/hsa/5.html",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) "
                    "Gecko/20100101 Firefox/140.0"
                ),
            }

            def _get_notices(
                code: str, page_size: int = 50, page_index: int = 1
            ) -> List[Dict[str, Any]]:
                params = {
                    "page_size": page_size,
                    "page_index": page_index,
                    "ann_type": "SHA,CYB,SZA,BJA,INV",
                    "client_source": "web",
                    "f_node": "0",
                    "stock_list": _clean_symbol(code),
                }
                resp = requests.get(
                    base_url, params=params, headers=headers_list, timeout=15
                )
                resp.raise_for_status()
                payload = resp.json() or {}
                return payload.get("data", {}).get("list", []) or []

            def _fetch_notice_detail(art_code: str) -> Dict[str, Any]:
                if not art_code:
                    return {}
                params = {"art_code": art_code, "client_source": "web"}
                headers_detail = {
                    "Referer": "https://data.eastmoney.com/",
                    "User-Agent": headers_list["User-Agent"],
                    "Accept": "application/json,text/plain,*/*",
                }
                try:
                    resp = requests.get(
                        content_api,
                        params=params,
                        headers=headers_detail,
                        timeout=15,
                        proxies={},
                    )
                    if resp.status_code != 200:
                        return {}
                    return resp.json().get("data", {}) or {}
                except Exception as e:  # noqa: BLE001
                    try:
                        debug_logger.debug(
                            "东方财富公告详情请求失败",
                            art_code=art_code,
                            error=str(e),
                        )
                    except Exception:  # noqa: BLE001
                        pass
                    return {}

            def _extract_pdf_urls(detail: Dict[str, Any]) -> List[str]:
                urls: List[str] = []
                if not detail:
                    return urls
                attaches = (
                    detail.get("attachments")
                    or detail.get("attach_list")
                    or []
                )
                for att in attaches:
                    if not isinstance(att, Dict):
                        continue
                    u = (
                        att.get("url")
                        or att.get("oss_url")
                        or att.get("file_url")
                    )
                    if isinstance(u, str) and u.lower().endswith(".pdf"):
                        urls.append(u)
                for key in ("pdf_url", "em_pdf", "notice_pdf"):
                    u = detail.get(key)
                    if isinstance(u, str) and u.lower().endswith(".pdf"):
                        urls.append(u)
                seen: Dict[str, None] = {}
                result_urls: List[str] = []
                for u in urls:
                    if u not in seen:
                        seen[u] = None
                        result_urls.append(u)
                return result_urls

            notices = _get_notices(symbol, page_size=50)
            if not notices:
                return [], []

            notices.sort(key=lambda x: x.get("notice_date", ""), reverse=True)
            max_items = 20
            notices = notices[:max_items]

            announcements: List[Dict[str, Any]] = []
            pdf_analysis: List[Dict[str, Any]] = []

            for notice in notices:
                stock_info = (notice.get("codes") or [{}])[0] or {}
                art_code = notice.get("art_code") or ""
                date_str = notice.get("notice_date") or ""
                title = notice.get("title") or "N/A"
                ann_type = (notice.get("columns") or [{}])[0].get(
                    "column_name", ""
                )

                ann_item: Dict[str, Any] = {
                    "日期": date_str or "N/A",
                    "公告标题": title,
                    "公告类型": ann_type or "N/A",
                    "公告摘要": "",
                    "pdf_url": (
                        f"https://pdf.dfcfw.com/pdf/H2_{art_code}_1.pdf"
                        if art_code
                        else "N/A"
                    ),
                    "download_url": (
                        f"https://pdf.dfcfw.com/pdf/H2_{art_code}_1.pdf"
                        if art_code
                        else "N/A"
                    ),
                    "detail_url": (
                        f"https://data.eastmoney.com/notices/detail/{stock_info.get('stock_code', '')}/{art_code}.html"
                        if art_code
                        else "N/A"
                    ),
                    "原始数据": notice,
                }

                detail = _fetch_notice_detail(art_code)
                pdf_urls = _extract_pdf_urls(detail)
                if pdf_urls:
                    ann_item["pdf_url"] = pdf_urls[0]
                    ann_item["download_url"] = pdf_urls[0]

                announcements.append(ann_item)

            for ann in announcements[:5]:
                pdf_url = ann.get("pdf_url")
                analysis_entry: Dict[str, Any] = {
                    "date": ann.get("日期"),
                    "title": ann.get("公告标题"),
                    "pdf_url": pdf_url,
                    "text": None,
                    "success": False,
                }
                if pdf_url and pdf_url != "N/A":
                    pdf_text, saved_path = _download_and_parse_pdf(pdf_url, ann)
                    if pdf_text:
                        analysis_entry["text"] = pdf_text
                        analysis_entry["success"] = True
                    if saved_path:
                        analysis_entry["saved_path"] = saved_path
                        ann["saved_path"] = saved_path
                else:
                    analysis_entry["text"] = "未提供PDF链接。"
                pdf_analysis.append(analysis_entry)

            return announcements, pdf_analysis

        try:
            if analysis_date:
                end_dt = datetime.strptime(analysis_date, "%Y%m%d")
            else:
                end_dt = datetime.now()

            start_dt = end_dt - timedelta(days=days)
            start_date_str = start_dt.strftime("%Y%m%d")
            end_date_str = end_dt.strftime("%Y%m%d")
            data["date_range"] = {"start": start_date_str, "end": end_date_str}

            # 1) 首选东方财富公告接口
            print("   [Eastmoney] 正在通过东方财富公告接口获取数据...")
            anns_em, pdf_em = _fetch_announcements_from_eastmoney(symbol)
            if anns_em:
                data["announcements"] = anns_em
                data["pdf_analysis"] = pdf_em
                data["source"] = "eastmoney"
                data["data_success"] = True
                data["count"] = len(anns_em)
                return data

            # 2) 东方财富无数据时，尝试 Tushare anns_d
            if not data_source_manager.tushare_available:
                data["error"] = "东方财富公告接口无数据且Tushare不可用"
                print("   ⚠️ 东方财富公告接口无数据，且当前环境未启用Tushare")
                return data

            ts_code = data_source_manager._convert_to_ts_code(symbol)

            print("   [Tushare] 东方财富无数据，尝试通过Tushare anns_d 获取公告列表...")
            all_rows: List[pd.DataFrame] = []
            limit = 50
            offset = 0
            while True:
                with network_optimizer.apply():
                    df_batch = data_source_manager.tushare_api.anns_d(
                        ts_code=ts_code,
                        start_date=start_date_str,
                        end_date=end_date_str,
                        limit=limit,
                        offset=offset,
                        fields=(
                            "ts_code,ann_date,ann_type,title,content,file_url,adjunct_url,"
                            "page_pdf_url,pdf_url,org_id,announcement_id,announcement_type,src,url"
                        ),
                    )

                if df_batch is None or df_batch.empty:
                    break

                all_rows.append(df_batch)
                if len(df_batch) < limit:
                    break
                offset += limit

            if not all_rows:
                print("   ℹ️ 东方财富与Tushare均未查询到公告数据")
                data["error"] = "东方财富与Tushare均未查询到公告数据"
                return data

            df_all = pd.concat(all_rows, ignore_index=True)
            df_all = df_all.sort_values("ann_date", ascending=False)

            announcements_ts: List[Dict[str, Any]] = []
            for _, row in df_all.iterrows():
                ann_date = str(row.get("ann_date", ""))
                ann_date_fmt = "N/A"
                if ann_date:
                    try:
                        ann_date_fmt = datetime.strptime(
                            ann_date, "%Y%m%d"
                        ).strftime("%Y-%m-%d")
                    except Exception:  # noqa: BLE001
                        ann_date_fmt = ann_date

                pdf_url = _resolve_pdf_url(row, ts_code, ann_date)
                download_url = (
                    _cninfo_download_url(pdf_url) if pdf_url else None
                )
                announcement = {
                    "日期": ann_date_fmt,
                    "公告标题": str(row.get("title", "N/A")),
                    "公告类型": str(row.get("ann_type", "N/A")),
                    "公告摘要": (
                        str(row.get("content", ""))[:400]
                        if pd.notna(row.get("content"))
                        else ""
                    ),
                    "pdf_url": download_url or pdf_url or "N/A",
                    "download_url": download_url or pdf_url or "N/A",
                    "detail_url": pdf_url or "N/A",
                    "原始数据": {k: row.get(k) for k in row.index},
                }
                announcements_ts.append(announcement)

            if not announcements_ts:
                print("   ℹ️ Tushare 公告数据为空")
                data["error"] = "东方财富公告接口无数据且Tushare公告数据为空"
                return data

            data["announcements"] = announcements_ts
            data["source"] = "tushare"
            data["data_success"] = True
            data["count"] = len(announcements_ts)

            pdf_analysis_ts: List[Dict[str, Any]] = []
            for ann in announcements_ts[:5]:
                pdf_url = ann.get("pdf_url")
                analysis_entry_ts: Dict[str, Any] = {
                    "date": ann.get("日期"),
                    "title": ann.get("公告标题"),
                    "pdf_url": pdf_url,
                    "text": None,
                    "success": False,
                }
                if pdf_url and pdf_url != "N/A":
                    pdf_text, saved_path = _download_and_parse_pdf(pdf_url, ann)
                    if pdf_text:
                        analysis_entry_ts["text"] = pdf_text
                        analysis_entry_ts["success"] = True
                    if saved_path:
                        analysis_entry_ts["saved_path"] = saved_path
                        ann["saved_path"] = saved_path
                else:
                    analysis_entry_ts["text"] = "未提供PDF链接。"
                pdf_analysis_ts.append(analysis_entry_ts)

            data["pdf_analysis"] = pdf_analysis_ts

        except Exception as e:  # noqa: BLE001
            debug_logger.error("获取公告数据失败", error=str(e), symbol=symbol)
            data["error"] = str(e)

        elapsed_time = time_module.time() - start_time
        debug_logger.info(
            "公告数据获取完成",
            symbol=symbol,
            success=data.get("data_success", False),
            count=data.get("count", 0),
            elapsed=f"{elapsed_time:.2f}s",
        )

        return data

    def get_chip_distribution_data(
        self,
        symbol: str,
        trade_date: str | None = None,
        current_price: float | None = None,
        analysis_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """获取筹码分布数据 - 使用 Tushare 的 cyq_perf 和 cyq_chips 接口（仅 A 股）。

        行为与根目录 unified_data_access.UnifiedDataAccess.get_chip_distribution_data 保持一致：
        - 返回 cyq_perf / cyq_chips 原始数据；
        - 生成 summary 汇总字段；
        - 调用 _analyze_chip_changes 生成 30 天筹码变化分析。
        """

        start_time = time_module.time()
        if analysis_date and not trade_date:
            trade_date = analysis_date

        debug_logger.info(
            "开始获取筹码分布数据",
            symbol=symbol,
            trade_date=trade_date,
            analysis_date=analysis_date,
            method="get_chip_distribution_data",
        )
        print(f"🎯 [UnifiedDataAccess] 正在获取 {symbol} 的筹码分布数据...")

        data: Dict[str, Any] = {
            "symbol": symbol,
            "data_success": False,
            "cyq_perf": None,
            "cyq_chips": None,
            "latest_date": None,
            "source": None,
        }

        # 只支持 A 股
        if not self._is_chinese_stock(symbol):
            data["error"] = "筹码分布数据仅支持中国A股股票"
            debug_logger.warning("筹码数据仅支持A股", symbol=symbol, is_chinese=False)
            print("   ⚠️ 筹码分布数据仅支持A股")
            return data

        try:
            if not data_source_manager.tushare_available:
                data["error"] = "Tushare数据源不可用，筹码分布数据需要Tushare支持"
                print("   ⚠️ Tushare不可用，无法获取筹码分布数据")
                return data

            print("   [Tushare] 正在获取筹码分布数据...")
            ts_code = data_source_manager._convert_to_ts_code(symbol)

            if not trade_date:
                trade_date = datetime.now().strftime("%Y%m%d")

            # 方法1: cyq_perf - 过去30天筹码及胜率数据
            try:
                print("   [方法1] 正在获取cyq_perf数据（筹码分布及胜率）...")
                end_date = trade_date
                start_date = (
                    datetime.strptime(end_date, "%Y%m%d")
                    - timedelta(days=30)
                ).strftime("%Y%m%d")

                with network_optimizer.apply():
                    df_perf = data_source_manager.tushare_api.cyq_perf(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date,
                    )

                if (
                    df_perf is not None
                    and isinstance(df_perf, pd.DataFrame)
                    and not df_perf.empty
                ):
                    perf_records = df_perf.to_dict("records")
                    latest_perf = perf_records[-1] if perf_records else None

                    data["cyq_perf"] = {
                        "data": perf_records,
                        "latest": latest_perf,
                        "count": len(perf_records),
                    }

                    if latest_perf:
                        data["latest_date"] = latest_perf.get(
                            "trade_date", trade_date
                        )

                    print(
                        f"   [方法1] ✅ 成功获取 {len(perf_records)} 条cyq_perf数据"
                    )
                    debug_logger.info(
                        "Tushare cyq_perf获取成功",
                        symbol=symbol,
                        count=len(perf_records),
                        latest_date=data.get("latest_date"),
                    )
                else:
                    print("   [方法1] ⚠️ 未获取到cyq_perf数据")
            except Exception as e:  # noqa: BLE001
                debug_logger.warning(
                    "Tushare cyq_perf获取失败", error=str(e), symbol=symbol
                )
                print(f"   [方法1] ❌ 失败: {e}")

            # 方法2: cyq_chips - 指定日期及回溯数日的筹码分布
            try:
                print("   [方法2] 正在获取cyq_chips数据（每日筹码分布）...")
                with network_optimizer.apply():
                    df_chips = data_source_manager.tushare_api.cyq_chips(
                        ts_code=ts_code, trade_date=trade_date
                    )

                if (
                    df_chips is not None
                    and isinstance(df_chips, pd.DataFrame)
                    and not df_chips.empty
                ):
                    chips_records = df_chips.to_dict("records")
                    data["cyq_chips"] = {
                        "data": chips_records,
                        "count": len(chips_records),
                        "trade_date": trade_date,
                    }

                    if not data.get("latest_date"):
                        data["latest_date"] = trade_date

                    print(
                        f"   [方法2] ✅ 成功获取 {len(chips_records)} 条cyq_chips数据"
                    )
                    debug_logger.info(
                        "Tushare cyq_chips获取成功",
                        symbol=symbol,
                        count=len(chips_records),
                        trade_date=trade_date,
                    )
                else:
                    print(
                        f"   [方法2] ⚠️ {trade_date}未获取到数据，尝试获取最近交易日数据..."
                    )
                    for i in range(1, 6):
                        try_date = (
                            datetime.strptime(trade_date, "%Y%m%d")
                            - timedelta(days=i)
                        ).strftime("%Y%m%d")
                        with network_optimizer.apply():
                            df_chips = data_source_manager.tushare_api.cyq_chips(
                                ts_code=ts_code,
                                trade_date=try_date,
                            )
                        if (
                            df_chips is not None
                            and isinstance(df_chips, pd.DataFrame)
                            and not df_chips.empty
                        ):
                            chips_records = df_chips.to_dict("records")
                            data["cyq_chips"] = {
                                "data": chips_records,
                                "count": len(chips_records),
                                "trade_date": try_date,
                            }
                            data["latest_date"] = try_date
                            print(
                                f"   [方法2] ✅ 成功获取 {try_date} 的 {len(chips_records)} 条cyq_chips数据"
                            )
                            break
                    else:
                        print(
                            "   [方法2] ⚠️ 最近5个自然日均未获取到cyq_chips数据"
                        )
            except Exception as e:  # noqa: BLE001
                debug_logger.warning(
                    "Tushare cyq_chips获取失败", error=str(e), symbol=symbol
                )
                print(f"   [方法2] ❌ 失败: {e}")

            # 成功性判断与汇总
            if data.get("cyq_perf") or data.get("cyq_chips"):
                data["data_success"] = True
                data["source"] = "tushare"

                summary: Dict[str, Any] = {}
                latest = None
                if data.get("cyq_perf") and data["cyq_perf"].get("latest"):
                    latest = data["cyq_perf"]["latest"]
                    summary["交易日期"] = latest.get("trade_date", "N/A")
                    summary["5%成本"] = latest.get("cost_5pct", "N/A")
                    summary["15%成本"] = latest.get("cost_15pct", "N/A")
                    summary["50%成本（中位）"] = latest.get("cost_50pct", "N/A")
                    summary["85%成本"] = latest.get("cost_85pct", "N/A")
                    summary["95%成本"] = latest.get("cost_95pct", "N/A")
                    summary["加权平均成本"] = latest.get("weight_avg", "N/A")
                    summary["历史最低"] = latest.get("his_low", "N/A")
                    summary["历史最高"] = latest.get("his_high", "N/A")

                    if (
                        pd.notna(latest.get("cost_50pct"))
                        and pd.notna(latest.get("cost_85pct"))
                        and pd.notna(latest.get("cost_15pct"))
                    ):
                        try:
                            cost_range = float(latest["cost_85pct"]) - float(
                                latest["cost_15pct"]
                            )
                            cost_center = float(latest["cost_50pct"])
                            if cost_center > 0:
                                concentration_pct = (
                                    cost_range / cost_center
                                ) * 100
                                if concentration_pct < 10:
                                    summary["筹码集中度"] = "高"
                                elif concentration_pct > 30:
                                    summary["筹码集中度"] = "低"
                                else:
                                    summary["筹码集中度"] = "中等"
                                summary["成本区间"] = f"{cost_range:.2f} ({concentration_pct:.1f}%)"
                        except Exception:  # noqa: BLE001
                            summary["筹码集中度"] = "N/A"
                    else:
                        summary["筹码集中度"] = "N/A"

                    summary["数据期数"] = data["cyq_perf"].get("count", 0)

                if (
                    data.get("cyq_perf")
                    and data["cyq_perf"].get("data")
                    and len(data["cyq_perf"]["data"]) >= 2
                ):
                    analysis_price = current_price
                    if (
                        (not analysis_price)
                        and latest
                        and pd.notna(latest.get("weight_avg"))
                    ):
                        try:
                            analysis_price = float(latest.get("weight_avg", 0))
                        except Exception:  # noqa: BLE001
                            analysis_price = None

                    change_analysis = self._analyze_chip_changes(
                        data["cyq_perf"]["data"], analysis_price
                    )
                    if change_analysis:
                        summary["30天变化分析"] = change_analysis
                        data["change_analysis"] = change_analysis

                if data.get("cyq_chips"):
                    summary["筹码分布数据点"] = data["cyq_chips"].get("count")
                    summary["筹码分布日期"] = data["cyq_chips"].get(
                        "trade_date", "N/A"
                    )

                data["summary"] = summary

                print(
                    f"   ✅ 筹码分布数据获取完成（数据日期: {data.get('latest_date', 'N/A')}）"
                )
                debug_logger.info(
                    "筹码分布数据获取成功",
                    symbol=symbol,
                    has_perf=data.get("cyq_perf") is not None,
                    has_chips=data.get("cyq_chips") is not None,
                    latest_date=data.get("latest_date"),
                )
            else:
                data["error"] = "未能获取筹码分布数据，cyq_perf和cyq_chips均失败"
                print("   ⚠️ 所有数据源均未获取到筹码数据")

        except Exception as e:  # noqa: BLE001
            debug_logger.error("获取筹码数据失败", error=str(e), symbol=symbol)
            print(f"   ❌ 获取筹码数据失败: {e}")
            try:
                import traceback

                traceback.print_exc()
            except Exception:  # noqa: BLE001
                pass
            data["error"] = str(e)

        elapsed_time = time_module.time() - start_time
        debug_logger.info(
            "筹码数据获取完成",
            symbol=symbol,
            success=data.get("data_success", False),
            source=data.get("source"),
            has_perf=data.get("cyq_perf") is not None,
            has_chips=data.get("cyq_chips") is not None,
            elapsed=f"{elapsed_time:.2f}s",
        )

        return data

    def _analyze_research_reports(self, df_reports: pd.DataFrame) -> Dict[str, Any]:
        """分析研报数据（增强版）。"""

        if df_reports is None or df_reports.empty:
            return {
                "total_reports": 0,
                "reports_data": [],
                "summary": {},
            }

        analysis: Dict[str, Any] = {
            "total_reports": len(df_reports),
            "reports_data": [],
            "summary": {},
        }

        all_contents: List[str] = []

        if len(df_reports) > 0:
            debug_logger.debug(
                f"report_rc接口返回的列名: {df_reports.columns.tolist()}"
            )

        for _, row in df_reports.iterrows():
            content = ""  # Tushare report_rc 当前不提供完整内容

            content_summary = ""
            if content:
                if len(content) > 500:
                    content_summary = content[:500] + "..."
                else:
                    content_summary = content
                all_contents.append(content)

            report_data = {
                "report_date": str(row.get("report_date", "")),
                "report_title": str(row.get("report_title", "")),
                "org_name": str(row.get("org_name", "")),
                "author_name": str(row.get("author_name", "")),
                "rating": str(row.get("rating", "")),
                "report_type": str(row.get("report_type", "")),
                "classify": str(row.get("classify", "")),
                "quarter": str(row.get("quarter", "")),
                "target_price_max": row.get("max_price"),
                "target_price_min": row.get("min_price"),
                "op_rt": row.get("op_rt"),
                "op_pr": row.get("op_pr"),
                "np": row.get("np"),
                "eps": row.get("eps"),
                "pe": row.get("pe"),
                "roe": row.get("roe"),
                "ev_ebitda": row.get("ev_ebitda"),
                "content": content,
                "content_summary": content_summary,
            }
            analysis["reports_data"].append(report_data)

        if all_contents:
            analysis["content_analysis"] = self._analyze_research_content(
                all_contents
            )

        if len(df_reports) > 0:
            if "org_name" in df_reports.columns:
                org_counts = df_reports["org_name"].value_counts()
                analysis["summary"]["top_institutions"] = (
                    org_counts.head(5).to_dict()
                )

            if "rating" in df_reports.columns:
                rating_counts = df_reports["rating"].value_counts()
                analysis["summary"]["rating_distribution"] = (
                    rating_counts.to_dict()
                )

                total = len(df_reports)
                buy_count = sum(
                    1
                    for r in rating_counts.index
                    if any(
                        keyword in str(r)
                        for keyword in ["买入", "增持", "推荐", "强推"]
                    )
                )
                neutral_count = sum(
                    1
                    for r in rating_counts.index
                    if any(
                        keyword in str(r)
                        for keyword in ["持有", "中性", "观望"]
                    )
                )
                sell_count = sum(
                    1
                    for r in rating_counts.index
                    if any(
                        keyword in str(r)
                        for keyword in ["卖出", "减持", "回避"]
                    )
                )

                analysis["summary"]["rating_ratio"] = {
                    "buy_ratio": round(buy_count / total * 100, 2)
                    if total > 0
                    else 0,
                    "neutral_ratio": round(neutral_count / total * 100, 2)
                    if total > 0
                    else 0,
                    "sell_ratio": round(sell_count / total * 100, 2)
                    if total > 0
                    else 0,
                }

            if "max_price" in df_reports.columns:
                max_prices = df_reports["max_price"].dropna()
                if not max_prices.empty:
                    analysis["summary"]["target_price_stats"] = {
                        "max": float(max_prices.max()),
                        "min": float(max_prices.min()),
                        "avg": float(max_prices.mean()),
                        "count": len(max_prices),
                    }
            elif "min_price" in df_reports.columns:
                min_prices = df_reports["min_price"].dropna()
                if not min_prices.empty:
                    analysis["summary"]["target_price_stats"] = {
                        "max": float(min_prices.max()),
                        "min": float(min_prices.min()),
                        "avg": float(min_prices.mean()),
                        "count": len(min_prices),
                    }

            for col in ["eps", "pe", "roe"]:
                if col in df_reports.columns:
                    values = df_reports[col].dropna()
                    if not values.empty:
                        analysis["summary"][f"{col}_stats"] = {
                            "max": float(values.max()),
                            "min": float(values.min()),
                            "avg": float(values.mean()),
                        }

            if len(df_reports) > 0:
                latest_report = df_reports.iloc[0]
                analysis["summary"]["latest_report"] = {
                    "date": str(latest_report.get("report_date", "")),
                    "title": str(latest_report.get("report_title", "")),
                    "org": str(latest_report.get("org_name", "")),
                    "rating": str(latest_report.get("rating", "")),
                    "target_price": latest_report.get("max_price")
                    or latest_report.get("min_price"),
                }

        if "content_analysis" not in analysis:
            analysis["content_analysis"] = {}

        return analysis

    def _analyze_research_content(self, contents: List[str]) -> Dict[str, Any]:
        """分析研报内容。"""

        if not contents:
            return {
                "has_content": False,
                "total_length": 0,
                "avg_length": 0,
                "key_topics": [],
                "sentiment_analysis": {},
            }

        combined_content = " ".join([c for c in contents if c])
        total_length = len(combined_content)
        avg_length = total_length / len(contents) if contents else 0

        key_topics: List[str] = []
        common_keywords = [
            "增长",
            "业绩",
            "盈利",
            "收入",
            "净利润",
            "EPS",
            "ROE",
            "估值",
            "买入",
            "持有",
            "推荐",
            "目标价",
            "风险",
            "机会",
            "前景",
            "行业",
            "市场",
            "竞争",
            "优势",
            "创新",
            "转型",
            "扩张",
        ]

        content_lower = combined_content.lower()
        for keyword in common_keywords:
            if keyword in content_lower:
                key_topics.append(keyword)

        positive_words = [
            "增长",
            "提升",
            "改善",
            "利好",
            "看好",
            "买入",
            "推荐",
            "机会",
            "优势",
        ]
        negative_words = [
            "下降",
            "下滑",
            "风险",
            "担忧",
            "卖出",
            "减持",
            "挑战",
            "困难",
        ]

        positive_count = sum(1 for word in positive_words if word in content_lower)
        negative_count = sum(1 for word in negative_words if word in content_lower)

        sentiment = "neutral"
        if positive_count > negative_count * 1.5:
            sentiment = "positive"
        elif negative_count > positive_count * 1.5:
            sentiment = "negative"

        return {
            "has_content": True,
            "total_reports_with_content": len([c for c in contents if c]),
            "total_length": total_length,
            "avg_length": round(avg_length, 0),
            "key_topics": key_topics[:10],
            "sentiment_analysis": {
                "sentiment": sentiment,
                "positive_signals": positive_count,
                "negative_signals": negative_count,
                "sentiment_score": round(
                    (positive_count - negative_count)
                    / max(positive_count + negative_count, 1)
                    * 100,
                    2,
                ),
            },
        }

    def _analyze_chip_changes(
        self, perf_data: list, current_price: float | None = None
    ) -> Optional[Dict[str, Any]]:
        """分析过去30天筹码分布变化，判断主力资金行为。

        直接迁移自旧 unified_data_access._analyze_chip_changes，保持分析口径一致。
        """

        if not perf_data or len(perf_data) < 2:
            return None

        try:
            sorted_data = sorted(
                perf_data,
                key=lambda x: str(x.get("trade_date", "")),
                reverse=False,
            )
            earliest = sorted_data[0]
            latest = sorted_data[-1]

            analysis: Dict[str, Any] = {
                "period": f"{earliest.get('trade_date', 'N/A')} 至 {latest.get('trade_date', 'N/A')}",
                "days_count": len(sorted_data),
                "cost_changes": {},
                "concentration_changes": {},
                "main_force_behavior": {},
                "chip_peak_analysis": {},
            }

            # 1. 成本价格变化
            cost_fields = [
                "cost_5pct",
                "cost_15pct",
                "cost_50pct",
                "cost_85pct",
                "cost_95pct",
                "weight_avg",
            ]
            for field in cost_fields:
                earliest_val = earliest.get(field)
                latest_val = latest.get(field)
                if pd.notna(earliest_val) and pd.notna(latest_val):
                    try:
                        change = float(latest_val) - float(earliest_val)
                        change_pct = (
                            (change / float(earliest_val)) * 100
                            if float(earliest_val) > 0
                            else 0
                        )
                        analysis["cost_changes"][field] = {
                            "earliest": round(float(earliest_val), 2),
                            "latest": round(float(latest_val), 2),
                            "change": round(change, 2),
                            "change_pct": round(change_pct, 2),
                        }
                    except Exception:  # noqa: BLE001
                        pass

            # 2. 筹码集中度变化
            def calc_concentration(record: Dict[str, Any]):
                try:
                    cost_15 = float(record.get("cost_15pct", 0))
                    cost_85 = float(record.get("cost_85pct", 0))
                    cost_50 = float(record.get("cost_50pct", 0))
                    if cost_50 > 0:
                        range_pct = ((cost_85 - cost_15) / cost_50) * 100
                        if range_pct < 10:
                            return "高", range_pct
                        if range_pct > 30:
                            return "低", range_pct
                        return "中", range_pct
                except Exception:  # noqa: BLE001
                    pass
                return None, None

            earliest_conc_level, earliest_conc_pct = calc_concentration(earliest)
            latest_conc_level, latest_conc_pct = calc_concentration(latest)

            if earliest_conc_level and latest_conc_level:
                analysis["concentration_changes"] = {
                    "earliest_level": earliest_conc_level,
                    "latest_level": latest_conc_level,
                    "earliest_pct": round(earliest_conc_pct, 2)
                    if earliest_conc_pct
                    else None,
                    "latest_pct": round(latest_conc_pct, 2)
                    if latest_conc_pct
                    else None,
                    "trend": (
                        "提升"
                        if latest_conc_pct < earliest_conc_pct
                        else "下降"
                        if latest_conc_pct > earliest_conc_pct
                        else "稳定"
                    ),
                }

            # 3. 筹码峰移动分析
            if (
                "cost_changes" in analysis
                and "weight_avg" in analysis["cost_changes"]
            ):
                weight_avg_change = analysis["cost_changes"]["weight_avg"][
                    "change"
                ]
                cost_50_change = analysis["cost_changes"].get("cost_50pct", {}).get(
                    "change", 0
                )

                if weight_avg_change > 0 and cost_50_change > 0:
                    analysis["chip_peak_analysis"]["peak_direction"] = "上移"
                    analysis["chip_peak_analysis"]["peak_speed"] = (
                        "快速"
                        if abs(weight_avg_change) > abs(cost_50_change) * 1.5
                        else "缓慢"
                    )
                elif weight_avg_change < 0 and cost_50_change < 0:
                    analysis["chip_peak_analysis"]["peak_direction"] = "下移"
                    analysis["chip_peak_analysis"]["peak_speed"] = (
                        "快速"
                        if abs(weight_avg_change) > abs(cost_50_change) * 1.5
                        else "缓慢"
                    )
                else:
                    analysis["chip_peak_analysis"]["peak_direction"] = "震荡"
                    analysis["chip_peak_analysis"]["peak_speed"] = "不稳定"

            # 4. 主力资金行为判断
            main_force_signals: list[str] = []
            behavior_score = 0

            if analysis["concentration_changes"].get("trend") == "提升":
                if latest_conc_level in ["高", "中"]:
                    main_force_signals.append("集中度提升，可能主力收集筹码")
                    behavior_score += 2

            if "weight_avg" in analysis["cost_changes"]:
                weight_change = analysis["cost_changes"]["weight_avg"]["change"]
                if weight_change < 0 and current_price:
                    try:
                        price_vs_cost = (
                            (
                                float(current_price)
                                - float(latest.get("weight_avg", 0))
                            )
                            / float(latest.get("weight_avg", 0))
                            * 100
                        )
                        if price_vs_cost < 10:
                            main_force_signals.append(
                                "平均成本下降且股价接近成本，可能低位吸筹"
                            )
                            behavior_score += 2
                    except Exception:  # noqa: BLE001
                        pass

            if analysis["chip_peak_analysis"].get("peak_direction") == "上移":
                if (
                    "cost_85pct" in analysis["cost_changes"]
                    and "cost_15pct" in analysis["cost_changes"]
                ):
                    high_cost_increase = analysis["cost_changes"]["cost_85pct"][
                        "change"
                    ]
                    low_cost_change = analysis["cost_changes"]["cost_15pct"][
                        "change"
                    ]
                    if (
                        high_cost_increase > 0
                        and abs(high_cost_increase)
                        > abs(low_cost_change) * 1.5
                    ):
                        main_force_signals.append(
                            "高位成本快速上升，筹码峰上移，可能获利出逃"
                        )
                        behavior_score -= 3

            if analysis["concentration_changes"].get("trend") == "下降":
                if latest_conc_level == "低":
                    main_force_signals.append(
                        "集中度下降且区间扩大，可能散户接盘"
                    )
                    behavior_score -= 2

            if (
                "cost_5pct" in analysis["cost_changes"]
                and "cost_50pct" in analysis["cost_changes"]
            ):
                low_stable = abs(
                    analysis["cost_changes"]["cost_5pct"]["change"]
                ) < abs(
                    analysis["cost_changes"]["cost_5pct"]["earliest"]
                ) * 0.1
                mid_up = analysis["cost_changes"]["cost_50pct"]["change"] > 0
                if low_stable and mid_up:
                    main_force_signals.append(
                        "低位成本稳定，中位成本上移，可能洗盘后拉升"
                    )
                    behavior_score += 1

            if behavior_score >= 3:
                main_force_judgment = "收集低价筹码"
                main_force_confidence = "高"
            elif behavior_score >= 1:
                main_force_judgment = "可能收集筹码"
                main_force_confidence = "中"
            elif behavior_score <= -3:
                main_force_judgment = "获利出逃"
                main_force_confidence = "高"
            elif behavior_score <= -1:
                main_force_judgment = "可能获利了结"
                main_force_confidence = "中"
            else:
                main_force_judgment = "震荡整理"
                main_force_confidence = "低"

            analysis["main_force_behavior"] = {
                "judgment": main_force_judgment,
                "confidence": main_force_confidence,
                "score": behavior_score,
                "signals": main_force_signals,
                "description": self._generate_main_force_description(
                    main_force_judgment, main_force_signals, analysis
                ),
            }

            return analysis

        except Exception as e:  # noqa: BLE001
            debug_logger.warning("筹码变化分析失败", error=str(e))
            try:
                import traceback

                traceback.print_exc()
            except Exception:  # noqa: BLE001
                pass
            return None

    def _generate_main_force_description(
        self, judgment: str, signals: list, analysis: Dict[str, Any]
    ) -> str:
        """生成主力行为描述文本（迁移自旧版实现）。"""

        desc = f"主力行为判断: {judgment}\n"
        desc += (
            f"置信度: {analysis.get('main_force_behavior', {}).get('confidence', 'N/A')}\n\n"
        )

        if signals:
            desc += "关键信号:\n"
            for i, signal in enumerate(signals, 1):
                desc += f"{i}. {signal}\n"

        peak = analysis.get("chip_peak_analysis", {})
        desc += (
            f"\n筹码峰变化: {peak.get('peak_direction', 'N/A')} "
            f"({peak.get('peak_speed', 'N/A')})\n"
        )

        if "cost_changes" in analysis and "weight_avg" in analysis["cost_changes"]:
            change_info = analysis["cost_changes"]["weight_avg"]
            desc += (
                f"平均成本变化: {change_info['earliest']:.2f} → {change_info['latest']:.2f} "
                f"({change_info['change']:+.2f}, {change_info['change_pct']:+.2f}%)\n"
            )

        conc = analysis.get("concentration_changes", {})
        if conc:
            desc += (
                f"筹码集中度变化: {conc.get('earliest_level', 'N/A')} → {conc.get('latest_level', 'N/A')} "
                f"({conc.get('trend', 'N/A')})"
            )

        return desc

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------

    def _get_appropriate_trade_date(
        self, analysis_date: Optional[str] = None
    ) -> str:
        """选择合适的交易日，用于 Tushare 日线/估值查询。

        这是原始实现中的一个简化版本：
        - 若给定 analysis_date，则直接返回；
        - 否则使用当前日期（若非交易日，回退到最近的交易日）。
        """

        if analysis_date:
            return analysis_date

        # 无显式分析日期时，尝试使用最近的一个交易日
        try:
            if not data_source_manager.tushare_available:
                return datetime.now().strftime("%Y%m%d")

            today = datetime.now().strftime("%Y%m%d")
            with network_optimizer.apply():
                cal = data_source_manager.tushare_api.trade_cal(
                    start_date=(datetime.now() - timedelta(days=10)).strftime(
                        "%Y%m%d"
                    ),
                    end_date=today,
                    is_open=1,
                )
            if cal is None or cal.empty:
                return today
            trade_dates = cal["cal_date"].tolist()
            return str(trade_dates[-1])
        except Exception:  # noqa: BLE001
            return datetime.now().strftime("%Y%m%d")

    def get_beta_coefficient(self, symbol: str) -> Optional[float]:
        """占位实现：Beta 计算逻辑沿用原始数据源实现。

        当前版本直接委托给 data_source_manager（若其提供该能力），
        否则返回 None。
        """

        try:
            if hasattr(data_source_manager, "get_beta_coefficient"):
                return data_source_manager.get_beta_coefficient(symbol)
        except Exception as e:  # noqa: BLE001
            debug_logger.debug("get_beta_coefficient失败", error=str(e), symbol=symbol)
        return None

    def get_52week_high_low(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取 52 周高低位（如果数据源支持）。"""

        try:
            if hasattr(data_source_manager, "get_52week_high_low"):
                return data_source_manager.get_52week_high_low(symbol)
        except Exception as e:  # noqa: BLE001
            debug_logger.debug("get_52week_high_low失败", error=str(e), symbol=symbol)
        return None

    def _is_chinese_stock(self, symbol: str) -> bool:
        """判断是否为中国 A 股（基于 6 位数字代码的简单规则）。"""

        return symbol.isdigit() and len(symbol) == 6
