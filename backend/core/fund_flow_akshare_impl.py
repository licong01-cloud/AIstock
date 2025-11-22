"""
资金流向数据获取模块（akshare版本） - next_app 内部实现
从根目录 fund_flow_akshare.py 迁移而来，行为保持一致，只是改为依赖
next_app 内部的 data_source_manager_impl 和 infra.network_optimizer。
"""

import pandas as pd
import sys
import io
import warnings
from datetime import datetime, timedelta
import akshare as ak

from .data_source_manager_impl import data_source_manager
from ..infra.network_optimizer import network_optimizer

warnings.filterwarnings("ignore")


# 设置标准输出编码为UTF-8（仅在命令行环境，避免streamlit冲突）
def _setup_stdout_encoding() -> None:
    """仅在命令行环境设置标准输出编码。

    为保持与旧实现兼容，保留该逻辑；在 FastAPI 进程中通常不会触发。
    """

    if sys.platform == "win32" and not hasattr(sys.stdout, "_original_stream"):
        try:
            # 检测是否在streamlit环境中
            import streamlit  # type: ignore  # noqa: F401

            # 在streamlit中不修改stdout
            return
        except ImportError:
            # 不在streamlit环境，可以安全修改
            try:
                sys.stdout = io.TextIOWrapper(  # type: ignore[assignment]
                    sys.stdout.buffer, encoding="utf-8", errors="ignore"
                )
            except Exception:
                pass


_setup_stdout_encoding()


class FundFlowAkshareDataFetcher:
    """资金流向数据获取类（使用akshare数据源）。

    该实现从根目录 fund_flow_akshare 迁移而来，接口和行为保持一致，
    仅依赖 next_app 内部的数据源管理器和网络优化器。
    """

    def __init__(self) -> None:
        self.days = 30  # 获取最近30个交易日
        self.available = True
        print("[OK] 资金流向数据获取器初始化成功（akshare数据源）")

    def get_fund_flow_data(self, symbol: str, analysis_date: str | None = None) -> dict:
        """获取个股资金流向数据。

        Args:
            symbol: 股票代码（6位数字）
            analysis_date: 分析时间点（可选），格式："YYYYMMDD"
        """

        data: dict = {
            "symbol": symbol,
            "fund_flow_data": None,
            "data_success": False,
            "source": None,
        }

        # 只支持中国股票
        if not self._is_chinese_stock(symbol):
            data["error"] = "资金流向数据仅支持中国A股股票"
            return data

        try:
            print(f"[资金流向] 正在获取 {symbol} 的资金流向数据...")

            # 确定市场
            market = self._get_market(symbol)

            # 获取资金流向数据
            fund_flow_data = self._get_individual_fund_flow(
                symbol, market, analysis_date=analysis_date
            )

            if fund_flow_data:
                data["fund_flow_data"] = fund_flow_data
                data["source"] = fund_flow_data.get("source", "unknown")
                print(
                    f"   [OK] 成功获取 "
                    f"{len(fund_flow_data.get('data', []))} 个交易日的资金流向数据（数据源: {data['source']}）"
                )
                data["data_success"] = True
                print("[完成] 资金流向数据获取完成")
            else:
                print("[警告] 未能获取到资金流向数据")

        except Exception as e:  # noqa: BLE001
            print(f"[ERROR] 获取资金流向数据失败: {e}")
            data["error"] = str(e)

        return data

    def _is_chinese_stock(self, symbol: str) -> bool:
        """判断是否为中国股票。"""

        return symbol.isdigit() and len(symbol) == 6

    def _get_market(self, symbol: str) -> str:
        """根据股票代码判断市场。

        上海证券交易所: sh (60开头, 688开头)
        深圳证券交易所: sz (00开头, 30开头)
        北京证券交易所: bj (8开头, 4开头)
        """

        if symbol.startswith("60") or symbol.startswith("688"):
            return "sh"
        if symbol.startswith("00") or symbol.startswith("30"):
            return "sz"
        if symbol.startswith("8") or symbol.startswith("4"):
            return "bj"
        # 默认深圳
        return "sz"

    def _get_individual_fund_flow(
        self, symbol: str, market: str, analysis_date: str | None = None
    ) -> dict | None:
        """获取个股资金流向数据（优先Tushare，失败则使用Akshare）。"""

        source: str | None = None
        try:
            df = None

            # 优先使用Tushare的 moneyflow 接口
            if data_source_manager.tushare_available:
                try:
                    print("   [Tushare] 正在获取资金流向数据（优先数据源）...")
                    ts_code = data_source_manager._convert_to_ts_code(symbol)  # type: ignore[attr-defined]

                    # 计算日期范围
                    if analysis_date:
                        end_date = (
                            analysis_date.replace("-", "")
                            if "-" in analysis_date
                            else analysis_date
                        )
                        end_dt = datetime.strptime(end_date, "%Y%m%d")
                        start_dt = end_dt - timedelta(days=self.days * 2)
                        start_date = start_dt.strftime("%Y%m%d")
                    else:
                        end_date = datetime.now().strftime("%Y%m%d")
                        start_dt = datetime.now() - timedelta(days=self.days * 2)
                        start_date = start_dt.strftime("%Y%m%d")

                    with network_optimizer.apply():
                        df = data_source_manager.tushare_api.moneyflow(
                            ts_code=ts_code,
                            start_date=start_date,
                            end_date=end_date,
                        )

                    if df is not None and not df.empty:
                        # 按日期倒序，最新在前
                        df = df.sort_values("trade_date", ascending=False)

                        # 转换日期格式为 YYYY-MM-DD
                        if "trade_date" in df.columns:
                            df["trade_date"] = pd.to_datetime(
                                df["trade_date"], format="%Y%m%d"
                            ).dt.strftime("%Y-%m-%d")

                        # 标准化列名以匹配 akshare 格式
                        df = df.rename(
                            columns={
                                "trade_date": "日期",
                                "buy_sm_amount": "小单买入",
                                "sell_sm_amount": "小单卖出",
                                "buy_md_amount": "中单买入",
                                "sell_md_amount": "中单卖出",
                                "buy_lg_amount": "大单买入",
                                "sell_lg_amount": "大单卖出",
                                "buy_elg_amount": "超大单买入",
                                "sell_elg_amount": "超大单卖出",
                                "net_mf_amount": "主力净流入-净额",
                            }
                        )

                        # 计算各类型净流入
                        if "小单买入" in df.columns and "小单卖出" in df.columns:
                            df["小单净流入-净额"] = df["小单买入"] - df["小单卖出"]
                        if "中单买入" in df.columns and "中单卖出" in df.columns:
                            df["中单净流入-净额"] = df["中单买入"] - df["中单卖出"]
                        if "大单买入" in df.columns and "大单卖出" in df.columns:
                            df["大单净流入-净额"] = df["大单买入"] - df["大单卖出"]
                        if "超大单买入" in df.columns and "超大单卖出" in df.columns:
                            df["超大单净流入-净额"] = df["超大单买入"] - df["超大单卖出"]

                        # 主力净流入（大单+超大单）
                        if (
                            "大单净流入-净额" in df.columns
                            and "超大单净流入-净额" in df.columns
                        ):
                            df["主力净流入-净额"] = (
                                df["大单净流入-净额"] + df["超大单净流入-净额"]
                            )

                        # 限制为最近 N 天
                        df = df.head(self.days)

                        # 获取收盘价和涨跌幅，补充展示
                        try:
                            with network_optimizer.apply():
                                daily_df = data_source_manager.tushare_api.daily(
                                    ts_code=ts_code,
                                    start_date=start_date,
                                    end_date=end_date,
                                    fields="trade_date,close,pct_chg",
                                )
                            if daily_df is not None and not daily_df.empty:
                                daily_df["trade_date"] = pd.to_datetime(
                                    daily_df["trade_date"], format="%Y%m%d"
                                ).dt.strftime("%Y-%m-%d")
                                daily_df = daily_df.rename(
                                    columns={
                                        "trade_date": "日期",
                                        "close": "收盘价",
                                        "pct_chg": "涨跌幅",
                                    }
                                )
                                df = df.merge(
                                    daily_df[["日期", "收盘价", "涨跌幅"]],
                                    on="日期",
                                    how="left",
                                )
                        except Exception as e:  # noqa: BLE001
                            print(f"   [Tushare] ⚠️ 获取收盘价数据失败: {e}")

                        print(
                            f"   [Tushare] ✅ 成功获取 {len(df)} 条资金流向数据"
                        )
                        source = "tushare"
                    else:
                        print(
                            "   [Tushare] ❌ 未找到资金流向数据，尝试备用数据源..."
                        )
                        df = None
                except Exception as te:  # noqa: BLE001
                    print(f"   [Tushare] ❌ 获取失败: {te}，尝试备用数据源...")
                    df = None
            else:
                df = None

            # Tushare 失败或不可用时，尝试 Akshare
            if df is None or df.empty:
                print(
                    f"   [Akshare] 正在获取资金流向数据（备用数据源，市场: {market})..."
                )
                try:
                    with network_optimizer.apply():
                        df = ak.stock_individual_fund_flow(stock=symbol, market=market)

                    if df is None or df.empty:
                        print("   [Akshare] ❌ 未找到资金流向数据")
                        return None

                    # 取最近 N 天，并倒序
                    df = df.tail(self.days)
                    df = df.iloc[::-1].reset_index(drop=True)
                    source = "akshare"
                except Exception as ae:  # noqa: BLE001
                    print(f"   [Akshare] ❌ 获取失败: {ae}")
                    return None
            else:
                # Tushare 数据已是倒序
                df = df.reset_index(drop=True)

            # 转换为字典列表
            data_list: list[dict] = []
            for _, row in df.iterrows():
                item: dict = {}
                for col in df.columns:
                    value = row.get(col)
                    if value is None or (
                        isinstance(value, float) and pd.isna(value)
                    ):
                        continue
                    try:
                        if isinstance(value, (int, float)):
                            item[col] = value
                        else:
                            item[col] = str(value)
                    except Exception:  # noqa: BLE001
                        item[col] = "N/A"
                if item:
                    data_list.append(item)

            if source is None:
                source = "akshare"

            return {
                "data": data_list,
                "days": len(data_list),
                "columns": df.columns.tolist(),
                "market": market,
                "source": source,
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

        except Exception as e:  # noqa: BLE001
            print(f"   获取资金流向数据异常: {e}")
            import traceback

            traceback.print_exc()
            return None

    def format_fund_flow_for_ai(self, data: dict) -> str:
        """将资金流向数据格式化为适合 AI 阅读的长文本。"""

        if not data or not data.get("data_success"):
            return "未能获取资金流向数据"

        text_parts: list[str] = []
        fund_flow_data = data.get("fund_flow_data")
        if fund_flow_data:
            text_parts.append(
                f"""
【个股资金流向数据 - {data.get('source', 'unknown')}】
股票代码：{data.get('symbol', 'N/A')}
市场：{fund_flow_data.get('market', 'N/A').upper()}
交易日数：最近{fund_flow_data.get('days', 0)}个交易日
查询时间：{fund_flow_data.get('query_time', 'N/A')}

═══════════════════════════════════════
[资金流向详细数据]
═══════════════════════════════════════
"""
            )

            for idx, item in enumerate(fund_flow_data.get("data", []), 1):
                date = item.get("日期", "N/A")
                close_price = item.get("收盘价", "N/A")
                change_pct = item.get("涨跌幅", "N/A")

                text_parts.append(
                    f"""
第 {idx} 个交易日 ({date}):
  基本信息:
    - 收盘价: {close_price}
    - 涨跌幅: {change_pct}%
  
  主力资金:
    - 主力净流入-净额: {item.get('主力净流入-净额', 'N/A')}
    - 主力净流入-净占比: {item.get('主力净流入-净占比', 'N/A')}%
  
  超大单:
    - 超大单净流入-净额: {item.get('超大单净流入-净额', 'N/A')}
    - 超大单净流入-净占比: {item.get('超大单净流入-净占比', 'N/A')}%
  
  大单:
    - 大单净流入-净额: {item.get('大单净流入-净额', 'N/A')}
    - 大单净流入-净占比: {item.get('大单净流入-净占比', 'N/A')}%
  
  中单:
    - 中单净流入-净额: {item.get('中单净流入-净额', 'N/A')}
    - 中单净流入-净占比: {item.get('中单净流入-净占比', 'N/A')}%
  
  小单:
    - 小单净流入-净额: {item.get('小单净流入-净额', 'N/A')}
    - 小单净流入-净占比: {item.get('小单净流入-净占比', 'N/A')}%
"""
                )

            # 统计汇总
            text_parts.append(
                """
═══════════════════════════════════════
[统计汇总 - 最近30个交易日]
═══════════════════════════════════════
"""
            )

            data_list = fund_flow_data.get("data", [])
            if data_list:
                main_inflow_list = [
                    item.get("主力净流入-净额", 0)
                    for item in data_list
                    if isinstance(item.get("主力净流入-净额"), (int, float))
                ]
                if main_inflow_list:
                    total_main_inflow = sum(main_inflow_list)
                    avg_main_inflow = total_main_inflow / len(main_inflow_list)
                    positive_days = len([x for x in main_inflow_list if x > 0])
                    negative_days = len([x for x in main_inflow_list if x < 0])

                    text_parts.append(
                        f"""
主力资金统计:
  - 累计净流入: {total_main_inflow:.2f}
  - 平均每日净流入: {avg_main_inflow:.2f}
  - 净流入天数: {positive_days}天
  - 净流出天数: {negative_days}天
  - 净流入占比: {positive_days/len(main_inflow_list)*100:.1f}%
"""
                    )

                change_pct_list = [
                    item.get("涨跌幅", 0)
                    for item in data_list
                    if isinstance(item.get("涨跌幅"), (int, float))
                ]
                if change_pct_list:
                    avg_change = sum(change_pct_list) / len(change_pct_list)
                    up_days = len([x for x in change_pct_list if x > 0])
                    down_days = len([x for x in change_pct_list if x < 0])

                    text_parts.append(
                        f"""
股价统计:
  - 平均涨跌幅: {avg_change:.2f}%
  - 上涨天数: {up_days}天
  - 下跌天数: {down_days}天
  - 上涨占比: {up_days/len(change_pct_list)*100:.1f}%
"""
                    )

        return "\n".join(text_parts)
