"""
新闻数据获取模块（next_app 内部实现）
使用 akshare 获取股票的最新新闻信息（替代 qstock）
从根目录 qstock_news_data.py 迁移而来，行为保持一致，只是改为依赖
next_app 内部的 infra.network_optimizer。
"""

import pandas as pd
import sys
import warnings
from datetime import datetime
import akshare as ak

from ..infra.network_optimizer import network_optimizer

warnings.filterwarnings("ignore")


# 设置标准输出编码为UTF-8（仅在命令行环境，避免streamlit冲突）
def _setup_stdout_encoding() -> None:
    """Configure the real Windows console without replacing captured streams."""
    if sys.platform != "win32" or sys.stdout is not sys.__stdout__:
        return
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        try:
            reconfigure(encoding="utf-8", errors="ignore")
        except (AttributeError, OSError, ValueError):
            pass


_setup_stdout_encoding()


class QStockNewsDataFetcher:
    """新闻数据获取类（使用 akshare 作为数据源）"""

    def __init__(self) -> None:
        self.max_items = 30  # 最多获取的新闻数量
        self.available = True
        print("✓ 新闻数据获取器初始化成功（akshare 数据源）")

    def get_stock_news(self, symbol: str, analysis_date: str | None = None) -> dict:
        """获取股票的新闻数据。

        Args:
            symbol: 股票代码（6位数字）
            analysis_date: 分析时间点（可选），格式："YYYYMMDD"
        """

        data: dict = {
            "symbol": symbol,
            "news_data": None,
            "data_success": False,
            "source": "qstock",
        }

        if not self.available:
            data["error"] = "qstock 库未安装或不可用"
            return data

        # 只支持中国股票
        if not self._is_chinese_stock(symbol):
            data["error"] = "新闻数据仅支持中国 A 股股票"
            return data

        try:
            # 获取新闻数据
            print(f"📰 正在使用 qstock 获取 {symbol} 的最新新闻...")
            news_data = self._get_news_data(symbol)

            if news_data:
                data["news_data"] = news_data
                print(f"   ✓ 成功获取 {len(news_data.get('items', []))} 条新闻")
                data["data_success"] = True
                print("✅ 新闻数据获取完成")
            else:
                print("⚠️ 未能获取到新闻数据")

        except Exception as e:  # noqa: BLE001
            print(f"❌ 获取新闻数据失败: {e}")
            data["error"] = str(e)

        return data

    def _is_chinese_stock(self, symbol: str) -> bool:
        """判断是否为中国股票"""

        return symbol.isdigit() and len(symbol) == 6

    def _get_news_data(self, symbol: str) -> dict | None:
        """获取新闻数据（使用 akshare）。"""

        try:
            print("   使用 akshare 获取新闻...")

            news_items: list[dict] = []

            # 方法1: 尝试获取个股新闻（东方财富）
            try:
                # stock_news_em(symbol="600519") - 东方财富个股新闻
                with network_optimizer.apply():
                    df = ak.stock_news_em(symbol=symbol)

                if df is not None and not df.empty:
                    print(f"   ✓ 从东方财富获取到 {len(df)} 条新闻")

                    # 处理 DataFrame，提取新闻
                    for _, row in df.head(self.max_items).iterrows():
                        item: dict = {"source": "东方财富"}

                        # 提取所有列
                        for col in df.columns:
                            value = row.get(col)

                            # 跳过空值
                            if value is None or (
                                isinstance(value, float) and pd.isna(value)
                            ):
                                continue

                            # 保存字段
                            try:
                                item[col] = str(value)
                            except Exception:  # noqa: BLE001
                                item[col] = "无法解析"

                        if len(item) > 1:  # 如果有数据才添加
                            news_items.append(item)

            except Exception as e:  # noqa: BLE001
                print(f"   ⚠ 从东方财富获取失败: {e}")

            # 方法2: 如果没有获取到，尝试获取新浪财经新闻
            if not news_items:
                try:
                    # stock_zh_a_spot_em() - 获取股票信息，包含代码和名称
                    with network_optimizer.apply():
                        df_info = ak.stock_zh_a_spot_em()

                    # 查找股票名称
                    stock_name: str | None = None
                    if df_info is not None and not df_info.empty:
                        match = df_info[df_info["代码"] == symbol]
                        if not match.empty:
                            stock_name = str(match.iloc[0]["名称"])
                            print(f"   找到股票名称: {stock_name}")

                    # 使用股票名称搜索新闻
                    if stock_name:
                        try:
                            with network_optimizer.apply():
                                df = ak.stock_news_sina(symbol=stock_name)
                            if df is not None and not df.empty:
                                print(f"   ✓ 从新浪财经获取到 {len(df)} 条新闻")

                                for _, row in df.head(self.max_items).iterrows():
                                    item = {"source": "新浪财经"}

                                    for col in df.columns:
                                        value = row.get(col)
                                        if value is None or (
                                            isinstance(value, float)
                                            and pd.isna(value)
                                        ):
                                            continue
                                        try:
                                            item[col] = str(value)
                                        except Exception:  # noqa: BLE001
                                            item[col] = "无法解析"

                                    if len(item) > 1:
                                        news_items.append(item)
                        except Exception as e:  # noqa: BLE001
                            print(f"   ⚠ 新浪财经新闻过滤失败: {e}")

                except Exception as e:  # noqa: BLE001
                    print(f"   ⚠ 从新浪财经获取失败: {e}")

            # 方法3: 尝试获取财联社电报
            if not news_items or len(news_items) < 5:
                try:
                    # stock_info_global_cls(symbol="全部") - 财联社电报
                    with network_optimizer.apply():
                        df_cls = ak.stock_info_global_cls(symbol="全部")

                    if df_cls is not None and not df_cls.empty:
                        # 优先按标题包含股票代码过滤；如有内容列再一并过滤
                        cond = df_cls["标题"].astype(str).str.contains(
                            symbol, na=False
                        )
                        if "内容" in df_cls.columns:
                            cond = cond | df_cls["内容"].astype(str).str.contains(
                                symbol, na=False
                            )

                        df_filtered = df_cls[cond]

                        if not df_filtered.empty:
                            print(
                                f"   ✓ 从财联社获取到 {len(df_filtered)} 条相关新闻"
                            )

                            remain = self.max_items - len(news_items)
                            for _, row in df_filtered.head(remain).iterrows():
                                item = {"source": "财联社"}

                                for col in df_filtered.columns:
                                    value = row.get(col)
                                    if value is None or (
                                        isinstance(value, float)
                                        and pd.isna(value)
                                    ):
                                        continue
                                    try:
                                        item[col] = str(value)
                                    except Exception:  # noqa: BLE001
                                        item[col] = "无法解析"

                                if len(item) > 1:
                                    news_items.append(item)

                except Exception as e:  # noqa: BLE001
                    print(f"   ⚠ 从财联社获取失败: {e}")

            if not news_items:
                print(f"   未找到股票 {symbol} 的新闻")
                return None

            # 限制数量
            news_items = news_items[: self.max_items]

            return {
                "items": news_items,
                "count": len(news_items),
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "date_range": "最近新闻",
            }

        except Exception as e:  # noqa: BLE001
            print(f"   获取新闻数据异常: {e}")
            import traceback

            traceback.print_exc()
            return None

    def format_news_for_ai(self, data: dict) -> str:
        """将新闻数据格式化为适合 AI 阅读的文本。"""

        if not data or not data.get("data_success"):
            return "未能获取新闻数据"

        text_parts: list[str] = []

        # 新闻数据
        if data.get("news_data"):
            news_data = data["news_data"]
            text_parts.append(
                f"""
【最新新闻 - akshare 数据源】
查询时间：{news_data.get('query_time', 'N/A')}
时间范围：{news_data.get('date_range', 'N/A')}
新闻数量：{news_data.get('count', 0)}条

"""
            )

            for idx, item in enumerate(news_data.get("items", []), 1):
                text_parts.append(f"新闻 {idx}:")

                # 优先显示的字段
                priority_fields = [
                    "title",
                    "date",
                    "time",
                    "source",
                    "content",
                    "url",
                ]

                # 先显示优先字段
                for field in priority_fields:
                    if field in item:
                        value = item[field]
                        # 限制 content 长度
                        if field == "content" and len(str(value)) > 500:
                            value = str(value)[:500] + "..."
                        text_parts.append(f"  {field}: {value}")

                # 再显示其他字段
                for key, value in item.items():
                    if key not in priority_fields and key != "source":
                        if len(str(value)) > 300:
                            value = str(value)[:300] + "..."
                        text_parts.append(f"  {key}: {value}")

                text_parts.append("")  # 空行分隔

        return "\n".join(text_parts)


if __name__ == "__main__":
    print("测试新闻数据获取（akshare 数据源）...")
    print("=" * 60)

    fetcher = QStockNewsDataFetcher()

    if not fetcher.available:
        print("❌ 新闻数据获取器不可用")
        raise SystemExit(1)

    test_symbols = ["000001", "600519"]

    for symbol in test_symbols:
        print("\n" + "=" * 60)
        print(f"正在测试股票: {symbol}")
        print("=" * 60 + "\n")

        data = fetcher.get_stock_news(symbol)

        if data.get("data_success"):
            print("\n" + "=" * 60)
            print("新闻数据获取成功！")
            print("=" * 60)

            formatted_text = fetcher.format_news_for_ai(data)
            print(formatted_text)
        else:
            print(f"\n获取失败: {data.get('error', '未知错误')}")

        print("\n")
