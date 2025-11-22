"""
风险数据获取模块（next_app 内部实现）
全部通过 Tushare（统一数据访问模块）获取：
1. 限售解禁（share_float）
2. 大股东增减持（stk_holdertrade）
3. 近期重要公告（anns）
从根目录 risk_data_fetcher.py 迁移而来，行为保持一致，只是改为依赖
next_app 内部的 data_source_manager_impl 和 infra.network_optimizer。
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

import pandas as pd

from .data_source_manager_impl import data_source_manager
from ..infra.network_optimizer import network_optimizer


class RiskDataFetcher:
    """风险数据获取类（统一数据接口）。"""

    def __init__(self) -> None:
        pass

    # ------------------------------------------------------------------
    # 对外主方法
    # ------------------------------------------------------------------
    def get_risk_data(self, symbol: str, analysis_date: Optional[str] = None) -> Dict[str, Any]:
        print(f"\n正在获取 {symbol} 的风险数据（统一数据接口）...")

        result: Dict[str, Any] = {
            "symbol": symbol,
            "data_success": False,
            "source": "unified_data_access",
            "lifting_ban": None,
            "shareholder_reduction": None,
            "important_events": None,
            "liquidity_metrics": None,
            "error": None,
        }

        try:
            base_date = (
                datetime.strptime(analysis_date, "%Y%m%d")
                if analysis_date
                else datetime.now()
            )

            lifting_ban = self._get_lifting_ban_data(symbol, base_date)
            result["lifting_ban"] = lifting_ban

            reduction = self._get_shareholder_reduction_data(symbol, base_date)
            result["shareholder_reduction"] = reduction

            events = self._get_important_events_data(symbol, base_date)
            result["important_events"] = events

            liquidity_metrics = self._get_liquidity_metrics(symbol, base_date)
            result["liquidity_metrics"] = liquidity_metrics

            if (
                (lifting_ban and lifting_ban.get("has_data"))
                or (reduction and reduction.get("has_data"))
                or (events and events.get("has_data"))
                or (liquidity_metrics and liquidity_metrics.get("has_data"))
            ):
                result["data_success"] = True
                print("✅ 风险数据获取完成（统一数据接口）")
            else:
                print("⚠️ 未获取到风险相关数据")

        except Exception as e:  # noqa: BLE001
            print(f"风险数据获取失败: {e}")
            result["error"] = str(e)

        return result

    # ------------------------------------------------------------------
    # 限售解禁
    # ------------------------------------------------------------------
    def _get_lifting_ban_data(self, symbol: str, base_date: datetime) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "has_data": False,
            "records": [],
            "summary": None,
            "source": "tushare_share_float",
        }

        if not data_source_manager.tushare_available:
            return data

        try:
            ts_code = data_source_manager._convert_to_ts_code(symbol)

            def fetch_window(
                start_dt: datetime, end_dt: datetime, category: str
            ) -> List[Dict[str, Any]]:
                with network_optimizer.apply():
                    df_window = data_source_manager.tushare_api.share_float(
                        ts_code=ts_code,
                        start_date=start_dt.strftime("%Y%m%d"),
                        end_date=end_dt.strftime("%Y%m%d"),
                    )

                records_window: List[Dict[str, Any]] = []
                if df_window is None or df_window.empty:
                    return records_window

                df_window["float_date"] = pd.to_datetime(df_window["float_date"])
                df_window = df_window.sort_values("float_date")

                for _, row in df_window.iterrows():
                    float_dt = row["float_date"].strftime("%Y-%m-%d")
                    records_window.append(
                        {
                            "float_date": float_dt,
                            "ann_date": self._fmt_date(row.get("ann_date")),
                            "holder_name": row.get("holder_name"),
                            "share_type": row.get("share_type"),
                            "float_share": self._safe_float(
                                row.get("float_share"), scale=10000
                            ),
                            "float_ratio": self._safe_float(row.get("float_ratio")),
                            "category": category,
                        }
                    )
                return records_window

            upcoming_records = fetch_window(
                base_date, base_date + timedelta(days=365), "upcoming"
            )
            history_records: List[Dict[str, Any]] = []
            # 同时补充最近一年的历史，便于趋势判断
            history_records = fetch_window(
                base_date - timedelta(days=365), base_date, "history"
            )

            combined_records = upcoming_records + history_records
            if not combined_records:
                return data

            summary_lines: list[str] = []
            future_count = len(upcoming_records)
            history_count = len(history_records)
            if future_count:
                summary_lines.append(f"未来一年预计有 {future_count} 笔限售解禁")
                summary_lines.append(
                    "最近未来解禁："
                    f"{upcoming_records[0]['float_date']}，股东 "
                    f"{upcoming_records[0].get('holder_name', '未知')}"
                )
            if history_count:
                summary_lines.append(f"过去一年已完成 {history_count} 笔限售解禁")
                summary_lines.append(
                    "最近历史解禁："
                    f"{history_records[-1]['float_date']}，股东 "
                    f"{history_records[-1].get('holder_name', '未知')}"
                )

            data["has_data"] = True
            data["records"] = combined_records
            data["summary"] = "\n".join(summary_lines)
        except Exception as e:  # noqa: BLE001
            data["error"] = str(e)

        return data

    # ------------------------------------------------------------------
    # 股东增减持
    # ------------------------------------------------------------------
    def _get_shareholder_reduction_data(
        self, symbol: str, base_date: datetime
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "has_data": False,
            "records": [],
            "summary": None,
            "source": "tushare_stk_holdertrade",
        }

        if not data_source_manager.tushare_available:
            return data

        try:
            ts_code = data_source_manager._convert_to_ts_code(symbol)
            end_date = base_date.strftime("%Y%m%d")
            start_date = (base_date - timedelta(days=365)).strftime("%Y%m%d")

            with network_optimizer.apply():
                df = data_source_manager.tushare_api.stk_holdertrade(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                )

            if df is None or df.empty:
                return data

            df["ann_date"] = pd.to_datetime(df["ann_date"])
            df = df.sort_values("ann_date", ascending=False)

            records: List[Dict[str, Any]] = []
            for _, row in df.head(30).iterrows():
                records.append(
                    {
                        "ann_date": row["ann_date"].strftime("%Y-%m-%d"),
                        "holder_name": row.get("holder_name"),
                        "trade_type": row.get("trade_type"),
                        "change_vol": self._safe_float(row.get("change_vol")),
                        "change_ratio": self._safe_float(row.get("change_ratio")),
                        "after_share": self._safe_float(row.get("after_share")),
                        "after_ratio": self._safe_float(row.get("after_ratio")),
                    }
                )

            sells = df[df.get("change_vol", 0) < 0]
            summary: list[str] = [f"近一年公告的股东变动记录共 {len(df)} 条"]
            if not sells.empty:
                summary.append(
                    "其中减持 "
                    f"{len(sells)} 次，最近一次 "
                    f"{sells.iloc[0]['ann_date'].strftime('%Y-%m-%d')}"
                )

            data["has_data"] = True
            data["records"] = records
            data["summary"] = "\n".join(summary)
        except Exception as e:  # noqa: BLE001
            data["error"] = str(e)

        return data

    # ------------------------------------------------------------------
    # 重要公告 / 事件
    # ------------------------------------------------------------------
    def _get_important_events_data(
        self, symbol: str, base_date: datetime
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "has_data": False,
            "records": [],
            "summary": None,
            "source": "tushare_anns",
        }

        if not data_source_manager.tushare_available:
            return data

        try:
            ts_code = data_source_manager._convert_to_ts_code(symbol)
            end_date = base_date.strftime("%Y%m%d")
            start_date = (base_date - timedelta(days=120)).strftime("%Y%m%d")

            limit = 50
            offset = 0
            batches: List[pd.DataFrame] = []
            while True:
                with network_optimizer.apply():
                    df_batch = data_source_manager.tushare_api.anns_d(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date,
                        limit=limit,
                        offset=offset,
                        fields=(
                            "ts_code,ann_date,ann_type,title,pdf_url,page_url,content"
                        ),
                    )

                if df_batch is None or df_batch.empty:
                    break

                batches.append(df_batch)
                if len(df_batch) < limit:
                    break
                offset += limit

            if not batches:
                return data

            df = pd.concat(batches, ignore_index=True)
            df["ann_date"] = pd.to_datetime(df["ann_date"])
            df = df.sort_values("ann_date", ascending=False)

            records: List[Dict[str, Any]] = []
            for _, row in df.head(60).iterrows():
                records.append(
                    {
                        "ann_date": row["ann_date"].strftime("%Y-%m-%d"),
                        "ann_type": row.get("ann_type"),
                        "title": row.get("title"),
                        "pdf_url": row.get("pdf_url") or row.get("page_url"),
                        "summary": (row.get("content") or "")[:400],
                    }
                )

            summary: list[str] = [f"最近120天披露 {len(df)} 条公告"]
            if not df.empty:
                latest = df.iloc[0]
                summary.append(
                    "最新公告："
                    f"{latest['ann_date'].strftime('%Y-%m-%d')}《"
                    f"{latest.get('title', '未知标题')}》"
                )

            data["has_data"] = True
            data["records"] = records
            data["summary"] = "\n".join(summary)
        except Exception as e:  # noqa: BLE001
            data["error"] = str(e)

        return data

    # ------------------------------------------------------------------
    # 流动性指标
    # ------------------------------------------------------------------
    def _get_liquidity_metrics(
        self, symbol: str, base_date: datetime
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "has_data": False,
            "records": [],
            "summary": None,
            "source": "tushare_daily_basic",
        }

        if not data_source_manager.tushare_available:
            return data

        try:
            ts_code = data_source_manager._convert_to_ts_code(symbol)
            end_date = base_date.strftime("%Y%m%d")
            # 多取几天确保 5 个交易日
            start_date = (base_date - timedelta(days=12)).strftime("%Y%m%d")

            with network_optimizer.apply():
                df_basic = data_source_manager.tushare_api.daily_basic(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields="trade_date,turnover_rate,turnover_rate_f,volume_ratio",
                )

            with network_optimizer.apply():
                df_daily = data_source_manager.tushare_api.daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields="trade_date,vol,amount",
                )

            if df_basic is None or df_basic.empty:
                return data

            df_basic["trade_date"] = pd.to_datetime(df_basic["trade_date"])
            df_basic = df_basic.sort_values("trade_date", ascending=False)

            if df_daily is not None and not df_daily.empty:
                df_daily["trade_date"] = pd.to_datetime(df_daily["trade_date"])
                df_daily = df_daily.sort_values("trade_date", ascending=False)
                df_merged = pd.merge(df_basic, df_daily, on="trade_date", how="left")
            else:
                df_merged = df_basic.copy()

            df_merged = df_merged.head(5)
            if df_merged.empty:
                return data

            records: List[Dict[str, Any]] = []
            latest_volume = None
            avg_volume = None
            for _, row in df_merged.iterrows():
                trade_date = row["trade_date"].strftime("%Y-%m-%d")
                turnover = self._safe_float(row.get("turnover_rate"))
                turnover_f = self._safe_float(row.get("turnover_rate_f"))
                volume_ratio = self._safe_float(row.get("volume_ratio"))
                vol = self._safe_float(row.get("vol"))
                amount = self._safe_float(row.get("amount"))

                records.append(
                    {
                        "trade_date": trade_date,
                        "turnover_rate": turnover,
                        "turnover_rate_f": turnover_f,
                        "volume_ratio": volume_ratio,
                        "volume": vol * 100 if vol is not None else None,
                        "amount": amount,
                    }
                )

            if records:
                latest_volume = records[0].get("volume")
                volumes = [
                    rec.get("volume") for rec in records if rec.get("volume") is not None
                ]
                avg_volume = (
                    sum(volumes[1:]) / (len(volumes[1:]) or 1)
                    if len(volumes) > 1
                    else None
                )

                if avg_volume and latest_volume is not None and avg_volume != 0:
                    change_ratio = (latest_volume - avg_volume) / avg_volume * 100
                else:
                    change_ratio = None

                summary_lines: list[str] = [
                    f"最近5个交易日已获取流动性数据 {len(records)} 条",
                    "最新交易日："
                    f"{records[0]['trade_date']}，换手率 "
                    f"{self._format_percentage(records[0].get('turnover_rate'))}，"
                    f"成交量 {self._format_number(latest_volume)}股",
                ]
                if change_ratio is not None:
                    summary_lines.append(
                        f"较前4日平均成交量变化：{change_ratio:+.2f}%"
                    )
                if records[0].get("volume_ratio") is not None:
                    summary_lines.append(
                        f"量比：{records[0]['volume_ratio']:.2f}"
                    )

                data["has_data"] = True
                data["records"] = records
                data["summary"] = "\n".join(summary_lines)
        except Exception as e:  # noqa: BLE001
            data["error"] = str(e)

        return data

    # ------------------------------------------------------------------
    # 格式化输出
    # ------------------------------------------------------------------
    def format_risk_data_for_ai(self, risk_data: Dict[str, Any]) -> str:
        if not risk_data or not risk_data.get("data_success"):
            return "未获取到风险数据"

        parts: List[str] = []

        lifting = risk_data.get("lifting_ban")
        if lifting and lifting.get("has_data"):
            parts.append("=" * 80)
            parts.append("【限售解禁数据】（来源：Tushare share_float）")
            parts.append("=" * 80)
            if lifting.get("summary"):
                parts.append(lifting["summary"])
            upcoming_records = [
                r for r in lifting.get("records", []) if r.get("category") == "upcoming"
            ]
            history_records = [
                r for r in lifting.get("records", []) if r.get("category") == "history"
            ]

            if upcoming_records:
                parts.append("未来解禁安排：")
                for rec in upcoming_records[:6]:
                    parts.append(
                        f"  - {rec.get('float_date')} | 股东 {rec.get('holder_name', '未知')} | "
                        f"解禁股数 {self._format_number(rec.get('float_share'))}股 | 解禁比例 {self._format_percentage(rec.get('float_ratio'))}"
                    )
            if history_records:
                parts.append("最近一年已完成解禁：")
                for rec in history_records[-6:]:
                    parts.append(
                        f"  - {rec.get('float_date')} | 股东 {rec.get('holder_name', '未知')} | "
                        f"解禁股数 {self._format_number(rec.get('float_share'))}股 | 解禁比例 {self._format_percentage(rec.get('float_ratio'))}"
                    )
            parts.append("")

        reduction = risk_data.get("shareholder_reduction")
        if reduction and reduction.get("has_data"):
            parts.append("=" * 80)
            parts.append("【股东增减持数据】（来源：Tushare stk_holdertrade）")
            parts.append("=" * 80)
            if reduction.get("summary"):
                parts.append(reduction["summary"])
            for rec in reduction.get("records", [])[:8]:
                parts.append(
                    f"  * {rec.get('ann_date')} | {rec.get('holder_name', '未知')} | {rec.get('trade_type', '变动')} | "
                    f"变动股数 {self._format_number(rec.get('change_vol'))}股 | 变动比例 {self._format_percentage(rec.get('change_ratio'))}"
                )
            parts.append("")

        events = risk_data.get("important_events")
        if events and events.get("has_data"):
            parts.append("=" * 80)
            parts.append("【近期重要公告】（来源：Tushare anns）")
            parts.append("=" * 80)
            if events.get("summary"):
                parts.append(events["summary"])
            for rec in events.get("records", [])[:10]:
                parts.append(
                    f"  * {rec.get('ann_date')} | {rec.get('ann_type', '公告')} | {rec.get('title', '')}\n"
                    f"    摘要: {(rec.get('summary') or '无')[:120]}"
                )
            parts.append("")

        liquidity = risk_data.get("liquidity_metrics")
        if liquidity and liquidity.get("has_data"):
            parts.append("=" * 80)
            parts.append("【流动性监控】（来源：Tushare daily/daily_basic）")
            parts.append("=" * 80)
            if liquidity.get("summary"):
                parts.append(liquidity["summary"])
            for rec in liquidity.get("records", [])[:5]:
                parts.append(
                    f"  * {rec.get('trade_date')} | 换手率 {self._format_percentage(rec.get('turnover_rate'))}"
                    f" | 成交量 {self._format_number(rec.get('volume'))}股 | 成交额 {self._format_number(rec.get('amount'))}元"
                    + (
                        f" | 量比 {rec.get('volume_ratio'):.2f}"
                        if rec.get("volume_ratio") is not None
                        else ""
                    )
                )
            parts.append("")

        return "\n".join(parts) if parts else "暂无风险数据"

    # ------------------------------------------------------------------
    # 辅助工具
    # ------------------------------------------------------------------
    @staticmethod
    def _safe_float(value, scale: Optional[float] = None) -> Optional[float]:
        try:
            if value is None or pd.isna(value):
                return None
            value = float(value)
            if scale:
                value *= scale
            return value
        except (TypeError, ValueError):  # noqa: BLE001
            return None

    @staticmethod
    def _fmt_date(value) -> Optional[str]:
        if value is None or pd.isna(value):
            return None
        value = str(value)
        if len(value) == 8 and value.isdigit():
            return f"{value[:4]}-{value[4:6]}-{value[6:]}"
        return value

    @staticmethod
    def _format_number(value) -> str:
        if value is None:
            return "N/A"
        try:
            value = float(value)
        except (TypeError, ValueError):  # noqa: BLE001
            return str(value)
        if abs(value) >= 1e12:
            return f"{value / 1e12:.2f}万亿"
        if abs(value) >= 1e8:
            return f"{value / 1e8:.2f}亿"
        return f"{value:,.0f}"

    @staticmethod
    def _format_percentage(value) -> str:
        if value is None:
            return "N/A"
        try:
            value = float(value)
            return f"{value:.2f}%"
        except (TypeError, ValueError):  # noqa: BLE001
            return str(value)


if __name__ == "__main__":
    fetcher = RiskDataFetcher()
    test_symbol = "600000"
    data = fetcher.get_risk_data(test_symbol)
    print("data_success:", data.get("data_success"))
    if data.get("data_success"):
        print(fetcher.format_risk_data_for_ai(data))
