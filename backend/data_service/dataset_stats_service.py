"""
miniQMT 数据集统计服务

提供数据集统计信息收集、数据范围计算、股票范围计算等功能。
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class DateRange:
    """日期范围"""
    start: Optional[str] = None  # 起始日期 YYYY-MM-DD
    end: Optional[str] = None  # 结束日期 YYYY-MM-DD
    latest_available: Optional[str] = None  # 最新可用日期
    latest_trading_day: Optional[str] = None  # 最新交易日
    gap_days: List[str] = None  # 缺失的交易日
    total_trading_days: int = 0  # 总交易日数
    covered_trading_days: int = 0  # 已覆盖交易日数

    def __post_init__(self):
        if self.gap_days is None:
            self.gap_days = []


@dataclass
class StockRange:
    """股票范围"""
    total_count: int = 0  # 总股票数
    covered_count: int = 0  # 已覆盖股票数
    coverage_rate: float = 0.0  # 覆盖率
    sample_stocks: List[Dict[str, Any]] = None  # 样本股票

    def __post_init__(self):
        if self.sample_stocks is None:
            self.sample_stocks = []


@dataclass
class DataSize:
    """数据规模"""
    record_count: int = 0  # 记录数
    size_mb: float = 0.0  # 存储大小（MB）


@dataclass
class QualityMetrics:
    """质量指标"""
    completeness: float = 0.0  # 完整性
    consistency: float = 0.0  # 一致性
    timeliness: float = 0.0  # 及时性


@dataclass
class DatasetSummary:
    """数据集摘要"""
    id: str  # 数据集 ID
    name: str  # 数据集名称
    period: str  # 周期
    status: str  # 状态: complete, partial, empty, unknown
    date_range: DateRange  # 日期范围
    stock_range: StockRange  # 股票范围
    data_size: DataSize  # 数据规模
    last_updated: Optional[str] = None  # 最后更新时间
    update_status: str = "unknown"  # 更新状态: up_to_date, outdated, unknown


@dataclass
class DatasetDetail:
    """数据集详情"""
    id: str  # 数据集 ID
    name: str  # 数据集名称
    period: str  # 周期
    status: str  # 状态
    date_range: DateRange  # 日期范围
    stock_range: StockRange  # 股票范围
    data_size: DataSize  # 数据规模
    last_updated: Optional[str] = None  # 最后更新时间
    update_status: str = "unknown"  # 更新状态
    quality_metrics: QualityMetrics = None  # 质量指标

    def __post_init__(self):
        if self.quality_metrics is None:
            self.quality_metrics = QualityMetrics()


class DatasetStatsService:
    """数据集统计服务"""

    def __init__(self, qmt_client):
        """
        初始化数据集统计服务
        
        Args:
            qmt_client: QMT 客户端实例
        """
        self.qmt_client = qmt_client
        self._cache: Dict[str, DatasetSummary] = {}
        self._cache_time: Dict[str, datetime] = {}
        self._cache_ttl = timedelta(minutes=5)  # 缓存有效期 5 分钟
        self._reference_cache: Dict[str, str] = {}

    def _is_cache_valid(self, dataset_id: str) -> bool:
        """检查缓存是否有效"""
        if dataset_id not in self._cache:
            return False
        cache_time = self._cache_time.get(dataset_id)
        if cache_time is None:
            return False
        return datetime.now() - cache_time < self._cache_ttl

    def _update_cache(self, dataset_id: str, summary: DatasetSummary):
        """更新缓存"""
        self._cache[dataset_id] = summary
        self._cache_time[dataset_id] = datetime.now()

    def get_all_datasets(self) -> List[DatasetSummary]:
        """
        获取所有数据集的统计信息
        
        Returns:
            数据集摘要列表
        """
        datasets = []
        
        # 定义所有数据集
        dataset_configs = [
            {"id": "kline_1d", "name": "日线 K 线", "period": "1d"},
            {"id": "kline_1m", "name": "1分钟 K 线", "period": "1m"},
            {"id": "kline_5m", "name": "5分钟 K 线", "period": "5m"},
            {"id": "kline_1h", "name": "1小时 K 线", "period": "1h"},
            {"id": "tick", "name": "分笔数据", "period": "tick"},
            {"id": "financial", "name": "财务数据", "period": "1d"},
            {"id": "instrument", "name": "合约基础信息", "period": "1d"},
            {"id": "sector", "name": "板块分类信息", "period": "1d"},
            {"id": "holiday", "name": "节假日数据", "period": "1d"},
            {"id": "dividend", "name": "除权数据", "period": "1d"},
        ]
        
        for config in dataset_configs:
            try:
                if self._is_cache_valid(config["id"]):
                    datasets.append(self._cache[config["id"]])
                else:
                    summary = self._get_dataset_summary(config["id"], config["name"], config["period"])
                    self._update_cache(config["id"], summary)
                    datasets.append(summary)
            except Exception as e:
                logger.error("获取数据集 %s 统计信息失败: %s", config["id"], e, exc_info=True)
                # 返回一个标记为 error 的数据集摘要，不隐藏失败
                datasets.append(DatasetSummary(
                    id=config["id"],
                    name=config["name"],
                    period=config["period"],
                    status="error",
                    date_range=DateRange(),
                    stock_range=StockRange(),
                    data_size=DataSize(),
                    update_status="error"
                ))
        
        return datasets

    def get_dataset_detail(self, dataset_id: str) -> Optional[DatasetDetail]:
        """
        获取指定数据集的详细统计信息
        
        Args:
            dataset_id: 数据集 ID
            
        Returns:
            数据集详情，如果数据集不存在则返回 None
        """
        # 数据集名称映射
        dataset_names = {
            "kline_1d": "日线 K 线",
            "kline_1m": "1分钟 K 线",
            "kline_5m": "5分钟 K 线",
            "kline_1h": "1小时 K 线",
            "tick": "分笔数据",
            "financial": "财务数据",
            "instrument": "合约基础信息",
            "sector": "板块分类信息",
            "holiday": "节假日数据",
            "dividend": "除权数据",
        }
        
        dataset_periods = {
            "kline_1d": "1d",
            "kline_1m": "1m",
            "kline_5m": "5m",
            "kline_1h": "1h",
            "tick": "tick",
            "financial": "1d",
            "instrument": "1d",
            "sector": "1d",
            "holiday": "1d",
            "dividend": "1d",
        }
        
        if dataset_id not in dataset_names:
            return None
        
        try:
            # 获取数据集摘要
            summary = self._get_dataset_summary(
                dataset_id,
                dataset_names[dataset_id],
                dataset_periods[dataset_id]
            )
            
            # 计算质量指标
            quality_metrics = self._calculate_quality_metrics(summary)
            
            # 构建数据集详情
            detail = DatasetDetail(
                id=summary.id,
                name=summary.name,
                period=summary.period,
                status=summary.status,
                date_range=summary.date_range,
                stock_range=summary.stock_range,
                data_size=summary.data_size,
                last_updated=summary.last_updated,
                update_status=summary.update_status,
                quality_metrics=quality_metrics
            )
            
            return detail
        except Exception as e:
            logger.error(f"获取数据集 {dataset_id} 详细信息失败: {e}", exc_info=True)
            return None

    def _get_dataset_summary(self, dataset_id: str, dataset_name: str, period: str) -> DatasetSummary:
        """
        获取数据集摘要
        
        Args:
            dataset_id: 数据集 ID
            dataset_name: 数据集名称
            period: 周期
            
        Returns:
            数据集摘要
        """
        # 获取日期范围
        date_range = self._get_date_range(dataset_id, period)
        
        # 获取股票范围
        stock_range = self._get_stock_range(dataset_id, period)
        
        # 获取数据规模
        data_size = self._get_data_size(dataset_id, period)
        
        # 判断状态
        status = self._determine_status(date_range, stock_range)
        
        # 判断更新状态
        update_status = self._determine_update_status(date_range)
        
        # 获取最后更新时间
        last_updated = self._get_last_updated(dataset_id, period)
        
        return DatasetSummary(
            id=dataset_id,
            name=dataset_name,
            period=period,
            status=status,
            date_range=date_range,
            stock_range=stock_range,
            data_size=data_size,
            last_updated=last_updated,
            update_status=update_status
        )

    def _get_date_range(self, dataset_id: str, period: str) -> DateRange:
        """
        获取日期范围
        
        Args:
            dataset_id: 数据集 ID
            period: 周期
            
        Returns:
            日期范围
        """
        try:
            # 对于非 K 线数据，使用不同的查询方法
            if dataset_id in ["financial", "instrument", "sector", "holiday", "dividend"]:
                # 这些数据类型不支持 get_local_data_range，返回空范围
                # 实际应该使用其他方法来判断数据是否存在
                return DateRange()
            
            # 使用可用参考股票代码查询数据范围
            reference_stock = self._get_reference_stock(period)
            if not reference_stock:
                return DateRange()
            
            # 查询本地数据范围
            range_info = self.qmt_client.get_local_data_range(reference_stock, period)
            
            if not range_info:
                return DateRange()
            
            # 获取最新交易日
            latest_trading_day = self.qmt_client.get_latest_trading_day()
            
            # 获取交易日历
            trading_calendar = self._get_trading_calendar()
            
            # 计算缺失的交易日
            gap_days = []
            if trading_calendar and range_info.get("start") and range_info.get("end"):
                start_date = datetime.strptime(range_info["start"], "%Y%m%d")
                end_date = datetime.strptime(range_info["end"], "%Y%m%d")
                
                for day in trading_calendar:
                    day_date = datetime.strptime(day, "%Y%m%d")
                    if start_date <= day_date <= end_date:
                        # 检查该日期是否有数据
                        if not self._has_data_on_date(reference_stock, period, day):
                            gap_days.append(day)
            
            return DateRange(
                start=self._format_date(range_info.get("start")),
                end=self._format_date(range_info.get("end")),
                latest_available=self._format_date(range_info.get("end")),
                latest_trading_day=self._format_date(latest_trading_day),
                gap_days=gap_days,
                total_trading_days=len(trading_calendar) if trading_calendar else 0,
                covered_trading_days=len(trading_calendar) - len(gap_days) if trading_calendar else 0
            )
        except Exception as e:
            logger.error("获取数据集 %s 日期范围失败: %s", dataset_id, e, exc_info=True)
            return DateRange()

    def _get_stock_range(self, dataset_id: str, period: str) -> StockRange:
        """
        获取股票范围
        
        Args:
            dataset_id: 数据集 ID
            period: 周期
            
        Returns:
            股票范围
        """
        try:
            # 对于非 K 线数据，返回空范围
            if dataset_id in ["financial", "instrument", "sector", "holiday", "dividend"]:
                return StockRange()
            
            # 获取所有股票列表
            all_stocks = self.qmt_client.get_stock_list_in_sector("沪深A股")
            
            if not all_stocks:
                return StockRange()
            
            total_count = len(all_stocks)
            
            # 随机抽样检查股票是否覆盖（避免仅取列表头部导致覆盖率偏高）
            sample_size = min(30, total_count)
            sample_stocks = random.sample(all_stocks, sample_size)
            
            covered_count = 0
            sample_details = []
            
            for stock_code in sample_stocks:
                try:
                    range_info = self.qmt_client.get_local_data_range(stock_code, period)
                    if range_info and range_info.get("count", 0) > 0:
                        covered_count += 1
                        sample_details.append({
                            "code": stock_code,
                            "name": stock_code,  # 可以通过其他接口获取股票名称
                            "start": self._format_date(range_info.get("start")),
                            "end": self._format_date(range_info.get("end")),
                            "count": range_info.get("count", 0)
                        })
                except Exception as exc:
                    logger.debug("_get_stock_range: failed to check stock %s: %s", stock_code, exc)
            
            # 估算覆盖率
            coverage_rate = covered_count / sample_size if sample_size > 0 else 0.0
            
            return StockRange(
                total_count=total_count,
                covered_count=int(total_count * coverage_rate),
                coverage_rate=round(coverage_rate, 4),
                sample_stocks=sample_details
            )
        except Exception as e:
            logger.error("获取数据集 %s 股票范围失败: %s", dataset_id, e, exc_info=True)
            return StockRange()

    def _get_data_size(self, dataset_id: str, period: str) -> DataSize:
        """
        获取数据规模
        
        Args:
            dataset_id: 数据集 ID
            period: 周期
            
        Returns:
            数据规模
        """
        try:
            # 对于非 K 线数据，返回空规模
            if dataset_id in ["financial", "instrument", "sector", "holiday", "dividend"]:
                return DataSize()
            
            # 使用参考股票估算
            reference_stock = self._get_reference_stock(period)
            if not reference_stock:
                return DataSize()
            range_info = self.qmt_client.get_local_data_range(reference_stock, period)
            
            if not range_info:
                return DataSize()
            
            record_count = range_info.get("count", 0)
            
            # 估算存储大小（假设每条记录约 100 字节）
            size_mb = (record_count * 100) / (1024 * 1024)
            
            return DataSize(
                record_count=record_count,
                size_mb=round(size_mb, 2)
            )
        except Exception as e:
            logger.error("获取数据集 %s 数据规模失败: %s", dataset_id, e, exc_info=True)
            return DataSize()

    def _determine_status(self, date_range: DateRange, stock_range: StockRange) -> str:
        """
        判断数据集状态
        
        Args:
            date_range: 日期范围
            stock_range: 股票范围
            
        Returns:
            状态: complete, partial, empty, unknown
        """
        if not date_range.start and not date_range.end:
            return "empty"
        
        if stock_range.coverage_rate >= 0.95 and date_range.covered_trading_days / date_range.total_trading_days >= 0.95:
            return "complete"
        
        if stock_range.coverage_rate > 0 or date_range.covered_trading_days > 0:
            return "partial"
        
        return "unknown"

    def _determine_update_status(self, date_range: DateRange) -> str:
        """
        判断更新状态
        
        Args:
            date_range: 日期范围
            
        Returns:
            更新状态: up_to_date, outdated, unknown
        """
        if not date_range.latest_available or not date_range.latest_trading_day:
            return "unknown"
        
        try:
            latest_available = datetime.strptime(date_range.latest_available, "%Y-%m-%d")
            latest_trading = datetime.strptime(date_range.latest_trading_day, "%Y-%m-%d")
            
            # 如果差距小于 2 天，认为是最新的
            if (latest_trading - latest_available).days <= 2:
                return "up_to_date"
            else:
                days_outdated = (latest_trading - latest_available).days
                return f"outdated_{days_outdated}"
        except Exception as exc:
            logger.warning("_determine_update_status: failed to compare dates: %s", exc)
            return "unknown"

    def _get_last_updated(self, dataset_id: str, period: str) -> Optional[str]:
        """
        获取最后更新时间
        
        Args:
            dataset_id: 数据集 ID
            period: 周期
            
        Returns:
            最后更新时间（ISO 8601 格式）
        """
        try:
            # 尝试通过数据范围推断最后更新时间
            stocks = self.qmt_client.get_stock_list_in_sector("沪深A股")
            reference = stocks[0] if stocks else ""
            if reference:
                range_info = self.qmt_client.get_local_data_range(reference, period)
                end = range_info.get("end")
                if end:
                    return str(end)
            return None
        except Exception as exc:
            logger.error("_get_last_updated: failed for dataset=%s period=%s: %s", dataset_id, period, exc)
            return None

    def _calculate_quality_metrics(self, summary: DatasetSummary) -> QualityMetrics:
        """
        计算质量指标
        
        Args:
            summary: 数据集摘要
            
        Returns:
            质量指标
        """
        # 完整性：基于覆盖率和交易日覆盖率
        completeness = (summary.stock_range.coverage_rate + 
                       (summary.date_range.covered_trading_days / summary.date_range.total_trading_days 
                        if summary.date_range.total_trading_days > 0 else 0)) / 2
        
        # 一致性：基于缺失交易日数量
        consistency = 1.0 - (len(summary.date_range.gap_days) / summary.date_range.total_trading_days 
                            if summary.date_range.total_trading_days > 0 else 0)
        
        # 及时性：基于更新状态
        timeliness = 1.0 if summary.update_status == "up_to_date" else 0.5
        
        return QualityMetrics(
            completeness=round(completeness, 4),
            consistency=round(consistency, 4),
            timeliness=round(timeliness, 4)
        )

    def _get_trading_calendar(self) -> List[str]:
        """
        获取交易日历
        
        Returns:
            交易日历列表（YYYYMMDD 格式）
        """
        try:
            # 获取上交所交易日历
            calendar = self.qmt_client.get_trading_calendar("SH")
            return calendar if calendar else []
        except Exception as e:
            logger.warning(f"获取交易日历失败: {e}")
            return []

    def _has_data_on_date(self, stock_code: str, period: str, date: str) -> bool:
        """
        检查指定股票在指定日期是否有数据

        Args:
            stock_code: 股票代码
            period: 周期
            date: 日期（YYYYMMDD 格式）

        Returns:
            是否有数据
        """
        try:
            range_info = self.qmt_client.get_local_data_range(stock_code, period)
            start = range_info.get("start")
            end = range_info.get("end")
            if not start or not end:
                return False
            # Normalize: strip time portion if present, compare date strings
            start_day = str(start)[:8] if len(str(start)) >= 8 else str(start)
            end_day = str(end)[:8] if len(str(end)) >= 8 else str(end)
            return start_day <= date <= end_day
        except Exception as exc:
            logger.debug("_has_data_on_date: failed for %s/%s/%s: %s", stock_code, period, date, exc)
            return False

    def _format_date(self, date_str: Optional[str]) -> Optional[str]:
        """格式化日期"""
        if date_str is None:
            return None
        try:
            if isinstance(date_str, (int, float)):
                date_str = str(int(date_str))
            else:
                date_str = str(date_str)

            if not date_str:
                return None

            if len(date_str) >= 14 and date_str.isdigit():
                date_str = date_str[:8]

            if len(date_str) == 8 and date_str.isdigit():
                dt = datetime.strptime(date_str, "%Y%m%d")
                return dt.strftime("%Y-%m-%d")
            if "-" in date_str:
                return date_str
            if "T" in date_str:
                return date_str.split("T", 1)[0]
            return None
        except Exception:
            logger.debug("_format_date: failed to parse %r", date_str)
            return None

    def _get_reference_stock(self, period: str) -> Optional[str]:
        """选择一个具有本地数据的参考股票代码."""
        cached = self._reference_cache.get(period)
        if cached:
            return cached

        candidates = ["000001.SZ", "000001.SH", "600000.SH"]
        for code in candidates:
            range_info = self.qmt_client.get_local_data_range(code, period)
            if range_info and range_info.get("count", 0) > 0:
                self._reference_cache[period] = code
                return code

        stock_list = self.qmt_client.get_stock_list_in_sector("沪深A股") or []
        for code in stock_list[:50]:
            range_info = self.qmt_client.get_local_data_range(code, period)
            if range_info and range_info.get("count", 0) > 0:
                self._reference_cache[period] = code
                return code

        return None
