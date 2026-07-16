"""
HMM 数据源抽象层

提供统一的数据源接口，支持回测和实时两种模式。

Modules:
    base: 抽象基类 HMMDataSourceInterface
    backtest_source: 回测数据源（使用 QE artifact）
    realtime_source: 实时数据源（连接 DB t-1）
    cache_manager: QE artifact 缓存管理
    models: Pydantic 数据模型
    exceptions: 异常定义

Example:
    from backend.services.hmm_data_source import BacktestDataSource

    # 创建回测数据源
    source = BacktestDataSource(
        base_loop_ref="qe_20260502_131502_9b54/Loop1",
        cache_dir="tmp/hmm_evolution_cache/",
    )

    # 获取预测数据
    pred_df = await source.get_predictions(
        start_date=date(2024, 7, 1),
        end_date=date(2024, 7, 5),
    )
"""

from .base import HMMDataSourceInterface
from .backtest_source import BacktestDataSource
from .realtime_source import RealtimeDataSource
from .cache_manager import ArtifactCacheManager
from .models import (
    DataSourceConfig,
    PredictionRecord,
    LabelRecord,
    SectorMapping,
    DateRange,
    CacheInfo,
)
from .exceptions import (
    DataSourceError,
    DateRangeError,
    HorizonError,
    CacheError,
    DataNotFoundError,
)

__all__ = [
    # 接口和实现
    "HMMDataSourceInterface",
    "BacktestDataSource",
    "RealtimeDataSource",
    "ArtifactCacheManager",

    # 数据模型
    "DataSourceConfig",
    "PredictionRecord",
    "LabelRecord",
    "SectorMapping",
    "DateRange",
    "CacheInfo",

    # 异常
    "DataSourceError",
    "DateRangeError",
    "HorizonError",
    "CacheError",
    "DataNotFoundError",
]
