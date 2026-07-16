"""
集成测试

测试完整的数据源集成流程：
1. 数据源切换（回测 ↔ 实时）
2. 真实 QE artifact 下载（需要 --run-integration）
3. 真实 DB 查询（需要 --run-integration）

运行方式:
    pytest tests/backend/services/hmm_data_source/test_integration.py --run-integration
"""

from datetime import date

import pytest

from backend.services.hmm_data_source import (
    BacktestDataSource,
    RealtimeDataSource,
    DataSourceConfig,
)


class TestDataSourceIntegration:
    """数据源集成测试"""

    @pytest.mark.asyncio
    async def test_data_source_switching(self):
        """测试数据源模式切换"""
        # 回测模式
        backtest_config = DataSourceConfig(
            mode="backtest",
            base_loop_ref="qe_20260502_131502_9b54/Loop1",
        )
        assert backtest_config.mode == "backtest"

        # 实时模式
        realtime_config = DataSourceConfig(
            mode="realtime",
            snapshot_id="latest",
            lag_days=1,
        )
        assert realtime_config.mode == "realtime"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_real_qe_artifact_download(self):
        """
        测试真实 QE artifact 下载

        需要:
        - QE workspace 可访问
        - 存在可用的 QE 任务
        - 使用 --run-integration 标志运行
        """
        source = BacktestDataSource(
            base_loop_ref="qe_20260502_131502_9b54/Loop1",
            cache_dir="tmp/test_integration_cache/",
        )

        # 尝试获取预测数据（会触发下载）
        df = await source.get_predictions(
            start_date=date(2024, 7, 1),
            end_date=date(2024, 7, 5),
        )

        # 验证数据
        assert not df.empty
        assert 'trade_date' in df.columns
        assert 'symbol' in df.columns
        assert 'score' in df.columns

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_real_db_query(self):
        """
        测试真实 DB 查询

        需要:
        - 数据库连接可用
        - market.kline_daily_raw 表有数据
        - 使用 --run-integration 标志运行
        """
        source = RealtimeDataSource(
            snapshot_id="latest",
            lag_days=1,
        )

        # 获取可用日期范围
        min_date, max_date = await source.get_available_date_range()

        assert min_date < max_date

        # 查询板块映射
        mapping = await source.get_sector_mapping(max_date)

        assert isinstance(mapping, dict)
        assert len(mapping) > 0

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_backtest_vs_realtime_consistency(self):
        """
        测试回测和实时数据源的一致性

        使用相同的日期范围，验证两种数据源返回的数据结构一致。
        """
        # 回测数据源
        backtest_source = BacktestDataSource(
            base_loop_ref="qe_20260502_131502_9b54/Loop1",
            cache_dir="tmp/test_consistency_cache/",
        )

        # 实时数据源
        realtime_source = RealtimeDataSource(
            snapshot_id="latest",
            lag_days=1,
        )

        # 获取两种数据源的日期范围
        backtest_min, backtest_max = await backtest_source.get_available_date_range()
        realtime_min, realtime_max = await realtime_source.get_available_date_range()

        # 选择一个共同的日期
        common_date = min(backtest_max, realtime_max)

        # 查询板块映射
        backtest_mapping = await backtest_source.get_sector_mapping(common_date)
        realtime_mapping = await realtime_source.get_sector_mapping(common_date)

        # 验证数据结构一致
        assert isinstance(backtest_mapping, dict)
        assert isinstance(realtime_mapping, dict)

        # 验证有共同的股票
        common_symbols = set(backtest_mapping.keys()) & set(realtime_mapping.keys())
        assert len(common_symbols) > 0
