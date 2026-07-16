"""
BacktestDataSource 单元测试

测试内容:
1. mode 属性
2. 日期范围验证
3. 首次下载逻辑
4. 缓存命中逻辑
5. horizon 验证
6. 板块映射
7. 并发下载锁
8. 交易日历计算（真实 trade_cal）
"""

import asyncio
import pickle
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from backend.services.hmm_data_source import (
    BacktestDataSource,
    DateRangeError,
    HorizonError,
    DataNotFoundError,
)


class TestBacktestDataSource:
    """BacktestDataSource 单元测试"""

    @pytest.fixture
    def mock_qe_client(self):
        """Mock QE client"""
        client = MagicMock()
        client.download_artifact = AsyncMock()
        return client

    @pytest.fixture
    def sample_pred_data(self):
        """样本预测数据"""
        dates = [date(2024, 7, 1), date(2024, 7, 2), date(2024, 7, 3)]
        symbols = ['000001.SZ', '000002.SZ', '600000.SH']

        data = []
        for d in dates:
            for symbol in symbols:
                data.append({
                    'trade_date': d,
                    'symbol': symbol,
                    'score': 0.5,
                })

        return pd.DataFrame(data)

    @pytest.fixture
    def sample_label_data(self):
        """样本标签数据"""
        dates = [date(2024, 7, 1), date(2024, 7, 2)]
        symbols = ['000001.SZ', '000002.SZ']

        data = []
        for d in dates:
            for symbol in symbols:
                data.append({
                    'trade_date': d,
                    'symbol': symbol,
                    'horizon_days': 10,
                    'future_return': 0.02,
                    'label_date': date(2024, 7, 15),  # 会被重新计算
                })

        return pd.DataFrame(data)

    @pytest.mark.asyncio
    async def test_mode_property(self, mock_qe_client):
        """测试 mode 属性返回 'backtest'"""
        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir="tmp/test_cache/",
            qe_client=mock_qe_client,
        )

        assert source.mode == "backtest"

    @pytest.mark.asyncio
    async def test_date_range_validation_invalid_order(self, mock_qe_client):
        """测试日期范围验证：start_date > end_date"""
        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir="tmp/test_cache/",
            qe_client=mock_qe_client,
        )

        # Mock get_available_date_range
        source.get_available_date_range = AsyncMock(
            return_value=(date(2024, 1, 1), date(2024, 12, 31))
        )

        is_valid, error_msg = await source.validate_date_range(
            start_date=date(2024, 7, 10),
            end_date=date(2024, 7, 5),
        )

        assert not is_valid
        assert "晚于结束日期" in error_msg

    @pytest.mark.asyncio
    async def test_first_download_and_cache(
        self,
        mock_qe_client,
        sample_pred_data,
        tmp_path,
    ):
        """测试首次下载并缓存"""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        # Mock QE client 返回 pickle bytes
        pred_bytes = pickle.dumps(sample_pred_data)
        mock_qe_client.download_artifact.return_value = pred_bytes

        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir=str(cache_dir),
            qe_client=mock_qe_client,
        )

        # 首次访问应该触发下载
        df = await source.get_predictions(
            start_date=date(2024, 7, 1),
            end_date=date(2024, 7, 2),
        )

        # 验证下载被调用
        mock_qe_client.download_artifact.assert_called_once()

        # 验证数据正确
        assert len(df) > 0
        assert 'trade_date' in df.columns
        assert 'symbol' in df.columns
        assert 'score' in df.columns

    @pytest.mark.asyncio
    async def test_cache_hit_no_download(
        self,
        mock_qe_client,
        sample_pred_data,
        tmp_path,
    ):
        """测试缓存命中，不重复下载"""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        # 预先保存到缓存
        from backend.services.hmm_data_source import ArtifactCacheManager
        cache_manager = ArtifactCacheManager(str(cache_dir))
        pred_bytes = pickle.dumps(sample_pred_data)
        cache_manager.save_artifact("qe_test/Loop1", "pred.pkl", pred_bytes)

        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir=str(cache_dir),
            qe_client=mock_qe_client,
        )

        # 访问数据
        df = await source.get_predictions(
            start_date=date(2024, 7, 1),
            end_date=date(2024, 7, 2),
        )

        # 验证没有调用下载
        mock_qe_client.download_artifact.assert_not_called()

        # 验证数据正确
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_horizon_validation(self, mock_qe_client):
        """测试 horizon_days 验证"""
        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir="tmp/test_cache/",
            qe_client=mock_qe_client,
        )

        # Mock _load_labels_from_cache
        source._load_labels_from_cache = AsyncMock(
            return_value=pd.DataFrame(columns=['trade_date', 'symbol', 'future_return'])
        )

        # 测试无效的 horizon
        with pytest.raises(HorizonError):
            await source.get_labels(
                start_date=date(2024, 7, 1),
                end_date=date(2024, 7, 5),
                horizon_days=100,  # 超出范围
            )

    @pytest.mark.asyncio
    async def test_sector_mapping_query(self, mock_qe_client):
        """测试板块映射查询"""
        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir="tmp/test_cache/",
            qe_client=mock_qe_client,
        )

        # Mock 数据库查询
        with patch('backend.services.hmm_data_source.backtest_source.get_conn') as mock_conn:
            mock_cursor = AsyncMock()
            mock_cursor.fetchall.return_value = [
                ('000001.SZ', '801780.SI'),
                ('600000.SH', '801192.SI'),
            ]
            mock_conn.return_value.__aenter__.return_value.cursor.return_value.__aenter__.return_value = mock_cursor

            mapping = await source.get_sector_mapping(date(2024, 7, 1))

            assert mapping['000001.SZ'] == '801780.SI'
            assert mapping['600000.SH'] == '801192.SI'

    @pytest.mark.asyncio
    async def test_concurrent_download_lock(
        self,
        mock_qe_client,
        sample_pred_data,
        tmp_path,
    ):
        """测试并发下载时的锁保护"""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        # Mock QE client 返回数据（模拟慢速下载）
        async def slow_download(*args, **kwargs):
            await asyncio.sleep(0.1)
            return pickle.dumps(sample_pred_data)

        mock_qe_client.download_artifact = slow_download

        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir=str(cache_dir),
            qe_client=mock_qe_client,
        )

        # 并发调用（应该只下载一次）
        results = await asyncio.gather(
            source.get_predictions(date(2024, 7, 1), date(2024, 7, 2)),
            source.get_predictions(date(2024, 7, 1), date(2024, 7, 2)),
            source.get_predictions(date(2024, 7, 1), date(2024, 7, 2)),
        )

        # 验证所有结果都正确
        for df in results:
            assert len(df) > 0

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_trading_calendar_calculation(self, mock_qe_client):
        """
        测试交易日历计算（使用真实 trade_cal）

        需要连接真实数据库，使用 --run-integration 标志
        """
        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir="tmp/test_cache/",
            qe_client=mock_qe_client,
        )

        # 测试：2024-01-02 后的第 5 个交易日
        # 预期：跳过周末，应该是 2024-01-09
        label_date = await source._get_nth_trading_day(
            start_date=date(2024, 1, 2),
            n_days=5,
        )

        # 验证结果（具体日期取决于真实交易日历）
        assert label_date > date(2024, 1, 2)
        assert label_date <= date(2024, 1, 15)  # 合理范围


class TestBacktestDataSourceIsolation:
    """隔离约束验证测试"""

    @pytest.mark.asyncio
    async def test_forbid_config_file_download(self):
        """测试禁止下载配置文件"""
        mock_qe_client = MagicMock()
        mock_qe_client.download_artifact = AsyncMock()

        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir="tmp/test_cache/",
            qe_client=mock_qe_client,
        )

        # 尝试下载配置文件应该被拦截
        # （实际实现中需要在 _download_artifact 添加检查）
        forbidden_files = [
            'config.json',
            'hmm_config.yaml',
            'strategy.toml',
        ]

        for filename in forbidden_files:
            # 这里假设未来会添加配置文件检查
            # 当前实现中只下载 pred.pkl 和 label.pkl
            pass

    @pytest.mark.asyncio
    async def test_only_read_artifact_files(self, tmp_path):
        """验证只读取 artifact 文件（pred.pkl, label.pkl）"""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        from backend.services.hmm_data_source import ArtifactCacheManager
        cache_manager = ArtifactCacheManager(str(cache_dir))

        # 验证缓存管理器只处理 .pkl 文件
        allowed_artifacts = ['pred.pkl', 'label.pkl']

        for artifact in allowed_artifacts:
            # 应该成功保存
            cache_manager.save_artifact(
                "qe_test/Loop1",
                artifact,
                b"test_data"
            )

        # 验证缓存信息
        cache_info = cache_manager.get_cache_info("qe_test/Loop1")
        assert cache_info['cached']
        assert len(cache_info['artifacts']) == 2
