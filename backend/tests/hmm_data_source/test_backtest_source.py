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
8. 交易日历计算（真实 trading_calendar）
"""

import asyncio
import hashlib
import io
import json
import pickle
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from backend.services.model_store import ModelStoreService, PredictionArtifactStore

from backend.services.hmm_data_source import (
    BacktestDataSource,
    DataSourceError,
    HorizonError,
)
from backend.services.hmm_data_source.legacy_qe_artifact_manifests import (
    LegacyQEArtifactManifest,
    LegacyQEArtifactReceipt,
    LegacyQERecorderEvidence,
    find_legacy_qe_artifact_manifest,
)


def qe_provenance(payload: bytes, *, artifact_name: str, row_count: int) -> dict:
    workspace_path = f"mlruns/1/abc123/artifacts/{artifact_name}"
    return {
        "source": "qe_workspace",
        "task_id": "qe_test",
        "loop_name": "Loop1",
        "workspace_path": workspace_path,
        "remote_manifest_path": f"{workspace_path}.manifest.json",
        "remote_schema_version": "qe_dataframe_v1",
        "remote_sha256": hashlib.sha256(payload).hexdigest(),
        "remote_size_bytes": len(payload),
        "remote_row_count": row_count,
        "remote_quality_status": "ok",
    }


def legacy_manifest_fixture(log_payload: str) -> LegacyQEArtifactManifest:
    recorder_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    prefix = f"mlruns/42/{recorder_id}/artifacts"
    return LegacyQEArtifactManifest(
        base_loop_ref="qe_test/Loop1",
        logical_experiment_id="qe_test_L1",
        recorder_experiment_id="42",
        recorder_id=recorder_id,
        recorder_evidence=LegacyQERecorderEvidence(
            workspace_path="run.log",
            sha256=hashlib.sha256(log_payload.encode("utf-8")).hexdigest(),
            size_bytes=len(log_payload.encode("utf-8")),
            terminal_status="FINISHED",
        ),
        artifacts=(
            LegacyQEArtifactReceipt(
                artifact_name="pred.pkl",
                workspace_path=f"{prefix}/pred.pkl",
                schema_version="legacy_qe_dataframe_pickle_v1",
                sha256="1" * 64,
                size_bytes=100,
                row_count=10,
            ),
            LegacyQEArtifactReceipt(
                artifact_name="label.pkl",
                workspace_path=f"{prefix}/label.pkl",
                schema_version="legacy_qe_dataframe_pickle_v1",
                sha256="2" * 64,
                size_bytes=80,
                row_count=10,
            ),
        ),
    )


def build_prediction_store_service(
    tmp_path,
    *,
    pred_obj,
    label_obj=None,
    task_id: str = "qe_store",
    loop_index: int = 1,
) -> ModelStoreService:
    files = {"prediction": ("pred.pkl", io.BytesIO(pickle.dumps(pred_obj)))}
    if label_obj is not None:
        files["label"] = ("label.pkl", io.BytesIO(pickle.dumps(label_obj)))
    store = PredictionArtifactStore(root=tmp_path / "prediction_store")
    store.write_artifacts(
        run_key=f"{task_id}_L{loop_index}",
        files=files,
        metadata={"task_id": task_id, "loop_index": loop_index},
    )
    return ModelStoreService(artifact_store=store)


class TestBacktestDataSource:
    """BacktestDataSource 单元测试"""

    @pytest.fixture
    def mock_qe_client(self):
        """Mock QE client"""
        client = MagicMock()
        client.download_workspace_file_bytes = AsyncMock()

        async def get_workspace_file(task_id, loop_name, path):
            if path in {"qe_current_recorder.json", "qe_extracted_recorder.json"}:
                return {"experiment_id": "1", "recorder_id": "abc123"}
            payload = client.download_workspace_file_bytes.return_value
            if not isinstance(payload, bytes):
                raise AssertionError("test must configure artifact bytes before manifest read")
            frame = pickle.loads(payload)
            return {
                "artifact_name": path.removesuffix(".manifest.json").rsplit("/", 1)[-1],
                "schema_version": "qe_dataframe_v1",
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
                "row_count": len(frame),
                "quality_status": "ok",
            }

        client.get_workspace_file = AsyncMock(side_effect=get_workspace_file)
        client.close = AsyncMock()
        return client

    @pytest.fixture
    def sample_pred_data(self):
        """样本预测数据"""
        dates = [date(2024, 7, 1), date(2024, 7, 2), date(2024, 7, 3)]
        symbols = ["000001.SZ", "000002.SZ", "600000.SH"]

        data = []
        for d in dates:
            for symbol in symbols:
                data.append(
                    {
                        "trade_date": d,
                        "symbol": symbol,
                        "score": 0.5,
                    }
                )

        return pd.DataFrame(data)

    @pytest.fixture
    def sample_label_data(self):
        """样本标签数据"""
        dates = [date(2024, 7, 1), date(2024, 7, 2)]
        symbols = ["000001.SZ", "000002.SZ"]

        data = []
        for d in dates:
            for symbol in symbols:
                data.append(
                    {
                        "trade_date": d,
                        "symbol": symbol,
                        "horizon_days": 10,
                        "future_return": 0.02,
                        "label_date": date(2024, 7, 15),  # 会被重新计算
                    }
                )

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
        source.get_available_date_range = AsyncMock(return_value=(date(2024, 1, 1), date(2024, 12, 31)))

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
        mock_qe_client.download_workspace_file_bytes.return_value = pred_bytes

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
        mock_qe_client.download_workspace_file_bytes.assert_awaited_once_with(
            "qe_test",
            "Loop1",
            "mlruns/1/abc123/artifacts/pred.pkl",
        )

        # 验证数据正确
        assert len(df) > 0
        assert "trade_date" in df.columns
        assert "symbol" in df.columns
        assert "score" in df.columns

    @pytest.mark.asyncio
    async def test_prediction_store_first_reuses_blob_without_workspace_copy(
        self,
        mock_qe_client,
        sample_pred_data,
        sample_label_data,
        tmp_path,
    ):
        cache_dir = tmp_path / "cache"
        repository = MagicMock()
        repository.get_nth_trading_day.side_effect = lambda trade_date, horizon: trade_date + timedelta(days=horizon)
        model_store = build_prediction_store_service(
            tmp_path,
            pred_obj=sample_pred_data,
            label_obj=sample_label_data,
        )
        source = BacktestDataSource(
            base_loop_ref="qe_store/Loop1",
            cache_dir=str(cache_dir),
            qe_client=mock_qe_client,
            repository=repository,
            model_store=model_store,
        )

        pred = await source.get_predictions(date(2024, 7, 1), date(2024, 7, 2))
        labels = await source.get_labels(
            date(2024, 7, 1),
            date(2024, 7, 2),
            horizon_days=10,
        )

        assert not pred.empty
        assert not labels.empty
        mock_qe_client.download_workspace_file_bytes.assert_not_awaited()
        assert not source.cache_manager.is_cached("qe_store/Loop1", "pred.pkl")
        assert not source.cache_manager.is_cached("qe_store/Loop1", "label.pkl")
        source_info = source.get_artifact_source_info()
        assert source_info["pred.pkl"]["source"] == "prediction_store"
        assert source_info["pred.pkl"]["zero_copy"] is True
        assert source_info["label.pkl"]["source"] == "prediction_store"
        assert source_info["label.pkl"]["zero_copy"] is True

    @pytest.mark.asyncio
    async def test_prediction_store_reads_real_qlib_multiindex_shape(
        self,
        mock_qe_client,
        tmp_path,
    ):
        index = pd.MultiIndex.from_product(
            [
                [pd.Timestamp("2024-07-01"), pd.Timestamp("2024-07-02")],
                ["000001.SZ", "000002.SZ"],
            ],
            names=["datetime", "instrument"],
        )
        pred_obj = pd.DataFrame({"score": [0.4, 0.2, 0.1, 0.3]}, index=index)
        label_obj = pd.DataFrame({"LABEL0": [0.02, -0.01, 0.03, 0.01]}, index=index)
        repository = MagicMock()
        repository.get_nth_trading_day.side_effect = lambda trade_date, horizon: trade_date + timedelta(days=horizon)
        model_store = build_prediction_store_service(
            tmp_path,
            pred_obj=pred_obj,
            label_obj=label_obj,
        )
        source = BacktestDataSource(
            base_loop_ref="qe_store/Loop1",
            cache_dir=str(tmp_path / "cache"),
            qe_client=mock_qe_client,
            repository=repository,
            model_store=model_store,
            label_horizon_days=10,
        )

        pred = await source.get_predictions(date(2024, 7, 1), date(2024, 7, 2))
        labels = await source.get_labels(
            date(2024, 7, 1),
            date(2024, 7, 2),
            horizon_days=10,
        )

        assert list(pred.columns) == ["trade_date", "symbol", "score", "rank"]
        assert len(pred) == 4
        assert pred.groupby("trade_date")["rank"].min().eq(1).all()
        assert {"trade_date", "symbol", "future_return", "horizon_days", "label_date"}.issubset(labels.columns)
        assert set(labels["horizon_days"]) == {10}
        mock_qe_client.download_workspace_file_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_prediction_store_corruption_does_not_fallback_to_workspace(
        self,
        mock_qe_client,
        sample_pred_data,
        tmp_path,
    ):
        model_store = build_prediction_store_service(
            tmp_path,
            pred_obj=sample_pred_data,
        )
        manifest = model_store.artifact_store.load_manifest("qe_store_L1")
        blob = model_store.artifact_store.resolve_artifact_path(
            manifest["uri"],
            artifact_type="prediction",
        )
        blob.write_bytes(b"tampered")
        source = BacktestDataSource(
            base_loop_ref="qe_store/Loop1",
            cache_dir=str(tmp_path / "cache"),
            qe_client=mock_qe_client,
            model_store=model_store,
        )

        with pytest.raises(DataSourceError, match="manifest is corrupt"):
            await source.get_available_date_range()

        mock_qe_client.download_workspace_file_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_partial_manifest_target_corruption_does_not_fallback(
        self,
        mock_qe_client,
        sample_pred_data,
        sample_label_data,
        tmp_path,
    ):
        model_store = build_prediction_store_service(
            tmp_path,
            pred_obj=sample_pred_data,
            label_obj=sample_label_data,
        )
        manifest = model_store.artifact_store.load_manifest("qe_store_L1")
        label_blob = model_store.artifact_store.resolve_artifact_path(
            manifest["uri"],
            artifact_type="label",
        )
        label_blob.write_bytes(b"tampered-label")
        source = BacktestDataSource(
            base_loop_ref="qe_store/Loop1",
            cache_dir=str(tmp_path / "cache"),
            qe_client=mock_qe_client,
            model_store=model_store,
        )

        with pytest.raises(DataSourceError, match="target artifact is present but invalid"):
            await source.get_labels(
                date(2024, 7, 1),
                date(2024, 7, 2),
                horizon_days=10,
            )

        mock_qe_client.download_workspace_file_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_target_artifact_uses_explicit_workspace_fallback(
        self,
        mock_qe_client,
        sample_pred_data,
        sample_label_data,
        tmp_path,
    ):
        model_store = build_prediction_store_service(
            tmp_path,
            pred_obj=sample_pred_data,
        )
        mock_qe_client.download_workspace_file_bytes.return_value = pickle.dumps(sample_label_data)
        repository = MagicMock()
        repository.get_nth_trading_day.side_effect = lambda trade_date, horizon: trade_date + timedelta(days=horizon)
        source = BacktestDataSource(
            base_loop_ref="qe_store/Loop1",
            cache_dir=str(tmp_path / "cache"),
            qe_client=mock_qe_client,
            repository=repository,
            model_store=model_store,
        )

        labels = await source.get_labels(
            date(2024, 7, 1),
            date(2024, 7, 2),
            horizon_days=10,
        )

        assert len(labels) == len(sample_label_data)
        mock_qe_client.download_workspace_file_bytes.assert_awaited_once_with(
            "qe_store",
            "Loop1",
            "mlruns/1/abc123/artifacts/label.pkl",
        )
        source_info = source.get_artifact_source_info()
        assert source_info["pred.pkl"]["source"] == "prediction_store"
        assert source_info["label.pkl"]["source"] == "qe_workspace_cache"

    @pytest.mark.asyncio
    async def test_duplicate_prediction_identity_fails_loud(
        self,
        mock_qe_client,
        sample_pred_data,
        tmp_path,
    ):
        duplicate_data = pd.concat([sample_pred_data, sample_pred_data.iloc[[0]]], ignore_index=True)
        model_store = build_prediction_store_service(
            tmp_path,
            pred_obj=duplicate_data,
        )
        source = BacktestDataSource(
            base_loop_ref="qe_store/Loop1",
            cache_dir=str(tmp_path / "cache"),
            qe_client=mock_qe_client,
            model_store=model_store,
        )

        with pytest.raises(DataSourceError, match="duplicate identity keys"):
            await source.get_available_date_range()

        mock_qe_client.download_workspace_file_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_prediction_store_only_rejects_missing_artifact(
        self,
        mock_qe_client,
        tmp_path,
    ):
        model_store = ModelStoreService(artifact_store=PredictionArtifactStore(root=tmp_path / "empty_store"))
        source = BacktestDataSource(
            base_loop_ref="qe_missing/Loop1",
            cache_dir=str(tmp_path / "cache"),
            qe_client=mock_qe_client,
            model_store=model_store,
            artifact_source_preference="prediction_store_only",
        )

        with pytest.raises(DataSourceError, match="required but missing"):
            await source.get_available_date_range()

        mock_qe_client.download_workspace_file_bytes.assert_not_awaited()

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
        cache_manager.save_artifact(
            "qe_test/Loop1",
            "pred.pkl",
            pred_bytes,
            metadata=qe_provenance(
                pred_bytes,
                artifact_name="pred.pkl",
                row_count=len(sample_pred_data),
            ),
        )

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
        mock_qe_client.download_workspace_file_bytes.assert_not_called()

        # 验证数据正确
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_expired_manifest_invalidates_memory_cache(
        self,
        mock_qe_client,
        sample_pred_data,
        tmp_path,
    ):
        pred_bytes = pickle.dumps(sample_pred_data)
        mock_qe_client.download_workspace_file_bytes.return_value = pred_bytes
        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir=str(tmp_path / "cache"),
            qe_client=mock_qe_client,
        )
        await source.get_predictions(date(2024, 7, 1), date(2024, 7, 2))
        manifest_path = source.cache_manager._manifest_path("qe_test/Loop1", "pred.pkl")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["expires_at"] = "2000-01-01T00:00:00Z"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        await source.get_predictions(date(2024, 7, 1), date(2024, 7, 2))

        assert mock_qe_client.download_workspace_file_bytes.await_count == 2

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
            return_value=pd.DataFrame(columns=["trade_date", "symbol", "future_return"])
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
        repository = MagicMock()
        repository.get_sector_mapping.return_value = {
            "000001.SZ": "801780.SI",
            "600000.SH": "801192.SI",
        }
        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir="tmp/test_cache/",
            qe_client=mock_qe_client,
            repository=repository,
        )

        mapping = await source.get_sector_mapping(date(2024, 7, 1))

        repository.get_sector_mapping.assert_called_once_with(date(2024, 7, 1))
        assert mapping["000001.SZ"] == "801780.SI"
        assert mapping["600000.SH"] == "801192.SI"

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
            return pred_bytes

        pred_bytes = pickle.dumps(sample_pred_data)
        mock_qe_client.download_workspace_file_bytes = slow_download

        async def get_workspace_file(task_id, loop_name, path):
            if path in {"qe_current_recorder.json", "qe_extracted_recorder.json"}:
                return {"experiment_id": "1", "recorder_id": "abc123"}
            return {
                "artifact_name": "pred.pkl",
                "schema_version": "qe_dataframe_v1",
                "sha256": hashlib.sha256(pred_bytes).hexdigest(),
                "size_bytes": len(pred_bytes),
                "row_count": len(sample_pred_data),
                "quality_status": "ok",
            }

        mock_qe_client.get_workspace_file = AsyncMock(side_effect=get_workspace_file)

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
        测试交易日历计算（使用真实 trading_calendar）

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
    async def test_forbid_config_file_download(self, tmp_path):
        """验证 _download_artifact 强制拒绝非白名单文件（如配置文件）"""
        mock_qe_client = MagicMock()
        mock_qe_client.get_workspace_file = AsyncMock()
        mock_qe_client.download_workspace_file_bytes = AsyncMock()

        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir=str(tmp_path / "cache"),
            qe_client=mock_qe_client,
        )

        forbidden_files = [
            "config.json",
            "hmm_config.yaml",
            "strategy.toml",
            "model_train_configs.json",
        ]

        for filename in forbidden_files:
            with pytest.raises(DataSourceError, match="only|permitted"):
                await source._download_artifact(filename)

        # 被拒绝时不得真正发起下载
        mock_qe_client.get_workspace_file.assert_not_called()
        mock_qe_client.download_workspace_file_bytes.assert_not_called()

    @pytest.mark.asyncio
    async def test_allow_whitelisted_artifact_download(self, tmp_path):
        """验证白名单内的 artifact（pred.pkl/label.pkl）允许下载"""
        import pickle

        sample_df = pd.DataFrame(
            [
                {"trade_date": date(2024, 7, 1), "symbol": "000001.SZ", "score": 0.5},
            ]
        )
        mock_qe_client = MagicMock()
        pred_bytes = pickle.dumps(sample_df)
        mock_qe_client.download_workspace_file_bytes = AsyncMock(return_value=pred_bytes)

        async def get_workspace_file(task_id, loop_name, path):
            if path in {"qe_current_recorder.json", "qe_extracted_recorder.json"}:
                return {"experiment_id": "1", "recorder_id": "abc123"}
            return {
                "artifact_name": "pred.pkl",
                "schema_version": "qe_dataframe_v1",
                "sha256": hashlib.sha256(pred_bytes).hexdigest(),
                "size_bytes": len(pred_bytes),
                "row_count": len(sample_df),
                "quality_status": "ok",
            }

        mock_qe_client.get_workspace_file = AsyncMock(side_effect=get_workspace_file)

        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir=str(tmp_path / "cache"),
            qe_client=mock_qe_client,
        )

        # pred.pkl 在白名单内，应成功下载并缓存
        await source._download_artifact("pred.pkl")
        mock_qe_client.download_workspace_file_bytes.assert_awaited_once_with(
            "qe_test",
            "Loop1",
            "mlruns/1/abc123/artifacts/pred.pkl",
        )
        assert source.cache_manager.is_cached("qe_test/Loop1", "pred.pkl")

    @pytest.mark.asyncio
    async def test_resolve_recorder_metadata_fallback(self, tmp_path):
        """首选 recorder ref 缺失时使用受控的 extracted metadata。"""
        client = MagicMock()
        sample_df = pd.DataFrame(
            [
                {
                    "trade_date": date(2024, 7, 1),
                    "symbol": "000001.SZ",
                    "score": 0.5,
                }
            ]
        )
        pred_bytes = pickle.dumps(sample_df)
        client.download_workspace_file_bytes = AsyncMock(return_value=pred_bytes)

        async def get_workspace_file(task_id, loop_name, path):
            if path == "qe_current_recorder.json":
                raise FileNotFoundError("current recorder missing")
            if path == "qe_extracted_recorder.json":
                return {"selected_experiment_id": "42", "selected_recorder_id": "deadbeef"}
            return {
                "artifact_name": "pred.pkl",
                "schema_version": "qe_dataframe_v1",
                "sha256": hashlib.sha256(pred_bytes).hexdigest(),
                "size_bytes": len(pred_bytes),
                "row_count": len(sample_df),
                "quality_status": "ok",
            }

        client.get_workspace_file = AsyncMock(side_effect=get_workspace_file)

        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir=str(tmp_path / "cache"),
            qe_client=client,
        )
        await source._download_artifact("pred.pkl")

        client.download_workspace_file_bytes.assert_awaited_once_with(
            "qe_test",
            "Loop1",
            "mlruns/42/deadbeef/artifacts/pred.pkl",
        )

    @pytest.mark.asyncio
    async def test_legacy_terminal_log_resolves_only_the_receipted_recorder(self):
        recorder_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        log_payload = (
            f"Recorder {recorder_id} starts running under Experiment 42 ...\n"
            f"Latest recorder: {{'class': 'Recorder', 'id': '{recorder_id}', "
            "'experiment_id': '42', 'status': 'FINISHED'}\n"
        )
        manifest = legacy_manifest_fixture(log_payload)
        client = MagicMock()

        async def get_workspace_file(task_id, loop_name, path):
            if path in {"qe_current_recorder.json", "qe_extracted_recorder.json"}:
                raise FileNotFoundError("sidecar missing")
            if path == "run.log":
                return log_payload
            raise FileNotFoundError(path)

        client.get_workspace_file = AsyncMock(side_effect=get_workspace_file)
        client.list_workspace_files = AsyncMock(
            return_value={
                "catalog_completeness": "complete",
                "files": [
                    {
                        "relative_path": "run.log",
                        "size_bytes": len(log_payload.encode("utf-8")),
                        "access_mode": "inspection_only",
                    },
                    *[
                        {"relative_path": item.workspace_path}
                        for item in manifest.artifacts
                    ],
                ],
            }
        )

        with patch(
            "backend.services.hmm_data_source.backtest_source.find_legacy_qe_artifact_manifest",
            return_value=manifest,
        ):
            path = await BacktestDataSource._resolve_workspace_artifact_path(
                client,
                task_id="qe_test",
                loop_name="Loop1",
                artifact_name="pred.pkl",
            )

        assert path == manifest.artifact("pred.pkl").workspace_path

    @pytest.mark.asyncio
    async def test_legacy_terminal_log_tamper_fails_closed(self):
        recorder_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        original_log = (
            f"Recorder {recorder_id} starts running under Experiment 42 ...\n"
            f"Latest recorder: {{'class': 'Recorder', 'id': '{recorder_id}', "
            "'experiment_id': '42', 'status': 'FINISHED'}\n"
        )
        manifest = legacy_manifest_fixture(original_log)
        tampered_log = original_log.replace("FINISHED", "FAILED__")
        client = MagicMock()

        async def get_workspace_file(task_id, loop_name, path):
            if path in {"qe_current_recorder.json", "qe_extracted_recorder.json"}:
                raise FileNotFoundError("sidecar missing")
            return tampered_log

        client.get_workspace_file = AsyncMock(side_effect=get_workspace_file)
        client.list_workspace_files = AsyncMock(
            return_value={
                "catalog_completeness": "complete",
                "files": [
                    {
                        "relative_path": "run.log",
                        "size_bytes": len(original_log.encode("utf-8")),
                        "access_mode": "inspection_only",
                    },
                    *[
                        {"relative_path": item.workspace_path}
                        for item in manifest.artifacts
                    ],
                ],
            }
        )

        with (
            patch(
                "backend.services.hmm_data_source.backtest_source.find_legacy_qe_artifact_manifest",
                return_value=manifest,
            ),
            pytest.raises(DataSourceError, match="immutable receipt"),
        ):
            await BacktestDataSource._resolve_workspace_artifact_path(
                client,
                task_id="qe_test",
                loop_name="Loop1",
                artifact_name="pred.pkl",
            )

    @pytest.mark.asyncio
    async def test_legacy_manifest_supplies_exact_artifact_integrity(self):
        manifest = legacy_manifest_fixture("receipted run log")
        client = MagicMock()
        client.get_workspace_file = AsyncMock(side_effect=FileNotFoundError("remote manifest missing"))
        client.list_workspace_files = AsyncMock(
            return_value={"catalog_completeness": "complete", "files": []}
        )

        with patch(
            "backend.services.hmm_data_source.backtest_source.find_legacy_qe_artifact_manifest",
            return_value=manifest,
        ):
            receipt, source = await BacktestDataSource._resolve_remote_artifact_manifest(
                client,
                task_id="qe_test",
                loop_name="Loop1",
                artifact_name="pred.pkl",
                artifact_path=manifest.artifact("pred.pkl").workspace_path,
            )

        assert source == "legacy_qe_artifact_manifests.py"
        assert receipt.sha256 == "1" * 64
        assert receipt.size_bytes == 100
        assert receipt.row_count == 10

    @pytest.mark.asyncio
    async def test_cataloged_invalid_remote_manifest_blocks_legacy_fallback(self):
        manifest = legacy_manifest_fixture("receipted run log")
        artifact_path = manifest.artifact("pred.pkl").workspace_path
        remote_manifest_path = f"{artifact_path}.manifest.json"
        client = MagicMock()
        client.get_workspace_file = AsyncMock(return_value={"artifact_name": "pred.pkl"})
        client.list_workspace_files = AsyncMock(
            return_value={
                "catalog_completeness": "complete",
                "files": [{"relative_path": remote_manifest_path}],
            }
        )

        with (
            patch(
                "backend.services.hmm_data_source.backtest_source.find_legacy_qe_artifact_manifest",
                return_value=manifest,
            ),
            pytest.raises(DataSourceError, match="cataloged but could not be validated"),
        ):
            await BacktestDataSource._resolve_remote_artifact_manifest(
                client,
                task_id="qe_test",
                loop_name="Loop1",
                artifact_name="pred.pkl",
                artifact_path=artifact_path,
            )

    @pytest.mark.asyncio
    async def test_conflicting_recorder_sidecars_fail_closed(self):
        client = MagicMock()

        async def get_workspace_file(task_id, loop_name, path):
            if path == "qe_current_recorder.json":
                return {"experiment_id": "42", "recorder_id": "recorder-a"}
            return {"selected_experiment_id": "42", "selected_recorder_id": "recorder-b"}

        client.get_workspace_file = AsyncMock(side_effect=get_workspace_file)

        with pytest.raises(DataSourceError, match="Conflicting QE recorder sidecars"):
            await BacktestDataSource._resolve_workspace_artifact_path(
                client,
                task_id="qe_test",
                loop_name="Loop1",
                artifact_name="pred.pkl",
            )

    def test_repository_legacy_manifest_is_exact(self):
        manifest = find_legacy_qe_artifact_manifest("qe_20260502_131502_9b54/Loop1")

        assert manifest is not None
        assert manifest.recorder_experiment_id == "308973027052385728"
        assert manifest.recorder_id == "5c85da5785e9495b85c36d5b6f6e97b9"
        assert manifest.artifact("pred.pkl").sha256 == (
            "24ca37fc573f57b0c1759501af7b0b17e4cf02c8fbf97144e49c73696a694da6"
        )

    @pytest.mark.asyncio
    async def test_invalid_recorder_metadata_fails_before_download(self, tmp_path):
        """Recorder identity 不能包含路径穿越片段。"""
        client = MagicMock()
        client.get_workspace_file = AsyncMock(
            return_value={
                "experiment_id": "../42",
                "recorder_id": "abc123",
            }
        )
        client.download_workspace_file_bytes = AsyncMock()

        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir=str(tmp_path / "cache"),
            qe_client=client,
        )

        with pytest.raises(DataSourceError, match="Invalid QE recorder sidecar"):
            await source._download_artifact("pred.pkl")
        client.download_workspace_file_bytes.assert_not_called()

    @pytest.mark.asyncio
    async def test_default_client_resolves_task_node_and_is_closed(self, tmp_path):
        """未注入 client 时按任务 node_id 创建，并由 source 释放连接池。"""
        client = MagicMock()
        sample_df = pd.DataFrame(
            [
                {
                    "trade_date": date(2024, 7, 1),
                    "symbol": "000001.SZ",
                    "score": 0.5,
                }
            ]
        )
        pred_bytes = pickle.dumps(sample_df)
        client.download_workspace_file_bytes = AsyncMock(return_value=pred_bytes)

        async def get_workspace_file(task_id, loop_name, path):
            if path in {"qe_current_recorder.json", "qe_extracted_recorder.json"}:
                return {"experiment_id": "1", "recorder_id": "abc123"}
            return {
                "artifact_name": "pred.pkl",
                "schema_version": "qe_dataframe_v1",
                "sha256": hashlib.sha256(pred_bytes).hexdigest(),
                "size_bytes": len(pred_bytes),
                "row_count": len(sample_df),
                "quality_status": "ok",
            }

        client.get_workspace_file = AsyncMock(side_effect=get_workspace_file)
        client.close = AsyncMock()

        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir=str(tmp_path / "cache"),
        )
        with (
            patch.object(source, "_resolve_task_node_id", return_value="rdagent-node1"),
            patch(
                "backend.services.hmm_data_source.backtest_source.QEWorkspaceClient.for_node",
                return_value=client,
            ) as for_node,
        ):
            await source._download_artifact("pred.pkl")
            await source.aclose()

        for_node.assert_called_once_with("rdagent-node1")
        client.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_remote_manifest_mismatch_fails_before_cache_publish(self, tmp_path):
        """远端 SHA/size 不匹配时不得发布或反序列化 artifact。"""
        pred_bytes = pickle.dumps(pd.DataFrame([{"score": 0.5}]))
        client = MagicMock()
        client.download_workspace_file_bytes = AsyncMock(return_value=pred_bytes)

        async def get_workspace_file(task_id, loop_name, path):
            if path in {"qe_current_recorder.json", "qe_extracted_recorder.json"}:
                return {"experiment_id": "1", "recorder_id": "abc123"}
            return {
                "artifact_name": "pred.pkl",
                "schema_version": "qe_dataframe_v1",
                "sha256": "0" * 64,
                "size_bytes": len(pred_bytes),
                "row_count": 1,
                "quality_status": "ok",
            }

        client.get_workspace_file = AsyncMock(side_effect=get_workspace_file)
        source = BacktestDataSource(
            base_loop_ref="qe_test/Loop1",
            cache_dir=str(tmp_path / "cache"),
            qe_client=client,
        )

        with pytest.raises(DataSourceError, match="trusted remote manifest"):
            await source._download_artifact("pred.pkl")
        assert not source.cache_manager.is_cached("qe_test/Loop1", "pred.pkl")

    @pytest.mark.asyncio
    async def test_only_read_artifact_files(self, tmp_path):
        """验证只读取 artifact 文件（pred.pkl, label.pkl）"""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        from backend.services.hmm_data_source import ArtifactCacheManager

        cache_manager = ArtifactCacheManager(str(cache_dir), allow_test_fixtures=True)

        # 验证缓存管理器只处理 .pkl 文件
        allowed_artifacts = ["pred.pkl", "label.pkl"]

        for artifact in allowed_artifacts:
            # 应该成功保存
            cache_manager.save_artifact(
                "qe_test/Loop1",
                artifact,
                b"test_data",
                metadata={"source": "test_fixture"},
            )

        # 验证缓存信息
        cache_info = cache_manager.get_cache_info("qe_test/Loop1")
        assert cache_info["cached"]
        assert len(cache_info["artifacts"]) == 2
