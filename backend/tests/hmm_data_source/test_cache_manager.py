"""
ArtifactCacheManager 单元测试

测试内容:
1. artifact 路径生成
2. 保存功能
3. 校验功能（有效文件）
4. 校验功能（损坏文件）
5. 清理缓存
6. 缓存信息查询
"""

import hashlib
import pickle

import pytest

from backend.services.hmm_data_source import ArtifactCacheManager, CacheError


class TestArtifactCacheManager:
    """ArtifactCacheManager 单元测试"""

    @pytest.fixture
    def cache_dir(self, tmp_path):
        """临时缓存目录"""
        cache = tmp_path / "cache"
        cache.mkdir()
        return str(cache)

    @pytest.fixture
    def cache_manager(self, cache_dir):
        """缓存管理器实例"""
        return ArtifactCacheManager(cache_dir)

    def test_get_artifact_path(self, cache_manager):
        """测试 artifact 路径生成"""
        path = cache_manager.get_artifact_path(
            "qe_20260502_131502_9b54/Loop1",
            "pred.pkl"
        )

        # 验证路径格式
        assert "qe_20260502_131502_9b54_Loop1" in str(path)
        assert path.name == "pred.pkl"

    def test_save_and_load_artifact(self, cache_manager):
        """测试保存和加载 artifact"""
        test_data = b"test_artifact_content"
        loop_ref = "qe_test/Loop1"
        artifact_name = "pred.pkl"

        # 保存
        saved_path = cache_manager.save_artifact(
            loop_ref,
            artifact_name,
            test_data
        )

        assert saved_path.exists()

        # 加载
        loaded_data = cache_manager.load_artifact(loop_ref, artifact_name)
        assert loaded_data == test_data

    def test_checksum_validation_valid(self, cache_manager):
        """测试 SHA256 校验（有效文件）"""
        test_data = b"test_content_for_checksum"
        loop_ref = "qe_test/Loop1"
        artifact_name = "test.pkl"

        # 保存
        cache_manager.save_artifact(loop_ref, artifact_name, test_data)

        # 加载并验证校验和
        loaded_data = cache_manager.load_artifact(
            loop_ref,
            artifact_name,
            verify_checksum=True
        )

        assert loaded_data == test_data

    def test_checksum_validation_corrupted(self, cache_manager):
        """测试 SHA256 校验（损坏文件）"""
        test_data = b"original_content"
        loop_ref = "qe_test/Loop1"
        artifact_name = "test.pkl"

        # 保存
        saved_path = cache_manager.save_artifact(loop_ref, artifact_name, test_data)

        # 手动篡改文件
        saved_path.write_bytes(b"corrupted_content")

        # 尝试加载应该失败
        with pytest.raises(CacheError, match="Checksum mismatch"):
            cache_manager.load_artifact(
                loop_ref,
                artifact_name,
                verify_checksum=True
            )

    def test_clear_cache_specific_loop(self, cache_manager):
        """测试清理特定 loop 的缓存"""
        loop1 = "qe_test/Loop1"
        loop2 = "qe_test/Loop2"

        # 保存两个 loop 的数据
        cache_manager.save_artifact(loop1, "pred.pkl", b"data1")
        cache_manager.save_artifact(loop2, "pred.pkl", b"data2")

        # 清理 loop1
        cache_manager.clear_cache(loop1)

        # 验证 loop1 已清理，loop2 仍存在
        assert not cache_manager.is_cached(loop1, "pred.pkl")
        assert cache_manager.is_cached(loop2, "pred.pkl")

    def test_clear_cache_all(self, cache_manager):
        """测试清理所有缓存"""
        loop1 = "qe_test/Loop1"
        loop2 = "qe_test/Loop2"

        # 保存多个 loop 的数据
        cache_manager.save_artifact(loop1, "pred.pkl", b"data1")
        cache_manager.save_artifact(loop2, "pred.pkl", b"data2")

        # 清理所有
        cache_manager.clear_cache()

        # 验证都已清理
        assert not cache_manager.is_cached(loop1, "pred.pkl")
        assert not cache_manager.is_cached(loop2, "pred.pkl")

    def test_get_cache_info_specific_loop(self, cache_manager):
        """测试获取特定 loop 的缓存信息"""
        loop_ref = "qe_test/Loop1"

        # 保存数据
        cache_manager.save_artifact(loop_ref, "pred.pkl", b"x" * 1000)
        cache_manager.save_artifact(loop_ref, "label.pkl", b"y" * 2000)

        # 获取缓存信息
        info = cache_manager.get_cache_info(loop_ref)

        assert info['cached']
        assert info['loop_ref'] == loop_ref
        assert len(info['artifacts']) == 2
        assert info['artifacts']['pred.pkl']['size'] == 1000
        assert info['artifacts']['label.pkl']['size'] == 2000
        assert info['total_size'] == 3000

    def test_get_cache_info_all_loops(self, cache_manager):
        """测试获取所有 loop 的缓存信息"""
        loop1 = "qe_test/Loop1"
        loop2 = "qe_test/Loop2"

        # 保存多个 loop 的数据
        cache_manager.save_artifact(loop1, "pred.pkl", b"data1")
        cache_manager.save_artifact(loop2, "pred.pkl", b"data2")

        # 获取所有缓存信息
        all_info = cache_manager.get_cache_info()

        assert loop1 in all_info
        assert loop2 in all_info
        assert all_info[loop1]['cached']
        assert all_info[loop2]['cached']

    def test_load_pickle(self, cache_manager):
        """测试加载 pickle 文件并反序列化"""
        loop_ref = "qe_test/Loop1"
        artifact_name = "data.pkl"

        # 保存 Python 对象
        test_obj = {'key': 'value', 'list': [1, 2, 3]}
        pickle_bytes = pickle.dumps(test_obj)
        cache_manager.save_artifact(loop_ref, artifact_name, pickle_bytes)

        # 加载并反序列化
        loaded_obj = cache_manager.load_pickle(loop_ref, artifact_name)

        assert loaded_obj == test_obj

    def test_is_cached(self, cache_manager):
        """测试检查缓存是否存在"""
        loop_ref = "qe_test/Loop1"
        artifact_name = "pred.pkl"

        # 初始不存在
        assert not cache_manager.is_cached(loop_ref, artifact_name)

        # 保存后存在
        cache_manager.save_artifact(loop_ref, artifact_name, b"data")
        assert cache_manager.is_cached(loop_ref, artifact_name)

    def test_metadata_persistence(self, cache_manager):
        """测试元数据持久化"""
        loop_ref = "qe_test/Loop1"
        artifact_name = "pred.pkl"
        test_data = b"test_data"

        # 保存时附带元数据
        cache_manager.save_artifact(
            loop_ref,
            artifact_name,
            test_data,
            metadata={'custom_key': 'custom_value'}
        )

        # 验证元数据被保存
        metadata = cache_manager._load_metadata(loop_ref, artifact_name)
        assert metadata is not None
        assert metadata['custom_key'] == 'custom_value'
        assert 'sha256' in metadata
        assert metadata['sha256'] == hashlib.sha256(test_data).hexdigest()
