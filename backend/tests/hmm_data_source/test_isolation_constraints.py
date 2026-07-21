"""
隔离约束验证测试

验证 Phase 0 完全符合隔离约束，确保：
1. 不读取生产配置表
2. 不修改生产表
3. 只使用独立缓存目录
4. 只下载 artifact 文件（不下载配置）

这些测试是阻塞性的 - 任何一项失败都应该停止验收。
"""

from datetime import date
from pathlib import Path

import pytest

from backend.services.hmm_data_source import (
    BacktestDataSource,
    ArtifactCacheManager,
)
from backend.services.hmm_data_source.legacy_qe_artifact_manifests import (
    LEGACY_QE_ARTIFACT_MANIFESTS,
    LegacyQESTPITCompatibilityReceipt,
)


class TestIsolationConstraints:
    """隔离约束验证测试（阻塞项）"""

    def test_no_production_table_imports(self):
        """验证代码中不导入生产表相关模块"""
        # 检查所有源文件
        source_dir = Path("backend/services/hmm_data_source")

        forbidden_imports = [
            'model_train_configs',
            'model_train_snapshots',
            'strategy_packages',
            'paper_v2',
        ]

        for py_file in source_dir.glob("*.py"):
            content = py_file.read_text(encoding="utf-8")

            for forbidden in forbidden_imports:
                assert forbidden not in content, (
                    f"Found forbidden reference '{forbidden}' in {py_file.name}"
                )

    def test_no_write_operations_to_production_tables(self):
        """验证代码中没有对生产表的写操作"""
        source_dir = Path("backend/services/hmm_data_source")

        forbidden_patterns = [
            'UPDATE model_train_configs',
            'DELETE FROM model_train_configs',
            'INSERT INTO model_train_configs',
            'UPDATE model_train_snapshots',
            'DELETE FROM model_train_snapshots',
            'INSERT INTO model_train_snapshots',
            'UPDATE strategy_packages',
            'DELETE FROM strategy_packages',
            'INSERT INTO strategy_packages',
            'UPDATE paper_v2',
            'DELETE FROM paper_v2',
            'INSERT INTO paper_v2',
        ]

        for py_file in source_dir.glob("*.py"):
            content = py_file.read_text(encoding="utf-8")

            for pattern in forbidden_patterns:
                assert pattern not in content, (
                    f"Found forbidden SQL '{pattern}' in {py_file.name}"
                )

    def test_only_artifact_files_allowed(self):
        """验证回测数据源在代码级强制只允许下载 pred.pkl/label.pkl 白名单"""
        # 白名单必须精确等于 {pred.pkl, label.pkl}，不得放宽
        assert BacktestDataSource.ALLOWED_ARTIFACTS == frozenset(
            {"pred.pkl", "label.pkl"}
        )

        # 配置类文件后缀不得出现在白名单内
        for forbidden_suffix in (".json", ".yaml", ".toml"):
            for allowed in BacktestDataSource.ALLOWED_ARTIFACTS:
                assert not allowed.endswith(forbidden_suffix)

    def test_legacy_manifest_does_not_expand_artifact_download_whitelist(self):
        assert LEGACY_QE_ARTIFACT_MANIFESTS
        for entry in LEGACY_QE_ARTIFACT_MANIFESTS:
            assert entry.recorder_evidence.workspace_path == "run.log"
            assert entry.recorder_evidence.terminal_status == "FINISHED"
            assert {item.artifact_name for item in entry.artifacts} == {
                "pred.pkl",
                "label.pkl",
            }
            assert all(
                item.workspace_path.endswith(f"/artifacts/{item.artifact_name}")
                for item in entry.artifacts
            )
            if entry.st_pit_compatibility is not None:
                assert entry.st_pit_compatibility.workspace_path == "qe_event_risk_policy.json"
                assert entry.st_pit_compatibility.binding_mode == (
                    "legacy_allowlisted_compatibility_artifact_v1"
                )
                assert entry.st_pit_compatibility.sha256
                assert entry.st_pit_compatibility.source_config_sha256
                assert entry.st_pit_compatibility.stock_pool_sha256

        assert "run.log" not in BacktestDataSource.ALLOWED_ARTIFACTS
        assert "qe_event_risk_policy.json" not in BacktestDataSource.ALLOWED_ARTIFACTS

    def test_legacy_st_pit_receipt_rejects_arbitrary_workspace_path(self):
        with pytest.raises(ValueError, match="source identity is invalid"):
            LegacyQESTPITCompatibilityReceipt(
                artifact_source_task_id="qe_task",
                artifact_source_loop_name="Loop1",
                workspace_path="config.json",
                sha256="a" * 64,
                size_bytes=1,
                source_config_sha256="b" * 64,
                stock_pool_sha256="c" * 64,
                universe_key="shsz_st_pit_active_v1",
                rule_version="st_pub_next_trade_restore_active_l_v1",
                scope="st_only_active",
                source_fingerprint_sha256="d" * 64,
                start_date=date(2024, 7, 1),
                end_date=date(2026, 4, 27),
                span_count=1,
            )

    def test_cache_directory_isolation(self, tmp_path):
        """验证缓存目录完全隔离"""
        # 创建缓存管理器
        cache_dir = tmp_path / "hmm_evolution_cache"
        cache_manager = ArtifactCacheManager(
            str(cache_dir), allow_test_fixtures=True
        )

        # 保存数据
        cache_manager.save_artifact(
            "qe_test/Loop1",
            "pred.pkl",
            b"test_data",
            metadata={"source": "test_fixture"},
        )

        # 验证缓存只在指定目录
        assert cache_dir.exists()
        assert cache_manager.get_artifact_path("qe_test/Loop1", "pred.pkl").exists()

        # 验证没有污染其他目录
        parent_dir = tmp_path
        for item in parent_dir.iterdir():
            if item.name != "hmm_evolution_cache":
                assert not (item / "pred.pkl").exists()

    @pytest.mark.asyncio
    async def test_only_read_operations_on_market_tables(self):
        """验证对 market.* 表只有 SELECT 操作"""
        source_dir = Path("backend/services/hmm_data_source")

        allowed_market_tables = [
            'market.kline_daily_raw',
            'market.sw_index_member',
            'market.trading_calendar',
        ]

        forbidden_operations = ['UPDATE', 'DELETE', 'INSERT INTO', 'DROP', 'ALTER']

        for py_file in source_dir.glob("*.py"):
            content = py_file.read_text(encoding="utf-8")

            for table in allowed_market_tables:
                if table in content:
                    # 检查是否有写操作
                    for op in forbidden_operations:
                        pattern = f"{op} {table}"
                        assert pattern not in content, (
                            f"Found forbidden operation '{pattern}' in {py_file.name}"
                        )

    def test_no_qe_config_api_calls(self):
        """验证不调用 QE 配置相关 API"""
        source_file = Path("backend/services/hmm_data_source/backtest_source.py")
        content = source_file.read_text(encoding="utf-8")

        # 只允许通过现有 workspace 文件下载契约读取 allowlisted artifact
        assert 'download_workspace_file_bytes' in content
        assert '.download_artifact(' not in content

        # 禁止调用配置 API
        forbidden_api_calls = [
            'get_config',
            'update_config',
            'get_hmm_config',
            'update_hmm_config',
        ]

        for api_call in forbidden_api_calls:
            assert api_call not in content, (
                f"Found forbidden API call '{api_call}' in backtest_source.py"
            )

    def test_no_paper_v2_api_calls(self):
        """验证不调用模拟盘 API"""
        source_dir = Path("backend/services/hmm_data_source")

        forbidden_api_patterns = [
            'paper_v2',
            'portfolio',
            'simulation',
        ]

        for py_file in source_dir.glob("*.py"):
            content = py_file.read_text(encoding="utf-8")

            for pattern in forbidden_api_patterns:
                # 允许在注释中提及
                lines = [line for line in content.split('\n') if not line.strip().startswith('#')]
                content_without_comments = '\n'.join(lines)

                assert pattern not in content_without_comments, (
                    f"Found forbidden API pattern '{pattern}' in {py_file.name}"
                )


class TestCodeQuality:
    """代码质量检查（隔离相关）"""

    def test_no_absolute_paths_in_code(self):
        """验证代码中没有硬编码的绝对路径"""
        source_dir = Path("backend/services/hmm_data_source")

        for py_file in source_dir.glob("*.py"):
            content = py_file.read_text(encoding="utf-8")

            # 检查常见的绝对路径模式
            forbidden_patterns = [
                '/f/Dev/',
                'C:\\',
                'D:\\',
                '/home/',
                '/root/',
            ]

            for pattern in forbidden_patterns:
                assert pattern not in content, (
                    f"Found hardcoded absolute path '{pattern}' in {py_file.name}"
                )

    def test_cache_dir_configurable(self, tmp_path):
        """验证缓存目录可配置"""
        from backend.services.hmm_data_source import ArtifactCacheManager

        # 测试自定义缓存目录（跨平台比较，规避 Windows 反斜杠差异）
        custom_dir = tmp_path / "custom-cache"
        manager = ArtifactCacheManager(str(custom_dir))

        assert manager.cache_dir == custom_dir

    def test_no_production_credentials_in_code(self):
        """验证代码中没有硬编码的生产凭证"""
        source_dir = Path("backend/services/hmm_data_source")

        forbidden_patterns = [
            'password',
            'secret',
            'token',
            'api_key',
        ]

        for py_file in source_dir.glob("*.py"):
            content = py_file.read_text(encoding="utf-8").lower()

            # 检查赋值语句中的凭证
            lines = content.split('\n')
            for line in lines:
                if '=' in line and not line.strip().startswith('#'):
                    for pattern in forbidden_patterns:
                        if pattern in line and '"""' not in line:
                            # 允许在文档字符串中提及
                            if 'password' in line or 'secret' in line:
                                raise AssertionError(
                                    f"Found potential hardcoded credential in {py_file.name}: {line.strip()}"
                                )
