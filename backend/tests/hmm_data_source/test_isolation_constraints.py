"""
隔离约束验证测试

验证 Phase 0 完全符合隔离约束，确保：
1. 不读取生产配置表
2. 不修改生产表
3. 只使用独立缓存目录
4. 只下载 artifact 文件（不下载配置）

这些测试是阻塞性的 - 任何一项失败都应该停止验收。
"""

from pathlib import Path

import pytest

from backend.services.hmm_data_source import (
    BacktestDataSource,
    ArtifactCacheManager,
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

    def test_cache_directory_isolation(self, tmp_path):
        """验证缓存目录完全隔离"""
        # 创建缓存管理器
        cache_dir = tmp_path / "hmm_evolution_cache"
        cache_manager = ArtifactCacheManager(str(cache_dir))

        # 保存数据
        cache_manager.save_artifact("qe_test/Loop1", "pred.pkl", b"test_data")

        # 验证缓存只在指定目录
        assert cache_dir.exists()
        assert (cache_dir / "qe_test_Loop1" / "pred.pkl").exists()

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
            'market.sw_member',
            'market.trade_cal',
            'market.stock_basic',
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

        # 只允许调用 download_artifact
        assert 'download_artifact' in content

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


class TestDatabasePermissions:
    """数据库权限隔离验证"""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_readonly_user_cannot_write_production_tables(self):
        """
        验证只读用户无法修改生产表

        需要真实数据库连接和权限配置
        """
        from backend.db.pg_pool import get_conn

        # 尝试更新生产表（应该失败）
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                with pytest.raises(Exception):
                    # 尝试更新 model_train_configs（应该被拒绝）
                    await cur.execute("""
                        UPDATE model_train_configs
                        SET config_json = '{}'
                        WHERE 1=0
                    """)

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_can_write_evolution_tables(self):
        """
        验证可以写入演进系统专用表

        需要真实数据库连接和权限配置
        """
        from backend.db.pg_pool import get_conn

        # 尝试写入演进系统表（应该成功）
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                # 创建测试表（如果不存在）
                await cur.execute("""
                    CREATE SCHEMA IF NOT EXISTS hmm_evolution
                """)

                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS hmm_evolution.test_isolation (
                        id SERIAL PRIMARY KEY,
                        test_data TEXT
                    )
                """)

                # 插入测试数据（应该成功）
                await cur.execute("""
                    INSERT INTO hmm_evolution.test_isolation (test_data)
                    VALUES ('test')
                """)

                # 清理
                await cur.execute("""
                    DROP TABLE hmm_evolution.test_isolation
                """)


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

    def test_cache_dir_configurable(self):
        """验证缓存目录可配置"""
        from backend.services.hmm_data_source import ArtifactCacheManager

        # 测试自定义缓存目录（跨平台比较，规避 Windows 反斜杠差异）
        custom_dir = "custom/cache/path"
        manager = ArtifactCacheManager(custom_dir)

        assert manager.cache_dir == Path(custom_dir)

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
