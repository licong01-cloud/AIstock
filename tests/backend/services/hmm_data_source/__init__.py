"""
HMM 数据源单元测试

测试覆盖:
- BacktestDataSource
- RealtimeDataSource
- ArtifactCacheManager
- 异常处理
- 隔离约束验证
"""

import pytest

# 测试配置
pytest_plugins = ['pytest_asyncio']
