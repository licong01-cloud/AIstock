"""
HMM 数据源单元测试

测试覆盖:
- BacktestDataSource
- RealtimeDataSource
- ArtifactCacheManager
- 异常处理
- 隔离约束验证

异步测试采用项目约定：同步 test 函数内部调用 asyncio.run(...)，
不依赖 pytest_asyncio（项目未安装该插件）。
"""
