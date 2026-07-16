"""
hmm_data_source 测试包的本地配置。

项目未安装 pytest_asyncio，且既有测试约定为「同步 test 函数内部调用 asyncio.run」。
本目录的测试使用 `async def test_* + @pytest.mark.asyncio` 风格，因此在此提供一个
最小的协程执行钩子：当 test 函数是协程函数时，用 asyncio.run 驱动它。

这样既保持测试文件风格统一，又不引入新的第三方依赖。
"""
import asyncio
import inspect

import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: 以 asyncio.run 驱动的协程测试")
    config.addinivalue_line("markers", "integration: 需要真实数据库/外部依赖的集成测试")


def pytest_addoption(parser):
    # 若上层 conftest 已注册同名选项，避免重复注册报错
    try:
        parser.addoption(
            "--run-integration",
            action="store_true",
            default=False,
            help="运行需要真实数据库/QE workspace 的集成测试",
        )
    except ValueError:
        pass


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration"):
        return
    skip_integration = pytest.mark.skip(
        reason="集成测试需真实 DB/QE，使用 --run-integration 运行"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem):
    """协程 test 函数：用 asyncio.run 执行，返回 True 表示已处理。"""
    test_fn = pyfuncitem.obj
    if not inspect.iscoroutinefunction(test_fn):
        return None

    # 只传入该测试实际声明的 fixture 参数
    sig = inspect.signature(test_fn)
    kwargs = {
        name: pyfuncitem.funcargs[name]
        for name in sig.parameters
        if name in pyfuncitem.funcargs
    }
    asyncio.run(test_fn(**kwargs))
    return True
