"""分钟线回测专用 runner：按天重建 Exchange + benchmark 注入。

基于 qrun_limit.py，新增：
1. apply_minute_memory_patch(): monkey-patch NestedExecutor 按天重建 Exchange.quote
   内存 121GB → ~200MB/天，消除 swap thrashing
2. load_benchmark_series(): 加载预计算的 SH000300 日收益率，注入 backtest config
   解决 benchmark=None 导致所有收益/风险指标 NaN 的问题

回退方式：将 workspace.py 的 entry 改回 "python qrun_limit.py" 即可使用原始全量加载模式。

用法：python qrun_limit_minute.py conf.yaml
"""
import gc
import os
import sys
import warnings
from pathlib import Path

# 尾盘策略 14:30 前不出单，空 bar 触发 np.nanmean([]) 警告，无害
# 仅过滤 qlib.utils.index_data 模块中的这一条，不影响其他来源的 RuntimeWarning
warnings.filterwarnings("ignore", message="Mean of empty slice", category=RuntimeWarning, module=r"qlib\.utils\.index_data")

from jinja2 import Template, meta
from ruamel.yaml import YAML
from qlib.workflow.cli import sys_config, task_train
import qlib
from qlib.config import C


def render_yaml_template(yaml_path: str) -> str:
    """用环境变量渲染 Jinja2 模板，返回渲染后的 YAML 字符串。"""
    with open(yaml_path, "r") as f:
        content = f.read()
    template = Template(content)
    env = template.environment
    parsed = env.parse(content)
    variables = meta.find_undeclared_variables(parsed)
    context = {var: os.environ[var] for var in variables if var in os.environ}
    return template.render(context)


def patch_backtest_config(config: dict):
    """递归修补 backtest 配置：
    1. exchange_kwargs.limit_threshold: list → tuple (Qlib LT_TP_EXP 模式)
    2. benchmark: null 保留为 None（Qlib create_account_instance 已修补为传 None 而非 {}）
    """
    if isinstance(config, dict):
        for key, val in config.items():
            if key == 'exchange_kwargs' and isinstance(val, dict):
                lt = val.get('limit_threshold')
                if isinstance(lt, list):
                    val['limit_threshold'] = tuple(lt)
            elif key == 'backtest' and isinstance(val, dict):
                patch_backtest_config(val)
            else:
                patch_backtest_config(val)
    elif isinstance(config, list):
        for item in config:
            patch_backtest_config(item)


def load_benchmark_series(config=None):
    """加载预计算的 SH000300 日收益率 Series。

    优先读取本地 parquet 文件。如果不存在，尝试从 qlib 在线生成（需要 qlib 已初始化）。

    Returns:
        pd.Series(index=DatetimeIndex, values=daily_return) or None
    """
    import pandas as pd

    # 优先从 parquet 文件加载
    benchmark_path = Path(__file__).parent / "benchmark_sh000300.parquet"
    if benchmark_path.exists():
        df = pd.read_parquet(benchmark_path)
        sr = df["bench"]
        sr.index.name = "datetime"
        print(f"[INFO] Loaded benchmark from parquet: {len(sr)} days, {sr.index.min()} ~ {sr.index.max()}")
        return sr

    # Fallback: 从 qlib 在线生成（需要 qlib 已初始化且有 000300.sh 日线数据）
    try:
        from qlib.data import D
        # 从 config 提取回测区间
        start, end = "2024-07-01", "2026-03-10"
        if config:
            _extract_backtest_range(config, lambda s, e: None)  # just to find range
            # 简单递归查找 backtest.start_time / end_time
            bt = _find_backtest_config(config)
            if bt:
                start = str(bt.get("start_time", start))
                end = str(bt.get("end_time", end))
        df = D.features(["000300.sh"], ["$close/Ref($close,1)-1"], start_time=start, end_time=end, freq="day")
        if df.empty:
            print("[WARN] 000300.sh benchmark data empty, benchmark disabled")
            return None
        df.columns = ["bench"]
        sr = df["bench"].droplevel("instrument")
        sr.index.name = "datetime"
        # 缓存到本地
        sr.to_frame().to_parquet(benchmark_path)
        print(f"[INFO] Generated benchmark from qlib: {len(sr)} days, cached to {benchmark_path}")
        return sr
    except Exception as e:
        print(f"[WARN] Failed to generate benchmark: {e}, benchmark disabled")
        return None


def _find_backtest_config(config):
    """递归查找 config 中第一个 backtest 配置。"""
    if isinstance(config, dict):
        if 'backtest' in config and isinstance(config['backtest'], dict):
            return config['backtest']
        for val in config.values():
            result = _find_backtest_config(val)
            if result:
                return result
    elif isinstance(config, list):
        for item in config:
            result = _find_backtest_config(item)
            if result:
                return result
    return None


def inject_benchmark(config: dict, benchmark_series):
    """将 benchmark Series 注入到所有 backtest 配置中。"""
    if benchmark_series is None:
        return
    if isinstance(config, dict):
        for key, val in config.items():
            if key == 'backtest' and isinstance(val, dict):
                val['benchmark'] = benchmark_series
            else:
                inject_benchmark(val, benchmark_series)
    elif isinstance(config, list):
        for item in config:
            inject_benchmark(item, benchmark_series)


_BATCH_TRADING_DAYS = 20  # 每次加载的交易日数量（20天≈1交易月）


def _reload_exchange_for_day(exchange, trade_start_time, trade_end_time):
    """将 Exchange 的 quote 数据重建为从 trade_start_time 起 N 个交易日的分钟线数据。

    跳过条件：如果当前交易日在已加载的 [_loaded_start, _loaded_end] 范围内，则不重复加载。
    批量加载 _BATCH_TRADING_DAYS 个交易日，减少 D.features() 调用次数，
    408 天回测数据加载时间从 35.5min 降至 ~20.7min（-42%）。
    """
    import pandas as pd
    from qlib.data import D
    from qlib.backtest.high_performance_ds import NumpyQuote

    day_key = pd.Timestamp(trade_start_time).normalize()

    # 检查当前日是否在已加载的批次范围内
    loaded_start = getattr(exchange, '_loaded_start', None)
    loaded_end = getattr(exchange, '_loaded_end', None)
    if loaded_start is not None and loaded_start <= day_key <= loaded_end:
        return  # 当天数据已在批次中，跳过

    # 获取从当前日到回测结束的交易日历，确定批次边界
    backtest_end = getattr(exchange, 'end_time', trade_end_time)
    cal = D.calendar(start_time=trade_start_time, end_time=backtest_end, freq="day")
    if len(cal) == 0:
        return
    batch_end_day = cal[min(_BATCH_TRADING_DAYS - 1, len(cal) - 1)]
    batch_end_time = pd.Timestamp(batch_end_day).normalize() + pd.Timedelta(hours=23, minutes=59, seconds=59)

    # 清理 NumpyQuote 的 lru_cache
    if hasattr(exchange, 'quote') and exchange.quote is not None:
        if hasattr(exchange.quote, 'get_data') and hasattr(exchange.quote.get_data, 'cache_clear'):
            exchange.quote.get_data.cache_clear()
    old_quote = getattr(exchange, 'quote', None)
    old_df = getattr(exchange, 'quote_df', None)

    day_df = D.features(
        exchange.codes, exchange.all_fields,
        trade_start_time, batch_end_time,
        freq=exchange.freq, disk_cache=False,
    )
    day_df.columns = exchange.all_fields
    exchange.quote_df = day_df
    exchange._update_limit(exchange.limit_threshold)
    exchange.quote = NumpyQuote(exchange.quote_df, exchange.freq)
    exchange._loaded_start = day_key
    exchange._loaded_end = pd.Timestamp(batch_end_day).normalize()

    del old_quote, old_df, day_df
    gc.collect()


def apply_minute_memory_patch():
    """Monkey-patch NestedExecutor 按天重建 Exchange.quote，121GB → 200MB。

    Hook 点：
    1. Exchange.get_quote_from_qlib — 分钟线只加载第一天数据
    2. NestedExecutor._init_sub_trading — 每个交易日开始时重建 Exchange.quote
    3. TopkDropoutStrategy.generate_trade_decision — 策略生成订单前先更新 Exchange 数据
       （解决时序问题：策略在 _init_sub_trading 之前运行，需要当天数据判断 is_stock_tradable）

    通过环境变量 QLIB_MINUTE_FULL_LOAD=1 可跳过 patch（适用于 128GB+ 内存机器）。
    """
    if os.environ.get("QLIB_MINUTE_FULL_LOAD") == "1":
        print("[INFO] QLIB_MINUTE_FULL_LOAD=1, skipping minute memory patch")
        return

    from qlib.data import D
    from qlib.backtest.executor import NestedExecutor
    from qlib.backtest.exchange import Exchange
    from qlib.backtest.high_performance_ds import NumpyQuote

    # Patch 1: Exchange.get_quote_from_qlib — 分钟线只加载第一天
    _orig_get_quote = Exchange.get_quote_from_qlib

    def _patched_get_quote(self):
        if self.freq in ('1min', '5min'):
            import pandas as pd
            if len(self.codes) == 0:
                self.codes = D.instruments()
            # 首次加载 _BATCH_TRADING_DAYS 个交易日
            cal = D.calendar(start_time=self.start_time, end_time=self.end_time, freq="day")
            if len(cal) == 0:
                _orig_get_quote(self)
                return
            batch_end_day = cal[min(_BATCH_TRADING_DAYS - 1, len(cal) - 1)]
            batch_end_time = pd.Timestamp(batch_end_day).normalize() + pd.Timedelta(hours=23, minutes=59, seconds=59)
            self.quote_df = D.features(
                self.codes, self.all_fields,
                self.start_time, batch_end_time,
                freq=self.freq, disk_cache=False,
            )
            self.quote_df.columns = self.all_fields
            self.trade_w_adj_price = (
                (self.quote_df["$factor"].isna() & ~self.quote_df["$close"].isna()).any()
            )
            self._update_limit(self.limit_threshold)
            self._loaded_start = pd.Timestamp(self.start_time).normalize()
            self._loaded_end = pd.Timestamp(batch_end_day).normalize()
        else:
            _orig_get_quote(self)

    Exchange.get_quote_from_qlib = _patched_get_quote

    # Patch 2: NestedExecutor._init_sub_trading — 每天重建 quote（内层执行前）
    _orig_init_sub = NestedExecutor._init_sub_trading

    def _patched_init_sub(self, trade_decision):
        trade_start_time, trade_end_time = self.trade_calendar.get_step_time()
        exchange = self.trade_exchange
        if exchange.freq in ('1min', '5min'):
            _reload_exchange_for_day(exchange, trade_start_time, trade_end_time)
        _orig_init_sub(self, trade_decision)

    NestedExecutor._init_sub_trading = _patched_init_sub

    # Patch 3: TopkDropoutStrategy.generate_trade_decision — 策略生成订单前更新 Exchange
    from qlib.contrib.strategy.signal_strategy import TopkDropoutStrategy
    _orig_gen_trade = TopkDropoutStrategy.generate_trade_decision

    def _patched_gen_trade(self, execute_result=None):
        exchange = self.trade_exchange
        if exchange.freq in ('1min', '5min'):
            try:
                trade_start_time, trade_end_time = self.trade_calendar.get_step_time()
                _reload_exchange_for_day(exchange, trade_start_time, trade_end_time)
            except IndexError:
                pass  # 最后一步越界，跳过 reload
        return _orig_gen_trade(self, execute_result)

    TopkDropoutStrategy.generate_trade_decision = _patched_gen_trade

    print("[INFO] Minute memory patch applied: per-day Exchange rebuild enabled")


def main():
    yaml_path = sys.argv[1] if len(sys.argv) > 1 else "conf.yaml"

    # Jinja2 渲染 → YAML 解析
    rendered = render_yaml_template(yaml_path)
    yaml = YAML(typ="safe", pure=True)
    config = yaml.load(rendered)

    patch_backtest_config(config)
    sys_config(config, config_path=yaml_path)

    # Init qlib
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        tracking_uri = str(Path(os.getcwd()).resolve() / "mlruns")
        os.environ["MLFLOW_TRACKING_URI"] = tracking_uri
    exp_manager = C["exp_manager"]
    exp_manager["kwargs"]["uri"] = "file:" + tracking_uri
    qlib.init(**config.get("qlib_init"), exp_manager=exp_manager)

    # 注入 benchmark Series（在 qlib init 之后，fallback 需要 D.features）
    benchmark_series = load_benchmark_series(config)
    inject_benchmark(config, benchmark_series)

    # 应用分钟线内存优化 patch（必须在 qlib.init 之后）
    apply_minute_memory_patch()

    # Run training + backtesting
    experiment_name = config.get("experiment_name", "workflow")
    recorder = task_train(config.get("task"), experiment_name=experiment_name)
    recorder.save_objects(config=config)


if __name__ == '__main__':
    main()
