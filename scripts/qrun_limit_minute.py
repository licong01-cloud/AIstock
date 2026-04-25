"""分钟线回测专用 runner：benchmark 注入 + qlib 内存优化参数。

基于 qrun_limit.py，新增：
1. load_benchmark_series(): 加载预计算的 SH000300 日收益率，注入 backtest config
   解决 benchmark=None 导致所有收益/风险指标 NaN 的问题
2. --backtest-only: 跳过模型训练，从已有 mlruns 加载模型直接回测
   用于失败 Loop 恢复（训练已完成、回测失败）或策略对比分析
3. --train-only: 只训练模型+生成 pred.pkl，跳过回测（PortAnaRecord）
   用于多Alpha分布式架构：从节点只训练，主节点统一回测

分钟线内存优化（121GB → 200MB/天）已移入 qlib 源码:
  - exchange.py: Exchange.get_quote_from_qlib() 批次加载 + ensure_data_for_day()
  - backtest.py: collect_data_loop 在 generate_trade_decision 前调用 ensure_data_for_day
  环境变量: QLIB_MINUTE_FULL_LOAD=1 跳过批次加载, QLIB_MINUTE_BATCH_DAYS=20 控制批次大小

回退方式：将 workspace.py 的 entry 改回 "python qrun_limit.py" 即可使用原始全量加载模式。

用法：
  python qrun_limit_minute.py conf.yaml                  # 完整训练+回测
  python qrun_limit_minute.py conf.yaml --backtest-only   # 跳过训练，只回测
  python qrun_limit_minute.py conf.yaml --train-only      # 只训练，跳过回测
"""
import argparse
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
from qlib.model.trainer import task_train
try:
    from qlib.workflow.cli import sys_config
except ModuleNotFoundError:
    # qlib ≥ 0.9.7 removed cli.py; inline the function
    def sys_config(config, config_path):
        sc = config.get("sys", {})
        for p in ([sc["path"]] if isinstance(sc.get("path"), str) else list(sc.get("path", []))):
            sys.path.append(p)
        for p in ([sc["rel_path"]] if isinstance(sc.get("rel_path"), str) else list(sc.get("rel_path", []))):
            sys.path.append(str(Path(config_path).parent.resolve().absolute() / p))
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
                # v24 策略需要 $high/$low/$open/$up_limit_price/$down_limit_price/$prev_close
                # Exchange 默认不加载, 通过 subscribe_fields 注入
                v24_fields = ['$high', '$low', '$open',
                              '$up_limit_price', '$down_limit_price', '$prev_close']
                existing = set(val.get('subscribe_fields', []))
                missing = [f for f in v24_fields if f not in existing]
                if missing:
                    val.setdefault('subscribe_fields', [])
                    val['subscribe_fields'].extend(missing)
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



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("yaml_path", nargs="?", default="conf.yaml")
    parser.add_argument("--backtest-only", action="store_true",
                        help="跳过模型训练，从已有 mlruns 加载模型直接回测")
    parser.add_argument("--train-only", action="store_true",
                        help="只训练模型+生成 pred.pkl，跳过回测（多Alpha从节点模式）")
    args = parser.parse_args()

    # Jinja2 渲染 → YAML 解析
    rendered = render_yaml_template(args.yaml_path)
    yaml = YAML(typ="safe", pure=True)
    config = yaml.load(rendered)

    patch_backtest_config(config)
    sys_config(config, config_path=args.yaml_path)

    # 限制 qlib 并行度（必须在 qlib.init 之前！）
    # 默认 kernels=28 会导致 28 个子进程各自继承父进程内存
    C["kernels"] = 4
    print(f"[INFO] Limited qlib kernels to 4")

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

    experiment_name = config.get("experiment_name", "workflow")

    if args.backtest_only:
        # ── Backtest-only 模式：跳过训练，加载已有模型 ──
        print("[INFO] Backtest-only mode: skipping model training, loading existing model")
        _run_backtest_only(config, experiment_name)
    elif args.train_only:
        # ── Train-only 模式：训练+生成pred.pkl，跳过回测 ──
        print("[INFO] Train-only mode: training model + generating predictions, skipping backtest")
        _run_train_only(config, experiment_name)
    else:
        # ── 完整模式：训练 + 回测 ──
        recorder = task_train(config.get("task"), experiment_name=experiment_name)
        recorder.save_objects(config=config)


def _run_train_only(config: dict, experiment_name: str):
    """只训练模型 + 生成 pred.pkl，跳过回测（PortAnaRecord）。

    用于多Alpha分布式架构：从节点只负责训练，主节点收集 pred.pkl 后统一回测。
    保留 SignalRecord（生成 pred.pkl）和 SigAnaRecord（计算 IC 指标），
    移除 PortAnaRecord（回测）以避免需要 v24 执行策略和分钟线数据。
    """
    import copy

    task_config = copy.deepcopy(config.get("task"))

    # 过滤 records：移除 PortAnaRecord（回测），保留 SignalRecord + SigAnaRecord
    records = task_config.get("record", [])
    if isinstance(records, dict):
        records = [records]

    filtered_records = []
    for rec in records:
        rec_class = rec.get("class", "")
        # PortAnaRecord 是回测记录，train-only 模式下跳过
        if "PortAna" in rec_class:
            print(f"[INFO] Train-only: skipping {rec_class}")
            continue
        filtered_records.append(rec)

    task_config["record"] = filtered_records

    # 执行训练（task_train 内部会执行 filtered_records 中的 SignalRecord + SigAnaRecord）
    recorder = task_train(task_config, experiment_name=experiment_name)
    recorder.save_objects(config=config)
    print("[INFO] Train-only completed: model trained, pred.pkl generated")


def _run_backtest_only(config: dict, experiment_name: str):
    """从已有 mlruns 加载训练好的模型，只执行信号生成 + 回测。

    前提：之前的训练已完成（params.pkl 和 dataset 已保存到 mlruns）。
    """
    from qlib.utils import init_instance_by_config
    from qlib.workflow import R
    from qlib.data.dataset import Dataset
    from qlib.model.base import Model
    from qlib.model.trainer import fill_placeholder

    task_config = config.get("task")

    # 查找已有的 experiment 和 recorder
    exp = R.get_exp(experiment_name=experiment_name)
    recorders = exp.list_recorders()
    if not recorders:
        raise RuntimeError(
            f"Backtest-only: experiment '{experiment_name}' 中没有已有的 recorder。"
            f"需要先执行完整训练。"
        )

    # 从所有 recorders 中找到包含 params.pkl 的那个（跳过未完成的训练 run）
    recorder = None
    rec_id = None
    for rid in reversed(list(recorders.keys())):
        r = recorders[rid]
        try:
            obj = r.load_object("params.pkl")
            if obj is not None:
                recorder = r
                rec_id = rid
                model = obj
                break
        except Exception:
            continue

    if recorder is None:
        raise RuntimeError(
            "Backtest-only: 所有 recorder 中均未找到 params.pkl，模型训练未完成。"
            "无法跳过训练。"
        )
    print(f"[INFO] Loaded trained model from recorder {rec_id}")

    # 重建 dataset（从配置重新初始化，不需要训练数据）
    dataset: Dataset = init_instance_by_config(task_config["dataset"], accept_types=Dataset)
    dataset.config(dump_all=False, recursive=True)

    # 填充占位符并执行 records（SignalRecord + SigAnaRecord + PortAnaRecord）
    import copy
    task_config_filled = copy.deepcopy(task_config)
    placeholder_value = {"<MODEL>": model, "<DATASET>": dataset}
    task_config_filled = fill_placeholder(task_config_filled, placeholder_value)

    records = task_config_filled.get("record", [])
    if isinstance(records, dict):
        records = [records]

    # 创建新 recorder（不 resume 旧的），避免并行 loop 共用同一个 run 导致冲突
    with R.start(experiment_name=experiment_name):
        for record_config in records:
            r = init_instance_by_config(
                record_config,
                recorder=R.get_recorder(),
                default_module="qlib.workflow.record_temp",
                try_kwargs={"model": model, "dataset": dataset},
            )
            r.generate()
        R.get_recorder().save_objects(config=config, params_pkl=model)

    print("[INFO] Backtest-only completed successfully")


if __name__ == '__main__':
    main()
