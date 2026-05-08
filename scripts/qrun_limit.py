"""日线回测 runner：Jinja2 渲染 + benchmark 注入 + Qlib workflow。

与 qrun_limit_minute.py 保持一致的功能：
1. Jinja2 模板渲染（环境变量注入 conf.yaml）
2. limit_threshold list→tuple 转换（LT_TP_EXP 模式）
3. benchmark 注入（加载预计算 SH000300 日收益率）
4. 完整的 qlib.init + task_train 流程

唯一区别：不含分钟线内存优化 patch（日线无需按天重建 Exchange）。

用法：python qrun_limit.py conf.yaml
"""
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Template, meta
from ruamel.yaml import YAML
from qlib.workflow.cli import sys_config, task_train
import qlib
from qlib.config import C


RECORDER_REF_FILE = "qe_current_recorder.json"


def _write_qe_current_recorder(recorder, mode: str, experiment_name: str):
    """Persist the recorder created by this runner for read_exp_res.py."""
    info = getattr(recorder, "info", {}) or {}
    recorder_id = str(info.get("id") or info.get("recorder_id") or getattr(recorder, "id", "") or "")
    if not recorder_id:
        print(f"[WARN] QE recorder binding skipped: recorder id missing for mode={mode}")
        return None

    target_mlruns = Path.cwd() / "mlruns"
    payload = {
        "schema_version": 1,
        "recorder_id": recorder_id,
        "experiment_name": experiment_name,
        "experiment_id": str(info.get("experiment_id") or ""),
        "mode": mode,
        "runner": Path(__file__).name,
        "cwd": str(Path.cwd()),
        "mlflow_tracking_uri": os.environ.get("MLFLOW_TRACKING_URI", ""),
        "target_mlruns_realpath": str(target_mlruns.resolve()) if target_mlruns.exists() else "",
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    path = Path.cwd() / RECORDER_REF_FILE
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    print(f"[INFO] QE recorder binding written: {path} recorder_id={recorder_id} mode={mode}")
    return payload


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
        start, end = "2024-07-01", "2026-04-27"
        if config:
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

    # Run training + backtesting
    experiment_name = config.get("experiment_name", "workflow")
    recorder = task_train(config.get("task"), experiment_name=experiment_name)
    _write_qe_current_recorder(recorder, "full", experiment_name)
    recorder.save_objects(config=config)


if __name__ == '__main__':
    main()
