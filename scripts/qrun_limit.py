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
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Template, meta
from ruamel.yaml import YAML
from qlib.workflow.cli import sys_config
from qlib.workflow.cli import task_train
import qlib
from qlib.config import C

try:
    from qe_prediction_store_client import maybe_upload_prediction_artifacts
except ModuleNotFoundError as exc:  # Backward-compatible for already-copied workspaces.
    if exc.name != "qe_prediction_store_client":
        raise
    maybe_upload_prediction_artifacts = None

try:
    from qe_runtime_resource import finish_resource_monitor, start_resource_monitor
except ModuleNotFoundError as exc:  # Backward-compatible for already-copied workspaces.
    if exc.name != "qe_runtime_resource":
        raise

    def start_resource_monitor():
        return None

    def finish_resource_monitor(*, status: str, error: str | None = None):
        return None


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


def _maybe_upload_prediction_store(recorder, recorder_ref, mode: str, experiment_name: str, config: dict):
    if maybe_upload_prediction_artifacts is None:
        print("[INFO] Prediction-store uploader helper not present; skipping upload")
        return None
    return maybe_upload_prediction_artifacts(
        recorder=recorder,
        recorder_ref=recorder_ref,
        experiment_name=experiment_name,
        mode=mode,
        config=config,
    )


def _task_train_with_gats_industry_provider(config: dict, experiment_name: str):
    task_config = (config or {}).get("task") if isinstance(config, dict) else None
    model_cfg = (task_config or {}).get("model") if isinstance(task_config, dict) else {}
    model_kwargs = model_cfg.get("kwargs") or {}
    if isinstance(model_kwargs, dict) and (
        model_kwargs.get("gats_adjacency_mode", "off") == "industry_bias"
        or model_kwargs.get("gats_industry_embedding") is True
        or str(model_kwargs.get("gats_industry_embedding", "off")).lower() == "on"
    ):
        from aistock_models.gats_industry_provider import inject_gats_industry_provider_if_needed

        inject_gats_industry_provider_if_needed(config, cwd=Path.cwd(), print_fn=print)
    return task_train(task_config, experiment_name=experiment_name)


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
    """从 Qlib bin 加载 SH000300(000300.SH) 日收益率 Series（BUG-625/方案A）。

    benchmark 直接取自回测所用的 Qlib daily bin —— provider 内已含 000300.SH 指数
    日线(features/000300.sh)，覆盖范围与价格同源、自动到数据末日。校验其完整覆盖
    backtest 的 start_time/end_time；缺失/不足即 fail-fast，绝不静默 disable/截断/填0。
    不再读取 benchmark_sh000300.parquet（该 parquet workaround 已废弃）。

    Returns:
        pd.Series(index=DatetimeIndex name=datetime, values=daily_return)
    """
    import pandas as pd
    from qlib.data import D

    start = end = None
    if config:
        bt = _find_backtest_config(config)
        if bt:
            start, end = bt.get("start_time"), bt.get("end_time")
    if not start or not end:
        raise RuntimeError(
            "[BENCH-FATAL bench_no_window] backtest start_time/end_time not found in config; "
            "cannot load benchmark from Qlib bin"
        )
    df = D.features(["000300.SH"], ["$close/Ref($close,1)-1"], start_time=str(start), end_time=str(end), freq="day")
    if df is None or df.empty:
        raise RuntimeError(
            f"[BENCH-FATAL bench_empty] Qlib bin has no 000300.SH benchmark in [{start},{end}]; "
            "ensure features/000300.sh is present in the daily bin (no silent benchmark disable)"
        )
    sr = df.iloc[:, 0].droplevel("instrument")
    sr.index = pd.to_datetime(sr.index)
    sr.index.name = "datetime"
    sr = sr.sort_index()
    re_ = pd.Timestamp(end).normalize()
    if sr.index.max() < re_:
        raise RuntimeError(
            f"[BENCH-FATAL bench_end_short] benchmark ends {sr.index.max().date()} < required_end {re_.date()}; "
            "refusing to use partial benchmark"
        )
    if sr.isna().any():
        # a leading NaN (no prior close at window start) is tolerable; any interior NaN is a gap
        if sr.iloc[1:].isna().any():
            raise RuntimeError("[BENCH-FATAL bench_internal_gap] benchmark has NaN returns within window")
        sr = sr.iloc[1:]
    print(
        f"[INFO] Loaded benchmark from Qlib bin (000300.SH): {len(sr)} days, "
        f"{sr.index.min().date()} ~ {sr.index.max().date()} (required {start} ~ {end})"
    )
    return sr


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


def apply_qe_fixed_seed(config: dict) -> int | None:
    """Apply QE fixed seed before qlib/model initialization."""

    runtime = config.get("qe_runtime") if isinstance(config, dict) else None
    if not isinstance(runtime, dict):
        return None
    seed_value = runtime.get("random_seed")
    if seed_value in (None, ""):
        return None
    seed = int(seed_value)
    os.environ.setdefault("PYTHONHASHSEED", str(seed))
    random.seed(seed)
    try:
        import numpy as np
        np.random.seed(seed)
    except Exception as exc:  # pragma: no cover - numpy is expected in QE env
        print(f"[WARN] QE fixed seed: numpy seed failed: {exc}")
    try:
        import torch
        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)
        if hasattr(torch.backends, "cudnn"):
            torch.backends.cudnn.deterministic = True
            torch.backends.cudnn.benchmark = False
        try:
            torch.use_deterministic_algorithms(True, warn_only=True)
        except TypeError:
            torch.use_deterministic_algorithms(True)
    except Exception as exc:
        print(f"[INFO] QE fixed seed: torch seeding skipped: {exc}")
    print(f"[INFO] QE fixed seed: {seed}")
    return seed


def main():
    yaml_path = sys.argv[1] if len(sys.argv) > 1 else "conf.yaml"

    start_resource_monitor()
    try:
        _run_main(yaml_path)
    except Exception as exc:
        finish_resource_monitor(status="failed", error=type(exc).__name__)
        raise
    else:
        finish_resource_monitor(status="completed")


def _run_main(yaml_path):

    # Jinja2 渲染 → YAML 解析
    rendered = render_yaml_template(yaml_path)
    yaml = YAML(typ="safe", pure=True)
    config = yaml.load(rendered)

    patch_backtest_config(config)
    apply_qe_fixed_seed(config)
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
    recorder = _task_train_with_gats_industry_provider(config, experiment_name=experiment_name)
    recorder_ref = _write_qe_current_recorder(recorder, "full", experiment_name)
    recorder.save_objects(config=config)
    _maybe_upload_prediction_store(recorder, recorder_ref, "full", experiment_name, config)


if __name__ == '__main__':
    main()
