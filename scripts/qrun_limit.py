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
import re
import sys
import threading
import time
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
    from qe_sector_risk_overlay_artifacts import persist_sector_risk_overlay_artifacts
except ModuleNotFoundError as exc:
    if exc.name != "qe_sector_risk_overlay_artifacts":
        raise
    persist_sector_risk_overlay_artifacts = None

try:
    from qe_runtime_resource import (
        finish_phase_publisher,
        start_phase_publisher,
        task_train_with_phase_events,
        transition_runtime_phase,
    )
except ModuleNotFoundError as exc:  # Backward-compatible for already-copied workspaces.
    if exc.name != "qe_runtime_resource":
        raise

    def start_phase_publisher():
        return None

    def finish_phase_publisher(*, status: str, error: str | None = None):
        return None

    def task_train_with_phase_events(
        task_config,
        *,
        experiment_name: str,
        recorder_name: str | None = None,
        release_next_phase: str = "backtest",
    ):
        if os.environ.get("QE_PHASE_PIPELINE_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}:
            raise RuntimeError("QE_RUNTIME_PHASE_HELPER_MISSING")
        return task_train(
            task_config,
            experiment_name=experiment_name,
            recorder_name=recorder_name,
        )

    def transition_runtime_phase(phase: str, *, metadata: dict | None = None):
        if os.environ.get("QE_PHASE_PIPELINE_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}:
            raise RuntimeError("QE_RUNTIME_PHASE_HELPER_MISSING")


RECORDER_REF_FILE = "qe_current_recorder.json"
MLFLOW_EMPTY_METRIC_RETRY_ATTEMPTS_ENV = "QE_MLFLOW_EMPTY_METRIC_RETRY_ATTEMPTS"
MLFLOW_EMPTY_METRIC_RETRY_SLEEP_SEC_ENV = "QE_MLFLOW_EMPTY_METRIC_RETRY_SLEEP_SEC"
MLFLOW_ASYNC_DRAIN_TIMEOUT_SEC_ENV = "QE_MLFLOW_ASYNC_DRAIN_TIMEOUT_SEC"
DEFAULT_MLFLOW_EMPTY_METRIC_RETRY_ATTEMPTS = 3
DEFAULT_MLFLOW_EMPTY_METRIC_RETRY_SLEEP_SEC = 0.1
DEFAULT_MLFLOW_ASYNC_DRAIN_TIMEOUT_SEC = 30.0
_MLFLOW_EMPTY_METRIC_RE = re.compile(r"Metric '([^']+)' is malformed\. No data found\.")


class QEMlflowMetricReadRaceError(RuntimeError):
    """Raised after exhausting the specific MLflow empty metric read retry."""


class QEMlflowAsyncDrainError(RuntimeError):
    """Raised when queued Qlib MLflow writes cannot reach a read barrier."""


def _env_int(name: str, default_value: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None or str(raw_value).strip() == "":
        return default_value
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw_value!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")
    return value


def _env_float(name: str, default_value: float) -> float:
    raw_value = os.environ.get(name)
    if raw_value is None or str(raw_value).strip() == "":
        return default_value
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a float, got {raw_value!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")
    return value


def _empty_mlflow_metric_name(exc: BaseException) -> str | None:
    """Find only Qlib/MLflow's exact empty-metric error in a wrapped cause chain."""

    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ValueError) or type(current).__name__ == "LoadObjectError":
            match = _MLFLOW_EMPTY_METRIC_RE.search(str(current))
            if match is not None:
                return match.group(1)
        current = current.__cause__ or current.__context__
    return None


def _mlflow_metric_file_hint(recorder, metric_name: str) -> str:
    if recorder is None:
        return "<unknown-recorder>/metrics/" + metric_name
    try:
        local_dir = recorder.get_local_dir()
    except (AttributeError, RuntimeError, ValueError, OSError):
        local_dir = None
    if local_dir:
        return str(Path(local_dir) / "metrics" / metric_name)

    info = getattr(recorder, "info", {}) or {}
    exp_id = str(info.get("experiment_id") or getattr(recorder, "experiment_id", "") or "")
    run_id = str(info.get("id") or info.get("recorder_id") or getattr(recorder, "id", "") or "")
    tracking_uri = str(os.environ.get("MLFLOW_TRACKING_URI") or getattr(recorder, "uri", "") or "")
    tracking_uri = tracking_uri.removeprefix("file:")
    if tracking_uri and exp_id and run_id:
        return str(Path(tracking_uri) / exp_id / run_id / "metrics" / metric_name)
    if exp_id or run_id:
        return f"<unknown-mlruns>/{exp_id}/{run_id}/metrics/{metric_name}"
    return "<unknown-recorder>/metrics/" + metric_name


def _retry_empty_mlflow_metric_read(operation, *, recorder=None, context: str):
    attempts = _env_int(
        MLFLOW_EMPTY_METRIC_RETRY_ATTEMPTS_ENV,
        DEFAULT_MLFLOW_EMPTY_METRIC_RETRY_ATTEMPTS,
    )
    sleep_seconds = _env_float(
        MLFLOW_EMPTY_METRIC_RETRY_SLEEP_SEC_ENV,
        DEFAULT_MLFLOW_EMPTY_METRIC_RETRY_SLEEP_SEC,
    )
    for attempt in range(attempts + 1):
        try:
            return operation()
        except Exception as exc:
            metric_name = _empty_mlflow_metric_name(exc)
            if metric_name is None:
                raise
            metric_path = _mlflow_metric_file_hint(recorder, metric_name)
            if attempt >= attempts:
                raise QEMlflowMetricReadRaceError(
                    f"{context} failed after {attempts} retries because MLflow metric "
                    f"{metric_name!r} stayed empty/malformed; metric_path={metric_path}; original_error={exc}"
                ) from exc
            print(
                f"[WARN] {context}: transient MLflow empty metric read for metric={metric_name!r} "
                f"metric_path={metric_path}; retry {attempt + 1}/{attempts} after {sleep_seconds:.3f}s"
            )
            if sleep_seconds:
                time.sleep(sleep_seconds)


def _drain_mlflow_async_writes(recorder, *, context: str) -> bool:
    """Wait for all MLflow writes queued before this call without stopping Qlib's worker."""

    async_log = getattr(recorder, "async_log", None) if recorder is not None else None
    if async_log is None:
        return False
    if not callable(async_log):
        raise QEMlflowAsyncDrainError(
            f"{context}: recorder.async_log is not callable: {type(async_log).__name__}"
        )
    worker = getattr(async_log, "_t", None)
    if worker is not None and hasattr(worker, "is_alive") and not worker.is_alive():
        raise QEMlflowAsyncDrainError(f"{context}: Qlib MLflow async writer thread is not alive")

    timeout = _env_float(
        MLFLOW_ASYNC_DRAIN_TIMEOUT_SEC_ENV,
        DEFAULT_MLFLOW_ASYNC_DRAIN_TIMEOUT_SEC,
    )
    reached = threading.Event()
    try:
        async_log(reached.set)
    except Exception as exc:
        raise QEMlflowAsyncDrainError(
            f"{context}: failed to enqueue Qlib MLflow async write barrier"
        ) from exc
    if not reached.wait(timeout):
        raise QEMlflowAsyncDrainError(
            f"{context}: Qlib MLflow async write barrier timed out after {timeout:.3f}s"
        )
    return True


def _install_mlflow_metric_read_retry() -> bool:
    from qlib.workflow import record_temp

    record_temp_cls = getattr(record_temp, "RecordTemp", None)
    if record_temp_cls is None:
        raise RuntimeError("qlib.workflow.record_temp.RecordTemp is unavailable; cannot install QE metric retry")
    installed = False

    if not getattr(record_temp_cls.load, "_qe_mlflow_metric_retry", False):
        original_load = record_temp_cls.load

        def _load_with_mlflow_write_barrier(self, *args, **kwargs):
            recorder = getattr(self, "recorder", None)
            context = f"{type(self).__name__}.load"
            _drain_mlflow_async_writes(recorder, context=context)
            return _retry_empty_mlflow_metric_read(
                lambda: original_load(self, *args, **kwargs),
                recorder=recorder,
                context=context,
            )

        _load_with_mlflow_write_barrier._qe_mlflow_metric_retry = True
        _load_with_mlflow_write_barrier._qe_original_load = original_load
        record_temp_cls.load = _load_with_mlflow_write_barrier
        installed = True

    if not getattr(record_temp_cls.check, "_qe_mlflow_metric_retry", False):
        original_check = record_temp_cls.check

        def _check_with_mlflow_metric_retry(self, *args, **kwargs):
            recorder = getattr(self, "recorder", None)
            return _retry_empty_mlflow_metric_read(
                lambda: original_check(self, *args, **kwargs),
                recorder=recorder,
                context=f"{type(self).__name__}.check",
            )

        _check_with_mlflow_metric_retry._qe_mlflow_metric_retry = True
        _check_with_mlflow_metric_retry._qe_original_check = original_check
        record_temp_cls.check = _check_with_mlflow_metric_retry
        installed = True

    if installed:
        print("[INFO] Installed QE MLflow async write barrier and exact empty-metric read retry")
    return installed


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


def _persist_sector_risk_overlay(recorder, config):
    def contains_enabled(value):
        if isinstance(value, dict):
            if value.get("sector_risk_overlay_enabled") is True:
                return True
            return any(contains_enabled(item) for item in value.values())
        if isinstance(value, list):
            return any(contains_enabled(item) for item in value)
        return False

    enabled = contains_enabled(config)
    if persist_sector_risk_overlay_artifacts is None:
        if enabled:
            raise RuntimeError("QE_SECTOR_RISK_OVERLAY_ARTIFACT_HELPER_MISSING")
        return None
    return persist_sector_risk_overlay_artifacts(recorder, config)


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
    return task_train_with_phase_events(
        task_config,
        experiment_name=experiment_name,
        release_next_phase="backtest",
    )


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

    start_phase_publisher()
    try:
        _run_main(yaml_path)
    except Exception as exc:
        finish_phase_publisher(status="failed", error=type(exc).__name__)
        raise
    else:
        finish_phase_publisher(status="completed")


def _run_main(yaml_path):

    # Jinja2 渲染 → YAML 解析
    rendered = render_yaml_template(yaml_path)
    yaml = YAML(typ="safe", pure=True)
    config = yaml.load(rendered)

    # BUG-989 zero-DB data plane: rebuild qe_event_risk_policy.json from the
    # frozen qlib bin dataset (pinned by qe_frozen_build_spec.json) before
    # qlib init.  No database fallback; pin/identity mismatches fail closed.
    try:
        from qe_build_frozen_risk_policy import ensure_frozen_risk_policy_artifact
    except ImportError:
        ensure_frozen_risk_policy_artifact = None
    if ensure_frozen_risk_policy_artifact is not None:
        ensure_frozen_risk_policy_artifact(cwd=Path.cwd(), print_fn=print)
    elif (Path.cwd() / "qe_frozen_build_spec.json").exists():
        raise RuntimeError(
            "qe_frozen_build_spec.json present but qe_build_frozen_risk_policy.py "
            "helper is missing from the workspace"
        )

    # BUG-989 continuation: rebuild qe_suspend_filter.json from the frozen
    # suspend_d candidate dataset pinned in the same build spec.  No database
    # fallback; pin/identity/coverage mismatches fail closed.
    try:
        from qe_build_frozen_suspend_filter import ensure_frozen_suspend_filter_artifact
    except ImportError:
        ensure_frozen_suspend_filter_artifact = None
    if ensure_frozen_suspend_filter_artifact is not None:
        ensure_frozen_suspend_filter_artifact(cwd=Path.cwd(), print_fn=print)
    else:
        _spec_path = Path.cwd() / "qe_frozen_build_spec.json"
        if _spec_path.exists():
            try:
                _spec = json.loads(_spec_path.read_text(encoding="utf-8"))
            except Exception:
                _spec = {}
            if isinstance(_spec, dict) and isinstance(_spec.get("suspend"), dict):
                raise RuntimeError(
                    "qe_frozen_build_spec.json declares a suspend section but "
                    "qe_build_frozen_suspend_filter.py helper is missing from the workspace"
                )

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
    _install_mlflow_metric_read_retry()

    # 注入 benchmark Series（在 qlib init 之后，fallback 需要 D.features）
    benchmark_series = load_benchmark_series(config)
    inject_benchmark(config, benchmark_series)

    # Run training + backtesting
    experiment_name = config.get("experiment_name", "workflow")
    recorder = _task_train_with_gats_industry_provider(config, experiment_name=experiment_name)
    recorder_ref = _write_qe_current_recorder(recorder, "full", experiment_name)
    recorder.save_objects(config=config)
    _persist_sector_risk_overlay(recorder, config)
    _maybe_upload_prediction_store(recorder, recorder_ref, "full", experiment_name, config)
    transition_runtime_phase("finalize", metadata={"runner_mode": "full"})


if __name__ == '__main__':
    main()
