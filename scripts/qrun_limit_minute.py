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
  python qrun_limit_minute.py conf.yaml --pred-backtest combined_prediction.pkl  # 从已有预测直接回测
"""
# ruff: noqa: E402
import argparse
from contextlib import contextmanager
import os
import random
import re
import shutil
import stat
import sys
import threading
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

try:
    from qe_prediction_store_client import maybe_upload_prediction_artifacts
except ModuleNotFoundError as exc:  # Backward-compatible for already-copied workspaces.
    if exc.name != "qe_prediction_store_client":
        raise
    maybe_upload_prediction_artifacts = None

try:
    from qe_runtime_resource import (
        defer_runtime_phase_events,
        finalize_gpu_phase_lifecycle,
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

    def _phase_helper_required() -> bool:
        return os.environ.get("QE_PHASE_PIPELINE_ENABLED", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    def task_train_with_phase_events(
        task_config,
        *,
        experiment_name: str,
        recorder_name: str | None = None,
        release_next_phase: str = "backtest",
    ):
        if _phase_helper_required():
            raise RuntimeError("QE_RUNTIME_PHASE_HELPER_MISSING")
        return task_train(
            task_config,
            experiment_name=experiment_name,
            recorder_name=recorder_name,
        )

    def transition_runtime_phase(phase: str, *, metadata: dict | None = None):
        if _phase_helper_required():
            raise RuntimeError("QE_RUNTIME_PHASE_HELPER_MISSING")

    def finalize_gpu_phase_lifecycle(
        *,
        predict_error: BaseException | None = None,
        next_phase: str = "backtest",
    ) -> bool:
        if _phase_helper_required():
            raise RuntimeError("QE_RUNTIME_PHASE_HELPER_MISSING")
        return False

    @contextmanager
    def defer_runtime_phase_events(reason: str):
        if _phase_helper_required():
            raise RuntimeError("QE_RUNTIME_PHASE_HELPER_MISSING")
        yield



# === 分钟级交易记录功能（环境变量控制）===
import json
import pickle
from collections import defaultdict
import pandas as pd

# 环境变量：SAVE_MINUTE_TRADES=1 启用分钟级记录
SAVE_MINUTE_TRADES = os.environ.get('SAVE_MINUTE_TRADES', '0') == '1'

RECORDER_REF_FILE = "qe_current_recorder.json"
RECORDER_ISOLATION_FILE = "qe_recorder_isolation.json"
SOURCE_PARAMS_ENV = "QE_BACKTEST_SOURCE_PARAMS_DIR"
SOURCE_MLRUNS_ENV = "QE_BACKTEST_SOURCE_MLRUNS_DIR"
ALLOW_LEGACY_SOURCE_ENV = "QE_BACKTEST_ALLOW_LEGACY_MLRUNS_SOURCE"

ERR_TARGET_MLRUNS_SYMLINK = "QE_BACKTEST_TARGET_MLRUNS_IS_SYMLINK"
ERR_REALPATH_COLLISION = "QE_BACKTEST_SOURCE_TARGET_REALPATH_COLLISION"
ERR_RECORDER_NOT_ISOLATED = "QE_BACKTEST_RECORDER_NOT_ISOLATED"
ERR_SOURCE_PARAMS_MISSING = "QE_BACKTEST_SOURCE_PARAMS_MISSING"
PRED_BACKTEST_PICKLE_MAX_BYTES_ENV = "QE_PRED_BACKTEST_PICKLE_MAX_BYTES"
PARAMS_PICKLE_MAX_BYTES_ENV = "QE_BACKTEST_PARAMS_PICKLE_MAX_BYTES"
DEFAULT_PRED_BACKTEST_PICKLE_MAX_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_PARAMS_PICKLE_MAX_BYTES = 2 * 1024 * 1024 * 1024
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


def _pickle_max_bytes(env_name: str, default_bytes: int) -> int:
    raw_value = os.environ.get(env_name)
    if not raw_value:
        return default_bytes
    try:
        max_bytes = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{env_name} must be an integer byte limit, got {raw_value!r}") from exc
    if max_bytes <= 0:
        raise ValueError(f"{env_name} must be positive, got {max_bytes}")
    return max_bytes


def _load_pickle_with_size_bound(path: Path, *, max_bytes: int, purpose: str):
    size_bytes = path.stat().st_size
    if size_bytes > max_bytes:
        raise MemoryError(
            f"{purpose} is too large to load in one process: {path} has {size_bytes} bytes, "
            f"limit={max_bytes}. Increase the explicit QE pickle byte limit only after "
            "confirming the runner has enough memory."
        )
    with path.open("rb") as f:
        return pickle.Unpickler(f).load()


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


class BacktestRecorderIsolationError(RuntimeError):
    """Fail-fast error with a stable QE_BACKTEST_* code for orchestration."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(f"{code}: {message}")


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _is_reparse_or_symlink(path: Path) -> bool:
    if path.is_symlink():
        return True
    is_junction = getattr(path, "is_junction", None)
    if callable(is_junction) and is_junction():
        return True
    try:
        attrs = getattr(path.lstat(), "st_file_attributes", 0)
    except OSError:
        return False
    return bool(attrs & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _has_params_pkl(path: Path) -> bool:
    return path.exists() and any(path.glob("**/params.pkl"))


def _write_backtest_recorder_isolation(payload: dict) -> dict:
    path = Path.cwd() / RECORDER_ISOLATION_FILE
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    print(
        "[INFO] QE backtest recorder isolation passed: "
        f"source={payload.get('source_mlruns_realpath')} "
        f"target={payload.get('target_mlruns_realpath')}"
    )
    return payload


def _relocate_payload_mlruns_to_source_model(cwd: Path) -> None:
    """Move source mlruns payload out of target mlruns before qlib.init."""
    if os.environ.get(SOURCE_PARAMS_ENV) or os.environ.get(SOURCE_MLRUNS_ENV):
        return

    extracted_mlruns = cwd / "mlruns"
    source_mlruns = cwd / "source_model" / "mlruns"
    if not _has_params_pkl(extracted_mlruns):
        return
    if source_mlruns.exists():
        raise BacktestRecorderIsolationError(
            ERR_RECORDER_NOT_ISOLATED,
            f"source_model/mlruns already exists while target mlruns contains params.pkl: {source_mlruns}",
        )

    source_mlruns.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(extracted_mlruns), str(source_mlruns))
    os.environ[SOURCE_MLRUNS_ENV] = str(source_mlruns)
    os.environ[SOURCE_PARAMS_ENV] = str(source_mlruns)
    print(f"[INFO] Backtest-only source mlruns relocated to read-only source_model: {source_mlruns}")


def _find_backtest_source_params_dir(cwd: Path) -> Path | None:
    candidates = []
    for env_name in (SOURCE_PARAMS_ENV, SOURCE_MLRUNS_ENV):
        env_value = os.environ.get(env_name)
        if env_value:
            candidates.append(Path(env_value))
    candidates.extend([cwd / "source_model", cwd / "source_model" / "mlruns"])

    for candidate in candidates:
        if _has_params_pkl(candidate):
            return candidate

    legacy_mlruns = cwd / "mlruns"
    if os.environ.get(ALLOW_LEGACY_SOURCE_ENV) == "1" and _has_params_pkl(legacy_mlruns):
        return legacy_mlruns
    return None


def _prepare_backtest_recorder_isolation(
    experiment_name: str,
    source_ref: dict | None = None,
) -> dict:
    """Ensure backtest-only source reads and target recorder writes are separated."""
    cwd = Path.cwd().resolve()
    target_mlruns = cwd / "mlruns"

    _relocate_payload_mlruns_to_source_model(cwd)
    if _is_reparse_or_symlink(target_mlruns):
        raise BacktestRecorderIsolationError(
            ERR_TARGET_MLRUNS_SYMLINK,
            f"target mlruns must be a loop-local directory, not a symlink/reparse point: {target_mlruns}",
        )

    source_params_dir = _find_backtest_source_params_dir(cwd)
    if source_params_dir is None:
        raise BacktestRecorderIsolationError(
            ERR_SOURCE_PARAMS_MISSING,
            "backtest-only requires readable source params.pkl under source_model or "
            f"{SOURCE_PARAMS_ENV}; refusing to read source model from target mlruns",
        )

    target_mlruns.mkdir(parents=True, exist_ok=True)
    if _is_reparse_or_symlink(target_mlruns):
        raise BacktestRecorderIsolationError(
            ERR_TARGET_MLRUNS_SYMLINK,
            f"target mlruns must be a loop-local directory, not a symlink/reparse point: {target_mlruns}",
        )

    source_mlruns = Path(os.environ.get(SOURCE_MLRUNS_ENV) or source_params_dir)
    source_real = source_mlruns.resolve()
    target_real = target_mlruns.resolve()

    if source_real == target_real:
        raise BacktestRecorderIsolationError(
            ERR_REALPATH_COLLISION,
            f"source and target mlruns resolve to the same path: {source_real}",
        )
    if _is_relative_to(target_real, source_real):
        raise BacktestRecorderIsolationError(
            ERR_RECORDER_NOT_ISOLATED,
            f"target mlruns is inside source mlruns: source={source_real} target={target_real}",
        )
    if _is_relative_to(source_real, target_real):
        raise BacktestRecorderIsolationError(
            ERR_RECORDER_NOT_ISOLATED,
            f"source mlruns is inside target mlruns: source={source_real} target={target_real}",
        )

    os.environ["MLFLOW_TRACKING_URI"] = str(target_real)
    os.environ[SOURCE_PARAMS_ENV] = str(source_params_dir.resolve())
    os.environ[SOURCE_MLRUNS_ENV] = str(source_real)

    ref = source_ref or _read_backtest_source_ref()
    payload = {
        "schema_version": "qe_backtest_recorder_isolation_v1",
        "mode": "backtest_only",
        "experiment_name": experiment_name,
        "source_task_id": ref.get("source_task_id"),
        "source_loop_id": ref.get("source_loop") or ref.get("source_loop_id"),
        "source_recorder_id": ref.get("source_recorder_id"),
        "source_params_dir_realpath": str(source_params_dir.resolve()),
        "source_mlruns_realpath": str(source_real),
        "target_task_id": ref.get("target_task_id"),
        "target_loop_id": ref.get("target_loop_id"),
        "target_mlruns_realpath": str(target_real),
        "target_mlruns_is_symlink": False,
        "parallel_group_id": ref.get("parallel_group_id"),
        "recorder_isolation_status": "passed",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    return _write_backtest_recorder_isolation(payload)


def _read_backtest_recorder_isolation_manifest() -> dict:
    path = Path.cwd() / RECORDER_ISOLATION_FILE
    if not path.exists():
        raise BacktestRecorderIsolationError(
            ERR_RECORDER_NOT_ISOLATED,
            f"missing recorder isolation manifest before recorder creation: {path}",
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise BacktestRecorderIsolationError(
            ERR_RECORDER_NOT_ISOLATED,
            f"recorder isolation manifest is not valid JSON: {path}",
        ) from exc
    if not isinstance(payload, dict) or payload.get("recorder_isolation_status") != "passed":
        raise BacktestRecorderIsolationError(
            ERR_RECORDER_NOT_ISOLATED,
            f"recorder isolation manifest did not pass: {path}",
        )
    return payload


def _validate_backtest_recorder_isolation_manifest(payload: dict | None = None) -> dict:
    payload = payload or _read_backtest_recorder_isolation_manifest()
    cwd = Path.cwd().resolve()
    target_mlruns = cwd / "mlruns"
    if _is_reparse_or_symlink(target_mlruns):
        raise BacktestRecorderIsolationError(
            ERR_TARGET_MLRUNS_SYMLINK,
            f"target mlruns changed to a symlink/reparse point after isolation gate: {target_mlruns}",
        )
    expected_target_raw = str(payload.get("target_mlruns_realpath") or "")
    expected_source_raw = str(payload.get("source_mlruns_realpath") or "")
    if not expected_target_raw or not expected_source_raw:
        raise BacktestRecorderIsolationError(
            ERR_RECORDER_NOT_ISOLATED,
            "recorder isolation manifest is missing source/target realpaths",
        )
    expected_target = Path(expected_target_raw)
    expected_source = Path(expected_source_raw)
    target_real = target_mlruns.resolve()
    source_real = expected_source.resolve()
    if target_real != expected_target.resolve():
        raise BacktestRecorderIsolationError(
            ERR_RECORDER_NOT_ISOLATED,
            f"target mlruns realpath changed after isolation gate: expected={expected_target} actual={target_real}",
        )
    if source_real == target_real:
        raise BacktestRecorderIsolationError(
            ERR_REALPATH_COLLISION,
            f"source and target mlruns resolve to the same path: {source_real}",
        )
    if _is_relative_to(target_real, source_real) or _is_relative_to(source_real, target_real):
        raise BacktestRecorderIsolationError(
            ERR_RECORDER_NOT_ISOLATED,
            f"source and target mlruns are not physically isolated: source={source_real} target={target_real}",
        )
    return payload


def _read_backtest_source_ref() -> dict:
    for path in (Path.cwd() / "qe_backtest_source_ref.json", Path.cwd() / "source_model" / "source_recorder_ref.json"):
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                print(f"[WARN] Failed to parse source ref {path}: {exc}")
    return {}


def save_minute_trades_from_recorder(recorder, output_dir='.'):
    """从recorder中提取并保存分钟级交易记录
    
    环境变量控制：
        SAVE_MINUTE_TRADES=1  启用分钟级记录保存（默认关闭）
        
    保存文件：
        minute_trades.json - 分钟级交易详情
        minute_summary.csv - 汇总统计（前30分钟vs后210分钟）
    """
    if not SAVE_MINUTE_TRADES:
        return  # 默认行为：不保存，无性能影响
    
    try:
        print('[INFO] SAVE_MINUTE_TRADES=1: Extracting minute-level trades...')
        
        # 尝试加载positions
        try:
            positions = recorder.load_object('portfolio_analysis/positions.pkl')
        except Exception as e:
            print(f'[WARN] No positions.pkl found: {e}')
            return
        
        if positions is None or (hasattr(positions, 'empty') and positions.empty):
            print('[WARN] Positions data is empty')
            return
        
        print(f'[INFO] Positions shape: {positions.shape}')
        print(f'[INFO] Positions index: {positions.index.names}')
        
        # 提取分钟级数据
        minute_data = defaultdict(lambda: defaultdict(list))
        
        if isinstance(positions.index, pd.MultiIndex):
            dates = positions.index.get_level_values(0).unique()
            
            for date in dates:
                date_str = str(date)[:10]
                try:
                    day_positions = positions.loc[date]
                    
                    # 检查是否是分钟级数据
                    if isinstance(day_positions.index, pd.DatetimeIndex):
                        for timestamp in day_positions.index:
                            if hasattr(timestamp, 'hour'):
                                minute = timestamp.hour * 60 + timestamp.minute - 9 * 60 - 30
                                if 0 <= minute < 240:
                                    row = day_positions.loc[timestamp]
                                    for stock in row.index:
                                        pos = row[stock]
                                        if abs(pos) > 1e-6:
                                            minute_data[date_str][stock].append({
                                                'minute': int(minute),
                                                'position': float(pos),
                                                'timestamp': str(timestamp)
                                            })
                except Exception as e:
                    print(f'[WARN] Failed to process {date}: {e}')
        
        if not minute_data:
            print('[WARN] No minute-level data extracted')
            return
        
        # 保存JSON
        output_path = Path(output_dir) / 'minute_trades.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(dict(minute_data), f, indent=2, ensure_ascii=False)
        
        print(f'[INFO] ✓ Minute trades saved: {output_path}')
        
        # 生成汇总统计
        summary_data = []
        for date, stocks in minute_data.items():
            for stock, trades in stocks.items():
                if not trades:
                    continue
                
                early = [t for t in trades if t['minute'] < 30]
                late = [t for t in trades if t['minute'] >= 30]
                
                early_qty = sum(abs(t['position']) for t in early)
                late_qty = sum(abs(t['position']) for t in late)
                total = early_qty + late_qty
                
                if total > 0:
                    summary_data.append({
                        'date': date,
                        'stock': stock,
                        'early_30min_qty': early_qty,
                        'late_210min_qty': late_qty,
                        'early_pct': early_qty / total * 100,
                        'late_pct': late_qty / total * 100,
                        'total_minutes': len(trades)
                    })
        
        if summary_data:
            df = pd.DataFrame(summary_data)
            summary_path = Path(output_dir) / 'minute_summary.csv'
            df.to_csv(summary_path, index=False)
            
            print(f'[INFO] ✓ Summary saved: {summary_path}')
            print(f'[INFO]   Samples: {len(df)}')
            print(f'[INFO]   Avg early 30min: {df["early_pct"].mean():.2f}%')
            print('[INFO]   v25 target: 88.79%')
            
            if df['early_pct'].mean() > 70:
                print('[INFO]   ✅ Matches v25 high-weight pattern')
            else:
                print('[INFO]   ⚠️  Lower than expected v25 pattern')
        
    except Exception as e:
        print(f'[ERROR] Failed to save minute trades: {e}')
        import traceback
        traceback.print_exc()

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
                _maybe_enable_board_lot_exchange(val)
                lt = val.get('limit_threshold')
                if isinstance(lt, list):
                    val['limit_threshold'] = tuple(lt)
                # V24/V25 策略需要额外分钟字段；V25 必须用 $factor
                # 将复权 OHLC 转回 raw price 后再和 raw limit/pre_close 比较。
                # Exchange 默认不加载, 通过 subscribe_fields 注入
                v24_fields = ['$high', '$low', '$open',
                              '$up_limit_price', '$down_limit_price', '$prev_close', '$factor']
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
    """从 Qlib bin 加载 SH000300(000300.SH) 日收益率 Series（BUG-625/方案A，与日线路径一致）。

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


def _get_seed_ensemble_config(config: dict) -> dict | None:
    runtime = config.get("qe_runtime") if isinstance(config, dict) else None
    if not isinstance(runtime, dict):
        return None
    ensemble = runtime.get("ensemble")
    if not isinstance(ensemble, dict) or not ensemble.get("enabled"):
        return None
    seeds = ensemble.get("seeds")
    if not isinstance(seeds, list) or not seeds:
        raise ValueError("qe_runtime.ensemble.seeds must be a non-empty list")
    normalized_seeds = [int(seed) for seed in seeds]
    if len(set(normalized_seeds)) != len(normalized_seeds):
        raise ValueError("qe_runtime.ensemble.seeds must not contain duplicate seeds")
    normalized = {
        "enabled": True,
        "seeds": normalized_seeds,
        "level": str(ensemble.get("level") or "score"),
        "agg": str(ensemble.get("agg") or "mean"),
    }
    if normalized["level"] not in {"score", "portfolio"}:
        raise ValueError("qe_runtime.ensemble.level must be score or portfolio")
    if normalized["agg"] not in {"mean", "rank_mean", "median"}:
        raise ValueError("qe_runtime.ensemble.agg must be mean, rank_mean, or median")
    return normalized


def _set_config_seed(config: dict, seed: int) -> None:
    runtime = config.setdefault("qe_runtime", {})
    runtime["seed_policy"] = "fixed"
    runtime["random_seed"] = int(seed)
    model_cfg = ((config.get("task") or {}).get("model") or {})
    model_kwargs = model_cfg.setdefault("kwargs", {})
    if not isinstance(model_kwargs, dict):
        return
    model_class = str(model_cfg.get("class") or "")
    if model_class in {"LGBModel", "AIStockXGBModel", "XGBModel"}:
        model_kwargs["seed"] = int(seed)
        model_kwargs["random_state"] = int(seed)
    elif model_class == "CatBoostModel":
        model_kwargs["random_seed"] = int(seed)
    elif model_class in {"TabPFNModel", "LambdaRankModel"}:
        model_kwargs["random_state"] = int(seed)
    else:
        for key in ("seed", "random_seed", "random_state"):
            if key in model_kwargs:
                model_kwargs[key] = int(seed)


def _prediction_score_series(pred_obj, *, seed: int) -> pd.Series:
    if isinstance(pred_obj, pd.Series):
        pred_df = pred_obj.to_frame("score")
    elif isinstance(pred_obj, pd.DataFrame):
        pred_df = pred_obj.copy()
    else:
        raise TypeError(f"seed {seed}: pred.pkl must be pandas Series/DataFrame, got {type(pred_obj).__name__}")
    if not isinstance(pred_df.index, pd.MultiIndex):
        raise ValueError(f"seed {seed}: pred.pkl index must be MultiIndex(datetime, instrument)")
    if "score" not in pred_df.columns:
        if pred_df.shape[1] != 1:
            raise ValueError(f"seed {seed}: pred.pkl missing score column and has {pred_df.shape[1]} columns")
        pred_df = pred_df.rename(columns={pred_df.columns[0]: "score"})
    score = pd.to_numeric(pred_df["score"], errors="coerce").dropna()
    if score.empty:
        raise ValueError(f"seed {seed}: pred.pkl contains no numeric scores")
    return score.sort_index()


def _aggregate_seed_predictions(seed_scores: list[tuple[int, pd.Series]], agg: str) -> pd.DataFrame:
    columns = [series.rename(f"seed_{seed}") for seed, series in seed_scores]
    score_matrix = pd.concat(columns, axis=1, join="inner").sort_index()
    if score_matrix.empty:
        raise ValueError("seed ensemble prediction intersection is empty")
    if score_matrix.shape[1] != len(seed_scores):
        raise ValueError(
            "seed ensemble prediction matrix lost seed columns during alignment: "
            f"expected={len(seed_scores)} actual={score_matrix.shape[1]}"
        )
    if agg == "mean":
        combined = score_matrix.mean(axis=1)
    elif agg == "median":
        combined = score_matrix.median(axis=1)
    elif agg == "rank_mean":
        ranked = score_matrix.groupby(level=0, group_keys=False).rank(
            method="average",
            ascending=True,
            pct=True,
        )
        combined = ranked.mean(axis=1)
    else:
        raise ValueError(f"unsupported seed ensemble agg: {agg}")
    combined = combined.sort_index()
    combined.name = "score"
    return combined.to_frame("score")


def _position_raw(position_obj: Any, *, seed: int | None = None, date: Any | None = None) -> dict:
    if isinstance(position_obj, dict):
        return position_obj
    raw = getattr(position_obj, "position", None)
    if isinstance(raw, dict):
        return raw
    location = ""
    if seed is not None:
        location += f" seed={seed}"
    if date is not None:
        location += f" date={date}"
    raise TypeError(
        "portfolio seed ensemble expects qlib Position/dict snapshots; "
        f"got {type(position_obj).__name__}{location}"
    )


def _numeric_value(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, dict):
        value = value.get("amount", default)
    if value is None:
        return default
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"portfolio seed ensemble expected numeric value, got {value!r}") from exc
    return out if pd.notna(out) else default


def _position_account_value(raw_position: dict) -> float:
    account = _numeric_value(raw_position.get("now_account_value"), 0.0)
    if account > 0:
        return account
    cash = _numeric_value(raw_position.get("cash"), 0.0)
    stock_value = 0.0
    for instrument, info in raw_position.items():
        if instrument in {"cash", "now_account_value"} or not isinstance(info, dict):
            continue
        stock_value += _numeric_value(info.get("amount"), 0.0) * _numeric_value(info.get("price"), 0.0)
    return cash + stock_value


def _normalize_daily_frame(obj: Any, *, seed: int, artifact_name: str) -> pd.DataFrame:
    if isinstance(obj, pd.Series):
        frame = obj.to_frame(obj.name or "value")
    elif isinstance(obj, pd.DataFrame):
        frame = obj.copy()
    else:
        raise TypeError(f"seed {seed}: {artifact_name} must be pandas Series/DataFrame, got {type(obj).__name__}")
    if isinstance(frame.index, pd.MultiIndex):
        raise ValueError(f"seed {seed}: {artifact_name} must use a daily DatetimeIndex, got MultiIndex")
    try:
        frame.index = pd.to_datetime(frame.index)
    except Exception as exc:
        raise ValueError(f"seed {seed}: {artifact_name} index cannot be parsed as datetime") from exc
    if frame.index.has_duplicates:
        frame = frame.groupby(level=0).last()
    return frame.sort_index()


def _normalize_position_dates(position_obj: Any, *, seed: int) -> dict[pd.Timestamp, Any]:
    if not isinstance(position_obj, dict) or not position_obj:
        raise ValueError(f"seed {seed}: positions_normal_1day.pkl must be a non-empty dict")
    out: dict[pd.Timestamp, Any] = {}
    for raw_dt, snapshot in position_obj.items():
        out[pd.Timestamp(raw_dt)] = snapshot
    return dict(sorted(out.items()))


def _mean_daily_frames(seed_frames: list[tuple[int, pd.DataFrame]], common_dates: list[pd.Timestamp]) -> pd.DataFrame:
    if not seed_frames:
        raise ValueError("portfolio seed ensemble requires at least one daily frame")
    combined = pd.DataFrame(index=pd.DatetimeIndex(common_dates))
    common_index = combined.index
    all_columns: list[Any] = []
    for _, frame in seed_frames:
        for col in frame.columns:
            if col not in all_columns:
                all_columns.append(col)
    for col in all_columns:
        total = pd.Series(0.0, index=common_index)
        count = pd.Series(0.0, index=common_index)
        for seed, frame in seed_frames:
            if col not in frame.columns:
                continue
            series = pd.to_numeric(frame.reindex(common_index)[col], errors="coerce")
            valid = series.notna()
            total = total.add(series.fillna(0.0), fill_value=0.0)
            count = count.add(valid.astype(float), fill_value=0.0)
        if count.gt(0).any():
            combined[col] = total / count.where(count > 0)
    return combined


def _aggregate_seed_positions(
    seed_positions: list[tuple[int, dict[pd.Timestamp, Any]]],
    seed_reports: list[tuple[int, pd.DataFrame]],
) -> dict[pd.Timestamp, dict[str, Any]]:
    if not seed_positions:
        raise ValueError("portfolio seed ensemble requires positions from at least one seed")
    position_date_sets = [set(positions) for _, positions in seed_positions]
    report_date_sets = [set(report.index) for _, report in seed_reports]
    common_dates = sorted(set.intersection(*(position_date_sets + report_date_sets)))
    if not common_dates:
        raise ValueError("portfolio seed ensemble daily position/report intersection is empty")

    seed_count = len(seed_positions)
    merged: dict[pd.Timestamp, dict[str, Any]] = {}
    for date in common_dates:
        stock_weight_sum: dict[str, float] = defaultdict(float)
        stock_price_sum: dict[str, float] = defaultdict(float)
        stock_price_count: dict[str, int] = defaultdict(int)
        cash_weight_sum = 0.0
        account_values: list[float] = []

        for seed, positions in seed_positions:
            raw = _position_raw(positions[date], seed=seed, date=date)
            account = _position_account_value(raw)
            if account <= 0:
                raise ValueError(f"seed {seed}: invalid non-positive account value for portfolio ensemble on {date}")
            account_values.append(account)
            cash_weight_sum += _numeric_value(raw.get("cash"), 0.0) / account

            for instrument, info in raw.items():
                if instrument in {"cash", "now_account_value"} or not isinstance(info, dict):
                    continue
                amount = _numeric_value(info.get("amount"), 0.0)
                price = _numeric_value(info.get("price"), 0.0)
                stock_value = amount * price
                if abs(amount) > 1e-12 and price <= 0:
                    raise ValueError(
                        f"seed {seed}: invalid non-positive price for held instrument "
                        f"{instrument} on {date}"
                    )
                if abs(stock_value) <= 1e-12:
                    continue
                stock_weight_sum[str(instrument)] += stock_value / account
                stock_price_sum[str(instrument)] += price
                stock_price_count[str(instrument)] += 1

        account = float(sum(account_values) / len(account_values))
        averaged_weights = {instrument: weight / seed_count for instrument, weight in stock_weight_sum.items()}
        stock_weight_total = sum(averaged_weights.values())
        cash_weight = cash_weight_sum / seed_count
        weight_total = cash_weight + stock_weight_total
        if abs(weight_total - 1.0) > 1e-4:
            raise ValueError(
                "portfolio seed ensemble merged weights do not reconcile on "
                f"{date}: cash_weight={cash_weight:.8f} stock_weight={stock_weight_total:.8f}"
            )
        if abs(weight_total - 1.0) > 1e-8:
            cash_weight = max(0.0, 1.0 - stock_weight_total)

        snapshot: dict[str, Any] = {
            "cash": float(cash_weight * account),
            "now_account_value": account,
        }
        for instrument in sorted(averaged_weights):
            price = stock_price_sum[instrument] / max(stock_price_count[instrument], 1)
            value = averaged_weights[instrument] * account
            snapshot[instrument] = {
                "amount": float(value / price) if price > 0 else 0.0,
                "price": float(price),
                "weight": float(averaged_weights[instrument]),
            }
        merged[date] = snapshot

    return merged


def _aggregate_seed_reports(
    seed_reports: list[tuple[int, pd.DataFrame]],
    merged_positions: dict[pd.Timestamp, dict[str, Any]],
) -> pd.DataFrame:
    common_dates = sorted(merged_positions)
    report = _mean_daily_frames(seed_reports, common_dates)
    nav = pd.Series(
        {date: _numeric_value(snapshot.get("now_account_value"), 0.0) for date, snapshot in merged_positions.items()},
        dtype=float,
    ).sort_index()
    cash = pd.Series(
        {date: _numeric_value(snapshot.get("cash"), 0.0) for date, snapshot in merged_positions.items()},
        dtype=float,
    ).sort_index()
    report["account"] = nav
    report["cash"] = cash
    report["value"] = nav - cash
    report["return"] = nav.pct_change().replace([float("inf"), float("-inf")], 0.0).fillna(0.0)
    if "bench" in report.columns:
        bench = pd.to_numeric(report["bench"], errors="coerce").fillna(0.0)
        excess = report["return"] - bench
        report["excess_return_without_cost"] = excess
        report["excess_return_with_cost"] = excess
    return report


def _aggregate_seed_indicators(
    seed_indicators: list[tuple[int, pd.DataFrame]],
    common_dates: list[pd.Timestamp],
) -> pd.DataFrame | None:
    if not seed_indicators:
        return None
    return _mean_daily_frames(seed_indicators, common_dates)


def _series_metrics(series: pd.Series, *, prefix: str, trading_days: int) -> dict[str, float]:
    clean = pd.to_numeric(series, errors="coerce").fillna(0.0)
    if clean.empty:
        return {}
    cumulative = (1.0 + clean).cumprod()
    final_nav = float(cumulative.iloc[-1])
    annualized = final_nav ** (252.0 / max(trading_days, 1)) - 1.0 if final_nav > 0 else -1.0
    std = float(clean.std())
    mean = float(clean.mean())
    ir = float(mean / std * (252.0 ** 0.5)) if std > 0 else 0.0
    drawdown = cumulative / cumulative.cummax() - 1.0
    return {
        f"{prefix}.annualized_return": float(annualized),
        f"{prefix}.information_ratio": ir,
        f"{prefix}.max_drawdown": float(drawdown.min()),
        f"{prefix}.mean": mean,
        f"{prefix}.std": std,
    }


def _build_portfolio_ensemble_metrics(report: pd.DataFrame) -> dict[str, float]:
    if "return" not in report.columns:
        return {}
    ret = pd.to_numeric(report["return"], errors="coerce").fillna(0.0)
    trading_days = int(len(ret))
    metrics = _series_metrics(ret, prefix="1day.return", trading_days=trading_days)
    metrics["n_trading_days"] = float(trading_days)
    cumulative = (1.0 + ret).cumprod()
    if not cumulative.empty:
        metrics["final_nav"] = float(cumulative.iloc[-1])
        metrics["cagr"] = float(cumulative.iloc[-1] ** (252.0 / max(trading_days, 1)) - 1.0)
    if "bench" in report.columns:
        bench = pd.to_numeric(report["bench"], errors="coerce").fillna(0.0)
    else:
        bench = pd.Series(0.0, index=report.index)
    excess = ret - bench
    metrics.update(_series_metrics(excess, prefix="1day.excess_return_without_cost", trading_days=trading_days))
    metrics.update(_series_metrics(excess, prefix="1day.excess_return_with_cost", trading_days=trading_days))
    return {key: value for key, value in metrics.items() if pd.notna(value)}


def _is_missing_recorder_artifact_error(exc: BaseException) -> bool:
    if isinstance(exc, (FileNotFoundError, KeyError)):
        return True
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "not found",
            "does not exist",
            "doesn't exist",
            "no such file",
            "not exist",
            "missing artifact",
        )
    )


def _load_recorder_object(recorder, name: str, *, seed: int, required: bool = True):
    try:
        return recorder.load_object(name)
    except Exception as exc:
        if not required and _is_missing_recorder_artifact_error(exc):
            print(f"[WARN] Seed ensemble: optional artifact missing for seed={seed}: {name}: {exc}")
            return None
        if required:
            raise RuntimeError(f"seed {seed}: required recorder artifact missing: {name}") from exc
        raise RuntimeError(f"seed {seed}: optional recorder artifact load failed: {name}") from exc


def _load_test_label_from_config(config: dict):
    from qlib.data.dataset import Dataset, DatasetH
    from qlib.data.dataset.handler import DataHandlerLP
    from qlib.utils import class_casting, init_instance_by_config

    task_config = config.get("task") or {}
    dataset: Dataset = init_instance_by_config(task_config["dataset"], accept_types=Dataset)
    dataset.config(dump_all=False, recursive=True)
    with class_casting(dataset, DatasetH):
        try:
            return dataset.prepare(segments="test", col_set="label", data_key=DataHandlerLP.DK_R)
        except TypeError:
            return dataset.prepare(segments="test", col_set="label")


def _run_seed_analysis_records(config: dict, recorder, label_obj) -> None:
    import copy
    from qlib.data.dataset import Dataset
    from qlib.utils import init_instance_by_config

    if label_obj is None:
        raise RuntimeError("portfolio seed ensemble final recorder requires label.pkl for IC diagnostics")
    task_config = copy.deepcopy(config.get("task") or {})
    records_config = task_config.get("record", [])
    if isinstance(records_config, dict):
        records_config = [records_config]
    analysis_records = [
        rec for rec in records_config
        if "SigAna" in str(rec.get("class", "")) and "SignalRecord" not in str(rec.get("class", ""))
    ]
    if not analysis_records:
        return
    dataset: Dataset = init_instance_by_config(task_config["dataset"], accept_types=Dataset)
    dataset.config(dump_all=False, recursive=True)
    for record_config in analysis_records:
        rec_class = record_config.get("class", "")
        print(f"[INFO] Portfolio seed ensemble: executing final analysis record {rec_class}")
        record = init_instance_by_config(
            record_config,
            recorder=recorder,
            default_module="qlib.workflow.record_temp",
            try_kwargs={"dataset": dataset},
        )
        record.generate()


def _task_train_with_gats_industry_provider(
    config: dict,
    experiment_name: str,
    *,
    manage_resource_phases: bool = True,
    release_next_phase: str = "backtest",
):
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
    if not manage_resource_phases:
        with defer_runtime_phase_events("nested_qe_task_train"):
            return task_train(task_config, experiment_name=experiment_name)
    return task_train_with_phase_events(
        task_config,
        experiment_name=experiment_name,
        release_next_phase=release_next_phase,
    )


def _run_full_backtest(
    config: dict,
    experiment_name: str,
    *,
    mode: str = "full",
    output_dir: Path | str | None = None,
    manage_resource_phases: bool = True,
):
    recorder = _task_train_with_gats_industry_provider(
        config,
        experiment_name=experiment_name,
        manage_resource_phases=manage_resource_phases,
        release_next_phase="backtest",
    )
    recorder_ref = _write_qe_current_recorder(recorder, mode, experiment_name)
    recorder.save_objects(config=config)
    _maybe_upload_prediction_store(recorder, recorder_ref, mode, experiment_name, config)
    save_minute_trades_from_recorder(recorder, output_dir=output_dir or os.getcwd())
    return recorder


def _run_seed_score_ensemble(config: dict, experiment_name: str, ensemble: dict) -> None:
    import copy

    seeds = [int(seed) for seed in ensemble["seeds"]]
    agg = ensemble["agg"]
    output_dir = Path("seed_ensemble")
    output_dir.mkdir(parents=True, exist_ok=True)
    seed_scores: list[tuple[int, pd.Series]] = []
    manifest = {
        "schema_version": "qe_seed_ensemble_v1",
        "level": "score",
        "agg": agg,
        "seeds": seeds,
        "seed_recorders": [],
    }
    transition_runtime_phase(
        "train",
        metadata={"runner_mode": "seed_score_ensemble", "seed_count": len(seeds)},
    )
    for seed in seeds:
        seed_config = copy.deepcopy(config)
        _set_config_seed(seed_config, seed)
        apply_qe_fixed_seed(seed_config)
        seed_experiment_name = f"{experiment_name}__seed_{seed}"
        print(f"[INFO] Seed ensemble: training seed={seed} experiment={seed_experiment_name}")
        recorder = _run_train_only(
            seed_config,
            seed_experiment_name,
            manage_resource_phases=False,
        )
        pred_obj = recorder.load_object("pred.pkl")
        score_series = _prediction_score_series(pred_obj, seed=seed)
        seed_scores.append((seed, score_series))
        seed_dir = output_dir / f"seed_{seed}"
        seed_dir.mkdir(parents=True, exist_ok=True)
        with (seed_dir / "pred.pkl").open("wb") as f:
            pickle.dump(score_series.to_frame("score"), f, protocol=pickle.HIGHEST_PROTOCOL)
        manifest["seed_recorders"].append(
            {
                "seed": seed,
                "experiment_name": seed_experiment_name,
                "recorder_id": str((getattr(recorder, "info", {}) or {}).get("id") or ""),
                "rows": int(score_series.shape[0]),
            }
        )

    transition_runtime_phase(
        "predict",
        metadata={"runner_mode": "seed_score_ensemble", "seed_count": len(seeds)},
    )
    combined_pred = _aggregate_seed_predictions(seed_scores, agg)
    combined_path = output_dir / "ensemble_pred.pkl"
    with combined_path.open("wb") as f:
        pickle.dump(combined_pred, f, protocol=pickle.HIGHEST_PROTOCOL)
    manifest["combined_rows"] = int(combined_pred.shape[0])
    manifest["combined_path"] = str(combined_path)
    (output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"[INFO] Seed ensemble: aggregated {len(seeds)} seeds with agg={agg}; "
        f"rows={combined_pred.shape[0]}"
    )
    finalize_gpu_phase_lifecycle(next_phase="backtest")
    _run_pred_backtest(config, experiment_name, combined_path)


def _run_seed_portfolio_ensemble(config: dict, experiment_name: str, ensemble: dict) -> None:
    import copy
    from qlib.workflow import R

    seeds = [int(seed) for seed in ensemble["seeds"]]
    agg = ensemble["agg"]
    output_dir = Path("seed_ensemble")
    output_dir.mkdir(parents=True, exist_ok=True)
    seed_scores: list[tuple[int, pd.Series]] = []
    seed_reports: list[tuple[int, pd.DataFrame]] = []
    seed_positions: list[tuple[int, dict[pd.Timestamp, Any]]] = []
    seed_indicators: list[tuple[int, pd.DataFrame]] = []
    label_obj = None
    manifest = {
        "schema_version": "qe_seed_ensemble_v1",
        "level": "portfolio",
        "agg": agg,
        "portfolio_agg": "mean",
        "seeds": seeds,
        "seed_recorders": [],
    }
    transition_runtime_phase(
        "train",
        metadata={"runner_mode": "seed_portfolio_ensemble", "seed_count": len(seeds)},
    )
    for seed in seeds:
        seed_config = copy.deepcopy(config)
        _set_config_seed(seed_config, seed)
        apply_qe_fixed_seed(seed_config)
        seed_experiment_name = f"{experiment_name}__seed_{seed}"
        seed_dir = output_dir / f"seed_{seed}"
        seed_dir.mkdir(parents=True, exist_ok=True)
        print(f"[INFO] Portfolio seed ensemble: full backtest seed={seed} experiment={seed_experiment_name}")
        recorder = _run_full_backtest(
            seed_config,
            seed_experiment_name,
            mode="seed_portfolio_member",
            output_dir=seed_dir,
            manage_resource_phases=False,
        )

        pred_obj = _load_recorder_object(recorder, "pred.pkl", seed=seed)
        seed_scores.append((seed, _prediction_score_series(pred_obj, seed=seed)))

        report_obj = _load_recorder_object(recorder, "portfolio_analysis/report_normal_1day.pkl", seed=seed)
        seed_reports.append((seed, _normalize_daily_frame(report_obj, seed=seed, artifact_name="report_normal_1day.pkl")))

        positions_obj = _load_recorder_object(recorder, "portfolio_analysis/positions_normal_1day.pkl", seed=seed)
        seed_positions.append((seed, _normalize_position_dates(positions_obj, seed=seed)))

        indicators_obj = _load_recorder_object(
            recorder,
            "portfolio_analysis/indicators_normal_1day.pkl",
            seed=seed,
            required=False,
        )
        if indicators_obj is not None:
            seed_indicators.append(
                (seed, _normalize_daily_frame(indicators_obj, seed=seed, artifact_name="indicators_normal_1day.pkl"))
            )

        if label_obj is None:
            label_obj = _load_recorder_object(recorder, "label.pkl", seed=seed, required=False)
        if label_obj is None:
            label_obj = _load_recorder_object(recorder, "sig_analysis/label.pkl", seed=seed, required=False)

        with (seed_dir / "report_normal_1day.pkl").open("wb") as f:
            pickle.dump(seed_reports[-1][1], f, protocol=pickle.HIGHEST_PROTOCOL)
        with (seed_dir / "positions_normal_1day.pkl").open("wb") as f:
            pickle.dump(seed_positions[-1][1], f, protocol=pickle.HIGHEST_PROTOCOL)
        manifest["seed_recorders"].append(
            {
                "seed": seed,
                "experiment_name": seed_experiment_name,
                "recorder_id": str((getattr(recorder, "info", {}) or {}).get("id") or ""),
                "report_rows": int(seed_reports[-1][1].shape[0]),
                "position_days": int(len(seed_positions[-1][1])),
            }
        )

    transition_runtime_phase(
        "predict",
        metadata={"runner_mode": "seed_portfolio_ensemble", "seed_count": len(seeds)},
    )
    finalize_gpu_phase_lifecycle(next_phase="finalize")

    combined_pred = _aggregate_seed_predictions(seed_scores, agg)
    merged_positions = _aggregate_seed_positions(seed_positions, seed_reports)
    merged_report = _aggregate_seed_reports(seed_reports, merged_positions)
    common_dates = sorted(merged_positions)
    merged_indicators = _aggregate_seed_indicators(seed_indicators, common_dates)
    metrics = _build_portfolio_ensemble_metrics(merged_report)
    if label_obj is None:
        try:
            label_obj = _load_test_label_from_config(config)
        except Exception as exc:
            print(f"[WARN] Portfolio seed ensemble: final IC label fallback failed: {exc}")
    if label_obj is None or (hasattr(label_obj, "empty") and label_obj.empty):
        raise RuntimeError("portfolio seed ensemble could not load a non-empty label for final IC diagnostics")

    combined_path = output_dir / "ensemble_pred.pkl"
    report_path = output_dir / "ensemble_report_normal_1day.pkl"
    positions_path = output_dir / "ensemble_positions_normal_1day.pkl"
    with combined_path.open("wb") as f:
        pickle.dump(combined_pred, f, protocol=pickle.HIGHEST_PROTOCOL)
    with report_path.open("wb") as f:
        pickle.dump(merged_report, f, protocol=pickle.HIGHEST_PROTOCOL)
    with positions_path.open("wb") as f:
        pickle.dump(merged_positions, f, protocol=pickle.HIGHEST_PROTOCOL)

    print(
        f"[INFO] Portfolio seed ensemble: merged {len(seeds)} seeds; "
        f"days={len(merged_positions)} pred_rows={combined_pred.shape[0]}"
    )
    with R.start(experiment_name=experiment_name):
        recorder = R.get_recorder()
        recorder_ref = _write_qe_current_recorder(recorder, "seed_portfolio_ensemble", experiment_name)
        save_payload = {
            "pred.pkl": combined_pred,
            "config": config,
        }
        if label_obj is not None and not (hasattr(label_obj, "empty") and label_obj.empty):
            save_payload["label.pkl"] = label_obj
        recorder.save_objects(**save_payload)
        recorder.save_objects(**{"report_normal_1day.pkl": merged_report}, artifact_path="portfolio_analysis")
        recorder.save_objects(**{"positions_normal_1day.pkl": merged_positions}, artifact_path="portfolio_analysis")
        if merged_indicators is not None:
            recorder.save_objects(**{"indicators_normal_1day.pkl": merged_indicators}, artifact_path="portfolio_analysis")
        if metrics:
            recorder.log_metrics(**metrics)
        if "label.pkl" in save_payload:
            _run_seed_analysis_records(config, recorder, label_obj)
        recorder.save_objects(config=config)
        _maybe_upload_prediction_store(recorder, recorder_ref, "seed_portfolio_ensemble", experiment_name, config)
        manifest["final_recorder_id"] = str((getattr(recorder, "info", {}) or {}).get("id") or "")

    manifest["combined_rows"] = int(combined_pred.shape[0])
    manifest["combined_days"] = int(len(merged_positions))
    manifest["combined_path"] = str(combined_path)
    manifest["report_path"] = str(report_path)
    manifest["positions_path"] = str(positions_path)
    (output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("yaml_path", nargs="?", default="conf.yaml")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--backtest-only", action="store_true",
                            help="skip training and backtest with an existing mlruns model")
    mode_group.add_argument("--train-only", action="store_true",
                            help="train and generate pred.pkl without portfolio backtest")
    mode_group.add_argument("--pred-backtest", type=str, metavar="PRED_PKL",
                            help="run IC analysis and portfolio backtest from an existing prediction pkl")
    args = parser.parse_args()

    start_phase_publisher()
    try:
        _run_main(args)
    except Exception as exc:
        finish_phase_publisher(status="failed", error=type(exc).__name__)
        raise
    else:
        finish_phase_publisher(status="completed")


def _run_main(args):

    # Jinja2 渲染 → YAML 解析
    rendered = render_yaml_template(args.yaml_path)
    yaml = YAML(typ="safe", pure=True)
    config = yaml.load(rendered)

    patch_backtest_config(config)
    apply_qe_fixed_seed(config)
    sys_config(config, config_path=args.yaml_path)

    # 限制 qlib 并行度（必须在 qlib.init 之前！）
    # 默认 kernels=28 会导致 28 个子进程各自继承父进程内存
    C["kernels"] = 4
    print("[INFO] Limited qlib kernels to 4")

    isolation_manifest = None
    if args.backtest_only:
        isolation_manifest = _prepare_backtest_recorder_isolation(config.get("experiment_name", "workflow"))

    # Init qlib
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        tracking_uri = str(Path(os.getcwd()).resolve() / "mlruns")
        os.environ["MLFLOW_TRACKING_URI"] = tracking_uri
    exp_manager = C["exp_manager"]
    exp_manager["kwargs"]["uri"] = "file:" + tracking_uri
    if args.backtest_only:
        _validate_backtest_recorder_isolation_manifest(isolation_manifest)
    qlib.init(**config.get("qlib_init"), exp_manager=exp_manager)
    _install_mlflow_metric_read_retry()

    # 注入 benchmark Series（在 qlib init 之后，fallback 需要 D.features）
    benchmark_series = load_benchmark_series(config)
    inject_benchmark(config, benchmark_series)

    experiment_name = config.get("experiment_name", "workflow")
    seed_ensemble = _get_seed_ensemble_config(config)
    if seed_ensemble and (args.backtest_only or args.train_only or args.pred_backtest):
        raise RuntimeError("qe_runtime.ensemble is only supported in full submit mode")

    runner_mode = "full"
    if seed_ensemble:
        runner_mode = f"seed_{seed_ensemble['level']}_ensemble"
        print(f"[INFO] Seed ensemble mode: {seed_ensemble}")
        if seed_ensemble["level"] == "score":
            _run_seed_score_ensemble(config, experiment_name, seed_ensemble)
        elif seed_ensemble["level"] == "portfolio":
            _run_seed_portfolio_ensemble(config, experiment_name, seed_ensemble)
        else:
            raise ValueError(f"unsupported seed ensemble level: {seed_ensemble['level']}")
    elif args.pred_backtest:
        runner_mode = "pred_backtest"
        # Pred-backtest mode: use an externally supplied prediction file.
        pred_path = Path(args.pred_backtest)
        if not pred_path.exists():
            raise FileNotFoundError(
                f"--pred-backtest: prediction file not found: {pred_path.resolve()}"
            )
        print(f"[INFO] Pred-backtest mode: loading prediction from {pred_path}")
        transition_runtime_phase("backtest", metadata={"runner_mode": runner_mode})
        _run_pred_backtest(config, experiment_name, pred_path)
    elif args.backtest_only:
        runner_mode = "backtest_only"
        # Backtest-only mode: skip training and load an existing model.
        print("[INFO] Backtest-only mode: skipping model training, loading existing model")
        transition_runtime_phase("backtest", metadata={"runner_mode": runner_mode})
        _run_backtest_only(config, experiment_name)
    elif args.train_only:
        runner_mode = "train_only"
        # Train-only mode: generate pred.pkl but skip portfolio backtest.
        print("[INFO] Train-only mode: training model + generating predictions, skipping backtest")
        _run_train_only(config, experiment_name)
    else:
        # Full mode: train and backtest.
        _run_full_backtest(config, experiment_name)
    transition_runtime_phase("finalize", metadata={"runner_mode": runner_mode})


def _run_pred_backtest(config: dict, experiment_name: str, pred_path: Path):
    """从已有的 prediction pkl 文件直接执行 IC分析 + 选股 + 分钟线回测。

    用于多Alpha统一回测：主节点合并各组 pred.pkl 后，用 combined prediction
    执行完整的选股策略（TopK）+ 分钟线执行（TailTWAP）+ 回测分析。

    流程：
    1. 加载 combined_prediction.pkl（MultiIndex: datetime × instrument → score）
    2. 初始化 dataset（只需要 test segment 的 label，用于 SigAnaRecord 计算 IC）
    3. 创建新 recorder，注入 pred.pkl
    4. 执行 SigAnaRecord（IC/ICIR 分析）
    5. 执行 PortAnaRecord（选股+分钟线回测：收益/回撤/Sharpe/换手率/持仓）
    """
    import copy
    import pandas as pd
    from qlib.utils import init_instance_by_config
    from qlib.workflow import R
    from qlib.data.dataset import Dataset

    # 1. 加载 prediction
    pred_df = _load_pickle_with_size_bound(
        pred_path,
        max_bytes=_pickle_max_bytes(
            PRED_BACKTEST_PICKLE_MAX_BYTES_ENV,
            DEFAULT_PRED_BACKTEST_PICKLE_MAX_BYTES,
        ),
        purpose="pred-backtest prediction pickle",
    )

    if isinstance(pred_df, pd.Series):
        pred_df = pred_df.to_frame("score")
    if not isinstance(pred_df, pd.DataFrame):
        raise TypeError(
            f"--pred-backtest: prediction 文件内容类型错误: {type(pred_df).__name__}，"
            f"期望 pd.DataFrame 或 pd.Series"
        )
    if not isinstance(pred_df.index, pd.MultiIndex):
        raise ValueError(
            f"--pred-backtest: prediction 必须是 MultiIndex (datetime, instrument)，"
            f"实际 index 类型: {type(pred_df.index).__name__}"
        )

    print(f"[INFO] Loaded prediction: {len(pred_df)} rows, "
          f"columns={list(pred_df.columns)}, "
          f"date range: {pred_df.index.get_level_values(0).min()} ~ "
          f"{pred_df.index.get_level_values(0).max()}")

    # 2. 初始化 dataset（需要 label 用于 SigAnaRecord 计算 IC）
    task_config = copy.deepcopy(config.get("task"))
    dataset: Dataset = init_instance_by_config(task_config["dataset"], accept_types=Dataset)
    dataset.config(dump_all=False, recursive=True)

    # 从 dataset 提取 label（SigAnaRecord 依赖 label.pkl 存在于 recorder 中）
    from qlib.data.dataset.handler import DataHandlerLP
    from qlib.data.dataset import DatasetH
    from qlib.utils import class_casting
    with class_casting(dataset, DatasetH):
        try:
            raw_label = dataset.prepare(segments="test", col_set="label", data_key=DataHandlerLP.DK_R)
        except TypeError:
            raw_label = dataset.prepare(segments="test", col_set="label")
    if raw_label is None or (hasattr(raw_label, 'empty') and raw_label.empty):
        raise RuntimeError(
            "--pred-backtest: 无法从 dataset 获取 label。"
            "SigAnaRecord 需要 label 来计算 IC/ICIR。"
            "请检查 conf.yaml 的 dataset 配置和数据路径。"
        )
    print(f"[INFO] Extracted label from dataset: {len(raw_label)} rows")

    # 3. 构建 records 列表：跳过 SignalRecord（不需要模型预测），保留 SigAnaRecord + PortAnaRecord
    records_config = task_config.get("record", [])
    if isinstance(records_config, dict):
        records_config = [records_config]

    filtered_records = []
    for rec in records_config:
        rec_class = rec.get("class", "")
        if "SignalRecord" in rec_class:
            # SignalRecord 需要 model 来生成 pred.pkl，pred-backtest 模式下跳过
            # pred.pkl 已经直接注入 recorder
            print(f"[INFO] Pred-backtest: skipping {rec_class} (prediction already provided)")
            continue
        filtered_records.append(rec)

    if not filtered_records:
        raise RuntimeError(
            "--pred-backtest: 过滤后没有可执行的 record。"
            "conf.yaml 必须包含 SigAnaRecord 和/或 PortAnaRecord。"
        )

    # 检查必须有 PortAnaRecord（回测是核心目的）
    has_port_ana = any("PortAna" in r.get("class", "") for r in filtered_records)
    if not has_port_ana:
        raise RuntimeError(
            "--pred-backtest: conf.yaml 中缺少 PortAnaRecord。"
            "统一回测必须包含 PortAnaRecord 才能执行选股+回测。"
        )

    # 4. 创建新 recorder，注入 pred.pkl + label.pkl，执行 records
    # SigAnaRecord 和 PortAnaRecord 不包含 <MODEL>/<DATASET> 占位符，
    # 不需要 fill_placeholder。直接用 init_instance_by_config 实例化。

    with R.start(experiment_name=experiment_name):
        recorder = R.get_recorder()
        recorder_ref = _write_qe_current_recorder(recorder, "pred_backtest", experiment_name)
        # 注入 prediction 和 label 到 recorder
        # SigAnaRecord 依赖: pred.pkl + label.pkl（check() 验证两者都存在）
        # PortAnaRecord 依赖: pred.pkl（从 recorder 加载预测信号）
        recorder.save_objects(**{"pred.pkl": pred_df, "label.pkl": raw_label})
        print(f"[INFO] Injected pred.pkl + label.pkl into recorder: {recorder.info['id']}")

        for record_config in filtered_records:
            rec_class = record_config.get("class", "")
            print(f"[INFO] Executing: {rec_class}")
            r = init_instance_by_config(
                record_config,
                recorder=recorder,
                default_module="qlib.workflow.record_temp",
                try_kwargs={"dataset": dataset},
            )
            r.generate()
            print(f"[INFO] Completed: {rec_class}")

        recorder.save_objects(config=config)
        _maybe_upload_prediction_store(recorder, recorder_ref, "pred_backtest", experiment_name, config)
        
        # 保存分钟级交易记录（环境变量控制）
        save_minute_trades_from_recorder(recorder, output_dir=os.getcwd())

    print("[INFO] Pred-backtest completed: IC analysis + portfolio backtest done")


def _run_train_only(
    config: dict,
    experiment_name: str,
    *,
    manage_resource_phases: bool = True,
):
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

    config_for_train = copy.deepcopy(config)
    config_for_train["task"] = task_config
    recorder = _task_train_with_gats_industry_provider(
        config_for_train,
        experiment_name=experiment_name,
        manage_resource_phases=manage_resource_phases,
        release_next_phase="finalize",
    )
    recorder_ref = _write_qe_current_recorder(recorder, "train_only", experiment_name)
    recorder.save_objects(config=config)
    _maybe_upload_prediction_store(recorder, recorder_ref, "train_only", experiment_name, config)
    print("[INFO] Train-only completed: model trained, pred.pkl generated")
    return recorder


def _load_backtest_only_model_from_loose_params(mlruns_dir: Path):
    """Load a model from bare params.pkl archives when MLflow metadata is absent."""
    if not mlruns_dir.exists():
        return None, None
    params_files = sorted(
        mlruns_dir.glob("**/params.pkl"),
        key=lambda p: p.stat().st_mtime,
    )
    for params_path in reversed(params_files):
        try:
            return (
                _load_pickle_with_size_bound(
                    params_path,
                    max_bytes=_pickle_max_bytes(
                        PARAMS_PICKLE_MAX_BYTES_ENV,
                        DEFAULT_PARAMS_PICKLE_MAX_BYTES,
                    ),
                    purpose="backtest-only source params pickle",
                ),
                params_path,
            )
        except Exception as exc:
            print(f"[WARN] Failed to load loose params.pkl {params_path}: {exc}")
    return None, None


def _run_backtest_only(config: dict, experiment_name: str):
    """从已有 mlruns 加载训练好的模型，只执行信号生成 + 回测。

    前提：之前的训练已完成（params.pkl 和 dataset 已保存到 mlruns）。
    """
    from qlib.utils import init_instance_by_config
    from qlib.workflow import R
    from qlib.data.dataset import Dataset
    from qlib.model.trainer import fill_placeholder

    task_config = config.get("task")
    source_params_dir = Path(os.environ.get(SOURCE_PARAMS_ENV, "source_model"))
    model, params_path = _load_backtest_only_model_from_loose_params(source_params_dir)
    if model is None:
        raise RuntimeError(
            f"{ERR_SOURCE_PARAMS_MISSING}: Backtest-only source params.pkl not found "
            f"under {source_params_dir}; target mlruns is reserved for recorder writes."
        )
    print(f"[INFO] Loaded trained model from isolated source params.pkl {params_path}")
    if (
        getattr(model, "gats_adjacency_mode", None) == "industry_bias"
        or getattr(model, "gats_industry_embedding", None) is True
        or str(getattr(model, "gats_industry_embedding", "off")).lower() == "on"
    ):
        from aistock_models.gats_industry_provider import attach_gats_industry_provider_to_model

        attach_gats_industry_provider_to_model(model, config, cwd=Path.cwd(), print_fn=print)

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
    _validate_backtest_recorder_isolation_manifest()

    with R.start(experiment_name=experiment_name):
        recorder = R.get_recorder()
        recorder_ref = _write_qe_current_recorder(recorder, "backtest_only", experiment_name)
        for record_config in records:
            r = init_instance_by_config(
                record_config,
                recorder=recorder,
                default_module="qlib.workflow.record_temp",
                try_kwargs={"model": model, "dataset": dataset},
            )
            r.generate()
        recorder.save_objects(config=config, params_pkl=model)
        _maybe_upload_prediction_store(recorder, recorder_ref, "backtest_only", experiment_name, config)

    print("[INFO] Backtest-only completed successfully")



def _maybe_enable_board_lot_exchange(exchange_kwargs: dict) -> None:
    """Enable stock-aware Qlib Exchange rounding when V25.1 requests it."""

    if not exchange_kwargs.pop('board_lot_trade_unit', False):
        return
    exchange_kwargs['trade_unit'] = None
    from qe_board_lot_exchange import install_board_lot_exchange_patch

    install_board_lot_exchange_patch()
    print("[INFO] Enabled board-lot-aware Qlib Exchange patch")

if __name__ == '__main__':
    main()
