#!/usr/bin/env python3
"""统一因子独立指标计算脚本（WSL 端执行）。

批量执行因子代码 + 调用 engine 计算全部独立指标。
替代旧的 RDAgent Results API 链路，不依赖任何 task workspace。

用法:
    python compute_factor_metrics_unified.py <workspace_dir> <factor1> [factor2] ...

workspace_dir 结构:
    workspace/
    ├── daily_pv.h5          (symlink to factor data)
    ├── daily_basic.h5
    ├── moneyflow.h5
    ├── bak_basic.h5
    ├── cyq_perf.h5
    ├── sector_data.h5
    ├── _factor_xxx/
    │   └── factor.py
    └── _factor_yyy/
        └── factor.py

输出: JSON 到 stdout
"""
import json
import logging
import os
import subprocess
import sys
import time
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd

# 抑制 qlib 和其他库的日志输出到 stderr，保持 stdout 纯净 JSON
logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("compute_factor_metrics_unified")

# 抑制 rdagent/qlib 的 INFO 日志
for noisy in ("qlib", "rdagent", "urllib3", "filelock"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

FACTOR_EXEC_TIMEOUT = 600  # 单因子执行超时秒数

# PyTables C 扩展不是完全线程安全的，多线程并发 pd.read_hdf() 可导致 SIGSEGV
_hdf_read_lock = threading.Lock()

DATA_H5_FILES = [
    "daily_pv.h5", "daily_basic.h5", "moneyflow.h5",
    "bak_basic.h5", "cyq_perf.h5", "sector_data.h5",
    "static_factors.parquet",
]


def execute_factor(workspace: Path, factor_name: str) -> dict:
    """执行单个因子代码，返回 result.h5 路径或错误。"""
    factor_dir = workspace / f"_factor_{factor_name}"
    factor_py = factor_dir / "factor.py"

    if not factor_py.exists():
        return {"success": False, "error": f"factor.py not found: {factor_py}"}

    # 确保因子子目录中有数据文件的 symlink
    # RDAgent 因子代码使用相对路径如 pd.read_hdf("daily_pv.h5")
    for h5 in DATA_H5_FILES:
        src = workspace / h5
        dst = factor_dir / h5
        if src.exists() and not dst.exists():
            try:
                dst.symlink_to(src)
            except OSError:
                pass  # 可能已存在或权限问题

    t0 = time.time()
    try:
        result = subprocess.run(
            [sys.executable, "factor.py"],
            cwd=str(factor_dir),
            capture_output=True,
            text=True,
            timeout=FACTOR_EXEC_TIMEOUT,
        )
        duration = time.time() - t0

        if result.returncode != 0:
            stderr_tail = result.stderr[-500:] if result.stderr else ""
            return {
                "success": False,
                "error": f"Exit code {result.returncode}: {stderr_tail}",
                "duration": duration,
            }

        result_h5 = factor_dir / "result.h5"
        if not result_h5.exists():
            return {
                "success": False,
                "error": "factor.py 执行完成但未生成 result.h5",
                "duration": duration,
            }

        return {"success": True, "path": str(result_h5), "duration": duration}

    except subprocess.TimeoutExpired:
        return {"success": False, "error": f"执行超时 ({FACTOR_EXEC_TIMEOUT}s)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def read_and_validate_result(h5_path: str, factor_name: str) -> dict:
    """读取 result.h5 并验证格式。"""
    try:
        # PyTables 不是线程安全的，必须串行读取防止 SIGSEGV
        with _hdf_read_lock:
            df = pd.read_hdf(h5_path)

        if not isinstance(df.index, pd.MultiIndex):
            return {"success": False, "error": "result.h5 缺少 MultiIndex(datetime, instrument)"}

        if df.index.nlevels != 2:
            return {"success": False, "error": f"MultiIndex 层数应为 2，实际 {df.index.nlevels}"}

        # 标准化 index names
        df.index.names = ["datetime", "instrument"]

        # 确保至少有一列数据
        if df.shape[1] == 0:
            return {"success": False, "error": "result.h5 没有数据列"}

        # 取第一列作为因子值（如果列名不匹配则重命名）
        if df.shape[1] == 1:
            df.columns = [factor_name]
        elif factor_name not in df.columns:
            # 多列情况，取第一列重命名
            df = df.iloc[:, :1]
            df.columns = [factor_name]

        return {"success": True, "df": df, "shape": df.shape}

    except Exception as e:
        return {"success": False, "error": f"读取 result.h5 失败: {e}"}


def _execute_and_validate(workspace: Path, fname: str) -> dict:
    """执行单因子代码 + 验证 result.h5，返回统一结构。

    Returns dict with keys: factor_name, status, error, duration, shape, df.
    status: "ok" | "timeout" | "error"
    """
    exec_result = execute_factor(workspace, fname)
    if not exec_result["success"]:
        err_msg = exec_result.get("error", "")
        return {
            "factor_name": fname,
            "status": "timeout" if "超时" in err_msg else "error",
            "error": err_msg,
            "duration": exec_result.get("duration"),
            "df": None,
        }
    read_result = read_and_validate_result(exec_result["path"], fname)
    if not read_result["success"]:
        return {
            "factor_name": fname,
            "status": "error",
            "error": read_result["error"],
            "duration": exec_result.get("duration"),
            "df": None,
        }
    return {
        "factor_name": fname,
        "status": "ok",
        "shape": list(read_result["shape"]),
        "duration": round(exec_result.get("duration", 0), 2),
        "df": read_result["df"],
    }


def merge_to_parquet(factor_dfs: dict, output_path: Path) -> tuple:
    """合并多个因子 DataFrame 为 combined_factors_df.parquet。

    单因子数据异常跳过，不影响其他因子。
    Returns: (error_message, merge_errors_dict)
    """
    if not factor_dfs:
        return "没有可合并的因子数据", {}

    valid_dfs = {}
    merge_errors = {}
    for fname, df in factor_dfs.items():
        try:
            if df.empty or df.iloc[:, 0].isna().all():
                merge_errors[fname] = "因子数据全部为 NaN"
                continue
            valid_dfs[fname] = df
        except Exception as e:
            merge_errors[fname] = f"验证失败: {e}"

    if not valid_dfs:
        return "所有因子数据无效（全 NaN 或验证失败）", merge_errors

    try:
        combined = pd.concat(valid_dfs.values(), axis=1)
        combined.index.names = ["datetime", "instrument"]
        combined.to_parquet(output_path)
        logger.info(f"Parquet saved: {combined.shape} -> {output_path}")
        if merge_errors:
            logger.warning(f"Merge 阶段跳过因子: {merge_errors}")
        return "", merge_errors
    except Exception as e:
        return f"合并 parquet 失败: {e}", merge_errors


def compute_metrics(parquet_path: Path) -> dict:
    """调用 engine 计算全部独立指标。"""
    try:
        # 动态 import engine（需要 rdagent 在 PYTHONPATH 中）
        from rdagent.app.factor_metrics.engine import compute_all_factors_metrics

        result = compute_all_factors_metrics(
            parquet_path=parquet_path,
            max_workers=4,
        )
        return {"success": True, "data": result}
    except Exception as e:
        logger.error(f"Engine error: {traceback.format_exc()}")
        return {"success": False, "error": str(e)}


def main():
    # 解析参数
    data_date = None
    stream_mode = False
    args = sys.argv[1:]
    filtered_args = []
    i = 0
    while i < len(args):
        if args[i] == "--data-date" and i + 1 < len(args):
            data_date = args[i + 1]
            i += 2
        elif args[i] == "--stream":
            stream_mode = True
            i += 1
        else:
            filtered_args.append(args[i])
            i += 1

    if len(filtered_args) < 2:
        print(json.dumps({
            "success": False,
            "error": "用法: python compute_factor_metrics_unified.py <workspace_dir> <factor1> [factor2] ... [--data-date YYYYMMDD] [--stream]"
        }))
        sys.exit(1)

    workspace = Path(filtered_args[0])
    factor_names = filtered_args[1:]

    if not workspace.exists():
        print(json.dumps({"success": False, "error": f"Workspace 不存在: {workspace}"}))
        sys.exit(1)

    if data_date:
        logger.info(f"使用数据快照: {data_date}")

    if stream_mode:
        _main_stream(workspace, factor_names, data_date)
    else:
        _main_batch(workspace, factor_names, data_date)


# NaN/Inf 安全序列化
def _sanitize(obj):
    if isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        v = float(obj)
        return None if (np.isnan(v) or np.isinf(v)) else v
    return obj


def _emit(obj: dict):
    """线程安全地输出一行 JSON 到 stdout（NDJSON 格式）。"""
    print(json.dumps(_sanitize(obj), ensure_ascii=False), flush=True)


# stdout 锁 — 多线程输出 NDJSON 时保证单行完整性
_stdout_lock = threading.Lock()


def _emit_locked(obj: dict):
    """线程安全 emit，用于 ThreadPoolExecutor 内。"""
    with _stdout_lock:
        _emit(obj)


# ================================================================
# Stream mode: 预加载 shared context → 逐因子流式输出
# ================================================================

def _main_stream(workspace: Path, factor_names: list, data_date: str | None):
    """流式模式：每完成一个因子立即输出结果行。"""
    from rdagent.app.factor_metrics.engine import (
        prepare_shared_context,
        compute_single_factor_metrics,
    )

    logger.info(f"[stream] Workspace: {workspace}, Factors: {factor_names}")
    t_total = time.time()

    # Step 1: 预加载 shared context（一次性）
    try:
        ctx = prepare_shared_context()
        _emit({
            "type": "init", "status": "ok",
            "data_start": ctx["data_start"], "data_end": ctx["data_end"],
            "n_instruments": len(ctx["close_unstacked"].columns),
            "calc_batch_id": ctx["calc_batch_id"],
        })
    except Exception as e:
        _emit({"type": "init", "status": "error", "error": str(e)})
        sys.exit(1)

    # Step 2: 并行执行因子 + 计算指标，逐个输出
    ok_count = 0
    fail_count = 0
    max_parallel = min(4, len(factor_names))

    def _process_one_factor(fname: str):
        """单因子完整流水线：执行代码 → 验证 → 计算指标 → 输出。"""
        nonlocal ok_count, fail_count
        t0 = time.time()

        # 2a: 执行因子代码
        exec_result = execute_factor(workspace, fname)
        if not exec_result["success"]:
            err_msg = exec_result.get("error", "")
            status = "timeout" if "超时" in err_msg else "error"
            _emit_locked({
                "type": "factor_done", "factor_name": fname,
                "status": status, "error": err_msg,
                "duration": round(time.time() - t0, 2),
            })
            fail_count += 1
            return

        # 2b: 读取并验证 result.h5
        read_result = read_and_validate_result(exec_result["path"], fname)
        if not read_result["success"]:
            _emit_locked({
                "type": "factor_done", "factor_name": fname,
                "status": "error", "error": read_result["error"],
                "duration": round(time.time() - t0, 2),
            })
            fail_count += 1
            return

        # 2c: 使用 shared context 计算指标
        try:
            metrics_result = compute_single_factor_metrics(
                fname, read_result["df"], ctx,
            )
            metrics = metrics_result.get("metrics", {})
            if not metrics:
                _emit_locked({
                    "type": "factor_done", "factor_name": fname,
                    "status": "error",
                    "error": f"指标计算返回空结果, reports={metrics_result.get('reports', [])}",
                    "duration": round(time.time() - t0, 2),
                })
                fail_count += 1
                return
            _emit_locked({
                "type": "factor_done", "factor_name": fname,
                "status": "ok",
                "metrics": metrics,
                "reports": metrics_result.get("reports", []),
                "duration": round(time.time() - t0, 2),
            })
            ok_count += 1
        except Exception as e:
            logger.error(f"Metrics compute error for {fname}: {traceback.format_exc()}")
            _emit_locked({
                "type": "factor_done", "factor_name": fname,
                "status": "error", "error": f"指标计算失败: {e}",
                "duration": round(time.time() - t0, 2),
            })
            fail_count += 1

    logger.info(f"[stream] Parallel processing {len(factor_names)} factors (max_parallel={max_parallel})")

    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        futures = {
            pool.submit(_process_one_factor, fname): fname
            for fname in factor_names
        }
        for future in as_completed(futures):
            fname = futures[future]
            try:
                future.result(timeout=FACTOR_EXEC_TIMEOUT + 120)
            except Exception as e:
                logger.error(f"Future exception for {fname}: {e}")
                _emit_locked({
                    "type": "factor_done", "factor_name": fname,
                    "status": "error", "error": f"执行异常: {e}",
                })
                fail_count += 1

    # Step 3: 输出 summary
    _emit({
        "type": "summary",
        "total": len(factor_names), "ok": ok_count,
        "failed": fail_count,
        "total_duration_sec": round(time.time() - t_total, 2),
        "calc_batch_id": ctx["calc_batch_id"],
    })
    logger.info(f"[stream] Done. {ok_count}/{len(factor_names)} ok, {time.time()-t_total:.1f}s")


# ================================================================
# Batch mode: 旧逻辑（兼容，不加 --stream 时使用）
# ================================================================

def _main_batch(workspace: Path, factor_names: list, data_date: str | None):
    """批量模式：所有因子完成后一次性输出 JSON。"""
    logger.info(f"[batch] Workspace: {workspace}, Factors: {factor_names}")
    t_total = time.time()

    # Step 1: 并行执行因子代码
    execution_log = {}
    factor_dfs = {}

    max_parallel = min(4, len(factor_names))
    logger.info(f"Parallel executing {len(factor_names)} factors (max_parallel={max_parallel})")

    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        future_map = {
            pool.submit(_execute_and_validate, workspace, fname): fname
            for fname in factor_names
        }
        for future in as_completed(future_map):
            fname = future_map[future]
            try:
                result = future.result(timeout=FACTOR_EXEC_TIMEOUT + 60)
            except Exception as e:
                result = {
                    "factor_name": fname, "status": "error",
                    "error": f"并行执行异常: {e}", "df": None,
                }

            df = result.pop("df", None)
            if df is not None:
                factor_dfs[fname] = df
            execution_log[fname] = result
            status = result["status"]
            if status == "ok":
                logger.info(f"  {fname}: ok, shape={result.get('shape')}, {result.get('duration', '?')}s")
            elif status == "timeout":
                logger.warning(f"  {fname}: TIMEOUT - {result.get('error')}")
            else:
                logger.warning(f"  {fname}: error - {result.get('error')}")

    if not factor_dfs:
        output = {
            "success": False,
            "error": "所有因子执行失败，无可计算的因子",
            "execution_log": execution_log,
            "total_duration_sec": round(time.time() - t_total, 2),
        }
        print(json.dumps(output, ensure_ascii=False))
        sys.exit(0)

    # Step 2: 合并为 parquet
    parquet_path = workspace / "combined_factors_df.parquet"
    merge_err, merge_errors = merge_to_parquet(factor_dfs, parquet_path)

    for fname, err_msg in merge_errors.items():
        execution_log[fname] = {
            "factor_name": fname,
            "status": "error",
            "error": f"merge 阶段: {err_msg}",
        }

    if merge_err:
        output = {
            "success": False,
            "error": merge_err,
            "execution_log": execution_log,
            "total_duration_sec": round(time.time() - t_total, 2),
        }
        print(json.dumps(output, ensure_ascii=False))
        sys.exit(0)

    # Step 3: 调用 engine 计算指标
    logger.info("Computing metrics via engine...")
    metrics_result = compute_metrics(parquet_path)

    if not metrics_result["success"]:
        output = {
            "success": False,
            "error": metrics_result["error"],
            "execution_log": execution_log,
            "total_duration_sec": round(time.time() - t_total, 2),
        }
        print(json.dumps(output, ensure_ascii=False))
        sys.exit(0)

    # Step 4: 重组输出
    engine_data = metrics_result["data"]
    factors_metrics = {}
    for rec in engine_data.get("metrics", []):
        fname = rec.get("factor_name", "")
        window = rec.get("eval_window", "")
        if fname not in factors_metrics:
            factors_metrics[fname] = {}
        factors_metrics[fname][window] = {
            k: v for k, v in rec.items()
            if k not in ("factor_name", "eval_window", "calc_batch_id",
                         "data_source", "calc_engine", "calculated_at")
        }

    output = _sanitize({
        "success": True,
        "factors": factors_metrics,
        "execution_log": execution_log,
        "engine_summary": engine_data.get("summary", {}),
        "calc_batch_id": engine_data.get("calc_batch_id", ""),
        "total_duration_sec": round(time.time() - t_total, 2),
    })

    print(json.dumps(output, ensure_ascii=False))
    logger.info(f"Done. Total: {time.time() - t_total:.1f}s, "
                f"factors ok: {len(factor_dfs)}/{len(factor_names)}, "
                f"timeout: {sum(1 for v in execution_log.values() if v.get('status') == 'timeout')}, "
                f"error: {sum(1 for v in execution_log.values() if v.get('status') == 'error')}")


if __name__ == "__main__":
    main()
