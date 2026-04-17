"""一次性脚本：回测口径回填/增量补齐因子缓存。

用途：
- 扫描 rdagent_assets/qe_factors/*.py 所有因子代码（fallback 模式）
- 或通过 --code-manifest 接收 {factor_name: code_text} JSON 文件（推荐模式）
- 严格基于 experiment factor_data_dir 的历史 h5/parquet 文件执行 factor.py
- 推荐模式使用原始 code_text + subprocess 执行（与回测 prepare_factors.py 完全一致）
- 输出 single/{name}.parquet、_meta.json、_tasks/{task_id}.json、_tasks/{task_id}.failed.ndjson
- 不走 realtime loader / DB loader 作为主计算来源

运行：
  python scripts/backfill_factor_cache.py --experiment-id qe_xxx --code-manifest manifest.json --workers 4
  python scripts/backfill_factor_cache.py --experiment-id qe_xxx --resume-task-id cache_xxx --retry-failed-only --code-manifest manifest.json
"""
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from backend.services.quantevolver.config_composer import ConfigComposer
from backend.services.quantevolver.factor_value_pipeline import FactorComputeResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill")

CODE_DIR = REPO_ROOT / "rdagent_assets" / "qe_factors"
CACHE_DIR = REPO_ROOT / "rdagent_assets" / "factor_values"
SINGLE_DIR = CACHE_DIR / "single"
META_PATH = CACHE_DIR / "_meta.json"
TASKS_DIR = CACHE_DIR / "_tasks"
ALLOWED_DATA_FILES = (
    "daily_pv.h5",
    "daily_basic.h5",
    "moneyflow.h5",
    "bak_basic.h5",
    "cyq_perf.h5",
    "sector_data.h5",
    "margin_detail.h5",
    "static_factors.parquet",
)


def now_iso() -> str:
    return datetime.now().isoformat()


def _get_rss_mb() -> float:
    """获取当前进程 RSS (MB)"""
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except ImportError:
        # Windows: 用 psutil 或 tasklist
        try:
            import psutil
            return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        except ImportError:
            return 0.0


def compute_source_hash_raw(code_path: Path) -> str:
    raw = code_path.read_text(encoding="utf-8")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def load_meta() -> dict:
    if META_PATH.exists():
        return json.loads(META_PATH.read_text(encoding="utf-8"))
    return {"factors": {}}


def save_meta(meta: dict):
    META_PATH.write_text(
        json.dumps(meta, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def ensure_task_dir() -> None:
    TASKS_DIR.mkdir(parents=True, exist_ok=True)
    SINGLE_DIR.mkdir(parents=True, exist_ok=True)


def task_state_path(task_id: str) -> Path:
    return TASKS_DIR / f"{task_id}.json"


def failed_log_path(task_id: str) -> Path:
    return TASKS_DIR / f"{task_id}.failed.ndjson"


def load_task_state(task_id: str) -> dict:
    path = task_state_path(task_id)
    if not path.exists():
        raise FileNotFoundError(f"任务状态文件不存在: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def save_task_state(state: dict) -> None:
    ensure_task_dir()
    state["updated_at"] = now_iso()
    task_state_path(state["task_id"]).write_text(
        json.dumps(state, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def append_failed_log(task_id: str, payload: dict) -> None:
    ensure_task_dir()
    with failed_log_path(task_id).open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def extract_names(items: list[Any] | None) -> list[str]:
    result: list[str] = []
    for item in items or []:
        if isinstance(item, str):
            result.append(item)
        elif isinstance(item, dict) and item.get("name"):
            result.append(str(item["name"]))
    return result


def _json_load_if_needed(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def resolve_execution_context(args) -> dict[str, Any]:
    from backend.services.quantevolver.config_composer import RDAGENT_DEFAULT_DATA_SPLIT

    cc = ConfigComposer()
    exp_record = None
    data_split: dict[str, str] = dict(RDAGENT_DEFAULT_DATA_SPLIT)
    node_id = args.node_id or None

    if args.experiment_id:
        exp_record = cc._get_experiment_record(args.experiment_id)
        if not exp_record:
            raise ValueError(f"实验不存在: {args.experiment_id}")
        exp_split = _json_load_if_needed(exp_record.get("data_split")) or {}
        if exp_split and not isinstance(exp_split, dict):
            raise ValueError(f"实验 {args.experiment_id} 的 data_split 非法")
        if exp_split:
            cc._validate_data_split(exp_split)
            data_split.update(exp_split)
        node_id = node_id or exp_record.get("node_id") or None

    workspace_cfg = cc._fetch_workspace_config(node_id)
    factor_data_dir = args.factor_data_dir or workspace_cfg.get("factor_data_dir")
    if not factor_data_dir:
        raise ValueError("无法解析 factor_data_dir")

    default_train = data_split.get("train_start") or args.start or "2018-08-01"
    default_end = data_split.get("test_end") or args.end or "2026-04-03"
    if not data_split.get("train_start") and not args.start:
        print(f"[WARN] 未指定 train_start，使用 fallback: {default_train}")
    if not data_split.get("test_end") and not args.end:
        print(f"[WARN] 未指定 test_end，使用 fallback: {default_end}")
    req_train = args.window_train_start or args.start or default_train
    req_end = args.window_backtest_end or args.end or default_end
    print(f"缓存窗口: {req_train} ~ {req_end}")

    window_train_start = max(default_train, req_train)
    window_backtest_end = min(default_end, req_end)

    if window_train_start > window_backtest_end:
        raise ValueError(
            f"窗口非法: start={window_train_start} > end={window_backtest_end}"
        )

    return {
        "experiment_id": args.experiment_id,
        "node_id": node_id,
        "factor_data_dir": factor_data_dir,
        "window_train_start": window_train_start,
        "window_backtest_end": window_backtest_end,
        "data_split": data_split,
        "data_source_mode": "backtest_factor_data_dir",
        "strict_backtest_data": bool(args.strict_backtest_data),
    }


def plan_factor_action(
    name: str,
    target_start: str,
    target_end: str,
    meta: dict,
    *,
    incremental: bool,
    force: bool,
) -> dict:
    parquet_path = SINGLE_DIR / f"{name}.parquet"
    entry = meta.get("factors", {}).get(name, {})
    date_range = entry.get("date_range", "")

    if force:
        return {"action": "full_rebuild", "reason": "force", "entry": entry}
    if not parquet_path.exists():
        return {"action": "full_rebuild", "reason": "parquet_missing", "entry": entry}
    if not date_range or "~" not in date_range:
        return {"action": "full_rebuild", "reason": "no_date_range", "entry": entry}

    try:
        cached_start, cached_end = date_range.split("~")
    except Exception as e:
        return {"action": "full_rebuild", "reason": f"parse_error: {e}", "entry": entry}

    if cached_start <= target_start and cached_end >= target_end:
        return {
            "action": "skip",
            "reason": "covered",
            "cached_start": cached_start,
            "cached_end": cached_end,
            "entry": entry,
        }

    if incremental and cached_start <= target_start and cached_end < target_end:
        return {
            "action": "extend_forward",
            "reason": f"need_later ({cached_end} < {target_end})",
            "cached_start": cached_start,
            "cached_end": cached_end,
            "entry": entry,
        }

    if cached_start > target_start:
        return {
            "action": "full_rebuild",
            "reason": f"need_earlier ({cached_start} > {target_start})",
            "cached_start": cached_start,
            "cached_end": cached_end,
            "entry": entry,
        }

    if cached_end < target_end:
        return {
            "action": "full_rebuild",
            "reason": f"need_later ({cached_end} < {target_end})",
            "cached_start": cached_start,
            "cached_end": cached_end,
            "entry": entry,
        }

    return {"action": "full_rebuild", "reason": "range_mismatch", "entry": entry}


def resolve_targets(args, code_files: dict[str, Path]) -> dict[str, Path]:
    explicit_names: list[str] | None = None
    if args.factor:
        explicit_names = [args.factor]
    elif args.factors:
        explicit_names = [n.strip() for n in args.factors.split(",") if n.strip()]

    if args.retry_failed_only and not args.resume_task_id:
        raise ValueError("--retry-failed-only 必须与 --resume-task-id 一起使用")

    if args.resume_task_id:
        prev = load_task_state(args.resume_task_id)
        if args.retry_failed_only:
            names = extract_names(prev.get("failed_factors", []))
        else:
            planned = extract_names(prev.get("planned_factors", [])) or extract_names(prev.get("requested_factors", []))
            done = set(extract_names(prev.get("success_factors", []))) | set(extract_names(prev.get("skipped_factors", [])))
            names = [n for n in planned if n not in done]
        if explicit_names is not None:
            allowed = set(explicit_names)
            names = [n for n in names if n in allowed]
        missing = [n for n in names if n not in code_files]
        if missing:
            print(f"[WARN] 恢复任务中以下因子代码不存在: {missing[:10]}")
        return {n: code_files[n] for n in names if n in code_files}

    if explicit_names is not None:
        missing = [n for n in explicit_names if n not in code_files]
        if missing:
            print(f"[WARN] 以下因子代码不存在: {missing[:10]}")
        targets = {n: code_files[n] for n in explicit_names if n in code_files}
        if args.factor and not targets:
            raise ValueError(f"因子 {args.factor} 不存在于 code_manifest 或 {CODE_DIR}")
        return targets

    return code_files


def summarize_success(result: FactorComputeResult) -> str:
    detail = []
    if getattr(result, "num_rows", 0):
        detail.append(f"rows={result.num_rows}")
    if getattr(result, "date_range", None):
        detail.append(str(result.date_range))
    if getattr(result, "elapsed_sec", None) is not None:
        detail.append(f"{result.elapsed_sec:.1f}s")
    return ", ".join(detail) or "ok"


def _link_allowed_files(src_dir: str, dst_dir: str) -> list[str]:
    linked: list[str] = []
    os.makedirs(dst_dir, exist_ok=True)
    for item in ALLOWED_DATA_FILES:
        src_path = os.path.join(src_dir, item)
        dst_path = os.path.join(dst_dir, item)
        if not os.path.isfile(src_path):
            continue
        if os.path.exists(dst_path) or os.path.islink(dst_path):
            os.remove(dst_path)
        linked_ok = False
        try:
            os.link(src_path, dst_path)
            linked_ok = True
        except OSError:
            pass
        if not linked_ok:
            try:
                os.symlink(src_path, dst_path)
                linked_ok = True
            except OSError:
                pass
        if not linked_ok:
            file_size_mb = os.path.getsize(src_path) / 1024 / 1024
            raise RuntimeError(
                f"无法链接数据文件 {item} ({file_size_mb:.0f}MB): "
                f"hard link 和 symlink 均失败，src={src_path}, dst={dst_path}。"
                f"请确保 factor_data_dir 与执行目录在同一文件系统。"
            )
        linked.append(item)
    return linked



# ── Subprocess pipeline wrapper: all DataFrame operations happen here ──
# This template is appended to factor code and runs inside the subprocess.
# Placeholder markers (__XX__) are replaced with actual values before execution.
_PIPELINE_WRAPPER = r'''
import os as _os, json as _json
import pandas as _pd

_FN = __FN__
_SD = __SD__
_ED = __ED__
_CP = __CP__
_ME = __ME__
_EE = __EE__

if not _os.path.exists("result.h5"):
    raise RuntimeError("factor.py did not produce result.h5")

_df = _pd.read_hdf("result.h5")
_os.remove("result.h5")

# Normalize
if isinstance(_df, _pd.Series):
    _df = _df.to_frame(name=_FN)
if not isinstance(_df, _pd.DataFrame):
    raise RuntimeError(f"{_FN}: result is not DataFrame/Series")
if _df.empty:
    raise RuntimeError(f"{_FN}: result is empty")
if not isinstance(_df.index, _pd.MultiIndex):
    if "datetime" in _df.columns and "instrument" in _df.columns:
        _df = _df.copy()
        _df["datetime"] = _pd.to_datetime(_df["datetime"], errors="coerce")
        _df = _df.set_index(["datetime", "instrument"])
    else:
        raise RuntimeError(f"{_FN}: missing MultiIndex(datetime, instrument)")
_ns = list(_df.index.names)
if set(_ns) == {"datetime", "instrument"} and _ns != ["datetime", "instrument"]:
    _df = _df.swaplevel("datetime", "instrument")
_df = _df.sort_index()
if len(_df.columns) == 1 and _df.columns[0] != _FN:
    _df = _df.rename(columns={_df.columns[0]: _FN})

# Clip to window
_dates = _pd.to_datetime(_df.index.get_level_values(0))
_df = _df[(_dates >= _pd.Timestamp(_SD)) & (_dates <= _pd.Timestamp(_ED))]
if _df.empty:
    raise RuntimeError(f"no data in window: {_SD}~{_ED}")

# Merge with existing cache (extend mode)
if _ME and _os.path.exists(_CP):
    _ex = _pd.read_parquet(_CP)
    if "value" in _ex.columns:
        _ex = _ex.rename(columns={"value": _FN})
    if _EE:
        _cutoff = _pd.Timestamp(_EE)
        _new = _df.loc[_df.index.get_level_values(0) > _cutoff]
        if _new.empty:
            _df = _ex
        else:
            _df = _pd.concat([_ex, _new])
            del _new
    else:
        _df = _pd.concat([_ex, _df])
    del _ex
    if not _df.index.is_monotonic_increasing:
        _df = _df.sort_index()
    _df = _df[~_df.index.duplicated(keep="last")]

# Write cache parquet
_dir = _os.path.dirname(_CP)
if _dir:
    _os.makedirs(_dir, exist_ok=True)
_save = _df.rename(columns={_df.columns[0]: "value"})
_save.to_parquet(_CP, engine="pyarrow", compression="snappy")

# Write metadata JSON (for main process to read — no DataFrame needed)
_ad = _df.index.get_level_values(0)
_meta = {
    "success": True,
    "num_rows": len(_df),
    "date_range": "{}~{}".format(
        _ad.min().strftime("%Y-%m-%d"),
        _ad.max().strftime("%Y-%m-%d"),
    ),
}
with open("_result_meta.json", "w") as _f:
    _json.dump(_meta, _f)
'''

def _build_pipeline_wrapper(
    factor_name: str,
    start_date: str,
    end_date: str,
    cache_parquet_path: str,
    merge_existing: bool,
    existing_end_date: Optional[str],
) -> str:
    """Build the subprocess pipeline wrapper with actual parameter values."""
    return (
        _PIPELINE_WRAPPER
        .replace("__FN__", repr(factor_name))
        .replace("__SD__", repr(start_date))
        .replace("__ED__", repr(end_date))
        .replace("__CP__", repr(cache_parquet_path))
        .replace("__ME__", repr(merge_existing))
        .replace("__EE__", repr(existing_end_date or ""))
    )


def _execute_factor_subprocess(
    *,
    factor_name: str,
    code_text: str,
    factor_data_dir: str,
    work_dir: str,
    cache_parquet_path: str,
    start_date: str,
    end_date: str,
    merge_existing: bool = False,
    existing_end_date: Optional[str] = None,
    timeout: int = 0,
) -> dict:
    """通过 subprocess 执行因子计算 + 全部后处理，直接写入缓存 parquet。

    主进程不持有任何 DataFrame。所有内存密集操作在子进程中完成：
    1. 因子计算 → result.h5
    2. result.h5 → normalize → clip → merge → 写入缓存 parquet
    3. 写入 _result_meta.json（轻量元数据）
    子进程完成后退出，其内存自动释放。

    Returns: 轻量元数据 dict {num_rows, date_range}
    """
    _timeout = timeout or int(os.environ.get("AISTOCK_FACTOR_TIMEOUT", "1800"))
    factor_dir = os.path.join(work_dir, f"_factor_{factor_name}")
    if os.path.isdir(factor_dir):
        shutil.rmtree(factor_dir)
    os.makedirs(factor_dir, exist_ok=True)
    _link_allowed_files(factor_data_dir, factor_dir)
    _link_allowed_files(factor_data_dir, work_dir)

    # 追加全流程 pipeline wrapper 到因子代码末尾
    wrapper = _build_pipeline_wrapper(
        factor_name, start_date, end_date, cache_parquet_path,
        merge_existing, existing_end_date,
    )
    factor_py = os.path.join(factor_dir, "factor.py")
    with open(factor_py, "w", encoding="utf-8") as f:
        f.write(code_text)
        f.write(wrapper)

    try:
        subprocess.check_output(
            [sys.executable, "factor.py"],
            cwd=factor_dir,
            stderr=subprocess.STDOUT,
            timeout=_timeout,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"因子执行超时(>{_timeout}s)")
    except subprocess.CalledProcessError as e:
        stderr_text = e.output.decode("utf-8", errors="replace")[-2000:] if e.output else ""
        raise RuntimeError(f"因子执行失败(exit={e.returncode}): {stderr_text}")

    # 读取轻量元数据 JSON（主进程不持有 DataFrame）
    meta_path = os.path.join(factor_dir, "_result_meta.json")
    if os.path.isfile(meta_path):
        with open(meta_path, "r") as f:
            return json.load(f)
    else:
        raise RuntimeError("factor.py 执行完成但未生成 _result_meta.json")


def _execute_factor_via_backtest_data(
    *,
    factor_name: str,
    code_text: Optional[str] = None,
    code_path: Optional[Path] = None,
    factor_data_dir: str,
    start_date: str,
    end_date: str,
    timeout: int,
    merge_existing: bool,
    existing_end_date: Optional[str],
    audit_context: dict[str, Any],
) -> FactorComputeResult:
    """执行因子计算并写入缓存。主进程不持有任何 DataFrame。"""
    t0 = time.time()
    # source hash
    if code_text is not None:
        source_hash_raw = hashlib.sha256(code_text.encode("utf-8")).hexdigest()[:16]
    elif code_path is not None:
        source_hash_raw = compute_source_hash_raw(code_path)
        code_text = code_path.read_text(encoding="utf-8")
    else:
        return FactorComputeResult(
            factor_name=factor_name, success=False,
            error="code_text 和 code_path 均为空",
            error_short="无因子代码", error_type="ValueError",
        )
    parquet_path = SINGLE_DIR / f"{factor_name}.parquet"

    with tempfile.TemporaryDirectory(prefix=f"factor_cache_{factor_name}_") as tmpdir:
        try:
            meta = _execute_factor_subprocess(
                factor_name=factor_name,
                code_text=code_text,
                factor_data_dir=factor_data_dir,
                work_dir=tmpdir,
                cache_parquet_path=str(parquet_path),
                start_date=start_date,
                end_date=end_date,
                merge_existing=merge_existing,
                existing_end_date=existing_end_date,
                timeout=timeout,
            )
        except Exception as e:
            trace = traceback.format_exc()
            short = f"{type(e).__name__}: {e}"
            return FactorComputeResult(
                factor_name=factor_name,
                success=False,
                elapsed_sec=round(time.time() - t0, 1),
                error=short,
                error_short=short,
                error_type=type(e).__name__,
                traceback_full=trace,
            )

        # 从轻量元数据构建结果（主进程零 DataFrame）
        num_rows = meta.get("num_rows", 0)
        date_range = meta.get("date_range", "")
        result = FactorComputeResult(
            factor_name=factor_name,
            success=True,
            num_rows=num_rows,
            date_range=date_range,
            elapsed_sec=round(time.time() - t0, 1),
        )
        result.meta_entry = {
            "status": "ok",
            "computed_at": datetime.now().isoformat(),
            "rows": num_rows,
            "date_range": date_range,
            "as_of_date": end_date,
            "source_hash_raw": source_hash_raw,
            "data_source_mode": audit_context["data_source_mode"],
            "factor_data_dir": audit_context["factor_data_dir"],
            "window_train_start": audit_context["window_train_start"],
            "window_backtest_end": audit_context["window_backtest_end"],
        }
        result.meta_as_of_date = end_date
        return result


def run_plan_action(
    plan: dict,
    name: str,
    code_text: Optional[str],
    code_path: Optional[Path],
    start: str,
    end: str,
    timeout: int,
    audit_context: dict[str, Any],
) -> FactorComputeResult:
    if plan["action"] == "extend_forward":
        cached_end = plan.get("cached_end")
        if not cached_end:
            return FactorComputeResult(
                factor_name=name,
                success=False,
                error="cached_end missing for extend_forward",
                error_short="cached_end missing for extend_forward",
                error_type="InvalidPlan",
            )
        buffer_start = (
            datetime.strptime(cached_end, "%Y-%m-%d") - timedelta(days=90)
        ).strftime("%Y-%m-%d")
        calc_start = max(start, buffer_start)
        return _execute_factor_via_backtest_data(
            factor_name=name,
            code_text=code_text,
            code_path=code_path,
            factor_data_dir=audit_context["factor_data_dir"],
            start_date=calc_start,
            end_date=end,
            timeout=timeout,
            merge_existing=True,
            existing_end_date=cached_end,
            audit_context=audit_context,
        )

    return _execute_factor_via_backtest_data(
        factor_name=name,
        code_text=code_text,
        code_path=code_path,
        factor_data_dir=audit_context["factor_data_dir"],
        start_date=start,
        end_date=end,
        timeout=timeout,
        merge_existing=False,
        existing_end_date=None,
        audit_context=audit_context,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--factor", help="只回填指定因子（用于测试）")
    ap.add_argument("--factors", help="逗号分隔的因子名列表（替代 --factor）")
    ap.add_argument("--experiment-id", help="实验 ID，用于解析回测窗口和数据目录")
    ap.add_argument("--factor-data-dir", help="显式指定回测数据目录（WSL 路径）")
    ap.add_argument("--node-id", help="节点 ID，用于解析节点 factor_data_dir")
    ap.add_argument("--start", default=None, help="缓存窗口起始日期")
    ap.add_argument("--end", default=None, help="缓存窗口结束日期")
    ap.add_argument("--window-train-start", default=None, help="服务端解析后的窗口下界")
    ap.add_argument("--window-backtest-end", default=None, help="服务端解析后的窗口上界")
    ap.add_argument("--workers", type=int, default=1, help="并发 worker 数（默认 1，串行）")
    ap.add_argument("--timeout", type=int, default=0, help="单因子超时秒数(默认1800, 与回测一致)")
    ap.add_argument("--force", action="store_true", help="忽略已覆盖的缓存，强制回填")
    ap.add_argument("--dry-run", action="store_true", help="只打印计划，不执行")
    ap.add_argument("--limit", type=int, help="最多处理多少个因子（测试用）")
    ap.add_argument("--incremental", action="store_true", help="增量模式：优先仅补齐缺失后段")
    ap.add_argument("--json-output", help="结果写入 JSON 文件（供后端读取）")
    ap.add_argument("--task-id", help="外部指定任务 ID")
    ap.add_argument("--resume-task-id", help="从历史任务状态恢复未完成因子")
    ap.add_argument("--retry-failed-only", action="store_true", help="恢复历史任务时仅重试失败因子")
    ap.add_argument("--strict-backtest-data", action="store_true", help="要求严格使用回测数据目录")
    ap.add_argument("--code-manifest", help="JSON 文件路径，包含 {factor_name: code_text}，使用原始代码+subprocess 执行")
    args = ap.parse_args()

    task_id = args.task_id or f"cache_local_{int(time.time() * 1000)}"
    ensure_task_dir()
    audit_context = resolve_execution_context(args)
    start_date = audit_context["window_train_start"]
    end_date = audit_context["window_backtest_end"]

    print("=" * 60)
    print(f"Factor Cache Backfill: {start_date} ~ {end_date}")
    print(f"Task ID: {task_id}")
    print(f"Data mode: {audit_context['data_source_mode']}")
    print(f"Factor data dir: {audit_context['factor_data_dir']}")
    print("=" * 60)

    meta = load_meta()

    # ── 因子代码来源: 必须通过 --code-manifest 提供原始 code_text ──
    code_texts: dict[str, str] = {}
    code_files: dict[str, Path] = {}

    if args.code_manifest:
        manifest_path = Path(args.code_manifest)
        if not manifest_path.exists():
            print(f"[ERROR] code manifest 不存在: {manifest_path}")
            sys.exit(1)
        try:
            manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            print(f"[ERROR] code manifest JSON 解析失败: {e}")
            sys.exit(1)
        if not isinstance(manifest_data, dict) or not manifest_data:
            print(f"[ERROR] code manifest 为空或格式错误 (期望 dict)")
            sys.exit(1)
        code_texts = manifest_data
        print(f"  代码模式: 原始 code_text (subprocess), {len(code_texts)} 因子")
        code_files = {name: Path("<manifest>") for name in code_texts}
    else:
        print("[ERROR] 必须通过 --code-manifest 提供原始因子代码")
        print("  用法: python backfill_factor_cache.py --code-manifest manifest.json ...")
        sys.exit(1)

    targets = resolve_targets(args, code_files)

    plan_items: list[dict] = []
    skipped_items: list[dict] = []
    action_counts: dict[str, int] = {}

    for name in sorted(targets.keys()):
        plan = plan_factor_action(
            name,
            start_date,
            end_date,
            meta,
            incremental=args.incremental,
            force=args.force,
        )
        action_counts[plan["action"]] = action_counts.get(plan["action"], 0) + 1
        if plan["action"] == "skip":
            skipped_items.append({
                "name": name,
                "reason": plan["reason"],
                "cached_range": meta.get("factors", {}).get(name, {}).get("date_range"),
            })
            continue
        plan_items.append({
            "name": name,
            "action": plan["action"],
            "reason": plan["reason"],
            "cached_range": meta.get("factors", {}).get(name, {}).get("date_range"),
            "cached_end": plan.get("cached_end"),
        })

    if args.limit:
        plan_items = plan_items[:args.limit]
        print(f"  [--limit] 裁剪为: {len(plan_items)}")

    state = {
        "task_id": task_id,
        "status": "running",
        "started_at": now_iso(),
        "finished_at": None,
        "experiment_id": audit_context["experiment_id"],
        "factor_data_dir": audit_context["factor_data_dir"],
        "data_source_mode": audit_context["data_source_mode"],
        "strict_backtest_data": audit_context["strict_backtest_data"],
        "window_train_start": start_date,
        "window_backtest_end": end_date,
        "start_date": start_date,
        "end_date": end_date,
        "workers": args.workers,
        "incremental": args.incremental,
        "force": args.force,
        "resume_task_id": args.resume_task_id,
        "retry_failed_only": args.retry_failed_only,
        "requested_factors": sorted(targets.keys()),
        "planned_factors": [item["name"] for item in plan_items],
        "plan_items": plan_items,
        "plan_summary": action_counts,
        "skipped_factors": skipped_items,
        "success_factors": [],
        "failed_factors": [],
    }
    save_task_state(state)

    print(f"  代码因子总数: {len(targets)}")
    print(f"  已覆盖跳过: {len(skipped_items)}")
    print(f"  需要执行: {len(plan_items)}")
    print(f"  动作分布: {action_counts}")

    if not plan_items:
        print("所有因子已被覆盖，无需回填。")
        state["status"] = "completed"
        state["finished_at"] = now_iso()
        save_task_state(state)
        result = {
            "ok": True,
            "task_id": task_id,
            "total": 0,
            "success": 0,
            "failed": 0,
            "skipped": len(skipped_items),
            "failed_factors": [],
            "start": start_date,
            "end": end_date,
            "workers": args.workers,
            "experiment_id": audit_context["experiment_id"],
            "factor_data_dir": audit_context["factor_data_dir"],
            "data_source_mode": audit_context["data_source_mode"],
            "task_state_path": str(task_state_path(task_id)),
            "failed_log_path": str(failed_log_path(task_id)),
        }
        if args.json_output:
            Path(args.json_output).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        return

    print("\n待执行因子 (前 20):")
    for item in plan_items[:20]:
        print(f"  {item['name']}: {item['action']} ({item['reason']})")

    if args.dry_run:
        print("\n[dry-run] 退出")
        state["status"] = "dry_run"
        state["finished_at"] = now_iso()
        save_task_state(state)
        return

    print("\n开始回填...")
    t_start = time.time()
    success_cnt = 0
    failed_items: list[dict] = []

    def handle_result(name: str, code_path: Path, plan: dict, result: FactorComputeResult):
        nonlocal success_cnt
        if result.success:
            success_cnt += 1
            if result.meta_entry:
                meta_now = load_meta()
                meta_now.setdefault("factors", {})[name] = result.meta_entry
                if result.meta_as_of_date:
                    meta_now["as_of_date"] = result.meta_as_of_date
                save_meta(meta_now)
            state["success_factors"].append({
                "name": name,
                "action": plan["action"],
                "reason": plan["reason"],
                "rows": result.num_rows,
                "elapsed_sec": round(result.elapsed_sec, 2),
                "date_range": result.date_range,
            })
            save_task_state(state)
            return True

        error_short = getattr(result, "error_short", None) or result.error or "unknown error"
        error_type = getattr(result, "error_type", None)
        traceback_full = getattr(result, "traceback_full", None)
        # source hash: 从 code_text 或文件计算
        ct = code_texts.get(name)
        if ct is not None:
            src_hash = hashlib.sha256(ct.encode("utf-8")).hexdigest()[:16]
        else:
            src_hash = compute_source_hash_raw(code_path)

        # 写入 error status 到 _meta.json
        meta_now = load_meta()
        meta_now.setdefault("factors", {})[name] = {
            "status": "error",
            "computed_at": datetime.now().isoformat(),
            "error": error_short[:200],
            "error_type": error_type,
            "source_hash_raw": src_hash,
        }
        save_meta(meta_now)

        failed_item = {
            "name": name,
            "action": plan["action"],
            "reason": plan["reason"],
            "error": error_short,
            "error_type": error_type,
            "elapsed_sec": round(getattr(result, "elapsed_sec", 0.0), 2),
            "cached_range": plan.get("cached_range"),
        }
        failed_items.append(failed_item)
        state["failed_factors"].append(failed_item)
        append_failed_log(task_id, {
            "task_id": task_id,
            "experiment_id": audit_context["experiment_id"],
            "factor_name": name,
            "action": plan["action"],
            "reason": plan["reason"],
            "start_date": start_date,
            "end_date": end_date,
            "elapsed_sec": round(getattr(result, "elapsed_sec", 0.0), 2),
            "error_type": error_type,
            "error_short": error_short,
            "traceback_full": traceback_full,
            "code_path": str(code_path),
            "source_hash_raw": src_hash,
            "factor_data_dir": audit_context["factor_data_dir"],
            "data_source_mode": audit_context["data_source_mode"],
            "window_train_start": start_date,
            "window_backtest_end": end_date,
        })
        save_task_state(state)
        return False

    if args.workers == 1:
        for i, plan in enumerate(plan_items, 1):
            name = plan["name"]
            code_path = targets[name]
            code_text_val = code_texts.get(name)
            print(f"[{i}/{len(plan_items)}] {name} ({plan['action']} / {plan['reason']}) ... ", end="", flush=True)
            try:
                result = run_plan_action(plan, name, code_text_val, code_path if code_text_val is None else None, start_date, end_date, args.timeout, audit_context)
            except Exception as e:
                result = FactorComputeResult(
                    factor_name=name,
                    success=False,
                    error=f"exception: {e}",
                    error_short=f"exception: {e}",
                    error_type=type(e).__name__,
                )
            ok = handle_result(name, code_path, plan, result)
            if ok:
                print(f"OK ({summarize_success(result)})")
            else:
                print(f"FAIL: {(getattr(result, 'error_short', None) or result.error or 'unknown error')} ({getattr(result, 'elapsed_sec', 0.0):.1f}s)")
            # 每 20 个因子打印一次内存使用
            if i % 20 == 0:
                rss = _get_rss_mb()
                if rss > 0:
                    print(f"  [mem] RSS={rss:.0f}MB after {i} factors")
                gc.collect()
    else:
        futures = {}
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for plan in plan_items:
                name = plan["name"]
                code_path = targets[name]
                code_text_val = code_texts.get(name)
                fut = pool.submit(run_plan_action, plan, name, code_text_val, code_path if code_text_val is None else None, start_date, end_date, args.timeout, audit_context)
                futures[fut] = (plan, code_path)

            for i, fut in enumerate(as_completed(futures), 1):
                plan, code_path = futures[fut]
                name = plan["name"]
                try:
                    result = fut.result()
                except Exception as e:
                    result = FactorComputeResult(
                        factor_name=name,
                        success=False,
                        error=f"ThreadPool error: {e}",
                        error_short=f"ThreadPool error: {e}",
                        error_type=type(e).__name__,
                    )
                ok = handle_result(name, code_path, plan, result)
                if ok:
                    print(f"[{i}/{len(plan_items)}] {name} OK ({summarize_success(result)})")
                else:
                    print(f"[{i}/{len(plan_items)}] {name} FAIL: {(getattr(result, 'error_short', None) or result.error or 'unknown error')}")
                del result
                # 每 20 个因子打印内存 + 强制 GC
                if i % 20 == 0:
                    rss = _get_rss_mb()
                    if rss > 0:
                        print(f"  [mem] RSS={rss:.0f}MB after {i}/{len(plan_items)} factors")
                    gc.collect()

    meta = load_meta()
    meta["_last_backfill"] = {
        "completed_at": now_iso(),
        "start": start_date,
        "end": end_date,
        "total_success": success_cnt,
        "total_failed": len(failed_items),
        "task_id": task_id,
        "incremental": args.incremental,
        "resume_task_id": args.resume_task_id,
        "experiment_id": audit_context["experiment_id"],
        "factor_data_dir": audit_context["factor_data_dir"],
        "data_source_mode": audit_context["data_source_mode"],
    }
    save_meta(meta)

    total_elapsed = time.time() - t_start
    state["status"] = "completed" if not failed_items else "failed"
    state["finished_at"] = now_iso()
    save_task_state(state)

    print("\n" + "=" * 60)
    print(f"回填完成: {success_cnt}/{len(plan_items)} 成功, {len(failed_items)} 失败, {len(skipped_items)} 跳过")
    print(f"总耗时: {total_elapsed / 60:.1f} 分钟")
    if failed_items:
        print("\n失败因子:")
        for item in failed_items[:20]:
            print(f"  {item['name']}: {item['error']}")
        if len(failed_items) > 20:
            print(f"  ... 还有 {len(failed_items) - 20} 个")
    print("=" * 60)

    if args.json_output:
        result = {
            "ok": True,
            "task_id": task_id,
            "total": len(plan_items),
            "success": success_cnt,
            "failed": len(failed_items),
            "skipped": len(skipped_items),
            "failed_factors": failed_items,
            "elapsed_sec": round(total_elapsed, 1),
            "start": start_date,
            "end": end_date,
            "workers": args.workers,
            "incremental": args.incremental,
            "resume_task_id": args.resume_task_id,
            "retry_failed_only": args.retry_failed_only,
            "experiment_id": audit_context["experiment_id"],
            "factor_data_dir": audit_context["factor_data_dir"],
            "data_source_mode": audit_context["data_source_mode"],
            "task_state_path": str(task_state_path(task_id)),
            "failed_log_path": str(failed_log_path(task_id)),
        }
        try:
            Path(args.json_output).write_text(
                json.dumps(result, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            print(f"结果已写入: {args.json_output}")
        except Exception as e:
            print(f"[ERROR] 写入 JSON 失败: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
