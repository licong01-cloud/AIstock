"""
Step 1: 在 WSL 中从 pkl 提取因子指标 (输出 JSON)
"""
import argparse
import glob
import json
import os
import pickle
import re
import sys
from pathlib import Path

LOG_ROOT_ENV_KEYS = ("RDAGENT_LOG_ROOT", "QLIB_RDAGENT_LOG_ROOT")


def _load_project_env():
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    repo_root = Path(__file__).resolve().parents[2]
    candidates = []
    if os.environ.get("AISTOCK_ENV_FILE"):
        candidates.append(Path(os.environ["AISTOCK_ENV_FILE"]))
    candidates.append(repo_root / ".env")
    dotgit = repo_root / ".git"
    if dotgit.is_file():
        text = dotgit.read_text(encoding="utf-8", errors="ignore").strip()
        if text.lower().startswith("gitdir:"):
            gitdir = Path(text.split(":", 1)[1].strip())
            if not gitdir.is_absolute():
                gitdir = (repo_root / gitdir).resolve()
            parts = list(gitdir.parts)
            if ".git" in parts:
                candidates.append(Path(*parts[: parts.index(".git") + 1]).parent / ".env")
    for env_file in candidates:
        if env_file.exists():
            override = env_file == candidates[0] and os.environ.get("AISTOCK_ENV_FILE")
            load_dotenv(env_file, override=bool(override))
            return


def resolve_log_root(explicit=None):
    if explicit:
        return explicit
    _load_project_env()
    for name in LOG_ROOT_ENV_KEYS:
        value = (os.environ.get(name) or "").strip()
        if value:
            return value
    root = (os.environ.get("QLIB_RDAGENT_ROOT_WSL") or "").strip()
    if root:
        return os.path.join(root.rstrip("/"), "log")
    raise RuntimeError(
        "RD-Agent log root is not configured; set RDAGENT_LOG_ROOT, "
        "QLIB_RDAGENT_LOG_ROOT, or QLIB_RDAGENT_ROOT_WSL."
    )

FACTORS = {
    "LogCircMV": "2026-01-10_07-31-04-239738",
    "MainNetAmtRatio_5D": "2026-01-10_07-31-04-239738",
    "MainNetInflowRatio_1D": "2026-01-13_06-56-49-446055",
    "Momentum_10D": "2026-01-13_06-56-49-446055",
}

def normalize_factor_name(name):
    m = re.search(r'[（(]([A-Za-z_][A-Za-z0-9_]*)[）)]', name)
    return m.group(1) if m else name

def extract_metrics(task_id, target_factors, log_root=None):
    task_dir = os.path.join(log_root or resolve_log_root(), task_id)
    session_files = sorted(glob.glob(os.path.join(task_dir, "__session__/*/*")))
    if not session_files:
        return {}

    with open(session_files[-1], "rb") as f:
        data = pickle.load(f)

    hist = data.trace.hist
    results = {}

    for loop_idx, entry in enumerate(hist):
        exp = entry[0] if isinstance(entry, tuple) else entry
        factor_names_raw = []
        if hasattr(exp, "sub_workspace_list") and exp.sub_workspace_list:
            for sw in exp.sub_workspace_list:
                if hasattr(sw, "target_task"):
                    name = getattr(sw.target_task, "factor_name", None) or getattr(sw.target_task, "name", "?")
                    factor_names_raw.append(name)

        name_map = {normalize_factor_name(n): n for n in factor_names_raw}
        matched = [f for f in target_factors if f in name_map and f not in results]
        if not matched:
            continue

        result = getattr(exp, "result", None)
        metrics = {}
        if result is not None:
            if hasattr(result, "to_dict") and callable(result.to_dict):
                metrics = result.to_dict()
            elif isinstance(result, dict):
                metrics = result

        if not metrics:
            continue

        for fname in matched:
            entry_data = {
                "ic": metrics.get("IC"),
                "icir": metrics.get("ICIR"),
                "annualized_return": metrics.get("1day.excess_return_without_cost.annualized_return"),
                "max_drawdown": metrics.get("1day.excess_return_without_cost.max_drawdown"),
                "information_ratio": metrics.get("1day.excess_return_without_cost.information_ratio"),
                "source_loop_id": loop_idx,
                "source_loop_tag": str(loop_idx),
            }
            # Convert any non-serializable values
            for k, v in entry_data.items():
                if v is not None and not isinstance(v, (int, float, str)):
                    entry_data[k] = float(v)
            results[fname] = entry_data
            print(f"  [FOUND] {fname} @ Loop {loop_idx}: IC={entry_data['ic']}, ann_ret={entry_data['annualized_return']}", file=sys.stderr)

    return results

def main(argv=None):
    parser = argparse.ArgumentParser(description="Extract RD-Agent factor metrics from WSL pickle logs")
    parser.add_argument("--log-root", default=None, help="RD-Agent log root; defaults to env or QLIB_RDAGENT_ROOT_WSL/log")
    args = parser.parse_args(argv)

    log_root = resolve_log_root(args.log_root)
    all_metrics = {}
    task_factors = {}
    for fname, tid in FACTORS.items():
        task_factors.setdefault(tid, []).append(fname)

    for task_id, factors in task_factors.items():
        print(f"--- Task: {task_id} ---", file=sys.stderr)
        m = extract_metrics(task_id, factors, log_root)
        all_metrics.update(m)

    print(json.dumps(all_metrics, ensure_ascii=False))


if __name__ == "__main__":
    main()
