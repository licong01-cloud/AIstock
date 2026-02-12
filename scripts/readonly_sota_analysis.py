import json
import pickle
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


DEFAULT_TASKS = [
    "2026-01-03_03-53-51-394540",
    "2026-01-01_07-10-05-716729",
]


def _safe_read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return None


def _load_model_feature_names(model_path: Path) -> Optional[List[str]]:
    try:
        with model_path.open("rb") as f:
            obj = pickle.load(f)
    except Exception:
        return None
    try:
        booster = getattr(obj, "model", None)
        fn = getattr(booster, "feature_name", None)
        feats = fn() if callable(fn) else None
        if feats:
            return list(feats)
    except Exception:
        pass
    try:
        fn = getattr(obj, "feature_name", None)
        feats = fn() if callable(fn) else None
        if feats:
            return list(feats)
    except Exception:
        pass
    return None


def _extract_filter_cols(model_meta: Optional[Dict[str, Any]]) -> List[str]:
    if not model_meta:
        return []
    try:
        infer_processors = (
            model_meta.get("dataset_conf", {})
            .get("kwargs", {})
            .get("handler", {})
            .get("kwargs", {})
            .get("infer_processors", [])
        )
    except Exception:
        infer_processors = []
    for proc in infer_processors or []:
        if proc.get("class") == "FilterCol":
            return list(proc.get("kwargs", {}).get("col_list", []) or [])
    return []


def analyze_task(task_id: str, assets_root: Path) -> Dict[str, Any]:
    task_dir = (assets_root / task_id).resolve()
    manifest = _safe_read_json(task_dir / "manifest.json") or {}
    workspace_dir = (
        manifest.get("task_only", {}).get("workspace", {}).get("workspace_dir")
        if isinstance(manifest, dict)
        else None
    )
    factor_meta_path = Path(workspace_dir) / "factor_meta.json" if workspace_dir else None
    factor_meta = _safe_read_json(factor_meta_path) if factor_meta_path else None
    model_meta = _safe_read_json(task_dir / "model_meta.json")

    factors = [f.get("name") for f in (factor_meta or {}).get("factors", []) if isinstance(f, dict)]
    factor_base_names = [x.replace("feature_", "", 1) if x else x for x in factors]
    filter_cols = _extract_filter_cols(model_meta)

    combined_cols: List[str] = []
    combined_path = task_dir / "combined_factors_df.parquet"
    try:
        df = pd.read_parquet(combined_path)
        combined_cols = list(df.columns)
    except Exception:
        combined_cols = []

    model_feats = _load_model_feature_names(task_dir / "model.pkl")

    missing_in_combined = [x for x in factor_base_names if x and x not in combined_cols]
    in_combined = [x for x in factor_base_names if x and x in combined_cols]
    in_filter = [x for x in factor_base_names if x and x in filter_cols]

    return {
        "task_id": task_id,
        "workspace_dir": workspace_dir,
        "factor_meta_factors": factors,
        "factor_base_names": factor_base_names,
        "model_meta_filter_cols_len": len(filter_cols),
        "model_meta_filter_cols_head": filter_cols[:20],
        "combined_cols_len": len(combined_cols),
        "combined_cols_head": combined_cols[:20],
        "model_feature_len": None if model_feats is None else len(model_feats),
        "model_feature_head": [] if not model_feats else model_feats[:20],
        "sota_in_combined": in_combined,
        "sota_missing_in_combined": missing_in_combined,
        "sota_in_filter_cols": in_filter,
    }


def main() -> int:
    task_ids = [x for x in sys.argv[1:] if x.strip()] or DEFAULT_TASKS
    assets_root = Path(r"f:\Dev\AIstock\rdagent_assets\rdagent_tasks")
    reports = [analyze_task(tid, assets_root) for tid in task_ids]
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
