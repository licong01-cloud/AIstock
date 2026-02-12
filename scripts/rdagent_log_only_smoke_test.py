from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import importlib

import requests

AISTOCK_API_BASE = "http://127.0.0.1:8001/api/v1"
RESULTS_API_BASE = "http://127.0.0.1:9000"
TASK_API_PREFIXES = ["", "/api/v1/rdagent", "/rdagent", "/api/v1"]
DEFAULT_TASKS = [
    "2026-01-03_03-53-51-394540",
    "2026-01-01_07-10-05-716729",
]


def _sync_task_log_only(task_id: str) -> Dict[str, Any]:
    project_root = Path(__file__).resolve().parents[1]
    _load_env_file(project_root / ".env")
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    svc = importlib.import_module("backend.services.rdagent_task_sync_service")
    rdagent_task_sync_service = getattr(svc, "rdagent_task_sync_service")
    result = rdagent_task_sync_service.sync_task_from_log(task_id=task_id, operator="script:log_only_test")
    return {
        "ok": bool(result.ok),
        "task_id": result.task_id,
        "sync_status": result.sync_status,
        "task_dir": result.task_dir,
        "manifest_path": result.manifest_path,
        "error": result.error,
        "diagnostics": result.diagnostics,
    }


def _get_task_json(path: str, *, timeout_s: float = 60.0) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for prefix in TASK_API_PREFIXES:
        url = f"{RESULTS_API_BASE}{prefix}{path}"
        try:
            resp = requests.get(url, timeout=timeout_s)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            continue
    if last_exc:
        raise last_exc
    raise RuntimeError(f"task api not found: {path}")


def _list_tasks() -> List[Dict[str, Any]]:
    payload = _get_task_json("/tasks")
    for key in ("tasks", "items", "data"):
        if isinstance(payload.get(key), list):
            return payload[key]
    if isinstance(payload, list):
        return payload
    return []


def _extract_task_id(item: Any) -> Optional[str]:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        if item.get("task_id"):
            return str(item.get("task_id"))
        if item.get("id"):
            return str(item.get("id"))
    return None


def _collect_sota_tasks(limit: int = 5000) -> List[str]:
    tasks = _list_tasks()
    sota: List[str] = []
    for idx, item in enumerate(tasks, start=1):
        if idx % 10 == 0:
            print(f"scan tasks... {idx}/{len(tasks)}")
        tid = _extract_task_id(item)
        if not tid:
            continue
        try:
            anchor = _get_task_json(f"/tasks/{tid}/sota_factor_anchor")
        except Exception:
            continue
        factor_key = anchor.get("resolved_factor_entry_key")
        model_key = anchor.get("resolved_model_weight_key")
        model_meta_key = anchor.get("resolved_model_meta_key")
        if factor_key and model_key and model_meta_key:
            sota.append(tid)
        if len(sota) >= limit:
            break
    return sota


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists() or not env_path.is_file():
        return
    try:
        for raw in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip("\"'")
            if key and key not in os.environ:
                os.environ[key] = val
    except Exception:
        return


def _read_manifest(manifest_path: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        if not manifest_path.exists():
            return None, f"manifest_not_found: {manifest_path}"
        return json.loads(manifest_path.read_text(encoding="utf-8", errors="ignore")), None
    except Exception as exc:
        return None, str(exc)


def _check_required_assets(task_dir: Path, manifest_path: Path) -> Dict[str, Any]:
    manifest, err = _read_manifest(manifest_path)
    if err or not isinstance(manifest, dict):
        return {"ok": False, "error": err or "manifest_invalid"}

    primary_assets = manifest.get("primary_assets") if isinstance(manifest.get("primary_assets"), dict) else {}
    factor_rel = primary_assets.get("factor_entry_relpath")
    model_rel = primary_assets.get("model_weight_relpath")
    model_meta_rel = primary_assets.get("model_meta_relpath")
    combined_rel = primary_assets.get("combined_factors_relpath")

    def _exists(rel: Optional[str]) -> Tuple[Optional[str], bool]:
        if not rel:
            return None, False
        p = (task_dir / rel).resolve()
        return str(p), bool(p.exists() and p.is_file())

    factor_path, factor_ok = _exists(factor_rel)
    model_path, model_ok = _exists(model_rel)
    model_meta_path, model_meta_ok = _exists(model_meta_rel)
    combined_path, combined_ok = _exists(combined_rel)

    required_missing: List[str] = []
    if not factor_ok:
        required_missing.append("factor_entry")
    if not model_ok:
        required_missing.append("model_weight")
    if not model_meta_ok:
        required_missing.append("model_meta")
    if not combined_ok:
        required_missing.append("combined_factors")

    return {
        "ok": not required_missing,
        "missing": required_missing,
        "paths": {
            "factor_entry": factor_path,
            "model_weight": model_path,
            "model_meta": model_meta_path,
            "combined_factors": combined_path,
        },
    }


def _select_task(task_id: str, cutoff_date: str, top_k: int, *, timeout_s: float = 600.0) -> Dict[str, Any]:
    payload = {"cutoff_date": cutoff_date, "top_k": top_k}
    resp = requests.post(
        f"{AISTOCK_API_BASE}/rdagent/tasks/{task_id}/selection",
        json=payload,
        timeout=timeout_s,
    )
    resp.raise_for_status()
    return resp.json()


def _parse_cli_tasks() -> List[str]:
    tasks: List[str] = []
    for arg in sys.argv[1:]:
        t = str(arg or "").strip()
        if not t or t.startswith("--"):
            continue
        if t:
            tasks.append(t)
    return tasks


def _use_all_sota() -> bool:
    return any(str(arg).strip().lower() in {"--all", "--sota"} for arg in sys.argv[1:])


def main() -> int:
    cutoff_date = "2026-01-18"
    top_k = 50

    if _use_all_sota():
        tasks = _collect_sota_tasks()
    else:
        tasks = _parse_cli_tasks() or DEFAULT_TASKS
    print("LOG-ONLY tasks:", json.dumps(tasks, ensure_ascii=False))

    failures: List[Dict[str, Any]] = []
    successes: List[str] = []

    for tid in tasks:
        print(f"[START] log-only sync task={tid}")
        sync_start = time.time()
        sync_resp = _sync_task_log_only(tid)
        sync_cost = time.time() - sync_start
        print("SYNC:", tid, f"{sync_cost:.2f}s", json.dumps(sync_resp, ensure_ascii=False))

        if not sync_resp.get("ok"):
            failures.append({"task_id": tid, "stage": "sync_failed", "detail": sync_resp})
            print("FAIL:", json.dumps(failures[-1], ensure_ascii=False))
            continue

        task_dir = Path(sync_resp.get("task_dir") or "").resolve()
        manifest_path = Path(sync_resp.get("manifest_path") or "").resolve()
        asset_check = _check_required_assets(task_dir, manifest_path)
        print("ASSET_CHECK:", tid, json.dumps(asset_check, ensure_ascii=False))
        if not asset_check.get("ok"):
            failures.append({"task_id": tid, "stage": "asset_missing", "detail": asset_check})
            print("FAIL:", json.dumps(failures[-1], ensure_ascii=False))
            continue

        try:
            print(f"[START] selection task={tid} cutoff={cutoff_date} top_k={top_k}")
            sel_start = time.time()
            sel_resp = _select_task(tid, cutoff_date=cutoff_date, top_k=top_k)
            sel_cost = time.time() - sel_start
            print("SELECTION:", tid, f"{sel_cost:.2f}s", json.dumps(sel_resp, ensure_ascii=False))
            successes.append(tid)
        except requests.Timeout as exc:
            failures.append({"task_id": tid, "stage": "selection_timeout", "error": str(exc)})
            print("FAIL:", json.dumps(failures[-1], ensure_ascii=False))
        except requests.HTTPError as exc:
            detail = None
            try:
                detail = exc.response.text
            except Exception:
                detail = None
            failures.append({"task_id": tid, "stage": "selection_http", "error": str(exc), "detail": detail})
            print("FAIL:", json.dumps(failures[-1], ensure_ascii=False))
        except requests.ConnectionError as exc:
            failures.append({"task_id": tid, "stage": "selection_connection", "error": str(exc)})
            print("FAIL:", json.dumps(failures[-1], ensure_ascii=False))
        except Exception as exc:
            failures.append({"task_id": tid, "stage": "selection_unknown", "error": str(exc)})
            print("FAIL:", json.dumps(failures[-1], ensure_ascii=False))
        finally:
            print(f"[DONE] task={tid} success={tid in successes}")

    print("SUCCESS_TASKS:", json.dumps(successes, ensure_ascii=False))
    if failures:
        print("FAILURES:", json.dumps(failures, ensure_ascii=False))
    return 0 if len(successes) == len(tasks) else 1


if __name__ == "__main__":
    sys.exit(main())
