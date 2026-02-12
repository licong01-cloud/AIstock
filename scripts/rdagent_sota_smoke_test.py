from __future__ import annotations

import json
import sys
import time
from typing import Any, Dict, List, Optional

import requests

RESULTS_API_BASE = "http://127.0.0.1:9000"
AISTOCK_API_BASE = "http://127.0.0.1:8001/api/v1"
TASK_API_PREFIXES = ["", "/api/v1/rdagent", "/rdagent", "/api/v1"]


def _get_json(base: str, path: str, *, timeout_s: float = 60.0) -> Dict[str, Any]:
    url = f"{base}{path}"
    resp = requests.get(url, timeout=timeout_s)
    resp.raise_for_status()
    return resp.json()


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


def _collect_sota_tasks(limit: int = 50) -> List[str]:
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


def _sync_task(task_id: str, *, timeout_s: float = 600.0, retries: int = 2) -> Dict[str, Any]:
    payload = {"task_ids": [task_id], "operator": "script"}
    last_exc: Optional[Exception] = None
    for _ in range(retries + 1):
        try:
            resp = requests.post(f"{AISTOCK_API_BASE}/rdagent/tasks/sync", json=payload, timeout=timeout_s)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            continue
    if last_exc:
        raise last_exc
    raise RuntimeError("sync failed")


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
        if t:
            tasks.append(t)
    return tasks


def main() -> int:
    cutoff_date = "2026-01-18"
    top_k = 50
    success_target = 2
    max_candidates = 60

    cli_tasks = _parse_cli_tasks()
    if cli_tasks:
        candidate_tasks = cli_tasks
    else:
        candidate_tasks = _collect_sota_tasks(limit=max_candidates)

    print("Candidate SOTA tasks:", json.dumps(candidate_tasks, ensure_ascii=False))
    if len(candidate_tasks) < success_target:
        print(f"ERROR: SOTA task数量不足{success_target}个，无法继续。")
        return 2

    failures: List[Dict[str, Any]] = []
    successes: List[str] = []
    for tid in candidate_tasks:
        if len(successes) >= success_target:
            break
        try:
            print(f"[START] sync task={tid}")
            sync_start = time.time()
            sync_resp = _sync_task(tid)
            sync_cost = time.time() - sync_start
            print("SYNC:", tid, f"{sync_cost:.2f}s", json.dumps(sync_resp, ensure_ascii=False))

            # 若同步失败，直接记录并跳过选股
            if isinstance(sync_resp, dict):
                results = sync_resp.get("results") if isinstance(sync_resp.get("results"), list) else []
                if results:
                    first = results[0] if isinstance(results[0], dict) else {}
                    if not first.get("ok", False) or first.get("sync_status") != "success":
                        payload = {
                            "task_id": tid,
                            "stage": "sync_failed",
                            "error": first.get("error"),
                            "diagnostics": first.get("diagnostics"),
                        }
                        failures.append(payload)
                        print("FAIL:", json.dumps(payload, ensure_ascii=False))
                        continue

            print(f"[START] selection task={tid} cutoff={cutoff_date} top_k={top_k}")
            sel_start = time.time()
            sel_resp = _select_task(tid, cutoff_date=cutoff_date, top_k=top_k)
            sel_cost = time.time() - sel_start
            print("SELECTION:", tid, f"{sel_cost:.2f}s", json.dumps(sel_resp, ensure_ascii=False))
            successes.append(tid)
        except (requests.Timeout, TimeoutError) as exc:
            payload = {"task_id": tid, "stage": "timeout", "error": str(exc)}
            failures.append(payload)
            print("FAIL:", json.dumps(payload, ensure_ascii=False))
        except requests.ConnectionError as exc:
            payload = {"task_id": tid, "stage": "connection", "error": str(exc)}
            failures.append(payload)
            print("FAIL:", json.dumps(payload, ensure_ascii=False))
        except requests.HTTPError as exc:
            detail = None
            try:
                detail = exc.response.text
            except Exception:
                detail = None
            payload = {"task_id": tid, "stage": "http", "error": str(exc), "detail": str(detail) if detail is not None else None}
            failures.append(payload)
            print("FAIL:", json.dumps(payload, ensure_ascii=False))
        except Exception as exc:
            payload = {"task_id": tid, "stage": "unknown", "error": str(exc)}
            failures.append(payload)
            print("FAIL:", json.dumps(payload, ensure_ascii=False))
        finally:
            print(f"[DONE] task={tid} success={tid in successes}")

    print("SUCCESS_TASKS:", json.dumps(successes, ensure_ascii=False))
    if len(successes) < success_target:
        print("FAILURES:", json.dumps(failures, ensure_ascii=False))
        return 1

    print("OK: selected tasks synced and selected successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
