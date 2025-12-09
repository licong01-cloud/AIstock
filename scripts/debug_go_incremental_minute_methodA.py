from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict

import requests

BACKEND_BASE = os.getenv("BACKEND_BASE", "http://localhost:8001").rstrip("/")


def backend_request(method: str, path: str, **kwargs: Any) -> Any:
    url = BACKEND_BASE + path
    resp = requests.request(method, url, timeout=30, **kwargs)
    resp.raise_for_status()
    if "application/json" in resp.headers.get("Content-Type", ""):
        return resp.json()
    return resp.text


def trigger_incremental(start_date: str = "2025-11-28", workers: int = 1) -> str:
    """通过 /api/ingestion/incremental 触发 kline_minute_raw 增量任务。"""
    payload: Dict[str, Any] = {
        "data_kind": "kline_minute_raw",
        "start_date": start_date,
        "workers": workers,
    }
    print(f"[REQ] POST /api/ingestion/incremental -> {json.dumps(payload, ensure_ascii=False)}")
    data = backend_request(
        "POST",
        "/api/ingestion/incremental",
        json=payload,
    )
    job_id = data.get("job_id") or data.get("jobId")
    run_id = data.get("run_id") or data.get("runId")
    print(f"[RESP] job_id={job_id}, run_id={run_id}")
    if not job_id:
        raise RuntimeError(f"no job_id in response: {data}")
    return str(job_id)


def poll_job(job_id: str, interval: float = 5.0, max_wait: float = 600.0) -> Dict[str, Any]:
    """轮询 /api/ingestion/job/{job_id}，直到任务结束或超时。"""
    print(f"[POLL] watching job {job_id} ...")
    waited = 0.0
    last_status = None
    while waited < max_wait:
        try:
            data = backend_request("GET", f"/api/ingestion/job/{job_id}")
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] poll job failed: {exc}")
            time.sleep(interval)
            waited += interval
            continue
        status = (data.get("status") or "").lower()
        summary = data.get("summary") or {}
        if isinstance(summary, str):
            try:
                summary = json.loads(summary)
            except Exception:
                pass
        if status != last_status:
            print(f"[JOB] status={status}, summary={summary}")
            last_status = status
        if status in {"success", "failed", "cancelled", "canceled"}:
            print(f"[DONE] job finished with status={status}")
            return data
        time.sleep(interval)
        waited += interval
    print(f"[TIMEOUT] job {job_id} did not finish within {max_wait} seconds")
    return {}


def fetch_logs(job_id: str, limit: int = 200) -> None:
    """拉取该 job 的 ingestion 日志，打印最近若干条。"""
    print(f"[LOG] fetching last {limit} logs for job {job_id} ...")
    try:
        data = backend_request(
            "GET",
            f"/api/ingestion/logs?job_id={job_id}&limit={limit}&offset=0",
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] fetch logs failed: {exc}")
        return
    items = data.get("items") or []
    print(f"[LOG] got {len(items)} log rows")
    for row in items:
        ts = row.get("ts")
        level = row.get("level")
        msg = row.get("message")
        print(f"  [{ts}] {level}: {msg}")


def main() -> None:
    start_date = "2025-11-28"
    if len(sys.argv) >= 2:
        start_date = sys.argv[1]
    print(f"[INFO] BACKEND_BASE={BACKEND_BASE}")
    print(f"[INFO] start_date={start_date}")

    try:
        job_id = trigger_incremental(start_date=start_date, workers=1)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] failed to trigger incremental: {exc}")
        return

    job_data = poll_job(job_id)
    fetch_logs(job_id)
    print("[INFO] final job payload:")
    print(json.dumps(job_data, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
