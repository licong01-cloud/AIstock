import os
import sys
import time
import json
import datetime as dt

import requests


def main() -> int:
    backend = os.getenv("BACKEND_BASE", "http://localhost:8001").rstrip("/")

    # 测试只同步两只股票，一个很小的时间窗口，避免一次性全市场
    options = {
        "start_date": "2023-01-03",
        "end_date": "2023-01-05",
        "workers": 2,
        # 为避免测试时清空整张表，这里默认不 truncate，如需真正全量初始化可改为 True
        "truncate": False,
        "codes": ["000001.SZ", "600519.SH"],
        "max_rows_per_chunk": 50_000,
    }

    payload = {
        "dataset": "kline_minute_raw",
        "options": options,
    }

    print("Backend:", backend)
    print("Request payload:\n", json.dumps(payload, ensure_ascii=False, indent=2))

    url = f"{backend}/api/ingestion/init"
    try:
        resp = requests.post(url, json=payload, timeout=30)
    except Exception as exc:  # noqa: BLE001
        print("Request failed:", exc)
        return 1

    print("Status:", resp.status_code)
    print("Body:", resp.text)

    try:
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        print("Decode JSON failed:", exc)
        return 1

    if resp.status_code != 200:
        print("Non-200 response, abort.")
        return 1

    job_id = data.get("job_id")
    task_id = data.get("task_id") or data.get("run_id")
    print("job_id:", job_id)
    print("task_id/run_id:", task_id)

    if not job_id:
        print("No job_id returned, abort.")
        return 1

    # 轮询 job 状态，最多 30 次 * 5s = 150 秒
    job_url = f"{backend}/api/ingestion/job/{job_id}"
    print("\nPolling job status:")
    for i in range(30):
        time.sleep(5)
        try:
            jr = requests.get(job_url, timeout=15)
            jr.raise_for_status()
            js = jr.json()
        except Exception as exc:  # noqa: BLE001
            print(f"[{i}] poll failed:", exc)
            continue

        status = js.get("status")
        progress = js.get("progress")
        counters = js.get("counters")
        print(f"[{i}] status={status} progress={progress}% counters={counters}")

        if status not in {"queued", "pending", "running"}:
            print("Job finished with status:", status)
            break

    print("\nFinal job detail:")
    try:
        jr = requests.get(job_url, timeout=15)
        jr.raise_for_status()
        js = jr.json()
        print(json.dumps(js, ensure_ascii=False, indent=2, default=str))
    except Exception as exc:  # noqa: BLE001
        print("Fetch final job detail failed:", exc)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
