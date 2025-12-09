import uuid
import time
import json

import requests


BASE = "http://localhost:19080"  # 与 TDX_API_BASE 默认保持一致


def main() -> None:
    job_id = str(uuid.uuid4())
    # 使用一只常见股票代码做测试；如需指定多只，可以自行调整
    codes = ["600000.SH"]

    payload = {
        "job_id": job_id,
        "codes": codes,
        "workers": 1,
        "options": {
            # 测试时不清空表，避免误删历史数据
            "truncate_before": False,
            # 按 Go 端默认值设置一个较大的 chunk
            "max_rows_per_chunk": 500_000,
            # 与 Go 端默认一致，用于标记来源（满足 kline_daily_qfq_source_check 约束）
            "source": "tdx_api",
        },
    }

    url = f"{BASE.rstrip('/')}/api/tasks/ingest-daily-qfq-init"
    print("POST", url)
    print("payload:")
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    resp = requests.post(url, json=payload, timeout=30)
    print("status:", resp.status_code, resp.reason)
    print("raw body:", resp.text)

    data = resp.json()
    if not isinstance(data, dict) or data.get("code") not in (0, None):
        raise SystemExit(f"unexpected response: {data}")

    task_data = data.get("data") or {}
    task_id = task_data.get("task_id")
    print("go task_id:", task_id)
    if not task_id:
        return

    # 简单轮询一下任务状态，确认 Go 端任务进入运行/完成状态
    task_url = f"{BASE.rstrip('/')}/api/tasks/{task_id}"
    print("polling:", task_url)
    for i in range(10):
        r = requests.get(task_url, timeout=10)
        print(f"[{i}] status:", r.status_code)
        try:
            obj = r.json()
        except Exception:
            print("  body:", r.text)
            break
        print("  body:", json.dumps(obj, ensure_ascii=False, indent=2))
        time.sleep(2)


if __name__ == "__main__":
    main()
