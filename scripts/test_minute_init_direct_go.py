import uuid
import datetime as dt
import json

import requests


def main() -> int:
    base = "http://localhost:19080"
    url = base.rstrip("/") + "/api/tasks/ingest-minute-raw-init"

    job_id = str(uuid.uuid4())
    # 使用东八区时间，从 2023-01-03 起，结束时间由 Go 端自动扩展到最新可用数据
    start = dt.datetime(2023, 1, 3, 0, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=8)))

    payload = {
        "job_id": job_id,
        "codes": ["000001.SZ", "600519.SH"],
        "start_time": start.isoformat(),
        "workers": 1,
        "options": {
            "truncate_before": False,
            "max_rows_per_chunk": 50_000,
            "source": "tdx_api",
        },
    }

    print("POST", url)
    print("payload:\n", json.dumps(payload, ensure_ascii=False, indent=2))

    try:
        resp = requests.post(url, json=payload, timeout=30)
    except Exception as exc:  # noqa: BLE001
        print("request failed:", exc)
        return 1

    print("status:", resp.status_code)
    print("body:", resp.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
