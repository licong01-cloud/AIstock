import os
import sys
from datetime import datetime

import requests

# 修改 BASE_URL 为你实际运行 tdx-api-main 服务的地址，默认使用本机 19080 端口
BASE_URL = os.environ.get("TDX_API_BASE", "http://localhost:19080")


def main() -> None:
    print(
        f"=== 测试 tdx-api 指数日线: sh000300 在 2010-01-07 是否有数据 ===\n"
        f"时间: {datetime.now().isoformat()}\n"
        f"BASE_URL: {BASE_URL}\n"
    )

    url = f"{BASE_URL.rstrip('/')}/api/index/all"
    params = {
        "code": "sh000300",  # 沪深300
        "type": "day",
    }

    print("GET", url, "params=", params)
    try:
        resp = requests.get(url, params=params, timeout=60)
    except Exception as exc:  # noqa: BLE001
        print("请求失败:", repr(exc))
        sys.exit(1)

    print("HTTP", resp.status_code, resp.reason)
    try:
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        print("解析 JSON 失败:", repr(exc))
        print("原始响应:\n", resp.text[:1000])
        sys.exit(1)

    if data.get("code") != 0:
        print("API 返回错误:", data)
        sys.exit(1)

    payload = data.get("data") or {}
    items = payload.get("list") or payload.get("List") or []
    total = payload.get("count") or payload.get("Count") or len(items)
    print(f"返回 K 线总条数: {total}")

    target_date = "2010-01-07"
    rows = [
        k for k in items
        if isinstance(k.get("Time"), str) and k["Time"].startswith(target_date)
    ]

    if not rows:
        print(f"\n❌ 未找到 {target_date} 的 sh000300 日线记录。")
        sys.exit(0)

    print(f"\n✅ 找到 {len(rows)} 条 {target_date} 的 sh000300 日线记录:")
    for k in rows:
        o = k.get("Open", 0) / 1000
        h = k.get("High", 0) / 1000
        l = k.get("Low", 0) / 1000
        c = k.get("Close", 0) / 1000
        v = k.get("Volume", 0)
        amt = k.get("Amount", 0) / 1000
        t = k.get("Time")
        print(
            f"Time={t}  开:{o:.3f} 高:{h:.3f} 低:{l:.3f} 收:{c:.3f} 量:{v}手 额:{amt:.0f}元"
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(1)
