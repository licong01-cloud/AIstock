from __future__ import annotations

"""简单的 Qlib Snapshot 导出与检查脚本.

依赖:
- 本地已启动 FastAPI 后端 (默认 http://localhost:8000)
- 已在项目根目录下运行本脚本，或根据 PROJECT_ROOT 调整路径

功能:
1. 调用后端接口导出:
   - 日线:   /api/v1/qlib/snapshots/daily
   - 分钟线: /api/v1/qlib/snapshots/minute
   - 板块日线: /api/v1/qlib/boards/daily
2. 读取生成的 HDF5 文件并打印基本信息.
"""

import json
from pathlib import Path

import pandas as pd
import requests


# ---------------- 基本配置 ----------------

BASE_URL = "http://localhost:8001"  # FastAPI 服务地址
PROJECT_ROOT = Path(r"C:\Users\lc999\NewAIstock\AIstock")  # 按需修改
SNAPSHOT_ID = "test_qlib_single_1mtest"  # 按需修改
SNAPSHOT_ROOT = PROJECT_ROOT / "qlib_snapshots" / SNAPSHOT_ID

START_DATE = "2025-11-03"  # 缩小时间范围，便于快速验证
END_DATE = "2025-11-03"


# ---------------- HTTP 调用工具 ----------------


def _post_json(path: str, payload: dict, timeout: int = 600) -> dict:
    url = f"{BASE_URL}{path}"
    print(f"\n[HTTP] POST {url}")
    print("[HTTP] payload:", json.dumps(payload, ensure_ascii=False))
    resp = requests.post(url, json=payload, timeout=timeout)
    print("[HTTP] status:", resp.status_code)
    try:
        data = resp.json()
    except Exception:
        print("[HTTP] raw text:", resp.text[:1000])
        raise
    print("[HTTP] response:", json.dumps(data, ensure_ascii=False))
    resp.raise_for_status()
    return data


# ---------------- 导出调用封装 ----------------


def export_daily() -> dict:
    body = {
        "snapshot_id": SNAPSHOT_ID,
        "start": START_DATE,
        "end": END_DATE,
        # 仅测试少量标的，避免全市场导出过慢
        "ts_codes": ["000001.SZ", "000002.SZ"],
        "exchanges": ["sh", "sz", "bj"],
    }
    return _post_json("/api/v1/qlib/snapshots/daily", body)


def export_minute() -> dict:
    body = {
        "snapshot_id": SNAPSHOT_ID,
        "start": START_DATE,
        "end": END_DATE,
        # 仅测试少量标的，避免全市场导出过慢
        "ts_codes": ["000001.SZ", "000002.SZ"],
        "exchanges": ["sh", "sz", "bj"],
        "freq": "1m",
    }
    return _post_json("/api/v1/qlib/snapshots/minute", body)


def export_boards_daily() -> dict:
    body = {
        "snapshot_id": SNAPSHOT_ID,
        "start": START_DATE,
        "end": END_DATE,
        "board_codes": None,
        "idx_types": None,  # 也可以指定 ["行业", "概念"] 等
    }
    return _post_json("/api/v1/qlib/boards/daily", body)


# ---------------- 文件检查工具 ----------------


def inspect_h5(path: Path, key: str = "data", max_rows: int = 5) -> None:
    print(f"\n[INSPECT] {path}")
    if not path.exists():
        print("  - 文件不存在")
        return
    df = pd.read_hdf(path, key=key)
    print("  - shape:", df.shape)
    print("  - index names:", df.index.names)
    print("  - columns:", df.columns.tolist())
    print("  - head:")
    print(df.head(max_rows))


def inspect_snapshot() -> None:
    print("\n========== Inspect Daily ==========")
    inspect_h5(SNAPSHOT_ROOT / "daily_pv.h5")

    print("\n========== Inspect Minute (1m) ==========")
    inspect_h5(SNAPSHOT_ROOT / "minute_1min.h5")

    print("\n========== Inspect Boards Daily ==========")
    inspect_h5(SNAPSHOT_ROOT / "boards" / "board_daily.h5")


# ---------------- 主流程 ----------------


def main() -> None:
    print("=== Step 1: export daily ===")
    try:
        export_daily()
    except Exception as e:  # noqa: BLE001
        print("[ERROR] export_daily failed:", e)

    print("\n=== Step 2: export minute (1m) ===")
    try:
        export_minute()
    except Exception as e:  # noqa: BLE001
        print("[ERROR] export_minute failed:", e)

    print("\n=== Step 3: export board daily ===")
    try:
        export_boards_daily()
    except Exception as e:  # noqa: BLE001
        print("[ERROR] export_boards_daily failed:", e)

    print("\n=== Step 4: inspect generated snapshot files ===")
    inspect_snapshot()


if __name__ == "__main__":  # pragma: no cover
    main()
