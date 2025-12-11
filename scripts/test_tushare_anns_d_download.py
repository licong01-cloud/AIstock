"""Quick test script for Tushare anns_d.

功能：
- 从项目根目录的 .env 文件读取 TUSHARE_TOKEN（若已有环境变量则直接使用环境变量）。
- 调用 Tushare anns_d，按指定日期获取一小批公告数据。
- 尝试下载第一条公告的 PDF/原文到本地 data/anns_test/ 目录。

用法（在 AIstock 根目录下）：

    (AIstock) python scripts/test_tushare_anns_d_download.py --date 20230621

注意：
- 需要已安装 tushare 和 requests（requirements 中一般已有）。
- anns_d 接口需要单独权限，请确保你的账号已开通。
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys
import time
from typing import Optional

import requests

try:
    import tushare as ts
except ImportError:  # noqa: BLE001
    print("[ERROR] tushare is not installed. Please install it first: pip install tushare")
    sys.exit(1)


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"


def load_env_token() -> Optional[str]:
    """Load TUSHARE_TOKEN from .env or environment.

    优先使用环境变量 TUSHARE_TOKEN；若不存在，则尝试从项目根目录 .env 文件中读取。
    .env 格式示例：
        TUSHARE_TOKEN=your_token_here
    """

    token = os.getenv("TUSHARE_TOKEN")
    if token:
        return token.strip()

    if ENV_PATH.exists():
        try:
            with ENV_PATH.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.startswith("TUSHARE_TOKEN="):
                        _, value = line.split("=", 1)
                        value = value.strip().strip('"').strip("'")
                        if value:
                            os.environ["TUSHARE_TOKEN"] = value
                            return value
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Failed to read .env file: {exc}")

    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test Tushare anns_d + PDF download")
    parser.add_argument(
        "--date",
        type=str,
        default="20230621",
        help="公告日期，格式 yyyymmdd，例如 20230621",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="最多检查前 N 条公告以寻找可下载的 URL",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    token = load_env_token()
    if not token:
        print("[ERROR] TUSHARE_TOKEN not found in environment or .env file")
        return 1

    print(f"Using TUSHARE_TOKEN from env/.env, length={len(token)}")
    ts.set_token(token)

    pro = ts.pro_api()

    ann_date = args.date
    print(f"\n[STEP] Fetching anns_d for ann_date={ann_date} ...")

    try:
        df = pro.anns_d(ann_date=ann_date)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] anns_d call failed: {exc}")
        return 1

    if df is None or df.empty:
        print("[WARN] anns_d returned empty result for this date")
        return 0

    print(f"[INFO] anns_d returned {len(df)} rows")
    print(df.head(min(5, len(df))))

    # 尝试从前 N 条记录中找到第一个有 url 的公告，并下载 PDF/原文
    save_dir = PROJECT_ROOT / "data" / "anns_test" / ann_date
    save_dir.mkdir(parents=True, exist_ok=True)

    max_check = max(1, args.limit)
    downloaded = 0

    for idx, row in df.head(max_check).iterrows():
        ts_code = str(row.get("ts_code") or "").strip()
        title = str(row.get("title") or "").strip()
        url = str(row.get("url") or "").strip()

        if not url:
            continue

        safe_title = title.replace("/", "_").replace("\\", "_").replace(" ", "_")
        if not safe_title:
            safe_title = f"ann_{idx}"

        ext = ".pdf"
        # 简单根据 URL 猜测扩展名（可选）
        lower_url = url.lower()
        if ".pdf" in lower_url:
            ext = ".pdf"
        elif any(lower_url.endswith(suf) for suf in (".html", ".htm")):
            ext = ".html"

        filename = f"{ts_code}_{idx}{ext}"
        filepath = save_dir / filename

        if filepath.exists():
            print(f"[INFO] File already exists, skip: {filepath}")
            downloaded += 1
            break

        print(f"\n[STEP] Downloading announcement #{idx}: {ts_code} {title}")
        print(f"       URL: {url}")

        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Failed to download: {exc}")
            continue

        try:
            with filepath.open("wb") as f:
                f.write(resp.content)
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Failed to save file {filepath}: {exc}")
            continue

        print(f"[OK] Saved announcement to {filepath}")
        downloaded += 1
        # 只要验证成功下载一个就足够，避免不必要的请求
        break

    if downloaded == 0:
        print(f"[WARN] No announcement with valid URL downloaded within first {max_check} rows")
    else:
        print(f"[DONE] Successfully downloaded {downloaded} file(s)")

    # 小小等待，方便在 CI / 终端中阅读输出
    time.sleep(0.5)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
