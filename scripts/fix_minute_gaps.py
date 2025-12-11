import os
import sys
import subprocess
from pathlib import Path


def main() -> int:
    """补齐 2025-04-30 和 2025-12-01 的分钟线数据（kline_minute_raw）。

    不修改任何应用/调度逻辑，仅作为运维脚本手工执行。
    内部通过调用现有的 scripts/ingest_incremental.py，使用其 upsert_minute 逻辑。
    """

    project_root = Path(__file__).resolve().parents[1]

    python_exe = sys.executable or "python"

    base_cmd = [
        python_exe,
        str(project_root / "scripts" / "ingest_incremental.py"),
        "--datasets",
        "kline_minute_raw",
        "--exchanges",
        "sh,sz,bj",
        "--batch-size",
        "100",
        "--max-empty",
        "0",
        "--workers",
        "1",
    ]

    ranges = [
        ("2025-04-30", "2025-04-30"),
        ("2025-12-01", "2025-12-01"),
    ]

    for start_date, end_date in ranges:
        cmd = base_cmd + ["--start-date", start_date, "--date", end_date]
        print("==========")
        print(f"[fix_minute_gaps] start_date={start_date} end_date={end_date}")
        print("Command:", " ".join(cmd))
        result = subprocess.run(cmd, cwd=project_root)
        if result.returncode != 0:
            print(f"[fix_minute_gaps] FAILED for {start_date}..{end_date}, return code={result.returncode}")
            return result.returncode
        print(f"[fix_minute_gaps] OK for {start_date}..{end_date}")

    print("[fix_minute_gaps] ALL DONE.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
