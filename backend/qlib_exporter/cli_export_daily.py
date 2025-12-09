from __future__ import annotations

"""命令行工具：导出日频前复权 Qlib Snapshot（测试用）.

用法示例（在项目根目录 AIstock 下）：

  # 导出单只股票最近一个月的数据
  python -m backend.qlib_exporter.cli_export_daily \
      --snapshot-id test_000001_2025_11 \
      --ts-code 000001.SZ \
      --start 2025-11-01 \
      --end 2025-12-01

实际运行时请根据需要替换 ts_code 与日期区间。
"""

import argparse
from datetime import date
from typing import List, Optional

from .exporter import QlibDailyExporter


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:  # noqa: BLE001
        raise argparse.ArgumentTypeError(f"invalid date '{value}', expected YYYY-MM-DD") from exc


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Export daily QFQ data to Qlib snapshot (single run)")
    parser.add_argument("--snapshot-id", required=True, help="Snapshot ID, e.g. test_000001_2025_11")
    parser.add_argument("--ts-code", required=True, help="Single ts_code to export, e.g. 000001.SZ")
    parser.add_argument("--start", required=True, type=parse_date, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=parse_date, help="End date YYYY-MM-DD (inclusive)")

    args = parser.parse_args(argv)

    exporter = QlibDailyExporter()
    result = exporter.export_full(
        snapshot_id=args.snapshot_id,
        start=args.start,
        end=args.end,
        ts_codes=[args.ts_code],
    )

    print("[导出完成]")
    print(f"- snapshot_id: {result.snapshot_id}")
    print(f"- freq      : {result.freq}")
    print(f"- ts_codes  : {result.ts_codes}")
    print(f"- start     : {result.start}")
    print(f"- end       : {result.end}")
    print(f"- rows      : {result.rows}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
