import os
from pathlib import Path
from datetime import datetime


START_DATE = "2010-01-07"
END_DATE = "2025-12-01"


def main() -> None:
    project_root = Path(__file__).resolve().parent

    csv_dir = project_root / "qlib_csv" / "qlib_bin_20251209"
    bin_instruments_dir = project_root / "qlib_bin" / "qlib_bin_20251209" / "instruments"
    all_txt = bin_instruments_dir / "all.txt"

    if not csv_dir.is_dir():
        raise SystemExit(f"CSV directory not found: {csv_dir}")

    if not bin_instruments_dir.is_dir():
        raise SystemExit(f"Instruments directory not found: {bin_instruments_dir}")

    # 1. 枚举 CSV 文件名（只看当前目录的 *.csv，不递归 index/ 子目录）
    codes: set[str] = set()
    for csv_path in sorted(csv_dir.glob("*.csv")):
        stem = csv_path.stem  # e.g. 000001.SZ
        if not stem:
            continue
        codes.add(stem)

    # 2. 确保 000300.SH 在 all 池中
    if "000300.SH" not in codes:
        codes.add("000300.SH")

    if not codes:
        raise SystemExit("No CSV files found, codes set is empty – aborting.")

    codes_sorted = sorted(codes)

    # 3. 备份旧 all.txt
    if all_txt.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = all_txt.with_name(f"all.txt.bak_{ts}")
        backup.write_text(all_txt.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"Backed up existing all.txt to: {backup}")
    else:
        print("No existing all.txt found, will create a new one.")

    # 4. 写入新的 all.txt（tab 分隔三列，无表头）
    lines = [f"{code}\t{START_DATE}\t{END_DATE}\n" for code in codes_sorted]
    bin_instruments_dir.mkdir(parents=True, exist_ok=True)
    all_txt.write_text("".join(lines), encoding="utf-8")

    print(f"Wrote {len(codes_sorted)} instruments to {all_txt}")


if __name__ == "__main__":
    main()
