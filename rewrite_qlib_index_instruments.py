import argparse
import os
import sys
from datetime import datetime, date
from typing import List, Tuple

from app_pg import get_conn  # type: ignore[attr-defined]

QLIB_BIN_ROOT_ENV = "QLIB_BIN_ROOT_WIN"
DEFAULT_QLIB_BIN_ROOT = r"C:\\Users\\lc999\\NewAIstock\\AIstock\\qlib_bin"

INDEX_DAILY_TABLE = "market.index_daily"
INSTRUMENTS_SUBDIR = "instruments"
INDEX_FILE_NAME = "index.txt"


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def get_qlib_bin_root() -> str:
    root = os.environ.get(QLIB_BIN_ROOT_ENV, DEFAULT_QLIB_BIN_ROOT)
    return root.rstrip("/\\")


def fetch_date_range(ts_code: str, start: date, end: date) -> Tuple[date | None, date | None]:
    """查询指定 ts_code 在 index_daily 中、给定区间内的最早/最晚交易日.

    返回 (min_date, max_date)，若无数据则两者均为 None。
    """

    sql = f"""
        SELECT MIN(trade_date) AS min_d, MAX(trade_date) AS max_d
          FROM {INDEX_DAILY_TABLE}
         WHERE ts_code = %(ts_code)s
           AND trade_date >= %(start)s
           AND trade_date <= %(end)s
    """
    params = {"ts_code": ts_code, "start": start, "end": end}

    with get_conn() as conn:  # type: ignore[attr-defined]
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()

    if not row:
        return None, None
    min_d, max_d = row
    return min_d, max_d


def rewrite_index_file(
    snapshot_id: str,
    ts_codes: List[str],
    start: date,
    end: date,
) -> None:
    qlib_root = get_qlib_bin_root()
    snapshot_dir = os.path.join(qlib_root, snapshot_id)
    inst_dir = os.path.join(snapshot_dir, INSTRUMENTS_SUBDIR)
    index_path = os.path.join(inst_dir, INDEX_FILE_NAME)

    print("QLIB_BIN_ROOT:", qlib_root)
    print("Snapshot dir:", snapshot_dir)
    print("Instruments dir:", inst_dir)
    print("Index file path:", index_path)

    if not os.path.isdir(snapshot_dir):
        raise SystemExit(f"ERROR: snapshot_dir 不存在: {snapshot_dir}")
    os.makedirs(inst_dir, exist_ok=True)

    lines: List[str] = []
    for code in ts_codes:
        c = code.strip()
        if not c:
            continue
        min_d, max_d = fetch_date_range(c, start, end)
        if min_d is None or max_d is None:
            print(f"WARNING: 在 {INDEX_DAILY_TABLE} 中未找到 {c} 在区间 {start}~{end} 的数据，跳过该代码。")
            continue
        # 使用制表符分隔 3 列，严格匹配 Qlib InstrumentStorage._read_instrument 的 sep="\t" 行为
        # 对应列名: [symbol, start, end]
        line = f"{c}\t{min_d.isoformat()}\t{max_d.isoformat()}"
        print("  +", line)
        lines.append(line)

    if not lines:
        raise SystemExit("ERROR: 没有任何代码生成 index.txt，放弃覆盖。")

    # 备份旧文件
    if os.path.exists(index_path):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = index_path + f".bak_{ts}"
        os.replace(index_path, backup_path)
        print("已备份原 index.txt 到:", backup_path)

    tmp_path = index_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        for i, line in enumerate(lines):
            # 写入每一行，行尾统一加换行符，不额外写入空行
            f.write(line)
            if i != len(lines) - 1:
                f.write("\n")

    os.replace(tmp_path, index_path)
    print(f"已重写 {index_path}，共 {len(lines)} 行。")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "根据 market.index_daily 中的实际数据，"
            "重写指定 snapshot 下的 instruments/index.txt 为 Qlib 期望的 3 列格式。"
        )
    )
    parser.add_argument(
        "--snapshot-id",
        required=True,
        help="Qlib snapshot ID，例如 qlib_bin_20251209",
    )
    parser.add_argument(
        "--codes",
        required=True,
        help=(
            "逗号分隔的指数 ts_code 列表，例如 "
            "000300.SH,000001.SH,399001.SZ,399006.SZ,000905.SH,000852.SH"
        ),
    )
    parser.add_argument("--start", required=True, help="目标开始日期 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="目标结束日期 YYYY-MM-DD")

    args = parser.parse_args()
    snapshot_id = args.snapshot_id.strip()
    ts_codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    start = parse_date(args.start)
    end = parse_date(args.end)

    if start > end:
        print("start 不能晚于 end")
        sys.exit(1)

    print(
        f"=== 重写 Qlib 指数 instruments/index.txt ===\n"
        f"INDEX_DAILY_TABLE = {INDEX_DAILY_TABLE}\n"
        f"snapshot_id       = {snapshot_id}\n"
        f"codes             = {ts_codes}\n"
        f"date range        = {start} ~ {end}\n"
        f"QLIB_BIN_ROOT_ENV = {QLIB_BIN_ROOT_ENV}\n"
        f"time              = {datetime.now().isoformat()}\n"
    )

    rewrite_index_file(snapshot_id, ts_codes, start, end)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(1)
    except Exception as e:  # noqa: BLE001
        print("脚本执行异常:", repr(e))
        sys.exit(1)
