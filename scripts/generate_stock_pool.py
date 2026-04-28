#!/usr/bin/env python3
"""
股票池生成脚本

根据 sw2_pool_config 黑名单表，从 Qlib all.txt 中过滤掉被排除的行业股票，
生成适用于 Qlib 回测的股票池文件（三列 TSV 格式）。

用法:
    python generate_stock_pool.py --date 2024-01-01
    python generate_stock_pool.py  # 不传 --date 则使用今天

输出:
    F:/Dev/AIstock/stock_pools/filtered_pool_YYYYMMDD.txt
    格式与 Qlib all.txt 一致：000001.SZ\t2018-08-01\t2026-03-10
"""

import argparse
import hashlib
import os
import shlex
import subprocess
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# 加载 AIstock .env
ENV_PATH = Path(__file__).parent.parent / ".env"
load_dotenv(ENV_PATH)

DB_HOST = os.getenv("TDX_DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("TDX_DB_PORT", 5432))
DB_NAME = os.getenv("TDX_DB_NAME", "aistock")
DB_USER = os.getenv("TDX_DB_USER", "postgres")
DB_PASS = os.getenv("TDX_DB_PASSWORD", "")

# 输出目录
OUTPUT_DIR = Path(os.getenv(
    "STOCK_POOL_OUTPUT_DIR",
    "F:/Dev/AIstock/stock_pools"
))


def _wsl_distro() -> str:
    distro = (os.getenv("AISTOCK_WSL_DISTRO") or os.getenv("QLIB_WSL_DISTRO") or "").strip()
    if not distro:
        raise RuntimeError("AISTOCK_WSL_DISTRO or QLIB_WSL_DISTRO is required")
    return distro


def _resolve_qlib_instruments_wsl() -> str:
    explicit = (os.getenv("QLIB_INSTRUMENTS_WSL") or "").strip()
    if explicit:
        return explicit.rstrip("/")
    qlib_data = (os.getenv("QLIB_DATA_PATH_WSL") or "").strip()
    if not qlib_data:
        raise RuntimeError("QLIB_INSTRUMENTS_WSL or QLIB_DATA_PATH_WSL is required")
    return f"{qlib_data.rstrip('/')}/instruments"


def _win_to_wsl(win_path: str) -> str:
    value = win_path.replace("\\", "/")
    if len(value) >= 2 and value[1] == ":":
        return f"/mnt/{value[0].lower()}{value[2:]}"
    return value


def _run_wsl(script: str, *, timeout: int = 30, input_bytes: bytes | None = None) -> str:
    cmd = ["wsl", "-d", _wsl_distro(), "--", "bash", "-lc", script]
    try:
        result = subprocess.run(
            cmd,
            input=input_bytes,
            timeout=timeout,
            check=False,
            capture_output=True,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"WSL command timed out after {timeout}s while syncing stock_pool. "
            f"cmd={cmd!r}"
        ) from exc
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        raise RuntimeError(stderr or stdout or f"WSL command failed with exit code {result.returncode}")
    return result.stdout.decode("utf-8", errors="replace")


def get_blocked_stocks(conn, query_date: date) -> set:
    """查询指定日期应被排除的股票代码集合。"""
    with conn.cursor() as cur:
        # 1. 获取当日 blocked 的申万二级行业代码
        cur.execute("""
            SELECT sw2_code FROM sw2_pool_config
            WHERE status = 'blocked'
              AND is_active = TRUE
              AND (effective_from IS NULL OR effective_from <= %s)
              AND (effective_to IS NULL OR effective_to >= %s)
        """, (query_date, query_date))
        blocked_sw2 = [row[0] for row in cur.fetchall()]

    if not blocked_sw2:
        print(f"[INFO] 日期 {query_date}: 无 blocked 行业，股票池不做过滤")
        return set()

    print(f"[INFO] 日期 {query_date}: blocked 申万二级行业 {len(blocked_sw2)} 个: {blocked_sw2}")

    with conn.cursor() as cur:
        # 2. 查询这些行业在该日期的成分股（PIT）
        placeholders = ",".join(["%s"] * len(blocked_sw2))
        cur.execute(f"""
            SELECT DISTINCT ts_code FROM market.sw_index_member
            WHERE l2_code IN ({placeholders})
              AND in_date <= %s
              AND (out_date IS NULL OR out_date >= %s)
        """, blocked_sw2 + [query_date, query_date])
        blocked_stocks = {row[0] for row in cur.fetchall()}

    print(f"[INFO] 日期 {query_date}: 排除股票 {len(blocked_stocks)} 只")
    return blocked_stocks


def load_all_txt(qlib_instruments_wsl: str) -> list:
    """通过 WSL 子进程加载 Qlib all.txt，避免 Windows 侧 UNC 访问。"""
    all_txt_wsl = f"{qlib_instruments_wsl.rstrip('/')}/all.txt"
    stdout = _run_wsl(
        f"test -f {shlex.quote(all_txt_wsl)} && cat {shlex.quote(all_txt_wsl)}",
        timeout=30,
    )
    lines = [line.rstrip("\n") for line in stdout.splitlines() if line.strip()]
    print(f"[INFO] 加载 all.txt: {len(lines)} 只股票")
    return lines


def sync_pool_to_qlib(output_path: Path, instrument_name: str, qlib_instruments_wsl: str) -> str:
    """Copy the generated pool into Qlib instruments using WSL stdin."""
    if not output_path.exists():
        raise FileNotFoundError(f"stock_pool output file not found: {output_path}")
    payload = output_path.read_bytes()
    expected_sha256 = hashlib.sha256(payload).hexdigest()
    qlib_dir = qlib_instruments_wsl.rstrip("/")
    dest_wsl = f"{qlib_dir}/{instrument_name}.txt"
    tmp_wsl = f"{qlib_dir}/.{instrument_name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    sha_wsl = f"{tmp_wsl}.sha256"
    sync_timeout = int(os.getenv("STOCK_POOL_SYNC_TIMEOUT_SEC", "120"))
    stdout = _run_wsl(
        "set -euo pipefail\n"
        f"mkdir -p {shlex.quote(qlib_dir)}\n"
        f"trap 'rm -f {shlex.quote(tmp_wsl)} {shlex.quote(sha_wsl)}' EXIT\n"
        f"cat > {shlex.quote(tmp_wsl)}\n"
        f"sha256sum {shlex.quote(tmp_wsl)} > {shlex.quote(sha_wsl)}\n"
        f"read -r actual_sha _ < {shlex.quote(sha_wsl)}\n"
        f"if [ \"\\$actual_sha\" != {shlex.quote(expected_sha256)} ]; then "
        f"echo \"stock_pool tmp checksum mismatch: \\$actual_sha\" >&2; exit 1; fi\n"
        f"mv -f {shlex.quote(tmp_wsl)} {shlex.quote(dest_wsl)}\n"
        f"sha256sum {shlex.quote(dest_wsl)} > {shlex.quote(sha_wsl)}\n"
        f"read -r final_sha _ < {shlex.quote(sha_wsl)}\n"
        "printf '%s\\n' \"\\$final_sha\"\n",
        timeout=sync_timeout,
        input_bytes=payload,
    )
    actual_sha256 = stdout.strip().splitlines()[-1] if stdout.strip() else ""
    if actual_sha256 != expected_sha256:
        raise RuntimeError(
            f"Qlib instruments stock_pool checksum mismatch: "
            f"expected={expected_sha256} actual={actual_sha256} dest={dest_wsl}"
        )
    return dest_wsl


def generate_pool(query_date: date, output_dir: Path) -> Path:
    """生成过滤后的股票池文件，返回输出路径。"""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    try:
        blocked = get_blocked_stocks(conn, query_date)
    finally:
        conn.close()

    qlib_instruments_wsl = _resolve_qlib_instruments_wsl()
    all_lines = load_all_txt(qlib_instruments_wsl)

    if not blocked:
        # 无排除行业，直接复制 all.txt 内容
        filtered = all_lines
    else:
        filtered = []
        for line in all_lines:
            ts_code = line.split("\t")[0]
            if ts_code not in blocked:
                filtered.append(line)

    removed = len(all_lines) - len(filtered)
    print(f"[INFO] 过滤后: {len(filtered)} 只股票（排除 {removed} 只）")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"filtered_pool_{query_date.strftime('%Y%m%d')}.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(filtered))
        if filtered:
            f.write("\n")

    print(f"[INFO] 输出文件: {output_path}")

    # 同步到 Qlib instruments 目录（Qlib 按名称+自动加.txt查找）
    instrument_name = f"filtered_pool_{query_date.strftime('%Y%m%d')}"
    try:
        qlib_dest = sync_pool_to_qlib(output_path, instrument_name, qlib_instruments_wsl)
        print(f"[INFO] 已同步到 Qlib instruments: {qlib_dest}")
    except Exception as e:
        print(f"[ERROR] 同步到 Qlib instruments 失败: {e}")
        raise

    return output_path, instrument_name


def main():
    parser = argparse.ArgumentParser(description="生成 Qlib 股票池过滤文件")
    parser.add_argument(
        "--date", type=str, default=None,
        help="查询日期 YYYY-MM-DD，默认今天"
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="输出目录，默认 F:/Dev/AIstock/stock_pools"
    )
    args = parser.parse_args()

    query_date = date.today()
    if args.date:
        try:
            query_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"[ERROR] 日期格式错误: {args.date}，应为 YYYY-MM-DD")
            sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR

    output_path, instrument_name = generate_pool(query_date, output_dir)
    # 输出 instrument 名称（不带 .txt），Qlib 会自动加 .txt 从 instruments/ 目录查找
    print(f"[WSL PATH] {instrument_name}")


if __name__ == "__main__":
    main()
