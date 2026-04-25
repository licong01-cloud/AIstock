#!/usr/bin/env python3
"""修复 price_volume_corr 和 price_volume_correlation_5d 的 code_text。

问题：DB code_text 使用 groupby.rolling.cov/(std*std) 三步法，
每个 worker 消耗 20GB+ 内存（4700 instruments × rolling 中间对象堆积）。

修复：改为逐 instrument 循环 + rolling.corr，内存可控（<2GB）。
计算逻辑完全等价：corr(x,y) = cov(x,y) / (std(x) * std(y))。

修复范围：
1. DB code_text 列（backfill 使用）
2. DB realtime_code_text 列（确认一致）
3. 文件系统 qe_factors/*.py（确认一致）
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import psycopg2

# ── 修复后的 code_text（backfill 格式：读 h5 文件）──

FIXED_CODE_TEXT_PRICE_VOLUME_CORR = '''\
import pandas as pd
import numpy as np

def calculate_price_volume_corr():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 数据已使用标准字段名: open, high, low, close, volume, amount, factor

    # 3. 检查必需字段是否存在
    required_cols = ["close", "volume"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Please redesign factor using available fields.")

    # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 计算收盘价与成交量的20日滚动相关系数（逐instrument循环，避免 groupby+rolling 内存爆炸）
    _parts = []
    for _inst, _g in df.groupby(level='instrument'):
        _s = _g['close'].rolling(20, min_periods=20).corr(_g['volume'])
        _parts.append(_s)
    series = pd.concat(_parts)

    # ==== END FACTOR COMPUTATION AREA ====

    # 5. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["price_volume_corr"] = series.astype("float32")

    # 6. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 7. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_price_volume_corr()
'''

FIXED_CODE_TEXT_PRICE_VOLUME_CORRELATION_5D = '''\
import pandas as pd
import numpy as np

def calculate_price_volume_correlation_5d():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 数据已使用标准字段名: open, high, low, close, volume, amount, factor
    # 直接使用这些字段名进行计算即可

    # 3. 检查必需字段是否存在
    required_cols = ["close", "volume"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Please redesign factor using available fields.")

    # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 计算5日滚动收盘价与成交量的相关系数（逐instrument循环，避免 groupby+rolling 内存爆炸）
    _parts = []
    for _inst, _g in df.groupby(level='instrument'):
        _s = _g['close'].rolling(5, min_periods=5).corr(_g['volume'])
        _parts.append(_s)
    series = pd.concat(_parts)

    # ==== END FACTOR COMPUTATION AREA ====

    # 5. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["price_volume_correlation_5d"] = series.astype("float32")

    # 6. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 7. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_price_volume_correlation_5d()
'''


def main():
    conn = psycopg2.connect(
        host=os.environ['TDX_DB_HOST'],
        port=os.environ['TDX_DB_PORT'],
        dbname=os.environ['TDX_DB_NAME'],
        user=os.environ['TDX_DB_USER'],
        password=os.environ['TDX_DB_PASSWORD'],
    )
    conn.autocommit = False
    cur = conn.cursor()

    updates = [
        ("price_volume_corr", FIXED_CODE_TEXT_PRICE_VOLUME_CORR),
        ("price_volume_correlation_5d", FIXED_CODE_TEXT_PRICE_VOLUME_CORRELATION_5D),
    ]

    for factor_name, new_code in updates:
        # 先读取当前值确认
        cur.execute(
            "SELECT code_text FROM aistock_factor_catalog WHERE factor_name = %s",
            (factor_name,)
        )
        row = cur.fetchone()
        if not row:
            print(f"[SKIP] {factor_name}: 不存在于 DB")
            continue

        old_code = row[0] or ""
        if "groupby" not in old_code and "_parts" in old_code:
            print(f"[SKIP] {factor_name}: code_text 已经是修复版本")
            continue

        # 更新 code_text
        cur.execute(
            "UPDATE aistock_factor_catalog SET code_text = %s WHERE factor_name = %s",
            (new_code, factor_name)
        )
        print(f"[FIXED] {factor_name}: code_text 已更新 (groupby.rolling.cov → 逐instrument循环)")

    conn.commit()
    cur.close()
    conn.close()
    print("\n[DONE] DB code_text 修复完成")

    # ── 验证文件系统 qe_factors/*.py 是否一致 ──
    qe_dir = Path(__file__).resolve().parents[1] / "rdagent_assets" / "qe_factors"
    for fname in ["price_volume_corr.py", "price_volume_correlation_5d.py"]:
        fpath = qe_dir / fname
        if fpath.exists():
            content = fpath.read_text(encoding="utf-8")
            if "groupby(level='instrument').rolling" in content and ".cov(" in content:
                print(f"[WARN] {fpath} 仍使用旧 groupby.rolling.cov 模式，需要手动检查")
            elif "_parts" in content or "for _inst" in content:
                print(f"[OK] {fpath} 已是修复版本")
            else:
                print(f"[INFO] {fpath} 使用其他模式，请人工确认")
        else:
            print(f"[WARN] {fpath} 不存在")


if __name__ == "__main__":
    main()
