"""Test xtquant daily history access for sample SH stocks via data_service.

Usage (from project root):

    python -m backend.scripts.test_xtquant_sh_sample

Prerequisites:
- QMT 已经在本地数据管理中下载了上交所股票最近一个月日线；
- 后端能够连接到包含 stock_basic 数据集的数据库；
- xtquant/miniQMT 已经启动并可用。
"""

from __future__ import annotations

import logging
from typing import List

from backend.data_service import api as data_api

logger = logging.getLogger(__name__)


def test_daily_history_for_codes(codes: List[str]) -> None:
    """调用 data_service.get_history_window 测试每只 SH 股票的日线数据是否可用。"""
    universe = codes

    print("Testing get_history_window for SH codes:", universe)

    try:
        # 取最近 60 根日线（大约 3 个月），你现在 QMT 已下载最近 1 个月，也可以覆盖其中一部分
        df = data_api.get_history_window(
            universe,
            bars=60,
            fields=["open", "high", "low", "close", "volume", "amount"],
            freq="1d",
        )
    except Exception as exc:
        print("get_history_window FAILED:", repr(exc))
        return

    print("total rows:", len(df))
    print("index names:", df.index.names)

    # 按单只股票拆开看前几行
    instruments = sorted(set(df.index.get_level_values("instrument")))
    for inst in instruments:
        df_inst = df.xs(inst, level="instrument")
        print("\n===", inst, "===")
        print("rows:", len(df_inst))
        print(df_inst.head())


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
    )

    # 直接使用一组典型的上交所股票代码进行测试，避免依赖数据库结构
    sample_codes = [
        "600000.SH",
        "600519.SH",
        "600036.SH",
        "601318.SH",
        "600104.SH",
        "600030.SH",
        "600340.SH",
        "600104.SH",
        "600016.SH",
        "600837.SH",
    ]
    logger.info("Sample SH codes: %s", sample_codes)

    test_daily_history_for_codes(sample_codes)


if __name__ == "__main__":
    main()
