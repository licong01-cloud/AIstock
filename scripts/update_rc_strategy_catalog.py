#!/usr/bin/env python3
"""Update aistock_strategy_catalog with the Qlib-compatible TopkDropoutRC source code.

Usage:
    python update_rc_strategy_catalog.py

Reads the strategy source from AIstock/qe_strategies/topk_dropout_rc_qlib.py
and updates the `source_code` column for strategy_id='topk_dropout_rc'.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# 添加项目根目录到 path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backend.db.pg_pool import get_conn


def main():
    # Locate the strategy source file
    script_dir = Path(__file__).resolve().parent
    source_file = script_dir.parent / "qe_strategies" / "topk_dropout_rc_qlib.py"

    if not source_file.exists():
        print(f"ERROR: Source file not found: {source_file}", file=sys.stderr)
        sys.exit(1)

    source_code = source_file.read_text(encoding="utf-8")
    print(f"Read {len(source_code)} chars from {source_file}")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT strategy_id FROM public.aistock_strategy_catalog "
                "WHERE strategy_id = %s",
                ("topk_dropout_rc",),
            )
            row = cur.fetchone()

            if row:
                cur.execute(
                    "UPDATE public.aistock_strategy_catalog "
                    "SET source_code = %s, updated_at = NOW() "
                    "WHERE strategy_id = %s",
                    (source_code, "topk_dropout_rc"),
                )
                print("Updated source_code for strategy_id='topk_dropout_rc'")
            else:
                cur.execute(
                    "INSERT INTO public.aistock_strategy_catalog "
                    "(strategy_id, display_name, description, strategy_type, "
                    " source_code, source_code_relpath, catalog_source, "
                    " market, freq, in_selection_center, "
                    " created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, 'custom', "
                    " 'cn', 'day', TRUE, NOW(), NOW())",
                    (
                        "topk_dropout_rc",
                        "TopkDropout + Risk Control (Qlib)",
                        "Qlib-compatible TopkDropout strategy with stop-loss, "
                        "HMM sector heat adjustment, and turnover cap.",
                        "rebalance",
                        source_code,
                        "qe_strategies/topk_dropout_rc_qlib.py",
                    ),
                )
                print("Inserted new row for strategy_id='topk_dropout_rc'")

        conn.commit()
        print("Done.")


if __name__ == "__main__":
    main()
