"""零 LLM 回填 ts_info_density + cross_horizon_consistency。

场景: 当前全量 batch-analyze 已跑完, 但这两个字段为 NULL (Phase 1 修复前的数据)。
此脚本只 UPDATE 已有记录, 不 INSERT 新行, 不调 LLM。

用法:
    python F:\\Dev\\AIstock\\scripts\\_backfill_v2_deterministic.py                    # 全量回填 (默认 skip_if_present=True)
    python F:\\Dev\\AIstock\\scripts\\_backfill_v2_deterministic.py --force             # 强制重算 (覆盖已有值)
    python F:\\Dev\\AIstock\\scripts\\_backfill_v2_deterministic.py --source rdagent_task_sync  # 仅指定 source

执行前置:
    1. 已重启后端(让新代码在脚本进程里也生效, 但此脚本不经过后端, 只需代码更新到最新)
    2. UI 已跑 10 个因子小样本验证过两个字段能正常写入
"""
import argparse
import os
import sys
import time
from dotenv import load_dotenv

load_dotenv(r"F:\Dev\AIstock\.env")
sys.path.insert(0, r"F:\Dev\AIstock")

import logging
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

from backend.services.quantevolver.factor_analyst import FactorAnalyst


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true",
                        help="强制重算 (skip_if_present=False)")
    parser.add_argument("--source", type=str, default=None,
                        help="仅处理指定 source (如 rdagent_task_sync)")
    parser.add_argument("--limit", type=int, default=None,
                        help="仅处理前 N 个 (测试用)")
    args = parser.parse_args()

    fa = FactorAnalyst()
    t_start = time.time()

    if args.limit:
        # 测试路径: 手动取 N 个
        from backend.services.quantevolver.factor_analyst import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT factor_name, factor_source
                    FROM qe_factor_classification
                """
                params = []
                if args.source:
                    sql += " WHERE factor_source = %s"
                    params.append(args.source)
                sql += f" ORDER BY id LIMIT {args.limit}"
                cur.execute(sql, params)
                rows = cur.fetchall()

        print(f"=== 测试模式: 处理前 {len(rows)} 个因子 ===")
        print(f"skip_if_present = {not args.force}\n")
        updated = 0; skipped = 0; errors = 0
        for name, source in rows:
            try:
                r = fa.backfill_deterministic_v2(name, source, skip_if_present=not args.force)
                status = "UPD" if r.get("updated") else "SKIP"
                ts = r.get("ts_info_density")
                xhz = r.get("cross_horizon_consistency")
                reason = r.get("skipped_reason", "")
                print(f"  [{status}] {name:<42} ts={ts}  xhz={xhz}  {reason}")
                if r.get("updated"): updated += 1
                else: skipped += 1
            except Exception as e:
                print(f"  [ERR] {name}: {e}")
                errors += 1
        print(f"\n小计: updated={updated}  skipped={skipped}  errors={errors}")
    else:
        print(f"=== 全量回填 ===  source={args.source or '(all)'}  skip_if_present={not args.force}")
        result = fa.batch_backfill_deterministic_v2(
            source_filter=args.source, skip_if_present=not args.force)
        print(f"\n结果: total={result['total']}  updated={result['updated']}  "
              f"skipped={result['skipped']}  errors={result['error_count']}")
        if result.get("errors"):
            print("\n前 10 个错误:")
            for e in result["errors"]:
                print(f"  {e['factor_name']}: {e['error']}")

    print(f"\n耗时: {time.time()-t_start:.1f}s")
    print("\n验证 SQL:")
    print("  psql ... -f F:\\Dev\\AIstock\\scripts\\_check_v2_coverage.sql")


if __name__ == "__main__":
    main()
