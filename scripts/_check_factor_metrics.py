"""Check factor catalog table schema and data."""
import sys
sys.path.insert(0, r"f:\Dev\AIstock\backend")

from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(r"f:\Dev\AIstock\.env"))

from db.pg_pool import get_conn

with get_conn() as conn:
    with conn.cursor() as cur:
        # Check table columns
        print("=== aistock_factor_catalog columns ===")
        cur.execute("""
            SELECT column_name, data_type, column_default
            FROM information_schema.columns
            WHERE table_name = 'aistock_factor_catalog'
            ORDER BY ordinal_position
        """)
        for row in cur.fetchall():
            print(f"  {row[0]:40s} {row[1]:20s} default={row[2]}")

        # Check if metrics columns exist and have data
        print("\n=== Metrics columns data check ===")
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(annualized_return) as has_ann_ret,
                COUNT(max_drawdown) as has_mdd,
                COUNT(sharpe) as has_sharpe,
                COUNT(ic) as has_ic,
                COUNT(performance_metrics) as has_perf_json
            FROM aistock_factor_catalog
            WHERE is_sota_factor = true
        """)
        row = cur.fetchone()
        print(f"  Total SOTA factors: {row[0]}")
        print(f"  Has annualized_return: {row[1]}")
        print(f"  Has max_drawdown: {row[2]}")
        print(f"  Has sharpe: {row[3]}")
        print(f"  Has ic: {row[4]}")
        print(f"  Has performance_metrics JSON: {row[5]}")

        # Sample data
        print("\n=== Sample factor data ===")
        cur.execute("""
            SELECT factor_name, annualized_return, max_drawdown, sharpe, ic,
                   performance_metrics, best_performance_sharpe, best_performance_ann_ret
            FROM aistock_factor_catalog
            WHERE is_sota_factor = true
            LIMIT 3
        """)
        for row in cur.fetchall():
            print(f"  Factor: {row[0]}")
            print(f"    annualized_return: {row[1]}")
            print(f"    max_drawdown: {row[2]}")
            print(f"    sharpe: {row[3]}")
            print(f"    ic: {row[4]}")
            print(f"    performance_metrics: {row[5]}")
            print(f"    best_performance_sharpe: {row[6]}")
            print(f"    best_performance_ann_ret: {row[7]}")
            print()
