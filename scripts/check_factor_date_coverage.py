"""检查因子日期覆盖率 — 找出 >50% 日期无数据的因子"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import numpy as np
import pandas as pd

SINGLE_DIR = r"F:\Dev\AIstock\rdagent_assets\factor_values\single"

def main():
    # Get all available parquet files
    all_files = [f[:-8] for f in os.listdir(SINGLE_DIR) if f.endswith('.parquet')]
    print(f"Total parquet files: {len(all_files)}")

    # Get the reference date range from first factor
    ref_fp = os.path.join(SINGLE_DIR, f"{all_files[0]}.parquet")
    ref_df = pd.read_parquet(ref_fp, columns=[])
    all_dates = ref_df.index.get_level_values(0).unique().sort_values()
    window_dates = all_dates[-252:]
    n_window = len(window_dates)
    print(f"252-day window: {window_dates[0]} ~ {window_dates[-1]}")
    print(f"Window dates: {n_window}")

    # Check each factor
    results = []
    for i, name in enumerate(sorted(all_files)):
        if i % 100 == 0:
            print(f"  Scanning {i}/{len(all_files)}...")
        fp = os.path.join(SINGLE_DIR, f"{name}.parquet")
        try:
            df = pd.read_parquet(fp, columns=[])
            factor_dates = df.index.get_level_values(0).unique()
            # How many of the 252 window dates does this factor have?
            covered = factor_dates.isin(window_dates).sum()
            # Also check: for covered dates, what's the avg valid stock count?
            coverage_pct = covered / n_window * 100
            results.append({
                'name': name,
                'covered_dates': int(covered),
                'total_dates': n_window,
                'coverage_pct': round(coverage_pct, 1),
            })
        except Exception as e:
            results.append({
                'name': name,
                'covered_dates': 0,
                'total_dates': n_window,
                'coverage_pct': 0.0,
            })

    # Sort by coverage
    results.sort(key=lambda x: x['coverage_pct'])

    # Report factors with <50% coverage
    low_coverage = [r for r in results if r['coverage_pct'] < 50]
    mid_coverage = [r for r in results if 50 <= r['coverage_pct'] < 80]
    high_coverage = [r for r in results if r['coverage_pct'] >= 80]

    print(f"\n{'='*70}")
    print(f"  因子日期覆盖率统计 (252天窗口)")
    print(f"{'='*70}")
    print(f"  <50% 覆盖: {len(low_coverage)} 个因子")
    print(f"  50-80% 覆盖: {len(mid_coverage)} 个因子")
    print(f"  >=80% 覆盖: {len(high_coverage)} 个因子")

    if low_coverage:
        print(f"\n  <50% 覆盖的因子 (建议人工处理):")
        print(f"  {'因子名':<55} {'覆盖天数':>8} {'覆盖率':>8}")
        print(f"  {'-'*75}")
        for r in low_coverage:
            print(f"  {r['name']:<55} {r['covered_dates']:>8} {r['coverage_pct']:>7.1f}%")

    if mid_coverage:
        print(f"\n  50-80% 覆盖的因子:")
        print(f"  {'因子名':<55} {'覆盖天数':>8} {'覆盖率':>8}")
        print(f"  {'-'*75}")
        for r in mid_coverage:
            print(f"  {r['name']:<55} {r['covered_dates']:>8} {r['coverage_pct']:>7.1f}%")

    # Summary stats
    coverages = [r['coverage_pct'] for r in results]
    print(f"\n  覆盖率统计:")
    print(f"    均值: {np.mean(coverages):.1f}%")
    print(f"    中位: {np.median(coverages):.1f}%")
    print(f"    P10:  {np.percentile(coverages, 10):.1f}%")
    print(f"    P25:  {np.percentile(coverages, 25):.1f}%")

    # Save results
    import json
    out = {
        'low_coverage_factors': [r['name'] for r in low_coverage],
        'mid_coverage_factors': [r['name'] for r in mid_coverage],
        'all_results': results,
    }
    out_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'factor_date_coverage.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n  结果已保存: {out_path}")

if __name__ == "__main__":
    main()
