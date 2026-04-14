"""
列出所有错误因子清单 + 评估内存占用。
"""
import sys, os, hashlib, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import numpy as np
import pandas as pd
import psycopg2

SINGLE_DIR = r"F:\Dev\AIstock\rdagent_assets\factor_values\single"
HDF5_PATH = r"F:\Dev\AIstock\data\correlation_matrices\corr_20260403.h5"


def main():
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="aistock",
        user="postgres", password=os.environ.get("PGPASSWORD", "lc78080808")
    )

    # ═══════════════════════════════════════
    #  1. 因子库全貌
    # ═══════════════════════════════════════
    print("=" * 70)
    print("  1. 因子库全貌")
    print("=" * 70)

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM aistock_factor_catalog")
        total = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM aistock_factor_catalog WHERE is_available = true")
        available = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM aistock_factor_catalog WHERE is_available = false")
        disabled = cur.fetchone()[0]
        cur.execute("SELECT source, count(*) FROM aistock_factor_catalog GROUP BY source ORDER BY count(*) DESC")
        sources = cur.fetchall()

    print(f"  总因子: {total}")
    print(f"  可用: {available}")
    print(f"  禁用: {disabled}")
    print(f"  来源分布:")
    for src, cnt in sources:
        print(f"    {src}: {cnt}")

    # ═══════════════════════════════════════
    #  2. 无 parquet 的可用因子
    # ═══════════════════════════════════════
    print(f"\n{'='*70}")
    print("  2. 无 parquet 文件的可用因子 (is_available=true)")
    print("=" * 70)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT factor_name, source, created_at_utc::date
            FROM aistock_factor_catalog
            WHERE is_available = true
            ORDER BY factor_name
        """)
        all_available = cur.fetchall()

    no_parquet = []
    has_parquet = []
    for name, source, created in all_available:
        fp = os.path.join(SINGLE_DIR, f"{name}.parquet")
        if os.path.isfile(fp):
            has_parquet.append(name)
        else:
            no_parquet.append((name, source, created))

    print(f"  有 parquet: {len(has_parquet)}")
    print(f"  无 parquet: {len(no_parquet)}")
    if no_parquet:
        print(f"\n  无 parquet 的因子列表:")
        for name, source, created in no_parquet:
            print(f"    {name} (source={source}, created={created})")

    # ═══════════════════════════════════════
    #  3. MD5 重复组 (完全相同的 parquet)
    # ═══════════════════════════════════════
    print(f"\n{'='*70}")
    print("  3. MD5 重复组 (完全相同的 parquet 文件)")
    print("=" * 70)

    md5_map = {}
    for name in has_parquet:
        fp = os.path.join(SINGLE_DIR, f"{name}.parquet")
        with open(fp, "rb") as f:
            md5_map[name] = hashlib.md5(f.read()).hexdigest()

    from collections import defaultdict
    md5_groups = defaultdict(list)
    for name, md5 in md5_map.items():
        md5_groups[md5].append(name)

    dup_groups = {md5: names for md5, names in md5_groups.items() if len(names) > 1}
    total_dups = sum(len(v) for v in dup_groups.values())
    redundant = total_dups - len(dup_groups)  # 每组只需保留1个

    print(f"  重复组: {len(dup_groups)}")
    print(f"  涉及因子: {total_dups}")
    print(f"  冗余因子 (可禁用): {redundant}")

    # 查询每个因子的 IC
    with conn.cursor() as cur:
        cur.execute("""
            SELECT factor_name, ic_mean
            FROM aistock_factor_metrics
            WHERE eval_window = 'out_sample'
        """)
        ic_map = {}
        for fn, ic in cur.fetchall():
            if fn not in ic_map or (ic is not None and abs(ic) > abs(ic_map.get(fn, 0) or 0)):
                ic_map[fn] = ic

    print(f"\n  重复组详情 (保留 IC 最高的):")
    keep_list = []
    disable_list = []
    for md5, names in sorted(dup_groups.items(), key=lambda x: -len(x[1])):
        # 按 abs(IC) 排序，保留最高的
        sorted_by_ic = sorted(names, key=lambda n: abs(ic_map.get(n) or 0), reverse=True)
        keep = sorted_by_ic[0]
        remove = sorted_by_ic[1:]
        keep_ic = ic_map.get(keep)
        keep_list.append(keep)
        disable_list.extend(remove)
        print(f"\n    组 ({len(names)}因子):")
        print(f"      保留: {keep} (IC={keep_ic})")
        for r in remove:
            print(f"      禁用: {r} (IC={ic_map.get(r)})")

    # ═══════════════════════════════════════
    #  4. 退化因子
    # ═══════════════════════════════════════
    print(f"\n{'='*70}")
    print("  4. 退化因子 (数据质量极差)")
    print("=" * 70)

    # 取一天数据做快速扫描
    sample_factor = has_parquet[0]
    fp = os.path.join(SINGLE_DIR, f"{sample_factor}.parquet")
    df = pd.read_parquet(fp, columns=[])
    dates = df.index.get_level_values(0).unique().sort_values()
    test_date = dates[-10]

    degenerate_factors = []
    all_nan_factors = []

    for name in has_parquet:
        fp = os.path.join(SINGLE_DIR, f"{name}.parquet")
        try:
            df = pd.read_parquet(fp)
            if "value" in df.columns:
                df = df.rename(columns={"value": name})
            try:
                day = df.loc[test_date]
                vals = day.iloc[:, 0] if isinstance(day, pd.DataFrame) else day
                n_total = len(vals)
                n_nan = vals.isna().sum()
                valid = vals.dropna()
                n_valid = len(valid)
                if n_valid == 0:
                    all_nan_factors.append(name)
                    continue
                n_unique = valid.nunique()
                zero_pct = (valid == 0).sum() / n_valid * 100
                unique_ratio = n_unique / n_valid

                if zero_pct > 90 or n_unique < 10 or unique_ratio < 0.005:
                    degenerate_factors.append({
                        "name": name,
                        "n_valid": n_valid,
                        "n_unique": n_unique,
                        "zero_pct": round(zero_pct, 1),
                        "unique_ratio": round(unique_ratio, 4),
                    })
            except KeyError:
                all_nan_factors.append(name)
        except Exception:
            all_nan_factors.append(name)

    print(f"  全 NaN (指定日无数据): {len(all_nan_factors)}")
    for name in all_nan_factors:
        print(f"    {name}")

    print(f"\n  退化因子 (zero>90% 或 unique<10 或 unique_ratio<0.5%): {len(degenerate_factors)}")
    print(f"  {'因子名':<55} {'有效数':>6} {'唯一值':>6} {'零值%':>6}")
    print(f"  {'-'*80}")
    for d in sorted(degenerate_factors, key=lambda x: -x["zero_pct"]):
        print(f"  {d['name']:<55} {d['n_valid']:>6} {d['n_unique']:>6} {d['zero_pct']:>6}")

    # ═══════════════════════════════════════
    #  5. Metrics 表重复
    # ═══════════════════════════════════════
    print(f"\n{'='*70}")
    print("  5. Metrics 表重复记录")
    print("=" * 70)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT factor_name, eval_window, count(*) as cnt
            FROM aistock_factor_metrics
            GROUP BY factor_name, eval_window
            HAVING count(*) > 1
            ORDER BY cnt DESC
            LIMIT 20
        """)
        dup_metrics = cur.fetchall()
        cur.execute("""
            SELECT count(*) FROM (
                SELECT factor_name, eval_window
                FROM aistock_factor_metrics
                GROUP BY factor_name, eval_window
                HAVING count(*) > 1
            ) t
        """)
        total_dup_metrics = cur.fetchone()[0]

    print(f"  重复 (factor_name + eval_window) 组数: {total_dup_metrics}")
    if dup_metrics:
        print(f"  Top 20:")
        for fn, ew, cnt in dup_metrics:
            print(f"    {fn} / {ew}: {cnt} 条")

    # ═══════════════════════════════════════
    #  6. HDF5 + DB 中的错误相关性 (由 factor_names 错位导致)
    # ═══════════════════════════════════════
    print(f"\n{'='*70}")
    print("  6. HDF5/DB 中因 factor_names 错位导致的错误记录")
    print("=" * 70)

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM qe_factor_correlations")
        total_corr = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM qe_factor_correlations WHERE abs(correlation - 1.0) < 1e-6")
        corr_1 = cur.fetchone()[0]

    print(f"  DB 总相关性记录: {total_corr}")
    print(f"  DB corr=1.0 记录: {corr_1}")
    print(f"  结论: 由于 compute_full_matrix 的 factor_names 错位 bug,")
    print(f"         DB 中 {total_corr} 条记录全部不可信!")
    print(f"         需要修复 bug 后全量重算。")

    # ═══════════════════════════════════════
    #  7. 内存评估
    # ═══════════════════════════════════════
    print(f"\n{'='*70}")
    print("  7. 全量重算内存评估")
    print("=" * 70)

    # 统计 parquet 文件的典型大小和行数
    sample_sizes = []
    for name in has_parquet[:10]:
        fp = os.path.join(SINGLE_DIR, f"{name}.parquet")
        df = pd.read_parquet(fp)
        n_rows = len(df)
        file_mb = os.path.getsize(fp) / (1024**2)
        sample_sizes.append((n_rows, file_mb))

    avg_rows = int(np.mean([s[0] for s in sample_sizes]))
    avg_file_mb = np.mean([s[1] for s in sample_sizes])

    # 获取 252 天窗口的行数
    fp = os.path.join(SINGLE_DIR, f"{has_parquet[0]}.parquet")
    df = pd.read_parquet(fp, columns=[])
    dates = df.index.get_level_values(0).unique().sort_values()
    window_dates = dates[-252:]
    window_idx = df.index[df.index.get_level_values(0).isin(window_dates)]
    n_rows_252 = len(window_idx)
    n_dates = len(window_dates)
    n_stocks = window_idx.get_level_values(1).nunique()

    n_factors = len(has_parquet)
    # 去掉重复因子
    n_factors_unique = n_factors - redundant

    print(f"\n  数据集参数:")
    print(f"    252天窗口行数: {n_rows_252:,}")
    print(f"    交易日数: {n_dates}")
    print(f"    股票数: {n_stocks}")
    print(f"    有 parquet 的因子数: {n_factors}")
    print(f"    去重后因子数: {n_factors_unique}")

    # 面板内存估算
    for dtype, bytes_per in [("float32", 4), ("float64", 8)]:
        panel_bytes = n_rows_252 * n_factors_unique * bytes_per
        panel_gb = panel_bytes / (1024**3)
        # GEMM 中间矩阵
        gemm_factor_count = n_factors_unique
        gemm_matrices = 5  # N_pairs, SX, SX2, SXY, output
        gemm_bytes = gemm_matrices * gemm_factor_count * gemm_factor_count * 8  # always float64
        gemm_gb = gemm_bytes / (1024**3)
        # daily corr stack (252 * K * K * 8)
        stack_bytes = n_dates * gemm_factor_count * gemm_factor_count * 8
        stack_gb = stack_bytes / (1024**3)
        total_gb = panel_gb + gemm_gb + stack_gb

        print(f"\n  {dtype} 方案:")
        print(f"    面板 ({n_rows_252:,} x {n_factors_unique}): {panel_gb:.2f} GB")
        print(f"    GEMM 中间矩阵 ({gemm_factor_count}x{gemm_factor_count}x5): {gemm_gb:.2f} GB")
        print(f"    日相关矩阵栈 (252x{gemm_factor_count}x{gemm_factor_count}): {stack_gb:.2f} GB")
        print(f"    总内存峰值: ~{total_gb:.2f} GB")

    # 全量因子 (不去重)
    print(f"\n  全量因子 (不去重) 内存估算:")
    for dtype, bytes_per in [("float32", 4), ("float64", 8)]:
        panel_bytes = n_rows_252 * n_factors * bytes_per
        panel_gb = panel_bytes / (1024**3)
        gemm_bytes = 5 * n_factors * n_factors * 8
        gemm_gb = gemm_bytes / (1024**3)
        stack_bytes = n_dates * n_factors * n_factors * 8
        stack_gb = stack_bytes / (1024**3)
        total_gb = panel_gb + gemm_gb + stack_gb
        print(f"    {dtype}: 面板={panel_gb:.2f}GB + GEMM={gemm_gb:.2f}GB + 栈={stack_gb:.2f}GB = {total_gb:.2f}GB")

    print(f"\n  系统内存: 64GB RAM + 256GB SWAP")
    print(f"  结论: float32 方案安全, float64 需评估峰值是否超 64GB")

    # ═══════════════════════════════════════
    #  8. 汇总
    # ═══════════════════════════════════════
    print(f"\n{'='*70}")
    print("  8. 完整问题清单汇总")
    print("=" * 70)

    print(f"""
  P0 - compute_full_matrix factor_names 错位 Bug
    位置: correlation_engine.py:253
    影响: DB 中 {total_corr} 条相关性记录全部不可信
    修复: factor_names = list(panel.columns) 无条件同步

  P1 - MD5 重复因子
    数量: {len(dup_groups)} 组, {redundant} 个冗余因子
    修复: 每组保留 IC 最高的, 其余 is_available=false

  P1 - 退化因子
    全 NaN: {len(all_nan_factors)} 个
    退化: {len(degenerate_factors)} 个
    修复: is_available=false

  P1 - 无 parquet 因子
    数量: {len(no_parquet)} 个 (is_available=true 但无数据)
    修复: 重新生成或 is_available=false

  P2 - Metrics 表重复
    数量: {total_dup_metrics} 组重复
    修复: 去重保留最新记录

  P2 - DB 相关性全量作废
    数量: {total_corr} 条
    修复: 修复 P0 后清空 + 全量重算
""")

    conn.close()

    # 保存结果到 JSON
    result = {
        "bug_root_cause": "compute_full_matrix line 253: factor_names not synced with panel.columns when len matches",
        "no_parquet_factors": [n for n, s, c in no_parquet],
        "md5_duplicate_groups": {md5: names for md5, names in dup_groups.items()},
        "disable_list_duplicates": disable_list,
        "all_nan_factors": all_nan_factors,
        "degenerate_factors": [d["name"] for d in degenerate_factors],
        "total_db_correlations_invalid": total_corr,
    }

    out_path = os.path.join(os.path.dirname(__file__), "..", "data", "factor_issues_report.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"  报告已保存: {out_path}")


if __name__ == "__main__":
    main()
