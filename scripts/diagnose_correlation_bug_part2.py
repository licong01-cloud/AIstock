"""
诊断 Part 2: 既然 GEMM 正确，追查 HDF5 中 corr=1.0 的真正来源。

假说：factor parquets 在 HDF5 生成后被更新，当前 MD5 已经不同，
但计算时它们还是相同的文件。
"""
import sys, os, time, hashlib
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import numpy as np
import pandas as pd
import h5py
from collections import defaultdict

SINGLE_DIR = r"F:\Dev\AIstock\rdagent_assets\factor_values\single"
HDF5_PATH = r"F:\Dev\AIstock\data\correlation_matrices\corr_20260403.h5"


def main():
    print("=" * 70)
    print("  追查 HDF5 corr=1.0 真正来源")
    print("=" * 70)

    # 1. HDF5 生成时间
    hdf5_mtime = os.path.getmtime(HDF5_PATH)
    hdf5_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(hdf5_mtime))
    print(f"\nHDF5 生成时间: {hdf5_time}")

    # 2. 加载 HDF5
    with h5py.File(HDF5_PATH, "r") as f:
        matrix = f["matrix"][:]
        raw_names = f.attrs["factor_names"]
        names = [s.decode("utf-8") if isinstance(s, bytes) else str(s) for s in raw_names]

    K = len(names)

    # 3. 检查所有 parquet 的修改时间
    print(f"\n检查 parquet 修改时间 vs HDF5 生成时间:")
    updated_after = []
    created_after = []
    parquet_times = {}

    for name in names:
        fpath = os.path.join(SINGLE_DIR, f"{name}.parquet")
        if os.path.isfile(fpath):
            mtime = os.path.getmtime(fpath)
            parquet_times[name] = mtime
            if mtime > hdf5_mtime:
                updated_after.append((name, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mtime))))

    print(f"HDF5 后更新的 parquet: {len(updated_after)} / {K}")
    if updated_after:
        print(f"\n在 HDF5 之后更新的因子:")
        for name, t in sorted(updated_after, key=lambda x: x[1]):
            print(f"  {name}: {t}")

    # 4. 找出 corr=1.0 的假阳性，区分 HDF5 前后
    print(f"\n" + "=" * 70)
    print(f"  corr=1.0 对分析: 计算时的 MD5 vs 当前 MD5")
    print(f"=" * 70)

    # 当前 MD5
    md5_map = {}
    for name in names:
        fpath = os.path.join(SINGLE_DIR, f"{name}.parquet")
        if os.path.isfile(fpath):
            with open(fpath, "rb") as f:
                md5_map[name] = hashlib.md5(f.read()).hexdigest()

    # 所有 corr=1.0 对
    corr1_pairs = []
    for i in range(K):
        for j in range(i+1, K):
            if abs(matrix[i, j] - 1.0) < 1e-6:
                corr1_pairs.append((names[i], names[j]))

    print(f"总 corr=1.0 对: {len(corr1_pairs)}")

    # 分类
    both_unchanged = []  # 两个因子都没在 HDF5 后更新
    one_updated = []     # 其中一个在 HDF5 后更新
    both_updated = []    # 两个都更新了
    updated_set = set(n for n, t in updated_after)

    for a, b in corr1_pairs:
        a_updated = a in updated_set
        b_updated = b in updated_set
        if a_updated and b_updated:
            both_updated.append((a, b))
        elif a_updated or b_updated:
            one_updated.append((a, b))
        else:
            both_unchanged.append((a, b))

    print(f"\n按 parquet 更新状态分类:")
    print(f"  两者都未更新: {len(both_unchanged)} 对")
    print(f"  其中一个更新: {len(one_updated)} 对")
    print(f"  两者都更新:   {len(both_updated)} 对")

    # 5. 对于"两者都未更新"的对，当前 MD5 是否相同？
    # 这些是真正的问题对 — 数据没变过，corr=1.0，但 MD5 不同
    print(f"\n--- 两者都未更新 + MD5 不同 (真假阳性) ---")
    real_false_pos = []
    real_true_dup = []
    for a, b in both_unchanged:
        md5_a = md5_map.get(a)
        md5_b = md5_map.get(b)
        if md5_a and md5_b:
            if md5_a == md5_b:
                real_true_dup.append((a, b))
            else:
                real_false_pos.append((a, b))

    print(f"  真重复 (MD5 相同): {len(real_true_dup)}")
    print(f"  真假阳性 (MD5 不同): {len(real_false_pos)}")

    if real_false_pos:
        print(f"\n  真假阳性详情:")
        for a, b in real_false_pos[:20]:
            print(f"    {a} <-> {b}")

    # 6. 对于"一个更新"的对，检查更新前是否可能是同一文件
    print(f"\n--- 其中一个更新的对 ---")
    if one_updated:
        # 按更新的那个因子分组
        updated_factor_groups = defaultdict(list)
        for a, b in one_updated:
            if a in updated_set:
                updated_factor_groups[a].append(b)
            else:
                updated_factor_groups[b].append(a)

        print(f"  涉及 {len(updated_factor_groups)} 个更新后的因子:")
        for uf, partners in sorted(updated_factor_groups.items(), key=lambda x: -len(x[1])):
            mtime_str = time.strftime("%m-%d %H:%M", time.localtime(parquet_times.get(uf, 0)))
            print(f"    {uf} (更新于{mtime_str}): 与 {len(partners)} 个因子 corr=1.0")
            # 检查 partners 之间当前 MD5 是否相同
            partner_md5s = [md5_map.get(p) for p in partners]
            unique_md5s = set(m for m in partner_md5s if m)
            if len(unique_md5s) == 1:
                print(f"      -> 所有 partners 当前 MD5 相同!")
                uf_md5 = md5_map.get(uf)
                if uf_md5 in unique_md5s:
                    print(f"      -> 更新后的因子 MD5 仍与 partners 相同 (内容未变)")
                else:
                    print(f"      -> 更新后的因子 MD5 已不同 (内容已变)")
            elif len(unique_md5s) > 1:
                print(f"      -> partners 有 {len(unique_md5s)} 种不同 MD5")

    # 7. 验证"真假阳性": 用当前数据重算 Spearman
    if real_false_pos:
        from scipy import stats
        print(f"\n--- 真假阳性验证: 用当前数据重算 ---")
        # 找测试日期
        first_fp_name = real_false_pos[0][0]
        fp = os.path.join(SINGLE_DIR, f"{first_fp_name}.parquet")
        df = pd.read_parquet(fp, columns=[])
        dates = df.index.get_level_values(0).unique().sort_values()
        test_date = dates[-10]
        print(f"测试日期: {test_date.strftime('%Y-%m-%d')}")

        for a, b in real_false_pos[:10]:
            fp_a = os.path.join(SINGLE_DIR, f"{a}.parquet")
            fp_b = os.path.join(SINGLE_DIR, f"{b}.parquet")
            if not os.path.isfile(fp_a) or not os.path.isfile(fp_b):
                continue
            df_a = pd.read_parquet(fp_a)
            df_b = pd.read_parquet(fp_b)
            if "value" in df_a.columns:
                df_a = df_a.rename(columns={"value": a})
            if "value" in df_b.columns:
                df_b = df_b.rename(columns={"value": b})
            try:
                sec_a = df_a.loc[test_date]
                sec_b = df_b.loc[test_date]
            except KeyError:
                print(f"  {a} <-> {b}: 无数据")
                continue
            merged = pd.concat([sec_a, sec_b], axis=1).dropna()
            if len(merged) < 30:
                print(f"  {a} <-> {b}: 有效数据不足 ({len(merged)})")
                continue
            corr, _ = stats.spearmanr(merged.iloc[:, 0], merged.iloc[:, 1])
            print(f"  {a} <-> {b}: 当前scipy={corr:.6f}, HDF5=1.0")

    # 8. 全局统计: MD5 重复组
    print(f"\n" + "=" * 70)
    print(f"  MD5 重复组分析 (当前状态)")
    print(f"=" * 70)

    md5_groups = defaultdict(list)
    for name, md5 in md5_map.items():
        md5_groups[md5].append(name)

    dup_groups = {md5: names for md5, names in md5_groups.items() if len(names) > 1}
    print(f"总 MD5 唯一值: {len(md5_groups)}")
    print(f"重复组(2+因子): {len(dup_groups)}")

    total_dups = sum(len(v) for v in dup_groups.values())
    print(f"重复因子总数: {total_dups}")

    for md5, fnames in sorted(dup_groups.items(), key=lambda x: -len(x[1])):
        print(f"\n  组({len(fnames)}因子): {', '.join(fnames[:6])}{'...' if len(fnames)>6 else ''}")
        # 期望的 corr=1.0 对数
        expected_pairs = len(fnames) * (len(fnames) - 1) // 2
        # 实际 HDF5 中 corr=1.0 的对数
        actual_pairs = 0
        for i, fn_i in enumerate(fnames):
            if fn_i not in names:
                continue
            idx_i = names.index(fn_i)
            for fn_j in fnames[i+1:]:
                if fn_j not in names:
                    continue
                idx_j = names.index(fn_j)
                if abs(matrix[idx_i, idx_j] - 1.0) < 1e-6:
                    actual_pairs += 1
        print(f"    期望 corr=1.0 对数: {expected_pairs}, 实际: {actual_pairs}")

    # 9. 检查 HDF5 factor_names 的顺序
    print(f"\n" + "=" * 70)
    print(f"  因子名顺序检查")
    print(f"=" * 70)
    is_sorted = names == sorted(names)
    print(f"  HDF5 factor_names 是否字母序: {is_sorted}")
    if not is_sorted:
        # 找出不按顺序的位置
        sorted_names = sorted(names)
        mismatches = [(i, names[i], sorted_names[i])
                      for i in range(K) if names[i] != sorted_names[i]]
        print(f"  不匹配位置数: {len(mismatches)}")
        if mismatches:
            print(f"  前5个不匹配:")
            for i, actual, expected in mismatches[:5]:
                print(f"    [{i}] actual={actual}, expected={expected}")

    # 10. 检查是否有因子名重复
    name_counts = defaultdict(int)
    for n in names:
        name_counts[n] += 1
    dups = {n: c for n, c in name_counts.items() if c > 1}
    if dups:
        print(f"\n  重复因子名: {dups}")
    else:
        print(f"\n  无重复因子名")


if __name__ == "__main__":
    main()
