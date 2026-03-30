"""检查HDF5文件中存储的相关性值"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import h5py
import numpy as np

def main():
    hdf5_dir = r"F:\Dev\AIstock\data\correlation_matrices"

    # 找到最新的HDF5文件
    files = sorted([f for f in os.listdir(hdf5_dir) if f.startswith("corr_") and f.endswith(".h5")], reverse=True)

    if not files:
        print("未找到HDF5文件")
        return

    latest = os.path.join(hdf5_dir, files[0])
    print(f"最新HDF5文件: {files[0]}\n")

    with h5py.File(latest, "r") as f:
        # 因子名称在attrs中，不是dataset
        raw_names = f.attrs["factor_names"]
        factor_names = [
            s.decode("utf-8") if isinstance(s, bytes) else str(s)
            for s in raw_names
        ]
        matrix = f["matrix"][:]

        print(f"矩阵大小: {matrix.shape}")
        print(f"因子数量: {len(factor_names)}\n")

        # 查找目标因子
        target_a = "Earnings_Growth_Acceleration"
        target_b = "turnover_adjusted_momentum_10d"

        if target_a not in factor_names:
            print(f"[ERROR] 因子A '{target_a}' 不在矩阵中")
            return
        if target_b not in factor_names:
            print(f"[ERROR] 因子B '{target_b}' 不在矩阵中")
            return

        idx_a = factor_names.index(target_a)
        idx_b = factor_names.index(target_b)

        corr_value = matrix[idx_a, idx_b]

        print(f"=== HDF5中存储的相关性 ===")
        print(f"因子A: {target_a} (索引 {idx_a})")
        print(f"因子B: {target_b} (索引 {idx_b})")
        print(f"相关性值: {corr_value:.6f}")
        print(f"对称位置值: {matrix[idx_b, idx_a]:.6f}")

        # 检查因子A的自相关
        print(f"\n因子A自相关: {matrix[idx_a, idx_a]:.6f}")
        print(f"因子B自相关: {matrix[idx_b, idx_b]:.6f}")

        # 检查因子A与其他因子的相关性
        print(f"\n因子A与其他因子的相关性（前10个）:")
        for i in range(min(10, len(factor_names))):
            if i != idx_a:
                print(f"  {factor_names[i]}: {matrix[idx_a, i]:.6f}")

if __name__ == "__main__":
    main()
