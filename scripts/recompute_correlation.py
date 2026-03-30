"""使用相关性引擎重新计算两个因子的相关性"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from services.quantevolver.factor_value_loader import FactorValueLoader
from services.quantevolver.correlation_engine import CorrelationEngine

def main():
    loader = FactorValueLoader(source="single")
    engine = CorrelationEngine(loader)

    factor_a = "Earnings_Growth_Acceleration"
    factor_b = "turnover_adjusted_momentum_10d"

    print(f"=== 使用相关性引擎计算 ===")
    print(f"因子A: {factor_a}")
    print(f"因子B: {factor_b}\n")

    # 计算这两个因子的相关性矩阵
    result = engine.compute_full_matrix(
        factor_names=[factor_a, factor_b],
        as_of_date="2026-03-12",
        save_hdf5=False
    )

    print(f"计算完成:")
    print(f"矩阵大小: {result.matrix.shape}")
    print(f"因子列表: {result.factor_names}")
    print(f"有效窗口: {result.effective_window} 天")
    print(f"计算耗时: {result.computation_time_sec:.2f}s\n")

    print(f"=== 相关性矩阵 ===")
    print(f"{factor_a} vs {factor_a}: {result.matrix[0, 0]:.6f}")
    print(f"{factor_a} vs {factor_b}: {result.matrix[0, 1]:.6f}")
    print(f"{factor_b} vs {factor_a}: {result.matrix[1, 0]:.6f}")
    print(f"{factor_b} vs {factor_b}: {result.matrix[1, 1]:.6f}")

if __name__ == "__main__":
    main()
