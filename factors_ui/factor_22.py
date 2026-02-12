import pandas as pd
import numpy as np

def calculate_mf_main_net_amt_stability_5D():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 再次确保索引按 (datetime, instrument) 排序
    df = df.sort_index()

    # 3. 读取静态因子数据并合并
    try:
        static_df = pd.read_parquet("static_factors.parquet").sort_index()
        df = df.join(static_df, how="left")
    except Exception as e:
        print(f"Warning: Failed to load static_factors.parquet: {e}. Proceeding with daily_pv data only.")
        # 如果静态数据加载失败，df 将只包含 daily_pv 的列

    # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 检查所需原始字段是否存在
    required_cols = ["mf_main_net_amt"]
    missing = [c for c in required_cols if c not in df.columns]
    
    if missing:
        # 根据约束，静态数据缺失时不应让脚本崩溃，输出全 NaN
        print(f"Warning: Missing columns: {missing}. Outputting all NaN for factor mf_main_net_amt_stability_5D.")
        series = pd.Series(np.nan, index=df.index, dtype="float64")
    else:
        # 使用 groupby + rolling 计算 5 日滚动均值和标准差，min_periods=5 确保稳定性计算有效
        rolling_mean = df["mf_main_net_amt"].groupby(level="instrument").rolling(window=5, min_periods=5).mean()
        rolling_mean = rolling_mean.reset_index(level=0, drop=True)  # 恢复索引对齐
        
        rolling_std = df["mf_main_net_amt"].groupby(level="instrument").rolling(window=5, min_periods=5).std()
        rolling_std = rolling_std.reset_index(level=0, drop=True)  # 恢复索引对齐
        
        # 计算变异系数：标准差 / |均值|，严格遵循公式，分母为零时结果为 NaN
        series = rolling_std.div(rolling_mean.abs())
    # ==== END FACTOR COMPUTATION AREA ====

    # 5. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["mf_main_net_amt_stability_5D"] = series.astype("float64")

    # 6. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 7. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_mf_main_net_amt_stability_5D()