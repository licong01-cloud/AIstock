import pandas as pd
import numpy as np

def calculate_DividendYieldStability_20D():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 再次确保索引按 (datetime, instrument) 排序
    df = df.sort_index()

    # 3. 读取静态因子表并 join
    try:
        static_df = pd.read_parquet("static_factors.parquet").sort_index()
        df = df.join(static_df, how="left")
    except Exception as e:
        # 如果静态因子表不存在或读取失败，则输出全 NaN 并解释原因
        print(f"Warning: static_factors.parquet not available or failed to load: {e}")
        print("Factor DividendYieldStability_20D requires db_dv_ttm, outputting all NaN.")
        result_df = pd.DataFrame(index=df.index)
        result_df["DividendYieldStability_20D"] = np.nan
        result_df.index.names = df.index.names
        result_df = result_df.sort_index()
        result_df.to_hdf("result.h5", key="data", mode="w")
        return result_df

    # 4. 检查所需列是否存在
    required_cols = ["db_dv_ttm"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Please ensure static_factors.parquet contains db_dv_ttm.")

    # 5. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 计算过去20个交易日的滚动均值和标准差
    mean_20d = df["db_dv_ttm"].groupby(level="instrument").rolling(window=20, min_periods=20).mean()
    std_20d = df["db_dv_ttm"].groupby(level="instrument").rolling(window=20, min_periods=20).std(ddof=0)
    
    # 重置索引以与原始 df.index 对齐
    mean_20d = mean_20d.reset_index(level=0, drop=True)
    std_20d = std_20d.reset_index(level=0, drop=True)
    
    # 计算变异系数倒数：|mean| / std，避免除零和负值问题
    # 当 std = 0 或 mean = 0 时，结果可能为 inf 或 NaN，我们保留 NaN
    stability = np.abs(mean_20d) / std_20d
    
    # 将稳定性序列赋值给 series
    series = stability
    # ==== END FACTOR COMPUTATION AREA ====

    # 6. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["DividendYieldStability_20D"] = series.astype("float64")

    # 7. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 8. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_DividendYieldStability_20D()