import pandas as pd
import numpy as np

def calculate_DivAdjTurnover_20D():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 再次确保索引按 (datetime, instrument) 排序
    df = df.sort_index()

    # 3. 读取静态因子表以获取换手率和股息率TTM字段
    try:
        static_df = pd.read_parquet("static_factors.parquet").sort_index()
    except FileNotFoundError:
        # 如果静态因子表不存在，因子无法计算，输出全NaN
        result_df = pd.DataFrame(index=df.index)
        result_df["DivAdjTurnover_20D"] = np.nan
        result_df.index.names = df.index.names
        result_df = result_df.sort_index()
        result_df.to_hdf("result.h5", key="data", mode="w")
        print("Warning: static_factors.parquet not found. Factor values set to NaN.")
        return result_df
    
    # 检查所需列是否存在
    required_cols = ["db_turnover_rate", "db_dv_ttm"]
    missing = [c for c in required_cols if c not in static_df.columns]
    if missing:
        # 如果缺失列，因子无法计算，输出全NaN
        result_df = pd.DataFrame(index=df.index)
        result_df["DivAdjTurnover_20D"] = np.nan
        result_df.index.names = df.index.names
        result_df = result_df.sort_index()
        result_df.to_hdf("result.h5", key="data", mode="w")
        print(f"Warning: Missing columns in static_factors.parquet: {missing}. Factor values set to NaN.")
        return result_df
    
    # 合并静态因子数据
    df = df.join(static_df[required_cols], how="left")
    
    # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 计算过去20个交易日的平均换手率
    window_size = 20
    avg_turnover = df["db_turnover_rate"].groupby(level="instrument").rolling(window=window_size, min_periods=window_size).mean()
    avg_turnover = avg_turnover.reset_index(level=0, drop=True)
    
    # 获取当前股息率TTM
    dv_ttm = df["db_dv_ttm"]
    
    # 计算股息率调整换手率：平均换手率乘以股息率TTM
    series = avg_turnover * dv_ttm
    # ==== END FACTOR COMPUTATION AREA ====

    # 5. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["DivAdjTurnover_20D"] = series.astype("float64")

    # 6. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 7. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_DivAdjTurnover_20D()