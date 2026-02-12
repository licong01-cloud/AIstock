import pandas as pd
import numpy as np

def calculate_SizeAdjTurnover_5D():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 再次确保索引按 (datetime, instrument) 排序
    df = df.sort_index()

    # 3. 读取静态因子数据并合并
    static_df = pd.read_parquet("static_factors.parquet").sort_index()
    df = df.join(static_df, how="left")

    # 检查所需列是否存在
    required_cols = ["db_turnover_rate", "db_circ_mv"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Please ensure static_factors.parquet contains these fields.")

    # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 计算5日平均换手率
    mean_turnover_5d = df["db_turnover_rate"].groupby(level="instrument").rolling(window=5, min_periods=5).mean()
    mean_turnover_5d = mean_turnover_5d.reset_index(level=0, drop=True)
    
    # 计算流通市值的自然对数，仅对正值计算
    log_circ_mv = np.log(df["db_circ_mv"])
    
    # 计算因子：5日平均换手率除以流通市值对数
    series = mean_turnover_5d / log_circ_mv
    # ==== END FACTOR COMPUTATION AREA ====

    # 5. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["SizeAdjTurnover_5D"] = series.astype("float64")

    # 6. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 7. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_SizeAdjTurnover_5D()