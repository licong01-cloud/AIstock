import pandas as pd
import numpy as np

def calculate_size_log_mv():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 再次确保索引按 (datetime, instrument) 排序
    df = df.sort_index()

    # 3. 读取静态因子数据并合并
    try:
        static_df = pd.read_parquet("static_factors.parquet").sort_index()
        df = df.join(static_df, how="left")
    except FileNotFoundError:
        # 如果 static_factors.parquet 不存在，则仅使用 daily_pv.h5 的字段
        pass

    # 4. 检查所需列是否存在
    required_cols = ["db_circ_mv"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Please redesign factor using available fields.")

    # 5. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 计算流通市值的自然对数，对 <=0 的值置为 NaN
    series = np.log(df["db_circ_mv"])
    series[df["db_circ_mv"] <= 0] = np.nan
    # ==== END FACTOR COMPUTATION AREA ====

    # 6. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["size_log_mv"] = series.astype("float64")

    # 7. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 8. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_size_log_mv()