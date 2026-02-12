import pandas as pd
import numpy as np

def calculate_SizeAdjElgNet_5D():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 再次确保索引按 (datetime, instrument) 排序
    df = df.sort_index()

    # 3. 读取静态因子数据并合并
    static_df = pd.read_parquet("static_factors.parquet").sort_index()
    df = df.join(static_df, how="left")

    # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 检查所需列是否存在
    required_cols = ["mf_elg_buy_amt", "mf_elg_sell_amt", "amount", "db_circ_mv"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Please redesign factor using available fields.")

    # 计算每日特大单净流入强度 mf_elg_net_amt_ratio_t
    numerator = df["mf_elg_buy_amt"] - df["mf_elg_sell_amt"]
    denominator = df["amount"]
    # 当分母为 0 或 NaN 时，设置因子值为 NaN
    daily_ratio = np.where(denominator == 0, np.nan, numerator / denominator)
    daily_ratio = np.where(pd.isna(denominator), np.nan, daily_ratio)
    daily_ratio = pd.Series(daily_ratio, index=df.index, dtype="float64")

    # 计算过去5个交易日的滚动均值，min_periods=5 确保数据充足
    mean_5D = daily_ratio.groupby(level="instrument").rolling(window=5, min_periods=5).mean()
    mean_5D = mean_5D.reset_index(level=0, drop=True)  # 恢复索引对齐

    # 计算流通市值对数，仅对正值计算，非正值设为NaN
    log_circ_mv = np.where(df["db_circ_mv"] > 0, np.log(df["db_circ_mv"]), np.nan)
    log_circ_mv = pd.Series(log_circ_mv, index=df.index, dtype="float64")

    # 计算规模调整因子：滚动均值除以市值对数
    series = mean_5D / log_circ_mv
    series = pd.Series(series, index=df.index, dtype="float64")
    # ==== END FACTOR COMPUTATION AREA ====

    # 5. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["SizeAdjElgNet_5D"] = series.astype("float64")

    # 6. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 7. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_SizeAdjElgNet_5D()