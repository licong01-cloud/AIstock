import pandas as pd
import numpy as np

def calculate_SizeAdjElgNet_5D():
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
        # 如果静态因子文件不存在，则输出全 NaN 并解释原因
        result_df = pd.DataFrame(index=df.index)
        result_df["SizeAdjElgNet_5D"] = np.nan
        result_df.index.names = df.index.names
        result_df = result_df.sort_index()
        result_df.to_hdf("result.h5", key="data", mode="w")
        print("Warning: static_factors.parquet not found. Factor values set to NaN.")
        return result_df

    # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 检查所需列是否存在
    required_cols = ["amount", "mf_elg_buy_amt", "mf_elg_sell_amt", "db_circ_mv"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Please redesign factor using available fields.")

    # 计算特大单净流入强度 (mf_elg_net_amt_ratio_t)
    numerator = df["mf_elg_buy_amt"] - df["mf_elg_sell_amt"]
    denominator = df["amount"]
    # 处理分母为零或 NaN 的情况
    mf_elg_net_amt_ratio_t = np.where((denominator == 0) | pd.isna(denominator), np.nan, numerator / denominator)
    mf_elg_net_amt_ratio_t = pd.Series(mf_elg_net_amt_ratio_t, index=df.index, dtype="float64")

    # 计算过去5个交易日的滚动均值
    window = 5
    mean_5D = mf_elg_net_amt_ratio_t.groupby(level="instrument").rolling(window=window, min_periods=window).mean()
    mean_5D = mean_5D.reset_index(level=0, drop=True)  # 恢复与 df.index 对齐

    # 计算流通市值对数，仅对正值计算
    log_circ_mv = np.where(df["db_circ_mv"] > 0, np.log(df["db_circ_mv"]), np.nan)
    log_circ_mv = pd.Series(log_circ_mv, index=df.index, dtype="float64")

    # 计算规模调整因子：均值除以对数市值
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