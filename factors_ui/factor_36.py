import pandas as pd
import numpy as np

def calculate_mf_elg_net_amt_ratio_stability_5D():
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
        # 如果 static_factors.parquet 文件不存在，降级处理
        print("Warning: static_factors.parquet not found. Outputting all NaN values.")
        series = pd.Series(np.nan, index=df.index, dtype="float64")
        result_df = pd.DataFrame(index=df.index)
        result_df["mf_elg_net_amt_ratio_stability_5D"] = series.astype("float64")
        result_df.index.names = df.index.names
        result_df = result_df.sort_index()
        result_df.to_hdf("result.h5", key="data", mode="w")
        return result_df

    # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 检查所需预计算字段是否存在
    required_cols = ["mf_elg_net_amt_ratio"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        # 如果静态字段缺失，尝试使用原始字段计算降级版本
        print(f"Warning: Missing columns: {missing}. Attempting to compute from raw fields.")
        # 检查原始字段是否存在
        raw_cols = ["mf_elg_buy_amt", "mf_elg_sell_amt", "amount"]
        missing_raw = [c for c in raw_cols if c not in df.columns]
        if missing_raw:
            print(f"Warning: Missing raw columns: {missing_raw}. Outputting all NaN values.")
            series = pd.Series(np.nan, index=df.index, dtype="float64")
        else:
            # 计算 mf_elg_net_amt_ratio 降级版本
            numerator = df["mf_elg_buy_amt"] - df["mf_elg_sell_amt"]
            denominator = df["amount"]
            mf_elg_net_amt_ratio = numerator / denominator
            mf_elg_net_amt_ratio = mf_elg_net_amt_ratio.replace([np.inf, -np.inf], np.nan)
    else:
        # 使用预计算字段 mf_elg_net_amt_ratio
        mf_elg_net_amt_ratio = df["mf_elg_net_amt_ratio"]
    
    # 如果 mf_elg_net_amt_ratio 未定义（例如降级计算失败），则输出全 NaN
    if 'mf_elg_net_amt_ratio' not in locals():
        series = pd.Series(np.nan, index=df.index, dtype="float64")
    else:
        # 计算5日滚动均值和标准差，min_periods=5 确保完整窗口
        rolling_mean = mf_elg_net_amt_ratio.groupby(level="instrument").rolling(window=5, min_periods=5).mean()
        rolling_std = mf_elg_net_amt_ratio.groupby(level="instrument").rolling(window=5, min_periods=5).std()
        # 重置索引以对齐原始df
        rolling_mean = rolling_mean.reset_index(level=0, drop=True)
        rolling_std = rolling_std.reset_index(level=0, drop=True)
        
        # 计算变异系数倒数：|mean| / std，正确处理除零
        abs_mean = np.abs(rolling_mean)
        # 当标准差为零时，变异系数未定义，返回 NaN
        series = np.where(rolling_std > 0, abs_mean / rolling_std, np.nan)
        series = pd.Series(series, index=df.index, dtype="float64")
    # ==== END FACTOR COMPUTATION AREA ====

    # 5. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["mf_elg_net_amt_ratio_stability_5D"] = series.astype("float64")

    # 6. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 7. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_mf_elg_net_amt_ratio_stability_5D()