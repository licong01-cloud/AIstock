import pandas as pd
import numpy as np

def calculate_PVF_Sync_10D():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 再次确保索引按 (datetime, instrument) 排序
    df = df.sort_index()

    # 3. 读取静态因子数据（用于资金流字段）
    try:
        static_df = pd.read_parquet("static_factors.parquet").sort_index()
        df = df.join(static_df, how="left")
    except FileNotFoundError:
        # 如果静态因子文件不存在，则只使用 daily_pv 数据
        pass

    # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 检查必需字段是否存在
    required_cols = ["close", "volume", "amount"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Please redesign factor using available fields.")
    
    # 计算日收益率 r_t = close_t / close_{t-1} - 1
    r_t = df["close"].groupby(level="instrument").pct_change()
    
    # 计算成交量变化率 Δv_t = volume_t / volume_{t-1} - 1
    delta_v_t = df["volume"].groupby(level="instrument").pct_change()
    
    # 计算主力净流入强度 mf_main_net_amt_ratio_t
    # 检查资金流字段是否存在
    mf_cols = ["mf_lg_buy_amt", "mf_elg_buy_amt", "mf_lg_sell_amt", "mf_elg_sell_amt"]
    if all(col in df.columns for col in mf_cols):
        mf_main_net_amt = (df["mf_lg_buy_amt"] + df["mf_elg_buy_amt"]) - (df["mf_lg_sell_amt"] + df["mf_elg_sell_amt"])
        # 安全除法，避免除零或缺失值
        amount_safe = df["amount"].replace(0, np.nan)
        mf_main_net_amt_ratio_t = mf_main_net_amt / amount_safe
        mf_available = True
    else:
        # 如果资金流字段缺失，则输出全 NaN 并终止计算
        series = pd.Series(np.nan, index=df.index)
        print("Warning: Capital flow data not available. PVF_Sync_10D requires mf_lg_buy_amt, mf_elg_buy_amt, mf_lg_sell_amt, mf_elg_sell_amt. Outputting NaN.")
        # 直接构造结果并返回
        result_df = pd.DataFrame(index=df.index)
        result_df["PVF_Sync_10D"] = series.astype("float64")
        result_df.index.names = df.index.names
        result_df = result_df.sort_index()
        result_df.to_hdf("result.h5", key="data", mode="w")
        return result_df
    
    # 创建临时 DataFrame 用于滚动计算相关系数
    temp_df = pd.DataFrame({
        "r_t": r_t,
        "delta_v_t": delta_v_t,
        "mf_main_net_amt_ratio_t": mf_main_net_amt_ratio_t
    }, index=df.index)
    
    # 定义滚动窗口长度和最小观测值
    window = 10
    min_periods = 8  # 提高稳健性，要求窗口内至少8个有效数据点
    
    # 计算三个滚动相关系数
    # 使用 groupby + rolling + corr 的正确方法
    # 1. 价格与成交量的相关系数
    corr_r_delta_v = temp_df.groupby(level="instrument")[["r_t", "delta_v_t"]].rolling(window=window, min_periods=min_periods).corr().unstack().iloc[:, 1]
    corr_r_delta_v = corr_r_delta_v.droplevel(level=0).reindex(df.index)
    
    # 2. 价格与主力净流入强度的相关系数
    corr_r_mf = temp_df.groupby(level="instrument")[["r_t", "mf_main_net_amt_ratio_t"]].rolling(window=window, min_periods=min_periods).corr().unstack().iloc[:, 1]
    corr_r_mf = corr_r_mf.droplevel(level=0).reindex(df.index)
    
    # 3. 成交量与主力净流入强度的相关系数
    corr_delta_v_mf = temp_df.groupby(level="instrument")[["delta_v_t", "mf_main_net_amt_ratio_t"]].rolling(window=window, min_periods=min_periods).corr().unstack().iloc[:, 1]
    corr_delta_v_mf = corr_delta_v_mf.droplevel(level=0).reindex(df.index)
    
    # 计算三个相关系数的平均值
    series = (corr_r_delta_v + corr_r_mf + corr_delta_v_mf) / 3
    
    # ==== END FACTOR COMPUTATION AREA ====

    # 5. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["PVF_Sync_10D"] = series.astype("float64")

    # 6. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 7. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_PVF_Sync_10D()