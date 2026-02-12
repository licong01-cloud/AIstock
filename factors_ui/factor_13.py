import pandas as pd
import numpy as np

def calculate_mf_main_net_amt_ratio_5d():
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
        # 如果静态因子文件不存在，则只使用基础数据
        pass

    # 4. 检查所需列是否存在
    required_cols = ["amount"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Please redesign factor using available fields.")

    # 5. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 检查资金流列是否存在，如果缺失则返回全 NaN
    flow_cols = ["mf_lg_buy_amt", "mf_elg_buy_amt", "mf_lg_sell_amt", "mf_elg_sell_amt"]
    missing_flow = [c for c in flow_cols if c not in df.columns]
    if missing_flow:
        # 资金流数据缺失，返回全 NaN
        series = pd.Series(np.nan, index=df.index, dtype="float64")
    else:
        # 计算每日主力净流入金额
        daily_main_net_amt = df["mf_lg_buy_amt"] + df["mf_elg_buy_amt"] - df["mf_lg_sell_amt"] - df["mf_elg_sell_amt"]
        
        # 计算过去5个交易日的滚动求和
        window = 5
        min_periods = 1  # 提高覆盖率，允许部分缺失
        
        # 主力净流入金额的5日滚动和
        net_amt_5d = daily_main_net_amt.groupby(level="instrument").rolling(window=window, min_periods=min_periods).sum()
        net_amt_5d = net_amt_5d.reset_index(level=0, drop=True)
        
        # 成交额的5日滚动和
        amount_5d = df["amount"].groupby(level="instrument").rolling(window=window, min_periods=min_periods).sum()
        amount_5d = amount_5d.reset_index(level=0, drop=True)
        
        # 计算比率，分母为零时设为 NaN
        series = net_amt_5d.div(amount_5d).where(amount_5d != 0, np.nan)
    # ==== END FACTOR COMPUTATION AREA ====

    # 6. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["mf_main_net_amt_ratio_5d"] = series.astype("float64")

    # 7. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 8. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_mf_main_net_amt_ratio_5d()