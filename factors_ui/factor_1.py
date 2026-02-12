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

    # 5. 计算 mf_main_net_amt（如果不存在）
    if "mf_main_net_amt" not in df.columns:
        # 根据 schema 定义计算主力净流入金额：大单+特大单买入金额减去卖出金额
        buy_cols = ["mf_lg_buy_amt", "mf_elg_buy_amt"]
        sell_cols = ["mf_lg_sell_amt", "mf_elg_sell_amt"]
        
        # 检查所需资金流列是否存在
        flow_cols = buy_cols + sell_cols
        missing_flow = [c for c in flow_cols if c not in df.columns]
        if missing_flow:
            # 如果资金流数据缺失，则无法计算该因子，返回全 NaN
            series = pd.Series(np.nan, index=df.index, dtype="float64")
        else:
            # 计算主力净流入金额
            df["mf_main_net_amt"] = df[buy_cols].sum(axis=1) - df[sell_cols].sum(axis=1)
    
    # 6. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 计算 5 日主力资金净流入强度
    if "mf_main_net_amt" in df.columns:
        # 计算过去5个交易日的滚动求和
        window = 5
        
        # 主力净流入金额的5日滚动和
        main_net_sum = df["mf_main_net_amt"].groupby(level="instrument").rolling(window=window, min_periods=window).sum()
        main_net_sum = main_net_sum.reset_index(level=0, drop=True)
        
        # 成交额的5日滚动和
        amount_sum = df["amount"].groupby(level="instrument").rolling(window=window, min_periods=window).sum()
        amount_sum = amount_sum.reset_index(level=0, drop=True)
        
        # 计算比率
        series = main_net_sum / amount_sum
    else:
        # 如果 mf_main_net_amt 不存在（资金流数据缺失），则返回全 NaN
        series = pd.Series(np.nan, index=df.index, dtype="float64")
    
    # ==== END FACTOR COMPUTATION AREA ====

    # 7. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["mf_main_net_amt_ratio_5d"] = series.astype("float64")

    # 8. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 9. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_mf_main_net_amt_ratio_5d()