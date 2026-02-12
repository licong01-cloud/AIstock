import pandas as pd
import numpy as np

def calculate_PriceStrength_10D():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 再次确保索引按 (datetime, instrument) 排序
    df = df.sort_index()

    # 3. 检查必需字段是否存在
    required_cols = ["close"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Please redesign factor using available fields.")

    # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 计算10日价格强度：当前收盘价除以10个交易日前的收盘价减1
    # 使用groupby按股票分组，shift(9)获取t-9日的收盘价
    close_t_minus_9 = df["close"].groupby(level="instrument").shift(9)
    # 计算因子值
    series = df["close"] / close_t_minus_9 - 1
    # ==== END FACTOR COMPUTATION AREA ====

    # 5. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["PriceStrength_10D"] = series.astype("float64")

    # 6. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 7. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_PriceStrength_10D()