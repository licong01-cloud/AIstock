import pandas as pd
import numpy as np

def calculate_PriceVolumeDivergence_5D():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 再次确保索引按 (datetime, instrument) 排序
    df = df.sort_index()

    # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 计算收盘价变化率
    close_ret = df["close"] / df["close"].groupby(level="instrument").shift(1) - 1
    
    # 计算成交量变化率
    volume_ret = df["volume"] / df["volume"].groupby(level="instrument").shift(1) - 1
    
    # 将变化率组合成 DataFrame 以便滚动计算相关系数
    ret_df = pd.DataFrame({
        'close_ret': close_ret,
        'volume_ret': volume_ret
    }, index=df.index)
    
    # 定义滚动相关系数计算函数
    def rolling_corr(group):
        return group['close_ret'].rolling(window=5, min_periods=5).corr(group['volume_ret'])
    
    # 按 instrument 分组计算 5 日滚动相关系数
    series = ret_df.groupby(level="instrument", group_keys=False).apply(rolling_corr)
    
    # 确保索引对齐
    series = series.reindex(df.index)
    
    # ==== END FACTOR COMPUTATION AREA ====

    # 5. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["PriceVolumeDivergence_5D"] = series.astype("float64")

    # 6. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 7. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_PriceVolumeDivergence_5D()