import pandas as pd
import numpy as np

def calculate_MomentumVolAdj_10D():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 再次确保索引按 (datetime, instrument) 排序
    df = df.sort_index()

    # 3. 检查必需列
    required_cols = ["close"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Please redesign factor using available fields.")

    # 4. 尝试读取静态因子表以获取 PriceStrength_10D
    try:
        static_df = pd.read_parquet("static_factors.parquet").sort_index()
        if "PriceStrength_10D" in static_df.columns:
            df = df.join(static_df[["PriceStrength_10D"]], how="left")
            price_strength_10d = df["PriceStrength_10D"]
        else:
            # 如果静态表中没有 PriceStrength_10D，则自行计算
            price_strength_10d = df["close"] / df["close"].groupby(level="instrument").shift(9) - 1
    except FileNotFoundError:
        # 如果静态因子表不存在，则自行计算
        price_strength_10d = df["close"] / df["close"].groupby(level="instrument").shift(9) - 1

    # 5. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 计算日收益率: returns = close_t / close_{t-1} - 1
    daily_ret = df["close"] / df["close"].groupby(level="instrument").shift(1) - 1

    # 计算10日收益率波动率: σ_10D = std(returns_{t-9:t}) × √252
    rolling_std = daily_ret.groupby(level="instrument").rolling(window=10, min_periods=10).std(ddof=1)
    rolling_std = rolling_std.reset_index(level=0, drop=True)
    volatility_10d = rolling_std * np.sqrt(252)

    # 计算风险调整动量因子: MomentumVolAdj_10D = PriceStrength_10D / σ_10D
    # 当波动率为零时，返回 NaN 而不是 0，以避免引入噪声
    series = np.where(volatility_10d != 0, price_strength_10d / volatility_10d, np.nan)

    # ==== END FACTOR COMPUTATION AREA ====

    # 6. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["MomentumVolAdj_10D"] = series.astype("float64")

    # 7. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 8. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_MomentumVolAdj_10D()