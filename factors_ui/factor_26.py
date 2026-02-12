import pandas as pd
import numpy as np

def calculate_MomentumVolAdj_20D():
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

    # 4. 尝试读取 static_factors.parquet 以获取 PriceStrength_20D 预计算因子
    try:
        static_df = pd.read_parquet("static_factors.parquet").sort_index()
        # 检查 PriceStrength_20D 是否存在
        if "PriceStrength_20D" in static_df.columns:
            df = df.join(static_df[["PriceStrength_20D"]], how="left")
            # 使用预计算的 PriceStrength_20D
            price_strength_20d = df["PriceStrength_20D"]
        else:
            # 如果列不存在，回退到自行计算
            price_strength_20d = df["close"] / df["close"].groupby(level="instrument").shift(19) - 1
    except Exception as e:
        # 如果文件不存在或读取失败，回退到自行计算
        price_strength_20d = df["close"] / df["close"].groupby(level="instrument").shift(19) - 1

    # 5. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 计算日收益率: returns = close_t / close_{t-1} - 1
    daily_ret = df["close"] / df["close"].groupby(level="instrument").shift(1) - 1

    # 计算20日收益率波动率 (σ_20D): std(returns_{t-19:t}) × √252
    # 窗口大小为20，min_periods=20，覆盖 t-19 到 t 的20个收益率，与价格强度窗口对齐
    rolling_std = daily_ret.groupby(level="instrument").rolling(window=20, min_periods=20).std(ddof=1)
    rolling_std = rolling_std.reset_index(level=0, drop=True)
    volatility_20d = rolling_std * np.sqrt(252)  # 年化波动率，假设252个交易日

    # 计算风险调整动量因子: MomentumVolAdj_20D = PriceStrength_20D / σ_20D
    # 使用 np.divide 处理除零和 NaN，提高可读性
    series = np.divide(price_strength_20d, volatility_20d, out=np.full_like(price_strength_20d, np.nan), where=volatility_20d != 0)

    # ==== END FACTOR COMPUTATION AREA ====

    # 6. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["MomentumVolAdj_20D"] = series.astype("float64")

    # 7. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 8. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_MomentumVolAdj_20D()