import pandas as pd
import numpy as np

def calculate_DivAdjTurnover_20D():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 再次确保索引按 (datetime, instrument) 排序
    df = df.sort_index()

    # 3. 尝试读取静态因子表以获取换手率和股息率TTM字段
    try:
        static_df = pd.read_parquet("static_factors.parquet").sort_index()
        # 检查所需列是否存在
        required_cols = ["db_turnover_rate", "db_dv_ttm"]
        missing = [c for c in required_cols if c not in static_df.columns]
        if missing:
            # 如果列缺失，降级为仅依赖价量数据，输出全NaN并打印警告
            print(f"Warning: Missing columns in static_factors.parquet: {missing}. Factor will be set to NaN.")
            series = pd.Series(np.nan, index=df.index, dtype="float64")
        else:
            # 合并静态因子数据
            df = df.join(static_df[required_cols], how="left")
            
            # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
            # 计算过去20个交易日的平均换手率，使用min_periods=1以最大化数据可用性
            window_size = 20
            avg_turnover = df["db_turnover_rate"].groupby(level="instrument").rolling(window=window_size, min_periods=1).mean()
            avg_turnover = avg_turnover.reset_index(level=0, drop=True)
            
            # 将换手率从百分比转换为小数形式（除以100），避免量纲问题
            avg_turnover_decimal = avg_turnover / 100.0
            
            # 计算股息率调整换手率因子：平均换手率（小数）乘以股息率TTM（百分比）
            series = avg_turnover_decimal * df["db_dv_ttm"]
            # ==== END FACTOR COMPUTATION AREA ====
    except FileNotFoundError:
        # 如果静态因子表不存在，降级为仅依赖价量数据，输出全NaN并打印警告
        print("Warning: static_factors.parquet not found. Factor will be set to NaN.")
        series = pd.Series(np.nan, index=df.index, dtype="float64")

    # 5. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["DivAdjTurnover_20D"] = series.astype("float64")

    # 6. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 7. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_DivAdjTurnover_20D()