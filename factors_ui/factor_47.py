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
            print(f"Warning: Missing columns in static_factors.parquet: {missing}. Outputting NaN.")
            # 创建全NaN序列与df索引对齐
            series = pd.Series(np.nan, index=df.index, dtype="float64")
        else:
            # 合并静态因子数据
            df = df.join(static_df[required_cols], how="left")
            
            # 4. ==== BEGIN FACTOR COMPUTATION AREA ====
            # 计算过去20个交易日的平均换手率，使用滞后一期的股息率TTM以避免前视偏差
            window_size = 20
            min_periods = 15  # 调整为15以提高数据可用性，同时保持窗口代表性
            avg_turnover = df["db_turnover_rate"].groupby(level="instrument").rolling(window=window_size, min_periods=min_periods).mean()
            avg_turnover = avg_turnover.reset_index(level=0, drop=True)
            
            # 使用滞后一期的股息率TTM（db_dv_ttm_t-1）以确保仅基于历史信息
            dv_ttm_lagged = df["db_dv_ttm"].groupby(level="instrument").shift(1)
            
            # 计算股息率调整换手率因子
            series = avg_turnover * dv_ttm_lagged
            # ==== END FACTOR COMPUTATION AREA ====
    except FileNotFoundError:
        print("Warning: static_factors.parquet not found. Outputting NaN.")
        series = pd.Series(np.nan, index=df.index, dtype="float64")
    except Exception as e:
        print(f"Warning: Error reading static_factors.parquet: {e}. Outputting NaN.")
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