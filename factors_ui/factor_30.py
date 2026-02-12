import pandas as pd
import numpy as np

def calculate_DividendYieldStability_20D():
    """根据给定因子定义计算因子值，并写入 result.h5"""

    # 1. 读取数据并按索引排序（索引应为 MultiIndex(datetime, instrument)）
    df = pd.read_hdf("daily_pv.h5", key="data").sort_index()

    # 2. 再次确保索引按 (datetime, instrument) 排序
    df = df.sort_index()

    # 3. 读取静态因子表并合并
    try:
        static_df = pd.read_parquet("static_factors.parquet").sort_index()
        df = df.join(static_df, how="left")
    except FileNotFoundError:
        raise ValueError("static_factors.parquet not found. Factor requires db_dv_ttm field.")
    except Exception as e:
        raise ValueError(f"Error loading static_factors.parquet: {e}")

    # 4. 检查所需列是否存在
    required_cols = ["db_dv_ttm"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Factor requires db_dv_ttm from static_factors.parquet.")

    # 5. ==== BEGIN FACTOR COMPUTATION AREA ====
    # 获取股息率TTM数据
    dv_ttm = df["db_dv_ttm"]
    
    # 计算过去20个交易日的滚动均值和标准差
    mean_20d = dv_ttm.groupby(level="instrument").rolling(window=20, min_periods=20).mean()
    std_20d = dv_ttm.groupby(level="instrument").rolling(window=20, min_periods=20).std(ddof=1)
    
    # 重置索引以与原始df.index对齐
    mean_20d = mean_20d.reset_index(level=0, drop=True)
    std_20d = std_20d.reset_index(level=0, drop=True)
    
    # 计算变异系数倒数：|mean| / std
    # 处理std为0的情况（避免除以0）
    with np.errstate(divide='ignore', invalid='ignore'):
        stability = np.abs(mean_20d) / std_20d
    
    # 将结果赋值给series
    series = stability
    # ==== END FACTOR COMPUTATION AREA ====

    # 6. 构造结果 DataFrame：索引必须与 df.index 完全一致
    result_df = pd.DataFrame(index=df.index)
    result_df["DividendYieldStability_20D"] = series.astype("float64")

    # 7. 索引名称必须直接继承 df.index.names，禁止手写 ["datetime", "instrument"]
    result_df.index.names = df.index.names

    # 8. 按索引排序并写入 result.h5
    result_df = result_df.sort_index()
    result_df.to_hdf("result.h5", key="data", mode="w")

    return result_df

if __name__ == "__main__":
    calculate_DividendYieldStability_20D()