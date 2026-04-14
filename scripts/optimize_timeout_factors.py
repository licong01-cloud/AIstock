"""
优化6个超时因子：将 groupby.rolling.corr() 替换为 unstack + 向量化滚动计算
"""
import asyncio
import asyncpg
import datetime
import json
import sys

DB_DSN = "postgresql://postgres:lc78080808@127.0.0.1:5432/aistock"
API_BASE = "http://127.0.0.1:8001/api/v1"

# ── 向量化滚动相关的通用辅助函数 ──────────────────────────────
VECTORIZED_CORR_HELPER = '''
def _fast_rolling_corr(x_series, y_series, window, min_periods):
    """向量化滚动相关: unstack -> rolling -> stack, 比 groupby.rolling.corr 快 50-100x"""
    x_wide = x_series.unstack("instrument")
    y_wide = y_series.unstack("instrument")
    xy = x_wide * y_wide
    mean_x = x_wide.rolling(window, min_periods=min_periods).mean()
    mean_y = y_wide.rolling(window, min_periods=min_periods).mean()
    mean_xy = xy.rolling(window, min_periods=min_periods).mean()
    std_x = x_wide.rolling(window, min_periods=min_periods).std(ddof=1)
    std_y = y_wide.rolling(window, min_periods=min_periods).std(ddof=1)
    cov = mean_xy - mean_x * mean_y
    denom = std_x * std_y
    corr_wide = cov / denom.replace(0, np.nan)
    return corr_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
'''

# ── 6个优化后的因子代码 ──────────────────────────────────────

FACTORS = {}

FACTORS["m_beta_60d"] = f'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_beta_60d"

{VECTORIZED_CORR_HELPER}

pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")

stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0

# 向量化: unstack计算rolling std
stock_wide = stock_ret.unstack("instrument")
mkt_wide = mkt_ret.unstack("instrument")

stock_std_w = stock_wide.rolling(60, min_periods=40).std(ddof=1)
mkt_std_w = mkt_wide.rolling(60, min_periods=40).std(ddof=1)

# 向量化相关系数
corr_wide = (stock_wide * mkt_wide).rolling(60, min_periods=40).mean() - \\
            stock_wide.rolling(60, min_periods=40).mean() * mkt_wide.rolling(60, min_periods=40).mean()
denom = stock_std_w * mkt_std_w
corr_wide = corr_wide / denom.replace(0, np.nan)

beta_wide = corr_wide * (stock_std_w / mkt_std_w.replace(0, np.nan))
beta_wide = beta_wide.replace([np.inf, -np.inf], np.nan)

factor = beta_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 低Beta因子
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
'''

FACTORS["m_r_squared_60d"] = f'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_r_squared_60d"

pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")

stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0

# 向量化 rolling corr
stock_wide = stock_ret.unstack("instrument")
mkt_wide = mkt_ret.unstack("instrument")

xy_mean = (stock_wide * mkt_wide).rolling(60, min_periods=40).mean()
x_mean = stock_wide.rolling(60, min_periods=40).mean()
y_mean = mkt_wide.rolling(60, min_periods=40).mean()
x_std = stock_wide.rolling(60, min_periods=40).std(ddof=1)
y_std = mkt_wide.rolling(60, min_periods=40).std(ddof=1)

cov = xy_mean - x_mean * y_mean
corr_wide = cov / (x_std * y_std).replace(0, np.nan)
r_squared_wide = corr_wide ** 2

factor = r_squared_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 低R2 = 高特质性
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
'''

FACTORS["m_beta_change_20d"] = f'''import pandas as pd
import numpy as np

FACTOR_NAME = "m_beta_change_20d"

pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")

stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0

# 向量化 rolling beta
stock_wide = stock_ret.unstack("instrument")
mkt_wide = mkt_ret.unstack("instrument")

xy_mean = (stock_wide * mkt_wide).rolling(60, min_periods=40).mean()
x_mean = stock_wide.rolling(60, min_periods=40).mean()
y_mean = mkt_wide.rolling(60, min_periods=40).mean()
x_std = stock_wide.rolling(60, min_periods=40).std(ddof=1)
y_std = mkt_wide.rolling(60, min_periods=40).std(ddof=1)

cov = xy_mean - x_mean * y_mean
corr_wide = cov / (x_std * y_std).replace(0, np.nan)
beta_wide = corr_wide * (x_std / y_std.replace(0, np.nan))
beta_wide = beta_wide.replace([np.inf, -np.inf], np.nan)

# 20日beta变化
beta_change_wide = beta_wide.diff(20)

factor = beta_change_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
'''

FACTORS["m_corr_decay_5_20"] = '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_corr_decay_5_20"

pv = pd.read_hdf("daily_pv.h5")
sector = pd.read_hdf("sector_data.h5")

stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
mkt_ret = sector["sw2_pct_change"] / 100.0

stock_wide = stock_ret.unstack("instrument")
mkt_wide = mkt_ret.unstack("instrument")

def _vec_corr(sw, mw, window, min_p):
    xy_m = (sw * mw).rolling(window, min_periods=min_p).mean()
    x_m = sw.rolling(window, min_periods=min_p).mean()
    y_m = mw.rolling(window, min_periods=min_p).mean()
    x_s = sw.rolling(window, min_periods=min_p).std(ddof=1)
    y_s = mw.rolling(window, min_periods=min_p).std(ddof=1)
    return (xy_m - x_m * y_m) / (x_s * y_s).replace(0, np.nan)

corr5_wide = _vec_corr(stock_wide, mkt_wide, 5, 4)
corr20_wide = _vec_corr(stock_wide, mkt_wide, 20, 15)

decay_wide = corr20_wide - corr5_wide

factor = decay_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
'''

FACTORS["m_corr_volume_return_20d"] = '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_corr_volume_return_20d"

pv = pd.read_hdf("daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)
vol = pv["volume"]

# 向量化 rolling corr(ret, volume)
ret_wide = ret.unstack("instrument")
vol_wide = vol.unstack("instrument")

xy_mean = (ret_wide * vol_wide).rolling(20, min_periods=15).mean()
x_mean = ret_wide.rolling(20, min_periods=15).mean()
y_mean = vol_wide.rolling(20, min_periods=15).mean()
x_std = ret_wide.rolling(20, min_periods=15).std(ddof=1)
y_std = vol_wide.rolling(20, min_periods=15).std(ddof=1)

cov = xy_mean - x_mean * y_mean
corr_wide = cov / (x_std * y_std).replace(0, np.nan)

factor = corr_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
'''

FACTORS["m_return_autocorr_5d"] = '''import pandas as pd
import numpy as np

FACTOR_NAME = "m_return_autocorr_5d"

pv = pd.read_hdf("daily_pv.h5")
ret = pv["close"].groupby(level="instrument").pct_change(1)

# 向量化: ret 和 ret_lag1 的 rolling corr
ret_wide = ret.unstack("instrument")
ret_lag1_wide = ret_wide.shift(1)

xy_mean = (ret_wide * ret_lag1_wide).rolling(20, min_periods=15).mean()
x_mean = ret_wide.rolling(20, min_periods=15).mean()
y_mean = ret_lag1_wide.rolling(20, min_periods=15).mean()
x_std = ret_wide.rolling(20, min_periods=15).std(ddof=1)
y_std = ret_lag1_wide.rolling(20, min_periods=15).std(ddof=1)

cov = xy_mean - x_mean * y_mean
corr_wide = cov / (x_std * y_std).replace(0, np.nan)

factor = corr_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
factor = -factor  # 负自相关因子
factor = factor.rename(FACTOR_NAME)

result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
'''


async def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "update"

    if mode == "update":
        # Step 1: Update code_text + re-enable in DB
        conn = await asyncpg.connect(DB_DSN)
        print("=" * 60)
        print("Step 1: Update factor code + re-enable")
        print("=" * 60)
        for name, code in FACTORS.items():
            await conn.execute("""
                UPDATE aistock_factor_catalog
                SET code_text = $1, is_available = true
                WHERE factor_name = $2
            """, code, name)
            print(f"  {name} ... updated + enabled")
        await conn.close()
        print(f"\nUpdated {len(FACTORS)} factors\n")

    # Step 2: Compute metrics via WSL directly (bypass API)
    print("=" * 60)
    print("Step 2: Compute metrics via WSL (individual, 600s timeout)")
    print("=" * 60)

    WSL_WORKSPACE = "/home/lc999/factor_workspace"
    COMPUTE_SCRIPT = "/mnt/f/Dev/AIstock/scripts/compute_factor_metrics_unified.py"
    CONDA_ENV = "rdagent-gpu"
    RDAGENT_ROOT = "/mnt/f/Dev/RD-Agent-main"

    # First: write factor code to WSL workspace
    print("  Writing factor code to WSL workspace...")
    conn = await asyncpg.connect(DB_DSN)
    rows = await conn.fetch("""
        SELECT factor_name, code_text FROM aistock_factor_catalog
        WHERE factor_name = ANY($1::text[])
    """, list(FACTORS.keys()))
    await conn.close()

    setup_cmds = []
    for row in rows:
        fname = row["factor_name"]
        code = row["code_text"]
        wsl_fdir = f"{WSL_WORKSPACE}/_factor_{fname}"
        setup_cmds.append(f"rm -rf {wsl_fdir} && mkdir -p {wsl_fdir}")
        setup_cmds.append(f"cat > {wsl_fdir}/factor.py << 'FACTOREOF'\n{code}\nFACTOREOF")

    setup_script = "\n".join(setup_cmds)
    proc = await asyncio.create_subprocess_exec(
        "wsl", "bash", "-c", setup_script,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    await asyncio.wait_for(proc.communicate(), timeout=30)
    print(f"  Written {len(rows)} factor files")

    # Then: compute metrics one by one
    ok, err = 0, 0
    conn = await asyncpg.connect(DB_DSN)

    # Pre-fetch catalog IDs
    catalog_rows = await conn.fetch("""
        SELECT id, factor_name FROM aistock_factor_catalog
        WHERE factor_name = ANY($1::text[])
    """, list(FACTORS.keys()))
    catalog_id_map = {r["factor_name"]: r["id"] for r in catalog_rows}

    for name in FACTORS:
        print(f"  {name}...", end=" ", flush=True)
        wsl_cmd = (
            f"source ~/miniconda3/etc/profile.d/conda.sh && "
            f"conda activate {CONDA_ENV} && "
            f"export PYTHONPATH={RDAGENT_ROOT}:$PYTHONPATH && "
            f"python {COMPUTE_SCRIPT} {WSL_WORKSPACE} {name}"
        )
        try:
            proc = await asyncio.create_subprocess_exec(
                "wsl", "bash", "-c", wsl_cmd,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
            stdout_text = stdout.decode("utf-8", errors="replace").strip()
            stderr_text = stderr.decode("utf-8", errors="replace").strip()

            if proc.returncode != 0:
                # Check stderr for useful info
                print(f"FAIL (exit {proc.returncode}): {stderr_text[-150:]}")
                err += 1
                continue

            # Parse JSON from stdout
            json_line = ""
            for line in reversed(stdout_text.split("\n")):
                line = line.strip()
                if line.startswith("{"):
                    json_line = line
                    break

            if not json_line:
                print(f"FAIL: no JSON in output")
                err += 1
                continue

            data = json.loads(json_line)

            # Parse: format is {"factors": {name: {"full": {...}}}, "execution_log": {name: {"status": "ok"}}}
            exec_log = data.get("execution_log", {}).get(name, {})
            factor_data = data.get("factors", {}).get(name, {})
            full_metrics = factor_data.get("full", {})

            if exec_log.get("status") == "ok" and full_metrics:
                ic = full_metrics.get("ic_mean", 0) or 0
                sharpe = full_metrics.get("top_sharpe", 0) or 0
                dur = exec_log.get("duration", "?")
                print(f"OK IC={ic:.4f} Sharpe={sharpe:.2f} ({dur}s)")

                # Delete old + insert new
                await conn.execute(
                    "DELETE FROM aistock_factor_metrics WHERE factor_name=$1 AND eval_window='full'",
                    name
                )
                cat_id = catalog_id_map.get(name)
                await conn.execute("""
                    INSERT INTO aistock_factor_metrics
                        (factor_name, eval_window, calculated_at, factor_catalog_id,
                         ic_mean, icir, rank_ic_mean, rank_icir,
                         ic_std, rank_ic_std, ic_positive_ratio,
                         top_annual_return, top_sharpe, top_max_drawdown,
                         top_excess_annual_return, top_excess_sharpe, benchmark_annual_return,
                         group_return_monotonicity, turnover, ic_decay_half_life,
                         coverage, n_trading_days, ic_csz_mean,
                         data_start, data_end)
                    VALUES ($1, 'full', now(), $23,
                            $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                            $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
                """,
                    name,
                    full_metrics.get("ic_mean"),
                    full_metrics.get("icir"),
                    full_metrics.get("rank_ic_mean"),
                    full_metrics.get("rank_icir"),
                    full_metrics.get("ic_std"),
                    full_metrics.get("rank_ic_std"),
                    full_metrics.get("ic_positive_ratio"),
                    full_metrics.get("top_annual_return"),
                    full_metrics.get("top_sharpe"),
                    full_metrics.get("top_max_drawdown"),
                    full_metrics.get("top_excess_annual_return"),
                    full_metrics.get("top_excess_sharpe"),
                    full_metrics.get("benchmark_annual_return"),
                    full_metrics.get("group_return_monotonicity"),
                    full_metrics.get("turnover"),
                    full_metrics.get("ic_decay_half_life"),
                    full_metrics.get("coverage"),
                    full_metrics.get("n_trading_days"),
                    full_metrics.get("ic_csz_mean"),
                    datetime.date.fromisoformat(full_metrics["data_start"]) if full_metrics.get("data_start") else None,
                    datetime.date.fromisoformat(full_metrics["data_end"]) if full_metrics.get("data_end") else None,
                    cat_id,
                )
                ok += 1
            else:
                err_msg = exec_log.get("error", data.get("error", "unknown"))
                print(f"FAIL: {str(err_msg)[:100]}")
                err += 1

        except asyncio.TimeoutError:
            print(f"TIMEOUT (600s)")
            err += 1
        except Exception as e:
            print(f"ERROR: {e}")
            err += 1

    await conn.close()
    print(f"\nDone: OK={ok}, ERR={err}")


if __name__ == "__main__":
    asyncio.run(main())
