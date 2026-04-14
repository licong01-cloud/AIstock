"""
Batch 2 Factor Development Script — 9 factors
P0: 行业残差反转×换手 / 行业残差波动率 / 行业资金流残差
P1: 换手率偏度 / 换手率自相关 / 自由流通换手行业中性
P1.5: ATR压缩比 / 布林带宽收窄 / 日内波幅压缩
"""
import subprocess, json, sys, os, time

WSL_BASE = "/home/lc999/factor_workspace"
CONDA_ACTIVATE = "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu"

# ═══════════════════════════════════════════════════════════════
# Factor definitions
# ═══════════════════════════════════════════════════════════════

FACTORS = {}

# ── P0 #1: 行业残差反转×换手 ──
FACTORS["m_ind_residual_rev_turnover"] = {
    "desc": "行业残差5日反转×换手率排名：残差=(个股收益-行业收益)，取负5日累加后乘以换手率排名，高换手放大反转信号",
    "code": r'''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_ind_residual_rev_turnover"

def compute_factor():
    pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
    sector = pd.read_hdf(DATA_DIR / "sector_data.h5")
    db = pd.read_hdf(DATA_DIR / "daily_basic.h5")

    stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
    ind_ret = sector["sw2_pct_change"] / 100.0
    residual = stock_ret - ind_ret

    res_wide = residual.unstack("instrument")
    rev_5d = -res_wide.rolling(5, min_periods=4).sum()

    turn_wide = db["db_turnover_rate"].unstack("instrument")
    turn_ma20 = turn_wide.rolling(20, min_periods=15).mean()

    factor_wide = rev_5d.rank(axis=1, pct=True) * turn_ma20.rank(axis=1, pct=True)

    factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
    result = factor.to_frame(FACTOR_NAME)
    result.index.names = ["datetime", "instrument"]
    result = result.dropna()
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")

if __name__ == "__main__":
    compute_factor()
''',
}

# ── P0 #2: 行业残差波动率 ──
FACTORS["m_ind_residual_vol_ratio"] = {
    "desc": "个股残差20d波动率/行业20d波动率，取负（低残差波动=低特质风险溢价）",
    "code": r'''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_ind_residual_vol_ratio"

def compute_factor():
    pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
    sector = pd.read_hdf(DATA_DIR / "sector_data.h5")

    stock_ret = pv["close"].groupby(level="instrument").pct_change(1)
    ind_ret = sector["sw2_pct_change"] / 100.0
    residual = stock_ret - ind_ret

    res_wide = residual.unstack("instrument")
    ind_wide = ind_ret.unstack("instrument")

    res_vol = res_wide.rolling(20, min_periods=15).std()
    ind_vol = ind_wide.rolling(20, min_periods=15).std()

    factor_wide = -(res_vol / ind_vol.replace(0, np.nan))

    factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
    result = factor.to_frame(FACTOR_NAME)
    result.index.names = ["datetime", "instrument"]
    result = result.dropna()
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")

if __name__ == "__main__":
    compute_factor()
''',
}

# ── P0 #3: 行业资金流残差 ──
FACTORS["m_ind_flow_residual_mom"] = {
    "desc": "个股大单净流入比率-行业净流入比率的5日vs10日动量差，资金流残差变化方向",
    "code": r'''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_ind_flow_residual_mom"

def compute_factor():
    pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")
    mf = pd.read_hdf(DATA_DIR / "moneyflow.h5")
    sector = pd.read_hdf(DATA_DIR / "sector_data.h5")

    big_net = (mf["mf_lg_buy_amt"] - mf["mf_lg_sell_amt"] +
               mf["mf_elg_buy_amt"] - mf["mf_elg_sell_amt"])
    stock_flow = big_net / pv["amount"].replace(0, np.nan)

    ind_net = sector["sw2_mf_net_amt"]
    ind_amt = sector["sw2_amount"].replace(0, np.nan)
    ind_flow = ind_net / ind_amt

    flow_res = stock_flow - ind_flow

    fr_wide = flow_res.unstack("instrument")
    fr_ma5 = fr_wide.rolling(5, min_periods=4).mean()
    fr_ma10 = fr_wide.rolling(10, min_periods=8).mean()

    factor_wide = fr_ma5 - fr_ma10

    factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
    result = factor.to_frame(FACTOR_NAME)
    result.index.names = ["datetime", "instrument"]
    result = result.dropna()
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")

if __name__ == "__main__":
    compute_factor()
''',
}

# ── P1 #4: 换手率偏度 ──
FACTORS["m_turnover_skew_20d"] = {
    "desc": "20日滚动换手率偏度取负：负偏度=偶发高换手后回归，有预测力",
    "code": r'''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_turnover_skew_20d"

def compute_factor():
    db = pd.read_hdf(DATA_DIR / "daily_basic.h5")

    turn_wide = db["db_turnover_rate"].unstack("instrument")
    factor_wide = -turn_wide.rolling(20, min_periods=15).skew()

    factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
    result = factor.to_frame(FACTOR_NAME)
    result.index.names = ["datetime", "instrument"]
    result = result.dropna()
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")

if __name__ == "__main__":
    compute_factor()
''',
}

# ── P1 #5: 换手率自相关 ──
FACTORS["m_turnover_autocorr_5d"] = {
    "desc": "5日窗口换手率lag-1自相关取负：低自相关=换手率突变信号",
    "code": r'''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_turnover_autocorr_5d"

def compute_factor():
    db = pd.read_hdf(DATA_DIR / "daily_basic.h5")

    turn_wide = db["db_turnover_rate"].unstack("instrument")
    turn_lag = turn_wide.shift(1)

    xy = (turn_wide * turn_lag).rolling(5, min_periods=4).mean()
    x_m = turn_wide.rolling(5, min_periods=4).mean()
    y_m = turn_lag.rolling(5, min_periods=4).mean()
    x_s = turn_wide.rolling(5, min_periods=4).std(ddof=1)
    y_s = turn_lag.rolling(5, min_periods=4).std(ddof=1)

    factor_wide = -(xy - x_m * y_m) / (x_s * y_s).replace(0, np.nan)

    factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
    result = factor.to_frame(FACTOR_NAME)
    result.index.names = ["datetime", "instrument"]
    result = result.dropna()
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")

if __name__ == "__main__":
    compute_factor()
''',
}

# ── P1 #6: 自由流通换手×行业中性 ──
FACTORS["m_free_turnover_ind_neutral"] = {
    "desc": "自由流通换手率-行业换手率代理(sw2_amount/sw2_total_mv*100)，5日均取负，低相对换手=低流动性溢价",
    "code": r'''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_free_turnover_ind_neutral"

def compute_factor():
    db = pd.read_hdf(DATA_DIR / "daily_basic.h5")
    sector = pd.read_hdf(DATA_DIR / "sector_data.h5")

    free_turn = db["db_turnover_rate_f"]
    ind_turn = sector["sw2_amount"] / sector["sw2_total_mv"].replace(0, np.nan) * 100

    diff = free_turn - ind_turn
    diff_wide = diff.unstack("instrument")
    factor_wide = -diff_wide.rolling(5, min_periods=4).mean()

    factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
    result = factor.to_frame(FACTOR_NAME)
    result.index.names = ["datetime", "instrument"]
    result = result.dropna()
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")

if __name__ == "__main__":
    compute_factor()
''',
}

# ── P1.5 #7: ATR压缩比 ──
FACTORS["m_atr_compression"] = {
    "desc": "ATR(5d)/ATR(20d)取负：低比值=短期波动率压缩，预示突破方向",
    "code": r'''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_atr_compression"

def compute_factor():
    pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")

    c = pv["close"].unstack("instrument")
    h = pv["high"].unstack("instrument")
    lo = pv["low"].unstack("instrument")
    pc = c.shift(1)

    tr = pd.DataFrame(
        np.maximum(h - lo, np.maximum((h - pc).abs(), (lo - pc).abs())),
        index=c.index, columns=c.columns
    )

    atr5 = tr.rolling(5, min_periods=4).mean()
    atr20 = tr.rolling(20, min_periods=15).mean()

    factor_wide = -(atr5 / atr20.replace(0, np.nan))

    factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
    result = factor.to_frame(FACTOR_NAME)
    result.index.names = ["datetime", "instrument"]
    result = result.dropna()
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")

if __name__ == "__main__":
    compute_factor()
''',
}

# ── P1.5 #8: 布林带宽收窄 ──
FACTORS["m_bbwidth_shrink"] = {
    "desc": "短期BB宽度(5d)/长期BB宽度(20d)取负：低比=布林带收窄，波动率压缩信号",
    "code": r'''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_bbwidth_shrink"

def compute_factor():
    pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")

    c = pv["close"].unstack("instrument")

    ma5 = c.rolling(5, min_periods=4).mean()
    s5 = c.rolling(5, min_periods=4).std(ddof=1)
    bbw5 = (4 * s5) / ma5.replace(0, np.nan)

    ma20 = c.rolling(20, min_periods=15).mean()
    s20 = c.rolling(20, min_periods=15).std(ddof=1)
    bbw20 = (4 * s20) / ma20.replace(0, np.nan)

    factor_wide = -(bbw5 / bbw20.replace(0, np.nan))

    factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
    result = factor.to_frame(FACTOR_NAME)
    result.index.names = ["datetime", "instrument"]
    result = result.dropna()
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")

if __name__ == "__main__":
    compute_factor()
''',
}

# ── P1.5 #9: 日内波幅压缩 ──
FACTORS["m_intraday_range_compress"] = {
    "desc": "5日均日内波幅/20日均日内波幅取负：低比=日内波动压缩，突破前兆",
    "code": r'''import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "m_intraday_range_compress"

def compute_factor():
    pv = pd.read_hdf(DATA_DIR / "daily_pv.h5")

    h = pv["high"].unstack("instrument")
    lo = pv["low"].unstack("instrument")
    o = pv["open"].unstack("instrument")

    rng = (h - lo) / o.replace(0, np.nan)

    r5 = rng.rolling(5, min_periods=4).mean()
    r20 = rng.rolling(20, min_periods=15).mean()

    factor_wide = -(r5 / r20.replace(0, np.nan))

    factor = factor_wide.stack().reorder_levels(["datetime", "instrument"]).sort_index()
    result = factor.to_frame(FACTOR_NAME)
    result.index.names = ["datetime", "instrument"]
    result = result.dropna()
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")

if __name__ == "__main__":
    compute_factor()
''',
}

# ═══════════════════════════════════════════════════════════════
# Execution
# ═══════════════════════════════════════════════════════════════

def run_wsl(cmd, timeout=300):
    """Run command in WSL"""
    result = subprocess.run(
        ["wsl", "bash", "-c", cmd],
        capture_output=True, text=True, timeout=timeout
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()

def main():
    names = list(FACTORS.keys())
    print(f"=== Batch 2 Factor Development: {len(names)} factors ===\n")

    results = {}
    for i, name in enumerate(names):
        info = FACTORS[name]
        factor_dir = f"{WSL_BASE}/_factor_{name}"
        factor_py = f"{factor_dir}/factor.py"
        result_h5 = f"{factor_dir}/result.h5"

        print(f"[{i+1}/{len(names)}] {name}")
        print(f"  Creating directory...")

        # Create directory
        rc, out, err = run_wsl(f"mkdir -p {factor_dir}")
        if rc != 0:
            print(f"  FAIL: mkdir error: {err}")
            results[name] = "FAIL:mkdir"
            continue

        # Write factor.py via base64 to avoid heredoc escaping issues
        import base64
        code_b64 = base64.b64encode(info["code"].encode("utf-8")).decode("ascii")
        rc, out, err = run_wsl(f"echo '{code_b64}' | base64 -d > {factor_py}")
        if rc != 0:
            print(f"  FAIL: write error: {err}")
            results[name] = "FAIL:write"
            continue

        # Execute
        print(f"  Executing...")
        t0 = time.time()
        rc, out, err = run_wsl(
            f"{CONDA_ACTIVATE} && cd {factor_dir} && python factor.py",
            timeout=600
        )
        elapsed = time.time() - t0

        if rc != 0:
            print(f"  FAIL: execution error ({elapsed:.1f}s): {err[-500:]}")
            results[name] = f"FAIL:exec:{err[-200:]}"
            continue

        if err:
            for line in err.strip().splitlines()[-3:]:
                print(f"    [stderr] {line}")

        # Validate output
        validate_py = f"""
import pandas as pd
df = pd.read_hdf('{result_h5}')
print(f'Shape:{{df.shape}}')
print(f'Idx:{{df.index.names}}')
print(f'Col:{{list(df.columns)}}')
print(f'Date:{{df.index.get_level_values(0).min()}}~{{df.index.get_level_values(0).max()}}')
print(f'Stocks:{{df.index.get_level_values(1).nunique()}}')
print(f'NaN:{{df.isna().sum().values[0]}}')
"""
        rc, out, err = run_wsl(
            f"{CONDA_ACTIVATE} && python -c \"{validate_py}\"",
            timeout=30
        )

        if rc != 0:
            print(f"  FAIL: validation error: {err}")
            results[name] = f"FAIL:validate"
            continue

        print(f"  OK ({elapsed:.1f}s)")
        for line in out.strip().splitlines():
            print(f"    {line}")
        results[name] = f"OK"

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    ok_count = sum(1 for v in results.values() if v == "OK")
    fail_count = sum(1 for v in results.values() if v != "OK")
    for name, status in results.items():
        marker = "✓" if status == "OK" else "✗"
        print(f"  {marker} {name}: {status}")
    print(f"\nTotal: {ok_count} OK, {fail_count} FAIL")

    # Save factor info for registration
    with open(os.path.join(os.path.dirname(__file__), "batch2_factor_info.json"), "w") as f:
        json.dump({
            name: {"desc": info["desc"], "code": info["code"], "status": results[name]}
            for name, info in FACTORS.items()
        }, f, ensure_ascii=False, indent=2)
    print(f"\nFactor info saved to batch2_factor_info.json")

if __name__ == "__main__":
    main()
