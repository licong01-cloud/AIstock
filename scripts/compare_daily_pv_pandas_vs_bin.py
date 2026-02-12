import pandas as pd
import qlib
from qlib.data import D
from pathlib import Path

H5_PATH = Path("F:/Dev/AIstock/qlib_snapshots/qlib_export_20251209/daily_pv.h5")
BIN_PROVIDER = "F:/Dev/AIstock/qlib_bin/qlib_bin_20251209"
SYMBOL = "000001.SZ"
FIELDS = ["$close", "$volume", "$amount"]
FREQ = "day"
OUTPUT_MD = Path("F:/Dev/AIstock/docs/daily_pv_vs_bin_compare_pandas.md")


def load_snapshot_from_h5() -> pd.DataFrame:
    with pd.HDFStore(H5_PATH, mode="r") as store:
        df = store.get("/data")
    df = df.reset_index()
    df = df[df["instrument"] == SYMBOL].copy()
    df["date"] = pd.to_datetime(df["datetime"]).dt.normalize()
    return df


def load_bin_from_qlib() -> pd.DataFrame:
    qlib.init(
        provider_uri=BIN_PROVIDER,
        mount_path=BIN_PROVIDER,
        auto_mount=False,
        redis_port=-1,
    )
    df = D.features([SYMBOL], FIELDS, freq=FREQ)
    df.columns = [c.replace("$", "") for c in df.columns]
    df = df.reset_index()
    df["date"] = pd.to_datetime(df["datetime"]).dt.normalize()
    return df


def summarize_dates(df: pd.DataFrame) -> dict:
    dates = df["date"]
    return {
        "count": len(dates),
        "unique": dates.nunique(),
        "min": dates.min(),
        "max": dates.max(),
    }


def compare_values(snapshot_df: pd.DataFrame, bin_df: pd.DataFrame) -> pd.DataFrame:
    merged = snapshot_df.merge(
        bin_df,
        on="date",
        suffixes=("_snapshot", "_bin"),
        how="inner",
    )
    for field in ["close", "volume", "amount"]:
        merged[f"diff_{field}"] = merged[f"{field}_snapshot"] - merged[f"{field}_bin"]
    return merged


def main() -> None:
    print("读取 daily_pv.h5 (pandas HDFStore)...")
    snapshot_df = load_snapshot_from_h5()
    print("读取 bin provider (Qlib)...")
    bin_df = load_bin_from_qlib()

    snapshot_summary = summarize_dates(snapshot_df)
    bin_summary = summarize_dates(bin_df)

    snapshot_dates = set(snapshot_df["date"].dt.strftime("%Y-%m-%d"))
    bin_dates = set(bin_df["date"].dt.strftime("%Y-%m-%d"))

    snapshot_only = sorted(snapshot_dates - bin_dates)
    bin_only = sorted(bin_dates - snapshot_dates)

    merged = compare_values(snapshot_df, bin_df)

    diff_summary = {}
    for field in ["close", "volume", "amount"]:
        diff_col = f"diff_{field}"
        diff_summary[field] = {
            "max_abs": float(merged[diff_col].abs().max()) if not merged.empty else None,
            "mean_abs": float(merged[diff_col].abs().mean()) if not merged.empty else None,
            "nonzero_count": int((merged[diff_col] != 0).sum()) if not merged.empty else 0,
        }

    OUTPUT_MD.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_MD.open("w", encoding="utf-8") as f:
        f.write("# daily_pv.h5 与 bin 数据对比（pandas + Qlib）\n\n")
        f.write(f"- 股票: {SYMBOL}\n")
        f.write(f"- 字段: {', '.join(FIELDS)}\n")
        f.write(f"- 频率: {FREQ}\n\n")

        f.write("## 日期范围与记录数\n\n")
        f.write("### daily_pv.h5 (/data)\n")
        f.write(f"- 记录数: {snapshot_summary['count']}\n")
        f.write(f"- 唯一日期数: {snapshot_summary['unique']}\n")
        f.write(f"- 日期范围: {snapshot_summary['min']} 到 {snapshot_summary['max']}\n\n")

        f.write("### bin provider\n")
        f.write(f"- 记录数: {bin_summary['count']}\n")
        f.write(f"- 唯一日期数: {bin_summary['unique']}\n")
        f.write(f"- 日期范围: {bin_summary['min']} 到 {bin_summary['max']}\n\n")

        f.write("## 日期差异\n\n")
        f.write(f"- snapshot 独有日期数: {len(snapshot_only)}\n")
        f.write(f"- bin 独有日期数: {len(bin_only)}\n\n")

        if snapshot_only:
            f.write("### snapshot 独有日期示例\n")
            for d in snapshot_only[:20]:
                f.write(f"- {d}\n")
            f.write("\n")

        if bin_only:
            f.write("### bin 独有日期示例\n")
            for d in bin_only[:20]:
                f.write(f"- {d}\n")
            f.write("\n")

        f.write("## 数值差异（按日期交集）\n\n")
        for field, stats in diff_summary.items():
            f.write(f"### {field}\n")
            f.write(f"- 最大绝对差: {stats['max_abs']}\n")
            f.write(f"- 平均绝对差: {stats['mean_abs']}\n")
            f.write(f"- 非零差异数: {stats['nonzero_count']}\n\n")

    print(f"对比完成，报告已生成: {OUTPUT_MD}")


if __name__ == "__main__":
    main()
