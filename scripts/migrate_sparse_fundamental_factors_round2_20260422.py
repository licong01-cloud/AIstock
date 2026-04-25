"""
Round2 迁移脚本: 补处理 A 组漏网的 2 个 shift(20)/diff(20) 反模式因子
日期: 2026-04-22

涉及因子:
- GrossProfitMargin_Momentum    (shift(20) on bb_gpr)          → kind='diff'
- bb_gpr_chg_mul_value_ps_inv   (diff(20) on bb_gpr × ps_inv)  → kind='diff_mul_daily'

处理策略:
- 复用 round1 的改造路径 (code_text / realtime_code_text / .py / DB source 统一 manual / 清理缓存和指标)
- 新增 `diff_mul_daily` 模板: 季度字段做 PIT+ffill 得到 daily_delta, 再乘以日频字段 (db_ps_ttm)

执行前要求:
- 后端服务可访问 http://127.0.0.1:8001
- 具有数据库读写权限
"""
import os
import sys
import json
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

# ══════════════════════════════════════════════════════════════════
# 配置
# ══════════════════════════════════════════════════════════════════
DB = dict(host='127.0.0.1', port=5432, dbname='aistock', user='postgres', password='lc78080808')
PROJECT_ROOT = Path("F:/Dev/AIstock")
QE_CODE_DIR = PROJECT_ROOT / "rdagent_assets" / "qe_factors"
CACHE_ROOTS = [
    PROJECT_ROOT / "rdagent_assets" / "factor_values",
    PROJECT_ROOT / "rdagent_assets" / "factor_values_realtime",
]

REFACTOR_FACTORS = [
    dict(
        name="GrossProfitMargin_Momentum",
        old_source="rdagent_task_sync",
        fields=["bb_gpr"],
        kind="diff",
        sign=1,
    ),
    dict(
        name="bb_gpr_chg_mul_value_ps_inv",
        old_source="rdagent_task_sync",
        fields=["bb_gpr", "db_ps_ttm"],
        kind="diff_mul_daily",
        sign=1,
    ),
]


# ══════════════════════════════════════════════════════════════════
# 代码生成 (PIT 模板)
# ══════════════════════════════════════════════════════════════════
def _pit_block_for_kind(kind: str, fields: list, sign: int) -> str:
    """生成 PIT 核心计算代码块。此 round2 只引入 diff 和 diff_mul_daily。"""
    if kind == "diff":
        X = fields[0]
        return (
            f"    X = data_src['{X}']\n"
            f"    prev = X.groupby(level='instrument').shift(1)\n"
            f"    change_at_publish = (X - prev).where(X != prev)\n"
            f"    factor_series = {'-1 * ' if sign < 0 else ''}change_at_publish.groupby(level='instrument').ffill()\n"
        )
    if kind == "diff_mul_daily":
        # 季度字段 A 做 PIT+ffill, 乘以日频字段 B 的 inverse
        A, B = fields
        return (
            f"    X = data_src['{A}']\n"
            f"    prev = X.groupby(level='instrument').shift(1)\n"
            f"    chg = (X - prev).where(X != prev)\n"
            f"    delta_{A} = chg.groupby(level='instrument').ffill()\n"
            f"    daily_inv = (1 / data_src['{B}']).replace([np.inf, -np.inf], np.nan)\n"
            f"    factor_series = {'-1 * ' if sign < 0 else ''}(delta_{A} * daily_inv)\n"
        )
    raise ValueError(f"unknown kind: {kind}")


def build_code_text(cfg: dict) -> str:
    """HDF5 回测形态 (code_text): 读 bak_basic.h5, 写 result.h5"""
    name = cfg["name"]
    pit_block = _pit_block_for_kind(cfg["kind"], cfg["fields"], cfg["sign"])
    # 回测版: data_src -> bb, 去掉函数内 4 空格缩进
    pit_block_hdf5 = pit_block.replace("data_src", "bb").replace("    ", "")
    return f'''import pandas as pd
import numpy as np

FACTOR_NAME = "{name}"

# PIT 改造版 (round2 2026-04-22): 用 PIT + ffill 替代错误的 diff(N)/shift(N)
# 原因: bb_* 是季度财报字段, diff(N) 天会产生 97% 零值
bb = pd.read_hdf("bak_basic.h5")

{pit_block_hdf5}
factor = factor_series.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
'''


def build_realtime_code_text(cfg: dict) -> str:
    """实盘函数形态 (realtime_code_text / .py 文件)"""
    name = cfg["name"]
    fields = cfg["fields"]
    fields_literal = "[" + ", ".join(f"'{f}'" for f in fields) + "]"
    pit_block = _pit_block_for_kind(cfg["kind"], fields, cfg["sign"])
    pit_block_rt = pit_block.replace("data_src", "static_df")
    return f'''# ============================================================
# [AISTOCK FACTOR PIT REFACTOR round2 2026-04-22]
# Factor: {name}
# 说明: 替代原 diff(N)/shift(N) 反模式, 使用 PIT + ffill
# ============================================================
import pandas as pd
import numpy as np


def calculate_{name}(instruments: list, start_date: str, end_date: str) -> pd.DataFrame:
    FACTOR_NAME = "{name}"

    required_cols = {fields_literal}
    static_df = _STATIC_FACTORS_LOADER.load(
        instruments=instruments, start_date=start_date, end_date=end_date,
        columns=required_cols,
    )
    static_df = static_df.sort_index()

    for _c in required_cols:
        if _c not in static_df.columns:
            raise ValueError(f"Required column '{{_c}}' not found in static factors data.")

{pit_block_rt}
    factor = factor_series.rename(FACTOR_NAME)
    result_df = factor.to_frame()
    result_df = result_df.dropna()
    result_df = result_df.sort_index()
    return result_df
'''


# ══════════════════════════════════════════════════════════════════
# 主执行
# ══════════════════════════════════════════════════════════════════
def _cleanup_cache_for_factor(factor_name: str, report: list):
    """清理因子的双缓存 (parquet + _meta.json + merged cache 失效)"""
    for cache_root in CACHE_ROOTS:
        p = cache_root / "single" / f"{factor_name}.parquet"
        if p.exists():
            p.unlink()
            report.append(f"  removed {p}")
        meta_path = cache_root / "_meta.json"
        if meta_path.exists():
            try:
                m = json.loads(meta_path.read_text(encoding="utf-8"))
                if factor_name in m.get("factors", {}):
                    del m["factors"][factor_name]
                    m["factor_count"] = len(m["factors"])
                    tmp = meta_path.with_suffix(".json.tmp")
                    tmp.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")
                    os.replace(tmp, meta_path)
                    report.append(f"  cleaned {meta_path} entry")
            except Exception as e:
                report.append(f"  WARN {meta_path} cleanup failed: {e}")
        merged = cache_root / "_merged_panel.parquet"
        merged_meta = cache_root / "_merged_panel.meta.json"
        for mf in (merged, merged_meta):
            if mf.exists():
                mf.unlink()
                report.append(f"  invalidated {mf}")


def refactor_factor(cfg: dict, conn) -> dict:
    name = cfg["name"]
    old_source = cfg["old_source"]
    new_source = "manual"
    report = [f"=== {name} ({old_source} -> {new_source}) ==="]

    new_code_text = build_code_text(cfg)
    new_rt_code = build_realtime_code_text(cfg)
    new_qe_code_path = f"rdagent_assets/qe_factors/{name}.py"

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM aistock_factor_catalog WHERE factor_name=%s AND source=%s",
            (name, old_source),
        )
        row = cur.fetchone()
        if not row:
            report.append(f"  NOT FOUND under ({name}, {old_source}), skip")
            return {"ok": False, "report": report}
        catalog_id = row[0]

        if old_source != new_source:
            cur.execute(
                "SELECT 1 FROM aistock_factor_catalog WHERE factor_name=%s AND source=%s",
                (name, new_source),
            )
            if cur.fetchone():
                report.append(f"  CONFLICT: {name} already exists under manual, abort")
                return {"ok": False, "report": report}

        cur.execute(
            """
            UPDATE aistock_factor_catalog SET
                source = %s,
                code_text = %s,
                realtime_code_text = %s,
                qe_code_path = %s,
                transformation_status = 'SUCCESS',
                last_transformation_at = NOW(),
                ic = NULL, icir = NULL, sharpe = NULL,
                annualized_return = NULL, max_drawdown = NULL, information_ratio = NULL,
                best_performance_sharpe = NULL, best_performance_ann_ret = NULL,
                performance_metrics = NULL,
                is_sota_factor = FALSE,
                correlation_computed_at = NULL,
                correlation_pair_count = NULL
            WHERE id = %s
            """,
            (new_source, new_code_text, new_rt_code, new_qe_code_path, catalog_id),
        )
        report.append(f"  catalog UPDATE: rows={cur.rowcount}, id={catalog_id}")

        if old_source != new_source:
            for tbl in ("qe_factor_classification", "qe_factor_experiment_metrics",
                        "qe_factor_transformation_jobs", "qe_loop_factor_records"):
                cur.execute(
                    f"UPDATE {tbl} SET factor_source=%s WHERE factor_name=%s AND factor_source=%s",
                    (new_source, name, old_source),
                )
                report.append(f"  {tbl}: source updated rows={cur.rowcount}")

        for tbl in ("aistock_factor_metrics", "aistock_factor_monthly_ic"):
            cur.execute(f"DELETE FROM {tbl} WHERE factor_name=%s", (name,))
            report.append(f"  {tbl}: DELETE rows={cur.rowcount}")

        cur.execute(
            "DELETE FROM qe_factor_official_ratings WHERE factor_catalog_id=%s",
            (catalog_id,),
        )
        report.append(f"  qe_factor_official_ratings: DELETE rows={cur.rowcount}")

        cur.execute(
            "DELETE FROM qe_factor_correlations WHERE factor_a_id=%s OR factor_b_id=%s",
            (catalog_id, catalog_id),
        )
        report.append(f"  qe_factor_correlations: DELETE rows={cur.rowcount}")
        cur.execute(
            "DELETE FROM qe_factor_correlations_backup WHERE factor_a_catalog_id=%s OR factor_b_catalog_id=%s",
            (catalog_id, catalog_id),
        )
        report.append(f"  qe_factor_correlations_backup: DELETE rows={cur.rowcount}")

        cur.execute("DELETE FROM aistock_factor_calc_log WHERE factor_name=%s", (name,))
        report.append(f"  aistock_factor_calc_log: DELETE rows={cur.rowcount}")

    py_path = QE_CODE_DIR / f"{name}.py"
    py_path.write_text(new_rt_code, encoding="utf-8")
    report.append(f"  wrote {py_path} ({len(new_rt_code)} bytes)")

    _cleanup_cache_for_factor(name, report)

    return {"ok": True, "report": report}


def main(dry_run: bool = False):
    print(f"=== round2 sparse fundamental factor migration ({datetime.now().isoformat()}) ===")
    print(f"DRY_RUN={dry_run}")
    print(f"REFACTOR count: {len(REFACTOR_FACTORS)}")

    if dry_run:
        for cfg in REFACTOR_FACTORS:
            print(f"\n--- sample code for {cfg['name']} (kind={cfg['kind']}) ---")
            print("== code_text (HDF5 回测版) ==")
            print(build_code_text(cfg))
            print("== realtime_code_text (实盘函数版) ==")
            print(build_realtime_code_text(cfg))
        return

    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    all_reports = []
    try:
        for cfg in REFACTOR_FACTORS:
            try:
                res = refactor_factor(cfg, conn)
                if res["ok"]:
                    conn.commit()
                    all_reports.append(("OK", cfg["name"], res["report"]))
                else:
                    conn.rollback()
                    all_reports.append(("SKIP", cfg["name"], res["report"]))
            except Exception as e:
                conn.rollback()
                all_reports.append(("FAIL", cfg["name"], [f"  EXCEPTION: {e}"]))
                import traceback; traceback.print_exc()
    finally:
        conn.close()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for status, name, report in all_reports:
        print(f"\n[{status}] {name}")
        for line in report:
            print(line)
    ok_count = sum(1 for s, _, _ in all_reports if s == "OK")
    print(f"\n{ok_count}/{len(all_reports)} refactored successfully")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    main(dry_run=dry_run)
