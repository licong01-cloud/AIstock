"""
一次性迁移脚本: 稀疏基本面因子 PIT 改造 + 级联删除
日期: 2026-04-22
涉及:
- 删除 3 个 C 组因子 (语义无法保留, 通过 DELETE API 级联清理)
- 改造 11 个 A+B 组因子 (PIT + ffill, 替代 diff(N)/shift(N))
- 改造后统一 source='manual'

执行前要求:
- 后端服务可访问 http://127.0.0.1:8001 (用于 DELETE API 调用)
- 具有数据库读写权限
"""
import os
import sys
import json
import traceback
import psycopg2
from datetime import datetime
from pathlib import Path

# ══════════════════════════════════════════════════════════════════
# 配置
# ══════════════════════════════════════════════════════════════════
DB = dict(host='127.0.0.1', port=5432, dbname='aistock', user='postgres', password='lc78080808')
PROJECT_ROOT = Path("F:/Dev/AIstock")
QE_CODE_DIR = PROJECT_ROOT / "rdagent_assets" / "qe_factors"
CACHE_ROOTS = [
    PROJECT_ROOT / "rdagent_assets" / "factor_values",
]
API = "http://127.0.0.1:8001/api/v1"

# C 组 — 级联删除
DELETE_FACTORS = [
    ("EarningsGrowthMomentum", "rdagent_task_sync"),
    ("Earnings_Growth_Acceleration", "rdagent_task_sync"),
    ("rev_yoy_momentum_accel", "rdagent_task_sync"),
]

# A+B 组改造: 每个配置 = (factor_name, 当前source, 因子核心表达式 expr, 聚合方式 kind)
# kind: 'diff' (差分), 'pct' (百分比变化), 'combine_add', 'combine_mul'
# expr: 字段表达式字符串 (用 X, Y 占位); 或 dict 描述组合字段
REFACTOR_FACTORS = [
    # A 组 (9)
    dict(name="m_profit_yoy_change",              old_source="manual",            fields=["bb_profit_yoy"], kind="diff",       sign=1),
    dict(name="m_revenue_growth_accel",           old_source="manual",            fields=["bb_rev_yoy"],    kind="diff",       sign=1),
    dict(name="RevenueYOY_Momentum",              old_source="rdagent_task_sync", fields=["bb_rev_yoy"],    kind="diff",       sign=1),
    dict(name="ProfitMarginChange",               old_source="rdagent_task_sync", fields=["bb_gpr"],        kind="diff",       sign=1),
    dict(name="m_eps_momentum_20d",               old_source="manual",            fields=["bb_eps"],        kind="pct",        sign=1),
    dict(name="m_undp_change_20d",                old_source="manual",            fields=["bb_undp"],       kind="pct",        sign=1),
    dict(name="FixedAssetsProportion_Momentum",   old_source="rdagent_task_sync", fields=["bb_fixed_assets","bb_total_assets"], kind="ratio_diff", sign=1),
    dict(name="m_liquid_asset_ratio_change",      old_source="manual",            fields=["bb_liquid_assets","bb_total_assets"], kind="ratio_diff", sign=1),
    dict(name="m_gpr_npr_spread_change",          old_source="manual",            fields=["bb_gpr","bb_npr"], kind="spread_diff", sign=-1),
    # B 组 (2)
    dict(name="factor_profit_quality_change",     old_source="rdagent_task_sync", fields=["bb_gpr","bb_npr"], kind="combine_add", sign=1),
    dict(name="profit_quality_synergy",           old_source="rdagent_task_sync", fields=["bb_gpr","bb_npr"], kind="combine_mul", sign=1),
]

# ══════════════════════════════════════════════════════════════════
# 代码生成: 两种模板 (HDF5 回测 + Realtime 实盘)
# ══════════════════════════════════════════════════════════════════
def _pit_block_for_kind(kind: str, fields: list, sign: int) -> str:
    """生成 PIT 核心计算代码块 (用于嵌入模板)。"""
    if kind == "diff":
        # 单字段 PIT diff
        X = fields[0]
        return (
            f"    X = data_src['{X}']\n"
            f"    prev = X.groupby(level='instrument').shift(1)\n"
            f"    change_at_publish = (X - prev).where(X != prev)\n"
            f"    factor_series = {'-1 * ' if sign < 0 else ''}change_at_publish.groupby(level='instrument').ffill()\n"
        )
    if kind == "pct":
        X = fields[0]
        return (
            f"    X = data_src['{X}']\n"
            f"    prev = X.groupby(level='instrument').shift(1)\n"
            f"    pct = ((X - prev) / prev.replace(0, np.nan)).where(X != prev)\n"
            f"    factor_series = {'-1 * ' if sign < 0 else ''}pct.groupby(level='instrument').ffill()\n"
        )
    if kind == "ratio_diff":
        A, B = fields
        return (
            f"    ratio = data_src['{A}'] / data_src['{B}'].replace(0, np.nan)\n"
            f"    prev_ratio = ratio.groupby(level='instrument').shift(1)\n"
            f"    change_at_publish = (ratio - prev_ratio).where(ratio != prev_ratio)\n"
            f"    factor_series = {'-1 * ' if sign < 0 else ''}change_at_publish.groupby(level='instrument').ffill()\n"
        )
    if kind == "spread_diff":
        A, B = fields
        return (
            f"    spread = data_src['{A}'] - data_src['{B}']\n"
            f"    prev_spread = spread.groupby(level='instrument').shift(1)\n"
            f"    change_at_publish = (spread - prev_spread).where(spread != prev_spread)\n"
            f"    factor_series = {'-1 * ' if sign < 0 else ''}change_at_publish.groupby(level='instrument').ffill()\n"
        )
    if kind in ("combine_add", "combine_mul"):
        A, B = fields
        op = "+" if kind == "combine_add" else "*"
        return (
            f"    deltas = {{}}\n"
            f"    for _col in ['{A}', '{B}']:\n"
            f"        _x = data_src[_col]\n"
            f"        _prev = _x.groupby(level='instrument').shift(1)\n"
            f"        _chg = (_x - _prev).where(_x != _prev)\n"
            f"        deltas[_col] = _chg.groupby(level='instrument').ffill()\n"
            f"    factor_series = {'-1 * (' if sign < 0 else ''}deltas['{A}'] {op} deltas['{B}']{')' if sign < 0 else ''}\n"
        )
    raise ValueError(f"unknown kind: {kind}")


def build_code_text(cfg: dict) -> str:
    """生成 HDF5 回测形态 (code_text): 读 bak_basic.h5, 写 result.h5"""
    name = cfg["name"]
    pit_block = _pit_block_for_kind(cfg["kind"], cfg["fields"], cfg["sign"])
    # 在 HDF5 版里 data_src = bb (读 bak_basic.h5)
    pit_block_hdf5 = pit_block.replace("data_src", "bb")
    return f'''import pandas as pd
import numpy as np

FACTOR_NAME = "{name}"

# PIT 改造版 (2026-04-22): 用 PIT + ffill 替代错误的 diff(N)/shift(N)
# 原因: bb_* 是季度财报字段, diff(N) 天会产生 97% 零值
bb = pd.read_hdf("bak_basic.h5")

{pit_block_hdf5.replace('    ', '')}

factor = factor_series.rename(FACTOR_NAME)
result_df = factor.to_frame()
result_df.index.names = ["datetime", "instrument"]
result_df = result_df.dropna()
result_df = result_df.sort_index()
result_df.to_hdf("result.h5", key="data", mode="w")
'''


def build_realtime_code_text(cfg: dict) -> str:
    """生成实盘可运行形态 (realtime_code_text / .py 文件):
    封装为 calculate_{name}(instruments, start_date, end_date) 函数,
    通过 _STATIC_FACTORS_LOADER 读数据。
    """
    name = cfg["name"]
    fields = cfg["fields"]
    fields_literal = "[" + ", ".join(f"'{f}'" for f in fields) + "]"
    pit_block = _pit_block_for_kind(cfg["kind"], fields, cfg["sign"])
    # realtime 版: data_src = static_df
    pit_block_rt = pit_block.replace("data_src", "static_df")
    return f'''# ============================================================
# [AISTOCK FACTOR PIT REFACTOR 2026-04-22]
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
        # parquet
        p = cache_root / "single" / f"{factor_name}.parquet"
        if p.exists():
            p.unlink()
            report.append(f"  removed {p}")
        # _meta.json
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
        # merged cache
        merged = cache_root / "_merged_panel.parquet"
        merged_meta = cache_root / "_merged_panel.meta.json"
        for mf in (merged, merged_meta):
            if mf.exists():
                mf.unlink()
                report.append(f"  invalidated {mf}")


def delete_c_group():
    """删除 C 组 3 个因子 (调用 HTTP DELETE API)"""
    import urllib.parse
    import urllib.request

    for name, source in DELETE_FACTORS:
        print(f"\n--- DELETE (C-group) {name} ---")
        qs = urllib.parse.urlencode({"factor_name": name, "source": source})
        req = urllib.request.Request(f"{API}/quantevolver/factors?{qs}", method="DELETE")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = resp.read().decode("utf-8")
                print(f"  HTTP {resp.status}: {body[:300]}")
        except urllib.error.HTTPError as e:
            print(f"  HTTP ERROR {e.code}: {e.read().decode('utf-8')[:300]}")


def refactor_factor(cfg: dict, conn) -> dict:
    """改造单个因子 (A+B 组), 原子事务"""
    name = cfg["name"]
    old_source = cfg["old_source"]
    new_source = "manual"
    report = [f"=== {name} ({old_source} -> {new_source}) ==="]

    new_code_text = build_code_text(cfg)
    new_rt_code = build_realtime_code_text(cfg)
    new_qe_code_path = f"rdagent_assets/qe_factors/{name}.py"
    with conn.cursor() as cur:
        # 1. 找 catalog id
        cur.execute(
            "SELECT id FROM aistock_factor_catalog WHERE factor_name=%s AND source=%s",
            (name, old_source),
        )
        row = cur.fetchone()
        if not row:
            report.append("  NOT FOUND, skip")
            return {"ok": False, "report": report}
        catalog_id = row[0]

        # 2. 若 source 要变更 (rdagent -> manual), 先检查冲突
        if old_source != new_source:
            cur.execute(
                "SELECT 1 FROM aistock_factor_catalog WHERE factor_name=%s AND source=%s",
                (name, new_source),
            )
            if cur.fetchone():
                report.append(f"  CONFLICT: {name} already exists under manual, abort")
                return {"ok": False, "report": report}

        # 3. UPDATE catalog (核心字段)
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

        # 4. 若 source 变更, 同步更新关联表的 factor_source
        if old_source != new_source:
            for tbl in ("qe_factor_classification", "qe_factor_experiment_metrics",
                        "qe_factor_transformation_jobs", "qe_loop_factor_records"):
                cur.execute(
                    f"UPDATE {tbl} SET factor_source=%s WHERE factor_name=%s AND factor_source=%s",
                    (new_source, name, old_source),
                )
                report.append(f"  {tbl}: source updated rows={cur.rowcount}")

        # 5. 清除独立指标表的因子记录 (保留 classification, 因子分类依然有效)
        for tbl in ("aistock_factor_metrics", "aistock_factor_monthly_ic"):
            cur.execute(f"DELETE FROM {tbl} WHERE factor_name=%s", (name,))
            report.append(f"  {tbl}: DELETE rows={cur.rowcount}")

        # 6. 清除正式评级 (CASCADE 只在删除 catalog 时触发, 这里是 update 所以手动)
        cur.execute(
            "DELETE FROM qe_factor_official_ratings WHERE factor_catalog_id=%s",
            (catalog_id,),
        )
        report.append(f"  qe_factor_official_ratings: DELETE rows={cur.rowcount}")

        # 7. 清除相关性记录 (该因子参与的所有 pair)
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

        # 8. 计算日志清除 (避免沿用旧指标的日志)
        cur.execute("DELETE FROM aistock_factor_calc_log WHERE factor_name=%s", (name,))
        report.append(f"  aistock_factor_calc_log: DELETE rows={cur.rowcount}")

    # 9. 文件系统 — .py 写入 (在 DB 事务之后, 避免 DB 失败留下脏文件)
    py_path = QE_CODE_DIR / f"{name}.py"
    py_path.write_text(new_rt_code, encoding="utf-8")
    report.append(f"  wrote {py_path} ({len(new_rt_code)} bytes)")

    # 10. 双缓存清理
    _cleanup_cache_for_factor(name, report)

    return {"ok": True, "report": report}


def main(dry_run: bool = False):
    print(f"=== sparse fundamental factor migration ({datetime.now().isoformat()}) ===")
    print(f"DRY_RUN={dry_run}")
    print(f"C-group DELETE count: {len(DELETE_FACTORS)}")
    print(f"A+B-group REFACTOR count: {len(REFACTOR_FACTORS)}")

    if dry_run:
        # 打印一个样板
        print("\n--- sample code generation (m_profit_yoy_change) ---")
        sample = REFACTOR_FACTORS[0]
        print("== code_text (HDF5 回测版) ==")
        print(build_code_text(sample))
        print("== realtime_code_text (实盘函数版) ==")
        print(build_realtime_code_text(sample))
        return

    # 1) C 组删除 (先删, 因为删除不需要事务, HTTP 完成即生效)
    delete_c_group()

    # 2) A+B 组改造 (逐个原子事务, 任何一个失败回滚自己, 其他不受影响)
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
                traceback.print_exc()
    finally:
        conn.close()

    # 打印汇总
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
