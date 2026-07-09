"""
AIstock 因子库全面分析报告
用法:
  python analyze_factor_library.py          # Markdown 报告
  python analyze_factor_library.py --json   # JSON 输出
  python analyze_factor_library.py --source manual  # 仅分析指定来源
"""
import asyncio
import asyncpg
import json
import re
import sys
import statistics
from collections import Counter, defaultdict
from datetime import datetime

DB_DSN = "postgresql://postgres:lc78080808@127.0.0.1:5432/aistock"

# ── 完整字段清单 ──────────────────────────────────────────────
DATASET_FIELDS = {
    "daily_pv.h5": ["open", "close", "high", "low", "volume", "factor", "amount"],
    "daily_basic.h5": [
        "db_close", "db_turnover_rate", "db_turnover_rate_f", "db_volume_ratio",
        "db_pe", "db_pe_ttm", "db_pb", "db_ps", "db_ps_ttm",
        "db_dv_ratio", "db_dv_ttm", "db_total_share", "db_float_share",
        "db_free_share", "db_total_mv", "db_circ_mv",
    ],
    "moneyflow.h5": [
        "mf_sm_buy_vol", "mf_sm_buy_amt", "mf_sm_sell_vol", "mf_sm_sell_amt",
        "mf_md_buy_vol", "mf_md_buy_amt", "mf_md_sell_vol", "mf_md_sell_amt",
        "mf_lg_buy_vol", "mf_lg_buy_amt", "mf_lg_sell_vol", "mf_lg_sell_amt",
        "mf_elg_buy_vol", "mf_elg_buy_amt", "mf_elg_sell_vol", "mf_elg_sell_amt",
        "mf_net_vol", "mf_net_amt",
    ],
    "bak_basic.h5": [
        "bb_pe_dyn", "bb_total_assets", "bb_liquid_assets", "bb_fixed_assets",
        "bb_reserved", "bb_reserved_pershare", "bb_eps", "bb_bvps",
        "bb_undp", "bb_per_undp", "bb_rev_yoy", "bb_profit_yoy",
        "bb_gpr", "bb_npr", "bb_holder_num",
    ],
    "cyq_perf.h5": [
        "cp_his_low", "cp_his_high", "cp_cost_5pct", "cp_cost_15pct",
        "cp_cost_50pct", "cp_cost_85pct", "cp_cost_95pct",
        "cp_weight_avg", "cp_winner_rate",
    ],
    "sector_data.h5": [
        "sw2_open", "sw2_high", "sw2_low", "sw2_close", "sw2_pct_change",
        "sw2_vol", "sw2_amount", "sw2_pe", "sw2_pb", "sw2_total_mv",
        "sw2_mf_buy_sm_amt", "sw2_mf_sell_sm_amt",
        "sw2_mf_buy_md_amt", "sw2_mf_sell_md_amt",
        "sw2_mf_buy_lg_amt", "sw2_mf_sell_lg_amt",
        "sw2_mf_buy_elg_amt", "sw2_mf_sell_elg_amt",
        "sw2_mf_net_amt", "sw2_mf_buy_elg_vol", "sw2_mf_sell_elg_vol",
        "sw2_mf_net_vol", "l2_code_id",
    ],
    "static_factors.parquet": ["static_factors"],
}

CATEGORY_NAMES = {
    "MOM": "动量", "VOL": "波动率", "LIQ": "流动性", "VAL": "价值",
    "QUAL": "质量", "CORR": "相关性", "TECH": "技术", "SIZE": "规模",
    "STAT": "统计", "MF": "资金流", "CHIP": "筹码", "ML": "机器学习",
}


def scan_code_fields(code_text: str) -> dict:
    """扫描因子代码中引用的 h5 文件和字段"""
    if not code_text:
        return {}
    used = defaultdict(set)
    for ds_name, fields in DATASET_FIELDS.items():
        # 检查是否引用了这个数据文件
        if ds_name not in code_text and ds_name.replace(".h5", "") not in code_text and ds_name.replace(".parquet", "") not in code_text:
            # Also check for generic read_hdf patterns
            if "daily_pv" in ds_name and ("daily_pv" not in code_text and "pv" not in code_text):
                continue
            elif "daily_basic" in ds_name and "daily_basic" not in code_text:
                continue
            elif "moneyflow" in ds_name and "moneyflow" not in code_text:
                continue
            elif "bak_basic" in ds_name and "bak_basic" not in code_text:
                continue
            elif "cyq_perf" in ds_name and "cyq_perf" not in code_text:
                continue
            elif "sector_data" in ds_name and "sector_data" not in code_text:
                continue
            elif "static_factors" in ds_name and "static_factors" not in code_text:
                continue
        # 扫描用到的字段
        for field in fields:
            patterns = [
                rf'\["{re.escape(field)}"\]',
                rf"\['{re.escape(field)}'\]",
                rf'"{re.escape(field)}"',
                rf"'{re.escape(field)}'",
            ]
            for pat in patterns:
                if re.search(pat, code_text):
                    used[ds_name].add(field)
                    break
    return dict(used)


def percentile(data, p):
    if not data:
        return 0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * p / 100
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


async def main():
    args = sys.argv[1:]
    json_mode = "--json" in args
    source_filter = None
    for i, a in enumerate(args):
        if a == "--source" and i + 1 < len(args):
            source_filter = args[i + 1]

    conn = await asyncpg.connect(DB_DSN)

    # ── 1. 因子库总览 ──────────────────────────────────────
    where_clause = f"AND c.source = '{source_filter}'" if source_filter else ""

    catalog = await conn.fetch(f"""
        SELECT c.id, c.factor_name, c.source, c.is_available, c.code_text, c.description_cn
        FROM aistock_factor_catalog c
        WHERE 1=1 {where_clause}
        ORDER BY c.factor_name
    """)

    total = len(catalog)
    available = sum(1 for r in catalog if r["is_available"])
    disabled = total - available

    metrics_count = await conn.fetch(f"""
        SELECT m.factor_name, count(DISTINCT m.eval_window) as wc
        FROM aistock_factor_metrics m
        JOIN aistock_factor_catalog c ON c.factor_name = m.factor_name
        WHERE 1=1 {where_clause}
        GROUP BY m.factor_name
    """)
    has_full_metrics = sum(1 for r in metrics_count if r["wc"] >= 4)
    has_any_metrics = len(metrics_count)

    classified = await conn.fetch(f"""
        SELECT count(DISTINCT cl.factor_name) as cnt
        FROM qe_factor_classification cl
        JOIN aistock_factor_catalog c ON c.factor_name = cl.factor_name
        WHERE 1=1 {where_clause}
    """)
    classified_count = classified[0]["cnt"] if classified else 0

    source_dist = Counter(r["source"] for r in catalog)

    # ── 2. 分类分布 ──────────────────────────────────────
    classifications = await conn.fetch(f"""
        SELECT cl.factor_name, cl.category, cl.grade
        FROM qe_factor_classification cl
        JOIN aistock_factor_catalog c ON c.factor_name = cl.factor_name
        WHERE 1=1 {where_clause}
    """)
    cat_dist = Counter(r["category"] for r in classifications if r["category"])

    # ── 3. IC 分布统计 ──────────────────────────────────────
    metrics_data = await conn.fetch(f"""
        SELECT m.factor_name, m.eval_window, m.ic_mean, m.icir, m.rank_ic_mean,
               m.top_sharpe, m.top_annual_return, m.top_max_drawdown, m.turnover
        FROM aistock_factor_metrics m
        JOIN aistock_factor_catalog c ON c.factor_name = m.factor_name
        WHERE c.is_available = true {where_clause}
    """)

    ic_by_window = defaultdict(list)
    for r in metrics_data:
        if r["ic_mean"] is not None:
            ic_by_window[r["eval_window"]].append({
                "factor_name": r["factor_name"],
                "ic_mean": float(r["ic_mean"]),
                "icir": float(r["icir"]) if r["icir"] else 0,
                "rank_ic": float(r["rank_ic_mean"]) if r["rank_ic_mean"] else 0,
                "top_sharpe": float(r["top_sharpe"]) if r["top_sharpe"] else 0,
                "top_ann_ret": float(r["top_annual_return"]) if r["top_annual_return"] else 0,
                "turnover": float(r["turnover"]) if r["turnover"] else 0,
            })

    # ── 4. 数据集覆盖分析 ──────────────────────────────────────
    all_used_fields = defaultdict(set)
    for r in catalog:
        code = r["code_text"] or ""
        fields = scan_code_fields(code)
        for ds, flds in fields.items():
            all_used_fields[ds].update(flds)

    # ── 5. Top/Bottom 因子 (out_sample) ──────────────────────────
    os_factors = ic_by_window.get("out_sample", [])
    os_sorted = sorted(os_factors, key=lambda x: abs(x["ic_mean"]), reverse=True)

    # ── 6. 分类 + IC 交叉分析 ──────────────────────────────────
    cat_map = {r["factor_name"]: r["category"] for r in classifications}
    cat_ic = defaultdict(list)
    for f in os_factors:
        cat = cat_map.get(f["factor_name"], "?")
        cat_ic[cat].append(abs(f["ic_mean"]))

    # ── 7. 相关性分析 ──────────────────────────────────────
    corr_total = await conn.fetchval("SELECT count(*) FROM qe_factor_correlations")
    corr_data = {}  # 只在有数据时填充
    if corr_total > 0:
        # 相关性分布
        corr_dist = await conn.fetch("""
            SELECT
                CASE
                    WHEN abs(correlation) > 0.9 THEN '>0.9'
                    WHEN abs(correlation) > 0.8 THEN '0.8-0.9'
                    WHEN abs(correlation) > 0.7 THEN '0.7-0.8'
                    WHEN abs(correlation) > 0.5 THEN '0.5-0.7'
                    WHEN abs(correlation) > 0.3 THEN '0.3-0.5'
                    ELSE '<0.3'
                END AS band,
                count(*) AS cnt
            FROM qe_factor_correlations
            GROUP BY 1 ORDER BY 1 DESC
        """)
        corr_data["distribution"] = {r["band"]: r["cnt"] for r in corr_dist}

        # 高相关因子簇 (|corr| > 0.8, 非 neg_ 对)
        high_corr_pairs = await conn.fetch("""
            SELECT ca.factor_name AS fa, cb.factor_name AS fb, fc.correlation,
                   ma.ic_mean AS ic_a, ma.icir AS icir_a,
                   mb.ic_mean AS ic_b, mb.icir AS icir_b
            FROM qe_factor_correlations fc
            JOIN aistock_factor_catalog ca ON ca.id = fc.factor_a_id AND ca.is_available = true
            JOIN aistock_factor_catalog cb ON cb.id = fc.factor_b_id AND cb.is_available = true
            LEFT JOIN LATERAL (
                SELECT ic_mean, icir FROM aistock_factor_metrics
                WHERE factor_name = ca.factor_name AND eval_window = 'out_sample' LIMIT 1
            ) ma ON true
            LEFT JOIN LATERAL (
                SELECT ic_mean, icir FROM aistock_factor_metrics
                WHERE factor_name = cb.factor_name AND eval_window = 'out_sample' LIMIT 1
            ) mb ON true
            WHERE abs(fc.correlation) > 0.8
            ORDER BY abs(fc.correlation) DESC
            LIMIT 50
        """)
        corr_data["high_pairs"] = high_corr_pairs
        corr_data["high_pair_counts"] = {
            ">0.9": await conn.fetchval("""
                SELECT count(*) FROM qe_factor_correlations fc
                JOIN aistock_factor_catalog ca ON ca.id = fc.factor_a_id AND ca.is_available = true
                JOIN aistock_factor_catalog cb ON cb.id = fc.factor_b_id AND cb.is_available = true
                WHERE abs(fc.correlation) > 0.9
            """),
            ">0.8": await conn.fetchval("""
                SELECT count(*) FROM qe_factor_correlations fc
                JOIN aistock_factor_catalog ca ON ca.id = fc.factor_a_id AND ca.is_available = true
                JOIN aistock_factor_catalog cb ON cb.id = fc.factor_b_id AND cb.is_available = true
                WHERE abs(fc.correlation) > 0.8
            """),
        }

        # neg_ 冗余对
        neg_redundant = await conn.fetch("""
            SELECT ca.factor_name AS neg_name, cb.factor_name AS orig_name, fc.correlation
            FROM qe_factor_correlations fc
            JOIN aistock_factor_catalog ca ON ca.id = fc.factor_a_id AND ca.is_available = true
            JOIN aistock_factor_catalog cb ON cb.id = fc.factor_b_id AND cb.is_available = true
            WHERE (ca.factor_name LIKE 'neg_%%' AND cb.factor_name = substring(ca.factor_name from 5))
               OR (cb.factor_name LIKE 'neg_%%' AND ca.factor_name = substring(cb.factor_name from 5))
            ORDER BY abs(fc.correlation) DESC
        """)
        corr_data["neg_redundant"] = neg_redundant

        # 因子替换建议: 高相关对中 IC 提升 >= 30%
        replace_candidates = []
        for r in high_corr_pairs:
            ic_a = abs(float(r["ic_a"])) if r["ic_a"] else 0
            ic_b = abs(float(r["ic_b"])) if r["ic_b"] else 0
            icir_a = abs(float(r["icir_a"])) if r["icir_a"] else 0
            icir_b = abs(float(r["icir_b"])) if r["icir_b"] else 0
            if ic_a > 0.005 and ic_b > 0.005:
                if ic_a / ic_b >= 1.3 and icir_a > icir_b:
                    replace_candidates.append({
                        "keep": r["fa"], "drop": r["fb"],
                        "corr": float(r["correlation"]),
                        "ic_keep": ic_a, "ic_drop": ic_b,
                        "icir_keep": icir_a, "icir_drop": icir_b,
                        "ic_improve": (ic_a / ic_b - 1) * 100,
                    })
                elif ic_b / ic_a >= 1.3 and icir_b > icir_a:
                    replace_candidates.append({
                        "keep": r["fb"], "drop": r["fa"],
                        "corr": float(r["correlation"]),
                        "ic_keep": ic_b, "ic_drop": ic_a,
                        "icir_keep": icir_b, "icir_drop": icir_a,
                        "ic_improve": (ic_b / ic_a - 1) * 100,
                    })
        corr_data["replace_candidates"] = replace_candidates

    # ── 8. IC 衰变分析 ──────────────────────────────────────
    decay_data = await conn.fetch("""
        SELECT f.factor_name, f.ic_mean AS ic_full, r.ic_mean AS ic_3m,
               f.icir AS icir_full,
               CASE WHEN abs(f.ic_mean) > 0.001
                    THEN (abs(r.ic_mean) - abs(f.ic_mean)) / abs(f.ic_mean) * 100
                    ELSE 0 END AS decay_pct
        FROM aistock_factor_metrics f
        JOIN aistock_factor_metrics r ON r.factor_name = f.factor_name AND r.eval_window = 'recent_3m'
        JOIN aistock_factor_catalog c ON c.factor_name = f.factor_name AND c.is_available = true
        WHERE f.eval_window = 'full' AND abs(f.ic_mean) > 0.01
        ORDER BY decay_pct ASC
        LIMIT 20
    """)
    improve_data = await conn.fetch("""
        SELECT f.factor_name, f.ic_mean AS ic_full, r.ic_mean AS ic_3m,
               CASE WHEN abs(f.ic_mean) > 0.001
                    THEN (abs(r.ic_mean) - abs(f.ic_mean)) / abs(f.ic_mean) * 100
                    ELSE 0 END AS improve_pct
        FROM aistock_factor_metrics f
        JOIN aistock_factor_metrics r ON r.factor_name = f.factor_name AND r.eval_window = 'recent_3m'
        JOIN aistock_factor_catalog c ON c.factor_name = f.factor_name AND c.is_available = true
        WHERE f.eval_window = 'full' AND abs(f.ic_mean) > 0.01
        ORDER BY improve_pct DESC
        LIMIT 20
    """)

    await conn.close()

    # ══════════════════════════════════════════════════════════
    # 输出报告
    # ══════════════════════════════════════════════════════════

    if json_mode:
        report = {
            "generated_at": datetime.now().isoformat(),
            "source_filter": source_filter,
            "overview": {
                "total": total, "available": available, "disabled": disabled,
                "has_full_metrics": has_full_metrics, "has_any_metrics": has_any_metrics,
                "classified": classified_count,
                "source_distribution": dict(source_dist),
            },
            "category_distribution": {k: cat_dist.get(k, 0) for k in CATEGORY_NAMES},
            "ic_statistics": {},
            "dataset_coverage": {},
            "top_factors": [{"name": f["factor_name"], "ic": f["ic_mean"], "icir": f["icir"]}
                           for f in os_sorted[:20]],
            "bottom_factors": [{"name": f["factor_name"], "ic": f["ic_mean"], "icir": f["icir"]}
                              for f in os_sorted[-20:]],
            "category_ic": {k: {"mean": statistics.mean(v) if v else 0, "count": len(v)}
                           for k, v in cat_ic.items()},
        }
        for window, data in ic_by_window.items():
            ics = [d["ic_mean"] for d in data]
            abs_ics = [abs(x) for x in ics]
            report["ic_statistics"][window] = {
                "count": len(ics),
                "abs_ic_mean": statistics.mean(abs_ics) if ics else 0,
                "abs_ic_median": statistics.median(abs_ics) if ics else 0,
                "abs_ic_p25": percentile(abs_ics, 25),
                "abs_ic_p75": percentile(abs_ics, 75),
                "abs_ic_gt_002": sum(1 for x in abs_ics if x > 0.02),
                "abs_ic_gt_003": sum(1 for x in abs_ics if x > 0.03),
            }
        for ds, fields in DATASET_FIELDS.items():
            used = all_used_fields.get(ds, set())
            report["dataset_coverage"][ds] = {
                "total_fields": len(fields),
                "used_fields": len(used),
                "coverage_pct": round(len(used) / len(fields) * 100, 1) if fields else 0,
                "unused_fields": sorted(set(fields) - used),
            }
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return

    # ── Markdown 报告 ──────────────────────────────────────
    src_label = f" (来源: {source_filter})" if source_filter else ""
    print(f"{'=' * 60}")
    print(f"  AIstock 因子库分析报告{src_label}")
    print(f"  生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

    # §1
    print(f"\n§1 因子库总览")
    print(f"{'-' * 40}")
    print(f"  总因子数:          {total}")
    print(f"  可用(available):   {available}")
    print(f"  禁用(disabled):    {disabled}")
    print(f"  有指标(4窗口完整): {has_full_metrics}")
    print(f"  有指标(任意窗口):  {has_any_metrics}")
    print(f"  已LLM分类:         {classified_count}")
    print(f"  来源分布: {', '.join(f'{k}={v}' for k, v in sorted(source_dist.items()))}")

    missing_metrics = available - has_full_metrics
    missing_class = available - classified_count
    if missing_metrics > 0 or missing_class > 0:
        print(f"\n  !! 数据完整性:")
        if missing_metrics > 0:
            print(f"    - {missing_metrics} 个可用因子缺少完整指标(4窗口)")
        if missing_class > 0:
            print(f"    - {missing_class} 个可用因子缺少LLM分类")

    # §2
    print(f"\n§2 分类分布")
    print(f"{'-' * 40}")
    max_count = max(cat_dist.values()) if cat_dist else 1
    for code, name in CATEGORY_NAMES.items():
        cnt = cat_dist.get(code, 0)
        bar_len = max(1, int(cnt / max_count * 20)) if cnt > 0 else 0
        bar = "#" * bar_len
        warn = " !! 偏少" if cnt < 5 else ""
        print(f"  {code:5s} {name:6s} {cnt:3d}  {bar}{warn}")
    unclassified = sum(1 for r in classifications if not r["category"] or r["category"] not in CATEGORY_NAMES)
    if unclassified:
        print(f"  ???   未分类  {unclassified}")

    # §3
    print(f"\n§3 IC 分布统计")
    print(f"{'-' * 40}")
    for window in ["out_sample", "full", "recent_6m", "recent_3m"]:
        data = ic_by_window.get(window, [])
        if not data:
            print(f"  {window}: 无数据")
            continue
        ics = [d["ic_mean"] for d in data]
        abs_ics = [abs(x) for x in ics]
        gt002 = sum(1 for x in abs_ics if x > 0.02)
        gt003 = sum(1 for x in abs_ics if x > 0.03)
        n = len(ics)
        print(f"  [{window}] (n={n})")
        print(f"    |IC| 均值:  {statistics.mean(abs_ics):.4f}")
        print(f"    |IC| 中位:  {statistics.median(abs_ics):.4f}")
        print(f"    |IC| P25:   {percentile(abs_ics, 25):.4f}")
        print(f"    |IC| P75:   {percentile(abs_ics, 75):.4f}")
        print(f"    |IC|>0.02: {gt002} ({gt002*100//n}%)")
        print(f"    |IC|>0.03: {gt003} ({gt003*100//n}%)")

    # §4
    print(f"\n§4 数据集覆盖分析")
    print(f"{'-' * 40}")
    for ds_name, fields in DATASET_FIELDS.items():
        used = all_used_fields.get(ds_name, set())
        pct = len(used) / len(fields) * 100 if fields else 0
        status = "[OK]" if pct >= 80 else "[!!]" if pct >= 50 else "[XX]"
        print(f"  {ds_name:25s} {len(used):2d}/{len(fields):2d}  {pct:5.1f}% {status}")
        unused = sorted(set(fields) - used)
        if unused and pct < 100:
            if len(unused) <= 6:
                print(f"    未用: {', '.join(unused)}")
            else:
                print(f"    未用: {', '.join(unused[:5])}, ... (+{len(unused)-5})")

    # §5
    print(f"\n§5 Top/Bottom 因子 (out_sample IC)")
    print(f"{'-' * 40}")
    if os_sorted:
        print(f"  Top 20 (|IC| 最大):")
        for i, f in enumerate(os_sorted[:20], 1):
            cat = cat_map.get(f["factor_name"], "?")
            print(f"    {i:2d}. {f['factor_name']:40s} IC={f['ic_mean']:+.4f}  ICIR={f['icir']:.2f}  {cat}")
        print(f"\n  Bottom 10 (|IC| 最小):")
        for i, f in enumerate(reversed(os_sorted[-10:]), 1):
            cat = cat_map.get(f["factor_name"], "?")
            print(f"    {i:2d}. {f['factor_name']:40s} IC={f['ic_mean']:+.4f}  ICIR={f['icir']:.2f}  {cat}")
    else:
        print("  无 out_sample 数据")

    # §6 相关性分析
    print(f"\n§6 相关性分析")
    print(f"{'-' * 40}")
    if corr_total == 0:
        print("  ⚠ 相关性数据为空，请先执行相关性计算")
    else:
        print(f"  总记录数: {corr_total}")
        dist = corr_data.get("distribution", {})
        for band in [">0.9", "0.8-0.9", "0.7-0.8", "0.5-0.7", "0.3-0.5", "<0.3"]:
            cnt = dist.get(band, 0)
            print(f"    |corr| {band:8s}: {cnt:6d}")

        hpc = corr_data.get("high_pair_counts", {})
        print(f"\n  可用因子间高相关对 (|corr|>0.8): {hpc.get('>0.8', 0)}")
        print(f"  可用因子间极高相关对 (|corr|>0.9): {hpc.get('>0.9', 0)}")

        # 高相关因子对 Top 20
        high_pairs = corr_data.get("high_pairs", [])
        if high_pairs:
            print(f"\n  高相关因子对 Top 20:")
            for i, r in enumerate(high_pairs[:20], 1):
                ic_a = float(r["ic_a"]) if r["ic_a"] else 0
                ic_b = float(r["ic_b"]) if r["ic_b"] else 0
                print(f"    {i:2d}. {r['fa']:35s} <-> {r['fb']:35s}  corr={float(r['correlation']):+.3f}  IC={ic_a:+.4f}/{ic_b:+.4f}")

        # neg_ 冗余对
        neg_red = corr_data.get("neg_redundant", [])
        if neg_red:
            print(f"\n  neg_ 取反因子对: {len(neg_red)} 对")
            truly_neg = sum(1 for r in neg_red if float(r["correlation"]) < -0.9)
            weak_neg = sum(1 for r in neg_red if abs(float(r["correlation"])) < 0.5)
            print(f"    corr < -0.9 (正常): {truly_neg}")
            print(f"    |corr| < 0.5 (异常): {weak_neg}")

        # 因子替换建议
        replacements = corr_data.get("replace_candidates", [])
        if replacements:
            print(f"\n  因子替换建议 (|corr|>0.8 且 IC 提升>=30%):")
            for r in replacements[:10]:
                print(f"    ✓ {r['keep']:40s} IC={r['ic_keep']:.4f}  ICIR={r['icir_keep']:.2f}")
                print(f"      → 替换 {r['drop']:36s} IC={r['ic_drop']:.4f}  ICIR={r['icir_drop']:.2f}")
                print(f"        corr={r['corr']:+.3f}  IC提升={r['ic_improve']:.0f}%")

    # §7 IC 衰变分析
    print(f"\n§7 IC 衰变分析 (full → recent_3m)")
    print(f"{'-' * 40}")
    if decay_data:
        print(f"  衰变最严重 (Top 15):")
        seen = set()
        count = 0
        for d in decay_data:
            name = d["factor_name"]
            if name in seen:
                continue
            seen.add(name)
            count += 1
            if count > 15:
                break
            ic_f = float(d["ic_full"])
            ic_3 = float(d["ic_3m"])
            decay = float(d["decay_pct"])
            print(f"    {name:45s} IC_full={ic_f:+.4f}  IC_3m={ic_3:+.4f}  衰变={decay:+.0f}%")
    else:
        print("  无衰变数据")

    if improve_data:
        print(f"\n  近期增强 (Top 15):")
        seen = set()
        count = 0
        for d in improve_data:
            name = d["factor_name"]
            if name in seen:
                continue
            seen.add(name)
            count += 1
            if count > 15:
                break
            ic_f = float(d["ic_full"])
            ic_3 = float(d["ic_3m"])
            imp = float(d["improve_pct"])
            print(f"    {name:45s} IC_full={ic_f:+.4f}  IC_3m={ic_3:+.4f}  增强={imp:+.0f}%")

    # §8 补充建议
    print(f"\n§8 补充建议")
    print(f"{'-' * 40}")
    suggestions = []

    for ds_name, fields in DATASET_FIELDS.items():
        used = all_used_fields.get(ds_name, set())
        unused = sorted(set(fields) - used)
        pct = len(used) / len(fields) * 100 if fields else 0
        if unused and pct < 70:
            suggestions.append(
                f"[数据覆盖] {ds_name} 覆盖率仅 {pct:.0f}%，"
                f"建议基于未用字段开发新因子: {', '.join(unused[:5])}"
            )

    for code, name in CATEGORY_NAMES.items():
        cnt = cat_dist.get(code, 0)
        if cnt < 5:
            target = max(8, cnt + 5)
            suggestions.append(f"[类别补充] {code}({name}) 仅 {cnt} 个因子，建议补充至 {target} 个")

    if cat_ic:
        best_cat = max(cat_ic.items(), key=lambda x: statistics.mean(x[1]) if x[1] else 0)
        if best_cat[1]:
            cat_name = CATEGORY_NAMES.get(best_cat[0], best_cat[0])
            suggestions.append(
                f"[IC方向] {best_cat[0]}({cat_name}) 类 |IC| 均值最高 "
                f"({statistics.mean(best_cat[1]):.4f})，可扩展该方向变体"
            )

    low_ic = [f for f in os_factors if abs(f["ic_mean"]) < 0.01]
    if low_ic:
        suggestions.append(
            f"[因子清理] {len(low_ic)} 个因子 out_sample |IC| < 0.01，"
            f"建议禁用: {', '.join(f['factor_name'] for f in low_ic[:5])}"
            + (f" 等" if len(low_ic) > 5 else "")
        )

    if corr_total > 0:
        hpc = corr_data.get("high_pair_counts", {})
        if hpc.get(">0.8", 0) > 0:
            suggestions.append(
                f"[相关性去冗余] {hpc['>0.8']} 对可用因子 |corr|>0.8，建议聚类后每簇只保留 IC_IR 最高的代表因子"
            )
        replacements = corr_data.get("replace_candidates", [])
        if replacements:
            suggestions.append(
                f"[因子替换] {len(replacements)} 对因子可替换 (IC提升>=30%)，建议禁用较弱因子"
            )

    for i, s in enumerate(suggestions, 1):
        print(f"  {i}. {s}")
    if not suggestions:
        print("  无特别建议")

    print(f"\n{'=' * 60}")
    print(f"  报告完成")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(main())
