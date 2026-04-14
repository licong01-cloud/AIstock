#!/usr/bin/env python3
"""因子库深度诊断脚本 — 从多维度找出结构性问题。

用法:
    python diagnose_factor_library.py [--output report.md]
"""
import argparse
import os
import sys
import re
import json
from collections import Counter, defaultdict
from datetime import datetime
from itertools import combinations

import psycopg2
import psycopg2.extras

DB_CFG = {
    "host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("TDX_DB_PORT", "5432")),
    "user": os.getenv("TDX_DB_USER", "postgres"),
    "password": os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    "dbname": os.getenv("TDX_DB_NAME", "aistock"),
}

CATEGORY_LABELS = {
    "MOM": "动量", "VOL": "波动率", "LIQ": "流动性", "VAL": "价值",
    "QUAL": "质量", "CORR": "相关性", "TECH": "技术", "SIZE": "规模",
    "STAT": "统计", "MF": "资金流", "CHIP": "筹码", "ML": "机器学习",
}


def get_conn():
    return psycopg2.connect(**DB_CFG)


def run():
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    sections = []

    # ============================================================
    # 1. 因子IC分布与质量诊断
    # ============================================================
    cur.execute("""
        SELECT m.factor_name, m.ic_mean, m.rank_ic_mean, m.icir, m.rank_icir,
               m.top_sharpe, m.top_annual_return, m.top_max_drawdown,
               cl.category, cl.grade
        FROM aistock_factor_metrics m
        LEFT JOIN qe_factor_classification cl ON m.factor_name = cl.factor_name
        WHERE m.eval_window = 'full'
        ORDER BY m.ic_mean DESC
    """)
    all_metrics = cur.fetchall()

    total = len(all_metrics)
    ic_values = [r["ic_mean"] for r in all_metrics if r["ic_mean"] is not None]
    abs_ic = [abs(v) for v in ic_values]

    sec = []
    sec.append("## 1. IC 分布与信号质量诊断")
    sec.append("")
    sec.append(f"有指标因子总数: **{total}**")
    sec.append("")

    # IC 分布区间
    ic_bins = {"IC>0.03 (强)": 0, "0.02<IC≤0.03": 0, "0.01<IC≤0.02": 0,
               "0<IC≤0.01 (弱)": 0, "IC≤0 (无效/反向)": 0}
    ic_abs_bins = {"|IC|>0.03": 0, "0.02<|IC|≤0.03": 0, "0.01<|IC|≤0.02": 0, "|IC|≤0.01": 0}
    for v in ic_values:
        if v > 0.03: ic_bins["IC>0.03 (强)"] += 1
        elif v > 0.02: ic_bins["0.02<IC≤0.03"] += 1
        elif v > 0.01: ic_bins["0.01<IC≤0.02"] += 1
        elif v > 0: ic_bins["0<IC≤0.01 (弱)"] += 1
        else: ic_bins["IC≤0 (无效/反向)"] += 1

        av = abs(v)
        if av > 0.03: ic_abs_bins["|IC|>0.03"] += 1
        elif av > 0.02: ic_abs_bins["0.02<|IC|≤0.03"] += 1
        elif av > 0.01: ic_abs_bins["0.01<|IC|≤0.02"] += 1
        else: ic_abs_bins["|IC|≤0.01"] += 1

    sec.append("### 1.1 IC 方向分布（正=多头有效 / 负=需反转）")
    sec.append("")
    sec.append("| IC 区间 | 数量 | 占比 |")
    sec.append("|---------|------|------|")
    for label, cnt in ic_bins.items():
        sec.append(f"| {label} | {cnt} | {cnt*100//max(total,1)}% |")
    sec.append("")

    sec.append("### 1.2 |IC| 绝对值分布（信号强度）")
    sec.append("")
    sec.append("| |IC| 区间 | 数量 | 占比 |")
    sec.append("|----------|------|------|")
    for label, cnt in ic_abs_bins.items():
        sec.append(f"| {label} | {cnt} | {cnt*100//max(total,1)}% |")
    sec.append("")

    # IC 统计
    if ic_values:
        import statistics
        sec.append("### 1.3 IC 统计摘要")
        sec.append("")
        sec.append(f"- 均值: {statistics.mean(ic_values):.4f}")
        sec.append(f"- 中位数: {statistics.median(ic_values):.4f}")
        sec.append(f"- 标准差: {statistics.stdev(ic_values):.4f}")
        sec.append(f"- 最大值: {max(ic_values):.4f}")
        sec.append(f"- 最小值: {min(ic_values):.4f}")
        sec.append(f"- 正IC因子占比: {sum(1 for v in ic_values if v>0)*100//len(ic_values)}%")
        sec.append(f"- |IC|>0.02 因子数: {sum(1 for v in abs_ic if v>0.02)} ({sum(1 for v in abs_ic if v>0.02)*100//len(abs_ic)}%)")
        sec.append("")

    sections.append("\n".join(sec))

    # ============================================================
    # 2. 因子同质化 / 重复性分析
    # ============================================================
    cur.execute("""
        SELECT c.factor_name, c.code_text, cl.category
        FROM aistock_factor_catalog c
        LEFT JOIN qe_factor_classification cl ON c.factor_name = cl.factor_name
        WHERE COALESCE(c.is_available, true) = true AND c.code_text IS NOT NULL
        ORDER BY c.factor_name
    """)
    all_factors = cur.fetchall()

    sec = []
    sec.append("## 2. 因子同质化 / 重复性诊断")
    sec.append("")

    # 2.1 名称相似检测 — 提取关键词
    name_groups = defaultdict(list)
    for f in all_factors:
        name = f["factor_name"].lower()
        # 提取核心词根
        tokens = set(re.split(r'[_\d]+', name)) - {'', 'factor', 'adj', 'adjusted', 'composite', 'ratio', 'rate', 'log', 'momentum'}
        key = frozenset(tokens)
        if len(tokens) >= 2:
            name_groups[key].append(f["factor_name"])

    # 找出 token 重叠度高的组
    dup_candidates = []
    seen_names = set()
    for key, names in name_groups.items():
        if len(names) >= 2:
            dup_candidates.append(names)
            seen_names.update(names)

    # 2.2 IC 相似检测 — IC 差异极小的因子对
    metrics_by_name = {r["factor_name"]: r for r in all_metrics}
    ic_similar_pairs = []
    factors_with_ic = [(f["factor_name"], metrics_by_name[f["factor_name"]]["ic_mean"])
                       for f in all_factors
                       if f["factor_name"] in metrics_by_name
                       and metrics_by_name[f["factor_name"]]["ic_mean"] is not None]

    # 按 IC 排序后检查相邻因子
    factors_with_ic.sort(key=lambda x: x[1])
    for i in range(len(factors_with_ic) - 1):
        n1, ic1 = factors_with_ic[i]
        n2, ic2 = factors_with_ic[i+1]
        if abs(ic1 - ic2) < 0.0001 and n1 != n2:
            # 检查是否 category 相同
            cat1 = metrics_by_name.get(n1, {}).get("category")
            cat2 = metrics_by_name.get(n2, {}).get("category")
            ic_similar_pairs.append((n1, n2, ic1, ic2, cat1, cat2))

    sec.append(f"### 2.1 名称高度相似的因子组（共 {len(dup_candidates)} 组）")
    sec.append("")
    if dup_candidates:
        shown = 0
        for names in sorted(dup_candidates, key=lambda x: -len(x))[:15]:
            sec.append(f"- **{len(names)}个**: {', '.join(names[:5])}{'...' if len(names)>5 else ''}")
            shown += 1
        if len(dup_candidates) > 15:
            sec.append(f"- ... 另有 {len(dup_candidates)-15} 组")
    sec.append("")

    sec.append(f"### 2.2 IC 几乎相同的因子对（差异<0.0001，共 {len(ic_similar_pairs)} 对）")
    sec.append("")
    if ic_similar_pairs:
        sec.append("| 因子A | 因子B | IC_A | IC_B | 类别A | 类别B |")
        sec.append("|-------|-------|------|------|-------|-------|")
        for n1, n2, ic1, ic2, c1, c2 in ic_similar_pairs[:20]:
            sec.append(f"| {n1[:30]} | {n2[:30]} | {ic1:.4f} | {ic2:.4f} | {c1 or '-'} | {c2 or '-'} |")
        if len(ic_similar_pairs) > 20:
            sec.append(f"\n*另有 {len(ic_similar_pairs)-20} 对...*")
    sec.append("")

    # 2.3 代码模式相似检测 — 提取核心计算逻辑
    code_patterns = defaultdict(list)
    for f in all_factors:
        code = f["code_text"] or ""
        # 提取 compute_factor 函数体中的关键操作
        ops = []
        for pattern in [r'\.pct_change\((\d+)\)', r'\.rolling\((\d+)\)', r'\.shift\((\d+)\)',
                        r'\.rank\(', r'\.std\(', r'\.mean\(', r'\.corr\(',
                        r'\.ewm\(', r'\.diff\(', r'np\.log', r'\.quantile\(']:
            matches = re.findall(pattern, code)
            if matches:
                ops.append(f"{pattern}:{','.join(matches) if matches[0] else 'y'}")
        key = "|".join(sorted(ops))
        if key and len(ops) >= 2:
            code_patterns[key].append(f["factor_name"])

    code_dup_groups = [(k, v) for k, v in code_patterns.items() if len(v) >= 3]
    code_dup_groups.sort(key=lambda x: -len(x[1]))

    sec.append(f"### 2.3 代码计算模式相同的因子组（≥3个因子使用相同操作组合，共 {len(code_dup_groups)} 组）")
    sec.append("")
    for pattern, names in code_dup_groups[:10]:
        ops_readable = pattern.replace("\\.", ".").replace("\\(", "(").replace("\\)", ")")
        sec.append(f"- **{len(names)}个因子**共享模式 `{ops_readable[:80]}...`")
        sec.append(f"  例: {', '.join(names[:4])}")
    sec.append("")

    sections.append("\n".join(sec))

    # ============================================================
    # 3. 数据集利用深度分析
    # ============================================================
    sec = []
    sec.append("## 3. 数据集利用深度分析")
    sec.append("")

    DATASET_COLS = {
        "daily_pv.h5": ["open", "close", "high", "low", "volume", "amount", "factor"],
        "daily_basic.h5": ["db_close", "db_turnover_rate", "db_turnover_rate_f", "db_volume_ratio",
                           "db_pe", "db_pe_ttm", "db_pb", "db_ps", "db_ps_ttm",
                           "db_dv_ratio", "db_dv_ttm", "db_total_share", "db_float_share",
                           "db_free_share", "db_total_mv", "db_circ_mv"],
        "moneyflow.h5": ["mf_sm_buy_vol", "mf_sm_buy_amt", "mf_sm_sell_vol", "mf_sm_sell_amt",
                          "mf_md_buy_vol", "mf_md_buy_amt", "mf_md_sell_vol", "mf_md_sell_amt",
                          "mf_lg_buy_vol", "mf_lg_buy_amt", "mf_lg_sell_vol", "mf_lg_sell_amt",
                          "mf_elg_buy_vol", "mf_elg_buy_amt", "mf_elg_sell_vol", "mf_elg_sell_amt",
                          "mf_net_vol", "mf_net_amt"],
        "bak_basic.h5": ["bb_pe_dyn", "bb_total_assets", "bb_liquid_assets", "bb_fixed_assets",
                          "bb_reserved", "bb_reserved_pershare", "bb_eps", "bb_bvps",
                          "bb_undp", "bb_per_undp", "bb_rev_yoy", "bb_profit_yoy",
                          "bb_gpr", "bb_npr", "bb_holder_num"],
        "cyq_perf.h5": ["cp_his_low", "cp_his_high", "cp_cost_5pct", "cp_cost_15pct",
                         "cp_cost_50pct", "cp_cost_85pct", "cp_cost_95pct",
                         "cp_weight_avg", "cp_winner_rate"],
        "sector_data.h5": ["sw2_open", "sw2_high", "sw2_low", "sw2_close", "sw2_pct_change",
                            "sw2_vol", "sw2_amount", "sw2_pe", "sw2_pb", "sw2_total_mv"],
    }

    col_usage = defaultdict(int)
    for f in all_factors:
        code = f["code_text"] or ""
        for ds, cols in DATASET_COLS.items():
            for col in cols:
                if col in code:
                    col_usage[(ds, col)] += 1

    sec.append("### 3.1 各数据集字段使用频次")
    sec.append("")
    for ds in DATASET_COLS:
        sec.append(f"**{ds}**:")
        sec.append("")
        sec.append("| 字段 | 使用次数 | 状态 |")
        sec.append("|------|---------|------|")
        cols_sorted = sorted(DATASET_COLS[ds], key=lambda c: -col_usage.get((ds, c), 0))
        for col in cols_sorted:
            cnt = col_usage.get((ds, col), 0)
            status = "充分" if cnt > 50 else ("中等" if cnt > 10 else ("不足" if cnt > 0 else "**未使用**"))
            sec.append(f"| {col} | {cnt} | {status} |")
        sec.append("")

    # 识别未使用/低使用的字段
    unused_cols = []
    low_use_cols = []
    for ds, cols in DATASET_COLS.items():
        for col in cols:
            cnt = col_usage.get((ds, col), 0)
            if cnt == 0:
                unused_cols.append((ds, col))
            elif cnt < 5:
                low_use_cols.append((ds, col, cnt))

    sec.append("### 3.2 未使用/低使用字段汇总")
    sec.append("")
    if unused_cols:
        sec.append(f"**完全未使用 ({len(unused_cols)} 个):**")
        for ds, col in unused_cols:
            sec.append(f"- `{ds}` → `{col}`")
        sec.append("")
    if low_use_cols:
        sec.append(f"**使用<5次 ({len(low_use_cols)} 个):**")
        for ds, col, cnt in low_use_cols:
            sec.append(f"- `{ds}` → `{col}` ({cnt}次)")
        sec.append("")

    # 3.3 跨数据集组合使用
    ds_combos = Counter()
    for f in all_factors:
        code = f["code_text"] or ""
        used_ds = set()
        for ds, cols in DATASET_COLS.items():
            if any(col in code for col in cols):
                used_ds.add(ds)
        if len(used_ds) >= 2:
            ds_combos[frozenset(used_ds)] += 1

    sec.append("### 3.3 跨数据集组合使用频次")
    sec.append("")
    sec.append("| 数据集组合 | 因子数 |")
    sec.append("|-----------|--------|")
    for combo, cnt in sorted(ds_combos.items(), key=lambda x: -x[1])[:15]:
        sec.append(f"| {' + '.join(sorted(c.replace('.h5','') for c in combo))} | {cnt} |")
    sec.append("")

    sections.append("\n".join(sec))

    # ============================================================
    # 4. 类别内部质量分析
    # ============================================================
    sec = []
    sec.append("## 4. 各类别内部质量诊断")
    sec.append("")

    cat_metrics = defaultdict(list)
    for r in all_metrics:
        cat = r.get("category") or "未分类"
        if r["ic_mean"] is not None:
            cat_metrics[cat].append(r)

    sec.append("| 类别 | 因子数 | 正IC数 | 正IC率 | 平均IC | 中位IC | 最佳IC | IC>0.02数 | 平均Sharpe | 平均年化 |")
    sec.append("|------|--------|--------|--------|--------|--------|--------|----------|-----------|---------|")
    for cat in sorted(CATEGORY_LABELS.keys()):
        rows = cat_metrics.get(cat, [])
        if not rows:
            sec.append(f"| {CATEGORY_LABELS[cat]} ({cat}) | 0 | - | - | - | - | - | - | - | - |")
            continue
        ics = [r["ic_mean"] for r in rows]
        pos = sum(1 for v in ics if v > 0)
        ic_gt02 = sum(1 for v in ics if v > 0.02)
        sharpes = [r["top_sharpe"] for r in rows if r["top_sharpe"] is not None]
        anns = [r["top_annual_return"] for r in rows if r["top_annual_return"] is not None]
        import statistics
        avg_sh = f"{statistics.mean(sharpes):.2f}" if sharpes else "-"
        avg_an = f"{statistics.mean(anns)*100:.1f}%" if anns else "-"
        sec.append(f"| {CATEGORY_LABELS[cat]} ({cat}) | {len(rows)} | {pos} | {pos*100//len(rows)}% | "
                   f"{statistics.mean(ics):.4f} | {statistics.median(ics):.4f} | {max(ics):.4f} | "
                   f"{ic_gt02} | {avg_sh} | {avg_an} |")
    sec.append("")

    # 各类别的 "有效因子"（IC>0.02）比率
    sec.append("### 4.1 各类别有效因子率（IC>0.02 视为有效）")
    sec.append("")
    for cat in sorted(CATEGORY_LABELS.keys()):
        rows = cat_metrics.get(cat, [])
        if not rows: continue
        effective = sum(1 for r in rows if r["ic_mean"] > 0.02)
        sec.append(f"- **{CATEGORY_LABELS[cat]}**: {effective}/{len(rows)} ({effective*100//len(rows)}%)")
    sec.append("")

    sections.append("\n".join(sec))

    # ============================================================
    # 5. 反向因子（负IC有alpha）
    # ============================================================
    sec = []
    sec.append("## 5. 反向因子分析（IC<0 但 |IC|>0.02）")
    sec.append("")
    sec.append("这些因子虽然 IC 为负，但取反后可能是有效的 alpha 信号:")
    sec.append("")

    reverse_factors = [(r["factor_name"], r["ic_mean"], r["icir"], r["top_sharpe"],
                        r["top_annual_return"], r.get("category"))
                       for r in all_metrics
                       if r["ic_mean"] is not None and r["ic_mean"] < -0.02]
    reverse_factors.sort(key=lambda x: x[1])

    sec.append(f"**反向因子总数: {len(reverse_factors)}**")
    sec.append("")
    if reverse_factors:
        sec.append("| 因子名 | IC | ICIR | 类别 | 取反后IC |")
        sec.append("|--------|-----|------|------|---------|")
        for name, ic, icir, sharpe, ann, cat in reverse_factors[:20]:
            icir_s = f"{icir:.3f}" if icir else "-"
            sec.append(f"| {name[:35]} | {ic:.4f} | {icir_s} | {cat or '-'} | {-ic:.4f} |")
    sec.append("")

    sections.append("\n".join(sec))

    # ============================================================
    # 6. 评级瓶颈分析
    # ============================================================
    sec = []
    sec.append("## 6. 评级瓶颈分析")
    sec.append("")

    cur.execute("""
        SELECT cl.grade, cl.category,
               AVG(m.ic_mean) as avg_ic, AVG(m.top_sharpe) as avg_sharpe,
               AVG(m.top_annual_return) as avg_ann,
               AVG(m.top_max_drawdown) as avg_mdd,
               COUNT(*) as cnt
        FROM qe_factor_classification cl
        JOIN aistock_factor_metrics m ON cl.factor_name = m.factor_name AND m.eval_window = 'full'
        GROUP BY cl.grade, cl.category
        ORDER BY cl.grade, cl.category
    """)
    grade_cat = cur.fetchall()

    # 按评级汇总
    grade_agg = defaultdict(lambda: {"cnt": 0, "ics": [], "sharpes": [], "anns": [], "mdds": []})
    for r in grade_cat:
        g = r["grade"]
        grade_agg[g]["cnt"] += r["cnt"]
        if r["avg_ic"] is not None: grade_agg[g]["ics"].append((r["avg_ic"], r["cnt"]))
        if r["avg_sharpe"] is not None: grade_agg[g]["sharpes"].append((r["avg_sharpe"], r["cnt"]))
        if r["avg_ann"] is not None: grade_agg[g]["anns"].append((r["avg_ann"], r["cnt"]))
        if r["avg_mdd"] is not None: grade_agg[g]["mdds"].append((r["avg_mdd"], r["cnt"]))

    sec.append("| 评级 | 因子数 | 加权平均IC | 加权平均Sharpe | 加权平均年化 | 加权平均回撤 |")
    sec.append("|------|--------|-----------|--------------|------------|------------|")
    for g in ["S", "A", "B", "C", "D"]:
        d = grade_agg.get(g, {"cnt": 0})
        if d["cnt"] == 0:
            sec.append(f"| {g} | 0 | - | - | - | - |")
            continue
        def wavg(lst):
            if not lst: return None
            total_w = sum(w for _, w in lst)
            return sum(v*w for v, w in lst) / total_w if total_w else None
        ic = wavg(d["ics"])
        sh = wavg(d["sharpes"])
        an = wavg(d["anns"])
        md = wavg(d["mdds"])
        ic_s = f"{ic:.4f}" if ic is not None else "-"
        sh_s = f"{sh:.2f}" if sh is not None else "-"
        an_s = f"{an*100:.1f}%" if an is not None else "-"
        md_s = f"{md*100:.1f}%" if md is not None else "-"
        sec.append(f"| {g} | {d['cnt']} | {ic_s} | {sh_s} | {an_s} | {md_s} |")
    sec.append("")

    # D 级因子的问题根因
    sec.append("### 6.1 D 级因子根因分析")
    sec.append("")
    d_factors = [r for r in all_metrics if r.get("grade") == "D" and r["ic_mean"] is not None]
    if d_factors:
        d_ic_neg = sum(1 for r in d_factors if r["ic_mean"] <= 0)
        d_ic_weak = sum(1 for r in d_factors if 0 < r["ic_mean"] <= 0.01)
        d_sharpe_neg = sum(1 for r in d_factors if r["top_sharpe"] is not None and r["top_sharpe"] <= 0)
        d_mdd_severe = sum(1 for r in d_factors if r["top_max_drawdown"] is not None and r["top_max_drawdown"] < -0.30)
        sec.append(f"- D 级因子总数: {len(d_factors)}")
        sec.append(f"- IC≤0（无正向信号）: {d_ic_neg} ({d_ic_neg*100//len(d_factors)}%)")
        sec.append(f"- 0<IC≤0.01（信号极弱）: {d_ic_weak} ({d_ic_weak*100//len(d_factors)}%)")
        sec.append(f"- Sharpe≤0: {d_sharpe_neg} ({d_sharpe_neg*100//len(d_factors)}%)")
        sec.append(f"- 最大回撤>30%: {d_mdd_severe} ({d_mdd_severe*100//len(d_factors)}%)")
    sec.append("")

    sections.append("\n".join(sec))

    # ============================================================
    # 7. 因子构造复杂度分析
    # ============================================================
    sec = []
    sec.append("## 7. 因子构造复杂度分析")
    sec.append("")

    complexity_stats = []
    for f in all_factors:
        code = f["code_text"] or ""
        lines = len(code.split("\n"))
        ops_count = sum(len(re.findall(p, code)) for p in [
            r'\.rolling\(', r'\.shift\(', r'\.pct_change\(', r'\.rank\(',
            r'\.corr\(', r'\.std\(', r'\.mean\(', r'np\.log', r'\.ewm\(',
            r'\.diff\(', r'\.clip\(', r'\.quantile\(', r'groupby',
        ])
        ds_count = sum(1 for ds, cols in DATASET_COLS.items()
                       if any(col in code for col in cols))
        m = metrics_by_name.get(f["factor_name"])
        ic = m["ic_mean"] if m and m.get("ic_mean") is not None else None
        complexity_stats.append({
            "name": f["factor_name"], "lines": lines, "ops": ops_count,
            "datasets": ds_count, "ic": ic, "category": f.get("category"),
        })

    # 按数据集数分组
    ds_groups = defaultdict(list)
    for s in complexity_stats:
        if s["ic"] is not None:
            ds_groups[s["datasets"]].append(s["ic"])

    sec.append("### 7.1 数据集使用数量 vs 平均IC")
    sec.append("")
    sec.append("| 使用数据集数 | 因子数 | 平均IC | 中位IC | IC>0.02率 |")
    sec.append("|-------------|--------|--------|--------|----------|")
    for n_ds in sorted(ds_groups.keys()):
        ics = ds_groups[n_ds]
        import statistics
        avg_ic = statistics.mean(ics)
        med_ic = statistics.median(ics)
        gt02 = sum(1 for v in ics if v > 0.02)
        sec.append(f"| {n_ds} | {len(ics)} | {avg_ic:.4f} | {med_ic:.4f} | {gt02*100//len(ics)}% |")
    sec.append("")

    # 按操作复杂度分组
    ops_groups = defaultdict(list)
    for s in complexity_stats:
        if s["ic"] is not None:
            bucket = s["ops"] // 3  # 0-2, 3-5, 6-8, 9+
            ops_groups[bucket].append(s["ic"])

    sec.append("### 7.2 操作复杂度 vs 平均IC")
    sec.append("")
    sec.append("| 操作数区间 | 因子数 | 平均IC | IC>0.02率 |")
    sec.append("|-----------|--------|--------|----------|")
    for bucket in sorted(ops_groups.keys()):
        ics = ops_groups[bucket]
        label = f"{bucket*3}-{bucket*3+2}"
        avg_ic = statistics.mean(ics)
        gt02 = sum(1 for v in ics if v > 0.02)
        sec.append(f"| {label} | {len(ics)} | {avg_ic:.4f} | {gt02*100//max(len(ics),1)}% |")
    sec.append("")

    sections.append("\n".join(sec))

    # ============================================================
    # 8. 关键缺失因子类型识别
    # ============================================================
    sec = []
    sec.append("## 8. 关键缺失因子类型识别")
    sec.append("")

    # 基于代码分析：哪些经典量化因子计算方式缺失
    classic_patterns = {
        "MACD/信号线": (r'ewm.*span.*12|ewm.*span.*26|macd', "TECH"),
        "RSI/相对强弱": (r'rsi|rs_up|rs_down|gain.*loss', "TECH"),
        "布林带/BB": (r'bollinger|bband|bb_upper|bb_lower', "TECH"),
        "ATR/平均真实波幅": (r'atr|true_range|tr_', "VOL"),
        "OBV/能量潮": (r'obv|on_balance', "TECH"),
        "Hurst指数": (r'hurst', "STAT"),
        "偏度因子": (r'skew', "STAT"),
        "峰度因子": (r'kurt', "STAT"),
        "Beta/市场敏感度": (r'beta.*market|market.*beta|capm', "CORR"),
        "残差动量": (r'residual.*mom|idiosyncratic', "CORR"),
        "Amihud非流动性": (r'amihud|illiq', "LIQ"),
        "信息比率": (r'information_ratio|ir_', "STAT"),
        "最大回撤因子": (r'max_drawdown|mdd_', "VOL"),
        "资金集中度": (r'concentration.*flow|hhi.*flow', "MF"),
        "机构持仓变化": (r'inst_hold|institution', "QUAL"),
        "ROE/ROA动量": (r'roe.*mom|roa.*mom|roe.*change|roa.*change', "QUAL"),
        "营收增速": (r'rev.*growth|revenue.*grow|rev_yoy.*mom', "QUAL"),
        "PEG": (r'peg_|pe.*growth|pe_g', "VAL"),
        "自由现金流收益率": (r'fcf.*yield|free_cash.*yield', "VAL"),
        "股息率动量": (r'div.*mom|dv.*mom|dividend.*change', "VAL"),
    }

    all_code = "\n".join(f["code_text"] or "" for f in all_factors).lower()
    found_patterns = {}
    missing_patterns = {}
    for name, (pattern, cat) in classic_patterns.items():
        if re.search(pattern, all_code, re.IGNORECASE):
            found_patterns[name] = cat
        else:
            missing_patterns[name] = cat

    sec.append(f"### 8.1 经典量化因子覆盖检测（{len(classic_patterns)} 种经典因子）")
    sec.append("")
    sec.append(f"- 已覆盖: {len(found_patterns)}")
    sec.append(f"- **未覆盖: {len(missing_patterns)}**")
    sec.append("")

    if missing_patterns:
        sec.append("**缺失的经典因子类型:**")
        sec.append("")
        sec.append("| 因子类型 | 所属类别 | 经济学意义 |")
        sec.append("|---------|---------|-----------|")
        econ_meaning = {
            "MACD/信号线": "趋势跟踪信号，捕捉价格动量转折",
            "RSI/相对强弱": "超买超卖判断，均值回归信号",
            "布林带/BB": "波动率通道突破，价格偏离度",
            "ATR/平均真实波幅": "真实波动幅度，风险度量",
            "OBV/能量潮": "量价关系确认，资金方向判断",
            "Hurst指数": "时序记忆性检验，趋势/均值回复判断",
            "偏度因子": "收益分布不对称性，尾部风险",
            "峰度因子": "收益分布厚尾程度，极端事件频率",
            "Beta/市场敏感度": "系统性风险暴露，CAPM核心风险因子",
            "残差动量": "剥离市场影响后的个股alpha，Blitz et al. (2011)",
            "Amihud非流动性": "价格冲击成本，流动性风险溢价",
            "信息比率": "超额收益稳定性，因子有效性度量",
            "最大回撤因子": "下行风险度量，投资者行为锚定",
            "资金集中度": "大资金行为一致性，信息不对称信号",
            "机构持仓变化": "聪明钱信号，机构信息优势",
            "ROE/ROA动量": "盈利能力变化趋势，基本面动量",
            "营收增速": "成长性信号，市场对增长的定价偏差",
            "PEG": "估值与成长的综合度量，Peter Lynch经典指标",
            "自由现金流收益率": "企业真实造血能力，价值投资核心",
            "股息率动量": "分红政策变化信号，稳定性指标",
        }
        for name, cat in sorted(missing_patterns.items(), key=lambda x: x[1]):
            meaning = econ_meaning.get(name, "")
            sec.append(f"| {name} | {CATEGORY_LABELS.get(cat, cat)} | {meaning} |")
    sec.append("")

    if found_patterns:
        sec.append(f"**已覆盖的经典因子:** {', '.join(sorted(found_patterns.keys()))}")
    sec.append("")

    sections.append("\n".join(sec))

    # ============================================================
    # 9. 综合问题清单
    # ============================================================
    sec = []
    sec.append("## 9. 综合问题清单（按严重度排序）")
    sec.append("")

    issues = []

    # 严重问题
    neg_ic_pct = sum(1 for v in ic_values if v <= 0) * 100 // len(ic_values)
    if neg_ic_pct > 50:
        issues.append(("CRITICAL", f"**{neg_ic_pct}% 因子 IC≤0** — 超过半数因子无正向预测能力，因子库整体信号质量堪忧"))
    d_pct = sum(1 for r in all_metrics if r.get("grade") == "D") * 100 // total
    if d_pct > 80:
        issues.append(("CRITICAL", f"**{d_pct}% 因子评级 D** — 绝大多数因子质量不合格，需要系统性重构"))
    if len(missing_patterns) > 10:
        issues.append(("CRITICAL", f"**{len(missing_patterns)} 种经典因子缺失** — 大量学术界/业界验证的有效因子未覆盖"))

    # 高严重
    if ic_similar_pairs:
        issues.append(("HIGH", f"**{len(ic_similar_pairs)} 对因子IC几乎相同** — 存在大量同质化因子，信息冗余"))
    for cat in ["ML", "STAT", "SIZE", "TECH", "CORR"]:
        cnt = sum(1 for f in all_factors if f.get("category") == cat)
        if cnt < 5:
            issues.append(("HIGH", f"**{CATEGORY_LABELS[cat]}类仅{cnt}个因子** — 类别严重不足"))

    # 中等
    for ds, col in unused_cols:
        issues.append(("MEDIUM", f"**`{ds}`→`{col}` 完全未使用** — 潜在信号未被挖掘"))

    cat_imbalance = max(len(cat_metrics.get(c, [])) for c in CATEGORY_LABELS) / max(min(len(cat_metrics.get(c, [])) for c in CATEGORY_LABELS if cat_metrics.get(c)), 1)
    if cat_imbalance > 20:
        issues.append(("MEDIUM", f"**类别极度不平衡（最大/最小={cat_imbalance:.0f}x）** — LIQ/VAL 过多，STAT/SIZE/TECH 过少"))

    for sev in ["CRITICAL", "HIGH", "MEDIUM"]:
        sev_issues = [(s, msg) for s, msg in issues if s == sev]
        if sev_issues:
            emoji = {"CRITICAL": "🔴", "HIGH": "🟡", "MEDIUM": "🟠"}[sev]
            sec.append(f"### {emoji} {sev}")
            sec.append("")
            for _, msg in sev_issues:
                sec.append(f"- {msg}")
            sec.append("")

    sections.append("\n".join(sec))

    # ============================================================
    # 组装报告
    # ============================================================
    report = []
    report.append("# 因子库深度诊断报告")
    report.append("")
    report.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append(f"因子总数: {len(all_factors)} | 有指标: {total}")
    report.append("")
    report.append("\n\n".join(sections))

    cur.close()
    conn.close()

    return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(description="因子库深度诊断")
    parser.add_argument("--output", "-o", default=None)
    args = parser.parse_args()

    report = run()

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"报告已保存到: {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()
