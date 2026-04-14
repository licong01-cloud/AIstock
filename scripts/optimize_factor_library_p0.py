#!/usr/bin/env python3
"""P0 因子库优化脚本 — 基于相关性去重 + 反向因子注册。

子命令:
    dedup    — 基于 qe_factor_correlations 相关性矩阵去重（高相关因子禁用）
    reverse  — 批量注册 IC<0 因子的反向版本
    report   — 输出优化前后对比

用法:
    python optimize_factor_library_p0.py dedup [--threshold 0.95] [--apply] [--log FILE]
    python optimize_factor_library_p0.py reverse [--min-ic 0.02] [--apply]
    python optimize_factor_library_p0.py report
"""
import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime

import psycopg2
import psycopg2.extras

DB_CFG = {
    "host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("TDX_DB_PORT", "5432")),
    "user": os.getenv("TDX_DB_USER", "postgres"),
    "password": os.getenv("TDX_DB_PASSWORD", "lc78080808"),
    "dbname": os.getenv("TDX_DB_NAME", "aistock"),
}


def get_conn():
    conn = psycopg2.connect(**DB_CFG)
    conn.autocommit = True
    return conn


# ======================================================================
# Union-Find for connected components
# ======================================================================
class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1

    def components(self):
        groups = defaultdict(set)
        for x in self.parent:
            groups[self.find(x)].add(x)
        return list(groups.values())


# ======================================================================
# DEDUP — 基于相关性矩阵去重
# ======================================================================
def cmd_dedup(args):
    threshold = args.threshold
    apply_changes = args.apply
    log_path = args.log

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print(f"=== 因子去重（相关性阈值: {threshold}）===\n")

    # 1. 查询高相关因子对
    cur.execute("""
        SELECT c1.factor_name AS factor_a, c2.factor_name AS factor_b,
               c1.source AS source_a, c2.source AS source_b,
               cor.correlation,
               m1.ic_mean AS ic_a, m1.icir AS icir_a, m1.top_sharpe AS sharpe_a, m1.top_max_drawdown AS mdd_a,
               m2.ic_mean AS ic_b, m2.icir AS icir_b, m2.top_sharpe AS sharpe_b, m2.top_max_drawdown AS mdd_b
        FROM qe_factor_correlations cor
        JOIN aistock_factor_catalog c1 ON cor.factor_a_id = c1.id AND c1.is_available = true
        JOIN aistock_factor_catalog c2 ON cor.factor_b_id = c2.id AND c2.is_available = true
        LEFT JOIN aistock_factor_metrics m1 ON c1.factor_name = m1.factor_name AND m1.eval_window = 'full'
        LEFT JOIN aistock_factor_metrics m2 ON c2.factor_name = m2.factor_name AND m2.eval_window = 'full'
        WHERE ABS(cor.correlation) >= %s
    """, (threshold,))
    pairs = cur.fetchall()

    if not pairs:
        print(f"未找到相关性 >= {threshold} 的因子对。")
        cur.close()
        conn.close()
        return

    print(f"高相关因子对数: {len(pairs)}")

    # 2. 构建 Union-Find，找连通分量
    uf = UnionFind()
    factor_info = {}  # factor_name -> {source, ic, icir, sharpe, mdd}

    for p in pairs:
        uf.union(p["factor_a"], p["factor_b"])
        # 记录因子信息
        for suffix in ["a", "b"]:
            name = p[f"factor_{suffix}"]
            if name not in factor_info:
                factor_info[name] = {
                    "source": p[f"source_{suffix}"],
                    "ic": p.get(f"ic_{suffix}"),
                    "icir": p.get(f"icir_{suffix}"),
                    "sharpe": p.get(f"sharpe_{suffix}"),
                    "mdd": p.get(f"mdd_{suffix}"),
                }

    components = uf.components()
    # 只关注大小 >= 2 的簇
    clusters = [c for c in components if len(c) >= 2]
    clusters.sort(key=lambda c: -len(c))

    print(f"高相关因子簇数: {len(clusters)}")
    total_in_clusters = sum(len(c) for c in clusters)
    print(f"涉及因子总数: {total_in_clusters}")

    # 3. 每个簇选 keeper
    def score_factor(name):
        info = factor_info.get(name, {})
        ic = abs(info.get("ic") or 0)
        icir = abs(info.get("icir") or 0)
        sharpe = info.get("sharpe") or 0
        mdd = abs(info.get("mdd") or 0)
        return ic * 0.4 + icir * 0.3 + max(sharpe, 0) * 0.2 + max(1 - mdd, 0) * 0.1

    keepers = set()
    to_disable = []
    cluster_details = []

    for cluster in clusters:
        ranked = sorted(cluster, key=score_factor, reverse=True)
        keeper = ranked[0]
        keepers.add(keeper)
        disabled = ranked[1:]
        for d in disabled:
            info = factor_info.get(d, {})
            to_disable.append({
                "factor_name": d,
                "source": info.get("source", "rdagent_task_sync"),
                "keeper": keeper,
                "score": round(score_factor(d), 6),
            })
        keeper_info = factor_info.get(keeper, {})
        cluster_details.append({
            "keeper": keeper,
            "keeper_ic": keeper_info.get("ic"),
            "keeper_score": round(score_factor(keeper), 6),
            "disabled_count": len(disabled),
            "disabled": [d for d in disabled],
        })

    print(f"\n保留因子数: {len(keepers)}")
    print(f"将禁用因子数: {len(to_disable)}")

    # 4. 输出详情
    print(f"\n--- 前 20 个簇详情 ---\n")
    for i, cd in enumerate(cluster_details[:20]):
        ki = factor_info.get(cd["keeper"], {})
        ic_str = f"{ki.get('ic', 0):.4f}" if ki.get("ic") is not None else "-"
        print(f"簇 {i+1} ({cd['disabled_count']+1} 个因子):")
        print(f"  KEEP: {cd['keeper']} (IC={ic_str}, score={cd['keeper_score']})")
        for d in cd["disabled"][:5]:
            di = factor_info.get(d, {})
            dic = f"{di.get('ic', 0):.4f}" if di.get("ic") is not None else "-"
            print(f"  DISABLE: {d} (IC={dic}, score={round(score_factor(d), 6)})")
        if len(cd["disabled"]) > 5:
            print(f"  ... 另有 {len(cd['disabled'])-5} 个")
        print()

    if len(cluster_details) > 20:
        print(f"... 另有 {len(cluster_details)-20} 个簇\n")

    # 5. 保存日志
    if log_path:
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "threshold": threshold,
            "applied": apply_changes,
            "total_pairs": len(pairs),
            "total_clusters": len(clusters),
            "keepers_count": len(keepers),
            "disabled_count": len(to_disable),
            "clusters": cluster_details,
        }
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        print(f"日志已保存: {log_path}")

    # 6. 执行
    if apply_changes:
        print(f"\n=== 执行去重: 禁用 {len(to_disable)} 个因子 ===")
        disabled_count = 0
        for item in to_disable:
            cur.execute("""
                UPDATE aistock_factor_catalog
                SET is_available = false
                WHERE factor_name = %s AND source = %s AND is_available = true
            """, (item["factor_name"], item["source"]))
            if cur.rowcount > 0:
                disabled_count += 1
        print(f"成功禁用: {disabled_count} 个因子")
    else:
        print("\n[DRY-RUN] 添加 --apply 参数以执行")

    cur.close()
    conn.close()


# ======================================================================
# REVERSE — 批量注册反向因子
# ======================================================================
def cmd_reverse(args):
    min_ic = args.min_ic
    apply_changes = args.apply

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print(f"=== 反向因子注册（|IC| > {min_ic}）===\n")

    # 1. 读取 IC<0 且 |IC|>min_ic 的可用因子（去重取最新指标）
    cur.execute("""
        SELECT DISTINCT ON (c.factor_name)
               c.factor_name, c.source, c.code_text,
               m.ic_mean, m.icir, m.top_sharpe,
               cl.category
        FROM aistock_factor_catalog c
        JOIN aistock_factor_metrics m ON c.factor_name = m.factor_name AND m.eval_window = 'full'
        LEFT JOIN qe_factor_classification cl ON c.factor_name = cl.factor_name
        WHERE c.is_available = true
          AND c.code_text IS NOT NULL
          AND m.ic_mean < %s
        ORDER BY c.factor_name, m.calculated_at DESC
    """, (-min_ic,))
    candidates = cur.fetchall()

    print(f"IC < -{min_ic} 的可用因子: {len(candidates)}")

    # 2. 过滤已存在的 neg_ 因子
    existing_neg = set()
    cur.execute("""
        SELECT factor_name FROM aistock_factor_catalog
        WHERE factor_name LIKE 'neg\\_%'
    """)
    for row in cur.fetchall():
        existing_neg.add(row["factor_name"])

    to_register = []
    skipped_exists = 0
    skipped_no_code = 0

    for f in candidates:
        neg_name = f"neg_{f['factor_name']}"
        # 截断到合理长度（PostgreSQL TEXT 无限制，但保持可读）
        if len(neg_name) > 80:
            neg_name = neg_name[:80]

        if neg_name in existing_neg:
            skipped_exists += 1
            continue

        code = f["code_text"]
        if not code:
            skipped_no_code += 1
            continue

        # 注入 factor = -factor
        new_code = _inject_negation(code, neg_name, f["factor_name"])
        if new_code is None:
            skipped_no_code += 1
            continue

        to_register.append({
            "factor_name": neg_name,
            "original_name": f["factor_name"],
            "code_text": new_code,
            "ic_original": f["ic_mean"],
            "ic_reversed": -f["ic_mean"] if f["ic_mean"] else None,
            "category": f.get("category"),
        })

    print(f"将注册反向因子: {len(to_register)}")
    print(f"已存在跳过: {skipped_exists}")
    print(f"无法处理跳过: {skipped_no_code}")

    # 3. 预览
    print(f"\n--- 前 20 个反向因子 ---\n")
    for i, r in enumerate(to_register[:20]):
        ic_rev = f"{r['ic_reversed']:.4f}" if r["ic_reversed"] else "-"
        print(f"  {r['factor_name']} (原IC={r['ic_original']:.4f} → 反转IC={ic_rev}) [{r.get('category', '-')}]")

    if len(to_register) > 20:
        print(f"  ... 另有 {len(to_register)-20} 个")

    # 4. 执行
    if apply_changes:
        print(f"\n=== 执行注册: {len(to_register)} 个反向因子 ===")
        registered = 0
        for r in to_register:
            cur.execute("""
                INSERT INTO aistock_factor_catalog (
                    factor_name, source, catalog_version, generated_at_utc,
                    catalog_source, code_text, is_available
                )
                VALUES (%s, 'manual', 'neg_v1', %s, 'reverse_factor', %s, true)
                ON CONFLICT (factor_name, source) DO UPDATE SET
                    catalog_version = EXCLUDED.catalog_version,
                    generated_at_utc = EXCLUDED.generated_at_utc,
                    code_text = EXCLUDED.code_text,
                    is_available = true
                RETURNING id
            """, (r["factor_name"], datetime.utcnow().isoformat(), r["code_text"]))
            if cur.fetchone():
                registered += 1
        print(f"成功注册: {registered} 个反向因子")
        print(f"\n注意: 需要运行独立指标计算:")
        print(f"  python scripts/register_manual_factor.py compute-metrics --all")
    else:
        print("\n[DRY-RUN] 添加 --apply 参数以执行")

    cur.close()
    conn.close()


def _inject_negation(code: str, neg_name: str, orig_name: str) -> str | None:
    """在因子代码中注入取反操作，同时替换因子名。

    支持两种代码风格:
    - 手工模板: result = factor.to_frame(FACTOR_NAME) → result.to_hdf(...)
    - RDAgent 模板: result_df["factor_name"] = series → result_df.to_hdf("result.h5", ...)
    """
    # 替换因子名（字符串字面量中的原名 → 新名）
    new_code = code.replace(f'"{orig_name}"', f'"{neg_name}"')
    new_code = new_code.replace(f"'{orig_name}'", f"'{neg_name}'")
    # 替换 FACTOR_NAME 变量赋值
    new_code = re.sub(
        r'FACTOR_NAME\s*=\s*["\']' + re.escape(orig_name) + r'["\']',
        f'FACTOR_NAME = "{neg_name}"',
        new_code,
    )

    # === 模式 1: 手工模板 — result = factor.to_frame(...) ===
    pattern1 = r'(\n)([ \t]*)(result\s*=\s*factor\.to_frame)'
    match1 = re.search(pattern1, new_code)
    if match1:
        indent = match1.group(2)
        new_code = new_code[:match1.start()] + \
            f"\n{indent}# Reverse factor (negate)\n{indent}factor = -factor\n{indent}" + \
            match1.group(3) + new_code[match1.end():]
        return new_code

    # === 模式 2: RDAgent 模板 — result_df.to_hdf("result.h5", ...) ===
    pattern2 = r'(\n)([ \t]*)(result_df\.to_hdf\()'
    match2 = re.search(pattern2, new_code)
    if match2:
        indent = match2.group(2)
        new_code = new_code[:match2.start()] + \
            f"\n{indent}# Reverse factor (negate)\n{indent}result_df = -result_df\n{indent}" + \
            match2.group(3) + new_code[match2.end():]
        return new_code

    # === 模式 3: result.to_hdf(...) ===
    pattern3 = r'(\n)([ \t]*)(result\.to_hdf\()'
    match3 = re.search(pattern3, new_code)
    if match3:
        indent = match3.group(2)
        new_code = new_code[:match3.start()] + \
            f"\n{indent}# Reverse factor (negate)\n{indent}result = -result\n{indent}" + \
            match3.group(3) + new_code[match3.end():]
        return new_code

    # === 模式 4: result_df = result_df.sort_index() 之前 ===
    pattern4 = r'(\n)([ \t]*)(result_df\s*=\s*result_df\.sort_index\(\))'
    match4 = re.search(pattern4, new_code)
    if match4:
        indent = match4.group(2)
        new_code = new_code[:match4.start()] + \
            f"\n{indent}# Reverse factor (negate)\n{indent}result_df = -result_df\n{indent}" + \
            match4.group(3) + new_code[match4.end():]
        return new_code

    return None


# ======================================================================
# REPORT — 优化前后对比
# ======================================================================
def cmd_report(args):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("=== 因子库优化报告 ===\n")

    # 总数
    cur.execute("SELECT COUNT(*) as total FROM aistock_factor_catalog")
    total = cur.fetchone()["total"]

    cur.execute("SELECT COUNT(*) as cnt FROM aistock_factor_catalog WHERE is_available = true")
    available = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) as cnt FROM aistock_factor_catalog WHERE is_available = false")
    disabled = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) as cnt FROM aistock_factor_catalog WHERE factor_name LIKE 'neg\\_%'")
    neg_count = cur.fetchone()["cnt"]

    print(f"因子总数: {total}")
    print(f"可用因子: {available}")
    print(f"已禁用因子: {disabled}")
    print(f"反向因子: {neg_count}")
    print()

    # 可用因子 IC 分布
    cur.execute("""
        SELECT m.ic_mean, m.icir, m.top_sharpe, cl.category
        FROM aistock_factor_catalog c
        JOIN aistock_factor_metrics m ON c.factor_name = m.factor_name AND m.eval_window = 'full'
        LEFT JOIN qe_factor_classification cl ON c.factor_name = cl.factor_name
        WHERE c.is_available = true
    """)
    metrics = cur.fetchall()

    ic_values = [r["ic_mean"] for r in metrics if r["ic_mean"] is not None]
    if ic_values:
        pos_ic = sum(1 for v in ic_values if v > 0)
        ic_gt02 = sum(1 for v in ic_values if v > 0.02)
        ic_gt03 = sum(1 for v in ic_values if v > 0.03)
        import statistics
        print("--- 可用因子 IC 分布 ---")
        print(f"  有指标的因子: {len(ic_values)}")
        print(f"  IC 均值: {statistics.mean(ic_values):.4f}")
        print(f"  IC 中位数: {statistics.median(ic_values):.4f}")
        print(f"  正 IC 因子: {pos_ic} ({pos_ic*100//len(ic_values)}%)")
        print(f"  IC > 0.02: {ic_gt02} ({ic_gt02*100//len(ic_values)}%)")
        print(f"  IC > 0.03: {ic_gt03} ({ic_gt03*100//len(ic_values)}%)")
        print()

    # 按类别分布
    cat_dist = defaultdict(int)
    for r in metrics:
        cat = r.get("category") or "未分类"
        cat_dist[cat] += 1

    CATEGORY_LABELS = {
        "MOM": "动量", "VOL": "波动率", "LIQ": "流动性", "VAL": "价值",
        "QUAL": "质量", "CORR": "相关性", "TECH": "技术", "SIZE": "规模",
        "STAT": "统计", "MF": "资金流", "CHIP": "筹码", "ML": "机器学习",
    }

    print("--- 可用因子类别分布 ---")
    for cat in sorted(CATEGORY_LABELS.keys()):
        cnt = cat_dist.get(cat, 0)
        label = CATEGORY_LABELS[cat]
        print(f"  {label} ({cat}): {cnt}")
    print()

    # 按 source 分布
    cur.execute("""
        SELECT source, COUNT(*) as cnt
        FROM aistock_factor_catalog
        WHERE is_available = true
        GROUP BY source
        ORDER BY cnt DESC
    """)
    print("--- 可用因子来源分布 ---")
    for row in cur.fetchall():
        print(f"  {row['source']}: {row['cnt']}")
    print()

    # 被禁用因子来源分布
    if disabled > 0:
        cur.execute("""
            SELECT source, COUNT(*) as cnt
            FROM aistock_factor_catalog
            WHERE is_available = false
            GROUP BY source
            ORDER BY cnt DESC
        """)
        print("--- 已禁用因子来源分布 ---")
        for row in cur.fetchall():
            print(f"  {row['source']}: {row['cnt']}")
        print()

    cur.close()
    conn.close()


# ======================================================================
# Main
# ======================================================================
def main():
    parser = argparse.ArgumentParser(
        description="P0 因子库优化: 去重 + 反向因子注册",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # dedup
    p_dedup = sub.add_parser("dedup", help="基于相关性矩阵去重")
    p_dedup.add_argument("--threshold", type=float, default=0.95,
                         help="相关性阈值 (默认 0.95)")
    p_dedup.add_argument("--apply", action="store_true",
                         help="执行去重 (默认 dry-run)")
    p_dedup.add_argument("--log", type=str, default=None,
                         help="操作日志保存路径")

    # reverse
    p_rev = sub.add_parser("reverse", help="批量注册反向因子")
    p_rev.add_argument("--min-ic", type=float, default=0.02,
                        help="最小 |IC| 阈值 (默认 0.02)")
    p_rev.add_argument("--apply", action="store_true",
                        help="执行注册 (默认 dry-run)")

    # report
    sub.add_parser("report", help="输出优化前后对比")

    args = parser.parse_args()

    if args.command == "dedup":
        cmd_dedup(args)
    elif args.command == "reverse":
        cmd_reverse(args)
    elif args.command == "report":
        cmd_report(args)


if __name__ == "__main__":
    main()
