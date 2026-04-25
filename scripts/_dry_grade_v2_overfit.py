"""Dry-grade a sample of factors with v2 rules, focus on overfit_gate effects."""
import os, sys
from pathlib import Path

REPO_ROOT = Path(r"F:/Dev/AIstock")
for line in (REPO_ROOT / ".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

sys.path.insert(0, str(REPO_ROOT))

import psycopg2
from backend.services.quantevolver.factor_rating_service import factor_rating_service

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ["TDX_DB_PORT"]),
    dbname=os.environ["TDX_DB_NAME"], user=os.environ["TDX_DB_USER"],
    password=os.environ["TDX_DB_PASSWORD"],
)

# Pick: 3 overfit (oos<0.1), 3 high oos (>=0.5), 3 mid (0.2-0.4)
with conn.cursor() as cur:
    cur.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (factor_name) factor_name, oos_is_ratio
            FROM aistock_factor_monthly_ic
            ORDER BY factor_name, month_end DESC
        )
        (SELECT factor_name, oos_is_ratio, 'overfit' AS bucket FROM latest
         WHERE oos_is_ratio < 0.1 ORDER BY oos_is_ratio LIMIT 3)
        UNION ALL
        (SELECT factor_name, oos_is_ratio, 'strong' FROM latest
         WHERE oos_is_ratio >= 0.5 ORDER BY oos_is_ratio DESC LIMIT 3)
        UNION ALL
        (SELECT factor_name, oos_is_ratio, 'mid' FROM latest
         WHERE oos_is_ratio BETWEEN 0.2 AND 0.4 LIMIT 3)
    """)
    sample = cur.fetchall()

# Fetch full factor rows (metrics + catalog)
# Match columns the v1 `_grade_factor` expects: factor_catalog_id, factor_name,
# factor_source, and all aistock_factor_metrics columns
with conn.cursor() as cur:
    cur.execute("""
        SELECT c.id, c.factor_name, c.source,
               m.ic_mean, m.rank_ic_mean, m.icir, m.rank_icir, m.icir_annualized,
               m.rank_icir_annualized, m.rank_ic_1d, m.rank_ic_5d, m.rank_ic_10d,
               m.rank_ic_20d, m.ic_positive_ratio, m.ic_decay_half_life,
               m.top_excess_sharpe, m.top_excess_annual_return,
               m.group_return_monotonicity, m.turnover, m.coverage,
               m.n_trading_days, m.top_annual_return, m.top_max_drawdown,
               m.benchmark_annual_return, m.direction, m.best_horizon,
               m.best_horizon_advantage
        FROM aistock_factor_catalog c
        LEFT JOIN aistock_factor_metrics m ON m.factor_catalog_id = c.id
        WHERE c.factor_name = ANY(%s)
    """, ([s[0] for s in sample],))
    col_names = [d[0] for d in cur.description]
    rows = [dict(zip(col_names, r)) for r in cur.fetchall()]

by_name = {r["factor_name"]: r for r in rows}

# Load v2 rule
detail = factor_rating_service.get_rule_detail("v2.0.0")
rule = {
    "rule_version": "v2.0.0",
    "spec": detail["spec"],
    "grade_bands": detail["grade_bands"],
}

print(f"{'bucket':8s} {'factor':40s}{'oos':>7s}  {'grade':>5s}  {'score':>7s}  failed_gates")
print("-" * 120)
for fname, oos, bucket in sample:
    factor = by_name.get(fname)
    if not factor:
        print(f"{bucket:8s} {fname[:39]:40s} (no metrics)")
        continue
    result = factor_rating_service._grade_factor(factor, rule)
    reason = result.get("grade_reason_structured") or {}
    failed = reason.get("failed_gates") or []
    overfit_d = reason.get("overfit_force_d")
    dedup = reason.get("dedup_suppressed")
    flags = list(failed)
    if overfit_d:
        flags.insert(0, "OVERFIT→D")
    if dedup:
        flags.insert(0, "DEDUP→≤C")
    oos_show = oos if oos is not None else 0
    g = result.get("official_grade", "?")
    s = result.get("official_score") or 0
    print(f"{bucket:8s} {fname[:39]:40s}{oos_show:>7.3f}  {g:>5s}  {s:>7.2f}  {flags}")

conn.close()
