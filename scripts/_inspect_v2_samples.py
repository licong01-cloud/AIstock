"""检查已填充 v2 字段的 3 个因子的分类+评级+IC原始数据."""
import os
import sys
from dotenv import load_dotenv
load_dotenv(r"F:\Dev\AIstock\.env")

import psycopg2
import psycopg2.extras
import json

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"],
    port=int(os.environ.get("TDX_DB_PORT", "5432")),
    user=os.environ["TDX_DB_USER"],
    password=os.environ["TDX_DB_PASSWORD"],
    dbname=os.environ["TDX_DB_NAME"],
)

with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    # 1. 找到已填充 v2 字段的因子
    cur.execute("""
        SELECT id, factor_name, factor_source, factor_catalog_id,
               category, factor_dimension, holding_period_class, data_source_group,
               linearity, ts_info_density, update_freq,
               direction, signal_mechanism, sector_exposure_corr,
               best_horizon, best_horizon_advantage, horizon_class,
               ic_sign_consistency_12m, ic_oos_is_ratio, monthly_ic_trend_slope,
               cross_horizon_consistency,
               cluster_id, cluster_role, cluster_size, intra_cluster_max_corr, representative_score,
               description, classification_reason, llm_analysis,
               analyzed_at, analyzed_by
        FROM qe_factor_classification
        WHERE direction IS NOT NULL
           OR signal_mechanism IS NOT NULL
           OR best_horizon IS NOT NULL
           OR horizon_class IS NOT NULL
        ORDER BY analyzed_at DESC NULLS LAST
        LIMIT 10
    """)
    rows = cur.fetchall()
    print(f"=== 已填充 v2 字段的因子: {len(rows)} 个 ===\n")
    catalog_ids = []
    factor_keys = []  # (factor_name, factor_source)
    for r in rows:
        catalog_ids.append(r['factor_catalog_id'])
        factor_keys.append((r['factor_name'], r['factor_source']))
        print("=" * 90)
        print(f"id={r['id']} catalog_id={r['factor_catalog_id']} name={r['factor_name']} source={r['factor_source']}")
        print(f"\n[分类维度 v1]")
        print(f"  category              = {r['category']}")
        print(f"  factor_dimension      = {r['factor_dimension']}")
        print(f"  holding_period_class  = {r['holding_period_class']}")
        print(f"  data_source_group     = {r['data_source_group']}")
        print(f"  linearity             = {r['linearity']}")
        print(f"  ts_info_density       = {r['ts_info_density']}")
        print(f"  update_freq           = {r['update_freq']}")
        print(f"\n[分类维度 v2]")
        print(f"  direction             = {r['direction']}   (1=long, -1=short, 0=bidirectional)")
        print(f"  signal_mechanism      = {r['signal_mechanism']}")
        print(f"  sector_exposure_corr  = {r['sector_exposure_corr']}")
        print(f"  best_horizon          = {r['best_horizon']} (天)")
        print(f"  best_horizon_adv      = {r['best_horizon_advantage']}")
        print(f"  horizon_class         = {r['horizon_class']}")
        print(f"  ic_sign_consistency_12m   = {r['ic_sign_consistency_12m']}")
        print(f"  ic_oos_is_ratio           = {r['ic_oos_is_ratio']}")
        print(f"  monthly_ic_trend_slope    = {r['monthly_ic_trend_slope']}")
        print(f"  cross_horizon_consistency = {r['cross_horizon_consistency']}")
        print(f"  cluster_id            = {r['cluster_id']}  role={r['cluster_role']} size={r['cluster_size']}")
        print(f"  intra_cluster_max_corr= {r['intra_cluster_max_corr']}")
        print(f"  representative_score  = {r['representative_score']}")
        print(f"\n[描述]")
        print(f"  description           = {(r['description'] or '')[:300]}")
        print(f"  classification_reason = {(r['classification_reason'] or '')[:300]}")
        print(f"  llm_analysis          = {(r['llm_analysis'] or '')[:300]}")
        print(f"\n[元数据]")
        print(f"  analyzed_at           = {r['analyzed_at']}")
        print(f"  analyzed_by           = {r['analyzed_by']}")
        print()

    if not catalog_ids:
        print("无数据")
        sys.exit(0)

    # 2. 官方评级
    print("\n\n" + "=" * 90)
    print("=== 官方评级 (qe_factor_official_ratings) ===")
    print("=" * 90 + "\n")
    cur.execute("""
        SELECT r.*
        FROM qe_factor_official_ratings r
        WHERE r.factor_catalog_id = ANY(%s)
        ORDER BY r.factor_catalog_id, r.graded_at DESC
    """, (catalog_ids,))
    for r in cur.fetchall():
        print("-" * 90)
        print(f"catalog_id={r['factor_catalog_id']}  rule={r['rule_version']}  run_id={r['run_id']}")
        print(f"  grade / score     = {r['official_grade']} / {r['official_score']}")
        print(f"  snapshot_date     = {r['snapshot_date']}")
        print(f"  graded_at         = {r['graded_at']}")
        for k in ['dimension_scores', 'hard_gate_flags', 'grade_reason_structured',
                  'metrics_snapshot', 'llm_risk_notes']:
            v = r[k]
            if v is None:
                continue
            try:
                j = v if isinstance(v, (dict, list)) else json.loads(v)
                s = json.dumps(j, ensure_ascii=False, default=str)
                print(f"  {k:25s} = {s[:600]}")
            except Exception:
                print(f"  {k:25s} = {str(v)[:600]}")
        if r['llm_audit_summary']:
            print(f"  llm_audit_summary = {r['llm_audit_summary'][:600]}")

    # 3. 原始 IC 指标 (验证 direction 符号是否正确)
    print("\n\n" + "=" * 90)
    print("=== 原始 IC / 指标 (qe_factor_experiment_metrics) — 验证 direction 符号 ===")
    print("=" * 90 + "\n")
    cur.execute("""
        SELECT factor_name, factor_source, experiment_id, experiment_name,
               ic, icir, rank_ic, rank_icir,
               ann_return_no_cost, info_ratio_no_cost, sharpe_ratio,
               avg_turnover, data_split
        FROM qe_factor_experiment_metrics
        WHERE factor_catalog_id = ANY(%s)
        ORDER BY factor_catalog_id, collected_at DESC
        LIMIT 30
    """, (catalog_ids,))
    for r in cur.fetchall():
        print("-" * 90)
        print(f"{r['factor_name']} / {r['factor_source']}  exp={r['experiment_id']} ({r['experiment_name']})")
        print(f"  IC={r['ic']}  ICIR={r['icir']}  RankIC={r['rank_ic']}  RankICIR={r['rank_icir']}")
        print(f"  ann_ret={r['ann_return_no_cost']}  IR={r['info_ratio_no_cost']}  Sharpe={r['sharpe_ratio']}  Turnover={r['avg_turnover']}")
        if r['data_split']:
            ds = r['data_split'] if isinstance(r['data_split'], dict) else json.loads(r['data_split'])
            print(f"  data_split = {json.dumps(ds, ensure_ascii=False, default=str)[:200]}")

conn.close()
