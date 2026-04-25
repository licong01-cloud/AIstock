"""直接跑 10 个因子的完整 Pipeline (Step A + Step B), 验证修复后分类与评级.

不经过 HTTP API — 直接调 FactorAnalyst + factor_rating_service.
"""
import os
import sys
import uuid
import json
import time
from dotenv import load_dotenv

load_dotenv(r"F:\Dev\AIstock\.env")
sys.path.insert(0, r"F:\Dev\AIstock")

import logging
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

from backend.services.quantevolver.factor_analyst import FactorAnalyst
from backend.services.quantevolver.factor_rating_service import factor_rating_service as svc

TEST_FACTORS = [
    # (factor_name, source, note)
    ("Value_PBInv_Momentum_20D", "rdagent_task_sync", "旧错判 LIQ / 名字+价值+动量复合"),
    ("dynamic_flow_volatility_sentiment", "rdagent_task_sync", "旧错判 LIQ / 代码含 mf_ + db_turnover + db_pb 复合"),
    ("industry_stock_momentum_diff_10d", "rdagent_task_sync", "期望保持 MOM"),
    ("dynamic_pe_inv_elg_net_zscore", "rdagent_task_sync", "名字含 pe_inv → 期望 VAL/复合"),
    ("MF_MainNetAmtRatio_5D_Mom", "rdagent_task_sync", "期望 MF"),
    ("cost_pressure_winner_rate", "rdagent_task_sync", "期望 CHIP"),
    ("Large_Cap_Momentum_Bias", "rdagent_task_sync", "期望 SIZE 或 MOM"),
    ("roe_stability_score", "rdagent_task_sync", "旧错判 LIQ / 新 QUAL 规则后期望 QUAL"),
    ("Volatility_Adjusted_Turnover", "rdagent_task_sync", "期望 LIQ/VOL 复合"),
    ("Price_Volume_Convergence_10D", "rdagent_task_sync", "期望 VOL/CORR"),
    # 新增 10 个
    ("SW2_MOM5", "rdagent_task_sync", "期望 MOM (行业动量)"),
    ("dynamic_pe_momentum_factor", "rdagent_task_sync", "期望 VAL+MOM 复合"),
    ("dividend_flow_risk_adjusted_v2", "rdagent_task_sync", "期望 VAL (股息) 或复合"),
    ("VolumePriceDivergence", "rdagent_task_sync", "期望 VOL/CORR"),
    ("chip_concentration_index", "rdagent_task_sync", "期望 CHIP"),
    ("vol_adj_momentum", "rdagent_task_sync", "期望 MOM 或 VOL 复合"),
    ("IndustryMomentumChipAvgCostRatio", "rdagent_task_sync", "期望 MOM+CHIP 复合"),
    ("quality_capex_efficiency", "rdagent_task_sync", "期望 QUAL (验证新规则)"),
    ("GrossProfitMargin_Momentum", "rdagent_task_sync", "期望 QUAL+MOM 复合 (验证新规则)"),
    ("retail_sentiment", "rdagent_task_sync", "期望 MF 或 CHIP"),
    # 第 3 批 5 个 — 验证名字↔数据列冲突降级
    ("quality_value_nonlinear", "rdagent_task_sync", "名字{QUAL,VAL} — 期望 LLM 决定主类"),
    ("value_momentum_reversal", "rdagent_task_sync", "名字{VAL,MOM} — 期望 LLM 或名字兜底"),
    ("RetailOutflowSizeRatio", "rdagent_task_sync", "名字{MF,SIZE} — 期望 MF 或复合"),
    ("high_amount_turnover_momentum_5d", "rdagent_task_sync", "名字{LIQ,MOM} — 名字含 LIQ 且数据列 LIQ，应稳定 LIQ"),
    ("gross_margin_historical_high_distance", "rdagent_task_sync", "名字{QUAL} — 若用 bb_*ge/gross_margin 应为 QUAL"),
]

def main():
    # 1. 加载规则
    svc.sync_rule_versions()
    rules = svc.list_rule_versions()
    rule_version = rules.get("active_version") or rules.get("default_version")
    print(f"rule_version = {rule_version}")
    rule = svc.get_rule_detail(rule_version)
    assert rule, f"rule {rule_version} 读取失败"

    # 2. 解析 factor_catalog_id
    scope = svc._resolve_selected_factors(
        [{"factor_name": n, "source": s} for (n, s, _) in TEST_FACTORS]
    )
    print(f"resolved {len(scope)} / {len(TEST_FACTORS)} factors")
    for f in scope:
        print(f"  id={f['id']:>6}  {f['factor_name']}  ({f['source']})")

    resolved_map = {(f["factor_name"], f["source"]): f for f in scope}

    # 3. 新建 rating_run
    run_id = str(uuid.uuid4())
    svc._insert_run(run_id, rule_version, "selected",
                    {"selected_factors": [{"factor_name": n, "source": s} for (n, s, _) in TEST_FACTORS]},
                    "test_script")
    print(f"run_id = {run_id}")

    fa = FactorAnalyst()
    summary_rows = []
    ok_count = 0
    failed_count = 0
    t_start = time.time()

    for idx, (name, source, note) in enumerate(TEST_FACTORS, 1):
        key = (name, source)
        if key not in resolved_map:
            print(f"\n[{idx}/{len(TEST_FACTORS)}] MISSING {name} ({source})")
            continue
        factor = resolved_map[key]
        print(f"\n[{idx}/{len(TEST_FACTORS)}] === {name} === ({note})")

        # Step A
        t0 = time.time()
        try:
            result_a = fa.analyze_single_factor(name, source, use_llm=True)
        except Exception as e:
            print(f"  STEP A EXCEPTION: {e}")
            failed_count += 1
            summary_rows.append((name, "ERROR-A", str(e)[:60], "-", "-", "-"))
            continue
        if not result_a.get("ok"):
            print(f"  STEP A FAILED: {result_a.get('error')}")
            failed_count += 1
            summary_rows.append((name, "ERROR-A", result_a.get("error", "")[:60], "-", "-", "-"))
            continue
        cat = result_a.get("category")
        reason = result_a.get("classification_reason", "")
        print(f"  STEP A ok ({time.time()-t0:.1f}s) category={cat}  reason={reason[:100]}")

        # Step B
        t1 = time.time()
        try:
            grade_result = svc._grade_factor_v2(factor, rule, enable_llm_audit=True)
            svc._upsert_official_rating(run_id, rule_version, factor["id"], grade_result)
        except Exception as e:
            print(f"  STEP B EXCEPTION: {e}")
            failed_count += 1
            summary_rows.append((name, cat, reason[:60], "ERROR-B", "-", str(e)[:60]))
            continue
        grade = grade_result.get("official_grade")
        score = grade_result.get("official_score")
        gates = grade_result.get("hard_gate_flags") or {}
        failed_gates = [k for k, v in gates.items() if v is False]
        print(f"  STEP B ok ({time.time()-t1:.1f}s) grade={grade}  score={score}  failed_gates={failed_gates}")
        ok_count += 1
        summary_rows.append((name, cat, reason[:50], grade, f"{score:.2f}" if score else "-",
                             ",".join(failed_gates)[:40]))

    elapsed = time.time() - t_start
    svc._finish_run(run_id, "completed" if failed_count == 0 else "failed",
                    {"total_factors": len(TEST_FACTORS), "success_count": ok_count, "failed_count": failed_count},
                    None)

    # 总结
    print("\n" + "=" * 110)
    print(f"SUMMARY: {ok_count}/{len(TEST_FACTORS)} ok  elapsed={elapsed:.1f}s  run_id={run_id}")
    print("=" * 110)
    print(f"{'FACTOR':<42} {'CAT':<6} {'GRADE':<5} {'SCORE':<7} {'FAILED_GATES':<30} REASON")
    print("-" * 110)
    for name, cat, reason, grade, score, gates in summary_rows:
        print(f"{name:<42} {str(cat):<6} {str(grade):<5} {str(score):<7} {gates:<30} {reason}")

if __name__ == "__main__":
    main()
