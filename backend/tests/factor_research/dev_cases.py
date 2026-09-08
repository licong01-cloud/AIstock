"""Two P1 cases: existing data read-only, research records written only to DEV."""
from __future__ import annotations

import argparse
import sys
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from backend.services.factor_research.models import encode  # noqa: E402
from backend.services.factor_research.repository import ResearchRepository  # noqa: E402
from backend.services.factor_research.runner import CANONICAL_UNIVERSE, write_json  # noqa: E402
from backend.services.factor_research.service import ResearchService  # noqa: E402
from scripts.factor_research import configure  # noqa: E402


def main():
    import psycopg2
    from dotenv import dotenv_values

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--resume-case-a", help="Previously completed DEV case A task; do not recompute")
    args = parser.parse_args()
    dev_target = configure(args.env_file, "dev")
    if dev_target["database"] != "aistock_dev":
        raise ValueError("This DEV case must target the existing aistock_dev database")
    repo = ResearchRepository()
    env = dotenv_values(args.env_file)
    prod = {key: env["TDX_DB_" + field] for key, field in (
        ("host", "HOST"), ("port", "PORT"), ("dbname", "NAME"), ("user", "USER"), ("password", "PASSWORD"))}

    @contextmanager
    def production_readonly(**kwargs):
        conn = psycopg2.connect(**prod, connect_timeout=5, options="-c default_transaction_read_only=on -c statement_timeout=60000")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    source_repo = ResearchRepository(production_readonly)
    # DEV has no suspend_d rows for this period. Select an explicitly audited
    # non-suspended functional sample; never claim this tests suspended stocks.
    with repo.cursor() as cur:
        cur.execute("""SELECT DISTINCT ts_code FROM market.stock_universe_pit_spans
            WHERE universe_key=%s AND eligible_start <= %s AND eligible_end >= %s
            ORDER BY ts_code LIMIT 200""", (CANONICAL_UNIVERSE, "2025-01-01", "2026-03-31"))
        options = [row["ts_code"] for row in cur.fetchall()]
    with source_repo.cursor() as cur:
        cur.execute("""SELECT DISTINCT p.ts_code FROM market.stock_universe_pit_spans p
            WHERE p.universe_key=%s AND p.ts_code=ANY(%s) AND p.eligible_start <= %s AND p.eligible_end >= %s
            AND NOT EXISTS (SELECT 1 FROM market.suspend_d s WHERE s.ts_code=p.ts_code
                AND s.trade_date BETWEEN %s AND %s AND s.suspend_type='S')
            ORDER BY p.ts_code LIMIT 40""",
                    (CANONICAL_UNIVERSE, options, "2025-01-01", "2026-03-31", "2025-01-01", "2026-03-31"))
        names = [row["ts_code"] for row in cur.fetchall()]
        cur.execute("""SELECT factor_name FROM aistock_factor_metrics
            WHERE calc_engine='qe_eval_v2' AND factor_name LIKE 'm_%'
            ORDER BY calculated_at DESC,id DESC LIMIT 1""")
        row = cur.fetchone()
        if not row:
            raise ValueError("No actual factor metrics for the diagnosis case")
        factor_name = row["factor_name"]
    if len(names) != 40:
        raise ValueError("The explicitly audited functional sample is not available")
    if args.resume_case_a:
        task_id = args.resume_case_a
        existing = repo.show(task_id)
        if existing["task"]["status"] != "completed" or existing["task"]["factor_names"] != ["m_p1_trend_pullback_case"]:
            raise ValueError("Resume requires a completed case A task")
        starts = [r for r in existing["records"] if r["record_type"] == "attempt"]
        if len(starts) != 1:
            raise ValueError("Resume requires the exact case A attempt")
        original = starts[0]["payload_json"]["execution"]["spec"]
        if original["instruments"] != names or Path(original["artifact_root"]) != args.artifact_root.resolve():
            raise ValueError("Resume case A inputs differ")
    else:
        task_id = str(uuid4())
        repo.create({"task_id": task_id, "record_id": str(uuid4()), "summary": "P1真实数据功能案例，非alpha结论",
                     "task": {"title": "P1趋势内回撤功能案例", "objective": "验证候选执行、官方公式复用和记录恢复",
                              "task_type": "new_factor", "factor_names": ["m_p1_trend_pullback_case"],
                              "context_json": {"method_version": "1.0", "purpose": "functional_validation",
                                               "sample": "40 audited non-suspended symbols, not full-universe validity"}}})
        spec = {"task_id": task_id, "record_id": str(uuid4()), "attempt_id": str(uuid4()), "expected_revision": 1,
                "method_version": "1.0", "universe_key": CANONICAL_UNIVERSE,
                "data_dir": str(args.candidate_root / "components" / "factor_h5_static_candidate_v2"),
                "qlib_bin_path": str(args.candidate_root / "components" / "daily_bin_candidate"),
                "artifact_root": str(args.artifact_root), "read_start": "2025-01-01", "signal_start": "2025-04-01",
                "signal_end": "2026-02-27", "read_end": "2026-03-31", "cutoff": "2026-08-31", "instruments": names,
                "candidates": [{"factor_name": "m_p1_trend_pullback_case",
                                "script": str(Path(__file__).parent / "fixtures" / "trend_pullback.py")}]}
        computed = ResearchService(repo, target=dev_target).run(spec)
        repo.record({"task_id": task_id, "record_id": str(uuid4()), "expected_revision": computed["revision"],
                     "record_type": "decision", "summary": "功能案例完成，不推广为有效因子",
                     "task_update": {"status": "completed", "completed_summary": "40股真实数据执行和研究结果读回",
                                     "next_action": "后续独立研究，不入生产catalog"}})
    source_target = {"target": "production_readonly", "host": prod["host"], "port": prod["port"], "database": prod["dbname"]}
    context = ResearchService(source_repo, target=source_target).context(
        [factor_name], start_date="2026-06-01", end_date="2026-09-08", limit=20)
    if not context["availability"]["metrics"]:
        raise ValueError("No real metrics in the diagnostic date range")
    diagnosis_id = str(uuid4())
    repo.create({"task_id": diagnosis_id, "record_id": str(uuid4()), "summary": "存量因子诊断案例",
                 "task": {"title": "P1存量因子诊断", "objective": "用实际已有指标恢复研究依据，不自动处置",
                          "task_type": "diagnosis", "factor_names": [factor_name],
                          "context_json": {"method_version": "1.0", "purpose": "diagnosis"}}})
    diagnosis = repo.record({"task_id": diagnosis_id, "record_id": str(uuid4()), "expected_revision": 1,
                             "record_type": "result", "summary": "已读实际官方指标；增量与消费者引用仍需确认",
                             "payload": {"existing_metrics": context},
                             "task_update": {"next_action": "比较同口径窗口与原用途；不根据单项IC禁用"}})
    receipt = {"case_a_task": task_id, "case_b_task": diagnosis_id, "sample_stocks": len(names),
               "case_a_revision": repo.show(task_id)["task"]["revision"], "case_b_revision": diagnosis["revision"],
               "case_b_factor": factor_name, "candidate_activation": False, "production_writes": False,
               "limitation": "A is an audited non-suspended functional sample; B is diagnostic context, not retirement approval"}
    path = args.artifact_root / f"cases-{uuid4()}.json"
    write_json(path, receipt)
    print(encode({**receipt, "receipt": str(path)}))


if __name__ == "__main__":
    main()
