"""Read existing small evaluation summaries, never compute or update official assets."""
from __future__ import annotations

from .quality import diagnose, window


def quality_report(repository, *, as_of, history_start, recent_start, names=None):
    window(as_of, history_start, recent_start)
    with repository.cursor() as cur:
        # One normal DB transaction gives coherent catalog/summary reads. No data freeze/artifact.
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cur.execute("""SELECT id,factor_name,source,is_available,expression,description_cn,
            experiment_id,best_loop_task_run_id,source_task_id
            FROM public.aistock_factor_catalog WHERE (%s IS NULL OR factor_name=ANY(%s))
            ORDER BY id""", (names, names))
        catalog = [dict(r) for r in cur.fetchall()]
        selected = sorted({r["factor_name"] for r in catalog})
        ids = [r["id"] for r in catalog]
        cur.execute("""SELECT id,factor_name,factor_catalog_id,eval_window,calc_engine,calculated_at,
            data_start,data_end,return_horizon,universe,universe_rule_version,universe_fingerprint_sha256,
            index_policy,calc_batch_id,snapshot_date,direction,coverage_semantics,coverage,
            coverage_numerator,coverage_denominator,n_trading_days,ic_mean,rank_ic_mean,icir,
            h20_ic_mean,h20_rank_ic_mean,h20_n_obs,h20_return_horizon
            FROM public.aistock_factor_metrics
            WHERE factor_name=ANY(%s) AND snapshot_date<=%s ORDER BY id""", (selected, as_of))
        metrics = [dict(r) for r in cur.fetchall()]
        cur.execute("""SELECT id,factor_name,month_end,snapshot_date,ic_mean,rank_ic_mean,n_days
            FROM public.aistock_factor_monthly_ic
            WHERE factor_name=ANY(%s) AND snapshot_date<=%s ORDER BY id""", (selected, as_of))
        monthly = [dict(r) for r in cur.fetchall()]
        cur.execute("""SELECT id,factor_a_id,factor_b_id,correlation,method,as_of_date,data_window_days,
            universe,universe_rule_version,universe_fingerprint_sha256,index_policy
            FROM public.qe_factor_correlations
            WHERE (factor_a_id=ANY(%s) OR factor_b_id=ANY(%s)) AND as_of_date<=%s ORDER BY id""",
                    (ids, ids, as_of))
        correlations = [dict(r) for r in cur.fetchall()]
    report = diagnose(catalog, metrics, monthly, correlations, as_of=as_of,
                      history_start=history_start, recent_start=recent_start)
    report["requested_names_missing"] = sorted(set(names or ()) - set(selected))
    return report
