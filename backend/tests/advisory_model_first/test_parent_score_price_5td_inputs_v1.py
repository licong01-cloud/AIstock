"""Shared synthetic contracts only; no live DB, package admission or research fit."""
from datetime import date
import hashlib
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, POLICY, POLICY_SHA256
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import ParentScoreStudyPlanV1
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import build_inputs, training_encoding, warmup_calendar
from backend.services.advisory_model_first.parent_score_price_5td_pipeline_v1 import implementation_sha256
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def sample_plan(tmp_path):
    # Intentionally sparse synthetic sessions, not an asserted real market calendar.
    calendar = [v.date() for v in pd.bdate_range("2024-06-06", periods=59)]+[date(2025, 5, 30)]
    calendar += [v.date() for v in pd.bdate_range("2025-06-03", periods=19)]+[date(2025, 9, 30)]
    calendar += [v.date() for v in pd.bdate_range("2025-10-01", periods=10)]
    path = tmp_path/"original_calendar.json"
    content = json.dumps([d.isoformat() for d in calendar]).encode()
    path.write_bytes(content)
    sources = [dict(source_id="source"+str(bit), package_id="package"+str(bit), manifest_sha256=str(bit+1)*64,
        run_id="original_run"+str(bit), context_bit=bit, prediction_source=dict(package_id="package"+str(bit),
        manifest_sha256=str(bit+1)*64, run_id="original_run"+str(bit), status="FROZEN_SCORE_AVAILABLE",
        descriptor=dict(sha256="a"*64, size_bytes=100))) for bit in (0, 1)]
    plan = ParentScoreStudyPlanV1(sources=sources, calendar_ref=dict(role="original_calendar", artifact_uri=str(path),
        sha256=hashlib.sha256(content).hexdigest(), size_bytes=len(content)), train_start=calendar[20], train_end=calendar[59],
        evaluation_start=calendar[60], evaluation_end=calendar[79], implementation_sha256=implementation_sha256(),
        source_commit="a"*40, node_python_uri="/fixture/existing/python", qe_api_base="http://127.0.0.1:8001/api/v1",
        library_versions={n: "fixture" for n in ("numpy", "pandas", "scikit-learn", "pyarrow", "pydantic")})
    return plan, calendar


def sample_rows(plan, calendar):
    rows, days = [], []
    for source in plan.sources:
        for pos in range(20, 80):
            d, t, h = (pd.Timestamp(calendar[i]) for i in (pos, pos+1, pos+5))
            for k in range(50):
                symbol = f"{600000+k}.SH"
                row = {n: .01+(k%3)*.001 for n in FEATURES}
                row.update(dict(package_id=source.package_id, manifest_sha256=source.manifest_sha256, run_id=source.run_id,
                    source_id=source.source_id, **dict(zip(KEY, (d, t, symbol), strict=True)),
                    selection_effective_rank=k+1, candidate_group_size=50, parent_score=(pos*50+k)*(100 if source.context_bit else 1),
                    observed_gap_bps=-50. if k%2 else 50., label_information_end=h,
                    label_cluster=sha([d.date().isoformat(), t.date().isoformat(), symbol, VALUATION_POLICY_SHA256]),
                    valuation_status="AVAILABLE", valuation_policy_sha256=VALUATION_POLICY_SHA256,
                    valuation_gross_terminal_ratio=1.03+(k%3)*.001, valuation_path_min_ratio=.98,
                    label_status="AVAILABLE", policy_sha256=POLICY_SHA256,
                    exit_execution_status="NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF"))
                raw = row["valuation_gross_terminal_ratio"]/((1+row["observed_gap_bps"]/10000)*(1+POLICY["buy_bps"]/10000))
                row["hypothetical_liquidation_net_bps"] = 10000*(raw*(1-POLICY["sell_bps"]/10000)-1)
                row["mark_to_market_net_bps"] = 10000*(raw-1)
                if h > pd.Timestamp(plan.evaluation_end):
                    row["valuation_status"], row["label_status"] = "IMMATURE", "IMMATURE"
                    for n in ("valuation_gross_terminal_ratio", "valuation_path_min_ratio", "hypothetical_liquidation_net_bps", "mark_to_market_net_bps"):
                        row[n] = np.nan
                rows.append(row)
            days.append(dict(package_id=source.package_id, decision_date=d, target_date=t, horizon_end=h,
                             original_candidates=50, source_status="KNOWN_ROSTER"))
    return pd.DataFrame(rows), pd.DataFrame(days)


def test_train_only_scale_shared_clusters_and_honest_purge(tmp_path):
    plan, calendar = sample_plan(tmp_path)
    rows, _ = sample_rows(plan, calendar)
    encoding = training_encoding(rows=rows, plan=plan)
    a, b = encoding["adapters"].values()
    assert b["center"] == 100*a["center"] and b["scale"] == 100*a["scale"]
    active = rows.loc[rows.pool.isin(["STRUCTURE", "ESTIMATION"])]
    assert np.allclose(active.groupby("label_cluster").cluster_mass.sum(), 1)
    assert active.groupby("label_cluster").pool.nunique().eq(1).all()
    assert active.loc[active.pool.eq("STRUCTURE"), "label_information_end"].lt(encoding["estimation_first_D"]).all()
    poisoned = rows.copy()
    held = poisoned[KEY[0]].ge(pd.Timestamp(plan.evaluation_start))
    for name in (*FEATURES, "parent_score", "valuation_gross_terminal_ratio", "valuation_path_min_ratio"):
        poisoned[name] = poisoned[name].astype(object)
        poisoned.loc[held, name] = "OPAQUE_FUTURE_VALUE"
    assert training_encoding(rows=poisoned, plan=plan) == encoding
    conflict = rows.copy()
    conflict.loc[0, "valuation_gross_terminal_ratio"] = 1.7
    with pytest.raises(ValueError, match="contradictory"):
        training_encoding(rows=conflict, plan=plan)


def test_source_score_scale_does_not_select_on_future_label_availability(tmp_path):
    plan, calendar = sample_plan(tmp_path)
    rows, _ = sample_rows(plan, calendar)
    baseline = training_encoding(rows=rows, plan=plan)
    modified = rows.copy()
    modified.loc[modified.instrument.eq("600000.SH"), "valuation_status"] = "UNKNOWN"
    observed = training_encoding(rows=modified, plan=plan)
    assert observed["adapters"] == baseline["adapters"] and observed["medians"] == baseline["medians"]
    modified["parent_score"] = 1.
    unavailable = training_encoding(rows=modified, plan=plan)
    assert unavailable["contrast_status"] == "UNKNOWN_CONTRAST_NOT_IDENTIFIABLE"


def test_cold_start_and_normal_suspension_are_retained_not_filled():
    calendar = [d.date() for d in pd.bdate_range("2024-07-04", periods=30)]
    sources = [SimpleNamespace(package_id=f"p{i}", context_bit=i, manifest_sha256=str(i+1)*64, run_id=f"r{i}") for i in (0, 1)]
    plan = SimpleNamespace(sources=sources, train_start=calendar[0], train_end=calendar[19],
                           evaluation_start=calendar[20], evaluation_end=calendar[29])
    roster = pd.DataFrame([dict(package_id=s.package_id, manifest_sha256=s.manifest_sha256, run_id=s.run_id,
        **dict(zip(KEY, (pd.Timestamp(calendar[pos]), pd.Timestamp(calendar[pos+1]), "600000.SH"), strict=True)),
        selection_effective_rank=1, candidate_group_size=1, parent_score=float(pos+1)) for s in sources for pos in (0, 20)])
    daily = pd.DataFrame([dict(trade_date=pd.Timestamp(d), instrument="600000.SH", raw_open_cny=10., raw_high_cny=10.2,
        raw_low_cny=9.8, raw_close_cny=10., adj_factor=1., volume_hand=100., up_limit=11., down_limit=9.,
        suspended=i == 22, tradability_unknown=False, price_placeholder_fields="") for i, d in enumerate(calendar)])
    benchmark = pd.DataFrame(dict(trade_date=pd.to_datetime(calendar), instrument="000300.SH", close=4000.))
    rows, _ = build_inputs(plan=plan, roster=roster, calendar=calendar, daily=daily, index_daily=benchmark)
    assert len(rows) == 4 and rows.loc[rows[KEY[0]].eq(pd.Timestamp(calendar[0])), list(FEATURES)].isna().all().all()
    assert rows.loc[rows[KEY[0]].eq(pd.Timestamp(calendar[20])), "carried_valuation_sessions"].eq(1).all()
    assert rows.actual_fill_proven.eq(False).all()
    missing = daily.loc[daily.trade_date.ne(pd.Timestamp(calendar[23]))]
    unknown, _ = build_inputs(plan=plan, roster=roster, calendar=calendar, daily=missing, index_daily=benchmark)
    assert unknown.loc[unknown[KEY[0]].eq(pd.Timestamp(calendar[20])), "valuation_status"].eq("UNKNOWN").all()


def test_warmup_uses_readonly_transaction_and_preserves_insufficient_real_prefix():
    statements = []
    class Cursor:
        def execute(self, sql, params):
            statements.append((sql, params))
        def fetchall(self):
            return [(date(2024, 7, 3),)]
        def close(self):
            statements.append("closed")
    class Connection:
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            pass
        def cursor(self):
            return Cursor()
        def set_session(self, **kwargs):
            assert kwargs == dict(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        def rollback(self):
            statements.append("rollback")
    original = [date(2024, 7, 4), date(2024, 7, 5)]
    actual, receipt = warmup_calendar(calendar=original, train_start=original[0], connection_context_factory=Connection)
    assert actual == [date(2024, 7, 3), *original] and receipt["status"].startswith("INSUFFICIENT")
    assert original == [date(2024, 7, 4), date(2024, 7, 5)] and statements[-2:] == ["rollback", "closed"]
