"""Complete-list context, source symmetry, unknown preservation and honest temporal projection."""
from copy import deepcopy
from datetime import date
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.cross_package_consensus_price_5td_contracts_v1 import (
    BASE_ORDER, CONTEXT_COLUMNS, FEATURES, KEY, MARK_POLICY_SHA256, PACKAGES, SOURCE_COLUMNS,
)
from backend.services.advisory_model_first.cross_package_consensus_price_5td_inputs_v1 import (
    build_consensus_training_encoding_v1, build_cross_package_rank_context_v1, validate_context_rows_v1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def synthetic_inputs():
    """One reusable synthetic packet, not a research run or proof of historical returns."""
    plan = SimpleNamespace(train_start=date(2024, 7, 4), train_end=date(2025, 5, 30),
        estimation_first_D=date(2024, 12, 24), evaluation_start=date(2025, 6, 3), evaluation_end=date(2025, 9, 30))
    calendar = list(pd.bdate_range("2024-07-04", periods=16))+list(pd.bdate_range("2025-01-02", periods=16))
    calendar += list(pd.bdate_range("2025-06-03", periods=18))+list(pd.bdate_range("2025-09-30", periods=7))
    sources = [dict(package_id=p, manifest_sha256=v[0], run_id=v[1], source_id=v[2], artifact_sha256="a"*64,
        original_source_stage_sha256="b"*64, semantic_clock="D_FROZEN_BACKTEST") for p, v in PACKAGES.items()]
    records, days = [], []
    for source_index, source in enumerate(sources):
        for pos, d in enumerate(calendar[:-1]):
            if not (plan.train_start <= d.date() <= plan.train_end or plan.evaluation_start <= d.date() <= plan.evaluation_end):
                continue
            t, h = calendar[pos+1], calendar[pos+5] if pos+5 < len(calendar) else pd.NaT
            symbols = [f"{600000+n}.SH" for n in range(8)] if source_index == 0 else [f"{600000+n}.SH" for n in range(4, 12)]
            days.append(dict(package_id=source["package_id"], decision_date=d, target_date=t, horizon_end=h,
                original_candidates=8, source_status="KNOWN_ROSTER"))
            for rank, symbol in enumerate(symbols, 1):
                cutoff = plan.train_end if d.date() <= plan.train_end else plan.evaluation_end
                mature = pd.notna(h) and h.date() <= cutoff
                stock = int(symbol[:6])-600000
                row = {k: source[k] for k in ("package_id", "manifest_sha256", "run_id", "source_id")}
                row.update({KEY[0]: d, KEY[1]: t, "instrument": symbol, "selection_effective_rank": rank, "candidate_group_size": 8})
                row.update({n: .02+stock/1000 for n in FEATURES})
                row.update(reference_cny=10., reference_visible_through=d, label_information_end=h,
                    terminal_mark_ratio=1.04+stock/1000 if mature else np.nan,
                    path_mark_min_ratio=.97 if mature else np.nan, mark_label_status="AVAILABLE" if mature else "IMMATURE",
                    mark_label_reason="KNOWN_MARK" if mature else "HORIZON_AFTER_CUTOFF", mark_policy_sha256=MARK_POLICY_SHA256,
                    entry_price_cny=10., observed_gap_bps=0., entry_execution_status="NOMINAL_ENTRY_ELIGIBLE_NOT_FILL_PROOF",
                    endpoint_execution_status="NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF" if mature else "IMMATURE",
                    carried_mark_sessions=0, actual_fill_proven=False, realized_return_bps=None)
                row["label_cluster"] = sha([d.date().isoformat(), t.date().isoformat(), symbol, MARK_POLICY_SHA256])
                records.append(row)
    rows, days = pd.DataFrame(records), pd.DataFrame(days)
    context = build_cross_package_rank_context_v1(roster_metadata=rows, day_metadata=days, calendar=calendar, package_pair=sources)
    rows = rows.merge(context.loc[:, [*KEY, *CONTEXT_COLUMNS]], on=list(KEY), validate="many_to_one", sort=False)
    parent_encoding = dict(matrix_order=list(BASE_ORDER), medians=[.1]*9,
        **{n: getattr(plan, n).isoformat() for n in vars(plan)}, median_population="D_MEASURABLE_MATURE_TRAIN_BEFORE_TARGET_OR_ARM_SELECTION")
    return rows, days, calendar, sources, context, parent_encoding, plan


def test_symmetric_context_preserves_original_keys_and_absence_is_not_missing():
    rows, days, calendar, pair, context, _, _ = synthetic_inputs()
    reverse = build_cross_package_rank_context_v1(roster_metadata=rows, day_metadata=days, calendar=calendar, package_pair=pair[::-1])
    pd.testing.assert_frame_equal(context, reverse)
    zero, shared = context.loc[context.instrument.eq("600000.SH")].iloc[0], context.loc[context.instrument.eq("600004.SH")].iloc[0]
    assert zero.cross_package_consensus_strength == 0 and zero.strongest_rank_strength == 1
    assert shared.cross_package_consensus_strength == .5 and shared.strongest_rank_strength == 1
    assert {m["membership"] for m in zero.per_source_membership} == {"KNOWN_MEMBER", "KNOWN_ABSENT_FROM_COMPLETE_LIST"}
    assert rows.loc[:, SOURCE_COLUMNS].duplicated().sum() == 0
    assert context.capture_timestamp_if_originally_proven.map(lambda v: v == [None, None]).all()


def test_known_single_empty_and_unknown_peer_have_different_semantics():
    rows, days, calendar, pair, _, _, _ = synthetic_inputs()
    d = days.decision_date.iloc[0]
    day = days.loc[days.decision_date.eq(d)].copy()
    one = rows.loc[rows[KEY[0]].eq(d) & rows.package_id.eq(pair[0]["package_id"])].iloc[:1].copy()
    one["candidate_group_size"] = 1
    day["original_candidates"], day["source_status"] = 0, "KNOWN_EMPTY"
    day.loc[day.package_id.eq(pair[0]["package_id"]), ["original_candidates", "source_status"]] = [1, "KNOWN_ROSTER"]
    result = build_cross_package_rank_context_v1(roster_metadata=one, day_metadata=day, calendar=calendar, package_pair=pair)
    assert result.strongest_rank_strength.iloc[0] == 1 and result.cross_package_consensus_strength.iloc[0] == 0
    day.loc[day.package_id.eq(pair[1]["package_id"]), "source_status"] = "UNKNOWN"
    unknown = build_cross_package_rank_context_v1(roster_metadata=one, day_metadata=day, calendar=calendar, package_pair=pair)
    assert unknown.context_availability.tolist() == ["UNKNOWN_CONTEXT"] and unknown.strongest_rank_strength.isna().all()
    validate_context_rows_v1(unknown)
    with pytest.raises(ValueError, match="missing"):
        build_cross_package_rank_context_v1(roster_metadata=one, day_metadata=day.iloc[:1], calendar=calendar, package_pair=pair)


@pytest.mark.parametrize("contradiction", ["member", "rank", "identity", "clock", "hash"])
def test_known_roster_contradictions_fail_closed(contradiction):
    rows, days, calendar, pair, _, _, _ = synthetic_inputs()
    if contradiction == "member":
        rows = rows.iloc[1:]
    elif contradiction == "rank":
        rows.loc[0, "selection_effective_rank"] = 2
    elif contradiction == "identity":
        rows.loc[0, "manifest_sha256"] = "f"*64
    elif contradiction == "clock":
        rows.loc[0, KEY[1]] = rows.loc[0, KEY[0]]
    else:
        days["roster_sha256"] = "f"*64
    with pytest.raises(ValueError):
        build_cross_package_rank_context_v1(roster_metadata=rows, day_metadata=days, calendar=calendar, package_pair=pair)


def test_common_unique_roles_reuse_medians_and_do_not_decode_held_poison():
    rows, _, _, _, _, parent, plan = synthetic_inputs()
    held = rows[KEY[0]].ge(pd.Timestamp(plan.evaluation_start))
    rows[[*FEATURES, "terminal_mark_ratio", "path_mark_min_ratio"]] = rows[[*FEATURES, "terminal_mark_ratio", "path_mark_min_ratio"]].astype(object)
    rows.loc[held, [*FEATURES, "terminal_mark_ratio", "path_mark_min_ratio"]] = "opaque held finance"
    encoding = build_consensus_training_encoding_v1(rows=rows, parent_encoding=parent, plan=plan)
    assert encoding["contrast_status"] == "PREPARED_IDENTIFIABLE_NO_FIT" and encoding["medians"] == parent["medians"]
    assert sum(encoding["pools"].values()) < rows.consensus_training_role.isin(["STRUCTURE", "ESTIMATION"]).sum()
    for role, keys in encoding["role_clusters"].items():
        block = rows.loc[rows.label_cluster.isin(keys)]
        assert block.consensus_training_role.eq(role).all()
        if role == "STRUCTURE":
            assert block.label_information_end.lt(pd.Timestamp(plan.estimation_first_D)).all()
    nonidentical = rows.copy()
    nonidentical["cross_package_consensus_strength"], nonidentical["consensus_rank_fraction"] = 0., "0"
    assert build_consensus_training_encoding_v1(rows=nonidentical, parent_encoding=parent, plan=plan)["contrast_status"] == "NOT_IDENTIFIABLE_CONSENSUS_CONTRAST"
    bad = deepcopy(rows)
    duplicate = bad.loc[bad.instrument.eq("600004.SH") & bad.consensus_training_role.eq("STRUCTURE")].index[0]
    bad.loc[duplicate, "terminal_mark_ratio"] = 2.
    with pytest.raises(ValueError, match="contradictory"):
        build_consensus_training_encoding_v1(rows=bad, parent_encoding=parent, plan=plan)
