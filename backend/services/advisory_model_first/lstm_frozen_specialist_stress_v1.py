"""One fixed P19 model on P18's consumed historical axis; no model selection."""
import numpy as np
import pandas as pd

from . import lstm_complementarity_v1 as prior
from . import lstm_specialist_spo_v1 as specialist
from .lstm_historical_engineering_replay import verify_fees

spo, KEY, DAY = prior.spo, list(prior.KEY), prior.DAY
AXIS = [*KEY, "selection_effective_rank"]
CONTRACT = dict(schema="lstm_frozen_specialist_stress_v1", target=prior.LSTM, fold=3,
    cutoff="2025-07-31", first=prior.FIRST, last=prior.LAST, days=118, rows=5900,
    model_sha256="8c915844002eab8b2dde0f8bc91f176ee7768b16ba5089520f106167bdf48813",
    target_increment_bps=100., model_fits=0, new_candidates=0,
    evidence="PREVIOUSLY_CONSUMED_NON_VINTAGE_DEVELOPMENT_STRESS",
    primary="SPECIALIST_MINUS_NATIVE_OWN_COMPLETE_PAIRS", auxiliary="THREE_ARM_COMMON_DAYS",
    legacy_stress_rule="P18_UNCHANGED_DEVELOPMENT_RULE_NOT_100BPS_BUSINESS_ACCEPTANCE",
    independent_oos_evidence=False, production_ready=False, v2_changed=False)


def validate_inputs(inputs, axis):
    spo.validate_population(inputs)
    days = sorted(inputs[DAY].unique())
    if (len(inputs) != CONTRACT["rows"] or len(days) != CONTRACT["days"]
            or days[0] != pd.Timestamp(CONTRACT["first"]) or days[-1] != pd.Timestamp(CONTRACT["last"])
            or any(not inputs[k].eq(v).all() for k, v in prior.LSTM.items())):
        raise ValueError("fixed LSTM historical population or date axis differs")
    axis = axis.copy()
    for field in (DAY, "target_trade_date"):
        axis[field] = pd.to_datetime(axis[field])
    order = [DAY, "selection_effective_rank"]
    pd.testing.assert_frame_equal(inputs[AXIS].sort_values(order).reset_index(drop=True),
        axis[AXIS].sort_values(order).reset_index(drop=True), check_dtype=False)
    return days


def predict(inputs, packet, axis):
    validate_inputs(inputs, axis)
    if (packet.get("schema") != specialist.CONTRACT["schema"] or packet.get("fold") != 3
            or packet.get("contract_sha256") != spo.sha(specialist.CONTRACT)
            or packet.get("target") != prior.LSTM
            or packet.get("payload_sha256") != spo.sha({k: v for k, v in packet.items() if k != "payload_sha256"})
            or packet["model"].get("cutoff") != CONTRACT["cutoff"]
            or packet["model"].get("arm") != "candidate"):
        raise ValueError("current frozen specialist model identity or cutoff differs")
    # This is a new explicit offline contract, not the original 2025-fold or live capture API.
    return spo.predict(inputs, packet["model"])


def decision(report):
    state, mean = report["classification"], report["statistics"]["mean_bps"]
    if state == "STRESS_NOT_SUPPORTED_CURRENT_PAIR":
        action = "STOP_ADDITIONAL_DEVELOPMENT_THIS_FROZEN_MODEL"
    elif state == "STRESS_SUPPORTS_LSTM_PROSPECTIVE":
        action = "KEEP_FROZEN_OBSERVATION_NO_EXPANSION"
    elif state == "STRESS_INCONCLUSIVE":
        action = "INCONCLUSIVE_NO_NEW_FIT_OR_EXPANSION"
    else:
        raise ValueError("unknown stress decision branch")
    if mean is not None and not np.isfinite(mean):
        raise ValueError("business target requires a finite mean or explicit unknown")
    return dict(action=action, target_increment_bps=100.,
        target_status="NOT_EVALUATED" if mean is None else "POINT_ESTIMATE_AT_OR_ABOVE_TARGET" if mean >= 100 else "POINT_ESTIMATE_BELOW_TARGET",
        v2_changed=False, p20_stopped=False, production_ranking_replaced=False,
        independent_economic_confirmation=False, extra_fit_budget=0)


def evaluate(predictions, finance, shared_predictions, old_panel, old_report, axis):
    days = validate_inputs(predictions, axis)
    validate_inputs(shared_predictions, axis)
    verify_fees(finance)
    report, panel = prior.stress_report(predictions, finance, days)
    shared = spo.base.cohort(shared_predictions, finance).sort_values("decision_date").reset_index(drop=True)
    old = old_panel.sort_values("decision_date").reset_index(drop=True)
    pd.testing.assert_frame_equal(shared[old.columns], old, check_dtype=False, check_exact=False, rtol=1e-10, atol=1e-8)
    # Equal native values even where one of the two rerankers has unknown slots.
    np.testing.assert_allclose(panel.baseline, shared.baseline, equal_nan=True, rtol=1e-10, atol=1e-8)
    if not np.isclose(shared.delta.mean(), old_report["lstm_stress"]["statistics"]["mean_bps"], atol=1e-8):
        raise ValueError("reused shared result differs from original P18 report")
    wide = panel[["decision_date", "mature", "original_candidates", "baseline", "candidate"]].rename(
        columns={"baseline": "native", "candidate": "specialist"})
    wide["shared"] = shared.candidate
    common = wide.mature & wide[["native", "specialist", "shared"]].notna().all(axis=1)
    wide["three_arm_complete"] = common
    wide["specialist_minus_native"] = (wide.specialist - wide.native).where(common)
    wide["specialist_minus_shared"] = (wide.specialist - wide.shared).where(common)
    means = {arm + "_mean_bps": float(wide.loc[common, arm].mean()) if common.any() else None
             for arm in ("native", "shared", "specialist")}
    report.update(three_arm_common=dict(days=int(common.sum()), declared_days=len(wide), **means,
        specialist_minus_native=spo.base.paired_interval(wide.specialist_minus_native.to_numpy(float)),
        specialist_minus_shared=spo.base.paired_interval(wide.specialist_minus_shared.to_numpy(float))),
        decision=decision(report), original_shared_result=old_report["lstm_stress"],
        original_slot_status_counts={str(k): int(v) for k, v in finance.valuation_status.value_counts().items()},
        actual_model_training_cutoff=CONTRACT["cutoff"],
        shared_training_cutoff=prior.CUTOFF, pure_specialization_causality_proven=False,
        new_candidates=0, cumulative_actual_fits=248, old_index_count=1,
        historical_pit_proven=False, economic_confirmation=False)
    return report, panel, wide
