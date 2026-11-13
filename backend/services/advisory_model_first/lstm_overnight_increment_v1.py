"""One native overnight coordinate added to the frozen LSTM 54-field Ridge."""
from datetime import datetime, timezone
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

from . import lstm_factor_nonlinear_v1 as control

info = control.info
DAY, KEY, GROUP = info.DAY, info.KEY, info.GROUP
SIGNAL = "m_overnight_return_5d"
NEW = "extra__" + SIGNAL
BASE, FEATURES = info.FEATURES, (*info.FEATURES, NEW)
PARAMS = dict(control.PARAMS["ridge"])
INPUT_COLUMNS = tuple(dict.fromkeys((*info.spo.IDENTITY, "parent_score", "source_id", *BASE)))
SIGNAL_COLUMNS = (*KEY, "manifest_sha256", "selection_effective_rank", "catalog_id", "signal_name",
                  "signal_value", "signal_status", "calculation_visible_through",
                  "declared_next_session_consumption", "historical_first_availability")
MISSING = ("SOURCE_NA_WARMUP_OR_INPUT_UNKNOWN", "SOURCE_SYMBOL_ABSENT", "SOURCE_DATE_ABSENT")
CONTRACT = dict(schema="lstm_overnight_increment_v1", hypothesis="LSTM_OVERNIGHT_RATIO_INCREMENT_FIXED5_V1",
    target=info.prior.LSTM, base_fields=BASE, fields=FEATURES, params=PARAMS,
    folds=info.spo.base.FOLDS, development=info.CONTRACT["development"], stress=info.CONTRACT["stress"],
    label="GROUP_CENTERED_FIXED5_NET_BPS", encoding="TRAIN_MEDIAN_IQR_STD_ONE_MISSING_GROUP_CENTERED",
    max_new_fits=3, prior_fits=266, prior_index=1, final_fit=False, top_k=10, target_bps=100.,
    historical_pit=False, independent_confirmation=False, production_ready=False,
    signal=dict(catalog_id=1272, name=SIGNAL, semantics="NATIVE_UNADJUSTED_PRICE_RATIO_NON_VINTAGE",
                code_sha256="de0856a4ff1dfd5e7aacd31276eba566284074a809a1b046f33ab2e7ecd1e2b6"),
    bootstrap=info.spo.base.CONTRACT["bootstrap"])


def check_inputs(frame):
    info.check_inputs(frame)
    if NEW not in frame or frame[NEW].dtype.kind not in "fiu" or np.isinf(frame[NEW].to_numpy(float)).any():
        raise ValueError("invalid overnight coordinate; missing is distinct from invalid input")
    return frame


def assemble(inputs, signal):
    """Join only an exactly matching, version-bound projection; preserve all original rows."""
    info.check_inputs(inputs)
    if NEW in inputs or not set(SIGNAL_COLUMNS).issubset(signal.columns):
        raise ValueError("signal projection schema differs")
    if (len(signal) != len(inputs) or signal.duplicated(KEY).any() or signal[KEY].isna().any().any()
            or signal.catalog_id.dtype.kind not in "iu" or not signal.catalog_id.eq(1272).all()
            or not signal.signal_name.eq(SIGNAL).all() or signal.signal_value.dtype.kind not in "fiu"):
        raise ValueError("signal identity, KEY or numeric dtype differs")
    values = signal.signal_value.to_numpy(float)
    if np.isinf(values).any() or not np.array_equal(signal.signal_status.eq("AVAILABLE_NATIVE_RATIO"), np.isfinite(values)):
        raise ValueError("signal finite status differs")
    if not signal.loc[np.isnan(values), "signal_status"].isin(MISSING).all():
        raise ValueError("unregistered missing reason")
    if (not signal.calculation_visible_through.equals(signal[DAY])
            or not signal.declared_next_session_consumption.equals(signal.target_trade_date)
            or not signal.historical_first_availability.eq("UNKNOWN_NON_VINTAGE").all()):
        raise ValueError("signal calculation/availability clock differs")
    right = signal.loc[:, [*KEY, "manifest_sha256", "selection_effective_rank", "signal_value"]]
    joined = inputs.merge(right, on=KEY, how="left", sort=False, validate="one_to_one", indicator=True,
                          suffixes=("", "__signal"))
    if (not joined._merge.eq("both").all()
            or not joined.manifest_sha256.eq(joined.manifest_sha256__signal).all()
            or not joined.selection_effective_rank.eq(joined.selection_effective_rank__signal).all()):
        raise ValueError("signal original population/manifest/rank differs")
    pd.testing.assert_frame_equal(inputs[KEY].reset_index(drop=True), joined[KEY].reset_index(drop=True))
    output = inputs.copy().reset_index(drop=True)
    output[NEW] = joined.signal_value.to_numpy(copy=True)
    check_inputs(output)
    return output


def key_hash(frame):
    return info.spo.sha([[str(v) for v in row] for row in frame[KEY].itertuples(index=False, name=None)])


def training(rows, fold, old):
    check_inputs(rows)
    control.validate_model(old)
    if type(fold) is not int or fold not in (1, 2, 3) or old["arm"] != "ridge" or old["fold"] != fold:
        raise ValueError("CONTROL_NOT_MATCHED: original Ridge/fold required")
    cutoff = info.spo.base.FOLDS[fold - 1][0]
    if not rows[DAY].le(pd.Timestamp(cutoff)).all():
        raise ValueError("future training rows")
    part, stats = info.spo.training(rows, cutoff)  # Eligibility uses the original eight only.
    bx, by, metadata = control.training(rows, fold, old["training_keys_sha256"])
    for name in ("fields", "inactive_fields", "median", "scale", "training_keys_sha256",
                 "training_support", "train_rows", "train_days", "cutoff"):
        if metadata[name] != old[name]:
            raise ValueError("CONTROL_NOT_MATCHED: " + name)
    # Check the actual target and old coefficients, not just a KEY fingerprint.
    coef = np.asarray(old["learner"]["coef"], float)
    rhs = bx.T @ by
    residual = np.max(np.abs((bx.T @ bx + 100 * np.eye(bx.shape[1])) @ coef - rhs)) / max(1., np.max(np.abs(rhs)))
    if not np.isfinite(residual) or residual > 1e-8:
        raise ValueError("CONTROL_NOT_MATCHED: target/encoding normal equation")
    raw = info.numeric(part, FEATURES)
    active, median, scale = info.encoding(raw)
    if not active[-1]:
        raise ValueError("NO_TRAIN_SIGNAL_SUPPORT")
    fields = [name for name, keep in zip(FEATURES, active, strict=True) if keep]
    x = info.spo.group_center(info.transform(raw[:, active], median, scale), part)
    n = len(old["fields"])
    # Numeric coordinates and missing bits have separate offsets in the 55-field layout.
    common = [fields.index(f) for f in old["fields"]]
    np.testing.assert_array_equal(x[:, common + [len(fields) + i for i in common]], bx)
    if not np.any(x[:, fields.index(NEW)] != 0):
        raise ValueError("NO_TRAIN_SIGNAL_SUPPORT: constant within original groups")
    if n * 2 != bx.shape[1] or not np.isfinite(x).all():
        raise ValueError("invalid common training matrix")
    meta = dict(fields=fields, inactive_fields=[f for f, a in zip(FEATURES, active, strict=True) if not a],
        median=median.tolist(), scale=scale.tolist(), training_keys_sha256=key_hash(part),
        training_support=stats, train_rows=len(part), train_days=int(part[DAY].nunique()), cutoff=cutoff,
        control_payload_sha256=old["payload_sha256"], control_target_residual=float(residual))
    return x, by, meta


def sign(packet):
    packet["payload_sha256"] = info.spo.sha({k: v for k, v in packet.items() if k != "payload_sha256"})
    return packet


def validate_model(packet):
    fold = packet.get("fold")
    if (type(fold) is not int or fold not in (1, 2, 3) or packet.get("schema") != CONTRACT["schema"]
            or packet.get("contract_sha256") != info.spo.sha(CONTRACT) or packet.get("target") != info.prior.LSTM
            or packet.get("params") != PARAMS or packet.get("cutoff") != info.spo.base.FOLDS[fold - 1][0]
            or packet.get("payload_sha256") != info.spo.sha({k: v for k, v in packet.items() if k != "payload_sha256"})):
        raise ValueError("model contract/hash/clock differs")
    fields = packet.get("fields", [])
    if NEW not in fields or fields != [f for f in FEATURES if f in fields]:
        raise ValueError("model fields/order differ")
    arrays = [packet.get(k) for k in ("median", "scale", "coef")]
    if any(not isinstance(a, list) or any(type(v) not in (float, int) for v in a) for a in arrays):
        raise ValueError("model arrays must be numeric, excluding bool")
    median, scale, coef = map(lambda a: np.asarray(a, float), arrays)
    if (median.shape != (len(fields),) or scale.shape != median.shape or coef.shape != (2 * len(fields),)
            or not np.isfinite(np.r_[median, scale, coef]).all() or (scale <= 0).any()
            or not re.fullmatch(r"[0-9a-f]{64}", packet.get("control_payload_sha256", ""))):
        raise ValueError("invalid model encoding/coefficients/control binding")
    if (packet.get("inactive_fields") != [f for f in FEATURES if f not in fields]
            or not re.fullmatch(r"[0-9a-f]{64}", packet.get("training_keys_sha256", ""))
            or type(packet.get("train_rows")) is not int or type(packet.get("train_days")) is not int
            or packet["train_days"] < 2 or packet["train_rows"] != 50 * packet["train_days"]
            or packet.get("production_ready") is not False):
        raise ValueError("invalid model training identity/support")
    for value in (packet.get("control_target_residual"), packet.get("certificate", {}).get("relative_normal_equation_residual")):
        if type(value) not in (float, int) or not np.isfinite(value) or not 0 <= value <= 1e-8:
            raise ValueError("invalid model training certificate")


def fit(rows, fold, old, *, before_fit, budget=lambda: None):
    x, y, metadata = training(rows, fold, old)
    estimator = control.factory("ridge")
    budget()
    before_fit()  # Caller must already hold the research owner lock and authorization.
    estimator.fit(x, y, sample_weight=np.ones(len(y)))
    budget()
    coef = estimator.coef_
    rhs = x.T @ y
    residual = float(np.max(np.abs((x.T @ x + 100 * np.eye(x.shape[1])) @ coef - rhs)) / max(1., np.max(np.abs(rhs))))
    if not np.isfinite(residual) or residual > 1e-8:
        raise ValueError("Ridge normal equation residual exceeds contract")
    np.testing.assert_allclose(x @ coef, estimator.predict(x), rtol=1e-11, atol=1e-8)
    packet = sign(dict(schema=CONTRACT["schema"], contract_sha256=info.spo.sha(CONTRACT),
        target=info.prior.LSTM, fold=fold, params=PARAMS, coef=coef.tolist(), **metadata,
        certificate=dict(relative_normal_equation_residual=residual, official_export_parity=True),
        production_ready=False, score_semantics="UNCALIBRATED_GROUP_RELATIVE_BPS_DECISION_SCORE"))
    validate_model(packet)
    return packet, estimator


def feature_matrix(inputs, packet):
    check_inputs(inputs)
    info.numeric(inputs, FEATURES)
    validate_model(packet)
    eligible = np.isfinite(info.spo.numeric(inputs)).any(axis=1)
    raw = info.numeric(inputs.loc[eligible], packet["fields"])
    x = info.transform(raw, np.asarray(packet["median"]), np.asarray(packet["scale"]))
    return info.spo.group_center(x, inputs.loc[eligible]) if len(x) else x, eligible


def predict(inputs, packet, *, phase, oracle=None):
    x, eligible = feature_matrix(inputs, packet)
    _, first, last = info.spo.base.FOLDS[packet["fold"] - 1]
    if phase == "stress" and packet["fold"] == 3:
        first, last = CONTRACT["stress"]
    elif phase != "fold":
        raise ValueError("registered offline phase required")
    if not inputs[DAY].between(pd.Timestamp(first), pd.Timestamp(last)).all():
        raise ValueError("prediction outside registered fold/stress")
    values = x @ np.asarray(packet["coef"])
    if oracle is not None and len(x):
        np.testing.assert_allclose(values, oracle.predict(x), rtol=1e-11, atol=1e-8)
    if len(x):
        values = info.spo.group_center(values[:, None], inputs.loc[eligible]).ravel()
    if not np.isfinite(values).all():
        raise ValueError("nonfinite scores")
    result = inputs.loc[:, [*info.spo.IDENTITY, "parent_score", "source_id"]].copy().reset_index(drop=True)
    result["decision_score"], result["no_rank_advice"] = np.nan, ~eligible
    result["advisory_rank"] = result.selection_effective_rank.copy()
    result.loc[eligible, "decision_score"] = values
    for _, group in result.groupby(GROUP, sort=False):
        active = group.loc[~group.no_rank_advice]
        order = active.sort_values(["decision_score", "selection_effective_rank"], ascending=[False, True], kind="stable").index
        result.loc[order, "advisory_rank"] = sorted(active.selection_effective_rank)
    result["selected"] = result.advisory_rank.le(10)
    info.spo.evaluation.same_population(inputs, result)
    return result


def summarize(panel, comparator):
    result = info.summarize(panel, comparator)
    result["versus_matched_54_ridge"] = result.pop("versus_eight_fields")
    result["auxiliary_coverage"] = result["auxiliary_complete_days"] / result["mature_days"] if result["mature_days"] else 0.
    return result


def decision(development, stress, fold_means, fold_interventions):
    records = development, stress
    primary = [r["statistics"]["mean_bps"] for r in records]
    auxiliary = [r["versus_matched_54_ridge"]["mean_bps"] for r in records]
    required = [*primary, *auxiliary, *[r[k] for r in records for k in ("tail_delta_bps", "unknown_excess")]]
    if any(v is None or not np.isfinite(v) for v in required):
        action = "INCONCLUSIVE_NO_EXPANSION"
    elif any(v <= 0 for v in [*primary, *auxiliary]) or any(r["tail_delta_bps"] < -100 or r["unknown_excess"] > .02 for r in records):
        action = "STOP_EXACT_OVERNIGHT_INCREMENT_CANDIDATE"
    elif any(v < 100 for v in primary):
        action = "BELOW_BUSINESS_TARGET_NO_EXPANSION"
    elif (all(r["coverage"] >= .8 and r["auxiliary_coverage"] >= .8 and r["intervention_days"] >= 20
              and r["intervention_fraction"] >= .2 for r in records)
          and sum(v is not None and v > 0 and fold_interventions.get(k, 0) > 0 for k, v in fold_means.items()) >= 2):
        action = "DESIGN_INDEPENDENT_CONFIRMATION_ONLY"
    else:
        action = "INCONCLUSIVE_NO_EXPANSION"
    return dict(action=action, target_bps=100., extra_fit_budget=0, independent_economic_confirmation=False,
                production_ready=False, v2_changed=False)


class FitClaims:
    """Exactly three fold identities; failed STARTED claims consume their budget."""
    def __init__(self, root, digest):
        self.root, self.digest = Path(root), digest

    def write(self, fold, stage_sha256=None):
        if type(fold) is not int or fold not in (1, 2, 3) or not re.fullmatch(r"[0-9a-f]{64}", self.digest):
            raise ValueError("unregistered fit identity")
        self.root.mkdir(parents=True, exist_ok=True)
        starts = list(self.root.glob("*_started.json"))
        for path in starts:
            if path.name not in {f"fold{f}_started.json" for f in (1, 2, 3)} or json.loads(path.read_bytes())["plan_sha256"] != self.digest:
                raise ValueError("foreign fit claim")
        path = self.root / f"fold{fold}_started.json"
        payload = dict(plan_sha256=self.digest, fold=fold, status="STARTED", observed_at=datetime.now(timezone.utc).isoformat())
        if stage_sha256 is None:
            if len(starts) >= 3:
                raise ValueError("three-start financial budget exhausted")
        else:
            if not path.exists() or not re.fullmatch(r"[0-9a-f]{64}", stage_sha256):
                raise ValueError("finished claim requires its actual start and stage")
            path = self.root / f"fold{fold}_finished.json"
            payload.update(status="FINISHED", stage_sha256=stage_sha256)
        with path.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True)
