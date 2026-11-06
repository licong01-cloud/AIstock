"""Frozen parent-state readout, fixed population/5TD; offline research only."""
from datetime import datetime, timezone
from fractions import Fraction
import json
from pathlib import Path

import numpy as np
import pandas as pd

from . import lstm_factor_information_v1 as info
from . import lstm_factor_nonlinear_v1 as old

DAY, KEY, GROUP = info.DAY, info.KEY, info.GROUP
HIDDEN = tuple(f"lstm_h_{i:02}" for i in range(64))
PARAMS = dict(old.PARAMS["ridge"])
CONTRACT = dict(schema="lstm_frozen_representation_v1", hypothesis="FROZEN_LSTM_STATE_FIXED5_V1",
    target=info.prior.LSTM, base_fields=info.FEATURES, hidden_fields=HIDDEN,
    representation_sha256="fb4c5ba066c794324eacf3bf1d642dc535e13766ac592043b1fd53f4647ce0ee",
    folds=info.spo.base.FOLDS, params=PARAMS, step_len=20, parent_features=46, hidden_dimensions=64,
    hidden_encoding="TRAIN_MEDIAN_IQR_ELSE_STD_ELSE_ONE_PLUS_SINGLE_BLOCK_MISSING_FLAG",
    target_bps=100., max_new_fits=3, prior_fits=263, old_index_count=1, final_fit=False,
    score="EXACT_RATIONAL_UNCENTERED_LINEAR_V1", development=info.CONTRACT["development"],
    stress=info.CONTRACT["stress"], evidence="NON_VINTAGE_CONSUMED_HISTORY", production_ready=False)


def check_inputs(frame):
    info.check_inputs(frame)
    if not set((*HIDDEN, "representation_supported", "representation_reason")).issubset(frame.columns):
        raise ValueError("missing representation contract fields")
    if frame.representation_supported.dtype != bool:
        raise ValueError("representation support must be bool")
    if any(frame[h].dtype.kind not in "fiu" for h in HIDDEN):
        raise ValueError("hidden values must be numeric")
    h = frame.loc[:, HIDDEN].to_numpy(float)
    support = frame.representation_supported.to_numpy()
    if not np.isfinite(h[support]).all() or (np.abs(h[support]) > 1.000001).any():
        raise ValueError("supported hidden state is invalid")
    if not np.isnan(h[~support]).all():
        raise ValueError("unsupported hidden state must be a complete missing block")
    if not frame.loc[support, "representation_reason"].eq("AVAILABLE").all() or not frame.loc[
            ~support, "representation_reason"].eq("SOURCE_SEQUENCE_UNAVAILABLE").all():
        raise ValueError("representation support/reason mismatch")
    return h


def hidden_matrix(raw, median, scale):
    missing = np.isnan(raw).all(axis=1)
    return np.column_stack(((np.where(np.isnan(raw), median, raw) - median) / scale, missing.astype(float)))


def training(rows, fold, control):
    if type(fold) is not int or fold not in (1, 2, 3):
        raise ValueError("registered fold required")
    check_inputs(rows)
    old.validate_model(control)
    if control["arm"] != "ridge" or control["fold"] != fold:
        raise ValueError("exact original Ridge control required")
    x, y, meta = old.training(rows, fold, control["training_keys_sha256"])
    for field in ("fields", "median", "scale", "inactive_fields", "cutoff", "train_rows", "train_days",
                  "training_support", "training_keys_sha256"):
        if meta[field] != control[field]:
            raise ValueError("original control training contract differs: " + field)
    part, _ = info.spo.training(rows, meta["cutoff"])
    raw = check_inputs(part)
    active, median, scale = info.encoding(raw)
    if not active.all():
        raise ValueError("training has no supported hidden block")
    h = info.spo.group_center(hidden_matrix(raw, median, scale), part)
    full = np.column_stack((x, h))
    if not np.isfinite(full).all():
        raise ValueError("nonfinite encoded training values")
    return full, y, dict(**meta, hidden_median=median.tolist(), hidden_scale=scale.tolist(),
                        hidden_train_missing=int(np.isnan(raw).all(axis=1).sum()))


def validate_model(packet):
    fold = packet.get("fold")
    if type(fold) is not int or fold not in (1, 2, 3):
        raise ValueError("invalid model fold")
    if (packet.get("schema") != CONTRACT["schema"] or packet.get("contract_sha256") != info.spo.sha(CONTRACT)
            or packet.get("payload_sha256") != info.spo.sha({k: v for k, v in packet.items() if k != "payload_sha256"})
            or packet.get("target") != info.prior.LSTM or packet.get("params") != PARAMS
            or packet.get("cutoff") != info.spo.base.FOLDS[fold - 1][0]
            or packet.get("representation_sha256") != CONTRACT["representation_sha256"]
            or packet.get("hidden_fields") != list(HIDDEN)):
        raise ValueError("model contract, hash, clock or representation differs")
    fields = packet.get("fields", [])
    if not fields or fields != [f for f in info.FEATURES if f in fields]:
        raise ValueError("invalid base field order")
    for prefix, size in (("", len(fields)), ("hidden_", 64)):
        median, scale = (np.asarray(packet[prefix + name], float) for name in ("median", "scale"))
        if (median.shape != (size,) or scale.shape != (size,) or not np.isfinite(np.r_[median, scale]).all()
                or (scale <= 0).any()):
            raise ValueError("invalid model encoding")
    coef = np.asarray(packet["learner"]["coef"], float)
    if coef.shape != (2 * len(fields) + 65,) or not np.isfinite(coef).all():
        raise ValueError("invalid readout coefficients")


def fit(rows, fold, control, *, before_fit, budget=lambda: None):
    from sklearn.linear_model import Ridge
    x, y, meta = training(rows, fold, control)
    estimator = Ridge(**PARAMS)
    if estimator.get_params(deep=False) != PARAMS:
        raise ValueError("installed Ridge parameters differ")
    budget()
    before_fit()
    estimator.fit(x, y, sample_weight=np.ones(len(y)))
    budget()
    result = x @ estimator.coef_
    np.testing.assert_allclose(result, estimator.predict(x), rtol=1e-11, atol=1e-8)
    lhs, rhs = x.T @ x + 100 * np.eye(x.shape[1]), x.T @ y
    residual = float(np.max(np.abs(lhs @ estimator.coef_ - rhs)) / max(1., np.max(np.abs(rhs))))
    if residual > 1e-8:
        raise ValueError("normal equation residual exceeds tolerance")
    packet = dict(schema=CONTRACT["schema"], contract_sha256=info.spo.sha(CONTRACT), target=info.prior.LSTM,
        params=PARAMS, fold=fold, representation_sha256=CONTRACT["representation_sha256"],
        hidden_fields=list(HIDDEN), learner=dict(coef=estimator.coef_.tolist()), **meta,
        certificate=dict(relative_normal_equation_residual=residual, official_predict_parity=True,
                         train_mse=float(np.mean((result - y) ** 2))), production_ready=False)
    packet["payload_sha256"] = info.spo.sha(packet)
    validate_model(packet)
    return packet, estimator


def exact_scores(x, coef):
    """BUG-2178 semantics: rank original encoded floats without a rounded group offset."""
    matrix, weights = np.asarray(x, float), np.asarray(coef, float)
    if matrix.ndim != 2 or weights.shape != (matrix.shape[1],) or not np.isfinite(matrix).all() or not np.isfinite(weights).all():
        raise ValueError("invalid exact linear operands")
    active = [(j, Fraction(float(v))) for j, v in enumerate(weights) if v != 0]
    return [sum((Fraction(float(row[j])) * weight for j, weight in active), Fraction(0)) for row in matrix]


def predict(inputs, packet, *, phase, control=False):
    hidden = check_inputs(inputs)
    if control:
        old.validate_model(packet)
        if packet["arm"] != "ridge":
            raise ValueError("original Ridge control required")
    else:
        validate_model(packet)
    fold = packet["fold"]
    _, first, last = info.spo.base.FOLDS[fold - 1]
    if phase == "stress" and fold == 3:
        first, last = CONTRACT["stress"]
    elif phase != "fold":
        raise ValueError("registered offline phase required")
    if not inputs[DAY].between(pd.Timestamp(first), pd.Timestamp(last)).all():
        raise ValueError("prediction outside frozen window")
    info.numeric(inputs)
    eligible = np.isfinite(info.spo.numeric(inputs)).any(axis=1)
    raw = info.numeric(inputs.loc[eligible], packet["fields"])
    x = info.transform(raw, np.asarray(packet["median"]), np.asarray(packet["scale"]))
    if not control:
        x = np.column_stack((x, hidden_matrix(hidden[eligible], np.asarray(packet["hidden_median"]), np.asarray(packet["hidden_scale"]))))
    exact = exact_scores(x, packet["learner"]["coef"])
    result = inputs.loc[:, [*info.spo.IDENTITY, "parent_score", "source_id"]].copy().reset_index(drop=True)
    result["decision_score"], result["ranking_score_exact"] = np.nan, None
    result["ranking_semantics"] = CONTRACT["score"]
    result["no_rank_advice"] = ~eligible
    result["advisory_rank"] = result.selection_effective_rank.copy()
    lookup = dict(zip(np.flatnonzero(eligible), exact, strict=True))
    for _, group in result.groupby(GROUP, sort=False):
        active = group.loc[~group.no_rank_advice]
        if active.empty:
            continue
        mean = sum((lookup[i] for i in active.index), Fraction(0)) / len(active)
        for i in active.index:
            value = float(lookup[i] - mean)
            if not np.isfinite(value):
                raise ValueError("unrepresentable diagnostic score")
            result.loc[i, "decision_score"] = value
            result.loc[i, "ranking_score_exact"] = str(lookup[i])
        order = sorted(active.index, key=lambda i: (-lookup[i], int(result.loc[i, "selection_effective_rank"])))
        result.loc[order, "advisory_rank"] = sorted(active.selection_effective_rank)
    result["selected"] = result.advisory_rank.le(10)
    info.spo.evaluation.same_population(inputs, result)
    return result


def summarize(panel, control):
    report = info.summarize(panel, control)
    report["versus_frozen_ridge"] = report.pop("versus_eight_fields")
    return report


def decision(development, stress, fold_means):
    result = info.decision(development, stress, fold_means)
    result["action"] = dict(STOP_THIS_FACTOR_CANDIDATE="STOP_EXACT_REPRESENTATION_CANDIDATE",
        BELOW_BUSINESS_TARGET_NO_EXPANSION="BELOW_TARGET_STOP",
        DESIGN_INDEPENDENT_CONFIRMATION_ONLY="DESIGN_CONFIRMATION_ONLY").get(result["action"], result["action"])
    if any(r["statistics"]["mean_bps"] is None for r in (development, stress)):
        result["action"] = "NOT_EVALUABLE"
    result["representation_increment_supported_descriptively"] = all(
        r["versus_frozen_ridge"]["mean_bps"] is not None and r["versus_frozen_ridge"]["mean_bps"] > 0
        for r in (development, stress))
    return result


class FitClaims:
    def __init__(self, root, digest):
        self.root, self.digest = Path(root), digest

    def write(self, fold, stage_sha256=None):
        if type(fold) is not int or fold not in (1, 2, 3) or len(self.digest) != 64:
            raise ValueError("unregistered fit identity")
        self.root.mkdir(parents=True, exist_ok=True)
        starts = list(self.root.glob("*_started.json"))
        allowed = {f"candidate{i}_started.json" for i in (1, 2, 3)}
        for p in starts:
            if p.name not in allowed or json.loads(p.read_bytes())["plan_sha256"] != self.digest:
                raise ValueError("foreign fit claim")
        path = self.root / f"candidate{fold}_started.json"
        if stage_sha256 is None:
            if len(starts) >= 3:
                raise ValueError("three-start budget exhausted")
            packet = dict(status="STARTED", fold=fold, plan_sha256=self.digest,
                          observed_at=datetime.now(timezone.utc).isoformat())
        else:
            if not path.exists() or len(stage_sha256) != 64:
                raise ValueError("finish requires valid start and stage")
            path = self.root / f"candidate{fold}_finished.json"
            packet = dict(status="FINISHED", fold=fold, plan_sha256=self.digest, stage_sha256=stage_sha256)
        with path.open("x", encoding="utf8") as stream:
            json.dump(packet, stream, indent=2)
