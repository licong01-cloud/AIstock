"""Approved L2 terminal-loss study and explicit dual-source loss inference.

No deployment, policy application, event-probability substitution or implicit fit.
"""

from __future__ import annotations

from collections import Counter
import hashlib
import math
from pathlib import Path
import re

import numpy as np
from scipy.stats import rankdata
from sklearn.linear_model import Ridge

from backend.services.hmm_risk import risk_l2_rolling as rolling
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_sha256
from backend.services.hmm_risk.formal_state_executor import EXPECTED_VERSIONS, THREAD_VARIABLES, read_json, write_once
from backend.services.hmm_risk.formal_state_model import receipt

core = rolling.core
require = rolling.require
VERSION = "hmm_risk_l2_terminal_loss_ridge_v1"
TARGET = {
    "anchor": "decision_close",
    "first_return_offset": 1,
    "horizon": 10,
    "formula": "max(0,1-product(1+r[t+j,s],j=1..10))",
}
PARAMS = dict(
    alpha=1.0, fit_intercept=True, copy_X=True, max_iter=None, tol=1e-4, solver="svd", positive=False, random_state=42
)
CONTRACT = dict(
    version=VERSION,
    target=TARGET,
    parameters=PARAMS,
    feature_names=list(ALL_CORE_FEATURES),
    vintages=list(rolling.VINTAGES),
    planned_fits=4,
    numeric_versions=EXPECTED_VERSIONS,
    numeric_threads=dict.fromkeys(THREAD_VARIABLES, "1"),
    historical_as_known=False,
    untouched_holdout=False,
    prediction_projection="clip_raw_to_0_1",
    policy_applied=False,
)
PREPARED_PIN = "3c03b4fdbe6155c56b97a1cd049d4d36ca2fca83f6c86053ad949234028cf567"
FEATURE_PIN = "94be96e74fb906098b6c9691fccc33895989586330af94b2243af046baf96b62"
MANIFEST_PIN = "590b3b204085373ff4bd101efc50a45c6dd3844200200f987f9bc783e0c6b394"
LABEL_PINS = {
    "2026Q2": "5e1e5170eae9a992b92bc523873b3be40f37e8d355374b0c501930d92cc9f38a",
    "2026Q3": "18a8334a7f897ea2dd5901a51316c0720fdc1d0729cf4ced5f3ab6141cdfc59e",
}
PREDICTION_PINS = {
    "M0": "a8b58217469682ae1a6ad0aac141a516b81724eb71b5aa8f548e405204debbd8",
    "M1": "16a92d69367d2f418b2bce1e7c0bf96218a09a0d0f6c2b4bf357c5a1fafe349c",
}


def load_study(path):
    original = core.inputs._asset(str(path), PREPARED_PIN)
    require(
        original["contract"] == rolling.CONTRACT and original["feature_sha256"] == FEATURE_PIN,
        "original study identity differs",
        "identity_mismatch",
    )
    features = core.inputs._asset(original["feature_path"], FEATURE_PIN)
    rolling.validate_features(features)
    require(
        features["source"]["release_binding"]["manifest_sha256"] == MANIFEST_PIN,
        "approved release differs",
        "identity_mismatch",
    )
    return original, features


def loss_from_label(label, *, mature):
    rolling.risk._validate_target(label, mature=mature)
    if label["status"] != "AVAILABLE":
        return None
    value = label["return"]
    require(
        type(value) in (int, float) and math.isfinite(value) and value > -1,
        "terminal return must be finite and greater than -1",
    )
    return max(0.0, -float(value))


def fit_parameters(rows):
    require(bool(rows), "no mature loss training rows", "train_label_evidence_insufficient")
    x = np.asarray([r[2] for r in rows], dtype=np.float64)
    y = np.asarray([r[3] for r in rows], dtype=np.float64)
    require(
        x.ndim == 2
        and x.shape[1] == 20
        and np.isfinite(x).all()
        and np.isfinite(y).all()
        and np.all((y >= 0) & (y < 1)),
        "invalid loss training data",
    )
    mean, scale = x.mean(axis=0), x.std(axis=0, ddof=0)
    active = np.any(x != x[0], axis=0)
    mean[~active], scale[~active] = x[0, ~active], 0.0
    require(
        active.any() and np.isfinite(mean).all() and np.isfinite(scale).all() and np.all(scale[active] > 0),
        "invalid train-only scaler",
        "fit_failed",
    )
    counts = Counter(r[0] for r in rows)
    weights = np.asarray([len(rows) / (len(counts) * counts[r[0]]) for r in rows], dtype=np.float64)
    z = (x[:, active] - mean[active]) / scale[active]
    require(np.isfinite(z).all(), "nonfinite train transform", "fit_failed")
    model = Ridge(**PARAMS).fit(z, y, sample_weight=weights)
    require(np.isfinite(model.coef_).all() and math.isfinite(float(model.intercept_)), "nonfinite fit", "fit_failed")
    parameters = dict(
        mean=mean.tolist(),
        scale=scale.tolist(),
        active=active.tolist(),
        coef=model.coef_.tolist(),
        intercept=float(model.intercept_),
    )
    return parameters, dict(
        fit_rows=len(rows),
        fit_dates=len(counts),
        sample_weight_sum=float(weights.sum()),
        training_loss_mean=float(np.average(y, weights=weights)),
    )


def parameter_arrays(parameters):
    require(set(parameters) == {"mean", "scale", "active", "coef", "intercept"}, "loss parameter fields differ")
    require(
        isinstance(parameters["active"], list) and all(type(v) is bool for v in parameters["active"]),
        "active mask must be boolean",
    )
    require(
        all(
            isinstance(parameters[k], list) and all(type(v) in (int, float) for v in parameters[k])
            for k in ("mean", "scale", "coef")
        ),
        "parameters must be numeric arrays",
    )
    mean, scale, coef = (np.asarray(parameters[k], dtype=np.float64) for k in ("mean", "scale", "coef"))
    active = np.asarray(parameters["active"], dtype=bool)
    intercept = parameters["intercept"]
    require(
        mean.shape == scale.shape == active.shape == (20,)
        and active.any()
        and coef.shape == (int(active.sum()),)
        and np.all(scale[active] > 0)
        and np.all(scale[~active] == 0)
        and type(intercept) in (float, int)
        and math.isfinite(intercept)
        and all(np.isfinite(v).all() for v in (mean, scale, coef)),
        "loss/scaler shape or values differ",
        "identity_mismatch",
    )
    return mean, scale, active, coef, intercept


def validate_model(model, *, expected_sha256=None):
    core.verify_receipt(model, expected_sha256)
    require(
        set(model)
        == {
            "schema_version",
            "contract",
            "vintage",
            "parameters",
            "model_sha256",
            "feature_definition_sha256",
            "feature_sha256",
            "training_label_sha256",
            "training_input_identity",
            "training_release_binding",
            "source_commit",
            "catalog",
            "fit_rows",
            "fit_dates",
            "sample_weight_sum",
            "training_loss_mean",
            "receipt_sha256",
        },
        "loss model schema differs",
    )
    require(
        model["schema_version"] == VERSION + "_model"
        and model["contract"] == CONTRACT
        and model["vintage"] in rolling.VINTAGES
        and len(model["catalog"]) == 131
        and model["catalog"] == sorted(set(model["catalog"]))
        and model["model_sha256"] == canonical_sha256(model["parameters"])
        and re.fullmatch(r"[0-9a-f]{40}", model["source_commit"]) is not None,
        "loss model contract/identity differs",
        "identity_mismatch",
    )
    for key in ("feature_definition_sha256", "feature_sha256", "training_label_sha256", "model_sha256"):
        core.reader._require_sha256(model[key], key)
    require(
        type(model["fit_rows"]) is int
        and type(model["fit_dates"]) is int
        and 0 < model["fit_dates"] <= model["fit_rows"] <= 131 * model["fit_dates"]
        and math.isfinite(model["sample_weight_sum"])
        and abs(model["sample_weight_sum"] - model["fit_rows"]) < 1e-6
        and math.isfinite(model["training_loss_mean"])
        and 0 <= model["training_loss_mean"] < 1,
        "loss training statistics invalid",
    )
    binding, identity = model["training_release_binding"], model["training_input_identity"]["release_identity"]
    require(
        binding["manifest_sha256"] == MANIFEST_PIN
        and model["feature_sha256"] == FEATURE_PIN
        and model["training_label_sha256"] == LABEL_PINS[model["vintage"]["id"]]
        and identity["dataset_manifest_sha256"] == MANIFEST_PIN
        and identity["frozen_release_binding_sha256"] == canonical_sha256(binding),
        "loss training release identity differs",
        "identity_mismatch",
    )
    parameter_arrays(model["parameters"])


def predict(model, rows, days, as_of, calendar):
    validate_model(model)
    vintage, codes = model["vintage"], model["catalog"]
    require(days and days == sorted(set(days)) and calendar == sorted(set(calendar)), "date ordering differs")
    positions = {d: i for i, d in enumerate(calendar)}
    require(
        all(
            d in positions
            and positions[d] > 0
            and as_of[d] == calendar[positions[d] - 1]
            and vintage["train_end"] < d
            and vintage["effective_start"] <= d <= vintage["effective_end"]
            for d in days
        ),
        "loss inference outside causal/effective interval",
    )
    core.validate_feature_rows({"decision_dates": days}, {"rows": rows, "catalog": codes}, as_of)
    mean, scale, active, coef, intercept = parameter_arrays(model["parameters"])
    output, clipped, inactive = [], 0, 0
    for day in days:
        future = calendar[positions[day] + 1 : positions[day] + 11]
        for code in codes:
            item = rows[day][code]
            raw = loss = None
            if item["features"] is not None:
                values = np.asarray(item["features"], dtype=np.float64)
                inactive += int(np.any(values[~active] != mean[~active]))
                z = (values[active] - mean[active]) / scale[active]
                raw = float(z @ coef + intercept)
                require(np.isfinite(z).all() and math.isfinite(raw), "nonfinite loss prediction", "prediction_failed")
                loss = min(1.0, max(0.0, raw))
                clipped += int(loss != raw)
            output.append(
                dict(
                    trade_date=day,
                    as_of_date=as_of[day],
                    sector_code=code,
                    vintage_id=vintage["id"],
                    model_sha256=model["model_sha256"],
                    raw_expected_loss=raw,
                    expected_loss=loss,
                    availability="available" if loss is not None else "unavailable",
                    reason_code=item["reason_code"],
                    target_first_return_date=future[0] if future else None,
                    target_last_return_date=future[-1] if len(future) == 10 else None,
                )
            )
    return output, dict(clipped_rows=clipped, inactive_changed_rows=inactive)


def fit_vintage(features, training, vintage, source_commit, on_fit_start):
    plan = rolling.schedule(features["calendar"], vintage)
    core.verify_receipt(training, LABEL_PINS[vintage["id"]])
    require(
        set(training) == {"schema_version", "vintage", "feature_sha256", "target", "labels", "receipt_sha256"}
        and training["schema_version"] == rolling.VERSION + "_train_labels"
        and training["vintage"] == vintage
        and training["target"] == rolling.TARGET
        and training["feature_sha256"] == features["receipt_sha256"]
        and set(training["labels"]) == set(plan["train"]),
        "training label authority differs",
    )
    rows, fit_days = [], set(plan["fit"])
    for day in plan["train"]:
        require(set(training["labels"][day]) == set(features["catalog"]), "training grid differs")
        for code in features["catalog"]:
            loss = loss_from_label(training["labels"][day][code], mature=day in fit_days)
            vector = features["rows"][day][code]["features"]
            if day in fit_days and loss is not None and vector is not None:
                rows.append((day, code, vector, loss))
    on_fit_start()
    parameters, stats = fit_parameters(rows)
    model = receipt(
        dict(
            schema_version=VERSION + "_model",
            contract=CONTRACT,
            vintage=vintage,
            parameters=parameters,
            model_sha256=canonical_sha256(parameters),
            feature_definition_sha256=features["model_identity"]["feature_definition_sha256"],
            feature_sha256=features["receipt_sha256"],
            training_label_sha256=training["receipt_sha256"],
            training_input_identity=features["inference_identity"],
            training_release_binding=features["source"]["release_binding"],
            source_commit=source_commit,
            catalog=features["catalog"],
            **stats,
        )
    )
    predictions, counts = predict(
        model,
        {d: features["rows"][d] for d in plan["predict"]},
        plan["predict"],
        features["as_of"],
        features["calendar"],
    )
    return receipt(dict(model=model, predictions=predictions, fits=1, **counts))


def rank_correlation(x, y):
    a, b = rankdata(x), rankdata(y)
    return float(np.corrcoef(a, b)[0, 1]) if len(a) > 1 and np.std(a) > 0 and np.std(b) > 0 else None


def anchor_loss(calendar, returns, day, code):
    position = calendar.index(day)
    path = [1 + returns[d][code] for d in calendar[position + 1 : position + 11]]
    require(len(path) == 10 and all(math.isfinite(v) and v > 0 for v in path), "invalid mature return path")
    return max(0.0, 1.0 - min(1.0, float(np.cumprod(path).min())))


def evaluate(features, sealed, facts, baselines):
    core.verify_receipt(facts)
    require(
        facts["feature_sha256"] == features["receipt_sha256"]
        and facts["schema_version"] == rolling.VERSION + "_outcomes",
        "evaluation facts differ",
    )
    predictions = [r for s in sealed for r in s["predictions"]]
    days = sorted({r["trade_date"] for r in predictions})
    labels = rolling.risk.drawdown_outcomes(features["calendar"], days, features["catalog"], facts["returns"], days[-1])
    means = {s["model"]["vintage"]["id"]: s["model"]["training_loss_mean"] for s in sealed}
    daily, excluded, native_baselines = [], Counter(), {}
    for arm, mapping in baselines.items():
        native = []
        reasons = Counter()
        for day in days:
            pairs = []
            for code in features["catalog"]:
                row, label = mapping[day, code], labels[day][code]
                if row["availability"] != "available":
                    reasons[row["reason_code"]] += 1
                elif label["status"] != "AVAILABLE":
                    reasons[label["status"]] += 1
                else:
                    pairs.append((row["probability"], loss_from_label(label, mature=True)))
            native.append(
                dict(day=day, rows=len(pairs), rank_ic=rank_correlation([p for p, _ in pairs], [y for _, y in pairs]))
            )
        native_baselines[arm] = dict(daily=native, NA_reasons=dict(reasons))
    for day in days:
        valid = []
        for prediction in (r for r in predictions if r["trade_date"] == day):
            label = labels[day][prediction["sector_code"]]
            if prediction["availability"] != "available":
                excluded[prediction["reason_code"]] += 1
            elif label["status"] != "AVAILABLE":
                excluded[label["status"]] += 1
            else:
                valid.append((prediction, loss_from_label(label, mature=True)))
        if not valid:
            daily.append(
                dict(day=day, rows=0, mse=None, constant_mse=None, mae=None, rank_ic=None, baseline_rank_ic={})
            )
            continue
        actual = [y for _, y in valid]
        paired = {}
        for arm, mapping in baselines.items():
            pairs = [(r, y, mapping[day, r["sector_code"]]) for r, y in valid]
            eligible = [
                (y, b["probability"], r["expected_loss"]) for r, y, b in pairs if b["availability"] == "available"
            ]
            paired[arm] = dict(
                rows=len(eligible),
                rank_ic=rank_correlation([p for _, p, _ in eligible], [y for y, _, _ in eligible]),
                loss_rank_ic=rank_correlation([v for _, _, v in eligible], [y for y, _, _ in eligible]),
            )
        daily.append(
            dict(
                day=day,
                rows=len(valid),
                mse=float(np.mean([(r["expected_loss"] - y) ** 2 for r, y in valid])),
                constant_mse=float(np.mean([(means[r["vintage_id"]] - y) ** 2 for r, y in valid])),
                mae=float(np.mean([abs(r["expected_loss"] - y) for r, y in valid])),
                rank_ic=rank_correlation([r["expected_loss"] for r, _ in valid], actual),
                return_rank_ic=rank_correlation(
                    [r["expected_loss"] for r, _ in valid],
                    [labels[day][r["sector_code"]]["return"] for r, _ in valid],
                ),
                fixed_anchor_loss_rank_ic=rank_correlation(
                    [r["expected_loss"] for r, _ in valid],
                    [anchor_loss(features["calendar"], facts["returns"], day, r["sector_code"]) for r, _ in valid],
                ),
                baseline_rank_ic=paired,
            )
        )
    segments = {}
    for name, start, end in [("full", days[0], days[-1])] + [
        (v["id"], v["effective_start"], v["effective_end"]) for v in rolling.VINTAGES
    ]:
        part = [r for r in daily if start <= r["day"] <= end and r["rows"]]
        mse = float(np.mean([r["mse"] for r in part])) if part else None
        constant = float(np.mean([r["constant_mse"] for r in part])) if part else None
        ranks = [r["rank_ic"] for r in part if r["rank_ic"] is not None]
        segments[name] = dict(
            days=len(part),
            rows=sum(r["rows"] for r in part),
            mse=mse,
            constant_mse=constant,
            mse_skill=1 - mse / constant if constant is not None and constant > 0 else None,
            mae=float(np.mean([r["mae"] for r in part])) if part else None,
            mean_daily_rank_ic=float(np.mean(ranks)) if ranks else None,
            rank_defined_days=len(ranks),
        )
    skill = segments["full"]["mse_skill"]
    return receipt(
        dict(
            schema_version=VERSION + "_evaluation",
            segments=segments,
            daily_metrics=daily,
            NA_reasons=dict(excluded),
            native_baseline_metrics=native_baselines,
            prediction_days=len(days),
            prediction_rows=len(predictions),
            research_disposition="ELIGIBLE_FOR_QE_CONSUMER_DESIGN"
            if skill is not None and skill > 0
            else "STOP_NO_PREDICTIVE_LOSS_INCREMENT",
            economic_value_confirmed=False,
            automatic_promotion=False,
            historical_as_known=False,
            untouched_holdout=False,
        )
    )


def load_baselines(paths, features):
    expected = [
        (d, c)
        for v in rolling.VINTAGES
        for d in rolling.schedule(features["calendar"], v)["predict"]
        for c in features["catalog"]
    ]
    baselines = {}
    for arm, pin in PREDICTION_PINS.items():
        path = Path(paths[arm])
        require(path.is_absolute() and path.is_file() and not path.is_symlink(), "invalid baseline path")
        before = core.inputs._stamp(path)
        require(
            hashlib.sha256(path.read_bytes()).hexdigest() == pin, "old prediction byte pin differs", "identity_mismatch"
        )
        data = read_json(path)
        require(core.inputs._stamp(path) == before, "baseline changed while read", "identity_mismatch")
        core.verify_receipt(data)
        grid = [(r["trade_date"], r["sector_code"]) for r in data["predictions"]]
        require(grid == expected, "baseline full grid differs")
        baselines[arm] = dict(zip(grid, data["predictions"]))
    return baselines


def execute_child(study_path, baseline_paths, output, source_commit):
    original, features = load_study(study_path)
    baselines = load_baselines(baseline_paths, features)
    sealed = []
    for vintage in rolling.VINTAGES:
        locator = original["training_labels"][vintage["id"]]
        training = core.inputs._asset(locator["path"], locator["sha256"])
        result = fit_vintage(
            features,
            training,
            vintage,
            source_commit,
            lambda: write_once(
                output / (vintage["id"] + ".attempt.json"), receipt(dict(vintage=vintage, fit_kernel_attempts=1))
            ),
        )
        path = output / (vintage["id"] + ".sealed.json")
        write_once(path, result)
        require(core.inputs._asset(str(path), result["receipt_sha256"]) == result, "sealed readback differs")
        sealed.append(result)
    # Both quarters have been sealed before these already-consumed outcomes are opened.
    facts = core.inputs._asset(original["outcome_path"], original["outcome_sha256"])
    return receipt(
        dict(
            schema_version=VERSION + "_repeat",
            contract=CONTRACT,
            source_commit=source_commit,
            original_prepared_sha256=PREPARED_PIN,
            fits=2,
            sealed=sealed,
            evaluation=evaluate(features, sealed, facts, baselines),
            database_access=False,
            runtime_action=False,
        )
    )


def infer_prepared(model, request, bundle, *, source_mode, expected_model_sha256, expected_bundle_sha256):
    """Reuse authentic 20D source carriers; never call the old probability kernel."""
    if source_mode == "frozen_file":
        consumer = core
    elif source_mode == "database_daily":
        from backend.services.hmm_risk import risk_l2_database_inference as consumer
    else:
        raise rolling.risk.fail("unknown loss input source", "source_invalid")
    consumer.validate_bundle(request, bundle, expected_bundle_sha256)
    validate_model(model, expected_sha256=expected_model_sha256)
    require(
        bundle["catalog"] == model["catalog"]
        and bundle["model_identity"]["feature_definition_sha256"] == model["feature_definition_sha256"],
        "loss model/source feature identity differs",
        "identity_mismatch",
    )
    if source_mode == "frozen_file":
        core.date_plan(request)
        source = request["source"]
        path = Path(source["candidate_root"]) / "components/daily_bin_candidate/calendars/day.txt"
        before = core.inputs._stamp(path)
        path = core.inputs.reader._require_file(
            Path(source["candidate_root"]),
            "components/daily_bin_candidate/calendars/day.txt",
            source["calendar_sha256"],
        )
        calendar = path.read_text(encoding="utf-8").splitlines()
        require(core.inputs._stamp(path) == before, "calendar changed during read", "identity_mismatch")
    else:
        calendar = bundle["inference_identity"]["calendar"]
    predictions, counts = predict(model, bundle["rows"], request["decision_dates"], bundle["as_of"], calendar)
    return receipt(
        dict(
            schema_version=VERSION + "_prediction",
            contract=CONTRACT,
            model_bundle_sha256=model["receipt_sha256"],
            model_sha256=model["model_sha256"],
            source_mode=source_mode,
            source_features_sha256=bundle["receipt_sha256"],
            source_commit=bundle["source_commit"],
            inference_identity=bundle["inference_identity"],
            predictions=predictions,
            **counts,
            fits=0,
            labels_read=False,
            outcomes_read=False,
            policy_applied=False,
            historical_as_known=False,
            forward_confirmed=False,
        )
    )
