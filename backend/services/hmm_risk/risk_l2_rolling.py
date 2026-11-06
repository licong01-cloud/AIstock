"""One approved quarterly L2 risk update; immutable files, no deployment."""

from __future__ import annotations

from collections import Counter
import hashlib
import math
from pathlib import Path
import re

import numpy as np
from sklearn.metrics import roc_auc_score

from backend.services.hmm_risk import risk_l2 as risk
from backend.services.hmm_risk import risk_l2_date_inference as core
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_sha256
from backend.services.hmm_risk.formal_state_executor import read_json, validate_output_location, write_once
from backend.services.hmm_risk.formal_state_model import receipt

VERSION = "hmm_risk_l2_quarterly_rolling_v1"
VINTAGES = (
    dict(
        id="2026Q2",
        train_start="2023-04-01",
        train_end="2026-03-31",
        effective_start="2026-04-01",
        effective_end="2026-06-30",
    ),
    dict(
        id="2026Q3",
        train_start="2023-07-01",
        train_end="2026-06-30",
        effective_start="2026-07-01",
        effective_end="2026-09-30",
    ),
)
TARGET = {"anchor": "decision_plus_one_open_session", "horizon": 10, "drawdown_boundary": -0.08}
CONTRACT = dict(
    version=VERSION,
    vintages=list(VINTAGES),
    target=TARGET,
    feature_names=list(ALL_CORE_FEATURES),
    parameters=risk.PARAMS,
    warning_threshold=0.20,
    planned_fits=4,
    historical_as_known=False,
    untouched_holdout=False,
)
BASE_MODEL_FIELDS = {"source_request_path", "sealed_path", "acceptance_path"}
BUNDLE_MODEL_FIELDS = BASE_MODEL_FIELDS | {"model_bundle_path", "model_bundle_sha256"}


def require(condition, message, reason="rolling_invalid"):
    core.require(condition, message, reason)


def schedule(calendar, vintage):
    require(calendar == sorted(set(calendar)) and bool(calendar), "calendar ordering differs")
    require(vintage in VINTAGES, "unapproved vintage")
    positions = {d: i for i, d in enumerate(calendar)}
    require(vintage["train_end"] in positions and vintage["effective_end"] in positions, "vintage boundaries absent")
    train = [d for d in calendar if vintage["train_start"] <= d <= vintage["train_end"]]
    predict = [d for d in calendar if vintage["effective_start"] <= d <= vintage["effective_end"]]
    require(
        train and predict and positions[train[0]] >= core.CONTEXT_OPEN_DAYS,
        "training context or prediction dates absent",
    )
    fit = [d for d in train if positions[d] + TARGET["horizon"] <= positions[vintage["train_end"]]]
    require(fit and train[-1] < predict[0], "training/prediction overlap")
    return dict(train=train, fit=fit, predict=predict, as_of={d: calendar[positions[d] - 1] for d in train + predict})


def prepare(source_request, source_sha256, output, source_commit):
    """One shared file read; isolate each vintage's training labels on disk."""
    source_request, output = Path(source_request), Path(output)
    core.reader._require_sha256(source_sha256, "source request file SHA")
    require(
        hashlib.sha256(source_request.read_bytes()).hexdigest() == source_sha256,
        "source request bytes differ",
        "identity_mismatch",
    )
    request = read_json(source_request)
    core.validate_request(request)
    require(set(request["model_assets"]) == BASE_MODEL_FIELDS, "bootstrap must be frozen M0")
    require(re.fullmatch(r"[0-9a-f]{40}", source_commit) is not None, "source commit invalid")
    source = {**request["source"], "work_parent": str(output / "source_work")}
    validate_output_location(output, dataset_root=Path(source["candidate_root"]))
    require(not output.exists(), "prepare output collision")
    frozen, parameters, model_identity = core.model_source(request)
    core.date_plan(request)  # Authenticate the complete pinned release calendar.
    calendar = (
        (Path(source["candidate_root"]) / "components/daily_bin_candidate/calendars/day.txt")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    plans = [schedule(calendar, v) for v in VINTAGES]
    decisions = sorted({d for p in plans for d in p["train"] + p["predict"]})
    predict = [d for p in plans for d in p["predict"]]
    require(len(predict) == 125 and predict == request["decision_dates"], "approved 125-day window differs")
    positions = {d: i for i, d in enumerate(calendar)}
    as_of = {d: calendar[positions[d] - 1] for d in decisions}
    data_frozen = {
        **frozen,
        "industry_authority": source["industry_authority"],
        "security_identity_sha256": source["security_identity_sha256"],
        "provider_absence_sha256": source["provider_absence_sha256"],
    }
    assets, window, aggregates, identity = core._bounded_l2_stock_facts(
        data_frozen,
        source,
        start=core._day(calendar[positions[decisions[0]] - core.CONTEXT_OPEN_DAYS]),
        end=core._day(VINTAGES[-1]["effective_end"]),
        prewarm_price_history=True,
        release_binding=source["release_binding"],
        expected_calendar_sha256=source["calendar_sha256"],
    )
    extended = receipt(
        {**{k: v for k, v in request.items() if k != "receipt_sha256"}, "source": source, "decision_dates": decisions}
    )
    features = core.features_from_aggregates(
        extended,
        source_commit,
        frozen,
        parameters,
        model_identity,
        window,
        aggregates,
        assets["benchmark"],
        as_of,
        {
            **{k: v for k, v in identity.items() if k not in {"structural_membership", "domain_reasons"}},
            "calendar_sha256": source["calendar_sha256"],
            "industry_authority_sha256": canonical_sha256(source["industry_authority"]),
            "security_identity_sha256": source["security_identity_sha256"],
            "provider_absence_sha256": source["provider_absence_sha256"],
        },
        identity["domain_reasons"],
        version=VERSION,
    )
    features = receipt(
        {k: v for k, v in features.items() if k != "receipt_sha256"}
        | {
            "contract": CONTRACT,
            "calendar": calendar,
            "source": source,
            "bootstrap_assets": request["model_assets"],
            "source_request_sha256": source_sha256,
        }
    )
    # Close the producer/child contract before publishing a prepared carrier.
    validate_features(features)
    returns = {d.isoformat(): {c: None for c in frozen["catalog"]} for d in window}
    for aggregate in aggregates:
        value = float(aggregate.l1_return)
        require(math.isfinite(value) and value > -1, "unknown industry return", "source_invalid")
        returns[aggregate.trade_date.isoformat()][aggregate.l1_code] = value
    write_once(output / "features.json", features)
    labels = {}
    for vintage, plan in zip(VINTAGES, plans):
        target = receipt(
            dict(
                schema_version=VERSION + "_train_labels",
                vintage=vintage,
                feature_sha256=features["receipt_sha256"],
                target=TARGET,
                labels=risk.drawdown_outcomes(
                    calendar, plan["train"], frozen["catalog"], returns, vintage["train_end"]
                ),
            )
        )
        name = f"train_{vintage['id']}.json"
        write_once(output / name, target)
        labels[vintage["id"]] = dict(path=str(output / name), sha256=target["receipt_sha256"])
    facts = receipt(
        dict(
            schema_version=VERSION + "_outcomes",
            feature_sha256=features["receipt_sha256"],
            returns={d: r for d, r in returns.items() if d >= VINTAGES[0]["effective_start"]},
        )
    )
    write_once(output / "outcome_facts.json", facts)
    prepared = receipt(
        dict(
            schema_version=VERSION + "_prepared",
            contract=CONTRACT,
            source_commit=source_commit,
            source_request_path=str(source_request),
            source_request_sha256=source_sha256,
            feature_path=str(output / "features.json"),
            feature_sha256=features["receipt_sha256"],
            training_labels=labels,
            outcome_path=str(output / "outcome_facts.json"),
            outcome_sha256=facts["receipt_sha256"],
            fits=0,
            database_access=False,
        )
    )
    write_once(output / "prepared.json", prepared)
    return prepared


def validate_features(features):
    core.verify_receipt(features)
    require(
        features["schema_version"] == VERSION + "_features" and features["contract"] == CONTRACT,
        "rolling feature contract differs",
    )
    codes, calendar = features["catalog"], features["calendar"]
    require(
        len(codes) == 131 and codes == sorted(set(codes)) and features["feature_names"] == list(ALL_CORE_FEATURES),
        "131-sector/20D identity differs",
    )
    plans = [schedule(calendar, v) for v in VINTAGES]
    days = sorted({d for p in plans for d in p["train"] + p["predict"]})
    positions = {d: i for i, d in enumerate(calendar)}
    core.validate_feature_rows({"decision_dates": days}, features, {d: calendar[positions[d] - 1] for d in days})
    require(
        canonical_sha256(features["parameters"]) == core.value.APPROVED_PINS["model_hash"],
        "M0 parameters differ",
        "identity_mismatch",
    )
    source, data = features["source"], features["inference_identity"]
    binding = source["release_binding"]
    require(
        isinstance(data, dict)
        and isinstance(data.get("release_identity"), dict)
        and data["release_identity"].get("frozen_release_binding_sha256") == canonical_sha256(binding)
        and data["release_identity"].get("dataset_manifest_sha256") == binding["manifest_sha256"]
        and data.get("calendar_sha256") == source["calendar_sha256"]
        and data.get("industry_authority_sha256") == canonical_sha256(source["industry_authority"])
        and data.get("security_identity_sha256") == source["security_identity_sha256"]
        and data.get("provider_absence_sha256") == source["provider_absence_sha256"],
        "feature source identity differs",
        "identity_mismatch",
    )
    return plans


def fit_vintage(features, training, vintage, *, on_fit_start=None):
    plan = schedule(features["calendar"], vintage)
    core.verify_receipt(training)
    require(
        set(training) == {"schema_version", "vintage", "feature_sha256", "target", "labels", "receipt_sha256"}
        and training["schema_version"] == VERSION + "_train_labels"
        and training["vintage"] == vintage
        and training["feature_sha256"] == features["receipt_sha256"]
        and training["target"] == TARGET
        and set(training["labels"]) == set(plan["train"]),
        "training label authority differs",
    )
    fit_days, rows = set(plan["fit"]), []
    for day in plan["train"]:
        require(set(training["labels"][day]) == set(features["catalog"]), "training label grid differs")
        for code in features["catalog"]:
            target = training["labels"][day][code]
            risk._validate_target(target, mature=day in fit_days)
            values = features["rows"][day][code]["features"]
            if day in fit_days and values is not None and target["status"] == "AVAILABLE":
                rows.append((day, code, values, target["event"]))
    if on_fit_start is not None:
        on_fit_start()
    parameters, stats = risk.fit_parameters(rows)
    predictions, inactive = risk.predictions_from_parameters(
        features["rows"], plan["predict"], features["catalog"], parameters
    )
    model = receipt(
        dict(
            schema_version=VERSION + "_model",
            contract=CONTRACT,
            vintage=vintage,
            parameters=parameters,
            model_sha256=canonical_sha256(parameters),
            target=TARGET,
            bootstrap_model_sha256=core.value.APPROVED_PINS["model_hash"],
            feature_definition_sha256=features["model_identity"]["feature_definition_sha256"],
            feature_sha256=features["receipt_sha256"],
            training_label_sha256=training["receipt_sha256"],
            training_input_identity=features["inference_identity"],
            training_pit_bundle_sha256=features["source"]["industry_authority"]["identity"]["bundle_hash"],
            training_release_binding=features["source"]["release_binding"],
            source_commit=features["source_commit"],
            catalog=features["catalog"],
            **stats,
        )
    )
    return receipt(
        dict(
            schema_version=VERSION + "_sealed",
            model=model,
            predictions=predictions,
            inactive_changed_rows=inactive,
            fits=1,
            evaluation_labels_read=False,
        )
    )


def metrics(predictions, labels):
    rows, reasons = [], Counter()
    for prediction in predictions:
        target = labels[prediction["trade_date"]][prediction["sector_code"]]
        if prediction["availability"] != "available":
            reasons[prediction["reason_code"]] += 1
        elif target["status"] != "AVAILABLE":
            reasons[target["status"]] += 1
        else:
            rows.append({**prediction, **target})
    result = risk._counts(rows)
    if rows:
        y, p = np.asarray([r["event"] for r in rows]), np.asarray([r["probability"] for r in rows])
        bounded = np.clip(p, 1e-15, 1 - 1e-15)  # Diagnostic log only; predictions are never changed.
        result.update(
            brier=float(np.mean((p - y) ** 2)),
            log_loss=float(-np.mean(y * np.log(bounded) + (1 - y) * np.log(1 - bounded))),
            auc=float(roc_auc_score(y, p)) if len(set(y)) == 2 else None,
        )
    else:
        result.update(brier=None, log_loss=None, auc=None)
    return {**result, "prediction_rows": len(predictions), "labelled_rows": len(rows), "NA_reasons": dict(reasons)}


def evaluate(features, models, facts, baseline):
    core.verify_receipt(facts)
    require(
        facts["schema_version"] == VERSION + "_outcomes" and facts["feature_sha256"] == features["receipt_sha256"],
        "outcome identity differs",
    )
    plans = [schedule(features["calendar"], v) for v in VINTAGES]
    days = [d for p in plans for d in p["predict"]]
    core.verify_receipt(baseline)
    require(
        baseline["feature_sha256"] == features["receipt_sha256"]
        and baseline["model_sha256"] == core.value.APPROVED_PINS["model_hash"],
        "baseline identity differs",
    )
    m0 = [dict(row) for row in baseline["predictions"]]
    m1 = [dict(row) for model in models for row in model["predictions"]]
    require(
        [(r["trade_date"], r["sector_code"]) for r in m0] == [(r["trade_date"], r["sector_code"]) for r in m1],
        "paired prediction grid differs",
    )
    labels = risk.drawdown_outcomes(features["calendar"], days, features["catalog"], facts["returns"], days[-1])
    segments = {}
    for name, segment_days in [("full", days)] + [(v["id"], p["predict"]) for v, p in zip(VINTAGES, plans)]:
        subset = set(segment_days)
        segments[name] = {
            arm: metrics([r for r in predictions if r["trade_date"] in subset], labels)
            for arm, predictions in (("M0", m0), ("M1", m1))
        }
        segments[name]["M1_minus_M0"] = {
            metric: segments[name]["M1"][metric] - segments[name]["M0"][metric]
            if segments[name]["M1"][metric] is not None and segments[name]["M0"][metric] is not None
            else None
            for metric in ("brier", "log_loss", "auc")
        }
    daily = []
    for day in days:
        arms = {
            arm: metrics([r for r in predictions if r["trade_date"] == day], labels)
            for arm, predictions in (("M0", m0), ("M1", m1))
        }
        daily.append(dict(trade_date=day, **arms))
    positions = {d: i for i, d in enumerate(features["calendar"])}
    for predictions, vintage_by_day in (
        (m0, {d: ("M0", core.value.APPROVED_PINS["model_hash"]) for d in days}),
        (
            m1,
            {
                d: (v["id"], model["model"]["model_sha256"])
                for v, plan, model in zip(VINTAGES, plans, models)
                for d in plan["predict"]
            },
        ),
    ):
        for row in predictions:
            day = row["trade_date"]
            row["vintage_id"], row["model_sha256"] = vintage_by_day[day]
            future = features["calendar"][positions[day] + 1 : positions[day] + 11]
            row["target_first_return_date"] = future[0] if future else None
            row["target_last_return_date"] = future[-1] if len(future) == 10 else None
    return receipt(
        dict(
            schema_version=VERSION + "_evaluation",
            target=TARGET,
            segments=segments,
            prediction_days=len(days),
            catalog_count=len(features["catalog"]),
            historical_as_known=False,
            untouched_holdout=False,
            economic_value_confirmed=False,
            automatic_promotion=False,
            daily_metrics=daily,
            M0_predictions=m0,
            M1_predictions=m1,
        )
    )


def load_model(assets, frozen, bootstrap_identity):
    require(set(assets) == BUNDLE_MODEL_FIELDS, "rolling model locators differ")
    bundle = core.inputs._asset(assets["model_bundle_path"], assets["model_bundle_sha256"])
    require(
        set(bundle)
        == {
            "schema_version",
            "contract",
            "vintage",
            "parameters",
            "model_sha256",
            "target",
            "bootstrap_model_sha256",
            "feature_definition_sha256",
            "feature_sha256",
            "training_label_sha256",
            "training_input_identity",
            "training_release_binding",
            "training_pit_bundle_sha256",
            "source_commit",
            "catalog",
            "fit_rows",
            "fit_dates",
            "sample_weight_sum",
            "receipt_sha256",
        },
        "rolling model schema fields differ",
    )
    require(
        bundle["schema_version"] == VERSION + "_model"
        and bundle["contract"] == CONTRACT
        and bundle["vintage"] in VINTAGES
        and bundle["target"] == TARGET
        and bundle["bootstrap_model_sha256"] == bootstrap_identity["model_sha256"]
        and bundle["feature_definition_sha256"] == canonical_sha256(frozen["feature_definition"])
        and bundle["catalog"] == frozen["catalog"]
        and bundle["model_sha256"] == canonical_sha256(bundle["parameters"])
        and re.fullmatch(r"[0-9a-f]{40}", bundle["source_commit"]),
        "rolling model authority differs",
        "identity_mismatch",
    )
    for field in ("feature_sha256", "training_label_sha256", "model_sha256", "training_pit_bundle_sha256"):
        core.reader._require_sha256(bundle[field], field)
    require(
        type(bundle["fit_dates"]) is int
        and type(bundle["fit_rows"]) is int
        and 0 < bundle["fit_dates"] <= bundle["fit_rows"] <= 131 * bundle["fit_dates"]
        and isinstance(bundle["sample_weight_sum"], (int, float))
        and math.isfinite(bundle["sample_weight_sum"])
        and abs(bundle["sample_weight_sum"] - bundle["fit_rows"]) < 1e-6,
        "rolling training population differs",
    )
    binding, source = bundle["training_release_binding"], bundle["training_input_identity"]["release_identity"]
    require(
        source["frozen_release_binding_sha256"] == canonical_sha256(binding)
        and source["dataset_manifest_sha256"] == binding["manifest_sha256"],
        "rolling training release identity differs",
        "identity_mismatch",
    )
    risk.predictions_from_parameters({}, [], bundle["catalog"], bundle["parameters"])
    identity = {
        k: v
        for k, v in bootstrap_identity.items()
        if k
        not in {
            "acceptance_sha256",
            "sealed_sha256",
            "source_request_file_sha256",
            "training_input_sha256",
            "training_pit_bundle_sha256",
        }
    }
    identity.update(
        {
            "model_sha256": bundle["model_sha256"],
            "bootstrap_identity": bootstrap_identity,
            "bootstrap_model_sha256": bootstrap_identity["model_sha256"],
            "model_bundle_sha256": bundle["receipt_sha256"],
            "vintage": bundle["vintage"],
            "target": TARGET,
            "training_input_sha256": canonical_sha256(bundle["training_input_identity"]),
            "training_feature_sha256": bundle["feature_sha256"],
            "training_label_sha256": bundle["training_label_sha256"],
            "training_pit_bundle_sha256": bundle["training_pit_bundle_sha256"],
            "training_release_binding": binding,
        }
    )
    return bundle["parameters"], identity


def validate_model_dates(request, identity):
    vintage = identity.get("vintage")
    if vintage:
        require(
            all(
                vintage["train_end"] < d and vintage["effective_start"] <= d <= vintage["effective_end"]
                for d in request["decision_dates"]
            ),
            "prediction outside model effective interval",
        )


def execute_child(prepared, output, source_commit):
    core.verify_receipt(prepared)
    require(
        prepared["schema_version"] == VERSION + "_prepared" and prepared["contract"] == CONTRACT,
        "prepared contract differs",
    )
    features = core.inputs._asset(prepared["feature_path"], prepared["feature_sha256"])
    require(features["source_commit"] == source_commit == prepared["source_commit"], "source commit differs")
    plans = validate_features(features)
    days = [d for plan in plans for d in plan["predict"]]
    predictions, _ = risk.predictions_from_parameters(
        features["rows"], days, features["catalog"], features["parameters"]
    )
    baseline = receipt(
        dict(
            feature_sha256=features["receipt_sha256"],
            model_sha256=core.value.APPROVED_PINS["model_hash"],
            predictions=predictions,
            fits=0,
        )
    )
    baseline_path = output / "M0.predictions.json"
    write_once(baseline_path, baseline)
    require(core.inputs._asset(str(baseline_path), baseline["receipt_sha256"]) == baseline, "M0 readback differs")
    models = []
    for vintage in VINTAGES:
        locator = prepared["training_labels"][vintage["id"]]
        training = core.inputs._asset(locator["path"], locator["sha256"])
        sealed = fit_vintage(
            features,
            training,
            vintage,
            on_fit_start=lambda: write_once(
                output / f"{vintage['id']}.attempt.json", receipt(dict(vintage=vintage, fit_kernel_attempts=1))
            ),
        )
        path = output / f"{vintage['id']}.sealed.json"
        write_once(path, sealed)
        require(core.inputs._asset(str(path), sealed["receipt_sha256"]) == sealed, "sealed readback differs")
        models.append(sealed)
    # Q2 is sealed before Q3 training labels are opened; all predictions precede evaluation.
    facts = core.inputs._asset(prepared["outcome_path"], prepared["outcome_sha256"])
    evaluation = evaluate(features, models, facts, baseline)
    result = receipt(
        dict(
            schema_version=VERSION + "_repeat",
            contract=CONTRACT,
            source_commit=source_commit,
            prepared_sha256=prepared["receipt_sha256"],
            fits=2,
            model_bundles=[m["model"] for m in models],
            evaluation=evaluation,
            database_access=False,
            runtime_action=False,
        )
    )
    write_once(output / "result.json", result)
    return result
