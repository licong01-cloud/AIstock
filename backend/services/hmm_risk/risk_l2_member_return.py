"""Approved single signed-member target, matched C controls, ten formal fits.

Only frozen files; quarter-local training packets never contain later labels.
No trading policy, dataset mutation, database fallback, or automatic refit.
"""

from __future__ import annotations

from collections import Counter
import math
from pathlib import Path
import re

import numpy as np
from scipy.stats import rankdata

from backend.services.hmm_risk import risk_l2_member_release as release
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_json_bytes, canonical_sha256
from backend.services.hmm_risk.formal_state_executor import THREAD_VARIABLES, validate_output_location, write_once
from backend.services.hmm_risk.formal_state_model import receipt

member, core, require = release.member, release.core, release.require
loss = member.loss
VERSION = "hmm_l2_signed_member_return_ridge_v1"
VINTAGES = (*member.rolling.VINTAGES, release.Q4)
PINS = dict(
    manifest="772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d",
    manifest_file="2242c44261dc0eaaf54cda3416a260cf6712ec960cff8bfbb46a527f6587b958",
    prepared="203e4beaf175e453953bd81d860c2af9031265d2589347fb8ba971663c8b6929",
    features="78472f692dac2cdfea27553b7d51e4ff053f046f969bfd24c88974be78a5c5ee",
    training="de3ce3723462beeb11e6fcc13abc640664772f9891f49427c7a90b20a84a5516",
    mask="a774fd8382d5369e2094ba1fb96be716b7d90f6d30414dc56ae847f6ab387937",
    feature_definition="c9c197c189c580cf203332944d160b5c88fe414ca1145ef1e6d5a8221a25dccd",
    control_file="602a0027536f33414dbc31575130e0c4bb56da9038872721108cbde77ed9f449",
    control_model="706719ca85bbca99fc1e9e8294974cdbc8fe9dcc794ef708f04180c213347f7b",
    control_acceptance_file="b7faf857c082e836e7611a354a140ad944a70c150875be38fd12c42c7063b6ef",
    control_acceptance="8342abe3b6c65b0a2e61e4a91cd1303174b04feaa62c25ddecea0f2fbaae52a6",
    cohort_file="2a367e0f82bf540a5ab18e4863d97c6bef0e2413f78ba193e0c23531de7a0f7a",
    cohort="9058b93b4bf26ad68c45a4217508e99c9cffbf5e3250c03c84ce6c9d86529997",
)
CONTRACT = dict(
    version=VERSION,
    vintages=list(VINTAGES),
    parameters=loss.PARAMS,
    feature_names=list(ALL_CORE_FEATURES),
    target="S=upside-C=sum(fixed_asof_circ_mv_weight*R)",
    price_basis=member.TARGET["price_basis"],
    horizon=10,
    anchor="decision_close",
    prediction_start="2026-04-01",
    prediction_end="2026-10-30",
    planned_fits=10,
    numeric_versions=loss.EXPECTED_VERSIONS,
    source_pins=PINS,
    historical_as_known=False,
    untouched_holdout=False,
    policy_applied=False,
)
REQUEST_FIELDS = {
    "schema_version",
    "contract",
    "source_request_path",
    "source_request_sha256",
    "q4_prepared_path",
    "q4_control_path",
    "q4_acceptance_path",
    "cohort_path",
}


def validate_numeric(numeric):
    require(
        numeric["versions"] == loss.EXPECTED_VERSIONS
        and numeric["thread_variables"] == dict.fromkeys(THREAD_VARIABLES, "1")
        and numeric["thread_pools"]
        and all(p["num_threads"] == 1 for p in numeric["thread_pools"]),
        "fixed numeric environment differs",
    )


def schedule(calendar, vintage):
    require(vintage in VINTAGES and calendar == sorted(set(calendar)), "quarter/calendar differs")
    positions = {d: i for i, d in enumerate(calendar)}
    end = min(vintage["effective_end"], CONTRACT["prediction_end"])
    require(vintage["train_end"] in positions and end in positions, "quarter boundary absent")
    train = [d for d in calendar if vintage["train_start"] <= d <= vintage["train_end"]]
    predict = [d for d in calendar if vintage["effective_start"] <= d <= end]
    require(train and predict and positions[train[0]] >= core.CONTEXT_OPEN_DAYS, "training context absent")
    return dict(train=train, fit=member.target_dates(calendar, train, vintage["train_end"])["mature"], predict=predict)


def validate_cached_source(current, cached):
    """An explicit same-content locator is not a new provider authority."""
    locator_fields = {"work_parent", "provider_absence_manifest"}
    require(
        {k: v for k, v in current.items() if k not in locator_fields}
        == {k: v for k, v in cached.items() if k not in locator_fields},
        "cached source identity differs",
        "identity_mismatch",
    )
    # Authenticate the explicitly supplied file, never discover/fallback to a path.
    member.provider_path(current["provider_absence_manifest"], cached["provider_absence_sha256"])


def load_request(path, expected):
    request = member.pinned_json(path, expected)
    require(
        set(request) == REQUEST_FIELDS
        and request["schema_version"] == VERSION + "_request"
        and request["contract"] == CONTRACT,
        "unapproved request",
    )
    original = member.pinned_json(request["source_request_path"], request["source_request_sha256"])
    core.validate_request(original)
    cached, features, training, _ = release.load_prepared(request["q4_prepared_path"], PINS["prepared"])
    require(
        cached["feature_sha256"] == PINS["features"]
        and cached["training_sha256"] == PINS["training"]
        and cached["common_mask_sha256"] == PINS["mask"],
        "cached training identity differs",
    )
    binding = original["source"]["release_binding"]
    require(
        binding == cached["release_binding"]
        and binding["manifest_sha256"] == PINS["manifest"]
        and binding["manifest_file_sha256"] == PINS["manifest_file"]
        and binding["cutoff"] == CONTRACT["prediction_end"],
        "release differs",
        "identity_mismatch",
    )

    validate_cached_source(original["source"], features["source"])
    require(
        features["model_identity"]["feature_definition_sha256"] == PINS["feature_definition"],
        "cached authority or feature definition differs",
        "identity_mismatch",
    )
    boundary = release.source_preflight(original)
    require(boundary["status"] == "BOUNDARY_PASS", "authority boundary blocked")
    sealed = member.pinned_json(request["q4_control_path"], PINS["control_file"])
    acceptance = member.pinned_json(request["q4_acceptance_path"], PINS["control_acceptance_file"])
    core.verify_receipt(sealed)
    core.verify_receipt(acceptance, PINS["control_acceptance"])
    old_process = core.inputs._asset(
        str(Path(request["q4_control_path"]).parent / "process.json"), acceptance["process_sha256"]
    )
    validate_numeric(old_process["numeric_environment"])
    require(old_process["sealed"]["C"] == sealed, "Q4 numerical receipt/model differs")
    control = sealed["model"]
    release.validate_model(control)
    require(
        control["arm"] == "C"
        and control["model_sha256"] == PINS["control_model"]
        and control["training_mask_sha256"] == PINS["mask"]
        and control["training_label_sha256"] == PINS["training"]
        and control["feature_sha256"] == PINS["features"]
        and control["release_binding"] == binding
        and acceptance["completed_fits"] == 4
        and acceptance["status"] == "COMPLETED",
        "Q4 control identity differs",
        "CONTROL_IDENTITY_MISMATCH",
    )
    return request, original, cached, features, training, control


def prefix_inputs(original, cached_features, output, head):
    """Build only the uncached Q2/Q3 prefix with the existing formal reader."""
    calendar, catalog = cached_features["calendar"], cached_features["catalog"]
    positions = {d: i for i, d in enumerate(calendar)}
    prefix = [d for d in calendar if VINTAGES[0]["train_start"] <= d < release.Q4["train_start"]]
    require(prefix and not set(prefix) & set(cached_features["rows"]), "cache/prefix overlap")
    source = {**original["source"], "work_parent": str(output / "source_work")}
    frozen, parameters, bootstrap = core.model_source(original)
    as_of = {d: calendar[positions[d] - 1] for d in prefix}
    data_frozen = {
        **frozen,
        **{k: source[k] for k in ("industry_authority", "security_identity_sha256", "provider_absence_sha256")},
    }
    assets, window, aggregates, identity = core._bounded_l2_stock_facts(
        data_frozen,
        source,
        start=core._day(calendar[positions[prefix[0]] - core.CONTEXT_OPEN_DAYS]),
        end=core._day(calendar[positions[prefix[-1]] + 10]),
        prewarm_price_history=True,
        release_binding=source["release_binding"],
        expected_calendar_sha256=source["calendar_sha256"],
    )
    extended = receipt(
        {k: v for k, v in original.items() if k != "receipt_sha256"} | dict(source=source, decision_dates=prefix)
    )
    features = core.features_from_aggregates(
        extended,
        head,
        frozen,
        parameters,
        bootstrap,
        window,
        aggregates,
        assets["benchmark"],
        as_of,
        {k: v for k, v in identity.items() if k not in {"structural_membership", "domain_reasons"}},
        identity["domain_reasons"],
        version=VERSION,
    )
    core.validate_feature_rows(dict(decision_dates=prefix), features, as_of)
    returns = {d.isoformat(): dict.fromkeys(catalog) for d in window}
    for item in aggregates:
        returns[item.trade_date.isoformat()][item.l1_code] = float(item.l1_return)
    statuses = member.rolling.risk.drawdown_outcomes(calendar, prefix, catalog, returns, VINTAGES[0]["train_end"])
    return features, {d: {c: statuses[d][c]["status"] for c in catalog} for d in prefix}


def signed_label(label):
    b, c, u = (member._number(label[k]) for k in ("B", "C", "upside"))
    require(0 <= b < 1 and 0 <= c < 1 and u >= 0, "invalid member target")
    signed = u - c
    require(
        math.isfinite(signed) and signed > -1 and math.isclose(b, max(0.0, -signed), abs_tol=1e-12),
        "member target algebra differs",
    )
    return signed, c


def packet_rows(features, labels, original_status, vintage):
    plan = schedule(features["calendar"], vintage)
    fit = set(plan["fit"])
    rows, excluded = [], Counter()
    for day in plan["train"]:
        if day not in fit:  # Do not consult the later-quarter cache's label.
            excluded["OUTCOME_NOT_MATURE"] += len(features["catalog"])
            continue
        require(set(labels[day]) == set(original_status[day]) == set(features["catalog"]), "label grid differs")
        for code in features["catalog"]:
            label = labels[day][code]
            if label["status"] == "UNAVAILABLE":
                require(
                    label["reason"] in {"NO_ASOF_MEMBER", "FIXED_COHORT_ENDPOINT_NA"}
                    and all(label[k] is None for k in ("B", "C", "upside")),
                    "unknown target absence",
                )
                excluded[label["reason"]] += 1
                continue
            require(label["status"] == "AVAILABLE", "mature target missing")
            s, c = signed_label(label)
            state = original_status[day][code]
            require(state in {"AVAILABLE", "OUTCOME_LEGAL_NA"}, "unknown original status")
            vector = features["rows"][day][code]["features"]
            if state != "AVAILABLE" or vector is None:
                excluded["ORIGINAL_COMMON_MASK_EXCLUDED"] += 1
                continue
            rows.append([day, code, vector, s, c])
    require(rows, "empty common training mask", "train_label_evidence_insufficient")
    return rows, dict(excluded)


def validate_packet(packet):
    core.verify_receipt(packet)
    require(
        packet["schema_version"] == VERSION + "_train" and packet["contract"] == CONTRACT,
        "training packet contract differs",
    )
    vintage, calendar, catalog = packet["vintage"], packet["calendar"], packet["catalog"]
    require(catalog == sorted(set(catalog)) and len(catalog) == 131, "training catalog differs")
    plan = schedule(calendar, vintage)
    fit_days = set(plan["fit"])
    keys = []
    for row in packet["rows"]:
        require(len(row) == 5, "training row shape differs")
        day, code, vector, s, c = row
        require(
            day in fit_days
            and code in catalog
            and len(vector) == 20
            and all(type(x) in (int, float) and math.isfinite(x) for x in [*vector, s, c])
            and s > -1
            and 0 <= c < 1,
            "training maturity/values differ",
        )
        keys.append([day, code])
    require(
        keys
        and keys == sorted(keys)
        and len({tuple(k) for k in keys}) == len(keys)
        and canonical_sha256(keys) == packet["mask_sha256"],
        "training common mask differs",
    )
    require(
        packet["release_binding"]["manifest_sha256"] == PINS["manifest"]
        and packet["feature_definition_sha256"] == PINS["feature_definition"]
        and re.fullmatch(r"[0-9a-f]{40}", packet["source_commit"]) is not None
        and all(type(v) is int and v >= 0 for v in packet["excluded"].values())
        and len(keys) + sum(packet["excluded"].values()) == 131 * len(plan["train"]),
        "training source/count differs",
    )
    return packet["rows"]


def validate_q4_control(packet, control):
    release.validate_model(control)
    rows = validate_packet(packet)
    _, _, _, scaler = loss._ridge_inputs([r[:3] + [r[4]] for r in rows])
    require(
        packet["vintage"] == release.Q4
        and packet["mask_sha256"] == PINS["mask"]
        and control["model_sha256"] == PINS["control_model"]
        and control["fit_rows"] == len(rows)
        and control["fit_dates"] == len({r[0] for r in rows})
        and all(control["parameters"][k] == scaler[k] for k in scaler)
        and packet["feature_sha256"] == control["feature_sha256"] == PINS["features"]
        and packet["target_facts_sha256"] == control["training_label_sha256"] == PINS["training"]
        and packet["release_binding"] == control["release_binding"],
        "Q4 control is not identical; no extra fits",
        "CONTROL_IDENTITY_MISMATCH",
    )


def prepare(request_path, request_sha256, output, head):
    request, original, cached, features, training, control = load_request(request_path, request_sha256)
    output = validate_output_location(Path(output), dataset_root=Path(original["source"]["candidate_root"]))
    require(not output.exists(), "prepare destination exists")
    prefix, prefix_status = prefix_inputs(original, features, output, head)
    combined = {
        **features,
        "rows": {**prefix["rows"], **features["rows"]},
        "as_of": {**prefix["as_of"], **features["as_of"]},
    }
    facts = member.FrozenMemberSource(
        combined, request, provider_absence_manifest=Path(original["source"]["provider_absence_manifest"])
    )
    labels = {**facts.targets(sorted(prefix["rows"]), VINTAGES[0]["train_end"]), **training["labels"]}
    states = {**prefix_status, **training["original_status"]}
    packets = {}
    for vintage in VINTAGES:
        rows, excluded = packet_rows(combined, labels, states, vintage)
        q4 = vintage == release.Q4
        packet = receipt(
            dict(
                schema_version=VERSION + "_train",
                contract=CONTRACT,
                vintage=vintage,
                calendar=features["calendar"],
                catalog=features["catalog"],
                rows=rows,
                excluded=excluded,
                mask_sha256=canonical_sha256([r[:2] for r in rows]),
                feature_sha256=PINS["features"] if q4 else canonical_sha256([r[:3] for r in rows]),
                target_facts_sha256=PINS["training"] if q4 else canonical_sha256([r[:2] + r[3:] for r in rows]),
                feature_definition_sha256=PINS["feature_definition"],
                release_binding=cached["release_binding"],
                source_commit=head,
            )
        )
        validate_packet(packet)
        if q4:
            validate_q4_control(packet, control)
        path = output / (vintage["id"] + ".train.json")
        write_once(path, packet)
        packets[vintage["id"]] = dict(path=str(path), sha256=packet["receipt_sha256"])
    facts.unchanged()
    # Forecast-only carrier: no TRAIN labels in either prediction process.
    days = features["prediction_dates"]
    prediction = receipt(
        dict(
            schema_version=VERSION + "_features",
            contract=CONTRACT,
            catalog=features["catalog"],
            calendar=features["calendar"],
            prediction_dates=days,
            as_of={d: features["as_of"][d] for d in days},
            rows={d: features["rows"][d] for d in days},
            cached_features_sha256=PINS["features"],
            release_binding=cached["release_binding"],
            inference_identity=features["inference_identity"],
        )
    )
    validate_prediction_features(prediction)
    write_once(output / "prediction_features.json", prediction)
    write_once(output / "q4_control.json", control)
    result = receipt(
        dict(
            schema_version=VERSION + "_prepared",
            contract=CONTRACT,
            source_commit=head,
            request_path=str(request_path),
            request_sha256=request_sha256,
            packets=packets,
            prediction_features_path=str(output / "prediction_features.json"),
            prediction_features_sha256=prediction["receipt_sha256"],
            control_path=str(output / "q4_control.json"),
            control_sha256=control["receipt_sha256"],
            cached_prepared_sha256=cached["receipt_sha256"],
            prefix_feature_sha256=prefix["receipt_sha256"],
            prefix_source_identity=facts.identity(),
            fits=0,
            evaluation_outcomes_read=False,
            database_access=False,
            dataset_write=False,
        )
    )
    write_once(output / "prepared.json", result)
    return result


def load_prepared(path, expected):
    p = core.inputs._asset(str(path), expected)
    require(
        p["schema_version"] == VERSION + "_prepared"
        and p["contract"] == CONTRACT
        and p["fits"] == 0
        and p["evaluation_outcomes_read"] is False
        and set(p["packets"]) == {v["id"] for v in VINTAGES},
        "prepared differs",
    )
    packets = {k: core.inputs._asset(v["path"], v["sha256"]) for k, v in p["packets"].items()}
    for key, packet in packets.items():
        validate_packet(packet)
        require(
            packet["vintage"]["id"] == key and packet["source_commit"] == p["source_commit"],
            "quarter packet/source swapped",
        )
    control = core.inputs._asset(p["control_path"], p["control_sha256"])
    validate_q4_control(packets["2026Q4"], control)
    features = core.inputs._asset(p["prediction_features_path"], p["prediction_features_sha256"])
    validate_prediction_features(features)
    return p, packets, control, features


def fit_packet(packet, control, output, attempt_root, head, numeric):
    validate_numeric(numeric)
    rows = validate_packet(packet)
    q4 = packet["vintage"] == release.Q4
    if q4:
        validate_q4_control(packet, control)
    models = {}
    for arm, column in (("S", 3), ("C", 4)):
        if q4 and arm == "C":
            models[arm] = control
            continue
        write_once(
            Path(attempt_root) / (packet["vintage"]["id"] + "." + arm + ".json"),
            receipt(dict(packet_sha256=packet["receipt_sha256"], arm=arm, fit_kernel_attempts=1)),
        )
        target_rows = [r[:3] + [r[column]] for r in rows]
        params, stats = loss._fit_weighted_ridge(target_rows) if arm == "S" else loss.fit_parameters(target_rows)
        if arm == "C":
            stats["training_target_mean"] = stats.pop("training_loss_mean")
        model = receipt(
            dict(
                schema_version=VERSION + "_model",
                contract=CONTRACT,
                arm=arm,
                vintage=packet["vintage"],
                parameters=params,
                model_sha256=canonical_sha256(params),
                catalog=packet["catalog"],
                packet_sha256=packet["receipt_sha256"],
                mask_sha256=packet["mask_sha256"],
                feature_sha256=packet["feature_sha256"],
                target_facts_sha256=packet["target_facts_sha256"],
                release_binding=packet["release_binding"],
                source_commit=head,
                numeric_environment=numeric,
                **stats,
            )
        )
        validate_model(model)
        models[arm] = model
    require(
        all(models["S"]["parameters"][k] == models["C"]["parameters"][k] for k in ("mean", "scale", "active")),
        "matched scaler differs",
    )
    result = receipt(
        dict(
            schema_version=VERSION + "_fit",
            contract=CONTRACT,
            models=models,
            source_commit=head,
            packet_sha256=packet["receipt_sha256"],
            fits=1 if q4 else 2,
            evaluation_outcomes_read=False,
            numeric_environment=numeric,
        )
    )
    write_once(Path(output) / "models.json", result)
    return result


def validate_model(model):
    if model.get("schema_version") == release.VERSION + "_model":
        release.validate_model(model)
        require(model["arm"] == "C" and model["model_sha256"] == PINS["control_model"], "wrong reused model")
        return
    core.verify_receipt(model)
    require(
        model["schema_version"] == VERSION + "_model"
        and model["contract"] == CONTRACT
        and model["arm"] in {"S", "C"}
        and model["vintage"] in VINTAGES
        and not (model["arm"] == "C" and model["vintage"] == release.Q4)
        and model["model_sha256"] == canonical_sha256(model["parameters"])
        and model["release_binding"]["manifest_sha256"] == PINS["manifest"]
        and len(model["catalog"]) == 131
        and model["catalog"] == sorted(set(model["catalog"]))
        and re.fullmatch(r"[0-9a-f]{40}", model["source_commit"]) is not None,
        "model contract differs",
    )
    loss.parameter_arrays(model["parameters"])
    validate_numeric(model["numeric_environment"])


def validate_prediction_features(features):
    core.verify_receipt(features)
    require(
        set(features)
        == {
            "schema_version",
            "contract",
            "catalog",
            "calendar",
            "prediction_dates",
            "as_of",
            "rows",
            "cached_features_sha256",
            "release_binding",
            "inference_identity",
            "receipt_sha256",
        }
        and features["schema_version"] == VERSION + "_features"
        and features["contract"] == CONTRACT
        and features["cached_features_sha256"] == PINS["features"]
        and features["release_binding"]["manifest_sha256"] == PINS["manifest"],
        "prediction carrier differs",
    )
    calendar = features["calendar"]
    days = [d for d in calendar if CONTRACT["prediction_start"] <= d <= CONTRACT["prediction_end"]]
    require(
        calendar == sorted(set(calendar))
        and features["prediction_dates"] == days
        and len(days) == 142
        and len(features["catalog"]) == 131
        and features["catalog"] == sorted(set(features["catalog"])),
        "full prediction denominator differs",
    )
    as_of = {d: calendar[calendar.index(d) - 1] for d in days}
    require(features["as_of"] == as_of, "prediction as-of differs")
    core.validate_feature_rows(dict(decision_dates=days), features, as_of)


def predict_model(model, features):
    validate_model(model)
    validate_prediction_features(features)
    vintage = model["vintage"]
    days = schedule(features["calendar"], vintage)["predict"]
    if model["arm"] == "C":
        rows, _ = member.predict_parameters(
            model, {d: features["rows"][d] for d in days}, days, features["as_of"], features["calendar"]
        )
        return rows
    mean, scale, active, coef, intercept = loss.parameter_arrays(model["parameters"])
    output = []
    for day in days:
        require(day > vintage["train_end"], "prediction leaks training interval")
        at = features["calendar"].index(day)
        future = features["calendar"][at + 1 : at + 11]
        for code in features["catalog"]:
            item = features["rows"][day][code]
            value = None
            if item["features"] is not None:
                z = (np.asarray(item["features"], dtype=float)[active] - mean[active]) / scale[active]
                value = float(z @ coef + intercept)
                require(math.isfinite(value) and np.isfinite(z).all(), "nonfinite prediction")
            output.append(
                dict(
                    trade_date=day,
                    as_of_date=features["as_of"][day],
                    sector_code=code,
                    vintage_id=vintage["id"],
                    model_sha256=model["model_sha256"],
                    expected_member_return=value,
                    availability="available" if value is not None else "unavailable",
                    reason_code=item["reason_code"],
                    target_first_return_date=future[0] if future else None,
                    target_last_return_date=future[-1] if len(future) == 10 else None,
                )
            )
    return output


def close_fits(processes, packets, head):
    require(len(processes) == 2, "two fresh processes per quarter required")
    for group in processes:
        require(set(group) == set(packets), "missing quarter result")
        for key, run in group.items():
            core.verify_receipt(run)
            packet = packets[key]
            require(
                run["contract"] == CONTRACT
                and run["source_commit"] == head
                and run["packet_sha256"] == packet["receipt_sha256"]
                and run["fits"] == (1 if key == "2026Q4" else 2)
                and run["evaluation_outcomes_read"] is False
                and run["numeric_environment"]["versions"] == loss.EXPECTED_VERSIONS
                and set(run["models"]) == {"S", "C"},
                "fit result source/budget differs",
            )
            for arm, model in run["models"].items():
                validate_model(model)
                require(model["arm"] == arm and model["vintage"] == packet["vintage"], "model swapped")
                if key == "2026Q4" and arm == "C":
                    validate_q4_control(packet, model)
                else:
                    require(
                        model["packet_sha256"] == packet["receipt_sha256"]
                        and model["source_commit"] == head
                        and model["mask_sha256"] == packet["mask_sha256"],
                        "model training identity differs",
                    )
            require(
                all(
                    run["models"]["S"]["parameters"][k] == run["models"]["C"]["parameters"][k]
                    for k in ("mean", "scale", "active")
                ),
                "control scaler differs",
            )
    require(canonical_json_bytes(processes[0]) == canonical_json_bytes(processes[1]), "fresh fits differ")
    return receipt(
        dict(
            schema_version=VERSION + "_fit_acceptance",
            contract=CONTRACT,
            source_commit=head,
            status="COMPLETED",
            completed_fits=10,
            process_sha256=canonical_sha256(processes[0]),
            models={k: v["models"] for k, v in processes[0].items()},
            automatic_promotion=False,
        )
    )


def predict_collection(acceptance, features):
    core.verify_receipt(acceptance)
    require(
        acceptance["schema_version"] == VERSION + "_fit_acceptance"
        and acceptance["contract"] == CONTRACT
        and acceptance["completed_fits"] == 10
        and acceptance["status"] == "COMPLETED",
        "fit acceptance differs",
    )
    validate_prediction_features(features)
    output = {arm: [] for arm in ("S", "C")}
    for vintage in VINTAGES:
        for arm in output:
            model = acceptance["models"][vintage["id"]][arm]
            require(model["arm"] == arm and model["vintage"] == vintage, "prediction model quarter differs")
            output[arm].extend(predict_model(model, features))
    return receipt(
        dict(
            schema_version=VERSION + "_predictions",
            contract=CONTRACT,
            fit_acceptance_sha256=acceptance["receipt_sha256"],
            features_sha256=features["receipt_sha256"],
            release_binding=features["release_binding"],
            predictions=output,
            fits=0,
            evaluation_outcomes_read=False,
            policy_applied=False,
        )
    )


def rank_ic(scores, outcomes):
    if len(scores) < 2 or len(set(scores)) < 2 or len(set(outcomes)) < 2:
        return None
    return float(np.corrcoef(rankdata(scores), rankdata(outcomes))[0, 1])


def hac(values, calendar):
    values = {d: float(x) for d, x in values.items() if x is not None}
    if len(values) < 2:
        return dict(n=len(values), mean=None, se=None, interval=None, status="INSUFFICIENT_DATES")
    require(all(math.isfinite(x) for x in values.values()), "nonfinite statistic")
    n = len(values)
    mean = math.fsum(values.values()) / n
    centered = {calendar.index(d): x - mean for d, x in values.items()}
    variance = (
        math.fsum(x * x for x in centered.values())
        + 2
        * math.fsum(
            (1 - j / 10) * math.fsum(x * centered[i - j] for i, x in centered.items() if i - j in centered)
            for j in range(1, 10)
        )
    ) / n**2
    if variance < 0:
        return dict(n=n, mean=mean, variance=variance, se=None, interval=None, status="NUMERIC_UNDEFINED")
    se = math.sqrt(variance)
    return dict(
        n=n,
        mean=mean,
        variance=variance,
        se=se,
        interval=[mean - 1.959963984540054 * se, mean + 1.959963984540054 * se],
        status="DEFINED",
    )


def extreme_mean(scores, outcomes, *, high):
    require(len(scores) == len(outcomes) > 0, "empty extreme group")
    k = math.ceil(0.20 * len(scores))
    remaining, total = float(k), 0.0
    for score in sorted(set(scores), reverse=high):
        indices = [i for i, s in enumerate(scores) if s == score]
        weight = min(remaining, len(indices)) / len(indices)
        total += weight * math.fsum(outcomes[i] for i in indices)
        remaining -= weight * len(indices)
        if remaining <= 0:
            break
    return total / k


def decision(l2, cohort):
    s, delta = l2["S"]["mean"], l2["delta"]["mean"]
    if s is None or delta is None:
        return "INSUFFICIENT_EVALUABLE_EVIDENCE"
    if s <= 0 or delta <= 0:
        return "STOP_NO_INCREMENTAL_ORDERING"
    s, delta = cohort["S"]["mean"], cohort["delta"]["mean"]
    if s is None or delta is None:
        return "INCONCLUSIVE_TRANSFER"
    if s <= 0 or delta <= 0:
        return "STOP_NO_TRANSFER_LEAD"
    return "RESEARCH_LEAD_REQUIRES_VALUE_REVIEW"


def summarize_daily(daily, calendar):
    def summary(days):
        paired = [d for d in days if daily[d]["delta"] is not None]
        result = {key: hac({d: daily[d].get(key) for d in paired}, calendar) for key in ("S", "C", "delta", "momentum")}
        result["coverage"] = dict(
            requested_dates=len(days),
            paired_dates=len(paired),
            undefined_dates=[d for d in days if daily[d]["delta"] is None],
        )
        return result

    return dict(
        full=summary(daily),
        quarters={
            v["id"]: summary([d for d in daily if v["effective_start"] <= d <= v["effective_end"]]) for v in VINTAGES
        },
    )


def score_outcomes(prediction, labels, features, acceptance, cohort):
    """Pure post-seal metrics. No selection of dates, outcomes, or formula."""
    expected = [(d, c) for d in features["prediction_dates"] for c in features["catalog"]]
    maps = {}
    for arm, field in (("S", "expected_member_return"), ("C", "expected_member_loss")):
        items = prediction["predictions"][arm]
        require([(r["trade_date"], r["sector_code"]) for r in items] == expected, "prediction grid differs")
        maps[arm] = {(r["trade_date"], r["sector_code"]): r[field] for r in items}
    daily, groups, counts, errors = {}, {}, Counter(), {}
    require(set(labels) == set(features["prediction_dates"]), "evaluation dates missing")
    for day in features["prediction_dates"]:
        require(set(labels[day]) == set(features["catalog"]), "evaluation sectors missing")
        outcomes, scores, controls, momentum, downs, ups = [], [], [], [], [], []
        for code in features["catalog"]:
            label = labels[day][code]
            counts[label["status"]] += 1
            if label["status"] != "AVAILABLE":
                require(
                    (label["status"], label["reason"])
                    in {
                        ("UNAVAILABLE", "NO_ASOF_MEMBER"),
                        ("UNAVAILABLE", "FIXED_COHORT_ENDPOINT_NA"),
                        ("OUTCOME_NOT_MATURE", "OUTCOME_NOT_MATURE"),
                    }
                    and all(label[k] is None for k in ("B", "C", "upside")),
                    "unknown evaluation absence",
                )
                counts[label["reason"] + "_reason"] += 1
                continue
            signed, c = signed_label(label)
            s, control = maps["S"][(day, code)], maps["C"][(day, code)]
            if s is None or control is None:
                counts["PREDICTION_UNAVAILABLE"] += 1
                continue
            outcomes.append(signed)
            scores.append(s)
            controls.append(-control)
            momentum.append(features["rows"][day][code]["features"][ALL_CORE_FEATURES.index("excess_return_Nd")])
            downs.append(c)
            ups.append(label["upside"])
        a, b = rank_ic(scores, outcomes), rank_ic(controls, outcomes)
        daily[day] = dict(
            S=a,
            C=b,
            delta=a - b if a is not None and b is not None else None,
            momentum=rank_ic(momentum, outcomes),
            n=len(outcomes),
        )
        if outcomes:
            vintage = next(v for v in VINTAGES if v["effective_start"] <= day <= v["effective_end"])
            base = acceptance["models"][vintage["id"]]["S"]["training_target_mean"]
            errors[day] = dict(
                mse=float(np.mean((np.array(scores) - outcomes) ** 2)),
                mae=float(np.mean(np.abs(np.array(scores) - outcomes))),
                baseline_mse=float(np.mean((base - np.array(outcomes)) ** 2)),
                baseline_mae=float(np.mean(np.abs(base - np.array(outcomes)))),
            )
            groups[day] = {
                arm: {
                    side: {
                        name: extreme_mean(values, ys, high=side == "high")
                        for name, ys in (("signed_return", outcomes), ("downside", downs), ("upside", ups))
                    }
                    for side in ("high", "low")
                }
                for arm, values in (("S", scores), ("C", controls))
            }
    unique = {}
    for row in cohort["rows"]:
        key = (row["date"], row["stock"])
        facts = (row["code"], row["stock_return"], row["reason"])
        require(key not in unique or unique[key] == facts, "cross-seed fact conflict")
        unique[key] = facts

    def cohort_metrics(selected):
        days, counts = {}, Counter()
        for day in sorted({d for d, _ in selected}):
            xs, cs, ys = [], [], []
            require(day in features["prediction_dates"], "cohort outside requested dates")
            for (d, _), (code, outcome, reason) in selected.items():
                if d != day:
                    continue
                if outcome is None:
                    require(isinstance(reason, str) and reason, "untyped missing cohort fact")
                    counts[reason] += 1
                    continue
                require(code in features["catalog"] and math.isfinite(outcome), "unknown cohort industry/outcome")
                s, c = maps["S"][(day, code)], maps["C"][(day, code)]
                if s is None or c is None:
                    counts["PREDICTION_UNAVAILABLE"] += 1
                    continue
                xs.append(s)
                cs.append(-c)
                ys.append(outcome)
            a, b = rank_ic(xs, ys), rank_ic(cs, ys)
            days[day] = dict(S=a, C=b, delta=a - b if a is not None and b is not None else None, n=len(ys))
        return days, dict(counts)

    cohort_days, cohort_counts = cohort_metrics(unique)
    seed_diagnostics = {}
    for seed in sorted({r["seed"] for r in cohort["rows"]}):
        selected = {
            (r["date"], r["stock"]): unique[(r["date"], r["stock"])] for r in cohort["rows"] if r["seed"] == seed
        }
        days, na = cohort_metrics(selected)
        seed_diagnostics[str(seed)] = dict(
            stats=summarize_daily(days, features["calendar"]), na=na, independent_sample=False
        )

    def auxiliary(days):
        error_days = [d for d in days if d in errors]
        error_summary = {
            k: math.fsum(errors[d][k] for d in error_days) / len(error_days) if error_days else None
            for k in ("mse", "mae", "baseline_mse", "baseline_mae")
        }
        group_days = [d for d in days if d in groups]
        group_summary = {
            arm: {
                side: {
                    name: math.fsum(groups[d][arm][side][name] for d in group_days) / len(group_days)
                    if group_days
                    else None
                    for name in ("signed_return", "downside", "upside")
                }
                for side in ("high", "low")
            }
            for arm in ("S", "C")
        }
        spreads = {
            arm: {
                d: groups[d][arm]["high"]["signed_return"] - groups[d][arm]["low"]["signed_return"] for d in group_days
            }
            for arm in ("S", "C")
        }
        spread_stats = {arm: hac(series, features["calendar"]) for arm, series in spreads.items()}
        spread_stats["delta"] = hac({d: spreads["S"][d] - spreads["C"][d] for d in group_days}, features["calendar"])
        return dict(
            errors=error_summary,
            groups=group_summary,
            spread=spread_stats,
            dates=len(error_days),
            overlapping_reference_not_nav=True,
        )

    prediction_values = [
        r["expected_member_return"] for r in prediction["predictions"]["S"] if r["expected_member_return"] is not None
    ]
    physical = dict(
        count=len(prediction_values),
        below_or_equal_minus_one=sum(v <= -1 for v in prediction_values),
        min=min(prediction_values) if prediction_values else None,
        max=max(prediction_values) if prediction_values else None,
        upper_bound=None,
        clipped=False,
    )
    # No seed contributes a second independent date observation.
    for day in cohort_days:
        require(day in features["prediction_dates"], "cohort outside requested dates")
    l2_stats, transfer = (
        summarize_daily(daily, features["calendar"]),
        summarize_daily(cohort_days, features["calendar"]),
    )
    return receipt(
        dict(
            schema_version=VERSION + "_evaluation",
            contract=CONTRACT,
            prediction_sha256=prediction["receipt_sha256"],
            cohort_sha256=PINS["cohort"],
            status=decision(l2_stats["full"], transfer["full"]),
            l2=l2_stats,
            cohort=transfer,
            daily_l2=daily,
            daily_cohort=cohort_days,
            groups=groups,
            daily_errors=errors,
            auxiliary=dict(
                full=auxiliary(daily),
                quarters={
                    v["id"]: auxiliary([d for d in daily if v["effective_start"] <= d <= v["effective_end"]])
                    for v in VINTAGES
                },
            ),
            seed_diagnostics=seed_diagnostics,
            signed_prediction_range=physical,
            evaluation_counts=dict(counts),
            cohort_na=dict(cohort_counts),
            cohort_source_rows=len(cohort["rows"]),
            cohort_unique_keys=len(unique),
            fits=0,
            historical_as_known=False,
            untouched_holdout=False,
            economic_value_confirmed=False,
            qe_submitted=False,
        )
    )


def evaluate(prepared, acceptance, prediction, prediction_acceptance):
    core.verify_receipt(prediction_acceptance)
    require(
        prediction_acceptance["schema_version"] == VERSION + "_prediction_acceptance"
        and prediction_acceptance["status"] == "PASS"
        and prediction_acceptance["fresh_processes"] == 2
        and prediction_acceptance["prediction_sha256"] == prediction["receipt_sha256"]
        and prediction_acceptance["fit_acceptance_sha256"] == acceptance["receipt_sha256"],
        "all two-process predictions must be sealed before evaluation",
    )
    request, original, _, cached_features, _, _ = load_request(prepared["request_path"], prepared["request_sha256"])
    features = core.inputs._asset(prepared["prediction_features_path"], prepared["prediction_features_sha256"])
    require(predict_collection(acceptance, features) == prediction, "predictions differ from sealed model")
    cohort = member.pinned_json(request["cohort_path"], PINS["cohort_file"])
    core.verify_receipt(cohort, PINS["cohort"])
    facts = member.FrozenMemberSource(
        cached_features, request, provider_absence_manifest=Path(original["source"]["provider_absence_manifest"])
    )
    labels = facts.targets(features["prediction_dates"], CONTRACT["prediction_end"])
    result = score_outcomes(prediction, labels, features, acceptance, cohort)
    facts.unchanged()
    return receipt(
        {k: v for k, v in result.items() if k != "receipt_sha256"} | dict(evaluation_source_identity=facts.identity())
    )


# Independent approved W contract. Legacy S/C schemas and functions above stay strict.
WITHIN_VERSION = "hmm_l2_within_date_member_return_ridge_v1"
WITHIN_PINS = dict(
    prepared="06d3779b02a8fa295827f6d0e396c5f95c99f0ec7b1ed31ba414811b5f6bc82a",
    controls="a20f6b3e5db867ba78775b56c2ad6acb1d6adfd5a06550c44ea03a2b534bf95d",
    predictions="d58daf75098914efcf03a59aa2db5e60352040b5c6f5cfcacb79addd36b2c36a",
)
WITHIN_CONTRACT = dict(
    CONTRACT,
    version=WITHIN_VERSION,
    target="within_date_S_residual_on_original_common_TRAIN_mask",
    prediction="relative_member_return_score; center_on_ex_ante_feature_available_L2",
    scaler="reuse_matching_frozen_S_raw_TRAIN_mean_population_std_active",
    planned_fits=6,
    frozen_controls=WITHIN_PINS,
)


def within_inputs(prepared_path, controls_path, predictions_path):
    """Full file-only preflight; fit children receive only one quarter wrapper."""
    prepared, packets, _, features = load_prepared(prepared_path, WITHIN_PINS["prepared"])
    controls = core.inputs._asset(str(controls_path), WITHIN_PINS["controls"])
    baseline = core.inputs._asset(str(predictions_path), WITHIN_PINS["predictions"])
    require(
        controls["schema_version"] == VERSION + "_fit_acceptance"
        and controls["contract"] == CONTRACT
        and controls["completed_fits"] == 10
        and controls["status"] == "COMPLETED"
        and baseline["schema_version"] == VERSION + "_predictions"
        and baseline["fit_acceptance_sha256"] == controls["receipt_sha256"]
        and baseline["features_sha256"] == features["receipt_sha256"],
        "W frozen control identity differs",
    )
    for vintage in VINTAGES:
        q = vintage["id"]
        for arm in ("S", "C"):
            model = controls["models"][q][arm]
            validate_model(model)
            require(model["vintage"] == vintage and model["arm"] == arm, "W control quarter swapped")
        within_training(packets[q], controls["models"][q]["S"])
    return prepared, packets, controls, baseline, features


def within_training(packet, reference):
    rows = validate_packet(packet)
    validate_model(reference)
    require(
        reference["arm"] == "S"
        and reference["vintage"] == packet["vintage"]
        and reference["packet_sha256"] == packet["receipt_sha256"]
        and reference["mask_sha256"] == packet["mask_sha256"],
        "W reference TRAIN identity differs",
    )
    z, y, weights, scaler = loss._ridge_inputs([r[:4] for r in rows])
    require(all(reference["parameters"][k] == scaler[k] for k in scaler), "W frozen scaler differs")
    # The recomputation above verifies the frozen scaler; it does not choose another one.
    residual_x, residual_y = z.copy(), y.copy()
    days = np.asarray([r[0] for r in rows])
    for day in sorted(set(days)):
        ids = np.flatnonzero(days == day)
        residual_x[ids] -= z[ids].mean(axis=0)
        residual_y[ids] -= y[ids].mean()
    require(np.isfinite(residual_x).all() and np.isfinite(residual_y).all(), "W nonfinite TRAIN residual")
    return residual_x, residual_y, weights, scaler


def within_fit(packet, reference, output, attempt_root, head, numeric):
    from sklearn.linear_model import Ridge

    validate_numeric(numeric)
    x, y, weights, scaler = within_training(packet, reference)
    write_once(
        Path(attempt_root) / (packet["vintage"]["id"] + ".W.json"),
        receipt(dict(packet_sha256=packet["receipt_sha256"], arm="W", fit_kernel_attempts=1)),
    )
    fitted = Ridge(**loss.PARAMS).fit(x, y, sample_weight=weights)
    parameters = dict(**scaler, coef=fitted.coef_.tolist(), intercept=float(fitted.intercept_))
    result = receipt(
        dict(
            schema_version=WITHIN_VERSION + "_model",
            contract=WITHIN_CONTRACT,
            arm="W",
            vintage=packet["vintage"],
            parameters=parameters,
            model_sha256=canonical_sha256(parameters),
            reference_S_sha256=reference["model_sha256"],
            packet_sha256=packet["receipt_sha256"],
            mask_sha256=packet["mask_sha256"],
            catalog=packet["catalog"],
            release_binding=packet["release_binding"],
            source_commit=head,
            numeric_environment=numeric,
            fit_rows=len(y),
            fit_dates=len({r[0] for r in packet["rows"]}),
            sample_weight_sum=float(weights.sum()),
            training_residual_mean=float(np.average(y, weights=weights)),
            train_center_count_range=[
                min(Counter(r[0] for r in packet["rows"]).values()),
                max(Counter(r[0] for r in packet["rows"]).values()),
            ],
            fits=1,
            evaluation_outcomes_read=False,
        )
    )
    within_validate_model(result)
    write_once(Path(output) / "model.json", result)
    return result


def within_validate_model(model):
    core.verify_receipt(model)
    require(
        model["schema_version"] == WITHIN_VERSION + "_model"
        and model["contract"] == WITHIN_CONTRACT
        and model["arm"] == "W"
        and model["vintage"] in VINTAGES
        and model["fits"] == 1
        and model["evaluation_outcomes_read"] is False
        and model["model_sha256"] == canonical_sha256(model["parameters"])
        and model["release_binding"]["manifest_sha256"] == PINS["manifest"]
        and model["catalog"] == sorted(set(model["catalog"]))
        and len(model["catalog"]) == 131
        and re.fullmatch(r"[0-9a-f]{40}", model["source_commit"]) is not None,
        "W model contract differs",
    )
    loss.parameter_arrays(model["parameters"])
    validate_numeric(model["numeric_environment"])


def within_close(groups, wrappers, head, features_pin):
    require(
        len(groups) == 2 and canonical_json_bytes(groups[0]) == canonical_json_bytes(groups[1]), "W fresh fits differ"
    )
    require(set(groups[0]) == set(wrappers) == {v["id"] for v in VINTAGES}, "W missing quarter")
    for q, model in groups[0].items():
        within_validate_model(model)
        wrapper = wrappers[q]
        core.verify_receipt(wrapper)
        reference = wrapper["reference_S"]
        validate_model(reference)
        require(
            wrapper["schema_version"] == WITHIN_VERSION + "_train_reference"
            and wrapper["contract"] == WITHIN_CONTRACT
            and wrapper["source_commit"] == head
            and model["source_commit"] == head
            and reference["vintage"] == model["vintage"]
            and model["vintage"]["id"] == q
            and model["packet_sha256"] == wrapper["packet_sha256"]
            and model["reference_S_sha256"] == reference["model_sha256"]
            and model["mask_sha256"] == reference["mask_sha256"]
            and model["fit_rows"] == reference["fit_rows"]
            and model["fit_dates"] == reference["fit_dates"]
            and all(model["parameters"][k] == reference["parameters"][k] for k in ("mean", "scale", "active")),
            "W fit identity differs",
        )
    return receipt(
        dict(
            schema_version=WITHIN_VERSION + "_fit_acceptance",
            contract=WITHIN_CONTRACT,
            source_commit=head,
            status="COMPLETED",
            completed_fits=6,
            models=groups[0],
            features_sha256=features_pin,
            fresh_processes=2,
            automatic_promotion=False,
        )
    )


def within_validate_acceptance(acceptance):
    core.verify_receipt(acceptance)
    require(
        acceptance["schema_version"] == WITHIN_VERSION + "_fit_acceptance"
        and acceptance["contract"] == WITHIN_CONTRACT
        and acceptance["status"] == "COMPLETED"
        and acceptance["completed_fits"] == 6
        and acceptance["fresh_processes"] == 2
        and set(acceptance["models"]) == {v["id"] for v in VINTAGES},
        "W fit acceptance differs",
    )
    for q, model in acceptance["models"].items():
        within_validate_model(model)
        require(
            model["source_commit"] == acceptance["source_commit"] and model["vintage"]["id"] == q, "W model swapped"
        )


def within_predict(acceptance, features):
    within_validate_acceptance(acceptance)
    validate_prediction_features(features)
    require(acceptance["features_sha256"] == features["receipt_sha256"], "W feature binding differs")
    rows, centers = [], {}
    for vintage in VINTAGES:
        model = acceptance["models"][vintage["id"]]
        require(model["vintage"] == vintage and model["catalog"] == features["catalog"], "W forecast identity differs")
        mean, scale, active, coef, intercept = loss.parameter_arrays(model["parameters"])
        for day in schedule(features["calendar"], vintage)["predict"]:
            items = features["rows"][day]
            eligible = [c for c in features["catalog"] if items[c]["features"] is not None]
            values = {}
            if len(eligible) >= 2:
                x = np.asarray([items[c]["features"] for c in eligible], dtype=float)
                z = (x[:, active] - mean[active]) / scale[active]
                require(np.isfinite(z).all(), "W nonfinite forecast")
                scores = (z - z.mean(axis=0)) @ coef + intercept
                require(np.isfinite(z).all() and np.isfinite(scores).all(), "W nonfinite forecast")
                values = dict(zip(eligible, scores.tolist()))
            centers[day] = dict(codes=eligible, canonical_sha256=canonical_sha256(eligible))
            for code in features["catalog"]:
                value = values.get(code)
                reason = items[code]["reason_code"]
                if items[code]["features"] is not None and len(eligible) < 2:
                    reason = "insufficient_cross_section"
                rows.append(
                    dict(
                        trade_date=day,
                        as_of_date=features["as_of"][day],
                        sector_code=code,
                        vintage_id=vintage["id"],
                        model_sha256=model["model_sha256"],
                        relative_member_return_score=value,
                        availability="available" if value is not None else "unavailable",
                        reason_code=reason,
                    )
                )
    return receipt(
        dict(
            schema_version=WITHIN_VERSION + "_predictions",
            contract=WITHIN_CONTRACT,
            fit_acceptance_sha256=acceptance["receipt_sha256"],
            features_sha256=features["receipt_sha256"],
            release_binding=features["release_binding"],
            predictions=rows,
            centering_sets=centers,
            fits=0,
            evaluation_outcomes_read=False,
            policy_applied=False,
        )
    )


def within_summary(daily, calendar):
    def part(days):
        paired = [d for d in days if daily[d]["delta"] is not None]
        ws = [d for d in days if daily[d]["versus_S"] is not None]
        return {
            **{k: hac({d: daily[d][k] for d in paired}, calendar) for k in ("W", "C", "delta", "momentum")},
            "S": hac({d: daily[d]["S"] for d in ws}, calendar),
            "W_on_WS": hac({d: daily[d]["W"] for d in ws}, calendar),
            "versus_S": hac({d: daily[d]["versus_S"] for d in ws}, calendar),
            "coverage": dict(
                requested_dates=len(days),
                paired_WC=len(paired),
                paired_WS=len(ws),
                undefined_dates=[d for d in days if daily[d]["delta"] is None],
            ),
        }

    return dict(
        full=part(daily),
        quarters={
            v["id"]: part([d for d in daily if v["effective_start"] <= d <= v["effective_end"]]) for v in VINTAGES
        },
    )


def within_daily(scores, outcomes, momentum=None):
    values = {arm: rank_ic(xs, outcomes) for arm, xs in scores.items()}
    return dict(
        **values,
        n=len(outcomes),
        momentum=rank_ic(momentum, outcomes) if momentum is not None else None,
        delta=values["W"] - values["C"] if values["W"] is not None and values["C"] is not None else None,
        versus_S=values["W"] - values["S"] if values["W"] is not None and values["S"] is not None else None,
    )


def within_metrics(prediction, baseline, labels, features, cohort):
    """One common row universe; separate main WC and diagnostic WS date pairs."""
    expected = [(d, c) for d in features["prediction_dates"] for c in features["catalog"]]
    maps = {}
    for arm, rows, field in (
        ("W", prediction["predictions"], "relative_member_return_score"),
        ("S", baseline["predictions"]["S"], "expected_member_return"),
        ("C", baseline["predictions"]["C"], "expected_member_loss"),
    ):
        require([(r["trade_date"], r["sector_code"]) for r in rows] == expected, "W evaluation grid differs")
        require(all(r[field] is None or math.isfinite(r[field]) for r in rows), "W invalid evaluation score")
        maps[arm] = {
            (r["trade_date"], r["sector_code"]): (-r[field] if arm == "C" else r[field])
            if r[field] is not None
            else None
            for r in rows
        }
    require(set(labels) == set(features["prediction_dates"]), "W label dates differ")
    daily, errors, groups, counts = {}, {}, {}, Counter()
    for day in features["prediction_dates"]:
        require(set(labels[day]) == set(features["catalog"]), "W label denominator differs")
        scores, ys, downs, ups, momentum = {a: [] for a in maps}, [], [], [], []
        for code in features["catalog"]:
            label = labels[day][code]
            counts[label["status"]] += 1
            if label["status"] != "AVAILABLE":
                require(
                    (label["status"], label["reason"])
                    in {
                        ("UNAVAILABLE", "NO_ASOF_MEMBER"),
                        ("UNAVAILABLE", "FIXED_COHORT_ENDPOINT_NA"),
                        ("OUTCOME_NOT_MATURE", "OUTCOME_NOT_MATURE"),
                    }
                    and all(label[k] is None for k in ("B", "C", "upside")),
                    "W unknown target absence",
                )
                counts[label["reason"] + "_reason"] += 1
                continue
            if any(maps[a][day, code] is None for a in maps):
                counts["PREDICTION_UNAVAILABLE"] += 1
                continue
            y, down = signed_label(label)
            ys.append(y)
            downs.append(down)
            ups.append(label["upside"])
            momentum.append(features["rows"][day][code]["features"][ALL_CORE_FEATURES.index("excess_return_Nd")])
            for arm in maps:
                scores[arm].append(maps[arm][day, code])
        daily[day] = within_daily(scores, ys, momentum)
        if ys:
            centered_y = np.asarray(ys) - np.mean(ys)
            errors[day] = {}
            for arm in ("W", "S"):
                residual = np.asarray(scores[arm]) - np.mean(scores[arm]) - centered_y
                errors[day][arm + "_mse"] = float(np.mean(residual**2))
                errors[day][arm + "_mae"] = float(np.mean(np.abs(residual)))
            errors[day].update(zero_mse=float(np.mean(centered_y**2)), zero_mae=float(np.mean(np.abs(centered_y))))
            groups[day] = {
                a: {
                    side: {
                        name: extreme_mean(xs, values, high=side == "high")
                        for name, values in (("signed_return", ys), ("downside", downs), ("upside", ups))
                    }
                    for side in ("high", "low")
                }
                for a, xs in scores.items()
            }
    unique = {}
    for row in cohort["rows"]:
        key, fact = (row["date"], row["stock"]), (row["code"], row["stock_return"], row["reason"])
        require(key not in unique or unique[key] == fact, "W cross-seed fact conflict")
        unique[key] = fact

    def transfer(selected):
        result, na = {}, Counter()
        for day in sorted({d for d, _ in selected}):
            require(day in features["prediction_dates"], "W cohort date outside request")
            scores, ys = {a: [] for a in maps}, []
            for (d, _), (code, outcome, reason) in selected.items():
                if d != day:
                    continue
                if outcome is None:
                    require(isinstance(reason, str) and reason, "W untyped cohort absence")
                    na[reason] += 1
                    continue
                require(code in features["catalog"] and math.isfinite(outcome), "W unknown cohort identity")
                if any(maps[a][day, code] is None for a in maps):
                    na["PREDICTION_UNAVAILABLE"] += 1
                    continue
                ys.append(outcome)
                for arm in maps:
                    scores[arm].append(maps[arm][day, code])
            result[day] = within_daily(scores, ys)
        return result, dict(na)

    transfer_days, transfer_na = transfer(unique)
    seeds = {}
    for seed in sorted({r["seed"] for r in cohort["rows"]}):
        selected = {(r["date"], r["stock"]): unique[r["date"], r["stock"]] for r in cohort["rows"] if r["seed"] == seed}
        ds, na = transfer(selected)
        seeds[str(seed)] = dict(stats=within_summary(ds, features["calendar"]), na=na, independent_sample=False)

    def auxiliary(days):
        ds = [d for d in days if d in errors]
        spreads = {
            a: {d: groups[d][a]["high"]["signed_return"] - groups[d][a]["low"]["signed_return"] for d in ds}
            for a in maps
        }
        return dict(
            dates=len(ds),
            centered_errors={
                k: math.fsum(errors[d][k] for d in ds) / len(ds) if ds else None
                for k in ("W_mse", "S_mse", "zero_mse", "W_mae", "S_mae", "zero_mae")
            },
            groups={
                a: {
                    side: {
                        name: math.fsum(groups[d][a][side][name] for d in ds) / len(ds) if ds else None
                        for name in ("signed_return", "downside", "upside")
                    }
                    for side in ("high", "low")
                }
                for a in maps
            },
            overlapping_reference_not_nav=True,
            spread={
                **{a: hac(values, features["calendar"]) for a, values in spreads.items()},
                **{
                    key: hac({d: spreads["W"][d] - spreads[a][d] for d in ds}, features["calendar"])
                    for key, a in (("delta", "C"), ("versus_S", "S"))
                },
            },
        )

    l2, cohort_stats = within_summary(daily, features["calendar"]), within_summary(transfer_days, features["calendar"])
    status = decision(
        {"S": l2["full"]["W"], "delta": l2["full"]["delta"]},
        {"S": cohort_stats["full"]["W"], "delta": cohort_stats["full"]["delta"]},
    )
    return receipt(
        dict(
            schema_version=WITHIN_VERSION + "_evaluation",
            contract=WITHIN_CONTRACT,
            status=status,
            prediction_sha256=prediction["receipt_sha256"],
            baseline_prediction_sha256=baseline["receipt_sha256"],
            l2=l2,
            cohort=cohort_stats,
            daily_l2=daily,
            daily_cohort=transfer_days,
            daily_errors=errors,
            groups=groups,
            auxiliary=dict(
                full=auxiliary(daily),
                quarters={
                    v["id"]: auxiliary([d for d in daily if v["effective_start"] <= d <= v["effective_end"]])
                    for v in VINTAGES
                },
            ),
            seed_diagnostics=seeds,
            evaluation_counts=dict(counts),
            cohort_na=transfer_na,
            cohort_source_rows=len(cohort["rows"]),
            cohort_unique_keys=len(unique),
            fits=0,
            historical_as_known=False,
            untouched_holdout=False,
            economic_value_confirmed=False,
            qe_submitted=False,
        )
    )


def within_evaluate(prepared, controls, baseline, features, acceptance, prediction, sealed):
    core.verify_receipt(sealed)
    require(
        sealed["schema_version"] == WITHIN_VERSION + "_prediction_acceptance"
        and sealed["status"] == "PASS"
        and sealed["fresh_processes"] == 2
        and sealed["prediction_sha256"] == prediction["receipt_sha256"]
        and sealed["fit_acceptance_sha256"] == acceptance["receipt_sha256"],
        "W evaluation requires sealed dual-process predictions",
    )
    require(within_predict(acceptance, features) == prediction, "W sealed forecast differs")
    core.verify_receipt(controls, WITHIN_PINS["controls"])
    core.verify_receipt(baseline, WITHIN_PINS["predictions"])
    request, original, _, cached_features, _, _ = load_request(prepared["request_path"], prepared["request_sha256"])
    cohort = member.pinned_json(request["cohort_path"], PINS["cohort_file"])
    core.verify_receipt(cohort, PINS["cohort"])
    facts = member.FrozenMemberSource(
        cached_features, request, provider_absence_manifest=Path(original["source"]["provider_absence_manifest"])
    )
    labels = facts.targets(features["prediction_dates"], CONTRACT["prediction_end"])
    result = within_metrics(prediction, baseline, labels, features, cohort)
    facts.unchanged()
    return receipt(
        {k: v for k, v in result.items() if k != "receipt_sha256"} | dict(evaluation_source_identity=facts.identity())
    )
