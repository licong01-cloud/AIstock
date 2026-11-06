"""Explicit-release member-loss preparation and one paired Q4 update.

Training identity stays immutable. This module never resolves latest, writes a
release, accesses a database, or turns a loss estimate into a trading policy.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import re

from backend.services.hmm_risk import risk_l2_member_loss as member
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_json_bytes, canonical_sha256
from backend.services.hmm_risk.formal_state_executor import validate_output_location, write_once
from backend.services.hmm_risk.formal_state_model import receipt
from backend.services.hmm_risk.monthly_release_input import _Files

core, require = member.core, member.require
VERSION = "hmm_risk_l2_member_release_v1"
Q4 = dict(
    id="2026Q4",
    train_start="2023-10-01",
    train_end="2026-09-30",
    effective_start="2026-10-01",
    effective_end="2026-12-31",
)
CONTRACT = dict(
    version=VERSION,
    vintage=Q4,
    target=member.TARGET,
    feature_names=list(ALL_CORE_FEATURES),
    parameters=member.loss.PARAMS,
    numeric_versions=member.loss.EXPECTED_VERSIONS,
    planned_fits=4,
    historical_as_known=False,
    untouched_holdout=False,
    policy_applied=False,
)


def schedule(calendar, request_end):
    """The model's quarter end is not the current request's data cutoff."""
    require(calendar and calendar == sorted(set(calendar)), "calendar order differs")
    positions = {d: i for i, d in enumerate(calendar)}
    require(Q4["train_end"] in positions and request_end in positions, "explicit boundary absent")
    require(Q4["effective_start"] <= request_end <= Q4["effective_end"], "request outside Q4")
    train = [d for d in calendar if Q4["train_start"] <= d <= Q4["train_end"]]
    predict = [d for d in calendar if Q4["effective_start"] <= d <= request_end]
    require(train and predict and positions[train[0]] >= core.CONTEXT_OPEN_DAYS, "context absent")
    fit = member.target_dates(calendar, train, Q4["train_end"])["mature"]
    return dict(train=train, fit=fit, predict=predict, as_of={d: calendar[positions[d] - 1] for d in train + predict})


def source_preflight(request):
    """Cheap authenticated boundary check before reading bulk facts or any fit."""
    core.validate_request(request)
    require(set(request["model_assets"]) == core.MODEL_FIELDS, "explicit original bootstrap required")
    core.model_source(request)  # Unchanged feature definition, not a new model fit.
    plan = core.date_plan(request)
    source = request["source"]
    files = _Files(Path(source["candidate_root"]))
    path = files.path("qe_dataset_manifest.json")
    require(files.digest(path) == source["release_binding"]["manifest_file_sha256"], "manifest bytes differ")
    manifest = files.read(path)
    require(
        manifest["dataset_manifest_sha256"] == source["release_binding"]["manifest_sha256"],
        "release identity differs",
        "identity_mismatch",
    )
    authority = core.reader._industry_adapter(
        source["industry_authority"],
        forbidden_roots=(),
        candidate_root=Path(source["candidate_root"]),
        release_binding=source["release_binding"],
    )
    bounds = getattr(authority, "coverage_window", authority.authority_bundle.preflight_report)
    needed = sorted(set(plan["as_of"].values()))
    outside = [d for d in needed if not bounds["window_start"] <= d <= bounds["window_end"]]
    calendar_path = files.path("components/daily_bin_candidate/calendars/day.txt")
    require(files.digest(calendar_path) == source["calendar_sha256"], "calendar bytes differ")
    calendar = calendar_path.read_text(encoding="utf-8").splitlines()
    stages = {}
    if Q4["effective_start"] <= request["decision_dates"][-1] <= Q4["effective_end"]:
        quarter = schedule(calendar, request["decision_dates"][-1])
        context_start = calendar[calendar.index(quarter["train"][0]) - core.CONTEXT_OPEN_DAYS]
        stages["q4_training_authority_boundary"] = (
            "BOUNDARY_PASS"
            if bounds["window_start"] <= context_start and Q4["train_end"] <= bounds["window_end"]
            else "BLOCKED"
        )
        stages["q4_prediction_authority_boundary"] = "BLOCKED" if outside else "BOUNDARY_PASS"
    historical = [d for d in needed if d <= member.rolling.VINTAGES[-1]["effective_end"]]
    stages["old_model_reinference_authority_boundary"] = (
        "BOUNDARY_PASS"
        if historical and bounds["window_start"] <= historical[0] and historical[-1] <= bounds["window_end"]
        else "BLOCKED"
    )
    files.unchanged()
    return receipt(
        dict(
            schema_version=VERSION + "_source_preflight",
            status="BLOCKED" if outside else "BOUNDARY_PASS",
            release_binding=source["release_binding"],
            calendar_sha256=source["calendar_sha256"],
            industry_authority_sha256=canonical_sha256(source["industry_authority"]),
            authority_window_start=bounds["window_start"],
            authority_window_end=bounds["window_end"],
            baseline_c013_window_end=authority.authority_bundle.preflight_report["window_end"],
            authority_binding_type=source["industry_authority"].get("schema_version", "original_c013"),
            first_unsupported_as_of=outside[0] if outside else None,
            unsupported_as_of_dates=outside,
            stage_boundaries=stages,
            reason_code="hmm_risk_classification_authority_window_unavailable" if outside else None,
            fits=0,
            full_file_preflight=False,
            database_access=False,
            dataset_write=False,
        )
    )


def prepare(request_path, request_sha256, output, head):
    request = member.pinned_json(request_path, request_sha256)
    require(re.fullmatch(r"[0-9a-f]{40}", head) is not None, "invalid source commit")
    boundary = source_preflight(request)
    require(
        boundary["status"] == "BOUNDARY_PASS",
        f"C-013 ends {boundary['authority_window_end']}; first unsupported as-of {boundary['first_unsupported_as_of']}",
        "classification_authority_unavailable",
    )
    output = validate_output_location(Path(output), dataset_root=Path(request["source"]["candidate_root"]))
    require(not output.exists(), "prepare output collision")
    source = {**request["source"], "work_parent": str(output / "source_work")}
    frozen, parameters, bootstrap = core.model_source(request)
    calendar_path = core.inputs.reader._require_file(
        Path(source["candidate_root"]), "components/daily_bin_candidate/calendars/day.txt", source["calendar_sha256"]
    )
    stamp = core.inputs._stamp(calendar_path)
    calendar = calendar_path.read_text(encoding="utf-8").splitlines()
    require(core.inputs._stamp(calendar_path) == stamp, "calendar changed")
    plan = schedule(calendar, request["decision_dates"][-1])
    expected = [d for d in calendar if member.rolling.VINTAGES[0]["effective_start"] <= d <= plan["predict"][-1]]
    require(request["decision_dates"] == expected, "complete historical/new-quarter request required")
    decisions = sorted(set(plan["train"] + expected))
    positions = {d: i for i, d in enumerate(calendar)}
    as_of = {d: calendar[positions[d] - 1] for d in decisions}
    data_frozen = {
        **frozen,
        **{k: source[k] for k in ("industry_authority", "security_identity_sha256", "provider_absence_sha256")},
    }
    assets, window, aggregates, identity = core._bounded_l2_stock_facts(
        data_frozen,
        source,
        start=core._day(calendar[positions[decisions[0]] - core.CONTEXT_OPEN_DAYS]),
        end=core._day(as_of[decisions[-1]]),
        prewarm_price_history=True,
        release_binding=source["release_binding"],
        expected_calendar_sha256=source["calendar_sha256"],
    )
    extended = receipt(
        {k: v for k, v in request.items() if k != "receipt_sha256"} | dict(source=source, decision_dates=decisions)
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
        | dict(contract=CONTRACT, source=source, calendar=calendar, prediction_dates=expected)
    )
    validate_features(features)
    original = dict(source_request_path=str(request_path), source_request_sha256=request_sha256)
    facts = member.FrozenMemberSource(
        features, original, provider_absence_manifest=Path(source["provider_absence_manifest"])
    )
    # Resolve maturity before any member endpoint reads. No evaluation outcomes.
    labels = facts.targets(plan["train"], Q4["train_end"])
    returns = {d.isoformat(): dict.fromkeys(features["catalog"]) for d in window}
    for item in aggregates:
        returns[item.trade_date.isoformat()][item.l1_code] = float(item.l1_return)
    original_labels = member.rolling.risk.drawdown_outcomes(
        calendar, plan["train"], features["catalog"], returns, Q4["train_end"]
    )
    training = receipt(
        dict(
            schema_version=VERSION + "_train",
            contract=CONTRACT,
            feature_sha256=features["receipt_sha256"],
            labels=labels,
            original_status={
                d: {c: original_labels[d][c]["status"] for c in features["catalog"]} for d in plan["train"]
            },
        )
    )
    rows = training_rows(features, training)
    identity = facts.identity()
    write_once(output / "features.json", features)
    write_once(output / "train.json", training)
    prepared = receipt(
        dict(
            schema_version=VERSION + "_prepared",
            contract=CONTRACT,
            source_commit=head,
            feature_path=str(output / "features.json"),
            feature_sha256=features["receipt_sha256"],
            training_path=str(output / "train.json"),
            training_sha256=training["receipt_sha256"],
            member_source_identity=identity,
            release_binding=source["release_binding"],
            common_mask_sha256=canonical_sha256([[r[0], r[1]] for r in rows]),
            fits=0,
            evaluation_outcomes_read=False,
            database_access=False,
            dataset_write=False,
        )
    )
    write_once(output / "prepared.json", prepared)
    return prepared


def validate_features(features):
    core.verify_receipt(features)
    require(
        set(features)
        == {
            "schema_version",
            "request_sha256",
            "source_commit",
            "model_identity",
            "catalog",
            "feature_names",
            "parameters",
            "rows",
            "as_of",
            "inference_identity",
            "receipt_sha256",
            "contract",
            "source",
            "calendar",
            "prediction_dates",
        }
        and features["schema_version"] == VERSION + "_features"
        and features["contract"] == CONTRACT,
        "release feature contract differs",
    )
    catalog = features["catalog"]
    require(
        len(catalog) == 131
        and catalog == sorted(set(catalog))
        and features["feature_names"] == list(ALL_CORE_FEATURES),
        "131/20D differs",
    )
    prediction = features["prediction_dates"]
    plan = schedule(features["calendar"], prediction[-1])
    expected = [d for d in features["calendar"] if "2026-04-01" <= d <= prediction[-1]]
    require(prediction == expected, "prediction window incomplete")
    days = sorted(set(plan["train"] + prediction))
    positions = {d: i for i, d in enumerate(features["calendar"])}
    as_of = {d: features["calendar"][positions[d] - 1] for d in days}
    require(features["as_of"] == as_of, "feature as-of differs")
    core.validate_feature_rows(dict(decision_dates=days), features, as_of)
    data, source = features["inference_identity"], features["source"]
    release = data["release_identity"]
    require(
        release["frozen_release_binding_sha256"] == canonical_sha256(source["release_binding"])
        and release["dataset_manifest_sha256"] == source["release_binding"]["manifest_sha256"]
        and data["calendar_sha256"] == source["calendar_sha256"]
        and data["industry_authority_sha256"] == canonical_sha256(source["industry_authority"])
        and all(data[k] == source[k] for k in ("security_identity_sha256", "provider_absence_sha256")),
        "feature input identities differ",
        "identity_mismatch",
    )
    require(
        canonical_sha256(features["parameters"]) == core.value.APPROVED_PINS["model_hash"],
        "bootstrap parameters changed",
        "identity_mismatch",
    )


def training_rows(features, training):
    validate_features(features)
    core.verify_receipt(training)
    plan = schedule(features["calendar"], features["prediction_dates"][-1])
    require(
        training["schema_version"] == VERSION + "_train"
        and training["contract"] == CONTRACT
        and training["feature_sha256"] == features["receipt_sha256"]
        and set(training["labels"]) == set(training["original_status"]) == set(plan["train"]),
        "training carrier differs",
        "identity_mismatch",
    )
    fit_days = set(plan["fit"])
    rows = []
    for day in plan["train"]:
        require(
            set(training["labels"][day]) == set(training["original_status"][day]) == set(features["catalog"]),
            "training sector grid differs",
        )
        for code in features["catalog"]:
            label = training["labels"][day][code]
            status = label["status"]
            if day not in fit_days:
                require(
                    status == "OUTCOME_NOT_MATURE" and label["B"] is None and label["C"] is None,
                    "unmature training target populated",
                )
                continue
            require(status in {"AVAILABLE", "UNAVAILABLE"}, "unknown target status")
            vector = features["rows"][day][code]["features"]
            if status == "AVAILABLE":
                for arm in member.ARMS:
                    require(0 <= member._number(label[arm]) < 1, "invalid member target")
            else:
                require(
                    label["B"] is None
                    and label["C"] is None
                    and label["reason"] in {"NO_ASOF_MEMBER", "FIXED_COHORT_ENDPOINT_NA"},
                    "unknown training absence",
                )
            original = training["original_status"][day][code]
            require(original in {"AVAILABLE", "OUTCOME_LEGAL_NA"}, "unknown original target status")
            if status == "AVAILABLE" and vector is not None and original == "AVAILABLE":
                rows.append((day, code, vector, label["B"], label["C"]))
    require(bool(rows), "empty common mask", "train_label_evidence_insufficient")
    return rows


def load_prepared(path, expected):
    value = core.inputs._asset(str(path), expected)
    require(
        value["schema_version"] == VERSION + "_prepared"
        and value["contract"] == CONTRACT
        and value["fits"] == 0
        and value["evaluation_outcomes_read"] is False,
        "prepared differs",
    )
    features = core.inputs._asset(value["feature_path"], value["feature_sha256"])
    training = core.inputs._asset(value["training_path"], value["training_sha256"])
    rows = training_rows(features, training)
    require(
        value["common_mask_sha256"] == canonical_sha256([[r[0], r[1]] for r in rows])
        and value["release_binding"] == features["source"]["release_binding"],
        "prepared identities differ",
    )
    return value, features, training, rows


def validate_model(model, expected=None):
    core.verify_receipt(model, expected)
    require(
        model["schema_version"] == VERSION + "_model"
        and model["contract"] == CONTRACT
        and model["vintage"] == Q4
        and model["arm"] in member.ARMS
        and model["model_sha256"] == canonical_sha256(model["parameters"])
        and model["catalog"] == sorted(set(model["catalog"]))
        and len(model["catalog"]) == 131,
        "Q4 model contract differs",
        "identity_mismatch",
    )
    for key in ("training_mask_sha256", "training_label_sha256", "feature_sha256", "feature_definition_sha256"):
        core.reader._require_sha256(model[key], key)
    require(
        model["member_source_identity"]["release_identity"]["dataset_manifest_sha256"]
        == model["release_binding"]["manifest_sha256"]
        and model["member_source_identity"]["target_contract"] == member.TARGET,
        "training release changed",
        "identity_mismatch",
    )
    require(
        re.fullmatch(r"[0-9a-f]{40}", model["source_commit"]) is not None
        and 0 < model["fit_dates"] <= model["fit_rows"] <= 131 * model["fit_dates"]
        and abs(model["sample_weight_sum"] - model["fit_rows"]) < 1e-6,
        "model source/sample statistics differ",
    )
    member.loss.parameter_arrays(model["parameters"])


def execute_child(path, expected, output, head):
    prepared, features, _, rows = load_prepared(path, expected)
    # Both targets, masks and all input identities checked before the first fit.
    parameters, stats = member.fit_pair(
        rows,
        lambda arm: write_once(
            Path(output) / f"{arm}.attempt.json",
            receipt(dict(arm=arm, vintage=Q4, fit_kernel_attempts=1, prepared_sha256=expected)),
        ),
    )
    days = schedule(features["calendar"], features["prediction_dates"][-1])["predict"]
    sealed = {}
    for arm in member.ARMS:
        model = receipt(
            dict(
                schema_version=VERSION + "_model",
                contract=CONTRACT,
                arm=arm,
                vintage=Q4,
                parameters=parameters[arm],
                model_sha256=canonical_sha256(parameters[arm]),
                feature_sha256=features["receipt_sha256"],
                feature_definition_sha256=features["model_identity"]["feature_definition_sha256"],
                training_label_sha256=prepared["training_sha256"],
                training_mask_sha256=prepared["common_mask_sha256"],
                member_source_identity=prepared["member_source_identity"],
                release_binding=prepared["release_binding"],
                catalog=features["catalog"],
                source_commit=head,
                **stats[arm],
            )
        )
        validate_model(model)
        predictions, counts = member.predict_parameters(
            model, {d: features["rows"][d] for d in days}, days, features["as_of"], features["calendar"]
        )
        sealed[arm] = receipt(dict(model=model, predictions=predictions, fits=1, **counts))
        write_once(Path(output) / f"{arm}.sealed.json", sealed[arm])
    return receipt(
        dict(
            schema_version=VERSION + "_repeat",
            contract=CONTRACT,
            source_commit=head,
            prepared_sha256=expected,
            fits=2,
            sealed=sealed,
            prediction_dates=days,
            labels_read=True,
            evaluation_outcomes_read=False,
            database_access=False,
            runtime_action=False,
        )
    )


def close_processes(first, second, head, prepared, features):
    validate_features(features)
    days = schedule(features["calendar"], features["prediction_dates"][-1])["predict"]
    for result in (first, second):
        core.verify_receipt(result)
        require(
            result["contract"] == CONTRACT
            and result["source_commit"] == head
            and result["prepared_sha256"] == prepared["receipt_sha256"]
            and result["fits"] == 2
            and result["prediction_dates"] == days
            and result["evaluation_outcomes_read"] is False
            and result["numeric_environment"]["versions"] == member.loss.EXPECTED_VERSIONS
            and set(result["sealed"]) == set(member.ARMS),
            "child source/budget differs",
        )
        models = []
        for arm in member.ARMS:
            packet = result["sealed"][arm]
            core.verify_receipt(packet)
            model = packet["model"]
            validate_model(model)
            require(
                model["source_commit"] == head
                and model["arm"] == arm
                and packet["fits"] == 1
                and model["training_mask_sha256"] == prepared["common_mask_sha256"]
                and model["training_label_sha256"] == prepared["training_sha256"]
                and model["feature_sha256"] == features["receipt_sha256"]
                and model["release_binding"] == prepared["release_binding"]
                and model["member_source_identity"] == prepared["member_source_identity"]
                and model["catalog"] == features["catalog"]
                and model["feature_definition_sha256"] == features["model_identity"]["feature_definition_sha256"],
                "sealed model source differs",
            )
            predictions = packet["predictions"]
            require(
                days
                and days == sorted(set(days))
                and all(Q4["effective_start"] <= d <= Q4["effective_end"] for d in days)
                and [(r["trade_date"], r["sector_code"]) for r in predictions]
                == [(d, c) for d in days for c in model["catalog"]],
                "prediction grid differs",
            )
            expected, counts = member.predict_parameters(
                model, {d: features["rows"][d] for d in days}, days, features["as_of"], features["calendar"]
            )
            require(
                predictions == expected and packet["clipped_rows"] == counts["clipped_rows"],
                "prediction differs from parent kernel",
            )
            models.append(model)
        require(
            models[0]["training_mask_sha256"] == models[1]["training_mask_sha256"]
            and all(models[0]["parameters"][k] == models[1]["parameters"][k] for k in ("mean", "scale", "active")),
            "paired mask/scaler differs",
        )
    require(canonical_json_bytes(first) == canonical_json_bytes(second), "fresh processes differ")
    return receipt(
        dict(
            schema_version=VERSION + "_acceptance",
            contract=CONTRACT,
            status="COMPLETED",
            source_commit=head,
            prepared_sha256=prepared["receipt_sha256"],
            completed_fits=4,
            process_sha256=first["receipt_sha256"],
            database_access=False,
            database_write=False,
            runtime_action=False,
            automatic_promotion=False,
        )
    )


def reinfer(model, request, bundle, *, model_sha256, bundle_sha256, source_mode="frozen_file"):
    """Old v1 models keep their original training authority and effective dates."""
    if model.get("schema_version") != VERSION + "_model":
        return member.infer_prepared(
            model,
            request,
            bundle,
            source_mode=source_mode,
            expected_model_sha256=model_sha256,
            expected_bundle_sha256=bundle_sha256,
        )
    validate_model(model, model_sha256)
    if source_mode == "frozen_file":
        consumer = core
    elif source_mode == "database_daily":
        from backend.services.hmm_risk import risk_l2_database_inference as consumer
    else:
        raise member.rolling.risk.fail("unknown source mode", "source_invalid")
    consumer.validate_bundle(request, bundle, bundle_sha256)
    require(
        bundle["catalog"] == model["catalog"]
        and bundle["model_identity"]["feature_definition_sha256"] == model["feature_definition_sha256"],
        "model feature definition differs",
    )
    if source_mode == "frozen_file":
        path = core.inputs.reader._require_file(
            Path(request["source"]["candidate_root"]),
            "components/daily_bin_candidate/calendars/day.txt",
            request["source"]["calendar_sha256"],
        )
        stamp = core.inputs._stamp(path)
        calendar = path.read_text(encoding="utf-8").splitlines()
        require(core.inputs._stamp(path) == stamp, "calendar changed")
    else:
        calendar = bundle["inference_identity"]["calendar"]
    predictions, counts = member.predict_parameters(
        model, bundle["rows"], request["decision_dates"], bundle["as_of"], calendar
    )
    return receipt(
        dict(
            schema_version=VERSION + "_prediction",
            contract=CONTRACT,
            model_bundle_sha256=model_sha256,
            training_release_binding=model["release_binding"],
            source_features_sha256=bundle_sha256,
            inference_identity=bundle["inference_identity"],
            source_mode=source_mode,
            predictions=predictions,
            fits=0,
            labels_read=False,
            outcomes_read=False,
            policy_applied=False,
            historical_as_known=False,
            forward_confirmed=False,
            **counts,
        )
    )


def prediction_summary(predictions):
    return dict(
        rows=len(predictions),
        days=len({r["trade_date"] for r in predictions}),
        availability=dict(Counter(r["availability"] for r in predictions)),
        reasons=dict(Counter(r["reason_code"] for r in predictions if r["reason_code"] is not None)),
    )
