"""唯一N：原W日期内输入上的固定浅树，六次拟合、封存后评价。"""

from __future__ import annotations

from collections import Counter
import hashlib
import math
from pathlib import Path

import numpy as np

from backend.services.hmm_risk import l2_economic_candidates as trees
from backend.services.hmm_risk import risk_l2_margin_information as budget
from backend.services.hmm_risk import risk_l2_member_return as prior
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_json_bytes, canonical_sha256
from backend.services.hmm_risk.formal_state_executor import validate_output_location, write_once
from backend.services.hmm_risk.formal_state_model import receipt

core, member, require = prior.core, prior.member, prior.require
VERSION = "hmm_l2_within_date_member_return_gbrt_v1"
HELPER_PINS = {
    "risk_l2_member_return.py": "44b02c47a8ea4d461e5f0d0532977da3e715654b436c074cf5b5af9193b14f8d",
    "l2_economic_candidates.py": "dbd6f9ce3a909393e380fbdf88e6384648f69d8204d570c717d1dea015a734bc",
}
PARAMS = dict(
    loss="squared_error",
    learning_rate=0.05,
    n_estimators=64,
    subsample=1.0,
    criterion="friedman_mse",
    min_samples_split=620,
    min_samples_leaf=310,
    min_weight_fraction_leaf=0.0,
    max_depth=2,
    min_impurity_decrease=0.0,
    init=None,
    random_state=42,
    max_features=None,
    alpha=0.9,
    verbose=0,
    max_leaf_nodes=None,
    warm_start=False,
    validation_fraction=0.1,
    n_iter_no_change=None,
    tol=0.0001,
    ccp_alpha=0.0,
)
PINS = dict(
    prepared=prior.WITHIN_PINS["prepared"],
    controls=prior.WITHIN_PINS["controls"],
    baseline=prior.WITHIN_PINS["predictions"],
    w_acceptance="4fab5f4c2c987eb16c862477378dc255f8626e15dca9143ae777f40a7cdbc6ca",
    w_prediction="f9b9d881365eecdbcd08769da73483c299ded420aa986bd8ec37802fc86f5a29",
)
CONTRACT = dict(
    version=VERSION,
    parameters=PARAMS,
    feature_names=list(ALL_CORE_FEATURES),
    vintages=list(prior.VINTAGES),
    horizon=10,
    anchor="decision_close",
    target="within_date_S=sum(fixed_asof_circ_mv_weight*R)",
    prediction_start="2026-04-01",
    prediction_end="2026-10-30",
    manifest_sha256=prior.PINS["manifest"],
    source_pins=PINS,
    planned_fits=6,
    primary="date_equal_mean_J_N_minus_C",
    budget_fraction=0.20,
    historical_as_known=False,
    untouched_holdout=False,
    policy_applied=False,
)


def asset(path, pin):
    require(path and pin and Path(path).is_absolute(), "explicit absolute source path/pin required")
    return core.inputs._asset(str(path), pin)


def load_request(path, pin):
    request = member.pinned_json(path, pin)
    fields = {"schema_version", "contract", "fit_ledger_root"} | {k + "_path" for k in PINS}
    require(
        set(request) == fields
        and request["schema_version"] == VERSION + "_request"
        and request["contract"] == CONTRACT,
        "unapproved N request",
    )
    values = {k: asset(request[k + "_path"], v) for k, v in PINS.items()}
    prepared, packets, controls, baseline, features = prior.within_inputs(
        request["prepared_path"], request["controls_path"], request["baseline_path"]
    )
    prior.within_validate_acceptance(values["w_acceptance"])
    require(
        values["w_prediction"]["fit_acceptance_sha256"] == values["w_acceptance"]["receipt_sha256"]
        and values["w_prediction"]["features_sha256"] == features["receipt_sha256"],
        "frozen W forecast differs",
    )
    # 完整正式来源链核验，不重建特征/标签、不打开评价outcome。
    prior.load_request(prepared["request_path"], prepared["request_sha256"])
    validate_output_location(Path(request["fit_ledger_root"]))
    return request, prepared, packets, controls, baseline, features, values


def training_identity(x, y, weights, scaler):
    return canonical_sha256(dict(x=x.tolist(), y=y.tolist(), weights=weights.tolist(), scaler=scaler))


def prepare(path, pin, output, head):
    require(PARAMS == trees.TREE_PARAMS, "frozen tree kernel parameters drifted")
    require(
        all(
            hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() == expected
            for name, expected in HELPER_PINS.items()
        ),
        "frozen helper source drifted; review required",
    )
    request, old, packets, controls, baseline, features, values = load_request(path, pin)
    output = validate_output_location(Path(output))
    require(not output.exists(), "N preflight output already exists")
    locators, training = {}, {}
    for vintage in prior.VINTAGES:
        q, packet = vintage["id"], packets[vintage["id"]]
        reference = controls["models"][q]["S"]
        x, y, weights, scaler = prior.within_training(packet, reference)
        w = values["w_acceptance"]["models"][q]
        require(
            w["packet_sha256"] == packet["receipt_sha256"]
            and w["reference_S_sha256"] == reference["model_sha256"]
            and all(w["parameters"][k] == scaler[k] for k in scaler),
            "N/W TRAIN differs",
        )
        require(np.isfinite(x.astype(np.float32)).all(), "float32 TRAIN overflow")
        wrapper = receipt(
            dict(
                schema_version=VERSION + "_train",
                contract=CONTRACT,
                vintage=vintage,
                original_packet=old["packets"][q],
                reference_S=reference,
                reference_W_model_sha256=w["model_sha256"],
                training_sha256=training_identity(x, y, weights, scaler),
                source_commit=head,
            )
        )
        target = output / (q + ".json")
        write_once(target, wrapper)
        locators[q] = dict(path=str(target), sha256=wrapper["receipt_sha256"])
        training[q] = dict(
            rows=len(y),
            dates=len({r[0] for r in packet["rows"]}),
            mask_sha256=packet["mask_sha256"],
            training_sha256=wrapper["training_sha256"],
            original_excluded=packet["excluded"],
        )
    result = receipt(
        dict(
            schema_version=VERSION + "_preflight",
            contract=CONTRACT,
            status="PASS",
            source_commit=head,
            request_path=str(path),
            request_sha256=pin,
            packets=locators,
            training=training,
            fit_ledger_root=request["fit_ledger_root"],
            attempts=[dict(process=p, quarter=v["id"], arm="N") for p in (1, 2) for v in prior.VINTAGES],
            prediction_features_path=old["prediction_features_path"],
            prediction_features_sha256=features["receipt_sha256"],
            control_prediction_path=request["baseline_path"],
            control_prediction_sha256=baseline["receipt_sha256"],
            prediction_rows=len(features["prediction_dates"]) * len(features["catalog"]),
            fits=0,
            evaluation_outcomes_read=False,
            database_access=False,
            dataset_write=False,
        )
    )
    write_once(output / "preflight.json", result)
    return result


def load_packet(wrapper):
    core.verify_receipt(wrapper)
    require(
        wrapper["schema_version"] == VERSION + "_train"
        and wrapper["contract"] == CONTRACT
        and wrapper["vintage"] in prior.VINTAGES,
        "N TRAIN contract differs",
    )
    packet = asset(wrapper["original_packet"]["path"], wrapper["original_packet"]["sha256"])
    require(packet["vintage"] == wrapper["vintage"], "N quarter carrier swapped")
    x, y, weights, scaler = prior.within_training(packet, wrapper["reference_S"])
    require(training_identity(x, y, weights, scaler) == wrapper["training_sha256"], "N TRAIN math identity differs")
    require(np.isfinite(x.astype(np.float32)).all(), "float32 TRAIN overflow")
    return packet, x, y, weights, scaler


def identity_scaler(active):
    return dict(mean=[0.0] * 20, scale=[1.0 if v else 0.0 for v in active], active=list(active))


def pad(x, active):
    mask = np.asarray(active, dtype=bool)
    require(
        mask.shape == (20,)
        and x.ndim == 2
        and x.shape[1] == int(mask.sum())
        and np.isfinite(x).all()
        and np.isfinite(x.astype(np.float32)).all(),
        "invalid centered tree input",
    )
    result = np.zeros((len(x), 20))
    result[:, mask] = x
    return result


def validate_parameters(parameters):
    require(parameters["tree_parameters"] == PARAMS == trees.TREE_PARAMS, "N estimator parameters differ")
    scaler, tree = parameters["original_scaler"], parameters["tree"]
    trees.transform(np.zeros((1, 20)), scaler)
    require(all(tree[k] == v for k, v in identity_scaler(scaler["active"]).items()), "N identity tree scaler differs")
    require(len(tree["trees"]) == 64, "N tree count differs")
    # 核查所有节点（不只一条测试路径），包括不可达/环/不合法深度。
    width = sum(scaler["active"])
    for item in tree["trees"]:
        n = len(item["left"])
        require(
            1 <= n <= 7 and all(len(item[k]) == n for k in ("right", "feature", "threshold", "value")),
            "N tree shape differs",
        )
        require(
            all(type(v) is int for k in ("left", "right", "feature") for v in item[k])
            and all(type(v) in (int, float) and math.isfinite(v) for k in ("threshold", "value") for v in item[k]),
            "N invalid tree node",
        )
        seen, pending = set(), [(0, 0)]
        while pending:
            i, depth = pending.pop()
            require(i not in seen and 0 <= i < n and depth <= 2, "N cyclic/shared/deep tree")
            seen.add(i)
            left, right = item["left"][i], item["right"][i]
            if left == -1 or right == -1:
                require(left == right == -1 and item["feature"][i] == -2, "N malformed leaf")
            else:
                require(i < left < n and i < right < n and 0 <= item["feature"][i] < width, "N invalid branch")
                pending.extend(((left, depth + 1), (right, depth + 1)))
        require(len(seen) == n, "N unreachable tree nodes")
    trees.tree_predict(np.zeros((1, 20)), tree)


def fit_packet(wrapper, output, ledger, head, numeric):
    from sklearn.ensemble import GradientBoostingRegressor

    prior.validate_numeric(numeric)
    require(wrapper["source_commit"] == head, "N fit source differs")
    packet, x, y, weights, scaler = load_packet(wrapper)
    write_once(
        Path(ledger) / (packet["vintage"]["id"] + ".N.json"),
        receipt(dict(arm="N", packet_sha256=wrapper["receipt_sha256"], fit_kernel_attempts=1)),
    )
    fitted = GradientBoostingRegressor(**PARAMS).fit(x, y, sample_weight=weights)
    require(fitted.get_params() == PARAMS, "N sklearn parameters differ")
    forest, diagnostics = [], []
    dates = np.asarray([r[0] for r in packet["rows"]])
    for estimator in fitted.estimators_[:, 0]:
        t = estimator.tree_
        forest.append(
            dict(
                left=t.children_left.tolist(),
                right=t.children_right.tolist(),
                feature=t.feature.tolist(),
                threshold=t.threshold.tolist(),
                value=t.value[:, 0, 0].tolist(),
            )
        )
        leaves = estimator.apply(x)
        diagnostics.append(
            {
                str(i): dict(rows=int(np.count_nonzero(leaves == i)), distinct_dates=len(set(dates[leaves == i])))
                for i in sorted(set(leaves))
            }
        )
    tree = dict(
        **identity_scaler(scaler["active"]),
        trees=forest,
        init=float(fitted.init_.constant_[0, 0]),
        learning_rate=PARAMS["learning_rate"],
    )
    parameters = dict(original_scaler=scaler, tree=tree, tree_parameters=PARAMS)
    validate_parameters(parameters)
    require(
        np.array_equal(fitted.predict(x), trees.tree_predict(pad(x, scaler["active"]), tree)),
        "N JSON restore differs from sklearn",
    )
    model = receipt(
        dict(
            schema_version=VERSION + "_model",
            contract=CONTRACT,
            arm="N",
            vintage=packet["vintage"],
            parameters=parameters,
            model_sha256=canonical_sha256(parameters),
            training_sha256=wrapper["training_sha256"],
            packet_sha256=packet["receipt_sha256"],
            wrapper_sha256=wrapper["receipt_sha256"],
            mask_sha256=packet["mask_sha256"],
            reference_S_sha256=wrapper["reference_S"]["model_sha256"],
            reference_W_model_sha256=wrapper["reference_W_model_sha256"],
            catalog=packet["catalog"],
            release_binding=packet["release_binding"],
            source_commit=head,
            numeric_environment=numeric,
            fit_rows=len(y),
            fit_dates=len(set(dates)),
            sample_weight_sum=float(weights.sum()),
            leaf_diagnostics=diagnostics,
            fits=1,
            evaluation_outcomes_read=False,
        )
    )
    validate_model(model, head)
    write_once(Path(output) / "model.json", model)
    return model


def validate_model(model, head):
    core.verify_receipt(model)
    require(
        model["schema_version"] == VERSION + "_model"
        and model["contract"] == CONTRACT
        and model["arm"] == "N"
        and model["source_commit"] == head
        and model["vintage"] in prior.VINTAGES
        and model["model_sha256"] == canonical_sha256(model["parameters"])
        and model["fits"] == 1
        and model["evaluation_outcomes_read"] is False
        and len(model["catalog"]) == 131
        and model["catalog"] == sorted(set(model["catalog"]))
        and model["release_binding"]["manifest_sha256"] == CONTRACT["manifest_sha256"],
        "N model identity differs",
    )
    prior.validate_numeric(model["numeric_environment"])
    validate_parameters(model["parameters"])


def close_fits(processes, wrappers, preflight, head):
    require(
        len(processes) == 2 and canonical_json_bytes(processes[0]) == canonical_json_bytes(processes[1]),
        "N fresh process models differ",
    )
    require(set(processes[0]) == set(wrappers) == {v["id"] for v in prior.VINTAGES}, "N quarter set differs")
    for q, model in processes[0].items():
        validate_model(model, head)
        wrapper = wrappers[q]
        require(
            model["vintage"] == wrapper["vintage"]
            and wrapper["vintage"]["id"] == q
            and model["wrapper_sha256"] == wrapper["receipt_sha256"]
            and model["training_sha256"] == wrapper["training_sha256"],
            "N fit carrier differs",
        )
    return receipt(
        dict(
            schema_version=VERSION + "_fit_acceptance",
            contract=CONTRACT,
            status="PASS",
            source_commit=head,
            preflight_sha256=preflight["receipt_sha256"],
            models=processes[0],
            completed_fits=6,
            fresh_processes=2,
            features_sha256=preflight["prediction_features_sha256"],
            evaluation_outcomes_read=False,
        )
    )


def validate_acceptance(accepted, head):
    core.verify_receipt(accepted)
    require(
        accepted["schema_version"] == VERSION + "_fit_acceptance"
        and accepted["contract"] == CONTRACT
        and accepted["status"] == "PASS"
        and accepted["source_commit"] == head
        and accepted["completed_fits"] == 6
        and accepted["fresh_processes"] == 2
        and accepted["evaluation_outcomes_read"] is False
        and set(accepted["models"]) == {v["id"] for v in prior.VINTAGES},
        "N fit acceptance differs",
    )
    for q, model in accepted["models"].items():
        validate_model(model, head)
        require(model["vintage"]["id"] == q, "N accepted quarter swapped")


def freeze_groups(rows, baseline, days, catalog):
    expected = [(d, c) for d in days for c in catalog]
    require(
        [(r["trade_date"], r["sector_code"]) for r in rows] == expected
        and [(r["trade_date"], r["sector_code"]) for r in baseline] == expected,
        "N/C ex-ante grid differs",
    )
    grouped = {}
    for day in days:
        pairs = [
            (n["sector_code"], n["relative_member_return_score"], c["expected_member_loss"])
            for n, c in zip(rows, baseline, strict=True)
            if n["trade_date"] == day
        ]
        common = [(c, n, b) for c, n, b in pairs if n is not None and b is not None]
        a = [c for c, _, _ in common]
        grouped[day] = dict(
            codes=a,
            N=budget.risk_weights({c: -n for c, n, _ in common}) if a else None,
            C=budget.risk_weights({c: b for c, _, b in common}) if a else None,
            X=dict.fromkeys(a, math.ceil(0.20 * len(a)) / len(a)) if len(a) >= 2 else None,
        )
    return grouped


def predict(accepted, features, baseline, head):
    validate_acceptance(accepted, head)
    prior.validate_prediction_features(features)
    core.verify_receipt(baseline, PINS["baseline"])
    require(accepted["features_sha256"] == features["receipt_sha256"], "N forecast feature identity differs")
    rows, centers = [], {}
    for vintage in prior.VINTAGES:
        model = accepted["models"][vintage["id"]]
        require(
            model["catalog"] == features["catalog"] and model["release_binding"] == features["release_binding"],
            "N forecast source differs",
        )
        p = model["parameters"]
        for day in prior.schedule(features["calendar"], vintage)["predict"]:
            items = features["rows"][day]
            eligible = [c for c in features["catalog"] if items[c]["features"] is not None]
            values = {}
            if len(eligible) >= 2:
                z = trees.transform(np.asarray([items[c]["features"] for c in eligible]), p["original_scaler"])
                u = z - z.mean(axis=0)
                scores = trees.tree_predict(pad(u, p["original_scaler"]["active"]), p["tree"])
                values = dict(zip(eligible, scores.tolist(), strict=True))
            centers[day] = dict(codes=eligible, canonical_sha256=canonical_sha256(eligible))
            for code in features["catalog"]:
                value = values.get(code)
                reason = (
                    "insufficient_cross_section"
                    if items[code]["features"] is not None and len(eligible) < 2
                    else items[code]["reason_code"]
                )
                rows.append(
                    dict(
                        trade_date=day,
                        as_of_date=features["as_of"][day],
                        sector_code=code,
                        vintage_id=vintage["id"],
                        model_sha256=model["model_sha256"],
                        relative_member_return_score=value,
                        raw_score=value,
                        availability="available" if value is not None else "unavailable",
                        reason_code=reason,
                    )
                )
    groups = freeze_groups(rows, baseline["predictions"]["C"], features["prediction_dates"], features["catalog"])
    return receipt(
        dict(
            schema_version=VERSION + "_predictions",
            contract=CONTRACT,
            source_commit=head,
            fit_acceptance_sha256=accepted["receipt_sha256"],
            features_sha256=features["receipt_sha256"],
            baseline_prediction_sha256=baseline["receipt_sha256"],
            release_binding=features["release_binding"],
            predictions=rows,
            centering_sets=centers,
            ex_ante_groups=groups,
            ex_ante_groups_sha256=canonical_sha256(groups),
            fits=0,
            evaluation_outcomes_read=False,
            policy_applied=False,
        )
    )


def compare_groups(groups, labels):
    return {
        name: budget.paired_value({"M": groups[a], "C20": groups[b]}, labels)
        for name, a, b in (("N_minus_C", "N", "C"), ("N_minus_X", "N", "X"), ("C_minus_X", "C", "X"))
    }


def relabel_statistics(value):
    """仅适配旧纯统计函数的W角色；绝不搬用其模型/schema/终态。"""
    if isinstance(value, dict):
        return {k.replace("W", "N"): relabel_statistics(v) for k, v in value.items()}
    if isinstance(value, list):
        return [relabel_statistics(v) for v in value]
    return value


def evaluate(p, accepted, prediction, sealed, head):
    validate_acceptance(accepted, head)
    for value in (p, prediction, sealed):
        core.verify_receipt(value)
    require(
        sealed["schema_version"] == VERSION + "_prediction_acceptance"
        and sealed["status"] == "PASS"
        and sealed["fresh_processes"] == 2
        and sealed["evaluation_outcomes_read"] is False
        and sealed["prediction_sha256"] == prediction["receipt_sha256"]
        and sealed["fit_acceptance_sha256"] == accepted["receipt_sha256"]
        and prediction["fit_acceptance_sha256"] == accepted["receipt_sha256"]
        and accepted["preflight_sha256"] == p["receipt_sha256"]
        and p["source_commit"] == prediction["source_commit"] == sealed["source_commit"] == head,
        "N all models/forecasts must be sealed before outcome access",
    )
    _, prepared, _, _, baseline, features, values = load_request(p["request_path"], p["request_sha256"])
    groups = freeze_groups(
        prediction["predictions"], baseline["predictions"]["C"], features["prediction_dates"], features["catalog"]
    )
    require(
        groups == prediction["ex_ante_groups"]
        and canonical_sha256(groups) == prediction["ex_ante_groups_sha256"]
        and prediction["features_sha256"] == features["receipt_sha256"]
        and prediction["baseline_prediction_sha256"] == baseline["receipt_sha256"],
        "N frozen ex-ante grouping differs",
    )
    request, original, _, cached, _, _ = prior.load_request(prepared["request_path"], prepared["request_sha256"])
    cohort = member.pinned_json(request["cohort_path"], prior.PINS["cohort_file"])
    core.verify_receipt(cohort, prior.PINS["cohort"])
    source = member.FrozenMemberSource(
        cached, request, provider_absence_manifest=Path(original["source"]["provider_absence_manifest"])
    )
    days, calendar = features["prediction_dates"], features["calendar"]
    labels = source.targets(days, CONTRACT["prediction_end"])
    daily = {d: compare_groups(groups[d], labels[d]) for d in days}

    def summary(selected, data=daily):
        return {
            name: dict(
                **{
                    k: prior.hac({d: data[d][name][k] for d in selected}, calendar)
                    for k in ("J", "avoided_loss", "missed_upside")
                },
                undefined=dict(Counter(data[d][name]["reason"] for d in selected if data[d][name]["reason"])),
            )
            for name in ("N_minus_C", "N_minus_X", "C_minus_X")
        }

    # 仅调用模型无关的统计算术；丢弃旧schema/contract/status。
    diagnostics = prior.within_metrics(prediction, baseline, labels, features, cohort)
    w_metrics = prior.within_metrics(values["w_prediction"], baseline, labels, features, cohort)
    wanted = (
        "l2",
        "cohort",
        "daily_l2",
        "daily_cohort",
        "daily_errors",
        "groups",
        "auxiliary",
        "seed_diagnostics",
        "evaluation_counts",
        "cohort_na",
        "cohort_source_rows",
        "cohort_unique_keys",
    )
    stats = relabel_statistics({k: diagnostics[k] for k in wanted})
    paired_nw = {
        d: (diagnostics["daily_l2"][d]["W"] - w_metrics["daily_l2"][d]["W"])
        if all(m["daily_l2"][d]["W"] is not None for m in (diagnostics, w_metrics))
        else None
        for d in days
    }
    adjusted = budget.adjusted_targets(source, days, labels)
    adjusted_daily = {d: compare_groups(groups[d], adjusted[d]) for d in days}
    full = summary(days)
    mean = full["N_minus_C"]["J"]["mean"]
    status = (
        "INSUFFICIENT_EVALUABLE_EVIDENCE"
        if mean is None
        else ("STOP_NO_INCREMENTAL_VALUE_REFERENCE" if mean <= 0 else "RESEARCH_LEAD_REQUIRES_VALUE_REVIEW")
    )
    source.unchanged()
    return receipt(
        dict(
            schema_version=VERSION + "_evaluation",
            contract=CONTRACT,
            status=status,
            source_commit=head,
            preflight_sha256=p["receipt_sha256"],
            fit_acceptance_sha256=accepted["receipt_sha256"],
            prediction_sha256=prediction["receipt_sha256"],
            ex_ante_groups_sha256=prediction["ex_ante_groups_sha256"],
            full=full,
            quarters={
                v["id"]: summary([d for d in days if v["effective_start"] <= d <= v["effective_end"]])
                for v in prior.VINTAGES
            },
            daily=daily,
            diagnostics=stats,
            frozen_W=dict(
                l2=w_metrics["l2"],
                auxiliary=w_metrics["auxiliary"],
                paired_N_minus_W_rank_ic=prior.hac(paired_nw, calendar),
            ),
            adjusted_price_sensitivity=dict(
                same_frozen_groups=True, primary_unchanged=True, full=summary(days, adjusted_daily)
            ),
            evaluation_source_identity=source.identity(),
            availability_reasons=dict(
                Counter(r["reason_code"] or r["availability"] for r in prediction["predictions"])
            ),
            fits=0,
            database_access=False,
            dataset_write=False,
            qe_submitted=False,
            automatic_promotion=False,
            limitations=[
                "已消费历史/回溯分类/NON_VINTAGE，不是未消费holdout或严格historical-as-known。",
                "142日预测、最多132成熟；Q4仅7成熟日；重叠10日gross预算参考不是NAV/净收益。",
                "固定新买队列/三seed不独立，不重建旧账户或自动接QE。",
            ],
        )
    )
