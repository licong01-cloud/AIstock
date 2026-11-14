"""One approved earnings-information increment; matched C20, at most 12 fits.

Only the financial covariate is new. Immutable C carriers, member facts, Ridge,
rank grouping and adjusted-price diagnostic kernels retain their old semantics.
"""

from __future__ import annotations

from collections import Counter
import math
from pathlib import Path

import numpy as np
from scipy.stats import rankdata

from . import earnings_information_input as inp
from . import risk_l2_member_return as prior
from .contracts import ALL_CORE_FEATURES, canonical_sha256
from .formal_state_executor import validate_output_location, write_once
from .formal_state_model import receipt
from .risk_l2_margin_information import adjusted_targets, risk_weights

member, loss, core, require = prior.member, prior.loss, prior.core, prior.require
VERSION = "hmm_risk_l2_earnings_growth_member_loss_ridge_v1"
ARMS = ("C20", "E21")
SIGNED_REQUEST_PIN = "b537a156c4d36ef3561df1cabe0121ebb538b9cfccdfbc64f71d93892e90339c"
PREPARED_PIN = "06d3779b02a8fa295827f6d0e396c5f95c99f0ec7b1ed31ba414811b5f6bc82a"
FIT_LEDGER_ROOT = "F:/Dev/AIstock_runtime/hmm_l2_earnings_information/20261114/formal_fit_attempts"
CONTRACT = dict(
    version=VERSION,
    feature_names=[*ALL_CORE_FEATURES, "earnings_growth_rank"],
    parameters=loss.PARAMS,
    vintages=list(prior.VINTAGES),
    target=member.TARGET,
    announcement_clock="ann_date_strictly_before_previous_open_session",
    expiry_sessions_from_report_period=252,
    version_rule="earliest_first_seen_field_consensus_before_source_cutoff",
    common_mask="original_C_TRAIN_mask_intersect_ex_ante_earnings_observation",
    primary="date_equal_mean_incremental_avoided_loss_minus_missed_upside_fixed_131",
    risk_fraction=0.20,
    max_fits=12,
    numeric_versions=loss.EXPECTED_VERSIONS,
    manifest_sha256=prior.PINS["manifest"],
    financial_manifest_file_sha256=inp.PACKAGE_PIN,
    financial_projection_sha256=canonical_sha256(inp.PROJECTION),
    historical_as_known=False,
    untouched_holdout=False,
    policy_applied=False,
)
REQUEST_FIELDS = {
    "schema_version",
    "contract",
    "dataset_root",
    "package_root",
    "authority_path",
    "financial_source_projection",
    "signed_request_path",
    "signed_request_sha256",
    "prepared_path",
    "prepared_sha256",
    "fit_ledger_root",
}


def load_source(request_path, request_pin):
    request = member.pinned_json(request_path, request_pin)
    require(
        set(request) == REQUEST_FIELDS
        and request["schema_version"] == VERSION + "_request"
        and request["contract"] == CONTRACT
        and request["financial_source_projection"] == inp.PROJECTION
        and request["signed_request_sha256"] == SIGNED_REQUEST_PIN
        and request["prepared_sha256"] == PREPARED_PIN
        and Path(request["fit_ledger_root"]).resolve() == Path(FIT_LEDGER_ROOT).resolve(),
        "unapproved earnings request",
    )
    signed, original, _, cached, _, _ = prior.load_request(request["signed_request_path"], SIGNED_REQUEST_PIN)
    old, packets, _, features = prior.load_prepared(request["prepared_path"], PREPARED_PIN)
    require(
        old["request_sha256"] == SIGNED_REQUEST_PIN
        and Path(request["dataset_root"]).resolve() == Path(cached["source"]["candidate_root"]).resolve()
        and features["release_binding"] == cached["source"]["release_binding"],
        "earnings release differs",
    )
    validate_output_location(Path(request["fit_ledger_root"]), dataset_root=Path(request["dataset_root"]))
    return request, signed, original, cached, old, packets, features


def aggregate(members, observations):
    """Rank over current original-C members only, not a future-return subset."""
    population = [s for group in members.values() for s, _ in group]
    require(len(population) == len(set(population)), "duplicate cross-sector member")
    require(set(population) == set(observations), "earnings observation population differs")
    observed = sorted(s for s in population if observations[s]["value"] is not None)
    require(
        all(math.isfinite(observations[s]["value"]) and observations[s]["reason"] is None for s in observed),
        "invalid observed earnings growth",
    )
    ranks = (
        dict(
            zip(
                observed,
                (rankdata([observations[s]["value"] for s in observed], method="average") - 1) / (len(observed) - 1)
                - 0.5,
                strict=True,
            )
        )
        if len(observed) >= 2
        else {}
    )
    output = {}
    for code, group in members.items():
        require(all(math.isfinite(v) and v > 0 for _, v in group), "invalid original cap")
        subset = [(s, v) for s, v in group if s in ranks]
        total, capital = math.fsum(v for _, v in group), math.fsum(v for _, v in subset)
        reasons = Counter(observations[s]["reason"] for s, _ in group if observations[s]["reason"])
        ages = [observations[s]["age_sessions"] for s, _ in subset if "age_sessions" in observations[s]]
        output[code] = dict(
            value=float(math.fsum(v * ranks[s] for s, v in subset) / capital) if subset else None,
            reason=None
            if subset
            else "RANK_POPULATION_TOO_SMALL"
            if len(observed) < 2
            else "NO_OBSERVED_EARNINGS_MEMBER",
            members=len(group),
            observed=len(subset),
            observed_capital_fraction=capital / total if total else None,
            rank_population=len(observed),
            member_sha256=canonical_sha256(group),
            observed_sha256=canonical_sha256([(s, v, observations[s]) for s, v in subset]),
            age_range=[min(ages), max(ages)] if ages else None,
            report_periods=dict(Counter(observations[s].get("period") for s, _ in subset)),
            reasons=dict(reasons),
        )
    return output


def design(rows, values, arm):
    require(arm in ARMS and len(rows) == len(values) > 0, "empty/invalid matched TRAIN")
    z, y, weights, scaler = loss._ridge_inputs([r[:3] + [r[4]] for r in rows])
    require(np.all((y >= 0) & (y < 1)), "invalid original C target")
    if arm == "E21":
        v = np.asarray(values, dtype=np.float64)
        require(np.isfinite(v).all() and np.all(np.abs(v) <= 0.5), "invalid earnings rank feature")
        active = bool(np.any(v != v[0]))
        mean, scale = float(v.mean()) if active else float(v[0]), float(v.std(ddof=0)) if active else 0.0
        require(math.isfinite(mean) and math.isfinite(scale) and (not active or scale > 0), "earnings scaler invalid")
        if active:
            z = np.column_stack([z, (v - mean) / scale])
        scaler = {k: [*scaler[k], x] for k, x in dict(mean=mean, scale=scale, active=active).items()}
    return z, y, weights, scaler


def parameters(p, arm):
    n = 21 if arm == "E21" else 20
    require(
        arm in ARMS
        and set(p) == {"mean", "scale", "active", "coef", "intercept"}
        and all(type(x) is bool for x in p["active"]),
        "model parameter schema differs",
    )
    mean, scale, coef = (np.asarray(p[k], dtype=float) for k in ("mean", "scale", "coef"))
    active = np.asarray(p["active"], dtype=bool)
    require(
        mean.shape == scale.shape == active.shape == (n,)
        and active.any()
        and coef.shape == (int(active.sum()),)
        and np.all(scale[active] > 0)
        and np.all(scale[~active] == 0)
        and all(np.isfinite(x).all() for x in (mean, scale, coef))
        and type(p["intercept"]) in (int, float)
        and math.isfinite(p["intercept"]),
        "nonfinite/invalid model",
    )
    return mean, scale, active, coef, p["intercept"]


def prepare(request_path, request_pin, output, head):
    request, signed, original, cached, old, packets, features = load_source(request_path, request_pin)
    calendar = features["calendar"]
    state = inp.load(
        request["package_root"], request["authority_path"], request["financial_source_projection"], calendar
    )
    decisions = sorted(set(features["prediction_dates"]) | {r[0] for p in packets.values() for r in p["rows"]})
    as_of = {d: calendar[calendar.index(d) - 1] for d in decisions}
    source = member.FrozenMemberSource(
        {**cached, "as_of": as_of},
        signed,
        provider_absence_manifest=Path(original["source"]["provider_absence_manifest"]),
    )
    rows = {}
    for day in decisions:
        members = source.members(day)
        observations = {s: state.at(s, as_of[day]) for group in members.values() for s, _ in group}
        rows[day] = aggregate(members, observations)
    source.unchanged()
    state.files.unchanged()
    extra = receipt(
        dict(
            schema_version=VERSION + "_feature",
            contract=CONTRACT,
            calendar=calendar,
            catalog=features["catalog"],
            as_of=as_of,
            rows=rows,
            source=state.evidence,
            original_member_source=source.identity(),
            fits=0,
            evaluation_outcomes_read=False,
        )
    )
    output = validate_output_location(Path(output), dataset_root=Path(request["dataset_root"]))
    write_once(output / "earnings.json", extra)
    locators, training = {}, {}
    for vintage in prior.VINTAGES:
        q, packet = vintage["id"], packets[vintage["id"]]
        indices = [i for i, r in enumerate(packet["rows"]) if rows[r[0]][r[1]]["value"] is not None]
        selected = [packet["rows"][i] for i in indices]
        values = [rows[r[0]][r[1]]["value"] for r in selected]
        s20, s21 = design(selected, values, "C20")[3], design(selected, values, "E21")[3]
        require(all(s20[k] == s21[k][:20] for k in s20), "common scaler differs")
        wrapper = receipt(
            dict(
                schema_version=VERSION + "_train",
                contract=CONTRACT,
                vintage=vintage,
                original_packet=old["packets"][q],
                indices=indices,
                earnings_values=values,
                mask_sha256=canonical_sha256([r[:2] for r in selected]),
                feature_sha256=extra["receipt_sha256"],
                source_commit=head,
            )
        )
        path = output / (q + ".json")
        write_once(path, wrapper)
        locators[q] = dict(path=str(path), sha256=wrapper["receipt_sha256"])
        training[q] = dict(
            original_rows=len(packet["rows"]),
            matched_rows=len(selected),
            matched_dates=len({r[0] for r in selected}),
            mask_sha256=wrapper["mask_sha256"],
            local_feature_na=len(packet["rows"]) - len(selected),
            original_excluded=packet["excluded"],
        )
    result = receipt(
        dict(
            schema_version=VERSION + "_preflight",
            contract=CONTRACT,
            status="PASS",
            source_commit=head,
            request_path=str(request_path),
            request_sha256=request_pin,
            packets=locators,
            training=training,
            feature_path=str(output / "earnings.json"),
            feature_sha256=extra["receipt_sha256"],
            prediction_features_path=old["prediction_features_path"],
            prediction_features_sha256=features["receipt_sha256"],
            fit_ledger_root=request["fit_ledger_root"],
            attempts=attempts(),
            prediction_rows=len(features["prediction_dates"]) * len(features["catalog"]),
            fits=0,
            evaluation_outcomes_read=False,
            database_access=False,
            dataset_write=False,
        )
    )
    require(result["prediction_rows"] == 18602, "full 131 by 142 prediction grid absent")
    write_once(output / "preflight.json", result)
    return result


def attempts():
    return [dict(process=p, quarter=v["id"], arm=a) for p in (1, 2) for v in prior.VINTAGES for a in ARMS]


def load_packet(wrapper):
    core.verify_receipt(wrapper)
    require(
        wrapper["contract"] == CONTRACT
        and wrapper["schema_version"] == VERSION + "_train"
        and wrapper["vintage"] in prior.VINTAGES,
        "earnings packet identity differs",
    )
    packet = core.inputs._asset(wrapper["original_packet"]["path"], wrapper["original_packet"]["sha256"])
    original = prior.validate_packet(packet)
    indices = wrapper["indices"]
    require(
        packet["vintage"] == wrapper["vintage"]
        and indices == sorted(set(indices))
        and all(type(i) is int and 0 <= i < len(original) for i in indices),
        "common mask index differs",
    )
    rows = [original[i] for i in indices]
    require(canonical_sha256([r[:2] for r in rows]) == wrapper["mask_sha256"], "common mask hash differs")
    design(rows, wrapper["earnings_values"], "E21")
    return rows


def fit_packet(wrapper, ledger, head, numeric):
    from sklearn.linear_model import Ridge

    prior.validate_numeric(numeric)
    rows = load_packet(wrapper)
    require(wrapper["source_commit"] == head, "TRAIN source differs")
    models = {}
    for arm in ARMS:
        z, y, weights, scaler = design(rows, wrapper["earnings_values"], arm)
        # O_EXCL before the actual kernel: failed fit consumes its one attempt.
        write_once(
            Path(ledger) / (wrapper["vintage"]["id"] + "." + arm + ".json"),
            receipt(dict(packet_sha256=wrapper["receipt_sha256"], arm=arm, fit_kernel_attempts=1)),
        )
        fitted = Ridge(**loss.PARAMS).fit(z, y, sample_weight=weights)
        p = dict(**scaler, coef=fitted.coef_.tolist(), intercept=float(fitted.intercept_))
        parameters(p, arm)
        models[arm] = receipt(
            dict(
                schema_version=VERSION + "_model",
                contract=CONTRACT,
                arm=arm,
                vintage=wrapper["vintage"],
                parameters=p,
                model_sha256=canonical_sha256(p),
                packet_sha256=wrapper["receipt_sha256"],
                mask_sha256=wrapper["mask_sha256"],
                feature_sha256=wrapper["feature_sha256"],
                source_commit=head,
                fit_rows=len(rows),
                fit_dates=len({r[0] for r in rows}),
                sample_weight_sum=float(weights.sum()),
                training_target_mean=float(np.average(y, weights=weights)),
            )
        )
    return receipt(
        dict(
            schema_version=VERSION + "_fit",
            contract=CONTRACT,
            models=models,
            source_commit=head,
            fits=2,
            numeric_environment=numeric,
            evaluation_outcomes_read=False,
        )
    )


def validate_models(models, vintage, head):
    require(set(models) == set(ARMS), "matched model set incomplete")
    for arm, model in models.items():
        core.verify_receipt(model)
        require(
            model["schema_version"] == VERSION + "_model"
            and model["contract"] == CONTRACT
            and model["arm"] == arm
            and model["vintage"] == vintage
            and model["source_commit"] == head
            and model["model_sha256"] == canonical_sha256(model["parameters"]),
            "model identity differs",
        )
        parameters(model["parameters"], arm)
    a, b = (models[k] for k in ARMS)
    require(
        all(a[k] == b[k] for k in ("packet_sha256", "mask_sha256", "feature_sha256", "fit_rows", "fit_dates"))
        and all(a["parameters"][k] == b["parameters"][k][:20] for k in ("mean", "scale", "active")),
        "matched TRAIN/scalers differ",
    )


def close_fits(groups, wrappers, p, head):
    require(len(groups) == 2 and groups[0] == groups[1], "fresh-process models differ")
    for group in groups:
        require(set(group) == {v["id"] for v in prior.VINTAGES}, "quarter set incomplete")
        for v in prior.VINTAGES:
            run, wrapper = group[v["id"]], wrappers[v["id"]]
            core.verify_receipt(run)
            prior.validate_numeric(run["numeric_environment"])
            require(
                run["contract"] == CONTRACT
                and run["fits"] == 2
                and run["source_commit"] == head
                and run["evaluation_outcomes_read"] is False,
                "fit result/budget differs",
            )
            validate_models(run["models"], v, head)
            require(
                all(
                    m["packet_sha256"] == wrapper["receipt_sha256"]
                    and m["mask_sha256"] == wrapper["mask_sha256"]
                    and m["feature_sha256"] == p["feature_sha256"]
                    for m in run["models"].values()
                ),
                "fit carrier differs",
            )
    return receipt(
        dict(
            schema_version=VERSION + "_fit_acceptance",
            contract=CONTRACT,
            status="PASS",
            source_commit=head,
            preflight_sha256=p["receipt_sha256"],
            completed_fits=12,
            models={q: r["models"] for q, r in groups[0].items()},
            fresh_processes=2,
            evaluation_outcomes_read=False,
            automatic_promotion=False,
        )
    )


def validate_acceptance(a, head):
    core.verify_receipt(a)
    require(
        a["schema_version"] == VERSION + "_fit_acceptance"
        and a["contract"] == CONTRACT
        and a["status"] == "PASS"
        and a["source_commit"] == head
        and a["fresh_processes"] == 2
        and a["completed_fits"] == 12
        and set(a["models"]) == {v["id"] for v in prior.VINTAGES},
        "unsealed fit set",
    )
    for v in prior.VINTAGES:
        validate_models(a["models"][v["id"]], v, head)


def freeze_groups(rows, days, catalog):
    require(
        [(r["trade_date"], r["sector_code"]) for r in rows] == [(d, c) for d in days for c in catalog],
        "full prediction grid differs",
    )
    groups = {}
    for day in days:
        today = [r for r in rows if r["trade_date"] == day]
        common = [r for r in today if all(r[a] is not None for a in ARMS)]
        weights = {a: risk_weights({r["sector_code"]: r[a] for r in common}) if common else None for a in ARMS}
        weights["X"] = (
            dict.fromkeys([r["sector_code"] for r in common], math.ceil(0.2 * len(common)) / len(common))
            if common
            else None
        )
        groups[day] = weights
    return groups


def predict(accepted, features, extra, head):
    validate_acceptance(accepted, head)
    core.verify_receipt(extra)
    prior.validate_prediction_features(features)
    require(
        extra["contract"] == CONTRACT
        and extra["catalog"] == features["catalog"]
        and extra["calendar"] == features["calendar"],
        "prediction features differ",
    )
    rows = []
    for day in features["prediction_dates"]:
        vintage = next(v for v in prior.VINTAGES if v["effective_start"] <= day <= v["effective_end"])
        for code in features["catalog"]:
            old, extra_row = features["rows"][day][code], extra["rows"][day][code]
            require(extra["as_of"][day] == old["as_of_date"] == features["as_of"][day], "decision/as-of clock differs")
            row = dict(
                trade_date=day,
                as_of_date=old["as_of_date"],
                sector_code=code,
                reasons={},
                availability={},
                C20=None,
                E21=None,
                raw_C20=None,
                raw_E21=None,
            )
            for arm in ARMS:
                model = accepted["models"][vintage["id"]][arm]
                require(model["feature_sha256"] == extra["receipt_sha256"], "earnings carrier changed")
                reason = (
                    old["reason_code"] if old["features"] is None else extra_row["reason"] if arm == "E21" else None
                )
                if reason:
                    row["reasons"][arm] = reason
                    row["availability"][arm] = "UNAVAILABLE"
                    continue
                vector = old["features"] + ([extra_row["value"]] if arm == "E21" else [])
                mean, scale, active, coef, intercept = parameters(model["parameters"], arm)
                x = np.asarray(vector, dtype=float)
                require(x.shape == mean.shape and np.isfinite(x).all(), "prediction vector invalid")
                raw = float(((x[active] - mean[active]) / scale[active]) @ coef + intercept)
                require(math.isfinite(raw), "nonfinite prediction")
                row["raw_" + arm], row[arm] = raw, min(1.0, max(0.0, raw))
                row["availability"][arm] = "AVAILABLE"
            rows.append(row)
    groups = freeze_groups(rows, features["prediction_dates"], features["catalog"])
    return receipt(
        dict(
            schema_version=VERSION + "_predictions",
            contract=CONTRACT,
            source_commit=head,
            fit_acceptance_sha256=accepted["receipt_sha256"],
            feature_sha256=extra["receipt_sha256"],
            features_sha256=features["receipt_sha256"],
            rows=rows,
            ex_ante_groups=groups,
            ex_ante_groups_sha256=canonical_sha256(groups),
            fits=0,
            evaluation_outcomes_read=False,
            policy_applied=False,
            historical_as_known=False,
            untouched_holdout=False,
        )
    )


def paired(first, control, labels):
    reason = None
    if all(v["status"] == "OUTCOME_NOT_MATURE" for v in labels.values()):
        reason = "OUTCOME_NOT_MATURE"
    elif first is None or control is None:
        reason = "NO_EX_ANTE_ORDERING"
    if reason:
        return dict(J=None, avoided_loss=None, missed_upside=None, reason=reason)
    require(set(first) == set(control) <= set(labels), "paired ex-ante sets differ")
    terms = []
    for code in first:
        delta = first[code] - control[code]
        if delta == 0:
            continue  # Pairwise algebra, not an imputed asset return.
        label = labels[code]
        if label["status"] != "AVAILABLE":
            return dict(J=None, avoided_loss=None, missed_upside=None, reason=label["reason"])
        prior.signed_label(label)
        terms.append((delta * label["C"], delta * label["upside"]))
    avoided, missed = (math.fsum(t[i] for t in terms) / 131 for i in (0, 1))
    return dict(J=avoided - missed, avoided_loss=avoided, missed_upside=missed, reason=None)


def statistics(values, calendar):
    """HAC needs >=2 dates; the point-mean resource rule does not add that gate."""
    result = prior.hac(values, calendar)
    finite = [x for x in values.values() if x is not None]
    if len(finite) == 1:
        result["mean"] = float(finite[0])
    return result


def evaluate(p, accepted, prediction, sealed, head):
    core.verify_receipt(p)
    validate_acceptance(accepted, head)
    core.verify_receipt(prediction)
    core.verify_receipt(sealed)
    require(
        sealed["schema_version"] == VERSION + "_prediction_acceptance"
        and sealed["status"] == "PASS"
        and sealed["source_commit"] == prediction["source_commit"] == head
        and sealed["fresh_processes"] == 2
        and sealed["prediction_sha256"] == prediction["receipt_sha256"]
        and sealed["fit_acceptance_sha256"] == prediction["fit_acceptance_sha256"] == accepted["receipt_sha256"]
        and accepted["preflight_sha256"] == p["receipt_sha256"]
        and canonical_sha256(prediction["ex_ante_groups"]) == prediction["ex_ante_groups_sha256"],
        "predictions/groups must be sealed before evaluation outcomes",
    )
    request, signed, original, cached, _, _, features = load_source(p["request_path"], p["request_sha256"])
    days, catalog, calendar = features["prediction_dates"], features["catalog"], features["calendar"]
    require(
        prediction["contract"] == CONTRACT
        and prediction["features_sha256"] == features["receipt_sha256"]
        and prediction["feature_sha256"] == p["feature_sha256"]
        and freeze_groups(prediction["rows"], days, catalog) == prediction["ex_ante_groups"],
        "full prediction grid or sealed grouping differs",
    )
    source = member.FrozenMemberSource(
        cached, signed, provider_absence_manifest=Path(original["source"]["provider_absence_manifest"])
    )
    labels = source.targets(days, prior.CONTRACT["prediction_end"])  # First evaluation outcome read.
    adjusted = adjusted_targets(source, days, labels)
    rows = {(r["trade_date"], r["sector_code"]): r for r in prediction["rows"]}
    daily, sensitivity, errors = {}, {}, {a: [] for a in ARMS}
    for day in days:
        groups = prediction["ex_ante_groups"][day]
        daily[day] = dict(
            **paired(groups["E21"], groups["C20"], labels[day]),
            against_X={a: paired(groups[a], groups["X"], labels[day]) for a in ARMS},
            rank_ic={},
        )
        sensitivity[day] = paired(groups["E21"], groups["C20"], adjusted[day])
        for arm in ARMS:
            pairs = [
                (rows[day, c][arm], labels[day][c]["C"])
                for c in catalog
                if labels[day][c]["status"] == "AVAILABLE" and all(rows[day, c][a] is not None for a in ARMS)
            ]
            daily[day]["rank_ic"][arm] = prior.rank_ic([x for x, _ in pairs], [y for _, y in pairs])
            errors[arm].extend(x - y for x, y in pairs)

    def summary(selected):
        return dict(
            dates=len(selected),
            value={
                k: statistics({d: daily[d][k] for d in selected}, calendar)
                for k in ("J", "avoided_loss", "missed_upside")
            },
            against_X={
                a: {
                    k: statistics({d: daily[d]["against_X"][a][k] for d in selected}, calendar)
                    for k in ("J", "avoided_loss", "missed_upside")
                }
                for a in ARMS
            },
            rank_ic={a: prior.hac({d: daily[d]["rank_ic"][a] for d in selected}, calendar) for a in ARMS},
            undefined=dict(Counter(daily[d]["reason"] for d in selected if daily[d]["reason"])),
        )

    cohort = member.pinned_json(signed["cohort_path"], prior.PINS["cohort_file"])
    core.verify_receipt(cohort, prior.PINS["cohort"])
    unique = {}
    for r in cohort["rows"]:
        key, fact = (r["date"], r["stock"]), (r["code"], r["stock_return"], r["reason"])
        require(key not in unique or unique[key] == fact, "cohort cross-seed facts differ")
        unique[key] = fact
    cohort_daily, cohort_missing = {}, Counter()
    for day in sorted({k[0] for k in unique}):
        observed = []
        for (d, _), (code, value, reason) in unique.items():
            if d != day:
                continue
            require((d, code) in rows, "cohort escapes prediction grid")
            if value is None:
                require(bool(reason), "untyped cohort outcome NA")
                cohort_missing[reason] += 1
            elif all(rows[d, code][a] is not None for a in ARMS):
                observed.append((rows[d, code], max(0.0, -value)))
            else:
                cohort_missing["PREDICTION_UNAVAILABLE"] += 1
        cohort_daily[day] = {a: prior.rank_ic([r[a] for r, _ in observed], [v for _, v in observed]) for a in ARMS}
    full = summary(days)
    mean = full["value"]["J"]["mean"]
    status = (
        "INSUFFICIENT_EVALUABLE_EVIDENCE"
        if mean is None
        else ("STOP_NO_INCREMENTAL_VALUE_REFERENCE" if mean <= 0 else "RESEARCH_LEAD_REQUIRES_VALUE_AND_VINTAGE_REVIEW")
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
            errors={
                a: dict(mse=float(np.mean(np.square(v))) if v else None, mae=float(np.mean(np.abs(v))) if v else None)
                for a, v in errors.items()
            },
            adjusted_price_sensitivity={
                k: statistics({d: sensitivity[d][k] for d in days}, calendar)
                for k in ("J", "avoided_loss", "missed_upside")
            },
            cohort=dict(
                unique=len(unique),
                rows=len(cohort["rows"]),
                missing=dict(cohort_missing),
                stats={a: prior.hac({d: row[a] for d, row in cohort_daily.items()}, calendar) for a in ARMS},
                fixed_reference_not_account_return=True,
            ),
            training=p["training"],
            label_reasons=dict(Counter(x["reason"] or x["status"] for g in labels.values() for x in g.values())),
            prediction_reasons=dict(Counter(reason for r in prediction["rows"] for reason in r["reasons"].values())),
            evaluation_source_identity=source.identity(),
            fits=0,
            database_access=False,
            dataset_write=False,
            automatic_promotion=False,
            limitations=[
                "NON_VINTAGE backcast classification and financial revisions.",
                "Overlapping fixed-budget gross reference, not daily NAV, tradable strategy or net profit.",
                "142 prediction days; at most 132 mature; Q4 has only seven mature days.",
                "New to original 20D does not prove incremental information relative to QE AF.",
            ],
        )
    )
