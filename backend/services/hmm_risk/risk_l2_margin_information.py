"""One approved financing-information increment, matched TRAIN masks, no policy.

Reuse immutable 20D/C carriers. Only the new financing covariate is constructed;
no legacy model, dataset or global feature contract is changed.
"""

from __future__ import annotations

from collections import Counter
import math
from pathlib import Path

import numpy as np
import pandas as pd

from . import risk_l2_member_return as prior
from .contracts import ALL_CORE_FEATURES, canonical_json_bytes, canonical_sha256
from .formal_state_executor import validate_output_location, write_once
from .formal_state_model import receipt
from .margin_information_input import MARGIN_SOURCE, load_margin_source_binding

member, loss, require, core = prior.member, prior.loss, prior.require, prior.core
VERSION = "hmm_risk_l2_margin_balance_member_loss_ridge_v1"
FEATURE = "margin_balance_ratio"
PACKAGE_PIN = "c65825014e2137770473877a0add20669300b09d7ab239cd77a44554a0564de0"
CONTRACT = dict(
    version=VERSION,
    feature_names=[*ALL_CORE_FEATURES, FEATURE],
    parameters=loss.PARAMS,
    vintages=list(prior.VINTAGES),
    target=member.TARGET,
    financing_lag_open_sessions=2,
    balance_field="md_rzye",
    market_cap_unit="CNY",
    raw_market_cap_multiplier=10000,
    common_mask="original_C_TRAIN_mask_intersect_ex_ante_financing_observation",
    primary="date_equal_mean_incremental_avoided_loss_minus_missed_upside_fixed_131",
    risk_fraction=0.20,
    max_fits=12,
    numeric_versions=loss.EXPECTED_VERSIONS,
    manifest_sha256=prior.PINS["manifest"],
    shared_package_file_sha256=PACKAGE_PIN,
    historical_as_known=False,
    untouched_holdout=False,
    policy_applied=False,
)
REQUEST_FIELDS = {
    "schema_version",
    "contract",
    "dataset_root",
    "package_root",
    "fit_ledger_root",
    "prepared_path",
    "prepared_sha256",
    "controls_path",
    "controls_sha256",
    "cached_features_path",
    "cached_features_sha256",
    "baseline_path",
    "baseline_sha256",
    "cohort_path",
}


def load_request(path, pin):
    request = member.pinned_json(path, pin)
    require(
        set(request) == REQUEST_FIELDS
        and request["schema_version"] == VERSION + "_request"
        and request["contract"] == CONTRACT,
        "unapproved financing request",
    )
    require(
        request["prepared_sha256"] == prior.WITHIN_PINS["prepared"]
        and request["controls_sha256"] == prior.WITHIN_PINS["controls"]
        and request["cached_features_sha256"] == prior.PINS["features"]
        and request["baseline_sha256"] == prior.WITHIN_PINS["predictions"],
        "frozen source pin differs",
    )
    prepared, packets, _, features = prior.load_prepared(request["prepared_path"], request["prepared_sha256"])
    cached = core.inputs._asset(request["cached_features_path"], request["cached_features_sha256"])
    signed_request = member.pinned_json(prepared["request_path"], prepared["request_sha256"])
    original = member.pinned_json(signed_request["source_request_path"], signed_request["source_request_sha256"])
    core.validate_request(original)
    prior.validate_cached_source(original["source"], cached["source"])
    require(
        Path(request["dataset_root"]).resolve() == Path(cached["source"]["candidate_root"]).resolve()
        and features["release_binding"] == cached["source"]["release_binding"],
        "release locator differs",
    )
    controls = core.inputs._asset(request["controls_path"], request["controls_sha256"])
    require(
        controls["status"] == "COMPLETED"
        and controls["completed_fits"] == 10
        and controls["contract"] == prior.CONTRACT,
        "original controls are not sealed",
    )
    for vintage in prior.VINTAGES:
        model = controls["models"][vintage["id"]]["C"]
        prior.validate_model(model)
        require(model["vintage"] == vintage and model["arm"] == "C", "control quarter differs")
    validate_output_location(Path(request["fit_ledger_root"]), dataset_root=Path(request["dataset_root"]))
    return request, prepared, packets, features, cached, signed_request, original, controls


def aggregate_ratio(members_cny, balances, *, unit):
    """Observed financing subset; absence is not eligibility or zero balance."""
    require(unit == "CNY", "explicit CNY capitalization required")
    require(len({s for s, _ in members_cny}) == len(members_cny), "duplicate as-of member")
    require(all(math.isfinite(v) and v > 0 for _, v in members_cny), "invalid member capitalization")
    observed = [(s, cap, balances[s]) for s, cap in members_cny if s in balances]
    require(all(math.isfinite(b) and b >= 0 for _, _, b in observed), "invalid published financing balance")
    total = math.fsum(cap for _, cap in members_cny)
    capital = math.fsum(cap for _, cap, _ in observed)
    return dict(
        value=math.fsum(b for _, _, b in observed) / capital if observed else None,
        reason=None if observed else "NO_OBSERVED_MARGIN_MEMBER",
        members=len(members_cny),
        observed=len(observed),
        observed_capital_fraction=capital / total if total else None,
        member_sha256=canonical_sha256(members_cny),
        observed_sha256=canonical_sha256(observed),
    )


def margin_frame(binding, source_dates):
    """Read only exact source sessions, retaining raw zeros and sparse absence."""
    with pd.HDFStore(binding.margin_path, mode="r") as store:
        require(store.get_storer("data").is_table, "financing H5 must be the pinned native table")
        frame = store.select(
            "data",
            where=[f"datetime >= Timestamp('{source_dates[0]}')", f"datetime <= Timestamp('{source_dates[-1]}')"],
            columns=["md_rzye"],
        )
    require(frame.index.names == ["datetime", "instrument"] and frame.index.is_unique, "financing index differs")
    days = frame.index.get_level_values("datetime")
    source_codes = frame.index.get_level_values("instrument").to_numpy(copy=True)
    codes = source_codes.copy()
    require(not days.hasnans and days.equals(days.normalize()), "invalid financing date")
    require(set(days.strftime("%Y-%m-%d")) <= set(source_dates), "financing fact outside source calendar")
    known = {
        s
        for r in binding.identity.rows
        if r.source_dataset == MARGIN_SOURCE
        for s in (r.source_ts_code, r.canonical_ts_code)
    }
    for symbol in sorted(known):
        # Locate native rows only: a converted code must never be resolved twice.
        positions = np.flatnonzero(source_codes == symbol)
        if len(positions):
            codes[positions] = [binding.canonical_code(symbol, days[i].date()) for i in positions]
    frame.index = pd.MultiIndex.from_arrays([days, codes], names=frame.index.names)
    require(frame.index.is_unique, "duplicate canonical financing fact")
    values = frame["md_rzye"].to_numpy()
    require(np.isfinite(values).all() and (values >= 0).all(), "invalid observed financing balance")
    binding.unchanged()
    return frame.sort_index()


def financing_features(source, binding, decisions):
    calendar, positions = source.calendar, source.positions
    require(
        decisions == sorted(set(decisions)) and all(positions[d] >= 2 for d in decisions), "financing context absent"
    )
    source_dates = [calendar[positions[d] - 2] for d in decisions]
    frame = margin_frame(binding, source_dates)
    groups = {str(day.date()): g.droplevel("datetime")["md_rzye"] for day, g in frame.groupby(level="datetime")}
    rows = {}
    for day, u in zip(decisions, source_dates, strict=True):
        require(u in groups, f"no financing publication on required source session: {u}")
        balances = groups[u].to_dict()
        members = source.members(day)
        rows[day] = {
            code: aggregate_ratio([(s, cap * 10000) for s, cap in members[code]], balances, unit="CNY")
            for code in source.catalog
        }
    source.unchanged()
    binding.unchanged()
    return receipt(
        dict(
            schema_version=VERSION + "_financing",
            contract=CONTRACT,
            calendar=calendar,
            catalog=source.catalog,
            decisions=decisions,
            as_of={d: calendar[positions[d] - 1] for d in decisions},
            source_dates=dict(zip(decisions, source_dates, strict=True)),
            rows=rows,
            binding=binding.evidence(),
            original_member_source=source.identity(),
            fits=0,
            evaluation_outcomes_read=False,
            database_access=False,
            dataset_write=False,
        )
    )


def design(rows, values, arm):
    require(arm in {"M", "C20"} and len(rows) == len(values) > 0, "invalid matched training input")
    z, y, weights, scaler = loss._ridge_inputs([r[:3] + [r[4]] for r in rows])
    require(np.all((y >= 0) & (y < 1)), "invalid C training target")
    if arm == "M":
        v = np.asarray(values, dtype=np.float64)
        require(np.isfinite(v).all() and np.all(v >= 0), "invalid financing feature")
        active = bool(np.any(v != v[0]))
        mean, scale = (float(v.mean()), float(v.std(ddof=0))) if active else (float(v[0]), 0.0)
        require(math.isfinite(mean) and math.isfinite(scale) and (not active or scale > 0), "invalid financing scaler")
        if active:
            z = np.column_stack([z, (v - mean) / scale])
        scaler = {
            **scaler,
            "mean": [*scaler["mean"], mean],
            "scale": [*scaler["scale"], scale],
            "active": [*scaler["active"], active],
        }
    return z, y, weights, scaler


def validate_parameters(parameters, arm):
    n = 21 if arm == "M" else 20
    require(
        arm in {"M", "C20"} and set(parameters) == {"mean", "scale", "active", "coef", "intercept"},
        "parameter schema differs",
    )
    mean, scale, coef = (np.asarray(parameters[k], dtype=float) for k in ("mean", "scale", "coef"))
    active = np.asarray(parameters["active"], dtype=bool)
    require(
        all(type(x) is bool for x in parameters["active"])
        and mean.shape == scale.shape == active.shape == (n,)
        and active.any()
        and coef.shape == (int(active.sum()),)
        and np.all(scale[active] > 0)
        and np.all(scale[~active] == 0)
        and all(np.isfinite(x).all() for x in (mean, scale, coef))
        and type(parameters["intercept"]) in (int, float)
        and math.isfinite(parameters["intercept"]),
        "invalid parameter arrays",
    )
    return mean, scale, active, coef, parameters["intercept"]


def can_reuse(rows, indices, original, control, values):
    if indices != list(range(len(original["rows"]))):
        return False
    _, _, _, scaler = design(rows, values, "C20")
    if original["vintage"] == prior.release.Q4:
        prior.validate_q4_control(original, control)
    else:
        require(
            control["packet_sha256"] == original["receipt_sha256"]
            and control["mask_sha256"] == original["mask_sha256"]
            and control["target_facts_sha256"] == original["target_facts_sha256"],
            "old control does not bind the original targets/mask",
        )
    return (
        all(control["parameters"][k] == scaler[k] for k in scaler)
        and control["fit_rows"] == len(rows)
        and control["fit_dates"] == len({r[0] for r in rows})
    )


def prepare(request_path, request_pin, output, head):
    request, old, packets, features, cached, signed, original, controls = load_request(request_path, request_pin)
    binding = load_margin_source_binding(
        dataset_root=Path(request["dataset_root"]),
        expected_dataset_manifest_sha256=CONTRACT["manifest_sha256"],
        package_root=Path(request["package_root"]),
        expected_package_manifest_sha256=PACKAGE_PIN,
    )
    decisions = sorted(
        set(features["prediction_dates"])
        | {d for v in prior.VINTAGES for d in prior.schedule(features["calendar"], v)["train"]}
    )
    combined = {**cached, "as_of": {d: features["calendar"][features["calendar"].index(d) - 1] for d in decisions}}
    source = member.FrozenMemberSource(
        combined, signed, provider_absence_manifest=Path(original["source"]["provider_absence_manifest"])
    )
    finance = financing_features(source, binding, decisions)
    output = validate_output_location(Path(output), dataset_root=Path(request["dataset_root"]))
    require(not output.exists(), "financing preflight output already exists")
    finance_path = output / "financing.json"
    write_once(finance_path, finance)
    locators, counts, attempts = {}, {}, []
    for vintage in prior.VINTAGES:
        q, packet = vintage["id"], packets[vintage["id"]]
        indices = [i for i, r in enumerate(packet["rows"]) if finance["rows"][r[0]][r[1]]["value"] is not None]
        rows = [packet["rows"][i] for i in indices]
        values = [finance["rows"][r[0]][r[1]]["value"] for r in rows]
        _, _, _, s20 = design(rows, values, "C20")
        _, _, _, s21 = design(rows, values, "M")
        require(all(s20[k] == s21[k][:20] for k in s20), "matched scalers differ")
        control = controls["models"][q]["C"]
        reuse = can_reuse(rows, indices, packet, control, values)
        wrapper = receipt(
            dict(
                schema_version=VERSION + "_train",
                contract=CONTRACT,
                vintage=vintage,
                original_packet=old["packets"][q],
                indices=indices,
                margin_values=values,
                mask_sha256=canonical_sha256([r[:2] for r in rows]),
                reused_control=control if reuse else None,
                finance_sha256=finance["receipt_sha256"],
                source_commit=head,
            )
        )
        path = output / (q + ".json")
        write_once(path, wrapper)
        locators[q] = dict(path=str(path), sha256=wrapper["receipt_sha256"])
        counts[q] = dict(
            original_rows=len(packet["rows"]),
            matched_rows=len(rows),
            financing_unavailable=len(packet["rows"]) - len(rows),
            matched_dates=len({r[0] for r in rows}),
            reused_control=reuse,
            original_excluded=packet["excluded"],
        )
        attempts.extend([dict(process=p, quarter=q, arm=a) for p in (1, 2) for a in (["M"] if reuse else ["C20", "M"])])
    require(6 <= len(attempts) <= 12, "unapproved attempt budget")
    result = receipt(
        dict(
            schema_version=VERSION + "_preflight",
            contract=CONTRACT,
            status="PASS",
            source_commit=head,
            request_path=str(request_path),
            request_sha256=request_pin,
            original_prepared_sha256=old["receipt_sha256"],
            prediction_features_path=old["prediction_features_path"],
            prediction_features_sha256=features["receipt_sha256"],
            finance_path=str(finance_path),
            finance_sha256=finance["receipt_sha256"],
            packets=locators,
            training=counts,
            attempts=attempts,
            fit_ledger_root=request["fit_ledger_root"],
            fits=0,
            evaluation_outcomes_read=False,
            prediction_rows=len(features["prediction_dates"]) * len(features["catalog"]),
            missing_financing=Counter(
                row["reason"] for day in finance["rows"].values() for row in day.values() if row["reason"]
            ),
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
        "training wrapper differs",
    )
    packet = core.inputs._asset(wrapper["original_packet"]["path"], wrapper["original_packet"]["sha256"])
    original = prior.validate_packet(packet)
    require(packet["vintage"] == wrapper["vintage"], "quarter wrapper swapped")
    indices = wrapper["indices"]
    require(
        indices == sorted(set(indices)) and all(type(i) is int and 0 <= i < len(original) for i in indices),
        "invalid matched indices",
    )
    rows = [original[i] for i in indices]
    require(canonical_sha256([r[:2] for r in rows]) == wrapper["mask_sha256"], "matched mask differs")
    design(rows, wrapper["margin_values"], "M")
    if wrapper["reused_control"] is not None:
        prior.validate_model(wrapper["reused_control"])
        require(
            can_reuse(rows, indices, packet, wrapper["reused_control"], wrapper["margin_values"]),
            "reused control does not have the exact matched carrier",
        )
    return rows


def fit_packet(wrapper, output, ledger, head, numeric):
    from sklearn.linear_model import Ridge

    prior.validate_numeric(numeric)
    rows = load_packet(wrapper)
    require(wrapper["source_commit"] == head, "training source differs")
    models, fits = {}, 0
    for arm in ("C20", "M"):
        z, y, weights, scaler = design(rows, wrapper["margin_values"], arm)
        reused = wrapper["reused_control"] if arm == "C20" else None
        if reused is not None:
            parameters = reused["parameters"]
        else:
            write_once(
                Path(ledger) / (wrapper["vintage"]["id"] + "." + arm + ".json"),
                receipt(dict(packet_sha256=wrapper["receipt_sha256"], arm=arm, fit_kernel_attempts=1)),
            )
            fits += 1
            fitted = Ridge(**loss.PARAMS).fit(z, y, sample_weight=weights)
            parameters = dict(**scaler, coef=fitted.coef_.tolist(), intercept=float(fitted.intercept_))
        validate_parameters(parameters, arm)
        models[arm] = receipt(
            dict(
                schema_version=VERSION + "_model",
                contract=CONTRACT,
                arm=arm,
                vintage=wrapper["vintage"],
                parameters=parameters,
                model_sha256=canonical_sha256(parameters),
                training_packet_sha256=wrapper["receipt_sha256"],
                mask_sha256=wrapper["mask_sha256"],
                finance_sha256=wrapper["finance_sha256"],
                source_commit=head,
                reused_model_sha256=reused["model_sha256"] if reused else None,
                fit_rows=len(rows),
                fit_dates=len({r[0] for r in rows}),
                sample_weight_sum=float(weights.sum()),
                training_target_mean=float(np.average(y, weights=weights)),
            )
        )
    require(
        all(models["C20"]["parameters"][k] == models["M"]["parameters"][k][:20] for k in ("mean", "scale", "active")),
        "matched fitted scaler differs",
    )
    result = receipt(
        dict(
            schema_version=VERSION + "_fit",
            contract=CONTRACT,
            models=models,
            packet_sha256=wrapper["receipt_sha256"],
            source_commit=head,
            fits=fits,
            numeric_environment=numeric,
            evaluation_outcomes_read=False,
        )
    )
    write_once(Path(output) / "models.json", result)
    return result


def validate_model(model, arm, vintage, head):
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
    validate_parameters(model["parameters"], arm)


def close_fits(processes, wrappers, preflight, head):
    require(
        len(processes) == 2 and canonical_json_bytes(processes[0]) == canonical_json_bytes(processes[1]),
        "fresh process models differ",
    )
    completed = 0
    for group in processes:
        require(set(group) == set(wrappers), "incomplete quarter model set")
        for vintage in prior.VINTAGES:
            wrapper, run = wrappers[vintage["id"]], group[vintage["id"]]
            core.verify_receipt(run)
            prior.validate_numeric(run["numeric_environment"])
            require(
                run["contract"] == CONTRACT
                and run["source_commit"] == head
                and run["packet_sha256"] == wrapper["receipt_sha256"]
                and run["fits"] == (1 if wrapper["reused_control"] else 2)
                and run["evaluation_outcomes_read"] is False
                and set(run["models"]) == {"C20", "M"},
                "fit result identity/budget differs",
            )
            for arm, model in run["models"].items():
                validate_model(model, arm, vintage, head)
                require(
                    model["training_packet_sha256"] == wrapper["receipt_sha256"]
                    and model["mask_sha256"] == wrapper["mask_sha256"]
                    and model["finance_sha256"] == preflight["finance_sha256"],
                    "model carrier differs",
                )
            completed += run["fits"]
    require(completed == len(preflight["attempts"]) <= 12, "formal attempts differ")
    return receipt(
        dict(
            schema_version=VERSION + "_fit_acceptance",
            contract=CONTRACT,
            status="PASS",
            source_commit=head,
            preflight_sha256=preflight["receipt_sha256"],
            completed_fits=completed,
            models={q: r["models"] for q, r in processes[0].items()},
            fresh_processes=2,
            evaluation_outcomes_read=False,
            automatic_promotion=False,
        )
    )


def raw_prediction(parameters, vector, arm):
    mean, scale, active, coef, intercept = validate_parameters(parameters, arm)
    x = np.asarray(vector, dtype=float)
    require(x.shape == mean.shape and np.isfinite(x).all(), "invalid prediction feature")
    value = float(((x[active] - mean[active]) / scale[active]) @ coef + intercept)
    require(math.isfinite(value), "nonfinite prediction")
    return value


def validate_acceptance(acceptance, head):
    core.verify_receipt(acceptance)
    require(
        acceptance["schema_version"] == VERSION + "_fit_acceptance"
        and acceptance["contract"] == CONTRACT
        and acceptance["status"] == "PASS"
        and acceptance["source_commit"] == head
        and acceptance["fresh_processes"] == 2
        and set(acceptance["models"]) == {v["id"] for v in prior.VINTAGES},
        "fit acceptance differs",
    )
    expected = 0
    for vintage in prior.VINTAGES:
        models = acceptance["models"][vintage["id"]]
        require(set(models) == {"M", "C20"}, "incomplete matched models")
        for arm, model in models.items():
            validate_model(model, arm, vintage, head)
        require(
            all(
                models["C20"]["parameters"][k] == models["M"]["parameters"][k][:20] for k in ("mean", "scale", "active")
            )
            and models["M"]["training_packet_sha256"] == models["C20"]["training_packet_sha256"]
            and models["M"]["mask_sha256"] == models["C20"]["mask_sha256"]
            and models["M"]["reused_model_sha256"] is None,
            "accepted matched scalers/masks differ",
        )
        expected += 2 * (1 if models["C20"]["reused_model_sha256"] else 2)
    require(acceptance["completed_fits"] == expected <= 12, "accepted fit count differs")


def predict(acceptance, features, finance, head):
    validate_acceptance(acceptance, head)
    core.verify_receipt(finance)
    prior.validate_prediction_features(features)
    require(
        acceptance["schema_version"] == VERSION + "_fit_acceptance"
        and acceptance["contract"] == CONTRACT
        and acceptance["status"] == "PASS"
        and acceptance["source_commit"] == head
        and acceptance["fresh_processes"] == 2
        and finance["contract"] == CONTRACT
        and finance["catalog"] == features["catalog"]
        and finance["calendar"] == features["calendar"],
        "prediction input differs",
    )
    rows = []
    for day in features["prediction_dates"]:
        vintage = next(v for v in prior.VINTAGES if v["effective_start"] <= day <= v["effective_end"])
        for code in features["catalog"]:
            old, extra = features["rows"][day][code], finance["rows"][day][code]
            require(
                finance["as_of"][day] == old["as_of_date"] == features["as_of"][day]
                and finance["source_dates"][day] == features["calendar"][features["calendar"].index(day) - 2],
                "source/as-of clock differs",
            )
            row = dict(
                trade_date=day,
                as_of_date=old["as_of_date"],
                financing_source_date=finance["source_dates"][day],
                sector_code=code,
                M=None,
                C20=None,
                raw_M=None,
                raw_C20=None,
                reasons={},
            )
            for arm in ("C20", "M"):
                model = acceptance["models"][vintage["id"]][arm]
                require(model["finance_sha256"] == finance["receipt_sha256"], "financing carrier differs")
                reason = old["reason_code"] if old["features"] is None else extra["reason"] if arm == "M" else None
                if reason is not None:
                    row["reasons"][arm] = reason
                    continue
                vector = old["features"] + ([extra["value"]] if arm == "M" else [])
                row["raw_" + arm] = raw_prediction(model["parameters"], vector, arm)
                row[arm] = max(0.0, row["raw_" + arm])
            rows.append(row)
    groups = freeze_groups(dict(rows=rows), features["prediction_dates"], features["catalog"])
    return receipt(
        dict(
            schema_version=VERSION + "_predictions",
            contract=CONTRACT,
            source_commit=head,
            fit_acceptance_sha256=acceptance["receipt_sha256"],
            features_sha256=features["receipt_sha256"],
            finance_sha256=finance["receipt_sha256"],
            rows=rows,
            ex_ante_groups=groups,
            ex_ante_groups_sha256=canonical_sha256(groups),
            fits=0,
            policy_applied=False,
            evaluation_outcomes_read=False,
            historical_as_known=False,
            untouched_holdout=False,
        )
    )


def risk_weights(scores):
    require(scores and all(math.isfinite(v) for v in scores.values()), "invalid ex-ante risk scores")
    if len(scores) < 2 or len(set(scores.values())) < 2:
        return None
    left, weights = math.ceil(0.20 * len(scores)), dict.fromkeys(scores, 0.0)
    for score in sorted(set(scores.values()), reverse=True):
        keys = [k for k, v in scores.items() if v == score]
        weight = min(left, len(keys)) / len(keys)
        for key in keys:
            weights[key] = weight
        if left <= len(keys):
            break
        left -= len(keys)
    return weights


def freeze_groups(prediction, days, catalog):
    require(
        [(r["trade_date"], r["sector_code"]) for r in prediction["rows"]] == [(d, c) for d in days for c in catalog],
        "full prediction grid differs",
    )
    groups = {}
    for day in days:
        common = [
            r for r in prediction["rows"] if r["trade_date"] == day and r["M"] is not None and r["C20"] is not None
        ]
        groups[day] = {
            arm: risk_weights({r["sector_code"]: r[arm] for r in common}) if common else None for arm in ("M", "C20")
        }
    return groups


def paired_value(weights, labels):
    if all(label["status"] == "OUTCOME_NOT_MATURE" for label in labels.values()):
        return dict(J=None, avoided_loss=None, missed_upside=None, reason="OUTCOME_NOT_MATURE")
    if any(weights[a] is None for a in ("M", "C20")):
        return dict(J=None, avoided_loss=None, missed_upside=None, reason="NO_EX_ANTE_ORDERING")
    require(set(weights["M"]) == set(weights["C20"]), "paired ex-ante sets differ")
    terms = []
    for code, h_m in weights["M"].items():
        delta = h_m - weights["C20"][code]
        if delta == 0:
            continue  # Cancels in the difference; never an imputed asset return.
        label = labels[code]
        if label["status"] != "AVAILABLE":
            return dict(J=None, avoided_loss=None, missed_upside=None, reason=label["reason"])
        prior.signed_label(label)
        terms.append((delta * label["C"], delta * label["upside"]))
    avoided, missed = (math.fsum(t[i] for t in terms) / 131 for i in (0, 1))
    return dict(J=avoided - missed, avoided_loss=avoided, missed_upside=missed, reason=None)


def adjusted_targets(source, days, primary):
    """Declared price sensitivity only; same members/weights and frozen groups."""
    mature = set(member.target_dates(source.calendar, days, prior.CONTRACT["prediction_end"])["mature"])
    output = {}
    for day in days:
        if day not in mature:
            output[day] = primary[day]
            continue
        end = source.calendar[source.positions[day] + 10]
        output[day] = {}
        for code, members in source.members(day).items():
            facts = []
            for symbol, weight in members:
                first, a = source.price(symbol, day, adjusted=True)
                last, b = source.price(symbol, end, adjusted=True)
                facts.append(
                    dict(
                        symbol=symbol,
                        weight=weight,
                        value=last / first - 1 if first is not None and last is not None else None,
                        reason=a or b,
                    )
                )
            output[day][code] = member.pair_target(facts)
    return output


def evaluate(preflight, acceptance, prediction, seal, head):
    core.verify_receipt(preflight)
    validate_acceptance(acceptance, head)
    core.verify_receipt(seal)
    core.verify_receipt(prediction)
    require(
        seal["schema_version"] == VERSION + "_prediction_acceptance"
        and seal["status"] == "PASS"
        and seal["fresh_processes"] == 2
        and seal["prediction_sha256"] == prediction["receipt_sha256"]
        and seal["fit_acceptance_sha256"] == acceptance["receipt_sha256"]
        and prediction["fit_acceptance_sha256"] == acceptance["receipt_sha256"]
        and acceptance["preflight_sha256"] == preflight["receipt_sha256"]
        and seal["source_commit"] == acceptance["source_commit"] == prediction["source_commit"] == head,
        "all models/predictions must be sealed before reading evaluation facts",
    )
    request, _, _, features, cached, signed, original, _ = load_request(
        preflight["request_path"], preflight["request_sha256"]
    )
    days, catalog, calendar = features["prediction_dates"], features["catalog"], features["calendar"]
    groups = freeze_groups(prediction, days, catalog)  # Must precede any outcome read.
    groups_pin = canonical_sha256(groups)
    require(
        groups == prediction["ex_ante_groups"] and groups_pin == prediction["ex_ante_groups_sha256"],
        "ex-ante groups were not sealed with predictions",
    )
    source = member.FrozenMemberSource(
        cached, signed, provider_absence_manifest=Path(original["source"]["provider_absence_manifest"])
    )
    labels = source.targets(days, prior.CONTRACT["prediction_end"])
    rows = {(r["trade_date"], r["sector_code"]): r for r in prediction["rows"]}
    baseline = core.inputs._asset(request["baseline_path"], request["baseline_sha256"])
    baseline_c = {(r["trade_date"], r["sector_code"]): r["expected_member_loss"] for r in baseline["predictions"]["C"]}
    daily, na, errors = {}, Counter(), {a: [] for a in ("M", "C20")}
    for day in days:
        value = paired_value(groups[day], labels[day])
        diagnostic = {}
        for arm in ("M", "C20", "original_C"):
            pairs = [
                (baseline_c[(day, c)] if arm == "original_C" else rows[(day, c)][arm], labels[day][c]["C"])
                for c in catalog
                if labels[day][c]["status"] == "AVAILABLE"
                and rows[(day, c)]["M"] is not None
                and rows[(day, c)]["C20"] is not None
            ]
            require(all(x is not None for x, _ in pairs), "historical control unexpectedly unavailable")
            diagnostic[arm] = prior.rank_ic([x for x, _ in pairs], [y for _, y in pairs])
            if arm != "original_C":
                errors[arm].extend((x - y) for x, y in pairs)
        diagnostic["delta"] = (
            diagnostic["M"] - diagnostic["C20"] if all(diagnostic[a] is not None for a in ("M", "C20")) else None
        )
        daily[day] = dict(**value, rank_ic=diagnostic, common_ex_ante=len(groups[day]["M"] or {}))
        na.update(label["reason"] or label["status"] for label in labels[day].values())

    def summary(selected):
        return dict(
            value={
                k: prior.hac({d: daily[d][k] for d in selected}, calendar)
                for k in ("J", "avoided_loss", "missed_upside")
            },
            rank_ic={
                a: prior.hac({d: daily[d]["rank_ic"][a] for d in selected}, calendar)
                for a in ("M", "C20", "delta", "original_C")
            },
            dates=len(selected),
            undefined=Counter(daily[d]["reason"] for d in selected if daily[d]["reason"]),
        )

    cohort = member.pinned_json(request["cohort_path"], prior.PINS["cohort_file"])
    core.verify_receipt(cohort, prior.PINS["cohort"])
    unique = {}
    for r in cohort["rows"]:
        k, fact = (r["date"], r["stock"]), (r["code"], r["stock_return"], r["reason"])
        require(k not in unique or unique[k] == fact, "cross-seed cohort facts differ")
        unique[k] = fact

    def cohort_metrics(selected):
        values, missing = {}, Counter()
        for day in sorted({k[0] for k in selected}):
            observed = []
            for (d, _), (code, outcome, reason) in selected.items():
                if d != day:
                    continue
                require((d, code) in rows, "cohort escapes prediction grid")
                if outcome is None:
                    require(isinstance(reason, str) and reason, "untyped cohort absence")
                    missing[reason] += 1
                    continue
                item = rows[(d, code)]
                if item["M"] is None or item["C20"] is None:
                    missing["PREDICTION_UNAVAILABLE"] += 1
                    continue
                observed.append((item["M"], item["C20"], max(0.0, -outcome)))
            values[day] = {
                arm: prior.rank_ic([r[i] for r in observed], [r[2] for r in observed])
                for i, arm in enumerate(("M", "C20"))
            }
            values[day]["delta"] = (
                values[day]["M"] - values[day]["C20"] if all(values[day][a] is not None for a in ("M", "C20")) else None
            )
        return dict(
            stats={a: prior.hac({d: v[a] for d, v in values.items()}, calendar) for a in ("M", "C20", "delta")},
            missing=missing,
            fixed_10d_reference_not_trade_or_account_return=True,
        )

    full = summary(days)
    adjusted = adjusted_targets(source, days, labels)
    sensitivity = {d: paired_value(groups[d], adjusted[d]) for d in days}
    mean = full["value"]["J"]["mean"]
    status = (
        "INSUFFICIENT_EVALUABLE_EVIDENCE"
        if mean is None
        else "STOP_NO_INCREMENTAL_VALUE_REFERENCE"
        if mean <= 0
        else "RESEARCH_LEAD_REQUIRES_VALUE_REVIEW"
    )
    source.unchanged()
    return receipt(
        dict(
            schema_version=VERSION + "_evaluation",
            contract=CONTRACT,
            status=status,
            source_commit=head,
            preflight_sha256=preflight["receipt_sha256"],
            fit_acceptance_sha256=acceptance["receipt_sha256"],
            prediction_sha256=prediction["receipt_sha256"],
            ex_ante_groups_sha256=groups_pin,
            full=full,
            quarters={
                v["id"]: summary([d for d in days if v["effective_start"] <= d <= v["effective_end"]])
                for v in prior.VINTAGES
            },
            errors={
                a: dict(
                    mse=float(np.mean(np.square(v))) if v else None,
                    mae=float(np.mean(np.abs(v))) if v else None,
                    pooled_diagnostic_not_primary=True,
                )
                for a, v in errors.items()
            },
            daily=daily,
            prediction_reasons=Counter(reason for r in prediction["rows"] for reason in r["reasons"].values()),
            training=preflight["training"],
            adjusted_price_sensitivity=dict(
                primary_unchanged=True,
                same_frozen_groups=True,
                outcome_basis="Qlib_adjusted_decision_close_to_t_plus_10_close",
                value={
                    k: prior.hac({d: sensitivity[d][k] for d in days}, calendar)
                    for k in ("J", "avoided_loss", "missed_upside")
                },
            ),
            label_reasons=na,
            evaluation_source_identity=source.identity(),
            cohort=cohort_metrics(unique),
            cohort_rows=len(cohort["rows"]),
            cohort_unique=len(unique),
            seed_diagnostics={
                str(s): dict(
                    **cohort_metrics(
                        {
                            k: v
                            for k, v in unique.items()
                            if k in {(r["date"], r["stock"]) for r in cohort["rows"] if r["seed"] == s}
                        }
                    ),
                    independent_sample=False,
                )
                for s in sorted({r["seed"] for r in cohort["rows"]})
            },
            fits=0,
            database_access=False,
            dataset_write=False,
            policy_applied=False,
            automatic_promotion=False,
            limitations=[
                "Retrospective classification; financing historical publication/revision UNKNOWN/NON_VINTAGE.",
                "132 maximum mature days; Q4 only seven. Overlapping gross fixed-budget reference, not NAV/net return.",
                "New to HMM20D does not prove incremental information relative to QE AF.",
            ],
        )
    )
