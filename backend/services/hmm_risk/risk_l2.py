"""One approved pooled L2 drawdown classifier, not an HMM structure gate."""

from __future__ import annotations

from collections import Counter
from datetime import date
import math
from pathlib import Path
import warnings
from typing import Any, Mapping, Sequence

import numpy as np
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LogisticRegression

from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_json_bytes, canonical_sha256
from backend.services.hmm_risk.formal_state_effect import CALENDAR_SHA, verify_receipt
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt

VERSION = "hmm_risk_l2_absolute_drawdown_logistic_v1"
BASIS = "HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT"
TRAIN_START, TRAIN_END = "2022-01-04", "2024-06-28"
DEV_START, DEV_END = "2024-07-01", "2026-03-31"
PARAMS = dict(
    penalty="l2",
    C=1.0,
    solver="lbfgs",
    fit_intercept=True,
    tol=1e-8,
    max_iter=1000,
    class_weight=None,
    random_state=42,
    warm_start=False,
)
CONTRACT = {
    "version": VERSION,
    "feature_names": list(ALL_CORE_FEATURES),
    "parameters": PARAMS,
    "train_window": [TRAIN_START, TRAIN_END],
    "development_window": [DEV_START, DEV_END],
    "horizon": 10,
    "drawdown_boundary": -0.08,
    "warning_threshold": 0.20,
    "binding_precision_lift": 0.05,
    "binding_recall": 0.25,
    "coverage_minimum": 0.90,
    "hac_lag": 9,
    "hac_is_gate": False,
    "validation_basis": BASIS,
}
SOURCE_REQUEST_FILE_SHA = "2d61c54eda375ba41a6bddbeb6e555b36a61fb6b53649309c312621c775ffa70"
PIT_BUNDLE_SHA = "051e2af357703734080ff3ea5b4311926905aa7cbd1f31d926ef5b8575261313"


def fail(message: str, reason: str = "input_invalid") -> FormalStateError:
    return FormalStateError("hmm_risk_l2_risk_" + reason, message)


def prepare_file_inputs(source_request: Path, *, work_parent: Path, source_commit: str) -> tuple[dict, dict]:
    """Reuse the current frozen C-010/A5 source once, never an active/latest lookup.

    Features and future outcome facts are separate assets. The predictor only
    receives the former; outcome access follows sealed prediction readback.
    """
    import hashlib
    import pandas as pd
    from backend.services.hmm_risk import rotation_l1_input_bundle as reader
    from backend.services.hmm_risk.formal_state_executor import load_effect_request
    from backend.services.hmm_risk.formal_state_input import _l2_stock_facts
    from backend.services.hmm_risk.stock_fact_observation import build_c010_feature_domain_panel

    if hashlib.sha256(source_request.read_bytes()).hexdigest() != SOURCE_REQUEST_FILE_SHA:
        raise fail("original C-010/A5 frozen request differs", "identity_mismatch")
    request = load_effect_request(source_request)
    frozen, source = request["frozen"], {**request["source"], "work_parent": str(work_parent)}
    if frozen["industry_authority"]["identity"]["bundle_hash"] != PIT_BUNDLE_SHA:
        raise fail("full-v3 PIT authority differs", "identity_mismatch")
    calendar_path = Path(source["candidate_root"]) / "components/daily_bin_candidate/calendars/day.txt"
    if reader._sha256_file(calendar_path) != CALENDAR_SHA:
        raise fail("frozen calendar differs", "identity_mismatch")
    full = [d.isoformat() for d in reader._load_qlib_calendar(calendar_path) if d.isoformat() <= DEV_END]
    plan = schedule(full)
    first = full[plan["positions"][TRAIN_START] - 260]
    assets, window, aggregates, identity = _l2_stock_facts(
        frozen,
        source,
        start=date.fromisoformat(first),
        end=date.fromisoformat(DEV_END),
    )
    panel, definition, cross = build_c010_feature_domain_panel(
        aggregates,
        trading_dates=window,
        csi300_returns={d: assets["benchmark"][d] for d in window},
        expected_sector_count=131,
        direct_sector_level="L2",
        canonical_sector_codes=frozen["catalog"],
    )
    if definition != frozen["feature_definition"]:
        raise fail("original C-010 20D feature definition differs", "identity_mismatch")
    rows = {d: {} for d in plan["train"] + plan["dev"]}
    for code in frozen["catalog"]:
        history = panel.xs(code, level="l1_code")
        for day in rows:
            as_of = plan["as_of"][day]
            values = history.reindex(pd.to_datetime([as_of]))[list(ALL_CORE_FEATURES)].to_numpy(dtype=np.float64)[0]
            finite = np.isfinite(values).all()
            rows[day][code] = {
                "as_of_date": as_of,
                "features": values.tolist() if finite else None,
                "reason_code": None
                if finite
                else identity["domain_reasons"].get((as_of, code), "hmm_risk_c010_observation_unavailable"),
            }
    returns = {d.isoformat(): {c: None for c in frozen["catalog"]} for d in window}
    for aggregate in aggregates:
        returns[aggregate.trade_date.isoformat()][aggregate.l1_code] = float(aggregate.l1_return)
    training_labels = drawdown_outcomes(full, plan["train"], frozen["catalog"], returns, TRAIN_END)
    input_identity = {k: v for k, v in identity.items() if k not in {"structural_membership", "domain_reasons"}}
    input_identity.update(
        source_request_file_sha256=SOURCE_REQUEST_FILE_SHA,
        source_commit=source_commit,
        feature_definition_sha256=canonical_sha256(definition),
        cross_section_lineage_sha256=canonical_sha256(cross),
        eligibility_sha256=frozen["eligibility_receipt_sha256"],
        industry_authority_sha256=canonical_sha256(frozen["industry_authority"]),
        calendar_file_sha256=CALENDAR_SHA,
        pit_bundle_sha256=PIT_BUNDLE_SHA,
    )
    features = receipt(
        {
            "schema_version": VERSION + "_features",
            "contract": CONTRACT,
            "calendar": full,
            "catalog": frozen["catalog"],
            "feature_names": list(ALL_CORE_FEATURES),
            "rows": rows,
            "train_labels": training_labels,
            "input_identity": input_identity,
        }
    )
    outcome_inputs = receipt(
        {
            "schema_version": VERSION + "_outcome_facts",
            "feature_sha256": features["receipt_sha256"],
            "calendar": full,
            "catalog": frozen["catalog"],
            "returns": {d: v for d, v in returns.items() if DEV_START < d <= DEV_END},
            "source_identity": input_identity,
            "tail_accessed": False,
        }
    )
    return features, outcome_inputs


def schedule(calendar: Sequence[str]) -> dict[str, Any]:
    if list(calendar) != sorted(set(calendar)):
        raise fail("calendar must be unique and ordered")
    days = [date.fromisoformat(d) for d in calendar]
    positions = {d: i for i, d in enumerate(calendar)}
    train = [d for d in calendar if TRAIN_START <= d <= TRAIN_END]
    dev = [d for d in calendar if DEV_START <= d <= DEV_END]
    fit = [d for d in train if positions[d] + 10 <= positions.get(TRAIN_END, -1)]
    mature = [d for d in dev if positions[d] + 10 <= positions.get(DEV_END, -1)]
    if (len(train), len(fit), len(dev), len(mature)) != (601, 591, 424, 414):
        raise fail("approved 601/591/424/414 calendar differs")
    if fit[-1] != "2024-06-14" or mature[-1] != "2026-03-17" or not days:
        raise fail("approved maturity boundary differs")
    if positions[TRAIN_START] < 260 or positions[DEV_START] == 0:
        raise fail("250-day plus price warmup unavailable")
    return {
        "train": train,
        "fit": fit,
        "dev": dev,
        "mature": mature,
        "as_of": {d: calendar[positions[d] - 1] for d in train + dev},
        "positions": positions,
    }


def drawdown_outcomes(
    calendar: Sequence[str],
    decisions: Sequence[str],
    codes: Sequence[str],
    returns: Mapping[str, Mapping[str, float | None]],
    end: str,
) -> dict[str, Any]:
    """Absolute path drawdown, including P0=1; never subtracts a benchmark."""
    positions = {d: i for i, d in enumerate(calendar)}
    output = {}
    for day in decisions:
        future = calendar[positions[day] + 1 : positions[day] + 11]
        mature = len(future) == 10 and future[-1] <= end
        rows = {}
        for code in codes:
            if not mature:
                rows[code] = {"status": "OUTCOME_NOT_MATURE", "event": None, "drawdown": None, "return": None}
                continue
            values = []
            for d in future:
                if d not in returns or code not in returns[d]:
                    raise fail("missing outcome key is not a legal NA", "source_invalid")
                value = returns[d][code]
                if value is not None and (type(value) not in (int, float) or not math.isfinite(value) or value <= -1):
                    raise fail("unknown/non-finite/invalid return", "source_invalid")
                values.append(value)
            if any(v is None for v in values):
                rows[code] = {"status": "OUTCOME_LEGAL_NA", "event": None, "drawdown": None, "return": None}
                continue
            wealth = peak = 1.0
            minimum = 0.0
            for value in values:
                wealth *= 1 + value
                peak = max(peak, wealth)
                minimum = min(minimum, wealth / peak - 1)
            if not math.isfinite(wealth):
                raise fail("compounded return is not finite", "source_invalid")
            rows[code] = {
                "status": "AVAILABLE",
                "event": int(minimum <= -0.08),
                "drawdown": minimum,
                "return": wealth - 1,
            }
        output[day] = rows
    return output


def _validate_target(target: Mapping[str, Any], *, mature: bool) -> None:
    if set(target) != {"status", "event", "drawdown", "return"}:
        raise fail("outcome fields differ", "source_invalid")
    allowed = {"AVAILABLE", "OUTCOME_LEGAL_NA"} if mature else {"OUTCOME_NOT_MATURE"}
    if target["status"] not in allowed:
        raise fail("outcome status/maturity differs", "source_invalid")
    if target["status"] != "AVAILABLE":
        if any(target[k] is not None for k in ("event", "drawdown", "return")):
            raise fail("unavailable outcome cannot be a negative example", "source_invalid")
    elif (
        type(target["event"]) is not int
        or target["event"] not in (0, 1)
        or any(type(target[k]) not in (int, float) or not math.isfinite(target[k]) for k in ("drawdown", "return"))
        or not -1 < target["drawdown"] <= 0
        or target["return"] <= -1
        or target["event"] != int(target["drawdown"] <= -0.08)
    ):
        raise fail("absolute binary outcome/value differs", "source_invalid")


def fit_predict(panel: Mapping[str, Any]) -> dict[str, Any]:
    """Receives train labels and features only, never development outcomes."""
    verify_receipt(panel)
    if panel.get("schema_version") != VERSION + "_features" or panel.get("contract") != CONTRACT:
        raise fail("feature contract differs")
    plan = schedule(panel["calendar"])
    codes = panel["catalog"]
    if len(codes) != 131 or codes != sorted(set(codes)) or panel["feature_names"] != list(ALL_CORE_FEATURES):
        raise fail("canonical 131-sector/20D contract differs")
    if set(panel["rows"]) != set(plan["train"] + plan["dev"]) or set(panel["train_labels"]) != set(plan["train"]):
        raise fail("feature/label calendar differs")
    fit_rows = []
    for day in plan["train"]:
        if set(panel["train_labels"][day]) != set(codes):
            raise fail("training label catalog differs")
        for target in panel["train_labels"][day].values():
            _validate_target(target, mature=day in plan["fit"])
    for day in plan["train"] + plan["dev"]:
        if set(panel["rows"][day]) != set(codes):
            raise fail("feature grid differs")
        for code in codes:
            row = panel["rows"][day][code]
            value = row["features"]
            if row["as_of_date"] != plan["as_of"][day]:
                raise fail("feature as-of differs")
            if value is not None:
                if len(value) != 20 or any(type(v) not in (int, float) or not math.isfinite(v) for v in value):
                    raise fail("20D observation is not finite", "source_invalid")
            elif not isinstance(row.get("reason_code"), str) or not row["reason_code"]:
                raise fail("legal unavailable observation needs a reason")
            if day in plan["fit"]:
                target = panel["train_labels"][day][code]
                if value is not None and target["status"] == "AVAILABLE" and target["event"] in (0, 1):
                    fit_rows.append((day, code, value, target["event"]))
    if not fit_rows or {r[3] for r in fit_rows} != {0, 1}:
        return receipt(
            {
                "schema_version": VERSION + "_sealed",
                "contract": CONTRACT,
                "feature_sha256": panel["receipt_sha256"],
                "fits": 0,
                "status": "TRAIN_LABEL_EVIDENCE_INSUFFICIENT",
                "predictions": [
                    {
                        "trade_date": d,
                        "as_of_date": plan["as_of"][d],
                        "sector_code": c,
                        "probability": None,
                        "warning": None,
                        "availability": "unavailable",
                        "reason_code": "hmm_risk_l2_risk_train_label_evidence_insufficient",
                    }
                    for d in plan["dev"]
                    for c in codes
                ],
            }
        )
    x = np.asarray([r[2] for r in fit_rows], dtype=np.float64)
    y = np.asarray([r[3] for r in fit_rows], dtype=np.int64)
    mean, scale = x.mean(axis=0), x.std(axis=0, ddof=0)
    # Exact constant columns can acquire tiny std from mean summation rounding.
    # This is equality-based inactivity, not an epsilon variance clamp.
    active = np.any(x != x[0], axis=0)
    mean[~active] = x[0, ~active]
    scale[~active] = 0.0
    if not active.any() or (scale[active] == 0).any() or not np.isfinite(mean).all() or not np.isfinite(scale).all():
        raise fail("invalid train-only scaling", "fit_failed")
    dates = Counter(r[0] for r in fit_rows)
    weights = np.asarray([len(fit_rows) / (len(dates) * dates[r[0]]) for r in fit_rows], dtype=np.float64)
    transformed = (x[:, active] - mean[active]) / scale[active]
    if not np.isfinite(transformed).all():
        raise fail("training transform not finite", "fit_failed")
    model = LogisticRegression(**PARAMS)
    with warnings.catch_warnings():
        warnings.simplefilter("error", ConvergenceWarning)
        try:
            model.fit(transformed, y, sample_weight=weights)
        except ConvergenceWarning as exc:
            raise fail("optimizer did not converge; no retry", "fit_failed") from exc
    if list(model.classes_) != [0, 1] or not np.isfinite(model.coef_).all() or not np.isfinite(model.intercept_).all():
        raise fail("class/parameter contract differs", "fit_failed")
    parameters = {
        "mean": mean.tolist(),
        "scale": scale.tolist(),
        "active": active.tolist(),
        "coef": model.coef_.tolist(),
        "intercept": model.intercept_.tolist(),
        "classes": model.classes_.tolist(),
        "iterations": model.n_iter_.tolist(),
    }
    predictions = []
    inactive_changed = 0
    for day in plan["dev"]:
        for code in codes:
            source = panel["rows"][day][code]
            value = source["features"]
            probability = None
            if value is not None:
                raw = np.asarray(value, dtype=np.float64)
                inactive_changed += int(np.any(raw[~active] != mean[~active]))
                z = (raw[active] - mean[active]) / scale[active]
                if not np.isfinite(z).all():
                    raise fail("development transform is not finite", "prediction_failed")
                probability = float(model.predict_proba(z.reshape(1, -1))[0, 1])
                if not math.isfinite(probability) or not 0 <= probability <= 1:
                    raise fail("class=1 probability invalid", "prediction_failed")
            predictions.append(
                {
                    "trade_date": day,
                    "as_of_date": plan["as_of"][day],
                    "sector_code": code,
                    "probability": probability,
                    "warning": probability >= 0.20 if probability is not None else None,
                    "availability": "available" if value is not None else "unavailable",
                    "reason_code": source["reason_code"] if value is None else None,
                    "structural_eligible": value is not None,
                    "volatility_Nd": value[ALL_CORE_FEATURES.index("volatility_Nd")] if value is not None else None,
                }
            )
    return receipt(
        {
            "schema_version": VERSION + "_sealed",
            "contract": CONTRACT,
            "status": "PREDICTIONS_SEALED",
            "feature_sha256": panel["receipt_sha256"],
            "input_identity": panel["input_identity"],
            "parameters": parameters,
            "model_sha256": canonical_sha256(parameters),
            "fit_rows": len(fit_rows),
            "fit_dates": len(dates),
            "sample_weight_sum": float(weights.sum()),
            "inactive_changed_rows": inactive_changed,
            "predictions": predictions,
            "fits": 1,
            "target_accessed": False,
            "tail_accessed": False,
        }
    )


def _counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    known = [r for r in rows if r["event"] is not None]
    positive = sum(r["event"] for r in known)
    warned = [r for r in known if r["warning"]]
    tp = sum(r["event"] for r in warned)
    precision = tp / len(warned) if warned else None
    recall = tp / positive if positive else None
    base = positive / len(known) if known else None
    return {
        "M": len(known),
        "positives": positive,
        "known_warnings": len(warned),
        "TP": tp,
        "FP": len(warned) - tp,
        "FN": positive - tp,
        "base_rate": base,
        "precision": precision,
        "recall": recall,
        "precision_lift": precision - base if precision is not None and base is not None else None,
        "brier": math.fsum((r["probability"] - r["event"]) ** 2 for r in known) / len(known) if known else None,
        "missed_drawdown_mean": _mean([r["drawdown"] for r in known if r["event"] and not r["warning"]]),
        "false_alert_return_mean": _mean([r["return"] for r in warned if not r["event"]]),
        "false_alert_upside_mean": _mean([max(0.0, r["return"]) for r in warned if not r["event"]]),
    }


def _mean(values: Sequence[float]) -> float | None:
    return math.fsum(values) / len(values) if values else None


def _hac(daily: Sequence[Mapping[str, Any]], calendar: Sequence[str], totals: Mapping[str, Any]) -> dict[str, Any]:
    rows = [r for r in daily if r["M"]]
    n = len(rows)
    if n < 2 or not totals["positives"] or not totals["known_warnings"]:
        return {"status": "HAC_UNAVAILABLE", "reason": "empty_denominator_or_dates"}
    w, e, m = (math.fsum(r[k] for r in rows) / n for k in ("known_warnings", "positives", "M"))
    p, rec, base = (totals[k] for k in ("precision", "recall", "base_rate"))
    influences = {
        "precision": [(r["TP"] - p * r["known_warnings"]) / w for r in rows],
        "recall": [(r["TP"] - rec * r["positives"]) / e for r in rows],
        "base_rate": [(r["positives"] - base * r["M"]) / m for r in rows],
    }
    influences["precision_lift"] = [
        a - b for a, b in zip(influences["precision"], influences["base_rate"], strict=True)
    ]
    positions = {d: i for i, d in enumerate(calendar)}
    output = {}
    for name, values in influences.items():
        lrv = math.fsum(v * v for v in values) / n
        for lag in range(1, 10):
            terms = [
                values[i] * values[j]
                for i in range(n)
                for j in range(i)
                if positions[rows[i]["trade_date"]] - positions[rows[j]["trade_date"]] == lag
            ]
            lrv += 2 * (1 - lag / 10) * math.fsum(terms) / n
        if not math.isfinite(lrv) or lrv < 0:
            output[name] = {"status": "HAC_UNAVAILABLE", "reason": "invalid_lrv"}
        else:
            se = math.sqrt(lrv / n)
            output[name] = {
                "status": "AVAILABLE",
                "n_dates": n,
                "lrv": lrv,
                "se": se,
                "interval_95": [totals[name] - 1.96 * se, totals[name] + 1.96 * se],
            }
    return output


def evaluate(sealed: Mapping[str, Any], labels: Mapping[str, Any], calendar: Sequence[str]) -> dict[str, Any]:
    verify_receipt(sealed)
    if sealed.get("schema_version") != VERSION + "_sealed" or sealed.get("contract") != CONTRACT:
        raise fail("sealed prediction contract differs")
    if sealed["status"] == "TRAIN_LABEL_EVIDENCE_INSUFFICIENT":
        if (
            type(sealed.get("fits")) is not int
            or sealed["fits"] != 0
            or any(r["probability"] is not None or r["warning"] is not None for r in sealed["predictions"])
        ):
            raise fail("insufficient training cannot contain fake probabilities")
        return {
            "effect_status": "EVIDENCE_INSUFFICIENT",
            "reason": sealed["status"],
            "metrics": None,
            "predictions": sealed["predictions"],
        }
    if (
        sealed["status"] != "PREDICTIONS_SEALED"
        or type(sealed.get("fits")) is not int
        or sealed["fits"] != 1
        or sealed.get("tail_accessed") is not False
        or sealed.get("target_accessed") is not False
        or sealed.get("model_sha256") != canonical_sha256(sealed["parameters"])
    ):
        raise fail("sealed model/fit/target boundary differs", "identity_mismatch")
    plan = schedule(calendar)
    predictions = sealed["predictions"]
    codes = sorted({r["sector_code"] for r in predictions})
    if len(codes) != 131 or len(predictions) != 131 * len(plan["dev"]) or set(labels) != set(plan["dev"]):
        raise fail("prediction/outcome denominator differs")
    daily, evaluated, baseline_rows = [], [], []
    for day in plan["dev"]:
        rows = [dict(r) for r in predictions if r["trade_date"] == day]
        if sorted(r["sector_code"] for r in rows) != codes or set(labels[day]) != set(codes):
            raise fail("duplicate/missing prediction key")
        for row in rows:
            if row["as_of_date"] != plan["as_of"][day]:
                raise fail("prediction as-of differs")
            target = labels[day][row["sector_code"]]
            _validate_target(target, mature=day in plan["mature"])
            probability = row["probability"]
            if probability is None:
                if row["warning"] is not None or row["availability"] != "unavailable" or not row["reason_code"]:
                    raise fail("unavailable probability cannot be a no-warning default")
            elif (
                type(probability) not in (int, float)
                or not math.isfinite(probability)
                or not 0 <= probability <= 1
                or type(row["warning"]) is not bool
                or row["warning"] != (probability >= 0.20)
                or row["availability"] != "available"
                or not row["structural_eligible"]
                or type(row["volatility_Nd"]) not in (int, float)
                or not math.isfinite(row["volatility_Nd"])
            ):
                raise fail("probability/warning/population contract differs")
            row.update(target)
            evaluated.append(row)
        available = [r for r in rows if r["probability"] is not None]
        budget = sum(r["warning"] for r in available)
        ordered = sorted(available, key=lambda r: -r["volatility_Nd"])
        chosen = set()
        if budget == len(ordered):
            chosen = {r["sector_code"] for r in ordered}
        elif budget:
            boundary = ordered[budget - 1]["volatility_Nd"]
            tie_crosses = ordered[budget]["volatility_Nd"] == boundary
            chosen = {
                r["sector_code"]
                for r in ordered
                if r["volatility_Nd"] > boundary or (not tie_crosses and r["volatility_Nd"] == boundary)
            }
        baseline_rows.extend({**r, "warning": r["sector_code"] in chosen} for r in available)
        stats = _counts(available)
        stats.update(
            trade_date=day,
            D=131,
            S=sum(r["structural_eligible"] for r in rows),
            P=len(available),
            warning_count=budget,
            baseline_warning_count=len(chosen),
            budget_difference=len(chosen) - budget,
            baseline_reason="P_EMPTY" if not available else None,
            evidence_day_valid=day in plan["mature"] and stats["M"] >= max(2, math.ceil(0.9 * len(available))),
        )
        daily.append(stats)
    totals = _counts([r for r in evaluated if r["probability"] is not None])
    total_s, total_p = (sum(r[k] for r in daily) for k in ("S", "P"))
    coverage = total_p / total_s if total_s else None
    warning_count = sum(r["warning_count"] for r in daily)
    totals.update(warning_count=warning_count, warning_prediction_share=warning_count / total_p if total_p else None)
    valid_share = sum(r["evidence_day_valid"] for r in daily) / len(plan["mature"])
    sufficient = (
        coverage is not None
        and coverage >= 0.9
        and valid_share >= 0.9
        and totals["positives"] > 0
        and totals["known_warnings"] > 0
    )
    effect = (
        "EVIDENCE_INSUFFICIENT"
        if not sufficient
        else (
            "DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED"
            if totals["precision_lift"] >= 0.05 and totals["recall"] >= 0.25
            else "BELOW_BINDING_RISK_MBE"
        )
    )
    blocks = [
        {
            "start": start,
            "end": end,
            **_counts([r for r in evaluated if r["probability"] is not None and start <= r["trade_date"] <= end]),
        }
        for start, end in (("2024-07-01", "2024-12-31"), ("2025-01-01", "2025-06-30"), ("2025-07-01", DEV_END))
    ]
    return {
        "effect_status": effect,
        "metrics": {
            "overall": totals,
            "prediction_coverage": coverage,
            "valid_mature_day_share": valid_share,
            "evidence_sufficient": sufficient,
            "daily": daily,
            "hac": _hac(daily, calendar, totals),
            "blocks": blocks,
            "baseline": {
                **_counts(baseline_rows),
                "brier": None,
                "promotion_gate": False,
                "probability_brier_applicable": False,
            },
            "outcome_status_counts": dict(Counter(r["status"] for r in evaluated)),
        },
        "predictions": evaluated,
    }


def close_processes(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
    *,
    feature_sha256: str,
    outcome_sha256: str,
    executor_commit: str,
) -> dict[str, Any]:
    for payload in (first, second):
        verify_receipt(payload)
        if payload.get("schema_version") != VERSION + "_repeat" or payload.get("contract") != CONTRACT:
            raise fail("fresh process contract differs")
        verify_receipt(payload["sealed"])
        sealed = payload["sealed"]
        if (
            payload.get("outcome_sha256") != outcome_sha256
            or sealed.get("feature_sha256") != feature_sha256
            or payload.get("executor_commit") != executor_commit
            or type(sealed.get("fits")) is not int
            or sealed["fits"] not in (0, 1)
            or (
                sealed["fits"] == 0
                and (
                    sealed["status"] != "TRAIN_LABEL_EVIDENCE_INSUFFICIENT"
                    or payload["result"]["effect_status"] != "EVIDENCE_INSUFFICIENT"
                )
            )
        ):
            raise fail("child differs from parent input/fit authority", "identity_mismatch")
    if canonical_json_bytes(first) != canonical_json_bytes(second):
        raise fail("fresh process parameters/predictions/metrics differ", "repeat_mismatch")
    return receipt(
        {
            "schema_version": VERSION + "_acceptance",
            "contract": CONTRACT,
            "execution_status": "COMPLETED",
            "effect_status": first["result"]["effect_status"],
            "planned_fits": 2,
            "completed_fits": 2 * first["sealed"]["fits"],
            "fresh_process_bitwise_equal": True,
            "numeric_environment": first["numeric_environment"],
            "model": {k: v for k, v in first["sealed"].items() if k not in {"predictions", "receipt_sha256"}},
            "sealed_prediction_sha256": first["sealed"]["receipt_sha256"],
            "executor_commit": executor_commit,
            "result": first["result"],
            "research_surface_status": "NOT_AVAILABLE",
            "advisory_status": "NOT_AVAILABLE",
            "database_write": False,
            "runtime_action": False,
            "tail_accessed": False,
            "validation_basis": BASIS,
        }
    )
