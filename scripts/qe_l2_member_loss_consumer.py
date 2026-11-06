"""QE member-loss preflight/formal readers and natural-entry budget adapter.

No admission registration, model fitting, inference, network IO or order creation.
"""

from __future__ import annotations

import argparse
import base64
import binascii
from copy import deepcopy
from dataclasses import dataclass
from datetime import date
import hashlib
import json
import math
from pathlib import Path
import re

from qe_l2_natural_entry_consumer import adjust_natural_entry_budgets
from qe_l2_risk_admission import canonical_sha

CONTRACT = "QE-L2-MEMBER-LOSS-NATURAL-ENTRY-V1"
PREFLIGHT = "qe_l2_member_loss_preflight_binding_v1"
PARTITION_PREFLIGHT = "qe_l2_member_loss_partition_binding_v1"
PARTITION_BUNDLE = "qe_l2_member_loss_partition_bundle_v1"
PRODUCER = "hmm_risk_l2_member_terminal_loss_ridge_v1"
RELEASE = "qe_hmm_full_v2_20261030"
MANIFEST = "772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d"
FIELD = "expected_member_loss"
START, SIGNAL_END, ECONOMIC_END = "2026-04-01", "2026-10-30", "2026-10-29"
TARGET = {
    "as_of": "previous_open_session",
    "anchor": "decision_close",
    "horizon": 10,
    "members": "PIT_at_asof_excluding_asof_suspensions",
    "weights": "strict_prior_circ_mv_fixed_at_asof",
    "price_basis": "C010_unadjusted_integer_li",
    "B": "max(0,-sum(w*R))",
    "C": "sum(w*max(0,-R))",
}


class MemberLossError(ValueError):
    def __init__(self, code, detail=""):
        self.reason_code = code
        super().__init__(f"{code}: {detail}")


def require(condition, code, detail=""):
    if not condition:
        raise MemberLossError("QE_L2_MEMBER_" + code, detail)


def _object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "JSON_DUPLICATE_KEY", key)
        result[key] = value
    return result


def _read_json_text(text):
    def nonfinite(value):
        raise MemberLossError("QE_L2_MEMBER_JSON_NONFINITE", value)

    return json.loads(text, object_pairs_hook=_object, parse_constant=nonfinite)


def read_json(path):
    return _read_json_text(Path(path).read_text(encoding="utf-8"))


def _sha(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _seal(value):
    require(
        isinstance(value, dict)
        and _sha(value.get("receipt_sha256"))
        and value["receipt_sha256"] == canonical_sha({k: v for k, v in value.items() if k != "receipt_sha256"}),
        "PRODUCER_RECEIPT",
    )


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _published(row):
    raw, value = row.get("raw_" + FIELD), row.get(FIELD)
    require(
        row.get("availability") == "available" and row.get("reason_code", "ABSENT") is None,
        "RISK_UNAVAILABLE",
        str((row.get("trade_date"), row.get("sector_code"), row.get("availability"), row.get("reason_code"))),
    )
    require(_number(raw) and _number(value) and value == min(1, max(0, raw)), "PUBLISHED_RAW_MISMATCH")
    return value


@dataclass
class ValidatedMemberLoss:
    rows: dict
    as_of: dict
    receipt: dict

    def risk_context(self, day, stock_sectors):
        require(day in self.as_of, "DECISION_DAY", day)
        rows = {sector: deepcopy(row) for (d, sector), row in self.rows.items() if d == day}
        require(isinstance(stock_sectors, dict) and set(stock_sectors.values()) <= set(rows), "MAPPING_MISSING")
        return {"sector_rows": rows, "stock_sectors": dict(stock_sectors)}


def validate_predictions(prediction, binding, calendar):
    """Validate an explicitly pinned file; this never grants formal admission."""
    if isinstance(binding, dict) and binding.get("schema_version") == PARTITION_PREFLIGHT:
        return _validate_partition_bundle(prediction, binding, calendar)
    require(
        isinstance(binding, dict)
        and set(binding) == {
            "schema_version", "release_binding", "prediction_sha256", "producer_contract_sha256",
            "calendar_sha256", "industry_catalog", "models",
        }
        and binding.get("schema_version") == PREFLIGHT,
        "PREFLIGHT_BINDING",
    )
    release = binding["release_binding"]
    require(
        isinstance(release, dict) and release.get("manifest_sha256") == MANIFEST
        and release.get("release_id") == RELEASE and release.get("cutoff") == SIGNAL_END,
        "DATASET_IDENTITY",
    )
    require(all(_sha(binding[k]) for k in ("prediction_sha256", "producer_contract_sha256", "calendar_sha256")), "PIN")
    require(isinstance(calendar, list) and calendar == sorted(set(calendar)), "CALENDAR")
    require(all(isinstance(d, str) and date.fromisoformat(d).isoformat() == d for d in calendar), "CALENDAR")
    days = [d for d in calendar if START <= d <= SIGNAL_END]
    economic = [d for d in days if d <= ECONOMIC_END]
    require(
        len(days) == 142 and days[0] == START and days[-1] == SIGNAL_END
        and len(economic) == 141 and economic[-1] == ECONOMIC_END
        and sum(d <= "2026-09-30" for d in days) == 125
        and sum(d >= "2026-10-01" for d in economic) == 16
        and calendar.index(days[0]) > 0,
        "SIGNAL_ECONOMIC_WINDOW",
    )
    positions = {d: i for i, d in enumerate(calendar)}
    as_of = {d: calendar[positions[d] - 1] for d in days}
    catalog = binding["industry_catalog"]
    require(
        isinstance(catalog, list) and len(catalog) == 131 and catalog == sorted(set(catalog))
        and all(isinstance(s, str) and re.fullmatch(r"\d{6}\.SI", s) for s in catalog),
        "FULL_CATALOG",
    )
    _seal(prediction)
    require(
        prediction.get("schema_version") == PRODUCER + "_predictions" and prediction.get("arm") == "C"
        and prediction.get("release_binding") == release
        and prediction.get("research_only") is True and prediction.get("historical_as_known") is False
        and prediction.get("untouched_holdout") is False and prediction.get("policy_applied") is False,
        "PRODUCER_IDENTITY",
    )
    contract = prediction.get("contract", {})
    require(
        isinstance(contract, dict) and contract.get("version") == "hmm_risk_l2_member_loss_study_v1"
        and contract.get("target") == TARGET and canonical_sha(contract) == binding["producer_contract_sha256"],
        "PRODUCER_CONTRACT",
    )
    pins = binding["models"]
    require(isinstance(pins, dict) and set(pins) == {"2026Q2", "2026Q3", "2026Q4"}, "Q4_MODELS_REQUIRED")
    bundles = prediction.get("model_bundles")
    require(isinstance(bundles, list) and len(bundles) == 3, "MODEL_POPULATION")
    vintages = {}
    for bundle in bundles:
        _seal(bundle)
        vintage = bundle.get("vintage", {})
        require(isinstance(vintage, dict), "MODEL_VINTAGE")
        key = vintage.get("id")
        require(key in pins and key not in vintages, "MODEL_VINTAGE")
        pin = pins[key]
        require(
            isinstance(pin, dict) and set(pin) == {"vintage", "model_sha256", "receipt_sha256"}
            and vintage == pin["vintage"] and bundle["receipt_sha256"] == pin["receipt_sha256"]
            and _sha(pin["model_sha256"]) and bundle.get("model_sha256") == pin["model_sha256"]
            == canonical_sha(bundle.get("parameters"))
            and bundle.get("schema_version") == PRODUCER + "_model" and bundle.get("arm") == "C"
            and bundle.get("catalog") == catalog and bundle.get("contract") == contract,
            "MODEL_PIN",
        )
        bounds = [vintage.get(k) for k in ("train_start", "train_end", "effective_start", "effective_end")]
        require(all(isinstance(d, str) and date.fromisoformat(d).isoformat() == d for d in bounds), "MODEL_DATES")
        require(bounds[0] <= bounds[1] < bounds[2] <= bounds[3], "MODEL_CLOCK")
        vintages[key] = vintage
    vintage_by_day = {}
    for day in days:
        effective = [k for k, v in vintages.items() if v["effective_start"] <= day <= v["effective_end"]]
        require(len(effective) == 1, "MODEL_DAY_COVERAGE", day)
        vintage_by_day[day] = effective[0]
    rows = {}
    predictions = prediction.get("predictions")
    require(isinstance(predictions, list) and len(predictions) == 142 * 131, "FULL_GRID")
    for row in predictions:
        require(isinstance(row, dict), "ROW_SCHEMA")
        day, sector = row.get("trade_date"), row.get("sector_code")
        require(day in as_of and sector in catalog and (day, sector) not in rows, "ROW_KEY")
        require(row.get("as_of_date") == as_of[day], "EXTRA_LAG_OR_CLOCK")
        vintage = vintage_by_day[day]
        require(row.get("vintage_id") == vintage and row.get("model_sha256") == pins[vintage]["model_sha256"], "ROW_MODEL")
        future = calendar[positions[day] + 1: positions[day] + 11]
        require(
            "target_first_return_date" in row and "target_last_return_date" in row
            and row["target_first_return_date"] == (future[0] if future else None)
            and row["target_last_return_date"] == (future[-1] if len(future) == 10 else None),
            "TARGET_CALENDAR",
        )
        _published(row)
        rows[day, sector] = deepcopy(row)
    require(set(rows) == {(d, s) for d in days for s in catalog}, "FULL_GRID")
    return ValidatedMemberLoss(rows, as_of, {
        "status": "PRE_ADMISSION_VALIDATED", "contract": CONTRACT, "risk_field": FIELD,
        "signal_days": 142, "economic_days": 141, "original_days": 125, "october_economic_days": 16,
        "rows": len(rows), "industries": 131, "producer_receipt_sha256": prediction["receipt_sha256"],
        "historical_as_known": False, "untouched_holdout": False,
        "formal_admission": "NOT_EVALUATED", "AF_mapping_Composer": "NOT_EVALUATED", "experiment_ready": False,
    })


def _partition_document(part, key, expected_sha):
    """Decode bounded, exact publisher bytes; never rewrite a producer receipt."""
    ref = part.get(key)
    require(
        isinstance(ref, dict) and set(ref) == {"sha256", "base64"}
        and _sha(expected_sha) and ref["sha256"] == expected_sha
        and isinstance(ref["base64"], str) and len(ref["base64"]) <= 16 * 1024 * 1024,
        "PARTITION_ASSET", key,
    )
    try:
        raw = base64.b64decode(ref["base64"], validate=True)
    except (ValueError, binascii.Error) as error:
        raise MemberLossError("QE_L2_MEMBER_PARTITION_ENCODING", key) from error
    require(hashlib.sha256(raw).hexdigest() == expected_sha, "PARTITION_FILE_HASH", key)
    value = _read_json_text(raw.decode("utf-8"))
    _seal(value)
    return value


def _validate_partition_bundle(bundle, binding, calendar):
    require(
        set(binding) == {"schema_version", "release_binding", "prediction_sha256", "calendar_sha256",
                         "industry_catalog", "inference_identity_sha256", "partitions"}, "PARTITION_BINDING",
    )
    release = binding["release_binding"]
    require(
        isinstance(release, dict) and release.get("manifest_sha256") == MANIFEST
        and release.get("release_id") == RELEASE and release.get("cutoff") == SIGNAL_END, "DATASET_IDENTITY",
    )
    require(all(_sha(binding[k]) for k in ("prediction_sha256", "calendar_sha256", "inference_identity_sha256")), "PIN")
    require(isinstance(calendar, list) and calendar == sorted(set(calendar)), "CALENDAR")
    require(all(isinstance(d, str) and date.fromisoformat(d).isoformat() == d for d in calendar), "CALENDAR")
    days = [d for d in calendar if START <= d <= SIGNAL_END]
    require(
        len(days) == 142 and days[0] == START and days[-1] == SIGNAL_END
        and len([d for d in days if d <= ECONOMIC_END]) == 141
        and sum(d <= "2026-09-30" for d in days) == 125 and calendar.index(START) > 0,
        "SIGNAL_ECONOMIC_WINDOW",
    )
    positions = {d: i for i, d in enumerate(calendar)}
    as_of = {d: calendar[positions[d] - 1] for d in days}
    catalog = binding["industry_catalog"]
    require(
        isinstance(catalog, list) and len(catalog) == 131 and catalog == sorted(set(catalog))
        and all(isinstance(s, str) and re.fullmatch(r"\d{6}\.SI", s) for s in catalog), "FULL_CATALOG",
    )
    quarters = ("2026Q2", "2026Q3", "2026Q4")
    pins = binding["partitions"]
    require(isinstance(pins, dict) and set(pins) == set(quarters), "Q4_MODELS_REQUIRED")
    require(
        isinstance(bundle, dict) and set(bundle) == {"schema_version", "partitions"}
        and bundle["schema_version"] == PARTITION_BUNDLE
        and isinstance(bundle["partitions"], dict) and set(bundle["partitions"]) == set(quarters), "PARTITION_BUNDLE",
    )
    rows, source_receipts = {}, {}
    for quarter in quarters:
        pin, part = pins[quarter], bundle["partitions"][quarter]
        require(
            isinstance(pin, dict) and set(pin) == {"prediction_sha256", "model_file_sha256", "model_receipt_sha256",
                "model_sha256", "producer_contract_sha256", "training_release_binding_sha256", "vintage", "acceptance_sha256"}
            and isinstance(part, dict) and set(part) == {"prediction", "model"}, "PARTITION_PIN", quarter,
        )
        prediction = _partition_document(part, "prediction", pin["prediction_sha256"])
        source_model = _partition_document(part, "model", pin["model_file_sha256"])
        current = quarter == "2026Q4"
        model = source_model.get("model") if current else source_model
        _seal(model)
        producer = "hmm_risk_l2_member_release_v1" if current else PRODUCER
        version = "hmm_risk_l2_member_release_v1" if current else "hmm_risk_l2_member_loss_study_v1"
        contract = prediction.get("contract")
        require(
            isinstance(contract, dict) and contract.get("version") == version and contract.get("target") == TARGET
            and canonical_sha(contract) == pin["producer_contract_sha256"]
            and all(contract.get(k) is False for k in ("historical_as_known", "untouched_holdout", "policy_applied")),
            "PRODUCER_CONTRACT", quarter,
        )
        require(
            prediction.get("schema_version") == producer + "_prediction"
            and type(prediction.get("fits")) is int and prediction["fits"] == 0
            and prediction.get("policy_applied") is False, "PRODUCER_IDENTITY", quarter,
        )
        inference = prediction.get("inference_identity")
        require(
            isinstance(inference, dict) and canonical_sha(inference) == binding["inference_identity_sha256"]
            and inference.get("calendar_sha256") == binding["calendar_sha256"], "PARTITION_INFERENCE_IDENTITY", quarter,
        )
        identity = inference.get("release_identity", {})
        require(
            identity.get("dataset_manifest_sha256") == release["manifest_sha256"]
            and identity.get("release_id") == release["release_id"] and identity.get("cutoff") == SIGNAL_END
            and identity.get("frozen_release_generation") == release.get("generation")
            and identity.get("metadata_sha256", {}).get("dataset_manifest") == release.get("manifest_file_sha256"),
            "DATASET_IDENTITY", quarter,
        )
        require(
            model.get("schema_version") == producer + "_model" and model.get("arm") == "C"
            and model.get("catalog") == catalog and model.get("contract") == contract
            and model["receipt_sha256"] == pin["model_receipt_sha256"]
            and _sha(pin["model_sha256"]) and model.get("model_sha256") == pin["model_sha256"]
            == canonical_sha(model.get("parameters"))
            and canonical_sha(model.get("release_binding")) == pin["training_release_binding_sha256"],
            "MODEL_PIN", quarter,
        )
        vintage = model.get("vintage")
        require(isinstance(vintage, dict) and vintage == pin["vintage"] and vintage.get("id") == quarter, "MODEL_VINTAGE")
        bounds = [vintage.get(k) for k in ("train_start", "train_end", "effective_start", "effective_end")]
        require(all(isinstance(d, str) and date.fromisoformat(d).isoformat() == d for d in bounds), "MODEL_DATES")
        require(bounds[0] <= bounds[1] < bounds[2] <= bounds[3], "MODEL_CLOCK")
        if current:
            require(
                prediction.get("arm") == "C" and contract.get("vintage") == vintage
                and model.get("release_binding") == release
                and _sha(pin["acceptance_sha256"]) and prediction.get("acceptance_sha256") == pin["acceptance_sha256"]
                and source_model.get("predictions") == prediction.get("predictions"), "Q4_MODEL_LINK",
            )
        else:
            require(
                prediction.get("model_bundle_sha256") == model["receipt_sha256"]
                and vintage in contract.get("vintages", []) and pin["acceptance_sha256"] is None
                and all(prediction.get(k) is False for k in ("labels_read", "outcomes_read", "forward_confirmed", "historical_as_known")),
                "HISTORICAL_MODEL_LINK",
            )
        expected_days = {d for d in days if bounds[2] <= d <= bounds[3]}
        require(bool(expected_days), "MODEL_DAY_COVERAGE", quarter)
        predictions = prediction.get("predictions")
        require(isinstance(predictions, list) and len(predictions) == len(expected_days) * 131, "FULL_GRID", quarter)
        for row in predictions:
            require(isinstance(row, dict), "ROW_SCHEMA")
            day, sector = row.get("trade_date"), row.get("sector_code")
            require(day in expected_days and sector in catalog and (day, sector) not in rows, "ROW_KEY")
            require(row.get("as_of_date") == as_of[day], "EXTRA_LAG_OR_CLOCK")
            require(row.get("vintage_id") == quarter and row.get("model_sha256") == pin["model_sha256"], "ROW_MODEL")
            future = calendar[positions[day] + 1:positions[day] + 11]
            require(
                "target_first_return_date" in row and "target_last_return_date" in row
                and row["target_first_return_date"] == (future[0] if future else None)
                and row["target_last_return_date"] == (future[-1] if len(future) == 10 else None), "TARGET_CALENDAR",
            )
            _published(row)
            rows[day, sector] = deepcopy(row)
        source_receipts[quarter] = {"prediction_sha256": pin["prediction_sha256"],
                                    "producer_receipt_sha256": prediction["receipt_sha256"],
                                    "model_receipt_sha256": model["receipt_sha256"]}
    require(set(rows) == {(d, s) for d in days for s in catalog}, "FULL_GRID")
    return ValidatedMemberLoss(rows, as_of, {
        "status": "PRE_ADMISSION_VALIDATED", "contract": CONTRACT, "risk_field": FIELD,
        "signal_days": 142, "economic_days": 141, "original_days": 125, "october_economic_days": 16,
        "rows": len(rows), "industries": 131, "source_receipts": source_receipts,
        "historical_as_known": False, "untouched_holdout": False,
        "formal_admission": "NOT_EVALUATED", "AF_mapping_Composer": "NOT_EVALUATED", "experiment_ready": False,
    })


def adjust_member_entry_budgets(
    decision_date, as_of, ordered_targets, natural_new_buys, base_budgets_cny, nav_cny, original_caps, risk_context
):
    """Adapter to the unchanged approved kernel; never relabel a stored asset."""
    internal = None
    if risk_context is not None:
        require(isinstance(risk_context, dict) and set(risk_context) == {"sector_rows", "stock_sectors"}, "CONTEXT")
        internal = deepcopy(risk_context)
        require(isinstance(internal["sector_rows"], dict) and len(internal["sector_rows"]) == 131, "FULL_CATALOG")
        for row in internal["sector_rows"].values():
            require(isinstance(row, dict) and "expected_loss" not in row, "FIELD_AMBIGUITY")
            row["expected_loss"] = _published(row)
    result, audit = adjust_natural_entry_budgets(
        decision_date, as_of, ordered_targets, natural_new_buys, base_budgets_cny, nav_cny, original_caps, internal
    )
    audit["contract"] = CONTRACT
    audit["risk_field"] = FIELD if risk_context is not None else None
    audit["risk_inputs"] = deepcopy(risk_context["sector_rows"]) if risk_context is not None else {}
    return result, audit


def validate_saved_af_evidence(evidence, manifest):
    """Accept reviewed saved bytes without claiming a successful old inference."""
    from qe_l2_member_loss_admission import SAVED_AF_MODE

    require(manifest.get("af_evidence_mode") == SAVED_AF_MODE, "AF_EVIDENCE_MODE")
    require(
        isinstance(evidence, dict)
        and evidence.get("schema_version") == "qe_saved_af_asset_evidence_v1"
        and evidence.get("evidence_type") == "SAVED_ASSET_NOT_ORIGINAL_INFERENCE"
        and evidence.get("seed") == manifest["seed"]
        and evidence.get("release_id") == RELEASE
        and evidence.get("dataset_manifest_sha256") == MANIFEST
        and evidence.get("original_runtime_integrity") == "UNKNOWN"
        and evidence.get("legacy_reference_equivalent") is False
        and evidence.get("data_asset_acceptance") == "ACCEPTED_WITH_DISCLOSED_GAPS"
        and evidence.get("formal_admission") == "NOT_ADMITTED",
        "AF_SAVED_EVIDENCE",
    )
    review = evidence.get("bridge_review", {})
    require(
        isinstance(review, dict)
        and review.get("schema_version") == "qe_af_version_bridge_review_v1"
        and review.get("contract_id") == "QE-AF-772-VERSION-BRIDGE-V1"
        and review.get("release_id") == RELEASE and review.get("dataset_manifest_sha256") == MANIFEST
        and review.get("data_asset_acceptance") == "ACCEPTED_WITH_DISCLOSED_GAPS"
        and review.get("legacy_reference_equivalent") is False
        and review.get("legacy_strict_acceptance") == "FAILED_PRESERVED"
        and review.get("original_runtime_integrity") == "UNKNOWN"
        and review.get("formal_admission") == "NOT_ADMITTED"
        and canonical_sha(review) == evidence.get("bridge_review_canonical_sha256"),
        "AF_BRIDGE_REVIEW",
    )
    unknown = review.get("original_runtime_fingerprints", {})
    require(
        all(key in unknown and unknown[key] is None for key in (
            "processor_before", "processor_after", "weights_before", "weights_after"
        )), "AF_RUNTIME_UNKNOWN_REQUIRED",
    )
    rows = review.get("fresh_readback", {}).get("rows", [])
    candidates = [row for row in rows if row.get("seed") == manifest["seed"]]
    require(len(candidates) == 1, "AF_SAVED_SOURCE")
    row = candidates[0]
    source, saved = evidence.get("source", {}), evidence.get("saved_prediction", {})
    loop = {42: 1, 2024: 3, 2026: 5}[manifest["seed"]]
    require(
        source.get("task_id") == row.get("source_task_id") == "qe_20261028_104720_a38a"
        and source.get("loop_index") == row.get("source_loop_index") == loop
        and source.get("model_sha256") == row.get("model_file_sha256")
        and source.get("dataset_sha256") == row.get("dataset_pickle_sha256")
        and source.get("processor_asset_fingerprint") == row.get("source_processor_fingerprint_now")
        and source.get("training_manifest_sha256") == "590b3b204085373ff4bd101efc50a45c6dd3844200200f987f9bc783e0c6b394"
        and all(_sha(source.get(k)) for k in ("model_sha256", "dataset_sha256", "processor_asset_fingerprint")),
        "AF_SAVED_SOURCE",
    )
    require(
        saved.get("sha256") == row.get("candidate_sha256") == manifest["assets"]["prediction"]["sha256"]
        and saved.get("index_sha256") == row.get("prediction_index_sha256")
        and type(saved.get("rows")) is int and saved["rows"] > 0
        and saved["rows"] == row.get("prediction_rows")
        and type(saved.get("days")) is int and saved["days"] > 0
        and saved["days"] == row.get("prediction_days")
        and (saved.get("start"), saved.get("end")) == ("2026-01-05", "2026-10-30")
        and _sha(saved.get("panel_sha256")) and _sha(saved.get("index_sha256")),
        "AF_SAVED_PREDICTION",
    )
    # These disclosures remain part of the admitted evidence, including causes
    # that the data owner could not establish. No all-NA or zero-risk rewrite.
    eps = review.get("data_owner_confirmation", {}).get("EPS", {})
    require(
        (eps.get("keys"), eps.get("stocks"), eps.get("days")) == (211, 15, 65)
        and eps.get("historical_initial_missing_cause") == "UNKNOWN"
        and eps.get("may_claim_legitimate_na") is False,
        "AF_MISSING_DISCLOSURE",
    )
    return saved


class MemberEntryInputs:
    """Formal admitted inputs. A preflight binding alone can never enter here."""

    def __init__(self, manifest_file):
        import pandas as pd
        from qe_l2_continuous_consumer import ContinuousInputs, prediction_panel_sha256
        from qe_l2_member_loss_admission import SAVED_AF_MODE, STRICT_AF_MODE, validate_admission, validate_projection
        from qe_l2_risk_consumer import checked_asset

        self.manifest = m = read_json(manifest_file)
        admission = validate_admission(m)
        code = m.get("execution_files_sha256")
        require(
            isinstance(code, dict) and bool(code)
            and canonical_sha({"files": code, "contract": m.get("execution_contract")}) == m.get("execution_sha256"),
            "EXECUTION_CODE_IDENTITY",
        )
        for name, digest in code.items():
            require(
                isinstance(name, str) and not Path(name).is_absolute() and ".." not in Path(name).parts
                and "\\" not in name and name.endswith(".py"), "EXECUTION_CODE_PATH",
            )
            checked_asset({"path": name, "sha256": digest})
        paths = {k: checked_asset(v) for k, v in m["assets"].items() if k != "loss" or m["arm"] == "C"}
        ContinuousInputs._validate_dataset(m)
        calendar = paths["calendar"].read_text().splitlines()
        self._calendar = pd.DatetimeIndex(pd.to_datetime(calendar))
        require(self._calendar.is_unique and self._calendar.is_monotonic_increasing, "CALENDAR")
        signal_days = [d for d in calendar if START <= d <= SIGNAL_END]
        require(
            len(signal_days) == 142 and signal_days[0] == START and signal_days[-1] == SIGNAL_END
            and sum(d <= ECONOMIC_END for d in signal_days) == 141 and self._calendar.get_loc(START) > 0,
            "SIGNAL_ECONOMIC_WINDOW",
        )
        self.as_of = {d: calendar[calendar.index(d) - 1] for d in signal_days}
        self.execution_as_of = {d: a for d, a in self.as_of.items() if d <= ECONOMIC_END}
        authority = read_json(paths["authority"])
        extended = authority.get("schema_version") == "hmm_risk_release_l2_classification_binding_v1"
        baseline = authority["baseline"] if extended else authority
        if extended:
            require(
                authority.get("receipt_sha256") == canonical_sha({k: v for k, v in authority.items() if k != "receipt_sha256"})
                and authority["release_binding"]["manifest_sha256"] == m["release_identity"]["dataset_manifest_sha256"]
                and authority["release_binding"]["release_id"] == RELEASE
                and (authority["extension_start"], authority["extension_end"]) == ("2026-10-01", SIGNAL_END)
                and all(_sha(authority.get(k)) for k in ("component_receipt_sha256", "code_map_sha256", "membership_sha256")),
                "AUTHORITY_RELEASE",
            )
        catalog = sorted(r["canonical_l2_code"] for r in baseline["l2_projection"]["rows"])
        require(
            len(set(catalog)) == len(catalog) == 131
            and canonical_sha(authority) == m["authority_canonical_sha256"]
            and baseline["research_basis"]["active_mode"] == "historical_replay"
            and baseline["research_basis"]["historical_classification_basis"] == "stable_taxonomy_backcast",
            "AUTHORITY",
        )
        binding = m["risk_binding"]
        require(
            binding["prediction_sha256"] == m["assets"]["loss"]["sha256"]
            and binding["calendar_sha256"] == m["assets"]["calendar"]["sha256"]
            and binding["industry_catalog"] == catalog
            and binding["release_binding"]["manifest_sha256"] == m["release_identity"]["dataset_manifest_sha256"],
            "RISK_BINDING",
        )
        af_receipt = read_json(paths["AF_receipt"])
        af_mode = m.get("af_evidence_mode", STRICT_AF_MODE)
        saved = validate_saved_af_evidence(af_receipt, m) if af_mode == SAVED_AF_MODE else None
        require(saved is not None or (
            af_receipt.get("schema_version") == "qe_frozen_inference_result_v1"
            and af_receipt.get("status") == "verified" and af_receipt.get("fit_calls") == 0
            and af_receipt.get("reference_equivalent") is True and af_receipt.get("source_bytes_unchanged") is True
            and af_receipt.get("reference_max_abs_error") == 0
            and af_receipt["spec"]["input_manifest_sha256"] == m["release_identity"]["dataset_manifest_sha256"]
            and af_receipt.get("processor_state_before") is not None
            and af_receipt.get("model_weights_before") is not None
            and af_receipt["processor_state_before"] == af_receipt["processor_state_after"]
            and af_receipt["model_weights_before"] == af_receipt["model_weights_after"]
        ), "AF_FROZEN_RECEIPT",
        )
        prediction = pd.read_pickle(paths["prediction"])
        # Native Qlib LGBModel returns Series; normalize only the in-memory
        # canonical panel. The admitted pickle bytes remain unchanged.
        if isinstance(prediction, pd.Series):
            prediction = prediction.to_frame("score")
        require(
            isinstance(prediction, pd.DataFrame) and list(prediction.columns) == ["score"]
            and isinstance(prediction.index, pd.MultiIndex) and prediction.index.names == ["datetime", "instrument"]
            and prediction.index.is_unique and prediction.index.is_monotonic_increasing
            and prediction.score.map(_number).all(), "AF_SCHEMA",
        )
        expected_panel = saved["panel_sha256"] if saved is not None else af_receipt["executable_prediction_panel_sha256"]
        require(prediction_panel_sha256(prediction) == expected_panel, "AF_RECEIPT_PANEL")
        if saved is not None:
            index_sha = hashlib.sha256(pd.util.hash_pandas_object(prediction.index, index=True).values.tobytes()).hexdigest()
            require(len(prediction) == saved["rows"] and index_sha == saved["index_sha256"], "AF_SAVED_INDEX")
        projection = validate_projection(m["prediction_projection"], m["assets"]["prediction"]["sha256"])
        dates = prediction.index.get_level_values("datetime")
        source_days = self._calendar[(self._calendar >= projection["source_start"]) & (self._calendar <= projection["source_end"])]
        require(pd.DatetimeIndex(dates.unique()).equals(source_days), "AF_FULL_REFERENCE_WINDOW")
        prediction = prediction.loc[(dates >= projection["start"]) & (dates <= projection["end"])]
        require(prediction_panel_sha256(prediction) == projection["panel_sha256"], "AF_PROJECTED_PANEL")
        self.mapping = self._mapping(paths, authority, catalog, prediction)
        self.risk = validate_predictions(read_json(paths["loss"]), binding, calendar).rows if m["arm"] == "C" else {}
        self.receipt = {
            "status": "FORMAL_MEMBER_INPUTS_VERIFIED", "contract": CONTRACT, "arm": m["arm"], "seed": m["seed"],
            "admission_version": admission, "signal_days": 142, "economic_days": 141, "industries": 131,
            "risk_rows": len(self.risk), "new_fits": 0, "new_inferences": 0,
            "af_evidence_mode": af_mode,
            "original_runtime_integrity": "UNKNOWN" if saved is not None else "STRICT_FROZEN_RECEIPT_VERIFIED",
            "legacy_reference_equivalent": False if saved is not None else True,
            "historical_as_known": False, "untouched_holdout": False,
        }

    def _mapping(self, paths, authority, catalog, prediction):
        import pandas as pd

        m = self.manifest
        receipt = read_json(paths["mapping_receipt"])
        mm = receipt["mapping_manifest"]
        extended = authority.get("schema_version") == "hmm_risk_release_l2_classification_binding_v1"
        base = authority["baseline"] if extended else authority
        base_mm = mm["baseline"] if extended else mm
        if extended:
            require(
                mm.get("schema_version") == "hmm_risk_release_l2_classification_binding_v1_mapping"
                and mm.get("binding_sha256") == authority["receipt_sha256"]
                and mm.get("release_binding") == authority["release_binding"]
                and mm.get("membership_sha256") == authority["membership_sha256"]
                and mm.get("code_map_sha256") == authority["code_map_sha256"]
                and mm.get("historical_as_known") is False and mm.get("l3_identity_inferred") is False
                and mm.get("canonical_l2_count") == 131,
                "MAPPING_RELEASE_IDENTITY",
            )
        require(
            receipt["mapping_sha256"] == m["assets"]["mapping"]["sha256"]
            and receipt["authority_spec_sha256"] == m["assets"]["authority"]["sha256"]
            and receipt["membership_file_sha256"] == m["assets"]["pit"]["sha256"]
            and base_mm["non_as_known_taxonomy"] is True and base_mm["active_classification_basis"] == "stable_taxonomy_backcast"
            and base_mm["candidate_bundle_hash"] == base["identity"]["bundle_hash"]
            and base_mm["research_basis_contract_sha256"] == base["research_basis"]["canonical_hash"]
            and base_mm["l2_code_projection_sha256"] == base["l2_projection"]["canonical_hash"], "MAPPING_IDENTITY",
        )
        frame = pd.read_parquet(paths["mapping"])
        for col in ("as_of_date", "trade_date"):
            frame[col] = pd.to_datetime(frame[col]).dt.strftime("%Y-%m-%d")
        require(not frame.duplicated(["as_of_date", "instrument"]).any(), "MAPPING_DUPLICATE")
        expected_classification = pd.Series(base_mm["classification_authority_receipt_hash"], index=frame.index)
        expected_reason = pd.Series("classification:classification_authority_unavailable", index=frame.index)
        if extended:
            extension_rows = frame.as_of_date.ge(authority["extension_start"])
            expected_classification.loc[extension_rows] = authority["receipt_sha256"]
            expected_reason.loc[extension_rows] = "classification:release_membership_unavailable"
        resolved, na = frame.mapping_status.eq("resolved"), frame.mapping_status.eq("unavailable")
        require(
            (resolved | na).all() and frame.loc[resolved, "l2_code"].isin(catalog).all()
            and frame.loc[resolved, "mapping_reason"].isna().all()
            and frame.loc[na, "l2_code"].isna().all()
            and frame.loc[na, "mapping_reason"].eq(expected_reason.loc[na]).all()
            and frame.non_as_known_taxonomy.eq(True).all()
            and frame.classification_receipt_hash.eq(expected_classification).all(), "MAPPING_AUTHORITY",
        )
        used = frame.used_in_142_signal_days.eq(True)
        economic = frame.used_in_141_economic_days.eq(True)
        require(
            frame.loc[used, "trade_date"].isin(self.as_of).all()
            and frame.loc[used, "trade_date"].map(self.as_of).eq(frame.loc[used, "as_of_date"]).all()
            and frame.loc[~used, "as_of_date"].eq(SIGNAL_END).all() and frame.loc[~used, "trade_date"].isna().all()
            and economic.equals(used & frame.trade_date.le(ECONOMIC_END)), "MAPPING_CLOCK",
        )
        keys = pd.MultiIndex.from_arrays([
            prediction.index.get_level_values("datetime").strftime("%Y-%m-%d"),
            prediction.index.get_level_values("instrument"),
        ])
        mapped = pd.MultiIndex.from_frame(frame[["as_of_date", "instrument"]])
        require(len(keys) == len(mapped) and keys.isin(mapped).all(), "MAPPING_FULL_AF_POPULATION")
        return frame.set_index(["as_of_date", "instrument"])

    def risk_context(self, day, stocks):
        from qe_l2_risk_consumer import symbol_key

        require(day in self.execution_as_of, "EXECUTION_DAY")
        if self.manifest["arm"] == "B0":
            return None
        sectors = {}
        for stock in stocks:
            key = self.as_of[day], symbol_key(stock)
            require(key in self.mapping.index, "MAPPING_MISSING", str(key))
            row = self.mapping.loc[key]
            require(row.mapping_status == "resolved", "MAPPING_UNAVAILABLE", str(key))
            sectors[stock] = row.l2_code
        return {"sector_rows": {s: deepcopy(r) for (d, s), r in self.risk.items() if d == day}, "stock_sectors": sectors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prediction", required=True)
    parser.add_argument("--binding")
    parser.add_argument("--calendar")
    parser.add_argument("--formal-manifest")
    parser.add_argument("--config")
    args = parser.parse_args()
    try:
        if args.formal_manifest:
            import yaml
            from qe_l2_risk_consumer import validate_execution_inputs

            require(args.config is not None and args.binding is None and args.calendar is None, "FORMAL_ARGUMENTS")
            inputs = MemberEntryInputs(args.formal_manifest)
            validate_execution_inputs(inputs.manifest, args.config, args.prediction)
            conf = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
            require(
                list(map(str, conf["task"]["dataset"]["kwargs"]["segments"]["test"]))
                == ["2026-03-31", SIGNAL_END], "EXECUTION_PREDICTION_WINDOW",
            )
            execution = json.loads(json.dumps({k: conf[k] for k in ("port_analysis_config", "qlib_init", "market")}, default=str))
            require(execution == inputs.manifest["execution_contract"], "EXECUTION_CONTRACT_DRIFT")
            print(json.dumps(inputs.receipt, ensure_ascii=False, allow_nan=False))
            return 0
        require(args.binding is not None and args.calendar is not None and args.config is None, "PREFLIGHT_ARGUMENTS")
        binding = read_json(args.binding)
        require(isinstance(binding, dict), "PREFLIGHT_BINDING")
        for key, path in (("prediction_sha256", args.prediction), ("calendar_sha256", args.calendar)):
            require(hashlib.sha256(Path(path).read_bytes()).hexdigest() == binding.get(key), "FILE_HASH", key)
        inputs = validate_predictions(read_json(args.prediction), binding, Path(args.calendar).read_text().splitlines())
    except (ValueError, KeyError, TypeError, OSError) as error:
        print(json.dumps({"status": "REJECTED", "reason": str(error), "experiment_ready": False}, allow_nan=False))
        return 2
    print(json.dumps(inputs.receipt, ensure_ascii=False, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
