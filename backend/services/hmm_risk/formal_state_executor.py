"""File-only authority binding and two-fresh-process current model-set runner."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, BASE_FEATURES, canonical_json_bytes, canonical_sha256
from backend.services.hmm_risk.formal_state_calendar import (
    evaluate_calendar_evidence,
    validate_calendar_carrier,
    validate_calendar_evidence,
)
from backend.services.hmm_risk.formal_state_model import (
    CONTRACTS,
    FAMILIES,
    SEEDS,
    VERSION,
    FormalStateError,
    array,
    fit_entry,
    preprocess_apply,
    preprocess_fit,
    receipt,
    restore_model,
    select_restart,
    validate_fit_entry,
)
from backend.services.hmm_risk.stock_fact_observation import validate_c010_policy_manifest

ACTIVE_GENERATION = "20260928-v15-unified-moneyflow1"
ACTIVE_MANIFEST = "2225e1ea28f099f4972b6a465e4aa093d3767592e2651484b79700586bf358fc"
PIT_BUNDLE = "051e2af357703734080ff3ea5b4311926905aa7cbd1f31d926ef5b8575261313"
TRAIN_CALENDAR_HASH = "b48fb5e911295d1c16920178b6ea48285c5890455aeaa31ad03ef7e11841f715"
THREAD_VARIABLES = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
)
EXPECTED_VERSIONS = {
    "python": "3.13.5",
    "numpy": "2.3.3",
    "scipy": "1.16.3",
    "scikit-learn": "1.8.0",
    "hmmlearn": "0.3.3",
    "threadpoolctl": "3.6.0",
}


def numeric_environment() -> dict[str, Any]:
    from importlib.metadata import version

    from threadpoolctl import threadpool_info

    versions = {name: platform.python_version() if name == "python" else version(name) for name in EXPECTED_VERSIONS}
    pools = threadpool_info()
    if versions != EXPECTED_VERSIONS or any(os.environ.get(key) != "1" for key in THREAD_VARIABLES):
        raise FormalStateError("hmm_risk_numeric_environment_mismatch", "fixed version/thread contract differs")
    if not pools or any(pool["num_threads"] != 1 for pool in pools):
        raise FormalStateError("hmm_risk_numeric_environment_mismatch", "detected pools are not single-threaded")
    return {
        "versions": versions,
        "thread_variables": {key: "1" for key in THREAD_VARIABLES},
        "thread_pools": pools,
        "executable": sys.executable,
        "host": platform.node(),
    }


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"), parse_constant=lambda v: (_ for _ in ()).throw(ValueError(v)))
    if not isinstance(value, dict):
        raise FormalStateError("hmm_risk_formal_request_invalid", "JSON object required")
    return value


def write_once(path: Path, body: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(body) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    if canonical_json_bytes(read_json(path)) != canonical_json_bytes(body):
        raise FormalStateError("hmm_risk_formal_output_readback_failed", str(path))


def verify_hash(value: Mapping[str, Any]) -> None:
    if value.get("receipt_sha256") != canonical_sha256({k: v for k, v in value.items() if k != "receipt_sha256"}):
        raise FormalStateError("hmm_risk_formal_identity_mismatch", "canonical receipt differs")


def load_request(path: Path) -> dict[str, Any]:
    from backend.services.hmm_risk.rotation_l1_input_bundle import _industry_adapter, load_active_hmm_dataset_identity

    request = read_json(path)
    verify_hash(request)
    required = {
        "schema_version",
        "contracts",
        "source_identity",
        "industry_authority",
        "policy",
        "train_calendar",
        "validation_calendar",
        "sector_codes",
        "series",
        "receipt_sha256",
    }
    if set(request) != required or request["schema_version"] != VERSION or request["contracts"] != CONTRACTS:
        raise FormalStateError("hmm_risk_formal_request_invalid", "request fields/version differ")
    active = load_active_hmm_dataset_identity()
    # The current release is explicitly approved; no latest or old-release fallback.
    source = request["source_identity"]
    if (
        source["generation"] != ACTIVE_GENERATION
        or source["manifest_sha256"] != ACTIVE_MANIFEST
        or source["cutoff"] != "2026-08-31"
    ):
        raise FormalStateError("hmm_risk_formal_identity_mismatch", "request is not approved active v15")
    if active.get("generation") != ACTIVE_GENERATION:
        raise FormalStateError("hmm_risk_formal_identity_mismatch", "active profile generation changed")
    from backend.services.hmm_risk.rotation_l1_input_bundle import (
        _is_indirect_path,
        _require_active_direct_v2_profile,
    )

    root = Path(source["dataset_root"])
    if not root.is_absolute() or _is_indirect_path(root) or not root.is_dir():
        raise FormalStateError("hmm_risk_formal_identity_mismatch", "dataset root invalid")
    state = read_json(root / "direct_monthly_state.json")
    _, _, manifest = _require_active_direct_v2_profile(root=root, state=state, release_cutoff=active["cutoff"])
    if manifest.get("dataset_manifest_sha256") != ACTIVE_MANIFEST:
        raise FormalStateError("hmm_risk_formal_identity_mismatch", "active manifest differs")
    # Use the existing shared PIT reader and projections; no integer-index mapping.
    authority = request["industry_authority"]
    if authority["identity"]["bundle_hash"] != PIT_BUNDLE:
        raise FormalStateError("hmm_risk_formal_identity_mismatch", "PIT authority is not full-v3-security-identity")
    adapter = _industry_adapter(authority, forbidden_roots=(Path(__file__).resolve().parents[3],))
    from backend.services.hmm_risk.rotation_l1_input_bundle import _canonical_sector_codes

    l1, l2 = _canonical_sector_codes(adapter)
    if request["sector_codes"] != {"L1": list(l1), "L2": list(l2)}:
        raise FormalStateError("hmm_risk_model_restart_family_incomplete", "31/131 authority differs")
    policy = validate_c010_policy_manifest(request["policy"])
    if policy["schema_version"] != "hmm_risk_c010_feature_domain_policy_v2":
        raise FormalStateError("hmm_risk_c010_policy_identity_mismatch", "A5 policy required")
    if policy["dataset_manifest_hash"] != source["dataset_manifest_hash"]:
        raise FormalStateError("hmm_risk_c010_policy_identity_mismatch", "policy/request dataset identity differs")
    train_calendar = request["train_calendar"]
    if (
        len(train_calendar) != 601
        or canonical_sha256(train_calendar) != TRAIN_CALENDAR_HASH
        or any(not "2022-01-01" <= d <= "2024-06-30" for d in train_calendar)
    ):
        raise FormalStateError("hmm_risk_formal_request_invalid", "601-day train calendar differs")
    validation_calendar = request["validation_calendar"]
    if (
        len(validation_calendar) != 182
        or validation_calendar != sorted(set(validation_calendar))
        or validation_calendar[0] != "2024-07-01"
        or validation_calendar[-1] != "2025-03-31"
    ):
        raise FormalStateError("hmm_risk_formal_request_invalid", "182-day validation calendar differs")
    from backend.services.hmm_risk.rotation_l1_input_bundle import _load_qlib_calendar

    frozen_dates = [
        d.isoformat() for d in _load_qlib_calendar(root / "components/daily_bin_candidate/calendars/day.txt")
    ]
    if train_calendar != [d for d in frozen_dates if "2022-01-01" <= d <= "2024-06-30"] or validation_calendar != [
        d for d in frozen_dates if "2024-07-01" <= d <= "2025-03-31"
    ]:
        raise FormalStateError("hmm_risk_semantic_calendar_invalid", "request is not the complete frozen file calendar")
    expected_keys = {f"{family}:{level}" for family in FAMILIES for level in ("L1", "L2")}
    if set(request["series"]) != expected_keys:
        raise FormalStateError("hmm_risk_formal_request_invalid", "four family/level inputs required")
    for key, series in request["series"].items():
        family, level = key.split(":")
        features = list(BASE_FEATURES if family == FAMILIES[0] else ALL_CORE_FEATURES)
        if sorted(series) != request["sector_codes"][level]:
            raise FormalStateError("hmm_risk_formal_request_invalid", f"{key} sector set differs")
        for code, entry in series.items():
            if set(entry) != {"feature_names", "train_dates", "train_values", "validation", "source_receipt_sha256"}:
                raise FormalStateError("hmm_risk_formal_request_invalid", f"{key}/{code} fields differ")
            if entry["source_receipt_sha256"] != canonical_sha256(
                {k: v for k, v in entry.items() if k != "source_receipt_sha256"}
            ):
                raise FormalStateError("hmm_risk_formal_identity_mismatch", f"{key}/{code} source receipt differs")
            dates = entry["train_dates"]
            if (
                entry["feature_names"] != features
                or len(dates) < 120
                or dates != sorted(set(dates))
                or not set(dates) <= set(train_calendar)
            ):
                raise FormalStateError("hmm_risk_model_train_coverage_insufficient", f"{key}/{code}")
            array(entry["train_values"], (len(dates), len(features)), "training observations")
            validate_calendar_carrier(
                entry["validation"],
                dates=validation_calendar,
                feature_names=features,
                source_identity_sha256=canonical_sha256(source),
                source_receipt_sha256=policy["receipt_sha256"],
            )
    return request


def train_repeat(request: Mapping[str, Any]) -> dict[str, Any]:
    environment = numeric_environment()
    groups = {}
    attempts = 0
    for family in FAMILIES:
        for level in ("L1", "L2"):
            key = f"{family}:{level}"
            raw_series = request["series"][key]
            codes = request["sector_codes"][level]
            preprocess = preprocess_fit([np.asarray(raw_series[c]["train_values"]) for c in codes], family)
            candidates = []
            for seed in SEEDS:
                entries = {}
                for code in codes:
                    attempts += 1
                    source = raw_series[code]
                    values = preprocess_apply(np.asarray(source["train_values"]), preprocess)
                    try:
                        result = fit_entry(values, source["train_dates"], seed)
                    except Exception as exc:
                        # Failure is durable and ineligible; every declared entry is still attempted.
                        result = receipt(
                            {
                                "accepted": False,
                                "reasons": [getattr(exc, "reason_code", "hmm_risk_model_fit_failed")],
                                "exception_type": type(exc).__name__,
                                "message": str(exc),
                                "evidence": getattr(exc, "evidence", None),
                            }
                        )
                    entries[code] = result
                    print(
                        f"fit {attempts}/2592 {key} seed={seed} sector={code} accepted={result['accepted']}", flush=True
                    )
                candidates.append({"seed": seed, "entries": entries})
            groups[key] = {"preprocess": preprocess, "candidates": candidates}
    if attempts != 2592:
        raise FormalStateError("hmm_risk_model_restart_schedule_incomplete", "2592 fits required per process")
    return receipt(
        {
            "schema_version": VERSION,
            "request_sha256": request["receipt_sha256"],
            "numeric_environment": environment,
            "fit_attempts": attempts,
            "groups": groups,
            "selection_performed": False,
            "validation_accessed": False,
            "ready": False,
        }
    )


def finalize(request: Mapping[str, Any], first: Mapping[str, Any], second: Mapping[str, Any]) -> dict[str, Any]:
    for repeat in (first, second):
        verify_hash(repeat)
        if (
            repeat["request_sha256"] != request["receipt_sha256"]
            or repeat["fit_attempts"] != 2592
            or repeat["selection_performed"]
            or repeat["validation_accessed"]
        ):
            raise FormalStateError("hmm_risk_model_restart_schedule_incomplete", "repeat lineage differs")
    if canonical_json_bytes(first) != canonical_json_bytes(second):
        raise FormalStateError("hmm_risk_model_repeat_mismatch", "fresh processes are not bitwise equal")
    expected_groups = {f"{family}:{level}" for family in FAMILIES for level in ("L1", "L2")}
    if set(first["groups"]) != expected_groups:
        raise FormalStateError("hmm_risk_model_restart_schedule_incomplete", "four family/level groups required")
    selections = {}
    semantic = {}
    for key, group in first["groups"].items():
        level = key.split(":")[1]
        codes = request["sector_codes"][level]
        series = request["series"][key]
        expected_preprocess = preprocess_fit(
            [np.asarray(series[code]["train_values"]) for code in codes], key.split(":")[0]
        )
        if group["preprocess"] != expected_preprocess:
            raise FormalStateError("hmm_risk_model_receipt_invalid", "train-global preprocess differs")
        for candidate in group["candidates"]:
            if sorted(candidate["entries"]) != codes:
                raise FormalStateError("hmm_risk_model_restart_family_incomplete", "child sector set differs")
            for code in codes:
                entry = candidate["entries"][code]
                verify_hash(entry)
                if (
                    "initialization" in entry
                    and entry["initialization"]["kmeans_parameters"]["random_state"] != candidate["seed"]
                ):
                    raise FormalStateError("hmm_risk_model_receipt_invalid", "candidate seed differs")
                validate_fit_entry(
                    entry,
                    preprocess_apply(np.asarray(series[code]["train_values"]), expected_preprocess),
                    series[code]["train_dates"],
                )
        selection = select_restart(group["candidates"], request["sector_codes"][level])
        selections[key] = selection
        if not selection["accepted"]:
            continue
        selected = next(c for c in group["candidates"] if c["seed"] == selection["selected_seed"])
        semantic[key] = {}
        for code in request["sector_codes"][level]:
            fitted = selected["entries"][code]
            source = request["series"][key][code]["validation"]
            try:
                values = preprocess_apply(
                    array(
                        source["observation_values_f64"],
                        (len(source["observation_available_positions"]), fitted["feature_count"]),
                        "validation values",
                    ),
                    group["preprocess"],
                )
                arguments = dict(
                    carrier=source,
                    processed_values=values,
                    dates=request["validation_calendar"],
                    feature_names=request["series"][key][code]["feature_names"],
                    source_identity_sha256=canonical_sha256(request["source_identity"]),
                    source_receipt_sha256=request["policy"]["receipt_sha256"],
                    selected_identity={
                        "family": key.split(":")[0],
                        "level": level,
                        "sector": code,
                        "seed": selected["seed"],
                    },
                )
                model = restore_model(fitted["model"])
                semantic[key][code] = evaluate_calendar_evidence(
                    model,
                    **arguments,
                )
            except Exception as exc:
                known = getattr(exc, "evidence", None)
                assignment = known.get("assignment_status", "failed") if isinstance(known, dict) else "failed"
                semantic[key][code] = receipt(
                    {
                        "assignment_status": assignment,
                        "evidence_status": "failed",
                        "reasons": [getattr(exc, "reason_code", "hmm_risk_semantic_validation_failed")],
                        "exception_type": type(exc).__name__,
                        "message": str(exc),
                    }
                )
    accepted = (
        len(selections) == 4
        and all(s["accepted"] for s in selections.values())
        and len(semantic) == 4
        and all(
            s["assignment_status"] == s["evidence_status"] == "accepted"
            for entries in semantic.values()
            for s in entries.values()
        )
    )
    return receipt(
        {
            "schema_version": VERSION,
            "request_sha256": request["receipt_sha256"],
            "repeat_sha256": first["receipt_sha256"],
            "fresh_process_bitwise_equal": True,
            "fit_attempts": 5184,
            "selection": selections,
            "semantic": semantic,
            "d3_d6_accepted": accepted,
            "ready": False,
            "phase2_ready": False,
            "product_capability_promoted": False,
            "database_write": False,
            "runtime_action": False,
        }
    )


def validate_semantic_readback(
    final: Mapping[str, Any],
    request: Mapping[str, Any],
    groups: Mapping[str, Any],
) -> None:
    """Validate durable D6 results against selected parameters, with no fitting."""
    verify_hash(final)
    expected_keys = {key for key, selection in final["selection"].items() if selection["accepted"]}
    if set(final["semantic"]) != expected_keys:
        raise FormalStateError("hmm_risk_semantic_validation_availability_receipt_mismatch", "D6 group closure differs")
    for key in expected_keys:
        family, level = key.split(":")
        features = list(BASE_FEATURES if family == FAMILIES[0] else ALL_CORE_FEATURES)
        codes = request["sector_codes"][level]
        if sorted(final["semantic"][key]) != codes:
            raise FormalStateError(
                "hmm_risk_semantic_validation_availability_receipt_mismatch", "D6 sector closure differs"
            )
        group = groups[key]
        selected = next(c for c in group["candidates"] if c["seed"] == final["selection"][key]["selected_seed"])
        for code in codes:
            source = request["series"][key][code]["validation"]
            values = preprocess_apply(
                array(
                    source["observation_values_f64"],
                    (len(source["observation_available_positions"]), len(features)),
                    "validation compact values",
                ),
                group["preprocess"],
            )
            validate_calendar_evidence(
                final["semantic"][key][code],
                restore_model(selected["entries"][code]["model"]),
                carrier=source,
                processed_values=values,
                dates=request["validation_calendar"],
                feature_names=features,
                source_identity_sha256=canonical_sha256(request["source_identity"]),
                source_receipt_sha256=request["policy"]["receipt_sha256"],
                selected_identity={"family": family, "level": level, "sector": code, "seed": selected["seed"]},
            )


def run_two_processes(request_path: Path, output: Path, child_script: Path) -> Path:
    if output.exists():
        raise FormalStateError("hmm_risk_formal_output_collision", "output must be a new directory")
    output.mkdir(parents=True)
    env = {**os.environ, **{key: "1" for key in THREAD_VARIABLES}, "PYTHONPATH": str(child_script.resolve().parents[2])}
    try:
        request = load_request(request_path)
        repeat_paths = []
        for number in (1, 2):
            result_path = output / f"process_{number}.json"
            with (output / f"process_{number}.log").open("xb") as log:
                result = subprocess.run(
                    [
                        sys.executable,
                        str(child_script),
                        "child",
                        "--request",
                        str(request_path),
                        "--output",
                        str(result_path),
                    ],
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=False,
                )
            if result.returncode:
                raise FormalStateError("hmm_risk_formal_child_failed", f"process {number} exited {result.returncode}")
            repeat_paths.append(result_path)
        final = finalize(request, *(read_json(path) for path in repeat_paths))
        write_once(output / "acceptance.json", final)
        groups = read_json(repeat_paths[0])["groups"]
        validate_semantic_readback(read_json(output / "acceptance.json"), request, groups)
        if final["d3_d6_accepted"]:
            # Numerical/semantic acceptance is not predictive-product READY.
            selected_models = {}
            for key, selection in final["selection"].items():
                selected = next(c for c in groups[key]["candidates"] if c["seed"] == selection["selected_seed"])
                selected_models[key] = {
                    "seed": selected["seed"],
                    "preprocess": groups[key]["preprocess"],
                    "models": {
                        code: {
                            "model": entry["model"],
                            "entry_sha256": entry["receipt_sha256"],
                            "semantic": final["semantic"][key][code],
                        }
                        for code, entry in selected["entries"].items()
                    },
                }
            payload = {
                "acceptance": final,
                "request_sha256": request["receipt_sha256"],
                "selected_models": selected_models,
                "contracts": CONTRACTS,
                "source_identity": request["source_identity"],
                "industry_authority": request["industry_authority"],
                "policy": request["policy"],
                "ready": False,
                "phase2_ready": False,
            }
            write_once(output / "accepted_model_set.json", receipt(payload))
        return output / "acceptance.json"
    except Exception as exc:
        write_once(
            output / "parent.failure.json",
            receipt(
                {
                    "status": "failed",
                    "reason": getattr(exc, "reason_code", "hmm_risk_formal_execution_failed"),
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                    "request_file_sha256": hashlib.sha256(request_path.read_bytes()).hexdigest()
                    if request_path.is_file()
                    else None,
                    "database_write": False,
                    "runtime_action": False,
                    "ready": False,
                }
            ),
        )
        raise
