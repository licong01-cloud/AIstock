"""Build causal reference assets and one bounded five-arm replay plan. No POSTs."""

from copy import deepcopy
import argparse
import json
from pathlib import Path

from qe_l2_continuous_admission import CONTRACT, validate_admission
from qe_l2_continuous_consumer import VERSION, finite, require
from qe_l2_risk_admission import canonical_sha


def baseline_contract(loop):
    fields = [
        "factor_keys",
        "model_id",
        "model_params",
        "strategy_id",
        "strategy_params",
        "execution_algo",
        "execution_algo_params",
        "data_split",
        "label_horizon",
        "universe_selection",
        "suspend_filter_strict",
        "filter_suspended_on_signal",
        "disable_alpha158",
        "sector_blacklist",
        "unfilled_handler",
        "unfilled_handler_params",
    ]
    require(all(key in loop for key in fields), "QE_L2_CONTINUOUS_BASE_CONTRACT_INCOMPLETE")
    value = {key: deepcopy(loop[key]) for key in fields}
    for name in ["seed", "random_seed", "random_state"]:
        value["model_params"].pop(name, None)
    value["model_params"].pop("qe_l2_continuous_manifest", None)
    return canonical_sha(value)


def exposure_reference(actions, manifest):
    require(manifest["arm"] in ("M0", "M1"), "QE_L2_CONTINUOUS_REFERENCE_SOURCE_INVALID")
    rows = []
    seen = set()
    for action in actions:
        require(
            action["arm"] == manifest["arm"]
            and action["seed"] == manifest["seed"]
            and action["base_contract_sha256"] == manifest["base_contract_sha256"]
            and action["trade_date"] not in seen,
            "QE_L2_CONTINUOUS_REFERENCE_SOURCE_INVALID",
        )
        target = finite(action["target_exposure"], "target_exposure", maximum=1)
        require(
            abs(sum(action["target_weights"].values()) - target) < 1e-10, "QE_L2_CONTINUOUS_REFERENCE_TARGET_MISMATCH"
        )
        rows.append({k: action[k] for k in ["trade_date", "as_of_date", "target_exposure"]})
        seen.add(action["trade_date"])
    require(len(rows) == 125, "QE_L2_CONTINUOUS_REFERENCE_GRID")
    return {
        "schema_version": VERSION + "_exposure",
        "contract": CONTRACT,
        "source_arm": manifest["arm"],
        "seed": manifest["seed"],
        "AF_prediction_sha256": manifest["assets"]["prediction"]["sha256"],
        "dataset_identity_sha256": canonical_sha(manifest["release_identity"]),
        "source_risk_sha256": manifest["assets"][manifest["arm"]]["sha256"],
        "base_contract_sha256": manifest["base_contract_sha256"],
        "rows": sorted(rows, key=lambda x: x["trade_date"]),
    }


def build_plan(base_loops, manifests, references=None):
    """Return B/M first, then X after real target tapes; never synthesize assets."""
    require(set(base_loops) == {42, 2024, 2026} and set(manifests) == set(base_loops), "QE_L2_CONTINUOUS_SEEDS_INVALID")
    stages = {"models": [], "references": []}
    shared = None
    for seed in (42, 2024, 2026):
        m = manifests[seed]
        validate_admission(m)
        require(m["seed"] == seed, "QE_L2_CONTINUOUS_SEED_IDENTITY")
        signature = (
            m["base_contract_sha256"],
            canonical_sha(m["release_identity"]),
            canonical_sha(m["models"]),
            m["assets"]["M0"]["sha256"],
            m["assets"]["M1"]["sha256"],
        )
        require(shared is None or signature == shared, "QE_L2_CONTINUOUS_BATCH_IDENTITY")
        shared = signature
        source = base_loops[seed]
        require(baseline_contract(source) == m["base_contract_sha256"], "QE_L2_CONTINUOUS_BASE_CONTRACT_MISMATCH")
        require(
            source.get("prediction_replay") is True
            and source.get("prediction_source_sha256") == m["assets"]["prediction"]["sha256"]
            and source["runtime_flags"]["random_seed"] == seed,
            "QE_L2_CONTINUOUS_AF_REPLAY_REQUIRED",
        )
        require(
            source["data_split"]["test_start"] == "2026-03-31"
            and source["data_split"]["test_end"] == "2026-09-30"
            and source["data_split"]["backtest_end"] == "2026-09-30",
            "QE_L2_CONTINUOUS_BASE_WINDOW_INVALID",
        )
        require(
            not any(source.get(k) for k in ["enable_sector_hmm", "backtest_only"])
            and source.get("feature_processing") is None,
            "QE_L2_CONTINUOUS_BASE_CONTRACT_INVALID",
        )
        for arm in ["B", "M0", "M1", "X0", "X1"]:
            group = "references" if arm.startswith("X") else "models"
            loop = deepcopy(source)
            manifest = deepcopy(m)
            manifest["arm"] = arm
            if group == "references":
                if references is None:
                    continue
                require((seed, arm) in references, "QE_L2_CONTINUOUS_REFERENCE_MISSING")
                manifest["assets"]["reference"] = deepcopy(references[seed, arm])
            loop["loop_index"] = (9 if group == "references" else 0) + len(stages[group]) + 1
            loop["label"] = f"L2 continuous {arm} seed{seed}"
            loop["model_params"]["qe_l2_continuous_manifest"] = manifest
            loop["runtime_flags"].update(qe_l2_continuous_arm=arm, research_feedback=True, not_untouched_holdout=True)
            stages[group].append(loop)
    return {
        "contract": CONTRACT,
        "new_fits": 0,
        "max_accounts": 15,
        "automatic_promotion": False,
        "node_allocation": "explicit",
        "reference_delivery": "Append six reference Loops to the existing nine-Loop task; do not create a duplicate task.",
        "stage_dependencies": {"references": "models complete, target tapes verified, archived"},
        "stages": stages,
        "status": "READY_WITH_REAL_REFERENCES"
        if references is not None
        else "MODEL_STAGE_READY_REFERENCES_REQUIRE_TARGET_TAPES",
    }


def main():
    p = argparse.ArgumentParser()
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument("--request")
    source.add_argument("--reference-actions")
    p.add_argument("--source-manifest")
    p.add_argument("--references", help="JSON {seed:{X0:asset_ref,X1:asset_ref}} for appending actual reference Loops")
    p.add_argument("--output", required=True)
    a = p.parse_args()
    if a.reference_actions:
        if not a.source_manifest:
            p.error("--source-manifest is required with --reference-actions")
        manifest = json.loads(Path(a.source_manifest).read_text(encoding="utf-8"))
        validate_admission(manifest)
        actions = [json.loads(line) for line in Path(a.reference_actions).read_text(encoding="utf-8").splitlines()]
        result = exposure_reference(actions, manifest)
    else:
        request = json.loads(Path(a.request).read_text(encoding="utf-8"))
        references = None
        if a.references:
            raw = json.loads(Path(a.references).read_text(encoding="utf-8"))
            references = {(int(seed), arm): ref for seed, arms in raw.items() for arm, ref in arms.items()}
        result = build_plan(
            {int(k): v for k, v in request["base_loops"].items()},
            {int(k): v for k, v in request["manifests"].items()},
            references,
        )
    with Path(a.output).open("x", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, allow_nan=False)


if __name__ == "__main__":
    main()
