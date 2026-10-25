"""Bounded research CLI using existing QE compose, registry and submission services.

No service reload, model fit, dataset write, or retry. The evidence directory
contains immutable prepared inputs and one intent per formal account.
"""
from __future__ import annotations

import argparse
import asyncio
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import re
import shlex
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
NODE = "rdagent-node1"
SEEDS = (42, 2024, 2026)


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def save_new(path, value):
    with Path(path).open("x", encoding="utf-8") as out:
        json.dump(value, out, ensure_ascii=False, indent=2, default=str, allow_nan=False)


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":"), default=str).encode()).hexdigest()


def compose(evidence):
    from backend.services.quantevolver.config_composer import ConfigComposer
    from backend.services.quantevolver.experiment_config_builders import build_config_from_custom_evo_loop
    from backend.services.quantevolver.executors.backtest import _strip_prediction_replay_factor_preparation
    from backend.services.quantevolver.qe_run_registry import build_qe_run_registration
    from backend.services.quantevolver.qe_active_dataset_profile import load_active_qe_profile, resolve_and_apply_active_qe_dataset
    from backend.services.quantevolver.l2_risk_consumer import bind_execution_metadata
    import yaml

    approval = read(evidence / "user_approval_receipt.json")
    if approval["status"] != "APPROVED" or approval["contract"] != "QE-L2-RISK-ENTRY-GATE-D1..D7":
        raise ValueError("QE_L2_APPROVAL_MISSING")
    shared = read(evidence / "shared_prediction_receipt.json")
    reference = read(evidence / "baseline_recipe_reference.json")
    output = evidence / "composed"
    output.mkdir(exist_ok=True)
    if any(output.iterdir()):
        raise ValueError("QE_L2_COMPOSE_DIRECTORY_NOT_EMPTY")
    cc = ConfigComposer()
    profile = load_active_qe_profile()
    if profile is None or profile.generation != "20260930-monthly-v2-unified-correction-4ab0b48642c3":
        raise ValueError("QE_L2_APPROVED_PROFILE_IDENTITY_CHANGED")
    plans, confs = [], {}
    for index, asset in enumerate(shared["assets"]):
        seed = asset["seed"]
        if seed != SEEDS[index]:
            raise ValueError("QE_L2_SEEDS_CHANGED")
        base = deepcopy(reference["loops"][index])
        base["data_split"].update(test_start="2026-03-31", test_end="2026-09-30", backtest_end="2026-09-30")
        cfg = build_config_from_custom_evo_loop(base, reference)
        params = cfg.build_custom_params()
        params.update(random_seed=seed)
        split, params, _ = resolve_and_apply_active_qe_dataset(
            node_id=NODE, data_split=dict(cfg.data_split), universe_selection={"mode": "stock_universe", "pool_ids": []},
            custom_params=params, label_horizon=20, profile=profile)
        observed_binding = deepcopy(params["_qe_direct_v2_dataset_binding"])
        approved_binding = deepcopy(shared["dataset_binding"])
        # The existing coverage service signs the requested run window separately;
        # physical provider/PIT/component identities must remain identical.
        for binding in (observed_binding, approved_binding):
            binding["selection_pins"].pop("coverage_receipt_sha256", None)
        if observed_binding != approved_binding or split != cfg.data_split:
            raise ValueError("QE_L2_COMPOSE_DATASET_IDENTITY_CHANGED")
        params["_qe_active_dataset_summary"] = {
            "generation": "20260930-monthly-v2-unified-correction-4ab0b48642c3",
            "release_id": "qe_hmm_full_v2_20260930", "cutoff": "2026-09-30"}
        # Original model/processor configuration is provenance only. --pred-backtest
        # strips every processor and never loads or fits the model.
        manifest = read(evidence / f"consumer_manifest_seed{seed}.json")
        for arm in ("B", "L"):
            task = f"qe_l2_entry_20261023_s{seed}_{arm.lower()}"
            expid = hashlib.sha256(task.encode()).hexdigest()[:8]
            runtime = deepcopy(params)
            runtime.update(qe_l2_study_manifest=manifest, qe_l2_risk_enabled=(arm == "L"),
                           archive_policy="AUTO", archive_reason="approved_l2_entry_gate_controlled_study",
                           execution_node_id=NODE)
            runtime["qe_l2_study_provenance"] = {
                "arm": arm, "seed": seed, "approval": approval, "shared_prediction": asset,
                "shared_prediction_receipt_sha256": hashlib.sha256((evidence / "shared_prediction_receipt.json").read_bytes()).hexdigest(),
                "mode": "external_frozen_prediction_replay", "model_fits": 0, "processor_fits": 0,
                "non_as_known_taxonomy": True, "untouched_holdout": False,
                "formal_account_limit": 6, "base_model_task": reference["task_id"]}
            # Do not feed study metadata or stale research flags into model kwargs.
            compose_params = deepcopy(params)
            compose_params.update(qe_l2_study_manifest=manifest, qe_l2_risk_enabled=(arm == "L"))
            result = cc.compose_experiment_in_memory(
                factor_names=cfg.factor_names, model_id=cfg.model_id, strategy_id=cfg.strategy_id,
                data_split=dict(cfg.data_split), custom_params=compose_params,
                experiment_name=task + "/Loop1", skip_db_save=True,
                execution_algo=cfg.execution_algo, execution_algo_params=cfg.execution_algo_params,
                strategy_params=cfg.build_strategy_params(), node_id=NODE,
                task_id=task, loop_index=1)
            files = result["experiment_files"]
            files.pop("prepare_factors.py", None)
            # Preserve exact frozen input identity while referring truthfully to a
            # prepared asset, not pretending it is an already completed QE loop.
            files["qe_prediction_replay_source_ref.json"] = json.dumps({
                "schema_version": "qe_prediction_replay_source_ref_v1",
                "source_kind": "approved_frozen_af_inference_asset",
                "source_asset": asset, "sha256": asset["prediction_sha256"],
                "size_bytes": 0})  # Replaced with verified CAS size by prepare_remote.
            command = _strip_prediction_replay_factor_preparation(result["wsl_command"])
            command, count = re.subn(r"(python\s+qrun_limit_minute\.py\s+\S+\.ya?ml)",
                r"\1 --pred-backtest frozen_prediction.pkl", command)
            if count != 1 or "--train-only" in command or "--backtest-only" in command:
                raise ValueError("QE_L2_REPLAY_COMMAND_INVALID")
            source = shlex.quote(asset["cas_path"])
            command = command.replace("python qe_l2_risk_consumer.py", f"cp {source} frozen_prediction.pkl && python qe_l2_risk_consumer.py", 1)
            conf = yaml.safe_load(files["conf.yaml"])
            runtime = bind_execution_metadata(runtime, conf)
            runtime["_qe_run_registration"] = build_qe_run_registration(
                run_kind="single", custom_params=runtime, consumer_id="qe_mainline",
                source_type="agent", created_by_name="Codex QE approved L2 study", purpose="research",
                node_id=NODE, model_id=cfg.model_id, strategy_id=cfg.strategy_id,
                factor_names=cfg.factor_names, data_split=cfg.data_split, task_id=task, loop_index=1)
            confs[seed, arm] = conf
            folder = output / task
            folder.mkdir()
            for name, content in files.items():
                target = folder / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content, encoding="utf-8")
            plan = {"experiment_id": expid, "task_id": task, "loop_index": 1, "node_id": NODE,
                    "seed": seed, "arm": arm, "factor_names": cfg.factor_names,
                    "model_id": cfg.model_id, "strategy_id": cfg.strategy_id,
                    "data_split": cfg.data_split, "custom_params": runtime,
                    "workspace_path": result["wsl_workdir"], "wsl_command": command,
                    "files": sorted(files), "prediction_sha256": asset["prediction_sha256"],
                    "status": "COMPOSED_PENDING_REMOTE_PREFLIGHT"}
            save_new(folder / "study_plan.json", plan)
            plans.append({"task_id": task, "experiment_id": expid, "seed": seed, "arm": arm})
        baseline, treatment = deepcopy(confs[seed, "B"]), deepcopy(confs[seed, "L"])
        strategy = treatment["port_analysis_config"]["strategy"]
        strategy["class"] = baseline["port_analysis_config"]["strategy"]["class"]
        strategy["module_path"] = baseline["port_analysis_config"]["strategy"]["module_path"]
        strategy["kwargs"].pop("qe_l2_risk_enabled")
        strategy["kwargs"].pop("qe_l2_manifest_file")
        if baseline != treatment:
            raise ValueError(f"QE_L2_UNAPPROVED_ARM_CONFIG_DIFFERENCE: seed={seed}")
    save_new(output / "study_manifest.json", {"contract": approval["contract"], "plans": plans,
              "status": "COMPOSED_6_ACCOUNTS_NOT_SUBMITTED", "arm_diff_check": "PASS"})
    print(json.dumps({"status": "COMPOSED", "plans": plans}, ensure_ascii=False))


async def submit_one(evidence, task):
    from backend.services.quantevolver.qe_run_registry import QERunRegistry
    from backend.services.quantevolver.qe_active_execution_capacity import (
        QEExecutionSourceClaimFactory, QEWorkspaceSubmissionCoordinator,
        QEWorkspaceSubmissionPayload, QEWorkspaceSubmissionSource, qe_submission_owner_id)
    from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient
    from backend.services.quantevolver.callback_urls import build_aistock_callback_url
    from backend.services.quantevolver.config_composer import ConfigComposer
    from backend.services.quantevolver.l2_risk_consumer import bind_execution_metadata
    import yaml

    if (evidence / "study_stop_receipt.json").exists():
        raise ValueError("QE_L2_STUDY_STOPPED_READ_RECEIPT_NO_AUTOMATIC_RESUME")
    folder = evidence / "composed" / task
    plan = read(folder / "study_plan.json")
    preflight = read(folder / "remote_preflight.json")
    if preflight["status"] != "PASS" or preflight["plan_sha256"] != digest(plan):
        raise ValueError("QE_L2_REMOTE_PREFLIGHT_MISSING_OR_STALE")
    files = {name: (folder / name).read_text(encoding="utf-8") for name in plan["files"]}
    if {n: hashlib.sha256(c.encode()).hexdigest() for n, c in files.items()} != preflight["file_hashes"]:
        raise ValueError("QE_L2_SOURCE_CHANGED_AFTER_PREFLIGHT")
    expected_params = bind_execution_metadata(plan["custom_params"], yaml.safe_load(files["conf.yaml"]))
    if expected_params != plan["custom_params"]:
        raise ValueError("QE_L2_REGISTRATION_RUNTIME_CONTRACT_MISSING_REVIEW_FROZEN_PLAN")
    intent = {"task_id": task, "experiment_id": plan["experiment_id"], "plan_sha256": digest(plan),
              "file_hashes": preflight["file_hashes"]}
    if ConfigComposer()._get_experiment_record(plan["experiment_id"]) is not None:
        raise ValueError("QE_L2_EXPERIMENT_ALREADY_EXISTS_READBACK_ONLY_NO_REPOST")
    capacity = QEWorkspaceSubmissionCoordinator().observe_capacity(
        node_id=NODE, requested_node_capacity=2, source_kind="qe_experiment",
        source_execution_id=plan["experiment_id"], qe_task_id=task, qe_loop_id="Loop1",
        submission_intent_hash=digest(intent), consumer_id="qe_mainline", backtest_only=True)
    if not capacity.available:
        raise RuntimeError("QE_L2_CAPACITY_UNAVAILABLE_BEFORE_RESERVATION")
    save_new(folder / "submission_intent.json", intent)
    # Existing business services own all reservation/state writes. No direct SQL.
    registry = QERunRegistry()
    registry.reserve_single(experiment_id=plan["experiment_id"], experiment_name=task,
        workspace_path=plan["workspace_path"], factor_names=plan["factor_names"],
        model_id=plan["model_id"], strategy_id=plan["strategy_id"], data_split=plan["data_split"],
        custom_params=plan["custom_params"], evolution_goal="QE-L2-RISK-ENTRY-GATE-D1..D7 approved controlled study")
    claim, _ = QEExecutionSourceClaimFactory.experiment(
        experiment_id=plan["experiment_id"], node_id=NODE, qe_task_id=task, qe_loop_id="Loop1")

    def reject_capacity_wait(*args):
        # A generic single-experiment queue resumes FULL_TRAIN in the resident
        # backend. This fresh-process external replay must never enter it.
        raise RuntimeError("QE_L2_CAPACITY_UNAVAILABLE_NO_AUTOMATIC_QUEUE_OR_RETRY")

    source = QEWorkspaceSubmissionSource(source_kind="qe_experiment", source_execution_id=plan["experiment_id"],
        node_id=NODE, submission_intent_hash=digest(intent), owner_id=qe_submission_owner_id(),
        claim_source=claim, record_waiting_capacity=reject_capacity_wait, requested_node_capacity=2,
        backtest_only=True, consumer_id="qe_mainline")
    node = ConfigComposer()._fetch_workspace_config(NODE)
    callback = build_aistock_callback_url(endpoint_path="/api/v1/quantevolver/webhook/loop-completed",
        node_id=NODE, node_callback_url=node.get("callback_url"))
    async with QEWorkspaceClient.for_node(NODE) as client:
        outcome = await QEWorkspaceSubmissionCoordinator().submit(client=client, source=source,
            payload=QEWorkspaceSubmissionPayload(task_id=task, loop_index=1,
                config={"source": "qe_l2_controlled_study_v1", "study_plan": plan,
                        "model_params": plan["custom_params"], "factor_list": plan["factor_names"],
                        "model_id": plan["model_id"], "strategy_id": plan["strategy_id"],
                        "data_split": plan["data_split"]},
                experiment_files=files, wsl_command=plan["wsl_command"], callback_url=callback))
    save_new(folder / "submission_receipt.json", asdict(outcome))
    print(json.dumps({"task_id": task, "experiment_id": plan["experiment_id"], "status": outcome.state}))
    if not outcome.submitted:
        raise RuntimeError("QE_L2_NOT_SUBMITTED_INSPECT_RECEIPT_DO_NOT_REPOST")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["compose", "submit"])
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--task")
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    if args.env_file is not None:
        from dotenv import load_dotenv

        load_dotenv(args.env_file, override=False)
    if args.action == "compose":
        compose(args.evidence)
    else:
        manifest = read(args.evidence / "composed/study_manifest.json")
        if args.task not in {p["task_id"] for p in manifest["plans"]}:
            raise ValueError("QE_L2_TASK_OUT_OF_SCOPE")
        asyncio.run(submit_one(args.evidence, args.task))


if __name__ == "__main__":
    main()
