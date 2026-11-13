"""Freeze/predict/evaluate the current LSTM specialist on one consumed historical axis."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
from scripts import advisory_lstm_rank_value_review as files  # noqa: E402
from backend.services.advisory_model_first import lstm_frozen_specialist_stress_v1 as core  # noqa: E402
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage, read_stage  # noqa: E402
from backend.services.advisory_model_first.lstm_spo_shadow_v1 import publish, read_packet  # noqa: E402

OUTPUT = Path("X:/AIstock_temp/ADVISORY/lstm-frozen-specialist-stress-20261102")
P18 = Path("X:/AIstock_temp/ADVISORY/p18-lstm-complementarity-20261027")
V2 = Path("X:/AIstock_temp/ADVISORY/lstm-business-review-20261101/study/preregistered/protocol.json")
P18_TERMINAL = "cc368bdac2fce51acb93c74ac9639654b7b972047ebb6c88e697dd6dc7213120"
SOURCE_FILES = (
    "scripts/advisory_lstm_frozen_specialist_stress.py",
    "backend/services/advisory_model_first/lstm_frozen_specialist_stress_v1.py",
    "backend/services/advisory_model_first/lstm_complementarity_v1.py",
    "backend/services/advisory_model_first/lstm_historical_engineering_replay.py",
    "backend/services/advisory_model_first/lstm_top10_business_review_v1.py",
    "backend/services/advisory_model_first/peer_residual_top10_5td_v1.py",
    "backend/services/advisory_model_first/peer_lead_lag_top10_5td_v1.py",
    "backend/services/advisory_model_first/native_anchored_top10_5td_v1.py",
    "backend/services/advisory_model_first/multiperiod_top10_5td_study_v1.py",
    "backend/services/advisory_model_first/multiperiod_top10_5td_inputs_v1.py",
    "backend/services/advisory_model_first/prequential_top10_5td_study_v1.py",
    *files.SOURCE_FILES,
)


def closure(committed=False):
    result = {}
    for name in dict.fromkeys(SOURCE_FILES):
        result[name] = files.code_sha((ROOT / name).read_bytes())
        if committed and result[name] != files.code_sha(subprocess.check_output(["git", "show", f"HEAD:{name}"], cwd=ROOT)):
            raise ValueError(f"commit producer source before freezing: {name}")
    return result


def output_root(path):
    path = Path(path)
    if (not path.is_absolute() or path.resolve() != path.absolute()
            or not path.is_relative_to(OUTPUT) or path == OUTPUT):
        raise ValueError("output must be an isolated child of the task evidence root")
    return path


def file_ref(path, role):
    return dict(path=str(path), role=role, sha256=files.file_sha(path), size_bytes=path.stat().st_size)


def bound_root(root, plan):
    root = output_root(root)
    if str(root) != plan["output_root"]:
        raise ValueError("prediction/evaluation output differs from the frozen protocol")
    return root


def refs():
    result = {name: file_ref(path, role) for name, (path, role) in files.source_spec().items()}
    additions = dict(p18_protocol=(P18 / "protocol.json", "OLD_PROTOCOL"),
        p18_axis=(P18 / "metadata/lstm_target_axis.json", "ORIGINAL_POPULATION"),
        p18_inputs=(P18 / "study/prepared/target_inputs.parquet", "FROZEN_INPUTS"),
        p18_predictions=(P18 / "study/trained/predictions.parquet", "OLD_SHARED_PREDICTIONS"),
        p18_finance=(P18 / "study/evaluated/target_finance.parquet", "ALREADY_CONSUMED_FROZEN_VALUATION"),
        p18_panel=(P18 / "study/evaluated/target_panel.parquet", "OLD_SHARED_PANEL"),
        p18_report=(P18 / "study/evaluated/report.json", "OLD_SHARED_REPORT"),
        v2_protocol=(V2, "PROTECTED_FORMAL_PROTOCOL"))
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        additions["p18_manifest_" + stage] = (P18 / f"study/{stage}/manifest.json", "OLD_STAGE_CHAIN")
    result.update({name: file_ref(path, role) for name, (path, role) in additions.items()})
    return result


def check_refs(sources):
    for name, item in sources.items():
        path = Path(item["path"])
        if path.stat().st_size != item["size_bytes"] or files.file_sha(path) != item["sha256"]:
            raise ValueError(f"frozen source changed: {name}")


def old_chain(sources):
    old = files.load_json(sources["p18_protocol"]["path"])
    if core.spo.sha(old) != "5e8813d19b8f16abbca0a98993fbc081dccbd18d2feb366c2358bcce9458a4ac":
        raise ValueError("original P18 protocol identity differs")
    parent = None
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        parent = read_stage(P18 / "study" / stage, stage=stage,
            plan_sha256=core.spo.sha(old), parent_sha256=parent)["stage_sha256"]
    if parent != P18_TERMINAL:
        raise ValueError("original P18 terminal chain differs")
    # Every consumed file must be the file bound by that original stage manifest.
    for key, stage, name in (("p18_inputs", "prepared", "target_inputs.parquet"),
                            ("p18_predictions", "trained", "predictions.parquet"),
                            ("p18_finance", "evaluated", "target_finance.parquet"),
                            ("p18_panel", "evaluated", "target_panel.parquet"),
                            ("p18_report", "evaluated", "report.json")):
        expected = files.load_json(P18 / f"study/{stage}/manifest.json")["files"][name]
        if any(sources[key][field] != expected[field] for field in ("sha256", "size_bytes")):
            raise ValueError("consumed P18 file was relabeled")
    if sources["p18_axis"]["sha256"] != old["sources"]["axis"]["sha256"]:
        raise ValueError("original P18 axis differs")


def verify(plan):
    if plan["contract"] != core.CONTRACT or plan["source_closure"] != closure():
        raise ValueError("frozen research contract/source changed")
    check_refs(plan["sources"])
    files.verify_archive(dict(sources=plan["sources"]))
    old_chain(plan["sources"])
    if any(plan["sources"][key]["sha256"] != core.CONTRACT["model_sha256"] for key in ("model3", "live_model")):
        raise ValueError("model is not the current frozen P19 fold3")
    v2 = files.load_json(plan["sources"]["v2_protocol"]["path"])
    if core.spo.sha(v2) != "0e9c51566efaebb59df2d5f9d0482b78f6a9a29af405921e47d772baa738e9d3":
        raise ValueError("protected V2 protocol identity differs")
    for name, expected in v2["source_closure"].items():
        if files.code_sha((ROOT / name).read_bytes()) != expected:
            raise ValueError("protected V2 source closure changed")


def freeze(root):
    root = output_root(root)
    path = root / "protocol.json"
    if path.exists():
        plan = read_packet(path)
        if plan["output_root"] != str(root):
            raise ValueError("frozen output path differs")
        verify(plan)
        return plan
    plan = dict(contract=core.CONTRACT, sources=refs(), source_closure=closure(committed=True),
        producer_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        registered_at=datetime.now(timezone.utc).isoformat(), output_root=str(root),
        prior_shared_negative_result_known=True, max_new_fits=0, max_new_candidates=0,
        formal_v2_unchanged=True)
    verify(plan)
    return publish(path, plan)


def predicted(root, plan):
    root = bound_root(root, plan)
    verify(plan)
    if (root / "trained").exists():
        return read_stage(root / "trained", stage="trained", plan_sha256=plan["sha256"], parent_sha256=None)
    inputs = pd.read_parquet(plan["sources"]["p18_inputs"]["path"])
    axis = pd.DataFrame(files.load_json(plan["sources"]["p18_axis"]["path"]))
    packet = files.load_json(plan["sources"]["model3"]["path"])
    prediction = core.predict(inputs, packet, axis)
    verify(plan)
    publish_stage(study_root=root, stage="trained", plan_sha256=plan["sha256"], parent_sha256=None,
        artifacts={"predictions.parquet": files.parquet_bytes(prediction), "receipt.json": files.json_bytes(dict(
            operation="FROZEN_WEIGHT_INFERENCE_ZERO_FIT", rows=len(prediction), days=prediction[core.DAY].nunique(), financial_payload_decoded=False, new_fits=0))})
    return read_stage(root / "trained", stage="trained", plan_sha256=plan["sha256"], parent_sha256=None)


def financial_inputs(plan):
    """Called only after read_stage validates the immutable prediction stage."""
    sources = plan["sources"]
    return (pd.read_parquet(sources["p18_finance"]["path"]),
            pd.read_parquet(sources["p18_predictions"]["path"]),
            pd.read_parquet(sources["p18_panel"]["path"]), files.load_json(sources["p18_report"]["path"]))


def evaluated(root, plan):
    root = bound_root(root, plan)
    verify(plan)
    parent = read_stage(root / "trained", stage="trained", plan_sha256=plan["sha256"], parent_sha256=None)
    if (root / "evaluated").exists():
        stage = read_stage(root / "evaluated", stage="evaluated", plan_sha256=plan["sha256"], parent_sha256=parent["stage_sha256"])
        return dict(status="STRESS_REUSED", stage_sha256=stage["stage_sha256"], new_fits=0,
            decision=files.load_json(root / "evaluated/report.json")["decision"])
    predictions = pd.read_parquet(root / "trained/predictions.parquet")
    axis = pd.DataFrame(files.load_json(plan["sources"]["p18_axis"]["path"]))
    finance, shared, panel, prior_report = financial_inputs(plan)
    report, candidate_panel, common_panel = core.evaluate(predictions, finance, shared, panel, prior_report, axis)
    verify(plan)
    publish_stage(study_root=root, stage="evaluated", plan_sha256=plan["sha256"], parent_sha256=parent["stage_sha256"],
        artifacts={"report.json": files.json_bytes(report), "panel.parquet": files.parquet_bytes(candidate_panel),
            "three_arm_panel.parquet": files.parquet_bytes(common_panel),
            "read_receipt.json": files.json_bytes(dict(predictions_frozen_before_financial_decode=True,
                prediction_stage_sha256=parent["stage_sha256"], financial_source_sha256=plan["sources"]["p18_finance"]["sha256"],
                source_hashes_unchanged=True, v2_unchanged=True, new_fits=0, database_writes=0))})
    stage = read_stage(root / "evaluated", stage="evaluated", plan_sha256=plan["sha256"], parent_sha256=parent["stage_sha256"])
    return dict(status="STRESS_COMPLETE", stage_sha256=stage["stage_sha256"], report=report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["freeze", "predict", "evaluate", "run"])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = output_root(args.output)
    plan = freeze(root)
    if args.command == "freeze":
        result = dict(status="PROTOCOL_FROZEN", sha256=plan["sha256"], new_fits=0)
    elif args.command == "predict":
        result = dict(status="PREDICTED", **predicted(root, plan))
    else:
        if args.command == "run":
            predicted(root, plan)
        result = evaluated(root, plan)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
