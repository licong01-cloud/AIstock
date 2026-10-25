"""Executable Advisory context prepare: pinned files only, no DB/outcomes/run."""
import argparse
import json
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_context_consumer_v1 import (
    _fail, _pinned, build_economic_context_rows_v1, context_roster_v1, load_economic_context_source_v1,
)
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1, EconomicEntryStudyPlanV1
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _authorize, _verify_reference, read_stage
from backend.services.advisory_model_first.policy_contracts import FrozenAdvisoryPolicyDatasetRequestV1
from backend.services.advisory_model_first.research_control import research_policy_identity
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def prepare_economic_context_v1(*, profile_path, profile_sha256, prepared_manifest_path, prepared_manifest_sha256,
                                universe_selection, authority_root=None):
    """Read only keys/identities; shared prepared outputs and policies stay intact."""
    path = _pinned(prepared_manifest_path, prepared_manifest_sha256)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if (path.name != "manifest.json" or path.parent.name != "prepared"
            or manifest.get("schema_version") != "economic_entry_stage_v1" or manifest.get("stage") != "prepared"
            or sha({key: value for key, value in manifest.items() if key != "stage_sha256"}) != manifest.get("stage_sha256")):
        _fail("context original prepared manifest is inconsistent")
    plan_path = path.parent.parent/"preregistered/plan.json"
    plan = EconomicEntryStudyPlanV1.model_validate_json(plan_path.read_text(encoding="utf-8"))
    if plan.plan_sha256 != manifest["plan_sha256"]:
        _fail("context parent plan differs from prepared inputs")
    registered = read_stage(plan_path.parent, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    if registered["stage_sha256"] != manifest["parent_sha256"]:
        _fail("context prepared parent chain differs")
    _authorize(plan)  # Before candidate/context reads; never consume a sealed window.
    pins = {path: prepared_manifest_sha256, plan_path: registered["files"]["plan.json"]["sha256"]}
    for name in ("features.parquet", "identity.json", "frozen_request.json", "frozen_rankings.parquet"):
        descriptor = manifest.get("files", {}).get(name, {})
        file = _pinned(path.parent/name, descriptor.get("sha256"))
        if file.stat().st_size != descriptor.get("size_bytes"):
            _fail("context original input size differs")
        pins[file] = descriptor["sha256"]
    identity = EconomicEntryInputIdentityV1.model_validate_json((path.parent/"identity.json").read_text(encoding="utf-8"))
    request = FrozenAdvisoryPolicyDatasetRequestV1.model_validate_json((path.parent/"frozen_request.json").read_text(encoding="utf-8"))
    fields = ("program_id", "binding_version_id", "package_id", "manifest_sha256", "request_id", "request_sha256",
              "selection_runtime_semantics_hash", "shadow_policy_sha256", "cost_policy_sha256")
    if any(getattr(identity, field) != getattr(request, field) for field in fields):
        _fail("context parent business/policy identity differs")
    if (identity.dataset_manifest_sha256 != plan.dataset_manifest_ref.sha256
            or sha({"historical_definition": request.selection_runtime_semantics}) != identity.universe_identity_sha256):
        _fail("context original dataset/universe identity differs")
    dataset_path = _verify_reference(plan.dataset_manifest_ref)
    dataset = json.loads(dataset_path.read_text(encoding="utf-8"))
    pins[dataset_path] = plan.dataset_manifest_ref.sha256
    if (dataset.get("policy_dataset_bundle_id") != plan.dataset_identity
            or any(dataset.get(field) != getattr(request, field) for field in fields if field != "selection_runtime_semantics_hash")
            or research_policy_identity(baseline_policy_sha256=request.baseline_policy_sha256,
                shadow_policy_sha256=request.shadow_policy_sha256, cost_policy_sha256=request.cost_policy_sha256) != plan.policy_identity):
        _fail("context request differs from the registered dataset/policy")
    for original, prepared in (("request.json", "frozen_request.json"), ("candidate_rankings.parquet", "frozen_rankings.parquet")):
        if dataset.get("files", {}).get(original, {}).get("sha256") != manifest["files"][prepared]["sha256"]:
            _fail("context copied request/rankings differ from the registered original")
    if identity.decision_use != "NAVIGATION_ONLY" or identity.deployable is not False:
        _fail("context exploratory prepare requires navigation-only nondeployable parent")
    selection = request.selection_runtime_semantics.get("universe_selection")
    if selection is not None and selection != universe_selection:
        _fail("context cannot change the frozen request's universe selection")
    keys = pd.read_parquet(path.parent/"features.parquet", columns=KEY)
    rankings = pd.read_parquet(path.parent/"frozen_rankings.parquet",
        columns=[*KEY, "selection_effective_rank", "is_candidate_decision"])
    candidates = rankings.loc[rankings.is_candidate_decision.eq(True) & rankings.selection_effective_rank.le(20)]
    if (candidate_roster_sha256(candidates) != identity.candidate_roster_sha256
            or not context_roster_v1(keys).equals(context_roster_v1(candidates))):
        _fail("context keys/ranks/order differ from the original frozen candidates")
    if (pd.to_datetime(keys[KEY[0]]).min().date().isoformat() != request.decision_date_start
            or pd.to_datetime(keys[KEY[0]]).max().date().isoformat() != request.decision_date_end):
        _fail("context frozen candidate window differs")
    source = load_economic_context_source_v1(profile_path=profile_path, profile_sha256=profile_sha256,
        universe_selection=universe_selection, authority_root=authority_root)
    rows, summary = build_economic_context_rows_v1(candidates=keys, source=source)
    for file, expected in pins.items():
        _pinned(file, expected)
    source.verify_unchanged()
    summary["frozen_parent"] = {field: getattr(identity, field) for field in fields}
    summary["frozen_parent"].update({"prepared_manifest_sha256": prepared_manifest_sha256,
        "universe_identity_sha256": identity.universe_identity_sha256, "source_evidence": identity.source_evidence,
        "evidence_level": identity.evidence_level, "evidence_limitations": list(identity.evidence_limitations),
        "original_pool_rule_version": "UNPROVEN", "current_pool_is_original_native_identity": False})
    summary["summary_sha256"] = sha(summary)
    return rows, summary


def main(argv=None):
    parser = argparse.ArgumentParser(description="只读Advisory探索准备：统一profile/池、保留UNKNOWN，不读取收益")
    parser.add_argument("--profile", required=True)
    parser.add_argument("--profile-sha256", required=True)
    parser.add_argument("--prepared-manifest", required=True)
    parser.add_argument("--prepared-manifest-sha256", required=True)
    parser.add_argument("--universe-selection", required=True, help="QE兼容JSON对象")
    parser.add_argument("--authority-root", help="显式既存classification bundle；不提供时仅准备core")
    args = parser.parse_args(argv)
    _, summary = prepare_economic_context_v1(profile_path=Path(args.profile), profile_sha256=args.profile_sha256,
        prepared_manifest_path=Path(args.prepared_manifest), prepared_manifest_sha256=args.prepared_manifest_sha256,
        universe_selection=json.loads(args.universe_selection), authority_root=args.authority_root)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
