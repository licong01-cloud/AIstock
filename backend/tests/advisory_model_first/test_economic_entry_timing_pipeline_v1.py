"""Fixture-only immutable lifecycle; real heads/shadow, not investment evidence."""
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_economic_entry_timing_training_v1 import arguments
from backend.services.advisory_model_first import economic_entry_timing_pipeline_v1 as pipeline
from backend.services.advisory_model_first import economic_entry_timing_evaluation_v1 as evaluation
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import SEMANTICS as CORE_SEMANTICS
from backend.services.advisory_model_first.economic_entry_timing_features_v1 import SEMANTICS as TIMING_SEMANTICS
from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryStudyPlanV3
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, publish_stage
from backend.services.advisory_model_first.economic_entry_timing_contracts_v1 import CANDIDATE_NAMES
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def setup_plan(study, tmp_path, monkeypatch):
    args, _ = arguments(study)
    prior = tmp_path/"parent"
    parent_file = publish_stage(study_root=prior, stage="preregistered", plan_sha256="a"*64, parent_sha256=None,
        artifacts={"plan.json": _json_bytes({"fixture": "not actual source evidence"})})/"plan.json"
    parent = AlignedEntryStudyPlanV3(v2_plan_ref=evidence_reference_for_file(parent_file, role="unit_v2"),
        v2_prepared_manifest_ref=evidence_reference_for_file(parent_file.parent/"manifest.json", role="unit_v2_prepared"),
        training_request=args["request"].parent_request, simulator_sha256=pipeline.file_sha256(pipeline.Path(pipeline.__file__).with_name("shadow_portfolio_policy.py")),
        parent_lineage=("original", "risk_v2"), expected_train_rows=53, expected_validation_rows=30)
    original = tmp_path/"original"
    ranks = study["features"].loc[:, KEY].copy()
    ranks["selection_effective_rank"] = ranks.instrument.str[:6].astype(int)
    ranks["combined_score"], ranks["is_candidate_decision"] = -ranks.selection_effective_rank.astype(float), True
    context = [{KEY[0]: decision, KEY[1]: target, "instrument": f"{rank:06d}.SZ", "selection_effective_rank": rank,
        "combined_score": -float(rank), "is_candidate_decision": False}
        for decision, target in ranks[KEY[:2]].drop_duplicates().itertuples(index=False, name=None) for rank in range(7, 41)]
    ranks = pd.concat([ranks, pd.DataFrame(context)], ignore_index=True).sort_values([KEY[0], "selection_effective_rank"])
    days = pd.bdate_range(study["request"].train_start, study["request"].label_cutoff)
    quotes = pd.MultiIndex.from_product([days, sorted(ranks.instrument.unique())], names=["trade_date", "instrument"]).to_frame(index=False)
    for field in ("open", "high", "low", "close"):
        quotes["raw_"+field+"_cny"] = 10.
    quotes["policy_price_per_raw_cny"], quotes["up_limit"], quotes["down_limit"] = 1., 20., 1.
    quotes["suspended"], quotes["tradability_unknown"] = False, False
    quotes["source_sha256"], quotes["price_coordinate_sha256"] = study["identity"].price_source_sha256, study["identity"].price_coordinate_sha256
    refs = ranks.loc[:, KEY].assign(target_reference_raw_cny=10., reference_visible_through=ranks[KEY[0]], source_sha256=study["identity"].reference_source_sha256)
    frozen_request = {"terminal_weights": {"lstm_fixture": .7, "fund_fixture": .3}, "package_id": study["identity"].package_id,
        "manifest_sha256": study["identity"].manifest_sha256, "shadow_policy_sha256": study["identity"].shadow_policy_sha256,
        "cost_policy_sha256": study["identity"].cost_policy_sha256}
    original_stage = publish_stage(study_root=original, stage="prepared", plan_sha256="d"*64, parent_sha256=None,
        artifacts={"frozen_rankings.parquet": _parquet_bytes(ranks), "identity.json": _json_bytes(study["identity"].model_dump(mode="json")),
            "prices.parquet": _parquet_bytes(quotes), "references.parquet": _parquet_bytes(refs),
            "calendar.json": _json_bytes([day.isoformat() for day in days]), "frozen_request.json": _json_bytes(frozen_request)})
    loaded = (None, SimpleNamespace(dataset_identity="d"*64, policy_identity="e"*64), original, None, args["labels"], study["request"], study["features"], None)
    monkeypatch.setattr(pipeline, "_parent", lambda path: (parent, prior, loaded))
    recipe = {"core_semantics": CORE_SEMANTICS, "timing_semantics": TIMING_SEMANTICS, "feature_names": list(CANDIDATE_NAMES[:-1]),
        "roles": {"lstm": "lstm_fixture", "fund": "fund_fixture"}, "terminal_weights": frozen_request["terminal_weights"],
        "package_id": frozen_request["package_id"], "manifest_sha256": frozen_request["manifest_sha256"],
        "policy_sha256": frozen_request["shadow_policy_sha256"], "cost_sha256": frozen_request["cost_policy_sha256"],
        "implementation_hash_algorithm": "UTF8_LF_BYTES_V1", "implementation": {name: pipeline.code_sha256(pipeline.Path(pipeline.__file__).with_name(name))
            for name in ("economic_daily_feature_core_v1.py", "economic_entry_timing_features_v1.py", "economic_common_core_daily_source_v1.py")}}
    payload = _parquet_bytes(args["inputs"])
    identity = {"rows": len(args["inputs"]), "rows_file_sha256": pipeline.hashlib.sha256(payload).hexdigest(),
        "recipe": recipe, "recipe_sha256": sha(recipe), "frozen_parent_manifest_sha256": pipeline.file_sha256(original_stage/"manifest.json"),
        "source_evidence": "CURRENT_DB_HISTORICAL_NON_VINTAGE", "native_identity": "UNPROVEN", "outcomes_read": False,
        "database_written": False, "sealed_accessed": False, "model_fits": 0}
    input_stage = publish_stage(study_root=tmp_path/"input", stage="prepared", plan_sha256=sha(identity), parent_sha256=identity["frozen_parent_manifest_sha256"],
        artifacts={"features.parquet": payload, "identity.json": _json_bytes(identity)})
    plan = pipeline.build_timing_plan_v1(parent_plan_path=parent_file, input_manifest_path=input_stage/"manifest.json")
    output = tmp_path/"timing"
    plan_file = pipeline.preregister_timing_study_v1(plan=plan, output_root=output)
    return plan, plan_file, output


def test_atomic_registry_real_four_heads_three_shadow_arms_and_exact_retry_no_refit(tmp_path, study, monkeypatch):
    plan, plan_file, output = setup_plan(study, tmp_path, monkeypatch)
    fitted_stage = pipeline.train_timing_study_v1(plan_path=plan_file, output_root=output, qe_training_idle=True)
    arms = pipeline.load_fitted_timing_v1(plan_path=plan_file, output_root=output)
    assert {len(value.feature_names) for value in arms.values()} == {13, 15}
    monkeypatch.setattr(pipeline, "train_timing_entry_v1", lambda **kw: pytest.fail("exact retry refitted"))
    assert pipeline.train_timing_study_v1(plan_path=plan_file, output_root=output, qe_training_idle=True) == fitted_stage
    target = evaluation.evaluate_timing_study_v1(plan_path=plan_file, output_root=output)
    report = json.loads((target/"evaluation.json").read_text(encoding="utf-8"))
    assert set(report["metrics"]) == {"baseline", "core_thirteen", "timing_fifteen"}
    assert report["navigation"] == "STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION" and not report["deployable"]
    assert report["top20_rows"] == 18 and report["test_decision_days"] == 3 and report["fitted_head_count"] == 4
    monkeypatch.setattr(evaluation, "replay_shadow_portfolio", lambda **kw: pytest.fail("exact retry re-evaluated"))
    assert evaluation.evaluate_timing_study_v1(plan_path=plan_file, output_root=output) == target
    records = AdvisoryResearchTrialRegistryV1(output/"trial_registry.jsonl").read()
    assert len(records) == 4 and {record.planned_trial_count for record in records} == {2}
    with pytest.raises(AdvisoryModelFirstError):
        pipeline.preregister_timing_study_v1(plan=plan.model_copy(update={"scope": {"fake": True}}), output_root=output)


def test_resource_conflict_and_incomplete_fit_attempt_never_refit_or_fake_trained(tmp_path, study, monkeypatch):
    _, plan_file, output = setup_plan(study, tmp_path, monkeypatch)
    with pytest.raises(AdvisoryModelFirstError, match="overlap"):
        pipeline.train_timing_study_v1(plan_path=plan_file, output_root=output, qe_training_idle=False)
    def fail(**kw):
        raise RuntimeError("fixture fit failure")
    monkeypatch.setattr(pipeline, "train_timing_entry_v1", fail)
    with pytest.raises(RuntimeError):
        pipeline.train_timing_study_v1(plan_path=plan_file, output_root=output, qe_training_idle=True)
    with pytest.raises(AdvisoryModelFirstError, match="prior fit attempt"):
        pipeline.train_timing_study_v1(plan_path=plan_file, output_root=output, qe_training_idle=True)
    assert not list(output.glob("*/trained"))


def test_algorithm_hash_only_normalizes_CRLF_not_code_or_artifact_bytes(tmp_path):
    one, two = tmp_path/"one.py", tmp_path/"two.py"
    one.write_bytes(b"value = 1\n")
    two.write_bytes(b"value = 1\r\n")
    assert pipeline.code_sha256(one) == pipeline.code_sha256(two) and pipeline.file_sha256(one) != pipeline.file_sha256(two)
    two.write_bytes(b"value = 2\r\n")
    assert pipeline.code_sha256(one) != pipeline.code_sha256(two)


def test_frozen_snapshot_adapter_authorizes_before_reading_any_label_stage(tmp_path, monkeypatch):
    # Isolate ordering only; the existing publisher/reference tests own byte
    # integrity. These stub plans are not alleged historical provenance.
    files = {}
    for name in ("aligned", "risk", "original"):
        path = tmp_path/name/"preregistered/plan.json"
        path.parent.mkdir(parents=True)
        path.write_text("{}", encoding="utf-8")
        files[name] = path
    original = SimpleNamespace(experiment_id="original", plan_sha256="a"*64)
    risk = SimpleNamespace(experiment_id="risk", plan_sha256="b"*64, parent_lineage=("original",),
        parent_plan_ref=evidence_reference_for_file(files["original"], role="fixture_original"))
    parent = SimpleNamespace(experiment_id="aligned", plan_sha256="c"*64, parent_lineage=("risk",),
        v2_plan_ref=evidence_reference_for_file(files["risk"], role="fixture_risk"))
    for cls, value in ((pipeline.AlignedEntryStudyPlanV3, parent), (pipeline.EntryLossStudyPlanV2, risk), (pipeline.EconomicEntryStudyPlanV1, original)):
        monkeypatch.setattr(cls, "model_validate_json", lambda text, value=value: value)
    seen = []
    def read(path, *, stage, **kw):
        assert stage == "preregistered", "label stage read before consumed-window authorization"
        seen.append(stage)
        return {"stage_sha256": "d"*64}
    monkeypatch.setattr(pipeline, "read_stage", read)
    monkeypatch.setattr(pipeline, "verify_timing_ledger", lambda *args: None)
    def deny(plan):
        raise AdvisoryModelFirstError("window denied", reason_code="UNIT_WINDOW_DENIED")
    monkeypatch.setattr(pipeline, "_authorize", deny)
    with pytest.raises(AdvisoryModelFirstError, match="window denied"):
        pipeline._parent(files["aligned"])
    assert seen == ["preregistered", "preregistered"]
