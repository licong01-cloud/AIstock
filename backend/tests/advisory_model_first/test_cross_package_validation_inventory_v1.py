import json

import pytest

from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import file_sha, publish_json
from backend.services.advisory_model_first.cross_package_validation_inventory_v1 import inspect_manifest


def test_real_weights_policy_and_corrupt_identity(tmp_path):
    model = tmp_path/"model.json"
    model.write_text(json.dumps(dict(models={"mean": {"kind": "frozen"}})), encoding="utf-8")
    manifest = dict(schema_version="economic_entry_stage_v1", plan_sha256="a"*64,
        files={"model.json": dict(sha256=file_sha(model), size_bytes=model.stat().st_size)})
    path = tmp_path/"manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    item = inspect_manifest("advisory_generic_price_5td", path)
    assert item["inventory_status"] == "VERIFIED_ARTIFACTS"
    assert item["weights"][0]["model_names"] == ["mean"]
    first = item["model_id"]
    manifest["plan_sha256"] = "b"*64
    path.write_text(json.dumps(manifest), encoding="utf-8")
    assert inspect_manifest("advisory_generic_price_5td", path)["model_id"] != first
    model.write_text("{}", encoding="utf-8")
    assert inspect_manifest("advisory_generic_price_5td", path)["errors"][0]["reason"] == "ARTIFACT_IDENTITY_MISMATCH"


def test_manifest_cannot_escape_or_claim_absent_weights(tmp_path):
    path = tmp_path/"manifest.json"
    path.write_text(json.dumps(dict(files={"../model.json": {}, "model.txt": {}})), encoding="utf-8")
    errors = inspect_manifest("bundles", path)["errors"]
    assert {value["reason"] for value in errors} == {"MANIFEST_PATH_ESCAPE", "ARTIFACT_UNAVAILABLE"}


def test_checkpoint_is_immutable(tmp_path):
    path = tmp_path/"checkpoint.json"
    publish_json(path, {"original": 1})
    publish_json(path, {"original": 1})
    with pytest.raises(ValueError, match="overwrite"):
        publish_json(path, {"original": 2})


def test_auxiliary_metadata_recovers_original_named_features_not_lightgbm_column_aliases(tmp_path):
    from backend.services.advisory_model_first.cross_package_validation_applicability_v1 import frozen_schema_evidence
    model = tmp_path/"model.txt"
    model.write_text("feature_names=Column_0 Column_1\nTree=0\n", encoding="utf-8")
    metadata = tmp_path/"model_metadata.json"
    metadata.write_text(json.dumps(dict(arms={"original": {"feature_names": ["leg_norm_score_gap", "ret_1"]}},
        diagnostics={"feature_names": ["not_a_model_feature"]})), encoding="utf-8")
    manifest = tmp_path/"manifest.json"
    manifest.write_text(json.dumps(dict(files={p.name: dict(sha256=file_sha(p), size_bytes=p.stat().st_size)
        for p in (model, metadata)})), encoding="utf-8")
    schema = frozen_schema_evidence(inspect_manifest("economic_entry", manifest))
    assert schema["named_dual_leg_fields"] == ["leg_norm_score_gap"]
    assert "ret_1" in schema["features"] and "not_a_model_feature" not in schema["features"]


def test_pair_classification_distinguishes_structural_absence_from_negative_result():
    from backend.services.advisory_model_first.cross_package_validation_applicability_v1 import classify_inventory_pair
    item = dict(model_id="frozen", family="original", role="REVIEW_CLOCK_ENTRY", manifest_ref="original.json",
        manifest_sha256="a"*64, errors=[], predictor_artifact_count=1, derived_calibrator_present=False)
    package = dict(package_id="new", alpha_mode="single_alpha")
    schema = dict(named_dual_leg_fields=["leg_norm_score_gap"], required_features=[], features=["leg_norm_score_gap"])
    result = classify_inventory_pair(item=item, package=package, schema=schema,
        source_state="ORIGINAL_SOURCE_READY", executed=[])
    assert result["applicability"] == "SEMANTIC_NOT_APPLICABLE" and result["effect_status"] == "NOT_TESTABLE"
    assert result["package_alpha_is_not_rejected"]
    item["predictor_artifact_count"] = 0
    unavailable = classify_inventory_pair(item=item, package=package, schema=schema,
        source_state="ORIGINAL_SOURCE_READY", executed=[])
    assert unavailable["applicability"] == "ARTIFACT_UNAVAILABLE"
    item["predictor_artifact_count"] = 1
    schema["named_dual_leg_fields"] = []
    evaluated = classify_inventory_pair(item=item, package=package, schema=schema,
        source_state="ORIGINAL_SOURCE_READY", executed=[dict(effect_status="INSUFFICIENT_NEGATIVE")])
    assert evaluated["applicability"] == "APPLICABLE_READY" and evaluated["executed_arm_count"] == 1
