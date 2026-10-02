from types import SimpleNamespace
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first import economic_entry_serving_bundle as module
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_entry_daily_inference import _daily
from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryCandidateProjectionV1, economic_D_feature_semantics_v1
from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryDailyInputV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def test_immutable_serving_view_requires_exact_model_scope_and_cannot_be_promoted(tmp_path, study, monkeypatch):
    source = tmp_path / "unit_source.json"
    source.write_bytes(b"{}\n")
    manifest = module.EconomicEntryServingManifestV1(aligned_plan_ref=evidence_reference_for_file(source, role="unit_plan"),
        aligned_trained_manifest_ref=evidence_reference_for_file(source, role="unit_trained"),
        original_input_identity_sha256=study["identity"].identity_sha256, scope=_daily(study)["model_scope"],
        source_evidence="RECOVERED_LIMITED", evidence_limitations=study["identity"].evidence_limitations,
        feature_semantics_qualification="FROZEN_COLUMN_CONTRACT_ONLY")
    loaded = SimpleNamespace(manifest=manifest)
    monkeypatch.setattr(module, "_verified_source_view", lambda *args: loaded)
    path = module.publish_research_serving_bundle_v1(plan_path=source, aligned_output_root=tmp_path, serving_root=tmp_path / "serving")
    assert module.load_research_serving_bundle_v1(manifest_path=path, aligned_output_root=tmp_path) is loaded
    assert module.publish_research_serving_bundle_v1(plan_path=source, aligned_output_root=tmp_path, serving_root=tmp_path / "serving") == path
    with pytest.raises(ValidationError):
        module.EconomicEntryServingManifestV1.model_validate({**manifest.model_dump(), "deployable": True})
    loaded.manifest = manifest.model_copy(update={"original_input_identity_sha256": "0" * 64})
    with pytest.raises(AdvisoryModelFirstError, match="differs from actual source"):
        module.load_research_serving_bundle_v1(manifest_path=path, aligned_output_root=tmp_path)


def _confirmation_request(tmp_path, study):
    # Synthetic contract-only input. Metric gates are explicitly NOT business qualification.
    scope = _daily(study)["model_scope"].model_copy(update={"universe_definition_evidence": "NATIVE_VERIFIED",
        "universe_definition_sha256": sha({"mode": "stock_universe", "pool_ids": []})})
    def reference(role):
        return {"role": role, "artifact_uri": str(tmp_path / "unit_only_not_business_evidence.json"), "sha256": "a" * 64, "size_bytes": 1}
    calendar = tuple(pd.bdate_range("2025-03-03", periods=20).date)
    return module.build_economic_entry_confirmation_request_v1(model_content_sha256="b" * 64,
        native_training_scope_ref=reference("ECONOMIC_ENTRY_NATIVE_TRAINING_SCOPE"), scope=scope,
        business_risk={"maximum_loss_bps": 800., "reference_use": "EXPLICIT_BUSINESS_CONFIGURATION", "configuration_sha256": "c" * 64},
        criteria={"minimum_decision_days": 20, "minimum_intervention_days": 10, "minimum_intervention_fraction": .5,
            "minimum_increment_bps": 0., "maximum_additional_drawdown_bps": 0., "block_days": 5,
            "bootstrap_samples": 1000, "bootstrap_seed": 123},
        target_calendar=calendar, performance_calendar=calendar, dataset_identity="unit-only-independent-window", policy_identity="unit-only-policy",
        registered_at=datetime(2025, 2, 27, tzinfo=timezone.utc), evidence_level="NATURAL_FORWARD",
        approved_protocol_ref=reference("ECONOMIC_ENTRY_VALUE_APPROVED_PROTOCOL"))


def test_economic_confirmation_metrics_recompute_profit_risk_and_actual_support(tmp_path, study):
    request = _confirmation_request(tmp_path, study)
    days = [{"target_date": day.isoformat(), "baseline_net_return": 0., "model_net_return": .003, "intervened": True}
            for day in request.performance_calendar]
    result = module.economic_entry_confirmation_metrics_v1(request=request, matched_days=days)
    assert result["status"] == "ECONOMIC_GATES_PASSED" and result["mean_increment_bps"] == pytest.approx(30.)
    assert result["ci95_increment_bps"] == pytest.approx([30., 30.])
    assert result["interpretation"] == "PROVENANCE_REGISTRY_AND_NATIVE_SCOPE_REQUIRED_SEPARATELY"
    for poisoned, expected in (([{**row, "intervened": False} for row in days], "INSUFFICIENT_ACTUAL_INTERVENTION_SUPPORT"),
                              ([{**row, "model_net_return": -.001} for row in days], "MODEL_ABSOLUTE_NET_RETURN_NOT_POSITIVE"),
                              ([{**row, "baseline_net_return": .004} for row in days], "NET_INCREMENT_CONFIDENCE_FLOOR_NOT_PASSED")):
        assert expected in module.economic_entry_confirmation_metrics_v1(request=request, matched_days=poisoned)["failure_reasons"]
    with pytest.raises(AdvisoryModelFirstError, match="dropped, reordered"):
        module.economic_entry_confirmation_metrics_v1(request=request, matched_days=list(reversed(days)))
    with pytest.raises(AdvisoryModelFirstError, match="finite explicit"):
        module.economic_entry_confirmation_metrics_v1(request=request, matched_days=[{**days[0], "model_net_return": True}, *days[1:]])
    with pytest.raises(ValidationError):
        module.EconomicEntryConfirmationRequestV1.model_validate({**request.model_dump(), "decision_use": "ACTIVATION_EVIDENCE"})


def _native_training_scope(tmp_path, study):
    from backend.services.advisory_model_first import shared_feature_builder, suspension_aware_bar_policy
    args = _daily(study)
    identity = study["identity"].model_copy(update={"source_evidence": "NATIVE_COMPLETE",
        "universe_identity_sha256": sha({"mode": "stock_universe", "pool_ids": []})})
    roles, weights = {"lstm": "unit-leg-a", "fund": "unit-leg-b"}, {"unit-leg-a": .4, "unit-leg-b": .6}
    builder_sha = hashlib.sha256(Path(shared_feature_builder.__file__).read_bytes()).hexdigest()
    semantics = economic_D_feature_semantics_v1(component_roles=roles, terminal_weights=weights,
        shared_builder_sha256=builder_sha, bar_policy_sha256=suspension_aware_bar_policy.BAR_POLICY_HASH,
        pit_universe_key="unit-only-native-PIT", pit_rule_version="unit-only-rule")
    scope = args["model_scope"].model_copy(update={"feature_schema_sha256": sha(semantics),
        "universe_definition_evidence": "NATIVE_VERIFIED", "universe_definition_sha256": identity.universe_identity_sha256})
    source_request = args["fitted"].request.source_request.model_copy(update={"input_identity_sha256": identity.identity_sha256})
    features = study["features"].to_parquet(index=False)
    prepared = tmp_path / "native-unit-model/prepared"
    prepared.mkdir(parents=True)
    review = {"schema_version": "economic_entry_feature_semantic_review_v1", "input_identity_sha256": identity.identity_sha256,
        "feature_source_sha256": source_request.feature_source_sha256, "features_file_sha256": hashlib.sha256(features).hexdigest(),
        "feature_schema_sha256": scope.feature_schema_sha256, "compared_candidate_rows": len(study["features"]),
        "compared_D_columns": list(scope.feature_names[:-1]), "mismatched_candidate_rows": 0, "missing_values_preserved": True,
        "training_temporal_parity": "EXACT_DAILY_CONTRACT"}
    (prepared / "feature_semantic_review.json").write_text(json.dumps(review), encoding="utf-8")
    projection = EconomicEntryCandidateProjectionV1(scope=scope, component_roles=roles, terminal_weights=weights,
        universe_selection={"mode": "stock_universe", "pool_ids": []}, review_policy_sha256="a" * 64)
    proof = module.EconomicEntryNativeTrainingScopeV1(input_identity_sha256=identity.identity_sha256, projection=projection,
        shared_builder_sha256=builder_sha, bar_policy_sha256=suspension_aware_bar_policy.BAR_POLICY_HASH,
        feature_semantic_review_ref=evidence_reference_for_file(prepared / "feature_semantic_review.json", role="ECONOMIC_TRAINING_FEATURE_SEMANTICS"),
        pit_universe_key="unit-only-native-PIT", pit_rule_version="unit-only-rule",
        latest_upstream_training_date=date(2010, 1, 1))
    (prepared / "native_applicability.json").write_text(proof.model_dump_json(), encoding="utf-8")
    (prepared / "frozen_request.json").write_text(json.dumps({"baseline_policy_sha256": "a" * 64,
        "selection_runtime_semantics_hash": scope.selection_runtime_semantics_hash, "terminal_weights": weights}), encoding="utf-8")
    descriptors = {file.name: {"size_bytes": file.stat().st_size, "sha256": hashlib.sha256(file.read_bytes()).hexdigest()} for file in prepared.iterdir()}
    descriptors["features.parquet"] = {"size_bytes": len(features), "sha256": hashlib.sha256(features).hexdigest()}
    parameters = dict(original=prepared.parent, prepared_manifest={"files": descriptors}, identity=identity,
        basis_scope=args["model_scope"], source_request=source_request, input_files={"features.parquet": features})
    return parameters, proof, review


def test_native_training_scope_requires_original_prepared_members_not_appended_claims(tmp_path, study):
    parameters, proof, review = _native_training_scope(tmp_path, study)
    assert module._verified_native_training_scope_v1(**parameters) == proof
    assert module._verified_native_training_scope_v1(**{**parameters, "prepared_manifest": {"files": {}}}) is None
    with pytest.raises(AdvisoryModelFirstError, match="cannot be upgraded"):
        module._verified_native_training_scope_v1(**{**parameters, "identity": study["identity"]})
    changed = {**review, "mismatched_candidate_rows": 1}
    (parameters["original"] / "prepared/feature_semantic_review.json").write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError, match="changed"):
        module._verified_native_training_scope_v1(**parameters)


def _native_prediction_day(tmp_path, study):
    # Unit-only artifact chain; NEVER used as real source, confirmation or activation.
    _, native, _ = _native_training_scope(tmp_path, study)
    values = _confirmation_request(tmp_path, study).model_dump(exclude={"experiment_id"})
    values["scope"] = native.projection.scope
    request = module.build_economic_entry_confirmation_request_v1(**values)
    target, decision = request.target_calendar[0], date(2025, 2, 28)
    captured = datetime(2025, 2, 28, 9, tzinfo=timezone.utc)
    roster, members = [{"instrument": "000001.SZ", "selection_rank": 1}], ["000001.SZ"]
    archive = {"schema_version": "advisory_selection_input_archive_v1", "selection_run_id": "unit-run", "program_id": "unit-program",
        "binding_version_id": "unit-binding", "observed_at": captured.isoformat(), "historical_capture_backfilled": False,
        "decision_as_of_trade_date": decision.isoformat(), "target_trade_date": target.isoformat(),
        "review_policy_sha256": native.projection.review_policy_sha256,
        "manifest_sha256_by_package": {request.scope.package_id: request.scope.manifest_sha256},
        "full_source_universe_members": members, "source_universe_members_sha256": sha(members)}
    context = {"cutoff_date": decision.isoformat(), "score_trade_date": decision.isoformat(), "requested_trade_date": target.isoformat(),
               "universe_input_hash": sha(members)}
    artifact = {"package_id": request.scope.package_id, "manifest_sha256": request.scope.manifest_sha256,
        "trade_date": target.isoformat(), "status": "SUCCEEDED", "universe_count": len(members),
        "metadata": {"artifact_input_context": context}, "artifact_input_context_hash": sha(context)}
    archive["package_inputs"] = {request.scope.package_id: {"artifact": artifact, "artifact_content_sha256": sha(artifact)}}
    archive_path = tmp_path / "unit_selection_archive.json"
    archive_path.write_text(json.dumps(archive), encoding="utf-8")
    receipt = {"outcomes_read": False, "new_selection_runs": 0, "pit_universe_key": native.pit_universe_key,
        "pit_rule_version": native.pit_rule_version, "projection_sha256": native.projection.projection_sha256,
        "original_archive_reference": evidence_reference_for_file(archive_path, role="SELECTION_EXECUTION_INPUTS").model_dump(mode="json"),
        "source_universe_members_sha256": sha(members),
        "D_source": {"outcomes_read": False, "decision_date": decision.isoformat(), "target_date": target.isoformat(),
            "shared_builder_sha256": native.shared_builder_sha256, "bar_policy_sha256": native.bar_policy_sha256,
            "pit_universe_key": native.pit_universe_key, "raw_price_unit_divisor": 1000.,
            "source_window_contract": {"candidate_sessions": 20, "benchmark_sessions": 20, "breadth_sessions": 2}}}
    features = {name: .5 for name in request.scope.feature_names[:-1]}
    source = EconomicEntryDailyInputV1(scope=request.scope, program_id="unit-program", binding_version_id="unit-binding",
        run_id="unit-run", list_id="unit-list", decision_date=decision, target_date=target,
        feature_visible_through=decision, price_visible_through=decision, captured_at=captured,
        instrument="000001.SZ", selection_rank=1, candidate_roster_sha256=sha(roster), feature_source_sha256=sha(receipt),
        feature_values_sha256=sha(features), universe_membership_sha256=sha(members),
        source_evidence="NATIVE_COMPLETE", evidence_level="PROSPECTIVE_INPUT")
    payload = {"schema_version": "economic_entry_native_prediction_day_v1", "model_content_sha256": request.model_content_sha256,
        "scope_sha256": request.scope.scope_sha256, "program_id": source.program_id, "binding_version_id": source.binding_version_id,
        "run_id": source.run_id, "list_id": source.list_id, "decision_date": decision.isoformat(), "target_date": target.isoformat(),
        "captured_at": captured.isoformat(), "candidate_roster_sha256": sha(roster), "roster": roster, "source_members": members,
        "source_receipt": receipt, "inputs": [{"prediction_input": source.model_dump(mode="json"), "decision_features": features}],
        "price_contexts": [None], "original_D_capsule_ref": None}
    return native, request, payload, archive


def test_confirmation_native_day_binds_actual_input_archive_PIT_and_D_features(tmp_path, study):
    native, request, payload, _ = _native_prediction_day(tmp_path, study)
    target, receipt, features = request.target_calendar[0], payload["source_receipt"], payload["inputs"][0]["decision_features"]
    roster = payload["roster"]
    path = tmp_path / "unit_native_prediction.json"
    def read(value):
        path.write_text(json.dumps(value), encoding="utf-8")
        return module._read_economic_native_prediction_day_v1(
            reference=evidence_reference_for_file(path, role="ECONOMIC_ENTRY_NATIVE_PREDICTION_DAY"), request=request, native=native,
            expected_date=target, prediction={key: payload[key] for key in ("decision_date", "target_date", "captured_at")})
    assert read(payload) == payload
    empty_receipt = {**receipt, "D_source": {"status": "NO_CANDIDATES", "query_count": 0, "outcomes_read": False}}
    empty_day = {**payload, "roster": [], "inputs": [], "price_contexts": [], "candidate_roster_sha256": sha([]), "source_receipt": empty_receipt}
    assert read(empty_day) == empty_day
    # Actual historical prediction time may be today, but the source capture
    # stays its original pre-T clock and must have unchanged native D evidence.
    historical = module.build_economic_entry_confirmation_request_v1(**{**request.model_dump(exclude={"experiment_id"}),
        "evidence_level": "LOCKED_HISTORICAL_OOT", "registered_at": datetime(2026, 10, 2, tzinfo=timezone.utc)})
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError, match="original immutable D capsule"):
        module._read_economic_native_prediction_day_v1(reference=evidence_reference_for_file(path, role="ECONOMIC_ENTRY_NATIVE_PREDICTION_DAY"),
            request=historical, native=native, expected_date=target, prediction={key: payload[key] for key in ("decision_date", "target_date", "captured_at")})
    for changed, expected in (({**payload, "run_id": "other-run"}, "original Selection archive"),
            ({**payload, "inputs": [{**payload["inputs"][0], "decision_features": {**features, "ret_1": -.9}}]}, "feature or candidate identity"),
            ({**payload, "source_receipt": {**receipt, "pit_rule_version": "different-rule"}}, "frozen PIT"),
            ({**payload, "roster": [*roster, *roster], "inputs": [*payload["inputs"], *payload["inputs"]]}, "frozen PIT")):
        with pytest.raises(AdvisoryModelFirstError, match=expected):
            read(changed)


def test_qualification_metadata_preflights_size_and_rejects_duplicate_keys(tmp_path):
    path = tmp_path / "metadata.json"
    path.write_bytes(b'{"status":"first","status":"second"}')
    reference = evidence_reference_for_file(path, role="UNIT_METADATA")
    with pytest.raises(AdvisoryModelFirstError, match="duplicate JSON keys"):
        module._read_economic_metadata_reference(reference, role="UNIT_METADATA")
    with pytest.raises(AdvisoryModelFirstError, match="immutable reference"):
        module._read_economic_metadata_reference(reference.model_copy(update={"size_bytes": 1}), role="UNIT_METADATA")
    path.write_bytes(b'[]')
    with pytest.raises(AdvisoryModelFirstError, match="JSON object"):
        module._read_economic_metadata_reference(evidence_reference_for_file(path, role="UNIT_METADATA"), role="UNIT_METADATA")


def _unit_confirmation_authorization(tmp_path, request, native, write):
    # Synthetic metadata ONLY; no real sealed window/authorization/receipt used.
    from statistics import NormalDist, stdev
    from backend.services.advisory_model_first.research_control_contracts import (
        build_window_contract, build_window_access_request, build_holdout_consumption_receipt,
    )
    authority = tmp_path / "unit_window_authority"
    authority.mkdir()
    receipt_path = authority / "sealed_holdout_consumption_receipt.json"
    contract = build_window_contract(package_id=request.scope.package_id, manifest_sha256=request.scope.manifest_sha256,
        runtime_semantics_hash=request.scope.selection_runtime_semantics_hash,
        baseline_policy_sha256=native.projection.review_policy_sha256, shadow_policy_sha256=request.scope.shadow_policy_sha256,
        cost_policy_sha256=request.scope.cost_policy_sha256, source_policy="UNIT_ONLY_NO_AUTHORIZATION",
        artifact_root_uri=authority.as_posix(), sealed_consumption_receipt_uri=receipt_path.as_posix(),
        created_at=request.registered_at, windows=[
            {"window_id": "unit-dev", "dataset_identity": "unit-dev", "start_date": "2025-01-01", "end_date": "2025-01-31",
             "state": "DEVELOPMENT_CONSUMED", "purpose": "unit-only variance"},
            {"window_id": "unit-test", "dataset_identity": "unit-test", "start_date": "2024-10-01", "end_date": "2024-10-31",
             "state": "FROZEN_TEST_CONSUMED", "purpose": "unit-only consumed"},
            {"window_id": "unit-history", "dataset_identity": "unit-history", "start_date": "2024-11-01", "end_date": "2024-11-30",
             "state": "HISTORICAL_REPLAY_CONSUMED", "purpose": "unit-only consumed"},
            {"window_id": "unit-only-independent", "dataset_identity": request.dataset_identity,
             "start_date": request.target_calendar[0], "end_date": request.performance_calendar[-1],
             "state": "SEALED_UNCONSUMED", "purpose": "synthetic test only"}])
    access = build_window_access_request(contract_sha256=contract.contract_sha256, study_type="CONFIRMATION",
        objective_contract=request.objective_contract, decision_use="DIRECTION_GATE", dataset_identity=request.dataset_identity,
        policy_identity=request.policy_identity, start_date=request.target_calendar[0], end_date=request.performance_calendar[-1],
        frontier_id="unit-parent", candidate_id=module._economic_confirmation_candidate_id(request=request))
    receipt = build_holdout_consumption_receipt(contract=contract, request=access, window_id="unit-only-independent").model_copy(
        update={"consumed_at": request.registered_at})
    means = [-1., 1., -1., 1.]
    measure = {"schema_version": "economic_entry_value_development_blocks_v1", "model_content_sha256": request.model_content_sha256,
        "scope_sha256": request.scope.scope_sha256, "policy_identity": request.policy_identity, "dataset_identity": "unit-dev",
        "block_days": 5, "calendar": [d.date().isoformat() for d in pd.bdate_range("2025-01-06", periods=20)],
        "block_means_bps": means, "recorded_at": request.registered_at.isoformat()}
    return {"window_contract_ref": write(authority / "window.json", contract.model_dump(mode="json"),
                "ECONOMIC_ENTRY_CONFIRMATION_WINDOW_CONTRACT").model_dump(mode="json"),
        "window_access_request_ref": write(authority / "access.json", access.model_dump(mode="json"),
                "ECONOMIC_ENTRY_CONFIRMATION_WINDOW_ACCESS").model_dump(mode="json"),
        "window_consumption_receipt_ref": write(receipt_path, receipt.model_dump(mode="json"),
                "ECONOMIC_ENTRY_CONFIRMATION_WINDOW_CONSUMPTION").model_dump(mode="json"),
        "power_analysis": {"method": "DEVELOPMENT_BLOCK_NORMAL_APPROX_V1", "alpha": .05, "target_power": .8,
            "anticipated_increment_bps": 10., "mde_increment_bps": (NormalDist().inv_cdf(.975) + NormalDist().inv_cdf(.8)) * stdev(means) / 2,
            "development_measurement_ref": write(authority / "power_blocks.json", measure,
                "ECONOMIC_ENTRY_VALUE_DEVELOPMENT_BLOCKS").model_dump(mode="json")}}


@pytest.mark.parametrize("fault", ["missing", "candidate", "receipt_alias", "late_consumption", "model_not_available"])
def test_confirmation_window_reads_existing_authority_without_consuming_it(tmp_path, study, monkeypatch, fault):
    from datetime import timedelta
    from backend.services.advisory_model_first import research_control
    from backend.services.advisory_model_first.research_control_contracts import build_window_access_request
    native, template, _, _ = _native_prediction_day(tmp_path, study)
    request = module.build_economic_entry_confirmation_request_v1(**{**template.model_dump(exclude={"experiment_id"}),
        "policy_identity": research_control.research_policy_identity(baseline_policy_sha256=native.projection.review_policy_sha256,
            shadow_policy_sha256=template.scope.shadow_policy_sha256, cost_policy_sha256=template.scope.cost_policy_sha256)})
    def write(path, payload, role):
        path.write_text(json.dumps(payload), encoding="utf-8")
        return evidence_reference_for_file(path, role=role)
    protocol = _unit_confirmation_authorization(tmp_path, request, native, write)
    source = SimpleNamespace(source_plan=SimpleNamespace(experiment_id="unit-parent"), model_available_at=request.registered_at)
    monkeypatch.setattr(research_control, "authorize_research_window_access", lambda **kwargs: pytest.fail("consumer cannot authorize/write holdout"))
    def read(value):
        return module._read_economic_confirmation_window_v1(protocol=value, request=request,
            source=source, native=native, approved_at=request.registered_at)
    assert read(protocol)[1].candidate_id == module._economic_confirmation_candidate_id(request=request)
    changed = dict(protocol)
    if fault == "missing":
        changed.pop("window_consumption_receipt_ref")
    elif fault == "model_not_available":
        source.model_available_at = request.registered_at + timedelta(seconds=1)
    elif fault == "candidate":
        reference = changed["window_access_request_ref"]
        original = json.loads(Path(reference["artifact_uri"]).read_text())
        original.pop("request_id")
        original.pop("request_sha256")
        foreign = build_window_access_request(**{**original, "candidate_id": "different-point-after-confirmation"})
        changed["window_access_request_ref"] = write(Path(reference["artifact_uri"]), foreign.model_dump(mode="json"), reference["role"]).model_dump(mode="json")
    else:
        reference = changed["window_consumption_receipt_ref"]
        value = json.loads(Path(reference["artifact_uri"]).read_text())
        path = tmp_path / "aliased_consumption.json" if fault == "receipt_alias" else Path(reference["artifact_uri"])
        if fault == "late_consumption":
            value["consumed_at"] = (request.registered_at + timedelta(seconds=1)).isoformat()
        changed["window_consumption_receipt_ref"] = write(path, value, reference["role"]).model_dump(mode="json")
    with pytest.raises(AdvisoryModelFirstError, match="canonical window"):
        read(changed)


@pytest.mark.parametrize("fault", ["flag_only", "reported_mde", "underpowered", "future_variance", "zero_variance"])
def test_confirmation_power_recomputes_development_measurement_not_a_flag(tmp_path, study, fault):
    from backend.services.advisory_model_first.research_control import research_policy_identity
    native, template, _, _ = _native_prediction_day(tmp_path, study)
    request = module.build_economic_entry_confirmation_request_v1(**{**template.model_dump(exclude={"experiment_id"}),
        "policy_identity": research_policy_identity(baseline_policy_sha256=native.projection.review_policy_sha256,
            shadow_policy_sha256=template.scope.shadow_policy_sha256, cost_policy_sha256=template.scope.cost_policy_sha256)})
    def write(path, payload, role):
        path.write_text(json.dumps(payload), encoding="utf-8")
        return evidence_reference_for_file(path, role=role)
    protocol = {**_unit_confirmation_authorization(tmp_path, request, native, write), "minimum_required_portfolio_days": 20}
    source = SimpleNamespace(source_plan=SimpleNamespace(experiment_id="unit-parent"), model_available_at=request.registered_at,
        fitted=SimpleNamespace(request=SimpleNamespace(source_request=SimpleNamespace(label_cutoff=date(2025, 2, 1)))))
    contract, _, _ = module._read_economic_confirmation_window_v1(protocol=protocol, request=request, source=source,
        native=native, approved_at=request.registered_at)
    def read(value):
        return module._read_economic_confirmation_power_v1(protocol=value, request=request, contract=contract,
            source=source, approved_at=request.registered_at)
    assert read(protocol) is None
    if fault == "flag_only":
        protocol.pop("power_analysis")
        protocol["power_status"] = "CONFIRMATORY"
    elif fault == "reported_mde":
        protocol["power_analysis"]["mde_increment_bps"] = 0.
    elif fault == "underpowered":
        protocol["power_analysis"]["anticipated_increment_bps"] = .01
    else:
        reference = protocol["power_analysis"]["development_measurement_ref"]
        path = Path(reference["artifact_uri"])
        measure = json.loads(path.read_text())
        if fault == "future_variance":
            measure["calendar"][-1] = "2025-02-10"
        else:
            measure["block_means_bps"] = [0.] * len(measure["block_means_bps"])
        protocol["power_analysis"]["development_measurement_ref"] = write(path, measure, reference["role"]).model_dump(mode="json")
    with pytest.raises(AdvisoryModelFirstError, match="power|underpowered|required days/MDE"):
        read(protocol)


@pytest.mark.parametrize("evidence_level", ["NATURAL_FORWARD", "LOCKED_HISTORICAL_OOT"])
def test_confirmation_reader_uses_full_frozen_chain_not_confirmation_flag(tmp_path, study, monkeypatch, evidence_level):
    from copy import deepcopy
    from backend.services.advisory_model_first.research_control import research_policy_identity
    from backend.services.advisory_model_first.research_control_contracts import build_trial_record
    native, template_request, template_day, template_archive = _native_prediction_day(tmp_path, study)
    values = template_request.model_dump(exclude={"experiment_id"})
    if evidence_level == "LOCKED_HISTORICAL_OOT":
        values.update(evidence_level=evidence_level, registered_at=datetime(2026, 10, 2, tzinfo=timezone.utc))
        template_request = module.build_economic_entry_confirmation_request_v1(**values)
        with pytest.raises(ValidationError):
            module.build_economic_entry_confirmation_request_v1(**{**values, "evidence_level": "NATURAL_FORWARD"})
    original = tmp_path / "native-unit-model"
    def write(path, payload, role):
        path.write_text(json.dumps(payload), encoding="utf-8")
        return evidence_reference_for_file(path, role=role)
    source_ref = write(tmp_path / "unit_only_model.json", {}, "aligned_plan")
    trained_ref = source_ref.model_copy(update={"role": "aligned_trained"})
    source = SimpleNamespace(original_study_root=original, original_input_files={}, model_available_at=template_request.registered_at,
        manifest=SimpleNamespace(aligned_plan_ref=source_ref,
        aligned_trained_manifest_ref=trained_ref, scope=native.projection.scope, source_evidence="NATIVE_COMPLETE",
        feature_semantics_qualification="NATIVE_DAILY_CONTRACT_VERIFIED"), fitted=SimpleNamespace(request=SimpleNamespace(
            source_request=SimpleNamespace(label_cutoff=date(2025, 2, 1)))), source_plan=SimpleNamespace(simulator_sha256="f" * 64, experiment_id="unit-parent"))
    model_sha = sha({"aligned_plan_sha256": source_ref.sha256, "aligned_trained_manifest_sha256": trained_ref.sha256})
    protocol = {"schema_version": "economic_entry_value_confirmation_protocol_v1", "model_content_sha256": model_sha,
        "scope_sha256": template_request.scope.scope_sha256, "criteria_sha256": sha(template_request.criteria.model_dump(mode="json")),
        "business_risk_sha256": sha(template_request.business_risk.model_dump(mode="json")),
        "calendar_sha256": sha({"decision": [d.isoformat() for d in template_request.target_calendar],
            "performance": [d.isoformat() for d in template_request.performance_calendar]}),
        "power_status": "CONFIRMATORY", "minimum_required_portfolio_days": 20,
        "authorization_ref": "UNIT_ONLY_NOT_BUSINESS_APPROVAL", "approved_at": template_request.registered_at.isoformat()}
    values = template_request.model_dump(exclude={"experiment_id"})
    values.update({"model_content_sha256": model_sha,
        "native_training_scope_ref": evidence_reference_for_file(original / "prepared/native_applicability.json", role="ECONOMIC_ENTRY_NATIVE_TRAINING_SCOPE"),
        "policy_identity": research_policy_identity(baseline_policy_sha256=native.projection.review_policy_sha256,
            shadow_policy_sha256=native.projection.scope.shadow_policy_sha256, cost_policy_sha256=native.projection.scope.cost_policy_sha256)})
    temporary = module.build_economic_entry_confirmation_request_v1(**values)
    protocol.update(_unit_confirmation_authorization(tmp_path, temporary, native, write))
    protocol_ref = write(tmp_path / "unit_only_protocol.json", protocol, "ECONOMIC_ENTRY_VALUE_APPROVED_PROTOCOL")
    request = module.build_economic_entry_confirmation_request_v1(**{**values, "approved_protocol_ref": protocol_ref})
    root = tmp_path / request.experiment_id
    root.mkdir()
    request_ref = write(root / "request.json", request.model_dump(mode="json"), "ECONOMIC_ENTRY_VALUE_CONFIRMATION_REQUEST")
    prediction_days = []
    prediction_frozen_at = datetime(2025, 3, 29, tzinfo=timezone.utc) if evidence_level == "NATURAL_FORWARD" else datetime(2026, 10, 2, 2, tzinfo=timezone.utc)
    outcomes_at = prediction_frozen_at.replace(hour=3)
    settlement_at = prediction_frozen_at.replace(hour=4)
    evaluation_at = prediction_frozen_at.replace(hour=5)
    for index, target in enumerate(request.target_calendar):
        decision = (pd.Timestamp(target) - pd.offsets.BDay()).date()
        captured = datetime.combine(decision, datetime.min.time(), timezone.utc).replace(hour=9)
        day, archive = deepcopy(template_day), deepcopy(template_archive)
        for payload in (day, archive):
            payload["captured_at" if payload is day else "observed_at"] = captured.isoformat()
            payload["decision_date" if payload is day else "decision_as_of_trade_date"] = decision.isoformat()
            payload["target_date" if payload is day else "target_trade_date"] = target.isoformat()
        item = archive["package_inputs"][request.scope.package_id]
        artifact = item["artifact"]
        artifact["trade_date"] = target.isoformat()
        artifact["metadata"]["artifact_input_context"].update(cutoff_date=decision.isoformat(), score_trade_date=decision.isoformat(),
                                                            requested_trade_date=target.isoformat())
        artifact["artifact_input_context_hash"] = sha(artifact["metadata"]["artifact_input_context"])
        item["artifact_content_sha256"] = sha(artifact)
        day["model_content_sha256"] = model_sha
        receipt = day["source_receipt"]
        receipt["original_archive_reference"] = write(root / f"unit_archive_{index}.json", archive, "SELECTION_EXECUTION_INPUTS").model_dump(mode="json")
        receipt["D_source"].update(decision_date=decision.isoformat(), target_date=target.isoformat())
        day["inputs"][0]["prediction_input"].update(decision_date=decision.isoformat(), target_date=target.isoformat(),
            captured_at=captured.isoformat(), feature_visible_through=decision.isoformat(), price_visible_through=decision.isoformat(),
            feature_source_sha256=sha(receipt))
        if evidence_level == "LOCKED_HISTORICAL_OOT":
            capsule = {name: value for name, value in day.items() if name not in {"model_content_sha256", "schema_version", "original_D_capsule_ref"}}
            capsule["schema_version"] = "economic_entry_native_D_capsule_v1"
            day["original_D_capsule_ref"] = write(root / f"unit_capsule_{index}.json", capsule, "ECONOMIC_ENTRY_NATIVE_D_CAPSULE").model_dump(mode="json")
        reference = write(root / f"unit_native_{index}.json", day, "ECONOMIC_ENTRY_NATIVE_PREDICTION_DAY")
        prediction_days.append({key: day[key] for key in ("decision_date", "target_date", "captured_at", "scope_sha256", "model_content_sha256")}
            | {"source_evidence": "NATIVE_COMPLETE", "native_input_ref": reference.model_dump(mode="json"),
               "predicted_at": (captured if evidence_level == "NATURAL_FORWARD" else prediction_frozen_at).isoformat()})
    matched = [{"target_date": day.isoformat(), "baseline_net_return": 0., "model_net_return": .003, "intervened": True}
               for day in request.performance_calendar]
    metrics = module.economic_entry_confirmation_metrics_v1(request=request, matched_days=matched)
    files = {"request.json": request_ref.model_dump(mode="json")}
    parent = request.request_sha256
    for name, phase, content in (("prediction.json", "PREDICTED", {"days": prediction_days, "recorded_at": prediction_frozen_at.isoformat()}),
            ("settlement.json", "SETTLED", {"scope_sha256": request.scope.scope_sha256, "policy_identity": request.policy_identity,
                "unknown_action_policy": request.unknown_action_policy, "simulator_sha256": "f" * 64, "matched_days": matched,
                "recorded_at": settlement_at.isoformat(), "outcomes_first_read_at": outcomes_at.isoformat()}),
            ("evaluation.json", "EVALUATED", {"metrics": metrics, "recorded_at": evaluation_at.isoformat()})):
        payload = {"stage": phase, "request_sha256": request.request_sha256, "parent_sha256": parent, **content}
        parent = sha(payload)
        files[name] = write(root / name, {**payload, "stage_sha256": parent}, f"ECONOMIC_ENTRY_VALUE_{phase}").model_dump(mode="json")
    manifest = {"schema_version": "economic_entry_value_confirmation_manifest_v1", "producer_version": "economic_entry_value_confirmation_v1",
        "request_sha256": request.request_sha256, "files": files}
    manifest_ref = write(root / "manifest.json", {**manifest, "manifest_sha256": sha(manifest)}, "ECONOMIC_ENTRY_VALUE_CONFIRMATION_MANIFEST")
    registry = []
    for stage, classification, reference, count, clock in (("PREREGISTERED", "CONTROL_READY", request_ref, 0, request.registered_at),
            ("EVALUATED", "CONFIRMED", manifest_ref, 1, evaluation_at)):
        registry.append(build_trial_record(experiment_id=request.experiment_id, attempt_id="unit-exact-attempt", research_stage=stage,
            study_type="CONFIRMATION", hypothesis_family_id="unit-only-family", parent_lineage=("unit-parent",), unique_variable="unit-variable",
            objective_contract=request.objective_contract, dataset_identity=request.dataset_identity, schema_identity=request.scope.feature_schema_sha256,
            policy_identity=request.policy_identity, planned_trial_count=1, generated_trial_count=count, evaluated_trial_count=count, selected_trial_count=count,
            consumed_windows=[{"window_id": "unit-only-independent", "dataset_identity": request.dataset_identity,
                "start_date": request.target_calendar[0], "end_date": request.performance_calendar[-1]}], result_class=classification,
            decision_use="DIRECTION_GATE", evidence_refs=(reference,
                module.EvidenceReferenceV1.model_validate(protocol["window_consumption_receipt_ref"])), recorded_at=clock))
    (tmp_path / "trial_registry.jsonl").write_text("\n".join(row.model_dump_json() for row in registry) + "\n", encoding="utf-8")
    assert module._read_economic_confirmation_v1(request_reference=request_ref, manifest_reference=manifest_ref, source=source, native=native) == (request, metrics)
    monkeypatch.setattr(module, "_verified_source_view", lambda *args: source)
    kwargs = dict(plan_path=source_ref.artifact_uri, aligned_output_root=tmp_path, serving_root=tmp_path / "qualified",
        confirmation_request_path=request_ref.artifact_uri, confirmation_manifest_path=manifest_ref.artifact_uri)
    published = module.publish_confirmed_economic_entry_serving_bundle_v1(**kwargs)
    assert module.publish_confirmed_economic_entry_serving_bundle_v1(**kwargs) == published
    loaded = module.load_confirmed_economic_entry_serving_bundle_v1(manifest_path=published, aligned_output_root=tmp_path)
    assert loaded.manifest.evidence_state == "CONFIRMED_ENTRY_VALUE" and loaded.manifest.deployable is False
    assert loaded.confirmation_request == request
    # A fresh experiment identity cannot make a second point from this frontier
    # look like an exact retry, even in another declared window.
    reused = build_trial_record(**{**registry[0].model_dump(exclude={"registry_entry_id"}),
        "experiment_id": "unit-second-confirmation"})
    registry_path = tmp_path / "trial_registry.jsonl"
    registry_path.write_text("\n".join(row.model_dump_json() for row in [*registry, reused]) + "\n", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError, match="frontier already belongs"):
        module._read_economic_confirmation_v1(request_reference=request_ref, manifest_reference=manifest_ref, source=source, native=native)
    registry_path.write_text("\n".join(row.model_dump_json() for row in registry) + "\n", encoding="utf-8")
    # Even a coherent unit confirmation chain cannot promote a recovered source.
    source.manifest.source_evidence = "RECOVERED_LIMITED"
    with pytest.raises(AdvisoryModelFirstError, match="cannot be promoted"):
        module.publish_confirmed_economic_entry_serving_bundle_v1(**{**kwargs, "serving_root": tmp_path / "must_not_exist"})
    assert not (tmp_path / "must_not_exist").exists()
    source.manifest.source_evidence = "NATIVE_COMPLETE"
    # Coherently rehashed metadata still cannot turn pre-freeze outcome access
    # into confirmation; validate the clock, not just the parent hash chain.
    parent = request.request_sha256
    for name, phase in (("prediction.json", "PREDICTED"), ("settlement.json", "SETTLED"), ("evaluation.json", "EVALUATED")):
        payload = json.loads((root / name).read_text(encoding="utf-8"))
        payload.pop("stage_sha256")
        payload["parent_sha256"] = parent
        if phase == "SETTLED":
            payload["outcomes_first_read_at"] = prediction_frozen_at.isoformat()
        parent = sha(payload)
        files[name] = write(root / name, {**payload, "stage_sha256": parent}, f"ECONOMIC_ENTRY_VALUE_{phase}").model_dump(mode="json")
    manifest_ref = write(root / "manifest.json", {**manifest, "files": files, "manifest_sha256": sha({**manifest, "files": files})},
        "ECONOMIC_ENTRY_VALUE_CONFIRMATION_MANIFEST")
    registry[-1] = build_trial_record(**{**registry[-1].model_dump(exclude={"registry_entry_id"}), "evidence_refs": (manifest_ref,
        module.EvidenceReferenceV1.model_validate(protocol["window_consumption_receipt_ref"]))})
    (tmp_path / "trial_registry.jsonl").write_text("\n".join(row.model_dump_json() for row in registry) + "\n", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError, match="outcomes were accessed before"):
        module._read_economic_confirmation_v1(request_reference=request_ref, manifest_reference=manifest_ref, source=source, native=native)
    # Valid JSON and digest, but a different original artifact must still fail closed.
    (root / "unit_archive_0.json").write_text("{}", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError, match="immutable reference"):
        module._read_economic_confirmation_v1(request_reference=request_ref, manifest_reference=manifest_ref, source=source, native=native)
