from datetime import date
import hashlib
import io
import json

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_context_preparation_v1 as m
from backend.services.advisory_model_first.economic_context_consumer_v1 import EconomicContextSourceV1, _digest
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1, EconomicEntryStudyPlanV1, EconomicEntryTrainingConfigurationV1
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control import evidence_reference_for_file, research_policy_identity
from backend.services.advisory_model_first.research_control_contracts import build_window_contract
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
from backend.tests.advisory_model_first.test_policy_contracts import _request


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    request = _request(decision_date_start="2026-01-02", decision_date_end="2026-01-05", data_cutoff="2026-01-06")
    keys = pd.DataFrame([("2026-01-02", "2026-01-05", "000001.SZ"), ("2026-01-05", "2026-01-06", "000001.SZ")], columns=KEY)
    ranks = keys.assign(selection_effective_rank=1, is_candidate_decision=True)
    def parquet(frame):
        handle = io.BytesIO()
        frame.to_parquet(handle, index=False)
        return handle.getvalue()
    rank_bytes, request_bytes = parquet(ranks), request.model_dump_json().encode()
    dataset = tmp_path/"dataset.json"
    dataset.write_text(json.dumps({"policy_dataset_bundle_id": "a"*64,
        **{field: getattr(request, field) for field in ("program_id", "binding_version_id", "package_id", "manifest_sha256",
            "request_id", "request_sha256", "selection_runtime_semantics_hash", "shadow_policy_sha256", "cost_policy_sha256")},
        "files": {"request.json": {"sha256": hashlib.sha256(request_bytes).hexdigest()},
            "candidate_rankings.parquet": {"sha256": hashlib.sha256(rank_bytes).hexdigest()}}}), encoding="utf-8")
    policy_identity = research_policy_identity(baseline_policy_sha256=request.baseline_policy_sha256,
        shadow_policy_sha256=request.shadow_policy_sha256, cost_policy_sha256=request.cost_policy_sha256)
    contract = build_window_contract(package_id=request.package_id, manifest_sha256=request.manifest_sha256,
        runtime_semantics_hash=request.selection_runtime_semantics_hash, baseline_policy_sha256=request.baseline_policy_sha256,
        shadow_policy_sha256=request.shadow_policy_sha256, cost_policy_sha256=request.cost_policy_sha256,
        source_policy="PIT_DAILY_PARENT_PREDICTIONS_AND_MARKET_OUTCOMES_V1", artifact_root_uri=tmp_path.as_posix(),
        sealed_consumption_receipt_uri=(tmp_path/"sealed_holdout_consumption_receipt.json").as_posix(), windows=(
            dict(window_id="dev", dataset_identity="a"*64, start_date=date(2026, 1, 2), end_date=date(2026, 1, 6),
                state="DEVELOPMENT_CONSUMED", purpose="consumed"),
            dict(window_id="test", dataset_identity="b"*64, start_date=date(2026, 1, 5), end_date=date(2026, 1, 6),
                state="FROZEN_TEST_CONSUMED", purpose="consumed"),
            dict(window_id="replay", dataset_identity="c"*64, start_date=date(2025, 1, 1), end_date=date(2025, 2, 1),
                state="HISTORICAL_REPLAY_CONSUMED", purpose="consumed"),
            dict(window_id="sealed", dataset_identity="d"*64, start_date=date(2026, 8, 1), end_date=date(2026, 9, 1),
                state="SEALED_UNCONSUMED", purpose="sealed"),))
    contract_path = tmp_path/"window.json"
    contract_path.write_text(contract.model_dump_json(), encoding="utf-8")
    plan = EconomicEntryStudyPlanV1(configuration=EconomicEntryTrainingConfigurationV1(train_start=date(2026, 1, 2),
        train_end=date(2026, 1, 2), validation_start=date(2026, 1, 3), validation_end=date(2026, 1, 3),
        test_start=date(2026, 1, 5), test_end=date(2026, 1, 5), label_cutoff=date(2026, 1, 6), downside_budget_bps=800),
        dataset_identity="a"*64, dataset_manifest_ref=evidence_reference_for_file(dataset, role="dataset"),
        feature_ref=evidence_reference_for_file(dataset, role="features"), window_contract_ref=evidence_reference_for_file(contract_path, role="window"),
        policy_identity=policy_identity, implementation_sha256="b"*64, parent_lineage=("parent",))
    identity = EconomicEntryInputIdentityV1(dataset_manifest_sha256=plan.dataset_manifest_ref.sha256,
        **{field: getattr(request, field) for field in ("program_id", "binding_version_id", "package_id", "manifest_sha256",
            "request_id", "request_sha256", "selection_runtime_semantics_hash", "shadow_policy", "shadow_policy_sha256", "cost_policy", "cost_policy_sha256")},
        universe_identity_sha256=sha({"historical_definition": request.selection_runtime_semantics}),
        candidate_roster_sha256=candidate_roster_sha256(ranks), price_coordinate_sha256="c"*64, price_source_sha256="d"*64,
        reference_source_sha256="d"*64, source_evidence="RECOVERED_LIMITED", evidence_limitations=("native unknown",))
    registered = publish_stage(study_root=tmp_path/"study", stage="preregistered", plan_sha256=plan.plan_sha256,
        parent_sha256=None, artifacts={"plan.json": plan.model_dump_json().encode()})
    target = publish_stage(study_root=tmp_path/"study", stage="prepared", plan_sha256=plan.plan_sha256,
        parent_sha256=json.loads((registered/"manifest.json").read_text())["stage_sha256"], artifacts={
            "identity.json": identity.model_dump_json().encode(), "frozen_request.json": request_bytes,
            "features.parquet": parquet(keys), "frozen_rankings.parquet": rank_bytes, "labels.json": b"must-not-read"})
    source = EconomicContextSourceV1({"cutoff": "2026-01-06", "universe_selection": {"mode": "stock_universe", "pool_ids": []}, "sector": None},
        {"stock_universe": {"000001.SZ": [(date(2026, 1, 1), date(2026, 1, 6))]}}, None, None, {})
    monkeypatch.setattr(m, "load_economic_context_source_v1", lambda **kw: source)
    return dict(profile_path=dataset, profile_sha256=_digest(dataset), prepared_manifest_path=target/"manifest.json",
        prepared_manifest_sha256=_digest(target/"manifest.json"), universe_selection={"mode": "stock_universe", "pool_ids": []}), keys


def test_executable_prepare_preserves_keys_and_evidence_without_reading_outcomes(prepared, monkeypatch, capsys):
    args, keys = prepared
    original, columns = pd.read_parquet, []
    def read(path, **kw):
        assert path.name in {"features.parquet", "frozen_rankings.parquet"} and "columns" in kw
        columns.extend(kw["columns"])
        return original(path, **kw)
    monkeypatch.setattr(pd, "read_parquet", read)
    rows, summary = m.prepare_economic_context_v1(**args)
    assert rows.instrument.tolist() == keys.instrument.tolist() and summary["rows"] == 2
    assert summary["frozen_parent"]["source_evidence"] == "RECOVERED_LIMITED"
    assert summary["frozen_parent"]["current_pool_is_original_native_identity"] is False
    assert summary["core_input_identity"] == "VERIFIED" and summary["optional_classification"] == "UNAVAILABLE"
    assert set(columns) == {*KEY, "selection_effective_rank", "is_candidate_decision"}
    assert m.main(["--profile", str(args["profile_path"]), "--profile-sha256", args["profile_sha256"],
        "--prepared-manifest", str(args["prepared_manifest_path"]), "--prepared-manifest-sha256", args["prepared_manifest_sha256"],
        "--universe-selection", json.dumps(args["universe_selection"])]) == 0
    assert json.loads(capsys.readouterr().out)["fit_count"] == 0


@pytest.mark.parametrize("defect", ["hash", "request", "population", "parent_chain", "sealed"])
def test_prepare_cannot_hide_tampered_identity_population_or_sealed_access(prepared, monkeypatch, defect):
    args, _ = prepared
    path = args["prepared_manifest_path"]
    manifest = json.loads(path.read_text())
    if defect == "sealed":
        def denied(plan):
            raise AdvisoryModelFirstError("sealed holdout", reason_code="DENIED")
        monkeypatch.setattr(m, "_authorize", denied)
        monkeypatch.setattr(pd, "read_parquet", lambda *a, **kw: pytest.fail("must authorize before reading candidates"))
    elif defect == "parent_chain":
        manifest["parent_sha256"] = "f"*64
    else:
        name = "identity.json" if defect in {"hash", "request"} else "features.parquet"
        file = path.parent/name
        if defect == "population":
            pd.read_parquet(file).iloc[:1].to_parquet(file, index=False)
        else:
            identity = json.loads(file.read_text())
            identity["package_id"] = "different"
            file.write_text(json.dumps(identity), encoding="utf-8")
        if defect != "hash":
            manifest["files"][name] = {"sha256": _digest(file), "size_bytes": file.stat().st_size}
    if defect not in {"hash", "sealed"}:
        manifest.pop("stage_sha256")
        manifest["stage_sha256"] = sha(manifest)
        path.write_text(json.dumps(manifest), encoding="utf-8")
        args["prepared_manifest_sha256"] = _digest(path)
    with pytest.raises(AdvisoryModelFirstError):
        m.prepare_economic_context_v1(**args)
