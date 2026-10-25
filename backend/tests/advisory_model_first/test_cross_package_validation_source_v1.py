from datetime import date
from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first.cross_package_validation_source_v1 import (
    NoArtifactWrites, ReadonlyCopyAssetConsumer, prepare_package_dates, validate_signal_artifact,
)


D = date(2026, 3, 11)
PACKAGE = dict(package_id="pkg_test", manifest_sha256="a"*64, portfolio_topk=25)


def packet(n=51):
    return dict(package_id=PACKAGE["package_id"], manifest_sha256=PACKAGE["manifest_sha256"],
        trade_date=D.isoformat(), data_source="DB_HISTORICAL", score_count=n, universe_count=100,
        metadata=dict(score_trade_date=D.isoformat(), cutoff_date=D.isoformat()),
        scores_json=[dict(symbol=f"{i:06d}.SZ", score=100.-i, rank=i) for i in range(1, n+1)])


def idle():
    return dict(active_counts=dict(single=0, custom_evo=0, multi_alpha=0))


def test_raw_scores_and_fixed_candidate_view_not_portfolio_topk(tmp_path):
    calls = []
    def prepare(**kwargs):
        calls.append(kwargs)
        return [packet()]
    result = prepare_package_dates(service=SimpleNamespace(prepare_from_live_inference_dates=prepare), package=PACKAGE,
        decision_dates=[D], output_root=tmp_path, observe_resources=idle)[0]
    assert len(result["artifact"]["scores_json"]) == 51
    assert len(result["candidate_view"]) == 50
    assert calls[0]["cutoff_date"] == D and calls[0]["trade_dates"] == [D]
    assert calls[0]["historical_read_only"] is True and calls[0]["include_reference_price"] is False
    assert calls[0]["runtime_config"]["runtime_profile"]["selection"]["top_k"] == 50
    # Checkpoint reuse must never invoke provider/save again.
    prepare_package_dates(service=None, package=PACKAGE, decision_dates=[D], output_root=tmp_path, observe_resources=None)
    assert len(calls) == 1


def test_unknown_or_shared_resource_is_not_zero_candidates(tmp_path):
    service = SimpleNamespace(prepare_from_live_inference_dates=lambda **kw: (_ for _ in ()).throw(ValueError("input wrong")))
    result = prepare_package_dates(service=service, package=PACKAGE, decision_dates=[D], output_root=tmp_path, observe_resources=idle)[0]
    assert result["status"] == "INPUT_UNAVAILABLE" and "artifact" not in result
    assert prepare_package_dates(service=None, package=PACKAGE, decision_dates=[D], output_root=tmp_path/"busy",
        observe_resources=lambda: dict(active_counts=dict(single=1, custom_evo=0, multi_alpha=0))) == []
    with pytest.raises(ValueError, match="writes"):
        NoArtifactWrites().save({})


def test_disjoint_cpu_capacity_allows_readonly_inference_but_not_fail_open(tmp_path):
    service = SimpleNamespace(prepare_from_live_inference_dates=lambda **kw:[packet()],_advisory_local_cpu_only=True)
    resources = dict(active_counts=dict(single=0,custom_evo=1,multi_alpha=0),readonly_local_cpu_safe=True)
    result = prepare_package_dates(service=service, package=PACKAGE, decision_dates=[D], output_root=tmp_path/"safe",
        observe_resources=lambda:resources)
    assert result[0]["status"] == "PREPARED" and result[0]["physical_fit_count"] == 0
    insufficient = dict(active_counts=dict(single=0,custom_evo=0,multi_alpha=0),readonly_local_cpu_safe=False)
    assert prepare_package_dates(service=service,package=PACKAGE,decision_dates=[D],output_root=tmp_path/"low",
        observe_resources=lambda:insufficient) == []
    service._advisory_local_cpu_only = False
    with pytest.raises(ValueError,match="CPU-only"):
        prepare_package_dates(service=service,package=PACKAGE,decision_dates=[D],output_root=tmp_path/"gpu",
            observe_resources=lambda:resources)


def test_public_multi_alpha_remote_node_capacity_not_a_global_training_gate(tmp_path, monkeypatch):
    import subprocess
    import psutil
    import backend.mcp.common as common
    import backend.services.advisory_model_first.generic_population_price_5td_cli_v1 as existing
    import backend.services.advisory_model_first.cross_package_validation_source_v1 as consumer
    from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import canonical_sha
    counts = dict(single=0, custom_evo=1, multi_alpha=1)
    monkeypatch.setattr(existing, "public_qe_observation_v1", lambda *a, **kw:dict(active_counts=counts))
    monkeypatch.setattr(psutil, "net_if_addrs", lambda:{})
    monkeypatch.setattr(psutil, "virtual_memory", lambda:SimpleNamespace(available=40*1024**3))
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw:SimpleNamespace(stdout="Mem: 64000000000 0 0 0 0 52000000000\n"))
    monkeypatch.setattr(consumer, "real_root", lambda *a, **kw:tmp_path)
    run = dict(id="original_run", backtest_config_json=dict(node_id="remote"),
        execution_identity_json=dict(dataset=dict(resolved_node_id="remote")),node_parallelism_json={"remote":1})
    calls, host = [], ["192.0.2.55"]
    runs = [run,{**run,"id":"other_remote_run"}]
    def get(path, params=None):
        calls.append(path)
        if path == "/dispatch/nodes/remote":
            return dict(api_base_url="http://"+host[0]+":8000")
        if path == "/dispatch/nodes/shared":
            return dict(api_base_url="http://127.0.0.1:8000")
        running = params["status"] == "running"
        if "evolution/tasks" in path:
            return dict(status="success",data=[dict(task_id="evo",node_id="remote")] if running else [])
        return dict(status="success",data=dict(count=len(runs) if running else 0,runs=runs if running else []))
    monkeypatch.setattr(common, "AIstockApiClient", lambda *a, **kw:SimpleNamespace(get=get))
    before = canonical_sha(run)
    result = consumer.local_cpu_resource_observation(api_base="readonly_api",temp_root=tmp_path)
    assert result["readonly_local_cpu_safe"] and result["qe_nodes"] == [dict(node_id="remote",disjoint_host=True)]
    assert result["active_counts"]["multi_alpha"] == 2  # limit=1 is not the global total.
    assert canonical_sha(run) == before and all(path.startswith("/") for path in calls)
    # A declared additional local worker or unresolved host may not be ignored.
    run["node_parallelism_json"]["shared"] = 1
    assert not consumer.local_cpu_resource_observation(api_base="readonly_api",temp_root=tmp_path)["readonly_local_cpu_safe"]
    run["node_parallelism_json"].pop("shared")
    host[0] = "unresolved-node-name"
    assert not consumer.local_cpu_resource_observation(api_base="readonly_api",temp_root=tmp_path)["readonly_local_cpu_safe"]
    host[0] = "192.0.2.55"
    runs.extend({**run,"id":str(i)} for i in range(8))
    with pytest.raises(ValueError,match="complete multi-alpha"):
        consumer.local_cpu_resource_observation(api_base="readonly_api",temp_root=tmp_path)


def test_future_cutoff_and_foreign_or_duplicate_candidates_are_rejected():
    wrong = packet()
    wrong["metadata"]["cutoff_date"] = "2026-03-12"
    with pytest.raises(ValueError, match="cutoff"):
        validate_signal_artifact(wrong, package_id=PACKAGE["package_id"], manifest_sha256=PACKAGE["manifest_sha256"], decision_date=D)


def test_readonly_asset_adapter_checks_original_and_copy_identity(tmp_path, monkeypatch):
    import hashlib
    import backend.services.advisory_model_first.cross_package_validation_source_v1 as source
    monkeypatch.setattr(source, "real_root", lambda value, **kwargs: tmp_path)
    payload = b"original frozen weight"
    digest = hashlib.sha256(payload).hexdigest()
    calls = []
    store = SimpleNamespace(get=lambda uri: payload, verify=lambda uri, **kw: calls.append((uri, kw)))
    consumer = ReadonlyCopyAssetConsumer(store, temp_root=tmp_path)
    consumer.materialize_file("frozen-uri", tmp_path/"weight.bin", sha256=digest, size_bytes=len(payload))
    assert (tmp_path/"weight.bin").read_bytes() == payload and len(calls) == 1
    with pytest.raises(ValueError, match="escapes"):
        consumer.materialize_file("frozen-uri", tmp_path.parent/"outside.bin", sha256=digest, size_bytes=len(payload))
    with pytest.raises(ValueError, match="hash differs"):
        consumer.materialize_file("frozen-uri", tmp_path/"bad.bin", sha256="a"*64, size_bytes=len(payload))
    wrong = packet()
    wrong["scores_json"][1]["symbol"] = wrong["scores_json"][0]["symbol"]
    with pytest.raises(ValueError, match="keys"):
        validate_signal_artifact(wrong, package_id=PACKAGE["package_id"], manifest_sha256=PACKAGE["manifest_sha256"], decision_date=D)


def test_stored_selection_uses_score_D_not_artifact_T_and_preserves_short_roster():
    from backend.services.advisory_model_first.cross_package_validation_source_v1 import read_selection_source_view
    from backend.services.strategy_package.runtime_variant import canonical_json_sha256
    scores = packet(3)["scores_json"]
    digest = canonical_json_sha256(scores)
    artifact = SimpleNamespace(artifact_id="ssa_original", artifact_sha256=digest, scores_json=scores,
        trade_date=date(2026, 3, 12), score_count=3, runtime_config_hash="original_policy",
        metadata=dict(score_trade_date=str(D), cutoff_date=str(D)))
    metadata = {**PACKAGE, "artifact_id": artifact.artifact_id, "artifact_sha256": digest,
        "trade_date": str(artifact.trade_date), "decision_date": str(D), "score_count": 3,
        "runtime_config_hash": artifact.runtime_config_hash, "data_source": "DB_HISTORICAL"}
    repository = SimpleNamespace(get=lambda **kwargs: artifact)
    frame, receipt = read_selection_source_view(package=PACKAGE, selected=[metadata], decision_dates=[D], repository=repository)
    assert frame.decision_date.tolist() == [D]*3 and frame["rank"].tolist() == [1, 2, 3]
    assert receipt["original_source_bindings"][0]["execution_date"] == "2026-03-12"
    assert receipt["original_N_preserved"] and not receipt["native_receipt_created"]
    artifact.metadata["cutoff_date"] = "2026-03-12"
    with pytest.raises(ValueError, match="provenance"):
        read_selection_source_view(package=PACKAGE, selected=[metadata], decision_dates=[D], repository=repository)


def test_current_canary_is_not_promoted_to_old_capture_and_keeps_missing_days(tmp_path):
    import json
    from backend.services.advisory_model_first.cross_package_validation_source_v1 import read_current_canary_view
    package = {**PACKAGE,"run_id":"frozen_parent"}
    service = SimpleNamespace(prepare_from_live_inference_dates=lambda **kw:[packet(3)])
    prepare_package_dates(service=service,package=package,decision_dates=[D],output_root=tmp_path,observe_resources=idle)
    later = date(2026,3,12)
    frame,receipt = read_current_canary_view(package=package,source_root=tmp_path,decision_dates=[D,later])
    assert len(frame) == 3 and receipt["missing_decision_dates"] == [str(later)]
    assert not receipt["native_receipt_created"] and not receipt["historical_original_receipt"]
    assert "CURRENT_DATABASE_NON_VINTAGE" in receipt["original_source"]
    path = tmp_path/package["package_id"]/(str(D)+".json")
    body = json.loads(path.read_text(encoding="utf-8"))
    body["candidate_view"][0]["score"] += 1
    path.write_text(json.dumps(body),encoding="utf-8")
    with pytest.raises(ValueError,match="checkpoint changed"):
        read_current_canary_view(package=package,source_root=tmp_path,decision_dates=[D,later])


def test_current_source_child_preserves_parent_axis_and_uses_a_separate_identity(tmp_path,monkeypatch):
    import json
    from backend.services.advisory_model_first import cross_package_validation_inputs_v1 as consumer
    from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import publish_json,file_sha
    package = {**PACKAGE,"run_id":"parent_run","disposition":"IN_MATRIX"}
    parent = tmp_path/"parent"
    publish_json(parent/"inventory.json",{"packages":[package]})
    publish_json(parent/"calendar.json",["2026-03-11","2026-03-12","2026-03-13"])
    plan = dict(experiment_id="parent_study",inventory_filename="inventory.json",calendar_filename="calendar.json",
        decision_dates=["2026-03-11","2026-03-12"],source_metadata_filename="old_metadata.json",source_metadata_sha256="b"*64)
    publish_json(parent/"plan.json",plan)
    original_sha = file_sha(parent/"plan.json")
    source = parent/"new_source"
    prepare_package_dates(service=SimpleNamespace(prepare_from_live_inference_dates=lambda **kw:[packet(3)]),
        package=package,decision_dates=[D],output_root=source,observe_resources=idle)
    monkeypatch.setattr(consumer,"checked_plan",lambda root:plan)
    registered = []
    monkeypatch.setattr(consumer,"register_frozen_plan",lambda root:registered.append(root))
    child,result = consumer.freeze_current_canary_study(parent,source_root=source,package_ids=[package["package_id"]])
    child_plan = json.loads((child/"plan.json").read_text(encoding="utf-8"))
    assert child_plan["decision_dates"] == plan["decision_dates"] and child_plan["parent_other_role_outcomes_already_seen"]
    assert child_plan["experiment_id"] != plan["experiment_id"] and file_sha(parent/"plan.json") == original_sha
    receipt = json.loads((child/"frozen_sources"/package["package_id"]/"receipt.json").read_text(encoding="utf-8"))
    assert result["prepared_package_count"] == 1 and registered == [child]
    assert receipt["missing_decision_dates"] == ["2026-03-12"] and not receipt["native_receipt_created"]
