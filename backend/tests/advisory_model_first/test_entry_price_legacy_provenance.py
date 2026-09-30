import json
from copy import deepcopy

import pytest

from backend.services.advisory_model_first.entry_price_confirmation_contracts import build_entry_price_confirmation_request
from backend.services.advisory_model_first.entry_price_legacy_provenance import load_legacy_exploratory_inputs
from backend.services.advisory_model_first.entry_price_service import _frame_sha256
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.price_range_contracts import canonical_json_sha256
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_entry_price_confirmation_contracts import request_values
from backend.tests.advisory_model_first.test_entry_price_service import D, T, integrated_service


def legacy_inputs(tmp_path, *, matched=True, violation=None):
    service, feature_source, args = integrated_service()
    prepared = service.prepare_day(**args)
    programs = service._programs
    detail = programs.recommendation_list_version_detail("list-1")
    version = detail["list_version"]
    version["program_id"] = args["program_id"]
    version["summary_json"] = {"selection_run_id": "selection-1", "advisory_date_context": {
        "decision_as_of_trade_date": D.isoformat(), "target_trade_date": T.isoformat()}}
    programs.recommendation_list_version_detail = lambda _id: deepcopy(detail)
    run = service._reader._selection_service.run
    payload = {"manifest_sha256": args["scope"].package_manifest_sha256}
    dse = dict(package_id=args["scope"].package_id, manifest_sha256=payload["manifest_sha256"],
               cutoff_date=D.isoformat(), target_trade_date=T.isoformat(), evidence_payload_json=payload,
               artifact_hash=canonical_json_sha256(payload), evidence_id="dse-1")
    run.runtime_config = {"daily_selection_evidence": {"artifact_hash_by_package": {
        dse["package_id"]: dse["artifact_hash"]}, "evidence_ids_by_package": {dse["package_id"]: "dse-1"}}}
    source = dict(selection_run=dict(run_id=run.run_id, trade_date=T.isoformat(), package_ids=run.package_ids,
                                     runtime_config=deepcopy(run.runtime_config), status="SUCCEEDED"),
                  list_version=deepcopy(version), list_items=deepcopy(detail["items"]),
                  review_run=dict(review_run_id="review-1", selection_run_id="selection-1",
                                  selection_run_ids=["selection-1"], program_id=args["program_id"],
                                  binding_version_id=args["binding_version_id"], trade_date=T.isoformat()),
                  daily_selection_evidence=[dse])
    values = request_values(tmp_path, formal=False, dates=1)
    values.update(program_id=args["program_id"], binding_version_id=args["binding_version_id"])
    values["days"] = [dict(decision_as_of_trade_date=D.isoformat(), target_trade_date=T.isoformat(),
                          list_version_id="list-1", review_run_id="review-1", selection_run_id="selection-1",
                          candidate_symbols=prepared.candidates.instrument.tolist(),
                          candidate_source_sha256=_frame_sha256(prepared.candidates),
                          historical_universe_receipt_sha256="d" * 64)]
    values["target_calendar"] = [T.isoformat()]
    # JSON fixtures retain the original fields; the source-plan digest pins the
    # obsolete summary reference without turning it into a native receipt.
    plan = json.loads(json.dumps(values, default=lambda x: x.model_dump(mode="json")
                                 if hasattr(x, "model_dump") else x.isoformat()))
    (tmp_path / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
    source_path = tmp_path / "sources" / f"{T}.json"
    source_path.parent.mkdir()
    if violation == "dse":
        source["daily_selection_evidence"][0]["artifact_hash"] = "f" * 64
    if violation == "policy":
        source["list_items"][0]["evidence_json"]["review_policy_sha256"] = "f" * 64
    if violation == "review":
        source["review_run"]["selection_run_ids"] = ["different"]
    source_path.write_text(json.dumps(source), encoding="utf-8")
    members = ["000001.SZ", "000002.SZ"]
    member_sha = canonical_json_sha256(members)
    comparison = dict(matches_historical_hash=matched, historically_witnessed_hashes=[member_sha],
                      observed_member_sha256=member_sha, observed_member_count=2)
    meta = dict(target_trade_date=T.isoformat(), decision_as_of_trade_date=D.isoformat(),
                list_version_id="list-1", review_run_id="review-1", selection_run_id="selection-1",
                frozen_candidate_symbols_from_plan=plan["days"][0]["candidate_symbols"],
                candidate_source_sha256_declared_by_plan=plan["days"][0]["candidate_source_sha256"],
                native_receipt=None, source_projection_path=f"sources/{T}.json",
                source_projection_canonical_sha256=canonical_json_sha256(source),
                existing_rolling_spans_diagnostic_comparison=comparison)
    if violation == "candidate":
        meta["candidate_source_sha256_declared_by_plan"] = "f" * 64
    if violation == "source_hash":
        meta["source_projection_canonical_sha256"] = "f" * 64
    if violation == "escape":
        meta["source_projection_path"] = "../../outside.json"
    if matched:
        (source_path.parent / f"{T}-content-matched-members.json").write_text(json.dumps(dict(
            members=members, members_sha256=member_sha, decision_date=D.isoformat(),
            not_native_receipt=True, observed_now_not_historical_capture_time=True)), encoding="utf-8")
    if violation == "members":
        comparison["observed_member_count"] = 3
    refs = {"source_plan": evidence_reference_for_file(tmp_path / "plan.json", role="plan")}
    evidence = dict(schema_version="local_data_historical_selection_identity_evidence_v1",
                    not_a_native_historical_universe_receipt=True, not_a_replay_spec_or_approval=True,
                    source_plan_sha256=refs["source_plan"].sha256, days=[meta],
                    program_id=args["program_id"], binding_version_id=args["binding_version_id"],
                    package_id=args["scope"].package_id, package_manifest_sha256=payload["manifest_sha256"],
                    review_policy_sha256_historically_witnessed=args["scope"].review_policy_sha256)
    (tmp_path / "evidence.json").write_text(json.dumps(evidence), encoding="utf-8")
    refs["identity_evidence"] = evidence_reference_for_file(tmp_path / "evidence.json", role="evidence")
    handoff = dict(schema_version="local_data_bug1640_legacy_consumer_owner_handoff_v1",
                   existing_evidence_inputs=[dict(path=str(tmp_path / "evidence.json"),
                                                 file_sha256=refs["identity_evidence"].sha256)],
                   data_readback=dict(targeted_legacy_dates=1, total_plan_dates=1,
                                      exact_content_matched_member_sets=int(matched),
                                      unproven_full_member_dates=[] if matched else [T.isoformat()],
                                      native_receipts_recovered=0))
    if violation == "handoff":
        handoff["data_readback"]["native_receipts_recovered"] = 1
    (tmp_path / "handoff.json").write_text(json.dumps(handoff), encoding="utf-8")
    refs["consumer_handoff"] = evidence_reference_for_file(tmp_path / "handoff.json", role="handoff")
    request = build_entry_price_confirmation_request(**dict(plan, legacy_provenance=refs))
    args["role_binding_sha256"] = request.request_sha256
    return request, service, args, feature_source


@pytest.mark.parametrize("matched", [True, False])
def test_approved_legacy_prepare_keeps_frozen_roster_and_evidence_grade(tmp_path, matched):
    request, service, args, features = legacy_inputs(tmp_path, matched=matched)
    legacy = load_legacy_exploratory_inputs(request)
    prepared = service.prepare_day(**args, legacy_provenance=legacy)
    assert tuple(prepared.candidates.instrument) == request.days[0].candidate_symbols
    assert features.calls == 0
    assert prepared.provenance_identity["full_member_content_match"] is matched
    assert not prepared.provenance_identity["native_receipt_complete"]
    assert not prepared.provenance_identity["full_universe_historical_capture_proven"]
    assert not prepared.provenance_identity["capture_time_backfilled"]
    assert prepared.provenance_identity["source_plan_historical_summary_ref"] == "d" * 64


@pytest.mark.parametrize("violation", ["dse", "policy", "review", "candidate", "source_hash", "escape", "members", "handoff"])
def test_legacy_source_chain_fails_closed(tmp_path, violation):
    request, *_ = legacy_inputs(tmp_path, violation=violation)
    with pytest.raises(AdvisoryModelFirstError):
        load_legacy_exploratory_inputs(request)


@pytest.mark.parametrize("violation", ["file_hash", "changed_plan", "run", "list", "native_receipt", "added_native", "natural", "authority"])
def test_authority_is_explicit_request_bound_and_does_not_replace_native_identity(tmp_path, violation):
    request, service, args, _ = legacy_inputs(tmp_path)
    if violation == "file_hash":
        (tmp_path / "evidence.json").write_text("{}", encoding="utf-8")
    elif violation == "changed_plan":
        values = request.functional_payload()
        values["replay_as_of"] = "2026-09-27"
        request = build_entry_price_confirmation_request(**values)
    if violation in {"file_hash", "changed_plan"}:
        with pytest.raises(AdvisoryModelFirstError):
            load_legacy_exploratory_inputs(request)
        return
    legacy = load_legacy_exploratory_inputs(request)
    if violation == "run":
        service._reader._selection_service.run.runtime_config["changed"] = True
    elif violation in {"list", "native_receipt", "added_native"}:
        get = service._programs.recommendation_list_version_detail
        def changed(key):
            value = get(key)
            if violation == "list":
                value["items"][0]["evidence_json"]["extra"] = True
            else:
                value["list_version"]["summary_json"]["advisory_universe_receipt"] = (
                    {"universe_selection": {"mode": "stock_universe", "pool_ids": []}}
                    if violation == "added_native" else {})
            return value
        service._programs.recommendation_list_version_detail = changed
    elif violation == "authority":
        args["role_binding_sha256"] = "f" * 64
    else:
        from datetime import datetime, timezone
        args["capture_as_of"] = datetime.now(timezone.utc)
    with pytest.raises(AdvisoryModelFirstError):
        service.prepare_day(**args, legacy_provenance=legacy)


def test_missing_authority_remains_blocked(tmp_path):
    _, service, args, _ = legacy_inputs(tmp_path)
    with pytest.raises(AdvisoryModelFirstError, match="universe"):
        service.prepare_day(**args)


def test_authority_parses_the_exact_bytes_whose_digest_was_verified(tmp_path, monkeypatch):
    from pathlib import Path
    from backend.services.advisory_model_first.entry_price_legacy_provenance import _read_reference
    request, *_ = legacy_inputs(tmp_path)
    # A different version on a second read must not become the authorized plan.
    monkeypatch.setattr(Path, "read_text", lambda *_args, **_kwargs: '{"foreign":true}')
    plan = _read_reference(request.legacy_provenance.source_plan)
    assert plan["program_id"] == request.program_id
    assert "foreign" not in plan
