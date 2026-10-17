"""Explicit, non-native authority for an unchanged approved exploratory plan only."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from .entry_price_confirmation_contracts import build_entry_price_confirmation_request
from .errors import AdvisoryModelFirstError
from .price_range_contracts import canonical_json_sha256


def _fail(message):
    raise AdvisoryModelFirstError(message, reason_code="ADVISORY_ENTRY_LEGACY_PROVENANCE_MISMATCH")


def _read_reference(reference):
    raw = Path(reference.artifact_uri).read_bytes()
    if (hashlib.sha256(raw).hexdigest(), len(raw)) != (reference.sha256, reference.size_bytes):
        _fail("legacy evidence file/hash differs from the approved reference")
    return json.loads(raw.decode("utf-8"))


def _child(root, relative):
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        _fail("legacy evidence source path escapes its evidence directory")
    return json.loads(path.read_text(encoding="utf-8"))


@dataclass(frozen=True)
class LegacyExploratoryInputs:
    request: object
    sources: dict
    metadata: dict

    def identity(self, target):
        day = self.metadata[target]
        matched = day["existing_rolling_spans_diagnostic_comparison"]["matches_historical_hash"]
        return {"kind": "RECOVERED_NON_NATIVE_LEGACY", "native_receipt_complete": False,
                "full_member_content_match": matched,
                "full_universe_historical_capture_proven": False,
                "capture_time_backfilled": False,
                "source_plan_historical_summary_ref": day.get("source_plan_historical_summary_ref"),
                "source_plan_summary_ref_is_native": False,
                "source_projection_sha256": day["source_projection_canonical_sha256"],
                "decision_use": "NAVIGATION_ONLY"}

    def validate_live_day(self, *, version, items, selection, decision, target, candidates, request_sha256):
        from .entry_price_service import _frame_sha256
        if request_sha256 != self.request.request_sha256 or target not in self.sources:
            _fail("legacy authority belongs to another request or date")
        source, meta = self.sources[target], self.metadata[target]
        expected = source["selection_run"]
        if (selection.run_id != expected["run_id"] or selection.trade_date.isoformat() != expected["trade_date"]
                or list(selection.package_ids) != expected["package_ids"]
                or selection.runtime_config != expected["runtime_config"]):
            _fail("live persisted Selection identity/config differs from frozen legacy source")
        for key in ("list_version_id", "review_run_id", "program_id", "binding_version_id", "summary_json"):
            if version.get(key) != source["list_version"].get(key):
                _fail("live list/review identity differs from frozen legacy source")
        keys = ("list_item_id", "symbol", "action", "rank", "score", "item_state", "evidence_json")
        def project(rows):
            return sorted([{k: row.get(k) for k in keys} for row in rows],
                          key=lambda row: str(row["list_item_id"]))
        if project(items) != project(source["list_items"]):
            _fail("live persisted list candidates differ from the frozen legacy source")
        if (decision.isoformat() != meta["decision_as_of_trade_date"]
                or tuple(candidates.instrument) != tuple(meta["frozen_candidate_symbols_from_plan"])
                or _frame_sha256(candidates) != meta["candidate_source_sha256_declared_by_plan"]):
            _fail("legacy D-1, frozen candidate roster or candidate frame hash differs")


def load_legacy_exploratory_inputs(request):
    refs = request.legacy_provenance
    if refs is None:
        return None
    if (request.study_type != "EXPLORATORY_SCREEN" or request.decision_use != "NAVIGATION_ONLY"
            or request.evidence_level != "HISTORICAL_REPLAY"
            or request.data_identity.qualification != "CONSUMED_OR_NON_VINTAGE"
            or request.scope.universe_selection.mode != "stock_universe"):
        _fail("legacy authority is not a native, natural, index or confirmation contract")
    plan = _read_reference(refs.source_plan)
    expected = build_entry_price_confirmation_request(**dict(plan, legacy_provenance=refs))
    if expected.request_sha256 != request.request_sha256:
        _fail("approved original plan was changed, reduced or given a different policy")
    evidence = _read_reference(refs.identity_evidence)
    handoff = _read_reference(refs.consumer_handoff)
    if (evidence.get("schema_version") != "local_data_historical_selection_identity_evidence_v1"
            or evidence.get("not_a_native_historical_universe_receipt") is not True
            or evidence.get("not_a_replay_spec_or_approval") is not True
            or evidence.get("source_plan_sha256") != refs.source_plan.sha256
            or handoff.get("schema_version") != "local_data_bug1640_legacy_consumer_owner_handoff_v1"
            or not any(row.get("file_sha256") == refs.identity_evidence.sha256
                       and Path(row.get("path", "")).resolve() == Path(refs.identity_evidence.artifact_uri).resolve()
                       for row in handoff.get("existing_evidence_inputs", []))):
        _fail("legacy evidence/handoff does not bind the exact original plan and authority")
    for key, value in (("program_id", request.program_id), ("binding_version_id", request.binding_version_id),
                       ("package_id", request.scope.package_id),
                       ("package_manifest_sha256", request.scope.package_manifest_sha256),
                       ("review_policy_sha256_historically_witnessed", request.scope.review_policy_sha256)):
        if evidence.get(key) != value:
            _fail("legacy authority package/Program/policy identity differs")
    sources, metadata = {}, {}
    planned = {day.target_trade_date.isoformat(): day for day in request.days}
    original_days = {day["target_trade_date"]: day for day in plan["days"]}
    root = Path(refs.identity_evidence.artifact_uri).parent
    for meta in evidence["days"]:
        target = meta["target_trade_date"]
        if target not in planned or target in metadata or meta.get("native_receipt") is not None:
            _fail("legacy authority has foreign/duplicate dates or fabricated native receipt")
        day = planned[target]
        if any(meta.get(key) != getattr(day, key) for key in ("list_version_id", "review_run_id", "selection_run_id")):
            _fail("legacy run/list/review differs from frozen original plan")
        if (meta["decision_as_of_trade_date"] != day.decision_as_of_trade_date.isoformat()
                or tuple(meta["frozen_candidate_symbols_from_plan"]) != day.candidate_symbols
                or meta["candidate_source_sha256_declared_by_plan"] != day.candidate_source_sha256):
            _fail("legacy D-1 or candidate identity differs from frozen original plan")
        source = _child(root, meta["source_projection_path"])
        if canonical_json_sha256(source) != meta["source_projection_canonical_sha256"]:
            _fail("legacy source projection content hash differs")
        _validate_source(request, day, source)
        comparison = meta["existing_rolling_spans_diagnostic_comparison"]
        if comparison["matches_historical_hash"]:
            members = _child(root, f"sources/{target}-content-matched-members.json")
            if (len(members["members"]) != len(set(members["members"]))
                    or canonical_json_sha256(sorted(members["members"])) != members["members_sha256"]
                    or members["members_sha256"] not in comparison["historically_witnessed_hashes"]
                    or members["members_sha256"] != comparison["observed_member_sha256"]
                    or len(members["members"]) != comparison["observed_member_count"]
                    or members["decision_date"] != meta["decision_as_of_trade_date"]
                    or members.get("not_native_receipt") is not True
                    or members.get("observed_now_not_historical_capture_time") is not True):
                _fail("content-matched members do not prove the claimed historical content hash")
        target_date = day.target_trade_date
        metadata[target_date] = dict(meta, source_plan_historical_summary_ref=
                                    original_days[target].get("historical_universe_receipt_sha256"))
        sources[target_date] = source
    unproven = sorted(t.isoformat() for t in metadata if not
                      metadata[t]["existing_rolling_spans_diagnostic_comparison"]["matches_historical_hash"])
    readback = handoff["data_readback"]
    if (len(metadata) != readback["targeted_legacy_dates"]
            or len(request.days) != readback["total_plan_dates"]
            or len(metadata) - len(unproven) != readback["exact_content_matched_member_sets"]
            or unproven != sorted(readback["unproven_full_member_dates"])
            or readback["native_receipts_recovered"] != 0):
        _fail("legacy authority date population differs from delivered handoff")
    return LegacyExploratoryInputs(request, sources, metadata)


def _validate_source(request, day, source):
    from .model_inference import _validate_review_policy_identity
    run, version, review = source["selection_run"], source["list_version"], source["review_run"]
    if (run["run_id"] != day.selection_run_id or version["list_version_id"] != day.list_version_id
            or review["review_run_id"] != day.review_run_id or version["review_run_id"] != day.review_run_id
            or review["selection_run_id"] != day.selection_run_id
            or review["selection_run_ids"] != [day.selection_run_id]
            or run["trade_date"] != day.target_trade_date.isoformat()
            or run["status"] != "SUCCEEDED"
            or run["package_ids"] != [request.scope.package_id]):
        _fail("frozen source run/list/review/package chain differs")
    for obj in (version, review):
        if (obj["program_id"] != request.program_id or obj["binding_version_id"] != request.binding_version_id
                or obj["trade_date"] != day.target_trade_date.isoformat()):
            _fail("frozen source Program/binding/target differs")
    context = version["summary_json"]["advisory_date_context"]
    if (context["decision_as_of_trade_date"] != day.decision_as_of_trade_date.isoformat()
            or context["target_trade_date"] != day.target_trade_date.isoformat()
            or version["summary_json"]["selection_run_id"] != day.selection_run_id):
        _fail("frozen source D-1 or Selection linkage differs")
    _validate_review_policy_identity(list_items=source["list_items"], expected_symbols=day.candidate_symbols,
                                     review_policy_sha256=request.scope.review_policy_sha256)
    refs = run["runtime_config"]["daily_selection_evidence"]
    if not source["daily_selection_evidence"]:
        _fail("legacy source lacks DSE identity")
    for dse in source["daily_selection_evidence"]:
        payload = dse["evidence_payload_json"]
        if (dse["package_id"] != request.scope.package_id
                or dse["manifest_sha256"] != request.scope.package_manifest_sha256
                or payload["manifest_sha256"] != request.scope.package_manifest_sha256
                or dse["cutoff_date"] != day.decision_as_of_trade_date.isoformat()
                or dse["target_trade_date"] != day.target_trade_date.isoformat()
                or canonical_json_sha256(payload) != dse["artifact_hash"]
                or refs["artifact_hash_by_package"].get(dse["package_id"]) != dse["artifact_hash"]
                or refs["evidence_ids_by_package"].get(dse["package_id"]) != dse["evidence_id"]):
            _fail("legacy DSE/package/clock/hash differs from the original Selection")
