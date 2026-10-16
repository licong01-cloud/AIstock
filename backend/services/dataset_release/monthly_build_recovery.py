"""Recover only unpublished quote-policy packaging; never repeat price exports.

This maintenance entry prepares the existing operation for its ordinary API
resume. It does not run producers, replace SOURCE, publish READY or activate.
The old manifest, receipt and checkpoint remain preserved, including on crash.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

from .canonical import digest_named_fields
from .monthly_build_resume import _plain, _read_record, _signature
from .monthly_unified import MonthlyOperationStore, _digest, _exclusive_lock, _file_sha256 as _sha, _write_exclusive
from .shared_sector_context import validate_release_sw_l2_code_map
from .sw_l2_quote_policy import build_quote_availability_payload, quote_publication_policy_identity

LEGACY_POLICY_ID = "958c6438d6ec320b301e7272a94f6635293391dd04ed6dc5801a32421631e7f9"


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _json(path):
    before = _signature(path)
    value = json.loads(path.read_bytes())
    _require(before == _signature(path) and isinstance(value, dict), "recovery metadata changed")
    return value


def _pinned(root, path, receipt, published_root):
    relative = (published_root / path.relative_to(root)).relative_to(published_root.parent).as_posix()
    pins = [pin for pin in receipt["output_refs"] if pin.get("id") == relative]
    _require(len(pins) == 1, "recovery output pin is missing or ambiguous")
    before = _signature(path)
    _require(before[2] == pins[0]["size"] and _sha(path) == pins[0]["sha256"], "recovery output pin differs")
    _require(before == _signature(path), "recovery output changed")


def _source_proof(plan, cas, start):
    source = plan.get("monthly_industry_quote_source", {}).get("quote_source", {})
    _require(source.get("source_dataset") == "market.sw_daily", "official frozen quote source missing")
    partitions = [p for p in source["partitions"] if p["partition_key"] == start[:7]]
    _require(len(partitions) == 1, "official quote partition missing or ambiguous")
    ref = cas.validate_reference_metadata(partitions[0]["ref"])
    payload = cas.get_json_bounded(ref, max_bytes=ref.size)
    rows = [r for r in payload["rows"] if r.get("index_code") == "801011.SI" and r.get("trade_date") == start]
    _require(len(rows) == 1, "official quote proof missing or ambiguous")
    _require(all(math.isfinite(float(rows[0][field])) for field in ("pct_change", "vol", "amount")),
             "official quote proof is non-finite")
    return ref.sha256


def recover_unpublished_quote_policy(*, store, operation_id, cas, active_profile, apply=False):
    """Narrow BUG-1935 recovery, fenced by the same worker and state locks."""
    from datetime import date

    operation = store.operation_root(operation_id)
    _plain(operation)
    with _exclusive_lock(operation / ".writer.lock", blocking=False), _exclusive_lock(operation / ".operation.lock"):
        state, plan = store.read_state(operation_id), store.read_plan(operation_id)
        _require(state.get("status") == "FAILED" and state.get("current_stage") == "DERIVE",
                 "recovery requires an idle FAILED DERIVE operation")
        _require(not state.get("cancel_requested"), "cancelled operation cannot be recovered")
        for stage in ("DERIVE", "LOCAL_VALIDATE", "DEPLOY", "CONSUMER_VALIDATE"):
            _require(store.read_checkpoint(operation_id, stage) is None, "downstream checkpoint prevents repackaging")
        _require(store.read_checkpoint(operation_id, "SOURCE") is not None, "sealed SOURCE missing")
        for path in (operation / "receipts/ready.json", Path(plan["profile_candidate"])):
            _plain(path)
            _require(not path.exists(), "published profile prevents repackaging")
        active_signature = _signature(active_profile)
        active = _json(active_profile)
        _require(active.get("generation") != plan["generation"], "active generation cannot be repackaged")
        target = Path(plan["candidate_root"])
        _require(target.is_absolute(), "candidate root must be absolute")
        _plain(target)
        staging_parent = target.parent / ".staging"
        _plain(staging_parent)
        journals = list(staging_parent.glob(f".{target.name}.{operation_id}.resume-*.json"))
        _require(len(journals) == 1, "retained export journal missing or ambiguous")
        journal_sha = _sha(journals[0])
        journal = _json(journals[0])
        binding = journal["binding"]
        digest = digest_named_fields("aistock_monthly_build_data_binding_v1", binding)
        _read_record(journals[0], binding, digest)
        _require(binding.get("operation_id") == operation_id and binding.get("candidate_name") == target.name
                 and binding.get("predecessor") == plan["predecessor"]
                 and binding.get("target_cutoff") == plan["target_cutoff"]
                 and binding.get("release_id") == plan["release_id"], "retained operation binding differs")
        _require({"prepare", "qlib_dump_daily", "qlib_dump_minute", "finalize-bins"}.issubset(journal["steps"])
                 and journal.get("finalization_started") is True, "completed export steps missing")
        attempt = journal["materialization_attempt"]
        _require(type(attempt) is int and 0 < attempt <= state["attempt"], "materialization attempt differs")
        relative = f".staging/.{target.name}.{operation_id}.attempt-{attempt}.building"
        _require(journal["staging_relative_path"] == relative, "retained staging identity differs")
        staging = target.parent / relative
        _plain(staging)
        intent_path = operation / "quote-policy-repack.json"
        _plain(intent_path)
        old_intent = _json(intent_path) if intent_path.exists() else None
        checkpoint = operation / "checkpoints/build.json"
        saved_checkpoint = operation / f"checkpoints/build-before-quote-policy-{state['attempt']}.json"
        _plain(saved_checkpoint)
        if old_intent is None:
            _require(target.is_dir() and not staging.exists(), "candidate/staging is not exclusively owned")
            build = store.read_checkpoint(operation_id, "BUILD")
            _require(build is not None, "sealed BUILD missing")
        else:
            _require(old_intent.get("operation_id") == operation_id and old_intent.get("execution_attempt") == state["attempt"],
                     "recovery intent identity differs")
            _require(target.is_dir() != staging.is_dir(), "candidate/staging recovery state is ambiguous")
            if checkpoint.exists():
                build = store.read_checkpoint(operation_id, "BUILD")
            else:
                _require(_signature(saved_checkpoint) == old_intent["checkpoint_signature"], "preserved checkpoint differs")
                meta = _json(saved_checkpoint)["receipt"]
                receipt_path = operation / meta["path"]
                _require(_sha(receipt_path) == meta["sha256"], "preserved receipt differs")
                build = _json(receipt_path)
        checkpoint_meta = _json(checkpoint if checkpoint.exists() else saved_checkpoint)
        _require(checkpoint_meta.get("plan_digest") == _digest(plan)
                 and checkpoint_meta.get("request_digest") == store.read_request(operation_id)["semantic_digest"],
                 "sealed BUILD request or plan changed")
        root = target if target.exists() else staging
        _require(active.get("components", {}).get("dataset_manifest_sha256") != build["scope"]["dataset_manifest_sha256"],
                 "active manifest cannot be repackaged")
        manifest = root / "qe_dataset_manifest.json"
        component = root / "components/sector_context_candidate_v1"
        paths = [manifest, *(component / n for n in ("component_receipt.json", "sector_code_map.json", "sector_quote_availability.json"))]
        for path in paths:
            # BUILD refs contain the published root name, even after a move.
            _pinned(root, path, build, target)
        receipt = _json(component / "component_receipt.json")
        _require(receipt["quote_availability"]["publication_policy_identity"] == LEGACY_POLICY_ID,
                 "recovery is limited to the known erroneous publication policy")
        mapping = validate_release_sw_l2_code_map(_json(component / "sector_code_map.json"))
        old = _json(component / "sector_quote_availability.json")
        starts = [span["start_date"] for row in old["entries"] for span in row["availability_spans"]]
        _require(bool(starts), "quote authority has no publication window")
        start = min(starts)
        proposed = build_quote_availability_payload(code_map=mapping, required_start=date.fromisoformat(start),
                                                    cutoff=date.fromisoformat(plan["target_cutoff"]))
        before = {r["canonical_l2_code"]: r["availability_spans"] for r in old["entries"]}
        after = {r["canonical_l2_code"]: r["availability_spans"] for r in proposed["entries"]}
        _require(set(before) == set(after) and [code for code in before if before[code] != after[code]] == ["801011.SI"]
                 and before["801011.SI"] == [], "publication correction exceeds BUG-1935 scope")
        proof_sha = _source_proof(plan, cas, start)
        for relative_file, signature in journal["files"].items():
            path = Path(relative_file)
            _require(not path.is_absolute() and not any(s in relative_file for s in ("\\", ":", "\x00"))
                     and ".." not in path.parts, "retained export path escapes candidate")
            _require(_signature(root / path) == signature, f"retained export changed: {relative_file}")
        _require(_sha(journals[0]) == journal_sha and _signature(active_profile) == active_signature,
                 "journal or active profile changed during recovery")
        intent = {"schema_version": "aistock_unpublished_quote_policy_repack_v1", "operation_id": operation_id,
                  "execution_attempt": state["attempt"], "candidate_root": str(target), "staging_root": str(staging),
                  "journal_sha256": journal_sha, "manifest_file_sha256": _sha(manifest),
                  "checkpoint_signature": _signature(checkpoint if checkpoint.exists() else saved_checkpoint),
                  "old_policy_identity": LEGACY_POLICY_ID, "new_policy_identity": quote_publication_policy_identity(),
                  "official_source_partition_sha256": proof_sha, "retained_file_count": len(journal["files"]),
                  "database_write": False, "active_profile_write": False, "exports_repeated": False}
        _require(old_intent is None or intent == old_intent, "recovery intent changed")
        if apply:
            if old_intent is None:
                _require(not saved_checkpoint.exists(), "preservation checkpoint already exists")
                _write_exclusive(intent_path, intent)
            if target.exists():
                target.rename(staging)
            if checkpoint.exists():
                _require(not saved_checkpoint.exists(), "preservation checkpoint already exists")
                checkpoint.rename(saved_checkpoint)
        return {**intent, "applied": apply, "next_action": "resume the same operation through the monthly release API"}


def main(argv=None):
    from .cas_store import CASStore

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation-id", required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--control-root", type=Path, required=True)
    parser.add_argument("--active-profile", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    result = recover_unpublished_quote_policy(store=MonthlyOperationStore(args.state_root), operation_id=args.operation_id,
                                             cas=CASStore(args.control_root), active_profile=args.active_profile, apply=args.apply)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
