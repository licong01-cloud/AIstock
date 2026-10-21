"""Retain a READY build for an explicit, unpublished correction attempt.

No market history is read here. SOURCE and completed physical exports keep
their original evidence; readiness, deployment and derived identities do not.
"""
from __future__ import annotations

import copy
import hashlib
import os
from pathlib import Path
import shutil

BINDING = "monthly_ready_correction"
SCHEMA = "aistock_monthly_ready_correction_v1"
EXPORT_ROOTS = (".month-inputs", "daily_bin", "minute_bin", "factor_bundle", "index_context")


def prepare_ready_correction(service, operation_id, *, principal):
    from .monthly_unified import (
        ActiveProfileSnapshot, MonthlyReleaseConflict, ReleaseState,
        _content_ref, _digest, _exclusive_lock,
        _file_sha256, _read_json, _require_plain_existing_chain, _write_exclusive,
    )
    root = service.store.operation_root(operation_id)
    with _exclusive_lock(root / ".writer.lock", blocking=False), _exclusive_lock(service.store.locks / "active-profile.lock"):
        plan = service.store.read_plan(operation_id)
        if BINDING in plan:
            _finish_preparation(service, operation_id, plan=plan)
            return plan[BINDING]["result"]
        state = service.store.read_state(operation_id)
        if state["status"] != ReleaseState.READY_TO_ACTIVATE.value:
            raise MonthlyReleaseConflict("only an unactivated READY candidate can be corrected")
        if (root / "receipts/activation.json").exists():
            raise MonthlyReleaseConflict("an activation intent already exists")
        base = Path(plan["candidate_root"])
        _require_plain_existing_chain(base, label="correction base")
        manifest = base / "qe_dataset_manifest.json"
        if not manifest.is_file():
            raise MonthlyReleaseConflict("completed correction base is unavailable")
        active = ActiveProfileSnapshot.read(service.active_profile)
        if active.cutoff.isoformat() >= plan["target_cutoff"]:
            raise MonthlyReleaseConflict("correction must precede target activation")
        source = service.store.read_checkpoint(operation_id, "SOURCE")
        build = service.store.read_checkpoint(operation_id, "BUILD")
        if source is None or build is None:
            raise MonthlyReleaseConflict("correction needs sealed SOURCE and completed BUILD")
        manifest_data = _read_json(manifest, label="correction manifest")
        if manifest_data.get("dataset_manifest_sha256") != build.get("scope", {}).get("dataset_manifest_sha256"):
            raise MonthlyReleaseConflict("correction base differs from completed BUILD")
        identity = _digest({"plan": plan, "manifest_file_sha256": _file_sha256(manifest),
                            "active_profile_sha256": active.file_sha256})
        suffix = f"-correction-{identity[:12]}"
        target = base.with_name(base.name + suffix)
        profile = Path(plan["profile_candidate"])
        profile = profile.with_name(profile.stem + suffix + profile.suffix)
        if target.exists() or profile.exists():
            raise MonthlyReleaseConflict("correction target already exists")
        archive = root / "corrections" / identity
        archive.mkdir(parents=True, exist_ok=True)
        _require_plain_existing_chain(archive, label="correction metadata")
        old_plan = archive / "plan.json"
        if not old_plan.exists():
            _write_exclusive(old_plan, plan)
        elif _read_json(old_plan, label="correction original plan") != plan:
            raise MonthlyReleaseConflict("retained correction plan differs")
        active_bytes = service.active_profile.read_bytes()
        if hashlib.sha256(active_bytes).hexdigest() != active.file_sha256:
            raise MonthlyReleaseConflict("active profile changed during correction preparation")
        active_path = archive / "activation-predecessor.json"
        if active_path.exists():
            if active_path.read_bytes() != active_bytes:
                raise MonthlyReleaseConflict("correction activation predecessor differs")
        else:
            with active_path.open("xb") as writer:
                writer.write(active_bytes)
        # Copy tiny controller metadata before any checkpoint is retired.
        for stage in ("BUILD", "DERIVE", "LOCAL_VALIDATE", "DEPLOY", "CONSUMER_VALIDATE"):
            path = root / "checkpoints" / f"{stage.lower()}.json"
            if path.exists():
                destination = archive / path.name
                if destination.exists() and destination.read_bytes() != path.read_bytes():
                    raise MonthlyReleaseConflict("retained correction checkpoint differs")
                if not destination.exists():
                    shutil.copyfile(path, destination)
        ready_path = root / "receipts/ready.json"
        if ready_path.exists() and not (archive / "ready.json").exists():
            shutil.copyfile(ready_path, archive / "ready.json")
        result = {"status": "CORRECTION_PREPARED", "operation_id": operation_id,
                  "candidate_root": str(target), "profile_candidate": str(profile),
                  "previous_candidate_root": str(base), "activation_authorized": False}
        binding = {"schema_version": SCHEMA, "base_plan": copy.deepcopy(plan),
                   "source_receipt_sha256": source["canonical_sha256"],
                   "base_manifest_ref": _content_ref(manifest),
                   "base_build_receipt": build, "bound_by": principal, "result": result}
        corrected = {**plan, BINDING: binding, "candidate_root": str(target),
                     "profile_candidate": str(profile), "generation": plan["generation"] + suffix,
                     "revision": plan["revision"] + suffix,
                     "node_roots": {k: v + suffix for k, v in plan["node_roots"].items()},
                     "activation_predecessor": {"profile_sha256": active.file_sha256},
                     "activation_predecessor_profile_ref": _content_ref(active_path)}
        corrected["target_roots"] = {"controller": str(target), **corrected["node_roots"]}
        # The completed minute amendment remains in its physical receipt and
        # immutable base plan. A new bounded input can now extend that lineage.
        corrected.pop("monthly_minute_repairs", None)
        corrected.pop("monthly_minute_repairs_ref", None)
        binding["archive_root"] = str(archive)
        service.store.replace_plan(operation_id, corrected)
        _finish_preparation(service, operation_id, plan=corrected)
        return result


def _finish_preparation(service, operation_id, *, plan):
    """Recover an interrupted metadata commit without retiring new evidence."""
    from .monthly_unified import MonthlyReleaseConflict, _read_json, _write_exclusive
    root = service.store.operation_root(operation_id)
    binding = plan[BINDING]
    archive = Path(binding["archive_root"])
    marker = archive / "prepared.json"
    state = service.store.read_state(operation_id)
    if marker.exists():
        if _read_json(marker, label="prepared correction") != binding["result"]:
            raise MonthlyReleaseConflict("prepared correction marker changed")
        if state.get("candidate_root") == plan["candidate_root"]:
            return
    else:
        for stage in ("BUILD", "DERIVE", "LOCAL_VALIDATE", "DEPLOY", "CONSUMER_VALIDATE"):
            path = root / "checkpoints" / f"{stage.lower()}.json"
            if path.exists():
                if not (archive / path.name).is_file() or (archive / path.name).read_bytes() != path.read_bytes():
                    raise MonthlyReleaseConflict("correction cannot retire an unpreserved checkpoint")
                path.unlink()  # Exact tiny checkpoint already preserved above.
        _write_exclusive(marker, binding["result"])
    # An idle FAILED state is not enqueued: bind repairs before explicit resume.
    service.store.update_state(operation_id, status="FAILED", current_stage="BUILD", last_error=None,
                               candidate_root=plan["candidate_root"], profile_candidate=plan["profile_candidate"])


def seed_completed_exports(*, context, staging_root, compiled, cas):
    """Copy-on-write adoption of the exact completed physical materialization."""
    binding = context.plan.get(BINDING)
    if binding is None:
        return False
    from .monthly_build_resume import (
        MonthlyBuildResume, MonthlyBuildResumeError, _atomic, _plain, _signature, load_sealed_build_json,
    )
    from .monthly_unified import _file_sha256, _read_json
    base = Path(binding["base_plan"]["candidate_root"])
    reference = binding["base_manifest_ref"]
    _plain(base)
    if _file_sha256(base / "qe_dataset_manifest.json") != reference["sha256"]:
        raise MonthlyBuildResumeError("completed correction base manifest changed")
    manifest = _read_json(base / "qe_dataset_manifest.json", label="completed manifest")
    evidence_path = base / "reports/monthly_candidate_build_evidence.json"
    pin = next((p for p in manifest["components"].values()
                if p["path"] == "reports/monthly_candidate_build_evidence.json"), None)
    if pin is None or _file_sha256(evidence_path) != pin["sha256"] or evidence_path.stat().st_size != pin["size"]:
        raise MonthlyBuildResumeError("completed BUILD evidence pin differs")
    evidence = _read_json(evidence_path, label="completed BUILD evidence")
    if evidence.get("operation_id") != context.operation_id or evidence.get("source_bundle_sha256") != compiled.source_bundle_sha256:
        raise MonthlyBuildResumeError("completed correction BUILD identity differs")
    from .monthly_build_resume import _identity
    identity = _identity(context, compiled, staging_root.parent.parent)
    if identity is None:
        raise MonthlyBuildResumeError("correction has no durable build identity")
    if identity[2].exists():
        return False  # Existing journal follows the normal retained-build path.
    release_root = staging_root.parent.parent
    intent = identity[2].with_suffix(".seed.json")
    if intent.exists():
        saved = _read_json(intent, label="correction seed intent")
        attempt = saved.get("attempt")
        if (saved.get("base_manifest_ref") != reference or type(attempt) is not int
                or not 0 < attempt <= context.attempt):
            raise MonthlyBuildResumeError("correction seed intent differs")
        expected = f".staging/.{Path(context.plan['candidate_root']).name}.{context.operation_id}.attempt-{attempt}.building"
        if saved.get("staging_relative_path") != expected:
            raise MonthlyBuildResumeError("correction seed path differs")
        staging_root = release_root / expected
    else:
        attempt = context.attempt
        _atomic(intent, {"base_manifest_ref": reference, "attempt": attempt,
                         "staging_relative_path": staging_root.relative_to(release_root).as_posix()})
    _plain(staging_root)
    marker = staging_root / ".ready-correction-seed.json"
    if staging_root.exists():
        if not marker.is_file() or _read_json(marker, label="correction seed") != {"base_manifest_ref": reference}:
            raise MonthlyBuildResumeError("correction staging exists without its seed identity")
    else:
        staging_root.mkdir(parents=True)
        from .monthly_unified import _write_exclusive
        _write_exclusive(marker, {"base_manifest_ref": reference})
    # These are metadata and filesystem checks, not a historical price audit.
    for name in EXPORT_ROOTS:
        origin = base / name
        if not origin.exists():
            continue
        for directory, dirs, files in os.walk(origin, followlinks=False):
            context.checkpoint()
            parent = Path(directory)
            _plain(parent)
            for child in dirs:
                _plain(parent / child)
            destination = staging_root / parent.relative_to(base)
            destination.mkdir(parents=True, exist_ok=True)
            for filename in files:
                source, target = parent / filename, destination / filename
                _plain(source)
                _plain(target)
                if target.exists():
                    if source.suffix not in {".json", ".jsonl", ".txt"} and not os.path.samefile(source, target):
                        raise MonthlyBuildResumeError("interrupted correction seed file differs")
                    if source.suffix in {".json", ".jsonl", ".txt"} and source.read_bytes() != target.read_bytes():
                        raise MonthlyBuildResumeError("interrupted correction seed metadata differs")
                    continue
                # Mutable metadata is private. Large immutable data are linked;
                # existing correction writers use replacement, never in-place IO.
                if source.suffix in {".json", ".jsonl", ".txt"}:
                    shutil.copyfile(source, target)
                else:
                    os.link(source, target)
    resume = MonthlyBuildResume(context=context, compiled=compiled, release_root=staging_root.parent.parent,
                                staging_root=staging_root, cas=cas)
    resume.record["materialization_attempt"] = attempt
    refs = evidence["stage_refs"]
    steps = [
        ("prepare", "prepare", (".month-inputs", "factor_bundle", "index_context")),
        *[(k, k, ("daily_bin" if "daily" in k or "day" in k else "minute_bin",))
          for k in refs if k.startswith("qlib_dump_")],
        ("finalize-bins", "finalize_bins", ("daily_bin", "minute_bin", "factor_bundle", "index_context")),
    ]
    for step, key, roots in steps:
        if key not in refs:
            if step.startswith("qlib_dump_"):
                continue
            raise MonthlyBuildResumeError("completed export receipt is missing")
        value = load_sealed_build_json(cas, refs[key])
        resume.record["steps"][step] = cas.put_json(dict(value)).as_dict()
    prepared = load_sealed_build_json(cas, refs["prepare"])
    if prepared.get("prepare_receipt_ref"):
        nested = load_sealed_build_json(cas, prepared["prepare_receipt_ref"])
        factor_ref = nested.get("refs", {}).get("factor_receipt")
        if factor_ref:
            factor = load_sealed_build_json(cas, factor_ref)
            proof_ref = factor.get("industry_history_projection_ref")
            if proof_ref:
                # A completed historical projection is not another monthly
                # export. Keep its actual proof so the runner does not redo it.
                load_sealed_build_json(cas, proof_ref)
                resume.record["steps"]["industry-history"] = proof_ref
    for name in EXPORT_ROOTS:
        for directory, dirs, files in os.walk(staging_root / name, followlinks=False):
            context.checkpoint()
            for filename in files:
                if filename == "meta_export.json":
                    continue
                path = Path(directory) / filename
                resume.record["files"][path.relative_to(staging_root).as_posix()] = _signature(path)
    if resume.path is None:
        raise MonthlyBuildResumeError("correction resume journal is unavailable")
    # All completed steps become visible together: a crash can never leave a
    # prepare-only seed that would accidentally re-run the minute exporter.
    _atomic(resume.path, resume.record)
    return True


def extend_minute_correction(previous, *, binding, plan, correction, cas):
    """Extend only the exact READY base; retain pins for earlier filled files."""
    if previous is None:
        return correction
    from .monthly_build_resume import MonthlyBuildResumeError
    old_binding = plan.get(BINDING, {}).get("base_plan", {}).get("monthly_minute_repairs")
    if not old_binding or previous["authority_ref"] != old_binding["authority_ref"]:
        raise MonthlyBuildResumeError("minute correction does not extend its completed base")
    if binding["authority_ref"] == previous["authority_ref"]:
        raise MonthlyBuildResumeError("minute correction extension has no new authority")
    pins = {entry["relative_path"]: entry for entry in previous["files"]}
    for entry in correction["files"]:
        old = pins.get(entry["relative_path"])
        if old is not None and entry["original_sha256"] != old["target_ref"]["sha256"]:
            raise MonthlyBuildResumeError("minute correction extension lost its original bytes")
        # Keep the oldest input SHA for the pre_close -> minute lineage check.
        if old is not None:
            entry = {**entry, "original_sha256": old["original_sha256"],
                     "original_signature": old["original_signature"]}
        pins[entry["relative_path"]] = entry
    return {**correction, "files": list(pins.values()),
            "previous_correction_ref": cas.put_json(previous).as_dict()}


def retained_source_receipt(service, operation_id, *, plan, request_digest):
    """Validate the original seal, not a newly invented SOURCE completion."""
    from .monthly_prev_close_repair import SUPPLEMENT_KEYS
    from .monthly_unified import MonthlyReleaseConflict, _digest
    binding = plan[BINDING]
    from .monthly_unified import _read_json
    marker = Path(binding["archive_root"]) / "prepared.json"
    if not marker.is_file() or _read_json(marker, label="prepared correction") != binding["result"]:
        raise MonthlyReleaseConflict("correction metadata preparation was interrupted")
    base = binding["base_plan"]
    supplements = [base[name] for name in (
        "monthly_industry_history", "monthly_prev_close_repairs",
        "monthly_suspension_authority", "monthly_minute_repairs") if base.get(name) is not None]
    original_digest = _digest({k: v for k, v in base.items() if k not in SUPPLEMENT_KEYS}) if supplements else _digest(base)
    receipt = service.store.read_checkpoint(operation_id, "SOURCE", request_digest=request_digest,
        plan_digest=original_digest, input_set_digest=_digest({"prior_receipts": {}}))
    if receipt is None or receipt["canonical_sha256"] != binding["source_receipt_sha256"]:
        raise MonthlyReleaseConflict("correction changed its original SOURCE seal")
    validator = getattr(service.pipeline, "checkpoint_is_valid", None)
    if callable(validator) and not validator("SOURCE", receipt):
        raise MonthlyReleaseConflict("retained correction SOURCE artifacts changed")
    return receipt
