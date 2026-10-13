"""Explicitly adopt old completed exports using their original sealed receipts.

This is not a publisher or writer. It creates only the missing BUILD journal;
the ordinary worker must still perform fresh consumer and release validation.
"""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Mapping

from .canonical import canonical_json_bytes
from .cas_store import CASRef, CASStore, _publish_no_replace
from .monthly_build_resume import _identity, _plain
from .monthly_legacy_prefix import load_legacy_prefix


class LegacyAdoptionError(ValueError):
    """Original receipts do not prove reuse of these exact completed files."""


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise LegacyAdoptionError(message)


def adopt_completed_exports(*, context, compiled, release_root: Path, cas: CASStore,
    prepare_ref: CASRef, finalize_ref: CASRef, apply: bool = False) -> Mapping[str, Any]:
    """Verify original CAS proofs and create one journal, never overwrite data."""
    _require(context.stage == "BUILD", "adoption is only for BUILD")
    identity = _identity(context, compiled, release_root)
    _require(identity is not None, "durable monthly identity is required")
    binding, binding_digest, journal_path = identity
    _plain(journal_path)
    if journal_path.exists():
        raise FileExistsError("BUILD journal already exists; use ordinary resume")
    prepare = cas.get_json_bounded(prepare_ref, max_bytes=1024 * 1024)
    finalize = cas.get_json_bounded(finalize_ref, max_bytes=1024 * 1024)
    _require(isinstance(prepare, Mapping) and isinstance(finalize, Mapping), "stage receipts are not objects")
    attempt = prepare.get("attempt_fence")
    _require(type(attempt) is int and 0 < attempt < context.attempt, "legacy attempt is invalid")
    relative = f".staging/.{binding['candidate_name']}.{context.operation_id}.attempt-{attempt}.building"
    root = release_root / relative
    _plain(root)
    _require(root.is_dir() and not Path(context.plan["candidate_root"]).exists(), "legacy staging is unavailable or published")
    stage_identity = {"schema_version": "dataset_release_build_stage_result_v1", "status": "PASS",
        "run_id": context.operation_id, "attempt_id": f"{context.operation_id}-attempt-{attempt}",
        "attempt_fence": attempt, "staging_relative_path": relative,
        "release_id": binding["release_id"], "release_digest": compiled.physical_plan.get("release_digest")}
    for value, stage in ((prepare, "prepare"), (finalize, "finalize-bins")):
        _require(value.get("stage") == stage and all(value.get(k) == v for k, v in stage_identity.items()), "legacy stage identity differs")
    original_files: dict[str, list[int]] = {}
    directories: dict[Path, list[int]] = {}

    def signature(path: Path) -> list[int]:
        # Validate each parent once, then recheck all parent identities before
        # publication. Avoid millions of repeated NTFS ancestor stat calls.
        pending = []
        current = path.parent
        while current not in directories:
            pending.append(current)
            if current == current.parent:
                break
            current = current.parent
        for parent in reversed(pending):
            value = parent.lstat()
            _require(stat.S_ISDIR(value.st_mode) and not int(getattr(value, "st_file_attributes", 0)) & 0x0400, "legacy directory contains a link")
            directories[parent] = [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns]
        value = path.lstat()
        _require(stat.S_ISREG(value.st_mode) and not int(getattr(value, "st_file_attributes", 0)) & 0x0400, "legacy file is not regular")
        return [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns]

    def checked_path(relative_path: str) -> Path:
        _require(isinstance(relative_path, str) and bool(relative_path) and not any(t in relative_path for t in ("\\", ":", "\x00"))
            and not Path(relative_path).is_absolute() and ".." not in Path(relative_path).parts, "legacy file escaped staging")
        path = root / relative_path
        return path

    def check_signature(relative_path: str, signature: Any) -> None:
        _require(isinstance(signature, list) and len(signature) == 4 and all(type(x) is int for x in signature), "original signature is missing")
        _require(file_signature(checked_path(relative_path)) == signature, f"completed export changed: {relative_path}")
        original_files[relative_path] = signature

    file_signature = signature

    def check_pin(relative_path: str, pin: Mapping[str, Any]) -> None:
        path = checked_path(relative_path)
        before = file_signature(path)
        ref = CASRef.from_value(pin)
        _require(before[2] == ref.size, f"receipt size differs: {relative_path}")
        digest = hashlib.sha256()
        with path.open("rb") as reader:
            while block := reader.read(1024 * 1024):
                digest.update(block)
        _require(file_signature(path) == before and digest.hexdigest() == ref.sha256, f"receipt bytes differ: {relative_path}")
        original_files[relative_path] = before

    def load_bound(ref, schema):
        value = cas.get_json_bounded(ref, max_bytes=64 * 1024 * 1024)
        expected = {"schema_version": schema, "cutoff": binding["target_cutoff"],
            "source_bundle_sha256": binding["source_bundle_sha256"],
            "predecessor_manifest_sha256": binding["predecessor"]["dataset_manifest_sha256"], "status": "PASS"}
        _require(isinstance(value, Mapping) and all(value.get(k) == v for k, v in expected.items()), "materialization source identity differs")
        return value

    p = cas.get_json_bounded(prepare["prepare_receipt_ref"], max_bytes=1024 * 1024)
    f = cas.get_json_bounded(finalize["finalize_receipt_ref"], max_bytes=1024 * 1024)
    _require(p.get("schema_version") == "dataset_release_build_prepare_v1" and f.get("schema_version") == "dataset_release_build_finalize_v1"
        and p.get("cutoff") == f.get("cutoff") == binding["target_cutoff"], "nested stage receipt differs")
    operations = prepare.get("qlib_dump_operations")
    _require(isinstance(operations, list) and len(operations) == 2
        and {x.get("dataset") for x in operations} == {"daily_bin", "minute_bin"}, "legacy Qlib operations differ")
    steps = {"prepare": prepare_ref.as_dict(), "finalize-bins": finalize_ref.as_dict()}
    expected_features: set[str] = set()
    baseline_roots = {}
    for operation in operations:
        dataset = operation["dataset"]
        _require(operation.get("mode") == "inherited_month" and operation.get("operation_id") == dataset.removesuffix("_bin"), "legacy writer mode differs")
        prepared = cas.get_json_bounded(operation["preparation_ref"], max_bytes=16 * 1024 * 1024)
        _require(prepared.get("source_bundle_sha256") == binding["source_bundle_sha256"] and prepared.get("dataset") == dataset
            and prepared.get("cutoff") == binding["target_cutoff"] and prepared.get("predecessor_manifest_sha256") == binding["predecessor"]["dataset_manifest_sha256"], "legacy Qlib input differs")
        ref = f[f"{dataset.removesuffix('_bin')}_materialization_receipt_file_ref"]
        value = load_bound(ref, "aistock_monthly_native_qlib_materialization_v1")
        _require(value.get("dataset") == dataset, "legacy materialization dataset differs")
        check_pin(f"{dataset}/materialization_receipt.json", ref)
        writer = value["native_writer"]
        baseline_roots[dataset] = writer.get("baseline_root")
        frequency = "day" if dataset == "daily_bin" else "1min"
        _require(Path(writer["target_root"]).absolute() == (root / dataset / "qlib").absolute()
            and writer["frequency"] == frequency, "legacy provider path or frequency differs")
        for file_path, key in ((f"calendars/{frequency}.txt", "calendar"), ("instruments/all.txt", "instruments")):
            check_signature(f"{dataset}/qlib/{file_path}", writer[f"{key}_signature"])
        features = writer["feature_receipts"]
        _require(len(features) == writer["feature_file_count"] and bool(features), "legacy feature count differs")
        for feature in features:
            path = f"{dataset}/qlib/features/{feature['instrument']}/{feature['feature']}.{frequency}.bin"
            _require(path not in expected_features and feature["target_size"] == feature["target_signature"][2], "duplicate or invalid feature pin")
            check_signature(path, feature["target_signature"])
            expected_features.add(path)
        steps[f"qlib_dump_{operation['operation_id']}"] = dict(ref)
    factor = load_bound(p["refs"]["factor_receipt"], "aistock_monthly_native_factor_materialization_v1")
    _require({r["dataset"] for r in factor["files"]} == {"daily_pv", "daily_basic", "moneyflow", "bak_basic", "cyq_perf", "sector_data", "margin_detail", "static_factors"}
        and len(factor["files"]) == 8, "legacy factor component is incomplete")
    for item in factor["files"]:
        suffix = ".parquet" if item["dataset"] == "static_factors" else ".h5"
        check_signature(f"factor_bundle/{item['dataset']}{suffix}", item["target_signature"])
    for item in factor["metadata_files"]:
        check_signature("factor_bundle/" + item["relative_path"], item["target_signature"])
    index_ref = p["refs"]["index_materialization_receipt_file"]
    index = load_bound(index_ref, "aistock_monthly_native_index_materialization_v1")
    check_pin("index_context/index_materialization_receipt.json", index_ref)
    for item in index["files"]:
        check_signature("index_context/" + item["relative_path"], item["target_signature"])
    # One metadata-only inventory: preserve old price bytes, reject unrecorded
    # feature/partial/link files, and bind auxiliary files for later resume.
    files = dict(original_files)
    actual_features: set[str] = set()
    for component in ("daily_bin", "minute_bin", "factor_bundle", "index_context"):
        for base, dirs, names in os.walk(root / component, followlinks=False):
            context.checkpoint()
            for name in dirs:
                _plain(Path(base) / name)
            for name in names:
                path = Path(base) / name
                relative_path = path.relative_to(root).as_posix()
                _require(not name.endswith(".partial") and ".partial." not in name, "legacy export is incomplete")
                if "qlib/features/" in relative_path and name.endswith(".bin"):
                    actual_features.add(relative_path)
                if name != "meta_export.json":
                    value = file_signature(path)
                    _require(relative_path not in original_files or value == original_files[relative_path], "export changed during adoption")
                    files[relative_path] = value
    _require(expected_features <= actual_features, "legacy recorded features are missing")
    inherited = actual_features - expected_features
    if inherited:
        predecessor = binding["predecessor"]
        _require(isinstance(predecessor.get("candidate_root"), str) and isinstance(context.plan.get("predecessor_manifest_ref"), Mapping), "inherited baseline identity is unavailable")
        baseline = Path(predecessor["candidate_root"])
        _require(baseline.is_absolute() and baseline.parent.absolute() == release_root.absolute(), "inherited prefix escaped release catalog")
        prefix = load_legacy_prefix(baseline, expected_manifest_sha256=predecessor["dataset_manifest_sha256"],
            expected_file_sha256=context.plan["predecessor_manifest_ref"]["sha256"],
            expected_cutoff=date.fromisoformat(predecessor["cutoff"]), expected_release_id=predecessor["release_id"])
        for relative_path in sorted(inherited):
            dataset, _, tail = relative_path.split("/", 2)
            key = "day_provider_catalog" if dataset == "daily_bin" else "minute_provider_catalog"
            catalog = prefix.manifest["components"][key]["path"]
            # all.txt pin names the exact inherited provider, not a path from
            # a receipt alone. Unknown extra files cannot become history.
            _require(isinstance(catalog, str) and not any(t in catalog for t in ("\\", ":", "\x00"))
                and not Path(catalog).is_absolute() and ".." not in Path(catalog).parts
                and Path(catalog).parts[-2:] == ("instruments", "all.txt"), "inherited provider pin differs")
            provider = baseline / Path(catalog).parent.parent
            _require(Path(baseline_roots[dataset]).absolute() == provider.absolute(), "inherited provider receipt differs")
            source, target = provider / tail, checked_path(relative_path)
            # Native same-volume copy-on-write keeps untouched files hardlinked.
            # This is exact byte identity without hashing old price history.
            _require(file_signature(source) == file_signature(target), f"unproved inherited feature: {relative_path}")
            original_files[relative_path] = files[relative_path]
    for directory, expected in directories.items():
        value = directory.lstat()
        actual = [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns]
        _require(stat.S_ISDIR(value.st_mode) and not int(getattr(value, "st_file_attributes", 0)) & 0x0400
            and (actual == expected if directory.is_relative_to(root) else actual[:2] == expected[:2]), "legacy directory changed during adoption")
    record = {"schema_version": "aistock_monthly_build_resume_v1", "binding": binding,
        "binding_digest": binding_digest, "materialization_attempt": attempt,
        "staging_relative_path": relative, "steps": steps, "files": files}
    if apply:
        context.checkpoint()
        descriptor, name = tempfile.mkstemp(prefix=f".{journal_path.name}.", suffix=".partial", dir=journal_path.parent)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as writer:
                writer.write(canonical_json_bytes(record) + b"\n")
                writer.flush()
                os.fsync(writer.fileno())
            context.checkpoint()
            if not _publish_no_replace(temporary, journal_path):
                raise FileExistsError("BUILD journal appeared during adoption")
        finally:
            temporary.unlink(missing_ok=True)
    return {"schema_version": "aistock_monthly_legacy_export_adoption_v1",
        "operation_id": context.operation_id, "source_bundle_sha256": binding["source_bundle_sha256"],
        "binding_digest": binding_digest, "materialization_attempt": attempt, "staging_root": str(root),
        "journal_path": str(journal_path), "journal_written": apply,
        "verified_file_count": len(files), "original_receipt_pinned_file_count": len(original_files),
        "feature_file_count": len(expected_features), "exports_written": False,
        "inherited_unchanged_feature_file_count": len(inherited),
        "database_access": False, "active_profile_write": False, "publication_allowed": False}


def main(argv=None) -> int:
    import argparse
    import json

    from backend.services.dataset_release.monthly_build_bridge import compile_initial_monthly_build
    from backend.services.dataset_release.monthly_unified import MonthlyOperationStore, _exclusive_lock
    from backend.services.dataset_release.monthly_worker import ProducerContext
    from backend.services.dataset_release.profile import load_dataset_profile
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation-id", required=True)
    parser.add_argument("--state-root", required=True, type=Path)
    parser.add_argument("--control-root", required=True, type=Path)
    parser.add_argument("--artifact-root", required=True, action="append", type=Path)
    parser.add_argument("--dataset-profile", type=Path, default=Path(__file__).resolve().parents[3] / "configs/datasets/qe_backtest_monthly_v2.yaml")
    parser.add_argument("--prepare-ref", required=True)
    parser.add_argument("--finalize-ref", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    store = MonthlyOperationStore(args.state_root)
    operation = store.operation_root(args.operation_id)
    for path in (operation, args.control_root, args.dataset_profile, *args.artifact_root):
        _plain(path)
    # Same lock used by the worker and API; no adoption while files can change.
    with _exclusive_lock(operation / ".writer.lock", blocking=False):
        state = store.read_state(args.operation_id)
        if state.get("status") != "FAILED" or state.get("current_stage") != "BUILD":
            raise ValueError("adoption requires an idle FAILED BUILD operation")
        plan, request = store.read_plan(args.operation_id), store.read_request(args.operation_id)
        source = store.read_checkpoint(args.operation_id, "SOURCE")
        if source is None:
            raise ValueError("formal SOURCE checkpoint is unavailable")
        cas = CASStore(args.control_root)
        compiled = compile_initial_monthly_build(context_plan=plan, source_receipt=source,
            profile=load_dataset_profile(args.dataset_profile), cas=cas, artifact_roots=args.artifact_root)
        context = ProducerContext(stage="BUILD", operation_id=args.operation_id,
            attempt=state["attempt"], plan=plan, request=request, prior_receipts={"SOURCE": source})
        result = adopt_completed_exports(context=context, compiled=compiled,
            release_root=Path(plan["candidate_root"]).parent, cas=cas,
            prepare_ref=cas.verify(CASRef.from_value(args.prepare_ref)),
            finalize_ref=cas.verify(CASRef.from_value(args.finalize_ref)), apply=args.apply)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
