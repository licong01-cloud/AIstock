"""Adopt existing validated factor repairs into one unpublished monthly build.

This never re-exports prices, queries a database, grants readiness or activates
a profile. Existing validation is retained under its real file identity;
ordinary release/consumer validation still runs after copy-on-write adoption.
"""
from __future__ import annotations

import copy
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import tempfile

from .monthly_build_resume import _atomic, _plain, _signature, load_sealed_build_json
from .canonical import canonical_json_bytes

BINDING = "monthly_fact_repairs"
SCHEMA = "aistock_monthly_validated_factor_correction_v1"
_SINGLE = {
    "daily_pv": "aistock_daily_pv_fact_reconciliation_component_v1",
    "sector_data": "aistock_private_september_sector_repair_v1",
    "static_factors": "aistock_private_full_fact_static_repair_v1",
}
_AUX = frozenset({"daily_basic", "moneyflow", "bak_basic", "cyq_perf", "margin_detail"})


def _name(dataset):
    return dataset + (".parquet" if dataset == "static_factors" else ".h5")


def _under(path, boundary):
    path = Path(path)
    boundary = Path(boundary)
    _plain(path)
    _plain(boundary)
    if not path.is_absolute() or not path.resolve().is_relative_to(boundary.resolve()):
        raise ValueError("factor repair escaped the registered repair root")
    _plain(path)
    if not path.is_file():
        raise ValueError("factor repair is not a regular file")
    return path


def capture_factor_repairs(inputs, *, repair_root, cutoff):
    """Bind existing producer receipts, not caller-supplied financial values."""
    if set(inputs) != {"components"} or not isinstance(inputs["components"], list) or not inputs["components"]:
        raise ValueError("factor repair requires explicit component receipts")
    date.fromisoformat(cutoff)
    files, seen = [], set()
    for entry in inputs["components"]:
        if not isinstance(entry, dict) or set(entry) != {"dataset", "receipt_path", "receipt_sha256"}:
            raise ValueError("factor correction receipt fields differ")
        dataset = entry["dataset"]
        if dataset not in {*_SINGLE, *_AUX} or dataset in seen:
            raise ValueError("factor repair dataset is unsupported or duplicated")
        seen.add(dataset)
        receipt_path = _under(entry["receipt_path"], repair_root)
        signature = _signature(receipt_path)
        # Use the actual pinned receipt size, not an arbitrary message cap.
        raw = receipt_path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry["receipt_sha256"] or _signature(receipt_path) != signature:
            raise ValueError("factor repair receipt changed")
        receipt = json.loads(raw)
        expected = _SINGLE.get(dataset, "aistock_auxiliary_fact_component_repair_v1")
        if (receipt.get("schema_version") != expected
                or receipt.get("database_write") is not False
                or receipt.get("profile_write") is not False
                or receipt.get("runtime_action") is not False
                or receipt.get("status") not in {"PRIVATE_COMPONENT_NOT_A_RELEASE", "PRIVATE_COMPONENTS_NOT_A_RELEASE"}):
            raise ValueError("factor repair is not an existing private producer output")
        if receipt.get("cutoff", cutoff) != cutoff:
            raise ValueError("factor repair cutoff differs")
        pin = receipt["components"][dataset] if dataset in _AUX else receipt
        source = _under(pin["target_path"], repair_root)
        if source.name != _name(dataset):
            raise ValueError("factor repair file and dataset differ")
        from .canonical import ensure_sha256
        digest = ensure_sha256(pin["target_sha256"], field="factor repair file")
        files.append({"dataset": dataset, "relative_path": f"factor_bundle/{_name(dataset)}",
            "source_path": str(source), "sha256": digest, "size": source.stat().st_size,
            "source_signature": list(_signature(source)), "validation": entry,
            "validation_payload": receipt})
    # No stored value is recomputed here. Every derived input must instead be
    # the same byte asset used by the already validated static producer.
    static = next((row for row in files if row["dataset"] == "static_factors"), None)
    if static is None:
        raise ValueError("factor source repairs require their matching static output")
    dependencies = static["validation_payload"].get("input_sha256", {})
    if any(dependencies.get(row["dataset"]) != row["sha256"] for row in files if row is not static):
        raise ValueError("static repair was built from different factor files")
    return {"schema_version": SCHEMA, "target_cutoff": cutoff, "files": files,
        "historical_business_audit_performed": False, "database_write": False,
        "active_profile_write": False, "runtime_action": False}


def _pending(root):
    return root.parent / f".{root.name}.factor-correction-commit.json"


def _copy_checked(source, target, pin, checkpoint):
    """One bounded streaming copy/hash, never load a full H5 into RAM."""
    _plain(source)
    _plain(target)
    if list(_signature(source)) != pin["source_signature"]:
        raise ValueError("factor repair source changed after binding")
    digest, size = hashlib.sha256(), 0
    with source.open("rb") as reader, target.open("wb") as writer:
        while block := reader.read(1024 * 1024):
            checkpoint()
            writer.write(block)
            digest.update(block)
            size += len(block)
        writer.flush()
        os.fsync(writer.fileno())
    if (digest.hexdigest() != pin["sha256"] or size != pin["size"]
            or list(_signature(source)) != pin["source_signature"]):
        raise ValueError("factor repair source bytes differ")


def _physical_metadata(path, dataset, *, cutoff):
    """Inspect schema and count only the new month; history was validated."""
    import pandas as pd
    import pyarrow.parquet as pq
    start = date.fromisoformat(cutoff).replace(day=1).isoformat()
    if dataset == "static_factors":
        with pq.ParquetFile(path) as reader:
            count = reader.metadata.num_rows
            template = reader.schema_arrow.empty_table().to_pandas()
        month = pd.read_parquet(path, columns=[], filters=[("datetime", ">=", pd.Timestamp(start)),
            ("datetime", "<=", pd.Timestamp(cutoff))])
        future = pd.read_parquet(path, columns=[], filters=[("datetime", ">", pd.Timestamp(cutoff))])
    else:
        with pd.HDFStore(path, "r") as reader:
            count = int(reader.get_storer("data").nrows)
            template = reader.select("data", start=0, stop=0)
            month = reader.select("data", where=[f"datetime >= {start!r}", f"datetime <= {cutoff!r}"], columns=[])
            future = reader.select("data", where=f"datetime > {cutoff!r}", start=0, stop=1, columns=[])
    if len(future) or count < len(month) or not month.index.is_unique:
        raise ValueError("factor repair physical rows or month keys are invalid")
    return {"inherited_rows": count - len(month), "month_rows": len(month),
        "storage_columns": list(template.columns),
        "storage_dtypes": {name: str(kind) for name, kind in template.dtypes.items()}}


def recover_factor_repairs(*, root, journal, record, cas):
    pending = _pending(root)
    _plain(pending)
    if not pending.exists():
        return record
    commit = json.loads(pending.read_bytes())
    if commit.get("schema_version") != SCHEMA or commit.get("binding_digest") != record["binding_digest"]:
        raise ValueError("factor repair transaction identity differs")
    allowed = {f"factor_bundle/{_name(d)}" for d in {*_SINGLE, *_AUX}} | {
        "factor_bundle/moneyflow_alias_coverage_v1.json"}
    if not commit["files"] or set(commit["files"]) - allowed:
        raise ValueError("factor repair transaction escaped its component")
    if record["steps"].get("fact-correction") == commit["record"]["steps"].get("fact-correction"):
        # The journal is already committed and may include later completions.
        # Never roll those back by replaying an obsolete whole-record proposal.
        if any(record["files"].get(p) != pin["target_signature"]
               or list(_signature(root / p)) != pin["target_signature"] for p, pin in commit["files"].items()):
            raise ValueError("completed factor repair output changed")
        pending.unlink()
        return record
    if record["steps"] != commit["original_steps"]:
        raise ValueError("factor repair transaction receipts differ")
    if {k: v for k, v in record.items() if k not in {"steps", "files"}} != {
            k: v for k, v in commit["record"].items() if k not in {"steps", "files"}}:
        raise ValueError("factor repair transaction changed the build identity")
    unaffected = set(record["files"]) - set(commit["files"])
    if (set(record["files"]) != set(commit["record"]["files"])
            or any(record["files"][p] != commit["record"]["files"][p] for p in unaffected)):
        raise ValueError("factor repair transaction changed an unrelated file")
    if (set(commit["record"]["steps"]) != set(record["steps"]) | {"fact-correction"}
            or any(commit["record"]["steps"].get(k) != v for k, v in record["steps"].items()
                   if k not in {"prepare", "fact-correction"})):
        raise ValueError("factor repair transaction changed an unrelated receipt")
    for relative, pin in commit["files"].items():
        path = root / relative
        temporary = Path(pin["temporary_path"])
        _plain(path)
        _plain(temporary)
        if temporary.parent != root.parent or not temporary.name.startswith(f".{root.name}.fact-repair-"):
            raise ValueError("factor repair temporary output escaped staging")
        actual = list(_signature(path))
        if (record["files"].get(relative) not in (pin["original_signature"], pin["target_signature"])
                or commit["record"]["files"].get(relative) != pin["target_signature"]):
            raise ValueError("factor repair lost its file provenance")
        if actual == pin["target_signature"]:
            continue  # Rename was already completed before an interruption.
        if actual != pin["original_signature"] or list(_signature(temporary)) != pin["target_signature"]:
            raise ValueError("factor repair target changed before replacement")
    result = copy.deepcopy(commit["record"])
    for relative, pin in commit["files"].items():
        path = root / relative
        if list(_signature(path)) != pin["target_signature"]:
            os.replace(pin["temporary_path"], path)  # Break hardlinks; old release stays immutable.
        result["files"][relative] = list(_signature(path))
    _atomic(journal, result)
    pending.unlink()
    return result


def _retain_alias_audit(resume, factor, entries, authority, binding, files, record):
    """Retain the sealed month's audited keys when existing values were preserved.

    The additional historical facts retain their own producer validation. This
    does not relabel a monthly alias audit as a new full-history audit.
    """
    flow = next((row for row in authority["files"] if row["dataset"] == "moneyflow"), None)
    if flow is None:
        return
    proof = flow["validation_payload"]["components"]["moneyflow"]
    old = factor["alias_coverage"]
    if (proof.get("existing_finite_values_unchanged") is not True
            or proof.get("baseline_sha256") != old["moneyflow_sha256"]
            or old.get("status") != "PASS" or old.get("expected") != old.get("resolved")
            or any(old.get(k) != 0 for k in ("unknown", "nonfinite", "mismatched"))):
        raise ValueError("factor repair cannot retain the original alias audit")
    alias = {**old, "moneyflow_sha256": entries["moneyflow"]["sha256"],
             "previous_moneyflow_sha256": old["moneyflow_sha256"], "fact_correction_ref": binding["authority_ref"]}
    relative = "factor_bundle/moneyflow_alias_coverage_v1.json"
    path = resume.root / relative
    original = list(_signature(path))
    if record["files"].get(relative) != original:
        raise ValueError("factor alias metadata lost original writer provenance")
    pin = next(row for row in factor["metadata_files"] if row["relative_path"] == path.name)
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != pin["sha256"] or json.loads(raw) != old:
        raise ValueError("factor alias metadata changed")
    descriptor, name = tempfile.mkstemp(prefix=f".{resume.root.name}.fact-repair-", suffix=".partial", dir=resume.root.parent)
    with os.fdopen(descriptor, "wb") as writer:
        payload = canonical_json_bytes(alias) + b"\n"
        writer.write(payload)
        writer.flush()
        os.fsync(writer.fileno())
    temporary = Path(name)
    signature = list(_signature(temporary))
    files[relative] = {"original_signature": original, "target_signature": signature, "temporary_path": name}
    record["files"][relative] = signature
    pin.update(sha256=hashlib.sha256(payload).hexdigest(), size=len(payload), target_signature=signature)
    factor["alias_coverage"] = alias


def apply_factor_repairs(resume, *, prepare, plan, cas):
    binding = plan.get(BINDING)
    if binding is None:
        return prepare
    if (resume.path is None or resume.root.parent.name != ".staging"
            or (resume.root / "qe_dataset_manifest.json").exists()
            or binding.get("operation_id") != resume.context.operation_id
            or binding.get("target_cutoff") != plan["target_cutoff"]):
        raise ValueError("factor corrections require the same private retained build")
    prior = resume.record["steps"].get("fact-correction")
    if prior is not None:
        if prior != binding["authority_ref"]:
            raise ValueError("factor correction authority changed")
        return load_sealed_build_json(cas, resume.record["steps"]["prepare"])
    authority = load_sealed_build_json(cas, binding["authority_ref"])
    if authority.get("schema_version") != SCHEMA or authority.get("target_cutoff") != plan["target_cutoff"]:
        raise ValueError("factor correction authority differs")
    nested = copy.deepcopy(load_sealed_build_json(cas, prepare["prepare_receipt_ref"]))
    factor = copy.deepcopy(load_sealed_build_json(cas, nested["refs"]["factor_receipt"]))
    entries = {row["dataset"]: row for row in factor["files"]}
    static = next(row for row in authority["files"] if row["dataset"] == "static_factors")
    actual = {name: entry["sha256"] for name, entry in entries.items() if name != "static_factors"}
    actual.update({row["dataset"]: row["sha256"] for row in authority["files"] if row is not static})
    if actual != static["validation_payload"]["input_sha256"]:
        raise ValueError("static repair dependencies differ from the retained factor component")
    files, record = {}, copy.deepcopy(resume.record)
    for row in authority["files"]:
        relative, dataset = row["relative_path"], row["dataset"]
        path = resume.root / relative
        _plain(path)
        original = list(_signature(path))
        if record["files"].get(relative) != original or dataset not in entries:
            raise ValueError("factor repair lost original writer provenance")
        descriptor, name = tempfile.mkstemp(prefix=f".{resume.root.name}.fact-repair-", suffix=".partial", dir=resume.root.parent)
        os.close(descriptor)
        temporary = Path(name)
        _copy_checked(Path(row["source_path"]), temporary, row, resume.context.checkpoint)
        metadata = _physical_metadata(temporary, dataset, cutoff=plan["target_cutoff"])
        if metadata["storage_columns"] != entries[dataset].get("storage_columns", metadata["storage_columns"]):
            raise ValueError("factor repair storage columns differ")
        signature = list(_signature(temporary))
        entries[dataset].update(metadata, sha256=row["sha256"], size=row["size"],
            target_signature=signature, fact_correction_ref=binding["authority_ref"])
        files[relative] = {"original_signature": original, "target_signature": signature,
            "temporary_path": str(temporary)}
        record["files"][relative] = signature
    _retain_alias_audit(resume, factor, entries, authority, binding, files, record)
    factor["fact_correction_ref"] = binding["authority_ref"]
    nested["refs"]["factor_receipt"] = cas.put_json(factor).as_dict()
    result = {**prepare, "prepare_receipt_ref": cas.put_json(nested).as_dict()}
    record["steps"].update({"prepare": cas.put_json(result).as_dict(), "fact-correction": binding["authority_ref"]})
    _atomic(_pending(resume.root), {"schema_version": SCHEMA, "binding_digest": record["binding_digest"],
        "original_steps": resume.record["steps"], "record": record, "files": files})
    resume.record = recover_factor_repairs(root=resume.root, journal=resume.path, record=resume.record, cas=cas)
    return result
