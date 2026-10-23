"""Capture one late financing date read-only, or build its private component amendment.

This tool never activates a release, writes a database, or changes HMM/QE code.
"""
from __future__ import annotations

import argparse
import copy
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.dataset_release.canonical import canonical_json_bytes  # noqa: E402
from backend.services.dataset_release.monthly_late_publication import (  # noqa: E402
    MARGIN_FIELDS, amend_margin_files, file_pin, validate_margin_source,
)


def _write(path, value):
    raw = canonical_json_bytes(value) + b"\n"
    if path.exists():
        if path.is_symlink() or not path.is_file() or path.read_bytes() != raw:
            raise FileExistsError(path)
        return
    with path.open("xb") as handle:
        handle.write(raw)


def capture(args):
    import pandas as pd
    import psycopg2
    from dotenv import load_dotenv
    from scripts.backfill_tushare_daily_basic_fields import db_config

    day = date.fromisoformat(args.trade_date)
    if args.output.exists():
        raise FileExistsError(args.output)
    load_dotenv(args.env_file, override=False)
    config = db_config()
    config.update(application_name="BUG-2067-late-margin-readonly", connect_timeout=10,
                  options="-c client_encoding=utf8 -c statement_timeout=30000 -c default_transaction_read_only=on")
    conn = psycopg2.connect(**config)
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with conn.cursor() as cursor:
            cursor.execute("SELECT ts_code,trade_date," + ",".join(MARGIN_FIELDS)
                + " FROM market.margin_detail WHERE trade_date=%s ORDER BY ts_code", (day,))
            rows = cursor.fetchall()
            cursor.execute("SELECT to_jsonb(a) FROM market.dataset_date_refresh_audit a "
                           "WHERE dataset='margin_detail' AND trade_date=%s AND data_source='tushare'", (day,))
            audit = [row[0] for row in cursor.fetchall()]
        frame = pd.DataFrame(rows, columns=["instrument", "datetime", *MARGIN_FIELDS])
        frame["datetime"] = pd.to_datetime(frame["datetime"])
        frame = frame.rename(columns={field: "md_" + field for field in MARGIN_FIELDS}).set_index(["datetime", "instrument"])
        frame = validate_margin_source(frame, trade_date=args.trade_date)
        if not any(row["status"] == "success" and int(row["row_count"]) == len(frame) for row in audit):
            raise ValueError("late financing DB date does not match its existing success audit")
        args.output.mkdir(parents=True, exist_ok=False)
        path = args.output / "source.parquet"
        frame.to_parquet(path)
        receipt = {"schema_version": "aistock_monthly_late_financing_capture_v1", "status": "PASS",
            "trade_date": args.trade_date, "captured_at": datetime.now(timezone.utc).isoformat(),
            "source_dataset": "market.margin_detail", "provider": "tushare", "rows": len(frame),
            "source": {"path": path.name, **file_pin(path)},
            "existing_refresh_audit": audit,
            "database_read": True, "database_write": False, "network_provider_access": False}
        _write(args.output / "capture.json", receipt)
        return receipt
    finally:
        conn.rollback()
        conn.close()


def build(args):
    import pandas as pd
    from backend.services.dataset_release.monthly_legacy_prefix import load_legacy_prefix

    profile_path = args.profile.absolute()
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    root = Path(profile["controller_paths"]["candidate_root"])
    prefix = load_legacy_prefix(root, expected_manifest_sha256=profile["components"]["dataset_manifest_sha256"],
        expected_file_sha256=profile["components"]["dataset_manifest_file_sha256"],
        expected_cutoff=date.fromisoformat(profile["cutoff"]), expected_release_id=profile["release_id"])
    capture_path = args.capture / "capture.json"
    receipt = json.loads(capture_path.read_text(encoding="utf-8"))
    if (receipt["schema_version"] != "aistock_monthly_late_financing_capture_v1"
            or receipt["trade_date"] > profile["cutoff"] or receipt["source"]["path"] != "source.parquet"
            or receipt.get("database_write") is not False or receipt.get("status") != "PASS"
            or file_pin(args.capture / "source.parquet") != {k: receipt["source"][k] for k in ("sha256", "size")}):
        raise ValueError("late financing capture identity differs")
    source = pd.read_parquet(args.capture / "source.parquet")
    instruments = {line.split()[0] for line in (root / "daily_bin/qlib/instruments/all.txt").read_text(encoding="utf-8").splitlines() if line.strip()}
    for filename in ("margin_detail.h5", "static_factors.parquet"):
        pin = prefix.manifest["components"]["factor_bundle__" + filename]
        if file_pin(root / "factor_bundle" / filename) != {k: pin[k] for k in ("sha256", "size")}:
            raise ValueError("financing amendment predecessor bytes differ")
    result = amend_margin_files(root / "factor_bundle", args.output, source=source,
        trade_date=receipt["trade_date"], instruments=instruments,
        progress=lambda event: print(json.dumps(event), flush=True))
    result.update({"predecessor_profile": {"path": str(profile_path), **file_pin(profile_path)},
        "predecessor_candidate_root": str(root), "predecessor_generation": profile["generation"],
        "predecessor_manifest_sha256": prefix.manifest_sha256,
        "predecessor_manifest_file_sha256": prefix.manifest_file_sha256,
        "capture_ref": {"path": str(capture_path), **file_pin(capture_path)}, "database_read": False})
    _write(args.output / "component-amendment.json", result)
    return result


def assemble(args):
    """Create an independent same-September content revision; never activate it."""
    profile = json.loads(args.profile.read_text(encoding="utf-8"))
    component = json.loads((args.component / "component-amendment.json").read_text(encoding="utf-8"))
    if (component["predecessor_profile"]["sha256"] != file_pin(args.profile)["sha256"]
            or component["status"] != "PRIVATE_COMPONENTS_NOT_A_RELEASE"):
        raise ValueError("amendment predecessor profile changed")
    base = Path(profile["controller_paths"]["candidate_root"])
    manifest = json.loads((base / "qe_dataset_manifest.json").read_text(encoding="utf-8"))
    if file_pin(base / "qe_dataset_manifest.json")["sha256"] != component["predecessor_manifest_file_sha256"]:
        raise ValueError("amendment predecessor manifest changed")
    target = args.output.absolute()
    if (target.parent != base.parent or target == base or target.exists()
            or not target.name.startswith(profile["cutoff"].replace("-", "") + "-")):
        raise ValueError("amendment requires a new same-cutoff catalog sibling")
    from backend.services.dataset_release.monthly_legacy_prefix import _plain_chain, _relative
    _plain_chain(target.parent)
    # The root is not READY while this script assembles it. Resume reuses the
    # completed private component; it never repeats DB capture or Parquet build.
    target.mkdir(exist_ok=False)
    changed_names = {"margin_detail.h5", "static_factors.parquet"}
    changed = {"components/factor_h5_static_candidate_v2/" + name for name in changed_names}
    changed |= {"factor_bundle/" + name for name in changed_names}
    meta_path = "components/factor_h5_static_candidate_v2/meta.json"
    changed.add(meta_path)
    for name in changed_names:
        if file_pin(args.component / name) != component["files"][name]:
            raise ValueError("private financing component changed")
    pins = manifest["components"]
    for count, pin in enumerate(pins.values(), 1):
        relative = _relative(pin["path"]).as_posix()
        if relative in changed:
            continue
        source, destination = base / relative, target / relative
        _plain_chain(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.link(source, destination)
        if count % 10000 == 0:
            print(json.dumps({"stage": "reuse_unchanged_files", "files": count, "total": len(pins)}), flush=True)
    for name in changed_names:
        destination = target / "factor_bundle" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.component / name, destination)
        alias = target / "components/factor_h5_static_candidate_v2" / name
        alias.parent.mkdir(parents=True, exist_ok=True)
        os.link(destination, alias)
    meta = json.loads((base / meta_path).read_text(encoding="utf-8"))
    meta["late_financing_content_revision"] = {"trade_date": component["trade_date"],
        "receipt": "reports/late-financing-amendment.json", "source_sha256": component["capture_ref"]["sha256"]}
    _write(target / meta_path, meta)
    proof_root = target / "reports/late-financing-source"
    proof_root.mkdir(parents=True)
    capture_path = Path(component["capture_ref"]["path"])
    for name in ("capture.json", "source.parquet"):
        shutil.copyfile(capture_path.parent / name, proof_root / name)
    _write(target / "reports/late-financing-amendment.json", component)
    additions = ["reports/late-financing-amendment.json", "reports/late-financing-source/capture.json",
                 "reports/late-financing-source/source.parquet"]
    revised = copy.deepcopy(manifest)
    for key, pin in revised["components"].items():
        if pin["path"] in changed:
            pin.update(file_pin(target / pin["path"]))
    for relative in additions:
        revised["components"][relative.replace("/", "__")] = {"path": relative, **file_pin(target / relative)}
    revised["revision"] = manifest["revision"] + "-margin1"
    revised["source_contract"]["late_fact_amendment"] = {"predecessor_manifest_sha256": manifest["dataset_manifest_sha256"],
        "receipt_sha256": file_pin(target / additions[0])["sha256"], "trade_date": component["trade_date"]}
    revised["deployment_content_sha256"] = hashlib.sha256(canonical_json_bytes(revised["components"])).hexdigest()
    revised["deployment_snapshot_id"] = revised["release_id"] + "_" + revised["deployment_content_sha256"][:16]
    revised.pop("dataset_manifest_sha256", None)
    revised["dataset_manifest_sha256"] = hashlib.sha256(canonical_json_bytes(revised)).hexdigest()
    _write(target / "qe_dataset_manifest.json", revised)
    draft = copy.deepcopy(profile)
    draft["generation"] = profile["generation"] + "-margin1"
    draft["controller_paths"] = {key: value.replace(str(base), str(target)) for key, value in profile["controller_paths"].items()}
    for node, binding in draft["node_bindings"].items():
        old = profile["node_bindings"][node]["candidate_root"]
        binding["candidate_root"] = old.rsplit("/", 1)[0] + "/" + target.name
    draft["components"].update({"dataset_manifest_sha256": revised["dataset_manifest_sha256"],
        "dataset_manifest_file_sha256": file_pin(target / "qe_dataset_manifest.json")["sha256"],
        "factor_meta_sha256": file_pin(target / meta_path)["sha256"]})
    # This is deliberately a draft, not a production profile: DERIVE/closure
    # must run on the new manifest before official adoption and activation.
    _write(args.component / "profile-draft.json", draft)
    return {"status": "ASSEMBLED_PENDING_DERIVE_AND_READBACK", "candidate_root": str(target),
        "generation": draft["generation"], "revision": revised["revision"],
        "release_id": revised["release_id"], "manifest_sha256": revised["dataset_manifest_sha256"],
        "manifest_file_sha256": draft["components"]["dataset_manifest_file_sha256"],
        "active_profile_write": False, "database_write": False, "runtime_action": False}


def seal(args):
    """Use existing code-owned DERIVE, profile builder and acyclic closure.

    Revalidate only the explicit correction. Inherit prior unchanged component
    evidence by exact manifest pins; never rerun historical price QA.
    """
    from dotenv import load_dotenv
    from backend.services.dataset_release.canonical import digest_named_fields
    from backend.services.dataset_release.monthly_consumer_layout import CONSUMER_LAYOUT_RECEIPT_SCHEMA
    from backend.services.dataset_release.monthly_local_validation import (
        _component_groups, _content_summary, CONSUMER_CONTRACT_SCHEMA,
        SOURCE_READINESS_SCHEMA, COMPONENT_VALIDATION_SCHEMA, RELEASE_LINEAGE_SCHEMA,
    )
    from backend.services.dataset_release.monthly_profile_candidate import SealedMonthlyProfileCandidateBuilder
    from backend.services.dataset_release.monthly_worker import ProducerContext
    from backend.services.dataset_release.monthly_worker_runtime import build_monthly_worker_runtime
    from backend.services.dataset_release.monthly_unified import REQUIRED_CONSUMERS
    from backend.services.dataset_release.profile_contract import ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS
    from backend.services.dataset_release.release_successor import build_release_closure

    completed_path = args.component / "seal-result.json"
    if completed_path.exists():
        completed = json.loads(completed_path.read_text(encoding="utf-8"))
        if (file_pin(args.output)["sha256"] != completed["profile_sha256"]
                or file_pin(Path(completed["candidate_root"]) / "qe_dataset_manifest.json")["sha256"]
                != completed["dataset_manifest_file_sha256"]):
            raise ValueError("sealed amendment changed before resume")
        return completed
    load_dotenv(args.env_file, override=False)
    runtime = build_monthly_worker_runtime(project_root=Path(args.env_file).parent)
    draft = json.loads((args.component / "profile-draft.json").read_text(encoding="utf-8"))
    root = Path(draft["controller_paths"]["candidate_root"])
    predecessor_path = runtime.settings.active_profile
    predecessor = json.loads(predecessor_path.read_text(encoding="utf-8"))
    correction = json.loads((args.component / "component-amendment.json").read_text(encoding="utf-8"))
    minute_amendment = correction.get("schema_version") == "aistock_monthly_minute_coverage_amendment_v1"
    physical_profile_path = Path(correction["predecessor_profile"]["path"])
    if file_pin(physical_profile_path)["sha256"] != correction["predecessor_profile"]["sha256"]:
        raise ValueError("physical correction predecessor changed before sealing")
    physical_profile = json.loads(physical_profile_path.read_text(encoding="utf-8"))
    if file_pin(predecessor_path)["sha256"] != correction["predecessor_profile"]["sha256"]:
        lineage = json.loads((Path(correction["predecessor_candidate_root"]) / "provenance/release-lineage.json").read_text(encoding="utf-8"))
        if (not minute_amendment or lineage["predecessor_profile_ref"]["sha256"] != file_pin(predecessor_path)["sha256"]
                or lineage["predecessor_dataset_manifest_sha256"] != predecessor["components"]["dataset_manifest_sha256"]):
            raise ValueError("active predecessor changed before sealing")
    manifest_path = root / "qe_dataset_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    # Refresh the existing consumer-layout summary from pins only, not content.
    layout_relative = "reports/monthly_consumer_layout_receipt.json"
    layout_path = root / layout_relative
    layout = json.loads(layout_path.read_text(encoding="utf-8"))
    by_path = {pin["path"]: pin for pin in manifest["components"].values()}
    layout["components"]["factor"] = _content_summary(by_path, prefix="components/factor_h5_static_candidate_v2/")
    if minute_amendment:
        layout["components"]["minute"] = _content_summary(by_path, prefix="components/minute_bin_candidate/")
        proof = json.loads((args.component / "full-window-validation.json").read_text(encoding="utf-8"))
        if (proof.get("status") != "PASS" or proof.get("physical_unexplained_gap_days_after") != 0
                or proof.get("generation") != draft["generation"]
                or proof.get("minute_content_digest") != hashlib.sha256(canonical_json_bytes({
                    p: pin for p, pin in by_path.items() if p.startswith("components/minute_bin_candidate/")})).hexdigest()):
            raise ValueError("minute correction full-window readback differs")
        proof_relative = "reports/minute-full-window-validation.json"
        _write(root / proof_relative, proof)
        proof_pin = {"path": proof_relative, **file_pin(root / proof_relative)}
        manifest["components"][proof_relative.replace("/", "__")] = proof_pin
        by_path[proof_relative] = proof_pin
    layout.pop("consumer_layout_digest", None)
    layout["consumer_layout_digest"] = digest_named_fields(CONSUMER_LAYOUT_RECEIPT_SCHEMA, layout)
    temporary = args.component / "consumer-layout-amended.json"
    _write(temporary, layout)
    os.replace(temporary, layout_path)  # COW; old release's hardlink is never opened for writing.
    by_path[layout_relative].update(file_pin(layout_path))
    manifest["deployment_content_sha256"] = hashlib.sha256(canonical_json_bytes(manifest["components"])).hexdigest()
    manifest["deployment_snapshot_id"] = manifest["release_id"] + "_" + manifest["deployment_content_sha256"][:16]
    manifest.pop("dataset_manifest_sha256", None)
    manifest["dataset_manifest_sha256"] = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    temporary = args.component / "manifest-amended.json"
    _write(temporary, manifest)
    os.replace(temporary, manifest_path)
    identity = manifest["dataset_manifest_sha256"]
    reference = {"id": predecessor_path.name, **file_pin(predecessor_path)}
    plan = {"candidate_root": str(root), "profile_candidate": str(args.output),
        "generation": draft["generation"], "revision": manifest["revision"],
        "release_id": draft["release_id"], "target_cutoff": draft["cutoff"],
        "node_roots": {node: value["candidate_root"] for node, value in draft["node_bindings"].items()},
        "predecessor_profile_ref": reference,
        "predecessor": {"profile_path": str(predecessor_path), "profile_sha256": reference["sha256"],
            "generation": predecessor["generation"], "release_id": predecessor["release_id"],
            "cutoff": predecessor["cutoff"], "candidate_root": predecessor["controller_paths"]["candidate_root"],
            "dataset_manifest_sha256": predecessor["components"]["dataset_manifest_sha256"]}}
    context = ProducerContext(stage="DERIVE", operation_id=("minute-amendment-" if minute_amendment else "margin-amendment-") + identity[:16], attempt=1,
        request={}, plan=plan, prior_receipts={"BUILD": {"scope": {"dataset_manifest_sha256": identity}}})
    derive_receipt = args.component / "derive-scope.json"
    if derive_receipt.exists():
        derive_scope = json.loads(derive_receipt.read_text(encoding="utf-8"))
        if derive_scope.get("dataset_manifest_sha256") != identity:
            raise ValueError("resumed financing DERIVE binds another manifest")
    else:
        derive_scope = dict(runtime.registry.derive.adapter.execute(context).scope)
        _write(derive_receipt, derive_scope)
    registry_path = root / "derived/derived_asset_registry.json"
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    derived_pins = [{"path": "derived/" + value["path"], "sha256": value["sha256"],
                     "size": value["size"]} for value in registry["assets"]]
    manifest_pin = {"path": manifest_path.name, **file_pin(manifest_path)}
    groups = _component_groups(manifest_pin, by_path, derived_pins)
    provenance = root / "provenance"
    provenance.mkdir(exist_ok=True)
    contracts = []
    for consumer in REQUIRED_CONSUMERS:
        requirements = sorted(ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS[consumer])
        path = provenance / f"consumer-contract-{consumer}.json"
        _write(path, {"schema_version": CONSUMER_CONTRACT_SCHEMA, "consumer_id": consumer,
            "dataset_manifest_sha256": identity, "required_components": requirements,
            "component_refs": {name: groups[name] for name in requirements}, "status": "READY_FOR_NODE_READBACK"})
        contracts.append(path)
    source_path, validation_path, lineage_path = [provenance / name for name in (
        "source-readiness.json", "component-validation.json", "release-lineage.json")]
    inherited = {"path": str(Path(correction["predecessor_candidate_root"]) / "release_closure_receipt.json"),
                 "sha256": physical_profile["components"]["release_closure_sha256"]}
    amendment_path = "reports/minute-coverage-amendment.json" if minute_amendment else "reports/late-financing-amendment.json"
    _write(source_path, {"schema_version": SOURCE_READINESS_SCHEMA, "dataset_manifest_sha256": identity,
        "inherited_unchanged_readiness": inherited, "late_fact_amendment_ref": by_path[amendment_path],
        "source_freeze": True, "database_write": False, "unexplained_gap_count": 0, "status": "PASS"})
    _write(validation_path, {"schema_version": COMPONENT_VALIDATION_SCHEMA, "dataset_manifest_sha256": identity,
        "deployment_content_sha256": manifest["deployment_content_sha256"], "manifest_component_count": len(by_path),
        "logical_components": sorted(groups), "derived_asset_count": len(derived_pins),
        "validation_scope": "FULL_EXECUTION_WINDOW_AND_EXACT_MINUTE_REPAIRS" if minute_amendment else "EXACT_LATE_FACT_DATE_AND_INHERITED_PINS", "inherited_unchanged_readiness": inherited,
        **({"minute_full_window_validation_ref": by_path[proof_relative], "minute_cells_filled": correction["filled_cells"]}
           if minute_amendment else {"raw_rows_added": correction["raw_rows_added"], "static_cells_changed": correction["static_cells_changed"]}),
        "historical_content_revalidated": False, "status": "PASS"})
    _write(lineage_path, {"schema_version": RELEASE_LINEAGE_SCHEMA, "dataset_manifest_sha256": identity,
        "release_id": plan["release_id"], "revision": plan["revision"], "cutoff_trade_date": plan["target_cutoff"],
        "predecessor_dataset_manifest_sha256": predecessor["components"]["dataset_manifest_sha256"],
        "component_revision_predecessor_manifest_sha256": correction["predecessor_manifest_sha256"],
        "predecessor_profile_ref": reference, "status": "SEALED_SUCCESSOR"})
    closure_path = root / "release_closure_receipt.json"
    build_release_closure(output_path=closure_path, dataset_manifest_path=manifest_path,
        derived_asset_paths=(registry_path, *(root / pin["path"] for pin in derived_pins)),
        consumer_contract_paths=tuple(contracts), source_readiness_paths=(source_path,),
        component_validation_paths=(validation_path,), lineage_path=lineage_path)
    context = ProducerContext(stage="LOCAL_VALIDATE", operation_id=context.operation_id, attempt=1,
        request={}, plan=plan, prior_receipts={"DERIVE": {"scope": derive_scope}})
    SealedMonthlyProfileCandidateBuilder().execute(context, release_closure_path=closure_path,
        derived_asset_registry_path=registry_path)
    result = {"status": "SEALED_PENDING_NODE_READBACK", "candidate_root": str(root),
        "profile_candidate": str(args.output), "profile_sha256": file_pin(args.output)["sha256"],
        "generation": draft["generation"], "dataset_manifest_sha256": identity,
        "dataset_manifest_file_sha256": file_pin(manifest_path)["sha256"],
        "database_write": False, "active_profile_write": False, "runtime_action": False}
    _write(completed_path, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    cap = sub.add_parser("capture")
    cap.add_argument("--trade-date", required=True)
    cap.add_argument("--env-file", type=Path, required=True)
    cap.add_argument("--output", type=Path, required=True)
    patch = sub.add_parser("build")
    patch.add_argument("--profile", type=Path, required=True)
    patch.add_argument("--capture", type=Path, required=True)
    patch.add_argument("--output", type=Path, required=True)
    assembly = sub.add_parser("assemble")
    assembly.add_argument("--profile", type=Path, required=True)
    assembly.add_argument("--component", type=Path, required=True)
    assembly.add_argument("--output", type=Path, required=True)
    sealing = sub.add_parser("seal")
    sealing.add_argument("--env-file", type=Path, required=True)
    sealing.add_argument("--component", type=Path, required=True)
    sealing.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = {"capture": capture, "build": build, "assemble": assemble, "seal": seal}[args.action](args)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
