"""Typed proof for a physical-minute successor of an unactivated financing fix.

The active logical predecessor and the deployed physical predecessor are distinct.
No rebuild, database writes, profile activation, or model execution occurs here.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path, PurePosixPath

from .canonical import canonical_json_bytes
from .monthly_late_publication import (
    SCHEMA as FINANCING_SCHEMA,
    READBACK_SCHEMA as FINANCING_READBACK,
    file_pin,
    validate_late_financing_adoption,
)

AMENDMENT = "aistock_monthly_minute_coverage_amendment_v1"
WINDOW = "aistock_monthly_minute_full_window_readback_v1"
READBACK = "aistock_monthly_minute_successor_readback_v1"
SCHEMAS = {AMENDMENT, WINDOW, READBACK}
FIELDS = {
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "factor",
    "up_limit_price",
    "down_limit_price",
    "prev_close",
    "limit_up",
    "limit_down",
}


def require(condition, message):
    if not condition:
        raise ValueError("minute adoption: " + message)


def read(path):
    before = file_pin(path)
    value = json.loads(path.read_bytes())
    require(file_pin(path) == before, "JSON changed")
    return value


def pins(manifest):
    result = {p["path"]: p for p in manifest["components"].values()}
    require(len(result) == len(manifest["components"]), "duplicate pins")
    for relative in result:
        p = PurePosixPath(relative)
        require(
            not p.is_absolute() and ".." not in p.parts and "\\" not in relative and ":" not in relative, "unsafe pin"
        )
    unsigned = {k: v for k, v in manifest.items() if k != "dataset_manifest_sha256"}
    require(
        hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest() == manifest["dataset_manifest_sha256"],
        "manifest identity",
    )
    return result


def validate_fact_amendment(extra, before, after, candidate):
    """Allow exact already-verified fact deltas, never arbitrary component edits."""
    require(isinstance(extra, dict) and extra.get('existing_finite_values_unchanged') is True
            and all(extra.get(k) is False for k in ('database_write', 'active_profile_write', 'runtime_action')),
            'fact amendment safety')
    files = extra.get('files')
    require(isinstance(files, dict) and bool(files), 'fact amendment files')
    allowed = {
        prefix + filename for prefix in ('components/factor_h5_static_candidate_v2/', 'factor_bundle/')
        for filename in ('sector_data.h5', 'static_factors.parquet')
    } | {'components/sector_context_candidate_v1/component_receipt.json'}
    for relative, entry in files.items():
        require(relative in allowed or re.fullmatch(
            r'(components/daily_bin_candidate|daily_bin/qlib)/features/[0-9]{6}\.(sz|sh)/prev_close\.day\.bin', relative),
            'fact amendment path')
        require(entry.get('before') == before.get(relative) and entry.get('after') == after.get(relative)
                and relative in after and file_pin(candidate / relative) == {
                    k: after[relative][k] for k in ('sha256', 'size')}, 'fact amendment pin')
    for relative, pin in extra.get('evidence', {}).items():
        require(relative.startswith('reports/combined-facts/') and '..' not in PurePosixPath(relative).parts
                and '\\' not in relative and ':' not in relative and pin == after.get(relative)
                and file_pin(candidate / relative) == {k: pin[k] for k in ('sha256', 'size')},
                'fact amendment evidence')
    require(isinstance(extra.get('stock_dates'), int) and extra['stock_dates'] > 0, 'fact amendment count')
    return set(files) | set(extra.get('evidence', {}))


def validate_minute_adoption(
    *,
    predecessor_root,
    predecessor_profile,
    predecessor_profile_sha256,
    candidate,
    profile,
    manifest,
    evidence,
    required_nodes,
):
    require(SCHEMAS.issubset(evidence), "typed proofs missing")
    repair, window, proof = (evidence[key] for key in (AMENDMENT, WINDOW, READBACK))
    base = Path(repair["predecessor_candidate_root"])
    require(base.absolute().parent == candidate.absolute().parent and base != candidate, "physical predecessor root")
    old = read(base / "qe_dataset_manifest.json")
    profile_ref = repair["predecessor_profile"]
    physical_profile_path = Path(profile_ref["path"])
    require(file_pin(physical_profile_path) == {k: profile_ref[k] for k in ("sha256", "size")}, "physical profile hash")
    physical_profile = read(physical_profile_path)
    require(Path(physical_profile["controller_paths"]["candidate_root"]) == base, "physical profile root")
    require(
        old["dataset_manifest_sha256"]
        == repair["predecessor_manifest_sha256"]
        == physical_profile["components"]["dataset_manifest_sha256"],
        "physical lineage",
    )
    direct = (base.absolute() == predecessor_root.absolute()
              and profile_ref['sha256'] == predecessor_profile_sha256)
    if not direct:
        validate_late_financing_adoption(
            predecessor_root=predecessor_root, predecessor_profile=predecessor_profile,
            candidate=base, profile=physical_profile, manifest=old,
            evidence={FINANCING_SCHEMA: read(base / 'reports/late-financing-amendment.json'),
                      FINANCING_READBACK: read(base / 'provenance/late-financing-readback.json')},
            required_nodes=required_nodes)
    before, after = pins(old), pins(manifest)
    allowed_manifest = {
        "revision",
        "components",
        "dataset_manifest_sha256",
        "source_contract",
        "deployment_content_sha256",
        "deployment_snapshot_id",
    }
    require(
        {k: v for k, v in old.items() if k not in allowed_manifest}
        == {k: v for k, v in manifest.items() if k not in allowed_manifest},
        "manifest semantics",
    )
    source = dict(manifest["source_contract"])
    amendment = source.pop("minute_coverage_amendment", None)
    require(
        source == {k: v for k, v in old['source_contract'].items() if k != 'minute_coverage_amendment'}
        and isinstance(amendment, dict)
        and amendment["predecessor_manifest_sha256"] == old["dataset_manifest_sha256"],
        "source lineage",
    )
    exclude = {"generation", "controller_paths", "node_bindings", "components"}
    require(
        {k: v for k, v in profile.items() if k not in exclude}
        == {k: v for k, v in physical_profile.items() if k not in exclude},
        "profile semantics",
    )
    allowed = {
        "dataset_manifest_sha256",
        "dataset_manifest_file_sha256",
        "minute_pins",
        "derived_asset_registry_path",
        "derived_asset_registry_sha256",
        "release_closure_path",
        "release_closure_sha256",
    }
    extra = repair.get('additional_fact_amendment')
    if extra is not None:
        allowed.add('sector_context_pins')
        old_sector = physical_profile['components']['sector_context_pins']
        new_sector = profile['components']['sector_context_pins']
        require(set(old_sector) == set(new_sector) and all(
            new_sector[k] == v for k, v in old_sector.items()
            if k not in {'sector_data_sha256', 'receipt_sha256'}), 'industry authority changed')
        require(new_sector['sector_data_sha256'] == after[
            'components/factor_h5_static_candidate_v2/sector_data.h5']['sha256']
            and new_sector['receipt_sha256'] == after[
                'components/sector_context_candidate_v1/component_receipt.json']['sha256'], 'industry profile pin')
    require(
        set(profile["components"]) == set(physical_profile["components"])
        and all(profile["components"][k] == v for k, v in physical_profile["components"].items() if k not in allowed),
        "unrelated profile pins",
    )
    require(
        profile["components"]["minute_pins"]["calendar_sha256"]
        == physical_profile["components"]["minute_pins"]["calendar_sha256"]
        == repair["calendar_sha256"],
        "calendar drift",
    )
    require(
        set(repair["fields"]) == FIELDS and repair.get("existing_finite_values_unchanged") is True, "field contract"
    )
    dates = {(r["ts_code"], r["trade_date"]) for r in repair["stock_dates"]}
    require(len(dates) == len(repair["stock_dates"]) and bool(dates), "dates invalid")
    symbols = {s.lower() for s, _ in dates}
    mutable = set()
    for relative, pin in repair["files"].items():
        p = PurePosixPath(relative)
        require(
            len(p.parts) == 3
            and p.parts[0] == "features"
            and p.parts[1] in symbols
            and p.parts[2] in {f + ".1min.bin" for f in FIELDS},
            "unexpected repaired file",
        )
        for prefix in ("components/minute_bin_candidate/", "minute_bin/qlib/"):
            name = prefix + relative
            mutable.add(name)
            require({k: after[name][k] for k in ("sha256", "size")} == pin["after"], "repair pin mismatch")
            require(
                (None if name not in before else {k: before[name][k] for k in ("sha256", "size")}) == pin["before"],
                "predecessor bytes mismatch",
            )
    require(len(repair["files"]) == len(symbols) * len(FIELDS), "incomplete per-symbol fields")
    for prefix in ("components/minute_bin_candidate/", "minute_bin/qlib/"):
        mutable.update(prefix + n for n in ("instruments/all.txt", "meta_export.json"))
    mutable.update(
        {
            "reports/minute-coverage-amendment.json",
            "reports/minute-coverage-source.json",
            "minute_bin/materialization_receipt.json",
            "reports/minute-full-window-validation.json",
            "reports/monthly_consumer_layout_receipt.json",
        }
    )
    if extra is not None:
        mutable.update(validate_fact_amendment(extra, before, after, candidate))
    require(
        set(before) <= set(after)
        and set(after) - set(before) <= mutable
        and all(after[p] == v for p, v in before.items() if p not in mutable),
        "unrelated component change",
    )
    for path in mutable:
        require(
            path in after and file_pin(candidate / path) == {k: after[path][k] for k in ("sha256", "size")},
            "changed bytes drift",
        )
    require(
        read(candidate / "reports/minute-coverage-amendment.json") == repair
        and amendment["receipt_sha256"] == after["reports/minute-coverage-amendment.json"]["sha256"],
        "repair receipt binding",
    )
    require(
        read(candidate / "reports/minute-full-window-validation.json") == window
        and window["status"] == "PASS"
        and window["physical_unexplained_gap_days_after"] == 0
        and window["physical_gap_days_before"] == len(dates)
        and window["predecessor_manifest_sha256"] == old["dataset_manifest_sha256"]
        and window["active_manifest_sha256"] == predecessor_profile["components"]["dataset_manifest_sha256"],
        "full window closure",
    )
    for value in (repair, window, proof):
        require(
            all(value.get(k) is False for k in ("database_write", "active_profile_write", "runtime_action")),
            "side effects",
        )
    manifest_pin = file_pin(candidate / "qe_dataset_manifest.json")
    require(
        proof["status"] == "PASS"
        and proof["dataset_manifest_sha256"] == manifest["dataset_manifest_sha256"]
        and proof["physical_predecessor_profile_sha256"] == profile_ref["sha256"]
        and proof["logical_predecessor_profile_sha256"] == predecessor_profile_sha256
        and set(proof["nodes"]) == set(required_nodes),
        "node proof identity",
    )
    observed_coverage = None
    for node in proof["nodes"].values():
        coverage = node.get('consumer_coverage', {})
        original_coverage = window['consumer_coverage']
        require({k: v for k, v in coverage.items() if k != 'suspension_explained_days'}
                == {k: v for k, v in original_coverage.items() if k != 'suspension_explained_days'}
                and set(coverage) == set(original_coverage)
                and isinstance(coverage.get('suspension_explained_days', 0), int)
                and 0 <= coverage.get('suspension_explained_days', 0) <= original_coverage.get('suspension_explained_days', 0)
                and (observed_coverage is None or observed_coverage == coverage), 'consumer population/coverage drift')
        observed_coverage = coverage
        require(
            node.get("status") == "PASS"
            and node.get("dataset_identity_complete") is True
            and node["manifest"] == manifest["dataset_manifest_sha256"]
            and node["manifest_file_sha256"] == manifest_pin["sha256"]
            and node["stock_dates_verified"] == len(dates)
            and node["field_cells_verified"] == repair["checked_cells"]
            and node["qrun_sha256"] == window["consumer_qrun_sha256"]
            and coverage == observed_coverage,
            "node exact readback",
        )
        if extra is not None:
            require(node.get('fact_amendment_files') == {
                p: item['after']['sha256'] for p, item in extra['files'].items()
            } and node.get('fact_stock_dates_verified') == extra['stock_dates'], 'node fact readback')
    if direct:
        require(proof.get('supersedes_operation_id') is None, 'direct successor supersedes an unrelated operation')
        return None
    operation = proof["supersedes_operation_id"]
    require(
        isinstance(operation, str)
        and len(operation) == 36
        and operation.startswith("dmr_")
        and all(c in "0123456789abcdef" for c in operation[4:]),
        "superseded operation id",
    )
    return {"operation_id": operation, "physical_profile_sha256": profile_ref["sha256"]}


def verify_pending_predecessor(service, supersession, logical_hash, target_hash):
    from .monthly_unified import MonthlyReleaseConflict

    root = service.store.operation_root(supersession["operation_id"])
    state = service.store.read_state(supersession["operation_id"])
    plan = service.store.read_plan(supersession["operation_id"])
    ready = read(root / "receipts/ready.json")
    if (
        (root / "receipts/activation.json").exists()
        or plan["predecessor"]["profile_sha256"] != logical_hash
        or ready["candidate_profile_ref"]["sha256"] != supersession["physical_profile_sha256"]
        or file_pin(Path(plan["profile_candidate"]))["sha256"] != supersession["physical_profile_sha256"]
    ):
        raise MonthlyReleaseConflict("minute physical predecessor is not the exact pending candidate")
    marker = root / "receipts/superseded.json"
    if marker.exists():
        receipt = read(marker)
        if (
            receipt.get("target_profile_sha256") != target_hash
            or receipt.get("logical_predecessor_profile_sha256") != logical_hash
            or state["status"] not in {"READY_TO_ACTIVATE", "CANCELLED"}
        ):
            raise MonthlyReleaseConflict("pending candidate already superseded differently")
    elif state["status"] != "READY_TO_ACTIVATE":
        raise MonthlyReleaseConflict("minute physical predecessor is not unactivated READY")


def retire_pending_predecessor(service, supersession, successor_id, logical_hash, target_hash):
    from .monthly_unified import MonthlyReleaseConflict, _exclusive_lock, _write_exclusive

    operation = supersession["operation_id"]
    root = service.store.operation_root(operation)
    with _exclusive_lock(root / ".writer.lock", blocking=False):
        verify_pending_predecessor(service, supersession, logical_hash, target_hash)
        successor = service.store.read_state(successor_id)
        if successor["status"] != "READY_TO_ACTIVATE":
            raise MonthlyReleaseConflict("replacement is not durably ready")
        receipt = {
            "schema_version": "aistock_monthly_pending_successor_v1",
            "operation_id": operation,
            "successor_operation_id": successor_id,
            "target_profile_sha256": target_hash,
            "logical_predecessor_profile_sha256": logical_hash,
            "physical_predecessor_profile_sha256": supersession["physical_profile_sha256"],
            "old_request_plan_ready_and_candidate_retained": True,
            "active_profile_write": False,
        }
        marker = root / "receipts/superseded.json"
        if marker.exists():
            if read(marker) != receipt:
                raise MonthlyReleaseConflict("supersession receipt differs")
        else:
            _write_exclusive(marker, receipt)
        if service.store.read_state(operation)["status"] != "CANCELLED":
            service.store.update_state(
                operation,
                status="CANCELLED",
                cancel_requested=True,
                last_error={
                    "code": "superseded_by_verified_immutable_successor",
                    "successor_operation_id": successor_id,
                },
            )


def guard_minute_activation(service, operation_id):
    """An interrupted supersession must block either candidate's activation."""
    from .monthly_unified import MonthlyReleaseNotReady

    root = service.store.operation_root(operation_id)
    if (root / "receipts/superseded.json").exists():
        raise MonthlyReleaseNotReady("superseded candidate cannot be activated")
    request = read(root / "request.json")
    predecessor = request.get("supersedes_pending_operation")
    if predecessor:
        oldroot = service.store.operation_root(predecessor["operation_id"])
        marker = oldroot / "receipts/superseded.json"
        if (
            service.store.read_state(predecessor["operation_id"])["status"] != "CANCELLED"
            or not marker.is_file()
            or read(marker).get("successor_operation_id") != operation_id
            or read(marker).get("target_profile_sha256") != request["target_profile_sha256"]
        ):
            raise MonthlyReleaseNotReady("minute supersession is not finalized")
    # request.json is written first. Even a crash before the retirement marker
    # cannot make the old READY candidate activatable again.
    for child in service.store.operations.iterdir():
        path = child / "request.json"
        if path.is_file() and read(path).get("supersedes_pending_operation", {}).get("operation_id") == operation_id:
            raise MonthlyReleaseNotReady("candidate has a pending immutable successor")
