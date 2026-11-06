"""Bounded late-financing amendment; never mutate a sealed release or query a provider.

Raw source NULL remains NULL. Static md_* keeps the existing per-field as-of
contract (qe_data_service.LIVE_ASOF_STATIC_PREFIXES); no new fill policy.
"""
from __future__ import annotations

from datetime import date
import hashlib
from pathlib import Path
import shutil
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .monthly_legacy_prefix import _plain_chain, _signature
from .monthly_repair_inputs import deferred_margin_authority

MARGIN_FIELDS = ("rzye", "rqye", "rzmre", "rqyl", "rzche", "rqchl", "rqmcl", "rzrqye")
MARGIN_COLUMNS = tuple("md_" + field for field in MARGIN_FIELDS)
SCHEMA = "aistock_monthly_late_financing_amendment_v1"
READBACK_SCHEMA = "aistock_monthly_late_financing_readback_v1"


def resolve_margin_deferral(payload: Mapping[str, Any] | None, *, trade_date: str,
                            source_available: bool) -> dict[str, Any]:
    """Resolve only this snapshot's effective deferral, not its immutable authorization."""
    authority = deferred_margin_authority(payload, target_cutoff=date.fromisoformat(trade_date))
    deferred = authority is not None and not source_available
    return {"status": "DEFERRED" if deferred else "AVAILABLE",
            "deferred_trade_date": trade_date if deferred else None,
            "original_deferral_authority_sha256": authority}


def effective_margin_deferral(connection, payload, *, trade_date: str):
    """One bounded read in the SOURCE snapshot; ordinary strict audit still runs.

    An old explicit 'collect later' must not hide facts that have since arrived.
    A partial publication is included, then rejected by existing completeness
    checks rather than silently exempted. No provider, DML, or historical scan.
    """
    result = resolve_margin_deferral(payload, trade_date=trade_date, source_available=False)
    if result["deferred_trade_date"] is None:
        return result
    with connection.cursor() as cursor:
        cursor.execute("SELECT EXISTS (SELECT 1 FROM market.margin_detail WHERE trade_date=%s)",
                       (date.fromisoformat(trade_date),))
        available = bool(cursor.fetchone()[0])
    return resolve_margin_deferral(payload, trade_date=trade_date, source_available=available)


def validate_late_financing_adoption(*, predecessor_root, predecessor_profile, candidate,
                                     profile, manifest, evidence, required_nodes):
    """Narrow existing-adoption lane for a same-cutoff v4 financing correction."""
    import json
    from .canonical import canonical_json_bytes

    old = json.loads((predecessor_root / "qe_dataset_manifest.json").read_text(encoding="utf-8"))
    if file_pin(predecessor_root / "qe_dataset_manifest.json")["sha256"] != predecessor_profile["components"]["dataset_manifest_file_sha256"]:
        raise ValueError("late financing predecessor manifest changed")
    for value in (old, manifest):
        unsigned = {key: item for key, item in value.items() if key != "dataset_manifest_sha256"}
        if hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest() != value["dataset_manifest_sha256"]:
            raise ValueError("late financing manifest canonical identity differs")
    repair, proof = evidence[SCHEMA], evidence[READBACK_SCHEMA]
    if (repair["predecessor_manifest_sha256"] != old["dataset_manifest_sha256"]
            or old["dataset_manifest_sha256"] != predecessor_profile["components"]["dataset_manifest_sha256"]
            or repair["trade_date"] != manifest["cutoff_trade_date"]
            or old["release_id"] != manifest["release_id"]
            or predecessor_profile["cutoff"] != profile["cutoff"]):
        raise ValueError("late financing successor lineage differs")
    mutable_manifest = {"revision", "components", "dataset_manifest_sha256", "source_contract",
                        "deployment_content_sha256", "deployment_snapshot_id"}
    if ({key: value for key, value in old.items() if key not in mutable_manifest}
            != {key: value for key, value in manifest.items() if key not in mutable_manifest}):
        raise ValueError("late financing amendment changed unrelated manifest semantics")
    new_source = dict(manifest.get("source_contract", {}))
    amendment = new_source.pop("late_fact_amendment", None)
    if (new_source != old.get("source_contract", {}) or not isinstance(amendment, Mapping)
            or amendment.get("predecessor_manifest_sha256") != old["dataset_manifest_sha256"]
            or amendment.get("trade_date") != repair["trade_date"]):
        raise ValueError("late financing source lineage differs")
    excluded_profile = {"generation", "controller_paths", "node_bindings", "components"}
    if ({key: value for key, value in profile.items() if key not in excluded_profile}
            != {key: value for key, value in predecessor_profile.items() if key not in excluded_profile}):
        raise ValueError("late financing amendment changed profile semantics")
    allowed_pins = {"dataset_manifest_sha256", "dataset_manifest_file_sha256", "factor_meta_sha256",
                    "derived_asset_registry_path", "derived_asset_registry_sha256",
                    "release_closure_path", "release_closure_sha256"}
    if (set(profile["components"]) != set(predecessor_profile["components"])
            or any(profile["components"][key] != value for key, value in predecessor_profile["components"].items()
                   if key not in allowed_pins)):
        raise ValueError("late financing amendment changed unrelated profile pins")
    mutable = {"factor_bundle/" + name for name in ("margin_detail.h5", "static_factors.parquet")}
    mutable |= {"components/factor_h5_static_candidate_v2/" + name for name in (
        "margin_detail.h5", "static_factors.parquet", "meta.json")}
    mutable.add("reports/monthly_consumer_layout_receipt.json")
    added = {"reports/late-financing-amendment.json", "reports/late-financing-source/capture.json",
             "reports/late-financing-source/source.parquet"}
    before = {pin["path"]: pin for pin in old["components"].values()}
    after = {pin["path"]: pin for pin in manifest["components"].values()}
    if (len(before) != len(old["components"]) or len(after) != len(manifest["components"])
            or set(after) != set(before) | added
            or any(after[key] != pin for key, pin in before.items() if key not in mutable)):
        raise ValueError("late financing amendment changed unrelated component pins")
    for relative in mutable | added:
        if file_pin(candidate / relative) != {name: after[relative][name] for name in ("sha256", "size")}:
            raise ValueError("late financing changed component bytes differ")
    if amendment.get("receipt_sha256") != after["reports/late-financing-amendment.json"]["sha256"]:
        raise ValueError("late financing source receipt identity differs")
    for name, pin in repair["files"].items():
        if name not in {"margin_detail.h5", "static_factors.parquet"} or any(
            pin != {field: after[prefix + name][field] for field in ("sha256", "size")}
            for prefix in ("factor_bundle/", "components/factor_h5_static_candidate_v2/")
        ):
            raise ValueError("late financing readback pins differ from manifest")
    if set(repair["files"]) != {"margin_detail.h5", "static_factors.parquet"}:
        raise ValueError("late financing readback pins are incomplete")
    if json.loads((candidate / "reports/late-financing-amendment.json").read_text(encoding="utf-8")) != repair:
        raise ValueError("late financing receipt differs from manifest-pinned proof")
    for flag in ("price_export_performed", "database_write", "active_profile_write", "runtime_action"):
        if repair.get(flag) is not False or proof.get(flag) is not False:
            raise ValueError("late financing proof crossed side-effect boundary")
    if (proof.get("status") != "PASS" or proof.get("dataset_manifest_sha256") != manifest["dataset_manifest_sha256"]
            or set(proof.get("nodes", {})) != set(required_nodes)):
        raise ValueError("late financing node proof is incomplete")
    manifest_file_sha256 = file_pin(candidate / "qe_dataset_manifest.json")["sha256"]
    for node in proof["nodes"].values():
        if (node.get("status") != "PASS" or node.get("dataset_identity_complete") is not True
                or node.get("dataset_manifest_sha256") != manifest["dataset_manifest_sha256"]
                or node.get("manifest_file_sha256") != manifest_file_sha256
                or node.get("financing_rows") != repair["applicable_rows"]
                or node.get("source_mismatch_count") != 0
                or node.get("component_hashes") != repair["files"]):
            raise ValueError("late financing node readback differs")


def file_pin(path: Path) -> dict[str, Any]:
    _plain_chain(path)
    before = _signature(path)
    digest = hashlib.sha256()
    with path.open("rb") as reader:
        for block in iter(lambda: reader.read(8 * 1024 * 1024), b""):
            digest.update(block)
    if _signature(path) != before:
        raise ValueError("amendment file changed while hashing")
    return {"sha256": digest.hexdigest(), "size": before[2]}


def validate_margin_source(source: pd.DataFrame, *, trade_date: str) -> pd.DataFrame:
    day = pd.Timestamp(date.fromisoformat(trade_date))
    if (not isinstance(source.index, pd.MultiIndex) or source.index.names != ["datetime", "instrument"]
            or not source.index.is_unique or source.empty or set(source.columns) != set(MARGIN_COLUMNS)
            or not (source.index.get_level_values("datetime") == day).all()
            or not source.index.get_level_values("instrument").str.fullmatch(r"[0-9]{6}\.(SH|SZ|BJ)").all()):
        raise ValueError("late financing source keys/schema differ")
    numeric = source[list(MARGIN_COLUMNS)].apply(pd.to_numeric, errors="raise")
    values = numeric.to_numpy(dtype="float64")
    # Provider corrections can contain signed rqchl; inherit source semantics,
    # never invent a positivity gate for already admitted database facts.
    if np.isinf(values).any():
        raise ValueError("late financing source contains invalid values")
    return numeric.sort_index()


def amend_margin_files(source_root: Path, output_root: Path, *, source: pd.DataFrame,
                       trade_date: str, instruments: Iterable[str] | None = None,
                       progress=lambda event: None) -> dict[str, Any]:
    """COW H5 append + streaming Parquet rewrite, touching only exact-date md_* cells.

    output_root must be new. Partial output is private, never a READY release.
    Only changed files are hashed; no historical content QA or price export.
    """
    source = validate_margin_source(source, trade_date=trade_date)
    day = pd.Timestamp(trade_date)
    source_root, output_root = source_root.absolute(), output_root.absolute()
    if source_root == output_root or output_root.is_relative_to(source_root) or source_root.is_relative_to(output_root):
        raise ValueError("late financing output must be separate from sealed input")
    _plain_chain(output_root.parent)
    paths = [source_root / "margin_detail.h5", source_root / "static_factors.parquet"]
    for path in paths:
        _plain_chain(path)
    signatures = {path: _signature(path) for path in paths}
    static_day = pd.read_parquet(paths[1], columns=list(MARGIN_COLUMNS),
        filters=[("datetime", "==", day)])
    if not static_day.index.is_unique or static_day.empty:
        raise ValueError("late financing date is outside existing static input")
    population = set(instruments) if instruments is not None else set(static_day.index.get_level_values("instrument"))
    applicable = source[source.index.get_level_values("instrument").isin(population)]
    if applicable.empty:
        raise ValueError("late financing source has no applicable release facts")
    with pd.HDFStore(paths[0], "r") as store:
        template = store.select("data", start=0, stop=0)
        if set(template.columns) != set(MARGIN_COLUMNS):
            raise ValueError("raw financing H5 schema differs")
        old_count = int(store.get_storer("data").nrows)
        existing = store.select("data", where=f"datetime == {trade_date!r}")
    applicable = applicable.reindex(columns=template.columns).astype(template.dtypes.to_dict())
    if not existing.index.is_unique:
        raise ValueError("raw financing H5 has duplicate target keys")
    overlap = existing.index.intersection(applicable.index)
    if len(overlap):
        pd.testing.assert_frame_equal(existing.loc[overlap], applicable.loc[overlap], check_exact=True)
    additions = applicable.loc[~applicable.index.isin(existing.index)]
    output_root.mkdir(exist_ok=False)
    raw_target, static_target = output_root / paths[0].name, output_root / paths[1].name
    shutil.copyfile(paths[0], raw_target)  # Never open an active hardlink for writing.
    with pd.HDFStore(raw_target, "a") as store:
        if len(additions):
            store.append("data", additions, format="table", data_columns=True)
        if int(store.get_storer("data").nrows) != old_count + len(additions):
            raise ValueError("late financing H5 row count differs")
        readback = store.select("data", where=f"datetime == {trade_date!r}")
    pd.testing.assert_frame_equal(readback.loc[applicable.index].sort_index(), applicable.sort_index(), check_exact=True)
    changed_cells = static_rows = rowgroups_changed = 0
    with pq.ParquetFile(paths[1]) as reader, pq.ParquetWriter(static_target, reader.schema_arrow,
                                                          compression="zstd") as writer:
        for group in range(reader.num_row_groups):
            table = reader.read_row_group(group)
            dates = table.column("datetime").to_pandas()
            mask = np.asarray(dates == day)
            if mask.any():
                symbols = np.asarray(table.column("instrument").to_pylist(), dtype=object)[mask]
                index = pd.MultiIndex.from_arrays([[day] * len(symbols), symbols], names=source.index.names)
                facts = applicable.reindex(index)
                static_rows += len(index.intersection(applicable.index))
                for column in MARGIN_COLUMNS:
                    position = table.schema.get_field_index(column)
                    field = table.schema.field(position)
                    values = table.column(column).to_numpy(zero_copy_only=False).copy()
                    old = values[mask].copy()
                    fresh = facts[column].to_numpy(dtype=values.dtype)
                    # Keep the already materialized as-of value only when the actual source field is NULL.
                    use = np.isfinite(fresh)
                    old[use] = fresh[use]
                    changed_cells += int(np.count_nonzero(~((values[mask] == old) | (np.isnan(values[mask]) & np.isnan(old)))))
                    values[mask] = old
                    table = table.set_column(position, field, pa.array(values, type=field.type, from_pandas=True))
                rowgroups_changed += 1
            writer.write_table(table)
            progress({"stage": "static_financing_rewrite", "row_group": group + 1, "row_groups": reader.num_row_groups})
    after_static = pd.read_parquet(static_target, columns=list(MARGIN_COLUMNS), filters=[("datetime", "==", day)])
    expected = static_day.copy()
    for column in MARGIN_COLUMNS:
        values = applicable[column].reindex(expected.index)
        finite = values.notna()
        expected.loc[finite, column] = values.loc[finite].astype(expected[column].dtype)
    pd.testing.assert_frame_equal(after_static, expected, check_exact=True)
    if any(_signature(path) != signature for path, signature in signatures.items()):
        raise ValueError("sealed financing input changed during amendment")
    return {"schema_version": SCHEMA, "status": "PRIVATE_COMPONENTS_NOT_A_RELEASE",
        "trade_date": trade_date, "source_rows": len(source), "applicable_rows": len(applicable),
        "out_of_release_scope_rows": len(source) - len(applicable), "raw_rows_added": len(additions),
        "raw_rows_before": old_count, "raw_rows_after": old_count + len(additions),
        "source_null_fields": {column: int(source[column].isna().sum()) for column in MARGIN_COLUMNS},
        "static_applicable_rows": static_rows, "static_cells_changed": changed_cells,
        "static_row_groups_changed": rowgroups_changed,
        "files": {p.name: file_pin(p) for p in (raw_target, static_target)},
        "historical_content_revalidated": False, "price_export_performed": False,
        "database_write": False, "active_profile_write": False, "runtime_action": False}
