"""Version-qualified historical classification, never a SW2021 backcast.

The small authority is frozen by SOURCE. BUILD reads that authority only;
prices and population are not reconstructed here. An explicit history repair
rebuilds sector context from independent frozen quotes and PIT stock flows.
SW2021 release ids remain unchanged. Identical official published text codes
reuse their shared ids; old-only SW2014 codes extend that shared namespace.
Version-qualified membership and dictionaries remain independently frozen.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import hashlib
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .canonical import canonical_json_bytes, ensure_sha256
from .errors import SourceManifestError
from .shared_sector_context import validate_release_sw_l2_code_map

SCHEMA = "aistock_release_versioned_sw_l2_history_v1"
BOUNDARY = date(2021, 7, 30)
STATIC_SECTOR_CONTRACT = "frozen_sector_exact_date_static_v1"
ROW_FIELDS = ("canonical_symbol", "taxonomy_version", "published_l2_code", "effective_from",
              "effective_to", "known_from", "knowledge_time_quality", "source_sha256", "source_row_sha256")


@dataclass(frozen=True)
class IndustryHistoryProjection:
    payload: Mapping[str, Any]
    _by_symbol: Mapping[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = field(repr=False)

    @classmethod
    def build(cls, rows: Sequence[Mapping[str, Any]], *, current_map: Mapping[str, Any],
              authority_sha256: str, historical_codes: Sequence[str] | None = None):
        ensure_sha256(authority_sha256, field="industry history authority")
        current = validate_release_sw_l2_code_map(current_map)
        old_codes = sorted(set(historical_codes if historical_codes is not None else
                               [str(row["published_l2_code"]) for row in rows if row["taxonomy_version"] == "SW2014"]))
        if not rows or len(current.id_to_code) != 131:
            raise SourceManifestError("industry history catalog is empty or invalid")
        ids = {("SW2021", code): id_ for code, id_ in current.code_to_id.items()}
        old_only = [code for code in old_codes if code not in current.code_to_id]
        next_id = max(current.id_to_code) + 1  # The official catalog has holes; never densify it.
        if next_id + len(old_only) > 32767:
            raise SourceManifestError("industry history exceeds shared storage precision")
        old_extension = {code: next_id + i for i, code in enumerate(old_only)}
        ids.update({("SW2014", code): current.code_to_id.get(code, old_extension.get(code)) for code in old_codes})
        if len({value for value in ids.values()}) != len({code for _, code in ids}):
            raise SourceManifestError("industry history assigns one shared id to distinct official codes")
        entries = [{"taxonomy_version": version, "canonical_l2_code": code, "l2_code_id": id_}
                   for (version, code), id_ in sorted(ids.items(), key=lambda item: item[1])]
        normalized = []
        grouped = {}
        for row in rows:
            try:
                value = {key: row[key] for key in ROW_FIELDS}
                start, end = (date.fromisoformat(str(value[key])) for key in ("effective_from", "effective_to"))
                version, code = value["taxonomy_version"], value["published_l2_code"]
                if start > end or version not in {"SW2014", "SW2021"} or (version == "SW2014" and end >= BOUNDARY) or (version == "SW2021" and start < BOUNDARY):
                    raise ValueError("taxonomy interval crosses its real publication boundary")
                if (version, code) not in ids:
                    raise ValueError("classification has no authority code")
                if value["known_from"] is not None or value["knowledge_time_quality"] != "FIRST_PUBLICATION_UNVERIFIED":
                    raise ValueError("recovered effective history must not invent as-known time")
                for key in ("source_sha256", "source_row_sha256"):
                    ensure_sha256(value[key], field=key)
                symbol = value["canonical_symbol"]
                if not isinstance(symbol, str) or len(symbol) != 9 or symbol[6:] not in {".SH", ".SZ"} or not symbol[:6].isdigit():
                    raise ValueError("canonical symbol is invalid")
                value["l2_code_id"] = ids[(version, code)]
                normalized.append(value)
                grouped.setdefault(symbol, []).append((start, end, value["l2_code_id"]))
            except (KeyError, TypeError, ValueError) as exc:
                raise SourceManifestError(f"industry history row is invalid: {exc}") from exc
        arrays = {}
        for symbol, spans in grouped.items():
            spans.sort()
            if any(left[1] >= right[0] for left, right in zip(spans, spans[1:])):
                raise SourceManifestError(f"industry history overlaps: {symbol}")
            arrays[symbol] = (np.array([np.datetime64(s[0], 'D') for s in spans]),
                              np.array([np.datetime64(s[1], 'D') for s in spans]),
                              np.array([s[2] for s in spans], dtype=np.int16))
        payload = {"schema_version": SCHEMA, "authority_sha256": authority_sha256,
                   "current_code_map_digest": current_map["code_map_digest"],
                   "current_code_map": dict(current_map), "entries": entries,
                   "effective_intervals": sorted(normalized, key=lambda row: (row["canonical_symbol"], row["effective_from"])),
                   "start_date": min(row["effective_from"] for row in normalized),
                   "end_date": max(row["effective_to"] for row in normalized),
                   "known_from_inferred": False, "account_economics_inferred": False}
        return cls(payload, arrays)

    @classmethod
    def read(cls, payload: Mapping[str, Any]):
        if payload.get("schema_version") != SCHEMA:
            raise SourceManifestError("industry history schema differs")
        rebuilt = cls.build(payload["effective_intervals"], current_map=payload["current_code_map"],
                            authority_sha256=payload["authority_sha256"],
                            historical_codes=[entry["canonical_l2_code"] for entry in payload["entries"] if entry["taxonomy_version"] == "SW2014"])
        if canonical_json_bytes(rebuilt.payload) != canonical_json_bytes(payload):
            raise SourceManifestError("industry history projection digest or fields differ")
        return rebuilt

    @property
    def sha256(self):
        return hashlib.sha256(canonical_json_bytes(self.payload)).hexdigest()

    @property
    def data_sha256(self):
        return hashlib.sha256(canonical_json_bytes({key: self.payload[key] for key in (
            "authority_sha256", "entries", "effective_intervals")})).hexdigest()

    def resolve(self, symbol: str, day: date) -> int:
        spans = self._by_symbol.get(symbol)
        if spans is None:
            return -1
        starts, ends, ids = spans
        value = np.datetime64(day, "D")
        location = int(np.searchsorted(starts, value, side="right")) - 1
        return int(ids[location]) if location >= 0 and value <= ends[location] else -1

    def project(self, frame: pd.DataFrame) -> pd.DataFrame:
        if tuple(frame.index.names) != ("datetime", "instrument") or frame.index.has_duplicates or "l2_code_id" not in frame:
            raise SourceManifestError("industry history target index/field differs")
        result = frame.copy()
        days = frame.index.get_level_values("datetime").to_numpy(dtype="datetime64[D]")
        symbols = frame.index.get_level_values("instrument")
        selected = (days >= np.datetime64(self.payload["start_date"])) & (days <= np.datetime64(self.payload["end_date"]))
        values = result["l2_code_id"].to_numpy(copy=True)
        values[selected] = -1  # No current classification carried into an unproven history gap.
        for symbol, positions in pd.Series(np.flatnonzero(selected), index=symbols[selected]).groupby(level=0, sort=False):
            spans = self._by_symbol.get(symbol)
            if spans is None:
                continue
            locations = positions.to_numpy()
            starts, ends, ids = spans
            match = np.searchsorted(starts, days[locations], side="right") - 1
            found = match >= 0
            found &= days[locations] <= ends[np.maximum(match, 0)]
            values[locations[found]] = ids[match[found]]
        result["l2_code_id"] = values.astype(frame["l2_code_id"].dtype)
        return result


def capture_industry_history(connection, *, current_map: Mapping[str, Any]) -> IndustryHistoryProjection | None:
    """Read immutable typed facts in the already imported read-only SOURCE transaction.

    No latest-row heuristic. One approved bundle is required. The canonical
    monthly producer cannot silently fall back to the old compatibility table.
    """
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass('market.sw_industry_classification_history_v1')")
        if cursor.fetchone()[0] is None:
            raise SourceManifestError("immutable industry history authority is unavailable")
        cursor.execute("SELECT DISTINCT bundle_sha256 FROM market.sw_industry_classification_history_v1")
        bundles = [row[0] for row in cursor.fetchall()]
        if len(bundles) != 1:
            raise SourceManifestError("industry history needs an explicit single approved bundle")
        authority_sha = bundles[0]
        cursor.execute("SELECT payload FROM market.sw_industry_classification_history_v1 WHERE bundle_sha256=%s ORDER BY ts_code,effective_from", (authority_sha,))
        rows = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT published_index_code FROM market.sw_industry_taxonomy_history_v1 WHERE bundle_sha256=%s AND taxonomy_version='SW2014' AND level=2 ORDER BY published_index_code", (authority_sha,))
        codes = [row[0] for row in cursor.fetchall()]
    if not codes or any(not isinstance(code, str) or not code.endswith(".SI") for code in codes):
        raise SourceManifestError("historical published taxonomy crosswalk is missing")
    return IndustryHistoryProjection.build(rows, current_map=current_map, authority_sha256=authority_sha, historical_codes=codes)


def load_frozen_history(cas, *, context_plan: Mapping[str, Any], build_inputs: Mapping[str, Any]):
    """The old sealed price SOURCE is never replaced by a late history repair."""
    reference = context_plan.get("monthly_industry_history_ref")
    if reference is not None:
        binding = context_plan.get("monthly_industry_history")
        if not isinstance(binding, Mapping) or hashlib.sha256(canonical_json_bytes(binding) + b"\n").hexdigest() != reference.get("sha256"):
            raise SourceManifestError("industry history supplement bytes differ")
        if binding.get("schema_version") != "aistock_monthly_industry_history_binding_v1" or any(
            binding.get(key) != expected for key, expected in {
                "operation_id": context_plan["operation_id"],
                "target_cutoff": context_plan["target_cutoff"],
                "predecessor_manifest_sha256": context_plan["predecessor"]["dataset_manifest_sha256"],
                "source_bundle_sha256": build_inputs["source_bundle_sha256"],
            }.items()
        ):
            raise SourceManifestError("industry history supplement is not bound to this sealed SOURCE")
        return IndustryHistoryProjection.read(binding["projection"])
    manifest_ref = build_inputs.get("source_manifest_ref")
    if manifest_ref is None:
        return None
    source = cas.get_json_bounded(manifest_ref, max_bytes=16 * 1024 * 1024)
    history = (source.get("monthly_sector_mapping") or {}).get("historical_classification")
    return IndustryHistoryProjection.read(history) if history is not None else None


def _history_scratch_paths(root: Path, projection_sha: str, context_sha: str | None) -> tuple[Path, ...]:
    """Portable private name, with exact legacy paths retained for recovery.

    Bind the full root name and both full digests; never truncate an identity.
    Receipts still carry the original projection/context and file signatures.
    """
    ensure_sha256(projection_sha, field="industry projection digest")
    if context_sha is not None:
        ensure_sha256(context_sha, field="industry context digest")
    identity = {"root_name": root.name, "projection_sha256": projection_sha, "context_sha256": context_sha}
    digest = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
    suffix = "" if context_sha is None else f"-{context_sha}"
    return (root.parent / f".industry-history-{digest}",
            root.parent / f".{root.name}.industry-history-{projection_sha}{suffix}")


def _align_sector_tail(frame: pd.DataFrame, facts: pd.DataFrame, *, after: str) -> pd.DataFrame:
    """Exact-date industry facts outside an explicitly rebuilt history window.

    A stock's previous quote, cash flow or classification is never evidence
    for a new date. An absent source row retains explicit NA, without an
    invented assignment. Historical rows and non-sector columns are untouched.
    """
    selected = frame.index.get_level_values("datetime") > pd.Timestamp(after)
    if not selected.any():
        return frame
    columns = [name for name in frame if name == "l2_code_id" or name.startswith("sw2_")]
    if facts.index.has_duplicates or set(columns).difference(facts):
        raise SourceManifestError("exact-date sector source keys or fields differ")
    actual = frame.copy()
    aligned = facts[columns].reindex(frame.index[selected])
    for name in columns:
        values = aligned[name]
        if name == "l2_code_id":
            values = values.fillna(-1)
        if np.isinf(values.to_numpy(dtype=float)).any():
            raise SourceManifestError("exact-date sector source is infinite")
        actual.loc[selected, name] = values.to_numpy().astype(frame[name].dtype)
    return actual


def _materialize_static_tail(root: Path, *, projection: IndustryHistoryProjection,
                             context_identity: Mapping[str, Any], after: str,
                             max_rows=100_000, checkpoint=lambda: None,
                             progress=lambda _value: None):
    """Correct a completed old writer's static tail; never rewrite sector/prices."""
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq
    from .monthly_build_resume import _plain, _signature
    from .monthly_file_identity import file_sha256

    if not 0 < max_rows <= 100_000:
        raise SourceManifestError("static sector repair row bound differs")
    factor = root / "factor_bundle"
    _plain(root)
    if (root / "qe_dataset_manifest.json").exists():
        raise SourceManifestError("static sector repair cannot change a published candidate")
    inputs = {name: _signature(factor / name) for name in ("sector_data.h5", "static_factors.parquet")}
    context_sha = hashlib.sha256(canonical_json_bytes(context_identity)).hexdigest()
    scratch = _history_scratch_paths(root, projection.sha256, context_sha)[0]
    _plain(scratch)
    receipt = scratch / "projection_receipt.json"
    target = scratch / "static_factors.parquet"
    if scratch.exists():
        _plain(receipt)
        if not receipt.is_file() or receipt.stat().st_size > 1024 * 1024:
            raise SourceManifestError("incomplete static sector output retained")
        result = json.loads(receipt.read_bytes())
        if (result.get("context_identity") != dict(context_identity)
                or result.get("input_signatures") != inputs
                or result.get("static_sector_join_contract") != STATIC_SECTOR_CONTRACT
                or result.get("files", {}).get("static_factors.parquet", {}).get("target_signature") != _signature(target)):
            raise SourceManifestError("retained static sector output identity differs")
        return result
    scratch.mkdir(exist_ok=False)
    changed_cells = aligned_rows = 0
    with pd.HDFStore(factor / "sector_data.h5", "r") as sector, pq.ParquetFile(factor / "static_factors.parquet") as source:
        with pq.ParquetWriter(target, source.schema_arrow) as writer:
            for batch in source.iter_batches(batch_size=max_rows):
                checkpoint()
                frame = pa.Table.from_batches([batch]).to_pandas()
                days = frame.index.get_level_values("datetime")
                selected = days > pd.Timestamp(after)
                actual = frame
                if selected.any():
                    columns = [name for name in frame if name == "l2_code_id" or name.startswith("sw2_")]
                    left, right = str(days[selected].min().date()), str(days[selected].max().date())
                    facts = sector.select("data", where=[f"datetime >= {left!r}", f"datetime <= {right!r}"], columns=columns)
                    actual = _align_sector_tail(frame, facts, after=after)
                    before, updated = frame[columns], actual[columns]
                    changed_cells += int((~(before.eq(updated) | (before.isna() & updated.isna()))).to_numpy().sum())
                    aligned_rows += int(selected.sum())
                    unrelated = [name for name in frame if name not in columns]
                    pd.testing.assert_frame_equal(frame[unrelated], actual[unrelated], check_exact=True)
                writer.write_table(pa.Table.from_pandas(actual, schema=source.schema_arrow, preserve_index=True))
                progress({"phase": "SECTOR_STATIC_EXACT_DATE_REPAIR", "dataset": "static_factors",
                          "rows_validated": aligned_rows, "rows_sealed": changed_cells})
    if any(_signature(factor / name) != value for name, value in inputs.items()):
        raise SourceManifestError("static sector repair input changed")
    # Read the affected month back through the real Parquet reader. This is
    # not a historical business audit or a price re-export.
    with pq.ParquetFile(target) as verified:
        columns = [name for name in verified.schema_arrow.names if name == "l2_code_id" or name.startswith("sw2_")]
    check = pq.read_table(target, columns=["datetime", "instrument", *columns],
                          filters=[("datetime", ">", pd.Timestamp(after))]).to_pandas()
    if not check.empty:
        left = str(check.index.get_level_values("datetime").min().date())
        right = str(check.index.get_level_values("datetime").max().date())
        with pd.HDFStore(factor / "sector_data.h5", "r") as sector:
            facts = sector.select("data", where=[f"datetime >= {left!r}", f"datetime <= {right!r}"], columns=columns)
        pd.testing.assert_frame_equal(check, _align_sector_tail(check, facts, after=after), check_exact=True)
    if any(_signature(factor / name) != value for name, value in inputs.items()):
        raise SourceManifestError("static sector repair input changed during readback")
    result = {"static_sector_join_contract": STATIC_SECTOR_CONTRACT,
              "context_identity": dict(context_identity), "input_signatures": inputs,
              "after": after, "aligned_rows": aligned_rows, "changed_cells": changed_cells,
              "files": {"static_factors.parquet": {
                  "sha256": file_sha256(target), "size": target.stat().st_size,
                  "target_signature": _signature(target), "original_signature": inputs["static_factors.parquet"],
                  "context_sha256": context_sha, "scratch_relative_path": target.relative_to(root.parent).as_posix()}}}
    with receipt.open("xb") as writer:
        writer.write(canonical_json_bytes(result) + b"\n")
    return result


def materialize_history_files(root: Path, projection: IndustryHistoryProjection, *, max_rows=100_000,
                              checkpoint=lambda: None, progress=lambda _value: None,
                              context_rebuilder=None, context_identity=None):
    """Produce two private replacement files, not a second dataset or price export.

    The sector population is the existing frozen sector rows UNION existing
    frozen price rows in the approved history window. Sector values come from
    independently sealed quotes and same-day PIT stock flows; unavailable facts
    remain NA. All unrelated static columns remain byte-value equivalent.
    """
    from .monthly_legacy_prefix import _plain_chain, _signature
    from .monthly_file_identity import file_sha256
    from .monthly_industry_static import materialize_static, read_checkpoint, seal_checkpoint

    if not 0 < max_rows <= 100_000:
        raise SourceManifestError("industry repair row bound differs")
    if (context_rebuilder is None) != (context_identity is None):
        raise SourceManifestError("industry context producer and identity must be supplied together")
    root = Path(root)
    factor = root / "factor_bundle"
    context_sha = None if context_identity is None else hashlib.sha256(canonical_json_bytes(context_identity)).hexdigest()
    candidates = _history_scratch_paths(root, projection.sha256, context_sha)
    # Oversized legacy names cannot be files on Windows/EXT4. Avoid stat'ing
    # those invalid names; valid legacy outputs are verified, never discarded.
    retained = [path for path in candidates if len(path.name) <= 255 and path.exists()]
    if len(retained) > 1:
        raise SourceManifestError("industry repair private output identity is ambiguous")
    scratch = retained[0] if retained else candidates[0]
    _plain_chain(factor)
    if (root / "qe_dataset_manifest.json").exists():
        raise SourceManifestError("industry repair cannot change a published candidate")
    for name in ("sector_data.h5", "static_factors.parquet", "daily_pv.h5"):
        _plain_chain(factor / name)
    names = ["sector_data.h5", "static_factors.parquet", "daily_pv.h5"]
    if context_rebuilder is not None:
        names += ["moneyflow.h5", "security_source_identity.json"]
        for name in names:
            _plain_chain(factor / name)
    originals = {name: _signature(factor / name) for name in names}
    if scratch.exists() and (scratch / "projection_receipt.json").exists():
        import json
        completed = scratch / "projection_receipt.json"
        _plain_chain(completed)
        if not completed.is_file() or completed.stat().st_size > 16 * 1024 * 1024:
            raise SourceManifestError("incomplete industry repair retained; do not overwrite completed exports")
        value = json.loads(completed.read_bytes())
        if (value.get("status") != "PASS" or value.get("projection_sha256") != projection.sha256 or
            value.get("context_identity") != context_identity or
            IndustryHistoryProjection.read(value["projection"]).sha256 != projection.sha256 or
            value.get("input_signatures") != {name: list(signature) for name, signature in originals.items()}):
            raise SourceManifestError("retained industry repair identity differs")
        for name, pin in value["files"].items():
            if name not in {"sector_data.h5", "static_factors.parquet"} or list(_signature(scratch / name)) != pin["target_signature"]:
                raise SourceManifestError("retained industry output changed")
        if set(value["files"]) != {"sector_data.h5", "static_factors.parquet"}:
            raise SourceManifestError("retained industry output population differs")
        progress({"phase": "INDUSTRY_HISTORY_REUSE_VERIFIED"})
        return value
    scratch.mkdir(exist_ok=True)
    outputs = {}
    counts = {"classification_cells_changed": 0, "classification_only_rows_added": 0}
    sector_target = scratch / "sector_data.h5"
    binding = {"schema_version": "industry_private_checkpoint_v1", "projection_sha256": projection.sha256,
               "context_identity": context_identity,
               "input_signatures": {name: list(signature) for name, signature in originals.items()}}
    stage_path = scratch / "sector_stage_receipt.json"
    retained_sector = read_checkpoint(stage_path, binding=binding, output=sector_target)
    left, right = projection.payload["start_date"], projection.payload["end_date"]
    def remap_context(frame, original):
        """Same-day frozen industry facts, not another industry's substitute.

        Reassign all sector fields when classification changes, otherwise a
        corrected id would silently label the old industry's quote/moneyflow.
        Never cross a date or taxonomy boundary. Missing facts remain NA.
        """
        if context_rebuilder is not None:
            return _align_sector_tail(context_rebuilder(frame), original, after=projection.payload["end_date"])
        if any(name.startswith("sw2_mf_") for name in frame):
            raise SourceManifestError("industry membership repair requires frozen PIT-cohort moneyflow and official quotes")
        actual = projection.project(frame)
        columns = [name for name in frame if name.startswith("sw2_")]
        if not columns:
            return _align_sector_tail(actual, original, after=projection.payload["end_date"])
        facts = original.reset_index()[["datetime", "l2_code_id", *columns]].drop_duplicates()
        if facts.duplicated(["datetime", "l2_code_id"]).any():
            raise SourceManifestError("frozen industry reference facts disagree for the same date/code")
        lookup = facts.set_index(["datetime", "l2_code_id"])
        days = frame.index.get_level_values("datetime")
        selected = (days >= pd.Timestamp(left)) & (days <= pd.Timestamp(right))
        keys = pd.MultiIndex.from_arrays([days[selected], actual.loc[selected, "l2_code_id"]], names=["datetime", "l2_code_id"])
        resolved = lookup.reindex(keys)
        for column in columns:
            actual.loc[selected, column] = resolved[column].to_numpy().astype(frame[column].dtype)
        return _align_sector_tail(actual, original, after=projection.payload["end_date"])
    try:
        if retained_sector is None:
            with pd.HDFStore(factor / "sector_data.h5", "r") as old, pd.HDFStore(factor / "daily_pv.h5", "r") as price, pd.HDFStore(sector_target, "w") as new:
                template = old.select("data", start=0, stop=1)
                columns, dtypes = list(template.columns), template.dtypes.to_dict()
                # Only the explicitly requested classification repair; no
                # full-price scan. Price values are never read here.
                for period in pd.period_range(left, right, freq="M"):
                    checkpoint()
                    a = max(pd.Timestamp(left), period.start_time)
                    b = min(pd.Timestamp(right), period.end_time.normalize())
                    where = [f"datetime >= {str(a.date())!r}", f"datetime <= {str(b.date())!r}"]
                    existing = old.select("data", where=where)
                    keys = price.select("data", where=where, columns=[]).index
                    union = existing.index.union(keys).sort_values()
                    for offset in range(0, len(union), max_rows):
                        checkpoint()
                        index = union[offset:offset + max_rows]
                        frame = existing.reindex(index)
                        for column in columns:
                            if pd.api.types.is_integer_dtype(dtypes[column]):
                                frame[column] = frame[column].fillna(-1).astype(dtypes[column])
                            else:
                                frame[column] = frame[column].astype(dtypes[column])
                        actual = remap_context(frame, existing)
                        counts["classification_cells_changed"] += int((actual["l2_code_id"] != frame["l2_code_id"]).sum())
                        counts["classification_only_rows_added"] += int((~index.isin(existing.index)).sum())
                        new.append("data", actual[columns], format="table", data_columns=["datetime", "instrument"], index=False, min_itemsize={"instrument": 16})
                    progress({"phase": "INDUSTRY_HISTORY_REPAIR", "dataset": "sector_data", "partition_key": str(period),
                              "rows_validated": counts["classification_cells_changed"],
                              "rows_sealed": counts["classification_only_rows_added"]})
                for where in ([f"datetime < {left!r}"], [f"datetime > {right!r}"]):
                    for frame in old.select("data", where=where, chunksize=max_rows):
                        checkpoint()
                        new.append("data", frame, format="table", data_columns=["datetime", "instrument"], index=False, min_itemsize={"instrument": 16})
                new.create_table_index("data", columns=["datetime", "instrument"], kind="full")
            retained_sector = seal_checkpoint(stage_path, binding=binding, output=sector_target, counts=counts)
        else:
            counts = dict(retained_sector["counts"])
            progress({"phase": "INDUSTRY_SECTOR_STAGE_REUSED"})
        def fallback(frame):
            with pd.HDFStore(factor / "sector_data.h5", "r") as old:
                days = frame.index.get_level_values("datetime")
                facts = old.select("data", where=[f"datetime >= {str(days.min().date())!r}",
                                                   f"datetime <= {str(days.max().date())!r}"])
            return remap_context(frame, facts)
        target = scratch / "static_factors.parquet"
        counts["classification_cells_changed"] += materialize_static(
            factor / "static_factors.parquet", sector_target, target, binding=binding, max_rows=max_rows,
            fallback=fallback, checkpoint=checkpoint, progress=progress)
        with pd.HDFStore(sector_target, "r") as repaired:
            total = int(repaired.get_storer("data").nrows)
            for frame in repaired.select("data", chunksize=max_rows):
                checkpoint()
                pd.testing.assert_series_equal(projection.project(frame)["l2_code_id"], frame["l2_code_id"], check_exact=True)
        for name in ("sector_data.h5", "static_factors.parquet"):
            path = scratch / name
            if name == "sector_data.h5" and list(_signature(path)) != retained_sector["signature"]:
                raise SourceManifestError("retained industry sector changed during static readback")
            digest = retained_sector["sha256"] if name == "sector_data.h5" else file_sha256(path)
            outputs[name] = {"sha256": digest, "size": path.stat().st_size,
                             "target_signature": list(_signature(path)), "scratch_relative_path": path.relative_to(root.parent).as_posix(),
                             "original_signature": list(originals[name]), "context_sha256": context_sha}
        if any(_signature(factor / name) != value for name, value in originals.items()):
            raise SourceManifestError("industry repair input changed while reading")
        result = {"schema_version": "aistock_monthly_industry_history_projection_receipt_v1", "status": "PASS",
                "projection_sha256": projection.sha256, "projection": projection.payload, "files": outputs,
                "sector_row_count": total, **counts, "database_write": False, "price_components_exported": False,
                "unrelated_static_columns_unchanged": True, "published_candidate_write": False,
                "context_identity": context_identity,
                "static_sector_join_contract": STATIC_SECTOR_CONTRACT,
                "industry_context_contract": None if context_identity is None else context_identity["contract"],
                "input_signatures": {name: list(signature) for name, signature in originals.items()}}
        with (scratch / "projection_receipt.json").open("xb") as writer:
            writer.write(canonical_json_bytes(result) + b"\n")
            writer.flush()
            import os
            os.fsync(writer.fileno())
        return result
    except BaseException:
        # Incomplete outputs are retained outside consumer roots. Never delete
        # previously completed components or start a price re-export on failure.
        raise


def _factor_successor_matches(name, signature, record, cas):
    """Recognize an already committed correction, not an arbitrary newer file."""
    if cas is None:
        return False
    from .monthly_build_resume import load_sealed_build_json
    from .monthly_fact_correction import SCHEMA, _name

    authority_ref = record["steps"].get("fact-correction")
    if authority_ref is None:
        return False
    authority = load_sealed_build_json(cas, authority_ref)
    prepared = load_sealed_build_json(cas, record["steps"]["prepare"])
    nested = load_sealed_build_json(cas, prepared["prepare_receipt_ref"])
    factor = load_sealed_build_json(cas, nested["refs"]["factor_receipt"])
    if (authority.get("schema_version") != SCHEMA
            or authority.get("target_cutoff") != record["binding"]["target_cutoff"]
            or factor.get("fact_correction_ref") != authority_ref):
        return False
    originals = [row for row in authority["files"]
                 if row.get("relative_path") == f"factor_bundle/{name}"]
    pins = [row for row in factor["files"] if _name(row["dataset"]) == name]
    if len(originals) != 1 or len(pins) != 1:
        return False
    original, pin = originals[0], pins[0]
    return (original["dataset"] == pin["dataset"] and original["sha256"] == pin["sha256"]
            and original["size"] == pin["size"] == signature[2]
            and pin.get("target_signature") == signature
            and pin.get("fact_correction_ref") == authority_ref)


def recover_history_commit(*, root: Path, journal: Path, record: Mapping[str, Any], expected_projection: str, cas=None):
    """Finish one previously verified two-file publication after interruption.

    Keep original exports outside the release, and publish the new journal only
    after both output renames. Exact signatures identify every crash boundary.
    """
    import json
    from .monthly_build_resume import _atomic, _signature, MonthlyBuildResumeError
    from .monthly_legacy_prefix import _plain_chain, _relative

    pending = root.parent / f".{root.name}.industry-history-commit.json"
    if not pending.exists():
        return record
    _plain_chain(pending)
    if pending.stat().st_size > 128 * 1024 * 1024:
        raise MonthlyBuildResumeError("industry commit metadata is unbounded")
    value = json.loads(pending.read_bytes())
    if value.get("binding_digest") != record["binding_digest"] or value.get("projection_sha256") != expected_projection:
        raise MonthlyBuildResumeError("industry commit belongs to different data inputs")
    proposed = value["record"]
    if proposed["binding"] != record["binding"] or proposed["binding_digest"] != record["binding_digest"]:
        raise MonthlyBuildResumeError("industry commit cannot replace the original data binding")
    files = value["files"]
    if set(files) not in ({"sector_data.h5", "static_factors.parquet"}, {"static_factors.parquet"}):
        raise MonthlyBuildResumeError("industry commit would change an unrelated component")
    changed = {f"factor_bundle/{name}" for name in files}
    completion_ref = proposed["steps"].get("industry-history")
    completed = completion_ref is not None and record["steps"].get("industry-history") == completion_ref
    if any(proposed["files"][f"factor_bundle/{name}"] != entry["target_signature"] for name, entry in files.items()):
        raise MonthlyBuildResumeError("industry commit journal does not identify verified outputs")
    if completed:
        # Old workers left this intent after committing the journal. A later
        # factor correction may legitimately supersede these two outputs.
        # Validate its own CAS pins; never replay the obsolete journal/files.
        from .cas_store import CASStoreError
        for name, entry in files.items():
            signature = record["files"].get(f"factor_bundle/{name}")
            if list(_signature(root / "factor_bundle" / name)) != signature:
                raise MonthlyBuildResumeError("completed industry commit output changed")
            if signature != entry["target_signature"]:
                try:
                    accepted = _factor_successor_matches(name, signature, record, cas)
                except (CASStoreError, KeyError, TypeError, ValueError) as exc:
                    raise MonthlyBuildResumeError("completed industry successor proof is invalid") from exc
                if not accepted:
                    raise MonthlyBuildResumeError("completed industry commit has no pinned successor")
            source = root.parent / _relative(entry["scratch_relative_path"])
            context_sha = entry.get("context_sha256")
            if context_sha is not None:
                ensure_sha256(context_sha, field="industry context digest")
            if source.parent not in _history_scratch_paths(root, expected_projection, context_sha) or source.name != name:
                raise MonthlyBuildResumeError("industry commit source escaped its private outputs")
            backup = source.with_name(name + ".before")
            _plain_chain(backup)
            if list(_signature(backup)) != entry["original_signature"]:
                raise MonthlyBuildResumeError("industry commit original export changed after publication")
        pending.unlink()
        return record
    elif any(record["files"].get(f"factor_bundle/{name}") != entry["original_signature"] for name, entry in files.items()):
        raise MonthlyBuildResumeError("industry commit lost original writer provenance")
    elif set(proposed["files"]) != set(record["files"]) or any(
        proposed["files"][key] != original for key, original in record["files"].items() if key not in changed
    ) or any(proposed["steps"].get(key) != original for key, original in record["steps"].items()
             if key not in {"prepare", "industry-history"}) or set(proposed["steps"]) != set(record["steps"]) | {"industry-history"}:
        raise MonthlyBuildResumeError("industry commit changed unrelated writer provenance")
    for name, entry in files.items():
        target = root / "factor_bundle" / name
        source = root.parent / _relative(entry["scratch_relative_path"])
        context_sha = entry.get("context_sha256")
        if context_sha is not None:
            ensure_sha256(context_sha, field="industry context digest")
        expected_parents = _history_scratch_paths(root, expected_projection, context_sha)
        if source.parent not in expected_parents or source.name != name:
            raise MonthlyBuildResumeError("industry commit source escaped its private outputs")
        _plain_chain(source.parent)
        backup = source.with_name(name + ".before")
        _plain_chain(target.parent)
        if target.exists() and list(_signature(target)) == entry["target_signature"]:
            _plain_chain(target)
            _plain_chain(backup)
            if list(_signature(backup)) != entry["original_signature"]:
                raise MonthlyBuildResumeError("industry commit original export changed after publication")
            continue
        if target.exists():
            _plain_chain(target)
            if list(_signature(target)) != entry["original_signature"] or backup.exists():
                raise MonthlyBuildResumeError("industry commit target changed before publication")
            target.rename(backup)
        elif not backup.exists() or list(_signature(backup)) != entry["original_signature"]:
            raise MonthlyBuildResumeError("industry commit original export was lost")
        _plain_chain(source)
        _plain_chain(backup)
        if list(_signature(source)) != entry["target_signature"]:
            raise MonthlyBuildResumeError("industry commit verified output changed")
        source.rename(target)
    _atomic(journal, proposed)
    pending.unlink()
    return proposed


def factor_file_receipts(receipt: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    """Resolve the native dataset identity contract, never an invented path field."""
    from .factor_materializer import FACTOR_H5_DATASETS

    expected = {*FACTOR_H5_DATASETS, "static_factors"}
    files = receipt.get("files")
    if not isinstance(files, list):
        raise SourceManifestError("native factor file population is invalid")
    result = {}
    for entry in files:
        dataset = entry.get("dataset") if isinstance(entry, Mapping) else None
        if not isinstance(dataset, str) or dataset not in expected:
            raise SourceManifestError("native factor file identity is invalid")
        name = f"{dataset}.{'parquet' if dataset == 'static_factors' else 'h5'}"
        if name in result:
            raise SourceManifestError("native factor file identity is duplicated")
        result[name] = entry
    if len(result) != len(expected):
        raise SourceManifestError("native factor file population is incomplete")
    return result


def apply_retained_history(resume, *, context, compiled, prepare, cas, profile):
    """Amend factor receipts only; completed day/minute/index remain untouched."""
    import copy
    from .monthly_build_resume import MonthlyBuildResumeError, load_sealed_build_json
    from .monthly_industry_facts import CONTRACT, FrozenIndustryContext, load_frozen_industry_quote_source, predecessor_context_matches

    projection = load_frozen_history(cas, context_plan=context.plan, build_inputs=compiled.physical_plan.get("build_inputs", {}))
    if projection is None:
        return prepare
    prior = resume.get("industry-history")
    if prior is not None:
        if prior["projection_sha256"] != projection.sha256:
            raise MonthlyBuildResumeError("retained industry projection differs from frozen authority")
        if prior.get("industry_context_contract") == CONTRACT:
            refresh_quote_context = False
            source = load_frozen_industry_quote_source(cas, context_plan=context.plan,
                build_inputs=compiled.physical_plan.get("build_inputs", {}), projection=projection)
            if source is not None and not prior.get("historical_projection_reused"):
                expected = hashlib.sha256(canonical_json_bytes(source)).hexdigest()
                if prior.get("context_identity", {}).get("quote_source_sha256") != expected:
                    binding = context.plan.get("monthly_industry_quote_source") or {}
                    previous = prior.get("context_identity", {}).get("quote_source_sha256")
                    if (not isinstance(previous, str) or len(previous) != 64 or
                            previous not in {binding.get("supersedes_quote_source_sha256"), binding.get("rebuild_from_quote_source_sha256")}):
                        raise MonthlyBuildResumeError("retained industry context differs from frozen quote source")
                    # Explicit additive SOURCE refresh only: rematerialize the
                    # two industry-bearing files, never completed price exports.
                    refresh_quote_context = True
            if not refresh_quote_context and prior.get("static_sector_join_contract") == STATIC_SECTOR_CONTRACT:
                return prepare
            # Upgrade the old writer's month only. Retain the already verified
            # historical projection, sector H5, source snapshot and price bins.
            if not refresh_quote_context:
                nested = copy.deepcopy(load_sealed_build_json(cas, prepare["prepare_receipt_ref"]))
                factor = copy.deepcopy(load_sealed_build_json(cas, nested["refs"]["factor_receipt"]))
                entries = factor_file_receipts(factor)
                from .monthly_build_resume import _signature
                for name in ("sector_data.h5", "static_factors.parquet"):
                    if entries[name].get("target_signature") != _signature(resume.root / "factor_bundle" / name):
                        raise MonthlyBuildResumeError("static sector repair lost its pinned inputs")
                cutoff = date.fromisoformat(context.plan["target_cutoff"])
                after = (cutoff.replace(day=1) - date.resolution).isoformat()
                identity = {"contract": STATIC_SECTOR_CONTRACT, "after": after,
                            "sector_sha256": entries["sector_data.h5"]["sha256"],
                            "static_sha256": entries["static_factors.parquet"]["sha256"]}
                tail = _materialize_static_tail(resume.root, projection=projection, context_identity=identity,
                    after=after, checkpoint=context.checkpoint, progress=context.progress)
                result = {**prior, "files": tail["files"], "static_sector_join_contract": STATIC_SECTOR_CONTRACT,
                          "static_sector_tail": {key: value for key, value in tail.items() if key != "files"}}
                return _commit_history_result(resume, result=result, projection=projection, nested=nested,
                                              factor=factor, entries=entries, prepare=prepare, cas=cas)
        # An earlier verified ID-only repair is retained as an input. Replace
        # only sector/static context under new pins, never re-export prices.
    if resume.path is None:
        raise MonthlyBuildResumeError("industry repair needs a durable completed-export journal")
    nested = copy.deepcopy(load_sealed_build_json(cas, prepare["prepare_receipt_ref"]))
    factor = copy.deepcopy(load_sealed_build_json(cas, nested["refs"]["factor_receipt"]))
    entries = factor_file_receipts(factor)
    sector_entry = entries["sector_data.h5"]
    from .monthly_build_bridge import load_monthly_predecessor_prefix
    # A future ordinary month inherits the repaired history without scanning
    # or re-serializing it. Only a genuinely changed history gets this producer.
    prefix = load_monthly_predecessor_prefix(context_plan=context.plan, profile=profile)
    if prefix is not None:
        candidates = [pin for pin in prefix.manifest["components"].values()
                      if pin.get("path", "").endswith("/industry_l2_history_authority.json")]
        if len(candidates) > 1:
            raise MonthlyBuildResumeError("predecessor industry history authority is ambiguous")
        if candidates:
            import json
            from .monthly_legacy_prefix import _plain_chain, _relative, _signature
            pin = candidates[0]
            path = prefix.root / _relative(pin["path"])
            _plain_chain(path)
            before = _signature(path)
            raw = path.read_bytes()
            if len(raw) != pin["size"] or hashlib.sha256(raw).hexdigest() != pin["sha256"] or before != _signature(path):
                raise MonthlyBuildResumeError("predecessor history pin changed")
            payload = json.loads(raw)
            old = IndustryHistoryProjection.read({key: value for key, value in payload.items()
                                                   if key not in {"projection_sha256", "factor_projection_receipt_ref", "industry_context_contract"}})
            if (old.data_sha256 == projection.data_sha256 and predecessor_context_matches(prefix, projection)
                    and context.plan.get("monthly_industry_quote_source_ref") is None):
                proof_ref = cas.put_json({"schema_version": "aistock_monthly_industry_history_projection_receipt_v1",
                    "status": "PASS", "projection": projection.payload, "projection_sha256": projection.sha256,
                    "historical_projection_reused": True, "predecessor_history_file_sha256": pin["sha256"],
                    "sector_row_count": sector_entry["inherited_rows"] + sector_entry["month_rows"],
                    "database_write": False, "classification_cells_changed": 0, "industry_context_contract": CONTRACT,
                    "static_sector_join_contract": STATIC_SECTOR_CONTRACT})
                factor["industry_history_projection_ref"] = proof_ref.as_dict()
                nested["refs"]["factor_receipt"] = cas.put_json(factor).as_dict()
                prepared = {**prepare, "prepare_receipt_ref": cas.put_json(nested).as_dict()}
                resume.save("prepare", cas.put_json(prepared), roots=())
                resume.save("industry-history", proof_ref, roots=())
                return prepared
    quote_source = load_frozen_industry_quote_source(cas, context_plan=context.plan,
        build_inputs=compiled.physical_plan.get("build_inputs", {}), projection=projection)
    if quote_source is None:
        raise MonthlyBuildResumeError("repaired industry history needs an independent frozen official quote source; bind industry history without re-exporting prices")
    from backend.data_service.security_source_identity import load_security_source_identity_manifest
    from .source_authority import load_source_stage_receipt
    from .monthly_legacy_prefix import _signature
    frozen = load_source_stage_receipt(cas, compiled.source_stage_receipt_ref,
        expected_profile=profile.profile, expected_cutoff=date.fromisoformat(context.plan["target_cutoff"]), profile=profile)
    moneyflow = resume.root / "factor_bundle/moneyflow.h5"
    identity = load_security_source_identity_manifest(resume.root / "factor_bundle/security_source_identity.json")
    flow_pin = entries["moneyflow.h5"]
    if flow_pin.get("target_signature") != list(_signature(moneyflow)):
        raise MonthlyBuildResumeError("sealed moneyflow input pin changed before cohort repair")
    context_identity = {"contract": CONTRACT, "quote_source_sha256": hashlib.sha256(canonical_json_bytes(quote_source)).hexdigest(),
                        "moneyflow_sha256": flow_pin["sha256"], "security_identity_sha256": identity.file_sha256,
                        "pit_snapshot_sha256": frozen.pit_snapshot_digest, "projection_sha256": projection.sha256}
    rebuilder = FrozenIndustryContext(cas=cas, source=quote_source, projection=projection,
        moneyflow_path=moneyflow, pit=frozen.pit_snapshot, identity=identity)
    result = materialize_history_files(resume.root, projection, checkpoint=context.checkpoint, progress=context.progress,
                                       context_rebuilder=rebuilder.remap, context_identity=context_identity)
    prepared = _commit_history_result(resume, result=result, projection=projection, nested=nested,
                                      factor=factor, entries=entries, prepare=prepare, cas=cas)
    # An older worker may have sealed, but not committed, its private history
    # output. Adopt it first, then upgrade its month in this same invocation.
    if result.get("static_sector_join_contract") != STATIC_SECTOR_CONTRACT:
        return apply_retained_history(resume, context=context, compiled=compiled,
                                      prepare=prepared, cas=cas, profile=profile)
    return prepared


def _commit_history_result(resume, *, result, projection, nested, factor, entries, prepare, cas):
    """Use the existing crash-safe factor transaction for one or both outputs."""
    import copy
    from .monthly_build_resume import _atomic, MonthlyBuildResumeError

    proof_ref = cas.put_json(result)
    for name, entry in entries.items():
        if name not in result["files"]:
            continue
        pin = result["files"][name]
        entry.update({key: pin[key] for key in ("sha256", "size", "target_signature")})
        entry["industry_history_projection_ref"] = proof_ref.as_dict()
        if name == "sector_data.h5":
            entry["inherited_rows"] = result["sector_row_count"] - entry["month_rows"]
    factor["industry_history_projection_ref"] = proof_ref.as_dict()
    factor_ref = cas.put_json(factor)
    nested["refs"]["factor_receipt"] = factor_ref.as_dict()
    prepared = {**prepare, "prepare_receipt_ref": cas.put_json(nested).as_dict()}
    prepared_ref = cas.put_json(prepared)
    # Native monthly factors have no physical legacy checkpoint. The amended
    # CAS receipt and prepare reference are committed with the two file pins
    # in the durable resume journal below.
    files = result["files"]
    record = copy.deepcopy(resume.record)
    for name, entry in files.items():
        relative = f"factor_bundle/{name}"
        if record["files"].get(relative) != entry["original_signature"]:
            raise MonthlyBuildResumeError("industry repair lost original writer provenance")
        record["files"][relative] = entry["target_signature"]
    record["steps"]["prepare"] = prepared_ref.as_dict()
    record["steps"]["industry-history"] = proof_ref.as_dict()
    pending = resume.root.parent / f".{resume.root.name}.industry-history-commit.json"
    _atomic(pending, {"binding_digest": record["binding_digest"], "projection_sha256": projection.sha256,
                      "record": record, "files": files})
    resume.record = dict(recover_history_commit(root=resume.root, journal=resume.path, record=resume.record,
                                               expected_projection=projection.sha256, cas=cas))
    return prepared
