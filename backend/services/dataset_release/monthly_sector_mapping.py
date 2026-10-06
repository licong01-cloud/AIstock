"""Keep shared sector IDs bound to the immutable prefix, not a mutable catalog.

Only small pinned classification metadata is read. All raw source rows remain
sealed; an unassigned, explicitly unpublished catalog addition is recorded,
never given an ID that would reinterpret the inherited H5 prefix.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from typing import Any, Iterable, Mapping

from .canonical import canonical_json_bytes, digest_named_fields, ensure_sha256
from .errors import SourceManifestError
from .monthly_legacy_prefix import _plain_chain, _relative, _signature
from .sector_enrichment import FrozenSectorEnricher
from .shared_sector_context import validate_release_sw_l2_code_map
from . import sw_l2_quote_policy


MAPPING_SCHEMA = "aistock_monthly_prefix_sw_l2_mapping_v1"
MAPPING_FIELD = "monthly_sector_mapping"


def _validate_binding(value: Any) -> Mapping[str, Any]:
    try:
        if not isinstance(value, Mapping) or set(value) != {
            "schema_version", "predecessor_manifest_sha256", "code_map_file", "code_map",
        } or value["schema_version"] != MAPPING_SCHEMA:
            raise ValueError("binding fields differ")
        ensure_sha256(value["predecessor_manifest_sha256"], field="predecessor_manifest_sha256")
        pin = value["code_map_file"]
        if not isinstance(pin, Mapping) or set(pin) != {"path", "sha256", "size"}:
            raise ValueError("code-map pin fields differ")
        _relative(pin["path"])
        ensure_sha256(pin["sha256"], field="code_map_file.sha256")
        raw = canonical_json_bytes(value["code_map"]) + b"\n"
        if type(pin["size"]) is not int or len(raw) != pin["size"] or hashlib.sha256(raw).hexdigest() != pin["sha256"]:
            raise ValueError("code-map pinned bytes differ")
        mapping = validate_release_sw_l2_code_map(value["code_map"])
        if len(mapping.code_to_id) != 131 or sw_l2_quote_policy.taxonomy_digest(mapping.code_to_id) != sw_l2_quote_policy.SW2021_L2_TAXONOMY_DIGEST:
            raise ValueError("approved SW L2 taxonomy differs")
        return value
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceManifestError(f"monthly predecessor sector mapping invalid: {exc}") from exc


def load_predecessor_sector_mapping(prefix: Any) -> Mapping[str, Any]:
    """The caller already resolved the controller-bound predecessor manifest."""
    try:
        pin = prefix.manifest["components"]["sector_code_map"]
        relative = _relative(pin["path"])
        path = prefix.root.joinpath(*relative.parts)
        _plain_chain(path)
        before = _signature(path)
        if before[2] > 64 * 1024:
            raise ValueError("code-map metadata exceeds bounded read")
        raw = path.read_bytes()
        if before != _signature(path) or len(raw) != pin["size"] or hashlib.sha256(raw).hexdigest() != pin["sha256"]:
            raise ValueError("code-map pinned bytes differ")
        return _validate_binding({
            "schema_version": MAPPING_SCHEMA,
            "predecessor_manifest_sha256": prefix.manifest_sha256,
            "code_map_file": {key: pin[key] for key in ("path", "sha256", "size")},
            "code_map": json.loads(raw),
        })
    except (KeyError, TypeError, ValueError, OSError) as exc:
        raise SourceManifestError(f"monthly predecessor sector mapping unreadable: {exc}") from exc


def frozen_sector_mapping_binding(cas: Any, snapshot: Any) -> Mapping[str, Any] | None:
    payload = cas.get_json_bounded(snapshot.source_manifest_ref, max_bytes=16 * 1024 * 1024)
    if not isinstance(payload, Mapping):
        raise SourceManifestError("frozen sector source manifest is invalid")
    # Legacy source graphs retain their original sorted-catalog contract. New
    # monthly native producers always bind this field before reading rows.
    if MAPPING_FIELD not in payload:
        return None
    return _validate_binding(payload[MAPPING_FIELD])


def build_bound_sector_enricher(
    classify_rows: Iterable[Mapping[str, Any]], member_rows: Iterable[Mapping[str, Any]],
    *, binding: Mapping[str, Any] | None,
) -> FrozenSectorEnricher:
    if binding is None:
        return FrozenSectorEnricher.build(classify_rows, member_rows)
    _validate_binding(binding)
    mapping = validate_release_sw_l2_code_map(binding["code_map"])
    rows = list(classify_rows)
    codes = [str(row.get("index_code", "")) for row in rows]
    if len(set(codes)) != len(codes) or not set(mapping.code_to_id) <= set(codes):
        raise SourceManifestError("monthly raw classification duplicates or omits a pinned L2 code")
    for row in rows:
        if str(row.get("level", "")).upper() != "L2":
            raise SourceManifestError("monthly raw classification is not L2")
        if row["index_code"] not in mapping.code_to_id and (
            row.get("is_pub") != "0" or row.get("src") != "SW2021"
        ):
            raise SourceManifestError("new L2 catalog requires a published taxonomy migration; not silently filtered")
    # The ordinary membership validator still rejects any extra-code member.
    enricher = FrozenSectorEnricher.build(
        (row for row in rows if row["index_code"] in mapping.code_to_id), member_rows,
    )
    return replace(enricher, code_map=dict(mapping.code_to_id))


def sector_mapping_catalog_receipt(
    classify_rows: Iterable[Mapping[str, Any]], member_rows: Iterable[Mapping[str, Any]],
    *, binding: Mapping[str, Any],
) -> Mapping[str, Any]:
    rows = sorted((dict(row) for row in classify_rows), key=lambda row: row["index_code"])
    enricher = build_bound_sector_enricher(rows, member_rows, binding=binding)
    return {
        "binding": dict(binding),
        "raw_catalog_count": len(rows),
        "canonical_catalog_count": len(enricher.code_map),
        "raw_catalog_sha256": digest_named_fields("aistock_monthly_sw_l2_raw_catalog_v1", {"rows": rows}),
        "unassigned_unpublished_codes": sorted(row["index_code"] for row in rows if row["index_code"] not in enricher.code_map),
        "addition_classification": "UNASSIGNED_UNPUBLISHED_CATALOG_METADATA",
        "historical_business_audit_performed": False,
    }
