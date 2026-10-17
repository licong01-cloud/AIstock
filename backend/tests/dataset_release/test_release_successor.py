from __future__ import annotations

import hashlib
import json
from pathlib import Path

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.release_successor import (
    build_profile_v3,
    build_successor,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: dict) -> None:
    path.write_bytes(canonical_json_bytes(value) + b"\n")


def _baseline(tmp_path: Path) -> tuple[Path, dict, str]:
    root = tmp_path / "r7"
    factor = root / "components" / "factor_h5_static_candidate_v2"
    factor.mkdir(parents=True)
    sector = factor / "sector_data.h5"
    sector.write_bytes(b"sector")
    payload = root / "payload.bin"
    payload.write_bytes(b"unchanged")
    components = {"payload": {"path": "payload.bin", "sha256": _sha(payload), "size": 9}}
    manifest = {
        "schema_version": "qe_dataset_manifest_v1",
        "release_id": "qe_hmm_full_v2_20260831",
        "revision": "20260918-r7",
        "cutoff_trade_date": "2026-08-31",
        "components": components,
        "deployment_content_sha256": hashlib.sha256(canonical_json_bytes(components)).hexdigest(),
    }
    identity = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    manifest["dataset_manifest_sha256"] = identity
    _write_json(root / "qe_dataset_manifest.json", manifest)
    _write_json(
        root / "direct_monthly_state.json",
        {
            "schema_version": "qe_direct_monthly_state_v3",
            "components": {},
            "status": "CANDIDATE_READY",
        },
    )
    return root, manifest, _sha(root / "qe_dataset_manifest.json")


def _sector_component(tmp_path: Path, baseline_identity: str, sector_sha: str) -> Path:
    root = tmp_path / "sector-context"
    root.mkdir()
    authority = {"authority_id": "fixture", "authority_sha256": "a" * 64}
    entries = [{"l2_code_id": index, "canonical_l2_code": f"801{index:03d}.SI"} for index in range(131)]
    from backend.services.dataset_release.shared_sector_context import (
        build_release_sw_l2_code_map_payload,
    )

    code_map = build_release_sw_l2_code_map_payload(
        code_to_id={row["canonical_l2_code"]: row["l2_code_id"] for row in entries},
        member_backed_codes=[row["canonical_l2_code"] for row in entries],
        authority_id=authority["authority_id"],
        authority_sha256=authority["authority_sha256"],
    )
    _write_json(root / "sector_code_map.json", code_map)
    quote_entries = [
        {
            "canonical_l2_code": row["canonical_l2_code"],
            "availability_spans": [
                {"start_date": "2024-07-01", "end_date": "2026-08-31"}
            ],
        }
        for row in sorted(code_map["entries"], key=lambda row: row["canonical_l2_code"])
    ]
    quote_schema = "aistock_release_sw_l2_quote_availability_v1"
    quote_digest = digest_named_fields(
        quote_schema,
        {"mapping_authority": authority, "entries": quote_entries},
    )
    _write_json(
        root / "sector_quote_availability.json",
        {
            "schema_version": quote_schema,
            "mapping_authority": authority,
            "entries": quote_entries,
            "quote_availability_digest": quote_digest,
        },
    )
    (root / "market_context.parquet").write_bytes(b"market")
    (root / "sector_membership_spans.parquet").write_bytes(b"membership")
    receipt = {
        "schema_version": "aistock_sector_context_receipt_v1",
        "source_dataset_manifest_sha256": baseline_identity,
        "sector_data": {
            "path": "components/factor_h5_static_candidate_v2/sector_data.h5",
            "sha256": sector_sha,
        },
        "sector_code_map": {
            "path": "sector_code_map.json",
            "sha256": _sha(root / "sector_code_map.json"),
            "schema_version": code_map["schema_version"],
            "code_map_digest": code_map["code_map_digest"],
            "authority": authority,
        },
        "market_context": {
            "path": "market_context.parquet",
            "sha256": _sha(root / "market_context.parquet"),
            "schema_version": "aistock_market_context_v1",
            "definition": "sum_market_sw_daily_vol_all_rows_v1",
            "start": "2024-07-01",
            "end": "2026-08-31",
            "row_count": 527,
        },
        "membership": {
            "path": "sector_membership_spans.parquet",
            "sha256": _sha(root / "sector_membership_spans.parquet"),
            "schema_version": "aistock_sector_membership_spans_v1",
            "start": "2024-07-01",
            "end": "2026-08-31",
            "span_count": 2,
            "symbol_count": 2,
        },
        "quote_availability": {
            "path": "sector_quote_availability.json",
            "sha256": _sha(root / "sector_quote_availability.json"),
            "schema_version": quote_schema,
            "canonical_digest": quote_digest,
            "catalog_count": 131,
        },
    }
    _write_json(root / "component_receipt.json", receipt)
    return root


def test_successor_is_create_exclusive_and_preserves_baseline(tmp_path: Path) -> None:
    baseline, manifest, manifest_file_sha = _baseline(tmp_path)
    sector_sha = _sha(baseline / "components" / "factor_h5_static_candidate_v2" / "sector_data.h5")
    component = _sector_component(tmp_path, manifest["dataset_manifest_sha256"], sector_sha)
    successor = tmp_path / "r8"

    result = build_successor(
        baseline_root=baseline,
        successor_root=successor,
        sector_component_root=component,
        revision="20260918-r8",
        expected_baseline_manifest_identity=manifest["dataset_manifest_sha256"],
        expected_baseline_manifest_file_sha256=manifest_file_sha,
        created_at="2026-09-18T00:00:00+00:00",
    )

    assert (successor / "payload.bin").read_bytes() == b"unchanged"
    assert (baseline / "payload.bin").read_bytes() == b"unchanged"
    new_manifest = json.loads((successor / "qe_dataset_manifest.json").read_text())
    assert new_manifest["revision"] == "20260918-r8"
    assert new_manifest["dataset_manifest_sha256"] == result["dataset_manifest_sha256"]
    assert new_manifest["components"]["sector_code_map"]["schema_version"] == ("aistock_release_sw_l2_code_map_v1")
    try:
        build_successor(
            baseline_root=baseline,
            successor_root=successor,
            sector_component_root=component,
            revision="20260918-r8",
            expected_baseline_manifest_identity=manifest["dataset_manifest_sha256"],
            expected_baseline_manifest_file_sha256=manifest_file_sha,
        )
    except FileExistsError:
        pass
    else:  # pragma: no cover
        raise AssertionError("successor overwrite was accepted")


def test_successor_replaces_existing_sector_context_without_touching_baseline(tmp_path: Path) -> None:
    baseline, manifest, manifest_file_sha = _baseline(tmp_path)
    old_component = baseline / "components" / "sector_context_candidate_v1"
    old_component.mkdir(parents=True)
    (old_component / "old-only.txt").write_text("predecessor", encoding="utf-8")
    component = _sector_component(
        tmp_path,
        manifest["dataset_manifest_sha256"],
        _sha(baseline / "components" / "factor_h5_static_candidate_v2" / "sector_data.h5"),
    )

    successor = tmp_path / "r8-pit1"
    build_successor(
        baseline_root=baseline,
        successor_root=successor,
        sector_component_root=component,
        revision="20260920-r8-pit1",
        expected_baseline_manifest_identity=manifest["dataset_manifest_sha256"],
        expected_baseline_manifest_file_sha256=manifest_file_sha,
    )

    assert (baseline / "components" / "sector_context_candidate_v1" / "old-only.txt").is_file()
    assert not (successor / "components" / "sector_context_candidate_v1" / "old-only.txt").exists()
    assert (successor / "components" / "sector_context_candidate_v1" / "component_receipt.json").is_file()


def test_profile_v3_uses_one_release_root_per_node(tmp_path: Path) -> None:
    baseline, manifest, manifest_file_sha = _baseline(tmp_path)
    component = _sector_component(
        tmp_path,
        manifest["dataset_manifest_sha256"],
        _sha(baseline / "components" / "factor_h5_static_candidate_v2" / "sector_data.h5"),
    )
    successor = tmp_path / "r8"
    receipt = build_successor(
        baseline_root=baseline,
        successor_root=successor,
        sector_component_root=component,
        revision="20260918-r8",
        expected_baseline_manifest_identity=manifest["dataset_manifest_sha256"],
        expected_baseline_manifest_file_sha256=manifest_file_sha,
    )
    base_profile = {
        "schema_version": "aistock_active_dataset_profile_v1",
        "generation": "old",
        "controller_paths": {"candidate_root": "old", "stock_pool_root": "old/pools"},
        "node_bindings": {"old": {"candidate_root": "/old"}},
        "components": {},
        "consumers": {
            "qe": {"defaults": {}, "default_universe": {}, "universes": {}, "coverage_receipt_sha256": "a" * 64}
        },
    }
    profile_source = tmp_path / "active.json"
    _write_json(profile_source, base_profile)
    output = tmp_path / "candidate-profile.json"

    profile = build_profile_v3(
        baseline_profile_path=profile_source,
        profile_output_path=output,
        successor_receipt=receipt,
        generation="20260918-v11",
        controller_candidate_root="X:\\r8",
        node_candidate_roots={"wsl2-5080": "/mnt/r8", "rdagent-node1": "/home/r8"},
    )

    assert profile["schema_version"] == "aistock_active_dataset_profile_v3"
    assert set(profile["consumers"]) == {"qe", "hmm", "selection", "advisory"}
    assert profile["node_bindings"]["wsl2-5080"]["candidate_root"] == "/mnt/r8"
    assert profile["components"]["dataset_manifest_sha256"] == receipt["dataset_manifest_sha256"]
