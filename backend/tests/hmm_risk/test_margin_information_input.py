from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from backend.services.hmm_risk import margin_information_input as subject
from backend.services.hmm_risk.contracts import StateModelSetError, canonical_json_bytes, canonical_sha256
from backend.services.hmm_risk.rotation_l1_input_bundle import RotationL1InputBundleError
from backend.services.hmm_risk.security_identity import load_security_source_identity_manifest


def _write(path: Path, value) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value))
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def inputs(tmp_path):
    root, package = tmp_path / "release", tmp_path / "shared"
    # The shared, merged authority is a fixture, not a default resolution path.
    base = Path(__file__).resolve().parents[2] / "data_service"
    authority_path = base / "manifests/security_source_identity_20261112_v2.json"
    authority = json.loads(authority_path.read_text(encoding="utf-8"))
    old = {**authority, "rows": [r for r in authority["rows"] if r["source_dataset"] != subject.MARGIN_SOURCE]}
    margin = root / subject.MARGIN_PATH
    margin.parent.mkdir(parents=True)
    margin.write_bytes(b"synthetic frozen margin bytes; no real dataset access")
    old_path = root / "components/factor_h5_static_candidate_v2/security_source_identity.json"
    _write(old_path, old)
    manifest = {
        "schema_version": "qe_dataset_manifest_v1",
        "components": {
            str(i): {
                "path": p.relative_to(root).as_posix(),
                "size": p.stat().st_size,
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
            }
            for i, p in enumerate([margin, old_path])
        },
    }
    manifest["dataset_manifest_sha256"] = canonical_sha256(manifest)
    manifest_file = _write(root / "qe_dataset_manifest.json", manifest)
    authority_sha = _write(package / "security_source_identity.json", authority)
    report = {
        "schema_version": "aistock_margin_source_identity_readback_v1",
        "status": "PASS",
        "predecessor_bytes_unchanged": True,
        "predecessor_manifest_identity": manifest["dataset_manifest_sha256"],
        "predecessor_manifest_file_sha256": manifest_file,
        "predecessor_identity_sha256": hashlib.sha256(old_path.read_bytes()).hexdigest(),
        "margin_detail_sha256": hashlib.sha256(margin.read_bytes()).hexdigest(),
        "identity": {"canonical_sha256": canonical_sha256(authority), "file_sha256": authority_sha},
    }
    state = dict(root=root, package=package, manifest=manifest, authority=authority, report=report)
    _seal(state)
    return state


def _seal(state):
    package, report = state["package"], state["report"]
    authority_sha = _write(package / "security_source_identity.json", state["authority"])
    report["identity"].update(canonical_sha256=canonical_sha256(state["authority"]), file_sha256=authority_sha)
    _write(package / "margin_source_identity_readback.json", report)
    manifest = {
        "schema_version": "aistock_shared_security_source_identity_package_v1",
        "authority_version": state["authority"]["manifest_version"],
        "predecessor_manifest_identity": state["manifest"]["dataset_manifest_sha256"],
        "predecessor_data_sha256": report["margin_detail_sha256"],
        "data_files_reused_unchanged": True,
        "files": {
            name: {
                "sha256": hashlib.sha256((package / name).read_bytes()).hexdigest(),
                "size": (package / name).stat().st_size,
            }
            for name in ("security_source_identity.json", "margin_source_identity_readback.json")
        },
    }
    state["package_sha"] = _write(package / "manifest.json", manifest)


def _load(state):
    return subject.load_margin_source_binding(
        dataset_root=state["root"],
        expected_dataset_manifest_sha256=state["manifest"]["dataset_manifest_sha256"],
        package_root=state["package"],
        expected_package_manifest_sha256=state["package_sha"],
    )


def test_explicit_shared_binding_resolves_all_switches_without_changing_original(inputs):
    binding = _load(inputs)
    for current, old, before, after in [
        ("001872.SZ", "000022.SZ", "2018-12-25", "2018-12-26"),
        ("001914.SZ", "000043.SZ", "2019-12-15", "2019-12-16"),
        ("302132.SZ", "300114.SZ", "2025-02-16", "2025-02-17"),
    ]:
        assert binding.canonical_code(old, date.fromisoformat(before)) == current
        assert binding.canonical_code(current, date.fromisoformat(after)) == current
        for wrong, day in [(current, before), (old, after)]:
            with pytest.raises(StateModelSetError, match="effective identity|financing alias"):
                binding.canonical_code(wrong, date.fromisoformat(day))
    assert binding.canonical_code("000001.SZ", date(2023, 4, 3)) == "000001.SZ"
    assert (
        binding.identity.resolve("001872.SZ", date(2018, 12, 25), "market.moneyflow_ts").source_ts_code == "001872.SZ"
    )
    old_path = inputs["root"] / "components/factor_h5_static_candidate_v2/security_source_identity.json"
    old = json.loads(old_path.read_bytes())
    original = load_security_source_identity_manifest(old_path, expected_sha256=canonical_sha256(old))
    assert original.resolve("001872.SZ", date(2018, 12, 25), "market.margin_detail").source_ts_code == "001872.SZ"
    binding.unchanged()
    assert binding.evidence()["authority"]["manifest_sha256"] == canonical_sha256(inputs["authority"])


@pytest.mark.parametrize("asset", ["margin", "package", "authority", "original"])
def test_pin_drift_is_rejected_without_fallback(inputs, asset):
    paths = {
        "margin": inputs["root"] / subject.MARGIN_PATH,
        "package": inputs["package"] / "manifest.json",
        "authority": inputs["package"] / "security_source_identity.json",
        "original": inputs["root"] / "components/factor_h5_static_candidate_v2/security_source_identity.json",
    }
    with paths[asset].open("ab") as stream:
        stream.write(b" ")
    with pytest.raises((StateModelSetError, RotationL1InputBundleError), match="differs|SHA256"):
        _load(inputs)


@pytest.mark.parametrize(
    "field",
    [
        "status",
        "predecessor_manifest_identity",
        "predecessor_identity_sha256",
        "predecessor_manifest_file_sha256",
        "predecessor_bytes_unchanged",
    ],
)
def test_resealed_package_cannot_override_readback_identity(inputs, field):
    inputs["report"][field] = False if field == "predecessor_bytes_unchanged" else "WRONG"
    _seal(inputs)
    with pytest.raises(StateModelSetError, match="readback identities/status"):
        _load(inputs)


def test_moneyflow_only_authority_is_not_accepted_as_margin_authority(inputs):
    inputs["authority"]["rows"] = [
        r for r in inputs["authority"]["rows"] if r["source_dataset"] != subject.MARGIN_SOURCE
    ]
    _seal(inputs)
    with pytest.raises(StateModelSetError, match="no financing-source alias"):
        _load(inputs)


@pytest.mark.parametrize("schema", ["hmm_risk_security_source_identity_manifest_v1", "unknown_v2"])
def test_shared_package_cannot_substitute_private_or_unknown_schema(inputs, schema):
    inputs["authority"]["schema_version"] = schema
    _seal(inputs)
    with pytest.raises(StateModelSetError, match="schema|readback identities/status"):
        _load(inputs)


def test_inverse_alias_ambiguity_is_rejected_even_when_forward_intervals_are_unique(inputs):
    row = dict(inputs["authority"]["rows"][0], canonical_ts_code="999999.SZ")
    row["row_hash"] = canonical_sha256({k: v for k, v in row.items() if k != "row_hash"})
    inputs["authority"]["rows"].append(row)
    inputs["authority"]["rows"].sort(
        key=lambda r: (r["source_dataset"], r["canonical_ts_code"], r["effective_start"], r["effective_end"])
    )
    _seal(inputs)
    with pytest.raises(StateModelSetError, match="ambiguous inverse"):
        _load(inputs)


def test_package_pin_cannot_override_asset_path(inputs):
    path = inputs["package"] / "manifest.json"
    package = json.loads(path.read_bytes())
    package["files"]["security_source_identity.json"]["path"] = "../other.json"
    inputs["package_sha"] = _write(path, package)
    with pytest.raises(StateModelSetError, match="pin schema"):
        _load(inputs)


def test_reader_detects_replacement_after_binding(inputs):
    binding = _load(inputs)
    binding.margin_path.write_bytes(b"changed")
    with pytest.raises(RotationL1InputBundleError, match="changed during readback"):
        binding.unchanged()


def test_explicit_paths_are_required_and_unknown_source_does_not_resolve(inputs):
    binding = _load(inputs)
    with pytest.raises(StateModelSetError, match="unsupported source_dataset"):
        binding.identity.resolve("302132.SZ", date(2023, 1, 3), "market.unapproved")
    with pytest.raises(StateModelSetError, match="date"):
        binding.canonical_code("300114.SZ", "2023-01-03")
    with pytest.raises(RotationL1InputBundleError, match="unavailable"):
        subject.load_margin_source_binding(
            dataset_root=inputs["root"],
            expected_dataset_manifest_sha256=inputs["manifest"]["dataset_manifest_sha256"],
            package_root=inputs["package"] / "missing",
            expected_package_manifest_sha256=inputs["package_sha"],
        )
