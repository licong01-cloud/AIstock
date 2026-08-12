from __future__ import annotations

import json
import subprocess
import sys
from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.model_binding_resolution import (
    AdvisoryModelBindingResolver,
    publish_program_model_descriptor,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def _descriptor() -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": "advisory_program_model_binding_v1",
        "program_id": "advp_test",
        "binding_version_id": "advb_test",
        "package_ids": ["pkg_test"],
        "package_id": "pkg_test",
        "manifest_sha256": "a" * 64,
        "style_profile_id": "short_rebound_test_v1",
        "style_profile_hash": "b" * 64,
        "selection_runtime_semantics_hash": "c" * 64,
        "feature_schema_version": "advisory_feature_schema_v1",
        "feature_schema_hash": "d" * 64,
        "bundle_id": "e" * 64,
        "bundle_manifest_sha256": "f" * 64,
        "candidate_projection": {
            "schema_version": "advisory_candidate_projection_v1",
            "component_roles": {"lstm": "alpha_lstm", "fund": "alpha_fund"},
        },
        "created_at": "2026-08-13T00:00:00+00:00",
    }
    payload["descriptor_sha256"] = canonical_json_sha256(payload)
    return payload


def test_exact_program_descriptor_resolves_without_latest_scan(tmp_path) -> None:
    path = tmp_path / "program_bindings" / "advp_test" / "advb_test.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(_descriptor()), encoding="utf-8")
    resolver = AdvisoryModelBindingResolver()

    resolution = resolver.resolve(
        model_root=tmp_path,
        program=SimpleNamespace(program_id="advp_test", package_ids=["pkg_test"]),
        active_binding={"binding_version_id": "advb_test", "package_ids": ["pkg_test"]},
        selection_run=SimpleNamespace(manifest_sha256_by_package={"pkg_test": "a" * 64}),
    )

    assert resolution.package_id == "pkg_test"
    assert resolution.component_roles == {"lstm": "alpha_lstm", "fund": "alpha_fund"}
    assert resolution.bundle_id == "e" * 64


def test_missing_exact_descriptor_is_typed_unavailable_without_directory_scan(tmp_path) -> None:
    unrelated = tmp_path / "program_bindings" / "other" / "latest.json"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text(json.dumps(_descriptor()), encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as error:
        AdvisoryModelBindingResolver().resolve(
            model_root=tmp_path,
            program=SimpleNamespace(program_id="advp_test", package_ids=["pkg_test"]),
            active_binding={"binding_version_id": "advb_test", "package_ids": ["pkg_test"]},
            selection_run=SimpleNamespace(manifest_sha256_by_package={"pkg_test": "a" * 64}),
        )

    assert error.value.reason_code == "ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE"


def test_descriptor_component_roles_and_hash_fail_closed(tmp_path) -> None:
    payload = _descriptor()
    payload["candidate_projection"] = {
        "schema_version": "advisory_candidate_projection_v1",
        "component_roles": {"lstm": "alpha_same", "fund": "alpha_same"},
    }
    payload["descriptor_sha256"] = canonical_json_sha256(
        {key: value for key, value in payload.items() if key != "descriptor_sha256"}
    )
    path = tmp_path / "program_bindings" / "advp_test" / "advb_test.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as error:
        AdvisoryModelBindingResolver().resolve(
            model_root=tmp_path,
            program=SimpleNamespace(program_id="advp_test", package_ids=["pkg_test"]),
            active_binding={"binding_version_id": "advb_test", "package_ids": ["pkg_test"]},
            selection_run=SimpleNamespace(manifest_sha256_by_package={"pkg_test": "a" * 64}),
        )

    assert error.value.reason_code == "ADVISORY_MODEL_CANDIDATE_PROJECTION_UNSUPPORTED"


def test_descriptor_publish_is_atomic_idempotent_and_refuses_identity_overwrite(tmp_path) -> None:
    payload = _descriptor()
    payload.pop("descriptor_sha256")

    first = publish_program_model_descriptor(model_root=tmp_path, payload=payload)
    second = publish_program_model_descriptor(model_root=tmp_path, payload=payload)

    assert first == second
    persisted = json.loads(first.read_text(encoding="utf-8"))
    assert persisted["descriptor_sha256"] == canonical_json_sha256(
        {key: value for key, value in persisted.items() if key != "descriptor_sha256"}
    )
    changed = dict(payload)
    changed["bundle_id"] = "1" * 64
    with pytest.raises(AdvisoryModelFirstError) as error:
        publish_program_model_descriptor(model_root=tmp_path, payload=changed)
    assert error.value.reason_code == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID"


def test_descriptor_publish_rejects_invalid_component_projection_before_write(tmp_path) -> None:
    payload = _descriptor()
    payload.pop("descriptor_sha256")
    payload["candidate_projection"] = {
        "schema_version": "advisory_candidate_projection_v1",
        "component_roles": {"lstm": "alpha_same", "fund": "alpha_same"},
    }

    with pytest.raises(AdvisoryModelFirstError) as error:
        publish_program_model_descriptor(model_root=tmp_path, payload=payload)

    assert error.value.reason_code == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID"
    assert not (tmp_path / "program_bindings" / "advp_test" / "advb_test.json").exists()


def test_descriptor_cli_requires_explicit_inputs_and_publishes_exact_path(tmp_path) -> None:
    payload = _descriptor()
    payload.pop("descriptor_sha256")
    payload_path = tmp_path / "descriptor.json"
    payload_path.write_text(json.dumps(payload), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/advisory_publish_program_model_descriptor.py",
            "--model-root",
            str(tmp_path / "model-root"),
            "--payload",
            str(payload_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    receipt = json.loads(completed.stdout)
    target = tmp_path / "model-root" / "program_bindings" / "advp_test" / "advb_test.json"
    assert receipt == {"ok": True, "descriptor_path": str(target)}
    assert target.is_file()
