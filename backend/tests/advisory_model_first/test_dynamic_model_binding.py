from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.model_binding_resolution import (
    AdvisoryModelBindingResolver,
    publish_program_model_descriptor,
    rollback_program_model_descriptor,
    rotate_program_model_descriptor,
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


def _meta_descriptor() -> dict[str, object]:
    payload = _descriptor()
    payload.pop("descriptor_sha256")
    payload.update(
        {
            "schema_version": "advisory_program_model_binding_v2",
            "model_role": "meta_label_take_skip_confidence",
            "shadow_policy_sha256": "1" * 64,
        }
    )
    payload["candidate_projection"] = {
        "schema_version": "advisory_candidate_projection_v1",
        "component_roles": {"lstm": "alpha_lstm", "fund": "alpha_fund"},
        "terminal_weights": {"alpha_lstm": 0.7, "alpha_fund": 0.3},
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
        selection_run=SimpleNamespace(
            manifest_sha256_by_package={"pkg_test": "a" * 64}
        ),
    )

    assert resolution.package_id == "pkg_test"
    assert resolution.component_roles == {"lstm": "alpha_lstm", "fund": "alpha_fund"}
    assert resolution.bundle_id == "e" * 64


def test_meta_label_descriptor_v2_resolves_exact_role_policy_and_weights(
    tmp_path,
) -> None:
    path = tmp_path / "program_bindings" / "advp_test" / "advb_test.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(_meta_descriptor()), encoding="utf-8")

    resolution = AdvisoryModelBindingResolver().resolve(
        model_root=tmp_path,
        program=SimpleNamespace(program_id="advp_test", package_ids=["pkg_test"]),
        active_binding={"binding_version_id": "advb_test", "package_ids": ["pkg_test"]},
        selection_run=SimpleNamespace(
            manifest_sha256_by_package={"pkg_test": "a" * 64}
        ),
    )

    assert resolution.model_role == "meta_label_take_skip_confidence"
    assert resolution.shadow_policy_sha256 == "1" * 64
    assert resolution.terminal_weights == {"alpha_lstm": 0.7, "alpha_fund": 0.3}


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    (
        (
            lambda payload: payload.update(model_role="quality_reranker"),
            "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID",
        ),
        (
            lambda payload: payload["candidate_projection"].update(
                terminal_weights={"alpha_lstm": 0.6, "alpha_fund": 0.3}
            ),
            "ADVISORY_MODEL_CANDIDATE_PROJECTION_UNSUPPORTED",
        ),
    ),
)
def test_meta_label_descriptor_v2_rejects_role_or_weight_drift(
    tmp_path, mutation, reason_code
) -> None:
    payload = _meta_descriptor()
    mutation(payload)
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
            active_binding={
                "binding_version_id": "advb_test",
                "package_ids": ["pkg_test"],
            },
            selection_run=SimpleNamespace(
                manifest_sha256_by_package={"pkg_test": "a" * 64}
            ),
        )

    assert error.value.reason_code == reason_code


def test_missing_exact_descriptor_is_typed_unavailable_without_directory_scan(
    tmp_path,
) -> None:
    unrelated = tmp_path / "program_bindings" / "other" / "latest.json"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text(json.dumps(_descriptor()), encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as error:
        AdvisoryModelBindingResolver().resolve(
            model_root=tmp_path,
            program=SimpleNamespace(program_id="advp_test", package_ids=["pkg_test"]),
            active_binding={
                "binding_version_id": "advb_test",
                "package_ids": ["pkg_test"],
            },
            selection_run=SimpleNamespace(
                manifest_sha256_by_package={"pkg_test": "a" * 64}
            ),
        )

    assert error.value.reason_code == "ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE"


def test_descriptor_rejects_unknown_schema_as_invalid_not_target_drift(
    tmp_path,
) -> None:
    payload = _descriptor()
    payload["schema_version"] = "advisory_program_model_binding_v999"
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
            active_binding={
                "binding_version_id": "advb_test",
                "package_ids": ["pkg_test"],
            },
            selection_run=SimpleNamespace(
                manifest_sha256_by_package={"pkg_test": "a" * 64}
            ),
        )

    assert error.value.reason_code == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID"


@pytest.mark.parametrize(
    ("program_id", "binding_version_id"),
    (
        ("../outside", "advb_test"),
        ("advp_test", "../../outside"),
        ("/absolute", "advb_test"),
        ("advp_test\\outside", "advb_test"),
        ("advp_test", "advb/test"),
        ("CON", "advb_test"),
        ("advp_test", "NUL"),
    ),
)
def test_descriptor_path_rejects_non_opaque_path_identities(
    tmp_path, program_id, binding_version_id
) -> None:
    with pytest.raises(AdvisoryModelFirstError) as error:
        AdvisoryModelBindingResolver.descriptor_path(
            model_root=tmp_path,
            program_id=program_id,
            binding_version_id=binding_version_id,
        )

    assert error.value.reason_code == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID"


def test_is_configured_rejects_traversal_before_filesystem_probe(tmp_path) -> None:
    with pytest.raises(AdvisoryModelFirstError) as error:
        AdvisoryModelBindingResolver().is_configured(
            model_root=tmp_path,
            program_id="../outside",
            binding_version_id="advb_test",
        )

    assert error.value.reason_code == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID"


def test_descriptor_path_rejects_program_bindings_symlink_escape(tmp_path) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    try:
        (tmp_path / "program_bindings").symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlinks unavailable: {type(exc).__name__}")

    with pytest.raises(AdvisoryModelFirstError) as error:
        AdvisoryModelBindingResolver.descriptor_path(
            model_root=tmp_path,
            program_id="advp_test",
            binding_version_id="advb_test",
        )

    assert error.value.reason_code == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID"


def test_descriptor_path_rejects_program_directory_symlink_escape(tmp_path) -> None:
    binding_root = tmp_path / "program_bindings"
    binding_root.mkdir()
    outside = tmp_path.parent / f"{tmp_path.name}-program-outside"
    outside.mkdir()
    try:
        (binding_root / "advp_test").symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlinks unavailable: {type(exc).__name__}")

    with pytest.raises(AdvisoryModelFirstError) as error:
        AdvisoryModelBindingResolver.descriptor_path(
            model_root=tmp_path,
            program_id="advp_test",
            binding_version_id="advb_test",
        )

    assert error.value.reason_code == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID"


def test_resolve_rejects_program_directory_symlink_before_descriptor_read(
    tmp_path,
) -> None:
    binding_root = tmp_path / "program_bindings"
    binding_root.mkdir()
    outside = tmp_path.parent / f"{tmp_path.name}-resolve-outside"
    outside.mkdir()
    (outside / "advb_test.json").write_text(json.dumps(_descriptor()), encoding="utf-8")
    try:
        (binding_root / "advp_test").symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlinks unavailable: {type(exc).__name__}")

    with pytest.raises(AdvisoryModelFirstError) as error:
        AdvisoryModelBindingResolver().resolve(
            model_root=tmp_path,
            program=SimpleNamespace(program_id="advp_test", package_ids=["pkg_test"]),
            active_binding={
                "binding_version_id": "advb_test",
                "package_ids": ["pkg_test"],
            },
            selection_run=SimpleNamespace(
                manifest_sha256_by_package={"pkg_test": "a" * 64}
            ),
        )

    assert error.value.reason_code == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID"


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
            active_binding={
                "binding_version_id": "advb_test",
                "package_ids": ["pkg_test"],
            },
            selection_run=SimpleNamespace(
                manifest_sha256_by_package={"pkg_test": "a" * 64}
            ),
        )

    assert error.value.reason_code == "ADVISORY_MODEL_CANDIDATE_PROJECTION_UNSUPPORTED"


def test_descriptor_publish_is_atomic_idempotent_and_refuses_identity_overwrite(
    tmp_path,
) -> None:
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


def test_descriptor_rotation_uses_expected_hash_and_preserves_exact_snapshot(
    tmp_path,
) -> None:
    initial = _descriptor()
    initial.pop("descriptor_sha256")
    target = publish_program_model_descriptor(model_root=tmp_path, payload=initial)
    initial_bytes = target.read_bytes()
    initial_sha256 = json.loads(initial_bytes)["descriptor_sha256"]
    replacement = _meta_descriptor()
    replacement.pop("descriptor_sha256")

    receipt = rotate_program_model_descriptor(
        model_root=tmp_path,
        payload=replacement,
        expected_current_descriptor_sha256=initial_sha256,
    )

    persisted = json.loads(target.read_text(encoding="utf-8"))
    assert receipt.operation == "ROTATED"
    assert receipt.previous_descriptor_sha256 == initial_sha256
    assert receipt.descriptor_sha256 == persisted["descriptor_sha256"]
    assert receipt.rollback_snapshot_path is not None
    assert receipt.rollback_snapshot_path.read_bytes() == initial_bytes
    assert (
        AdvisoryModelBindingResolver()
        .resolve(
            model_root=tmp_path,
            program=SimpleNamespace(program_id="advp_test", package_ids=["pkg_test"]),
            active_binding={
                "binding_version_id": "advb_test",
                "package_ids": ["pkg_test"],
            },
            selection_run=SimpleNamespace(
                manifest_sha256_by_package={"pkg_test": "a" * 64}
            ),
        )
        .model_role
        == "meta_label_take_skip_confidence"
    )


def test_descriptor_rotation_rejects_stale_expected_hash_without_mutation(
    tmp_path,
) -> None:
    initial = _descriptor()
    initial.pop("descriptor_sha256")
    target = publish_program_model_descriptor(model_root=tmp_path, payload=initial)
    initial_bytes = target.read_bytes()
    replacement = _meta_descriptor()
    replacement.pop("descriptor_sha256")

    with pytest.raises(AdvisoryModelFirstError) as error:
        rotate_program_model_descriptor(
            model_root=tmp_path,
            payload=replacement,
            expected_current_descriptor_sha256="0" * 64,
        )

    assert error.value.reason_code == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_CONFLICT"
    assert target.read_bytes() == initial_bytes
    assert not (target.parent / ".descriptor_history").exists()


def test_descriptor_rollback_restores_snapshot_and_keeps_reverse_snapshot(
    tmp_path,
) -> None:
    initial = _descriptor()
    initial.pop("descriptor_sha256")
    target = publish_program_model_descriptor(model_root=tmp_path, payload=initial)
    initial_bytes = target.read_bytes()
    initial_sha256 = json.loads(initial_bytes)["descriptor_sha256"]
    replacement = _meta_descriptor()
    replacement.pop("descriptor_sha256")
    rotated = rotate_program_model_descriptor(
        model_root=tmp_path,
        payload=replacement,
        expected_current_descriptor_sha256=initial_sha256,
    )
    replacement_bytes = target.read_bytes()

    receipt = rollback_program_model_descriptor(
        model_root=tmp_path,
        program_id="advp_test",
        binding_version_id="advb_test",
        expected_current_descriptor_sha256=rotated.descriptor_sha256,
        rollback_descriptor_sha256=initial_sha256,
    )

    assert receipt.operation == "ROLLED_BACK"
    assert receipt.previous_descriptor_sha256 == rotated.descriptor_sha256
    assert receipt.descriptor_sha256 == initial_sha256
    assert target.read_bytes() == initial_bytes
    assert receipt.rollback_snapshot_path is not None
    assert receipt.rollback_snapshot_path.read_bytes() == replacement_bytes
    assert (
        AdvisoryModelBindingResolver()
        .resolve(
            model_root=tmp_path,
            program=SimpleNamespace(program_id="advp_test", package_ids=["pkg_test"]),
            active_binding={
                "binding_version_id": "advb_test",
                "package_ids": ["pkg_test"],
            },
            selection_run=SimpleNamespace(
                manifest_sha256_by_package={"pkg_test": "a" * 64}
            ),
        )
        .model_role
        == "quality_reranker"
    )


def test_descriptor_rollback_rejects_missing_snapshot_without_mutation(
    tmp_path,
) -> None:
    initial = _descriptor()
    initial.pop("descriptor_sha256")
    target = publish_program_model_descriptor(model_root=tmp_path, payload=initial)
    initial_bytes = target.read_bytes()
    initial_sha256 = json.loads(initial_bytes)["descriptor_sha256"]

    with pytest.raises(AdvisoryModelFirstError) as error:
        rollback_program_model_descriptor(
            model_root=tmp_path,
            program_id="advp_test",
            binding_version_id="advb_test",
            expected_current_descriptor_sha256=initial_sha256,
            rollback_descriptor_sha256="0" * 64,
        )

    assert (
        error.value.reason_code
        == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE"
    )
    assert target.read_bytes() == initial_bytes


def test_descriptor_rollback_rejects_corrupt_snapshot_without_mutation(
    tmp_path,
) -> None:
    initial = _descriptor()
    initial.pop("descriptor_sha256")
    target = publish_program_model_descriptor(model_root=tmp_path, payload=initial)
    initial_sha256 = json.loads(target.read_text(encoding="utf-8"))["descriptor_sha256"]
    replacement = _meta_descriptor()
    replacement.pop("descriptor_sha256")
    rotated = rotate_program_model_descriptor(
        model_root=tmp_path,
        payload=replacement,
        expected_current_descriptor_sha256=initial_sha256,
    )
    replacement_bytes = target.read_bytes()
    assert rotated.rollback_snapshot_path is not None
    rotated.rollback_snapshot_path.write_text("{}", encoding="utf-8")

    with pytest.raises(AdvisoryModelFirstError) as error:
        rollback_program_model_descriptor(
            model_root=tmp_path,
            program_id="advp_test",
            binding_version_id="advb_test",
            expected_current_descriptor_sha256=rotated.descriptor_sha256,
            rollback_descriptor_sha256=initial_sha256,
        )

    assert (
        error.value.reason_code
        == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_ROLLBACK_UNAVAILABLE"
    )
    assert target.read_bytes() == replacement_bytes


def test_meta_label_descriptor_v2_publish_is_atomic_and_resolvable(tmp_path) -> None:
    payload = _meta_descriptor()
    payload.pop("descriptor_sha256")

    target = publish_program_model_descriptor(model_root=tmp_path, payload=payload)
    resolution = AdvisoryModelBindingResolver().resolve(
        model_root=tmp_path,
        program=SimpleNamespace(program_id="advp_test", package_ids=["pkg_test"]),
        active_binding={"binding_version_id": "advb_test", "package_ids": ["pkg_test"]},
        selection_run=SimpleNamespace(
            manifest_sha256_by_package={"pkg_test": "a" * 64}
        ),
    )

    assert target.is_file()
    assert resolution.model_role == "meta_label_take_skip_confidence"
    assert resolution.bundle_id == "e" * 64


def test_descriptor_publish_rejects_invalid_component_projection_before_write(
    tmp_path,
) -> None:
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


def test_descriptor_cli_requires_explicit_inputs_and_publishes_exact_path(
    tmp_path,
) -> None:
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
    target = (
        tmp_path / "model-root" / "program_bindings" / "advp_test" / "advb_test.json"
    )
    assert receipt == {"ok": True, "descriptor_path": str(target)}
    assert target.is_file()


def test_descriptor_cli_rotates_and_rolls_back_with_verifiable_receipts(
    tmp_path,
) -> None:
    model_root = tmp_path / "model-root"
    initial = _descriptor()
    initial.pop("descriptor_sha256")
    target = publish_program_model_descriptor(model_root=model_root, payload=initial)
    initial_bytes = target.read_bytes()
    initial_sha256 = json.loads(initial_bytes)["descriptor_sha256"]
    replacement = _meta_descriptor()
    replacement.pop("descriptor_sha256")
    replacement_path = tmp_path / "replacement.json"
    replacement_path.write_text(json.dumps(replacement), encoding="utf-8")

    rotated = subprocess.run(
        [
            sys.executable,
            "scripts/advisory_publish_program_model_descriptor.py",
            "--model-root",
            str(model_root),
            "--payload",
            str(replacement_path),
            "--expected-current-descriptor-sha256",
            initial_sha256,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    rotated_receipt = json.loads(rotated.stdout)
    assert rotated_receipt["ok"] is True
    assert rotated_receipt["operation"] == "ROTATED"
    assert rotated_receipt["previous_descriptor_sha256"] == initial_sha256
    replacement_sha256 = rotated_receipt["descriptor_sha256"]
    assert Path(rotated_receipt["rollback_snapshot_path"]).read_bytes() == initial_bytes
    replacement_bytes = target.read_bytes()

    conflict = subprocess.run(
        [
            sys.executable,
            "scripts/advisory_publish_program_model_descriptor.py",
            "--model-root",
            str(model_root),
            "--payload",
            str(replacement_path),
            "--expected-current-descriptor-sha256",
            initial_sha256,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    conflict_receipt = json.loads(conflict.stderr)
    assert conflict.returncode == 2
    assert conflict_receipt["status"] == "failed"
    assert (
        conflict_receipt["reason_code"] == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_CONFLICT"
    )
    assert target.read_bytes() == replacement_bytes

    rolled_back = subprocess.run(
        [
            sys.executable,
            "scripts/advisory_publish_program_model_descriptor.py",
            "--model-root",
            str(model_root),
            "--expected-current-descriptor-sha256",
            replacement_sha256,
            "--rollback-descriptor-sha256",
            initial_sha256,
            "--program-id",
            "advp_test",
            "--binding-version-id",
            "advb_test",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    rollback_receipt = json.loads(rolled_back.stdout)
    assert rollback_receipt["ok"] is True
    assert rollback_receipt["operation"] == "ROLLED_BACK"
    assert rollback_receipt["previous_descriptor_sha256"] == replacement_sha256
    assert rollback_receipt["descriptor_sha256"] == initial_sha256
    assert target.read_bytes() == initial_bytes


def test_descriptor_rotation_serializes_competing_processes(tmp_path) -> None:
    model_root = tmp_path / "model-root"
    initial = _descriptor()
    initial.pop("descriptor_sha256")
    target = publish_program_model_descriptor(model_root=model_root, payload=initial)
    initial_sha256 = json.loads(target.read_text(encoding="utf-8"))["descriptor_sha256"]
    payload_paths: list[Path] = []
    for index, bundle_id in enumerate(("7" * 64, "8" * 64)):
        replacement = _meta_descriptor()
        replacement.pop("descriptor_sha256")
        replacement["bundle_id"] = bundle_id
        path = tmp_path / f"replacement-{index}.json"
        path.write_text(json.dumps(replacement), encoding="utf-8")
        payload_paths.append(path)

    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "scripts/advisory_publish_program_model_descriptor.py",
                "--model-root",
                str(model_root),
                "--payload",
                str(payload_path),
                "--expected-current-descriptor-sha256",
                initial_sha256,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for payload_path in payload_paths
    ]
    completed = [
        (*process.communicate(timeout=20), process.returncode) for process in processes
    ]

    assert sorted(return_code for _, _, return_code in completed) == [0, 2]
    success = json.loads(
        next(stdout for stdout, _, return_code in completed if return_code == 0)
    )
    conflict = json.loads(
        next(stderr for _, stderr, return_code in completed if return_code == 2)
    )
    assert success["operation"] == "ROTATED"
    assert conflict["reason_code"] == "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_CONFLICT"
    assert (
        json.loads(target.read_text(encoding="utf-8"))["descriptor_sha256"]
        == success["descriptor_sha256"]
    )
