from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.execution_algos.vnpy_compat.locked_surface import (
    PINNED_SOURCE_ROOT,
    PinnedSourceManifestV2,
    extract_locked_surface_v1,
    load_pinned_source_manifest_v1,
)
from backend.execution_algos.vnpy_compat.receipts import (
    bound_compatibility_failures_v1,
    build_current_three_compatibility_receipts_v1,
    build_vnpy_compatibility_receipt_v1,
    readback_vnpy_compatibility_receipt_v1,
)
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v1,
    current_three_descriptors_v2,
    current_three_manifests_v2,
    current_three_process_bindings_v2,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    canonical_json_bytes_v1,
    hash_hex_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    FileHashV1,
    VnpyCompatibilityRequirementV2,
    VnpyMethodRequirementV1,
    VnpyParameterKindV1,
    VnpyParameterRequirementV1,
    compatibility_component_hashes_v2,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    CompatibilityStatusV1,
    PluginCatalogBuildError,
    VnpyCompatibilityFailureV1,
    VnpyCompatibilityReceiptV2,
    build_plugin_catalog_v2,
)


REPO_ROOT = Path(__file__).resolve().parents[3]


def _manifest(algo_code: str = "SNIPER_MINIQMT"):
    return next(item for item in current_three_manifests_v2() if item.algo_code == algo_code)


def _copy_authority(tmp_path: Path) -> Path:
    target = tmp_path / "pinned_source"
    shutil.copytree(PINNED_SOURCE_ROOT, target)
    return target


def _reason_codes(receipt: VnpyCompatibilityReceiptV2) -> set[str]:
    return {item.reason_code for item in receipt.ordered_failures}


def _rehash_source_manifest(payload: dict[str, object]) -> None:
    authorities = payload["upstream_sources"]
    assert isinstance(authorities, list)
    for authority in authorities:
        assert isinstance(authority, dict)
        files = authority["files"]
        assert isinstance(files, list)
        files.sort(key=lambda item: item["path"])
        authority["authority_sha256"] = hash_hex_v1(
            "miniqmt_vnpy_upstream_source_authority_v2",
            {key: value for key, value in authority.items() if key != "authority_sha256"},
        )
    payload["source_manifest_sha256"] = hash_hex_v1(
        "miniqmt_vnpy_pinned_source_manifest_v2",
        {key: value for key, value in payload.items() if key != "source_manifest_sha256"},
    )


def test_real_pinned_source_manifest_and_surface_read_back_exactly() -> None:
    source = load_pinned_source_manifest_v1(PINNED_SOURCE_ROOT)
    requirement = _manifest().compatibility_requirement
    surface = extract_locked_surface_v1(requirement=requirement, source_root=PINNED_SOURCE_ROOT)

    assert source.schema_version == "vnpy_pinned_source_manifest_v2"
    assert tuple(item.namespace for item in source.upstream_sources) == ("VNPY_ALGOTRADING", "VNPY_CORE")
    assert {item.upstream_commit for item in source.upstream_sources} == {
        "4133987530eb28f3538d1983545d81c4f83d7d59",
        "1049acf64afd5b2d06d09b1e139dd0cca5d9d6b9",
    }
    assert len(source.files) == 8
    assert {item.license for item in source.upstream_sources} == {"MIT License"}
    assert {item.license_file.path for item in source.upstream_sources} == {
        "vnpy_algotrading/LICENSE",
        "vnpy_core/LICENSE",
    }
    assert source.characterization_file.path == "surface_contract.json"
    assert surface.ordered_failures == ()
    assert surface.source_lock_sha256 == compatibility_component_hashes_v2(requirement)["source_lock_sha256"]
    assert surface.surface_sha256 == compatibility_component_hashes_v2(requirement)["surface_sha256"]


def test_k1_delegates_only_the_exact_k4_pinned_artifact_paths() -> None:
    receipts = build_current_three_compatibility_receipts_v1()

    assert len(receipts) == 3
    assert {receipt.status for receipt in receipts} == {CompatibilityStatusV1.PASSED}


def test_public_source_and_receipt_builders_reject_non_strict_carriers() -> None:
    requirement = _manifest().compatibility_requirement
    with pytest.raises(TypeError, match="pathlib.Path"):
        load_pinned_source_manifest_v1(str(PINNED_SOURCE_ROOT))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="strict production types"):
        extract_locked_surface_v1(requirement=requirement, source_root=str(PINNED_SOURCE_ROOT))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="tuple"):
        bound_compatibility_failures_v1([])  # type: ignore[arg-type]


def test_requirement_is_structured_strict_and_input_order_independent() -> None:
    requirement = _manifest().compatibility_requirement
    assert requirement.required_method_signatures
    assert all(type(item) is VnpyMethodRequirementV1 for item in requirement.required_method_signatures)
    send_order = next(item for item in requirement.required_method_signatures if item.name == "send_order")
    assert tuple(parameter.name for parameter in send_order.parameters) == (
        "algo",
        "direction",
        "price",
        "volume",
        "order_type",
        "offset",
    )
    assert all(parameter.kind is VnpyParameterKindV1.POSITIONAL_OR_KEYWORD for parameter in send_order.parameters)

    payload = requirement.model_dump(mode="python")
    payload["source_files_and_hashes"] = tuple(reversed(payload["source_files_and_hashes"]))
    payload["required_method_signatures"] = tuple(reversed(payload["required_method_signatures"]))
    payload["required_object_fields"] = tuple(reversed(payload["required_object_fields"]))
    payload["required_enum_values"] = tuple(reversed(payload["required_enum_values"]))
    reconstructed = VnpyCompatibilityRequirementV2.model_validate(payload, strict=True)
    assert reconstructed == requirement
    assert canonical_json_bytes_v1(reconstructed.canonical_payload_v1()) == canonical_json_bytes_v1(
        requirement.canonical_payload_v1()
    )


def test_requirement_rejects_bool_as_parameter_flags_and_hash_drift() -> None:
    payload = VnpyParameterRequirementV1(
        name="algo",
        kind=VnpyParameterKindV1.POSITIONAL_OR_KEYWORD,
        required=True,
        default_present=False,
        default_value=None,
        annotation="AlgoTemplate",
    ).model_dump(mode="python")
    payload["required"] = 1
    with pytest.raises(ValidationError):
        VnpyParameterRequirementV1.model_validate(payload, strict=True)

    requirement_payload = _manifest().compatibility_requirement.model_dump(mode="python")
    requirement_payload["requirement_sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="requirement_sha256"):
        VnpyCompatibilityRequirementV2.model_validate(requirement_payload, strict=True)


def test_requirement_rejects_impossible_parameter_default_closure() -> None:
    base = {
        "name": "algo",
        "kind": VnpyParameterKindV1.POSITIONAL_OR_KEYWORD,
        "required": False,
        "default_present": False,
        "default_value": None,
        "annotation": "AlgoTemplate",
    }
    with pytest.raises(ValidationError, match="required or have an explicit default"):
        VnpyParameterRequirementV1.model_validate(base, strict=True)

    variadic = {**base, "kind": VnpyParameterKindV1.VAR_POSITIONAL, "required": True}
    with pytest.raises(ValidationError, match="variadic parameter"):
        VnpyParameterRequirementV1.model_validate(variadic, strict=True)


def test_requirement_rejects_duplicate_source_paths_before_hash_acceptance() -> None:
    requirement = _manifest().compatibility_requirement
    payload = requirement.model_dump(mode="python")
    payload["source_files_and_hashes"] = (
        FileHashV1(path="vnpy_algotrading/engine.py", sha256="1" * 64),
        FileHashV1(path="vnpy_algotrading/engine.py", sha256="2" * 64),
    )
    with pytest.raises(ValidationError, match="unique keys"):
        VnpyCompatibilityRequirementV2.model_validate(payload, strict=True)


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_source_manifest_requires_exact_eight_file_path_set(mutation: str) -> None:
    payload = json.loads((PINNED_SOURCE_ROOT / "source_manifest.json").read_text(encoding="utf-8"))
    algotrading = payload["upstream_sources"][0]
    if mutation == "missing":
        algotrading["files"].pop()
    else:
        algotrading["files"].append(
            {
                "schema_version": "vnpy_source_file_v2",
                "path": "vnpy_algotrading/algos/unsupported_algo.py",
                "sha256": "1" * 64,
                "size_bytes": 1,
            }
        )
    _rehash_source_manifest(payload)

    with pytest.raises(ValidationError, match="exact eight-file"):
        PinnedSourceManifestV2.model_validate_json(canonical_json_bytes_v1(payload), strict=True)


def test_requirement_rejects_invalid_method_and_source_identity_before_hash_acceptance() -> None:
    requirement = _manifest().compatibility_requirement

    empty_methods = requirement.model_dump(mode="python")
    empty_methods["required_method_signatures"] = ()
    with pytest.raises(ValidationError, match="required_method_signatures"):
        VnpyCompatibilityRequirementV2.model_validate(empty_methods, strict=True)

    duplicate_methods = requirement.model_dump(mode="python")
    duplicate_methods["required_method_signatures"] = (
        requirement.required_method_signatures[0],
        requirement.required_method_signatures[0],
    )
    with pytest.raises(ValidationError, match="unique method identities"):
        VnpyCompatibilityRequirementV2.model_validate(duplicate_methods, strict=True)

    missing_method_source = requirement.model_dump(mode="python")
    method_payload = missing_method_source["required_method_signatures"][0]
    method_payload["source_path"] = "vnpy_core/vnpy/trader/object.py"
    canonical_method = requirement.required_method_signatures[0].canonical_payload_v1(
        exclude={"method_requirement_sha256"}
    )
    canonical_method["source_path"] = "vnpy_core/vnpy/trader/object.py"
    method_payload["method_requirement_sha256"] = hash_hex_v1(
        "miniqmt_vnpy_method_requirement_v1",
        canonical_method,
    )
    with pytest.raises(ValidationError, match="method source_path"):
        VnpyCompatibilityRequirementV2.model_validate(missing_method_source, strict=True)

    for invalid_path in ("../engine.py", "C:/escape.py", "..\\escape.py"):
        traversal = requirement.model_dump(mode="python")
        traversal["source_files_and_hashes"] = (
            FileHashV1(path=invalid_path, sha256="1" * 64),
            *requirement.source_files_and_hashes,
        )
        with pytest.raises(ValidationError, match="normalized relative POSIX"):
            VnpyCompatibilityRequirementV2.model_validate(traversal, strict=True)

    wrong_mode = requirement.model_dump(mode="python")
    wrong_mode["mode"] = "derived_source_exact_characterization"
    with pytest.raises(ValidationError, match="mode"):
        VnpyCompatibilityRequirementV2.model_validate(wrong_mode, strict=True)


@pytest.mark.parametrize(
    ("relative_path", "reason"),
    [
        ("vnpy_algotrading/engine.py", "MINIQMT_VNPY_COMPAT_SOURCE_MISSING"),
        ("vnpy_algotrading/template.py", "MINIQMT_VNPY_COMPAT_SOURCE_MISSING"),
    ],
)
def test_missing_pinned_source_produces_failed_receipt(tmp_path: Path, relative_path: str, reason: str) -> None:
    source_root = _copy_authority(tmp_path)
    (source_root / relative_path).unlink()

    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)

    assert receipt.status is CompatibilityStatusV1.FAILED
    assert reason in _reason_codes(receipt)


def test_source_hash_decode_ast_and_method_signature_drift_are_aggregated(tmp_path: Path) -> None:
    source_root = _copy_authority(tmp_path)
    engine = source_root / "vnpy_algotrading/engine.py"
    engine.write_text(
        engine.read_text(encoding="utf-8").replace(
            "direction: Direction,\n        price: float",
            "price: float,\n        direction: Direction",
        ),
        encoding="utf-8",
    )
    (source_root / "vnpy_algotrading/template.py").write_bytes(b"\xff\xfe")

    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)

    reasons = _reason_codes(receipt)
    assert receipt.status is CompatibilityStatusV1.FAILED
    assert "MINIQMT_VNPY_COMPAT_SOURCE_HASH_DRIFT" in reasons
    assert "MINIQMT_VNPY_COMPAT_SOURCE_DECODE_INVALID" in reasons
    assert "MINIQMT_VNPY_COMPAT_METHOD_SIGNATURE_DRIFT" in reasons
    assert receipt.ordered_failures == tuple(sorted(receipt.ordered_failures, key=lambda item: item.sort_key_v1()))


def test_core_object_and_enum_source_drift_is_explicitly_failed(tmp_path: Path) -> None:
    source_root = _copy_authority(tmp_path)
    object_path = source_root / "vnpy_core/vnpy/trader/object.py"
    object_path.write_text(
        object_path.read_text(encoding="utf-8").replace("datetime: Datetime\n", "datetime: Datetime | None\n", 1),
        encoding="utf-8",
    )
    constant_path = source_root / "vnpy_core/vnpy/trader/constant.py"
    constant_path.write_text(
        constant_path.read_text(encoding="utf-8").replace('LONG = _("多")', 'LONG = _("多变")', 1),
        encoding="utf-8",
    )

    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)

    assert receipt.status is CompatibilityStatusV1.FAILED
    reasons = _reason_codes(receipt)
    assert "MINIQMT_VNPY_COMPAT_SOURCE_HASH_DRIFT" in reasons
    assert "MINIQMT_VNPY_COMPAT_OBJECT_FIELD_DRIFT" in reasons
    assert "MINIQMT_VNPY_COMPAT_ENUM_VALUE_DRIFT" in reasons


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ("datetime: Datetime\n", "datetime: Datetime | None\n"),
        ("datetime: Datetime | None = None\n", "datetime: Datetime = None\n"),
        ("def is_active(self) -> bool:\n", "def is_active(self) -> str:\n"),
    ],
)
def test_core_object_type_nullability_and_callable_drift_is_explicit(
    tmp_path: Path,
    before: str,
    after: str,
) -> None:
    source_root = _copy_authority(tmp_path)
    object_path = source_root / "vnpy_core/vnpy/trader/object.py"
    original = object_path.read_text(encoding="utf-8")
    assert before in original
    object_path.write_text(original.replace(before, after), encoding="utf-8")

    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)

    assert receipt.status is CompatibilityStatusV1.FAILED
    assert "MINIQMT_VNPY_COMPAT_OBJECT_FIELD_DRIFT" in _reason_codes(receipt)


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ('    NET = _("净")\n', ""),
        ('    NET = _("净")\n', '    NET = _("净")\n    HEDGE = _("对冲")\n'),
        ('    LONG = _("多")\n', '    LONG = _("多变")\n'),
    ],
)
def test_core_enum_missing_extra_and_value_drift_is_explicit(
    tmp_path: Path,
    before: str,
    after: str,
) -> None:
    source_root = _copy_authority(tmp_path)
    constant_path = source_root / "vnpy_core/vnpy/trader/constant.py"
    original = constant_path.read_text(encoding="utf-8")
    assert before in original
    constant_path.write_text(original.replace(before, after, 1), encoding="utf-8")

    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)

    assert receipt.status is CompatibilityStatusV1.FAILED
    assert "MINIQMT_VNPY_COMPAT_ENUM_VALUE_DRIFT" in _reason_codes(receipt)


@pytest.mark.parametrize(
    ("relative_path", "reason"),
    [
        ("vnpy_core/vnpy/trader/object.py", "MINIQMT_VNPY_COMPAT_SOURCE_MISSING"),
        ("vnpy_core/vnpy/trader/constant.py", "MINIQMT_VNPY_COMPAT_SOURCE_MISSING"),
    ],
)
def test_core_source_missing_fails_loud(tmp_path: Path, relative_path: str, reason: str) -> None:
    source_root = _copy_authority(tmp_path)
    (source_root / relative_path).unlink()

    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)

    assert receipt.status is CompatibilityStatusV1.FAILED
    assert reason in _reason_codes(receipt)


def test_core_source_decode_and_ast_failures_are_retained(tmp_path: Path) -> None:
    source_root = _copy_authority(tmp_path)
    (source_root / "vnpy_core/vnpy/trader/constant.py").write_bytes(b"\xff\xfe")
    (source_root / "vnpy_core/vnpy/trader/object.py").write_text("def broken(:\n", encoding="utf-8")

    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)

    assert {
        "MINIQMT_VNPY_COMPAT_SOURCE_DECODE_INVALID",
        "MINIQMT_VNPY_COMPAT_SOURCE_AST_INVALID",
    }.issubset(_reason_codes(receipt))


def test_core_license_bytes_hash_and_size_drift_are_retained(tmp_path: Path) -> None:
    source_root = _copy_authority(tmp_path)
    license_path = source_root / "vnpy_core/LICENSE"
    license_path.write_bytes(license_path.read_bytes() + b"drift")

    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)

    assert {
        "MINIQMT_VNPY_COMPAT_LICENSE_HASH_DRIFT",
        "MINIQMT_VNPY_COMPAT_LICENSE_SIZE_DRIFT",
    }.issubset(_reason_codes(receipt))


def test_core_source_extra_duplicate_and_traversal_fail_loud(tmp_path: Path) -> None:
    source_root = _copy_authority(tmp_path)
    (source_root / "vnpy_core/extra.py").write_text("VALUE = 1\n", encoding="utf-8")
    extra_receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)
    assert "MINIQMT_VNPY_COMPAT_SOURCE_EXTRA" in _reason_codes(extra_receipt)

    payload = json.loads((source_root / "source_manifest.json").read_text(encoding="utf-8"))
    core = payload["upstream_sources"][1]
    core["files"].append(dict(core["files"][0]))
    _rehash_source_manifest(payload)
    (source_root / "source_manifest.json").write_bytes(canonical_json_bytes_v1(payload))
    duplicate_receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)
    assert "MINIQMT_VNPY_COMPAT_SOURCE_MANIFEST_INVALID" in _reason_codes(duplicate_receipt)

    traversal = json.loads((PINNED_SOURCE_ROOT / "source_manifest.json").read_text(encoding="utf-8"))
    traversal["upstream_sources"][1]["files"][0]["path"] = "../constant.py"
    _rehash_source_manifest(traversal)
    (source_root / "source_manifest.json").write_bytes(canonical_json_bytes_v1(traversal))
    traversal_receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)
    assert "MINIQMT_VNPY_COMPAT_SOURCE_MANIFEST_INVALID" in _reason_codes(traversal_receipt)


def test_requirement_and_surface_contract_drift_cannot_self_certify_core_bytes(tmp_path: Path) -> None:
    source_root = _copy_authority(tmp_path)
    contract = json.loads((source_root / "surface_contract.json").read_text(encoding="utf-8"))
    contract["required_object_fields"][0]["fields"][0]["annotation"] = "object"
    contract["surface_contract_sha256"] = hash_hex_v1(
        "miniqmt_vnpy_surface_contract_v2",
        {key: value for key, value in contract.items() if key != "surface_contract_sha256"},
    )
    (source_root / "surface_contract.json").write_bytes(canonical_json_bytes_v1(contract))

    requirement_payload = _manifest().compatibility_requirement.model_dump(mode="python")
    requirement_payload["required_object_fields"][0]["fields"][0]["annotation"] = "object"
    canonical_requirement = _manifest().compatibility_requirement.canonical_payload_v1(exclude={"requirement_sha256"})
    canonical_requirement["required_object_fields"][0]["fields"][0]["annotation"] = "object"
    requirement_payload["requirement_sha256"] = hash_hex_v1(
        "miniqmt_vnpy_compatibility_requirement_v2",
        canonical_requirement,
    )
    drifted_requirement = VnpyCompatibilityRequirementV2.model_validate(requirement_payload, strict=True)
    drifted_manifest = _manifest().model_copy(update={"compatibility_requirement": drifted_requirement})
    receipt = build_vnpy_compatibility_receipt_v1(manifest=drifted_manifest, source_root=source_root)

    assert receipt.status is CompatibilityStatusV1.FAILED
    assert "MINIQMT_VNPY_COMPAT_OBJECT_FIELD_DRIFT" in _reason_codes(receipt)


@pytest.mark.parametrize(
    ("field", "expected_reason"),
    [
        ("upstream_repo", "MINIQMT_VNPY_COMPAT_SOURCE_IDENTITY_DRIFT"),
        ("release_tag", "MINIQMT_VNPY_COMPAT_SOURCE_IDENTITY_DRIFT"),
        ("upstream_commit", "MINIQMT_VNPY_COMPAT_SOURCE_IDENTITY_DRIFT"),
        ("source_path", "MINIQMT_VNPY_COMPAT_SOURCE_MANIFEST_INVALID"),
        ("source_sha256", "MINIQMT_VNPY_COMPAT_SOURCE_IDENTITY_DRIFT"),
        ("source_size_bytes", "MINIQMT_VNPY_COMPAT_SOURCE_IDENTITY_DRIFT"),
        ("license", "MINIQMT_VNPY_COMPAT_SOURCE_IDENTITY_DRIFT"),
        ("license_path", "MINIQMT_VNPY_COMPAT_SOURCE_MANIFEST_INVALID"),
        ("license_traversal", "MINIQMT_VNPY_COMPAT_SOURCE_MANIFEST_INVALID"),
        ("license_sha256", "MINIQMT_VNPY_COMPAT_SOURCE_IDENTITY_DRIFT"),
    ],
)
def test_core_source_authority_metadata_drift_is_not_accepted(
    tmp_path: Path,
    field: str,
    expected_reason: str,
) -> None:
    source_root = _copy_authority(tmp_path)
    payload = json.loads((source_root / "source_manifest.json").read_text(encoding="utf-8"))
    core = payload["upstream_sources"][1]
    mutations = {
        "upstream_repo": lambda: core.__setitem__("upstream_repo", "https://github.com/vnpy/not-vnpy"),
        "release_tag": lambda: core.__setitem__("release_tag", "4.0.1"),
        "upstream_commit": lambda: core.__setitem__("upstream_commit", "0" * 40),
        "source_path": lambda: core["files"][0].__setitem__("path", "vnpy_core/vnpy/trader/constant_drift.py"),
        "source_sha256": lambda: core["files"][0].__setitem__("sha256", "0" * 64),
        "source_size_bytes": lambda: core["files"][0].__setitem__("size_bytes", 1),
        "license": lambda: core.__setitem__("license", "not-the-pinned-license"),
        "license_path": lambda: core["license_file"].__setitem__("path", "vnpy_core/LICENSE.drift"),
        "license_traversal": lambda: core["license_file"].__setitem__("path", "../LICENSE"),
        "license_sha256": lambda: core["license_file"].__setitem__("sha256", "0" * 64),
    }
    mutations[field]()
    _rehash_source_manifest(payload)
    (source_root / "source_manifest.json").write_bytes(canonical_json_bytes_v1(payload))

    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)

    assert receipt.status is CompatibilityStatusV1.FAILED
    assert expected_reason in _reason_codes(receipt)


def test_extra_source_and_path_traversal_fail_loud(tmp_path: Path) -> None:
    source_root = _copy_authority(tmp_path)
    extra = source_root / "vnpy_algotrading/extra.py"
    extra.write_text("VALUE = 1\n", encoding="utf-8")
    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)
    assert "MINIQMT_VNPY_COMPAT_SOURCE_EXTRA" in _reason_codes(receipt)

    payload = json.loads((source_root / "source_manifest.json").read_text(encoding="utf-8"))
    payload["upstream_sources"][0]["files"][0]["path"] = "../escape.py"
    _rehash_source_manifest(payload)
    with pytest.raises(ValidationError, match="path"):
        PinnedSourceManifestV2.model_validate_json(canonical_json_bytes_v1(payload), strict=True)

    for invalid_path in ("C:/escape.py", "..\\escape.py"):
        windows_payload = json.loads((PINNED_SOURCE_ROOT / "source_manifest.json").read_text(encoding="utf-8"))
        windows_payload["upstream_sources"][0]["files"][0]["path"] = invalid_path
        _rehash_source_manifest(windows_payload)
        with pytest.raises(ValidationError, match="path"):
            PinnedSourceManifestV2.model_validate_json(canonical_json_bytes_v1(windows_payload), strict=True)


def test_object_and_enum_surface_contract_drift_is_visible(tmp_path: Path) -> None:
    source_root = _copy_authority(tmp_path)
    contract_path = source_root / "surface_contract.json"
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract["required_object_fields"][0]["fields"][0]["annotation"] = "object"
    contract["required_enum_values"][0]["members"].append(
        {
            "schema_version": "vnpy_enum_member_requirement_v2",
            "name": "UNKNOWN",
            "value_expression": "'UNKNOWN'",
        }
    )
    contract["surface_contract_sha256"] = hash_hex_v1(
        "miniqmt_vnpy_surface_contract_v2",
        {key: value for key, value in contract.items() if key != "surface_contract_sha256"},
    )
    contract_path.write_bytes(canonical_json_bytes_v1(contract))

    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)

    reasons = _reason_codes(receipt)
    assert "MINIQMT_VNPY_COMPAT_OBJECT_FIELD_DRIFT" in reasons
    assert "MINIQMT_VNPY_COMPAT_ENUM_VALUE_DRIFT" in reasons


def test_current_three_real_receipts_are_passed_exact_and_readback_stable() -> None:
    receipts = build_current_three_compatibility_receipts_v1()
    manifests = current_three_manifests_v2()

    assert len(receipts) == 3
    assert all(item.status is CompatibilityStatusV1.PASSED for item in receipts)
    assert all(item.ordered_failures == () for item in receipts)
    for manifest, receipt in zip(manifests, receipts, strict=True):
        assert (
            readback_vnpy_compatibility_receipt_v1(
                manifest=manifest,
                receipt=receipt,
                source_root=PINNED_SOURCE_ROOT,
            )
            == receipt
        )
        assert (
            VnpyCompatibilityReceiptV2.model_validate_json(
                canonical_json_bytes_v1(receipt.canonical_payload_v1()), strict=True
            )
            == receipt
        )


def test_receipt_hash_drift_is_rejected_by_writer_and_authority_readback() -> None:
    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=PINNED_SOURCE_ROOT)
    payload = receipt.model_dump(mode="python")
    payload["receipt_sha256"] = "f" * 64
    with pytest.raises(ValidationError, match="receipt hash"):
        VnpyCompatibilityReceiptV2.model_validate(payload, strict=True)


def test_failed_receipt_readback_recomputes_the_same_authority(tmp_path: Path) -> None:
    source_root = _copy_authority(tmp_path)
    (source_root / "vnpy_algotrading/engine.py").unlink()
    manifest = _manifest()
    receipt = build_vnpy_compatibility_receipt_v1(manifest=manifest, source_root=source_root)

    assert receipt.status is CompatibilityStatusV1.FAILED
    assert (
        readback_vnpy_compatibility_receipt_v1(
            manifest=manifest,
            receipt=receipt,
            source_root=source_root,
        )
        == receipt
    )


def test_failed_receipt_is_checkout_path_independent(tmp_path: Path) -> None:
    first_root = _copy_authority(tmp_path / "first")
    second_root = _copy_authority(tmp_path / "second")
    for source_root in (first_root, second_root):
        (source_root / "vnpy_algotrading/engine.py").unlink()

    first = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=first_root)
    second = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=second_root)

    assert first.status is CompatibilityStatusV1.FAILED
    assert canonical_json_bytes_v1(first.canonical_payload_v1()) == canonical_json_bytes_v1(
        second.canonical_payload_v1()
    )


def test_receipt_status_and_failure_set_must_close() -> None:
    passed = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=PINNED_SOURCE_ROOT)
    failure = VnpyCompatibilityFailureV1.create(
        field_path="source_files[missing.py]",
        reason_code="MINIQMT_VNPY_COMPAT_SOURCE_MISSING",
        context={"path": "missing.py"},
    )
    with pytest.raises(ValidationError, match="PASSED compatibility receipt cannot contain failures"):
        VnpyCompatibilityReceiptV2.create(
            plugin_id=passed.plugin_id,
            plugin_version=passed.plugin_version,
            manifest_sha256=passed.manifest_sha256,
            requirement_sha256=passed.requirement_sha256,
            surface_sha256=passed.surface_sha256,
            source_lock_sha256=passed.source_lock_sha256,
            method_signature_sha256=passed.method_signature_sha256,
            object_field_sha256=passed.object_field_sha256,
            characterization_sha256=passed.characterization_sha256,
            status=CompatibilityStatusV1.PASSED,
            ordered_failures=(failure,),
        )
    with pytest.raises(ValidationError, match="FAILED compatibility receipt must contain failures"):
        VnpyCompatibilityReceiptV2.create(
            plugin_id=passed.plugin_id,
            plugin_version=passed.plugin_version,
            manifest_sha256=passed.manifest_sha256,
            requirement_sha256=passed.requirement_sha256,
            surface_sha256=passed.surface_sha256,
            source_lock_sha256=passed.source_lock_sha256,
            method_signature_sha256=passed.method_signature_sha256,
            object_field_sha256=passed.object_field_sha256,
            characterization_sha256=passed.characterization_sha256,
            status=CompatibilityStatusV1.FAILED,
            ordered_failures=(),
        )


def test_failure_evidence_is_bounded_with_explicit_truncation(tmp_path: Path) -> None:
    source_root = _copy_authority(tmp_path)
    for index in range(300):
        path = source_root / "vnpy_algotrading" / "extras" / f"rogue_{index:03d}.py"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("VALUE = 1\n", encoding="utf-8")

    receipt = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=source_root)

    assert receipt.status is CompatibilityStatusV1.FAILED
    assert len(receipt.ordered_failures) == 256
    marker = receipt.ordered_failures[-1]
    assert marker.field_path == "__failure_set__"
    assert marker.reason_code == "MINIQMT_VNPY_COMPAT_FAILURES_TRUNCATED"
    assert marker.context[0].key == "omitted_count"


def test_malformed_truncation_marker_is_rejected() -> None:
    passed = build_vnpy_compatibility_receipt_v1(manifest=_manifest(), source_root=PINNED_SOURCE_ROOT)
    retained = tuple(
        VnpyCompatibilityFailureV1.create(
            field_path=f"source_files[rogue_{index:03d}.py]",
            reason_code="MINIQMT_VNPY_COMPAT_SOURCE_EXTRA",
            context={"path": f"rogue_{index:03d}.py"},
        )
        for index in range(255)
    )
    malformed_marker = VnpyCompatibilityFailureV1.create(
        field_path="__failure_set__",
        reason_code="MINIQMT_VNPY_COMPAT_FAILURES_TRUNCATED",
        context={"omitted_count": 0, "omitted_failure_set_sha256": "0" * 64},
    )

    with pytest.raises(ValidationError, match="omitted_count"):
        VnpyCompatibilityReceiptV2.create(
            plugin_id=passed.plugin_id,
            plugin_version=passed.plugin_version,
            manifest_sha256=passed.manifest_sha256,
            requirement_sha256=passed.requirement_sha256,
            surface_sha256=passed.surface_sha256,
            source_lock_sha256=passed.source_lock_sha256,
            method_signature_sha256=passed.method_signature_sha256,
            object_field_sha256=passed.object_field_sha256,
            characterization_sha256=passed.characterization_sha256,
            status=CompatibilityStatusV1.FAILED,
            ordered_failures=(*retained, malformed_marker),
        )


def test_retry_and_fresh_process_receipts_are_byte_identical() -> None:
    expected = canonical_json_bytes_v1(
        [item.canonical_payload_v1() for item in build_current_three_compatibility_receipts_v1()]
    )
    repeated = canonical_json_bytes_v1(
        [item.canonical_payload_v1() for item in build_current_three_compatibility_receipts_v1()]
    )
    script = """
import sys
sys.path.insert(0, sys.argv[1])
from backend.execution_algos.vnpy_compat.receipts import build_current_three_compatibility_receipts_v1
from backend.services.miniqmt_execution_runtime.plugin_canonical import canonical_json_bytes_v1
sys.stdout.buffer.write(canonical_json_bytes_v1([item.canonical_payload_v1() for item in build_current_three_compatibility_receipts_v1()]))
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script, str(REPO_ROOT)],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
    )
    assert expected == repeated == completed.stdout
    assert b"0x" not in expected


def test_real_current_three_receipts_build_exact_catalog() -> None:
    receipts = build_current_three_compatibility_receipts_v1()
    runtime = build_plugin_catalog_v2(
        descriptors=current_three_descriptors_v2(),
        creation_bindings=current_three_creation_bindings_v1(),
        process_bindings=current_three_process_bindings_v2(),
        pinned_compatibility_receipts=receipts,
    )
    assert runtime.snapshot.pinned_compatibility_receipts == receipts
    assert len(runtime.snapshot.registration_descriptors) == 3


@pytest.mark.parametrize(
    "relative_path",
    [
        "vnpy_algotrading/algos/sniper_algo.py",
        "vnpy_core/vnpy/trader/object.py",
    ],
)
def test_one_real_failed_receipt_prevents_catalog_publication(tmp_path: Path, relative_path: str) -> None:
    source_root = _copy_authority(tmp_path)
    (source_root / relative_path).unlink()
    receipts = list(build_current_three_compatibility_receipts_v1())
    manifests = current_three_manifests_v2()
    sniper_index = next(index for index, item in enumerate(manifests) if item.algo_code == "SNIPER_MINIQMT")
    receipts[sniper_index] = build_vnpy_compatibility_receipt_v1(
        manifest=manifests[sniper_index],
        source_root=source_root,
    )

    with pytest.raises(PluginCatalogBuildError) as exc_info:
        build_plugin_catalog_v2(
            descriptors=current_three_descriptors_v2(),
            creation_bindings=current_three_creation_bindings_v1(),
            process_bindings=current_three_process_bindings_v2(),
            pinned_compatibility_receipts=tuple(receipts),
        )

    aggregate_failure = next(
        item for item in exc_info.value.receipt.ordered_failures if item.stage.value == "PINNED_COMPATIBILITY"
    )
    aggregate_context = thaw_json_v1(aggregate_failure.context)
    assert aggregate_context["status"] == "FAILED"
    assert any(
        item["reason_code"] == "MINIQMT_VNPY_COMPAT_SOURCE_MISSING" for item in aggregate_context["ordered_failures"]
    )


def test_requirement_binds_exact_vnpy_core_object_and_enum_authority() -> None:
    requirement = _manifest().compatibility_requirement

    assert requirement.schema_version == "vnpy_compatibility_requirement_v2"
    authorities = {item.namespace: item for item in requirement.upstream_sources}
    core = authorities["VNPY_CORE"]
    assert core.upstream_repo == "https://github.com/vnpy/vnpy"
    assert core.release_tag == "4.0.0"
    assert core.upstream_commit == "1049acf64afd5b2d06d09b1e139dd0cca5d9d6b9"
    assert {(item.path, item.size_bytes, item.sha256) for item in core.files} == {
        (
            "vnpy_core/vnpy/trader/constant.py",
            4342,
            "5a220fcc85bea0c4d92426533bcac444f74addd63b9b037a867370fe350df651",
        ),
        (
            "vnpy_core/vnpy/trader/object.py",
            10509,
            "c153445fdad392bf6ac645b992e624df66e10a49c87448ca8ab2bf770212d75a",
        ),
    }
    assert core.license_file.path == "vnpy_core/LICENSE"
    assert core.license_file.sha256 == "81294e5bcba945564df8586f1d789b016001b7b43eb4de97736679dd882cf191"
    source_paths = {item.path for item in requirement.source_files_and_hashes}
    assert "vnpy_core/vnpy/trader/object.py" in source_paths
    assert "vnpy_core/vnpy/trader/constant.py" in source_paths
    assert all(item.source_path in source_paths for item in requirement.required_object_fields)
    assert all(item.source_path in source_paths for item in requirement.required_enum_values)
