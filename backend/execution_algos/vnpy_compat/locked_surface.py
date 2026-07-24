"""Offline source and structured surface authority for pinned vn.py compatibility."""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Literal, Self

from pydantic import ValidationError, model_validator

from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    EnumValueRequirementV1,
    FrozenStrictModel,
    IdentityV1,
    NonNegativeIntV1,
    ObjectFieldRequirementV1,
    Sha256V1,
    VnpyCompatibilityRequirementV1,
    VnpyMethodRequirementV1,
    VnpyParameterKindV1,
    VnpyParameterRequirementV1,
    compatibility_component_hashes_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import VnpyCompatibilityFailureV1


PINNED_SOURCE_ROOT = Path(__file__).resolve().parent / "pinned_source"
_SOURCE_MANIFEST = "source_manifest.json"
_MAX_ERROR_TEXT = 2048
_PINNED_SOURCE_PATHS = frozenset(
    {
        "vnpy_algotrading/base.py",
        "vnpy_algotrading/engine.py",
        "vnpy_algotrading/template.py",
        "vnpy_algotrading/algos/sniper_algo.py",
        "vnpy_algotrading/algos/best_limit_algo.py",
        "vnpy_algotrading/algos/twap_algo.py",
    }
)


def _validate_relative_path(value: str, *, field_name: str) -> str:
    path = PurePosixPath(value)
    windows_path = PureWindowsPath(value)
    if (
        path.is_absolute()
        or windows_path.is_absolute()
        or bool(windows_path.drive)
        or "\\" in value
        or value != path.as_posix()
        or any(part in ("", ".", "..") for part in path.parts)
    ):
        raise ValueError(f"{field_name} must be a normalized relative POSIX path")
    return value


class PinnedSourceFileV1(FrozenStrictModel):
    path: IdentityV1
    sha256: Sha256V1
    size_bytes: NonNegativeIntV1

    @model_validator(mode="after")
    def _validate_path(self) -> Self:
        _validate_relative_path(self.path, field_name="source file path")
        return self


class PinnedSourceArtifactV1(FrozenStrictModel):
    path: IdentityV1
    sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_path(self) -> Self:
        _validate_relative_path(self.path, field_name="source artifact path")
        return self


class PinnedSourceManifestV1(FrozenStrictModel):
    schema_version: Literal["vnpy_pinned_source_manifest_v1"]
    upstream_repo: IdentityV1
    upstream_commit: IdentityV1
    files: tuple[PinnedSourceFileV1, ...]
    license_file: PinnedSourceArtifactV1
    characterization_file: PinnedSourceArtifactV1
    license: IdentityV1
    copyright: IdentityV1
    source_manifest_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_closure(self) -> Self:
        if len(self.upstream_commit) != 40 or any(
            character not in "0123456789abcdef" for character in self.upstream_commit
        ):
            raise ValueError("upstream_commit must be a lowercase 40-character git sha")
        if not self.files:
            raise ValueError("source manifest files must not be empty")
        paths = tuple(item.path for item in self.files)
        if len(paths) != len(set(paths)):
            raise ValueError("source manifest file paths must be unique")
        if set(paths) != _PINNED_SOURCE_PATHS:
            raise ValueError("source manifest must contain the exact pinned six-file path set")
        ordered = tuple(sorted(self.files, key=lambda item: item.path))
        object.__setattr__(self, "files", ordered)
        if self.license_file.path != "LICENSE" or self.characterization_file.path != "surface_contract.json":
            raise ValueError("source manifest artifact paths must use the pinned authority names")
        artifact_paths = {self.license_file.path, self.characterization_file.path}
        if artifact_paths.intersection(paths) or len(artifact_paths) != 2:
            raise ValueError("source artifact paths must be distinct from source files")
        expected = hash_hex_v1(
            "miniqmt_vnpy_pinned_source_manifest_v1",
            self.canonical_payload_v1(exclude={"source_manifest_sha256"}),
        )
        if self.source_manifest_sha256 != expected:
            raise ValueError("source manifest hash mismatch")
        return self


class VnpySurfaceContractV1(FrozenStrictModel):
    schema_version: Literal["vnpy_surface_contract_v1"]
    required_method_signatures: tuple[VnpyMethodRequirementV1, ...]
    required_object_fields: tuple[ObjectFieldRequirementV1, ...]
    required_enum_values: tuple[EnumValueRequirementV1, ...]
    surface_contract_sha256: Sha256V1

    @model_validator(mode="after")
    def _validate_closure(self) -> Self:
        methods = tuple(
            sorted(self.required_method_signatures, key=lambda item: (item.source_path, item.owner, item.name))
        )
        objects = tuple(sorted(self.required_object_fields, key=lambda item: (item.object_name, item.source_path)))
        enums = tuple(sorted(self.required_enum_values, key=lambda item: (item.enum_name, item.source_path)))
        for field_name, values, key in (
            ("required_method_signatures", methods, lambda item: (item.source_path, item.owner, item.name)),
            ("required_object_fields", objects, lambda item: (item.object_name, item.source_path)),
            ("required_enum_values", enums, lambda item: (item.enum_name, item.source_path)),
        ):
            keys = tuple(key(item) for item in values)
            if not values or len(keys) != len(set(keys)):
                raise ValueError(f"{field_name} must be non-empty with unique identities")
            object.__setattr__(self, field_name, values)
        expected = hash_hex_v1(
            "miniqmt_vnpy_surface_contract_v1",
            self.canonical_payload_v1(exclude={"surface_contract_sha256"}),
        )
        if self.surface_contract_sha256 != expected:
            raise ValueError("surface contract hash mismatch")
        return self


class LockedSurfaceV1(FrozenStrictModel):
    schema_version: Literal["vnpy_locked_surface_v1"] = "vnpy_locked_surface_v1"
    source_manifest_sha256: Sha256V1
    requirement_sha256: Sha256V1
    source_lock_sha256: Sha256V1
    method_signature_sha256: Sha256V1
    object_field_sha256: Sha256V1
    characterization_sha256: Sha256V1
    surface_sha256: Sha256V1
    ordered_failures: tuple[VnpyCompatibilityFailureV1, ...]

    @model_validator(mode="after")
    def _validate_order(self) -> Self:
        expected = tuple(sorted(self.ordered_failures, key=lambda item: item.sort_key_v1()))
        if self.ordered_failures != expected:
            raise ValueError("locked surface failures must be canonically sorted")
        return self


def _safe_error(exc: Exception, *, source_root: Path | None = None) -> dict[str, str]:
    try:
        message = str(exc)[:_MAX_ERROR_TEXT]
        if source_root is not None:
            # OSError messages include the absolute checkout path; receipts must
            # remain byte-stable when the same authority is read from another
            # worktree or fresh process.
            root = source_root.resolve()
            for spelling in (str(root), str(root).replace("\\", "\\\\"), root.as_posix()):
                message = message.replace(spelling, "<source_root>")
        render_error_type = "none"
    except Exception as render_error:  # pragma: no branch - defensive renderer boundary
        message = "<unavailable>"
        render_error_type = f"{type(render_error).__module__}.{type(render_error).__qualname__}"
    return {
        "error_type": f"{type(exc).__module__}.{type(exc).__qualname__}",
        "message": message,
        "message_render_error_type": render_error_type,
    }


def _failure(field_path: str, reason_code: str, context: Any) -> VnpyCompatibilityFailureV1:
    return VnpyCompatibilityFailureV1.create(field_path=field_path, reason_code=reason_code, context=context)


def load_pinned_source_manifest_v1(source_root: Path = PINNED_SOURCE_ROOT) -> PinnedSourceManifestV1:
    """Strictly load the repo-owned source manifest without reading external state."""

    if not isinstance(source_root, Path):
        raise TypeError("source_root must be pathlib.Path")
    path = source_root / _SOURCE_MANIFEST
    return PinnedSourceManifestV1.model_validate_json(path.read_bytes(), strict=True)


def _load_manifest(
    source_root: Path,
    failures: list[VnpyCompatibilityFailureV1],
) -> PinnedSourceManifestV1 | None:
    try:
        return load_pinned_source_manifest_v1(source_root)
    except (OSError, UnicodeError, ValueError, TypeError, ValidationError) as exc:
        failures.append(
            _failure(
                "source_manifest",
                "MINIQMT_VNPY_COMPAT_SOURCE_MANIFEST_INVALID",
                {"path": _SOURCE_MANIFEST, **_safe_error(exc, source_root=source_root)},
            )
        )
        return None


def _read_surface_contract(
    source_root: Path,
    manifest: PinnedSourceManifestV1,
    failures: list[VnpyCompatibilityFailureV1],
) -> VnpySurfaceContractV1 | None:
    path = source_root / manifest.characterization_file.path
    try:
        content = path.read_bytes()
    except OSError as exc:
        failures.append(
            _failure(
                "characterization_file",
                "MINIQMT_VNPY_COMPAT_CHARACTERIZATION_MISSING",
                {
                    "path": manifest.characterization_file.path,
                    **_safe_error(exc, source_root=source_root),
                },
            )
        )
        return None
    actual_hash = hashlib.sha256(content).hexdigest()
    if actual_hash != manifest.characterization_file.sha256:
        failures.append(
            _failure(
                "characterization_file.sha256",
                "MINIQMT_VNPY_COMPAT_CHARACTERIZATION_HASH_DRIFT",
                {"expected": manifest.characterization_file.sha256, "actual": actual_hash},
            )
        )
    try:
        return VnpySurfaceContractV1.model_validate_json(content, strict=True)
    except (ValueError, TypeError, ValidationError) as exc:
        failures.append(
            _failure(
                "characterization_file",
                "MINIQMT_VNPY_COMPAT_CHARACTERIZATION_INVALID",
                _safe_error(exc),
            )
        )
        return None


def _read_sources(
    source_root: Path,
    manifest: PinnedSourceManifestV1,
    failures: list[VnpyCompatibilityFailureV1],
) -> dict[str, ast.Module]:
    trees: dict[str, ast.Module] = {}
    declared = {item.path: item for item in manifest.files}
    actual = {path.relative_to(source_root).as_posix() for path in source_root.rglob("*") if path.is_file()}
    allowed_artifacts = {manifest.license_file.path, manifest.characterization_file.path, _SOURCE_MANIFEST}
    expected_paths = set(declared) | allowed_artifacts
    for extra in sorted(actual - expected_paths):
        failures.append(
            _failure(
                f"source_files[{extra}]",
                "MINIQMT_VNPY_COMPAT_SOURCE_EXTRA",
                {"path": extra},
            )
        )
    for path_name, entry in declared.items():
        path = source_root / path_name
        try:
            content = path.read_bytes()
        except OSError as exc:
            failures.append(
                _failure(
                    f"source_files[{path_name}]",
                    "MINIQMT_VNPY_COMPAT_SOURCE_MISSING",
                    {"path": path_name, **_safe_error(exc, source_root=source_root)},
                )
            )
            continue
        actual_hash = hashlib.sha256(content).hexdigest()
        if actual_hash != entry.sha256:
            failures.append(
                _failure(
                    f"source_files[{path_name}].sha256",
                    "MINIQMT_VNPY_COMPAT_SOURCE_HASH_DRIFT",
                    {"path": path_name, "expected": entry.sha256, "actual": actual_hash},
                )
            )
        if len(content) != entry.size_bytes:
            failures.append(
                _failure(
                    f"source_files[{path_name}].size_bytes",
                    "MINIQMT_VNPY_COMPAT_SOURCE_SIZE_DRIFT",
                    {"path": path_name, "expected": entry.size_bytes, "actual": len(content)},
                )
            )
        try:
            text = content.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            failures.append(
                _failure(
                    f"source_files[{path_name}]",
                    "MINIQMT_VNPY_COMPAT_SOURCE_DECODE_INVALID",
                    {"path": path_name, "start": exc.start, "end": exc.end},
                )
            )
            continue
        try:
            trees[path_name] = ast.parse(text, filename=path_name, mode="exec", type_comments=True)
        except SyntaxError as exc:
            failures.append(
                _failure(
                    f"source_files[{path_name}]",
                    "MINIQMT_VNPY_COMPAT_SOURCE_AST_INVALID",
                    {"path": path_name, "line": exc.lineno or 0, "column": exc.offset or 0, "message": exc.msg},
                )
            )
    return trees


def _annotation(node: ast.expr | None) -> str:
    return "MISSING" if node is None else ast.unparse(node)


def _default_value(node: ast.expr | None) -> Any:
    if node is None:
        return None
    if isinstance(node, ast.Constant) and node.value is None:
        return None
    if isinstance(node, ast.Constant) and type(node.value) in (bool, int, str):
        return node.value
    return {"expression": ast.unparse(node)}


def _extract_parameters(node: ast.FunctionDef | ast.AsyncFunctionDef) -> tuple[VnpyParameterRequirementV1, ...]:
    positional = [*node.args.posonlyargs, *node.args.args]
    defaults: list[ast.expr | None] = [None] * (len(positional) - len(node.args.defaults)) + list(node.args.defaults)
    extracted: list[VnpyParameterRequirementV1] = []
    for index, (argument, default) in enumerate(zip(positional, defaults, strict=True)):
        if index == 0 and argument.arg in ("self", "cls"):
            continue
        kind = (
            VnpyParameterKindV1.POSITIONAL_ONLY
            if index < len(node.args.posonlyargs)
            else VnpyParameterKindV1.POSITIONAL_OR_KEYWORD
        )
        extracted.append(
            VnpyParameterRequirementV1(
                name=argument.arg,
                kind=kind,
                required=default is None,
                default_present=default is not None,
                default_value=_default_value(default),
                annotation=_annotation(argument.annotation),
            )
        )
    if node.args.vararg is not None:
        extracted.append(
            VnpyParameterRequirementV1(
                name=node.args.vararg.arg,
                kind=VnpyParameterKindV1.VAR_POSITIONAL,
                required=False,
                default_present=False,
                default_value=None,
                annotation=_annotation(node.args.vararg.annotation),
            )
        )
    for argument, default in zip(node.args.kwonlyargs, node.args.kw_defaults, strict=True):
        extracted.append(
            VnpyParameterRequirementV1(
                name=argument.arg,
                kind=VnpyParameterKindV1.KEYWORD_ONLY,
                required=default is None,
                default_present=default is not None,
                default_value=_default_value(default),
                annotation=_annotation(argument.annotation),
            )
        )
    if node.args.kwarg is not None:
        extracted.append(
            VnpyParameterRequirementV1(
                name=node.args.kwarg.arg,
                kind=VnpyParameterKindV1.VAR_KEYWORD,
                required=False,
                default_present=False,
                default_value=None,
                annotation=_annotation(node.args.kwarg.annotation),
            )
        )
    return tuple(extracted)


def _validate_method_surface(
    requirement: VnpyCompatibilityRequirementV1,
    trees: dict[str, ast.Module],
    failures: list[VnpyCompatibilityFailureV1],
) -> None:
    for method in requirement.required_method_signatures:
        tree = trees.get(method.source_path)
        if tree is None:
            continue
        owner = next(
            (item for item in tree.body if isinstance(item, ast.ClassDef) and item.name == method.owner),
            None,
        )
        function = (
            next(
                (
                    item
                    for item in owner.body
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == method.name
                ),
                None,
            )
            if owner is not None
            else None
        )
        field_path = f"required_method_signatures[{method.owner}.{method.name}]"
        if function is None:
            failures.append(
                _failure(
                    field_path,
                    "MINIQMT_VNPY_COMPAT_METHOD_MISSING",
                    {"source_path": method.source_path, "owner": method.owner, "method": method.name},
                )
            )
            continue
        actual_parameters = _extract_parameters(function)
        expected_parameters = tuple(item.canonical_payload_v1() for item in method.parameters)
        actual_payload = tuple(item.canonical_payload_v1() for item in actual_parameters)
        actual_return = _annotation(function.returns)
        if actual_payload != expected_parameters or actual_return != method.return_annotation:
            failures.append(
                _failure(
                    field_path,
                    "MINIQMT_VNPY_COMPAT_METHOD_SIGNATURE_DRIFT",
                    {
                        "source_path": method.source_path,
                        "owner": method.owner,
                        "method": method.name,
                        "expected_parameters": expected_parameters,
                        "actual_parameters": actual_payload,
                        "expected_return": method.return_annotation,
                        "actual_return": actual_return,
                    },
                )
            )


def _compare_surface_contract(
    requirement: VnpyCompatibilityRequirementV1,
    contract: VnpySurfaceContractV1,
    failures: list[VnpyCompatibilityFailureV1],
) -> None:
    expected_methods = tuple(item.canonical_payload_v1() for item in requirement.required_method_signatures)
    actual_methods = tuple(item.canonical_payload_v1() for item in contract.required_method_signatures)
    if actual_methods != expected_methods:
        failures.append(
            _failure(
                "required_method_signatures",
                "MINIQMT_VNPY_COMPAT_METHOD_SIGNATURE_DRIFT",
                {"expected": expected_methods, "actual": actual_methods},
            )
        )
    expected_objects = tuple(item.canonical_payload_v1() for item in requirement.required_object_fields)
    actual_objects = tuple(item.canonical_payload_v1() for item in contract.required_object_fields)
    if actual_objects != expected_objects:
        failures.append(
            _failure(
                "required_object_fields",
                "MINIQMT_VNPY_COMPAT_OBJECT_FIELD_DRIFT",
                {"expected": expected_objects, "actual": actual_objects},
            )
        )
    expected_enums = tuple(item.canonical_payload_v1() for item in requirement.required_enum_values)
    actual_enums = tuple(item.canonical_payload_v1() for item in contract.required_enum_values)
    if actual_enums != expected_enums:
        failures.append(
            _failure(
                "required_enum_values",
                "MINIQMT_VNPY_COMPAT_ENUM_VALUE_DRIFT",
                {"expected": expected_enums, "actual": actual_enums},
            )
        )


def extract_locked_surface_v1(
    *,
    requirement: VnpyCompatibilityRequirementV1,
    source_root: Path = PINNED_SOURCE_ROOT,
) -> LockedSurfaceV1:
    """Read and compare the offline source/surface without importing vn.py."""

    if type(requirement) is not VnpyCompatibilityRequirementV1 or not isinstance(source_root, Path):
        raise TypeError("requirement and source_root must use strict production types")
    components = compatibility_component_hashes_v1(requirement)
    failures: list[VnpyCompatibilityFailureV1] = []
    manifest = _load_manifest(source_root, failures)
    source_manifest_sha256 = "0" * 64
    if manifest is not None:
        source_manifest_sha256 = manifest.source_manifest_sha256
        if (
            manifest.upstream_repo != requirement.upstream_repo
            or manifest.upstream_commit != requirement.upstream_commit
        ):
            failures.append(
                _failure(
                    "source_manifest.upstream",
                    "MINIQMT_VNPY_COMPAT_SOURCE_IDENTITY_DRIFT",
                    {
                        "expected_repo": requirement.upstream_repo,
                        "actual_repo": manifest.upstream_repo,
                        "expected_commit": requirement.upstream_commit,
                        "actual_commit": manifest.upstream_commit,
                    },
                )
            )
        manifest_hashes = {item.path: item.sha256 for item in manifest.files}
        requirement_hashes = {item.path: item.sha256 for item in requirement.source_files_and_hashes}
        mismatched = {
            path: {"expected": digest, "actual": manifest_hashes.get(path)}
            for path, digest in requirement_hashes.items()
            if manifest_hashes.get(path) != digest
        }
        if mismatched:
            failures.append(
                _failure(
                    "source_files_and_hashes",
                    "MINIQMT_VNPY_COMPAT_REQUIREMENT_SOURCE_DRIFT",
                    mismatched,
                )
            )
        trees = _read_sources(source_root, manifest, failures)
        _validate_method_surface(requirement, trees, failures)
        contract = _read_surface_contract(source_root, manifest, failures)
        if contract is not None:
            _compare_surface_contract(requirement, contract, failures)
        license_path = source_root / manifest.license_file.path
        try:
            license_hash = hashlib.sha256(license_path.read_bytes()).hexdigest()
        except OSError as exc:
            failures.append(
                _failure(
                    "license_file",
                    "MINIQMT_VNPY_COMPAT_LICENSE_MISSING",
                    {"path": manifest.license_file.path, **_safe_error(exc, source_root=source_root)},
                )
            )
        else:
            if license_hash != manifest.license_file.sha256:
                failures.append(
                    _failure(
                        "license_file.sha256",
                        "MINIQMT_VNPY_COMPAT_LICENSE_HASH_DRIFT",
                        {"expected": manifest.license_file.sha256, "actual": license_hash},
                    )
                )
    ordered = tuple(sorted(failures, key=lambda item: item.sort_key_v1()))
    return LockedSurfaceV1(
        source_manifest_sha256=source_manifest_sha256,
        requirement_sha256=requirement.requirement_sha256,
        source_lock_sha256=components["source_lock_sha256"],
        method_signature_sha256=components["method_signature_sha256"],
        object_field_sha256=components["object_field_sha256"],
        characterization_sha256=components["characterization_sha256"],
        surface_sha256=components["surface_sha256"],
        ordered_failures=ordered,
    )


validate_pinned_source_v1 = extract_locked_surface_v1


__all__ = [
    "PINNED_SOURCE_ROOT",
    "LockedSurfaceV1",
    "PinnedSourceArtifactV1",
    "PinnedSourceFileV1",
    "PinnedSourceManifestV1",
    "VnpySurfaceContractV1",
    "extract_locked_surface_v1",
    "load_pinned_source_manifest_v1",
    "validate_pinned_source_v1",
]
