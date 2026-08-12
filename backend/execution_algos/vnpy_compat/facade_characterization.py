"""K4 pinned-source authority and deterministic characterization primitives.

This module is deliberately broker-, repository-, clock-, random- and
network-free. It validates only repository-owned source bytes and returns
immutable contract carriers.
"""

from __future__ import annotations

import ast
import builtins
import hashlib
import inspect
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any

from .facade_contracts import (
    VnpyFacadeAlgorithmBindingV1,
    VnpyFacadeAlgorithmBindingV2,
    VnpyFacadeAlgorithmCharacterizationReceiptV1,
    VnpyFacadeAlgorithmCharacterizationReceiptV2,
    VnpyFacadeCharacterizationRequirementV1,
    VnpyFacadeCharacterizationManifestViewV1,
    VnpyFacadeCharacterizationVectorV1,
    VnpyFacadeCharacterizationVectorV2,
    VnpyFacadeCharacterizationVectorArtifactV2,
    VnpyFacadeCompatibilityStatusV1,
    VnpyFacadeConformanceFailureV1,
    VnpyFacadeConformanceAuthorityV2,
    VnpyFacadeConformanceAuthorityValidationReceiptV2,
    VnpyFacadeConformanceBuildItemV2,
    VnpyFacadeConformanceReceiptV2,
    VnpyFacadeConformanceSetV2,
    VnpyFacadeCommandAuthorityDispositionV1,
    VnpyFacadeConformanceSetV1,
    VnpyFacadeContractV1,
    VnpyFacadeConstructorDispositionV1,
    VnpyFacadeContractError,
    VnpyFacadeDeterministicInputsV1,
    VnpyFacadeFieldRoleV1,
    VnpyFacadeIsolatedBindingOwnerV1,
    VnpyFacadeIsolatedModuleBindingV1,
    VnpyFacadeImplementationBindingV1,
    VnpyFacadeMethodContractV1,
    VnpyFacadeRegistrationDispositionV1,
    VnpyFacadeRuntimeBindingDispositionV1,
    VnpyFacadeSourceManifestV1,
    VnpyFacadeSourceRoleV1,
    VnpyFacadeSourceV1,
    VnpyFacadeSourceExecutionSetV1,
    VnpyFacadeSourceExecutorBindingV1,
    VnpyFacadeStateFieldMappingV1,
    VnpyFacadeTerminalMappingV1,
    _seal_vnpy_facade_conformance_authority_v2,
)
from .facade_adapter import (
    VnpyFacadeBackedPluginAdapterV1,
    state_mapping_set_sha256_v1,
    terminal_mapping_set_sha256_v1,
)
from .facade_projection import (
    AlgoStatus,
    ContractData,
    Direction,
    Offset,
    OrderData,
    OrderType,
    TickData,
    TradeData,
    build_pinned_round_to_v1,
    build_vnpy_facade_dto_mappings_v1,
    dto_mapping_set_sha256_v1,
    project_order_status_v1,
)
from .locked_surface import PINNED_SOURCE_ROOT
from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    hash_hex_v1,
    json_safe_evidence_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    CompatibilityStatusV1,
    ExecutionAlgoPluginV2,
    PluginCatalogRuntimeV2,
    PluginRouteCompatibilityReceiptV1,
    callable_ref_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    VnpyCompatibilityRequirementV2,
    GatewayCapabilityCatalogV1,
    bounded_exception_summary_v1,
    compatibility_component_hashes_v2,
)

_ALGOTRADING_REPO = "https://github.com/vnpy/vnpy_algotrading"
_ALGOTRADING_COMMIT = "4133987530eb28f3538d1983545d81c4f83d7d59"
_ALGOTRADING_AUTHORITY = "ad72ed1dd243d45d41d3c476d9dd7fbf17f49e6efcb7c12739f8ae6982582541"
_CORE_REPO = "https://github.com/vnpy/vnpy"
_CORE_COMMIT = "1049acf64afd5b2d06d09b1e139dd0cca5d9d6b9"
_CORE_AUTHORITY = "8e73ba64d3d405c382ae3b8b8d1c2df334c809b3331828fa4d85b0d62ed00ad2"
_VECTOR_ARTIFACT_REPO_PATH = (
    "backend/execution_algos/vnpy_compat/characterization_artifacts/facade_characterization_vectors_v2.json"
)
_VECTOR_ARTIFACT_PATH = (
    Path(__file__).resolve().parent / "characterization_artifacts" / ("facade_characterization_vectors_v2.json")
)
_AST_DUMP_SUPPORTS_SHOW_EMPTY = "show_empty" in inspect.signature(ast.dump).parameters


def _stable_ast_dump_v1(node: ast.AST, *, annotate_fields: bool = True) -> str:
    """Render one AST with the Python 3.12 full-field shape on every runtime."""

    kwargs: dict[str, Any] = {
        "annotate_fields": annotate_fields,
        "include_attributes": False,
    }
    if _AST_DUMP_SUPPORTS_SHOW_EMPTY:
        kwargs["show_empty"] = True
    return ast.dump(node, **kwargs)


@dataclass(frozen=True, slots=True)
class VnpyFacadeCharacterizationArtifactAuthorityV2:
    artifact: VnpyFacadeCharacterizationVectorArtifactV2
    canonical_lf_file_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.artifact, VnpyFacadeCharacterizationVectorArtifactV2):
            raise TypeError("artifact must be VnpyFacadeCharacterizationVectorArtifactV2")
        if len(self.canonical_lf_file_sha256) != 64 or any(
            item not in "0123456789abcdef" for item in self.canonical_lf_file_sha256
        ):
            raise ValueError("canonical_lf_file_sha256 must be lowercase SHA-256")


def _strict_json_object_v1(payload: str) -> dict[str, Any]:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key {key}")
            result[key] = value
        return result

    parsed = json.loads(
        payload,
        object_pairs_hook=object_pairs,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"invalid JSON constant {value}")),
    )
    if type(parsed) is not dict:
        raise TypeError("characterization vector artifact root must be an object")
    return parsed


def readback_vnpy_facade_characterization_vector_artifact_v2(
    *, artifact_path: Path = _VECTOR_ARTIFACT_PATH
) -> VnpyFacadeCharacterizationArtifactAuthorityV2:
    """Strict-read the single repository-owned K4-B vector authority."""

    if not isinstance(artifact_path, Path):
        raise TypeError("artifact_path must be pathlib.Path")
    if not artifact_path.is_file():
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            "repository-owned characterization vector artifact is missing",
            context={"source_path": _VECTOR_ARTIFACT_REPO_PATH},
        )
    payload = artifact_path.read_bytes()
    canonical_lf = payload.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    try:
        _strict_json_object_v1(canonical_lf.decode("utf-8", errors="strict"))
        artifact = VnpyFacadeCharacterizationVectorArtifactV2.model_validate_json(canonical_lf, strict=True)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            "repository-owned characterization vector artifact failed strict readback",
            context={
                "source_path": _VECTOR_ARTIFACT_REPO_PATH,
                **_safe_exception_evidence_v1(exc),
            },
        ) from exc
    return VnpyFacadeCharacterizationArtifactAuthorityV2(
        artifact=artifact,
        canonical_lf_file_sha256=hashlib.sha256(canonical_lf).hexdigest(),
    )


@dataclass(frozen=True, slots=True)
class _SourceSpec:
    role: VnpyFacadeSourceRoleV1
    name: str
    namespace: str
    repo: str
    commit: str
    path: str
    size: int
    sha256: str
    disposition: VnpyFacadeRegistrationDispositionV1
    class_or_function: str


_SPECS = (
    _SourceSpec(
        VnpyFacadeSourceRoleV1.ALGORITHM,
        "BEST_LIMIT_MINIQMT",
        "VNPY_ALGOTRADING",
        _ALGOTRADING_REPO,
        _ALGOTRADING_COMMIT,
        "vnpy_algotrading/algos/best_limit_algo.py",
        3560,
        "b35227b932a160c2f786d3202283b61656d9f16631fb42f596a9d376765617e9",
        VnpyFacadeRegistrationDispositionV1.REGISTERED_CURRENT_THREE,
        "BestLimitAlgo",
    ),
    _SourceSpec(
        VnpyFacadeSourceRoleV1.ALGORITHM,
        "ICEBERG",
        "VNPY_ALGOTRADING",
        _ALGOTRADING_REPO,
        _ALGOTRADING_COMMIT,
        "vnpy_algotrading/algos/iceberg_algo.py",
        3228,
        "9019cd20e4288b1642f7bc5f1508244eb9ccb419a2a888f69040fd9c5c6a2c21",
        VnpyFacadeRegistrationDispositionV1.CHARACTERIZATION_ONLY_K5,
        "IcebergAlgo",
    ),
    _SourceSpec(
        VnpyFacadeSourceRoleV1.ALGORITHM,
        "SNIPER_MINIQMT",
        "VNPY_ALGOTRADING",
        _ALGOTRADING_REPO,
        _ALGOTRADING_COMMIT,
        "vnpy_algotrading/algos/sniper_algo.py",
        2186,
        "fbf84d2c61f8200079fe1f8da3b3412a036e5a7ffb6c601f9e4614ad110c8c76",
        VnpyFacadeRegistrationDispositionV1.REGISTERED_CURRENT_THREE,
        "SniperAlgo",
    ),
    _SourceSpec(
        VnpyFacadeSourceRoleV1.ALGORITHM,
        "STOP",
        "VNPY_ALGOTRADING",
        _ALGOTRADING_REPO,
        _ALGOTRADING_COMMIT,
        "vnpy_algotrading/algos/stop_algo.py",
        2631,
        "18a758b2d86b0704b00ce385f3517061e21dee57178c3abfd10271091e8db090",
        VnpyFacadeRegistrationDispositionV1.CHARACTERIZATION_ONLY_K5,
        "StopAlgo",
    ),
    _SourceSpec(
        VnpyFacadeSourceRoleV1.ALGORITHM,
        "TWAP_LITE_MINIQMT",
        "VNPY_ALGOTRADING",
        _ALGOTRADING_REPO,
        _ALGOTRADING_COMMIT,
        "vnpy_algotrading/algos/twap_algo.py",
        2532,
        "aeabb067ef79d48182f357b8d4736f8a90f6a4ecb77bc82506a3244575a6cd0f",
        VnpyFacadeRegistrationDispositionV1.REGISTERED_CURRENT_THREE,
        "TwapAlgo",
    ),
    _SourceSpec(
        VnpyFacadeSourceRoleV1.HELPER,
        "round_to",
        "VNPY_CORE",
        _CORE_REPO,
        _CORE_COMMIT,
        "vnpy_core/vnpy/trader/utility.py",
        32957,
        "9bce3f6e18c84668b0ffadd717f0b6fd4ca2b454dc748dad6572af78c850608d",
        VnpyFacadeRegistrationDispositionV1.FACADE_HELPER_ONLY,
        "round_to",
    ),
)


def _source_error(message: str, **context: Any) -> VnpyFacadeContractError:
    return VnpyFacadeContractError("MINIQMT_VNPY_FACADE_SOURCE_INVALID", message, context=context)


def _safe_exception_evidence_v1(exc: Exception) -> dict[str, Any]:
    repository_root = Path(__file__).resolve().parents[3]
    summary = bounded_exception_summary_v1(
        exc,
        redacted_values=(
            str(repository_root),
            str(repository_root).replace("\\", "\\\\"),
            repository_root.as_posix(),
        ),
    )
    render_error_type = summary.pop("renderer_error_type")
    return {
        **summary,
        "message_render_error_type": "none" if render_error_type is None else render_error_type,
    }


def _validated_source_root(source_root: Path) -> Path:
    if not isinstance(source_root, Path):
        raise TypeError("source_root must be pathlib.Path")
    if not source_root.is_dir():
        raise _source_error("pinned source root is missing", source_root=source_root.as_posix())
    return source_root


def _validated_source_bytes(source_root: Path, spec: _SourceSpec) -> bytes:
    pure = PurePosixPath(spec.path)
    if pure.is_absolute() or ".." in pure.parts or "\\" in spec.path or ":" in spec.path:
        raise _source_error("source specification path is unsafe", source_path=spec.path)
    path = source_root.joinpath(*pure.parts)
    if not path.is_file():
        raise _source_error("pinned source is missing", source_path=spec.path)
    payload = path.read_bytes()
    actual_hash = hashlib.sha256(payload).hexdigest()
    if len(payload) != spec.size or actual_hash != spec.sha256:
        raise _source_error(
            "pinned source bytes drifted",
            source_path=spec.path,
            expected_size=spec.size,
            actual_size=len(payload),
            expected_sha256=spec.sha256,
            actual_sha256=actual_hash,
        )
    try:
        tree = ast.parse(payload.decode("utf-8"), filename=spec.path)
    except (UnicodeDecodeError, SyntaxError) as exc:
        raise _source_error(
            "pinned source cannot be decoded and parsed",
            source_path=spec.path,
            **_safe_exception_evidence_v1(exc),
        ) from exc
    owner_type = ast.ClassDef if spec.role is VnpyFacadeSourceRoleV1.ALGORITHM else ast.FunctionDef
    matches = [item for item in tree.body if isinstance(item, owner_type) and item.name == spec.class_or_function]
    if len(matches) != 1:
        raise _source_error(
            "pinned source semantic owner is not unique",
            source_path=spec.path,
            owner=spec.class_or_function,
            owner_count=len(matches),
        )
    return payload


def build_vnpy_facade_source_manifest_v1(*, source_root: Path = PINNED_SOURCE_ROOT) -> VnpyFacadeSourceManifestV1:
    """Rebuild the exact six-source K4 authority from repository-owned bytes."""

    root = _validated_source_root(source_root)
    sources: list[VnpyFacadeSourceV1] = []
    for spec in _SPECS:
        _validated_source_bytes(root, spec)
        sources.append(
            VnpyFacadeSourceV1.create(
                source_role=spec.role,
                algo_code_or_helper_name=spec.name,
                upstream_namespace=spec.namespace,
                upstream_repo=spec.repo,
                upstream_commit=spec.commit,
                source_path=spec.path,
                source_size=spec.size,
                source_sha256=spec.sha256,
                registration_disposition=spec.disposition,
            )
        )
    return VnpyFacadeSourceManifestV1.create(
        upstream_authority_sha256=(_ALGOTRADING_AUTHORITY, _CORE_AUTHORITY),
        sources=tuple(sources),
    )


def readback_vnpy_facade_source_manifest_v1(
    payload: Any,
    *,
    source_root: Path = PINNED_SOURCE_ROOT,
) -> VnpyFacadeSourceManifestV1:
    """Strict-read a carrier and compare it with the live byte authority."""

    supplied = VnpyFacadeSourceManifestV1.model_validate(payload, strict=True)
    authoritative = build_vnpy_facade_source_manifest_v1(source_root=source_root)
    if supplied != authoritative:
        raise _source_error(
            "facade source manifest does not match repository-owned authority",
            expected_manifest_sha256=authoritative.manifest_sha256,
            actual_manifest_sha256=supplied.manifest_sha256,
        )
    return authoritative


class VnpyFacadeDeterministicUniformV1:
    """Consume exact u53 inputs without reading process-global random state."""

    __slots__ = ("_draws", "_next", "_trace")

    def __init__(self, inputs: VnpyFacadeDeterministicInputsV1) -> None:
        if not isinstance(inputs, VnpyFacadeDeterministicInputsV1):
            raise TypeError("inputs must be VnpyFacadeDeterministicInputsV1")
        self._draws = inputs.ordered_uniform_draws
        self._next = 0
        self._trace: list[dict[str, Any]] = []

    def __call__(self, a: float, b: float) -> float:
        if type(a) not in (int, float) or type(b) not in (int, float):
            raise TypeError("uniform bounds must be strict numeric values and not bool")
        lower = float(a)
        upper = float(b)
        if not math.isfinite(lower) or not math.isfinite(upper):
            raise ValueError("uniform bounds must be finite")
        if self._next >= len(self._draws):
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_DETERMINISTIC_INPUT_INVALID",
                "uniform call has no corresponding exact u53 draw",
                context={"ordinal": self._next, "lower": lower, "upper": upper},
            )
        draw = self._draws[self._next]
        if draw.ordinal != self._next:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_DETERMINISTIC_INPUT_INVALID",
                "uniform draw ordinal drifted",
                context={"expected": self._next, "actual": draw.ordinal},
            )
        u = draw.u53_integer / 2**53
        result = lower + (upper - lower) * u
        self._trace.append(
            {
                "ordinal": self._next,
                "lower": lower,
                "upper": upper,
                "u53_integer": draw.u53_integer,
                "result": result,
            }
        )
        self._next += 1
        return result

    def freeze_trace_v1(self) -> tuple[dict[str, Any], ...]:
        if self._next != len(self._draws):
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_DETERMINISTIC_INPUT_INVALID",
                "characterization left deterministic draws unconsumed",
                context={"consumed": self._next, "supplied": len(self._draws)},
            )
        return tuple(dict(item) for item in self._trace)

    def consumed_inputs_v1(self) -> VnpyFacadeDeterministicInputsV1:
        """Return only draws that were actually consumed by the source execution."""

        return VnpyFacadeDeterministicInputsV1.create(ordered_uniform_draws=tuple(self._draws[: self._next]))


def _load_class_source_v1(
    *,
    source_root: Path,
    relative_path: str,
    module_name: str,
    bindings: dict[str, Any],
) -> dict[str, Any]:
    source = source_root.joinpath(*PurePosixPath(relative_path).parts).read_text(encoding="utf-8")
    tree = ast.parse(source, filename=relative_path)
    import_nodes = [item for item in tree.body if isinstance(item, (ast.Import, ast.ImportFrom))]
    allowed_roots = {"typing", "enum", "random", "vnpy", "vnpy_algotrading"}
    for node in import_nodes:
        root = (
            node.names[0].name.split(".", 1)[0]
            if isinstance(node, ast.Import)
            else ("vnpy_algotrading" if node.level > 0 else (node.module or "vnpy_algotrading").split(".", 1)[0])
        )
        if root not in allowed_roots:
            raise _source_error(
                "pinned source requests an unsupported isolated import",
                source_path=relative_path,
                import_root=root,
            )
    isolated_tree = ast.Module(
        body=[item for item in tree.body if not isinstance(item, (ast.Import, ast.ImportFrom))],
        type_ignores=[],
    )
    ast.fix_missing_locations(isolated_tree)
    safe_builtins = dict(vars(builtins))
    safe_builtins.pop("__import__", None)
    namespace = {"__name__": module_name, "__builtins__": safe_builtins, **bindings}
    exec(compile(isolated_tree, filename=f"<pinned:{relative_path}>", mode="exec"), namespace)  # noqa: S102
    return namespace


def load_pinned_vnpy_algorithm_classes_v1(
    *,
    source_root: Path = PINNED_SOURCE_ROOT,
    deterministic_uniform: VnpyFacadeDeterministicUniformV1 | None = None,
) -> dict[str, type[Any]]:
    """Execute exact class bodies in a closed namespace without sys.modules writes."""

    build_vnpy_facade_source_manifest_v1(source_root=source_root)
    _load_class_source_v1(
        source_root=source_root,
        relative_path="vnpy_algotrading/base.py",
        module_name="vnpy_algotrading.base",
        bindings={"Enum": Enum},
    )
    template = _load_class_source_v1(
        source_root=source_root,
        relative_path="vnpy_algotrading/template.py",
        module_name="vnpy_algotrading.template",
        bindings={
            "TYPE_CHECKING": False,
            "BaseEngine": object,
            "TickData": TickData,
            "OrderData": OrderData,
            "TradeData": TradeData,
            "ContractData": ContractData,
            "OrderType": OrderType,
            "Offset": Offset,
            "Direction": Direction,
            "AlgoStatus": AlgoStatus,
        },
    )
    common = {
        "BaseEngine": object,
        "AlgoTemplate": template["AlgoTemplate"],
        "TickData": TickData,
        "OrderData": OrderData,
        "TradeData": TradeData,
        "ContractData": ContractData,
        "Direction": Direction,
        "Offset": Offset,
        "OrderType": OrderType,
        "round_to": build_pinned_round_to_v1(source_root),
    }
    class_names = {
        "BEST_LIMIT_MINIQMT": ("BestLimitAlgo", "best_limit_algo.py"),
        "ICEBERG": ("IcebergAlgo", "iceberg_algo.py"),
        "SNIPER_MINIQMT": ("SniperAlgo", "sniper_algo.py"),
        "STOP": ("StopAlgo", "stop_algo.py"),
        "TWAP_LITE_MINIQMT": ("TwapAlgo", "twap_algo.py"),
    }
    result: dict[str, type[Any]] = {}
    for algo_code, (class_name, filename) in class_names.items():
        bindings = dict(common)
        if algo_code == "BEST_LIMIT_MINIQMT":
            if deterministic_uniform is None:

                def missing_uniform(_a: float, _b: float) -> float:
                    raise VnpyFacadeContractError(
                        "MINIQMT_VNPY_FACADE_DETERMINISTIC_INPUT_INVALID",
                        "BestLimit characterization requires explicit u53 input",
                        context={"algo_code": algo_code},
                    )

                bindings["uniform"] = missing_uniform
            else:
                bindings["uniform"] = deterministic_uniform
        relative_path = f"vnpy_algotrading/algos/{filename}"
        namespace = _load_class_source_v1(
            source_root=source_root,
            relative_path=relative_path,
            module_name=f"vnpy_algotrading.algos.{filename[:-3]}",
            bindings=bindings,
        )
        algorithm_class = namespace.get(class_name)
        if not isinstance(algorithm_class, type):
            raise _source_error(
                "pinned algorithm class was not produced by isolated load",
                algo_code=algo_code,
                class_name=class_name,
            )
        result[algo_code] = algorithm_class
    return result


def _literal_class_value_v1(class_node: ast.ClassDef, name: str) -> Any:
    matches = [
        item
        for item in class_node.body
        if isinstance(item, (ast.Assign, ast.AnnAssign))
        and (
            (
                isinstance(item, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == name for target in item.targets)
            )
            or (isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name) and item.target.id == name)
        )
    ]
    if len(matches) != 1:
        raise _source_error("algorithm class literal authority is not unique", class_name=class_node.name, field=name)
    return ast.literal_eval(matches[0].value)


def _constructor_assignments_v1(class_node: ast.ClassDef) -> dict[str, tuple[str, str]]:
    constructors = [item for item in class_node.body if isinstance(item, ast.FunctionDef) and item.name == "__init__"]
    if len(constructors) != 1:
        raise _source_error("algorithm constructor authority is not unique", class_name=class_node.name)
    result: dict[str, tuple[str, str]] = {}
    for node in ast.walk(constructors[0]):
        target: ast.Attribute | None = None
        annotation = "Any"
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Attribute):
            target = node.target
            annotation = ast.unparse(node.annotation)
        elif isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Attribute):
            target = node.targets[0]
        if target is not None and isinstance(target.value, ast.Name) and target.value.id == "self":
            expression = _stable_ast_dump_v1(node.value)
            existing = result.get(target.attr)
            if annotation != "Any":
                if existing is not None and existing[0] != "Any" and existing[0] != annotation:
                    raise _source_error(
                        "algorithm field carries conflicting constructor annotations",
                        class_name=class_node.name,
                        attribute_name=target.attr,
                        first_annotation=existing[0],
                        second_annotation=annotation,
                    )
                result[target.attr] = (annotation, expression)
            elif existing is None:
                result[target.attr] = (annotation, expression)
    return result


def build_vnpy_facade_state_mappings_v1(
    *,
    source_root: Path = PINNED_SOURCE_ROOT,
) -> tuple[VnpyFacadeStateFieldMappingV1, ...]:
    """Derive base/parameter/variable mapping identities from exact AST facts."""

    manifest = build_vnpy_facade_source_manifest_v1(source_root=source_root)
    sources = {item.algo_code_or_helper_name: item for item in manifest.ordered_sources}
    base_fields = (
        ("algo_engine", "object", VnpyFacadeConstructorDispositionV1.INITIALIZE_ONLY),
        ("algo_name", "str", VnpyFacadeConstructorDispositionV1.RESTORE_FROM_STATE),
        ("vt_symbol", "str", VnpyFacadeConstructorDispositionV1.RESTORE_FROM_STATE),
        ("direction", "Direction", VnpyFacadeConstructorDispositionV1.RESTORE_FROM_STATE),
        ("offset", "Offset", VnpyFacadeConstructorDispositionV1.RESTORE_FROM_STATE),
        ("price", "float", VnpyFacadeConstructorDispositionV1.RESTORE_FROM_STATE),
        ("volume", "float", VnpyFacadeConstructorDispositionV1.RESTORE_FROM_STATE),
        ("status", "AlgoStatus", VnpyFacadeConstructorDispositionV1.RESTORE_FROM_STATE),
        ("traded", "float", VnpyFacadeConstructorDispositionV1.RESTORE_FROM_STATE),
        ("traded_price", "float", VnpyFacadeConstructorDispositionV1.RESTORE_FROM_STATE),
        ("active_orders", "dict[str,OrderData]", VnpyFacadeConstructorDispositionV1.RESTORE_FROM_STATE),
    )
    filename_by_code = {
        "BEST_LIMIT_MINIQMT": "best_limit_algo.py",
        "ICEBERG": "iceberg_algo.py",
        "SNIPER_MINIQMT": "sniper_algo.py",
        "STOP": "stop_algo.py",
        "TWAP_LITE_MINIQMT": "twap_algo.py",
    }
    result: list[VnpyFacadeStateFieldMappingV1] = []
    for algo_code, filename in filename_by_code.items():
        path = source_root / "vnpy_algotrading" / "algos" / filename
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=filename)
        classes = [item for item in tree.body if isinstance(item, ast.ClassDef)]
        if len(classes) != 1:
            raise _source_error("algorithm source must contain one class", algo_code=algo_code)
        class_node = classes[0]
        parameters = tuple(_literal_class_value_v1(class_node, "default_setting").keys())
        variables = tuple(_literal_class_value_v1(class_node, "variables"))
        assignments = _constructor_assignments_v1(class_node)
        for attribute_name, value_type, disposition in base_fields:
            result.append(
                VnpyFacadeStateFieldMappingV1.create(
                    algo_code=algo_code,
                    source_identity_sha256=sources[algo_code].source_identity_sha256,
                    attribute_name=attribute_name,
                    state_path=f"base.{attribute_name}",
                    field_role=(
                        VnpyFacadeFieldRoleV1.ACTIVE_ORDER
                        if attribute_name == "active_orders"
                        else VnpyFacadeFieldRoleV1.BASE
                    ),
                    value_type=value_type,
                    nullable=False,
                    mutable_container_disposition=(
                        "REBUILT_FROM_DURABLE_MAPPING"
                        if attribute_name == "active_orders"
                        else "IMMUTABLE_SCALAR_OR_TRANSITION_LOCAL"
                    ),
                    constructor_disposition=disposition,
                )
            )
        for role, names in (
            (VnpyFacadeFieldRoleV1.PARAMETER, parameters),
            (VnpyFacadeFieldRoleV1.VARIABLE, variables),
        ):
            for name in names:
                if name not in assignments:
                    raise _source_error(
                        "mapped algorithm field lacks constructor assignment",
                        algo_code=algo_code,
                        attribute_name=name,
                    )
                value_type = assignments[name][0]
                result.append(
                    VnpyFacadeStateFieldMappingV1.create(
                        algo_code=algo_code,
                        source_identity_sha256=sources[algo_code].source_identity_sha256,
                        attribute_name=name,
                        state_path=f"{role.value.lower()}.{name}",
                        field_role=role,
                        value_type=value_type,
                        nullable=False,
                        mutable_container_disposition="IMMUTABLE_SCALAR",
                        constructor_disposition=VnpyFacadeConstructorDispositionV1.RESTORE_FROM_STATE,
                    )
                )
    return tuple(
        sorted(
            result,
            key=lambda item: (item.algo_code, item.field_role.value, item.attribute_name, item.state_path),
        )
    )


def readback_vnpy_facade_state_mappings_v1(
    payload: Any,
    *,
    source_root: Path = PINNED_SOURCE_ROOT,
) -> tuple[VnpyFacadeStateFieldMappingV1, ...]:
    if type(payload) not in (tuple, list):
        raise TypeError("state mapping payload must be a tuple or JSON list")
    supplied = tuple(VnpyFacadeStateFieldMappingV1.model_validate(item, strict=True) for item in payload)
    authoritative = build_vnpy_facade_state_mappings_v1(source_root=source_root)
    if supplied != authoritative:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_STATE_MAPPING_INVALID",
            "state mapping readback conflicts with pinned AST authority",
            context={
                "expected_set_sha256": state_mapping_set_sha256_v1(authoritative),
                "actual_set_sha256": state_mapping_set_sha256_v1(supplied),
            },
        )
    return authoritative


def build_vnpy_facade_terminal_mappings_v1(
    *,
    source_root: Path = PINNED_SOURCE_ROOT,
) -> tuple[VnpyFacadeTerminalMappingV1, ...]:
    """Build the exact source-proven terminal mapping set for K4 algorithms."""

    mappings: list[VnpyFacadeTerminalMappingV1] = []
    filename_by_code = {
        "BEST_LIMIT_MINIQMT": "best_limit_algo.py",
        "ICEBERG": "iceberg_algo.py",
        "SNIPER_MINIQMT": "sniper_algo.py",
        "STOP": "stop_algo.py",
        "TWAP_LITE_MINIQMT": "twap_algo.py",
    }
    for algo_code, filename in filename_by_code.items():
        source = (source_root / "vnpy_algotrading" / "algos" / filename).read_text(encoding="utf-8")
        tree = ast.parse(source, filename=filename)
        finish_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "self"
            and node.func.attr == "finish"
        ]
        if not finish_calls:
            raise _source_error("algorithm source has no terminal finish authority", algo_code=algo_code)
        mappings.append(
            VnpyFacadeTerminalMappingV1.create(
                algo_code=algo_code,
                algo_status_member="FINISHED",
                trigger_event_type="TRADE",
                traded_relation="FULL",
                required_active_child_closure="CLEAN",
                terminal_outcome_or_none="FILLED",
                reason_code="MINIQMT_VNPY_FACADE_FILLED",
            )
        )
        if algo_code == "TWAP_LITE_MINIQMT":
            mappings.append(
                VnpyFacadeTerminalMappingV1.create(
                    algo_code=algo_code,
                    algo_status_member="FINISHED",
                    trigger_event_type="TIMER",
                    traded_relation="RESIDUAL",
                    required_active_child_closure="CLEAN",
                    terminal_outcome_or_none="EXPIRED_WITH_RESIDUAL",
                    reason_code="MINIQMT_VNPY_FACADE_TWAP_DURATION_EXHAUSTED",
                )
            )
    return tuple(
        sorted(
            mappings,
            key=lambda item: (
                item.algo_code,
                item.algo_status_member,
                item.trigger_event_type,
                item.traded_relation,
                item.required_active_child_closure,
            ),
        )
    )


def readback_vnpy_facade_terminal_mappings_v1(
    payload: Any,
    *,
    source_root: Path = PINNED_SOURCE_ROOT,
) -> tuple[VnpyFacadeTerminalMappingV1, ...]:
    if type(payload) not in (tuple, list):
        raise TypeError("terminal mapping payload must be a tuple or JSON list")
    supplied = tuple(VnpyFacadeTerminalMappingV1.model_validate(item, strict=True) for item in payload)
    authoritative = build_vnpy_facade_terminal_mappings_v1(source_root=source_root)
    if supplied != authoritative:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_STATE_MAPPING_INVALID",
            "terminal mapping readback conflicts with pinned source authority",
            context={
                "expected_set_sha256": terminal_mapping_set_sha256_v1(authoritative),
                "actual_set_sha256": terminal_mapping_set_sha256_v1(supplied),
            },
        )
    return authoritative


def isolated_module_binding_set_sha256_v1(
    bindings: tuple[VnpyFacadeIsolatedModuleBindingV1, ...],
) -> str:
    ordered = tuple(sorted(bindings, key=lambda item: (item.module_name, item.export_name)))
    keys = tuple((item.module_name, item.export_name) for item in ordered)
    if bindings != ordered or len(keys) != len(set(keys)):
        raise ValueError("isolated module bindings must be unique and sorted")
    return hash_hex_v1(
        "miniqmt_vnpy_facade_isolated_module_binding_set_v1",
        [item.canonical_payload_v1() for item in ordered],
    )


def build_vnpy_facade_isolated_module_bindings_v1(
    *,
    implementation_binding_sha256_by_component: Mapping[str, str],
    source_root: Path = PINNED_SOURCE_ROOT,
) -> tuple[VnpyFacadeIsolatedModuleBindingV1, ...]:
    """Build the one closed source-isolated module graph approved by K4."""

    required_components = {
        "facade.create",
        "projection.tick",
        "projection.order",
        "projection.trade",
        "projection.contract",
        "projection.enum",
        "helper.round_to",
        "characterization.deterministic_uniform",
    }
    missing = required_components - set(implementation_binding_sha256_by_component)
    if missing:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_ISOLATED_MODULE_INVALID",
            "implementation binding set is incomplete for isolated graph",
            context={"missing_components": sorted(missing)},
        )
    k1 = {
        "vnpy_algotrading.base.AlgoStatus": "8416653d8cf61ab45e26b593eea06417dd6fa21b331bba6c60a2bbb8bccf8f93",
        "vnpy_algotrading.template.AlgoTemplate": "b21fa36a8a2c347ab92379df1cd9f81ec69bc922233ec4096d75dbbade7454b8",
    }
    specs = [
        (
            "vnpy_algotrading.base",
            "AlgoStatus",
            VnpyFacadeIsolatedBindingOwnerV1.K1_PINNED_SOURCE,
            "vnpy_algotrading.base:AlgoStatus",
            k1["vnpy_algotrading.base.AlgoStatus"],
        ),
        (
            "vnpy_algotrading.template",
            "AlgoTemplate",
            VnpyFacadeIsolatedBindingOwnerV1.K1_PINNED_SOURCE,
            "vnpy_algotrading.template:AlgoTemplate",
            k1["vnpy_algotrading.template.AlgoTemplate"],
        ),
        (
            "vnpy.trader.engine",
            "BaseEngine",
            VnpyFacadeIsolatedBindingOwnerV1.K4_FACADE_IMPLEMENTATION,
            "backend.execution_algos.vnpy_compat.facade:VnpyAlgoEngineFacadeV1",
            implementation_binding_sha256_by_component["facade.create"],
        ),
        (
            "vnpy.trader.utility",
            "round_to",
            VnpyFacadeIsolatedBindingOwnerV1.K4_PINNED_HELPER_IMPLEMENTATION,
            "backend.execution_algos.vnpy_compat.facade_projection:build_pinned_round_to_v1",
            implementation_binding_sha256_by_component["helper.round_to"],
        ),
        (
            "random",
            "uniform",
            VnpyFacadeIsolatedBindingOwnerV1.K4_DETERMINISTIC_INPUT_ADAPTER,
            "backend.execution_algos.vnpy_compat.facade_characterization:VnpyFacadeDeterministicUniformV1.__call__",
            implementation_binding_sha256_by_component["characterization.deterministic_uniform"],
        ),
        (
            "vnpy.trader.object",
            "TickData",
            VnpyFacadeIsolatedBindingOwnerV1.K4_DTO_PROJECTION,
            "backend.execution_algos.vnpy_compat.facade_projection:TickData",
            implementation_binding_sha256_by_component["projection.tick"],
        ),
        (
            "vnpy.trader.object",
            "OrderData",
            VnpyFacadeIsolatedBindingOwnerV1.K4_DTO_PROJECTION,
            "backend.execution_algos.vnpy_compat.facade_projection:OrderData",
            implementation_binding_sha256_by_component["projection.order"],
        ),
        (
            "vnpy.trader.object",
            "TradeData",
            VnpyFacadeIsolatedBindingOwnerV1.K4_DTO_PROJECTION,
            "backend.execution_algos.vnpy_compat.facade_projection:TradeData",
            implementation_binding_sha256_by_component["projection.trade"],
        ),
        (
            "vnpy.trader.object",
            "ContractData",
            VnpyFacadeIsolatedBindingOwnerV1.K4_DTO_PROJECTION,
            "backend.execution_algos.vnpy_compat.facade_projection:ContractData",
            implementation_binding_sha256_by_component["projection.contract"],
        ),
    ]
    enum_sha = implementation_binding_sha256_by_component["projection.enum"]
    for enum_name in ("Direction", "Offset", "OrderType", "Exchange", "Status"):
        specs.append(
            (
                "vnpy.trader.constant",
                enum_name,
                VnpyFacadeIsolatedBindingOwnerV1.K4_ENUM_PROJECTION,
                f"backend.execution_algos.vnpy_compat.facade_projection:{enum_name}",
                enum_sha,
            )
        )
    bindings = tuple(
        sorted(
            (
                VnpyFacadeIsolatedModuleBindingV1.create(
                    module_name=module,
                    export_name=export,
                    binding_owner=owner,
                    binding_ref=ref,
                    binding_source_identity_sha256_or_implementation_binding_sha256=source_hash,
                )
                for module, export, owner, ref, source_hash in specs
            ),
            key=lambda item: (item.module_name, item.export_name),
        )
    )
    isolated_module_binding_set_sha256_v1(bindings)
    return bindings


def readback_vnpy_facade_isolated_module_bindings_v1(
    payload: Any,
    *,
    implementation_binding_sha256_by_component: Mapping[str, str],
    source_root: Path = PINNED_SOURCE_ROOT,
) -> tuple[VnpyFacadeIsolatedModuleBindingV1, ...]:
    if type(payload) not in (tuple, list):
        raise TypeError("isolated module binding payload must be a tuple or JSON list")
    supplied = tuple(VnpyFacadeIsolatedModuleBindingV1.model_validate(item, strict=True) for item in payload)
    expected = build_vnpy_facade_isolated_module_bindings_v1(
        implementation_binding_sha256_by_component=implementation_binding_sha256_by_component,
        source_root=source_root,
    )
    if supplied != expected:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_ISOLATED_MODULE_INVALID",
            "isolated module binding readback drifted",
            context={
                "expected_set_sha256": isolated_module_binding_set_sha256_v1(expected),
                "actual_set_sha256": isolated_module_binding_set_sha256_v1(supplied),
            },
        )
    return expected


def build_vnpy_facade_characterization_receipt_v1(
    *,
    requirement: VnpyFacadeCharacterizationRequirementV1,
    source_manifest: VnpyFacadeSourceManifestV1,
    facade_contract: VnpyFacadeContractV1,
    factory_probe_config: Mapping[str, Any],
    vectors: tuple[VnpyFacadeCharacterizationVectorV1, ...],
    executed_vector_results: Mapping[str, Mapping[str, Any]],
) -> VnpyFacadeAlgorithmCharacterizationReceiptV1:
    """Record an observation without treating caller data as execution authority.

    K4-A owns the strict carrier and comparison semantics, but it deliberately
    does not own the pinned-source executor.  K4-B must replace this
    observation-only input with results produced inside its exact isolated
    executor before a PASSED receipt can exist.  A caller-provided mapping is
    therefore never sufficient to self-certify conformance.
    """

    if not isinstance(requirement, VnpyFacadeCharacterizationRequirementV1):
        raise TypeError("requirement must be VnpyFacadeCharacterizationRequirementV1")
    if not isinstance(source_manifest, VnpyFacadeSourceManifestV1):
        raise TypeError("source_manifest must be VnpyFacadeSourceManifestV1")
    if not isinstance(facade_contract, VnpyFacadeContractV1):
        raise TypeError("facade_contract must be VnpyFacadeContractV1")
    if not isinstance(factory_probe_config, Mapping) or any(type(key) is not str for key in factory_probe_config):
        raise TypeError("factory_probe_config must be a string-keyed mapping")
    if type(vectors) is not tuple or any(not isinstance(item, VnpyFacadeCharacterizationVectorV1) for item in vectors):
        raise TypeError("vectors must be a tuple of VnpyFacadeCharacterizationVectorV1")
    if not isinstance(executed_vector_results, Mapping) or any(
        type(vector_id) is not str or not isinstance(result, Mapping)
        for vector_id, result in executed_vector_results.items()
    ):
        raise TypeError("executed_vector_results must be a string-keyed mapping of mappings")
    source_matches = tuple(
        item for item in source_manifest.ordered_sources if item.algo_code_or_helper_name == requirement.algo_code
    )
    failures: list[VnpyFacadeConformanceFailureV1] = []
    failures.append(
        VnpyFacadeConformanceFailureV1.create(
            field_path="source_execution_authority",
            reason_code="MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE",
            context={
                "algo_code": requirement.algo_code,
                "available_owner": "K4_A_OBSERVATION_ONLY",
                "required_owner": "K4_B_PINNED_SOURCE_EXECUTOR",
            },
        )
    )
    if len(source_matches) != 1 or source_matches[0].source_identity_sha256 != requirement.source_identity_sha256:
        failures.append(
            VnpyFacadeConformanceFailureV1.create(
                field_path="source_identity_sha256",
                reason_code="MINIQMT_VNPY_FACADE_SOURCE_INVALID",
                context={"algo_code": requirement.algo_code},
            )
        )
    ordered = tuple(sorted(vectors, key=lambda item: item.vector_id))
    vector_ids = tuple(item.vector_id for item in ordered)
    if (
        vectors != ordered
        or len(vector_ids) != len(set(vector_ids))
        or any(item.algo_code != requirement.algo_code for item in ordered)
    ):
        failures.append(
            VnpyFacadeConformanceFailureV1.create(
                field_path="ordered_vectors",
                reason_code="MINIQMT_VNPY_FACADE_CHARACTERIZATION_DRIFT",
                context={"vector_ids": list(vector_ids)},
            )
        )
    actual_ids = tuple(sorted(executed_vector_results))
    if actual_ids != vector_ids:
        failures.append(
            VnpyFacadeConformanceFailureV1.create(
                field_path="executed_vector_results",
                reason_code="MINIQMT_VNPY_FACADE_CHARACTERIZATION_DRIFT",
                context={"expected": list(vector_ids), "actual": list(actual_ids)},
            )
        )
    expected_fields = (
        "ordered_facade_calls",
        "ordered_effects",
        "after_state_sha256",
        "terminal_outcome",
    )
    for vector in ordered:
        actual = executed_vector_results.get(vector.vector_id)
        if actual is None:
            continue
        expected = {
            "ordered_facade_calls": [thaw_json_v1(item) for item in vector.expected_ordered_facade_calls],
            "ordered_effects": [thaw_json_v1(item) for item in vector.expected_ordered_effects],
            "after_state_sha256": vector.expected_after_state_sha256,
            "terminal_outcome": vector.expected_terminal_outcome,
        }
        normalized_actual = {field: actual.get(field) for field in expected_fields}
        if normalized_actual != expected or set(actual) != set(expected_fields):
            failures.append(
                VnpyFacadeConformanceFailureV1.create(
                    field_path=f"vectors.{vector.vector_id}",
                    reason_code="MINIQMT_VNPY_FACADE_CHARACTERIZATION_DRIFT",
                    context={"expected": expected, "actual": dict(actual)},
                )
            )
    vector_set_sha256 = hash_hex_v1(
        "miniqmt_vnpy_facade_characterization_vector_set_v1",
        [item.canonical_payload_v1() for item in ordered],
    )
    config = dict(factory_probe_config)
    return VnpyFacadeAlgorithmCharacterizationReceiptV1.create(
        algo_code=requirement.algo_code,
        source_identity_sha256=requirement.source_identity_sha256,
        facade_source_manifest_sha256=source_manifest.manifest_sha256,
        characterization_requirement_sha256=requirement.requirement_sha256,
        canonical_factory_probe_config=config,
        factory_probe_config_sha256=hash_hex_v1("miniqmt_vnpy_facade_factory_probe_config_v1", config),
        facade_contract_sha256=facade_contract.facade_contract_sha256,
        implementation_binding_set_sha256=facade_contract.implementation_binding_set_sha256,
        dto_mapping_set_sha256=facade_contract.dto_mapping_set_sha256,
        state_mapping_set_sha256=requirement.state_mapping_set_sha256,
        terminal_mapping_set_sha256=terminal_mapping_set_sha256_v1(
            tuple(item for item in build_vnpy_facade_terminal_mappings_v1() if item.algo_code == requirement.algo_code)
        ),
        isolated_module_binding_set_sha256=facade_contract.isolated_module_binding_set_sha256,
        ordered_vector_ids=vector_ids,
        vector_set_sha256=vector_set_sha256,
        status=(VnpyFacadeCompatibilityStatusV1.FAILED if failures else VnpyFacadeCompatibilityStatusV1.PASSED),
        ordered_failures=tuple(failures),
    )


def readback_vnpy_facade_characterization_receipt_v1(
    payload: Any,
    **authority: Any,
) -> VnpyFacadeAlgorithmCharacterizationReceiptV1:
    supplied = VnpyFacadeAlgorithmCharacterizationReceiptV1.model_validate(payload, strict=True)
    expected = build_vnpy_facade_characterization_receipt_v1(**authority)
    if supplied != expected:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_DRIFT",
            "characterization receipt conflicts with exact executor authority",
            context={
                "expected_receipt_sha256": expected.receipt_sha256,
                "actual_receipt_sha256": supplied.receipt_sha256,
            },
        )
    return expected


def build_vnpy_facade_algorithm_bindings_v1(
    *,
    characterization_receipts: Mapping[str, VnpyFacadeAlgorithmCharacterizationReceiptV1],
    adapter_contract_sha256: str,
    source_root: Path = PINNED_SOURCE_ROOT,
) -> tuple[VnpyFacadeAlgorithmBindingV1, ...]:
    if not isinstance(characterization_receipts, Mapping) or any(
        type(algo_code) is not str for algo_code in characterization_receipts
    ):
        raise TypeError("characterization_receipts must be a string-keyed mapping")
    if type(adapter_contract_sha256) is not str:
        raise TypeError("adapter_contract_sha256 must be a string")
    if not isinstance(source_root, Path):
        raise TypeError("source_root must be pathlib.Path")
    raise VnpyFacadeContractError(
        "MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE",
        "algorithm binding publication requires K4-B pinned source execution authority",
        context={
            "available_owner": "K4_A_OBSERVATION_ONLY",
            "required_owner": "K4_B_PINNED_SOURCE_EXECUTOR",
            "algorithms": sorted(characterization_receipts),
            "adapter_contract_sha256": adapter_contract_sha256,
            "source_root": source_root.name,
        },
    )


def readback_vnpy_facade_algorithm_bindings_v1(
    payload: Any, **authority: Any
) -> tuple[VnpyFacadeAlgorithmBindingV1, ...]:
    if type(payload) not in (tuple, list):
        raise TypeError("algorithm binding payload must be a tuple or JSON list")
    supplied = tuple(VnpyFacadeAlgorithmBindingV1.model_validate(item, strict=True) for item in payload)
    expected = build_vnpy_facade_algorithm_bindings_v1(**authority)
    if supplied != expected:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
            "algorithm binding readback conflicts with exact source authority",
            context={
                "expected": [item.binding_sha256 for item in expected],
                "actual": [item.binding_sha256 for item in supplied],
            },
        )
    return expected


def build_vnpy_facade_conformance_set_v1(
    *,
    catalog_runtime: PluginCatalogRuntimeV2,
    facade_contract: VnpyFacadeContractV1,
    source_manifest: VnpyFacadeSourceManifestV1,
    characterization_receipts: Mapping[str, VnpyFacadeAlgorithmCharacterizationReceiptV1],
    algorithm_bindings: Mapping[str, VnpyFacadeAlgorithmBindingV1],
) -> VnpyFacadeConformanceSetV1:
    """Derive the conformance view without publishing a second catalog."""

    if not isinstance(catalog_runtime, PluginCatalogRuntimeV2):
        raise TypeError("catalog_runtime must be PluginCatalogRuntimeV2")
    if not isinstance(facade_contract, VnpyFacadeContractV1):
        raise TypeError("facade_contract must be VnpyFacadeContractV1")
    if not isinstance(source_manifest, VnpyFacadeSourceManifestV1):
        raise TypeError("source_manifest must be VnpyFacadeSourceManifestV1")
    for field_name, values in (
        ("characterization_receipts", characterization_receipts),
        ("algorithm_bindings", algorithm_bindings),
    ):
        if not isinstance(values, Mapping) or any(type(algo_code) is not str for algo_code in values):
            raise TypeError(f"{field_name} must be a string-keyed mapping")
    raise VnpyFacadeContractError(
        "MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE",
        "conformance publication requires K4-B pinned source execution authority",
        context={
            "available_owner": "K4_A_OBSERVATION_ONLY",
            "required_owner": "K4_B_PINNED_SOURCE_EXECUTOR",
            "characterization_algorithms": sorted(characterization_receipts),
            "binding_algorithms": sorted(algorithm_bindings),
            "plugin_catalog_sha256": catalog_runtime.snapshot.catalog_sha256,
            "facade_contract_sha256": facade_contract.facade_contract_sha256,
            "facade_source_manifest_sha256": source_manifest.manifest_sha256,
        },
    )


def readback_vnpy_facade_conformance_set_v1(payload: Any, **authority: Any) -> VnpyFacadeConformanceSetV1:
    supplied = VnpyFacadeConformanceSetV1.model_validate(payload, strict=True)
    expected = build_vnpy_facade_conformance_set_v1(**authority)
    if supplied != expected:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONFORMANCE_DRIFT",
            "conformance set conflicts with live catalog/process authority",
            context={
                "expected_receipt_set_sha256": expected.receipt_set_sha256,
                "actual_receipt_set_sha256": supplied.receipt_set_sha256,
            },
        )
    return expected


_IMPLEMENTATION_COMPONENTS = (
    "adapter.extract_state",
    "adapter.initialize",
    "adapter.restore",
    "adapter.transition",
    "characterization.build",
    "characterization.deterministic_uniform",
    "characterization.module_binding.build",
    "characterization.module_binding.readback",
    "characterization.readback",
    "collector.create",
    "collector.freeze",
    "conformance.build",
    "conformance.readback",
    "facade.cancel_order",
    "facade.create",
    "facade.get_contract",
    "facade.get_tick",
    "facade.put_algo_event",
    "facade.send_order",
    "facade.write_log",
    "helper.round_to",
    "projection.contract",
    "projection.enum",
    "projection.order",
    "projection.tick",
    "projection.trade",
    "source_manifest.build",
    "source_manifest.readback",
    "state_mapping.build",
    "state_mapping.readback",
)


def _implementation_callable_map_v1() -> dict[str, Any]:
    from .facade import VnpyAlgoEngineFacadeV1, VnpyFacadeEffectCollectorV1

    return {
        "adapter.extract_state": VnpyFacadeBackedPluginAdapterV1.extract_state_v1,
        "adapter.initialize": VnpyFacadeBackedPluginAdapterV1.initialize_with_facade,
        "adapter.restore": VnpyFacadeBackedPluginAdapterV1.restore_algorithm_v1,
        "adapter.transition": VnpyFacadeBackedPluginAdapterV1.transition_with_facade,
        "characterization.build": build_vnpy_facade_characterization_receipt_v1,
        "characterization.deterministic_uniform": VnpyFacadeDeterministicUniformV1.__call__,
        "characterization.module_binding.build": build_vnpy_facade_isolated_module_bindings_v1,
        "characterization.module_binding.readback": readback_vnpy_facade_isolated_module_bindings_v1,
        "characterization.readback": readback_vnpy_facade_characterization_receipt_v1,
        "collector.create": VnpyFacadeEffectCollectorV1.create,
        "collector.freeze": VnpyFacadeEffectCollectorV1.freeze,
        "conformance.build": build_vnpy_facade_conformance_set_v1,
        "conformance.readback": readback_vnpy_facade_conformance_set_v1,
        "facade.cancel_order": VnpyAlgoEngineFacadeV1.cancel_order,
        "facade.create": VnpyAlgoEngineFacadeV1.create,
        "facade.get_contract": VnpyAlgoEngineFacadeV1.get_contract,
        "facade.get_tick": VnpyAlgoEngineFacadeV1.get_tick,
        "facade.put_algo_event": VnpyAlgoEngineFacadeV1.put_algo_event,
        "facade.send_order": VnpyAlgoEngineFacadeV1.send_order,
        "facade.write_log": VnpyAlgoEngineFacadeV1.write_log,
        "helper.round_to": build_pinned_round_to_v1,
        "projection.contract": ContractData,
        "projection.enum": project_order_status_v1,
        "projection.order": OrderData,
        "projection.tick": TickData,
        "projection.trade": TradeData,
        "source_manifest.build": build_vnpy_facade_source_manifest_v1,
        "source_manifest.readback": readback_vnpy_facade_source_manifest_v1,
        "state_mapping.build": build_vnpy_facade_state_mappings_v1,
        "state_mapping.readback": readback_vnpy_facade_state_mappings_v1,
    }


def _annotation_token_v1(value: Any) -> str | None:
    if value is inspect.Signature.empty:
        return None
    if type(value) is str:
        return value
    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if type(module) is str and type(qualname) is str:
        return f"{module}.{qualname}"
    rendered = str(value)
    if "0x" in rendered:
        raise TypeError("callable annotation has a process-specific representation")
    return rendered


def _facade_callable_signature_payload_v1(value: Any, *, root: Path) -> dict[str, Any]:
    signature = inspect.signature(value, eval_str=False)
    parameters: list[dict[str, Any]] = []
    for parameter in signature.parameters.values():
        default = parameter.default
        if default is inspect.Signature.empty:
            default_payload: Any = {"required": True}
        elif default is None or type(default) in (bool, int, str):
            default_payload = {"required": False, "value": default}
        elif isinstance(default, Path):
            try:
                relative = default.resolve().relative_to(root).as_posix()
            except ValueError as exc:
                raise TypeError("callable Path default is outside repository authority") from exc
            default_payload = {"required": False, "repo_relative_path": relative}
        else:
            raise TypeError(f"unsupported facade callable default for {parameter.name}")
        parameters.append(
            {
                "name": parameter.name,
                "kind": parameter.kind.name,
                "default": default_payload,
                "annotation": _annotation_token_v1(parameter.annotation),
            }
        )
    return {
        "parameters": parameters,
        "return_annotation": _annotation_token_v1(signature.return_annotation),
    }


def _facade_callable_signature_sha256_v1(value: Any, *, root: Path) -> str:
    return hash_hex_v1(
        "miniqmt_vnpy_facade_callable_signature_v1",
        _facade_callable_signature_payload_v1(value, root=root),
    )


def build_vnpy_facade_implementation_bindings_v1() -> tuple[VnpyFacadeImplementationBindingV1, ...]:
    callables = _implementation_callable_map_v1()
    if tuple(sorted(callables)) != _IMPLEMENTATION_COMPONENTS:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
            "implementation component set is not exact",
            context={"expected": list(_IMPLEMENTATION_COMPONENTS), "actual": sorted(callables)},
        )
    root = Path(__file__).resolve().parents[3]
    bindings: list[VnpyFacadeImplementationBindingV1] = []
    for component_name in _IMPLEMENTATION_COMPONENTS:
        callable_value = callables[component_name]
        source_file = inspect.getsourcefile(callable_value)
        if source_file is None:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_BINDING_INVALID",
                "implementation callable source is unavailable",
                context={"component_name": component_name},
            )
        path = Path(source_file).resolve()
        try:
            relative = path.relative_to(root).as_posix()
        except ValueError as exc:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_BINDING_INVALID",
                "implementation callable is outside repository authority",
                context={"component_name": component_name, "source_file": path.as_posix()},
            ) from exc
        canonical_lf = path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")
        bindings.append(
            VnpyFacadeImplementationBindingV1.create(
                component_name=component_name,
                callable_ref=callable_ref_v1(callable_value),
                callable_signature_sha256=_facade_callable_signature_sha256_v1(callable_value, root=root),
                repo_relative_source_path=relative,
                canonical_lf_source_size=len(canonical_lf),
                canonical_lf_source_sha256=hashlib.sha256(canonical_lf).hexdigest(),
            )
        )
    return tuple(bindings)


def readback_vnpy_facade_implementation_bindings_v1(payload: Any) -> tuple[VnpyFacadeImplementationBindingV1, ...]:
    if type(payload) not in (tuple, list):
        raise TypeError("implementation binding payload must be a tuple or JSON list")
    supplied = tuple(VnpyFacadeImplementationBindingV1.model_validate(item, strict=True) for item in payload)
    expected = build_vnpy_facade_implementation_bindings_v1()
    if supplied != expected:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
            "implementation binding readback conflicts with live source/callable authority",
            context={
                "expected": [item.binding_sha256 for item in expected],
                "actual": [item.binding_sha256 for item in supplied],
            },
        )
    return expected


def _template_helper_ref_sha256_v1(
    *,
    method_name: str,
    source_root: Path,
) -> str:
    path = source_root / "vnpy_algotrading" / "template.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename="vnpy_algotrading/template.py")
    classes = [item for item in tree.body if isinstance(item, ast.ClassDef) and item.name == "AlgoTemplate"]
    methods = (
        [item for item in classes[0].body if isinstance(item, ast.FunctionDef) and item.name == method_name]
        if len(classes) == 1
        else []
    )
    if len(methods) != 1:
        raise _source_error("pinned template helper is not unique", method_name=method_name)
    method = methods[0]
    return hash_hex_v1(
        "miniqmt_vnpy_facade_pinned_template_helper_v1",
        {
            "source_sha256": "b21fa36a8a2c347ab92379df1cd9f81ec69bc922233ec4096d75dbbade7454b8",
            "method_name": method_name,
            "signature": _stable_ast_dump_v1(method.args),
            "body": [_stable_ast_dump_v1(item) for item in method.body],
        },
    )


def build_vnpy_facade_method_contracts_v1(
    *,
    compatibility_requirement: VnpyCompatibilityRequirementV2,
    implementation_bindings: tuple[VnpyFacadeImplementationBindingV1, ...],
    source_root: Path = PINNED_SOURCE_ROOT,
) -> tuple[VnpyFacadeMethodContractV1, ...]:
    if not isinstance(compatibility_requirement, VnpyCompatibilityRequirementV2):
        raise TypeError("compatibility_requirement must be VnpyCompatibilityRequirementV2")
    implementation = {item.component_name: item for item in implementation_bindings}
    required = tuple(compatibility_requirement.required_method_signatures)
    contracts: list[VnpyFacadeMethodContractV1] = []
    component_by_method = {
        ("AlgoEngine", "cancel_order"): "facade.cancel_order",
        ("AlgoEngine", "get_contract"): "facade.get_contract",
        ("AlgoEngine", "get_tick"): "facade.get_tick",
        ("AlgoEngine", "put_algo_event"): "facade.put_algo_event",
        ("AlgoEngine", "send_order"): "facade.send_order",
        ("AlgoEngine", "write_log"): "facade.write_log",
        ("AlgoTemplate", "update_order"): "adapter.transition",
        ("AlgoTemplate", "update_tick"): "adapter.transition",
        ("AlgoTemplate", "update_timer"): "adapter.transition",
        ("AlgoTemplate", "update_trade"): "adapter.transition",
    }
    if {(item.owner, item.name) for item in required} != set(component_by_method):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
            "K1 required method surface is not the exact K4 surface",
            context={"methods": sorted((item.owner, item.name) for item in required)},
        )
    for item in required:
        component = component_by_method[(item.owner, item.name)]
        effect_types = (
            ("BrokerCommandV2",)
            if item.name in {"send_order", "cancel_order"}
            else (
                ("DiagnosticObservationV1",)
                if item.name in {"get_tick", "get_contract", "write_log", "put_algo_event"}
                else ("AlgoTransitionV1",)
            )
        )
        contracts.append(
            VnpyFacadeMethodContractV1.create(
                surface_owner=item.owner,
                method_name=item.name,
                pinned_surface_ref_kind="K1_METHOD_REQUIREMENT",
                pinned_surface_ref_sha256=item.method_requirement_sha256,
                ordered_invocation_phases=("TRANSITION",)
                if item.name.startswith("update_")
                else ("INITIALIZE", "TRANSITION"),
                ordered_required_authority_refs=("facade_contract_sha256", "authority_input_sha256"),
                return_disposition=item.return_behavior,
                empty_return_disposition=item.error_behavior,
                ordered_effect_types=effect_types,
                ordered_reason_codes=("MINIQMT_VNPY_FACADE_CONTRACT_INVALID",),
                implementation_binding_sha256=implementation[component].binding_sha256,
            )
        )
    for method_name in ("buy", "cancel_all", "finish", "pause", "put_event", "resume", "sell", "start"):
        component = "adapter.initialize" if method_name == "start" else "adapter.transition"
        contracts.append(
            VnpyFacadeMethodContractV1.create(
                surface_owner="AlgoTemplate",
                method_name=method_name,
                pinned_surface_ref_kind="PINNED_TEMPLATE_HELPER",
                pinned_surface_ref_sha256=_template_helper_ref_sha256_v1(
                    method_name=method_name, source_root=source_root
                ),
                ordered_invocation_phases=("INITIALIZE",) if method_name == "start" else ("INITIALIZE", "TRANSITION"),
                ordered_required_authority_refs=("facade_contract_sha256", "state_mapping_set_sha256"),
                return_disposition="PINNED_TEMPLATE_HELPER_EXACT",
                empty_return_disposition="PINNED_EMPTY_RETURN_WITH_TYPED_DIAGNOSTIC",
                ordered_effect_types=("AlgoTransitionV1",),
                ordered_reason_codes=("MINIQMT_VNPY_FACADE_CONTRACT_INVALID",),
                implementation_binding_sha256=implementation[component].binding_sha256,
            )
        )
    return tuple(sorted(contracts, key=lambda item: (item.surface_owner, item.method_name)))


def build_vnpy_facade_contract_v1(
    *,
    compatibility_requirements: tuple[VnpyCompatibilityRequirementV2, ...],
    source_root: Path = PINNED_SOURCE_ROOT,
) -> VnpyFacadeContractV1:
    if (
        type(compatibility_requirements) is not tuple
        or not compatibility_requirements
        or any(not isinstance(item, VnpyCompatibilityRequirementV2) for item in compatibility_requirements)
    ):
        raise TypeError("compatibility_requirements must be a non-empty tuple of VnpyCompatibilityRequirementV2")
    component_sets = tuple(compatibility_component_hashes_v2(item) for item in compatibility_requirements)
    shared_fields = ("source_lock_sha256", "method_signature_sha256", "object_field_sha256")
    if any(
        any(components[field] != component_sets[0][field] for field in shared_fields)
        for components in component_sets[1:]
    ):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
            "K1 requirements do not share one exact facade method/object/source authority",
            context={"components": list(component_sets)},
        )
    method_payloads = tuple(
        [item.canonical_payload_v1() for item in requirement.required_method_signatures]
        for requirement in compatibility_requirements
    )
    if any(payload != method_payloads[0] for payload in method_payloads[1:]):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
            "K1 required method payloads differ across current facade algorithms",
            context={},
        )
    shared_components = {field: component_sets[0][field] for field in shared_fields}
    shared_requirement_sha256 = hash_hex_v1(
        "miniqmt_vnpy_facade_shared_k1_requirement_v1",
        {
            **shared_components,
            "ordered_plugin_requirement_sha256": sorted(item.requirement_sha256 for item in compatibility_requirements),
        },
    )
    shared_surface_sha256 = hash_hex_v1(
        "miniqmt_vnpy_facade_shared_k1_surface_v1",
        shared_components,
    )
    implementations = build_vnpy_facade_implementation_bindings_v1()
    methods = build_vnpy_facade_method_contracts_v1(
        compatibility_requirement=compatibility_requirements[0],
        implementation_bindings=implementations,
        source_root=source_root,
    )
    dto = build_vnpy_facade_dto_mappings_v1()
    state = build_vnpy_facade_state_mappings_v1(source_root=source_root)
    terminal = build_vnpy_facade_terminal_mappings_v1(source_root=source_root)
    implementation_hashes = {item.component_name: item.binding_sha256 for item in implementations}
    isolated = build_vnpy_facade_isolated_module_bindings_v1(
        implementation_binding_sha256_by_component=implementation_hashes,
        source_root=source_root,
    )
    return VnpyFacadeContractV1.create(
        requirement_sha256=shared_requirement_sha256,
        surface_sha256=shared_surface_sha256,
        method_signature_sha256=shared_components["method_signature_sha256"],
        object_field_sha256=shared_components["object_field_sha256"],
        ordered_implementation_bindings=implementations,
        ordered_method_contracts=methods,
        dto_mapping_set_sha256=dto_mapping_set_sha256_v1(dto),
        state_mapping_set_sha256=state_mapping_set_sha256_v1(state),
        terminal_mapping_set_sha256=terminal_mapping_set_sha256_v1(terminal),
        isolated_module_binding_set_sha256=isolated_module_binding_set_sha256_v1(isolated),
    )


def readback_vnpy_facade_contract_v1(
    payload: Any,
    *,
    compatibility_requirements: tuple[VnpyCompatibilityRequirementV2, ...],
    source_root: Path = PINNED_SOURCE_ROOT,
) -> VnpyFacadeContractV1:
    supplied = VnpyFacadeContractV1.model_validate(payload, strict=True)
    expected = build_vnpy_facade_contract_v1(
        compatibility_requirements=compatibility_requirements,
        source_root=source_root,
    )
    if supplied != expected:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
            "facade contract readback conflicts with live implementation and pinned source",
            context={"expected": expected.facade_contract_sha256, "actual": supplied.facade_contract_sha256},
        )
    return expected


def build_vnpy_facade_characterization_requirements_v1(
    *,
    catalog_runtime: PluginCatalogRuntimeV2,
    source_manifest: VnpyFacadeSourceManifestV1,
    source_root: Path = PINNED_SOURCE_ROOT,
) -> tuple[VnpyFacadeCharacterizationRequirementV1, ...]:
    """Build five exact characterization requirements without registering Iceberg/Stop."""

    if not isinstance(catalog_runtime, PluginCatalogRuntimeV2):
        raise TypeError("catalog_runtime must be PluginCatalogRuntimeV2")
    if not isinstance(source_manifest, VnpyFacadeSourceManifestV1):
        raise TypeError("source_manifest must be VnpyFacadeSourceManifestV1")
    source_by_algo = {
        item.algo_code_or_helper_name: item
        for item in source_manifest.ordered_sources
        if item.source_role is VnpyFacadeSourceRoleV1.ALGORITHM
    }
    descriptor_by_algo = {
        item.manifest.algo_code: item
        for item in catalog_runtime.snapshot.registration_descriptors
        if item.manifest.required_facade_methods
    }
    if tuple(sorted(descriptor_by_algo)) != (
        "BEST_LIMIT_MINIQMT",
        "SNIPER_MINIQMT",
        "TWAP_LITE_MINIQMT",
    ):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            "current catalog characterization owner set differs from current-three",
            context={"algo_codes": sorted(descriptor_by_algo)},
        )
    state_mappings = build_vnpy_facade_state_mappings_v1(source_root=source_root)
    planned = {
        "ICEBERG": {
            "schema_version": "iceberg_characterization_config_v1",
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "required": ["display_volume", "interval"],
                "properties": {
                    "display_volume": {"type": "number", "minimum": 0},
                    "interval": {"type": "integer", "minimum": 0},
                },
            },
            "methods": (
                "cancel_order",
                "get_tick",
                "put_algo_event",
                "send_order",
                "update_order",
                "update_timer",
                "update_trade",
                "write_log",
            ),
            "objects": (
                "OrderData.price",
                "OrderData.status",
                "OrderData.traded",
                "OrderData.vt_orderid",
                "TickData.ask_price_1",
                "TickData.bid_price_1",
                "TickData.vt_symbol",
                "TradeData.price",
                "TradeData.volume",
                "TradeData.vt_orderid",
                "TradeData.vt_tradeid",
            ),
            "enums": (
                "AlgoStatus.FINISHED",
                "AlgoStatus.RUNNING",
                "Direction.LONG",
                "Direction.SHORT",
                "Offset.NONE",
                "OrderType.LIMIT",
            ),
            "events": ("ALGO_START", "ORDER", "TIMER", "TRADE"),
            "capabilities": ("L1_ASK", "L1_BID"),
        },
        "STOP": {
            "schema_version": "stop_characterization_config_v1",
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "required": ["price_add"],
                "properties": {
                    "price_add": {
                        "type": "string",
                        "pattern": r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$",
                    }
                },
            },
            "methods": (
                "put_algo_event",
                "send_order",
                "update_order",
                "update_tick",
                "update_trade",
                "write_log",
            ),
            "objects": (
                "OrderData.status",
                "OrderData.vt_orderid",
                "TickData.last_price",
                "TickData.limit_down",
                "TickData.limit_up",
                "TickData.vt_symbol",
                "TradeData.price",
                "TradeData.volume",
                "TradeData.vt_orderid",
                "TradeData.vt_tradeid",
            ),
            "enums": (
                "AlgoStatus.FINISHED",
                "AlgoStatus.RUNNING",
                "Direction.LONG",
                "Direction.SHORT",
                "Offset.NONE",
                "OrderType.LIMIT",
            ),
            "events": ("ALGO_START", "ORDER", "TICK", "TRADE"),
            "capabilities": ("LAST_PRICE", "LIMIT_UP_DOWN"),
        },
    }
    requirements: list[VnpyFacadeCharacterizationRequirementV1] = []
    for algo_code in sorted(source_by_algo):
        source = source_by_algo[algo_code]
        algo_state = tuple(item for item in state_mappings if item.algo_code == algo_code)
        descriptor = descriptor_by_algo.get(algo_code)
        if descriptor is not None:
            manifest = descriptor.manifest
            schema = thaw_json_v1(manifest.config_schema)
            object_fields = tuple(
                f"{owner.object_name}.{field.name}"
                for owner in manifest.required_facade_object_fields
                for field in owner.fields
            )
            enum_members = tuple(
                f"{owner.enum_name}.{member.name}"
                for owner in manifest.compatibility_requirement.required_enum_values
                for member in owner.members
            )
            methods = manifest.required_facade_methods
            events = tuple(item.value for item in manifest.subscribed_event_types)
            capabilities = tuple(item.capability.value for item in manifest.market_data_requirements)
            schema_version = manifest.config_schema_version
            config_contract = {
                "algo_code": algo_code,
                "config_validator_binding_id": descriptor.config_validator_binding_id,
                "config_validator_callable_ref": descriptor.config_validator_callable_ref,
                "config_validator_signature_sha256": descriptor.config_validator_signature_sha256,
                "config_schema_sha256": manifest.config_schema_sha256,
            }
        else:
            facts = planned[algo_code]
            schema = facts["schema"]
            object_fields = facts["objects"]
            enum_members = facts["enums"]
            methods = facts["methods"]
            events = facts["events"]
            capabilities = facts["capabilities"]
            schema_version = facts["schema_version"]
            config_contract = {
                "algo_code": algo_code,
                "source_identity_sha256": source.source_identity_sha256,
                "config_schema": schema,
                "validation": "STRICT_NO_DEFAULTS_ADDITIONAL_PROPERTIES_FALSE",
            }
        requirements.append(
            VnpyFacadeCharacterizationRequirementV1.create(
                algo_code=algo_code,
                registration_disposition=source.registration_disposition,
                source_identity_sha256=source.source_identity_sha256,
                config_schema_version=schema_version,
                config_schema=schema,
                config_schema_sha256=hash_hex_v1("miniqmt_plugin_config_schema_v1", schema),
                config_validation_contract_sha256=hash_hex_v1(
                    "miniqmt_vnpy_facade_config_validation_contract_v1", config_contract
                ),
                ordered_required_methods=methods,
                ordered_required_object_fields=object_fields,
                ordered_required_enum_members=enum_members,
                ordered_event_types=events,
                ordered_market_data_capabilities=capabilities,
                state_mapping_set_sha256=state_mapping_set_sha256_v1(algo_state),
            )
        )
    return tuple(requirements)


def build_vnpy_facade_characterization_manifest_views_v1(
    *,
    catalog_runtime: PluginCatalogRuntimeV2,
    source_manifest: VnpyFacadeSourceManifestV1,
    requirements: tuple[VnpyFacadeCharacterizationRequirementV1, ...],
) -> tuple[VnpyFacadeCharacterizationManifestViewV1, ...]:
    requirement_by_algo = {item.algo_code: item for item in requirements}
    descriptor_by_algo = {
        item.manifest.algo_code: item
        for item in catalog_runtime.snapshot.registration_descriptors
        if item.manifest.required_facade_methods
    }
    views: list[VnpyFacadeCharacterizationManifestViewV1] = []
    for source in source_manifest.ordered_sources:
        if source.source_role is not VnpyFacadeSourceRoleV1.ALGORITHM:
            continue
        algo_code = source.algo_code_or_helper_name
        requirement = requirement_by_algo[algo_code]
        descriptor = descriptor_by_algo.get(algo_code)
        manifest = None if descriptor is None else descriptor.manifest
        views.append(
            VnpyFacadeCharacterizationManifestViewV1.create(
                algo_code=algo_code,
                registration_disposition=source.registration_disposition,
                real_plugin_key_or_null=(None if descriptor is None else descriptor.plugin_key.canonical_payload_v1()),
                real_manifest_sha256_or_null=(None if manifest is None else manifest.manifest_sha256),
                required_facade_methods=requirement.ordered_required_methods,
                required_object_fields=requirement.ordered_required_object_fields,
                required_enum_members=requirement.ordered_required_enum_members,
                order_types=("LIMIT",),
                market_data_capabilities=requirement.ordered_market_data_capabilities,
                state_schema_sha256=(
                    requirement.state_mapping_set_sha256 if manifest is None else manifest.state_schema_sha256
                ),
                characterization_requirement_sha256=requirement.requirement_sha256,
            )
        )
    return tuple(sorted(views, key=lambda item: item.algo_code))


_CHARACTERIZATION_AUTHORITY_TOKEN_V2 = object()


class VnpyFacadeCharacterizationAuthorityV2:
    """Process-local sealed five-algorithm execution/receipt authority."""

    __slots__ = (
        "_authority_sha256",
        "_receipts",
        "_sealed",
        "_source_execution_sets",
        "_source_executor_binding",
    )

    def __init__(
        self,
        *,
        token: object,
        source_executor_binding: VnpyFacadeSourceExecutorBindingV1,
        source_execution_sets: tuple[VnpyFacadeSourceExecutionSetV1, ...],
        receipts: tuple[VnpyFacadeAlgorithmCharacterizationReceiptV2, ...],
        authority_sha256: str,
    ) -> None:
        if token is not _CHARACTERIZATION_AUTHORITY_TOKEN_V2:
            raise TypeError("VnpyFacadeCharacterizationAuthorityV2 can only be created by its authority builder")
        self._source_executor_binding = source_executor_binding
        self._source_execution_sets = source_execution_sets
        self._receipts = receipts
        self._authority_sha256 = authority_sha256
        self._sealed = True

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_sealed", False):
            raise AttributeError("VnpyFacadeCharacterizationAuthorityV2 is immutable")
        object.__setattr__(self, name, value)

    @property
    def source_executor_binding(self) -> VnpyFacadeSourceExecutorBindingV1:
        return self._source_executor_binding

    @property
    def source_execution_sets(self) -> tuple[VnpyFacadeSourceExecutionSetV1, ...]:
        return self._source_execution_sets

    @property
    def receipts(self) -> tuple[VnpyFacadeAlgorithmCharacterizationReceiptV2, ...]:
        return self._receipts

    @property
    def authority_sha256(self) -> str:
        return self._authority_sha256

    def receipt_for_algo_v2(self, algo_code: str) -> VnpyFacadeAlgorithmCharacterizationReceiptV2:
        matches = tuple(item for item in self._receipts if item.algo_code == algo_code)
        if len(matches) != 1:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
                "characterization authority does not contain one exact algorithm receipt",
                context={"algo_code": algo_code, "match_count": len(matches)},
            )
        return matches[0]

    def execution_set_for_algo_v2(self, algo_code: str) -> VnpyFacadeSourceExecutionSetV1:
        matches = tuple(item for item in self._source_execution_sets if item.algo_code == algo_code)
        if len(matches) != 1:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
                "characterization authority does not contain one exact source execution set",
                context={"algo_code": algo_code, "match_count": len(matches)},
            )
        return matches[0]


def _seal_characterization_authority_v2(
    *,
    source_executor_binding: VnpyFacadeSourceExecutorBindingV1,
    source_execution_sets: tuple[VnpyFacadeSourceExecutionSetV1, ...],
    receipts: tuple[VnpyFacadeAlgorithmCharacterizationReceiptV2, ...],
) -> VnpyFacadeCharacterizationAuthorityV2:
    ordered_sets = tuple(sorted(source_execution_sets, key=lambda item: item.algo_code))
    ordered_receipts = tuple(sorted(receipts, key=lambda item: item.algo_code))
    expected_algos = (
        "BEST_LIMIT_MINIQMT",
        "ICEBERG",
        "SNIPER_MINIQMT",
        "STOP",
        "TWAP_LITE_MINIQMT",
    )
    if (
        tuple(item.algo_code for item in ordered_sets) != expected_algos
        or tuple(item.algo_code for item in ordered_receipts) != expected_algos
        or any(item.status is not VnpyFacadeCompatibilityStatusV1.PASSED for item in ordered_sets)
        or any(item.status is not VnpyFacadeCompatibilityStatusV1.PASSED for item in ordered_receipts)
    ):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            "characterization authority requires complete PASSED five-algorithm sets and receipts",
            context={
                "execution_algorithms": [item.algo_code for item in ordered_sets],
                "receipt_algorithms": [item.algo_code for item in ordered_receipts],
            },
        )
    by_algo = {item.algo_code: item for item in ordered_sets}
    if any(
        item.source_executor_binding_sha256 != source_executor_binding.binding_sha256
        or item.source_execution_set_sha256 != by_algo[item.algo_code].execution_set_sha256
        for item in ordered_receipts
    ):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            "characterization receipt/execution-set/executor closure drifted",
            context={},
        )
    authority_payload = {
        "source_executor_binding": source_executor_binding.canonical_payload_v1(),
        "ordered_source_execution_sets": [item.canonical_payload_v1() for item in ordered_sets],
        "ordered_receipts": [item.canonical_payload_v1() for item in ordered_receipts],
    }
    return VnpyFacadeCharacterizationAuthorityV2(
        token=_CHARACTERIZATION_AUTHORITY_TOKEN_V2,
        source_executor_binding=source_executor_binding,
        source_execution_sets=ordered_sets,
        receipts=ordered_receipts,
        authority_sha256=hash_hex_v1("miniqmt_vnpy_facade_characterization_authority_v2", authority_payload),
    )


def _characterization_receipts_v2(
    *,
    source_manifest: VnpyFacadeSourceManifestV1,
    facade_contract: VnpyFacadeContractV1,
    requirements: tuple[VnpyFacadeCharacterizationRequirementV1, ...],
    ordered_vectors: tuple[VnpyFacadeCharacterizationVectorV2, ...],
    source_executor_binding: VnpyFacadeSourceExecutorBindingV1,
    execution_sets: tuple[VnpyFacadeSourceExecutionSetV1, ...],
) -> tuple[VnpyFacadeAlgorithmCharacterizationReceiptV2, ...]:
    requirements_by_algo = {item.algo_code: item for item in requirements}
    execution_by_algo = {item.algo_code: item for item in execution_sets}
    source_by_algo = {
        item.algo_code_or_helper_name: item
        for item in source_manifest.ordered_sources
        if item.source_role is VnpyFacadeSourceRoleV1.ALGORITHM
    }
    state_mappings = build_vnpy_facade_state_mappings_v1()
    terminal_mappings = build_vnpy_facade_terminal_mappings_v1()
    receipts: list[VnpyFacadeAlgorithmCharacterizationReceiptV2] = []
    for algo_code in sorted(requirements_by_algo):
        requirement = requirements_by_algo[algo_code]
        execution_set = execution_by_algo[algo_code]
        vectors = tuple(item for item in ordered_vectors if item.algo_code == algo_code)
        if not vectors:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
                "one algorithm characterization receipt requires at least one executable vector",
                context={"algo_code": algo_code},
            )
        # The receipt retains one deterministic factory probe while the full
        # (potentially multi-config) characterization matrix remains bound by
        # vector_set_sha256.  Every vector config has already passed the exact
        # requirement schema in the source executor.
        config = thaw_json_v1(vectors[0].canonical_config)
        vector_set_sha = hash_hex_v1(
            "miniqmt_vnpy_facade_characterization_vector_set_v1",
            [item.canonical_payload_v1() for item in vectors],
        )
        if (
            execution_set.status is not VnpyFacadeCompatibilityStatusV1.PASSED
            or execution_set.characterization_requirement_sha256 != requirement.requirement_sha256
            or execution_set.vector_set_sha256 != vector_set_sha
            or tuple(item.vector_id for item in execution_set.ordered_results)
            != tuple(item.vector_id for item in vectors)
        ):
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
                "source execution set does not close to its exact requirement/vector authority",
                context={"algo_code": algo_code, "execution_set_sha256": execution_set.execution_set_sha256},
            )
        receipts.append(
            VnpyFacadeAlgorithmCharacterizationReceiptV2.create(
                algo_code=algo_code,
                source_identity_sha256=source_by_algo[algo_code].source_identity_sha256,
                facade_source_manifest_sha256=source_manifest.manifest_sha256,
                characterization_requirement_sha256=requirement.requirement_sha256,
                canonical_factory_probe_config=config,
                factory_probe_config_sha256=hash_hex_v1("miniqmt_vnpy_facade_factory_probe_config_v1", config),
                facade_contract_sha256=facade_contract.facade_contract_sha256,
                implementation_binding_set_sha256=facade_contract.implementation_binding_set_sha256,
                dto_mapping_set_sha256=facade_contract.dto_mapping_set_sha256,
                state_mapping_set_sha256=state_mapping_set_sha256_v1(
                    tuple(item for item in state_mappings if item.algo_code == algo_code)
                ),
                terminal_mapping_set_sha256=terminal_mapping_set_sha256_v1(
                    tuple(item for item in terminal_mappings if item.algo_code == algo_code)
                ),
                isolated_module_binding_set_sha256=facade_contract.isolated_module_binding_set_sha256,
                source_executor_binding_sha256=source_executor_binding.binding_sha256,
                source_execution_set_sha256=execution_set.execution_set_sha256,
                ordered_vector_ids=tuple(item.vector_id for item in vectors),
                vector_set_sha256=vector_set_sha,
                status=VnpyFacadeCompatibilityStatusV1.PASSED,
                ordered_failures=(),
            )
        )
    return tuple(receipts)


def build_vnpy_facade_characterization_authority_v2(
    *,
    source_manifest: VnpyFacadeSourceManifestV1,
    facade_contract: VnpyFacadeContractV1,
    requirements: tuple[VnpyFacadeCharacterizationRequirementV1, ...],
    ordered_vectors: tuple[VnpyFacadeCharacterizationVectorV2, ...],
    source_executor_binding: VnpyFacadeSourceExecutorBindingV1,
    source_execution_sets: tuple[VnpyFacadeSourceExecutionSetV1, ...],
) -> VnpyFacadeCharacterizationAuthorityV2:
    if not isinstance(source_executor_binding, VnpyFacadeSourceExecutorBindingV1):
        raise TypeError("source_executor_binding must be VnpyFacadeSourceExecutorBindingV1")
    execution_sets = tuple(
        VnpyFacadeSourceExecutionSetV1.model_validate(item.model_dump(mode="python"), strict=True)
        for item in source_execution_sets
    )
    receipts = _characterization_receipts_v2(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=requirements,
        ordered_vectors=ordered_vectors,
        source_executor_binding=source_executor_binding,
        execution_sets=execution_sets,
    )
    return _seal_characterization_authority_v2(
        source_executor_binding=source_executor_binding,
        source_execution_sets=execution_sets,
        receipts=receipts,
    )


def validate_vnpy_facade_characterization_authority_v2(
    *,
    receipts: tuple[VnpyFacadeAlgorithmCharacterizationReceiptV2, ...],
    source_manifest: VnpyFacadeSourceManifestV1,
    facade_contract: VnpyFacadeContractV1,
    requirements: tuple[VnpyFacadeCharacterizationRequirementV1, ...],
    ordered_vectors: tuple[VnpyFacadeCharacterizationVectorV2, ...],
    source_executor_binding: VnpyFacadeSourceExecutorBindingV1,
    source_execution_sets: tuple[VnpyFacadeSourceExecutionSetV1, ...],
) -> VnpyFacadeCharacterizationAuthorityV2:
    authoritative = build_vnpy_facade_characterization_authority_v2(
        source_manifest=source_manifest,
        facade_contract=facade_contract,
        requirements=requirements,
        ordered_vectors=ordered_vectors,
        source_executor_binding=source_executor_binding,
        source_execution_sets=source_execution_sets,
    )
    supplied = tuple(
        VnpyFacadeAlgorithmCharacterizationReceiptV2.model_validate(item, strict=True) for item in receipts
    )
    if supplied != authoritative.receipts:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
            "V2 characterization receipt readback differs from a fresh five-algorithm execution",
            context={
                "expected": [item.receipt_sha256 for item in authoritative.receipts],
                "actual": [item.receipt_sha256 for item in supplied],
            },
        )
    return authoritative


def build_vnpy_facade_algorithm_bindings_v2(
    *,
    characterization_authority_v2: VnpyFacadeCharacterizationAuthorityV2,
    facade_contract: VnpyFacadeContractV1,
    source_manifest: VnpyFacadeSourceManifestV1,
    source_root: Path = PINNED_SOURCE_ROOT,
) -> tuple[VnpyFacadeAlgorithmBindingV2, ...]:
    if not isinstance(characterization_authority_v2, VnpyFacadeCharacterizationAuthorityV2):
        raise TypeError("characterization_authority_v2 must be sealed VnpyFacadeCharacterizationAuthorityV2")
    if not isinstance(facade_contract, VnpyFacadeContractV1):
        raise TypeError("facade_contract must be VnpyFacadeContractV1")
    if not isinstance(source_manifest, VnpyFacadeSourceManifestV1):
        raise TypeError("source_manifest must be VnpyFacadeSourceManifestV1")
    source_by_algo = {
        item.algo_code_or_helper_name: item
        for item in source_manifest.ordered_sources
        if item.source_role is VnpyFacadeSourceRoleV1.ALGORITHM
    }
    spec_by_algo = {item.name: item for item in _SPECS if item.role is VnpyFacadeSourceRoleV1.ALGORITHM}
    state_mappings = build_vnpy_facade_state_mappings_v1(source_root=source_root)
    terminal_mappings = build_vnpy_facade_terminal_mappings_v1(source_root=source_root)
    bindings: list[VnpyFacadeAlgorithmBindingV2] = []
    for receipt in characterization_authority_v2.receipts:
        spec = spec_by_algo[receipt.algo_code]
        source = _validated_source_bytes(source_root, spec).decode("utf-8")
        tree = ast.parse(source, filename=spec.path)
        classes = [item for item in tree.body if isinstance(item, ast.ClassDef) and item.name == spec.class_or_function]
        if len(classes) != 1:
            raise _source_error(
                "algorithm binding requires one exact pinned class",
                algo_code=receipt.algo_code,
                class_name=spec.class_or_function,
            )
        constructors = [
            item
            for item in classes[0].body
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == "__init__"
        ]
        if len(constructors) != 1:
            raise _source_error(
                "algorithm binding requires one exact constructor",
                algo_code=receipt.algo_code,
            )
        constructor = constructors[0]
        signature_payload = {
            "posonly": [item.arg for item in constructor.args.posonlyargs],
            "args": [item.arg for item in constructor.args.args],
            "vararg": None if constructor.args.vararg is None else constructor.args.vararg.arg,
            "kwonly": [item.arg for item in constructor.args.kwonlyargs],
            "kwarg": None if constructor.args.kwarg is None else constructor.args.kwarg.arg,
            "defaults": [_stable_ast_dump_v1(item) for item in constructor.args.defaults],
            "kw_defaults": [
                None if item is None else _stable_ast_dump_v1(item) for item in constructor.args.kw_defaults
            ],
            "returns": None if constructor.returns is None else _stable_ast_dump_v1(constructor.returns),
        }
        execution_set = characterization_authority_v2.execution_set_for_algo_v2(receipt.algo_code)
        bindings.append(
            VnpyFacadeAlgorithmBindingV2.create(
                algo_code=receipt.algo_code,
                source_identity_sha256=source_by_algo[receipt.algo_code].source_identity_sha256,
                class_ref=f"vnpy_algotrading.algos.{Path(spec.path).stem}:{spec.class_or_function}",
                constructor_signature_sha256=hash_hex_v1(
                    "miniqmt_vnpy_facade_constructor_signature_v1", signature_payload
                ),
                constructor_body_sha256=hash_hex_v1(
                    "miniqmt_vnpy_facade_constructor_body_v1",
                    [_stable_ast_dump_v1(item) for item in constructor.body],
                ),
                state_mapping_set_sha256=state_mapping_set_sha256_v1(
                    tuple(item for item in state_mappings if item.algo_code == receipt.algo_code)
                ),
                terminal_mapping_set_sha256=terminal_mapping_set_sha256_v1(
                    tuple(item for item in terminal_mappings if item.algo_code == receipt.algo_code)
                ),
                characterization_receipt_sha256=receipt.receipt_sha256,
                adapter_contract_sha256=facade_contract.facade_contract_sha256,
                source_executor_binding_sha256=characterization_authority_v2.source_executor_binding.binding_sha256,
                source_execution_set_sha256=execution_set.execution_set_sha256,
            )
        )
    return tuple(sorted(bindings, key=lambda item: item.algo_code))


def _probe_catalog_factory_v2(
    *,
    catalog_runtime: PluginCatalogRuntimeV2,
    descriptor: Any,
    characterization: VnpyFacadeAlgorithmCharacterizationReceiptV2,
    binding: VnpyFacadeAlgorithmBindingV2,
    facade_backed: bool,
) -> None:
    """Execute the catalog-bound validator and factory before PASSED publication."""

    validator = catalog_runtime.process_bindings.resolve(descriptor.config_validator_binding_id)
    factory = catalog_runtime.process_bindings.resolve(descriptor.factory_binding_id)
    if validator is None or factory is None:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
            "conformance factory probe requires exact catalog process bindings",
            context={
                "plugin_key": descriptor.plugin_key.canonical_payload_v1(),
                "factory_binding_id": descriptor.factory_binding_id,
                "config_validator_binding_id": descriptor.config_validator_binding_id,
                "factory_present": factory is not None,
                "config_validator_present": validator is not None,
            },
        )
    probe_config = thaw_json_v1(characterization.canonical_factory_probe_config)
    try:
        validated = validator(descriptor.manifest, probe_config)
        if thaw_json_v1(validated) != probe_config:
            raise ValueError("catalog-bound config validator changed the canonical probe config")
        first = factory(probe_config)
        second = factory(probe_config)
    except VnpyFacadeContractError:
        raise
    except Exception as exc:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
            "catalog-bound factory probe failed",
            context={
                "plugin_key": descriptor.plugin_key.canonical_payload_v1(),
                "factory_binding_id": descriptor.factory_binding_id,
                "error": json_safe_evidence_v1(exc),
            },
        ) from exc
    if first is second:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
            "catalog-bound factory returned shared mutable plugin state",
            context={"plugin_key": descriptor.plugin_key.canonical_payload_v1()},
        )
    if facade_backed:
        valid = type(first) is VnpyFacadeBackedPluginAdapterV1 and type(second) is VnpyFacadeBackedPluginAdapterV1
        if valid:
            first_binding, first_class_ref = first.conformance_runtime_binding_readback_v1()
            second_binding, second_class_ref = second.conformance_runtime_binding_readback_v1()
            valid = (
                first.manifest == descriptor.manifest
                and second.manifest == descriptor.manifest
                and first_binding == binding
                and second_binding == binding
                and first_class_ref == binding.class_ref
                and second_class_ref == binding.class_ref
            )
    else:
        valid = (
            isinstance(first, ExecutionAlgoPluginV2)
            and isinstance(second, ExecutionAlgoPluginV2)
            and not isinstance(first, VnpyFacadeBackedPluginAdapterV1)
            and not isinstance(second, VnpyFacadeBackedPluginAdapterV1)
            and first.manifest == descriptor.manifest
            and second.manifest == descriptor.manifest
        )
    if not valid:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_BINDING_INVALID",
            "catalog-bound factory probe does not close over its exact runtime disposition",
            context={
                "plugin_key": descriptor.plugin_key.canonical_payload_v1(),
                "facade_backed": facade_backed,
                "first_type": f"{type(first).__module__}.{type(first).__qualname__}",
                "second_type": f"{type(second).__module__}.{type(second).__qualname__}",
            },
        )


def _build_vnpy_facade_conformance_set_v2(
    *,
    catalog_runtime: PluginCatalogRuntimeV2,
    gateway_catalog: GatewayCapabilityCatalogV1,
    facade_contract: VnpyFacadeContractV1,
    source_manifest: VnpyFacadeSourceManifestV1,
    characterization_authority_v2: VnpyFacadeCharacterizationAuthorityV2,
    algorithm_bindings_v2: tuple[VnpyFacadeAlgorithmBindingV2, ...],
    expected_algo_codes: tuple[str, ...],
    facade_backed_algo_codes: frozenset[str],
) -> VnpyFacadeConformanceSetV2:
    if not isinstance(catalog_runtime, PluginCatalogRuntimeV2):
        raise TypeError("catalog_runtime must be PluginCatalogRuntimeV2")
    strict_gateway = GatewayCapabilityCatalogV1.model_validate(gateway_catalog.model_dump(mode="python"), strict=True)
    if not isinstance(characterization_authority_v2, VnpyFacadeCharacterizationAuthorityV2):
        raise TypeError("characterization_authority_v2 must be sealed V2 characterization authority")
    if (
        type(expected_algo_codes) is not tuple
        or not expected_algo_codes
        or tuple(sorted(expected_algo_codes)) != expected_algo_codes
        or len(expected_algo_codes) != len(set(expected_algo_codes))
        or not facade_backed_algo_codes.issubset(set(expected_algo_codes))
    ):
        raise TypeError("conformance evaluator expected algorithm/disposition authority is invalid")
    bindings = tuple(sorted(algorithm_bindings_v2, key=lambda item: item.algo_code))
    if len(bindings) != len({item.algo_code for item in bindings}):
        raise ValueError("algorithm_bindings_v2 must be unique by algo_code")
    binding_by_algo = {item.algo_code: item for item in bindings}
    snapshot = catalog_runtime.snapshot
    descriptors = tuple(item for item in snapshot.registration_descriptors if item.manifest.required_facade_methods)
    actual_algo_codes = tuple(sorted(item.manifest.algo_code for item in descriptors))
    if actual_algo_codes != expected_algo_codes:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID",
            "facade descriptor set differs from its exact conformance authority",
            context={"expected_algo_codes": list(expected_algo_codes), "actual_algo_codes": list(actual_algo_codes)},
        )
    k1_by_key = {item.plugin_key: item for item in snapshot.pinned_compatibility_receipts}
    receipts: list[VnpyFacadeConformanceReceiptV2] = []
    build_items: list[VnpyFacadeConformanceBuildItemV2] = []
    for descriptor in sorted(descriptors, key=lambda item: item.plugin_key.sort_key_v1()):
        manifest = descriptor.manifest
        plugin_key = descriptor.plugin_key
        k1 = k1_by_key.get(plugin_key)
        binding = binding_by_algo.get(manifest.algo_code)
        characterization = characterization_authority_v2.receipt_for_algo_v2(manifest.algo_code)
        execution_set = characterization_authority_v2.execution_set_for_algo_v2(manifest.algo_code)
        if k1 is None or binding is None:
            raise VnpyFacadeContractError(
                "MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID",
                "current-three descriptor lacks K1 or K4 algorithm authority",
                context={"plugin_key": plugin_key.canonical_payload_v1()},
            )
        _probe_catalog_factory_v2(
            catalog_runtime=catalog_runtime,
            descriptor=descriptor,
            characterization=characterization,
            binding=binding,
            facade_backed=manifest.algo_code in facade_backed_algo_codes,
        )
        route = PluginRouteCompatibilityReceiptV1.create(
            catalog_snapshot=snapshot,
            plugin_key=plugin_key,
            gateway_catalog=strict_gateway,
        ).validate_against_authority_v1(
            catalog_snapshot=snapshot,
            gateway_catalog=strict_gateway,
        )
        failures: tuple[VnpyFacadeConformanceFailureV1, ...] = ()
        if route.status is not CompatibilityStatusV1.PASSED:
            failures = tuple(
                VnpyFacadeConformanceFailureV1.create(
                    field_path=f"route.{item.field_path}",
                    reason_code=item.reason_code,
                    context=thaw_json_v1(item.context),
                )
                for item in route.ordered_failures
            )
        runtime_disposition = (
            VnpyFacadeRuntimeBindingDispositionV1.FACADE_BACKED_ADAPTER
            if manifest.algo_code in facade_backed_algo_codes
            else VnpyFacadeRuntimeBindingDispositionV1.PURE_PLUGIN_SHADOW_CONFORMANCE
        )
        command_disposition = (
            VnpyFacadeCommandAuthorityDispositionV1.SHADOW_ONLY_K2_V1
            if manifest.algo_code in facade_backed_algo_codes
            else VnpyFacadeCommandAuthorityDispositionV1.NOT_APPLICABLE_PURE_PLUGIN
        )
        receipt = VnpyFacadeConformanceReceiptV2.create(
            plugin_id=manifest.plugin_id,
            plugin_version=manifest.plugin_version,
            algo_code=manifest.algo_code,
            manifest_sha256=manifest.manifest_sha256,
            runtime_binding_disposition=runtime_disposition,
            command_authority_disposition=command_disposition,
            pinned_compatibility_receipt_sha256=k1.receipt_sha256,
            requirement_sha256=k1.requirement_sha256,
            surface_sha256=k1.surface_sha256,
            source_lock_sha256=k1.source_lock_sha256,
            method_signature_sha256=k1.method_signature_sha256,
            object_field_sha256=k1.object_field_sha256,
            characterization_sha256=k1.characterization_sha256,
            facade_contract_sha256=facade_contract.facade_contract_sha256,
            implementation_binding_set_sha256=facade_contract.implementation_binding_set_sha256,
            method_contract_set_sha256=facade_contract.method_contract_set_sha256,
            dto_mapping_set_sha256=facade_contract.dto_mapping_set_sha256,
            state_mapping_set_sha256=binding.state_mapping_set_sha256,
            terminal_mapping_set_sha256=binding.terminal_mapping_set_sha256,
            isolated_module_binding_set_sha256=facade_contract.isolated_module_binding_set_sha256,
            facade_source_manifest_sha256=source_manifest.manifest_sha256,
            source_executor_binding_sha256=characterization_authority_v2.source_executor_binding.binding_sha256,
            source_execution_set_sha256=execution_set.execution_set_sha256,
            algorithm_characterization_receipt_v2_sha256=characterization.receipt_sha256,
            algorithm_binding_sha256=binding.binding_sha256,
            status=(VnpyFacadeCompatibilityStatusV1.FAILED if failures else VnpyFacadeCompatibilityStatusV1.PASSED),
            ordered_failures=failures,
        )
        receipts.append(receipt)
        build_items.append(
            VnpyFacadeConformanceBuildItemV2.create(
                plugin_key=plugin_key.canonical_payload_v1(),
                registration_descriptor_full_payload=descriptor.canonical_payload_v1(),
                pinned_compatibility_receipt_sha256=k1.receipt_sha256,
                source_executor_binding_sha256=characterization_authority_v2.source_executor_binding.binding_sha256,
                source_execution_set_sha256=execution_set.execution_set_sha256,
                algorithm_characterization_receipt_v2_sha256=characterization.receipt_sha256,
                algorithm_binding_sha256=binding.binding_sha256,
                runtime_binding_disposition=runtime_disposition,
                command_authority_disposition=command_disposition,
            )
        )
    if any(item.status is not VnpyFacadeCompatibilityStatusV1.PASSED for item in receipts):
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID",
            "facade conformance contains route or component failures; no partial set is published",
            context={
                "failed_plugins": [
                    item.plugin_id for item in receipts if item.status is not VnpyFacadeCompatibilityStatusV1.PASSED
                ]
            },
        )
    selected_set_hashes = tuple(
        characterization_authority_v2.execution_set_for_algo_v2(item.manifest.algo_code).execution_set_sha256
        for item in descriptors
    )
    return VnpyFacadeConformanceSetV2.create(
        plugin_catalog_sha256=snapshot.catalog_sha256,
        facade_contract_sha256=facade_contract.facade_contract_sha256,
        dto_mapping_set_sha256=facade_contract.dto_mapping_set_sha256,
        state_mapping_set_sha256=facade_contract.state_mapping_set_sha256,
        terminal_mapping_set_sha256=facade_contract.terminal_mapping_set_sha256,
        isolated_module_binding_set_sha256=facade_contract.isolated_module_binding_set_sha256,
        facade_source_manifest_sha256=source_manifest.manifest_sha256,
        source_executor_binding_sha256=characterization_authority_v2.source_executor_binding.binding_sha256,
        ordered_source_execution_set_sha256s=selected_set_hashes,
        ordered_receipts=tuple(receipts),
        build_items=tuple(build_items),
    )


def build_vnpy_facade_conformance_set_v2(
    *,
    catalog_runtime: PluginCatalogRuntimeV2,
    gateway_catalog: GatewayCapabilityCatalogV1,
    facade_contract: VnpyFacadeContractV1,
    source_manifest: VnpyFacadeSourceManifestV1,
    characterization_authority_v2: VnpyFacadeCharacterizationAuthorityV2,
    algorithm_bindings_v2: tuple[VnpyFacadeAlgorithmBindingV2, ...],
) -> VnpyFacadeConformanceSetV2:
    """K4 writer: retain the exact current-three pure-plugin semantics."""

    return _build_vnpy_facade_conformance_set_v2(
        catalog_runtime=catalog_runtime,
        gateway_catalog=gateway_catalog,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization_authority_v2,
        algorithm_bindings_v2=algorithm_bindings_v2,
        expected_algo_codes=("BEST_LIMIT_MINIQMT", "SNIPER_MINIQMT", "TWAP_LITE_MINIQMT"),
        facade_backed_algo_codes=frozenset(),
    )


def build_vnpy_facade_full_five_conformance_set_v2(
    *,
    catalog_runtime: PluginCatalogRuntimeV2,
    gateway_catalog: GatewayCapabilityCatalogV1,
    facade_contract: VnpyFacadeContractV1,
    source_manifest: VnpyFacadeSourceManifestV1,
    characterization_authority_v2: VnpyFacadeCharacterizationAuthorityV2,
    algorithm_bindings_v2: tuple[VnpyFacadeAlgorithmBindingV2, ...],
) -> VnpyFacadeConformanceSetV2:
    """K5 writer: one full-five set with only Iceberg and Stop facade-backed."""

    return _build_vnpy_facade_conformance_set_v2(
        catalog_runtime=catalog_runtime,
        gateway_catalog=gateway_catalog,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization_authority_v2,
        algorithm_bindings_v2=algorithm_bindings_v2,
        expected_algo_codes=("BEST_LIMIT_MINIQMT", "ICEBERG", "SNIPER_MINIQMT", "STOP", "TWAP_LITE_MINIQMT"),
        facade_backed_algo_codes=frozenset({"ICEBERG", "STOP"}),
    )


def _validate_vnpy_facade_conformance_set_against_authority_v2(
    *,
    conformance_set: VnpyFacadeConformanceSetV2,
    catalog_runtime: PluginCatalogRuntimeV2,
    gateway_catalog: GatewayCapabilityCatalogV1,
    facade_contract: VnpyFacadeContractV1,
    source_manifest: VnpyFacadeSourceManifestV1,
    characterization_authority_v2: VnpyFacadeCharacterizationAuthorityV2,
    expected_algo_codes: tuple[str, ...],
    facade_backed_algo_codes: frozenset[str],
) -> VnpyFacadeConformanceAuthorityV2:
    supplied = VnpyFacadeConformanceSetV2.model_validate(conformance_set.model_dump(mode="python"), strict=True)
    if not isinstance(characterization_authority_v2, VnpyFacadeCharacterizationAuthorityV2):
        raise TypeError("characterization_authority_v2 must be sealed VnpyFacadeCharacterizationAuthorityV2")
    characterization = characterization_authority_v2
    bindings = build_vnpy_facade_algorithm_bindings_v2(
        characterization_authority_v2=characterization,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
    )
    expected = _build_vnpy_facade_conformance_set_v2(
        catalog_runtime=catalog_runtime,
        gateway_catalog=gateway_catalog,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization,
        algorithm_bindings_v2=bindings,
        expected_algo_codes=expected_algo_codes,
        facade_backed_algo_codes=facade_backed_algo_codes,
    )
    if supplied != expected:
        raise VnpyFacadeContractError(
            "MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID",
            "conformance set differs from fresh source/catalog/gateway authority reconstruction",
            context={
                "expected_receipt_set_sha256": expected.receipt_set_sha256,
                "actual_receipt_set_sha256": supplied.receipt_set_sha256,
            },
        )
    validation_input = {
        "conformance_set_v2_sha256": expected.receipt_set_sha256,
        "source_executor_binding_sha256": characterization.source_executor_binding.binding_sha256,
        "ordered_source_execution_set_sha256s": [
            item.execution_set_sha256 for item in characterization.source_execution_sets
        ],
        "characterization_authority_sha256": characterization.authority_sha256,
        "plugin_catalog_sha256": catalog_runtime.snapshot.catalog_sha256,
        "gateway_catalog_sha256": gateway_catalog.catalog_sha256,
    }
    validation_receipt = VnpyFacadeConformanceAuthorityValidationReceiptV2.create(
        conformance_set_v2_sha256=expected.receipt_set_sha256,
        source_executor_binding_sha256=characterization.source_executor_binding.binding_sha256,
        ordered_source_execution_set_sha256s=tuple(
            item.execution_set_sha256 for item in characterization.source_execution_sets
        ),
        validation_input_sha256=hash_hex_v1(
            "miniqmt_vnpy_facade_conformance_authority_validation_input_v2",
            validation_input,
        ),
        status=VnpyFacadeCompatibilityStatusV1.PASSED,
        ordered_failures=(),
    )
    return _seal_vnpy_facade_conformance_authority_v2(
        conformance_set=expected,
        source_executor_binding=characterization.source_executor_binding,
        source_execution_sets=characterization.source_execution_sets,
        characterization_receipts=characterization.receipts,
        algorithm_bindings=bindings,
        validation_receipt=validation_receipt,
    )


def validate_vnpy_facade_conformance_set_against_authority_v2(
    *,
    conformance_set: VnpyFacadeConformanceSetV2,
    catalog_runtime: PluginCatalogRuntimeV2,
    gateway_catalog: GatewayCapabilityCatalogV1,
    facade_contract: VnpyFacadeContractV1,
    source_manifest: VnpyFacadeSourceManifestV1,
    characterization_authority_v2: VnpyFacadeCharacterizationAuthorityV2,
) -> VnpyFacadeConformanceAuthorityV2:
    """K4 readback: retain the exact current-three pure-plugin authority."""

    return _validate_vnpy_facade_conformance_set_against_authority_v2(
        conformance_set=conformance_set,
        catalog_runtime=catalog_runtime,
        gateway_catalog=gateway_catalog,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization_authority_v2,
        expected_algo_codes=("BEST_LIMIT_MINIQMT", "SNIPER_MINIQMT", "TWAP_LITE_MINIQMT"),
        facade_backed_algo_codes=frozenset(),
    )


def validate_vnpy_facade_full_five_conformance_set_against_authority_v2(
    *,
    conformance_set: VnpyFacadeConformanceSetV2,
    catalog_runtime: PluginCatalogRuntimeV2,
    gateway_catalog: GatewayCapabilityCatalogV1,
    facade_contract: VnpyFacadeContractV1,
    source_manifest: VnpyFacadeSourceManifestV1,
    characterization_authority_v2: VnpyFacadeCharacterizationAuthorityV2,
) -> VnpyFacadeConformanceAuthorityV2:
    """K5 readback: strict full-five source/catalog/gateway authority."""

    return _validate_vnpy_facade_conformance_set_against_authority_v2(
        conformance_set=conformance_set,
        catalog_runtime=catalog_runtime,
        gateway_catalog=gateway_catalog,
        facade_contract=facade_contract,
        source_manifest=source_manifest,
        characterization_authority_v2=characterization_authority_v2,
        expected_algo_codes=("BEST_LIMIT_MINIQMT", "ICEBERG", "SNIPER_MINIQMT", "STOP", "TWAP_LITE_MINIQMT"),
        facade_backed_algo_codes=frozenset({"ICEBERG", "STOP"}),
    )


__all__ = [
    "VnpyFacadeCharacterizationAuthorityV2",
    "VnpyFacadeDeterministicUniformV1",
    "build_vnpy_facade_source_manifest_v1",
    "build_vnpy_facade_algorithm_bindings_v1",
    "build_vnpy_facade_algorithm_bindings_v2",
    "build_vnpy_facade_characterization_authority_v2",
    "build_vnpy_facade_characterization_manifest_views_v1",
    "build_vnpy_facade_characterization_requirements_v1",
    "build_vnpy_facade_characterization_receipt_v1",
    "build_vnpy_facade_conformance_set_v1",
    "build_vnpy_facade_conformance_set_v2",
    "build_vnpy_facade_full_five_conformance_set_v2",
    "build_vnpy_facade_isolated_module_bindings_v1",
    "build_vnpy_facade_contract_v1",
    "build_vnpy_facade_implementation_bindings_v1",
    "build_vnpy_facade_method_contracts_v1",
    "build_vnpy_facade_state_mappings_v1",
    "build_vnpy_facade_terminal_mappings_v1",
    "load_pinned_vnpy_algorithm_classes_v1",
    "isolated_module_binding_set_sha256_v1",
    "readback_vnpy_facade_algorithm_bindings_v1",
    "readback_vnpy_facade_characterization_receipt_v1",
    "readback_vnpy_facade_conformance_set_v1",
    "readback_vnpy_facade_isolated_module_bindings_v1",
    "readback_vnpy_facade_contract_v1",
    "readback_vnpy_facade_implementation_bindings_v1",
    "readback_vnpy_facade_source_manifest_v1",
    "readback_vnpy_facade_state_mappings_v1",
    "readback_vnpy_facade_terminal_mappings_v1",
    "validate_vnpy_facade_characterization_authority_v2",
    "validate_vnpy_facade_conformance_set_against_authority_v2",
    "validate_vnpy_facade_full_five_conformance_set_against_authority_v2",
]
