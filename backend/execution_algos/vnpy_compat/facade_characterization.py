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
import math
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any

from .facade_contracts import (
    VnpyFacadeAlgorithmBindingV1,
    VnpyFacadeAlgorithmCharacterizationReceiptV1,
    VnpyFacadeCharacterizationRequirementV1,
    VnpyFacadeCharacterizationVectorV1,
    VnpyFacadeCompatibilityStatusV1,
    VnpyFacadeConformanceFailureV1,
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
    VnpyFacadeSourceManifestV1,
    VnpyFacadeSourceRoleV1,
    VnpyFacadeSourceV1,
    VnpyFacadeStateFieldMappingV1,
    VnpyFacadeTerminalMappingV1,
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
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import (
    PluginCatalogRuntimeV2,
    callable_ref_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    VnpyCompatibilityRequirementV2,
    compatibility_component_hashes_v2,
)

_ALGOTRADING_REPO = "https://github.com/vnpy/vnpy_algotrading"
_ALGOTRADING_COMMIT = "4133987530eb28f3538d1983545d81c4f83d7d59"
_ALGOTRADING_AUTHORITY = "ad72ed1dd243d45d41d3c476d9dd7fbf17f49e6efcb7c12739f8ae6982582541"
_CORE_REPO = "https://github.com/vnpy/vnpy"
_CORE_COMMIT = "1049acf64afd5b2d06d09b1e139dd0cca5d9d6b9"
_CORE_AUTHORITY = "8e73ba64d3d405c382ae3b8b8d1c2df334c809b3331828fa4d85b0d62ed00ad2"


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


def _safe_exception_evidence_v1(exc: Exception) -> dict[str, str]:
    try:
        message = str(exc)[:2048]
        repository_root = Path(__file__).resolve().parents[3]
        for spelling in (
            str(repository_root),
            str(repository_root).replace("\\", "\\\\"),
            repository_root.as_posix(),
        ):
            message = message.replace(spelling, "<repository_root>")
        render_error_type = "none"
    except Exception as render_error:  # pragma: no branch - protects primary failure evidence
        message = "<unavailable>"
        render_error_type = f"{type(render_error).__module__}.{type(render_error).__qualname__}"
    return {
        "exception_type": f"{type(exc).__module__}.{type(exc).__qualname__}",
        "exception_message": message,
        "message_render_error_type": render_error_type,
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
            result[target.attr] = (annotation, ast.dump(node.value, include_attributes=False))
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


def _facade_callable_signature_sha256_v1(value: Any, *, root: Path) -> str:
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
    return hash_hex_v1(
        "miniqmt_vnpy_facade_callable_signature_v1",
        {
            "parameters": parameters,
            "return_annotation": _annotation_token_v1(signature.return_annotation),
        },
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
            "signature": ast.dump(method.args, annotate_fields=True, include_attributes=False),
            "body": [ast.dump(item, annotate_fields=True, include_attributes=False) for item in method.body],
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


__all__ = [
    "VnpyFacadeDeterministicUniformV1",
    "build_vnpy_facade_source_manifest_v1",
    "build_vnpy_facade_algorithm_bindings_v1",
    "build_vnpy_facade_characterization_receipt_v1",
    "build_vnpy_facade_conformance_set_v1",
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
]
