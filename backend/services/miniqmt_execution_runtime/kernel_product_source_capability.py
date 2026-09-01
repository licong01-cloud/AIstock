"""Fresh-process source identity and legacy-route retirement authority for K6-D."""

from __future__ import annotations

import ast
from enum import StrEnum
import hashlib
from pathlib import Path
import subprocess
from typing import Any, Literal

from pydantic import model_validator

from .kernel_delivery import KernelAlgoCreationRequestV2
from .kernel_product_contracts import ProductRouteCutoverReceiptV1, ProductRouteOwnerV1
from .plugin_canonical import hash_hex_v1
from .plugin_contracts import FrozenStrictModel, IdentityV1, Sha256V1


class K6DSourceCapabilityError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = {**context, "broker_called": False}
        super().__init__(message)


class K6DRetirementDispositionV1(StrEnum):
    REMOVED = "REMOVED"
    NON_PRODUCT_TEST_ADAPTER = "NON_PRODUCT_TEST_ADAPTER"


class K6DRetirementSourceItemV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_k6d_retirement_source_item_v1"] = "miniqmt_k6d_retirement_source_item_v1"
    module_path: IdentityV1
    symbol: IdentityV1
    disposition: K6DRetirementDispositionV1
    product_callers_before: tuple[IdentityV1, ...]
    product_callers_after: tuple[IdentityV1, ...]
    broker_capability_refs_before: tuple[IdentityV1, ...]
    replacement_owner: IdentityV1
    source_sha256_before: Sha256V1
    source_sha256_after: Sha256V1
    test_refs: tuple[IdentityV1, ...]
    item_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> "K6DRetirementSourceItemV1":
        disposition = K6DRetirementDispositionV1(values["disposition"])
        product_callers_before = tuple(values["product_callers_before"])
        product_callers_after = tuple(values["product_callers_after"])
        broker_refs = tuple(values["broker_capability_refs_before"])
        test_refs = tuple(values["test_refs"])
        payload = {
            "schema_version": "miniqmt_k6d_retirement_source_item_v1",
            **values,
            "disposition": disposition.value,
            "product_callers_before": list(product_callers_before),
            "product_callers_after": list(product_callers_after),
            "broker_capability_refs_before": list(broker_refs),
            "test_refs": list(test_refs),
        }
        return cls(
            **{
                **payload,
                "disposition": disposition,
                "product_callers_before": product_callers_before,
                "product_callers_after": product_callers_after,
                "broker_capability_refs_before": broker_refs,
                "test_refs": test_refs,
            },
            item_sha256=hash_hex_v1("miniqmt_k6d_retirement_source_item_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_contract(self) -> "K6DRetirementSourceItemV1":
        if not self.product_callers_before or self.product_callers_after:
            raise ValueError("retirement item requires nonempty before and zero after product callers")
        for field_name in ("product_callers_before", "broker_capability_refs_before", "test_refs"):
            values = getattr(self, field_name)
            if tuple(sorted(values)) != values or len(values) != len(set(values)):
                raise ValueError(f"{field_name} must be sorted and unique")
        if not self.test_refs or len(self.test_refs) > 64 or len(self.product_callers_before) > 256:
            raise ValueError("retirement evidence cardinality is invalid")
        if not self.module_path.endswith(".py") or "\\" in self.module_path or self.module_path.startswith("/"):
            raise ValueError("module_path must be a repo-relative POSIX Python path")
        expected = hash_hex_v1(
            "miniqmt_k6d_retirement_source_item_v1",
            self.canonical_payload_v1(exclude={"item_sha256"}),
        )
        if self.item_sha256 != expected:
            raise ValueError("retirement source item hash mismatch")
        return self


class K6DRetirementInventoryReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_k6d_retirement_inventory_receipt_v1"] = (
        "miniqmt_k6d_retirement_inventory_receipt_v1"
    )
    ordered_source_items: tuple[K6DRetirementSourceItemV1, ...]
    total_item_count: int
    retained_item_count: int
    omitted_item_count: int
    omitted_set_sha256: Sha256V1 | None
    source_tree_sha256: Sha256V1
    receipt_sha256: Sha256V1

    @classmethod
    def create(
        cls, *, ordered_source_items: tuple[K6DRetirementSourceItemV1, ...], source_tree_sha256: str
    ) -> "K6DRetirementInventoryReceiptV1":
        items = tuple(sorted(ordered_source_items, key=lambda item: (item.module_path, item.symbol)))
        if len(items) > 256:
            raise ValueError("final K6-D source inventory cannot silently truncate")
        payload = {
            "schema_version": "miniqmt_k6d_retirement_inventory_receipt_v1",
            "ordered_source_items": [item.model_dump(mode="json") for item in items],
            "total_item_count": len(items),
            "retained_item_count": len(items),
            "omitted_item_count": 0,
            "omitted_set_sha256": None,
            "source_tree_sha256": source_tree_sha256,
        }
        return cls(
            **{**payload, "ordered_source_items": items},
            receipt_sha256=hash_hex_v1("miniqmt_k6d_retirement_inventory_receipt_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_contract(self) -> "K6DRetirementInventoryReceiptV1":
        keys = tuple((item.module_path, item.symbol) for item in self.ordered_source_items)
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise ValueError("retirement source inventory must be sorted and unique")
        if (
            self.total_item_count != len(self.ordered_source_items)
            or self.retained_item_count != self.total_item_count
            or self.omitted_item_count != 0
            or self.omitted_set_sha256 is not None
            or any(item.product_callers_after for item in self.ordered_source_items)
        ):
            raise ValueError("final source inventory is partial or still has a product caller")
        expected = hash_hex_v1(
            "miniqmt_k6d_retirement_inventory_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("retirement inventory receipt hash mismatch")
        return self


class K6DRouteSourceCapabilityV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_k6d_route_contract_v1"] = "miniqmt_k6d_route_contract_v1"
    git_source_revision: IdentityV1
    git_source_tree: IdentityV1
    product_coordinator_module: IdentityV1
    product_coordinator_symbol: IdentityV1
    product_coordinator_source_sha256: Sha256V1
    creation_request_schema_sha256: Sha256V1
    algo_start_v2_payload_schema_sha256: Sha256V1
    cutover_receipt_schema_sha256: Sha256V1
    route_owner_schema_sha256: Sha256V1
    retirement_inventory_receipt_sha256: Sha256V1
    ordered_legacy_product_zero_proofs: tuple[Sha256V1, ...]
    capability_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> "K6DRouteSourceCapabilityV1":
        proofs = tuple(values["ordered_legacy_product_zero_proofs"])
        payload = {
            "schema_version": "miniqmt_k6d_route_contract_v1",
            **values,
            "ordered_legacy_product_zero_proofs": list(proofs),
        }
        return cls(
            **{**payload, "ordered_legacy_product_zero_proofs": proofs},
            capability_sha256=hash_hex_v1("miniqmt_k6d_route_source_capability_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_contract(self) -> "K6DRouteSourceCapabilityV1":
        if len(self.git_source_revision) != 40 or len(self.git_source_tree) != 40:
            raise ValueError("K6-D capability requires exact Git revision and tree identities")
        if (
            not self.ordered_legacy_product_zero_proofs
            or tuple(sorted(self.ordered_legacy_product_zero_proofs)) != self.ordered_legacy_product_zero_proofs
            or len(set(self.ordered_legacy_product_zero_proofs)) != len(self.ordered_legacy_product_zero_proofs)
        ):
            raise ValueError("legacy product zero proofs must be nonempty, sorted and unique")
        expected = hash_hex_v1(
            "miniqmt_k6d_route_source_capability_v1",
            self.canonical_payload_v1(exclude={"capability_sha256"}),
        )
        if self.capability_sha256 != expected:
            raise ValueError("K6-D source capability hash mismatch")
        return self


_ROOT_SYMBOLS = (
    (
        "backend/services/simulation_runtime/lifecycle.py",
        "SimulationLifecycleOrchestrator.submit_persisted_execution_plan",
    ),
    (
        "backend/services/simulation_runtime/scheduler.py",
        "SimulationLifecycleScheduler._build_miniqmt_kernel_product_runtime",
    ),
    (
        "backend/services/simulation_runtime/scheduler.py",
        "SimulationLifecycleScheduler._advance_miniqmt_quote_ingress_lifecycle",
    ),
    ("backend/services/simulation_runtime/miniqmt_kernel_product.py", "build_simulation_miniqmt_product_runtime_v1"),
    ("backend/services/simulation_runtime/miniqmt_quote_activation.py", "MiniQMTQuoteIngressActivation.watchdog_tick"),
)
_FORBIDDEN_PRODUCT_CALLS = frozenset(
    {
        "submit_event_loop_plan",
        "drive_event_loop_ticks",
        "submit_event_loop_vnpy_parent_intents",
        "_event_loop_dependent_buy_retry_result",
        "_submit_deferred_dependent_buy",
        "_retry_dependent_buy_batch",
    }
)
_BEFORE_HASHES = {
    "backend/services/simulation_runtime/bridges.py": "d77bb8b5e428852695a1576fd0b41796053cbf1e7bd2d24bddcbe0bdbfb8c62f",
    "backend/services/simulation_runtime/scheduler.py": "6d894846e30accacec4f14bb6de9ec9ed884582a698aea2e160c647e522022e2",
    "backend/services/miniqmt_execution_runtime/client.py": "4ce2351b8be1b5faa0899cda335e80c394937d396f40e3bcd933af82f1c4c303",
    "backend/services/miniqmt_execution_runtime/runtime.py": "8131d0543ed0797b249ebc5e5af0f614fb7ce996bdb52cf5a215deaab150a837",
    "backend/services/qmt_strategy_ledger/order_service.py": "03d9782ecced3a4f429c526d3b64156ce2fbcf5be65d95b2835393556cd872a7",
    "backend/execution_algos/vnpy_style/legacy_adapter.py": "0b07342910f780f9b594e2bf85989cc0aa07743731e899ab86d422e62f3c363c",
}
_RETIREMENT_SPECS = (
    (
        "backend/services/simulation_runtime/bridges.py",
        "submit_event_loop_plan",
        K6DRetirementDispositionV1.REMOVED,
        "simulation_runtime.lifecycle",
        ("MiniQMTExecutionRuntimeClient",),
    ),
    (
        "backend/services/simulation_runtime/bridges.py",
        "drive_event_loop_ticks",
        K6DRetirementDispositionV1.REMOVED,
        "simulation_runtime.lifecycle",
        ("MiniQMTExecutionRuntimeClient",),
    ),
    (
        "backend/services/simulation_runtime/scheduler.py",
        "_drive_miniqmt_event_loop_ticks",
        K6DRetirementDispositionV1.REMOVED,
        "simulation_runtime.miniqmt_kernel_product",
        ("MiniQMTExecutionBridge",),
    ),
    (
        "backend/services/miniqmt_execution_runtime/client.py",
        "_event_loop_dependent_buy_retry_result",
        K6DRetirementDispositionV1.NON_PRODUCT_TEST_ADAPTER,
        "kernel_dependent_buy",
        ("QmtManagedOrderService",),
    ),
    (
        "backend/services/miniqmt_execution_runtime/runtime.py",
        "_defer_dependent_buy_action_if_needed",
        K6DRetirementDispositionV1.NON_PRODUCT_TEST_ADAPTER,
        "kernel_dependent_buy",
        ("MiniQMTGateway",),
    ),
    (
        "backend/services/miniqmt_execution_runtime/runtime.py",
        "_submit_deferred_dependent_buy",
        K6DRetirementDispositionV1.NON_PRODUCT_TEST_ADAPTER,
        "kernel_dependent_buy",
        ("MiniQMTGateway",),
    ),
    (
        "backend/services/qmt_strategy_ledger/order_service.py",
        "_retry_dependent_buy_batch",
        K6DRetirementDispositionV1.NON_PRODUCT_TEST_ADAPTER,
        "kernel_dependent_buy",
        ("QmtManagedOrderService",),
    ),
    (
        "backend/execution_algos/vnpy_style/legacy_adapter.py",
        "<module>",
        K6DRetirementDispositionV1.NON_PRODUCT_TEST_ADAPTER,
        "full_five_catalog_authority",
        ("legacy_registry",),
    ),
)


def build_k6d_route_source_capability_v1(repo_root: Path | None = None) -> K6DRouteSourceCapabilityV1:
    root = (repo_root or Path(__file__).resolve().parents[3]).resolve()
    revision = _git(root, "rev-parse", "HEAD")
    tree = _git(root, "rev-parse", "HEAD^{tree}")
    zero_proofs = tuple(sorted(_zero_product_call_proof(root, path, symbol) for path, symbol in _ROOT_SYMBOLS))
    source_tree_sha256 = hash_hex_v1(
        "miniqmt_k6d_source_tree_v1",
        [{"path": path, "sha256": _file_sha256(root / path)} for path in sorted(_BEFORE_HASHES)],
    )
    for path, symbol, disposition, _replacement, _refs in _RETIREMENT_SPECS:
        _validate_retirement_disposition(
            root=root,
            module_path=path,
            symbol=symbol,
            disposition=disposition,
        )
    _validate_legacy_adapter_not_package_registered(root)
    items = tuple(
        K6DRetirementSourceItemV1.create(
            module_path=path,
            symbol=symbol,
            disposition=disposition,
            product_callers_before=("pre_k6d_product_root",),
            product_callers_after=(),
            broker_capability_refs_before=tuple(sorted(refs)),
            replacement_owner=replacement,
            source_sha256_before=_BEFORE_HASHES[path],
            source_sha256_after=_file_sha256(root / path),
            test_refs=("backend/tests/miniqmt_execution_runtime/test_kernel_legacy_route_retirement.py",),
        )
        for path, symbol, disposition, replacement, refs in _RETIREMENT_SPECS
    )
    inventory = K6DRetirementInventoryReceiptV1.create(
        ordered_source_items=items,
        source_tree_sha256=source_tree_sha256,
    )
    coordinator_path = root / "backend/services/miniqmt_execution_runtime/kernel_product_runtime.py"
    algo_start_fields = (
        "binding_id",
        "creation_request_sha256",
        "effective_new_instance_sequence",
        "execution_plan_id",
        "execution_plan_sha256",
        "gateway_capability_catalog",
        "parent_intent_id",
        "plugin_catalog_sha256",
        "plugin_route_compatibility_receipt",
        "plugin_route_compatibility_receipt_sha256",
        "policy_id",
        "policy_sha256",
        "product_route_cutover_receipt_sha256",
        "product_route_epoch",
        "product_route_owner_sha256",
        "release_id",
        "release_sha256",
        "strategy_slot_id",
        "target_quantity",
    )
    return K6DRouteSourceCapabilityV1.create(
        git_source_revision=revision,
        git_source_tree=tree,
        product_coordinator_module="backend.services.miniqmt_execution_runtime.kernel_product_runtime",
        product_coordinator_symbol="MiniQMTKernelV2ProductCoordinator",
        product_coordinator_source_sha256=_file_sha256(coordinator_path),
        creation_request_schema_sha256=hash_hex_v1(
            "miniqmt_kernel_algo_creation_request_v2_schema_v1", KernelAlgoCreationRequestV2.model_json_schema()
        ),
        algo_start_v2_payload_schema_sha256=hash_hex_v1(
            "miniqmt_algo_start_v2_payload_schema_v1", list(algo_start_fields)
        ),
        cutover_receipt_schema_sha256=hash_hex_v1(
            "miniqmt_product_route_cutover_receipt_schema_v1", ProductRouteCutoverReceiptV1.model_json_schema()
        ),
        route_owner_schema_sha256=hash_hex_v1(
            "miniqmt_product_route_owner_schema_v1", ProductRouteOwnerV1.model_json_schema()
        ),
        retirement_inventory_receipt_sha256=inventory.receipt_sha256,
        ordered_legacy_product_zero_proofs=zero_proofs,
    )


def _zero_product_call_proof(root: Path, module_path: str, symbol: str) -> str:
    path = root / module_path
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=module_path)
    node: ast.AST = tree
    for part in symbol.split("."):
        candidates = [
            item
            for item in getattr(node, "body", ())
            if isinstance(item, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == part
        ]
        if len(candidates) != 1:
            raise K6DSourceCapabilityError(
                "MINIQMT_K6D_SOURCE_ROOT_MISSING",
                "K6-D product source root is missing or ambiguous",
                context={"module_path": module_path, "symbol": symbol, "missing_part": part},
            )
        node = candidates[0]
    matches = []
    for item in ast.walk(node):
        if not isinstance(item, ast.Call):
            continue
        name = (
            item.func.attr
            if isinstance(item.func, ast.Attribute)
            else item.func.id
            if isinstance(item.func, ast.Name)
            else None
        )
        if name in _FORBIDDEN_PRODUCT_CALLS:
            matches.append({"name": name, "line": item.lineno, "column": item.col_offset})
    if matches:
        raise K6DSourceCapabilityError(
            "MINIQMT_K6D_LEGACY_PRODUCT_CALL_REACHABLE",
            "K6-D product root still calls a retired product route",
            context={"module_path": module_path, "symbol": symbol, "ordered_matches": matches},
        )
    return hash_hex_v1(
        "miniqmt_k6d_legacy_product_zero_proof_v1",
        {"module_path": module_path, "symbol": symbol, "source_sha256": _file_sha256(path), "matches": []},
    )


def _validate_retirement_disposition(
    *,
    root: Path,
    module_path: str,
    symbol: str,
    disposition: K6DRetirementDispositionV1,
) -> None:
    path = root / module_path
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=module_path)
    count = (
        1
        if symbol == "<module>"
        else sum(
            1
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name == symbol
        )
    )
    expected = 0 if disposition is K6DRetirementDispositionV1.REMOVED else 1
    if count != expected:
        raise K6DSourceCapabilityError(
            "MINIQMT_K6D_RETIREMENT_DISPOSITION_INVALID",
            "legacy source symbol does not match its declared final disposition",
            context={
                "module_path": module_path,
                "symbol": symbol,
                "disposition": disposition.value,
                "expected_symbol_count": expected,
                "actual_symbol_count": count,
            },
        )


def _validate_legacy_adapter_not_package_registered(root: Path) -> None:
    module_path = "backend/execution_algos/__init__.py"
    tree = ast.parse((root / module_path).read_text(encoding="utf-8"), filename=module_path)
    matches: list[dict[str, Any]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and "vnpy_style" in node.module:
            if any(alias.name == "legacy_adapter" for alias in node.names):
                matches.append({"line": node.lineno, "module": node.module})
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.endswith("vnpy_style.legacy_adapter"):
                    matches.append({"line": node.lineno, "module": alias.name})
    if matches:
        raise K6DSourceCapabilityError(
            "MINIQMT_K6D_LEGACY_PACKAGE_REGISTRATION_REACHABLE",
            "execution algorithm package still imports the legacy MiniQMT adapter",
            context={"module_path": module_path, "ordered_matches": matches},
        )


def _file_sha256(path: Path) -> str:
    if not path.is_file():
        raise K6DSourceCapabilityError(
            "MINIQMT_K6D_SOURCE_FILE_MISSING",
            "K6-D source capability file is missing",
            context={"path": path.as_posix()},
        )
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *args),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    value = completed.stdout.strip()
    if completed.returncode != 0 or len(value) != 40:
        raise K6DSourceCapabilityError(
            "MINIQMT_K6D_GIT_SOURCE_IDENTITY_UNAVAILABLE",
            "K6-D Git source revision/tree could not be read",
            context={"args": list(args), "returncode": completed.returncode, "stderr": completed.stderr[:512]},
        )
    return value


__all__ = [
    "K6DRetirementDispositionV1",
    "K6DRetirementInventoryReceiptV1",
    "K6DRetirementSourceItemV1",
    "K6DRouteSourceCapabilityV1",
    "K6DSourceCapabilityError",
    "build_k6d_route_source_capability_v1",
]
