"""Single MiniQMT KERNEL_V2 product composition root for K6-D."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Any, Callable, Literal, Protocol

from pydantic import model_validator

from .kernel_creation import KernelAlgoCreationCoordinatorV2
from .kernel_delivery import (
    KernelAlgoCreationRequestV1,
    KernelAlgoCreationRequestV2,
    KernelProductDeliveryWorkerV3,
)
from .kernel_ingress import KernelIngressCoordinatorV1
from .kernel_product_contracts import ProductRouteOwnerV1
from .kernel_product_cutover import KernelProductCutoverCoordinator
from .plugin_canonical import hash_hex_v1
from .plugin_contracts import (
    FrozenStrictModel,
    IdentityV1,
    RuntimeEventEnvelopeV2,
    RuntimeEventIngressReceiptV1,
    KernelCallbackMappingUpdateV1,
    Sha256V1,
    safe_exception_summary_v1,
)


class K6DProductStartStatusV1(StrEnum):
    STARTED = "STARTED"
    FAILED_TERMINAL = "FAILED_TERMINAL"


class K6DProductDeliveryAggregateError(RuntimeError):
    """Expose every failed target after all independent deliveries were attempted."""

    def __init__(self, *, event: RuntimeEventEnvelopeV2, failures: tuple[dict[str, Any], ...]) -> None:
        self.reason_code = "MINIQMT_K6_PRODUCT_DELIVERY_AGGREGATE_FAILED"
        self.context = {
            "runtime_id": event.runtime_id,
            "event_id": event.event_id,
            "event_type": event.event_type.value,
            "failure_count": len(failures),
            "ordered_failures": failures,
            "broker_called": False,
        }
        super().__init__("one or more K6-D product deliveries failed after independent processing")


class K6DProductParentStartResultV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_k6d_product_parent_start_result_v1"] = "miniqmt_k6d_product_parent_start_result_v1"
    plan_intent_ordinal: int
    parent_intent_id: IdentityV1
    algo_instance_id: IdentityV1
    event_id: IdentityV1
    ingress_receipt_sha256: Sha256V1
    start_status: K6DProductStartStatusV1
    terminal_reason_or_null: IdentityV1 | None
    coordinator_broker_called: bool
    result_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> "K6DProductParentStartResultV1":
        status = K6DProductStartStatusV1(values["start_status"])
        payload = {
            "schema_version": "miniqmt_k6d_product_parent_start_result_v1",
            **values,
            "start_status": status.value,
            "coordinator_broker_called": False,
        }
        return cls(
            **{**payload, "start_status": status},
            result_sha256=hash_hex_v1("miniqmt_k6d_product_parent_start_result_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_contract(self) -> "K6DProductParentStartResultV1":
        if type(self.plan_intent_ordinal) is not int or self.plan_intent_ordinal <= 0:
            raise ValueError("plan_intent_ordinal must be a strict positive integer")
        if self.coordinator_broker_called is not False:
            raise ValueError("product plan-start coordinator cannot report a broker call")
        if (self.start_status is K6DProductStartStatusV1.STARTED) == (self.terminal_reason_or_null is not None):
            raise ValueError("product parent start status and terminal reason conflict")
        expected = hash_hex_v1(
            "miniqmt_k6d_product_parent_start_result_v1",
            self.canonical_payload_v1(exclude={"result_sha256"}),
        )
        if self.result_sha256 != expected:
            raise ValueError("product parent start result hash mismatch")
        return self


class K6DProductPlanStartReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_k6d_product_plan_start_receipt_v1"] = "miniqmt_k6d_product_plan_start_receipt_v1"
    runtime_id: IdentityV1
    binding_id: IdentityV1
    execution_plan_id: IdentityV1
    execution_plan_sha256: Sha256V1
    product_route_receipt_sha256: Sha256V1
    ordered_parent_results: tuple[K6DProductParentStartResultV1, ...]
    total: int
    started: int
    failed: int
    success: bool
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> "K6DProductPlanStartReceiptV1":
        results = tuple(values["ordered_parent_results"])
        started = sum(item.start_status is K6DProductStartStatusV1.STARTED for item in results)
        failed = len(results) - started
        payload = {
            "schema_version": "miniqmt_k6d_product_plan_start_receipt_v1",
            **values,
            "ordered_parent_results": [item.model_dump(mode="json") for item in results],
            "total": len(results),
            "started": started,
            "failed": failed,
            "success": bool(results) and failed == 0,
        }
        return cls(
            **{**payload, "ordered_parent_results": results},
            receipt_sha256=hash_hex_v1("miniqmt_k6d_product_plan_start_receipt_v1", payload),
        )

    @model_validator(mode="after")
    def _validate_contract(self) -> "K6DProductPlanStartReceiptV1":
        ordinals = tuple(item.plan_intent_ordinal for item in self.ordered_parent_results)
        parents = tuple(item.parent_intent_id for item in self.ordered_parent_results)
        if ordinals != tuple(range(1, len(ordinals) + 1)) or len(parents) != len(set(parents)):
            raise ValueError("product plan start results must retain canonical plan order and unique parents")
        actual_started = sum(
            item.start_status is K6DProductStartStatusV1.STARTED for item in self.ordered_parent_results
        )
        if (self.total, self.started, self.failed) != (
            len(self.ordered_parent_results),
            actual_started,
            len(self.ordered_parent_results) - actual_started,
        ) or self.success != (self.total > 0 and self.failed == 0 and self.started == self.total):
            raise ValueError("product plan start counts/status do not close to durable results")
        expected = hash_hex_v1(
            "miniqmt_k6d_product_plan_start_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("product plan start receipt hash mismatch")
        return self


@dataclass(frozen=True)
class K6DProductPlanAuthorityV1:
    runtime_id: str
    binding_id: str
    execution_plan_id: str
    execution_plan_sha256: str
    trade_date: date
    ordered_creation_requests: tuple[KernelAlgoCreationRequestV1, ...]

    def __post_init__(self) -> None:
        for name in ("runtime_id", "binding_id", "execution_plan_id"):
            value = getattr(self, name)
            if type(value) is not str or not value or value != value.strip():
                raise TypeError(f"{name} must be a canonical identity")
        if type(self.execution_plan_sha256) is not str or len(self.execution_plan_sha256) != 64:
            raise TypeError("execution_plan_sha256 must be SHA-256")
        if type(self.trade_date) is not date:
            raise TypeError("trade_date must be date")
        if type(self.ordered_creation_requests) is not tuple or any(
            type(item) is not KernelAlgoCreationRequestV1 for item in self.ordered_creation_requests
        ):
            raise TypeError("ordered_creation_requests must contain exact V1 creation authorities")
        parents = tuple(item.parent_intent_id for item in self.ordered_creation_requests)
        if len(parents) != len(set(parents)):
            raise ValueError("product plan authority contains duplicate parent identity")
        if any(
            item.runtime_id != self.runtime_id
            or item.execution_plan_id != self.execution_plan_id
            or item.execution_plan_sha256 != self.execution_plan_sha256
            for item in self.ordered_creation_requests
        ):
            raise ValueError("product creation request differs from plan owner")


class K6DProductPlanAuthorityReaderV1(Protocol):
    def read_plan_authority_v1(
        self, *, runtime_id: str, binding_id: str, execution_plan_id: str
    ) -> K6DProductPlanAuthorityV1: ...


class K6DCommittedSourceEventReaderV1(Protocol):
    def read_committed_source_event_v1(
        self, *, runtime_id: str, binding_id: str, source_event_ref: str
    ) -> "K6DCommittedSourceEventReadbackV1": ...


@dataclass(frozen=True)
class K6DCommittedSourceEventReadbackV1:
    event: RuntimeEventEnvelopeV2
    ingress_receipt: RuntimeEventIngressReceiptV1

    def __post_init__(self) -> None:
        if not isinstance(self.event, RuntimeEventEnvelopeV2):
            raise TypeError("event must be RuntimeEventEnvelopeV2")
        if not isinstance(self.ingress_receipt, RuntimeEventIngressReceiptV1):
            raise TypeError("ingress_receipt must be RuntimeEventIngressReceiptV1")
        if (
            self.ingress_receipt.event_id != self.event.event_id
            or self.ingress_receipt.runtime_id != self.event.runtime_id
            or self.ingress_receipt.event_key_sha256 != self.event.event_key_sha256
            or self.ingress_receipt.runtime_sequence != self.event.sequence
        ):
            raise ValueError("committed source event and ingress receipt do not close")


class MiniQMTKernelV2ProductCoordinator:
    """Only product root for plan start and committed-source ingress."""

    def __init__(
        self,
        *,
        plan_authority_reader: K6DProductPlanAuthorityReaderV1,
        source_event_reader: K6DCommittedSourceEventReaderV1,
        cutover_coordinator: KernelProductCutoverCoordinator,
        creation_coordinator_factory: Callable[[str], KernelAlgoCreationCoordinatorV2],
        ingress_coordinator: KernelIngressCoordinatorV1,
        delivery_worker: KernelProductDeliveryWorkerV3,
    ) -> None:
        if not callable(getattr(plan_authority_reader, "read_plan_authority_v1", None)):
            raise TypeError("plan_authority_reader must expose read_plan_authority_v1")
        if not callable(getattr(source_event_reader, "read_committed_source_event_v1", None)):
            raise TypeError("source_event_reader must expose read_committed_source_event_v1")
        if not isinstance(cutover_coordinator, KernelProductCutoverCoordinator):
            raise TypeError("cutover_coordinator must be KernelProductCutoverCoordinator")
        if not callable(creation_coordinator_factory):
            raise TypeError("creation_coordinator_factory must be callable")
        if not isinstance(ingress_coordinator, KernelIngressCoordinatorV1):
            raise TypeError("ingress_coordinator must be KernelIngressCoordinatorV1")
        if not isinstance(delivery_worker, KernelProductDeliveryWorkerV3):
            raise TypeError("delivery_worker must be KernelProductDeliveryWorkerV3")
        self._plan_reader = plan_authority_reader
        self._source_reader = source_event_reader
        self._cutover = cutover_coordinator
        self._creation_factory = creation_coordinator_factory
        self._ingress = ingress_coordinator
        self._delivery_worker = delivery_worker

    def start_execution_plan_v1(
        self,
        *,
        runtime_id: str,
        binding_id: str,
        execution_plan_id: str,
        worker_incarnation_id: str,
    ) -> K6DProductPlanStartReceiptV1:
        for name, value in (
            ("runtime_id", runtime_id),
            ("binding_id", binding_id),
            ("execution_plan_id", execution_plan_id),
            ("worker_incarnation_id", worker_incarnation_id),
        ):
            self._identity(value, name)
        plan = self._plan_reader.read_plan_authority_v1(
            runtime_id=runtime_id,
            binding_id=binding_id,
            execution_plan_id=execution_plan_id,
        )
        if (plan.runtime_id, plan.binding_id, plan.execution_plan_id) != (
            runtime_id,
            binding_id,
            execution_plan_id,
        ):
            raise ValueError("plan authority readback differs from requested product owner")
        owner = self._cutover.activate_kernel_v2_route_v1(
            runtime_id=runtime_id,
            binding_id=binding_id,
            trade_date=plan.trade_date,
            worker_incarnation_id=worker_incarnation_id,
        )
        strict_owner = ProductRouteOwnerV1.model_validate_json(owner.model_dump_json(), strict=True)
        creator = self._creation_factory(worker_incarnation_id)
        if not isinstance(creator, KernelAlgoCreationCoordinatorV2):
            raise TypeError("creation_coordinator_factory must return KernelAlgoCreationCoordinatorV2")
        results: list[K6DProductParentStartResultV1] = []
        for ordinal, request in enumerate(plan.ordered_creation_requests, start=1):
            promoted = KernelAlgoCreationRequestV2.from_v1(
                request,
                binding_id=binding_id,
                product_route_cutover_receipt_sha256=strict_owner.current_receipt_sha256,
                product_route_owner_sha256=strict_owner.owner_sha256,
                product_route_epoch=strict_owner.current_route_epoch,
                effective_new_instance_sequence=strict_owner.effective_new_instance_sequence,
            )
            created = creator.create(promoted)
            event = created.get("event")
            ingress = created.get("ingress_receipt")
            algo = created.get("algo")
            if not isinstance(event, RuntimeEventEnvelopeV2) or not isinstance(ingress, RuntimeEventIngressReceiptV1):
                raise TypeError("product creation repository returned malformed durable readback")
            algo_id = getattr(algo, "algo_instance_id", None)
            if type(algo_id) is not str or not algo_id:
                raise TypeError("product creation readback lacks algo identity")
            if (
                event.event_type.value != "ALGO_START"
                or event.runtime_id != request.runtime_id
                or ingress.runtime_id != event.runtime_id
                or ingress.event_id != event.event_id
                or ingress.event_key_sha256 != event.event_key_sha256
                or ingress.runtime_sequence != event.sequence
                or getattr(algo, "runtime_id", None) != request.runtime_id
                or getattr(algo, "parent_intent_id", None) != request.parent_intent_id
                or algo_id not in ingress.ordered_target_algo_instance_ids
            ):
                raise ValueError("product creation readback does not close to its plan parent authority")
            failed = getattr(getattr(algo, "status", None), "value", None) == "FAILED"
            reason = getattr(algo, "failure_receipt_id", None) if failed else None
            results.append(
                K6DProductParentStartResultV1.create(
                    plan_intent_ordinal=ordinal,
                    parent_intent_id=request.parent_intent_id,
                    algo_instance_id=algo_id,
                    event_id=event.event_id,
                    ingress_receipt_sha256=ingress.receipt_sha256,
                    start_status=(
                        K6DProductStartStatusV1.FAILED_TERMINAL if failed else K6DProductStartStatusV1.STARTED
                    ),
                    terminal_reason_or_null=reason,
                )
            )
        return K6DProductPlanStartReceiptV1.create(
            runtime_id=runtime_id,
            binding_id=binding_id,
            execution_plan_id=execution_plan_id,
            execution_plan_sha256=plan.execution_plan_sha256,
            product_route_receipt_sha256=strict_owner.current_receipt_sha256,
            ordered_parent_results=tuple(results),
        )

    def ingest_committed_source_event_v1(
        self,
        *,
        runtime_id: str,
        binding_id: str,
        source_event_ref: str,
        worker_incarnation_id: str,
    ) -> RuntimeEventIngressReceiptV1:
        for name, value in (
            ("runtime_id", runtime_id),
            ("binding_id", binding_id),
            ("source_event_ref", source_event_ref),
            ("worker_incarnation_id", worker_incarnation_id),
        ):
            self._identity(value, name)
        readback = self._source_reader.read_committed_source_event_v1(
            runtime_id=runtime_id,
            binding_id=binding_id,
            source_event_ref=source_event_ref,
        )
        if not isinstance(readback, K6DCommittedSourceEventReadbackV1):
            raise TypeError("source reader must return K6DCommittedSourceEventReadbackV1")
        event = readback.event
        if event.runtime_id != runtime_id:
            raise ValueError("committed source event crosses runtime owner")
        self.process_committed_event_v1(event=event, receipt=readback.ingress_receipt)
        return readback.ingress_receipt

    def ingest_native_event_v1(self, *, event: RuntimeEventEnvelopeV2) -> RuntimeEventIngressReceiptV1:
        """Internal source-adapter seam: persist first, then run generic product delivery."""

        if not isinstance(event, RuntimeEventEnvelopeV2):
            raise TypeError("event must be RuntimeEventEnvelopeV2")
        receipt = self._ingress.ingest(event=event)
        self._process_deliveries(receipt=receipt, event=event)
        return receipt

    def ingest_callback_event_v1(
        self,
        *,
        event: RuntimeEventEnvelopeV2,
        callback_mapping_update: KernelCallbackMappingUpdateV1,
    ) -> RuntimeEventIngressReceiptV1:
        """Persist a gateway/OMS callback and mapping CAS before V3 delivery."""

        if not isinstance(event, RuntimeEventEnvelopeV2):
            raise TypeError("event must be RuntimeEventEnvelopeV2")
        if not isinstance(callback_mapping_update, KernelCallbackMappingUpdateV1):
            raise TypeError("callback_mapping_update must be KernelCallbackMappingUpdateV1")
        receipt = self._ingress.ingest(
            event=event,
            callback_mapping_update=callback_mapping_update,
        )
        self._process_deliveries(receipt=receipt, event=event)
        return receipt

    def process_committed_event_v1(
        self,
        *,
        event: RuntimeEventEnvelopeV2,
        receipt: RuntimeEventIngressReceiptV1,
    ) -> None:
        """Continue one independently read committed ingress receipt.

        Outbox outcome and callback writers own their atomic event/mapping
        transaction.  They pass the strict post-commit readback through this
        seam so the product root cannot manufacture a second ingress event or
        bypass the generic V3 delivery worker.
        """

        if not isinstance(event, RuntimeEventEnvelopeV2):
            raise TypeError("event must be RuntimeEventEnvelopeV2")
        if not isinstance(receipt, RuntimeEventIngressReceiptV1):
            raise TypeError("receipt must be RuntimeEventIngressReceiptV1")
        if (
            receipt.event_id != event.event_id
            or receipt.runtime_id != event.runtime_id
            or receipt.event_key_sha256 != event.event_key_sha256
            or receipt.runtime_sequence != event.sequence
        ):
            raise ValueError("committed ingress receipt differs from its event authority")
        self._process_deliveries(receipt=receipt, event=event)

    def _process_deliveries(
        self,
        *,
        receipt: RuntimeEventIngressReceiptV1,
        event: RuntimeEventEnvelopeV2,
    ) -> None:
        logical = datetime.fromisoformat(event.event_time_utc.replace("Z", "+00:00"))
        failures: list[dict[str, Any]] = []
        for delivery_id in receipt.ordered_delivery_ids:
            try:
                self._delivery_worker.process_committed_delivery_v3(
                    delivery_id=delivery_id,
                    lease_expires_at=logical + timedelta(seconds=60),
                    logical_time_utc=logical,
                )
            except Exception as exc:  # noqa: BLE001 - process every independently durable target, then fail loud.
                failures.append(
                    {
                        "delivery_id": delivery_id,
                        **safe_exception_summary_v1(exc),
                        "reason_code": getattr(exc, "reason_code", "MINIQMT_K6_PRODUCT_DELIVERY_FAILED"),
                    }
                )
        if failures:
            raise K6DProductDeliveryAggregateError(event=event, failures=tuple(failures))

    @staticmethod
    def _identity(value: Any, field_name: str) -> str:
        if type(value) is not str or not value or value != value.strip():
            raise TypeError(f"{field_name} must be a canonical identity")
        return value


__all__ = [
    "K6DCommittedSourceEventReaderV1",
    "K6DCommittedSourceEventReadbackV1",
    "K6DProductParentStartResultV1",
    "K6DProductPlanAuthorityReaderV1",
    "K6DProductPlanAuthorityV1",
    "K6DProductPlanStartReceiptV1",
    "K6DProductStartStatusV1",
    "K6DProductDeliveryAggregateError",
    "MiniQMTKernelV2ProductCoordinator",
]
