from __future__ import annotations

import pytest

from backend.services.miniqmt_execution_runtime.kernel_delivery import (
    KernelAlgoCreationRequestV2,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_creation import _request


def _request_v2() -> KernelAlgoCreationRequestV2:
    return KernelAlgoCreationRequestV2.from_v1(
        _request(),
        binding_id="binding_k6d",
        product_route_cutover_receipt_sha256="d" * 64,
        product_route_owner_sha256="e" * 64,
        product_route_epoch=1,
        effective_new_instance_sequence=7,
    )


def test_product_creation_request_v2_hash_closes_exact_binding_and_route_lineage() -> None:
    request = _request_v2()

    assert request.validate_hashes_v2() is request
    assert request.binding_id == "binding_k6d"
    assert request.product_route_epoch == 1
    assert request.effective_new_instance_sequence == 7


@pytest.mark.parametrize(
    "field_name,value",
    (
        ("binding_id", "binding_other"),
        ("product_route_cutover_receipt_sha256", "f" * 64),
        ("product_route_owner_sha256", "a" * 64),
        ("product_route_epoch", 2),
        ("effective_new_instance_sequence", 8),
    ),
)
def test_product_creation_request_v2_rejects_hash_correct_shape_with_lineage_drift(
    field_name: str, value: object
) -> None:
    request = _request_v2().model_copy(update={field_name: value})

    with pytest.raises(ValueError, match="product creation request hash differs"):
        request.validate_hashes_v2()
