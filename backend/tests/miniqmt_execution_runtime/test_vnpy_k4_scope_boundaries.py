from __future__ import annotations

import inspect

from backend.execution_algos.vnpy_compat.facade_adapter import VnpyFacadeBackedPluginAdapterV1
from backend.execution_algos.vnpy_style.plugin_factories import current_three_process_bindings_v3
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v3,
    current_three_descriptors_v3,
    current_three_manifests_v3,
)
from backend.services.miniqmt_execution_runtime.kernel_creation import KernelAlgoCreationCoordinatorV1
from backend.services.miniqmt_execution_runtime.kernel_delivery import KernelDeliveryWorkerV1


def test_k4_keeps_current_three_catalog_and_process_bindings_on_the_existing_pure_plugin_route() -> None:
    manifests = current_three_manifests_v3()
    descriptors = current_three_descriptors_v3()
    creation_bindings = current_three_creation_bindings_v3()
    process_bindings = current_three_process_bindings_v3()

    assert tuple(item.algo_code for item in manifests) == (
        "BEST_LIMIT_MINIQMT",
        "SNIPER_MINIQMT",
        "TWAP_LITE_MINIQMT",
    )
    assert all(item.algo_code not in {"ICEBERG", "STOP"} for item in manifests)
    assert all("backend.execution_algos.vnpy_style" in item.implementation_ref for item in manifests)
    assert tuple(item.algo_code for item in creation_bindings) == tuple(item.algo_code for item in manifests)
    for descriptor in descriptors:
        factory = process_bindings.resolve(descriptor.factory_binding_id)
        assert factory is not None
        assert not isinstance(factory, VnpyFacadeBackedPluginAdapterV1)


def test_k4_facade_authority_is_optional_and_disabled_at_the_existing_kernel_composition_boundary() -> None:
    creation = inspect.signature(KernelAlgoCreationCoordinatorV1)
    delivery = inspect.signature(KernelDeliveryWorkerV1)

    assert creation.parameters["facade_authority"].default is None
    assert delivery.parameters["facade_authority"].default is None
    assert delivery.parameters["gateway_catalog"].default is None
