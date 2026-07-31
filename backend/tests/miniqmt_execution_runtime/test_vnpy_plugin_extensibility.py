"""Exact K1 extension proof for the two approved K5 plugins."""

from backend.execution_algos.vnpy_compat.k5_plugin_manifests import (
    k5_creation_bindings_v1,
    k5_descriptors_v1,
)
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v3,
    current_three_descriptors_v3,
)


def test_k5_extends_the_immutable_current_three_with_exactly_two_descriptors() -> None:
    current_descriptors = current_three_descriptors_v3()
    current_creation = current_three_creation_bindings_v3()
    k5_descriptors = k5_descriptors_v1()
    k5_creation = k5_creation_bindings_v1()

    assert tuple(item.manifest.algo_code for item in current_descriptors) == (
        "BEST_LIMIT_MINIQMT",
        "SNIPER_MINIQMT",
        "TWAP_LITE_MINIQMT",
    )
    assert tuple(item.manifest.algo_code for item in k5_descriptors) == ("ICEBERG", "STOP")
    assert tuple(item.algo_code for item in k5_creation) == ("ICEBERG", "STOP")
    assert not ({item.plugin_key for item in current_descriptors} & {item.plugin_key for item in k5_descriptors})
    assert current_descriptors == current_three_descriptors_v3()
    assert current_creation == current_three_creation_bindings_v3()
