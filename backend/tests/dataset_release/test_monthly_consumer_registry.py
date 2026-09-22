from __future__ import annotations

from backend.services.dataset_release.monthly_consumer_registry import (
    monthly_controller_preflight_probes,
)
from backend.services.dataset_release.monthly_hmm_consumer_probe import (
    MonthlyHMMConsumerProbe,
)
from backend.services.dataset_release.monthly_qe_consumer_probe import (
    MonthlyQEConsumerProbe,
)
from backend.services.dataset_release.monthly_shared_consumer_probe import (
    MonthlySharedDatasetConsumerProbe,
)
from backend.services.dataset_release.monthly_unified import REQUIRED_CONSUMERS


def test_monthly_controller_preflight_registry_is_exact_and_code_owned() -> None:
    probes = monthly_controller_preflight_probes()

    assert tuple(probes) == REQUIRED_CONSUMERS
    assert all(isinstance(probes[name], MonthlyQEConsumerProbe) for name in REQUIRED_CONSUMERS[:5])
    assert isinstance(probes["hmm_file_only"], MonthlyHMMConsumerProbe)
    assert all(
        isinstance(probes[name], MonthlySharedDatasetConsumerProbe)
        for name in REQUIRED_CONSUMERS[6:]
    )


def test_monthly_consumer_registry_cannot_be_mutated() -> None:
    probes = monthly_controller_preflight_probes()

    try:
        probes["qe_single"] = MonthlyQEConsumerProbe()  # type: ignore[index]
    except TypeError:
        pass
    else:  # pragma: no cover - MappingProxyType contract
        raise AssertionError("monthly probe registry was mutable")
