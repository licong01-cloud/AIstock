"""Code-owned controller preflights for the exact monthly consumer set.

This module intentionally does not build ``CONSUMER_VALIDATE``.  These probes
read the controller candidate; a production consumer receipt must additionally
come from the registered WSL/node1 process.  Keeping that distinction prevents
controller readability from being mislabeled as a remote-node readback.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping, Sequence

from .monthly_consumer_validation import MonthlyConsumerProbe
from .monthly_hmm_consumer_probe import MonthlyHMMConsumerProbe
from .monthly_qe_consumer_probe import monthly_qe_consumer_probes
from .monthly_shared_consumer_probe import monthly_shared_dataset_consumer_probes
from .monthly_unified import REQUIRED_CONSUMERS


def monthly_controller_preflight_probes() -> Mapping[str, MonthlyConsumerProbe]:
    probes: dict[str, MonthlyConsumerProbe] = {
        **monthly_qe_consumer_probes(),
        "hmm_file_only": MonthlyHMMConsumerProbe(),
        **monthly_shared_dataset_consumer_probes(),
    }
    if tuple(probes) != REQUIRED_CONSUMERS:
        raise RuntimeError("monthly consumer probe registry order or coverage differs")
    return MappingProxyType(probes)


__all__: Sequence[str] = (
    "monthly_controller_preflight_probes",
)
