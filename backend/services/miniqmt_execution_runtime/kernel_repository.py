"""Stable public facade for the PostgreSQL-only MiniQMT K2-A repository."""

from __future__ import annotations

from .kernel_repository_common import (
    KernelRepositoryBase,
    KernelRepositoryCommitUnknown,
    KernelRepositoryConflict,
    KernelRepositorySchemaError,
)
from .kernel_repository_event_delivery import KernelRepositoryEventDeliveryMixin
from .kernel_repository_schema import KernelRepositorySchemaMixin
from .kernel_repository_timer_session import KernelRepositoryTimerSessionMixin
from .kernel_repository_transition_outbox import KernelRepositoryTransitionOutboxMixin


# Preserve the historical public exception import identity after moving implementation ownership.
KernelRepositoryConflict.__module__ = __name__
KernelRepositorySchemaError.__module__ = __name__
KernelRepositoryCommitUnknown.__module__ = __name__


class PostgresMiniQMTKernelRepository(
    KernelRepositorySchemaMixin,
    KernelRepositoryEventDeliveryMixin,
    KernelRepositoryTransitionOutboxMixin,
    KernelRepositoryTimerSessionMixin,
    KernelRepositoryBase,
):
    """Strict K2 persistence with one public facade and one connection owner."""


__all__ = [
    "KernelRepositoryCommitUnknown",
    "KernelRepositoryConflict",
    "KernelRepositorySchemaError",
    "PostgresMiniQMTKernelRepository",
]
