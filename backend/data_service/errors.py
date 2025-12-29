"""Common error types and helpers for the data service layer.

Strict-mode requirement:
- Never fabricate or approximate data to hide errors;
- Always surface data source failures explicitly to callers;
- Use structured logging so upstream layers can build alerts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

import logging


logger = logging.getLogger("aistock.data_service")


@dataclass
class DataSourceContext:
    api: str
    source: str
    universe_size: int | None = None
    freq: str | None = None
    extra: Mapping[str, Any] | None = None


class DataSourceError(RuntimeError):
    """Raised when a data source cannot fulfill a strict data request."""

    def __init__(self, message: str, *, context: DataSourceContext | None = None) -> None:
        super().__init__(message)
        self.context = context


def log_data_source_failure(message: str, *, context: DataSourceContext | None = None, exc: BaseException | None = None) -> None:
    """Log a structured data source failure event.

    This helper does *not* swallow errors; callers are expected to raise
    a DataSourceError or re-raise the original exception after logging.
    """

    payload: dict[str, Any] = {"message": message, "ts": datetime.now().isoformat()}
    if context is not None:
        payload.update(
            {
                "api": context.api,
                "source": context.source,
                "universe_size": context.universe_size,
                "freq": context.freq,
                "extra": dict(context.extra or {}),
            }
        )
    if exc is not None:
        payload["exc_type"] = type(exc).__name__
        payload["exc"] = str(exc)

    logger.error("data_source_failure", extra={"event": payload})
