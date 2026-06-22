"""Runtime-selection guard for MiniQMT execution.

The durable event-loop implementation is additive and must stay inert unless
explicitly selected by ``MINIQMT_EXECUTION_RUNTIME=event_loop``.
"""

from __future__ import annotations

import os
from enum import Enum
from typing import Mapping

MINIQMT_EXECUTION_RUNTIME_ENV = "MINIQMT_EXECUTION_RUNTIME"


class MiniQMTExecutionRuntimeKind(str, Enum):
    COMPILER = "compiler"
    EVENT_LOOP = "event_loop"


def get_miniqmt_execution_runtime_kind(
    environ: Mapping[str, str] | None = None,
) -> MiniQMTExecutionRuntimeKind:
    """Return the selected MiniQMT runtime, defaulting to the legacy compiler.

    Unknown values are loud because silently falling back could route real
    orders through the wrong execution lifecycle.
    """

    source = os.environ if environ is None else environ
    raw = str(source.get(MINIQMT_EXECUTION_RUNTIME_ENV, MiniQMTExecutionRuntimeKind.COMPILER.value) or "").strip().lower()
    if raw == MiniQMTExecutionRuntimeKind.COMPILER.value:
        return MiniQMTExecutionRuntimeKind.COMPILER
    if raw == MiniQMTExecutionRuntimeKind.EVENT_LOOP.value:
        return MiniQMTExecutionRuntimeKind.EVENT_LOOP
    raise ValueError(
        "unsupported MiniQMT execution runtime; "
        f"reason_code=MINIQMT_EXECUTION_RUNTIME_UNSUPPORTED, "
        f"{MINIQMT_EXECUTION_RUNTIME_ENV}={raw!r}, "
        "expected one of: compiler,event_loop"
    )

