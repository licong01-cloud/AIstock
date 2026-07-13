"""Runtime-selection guard for MiniQMT execution.

MiniQMT SIM now uses the event-loop runtime unconditionally.  The legacy
``MINIQMT_EXECUTION_RUNTIME`` flag is retained only as a deprecated read point
for older operator surfaces; it must never route SIM submissions to compiler.
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
    """Return the MiniQMT SIM runtime kind, fixed to event_loop.

    ``MINIQMT_EXECUTION_RUNTIME=compiler`` is treated as a retired spelling and
    resolves to event_loop rather than creating a hidden B-route fallback.
    Unknown values remain loud.
    """

    source = os.environ if environ is None else environ
    raw = str(source.get(MINIQMT_EXECUTION_RUNTIME_ENV, MiniQMTExecutionRuntimeKind.EVENT_LOOP.value) or "").strip().lower()
    if raw in {"", MiniQMTExecutionRuntimeKind.COMPILER.value, MiniQMTExecutionRuntimeKind.EVENT_LOOP.value}:
        return MiniQMTExecutionRuntimeKind.EVENT_LOOP
    raise ValueError(
        "unsupported MiniQMT execution runtime; "
        f"reason_code=MINIQMT_EXECUTION_RUNTIME_UNSUPPORTED, "
        f"{MINIQMT_EXECUTION_RUNTIME_ENV}={raw!r}, "
        "expected event_loop; compiler is retired for SIM"
    )

