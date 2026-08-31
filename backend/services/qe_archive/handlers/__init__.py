"""Active QE archive event handlers.

Per D5 protocol (drawer 9cd6d6bb) Q3.a=Option a: handlers live under
backend/services/qe_archive/handlers/ and coexist with existing qe handlers.
Worker default remains disabled until production cadence ratification (Q3.c).

BUG-1001 retired the former factor-value handler and its
``factor.recompute.completed`` event. This package exports only active handler
contracts; the retained ``qe_archive.factor_value`` table has no runtime writer
or reader.
"""

from .contract import (
    ArchiveHandler,
    ArchiveResult,
    HandlerStatus,
    PayloadValidationError,
    UnsupportedEventError,
)
from .multi_alpha_combine_archive_handler import (
    MULTI_ALPHA_COMBINE_EVENT_TYPE,
    MULTI_ALPHA_COMBINE_SCHEMA_VERSION,
    MultiAlphaCombineArchiveHandler,
)

__all__ = [
    "ArchiveHandler",
    "ArchiveResult",
    "HandlerStatus",
    "PayloadValidationError",
    "UnsupportedEventError",
    "MULTI_ALPHA_COMBINE_EVENT_TYPE",
    "MULTI_ALPHA_COMBINE_SCHEMA_VERSION",
    "MultiAlphaCombineArchiveHandler",
]
