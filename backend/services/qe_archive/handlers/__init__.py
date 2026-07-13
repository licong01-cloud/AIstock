"""QE archive event handlers (paper_v2 + factor_value capture).

Per D5 protocol (drawer 9cd6d6bb) Q3.a=Option a: handlers live under
backend/services/qe_archive/handlers/ and coexist with existing qe handlers.
Worker default remains disabled until production cadence ratification (Q3.c).
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
