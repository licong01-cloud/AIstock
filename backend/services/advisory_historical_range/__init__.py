"""Historical-range Advisory research contracts and persistence foundation."""

from importlib import import_module

__all__ = [
    "HistoricalRangeArtifactKind",
    "HistoricalRangeBatchStatus",
    "HistoricalRangeDayStatus",
    "HistoricalRangeOperationStatus",
    "HistoricalRangeOperationType",
    "HistoricalRangeOutcomeStatus",
    "HistoricalRangeProgramStatus",
]


def __getattr__(name: str) -> object:
    # Business contract consumers retain the original exported objects. Pure
    # inference helpers also run in the Python 3.10 Qlib node, where importing
    # the Windows backend's business models (datetime.UTC) is unnecessary.
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(".models", __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
