"""
__init__.py for the TDX scheduling and quoting utilities.

This module provides a minimal, well‑behaved public API that:
1. Guarantees a task identity is present when scheduling.
2. Enforces that any supplied repair date falls within the optional
   bounded window defined by ``repair_start`` and ``repair_end``.
3. Returns the most recent close price from a quote payload rather than
   a stale/previous value.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional


class SchedulerError(RuntimeError):
    """Base exception for scheduling related errors."""


class MissingTaskIdentityError(SchedulerError):
    """Raised when a task dictionary lacks an ``id`` field."""


class RepairDateOutOfBoundsError(SchedulerError):
    """Raised when a repair date is outside the allowed window."""


def _parse_date(value: Any) -> Optional[datetime]:
    """Parse a date-like value into a ``datetime`` instance.

    Accepts ``datetime`` objects, ISO‑8601 strings, or ``None``.
    Returns ``None`` if the value cannot be interpreted.
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def schedule_task(task: Dict[str, Any],
                 repair_start: Optional[Any] = None,
                 repair_end: Optional[Any] = None) -> Dict[str, Any]:
    """
    Validate and schedule a task.

    Parameters
    ----------
    task: dict
        Must contain an ``id`` key. Optional ``repair_date`` key can be
        validated against the optional bounds.
    repair_start, repair_end: optional
        Date bounds (datetime or ISO‑8601 string) that constrain
        ``task['repair_date']`` when provided.

    Returns
    -------
    dict
        The original task dictionary if validation succeeds.

    Raises
    ------
    MissingTaskIdentityError
        If ``task`` does not contain an ``id`` entry.
    RepairDateOutOfBoundsError
        If a ``repair_date`` is present but falls outside the supplied bounds.
    """
    # --- 1. Ensure task identity exists ------------------------------------
    if "id" not in task or task["id"] is None:
        raise MissingTaskIdentityError("Task must contain a non‑null 'id' field.")

    # --- 2. Validate repair date bounds if both are supplied -------------
    if "repair_date" in task:
        repair_date = _parse_date(task["repair_date"])
        if repair_date is None:
            raise RepairDateOutOfBoundsError(
                f"Unable to parse repair_date '{task['repair_date']}'."
            )

        start = _parse_date(repair_start) if repair_start is not None else None
        end = _parse_date(repair_end) if repair_end is not None else None

        if start is not None and repair_date < start:
            raise RepairDateOutOfBoundsError(
                f"Repair date {repair_date.isoformat()} is before allowed start "
                f"{start.isoformat()}."
            )
        if end is not None and repair_date > end:
            raise RepairDateOutOfBoundsError(
                f"Repair date {repair_date.isoformat()} is after allowed end "
                f"{end.isoformat()}."
            )

    # If we reach this point the task is considered valid.
    return task


def quote_adapter(quote: Dict[str, Any]) -> Any:
    """
    Extract the latest close price from a quote payload.

    The original implementation mistakenly returned a prior close price.
    This function now prefers ``quote['close']`` if present; otherwise,
    it falls back to ``quote.get('previous_close')`` for backward compatibility.

    Parameters
    ----------
    quote: dict
        Expected to contain a ``close`` key with the most recent price.

    Returns
    -------
    Any
        The close price value.

    Raises
    ------
    KeyError
        If neither ``close`` nor ``previous_close`` keys exist.
    """
    if "close" in quote:
        return quote["close"]
    if "previous_close" in quote:
        return quote["previous_close"]
    raise KeyError("Quote payload must contain a 'close' or 'previous_close' field.")


__all__ = [
    "SchedulerError",
    "MissingTaskIdentityError",
    "RepairDateOutOfBoundsError",
    "schedule_task",
    "quote_adapter",
]