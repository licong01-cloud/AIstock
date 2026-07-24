"""Execution algorithm package with explicit, lazy registry activation."""

from __future__ import annotations

from typing import Any


__all__ = ["ALGO_REGISTRY", "get_algo"]
_REGISTRY_EXPORTS = frozenset(__all__)
_REGISTRY_LOADED = False


def _load_registry_exports() -> None:
    global _REGISTRY_LOADED
    if _REGISTRY_LOADED:
        return

    from .registry import ALGO_REGISTRY as registry
    from .registry import get_algo as registry_get_algo

    # Named imports preserve the historical built-in set without package-import
    # registration side effects for unrelated subpackages such as vnpy_compat.
    from . import ac_optimal_algo as _ac_optimal_algo  # noqa: F401
    from . import close_price_algo as _close_price_algo  # noqa: F401
    from . import pov_algo as _pov_algo  # noqa: F401
    from . import sbb_ema_algo as _sbb_ema_algo  # noqa: F401
    from . import tail_boost_algo as _tail_boost_algo  # noqa: F401
    from . import tail_substitute_algo as _tail_substitute_algo  # noqa: F401
    from . import twap_algo as _twap_algo  # noqa: F401
    from . import v24_plan_algo as _v24_plan_algo  # noqa: F401
    from . import v25_1_small_cap_algo as _v25_1_small_cap_algo  # noqa: F401
    from . import v25_two_stage_algo as _v25_two_stage_algo  # noqa: F401
    from . import vwap_algo as _vwap_algo  # noqa: F401
    from .vnpy_style import legacy_adapter as _legacy_adapter  # noqa: F401

    globals()["ALGO_REGISTRY"] = registry
    globals()["get_algo"] = registry_get_algo
    _REGISTRY_LOADED = True


def __getattr__(name: str) -> Any:
    if name in _REGISTRY_EXPORTS:
        _load_registry_exports()
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted((*globals(), *_REGISTRY_EXPORTS))
